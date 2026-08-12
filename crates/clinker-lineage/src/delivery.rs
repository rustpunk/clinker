//! Independently bounded, non-blocking OpenLineage event delivery.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use clinker_plan::config::{ObservabilityDropPolicy, ResolvedLineageDeliveryPolicy};

use crate::RunEvent;

/// How long a producer keeps trying for the queue lock before calling it
/// contention. Orders of magnitude above the hold time, which is one
/// `VecDeque` operation, so it only matters when the scheduler is against us.
const PRODUCER_LOCK_PATIENCE: Duration = Duration::from_millis(50);

/// Fixed delivery limits copied from the admitted workspace policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LineageDeliveryConfig {
    queue_bytes: usize,
    max_event_bytes: usize,
    flush_deadline: Duration,
}

/// Invalid or unsupported delivery limits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineageDeliveryConfigError {
    /// The queue byte capacity was zero or did not fit this target.
    InvalidQueueBytes,
    /// The per-event cap was zero, exceeded the queue, or did not fit this target.
    InvalidMaxEventBytes,
    /// The flush deadline was zero.
    InvalidFlushDeadline,
}

impl std::fmt::Display for LineageDeliveryConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidQueueBytes => formatter.write_str(
                "lineage queue byte capacity must be non-zero and fit the current target",
            ),
            Self::InvalidMaxEventBytes => formatter.write_str(
                "lineage event byte cap must be non-zero, fit the current target, and not exceed the queue capacity",
            ),
            Self::InvalidFlushDeadline => {
                formatter.write_str("lineage flush deadline must be non-zero")
            }
        }
    }
}

impl std::error::Error for LineageDeliveryConfigError {}

impl LineageDeliveryConfig {
    /// Construct fixed limits for a delivery worker.
    ///
    /// Production callers should prefer [`Self::from_resolved`]. This checked
    /// constructor exists for deterministic sink fixtures and embedders that
    /// have already admitted equivalent bounds.
    pub fn new(
        queue_bytes: usize,
        max_event_bytes: usize,
        flush_deadline: Duration,
    ) -> Result<Self, LineageDeliveryConfigError> {
        if queue_bytes == 0 {
            return Err(LineageDeliveryConfigError::InvalidQueueBytes);
        }
        if max_event_bytes == 0 || max_event_bytes > queue_bytes {
            return Err(LineageDeliveryConfigError::InvalidMaxEventBytes);
        }
        if flush_deadline.is_zero() {
            return Err(LineageDeliveryConfigError::InvalidFlushDeadline);
        }
        Ok(Self {
            queue_bytes,
            max_event_bytes,
            flush_deadline,
        })
    }

    /// Copy all delivery bounds from the complete resolved lineage policy.
    pub fn from_resolved(
        policy: &ResolvedLineageDeliveryPolicy,
    ) -> Result<Self, LineageDeliveryConfigError> {
        let queue_bytes = usize::try_from(policy.queue_bytes().get())
            .map_err(|_| LineageDeliveryConfigError::InvalidQueueBytes)?;
        let max_event_bytes = usize::try_from(policy.max_event_bytes().get())
            .map_err(|_| LineageDeliveryConfigError::InvalidMaxEventBytes)?;
        match policy.drop_policy() {
            ObservabilityDropPolicy::DropNewest => {}
        }
        Self::new(queue_bytes, max_event_bytes, policy.flush_timeout())
    }
}

/// Result of one non-blocking producer admission attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineageAdmission {
    /// The complete owned event buffer entered the lineage queue.
    Accepted,
    /// Serialization exceeded the admitted per-event byte cap.
    DroppedEventTooLarge,
    /// The event could not be serialized at all. Distinct from
    /// [`Self::DroppedEventTooLarge`]: raising the byte cap does not fix it.
    DroppedEncodingFailed,
    /// The byte-accounted queue was full, so the newest event was dropped.
    DroppedQueueFull,
    /// Another producer or the worker briefly owned the queue lock.
    DroppedProducerBusy,
    /// The worker had already shut down.
    DroppedShutdown,
}

/// Finite terminal state of the dedicated lineage worker.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LineageDeliveryTerminal {
    /// The producer closed, all accepted events drained, and the sink flushed.
    Shutdown,
    /// The sink rejected event bytes.
    WriteFailed(io::ErrorKind),
    /// The sink accepted all event bytes but failed its final flush.
    FlushFailed(io::ErrorKind),
    /// The sink worker did not finish before the admitted deadline.
    DeadlineExceeded,
}

impl LineageDeliveryTerminal {
    /// Stable bounded label suitable for an operator diagnostic.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Shutdown => "shutdown",
            Self::WriteFailed(_) => "write-failed",
            Self::FlushFailed(_) => "flush-failed",
            Self::DeadlineExceeded => "deadline-exceeded",
        }
    }
}

/// Bounded delivery counters plus one typed worker terminal state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LineageDeliveryOutcome {
    accepted: u64,
    dropped: u64,
    full: u64,
    terminal: LineageDeliveryTerminal,
}

impl LineageDeliveryOutcome {
    /// Events admitted into the lineage queue.
    pub const fn accepted(self) -> u64 {
        self.accepted
    }

    /// Events dropped before queue admission.
    pub const fn dropped(self) -> u64 {
        self.dropped
    }

    /// Drops specifically caused by exhausted queue-byte capacity.
    pub const fn full(self) -> u64 {
        self.full
    }

    /// The worker's finite terminal state.
    pub const fn terminal(self) -> LineageDeliveryTerminal {
        self.terminal
    }
}

#[derive(Default)]
struct Counters {
    accepted: AtomicU64,
    dropped: AtomicU64,
    full: AtomicU64,
}

impl Counters {
    fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            Some(value.saturating_add(1))
        });
    }

    fn outcome(&self, terminal: LineageDeliveryTerminal) -> LineageDeliveryOutcome {
        LineageDeliveryOutcome {
            accepted: self.accepted.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            full: self.full.load(Ordering::Relaxed),
            terminal,
        }
    }
}

/// One queued record and the number of bytes it was admitted against.
///
/// The charge travels with the record so the two sides of the accounting
/// cannot disagree: what a record takes on the way in is what it gives back
/// on the way out.
struct QueuedRecord {
    bytes: Box<[u8]>,
    charge: usize,
}

struct QueueState {
    events: VecDeque<QueuedRecord>,
    queued_bytes: usize,
    closed: bool,
}

struct DeliveryQueue {
    capacity_bytes: usize,
    state: Mutex<QueueState>,
    ready: Condvar,
}

enum QueueAdmission {
    Accepted,
    Full,
    Busy,
    Closed,
}

impl DeliveryQueue {
    fn new(capacity_bytes: usize, max_event_bytes: usize) -> Self {
        let reserved_events = capacity_bytes.div_ceil(max_event_bytes).min(1_024);
        Self {
            // One byte over the operator's figure: the framing of the single
            // event that may sit exactly at the cap, which the validator
            // accepts and promises will enter an empty queue. Every record is
            // charged at the size it occupies, so this is the whole of the
            // excess.
            capacity_bytes: capacity_bytes.saturating_add(1),
            state: Mutex::new(QueueState {
                events: VecDeque::with_capacity(reserved_events),
                queued_bytes: 0,
                closed: false,
            }),
            ready: Condvar::new(),
        }
    }

    /// Take the queue lock, recovering the guard if the lock is poisoned.
    ///
    /// Every path that touches the queue recovers, so one lock cannot carry
    /// two policies: a producer that panics under the lock would otherwise
    /// turn every later emit into a permanent "the queue is closed" while the
    /// worker on the same lock kept draining as though nothing had happened.
    /// Lineage is optional observability whose failures are reported and never
    /// propagated, so the panic of one producer must not silently end the
    /// run's lineage.
    ///
    /// Recovery re-derives `queued_bytes` from the charges the deque actually
    /// holds, which is what keeps the bound trustworthy: the only state a
    /// panic can leave inconsistent is a charge added without its record, and
    /// re-deriving discards that guess rather than carrying it. Each record
    /// owns the charge it was admitted against, so the sum is the queue's true
    /// occupancy and not an estimate of it. The flag is then cleared, because
    /// the invariant is restored and leaving it set would re-derive on every
    /// lock for the rest of the run.
    fn lock_state(&self) -> MutexGuard<'_, QueueState> {
        match self.state.lock() {
            Ok(state) => state,
            Err(poisoned) => self.recovered(poisoned.into_inner()),
        }
    }

    fn recovered<'queue>(
        &'queue self,
        mut state: MutexGuard<'queue, QueueState>,
    ) -> MutexGuard<'queue, QueueState> {
        state.queued_bytes = state.events.iter().map(|record| record.charge).sum();
        self.state.clear_poison();
        state
    }

    /// Admit one framed record without waiting on capacity or the sink.
    ///
    /// `patience` bounds how long the producer re-attempts the queue lock.
    /// The lock is held only for the
    /// length of one `VecDeque` push or pop — plus the instant between the
    /// worker taking it and `Condvar::wait` releasing it — so contention there
    /// says nothing about capacity, the cap, or the sink. Capacity and
    /// shutdown outcomes are never retried.
    ///
    /// Time rather than a count of attempts, because a count measures the
    /// scheduler rather than the contention: on a loaded host a whole count
    /// can be spent inside a single hold. Waiting is still bounded and still
    /// never involves the sink.
    ///
    /// The record is charged at the size it occupies, separator included; the
    /// queue's own capacity carries the one byte of framing that the
    /// at-the-cap promise needs, so nothing has to be admitted against a
    /// figure different from the one it costs. The charge is stored with the
    /// record so releasing it credits back exactly what it took.
    fn try_push(&self, event: Box<[u8]>, patience: Duration) -> QueueAdmission {
        let deadline = Instant::now().checked_add(patience);
        let mut state = loop {
            match self.state.try_lock() {
                Ok(state) => break state,
                Err(TryLockError::WouldBlock)
                    if deadline.is_some_and(|deadline| Instant::now() < deadline) =>
                {
                    thread::yield_now();
                }
                Err(TryLockError::WouldBlock) => return QueueAdmission::Busy,
                Err(TryLockError::Poisoned(poisoned)) => {
                    break self.recovered(poisoned.into_inner());
                }
            }
        };
        if state.closed {
            return QueueAdmission::Closed;
        }
        if event.len() > self.capacity_bytes.saturating_sub(state.queued_bytes) {
            return QueueAdmission::Full;
        }
        let charge = event.len();
        state.queued_bytes += charge;
        state.events.push_back(QueuedRecord {
            bytes: event,
            charge,
        });
        drop(state);
        self.ready.notify_one();
        QueueAdmission::Accepted
    }

    fn pop(&self) -> Option<Box<[u8]>> {
        let mut state = self.lock_state();
        loop {
            if let Some(record) = state.events.pop_front() {
                state.queued_bytes = state.queued_bytes.saturating_sub(record.charge);
                return Some(record.bytes);
            }
            if state.closed {
                return None;
            }
            state = match self.ready.wait(state) {
                Ok(state) => state,
                Err(poisoned) => self.recovered(poisoned.into_inner()),
            };
        }
    }

    fn close(&self) {
        let mut state = self.lock_state();
        state.closed = true;
        drop(state);
        self.ready.notify_all();
    }
}

struct CappedEventBuffer {
    bytes: Vec<u8>,
    limit: usize,
}

impl CappedEventBuffer {
    fn new(limit: usize) -> Self {
        // The clamp first, then room for the separator: adding before
        // clamping put the byte back where the clamp took it away, so an
        // event that filled a 64 KiB cap exactly still reallocated on the one
        // push the headroom exists for.
        Self {
            bytes: Vec::with_capacity((limit.min(64 * 1024)).saturating_add(1)),
            limit,
        }
    }

    /// The bounded event followed by its record separator.
    ///
    /// The separator is in the buffer so one record is one write: the sink can
    /// share a handle with the run's own output, and two writes take that
    /// handle twice, letting an unrelated line land between an event and its
    /// newline. `max_event_bytes` bounds the event alone, which is why the
    /// cap is applied while the buffer fills rather than to this result.
    fn finish(mut self) -> Box<[u8]> {
        self.bytes.push(b'\n');
        // Shrunk to fit, because the queue holds these for as long as the sink
        // is behind and accounts for exactly the bytes it admitted. Carrying
        // the reservation's spare capacity in would put memory there that
        // nothing is counting.
        self.bytes.into_boxed_slice()
    }
}

impl Write for CappedEventBuffer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Outcome of bounding one event into an owned buffer.
enum SerializedEvent {
    Bounded { framed: Box<[u8]> },
    TooLarge,
    EncodingFailed,
}

fn serialize_event(event: &RunEvent, limit: usize) -> SerializedEvent {
    let mut buffer = CappedEventBuffer::new(limit);
    if let Err(error) = serde_json::to_writer(&mut buffer, event) {
        // `CappedEventBuffer` is the only writer here and its sole failure is
        // refusing the write that would cross the cap, which serde reports as
        // an I/O error. Anything else came from the serializer itself and
        // raising the cap would not fix it, so the two must not be conflated.
        return if error.is_io() {
            SerializedEvent::TooLarge
        } else {
            SerializedEvent::EncodingFailed
        };
    }
    SerializedEvent::Bounded {
        framed: buffer.finish(),
    }
}

fn run_worker<W: Write>(queue: &DeliveryQueue, sink: &mut W) -> LineageDeliveryTerminal {
    while let Some(event) = queue.pop() {
        if let Err(error) = sink.write_all(&event) {
            return LineageDeliveryTerminal::WriteFailed(error.kind());
        }
    }
    match sink.flush() {
        Ok(()) => LineageDeliveryTerminal::Shutdown,
        Err(error) => LineageDeliveryTerminal::FlushFailed(error.kind()),
    }
}

/// One dedicated synchronous OpenLineage sink worker and its non-blocking producer.
pub struct LineageDelivery {
    config: LineageDeliveryConfig,
    queue: Arc<DeliveryQueue>,
    counters: Arc<Counters>,
    outcome_rx: mpsc::Receiver<LineageDeliveryTerminal>,
    worker: Option<JoinHandle<()>>,
}

impl LineageDelivery {
    /// Start a worker that exclusively owns `sink`.
    ///
    /// # Errors
    ///
    /// Returns an I/O error when the dedicated worker thread cannot be
    /// created. No event is admitted and the sink is not written on failure.
    pub fn start<W>(config: LineageDeliveryConfig, mut sink: W) -> io::Result<Self>
    where
        W: Write + Send + 'static,
    {
        #[cfg(debug_assertions)]
        if std::env::var_os("CLINKER_TEST_LINEAGE_WORKER_START_FAILURE").as_deref()
            == Some(std::ffi::OsStr::new("1"))
        {
            return Err(io::Error::other("injected lineage worker startup failure"));
        }
        let queue = Arc::new(DeliveryQueue::new(
            config.queue_bytes,
            config.max_event_bytes,
        ));
        let counters = Arc::new(Counters::default());
        let worker_queue = Arc::clone(&queue);
        let (outcome_tx, outcome_rx) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("clinker-lineage-export".to_owned())
            .spawn(move || {
                let terminal = run_worker(&worker_queue, &mut sink);
                worker_queue.close();
                let _ = outcome_tx.send(terminal);
            })?;
        Ok(Self {
            config,
            queue,
            counters,
            outcome_rx,
            worker: Some(worker),
        })
    }

    /// Serialize a complete event into an owned capped buffer, then attempt
    /// immediate drop-newest admission without waiting on capacity or the sink.
    ///
    /// This is the bulkhead form: it runs beside a live pipeline and never
    /// waits on queue capacity or on the sink. It does yield for the queue
    /// lock, which the worker holds only long enough to take an event off the
    /// queue and never across a sink write, so the wait is bounded by a
    /// pop rather than by however long the collector takes. Dropping there
    /// would report an empty queue as backpressure; real backpressure is
    /// reported by capacity, and byte caps and worker shutdown are reported
    /// on their own terms.
    pub fn try_emit(&self, event: &RunEvent) -> LineageAdmission {
        self.emit(event, PRODUCER_LOCK_PATIENCE)
    }

    fn emit(&self, event: &RunEvent, patience: Duration) -> LineageAdmission {
        let bytes = match serialize_event(event, self.config.max_event_bytes) {
            SerializedEvent::Bounded { framed } => framed,
            SerializedEvent::TooLarge => {
                Counters::increment(&self.counters.dropped);
                return LineageAdmission::DroppedEventTooLarge;
            }
            SerializedEvent::EncodingFailed => {
                Counters::increment(&self.counters.dropped);
                return LineageAdmission::DroppedEncodingFailed;
            }
        };
        match self.queue.try_push(bytes, patience) {
            QueueAdmission::Accepted => {
                Counters::increment(&self.counters.accepted);
                LineageAdmission::Accepted
            }
            QueueAdmission::Full => {
                Counters::increment(&self.counters.dropped);
                Counters::increment(&self.counters.full);
                LineageAdmission::DroppedQueueFull
            }
            QueueAdmission::Busy => {
                Counters::increment(&self.counters.dropped);
                LineageAdmission::DroppedProducerBusy
            }
            QueueAdmission::Closed => {
                Counters::increment(&self.counters.dropped);
                LineageAdmission::DroppedShutdown
            }
        }
    }

    /// Close producer admission and wait no longer than the exact resolved
    /// lineage deadline. A timed-out worker handle is detached on return.
    pub fn finish(mut self) -> LineageDeliveryOutcome {
        let started = Instant::now();
        self.queue.close();
        let remaining = self
            .config
            .flush_deadline
            .checked_sub(started.elapsed())
            .unwrap_or(Duration::ZERO);
        let terminal = match self.outcome_rx.recv_timeout(remaining) {
            Ok(terminal) => {
                if let Some(worker) = self.worker.take() {
                    let _ = worker.join();
                }
                terminal
            }
            Err(mpsc::RecvTimeoutError::Timeout) => LineageDeliveryTerminal::DeadlineExceeded,
            // The worker sends its outcome before it returns, so a closed
            // channel means it did not return — it unwound. Calling that a
            // shutdown would report a dead exporter as a clean flush and leave
            // whatever it had not written unaccounted for.
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                LineageDeliveryTerminal::WriteFailed(io::ErrorKind::Other)
            }
        };
        self.counters.outcome(terminal)
    }
}

impl Drop for LineageDelivery {
    fn drop(&mut self) {
        self.queue.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::openlineage::{EventType, Job, Run};

    fn event_of_known_size() -> RunEvent {
        RunEvent {
            event_time: "2020-02-22T22:42:42Z".to_string(),
            producer: "https://example/producer".to_string(),
            schema_url: "https://example/schema".to_string(),
            event_type: EventType::Complete,
            run: Run::new("0190b7e0-0000-7000-8000-000000000000"),
            job: Job {
                namespace: "clinker".to_string(),
                name: "orders".to_string(),
                facets: None,
            },
            inputs: vec![],
            outputs: vec![],
        }
    }

    /// What a record takes on the way in, it gives back on the way out. The
    /// two sides used different numbers -- the event on entry, the framed
    /// buffer on exit -- so the accounted total fell by a byte per record
    /// until it hit zero and the queue admitted without any bound at all,
    /// which is the one thing a queue exists to prevent.
    #[test]
    fn a_record_releases_exactly_what_it_took() {
        let event = event_of_known_size();
        let exact = serde_json::to_vec(&event).expect("the probe event serializes");
        let queue = DeliveryQueue::new(exact.len() * 4, exact.len());

        for round in 0..64 {
            let SerializedEvent::Bounded { framed } = serialize_event(&event, exact.len()) else {
                panic!("the probe event is within the cap");
            };
            assert!(
                matches!(
                    queue.try_push(framed, Duration::ZERO),
                    QueueAdmission::Accepted
                ),
                "round {round}: a queue emptied of every record has room again"
            );
            assert!(
                queue.pop().is_some(),
                "round {round}: the record comes back"
            );
            let queued = queue.state.lock().expect("uncontended").queued_bytes;
            assert_eq!(
                queued, 0,
                "round {round}: an empty queue accounts for nothing"
            );
        }
    }

    /// One lock, one policy. A producer that panics under the queue lock would
    /// otherwise turn every later emit into "the queue is closed" while the
    /// worker on the same lock drained on, ending the run's lineage silently.
    /// Recovery re-derives the accounting, so the charge such a panic can
    /// leave without its record does not permanently shrink the bound.
    #[test]
    fn a_panicked_producer_leaves_the_queue_usable_and_exactly_accounted() {
        let event = event_of_known_size();
        let exact = serde_json::to_vec(&event).expect("the probe event serializes");
        let stray = exact.len();
        let queue = Arc::new(DeliveryQueue::new(exact.len() * 4, exact.len()));

        let poisoner = Arc::clone(&queue);
        let outcome = thread::spawn(move || {
            let mut state = poisoner.state.lock().expect("the first lock is clean");
            // A charge applied without the record it was charged for: the one
            // inconsistency a panic between the two updates can leave behind.
            state.queued_bytes += stray;
            panic!("a producer panics while holding the queue lock");
        })
        .join();
        assert!(outcome.is_err(), "the producer panicked under the lock");
        assert!(queue.state.is_poisoned(), "which poisons the queue lock");

        let SerializedEvent::Bounded { framed } = serialize_event(&event, exact.len()) else {
            panic!("the probe event is within the cap");
        };
        let charge = framed.len();
        assert!(
            matches!(
                queue.try_push(framed, Duration::ZERO),
                QueueAdmission::Accepted
            ),
            "a later emit still enters the queue"
        );
        assert!(!queue.state.is_poisoned(), "and the lock is usable again");
        assert_eq!(
            queue.state.lock().expect("uncontended").queued_bytes,
            charge,
            "the queue accounts for the record it holds and nothing besides"
        );
        assert!(queue.pop().is_some(), "and the worker still drains it");
    }

    /// The promise the validator makes to an author who sets `max_event_bytes`
    /// equal to `queue_bytes`: an event at the cap enters an empty queue. It
    /// held until the record separator was moved out of the cap and into the
    /// queued buffer, which put an at-cap event one byte over the queue and
    /// dropped every one of them on a policy the validator accepts.
    #[test]
    fn an_event_at_the_cap_enters_an_empty_queue() {
        let event = event_of_known_size();
        let exact = serde_json::to_vec(&event).expect("the probe event serializes");
        let config = LineageDeliveryConfig::new(exact.len(), exact.len(), Duration::from_millis(1))
            .expect("equal caps are a legal policy");
        let queue = DeliveryQueue::new(config.queue_bytes, config.max_event_bytes);

        let SerializedEvent::Bounded { framed } = serialize_event(&event, config.max_event_bytes)
        else {
            panic!("an event at the cap is within the cap");
        };
        assert_eq!(framed.len(), exact.len() + 1, "one record is one write");
        assert_eq!(framed.last(), Some(&b'\n'), "and it carries its separator");
        assert!(
            matches!(
                queue.try_push(framed, Duration::ZERO),
                QueueAdmission::Accepted
            ),
            "and an empty queue of the same size has room for it"
        );
    }

    /// The cap bounds the event, not the framing. An operator who measures a
    /// serialized event and sets `max_event_bytes` to exactly that number is
    /// asking for that event to be admitted; charging the record separator
    /// against the same budget dropped it instead, and because a lineage write
    /// failure is reported rather than propagated, the run exited zero with
    /// its terminal event missing and the catalog left showing it as running.
    #[test]
    fn an_event_measured_at_the_cap_is_admitted() {
        let event = event_of_known_size();
        let exact = serde_json::to_vec(&event).expect("the probe event serializes");

        match serialize_event(&event, exact.len()) {
            SerializedEvent::Bounded { framed } => {
                assert_eq!(
                    framed.len(),
                    exact.len() + 1,
                    "the cap bounds the event the operator authored, not the framing"
                );
            }
            _ => panic!("an event whose size equals the cap is within the cap"),
        }

        assert!(
            matches!(
                serialize_event(&event, exact.len().saturating_sub(1)),
                SerializedEvent::TooLarge
            ),
            "one byte under the measured size is still over the cap"
        );
    }
}
