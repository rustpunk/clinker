//! Independently bounded, non-blocking OpenLineage event delivery.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, TryLockError, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use clinker_plan::config::{ObservabilityDropPolicy, ResolvedLineageDeliveryPolicy};

use crate::RunEvent;

/// How many times a finite export re-attempts the queue lock before it treats
/// contention as a drop. Each attempt yields, and the lock is only ever held
/// across a `VecDeque` push or pop, so exhausting this bound means something
/// other than ordinary contention is wrong.
const PRODUCER_LOCK_RETRIES: usize = 64;

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
/// The charge travels with the record because the two sides of the accounting
/// must agree and only one of them could otherwise derive it: the buffer
/// carries its record separator, so charging the event on the way in and
/// crediting the buffer on the way out drifted the accounted total down by a
/// byte per record until it reached zero and the queue admitted without bound.
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
            capacity_bytes,
            state: Mutex::new(QueueState {
                events: VecDeque::with_capacity(reserved_events),
                queued_bytes: 0,
                closed: false,
            }),
            ready: Condvar::new(),
        }
    }

    /// Admit one framed record without waiting on capacity or the sink,
    /// charged as `event_bytes`.
    ///
    /// `lock_retries` bounds how many times the producer re-attempts the
    /// queue lock itself. The lock is held only for the length of one
    /// `VecDeque` push or pop — plus the instant between the worker taking it
    /// and `Condvar::wait` releasing it — so contention there says nothing
    /// about capacity, the cap, or the sink. Capacity and shutdown outcomes
    /// are never retried.
    ///
    /// The buffer carries its record separator so the sink writes it in one
    /// call, but the separator is framing this format adds rather than
    /// something the operator authored, and the budget it is admitted against
    /// is the same number that bounds the event. The slack is one byte per
    /// queued record, bounded by the reservation, and the charge is stored
    /// with the record so releasing it credits back exactly what it took.
    fn try_push(
        &self,
        event: Box<[u8]>,
        event_bytes: usize,
        lock_retries: usize,
    ) -> QueueAdmission {
        let mut remaining = lock_retries;
        let mut state = loop {
            match self.state.try_lock() {
                Ok(state) => break state,
                Err(TryLockError::WouldBlock) if remaining > 0 => {
                    remaining -= 1;
                    thread::yield_now();
                }
                Err(TryLockError::WouldBlock) => return QueueAdmission::Busy,
                Err(TryLockError::Poisoned(_)) => return QueueAdmission::Closed,
            }
        };
        if state.closed {
            return QueueAdmission::Closed;
        }
        if event_bytes > self.capacity_bytes.saturating_sub(state.queued_bytes) {
            return QueueAdmission::Full;
        }
        state.queued_bytes += event_bytes;
        state.events.push_back(QueuedRecord {
            bytes: event,
            charge: event_bytes,
        });
        drop(state);
        self.ready.notify_one();
        QueueAdmission::Accepted
    }

    fn pop(&self) -> Option<Box<[u8]>> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        loop {
            if let Some(record) = state.events.pop_front() {
                state.queued_bytes = state.queued_bytes.saturating_sub(record.charge);
                return Some(record.bytes);
            }
            if state.closed {
                return None;
            }
            state = self
                .ready
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
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
        Self {
            bytes: Vec::with_capacity(limit.min(64 * 1024)),
            limit,
        }
    }

    /// The bounded event followed by its record separator, and the size of
    /// the event alone.
    ///
    /// The separator is in the buffer so one record is one write: the sink can
    /// share a handle with the run's own output, and two writes take that
    /// handle twice, letting an unrelated line land between an event and its
    /// newline. It is not in the size, because `max_event_bytes` bounds the
    /// event an operator authored and the queue admits against that same
    /// number -- charging the framing to either made an event measured at
    /// exactly the cap fail, once at the cap itself and once at the door of an
    /// empty queue the validator sized to hold it.
    fn finish(mut self) -> (Box<[u8]>, usize) {
        let event_bytes = self.bytes.len();
        self.bytes.push(b'\n');
        (self.bytes.into_boxed_slice(), event_bytes)
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
    Bounded {
        framed: Box<[u8]>,
        event_bytes: usize,
    },
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
    let (framed, event_bytes) = buffer.finish();
    SerializedEvent::Bounded {
        framed,
        event_bytes,
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
        self.emit(event, PRODUCER_LOCK_RETRIES)
    }

    fn emit(&self, event: &RunEvent, lock_retries: usize) -> LineageAdmission {
        let (bytes, event_bytes) = match serialize_event(event, self.config.max_event_bytes) {
            SerializedEvent::Bounded {
                framed,
                event_bytes,
            } => (framed, event_bytes),
            SerializedEvent::TooLarge => {
                Counters::increment(&self.counters.dropped);
                return LineageAdmission::DroppedEventTooLarge;
            }
            SerializedEvent::EncodingFailed => {
                Counters::increment(&self.counters.dropped);
                return LineageAdmission::DroppedEncodingFailed;
            }
        };
        match self.queue.try_push(bytes, event_bytes, lock_retries) {
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
            let SerializedEvent::Bounded {
                framed,
                event_bytes,
            } = serialize_event(&event, exact.len())
            else {
                panic!("the probe event is within the cap");
            };
            assert!(
                matches!(
                    queue.try_push(framed, event_bytes, 0),
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

        let SerializedEvent::Bounded {
            framed,
            event_bytes,
        } = serialize_event(&event, config.max_event_bytes)
        else {
            panic!("an event at the cap is within the cap");
        };
        assert_eq!(
            event_bytes,
            exact.len(),
            "the event is charged, not its framing"
        );
        assert_eq!(framed.last(), Some(&b'\n'), "one record is one write");
        assert!(
            matches!(
                queue.try_push(framed, event_bytes, 0),
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
            SerializedEvent::Bounded { event_bytes, .. } => {
                assert_eq!(
                    event_bytes,
                    exact.len(),
                    "the cap bounds the event, and the framing is charged to neither budget"
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
