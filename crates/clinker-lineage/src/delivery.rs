//! Independently bounded, non-blocking OpenLineage event delivery.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, TryLockError, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use clinker_plan::config::{ObservabilityDropPolicy, ResolvedLineageDeliveryPolicy};

use crate::RunEvent;

/// How long a producer keeps trying for the queue lock before calling it
/// contention. Orders of magnitude above the hold time, which is one
/// `VecDeque` operation, so it only matters when the scheduler is against us.
const PRODUCER_LOCK_PATIENCE: Duration = Duration::from_millis(50);

/// The share of the flush deadline the worker may spend *starting* records.
///
/// The deadline bounds the whole flush, and a worker that is still inside a
/// record when the finisher gives up is detached mid-write: the bytes already
/// accepted stay on the destination, so the published NDJSON ends in a record
/// that never closes. A reader of a short file can stop at the last event; a
/// reader of a truncated one hits an unparseable line.
///
/// Splitting the deadline is what keeps the two ends from meeting on the same
/// instant. The worker stops taking new records off the queue once this share
/// has passed, so the only record that can be in flight at the deadline is one
/// begun before it, and the remaining share is time for that record to close
/// while the finisher is still listening. The whole flush therefore stays
/// inside exactly the deadline the operator configured — nothing is added to
/// the run's worst case — and the ordinary cost is bounded by the queue's own
/// depth: a run offers a START and one terminal event, so at most one record is
/// given up, and only when the destination is already too slow to have finished
/// in half the time it was given.
const DRAIN_SHARE_OF_DEADLINE: u32 = 2;

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
    records_complete: bool,
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

    /// Whether the bytes the destination holds end on a record boundary.
    ///
    /// A short file and a truncated one are different artifacts: the first is
    /// missing its tail, the second ends in a record that never closes and a
    /// conformant NDJSON reader fails on it. The counters alone cannot tell
    /// them apart — a run that gave up on a slow destination reports the same
    /// accepted total either way — so the completeness of the *file* is
    /// reported separately from the completeness of the *counts*.
    ///
    /// True on every normal shutdown. False only when a write was still
    /// outstanding, or had already been partly accepted, when this outcome was
    /// taken: the destination may then hold the opening bytes of a record whose
    /// remainder was never written. Conservative in that one direction — a
    /// write call that never returns is counted as incomplete, because a
    /// blocked write may already have transferred part of its buffer.
    pub const fn records_complete(self) -> bool {
        self.records_complete
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

    fn outcome(
        &self,
        terminal: LineageDeliveryTerminal,
        records_complete: bool,
    ) -> LineageDeliveryOutcome {
        LineageDeliveryOutcome {
            accepted: self.accepted.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            full: self.full.load(Ordering::Relaxed),
            terminal,
            records_complete,
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
    /// When producer admission closed — the instant the flush deadline is
    /// measured from, so the worker and the finisher bound the same window from
    /// the same origin rather than from two clocks started at two moments.
    closed_at: Option<Instant>,
    /// Set when the finisher has stopped waiting. The worker takes no further
    /// record: whatever it writes after that is not counted in the outcome the
    /// run already reported, and every record it adds is one more chance for
    /// the process to exit inside a write.
    abandoned: bool,
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
                closed_at: None,
                abandoned: false,
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

    /// Take the next record, or say why there is not one.
    ///
    /// `drain_budget` is the share of the flush deadline the worker may spend
    /// starting records, measured from the instant admission closed. Checking
    /// it here rather than after the write is the whole point: a record is
    /// either begun inside the window and carried to its last byte, or never
    /// begun at all, so the destination is only ever left between records.
    fn pop(&self, drain_budget: Duration) -> Popped {
        let mut state = self.lock_state();
        loop {
            if state.abandoned {
                return Popped::OutOfTime;
            }
            if state
                .closed_at
                .is_some_and(|closed_at| closed_at.elapsed() >= drain_budget)
            {
                return Popped::OutOfTime;
            }
            if let Some(record) = state.events.pop_front() {
                state.queued_bytes = state.queued_bytes.saturating_sub(record.charge);
                return Popped::Record(record.bytes);
            }
            if state.closed {
                return Popped::Drained;
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
        state.closed_at.get_or_insert_with(Instant::now);
        drop(state);
        self.ready.notify_all();
    }

    /// Stop the worker from beginning any further record.
    fn abandon(&self) {
        let mut state = self.lock_state();
        state.abandoned = true;
        drop(state);
        self.ready.notify_all();
    }
}

/// What one drain attempt found.
enum Popped {
    Record(Box<[u8]>),
    /// Admission is closed and every accepted record has been taken.
    Drained,
    /// The worker's share of the flush deadline is spent, or the finisher has
    /// stopped waiting. Whatever is still queued is not delivered.
    OutOfTime,
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

/// Write one framed record, tracking whether the destination is left inside it.
///
/// `mid_record` is set while a write call is outstanding and cleared only once
/// the record's last byte has been accepted, so the finisher can report whether
/// the bytes on the destination end on a record boundary. A call that returns an
/// error transferred nothing, so the flag then reflects what earlier calls of
/// this same record had already put there; a call that never returns is counted
/// as mid-record, because a blocked write may already have transferred part of
/// its buffer.
///
/// This is `write_all` with that accounting added — the loop cannot be avoided
/// by writing the whole record in one call, since a destination is free to
/// accept a record in pieces and a pipe with a slow reader routinely does.
fn write_record<W: Write>(sink: &mut W, record: &[u8], mid_record: &AtomicBool) -> io::Result<()> {
    let mut accepted = 0;
    while accepted < record.len() {
        mid_record.store(true, Ordering::Release);
        match sink.write(&record[accepted..]) {
            Ok(0) => {
                mid_record.store(accepted > 0, Ordering::Release);
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            Ok(written) => accepted += written,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                mid_record.store(accepted > 0, Ordering::Release);
                return Err(error);
            }
        }
        mid_record.store(accepted > 0 && accepted < record.len(), Ordering::Release);
    }
    Ok(())
}

fn run_worker<W: Write>(
    queue: &DeliveryQueue,
    sink: &mut W,
    drain_budget: Duration,
    mid_record: &AtomicBool,
) -> LineageDeliveryTerminal {
    let out_of_time = loop {
        match queue.pop(drain_budget) {
            Popped::Record(event) => {
                if let Err(error) = write_record(sink, &event, mid_record) {
                    return LineageDeliveryTerminal::WriteFailed(error.kind());
                }
            }
            Popped::Drained => break false,
            Popped::OutOfTime => break true,
        }
    };
    // Flushed on the out-of-time path too: everything buffered is a whole
    // record, so getting it out is what turns "the file stops early" into "the
    // file stops early at a record boundary". If the flush is what the slow
    // destination is slow at, the finisher's remaining share expires and the
    // file is short by exactly the buffered records, still never mid-record.
    match sink.flush() {
        Ok(()) if out_of_time => LineageDeliveryTerminal::DeadlineExceeded,
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
    /// Set by the worker while a record's bytes are only partly on the
    /// destination. Read once, when the outcome is taken.
    mid_record: Arc<AtomicBool>,
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
        let mid_record = Arc::new(AtomicBool::new(false));
        let worker_mid_record = Arc::clone(&mid_record);
        let drain_budget = config.flush_deadline / DRAIN_SHARE_OF_DEADLINE;
        let (outcome_tx, outcome_rx) = mpsc::channel();
        let worker = thread::Builder::new()
            .name("clinker-lineage-export".to_owned())
            .spawn(move || {
                let terminal =
                    run_worker(&worker_queue, &mut sink, drain_budget, &worker_mid_record);
                worker_queue.close();
                let _ = outcome_tx.send(terminal);
            })?;
        Ok(Self {
            config,
            queue,
            counters,
            outcome_rx,
            worker: Some(worker),
            mid_record,
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
    ///
    /// The wait is never extended: the worker gives up taking new records after
    /// [`DRAIN_SHARE_OF_DEADLINE`], so on a destination that is merely slow it
    /// reports back inside the deadline having stopped between records, and the
    /// file this run publishes is short rather than truncated. A destination
    /// that has stopped accepting bytes altogether cannot be waited on, so the
    /// handle is detached and [`LineageDeliveryOutcome::records_complete`]
    /// carries whether it was left inside a record.
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
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // The worker is inside a write that has not returned. It keeps
                // the handle it owns, but it takes nothing further off the
                // queue: a record begun after this point would be written by a
                // thread the process is already free to exit underneath, and
                // its opening bytes would be the last thing in the file.
                self.queue.abandon();
                LineageDeliveryTerminal::DeadlineExceeded
            }
            // The worker sends its outcome before it returns, so a closed
            // channel means it did not return — it unwound. Calling that a
            // shutdown would report a dead exporter as a clean flush and leave
            // whatever it had not written unaccounted for.
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                LineageDeliveryTerminal::WriteFailed(io::ErrorKind::Other)
            }
        };
        self.counters
            .outcome(terminal, !self.mid_record.load(Ordering::Acquire))
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
                matches!(queue.pop(Duration::MAX), Popped::Record(_)),
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
        assert!(
            matches!(queue.pop(Duration::MAX), Popped::Record(_)),
            "and the worker still drains it"
        );
    }

    /// A destination that takes a record in small pieces, slowly — a pipe with
    /// a reader that is behind, which is exactly the case the flush deadline
    /// exists for.
    struct DribblingSink {
        published: Arc<Mutex<Vec<u8>>>,
        chunk: usize,
        per_chunk: Duration,
    }

    impl Write for DribblingSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            thread::sleep(self.per_chunk);
            let taken = bytes.len().min(self.chunk);
            self.published
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(&bytes[..taken]);
            Ok(taken)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A run that gives up on a slow destination publishes a file that is
    /// short, not one that is corrupt.
    ///
    /// The deadline used to expire with the worker in the middle of a
    /// `write_all`, and the handle was then detached rather than joined, so the
    /// bytes the destination had already taken were the opening of a record
    /// whose remainder no one would write. A consumer reading the published
    /// NDJSON did not find fewer events; it found an unparseable last line. The
    /// worker now stops taking records once its share of the deadline is spent,
    /// so the only record it can be inside at the deadline is one begun before
    /// it, and that one is carried to its last byte while the finisher is still
    /// listening.
    #[test]
    fn a_file_cut_short_by_the_deadline_ends_on_a_record_boundary() {
        let event = event_of_known_size();
        let exact = serde_json::to_vec(&event).expect("the probe event serializes");
        let published = Arc::new(Mutex::new(Vec::new()));
        // Well under the drain share, so the record in flight when the share
        // ends closes with the whole second half of the deadline to spare.
        let per_record = Duration::from_millis(8);
        let chunk = exact.len().div_ceil(16);
        let config = LineageDeliveryConfig::new(
            (exact.len() + 1) * 64,
            exact.len(),
            Duration::from_millis(400),
        )
        .expect("legal delivery limits");
        let delivery = LineageDelivery::start(
            config,
            DribblingSink {
                published: Arc::clone(&published),
                chunk,
                per_chunk: per_record / 16,
            },
        )
        .expect("the worker starts");

        // More events than the destination can take in the whole deadline, so
        // the run is certain to give up on it.
        for _ in 0..64 {
            let _ = delivery.try_emit(&event);
        }
        let outcome = delivery.finish();

        assert_eq!(
            outcome.terminal(),
            LineageDeliveryTerminal::DeadlineExceeded,
            "the destination cannot take every event in the time it was given"
        );
        let published = published
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        assert!(
            !published.is_empty(),
            "the destination took what it could before the deadline"
        );
        assert_eq!(
            published.last(),
            Some(&b'\n'),
            "the published bytes end where a record ends"
        );
        for (index, line) in published.split(|byte| *byte == b'\n').enumerate() {
            if line.is_empty() {
                continue;
            }
            serde_json::from_slice::<serde_json::Value>(line).unwrap_or_else(|error| {
                panic!("record {index} is a complete JSON object: {error}")
            });
        }
        assert!(
            outcome.records_complete(),
            "and the outcome says so, so a consumer can tell this from a truncated file"
        );
        assert!(
            outcome.accepted() >= 1,
            "the events the destination did take are still counted"
        );
    }

    /// A destination that stops accepting bytes altogether cannot be waited
    /// on, and the run must not hang for one. The file may then end inside a
    /// record — and the outcome has to say so, because the counters cannot:
    /// they report what entered the queue, not what reached the file whole.
    #[test]
    fn a_destination_that_stops_mid_record_is_reported_as_incomplete() {
        let event = event_of_known_size();
        let exact = serde_json::to_vec(&event).expect("the probe event serializes");
        let config = LineageDeliveryConfig::new(
            (exact.len() + 1) * 4,
            exact.len(),
            Duration::from_millis(60),
        )
        .expect("legal delivery limits");

        struct StuckAfterFirstChunk {
            taken: bool,
        }

        impl Write for StuckAfterFirstChunk {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if self.taken {
                    thread::sleep(Duration::from_secs(30));
                    return Ok(bytes.len());
                }
                self.taken = true;
                Ok(bytes.len().min(8))
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let delivery = LineageDelivery::start(config, StuckAfterFirstChunk { taken: false })
            .expect("the worker starts");
        assert_eq!(delivery.try_emit(&event), LineageAdmission::Accepted);
        let outcome = delivery.finish();

        assert_eq!(
            outcome.terminal(),
            LineageDeliveryTerminal::DeadlineExceeded
        );
        assert!(
            !outcome.records_complete(),
            "the destination holds the opening of a record nothing will finish"
        );
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
