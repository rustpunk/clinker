//! Independently bounded, non-blocking OpenLineage event delivery.

use std::collections::VecDeque;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, TryLockError, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use clinker_plan::config::{ObservabilityDropPolicy, ResolvedLineageDeliveryPolicy};

use crate::RunEvent;

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

struct QueueState {
    events: VecDeque<Box<[u8]>>,
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

    fn try_push(&self, event: Box<[u8]>) -> QueueAdmission {
        let event_bytes = event.len();
        let mut state = match self.state.try_lock() {
            Ok(state) => state,
            Err(TryLockError::WouldBlock) => return QueueAdmission::Busy,
            Err(TryLockError::Poisoned(_)) => return QueueAdmission::Closed,
        };
        if state.closed {
            return QueueAdmission::Closed;
        }
        if event_bytes > self.capacity_bytes.saturating_sub(state.queued_bytes) {
            return QueueAdmission::Full;
        }
        state.queued_bytes += event_bytes;
        state.events.push_back(event);
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
            if let Some(event) = state.events.pop_front() {
                state.queued_bytes = state.queued_bytes.saturating_sub(event.len());
                return Some(event);
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

    fn finish(self) -> Box<[u8]> {
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

fn serialize_event(event: &RunEvent, limit: usize) -> Option<Box<[u8]>> {
    let mut buffer = CappedEventBuffer::new(limit);
    serde_json::to_writer(&mut buffer, event).ok()?;
    buffer.write_all(b"\n").ok()?;
    Some(buffer.finish())
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
    pub fn start<W>(config: LineageDeliveryConfig, mut sink: W) -> Self
    where
        W: Write + Send + 'static,
    {
        let queue = Arc::new(DeliveryQueue::new(
            config.queue_bytes,
            config.max_event_bytes,
        ));
        let counters = Arc::new(Counters::default());
        let worker_queue = Arc::clone(&queue);
        let (outcome_tx, outcome_rx) = mpsc::channel();
        let worker = thread::spawn(move || {
            let terminal = run_worker(&worker_queue, &mut sink);
            worker_queue.close();
            let _ = outcome_tx.send(terminal);
        });
        Self {
            config,
            queue,
            counters,
            outcome_rx,
            worker: Some(worker),
        }
    }

    /// Serialize a complete event into an owned capped buffer, then attempt
    /// immediate drop-newest admission without waiting on capacity or the sink.
    pub fn try_emit(&self, event: &RunEvent) -> LineageAdmission {
        let Some(bytes) = serialize_event(event, self.config.max_event_bytes) else {
            Counters::increment(&self.counters.dropped);
            return LineageAdmission::DroppedEventTooLarge;
        };
        match self.queue.try_push(bytes) {
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
            Err(mpsc::RecvTimeoutError::Disconnected) => LineageDeliveryTerminal::Shutdown,
        };
        self.counters.outcome(terminal)
    }
}

impl Drop for LineageDelivery {
    fn drop(&mut self) {
        self.queue.close();
    }
}
