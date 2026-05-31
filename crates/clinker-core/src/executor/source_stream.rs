//! Per-source live ingest channel.
//!
//! [`TokioSourceStream`] wraps a bounded `tokio::sync::mpsc::Sender` so a
//! Source-reader task can push records and document-boundary
//! punctuations into the executor's dispatch loop through the paired
//! `Receiver`. Channel capacity is bounded so producers `.await` (or
//! `blocking_send`) when the consumer falls behind, supplying
//! back-pressure end-to-end without an intermediate spill tier.
//!
//! Payload is [`StreamEvent`]: either a `(Record, u64)` body event or
//! a [`Punctuation`] marking a document boundary. Source ingest emits
//! one `DocumentOpen` before the first body record of each source
//! file and one `DocumentClose` after the last.

use std::sync::Arc;

use clinker_record::Record;

use crate::executor::stream_event::{Punctuation, StreamEvent};

/// Error surface for [`TokioSourceStream`] sends.
#[derive(Debug)]
pub(crate) enum SourceStreamError {
    /// Consumer dropped the receiver before this push completed.
    Closed,
}

impl std::fmt::Display for SourceStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Closed => write!(f, "source stream closed: consumer dropped receiver"),
        }
    }
}

impl std::error::Error for SourceStreamError {}

/// Tokio-mpsc-backed live ingest channel for one Source.
///
/// Capacity is bounded; producers block (sync `blocking_send` from inside
/// `tokio::task::spawn_blocking`) when the channel is full, providing
/// back-pressure to the upstream reader. The consumer is a paired
/// `tokio::sync::mpsc::Receiver` returned by [`Self::new`] and consumed
/// via `recv().await` by the dispatch loop's Source arm.
pub(crate) struct TokioSourceStream {
    tx: tokio::sync::mpsc::Sender<StreamEvent>,
    /// Shared with the registered `SourceConsumer` wrapper. Each
    /// `blocking_push` updates `handle.bytes` from the current channel
    /// queue depth times the most-recent record's estimated heap size,
    /// so the arbitrator's pull-mode `current_usage` reads the
    /// channel's in-flight memory at every policy poll. Punctuation
    /// sends carry no record bytes and leave the handle untouched.
    consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl TokioSourceStream {
    /// Default channel capacity. Matches the cooperative-yield cadence
    /// chosen for the async dispatch loop: one batch of `yield_now`
    /// calls in Transform/Route corresponds to roughly one channel
    /// drain.
    pub(crate) const DEFAULT_CAPACITY: usize = 1024;

    /// Create a new stream + paired receiver. The receiver is what the
    /// dispatch loop's Source arm consumes via `recv().await`. The
    /// `consumer_handle` is shared with the pipeline-scoped
    /// arbitrator's `SourceConsumer` wrapper.
    pub(crate) fn new(
        capacity: usize,
        consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
    ) -> (Self, tokio::sync::mpsc::Receiver<StreamEvent>) {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        (
            Self {
                tx,
                consumer_handle,
            },
            rx,
        )
    }

    /// Push a body record from a synchronous worker (typically the
    /// closure inside `tokio::task::spawn_blocking` driving a sync
    /// format reader). Blocks the calling thread when the channel is
    /// at capacity, preserving the bounded-channel back-pressure
    /// semantics without requiring the caller to be inside an
    /// `async fn`.
    pub(crate) fn blocking_push(
        &mut self,
        record: Record,
        row_num: u64,
    ) -> Result<(), SourceStreamError> {
        // Block here if the arbitrator has paused this consumer (e.g.
        // `BackPressurePreferred` policy elected this Source as the
        // pause victim). The fast path is lock-free; the slow path
        // parks the calling thread on a `Condvar` until `resume()`
        // notifies. Routed through the shared `ConsumerHandle` so
        // pause/resume from the arbitrator side and the producer-
        // side wait participate in the same primitive.
        self.consumer_handle.wait_while_paused();
        // Sample the record's estimated heap size before moving it
        // into the channel. The handle byte estimate uses this as a
        // per-record proxy multiplied by the post-send queue depth —
        // accurate for steady-state Source flows where records are
        // roughly uniform, conservative-over-the-channel-lifetime
        // when sizes drift (the arbitrator polls often enough that
        // staleness is bounded to a few records).
        let record_bytes = (std::mem::size_of::<Record>() + record.estimated_heap_size()) as u64;
        self.tx
            .blocking_send(StreamEvent::record(record, row_num))
            .map_err(|_| SourceStreamError::Closed)?;
        // `max_capacity - capacity` is the number of events sitting
        // in the channel buffer waiting for the consumer. The product
        // approximates the channel's in-flight memory footprint and
        // is what the arbitrator's `current_usage` reports.
        let max_cap = self.tx.max_capacity() as u64;
        let remaining = self.tx.capacity() as u64;
        let queued = max_cap.saturating_sub(remaining);
        self.consumer_handle
            .set_bytes(queued.saturating_mul(record_bytes));
        Ok(())
    }

    /// Push a document-boundary punctuation. One `DocumentOpen` and
    /// one `DocumentClose` per file; the executor's dispatch loop
    /// forwards them through downstream stages with operator-specific
    /// behavior (Aggregate / Output flush; Merge dedupes; Transform /
    /// Route pass through). Punctuations carry no record bytes, so the
    /// `ConsumerHandle` byte estimate is left untouched.
    pub(crate) fn blocking_push_punctuation(
        &mut self,
        punct: Punctuation,
    ) -> Result<(), SourceStreamError> {
        self.tx
            .blocking_send(StreamEvent::punctuation(punct))
            .map_err(|_| SourceStreamError::Closed)
    }
}

/// `MemoryConsumer` wrapper for a `TokioSourceStream` ingest channel.
/// Holds an `Arc<ConsumerHandle>` shared with the source ingest task:
/// the task updates `handle.bytes` from the bounded `mpsc` queue
/// depth × estimated per-record bytes at each batch boundary.
///
/// Sources do not spill: `try_spill` returns `Ok(0)` and the
/// arbitrator's policy is expected to choose `pause` instead via
/// `BackPressurePreferred`. `spill_priority = i32::MAX` so the
/// `Priority` fallback ranks Sources last among the spill candidates
/// when no back-pressureable consumer is available.
/// `can_back_pressure = true`; `pause` / `resume` forward to the
/// handle's pause flag, which the ingest task reads at every
/// `blocking_send` boundary.
pub struct SourceConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl SourceConsumer {
    pub fn new(handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl crate::pipeline::memory::MemoryConsumer for SourceConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        i32::MAX
    }

    fn try_spill(
        &mut self,
        _target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        Ok(0)
    }

    fn can_back_pressure(&self) -> bool {
        true
    }

    fn pause(&mut self) {
        self.handle.pause();
    }

    fn resume(&mut self) {
        self.handle.resume();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::{ConsumerHandle, MemoryConsumer};

    #[test]
    fn source_consumer_reports_handle_bytes_and_never_spills() {
        let handle = ConsumerHandle::new();
        handle.set_bytes(16 * 1024);
        let mut consumer = SourceConsumer::new(handle.clone());
        assert_eq!(consumer.current_usage(), 16 * 1024);
        // Sources don't spill: try_spill always reports zero freed.
        assert_eq!(consumer.try_spill(u64::MAX).unwrap(), 0);
        assert!(!handle.take_spill_request());
    }

    #[test]
    fn source_consumer_is_back_pressureable_and_routes_pause_to_handle() {
        let handle = ConsumerHandle::new();
        let mut consumer = SourceConsumer::new(handle.clone());
        assert!(consumer.can_back_pressure());
        // Source spill priority sits above the integer range used by
        // other consumers; the Priority policy ranks Sources last,
        // matching BackPressurePreferred's prefer-pause posture.
        assert_eq!(consumer.spill_priority(), i32::MAX);
        consumer.pause();
        assert!(handle.is_paused());
        consumer.resume();
        assert!(!handle.is_paused());
    }
}
