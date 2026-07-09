//! Per-source live ingest channel.
//!
//! [`SourceIngestChannel`] wraps a bounded `crossbeam_channel::Sender`
//! so a Source-reader thread can push records and document-boundary
//! punctuations into the executor's dispatch loop through the paired
//! `Receiver`. Channel capacity is bounded so the producer's `send`
//! blocks the calling OS thread when the consumer falls behind,
//! supplying back-pressure end-to-end without an intermediate spill
//! tier.
//!
//! Payload is [`StreamEvent`]: either a `(Record, u64)` body event or
//! a [`Punctuation`] marking a document boundary. Source ingest emits
//! one `DocumentOpen` before the first body record of each source
//! file and one `DocumentClose` after the last.

use std::sync::Arc;

use clinker_record::Record;

use crate::executor::stream_event::{Punctuation, StreamEvent};

/// Error surface for [`SourceIngestChannel`] sends.
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

/// Crossbeam-bounded live ingest channel for one Source.
///
/// Capacity is bounded; the producer's `send` blocks the calling OS
/// thread when the channel is full, providing back-pressure to the
/// upstream reader. The consumer is a paired `crossbeam_channel::Receiver`
/// returned by [`Self::new`] and consumed via `recv` by the dispatch
/// loop's Source arm.
pub(crate) struct SourceIngestChannel {
    tx: crossbeam_channel::Sender<StreamEvent>,
    /// Shared with the registered `SourceConsumer` wrapper. Each
    /// `push` updates `handle.bytes` from the current channel queue
    /// depth times a smoothed per-record byte estimate (see
    /// `record_bytes_ewma`), so the arbitrator's pull-mode
    /// `current_usage` reads the channel's in-flight memory at every
    /// policy poll. Punctuation sends carry no record bytes and leave
    /// the handle untouched.
    consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
    /// Exponentially weighted moving average (alpha = 1/8) of recent
    /// per-record heap sizes, in bytes. Updated on each body push and
    /// multiplied by the post-send queue depth to mirror the channel's
    /// in-flight footprint into `consumer_handle`. Smoothing the
    /// per-record cost across recent samples keeps the mirrored estimate
    /// stable when record sizes drift (variable-width strings, optional
    /// payload columns, mixed `Value` variants), which sharpens
    /// pause-victim ranking. This is ranking telemetry only — the abort
    /// gate trips on real OS RSS, never on this estimate. `0` means
    /// "unseeded": the first push adopts its own sample as the baseline
    /// rather than climbing from zero over several records.
    record_bytes_ewma: u64,
}

/// Folds one per-record byte `sample` into an exponentially weighted
/// moving average with alpha = 1/8.
///
/// `prev == 0` is the unseeded sentinel: the first sample becomes the
/// baseline directly (a real record can never be zero bytes because
/// `size_of::<Record>()` is a nonzero constant), avoiding the warm-up
/// where the estimate would otherwise spend several records climbing
/// from zero. Otherwise the average moves toward `sample` by one eighth
/// of the gap. The `/ 8` decay is a bit shift and the subtraction is
/// ordered to stay non-negative, so the update is allocation-free,
/// float-free, and underflow-safe — appropriate for the hot send path.
const fn ewma_step(prev: u64, sample: u64) -> u64 {
    if prev == 0 {
        sample
    } else if sample >= prev {
        prev + (sample - prev) / 8
    } else {
        prev - (prev - sample) / 8
    }
}

impl SourceIngestChannel {
    /// Default channel capacity. Bounds the in-flight depth between the
    /// ingest thread and the dispatch loop's Source arm; the producer's
    /// `send` blocks once this many events are buffered, so the value
    /// paces back-pressure.
    pub(crate) const DEFAULT_CAPACITY: usize = 1024;

    /// Create a new channel + paired receiver. The receiver is what the
    /// dispatch loop's Source arm consumes via `recv`. The
    /// `consumer_handle` is shared with the pipeline-scoped
    /// arbitrator's `SourceConsumer` wrapper.
    pub(crate) fn new(
        capacity: usize,
        consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
    ) -> (Self, crossbeam_channel::Receiver<StreamEvent>) {
        let (tx, rx) = crossbeam_channel::bounded(capacity);
        (
            Self {
                tx,
                consumer_handle,
                record_bytes_ewma: 0,
            },
            rx,
        )
    }

    /// Push a body record from the Source ingest thread driving a sync
    /// format reader. Blocks the calling thread when the channel is at
    /// capacity, preserving the bounded-channel back-pressure
    /// semantics.
    pub(crate) fn push(&mut self, record: Record, row_num: u64) -> Result<(), SourceStreamError> {
        // Block here if the arbitrator has paused this consumer (e.g.
        // `BackPressurePreferred` policy elected this Source as the
        // pause victim). The fast path is lock-free; the slow path
        // parks the calling thread on a `Condvar` until `resume()`
        // notifies. Routed through the shared `ConsumerHandle` so
        // pause/resume from the arbitrator side and the producer-
        // side wait participate in the same primitive.
        self.consumer_handle.wait_while_paused();
        // Sample this record's estimated heap size before moving it
        // into the channel, then fold it into a per-stream EWMA. The
        // smoothed value (not the raw last sample) multiplies the
        // post-send queue depth, so the mirrored estimate stays stable
        // when record sizes drift instead of swinging with whichever
        // record was pushed most recently — sharpening pause-victim
        // ranking. The accumulator is plain per-task state: `&mut self`
        // means no synchronization is needed. Ranking telemetry only;
        // the abort gate trips on real OS RSS, never on this estimate.
        let sample = (std::mem::size_of::<Record>() + record.estimated_heap_size()) as u64;
        self.record_bytes_ewma = ewma_step(self.record_bytes_ewma, sample);
        let record_bytes = self.record_bytes_ewma;
        self.tx
            .send(StreamEvent::record(record, row_num))
            .map_err(|_| SourceStreamError::Closed)?;
        // `Sender::len()` is the number of events sitting in the channel
        // buffer waiting for the consumer — the in-flight queue depth. The
        // product approximates the channel's in-flight memory footprint
        // and is what the arbitrator's `current_usage` reports.
        let queued = self.tx.len() as u64;
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
    pub(crate) fn push_punctuation(&mut self, punct: Punctuation) -> Result<(), SourceStreamError> {
        self.tx
            .send(StreamEvent::punctuation(punct))
            .map_err(|_| SourceStreamError::Closed)
    }
}

/// `MemoryConsumer` wrapper for a `SourceIngestChannel`.
/// Holds an `Arc<ConsumerHandle>` shared with the source ingest thread:
/// the thread updates `handle.bytes` from the bounded-channel queue
/// depth × estimated per-record bytes at each batch boundary.
///
/// Sources do not spill: `try_spill` returns `Ok(0)` and the
/// arbitrator's policy is expected to choose `pause` instead via
/// `BackPressurePreferred`. `spill_priority = i32::MAX` so the
/// `Priority` fallback ranks Sources last among the spill candidates
/// when no back-pressureable consumer is available.
/// `can_back_pressure = true`; `pause` / `resume` forward to the
/// handle's pause flag, which the ingest thread reads at every
/// `push` boundary.
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
        &self,
        _target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        Ok(0)
    }

    fn can_back_pressure(&self) -> bool {
        true
    }

    fn pause(&self) {
        self.handle.pause();
    }

    fn resume(&self) {
        self.handle.resume();
    }

    fn is_paused(&self) -> bool {
        self.handle.is_paused()
    }

    fn is_active(&self) -> bool {
        self.handle.is_active()
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
        let consumer = SourceConsumer::new(handle.clone());
        assert_eq!(consumer.current_usage(), 16 * 1024);
        // Sources don't spill: try_spill always reports zero freed.
        assert_eq!(consumer.try_spill(u64::MAX).unwrap(), 0);
        assert!(!handle.take_spill_request());
    }

    #[test]
    fn source_consumer_is_back_pressureable_and_routes_pause_to_handle() {
        let handle = ConsumerHandle::new();
        let consumer = SourceConsumer::new(handle.clone());
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

    #[test]
    fn ewma_step_seeds_on_first_sample() {
        // Unseeded (prev == 0) adopts the sample directly so the
        // estimate does not climb from zero over the first several
        // pushes.
        assert_eq!(ewma_step(0, 4096), 4096);
    }

    #[test]
    fn ewma_step_converges_toward_steady_sample() {
        // Repeated identical samples drive the average to that value
        // and hold it there.
        let mut ewma = ewma_step(0, 1000);
        for _ in 0..64 {
            ewma = ewma_step(ewma, 1000);
        }
        assert_eq!(ewma, 1000);
    }

    #[test]
    fn ewma_step_damps_a_single_spike() {
        // A 10x spike over an established baseline moves the estimate
        // by ~1/8 of the gap, not the full gap: 1000 + (10000-1000)/8.
        let seeded = ewma_step(0, 1000);
        assert_eq!(seeded, 1000);
        let after_spike = ewma_step(seeded, 10_000);
        assert_eq!(after_spike, 1000 + (10_000 - 1000) / 8);
        assert!(after_spike < 10_000);
    }

    #[test]
    fn ewma_step_is_underflow_safe_when_sample_shrinks() {
        // A sample far below the baseline decays downward by 1/8 of the
        // gap without underflowing the unsigned subtraction.
        let after = ewma_step(8000, 0);
        assert_eq!(after, 8000 - 8000 / 8);
        // Drive it down repeatedly: it approaches but never wraps past
        // zero.
        let mut ewma = 8000u64;
        for _ in 0..256 {
            ewma = ewma_step(ewma, 1);
        }
        assert!(ewma >= 1);
    }
}
