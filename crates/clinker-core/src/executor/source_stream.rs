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
}

impl TokioSourceStream {
    /// Default channel capacity. Matches the cooperative-yield cadence
    /// chosen for the async dispatch loop: one batch of `yield_now`
    /// calls in Transform/Route corresponds to roughly one channel
    /// drain.
    pub(crate) const DEFAULT_CAPACITY: usize = 1024;

    /// Create a new stream + paired receiver. The receiver is what the
    /// dispatch loop's Source arm consumes via `recv().await`.
    pub(crate) fn new(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<StreamEvent>) {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        (Self { tx }, rx)
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
        self.tx
            .blocking_send(StreamEvent::record(record, row_num))
            .map_err(|_| SourceStreamError::Closed)
    }

    /// Push a document-boundary punctuation. One `DocumentOpen` and
    /// one `DocumentClose` per file; the executor's dispatch loop
    /// forwards them through downstream stages with operator-specific
    /// behavior (Aggregate / Output flush; Merge dedupes; Transform /
    /// Route pass through).
    pub(crate) fn blocking_push_punctuation(
        &mut self,
        punct: Punctuation,
    ) -> Result<(), SourceStreamError> {
        self.tx
            .blocking_send(StreamEvent::punctuation(punct))
            .map_err(|_| SourceStreamError::Closed)
    }
}
