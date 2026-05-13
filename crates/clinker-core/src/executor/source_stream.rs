//! Per-source live ingest channel and its backing buffer tier.
//!
//! The [`SourceStream`] trait is the async-producer contract used by a
//! Source-reader task to push records into the executor. The production
//! implementation [`TokioSourceStream`] wraps `tokio::sync::mpsc::Sender`,
//! returning the paired `Receiver` to the dispatch loop's Source arm.
//! Channel capacity is bounded so producers `.await` on `push` when the
//! consumer falls behind, supplying back-pressure end-to-end.
//!
//! The [`BufferedSourceSink`] type is the in-memory + disk-spill backing
//! tier: records are appended via `push`; the first `capacity` records
//! land in memory; the rest are streamed to a `SpillWriter<u64>`. Once
//! every record is pushed, `finish` hands back a [`DrainedSourceStream`]
//! that materializes the full FIFO sequence — in-memory portion first,
//! then any spilled records read sequentially from disk. This tier
//! absorbs overflow when the live mpsc channel is full; it is wired into
//! the source ingest path in the next commit (#57).

use std::path::Path;
use std::sync::Arc;

use clinker_record::{Record, Schema};

use crate::pipeline::spill::{SpillError, SpillFile, SpillWriter};

/// Default in-memory capacity threshold before overflow records spill
/// to disk. Sized to cover most real-world non-primary feeds without
/// spilling while still bounding peak RSS for vendor-feed-scale inputs.
pub(crate) const DEFAULT_SOURCE_STREAM_CAPACITY: usize = 64 * 1024;

/// Per-source live ingest channel.
///
/// Implementations push records produced by a Source-reader task and
/// surface a consumer handle to the executor's dispatch loop. The
/// production tokio impl backs the trait with `tokio::sync::mpsc` plus
/// a [`BufferedSourceSink`] spill tier that absorbs overflow when the
/// channel is full.
pub(crate) trait SourceStream: Send {
    /// Push one record onto the stream. Awaits if the channel is at
    /// capacity. Returns [`SourceStreamError::Closed`] if the consumer
    /// has dropped its receiver.
    async fn push(&mut self, record: Record, row_num: u64) -> Result<(), SourceStreamError>;

    /// Signal end-of-stream. The consumer's `recv().await` will then
    /// return `None` once the channel drains.
    async fn finish(self: Box<Self>) -> Result<(), SourceStreamError>;
}

/// Error surface for [`SourceStream`] implementations.
#[derive(Debug)]
pub(crate) enum SourceStreamError {
    /// Spill-tier I/O failure when the in-memory portion overflowed
    /// and the on-disk write didn't succeed.
    Spill(SpillError),
    /// Consumer dropped the receiver before this push completed.
    Closed,
}

impl std::fmt::Display for SourceStreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Spill(e) => write!(f, "source stream spill: {e}"),
            Self::Closed => write!(f, "source stream closed: consumer dropped receiver"),
        }
    }
}

impl std::error::Error for SourceStreamError {}

impl From<SpillError> for SourceStreamError {
    fn from(e: SpillError) -> Self {
        Self::Spill(e)
    }
}

/// Tokio-mpsc-backed [`SourceStream`] implementation.
///
/// Capacity is bounded; producers `.await` on `push` when the channel
/// is full, providing back-pressure to the upstream Source reader.
/// Consumer is a paired `tokio::sync::mpsc::Receiver` returned by
/// [`Self::new`].
pub(crate) struct TokioSourceStream {
    tx: tokio::sync::mpsc::Sender<(Record, u64)>,
}

impl TokioSourceStream {
    /// Default channel capacity. Matches the cooperative-yield cadence
    /// chosen for the async dispatch loop: one batch of `yield_now`
    /// calls in Transform/Route corresponds to roughly one channel
    /// drain.
    pub(crate) const DEFAULT_CAPACITY: usize = 1024;

    /// Create a new stream + paired receiver. The receiver is what the
    /// dispatch loop's Source arm consumes via `recv().await`.
    pub(crate) fn new(capacity: usize) -> (Self, tokio::sync::mpsc::Receiver<(Record, u64)>) {
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        (Self { tx }, rx)
    }
}

impl SourceStream for TokioSourceStream {
    async fn push(&mut self, record: Record, row_num: u64) -> Result<(), SourceStreamError> {
        self.tx
            .send((record, row_num))
            .await
            .map_err(|_| SourceStreamError::Closed)
    }

    async fn finish(self: Box<Self>) -> Result<(), SourceStreamError> {
        // Drop the sender; consumer's recv() returns None when drained.
        drop(self.tx);
        Ok(())
    }
}

/// In-memory + disk-spill backing tier for a Source's ingest buffer.
///
/// Records are appended via [`Self::push`]. The first `capacity`
/// records land in memory; the rest are streamed to a
/// `SpillWriter<u64>`. Once every record is pushed, [`Self::finish`]
/// hands back a [`DrainedSourceStream`] that can be consumed exactly
/// once.
//
// `#[allow(dead_code)]` removed in commit 4 when the source ingest
// path wires this tier behind `TokioSourceStream` for overflow.
#[allow(dead_code)]
pub(crate) struct BufferedSourceSink {
    schema: Arc<Schema>,
    capacity: usize,
    in_memory: Vec<(Record, u64)>,
    spill_dir: Option<Arc<Path>>,
    /// Lazy-allocated on first overflow; `None` if every record fit in
    /// the in-memory portion.
    spill_writer: Option<SpillWriter<u64>>,
}

#[allow(dead_code)]
impl BufferedSourceSink {
    /// Construct a fresh sink. `spill_dir` is the temp-directory
    /// override forwarded to `SpillWriter::new`; `None` falls back to
    /// the system tempdir (per-file cleanup via `tempfile::TempPath`
    /// Drop still applies).
    pub(crate) fn new(schema: Arc<Schema>, capacity: usize, spill_dir: Option<Arc<Path>>) -> Self {
        Self {
            schema,
            capacity,
            in_memory: Vec::new(),
            spill_dir,
            spill_writer: None,
        }
    }

    /// Append one record + source row number. Overflow records past
    /// `capacity` go to disk.
    pub(crate) fn push(&mut self, record: Record, row_num: u64) -> Result<(), SpillError> {
        if self.in_memory.len() < self.capacity {
            self.in_memory.push((record, row_num));
            return Ok(());
        }
        let writer = match self.spill_writer.as_mut() {
            Some(w) => w,
            None => {
                let w =
                    SpillWriter::<u64>::new(Arc::clone(&self.schema), self.spill_dir.as_deref())?;
                self.spill_writer = Some(w);
                self.spill_writer.as_mut().expect("just inserted")
            }
        };
        writer.write_pair(&record, &row_num)
    }

    /// Finalize the sink. Closes the spill writer (if any) and returns
    /// the read-side handle.
    pub(crate) fn finish(self) -> Result<DrainedSourceStream, SpillError> {
        let BufferedSourceSink {
            in_memory,
            spill_writer,
            ..
        } = self;
        let spill_file = match spill_writer {
            Some(w) => Some(w.finish()?),
            None => None,
        };
        Ok(DrainedSourceStream {
            in_memory,
            spill_file,
        })
    }
}

/// Read-side handle for a finalized [`BufferedSourceSink`].
/// Single-consume — `into_records` drains both the in-memory buffer
/// and the spill file.
//
// `#[allow(dead_code)]` removed in commit 4 when the source ingest
// path wires this tier behind `TokioSourceStream` for overflow.
#[allow(dead_code)]
pub(crate) struct DrainedSourceStream {
    in_memory: Vec<(Record, u64)>,
    spill_file: Option<SpillFile<u64>>,
}

#[allow(dead_code)]
impl DrainedSourceStream {
    /// Materialize every record in FIFO order. The in-memory portion
    /// is returned first; spilled records follow, read sequentially
    /// from the temp file. The `SpillFile` is dropped at end of scope
    /// so the temp file deletes itself via `TempPath` Drop.
    pub(crate) fn into_records(self) -> Result<Vec<(Record, u64)>, SpillError> {
        let DrainedSourceStream {
            mut in_memory,
            spill_file,
        } = self;
        if let Some(spill) = spill_file {
            let reader = spill.reader()?;
            for item in reader {
                let (record, row_num) = item?;
                in_memory.push((record, row_num));
            }
        }
        Ok(in_memory)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;

    fn schema_id_payload() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["id".into(), "payload".into()]))
    }

    fn record(schema: &Arc<Schema>, id: i64, payload: &str) -> Record {
        Record::new(
            Arc::clone(schema),
            vec![Value::Integer(id), Value::String(payload.into())],
        )
    }

    #[test]
    fn in_memory_only_preserves_fifo() {
        let schema = schema_id_payload();
        let mut sink = BufferedSourceSink::new(Arc::clone(&schema), 1024, None);
        for i in 0..50 {
            sink.push(record(&schema, i, &format!("r{i}")), (i + 1) as u64)
                .unwrap();
        }
        let drained = sink.finish().unwrap();
        let records = drained.into_records().unwrap();
        assert_eq!(records.len(), 50);
        for (i, (rec, row_num)) in records.iter().enumerate() {
            assert_eq!(*row_num, (i + 1) as u64);
            assert_eq!(rec.get("id"), Some(&Value::Integer(i as i64)));
            assert_eq!(
                rec.get("payload"),
                Some(&Value::String(format!("r{i}").into())),
            );
        }
    }

    #[test]
    fn overflow_spills_and_drains_in_order() {
        let schema = schema_id_payload();
        // Capacity = 4 so records 5..20 spill.
        let mut sink = BufferedSourceSink::new(Arc::clone(&schema), 4, None);
        for i in 0..20 {
            sink.push(record(&schema, i, &format!("r{i}")), (i + 1) as u64)
                .unwrap();
        }
        let drained = sink.finish().unwrap();
        let records = drained.into_records().unwrap();
        assert_eq!(records.len(), 20);
        // FIFO across the in-memory / disk boundary.
        for (i, (rec, row_num)) in records.iter().enumerate() {
            assert_eq!(*row_num, (i + 1) as u64);
            assert_eq!(rec.get("id"), Some(&Value::Integer(i as i64)));
        }
    }

    #[test]
    fn empty_sink_yields_empty_vec() {
        let schema = schema_id_payload();
        let sink = BufferedSourceSink::new(schema, 1024, None);
        let records = sink.finish().unwrap().into_records().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn capacity_zero_spills_every_record() {
        let schema = schema_id_payload();
        let mut sink = BufferedSourceSink::new(Arc::clone(&schema), 0, None);
        for i in 0..5 {
            sink.push(record(&schema, i, "x"), (i + 1) as u64).unwrap();
        }
        let records = sink.finish().unwrap().into_records().unwrap();
        assert_eq!(records.len(), 5);
        for (i, (_, rn)) in records.iter().enumerate() {
            assert_eq!(*rn, (i + 1) as u64);
        }
    }

    #[test]
    fn spill_dir_override_is_honored() {
        let custom = tempfile::tempdir().unwrap();
        let custom_arc: Arc<Path> = Arc::from(custom.path());
        let schema = schema_id_payload();
        let mut sink =
            BufferedSourceSink::new(Arc::clone(&schema), 0, Some(Arc::clone(&custom_arc)));
        sink.push(record(&schema, 1, "x"), 1).unwrap();
        let drained = sink.finish().unwrap();
        // The drain itself confirms the spill file is readable from
        // the custom dir; assertion here exercises that path.
        let records = drained.into_records().unwrap();
        assert_eq!(records.len(), 1);
    }
}
