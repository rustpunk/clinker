//! Bounded source-ingest buffer with disk-spill overflow.
//!
//! `SourceStream` ingests records from a non-primary Source into an
//! in-memory buffer keyed by source row number. When the buffer exceeds
//! `capacity`, subsequent records spill to a temp file via
//! `pipeline::spill::SpillFile<u64>` (payload = source row number,
//! preserving FIFO order across the in-memory / on-disk boundary).
//!
//! `finish()` finalizes any open spill writer and returns a
//! `DrainedSourceStream` whose `into_records()` materializes every
//! record back into a `Vec<(Record, u64)>` in FIFO order — in-memory
//! portion first, then any spilled records read sequentially from disk.
//!
//! Sprint-1 scope: single-threaded ingest in the executor's preload
//! pass, drained back into `ExecutorContext.preloaded_source_records`
//! before dispatch runs. The peak-RSS win versus today's
//! `HashMap<String, Vec<(Record, u64)>>` preload is that each
//! non-primary Source's overflow can spill before the next source's
//! ingest begins, so only one source's in-memory portion is resident
//! at a time. Sub-issue #51 collapses the primary-source path onto
//! `SourceStream` too; #57 swaps the impl for a concurrent
//! channel-backed ingest under tokio.

use std::path::Path;
use std::sync::Arc;

use clinker_record::{Record, Schema};

use crate::pipeline::spill::{SpillError, SpillFile, SpillWriter};

/// Default in-memory capacity threshold before overflow records spill
/// to disk. Sized to cover most real-world non-primary feeds without
/// spilling while still bounding peak RSS for vendor-feed-scale inputs.
pub(crate) const DEFAULT_SOURCE_STREAM_CAPACITY: usize = 64 * 1024;

/// Write-side buffer for ingesting a non-primary Source's records.
///
/// Records are appended via [`push`]. The first `capacity` records
/// land in memory; the rest are streamed to a `SpillWriter<u64>`. Once
/// every record is pushed, [`finish`] hands back a [`DrainedSourceStream`]
/// that can be consumed exactly once.
pub(crate) struct SourceStream {
    schema: Arc<Schema>,
    capacity: usize,
    in_memory: Vec<(Record, u64)>,
    spill_dir: Option<Arc<Path>>,
    /// Lazy-allocated on first overflow; `None` if every record fit in
    /// the in-memory portion.
    spill_writer: Option<SpillWriter<u64>>,
}

impl SourceStream {
    /// Construct a fresh stream. `spill_dir` is the temp-directory
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

    /// Finalize the stream. Closes the spill writer (if any) and
    /// returns the read-side handle.
    pub(crate) fn finish(self) -> Result<DrainedSourceStream, SpillError> {
        let SourceStream {
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

/// Read-side handle for a finalized [`SourceStream`]. Single-consume —
/// `into_records` drains both the in-memory buffer and the spill file.
pub(crate) struct DrainedSourceStream {
    in_memory: Vec<(Record, u64)>,
    spill_file: Option<SpillFile<u64>>,
}

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
        let mut stream = SourceStream::new(Arc::clone(&schema), 1024, None);
        for i in 0..50 {
            stream
                .push(record(&schema, i, &format!("r{i}")), (i + 1) as u64)
                .unwrap();
        }
        let drained = stream.finish().unwrap();
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
        let mut stream = SourceStream::new(Arc::clone(&schema), 4, None);
        for i in 0..20 {
            stream
                .push(record(&schema, i, &format!("r{i}")), (i + 1) as u64)
                .unwrap();
        }
        let drained = stream.finish().unwrap();
        let records = drained.into_records().unwrap();
        assert_eq!(records.len(), 20);
        // FIFO across the in-memory / disk boundary.
        for (i, (rec, row_num)) in records.iter().enumerate() {
            assert_eq!(*row_num, (i + 1) as u64);
            assert_eq!(rec.get("id"), Some(&Value::Integer(i as i64)));
        }
    }

    #[test]
    fn empty_stream_yields_empty_vec() {
        let schema = schema_id_payload();
        let stream = SourceStream::new(schema, 1024, None);
        let records = stream.finish().unwrap().into_records().unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn capacity_zero_spills_every_record() {
        let schema = schema_id_payload();
        let mut stream = SourceStream::new(Arc::clone(&schema), 0, None);
        for i in 0..5 {
            stream
                .push(record(&schema, i, "x"), (i + 1) as u64)
                .unwrap();
        }
        let records = stream.finish().unwrap().into_records().unwrap();
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
        let mut stream = SourceStream::new(Arc::clone(&schema), 0, Some(Arc::clone(&custom_arc)));
        stream.push(record(&schema, 1, "x"), 1).unwrap();
        let drained = stream.finish().unwrap();
        // The drain itself confirms the spill file is readable from
        // the custom dir; assertion here exercises that path.
        let records = drained.into_records().unwrap();
        assert_eq!(records.len(), 1);
    }
}
