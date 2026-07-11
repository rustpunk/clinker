//! Sort buffer: accumulates `(record, payload)` pairs, sorts in-memory or
//! spills to disk.
//!
//! Used at source-sort, output-sort, and DAG enforcer-sort intercept points.
//! The buffer tracks its own memory usage and spills sorted chunks to
//! postcard temp files — optionally LZ4-framed per the workspace
//! `[storage.spill] compress` knob — when the budget is exceeded.
//!
//! Two ordering modes, chosen at construction:
//!   - Field-ordered ([`SortBuffer::new`]): the sort key is read from the
//!     record via [`compare_records_by_fields`] and the payload rides along
//!     inert. Every source/output/DAG/join sort uses this.
//!   - Payload-ordered ([`SortBuffer::new_payload_ordered`]): pairs order by the
//!     payload `P: Ord` directly, with no record field consulted. This serves a
//!     sort whose key is a value computed off the record — e.g. a range join
//!     keying on evaluated inequality expressions that back no single column —
//!     so the key need not be stamped onto the record as a synthetic field.
//!
//! Generic over per-record payload `P`. Source/output sort uses `SortBuffer<()>`;
//! the DAG enforcer-sort and the sort-merge join carry a `u64` row-order tag as
//! the payload. Payload travels inside the spill envelope (bundled-tuple sort),
//! not on a parallel array, to avoid permutation-reindex bugs.

use std::path::PathBuf;
use std::sync::Arc;

use rayon::slice::ParallelSliceMut;
use serde::{Serialize, de::DeserializeOwned};

use clinker_record::{Record, Schema};

use crate::pipeline::sort::compare_records_by_fields;
use crate::pipeline::spill::{SpillFile, SpillWriter};
use clinker_plan::SpillError;
use clinker_plan::config::SortField;

/// Result of finishing a sort buffer: either all (record, payload) pairs
/// fit in memory, or some were spilled to disk.
pub enum SortedOutput<P> {
    /// All pairs fit in memory. Sorted and ready to iterate.
    InMemory(Vec<(Record, P)>),
    /// Pairs were spilled to sorted temp files. Must be merged via LoserTree.
    Spilled(Vec<SpillFile<P>>),
}

/// How a [`SortBuffer`] orders its accumulated pairs.
enum SortOrdering {
    /// Order by [`compare_records_by_fields`] over these fields; the record
    /// carries the sort key and the payload rides along inert.
    Fields(Vec<SortField>),
    /// Order by the carried payload `P: Ord` directly, with no record field
    /// consulted. For a sort key computed off the record rather than stored in
    /// a column.
    Payload,
}

/// Accumulates `(record, payload)` pairs, sorts in-memory or spills to disk
/// when the byte budget is exceeded. Orders either by a record-field comparator
/// (payload inert) or by the payload `P: Ord` directly, per the constructor
/// chosen.
pub struct SortBuffer<P> {
    pairs: Vec<(Record, P)>,
    ordering: SortOrdering,
    bytes_used: usize,
    spill_threshold: usize,
    spill_dir: Option<PathBuf>,
    /// Whether spilled sorted runs are LZ4-compressed. Resolved by the caller
    /// from the workspace `[storage.spill] compress` knob against the sort
    /// schema's width and the run's batch size, so the on-disk format matches
    /// what `--explain` reports.
    spill_compress: bool,
    spill_files: Vec<SpillFile<P>>,
    schema: Arc<Schema>,
}

// `P: Ord` spans the whole impl, not just the payload-ordered constructor, so
// the shared `sort_and_spill` / `finish` path can compare payloads in the
// payload-ordered mode without splitting the buffer across two impl blocks.
// Every payload type in use is already `Ord`, so this constrains no caller.
impl<P: Serialize + DeserializeOwned + Send + Ord> SortBuffer<P> {
    /// Field-ordered buffer: pairs sort by `sort_by` over each record's fields
    /// and the payload rides along inert. The historical mode; used by every
    /// source/output/DAG/join sort.
    pub fn new(
        sort_by: Vec<SortField>,
        spill_threshold: usize,
        spill_dir: Option<PathBuf>,
        spill_compress: bool,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            pairs: Vec::new(),
            ordering: SortOrdering::Fields(sort_by),
            bytes_used: 0,
            spill_threshold,
            spill_dir,
            spill_compress,
            spill_files: Vec::new(),
            schema,
        }
    }

    /// Payload-ordered buffer: pairs sort by the payload `P: Ord` directly, with
    /// no record field consulted. For a sort key computed off the record (e.g. a
    /// range join's evaluated inequality keys) that backs no single column, so
    /// no synthetic sort column need be stamped onto the records. Spill,
    /// merge, and charging behave exactly as in the field-ordered mode.
    pub fn new_payload_ordered(
        spill_threshold: usize,
        spill_dir: Option<PathBuf>,
        spill_compress: bool,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            pairs: Vec::new(),
            ordering: SortOrdering::Payload,
            bytes_used: 0,
            spill_threshold,
            spill_dir,
            spill_compress,
            spill_files: Vec::new(),
            schema,
        }
    }

    /// Push a `(record, payload)` pair into the buffer.
    pub fn push(&mut self, record: Record, payload: P) {
        let size =
            std::mem::size_of::<Record>() + record.estimated_heap_size() + std::mem::size_of::<P>();
        self.bytes_used += size;
        self.pairs.push((record, payload));
    }

    /// Check if the buffer has exceeded its memory threshold.
    pub fn should_spill(&self) -> bool {
        self.bytes_used > 0 && self.bytes_used >= self.spill_threshold
    }

    /// Stable-sort the in-memory pairs by the buffer's ordering mode. Parallel
    /// stable sort on the shared kernel pool: `par_sort_by` preserves the
    /// tie-break order of the sequential `slice::sort_by`, so a spilled run is
    /// byte-identical to the sequential sort and equal keys keep input order.
    fn sort_pairs(&mut self) {
        // Split the borrow so the comparator can read `ordering` while
        // `par_sort_by` holds `pairs` mutably.
        let Self {
            pairs, ordering, ..
        } = self;
        match ordering {
            SortOrdering::Fields(sort_by) => {
                pairs.par_sort_by(|(a, _), (b, _)| compare_records_by_fields(a, b, sort_by));
            }
            SortOrdering::Payload => {
                pairs.par_sort_by(|(_, a), (_, b)| a.cmp(b));
            }
        }
    }

    /// Sort the current in-memory pairs and write them to a spill file. Clears
    /// the buffer and resets the byte counter. Returns the spilled file's exact
    /// on-disk byte length so the caller can charge it against the pipeline
    /// disk-spill quota; an empty buffer writes nothing and returns 0.
    pub fn sort_and_spill(&mut self) -> Result<u64, SpillError> {
        if self.pairs.is_empty() {
            return Ok(0);
        }

        self.sort_pairs();

        let mut writer: SpillWriter<P> = SpillWriter::new(
            self.schema.clone(),
            self.spill_dir.as_deref(),
            self.spill_compress,
        )?;
        for (record, payload) in self.pairs.drain(..) {
            writer.write_pair(&record, &payload)?;
        }
        // The writer reports its own on-disk byte total (the same figure the
        // disk-cap accounting charges for every spill op), so a written run is
        // always charged — no post-hoc `stat` that could fail and charge 0.
        let (spill_file, written) = writer.finish_with_bytes()?;
        self.spill_files.push(spill_file);
        self.bytes_used = 0;
        Ok(written)
    }

    /// Finish the sort buffer. If pairs were spilled, remaining in-memory
    /// pairs are also spilled. Returns the sorted output — in-memory pairs or
    /// the list of sorted spill files for merge — paired with the byte length
    /// of the residue run this call spilled (0 when nothing spilled here), so
    /// the caller can charge that final run against the disk-spill quota.
    pub fn finish(mut self) -> Result<(SortedOutput<P>, u64), SpillError> {
        if self.spill_files.is_empty() {
            self.sort_pairs();
            Ok((SortedOutput::InMemory(self.pairs), 0))
        } else {
            let residue = if !self.pairs.is_empty() {
                self.sort_and_spill()?
            } else {
                0
            };
            Ok((SortedOutput::Spilled(self.spill_files), residue))
        }
    }

    /// Current estimated memory usage in bytes.
    pub fn bytes_used(&self) -> usize {
        self.bytes_used
    }
}

/// `MemoryConsumer` wrapper for a `SortBuffer<P>`. Holds an
/// `Arc<ConsumerHandle>` shared with the buffer: the buffer mirrors
/// its `bytes_used` into `handle.bytes` on every `push` / `sort_and_spill`
/// transition. `try_spill` flips the handle's spill-request flag; the
/// buffer's owning operator reads it at the next batch boundary and
/// calls `sort_and_spill` in-thread.
///
/// `spill_priority = 20`: sort runs are cheaper to flush than hash-
/// aggregation rebuilds — every run is already sequentially ordered
/// and writes straight through `SpillWriter<P>` with no per-group
/// fixup. `can_back_pressure = false`: an in-flight sort buffer
/// cannot be paused without losing run continuity; sort consumers
/// expect inputs to arrive monotonically.
pub struct SortConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl SortConsumer {
    pub fn new(handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl crate::pipeline::memory::MemoryConsumer for SortConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        20
    }

    fn try_spill(
        &self,
        target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        self.handle.request_spill();
        let bytes = self.handle.bytes();
        if bytes >= target_bytes {
            Ok(bytes)
        } else {
            Err(crate::pipeline::memory::ConsumerSpillError::BelowTarget {
                target: target_bytes,
                freed: bytes,
            })
        }
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["name".into(), "value".into()]))
    }

    fn make_record(schema: &Arc<Schema>, name: &str, value: i64) -> Record {
        Record::new(
            schema.clone(),
            vec![Value::String(name.into()), Value::Integer(value)],
        )
    }

    fn sort_by_value_asc() -> Vec<SortField> {
        vec![SortField {
            field: "value".into(),
            order: clinker_plan::config::SortOrder::Asc,
            null_order: None,
        }]
    }

    #[test]
    fn test_sort_buffer_push_tracks_bytes() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> =
            SortBuffer::new(sort_by_value_asc(), 1_000_000, None, true, schema.clone());
        assert_eq!(buf.bytes_used(), 0);
        buf.push(make_record(&schema, "Alice", 1), ());
        assert!(buf.bytes_used() > 0);
        let first = buf.bytes_used();
        buf.push(make_record(&schema, "Bob", 2), ());
        assert!(buf.bytes_used() > first);
    }

    #[test]
    fn test_sort_buffer_should_spill_at_threshold() {
        let schema = test_schema();
        // Very small threshold — should spill after a few records
        let mut buf: SortBuffer<()> =
            SortBuffer::new(sort_by_value_asc(), 100, None, true, schema.clone());
        assert!(!buf.should_spill());
        // Push records until we exceed 100 bytes
        for i in 0..10 {
            buf.push(make_record(&schema, &format!("name_{i}"), i), ());
            if buf.should_spill() {
                return; // Test passes — spill triggered
            }
        }
        panic!("should_spill() never triggered with 100-byte threshold");
    }

    #[test]
    fn test_sort_buffer_in_memory_sort() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> =
            SortBuffer::new(sort_by_value_asc(), 1_000_000, None, true, schema.clone());
        buf.push(make_record(&schema, "Charlie", 30), ());
        buf.push(make_record(&schema, "Alice", 10), ());
        buf.push(make_record(&schema, "Bob", 20), ());

        match buf.finish().unwrap().0 {
            SortedOutput::InMemory(pairs) => {
                assert_eq!(pairs.len(), 3);
                assert_eq!(pairs[0].0.get("value"), Some(&Value::Integer(10)));
                assert_eq!(pairs[1].0.get("value"), Some(&Value::Integer(20)));
                assert_eq!(pairs[2].0.get("value"), Some(&Value::Integer(30)));
            }
            SortedOutput::Spilled(_) => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_buffer_spill_produces_spill_files() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> =
            SortBuffer::new(sort_by_value_asc(), 1, None, true, schema.clone()); // threshold=1 → spill immediately
        buf.push(make_record(&schema, "Alice", 10), ());
        assert!(buf.should_spill());
        buf.sort_and_spill().unwrap();
        assert_eq!(buf.bytes_used(), 0);
        assert_eq!(buf.pairs.len(), 0);
    }

    #[test]
    fn test_sort_buffer_finish_spilled_returns_files() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> =
            SortBuffer::new(sort_by_value_asc(), 1, None, true, schema.clone());

        // Push and spill twice
        buf.push(make_record(&schema, "B", 20), ());
        buf.sort_and_spill().unwrap();
        buf.push(make_record(&schema, "A", 10), ());
        buf.sort_and_spill().unwrap();
        // Push one more without spilling — finish() will spill it
        buf.push(make_record(&schema, "C", 30), ());

        match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => {
                assert_eq!(files.len(), 3); // 2 manual spills + 1 from finish()
            }
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_buffer_spill_files_are_sorted() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> =
            SortBuffer::new(sort_by_value_asc(), 1, None, true, schema.clone());
        buf.push(make_record(&schema, "C", 30), ());
        buf.push(make_record(&schema, "A", 10), ());
        buf.push(make_record(&schema, "B", 20), ());
        buf.sort_and_spill().unwrap();

        // Read back and verify sorted
        match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => {
                let reader = files[0].reader().unwrap();
                let recs: Vec<Record> = reader.map(|r| r.unwrap().0).collect();
                assert_eq!(recs.len(), 3);
                assert_eq!(recs[0].get("value"), Some(&Value::Integer(10)));
                assert_eq!(recs[1].get("value"), Some(&Value::Integer(20)));
                assert_eq!(recs[2].get("value"), Some(&Value::Integer(30)));
            }
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_buffer_payload_survives_in_memory_sort() {
        // Pattern B gate: payload travels with the record through sort permutation.
        let schema = test_schema();
        let mut buf: SortBuffer<u64> =
            SortBuffer::new(sort_by_value_asc(), 1_000_000, None, true, schema.clone());
        buf.push(make_record(&schema, "Charlie", 30), 100);
        buf.push(make_record(&schema, "Alice", 10), 200);
        buf.push(make_record(&schema, "Bob", 20), 300);
        match buf.finish().unwrap().0 {
            SortedOutput::InMemory(pairs) => {
                assert_eq!(pairs.len(), 3);
                // Sorted by value asc: Alice(10,200), Bob(20,300), Charlie(30,100)
                assert_eq!(pairs[0].1, 200);
                assert_eq!(pairs[1].1, 300);
                assert_eq!(pairs[2].1, 100);
            }
            _ => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_buffer_payload_survives_spill() {
        // Payload survives the postcard spill envelope through the round-trip.
        let schema = test_schema();
        let mut buf: SortBuffer<u64> =
            SortBuffer::new(sort_by_value_asc(), 1, None, true, schema.clone());
        buf.push(make_record(&schema, "C", 30), 100);
        buf.push(make_record(&schema, "A", 10), 200);
        buf.push(make_record(&schema, "B", 20), 300);
        buf.sort_and_spill().unwrap();
        match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => {
                let reader = files[0].reader().unwrap();
                let pairs: Vec<(Record, u64)> = reader.map(|r| r.unwrap()).collect();
                assert_eq!(pairs.len(), 3);
                assert_eq!(pairs[0].0.get("value"), Some(&Value::Integer(10)));
                assert_eq!(pairs[0].1, 200);
                assert_eq!(pairs[1].0.get("value"), Some(&Value::Integer(20)));
                assert_eq!(pairs[1].1, 300);
                assert_eq!(pairs[2].0.get("value"), Some(&Value::Integer(30)));
                assert_eq!(pairs[2].1, 100);
            }
            _ => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_buffer_payload_ordered_in_memory_sorts_by_payload() {
        // Payload-ordered mode sorts by the (i64, i64, u64) payload — the shape
        // a range join carries — and never consults a record field. Records
        // carry an unrelated `value`; ordering must ignore it. Negative primary
        // keys must order correctly.
        let schema = test_schema();
        let mut buf: SortBuffer<(i64, i64, u64)> =
            SortBuffer::new_payload_ordered(1_000_000, None, true, schema.clone());
        buf.push(make_record(&schema, "a", 999), (5, 0, 0));
        buf.push(make_record(&schema, "b", 111), (-3, 2, 1));
        buf.push(make_record(&schema, "c", 555), (-3, 1, 2));
        buf.push(make_record(&schema, "d", 222), (5, 0, 3));
        match buf.finish().unwrap().0 {
            SortedOutput::InMemory(pairs) => {
                let payloads: Vec<(i64, i64, u64)> = pairs.iter().map(|(_, p)| *p).collect();
                assert_eq!(payloads, vec![(-3, 1, 2), (-3, 2, 1), (5, 0, 0), (5, 0, 3)]);
            }
            SortedOutput::Spilled(_) => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_buffer_payload_ordered_forced_spill_multiple_runs() {
        // A tiny threshold plus explicit flushes forces several
        // individually-sorted runs to disk; finish() flushes the residue as one
        // more. Payload-ordered spill uses the same envelope as field-ordered.
        let schema = test_schema();
        let mut buf: SortBuffer<(i64, i64, u64)> =
            SortBuffer::new_payload_ordered(1, None, true, schema.clone());
        for i in 0..3i64 {
            buf.push(make_record(&schema, "r", i), (i, 0, i as u64));
            assert!(buf.should_spill());
            buf.sort_and_spill().unwrap();
        }
        // One more pair left resident for finish() to flush.
        buf.push(make_record(&schema, "r", 9), (9, 0, 9));
        match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => assert_eq!(files.len(), 4, "3 flushes + 1 residue"),
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_buffer_empty_returns_empty() {
        let schema = test_schema();
        let buf: SortBuffer<()> =
            SortBuffer::new(sort_by_value_asc(), 1_000_000, None, true, schema);
        match buf.finish().unwrap().0 {
            SortedOutput::InMemory(pairs) => assert!(pairs.is_empty()),
            SortedOutput::Spilled(_) => panic!("expected InMemory"),
        }
    }
}
