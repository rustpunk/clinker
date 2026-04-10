//! Sort buffer: accumulates `(record, payload)` pairs, sorts in-memory or
//! spills to disk.
//!
//! Used at source-sort, output-sort, and DAG enforcer-sort intercept points.
//! The buffer tracks its own memory usage and spills sorted chunks to
//! NDJSON+LZ4 temp files when the budget is exceeded.
//!
//! Generic over per-record payload `P`. Phase 8 source/output sort uses
//! `SortBuffer<()>`; the DAG executor's `PlanNode::Sort` dispatch arm uses
//! `SortBuffer<(u64, IndexMap<String,Value>, IndexMap<String,Value>)>` to
//! carry row number and metadata maps through the sort permutation.
//!
//! See `docs/research/RESEARCH-sort-node-sidecar-payload.md` for the
//! architectural rationale (Pattern B: bundled-tuple sort with payload in
//! the spill envelope).

use std::path::PathBuf;
use std::sync::Arc;

use serde::{Serialize, de::DeserializeOwned};

use clinker_record::{Record, Schema};

use crate::config::SortField;
use crate::pipeline::sort::compare_records_by_fields;
use crate::pipeline::spill::{SpillError, SpillFile, SpillWriter};

/// Result of finishing a sort buffer: either all (record, payload) pairs
/// fit in memory, or some were spilled to disk.
pub enum SortedOutput<P> {
    /// All pairs fit in memory. Sorted and ready to iterate.
    InMemory(Vec<(Record, P)>),
    /// Pairs were spilled to sorted temp files. Must be merged via LoserTree.
    Spilled(Vec<SpillFile<P>>),
}

/// Accumulates `(record, payload)` pairs, sorts in-memory or spills to disk
/// when the byte budget is exceeded. Sort key is extracted from the record
/// only — the payload rides along.
pub struct SortBuffer<P> {
    pairs: Vec<(Record, P)>,
    sort_by: Vec<SortField>,
    bytes_used: usize,
    spill_threshold: usize,
    spill_dir: Option<PathBuf>,
    spill_files: Vec<SpillFile<P>>,
    schema: Arc<Schema>,
}

impl<P: Serialize + DeserializeOwned> SortBuffer<P> {
    pub fn new(
        sort_by: Vec<SortField>,
        spill_threshold: usize,
        spill_dir: Option<PathBuf>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            pairs: Vec::new(),
            sort_by,
            bytes_used: 0,
            spill_threshold,
            spill_dir,
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

    /// Sort the current in-memory pairs and write them to a spill file.
    /// Clears the buffer and resets the byte counter.
    pub fn sort_and_spill(&mut self) -> Result<(), SpillError> {
        if self.pairs.is_empty() {
            return Ok(());
        }

        let sort_by = &self.sort_by;
        self.pairs
            .sort_by(|(a, _), (b, _)| compare_records_by_fields(a, b, sort_by));

        let mut writer: SpillWriter<P> =
            SpillWriter::new(self.schema.clone(), self.spill_dir.as_deref())?;
        for (record, payload) in self.pairs.drain(..) {
            writer.write_pair(&record, &payload)?;
        }
        let spill_file = writer.finish()?;
        self.spill_files.push(spill_file);
        self.bytes_used = 0;
        Ok(())
    }

    /// Finish the sort buffer. If pairs were spilled, remaining in-memory
    /// pairs are also spilled. Returns either sorted in-memory pairs or a
    /// list of sorted spill files for merge.
    pub fn finish(mut self) -> Result<SortedOutput<P>, SpillError> {
        if self.spill_files.is_empty() {
            let sort_by = &self.sort_by;
            self.pairs
                .sort_by(|(a, _), (b, _)| compare_records_by_fields(a, b, sort_by));
            Ok(SortedOutput::InMemory(self.pairs))
        } else {
            if !self.pairs.is_empty() {
                self.sort_and_spill()?;
            }
            Ok(SortedOutput::Spilled(self.spill_files))
        }
    }

    /// Current estimated memory usage in bytes.
    pub fn bytes_used(&self) -> usize {
        self.bytes_used
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
            order: crate::config::SortOrder::Asc,
            null_order: None,
        }]
    }

    #[test]
    fn test_sort_buffer_push_tracks_bytes() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> =
            SortBuffer::new(sort_by_value_asc(), 1_000_000, None, schema.clone());
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
            SortBuffer::new(sort_by_value_asc(), 100, None, schema.clone());
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
            SortBuffer::new(sort_by_value_asc(), 1_000_000, None, schema.clone());
        buf.push(make_record(&schema, "Charlie", 30), ());
        buf.push(make_record(&schema, "Alice", 10), ());
        buf.push(make_record(&schema, "Bob", 20), ());

        match buf.finish().unwrap() {
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
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by_value_asc(), 1, None, schema.clone()); // threshold=1 → spill immediately
        buf.push(make_record(&schema, "Alice", 10), ());
        assert!(buf.should_spill());
        buf.sort_and_spill().unwrap();
        assert_eq!(buf.bytes_used(), 0);
        assert_eq!(buf.pairs.len(), 0);
    }

    #[test]
    fn test_sort_buffer_finish_spilled_returns_files() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by_value_asc(), 1, None, schema.clone());

        // Push and spill twice
        buf.push(make_record(&schema, "B", 20), ());
        buf.sort_and_spill().unwrap();
        buf.push(make_record(&schema, "A", 10), ());
        buf.sort_and_spill().unwrap();
        // Push one more without spilling — finish() will spill it
        buf.push(make_record(&schema, "C", 30), ());

        match buf.finish().unwrap() {
            SortedOutput::Spilled(files) => {
                assert_eq!(files.len(), 3); // 2 manual spills + 1 from finish()
            }
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_buffer_spill_files_are_sorted() {
        let schema = test_schema();
        let mut buf: SortBuffer<()> = SortBuffer::new(sort_by_value_asc(), 1, None, schema.clone());
        buf.push(make_record(&schema, "C", 30), ());
        buf.push(make_record(&schema, "A", 10), ());
        buf.push(make_record(&schema, "B", 20), ());
        buf.sort_and_spill().unwrap();

        // Read back and verify sorted
        match buf.finish().unwrap() {
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
            SortBuffer::new(sort_by_value_asc(), 1_000_000, None, schema.clone());
        buf.push(make_record(&schema, "Charlie", 30), 100);
        buf.push(make_record(&schema, "Alice", 10), 200);
        buf.push(make_record(&schema, "Bob", 20), 300);
        match buf.finish().unwrap() {
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
        // Pattern B gate: payload survives the NDJSON+LZ4 spill envelope.
        let schema = test_schema();
        let mut buf: SortBuffer<u64> =
            SortBuffer::new(sort_by_value_asc(), 1, None, schema.clone());
        buf.push(make_record(&schema, "C", 30), 100);
        buf.push(make_record(&schema, "A", 10), 200);
        buf.push(make_record(&schema, "B", 20), 300);
        buf.sort_and_spill().unwrap();
        match buf.finish().unwrap() {
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
    fn test_sort_buffer_empty_returns_empty() {
        let schema = test_schema();
        let buf: SortBuffer<()> = SortBuffer::new(sort_by_value_asc(), 1_000_000, None, schema);
        match buf.finish().unwrap() {
            SortedOutput::InMemory(pairs) => assert!(pairs.is_empty()),
            SortedOutput::Spilled(_) => panic!("expected InMemory"),
        }
    }
}
