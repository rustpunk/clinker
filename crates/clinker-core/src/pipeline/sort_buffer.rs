//! Sort buffer: accumulates records, sorts in-memory or spills to disk.
//!
//! Used at both source-sort and output-sort intercept points. The buffer
//! tracks its own memory usage via `Value::heap_size()` and spills sorted
//! chunks to NDJSON+LZ4 temp files when the budget is exceeded.

use std::path::PathBuf;
use std::sync::Arc;

use clinker_record::{Record, Schema};

use crate::config::SortField;
use crate::pipeline::sort::compare_records_by_fields;
use crate::pipeline::spill::{SpillError, SpillFile, SpillWriter};

/// Result of finishing a sort buffer: either all records fit in memory,
/// or some were spilled to disk.
pub enum SortedOutput {
    /// All records fit in memory. Sorted and ready to iterate.
    InMemory(Vec<Record>),
    /// Records were spilled to sorted temp files. Must be merged via LoserTree.
    Spilled(Vec<SpillFile>),
}

/// Accumulates records, sorts in-memory or spills to disk when budget exceeded.
pub struct SortBuffer {
    records: Vec<Record>,
    sort_by: Vec<SortField>,
    bytes_used: usize,
    spill_threshold: usize,
    spill_dir: Option<PathBuf>,
    spill_files: Vec<SpillFile>,
    schema: Arc<Schema>,
}

impl SortBuffer {
    pub fn new(
        sort_by: Vec<SortField>,
        spill_threshold: usize,
        spill_dir: Option<PathBuf>,
        schema: Arc<Schema>,
    ) -> Self {
        Self {
            records: Vec::new(),
            sort_by,
            bytes_used: 0,
            spill_threshold,
            spill_dir,
            spill_files: Vec::new(),
            schema,
        }
    }

    /// Push a record into the buffer. Returns true if the buffer should be spilled.
    pub fn push(&mut self, record: Record) {
        let size = std::mem::size_of::<Record>() + record.estimated_heap_size();
        self.bytes_used += size;
        self.records.push(record);
    }

    /// Check if the buffer has exceeded its memory threshold.
    pub fn should_spill(&self) -> bool {
        self.bytes_used > 0 && self.bytes_used >= self.spill_threshold
    }

    /// Sort the current in-memory records and write them to a spill file.
    /// Clears the buffer and resets the byte counter.
    pub fn sort_and_spill(&mut self) -> Result<(), SpillError> {
        if self.records.is_empty() {
            return Ok(());
        }

        let sort_by = &self.sort_by;
        self.records
            .sort_by(|a, b| compare_records_by_fields(a, b, sort_by));

        let mut writer = SpillWriter::new(self.schema.clone(), self.spill_dir.as_deref())?;
        for record in self.records.drain(..) {
            writer.write_record(&record)?;
        }
        let spill_file = writer.finish()?;
        self.spill_files.push(spill_file);
        self.bytes_used = 0;
        Ok(())
    }

    /// Finish the sort buffer. If records were spilled, the remaining in-memory
    /// records are also spilled. Returns either sorted in-memory records or
    /// a list of sorted spill files for merge.
    pub fn finish(mut self) -> Result<SortedOutput, SpillError> {
        if self.spill_files.is_empty() {
            // Everything fits in memory — sort and return
            let sort_by = &self.sort_by;
            self.records
                .sort_by(|a, b| compare_records_by_fields(a, b, sort_by));
            Ok(SortedOutput::InMemory(self.records))
        } else {
            // Spill remaining records if any
            if !self.records.is_empty() {
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
        let mut buf = SortBuffer::new(sort_by_value_asc(), 1_000_000, None, schema.clone());
        assert_eq!(buf.bytes_used(), 0);
        buf.push(make_record(&schema, "Alice", 1));
        assert!(buf.bytes_used() > 0);
        let first = buf.bytes_used();
        buf.push(make_record(&schema, "Bob", 2));
        assert!(buf.bytes_used() > first);
    }

    #[test]
    fn test_sort_buffer_should_spill_at_threshold() {
        let schema = test_schema();
        // Very small threshold — should spill after a few records
        let mut buf = SortBuffer::new(sort_by_value_asc(), 100, None, schema.clone());
        assert!(!buf.should_spill());
        // Push records until we exceed 100 bytes
        for i in 0..10 {
            buf.push(make_record(&schema, &format!("name_{i}"), i));
            if buf.should_spill() {
                return; // Test passes — spill triggered
            }
        }
        panic!("should_spill() never triggered with 100-byte threshold");
    }

    #[test]
    fn test_sort_buffer_in_memory_sort() {
        let schema = test_schema();
        let mut buf = SortBuffer::new(sort_by_value_asc(), 1_000_000, None, schema.clone());
        buf.push(make_record(&schema, "Charlie", 30));
        buf.push(make_record(&schema, "Alice", 10));
        buf.push(make_record(&schema, "Bob", 20));

        match buf.finish().unwrap() {
            SortedOutput::InMemory(records) => {
                assert_eq!(records.len(), 3);
                assert_eq!(records[0].get("value"), Some(&Value::Integer(10)));
                assert_eq!(records[1].get("value"), Some(&Value::Integer(20)));
                assert_eq!(records[2].get("value"), Some(&Value::Integer(30)));
            }
            SortedOutput::Spilled(_) => panic!("expected InMemory"),
        }
    }

    #[test]
    fn test_sort_buffer_spill_produces_spill_files() {
        let schema = test_schema();
        let mut buf = SortBuffer::new(sort_by_value_asc(), 1, None, schema.clone()); // threshold=1 → spill immediately
        buf.push(make_record(&schema, "Alice", 10));
        assert!(buf.should_spill());
        buf.sort_and_spill().unwrap();
        assert_eq!(buf.bytes_used(), 0);
        assert_eq!(buf.records.len(), 0);
    }

    #[test]
    fn test_sort_buffer_finish_spilled_returns_files() {
        let schema = test_schema();
        let mut buf = SortBuffer::new(sort_by_value_asc(), 1, None, schema.clone());

        // Push and spill twice
        buf.push(make_record(&schema, "B", 20));
        buf.sort_and_spill().unwrap();
        buf.push(make_record(&schema, "A", 10));
        buf.sort_and_spill().unwrap();
        // Push one more without spilling — finish() will spill it
        buf.push(make_record(&schema, "C", 30));

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
        let mut buf = SortBuffer::new(sort_by_value_asc(), 1, None, schema.clone());
        buf.push(make_record(&schema, "C", 30));
        buf.push(make_record(&schema, "A", 10));
        buf.push(make_record(&schema, "B", 20));
        buf.sort_and_spill().unwrap();

        // Read back and verify sorted
        match buf.finish().unwrap() {
            SortedOutput::Spilled(files) => {
                let reader = files[0].reader().unwrap();
                let records: Vec<_> = reader.map(|r| r.unwrap()).collect();
                assert_eq!(records.len(), 3);
                assert_eq!(records[0].get("value"), Some(&Value::Integer(10)));
                assert_eq!(records[1].get("value"), Some(&Value::Integer(20)));
                assert_eq!(records[2].get("value"), Some(&Value::Integer(30)));
            }
            SortedOutput::InMemory(_) => panic!("expected Spilled"),
        }
    }

    #[test]
    fn test_sort_buffer_empty_returns_empty() {
        let schema = test_schema();
        let buf = SortBuffer::new(sort_by_value_asc(), 1_000_000, None, schema);
        match buf.finish().unwrap() {
            SortedOutput::InMemory(records) => assert!(records.is_empty()),
            SortedOutput::Spilled(_) => panic!("expected InMemory"),
        }
    }
}
