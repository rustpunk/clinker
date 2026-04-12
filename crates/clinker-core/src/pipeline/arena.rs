//! Arena: columnar-projected record storage for Phase 1 indexing.
//!
//! Stores only the fields needed by window expressions. Built by streaming
//! a `FormatReader` and projecting to the required field subset.

use std::sync::Arc;

use clinker_format::traits::FormatReader;
use clinker_record::{MinimalRecord, RecordStorage, Schema, Value};

/// Columnar-projected record storage for Phase 1 indexing.
/// Stores only the fields needed by window expressions.
///
/// Immutable after construction. `Send + Sync` for sharing across
/// rayon workers during Phase 2.
#[derive(Debug)]
pub struct Arena {
    schema: Arc<Schema>,
    records: Vec<MinimalRecord>,
}

impl Arena {
    /// Create an empty Arena with the given schema (for sources with no indices).
    pub fn empty(schema: Arc<Schema>) -> Self {
        Arena {
            schema,
            records: Vec::new(),
        }
    }

    /// Create an Arena from pre-built parts (used by MultiRecordDispatcher).
    pub fn from_parts(schema: Arc<Schema>, records: Vec<MinimalRecord>) -> Self {
        Arena { schema, records }
    }

    /// Stream records from reader, storing only the named fields.
    ///
    /// `fields`: field names to project into the Arena (from `IndexSpec.arena_fields`).
    /// `memory_limit`: maximum bytes the Arena may consume. If exceeded during
    /// construction, returns `ArenaError::MemoryBudgetExceeded`.
    pub fn build(
        reader: &mut dyn FormatReader,
        fields: &[String],
        memory_limit: usize,
        shutdown: Option<&super::shutdown::ShutdownToken>,
    ) -> Result<Self, ArenaError> {
        let source_schema = reader.schema()?;

        // Build the projected schema (only the requested fields, in order)
        let projected_columns: Vec<Box<str>> =
            fields.iter().map(|f| f.clone().into_boxed_str()).collect();
        let schema = Arc::new(Schema::new(projected_columns));

        // Map field names to their column indices in the source schema
        let field_indices: Vec<Option<usize>> =
            fields.iter().map(|f| source_schema.index(f)).collect();

        let mut records = Vec::new();
        let mut bytes_used: usize = 0;
        let mut records_since_check: u32 = 0;

        while let Some(record) = reader.next_record()? {
            // Check shutdown flag every 4096 records to stay responsive to SIGINT
            records_since_check += 1;
            if records_since_check >= 4096 {
                records_since_check = 0;
                if shutdown.is_some_and(|t| t.is_requested()) {
                    return Err(ArenaError::ShutdownRequested);
                }
            }
            // Project: extract only the requested fields
            let projected_values: Vec<Value> = field_indices
                .iter()
                .map(|idx| {
                    idx.and_then(|i| record.values().get(i).cloned())
                        .unwrap_or(Value::Null)
                })
                .collect();

            let minimal = MinimalRecord::new(projected_values);
            bytes_used += estimated_size(&minimal);

            if bytes_used > memory_limit {
                return Err(ArenaError::MemoryBudgetExceeded {
                    used: bytes_used,
                    limit: memory_limit,
                });
            }

            records.push(minimal);
        }

        Ok(Arena { schema, records })
    }

    /// Schema of the projected fields stored in this Arena.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Number of records in this Arena.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// Whether the Arena is empty.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}

impl RecordStorage for Arena {
    fn resolve_field(&self, index: u32, name: &str) -> Option<Value> {
        let col = self.schema.index(name)?;
        self.records.get(index as usize)?.get(col).cloned()
    }

    fn resolve_qualified(&self, _index: u32, _source: &str, _field: &str) -> Option<Value> {
        None // Arena is single-source; qualified lookups go through PartitionLookup
    }

    fn available_fields(&self, _index: u32) -> Vec<&str> {
        self.schema.columns().iter().map(|s| &**s).collect()
    }

    fn record_count(&self) -> u32 {
        u32::try_from(self.records.len()).expect("Arena exceeds u32::MAX records")
    }
}

/// Estimate the in-memory size of a MinimalRecord (rough heuristic).
fn estimated_size(record: &MinimalRecord) -> usize {
    let base = std::mem::size_of::<MinimalRecord>();
    let fields: usize = (0..record.len())
        .map(|i| match record.get(i) {
            Some(Value::String(s)) => std::mem::size_of::<Value>() + s.len(),
            _ => std::mem::size_of::<Value>(),
        })
        .sum();
    base + fields
}

/// Errors from Arena construction.
#[derive(Debug)]
pub enum ArenaError {
    /// Arena exceeded the configured memory budget during construction.
    MemoryBudgetExceeded { used: usize, limit: usize },
    /// Error reading from the source format reader.
    ReadError(clinker_format::error::FormatError),
    /// Shutdown was requested during Arena construction (SIGINT/SIGTERM).
    ShutdownRequested,
}

impl std::fmt::Display for ArenaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ArenaError::MemoryBudgetExceeded { used, limit } => {
                write!(
                    f,
                    "Arena requires ~{}MB, exceeds memory_limit of {}MB. \
                     Reduce input size or increase --memory-limit.",
                    used / (1024 * 1024),
                    limit / (1024 * 1024),
                )
            }
            ArenaError::ReadError(e) => write!(f, "arena read error: {}", e),
            ArenaError::ShutdownRequested => {
                write!(f, "arena construction interrupted by shutdown signal")
            }
        }
    }
}

impl std::error::Error for ArenaError {}

impl From<clinker_format::error::FormatError> for ArenaError {
    fn from(e: clinker_format::error::FormatError) -> Self {
        ArenaError::ReadError(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
    use clinker_record::{FieldResolver, RecordView};

    fn make_csv_reader(csv: &str) -> CsvReader<&[u8]> {
        CsvReader::from_reader(
            csv.as_bytes(),
            CsvReaderConfig {
                delimiter: b',',
                quote_char: b'"',
                has_header: true,
            },
        )
    }

    #[test]
    fn test_arena_build_from_csv() {
        let _csv = "dept,amount,name\nA,100,Alice\nB,200,Bob\nA,150,Carol\n";
        // Repeat to get ~100 rows
        let mut big_csv = "dept,amount,name\n".to_string();
        for i in 0..100 {
            big_csv.push_str(&format!("D{},{},Name{}\n", i % 3, i * 10, i));
        }
        let mut reader = make_csv_reader(&big_csv);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
        )
        .unwrap();
        assert_eq!(arena.record_count(), 100);
    }

    #[test]
    fn test_arena_field_projection() {
        let csv = "dept,amount,name,extra\nA,100,Alice,X\nB,200,Bob,Y\n";
        let mut reader = make_csv_reader(csv);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
        )
        .unwrap();

        // Schema has only projected fields
        assert_eq!(arena.schema().column_count(), 2);
        assert!(arena.schema().contains("dept"));
        assert!(arena.schema().contains("amount"));

        // Excluded fields return None
        assert!(arena.resolve_field(0, "name").is_none());
        assert!(arena.resolve_field(0, "extra").is_none());

        // Projected fields resolve correctly
        assert_eq!(
            arena.resolve_field(0, "dept"),
            Some(Value::String("A".into()))
        );
    }

    #[test]
    fn test_arena_record_view_resolve() {
        let csv = "dept,amount\nA,100\nB,200\nC,300\nD,400\nE,500\nF,600\n";
        let mut reader = make_csv_reader(csv);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
        )
        .unwrap();

        let view = RecordView::new(&arena, 5);
        assert_eq!(view.resolve("dept"), Some(Value::String("F".into())));
        assert_eq!(view.resolve("amount"), Some(Value::String("600".into())));
    }

    #[test]
    fn test_arena_record_view_missing_field() {
        let csv = "dept,amount\nA,100\n";
        let mut reader = make_csv_reader(csv);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
        )
        .unwrap();

        let view = RecordView::new(&arena, 0);
        assert_eq!(view.resolve("nonexistent"), None);
    }

    #[test]
    fn test_arena_record_view_zero_alloc() {
        assert_eq!(
            std::mem::size_of::<RecordView<'_, Arena>>(),
            16,
            "RecordView<Arena> should be 16 bytes (pointer + u32 + padding)"
        );
    }

    #[test]
    fn test_arena_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Arena>();
    }

    #[test]
    fn test_arena_empty_input() {
        let csv = "dept,amount\n";
        let mut reader = make_csv_reader(csv);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
        )
        .unwrap();
        assert_eq!(arena.record_count(), 0);
        assert_eq!(arena.schema().column_count(), 2);
    }

    #[test]
    fn test_arena_memory_budget_exceeded() {
        let mut csv = "dept,amount\n".to_string();
        for i in 0..1000 {
            csv.push_str(&format!("Department_{},{}00\n", i, i));
        }
        let mut reader = make_csv_reader(&csv);
        let result = Arena::build(&mut reader, &["dept".into(), "amount".into()], 100, None);
        assert!(result.is_err());
        match result.unwrap_err() {
            ArenaError::MemoryBudgetExceeded { used, limit } => {
                assert!(used > 100);
                assert_eq!(limit, 100);
            }
            other => panic!("Expected MemoryBudgetExceeded, got: {:?}", other),
        }
    }
}
