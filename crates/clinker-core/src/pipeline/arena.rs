//! Arena: columnar-projected record storage for Phase 1 indexing.
//!
//! Stores only the fields needed by window expressions. Built by streaming
//! a `FormatReader` and projecting to the required field subset.

use std::sync::Arc;

use clinker_format::traits::FormatReader;
use clinker_record::{MinimalRecord, RecordStorage, Schema, SchemaBuilder, Value};

use super::memory::MemoryBudget;

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
    /// `memory_limit`: hard byte limit threaded through this call only — the
    /// caller's per-arena cap, distinct from the cumulative budget tracked
    /// on `MemoryBudget`. Each admitted record's projected size charges
    /// the cumulative `arena_bytes_charged` counter via `budget`. If
    /// either the per-call `memory_limit` or the cumulative budget tips,
    /// returns `ArenaError::MemoryBudgetExceeded`.
    pub fn build(
        reader: &mut dyn FormatReader,
        fields: &[String],
        memory_limit: usize,
        shutdown: Option<&super::shutdown::ShutdownToken>,
        budget: &mut MemoryBudget,
    ) -> Result<Self, ArenaError> {
        let source_schema = reader.schema()?;

        // Build the projected schema (only the requested fields, in order)
        let schema = fields
            .iter()
            .map(|f| f.clone().into_boxed_str())
            .collect::<SchemaBuilder>()
            .build();

        // Map field names to their column indices in the source schema
        let field_indices: Vec<Option<usize>> =
            fields.iter().map(|f| source_schema.index(f)).collect();

        let mut records = Vec::new();
        let mut local_bytes_used: usize = 0;
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
            // D5 defense-in-depth: every record fed into the Arena must
            // carry the same schema the Arena was initialized against.
            // `field_indices` was computed once at the top of this
            // function; a mid-stream Arc or column-list shift would
            // silently misproject `values()` positions. Fast path is
            // `Arc::ptr_eq`; the structural fallback exists for
            // streaming drift surfaces (IPC boundaries, spill rehydrate)
            // where the upstream emits a fresh Arc whose columns are
            // still structurally identical.
            if !Arc::ptr_eq(record.schema(), &source_schema)
                && record.schema().columns() != source_schema.columns()
            {
                return Err(ArenaError::SchemaMismatch {
                    expected: Arc::clone(&source_schema),
                    actual: Arc::clone(record.schema()),
                });
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
            let size = estimated_size(&minimal);
            local_bytes_used += size;

            if local_bytes_used > memory_limit {
                return Err(ArenaError::MemoryBudgetExceeded {
                    used: local_bytes_used,
                    limit: memory_limit,
                });
            }
            if budget.charge_arena_bytes(size as u64) {
                return Err(ArenaError::MemoryBudgetExceeded {
                    used: budget.arena_bytes_charged() as usize,
                    limit: budget.hard_limit() as usize,
                });
            }

            records.push(minimal);
        }

        Ok(Arena { schema, records })
    }

    /// Materialize an Arena from already-realized records — the shape
    /// `node_buffers[upstream]` produces at the upstream operator's
    /// dispatch-arm exit. Projects each record's values onto `fields`
    /// in order using the column index map computed from
    /// `anchor_schema`; missing fields produce `Value::Null` (mirroring
    /// the source-rooted path's behavior).
    ///
    /// `budget` is the executor's ctx-owned `MemoryBudget`. Each
    /// admitted record's projected `MinimalRecord` size is charged to
    /// the cumulative arena counter; on overflow the call returns
    /// `ArenaError::MemoryBudgetExceeded` with the totals at the
    /// moment of failure. The shared budget is what makes the source
    /// arena + N node-rooted arenas fit under one declared limit.
    pub fn from_records(
        rows: &[(clinker_record::Record, u64)],
        fields: &[String],
        anchor_schema: &Arc<Schema>,
        budget: &mut MemoryBudget,
    ) -> Result<Self, ArenaError> {
        let schema: Arc<Schema> = fields
            .iter()
            .map(|f| f.clone().into_boxed_str())
            .collect::<SchemaBuilder>()
            .build();
        let field_indices: Vec<Option<usize>> =
            fields.iter().map(|f| anchor_schema.index(f)).collect();
        let records =
            project_records_into_minimal(rows.iter().map(|(r, _)| r), &field_indices, budget)?;
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
    fn resolve_field(&self, index: u32, name: &str) -> Option<&Value> {
        let col = self.schema.index(name)?;
        self.records.get(index as usize)?.get(col)
    }

    fn resolve_qualified(&self, _index: u32, _source: &str, _field: &str) -> Option<&Value> {
        None // Arena is single-source; qualified lookups go through PartitionLookup
    }

    fn available_fields(&self, _index: u32) -> Vec<&str> {
        self.schema.columns().iter().map(|s| &**s).collect()
    }

    fn record_count(&self) -> u32 {
        u32::try_from(self.records.len()).expect("Arena exceeds u32::MAX records")
    }
}

/// Project a stream of `Record`-shaped inputs onto `field_indices`
/// (one entry per output column, or `None` for a missing column),
/// charging each admitted `MinimalRecord` against the shared
/// `MemoryBudget`. Used by both `Arena::from_records` (post-operator
/// node-rooted arenas) and any caller that already holds materialized
/// records and wants the same projection + budget-charge contract
/// `Arena::build` provides at the source-stream boundary.
///
/// On budget overflow returns `ArenaError::MemoryBudgetExceeded` with
/// the cumulative bytes used and the configured hard limit.
fn project_records_into_minimal<'a, I>(
    records: I,
    field_indices: &[Option<usize>],
    budget: &mut MemoryBudget,
) -> Result<Vec<MinimalRecord>, ArenaError>
where
    I: IntoIterator<Item = &'a clinker_record::Record>,
{
    let mut out: Vec<MinimalRecord> = Vec::new();
    for record in records {
        let projected: Vec<Value> = field_indices
            .iter()
            .map(|idx| {
                idx.and_then(|i| record.values().get(i).cloned())
                    .unwrap_or(Value::Null)
            })
            .collect();
        let minimal = MinimalRecord::new(projected);
        let size = estimated_size(&minimal) as u64;
        if budget.charge_arena_bytes(size) {
            return Err(ArenaError::MemoryBudgetExceeded {
                used: budget.arena_bytes_charged() as usize,
                limit: budget.hard_limit() as usize,
            });
        }
        out.push(minimal);
    }
    Ok(out)
}

/// Estimate the in-memory size of a MinimalRecord (rough heuristic).
pub(crate) fn estimated_size(record: &MinimalRecord) -> usize {
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
    /// Record arrived with a schema whose column list diverges from the
    /// reader schema the Arena was initialized against. Arena storage
    /// depends on column-position alignment, so mid-stream drift would
    /// corrupt projected values — raise E314 at the ingest boundary.
    SchemaMismatch {
        expected: Arc<Schema>,
        actual: Arc<Schema>,
    },
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
            ArenaError::SchemaMismatch { expected, actual } => {
                let exp = expected
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("{c}@{i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let act = actual
                    .columns()
                    .iter()
                    .enumerate()
                    .map(|(i, c)| format!("{c}@{i}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    f,
                    "E314 Schema mismatch at operator 'arena' (kind=arena, input from 'source'): \
                     expected {} columns: [{exp}] record has {} columns: [{act}]",
                    expected.column_count(),
                    actual.column_count(),
                )
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
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
            &mut budget,
        )
        .unwrap();
        assert_eq!(arena.record_count(), 100);
    }

    #[test]
    fn test_arena_field_projection() {
        let csv = "dept,amount,name,extra\nA,100,Alice,X\nB,200,Bob,Y\n";
        let mut reader = make_csv_reader(csv);
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
            &mut budget,
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
            Some(&Value::String("A".into()))
        );
    }

    #[test]
    fn test_arena_record_view_resolve() {
        let csv = "dept,amount\nA,100\nB,200\nC,300\nD,400\nE,500\nF,600\n";
        let mut reader = make_csv_reader(csv);
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
            &mut budget,
        )
        .unwrap();

        let view = RecordView::new(&arena, 5);
        assert_eq!(view.resolve("dept"), Some(&Value::String("F".into())));
        assert_eq!(view.resolve("amount"), Some(&Value::String("600".into())));
    }

    #[test]
    fn test_arena_record_view_missing_field() {
        let csv = "dept,amount\nA,100\n";
        let mut reader = make_csv_reader(csv);
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
            &mut budget,
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
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        let arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
            &mut budget,
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
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        let result = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            100,
            None,
            &mut budget,
        );
        assert!(result.is_err());
        match result.unwrap_err() {
            ArenaError::MemoryBudgetExceeded { used, limit } => {
                assert!(used > 100);
                assert_eq!(limit, 100);
            }
            other => panic!("Expected MemoryBudgetExceeded, got: {:?}", other),
        }
    }

    /// D5 defense-in-depth: feeding records with a divergent schema into
    /// one Arena raises E314 (`ArenaError::SchemaMismatch`). Mocks a
    /// reader whose first record establishes a 2-column schema and
    /// whose second record carries a 3-column schema — the Arena loop
    /// catches the drift before projection misaligns.
    #[test]
    fn test_arena_entry_asserts_schema_homogeneity() {
        use clinker_format::error::FormatError;
        use clinker_format::traits::FormatReader;
        use clinker_record::Record;

        struct HeterogenousReader {
            first_schema: Arc<Schema>,
            second_schema: Arc<Schema>,
            emitted: usize,
        }

        impl FormatReader for HeterogenousReader {
            fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
                Ok(Arc::clone(&self.first_schema))
            }
            fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
                match self.emitted {
                    0 => {
                        self.emitted += 1;
                        Ok(Some(Record::new(
                            Arc::clone(&self.first_schema),
                            vec![Value::String("A".into()), Value::String("100".into())],
                        )))
                    }
                    1 => {
                        self.emitted += 1;
                        // Divergent schema — three columns vs the first
                        // record's two. The Arena's projection would
                        // misalign `values()` positions without the
                        // defense-in-depth guard.
                        Ok(Some(Record::new(
                            Arc::clone(&self.second_schema),
                            vec![
                                Value::String("B".into()),
                                Value::String("200".into()),
                                Value::String("extra".into()),
                            ],
                        )))
                    }
                    _ => Ok(None),
                }
            }
        }

        let first_schema = Arc::new(Schema::new(vec!["dept".into(), "amount".into()]));
        let second_schema = Arc::new(Schema::new(vec![
            "dept".into(),
            "amount".into(),
            "extra".into(),
        ]));
        let mut reader = HeterogenousReader {
            first_schema: Arc::clone(&first_schema),
            second_schema: Arc::clone(&second_schema),
            emitted: 0,
        };
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);
        let result = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
            &mut budget,
        );
        match result {
            Err(ArenaError::SchemaMismatch { expected, actual }) => {
                assert!(Arc::ptr_eq(&expected, &first_schema));
                assert!(Arc::ptr_eq(&actual, &second_schema));
                let rendered = format!("{}", ArenaError::SchemaMismatch { expected, actual });
                assert!(rendered.contains("E314"));
                assert!(rendered.contains("arena"));
            }
            other => panic!("expected SchemaMismatch, got: {other:?}"),
        }
    }

    /// Shared `MemoryBudget` across a source-stream `Arena::build` and
    /// a downstream `Arena::from_records`: when their combined byte
    /// charge crosses the cumulative limit, the second build raises
    /// `MemoryBudgetExceeded` even though its own per-call
    /// `memory_limit` is unbounded. This pins the cross-arena
    /// accounting that makes node-rooted post-aggregate windows fit
    /// under the user-declared pipeline budget.
    #[test]
    fn shared_memory_budget_aggregates_across_source_and_node_arenas() {
        use clinker_record::Record;

        // Source arena: 100 records, each ~80 bytes after projection.
        let mut src_csv = String::from("dept,amount\n");
        for i in 0..100 {
            src_csv.push_str(&format!("dept_{i:03},{}\n", i * 10));
        }
        let mut reader = make_csv_reader(&src_csv);
        // Cumulative budget sized just above the source arena so the
        // first build admits 100 records and the second build's
        // node-rooted projection trips on the cumulative ceiling.
        let cumulative_limit = 12_000u64;
        let mut shared_budget = MemoryBudget::new(cumulative_limit, 0.80);
        let src_arena = Arena::build(
            &mut reader,
            &["dept".into(), "amount".into()],
            usize::MAX,
            None,
            &mut shared_budget,
        )
        .expect("source arena fits under the shared budget");
        assert_eq!(src_arena.record_count(), 100);
        let after_source_charge = shared_budget.arena_bytes_charged();
        assert!(
            after_source_charge > 0,
            "source arena must charge bytes against the shared budget; \
             got 0 — budget plumbing regressed"
        );
        assert!(
            after_source_charge <= cumulative_limit,
            "source arena alone must fit under the shared budget"
        );

        // Build a second wave of materialized records and project them
        // through `from_records`. Each record is small but their
        // cumulative charge plus the source arena's already-charged
        // bytes pushes past `cumulative_limit`. The error MUST come
        // from the shared budget, not from the per-call cap.
        let anchor_schema = Arc::new(Schema::new(vec!["dept".into(), "amount".into()]));
        let mut rows: Vec<(Record, u64)> = Vec::with_capacity(200);
        for i in 0..200 {
            rows.push((
                Record::new(
                    Arc::clone(&anchor_schema),
                    vec![
                        Value::String(format!("dept_{i:03}").into()),
                        Value::String(format!("{}", i * 10).into()),
                    ],
                ),
                i as u64,
            ));
        }
        let result = Arena::from_records(
            &rows,
            &["dept".into(), "amount".into()],
            &anchor_schema,
            &mut shared_budget,
        );
        match result {
            Err(ArenaError::MemoryBudgetExceeded { used, limit }) => {
                assert!(
                    used as u64 > cumulative_limit,
                    "shared budget overflow must report cumulative bytes \
                     above the limit; used={used} limit={limit}"
                );
            }
            other => panic!("expected MemoryBudgetExceeded from shared budget; got: {other:?}"),
        }
    }
}
