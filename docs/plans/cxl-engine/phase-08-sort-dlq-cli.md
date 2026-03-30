# Phase 8: Sort, DLQ Polish, CLI Completion

**Status:** 🔲 Not Started
**Depends on:** Phase 6 (spill infrastructure), Phase 7 (all format readers/writers)
**Entry criteria:** Spill-to-disk infrastructure operational; CSV, JSON, XML readers/writers passing all Phase 7 tests
**Exit criteria:** External merge sort produces correctly ordered output with spill; DLQ writer emits all spec columns; `clinker --explain` prints execution plan; all CLI flags implemented; `cargo test -p clinker -p cxl-cli` passes 50+ tests

---

## Tasks

### Task 8.1: External Merge Sort + Loser Tree
**Status:** 🔲 Not Started
**Blocked by:** Phase 6 exit criteria (spill infrastructure), Phase 7 exit criteria (format writers)

**Description:**
Implement in-memory sort for small outputs and external merge sort with loser tree for
outputs exceeding the memory budget. Sort chunks are spilled as NDJSON+LZ4 temp files and
merged via a k-way loser tree comparator.

**Implementation notes:**
- `sort_output` config: `Vec<SortKey>` where `SortKey { field: String, order: Asc | Desc, nulls: First | Last }`.
- Compound sort keys: compare fields left-to-right, first non-equal comparison wins.
- In-memory path: if all output records fit within remaining memory budget, collect into `Vec<Record>`, sort with `slice::sort_by` (stable), write directly.
- Spill path: accumulate records up to chunk size (budget / estimated record size), sort chunk in-place, serialize to NDJSON+LZ4 temp file via `lz4_flex`.
- `loser_tree.rs` in `clinker-core`: k-way merge comparator. Tree of `k` leaves, `O(log k)` comparisons per output record. Each leaf holds a buffered reader over one spill file.
- `k_max = 16`: if more than 16 spill files, cascade merge — merge groups of 16 into intermediate files, then merge intermediates.
- Stable sort guarantee: include a sequence number (original input order) as tie-breaker in all comparisons so equal keys preserve input order.
- Temp files created in system temp dir with `tempfile` crate; cleaned up on drop via RAII wrapper.

**Acceptance criteria:**
- [ ] In-memory sort produces correctly ordered output for all sort key combinations
- [ ] Spill path triggers when output exceeds memory budget
- [ ] Loser tree merges k sorted streams with `O(log k)` comparisons per record
- [ ] Cascade merge handles >16 spill files correctly
- [ ] Compound sort keys compare left-to-right with correct null placement
- [ ] Stable sort: equal keys preserve original input order
- [ ] Temp files cleaned up after sort completes (including on error/panic)

**Required unit tests (must pass before Task 8.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_sort_single_field_asc` | 100 records sorted ascending by one integer field | ⛔ Hard gate |
| `test_sort_single_field_desc` | 100 records sorted descending by one string field | ⛔ Hard gate |
| `test_sort_compound_keys` | Sort by (dept ASC, salary DESC) — secondary key breaks ties | ⛔ Hard gate |
| `test_sort_nulls_first` | Null values sort before all non-null values when `nulls: First` | ⛔ Hard gate |
| `test_sort_nulls_last` | Null values sort after all non-null values when `nulls: Last` | ⛔ Hard gate |
| `test_sort_stable_equal_keys` | Records with identical sort keys preserve original input order | ⛔ Hard gate |
| `test_loser_tree_2way_merge` | Loser tree correctly merges 2 sorted streams | ⛔ Hard gate |
| `test_loser_tree_16way_merge` | Loser tree correctly merges 16 sorted streams (k_max boundary) | ⛔ Hard gate |
| `test_loser_tree_single_stream` | Degenerate case: 1 stream passes through unchanged | ⛔ Hard gate |
| `test_sort_spill_triggers_on_budget` | With 1KB memory budget and 10KB data, spill files are created | ⛔ Hard gate |
| `test_sort_cascade_merge` | 32 spill files triggers cascade merge (two rounds of 16-way merge) | ⛔ Hard gate |
| `test_sort_spill_cleanup` | After sort completes, no temp files remain in temp dir | ⛔ Hard gate |
| `test_sort_in_memory_path` | Small output (under budget) sorted without spill files | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 8.2 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **[8.1.1]** Add `Value::heap_size()` and `Record::estimated_heap_size()` to `clinker-record` for self-tracking allocation counting
- [ ] **[8.1.2]** Promote `compare_values` / `compare_values_with_nulls` to `pub` in `sort.rs`; add `compare_records_by_fields(&Record, &Record, &[SortField]) -> Ordering` for `Record`-level comparison
- [ ] **[8.1.3]** Implement memcomparable sort key encoding in `pipeline/sort_key.rs`: `encode_sort_key(&Record, &[SortField]) -> Vec<u8>` — null sentinels, big-endian integers with sign-flip, IEEE float reinterpretation, null-terminated strings, byte-inversion for descending, days/micros-since-epoch for Date/DateTime
- [ ] **[8.1.4]** Implement `SortBuffer` in `pipeline/sort_buffer.rs`: `push(record)` with self-tracking byte counter, `should_spill()` check, `sort_and_spill()` → `SpillFile` (reuses `SpillWriter`), `into_sorted_iter()` for in-memory path, `finish()` → `SortedOutput` enum (`InMemory` | `Spilled(Vec<SpillFile>)`)
- [ ] **[8.1.5]** Implement `LoserTree` in `pipeline/loser_tree.rs`: flat `Vec<usize>` array with DataFusion-style indexing, `MergeEntry { key: Vec<u8>, record: Record }` implementing `Ord` via byte comparison, `init_loser_tree()`, `update_loser_tree()`, `is_gt()` with exhausted-stream `None` sentinels, stable merge via stream-index tiebreaker, cascade merge when >16 spill files (~80 lines core logic)
- [ ] **[8.1.6]** Implement `SortFieldSpec` untagged serde enum in `config.rs` (`Short(String)` | `Full(SortField)`); upgrade `InputConfig.sort_order` from `Vec<String>` to `Vec<SortFieldSpec>`; unify `OutputConfig.sort_order` to `Vec<SortFieldSpec>`; remove `PipelineMeta.sort_output`; add `into_sort_field()` method for shorthand expansion

> ✅ **[V-1-1] Resolved:** `SortFieldSpec` is serde-only. Config structs store
> `Vec<SortFieldSpec>` for deserialization; resolve to `Vec<SortField>` in
> `load_config()` post-processing. All downstream code (`IndexSpec`,
> `sort_partition`, `SortBuffer`) uses `Vec<SortField>` — zero cascade.

> ✅ **[V-7-1] Resolved:** `InputConfig.sort_order` means "ensure this order"
> (active sort). Spec §7.2 to be amended — `is_sorted()` fast-path preserves
> the pre-sorted optimization. Decision confirmed during drill + validation.

- [ ] **[8.1.7]** Integrate `SortBuffer` into executor at two intercept points: source sort (after reader, before Arena build in two-pass / before transform in streaming) with `is_sorted()` fast-path check; output sort (after `project_output()`, before `FormatWriter`) — per-source and per-output, independently optional

> 🟡 **[V-7-2] Streaming mode + source sort:** Source sort in streaming mode
> forces buffering (with spill) — breaks O(1) memory guarantee. Document this
> in the plan and add a `tracing::info` when source sort forces buffered mode.

> 🟡 **[V-7-3] NullOrder::Drop on source sort:** `Drop` on source sort
> permanently discards records (unlike Phase 1.5 where records stay in Arena).
> Restrict source sort to `First`/`Last` only, or route dropped records to DLQ.

> ⚠️ **[V-7-4] spill_threshold=0 edge case:** `should_spill()` predicate
> triggers before any record is pushed. Guard: require `bytes_used > 0` or
> enforce minimum threshold.

#### Code scaffolding
```rust
// Module: clinker-record/src/value.rs
impl Value {
    /// Estimated heap bytes owned by this value (excludes the enum itself).
    pub fn heap_size(&self) -> usize {
        match self {
            Value::String(s) => s.len(),
            Value::Array(arr) => {
                arr.capacity() * std::mem::size_of::<Value>()
                    + arr.iter().map(Value::heap_size).sum::<usize>()
            }
            _ => 0,
        }
    }
}

// Module: clinker-record/src/record.rs
impl Record {
    /// Estimated heap bytes for self-tracking allocation counting.
    pub fn estimated_heap_size(&self) -> usize {
        self.values().capacity() * std::mem::size_of::<Value>()
            + self.values().iter().map(Value::heap_size).sum::<usize>()
            + self.overflow_heap_size()
    }
}

// Module: clinker-core/src/config.rs
/// Accepts either a plain string shorthand or a full SortField object in YAML.
/// Short("name") expands to SortField { field: "name", order: Asc, null_order: None }.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SortFieldSpec {
    Short(String),
    Full(SortField),
}

impl SortFieldSpec {
    pub fn into_sort_field(self) -> SortField {
        match self {
            SortFieldSpec::Short(name) => SortField {
                field: name,
                order: SortOrder::Asc,
                null_order: None,
            },
            SortFieldSpec::Full(sf) => sf,
        }
    }
}

// Module: clinker-core/src/pipeline/sort_key.rs

/// Encode a record's sort fields as a memcomparable byte sequence.
/// Lexicographic byte comparison on the result equals semantic sort ordering.
pub fn encode_sort_key(record: &Record, sort_by: &[SortField]) -> Vec<u8> {
    todo!()
}

fn encode_value(value: &Value, buf: &mut Vec<u8>) {
    todo!()
}

// Module: clinker-core/src/pipeline/sort_buffer.rs

/// Accumulates records, sorts in-memory or spills to disk when budget exceeded.
/// Used at both source-sort and output-sort intercept points.
pub struct SortBuffer {
    records: Vec<Record>,
    sort_by: Vec<SortField>,
    bytes_used: usize,
    spill_threshold: usize,
    spill_dir: Option<PathBuf>,
    spill_files: Vec<SpillFile>,
    schema: Arc<Schema>,
}

pub enum SortedOutput {
    InMemory(Vec<Record>),
    Spilled(Vec<SpillFile>),
}

impl SortBuffer {
    pub fn new(sort_by: Vec<SortField>, spill_threshold: usize, spill_dir: Option<PathBuf>, schema: Arc<Schema>) -> Self { todo!() }
    pub fn push(&mut self, record: Record) { todo!() }
    pub fn should_spill(&self) -> bool { self.bytes_used >= self.spill_threshold }
    pub fn sort_and_spill(&mut self) -> Result<(), SpillError> { todo!() }
    pub fn finish(self) -> Result<SortedOutput, SpillError> { todo!() }
}

// Module: clinker-core/src/pipeline/loser_tree.rs

/// A single entry in the k-way merge: pre-encoded sort key + full record.
pub struct MergeEntry {
    pub key: Vec<u8>,
    pub record: Record,
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

/// K-way merge via loser tree. O(log k) comparisons per output record.
/// Flat Vec<usize> array, DataFusion-style indexing for any k (no power-of-2 padding).
pub struct LoserTree {
    tree: Vec<usize>,          // internal nodes store stream indices; position 0 = winner
    cursors: Vec<Option<MergeEntry>>,
}

impl LoserTree {
    pub fn new(initial_entries: Vec<Option<MergeEntry>>) -> Self { todo!() }
    pub fn winner(&self) -> Option<&MergeEntry> { todo!() }
    pub fn replace_winner(&mut self, entry: Option<MergeEntry>) { todo!() }
    fn init_loser_tree(&mut self) { todo!() }
    fn update_loser_tree(&mut self) { todo!() }
    fn is_gt(&self, a: usize, b: usize) -> bool { todo!() }
    fn leaf_index(&self, cursor_index: usize) -> usize { (self.cursors.len() + cursor_index) / 2 }
    fn parent_index(&self, node_idx: usize) -> usize { node_idx / 2 }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Value::heap_size` | `crates/clinker-record/src/value.rs` | Self-tracking allocation counting |
| `Record::estimated_heap_size` | `crates/clinker-record/src/record.rs` | Sum of Value heap sizes + Vec overhead |
| `SortFieldSpec` | `crates/clinker-core/src/config.rs` | Untagged serde enum for YAML ergonomics |
| `compare_records_by_fields` | `crates/clinker-core/src/pipeline/sort.rs` | Record-level comparator (promoted pub) |
| `encode_sort_key` | `crates/clinker-core/src/pipeline/sort_key.rs` | Memcomparable encoding (~90 lines) |
| `SortBuffer` | `crates/clinker-core/src/pipeline/sort_buffer.rs` | Shared sort-and-spill engine |
| `LoserTree` | `crates/clinker-core/src/pipeline/loser_tree.rs` | K-way merge data structure (~80 lines) |
| `MergeEntry` | `crates/clinker-core/src/pipeline/loser_tree.rs` | Ord via memcomparable key bytes |

#### Intra-phase dependencies
- Requires: Phase 6 `SpillWriter`/`SpillReader`/`SpillFile` for spill path
- Requires: Phase 7 format writers for final output
- Unblocks: Task 8.2 (DLQ output may be sorted)

#### Expanded test stubs
```rust
#[cfg(test)]
mod sort_key_tests {
    use super::*;

    /// Encoded i64 values compare in same order: -100 < 0 < 100 via byte comparison.
    #[test]
    fn test_encode_sort_key_integer_ordering() { todo!() }

    /// Encoded f64 values: -1.5 < 0.0 < 1.5 < f64::MAX via byte comparison.
    /// Verifies sign-flip + IEEE reinterpretation.
    #[test]
    fn test_encode_sort_key_float_ordering() { todo!() }

    /// Encoded strings: "abc" < "abd" < "b" via byte comparison.
    #[test]
    fn test_encode_sort_key_string_ordering() { todo!() }

    /// Encoded dates: 2024-01-01 < 2025-06-15 < 2026-12-31 via byte comparison.
    #[test]
    fn test_encode_sort_key_date_ordering() { todo!() }

    /// Encoded datetimes maintain chronological order via byte comparison.
    #[test]
    fn test_encode_sort_key_datetime_ordering() { todo!() }

    /// With nulls: First, null encodes to 0x00 prefix — sorts before all non-null.
    #[test]
    fn test_encode_sort_key_null_first() { todo!() }

    /// With nulls: Last, null encodes to 0x02 prefix — sorts after all non-null.
    #[test]
    fn test_encode_sort_key_null_last() { todo!() }

    /// Descending order XORs all bytes with 0xFF — reverses comparison.
    #[test]
    fn test_encode_sort_key_desc_inverts() { todo!() }

    /// Two-field key (String ASC, Integer DESC) compares correctly.
    #[test]
    fn test_encode_sort_key_compound() { todo!() }

    /// Integer(42) and Float(42.0) both widen to f64 for comparable encoding.
    #[test]
    fn test_encode_sort_key_cross_type_numeric() { todo!() }

    /// Empty string encodes distinctly from null.
    #[test]
    fn test_encode_sort_key_empty_string() { todo!() }

    /// Same record + same sort fields always produce identical byte sequences.
    #[test]
    fn test_encode_sort_key_roundtrip_deterministic() { todo!() }
}

#[cfg(test)]
mod sort_buffer_tests {
    use super::*;

    /// After pushing 5 records, bytes_used matches expected sum.
    #[test]
    fn test_sort_buffer_push_tracks_bytes() { todo!() }

    /// With 1KB threshold, pushing >1KB causes should_spill() to return true.
    #[test]
    fn test_sort_buffer_should_spill_at_threshold() { todo!() }

    /// Under threshold: finish() returns InMemory with correctly sorted records.
    #[test]
    fn test_sort_buffer_in_memory_sort() { todo!() }

    /// Over threshold: sort_and_spill() creates a SpillFile; records cleared; bytes_used reset.
    #[test]
    fn test_sort_buffer_spill_produces_spill_files() { todo!() }

    /// After multiple spills, finish() returns Spilled with all SpillFiles.
    #[test]
    fn test_sort_buffer_finish_spilled_returns_files() { todo!() }

    /// Each SpillFile's records are in correct sort order when read back.
    #[test]
    fn test_sort_buffer_spill_files_are_sorted() { todo!() }

    /// Zero records pushed: finish() returns InMemory(vec![]).
    #[test]
    fn test_sort_buffer_empty_returns_empty() { todo!() }
}

#[cfg(test)]
mod heap_size_tests {
    use super::*;

    /// Null, Bool, Integer, Float, Date, DateTime all return 0.
    #[test]
    fn test_value_heap_size_scalar() { todo!() }

    /// String("hello".into()) returns 5.
    #[test]
    fn test_value_heap_size_string() { todo!() }

    /// Array with nested values returns capacity * sizeof(Value) + recursive heap.
    #[test]
    fn test_value_heap_size_array() { todo!() }

    /// Record heap size matches manual calculation.
    #[test]
    fn test_record_estimated_heap_size() { todo!() }
}

#[cfg(test)]
mod config_tests {
    use super::*;

    /// YAML sort_order: [field_a, field_b] deserializes as Short variants.
    #[test]
    fn test_sort_field_spec_shorthand_yaml() { todo!() }

    /// YAML sort_order: [{field: name, order: desc}] deserializes as Full variant.
    #[test]
    fn test_sort_field_spec_full_yaml() { todo!() }

    /// Mixed shorthand and full objects in same list.
    #[test]
    fn test_sort_field_spec_mixed_yaml() { todo!() }

    /// Short("name").into_sort_field() produces SortField with Asc default.
    #[test]
    fn test_sort_field_spec_into_sort_field() { todo!() }

    /// PipelineMeta no longer has sort_output field.
    #[test]
    fn test_sort_output_removed_from_pipeline_meta() { todo!() }
}

#[cfg(test)]
mod loser_tree_tests {
    use super::*;

    /// Tree with 2 cursors initializes correctly, winner at position 0.
    #[test]
    fn test_loser_tree_init_2_streams() { todo!() }

    /// Tree with 16 cursors initializes correctly (k_max boundary).
    #[test]
    fn test_loser_tree_init_16_streams() { todo!() }

    /// Tree with 5 cursors works correctly (non-power-of-2).
    #[test]
    fn test_loser_tree_non_power_of_2() { todo!() }

    /// After consuming winner and replaying, new winner is correct.
    #[test]
    fn test_loser_tree_replay_advances_winner() { todo!() }

    /// Exhausted stream (None cursor) always loses.
    #[test]
    fn test_loser_tree_exhausted_stream_loses() { todo!() }

    /// Equal keys from streams 0 and 3: stream 0 wins (stable merge).
    #[test]
    fn test_loser_tree_stable_merge_tiebreak() { todo!() }

    /// All streams exhausted: iteration terminates cleanly.
    #[test]
    fn test_loser_tree_all_exhausted() { todo!() }

    /// MergeEntry comparison uses key bytes, not record content.
    #[test]
    fn test_loser_tree_merge_entry_ord() { todo!() }
}
```

#### Risk / gotcha
> **Memcomparable encoding for Date/DateTime:** Must use a canonical integer
> representation (days-since-epoch for Date via `signed_duration_since(1970-01-01)`,
> microseconds-since-epoch for DateTime via `timestamp_micros()`). Sign-flip
> (XOR MSB with 0x80) converts signed to unsigned for correct byte ordering.
> If chrono's internal representation changes, the encoding remains correct
> because it uses the public API, not internal fields.

> **Float NaN in sort encoder:** NaN values should be DLQ'd before reaching
> the sort encoder (spec §10.1). The IEEE-to-signed conversion produces
> deterministic bytes for NaN but the sort position is arbitrary. A
> `debug_assert!(!f.is_nan())` in the encoder is prudent.

> **String null-termination safety:** UTF-8 strings cannot contain 0x00 bytes,
> so null-terminated encoding is safe. If a future `Value::Bytes` variant is
> added, the encoding would need block-based encoding (Arrow-rs pattern).

---

### Task 8.2: DLQ Writer Completion
**Status:** ⛔ Blocked (waiting on Task 8.1)
**Blocked by:** Task 8.1 — sort infrastructure must be working (DLQ output may be sorted)

**Description:**
Complete the DLQ (Dead Letter Queue) writer with all columns per spec section 10.4.
Each rejected record is written with full provenance metadata and error classification.

**Implementation notes:**
- DLQ output columns in order: `_cxl_dlq_id` (UUID v7), `_cxl_dlq_timestamp` (ISO 8601), `_cxl_dlq_source_file`, `_cxl_dlq_source_row` (1-indexed), `_cxl_dlq_error_category`, `_cxl_dlq_error_detail`, then all source fields from the original record.
- `_cxl_dlq_id`: UUID v7 via `uuid` crate with `v7` feature. Time-ordered so DLQ records sort chronologically by ID.
- `_cxl_dlq_timestamp`: `chrono::Utc::now().to_rfc3339()` at the moment of rejection.
- Error categories enum: `missing_required_field`, `type_coercion_failure`, `required_field_conversion_failure`, `nan_in_output_field`, `aggregate_type_error`, `validation_failure`.
- Config flags: `include_reason: bool` (default true) — when false, omit `_cxl_dlq_error_category` and `_cxl_dlq_error_detail` columns. `include_source_row: bool` (default true) — when false, omit source fields after the metadata columns.
- DLQ is always CSV regardless of pipeline output format (spec §10.4, decision #10).
- Source fields appended in schema order, preserving original field names.

> ✅ **[V-5-1] Resolved:** Removed contradictory AC "same format as configured output."
> Decision #10 is authoritative: DLQ always CSV.

**Acceptance criteria:**
- [ ] DLQ output contains all 6 metadata columns plus source fields
- [ ] UUID v7 IDs are time-ordered and unique across records
- [ ] All 6 error categories correctly assigned based on rejection reason
- [ ] `include_reason: false` suppresses error category and detail columns
- [ ] `include_source_row: false` suppresses source field columns
- [ ] DLQ is always CSV regardless of pipeline output format (spec §10.4)
- [ ] JSON/XML source records serialized as JSON string in `_cxl_source_record` column

**Required unit tests (must pass before Task 8.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_dlq_all_columns_present` | DLQ record contains all 6 metadata columns plus source fields in correct order | ⛔ Hard gate |
| `test_dlq_uuid_v7_time_ordered` | Two DLQ records created sequentially have monotonically increasing UUIDs | ⛔ Hard gate |
| `test_dlq_uuid_v7_unique` | 1000 DLQ records all have distinct UUIDs | ⛔ Hard gate |
| `test_dlq_error_category_missing_required` | Missing required field rejection tagged as `missing_required_field` | ⛔ Hard gate |
| `test_dlq_error_category_type_coercion` | Type coercion failure tagged as `type_coercion_failure` | ⛔ Hard gate |
| `test_dlq_error_category_nan` | NaN in output field tagged as `nan_in_output_field` | ⛔ Hard gate |
| `test_dlq_error_category_validation` | Validation check failure tagged as `validation_failure` | ⛔ Hard gate |
| `test_dlq_error_category_aggregate` | Aggregate type error tagged as `aggregate_type_error` | ⛔ Hard gate |
| `test_dlq_error_category_required_conversion` | Required field conversion failure tagged as `required_field_conversion_failure` | ⛔ Hard gate |
| `test_dlq_include_reason_false` | With `include_reason: false`, error columns omitted from output | ⛔ Hard gate |
| `test_dlq_include_source_row_false` | With `include_source_row: false`, source fields omitted from output | ⛔ Hard gate |
| `test_dlq_source_fields_schema_order` | Source fields appear in original schema order, not insertion order | ⛔ Hard gate |
| `test_dlq_timestamp_iso8601` | `_cxl_dlq_timestamp` is valid RFC 3339 / ISO 8601 | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 8.3 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **[8.2.1]** Expand `DlqErrorCategory` to all 6 spec variants: `MissingRequiredField`, `TypeCoercionFailure`, `RequiredFieldConversionFailure`, `NanInOutputField`, `AggregateTypeError`, `ValidationFailure`. Remove `classify_error()` string matching.
- [ ] **[8.2.2]** Thread `DlqErrorCategory` through error paths: evaluator error types carry the category; `DlqEntry` gains `category: DlqErrorCategory` field; each error site (evaluator, type coercion, validation, aggregation) constructs the correct variant
- [ ] **[8.2.3]** Add `include_reason` / `include_source_row` to `DlqConfig` with `#[serde(default)]` defaulting to true. Implement column suppression in `write_dlq()`.
- [ ] **[8.2.4]** Add `_cxl_source_record` column for JSON/XML sources: serialize raw record as JSON string instead of individual source field columns (spec §10.4)

#### Code scaffolding
```rust
// Module: clinker-core/src/dlq.rs

/// All 6 DLQ error categories per spec §10.4.
/// Passed from the error site — no string matching.
#[derive(Debug, Clone, Copy)]
pub enum DlqErrorCategory {
    MissingRequiredField,
    TypeCoercionFailure,
    RequiredFieldConversionFailure,
    NanInOutputField,
    AggregateTypeError,
    ValidationFailure,
}

impl DlqErrorCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MissingRequiredField => "missing_required_field",
            Self::TypeCoercionFailure => "type_coercion_failure",
            Self::RequiredFieldConversionFailure => "required_field_conversion_failure",
            Self::NanInOutputField => "nan_in_output_field",
            Self::AggregateTypeError => "aggregate_type_error",
            Self::ValidationFailure => "validation_failure",
        }
    }
}

// Module: clinker-core/src/config.rs (DlqConfig)
pub struct DlqConfig {
    pub path: Option<String>,
    pub max_size: Option<u64>,
    #[serde(default = "default_true")]
    pub include_reason: bool,
    #[serde(default = "default_true")]
    pub include_source_row: bool,
}

// Module: clinker-core/src/executor.rs (DlqEntry updated)
pub struct DlqEntry {
    pub source_row: u64,
    pub category: DlqErrorCategory,
    pub error_message: String,
    pub original_record: Record,
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `DlqErrorCategory` (6 variants) | `crates/clinker-core/src/dlq.rs` | Replaces 2-variant enum + classify_error() |
| `DlqConfig` (updated) | `crates/clinker-core/src/config.rs` | New include_reason, include_source_row fields |
| `DlqEntry` (updated) | `crates/clinker-core/src/executor.rs` | Gains category field, drops string classification |
| `write_dlq` (updated) | `crates/clinker-core/src/dlq.rs` | Column suppression + _cxl_source_record |

#### Intra-phase dependencies
- Requires: Task 8.1 (sort infrastructure — DLQ output may be sorted)
- Unblocks: Task 8.3 (explain mode describes DLQ routing)

#### Expanded test stubs
```rust
#[cfg(test)]
mod dlq_tests {
    use super::*;

    /// All 6 variants produce correct snake_case strings.
    #[test]
    fn test_dlq_category_as_str_all_variants() { todo!() }

    /// DlqConfig defaults include_reason to true.
    #[test]
    fn test_dlq_include_reason_true_default() { todo!() }

    /// DlqConfig defaults include_source_row to true.
    #[test]
    fn test_dlq_include_source_row_true_default() { todo!() }

    /// JSON source record serialized as JSON string in _cxl_source_record column.
    #[test]
    fn test_dlq_json_source_record_column() { todo!() }
}
```

#### Risk / gotcha
> **Threading DlqErrorCategory through evaluator error types:** Sub-task 8.2.2
> requires touching error types in `cxl::eval` and `clinker-core::executor`.
> The evaluator currently returns string errors — these need to become
> structured types carrying the category. Scope is mechanical but touches
> multiple crates (cxl, clinker-core).

> **UUID v7 monotonicity:** `Uuid::now_v7()` uses a global `Mutex<ContextV7>`
> with a 42-bit monotonic counter. Process-wide cross-thread monotonicity is
> guaranteed by the crate. No custom counter needed.

---

### Task 8.3: --explain Mode + Progress Reporting
**Status:** ⛔ Blocked (waiting on Task 8.2)
**Blocked by:** Task 8.2 — DLQ writer must be complete (explain mode describes DLQ routing)

**Description:**
Implement `clinker --explain pipeline.yaml` which parses the YAML, compiles the execution
plan, and prints a human-readable plan summary without reading any data. Implement throttled
progress reporting to stderr.

**Implementation notes:**
- `--explain` flag: parse pipeline YAML, run compiler phases A-F, print structured output:
  - Parsed AST pretty-print (CXL expressions reformatted)
  - Type annotations for each field (inferred types after Phase C)
  - Source DAG (which inputs feed which transforms)
  - Indices to build (group_by + sort_by for each)
  - Parallelism classification per transform
  - Memory budget allocation (sort budget, arena budget, spill threshold)
- No data is read from any input files. Exit immediately after printing.
- Progress reporting: write to stderr, throttled to 1 update/sec via `Instant::elapsed()`.
- Format: `[cxl] {filename}: Phase {n} {phase_name}... {processed}/{total} records ({pct}%) [{elapsed}]`
- `--quiet` flag suppresses all progress output (stderr silent).
- Total record count: if available from Phase 1 count, use it; otherwise show `{processed} records` without percentage.

**Acceptance criteria:**
- [ ] `--explain` prints execution plan without reading input data
- [ ] Plan output includes AST, types, DAG, indices, parallelism, memory budget
- [ ] Progress reporting throttled to 1 update per second
- [ ] Progress format matches spec exactly
- [ ] `--quiet` suppresses all stderr progress output
- [ ] `--explain` exit code is 0 on valid config, 1 on parse error

**Required unit tests (must pass before Task 8.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_explain_no_data_read` | `--explain` with nonexistent input files succeeds (files not opened) | ⛔ Hard gate |
| `test_explain_prints_ast` | Explain output contains reformatted CXL expressions | ⛔ Hard gate |
| `test_explain_prints_type_annotations` | Explain output includes inferred types for each output field | ⛔ Hard gate |
| `test_explain_prints_source_dag` | Explain output shows source-to-transform dependency graph | ⛔ Hard gate |
| `test_explain_prints_indices` | Explain output lists indices to build with group_by/sort_by | ⛔ Hard gate |
| `test_explain_prints_memory_budget` | Explain output shows memory budget breakdown | ⛔ Hard gate |
| `test_explain_prints_parallelism` | Explain output shows parallelism classification per transform | ⛔ Hard gate |
| `test_explain_invalid_config_exit_1` | Invalid YAML produces exit code 1 | ⛔ Hard gate |

> 🟡 **[V-5-2] Added:** `test_explain_prints_parallelism` was missing from hard-gate
> table despite being in acceptance criteria. Added.

> 🟡 **[V-8-2] Progress time estimate:** Spec §10.5 example shows `~8.1s remaining`.
> Add remaining-time estimate to `ProgressUpdate` and `StderrReporter` format when
> total is known.
| `test_progress_throttle_1sec` | Progress callback invoked at most once per second | ⛔ Hard gate |
| `test_progress_format_with_total` | Progress line matches `[cxl] file: Phase N name... X/Y records (Z%) [T]` | ⛔ Hard gate |
| `test_progress_format_without_total` | When total unknown, format omits denominator and percentage | ⛔ Hard gate |
| `test_quiet_suppresses_progress` | With `--quiet`, no progress output written to stderr | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 8.4 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **[8.3.1]** Define `ProgressReporter` trait and implementations: `trait ProgressReporter: Send + Sync`, `ProgressUpdate { phase, file, processed, total, elapsed }`, `StderrReporter` (throttles to 1/sec via `Mutex<Instant>`), `NullReporter` (no-op for `--quiet`), `VecReporter` (collects updates for testing)
- [ ] **[8.3.2]** Wire `ProgressReporter` into executor: `PipelineExecutor::run_with_readers_writers` takes `&dyn ProgressReporter`; Phase 1 ingestion reports at chunk boundaries (per source); Phase 2 transform reports at chunk boundaries; streaming mode reports Phase 2 only
- [ ] **[8.3.3]** Implement `--explain` mode: parse YAML, compile `ExecutionPlan` (phases A-F), no data read; print sections: AST, type annotations, source DAG (ASCII), indices to build, parallelism classification, memory budget; exit 0 on success, exit 1 on parse/compile error; nonexistent input files do NOT cause failure
- [ ] **[8.3.4]** Add `--quiet` and `--explain` flags to CLI struct; pass `NullReporter` when quiet, `StderrReporter` otherwise

#### Code scaffolding
```rust
// Module: clinker-core/src/progress.rs

/// Progress update emitted at chunk boundaries.
pub struct ProgressUpdate {
    pub phase: String,       // "Phase 1 indexing" / "Phase 2 transforming"
    pub file: String,
    pub processed: u64,
    pub total: Option<u64>,
    pub elapsed: Duration,
}

/// Callback trait for progress reporting. Testable via VecReporter.
pub trait ProgressReporter: Send + Sync {
    fn report(&self, update: &ProgressUpdate);
}

/// Writes progress to stderr, throttled to 1 update per second.
pub struct StderrReporter {
    last_report: Mutex<Instant>,
}

/// No-op reporter for --quiet mode.
pub struct NullReporter;

/// Collects all updates for testing.
#[cfg(test)]
pub struct VecReporter {
    pub updates: Mutex<Vec<ProgressUpdate>>,
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `ProgressReporter` trait | `crates/clinker-core/src/progress.rs` | New module |
| `StderrReporter` | `crates/clinker-core/src/progress.rs` | Throttled stderr output |
| `NullReporter` | `crates/clinker-core/src/progress.rs` | --quiet mode |
| `--explain` logic | `crates/clinker/src/main.rs` | Compile plan, format, print |
| `--quiet` / `--explain` flags | `crates/clinker/src/main.rs` | Clap derive additions |

#### Intra-phase dependencies
- Requires: Task 8.2 (explain mode describes DLQ routing)
- Unblocks: Task 8.4 (cxl-cli shares the parser)

#### Expanded test stubs
```rust
#[cfg(test)]
mod progress_tests {
    use super::*;

    /// Two reports <1sec apart: only first written.
    #[test]
    fn test_stderr_reporter_throttle() { todo!() }

    /// Output matches "[cxl] file: Phase N name... X/Y (Z%) [T]".
    #[test]
    fn test_stderr_reporter_format_with_total() { todo!() }

    /// Without total: omits denominator and percentage.
    #[test]
    fn test_stderr_reporter_format_without_total() { todo!() }

    /// NullReporter produces no output.
    #[test]
    fn test_null_reporter_silent() { todo!() }

    /// VecReporter stores all updates for inspection.
    #[test]
    fn test_vec_reporter_collects() { todo!() }

    /// Phase 1 and Phase 2 produce separate updates.
    #[test]
    fn test_progress_update_phase_specific() { todo!() }
}
```

#### Risk / gotcha
> **StderrReporter thread safety:** Uses `Mutex<Instant>` for throttle state
> because rayon workers may call `report()` from different threads. The Mutex
> is uncontended (1/sec writes at most) so no performance concern.

> **Explain mode and ExecutionPlan serialization:** The `ExecutionPlan` struct
> is the right internal abstraction. Plain text formatting for Phase 8. If
> Kiln's `pipeline/dry-run` JSON-RPC method is built later, add
> `#[derive(Serialize)]` to `ExecutionPlan` — mechanical addition, no
> architectural change needed now.

---

### Task 8.4: CLI Flag Completion + cxl-cli
**Status:** ⛔ Blocked (waiting on Task 8.3)
**Blocked by:** Task 8.3 — explain mode must be working (cxl-cli shares the parser)

**Description:**
Implement all remaining CLI flags for the `clinker` binary and the standalone `cxl-cli`
tool with `check`, `eval`, and `fmt` subcommands. Define and implement all exit codes.

**Implementation notes:**
- `clinker` CLI flags (clap derive):
  - `--memory-limit <BYTES>`: memory budget (supports K/M/G suffixes), default 256M
  - `--threads <N>`: thread pool size, default `num_cpus::get()`
  - `--error-threshold <N>`: max DLQ records before abort, default unlimited (0)
  - `--batch-id <STRING>`: pipeline.batch_id value, default generated UUID v7
  - `--rules-path <DIR>`: CXL module search path, default `./rules/`
  - `--log-level <LEVEL>`: tracing level (error/warn/info/debug/trace), default info
  - `--explain`: print execution plan and exit (Task 8.3)
  - `--dry-run`: parse + compile + plan, print summary, exit
  - `--quiet`: suppress stderr progress
  - `--force`: allow output file overwrite
  - `--base-dir <DIR>`: base directory for relative path resolution
  - `--allow-absolute-paths`: permit absolute paths in YAML config
- `cxl-cli` subcommands:
  - `cxl check <file.cxl>`: parse + typecheck, print errors or "OK"
  - `cxl eval <file.cxl>` or `cxl eval -e <expr>`: evaluate CXL expression with provided field values, print result
  - `cxl eval` field input: `--field name=value` (auto-type-inferred) or `--record JSON` (mutually exclusive)
  - `cxl fmt <file.cxl>`: pretty-print CXL file to stdout (canonical formatting)
- Exit codes: must match spec §10.2 and Phase 6 `exit_codes.rs` constants: 0 = success, 1 = config/CXL compile error, 2 = partial success (some DLQ rows), 3 = fatal data error (fail_fast / threshold exceeded / NaN in group_by), 4 = I/O or system error, 130 = interrupted by SIGINT/SIGTERM
- `-n` partial processing deferred to Phase 10

> 🔴 **[V-2-2] CORRECTED (Phase 6 validation):** Original exit codes 2/3/4 contradicted
> spec §10.2 and Phase 6's `exit_codes.rs`. Updated to match spec: 2 = partial success
> (DLQ), 3 = fatal data error, 4 = I/O error. Phase 6 owns the constants; Phase 8 owns
> CLI flags and end-to-end integration tests.

**Acceptance criteria:**
- [ ] All 12 CLI flags parsed and applied correctly
- [ ] Memory limit suffix parsing (256M → 268435456 bytes)
- [ ] `cxl check` reports parse and type errors with source spans
- [ ] `cxl eval` evaluates expression with field inputs and prints result
- [ ] `cxl eval -e` evaluates inline expression
- [ ] `cxl eval --field name=value` auto-infers types (integer → float → bool → null → string)
- [ ] `cxl fmt` produces canonical pretty-printed CXL output
- [ ] Exit code 0 for clean success (zero errors)
- [ ] Exit code 1 for config/CXL compile error (no data processed)
- [ ] Exit code 2 for partial success (some rows routed to DLQ)
- [ ] Exit code 3 for fatal data error (fail_fast / threshold exceeded / NaN in group_by)
- [ ] Exit code 4 for I/O or system error
- [ ] Exit code 130 for SIGINT/SIGTERM interruption

**Required unit tests (must pass before Phase 8 exit):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_cli_memory_limit_suffix_k` | `--memory-limit 512K` parses to 524288 | ⛔ Hard gate |
| `test_cli_memory_limit_suffix_m` | `--memory-limit 256M` parses to 268435456 | ⛔ Hard gate |
| `test_cli_memory_limit_suffix_g` | `--memory-limit 2G` parses to 2147483648 | ⛔ Hard gate |
| `test_cli_memory_limit_bare_bytes` | `--memory-limit 1000000` parses to 1000000 | ⛔ Hard gate |
| `test_cli_default_memory_limit` | No flag → 256MB default | ⛔ Hard gate |
| `test_cli_error_threshold_zero` | `--error-threshold 0` means unlimited | ⛔ Hard gate |
| `test_cli_batch_id_default_uuid` | No `--batch-id` → generated UUID v7 | ⛔ Hard gate |
| `test_cli_quiet_flag` | `--quiet` sets progress suppression | ⛔ Hard gate |
| `test_cli_force_flag` | `--force` enables output overwrite | ⛔ Hard gate |
| `test_cxl_check_valid` | `cxl check` on valid CXL file → exit 0, prints "OK" | ⛔ Hard gate |
| `test_cxl_check_invalid` | `cxl check` on CXL with type error → exit 1, prints error with span | ⛔ Hard gate |
| `test_cxl_eval_simple_expr` | `cxl eval -e "1 + 2"` → prints `3` | ⛔ Hard gate |
| `test_cxl_eval_with_fields` | `cxl eval -e "Price * Qty" --field Price=10.5 --field Qty=3` → prints `31.5` | ⛔ Hard gate |
| `test_cxl_fmt_canonical` | `cxl fmt` reformats whitespace to canonical form | ⛔ Hard gate |
| `test_exit_code_0_success` | Clean pipeline run → exit 0 | ⛔ Hard gate |
| `test_exit_code_1_compile_error` | Invalid YAML or CXL error → exit 1 | ⛔ Hard gate |
| `test_exit_code_2_partial_dlq` | Some DLQ records, under threshold → exit 2 | ⛔ Hard gate |
| `test_exit_code_3_fatal_data` | Error threshold exceeded → exit 3 | ⛔ Hard gate |
| `test_exit_code_4_io_error` | Missing input file → exit 4 | ⛔ Hard gate |
| `test_exit_code_130_interrupted` | AtomicBool flag set → exit 130 | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 8 exit criteria not met until all tests above pass.

#### Sub-tasks
- [ ] **[8.4.1]** Add all CLI flags to clinker `Cli` struct (clap derive): `--memory-limit`, `--threads`, `--error-threshold`, `--batch-id`, `--rules-path`, `--log-level`, `--explain`, `--dry-run`, `--quiet`, `--force`, `--base-dir`, `--allow-absolute-paths`. Reuse existing `parse_memory_limit_bytes()` from `pipeline::memory` for suffix parsing. Wire flags through to executor/config.

> 🟡 **[V-4-1] Reuse existing parser:** `parse_memory_limit_bytes()` already exists
> at `pipeline/memory.rs:87` with full suffix parsing and tests. Do NOT create a
> new `parse_memory_limit()` in main.rs — import and reuse.

> 🟡 **[V-5-3] Missing CLI flag tests:** Add basic parse+propagation tests for
> `--threads`, `--rules-path`, `--log-level`, and `--dry-run`.

- [ ] **[8.4.2]** Update cxl-cli `Eval` command: add `-e` flag for inline expressions (required unless file is provided); add `--field name=value` with auto-type-inference (integer → float → bool → null → string); keep `--record` for JSON input (mutually exclusive with `--field` via `conflicts_with`); implement `parse_field_value()`.

> ⚠️ **[V-5-4] Edge case:** `--field eq=a=b` must parse as field `eq` with value
> `a=b` (split on first `=` only). Add test.

- [ ] **[8.4.3]** Wire exit codes end-to-end in `clinker` main.rs: 0 (success), 1 (config/compile error), 2 (partial DLQ), 3 (fatal data — fail_fast / threshold / NaN), 4 (I/O), 130 (interrupted via `AtomicBool` flag checked by executor). On SIGINT/SIGTERM: flush output writers, close files, write DLQ summary before exit 130 (spec §10.3).

> 🟡 **[V-8-1] Signal flush:** Spec §10.3 requires flush writers + close files +
> write DLQ summary on SIGINT/SIGTERM. Add graceful shutdown flush to the
> executor's interrupt-handling path.

- [ ] **[8.4.4]** Implement `--base-dir` and `--allow-absolute-paths`: `base_dir` passed to `config::load_config()` for path resolution; `allow_absolute_paths` defaults to false — reject absolute paths in YAML unless flag is set. Canonicalize resolved paths and verify they are descendants of `base_dir` to prevent `../../` traversal.

> 🟠 **[V-8-3] Path traversal:** Must canonicalize resolved paths and check
> they are descendants of `base_dir`. Add `test_path_traversal_rejected`
> for `../../etc/passwd` style relative paths.

#### Code scaffolding
```rust
// Module: clinker/src/main.rs
#[derive(Parser, Debug)]
#[command(name = "clinker", about = "CXL streaming ETL engine")]
pub struct Cli {
    /// Path to the pipeline YAML configuration file
    pub config: PathBuf,
    /// Memory budget (supports K/M/G suffixes), default 256M
    #[arg(long)]
    pub memory_limit: Option<String>,
    /// Thread pool size, default num_cpus
    #[arg(long)]
    pub threads: Option<usize>,
    /// Max DLQ records before abort, 0 = unlimited
    #[arg(long, default_value = "0")]
    pub error_threshold: u64,
    /// Pipeline batch_id, default generated UUID v7
    #[arg(long)]
    pub batch_id: Option<String>,
    /// CXL module search path
    #[arg(long, default_value = "./rules/")]
    pub rules_path: PathBuf,
    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info")]
    pub log_level: String,
    /// Print execution plan and exit (no data read)
    #[arg(long)]
    pub explain: bool,
    /// Validate config and CXL without processing data
    #[arg(long)]
    pub dry_run: bool,
    /// Suppress stderr progress output
    #[arg(long)]
    pub quiet: bool,
    /// Allow output file overwrite
    #[arg(long)]
    pub force: bool,
    /// Base directory for relative path resolution
    #[arg(long)]
    pub base_dir: Option<PathBuf>,
    /// Permit absolute paths in YAML config
    #[arg(long)]
    pub allow_absolute_paths: bool,
}

/// Parse memory limit string with K/M/G suffix.
pub fn parse_memory_limit(s: &str) -> Result<u64, String> {
    todo!()
}

// Module: cxl-cli/src/main.rs (updated Eval command)
/// Evaluate a CXL expression against provided field values.
Eval {
    /// Path to a .cxl file (required unless -e is provided)
    #[arg(required_unless_present = "expr")]
    file: Option<String>,
    /// Inline CXL expression to evaluate
    #[arg(short = 'e', long)]
    expr: Option<String>,
    /// JSON record to evaluate against (mutually exclusive with --field)
    #[arg(long, conflicts_with = "fields")]
    record: Option<String>,
    /// Field values as name=value pairs with auto-type inference
    #[arg(long = "field")]
    fields: Vec<String>,
}

/// Parse a field value string, inferring type: integer → float → bool → null → string.
fn parse_field_value(s: &str) -> Value {
    todo!()
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Cli` struct (full flags) | `crates/clinker/src/main.rs` | 12 new flags via clap derive |
| `parse_memory_limit` | `crates/clinker/src/main.rs` | K/M/G suffix, case-insensitive |
| `Eval` command (updated) | `crates/cxl-cli/src/main.rs` | -e, --field, conflicts_with |
| `parse_field_value` | `crates/cxl-cli/src/main.rs` | Auto-type inference |
| Exit code wiring | `crates/clinker/src/main.rs` | Maps PipelineError → exit codes |

#### Intra-phase dependencies
- Requires: Task 8.3 (explain mode, progress reporting, --quiet)
- Unblocks: Phase 8 exit

#### Expanded test stubs
```rust
#[cfg(test)]
mod cli_tests {
    use super::*;

    /// "256m" same as "256M" (case-insensitive).
    #[test]
    fn test_parse_memory_limit_suffix_lowercase() { todo!() }

    /// "abc" returns error.
    #[test]
    fn test_parse_memory_limit_invalid() { todo!() }

    /// "42" → Value::Integer(42).
    #[test]
    fn test_parse_field_value_integer() { todo!() }

    /// "3.14" → Value::Float(3.14).
    #[test]
    fn test_parse_field_value_float() { todo!() }

    /// "true" → Value::Bool(true).
    #[test]
    fn test_parse_field_value_bool() { todo!() }

    /// "null" → Value::Null.
    #[test]
    fn test_parse_field_value_null() { todo!() }

    /// "hello" → Value::String("hello").
    #[test]
    fn test_parse_field_value_string() { todo!() }

    /// "-5" → Value::Integer(-5).
    #[test]
    fn test_parse_field_value_negative() { todo!() }

    /// cxl eval -e "1 + 2" → prints 3.
    #[test]
    fn test_cxl_eval_inline_expr() { todo!() }

    /// cxl eval -e "Price * Qty" --field Price=10 --field Qty=3 → prints 30.
    #[test]
    fn test_cxl_eval_inline_with_fields() { todo!() }

    /// --field and --record together → error.
    #[test]
    fn test_cxl_eval_field_and_record_conflict() { todo!() }

    /// Relative input path resolved against --base-dir.
    #[test]
    fn test_base_dir_resolves_relative_paths() { todo!() }

    /// Absolute path in YAML without --allow-absolute-paths → error.
    #[test]
    fn test_absolute_path_rejected_by_default() { todo!() }

    /// Absolute path + --allow-absolute-paths → accepted.
    #[test]
    fn test_absolute_path_allowed_with_flag() { todo!() }
}
```

#### Risk / gotcha
> **Memory limit suffix parsing edge cases:** Must handle "0" (zero budget),
> bare number without suffix, overflow for large G values on 32-bit. Use u64
> throughout — u64::MAX is 16 exabytes, no practical overflow risk.

> **Exit code 130 testing:** Uses `AtomicBool` manipulation per Phase 6
> design (PLAN.md assumption #18). Tests set the flag directly, never
> install the real `ctrlc` signal handler. The `ctrlc` handler registration
> is trivial boilerplate tested only via integration tests.

---

## Intra-Phase Dependency Graph

```
Task 8.1 (Sort + Loser Tree)
    |
    +---> Task 8.2 (DLQ Writer)
              |
              +---> Task 8.3 (--explain + Progress)
                        |
                        +---> Task 8.4 (CLI Flags + cxl-cli) --> Phase 8 exit
```

Critical path: 8.1 → 8.2 → 8.3 → 8.4
No parallelizable tasks (strict linear dependency chain).

---

## Decisions Log (drill-phase — 2026-03-30)
| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| 1 | `SortBuffer` as shared sort-and-spill engine for source-sort and output-sort | Single implementation of external merge sort; executor unchanged; wraps both intercept points | Task 8.1, 8.4 |
| 2 | `SortFieldSpec` untagged serde enum (Short/Full) for YAML ergonomics | serde-saphyr supports untagged; scalars vs mappings unambiguous; unified type for input and output sort config | Task 8.1 |
| 3 | Remove `PipelineMeta.sort_output` — sort lives only on per-output `OutputConfig.sort_order` and per-source `InputConfig.sort_order` | Per-source and per-output sorts are independently optional; global sort creates ambiguous precedence; stub was untyped `serde_json::Value` | Task 8.1, 8.4 |
| 4 | `InputConfig.sort_order` means "ensure this order" (active sort), not pre-sorted declaration | Subsumes pre-sorted optimization via `is_sorted()` fast path; one field, one semantic; no silent correctness bugs from false declarations | Task 8.1 |
| 5 | Hybrid comparator: closure `sort_by` for in-memory sorts, memcomparable byte encoding for loser tree merge | In-memory sorts don't need encoding overhead; loser tree needs `Ord` (memcmp on pre-encoded keys); Arrow-rs/DataFusion/Polars all converge on this split | Task 8.1 |
| 6 | Hand-rolled loser tree (~80 lines), DataFusion-style flat array | No usable crate exists; 50-72% faster than BinaryHeap (DataFusion PR #4301); 128 bytes fits in L1 cache for k=16 | Task 8.1 |
| 7 | `slice::sort_by` (stable) for in-memory path — no sequence numbers needed | Rust stdlib stable sort guarantees; loser tree uses stream-index tiebreaker for merge stability | Task 8.1 |
| 8 | Self-tracking allocation counting for sort buffer memory budget | Industry consensus (DataFusion, Spark, SQLite); deterministic and testable; ~85-90% accurate; no RSS, no custom allocator | Task 8.1 |
| 9 | Structured `DlqErrorCategory` enum (6 variants) passed from error site, not string classification | Correct by construction; compiler-enforced exhaustive match; category is a property of the error, not its message | Task 8.2 |
| 10 | DLQ always CSV per spec §10.4; JSON/XML sources get `_cxl_source_record` column | Industry consensus (Spark, Beam): fixed envelope + raw record as string; one DLQ file, cross-source queryable | Task 8.2 |
| 11 | `include_reason` / `include_source_row` added to `DlqConfig` | Natural YAML nesting under `error_handling.dlq`; defaults to true via `#[serde(default)]` | Task 8.2 |
| 12 | `Uuid::now_v7()` as-is — process-wide monotonicity guaranteed by crate's global Mutex + 42-bit counter | RFC 9562 method 2; no custom counter needed; DLQ is low-volume error path | Task 8.2 |
| 13 | `--explain` outputs plain text (human-readable terminal output) | Developer diagnostic tool; matches rustc/cargo/psql conventions; structured JSON for Kiln deferred (mechanical `#[derive(Serialize)]` on `ExecutionPlan` later) | Task 8.3 |
| 14 | Callback-based `ProgressReporter` trait | Testable via `VecReporter`; `NullReporter` for `--quiet`; extensible for Kiln's Tier 1 JSON Lines events | Task 8.3 |
| 15 | Phase-specific progress (separate Phase 1 / Phase 2 counters) | Matches spec §10.5 example; more informative than unified progress | Task 8.3 |
| 16 | `cxl eval`: `-e` for inline expressions, positional for file, both `--field` and `--record` | Follows sed/perl/ruby `-e` convention; `--field` auto-type-inferred; `--record` for programmatic JSON; no auto-detection (anti-pattern per clig.dev) | Task 8.4 |
| 17 | `-n` partial processing deferred to Phase 10 | Phase 8 scope is large enough; `--dry-run` stays config-validation-only | Task 8.4 |
| 18 | `--base-dir` and `--allow-absolute-paths` implemented in Phase 8 | Mechanical plumbing through config loader; no design ambiguity | Task 8.4 |
| 19 | Exit code 130 tested via `AtomicBool` manipulation, not real signals | Deterministic, cross-platform; consistent with Phase 6 design (PLAN.md assumption #18) | Task 8.4 |

## Assumptions Log (drill-phase — 2026-03-30)
| # | Assumption | Basis | Risk if wrong |
|---|-----------|-------|---------------|
| 1 | Memcomparable encoding for clinker's 7 Value variants is ~90 lines with no external dependencies | Arrow-rs row encoding is 2000+ lines but handles 20+ types; clinker's type set is small and fixed | May need block-based string encoding if Value::Bytes is added (v2) |
| 2 | Self-tracking allocation counting is ~85-90% accurate for sort buffer sizing | DataFusion and Spark both accept approximate accounting; undercounting causes earlier-than-needed spill, not OOM | If undercounting is severe (>30%), RSS backstop can be added mechanically |
| 3 | DLQ volume is low (<1% of records) making UUID v7 Mutex contention negligible | DLQ is error-path; typical pipelines produce 0-100 DLQ records | If DLQ volume is unexpectedly high (>10%), Mutex may become a bottleneck; switch to thread-local UUID generation |
| 4 | `StderrReporter` Mutex for throttle state is uncontended at 1/sec update rate | Rayon workers complete chunks faster than 1/sec; only one report per second can pass the throttle | No practical risk — even under contention, the Mutex protects a single Instant comparison |
