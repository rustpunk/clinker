# Phase 5: Two-Pass Pipeline — Arena, Indexing, Windows

**Status:** 🔲 Not Started
**Depends on:** Phase 4 (CSV + Minimal End-to-End Pipeline)
**Entry criteria:** `cargo test -p clinker` passes all Phase 4 tests; streaming executor handles stateless CSV→CXL→CSV pipeline
**Exit criteria:** Two-pass executor runs window functions (count/sum/avg/min/max/first/last/lag/lead/any/all) over grouped and sorted partitions; cross-source windows resolve against reference SecondaryIndex; `cargo test -p clinker-core` passes 60+ tests

---

## Tasks

### Task 5.1: ExecutionPlan + AST Compiler Phases D-F
**Status:** 🔲 Not Started
**Blocked by:** Phase 4 exit criteria

**Description:**
Implement the `ExecutionPlan` struct and compiler phases D (dependency analysis), E (index
planning), and F (plan emission) in `clinker-core`. This analyzes the resolved CXL AST to
determine which window indices must be built and classifies each transform by parallelism.

**Implementation notes:**
- `ExecutionPlan` struct: `stateless_transforms: Vec<TransformSpec>`, `indices_to_build: Vec<IndexSpec>`, `window_transforms: Vec<WindowTransformSpec>`, `parallelism: Vec<ParallelismClass>`.
- Phase D — dependency analysis: walk each resolved AST node. If a node references `window(...)`, `first(...)`, `last(...)`, `lag(...)`, `lead(...)`, `count(...)`, `sum(...)`, `avg(...)`, `min(...)`, `max(...)`, `any(...)`, or `all(...)`, mark the transform as window-dependent. Extract `group_by` and `sort_by` from window call arguments.
- Phase E — index planning: collect all `(source, group_by, sort_by)` triples from window-dependent transforms. Deduplicate: two transforms with the same triple share one `IndexSpec`. Produce `indices_to_build: Vec<IndexSpec>` where `IndexSpec { source: SourceRef, group_by: Vec<String>, sort_by: Vec<SortField> }`.
- Phase F — plan emission: classify each transform's `ParallelismClass`: `Stateless` (no field reads from arena), `IndexReading` (reads from built index, can parallelize across partitions), `Sequential` (must run in record order, e.g., running totals with side effects).
- `--dry-run` prints the execution plan: number of indices, transforms per phase, parallelism classification.

**Acceptance criteria:**
- [ ] `ExecutionPlan` struct constructed from `PipelineConfig` + resolved AST
- [ ] Dependency analysis identifies window-dependent transforms
- [ ] Index deduplication: identical `(source, group_by, sort_by)` triples share one IndexSpec
- [ ] Parallelism classification correct for stateless, index-reading, and sequential transforms
- [ ] `--dry-run` displays execution plan summary

**Required unit tests (must pass before Task 5.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_plan_stateless_only` | Config with no window functions → empty `indices_to_build`, all transforms `Stateless` | ⛔ Hard gate |
| `test_plan_single_window_index` | One `sum(amount, group_by: [dept])` → one IndexSpec with `group_by: ["dept"]` | ⛔ Hard gate |
| `test_plan_dedup_shared_index` | Two transforms with same `group_by`+`sort_by` → single IndexSpec, both reference it | ⛔ Hard gate |
| `test_plan_distinct_indices` | Different `group_by` keys → separate IndexSpec entries | ⛔ Hard gate |
| `test_plan_parallelism_stateless` | Pure arithmetic transform classified as `Stateless` | ⛔ Hard gate |
| `test_plan_parallelism_index_reading` | Window aggregate classified as `IndexReading` | ⛔ Hard gate |
| `test_plan_parallelism_sequential` | Positional function with ordering dependency classified as `Sequential` | ⛔ Hard gate |
| `test_plan_dry_run_output` | Dry-run mode produces human-readable plan summary without processing data | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 5.2 status remains `Blocked` until all tests above pass.

---

### Task 5.2: Arena + ArenaRecordView
**Status:** ⛔ Blocked (waiting on Task 5.1)
**Blocked by:** Task 5.1 — ExecutionPlan must identify which indices to build

**Description:**
Implement the `Arena` struct in `clinker-core` and the `ArenaRecordView` implementing
`FieldResolver` for zero-allocation stack-based field access into arena records.

**Implementation notes:**
- `Arena` struct: `schema: Schema`, `records: Vec<MinimalRecord>`. Constructed by streaming a `FormatReader`, extracting only the fields referenced by `IndexSpec.group_by` and `IndexSpec.sort_by` (plus any fields referenced in window expressions). Non-indexed fields are NOT stored in the arena.
- Construction: `Arena::build(reader: &mut dyn FormatReader, fields: &[String]) -> Result<Arena, ArenaError>`. Streams all records, plucks only the named fields, stores as `MinimalRecord`.
- `ArenaRecordView<'a>` borrows `&'a Arena` and a record index `usize`. Implements `FieldResolver` by looking up the field name in the arena schema, then returning `&arena.records[idx].fields[col]`.
- Zero allocation: `ArenaRecordView` is a stack struct with two fields (`&Arena`, `usize`). No heap allocation per lookup.
- Arena must be `Send + Sync` (immutable after construction, read by multiple partitions in Phase 6).

**Acceptance criteria:**
- [ ] Arena streams records from FormatReader, stores only indexed fields
- [ ] ArenaRecordView implements FieldResolver with zero-allocation lookups
- [ ] Non-indexed fields excluded from arena storage
- [ ] Arena is `Send + Sync`
- [ ] Arena construction is streaming (constant memory overhead proportional to field count, not record count beyond storage)

**Required unit tests (must pass before Task 5.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_arena_build_from_csv` | Arena built from 100-row CSV contains 100 MinimalRecords | ⛔ Hard gate |
| `test_arena_field_projection` | Arena built with `fields: ["dept", "amount"]` stores only those 2 fields per record | ⛔ Hard gate |
| `test_arena_record_view_resolve` | `ArenaRecordView` at index 5 resolves `"dept"` to correct value | ⛔ Hard gate |
| `test_arena_record_view_missing_field` | `ArenaRecordView::resolve("nonexistent")` returns None | ⛔ Hard gate |
| `test_arena_record_view_zero_alloc` | View construction and field resolution do not allocate (benchmark or size_of assert) | ⛔ Hard gate |
| `test_arena_send_sync` | Compile-time assertion that Arena is `Send + Sync` | ⛔ Hard gate |
| `test_arena_empty_input` | Arena built from empty reader has 0 records, schema still valid | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 5.3 status remains `Blocked` until all tests above pass.

---

### Task 5.3: SecondaryIndex + GroupByKey
**Status:** ⛔ Blocked (waiting on Task 5.2)
**Blocked by:** Task 5.2 — Arena must exist for index construction

**Description:**
Implement `GroupByKey` enum with numeric normalization and `SecondaryIndex` as a HashMap from
composite group keys to record position lists. NaN values in group_by keys are rejected as
hard errors.

**Implementation notes:**
- `GroupByKey` enum: `Str(Box<str>)`, `Int(i64)`, `Float(u64)` (stored as `f64::to_bits()` for `Eq`+`Hash`), `Bool(bool)`, `Date(NaiveDate)`, `DateTime(NaiveDateTime)`. No `Null` variant — null group_by values are excluded from the index.
- Numeric normalization: `Value::Integer(i)` widened to `GroupByKey::Float(f64::from(i as f64).to_bits())` so that `42` and `42.0` group together. Alternative: keep them separate and document. Chosen approach: widen Int to Float via `to_bits()`.
- `SecondaryIndex` struct: `groups: HashMap<Vec<GroupByKey>, Vec<usize>>` where `usize` is the record position in the Arena.
- Construction: `SecondaryIndex::build(arena: &Arena, group_by: &[String]) -> Result<SecondaryIndex, IndexError>`. Iterates arena records, extracts group_by fields, builds composite key, appends position to the group's Vec.
- NaN rejection: if any group_by field is `Value::Float(f)` where `f.is_nan()`, return `IndexError::NanInGroupBy { field, row }`. Pipeline exits with code 3.
- Null exclusion: if any group_by field is `Value::Null`, skip that record (do not insert into any group). Log at `debug` level.

**Acceptance criteria:**
- [ ] GroupByKey supports all non-null Value variants with Eq+Hash
- [ ] Int and Float values that represent the same number group together
- [ ] SecondaryIndex maps composite keys to Arena position lists
- [ ] NaN in group_by field produces hard error (not silent skip)
- [ ] Null group_by values excluded from index (record not in any group)
- [ ] Index construction is single-pass over Arena

**Required unit tests (must pass before Task 5.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_group_by_key_eq_hash` | Same value produces same GroupByKey hash; different values differ | ⛔ Hard gate |
| `test_group_by_key_int_float_unify` | `Value::Integer(42)` and `Value::Float(42.0)` produce equal GroupByKey | ⛔ Hard gate |
| `test_secondary_index_single_group_by` | Group by `"dept"` with 3 departments → 3 entries, correct position lists | ⛔ Hard gate |
| `test_secondary_index_composite_key` | Group by `["dept", "region"]` → composite key grouping correct | ⛔ Hard gate |
| `test_secondary_index_nan_rejection` | NaN in group_by field → `IndexError::NanInGroupBy` | ⛔ Hard gate |
| `test_secondary_index_null_exclusion` | Null group_by value → record absent from all groups | ⛔ Hard gate |
| `test_secondary_index_empty_arena` | Empty arena → empty index, no error | ⛔ Hard gate |
| `test_secondary_index_all_nulls` | All records have null group_by → empty index, no error | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 5.4 status remains `Blocked` until all tests above pass.

---

### Task 5.4: Phase 1.5 Pointer Sorting + WindowContext Impl
**Status:** ⛔ Blocked (waiting on Task 5.3)
**Blocked by:** Task 5.3 — SecondaryIndex must be built for partition sorting

**Description:**
Implement Phase 1.5: sort each partition's `Vec<usize>` pointers by looking up `sort_by`
fields in the Arena. Implement `WindowContext` providing positional and aggregate window
functions over sorted partitions.

**Implementation notes:**
- Pointer sorting: for each group in `SecondaryIndex`, sort the `Vec<usize>` by comparing `arena.records[a]` vs `arena.records[b]` on the `sort_by` fields. Use `slice::sort_by()` (stable sort).
- Null handling in sort: configurable via `NullOrder { First, Last, Drop }`. `First` = nulls sort before all values. `Last` = nulls sort after all values. `Drop` = remove records with null sort keys from the partition.
- Sort order: `SortField { field: String, order: SortOrder, null_order: NullOrder }`. `SortOrder::Asc` / `SortOrder::Desc`.
- Pre-sorted optimization: before sorting, check if the partition is already sorted (single linear scan). If yes, skip sort. Log at `debug` level.
- `WindowContext` struct: holds `&Arena`, `&SecondaryIndex`, current partition `&[usize]`, current position within partition. Implements:
  - Positional: `first(field) -> &Value`, `last(field) -> &Value`, `lag(field, offset) -> Option<&Value>`, `lead(field, offset) -> Option<&Value>`.
  - Aggregates: `count() -> usize`, `sum(field) -> Value`, `avg(field) -> Value`, `min(field) -> Value`, `max(field) -> Value`.
  - Iterables: `any(predicate) -> bool`, `all(predicate) -> bool` (predicate is a CXL expression evaluated against each record in partition).
- `WindowContext` implements `FieldResolver` so CXL expressions can call window functions naturally.
- `sum`/`avg` on non-numeric fields → return `Value::Null` (not error).

**Acceptance criteria:**
- [ ] Partitions sorted by sort_by fields with correct null handling
- [ ] Pre-sorted detection skips unnecessary sort
- [ ] All positional functions (first/last/lag/lead) return correct values
- [ ] All aggregate functions (count/sum/avg/min/max) compute correctly over partitions
- [ ] any/all evaluate predicate across partition records
- [ ] Non-numeric sum/avg returns Null

**Required unit tests (must pass before Task 5.5 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_sort_partition_ascending` | 5 records sorted by `amount ASC` → positions in correct order | ⛔ Hard gate |
| `test_sort_partition_descending` | 5 records sorted by `amount DESC` → positions in reverse order | ⛔ Hard gate |
| `test_sort_null_first` | `NullOrder::First` → null sort_by values at start of partition | ⛔ Hard gate |
| `test_sort_null_last` | `NullOrder::Last` → null sort_by values at end of partition | ⛔ Hard gate |
| `test_sort_null_drop` | `NullOrder::Drop` → records with null sort_by removed from partition | ⛔ Hard gate |
| `test_sort_presorted_skip` | Already-sorted partition detected, sort skipped (verify via log or counter) | ⛔ Hard gate |
| `test_window_first_last` | `first("name")` and `last("name")` return correct boundary values | ⛔ Hard gate |
| `test_window_lag_lead` | `lag("amount", 1)` at position 3 returns position 2's value; `lead` returns position 4's | ⛔ Hard gate |
| `test_window_lag_out_of_bounds` | `lag("amount", 1)` at position 0 returns None/Null | ⛔ Hard gate |
| `test_window_count` | `count()` over 5-record partition returns 5 | ⛔ Hard gate |
| `test_window_sum_avg` | `sum("amount")` and `avg("amount")` correct for known values | ⛔ Hard gate |
| `test_window_min_max` | `min("amount")` and `max("amount")` correct for known values | ⛔ Hard gate |
| `test_window_sum_non_numeric` | `sum("name")` over string field returns `Value::Null` | ⛔ Hard gate |
| `test_window_any_all` | `any(amount > 100)` true when one exceeds, `all(amount > 0)` true when all positive | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 5.5 status remains `Blocked` until all tests above pass.

---

### Task 5.5: Full Two-Pass Executor + Provenance
**Status:** ⛔ Blocked (waiting on Task 5.4)
**Blocked by:** Task 5.4 — window functions must be working

**Description:**
Orchestrate the complete two-pass pipeline: compile → Phase 1 (build arena + indices) →
Phase 1.5 (sort partitions) → Phase 2 (evaluate with windows) → Phase 3 (project + write).
Support cross-source windows and populate RecordProvenance.

**Implementation notes:**
- `TwoPassExecutor::run(config: &PipelineConfig) -> Result<PipelineCounters, PipelineError>`.
- Orchestration sequence:
  1. Parse config, compile CXL expressions, produce `ExecutionPlan` (Task 5.1).
  2. Phase 1: open primary FormatReader. If `indices_to_build` is non-empty, stream records into Arena. For reference sources (cross-source windows), open their FormatReaders and build separate Arenas.
  3. Phase 1.5: for each IndexSpec, build SecondaryIndex from its Arena, then sort partitions.
  4. Phase 2: re-read primary source (second pass). For each record: evaluate stateless transforms, then for window-dependent transforms, look up record's group in SecondaryIndex, construct WindowContext, evaluate.
  5. Phase 3: project output fields, write via FormatWriter.
- Cross-source windows: a CXL expression like `sum(ref.amount, group_by: [ref.dept], source: "reference")` evaluates the window against the reference Arena/SecondaryIndex, not the primary. The group key is computed from the primary record's field value matched against the reference index.
- RecordProvenance: populated during Phase 1 for each primary record. `source_file` from config `input.path`, `source_row` incremented per record, `ingestion_timestamp` set once at pipeline start.
- PipelineCounters: updated at chunk boundaries (every 10K records) to avoid per-record atomic overhead. Final flush at end.
- Fallback: if `ExecutionPlan.indices_to_build` is empty (all stateless), delegate to `StreamingExecutor` from Phase 4 (single pass, no arena overhead).

**Acceptance criteria:**
- [ ] Two-pass pipeline produces correct output for window functions
- [ ] Stateless-only configs fall back to single-pass (no arena built)
- [ ] Cross-source windows resolve against reference SecondaryIndex
- [ ] RecordProvenance populated with correct source_file, source_row
- [ ] PipelineCounters accurate at pipeline completion
- [ ] End-to-end: CSV with window transforms → correct aggregated output

**Required unit tests (must pass before Phase 5 exit):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_two_pass_sum_by_dept` | `sum(amount, group_by: [dept])` produces correct per-department totals | ⛔ Hard gate |
| `test_two_pass_avg_by_region` | `avg(score, group_by: [region])` produces correct averages | ⛔ Hard gate |
| `test_two_pass_count_by_group` | `count(group_by: [status])` produces correct counts per status | ⛔ Hard gate |
| `test_two_pass_first_last_sorted` | `first(name, group_by: [dept], sort_by: [hire_date])` returns earliest hire | ⛔ Hard gate |
| `test_two_pass_lag_lead_sorted` | `lag(amount, 1, group_by: [dept], sort_by: [date])` returns previous row's amount | ⛔ Hard gate |
| `test_two_pass_stateless_fallback` | Config with no windows → single-pass executor used, no arena allocated | ⛔ Hard gate |
| `test_two_pass_cross_source_window` | Window referencing `source: "reference"` resolves against reference file's index | ⛔ Hard gate |
| `test_two_pass_provenance_populated` | RecordProvenance has correct `source_file` and sequential `source_row` | ⛔ Hard gate |
| `test_two_pass_pipeline_counters` | Counters match: total, ok, dlq counts correct after run | ⛔ Hard gate |
| `test_two_pass_mixed_stateless_and_window` | Config with both stateless and window transforms → both evaluated correctly | ⛔ Hard gate |
| `test_two_pass_multiple_windows_shared_index` | Two transforms sharing same group_by → single index built, both correct | ⛔ Hard gate |
| `test_two_pass_nan_exit_code_3` | NaN in group_by field → pipeline exits with code 3 | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 5 exit criteria not met until all tests above pass.
