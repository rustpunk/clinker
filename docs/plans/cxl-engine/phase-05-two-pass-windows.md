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
- `--explain` prints the execution plan: number of indices, transforms per phase, parallelism classification. Compile-only mode — no data I/O.

**Acceptance criteria:**
- [ ] `ExecutionPlan` struct constructed from `PipelineConfig` + resolved AST
- [ ] Dependency analysis identifies window-dependent transforms
- [ ] Index deduplication: identical `(source, group_by, sort_by)` triples share one IndexSpec
- [ ] Parallelism classification correct for stateless, index-reading, and sequential transforms
- [ ] `--explain` displays execution plan summary (compile-only, no data read)
- [ ] Full spec struct shape: `source_dag`, `indices_to_build`, `transforms`, `output_projections`, `parallelism` — all populated
- [ ] Cross-source dependencies recorded in source DAG with topological ordering
- [ ] `ExecutionMode::Streaming` vs `ExecutionMode::TwoPass` derived from `indices_to_build.is_empty()`

#### Sub-tasks
- [ ] **5.1.1** `cxl::analyzer` module — AST walker producing `AnalysisReport` (window usage, field access, parallelism hints). Walks `TypedProgram` AST nodes, detects `WindowCall` expressions, extracts field names from postfix chains (`window.first().field_name`), records accessed fields for Arena projection, classifies parallelism hints per transform.
- [ ] **5.1.2** `AnalysisReport` struct — per-transform: `TransformAnalysis { name, window_calls: Vec<WindowCallInfo>, accessed_fields: HashSet<String>, parallelism_hint: ParallelismHint }`. `WindowCallInfo` records function name, argument field refs, and whether it's positional/aggregate/iterable.
- [ ] **5.1.3** `clinker-core::plan::index` — Phase E: define `LocalWindowConfig { source: Option<String>, group_by: Vec<String>, sort_by: Vec<SortField>, on: Option<String> }` typed struct, deserialize from `TransformConfig.local_window: Option<serde_json::Value>`. Extract configs, combine with `AnalysisReport` field sets, deduplicate into `Vec<IndexSpec>`. Each `IndexSpec` carries `arena_fields` = union of `group_by` + `sort_by` + all fields referenced in window expressions for that index. Define `collect_arena_fields(plan, source_ref) -> Vec<String>` helper that unions arena_fields across all IndexSpecs for a source.
- [ ] **5.1.4** `clinker-core::plan::execution` — Phase F: `ExecutionPlan` with full spec struct. `source_dag: Vec<SourceTier>` (topologically sorted), `indices_to_build: Vec<IndexSpec>`, `transforms: Vec<TransformPlan>`, `output_projections: Vec<OutputSpec>`, `parallelism: ParallelismProfile`.
- [ ] **5.1.5** Source DAG construction — identify cross-source dependencies from `local_window.source` references, topological sort into `Vec<SourceTier>`. Each tier contains sources that can be processed independently (Phase 6 parallelizes within tiers).
- [ ] **5.1.6** `ExecutionMode` enum — `Streaming` when `indices_to_build.is_empty()`, `TwoPass` otherwise. Exposed via `ExecutionPlan::mode()`.
- [ ] **5.1.7** `--explain` CLI integration — `clinker --explain pipeline.yaml` compiles config, produces `ExecutionPlan`, pretty-prints: AST per CXL block, type annotations, source DAG, indices to build, parallelism classification. No data read.

#### Code scaffolding
```rust
// Module: crates/cxl/src/analyzer/mod.rs

/// Phase D: AST dependency analysis.
/// Walks TypedProgram to extract window usage, field access, and parallelism hints.
pub fn analyze(typed: &TypedProgram, window_names: &[String]) -> AnalysisReport {
    todo!()
}

/// Per-transform analysis results from Phase D.
pub struct TransformAnalysis {
    pub name: String,
    pub window_calls: Vec<WindowCallInfo>,
    pub accessed_fields: HashSet<String>,
    pub parallelism_hint: ParallelismHint,
}

pub struct WindowCallInfo {
    pub function: WindowFunction,
    pub field_args: Vec<String>,
    pub postfix_fields: Vec<String>,  // fields accessed on positional results
}

pub enum ParallelismHint { Stateless, IndexReading, Sequential }

pub struct AnalysisReport {
    pub transforms: Vec<TransformAnalysis>,
}
```

```rust
// Module: crates/clinker-core/src/plan/execution.rs

/// Full execution plan — spec §6.1 Phase F output.
pub struct ExecutionPlan {
    pub source_dag: Vec<SourceTier>,
    pub indices_to_build: Vec<IndexSpec>,
    pub transforms: Vec<TransformPlan>,
    pub output_projections: Vec<OutputSpec>,
    pub parallelism: ParallelismProfile,
}

pub enum ExecutionMode { Streaming, TwoPass }

impl ExecutionPlan {
    /// Compile a PipelineConfig into an ExecutionPlan.
    pub fn compile(config: &PipelineConfig) -> Result<Self, PlanError> {
        todo!()
    }

    pub fn mode(&self) -> ExecutionMode {
        if self.indices_to_build.is_empty() {
            ExecutionMode::Streaming
        } else {
            ExecutionMode::TwoPass
        }
    }
}

/// One tier of the source dependency DAG. Sources within a tier are independent.
pub struct SourceTier {
    pub sources: Vec<SourceRef>,
}

/// Spec for one secondary index to build during Phase 1.
pub struct IndexSpec {
    pub source: SourceRef,
    pub group_by: Vec<String>,
    pub sort_by: Vec<SortField>,
    pub arena_fields: Vec<String>,  // union of group_by + sort_by + window-referenced fields
    pub already_sorted: bool,        // Phase E pre-sorted optimization from config match
}

pub struct TransformPlan {
    pub name: String,
    pub typed: Arc<TypedProgram>,
    pub parallelism_class: ParallelismClass,
    pub window_index: Option<usize>,  // index into indices_to_build, if window-dependent
    pub partition_lookup: Option<PartitionLookupKind>,
}

pub enum PartitionLookupKind { SameSource, CrossSource { on_expr: Arc<TypedProgram> } }
pub enum ParallelismClass { Stateless, IndexReading, Sequential, CrossSource }

pub struct OutputSpec {
    pub name: String,
    pub mapping: IndexMap<String, String>,
    pub exclude: Vec<String>,
    pub include_unmapped: bool,
}

pub struct ParallelismProfile {
    pub per_transform: Vec<ParallelismClass>,
    pub worker_threads: usize,
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `analyze()` | `crates/cxl/src/analyzer/mod.rs` | Phase D — AST walking |
| `AnalysisReport` | `crates/cxl/src/analyzer/mod.rs` | Exported from `cxl::analyzer` |
| `ExecutionPlan` | `crates/clinker-core/src/plan/execution.rs` | Phase F — full spec struct |
| `IndexSpec` | `crates/clinker-core/src/plan/execution.rs` | Phase E output |
| `SourceTier` | `crates/clinker-core/src/plan/execution.rs` | Source DAG tier |
| `TransformPlan` | `crates/clinker-core/src/plan/execution.rs` | Per-transform execution unit |
| `plan::index` | `crates/clinker-core/src/plan/index.rs` | Phase E dedup logic |
| `plan::mod` | `crates/clinker-core/src/plan/mod.rs` | Re-exports |

#### Intra-phase dependencies
- Requires: Phase 4 (`PipelineConfig`, `CompiledTransform`, `StreamingExecutor` to rename)
- Unblocks: Task 5.2 (Arena field set from `IndexSpec.arena_fields`)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Config with no window functions produces empty indices_to_build and all Stateless transforms.
    #[test]
    fn test_plan_stateless_only() {
        // Arrange: PipelineConfig with pure arithmetic transforms, no local_window
        // Act: ExecutionPlan::compile(config)
        // Assert: indices_to_build.is_empty(), all transforms Stateless, mode() == Streaming
    }

    /// One sum(amount, group_by: [dept]) produces one IndexSpec with group_by: ["dept"].
    #[test]
    fn test_plan_single_window_index() {
        // Arrange: config with one transform using window.sum, local_window { group_by: [dept] }
        // Act: compile
        // Assert: indices_to_build.len() == 1, group_by == ["dept"]
    }

    /// Two transforms with same group_by+sort_by share a single IndexSpec.
    #[test]
    fn test_plan_dedup_shared_index() {
        // Arrange: two transforms, both local_window { group_by: [dept], sort_by: [date] }
        // Act: compile
        // Assert: indices_to_build.len() == 1, both transforms reference index 0
    }

    /// Different group_by keys produce separate IndexSpec entries.
    #[test]
    fn test_plan_distinct_indices() {
        // Arrange: transform A group_by: [dept], transform B group_by: [region]
        // Act: compile
        // Assert: indices_to_build.len() == 2
    }

    /// Pure arithmetic transform classified as Stateless.
    #[test]
    fn test_plan_parallelism_stateless() {
        // Arrange: transform with only let + emit, no window.*
        // Act: compile
        // Assert: parallelism_class == Stateless
    }

    /// Window aggregate transform classified as IndexReading.
    #[test]
    fn test_plan_parallelism_index_reading() {
        // Arrange: transform with window.sum(amount, group_by: [dept])
        // Act: compile
        // Assert: parallelism_class == IndexReading
    }

    /// Positional function with ordering dependency classified as Sequential.
    #[test]
    fn test_plan_parallelism_sequential() {
        // Arrange: transform with window.lag(1) used as running accumulator
        // Act: compile
        // Assert: parallelism_class == Sequential
    }

    /// --explain mode produces human-readable plan summary without processing data.
    #[test]
    fn test_plan_explain_output() {
        // Arrange: config with mixed transforms
        // Act: ExecutionPlan::compile + format for explain
        // Assert: output contains index count, transform count, parallelism per transform
    }

    /// Cross-source window produces source DAG with correct topological order.
    #[test]
    fn test_plan_cross_source_dag() {
        // Arrange: config with primary input + reference input, transform with local_window.source: "reference"
        // Act: compile
        // Assert: source_dag has 2 tiers, reference source in tier 0, primary in tier 1
    }

    /// Config with window functions produces ExecutionMode::TwoPass.
    #[test]
    fn test_plan_mode_two_pass() {
        // Arrange: config with local_window
        // Act: compile
        // Assert: plan.mode() == ExecutionMode::TwoPass
    }

    /// Cross-source reference to non-existent source produces compile error.
    #[test]
    fn test_plan_cross_source_missing_reference() {
        // Arrange: config with local_window.source: "nonexistent" (no matching InputConfig)
        // Act: compile
        // Assert: Err(PlanError::UnknownSource { name: "nonexistent" })
    }
}
```

#### Risk / gotcha
> **AST postfix chain walking:** Phase D must detect field accesses on positional window results
> like `window.first().name`. This requires walking `Expr::MethodCall` receiver chains and
> `Expr::QualifiedFieldRef` on window call results. If the AST represents `window.first().name`
> as nested `MethodCall(FieldRef("name"), receiver=WindowCall("first"))`, the walker must
> traverse the receiver to find the `WindowCall` and collect `"name"` as an accessed field.
> Test with deeply chained expressions: `window.lag(1).field.method().other_field`.

---

### Task 5.2: Arena + RecordView
**Status:** ⛔ Blocked (waiting on Task 5.1)
**Blocked by:** Task 5.1 — ExecutionPlan must identify which indices to build and which fields the Arena stores

**Description:**
Implement the `RecordStorage` trait and `RecordView<'a, S>` in `clinker-record`, the `Arena` struct
in `clinker-core` implementing `RecordStorage`, and update `WindowContext` to use lifetime-parameterized
`RecordView` returns instead of `Box<dyn FieldResolver>`.

**Implementation notes:**
- `RecordStorage` trait in `clinker-record`: `resolve_field(index: u32, name: &str) -> Option<Value>`, `available_fields(index: u32) -> Vec<&str>`. Must be `Send + Sync`.
- `RecordView<'a, S: RecordStorage>`: `Copy` struct (16 bytes: `&'a S` + `u32`). Implements `FieldResolver` by delegating to `RecordStorage`.
- `Arena` struct in `clinker-core`: `schema: Arc<Schema>`, `records: Vec<MinimalRecord>`. Constructed by streaming a `FormatReader`, extracting only the fields listed in `IndexSpec.arena_fields`. Non-indexed fields are NOT stored.
- Arena memory budget: `Arena::build()` tracks `bytes_used` during construction. If `bytes_used > memory_limit`, returns `ArenaError::MemoryBudgetExceeded { used, limit }`.
- `WindowContext<'a, S: RecordStorage>` trait updated: positional methods return `Option<RecordView<'a, S>>` (not `Box<dyn FieldResolver>`). `any`/`all` removed from trait — evaluator controls iteration. `partition_len()` and `partition_record(index)` added for evaluator-driven iteration.
- Arena must be `Send + Sync` (immutable after construction).
- **Breaking change**: existing `WindowContext` trait signature replaced entirely.

**Acceptance criteria:**
- [ ] `RecordStorage` trait defined in `clinker-record` with `Send + Sync` bounds
- [ ] `RecordView<'a, S>` implements `FieldResolver` with zero-allocation lookups
- [ ] Arena streams records from FormatReader, stores only `IndexSpec.arena_fields`
- [ ] Non-indexed fields excluded from arena storage
- [ ] Arena is `Send + Sync`
- [ ] Arena construction halts with diagnostic if memory budget exceeded
- [ ] `WindowContext<'a, S>` returns `RecordView<'a, S>` for positional functions
- [ ] `any`/`all` removed from `WindowContext` trait — evaluator iterates via `partition_len()`/`partition_record()`

#### Sub-tasks
- [ ] **5.2.1** `RecordStorage` trait in `clinker-record/src/storage.rs` — define `resolve_field(index: u32, name: &str) -> Option<Value>`, `resolve_qualified(index: u32, source: &str, field: &str) -> Option<Value>`, `available_fields(index: u32) -> Vec<&str>`. Require `Send + Sync`.
- [ ] **5.2.2** `RecordView<'a, S: RecordStorage + ?Sized>` in `clinker-record/src/record_view.rs` — `#[derive(Clone, Copy)]` struct with `storage: &'a S` and `index: u32`. Implement `FieldResolver` by delegating to `storage.resolve_field(self.index, name)`.
- [ ] **5.2.3** `Arena` in `clinker-core/src/pipeline/arena.rs` — `schema: Arc<Schema>`, `records: Vec<MinimalRecord>`. Implement `RecordStorage for Arena`: `resolve_field` looks up column index in schema, returns `records[idx].fields[col].clone()`.
- [ ] **5.2.4** `Arena::build(reader, fields, memory_limit)` — stream records, project to `arena_fields` only, track `bytes_used` via `MinimalRecord::estimated_size()`, halt with `ArenaError::MemoryBudgetExceeded` if exceeded.
- [ ] **5.2.5** Update `WindowContext<'a, S>` trait in `clinker-record/src/resolver.rs` — replace `Box<dyn FieldResolver>` returns with `RecordView<'a, S>`. Remove `any()`/`all()`. Add `partition_len() -> usize`, `partition_record(index)`, `collect(field)`, and `distinct(field)`. Replace existing object safety assertion (`_assert_window_context_object_safe`) with test-local dummy `RecordStorage` impl (avoids circular dep with `clinker-core`).
- [ ] **5.2.6** Migrate `cxl::eval` module signatures — add `S: RecordStorage` type parameter to: `eval_program`, `eval_expr`, `eval_binary`, `eval_unary`, `eval_match`, `eval_method_call`, `eval_window_call` (~8 functions). Update `cxl::resolve::traits` re-export. Mechanical change — monomorphized once for `Arena`.

#### Code scaffolding
```rust
// Module: crates/clinker-record/src/storage.rs

/// Trait for indexed record storage. Arena implements this.
/// Defined in the foundation crate so RecordView and WindowContext
/// can reference it without depending on clinker-core.
pub trait RecordStorage: Send + Sync {
    fn resolve_field(&self, index: u32, name: &str) -> Option<Value>;
    fn resolve_qualified(&self, index: u32, source: &str, field: &str) -> Option<Value>;
    fn available_fields(&self, index: u32) -> Vec<&str>;
    fn record_count(&self) -> u32;
}
```

```rust
// Module: crates/clinker-record/src/record_view.rs

/// Zero-allocation view into an arena-backed record.
/// 16 bytes: pointer + index. Copy, stack-allocated.
#[derive(Clone, Copy)]
pub struct RecordView<'a, S: RecordStorage + ?Sized> {
    storage: &'a S,
    index: u32,
}

impl<'a, S: RecordStorage + ?Sized> RecordView<'a, S> {
    pub fn new(storage: &'a S, index: u32) -> Self {
        Self { storage, index }
    }
}

impl<S: RecordStorage + ?Sized> FieldResolver for RecordView<'_, S> {
    fn resolve(&self, name: &str) -> Option<Value> {
        self.storage.resolve_field(self.index, name)
    }

    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value> {
        self.storage.resolve_qualified(self.index, source, field)
    }

    fn available_fields(&self) -> Vec<&str> {
        self.storage.available_fields(self.index)
    }
}
```

```rust
// Module: crates/clinker-core/src/pipeline/arena.rs

/// Columnar-projected record storage for Phase 1 indexing.
/// Stores only the fields needed by window expressions.
pub struct Arena {
    schema: Arc<Schema>,
    records: Vec<MinimalRecord>,
}

impl Arena {
    /// Stream records from reader, storing only the named fields.
    /// Halts with ArenaError::MemoryBudgetExceeded if bytes_used > memory_limit.
    pub fn build(
        reader: &mut dyn FormatReader,
        fields: &[String],
        memory_limit: usize,
    ) -> Result<Self, ArenaError> {
        todo!()
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
        self.schema.columns().iter().map(|s| s.as_str()).collect()
    }

    fn record_count(&self) -> u32 {
        u32::try_from(self.records.len()).expect("Arena exceeds u32::MAX records")
    }
}
```

```rust
// Module: crates/clinker-record/src/resolver.rs (updated)

/// Window partition data access.
/// Lifetime 'a ties the context to the Arena's lifetime.
/// Type parameter S is the record storage backend (Arena in production).
pub trait WindowContext<'a, S: RecordStorage> {
    /// First record in the partition. None if empty.
    fn first(&self) -> Option<RecordView<'a, S>>;

    /// Last record in the partition. None if empty.
    fn last(&self) -> Option<RecordView<'a, S>>;

    /// Record `offset` positions before current. None if out of bounds.
    fn lag(&self, offset: usize) -> Option<RecordView<'a, S>>;

    /// Record `offset` positions after current. None if out of bounds.
    fn lead(&self, offset: usize) -> Option<RecordView<'a, S>>;

    /// Number of records in the partition.
    fn count(&self) -> i64;

    /// Sum of a named field across all records in the partition.
    fn sum(&self, field: &str) -> Value;

    /// Average of a named field across all records in the partition.
    fn avg(&self, field: &str) -> Value;

    /// Minimum value of a named field across all records in the partition.
    fn min(&self, field: &str) -> Value;

    /// Maximum value of a named field across all records in the partition.
    fn max(&self, field: &str) -> Value;

    /// Number of records in the partition (for evaluator-driven iteration).
    fn partition_len(&self) -> usize;

    /// Access record at position `index` in the partition (for evaluator-driven any/all).
    fn partition_record(&self, index: usize) -> RecordView<'a, S>;

    /// Collect field values from all partition records into an Array.
    fn collect(&self, field: &str) -> Value;

    /// Collect unique field values from all partition records into an Array.
    fn distinct(&self, field: &str) -> Value;
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `RecordStorage` | `crates/clinker-record/src/storage.rs` | Foundation trait |
| `RecordView<'a, S>` | `crates/clinker-record/src/record_view.rs` | Zero-alloc view |
| `WindowContext<'a, S>` | `crates/clinker-record/src/resolver.rs` | Updated trait |
| `Arena` | `crates/clinker-core/src/pipeline/arena.rs` | Implements RecordStorage |
| `ArenaError` | `crates/clinker-core/src/pipeline/arena.rs` | MemoryBudgetExceeded |

#### Intra-phase dependencies
- Requires: Task 5.1 (`IndexSpec.arena_fields` determines which fields Arena stores)
- Unblocks: Task 5.3 (SecondaryIndex constructed from Arena)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Arena built from 100-row CSV contains 100 MinimalRecords.
    #[test]
    fn test_arena_build_from_csv() {
        // Arrange: CsvReader over 100-row test data
        // Act: Arena::build(reader, &["dept", "amount"], usize::MAX)
        // Assert: arena.record_count() == 100
    }

    /// Arena built with fields: ["dept", "amount"] stores only those 2 fields per record.
    #[test]
    fn test_arena_field_projection() {
        // Arrange: CSV with 5 columns
        // Act: Arena::build with 2 field names
        // Assert: schema has 2 columns, resolve_field for excluded column returns None
    }

    /// RecordView at index 5 resolves "dept" to correct value.
    #[test]
    fn test_arena_record_view_resolve() {
        // Arrange: Arena with known data
        // Act: RecordView::new(&arena, 5).resolve("dept")
        // Assert: equals expected value at row 5
    }

    /// RecordView::resolve("nonexistent") returns None.
    #[test]
    fn test_arena_record_view_missing_field() {
        // Arrange: Arena with fields ["dept", "amount"]
        // Act: RecordView::new(&arena, 0).resolve("nonexistent")
        // Assert: None
    }

    /// RecordView is Copy and size_of is 16 bytes (pointer + u32 + padding).
    #[test]
    fn test_arena_record_view_zero_alloc() {
        // Assert: std::mem::size_of::<RecordView<Arena>>() == 16
        // Assert: RecordView is Copy (compile-time, implicit from derive)
    }

    /// Compile-time assertion that Arena is Send + Sync.
    #[test]
    fn test_arena_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Arena>();
    }

    /// Arena built from empty reader has 0 records, schema still valid.
    #[test]
    fn test_arena_empty_input() {
        // Arrange: CsvReader over empty CSV (header only)
        // Act: Arena::build
        // Assert: record_count() == 0, schema.column_count() == N (projected fields)
    }

    /// Arena construction halts when memory budget exceeded.
    #[test]
    fn test_arena_memory_budget_exceeded() {
        // Arrange: CsvReader over large data, memory_limit = 100 bytes
        // Act: Arena::build
        // Assert: Err(ArenaError::MemoryBudgetExceeded { .. })
    }
}
```

#### Risk / gotcha
> **Value::String clone cost:** `RecordView` implements `FieldResolver::resolve()` which returns
> `Option<Value>`. For `Value::String(Box<str>)`, each resolve call clones the Box<str> (heap
> allocation). This is spec-accepted (§3: "Box<str> not Arc<str>"), but is the hot path for
> window aggregates over string fields. If profiling shows this as a bottleneck, the upgrade
> path is `Value::SharedString(Arc<str>)` — but do not add preemptively.

---

### Task 5.3: SecondaryIndex + GroupByKey
**Status:** ⛔ Blocked (waiting on Task 5.2)
**Blocked by:** Task 5.2 — Arena must exist for index construction

**Description:**
Implement `GroupByKey` enum with numeric normalization and `SecondaryIndex` as a HashMap from
composite group keys to record position lists. NaN values in group_by keys are rejected as
hard errors. Int-to-Float widening by default; integer-pinned fields via `schema_overrides`
use `GroupByKey::Int`.

**Implementation notes:**
- `GroupByKey` enum: `Str(Box<str>)`, `Int(i64)`, `Float(u64)` (stored as `f64::to_bits()` for `Eq`+`Hash`), `Bool(bool)`, `Date(NaiveDate)`, `DateTime(NaiveDateTime)`. No `Null` variant — null group_by values are excluded from the index.
- Numeric normalization: default path widens `Value::Integer(i)` to `GroupByKey::Float((*i as f64).to_bits())` so `42` and `42.0` group together. Integer-pinned path (via `schema_overrides: { type: integer }`) uses `GroupByKey::Int(i64)`; a Float value in this position is a type error routed per error strategy.
- `-0.0` canonicalization: before `to_bits()`, canonicalize `-0.0` to `0.0` so they hash to the same partition.
- `SecondaryIndex` struct: `groups: HashMap<Vec<GroupByKey>, Vec<u32>>` where `u32` is the record position in the Arena.
- Construction: `SecondaryIndex::build(arena: &Arena, group_by: &[String], schema_pins: &HashMap<String, SchemaOverride>) -> Result<SecondaryIndex, IndexError>`. Iterates arena records, extracts group_by fields, builds composite key, appends position to the group's Vec.
- NaN rejection: if any group_by field is `Value::Float(f)` where `f.is_nan()`, return `IndexError::NanInGroupBy { field, row }`. Pipeline exits with code 3.
- Null exclusion: if any group_by field is `Value::Null`, skip that record (do not insert into any group). Log at `debug` level.
- `PartitionLookup` enum: `SameSource { group_by_field_indices, index }` for direct field extraction, `CrossSource { on_expr, foreign_index }` for expression-based lookup against a different source's index.

**Acceptance criteria:**
- [ ] GroupByKey supports all non-null Value variants with Eq+Hash
- [ ] Default path: Int and Float values that represent the same number group together
- [ ] Integer-pinned path: Float values rejected as type error
- [ ] `-0.0` canonicalized to `0.0` before `to_bits()`
- [ ] SecondaryIndex maps composite keys to Arena position lists
- [ ] NaN in group_by field produces hard error (not silent skip)
- [ ] Null group_by values excluded from index (record not in any group)
- [ ] Index construction is single-pass over Arena
- [ ] `PartitionLookup` enum dispatches same-source vs cross-source lookups

#### Sub-tasks
- [ ] **5.3.1** `GroupByKey` enum in `clinker-core/src/pipeline/index.rs` — 6 variants with `Eq + Hash` derives. `Float(u64)` variant stores `f64::to_bits()`. Manual `PartialEq`/`Hash` not needed — `u64` handles both natively.
- [ ] **5.3.2** `value_to_group_key()` conversion function — takes `&Value`, field name, optional `SchemaOverride`. Default: widen Int to Float. Integer-pinned: use `GroupByKey::Int`, reject Float. NaN → `IndexError::NanInGroupBy`. Null → `None` (caller skips record). `-0.0` → `0.0` before `to_bits()`.
- [ ] **5.3.3** `SecondaryIndex::build()` — iterate Arena records, extract group_by fields, call `value_to_group_key()` for each, build composite `Vec<GroupByKey>`, insert position into `groups` HashMap. Single-pass, no sorting.
- [ ] **5.3.4** `PartitionLookup` enum — `SameSource { group_by_field_indices: Vec<usize>, index: &SecondaryIndex }` for direct field extraction from the current Phase 2 record, `CrossSource { on_expr: &TypedProgram, foreign_index: &SecondaryIndex }` for expression evaluation. `lookup()` method returns `Option<&[u32]>`.

#### Code scaffolding
```rust
// Module: crates/clinker-core/src/pipeline/index.rs

/// Group-by key for SecondaryIndex. Supports Eq + Hash for HashMap keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupByKey {
    Str(Box<str>),
    Int(i64),
    Float(u64),  // f64::to_bits(), -0.0 canonicalized to 0.0
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
}

/// Secondary index: maps composite group keys to Arena record positions.
pub struct SecondaryIndex {
    pub groups: HashMap<Vec<GroupByKey>, Vec<u32>>,
}

impl SecondaryIndex {
    /// Build index from Arena in a single pass.
    pub fn build(
        arena: &Arena,
        group_by: &[String],
        schema_pins: &HashMap<String, SchemaOverride>,
    ) -> Result<Self, IndexError> {
        todo!()
    }
}

/// Convert a Value to a GroupByKey, applying numeric normalization rules.
fn value_to_group_key(
    val: &Value,
    field: &str,
    schema_pin: Option<&SchemaOverride>,
) -> Result<Option<GroupByKey>, IndexError> {
    match val {
        Value::Null => Ok(None),  // caller skips this record
        Value::Float(f) if f.is_nan() => Err(IndexError::NanInGroupBy {
            field: field.to_string(),
            row: 0, // caller fills in
        }),
        Value::Float(f) => {
            let canonical = if *f == 0.0 { 0.0f64 } else { *f };
            Ok(Some(GroupByKey::Float(canonical.to_bits())))
        }
        Value::Integer(i) => match schema_pin {
            Some(pin) if pin.ty == "integer" => Ok(Some(GroupByKey::Int(*i))),
            _ => Ok(Some(GroupByKey::Float((*i as f64).to_bits()))),
        },
        Value::String(s) => Ok(Some(GroupByKey::Str(s.clone()))),
        Value::Bool(b) => Ok(Some(GroupByKey::Bool(*b))),
        Value::Date(d) => Ok(Some(GroupByKey::Date(*d))),
        Value::DateTime(dt) => Ok(Some(GroupByKey::DateTime(*dt))),
        Value::Array(_) => unreachable!("Arrays rejected at compile time"),
    }
}

/// Partition lookup strategy for Phase 2.
pub enum PartitionLookup<'a> {
    /// Same-source: extract group_by fields by column index, probe own SecondaryIndex.
    SameSource {
        group_by_field_indices: Vec<usize>,
        index: &'a SecondaryIndex,
    },
    /// Cross-source: evaluate `on` expression, probe foreign SecondaryIndex.
    CrossSource {
        on_expr: &'a TypedProgram,
        foreign_index: &'a SecondaryIndex,
    },
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `GroupByKey` | `crates/clinker-core/src/pipeline/index.rs` | Not in public data model |
| `SecondaryIndex` | `crates/clinker-core/src/pipeline/index.rs` | HashMap-based |
| `PartitionLookup` | `crates/clinker-core/src/pipeline/index.rs` | Phase 2 dispatch |
| `value_to_group_key` | `crates/clinker-core/src/pipeline/index.rs` | Private helper |

#### Intra-phase dependencies
- Requires: Task 5.2 (`Arena` for index construction)
- Unblocks: Task 5.4 (partition sorting over SecondaryIndex groups)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Same value produces same GroupByKey hash; different values differ.
    #[test]
    fn test_group_by_key_eq_hash() {
        // Arrange: GroupByKey::Str("a"), GroupByKey::Str("a"), GroupByKey::Str("b")
        // Assert: first two are equal and hash the same, third differs
    }

    /// Value::Integer(42) and Value::Float(42.0) produce equal GroupByKey (default widening).
    #[test]
    fn test_group_by_key_int_float_unify() {
        // Arrange: value_to_group_key(&Value::Integer(42), "x", None)
        //          value_to_group_key(&Value::Float(42.0), "x", None)
        // Assert: both produce GroupByKey::Float with same bits
    }

    /// -0.0 and 0.0 produce the same GroupByKey::Float.
    #[test]
    fn test_group_by_key_neg_zero_canonical() {
        // Arrange: value_to_group_key(&Value::Float(-0.0), "x", None)
        //          value_to_group_key(&Value::Float(0.0), "x", None)
        // Assert: equal GroupByKey
    }

    /// Integer-pinned field rejects Float values as type error.
    #[test]
    fn test_group_by_key_integer_pin_rejects_float() {
        // Arrange: schema_pin = Some(SchemaOverride { ty: "integer" })
        // Act: value_to_group_key(&Value::Float(42.0), "x", schema_pin)
        // Assert: Err(IndexError::TypeMismatch { .. })
    }

    /// Group by "dept" with 3 departments → 3 entries, correct position lists.
    #[test]
    fn test_secondary_index_single_group_by() {
        // Arrange: Arena with 10 records, 3 distinct dept values
        // Act: SecondaryIndex::build
        // Assert: groups.len() == 3, position lists sum to 10
    }

    /// Group by ["dept", "region"] → composite key grouping correct.
    #[test]
    fn test_secondary_index_composite_key() {
        // Arrange: Arena with records having (dept, region) combinations
        // Act: SecondaryIndex::build with group_by: ["dept", "region"]
        // Assert: composite keys correct, positions assigned correctly
    }

    /// NaN in group_by field → IndexError::NanInGroupBy.
    #[test]
    fn test_secondary_index_nan_rejection() {
        // Arrange: Arena with one record where group_by field is NaN
        // Act: SecondaryIndex::build
        // Assert: Err(IndexError::NanInGroupBy { field: "amount", row: N })
    }

    /// Null group_by value → record absent from all groups.
    #[test]
    fn test_secondary_index_null_exclusion() {
        // Arrange: Arena with 5 records, one has null group_by field
        // Act: SecondaryIndex::build
        // Assert: all groups' position lists sum to 4 (not 5)
    }

    /// Empty arena → empty index, no error.
    #[test]
    fn test_secondary_index_empty_arena() {
        // Arrange: Arena with 0 records
        // Act: SecondaryIndex::build
        // Assert: groups.is_empty(), no error
    }

    /// All records have null group_by → empty index, no error.
    #[test]
    fn test_secondary_index_all_nulls() {
        // Arrange: Arena where every record's group_by field is null
        // Act: SecondaryIndex::build
        // Assert: groups.is_empty(), no error
    }
}
```

#### Risk / gotcha
> **`-0.0` canonicalization:** `f64::to_bits()` produces different values for `-0.0` and `0.0`
> (sign bit differs). Without canonicalization, records with `-0.0` and `0.0` in a group_by
> field would land in separate partitions. The `value_to_group_key` function must canonicalize
> `-0.0` to `0.0` before calling `to_bits()`. Test: `assert_eq!((-0.0f64).to_bits() != (0.0f64).to_bits(), true)`.

---

### Task 5.4: Phase 1.5 Pointer Sorting + WindowContext Impl
**Status:** ⛔ Blocked (waiting on Task 5.3)
**Blocked by:** Task 5.3 — SecondaryIndex must be built for partition sorting

**Description:**
Implement Phase 1.5: sort each partition's `Vec<u32>` pointers by looking up `sort_by`
fields in the Arena. Implement `PartitionWindowContext` providing positional and aggregate window
functions over sorted partitions. Implement evaluator-driven `any`/`all` iteration with
`IteratorResolver` for `it` binding.

**Implementation notes:**
- Pointer sorting: for each group in `SecondaryIndex`, sort the `Vec<u32>` by comparing `arena.records[a]` vs `arena.records[b]` on the `sort_by` fields. Use `slice::sort_by()` (stable sort).
- Null handling in sort: configurable via `NullOrder { First, Last, Drop }`. Default: `NullOrder::Last` (SQL convention). `First` = nulls sort before all values. `Last` = nulls sort after all values. `Drop` = remove records with null sort keys from the partition.
- Sort order: `SortField { field: String, order: SortOrder, null_order: NullOrder }`. `SortOrder::Asc` / `SortOrder::Desc`.
- Pre-sorted optimization: two levels. (1) Phase E flags `IndexSpec.already_sorted = true` when config `sort_order` matches window `sort_by` (compile-time, zero-cost). (2) Runtime: before sorting each partition, linear scan checks if already sorted — skip sort on first mismatch.
- `PartitionWindowContext` struct: holds `&'a Arena`, partition `&'a [u32]`, current position `usize`. Implements `WindowContext<'a, Arena>`.
  - Positional: `first()` → `RecordView` at partition[0], `last()` → partition[len-1], `lag(offset)` → partition[pos - offset], `lead(offset)` → partition[pos + offset].
  - Aggregates: `count()` → partition.len(), `sum(field)` → iterate partition, extract field, accumulate. `avg` = sum / count. `min`/`max` via comparisons.
  - `partition_len()` and `partition_record(i)` for evaluator-driven `any`/`all` iteration.
- `any`/`all` implemented in `cxl::eval` — evaluator iterates `partition_record(0..partition_len())`, constructs `IteratorResolver { it: &view, primary: &record }`, evaluates predicate expression per record, short-circuits.
- `sum`/`avg` on non-numeric fields → return `Value::Null` (not error).

**Acceptance criteria:**
- [ ] Partitions sorted by sort_by fields with correct null handling (First/Last/Drop)
- [ ] Default NullOrder is Last (SQL convention)
- [ ] Pre-sorted detection: compile-time (Phase E flag) and runtime (linear scan)
- [ ] All positional functions (first/last/lag/lead) return correct RecordView values
- [ ] All aggregate functions (count/sum/avg/min/max) compute correctly over partitions
- [ ] any/all evaluate predicate across partition records via evaluator-driven iteration
- [ ] `IteratorResolver` correctly provides `it` binding for partition element + primary record fields
- [ ] Non-numeric sum/avg returns Null

#### Sub-tasks
- [ ] **5.4.1** Extend existing `config::SortField` with `NullOrder` — add `null_order: Option<NullOrder>` field (defaults to `None` for output sorting, `Some(Last)` for window sorting). Rename `direction` → `order` and `SortDirection` → `SortOrder` for spec consistency. Define `NullOrder { First, Last, Drop }` with `Default` returning `Last`. Add `sort_order: Option<Vec<String>>` field to `InputConfig` for Phase E pre-sorted optimization.
- [ ] **5.4.2** `sort_partition()` function — takes `&Arena`, `&mut Vec<u32>`, `&[SortField]`. Uses `slice::sort_by()` (stable). Comparison: for each `SortField`, resolve both records' values from Arena, compare with `PartialOrd`, apply `Asc`/`Desc`. Null handling: `NullOrder::First` sorts null before non-null, `Last` sorts after, `Drop` removes null entries before sorting.
- [ ] **5.4.3** Pre-sorted optimization — two checks: (a) if `IndexSpec.already_sorted == true`, skip sort entirely. (b) Otherwise, linear scan: compare consecutive pairs, if all ordered correctly, skip sort. Log at `debug` level when sort is skipped.
- [ ] **5.4.4** `PartitionWindowContext<'a>` struct — `arena: &'a Arena`, `partition: &'a [u32]`, `current_pos: usize`. Implements `WindowContext<'a, Arena>`. Positional methods index into partition slice, construct `RecordView`. Aggregate methods iterate partition, accumulate values. `partition_len()` and `partition_record(i)` for evaluator-driven iteration.
- [ ] **5.4.5** `eval_window_any` / `eval_window_all` in `cxl::eval` — evaluator-side functions. Iterate `0..w.partition_len()`, construct `RecordView` via `w.partition_record(i)`, wrap in `IteratorResolver { it: &view, primary: &record }` where `it` resolves unqualified `it.field` references. Evaluate predicate expression. Short-circuit: `any` returns `true` on first `true`, `all` returns `false` on first `false`.

#### Code scaffolding
```rust
// Module: crates/clinker-core/src/pipeline/sort.rs

/// Sort a partition's position vector in-place by sort_by fields.
/// Stable sort preserves insertion order for equal keys.
pub fn sort_partition(
    arena: &Arena,
    positions: &mut Vec<u32>,
    sort_by: &[SortField],
) {
    // Drop nulls first if any SortField has NullOrder::Drop
    positions.retain(|&pos| {
        sort_by.iter().all(|sf| {
            sf.null_order != NullOrder::Drop
                || !arena.resolve_field(pos, &sf.field).map_or(true, |v| v.is_null())
        })
    });

    positions.sort_by(|&a, &b| {
        for sf in sort_by {
            let va = arena.resolve_field(a, &sf.field);
            let vb = arena.resolve_field(b, &sf.field);
            let ord = compare_values_with_nulls(va.as_ref(), vb.as_ref(), sf);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
}

/// Check if partition is already sorted (linear scan).
pub fn is_sorted(arena: &Arena, positions: &[u32], sort_by: &[SortField]) -> bool {
    positions.windows(2).all(|pair| {
        let ord = compare_pair(arena, pair[0], pair[1], sort_by);
        ord != Ordering::Greater
    })
}
```

```rust
// Module: crates/clinker-core/src/pipeline/window_context.rs

/// Concrete WindowContext implementation over a sorted Arena partition.
pub struct PartitionWindowContext<'a> {
    arena: &'a Arena,
    partition: &'a [u32],
    current_pos: usize,
}

impl<'a> PartitionWindowContext<'a> {
    pub fn new(arena: &'a Arena, partition: &'a [u32], current_pos: usize) -> Self {
        Self { arena, partition, current_pos }
    }
}

impl<'a> WindowContext<'a, Arena> for PartitionWindowContext<'a> {
    fn first(&self) -> Option<RecordView<'a, Arena>> {
        self.partition.first().map(|&idx| RecordView::new(self.arena, idx))
    }

    fn last(&self) -> Option<RecordView<'a, Arena>> {
        self.partition.last().map(|&idx| RecordView::new(self.arena, idx))
    }

    fn lag(&self, offset: usize) -> Option<RecordView<'a, Arena>> {
        self.current_pos.checked_sub(offset)
            .and_then(|i| self.partition.get(i))
            .map(|&idx| RecordView::new(self.arena, idx))
    }

    fn lead(&self, offset: usize) -> Option<RecordView<'a, Arena>> {
        self.partition.get(self.current_pos + offset)
            .map(|&idx| RecordView::new(self.arena, idx))
    }

    fn count(&self) -> i64 { self.partition.len() as i64 }

    fn sum(&self, field: &str) -> Value {
        let mut total = 0.0f64;
        let mut has_numeric = false;
        for &pos in self.partition {
            match self.arena.resolve_field(pos, field) {
                Some(Value::Integer(i)) => { total += i as f64; has_numeric = true; }
                Some(Value::Float(f)) => { total += f; has_numeric = true; }
                _ => {}
            }
        }
        if has_numeric { Value::Float(total) } else { Value::Null }
    }

    fn avg(&self, field: &str) -> Value {
        todo!() // sum / count of numeric values
    }

    fn min(&self, field: &str) -> Value { todo!() }
    fn max(&self, field: &str) -> Value { todo!() }

    fn partition_len(&self) -> usize { self.partition.len() }

    fn partition_record(&self, index: usize) -> RecordView<'a, Arena> {
        RecordView::new(self.arena, self.partition[index])
    }
}
```

```rust
// Module: crates/cxl/src/eval/window.rs

/// Evaluator-driven any() implementation. Short-circuits on first true.
pub fn eval_window_any<'a, S: RecordStorage>(
    w: &dyn WindowContext<'a, S>,
    pred_expr: &Expr,
    typed: &TypedProgram,
    ctx: &EvalContext,
    primary: &dyn FieldResolver,
    env: &HashMap<String, Value>,
) -> Result<Value, EvalError> {
    for i in 0..w.partition_len() {
        let view = w.partition_record(i);
        let combined = IteratorResolver { it: &view, primary };
        match eval_expr(pred_expr, typed, ctx, &combined, None, env)? {
            Value::Bool(true) => return Ok(Value::Bool(true)),
            _ => {}
        }
    }
    Ok(Value::Bool(false))
}

/// Evaluator-driven all() implementation. Short-circuits on first false.
pub fn eval_window_all<'a, S: RecordStorage>(
    w: &dyn WindowContext<'a, S>,
    pred_expr: &Expr,
    typed: &TypedProgram,
    ctx: &EvalContext,
    primary: &dyn FieldResolver,
    env: &HashMap<String, Value>,
) -> Result<Value, EvalError> {
    for i in 0..w.partition_len() {
        let view = w.partition_record(i);
        let combined = IteratorResolver { it: &view, primary };
        match eval_expr(pred_expr, typed, ctx, &combined, None, env)? {
            Value::Bool(false) => return Ok(Value::Bool(false)),
            _ => {}
        }
    }
    Ok(Value::Bool(true))
}

/// Resolver that provides `it` binding (partition element) alongside primary record fields.
/// Unqualified field refs resolve against primary first, then `it`.
/// The `it` identifier resolves to the partition element's FieldResolver.
struct IteratorResolver<'a> {
    it: &'a dyn FieldResolver,
    primary: &'a dyn FieldResolver,
}

impl FieldResolver for IteratorResolver<'_> {
    fn resolve(&self, name: &str) -> Option<Value> {
        // `it` is resolved via ResolvedBinding::IteratorBinding in the evaluator,
        // not through this resolver. This handles normal field refs.
        self.primary.resolve(name)
    }

    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value> {
        self.primary.resolve_qualified(source, field)
    }

    fn available_fields(&self) -> Vec<&str> {
        self.primary.available_fields()
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `sort_partition()` | `crates/clinker-core/src/pipeline/sort.rs` | Stable sort with null handling |
| `is_sorted()` | `crates/clinker-core/src/pipeline/sort.rs` | Runtime pre-sorted check |
| `SortField`, `NullOrder`, `SortOrder` | `crates/clinker-core/src/plan/execution.rs` | Config types |
| `PartitionWindowContext` | `crates/clinker-core/src/pipeline/window_context.rs` | WindowContext impl |
| `eval_window_any`, `eval_window_all` | `crates/cxl/src/eval/window.rs` | Evaluator-driven iteration |
| `IteratorResolver` | `crates/cxl/src/eval/window.rs` | `it` binding resolver |

#### Intra-phase dependencies
- Requires: Task 5.3 (`SecondaryIndex` groups to sort)
- Unblocks: Task 5.5 (full two-pass executor needs sorted partitions + WindowContext)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// 5 records sorted by amount ASC → positions in correct order.
    #[test]
    fn test_sort_partition_ascending() {
        // Arrange: Arena with amounts [30, 10, 50, 20, 40], partition = [0,1,2,3,4]
        // Act: sort_partition with SortOrder::Asc
        // Assert: partition == [1, 3, 0, 4, 2] (10, 20, 30, 40, 50)
    }

    /// 5 records sorted by amount DESC → positions in reverse order.
    #[test]
    fn test_sort_partition_descending() {
        // Arrange: same arena
        // Act: sort_partition with SortOrder::Desc
        // Assert: partition == [2, 4, 0, 3, 1] (50, 40, 30, 20, 10)
    }

    /// NullOrder::First → null sort_by values at start of partition.
    #[test]
    fn test_sort_null_first() {
        // Arrange: Arena with amounts [30, null, 10], NullOrder::First
        // Act: sort_partition
        // Assert: null record first, then 10, then 30
    }

    /// NullOrder::Last → null sort_by values at end of partition.
    #[test]
    fn test_sort_null_last() {
        // Arrange: same arena, NullOrder::Last (default)
        // Act: sort_partition
        // Assert: 10, 30, then null record last
    }

    /// NullOrder::Drop → records with null sort_by removed from partition.
    #[test]
    fn test_sort_null_drop() {
        // Arrange: same arena, NullOrder::Drop
        // Act: sort_partition
        // Assert: partition length reduced by 1, null record absent
    }

    /// Already-sorted partition detected, sort skipped.
    #[test]
    fn test_sort_presorted_skip() {
        // Arrange: Arena with amounts [10, 20, 30] already in order
        // Act: is_sorted() check
        // Assert: returns true
    }

    /// first("name") and last("name") return correct boundary values.
    #[test]
    fn test_window_first_last() {
        // Arrange: PartitionWindowContext over sorted partition [A, B, C]
        // Act: ctx.first().resolve("name"), ctx.last().resolve("name")
        // Assert: "A" and "C" respectively
    }

    /// lag("amount", 1) at position 3 returns position 2's value; lead returns position 4's.
    #[test]
    fn test_window_lag_lead() {
        // Arrange: PartitionWindowContext at position 3 in 5-element partition
        // Act: ctx.lag(1), ctx.lead(1)
        // Assert: correct values from positions 2 and 4
    }

    /// lag("amount", 1) at position 0 returns None.
    #[test]
    fn test_window_lag_out_of_bounds() {
        // Arrange: PartitionWindowContext at position 0
        // Act: ctx.lag(1)
        // Assert: None
    }

    /// count() over 5-record partition returns 5.
    #[test]
    fn test_window_count() {
        // Arrange: PartitionWindowContext over 5-element partition
        // Assert: ctx.count() == 5
    }

    /// sum("amount") and avg("amount") correct for known values.
    #[test]
    fn test_window_sum_avg() {
        // Arrange: partition with amounts [10, 20, 30]
        // Assert: sum == 60.0, avg == 20.0
    }

    /// min("amount") and max("amount") correct for known values.
    #[test]
    fn test_window_min_max() {
        // Arrange: partition with amounts [10, 20, 30]
        // Assert: min == 10, max == 30
    }

    /// sum("name") over string field returns Value::Null.
    #[test]
    fn test_window_sum_non_numeric() {
        // Arrange: partition with string "name" field
        // Act: ctx.sum("name")
        // Assert: Value::Null
    }

    /// any(amount > 100) true when one exceeds, all(amount > 0) true when all positive.
    #[test]
    fn test_window_any_all() {
        // Arrange: partition with amounts [50, 150, 200]
        // Act: eval_window_any with predicate "it.amount > 100"
        // Assert: true (150 and 200 exceed)
        // Act: eval_window_all with predicate "it.amount > 0"
        // Assert: true (all positive)
    }

    /// collect("name") returns Array of all partition field values.
    #[test]
    fn test_window_collect() {
        // Arrange: partition with names ["Alice", "Bob", "Carol"]
        // Act: ctx.collect("name")
        // Assert: Value::Array(vec![Value::String("Alice"), Value::String("Bob"), Value::String("Carol")])
    }

    /// distinct("dept") returns Array of unique field values.
    #[test]
    fn test_window_distinct() {
        // Arrange: partition with depts ["A", "B", "A", "C"]
        // Act: ctx.distinct("dept")
        // Assert: Value::Array with 3 unique values (A, B, C), order deterministic
    }

    /// 1-record partition: first == last, lag(1) == None, lead(1) == None.
    #[test]
    fn test_window_single_record_partition() {
        // Arrange: partition with 1 record
        // Assert: first == last, lag(1) == None, lead(1) == None, count == 1
    }

    /// Composite sort: sort by [dept ASC, amount DESC].
    #[test]
    fn test_sort_partition_composite() {
        // Arrange: Arena with records having varying dept + amount
        // Act: sort_partition with two SortFields
        // Assert: sorted by dept first, then amount descending within dept
    }
}
```

#### Risk / gotcha
> **`IteratorResolver` `it` binding resolution:** The `it` identifier in predicate expressions
> (e.g., `window.any(it.Amount > 1000)`) is resolved via `ResolvedBinding::IteratorBinding`
> during Phase B. The evaluator must detect this binding and resolve against the
> `PartitionWindowContext::partition_record()` view, NOT the primary record. If `it.field`
> accidentally resolves against the primary record, predicates will silently use wrong values.
> Test: predicate `it.Amount > my_amount` where partition element's Amount differs from
> primary record's Amount — verify the partition element's value is used for `it.Amount`.

---

### Task 5.5: Full Two-Pass Executor + Provenance
**Status:** ⛔ Blocked (waiting on Task 5.4)
**Blocked by:** Task 5.4 — window functions must be working

**Description:**
Rename `StreamingExecutor` to `PipelineExecutor`. Orchestrate the complete two-pass pipeline:
compile → Phase 1 (build arena + indices sequentially in DAG order) →
Phase 1.5 (sort partitions) → Phase 2 (evaluate with windows) → Phase 3 (project + write).
Support cross-source windows via `PartitionLookup` dispatch. Populate RecordProvenance.
Update PipelineCounters at chunk boundaries.

**Implementation notes:**
- `PipelineExecutor` replaces `StreamingExecutor` (unified executor, rip and replace).
- `PipelineExecutor::run()` compiles `ExecutionPlan`, branches on `plan.mode()`:
  - `ExecutionMode::Streaming` → `execute_streaming()` (current StreamingExecutor body)
  - `ExecutionMode::TwoPass` → `execute_two_pass()` (new Phase 1→1.5→2→3 orchestration)
- Shared private methods: `compile_transforms()`, `build_eval_context()`, `evaluate_record()`, `project_output()`, `handle_error()`.
- Phase 1 orchestration: iterate `source_dag` tiers sequentially. For each tier, build Arenas for each source. For each Arena, build SecondaryIndex per IndexSpec targeting that source.
- Phase 1.5: for each SecondaryIndex, sort partitions (respecting `already_sorted` flag and runtime pre-sorted check).
- Phase 2: re-read primary source (construct new FormatReader from config path — requires file path, not stdin, for two-pass mode). Process in chunks (default 1024). For each record: compute `PartitionLookup` (SameSource field extraction or CrossSource expression eval), construct `PartitionWindowContext`, evaluate all transforms.
- Cross-source windows: `PartitionLookup::CrossSource` evaluates `on` expression against current record, probes reference source's SecondaryIndex.
- RecordProvenance: `source_file` from `config.inputs[0].path`, `source_row` incremented per record (monotonic across chunks), `ingestion_timestamp` set once at pipeline start.
- PipelineCounters: local counters per chunk, flushed to aggregate at chunk end. Error threshold checked: `error_count / total_records > type_error_threshold` → halt.
- Two-pass mode requires file path input (not stdin). If config specifies stdin and plan requires two-pass, return `PipelineError::StdinIncompatibleWithTwoPass`.

**Acceptance criteria:**
- [ ] `PipelineExecutor` replaces `StreamingExecutor` as the unified executor
- [ ] Two-pass pipeline produces correct output for window functions
- [ ] Stateless-only configs fall back to single-pass (no arena built) via `ExecutionMode::Streaming`
- [ ] Cross-source windows resolve against reference SecondaryIndex via `PartitionLookup::CrossSource`
- [ ] RecordProvenance populated with correct source_file, source_row, ingestion_timestamp
- [ ] PipelineCounters accurate at pipeline completion, updated at chunk boundaries
- [ ] Error threshold checked at chunk boundaries
- [ ] Stdin input rejected for two-pass mode with clear diagnostic
- [ ] End-to-end: CSV with window transforms → correct aggregated output

#### Sub-tasks
- [ ] **5.5.1** Rename `StreamingExecutor` → `PipelineExecutor`. Preserve existing `run_with_readers_writers` as `execute_streaming()` private method. Add `run()` entry point with `ExecutionPlan::compile()` and `plan.mode()` branching.
- [ ] **5.5.2** `execute_two_pass()` — Phase 1: iterate `source_dag` tiers sequentially, build Arena per source (via `Arena::build()`), build SecondaryIndex per IndexSpec. Phase 1.5: sort partitions per SecondaryIndex. Store all Arenas and indices in a `HashMap<SourceRef, (Arena, Vec<SecondaryIndex>)>`.
- [ ] **5.5.3** Phase 2 chunk processing — construct new FormatReader for primary source (second pass). Read chunks of 1024 records. For each record: determine `PartitionLookup` variant, compute group key, look up partition, construct `PartitionWindowContext` at correct position, evaluate all transforms via `evaluate_record()`.
- [ ] **5.5.4** `PartitionLookup` dispatch — `SameSource`: extract group_by fields by column index from the Phase 2 record, build `Vec<GroupByKey>`, probe own SecondaryIndex. `CrossSource`: evaluate `on` expression via `eval_expr()`, convert result to group key, probe foreign SecondaryIndex. Return `Option<&[u32]>` partition.
- [ ] **5.5.5** `RecordProvenance` population — set `source_file` from config input path, increment `source_row` per record (start at 1, monotonic across chunks), set `ingestion_timestamp` once at pipeline start via `Utc::now()`.
- [ ] **5.5.6** `PipelineCounters` chunk-boundary aggregation — local `ChunkCounters { processed, ok, dlq, errors }` per chunk. At chunk end: add to aggregate `PipelineCounters`. Check error threshold: if `counters.errors as f64 / counters.processed as f64 > config.error_handling.type_error_threshold` and strategy is `Continue`, halt pipeline.

#### Code scaffolding
```rust
// Module: crates/clinker-core/src/executor.rs

/// Unified pipeline executor. Replaces StreamingExecutor.
/// Plan-driven branching: Streaming (single-pass) or TwoPass (arena + windows).
pub struct PipelineExecutor;

impl PipelineExecutor {
    /// Main entry point. Compiles config into ExecutionPlan, dispatches to
    /// streaming or two-pass execution based on plan.mode().
    pub fn run<R: Read + Send, W: Write + Send>(
        config: &PipelineConfig,
        reader: R,
        writer: W,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>), PipelineError> {
        let plan = ExecutionPlan::compile(config)?;
        match plan.mode() {
            ExecutionMode::Streaming => Self::execute_streaming(&plan, config, reader, writer),
            ExecutionMode::TwoPass => Self::execute_two_pass(&plan, config, reader, writer),
        }
    }

    /// Single-pass streaming execution (no windows). Former StreamingExecutor body.
    fn execute_streaming<R: Read + Send, W: Write + Send>(
        plan: &ExecutionPlan,
        config: &PipelineConfig,
        reader: R,
        writer: W,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>), PipelineError> {
        todo!() // Moved from StreamingExecutor::run_with_readers_writers
    }

    /// Two-pass execution: Phase 1 → 1.5 → Phase 2 → Phase 3.
    fn execute_two_pass<R: Read + Send, W: Write + Send>(
        plan: &ExecutionPlan,
        config: &PipelineConfig,
        reader: R,
        writer: W,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>), PipelineError> {
        // Phase 1: Build arenas and indices (sequential, DAG order)
        let mut source_data: HashMap<SourceRef, SourcePhase1> = HashMap::new();
        for tier in &plan.source_dag {
            for source_ref in &tier.sources {
                let mut reader = open_reader(config, source_ref)?;
                let arena_fields = collect_arena_fields(plan, source_ref);
                let arena = Arena::build(&mut *reader, &arena_fields, config.memory_limit())?;

                let indices: Vec<SecondaryIndex> = plan.indices_to_build.iter()
                    .filter(|spec| &spec.source == source_ref)
                    .map(|spec| SecondaryIndex::build(&arena, &spec.group_by, &spec.schema_pins))
                    .collect::<Result<_, _>>()?;

                source_data.insert(source_ref.clone(), SourcePhase1 { arena, indices });
            }
        }

        // Phase 1.5: Sort partitions
        for (source_ref, data) in &mut source_data {
            for (idx, spec) in plan.indices_for_source(source_ref).enumerate() {
                if !spec.already_sorted {
                    for partition in data.indices[idx].groups.values_mut() {
                        if !is_sorted(&data.arena, partition, &spec.sort_by) {
                            sort_partition(&data.arena, partition, &spec.sort_by);
                        }
                    }
                }
            }
        }

        // Phase 2: Re-read primary source, evaluate with window context
        let primary = &config.inputs[0];
        let mut reader2 = open_reader_from_path(&primary.path)?;
        let transforms = Self::compile_transforms(plan)?;
        let ingestion_ts = Utc::now().naive_utc();
        let mut counters = PipelineCounters::default();
        let mut dlq = Vec::new();
        let mut source_row = 0u64;

        // Chunk-based processing
        loop {
            let chunk = read_chunk(&mut reader2, 1024)?;
            if chunk.is_empty() { break; }

            let mut chunk_counters = ChunkCounters::default();
            for record in chunk {
                source_row += 1;
                // ... evaluate record with window context
                chunk_counters.processed += 1;
            }

            counters.merge(&chunk_counters);
            Self::check_error_threshold(&counters, config)?;
        }

        Ok((counters, dlq))
    }
}

struct SourcePhase1 {
    arena: Arena,
    indices: Vec<SecondaryIndex>,
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `PipelineExecutor` | `crates/clinker-core/src/executor.rs` | Replaces StreamingExecutor |
| `SourcePhase1` | `crates/clinker-core/src/executor.rs` | Phase 1 output per source |
| `ChunkCounters` | `crates/clinker-core/src/executor.rs` | Per-chunk local counters |

#### Intra-phase dependencies
- Requires: Task 5.4 (sorted partitions + `PartitionWindowContext`)
- Unblocks: Phase 6 (parallelism layers on top of PipelineExecutor)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// sum(amount, group_by: [dept]) produces correct per-department totals.
    #[test]
    fn test_two_pass_sum_by_dept() {
        // Arrange: CSV with dept/amount columns, config with window.sum
        // Act: PipelineExecutor::run
        // Assert: output records have correct dept_total per department
    }

    /// avg(score, group_by: [region]) produces correct averages.
    #[test]
    fn test_two_pass_avg_by_region() {
        // Arrange: CSV with region/score, window.avg config
        // Act: run
        // Assert: correct averages per region
    }

    /// count(group_by: [status]) produces correct counts per status.
    #[test]
    fn test_two_pass_count_by_group() {
        // Arrange: CSV with status column, 3 distinct statuses
        // Act: run
        // Assert: count matches expected per status
    }

    /// first(name, group_by: [dept], sort_by: [hire_date]) returns earliest hire.
    #[test]
    fn test_two_pass_first_last_sorted() {
        // Arrange: CSV with dept/name/hire_date, sorted window
        // Act: run
        // Assert: first returns name with earliest hire_date per dept
    }

    /// lag(amount, 1, group_by: [dept], sort_by: [date]) returns previous row's amount.
    #[test]
    fn test_two_pass_lag_lead_sorted() {
        // Arrange: CSV with dept/amount/date, lag(1) config
        // Act: run
        // Assert: each record's lag_amount matches previous record in partition
    }

    /// Config with no windows → single-pass executor used, no arena allocated.
    #[test]
    fn test_two_pass_stateless_fallback() {
        // Arrange: config with only stateless transforms (no local_window)
        // Act: PipelineExecutor::run
        // Assert: correct output, ExecutionMode::Streaming used
    }

    /// Window referencing source: "reference" resolves against reference file's index.
    #[test]
    fn test_two_pass_cross_source_window() {
        // Arrange: primary CSV (orders) + reference CSV (customers)
        //          window config with source: "customers", on: customer_id
        // Act: run
        // Assert: window functions evaluate against customer data
    }

    /// RecordProvenance has correct source_file and sequential source_row.
    #[test]
    fn test_two_pass_provenance_populated() {
        // Arrange: config with include_provenance: true
        // Act: run
        // Assert: output records have source_file matching input path,
        //         source_row monotonically increasing from 1
    }

    /// Counters match: total, ok, dlq counts correct after run.
    #[test]
    fn test_two_pass_pipeline_counters() {
        // Arrange: CSV with 100 records, some triggering DLQ
        // Act: run
        // Assert: counters.processed == 100, ok + dlq == 100
    }

    /// Config with both stateless and window transforms → both evaluated correctly.
    #[test]
    fn test_two_pass_mixed_stateless_and_window() {
        // Arrange: two transforms — one pure arithmetic, one window.sum
        // Act: run
        // Assert: both output fields present and correct
    }

    /// Two transforms sharing same group_by → single index built, both correct.
    #[test]
    fn test_two_pass_multiple_windows_shared_index() {
        // Arrange: two transforms with identical local_window config
        // Act: run
        // Assert: correct results for both, plan shows 1 IndexSpec (not 2)
    }

    /// NaN in group_by field → pipeline exits with code 3.
    #[test]
    fn test_two_pass_nan_exit_code_3() {
        // Arrange: CSV with NaN in group_by field
        // Act: run
        // Assert: Err(PipelineError::NanInGroupBy { .. }), exit code 3
    }

    /// Stdin input with two-pass config → clear error diagnostic.
    #[test]
    fn test_two_pass_stdin_rejected() {
        // Arrange: config reading from stdin, with window transforms
        // Act: run
        // Assert: Err(PipelineError::StdinIncompatibleWithTwoPass)
    }
}
```

#### Risk / gotcha
> **Phase 2 re-read requires seekable input:** The two-pass architecture re-reads the primary
> source from disk. This requires constructing a new `FormatReader` from the file path — stdin
> cannot be re-read. If the config specifies stdin as input and the plan requires two-pass mode,
> the executor must fail early with `PipelineError::StdinIncompatibleWithTwoPass` rather than
> silently producing incorrect results. Test this explicitly.

---

## Intra-Phase Dependency Graph

```
Task 5.1 (ExecutionPlan + Compiler D-F)
    │
    └──→ Task 5.2 (Arena + RecordView)
              │
              └──→ Task 5.3 (SecondaryIndex + GroupByKey)
                        │
                        └──→ Task 5.4 (Pointer Sorting + WindowContext)
                                  │
                                  └──→ Task 5.5 (Full Two-Pass Executor)
```

Critical path: 5.1 → 5.2 → 5.3 → 5.4 → 5.5 (strictly sequential)
No parallelizable tasks — each depends on the previous.

---

## Decisions Log (drill-phase — 2026-03-30)
| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| 1 | Window info from both YAML `local_window` and AST analysis | Config is authoritative for index params; AST validates usage and extracts field set for Arena projection | 5.1, 5.2 |
| 2 | Full spec `ExecutionPlan` struct, fully populated | No stubs — source DAG, parallelism profile, output projections all implemented | 5.1 |
| 3 | `--explain` for compile-only plan display (not `--dry-run`) | Spec §6.2 defines `--explain` as compile-only; `--dry-run` is limited data processing (different feature) | 5.1 |
| 4 | Full cross-source window support in Phase 5 | Source DAG is real; reference Arenas built; Phase 6 only adds parallelism | 5.1, 5.3, 5.5 |
| 5 | Polars-style: `cxl::analyzer` for Phase D, `clinker-core::plan` for E+F | AST analysis stays in cxl crate; plan construction in core. No new crate. DataFusion/Polars pattern research confirmed. | 5.1 |
| 6 | Lifetime `WindowContext<'a, S>` with zero-alloc `RecordView<'a, S>` | Architecturally correct Rust — compile-time borrow safety, no Box/Arc overhead. GATs eliminated (no dyn safety). Research confirmed lifetimes > Arc for this pattern. | 5.2, 5.4 |
| 7 | Arena halts if memory budget exceeded during construction | Spec: "spill or halt." Phase 5 takes halt branch. Spill-to-disk deferred to Phase 6. | 5.2 |
| 8 | Arena stores group_by + sort_by + all window-referenced fields | Phase D extracts field set from window expressions; Phase E includes in IndexSpec.arena_fields. Full records come from Phase 2 re-read. | 5.1, 5.2 |
| 9 | GroupByKey: default widens Int→Float; integer-pinned uses GroupByKey::Int | Spec §7.1: schema_overrides pins to integer, rejects Float. Default path unifies 42 and 42.0. | 5.3 |
| 10 | Hybrid: `RecordStorage` trait in clinker-record + evaluator-driven any/all iteration | DataFusion/Polars push data repr to foundation crate. any/all removed from WindowContext — evaluator controls iteration with short-circuit. Zero vtable dispatch. | 5.2, 5.4 |
| 11 | Default `NullOrder::Last` (SQL convention) | Least surprising for users. `Drop` must be explicit. | 5.4 |
| 12 | Both pre-sorted optimizations: compile-time (Phase E flag) + runtime (linear scan) | Declared pre-sorted is free; runtime detection catches undeclared sorted data. Short-circuits on first mismatch. | 5.4 |
| 13 | Unified `PipelineExecutor` replaces `StreamingExecutor` | Every production engine (DataFusion, DuckDB, Polars, Flink) uses one execution framework with plan-driven branching. Research confirmed unanimously. | 5.5 |
| 14 | Sequential Phase 1 arena builds in DAG topological order | Correct orchestration first; Phase 6 layers std::thread::scope parallelism on independent source tiers. | 5.5 |
| 15 | Separate partition lookup paths: SameSource (field extraction) vs CrossSource (expression eval) | Every production engine separates group membership from FK lookup. Different semantics, different perf characteristics. Research confirmed unanimously. | 5.3, 5.5 |
| 16 | PipelineCounters updated at chunk boundaries (default 1024 records) | Natural processing unit. Error threshold checked at same boundary. Zero contention in Phase 5. | 5.5 |

## Assumptions Log (drill-phase — 2026-03-30)
| # | Assumption | Basis | Risk if wrong |
|---|-----------|-------|---------------|
| 1 | Only one `RecordStorage` backend (Arena) in production | No other storage planned in spec | Generic `S` parameter is unused complexity; but cost is near-zero (monomorphized once) |
| 2 | `WindowContext<'a, S>` lifetime does not need to escape evaluator scope | Arena outlives all evaluation; views consumed immediately to produce owned Values | If Phase 6 parallelism sends views across task boundaries, must upgrade to Arc (mechanical change) |
| 3 | Phase 2 input is always a file path, never stdin, for two-pass mode | Two-pass requires re-reading the source | Must detect and reject stdin early with clear diagnostic |
| 4 | `window.first().field_name` is represented as nested Expr in AST | Phase D field extraction depends on AST structure for postfix chains | If AST represents this differently, analyzer must adapt |
| 5 | `-0.0` appears rarely in production data | Canonicalization handles it but may not be tested in real workloads | Silent partition split if canonicalization is missed; test covers this |
