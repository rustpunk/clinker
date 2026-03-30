# Phase 5 Execution Progress: Two-Pass Pipeline — Arena, Indexing, Windows

**Phase file:** docs/plans/cxl-engine/phase-05-two-pass-windows.md
**Started:** 2026-03-30
**Last updated:** 2026-03-30
**Status:** 🔄 In Progress

---

## Current state

**Active task:** [5.1] ExecutionPlan + AST Compiler Phases D-F
**Completed:** 0 of 5 tasks
**Blocked:** none (Phase 4 exit criteria met)

---

## Task list

### 🔄 [5.1] ExecutionPlan + AST Compiler Phases D-F  ← ACTIVE
**Sub-tasks:** 5.1.1–5.1.7 (analyzer module, AnalysisReport, Phase E index planning, Phase F ExecutionPlan, source DAG, ExecutionMode, --explain)
**Gate tests that must pass:**
- `test_plan_stateless_only` — no windows → empty indices, all Stateless, mode == Streaming
- `test_plan_single_window_index` — one window.sum → one IndexSpec
- `test_plan_dedup_shared_index` — same group_by+sort_by → single IndexSpec
- `test_plan_distinct_indices` — different group_by → separate IndexSpecs
- `test_plan_parallelism_stateless` — pure arithmetic → Stateless classification
- `test_plan_parallelism_index_reading` — window aggregate → IndexReading
- `test_plan_parallelism_sequential` — positional with ordering → Sequential
- `test_plan_explain_output` — --explain produces plan summary
- `test_plan_cross_source_dag` — cross-source → correct DAG topological order
- `test_plan_mode_two_pass` — window config → ExecutionMode::TwoPass
- `test_plan_cross_source_missing_reference` — unknown source → PlanError
**Done when:** `ExecutionPlan::compile()` produces full spec struct from PipelineConfig + resolved AST; `--explain` displays plan; all 11 gate tests pass
**Commit:** `feat(phase-5): implement execution plan compiler phases D-F`
**Commit ID:** --

---

### ⛔ [5.2] Arena + RecordView  ← BLOCKED on [5.1] gate tests
**Sub-tasks:** 5.2.1–5.2.6 (RecordStorage trait, RecordView, Arena, Arena::build with memory budget, WindowContext breaking change, eval module signature migration)
**Gate tests that must pass:**
- `test_arena_build_from_csv` — 100-row CSV → 100 MinimalRecords
- `test_arena_field_projection` — only projected fields stored
- `test_arena_record_view_resolve` — RecordView resolves correct value
- `test_arena_record_view_missing_field` — unknown field → None
- `test_arena_record_view_zero_alloc` — size_of == 16, Copy
- `test_arena_send_sync` — compile-time Send + Sync assertion
- `test_arena_empty_input` — empty reader → 0 records, valid schema
- `test_arena_memory_budget_exceeded` — over limit → ArenaError
**Done when:** RecordStorage + RecordView in clinker-record; Arena in clinker-core; WindowContext<'a, S> replaces old trait; eval module migrated; all 8 gate tests pass
**Commit:** `feat(phase-5): implement arena with RecordStorage trait and RecordView`
**Commit ID:** --

---

### ⛔ [5.3] SecondaryIndex + GroupByKey  ← BLOCKED on [5.2] gate tests
**Sub-tasks:** 5.3.1–5.3.4 (GroupByKey enum, value_to_group_key, SecondaryIndex::build, PartitionLookup)
**Gate tests that must pass:**
- `test_group_by_key_eq_hash` — equal values hash same
- `test_group_by_key_int_float_unify` — Int(42) and Float(42.0) equal
- `test_group_by_key_neg_zero_canonical` — -0.0 and 0.0 same key
- `test_group_by_key_integer_pin_rejects_float` — schema pin rejects Float
- `test_secondary_index_single_group_by` — 3 depts → 3 groups
- `test_secondary_index_composite_key` — multi-field grouping correct
- `test_secondary_index_nan_rejection` — NaN → IndexError
- `test_secondary_index_null_exclusion` — null → record excluded
- `test_secondary_index_empty_arena` — empty → empty index
- `test_secondary_index_all_nulls` — all null → empty index
**Done when:** GroupByKey with numeric normalization; SecondaryIndex single-pass build; PartitionLookup enum; NaN/null handling; all 10 gate tests pass
**Commit:** `feat(phase-5): implement secondary index with GroupByKey normalization`
**Commit ID:** --

---

### ⛔ [5.4] Phase 1.5 Pointer Sorting + WindowContext Impl  ← BLOCKED on [5.3] gate tests
**Sub-tasks:** 5.4.1–5.4.5 (extend SortField with NullOrder, sort_partition, pre-sorted optimization, PartitionWindowContext, eval_window_any/all)
**Gate tests that must pass:**
- `test_sort_partition_ascending` — ASC sort correct
- `test_sort_partition_descending` — DESC sort correct
- `test_sort_null_first` — NullOrder::First correct
- `test_sort_null_last` — NullOrder::Last correct
- `test_sort_null_drop` — NullOrder::Drop removes nulls
- `test_sort_presorted_skip` — pre-sorted detected
- `test_window_first_last` — boundary values correct
- `test_window_lag_lead` — offset access correct
- `test_window_lag_out_of_bounds` — boundary → None
- `test_window_count` — partition count correct
- `test_window_sum_avg` — numeric aggregation correct
- `test_window_min_max` — min/max correct
- `test_window_sum_non_numeric` — string field → Null
- `test_window_any_all` — predicate evaluation with short-circuit
- `test_window_collect` — collect field values into Array
- `test_window_distinct` — unique field values into Array
- `test_window_single_record_partition` — 1-record edge case
- `test_sort_partition_composite` — multi-field sort
**Done when:** Partitions sorted with null handling; all window functions working; evaluator-driven any/all; collect/distinct; all 18 gate tests pass
**Commit:** `feat(phase-5): implement pointer sorting and window context`
**Commit ID:** --

---

### ⛔ [5.5] Full Two-Pass Executor + Provenance  ← BLOCKED on [5.4] gate tests
**Sub-tasks:** 5.5.1–5.5.6 (rename to PipelineExecutor, execute_two_pass, Phase 2 chunks, PartitionLookup dispatch, RecordProvenance, PipelineCounters)
**Gate tests that must pass:**
- `test_two_pass_sum_by_dept` — per-department sum correct
- `test_two_pass_avg_by_region` — per-region average correct
- `test_two_pass_count_by_group` — per-status count correct
- `test_two_pass_first_last_sorted` — first/last with sort_by correct
- `test_two_pass_lag_lead_sorted` — lag/lead with sort_by correct
- `test_two_pass_stateless_fallback` — no windows → single-pass, no arena
- `test_two_pass_cross_source_window` — reference source window correct
- `test_two_pass_provenance_populated` — source_file + source_row correct
- `test_two_pass_pipeline_counters` — processed/ok/dlq counts match
- `test_two_pass_mixed_stateless_and_window` — both transform types work
- `test_two_pass_multiple_windows_shared_index` — deduped index, both correct
- `test_two_pass_nan_exit_code_3` — NaN → exit code 3
- `test_two_pass_stdin_rejected` — stdin + two-pass → clear error
**Done when:** PipelineExecutor replaces StreamingExecutor; two-pass pipeline end-to-end; cross-source windows; provenance; counters; all 13 gate tests pass
**Commit:** `feat(phase-5): implement two-pass pipeline executor with window functions`
**Commit ID:** --

---

## Gate test log

| Task | Test | Status | Run | Commit |
|------|------|--------|-----|--------|
| 5.1 | `test_plan_stateless_only` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_single_window_index` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_dedup_shared_index` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_distinct_indices` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_parallelism_stateless` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_parallelism_index_reading` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_parallelism_sequential` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_explain_output` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_cross_source_dag` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_mode_two_pass` | ⛔ Not run | -- | -- |
| 5.1 | `test_plan_cross_source_missing_reference` | ⛔ Not run | -- | -- |
| 5.2 | `test_arena_build_from_csv` | ⛔ Not run | -- | -- |
| 5.2 | `test_arena_field_projection` | ⛔ Not run | -- | -- |
| 5.2 | `test_arena_record_view_resolve` | ⛔ Not run | -- | -- |
| 5.2 | `test_arena_record_view_missing_field` | ⛔ Not run | -- | -- |
| 5.2 | `test_arena_record_view_zero_alloc` | ⛔ Not run | -- | -- |
| 5.2 | `test_arena_send_sync` | ⛔ Not run | -- | -- |
| 5.2 | `test_arena_empty_input` | ⛔ Not run | -- | -- |
| 5.2 | `test_arena_memory_budget_exceeded` | ⛔ Not run | -- | -- |
| 5.3 | `test_group_by_key_eq_hash` | ⛔ Not run | -- | -- |
| 5.3 | `test_group_by_key_int_float_unify` | ⛔ Not run | -- | -- |
| 5.3 | `test_group_by_key_neg_zero_canonical` | ⛔ Not run | -- | -- |
| 5.3 | `test_group_by_key_integer_pin_rejects_float` | ⛔ Not run | -- | -- |
| 5.3 | `test_secondary_index_single_group_by` | ⛔ Not run | -- | -- |
| 5.3 | `test_secondary_index_composite_key` | ⛔ Not run | -- | -- |
| 5.3 | `test_secondary_index_nan_rejection` | ⛔ Not run | -- | -- |
| 5.3 | `test_secondary_index_null_exclusion` | ⛔ Not run | -- | -- |
| 5.3 | `test_secondary_index_empty_arena` | ⛔ Not run | -- | -- |
| 5.3 | `test_secondary_index_all_nulls` | ⛔ Not run | -- | -- |
| 5.4 | `test_sort_partition_ascending` | ⛔ Not run | -- | -- |
| 5.4 | `test_sort_partition_descending` | ⛔ Not run | -- | -- |
| 5.4 | `test_sort_null_first` | ⛔ Not run | -- | -- |
| 5.4 | `test_sort_null_last` | ⛔ Not run | -- | -- |
| 5.4 | `test_sort_null_drop` | ⛔ Not run | -- | -- |
| 5.4 | `test_sort_presorted_skip` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_first_last` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_lag_lead` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_lag_out_of_bounds` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_count` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_sum_avg` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_min_max` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_sum_non_numeric` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_any_all` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_collect` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_distinct` | ⛔ Not run | -- | -- |
| 5.4 | `test_window_single_record_partition` | ⛔ Not run | -- | -- |
| 5.4 | `test_sort_partition_composite` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_sum_by_dept` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_avg_by_region` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_count_by_group` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_first_last_sorted` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_lag_lead_sorted` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_stateless_fallback` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_cross_source_window` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_provenance_populated` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_pipeline_counters` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_mixed_stateless_and_window` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_multiple_windows_shared_index` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_nan_exit_code_3` | ⛔ Not run | -- | -- |
| 5.5 | `test_two_pass_stdin_rejected` | ⛔ Not run | -- | -- |

---

## Completed tasks

| Task | Name | Commit message | Commit ID | Completed |
|------|------|---------------|-----------|-----------|
| (none yet) | | | | |

---

## Notes

**Drill session (2026-03-30):** 16 design decisions made. Key architectural choices:
- WindowContext<'a, S: RecordStorage> with zero-alloc RecordView (lifetime-parameterized, not Arc)
- RecordStorage trait pushed to clinker-record foundation crate (DataFusion/Polars pattern)
- Evaluator-driven any/all iteration (removed from WindowContext trait)
- Unified PipelineExecutor replaces StreamingExecutor (DuckDB/Flink pattern)
- Separate partition lookup paths: SameSource (field extraction) vs CrossSource (expression eval)
- Extend existing config::SortField with NullOrder (not a new struct)

**Validation (2026-03-30):** 4 blockers found and resolved:
- V-1-1: Object safety assertion → use test-local dummy RecordStorage
- V-1-2: eval signature blast radius → added Task 5.2.6 for eval module migration
- V-6-1: SortField collision → extend existing struct with optional null_order
- V-8-1: collect/distinct missing → added to WindowContext trait and Task 5.4

See VALIDATION-phase-05.md for full report.
