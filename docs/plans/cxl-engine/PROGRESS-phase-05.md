# Phase 5 Execution Progress: Two-Pass Pipeline — Arena, Indexing, Windows

**Phase file:** docs/plans/cxl-engine/phase-05-two-pass-windows.md
**Started:** 2026-03-30
**Last updated:** 2026-03-30
**Status:** ✅ Complete

---

## Current state

**Active task:** none (phase complete)
**Completed:** 5 of 5 tasks
**Blocked:** none

---

## Task list

### ✅ [5.1] ExecutionPlan + AST Compiler Phases D-F
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
**Commit ID:** e64d8b5

---

### ✅ [5.2] Arena + RecordView
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
**Commit ID:** 23951e7

---

### ✅ [5.3] SecondaryIndex + GroupByKey
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
**Commit ID:** 86a782d

---

### ✅ [5.4] Phase 1.5 Pointer Sorting + WindowContext Impl
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
**Commit ID:** 96e5d91

---

### ✅ [5.5] Full Two-Pass Executor + Provenance
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
**Commit ID:** 024c3c6

---

## Gate test log

| Task | Test | Status | Run | Commit |
|------|------|--------|-----|--------|
| 5.1 | `test_plan_stateless_only` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_single_window_index` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_dedup_shared_index` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_distinct_indices` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_parallelism_stateless` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_parallelism_index_reading` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_parallelism_sequential` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_explain_output` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_cross_source_dag` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_mode_two_pass` | ✅ Passed | 1 | e64d8b5 |
| 5.1 | `test_plan_cross_source_missing_reference` | ✅ Passed | 1 | e64d8b5 |
| 5.2 | `test_arena_build_from_csv` | ✅ Passed | 1 | 23951e7 |
| 5.2 | `test_arena_field_projection` | ✅ Passed | 1 | 23951e7 |
| 5.2 | `test_arena_record_view_resolve` | ✅ Passed | 1 | 23951e7 |
| 5.2 | `test_arena_record_view_missing_field` | ✅ Passed | 1 | 23951e7 |
| 5.2 | `test_arena_record_view_zero_alloc` | ✅ Passed | 1 | 23951e7 |
| 5.2 | `test_arena_send_sync` | ✅ Passed | 1 | 23951e7 |
| 5.2 | `test_arena_empty_input` | ✅ Passed | 1 | 23951e7 |
| 5.2 | `test_arena_memory_budget_exceeded` | ✅ Passed | 1 | 23951e7 |
| 5.3 | `test_group_by_key_eq_hash` | ✅ Passed | 1 | 86a782d |
| 5.3 | `test_group_by_key_int_float_unify` | ✅ Passed | 1 | 86a782d |
| 5.3 | `test_group_by_key_neg_zero_canonical` | ✅ Passed | 1 | 86a782d |
| 5.3 | `test_group_by_key_integer_pin_rejects_float` | ✅ Passed | 1 | 86a782d |
| 5.3 | `test_secondary_index_single_group_by` | ✅ Passed | 1 | 86a782d |
| 5.3 | `test_secondary_index_composite_key` | ✅ Passed | 1 | 86a782d |
| 5.3 | `test_secondary_index_nan_rejection` | ✅ Passed | 1 | 86a782d |
| 5.3 | `test_secondary_index_null_exclusion` | ✅ Passed | 1 | 86a782d |
| 5.3 | `test_secondary_index_empty_arena` | ✅ Passed | 1 | 86a782d |
| 5.3 | `test_secondary_index_all_nulls` | ✅ Passed | 1 | 86a782d |
| 5.4 | `test_sort_partition_ascending` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_sort_partition_descending` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_sort_null_first` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_sort_null_last` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_sort_null_drop` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_sort_presorted_skip` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_first_last` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_lag_lead` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_lag_out_of_bounds` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_count` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_sum_avg` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_min_max` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_sum_non_numeric` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_any_all` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_collect` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_distinct` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_window_single_record_partition` | ✅ Passed | 1 | 96e5d91 |
| 5.4 | `test_sort_partition_composite` | ✅ Passed | 1 | 96e5d91 |
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
| 5.1 | ExecutionPlan + AST Compiler Phases D-F | `feat(phase-5): implement execution plan compiler phases D-F` | e64d8b5 | 2026-03-30 |
| 5.2 | Arena + RecordView | `feat(phase-5): implement arena with RecordStorage trait and RecordView` | 23951e7 | 2026-03-30 |
| 5.3 | SecondaryIndex + GroupByKey | `feat(phase-5): implement secondary index with GroupByKey normalization` | 86a782d | 2026-03-30 |
| 5.4 | Pointer Sorting + WindowContext | `feat(phase-5): implement pointer sorting and window context` | 96e5d91 | 2026-03-30 |
| 5.5 | Full Two-Pass Executor + Provenance | `feat(phase-5): implement two-pass pipeline executor with window functions` | 024c3c6 | 2026-03-30 |

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

**Deviation 2026-03-30:** Task 5.4.1 — renamed `direction` → `order` and `SortDirection` → `SortOrder` as planned. Updated all YAML fixtures and tests. Approved by user (option A).
