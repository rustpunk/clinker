# Phase 1 Execution Progress: Types + State

**Phase file:** docs/plans/kiln-debugger/phase-01-types-and-state.md
**Started:** 2026-03-31
**Last updated:** 2026-03-31
**Status:** 🔄 In Progress

---

## Current state

**Active task:** [1.0] Create test scaffold for Phase 1
**Completed:** 0 of 4 tasks
**Blocked:** none

---

## Task list

### 🔄 [1.0] Create test scaffold for Phase 1  ← ACTIVE
**Gate tests:** none (this task IS the gate)
**Done when:** `cargo test -p clinker-kiln --lib debug_state` shows "running 0 tests" (module discovered)
**Commit:** `docs(phase-1): add test scaffold for debug state types`
**Commit ID:** --

Sub-tasks:
- [x] **1.0.1** Create `src/debug_state.rs` with `#[cfg(test)] mod tests {}`, add `mod debug_state;` to `main.rs`
- [ ] **1.0.G** Gate: `cargo test -p clinker-kiln --lib debug_state` shows "running 0 tests" (module discovered)

---

### ⛔ [1.1] Define debug enums and value types  ← BLOCKED on [1.0] gate tests
**Gate tests that must pass first:**
- `test_cell_value_display_null_shows_null` -- CellValue::Null displays "null"
- `test_cell_value_display_formats_correctly` -- Int/Float/Bool/Str display
- `test_cell_value_display_edge_cases` -- Empty string, negative int, NaN, infinity
- `test_cell_value_css_class_mapping` -- Each variant maps to correct CSS class
- `test_cell_value_display_array_short` -- Array <=5 elements renders inline
- `test_cell_value_display_array_truncated` -- Array >5 elements truncates
- `test_cell_value_display_array_nested` -- Nested arrays render recursively
- `test_cell_value_display_array_empty` -- Empty array renders []
- `test_cell_value_css_class_array` -- Array maps to kiln-debug-td--array
- `test_cell_value_from_record_value` -- All Value variants convert correctly
- `test_cell_value_from_record_value_date_to_str` -- Date/DateTime collapse to Str
- `test_cell_value_from_record_value_array` -- Value::Array converts recursively
- `test_cell_value_from_record_value_array_nested` -- Nested Value::Array converts
- `test_compact_scalars_pass_through` -- Compact on scalars matches full Display
- `test_compact_depth_2_shows_one_nesting` -- depth 2 shows one nesting level
- `test_compact_depth_1_collapses_inner` -- depth 1 collapses inner arrays
- `test_compact_depth_0_placeholder` -- depth 0 shows [N items]
- `test_compact_empty_array_depth_0` -- Empty array at depth 0 renders []
- `test_compact_depth_3_nesting` -- 3-level nesting at depth 2
- `test_diff_eq_nan_is_equal` -- NaN == NaN under diff_eq
- `test_diff_eq_zero_signs_equal` -- +0.0 == -0.0 under diff_eq
- `test_diff_eq_array_with_nan` -- diff_eq recurses into arrays with NaN
- `test_debug_run_state_clone_and_eq` -- Clone + PartialEq on all 4 variants
- `test_debug_run_state_not_copy` -- Running/Paused contain String (informational)
- `test_debug_tab_default_is_output` -- DebugTab::default() == Output
- `test_debug_tab_is_copy` -- DebugTab is Copy
- `test_stage_perf_stats_clone` -- StagePerfStats can be cloned
**Done when:** All 27 tests pass — `cargo test -p clinker-kiln --lib debug_state`
**Commit:** `feat(phase-1): define debug enums and value types`
**Commit ID:** --

Sub-tasks:
- [ ] **1.1.1** Define `DebugRunState` and `DebugTab` enums with derives
- [ ] **1.1.2** Define `CellValue` enum with `Display`, `cell_css_class()`, and `From<clinker_record::Value>`
- [ ] **1.1.3** Define `StagePerfStats` struct
- [ ] **1.1.4** Define `CompactCellValue` newtype wrapper with depth-limited `Display`
- [ ] **1.1.G** Gate: all 27 Task 1.1 tests pass

---

### ⛔ [1.2] Define StageDebugData and WatchExpression  ← BLOCKED on [1.1] gate tests
**Gate tests that must pass first:**
- `test_dropped_row_preserves_reason` -- DroppedRow stores cells and reason independently
- `test_dropped_row_empty_cells` -- DroppedRow with zero cells + reason is valid
- `test_stage_debug_data_clone` -- Can clone a fully populated StageDebugData
- `test_stage_debug_data_empty` -- StageDebugData with all empty vecs is valid
- `test_stage_debug_data_dropped_rows_typed` -- dropped_rows is Vec<DroppedRow>
- `test_watch_expression_none_values` -- WatchExpression with all None values is valid
- `test_watch_expression_mixed_values` -- WatchExpression with Some and None values
**Done when:** All 7 tests pass — `cargo test -p clinker-kiln --lib debug_state -- test_dropped test_stage_debug test_watch`
**Commit:** `feat(phase-1): define StageDebugData and WatchExpression`
**Commit ID:** --

Sub-tasks:
- [ ] **1.2.1** Define `DroppedRow` struct and `StageDebugData` struct
- [ ] **1.2.2** Define `WatchExpression` struct
- [ ] **1.2.G** Gate: all 7 tests pass

---

### ⛔ [1.3] Define DebugState context and use_debug_state()  ← BLOCKED on [1.2] gate tests
**Gate tests that must pass first:** None (verified by compilation)
**Done when:** `cargo check --workspace` and `cargo clippy --workspace -- -D warnings` both pass
**Commit:** `feat(phase-1): add debug state types and context`
**Commit ID:** --

Sub-tasks:
- [ ] **1.3.1** Define `DebugState` struct with 11 signal fields and `use_debug_state()` in `debug_state.rs`
- [ ] **1.3.2** Wire `use_context_provider(|| DebugState { ... })` in `app.rs` after `TabManagerState` provider
- [ ] **1.3.G** Gate: `cargo check --workspace` and `cargo clippy --workspace -- -D warnings` both pass

---

## Gate test log

| Task | Test | Status | Run | Commit |
|------|------|--------|-----|--------|
| 1.0 | (scaffold only) | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_display_null_shows_null` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_display_formats_correctly` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_display_edge_cases` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_css_class_mapping` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_display_array_short` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_display_array_truncated` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_display_array_nested` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_display_array_empty` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_css_class_array` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_from_record_value` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_from_record_value_date_to_str` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_from_record_value_array` | ⛔ Not run | -- | -- |
| 1.1 | `test_cell_value_from_record_value_array_nested` | ⛔ Not run | -- | -- |
| 1.1 | `test_compact_scalars_pass_through` | ⛔ Not run | -- | -- |
| 1.1 | `test_compact_depth_2_shows_one_nesting` | ⛔ Not run | -- | -- |
| 1.1 | `test_compact_depth_1_collapses_inner` | ⛔ Not run | -- | -- |
| 1.1 | `test_compact_depth_0_placeholder` | ⛔ Not run | -- | -- |
| 1.1 | `test_compact_empty_array_depth_0` | ⛔ Not run | -- | -- |
| 1.1 | `test_compact_depth_3_nesting` | ⛔ Not run | -- | -- |
| 1.1 | `test_diff_eq_nan_is_equal` | ⛔ Not run | -- | -- |
| 1.1 | `test_diff_eq_zero_signs_equal` | ⛔ Not run | -- | -- |
| 1.1 | `test_diff_eq_array_with_nan` | ⛔ Not run | -- | -- |
| 1.1 | `test_debug_run_state_clone_and_eq` | ⛔ Not run | -- | -- |
| 1.1 | `test_debug_run_state_not_copy` | ⛔ Not run | -- | -- |
| 1.1 | `test_debug_tab_default_is_output` | ⛔ Not run | -- | -- |
| 1.1 | `test_debug_tab_is_copy` | ⛔ Not run | -- | -- |
| 1.1 | `test_stage_perf_stats_clone` | ⛔ Not run | -- | -- |
| 1.2 | `test_dropped_row_preserves_reason` | ⛔ Not run | -- | -- |
| 1.2 | `test_dropped_row_empty_cells` | ⛔ Not run | -- | -- |
| 1.2 | `test_stage_debug_data_clone` | ⛔ Not run | -- | -- |
| 1.2 | `test_stage_debug_data_empty` | ⛔ Not run | -- | -- |
| 1.2 | `test_stage_debug_data_dropped_rows_typed` | ⛔ Not run | -- | -- |
| 1.2 | `test_watch_expression_none_values` | ⛔ Not run | -- | -- |
| 1.2 | `test_watch_expression_mixed_values` | ⛔ Not run | -- | -- |
| 1.3 | (compilation gate only) | ⛔ Not run | -- | -- |

---

## Completed tasks

| Task | Name | Commit message | Commit ID | Completed |
|------|------|---------------|-----------|-----------|
| (none yet) | | | | |

---

## Notes
[Any decisions made during execution, gotchas hit, deviations from the plan]
