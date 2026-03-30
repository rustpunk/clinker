# Phase 8 Execution Progress: Sort, DLQ Polish, CLI Completion

**Phase file:** docs/plans/cxl-engine/phase-08-sort-dlq-cli.md
**Started:** 2026-03-30
**Last updated:** 2026-03-30
**Status:** ✅ Complete

---

## Current state

**Active task:** none — Phase 8 complete
**Completed:** 4 of 4 tasks
**Blocked:** none

---

## Task list

### ✅ [8.1] External Merge Sort + Loser Tree
**Sub-tasks:**
- [ ] [8.1.1] Value::heap_size() + Record::estimated_heap_size()
- [ ] [8.1.2] Promote compare_values to pub; add compare_records_by_fields()
- [ ] [8.1.3] Memcomparable sort key encoding (sort_key.rs)
- [ ] [8.1.4] SortBuffer (sort_buffer.rs)
- [ ] [8.1.5] LoserTree + MergeEntry (loser_tree.rs)
- [ ] [8.1.6] SortFieldSpec config enum; upgrade InputConfig/OutputConfig sort_order; remove PipelineMeta.sort_output
- [ ] [8.1.7] Integrate SortBuffer into executor (source sort + output sort)
**Gate tests (13):**
- `test_sort_single_field_asc` -- 100 records sorted ascending by one integer field
- `test_sort_single_field_desc` -- 100 records sorted descending by one string field
- `test_sort_compound_keys` -- Sort by (dept ASC, salary DESC)
- `test_sort_nulls_first` -- Null values sort before all non-null
- `test_sort_nulls_last` -- Null values sort after all non-null
- `test_sort_stable_equal_keys` -- Equal keys preserve input order
- `test_loser_tree_2way_merge` -- Loser tree merges 2 sorted streams
- `test_loser_tree_16way_merge` -- Loser tree merges 16 sorted streams
- `test_loser_tree_single_stream` -- 1 stream passes through unchanged
- `test_sort_spill_triggers_on_budget` -- 1KB budget + 10KB data → spill
- `test_sort_cascade_merge` -- 32 spill files → cascade merge
- `test_sort_spill_cleanup` -- No temp files remain after sort
- `test_sort_in_memory_path` -- Small output sorted without spill
**Done when:** all 13 gate tests pass
**Commit:** `feat(phase-8): implement external merge sort with loser tree and SortBuffer`
**Commit ID:** 96b2c3b

**Validation notes:**
- V-1-1: SortFieldSpec is serde-only; resolve to Vec<SortField> in load_config(). Zero cascade.
- V-4-1: Reuse parse_memory_limit_bytes() from pipeline::memory — do NOT create new parser.
- V-4-2: Share Value::heap_size() with Arena's estimated_size() logic.
- V-7-2: Source sort in streaming mode forces buffered mode — document + tracing::info.
- V-7-3: Restrict source sort NullOrder to First/Last only (no Drop).
- V-7-4: Guard spill_threshold=0: require bytes_used > 0 before spill check.

---

### ✅ [8.2] DLQ Writer Completion
**Sub-tasks:**
- [ ] [8.2.1] Expand DlqErrorCategory to 6 variants; remove classify_error()
- [ ] [8.2.2] Thread DlqErrorCategory through evaluator error paths
- [ ] [8.2.3] Add include_reason/include_source_row to DlqConfig; column suppression
- [ ] [8.2.4] Add _cxl_source_record column for JSON/XML sources
**Gate tests (13):**
- `test_dlq_all_columns_present` -- All 6 metadata columns + source fields
- `test_dlq_uuid_v7_time_ordered` -- Sequential UUIDs monotonically increasing
- `test_dlq_uuid_v7_unique` -- 1000 UUIDs all distinct
- `test_dlq_error_category_missing_required` -- missing_required_field tag
- `test_dlq_error_category_type_coercion` -- type_coercion_failure tag
- `test_dlq_error_category_nan` -- nan_in_output_field tag
- `test_dlq_error_category_validation` -- validation_failure tag
- `test_dlq_error_category_aggregate` -- aggregate_type_error tag
- `test_dlq_error_category_required_conversion` -- required_field_conversion_failure tag
- `test_dlq_include_reason_false` -- Error columns omitted
- `test_dlq_include_source_row_false` -- Source fields omitted
- `test_dlq_source_fields_schema_order` -- Schema order preserved
- `test_dlq_timestamp_iso8601` -- Valid RFC 3339 timestamp
**Done when:** all 13 gate tests pass
**Commit:** `feat(phase-8): complete DLQ writer with 6 error categories and config flags`
**Commit ID:** --

---

### ✅ [8.3] --explain Mode + Progress Reporting
**Sub-tasks:**
- [ ] [8.3.1] ProgressReporter trait + StderrReporter/NullReporter/VecReporter
- [ ] [8.3.2] Wire ProgressReporter into executor (Phase 1 + Phase 2 progress)
- [ ] [8.3.3] Implement --explain mode (compile plan, print, no data read)
- [ ] [8.3.4] Add --quiet and --explain flags to CLI struct
**Gate tests (12):**
- `test_explain_no_data_read` -- Nonexistent input files succeed
- `test_explain_prints_ast` -- CXL expressions in output
- `test_explain_prints_type_annotations` -- Inferred types in output
- `test_explain_prints_source_dag` -- Source-transform DAG in output
- `test_explain_prints_indices` -- Indices with group_by/sort_by
- `test_explain_prints_memory_budget` -- Memory budget breakdown
- `test_explain_prints_parallelism` -- Parallelism classification (V-5-2 addition)
- `test_explain_invalid_config_exit_1` -- Invalid YAML → exit 1
- `test_progress_throttle_1sec` -- At most 1 update/sec
- `test_progress_format_with_total` -- Format with denominator + percentage
- `test_progress_format_without_total` -- Format without denominator
- `test_quiet_suppresses_progress` -- --quiet → no stderr
**Done when:** all 12 gate tests pass
**Commit:** `feat(phase-8): implement --explain mode and progress reporting`
**Commit ID:** --

---

### ✅ [8.4] CLI Flag Completion + cxl-cli
**Sub-tasks:**
- [ ] [8.4.1] Add all 12 CLI flags; reuse parse_memory_limit_bytes(); wire to executor
- [ ] [8.4.2] Update cxl eval: -e inline, --field name=value, --record JSON
- [ ] [8.4.3] Wire exit codes (0-4, 130) with graceful shutdown flush (V-8-1)
- [ ] [8.4.4] --base-dir + --allow-absolute-paths with path traversal protection (V-8-3)
**Gate tests (20):**
- `test_cli_memory_limit_suffix_k` -- 512K → 524288
- `test_cli_memory_limit_suffix_m` -- 256M → 268435456
- `test_cli_memory_limit_suffix_g` -- 2G → 2147483648
- `test_cli_memory_limit_bare_bytes` -- 1000000 → 1000000
- `test_cli_default_memory_limit` -- No flag → 256MB
- `test_cli_error_threshold_zero` -- 0 means unlimited
- `test_cli_batch_id_default_uuid` -- No flag → UUID v7
- `test_cli_quiet_flag` -- Sets progress suppression
- `test_cli_force_flag` -- Enables output overwrite
- `test_cxl_check_valid` -- Valid CXL → exit 0, "OK"
- `test_cxl_check_invalid` -- Type error → exit 1, error with span
- `test_cxl_eval_simple_expr` -- "1 + 2" → 3
- `test_cxl_eval_with_fields` -- "Price * Qty" --field Price=10.5 --field Qty=3 → 31.5
- `test_cxl_fmt_canonical` -- Canonical whitespace
- `test_exit_code_0_success` -- Clean run → 0
- `test_exit_code_1_compile_error` -- Invalid config → 1
- `test_exit_code_2_partial_dlq` -- Some DLQ → 2
- `test_exit_code_3_fatal_data` -- Threshold exceeded → 3
- `test_exit_code_4_io_error` -- Missing file → 4
- `test_exit_code_130_interrupted` -- AtomicBool → 130
**Done when:** all 20 gate tests pass
**Commit:** `feat(phase-8): complete CLI flags and cxl-cli eval/check/fmt`
**Commit ID:** --

---

## Gate test log

| Task | Test | Status | Run | Commit |
|------|------|--------|-----|--------|
| 8.1 | `test_sort_single_field_asc` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_sort_single_field_desc` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_sort_compound_keys` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_sort_nulls_first` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_sort_nulls_last` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_sort_stable_equal_keys` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_loser_tree_2way_merge` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_loser_tree_16way_merge` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_loser_tree_single_stream` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_sort_spill_triggers_on_budget` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_sort_cascade_merge` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_sort_spill_cleanup` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.1 | `test_sort_in_memory_path` | ✅ Passed | 2026-03-30 | 96b2c3b |
| 8.2 | `test_dlq_all_columns_present` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_uuid_v7_time_ordered` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_uuid_v7_unique` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_error_category_missing_required` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_error_category_type_coercion` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_error_category_nan` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_error_category_validation` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_error_category_aggregate` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_error_category_required_conversion` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_include_reason_false` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_include_source_row_false` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_source_fields_schema_order` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.2 | `test_dlq_timestamp_iso8601` | ✅ Passed | 2026-03-30 | 73dfdf7 |
| 8.3 | `test_explain_no_data_read` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_explain_prints_ast` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_explain_prints_type_annotations` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_explain_prints_source_dag` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_explain_prints_indices` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_explain_prints_memory_budget` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_explain_prints_parallelism` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_explain_invalid_config_exit_1` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_progress_throttle_1sec` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_progress_format_with_total` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_progress_format_without_total` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.3 | `test_quiet_suppresses_progress` | ✅ Passed | 2026-03-30 | a95e61f |
| 8.4 | `test_cli_memory_limit_suffix_k` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cli_memory_limit_suffix_m` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cli_memory_limit_suffix_g` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cli_memory_limit_bare_bytes` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cli_default_memory_limit` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cli_error_threshold_zero` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cli_batch_id_default_uuid` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cli_quiet_flag` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cli_force_flag` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cxl_check_valid` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cxl_check_invalid` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cxl_eval_simple_expr` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cxl_eval_with_fields` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_cxl_fmt_canonical` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_exit_code_0_success` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_exit_code_1_compile_error` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_exit_code_2_partial_dlq` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_exit_code_3_fatal_data` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_exit_code_4_io_error` | ✅ Passed | 2026-03-30 | bf08550 |
| 8.4 | `test_exit_code_130_interrupted` | ✅ Passed | 2026-03-30 | bf08550 |

---

## Completed tasks

| Task | Name | Commit message | Commit ID | Completed |
|------|------|---------------|-----------|-----------|
| 8.1 | External Merge Sort + Loser Tree | feat(phase-8): implement external merge sort with loser tree and SortBuffer | 96b2c3b | 2026-03-30 |
| 8.2 | DLQ Writer Completion | feat(phase-8): complete DLQ writer with 6 error categories and config flags | 73dfdf7 | 2026-03-30 |
| 8.3 | --explain + Progress Reporting | feat(phase-8): implement --explain mode and progress reporting | a95e61f | 2026-03-30 |
| 8.4 | CLI Flags + cxl-cli | feat(phase-8): complete CLI flags and cxl-cli eval/check/fmt | bf08550 | 2026-03-30 |

---

## Notes
- Phase 8 drilled 2026-03-30: 19 decisions made, 4 assumptions logged
- Phase 8 validated 2026-03-30: 3 blockers resolved, 18 warnings logged (see VALIDATION-phase-08.md)
- Spec amended 2026-03-30: §4 sort_output removed, §7.2 sort_order semantic changed, §7.5/§8.4/§9.3 updated to per-output sort_order
