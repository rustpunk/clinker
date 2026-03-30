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
- DLQ writer reuses the format writer for the configured output format (CSV, JSON, etc.).
- Source fields appended in schema order, preserving original field names.

**Acceptance criteria:**
- [ ] DLQ output contains all 6 metadata columns plus source fields
- [ ] UUID v7 IDs are time-ordered and unique across records
- [ ] All 6 error categories correctly assigned based on rejection reason
- [ ] `include_reason: false` suppresses error category and detail columns
- [ ] `include_source_row: false` suppresses source field columns
- [ ] DLQ records written in same format as configured output

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
| `test_explain_invalid_config_exit_1` | Invalid YAML produces exit code 1 | ⛔ Hard gate |
| `test_progress_throttle_1sec` | Progress callback invoked at most once per second | ⛔ Hard gate |
| `test_progress_format_with_total` | Progress line matches `[cxl] file: Phase N name... X/Y records (Z%) [T]` | ⛔ Hard gate |
| `test_progress_format_without_total` | When total unknown, format omits denominator and percentage | ⛔ Hard gate |
| `test_quiet_suppresses_progress` | With `--quiet`, no progress output written to stderr | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 8.4 status remains `Blocked` until all tests above pass.

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
  - `cxl eval <expr> --field name=value ...`: evaluate CXL expression with provided field values, print result
  - `cxl fmt <file.cxl>`: pretty-print CXL file to stdout (canonical formatting)
- Exit codes: 0 = success, 1 = config/parse error, 2 = runtime error (data processing failure), 3 = partial success (some DLQ records, threshold not exceeded), 4 = threshold exceeded (too many DLQ records)

**Acceptance criteria:**
- [ ] All 12 CLI flags parsed and applied correctly
- [ ] Memory limit suffix parsing (256M → 268435456 bytes)
- [ ] `cxl check` reports parse and type errors with source spans
- [ ] `cxl eval` evaluates expression with field inputs and prints result
- [ ] `cxl fmt` produces canonical pretty-printed CXL output
- [ ] Exit code 0 for clean success
- [ ] Exit code 1 for config/parse errors
- [ ] Exit code 2 for runtime failures
- [ ] Exit code 3 for partial success with DLQ records
- [ ] Exit code 4 for error threshold exceeded

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
| `test_cxl_eval_simple_expr` | `cxl eval "1 + 2"` → prints `3` | ⛔ Hard gate |
| `test_cxl_eval_with_fields` | `cxl eval "Price * Qty" --field Price=10.5 --field Qty=3` → prints `31.5` | ⛔ Hard gate |
| `test_cxl_fmt_canonical` | `cxl fmt` reformats whitespace to canonical form | ⛔ Hard gate |
| `test_exit_code_0_success` | Clean pipeline run → exit 0 | ⛔ Hard gate |
| `test_exit_code_1_parse_error` | Invalid YAML → exit 1 | ⛔ Hard gate |
| `test_exit_code_2_runtime_error` | Missing input file → exit 2 | ⛔ Hard gate |
| `test_exit_code_3_partial_dlq` | Some DLQ records, under threshold → exit 3 | ⛔ Hard gate |
| `test_exit_code_4_threshold_exceeded` | DLQ count exceeds `--error-threshold` → exit 4 | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 8 exit criteria not met until all tests above pass.
