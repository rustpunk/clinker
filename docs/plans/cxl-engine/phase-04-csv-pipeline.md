# Phase 4: CSV + Minimal End-to-End Pipeline

**Status:** 🔲 Not Started
**Depends on:** Phase 3 (CXL Resolver, Type Checker, Evaluator)
**Entry criteria:** `cargo test -p cxl` passes all Phase 3 tests; CXL evaluator can evaluate stateless expressions against a `FieldResolver`
**Exit criteria:** `clinker pipeline.yaml` reads CSV, applies stateless CXL transforms, writes CSV output; DLQ captures malformed rows; `cargo test -p clinker-format -p clinker-core -p clinker` passes 50+ tests

---

## Tasks

### Task 4.1: FormatReader/FormatWriter Traits + CSV Reader
**Status:** 🔲 Not Started
**Blocked by:** Phase 3 exit criteria

**Description:**
Define the `FormatReader` and `FormatWriter` traits in `clinker-format`. Implement the CSV reader
wrapping the `csv` crate with configurable delimiter, quote character, header detection, and
UTF-8 encoding.

**Implementation notes:**
- `FormatReader` trait: `fn schema(&self) -> &Schema`, `fn next_record(&mut self) -> Result<Option<Record>, FormatError>` — streaming pull-based interface. No buffering beyond what the `csv` crate provides internally.
- `FormatWriter` trait: `fn write_record(&mut self, record: &Record, schema: &Schema) -> Result<(), FormatError>`, `fn flush(&mut self) -> Result<(), FormatError>`.
- CSV reader: wraps `csv::ReaderBuilder`. Configurable via `CsvReaderConfig { delimiter: u8, quote_char: u8, has_header: bool, encoding: Encoding }`. `Encoding` is an enum with only `Utf8` initially.
- Schema inference: if `has_header` is true, first row becomes column names. If false, generate synthetic names (`col_0`, `col_1`, ...).
- All fields read as `Value::String` initially — type coercion is the CXL layer's responsibility.
- `FormatReader` and `FormatWriter` must be `Send` but NOT required to be `Sync` (single-threaded streaming).

**Acceptance criteria:**
- [ ] `FormatReader` trait defined with `schema()` and `next_record()`
- [ ] `FormatWriter` trait defined with `write_record()` and `flush()`
- [ ] CSV reader streams records one at a time without loading entire file
- [ ] Configurable delimiter (comma, tab, pipe) and quote character
- [ ] Schema inferred from header row or generated synthetically
- [ ] Reader handles quoted fields with embedded delimiters and newlines

**Required unit tests (must pass before Task 4.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_csv_reader_basic_three_rows` | Reads 3-row CSV, returns 3 Records with correct field values | ⛔ Hard gate |
| `test_csv_reader_schema_from_header` | Schema column names match header row exactly | ⛔ Hard gate |
| `test_csv_reader_no_header_synthetic_names` | `has_header: false` produces `col_0`, `col_1`, etc. | ⛔ Hard gate |
| `test_csv_reader_tab_delimiter` | Tab-delimited file parsed correctly | ⛔ Hard gate |
| `test_csv_reader_quoted_fields` | Quoted field with embedded comma and newline preserved | ⛔ Hard gate |
| `test_csv_reader_empty_file` | Empty file returns None on first `next_record()` | ⛔ Hard gate |
| `test_csv_reader_streaming_memory` | Reading 10K rows keeps RSS under 1MB (no full-file buffering) | ⛔ Hard gate |
| `test_format_reader_is_send` | Compile-time assertion that CsvReader is `Send` | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 4.2 status remains `Blocked` until all tests above pass.

---

### Task 4.2: CSV Writer
**Status:** ⛔ Blocked (waiting on Task 4.1)
**Blocked by:** Task 4.1 — FormatReader/FormatWriter traits must exist

**Description:**
Implement the CSV writer wrapping the `csv` crate `Writer`. Serializes `Record` fields in
schema order, followed by overflow fields in deterministic order.

**Implementation notes:**
- Wraps `csv::WriterBuilder`. Configurable via `CsvWriterConfig { delimiter: u8, include_header: bool }`.
- `write_record()` iterates schema columns by index, calling `record.get(name)` for each. Then appends overflow fields sorted by key name (deterministic output).
- `Value::Null` serializes as empty string. `Value::Bool` as `"true"`/`"false"`. `Value::Date`/`Value::DateTime` as ISO-8601.
- Header row written on first `write_record()` call if `include_header` is true. Header includes schema columns then sorted overflow column names.
- `flush()` delegates to the inner `csv::Writer::flush()`.

**Acceptance criteria:**
- [ ] Writes CSV matching schema column order
- [ ] Overflow fields appended in sorted key order
- [ ] Header row written on first record if configured
- [ ] Null values serialize as empty string
- [ ] Round-trip: CSV read → write → read produces identical Records

**Required unit tests (must pass before Task 4.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_csv_writer_basic_output` | 3 records written match expected CSV string byte-for-byte | ⛔ Hard gate |
| `test_csv_writer_with_header` | First line is header when `include_header: true` | ⛔ Hard gate |
| `test_csv_writer_no_header` | No header line when `include_header: false` | ⛔ Hard gate |
| `test_csv_writer_null_as_empty` | `Value::Null` produces empty field between delimiters | ⛔ Hard gate |
| `test_csv_writer_overflow_fields_sorted` | Overflow fields appear in alphabetical key order after schema fields | ⛔ Hard gate |
| `test_csv_writer_quoting_special_chars` | Fields containing delimiter/newline are quoted | ⛔ Hard gate |
| `test_csv_roundtrip_lossless` | Read CSV → write CSV → read CSV → records equal | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 4.3 status remains `Blocked` until all tests above pass.

---

### Task 4.3: Config Parsing
**Status:** ⛔ Blocked (waiting on Task 4.2)
**Blocked by:** Task 4.2 — CSV writer must be working for end-to-end tests

**Description:**
Implement YAML configuration parsing in `clinker-core` using `serde-saphyr`. Define the
`PipelineConfig` struct tree and support environment variable interpolation.

**Implementation notes:**
- Top-level struct: `PipelineConfig { input: InputConfig, output: OutputConfig, transforms: Vec<TransformConfig>, error_handling: ErrorHandlingConfig }`.
- `InputConfig { format: FormatKind, path: PathBuf, csv: Option<CsvInputConfig>, json: Option<JsonInputConfig> }`.
- `OutputConfig { format: FormatKind, path: PathBuf, csv: Option<CsvOutputConfig>, include_unmapped: bool, mapping: Option<Vec<FieldMapping>>, exclude: Option<Vec<String>> }`.
- `TransformConfig { field: String, expression: String, when: Option<String> }`. The `expression` is raw CXL source text.
- `ErrorHandlingConfig { strategy: ErrorStrategy, dlq_path: Option<PathBuf>, max_errors: Option<u64> }`. `ErrorStrategy` enum: `FailFast`, `Continue`, `BestEffort`.
- Environment variable interpolation: scan all string values for `${VAR}` patterns. Replace with `std::env::var("VAR")`. Missing var → error unless `${VAR:-default}` syntax used.
- Validation pass after deserialization: required fields present, paths resolve (warn if not exist), unknown keys rejected (`serde(deny_unknown_fields)`), `FormatKind` matches available readers.

**Acceptance criteria:**
- [ ] `PipelineConfig` deserializes from YAML string
- [ ] All config structs have `serde(deny_unknown_fields)`
- [ ] `${VAR}` interpolation replaces environment variables
- [ ] `${VAR:-default}` provides fallback for missing variables
- [ ] Missing required fields produce clear error messages with field path
- [ ] Unknown keys produce error mentioning the offending key name

**Required unit tests (must pass before Task 4.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_config_minimal_valid_yaml` | Minimal config with input/output/transforms deserializes | ⛔ Hard gate |
| `test_config_env_var_interpolation` | `${HOME}` replaced with env value in path field | ⛔ Hard gate |
| `test_config_env_var_default_fallback` | `${MISSING:-/tmp}` resolves to `/tmp` | ⛔ Hard gate |
| `test_config_env_var_missing_no_default` | `${MISSING}` with no fallback produces error | ⛔ Hard gate |
| `test_config_unknown_key_rejected` | Extra key `"bogus_field"` produces `deny_unknown_fields` error | ⛔ Hard gate |
| `test_config_missing_required_field` | Omitting `input.path` produces descriptive error | ⛔ Hard gate |
| `test_config_error_strategy_variants` | `fail_fast`, `continue`, `best_effort` all deserialize correctly | ⛔ Hard gate |
| `test_config_full_example` | Full realistic config with all optional fields round-trips | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 4.4 status remains `Blocked` until all tests above pass.

---

### Task 4.4: Minimal Streaming Executor
**Status:** ⛔ Blocked (waiting on Task 4.3)
**Blocked by:** Task 4.3 — config parsing must work

**Description:**
Implement the single-pass streaming executor in `clinker-core`. This is the first data path:
CSV in → CXL per-record evaluation → field projection → CSV out. Stateless transforms only
(no windows, no arena, no Phase 1).

**Implementation notes:**
- `StreamingExecutor::run(config: &PipelineConfig) -> Result<PipelineCounters, PipelineError>`.
- Loop: `while let Some(record) = reader.next_record()?` → evaluate each `TransformConfig.expression` as CXL against the record → project output fields → `writer.write_record()`.
- `Record` implements `FieldResolver` (the trait from Phase 3) by delegating `resolve(name)` to `record.get(name)`.
- Output projection: if `include_unmapped` is true, all input fields pass through. `mapping` renames fields. `exclude` removes fields. Order: schema fields (mapped/unmapped), then CXL-emitted overflow fields.
- Error handling: `FailFast` → first error aborts. `Continue` → log error, increment `dlq_count`, skip record. `BestEffort` → on transform error, emit original field value unchanged.
- `max_errors` threshold: if `dlq_count > max_errors`, abort even in `Continue` mode.
- `PipelineCounters` updated: `total_count` incremented per input record, `ok_count` per successful output, `dlq_count` per error.

**Acceptance criteria:**
- [ ] End-to-end: CSV file → YAML config → transformed CSV output file
- [ ] CXL expressions evaluated per-record (field access, string concat, arithmetic, conditionals)
- [ ] `include_unmapped` passes all input fields to output
- [ ] `mapping` renames fields in output
- [ ] `exclude` removes fields from output
- [ ] `FailFast` stops on first error
- [ ] `Continue` skips bad records, writes to DLQ
- [ ] `BestEffort` uses original values on transform failure
- [ ] `PipelineCounters` reflect actual record counts

**Required unit tests (must pass before Task 4.5 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_executor_identity_passthrough` | No transforms, `include_unmapped: true` → output equals input | ⛔ Hard gate |
| `test_executor_cxl_string_concat` | Transform `full_name: first_name + " " + last_name` produces correct output | ⛔ Hard gate |
| `test_executor_cxl_arithmetic` | Transform `total: price * quantity` evaluates correctly | ⛔ Hard gate |
| `test_executor_cxl_conditional` | Transform with `if(condition, a, b)` selects correct branch | ⛔ Hard gate |
| `test_executor_field_mapping` | `mapping: [{from: "old", to: "new"}]` renames field in output | ⛔ Hard gate |
| `test_executor_exclude_fields` | `exclude: ["secret"]` removes field from output | ⛔ Hard gate |
| `test_executor_fail_fast_aborts` | Bad expression on row 2 of 10 → only 1 output row, exit error | ⛔ Hard gate |
| `test_executor_continue_skips` | Bad row 2 skipped, rows 1+3 in output, row 2 in DLQ | ⛔ Hard gate |
| `test_executor_best_effort_preserves` | Transform error → original field value kept in output | ⛔ Hard gate |
| `test_executor_max_errors_threshold` | `max_errors: 3` → aborts after 4th error even in `Continue` mode | ⛔ Hard gate |
| `test_executor_pipeline_counters` | Counters match: total=10, ok=8, dlq=2 for known bad rows | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 4.5 status remains `Blocked` until all tests above pass.

---

### Task 4.5: CLI Binary + DLQ Writer
**Status:** ⛔ Blocked (waiting on Task 4.4)
**Blocked by:** Task 4.4 — streaming executor must work

**Description:**
Wire up the `clinker` binary crate with `clap` argument parsing. Implement the DLQ (dead letter
queue) writer as a CSV file with error metadata columns. Define exit codes.

**Implementation notes:**
- `clap` derive API: `struct Cli { config: PathBuf, dry_run: bool, log_level: Option<String> }`. Positional `config` argument (the YAML path).
- `--dry-run`: parse config, validate, print execution plan summary, exit 0 without processing data.
- `--log-level`: sets `tracing` subscriber filter (`error`, `warn`, `info`, `debug`, `trace`). Default: `info`.
- DLQ writer: CSV file at `error_handling.dlq_path`. Columns: all original input fields + `_error_message: String` + `_error_field: String` + `_error_row: u64`. Written via the same `CsvWriter` from Task 4.2.
- Exit codes: `0` = success, `1` = pipeline error (transform failure in fail_fast, max_errors exceeded), `2` = config error (parse failure, missing file, validation), `4` = internal error (panic, unexpected state).
- Wire `miette` for error reporting: config errors show YAML source span, CXL errors show expression source span.

**Acceptance criteria:**
- [ ] `clinker pipeline.yaml` runs end-to-end
- [ ] `clinker --dry-run pipeline.yaml` validates without processing
- [ ] `--log-level debug` produces verbose output
- [ ] DLQ file written with error metadata columns
- [ ] Exit code 0 on success
- [ ] Exit code 1 on pipeline error
- [ ] Exit code 2 on config error (bad YAML, missing file)
- [ ] Error messages include source location (YAML line or CXL expression)

**Required unit tests (must pass before Phase 4 exit):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_cli_positional_config_path` | `clinker foo.yaml` parses config path correctly | ⛔ Hard gate |
| `test_cli_dry_run_flag` | `--dry-run` sets flag, no data processing occurs | ⛔ Hard gate |
| `test_cli_log_level_default` | No `--log-level` defaults to `info` | ⛔ Hard gate |
| `test_dlq_writer_error_columns` | DLQ output has `_error_message`, `_error_field`, `_error_row` columns | ⛔ Hard gate |
| `test_dlq_writer_preserves_original_fields` | DLQ row contains all original input field values | ⛔ Hard gate |
| `test_exit_code_0_success` | Clean run exits 0 | ⛔ Hard gate |
| `test_exit_code_1_pipeline_error` | FailFast transform error exits 1 | ⛔ Hard gate |
| `test_exit_code_2_config_error` | Missing YAML file exits 2 | ⛔ Hard gate |
| `test_exit_code_2_invalid_yaml` | Malformed YAML exits 2 with diagnostic | ⛔ Hard gate |
| `test_end_to_end_csv_transform` | Full integration: CSV in → YAML config → transformed CSV out → verify content | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 4 exit criteria not met until all tests above pass.
