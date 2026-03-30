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
Move `FieldResolver` and `WindowContext` traits from `cxl::resolve::traits` to `clinker-record` (dependency inversion — Bevy/Tower pattern). Implement `FieldResolver` for `Record` directly. Define `FormatReader` and `FormatWriter` traits in `clinker-format`. Implement the CSV reader wrapping the `csv` crate.

**Implementation notes:**
- **Trait migration:** Move `FieldResolver` and `WindowContext` from `cxl::resolve::traits` to `clinker-record`. `cxl` re-exports from `clinker-record` for backwards compatibility. `Record` implements `FieldResolver` directly by delegating `resolve(name)` to `record.get(name).cloned()`. `HashMapResolver` stays in `cxl::resolve::test_double`, importing the trait from `clinker-record`. All Phase 3 imports updated.
- `FormatReader` trait (spec SS11.4 signature): `fn schema(&mut self) -> Result<Arc<Schema>>` — `&mut self` because CSV must read headers to know schema. `fn next_record(&mut self) -> Result<Option<Record>, FormatError>` — streaming pull-based. No buffering beyond `csv` crate internals.
- `FormatWriter` trait (spec SS11.4 signature): `fn write_record(&mut self, record: &Record) -> Result<(), FormatError>`, `fn flush(&mut self) -> Result<(), FormatError>`. Writer stores `Arc<Schema>` internally (passed at construction, not per-call).
- `FormatError` enum: `Io(std::io::Error)`, `Csv(csv::Error)`, `InvalidRecord { row: u64, message: String }`, `SchemaInference(String)`. `#[non_exhaustive]` for Phase 7 extensibility.
- `FormatReader` and `FormatWriter` must be `Send` but NOT `Sync` (single-threaded streaming).
- CSV reader: wraps `csv::ReaderBuilder`. Configurable via `CsvReaderConfig { delimiter: u8, quote_char: u8, has_header: bool }`.
- Schema inference: if `has_header` is true, first row becomes column names. If false, generate `col_0`, `col_1`, ...
- All fields read as `Value::String` — type coercion is CXL's responsibility.

**Acceptance criteria:**
- [ ] `FieldResolver` and `WindowContext` traits live in `clinker-record`
- [ ] `Record` implements `FieldResolver` directly (no newtype wrapper)
- [ ] All Phase 3 tests still pass after trait migration
- [ ] `FormatReader` trait defined with `schema()` and `next_record()`
- [ ] `FormatWriter` trait defined with `write_record()` and `flush()`
- [ ] `FormatError` enum defined with `#[non_exhaustive]`
- [ ] CSV reader streams records one at a time without loading entire file
- [ ] Configurable delimiter (comma, tab, pipe) and quote character
- [ ] Schema inferred from header row or generated synthetically

**Required unit tests (must pass before Task 4.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_record_implements_field_resolver` | `Record` directly implements `FieldResolver`, `resolve("name")` returns correct value | ⛔ Hard gate |
| `test_csv_reader_basic_three_rows` | Reads 3-row CSV, returns 3 Records with correct field values | ⛔ Hard gate |
| `test_csv_reader_schema_from_header` | Schema column names match header row exactly | ⛔ Hard gate |
| `test_csv_reader_no_header_synthetic_names` | `has_header: false` produces `col_0`, `col_1`, etc. | ⛔ Hard gate |
| `test_csv_reader_tab_delimiter` | Tab-delimited file parsed correctly | ⛔ Hard gate |
| `test_csv_reader_quoted_fields` | Quoted field with embedded comma and newline preserved | ⛔ Hard gate |
| `test_csv_reader_empty_file` | Empty file returns None on first `next_record()` | ⛔ Hard gate |
| `test_format_reader_is_send` | Compile-time assertion that CsvReader is `Send` | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 4.2 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **[4.1.1]** Move `FieldResolver` + `WindowContext` traits from `cxl::resolve::traits` to `clinker-record`. Implement `FieldResolver` for `Record`. Update all imports in `cxl`. Verify all 152 Phase 3 tests still pass.
- [ ] **[4.1.2]** Define `FormatReader`/`FormatWriter` traits + `FormatError` enum in `clinker-format`
- [ ] **[4.1.3]** Implement `CsvReader` wrapping `csv::ReaderBuilder` + all 8 gate tests

#### Code scaffolding
```rust
// Module: clinker-record/src/resolver.rs (NEW — traits moved here)

use crate::Value;

/// Resolve a field name to a value from the current record.
/// Object-safe: usable as `dyn FieldResolver`.
pub trait FieldResolver {
    fn resolve(&self, name: &str) -> Option<Value>;
    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value>;
    fn available_fields(&self) -> Vec<&str>;
}

/// Access window partition data (Arena + Secondary Index).
/// Object-safe: usable as `dyn WindowContext`.
pub trait WindowContext {
    fn first(&self) -> Option<Box<dyn FieldResolver>>;
    fn last(&self) -> Option<Box<dyn FieldResolver>>;
    fn lag(&self, offset: usize) -> Option<Box<dyn FieldResolver>>;
    fn lead(&self, offset: usize) -> Option<Box<dyn FieldResolver>>;
    fn count(&self) -> i64;
    fn sum(&self, field: &str) -> Value;
    fn avg(&self, field: &str) -> Value;
    fn min(&self, field: &str) -> Value;
    fn max(&self, field: &str) -> Value;
    fn any(&self, predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool;
    fn all(&self, predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool;
}
```

```rust
// Module: clinker-record/src/record.rs (add FieldResolver impl)

impl FieldResolver for Record {
    fn resolve(&self, name: &str) -> Option<Value> {
        self.get(name).cloned()
    }
    fn resolve_qualified(&self, _source: &str, field: &str) -> Option<Value> {
        self.get(field).cloned()
    }
    fn available_fields(&self) -> Vec<&str> {
        self.schema().columns().iter().map(|c| &**c).collect()
    }
}
```

```rust
// Module: clinker-format/src/error.rs

#[derive(Debug)]
#[non_exhaustive]
pub enum FormatError {
    Io(std::io::Error),
    Csv(csv::Error),
    InvalidRecord { row: u64, message: String },
    SchemaInference(String),
}
```

```rust
// Module: clinker-format/src/traits.rs

use std::sync::Arc;
use clinker_record::{Record, Schema};
use crate::error::FormatError;

/// Streaming record reader. Yields records one at a time.
pub trait FormatReader: Send {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError>;
    fn next_record(&mut self) -> Result<Option<Record>, FormatError>;
}

/// Streaming record writer. Consumes records one at a time.
pub trait FormatWriter: Send {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError>;
    fn flush(&mut self) -> Result<(), FormatError>;
}
```

```rust
// Module: clinker-format/src/csv/reader.rs

pub struct CsvReaderConfig {
    pub delimiter: u8,
    pub quote_char: u8,
    pub has_header: bool,
}

pub struct CsvReader<R: std::io::Read> {
    inner: csv::Reader<R>,
    schema: Option<Arc<Schema>>,
    config: CsvReaderConfig,
    row_count: u64,
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `FieldResolver` | `clinker-record/src/resolver.rs` | Moved from `cxl::resolve::traits` |
| `WindowContext` | `clinker-record/src/resolver.rs` | Moved from `cxl::resolve::traits` |
| `impl FieldResolver for Record` | `clinker-record/src/record.rs` | Direct impl, no newtype |
| `FormatReader` | `clinker-format/src/traits.rs` | Trait for all format readers |
| `FormatWriter` | `clinker-format/src/traits.rs` | Trait for all format writers |
| `FormatError` | `clinker-format/src/error.rs` | `#[non_exhaustive]` enum |
| `CsvReader` | `clinker-format/src/csv/reader.rs` | Wraps `csv::ReaderBuilder` |
| `CsvReaderConfig` | `clinker-format/src/csv/reader.rs` | Configuration struct |

#### Intra-phase dependencies
- Requires: Phase 3 complete (`cxl` crate with evaluator)
- Unblocks: Task 4.2 (CSV writer needs traits defined)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{FieldResolver, Value};

    #[test]
    fn test_record_implements_field_resolver() {
        let schema = Arc::new(Schema::new(vec!["name".into(), "age".into()]));
        let record = Record::new(schema, vec![
            Value::String("Ada".into()),
            Value::Integer(30),
        ]);
        assert_eq!(record.resolve("name"), Some(Value::String("Ada".into())));
        assert_eq!(record.resolve("age"), Some(Value::Integer(30)));
        assert_eq!(record.resolve("unknown"), None);
    }

    #[test]
    fn test_csv_reader_basic_three_rows() {
        let csv = "name,age\nAlice,30\nBob,25\nCharlie,35";
        let mut reader = CsvReader::from_reader(csv.as_bytes(), CsvReaderConfig::default());
        let _schema = reader.schema().unwrap();
        let mut count = 0;
        while reader.next_record().unwrap().is_some() { count += 1; }
        assert_eq!(count, 3);
    }

    // ... 6 more gate tests as specified
}
```

#### Risk / gotcha
> **Trait migration risk:** Moving `FieldResolver` and `WindowContext` from `cxl` to `clinker-record`
> touches all Phase 3 import paths. The `cxl` crate must re-export from `clinker-record` or all
> consumers (Phase 3 tests, evaluator, resolver) must update their `use` statements. Run the full
> 152-test suite after migration to catch any breakage.

---

### Task 4.2: CSV Writer
**Status:** ⛔ Blocked (waiting on Task 4.1)
**Blocked by:** Task 4.1 — FormatReader/FormatWriter traits must exist

**Description:**
Implement the CSV writer wrapping the `csv` crate `Writer`. Serializes `Record` fields in
schema order, followed by overflow fields in emit order (IndexMap insertion order).

**Implementation notes:**
- Wraps `csv::WriterBuilder`. Configurable via `CsvWriterConfig { delimiter: u8, include_header: bool }`.
- `write_record()` iterates schema columns by index, calling `record.get(name)` for each. Then appends overflow fields in emit order (IndexMap insertion order). Output order matches input order when no sort steps are configured.
- Value serialization table (spec-compliant, deterministic):
  - `Null` → `""` (empty string)
  - `Bool` → `"true"` / `"false"` (lowercase, canonical)
  - `Integer` → `n.to_string()` (decimal)
  - `Float` → `f.to_string()` (ryu — shortest round-trip representation)
  - `String` → as-is (csv crate handles quoting)
  - `Date` → `"%Y-%m-%d"` (ISO-8601)
  - `DateTime` → `"%Y-%m-%dT%H:%M:%S"` (ISO-8601, no timezone)
  - `Array` → JSON array string via `serde_json`
- Boolean input is case-insensitive (TRUE, True, true all accepted by CXL `.to_bool()`). Boolean output is always lowercase.
- Header row written on first `write_record()` call if `include_header` is true.
- `flush()` delegates to inner `csv::Writer::flush()`.

**Acceptance criteria:**
- [ ] Writes CSV matching schema column order
- [ ] Overflow fields appended in emit order (IndexMap insertion order)
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
| `test_csv_writer_overflow_fields_emit_order` | Overflow fields appear in emit order (insertion order) after schema fields | ⛔ Hard gate |
| `test_csv_writer_quoting_special_chars` | Fields containing delimiter/newline are quoted | ⛔ Hard gate |
| `test_csv_roundtrip_lossless` | Read CSV → write CSV → read CSV → records equal | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 4.3 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **[4.2.1]** Implement `CsvWriter` wrapping `csv::WriterBuilder` with Value serialization table
- [ ] **[4.2.2]** Write all 7 gate tests including round-trip

#### Code scaffolding
```rust
// Module: clinker-format/src/csv/writer.rs

pub struct CsvWriterConfig {
    pub delimiter: u8,
    pub include_header: bool,
}

pub struct CsvWriter<W: std::io::Write> {
    inner: csv::Writer<W>,
    schema: Arc<Schema>,
    config: CsvWriterConfig,
    header_written: bool,
}

impl<W: std::io::Write> CsvWriter<W> {
    pub fn new(writer: W, schema: Arc<Schema>, config: CsvWriterConfig) -> Self { ... }
}

impl<W: std::io::Write + Send> FormatWriter for CsvWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> { ... }
    fn flush(&mut self) -> Result<(), FormatError> { ... }
}

/// Serialize a Value to a CSV cell string.
fn value_to_csv_cell(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => if *b { "true" } else { "false" }.into(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.to_string(),
        Value::Date(d) => d.format("%Y-%m-%d").to_string(),
        Value::DateTime(dt) => dt.format("%Y-%m-%dT%H:%M:%S").to_string(),
        Value::Array(arr) => serde_json::to_string(arr).unwrap_or_default(),
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `CsvWriter` | `clinker-format/src/csv/writer.rs` | Wraps `csv::Writer` |
| `CsvWriterConfig` | `clinker-format/src/csv/writer.rs` | Config struct |
| `value_to_csv_cell` | `clinker-format/src/csv/writer.rs` | Serialization table |

#### Intra-phase dependencies
- Requires: Task 4.1 (FormatWriter trait, Schema, Record)
- Unblocks: Task 4.3 (config parsing needs writer for end-to-end tests)

#### Risk / gotcha
> **Float determinism:** `f64::to_string()` uses ryu (Rust's default Display impl for f64),
> which produces the shortest round-trip representation. This is deterministic across platforms
> and Rust versions. However, the round-trip test must account for this: reading `"1.0"` as a
> string, writing it back will produce `"1.0"` (not `"1"` or `"1.00"`). Since all CSV fields
> are read as `Value::String`, the round-trip test compares string values, which is lossless.

---

### Task 4.3: Config Parsing
**Status:** ⛔ Blocked (waiting on Task 4.2)
**Blocked by:** Task 4.2 — CSV writer must be working for end-to-end tests

**Description:**
Implement YAML configuration parsing in `clinker-core` using `serde-saphyr`. Define the
`PipelineConfig` struct tree matching the spec's YAML schema (SS4). Support environment
variable interpolation via pre-deserialize regex replacement.

**Implementation notes:**
- Top-level struct: `PipelineConfig { pipeline: PipelineMeta, inputs: Vec<InputConfig>, outputs: Vec<OutputConfig>, transformations: Vec<TransformConfig>, error_handling: ErrorHandlingConfig }`.
- `PipelineMeta { name: String, memory_limit: Option<String>, vars: Option<IndexMap<String, serde_saphyr::Value>>, date_formats: Option<Vec<String>>, rules_path: Option<String>, concurrency: Option<ConcurrencyConfig> }`.
- `InputConfig { name: String, r#type: FormatKind, path: String, schema: Option<String>, schema_overrides: Option<Vec<SchemaOverride>>, options: Option<InputOptions> }`.
- `OutputConfig { name: String, r#type: FormatKind, path: String, include_unmapped: bool, include_header: Option<bool>, mapping: Option<IndexMap<String, String>>, exclude: Option<Vec<String>>, options: Option<OutputOptions> }`.
- `TransformConfig { name: String, description: Option<String>, cxl: String, local_window: Option<serde_saphyr::Value>, log: Option<serde_saphyr::Value>, validations: Option<serde_saphyr::Value> }`. The `cxl` field is the multi-line CXL source text. `local_window`, `log`, `validations` are structurally present (deserialized as raw YAML values) but processed in later phases.
- `ErrorHandlingConfig { strategy: ErrorStrategy, dlq: Option<DlqConfig>, type_error_threshold: Option<f64> }`. `ErrorStrategy` enum: `FailFast`, `Continue`, `BestEffort`. `DlqConfig { path: Option<String>, include_reason: Option<bool>, include_source_row: Option<bool> }`.
- **Environment variable interpolation:** Pre-deserialize — regex `\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}` replaces `${VAR}` with `env::var("VAR")`. `${VAR:-default}` uses default on missing var. Missing var without default → error before deserialization.
- All config structs use `#[serde(deny_unknown_fields)]` except those with deferred fields (`TransformConfig`).
- Uses `serde_saphyr::from_str()` for deserialization.

**Acceptance criteria:**
- [ ] `PipelineConfig` deserializes from YAML string
- [ ] `TransformConfig.cxl` contains multi-line CXL source text
- [ ] `${VAR}` interpolation replaces environment variables
- [ ] `${VAR:-default}` provides fallback for missing variables
- [ ] Missing required fields produce clear error messages
- [ ] Unknown keys produce error mentioning the offending key name
- [ ] All ErrorStrategy variants deserialize correctly

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

#### Sub-tasks
- [ ] **[4.3.1]** Define `PipelineConfig` struct tree with `serde(deny_unknown_fields)` + `TransformConfig { name, description, cxl }` matching spec SS4
- [ ] **[4.3.2]** Implement pre-deserialize `${VAR}` / `${VAR:-default}` interpolation via regex
- [ ] **[4.3.3]** Validation pass + all 8 gate tests

#### Code scaffolding
```rust
// Module: clinker-core/src/config.rs

use indexmap::IndexMap;
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineConfig {
    pub pipeline: PipelineMeta,
    pub inputs: Vec<InputConfig>,
    pub outputs: Vec<OutputConfig>,
    pub transformations: Vec<TransformConfig>,
    #[serde(default)]
    pub error_handling: ErrorHandlingConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PipelineMeta {
    pub name: String,
    pub memory_limit: Option<String>,
    pub vars: Option<IndexMap<String, serde_saphyr::Value>>,
    pub date_formats: Option<Vec<String>>,
    pub rules_path: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TransformConfig {
    pub name: String,
    pub description: Option<String>,
    pub cxl: String,
    // Structurally present, processed in later phases
    pub local_window: Option<serde_saphyr::Value>,
    pub log: Option<serde_saphyr::Value>,
    pub validations: Option<serde_saphyr::Value>,
}

#[derive(Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ErrorHandlingConfig {
    #[serde(default = "default_strategy")]
    pub strategy: ErrorStrategy,
    pub dlq: Option<DlqConfig>,
    pub type_error_threshold: Option<f64>,
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ErrorStrategy {
    FailFast,
    Continue,
    BestEffort,
}

/// Pre-deserialize environment variable interpolation.
pub fn interpolate_env_vars(yaml: &str) -> Result<String, ConfigError> { ... }

/// Load and parse a pipeline config from a YAML file path.
pub fn load_config(path: &std::path::Path) -> Result<PipelineConfig, ConfigError> { ... }
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `PipelineConfig` | `clinker-core/src/config.rs` | Top-level config |
| `TransformConfig` | `clinker-core/src/config.rs` | Block-level CXL transform |
| `ErrorHandlingConfig` | `clinker-core/src/config.rs` | Strategy + DLQ + threshold |
| `interpolate_env_vars` | `clinker-core/src/config.rs` | Pre-deserialize regex replacement |
| `load_config` | `clinker-core/src/config.rs` | File read + interpolation + deser |

#### Intra-phase dependencies
- Requires: Task 4.2 (end-to-end tests need CSV writer)
- Unblocks: Task 4.4 (executor consumes PipelineConfig)

#### Risk / gotcha
> **serde-saphyr API surface:** `serde-saphyr` is actively maintained but v0.0.x — the API
> may have subtle differences from `serde_yaml`. Test deserialization early. Specifically verify
> that `#[serde(deny_unknown_fields)]` works correctly and that multi-line `|` blocks deserialize
> as expected for the `cxl` field. If `serde-saphyr` has issues, the deviation policy applies —
> stop and surface to the user before switching crates.

---

### Task 4.4: Minimal Streaming Executor
**Status:** ⛔ Blocked (waiting on Task 4.3)
**Blocked by:** Task 4.3 — config parsing must work

**Description:**
Implement the single-pass streaming executor in `clinker-core`. This is the first data path:
CSV in → CXL per-record evaluation → field projection → CSV out. Stateless transforms only
(no windows, no arena, no Phase 1).

**Implementation notes:**
- **CXL compilation at startup:** For each `TransformConfig`, parse → resolve → type-check the `cxl` source text once. Store `Arc<TypedProgram>` per transform. Compilation errors halt before any data processing (exit code 1).
- `StreamingExecutor::run(config: &PipelineConfig) -> Result<PipelineCounters, PipelineError>`.
- Loop: `while let Some(record) = reader.next_record()?` → build `EvalContext` per record (with `source_file`, `source_row` from provenance) → evaluate each transform's `Arc<TypedProgram>` against the record → project output fields → `writer.write_record()`.
- `Record` directly implements `FieldResolver` (from Task 4.1 trait migration) — no wrapping needed.
- **Output projection order:** (1) Gather: start with CXL-emitted fields; if `include_unmapped: true`, add all input fields not already emitted. (2) Exclude: remove any field in `exclude` list (by current name). (3) Mapping: rename surviving fields per `mapping` table. Output field order: schema fields (mapped/unmapped), then CXL-emitted overflow fields.
- **Error handling strategies:**
  - `FailFast` → first error aborts (exit code 3). No DLQ write.
  - `Continue` → log error, increment `dlq_count`, write to DLQ, skip record. Abort if `dlq_count > max_errors` (if configured) or error rate exceeds `type_error_threshold`.
  - `BestEffort` → on transform error, emit original field value unchanged, write to DLQ. Never halt.
- `PipelineCounters` updated: `total_count` per input record, `ok_count` per successful output, `dlq_count` per error.

**Acceptance criteria:**
- [ ] End-to-end: CSV file → YAML config → transformed CSV output file
- [ ] CXL expressions compiled once at startup, evaluated per-record
- [ ] `include_unmapped` passes all input fields to output
- [ ] `mapping` renames fields in output
- [ ] `exclude` removes fields from output
- [ ] Projection order: gather → exclude → mapping
- [ ] `FailFast` stops on first error (exit 3)
- [ ] `Continue` skips bad records, writes to DLQ
- [ ] `BestEffort` uses original values on transform failure
- [ ] `PipelineCounters` reflect actual record counts

**Required unit tests (must pass before Task 4.5 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_executor_identity_passthrough` | No transforms, `include_unmapped: true` → output equals input | ⛔ Hard gate |
| `test_executor_cxl_string_concat` | Transform `emit full_name = first_name + " " + last_name` produces correct output | ⛔ Hard gate |
| `test_executor_cxl_arithmetic` | Transform `emit total = price * quantity` evaluates correctly | ⛔ Hard gate |
| `test_executor_cxl_conditional` | Transform with `if/then/else` selects correct branch | ⛔ Hard gate |
| `test_executor_field_mapping` | `mapping: {old: "new"}` renames field in output | ⛔ Hard gate |
| `test_executor_exclude_fields` | `exclude: ["secret"]` removes field from output | ⛔ Hard gate |
| `test_executor_projection_order` | Gather → exclude → mapping applied in correct sequence | ⛔ Hard gate |
| `test_executor_fail_fast_aborts` | Bad expression on row 2 of 10 → only 1 output row, exit error | ⛔ Hard gate |
| `test_executor_continue_skips` | Bad row 2 skipped, rows 1+3 in output, row 2 in DLQ | ⛔ Hard gate |
| `test_executor_best_effort_preserves` | Transform error → original field value kept in output | ⛔ Hard gate |
| `test_executor_pipeline_counters` | Counters match: total=10, ok=8, dlq=2 for known bad rows | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 4.5 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **[4.4.1]** CXL compilation at startup — parse/resolve/type-check each transform block, store `Arc<TypedProgram>`. Compilation errors halt with exit code 1.
- [ ] **[4.4.2]** Streaming executor loop: read → build EvalContext → evaluate → project → write. Output projection: gather → exclude → mapping.
- [ ] **[4.4.3]** Error handling strategies (FailFast/Continue/BestEffort) + pipeline counters + all 11 gate tests

#### Code scaffolding
```rust
// Module: clinker-core/src/executor.rs

use std::sync::Arc;
use clinker_record::PipelineCounters;
use crate::config::{PipelineConfig, ErrorStrategy};
use crate::error::PipelineError;

/// Compiled transform: CXL source compiled once, evaluated per record.
struct CompiledTransform {
    name: String,
    typed: Arc<cxl::typecheck::TypedProgram>,
}

/// Single-pass streaming executor. CSV in → CXL eval → CSV out.
pub struct StreamingExecutor;

impl StreamingExecutor {
    pub fn run(config: &PipelineConfig) -> Result<PipelineCounters, PipelineError> {
        // 1. Compile all CXL transforms
        let transforms = Self::compile_transforms(&config.transformations)?;

        // 2. Open reader + writer
        // 3. Streaming loop: read → eval → project → write
        // 4. Error handling per strategy
        // 5. Return counters
        todo!()
    }

    fn compile_transforms(
        transforms: &[crate::config::TransformConfig],
    ) -> Result<Vec<CompiledTransform>, PipelineError> {
        todo!()
    }
}
```

```rust
// Module: clinker-core/src/projection.rs

use clinker_record::{Record, Value};
use indexmap::IndexMap;
use crate::config::OutputConfig;

/// Apply output projection: gather → exclude → mapping.
pub fn project_output(
    input_record: &Record,
    emitted: &IndexMap<String, Value>,
    config: &OutputConfig,
) -> Record {
    todo!()
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `StreamingExecutor` | `clinker-core/src/executor.rs` | Main execution loop |
| `CompiledTransform` | `clinker-core/src/executor.rs` | CXL compiled once at startup |
| `project_output` | `clinker-core/src/projection.rs` | Gather → exclude → mapping |
| `PipelineError` | `clinker-core/src/error.rs` | Top-level pipeline error enum |

#### Intra-phase dependencies
- Requires: Task 4.3 (PipelineConfig), Task 4.1 (FormatReader, FieldResolver on Record), Task 4.2 (FormatWriter)
- Unblocks: Task 4.5 (CLI wires the executor)

#### Risk / gotcha
> **CXL compilation field resolution:** The resolver pass needs the list of available field
> names to resolve identifiers. At compilation time, the schema is known (from the CSV header
> or config), so field names are available. The executor must call `reader.schema()` before
> compiling CXL transforms, and pass the schema's column names to `resolve_program()`.

---

### Task 4.5: CLI Binary + DLQ Writer
**Status:** ⛔ Blocked (waiting on Task 4.4)
**Blocked by:** Task 4.4 — streaming executor must work

**Description:**
Wire up the `clinker` binary crate with `clap` argument parsing. Implement the DLQ (dead letter
queue) writer matching spec SS10.4 column layout. Define exit codes per spec SS10.2.

**Implementation notes:**
- `clap` derive API: `struct Cli { config: PathBuf, dry_run: bool, log_level: Option<String> }`. Positional `config` argument.
- `--dry-run`: parse config, compile CXL, validate, print execution summary, exit 0 without processing data.
- `--log-level`: sets `tracing` subscriber filter. Default: `info`.
- **DLQ writer (spec SS10.4):** CSV file at `error_handling.dlq.path`. Columns:
  - `_cxl_dlq_id` — UUID v7 (unique, time-ordered)
  - `_cxl_dlq_timestamp` — ISO 8601 when DLQ'd
  - `_cxl_dlq_source_file` — from RecordProvenance
  - `_cxl_dlq_source_row` — 1-based row number
  - `_cxl_dlq_error_category` — structured enum (`missing_required_field`, `type_coercion_failure`, etc.)
  - `_cxl_dlq_error_detail` — human-readable message
  - All original source fields in schema order
- **Exit codes (spec SS10.2):**
  - `0` — success, zero errors
  - `1` — config or CXL compile error (no data processed)
  - `2` — partial success, some rows routed to DLQ
  - `3` — fatal data error (fail_fast triggered, error rate threshold exceeded)
  - `4` — I/O or system error
- Add `uuid` crate to workspace with `v7` feature (pure Rust, spec SS2 crate table).

**Acceptance criteria:**
- [ ] `clinker pipeline.yaml` runs end-to-end
- [ ] `clinker --dry-run pipeline.yaml` validates without processing
- [ ] `--log-level debug` produces verbose output
- [ ] DLQ file matches spec SS10.4 column layout exactly
- [ ] DLQ rows contain all original source fields
- [ ] Exit code 0 on success
- [ ] Exit code 1 on config/compile error
- [ ] Exit code 2 on partial success with DLQ rows
- [ ] Exit code 3 on fail_fast or threshold exceeded

**Required unit tests (must pass before Phase 4 exit):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_cli_positional_config_path` | `clinker foo.yaml` parses config path correctly | ⛔ Hard gate |
| `test_cli_dry_run_flag` | `--dry-run` sets flag, no data processing occurs | ⛔ Hard gate |
| `test_cli_log_level_default` | No `--log-level` defaults to `info` | ⛔ Hard gate |
| `test_dlq_columns_match_spec` | DLQ output has all `_cxl_dlq_*` columns in correct order | ⛔ Hard gate |
| `test_dlq_preserves_original_fields` | DLQ row contains all original input field values | ⛔ Hard gate |
| `test_dlq_uuid_v7_format` | `_cxl_dlq_id` is a valid UUID v7 | ⛔ Hard gate |
| `test_exit_code_0_success` | Clean run exits 0 | ⛔ Hard gate |
| `test_exit_code_1_config_error` | Missing YAML file exits 1 | ⛔ Hard gate |
| `test_exit_code_2_partial_success` | Some DLQ rows → exit 2 | ⛔ Hard gate |
| `test_exit_code_3_fatal_data_error` | FailFast transform error → exit 3 | ⛔ Hard gate |
| `test_end_to_end_csv_transform` | Full integration: CSV in → YAML config → transformed CSV out → verify content | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 4 exit criteria not met until all tests above pass.

#### Sub-tasks
- [ ] **[4.5.1]** `clinker` CLI binary with clap: positional config path, `--dry-run`, `--log-level`
- [ ] **[4.5.2]** DLQ writer: spec SS10.4 column layout with `_cxl_dlq_*` prefix, UUID v7, error categories. Add `uuid` crate to workspace.
- [ ] **[4.5.3]** Exit codes 0-4 per spec SS10.2 + all 11 gate tests

#### Code scaffolding
```rust
// Module: clinker/src/main.rs

use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "clinker", about = "CXL streaming ETL engine")]
struct Cli {
    /// Path to the pipeline YAML configuration file
    config: PathBuf,
    /// Validate config and CXL without processing data
    #[arg(long)]
    dry_run: bool,
    /// Log level: error, warn, info, debug, trace
    #[arg(long, default_value = "info")]
    log_level: String,
}
```

```rust
// Module: clinker-core/src/dlq.rs

use clinker_record::{Record, Schema};
use uuid::Uuid;

/// Structured DLQ error categories per spec SS10.4.
#[derive(Debug, Clone, Copy)]
pub enum DlqErrorCategory {
    MissingRequiredField,
    TypeCoercionFailure,
    RequiredFieldConversionFailure,
    NanInOutputField,
    AggregateTypeError,
    ValidationFailure,
}

/// DLQ writer matching spec SS10.4 column layout.
pub struct DlqWriter<W: std::io::Write> {
    inner: csv::Writer<W>,
    source_schema: Arc<Schema>,
    header_written: bool,
}

impl<W: std::io::Write> DlqWriter<W> {
    pub fn write_dlq_row(
        &mut self,
        record: &Record,
        source_file: &str,
        source_row: u64,
        category: DlqErrorCategory,
        detail: &str,
    ) -> Result<(), FormatError> {
        let id = Uuid::now_v7();
        let timestamp = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S").to_string();
        // Write: id, timestamp, source_file, source_row, category, detail, ...source fields
        todo!()
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Cli` | `clinker/src/main.rs` | clap derive struct |
| `DlqWriter` | `clinker-core/src/dlq.rs` | Spec SS10.4 column layout |
| `DlqErrorCategory` | `clinker-core/src/dlq.rs` | Structured error enum |

#### Intra-phase dependencies
- Requires: Task 4.4 (StreamingExecutor)
- Unblocks: Phase 5 (two-pass pipeline extends the executor)

#### Risk / gotcha
> **UUID v7 requires `uuid` crate with `v7` feature.** Verify the crate is pure Rust (it is —
> listed in spec SS2). Add to workspace `Cargo.toml` as `uuid = { version = "1", features = ["v7"] }`.
> UUID v7 is time-ordered — DLQ rows sort chronologically by ID, which is useful for debugging.

---

## Intra-Phase Dependency Graph

```
Task 4.1 (traits + CSV reader) ---> Task 4.2 (CSV writer) ---> Task 4.3 (config)
                                                                      |
                                                                      v
                                                                Task 4.4 (executor)
                                                                      |
                                                                      v
                                                                Task 4.5 (CLI + DLQ)
```

Critical path: 4.1 → 4.2 → 4.3 → 4.4 → 4.5 (strictly sequential)
Parallelizable: None within Phase 4 (linear dependency chain)

---

## Decisions Log (drill-phase — 2026-03-29)
| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| 1 | FormatReader::schema returns `Result<Arc<Schema>>` with `&mut self` (spec version) | CSV must read headers to know schema; `Arc<Schema>` sharing needed for Arena in Phase 5; `&mut self` honest about mutation | Task 4.1, all downstream phases |
| 2 | FormatWriter takes schema at construction, not per-call | Writer stores `Arc<Schema>` internally; matches spec SS11.4 | Task 4.2 |
| 3 | FormatError is a simple enum with `#[non_exhaustive]`, not struct+kind | Lower-level error (I/O, parsing) — simple enum is conventional. Row/file context added by pipeline layer. | Task 4.1, 4.2 |
| 4 | Boolean output normalized to lowercase `"true"`/`"false"`; input case-insensitive via CXL `.to_bool()` | Excel uses TRUE/FALSE, users may save CSV with any case. Normalize for deterministic output. | Task 4.2 |
| 5 | TransformConfig uses block-level CXL (`cxl: \|` multi-line) matching spec, not field-level expressions | Spec defines named transform blocks with full CXL programs. Plan's `{field, expression}` model was oversimplified. | Task 4.3, 4.4 |
| 6 | Environment variable interpolation is pre-deserialize regex replacement | Simple (10 lines), universal, `${VAR}` not valid CXL syntax. Same approach as Docker Compose, GitHub Actions. | Task 4.3 |
| 7 | Move FieldResolver + WindowContext traits from `cxl::resolve` to `clinker-record` (dependency inversion) | Bevy/Tower pattern — trait at bottom of graph. `Record` implements `FieldResolver` directly, no newtype. Eliminates orphan rule problem for all 10 phases. | Task 4.1, every downstream phase |
| 8 | CXL compiled once at startup, stored as `Arc<TypedProgram>`, evaluated per record | Spec SS6.1: "AST compiler runs once at startup." Compilation is expensive (regex, constraints), evaluation is cheap. | Task 4.4 |
| 9 | Output projection order: gather → exclude → mapping | Natural pipeline: see all fields, remove unwanted, rename the rest. Users think in input names for exclude, output names for mapping. | Task 4.4 |
| 10 | DLQ follows spec SS10.4 column layout with `_cxl_dlq_*` prefix, UUID v7, error categories | Plan's `_error_message, _error_field, _error_row` was oversimplified. Spec layout has structured categories, provenance, audit trail. | Task 4.5 |
| 11 | Exit codes follow spec SS10.2: 0, 1, 2, 3, 4 | Plan was missing exit code 3 (fatal data error). Needed for fail_fast and threshold-exceeded scenarios. | Task 4.5 |

## Assumptions Log (drill-phase — 2026-03-29)
| # | Assumption | Basis | Risk if wrong |
|---|-----------|-------|---------------|
| 1 | `serde-saphyr` supports `deny_unknown_fields` and multi-line `\|` blocks | Research confirmed v0.0.22 is serde-compliant, pure Rust | If API differs from serde_yaml in practice, deviation policy applies — stop and surface. |
| 2 | `uuid` crate v1 with `v7` feature is pure Rust | Spec crate table lists it; confirmed pure Rust | If it pulls C deps, use a simpler UUID generation (timestamp + random). |
| 3 | Moving traits from `cxl` to `clinker-record` won't break downstream phase plans | Phase 5+ reference `FieldResolver` by trait name, not crate path | If a phase plan hardcodes `cxl::resolve::FieldResolver`, update the plan. |
| 4 | CSV fields as `Value::String` is sufficient for Phase 4 | Type coercion delegated to CXL expressions | If users expect automatic type inference from CSV data, this is a feature gap for v2. |
