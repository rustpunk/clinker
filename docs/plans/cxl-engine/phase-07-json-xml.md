# Phase 7: JSON + XML Readers/Writers

**Status:** 🔲 Ready (drilled + validated)
**Validated:** 2026-03-30 — see `VALIDATION-phase-07.md`
**Depends on:** Phase 4 (CSV + Minimal End-to-End Pipeline — format traits exist)
**Entry criteria:** `cargo test -p clinker-format` passes all Phase 4 format trait tests; `FormatReader` and `FormatWriter` traits are defined and implemented for CSV
**Exit criteria:** JSON and XML readers/writers implement `FormatReader`/`FormatWriter` traits; auto-detection selects correct JSON mode; XML pull-parser handles nested elements and attributes; `cargo test -p clinker-format` passes 40+ new tests

---

## Tasks

### Task 7.0: Config Refactor — Tagged Enum for Format Options
**Status:** 🔲 Not Started
**Blocked by:** Phase 4 exit criteria

**Description:**
Refactor `InputConfig` and `OutputConfig` to use adjacently tagged serde enums
(`#[serde(tag = "type", content = "options")]`) instead of flat `InputOptions`/
`OutputOptions` structs. Each format gets its own options struct with
`deny_unknown_fields`. This ensures invalid combinations (e.g., `attribute_prefix`
on a CSV input) are rejected at parse time.

**Implementation notes:**
- Replace `InputOptions` flat struct with `InputFormat` enum: `Csv(CsvInputOptions)`, `Json(JsonInputOptions)`, `Xml(XmlInputOptions)`.
- Replace `OutputOptions` flat struct with `OutputFormat` enum: `Csv(CsvOutputOptions)`, `Json(JsonOutputOptions)`, `Xml(XmlOutputOptions)`.
- `InputConfig` changes: remove `r#type: FormatKind` and `options: Option<InputOptions>`, add `#[serde(flatten)] pub format: InputFormat`.
- Define `ArrayPathConfig { path: String, mode: ArrayMode, separator: Option<String> }` and `ArrayMode { Explode, Join }`.
- Define `JsonFormat { Array, Ndjson, Object }` for input auto-detect override.
- Define `JsonOutputFormat { Array, Ndjson }` for output mode.
- Fix all call sites: executor, plan compiler, config tests, integration tests.
- YAML shape is preserved — no user-facing config changes.

**Acceptance criteria:**
- [ ] Existing CSV YAML configs parse correctly
- [ ] JSON/XML-specific options rejected on CSV inputs at parse time
- [ ] `array_paths` accepts structured objects with `path`, `mode`, `separator`
- [ ] All existing tests pass after refactor

**Required unit tests (must pass before Task 7.1 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_config_csv_input_parses` | Existing CSV config YAML deserializes correctly | ⛔ Hard gate |
| `test_config_json_input_parses` | JSON config with `record_path`, `format`, `array_paths` parses | ⛔ Hard gate |
| `test_config_xml_input_parses` | XML config with `record_path`, `attribute_prefix`, `namespace_handling` parses | ⛔ Hard gate |
| `test_config_unknown_field_rejected` | CSV config with `attribute_prefix` fails to parse | ⛔ Hard gate |
| `test_config_array_path_struct` | `array_paths` with `{path, mode, separator}` objects parses | ⛔ Hard gate |
| `test_config_json_output_parses` | JSON output with `format: ndjson`, `pretty: true` parses | ⛔ Hard gate |
| `test_config_xml_output_parses` | XML output with `root_element`, `record_element` parses | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 7.1 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **7.0.1** Define `ArrayPathConfig`, `ArrayMode`, `JsonFormat`, `JsonOutputFormat` types in config.rs
- [ ] **7.0.2** Define per-format input option structs: `CsvInputOptions`, `JsonInputOptions`, `XmlInputOptions` — each with `#[serde(deny_unknown_fields)]`
- [ ] **7.0.3** Define `InputFormat` adjacently tagged enum: `#[serde(tag = "type", content = "options", rename_all = "snake_case")]`
- [ ] **7.0.4** Refactor `InputConfig`: remove `r#type` and `options`, add `#[serde(flatten)] pub format: InputFormat`. Handle the case where `options` is omitted (each variant needs a default).
- [ ] **7.0.5** Same for output: `CsvOutputOptions`, `JsonOutputOptions`, `XmlOutputOptions`, `OutputFormat` enum
- [ ] **7.0.6** Fix all call sites in executor.rs, plan/execution.rs, plan/index.rs, projection.rs, integration_tests.rs, and all test helpers. Remove or deprecate `FormatKind` enum.

> 🟡 **[V-1-1] Accepted:** `projection.rs` has 3 test call sites constructing
> `OutputConfig` not originally listed. Compiler catches all missing match arms.

#### Code scaffolding
```rust
// In crates/clinker-core/src/config.rs

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArrayPathConfig {
    pub path: String,
    #[serde(default)]
    pub mode: ArrayMode,
    pub separator: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArrayMode {
    #[default]
    Explode,
    Join,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonFormat {
    Array,
    Ndjson,
    Object,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JsonOutputFormat {
    Array,
    Ndjson,
}

// Per-format input options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CsvInputOptions {
    pub delimiter: Option<String>,
    pub quote_char: Option<String>,
    pub has_header: Option<bool>,
    pub encoding: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct JsonInputOptions {
    pub format: Option<JsonFormat>,
    pub record_path: Option<String>,
    pub array_paths: Option<Vec<ArrayPathConfig>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct XmlInputOptions {
    pub record_path: Option<String>,
    pub attribute_prefix: Option<String>,
    pub namespace_handling: Option<NamespaceHandling>,
    pub array_paths: Option<Vec<ArrayPathConfig>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NamespaceHandling {
    #[default]
    Strip,
    Qualify,
}

// Tagged enum for input format dispatch
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "options", rename_all = "snake_case")]
pub enum InputFormat {
    Csv(Option<CsvInputOptions>),
    Json(Option<JsonInputOptions>),
    Xml(Option<XmlInputOptions>),
}

// Refactored InputConfig
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputConfig {
    pub name: String,
    pub path: String,
    pub schema: Option<String>,
    pub schema_overrides: Option<Vec<SchemaOverride>>,
    pub array_paths: Option<Vec<ArrayPathConfig>>,
    pub sort_order: Option<Vec<String>>,
    #[serde(flatten)]
    pub format: InputFormat,
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| All config types | `crates/clinker-core/src/config.rs` | Refactored in place |

#### Intra-phase dependencies
- Requires: Phase 4 complete (existing config + traits)
- Unblocks: Tasks 7.1, 7.2, 7.3, 7.4

#### Risk / gotcha
> **`#[serde(flatten)]` + adjacently tagged enum interaction:** serde's `flatten`
> with tagged enums can have edge cases. If deserialization fails, test with the
> exact YAML shapes from existing configs before proceeding. Fallback: keep `type`
> and `options` as separate fields on `InputConfig` and manually dispatch.

---

### Task 7.1: JSON Reader
**Status:** ⛔ Blocked (waiting on Task 7.0)
**Blocked by:** Task 7.0 — config refactor must be complete

**Description:**
Implement a `JsonReader` that auto-detects JSON format (array vs NDJSON vs object with
`record_path`), performs schema inference, and handles nested structures including array
explosion/joining.

**Implementation notes:**
- Auto-detection: peek first non-whitespace byte from the input stream. `[` → JSON array mode (use `serde_json::StreamDeserializer` over the array contents). `{` → NDJSON mode (line-by-line `serde_json::from_str`). If `record_path` is configured, navigate to the specified path via JSON pointer before applying array/NDJSON logic.
- `record_path`: a dot-separated path (e.g., `"data.results"`) navigated via JSON pointer (`/data/results`). The value at that path must be an array — each element becomes a Record.
- `array_paths`: a list of dot-separated paths identifying nested arrays within each record. `explode` mode: each element in the nested array produces a separate Record (parent fields duplicated). `join` mode: array elements collapsed into a comma-delimited string.
- Schema inference from the first record: keys with integer values (no `.` in the JSON number) → `Integer`, decimal numbers → `Float`, strings → `String`, booleans → `Bool`, null → `Null`, nested objects → flatten recursively with `.` separator (e.g., `address.city`).
- Subsequent records may introduce new fields — these go to overflow.
- Implements `FormatReader` trait.

**Acceptance criteria:**
- [ ] Auto-detects JSON array vs NDJSON vs object-with-record-path
- [ ] `record_path` navigation reaches nested arrays
- [ ] `array_paths` explode mode produces correct record count
- [ ] `array_paths` join mode produces comma-delimited strings
- [ ] Schema inference maps JSON types to Value variants correctly
- [ ] Nested objects flattened with `.` separator
- [ ] Implements `FormatReader` trait

**Required unit tests (must pass before Task 7.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_json_autodetect_array` | Input `[{"a":1},{"a":2}]` detected as array mode, produces 2 records | ⛔ Hard gate |
| `test_json_autodetect_ndjson` | Input `{"a":1}\n{"a":2}\n` detected as NDJSON mode, produces 2 records | ⛔ Hard gate |
| `test_json_record_path_navigation` | Input `{"data":{"results":[{"x":1}]}}` with `record_path: "data.results"` → 1 record with field `x` | ⛔ Hard gate |
| `test_json_array_paths_explode` | Record with `orders: [{"id":1},{"id":2}]` and `array_paths: [{path: "orders", mode: "explode"}]` → 2 records | ⛔ Hard gate |
| `test_json_array_paths_join` | Record with `tags: ["a","b","c"]` and `array_paths: [{path: "tags", mode: "join"}]` → field value `"a,b,c"` | ⛔ Hard gate |
| `test_json_schema_inference_types` | Integer, float, string, bool, null in first record → correct Value variants | ⛔ Hard gate |
| `test_json_nested_object_flattening` | `{"a":{"b":{"c":1}}}` → field `a.b.c` with value `Integer(1)` | ⛔ Hard gate |
| `test_json_new_fields_to_overflow` | Second record has field not in first record → stored in overflow | ⛔ Hard gate |
| `test_json_empty_array` | Input `[]` → 0 records, no error | ⛔ Hard gate |
| `test_json_malformed_input` | Invalid JSON → FormatError propagated | ⛔ Hard gate |
| `test_json_array_paths_empty_array` | `"orders": []` with explode mode → 0 records from that parent | ⛔ Hard gate |

> 🟡 **[V-5-1] Resolved:** Added 3 missing edge-case tests: empty array, malformed
> input, empty array_paths.

> ⛔ **Hard gate:** Task 7.2 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **7.1.1** Implement `JsonReader` struct with three internal modes: Array (`serde_json::StreamDeserializer`), NDJSON (line-by-line `serde_json::from_str`), Object (`serde_json::Value::pointer`)
- [ ] **7.1.2** Auto-detect logic: peek first non-whitespace byte from input. `[` → array, `{` → ndjson, `record_path` set → object. If explicit `format` in config, use that instead.
- [ ] **7.1.3** `record_path` navigation: convert dot-path `"data.results"` to JSON pointer `"/data/results"`. Parse full `Value` tree, call `.pointer()`, extract array. Keys with `/` or `~` need RFC 6901 escaping. Add file-size guard: reject files > 100MB (configurable) before `Value` parsing to prevent OOM.

> 🟠 **[V-8-1] Accepted:** Add file-size check before `serde_json::Value` parsing
> in record_path mode (default 100MB limit). Add max flatten depth (64 levels) in
> the recursive flattening step to prevent stack overflow on malicious input.
- [ ] **7.1.4** Schema inference from first record: flatten nested objects with `.` separator, map JSON types to Value variants. Build `Arc<Schema>` from first record's keys.
- [ ] **7.1.5** `array_paths` explode/join: after flattening a record, check for configured array paths. Explode: yield N records (one per array element, parent fields duplicated). Join: collapse array to comma-separated string.
- [ ] **7.1.6** Implement `FormatReader` trait: `schema()` returns inferred schema, `next_record()` yields records with overflow for new fields.

#### Code scaffolding
```rust
// Module: crates/clinker-format/src/json/reader.rs

use std::io::Read;
use std::sync::Arc;
use clinker_record::{Record, Schema, Value};
use crate::error::FormatError;
use crate::traits::FormatReader;

/// JSON reader configuration (from parsed config).
pub struct JsonReaderConfig {
    pub format: Option<JsonFormat>,       // auto-detect if None
    pub record_path: Option<String>,
    pub array_paths: Vec<ArrayPathConfig>,
}

/// Auto-detected or configured JSON input mode.
enum JsonMode {
    /// Top-level JSON array: `[{...}, {...}, ...]`
    Array { records: Vec<serde_json::Value>, pos: usize },
    /// Newline-delimited JSON: one object per line
    Ndjson { lines: std::io::Lines<std::io::BufReader<Box<dyn Read + Send>>> },
    /// Object with record_path: navigate to nested array
    Object { records: Vec<serde_json::Value>, pos: usize },
}

pub struct JsonReader {
    mode: JsonMode,
    schema: Option<Arc<Schema>>,
    config: JsonReaderConfig,
}

impl FormatReader for JsonReader {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> { todo!() }
    fn next_record(&mut self) -> Result<Option<Record>, FormatError> { todo!() }
}

/// Convert dot-path to RFC 6901 JSON Pointer.
fn dot_path_to_pointer(path: &str) -> String {
    format!("/{}", path.replace('.', "/"))
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `JsonReader` | `crates/clinker-format/src/json/reader.rs` | New module |
| `JsonReaderConfig` | `crates/clinker-format/src/json/reader.rs` | Config bridge |
| `dot_path_to_pointer` | `crates/clinker-format/src/json/reader.rs` | Helper |

#### Intra-phase dependencies
- Requires: Task 7.0 (config types for JsonInputOptions)
- Unblocks: Task 7.2

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_autodetect_array() {
        let input = r#"[{"a":1},{"a":2}]"#;
        let mut reader = JsonReader::from_reader(
            std::io::Cursor::new(input),
            JsonReaderConfig { format: None, record_path: None, array_paths: vec![] },
        ).unwrap();
        let schema = reader.schema().unwrap();
        assert_eq!(schema.columns().len(), 1);
        let r1 = reader.next_record().unwrap().unwrap();
        let r2 = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(r1.get("a"), Some(&Value::Integer(1)));
        assert_eq!(r2.get("a"), Some(&Value::Integer(2)));
    }

    #[test]
    fn test_json_autodetect_ndjson() {
        let input = "{\"a\":1}\n{\"a\":2}\n";
        // ... similar pattern
    }
}
```

#### Risk / gotcha
> **`record_path` memory cost:** `serde_json::Value` tree for a 100MB JSON file
> consumes ~400-600MB RAM (3-11x overhead). Documented as known limitation.
> For NDJSON and top-level arrays, memory is bounded by one record at a time.

---

### Task 7.2: JSON Writer
**Status:** ⛔ Blocked (waiting on Task 7.1)
**Blocked by:** Task 7.1 — JSON reader must be working

**Description:**
Implement a `JsonWriter` supporting array mode, NDJSON mode, pretty-printing, and null
omission. Implements the `FormatWriter` trait.

**Implementation notes:**
- Array mode: write `[` as header, each record as a JSON object separated by commas, `]` as footer. No trailing comma before `]`.
- NDJSON mode: one JSON object per line, no array wrapper, no trailing newline after last record.
- `pretty: true` uses `serde_json::to_string_pretty` instead of `serde_json::to_string`.
- `preserve_nulls: false` (default) omits keys with `Value::Null` from the JSON output. `preserve_nulls: true` emits `"key": null`.
- Overflow fields are merged into the output object alongside schema fields.
- Field ordering: schema fields first (in schema order), then overflow fields in emit order (IndexMap insertion order).
- Implements `FormatWriter` trait.

**Acceptance criteria:**
- [ ] Array mode produces valid JSON array
- [ ] NDJSON mode produces one object per line
- [ ] Pretty mode produces indented output
- [ ] `preserve_nulls: false` omits null keys
- [ ] `preserve_nulls: true` emits null keys
- [ ] Overflow fields included in output
- [ ] Implements `FormatWriter` trait

**Required unit tests (must pass before Task 7.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_json_write_array_mode` | 3 records → valid JSON array with 3 objects, parseable by `serde_json::from_str` | ⛔ Hard gate |
| `test_json_write_ndjson_mode` | 3 records → 3 lines, each independently parseable as JSON | ⛔ Hard gate |
| `test_json_write_pretty` | Pretty mode output contains newlines and indentation within each object | ⛔ Hard gate |
| `test_json_write_omit_nulls` | `preserve_nulls: false` → null fields absent from JSON output | ⛔ Hard gate |
| `test_json_write_preserve_nulls` | `preserve_nulls: true` → null fields present as `null` in JSON output | ⛔ Hard gate |
| `test_json_write_overflow_fields` | Record with overflow fields → overflow fields appear in output object | ⛔ Hard gate |
| `test_json_write_field_ordering` | Schema fields appear first in schema order, overflow fields in emit order | ⛔ Hard gate |
| `test_json_roundtrip_reader_writer` | Write records via JsonWriter, read back via JsonReader, assert field-level equality | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 7.3 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **7.2.1** Implement `JsonWriter` struct with array/ndjson mode, `preserve_nulls` flag, `pretty` flag
- [ ] **7.2.2** Record → JSON object serialization: schema fields in order, overflow fields appended, null handling
- [ ] **7.2.3** Array mode: `[` header, comma-separated objects, `]` footer (no trailing comma)
- [ ] **7.2.4** NDJSON mode: one object per line, no wrapper
- [ ] **7.2.5** Implement `FormatWriter` trait: `write_record()` and `flush()`

#### Code scaffolding
```rust
// Module: crates/clinker-format/src/json/writer.rs

use std::io::Write;
use std::sync::Arc;
use clinker_record::{Record, Schema, Value};
use crate::error::FormatError;
use crate::traits::FormatWriter;

pub struct JsonWriterConfig {
    pub format: JsonOutputFormat,
    pub pretty: bool,
    pub preserve_nulls: bool,
}

pub struct JsonWriter<W: Write> {
    writer: W,
    schema: Arc<Schema>,
    config: JsonWriterConfig,
    records_written: u64,
}

impl<W: Write + Send> FormatWriter for JsonWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> { todo!() }
    fn flush(&mut self) -> Result<(), FormatError> { todo!() }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `JsonWriter` | `crates/clinker-format/src/json/writer.rs` | New module |
| `JsonWriterConfig` | `crates/clinker-format/src/json/writer.rs` | Config bridge |

#### Intra-phase dependencies
- Requires: Task 7.1 (JSON reader for roundtrip test)
- Unblocks: Task 7.3

#### Risk / gotcha
> **Array mode trailing comma:** Must track whether this is the first record to
> avoid emitting a leading comma. Use `records_written` counter: if > 0, emit
> `,\n` before the object.

---

### Task 7.3: XML Reader
**Status:** ⛔ Blocked (waiting on Task 7.2)
**Blocked by:** Task 7.2 — JSON writer must be working (ensures format trait pattern is validated end-to-end before adding XML)

**Description:**
Implement an `XmlReader` using `quick-xml` pull-parser that navigates to a `record_path`,
extracts attributes, flattens nested elements, and handles namespaces.

**Implementation notes:**
- `quick_xml::Reader` in buffered mode for pull-parsing. No DOM tree — streaming element-by-element.
- `record_path`: slash-separated element path (e.g., `"Orders/Order"`). The reader descends through nested elements matching each segment. Each matching terminal element becomes one Record.
- Attribute extraction: element attributes become Record fields. Configurable `attribute_prefix` (default `"@"`), so `<Order id="5">` produces field `@id` with value `"5"`.
- `namespace_handling`: `strip` (default) uses `QName::local_name()` to remove prefixes. `qualify` uses `QName::as_ref()` to keep them. No `NsReader` needed.
- Nested child elements become fields. Text content of `<Name>Alice</Name>` → field `Name` with value `"Alice"`.
- Multi-level nesting flattened with `.` separator: `<Address><City>NYC</City></Address>` → field `Address.City`.
- `array_paths`: list of element names that may repeat within a record. `explode` mode: each repetition produces a separate Record. `join` mode: text values joined with comma separator.
- Schema inference from first record, same type rules as JSON reader.
- Implements `FormatReader` trait.

**Acceptance criteria:**
- [ ] `record_path` navigation matches nested XML elements
- [ ] Attributes extracted with configurable prefix
- [ ] Namespace stripping and qualification both work
- [ ] Nested elements flattened with `.` separator
- [ ] `array_paths` explode and join modes work
- [ ] Schema inferred from first record
- [ ] Implements `FormatReader` trait

**Required unit tests (must pass before Task 7.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_xml_record_path_navigation` | `<Root><Orders><Order>...</Order></Orders></Root>` with `record_path: "Orders/Order"` → records from Order elements | ⛔ Hard gate |
| `test_xml_attribute_extraction_default_prefix` | `<Order id="5" status="open"/>` → fields `@id = "5"`, `@status = "open"` | ⛔ Hard gate |
| `test_xml_attribute_custom_prefix` | `attribute_prefix: "_"` → fields `_id`, `_status` | ⛔ Hard gate |
| `test_xml_namespace_strip` | `<ns:Order>` with `namespace_handling: strip` → element name `Order` | ⛔ Hard gate |
| `test_xml_namespace_qualify` | `<ns:Order>` with `namespace_handling: qualify` → element name `ns:Order` | ⛔ Hard gate |
| `test_xml_nested_element_flattening` | `<Address><City>NYC</City></Address>` → field `Address.City` = `"NYC"` | ⛔ Hard gate |
| `test_xml_array_paths_explode` | Repeating `<Item>` elements with explode mode → one record per Item | ⛔ Hard gate |
| `test_xml_array_paths_join` | Repeating `<Tag>` elements with join mode → comma-separated string | ⛔ Hard gate |
| `test_xml_cdata_handling` | `<![CDATA[some & content]]>` treated as text content | ⛔ Hard gate |
| `test_xml_empty_no_records` | XML with no matching record elements → 0 records | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 7.4 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **7.3.1** Implement `XmlReader` struct using `quick_xml::Reader::from_reader` with `BufReader`
- [ ] **7.3.2** `record_path` navigation: split on `/`, maintain depth counter, match element names at each level. Skip non-matching subtrees by counting `Start`/`End` events.
- [ ] **7.3.3** Record extraction: within a matching record element, collect attributes (with prefix) and child elements (text content). Track element path for nested flattening.
- [ ] **7.3.4** Namespace handling: `QName::local_name().as_ref()` for strip mode, `QName::as_ref()` for qualify mode. Convert bytes to `&str` via `std::str::from_utf8`.
- [ ] **7.3.5** `array_paths` explode/join: track repeating child elements. Explode: buffer parent fields, yield one Record per repetition. Join: accumulate text values, emit as comma-separated string.
- [ ] **7.3.6** Schema inference from first record, `FormatReader` trait implementation.

#### Code scaffolding
```rust
// Module: crates/clinker-format/src/xml/reader.rs

use std::io::BufRead;
use std::sync::Arc;
use quick_xml::events::Event;
use quick_xml::Reader as XmlParser;
use clinker_record::{Record, Schema, Value};
use crate::error::FormatError;
use crate::traits::FormatReader;

pub struct XmlReaderConfig {
    pub record_path: Option<String>,      // e.g., "Orders/Order"
    pub attribute_prefix: String,          // default "@"
    pub namespace_handling: NamespaceHandling,
    pub array_paths: Vec<ArrayPathConfig>,
}

pub struct XmlReader<R: BufRead> {
    parser: XmlParser<R>,
    config: XmlReaderConfig,
    schema: Option<Arc<Schema>>,
    buf: Vec<u8>,
    // State tracking for record_path navigation
    path_segments: Vec<String>,
    current_depth: usize,
    matched_depth: usize,
}

impl<R: BufRead + Send> FormatReader for XmlReader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> { todo!() }
    fn next_record(&mut self) -> Result<Option<Record>, FormatError> { todo!() }
}

/// Extract element name with namespace handling.
fn element_name(qname: &quick_xml::name::QName, ns: &NamespaceHandling) -> String {
    let bytes = match ns {
        NamespaceHandling::Strip => qname.local_name().as_ref(),
        NamespaceHandling::Qualify => qname.as_ref(),
    };
    String::from_utf8_lossy(bytes).into_owned()
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `XmlReader` | `crates/clinker-format/src/xml/reader.rs` | New module |
| `XmlReaderConfig` | `crates/clinker-format/src/xml/reader.rs` | Config bridge |

#### Intra-phase dependencies
- Requires: Task 7.2 (JSON writer validates FormatWriter pattern)
- Unblocks: Task 7.4

#### Risk / gotcha
> **quick-xml event-based API requires manual state tracking.** Unlike JSON where
> serde handles nesting, XML parsing requires counting `Start`/`End` events to
> track depth, buffering text across `Text` events, and handling self-closing
> `Empty` elements differently from `Start`+`End` pairs. This is the most code-
> intensive task in Phase 7.

---

### Task 7.4: XML Writer
**Status:** ⛔ Blocked (waiting on Task 7.3)
**Blocked by:** Task 7.3 — XML reader must be working

**Description:**
Implement an `XmlWriter` with configurable root/record elements, proper XML escaping,
and null handling. Implements the `FormatWriter` trait.

**Implementation notes:**
- Configurable `root_element` (default `"Root"`) and `record_element` (default `"Record"`). Output structure: `<Root><Record>...</Record><Record>...</Record></Root>`.
- Field values written as child elements: `<FieldName>value</FieldName>`.
- Fields containing `.` in their name are expanded back into nested elements: field `Address.City` → `<Address><City>NYC</City></Address>`. Shared prefixes are grouped under a single parent element.
- `preserve_nulls: true` → `<FieldName/>` (self-closing empty element). `preserve_nulls: false` → tag omitted entirely.
- XML escaping of text content: `&` → `&amp;`, `<` → `&lt;`, `>` → `&gt;`, `"` → `&quot;`, `'` → `&apos;`.
- Overflow fields included after schema fields in emit order (IndexMap insertion order).
- Output uses `quick_xml::Writer` for correct escaping and well-formed XML.
- Implements `FormatWriter` trait.

**Acceptance criteria:**
- [ ] Root and record elements configurable
- [ ] Field values written as child elements
- [ ] Dotted field names expanded to nested elements
- [ ] `preserve_nulls: true` emits self-closing tags
- [ ] `preserve_nulls: false` omits null fields
- [ ] XML special characters properly escaped
- [ ] Implements `FormatWriter` trait

**Required unit tests (must pass to close Phase 7):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_xml_write_basic_structure` | 2 records → `<Root><Record>...</Record><Record>...</Record></Root>`, valid XML | ⛔ Hard gate |
| `test_xml_write_custom_elements` | `root_element: "Data"`, `record_element: "Row"` → `<Data><Row>...</Row></Data>` | ⛔ Hard gate |
| `test_xml_write_nested_expansion` | Field `Address.City` → `<Address><City>value</City></Address>` | ⛔ Hard gate |
| `test_xml_write_shared_prefix_grouping` | Fields `Address.City` and `Address.State` → single `<Address>` parent with two children | ⛔ Hard gate |
| `test_xml_write_preserve_nulls_true` | Null field → `<FieldName/>` self-closing element | ⛔ Hard gate |
| `test_xml_write_preserve_nulls_false` | Null field → element omitted entirely | ⛔ Hard gate |
| `test_xml_write_escaping` | Values containing `&`, `<`, `>`, `"`, `'` properly escaped in output | ⛔ Hard gate |
| `test_xml_roundtrip_reader_writer` | Write records via XmlWriter, read back via XmlReader, assert field-level equality | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 7 exit criteria require all tests above to pass.

#### Sub-tasks
- [ ] **7.4.1** Implement `XmlWriter` struct using `quick_xml::Writer` wrapping a `BufWriter`
- [ ] **7.4.2** Root element open/close: write `<Root>` on first `write_record`, `</Root>` on `flush()`
- [ ] **7.4.3** Record serialization: for each field, emit `<FieldName>value</FieldName>`. Null handling based on `preserve_nulls`.
- [ ] **7.4.4** Dotted field → nested element expansion: sort fields, group by first `.` segment, recurse. Algorithm: build a tree of field segments, walk the tree emitting `Start`/`Text`/`End` events.
- [ ] **7.4.5** Implement `FormatWriter` trait: `write_record()` and `flush()` (flush writes closing root tag)

#### Code scaffolding
```rust
// Module: crates/clinker-format/src/xml/writer.rs

use std::io::Write;
use std::sync::Arc;
use quick_xml::Writer as XmlEmitter;
use quick_xml::events::{Event, BytesStart, BytesEnd, BytesText};
use clinker_record::{Record, Schema, Value};
use crate::error::FormatError;
use crate::traits::FormatWriter;

pub struct XmlWriterConfig {
    pub root_element: String,
    pub record_element: String,
    pub preserve_nulls: bool,
}

pub struct XmlWriter<W: Write> {
    writer: XmlEmitter<W>,
    schema: Arc<Schema>,
    config: XmlWriterConfig,
    header_written: bool,
}

impl<W: Write + Send> FormatWriter for XmlWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> { todo!() }
    fn flush(&mut self) -> Result<(), FormatError> { todo!() }
}

/// Expand dotted field names into a nested element tree.
/// Groups fields by shared prefix segments.
fn expand_dotted_fields(fields: &[(&str, &Value)]) -> Vec<FieldNode> {
    todo!()
}

enum FieldNode {
    Leaf { name: String, value: Value },
    Branch { name: String, children: Vec<FieldNode> },
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `XmlWriter` | `crates/clinker-format/src/xml/writer.rs` | New module |
| `XmlWriterConfig` | `crates/clinker-format/src/xml/writer.rs` | Config bridge |
| `expand_dotted_fields` | `crates/clinker-format/src/xml/writer.rs` | Nested expansion helper |

#### Intra-phase dependencies
- Requires: Task 7.3 (XML reader for roundtrip test)
- Unblocks: Phase 7 exit

#### Risk / gotcha
> **Shared prefix grouping for nested expansion:** Fields `Address.City` and
> `Address.State` must share a single `<Address>` parent, not produce two separate
> `<Address>` elements. The algorithm must collect all fields, sort by name, group
> by first segment, and recurse for deeper nesting (`A.B.C` and `A.B.D` share
> both `<A>` and `<B>` parents).

---

## Intra-Phase Dependency Graph

```
Task 7.0 (Config Refactor)
    │
    └──> Task 7.1 (JSON Reader)
              │
              └──> Task 7.2 (JSON Writer)
                        │
                        └──> Task 7.3 (XML Reader)
                                  │
                                  └──> Task 7.4 (XML Writer)
```

Critical path: 7.0 → 7.1 → 7.2 → 7.3 → 7.4
Parallelizable: None — strict linear chain (each validates the pattern for the next).

---

## Decisions Log (drill-phase — 2026-03-30)
| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| 1 | Adjacently tagged serde enum for format config (`InputFormat`, `OutputFormat`) | Research: Vector.dev uses per-format structs, DataFusion uses separate structs, serde `deny_unknown_fields` broken with internally tagged enums (#2123). Adjacently tagged preserves YAML shape, each struct gets working `deny_unknown_fields`. | Task 7.0, all downstream |
| 2 | `serde_json::Value` + `pointer()` for `record_path` navigation (not streaming) | Research: Value tree costs 3-11x file size (400-600MB for 100MB), but spec targets ≤100MB. No mature Rust crate for streaming path navigation. jq, Vector.dev, Polars all buffer the full tree. | Task 7.1 |
| 3 | Plain `quick_xml::Reader` with `QName::local_name()` for namespace stripping (no `NsReader`) | `NsReader` resolves URIs (overkill). `local_name()` strips prefix at byte level. Config flag selects `local_name()` vs `as_ref()`. | Task 7.3 |
| 4 | `record_path` uses dot-separated user syntax, converted to JSON pointer internally | Spec uses dots in JSON examples. Internal conversion: `"data.results"` → `"/data/results"`. RFC 6901 escaping for keys with `/` or `~`. | Task 7.1 |

## Assumptions Log (drill-phase — 2026-03-30)
| # | Assumption | Basis | Risk if wrong |
|---|-----------|-------|---------------|
| 1 | `#[serde(flatten)]` with adjacently tagged enum works correctly with serde-saphyr | Tested with serde_json; saphyr uses serde Deserialize trait so should work | If not, fall back to separate `type` and `options` fields with manual dispatch |
| 2 | JSON reader uses streaming DeserializeSeed for all modes — O(1 record) memory | Implemented: IgnoredAny skips non-matching keys, visit_seq streams elements. ~10KB buffer + 1 record. | N/A — streaming is the default |
| 3 | `quick-xml` 0.37 `QName::local_name()` API is stable | Spec pins 0.37, PLAN.md notes risk is low | Pin version, wrap in adapter if API breaks |
