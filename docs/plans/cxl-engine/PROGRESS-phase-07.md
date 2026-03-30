# Phase 7 Execution Progress: JSON + XML Readers/Writers

**Phase file:** docs/plans/cxl-engine/phase-07-json-xml.md
**Started:** 2026-03-30
**Last updated:** 2026-03-30
**Status:** 🔄 In Progress

---

## Current state

**Active task:** [7.0] Config Refactor — Tagged Enum for Format Options
**Completed:** 0 of 5 tasks
**Blocked:** none (Phase 4 entry criteria met)

---

## Task list

### 🔄 [7.0] Config Refactor — Tagged Enum for Format Options  ← ACTIVE
**Sub-tasks:**
- [ ] **7.0.1** Define `ArrayPathConfig`, `ArrayMode`, `JsonFormat`, `JsonOutputFormat` types
- [ ] **7.0.2** Define per-format input option structs: `CsvInputOptions`, `JsonInputOptions`, `XmlInputOptions`
- [ ] **7.0.3** Define `InputFormat` adjacently tagged enum
- [ ] **7.0.4** Refactor `InputConfig`: remove `r#type` and `options`, add `#[serde(flatten)] pub format: InputFormat`
- [ ] **7.0.5** Same for output: `CsvOutputOptions`, `JsonOutputOptions`, `XmlOutputOptions`, `OutputFormat` enum
- [ ] **7.0.6** Fix all call sites in executor.rs, plan/execution.rs, plan/index.rs, projection.rs, integration_tests.rs, and all test helpers. Remove `FormatKind` enum.

**Gate tests that must pass before Task 7.1 unlocks:**
- `test_config_csv_input_parses` -- existing CSV YAML deserializes correctly
- `test_config_json_input_parses` -- JSON config with record_path, format, array_paths parses
- `test_config_xml_input_parses` -- XML config with record_path, attribute_prefix, namespace_handling parses
- `test_config_unknown_field_rejected` -- CSV config with attribute_prefix fails
- `test_config_array_path_struct` -- array_paths with {path, mode, separator} parses
- `test_config_json_output_parses` -- JSON output with format, pretty parses
- `test_config_xml_output_parses` -- XML output with root_element, record_element parses

**Done when:** All existing tests pass with new config types; CSV/JSON/XML format options parse correctly; invalid cross-format options rejected at parse time
**Commit:** `feat(phase-7): refactor config to adjacently tagged format enums`
**Commit ID:** --

---

### ⛔ [7.1] JSON Reader  ← BLOCKED on [7.0] gate tests
**Sub-tasks:**
- [ ] **7.1.1** Implement `JsonReader` with Array, NDJSON, Object modes
- [ ] **7.1.2** Auto-detect logic (peek first byte)
- [ ] **7.1.3** `record_path` navigation (dot-to-pointer, Value::pointer)
- [ ] **7.1.4** Schema inference from first record
- [ ] **7.1.5** `array_paths` explode/join
- [ ] **7.1.6** Implement `FormatReader` trait

**Gate tests that must pass before Task 7.2 unlocks:**
- `test_json_autodetect_array`
- `test_json_autodetect_ndjson`
- `test_json_record_path_navigation`
- `test_json_array_paths_explode`
- `test_json_array_paths_join`
- `test_json_schema_inference_types`
- `test_json_nested_object_flattening`
- `test_json_new_fields_to_overflow`
- `test_json_empty_array`
- `test_json_malformed_input`
- `test_json_array_paths_empty_array`

**Done when:** JsonReader implements FormatReader, auto-detects format, navigates record_path, handles array_paths explode/join, infers schema
**Commit:** `feat(phase-7): implement JSON reader with auto-detect and schema inference`
**Commit ID:** --

---

### ⛔ [7.2] JSON Writer  ← BLOCKED on [7.1] gate tests
**Sub-tasks:**
- [ ] **7.2.1** Implement `JsonWriter` with array/ndjson mode
- [ ] **7.2.2** Record → JSON object serialization
- [ ] **7.2.3** Array mode ([ header, comma-separated, ] footer)
- [ ] **7.2.4** NDJSON mode (one object per line)
- [ ] **7.2.5** Implement `FormatWriter` trait

**Gate tests that must pass before Task 7.3 unlocks:**
- `test_json_write_array_mode`
- `test_json_write_ndjson_mode`
- `test_json_write_pretty`
- `test_json_write_omit_nulls`
- `test_json_write_preserve_nulls`
- `test_json_write_overflow_fields`
- `test_json_write_field_ordering`
- `test_json_roundtrip_reader_writer`

**Done when:** JsonWriter implements FormatWriter, supports array/ndjson/pretty modes, handles preserve_nulls and overflow fields
**Commit:** `feat(phase-7): implement JSON writer with array and NDJSON modes`
**Commit ID:** --

---

### ⛔ [7.3] XML Reader  ← BLOCKED on [7.2] gate tests
**Sub-tasks:**
- [ ] **7.3.1** Implement `XmlReader` using `quick_xml::Reader`
- [ ] **7.3.2** `record_path` navigation (split on /, depth tracking)
- [ ] **7.3.3** Record extraction (attributes + child elements)
- [ ] **7.3.4** Namespace handling (local_name vs as_ref)
- [ ] **7.3.5** `array_paths` explode/join
- [ ] **7.3.6** Schema inference, FormatReader trait

**Gate tests that must pass before Task 7.4 unlocks:**
- `test_xml_record_path_navigation`
- `test_xml_attribute_extraction_default_prefix`
- `test_xml_attribute_custom_prefix`
- `test_xml_namespace_strip`
- `test_xml_namespace_qualify`
- `test_xml_nested_element_flattening`
- `test_xml_array_paths_explode`
- `test_xml_array_paths_join`
- `test_xml_cdata_handling`
- `test_xml_empty_no_records`

**Done when:** XmlReader implements FormatReader, navigates record_path, extracts attributes with prefix, handles namespaces, flattens nested elements
**Commit:** `feat(phase-7): implement XML reader with pull-parser and namespace handling`
**Commit ID:** --

---

### ⛔ [7.4] XML Writer  ← BLOCKED on [7.3] gate tests
**Sub-tasks:**
- [ ] **7.4.1** Implement `XmlWriter` using `quick_xml::Writer`
- [ ] **7.4.2** Root element open/close
- [ ] **7.4.3** Record serialization with null handling
- [ ] **7.4.4** Dotted field → nested element expansion with shared prefix grouping
- [ ] **7.4.5** Implement `FormatWriter` trait

**Gate tests that must pass to close Phase 7:**
- `test_xml_write_basic_structure`
- `test_xml_write_custom_elements`
- `test_xml_write_nested_expansion`
- `test_xml_write_shared_prefix_grouping`
- `test_xml_write_preserve_nulls_true`
- `test_xml_write_preserve_nulls_false`
- `test_xml_write_escaping`
- `test_xml_roundtrip_reader_writer`

**Done when:** XmlWriter implements FormatWriter, configurable elements, dotted field expansion, proper escaping
**Commit:** `feat(phase-7): implement XML writer with nested element expansion`
**Commit ID:** --

---

## Gate test log

| Task | Test | Status | Run | Commit |
|------|------|--------|-----|--------|
| 7.0 | `test_config_csv_input_parses` | ⛔ Not run | -- | -- |
| 7.0 | `test_config_json_input_parses` | ⛔ Not run | -- | -- |
| 7.0 | `test_config_xml_input_parses` | ⛔ Not run | -- | -- |
| 7.0 | `test_config_unknown_field_rejected` | ⛔ Not run | -- | -- |
| 7.0 | `test_config_array_path_struct` | ⛔ Not run | -- | -- |
| 7.0 | `test_config_json_output_parses` | ⛔ Not run | -- | -- |
| 7.0 | `test_config_xml_output_parses` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_autodetect_array` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_autodetect_ndjson` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_record_path_navigation` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_array_paths_explode` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_array_paths_join` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_schema_inference_types` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_nested_object_flattening` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_new_fields_to_overflow` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_empty_array` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_malformed_input` | ⛔ Not run | -- | -- |
| 7.1 | `test_json_array_paths_empty_array` | ⛔ Not run | -- | -- |
| 7.2 | `test_json_write_array_mode` | ⛔ Not run | -- | -- |
| 7.2 | `test_json_write_ndjson_mode` | ⛔ Not run | -- | -- |
| 7.2 | `test_json_write_pretty` | ⛔ Not run | -- | -- |
| 7.2 | `test_json_write_omit_nulls` | ⛔ Not run | -- | -- |
| 7.2 | `test_json_write_preserve_nulls` | ⛔ Not run | -- | -- |
| 7.2 | `test_json_write_overflow_fields` | ⛔ Not run | -- | -- |
| 7.2 | `test_json_write_field_ordering` | ⛔ Not run | -- | -- |
| 7.2 | `test_json_roundtrip_reader_writer` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_record_path_navigation` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_attribute_extraction_default_prefix` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_attribute_custom_prefix` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_namespace_strip` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_namespace_qualify` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_nested_element_flattening` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_array_paths_explode` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_array_paths_join` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_cdata_handling` | ⛔ Not run | -- | -- |
| 7.3 | `test_xml_empty_no_records` | ⛔ Not run | -- | -- |
| 7.4 | `test_xml_write_basic_structure` | ⛔ Not run | -- | -- |
| 7.4 | `test_xml_write_custom_elements` | ⛔ Not run | -- | -- |
| 7.4 | `test_xml_write_nested_expansion` | ⛔ Not run | -- | -- |
| 7.4 | `test_xml_write_shared_prefix_grouping` | ⛔ Not run | -- | -- |
| 7.4 | `test_xml_write_preserve_nulls_true` | ⛔ Not run | -- | -- |
| 7.4 | `test_xml_write_preserve_nulls_false` | ⛔ Not run | -- | -- |
| 7.4 | `test_xml_write_escaping` | ⛔ Not run | -- | -- |
| 7.4 | `test_xml_roundtrip_reader_writer` | ⛔ Not run | -- | -- |

---

## Completed tasks

| Task | Name | Commit message | Commit ID | Completed |
|------|------|---------------|-----------|-----------|
| (none yet) | | | | |

---

## Notes

- Phase 7 was drilled and validated on 2026-03-30. See VALIDATION-phase-07.md for full report.
- Key decision: adjacently tagged serde enum for config (empirically validated with serde-saphyr).
- serde_json::Value + pointer() for record_path (buffers full tree, 3-11x overhead for ≤100MB files).
- plain quick_xml::Reader with QName::local_name() for namespace stripping.
- FormatKind enum to be removed in Task 7.0.6.
