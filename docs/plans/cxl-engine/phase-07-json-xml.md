# Phase 7: JSON + XML Readers/Writers

**Status:** 🔲 Not Started
**Depends on:** Phase 4 (CSV + Minimal End-to-End Pipeline — format traits exist)
**Entry criteria:** `cargo test -p clinker-format` passes all Phase 4 format trait tests; `FormatReader` and `FormatWriter` traits are defined and implemented for CSV
**Exit criteria:** JSON and XML readers/writers implement `FormatReader`/`FormatWriter` traits; auto-detection selects correct JSON mode; XML pull-parser handles nested elements and attributes; `cargo test -p clinker-format` passes 40+ new tests

---

## Tasks

### Task 7.1: JSON Reader
**Status:** 🔲 Not Started
**Blocked by:** Phase 4 exit criteria

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

> ⛔ **Hard gate:** Task 7.2 status remains `Blocked` until all tests above pass.

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
- Field ordering: schema fields first (in schema order), then overflow fields (sorted alphabetically for determinism).
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
| `test_json_write_field_ordering` | Schema fields appear first in schema order, overflow fields sorted alphabetically | ⛔ Hard gate |
| `test_json_roundtrip_reader_writer` | Write records via JsonWriter, read back via JsonReader, assert field-level equality | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 7.3 status remains `Blocked` until all tests above pass.

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
- `namespace_handling`: `strip` (default) removes namespace prefixes (`ns:Order` → `Order`). `qualify` keeps them (`ns:Order` → `ns:Order`).
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

> ⛔ **Hard gate:** Task 7.4 status remains `Blocked` until all tests above pass.

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
- Overflow fields included after schema fields, sorted alphabetically for deterministic output.
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
