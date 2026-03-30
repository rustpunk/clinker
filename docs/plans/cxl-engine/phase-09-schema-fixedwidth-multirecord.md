# Phase 9: Schema System, Fixed-Width, Multi-Record

**Status:** 🔲 Not Started
**Depends on:** Phase 7 (schema applies to all format readers/writers)
**Entry criteria:** All format readers/writers (CSV, JSON, XML) passing Phase 7 tests; schema-less pipeline operational
**Exit criteria:** External schema YAML loaded with inherits resolution; schema_overrides strategic merge operational; fixed-width reader/writer handles schema-driven extraction; multi-record dispatcher routes lines by discriminator; `cargo test -p clinker-format -p clinker-core` passes 55+ tests

---

## Tasks

### Task 9.1: Schema File Loading + Inherits Resolution
**Status:** 🔲 Not Started
**Blocked by:** Phase 7 exit criteria

**Description:**
Parse external schema YAML files (`.yaml`/`.yml`) with `serde-saphyr`. Implement `defs:`
template definitions and `inherits:` mechanism for field-level property inheritance.

**Implementation notes:**
- Schema YAML structure: top-level `defs:` section containing named template definitions, top-level `fields:` section containing the actual field list.
- Each field in `defs:` or `fields:` has properties: `name`, `type`, `start`, `width`, `end`, `format`, `justify`, `pad`, `required`, `default`, `trim`.
- `inherits: template_name` on a field pulls all properties from the named `defs:` entry. Field-level overrides win (shallow merge — field's own keys overwrite template keys).
- No inherits chains: a `defs:` entry cannot itself use `inherits:`. Validate and reject with a clear error.
- Validation rules: `width` and `end` are mutually exclusive (if both present, config error). Unknown properties → config error (strict parsing via `serde(deny_unknown_fields)`).
- `SchemaFile` struct: `defs: HashMap<String, FieldDef>`, `fields: Vec<FieldDef>`. Loaded via `SchemaFile::load(path: &Path) -> Result<SchemaFile, SchemaError>`.
- After loading, resolve all `inherits:` references: iterate `fields`, look up `defs[template_name]`, merge, remove `inherits` key from resolved output.

**Acceptance criteria:**
- [ ] Schema YAML parsed with all field properties
- [ ] `defs:` templates stored and accessible by name
- [ ] `inherits:` resolves template properties into field
- [ ] Field-level overrides win over template properties
- [ ] Inherits chains rejected with clear error
- [ ] `width` + `end` mutual exclusion enforced
- [ ] Unknown properties rejected (strict parsing)

**Required unit tests (must pass before Task 9.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_schema_load_basic` | Simple schema YAML with 3 fields parses correctly | ⛔ Hard gate |
| `test_schema_defs_template` | `defs:` section parsed and accessible by name | ⛔ Hard gate |
| `test_schema_inherits_resolution` | Field with `inherits: base_string` gets template properties | ⛔ Hard gate |
| `test_schema_inherits_override` | Field overrides template's `type` property, template's `width` preserved | ⛔ Hard gate |
| `test_schema_inherits_chain_rejected` | Defs entry with `inherits:` produces config error | ⛔ Hard gate |
| `test_schema_inherits_unknown_template` | `inherits: nonexistent` produces error naming the missing template | ⛔ Hard gate |
| `test_schema_width_end_mutual_exclusion` | Field with both `width` and `end` produces config error | ⛔ Hard gate |
| `test_schema_unknown_property_rejected` | Field with `colour: red` (unknown) produces strict parsing error | ⛔ Hard gate |
| `test_schema_load_yaml_and_yml` | Both `.yaml` and `.yml` extensions load successfully | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 9.2 status remains `Blocked` until all tests above pass.

---

### Task 9.2: Schema Overrides + Strategic Merge
**Status:** ⛔ Blocked (waiting on Task 9.1)
**Blocked by:** Task 9.1 — schema file loading must be working

**Description:**
Implement `schema_overrides` on input configurations. Overrides use strategic merge by
field `name` (and `record:` scope for multi-record) to deep-merge, append, drop, or
alias fields from the base schema.

**Implementation notes:**
- `schema_overrides` is a list of field override entries on an input config. Each entry has at minimum a `name` field to match against the base schema.
- Strategic merge algorithm: iterate overrides, for each, find matching field in base schema by `name` (and optionally `record:` scope for multi-record schemas).
  - Match found: deep-merge override keys onto the base field (override wins per key).
  - No match: append as a new field at the end of the field list.
  - `drop: true` on an override: remove the matched field entirely from the schema.
- `alias: output_name` on a field: CXL expressions use the original `name`, but output writers use `alias` as the column header.
- Fail-fast rule: if an input has both an inline `schema:` section AND `schema_overrides:`, emit a config error immediately. Overrides only apply to externally referenced schemas.
- `record:` scope in override entries: when present, the override only applies to fields within the named record type (for multi-record schemas in Task 9.4).

**Acceptance criteria:**
- [ ] Override matches by field name and deep-merges properties
- [ ] Unmatched override appends as new field
- [ ] `drop: true` removes field from schema
- [ ] `alias:` sets output name while CXL uses original name
- [ ] Inline schema + schema_overrides → config error (fail-fast)
- [ ] Record-scoped overrides match only within named record type

**Required unit tests (must pass before Task 9.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_override_merge_existing_field` | Override `type: Float` on existing `amount` field replaces type, preserves width | ⛔ Hard gate |
| `test_override_append_new_field` | Override with name not in base schema appends field at end | ⛔ Hard gate |
| `test_override_drop_field` | `drop: true` removes field from resolved schema | ⛔ Hard gate |
| `test_override_drop_nonexistent_field` | `drop: true` on unknown field name → config error | ⛔ Hard gate |
| `test_override_alias` | `alias: emp_name` sets output name; CXL lookup still uses original `name` | ⛔ Hard gate |
| `test_override_inline_schema_conflict` | Input with both inline `schema:` and `schema_overrides:` → config error | ⛔ Hard gate |
| `test_override_record_scoped` | Override with `record: DETAIL` only affects fields in DETAIL record type | ⛔ Hard gate |
| `test_override_multiple_fields` | Three overrides applied in order: merge, append, drop | ⛔ Hard gate |
| `test_override_deep_merge_preserves_unset` | Override sets `format` but base's `justify`, `pad`, `required` all preserved | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 9.3 status remains `Blocked` until all tests above pass.

---

### Task 9.3: Fixed-Width Reader/Writer
**Status:** ⛔ Blocked (waiting on Task 9.2)
**Blocked by:** Task 9.2 — schema overrides must be working (fixed-width is schema-driven)

**Description:**
Implement fixed-width format reader and writer in `clinker-format`. Field extraction is
entirely schema-driven: each field has a `start` position and either `width` or `end`
(mutually exclusive). Supports configurable line separators, trimming, justification, and
pad characters.

**Implementation notes:**
- Reader: read lines according to `line_separator` config (`newline` = `\n`, `crlf` = `\r\n`, `none` = fixed record length with no delimiter).
- For each line, extract fields by byte offset: `start` is 0-indexed, `width` is byte count. If `end` is used instead, `width = end - start`.
- Trim trailing whitespace from each extracted field value (default `trim: true`, configurable per field).
- Type parsing: apply field's `type` to the extracted string. Use `format` for date/datetime parsing (e.g., `format: "%Y%m%d"`). Apply `coerce_to_*` from `clinker-record`.
- Right-justified numerics: if `justify: right` and `pad: "0"` (or space), strip leading pad characters before parsing the numeric value.
- Writer: for each output record, format each field to its declared `width`. Apply `justify` (left/right, default left for strings, right for numerics). Pad with `pad` character (default space). Truncate values that exceed width (with warning logged).
- Writer line separator matches reader config.

**Acceptance criteria:**
- [ ] Reader extracts fields by start + width from fixed-width lines
- [ ] Reader extracts fields by start + end (mutually exclusive with width)
- [ ] Line separator configurable: newline, crlf, none (fixed length)
- [ ] Trailing whitespace trimmed by default, configurable per field
- [ ] Date fields parsed with custom format strings
- [ ] Right-justified numerics with leading pad stripped before parsing
- [ ] Writer pads fields to declared width with correct justification
- [ ] Writer truncates overlong values with warning

**Required unit tests (must pass before Task 9.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_fixedwidth_read_basic` | 3 fields extracted by start+width from a single line | ⛔ Hard gate |
| `test_fixedwidth_read_start_end` | Field defined with start+end extracts same bytes as equivalent start+width | ⛔ Hard gate |
| `test_fixedwidth_read_trim_trailing` | Field `"Smith   "` trimmed to `"Smith"` with default trim | ⛔ Hard gate |
| `test_fixedwidth_read_no_trim` | Field with `trim: false` preserves trailing whitespace | ⛔ Hard gate |
| `test_fixedwidth_read_right_justified_numeric` | `"  042"` with `justify: right, pad: " "` parsed as integer 42 | ⛔ Hard gate |
| `test_fixedwidth_read_zero_padded_numeric` | `"00042"` with `justify: right, pad: "0"` parsed as integer 42 | ⛔ Hard gate |
| `test_fixedwidth_read_date_format` | `"20240115"` with `format: "%Y%m%d"` parsed as NaiveDate 2024-01-15 | ⛔ Hard gate |
| `test_fixedwidth_read_crlf_separator` | Lines separated by `\r\n` parsed correctly | ⛔ Hard gate |
| `test_fixedwidth_read_no_separator` | Fixed-length records with no line separator (record length = sum of widths) | ⛔ Hard gate |
| `test_fixedwidth_write_basic` | 3 fields written with correct padding to declared widths | ⛔ Hard gate |
| `test_fixedwidth_write_left_justify` | String field left-justified with trailing spaces | ⛔ Hard gate |
| `test_fixedwidth_write_right_justify` | Numeric field right-justified with leading spaces | ⛔ Hard gate |
| `test_fixedwidth_write_truncate_warning` | Value exceeding field width truncated, warning logged | ⛔ Hard gate |
| `test_fixedwidth_roundtrip` | Write then read produces identical field values | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 9.4 status remains `Blocked` until all tests above pass.

---

### Task 9.4: Multi-Record Dispatcher
**Status:** ⛔ Blocked (waiting on Task 9.3)
**Blocked by:** Task 9.3 — fixed-width reader must be working (multi-record is primarily a fixed-width feature)

**Description:**
Implement multi-record dispatch for files containing heterogeneous record types. A
discriminator field identifies the record type for each line, routing it to the
appropriate schema and processing path.

**Implementation notes:**
- `discriminator` config on an input: specifies how to identify record type per line.
  - Fixed-width: `{ start: N, width: M }` — extract byte range, trim, match against record type tags.
  - CSV: `{ column: "record_type" }` — read named column value, match against record type tags.
- `record_types` config: list of `{ id: "HEADER", tag: "H", fields: [...] }` entries. Each defines its own field schema.
- Dispatch logic: for each input line, extract discriminator value, compare (exact match after trim) against each record type's `tag`. Route to matching record type.
- Each record type gets its own `Arena` and `SecondaryIndex` during Phase 1 (two-pass). Records from different types never share an arena.
- Unmatched lines: by default, skip with a warning logged. If `fail_fast: true` on the input, unmatched line → hard error (abort pipeline).
- CXL field access for multi-record: `input_name.record_type_id.field_name` (e.g., `benefits.EEID.employee_id`). Three-part dotted path resolved during Phase B.
- `parent:` and `join_key:` metadata on record types: declarative hints for the engine to auto-generate `local_window` config linking child records to parent records. Not evaluated in v1 — stored as metadata for future use.

**Acceptance criteria:**
- [ ] Discriminator extracts tag from fixed-width byte range
- [ ] Discriminator extracts tag from CSV column
- [ ] Lines routed to correct record type by exact tag match
- [ ] Each record type gets its own Arena and SecondaryIndex
- [ ] Unmatched lines skipped with warning (default behavior)
- [ ] Unmatched lines cause hard error when `fail_fast: true`
- [ ] CXL three-part path `input.record.field` resolves correctly
- [ ] `parent:` and `join_key:` metadata parsed and stored

**Required unit tests (must pass before Phase 9 exit):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_dispatch_fixedwidth_discriminator` | Byte range 0..1 routes "H" to HEADER, "D" to DETAIL, "T" to TRAILER | ⛔ Hard gate |
| `test_dispatch_csv_discriminator` | Column `rec_type` routes "header" and "detail" to correct record types | ⛔ Hard gate |
| `test_dispatch_trim_tag` | Discriminator value `"D "` (trailing space) matches tag `"D"` | ⛔ Hard gate |
| `test_dispatch_separate_arenas` | HEADER and DETAIL records stored in distinct Arena instances | ⛔ Hard gate |
| `test_dispatch_unmatched_skip_warning` | Line with tag `"X"` (no match) skipped, warning logged | ⛔ Hard gate |
| `test_dispatch_unmatched_fail_fast` | With `fail_fast: true`, unmatched tag → pipeline error | ⛔ Hard gate |
| `test_dispatch_cxl_three_part_path` | `benefits.EEID.employee_id` resolves to correct field in EEID record arena | ⛔ Hard gate |
| `test_dispatch_parent_join_key_stored` | `parent:` and `join_key:` metadata parsed and accessible on record type config | ⛔ Hard gate |
| `test_dispatch_mixed_record_counts` | File with 10 HEADER, 90 DETAIL, 1 TRAILER → correct counts per record type | ⛔ Hard gate |
| `test_dispatch_empty_record_type` | Record type with zero matching lines → empty Arena, no error | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 9 exit criteria not met until all tests above pass.
