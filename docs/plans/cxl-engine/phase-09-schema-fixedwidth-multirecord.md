# Phase 9: Schema System, Fixed-Width, Multi-Record

**Status:** 🔲 Not Started
**Depends on:** Phase 7 (schema applies to all format readers/writers)
**Entry criteria:** All format readers/writers (CSV, JSON, XML) passing Phase 7 tests; schema-less pipeline operational
**Exit criteria:** External schema YAML loaded with inherits resolution; schema_overrides strategic merge operational; fixed-width reader/writer handles schema-driven extraction; multi-record dispatcher routes lines by discriminator; all 42+ Phase 9 hard-gate tests pass

> ✅ **[V-5-3] Resolved:** Exit criterion reworded from "55+ tests" (already met by baseline) to "all 42+ Phase 9 hard-gate tests pass."

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
- `alias:` in base schema `fields:` has the same semantics as in overrides: output rename only, CXL uses original `name`. Per same-FieldDef decision (#42).
- `enum` (allowed_values) property is parsed and stored on `FieldDef` but enforcement (DLQ routing for values outside enum) is deferred to Phase 11 Task 11.4 (declarative validations).
- `drop: true` and `record:` fields on `FieldDef` must be validated as `None` in base schema context (non-override). Config error if present.
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
| `test_schema_source_filepath_deser` | YAML string `"schemas/base.yaml"` → `SchemaSource::FilePath` | ⛔ Hard gate |
| `test_schema_source_inline_deser` | YAML map `{ fields: [...] }` → `SchemaSource::Inline(SchemaDefinition)` | ⛔ Hard gate |
| `test_schema_drop_in_base_rejected` | Schema file field with `drop: true` → config error (override-only field) | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 9.2 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **9.1.1** Define `FieldDef`, `FieldType`, `Justify`, `LineSeparator`, `TruncationPolicy`, `Discriminator`, `RecordTypeDef`, `StructureConstraint`, `SchemaDefinition` in `clinker-record/src/schema_def.rs`
- [ ] **9.1.2** Implement `SchemaSource` custom `Deserialize` (`visit_str` → `FilePath`, `visit_map` → `Inline(SchemaDefinition)`) in `clinker-core/src/config.rs`
- [ ] **9.1.3** Implement `load_schema()` + `resolve_schema()` in `clinker-core/src/schema/mod.rs` — file I/O, deser into `SchemaDefinition`, inherits resolution, validation (fields/records exclusivity, width/end mutual exclusion, format cross-validation, no inherits chains, unknown template rejection). Define `SchemaError` enum in this module with `PipelineError::Schema(SchemaError)` wrapper variant.

> ✅ **[V-6-1] Resolved:** `SchemaError` defined in `clinker-core/src/schema/mod.rs` with `PipelineError::Schema(SchemaError)` wrapper. DataFusion `SchemaError` pattern. Research-backed.

> ✅ **[V-2-1] Resolved:** `SchemaDefinition.format` changed from `Option<FormatKind>` to `Option<String>`. `FormatKind` stays in `clinker-core`. Cross-validation in `resolve_schema()`. Research: 7/8 systems keep format external to schema (Arrow, Polars, Spark, Beam, Protobuf, Avro, JSON Schema).

> 🟠 **[V-8-2] DEFERRED:** Path validation for `load_schema()` deferred to Phase 11 Task 11.5 (central path security layer). Research: 15+ CVEs from per-subsystem validation across Ansible, Docker Compose, Terraform, Helm, dbt. Central validation is the correct pattern. Add `#[ignore] test_schema_path_traversal_rejected` placeholder.
- [ ] **9.1.4** Convert `SortFieldSpec` from `#[serde(untagged)]` to custom `Deserialize` (`visit_str` → Short, `visit_map` → Full) for consistent error messages
- [ ] **9.1.5** Wire `SchemaSource` into `InputConfig` (replace `Option<String>` with `Option<SchemaSource>`), update existing config tests

#### Code scaffolding
```rust
// Module: clinker-record/src/schema_def.rs

/// Field type enum — parse-time validated, exhaustive match downstream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldType {
    String, Integer, Float, Decimal, Boolean, Date, DateTime,
    Object,  // parse-only stub for JSON/XML nested schemas
    Array,   // parse-only stub
}

/// Field justification for fixed-width formatting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Justify { Left, Right }

/// Line separator mode for fixed-width I/O.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LineSeparator { Lf, CrLf, None }

/// Truncation policy for fixed-width writer.
/// Default resolved by field type: numeric → Error, string → Warn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncationPolicy { Error, Warn, Silent }

/// A field definition from a schema file, inline schema, or override.
/// All fields except `name` are Optional to support both full definitions
/// and partial overrides (schema_overrides use the same struct).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldDef {
    pub name: String,
    #[serde(default, rename = "type")]
    pub field_type: Option<FieldType>,    // default: String when None
    pub required: Option<bool>,
    pub format: Option<String>,
    pub coerce: Option<bool>,
    pub default: Option<serde_json::Value>,  // arbitrary YAML scalar
    #[serde(rename = "enum")]
    pub allowed_values: Option<Vec<String>>,
    pub alias: Option<String>,
    pub inherits: Option<String>,
    // Fixed-width positioning
    pub start: Option<usize>,
    pub width: Option<usize>,
    pub end: Option<usize>,
    pub justify: Option<Justify>,
    pub pad: Option<String>,
    pub trim: Option<bool>,
    pub truncation: Option<TruncationPolicy>,
    // Decimal
    pub precision: Option<u8>,
    pub scale: Option<u8>,
    // XML
    pub path: Option<String>,
    // Override-only (validated as None in schema file contexts)
    pub drop: Option<bool>,
    pub record: Option<String>,
}

/// Discriminator for multi-record dispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Discriminator {
    pub start: Option<usize>,    // FW: byte offset
    pub width: Option<usize>,    // FW: byte count
    pub field: Option<String>,   // CSV: column name
}

/// A record type definition within a multi-record schema.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordTypeDef {
    pub id: String,
    pub tag: String,
    pub description: Option<String>,
    pub parent: Option<String>,
    pub join_key: Option<String>,
    pub fields: Vec<FieldDef>,
}

/// Structural ordering constraint (parsed, validated in Phase 11).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StructureConstraint {
    pub record: String,
    pub count: String,
}

/// Parsed schema — from file or inline. Single struct, one resolution path.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaDefinition {
    pub version: Option<String>,         // passive metadata
    pub format: Option<String>,           // cross-validated against input type at resolution time
    pub encoding: Option<String>,        // stored for future use
    pub defs: Option<HashMap<String, FieldDef>>,
    pub fields: Option<Vec<FieldDef>>,            // single-record
    pub records: Option<Vec<RecordTypeDef>>,       // multi-record
    pub discriminator: Option<Discriminator>,
    pub structure: Option<Vec<StructureConstraint>>,
}

// Module: clinker-core/src/schema/mod.rs

/// Resolved schema — inherits merged, validated, ready for pipeline use.
pub enum ResolvedSchema {
    SingleRecord { fields: Vec<FieldDef> },
    MultiRecord {
        discriminator: Discriminator,
        record_types: Vec<RecordTypeDef>,
    },
}

pub fn load_schema(path: &Path) -> Result<SchemaDefinition, SchemaError> { todo!() }
pub fn resolve_schema(def: SchemaDefinition) -> Result<ResolvedSchema, SchemaError> { todo!() }

// Module: clinker-core/src/config.rs

/// Schema source — file path or inline definition.
/// Custom Deserialize: visit_str → FilePath, visit_map → Inline.
pub enum SchemaSource {
    FilePath(String),
    Inline(SchemaDefinition),
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `FieldDef`, `FieldType`, `Justify`, `LineSeparator`, `TruncationPolicy` | `clinker-record/src/schema_def.rs` | Shared data types — all crates access |
| `Discriminator`, `RecordTypeDef`, `StructureConstraint`, `SchemaDefinition` | `clinker-record/src/schema_def.rs` | Schema structure types |
| `ResolvedSchema`, `load_schema()`, `resolve_schema()` | `clinker-core/src/schema/mod.rs` | Loading + resolution logic |
| `SchemaSource` (custom Deserialize) | `clinker-core/src/config.rs` | Config serde boundary |

#### Intra-phase dependencies
- Requires: None (first task in phase)
- Unblocks: Task 9.2 (overrides merge onto ResolvedSchema)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies basic YAML schema parsing with 3 fields.
    #[test]
    fn test_schema_load_basic() {
        let yaml = r#"
fields:
  - name: id
    type: integer
    start: 0
    width: 5
  - name: name
    type: string
    start: 5
    width: 20
  - name: date
    type: date
    start: 25
    width: 8
    format: "%Y%m%d"
"#;
        let def: SchemaDefinition = serde_saphyr::from_str(yaml).unwrap();
        assert_eq!(def.fields.as_ref().unwrap().len(), 3);
        assert_eq!(def.fields.as_ref().unwrap()[0].name, "id");
    }

    /// Verifies SortFieldSpec custom deser: string input.
    #[test]
    fn test_sort_field_spec_custom_deser_string() {
        let yaml = "\"field_name\"";
        let spec: SortFieldSpec = serde_saphyr::from_str(yaml).unwrap();
        let sf = spec.into_sort_field();
        assert_eq!(sf.field, "field_name");
        assert_eq!(sf.order, SortOrder::Asc);
    }

    /// Verifies SortFieldSpec custom deser: map input with good error on typo.
    #[test]
    fn test_sort_field_spec_custom_deser_map() {
        let yaml = "{ field: name, order: desc }";
        let spec: SortFieldSpec = serde_saphyr::from_str(yaml).unwrap();
        let sf = spec.into_sort_field();
        assert_eq!(sf.order, SortOrder::Desc);
    }
}
```

#### Risk / gotcha
> **SchemaDefinition in clinker-record pulls serde_json::Value:** The `FieldDef.default` field uses `serde_json::Value` for arbitrary YAML scalars. `serde_json` is already a `clinker-record` dependency, so no new dependency edge. Confirmed safe. However, `serde_json::Value::Object` preserves insertion order only with the `preserve_order` feature — already enabled in workspace Cargo.toml.

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
| `test_override_alias_mapping_interaction` | Output mapping uses post-alias name `emp_id`; pre-alias `employee_id` does NOT match in mapping | ⛔ Hard gate |
| `test_override_inline_schema_conflict` | Input with both inline `schema:` and `schema_overrides:` → config error | ⛔ Hard gate |
| `test_override_record_scoped` | Override with `record: DETAIL` only affects fields in DETAIL record type | ⛔ Hard gate |
| `test_override_multiple_fields` | Three overrides applied in order: merge, append, drop | ⛔ Hard gate |
| `test_override_deep_merge_preserves_unset` | Override sets `format` but base's `justify`, `pad`, `required` all preserved | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 9.3 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **9.2.1** Expand `InputConfig.schema_overrides` from `Vec<SchemaOverride>` to `Vec<FieldDef>` (`FieldDef` now carries `drop` + `record` fields). Remove old `SchemaOverride` struct. **Atomically update all consumers:** `pipeline/index.rs` (`SecondaryIndex::build` parameter), `executor.rs` (map construction), `pipeline/ingestion.rs` (map passing).

> ✅ **[V-1-1] Resolved:** `index.rs`, `executor.rs`, `ingestion.rs` added to 9.2.1 scope for atomic migration. Research: Polars (32 files), SpacetimeDB (55 files), DataFusion (36 files) all do atomic struct replacement for internal workspace types. No project uses broken intermediate states.
- [ ] **9.2.2** Implement strategic merge: `resolve_overrides(schema: &mut ResolvedSchema, overrides: &[FieldDef]) -> Result<(), SchemaError>` — match by name (+ record scope for multi-record), deep-merge `Some` fields, append unmatched, drop flagged, error on drop of unknown
- [ ] **9.2.3** Implement fail-fast: inline schema + schema_overrides → config error
- [ ] **9.2.4** Implement alias resolution in executor output preparation — alias creates identity boundary (SQL model): CXL uses original names, mapping keys reference post-alias names, writers receive records with final names

#### Code scaffolding
```rust
// Module: clinker-core/src/schema/resolve.rs

/// Apply override entries onto a resolved schema.
/// Overrides use the same FieldDef struct with drop/record fields.
pub fn resolve_overrides(
    schema: &mut ResolvedSchema,
    overrides: &[FieldDef],
) -> Result<(), SchemaError> {
    for patch in overrides {
        if let Some(true) = patch.drop {
            drop_field(schema, &patch.name, patch.record.as_deref())?;
        } else {
            merge_or_append(schema, patch)?;
        }
    }
    Ok(())
}

fn merge_field(base: &mut FieldDef, patch: &FieldDef) {
    // For each Option field on patch: if Some, overwrite base
    if patch.field_type.is_some() { base.field_type = patch.field_type.clone(); }
    if patch.required.is_some() { base.required = patch.required; }
    if patch.format.is_some() { base.format = patch.format.clone(); }
    // ... etc for all Option fields
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `resolve_overrides()`, `merge_field()` | `clinker-core/src/schema/resolve.rs` | Override merge logic |
| `SchemaOverride` removal | `clinker-core/src/config.rs` | Replaced by `Vec<FieldDef>` |
| Alias resolution | `clinker-core/src/executor.rs` | Output preparation step |

#### Intra-phase dependencies
- Requires: Task 9.1 (`FieldDef`, `ResolvedSchema`)
- Unblocks: Task 9.3 (fixed-width reader needs resolved schema with overrides applied)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Alias creates identity boundary: mapping keys use post-alias names.
    #[test]
    fn test_override_alias() {
        // Schema: { name: employee_id, alias: emp_id }
        // CXL: employee_id resolves to field value
        // Output mapping: emp_id → "Employee ID"
        // Writer sees: "Employee ID" column with employee_id's value
    }
}
```

#### Risk / gotcha
> **Alias identity boundary interaction with OutputConfig.mapping:** Alias resolution happens in the executor's output preparation step. The precedence is: alias creates the field's "public name" (SQL model). Output mapping keys reference this post-alias name. If a mapping key references a pre-alias original name, it does NOT match — this is by design (industry consensus: SQL, Spark, Informatica, NiFi all work this way). Document this in the config reference.

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
| `test_fixedwidth_write_truncate_numeric_error` | Numeric value exceeding field width → FormatError (not silent truncation) | ⛔ Hard gate |
| `test_schema_zero_width_field_rejected` | Field with `width: 0` → config error at load time | ⛔ Hard gate |
| `test_fixedwidth_roundtrip` | Write then read produces identical field values | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 9.4 status remains `Blocked` until all tests above pass.

#### Sub-tasks
- [ ] **9.3.1** Add `FixedWidth` variant to `InputFormat` and `OutputFormat` enums. Define `FixedWidthInputOptions { line_separator: Option<LineSeparator> }` and `FixedWidthOutputOptions { line_separator: Option<LineSeparator> }` in `clinker-core/src/config.rs`.
- [ ] **9.3.2** Implement `FixedWidthReader` in `clinker-format/src/fixed_width/reader.rs` — `BufReader` + `read_until(b'\n')` for Lf/CrLf, `read_exact(record_length)` for None mode. Byte-offset extraction (`&line_bytes[start..start+width]`), `str::from_utf8()` per field, symmetric padding strip (left-justify → trim trailing pad, right-justify → trim leading pad), type coercion via `coerce_to_*`. Schema injected at construction (constructor injection pattern — no `FormatReader` trait changes).
- [ ] **9.3.3** Implement `FixedWidthWriter` in `clinker-format/src/fixed_width/writer.rs` — format fields to declared width, justify + pad, type-aware truncation defaults (numeric → `Error`/DLQ, string → `Warn`+truncate) with per-field override via `FieldDef.truncation`.
- [ ] **9.3.4** Wire `FixedWidth` into executor format dispatch — `create_reader()` matches `InputFormat::FixedWidth` → `FixedWidthReader`, `create_writer()` matches `OutputFormat::FixedWidth` → `FixedWidthWriter`. Schema-required validation: `type=fixed_width` and `schema=None` → config error.
- [ ] **9.3.5** Validate field layout at config load time — no overlapping fields, `start + width` ≤ record width, `width`/`end` mutual exclusion (from Task 9.1), all FW fields have `start` defined, all FW fields have `width` or `end` defined, `width: 0` rejected.

#### Code scaffolding
```rust
// Module: clinker-format/src/fixed_width/reader.rs

use std::io::{BufRead, BufReader, Read};
use std::sync::Arc;
use clinker_record::{Record, Schema, Value};
use clinker_record::schema_def::{FieldDef, FieldType, Justify, LineSeparator};
use crate::error::FormatError;
use crate::traits::FormatReader;

pub struct FixedWidthReaderConfig {
    pub line_separator: LineSeparator,
}

/// Schema-driven fixed-width record reader.
/// Byte-offset extraction with per-field UTF-8 validation and type coercion.
/// Schema injected at construction (constructor injection pattern).
pub struct FixedWidthReader<R: Read> {
    reader: BufReader<R>,
    fields: Vec<FieldDef>,          // resolved, no inherits
    schema: Arc<Schema>,            // column names for Record construction
    config: FixedWidthReaderConfig,
    record_length: usize,           // max(field.start + field.width) — for None mode
    line_buf: Vec<u8>,              // reused per read
    row_number: u64,
}

impl<R: Read> FixedWidthReader<R> {
    pub fn new(reader: R, fields: Vec<FieldDef>, config: FixedWidthReaderConfig) -> Self {
        let columns: Vec<Box<str>> = fields.iter()
            .map(|f| f.name.as_str().into())
            .collect();
        let schema = Arc::new(Schema::new(columns));
        let record_length = fields.iter()
            .map(|f| f.start.unwrap_or(0) + f.width.unwrap_or(0))
            .max()
            .unwrap_or(0);
        Self {
            reader: BufReader::new(reader),
            fields, schema, config, record_length,
            line_buf: Vec::with_capacity(256),
            row_number: 0,
        }
    }
}

impl<R: Read + Send> FormatReader for FixedWidthReader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        Ok(Arc::clone(&self.schema))
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        todo!()
    }
}

// Module: clinker-format/src/fixed_width/writer.rs

/// Schema-driven fixed-width record writer.
/// Type-aware truncation: numeric → Error, string → Warn (configurable per field).
pub struct FixedWidthWriter<W: Write> {
    writer: W,
    fields: Vec<FieldDef>,
    config: FixedWidthWriterConfig,
}

pub struct FixedWidthWriterConfig {
    pub line_separator: LineSeparator,
}

impl<W: Write + Send> FormatWriter for FixedWidthWriter<W> {
    fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
        todo!()
    }

    fn flush(&mut self) -> Result<(), FormatError> {
        self.writer.flush().map_err(FormatError::Io)
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `FixedWidthReader` | `clinker-format/src/fixed_width/reader.rs` | New module |
| `FixedWidthWriter` | `clinker-format/src/fixed_width/writer.rs` | New module |
| `FixedWidthInputOptions` | `clinker-core/src/config.rs` | Serde config |
| `FixedWidthOutputOptions` | `clinker-core/src/config.rs` | Serde config |

#### Intra-phase dependencies
- Requires: Task 9.1 (`FieldDef`), Task 9.2 (`ResolvedSchema` with overrides applied)
- Unblocks: Task 9.4 (multi-record dispatcher uses `FixedWidthReader` for FW line parsing)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Short line padded with spaces when not in fail_fast mode.
    #[test]
    fn test_fixedwidth_read_short_line_pad() {
        // Line: "AB" (2 bytes), schema expects 10 bytes
        // Reader pads to "AB        ", extracts fields normally
    }

    /// Invalid UTF-8 in a field slice → DLQ-able error.
    #[test]
    fn test_fixedwidth_read_invalid_utf8_field() {
        // Line contains 0xFF in a field position
        // from_utf8() fails → FormatError::InvalidRecord
    }

    /// CRLF stripping: \r removed before field extraction.
    #[test]
    fn test_fixedwidth_read_crlf_stripping() {
        // Line: "SMITH     \r\n" in CrLf mode
        // \r\n stripped → "SMITH     " → field extracted correctly
    }

    /// Numeric truncation → Error (type-aware default).
    #[test]
    fn test_fixedwidth_write_truncate_numeric_error() {
        // Value 123456789 written to 5-width integer field
        // → FormatError (not silent truncation)
    }
}
```

#### Risk / gotcha
> **FixedWidthReader constructor injection vs FormatReader trait:** `FixedWidthReader` receives `Vec<FieldDef>` at construction — schema is known before any data is read. This differs from CSV/JSON/XML readers that discover schema from data. The `open_reader` closure in `ingest_sources` has access to config and can resolve the schema before constructing the reader. No trait change needed. Research confirms this is the unanimous industry pattern (DataFusion `FileScanConfig`, Spark `ScanBuilder`, Polars `CsvReadOptions::with_schema()`).

> **Byte vs character semantics:** Field positions are byte offsets (spec says "byte position"). `str::from_utf8()` validates each extracted field slice. If a file was converted from single-byte encoding to UTF-8, byte offsets may not match — the error message must name the field, positions, and the invalid byte sequence to be actionable. Character-offset mode is a v2 toggle via `position_mode: bytes | characters` on the schema file.

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
| `test_dispatch_discriminator_short_line` | Line shorter than discriminator start+width → SchemaError, not panic | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 9 exit criteria not met until all tests above pass.

#### Sub-tasks
- [ ] **9.4.1** Extend `QualifiedFieldRef` AST node: replace `source: Box<str>` + `field: Box<str>` with `parts: Box<[Box<str>]>` (sqlparser-rs/Spark `CompoundIdentifier` pattern, boxed slice per SWC/oxc AST size conventions). Update parser to chain dot-access into parts, collect into Vec then `.into_boxed_slice()`. Update resolver, type checker, evaluator for parts-based access. **Also update `cxl-cli/src/main.rs`** (pattern-matches on QualifiedFieldRef). Ensure all existing 2-part CXL tests still pass.

> ✅ **[V-1-2] Resolved:** `cxl-cli/src/main.rs` added to 9.4.1 scope. `parts` changed from `Vec<Box<str>>` to `Box<[Box<str>]>` — saves 8 bytes inline (16 vs 24), eliminates wasted capacity word, per SWC/oxc AST node size conventions and Rust Perf Book boxed-slice guidance. Research-backed.
- [ ] **9.4.2** Verify `Discriminator` config types from Task 9.1 are sufficient. `FixedWidthDiscriminator` uses `start` + `width` fields; `CsvDiscriminator` uses `field` field. Both on the same `Discriminator` struct (already defined in `SchemaDefinition`).
- [ ] **9.4.3** Implement `MultiRecordDispatcher` in `clinker-core/src/pipeline/dispatch.rs` — reads file line by line, extracts discriminator value (byte range for FW, column for CSV), trims and matches against record type tags, routes each line to correct record type's field parser, builds per-type Arena + SecondaryIndex. Returns `HashMap<String, (Arena, SecondaryIndex)>`. Unmatched lines: skip with warning (default), error if `fail_fast`.
- [ ] **9.4.4** Wire `MultiRecordDispatcher` into `ingest_sources` — detect multi-record input (`ResolvedSchema::MultiRecord`), replace `Arena::build()` call with `dispatcher.ingest()`, register arenas with dotted compound keys (`"input_name.record_type_id"`). Store `parent`/`join_key` metadata on `RecordTypeDef`.
- [ ] **9.4.5** CXL three-part path resolution — resolver: `parts.len() == 3` → `resolve_qualified("source.record_type", field)` where arena key is the dotted compound. `RecordStorage` lookup uses the dotted key in the arena map. `FieldResolver::resolve_qualified` already works with string keys.

#### Code scaffolding
```rust
// Module: cxl/src/ast.rs (modified)

/// Qualified field reference — N-part dotted path.
/// 2 parts: source.field (existing). 3 parts: source.record_type.field (multi-record).
QualifiedFieldRef {
    node_id: NodeId,
    parts: Box<[Box<str>]>,  // sqlparser-rs CompoundIdentifier pattern, boxed slice per SWC/oxc
    span: Span,
}

// Module: clinker-core/src/pipeline/dispatch.rs (new)

/// Multi-record dispatcher — reads one file, produces N arenas.
pub struct MultiRecordDispatcher {
    discriminator: Discriminator,
    record_types: Vec<RecordTypeDef>,
}

impl MultiRecordDispatcher {
    /// Ingest a multi-record file. Returns one (Arena, SecondaryIndex) per record type.
    /// Arenas keyed by record type id (e.g., "EEID", "PLAN", "TRLR").
    pub fn ingest<R: Read>(
        &self,
        reader: BufReader<R>,
        plan: &ExecutionPlan,
        config: &PipelineConfig,
        input_name: &str,
        memory_limit: usize,
    ) -> Result<HashMap<String, (Arena, SecondaryIndex)>, PipelineError> {
        todo!()
    }

    /// Extract discriminator value from a line.
    fn extract_tag(&self, line: &[u8]) -> Result<String, PipelineError> {
        match (&self.discriminator.start, &self.discriminator.field) {
            (Some(start), _) => {
                // Fixed-width: byte range extraction
                let width = self.discriminator.width.unwrap_or(1);
                let end = *start + width;
                let slice = line.get(*start..end)
                    .ok_or_else(|| PipelineError::Schema(SchemaError::Validation(
                        format!("line too short for discriminator: need {} bytes, got {}", end, line.len())
                    )))?;
                let tag = std::str::from_utf8(slice)
                    .map_err(|_| PipelineError::Schema(SchemaError::Validation("invalid UTF-8 in discriminator".into())))?;

> ✅ **[V-8-1] Resolved:** `extract_tag` uses `.get(range)` for bounds-safe byte extraction. Short lines produce `SchemaError::Validation` instead of panic. Per fixed_width crate / csv-core pattern. Research-backed.

> ✅ **[V-6-2] Resolved:** All error constructors in dispatcher scaffolding use `PipelineError::Schema(SchemaError::Validation(...))` per V-6-1 DataFusion-pattern decision.
                Ok(tag.trim().to_string())
            }
            (_, Some(field)) => {
                // CSV: column-based extraction (requires header parsing)
                todo!("CSV discriminator")
            }
            _ => Err(PipelineError::Schema(SchemaError::Validation("discriminator must specify start+width or field".into()))),
        }
    }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `QualifiedFieldRef` (modified) | `cxl/src/ast.rs` | `parts: Vec<Box<str>>` replaces source+field |
| `MultiRecordDispatcher` | `clinker-core/src/pipeline/dispatch.rs` | New module |
| Parser update | `cxl/src/parser.rs` | Chain dot-access into parts vec |
| Resolver update | `cxl/src/resolve/pass.rs` | Handle `parts.len() == 2` and `== 3` |
| Evaluator update | `cxl/src/eval/mod.rs` | Construct dotted key from parts |

#### Intra-phase dependencies
- Requires: Task 9.1 (`RecordTypeDef`, `Discriminator`), Task 9.2 (overrides), Task 9.3 (`FixedWidthReader` for FW line parsing)
- Phase exit: all Phase 9 tests must pass

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// Existing two-part qualified refs still work after parts vec migration.
    #[test]
    fn test_qualified_ref_two_part_backward_compat() {
        // "source.field" → parts: ["source", "field"]
        // Resolver: resolve_qualified("source", "field") — unchanged behavior
    }

    /// Three-part qualified ref parses correctly.
    #[test]
    fn test_qualified_ref_three_part_parse() {
        // "benefits.EEID.employee_id" → parts: ["benefits", "EEID", "employee_id"]
    }
}
```

#### Risk / gotcha
> **QualifiedFieldRef parts change is cross-cutting:** Touches 4 modules in the `cxl` crate (parser, resolver, type checker, evaluator). All changes are mechanical (replace `source`/`field` access with `parts[0]`/`parts[last]` or `parts.join(".")`). The existing ~100 CXL tests are the safety net — run the full suite after the AST change. No behavioral change for 2-part paths.

> **Arena key format for multi-record:** Arenas from multi-record inputs are registered with dotted compound keys: `"benefits.EEID"`, `"benefits.PLAN"`, `"benefits.TRLR"`. The executor must construct these keys consistently during both ingestion (dispatcher output) and CXL resolution (from `parts[0..parts.len()-1].join(".")`).

---

## Intra-Phase Dependency Graph

```
Task 9.1 (Schema Types + Loading)
    |
    +---> Task 9.2 (Overrides + Strategic Merge)
              |
              +---> Task 9.3 (Fixed-Width Reader/Writer)
                        |
                        +---> Task 9.4 (Multi-Record Dispatcher)
                                  |
                                  +---> Phase 9 Exit
```

Critical path: 9.1 → 9.2 → 9.3 → 9.4 (strictly sequential)
No parallelizable tasks in this phase.

---

## Decisions Log (drill-phase — 2026-03-30)
| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| 1 | Schema types in `clinker-record`, loading in `clinker-core` (split) | Follows existing Schema/Arena split pattern. No new dependency edges. | Task 9.1, all tasks |
| 2 | `FieldDef` flat struct with typed `FieldType`/`Justify` enums | Research: every execution engine uses typed enums (Arrow, Polars, Spark, Beam). `serde_json::Value` for `default` (serde-saphyr has no Value type). | Task 9.1, all tasks |
| 3 | `SchemaSource` custom Deserialize (visit_str/visit_map) | Research: Docker Compose normalize-then-deser; serde untagged has bad error messages. Custom deser gives specific errors. | Task 9.1 |
| 4 | Convert `SortFieldSpec` from untagged to custom deser | Consistency with SchemaSource. Better error messages on malformed sort configs. | Task 9.1 |
| 5 | Single `SchemaDefinition` struct for file + inline schemas | One struct, one resolution path. `SchemaFile::load()` does I/O → SchemaDefinition → resolve(). | Task 9.1 |
| 6 | `version:` stored as passive metadata, `format:` cross-validated, `structure:` parsed but deferred to Phase 11 | version is schema evolution tracking (not YAML spec version). format mismatch is a cheap safety net. | Task 9.1 |
| 7 | `fields:` / `records:` mutual exclusion validated in `load()` | Simple 5-line check after deser. Clearer error message with file path than serde enum. | Task 9.1 |
| 8 | `ResolvedSchema` enum (SingleRecord / MultiRecord) | Research: 5-to-2 in favor of distinct resolved types (DataFusion, Protobuf, Rust compiler, TS compiler, serde). Eliminates variant ambiguity downstream. | Task 9.1, 9.2, 9.3, 9.4 |
| 9 | Same `FieldDef` struct for definitions and overrides (`drop` + `record` added) | Research: Docker Compose + Terraform use same type for base and override. Override-only fields validated as None in non-override contexts. | Task 9.2 |
| 10 | Alias creates identity boundary (SQL model) | Research: unanimous across SQL, dbt, Singer, Informatica, NiFi. Post-alias name is the canonical name for downstream. Mapping keys reference post-alias names. | Task 9.2 |
| 11 | Aliases resolved in executor (writers see final names) | Research: DataFusion Expr::Alias, Spark Project+Alias, Polars ExprIR — all resolve aliases before sink. Writers are alias-unaware. | Task 9.2 |
| 12 | `LineSeparator` on `FixedWidthInputOptions` (Lf/CrLf/None) | Research: line separator is I/O config, not schema. FlatFiles/.NET HasRecordSeparator pattern. | Task 9.3 |
| 13 | Byte-based extraction with `str::from_utf8()` per field | Research: Informatica, Cobrix, fixed_width crate all use byte offsets. Spec says "byte position". Character mode is v2 toggle. | Task 9.3 |
| 14 | Fixed-width output requires schema (FieldDef with width/justify/pad) | Research: all tools that support writing use same field schema for read + write. | Task 9.3 |
| 15 | Type-aware truncation defaults with per-field override | Research: Informatica rejects numeric overflow by default. Silent numeric truncation caused real payment routing failures. Integer/Float/Decimal → Error, String → Warn, per-field override via `TruncationPolicy`. | Task 9.3 |
| 16 | Short-line policy: pad with spaces (default), error if fail_fast | Pandas pattern for lenient. Always log warning with line number + expected vs actual length. | Task 9.3 |
| 17 | No-separator mode: record_length from schema, read_exact() | FlatFiles/.NET HasRecordSeparator=false. Detect surprise newlines with warning. | Task 9.3 |
| 18 | Constructor injection for FixedWidthReader schema | Research: unanimous — DataFusion FileScanConfig, Spark ScanBuilder, Polars CsvReadOptions.with_schema(). No FormatReader trait changes. | Task 9.3 |
| 19 | `MultiRecordDispatcher` in clinker-core between reader and arenas | One file read → N arenas. Arena::build() can't handle multi-record (consumes reader into one arena). | Task 9.4 |
| 20 | `QualifiedFieldRef.parts: Vec<Box<str>>` (sqlparser-rs pattern) | Research: sqlparser-rs CompoundIdentifier, Spark UnresolvedAttribute, PostgreSQL ColumnRef all use flat Vec. Dedicated 3-part node is a known PostgreSQL mistake. | Task 9.4 |

## Assumptions Log (drill-phase — 2026-03-30)
| # | Assumption | Basis | Risk if wrong |
|---|-----------|-------|---------------|
| 1 | Fixed-width files are byte-oriented (not character-oriented) | Spec says "byte position"; enterprise ETL convention | UTF-8 multibyte files need character mode — add `position_mode` toggle (~30-line variant) |
| 2 | `serde_json::Value` adequate for FieldDef default values | Research: Singer uses arbitrary JSON, no tool uses custom Value for schema defaults | If complex YAML defaults needed (anchors, tags), would need saphyr-specific handling |
| 3 | Three-part CXL paths sufficient (no four-part paths needed) | Spec only defines `input.record_type.field` | Vec<Box<str>> already supports N-part — just add resolver logic if needed |
| 4 | `structure:` ordering constraints deferred to Phase 11 | Phase 11 owns all validation rules | If users need structure validation earlier, move the validation logic forward |
