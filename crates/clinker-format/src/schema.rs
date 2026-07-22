//! Unified source-schema vocabulary.
//!
//! One [`Column`] type — the superset column carrying a single
//! [`cxl::typecheck::Type`] plus every physical-extraction / formatting
//! attribute — replaces the two historical schema layers (the format-layer
//! `FieldDef`/`FieldType` byte layout and the CXL-layer `ColumnDecl`/`Type`
//! declaration). A source declares each column exactly once.
//!
//! [`SourceSchema`] is a sum over record cardinality:
//!
//! - [`SourceSchema::Columns`] — a single-record column list (CSV / JSON / XML
//!   / single-layout fixed-width). YAML: a sequence of column maps.
//! - [`SourceSchema::MultiRecord`] — discriminator-driven multi-record flat
//!   files. YAML: a map carrying `discriminator` + `records` (+ optional
//!   `structure`).
//! - [`SourceSchema::Generated`] — engine-synthesized positional schema
//!   (EDI/HL7/SWIFT). YAML: `{ generated: {} }`.
//! - [`SourceSchema::File`] — an external `.schema.yaml` path (a shareable
//!   schema resolved into one of the inline forms). YAML: a bare string.
//!
//! Serde discipline (hard rule): the sum is deserialized through a manual
//! `deserialize_any` visitor that dispatches on the YAML node shape
//! (string / sequence / map). It never routes through a
//! `#[serde(flatten)]`+tagged enum or a `#[serde(untagged)]` enum — both
//! buffer through a span-losing `Content` intermediate. `Column` itself is a
//! single `#[serde(deny_unknown_fields)]` struct with flat `Option<_>`
//! attributes and a `type` field carrying the externally-tagged
//! `cxl::typecheck::Type` value, so saphyr source spans survive on every leaf.

use clinker_record::schema_def::{Justify, TruncationPolicy};
use cxl::typecheck::Type;
use serde::de::value::{MapAccessDeserializer, SeqAccessDeserializer};
use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

/// Separator a multi-value field is parsed from (read) and encoded with
/// (write) when the author declares none. Semicolon rather than comma: it is
/// the separator the business-facing bulk-loaders this audience already uses
/// mandate for multi-value cells, and it does not collide with the CSV field
/// delimiter. One constant, so read and write never drift apart.
pub const DEFAULT_VALUE_DELIMITER: &str = ";";

/// One declared source column — the superset of the historical format-layer
/// `FieldDef` and CXL-layer `ColumnDecl`. Carries a single `type`
/// ([`cxl::typecheck::Type`]) that drives both byte-level parsing and
/// compile-time CXL typechecking, plus optional physical-extraction and
/// formatting attributes present only where the format needs them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Column {
    /// Exposed column name (what downstream CXL and the output see).
    pub name: String,
    /// Physical source column this declared column reads FROM, when it differs
    /// from [`name`](Self::name). `None` (the common case) means physical ==
    /// exposed. A channel `schema` patch's `rename` op sets this to preserve a
    /// column's physical binding while relabeling it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    /// The one column type. Drives byte parsing (fixed-width / multi-record)
    /// and compile-time CXL typechecking.
    #[serde(rename = "type")]
    pub ty: Type,

    // --- physical extraction (fixed-width / positional) ---
    /// Fixed-width start offset (0-based byte index).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<usize>,
    /// Fixed-width field width in bytes. Mutually exclusive with `end`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub width: Option<usize>,
    /// Fixed-width end offset (exclusive). Mutually exclusive with `width`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end: Option<usize>,
    /// XML path. Inert consumer today; carried for future wiring.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,

    // --- physical formatting (fixed-width read + write) ---
    /// Justification for fixed-width write (numeric defaults right, else left).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub justify: Option<Justify>,
    /// Pad character for fixed-width write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pad: Option<String>,
    /// Trim whitespace on fixed-width read.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trim: Option<bool>,
    /// Truncation policy for fixed-width write overflow.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub truncation: Option<TruncationPolicy>,

    // --- metadata ---
    /// strftime pattern for date/datetime parse (read) — honored by the
    /// single coercion path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Decimal precision (total significant digits). An attribute of a
    /// `type: decimal` column (validation/formatting metadata), not a type
    /// parameter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub precision: Option<u8>,
    /// Decimal scale (fractional digits). For a `type: decimal` column, the
    /// coercion path rounds parsed values to this scale (banker's rounding).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scale: Option<u8>,
    /// Whether the column must be present/non-null.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    /// Default value when the column is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    /// Whether to coerce the raw value to the declared type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub coerce: Option<bool>,
    /// Allowed value set (enum constraint).
    #[serde(default, rename = "enum", skip_serializing_if = "Option::is_none")]
    pub allowed_values: Option<Vec<String>>,
    /// Advisory storage hint: store high-cardinality, effectively-unique string
    /// values in the header-free `Box`-backed representation. Purely advisory —
    /// changes in-memory footprint only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub long_unique: Option<bool>,
    /// Whether this column holds more than one value. A `multiple: true` column
    /// is always array-valued: reading collects every occurrence of the field
    /// into a [`clinker_record::Value::Array`] (a single occurrence becomes a
    /// one-element array), and each element carries the column's declared
    /// [`ty`](Self::ty). The declaration is direction-neutral — it states the
    /// shape of the data, so a writer that can encode repetition uses the same
    /// declaration. An output format with no multi-value encoding rejects the
    /// column at plan time (E359) rather than degrading it at run time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiple: Option<bool>,
}

impl Column {
    /// A bare column with only `name` + `type` set and every optional attribute
    /// unset. Callers spread over it (`Column { start: Some(0), ..Column::bare(..) }`).
    pub fn bare(name: impl Into<String>, ty: Type) -> Self {
        Column {
            name: name.into(),
            source_name: None,
            ty,
            start: None,
            width: None,
            end: None,
            path: None,
            justify: None,
            pad: None,
            trim: None,
            truncation: None,
            format: None,
            precision: None,
            scale: None,
            required: None,
            default: None,
            coerce: None,
            allowed_values: None,
            long_unique: None,
            multiple: None,
        }
    }

    /// Whether the `long_unique` storage hint is set.
    pub fn is_long_unique(&self) -> bool {
        self.long_unique.unwrap_or(false)
    }

    /// Whether the column is declared multi-value (`multiple: true`).
    pub fn is_multiple(&self) -> bool {
        self.multiple.unwrap_or(false)
    }

    /// The type this column binds into CXL.
    ///
    /// A `multiple: true` column holds a [`clinker_record::Value::Array`], so
    /// it binds as [`Type::Array`]; its declared [`ty`](Self::ty) describes each
    /// ELEMENT and drives coercion, not the shape a CXL expression sees. Binding
    /// the element type instead would let `upper(tags)` typecheck against an
    /// array at run time.
    pub fn bound_type(&self) -> Type {
        if self.is_multiple() {
            Type::Array
        } else {
            self.ty.clone()
        }
    }

    /// The physical input-field name this column reads FROM: `source_name`
    /// when it aliases a differently-named field, else the exposed `name`.
    pub fn physical_name(&self) -> &str {
        self.source_name.as_deref().unwrap_or(self.name.as_str())
    }
}

/// Discriminator for multi-record dispatch: a byte range (`start`/`width`,
/// fixed-width) or a named field (`field`, CSV).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Discriminator {
    pub start: Option<usize>,
    pub width: Option<usize>,
    pub field: Option<String>,
}

/// One record type within a [`SourceSchema::MultiRecord`] schema.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RecordType {
    pub id: String,
    pub tag: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_key: Option<String>,
    pub columns: Vec<Column>,
}

/// Structural ordering/count constraint (parsed here, validated downstream).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct StructureConstraint {
    pub record: String,
    pub count: String,
}

/// Marker payload for [`SourceSchema::Generated`]. An empty
/// `deny_unknown_fields` struct so `{ generated: {} }` is accepted and a typo'd
/// key is rejected; the positional column synthesis reads the format's own
/// options, not this marker.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct GeneratedSchema {}

/// A source's unified schema — a sum over record cardinality plus the external
/// file form. See the module docs for the YAML shapes and the serde discipline.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub enum SourceSchema {
    /// Single-record column list.
    Columns(Vec<Column>),
    /// Discriminator-driven multi-record flat file.
    MultiRecord {
        discriminator: Discriminator,
        record_types: Vec<RecordType>,
        structure: Option<Vec<StructureConstraint>>,
    },
    /// Engine-synthesized positional schema (EDI/HL7/SWIFT).
    Generated(GeneratedSchema),
    /// External `.schema.yaml` file path, resolved to an inline form.
    File(String),
}

impl SourceSchema {
    /// The single-record column list, if this is a [`SourceSchema::Columns`].
    pub fn as_columns(&self) -> Option<&[Column]> {
        match self {
            SourceSchema::Columns(cols) => Some(cols),
            _ => None,
        }
    }

    /// Mutable single-record column list, if this is [`SourceSchema::Columns`].
    pub fn as_columns_mut(&mut self) -> Option<&mut Vec<Column>> {
        match self {
            SourceSchema::Columns(cols) => Some(cols),
            _ => None,
        }
    }

    /// The effective ordered column list a Source seeds its runtime row from:
    /// the column list for `Columns`, or the discriminator-led superset over
    /// every record type for `MultiRecord`. `Generated`/`File` yield `None`
    /// (their columns are synthesized / resolved elsewhere).
    pub fn bound_columns(&self) -> Option<Vec<Column>> {
        match self {
            SourceSchema::Columns(cols) => Some(cols.clone()),
            SourceSchema::MultiRecord { record_types, .. } => {
                Some(multi_record_superset(record_types))
            }
            SourceSchema::Generated(_) | SourceSchema::File(_) => None,
        }
    }
}

/// Reserved lead column engine-stamped onto every multi-record superset: the
/// record type id of the line each record was parsed from.
pub const RECORD_TYPE_COLUMN: &str = "record_type";

/// Build the discriminator-led superset column list over a multi-record
/// schema's record types: a leading engine-stamped `record_type` string column
/// followed by the union of every record type's columns. The first declaration
/// of each name wins its position and physical-extraction attributes; a name
/// shared across record types resolves to the *unified* (widened) type, so the
/// superset column the planner typechecks against accepts every record type's
/// runtime value (e.g. a field declared `int` in one type and `float` in
/// another widens to `float`). This mirrors the reader's `SupersetBuilder`
/// unify compatibility rule, so the typechecked row and the emitted records
/// cannot disagree on a shared column's type. A pair that does not unify keeps
/// the first declaration's type here; the reader rejects that config at build.
///
/// `multiple:` widens the same way, by OR rather than first-wins: a name any
/// record type declares `multiple: true` is multi-valued in the superset. The
/// superset is the one column list every consumer reads — the reader's
/// collect-these-fields set and both multi-value gates come off
/// `bound_columns()` — so first-wins would let a later record type's
/// declaration vanish, leaving its repeated occurrences to collapse to the
/// first value with no diagnostic anywhere. Single-valued is a subset of
/// array-valued (one occurrence yields a one-element array), which is why the
/// widening direction is sound.
pub fn multi_record_superset(record_types: &[RecordType]) -> Vec<Column> {
    let mut columns: Vec<Column> = vec![Column::bare(RECORD_TYPE_COLUMN, Type::String)];
    for rt in record_types {
        for col in &rt.columns {
            if let Some(existing) = columns.iter_mut().find(|c| c.name == col.name) {
                if let Some(unified) = existing.ty.unify(&col.ty) {
                    existing.ty = unified;
                }
                if col.is_multiple() {
                    existing.multiple = Some(true);
                }
            } else {
                columns.push(col.clone());
            }
        }
    }
    columns
}

impl<'de> Deserialize<'de> for SourceSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SourceSchemaVisitor;

        impl<'de> Visitor<'de> for SourceSchemaVisitor {
            type Value = SourceSchema;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a column sequence, a multi-record/generated schema map, or a schema file path",
                )
            }

            fn visit_str<E>(self, v: &str) -> Result<SourceSchema, E>
            where
                E: de::Error,
            {
                Ok(SourceSchema::File(v.to_owned()))
            }

            fn visit_string<E>(self, v: String) -> Result<SourceSchema, E>
            where
                E: de::Error,
            {
                Ok(SourceSchema::File(v))
            }

            fn visit_seq<A>(self, seq: A) -> Result<SourceSchema, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let cols = Vec::<Column>::deserialize(SeqAccessDeserializer::new(seq))?;
                Ok(SourceSchema::Columns(cols))
            }

            fn visit_map<A>(self, map: A) -> Result<SourceSchema, A::Error>
            where
                A: MapAccess<'de>,
            {
                // Forward the map straight into a derived struct so saphyr spans
                // survive (no `Content` buffering), then dispatch on which keys
                // were present.
                let repr = MapRepr::deserialize(MapAccessDeserializer::new(map))?;
                repr.into_source_schema::<A::Error>()
            }
        }

        deserializer.deserialize_any(SourceSchemaVisitor)
    }
}

/// The accepted keys of a map-form [`SourceSchema`]. `deny_unknown_fields`
/// rejects a typo; the post-parse dispatch enforces that exactly one schema
/// shape's keys are present.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MapRepr {
    #[serde(default)]
    discriminator: Option<Discriminator>,
    #[serde(default)]
    records: Option<Vec<RecordType>>,
    #[serde(default)]
    structure: Option<Vec<StructureConstraint>>,
    #[serde(default)]
    generated: Option<GeneratedSchema>,
}

impl MapRepr {
    fn into_source_schema<E: de::Error>(self) -> Result<SourceSchema, E> {
        let has_multi = self.discriminator.is_some() || self.records.is_some();
        match (self.generated, has_multi) {
            (Some(g), false) => {
                if self.structure.is_some() {
                    return Err(de::Error::custom(
                        "a `generated` schema takes no `structure`",
                    ));
                }
                Ok(SourceSchema::Generated(g))
            }
            (Some(_), true) => Err(de::Error::custom(
                "schema map mixes `generated` with multi-record keys (`records` / \
                 `discriminator`); use one shape",
            )),
            (None, true) => {
                let discriminator = self.discriminator.ok_or_else(|| {
                    de::Error::custom("multi-record schema requires a `discriminator` block")
                })?;
                let record_types = self.records.ok_or_else(|| {
                    de::Error::custom("multi-record schema requires a `records` list")
                })?;
                Ok(SourceSchema::MultiRecord {
                    discriminator,
                    record_types,
                    structure: self.structure,
                })
            }
            (None, false) => Err(de::Error::custom(
                "schema map is neither multi-record (`records` + `discriminator`) nor \
                 `generated`; a single-record schema is a sequence of columns",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    // These unit tests exercise the `deserialize_any` shape dispatch (sequence →
    // Columns, map → MultiRecord/Generated, string → File) through the
    // self-describing `serde_json` deserializer — the same dispatch saphyr
    // drives from YAML in production, but without pulling the plan crate's YAML
    // parser into `clinker-format`.
    use super::*;

    fn from_json<T: for<'de> serde::Deserialize<'de>>(j: &str) -> T {
        serde_json::from_str(j).expect("parse")
    }

    #[test]
    fn columns_from_sequence() {
        let s: SourceSchema = from_json(
            r#"[{"name":"id","type":"int"},{"name":"name","type":"string","long_unique":true}]"#,
        );
        let cols = s.as_columns().expect("columns");
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].ty, Type::Int);
        assert!(cols[1].is_long_unique());
    }

    #[test]
    fn superset_widens_multiple_across_record_types_in_either_order() {
        // Every multi-value consumer — the reader's collect-these-fields set,
        // both plan-time gates — reads the superset, so a `multiple: true` any
        // record type declares has to survive into it. First-wins would drop a
        // later record type's declaration and let its repeated occurrences
        // collapse to the first value with no diagnostic anywhere.
        let single = r#"{"name":"code","type":"string"}"#;
        let multiple = r#"{"name":"code","type":"string","multiple":true}"#;
        for (first, second) in [(single, multiple), (multiple, single)] {
            let schema: SourceSchema = from_json(&format!(
                r#"{{"discriminator":{{"field":"kind"}},
                    "records":[
                      {{"id":"header","tag":"H","columns":[{first}]}},
                      {{"id":"detail","tag":"D","columns":[{second}]}}
                    ]}}"#
            ));
            let columns = schema.bound_columns().expect("superset");
            let code = columns.iter().find(|c| c.name == "code").expect("code");
            assert!(
                code.is_multiple(),
                "declaration order must not decide the shape"
            );
        }
    }

    #[test]
    fn fixed_width_column_carries_start_width() {
        let s: SourceSchema = from_json(r#"[{"name":"id","type":"int","start":0,"width":5}]"#);
        let cols = s.as_columns().unwrap();
        assert_eq!(cols[0].start, Some(0));
        assert_eq!(cols[0].width, Some(5));
    }

    #[test]
    fn multi_record_from_map() {
        let s: SourceSchema = from_json(
            r#"{
                "discriminator": { "start": 0, "width": 1 },
                "records": [
                    { "id": "detail", "tag": "D", "columns": [ { "name": "amount", "type": "float", "start": 1, "width": 9 } ] },
                    { "id": "trailer", "tag": "T", "columns": [ { "name": "count", "type": "int", "start": 1, "width": 9 } ] }
                ],
                "structure": [ { "record": "trailer", "count": "count" } ]
            }"#,
        );
        match &s {
            SourceSchema::MultiRecord {
                discriminator,
                record_types,
                structure,
            } => {
                assert_eq!(discriminator.width, Some(1));
                assert_eq!(record_types.len(), 2);
                assert_eq!(structure.as_ref().unwrap().len(), 1);
            }
            other => panic!("expected MultiRecord, got {other:?}"),
        }
        // The superset leads with the engine-stamped record_type column.
        let superset = s.bound_columns().unwrap();
        assert_eq!(superset[0].name, RECORD_TYPE_COLUMN);
        assert_eq!(superset[0].ty, Type::String);
        let names: Vec<&str> = superset.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, vec![RECORD_TYPE_COLUMN, "amount", "count"]);
    }

    #[test]
    fn generated_from_map() {
        let s: SourceSchema = from_json(r#"{"generated":{}}"#);
        assert!(matches!(s, SourceSchema::Generated(_)));
    }

    #[test]
    fn file_from_string() {
        let s: SourceSchema = from_json(r#""schemas/base.schema.yaml""#);
        match s {
            SourceSchema::File(p) => assert_eq!(p, "schemas/base.schema.yaml"),
            other => panic!("expected File, got {other:?}"),
        }
    }

    #[test]
    fn decimal_token_is_a_real_type_with_scale() {
        // `decimal` is now a first-class exact type; `precision`/`scale` stay
        // column attributes (not type parameters).
        let ok: SourceSchema =
            from_json(r#"[{"name":"price","type":"decimal","precision":10,"scale":2}]"#);
        let c = &ok.as_columns().unwrap()[0];
        assert_eq!(c.ty, Type::Decimal);
        assert_eq!(c.precision, Some(10));
        assert_eq!(c.scale, Some(2));
    }

    #[test]
    fn unknown_column_key_rejected() {
        let err =
            serde_json::from_str::<SourceSchema>(r#"[{"name":"x","type":"int","colour":"red"}]"#);
        assert!(err.is_err(), "unknown column key must be rejected");
    }

    #[test]
    fn multi_record_missing_discriminator_rejected() {
        let err = serde_json::from_str::<SourceSchema>(
            r#"{"records":[{"id":"a","tag":"A","columns":[{"name":"x","type":"string"}]}]}"#,
        );
        assert!(
            err.is_err(),
            "multi-record without discriminator must be rejected"
        );
    }
}
