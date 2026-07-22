//! Channel-driven source-config patches.
//!
//! A channel can override any part of a source node's parsed config through
//! partial, path-addressed patches applied to the typed [`PipelineConfig`]
//! *before* validation and compile. The patched config is exactly what
//! `validate_config` sees and what `compile()` consumes, so a run behaves as
//! if the operator had hand-edited the source YAML.
//!
//! The patch is applied by mutating the already-parsed typed config in place —
//! the span-aware `Spanned<PipelineNode>` structure is preserved, never
//! round-tripped through a span-losing intermediate value.
//!
//! Handled surfaces: the format-agnostic schema-shaping ops — the CXL-typed
//! column list (`schema`), multi-value fan-out and in-cell parsing
//! (`split_to_rows` / `split_values`), and scalar per-format input options
//! (`options`) — the format-structure ops for
//! X12 nested-envelope declarations (`group_section` / `set_section`) and HL7
//! composite-field splits (`split_fields`), and the multi-record flat-file ops
//! for discriminator-driven record types (`records` / `discriminator`).

use super::*;
use crate::config::composition::SchemaProvRecorder;
use crate::config::pipeline_node::{PipelineNode, SourceBody};
use crate::yaml::Spanned;
use clinker_format::{
    Column, Discriminator, EnvelopeFieldType, NestedEnvelopeSection, RecordType, SourceSchema,
    SplitToRows, SplitToRowsMode, SplitValues,
};
use clinker_record::schema_def::{Justify, TruncationPolicy};
use cxl::typecheck::Type;
use indexmap::IndexMap;
use serde::de;
use serde::{Deserialize, Serialize};

/// Per-source config patch carried by a channel.
///
/// Keyed forms only. `schema` ops are keyed by column name, `split_to_rows` /
/// `split_values` ops by field name, and `options` sets a scalar per-format
/// input option by its key. `Serialize` is derived for config-identity hashing only (folding the
/// applied patches into the pipeline `source_hash`); the wire deserialize form
/// is the channel YAML.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SourceConfigPatch {
    /// Column-name-keyed schema ops (`modify` / `rename` / `add` / `remove`).
    pub schema: IndexMap<String, SchemaColumnOp>,
    /// Field-keyed fan-out ops: set-by-field (add-or-modify) or `remove`.
    pub split_to_rows: IndexMap<String, SplitToRowsOp>,
    /// Field-keyed in-cell-parse ops: set-by-field (add-or-modify) or `remove`.
    pub split_values: IndexMap<String, SplitValuesOp>,
    /// Scalar per-format input options, keyed by option name. Merged onto the
    /// source's current options and re-validated through the format's option
    /// struct, so an unknown key is rejected exactly as in hand-written config.
    pub options: IndexMap<String, serde_json::Value>,
    /// Op on the X12 `GS` functional-group nested-envelope declaration
    /// (`set`/modify/`remove`). Applies only to an `x12` source. Applied after
    /// the `options` merge, so a keyed op layers on top of an `options`-blob
    /// replacement of the same declaration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_section: Option<NestedSectionOp>,
    /// Op on the X12 `ST` transaction-set nested-envelope declaration,
    /// mirroring `group_section`.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub set_section: Option<NestedSectionOp>,
    /// HL7 composite-field split ops keyed by positional field name (`f08`):
    /// set-by-key (add-or-modify) or `remove`. Applies only to an `hl7`
    /// source, after the `options` merge.
    #[serde(skip_serializing_if = "IndexMap::is_empty")]
    pub split_fields: IndexMap<String, SplitFieldOp>,
    /// Multi-record `records` ops keyed by record-type id (`modify` / `add` /
    /// `remove`). Applies only to a multi-record (discriminator-driven) schema.
    #[serde(skip_serializing_if = "IndexMap::is_empty")]
    pub records: IndexMap<String, RecordTypeOp>,
    /// Partial merge onto a multi-record source's `discriminator` block. Every
    /// field is optional; a named field overwrites, an omitted one is kept, and
    /// the merged result must be byte-range XOR field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discriminator: Option<DiscriminatorPatch>,
}

impl SourceConfigPatch {
    /// Whether this patch carries no ops — a no-op the apply pass can skip.
    pub fn is_empty(&self) -> bool {
        self.schema.is_empty()
            && self.split_to_rows.is_empty()
            && self.split_values.is_empty()
            && self.options.is_empty()
            && self.group_section.is_none()
            && self.set_section.is_none()
            && self.split_fields.is_empty()
            && self.records.is_empty()
            && self.discriminator.is_none()
    }
}

/// A partial [`Column`]: any subset of a column's attributes, all optional.
///
/// The payload of a `modify` (bare-attribute map) or an `add` op. Every setter
/// is `Option<_>` so a modify sets exactly the attributes it names and keeps the
/// rest of the base column; `deny_unknown_fields` turns a typo into an error
/// rather than a silent no-op. `Serialize` is for config-identity hashing only.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct ColumnPatch {
    /// CXL type. Required for `add`; on `modify`, retypes the column.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub ty: Option<Type>,
    /// Physical source column read FROM (see [`Column::source_name`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    /// Fixed-width start offset.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<usize>,
    /// Fixed-width field width.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<usize>,
    /// Fixed-width end offset (exclusive).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end: Option<usize>,
    /// XML path (inert consumer today).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Fixed-width write justification.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub justify: Option<Justify>,
    /// Fixed-width pad character.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pad: Option<String>,
    /// Fixed-width read trim.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trim: Option<bool>,
    /// Fixed-width write truncation policy.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation: Option<TruncationPolicy>,
    /// strftime pattern for date/datetime parse.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Decimal precision.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub precision: Option<u8>,
    /// Decimal scale.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scale: Option<u8>,
    /// Whether the column must be present/non-null.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
    /// Default value when absent.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<serde_json::Value>,
    /// Whether to coerce the raw value to the declared type.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coerce: Option<bool>,
    /// Allowed value set (enum constraint).
    #[serde(rename = "enum", skip_serializing_if = "Option::is_none")]
    pub allowed_values: Option<Vec<String>>,
    /// Advisory storage hint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub long_unique: Option<bool>,
    /// Whether the column holds more than one value. `multiple: false` clears
    /// the declaration on a column that had it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multiple: Option<bool>,
}

impl ColumnPatch {
    /// Whether the patch names no attribute at all (an empty `{}` modify).
    fn is_empty(&self) -> bool {
        *self == ColumnPatch::default()
    }
}

/// One column-keyed schema op — the keyed-map grammar shared by every override
/// layer (pipeline / group / channel) and the channel `sources:` patch.
///
/// YAML forms (the map key is the target column name):
///
/// ```yaml
/// amount:      { type: float, scale: 2 }    # modify: set any subset of attrs
/// cust_id:     { rename: customer_id }      # rename (physical alias preserved)
/// order_notes: remove                       # drop the column
/// region:      { add: { type: string } }    # add a new column (key = new name)
/// ```
///
/// The modify leaf sets *any* subset of a [`Column`]'s attributes, leaf-replace
/// per attribute onto the base column. `Serialize` is derived for config-identity
/// hashing only; the deserialize form is the channel YAML above.
#[derive(Debug, Clone, Serialize)]
pub enum SchemaColumnOp {
    /// Drop the keyed column.
    Remove,
    /// Rename the keyed column to the given name.
    Rename(String),
    /// Set a subset of the keyed column's attributes.
    Modify(ColumnPatch),
    /// Add a new column named by the map key.
    Add(ColumnPatch),
}

impl<'de> Deserialize<'de> for SchemaColumnOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Accepted YAML shapes:
        //   remove                          (bare scalar)
        //   { rename: <name> }              (rename op)
        //   { add: { <attrs> } }            (add op)
        //   { <attrs> }                     (modify op — bare column attributes)
        // `rename` and `add` are op selectors; every other key is a `Column`
        // attribute that belongs to the modify leaf. Mixing a selector with
        // modify attributes (or with each other) is rejected.
        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields, default)]
        struct OpMap {
            rename: Option<String>,
            add: Option<ColumnPatch>,
            // Modify leaf: the same attribute set as `ColumnPatch`, inlined so
            // `deny_unknown_fields` still rejects a typo (a `#[serde(flatten)]`
            // would disable that check and drop saphyr spans).
            #[serde(rename = "type")]
            ty: Option<Type>,
            source_name: Option<String>,
            start: Option<usize>,
            width: Option<usize>,
            end: Option<usize>,
            path: Option<String>,
            justify: Option<Justify>,
            pad: Option<String>,
            trim: Option<bool>,
            truncation: Option<TruncationPolicy>,
            format: Option<String>,
            precision: Option<u8>,
            scale: Option<u8>,
            required: Option<bool>,
            default: Option<serde_json::Value>,
            coerce: Option<bool>,
            #[serde(rename = "enum")]
            allowed_values: Option<Vec<String>>,
            long_unique: Option<bool>,
            multiple: Option<bool>,
        }

        impl OpMap {
            fn modify_leaf(self) -> ColumnPatch {
                ColumnPatch {
                    ty: self.ty,
                    source_name: self.source_name,
                    start: self.start,
                    width: self.width,
                    end: self.end,
                    path: self.path,
                    justify: self.justify,
                    pad: self.pad,
                    trim: self.trim,
                    truncation: self.truncation,
                    format: self.format,
                    precision: self.precision,
                    scale: self.scale,
                    required: self.required,
                    default: self.default,
                    coerce: self.coerce,
                    allowed_values: self.allowed_values,
                    long_unique: self.long_unique,
                    multiple: self.multiple,
                }
            }
        }

        // `OpMap` is boxed in the map arm: it carries the full column-attribute
        // set, so an unboxed variant would leave `Either` lopsided.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Scalar(String),
            Map(Box<OpMap>),
        }

        match Either::deserialize(d)? {
            Either::Scalar(s) if s == "remove" => Ok(SchemaColumnOp::Remove),
            Either::Scalar(other) => Err(de::Error::custom(format!(
                "unknown schema op {other:?}; the bare scalar form only accepts `remove` — \
                 use `{{ rename: <name> }}`, `{{ add: {{ type: <type> }} }}`, or a bare \
                 attribute map like `{{ type: <type>, scale: <n> }}` to modify"
            ))),
            Either::Map(m) => {
                let m = *m;
                let has_rename = m.rename.is_some();
                let has_add = m.add.is_some();
                let rename = m.rename.clone();
                let add = m.add.clone();
                let modify = m.modify_leaf();
                let has_modify = !modify.is_empty();
                match (has_rename, has_add, has_modify) {
                    (true, false, false) => {
                        Ok(SchemaColumnOp::Rename(rename.expect("rename present")))
                    }
                    (false, true, false) => Ok(SchemaColumnOp::Add(add.expect("add present"))),
                    (false, false, true) => Ok(SchemaColumnOp::Modify(modify)),
                    (false, false, false) => Err(de::Error::custom(
                        "empty schema op; use `remove`, `{ rename: <name> }`, \
                         `{ add: { type: <type> } }`, or a bare attribute map to modify",
                    )),
                    _ => Err(de::Error::custom(
                        "ambiguous schema op; `rename`, `add`, and a bare-attribute modify are \
                         mutually exclusive — use exactly one",
                    )),
                }
            }
        }
    }
}

/// One field-keyed `split_to_rows` op.
///
/// YAML forms (the map key is the field name):
///
/// ```yaml
/// line_items: { mode: split, position_column: line_no }  # add-or-modify
/// tags:       { position_column: ~ }                     # clear one attribute
/// items:      remove                                     # drop the entry
/// ```
///
/// `Serialize` is derived for config-identity hashing only.
#[derive(Debug, Clone, Serialize)]
pub enum SplitToRowsOp {
    /// Drop the keyed fan-out entry.
    Remove,
    /// Add-or-modify the keyed fan-out entry. Each attribute is optional so a
    /// partial modify keeps the omitted ones: on an **existing** entry an
    /// omitted attribute retains its current value; on a **new** entry an
    /// omitted attribute takes the same default hand-written config would.
    Set {
        /// `None` = keep current (existing) / `true` (new).
        keep_empty: Option<bool>,
        /// `None` = keep current (existing) / `extract` (new).
        mode: Option<SplitToRowsMode>,
        /// Three-state, so an attribute can be CLEARED rather than only
        /// overwritten: `None` = key omitted, keep current; `Some(None)` = an
        /// explicit YAML null (`~`), clear it; `Some(Some(v))` = set it.
        position_column: Option<Option<String>>,
    },
}

/// Dispatches on the YAML node kind rather than routing through
/// `#[serde(untagged)]`. An untagged enum buffers both variants and reports
/// only `data did not match any variant`, which names neither the offending key
/// nor the accepted shapes; dispatching keeps `SetMap`'s own
/// `deny_unknown_fields` message (which names the bad key and the valid ones)
/// intact, and a node that is neither shape gets a message naming both forms.
impl<'de> Deserialize<'de> for SplitToRowsOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Two accepted YAML shapes:
        //   remove                                            (bare scalar)
        //   { keep_empty?, mode?, position_column? }           (entry map)
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SetMap {
            #[serde(default)]
            keep_empty: Option<bool>,
            #[serde(default)]
            mode: Option<SplitToRowsMode>,
            #[serde(default, deserialize_with = "explicit_option")]
            position_column: Option<Option<String>>,
        }

        struct V;
        impl<'de> de::Visitor<'de> for V {
            type Value = SplitToRowsOp;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a `split_to_rows` op: either `remove` or a map \
                     `{ keep_empty: <bool>, mode: extract|split, position_column: <name> }` \
                     (`position_column: ~` clears it)",
                )
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                if v == "remove" {
                    return Ok(SplitToRowsOp::Remove);
                }
                Err(de::Error::custom(format!(
                    "unknown split_to_rows op {v:?}; the bare scalar form only accepts `remove` \
                     — use `{{ keep_empty: <bool>, mode: extract|split, position_column: \
                     <name> }}` to add or modify an entry, with `position_column: ~` to clear it"
                )))
            }

            fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                let m = SetMap::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(SplitToRowsOp::Set {
                    keep_empty: m.keep_empty,
                    mode: m.mode,
                    position_column: m.position_column,
                })
            }
        }

        d.deserialize_any(V)
    }
}

/// One field-keyed `split_values` op.
///
/// YAML forms (the map key is the field name):
///
/// ```yaml
/// tags:  { delimiter: "|" }   # add-or-modify the entry
/// codes: { delimiter: ~ }     # reset the delimiter to the default
/// codes: remove               # drop the entry
/// ```
///
/// `Serialize` is derived for config-identity hashing only.
#[derive(Debug, Clone, Serialize)]
pub enum SplitValuesOp {
    /// Drop the keyed in-cell-parse entry.
    Remove,
    /// Add-or-modify the keyed entry.
    Set {
        /// Three-state, matching [`SplitToRowsOp::Set::position_column`]:
        /// `None` = key omitted, keep current; `Some(None)` = an explicit YAML
        /// null (`~`), reset to the default delimiter; `Some(Some(v))` = set
        /// it. The attribute has no unset state — it always holds a separator —
        /// so the clear form restores the default rather than removing it.
        delimiter: Option<Option<String>>,
    },
}

/// Same node-kind dispatch and rationale as [`SplitToRowsOp`]'s.
impl<'de> Deserialize<'de> for SplitValuesOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Two accepted YAML shapes:
        //   remove             (bare scalar)
        //   { delimiter? }     (entry map)
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SetMap {
            #[serde(default, deserialize_with = "explicit_option")]
            delimiter: Option<Option<String>>,
        }

        struct V;
        impl<'de> de::Visitor<'de> for V {
            type Value = SplitValuesOp;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a `split_values` op: either `remove` or a map `{ delimiter: <str> }` \
                     (`delimiter: ~` resets it to the default)",
                )
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                if v == "remove" {
                    return Ok(SplitValuesOp::Remove);
                }
                Err(de::Error::custom(format!(
                    "unknown split_values op {v:?}; the bare scalar form only accepts `remove` \
                     — use `{{ delimiter: <str> }}` to add or modify an entry, with \
                     `delimiter: ~` to reset it to the default"
                )))
            }

            fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                let m = SetMap::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(SplitValuesOp::Set {
                    delimiter: m.delimiter,
                })
            }
        }

        d.deserialize_any(V)
    }
}

/// Deserialize an optional field so an explicit YAML null is distinguishable
/// from an absent key: `None` when the key is absent (serde supplies the
/// `#[serde(default)]`), `Some(None)` for an explicit `~` / `null`, and
/// `Some(Some(v))` for a value.
///
/// The patch grammar is otherwise "an omitted field keeps the current value",
/// which leaves no way to clear an attribute once set. This is the escape
/// hatch, and it reuses YAML's own null rather than inventing a sentinel
/// string that a legitimate value could collide with.
fn explicit_option<'de, D, T>(d: D) -> Result<Option<Option<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(d).map(Some)
}

/// One op on an X12 nested-envelope declaration (`group_section` /
/// `set_section`) — a whole [`NestedEnvelopeSection`], not a keyed map,
/// because each X12 source carries at most one declaration per level.
///
/// YAML forms:
///
/// ```yaml
/// group_section: remove                       # drop the declaration
/// group_section:                              # set / modify
///   name: fg                                  # optional on an existing declaration
///   fields:
///     e06: string                             # set/add a typed field
///     e05: remove                             # drop a declared field
/// ```
///
/// On an **existing** declaration an omitted `name` keeps the current one and
/// each field op applies individually; on an **absent** declaration the op
/// creates it, so `name` is required and a field `remove` is an error.
/// `Serialize` is derived for config-identity hashing only.
#[derive(Debug, Clone, Serialize)]
pub enum NestedSectionOp {
    /// Drop the declaration entirely, reverting the level to its default
    /// positional section.
    Remove,
    /// Create the declaration or modify the existing one.
    Set {
        /// Section name `$doc.<name>.<field>` resolves under. `None` = keep
        /// current (existing declaration only).
        name: Option<String>,
        /// Per-field ops, keyed by positional element name (`e06`).
        fields: IndexMap<String, EnvelopeFieldOp>,
    },
}

impl<'de> Deserialize<'de> for NestedSectionOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Two accepted YAML shapes:
        //   remove                       (bare scalar)
        //   { name?, fields? }           (declaration map)
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SetMap {
            #[serde(default)]
            name: Option<String>,
            #[serde(default)]
            fields: IndexMap<String, EnvelopeFieldOp>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Scalar(String),
            Map(SetMap),
        }

        match Either::deserialize(d)? {
            Either::Scalar(s) if s == "remove" => Ok(NestedSectionOp::Remove),
            Either::Scalar(other) => Err(de::Error::custom(format!(
                "unknown nested-section op {other:?}; the bare scalar form only accepts \
                 `remove` — use `{{ name: <section>, fields: {{ e06: string }} }}` to set or \
                 modify the declaration"
            ))),
            Either::Map(m) => Ok(NestedSectionOp::Set {
                name: m.name,
                fields: m.fields,
            }),
        }
    }
}

/// One per-field op inside a [`NestedSectionOp::Set`]'s `fields` map: set the
/// field's envelope type, or drop the field from the declaration.
/// `Serialize` is derived for config-identity hashing only.
#[derive(Debug, Clone, Copy, Serialize)]
pub enum EnvelopeFieldOp {
    /// Drop the keyed field from the declaration.
    Remove,
    /// Set (add-or-replace) the keyed field's envelope type.
    Set(EnvelopeFieldType),
}

impl<'de> Deserialize<'de> for EnvelopeFieldOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // A bare scalar: `remove`, or an envelope field type name.
        let s = String::deserialize(d)?;
        if s == "remove" {
            return Ok(EnvelopeFieldOp::Remove);
        }
        EnvelopeFieldType::deserialize(de::value::StrDeserializer::<D::Error>::new(&s))
            .map(EnvelopeFieldOp::Set)
            .map_err(|_| {
                de::Error::custom(format!(
                    "unknown envelope field op {s:?}; use a field type (`string`, `int`, \
                     `float`, `bool`, `date`, `date_time`) to set the field, or `remove` to \
                     drop it"
                ))
            })
    }
}

/// One HL7 composite-field split op, keyed by positional field name (`f08`).
///
/// YAML forms (the map key is the field name):
///
/// ```yaml
/// f08: { components: 3 }                     # add-or-modify a split
/// f03: remove                                # drop a declared split
/// ```
///
/// The key resolves by wire position, so `f8` and `f08` address the same
/// split. `Serialize` is derived for config-identity hashing only.
#[derive(Debug, Clone, Serialize)]
pub enum SplitFieldOp {
    /// Drop the keyed split declaration, restoring the verbatim `fNN` column.
    Remove,
    /// Add-or-modify the keyed split. Each axis is optional so a partial
    /// modify keeps the omitted axis: on an **existing** split an omitted
    /// axis retains its current width; on a **new** split `components` is
    /// required and an omitted `subcomponents`/`repetitions` defaults to 1,
    /// exactly as in hand-written config.
    Set {
        /// Component columns (the `^` axis); `None` = keep current /
        /// required (new).
        components: Option<usize>,
        /// Sub-component columns per component (the `&` axis); `None` =
        /// keep current / 1 (new).
        subcomponents: Option<usize>,
        /// Repetition columns (the `~` axis); `None` = keep current / 1
        /// (new).
        repetitions: Option<usize>,
    },
}

impl<'de> Deserialize<'de> for SplitFieldOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Two accepted YAML shapes:
        //   remove                                     (bare scalar)
        //   { components?, subcomponents?, repetitions? }   (split map)
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SetMap {
            #[serde(default)]
            components: Option<usize>,
            #[serde(default)]
            subcomponents: Option<usize>,
            #[serde(default)]
            repetitions: Option<usize>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Scalar(String),
            Map(SetMap),
        }

        match Either::deserialize(d)? {
            Either::Scalar(s) if s == "remove" => Ok(SplitFieldOp::Remove),
            Either::Scalar(other) => Err(de::Error::custom(format!(
                "unknown split_fields op {other:?}; the bare scalar form only accepts `remove` \
                 — use `{{ components: <n>, subcomponents: <n>, repetitions: <n> }}` to add or \
                 modify a split"
            ))),
            Either::Map(m) => Ok(SplitFieldOp::Set {
                components: m.components,
                subcomponents: m.subcomponents,
                repetitions: m.repetitions,
            }),
        }
    }
}

/// One op on a multi-record source's `records` list, keyed by record-type id.
///
/// YAML forms (the map key is the record-type id):
///
/// ```yaml
/// detail:  { tag: X, columns: { amount: { type: float } } }  # modify
/// trailer: remove                                             # drop the type
/// header:  { add: { tag: H, columns: [ { name: id, type: string } ] } }  # add
/// ```
///
/// A `modify` sets any subset of the record type's `tag` / `parent` /
/// `join_key` / `description`, plus a nested column-keyed op map that reshapes
/// that type's own columns through the same grammar as the top-level `schema`
/// surface. An `add` declares a whole new record type (its id is the map key).
/// `Serialize` is derived for config-identity hashing only.
#[derive(Debug, Clone, Serialize)]
pub enum RecordTypeOp {
    /// Drop the keyed record type.
    Remove,
    /// Set a subset of the keyed record type's attributes and reshape its
    /// columns.
    Modify(RecordTypePatch),
    /// Declare a new record type named by the map key.
    Add(RecordTypeAdd),
}

/// A partial [`RecordType`]: any subset of a record type's attributes plus a
/// nested column-op map. The payload of a [`RecordTypeOp::Modify`]. Each scalar
/// is `Option<_>` so a modify sets only the attributes it names and keeps the
/// rest; `columns` carries the same keyed column-op grammar as the top-level
/// `schema` surface. `Serialize` is for config-identity hashing only.
#[derive(Debug, Clone, Default, Serialize)]
pub struct RecordTypePatch {
    /// Discriminator tag the reader matches this type's lines on. `None` keeps
    /// the current tag.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    /// Parent record-type id (field inheritance). `None` keeps the current.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Join key field. `None` keeps the current.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub join_key: Option<String>,
    /// Human description. `None` keeps the current.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Column-name-keyed ops reshaping this record type's own columns.
    #[serde(skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, SchemaColumnOp>,
}

/// The payload of a [`RecordTypeOp::Add`]: a whole new record type minus its id
/// (which is the map key). `tag` and `columns` are required, matching a
/// hand-written `records:` entry; the rest are optional. `deny_unknown_fields`
/// turns a typo into an error. `Serialize` is for config-identity hashing only.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RecordTypeAdd {
    /// Discriminator tag the reader matches this record type's lines on.
    pub tag: String,
    /// Human description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Parent record-type id (field inheritance).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<String>,
    /// Join key field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub join_key: Option<String>,
    /// The record type's full column list.
    pub columns: Vec<Column>,
}

impl<'de> Deserialize<'de> for RecordTypeOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Accepted YAML shapes:
        //   remove                                              (bare scalar)
        //   { add: { tag, columns, ... } }                      (add op)
        //   { tag?, parent?, join_key?, description?, columns? } (modify op)
        // `add` is the op selector; every other key belongs to the modify leaf.
        // Mixing `add` with modify attributes is rejected.
        #[derive(Deserialize, Default)]
        #[serde(deny_unknown_fields, default)]
        struct OpMap {
            add: Option<RecordTypeAdd>,
            // Modify leaf: inlined so `deny_unknown_fields` still rejects a typo
            // (a `#[serde(flatten)]` would disable that check and drop spans).
            tag: Option<String>,
            parent: Option<String>,
            join_key: Option<String>,
            description: Option<String>,
            columns: IndexMap<String, SchemaColumnOp>,
        }

        impl OpMap {
            fn has_modify(&self) -> bool {
                self.tag.is_some()
                    || self.parent.is_some()
                    || self.join_key.is_some()
                    || self.description.is_some()
                    || !self.columns.is_empty()
            }
            fn modify_leaf(self) -> RecordTypePatch {
                RecordTypePatch {
                    tag: self.tag,
                    parent: self.parent,
                    join_key: self.join_key,
                    description: self.description,
                    columns: self.columns,
                }
            }
        }

        // `OpMap` is boxed in the map arm: it carries a full column list, so an
        // unboxed variant would leave `Either` lopsided.
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Scalar(String),
            Map(Box<OpMap>),
        }

        match Either::deserialize(d)? {
            Either::Scalar(s) if s == "remove" => Ok(RecordTypeOp::Remove),
            Either::Scalar(other) => Err(de::Error::custom(format!(
                "unknown records op {other:?}; the bare scalar form only accepts `remove` — use \
                 `{{ add: {{ tag: <tag>, columns: [ ... ] }} }}` to add a record type, or a bare \
                 attribute map like `{{ tag: <tag>, columns: {{ ... }} }}` to modify"
            ))),
            Either::Map(m) => {
                let m = *m;
                let has_add = m.add.is_some();
                let has_modify = m.has_modify();
                match (has_add, has_modify) {
                    (true, false) => Ok(RecordTypeOp::Add(m.add.expect("add present"))),
                    (false, true) => Ok(RecordTypeOp::Modify(m.modify_leaf())),
                    (false, false) => Err(de::Error::custom(
                        "empty records op; use `remove`, `{ add: { tag: <tag>, columns: [ ... ] } }`, \
                         or a bare attribute map (`tag` / `parent` / `join_key` / `description` / \
                         `columns`) to modify",
                    )),
                    (true, true) => Err(de::Error::custom(
                        "ambiguous records op; `add` and a bare-attribute modify are mutually \
                         exclusive — use exactly one",
                    )),
                }
            }
        }
    }
}

/// A partial [`Discriminator`]: a channel's merge onto a multi-record source's
/// discriminator. Every field is optional and overwrites the current when
/// named; an omitted field is kept. The merged result must be byte-range XOR
/// field (E244). `deny_unknown_fields` turns a typo into an error; `Serialize`
/// is for config-identity hashing only.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields, default)]
pub struct DiscriminatorPatch {
    /// Byte-range start offset (fixed-width discrimination).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start: Option<usize>,
    /// Byte-range width (fixed-width discrimination).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<usize>,
    /// Discriminator field name (CSV discrimination).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// Deferred channel `sources:` patches addressed at a source declared inside a
/// composition body, keyed `composition-node-name -> inner-source-name ->
/// patch`. Mirrors [`ConfigOverrides`](crate::config::ConfigOverrides).
///
/// A qualified `<composition>.<source>` patch key cannot be applied at patch
/// time: the composition body is expanded only during compile, so its source
/// nodes are not in `config.nodes` yet. [`apply_source_patches`] validates the
/// composition alias and stashes the patch here; `bind_composition` applies it
/// to the freshly re-read body before the body binds.
pub type BodySourcePatchMap = IndexMap<String, IndexMap<String, SourceConfigPatch>>;

/// Apply channel source-config patches to the parsed config in place.
///
/// Runs after parse and before `validate_config`, so the patched config is
/// the one validated and compiled. A plain key names a top-level source node; a
/// qualified `<composition>.<source>` key names a source inside that
/// composition's body and is deferred to compile (see [`BodySourcePatchMap`]).
/// An unknown target or an ill-formed op is a config error (E230–E245) reported
/// before the pipeline compiles.
pub fn apply_source_patches(
    config: &mut PipelineConfig,
    patches: &IndexMap<String, SourceConfigPatch>,
) -> Result<(), ConfigError> {
    // Whether the pipeline calls any composition. A plain patch key that names
    // no top-level source gets a hint pointing at the qualified-key form when
    // the pipeline has compositions, rather than implying a bare typo. Computed
    // once so the per-source lookup can stay a plain borrow.
    let has_composition = config
        .nodes
        .iter()
        .any(|n| matches!(n.value, PipelineNode::Composition { .. }));

    for (key, patch) in patches {
        // A qualified `<composition>.<source>` key targets a source inside a
        // composition body. That body is expanded only during compile, so the
        // target cannot resolve against `config.nodes` here; validate the
        // composition alias now and defer the patch to `bind_composition`,
        // which applies it after re-reading the body.
        if let Some((alias, inner)) = split_qualified_key(key)? {
            if !is_composition_node(config, alias) {
                return Err(unknown_composition_alias(key, alias));
            }
            config
                .body_source_patches
                .entry(alias.to_string())
                .or_default()
                .insert(inner.to_string(), patch.clone());
            continue;
        }

        if patch.is_empty() {
            // Still resolve the source so an empty patch on a typo'd name
            // fails rather than passing silently.
            source_body_mut(config, key).ok_or_else(|| unknown_source(key, has_composition))?;
            continue;
        }
        let body =
            source_body_mut(config, key).ok_or_else(|| unknown_source(key, has_composition))?;
        apply_patch_to_source_body(body, patch, key)?;
    }
    Ok(())
}

/// Apply every op carried by `patch` to one source's parsed body in place.
///
/// The shared core of the top-level channel `sources:` pass and the deferred
/// composition-body pass: both resolve a source node — top-level or inside an
/// expanded body — to its [`SourceBody`] and layer the identical op grammar
/// over it, so a body source patches byte-for-byte the way a top-level one
/// does. `src` names the source for the op diagnostics.
///
/// Format-structure ops apply after the `options` merge so a keyed op layers
/// deterministically on top of an `options`-blob replacement of the same
/// declaration in the same patch.
pub(crate) fn apply_patch_to_source_body(
    body: &mut SourceBody,
    patch: &SourceConfigPatch,
    src: &str,
) -> Result<(), ConfigError> {
    apply_schema_ops(&mut body.schema, &patch.schema, src, None)?;
    apply_split_to_rows_ops(&mut body.source, &patch.split_to_rows, src)?;
    apply_split_values_ops(&mut body.source, &patch.split_values, src)?;
    apply_option_ops(&mut body.source, &patch.options, src)?;
    apply_nested_section_op(
        &mut body.source,
        patch.group_section.as_ref(),
        X12SectionSlot::Group,
        src,
    )?;
    apply_nested_section_op(
        &mut body.source,
        patch.set_section.as_ref(),
        X12SectionSlot::Set,
        src,
    )?;
    apply_split_field_ops(&mut body.source, &patch.split_fields, src)?;
    apply_record_ops(
        &mut body.schema,
        &patch.records,
        patch.discriminator.as_ref(),
        src,
    )?;
    Ok(())
}

/// Split a channel `sources:` key into an optional `(composition, source)`
/// pair. Node names cannot contain dots, so a single dot marks a
/// composition-body target and no dot marks a plain top-level source.
///
/// - `Ok(None)` — no dot: a plain top-level source key.
/// - `Ok(Some((composition, source)))` — exactly one dot.
/// - `Err(_)` — an empty segment, or more than one dot (a source inside a
///   nested composition body, which is not addressable).
fn split_qualified_key(key: &str) -> Result<Option<(&str, &str)>, ConfigError> {
    match key.split_once('.') {
        None => Ok(None),
        Some((alias, rest)) => {
            if rest.contains('.') {
                return Err(nested_body_source(key));
            }
            if alias.is_empty() || rest.is_empty() {
                return Err(malformed_qualified_key(key));
            }
            Ok(Some((alias, rest)))
        }
    }
}

/// Whether a composition call-site node by this name exists in the pipeline.
fn is_composition_node(config: &PipelineConfig, name: &str) -> bool {
    config.nodes.iter().any(|spanned| {
        matches!(&spanned.value, PipelineNode::Composition { header, .. } if header.name == name)
    })
}

fn unknown_composition_alias(key: &str, alias: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E230] channel source patch key '{key}' targets a source in composition '{alias}', \
         but no composition node by that name exists in the pipeline"
    ))
}

fn nested_body_source(key: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E230] channel source patch key '{key}': a source inside a nested composition body is \
         not addressable; use '<composition>.<source>' to patch a source one level inside a \
         composition body"
    ))
}

fn malformed_qualified_key(key: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E230] channel source patch key '{key}' is malformed; use '<source>' for a top-level \
         source or '<composition>.<source>' for a source inside a composition body"
    ))
}

/// Split a `[CODE] message` diagnostic string into its bracketed E-code and the
/// remaining message. Channel-patch [`ConfigError`]s embed their stable E-code
/// as a `[E2xx]` prefix; the composition-body patch pass re-emits an op failure
/// as a structured `Diagnostic` and lifts the code back out here, so a
/// body-source op error carries the same code a top-level one would. Falls back
/// to the generic source-patch code when no prefix is present.
pub(crate) fn split_diagnostic_code(msg: &str) -> (&str, &str) {
    match msg.strip_prefix('[').and_then(|rest| rest.split_once(']')) {
        Some((code, tail)) => (code, tail.trim_start()),
        None => ("E230", msg),
    }
}

/// Locate a source node's body by its node identity (`header.name`) in a node
/// list. Shared by the top-level patch pass and the composition-body pass.
///
/// The channel system keys sources by node identity — the `name:` on the node
/// header — which can differ from the nested `config.name:`. Every other
/// channel block (`vars.source`, E111) resolves the same way, so the patch
/// target resolves by `header.name` for consistency.
pub(crate) fn source_body_in_nodes<'a>(
    nodes: &'a mut [Spanned<PipelineNode>],
    name: &str,
) -> Option<&'a mut SourceBody> {
    nodes
        .iter_mut()
        .find_map(|spanned| match &mut spanned.value {
            PipelineNode::Source {
                header,
                config: body,
            } if header.name == name => Some(body),
            _ => None,
        })
}

fn source_body_mut<'a>(config: &'a mut PipelineConfig, name: &str) -> Option<&'a mut SourceBody> {
    source_body_in_nodes(&mut config.nodes, name)
}

fn unknown_source(src_name: &str, has_composition: bool) -> ConfigError {
    let hint = if has_composition {
        " (to patch a source declared inside a composition body, qualify the key as \
         `<composition-node>.<source>`)"
    } else {
        ""
    };
    ConfigError::Validation(format!(
        "[E230] channel source patch targets unknown source '{src_name}': \
         no source node by that name in the pipeline{hint}"
    ))
}

fn unknown_column(src: &str, col: &str, op: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E231] channel schema patch on source '{src}': {op} of unknown column '{col}'"
    ))
}

/// Apply column-keyed schema ops to a source's declared schema in place,
/// optionally recording per-attribute provenance at the merge.
///
/// Shared by the channel source-config patch pass ([`apply_source_patches`])
/// and the overlay op engine's `patch_schema` op, so both surfaces resolve the
/// keyed-map grammar (`modify` / `rename` / `add` / `remove`) and its E231–E237
/// diagnostics identically. `src` names the source for the diagnostic text.
///
/// When `recorder` is `Some`, every attribute an op sets is recorded into the
/// shared schema-provenance table at the recorder's layer, in the same pass that
/// mutates the schema — so a resolved value and its layer attribution can never
/// drift. The channel `sources:` surface passes `None` (it folds into the Base
/// seed); the overlay op stream passes a per-op recorder tagged with its layer.
pub(crate) fn apply_schema_ops(
    schema: &mut SourceSchema,
    ops: &IndexMap<String, SchemaColumnOp>,
    src: &str,
    recorder: Option<&mut SchemaProvRecorder<'_>>,
) -> Result<(), ConfigError> {
    if ops.is_empty() {
        return Ok(());
    }
    // The keyed column ops target a single-record column list. A multi-record,
    // generated, or external-file schema has no flat column list to patch here.
    let columns = schema.as_columns_mut().ok_or_else(|| {
        ConfigError::Validation(format!(
            "[E237] channel schema patch on source '{src}': column ops apply only to a \
             single-record (column-list) schema, not a multi-record / generated / file schema"
        ))
    })?;
    apply_column_ops(columns, ops, src, recorder)
}

/// Apply the keyed column-op grammar (`modify` / `rename` / `add` / `remove`)
/// to a flat column list in place — the core shared by the single-record
/// `schema` surface ([`apply_schema_ops`], which wraps this with the E237
/// schema-kind guard) and the per-record-type column ops of a multi-record
/// `records` modify. `src` names the source for diagnostics; `recorder` records
/// per-attribute provenance when present.
fn apply_column_ops(
    columns: &mut Vec<Column>,
    ops: &IndexMap<String, SchemaColumnOp>,
    src: &str,
    mut recorder: Option<&mut SchemaProvRecorder<'_>>,
) -> Result<(), ConfigError> {
    for (col, op) in ops {
        match op {
            SchemaColumnOp::Remove => {
                let idx = columns
                    .iter()
                    .position(|c| c.name == *col)
                    .ok_or_else(|| unknown_column(src, col, "remove"))?;
                columns.remove(idx);
                if let Some(r) = recorder.as_deref_mut() {
                    r.remove_column(col);
                }
            }
            SchemaColumnOp::Modify(patch) => {
                let column = columns
                    .iter_mut()
                    .find(|c| c.name == *col)
                    .ok_or_else(|| unknown_column(src, col, "modify"))?;
                apply_column_patch(column, patch, col, recorder.as_deref_mut());
            }
            SchemaColumnOp::Rename(to) => {
                if !columns.iter().any(|c| c.name == *col) {
                    return Err(unknown_column(src, col, "rename"));
                }
                if to != col && columns.iter().any(|c| c.name == *to) {
                    return Err(ConfigError::Validation(format!(
                        "[E233] channel schema patch on source '{src}': rename of '{col}' to \
                         '{to}' collides with an existing column"
                    )));
                }
                let column = columns
                    .iter_mut()
                    .find(|c| c.name == *col)
                    .expect("existence checked above");
                // Rename is a physical→logical alias, not a bare relabel: the
                // reader binds columns to input fields by name, so preserve the
                // physical source column (defaulting to the current name on the
                // first rename) while exposing the new name downstream. A second
                // rename keeps the original physical binding.
                column.source_name = Some(
                    column
                        .source_name
                        .take()
                        .unwrap_or_else(|| column.name.clone()),
                );
                let physical = column.source_name.clone().expect("set above");
                column.name = to.clone();
                if let Some(r) = recorder.as_deref_mut() {
                    r.rename_column(col, to, &physical);
                }
            }
            SchemaColumnOp::Add(add) => {
                if columns.iter().any(|c| c.name == *col) {
                    return Err(ConfigError::Validation(format!(
                        "[E232] channel schema patch on source '{src}': add of column '{col}' \
                         that already exists"
                    )));
                }
                let ty = add.ty.clone().ok_or_else(|| {
                    ConfigError::Validation(format!(
                        "[E236] channel schema patch on source '{src}': add of column '{col}' \
                         requires a `type`"
                    ))
                })?;
                let mut column = Column::bare(col.clone(), ty);
                apply_column_patch(&mut column, add, col, None);
                if let Some(r) = recorder.as_deref_mut() {
                    r.add_column(&column);
                }
                columns.push(column);
            }
        }
    }
    Ok(())
}

/// Set each attribute a [`ColumnPatch`] names onto `column` (leaf-replace),
/// recording each set attribute into `rec` under `col_name` when present. The
/// attribute names match [`column_attrs`](crate::config::composition::column_attrs)
/// exactly, so a modify and the Base seed key the same leaves.
fn apply_column_patch(
    column: &mut Column,
    patch: &ColumnPatch,
    col_name: &str,
    mut rec: Option<&mut SchemaProvRecorder<'_>>,
) {
    if let Some(t) = &patch.ty {
        column.ty = t.clone();
        if let Some(r) = rec.as_deref_mut() {
            r.set_attr(col_name, "type", t);
        }
    }
    macro_rules! set {
        ($field:ident, $name:literal) => {
            if let Some(v) = &patch.$field {
                column.$field = Some(v.clone());
                if let Some(r) = rec.as_deref_mut() {
                    r.set_attr(col_name, $name, v);
                }
            }
        };
    }
    set!(source_name, "source_name");
    set!(start, "start");
    set!(width, "width");
    set!(end, "end");
    set!(path, "path");
    set!(justify, "justify");
    set!(pad, "pad");
    set!(trim, "trim");
    set!(truncation, "truncation");
    set!(format, "format");
    set!(precision, "precision");
    set!(scale, "scale");
    set!(required, "required");
    set!(default, "default");
    set!(coerce, "coerce");
    set!(allowed_values, "enum");
    set!(long_unique, "long_unique");
    set!(multiple, "multiple");
}

fn apply_split_to_rows_ops(
    source: &mut SourceConfig,
    ops: &IndexMap<String, SplitToRowsOp>,
    src: &str,
) -> Result<(), ConfigError> {
    for (field, op) in ops {
        match op {
            SplitToRowsOp::Remove => {
                let Some(entries) = source.split_to_rows.as_mut() else {
                    return Err(unknown_split_entry(src, "split_to_rows", field));
                };
                let Some(idx) = entries.iter().position(|e| e.field == *field) else {
                    return Err(unknown_split_entry(src, "split_to_rows", field));
                };
                entries.remove(idx);
            }
            SplitToRowsOp::Set {
                keep_empty,
                mode,
                position_column,
            } => {
                let entries = source.split_to_rows.get_or_insert_with(Vec::new);
                let entry = match entries.iter_mut().find(|e| e.field == *field) {
                    // Partial modify: an omitted attribute keeps the current
                    // value, so setting only `mode` on an entry does not
                    // silently revert its `keep_empty`.
                    Some(existing) => existing,
                    None => {
                        entries.push(SplitToRows::bare(field.clone()));
                        entries.last_mut().expect("just pushed")
                    }
                };
                if let Some(k) = keep_empty {
                    entry.keep_empty = *k;
                }
                if let Some(m) = mode {
                    entry.mode = *m;
                }
                // `Some(None)` is an explicit null — clear the attribute.
                if let Some(pc) = position_column {
                    entry.position_column = pc.clone();
                }
            }
        }
    }
    Ok(())
}

fn apply_split_values_ops(
    source: &mut SourceConfig,
    ops: &IndexMap<String, SplitValuesOp>,
    src: &str,
) -> Result<(), ConfigError> {
    for (field, op) in ops {
        match op {
            SplitValuesOp::Remove => {
                let Some(entries) = source.split_values.as_mut() else {
                    return Err(unknown_split_entry(src, "split_values", field));
                };
                let Some(idx) = entries.iter().position(|e| e.field == *field) else {
                    return Err(unknown_split_entry(src, "split_values", field));
                };
                entries.remove(idx);
            }
            SplitValuesOp::Set { delimiter } => {
                let entries = source.split_values.get_or_insert_with(Vec::new);
                let entry = match entries.iter_mut().find(|e| e.field == *field) {
                    Some(existing) => existing,
                    None => {
                        entries.push(SplitValues::bare(field.clone()));
                        entries.last_mut().expect("just pushed")
                    }
                };
                // `Some(None)` is an explicit null — a delimiter is never
                // unset, so clearing it restores the default separator.
                if let Some(d) = delimiter {
                    entry.delimiter = d
                        .clone()
                        .unwrap_or_else(|| clinker_format::DEFAULT_VALUE_DELIMITER.to_string());
                }
            }
        }
    }
    Ok(())
}

fn unknown_split_entry(src: &str, key: &str, field: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E234] channel {key} patch on source '{src}': remove of unknown field '{field}'"
    ))
}

/// Merge scalar option overrides onto the source's current per-format options.
///
/// Serializes the current options to a JSON object, sets each override key,
/// then re-deserializes through the format's option struct — reusing its
/// `deny_unknown_fields` so an unknown or mistyped option is rejected (E235)
/// exactly as it would be in hand-written config. The round-trip touches only
/// the leaf option struct, never the span-aware pipeline-node structure.
fn apply_option_ops(
    source: &mut SourceConfig,
    overrides: &IndexMap<String, serde_json::Value>,
    src: &str,
) -> Result<(), ConfigError> {
    if overrides.is_empty() {
        return Ok(());
    }
    let fmt = source.format.format_name();

    macro_rules! patch_options {
        ($cur:expr, $ty:ty, $wrap:path) => {{
            let mut value = match $cur {
                Some(existing) => {
                    serde_json::to_value(existing).map_err(|e| options_internal(src, &e))?
                }
                None => serde_json::Value::Object(serde_json::Map::new()),
            };
            let obj = value
                .as_object_mut()
                .expect("format input options serialize to a JSON object");
            for (key, val) in overrides {
                obj.insert(key.clone(), val.clone());
            }
            let parsed: $ty =
                serde_json::from_value(value).map_err(|e| options_rejected(src, fmt, &e))?;
            $wrap(Some(parsed))
        }};
    }

    let new_format = match &source.format {
        InputFormat::Csv(o) => patch_options!(o, CsvInputOptions, InputFormat::Csv),
        InputFormat::Json(o) => patch_options!(o, JsonInputOptions, InputFormat::Json),
        InputFormat::Xml(o) => patch_options!(o, XmlInputOptions, InputFormat::Xml),
        InputFormat::FixedWidth(o) => {
            patch_options!(o, FixedWidthInputOptions, InputFormat::FixedWidth)
        }
        InputFormat::Edifact(o) => patch_options!(o, EdifactInputOptions, InputFormat::Edifact),
        InputFormat::X12(o) => patch_options!(o, X12InputOptions, InputFormat::X12),
        InputFormat::Hl7(o) => patch_options!(o, Hl7InputOptions, InputFormat::Hl7),
        InputFormat::Swift(o) => patch_options!(o, SwiftInputOptions, InputFormat::Swift),
    };
    source.format = new_format;
    Ok(())
}

fn options_rejected(src: &str, fmt: &str, err: &serde_json::Error) -> ConfigError {
    ConfigError::Validation(format!(
        "[E235] channel options patch on source '{src}' ({fmt} format): {err}"
    ))
}

fn options_internal(src: &str, err: &serde_json::Error) -> ConfigError {
    ConfigError::Validation(format!(
        "[E235] channel options patch on source '{src}': could not read current options: {err}"
    ))
}

/// Which X12 nested-envelope declaration a [`NestedSectionOp`] targets.
#[derive(Clone, Copy)]
enum X12SectionSlot {
    /// The `GS` functional-group level (`group_section`).
    Group,
    /// The `ST` transaction-set level (`set_section`).
    Set,
}

impl X12SectionSlot {
    /// The patch key, used verbatim in diagnostics.
    fn key(self) -> &'static str {
        match self {
            X12SectionSlot::Group => "group_section",
            X12SectionSlot::Set => "set_section",
        }
    }

    /// The targeted declaration on the source's X12 options.
    fn slot(self, opts: &mut X12InputOptions) -> &mut Option<NestedEnvelopeSection> {
        match self {
            X12SectionSlot::Group => &mut opts.group_section,
            X12SectionSlot::Set => &mut opts.set_section,
        }
    }
}

/// Apply one `group_section` / `set_section` op to the source's X12 options
/// in place. A non-X12 source is rejected (E238); a `remove` of an absent
/// declaration or field is E239; creating a declaration without a `name` is
/// E240.
fn apply_nested_section_op(
    source: &mut SourceConfig,
    op: Option<&NestedSectionOp>,
    which: X12SectionSlot,
    src: &str,
) -> Result<(), ConfigError> {
    let Some(op) = op else {
        return Ok(());
    };
    let fmt = source.format.format_name();
    let InputFormat::X12(opts) = &mut source.format else {
        return Err(ConfigError::Validation(format!(
            "[E238] channel {key} patch on source '{src}': X12 nested-envelope section ops \
             apply only to an `x12` source (source format is {fmt})",
            key = which.key(),
        )));
    };
    match op {
        NestedSectionOp::Remove => {
            let removed = opts.as_mut().and_then(|o| which.slot(o).take());
            if removed.is_none() {
                return Err(ConfigError::Validation(format!(
                    "[E239] channel {key} patch on source '{src}': remove of a declaration the \
                     source does not carry",
                    key = which.key(),
                )));
            }
        }
        NestedSectionOp::Set { name, fields } => {
            let slot = which.slot(opts.get_or_insert_with(Default::default));
            match slot {
                Some(existing) => {
                    if let Some(n) = name {
                        existing.name = n.clone();
                    }
                    for (field, field_op) in fields {
                        match field_op {
                            EnvelopeFieldOp::Set(ty) => {
                                existing.fields.insert(field.clone(), *ty);
                            }
                            EnvelopeFieldOp::Remove => {
                                if existing.fields.shift_remove(field).is_none() {
                                    return Err(unknown_nested_field(src, which, field));
                                }
                            }
                        }
                    }
                }
                None => {
                    let name = name.clone().ok_or_else(|| {
                        ConfigError::Validation(format!(
                            "[E240] channel {key} patch on source '{src}': the source declares \
                             no {key} yet, so the patch creates one and must carry a `name`",
                            key = which.key(),
                        ))
                    })?;
                    let mut new_fields = IndexMap::new();
                    for (field, field_op) in fields {
                        match field_op {
                            EnvelopeFieldOp::Set(ty) => {
                                new_fields.insert(field.clone(), *ty);
                            }
                            EnvelopeFieldOp::Remove => {
                                return Err(unknown_nested_field(src, which, field));
                            }
                        }
                    }
                    *slot = Some(NestedEnvelopeSection {
                        name,
                        fields: new_fields,
                    });
                }
            }
        }
    }
    Ok(())
}

fn unknown_nested_field(src: &str, which: X12SectionSlot, field: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E239] channel {key} patch on source '{src}': remove of unknown field '{field}'",
        key = which.key(),
    ))
}

/// Apply field-name-keyed composite-split ops to the source's HL7 options in
/// place. A non-HL7 source is rejected (E238); a `remove` of an undeclared
/// split is E239; a malformed op — a key that is not a positional `fNN` name,
/// an added split without `components`, or a zero axis width — is E240. Keys
/// resolve by wire position, so `f8` addresses a split declared as `f08`.
/// Bounds against `max_fields` and duplicate-target detection stay with
/// `validate_config`, which runs on the patched result.
fn apply_split_field_ops(
    source: &mut SourceConfig,
    ops: &IndexMap<String, SplitFieldOp>,
    src: &str,
) -> Result<(), ConfigError> {
    if ops.is_empty() {
        return Ok(());
    }
    let fmt = source.format.format_name();
    let InputFormat::Hl7(opts) = &mut source.format else {
        return Err(ConfigError::Validation(format!(
            "[E238] channel split_fields patch on source '{src}': HL7 composite-field split \
             ops apply only to an `hl7` source (source format is {fmt})"
        )));
    };
    for (field, op) in ops {
        let position = Hl7FieldSplitOption::parse_field_position(field).ok_or_else(|| {
            ConfigError::Validation(format!(
                "[E240] channel split_fields patch on source '{src}': {field:?} is not a \
                 positional `fNN` column name (e.g. `f08`)"
            ))
        })?;
        match op {
            SplitFieldOp::Remove => {
                let Some(entries) = opts.as_mut().and_then(|o| o.split_fields.as_mut()) else {
                    return Err(unknown_split_field(src, field));
                };
                let Some(idx) = entries
                    .iter()
                    .position(|s| s.field_position() == Some(position))
                else {
                    return Err(unknown_split_field(src, field));
                };
                entries.remove(idx);
            }
            SplitFieldOp::Set {
                components,
                subcomponents,
                repetitions,
            } => {
                // Only the widths this op itself writes are checked here, so
                // the diagnostic blames the patch and never a pre-existing
                // declaration (validate_config re-checks the whole result).
                for (axis, provided) in [
                    ("components", components),
                    ("subcomponents", subcomponents),
                    ("repetitions", repetitions),
                ] {
                    if *provided == Some(0) {
                        return Err(ConfigError::Validation(format!(
                            "[E240] channel split_fields patch on source '{src}': split field \
                             '{field}' sets `{axis}: 0`; every axis width must be at least 1"
                        )));
                    }
                }
                let entries = opts
                    .get_or_insert_with(Default::default)
                    .split_fields
                    .get_or_insert_with(Vec::new);
                if let Some(existing) = entries
                    .iter_mut()
                    .find(|s| s.field_position() == Some(position))
                {
                    // Partial modify: an omitted axis keeps its current width.
                    if let Some(c) = components {
                        existing.components = *c;
                    }
                    if let Some(s) = subcomponents {
                        existing.subcomponents = *s;
                    }
                    if let Some(r) = repetitions {
                        existing.repetitions = *r;
                    }
                } else {
                    let components = components.ok_or_else(|| {
                        ConfigError::Validation(format!(
                            "[E240] channel split_fields patch on source '{src}': the source \
                             declares no split for field '{field}' yet, so the patch adds one \
                             and must carry `components`"
                        ))
                    })?;
                    entries.push(Hl7FieldSplitOption {
                        field: field.clone(),
                        components,
                        subcomponents: subcomponents.unwrap_or(1),
                        repetitions: repetitions.unwrap_or(1),
                    });
                }
            }
        }
    }
    Ok(())
}

fn unknown_split_field(src: &str, field: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E239] channel split_fields patch on source '{src}': remove of a split the source \
         does not declare for field '{field}'"
    ))
}

/// Apply a channel's multi-record `records` / `discriminator` ops to a source's
/// schema in place. Requires a [`SourceSchema::MultiRecord`] schema (E241 on a
/// single-record / generated / file schema). The discriminator patch merges
/// field by field and the merged result must be byte-range XOR field (E244).
/// Record ops resolve by record-type id: `modify` / `remove` of an unknown id
/// is E242, `add` of an existing id is E243, and a tag shared across record
/// types after the ops is E245. A `modify`'s nested column ops reuse the same
/// column-op core as the single-record `schema` surface.
fn apply_record_ops(
    schema: &mut SourceSchema,
    ops: &IndexMap<String, RecordTypeOp>,
    discriminator: Option<&DiscriminatorPatch>,
    src: &str,
) -> Result<(), ConfigError> {
    if ops.is_empty() && discriminator.is_none() {
        return Ok(());
    }
    let SourceSchema::MultiRecord {
        discriminator: disc,
        record_types,
        ..
    } = schema
    else {
        return Err(ConfigError::Validation(format!(
            "[E241] channel records patch on source '{src}': `records` / `discriminator` ops \
             apply only to a multi-record schema, not a single-record / generated / file schema"
        )));
    };

    if let Some(dp) = discriminator {
        // Partial merge: a named field overwrites, an omitted one is kept. The
        // merged result is then re-checked for the byte-range XOR field shape.
        if let Some(start) = dp.start {
            disc.start = Some(start);
        }
        if let Some(width) = dp.width {
            disc.width = Some(width);
        }
        if let Some(field) = &dp.field {
            disc.field = Some(field.clone());
        }
        validate_discriminator(disc, src)?;
    }

    for (id, op) in ops {
        match op {
            RecordTypeOp::Remove => {
                let idx = record_types
                    .iter()
                    .position(|rt| rt.id == *id)
                    .ok_or_else(|| unknown_record_type(src, id, "remove"))?;
                record_types.remove(idx);
            }
            RecordTypeOp::Modify(patch) => {
                let rt = record_types
                    .iter_mut()
                    .find(|rt| rt.id == *id)
                    .ok_or_else(|| unknown_record_type(src, id, "modify"))?;
                if let Some(tag) = &patch.tag {
                    rt.tag = tag.clone();
                }
                if let Some(parent) = &patch.parent {
                    rt.parent = Some(parent.clone());
                }
                if let Some(join_key) = &patch.join_key {
                    rt.join_key = Some(join_key.clone());
                }
                if let Some(description) = &patch.description {
                    rt.description = Some(description.clone());
                }
                // Reuse the single-record column-op core on this record type's
                // own column list; the id threads into the diagnostic label so a
                // column error names the offending record type. Multi-record
                // schema provenance is not modeled, so no recorder is passed.
                let ctx = format!("{src} records.{id}");
                apply_column_ops(&mut rt.columns, &patch.columns, &ctx, None)?;
            }
            RecordTypeOp::Add(add) => {
                if record_types.iter().any(|rt| rt.id == *id) {
                    return Err(ConfigError::Validation(format!(
                        "[E243] channel records patch on source '{src}': add of record type \
                         '{id}' that already exists"
                    )));
                }
                record_types.push(RecordType {
                    id: id.clone(),
                    tag: add.tag.clone(),
                    description: add.description.clone(),
                    parent: add.parent.clone(),
                    join_key: add.join_key.clone(),
                    columns: add.columns.clone(),
                });
            }
        }
    }

    // Tags must stay unique across record types so the reader's discriminator
    // dispatch is unambiguous; a collision an `add` or `modify` introduces is
    // caught here rather than deep in the reader. A discriminator-only patch
    // leaves tags untouched, so it skips this check.
    if !ops.is_empty() {
        check_tag_uniqueness(record_types, src)?;
    }
    Ok(())
}

/// Reject a merged discriminator that is not exactly one of byte-range or field
/// (E244), mirroring the reader's fixed-width-vs-CSV dispatch: byte-range needs
/// a `start` (width defaults to 1), field needs a `field` name, and the two
/// modes are mutually exclusive.
fn validate_discriminator(disc: &Discriminator, src: &str) -> Result<(), ConfigError> {
    let has_byte = disc.start.is_some() || disc.width.is_some();
    let has_field = disc.field.is_some();
    let malformed = |reason: &str| {
        ConfigError::Validation(format!(
            "[E244] channel discriminator patch on source '{src}': merged discriminator {reason}; \
             a discriminator is a byte range (`start` + optional `width`) XOR a `field`"
        ))
    };
    match (has_byte, has_field) {
        (true, true) => Err(malformed("names both a byte range and a `field`")),
        (false, false) => Err(malformed("names neither a byte range nor a `field`")),
        (true, false) if disc.start.is_none() => {
            Err(malformed("sets a byte `width` without a `start` offset"))
        }
        _ => Ok(()),
    }
}

/// Reject two record types sharing a discriminator tag after the ops (E245).
fn check_tag_uniqueness(record_types: &[RecordType], src: &str) -> Result<(), ConfigError> {
    for (i, rt) in record_types.iter().enumerate() {
        if record_types[..i].iter().any(|other| other.tag == rt.tag) {
            return Err(ConfigError::Validation(format!(
                "[E245] channel records patch on source '{src}': discriminator tag '{tag}' is \
                 declared by more than one record type after the patch",
                tag = rt.tag
            )));
        }
    }
    Ok(())
}

fn unknown_record_type(src: &str, id: &str, op: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E242] channel records patch on source '{src}': {op} of unknown record type '{id}'"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A CSV source named `src` with three declared columns, feeding one
    /// output. Parsed (not validated), which is all the patch handlers need.
    fn csv_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/in.csv
      schema:
        - { name: amount, type: int }
        - { name: cust_id, type: string }
        - { name: order_notes, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse csv pipeline")
    }

    fn patch_from_yaml(yaml: &str) -> SourceConfigPatch {
        crate::yaml::from_str(yaml).expect("parse patch")
    }

    fn columns(config: &PipelineConfig) -> Vec<(String, Type)> {
        config
            .source_bodies()
            .next()
            .expect("one source")
            .schema
            .as_columns()
            .expect("single-record schema")
            .iter()
            .map(|c| (c.name.clone(), c.ty.clone()))
            .collect()
    }

    fn apply(
        config: &mut PipelineConfig,
        source: &str,
        patch: SourceConfigPatch,
    ) -> Result<(), ConfigError> {
        let mut patches = IndexMap::new();
        patches.insert(source.to_string(), patch);
        apply_source_patches(config, &patches)
    }

    #[test]
    fn schema_modify_changes_type() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { type: float }\n"),
        )
        .unwrap();
        assert_eq!(columns(&config)[0], ("amount".to_string(), Type::Float));
    }

    #[test]
    fn schema_modify_sets_multiple_attributes() {
        // The enriched modify leaf sets any subset of a column's attributes,
        // leaf-replace, keeping every attribute it does not name.
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { type: float, scale: 2, format: \"%.2f\" }\n"),
        )
        .unwrap();
        let col = column_named(&config, "amount");
        assert_eq!(col.ty, Type::Float);
        assert_eq!(col.scale, Some(2));
        assert_eq!(col.format.as_deref(), Some("%.2f"));
    }

    #[test]
    fn schema_modify_only_scale_keeps_type() {
        // A modify that names only `scale` must keep the base column's type.
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { scale: 4 }\n"),
        )
        .unwrap();
        let col = column_named(&config, "amount");
        assert_eq!(col.ty, Type::Int, "type unchanged");
        assert_eq!(col.scale, Some(4));
    }

    #[test]
    fn schema_modify_unknown_attribute_rejected_at_parse() {
        // A typo on a modify attribute is an error, not a silent append.
        let err = crate::yaml::from_str::<SourceConfigPatch>("schema:\n  amount: { scal: 2 }\n")
            .unwrap_err();
        let msg = format!("{}", err.0);
        assert!(msg.contains("scal") || msg.contains("unknown"), "{msg}");
    }

    #[test]
    fn schema_add_with_extra_attributes() {
        // `add` accepts the full attribute set, not just `type`.
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  offset: { add: { type: int, start: 4, width: 8 } }\n"),
        )
        .unwrap();
        let col = column_named(&config, "offset");
        assert_eq!(col.ty, Type::Int);
        assert_eq!(col.start, Some(4));
        assert_eq!(col.width, Some(8));
    }

    #[test]
    fn schema_add_without_type_errors_e236() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  region: { add: { scale: 2 } }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E236"), "{err}");
    }

    /// Fetch a column by exposed name from the single source.
    fn column_named<'a>(config: &'a PipelineConfig, name: &str) -> &'a Column {
        config
            .source_bodies()
            .next()
            .expect("one source")
            .schema
            .as_columns()
            .expect("single-record schema")
            .iter()
            .find(|c| c.name == name)
            .expect("column present")
    }

    #[test]
    fn schema_rename_renames_column_and_sets_physical_alias() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  cust_id: { rename: customer_id }\n"),
        )
        .unwrap();
        // Exposed name changes...
        assert_eq!(columns(&config)[1].0, "customer_id");
        // ...but the physical binding is preserved: rename is an alias, not a
        // bare relabel, so the reader still reads the `cust_id` source field.
        let column = column_named(&config, "customer_id");
        assert_eq!(column.source_name.as_deref(), Some("cust_id"));
    }

    #[test]
    fn schema_double_rename_keeps_original_physical() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml(
                "schema:\n  cust_id: { rename: mid }\n  mid: { rename: customer_id }\n",
            ),
        )
        .unwrap();
        let column = column_named(&config, "customer_id");
        // Both renames applied in order; the physical binding is still the
        // original source column, not the intermediate name.
        assert_eq!(column.source_name.as_deref(), Some("cust_id"));
    }

    #[test]
    fn schema_remove_drops_column() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  order_notes: remove\n"),
        )
        .unwrap();
        let names: Vec<String> = columns(&config).into_iter().map(|(n, _)| n).collect();
        assert_eq!(names, vec!["amount".to_string(), "cust_id".to_string()]);
    }

    #[test]
    fn schema_add_appends_column() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  region: { add: { type: string } }\n"),
        )
        .unwrap();
        let cols = columns(&config);
        assert_eq!(cols.last().unwrap(), &("region".to_string(), Type::String));
    }

    #[test]
    fn schema_add_long_unique_flag_flows_through() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  uuid: { add: { type: string, long_unique: true } }\n"),
        )
        .unwrap();
        let body = config.source_bodies().next().unwrap();
        let col = body
            .schema
            .as_columns()
            .expect("single-record schema")
            .iter()
            .find(|c| c.name == "uuid")
            .unwrap();
        assert!(col.is_long_unique());
    }

    #[test]
    fn schema_combined_ops_apply_in_order() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml(
                "schema:\n  amount: { type: float }\n  cust_id: { rename: customer_id }\n  order_notes: remove\n  region: { add: { type: string } }\n",
            ),
        )
        .unwrap();
        assert_eq!(
            columns(&config),
            vec![
                ("amount".to_string(), Type::Float),
                ("customer_id".to_string(), Type::String),
                ("region".to_string(), Type::String),
            ]
        );
    }

    #[test]
    fn schema_modify_unknown_column_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  nope: { type: float }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E231"), "{err}");
    }

    #[test]
    fn schema_rename_unknown_column_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  nope: { rename: x }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E231"), "{err}");
    }

    #[test]
    fn schema_remove_unknown_column_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  nope: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E231"), "{err}");
    }

    #[test]
    fn schema_add_existing_column_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { add: { type: float } }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E232"), "{err}");
    }

    #[test]
    fn schema_rename_collision_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { rename: cust_id }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E233"), "{err}");
    }

    #[test]
    fn schema_rename_to_same_name_is_allowed() {
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { rename: amount }\n"),
        )
        .unwrap();
        assert_eq!(columns(&config)[0].0, "amount");
    }

    /// A column-keyed schema patch targets a single-record (column-list) schema.
    /// Targeting a multi-record source is rejected with E237 (enriched
    /// multi-record patching is a later phase), not silently ignored.
    #[test]
    fn schema_patch_on_multi_record_source_errors_e237() {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: fixed_width
      path: /tmp/in.dat
      schema:
        discriminator: { start: 0, width: 1 }
        records:
          - { id: detail, tag: D, columns: [ { name: amount, type: int, start: 1, width: 9 } ] }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        let mut config: PipelineConfig =
            crate::yaml::from_str(yaml).expect("parse multi-record pipeline");
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  amount: { type: float }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E237"), "{err}");
    }

    #[test]
    fn unknown_source_name_errors() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "ghost",
            patch_from_yaml("schema:\n  amount: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E230"), "{err}");
    }

    /// A pipeline whose source NODE identity (`header.name`) differs from the
    /// nested `config.name` — the two come from different YAML keys.
    fn distinct_name_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: node_ident
    config:
      name: cfg_name
      type: csv
      path: /tmp/in.csv
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: node_ident
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse distinct-name pipeline")
    }

    #[test]
    fn patch_resolves_by_node_header_name() {
        let mut config = distinct_name_pipeline();
        // Keyed by node identity (header.name), the same key every channel
        // block uses.
        apply(
            &mut config,
            "node_ident",
            patch_from_yaml("schema:\n  amount: { type: float }\n"),
        )
        .unwrap();
        assert_eq!(columns(&config)[0], ("amount".to_string(), Type::Float));
    }

    #[test]
    fn patch_by_config_name_does_not_resolve() {
        let mut config = distinct_name_pipeline();
        // The nested `config.name` is NOT the channel identity → unknown source.
        let err = apply(
            &mut config,
            "cfg_name",
            patch_from_yaml("schema:\n  amount: { type: float }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E230"), "{err}");
    }

    #[test]
    fn unknown_source_with_composition_gives_hint() {
        // A pipeline that calls a composition: an unresolved source name gets a
        // hint about composition-body sources rather than a bare typo error.
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/in.csv
      schema:
        - { name: id, type: string }
  - type: composition
    name: comp
    input: src
    use: ./thing.comp.yaml
  - type: output
    name: out
    input: comp
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        let mut config = crate::yaml::from_str(yaml).expect("parse composition pipeline");
        let err = apply(
            &mut config,
            "inner_src",
            patch_from_yaml("schema:\n  id: remove\n"),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("E230"), "{msg}");
        assert!(
            msg.contains("composition"),
            "expected a composition-body hint: {msg}"
        );
    }

    /// A pipeline that invokes a composition named `comp`, for the qualified
    /// `<composition>.<source>` body-source patch-key tests.
    fn composition_call_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: qualified_patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/in.csv
      schema:
        - { name: id, type: string }
  - type: composition
    name: comp
    input: src
    use: ./thing.comp.yaml
    inputs:
      driver: src
  - type: output
    name: out
    input: comp
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse composition pipeline")
    }

    #[test]
    fn qualified_key_defers_body_source_patch() {
        // `comp.ref` names the composition `comp` (present) and its body source
        // `ref`; the patch is stashed for compile, not applied to any top-level
        // source.
        let mut config = composition_call_pipeline();
        apply(
            &mut config,
            "comp.ref",
            patch_from_yaml("schema:\n  id: remove\n"),
        )
        .expect("a qualified patch on a real composition defers cleanly");
        let deferred = config
            .body_source_patches
            .get("comp")
            .expect("deferred under the composition node name");
        assert!(
            deferred.contains_key("ref"),
            "deferred patch keyed by the inner source name"
        );
    }

    #[test]
    fn qualified_key_unknown_composition_errors_e230() {
        let mut config = composition_call_pipeline();
        let err = apply(
            &mut config,
            "ghost.ref",
            patch_from_yaml("schema:\n  id: remove\n"),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("E230"), "{msg}");
        assert!(
            msg.contains("ghost"),
            "must name the missing composition: {msg}"
        );
    }

    #[test]
    fn nested_multidot_key_rejected_e230() {
        let mut config = composition_call_pipeline();
        let err = apply(
            &mut config,
            "comp.inner.ref",
            patch_from_yaml("schema:\n  id: remove\n"),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("E230"), "{msg}");
        assert!(
            msg.contains("nested"),
            "must explain a nested body source is not addressable: {msg}"
        );
    }

    #[test]
    fn empty_segment_qualified_key_rejected_e230() {
        let mut config = composition_call_pipeline();
        let err = apply(
            &mut config,
            "comp.",
            patch_from_yaml("schema:\n  id: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E230"), "{err}");
    }

    /// A JSON source declaring a `record_path` option, one fan-out entry, and
    /// one in-cell-parse entry over a multi-value column.
    fn json_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: /tmp/in.json
      options:
        record_path: data.rows
      split_to_rows:
        - items
      split_values:
        - field: tags
      schema:
        - { name: id, type: int }
        - { name: tags, type: string, multiple: true }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse json pipeline")
    }

    fn split_to_rows(config: &PipelineConfig) -> Vec<SplitToRows> {
        config
            .source_configs()
            .next()
            .unwrap()
            .split_to_rows
            .clone()
            .unwrap_or_default()
    }

    fn split_values(config: &PipelineConfig) -> Vec<SplitValues> {
        config
            .source_configs()
            .next()
            .unwrap()
            .split_values
            .clone()
            .unwrap_or_default()
    }

    #[test]
    fn split_to_rows_modify_existing_entry() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_to_rows:\n  items: { mode: split, position_column: line_no }\n"),
        )
        .unwrap();
        let entries = split_to_rows(&config);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].mode, SplitToRowsMode::Split);
        assert_eq!(entries[0].position_column.as_deref(), Some("line_no"));
    }

    #[test]
    fn split_to_rows_add_new_entry() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_to_rows:\n  tags: { mode: extract }\n"),
        )
        .unwrap();
        let entries = split_to_rows(&config);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|e| e.field == "tags"));
    }

    #[test]
    fn split_to_rows_partial_modify_keeps_omitted_attributes() {
        let mut config = json_pipeline();
        // Make `items` a split entry with a position column...
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_to_rows:\n  items: { mode: split, position_column: line_no }\n"),
        )
        .unwrap();
        // ...then patch only `keep_empty`: the mode and position column must
        // stay put, not silently revert to their defaults.
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_to_rows:\n  items: { keep_empty: false }\n"),
        )
        .unwrap();
        let entries = split_to_rows(&config);
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].mode,
            SplitToRowsMode::Split,
            "mode must remain split, got {:?}",
            entries[0].mode
        );
        assert_eq!(entries[0].position_column.as_deref(), Some("line_no"));
        assert!(!entries[0].keep_empty);
    }

    #[test]
    fn split_to_rows_explicit_null_clears_an_attribute() {
        // An omitted key keeps the current value, so a set attribute needs an
        // explicit way to be cleared: YAML's own null.
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_to_rows:\n  items: { position_column: line_no }\n"),
        )
        .unwrap();
        assert_eq!(
            split_to_rows(&config)[0].position_column.as_deref(),
            Some("line_no")
        );
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_to_rows:\n  items: { position_column: ~ }\n"),
        )
        .unwrap();
        assert_eq!(split_to_rows(&config)[0].position_column, None);
    }

    #[test]
    fn split_to_rows_new_entry_takes_the_hand_written_defaults() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_to_rows:\n  tags: {}\n"),
        )
        .unwrap();
        let entries = split_to_rows(&config);
        let tags = entries.iter().find(|e| e.field == "tags").unwrap();
        assert_eq!(tags.mode, SplitToRowsMode::Extract);
        assert!(tags.keep_empty, "keep_empty defaults to true");
        assert_eq!(tags.position_column, None);
    }

    #[test]
    fn split_to_rows_remove_entry() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_to_rows:\n  items: remove\n"),
        )
        .unwrap();
        assert!(split_to_rows(&config).is_empty());
    }

    #[test]
    fn split_to_rows_remove_unknown_errors() {
        let mut config = json_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("split_to_rows:\n  ghost: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E234"), "{err}");
    }

    #[test]
    fn split_values_modify_and_remove() {
        let mut config = json_pipeline();
        assert_eq!(split_values(&config)[0].delimiter, ";");
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_values:\n  tags: { delimiter: \"|\" }\n"),
        )
        .unwrap();
        assert_eq!(split_values(&config)[0].delimiter, "|");
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_values:\n  tags: remove\n"),
        )
        .unwrap();
        assert!(split_values(&config).is_empty());
    }

    #[test]
    fn split_values_remove_unknown_errors() {
        let mut config = json_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("split_values:\n  ghost: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E234"), "{err}");
    }

    /// A typo'd key inside a patch op must NAME the offending key and the
    /// valid ones, not collapse into one message that says only that nothing
    /// matched — and it must point at the key, not at the block around it.
    #[test]
    fn split_op_typo_names_the_offending_key() {
        let err = crate::yaml::from_str::<SourceConfigPatch>(
            "split_to_rows:\n  items: { keep_emty: false }\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("keep_emty"), "{err}");
        assert!(err.contains("keep_empty"), "{err}");

        let err = crate::yaml::from_str::<SourceConfigPatch>(
            "split_values:\n  tags: { delimeter: \";\" }\n",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("delimeter"), "{err}");
        assert!(err.contains("delimiter"), "{err}");
    }

    #[test]
    fn unknown_split_op_scalar_names_both_accepted_forms() {
        let err = crate::yaml::from_str::<SourceConfigPatch>("split_to_rows:\n  items: drop\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("remove"), "{err}");
        assert!(err.contains("keep_empty"), "{err}");

        let err = crate::yaml::from_str::<SourceConfigPatch>("split_values:\n  tags: drop\n")
            .unwrap_err()
            .to_string();
        assert!(err.contains("remove"), "{err}");
        assert!(err.contains("delimiter"), "{err}");
    }

    #[test]
    fn schema_patch_sets_and_clears_multiple() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  id: { multiple: true }\n"),
        )
        .unwrap();
        let columns = |c: &PipelineConfig| {
            c.source_bodies()
                .next()
                .unwrap()
                .schema
                .as_columns()
                .unwrap()
                .to_vec()
        };
        assert!(columns(&config)[0].is_multiple());
        apply(
            &mut config,
            "src",
            patch_from_yaml("schema:\n  id: { multiple: false }\n"),
        )
        .unwrap();
        assert!(!columns(&config)[0].is_multiple());
    }

    #[test]
    fn options_set_scalar_record_path() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("options:\n  record_path: batch_records\n"),
        )
        .unwrap();
        let source = config.source_configs().next().unwrap();
        match &source.format {
            InputFormat::Json(Some(opts)) => {
                assert_eq!(opts.record_path.as_deref(), Some("batch_records"));
            }
            other => panic!("expected json options, got {other:?}"),
        }
    }

    #[test]
    fn options_unknown_key_errors() {
        let mut config = json_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("options:\n  not_a_key: 5\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E235"), "{err}");
    }

    #[test]
    fn options_set_preserves_existing_keys() {
        // record_path was `data.rows`; overriding an unrelated csv-ish key is
        // rejected, but setting `format` should keep record_path intact.
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("options:\n  format: ndjson\n"),
        )
        .unwrap();
        let source = config.source_configs().next().unwrap();
        match &source.format {
            InputFormat::Json(Some(opts)) => {
                assert_eq!(opts.record_path.as_deref(), Some("data.rows"));
                assert!(matches!(opts.format, Some(JsonFormat::Ndjson)));
            }
            other => panic!("expected json options, got {other:?}"),
        }
    }

    #[test]
    fn options_on_source_without_options_block() {
        // A CSV source with no `options:` starts from an empty option struct;
        // the override materializes it.
        let mut config = csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("options:\n  delimiter: \"|\"\n"),
        )
        .unwrap();
        let source = config.source_configs().next().unwrap();
        match &source.format {
            InputFormat::Csv(Some(opts)) => assert_eq!(opts.delimiter.as_deref(), Some("|")),
            other => panic!("expected csv options, got {other:?}"),
        }
    }

    #[test]
    fn empty_patch_on_unknown_source_still_errors() {
        let mut config = csv_pipeline();
        let err = apply(&mut config, "ghost", SourceConfigPatch::default()).unwrap_err();
        assert!(err.to_string().contains("E230"), "{err}");
    }

    #[test]
    fn schema_op_ambiguous_map_rejected_at_parse() {
        let err = crate::yaml::from_str::<SourceConfigPatch>(
            "schema:\n  amount: { rename: x, type: float }\n",
        )
        .unwrap_err();
        assert!(
            format!("{}", err.0).contains("exactly one")
                || format!("{}", err.0).contains("ambiguous"),
            "{}",
            err.0
        );
    }

    /// An X12 source declaring a typed `group_section` (`set_section` left
    /// undeclared), feeding one output.
    fn x12_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: x12
      path: /tmp/in.x12
      options:
        group_section:
          name: grp
          fields:
            e06: string
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse x12 pipeline")
    }

    fn x12_options(config: &PipelineConfig) -> &X12InputOptions {
        match &config.source_configs().next().expect("one source").format {
            InputFormat::X12(Some(opts)) => opts,
            other => panic!("expected x12 options, got {other:?}"),
        }
    }

    #[test]
    fn nested_section_modify_patches_name_and_fields() {
        let mut config = x12_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml(
                "group_section:\n  name: fg\n  fields:\n    e06: int\n    e07: string\n",
            ),
        )
        .unwrap();
        let section = x12_options(&config).group_section.as_ref().unwrap();
        assert_eq!(section.name, "fg");
        assert_eq!(section.fields.get("e06"), Some(&EnvelopeFieldType::Int));
        assert_eq!(section.fields.get("e07"), Some(&EnvelopeFieldType::String));
        assert_eq!(section.fields.len(), 2);
    }

    #[test]
    fn nested_section_modify_keeps_omitted_name() {
        // A field-only modify must not clobber the declaration's name.
        let mut config = x12_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("group_section:\n  fields:\n    e07: date\n"),
        )
        .unwrap();
        let section = x12_options(&config).group_section.as_ref().unwrap();
        assert_eq!(section.name, "grp");
        assert_eq!(section.fields.get("e06"), Some(&EnvelopeFieldType::String));
        assert_eq!(section.fields.get("e07"), Some(&EnvelopeFieldType::Date));
    }

    #[test]
    fn nested_section_field_remove_drops_field() {
        let mut config = x12_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("group_section:\n  fields:\n    e06: remove\n"),
        )
        .unwrap();
        let section = x12_options(&config).group_section.as_ref().unwrap();
        assert_eq!(section.name, "grp");
        assert!(section.fields.is_empty());
    }

    #[test]
    fn nested_section_field_remove_unknown_errors_e239() {
        let mut config = x12_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("group_section:\n  fields:\n    e99: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E239"), "{err}");
    }

    #[test]
    fn nested_section_remove_drops_declaration() {
        let mut config = x12_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("group_section: remove\n"),
        )
        .unwrap();
        assert!(x12_options(&config).group_section.is_none());
    }

    #[test]
    fn nested_section_remove_absent_errors_e239() {
        // `set_section` is not declared on the fixture source.
        let mut config = x12_pipeline();
        let err = apply(&mut config, "src", patch_from_yaml("set_section: remove\n")).unwrap_err();
        assert!(err.to_string().contains("E239"), "{err}");
    }

    #[test]
    fn nested_section_create_sets_declaration() {
        let mut config = x12_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml(
                "set_section:\n  name: txn\n  fields:\n    e01: string\n    e02: int\n",
            ),
        )
        .unwrap();
        let section = x12_options(&config).set_section.as_ref().unwrap();
        assert_eq!(section.name, "txn");
        assert_eq!(section.fields.get("e01"), Some(&EnvelopeFieldType::String));
        assert_eq!(section.fields.get("e02"), Some(&EnvelopeFieldType::Int));
    }

    #[test]
    fn nested_section_create_requires_name_e240() {
        let mut config = x12_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("set_section:\n  fields:\n    e01: string\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E240"), "{err}");
    }

    #[test]
    fn nested_section_create_with_field_remove_errors_e239() {
        // A field `remove` while creating a new declaration removes a field
        // that cannot exist yet.
        let mut config = x12_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("set_section:\n  name: txn\n  fields:\n    e01: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E239"), "{err}");
    }

    #[test]
    fn nested_section_on_non_x12_errors_e238() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("group_section:\n  name: fg\n"),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("E238"), "{msg}");
        assert!(msg.contains("csv"), "names the actual format: {msg}");
    }

    #[test]
    fn nested_section_unknown_key_rejected_at_parse() {
        let err =
            crate::yaml::from_str::<SourceConfigPatch>("group_section:\n  nam: fg\n").unwrap_err();
        let msg = format!("{}", err.0);
        assert!(msg.contains("nam") || msg.contains("unknown"), "{msg}");
    }

    #[test]
    fn nested_section_unknown_field_type_rejected_at_parse() {
        let err = crate::yaml::from_str::<SourceConfigPatch>(
            "group_section:\n  fields:\n    e01: strin\n",
        )
        .unwrap_err();
        let msg = format!("{}", err.0);
        assert!(msg.contains("strin"), "{msg}");
    }

    #[test]
    fn nested_section_applies_after_options_blob() {
        // A patch carrying BOTH an `options`-blob replacement of the
        // declaration and a keyed op: the keyed op wins because it applies
        // after the options merge.
        let mut config = x12_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml(
                "options:\n  group_section: { name: blob, fields: { e01: string } }\ngroup_section:\n  name: keyed\n",
            ),
        )
        .unwrap();
        let section = x12_options(&config).group_section.as_ref().unwrap();
        assert_eq!(section.name, "keyed");
        // The blob-replaced fields survive the field-less keyed rename.
        assert_eq!(section.fields.get("e01"), Some(&EnvelopeFieldType::String));
        assert_eq!(section.fields.len(), 1);
    }

    /// An HL7 source declaring one composite-field split (`f03`), feeding one
    /// output.
    fn hl7_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: hl7
      path: /tmp/in.hl7
      options:
        split_fields:
          - { field: f03, components: 5 }
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse hl7 pipeline")
    }

    fn hl7_splits(config: &PipelineConfig) -> Vec<(String, usize, usize, usize)> {
        match &config.source_configs().next().expect("one source").format {
            InputFormat::Hl7(Some(opts)) => opts
                .split_fields
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|s| (s.field, s.components, s.subcomponents, s.repetitions))
                .collect(),
            other => panic!("expected hl7 options, got {other:?}"),
        }
    }

    #[test]
    fn split_fields_add_new_entry() {
        let mut config = hl7_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_fields:\n  f08: { components: 2 }\n"),
        )
        .unwrap();
        let splits = hl7_splits(&config);
        assert_eq!(splits.len(), 2);
        // New entry: omitted subcomponents/repetitions default to 1, exactly
        // as in hand-written config.
        assert!(splits.contains(&("f08".to_string(), 2, 1, 1)), "{splits:?}");
    }

    #[test]
    fn split_fields_partial_modify_keeps_omitted_axes() {
        let mut config = hl7_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_fields:\n  f03: { subcomponents: 2 }\n"),
        )
        .unwrap();
        // components stays 5 — a partial modify must not reset it.
        assert_eq!(hl7_splits(&config), vec![("f03".to_string(), 5, 2, 1)]);
    }

    #[test]
    fn split_fields_key_resolves_by_wire_position() {
        // `f3` and `f03` name the same wire position, so the patch modifies
        // the existing declaration rather than adding a duplicate split.
        let mut config = hl7_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_fields:\n  f3: { repetitions: 4 }\n"),
        )
        .unwrap();
        assert_eq!(hl7_splits(&config), vec![("f03".to_string(), 5, 1, 4)]);
    }

    #[test]
    fn split_fields_remove_entry() {
        let mut config = hl7_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("split_fields:\n  f03: remove\n"),
        )
        .unwrap();
        assert!(hl7_splits(&config).is_empty());
    }

    #[test]
    fn split_fields_remove_unknown_errors_e239() {
        let mut config = hl7_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("split_fields:\n  f09: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E239"), "{err}");
    }

    #[test]
    fn split_fields_add_requires_components_e240() {
        let mut config = hl7_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("split_fields:\n  f08: { repetitions: 2 }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E240"), "{err}");
    }

    #[test]
    fn split_fields_malformed_key_errors_e240() {
        let mut config = hl7_pipeline();
        for key in ["q08", "f", "f0", "f1x"] {
            let err = apply(
                &mut config,
                "src",
                patch_from_yaml(&format!("split_fields:\n  {key}: {{ components: 2 }}\n")),
            )
            .unwrap_err();
            assert!(err.to_string().contains("E240"), "key {key:?}: {err}");
        }
    }

    #[test]
    fn split_fields_zero_axis_errors_e240() {
        let mut config = hl7_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("split_fields:\n  f03: { components: 0 }\n"),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("E240"), "{msg}");
        assert!(msg.contains("components"), "names the zero axis: {msg}");
    }

    #[test]
    fn split_fields_on_non_hl7_errors_e238() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("split_fields:\n  f08: { components: 2 }\n"),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("E238"), "{msg}");
        assert!(msg.contains("csv"), "names the actual format: {msg}");
    }

    #[test]
    fn patch_with_only_structure_ops_is_not_empty() {
        // If `is_empty` misses a format-structure op the apply pass would
        // silently skip it.
        assert!(!patch_from_yaml("group_section: remove\n").is_empty());
        assert!(!patch_from_yaml("set_section:\n  name: txn\n").is_empty());
        assert!(!patch_from_yaml("split_fields:\n  f01: remove\n").is_empty());
        assert!(SourceConfigPatch::default().is_empty());
    }

    // ── Multi-record `records` / `discriminator` ────────────────────────

    /// A fixed-width multi-record source with a byte-range discriminator and two
    /// record types (`detail` tag `D`, `trailer` tag `T`), feeding one output.
    fn multi_record_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: fixed_width
      path: /tmp/in.dat
      schema:
        discriminator: { start: 0, width: 1 }
        records:
          - { id: detail, tag: D, columns: [ { name: amount, type: int, start: 1, width: 9 } ] }
          - { id: trailer, tag: T, columns: [ { name: count, type: int, start: 1, width: 9 } ] }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse multi-record pipeline")
    }

    /// A CSV multi-record source with a field-mode discriminator.
    fn multi_record_csv_pipeline() -> PipelineConfig {
        let yaml = "\
pipeline:
  name: patch_test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: /tmp/in.csv
      schema:
        discriminator: { field: rec_type }
        records:
          - { id: detail, tag: D, columns: [ { name: rec_type, type: string }, { name: amount, type: float } ] }
          - { id: trailer, tag: T, columns: [ { name: rec_type, type: string }, { name: count, type: int } ] }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: /tmp/out.csv
";
        crate::yaml::from_str(yaml).expect("parse csv multi-record pipeline")
    }

    fn multi_record(config: &PipelineConfig) -> (&Discriminator, &[RecordType]) {
        match &config.source_bodies().next().expect("one source").schema {
            SourceSchema::MultiRecord {
                discriminator,
                record_types,
                ..
            } => (discriminator, record_types),
            other => panic!("expected multi-record schema, got {other:?}"),
        }
    }

    fn record_type<'a>(record_types: &'a [RecordType], id: &str) -> &'a RecordType {
        record_types
            .iter()
            .find(|rt| rt.id == id)
            .unwrap_or_else(|| panic!("record type {id:?} present"))
    }

    #[test]
    fn records_modify_changes_tag() {
        let mut config = multi_record_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("records:\n  detail: { tag: X }\n"),
        )
        .unwrap();
        let (_disc, rts) = multi_record(&config);
        assert_eq!(record_type(rts, "detail").tag, "X");
        // A field-only modify keeps the rest of the record type intact.
        assert_eq!(record_type(rts, "detail").columns.len(), 1);
    }

    #[test]
    fn records_modify_sets_parent_and_join_key() {
        let mut config = multi_record_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("records:\n  trailer: { parent: detail, join_key: batch_id }\n"),
        )
        .unwrap();
        let (_disc, rts) = multi_record(&config);
        let trailer = record_type(rts, "trailer");
        assert_eq!(trailer.parent.as_deref(), Some("detail"));
        assert_eq!(trailer.join_key.as_deref(), Some("batch_id"));
        // The tag is untouched by an attribute-only modify.
        assert_eq!(trailer.tag, "T");
    }

    #[test]
    fn records_modify_reshapes_nested_columns() {
        // A modify's `columns` map runs the same column-op grammar as the
        // top-level `schema` surface, scoped to this record type.
        let mut config = multi_record_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml(
                "records:\n  detail:\n    columns:\n      amount: { type: float }\n      note: { add: { type: string, start: 10, width: 4 } }\n",
            ),
        )
        .unwrap();
        let (_disc, rts) = multi_record(&config);
        let detail = record_type(rts, "detail");
        assert_eq!(
            detail
                .columns
                .iter()
                .find(|c| c.name == "amount")
                .unwrap()
                .ty,
            Type::Float
        );
        let note = detail
            .columns
            .iter()
            .find(|c| c.name == "note")
            .expect("added column present");
        assert_eq!(note.ty, Type::String);
        assert_eq!(note.start, Some(10));
        assert_eq!(note.width, Some(4));
        // Only `detail` was touched; `trailer` is unchanged.
        assert_eq!(record_type(rts, "trailer").columns.len(), 1);
    }

    #[test]
    fn records_add_new_record_type() {
        let mut config = multi_record_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml(
                "records:\n  header:\n    add:\n      tag: H\n      parent: detail\n      columns:\n        - { name: hdr_id, type: string, start: 1, width: 8 }\n",
            ),
        )
        .unwrap();
        let (_disc, rts) = multi_record(&config);
        assert_eq!(rts.len(), 3);
        let header = record_type(rts, "header");
        assert_eq!(header.tag, "H");
        assert_eq!(header.parent.as_deref(), Some("detail"));
        assert_eq!(header.columns.len(), 1);
        assert_eq!(header.columns[0].name, "hdr_id");
    }

    #[test]
    fn records_remove_record_type() {
        let mut config = multi_record_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("records:\n  trailer: remove\n"),
        )
        .unwrap();
        let (_disc, rts) = multi_record(&config);
        let ids: Vec<&str> = rts.iter().map(|rt| rt.id.as_str()).collect();
        assert_eq!(ids, vec!["detail"]);
    }

    #[test]
    fn records_modify_unknown_id_errors_e242() {
        let mut config = multi_record_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("records:\n  ghost: { tag: G }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E242"), "{err}");
    }

    #[test]
    fn records_remove_unknown_id_errors_e242() {
        let mut config = multi_record_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("records:\n  ghost: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E242"), "{err}");
    }

    #[test]
    fn records_add_existing_id_errors_e243() {
        let mut config = multi_record_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml(
                "records:\n  detail:\n    add:\n      tag: Z\n      columns:\n        - { name: x, type: int, start: 1, width: 2 }\n",
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E243"), "{err}");
    }

    #[test]
    fn records_add_tag_collision_errors_e245() {
        // A new record type reusing an existing tag would make the reader's
        // discriminator dispatch ambiguous.
        let mut config = multi_record_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml(
                "records:\n  header:\n    add:\n      tag: D\n      columns:\n        - { name: hdr_id, type: string, start: 1, width: 8 }\n",
            ),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E245"), "{err}");
    }

    #[test]
    fn records_modify_tag_collision_errors_e245() {
        let mut config = multi_record_pipeline();
        // Retagging `trailer` to `D` collides with `detail`.
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("records:\n  trailer: { tag: D }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E245"), "{err}");
    }

    #[test]
    fn records_modify_nested_unknown_column_errors_and_names_record() {
        let mut config = multi_record_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("records:\n  detail:\n    columns:\n      ghost: { type: float }\n"),
        )
        .unwrap_err();
        let msg = err.to_string();
        // The reused column-op core raises E231 for the unknown column...
        assert!(msg.contains("E231"), "{msg}");
        // ...and the diagnostic names the offending record type.
        assert!(msg.contains("records.detail"), "{msg}");
    }

    #[test]
    fn discriminator_merge_moves_byte_range() {
        let mut config = multi_record_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("discriminator: { start: 2, width: 3 }\n"),
        )
        .unwrap();
        let (disc, _rts) = multi_record(&config);
        assert_eq!(disc.start, Some(2));
        assert_eq!(disc.width, Some(3));
        assert_eq!(disc.field, None);
    }

    #[test]
    fn discriminator_partial_merge_keeps_omitted_field() {
        let mut config = multi_record_pipeline();
        // Setting only `width` keeps the base `start: 0`.
        apply(
            &mut config,
            "src",
            patch_from_yaml("discriminator: { width: 4 }\n"),
        )
        .unwrap();
        let (disc, _rts) = multi_record(&config);
        assert_eq!(disc.start, Some(0));
        assert_eq!(disc.width, Some(4));
    }

    #[test]
    fn discriminator_field_merge_on_csv() {
        let mut config = multi_record_csv_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("discriminator: { field: kind }\n"),
        )
        .unwrap();
        let (disc, _rts) = multi_record(&config);
        assert_eq!(disc.field.as_deref(), Some("kind"));
        assert_eq!(disc.start, None);
        assert_eq!(disc.width, None);
    }

    #[test]
    fn discriminator_merge_mixing_modes_errors_e244() {
        // The byte-range base plus a `field` merge yields a discriminator that
        // is neither pure byte-range nor pure field.
        let mut config = multi_record_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("discriminator: { field: rec_type }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E244"), "{err}");
    }

    #[test]
    fn records_op_on_single_record_source_errors_e241() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("records:\n  detail: { tag: X }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E241"), "{err}");
    }

    #[test]
    fn discriminator_op_on_single_record_source_errors_e241() {
        let mut config = csv_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("discriminator: { field: kind }\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E241"), "{err}");
    }

    #[test]
    fn records_op_ambiguous_add_with_modify_rejected_at_parse() {
        let err = crate::yaml::from_str::<SourceConfigPatch>(
            "records:\n  detail: { add: { tag: H, columns: [] }, tag: X }\n",
        )
        .unwrap_err();
        let msg = format!("{}", err.0);
        assert!(
            msg.contains("exactly one") || msg.contains("ambiguous"),
            "{msg}"
        );
    }

    #[test]
    fn records_add_without_columns_rejected_at_parse() {
        // `columns` is required on an `add`, matching a hand-written `records:`
        // entry: an add missing it fails to deserialize (the untagged op grammar
        // surfaces this as a no-matching-variant error rather than propagating
        // the inner missing-field message).
        let err =
            crate::yaml::from_str::<SourceConfigPatch>("records:\n  header: { add: { tag: H } }\n")
                .unwrap_err();
        let msg = format!("{}", err.0);
        assert!(
            msg.contains("columns") || msg.contains("missing") || msg.contains("variant"),
            "{msg}"
        );
    }

    #[test]
    fn records_bare_scalar_other_than_remove_rejected_at_parse() {
        let err =
            crate::yaml::from_str::<SourceConfigPatch>("records:\n  detail: nope\n").unwrap_err();
        let msg = format!("{}", err.0);
        assert!(msg.contains("nope") || msg.contains("remove"), "{msg}");
    }

    #[test]
    fn patch_with_only_multi_record_ops_is_not_empty() {
        assert!(!patch_from_yaml("records:\n  detail: remove\n").is_empty());
        assert!(!patch_from_yaml("discriminator: { start: 1 }\n").is_empty());
    }
}
