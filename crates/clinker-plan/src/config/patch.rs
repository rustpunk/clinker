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
//! column list (`schema`), nested-array explosion/join (`array_paths`), and
//! scalar per-format input options (`options`) — plus the format-structure
//! ops for X12 nested-envelope declarations (`group_section` / `set_section`)
//! and HL7 composite-field splits (`split_fields`). Deeper format-layer
//! layouts (fixed-width/positional field schemas, multi-record
//! discrimination) reuse this same framework as additional handlers.

use super::*;
use crate::config::composition::SchemaProvRecorder;
use crate::config::pipeline_node::{PipelineNode, SourceBody};
use clinker_format::{Column, EnvelopeFieldType, NestedEnvelopeSection, SourceSchema};
use clinker_record::schema_def::{Justify, TruncationPolicy};
use cxl::typecheck::Type;
use indexmap::IndexMap;
use serde::de;
use serde::{Deserialize, Serialize};

/// Per-source config patch carried by a channel.
///
/// Keyed forms only. `schema` ops are keyed by column name, `array_paths` ops
/// by array path, and `options` sets a scalar per-format input option by its
/// key. `Serialize` is derived for config-identity hashing only (folding the
/// applied patches into the pipeline `source_hash`); the wire deserialize form
/// is the channel YAML.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct SourceConfigPatch {
    /// Column-name-keyed schema ops (`modify` / `rename` / `add` / `remove`).
    pub schema: IndexMap<String, SchemaColumnOp>,
    /// Array-path-keyed ops: set-by-path (add-or-modify) or `remove`.
    pub array_paths: IndexMap<String, ArrayPathOp>,
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
}

impl SourceConfigPatch {
    /// Whether this patch carries no ops — a no-op the apply pass can skip.
    pub fn is_empty(&self) -> bool {
        self.schema.is_empty()
            && self.array_paths.is_empty()
            && self.options.is_empty()
            && self.group_section.is_none()
            && self.set_section.is_none()
            && self.split_fields.is_empty()
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

/// One array-path-keyed op.
///
/// YAML forms (the map key is the array path):
///
/// ```yaml
/// items:      { mode: join, separator: ";" }  # add-or-modify the entry
/// line_items: remove                          # drop the entry
/// ```
///
/// `Serialize` is derived for config-identity hashing only.
#[derive(Debug, Clone, Serialize)]
pub enum ArrayPathOp {
    /// Drop the keyed array-path entry.
    Remove,
    /// Add-or-modify the keyed array-path entry. A field is optional so a
    /// partial modify keeps the omitted field: on an **existing** entry an
    /// omitted `mode`/`separator` retains the current value; on a **new** entry
    /// an omitted `mode` defaults to explode and an omitted `separator` is none.
    Set {
        /// Explode or join; `None` = keep current (existing) / explode (new).
        mode: Option<ArrayMode>,
        /// Join separator; `None` = keep current (existing) / none (new).
        separator: Option<String>,
    },
}

impl<'de> Deserialize<'de> for ArrayPathOp {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        // Two accepted YAML shapes:
        //   remove                       (bare scalar)
        //   { mode?, separator? }        (entry map)
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct SetMap {
            #[serde(default)]
            mode: Option<ArrayMode>,
            #[serde(default)]
            separator: Option<String>,
        }

        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            Scalar(String),
            Map(SetMap),
        }

        match Either::deserialize(d)? {
            Either::Scalar(s) if s == "remove" => Ok(ArrayPathOp::Remove),
            Either::Scalar(other) => Err(de::Error::custom(format!(
                "unknown array_paths op {other:?}; the bare scalar form only accepts `remove` — \
                 use `{{ mode: explode|join, separator: <str> }}` to add or modify an entry"
            ))),
            Either::Map(m) => Ok(ArrayPathOp::Set {
                mode: m.mode,
                separator: m.separator,
            }),
        }
    }
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

/// Apply channel source-config patches to the parsed config in place.
///
/// Runs after parse and before `validate_config`, so the patched config is
/// the one validated and compiled. Each entry names a source node; an unknown
/// source name or an ill-formed op is a config error (E230–E240) reported
/// before the pipeline compiles.
pub fn apply_source_patches(
    config: &mut PipelineConfig,
    patches: &IndexMap<String, SourceConfigPatch>,
) -> Result<(), ConfigError> {
    // Whether the pipeline calls any composition. Sources declared inside a
    // composition body are not present in `config.nodes` at patch time (the
    // body is expanded only during compile), so a patch targeting one cannot be
    // resolved here — the unknown-source error explains that rather than
    // implying a typo. Computed once so the per-source lookup can stay a plain
    // borrow.
    let has_composition = config
        .nodes
        .iter()
        .any(|n| matches!(n.value, PipelineNode::Composition { .. }));

    for (src_name, patch) in patches {
        if patch.is_empty() {
            // Still resolve the source so an empty patch on a typo'd name
            // fails rather than passing silently.
            source_body_mut(config, src_name)
                .ok_or_else(|| unknown_source(src_name, has_composition))?;
            continue;
        }
        let body = source_body_mut(config, src_name)
            .ok_or_else(|| unknown_source(src_name, has_composition))?;
        apply_schema_ops(&mut body.schema, &patch.schema, src_name, None)?;
        apply_array_path_ops(&mut body.source, &patch.array_paths, src_name)?;
        apply_option_ops(&mut body.source, &patch.options, src_name)?;
        // Format-structure ops apply after the options merge so a keyed op
        // layers deterministically on top of an `options`-blob replacement of
        // the same declaration in the same patch.
        apply_nested_section_op(
            &mut body.source,
            patch.group_section.as_ref(),
            X12SectionSlot::Group,
            src_name,
        )?;
        apply_nested_section_op(
            &mut body.source,
            patch.set_section.as_ref(),
            X12SectionSlot::Set,
            src_name,
        )?;
        apply_split_field_ops(&mut body.source, &patch.split_fields, src_name)?;
    }
    Ok(())
}

/// Locate a source node's body by its node identity (`header.name`).
///
/// The channel system keys sources by node identity — the `name:` on the node
/// header — which can differ from the nested `config.name:`. Every other
/// channel block (`vars.source`, E111) resolves the same way, so the patch
/// target resolves by `header.name` for consistency.
fn source_body_mut<'a>(config: &'a mut PipelineConfig, name: &str) -> Option<&'a mut SourceBody> {
    config
        .nodes
        .iter_mut()
        .find_map(|spanned| match &mut spanned.value {
            PipelineNode::Source {
                header,
                config: body,
            } if header.name == name => Some(body),
            _ => None,
        })
}

fn unknown_source(src_name: &str, has_composition: bool) -> ConfigError {
    let hint = if has_composition {
        " (a source declared inside a composition body is not yet patchable by a \
         channel `sources:` block — patch the composition's own source, or lift it \
         to the top-level pipeline)"
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
    mut recorder: Option<&mut SchemaProvRecorder<'_>>,
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
}

fn apply_array_path_ops(
    source: &mut SourceConfig,
    ops: &IndexMap<String, ArrayPathOp>,
    src: &str,
) -> Result<(), ConfigError> {
    for (path, op) in ops {
        match op {
            ArrayPathOp::Remove => {
                let Some(entries) = source.array_paths.as_mut() else {
                    return Err(unknown_array_path(src, path));
                };
                let Some(idx) = entries.iter().position(|a| a.path == *path) else {
                    return Err(unknown_array_path(src, path));
                };
                entries.remove(idx);
            }
            ArrayPathOp::Set { mode, separator } => {
                let entries = source.array_paths.get_or_insert_with(Vec::new);
                if let Some(existing) = entries.iter_mut().find(|a| a.path == *path) {
                    // Partial modify: an omitted field keeps the current value,
                    // so setting only `separator` on a `join` entry does not
                    // silently revert its mode to explode.
                    if let Some(m) = mode {
                        existing.mode = m.clone();
                    }
                    if let Some(s) = separator {
                        existing.separator = Some(s.clone());
                    }
                } else {
                    // New entry: an omitted `mode` defaults to explode.
                    entries.push(ArrayPathConfig {
                        path: path.clone(),
                        mode: mode.clone().unwrap_or_default(),
                        separator: separator.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

fn unknown_array_path(src: &str, path: &str) -> ConfigError {
    ConfigError::Validation(format!(
        "[E234] channel array_paths patch on source '{src}': remove of unknown path '{path}'"
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

    /// A JSON source declaring a `record_path` option and one array-path entry.
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
      array_paths:
        - { path: items, mode: explode }
      schema:
        - { name: id, type: int }
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

    fn array_paths(config: &PipelineConfig) -> Vec<(String, ArrayMode, Option<String>)> {
        config
            .source_configs()
            .next()
            .unwrap()
            .array_paths
            .clone()
            .unwrap_or_default()
            .into_iter()
            .map(|a| (a.path, a.mode, a.separator))
            .collect()
    }

    #[test]
    fn array_paths_modify_existing_entry() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  items: { mode: join, separator: \";\" }\n"),
        )
        .unwrap();
        let entries = array_paths(&config);
        assert_eq!(entries.len(), 1);
        assert!(matches!(entries[0].1, ArrayMode::Join));
        assert_eq!(entries[0].2.as_deref(), Some(";"));
    }

    #[test]
    fn array_paths_add_new_entry() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  tags: { mode: explode }\n"),
        )
        .unwrap();
        let entries = array_paths(&config);
        assert_eq!(entries.len(), 2);
        assert!(entries.iter().any(|(p, _, _)| p == "tags"));
    }

    #[test]
    fn array_paths_partial_modify_keeps_omitted_mode() {
        let mut config = json_pipeline();
        // Make `items` a join entry...
        apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  items: { mode: join, separator: \";\" }\n"),
        )
        .unwrap();
        // ...then patch only the separator: the mode must stay join, not
        // silently revert to explode.
        apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  items: { separator: \"|\" }\n"),
        )
        .unwrap();
        let entries = array_paths(&config);
        assert_eq!(entries.len(), 1);
        assert!(
            matches!(entries[0].1, ArrayMode::Join),
            "mode must remain join, got {:?}",
            entries[0].1
        );
        assert_eq!(entries[0].2.as_deref(), Some("|"));
    }

    #[test]
    fn array_paths_new_entry_defaults_mode_explode() {
        let mut config = json_pipeline();
        // A new entry with no `mode` defaults to explode.
        apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  tags: { separator: \",\" }\n"),
        )
        .unwrap();
        let entries = array_paths(&config);
        let tags = entries.iter().find(|(p, _, _)| p == "tags").unwrap();
        assert!(matches!(tags.1, ArrayMode::Explode));
        assert_eq!(tags.2.as_deref(), Some(","));
    }

    #[test]
    fn array_paths_remove_entry() {
        let mut config = json_pipeline();
        apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  items: remove\n"),
        )
        .unwrap();
        assert!(array_paths(&config).is_empty());
    }

    #[test]
    fn array_paths_remove_unknown_errors() {
        let mut config = json_pipeline();
        let err = apply(
            &mut config,
            "src",
            patch_from_yaml("array_paths:\n  ghost: remove\n"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("E234"), "{err}");
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
}
