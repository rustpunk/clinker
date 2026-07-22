//! Multi-value field declarations carried by a source.
//!
//! A column declared `multiple: true` in the schema holds several values; these
//! two source-level knobs say how a reader arrives at that shape:
//! [`SplitToRows`] fans one record out to one record per occurrence, and
//! [`SplitValues`] parses one delimited cell into several values.
//!
//! Both live in the format layer next to [`crate::Column`] because they are
//! deserialized from the same author-written YAML and consumed by the same
//! readers. Each accepts a bare field name or a full mapping, so — like
//! [`crate::SourceSchema`], the other sum over YAML node shapes in this crate —
//! they deserialize through a hand-written visitor dispatching on the node kind
//! rather than `#[serde(untagged)]`, which would buffer both shapes and collapse
//! every mistake into one message naming neither the offending key nor the
//! accepted forms.

use serde::de::{self};
use serde::{Deserialize, Deserializer, Serialize};

/// One `split_to_rows` entry: fan a record out to one record per occurrence
/// of a repeated field.
///
/// Two accepted YAML shapes, freely mixable in one sequence:
///
/// ```yaml
/// split_to_rows:
///   - line_items                    # shorthand: the field name, all defaults
///   - field: tags                   # full form
///     keep_empty: true
///     mode: extract
///     position_column: line_no
/// ```
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SplitToRows {
    /// Flattened field name of the repeated element — the same dotted form the
    /// source's field names use (`line_items`, `Items.Item`).
    pub field: String,
    /// Whether a record whose field is empty or absent survives the fan-out.
    /// Defaults to **true**: a vanished row is the costliest failure mode for
    /// this audience, so dropping is opt-in rather than the default several
    /// widely deployed engines chose.
    pub keep_empty: bool,
    /// Whether the occurrence becomes the record or the record shape is kept.
    pub mode: SplitToRowsMode,
    /// Optional column receiving each occurrence's 1-based position.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_column: Option<String>,
}

/// How one occurrence is projected onto its output record.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SplitToRowsMode {
    /// The occurrence becomes the record: its own fields are lifted out from
    /// under the field name and every field outside the group is merged onto
    /// each output. The default — it is the shape most authors want, and the
    /// other one is a second transform away in every tool that offers only it.
    #[default]
    Extract,
    /// The record shape is preserved: the occurrence's fields keep their
    /// dotted path under the field name, and each output carries exactly one
    /// occurrence.
    Split,
}

/// One `split_values` entry: parse a delimited string field into several
/// values. The field must be declared `multiple: true` in the source schema.
///
/// Two accepted YAML shapes, freely mixable in one sequence:
///
/// ```yaml
/// split_values:
///   - tags                          # shorthand: the field name, default delimiter
///   - field: codes                  # full form
///     delimiter: "|"
/// ```
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SplitValues {
    /// Flattened field name holding the delimited text.
    pub field: String,
    /// Separator the text is split on. Defaults to
    /// [`crate::schema::DEFAULT_VALUE_DELIMITER`].
    pub delimiter: String,
}

impl SplitToRows {
    /// A `split_to_rows` entry with every optional attribute at its default —
    /// what the bare-scalar shorthand deserializes to.
    pub fn bare(field: impl Into<String>) -> Self {
        SplitToRows {
            field: field.into(),
            keep_empty: default_keep_empty(),
            mode: SplitToRowsMode::default(),
            position_column: None,
        }
    }
}

impl SplitValues {
    /// A `split_values` entry using the default delimiter — what the
    /// bare-scalar shorthand deserializes to.
    pub fn bare(field: impl Into<String>) -> Self {
        SplitValues {
            field: field.into(),
            delimiter: crate::schema::DEFAULT_VALUE_DELIMITER.to_string(),
        }
    }
}

/// True when a flattened key sits at or under a declared field path: the key
/// IS the path (the element's own text) or extends it past a dot (its children
/// and attributes). Also the nesting test between two declared fields, since a
/// field name is itself a dotted element path. The XML reader's key-to-group
/// membership and the plan-time disjointness gate (E358) both read this one
/// predicate, so the rule the gate enforces is the rule the reader relies on.
pub fn under_field_path(key: &str, path: &str) -> bool {
    key.strip_prefix(path)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
}

/// `keep_empty`'s default. Named rather than inlined so the inverted-industry
/// default has exactly one definition.
fn default_keep_empty() -> bool {
    true
}

fn default_delimiter() -> String {
    crate::schema::DEFAULT_VALUE_DELIMITER.to_string()
}

/// Deserialize the `split_to_rows` shorthand grammar: a **two-way** union of a
/// bare scalar (the field name, every other attribute defaulted) and the full
/// mapping. Both forms may appear in the same sequence.
///
/// Hand-written rather than `#[serde(untagged)]` on purpose. An untagged enum
/// buffers through a span-losing intermediate and collapses every mistake — a
/// typo'd key, a wrong value type, a missing `field` — into one message that
/// names neither the offending key nor the accepted shapes. Dispatching on the
/// YAML node kind keeps the inner struct's own `deny_unknown_fields` error
/// (which names the bad key and the valid ones) intact, and a node that is
/// neither shape gets a message naming both accepted forms.
impl<'de> Deserialize<'de> for SplitToRows {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Full {
            field: String,
            #[serde(default = "default_keep_empty")]
            keep_empty: bool,
            #[serde(default)]
            mode: SplitToRowsMode,
            #[serde(default)]
            position_column: Option<String>,
        }

        struct V;
        impl<'de> de::Visitor<'de> for V {
            type Value = SplitToRows;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a `split_to_rows` entry: either a field name (`line_items`) or a map \
                     `{ field: <name>, keep_empty: <bool>, mode: extract|split, \
                     position_column: <name> }`",
                )
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(SplitToRows::bare(v))
            }

            fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                let full = Full::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(SplitToRows {
                    field: full.field,
                    keep_empty: full.keep_empty,
                    mode: full.mode,
                    position_column: full.position_column,
                })
            }
        }

        d.deserialize_any(V)
    }
}

/// Deserialize the `split_values` shorthand grammar. Same two-way union and
/// same rationale as [`SplitToRows`]'s.
impl<'de> Deserialize<'de> for SplitValues {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Full {
            field: String,
            #[serde(default = "default_delimiter")]
            delimiter: String,
        }

        struct V;
        impl<'de> de::Visitor<'de> for V {
            type Value = SplitValues;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a `split_values` entry: either a field name (`tags`) or a map \
                     `{ field: <name>, delimiter: <str> }`",
                )
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(SplitValues::bare(v))
            }

            fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                let full = Full::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(SplitValues {
                    field: full.field,
                    delimiter: full.delimiter,
                })
            }
        }

        d.deserialize_any(V)
    }
}
