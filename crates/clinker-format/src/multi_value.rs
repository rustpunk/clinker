//! Multi-value field declarations carried by a source or sink.
//!
//! A column declared `multiple: true` in the schema holds several values; these
//! knobs say how a reader arrives at that shape and how a writer collapses it:
//! [`SplitToRows`] fans one record out to one record per occurrence,
//! [`SplitValues`] parses one delimited cell into several values (read), and
//! [`JoinValues`] joins several values back into one delimited cell (write).
//! `split_values` and `join_values` are deliberately distinct key names with a
//! shared `delimiter`/`escape` sub-vocabulary, so a declaration used on the
//! wrong side is a rejected no-op rather than a silent one.
//!
//! All live in the format layer next to [`crate::Column`] because they are
//! deserialized from the same author-written YAML and consumed by the same
//! readers and writers. Each accepts a bare field name or a full mapping, so — like
//! [`crate::SourceSchema`], the other sum over YAML node shapes in this crate —
//! they deserialize through a hand-written visitor dispatching on the node kind
//! rather than `#[serde(untagged)]`, which would buffer both shapes and collapse
//! every mistake into one message naming neither the offending key nor the
//! accepted forms.

use clinker_record::Value;
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
///   - field: notes                  # recover an escape-encoded write
///     delimiter: "|"
///     escape: "\\"
///   - field: payload                # recover a JSON-encoded write
///     json: true
/// ```
///
/// `escape` and `json` are the read-side inverses of the sink's [`JoinValues`]
/// `on_conflict: escape` / `encode_json` policies, so a value that could not
/// survive a plain delimited join round-trips exactly.
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct SplitValues {
    /// Flattened field name holding the delimited text.
    pub field: String,
    /// Separator the text is split on. Defaults to
    /// [`crate::schema::DEFAULT_VALUE_DELIMITER`].
    pub delimiter: String,
    /// Escape character honored when splitting: an escaped delimiter is a
    /// literal part of a value, not a boundary, and an escaped escape is a
    /// literal escape. Empty (the default) means a plain split with no escape
    /// handling. Set it to recover a cell a sink wrote under `join_values`
    /// `on_conflict: escape`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub escape: String,
    /// Read the whole cell as an embedded JSON array — the read-side inverse of
    /// the sink's `on_conflict: encode_json`. When `true`, `delimiter` and
    /// `escape` are unused: the cell is parsed as JSON and each array element
    /// becomes one value. Defaults to `false`.
    #[serde(default, skip_serializing_if = "is_false")]
    pub json: bool,
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
    /// A `split_values` entry using the default delimiter and no escape/JSON
    /// decoding — what the bare-scalar shorthand deserializes to.
    pub fn bare(field: impl Into<String>) -> Self {
        SplitValues {
            field: field.into(),
            delimiter: crate::schema::DEFAULT_VALUE_DELIMITER.to_string(),
            escape: String::new(),
            json: false,
        }
    }
}

/// The join-collision policy a sink applies when a value being joined into one
/// delimited CSV cell itself contains the delimiter.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnConflict {
    /// Refuse to emit a cell that would split back wrongly: the record is
    /// dead-lettered (naming the field and the offending value) rather than
    /// silently corrupted. The default — it is what makes a defaulted delimiter
    /// safe rather than merely convenient.
    #[default]
    Error,
    /// Prefix each delimiter (and each escape character) inside a value with the
    /// escape character, so a matching `split_values` `escape:` recovers the
    /// original. Lossless.
    Escape,
    /// Encode the whole field as an embedded JSON array, recovered by a matching
    /// `split_values` `json: true`. Lossless for any value, including ones
    /// carrying the delimiter, quotes, or newlines.
    EncodeJson,
}

/// One `join_values` entry: collapse a `multiple:` field into one delimited
/// CSV cell on write. The write-side inverse of [`SplitValues`].
///
/// Two accepted YAML shapes, freely mixable in one sequence:
///
/// ```yaml
/// join_values:
///   - tags                          # shorthand: delimiter ";", on_conflict: error
///   - field: notes                  # full form
///     delimiter: "|"
///     on_conflict: escape           # error | escape | encode_json
///     escape: "\\"                  # only meaningful with on_conflict: escape
/// ```
#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct JoinValues {
    /// Flattened field name whose values are joined into one cell.
    pub field: String,
    /// Separator written between values. Defaults to
    /// [`crate::schema::DEFAULT_VALUE_DELIMITER`].
    pub delimiter: String,
    /// What to do when a value being joined contains the delimiter.
    pub on_conflict: OnConflict,
    /// Escape character used under [`OnConflict::Escape`]. Defaults to a
    /// backslash. Ignored by the other policies.
    pub escape: String,
}

impl JoinValues {
    /// A `join_values` entry using the default delimiter, `on_conflict: error`,
    /// and the default escape — what the bare-scalar shorthand deserializes to.
    pub fn bare(field: impl Into<String>) -> Self {
        JoinValues {
            field: field.into(),
            delimiter: crate::schema::DEFAULT_VALUE_DELIMITER.to_string(),
            on_conflict: OnConflict::default(),
            escape: default_escape(),
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

/// Parse a delimited cell into the several values a `multiple:` column holds.
///
/// A scalar becomes an array of its delimiter-separated parts; an array (a
/// field that both repeats and carries delimited text) splits each element and
/// flattens the result, so the two declarations compose instead of fighting.
/// Empty parts are preserved — an author who declared the delimiter is the
/// authority on what sits between two of them. A fully empty cell is the
/// reader's own decision (a CSV blank means zero values, an XML empty element
/// means one), so callers special-case it before reaching here.
///
/// Format-agnostic over [`clinker_record::Value`]: the XML, CSV, and
/// fixed-width readers all share this one implementation.
pub(crate) fn split_text_value(value: &Value, delimiter: &str) -> Value {
    split_text_value_escaped(value, delimiter, "")
}

/// Like [`split_text_value`], but honoring an escape character: an escaped
/// delimiter is a literal part of a value, and an escaped escape is a literal
/// escape. This is the read-side inverse of a sink's `on_conflict: escape`
/// join, so a value that itself contained the delimiter round-trips exactly.
///
/// An empty `escape` reduces to a plain split, keeping the shared helper
/// byte-identical for every caller that declares no escape. Escape handling
/// treats `delimiter` and `escape` as single characters (their first `char`);
/// the plan-time gate rejects a multi-character escape so this stays exact.
pub(crate) fn split_text_value_escaped(value: &Value, delimiter: &str, escape: &str) -> Value {
    fn parts(text: &str, delimiter: &str, escape: &str) -> Vec<Value> {
        let (Some(esc), Some(delim)) = (escape.chars().next(), delimiter.chars().next()) else {
            // Empty escape (or empty delimiter): a plain split with no escape
            // handling, identical to `split_text_value`.
            return text
                .split(delimiter)
                .map(|p| Value::String(p.into()))
                .collect();
        };
        let mut out = Vec::new();
        let mut cur = String::new();
        let mut chars = text.chars();
        while let Some(c) = chars.next() {
            if c == esc {
                // The escape marks the NEXT character as literal (an escaped
                // delimiter or an escaped escape). A trailing escape with no
                // following character is kept as a literal escape.
                match chars.next() {
                    Some(next) => cur.push(next),
                    None => cur.push(esc),
                }
            } else if c == delim {
                out.push(Value::String(std::mem::take(&mut cur).into()));
            } else {
                cur.push(c);
            }
        }
        out.push(Value::String(cur.into()));
        out
    }
    match value {
        Value::String(s) => Value::Array(parts(s.as_str(), delimiter, escape)),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .flat_map(|item| match item {
                    Value::String(s) => parts(s.as_str(), delimiter, escape),
                    other => vec![other.clone()],
                })
                .collect(),
        ),
        other => Value::Array(vec![other.clone()]),
    }
}

/// `keep_empty`'s default. Named rather than inlined so the inverted-industry
/// default has exactly one definition.
fn default_keep_empty() -> bool {
    true
}

fn default_delimiter() -> String {
    crate::schema::DEFAULT_VALUE_DELIMITER.to_string()
}

/// The default `join_values` / `split_values` escape character. A single
/// backslash, the near-universal escape convention, defined once so the write
/// and read sides never drift.
fn default_escape() -> String {
    "\\".to_string()
}

/// `skip_serializing_if` predicate for a `false` boolean flag.
fn is_false(b: &bool) -> bool {
    !*b
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
            #[serde(default)]
            escape: String,
            #[serde(default)]
            json: bool,
        }

        struct V;
        impl<'de> de::Visitor<'de> for V {
            type Value = SplitValues;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a `split_values` entry: either a field name (`tags`) or a map \
                     `{ field: <name>, delimiter: <str>, escape: <str>, json: <bool> }`",
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
                    escape: full.escape,
                    json: full.json,
                })
            }
        }

        d.deserialize_any(V)
    }
}

/// Deserialize the `join_values` shorthand grammar. Same two-way union and
/// same rationale as [`SplitToRows`]'s.
impl<'de> Deserialize<'de> for JoinValues {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Full {
            field: String,
            #[serde(default = "default_delimiter")]
            delimiter: String,
            #[serde(default)]
            on_conflict: OnConflict,
            #[serde(default = "default_escape")]
            escape: String,
        }

        struct V;
        impl<'de> de::Visitor<'de> for V {
            type Value = JoinValues;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str(
                    "a `join_values` entry: either a field name (`tags`) or a map \
                     `{ field: <name>, delimiter: <str>, on_conflict: error|escape|encode_json, \
                     escape: <str> }`",
                )
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
                Ok(JoinValues::bare(v))
            }

            fn visit_map<A: de::MapAccess<'de>>(self, map: A) -> Result<Self::Value, A::Error> {
                let full = Full::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(JoinValues {
                    field: full.field,
                    delimiter: full.delimiter,
                    on_conflict: full.on_conflict,
                    escape: full.escape,
                })
            }
        }

        d.deserialize_any(V)
    }
}
