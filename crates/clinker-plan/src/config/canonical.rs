//! Canonical, byte-preserving edits of authored pipeline configuration.
//!
//! In addition to multi-value shorthand expansion, this module owns the narrow
//! edit plan used by `clinker guess --write`: canonical YAML spans and typed
//! schema addresses locate one directly-authored `numeric` leaf, and a reparsed
//! semantic comparison proves that replacing it changes nothing else.
//!
//! `clinker config --resolved` prints a pipeline config with the multi-value
//! shorthand materialized: the bare-field forms of `split_to_rows:`,
//! `split_values:`, and `join_values:` rewritten to their full mappings with
//! every default spelled out. The rewrite is **surgical** — only the bytes of
//! those shorthand sequences change; comments, key order, indentation, and every
//! other surface of the document are preserved verbatim. A full `serde`
//! reserialize would reorder keys, drop comments, and reformat unrelated
//! surfaces, which is why the expansion works on the raw text and touches
//! nothing it does not have to.
//!
//! ## How the surfaces are located
//!
//! The three shorthand types already normalize to full form at deserialize time
//! (a bare scalar fills every default; the derived `Serialize` re-emits the full
//! mapping), so the canonical value of each sequence is just its parsed
//! [`Vec`]. To find *where* each sequence lives in the source, the raw YAML is
//! parsed a second time into a lenient probe whose shorthand fields are wrapped
//! in [`Spanned`]: the span's byte offset is the reliable START of the sequence
//! (the first `-` of a block sequence, or the `[` of a flow one). serde-saphyr
//! does not report a container's END, so the end of each sequence is derived by
//! scanning — bracket matching for flow, an indentation walk for block. The
//! probe uses plain nested structs (never a `#[serde(tag)]`/`flatten` context),
//! which is the documented way to keep `Spanned` locations from collapsing to
//! `UNKNOWN`.
//!
//! Schema columns carry no shorthand: a `multiple: true` column is always
//! written explicitly (the plan-time gates reject an implied one), so the schema
//! block is already canonical and is left byte-identical.

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use clinker_format::{JoinValues, SplitToRows, SplitValues};
use cxl::typecheck::Type;

use super::composition::ScopedSchemaLeafAddress;
use super::{PipelineConfig, PipelineNode, SourceSchema};
use crate::yaml::{self, Spanned};

/// Failure canonicalizing a config's multi-value shorthand.
#[derive(Debug)]
pub enum CanonicalError {
    /// The source did not parse as YAML.
    Parse(String),
    /// A located shorthand sequence could not be re-serialized or its extent
    /// could not be resolved. An invariant violation for input that parsed —
    /// surfaced loudly rather than emitting a silently-wrong document.
    Internal(String),
}

impl std::fmt::Display for CanonicalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CanonicalError::Parse(m) => write!(f, "could not parse config: {m}"),
            CanonicalError::Internal(m) => write!(f, "internal canonicalization error: {m}"),
        }
    }
}

impl std::error::Error for CanonicalError {}

/// Concrete type an authoring tool may substitute for one literal `numeric`
/// source-schema leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConcreteNumericType {
    Int,
    Float,
}

impl ConcreteNumericType {
    fn token(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::Float => "float",
        }
    }

    fn ty(self) -> Type {
        match self {
            Self::Int => Type::Int,
            Self::Float => Type::Float,
        }
    }
}

/// Why a source-schema owner cannot be edited in its authored document.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumericEditIneligibility {
    /// No inline authored leaf owns the effective address.
    MissingOwner,
    /// More than one authored leaf resolved to the address.
    AmbiguousOwner,
    /// An alias or another indirect YAML provenance path owns the value.
    IndirectProvenance,
    /// The effective numeric value is not the exact literal token in the raw
    /// document (for example, it came from interpolation or an anchor).
    NonLiteralToken,
}

impl NumericEditIneligibility {
    /// Stable report spelling for this ineligibility.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MissingOwner => "owner_not_inline",
            Self::AmbiguousOwner => "owner_ambiguous",
            Self::IndirectProvenance => "owner_indirect_provenance",
            Self::NonLiteralToken => "owner_not_literal_numeric",
        }
    }
}

/// One exact byte edit for an inline, directly-authored `numeric` type leaf.
///
/// The range covers only the seven bytes of the `numeric` scalar, including
/// when it is nested under `nullable:`. Applying it therefore cannot change a
/// column's nullability or any sibling attribute.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NumericTypeEdit {
    start: usize,
    end: usize,
    replacement: ConcreteNumericType,
}

impl NumericTypeEdit {
    /// Start byte of the exact authored `numeric` token.
    pub fn start(&self) -> usize {
        self.start
    }

    /// End byte (exclusive) of the exact authored `numeric` token.
    pub fn end(&self) -> usize {
        self.end
    }

    /// Apply this edit only when the raw bytes still contain the token that was
    /// planned. A changed token is rejected as a compare-and-swap mismatch.
    pub fn apply(&self, raw: &str) -> Result<String, CanonicalError> {
        if raw.get(self.start..self.end) != Some("numeric") {
            return Err(CanonicalError::Internal(
                "numeric edit token changed after it was located".to_owned(),
            ));
        }
        let mut edited = raw.to_owned();
        edited.replace_range(self.start..self.end, self.replacement.token());
        Ok(edited)
    }
}

/// Result of locating one exact source-schema owner in authored YAML.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NumericTypeEditDecision {
    Editable(NumericTypeEdit),
    Ineligible(NumericEditIneligibility),
}

#[derive(Deserialize)]
struct NumericDocProbe {
    #[serde(default)]
    nodes: Vec<NumericNodeProbe>,
}

#[derive(Deserialize)]
struct NumericNodeProbe {
    #[serde(default, rename = "type")]
    kind: Option<Spanned<String>>,
    #[serde(default)]
    name: Option<Spanned<String>>,
    #[serde(default)]
    config: Option<NumericConfigProbe>,
}

#[derive(Deserialize)]
struct NumericConfigProbe {
    #[serde(default)]
    schema: Option<Spanned<NumericSchemaProbe>>,
}

enum NumericSchemaProbe {
    Columns(Vec<NumericColumnProbe>),
    Mapping(NumericMultiRecordProbe),
    Other,
}

impl<'de> Deserialize<'de> for NumericSchemaProbe {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SchemaVisitor;

        impl<'de> Visitor<'de> for SchemaVisitor {
            type Value = NumericSchemaProbe;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a source schema")
            }

            fn visit_seq<A>(self, seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                Vec::<NumericColumnProbe>::deserialize(de::value::SeqAccessDeserializer::new(seq))
                    .map(NumericSchemaProbe::Columns)
            }

            fn visit_map<A>(self, map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                NumericMultiRecordProbe::deserialize(de::value::MapAccessDeserializer::new(map))
                    .map(NumericSchemaProbe::Mapping)
            }

            fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(NumericSchemaProbe::Other)
            }
        }

        deserializer.deserialize_any(SchemaVisitor)
    }
}

#[derive(Deserialize)]
struct NumericMultiRecordProbe {
    #[serde(default, rename = "records")]
    record_types: Vec<NumericRecordProbe>,
}

#[derive(Deserialize)]
struct NumericRecordProbe {
    id: Spanned<String>,
    #[serde(default)]
    columns: Vec<NumericColumnProbe>,
}

#[derive(Deserialize)]
struct NumericColumnProbe {
    name: Spanned<String>,
    #[serde(default, rename = "type")]
    ty: Option<Spanned<AuthoredTypeProbe>>,
}

enum AuthoredTypeProbe {
    Atomic(String),
    Nullable(Box<Spanned<AuthoredTypeProbe>>),
    Other,
}

impl AuthoredTypeProbe {
    fn numeric_leaf(&self) -> Option<&Spanned<AuthoredTypeProbe>> {
        match self {
            Self::Nullable(inner) => match &inner.value {
                Self::Atomic(value) if value == "numeric" => Some(inner),
                nested => nested.numeric_leaf(),
            },
            Self::Atomic(_) | Self::Other => None,
        }
    }
}

impl<'de> Deserialize<'de> for AuthoredTypeProbe {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TypeVisitor;

        impl<'de> Visitor<'de> for TypeVisitor {
            type Value = AuthoredTypeProbe;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a source column type")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(AuthoredTypeProbe::Atomic(value.to_owned()))
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut nullable = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == "nullable" && nullable.is_none() {
                        nullable = Some(map.next_value::<Spanned<AuthoredTypeProbe>>()?);
                    } else {
                        let _ = map.next_value::<de::IgnoredAny>()?;
                    }
                }
                Ok(nullable
                    .map(|inner| AuthoredTypeProbe::Nullable(Box::new(inner)))
                    .unwrap_or(AuthoredTypeProbe::Other))
            }
        }

        deserializer.deserialize_any(TypeVisitor)
    }
}

struct LocatedNumeric<'a> {
    kind: &'a Spanned<String>,
    source: &'a Spanned<String>,
    schema: &'a Spanned<NumericSchemaProbe>,
    record: Option<&'a Spanned<String>>,
    column: &'a Spanned<String>,
    authored_type: &'a Spanned<AuthoredTypeProbe>,
    leaf: &'a Spanned<AuthoredTypeProbe>,
}

/// Locate the exact directly-authored `numeric` leaf for `address`.
///
/// This is read-only planning: it parses through the canonical YAML boundary,
/// retains no source-sized state beyond that parser's already-capped document,
/// and refuses aliases, interpolation, external schemas, and ambiguous owners.
pub fn plan_numeric_type_edit(
    raw: &str,
    address: &ScopedSchemaLeafAddress,
    replacement: ConcreteNumericType,
) -> Result<NumericTypeEditDecision, CanonicalError> {
    let doc: NumericDocProbe =
        yaml::from_str(raw).map_err(|error| CanonicalError::Parse(error.to_string()))?;
    let mut located = Vec::new();
    for node in &doc.nodes {
        let (Some(kind), Some(source), Some(config)) = (&node.kind, &node.name, &node.config)
        else {
            continue;
        };
        if kind.value != "source" || source.value != address.source() {
            continue;
        }
        let Some(schema) = &config.schema else {
            continue;
        };
        match (&schema.value, address.record()) {
            (NumericSchemaProbe::Columns(columns), None) => {
                locate_column(kind, source, schema, None, columns, address, &mut located);
            }
            (NumericSchemaProbe::Mapping(mapping), Some(record_name)) => {
                for record in &mapping.record_types {
                    if record.id.value == record_name {
                        locate_column(
                            kind,
                            source,
                            schema,
                            Some(&record.id),
                            &record.columns,
                            address,
                            &mut located,
                        );
                    }
                }
            }
            (NumericSchemaProbe::Columns(_), Some(_))
            | (NumericSchemaProbe::Mapping(_), None)
            | (NumericSchemaProbe::Other, _) => {}
        }
    }
    let owner = match located.as_slice() {
        [] => {
            return Ok(NumericTypeEditDecision::Ineligible(
                NumericEditIneligibility::MissingOwner,
            ));
        }
        [owner] => owner,
        _ => {
            return Ok(NumericTypeEditDecision::Ineligible(
                NumericEditIneligibility::AmbiguousOwner,
            ));
        }
    };
    if !is_direct(owner.kind)
        || !is_direct(owner.source)
        || !is_direct(owner.schema)
        || owner.record.is_some_and(|record| !is_direct(record))
        || !is_direct(owner.column)
        || !is_direct(owner.authored_type)
        || !is_direct(owner.leaf)
    {
        return Ok(NumericTypeEditDecision::Ineligible(
            NumericEditIneligibility::IndirectProvenance,
        ));
    }
    let Some(start) = exact_numeric_start(raw, owner.leaf) else {
        return Ok(NumericTypeEditDecision::Ineligible(
            NumericEditIneligibility::NonLiteralToken,
        ));
    };
    Ok(NumericTypeEditDecision::Editable(NumericTypeEdit {
        start,
        end: start + "numeric".len(),
        replacement,
    }))
}

fn locate_column<'a>(
    kind: &'a Spanned<String>,
    source: &'a Spanned<String>,
    schema: &'a Spanned<NumericSchemaProbe>,
    record: Option<&'a Spanned<String>>,
    columns: &'a [NumericColumnProbe],
    address: &ScopedSchemaLeafAddress,
    located: &mut Vec<LocatedNumeric<'a>>,
) {
    for column in columns {
        if column.name.value != address.column_name() {
            continue;
        }
        let Some(ty) = &column.ty else { continue };
        let leaf = match &ty.value {
            AuthoredTypeProbe::Atomic(value) if value == "numeric" => ty,
            nested => nested.numeric_leaf().unwrap_or(ty),
        };
        located.push(LocatedNumeric {
            kind,
            source,
            schema,
            record,
            column: &column.name,
            authored_type: ty,
            leaf,
        });
    }
}

fn is_direct<T>(spanned: &Spanned<T>) -> bool {
    spanned.referenced == spanned.defined
}

fn exact_numeric_start(raw: &str, leaf: &Spanned<AuthoredTypeProbe>) -> Option<usize> {
    let reported = leaf.referenced.span().byte_offset()? as usize;
    if reported > raw.len() {
        return None;
    }
    let line_start = raw[..reported].rfind('\n').map_or(0, |index| index + 1);
    if raw.get(line_start..reported)?.contains('&') {
        return None;
    }
    if raw.get(reported..reported + "numeric".len()) == Some("numeric") {
        return Some(reported);
    }
    if raw
        .as_bytes()
        .get(reported)
        .is_some_and(|byte| matches!(byte, b'\'' | b'"'))
        && raw.get(reported + 1..reported + 1 + "numeric".len()) == Some("numeric")
    {
        return Some(reported + 1);
    }
    None
}

/// Hash the typed effective configuration while excluding source spans and the
/// raw-byte `source_hash`. Equal digests therefore mean equal executable
/// author semantics even when formatting differs.
pub fn semantic_config_digest(config: &PipelineConfig) -> Result<[u8; 32], CanonicalError> {
    let nodes = config
        .nodes
        .iter()
        .map(|node| &node.value)
        .collect::<Vec<_>>();
    let serialized = serde_json::to_vec(&(
        &config.pipeline,
        nodes,
        &config.error_handling,
        &config.notes,
        &config.body_source_patches,
    ))
    .map_err(|error| {
        CanonicalError::Internal(format!(
            "cannot serialize effective config semantics: {error}"
        ))
    })?;
    Ok(*blake3::hash(&serialized).as_bytes())
}

/// Reparse a proposed edit and prove its only semantic change is the selected
/// numeric leaf becoming `int` or `float`.
pub fn prove_numeric_type_only_change(
    original: &str,
    edited: &str,
    address: &ScopedSchemaLeafAddress,
    replacement: ConcreteNumericType,
) -> Result<(), CanonicalError> {
    let expected =
        super::parse_config(original).map_err(|error| CanonicalError::Parse(error.to_string()))?;
    let actual =
        super::parse_config(edited).map_err(|error| CanonicalError::Parse(error.to_string()))?;
    prove_resolved_numeric_type_only_change(&expected, &actual, address, replacement)
}

/// Prove that two already-resolved effective configurations differ only at the
/// selected numeric type leaf.
///
/// Callers use this after resolving a staged sibling file, immediately before
/// publication, so external schema content and every other effective semantic
/// field participate in the comparison while raw-byte identity remains a
/// separate compare-and-swap check.
pub fn prove_resolved_numeric_type_only_change(
    original: &PipelineConfig,
    edited: &PipelineConfig,
    address: &ScopedSchemaLeafAddress,
    replacement: ConcreteNumericType,
) -> Result<(), CanonicalError> {
    let mut expected = original.clone();
    let changed = replace_numeric_owner(&mut expected, address, replacement.ty());
    if changed != 1 {
        return Err(CanonicalError::Internal(format!(
            "expected one numeric owner for {}, found {changed}",
            address.render()
        )));
    }
    if semantic_config_digest(&expected)? != semantic_config_digest(edited)? {
        return Err(CanonicalError::Internal(
            "edited config changes semantics outside the intended numeric type leaf".to_owned(),
        ));
    }
    Ok(())
}

fn replace_numeric_owner(
    config: &mut PipelineConfig,
    address: &ScopedSchemaLeafAddress,
    replacement: Type,
) -> usize {
    let mut changed = 0;
    for node in &mut config.nodes {
        let PipelineNode::Source { header, config } = &mut node.value else {
            continue;
        };
        if header.name != address.source() {
            continue;
        }
        match (&mut config.schema, address.record()) {
            (SourceSchema::Columns(columns), None) => {
                changed += replace_column_type(columns, address.column_name(), &replacement);
            }
            (SourceSchema::MultiRecord { record_types, .. }, Some(record_name)) => {
                for record in record_types
                    .iter_mut()
                    .filter(|record| record.id == record_name)
                {
                    changed += replace_column_type(
                        &mut record.columns,
                        address.column_name(),
                        &replacement,
                    );
                }
            }
            _ => {}
        }
    }
    changed
}

fn replace_column_type(
    columns: &mut [clinker_format::Column],
    column_name: &str,
    replacement: &Type,
) -> usize {
    let mut changed = 0;
    for column in columns
        .iter_mut()
        .filter(|column| column.name == column_name)
    {
        if replace_numeric_leaf(&mut column.ty, replacement) {
            changed += 1;
        }
    }
    changed
}

fn replace_numeric_leaf(ty: &mut Type, replacement: &Type) -> bool {
    match ty {
        Type::Numeric => {
            *ty = replacement.clone();
            true
        }
        Type::Nullable(inner) => replace_numeric_leaf(inner, replacement),
        _ => false,
    }
}

/// Lenient view of a pipeline document that captures only the multi-value
/// shorthand sequences, each wrapped in [`Spanned`] to recover its byte offset.
///
/// Unknown fields are ignored (no `deny_unknown_fields`), so the same bytes the
/// strict [`super::PipelineConfig`] loader accepts parse here too, and any node
/// shape — source, transform, output — flows through with its irrelevant fields
/// discarded.
#[derive(Deserialize)]
struct DocProbe {
    #[serde(default)]
    nodes: Vec<NodeProbe>,
}

#[derive(Deserialize)]
struct NodeProbe {
    #[serde(default)]
    config: Option<ConfigProbe>,
}

#[derive(Deserialize)]
struct ConfigProbe {
    #[serde(default)]
    split_to_rows: Option<Spanned<Vec<SplitToRows>>>,
    #[serde(default)]
    split_values: Option<Spanned<Vec<SplitValues>>>,
    #[serde(default)]
    join_values: Option<Spanned<Vec<JoinValues>>>,
}

/// One byte-range replacement: swap `raw[start..end]` for `rendered`.
struct Edit {
    start: usize,
    end: usize,
    rendered: String,
}

/// Rewrite `raw` so every `split_to_rows` / `split_values` / `join_values`
/// shorthand sequence is expanded to its canonical full-mapping form, leaving
/// all other bytes untouched.
///
/// The output parses to a plan semantically identical to the input, and
/// re-running the expansion on the output is a no-op (each rendered sequence
/// re-parses to the same value and re-renders identically).
pub fn expand_multi_value_shorthand(raw: &str) -> Result<String, CanonicalError> {
    let doc: DocProbe = yaml::from_str(raw).map_err(|e| CanonicalError::Parse(e.to_string()))?;

    // Match the document's line ending so a CRLF file is not spliced with lone
    // `\n`s. `to_string` always emits `\n`; the renderer rewrites them.
    let newline = if raw.contains("\r\n") { "\r\n" } else { "\n" };

    let mut edits: Vec<Edit> = Vec::new();
    for node in &doc.nodes {
        let Some(cfg) = &node.config else { continue };
        push_edit(raw, cfg.split_to_rows.as_ref(), newline, &mut edits)?;
        push_edit(raw, cfg.split_values.as_ref(), newline, &mut edits)?;
        push_edit(raw, cfg.join_values.as_ref(), newline, &mut edits)?;
    }

    // Apply the replacements from the end of the document backwards so each
    // splice leaves every not-yet-applied (earlier) offset valid.
    edits.sort_by(|a, b| b.start.cmp(&a.start));
    for pair in edits.windows(2) {
        // Sorted descending: pair[0] is the later span. The two shorthand
        // surfaces are disjoint by construction; a violation means the extent
        // scan overran, which would corrupt the document — fail instead.
        if pair[1].end > pair[0].start {
            return Err(CanonicalError::Internal(
                "shorthand sequence extents overlap".to_string(),
            ));
        }
    }

    let mut out = raw.to_string();
    for edit in &edits {
        out.replace_range(edit.start..edit.end, &edit.rendered);
    }
    Ok(out)
}

/// Where a shorthand sequence's real opening token lives, relative to the
/// offset the parser reported for its value.
#[derive(Debug)]
enum SequenceStart {
    /// Splice the region beginning at this byte — guaranteed to be `-` or `[`.
    Splice(usize),
    /// The value is a YAML alias (`*anchor`); leave it byte-identical.
    PassThrough,
    /// The region does not begin with a sequence token and is not an alias —
    /// refuse to splice rather than risk corrupting it.
    Unexpected(String),
}

/// Resolve the reported value offset to the sequence's actual opening token.
///
/// serde-saphyr does not always report the opening `-` / `[`:
///
/// - a flow sequence reports the `[` itself (inline or on its own line);
/// - a YAML alias reports the `*anchor` use-site token;
/// - an anchored sequence definition (`key: &anchor` then the sequence) reports
///   its content token in this parser version, but the reported offset landing
///   on the `&anchor` token is handled defensively — the anchor is skipped to
///   the real `-` / `[` so a valid anchored config is never refused;
/// - a block sequence reports the `-` when the item is indented deeper than its
///   key, but the first item's *value* (two bytes past `- `) when the dash sits
///   at the key's own indent — the common `key:\n- item` form.
///
/// In every block case the dash lives at the indent of the line the reported
/// offset falls on, so recovering it is a fixed rule rather than a guess.
fn resolve_sequence_start(raw: &str, reported: usize) -> SequenceStart {
    let bytes = raw.as_bytes();
    match bytes[reported] {
        b'[' => SequenceStart::Splice(reported),
        b'*' => SequenceStart::PassThrough,
        b'&' => resolve_anchor_start(raw, reported),
        _ => {
            let line_start = raw[..reported].rfind('\n').map(|i| i + 1).unwrap_or(0);
            let after = &raw[line_start..];
            let indent = after.len() - after.trim_start_matches(' ').len();
            let dash = line_start + indent;
            let is_item = bytes.get(dash) == Some(&b'-')
                && matches!(
                    bytes.get(dash + 1).copied(),
                    None | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r')
                );
            if dash <= reported && is_item {
                SequenceStart::Splice(dash)
            } else {
                SequenceStart::Unexpected(format!(
                    "shorthand sequence at offset {reported} does not begin with a '-' or \
                     '[' token"
                ))
            }
        }
    }
}

/// Resolve a reported offset that lands on an anchor token (`&anchor`) to the
/// sequence's real opening `-` / `[`.
///
/// Skips the anchor name and the whitespace / line break that separates it from
/// the value, then reports the sequence token that follows. A shape with no
/// `-` / `[` after the anchor is passed through byte-identical rather than
/// erroring: an anchored value that already parsed is never corrupted, and the
/// unexpanded sequence still re-parses as authored.
fn resolve_anchor_start(raw: &str, anchor_at: usize) -> SequenceStart {
    let bytes = raw.as_bytes();
    // Skip `&` and the anchor name. A YAML anchor name runs until whitespace,
    // a line break, or a flow indicator.
    let mut i = anchor_at + 1;
    while i < bytes.len()
        && !matches!(
            bytes[i],
            b' ' | b'\t' | b'\r' | b'\n' | b'[' | b']' | b'{' | b'}' | b','
        )
    {
        i += 1;
    }
    // Skip the whitespace / newline between the anchor and its value.
    while i < bytes.len() && matches!(bytes[i], b' ' | b'\t' | b'\r' | b'\n') {
        i += 1;
    }
    match bytes.get(i) {
        Some(b'[') => SequenceStart::Splice(i),
        Some(b'-')
            if matches!(
                bytes.get(i + 1).copied(),
                None | Some(b' ') | Some(b'\t') | Some(b'\n') | Some(b'\r')
            ) =>
        {
            SequenceStart::Splice(i)
        }
        _ => SequenceStart::PassThrough,
    }
}

/// Plan the replacement for one located shorthand sequence and push it onto
/// `edits`. A `None` sequence (the key was absent) or an empty one (nothing to
/// expand) contributes nothing.
fn push_edit<T: Serialize>(
    raw: &str,
    spanned: Option<&Spanned<Vec<T>>>,
    newline: &str,
    edits: &mut Vec<Edit>,
) -> Result<(), CanonicalError> {
    let Some(sp) = spanned else { return Ok(()) };
    if sp.value.is_empty() {
        return Ok(());
    }
    // Byte offsets are always populated for a string source (they are absent
    // only when parsing from a reader), so a missing one is an invariant break.
    let reported = sp.referenced.span().byte_offset().ok_or_else(|| {
        CanonicalError::Internal("shorthand sequence has no source byte offset".to_string())
    })? as usize;
    if reported >= raw.len() {
        return Err(CanonicalError::Internal(
            "shorthand sequence offset past end of source".to_string(),
        ));
    }

    // The reported offset is not always the opening token (a same-indent dash's
    // value, or an alias use-site), so normalize it and refuse to splice
    // anything that is not a real sequence token.
    let start = match resolve_sequence_start(raw, reported) {
        SequenceStart::Splice(pos) => pos,
        SequenceStart::PassThrough => return Ok(()),
        SequenceStart::Unexpected(why) => return Err(CanonicalError::Internal(why)),
    };

    // Hard guard: never splice a region that does not open with a block (`-`)
    // or flow (`[`) token. Splicing anywhere else is exactly the corruption the
    // normalization above exists to prevent, so fail loudly if it is reached.
    let first = raw.as_bytes()[start];
    if first != b'-' && first != b'[' {
        return Err(CanonicalError::Internal(format!(
            "resolved sequence start at offset {start} is '{}', not '-' or '['",
            first as char
        )));
    }

    let end = if first == b'[' {
        flow_sequence_end(raw, start)?
    } else {
        block_sequence_end(raw, start)
    };

    // Regenerating a sequence discards any comment or blank line interleaved
    // among its items. Rather than silently drop an author's comment, leave a
    // sequence carrying interior comments/blanks untouched: it stays valid and
    // re-parses identically — only its shorthand is not expanded.
    if region_has_interior_noise(&raw[start..end]) {
        return Ok(());
    }

    let line_start = raw[..start].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let prefix = &raw[line_start..start];
    let rendered = if first == b'[' && !prefix.bytes().all(|b| b == b' ') {
        // A flow sequence inline after `key:` on one line: a block value is not
        // legal there, so keep it a flow sequence.
        render_flow(&sp.value)?
    } else {
        // A block sequence, or a flow sequence opening its own line — both
        // render as a canonical block sequence indented under `prefix`.
        render_block(&sp.value, prefix.len(), newline)?
    };

    edits.push(Edit {
        start,
        end,
        rendered,
    });
    Ok(())
}

/// Whether a sequence's byte range carries a comment or blank line that
/// regenerating it would drop. A blank line, or any line carrying a YAML
/// comment, counts — erring toward leaving the sequence untouched so no author
/// comment is ever lost.
fn region_has_interior_noise(region: &str) -> bool {
    region
        .split('\n')
        .any(|line| line.trim().is_empty() || line_has_comment(line))
}

/// Whether `line` carries a YAML comment: an unquoted `#` at the line start
/// (after indentation) or preceded by whitespace — a space **or a tab** — that
/// is not inside a quoted scalar.
///
/// This is stricter than a plain `" #"` substring on two axes: a tab-separated
/// comment (`- item\t# note`) is recognized so regenerating the sequence never
/// silently drops it, and a literal `#` inside a quoted value (e.g. a delimiter
/// spelled `" #"`) is *not* mistaken for a comment, so a sequence carrying one
/// still expands.
///
/// The distinction between a real quoted scalar and a bare quote character is
/// positional, not a quote count: in YAML a `'`/`"` opens a quoted scalar only
/// at a VALUE-START position — immediately after the `- ` block-sequence
/// indicator, after a `key:` separator, or at the start of the line's content.
/// A quote anywhere else is an ordinary character of a plain scalar, so `5'`,
/// `don't`, and `5" nail` never open a region that could swallow a following
/// `#`. This is what a bare quote-count scanner got wrong: an even number of
/// stray quotes (`- 5'  # feet, don't drop`) balanced out and hid the real
/// trailing comment.
///
/// A quote that DOES open at value-start but never closes leaves the scan unable
/// to trust its position; the line is then reported as a comment so the sequence
/// is passed through unexpanded (valid YAML, re-parses identically) rather than
/// regenerated over a possibly-skipped comment. The whole function biases toward
/// TRUE when ambiguous for the same reason: a false positive merely leaves a
/// block un-expanded, while a false negative silently drops an author's comment.
fn line_has_comment(line: &str) -> bool {
    let bytes = line.as_bytes();
    // `value_start`: a quote here would open a quoted scalar. True at the start
    // of the line's content and again right after a `- ` indicator or a `key:`
    // separator. `prev_ws`: the previous byte was whitespace (or the line start),
    // so a `#` here begins a comment.
    let mut value_start = true;
    let mut prev_ws = true;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b' ' | b'\t' => {
                prev_ws = true;
                i += 1;
            }
            // Outside any quoted scalar, a whitespace-preceded `#` is a comment.
            b'#' if prev_ws => return true,
            b'\'' | b'"' if value_start => match scan_quoted(bytes, i) {
                // A closed quoted scalar cannot hide a `#` from the rest of the
                // scan; resume just past it, no longer at a value start.
                Some(end) => {
                    value_start = false;
                    prev_ws = false;
                    i = end;
                }
                // An unterminated quote opened at value-start: preserve.
                None => return true,
            },
            // A block-sequence indicator (`- `): the value begins after it, so
            // the next token is still a value start.
            b'-' if value_start && is_ws_or_eol(bytes.get(i + 1)) => {
                prev_ws = false;
                i += 1;
            }
            // A `key:` mapping separator: the value begins after it.
            b':' if is_ws_or_eol(bytes.get(i + 1)) => {
                value_start = true;
                prev_ws = false;
                i += 1;
            }
            // Any other byte is part of a plain scalar; a bare quote or a `#`
            // without leading whitespace here is an ordinary character.
            _ => {
                value_start = false;
                prev_ws = false;
                i += 1;
            }
        }
    }
    false
}

/// Whether `b` is whitespace or the end of the line — the lookahead a
/// value-start indicator (`- `, `key:`) needs to tell an indicator from a plain
/// scalar character (`-5`, `a:b`).
fn is_ws_or_eol(b: Option<&u8>) -> bool {
    matches!(b, None | Some(b' ') | Some(b'\t'))
}

/// Scan the quoted scalar that opens at `bytes[start]` (a `'` or `"`), returning
/// the index just past its closing quote, or `None` when the line ends first.
///
/// Single quotes take a doubled `''` as an escaped quote; double quotes take a
/// backslash as escaping the next character. Shared by [`line_has_comment`] and
/// [`flow_sequence_end`] so the two cannot drift on the escape rules.
fn scan_quoted(bytes: &[u8], start: usize) -> Option<usize> {
    let quote = bytes[start];
    let mut i = start + 1;
    while i < bytes.len() {
        let c = bytes[i];
        if quote == b'\'' {
            if c == b'\'' {
                // A doubled `''` is an escaped quote, not a close.
                if bytes.get(i + 1) == Some(&b'\'') {
                    i += 2;
                    continue;
                }
                return Some(i + 1);
            }
        } else if c == b'\\' {
            i += 2;
            continue;
        } else if c == b'"' {
            return Some(i + 1);
        }
        i += 1;
    }
    None
}

/// End (exclusive) of the flow sequence that opens at `raw[start] == '['`.
///
/// Matches brackets and braces at depth, skipping over single- and
/// double-quoted scalars via [`scan_quoted`] so a delimiter like `"]"` inside a
/// value is not mistaken for the closing bracket. Input that already parsed is
/// balanced, so the scan terminates at the matching close. Unlike
/// [`line_has_comment`], every quote in a flow sequence sits at a value-start
/// position (after `[`, `,`, or `:`), so this opens on any quote it meets.
fn flow_sequence_end(raw: &str, start: usize) -> Result<usize, CanonicalError> {
    let bytes = raw.as_bytes();
    let mut depth: i32 = 0;
    let mut i = start;
    while i < bytes.len() {
        match bytes[i] {
            b'\'' | b'"' => match scan_quoted(bytes, i) {
                Some(end) => {
                    i = end;
                    continue;
                }
                None => break,
            },
            b'[' | b'{' => depth += 1,
            b']' | b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Ok(i + 1);
                }
            }
            _ => {}
        }
        i += 1;
    }
    Err(CanonicalError::Internal(
        "unterminated flow sequence".to_string(),
    ))
}

/// End (exclusive) of the block sequence whose first `-` is at `raw[start]`.
///
/// The sequence spans its item lines (a `-` at the sequence's own indent) and
/// their more-indented continuations. It ends at the first line that dedents to
/// the sequence indent as a sibling key, or below it. Trailing blank and
/// comment-only lines are excluded so they stay part of the surrounding
/// document rather than being swallowed into the replaced span.
fn block_sequence_end(raw: &str, start: usize) -> usize {
    let base_indent = leading_indent(raw, start);
    let mut line_start = raw[..start].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let mut last_content_end = start;
    let mut first = true;
    loop {
        let line_end = raw[line_start..]
            .find('\n')
            .map(|i| line_start + i)
            .unwrap_or(raw.len());
        let line = &raw[line_start..line_end];
        let indent = line.len() - line.trim_start_matches(' ').len();
        let content = &line[indent..];
        let line_content_end = line_start + line.trim_end().len();

        if first {
            // The line carrying the first `-` is always part of the sequence.
            last_content_end = line_content_end;
            first = false;
        } else if content.trim().is_empty() || content.starts_with('#') {
            // Blank or comment line: never extends the span. If a later item
            // follows, that item's line re-extends past this one; if nothing
            // follows, this line and the rest belong to the document.
        } else if indent > base_indent {
            // Continuation of the current item's mapping/scalar.
            last_content_end = line_content_end;
        } else if indent == base_indent && {
            // A further item at the sequence indent (`trim_end` so a trailing
            // `\r` under CRLF does not hide the dash).
            let item = content.trim_end();
            item == "-" || item.starts_with("- ")
        } {
            last_content_end = line_content_end;
        } else {
            // A sibling key at the sequence indent, or a dedent below it.
            break;
        }

        if line_end >= raw.len() {
            break;
        }
        line_start = line_end + 1;
    }
    last_content_end
}

/// Count of leading spaces on the line containing `offset`, i.e. the column the
/// sequence's first token sits at. Assumes YAML indentation (spaces, never
/// tabs), which the parser enforces upstream.
fn leading_indent(raw: &str, offset: usize) -> usize {
    let line_start = raw[..offset].rfind('\n').map(|i| i + 1).unwrap_or(0);
    offset - line_start
}

/// Render a sequence as a canonical block sequence indented by `base_indent`.
///
/// serde-saphyr emits the sequence at column zero (`- key: val\n  key2: ...`);
/// the first line is spliced directly where the document already provides the
/// leading indent, and every subsequent line is shifted right by `base_indent`
/// so continuations and later items align under it. `newline` is the document's
/// own line ending, so a CRLF file is not spliced with lone `\n`s.
fn render_block<T: Serialize>(
    items: &[T],
    base_indent: usize,
    newline: &str,
) -> Result<String, CanonicalError> {
    let yaml = yaml::to_string(&items).map_err(CanonicalError::Internal)?;
    let body = yaml.trim_end_matches('\n');
    let pad = " ".repeat(base_indent);
    let mut out = String::new();
    for (i, line) in body.split('\n').enumerate() {
        if i > 0 {
            out.push_str(newline);
            if !line.is_empty() {
                out.push_str(&pad);
            }
        }
        out.push_str(line);
    }
    Ok(out)
}

/// Render a sequence as a single-line flow sequence.
///
/// Used only for a shorthand sequence that sits inline after `key:` on one
/// line, where a block value is not legal. Each entry is emitted as JSON, which
/// is valid YAML flow and quotes every value unambiguously, so the result
/// re-parses to the identical sequence.
fn render_flow<T: Serialize>(items: &[T]) -> Result<String, CanonicalError> {
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        parts.push(
            serde_json::to_string(item).map_err(|e| CanonicalError::Internal(e.to_string()))?,
        );
    }
    Ok(format!("[{}]", parts.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn numeric_pipeline(type_token: &str) -> String {
        format!(
            "pipeline:\n  name: numeric_edit\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      path: input.csv\n      schema:\n        - name: n\n          type: {type_token}\n"
        )
    }

    #[test]
    fn numeric_edit_locates_only_the_literal_leaf_and_proves_semantics() {
        let raw = numeric_pipeline("{ nullable: numeric }");
        let address = ScopedSchemaLeafAddress::column("values", "n", "type");
        let NumericTypeEditDecision::Editable(edit) =
            plan_numeric_type_edit(&raw, &address, ConcreteNumericType::Int).unwrap()
        else {
            panic!("literal nullable numeric leaf must be editable");
        };
        assert_eq!(&raw[edit.start()..edit.end()], "numeric");
        let edited = edit.apply(&raw).unwrap();
        assert!(edited.contains("type: { nullable: int }"));
        prove_numeric_type_only_change(&raw, &edited, &address, ConcreteNumericType::Int).unwrap();
    }

    #[test]
    fn numeric_edit_rejects_alias_provenance() {
        let raw = "pipeline:\n  name: numeric_edit\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      path: input.csv\n      schema:\n        - { name: anchor, type: &number numeric }\n        - { name: n, type: *number }\n";
        let address = ScopedSchemaLeafAddress::column("values", "n", "type");
        assert_eq!(
            plan_numeric_type_edit(raw, &address, ConcreteNumericType::Float).unwrap(),
            NumericTypeEditDecision::Ineligible(NumericEditIneligibility::IndirectProvenance)
        );
        let anchor = ScopedSchemaLeafAddress::column("values", "anchor", "type");
        assert_eq!(
            plan_numeric_type_edit(raw, &anchor, ConcreteNumericType::Int).unwrap(),
            NumericTypeEditDecision::Ineligible(NumericEditIneligibility::NonLiteralToken)
        );
    }

    #[test]
    fn numeric_edit_addresses_one_multi_record_owner_exactly() {
        let raw = "pipeline:\n  name: numeric_edit\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      path: input.csv\n      schema:\n        discriminator: { field: kind }\n        records:\n          - id: detail\n            tag: D\n            columns: [{ name: n, type: numeric }]\n          - id: trailer\n            tag: T\n            columns: [{ name: n, type: numeric }]\n";
        let address = ScopedSchemaLeafAddress::record_column("values", "trailer", "n", "type");
        let NumericTypeEditDecision::Editable(edit) =
            plan_numeric_type_edit(raw, &address, ConcreteNumericType::Float).unwrap()
        else {
            panic!("record owner must be editable");
        };
        let edited = edit.apply(raw).unwrap();
        assert_eq!(edited.matches("type: numeric").count(), 1);
        assert_eq!(edited.matches("type: float").count(), 1);
        prove_numeric_type_only_change(raw, &edited, &address, ConcreteNumericType::Float).unwrap();
    }

    #[test]
    fn semantic_proof_rejects_a_sibling_change() {
        let raw = numeric_pipeline("numeric");
        let address = ScopedSchemaLeafAddress::column("values", "n", "type");
        let NumericTypeEditDecision::Editable(edit) =
            plan_numeric_type_edit(&raw, &address, ConcreteNumericType::Int).unwrap()
        else {
            panic!("numeric leaf must be editable");
        };
        let edited = edit
            .apply(&raw)
            .unwrap()
            .replace("path: input.csv", "path: other.csv");
        assert!(
            prove_numeric_type_only_change(&raw, &edited, &address, ConcreteNumericType::Int)
                .is_err()
        );
    }

    /// Every normalized shorthand sequence a document declares, grouped by
    /// surface — the semantic content that must survive canonicalization.
    #[derive(Debug, PartialEq)]
    struct Probed {
        split_to_rows: Vec<Vec<SplitToRows>>,
        split_values: Vec<Vec<SplitValues>>,
        join_values: Vec<Vec<JoinValues>>,
    }

    /// Re-extract the normalized shorthand values from a document, so a
    /// before/after pair can be compared for semantic identity of every
    /// multi-value surface.
    fn probe(raw: &str) -> Probed {
        let doc: DocProbe = yaml::from_str(raw).expect("probe parse");
        let mut out = Probed {
            split_to_rows: Vec::new(),
            split_values: Vec::new(),
            join_values: Vec::new(),
        };
        for node in &doc.nodes {
            if let Some(cfg) = &node.config {
                if let Some(s) = &cfg.split_to_rows {
                    out.split_to_rows.push(s.value.clone());
                }
                if let Some(s) = &cfg.split_values {
                    out.split_values.push(s.value.clone());
                }
                if let Some(s) = &cfg.join_values {
                    out.join_values.push(s.value.clone());
                }
            }
        }
        out
    }

    fn assert_parse_identity(before: &str, after: &str) {
        assert_eq!(probe(before), probe(after), "shorthand values must match");
    }

    fn assert_idempotent(raw: &str) {
        let once = expand_multi_value_shorthand(raw).expect("first pass");
        let twice = expand_multi_value_shorthand(&once).expect("second pass");
        assert_eq!(once, twice, "expansion must be idempotent");
    }

    const SPLIT_TO_ROWS_BARE: &str = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows:
        - line_items
      schema:
        - { name: order_id, type: string }
";

    #[test]
    fn bare_split_to_rows_expands_with_defaults() {
        let out = expand_multi_value_shorthand(SPLIT_TO_ROWS_BARE).unwrap();
        assert!(out.contains("field: line_items"), "\n{out}");
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert!(out.contains("mode: extract"), "\n{out}");
        // The bare form is gone.
        assert!(!out.contains("- line_items"), "\n{out}");
        assert_parse_identity(SPLIT_TO_ROWS_BARE, &out);
        assert_idempotent(SPLIT_TO_ROWS_BARE);
    }

    #[test]
    fn partial_split_to_rows_mapping_materializes_missing_defaults() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: xml
      path: in.xml
      split_to_rows:
        - field: LineItem
          mode: extract
          position_column: line_no
      schema:
        - { name: line_no, type: int }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // `keep_empty` was omitted; the canonical form spells it out.
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert!(out.contains("position_column: line_no"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn split_values_bare_and_escape_and_json_expand() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values:
        - tags
        - field: notes
          delimiter: \"|\"
          escape: \"\\\\\"
        - field: payload
          json: true
      schema:
        - { name: tags, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // Bare `tags` gains the default delimiter.
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(
            out.contains("delimiter: ';'") || out.contains("delimiter: ;"),
            "\n{out}"
        );
        // The escape entry keeps its explicit escape.
        assert!(out.contains("field: notes"), "\n{out}");
        // The JSON entry keeps its flag.
        assert!(out.contains("json: true"), "\n{out}");
        assert!(!out.contains("- tags\n"), "bare form gone:\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn join_values_bare_expands() {
        let raw = "\
nodes:
  - type: output
    name: o
    input: s
    config:
      name: o
      type: csv
      path: out.csv
      join_values:
        - tags
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(out.contains("on_conflict: error"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn surrounding_document_is_preserved_verbatim() {
        let raw = "\
pipeline:
  name: demo   # a trailing comment

# A header comment describing the source.
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json   # inline comment on path
      split_to_rows:
        - line_items
      schema:
        - { name: order_id, type: string }   # keep me
error_handling:
  strategy: fail_fast
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // Every non-shorthand line survives byte-for-byte.
        assert!(
            out.contains("  name: demo   # a trailing comment"),
            "\n{out}"
        );
        assert!(
            out.contains("# A header comment describing the source."),
            "\n{out}"
        );
        assert!(
            out.contains("      path: in.json   # inline comment on path"),
            "\n{out}"
        );
        assert!(
            out.contains("        - { name: order_id, type: string }   # keep me"),
            "\n{out}"
        );
        assert!(out.contains("  strategy: fail_fast"), "\n{out}");
        // And the shorthand did expand.
        assert!(out.contains("keep_empty: true"), "\n{out}");
    }

    #[test]
    fn multiple_shorthand_blocks_in_one_node_all_expand() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: xml
      path: in.xml
      split_to_rows:
        - LineItem
      split_values:
        - cost_centres
      schema:
        - { name: cost_centres, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(out.contains("field: LineItem"), "\n{out}");
        assert!(out.contains("field: cost_centres"), "\n{out}");
        assert!(out.contains("mode: extract"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn schema_multiple_column_is_left_unchanged() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values:
        - tags
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // The schema block is already canonical and must survive untouched.
        assert!(
            out.contains("        - { name: tags, type: string, multiple: true }"),
            "\n{out}"
        );
    }

    #[test]
    fn inline_flow_sequence_expands_in_place() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows: [line_items]
      schema:
        - { name: order_id, type: string }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // Stays on one line (flow), but the field/default are materialized.
        assert!(out.contains("split_to_rows: ["), "\n{out}");
        assert!(out.contains("\"field\":\"line_items\""), "\n{out}");
        assert!(out.contains("\"keep_empty\":true"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn flow_sequence_on_its_own_line_becomes_block() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows:
        [line_items]
      schema:
        - { name: order_id, type: string }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(out.contains("- field: line_items"), "\n{out}");
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn config_without_shorthand_is_unchanged() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      schema:
        - { name: order_id, type: string }
  - type: output
    name: o
    input: s
    config:
      name: o
      type: json
      path: out.json
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(raw, out, "no shorthand present, document must be unchanged");
    }

    // ---- Splice-corruption regressions: the reported offset is not always
    // the sequence's opening token ----

    #[test]
    fn same_indent_block_single_item_expands_without_corruption() {
        // The dash sits at the parent key's own indent (the `key:\n- item`
        // form); serde-saphyr reports the item's value, not the dash.
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows:
      - line_items
      schema:
        - { name: order_id, type: string }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: line_items"), "\n{out}");
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn same_indent_block_multi_item_expands_without_corruption() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values:
      - tags
      - codes
      schema:
        - { name: tags, type: string, multiple: true }
        - { name: codes, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(out.contains("field: codes"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn same_indent_block_mapping_item_expands_without_corruption() {
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: xml
      path: in.xml
      split_to_rows:
      - field: LineItem
        mode: extract
      schema:
        - { name: order_id, type: string }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: LineItem"), "\n{out}");
        // The omitted default is materialized.
        assert!(out.contains("keep_empty: true"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn same_indent_at_nonzero_indent_expands_without_corruption() {
        // Key and dash both at indent 6 (the same-indent form at a nonzero indent).
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_values:
      - tags
      schema:
        - { name: tags, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(
            out.contains("      - field: tags"),
            "correct indent:\n{out}"
        );
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn alias_use_site_is_left_untouched() {
        // An anchored shorthand sequence reused by an alias: the definition
        // expands, the `*sv` use-site is left byte-identical (it still resolves
        // to the expanded value on re-parse).
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values: &sv
        - field: tags
          delimiter: \"|\"
      schema:
        - { name: tags, type: string, multiple: true }
  - type: output
    name: o
    input: s
    config:
      name: o
      type: csv
      path: out.csv
      join_values: *sv
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(
            out.contains("join_values: *sv"),
            "alias use-site must be byte-identical:\n{out}"
        );
        // Not corrupted by an inline block splice.
        assert!(!out.contains("join_values: *sv\n        -"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn interior_comment_between_items_is_preserved() {
        // A comment between items cannot survive regeneration, so the sequence
        // is passed through byte-identical rather than expanded — never dropping
        // the comment.
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values:
        - tags
        # keep this comment
        - codes
      schema:
        - { name: tags, type: string, multiple: true }
        - { name: codes, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(
            raw, out,
            "a sequence with an interior comment is left untouched"
        );
        assert!(out.contains("# keep this comment"), "\n{out}");
    }

    #[test]
    fn anchored_block_same_indent_single_item_expands_and_reparses() {
        // An anchor on the sequence definition, with the block at the key's own
        // indent (`key: &sv\n- item`). The definition must expand — never error
        // — and the anchor must survive so a later alias still resolves.
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values: &sv
      - tags
      schema:
        - { name: tags, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(out.contains("delimiter:"), "defaults materialized:\n{out}");
        assert!(
            out.contains("split_values: &sv"),
            "anchor must be preserved:\n{out}"
        );
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn anchored_block_deeper_multi_item_expands_and_reparses() {
        // An anchor on the sequence definition, with a deeper-indented block of
        // several items. All items expand and the anchor is preserved.
        let raw = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: csv
      path: in.csv
      split_values: &sv
        - tags
        - codes
      schema:
        - { name: tags, type: string, multiple: true }
        - { name: codes, type: string, multiple: true }
";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert!(!out.contains("- - "), "no doubled dash corruption:\n{out}");
        assert!(out.contains("field: tags"), "\n{out}");
        assert!(out.contains("field: codes"), "\n{out}");
        assert!(
            out.contains("split_values: &sv"),
            "anchor must be preserved:\n{out}"
        );
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn resolve_sequence_start_skips_anchor_at_reported_offset() {
        // Defensive contract: if the parser ever reports the `&anchor` token as
        // the sequence value's offset, the resolver skips it to the real
        // `-` / `[` rather than classifying the region `Unexpected` (which
        // would hard-error on a valid anchored config).
        for (src, want_byte) in [
            ("key: &sv\n  - a\n", b'-'),
            ("key: &sv\n- a\n", b'-'),
            ("key: &sv [a]\n", b'['),
            ("key: &longer_name\n  - a\n", b'-'),
        ] {
            let amp = src.find('&').expect("anchor token");
            match resolve_sequence_start(src, amp) {
                SequenceStart::Splice(pos) => assert_eq!(
                    src.as_bytes()[pos],
                    want_byte,
                    "resolved to {:?}, want {:?} in {src:?}",
                    src.as_bytes()[pos] as char,
                    want_byte as char
                ),
                other => panic!("expected Splice for {src:?}, got {other:?}"),
            }
        }

        // An anchor with no sequence token after it is passed through, not
        // errored — a valid document is never corrupted.
        match resolve_sequence_start("key: &sv scalar\n", 5) {
            SequenceStart::PassThrough => {}
            other => panic!("expected PassThrough for a scalar anchor, got {other:?}"),
        }
    }

    #[test]
    fn tab_preceded_comment_is_preserved() {
        // A shorthand item ending in a TAB-separated comment must not be
        // dropped: the sequence is passed through byte-identical rather than
        // regenerated (which would discard the comment).
        let raw = "nodes:\n  - type: source\n    name: s\n    config:\n      name: s\n      \
                   type: csv\n      path: in.csv\n      split_values:\n        \
                   - tags\t# keep this\n      schema:\n        \
                   - { name: tags, type: string, multiple: true }\n";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(
            raw, out,
            "a tab-preceded comment must leave the sequence untouched:\n{out}"
        );
        assert!(out.contains("# keep this"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn quoted_hash_value_still_expands() {
        // A value containing `" #"` inside a quoted scalar must not be mistaken
        // for a comment: the sequence still expands, materializing the bare
        // item's defaults while the quoted value is preserved verbatim.
        let raw = "nodes:\n  - type: source\n    name: s\n    config:\n      name: s\n      \
                   type: csv\n      path: in.csv\n      split_values:\n        \
                   - field: notes\n          delimiter: \" #\"\n        - tags\n      \
                   schema:\n        - { name: notes, type: string, multiple: true }\n        \
                   - { name: tags, type: string, multiple: true }\n";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // The bare `- tags` gained its default delimiter — the block expanded.
        assert!(out.contains("field: tags"), "block should expand:\n{out}");
        assert!(out.contains("field: notes"), "\n{out}");
        // `assert_parse_identity` guarantees the quoted `#` delimiter survived.
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn bare_apostrophe_item_with_comment_is_preserved() {
        // A plain scalar carries a bare apostrophe (`it's`) mid-value: the `'` is
        // not at a value-start position, so it opens no quoted scalar and the
        // trailing `#` is seen as the real comment it is. The sequence is passed
        // through byte-identical rather than regenerated (which would drop it).
        let raw = "nodes:\n  - type: source\n    name: s\n    config:\n      name: s\n      \
                   type: csv\n      path: in.csv\n      split_values:\n        \
                   - it's  # KEEP THIS COMMENT\n      schema:\n        \
                   - { name: order_id, type: string }\n";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(
            raw, out,
            "an apostrophe-bearing bare item with a comment must be left untouched:\n{out}"
        );
        assert!(out.contains("# KEEP THIS COMMENT"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn bare_double_quote_item_with_comment_is_preserved() {
        // The double-quote variant (`5" nail`, inches): the `"` sits mid-scalar,
        // not at a value start, so it is an ordinary character and the trailing
        // comment is preserved.
        let raw = "nodes:\n  - type: source\n    name: s\n    config:\n      name: s\n      \
                   type: csv\n      path: in.csv\n      split_values:\n        \
                   - 5\" nail  # KEEP THIS COMMENT\n      schema:\n        \
                   - { name: order_id, type: string }\n";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(
            raw, out,
            "an inch-quote-bearing bare item with a comment must be left untouched:\n{out}"
        );
        assert!(out.contains("# KEEP THIS COMMENT"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn bare_apostrophe_item_without_comment_expands_safely() {
        // The behavior the value-start rule changes: a quote-bearing plain scalar
        // with NO comment (`- it's`) now EXPANDS rather than bailing, because the
        // mid-scalar `'` opens no quoted region and there is no `#` to protect.
        // The expansion must re-parse to the same shorthand values and be
        // idempotent — corruption-safety is the whole point of the gate.
        let raw = "nodes:\n  - type: source\n    name: s\n    config:\n      name: s\n      \
                   type: csv\n      path: in.csv\n      split_values:\n        \
                   - it's\n      schema:\n        \
                   - { name: it's, type: string, multiple: true }\n";
        let out = expand_multi_value_shorthand(raw).unwrap();
        // The bare item gained its materialized defaults — the block expanded.
        assert!(out.contains("field: it's"), "block should expand:\n{out}");
        assert!(out.contains("delimiter:"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn even_count_stray_quote_before_a_comment_is_preserved() {
        // The regression the value-start rule closes: `- 5'  # feet, don't drop`
        // has an EVEN number of apostrophes (`5'` … `don't`). A quote-count
        // scanner opened a fake region on `5'` and closed it on the `'` in
        // `don't`, ending balanced and skipping the real `#` — dropping the
        // comment. Neither apostrophe is at a value-start position, so no quoted
        // scalar opens and the comment is seen and preserved.
        let raw = "nodes:\n  - type: source\n    name: s\n    config:\n      name: s\n      \
                   type: csv\n      path: in.csv\n      split_values:\n        \
                   - 5'  # feet, don't drop\n      schema:\n        \
                   - { name: order_id, type: string }\n";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(
            raw, out,
            "an even-count stray-quote line with a comment must be left untouched:\n{out}"
        );
        assert!(out.contains("# feet, don't drop"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn mapping_value_trailing_comment_is_preserved() {
        // A `key: value  # c` line inside a shorthand item's mapping: the value
        // begins after `key:` and is a plain scalar, so the whitespace-preceded
        // `#` is a real comment and the block is passed through untouched.
        let raw = "nodes:\n  - type: source\n    name: s\n    config:\n      name: s\n      \
                   type: csv\n      path: in.csv\n      split_values:\n        \
                   - field: tags  # keep this trailing comment\n      schema:\n        \
                   - { name: tags, type: string, multiple: true }\n";
        let out = expand_multi_value_shorthand(raw).unwrap();
        assert_eq!(
            raw, out,
            "a mapping value's trailing comment must be left untouched:\n{out}"
        );
        assert!(out.contains("# keep this trailing comment"), "\n{out}");
        assert_parse_identity(raw, &out);
        assert_idempotent(raw);
    }

    #[test]
    fn value_start_quote_with_interior_hash_still_expands() {
        // A quote AT a value-start position (`delimiter: " #"`) opens a real
        // quoted scalar, so the `#` inside it is not a comment and the block
        // still expands. This is the counterpart to the stray-quote cases: the
        // value-start rule must still recognize a genuine quoted delimiter.
        assert!(!line_has_comment(r#"      delimiter: " #""#));
        assert!(!line_has_comment(r#"      escape: "\\""#));
        // The stray-quote and comment cases the block-level tests exercise, at
        // the unit boundary.
        assert!(line_has_comment("- 5'  # feet, don't drop"));
        assert!(line_has_comment("- tags\t# keep"));
        assert!(line_has_comment("key: value  # c"));
        // No comment: these expand.
        assert!(!line_has_comment("- it's"));
        assert!(!line_has_comment("- tags"));
    }

    #[test]
    fn crlf_document_keeps_crlf_line_endings() {
        let lf = "\
nodes:
  - type: source
    name: s
    config:
      name: s
      type: json
      path: in.json
      split_to_rows:
        - line_items
      schema:
        - { name: order_id, type: string }
";
        let raw = lf.replace('\n', "\r\n");
        let out = expand_multi_value_shorthand(&raw).unwrap();
        assert!(out.contains("keep_empty: true"), "\n{out}");
        // No lone `\n` survives: removing every CRLF leaves no bare newline.
        assert!(
            !out.replace("\r\n", "").contains('\n'),
            "mixed line endings in:\n{out:?}"
        );
        assert_parse_identity(&raw, &out);
        // Idempotent on the CRLF form too.
        let twice = expand_multi_value_shorthand(&out).unwrap();
        assert_eq!(out, twice);
    }
}
