//! Provenance tracking for resolved configuration values.
//!
//! Each config value in a compiled pipeline carries a [`ResolvedValue`] wrapper
//! that records which configuration layer contributed the winning value. The
//! provenance chain is stored in a side-table [`ProvenanceDb`], separate from
//! [`CompiledPlan`](crate::plan::compiled::CompiledPlan).
//!
//! # Layer stack
//!
//! The composition overlay resolves values through a fixed *semantic* order of
//! layers (never lexical or positional), encoded by [`LayerKind`]:
//!
//! ```text
//! PipelineDefault  <  Group(s) by priority  <  ChannelWide  <  ChannelPerTarget
//! ```
//!
//! Groups are *dynamic*: any number of groups may match a channel, so `Group`
//! carries its `priority` (higher wins) and a declaration-order source id
//! (`seq`, later declaration wins ties). Every group is a distinct layer.
//!
//! A layer may additionally be marked **`fixed`**: a fixed value locks the
//! resolution against every higher-precedence layer — nothing downstream may
//! override it. When several layers are fixed, the lowest-precedence fixed
//! layer wins, because it locked the value first.
//!
//! # Generic over the layer kind
//!
//! [`ResolvedValue`] and [`ProvenanceLayer`] are generic over the layer-kind
//! type `L` (any `Copy + Ord`). The composition overlay instantiates them with
//! [`LayerKind`]; a future schema-attribute resolver can reuse the *same*
//! implementation with a `SchemaLayer` kind and zero duplication. Precedence,
//! `fixed`-lock semantics, and single-winner selection are all defined purely
//! in terms of `L: Ord`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::num::NonZeroU32;

use clinker_core_types::span::{FileId, Span};
use serde::de::Error as _;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::plan::PlanNodeId;

// Keep ProvenanceLayer at or under 64 bytes — the provenance side-table
// is sized for this footprint. Checked for the composition instantiation
// (`ProvenanceLayer<LayerKind>`), the widest layer kind in the crate.
const _: () = assert!(std::mem::size_of::<ProvenanceLayer>() <= 64);

/// Which configuration layer contributed a value.
///
/// Precedence is encoded via the derived `Ord`, which orders enum variants by
/// declaration order and struct-variant fields top-to-bottom:
///
/// ```text
/// PipelineDefault < Group { priority, seq } < ChannelWide < ChannelPerTarget
/// ```
///
/// Among `Group` variants, `priority` is compared first (higher priority is a
/// greater layer, so it wins), then `seq` (the group's declaration-order source
/// id; a later declaration is greater and wins ties). Higher-precedence layers
/// win over lower-precedence ones during [`ResolvedValue::apply_layer`], unless
/// a lower layer is `fixed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum LayerKind {
    /// Base layer: a pipeline's own authored defaults / composition call-site
    /// values.
    PipelineDefault,
    /// A matching group overlay. `priority` (higher wins) then `seq` (the
    /// group's declaration-order source id, later wins) determine ordering
    /// among multiple matching groups.
    Group {
        /// Group priority; higher wins among multiple matching groups.
        priority: i32,
        /// Declaration-order source id of the group; breaks priority ties with
        /// later-declared groups winning.
        seq: u32,
    },
    /// Channel-wide overrides (`channel.cfg.yaml`), applied to every pipeline
    /// the channel runs.
    ChannelWide,
    /// Channel per-target overrides (`<target>.channel.yaml`).
    ChannelPerTarget,
}

impl fmt::Display for LayerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LayerKind::PipelineDefault => write!(f, "PipelineDefault"),
            LayerKind::Group { priority, seq } => {
                write!(f, "Group #{seq} (priority {priority})")
            }
            LayerKind::ChannelWide => write!(f, "ChannelWide"),
            LayerKind::ChannelPerTarget => write!(f, "ChannelPerTarget"),
        }
    }
}

/// A single provenance record: one layer's contribution to a config value.
///
/// The `won` flag marks the layer whose value was selected. Exactly one layer
/// in a [`ResolvedValue`]'s provenance chain has `won == true`.
///
/// Generic over the layer-kind `L`; the composition overlay uses the default
/// [`LayerKind`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProvenanceLayer<L = LayerKind> {
    /// Source span of the value's origin in the YAML file. File identity
    /// is resolved via `SourceDb.path(span.file)` at render time.
    #[serde(with = "span_serde")]
    pub span: Span,
    /// Which configuration layer this value came from.
    pub kind: L,
    /// Whether this layer locks its value against every higher-precedence
    /// layer. A `fixed` layer cannot be overridden by anything downstream.
    pub fixed: bool,
    /// Whether this layer's value was selected as the winner.
    pub won: bool,
}

/// A config value together with its full provenance chain.
///
/// The `won` flag makes the winning layer explicit for tooling inspectors
/// without requiring the inspector to re-run priority logic.
///
/// Generic over the value type `T` and the layer-kind `L` (defaulting to
/// [`LayerKind`]). The provenance chain length is *not* fixed: it grows with
/// the number of matching group layers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolvedValue<T, L = LayerKind> {
    /// The winning value after all layers have been applied.
    pub value: T,
    /// Ordered provenance chain. Each entry records a layer's contribution.
    /// Exactly one entry has `won == true`.
    pub provenance: Vec<ProvenanceLayer<L>>,
    /// Per-layer values in application order. Stores the value contributed by
    /// each layer so `--explain --field` can display shadowed values. Kept as
    /// an ordered collection (not a fixed-size map) so multiple `Group` layers
    /// each keep their own entry, and separate from [`ProvenanceLayer`] to
    /// preserve its 64-byte size.
    pub layer_values: Vec<(L, T)>,
}

impl<T: Clone, L: Copy + Ord> ResolvedValue<T, L> {
    /// Create a new resolved value with a single provenance layer.
    /// The initial layer is always the winner.
    pub fn new(value: T, kind: L, span: Span) -> Self {
        Self::with_fixed(value, kind, span, false)
    }

    /// Create a new resolved value whose sole layer is `fixed` — it locks the
    /// value against any higher-precedence layer applied later.
    pub fn new_fixed(value: T, kind: L, span: Span) -> Self {
        Self::with_fixed(value, kind, span, true)
    }

    fn with_fixed(value: T, kind: L, span: Span, fixed: bool) -> Self {
        Self {
            provenance: vec![ProvenanceLayer {
                span,
                kind,
                fixed,
                won: true,
            }],
            layer_values: vec![(kind, value.clone())],
            value,
        }
    }

    /// Returns the layer that won (the one with `won == true`).
    pub fn winning_layer(&self) -> Option<&ProvenanceLayer<L>> {
        self.provenance.iter().find(|l| l.won)
    }

    /// Apply a new (non-fixed) layer on top. The new layer wins if it is the
    /// highest-precedence layer and no lower layer is `fixed`.
    ///
    /// Same-kind layers replace in place: the span/value are updated. A `Group`
    /// with a distinct `(priority, seq)` is a distinct layer and is appended.
    pub fn apply_layer(&mut self, value: T, kind: L, span: Span) {
        self.apply_layer_inner(value, kind, span, false);
    }

    /// Apply a new `fixed` layer on top. A fixed layer locks its value against
    /// every higher-precedence layer applied afterwards.
    pub fn apply_layer_fixed(&mut self, value: T, kind: L, span: Span) {
        self.apply_layer_inner(value, kind, span, true);
    }

    fn apply_layer_inner(&mut self, value: T, kind: L, span: Span, fixed: bool) {
        // Record this layer's value (same-kind replace-in-place).
        match self.layer_values.iter_mut().find(|(k, _)| *k == kind) {
            Some(slot) => slot.1 = value,
            None => self.layer_values.push((kind, value)),
        }

        // Record/replace the provenance entry for this kind.
        match self.provenance.iter_mut().find(|l| l.kind == kind) {
            Some(existing) => {
                existing.span = span;
                existing.fixed = fixed;
            }
            None => self.provenance.push(ProvenanceLayer {
                span,
                kind,
                fixed,
                won: false,
            }),
        }

        self.recompute_winner();
    }

    /// The winning layer kind: the lowest-precedence `fixed` layer if any layer
    /// is fixed (it locked the value against everything downstream), otherwise
    /// the highest-precedence layer. Deterministic from the layer set alone —
    /// independent of application order.
    fn winner_kind(&self) -> L {
        if let Some(locked) = self
            .provenance
            .iter()
            .filter(|l| l.fixed)
            .map(|l| l.kind)
            .min()
        {
            return locked;
        }
        self.provenance
            .iter()
            .map(|l| l.kind)
            .max()
            .expect("provenance chain is non-empty")
    }

    /// Recompute `won` flags and the resolved `value` from the current layer
    /// set. Exactly one layer wins (layer kinds are unique per value).
    fn recompute_winner(&mut self) {
        let winner = self.winner_kind();
        for layer in &mut self.provenance {
            layer.won = layer.kind == winner;
        }
        if let Some((_, value)) = self.layer_values.iter().find(|(k, _)| *k == winner) {
            self.value = value.clone();
        }
    }

    /// Look up the value contributed by a specific layer kind.
    pub fn layer_value(&self, kind: L) -> Option<&T> {
        self.layer_values
            .iter()
            .find(|(k, _)| *k == kind)
            .map(|(_, v)| v)
    }
}

/// Typed field identity within a provenance-tracked plan node.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum ProvenanceField {
    /// A declared composition `config_schema` parameter.
    ConfigParam(String),
}

impl ProvenanceField {
    /// Authored field name, without an address prefix.
    pub fn name(&self) -> &str {
        match self {
            Self::ConfigParam(name) => name,
        }
    }
}

/// Collision-free internal identity for one provenance value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProvenanceKey {
    /// Stable node identity minted by the planner.
    pub node: PlanNodeId,
    /// Typed field identity within that node.
    pub field: ProvenanceField,
}

/// Paste-ready, versioned address for a composition config field.
///
/// Each authored segment uses RFC 6901 escaping (`~0` for `~`, `~1` for
/// `/`). Labels remain literal, so a segment can never be confused with an
/// address separator.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ScopedNodeAddress {
    call_path: Vec<String>,
    node_name: String,
    field: ProvenanceField,
}

/// Paste-ready, versioned address for one source-schema attribute.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ScopedSchemaAddress {
    source: String,
    column: String,
    attribute: String,
}

impl ScopedSchemaAddress {
    pub fn new(
        source: impl Into<String>,
        column: impl Into<String>,
        attribute: impl Into<String>,
    ) -> Self {
        Self {
            source: source.into(),
            column: column.into(),
            attribute: attribute.into(),
        }
    }

    pub fn parse(input: &str) -> Result<Self, ScopedNodeAddressParseError> {
        let raw_segments: Vec<&str> = input
            .strip_prefix('/')
            .ok_or_else(|| ScopedNodeAddressParseError::Malformed(input.to_owned()))?
            .split('/')
            .collect();
        let segments: Vec<String> = raw_segments
            .into_iter()
            .map(decode_pointer_segment)
            .collect::<Result<_, _>>()?;
        if segments.len() != 8
            || segments[0] != "v1"
            || segments[1] != "schema"
            || segments[2] != "sources"
            || segments[4] != "columns"
            || segments[6] != "attributes"
        {
            return Err(ScopedNodeAddressParseError::Malformed(input.to_owned()));
        }
        Ok(Self::new(
            segments[3].clone(),
            segments[5].clone(),
            segments[7].clone(),
        ))
    }

    pub fn render(&self) -> String {
        format!(
            "/v1/schema/sources/{}/columns/{}/attributes/{}",
            encode_pointer_segment(&self.source),
            encode_pointer_segment(&self.column),
            encode_pointer_segment(&self.attribute),
        )
    }

    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn attribute(&self) -> &str {
        &self.attribute
    }
}

impl ScopedNodeAddress {
    /// Build an address from an enclosing composition-call path, authored node
    /// name, and typed field.
    pub fn new<I, S>(call_path: I, node_name: impl Into<String>, field: ProvenanceField) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            call_path: call_path.into_iter().map(Into::into).collect(),
            node_name: node_name.into(),
            field,
        }
    }

    /// Convenience constructor for a composition config parameter.
    pub fn config_param<I, S>(
        call_path: I,
        node_name: impl Into<String>,
        param_name: impl Into<String>,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self::new(
            call_path,
            node_name,
            ProvenanceField::ConfigParam(param_name.into()),
        )
    }

    /// Build an address for a top-level node without an enclosing call path.
    pub fn top_level(node_name: impl Into<String>, field: ProvenanceField) -> Self {
        Self::new(std::iter::empty::<String>(), node_name, field)
    }

    /// Build a top-level composition-config address.
    pub fn top_level_config_param(
        node_name: impl Into<String>,
        param_name: impl Into<String>,
    ) -> Self {
        Self::top_level(node_name, ProvenanceField::ConfigParam(param_name.into()))
    }

    /// Parse a canonical `/v1/config/...` address.
    pub fn parse(input: &str) -> Result<Self, ScopedNodeAddressParseError> {
        let raw_segments: Vec<&str> = input
            .strip_prefix('/')
            .ok_or_else(|| ScopedNodeAddressParseError::Malformed(input.to_owned()))?
            .split('/')
            .collect();
        let segments: Vec<String> = raw_segments
            .into_iter()
            .map(decode_pointer_segment)
            .collect::<Result<_, _>>()?;

        if segments.first().map(String::as_str) != Some("v1")
            || segments.get(1).map(String::as_str) != Some("config")
        {
            return Err(ScopedNodeAddressParseError::Malformed(input.to_owned()));
        }

        let mut cursor = 2;
        let mut call_path = Vec::new();
        while segments.get(cursor).map(String::as_str) == Some("calls") {
            let call = segments
                .get(cursor + 1)
                .ok_or_else(|| ScopedNodeAddressParseError::Malformed(input.to_owned()))?;
            call_path.push(call.clone());
            cursor += 2;
        }

        if segments.len() != cursor + 4
            || segments[cursor] != "nodes"
            || segments[cursor + 2] != "fields"
        {
            return Err(ScopedNodeAddressParseError::Malformed(input.to_owned()));
        }

        Ok(Self::config_param(
            call_path,
            segments[cursor + 1].clone(),
            segments[cursor + 3].clone(),
        ))
    }

    /// Render the canonical versioned config address.
    pub fn render(&self) -> String {
        let mut output = String::from("/v1/config");
        for call in &self.call_path {
            output.push_str("/calls/");
            output.push_str(&encode_pointer_segment(call));
        }
        output.push_str("/nodes/");
        output.push_str(&encode_pointer_segment(&self.node_name));
        output.push_str("/fields/");
        output.push_str(&encode_pointer_segment(self.field.name()));
        output
    }

    /// Enclosing composition call-site segments, outermost first.
    pub fn call_path(&self) -> &[String] {
        &self.call_path
    }

    /// Authored node name within its scope.
    pub fn node_name(&self) -> &str {
        &self.node_name
    }

    /// Typed field selected by the address.
    pub fn field(&self) -> &ProvenanceField {
        &self.field
    }
}

impl fmt::Display for ScopedNodeAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.render())
    }
}

/// A structured address that could not be parsed without guessing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScopedNodeAddressParseError {
    Malformed(String),
}

impl fmt::Display for ScopedNodeAddressParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(input) => write!(f, "malformed provenance address {input:?}"),
        }
    }
}

impl std::error::Error for ScopedNodeAddressParseError {}

/// Parsed config provenance lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvenanceQuery {
    /// Exact, versioned structured address.
    Exact(ScopedNodeAddress),
    /// Legacy author shorthand, admitted only when it has one match.
    Shorthand {
        node_name: String,
        field: ProvenanceField,
    },
    /// Exact source-schema attribute address.
    SchemaExact(ScopedSchemaAddress),
    /// Legacy `source.column.attribute` shorthand.
    SchemaShorthand(ScopedSchemaAddress),
}

impl ProvenanceQuery {
    /// Parse an exact address or two-segment `node.param` shorthand.
    pub fn parse(input: &str) -> Result<Self, ProvenanceQueryParseError> {
        if input.is_empty() {
            return Err(ProvenanceQueryParseError::Empty);
        }
        if input.starts_with('/') {
            if input.starts_with("/v1/config") {
                return ScopedNodeAddress::parse(input)
                    .map(Self::Exact)
                    .map_err(|_| ProvenanceQueryParseError::Malformed(input.to_owned()));
            }
            if input.starts_with("/v1/schema") {
                return ScopedSchemaAddress::parse(input)
                    .map(Self::SchemaExact)
                    .map_err(|_| ProvenanceQueryParseError::Malformed(input.to_owned()));
            }
            return Err(ProvenanceQueryParseError::Malformed(input.to_owned()));
        }
        let segments: Vec<_> = input.split('.').collect();
        match segments.as_slice() {
            [node_name, field] if !node_name.is_empty() && !field.is_empty() => {
                Ok(Self::Shorthand {
                    node_name: (*node_name).to_owned(),
                    field: ProvenanceField::ConfigParam((*field).to_owned()),
                })
            }
            [source, column, attribute]
                if !source.is_empty() && !column.is_empty() && !attribute.is_empty() =>
            {
                Ok(Self::SchemaShorthand(ScopedSchemaAddress::new(
                    *source, *column, *attribute,
                )))
            }
            _ => Err(ProvenanceQueryParseError::Malformed(input.to_owned())),
        }
    }

    fn field(&self) -> Option<&ProvenanceField> {
        match self {
            Self::Exact(address) => Some(address.field()),
            Self::Shorthand { field, .. } => Some(field),
            Self::SchemaExact(_) | Self::SchemaShorthand(_) => None,
        }
    }
}

/// Parse failure classified separately so the CLI can reserve E127 for an
/// empty query and E128 for every other non-resolving form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvenanceQueryParseError {
    Empty,
    Malformed(String),
}

impl fmt::Display for ProvenanceQueryParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("provenance query is empty"),
            Self::Malformed(input) => write!(f, "unrecognized provenance query {input:?}"),
        }
    }
}

impl std::error::Error for ProvenanceQueryParseError {}

/// A successful provenance match.
#[derive(Debug, Clone, Copy)]
pub struct ProvenanceMatch<'a> {
    pub key: &'a ProvenanceKey,
    pub address: &'a ScopedNodeAddress,
    pub resolved: &'a ResolvedValue<serde_json::Value>,
}

/// Deterministic lookup failure with paste-ready exact candidates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProvenanceLookupError {
    Unknown { candidates: Vec<ScopedNodeAddress> },
    Ambiguous { candidates: Vec<ScopedNodeAddress> },
}

impl fmt::Display for ProvenanceLookupError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (label, candidates) = match self {
            Self::Unknown { candidates } => ("unknown provenance field", candidates),
            Self::Ambiguous { candidates } => ("ambiguous provenance shorthand", candidates),
        };
        write!(f, "{label}")?;
        for candidate in candidates {
            write!(f, "\n  - {}", candidate.render())?;
        }
        Ok(())
    }
}

impl std::error::Error for ProvenanceLookupError {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct ProvenanceEntry {
    key: ProvenanceKey,
    address: ScopedNodeAddress,
    resolved: ResolvedValue<serde_json::Value>,
}

/// Side-table mapping `(PlanNodeId, ProvenanceField)` to provenance-tracked config values.
///
/// Kept separate from [`CompileArtifacts`](crate::plan::bind_schema::CompileArtifacts)
/// to avoid polluting the hot typecheck path. Only populated for
/// `PipelineNode::Composition` nodes that have config params.
///
/// Part of the `CompileOutput { plan, provenance }` separation.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ProvenanceDb {
    entries: BTreeMap<ProvenanceKey, ProvenanceEntry>,
}

impl ProvenanceDb {
    /// Insert one typed entry. Existing values for the same stable key are
    /// replaced; equal authored names in other scopes remain distinct.
    pub fn insert(
        &mut self,
        key: ProvenanceKey,
        address: ScopedNodeAddress,
        resolved: ResolvedValue<serde_json::Value>,
    ) {
        self.entries.insert(
            key.clone(),
            ProvenanceEntry {
                key,
                address,
                resolved,
            },
        );
    }

    /// Insert during binding, before the enclosing call path is finalized from
    /// the completed body graph.
    pub fn insert_unscoped(
        &mut self,
        node: PlanNodeId,
        node_name: impl Into<String>,
        param_name: impl Into<String>,
        resolved: ResolvedValue<serde_json::Value>,
    ) {
        let node_name = node_name.into();
        let field = ProvenanceField::ConfigParam(param_name.into());
        self.insert(
            ProvenanceKey {
                node,
                field: field.clone(),
            },
            ScopedNodeAddress::top_level(node_name, field),
            resolved,
        );
    }

    /// Attach the stable authored scope discovered from the finished plan.
    pub(crate) fn assign_scoped_node<I, S>(
        &mut self,
        node: PlanNodeId,
        call_path: I,
        node_name: &str,
    ) where
        I: IntoIterator<Item = S> + Clone,
        S: Into<String>,
    {
        for (key, entry) in &mut self.entries {
            if key.node == node {
                entry.address =
                    ScopedNodeAddress::new(call_path.clone(), node_name, key.field.clone());
            }
        }
    }

    /// Look up a unique shorthand. Ambiguity fails closed.
    pub fn get(
        &self,
        node_name: &str,
        param_name: &str,
    ) -> Option<&ResolvedValue<serde_json::Value>> {
        let query = ProvenanceQuery::Shorthand {
            node_name: node_name.to_owned(),
            field: ProvenanceField::ConfigParam(param_name.to_owned()),
        };
        self.resolve_query(&query).ok().map(|found| found.resolved)
    }

    /// Mutable unique-shorthand lookup. The typed key is resolved before the
    /// side table is mutably borrowed, so ambiguous aliases never mutate.
    pub fn get_mut(
        &mut self,
        node_name: &str,
        param_name: &str,
    ) -> Option<&mut ResolvedValue<serde_json::Value>> {
        let query = ProvenanceQuery::Shorthand {
            node_name: node_name.to_owned(),
            field: ProvenanceField::ConfigParam(param_name.to_owned()),
        };
        let key = self.resolve_query_key(&query).ok()?.clone();
        self.entries.get_mut(&key).map(|entry| &mut entry.resolved)
    }

    /// Mutably access a value after the caller has resolved its alias to a
    /// stable typed key.
    pub fn get_by_key_mut(
        &mut self,
        key: &ProvenanceKey,
    ) -> Option<&mut ResolvedValue<serde_json::Value>> {
        self.entries.get_mut(key).map(|entry| &mut entry.resolved)
    }

    /// Resolve an exact address or unique shorthand without mutation.
    pub fn resolve_query(
        &self,
        query: &ProvenanceQuery,
    ) -> Result<ProvenanceMatch<'_>, ProvenanceLookupError> {
        let key = self.resolve_query_key(query)?;
        let entry = &self.entries[key];
        Ok(ProvenanceMatch {
            key,
            address: &entry.address,
            resolved: &entry.resolved,
        })
    }

    /// Resolve a query to the internal key used by mutation paths.
    pub fn resolve_query_key(
        &self,
        query: &ProvenanceQuery,
    ) -> Result<&ProvenanceKey, ProvenanceLookupError> {
        let mut matches: Vec<&ProvenanceEntry> = self
            .entries
            .values()
            .filter(|entry| match query {
                ProvenanceQuery::Exact(address) => entry.address == *address,
                ProvenanceQuery::Shorthand { node_name, field } => {
                    entry.address.node_name() == node_name && entry.key.field == *field
                }
                ProvenanceQuery::SchemaExact(_) | ProvenanceQuery::SchemaShorthand(_) => false,
            })
            .collect();
        matches.sort_by(|left, right| left.address.cmp(&right.address));

        match matches.as_slice() {
            [entry] => Ok(&entry.key),
            [] => Err(ProvenanceLookupError::Unknown {
                candidates: query
                    .field()
                    .map(|field| self.same_field_candidates(field))
                    .unwrap_or_default(),
            }),
            _ => Err(ProvenanceLookupError::Ambiguous {
                candidates: matches
                    .into_iter()
                    .map(|entry| entry.address.clone())
                    .collect(),
            }),
        }
    }

    fn same_field_candidates(&self, field: &ProvenanceField) -> Vec<ScopedNodeAddress> {
        let mut candidates: Vec<_> = self
            .entries
            .values()
            .filter(|entry| &entry.key.field == field)
            .map(|entry| entry.address.clone())
            .collect();
        candidates.sort();
        candidates
    }

    /// Deterministic exact-address listing.
    pub fn canonical_listing(&self) -> Vec<ScopedNodeAddress> {
        let mut addresses: Vec<_> = self
            .entries
            .values()
            .map(|entry| entry.address.clone())
            .collect();
        addresses.sort();
        addresses
    }

    /// Iterate over all provenance entries.
    pub fn iter(
        &self,
    ) -> impl Iterator<
        Item = (
            &ProvenanceKey,
            &ScopedNodeAddress,
            &ResolvedValue<serde_json::Value>,
        ),
    > {
        self.entries
            .values()
            .map(|entry| (&entry.key, &entry.address, &entry.resolved))
    }

    /// List all param names tracked for a given node.
    pub fn params_for_node(&self, node_name: &str) -> Vec<&str> {
        self.entries
            .values()
            .filter(|entry| entry.address.node_name() == node_name)
            .map(|entry| entry.key.field.name())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// List all tracked node names.
    pub fn node_names(&self) -> Vec<&str> {
        self.entries
            .values()
            .map(|entry| entry.address.node_name())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    /// Number of tracked entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the provenance table is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Serialize for ProvenanceDb {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut sequence = serializer.serialize_seq(Some(self.entries.len()))?;
        for entry in self.entries.values() {
            sequence.serialize_element(entry)?;
        }
        sequence.end()
    }
}

impl<'de> Deserialize<'de> for ProvenanceDb {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let entries = Vec::<ProvenanceEntry>::deserialize(deserializer)?;
        let mut db = Self::default();
        for entry in entries {
            if entry.key.field != entry.address.field {
                return Err(D::Error::custom(
                    "provenance entry key field does not match its address field",
                ));
            }
            if db.entries.insert(entry.key.clone(), entry).is_some() {
                return Err(D::Error::custom("duplicate provenance key"));
            }
        }
        Ok(db)
    }
}

fn encode_pointer_segment(segment: &str) -> String {
    segment.replace('~', "~0").replace('/', "~1")
}

fn decode_pointer_segment(segment: &str) -> Result<String, ScopedNodeAddressParseError> {
    let mut output = String::with_capacity(segment.len());
    let mut chars = segment.chars();
    while let Some(ch) = chars.next() {
        if ch != '~' {
            output.push(ch);
            continue;
        }
        match chars.next() {
            Some('0') => output.push('~'),
            Some('1') => output.push('/'),
            _ => {
                return Err(ScopedNodeAddressParseError::Malformed(segment.to_owned()));
            }
        }
    }
    Ok(output)
}

mod span_serde {
    use super::*;

    pub fn serialize<S>(span: &Span, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        (span.file.get(), span.start, span.len).serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Span, D::Error>
    where
        D: Deserializer<'de>,
    {
        let (file, start, len) = <(u32, u32, u32)>::deserialize(deserializer)?;
        let file = NonZeroU32::new(file)
            .ok_or_else(|| D::Error::custom("provenance span file id must be non-zero"))?;
        Ok(Span {
            file: FileId::new(file),
            start,
            len,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::PlanNodeId;
    use crate::plan::entity::EntityRef;

    fn span(line: u32) -> Span {
        Span::line_only(line)
    }

    fn group(priority: i32, seq: u32) -> LayerKind {
        LayerKind::Group { priority, seq }
    }

    // ── LayerKind precedence ─────────────────────────────────────────────

    #[test]
    fn layer_kind_total_order_across_stack() {
        assert!(LayerKind::PipelineDefault < group(0, 0));
        assert!(group(1000, 999) < LayerKind::ChannelWide);
        assert!(LayerKind::ChannelWide < LayerKind::ChannelPerTarget);
    }

    #[test]
    fn group_ordering_priority_then_declaration() {
        // Higher priority is a greater layer.
        assert!(group(10, 5) < group(20, 0));
        // Equal priority: later declaration (higher seq) is greater.
        assert!(group(20, 1) < group(20, 2));
    }

    // ── Precedence resolution ────────────────────────────────────────────

    #[test]
    fn higher_layer_wins_over_lower() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, LayerKind::ChannelWide, span(2));
        assert_eq!(rv.value, 2);
        assert_eq!(rv.winning_layer().unwrap().kind, LayerKind::ChannelWide);
    }

    #[test]
    fn full_stack_precedence_channel_per_target_wins() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, group(20, 0), span(2));
        rv.apply_layer(3, LayerKind::ChannelWide, span(3));
        rv.apply_layer(4, LayerKind::ChannelPerTarget, span(4));
        assert_eq!(rv.value, 4);
        assert_eq!(
            rv.winning_layer().unwrap().kind,
            LayerKind::ChannelPerTarget
        );
    }

    #[test]
    fn lower_layer_applied_later_does_not_win() {
        // Applying a lower-precedence layer after a higher one must not flip
        // the winner — precedence is semantic, not positional.
        let mut rv = ResolvedValue::new(1, LayerKind::ChannelPerTarget, span(1));
        rv.apply_layer(2, LayerKind::PipelineDefault, span(2));
        assert_eq!(rv.value, 1);
        assert_eq!(
            rv.winning_layer().unwrap().kind,
            LayerKind::ChannelPerTarget
        );
    }

    // ── Multiple groups ──────────────────────────────────────────────────

    #[test]
    fn multiple_groups_highest_priority_wins() {
        let mut rv = ResolvedValue::new(0, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(10, group(10, 0), span(2));
        rv.apply_layer(30, group(30, 1), span(3));
        rv.apply_layer(20, group(20, 2), span(4));
        assert_eq!(rv.value, 30, "priority 30 group must win");
        assert_eq!(rv.winning_layer().unwrap().kind, group(30, 1));
        // Every group keeps its own shadowed value.
        assert_eq!(rv.layer_value(group(10, 0)), Some(&10));
        assert_eq!(rv.layer_value(group(20, 2)), Some(&20));
        assert_eq!(rv.layer_value(group(30, 1)), Some(&30));
    }

    #[test]
    fn groups_same_priority_later_declaration_wins() {
        let mut rv = ResolvedValue::new(0, LayerKind::PipelineDefault, span(1));
        // Apply in reverse declaration order to prove ordering is by seq, not
        // application order.
        rv.apply_layer(200, group(20, 2), span(2));
        rv.apply_layer(100, group(20, 1), span(3));
        assert_eq!(rv.value, 200, "later-declared group (seq 2) must win");
        assert_eq!(rv.winning_layer().unwrap().kind, group(20, 2));
    }

    // ── Fixed lock ───────────────────────────────────────────────────────

    #[test]
    fn fixed_lower_layer_wins_over_higher() {
        let mut rv = ResolvedValue::new_fixed(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, LayerKind::ChannelPerTarget, span(2));
        assert_eq!(rv.value, 1, "fixed PipelineDefault must not be overridden");
        assert_eq!(rv.winning_layer().unwrap().kind, LayerKind::PipelineDefault);
    }

    #[test]
    fn fixed_applied_mid_stack_locks_downstream() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer_fixed(2, LayerKind::ChannelWide, span(2));
        rv.apply_layer(3, LayerKind::ChannelPerTarget, span(3));
        assert_eq!(rv.value, 2, "fixed ChannelWide locks out ChannelPerTarget");
        assert_eq!(rv.winning_layer().unwrap().kind, LayerKind::ChannelWide);
    }

    #[test]
    fn lowest_fixed_layer_wins_among_several_fixed() {
        let mut rv = ResolvedValue::new_fixed(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer_fixed(2, LayerKind::ChannelWide, span(2));
        assert_eq!(
            rv.value, 1,
            "lowest-precedence fixed layer locked the value first"
        );
        assert_eq!(rv.winning_layer().unwrap().kind, LayerKind::PipelineDefault);
    }

    #[test]
    fn non_fixed_higher_still_wins_when_lower_not_fixed() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer_fixed(2, LayerKind::ChannelPerTarget, span(2));
        assert_eq!(rv.value, 2);
        assert_eq!(
            rv.winning_layer().unwrap().kind,
            LayerKind::ChannelPerTarget
        );
    }

    // ── Exactly-one-winner invariant ─────────────────────────────────────

    #[test]
    fn exactly_one_winning_layer() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, group(10, 0), span(2));
        rv.apply_layer_fixed(3, group(20, 1), span(3));
        rv.apply_layer(4, LayerKind::ChannelWide, span(4));
        rv.apply_layer(5, LayerKind::ChannelPerTarget, span(5));
        let winners = rv.provenance.iter().filter(|l| l.won).count();
        assert_eq!(winners, 1, "exactly one layer must win");
        // The fixed group locks out both channel layers.
        assert_eq!(rv.value, 3);
        assert_eq!(rv.winning_layer().unwrap().kind, group(20, 1));
    }

    #[test]
    fn same_kind_replaces_in_place() {
        let mut rv = ResolvedValue::new(1, LayerKind::ChannelWide, span(1));
        rv.apply_layer(2, LayerKind::ChannelWide, span(9));
        assert_eq!(rv.value, 2);
        // A single ChannelWide provenance entry, with the updated span.
        let entries: Vec<_> = rv
            .provenance
            .iter()
            .filter(|l| l.kind == LayerKind::ChannelWide)
            .collect();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].span, span(9));
    }

    // ── Shadowed value retention ─────────────────────────────────────────

    #[test]
    fn shadowed_layer_values_are_retained() {
        let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, span(1));
        rv.apply_layer(2, LayerKind::ChannelPerTarget, span(2));
        assert_eq!(rv.layer_value(LayerKind::PipelineDefault), Some(&1));
        assert_eq!(rv.layer_value(LayerKind::ChannelPerTarget), Some(&2));
    }

    // ── Generic reuse over a different layer kind ────────────────────────

    #[test]
    fn generic_over_arbitrary_layer_kind() {
        // A stand-in for the future SchemaLayer proves the implementation is
        // reused with zero duplication for any `Copy + Ord` layer kind.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
        enum SchemaLayer {
            Base,
            Pipeline,
            Group,
            Channel,
        }

        let mut rv = ResolvedValue::<&str, SchemaLayer>::new("base", SchemaLayer::Base, span(1));
        rv.apply_layer("pipeline", SchemaLayer::Pipeline, span(2));
        rv.apply_layer_fixed("group", SchemaLayer::Group, span(3));
        rv.apply_layer("channel", SchemaLayer::Channel, span(4));
        assert_eq!(rv.value, "group", "fixed group layer locks out channel");
        assert_eq!(rv.winning_layer().unwrap().kind, SchemaLayer::Group);
        assert_eq!(rv.provenance.iter().filter(|l| l.won).count(), 1);
    }

    #[test]
    fn provenance_layer_fits_in_cache_budget() {
        assert!(
            std::mem::size_of::<ProvenanceLayer>() <= 64,
            "ProvenanceLayer must fit in 64 bytes, got {}",
            std::mem::size_of::<ProvenanceLayer>()
        );
    }

    #[test]
    fn unicode_address_round_trip() {
        let address = ScopedNodeAddress::config_param(
            ["親呼び出し", "déjà/vu~again"],
            "正規化/ノード~1",
            "しきい値/~",
        );
        let rendered = address.render();
        assert_eq!(ScopedNodeAddress::parse(&rendered).unwrap(), address);
        assert!(rendered.contains("~1"), "slash must use RFC 6901 escaping");
        assert!(rendered.contains("~0"), "tilde must use RFC 6901 escaping");
    }

    #[test]
    fn reserved_separator_round_trip_does_not_normalize_segments() {
        let left = ScopedNodeAddress::config_param(["a/b"], "c", "d~e");
        let right = ScopedNodeAddress::config_param(["a"], "b/c", "d~e");
        assert_ne!(left, right);
        assert_ne!(left.render(), right.render());
        assert_eq!(ScopedNodeAddress::parse(&left.render()).unwrap(), left);
        assert_eq!(ScopedNodeAddress::parse(&right.render()).unwrap(), right);
    }

    #[test]
    fn empty_query_is_rejected_before_lookup() {
        assert_eq!(
            ProvenanceQuery::parse(""),
            Err(ProvenanceQueryParseError::Empty)
        );
    }

    #[test]
    fn ambiguous_shorthand_lists_deterministic_scoped_candidates() {
        let mut db = ProvenanceDb::default();
        let field = ProvenanceField::ConfigParam("threshold".to_owned());
        for (index, parent, value) in [(7, "zeta", 7), (3, "alpha", 3)] {
            let key = ProvenanceKey {
                node: PlanNodeId::new(index),
                field: field.clone(),
            };
            let address = ScopedNodeAddress::new([parent], "shared", field.clone());
            db.insert(
                key,
                address,
                ResolvedValue::new(
                    serde_json::json!(value),
                    LayerKind::PipelineDefault,
                    span(index as u32),
                ),
            );
        }

        let query = ProvenanceQuery::parse("shared.threshold").unwrap();
        let error = db.resolve_query(&query).unwrap_err();
        assert_eq!(
            error,
            ProvenanceLookupError::Ambiguous {
                candidates: vec![
                    ScopedNodeAddress::config_param(["alpha"], "shared", "threshold"),
                    ScopedNodeAddress::config_param(["zeta"], "shared", "threshold"),
                ],
            }
        );
    }

    #[test]
    fn unknown_query_lists_only_same_field_candidates() {
        let mut db = ProvenanceDb::default();
        for (index, node, field) in [
            (0, "alpha", "threshold"),
            (1, "beta", "threshold"),
            (2, "unrelated", "mode"),
        ] {
            let field = ProvenanceField::ConfigParam(field.to_owned());
            db.insert(
                ProvenanceKey {
                    node: PlanNodeId::new(index),
                    field: field.clone(),
                },
                ScopedNodeAddress::top_level(node, field),
                ResolvedValue::new(
                    serde_json::json!(index),
                    LayerKind::PipelineDefault,
                    span(index as u32 + 1),
                ),
            );
        }

        let query = ProvenanceQuery::parse("missing.threshold").unwrap();
        let error = db.resolve_query(&query).unwrap_err();
        assert_eq!(
            error,
            ProvenanceLookupError::Unknown {
                candidates: vec![
                    ScopedNodeAddress::top_level_config_param("alpha", "threshold"),
                    ScopedNodeAddress::top_level_config_param("beta", "threshold"),
                ],
            }
        );
    }

    #[test]
    fn canonical_listing_and_serialized_overlay_attempts_are_reusable() {
        let mut db = ProvenanceDb::default();
        let key = ProvenanceKey {
            node: PlanNodeId::new(9),
            field: ProvenanceField::ConfigParam("threshold".to_owned()),
        };
        let address = ScopedNodeAddress::config_param(["outer"], "shared", "threshold");
        let mut resolved =
            ResolvedValue::new(serde_json::json!(10), LayerKind::PipelineDefault, span(1));
        resolved.apply_layer(serde_json::json!(20), group(10, 0), span(2));
        resolved.apply_layer(serde_json::json!(30), LayerKind::ChannelWide, span(3));
        resolved.apply_layer_fixed(serde_json::json!(40), LayerKind::ChannelPerTarget, span(4));
        db.insert(key.clone(), address.clone(), resolved);

        let encoded = serde_json::to_vec(&db).unwrap();
        let restored: ProvenanceDb = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(restored.canonical_listing(), vec![address.clone()]);

        for _ in 0..2 {
            let query = ProvenanceQuery::parse(&address.render()).unwrap();
            let found = restored.resolve_query(&query).unwrap();
            assert_eq!(*found.key, key);
            assert_eq!(found.resolved.value, serde_json::json!(40));
            assert_eq!(found.resolved.provenance.len(), 4);
            assert_eq!(found.resolved.layer_values.len(), 4);
            assert_eq!(
                found.resolved.winning_layer().unwrap().kind,
                LayerKind::ChannelPerTarget
            );
        }
    }
}
