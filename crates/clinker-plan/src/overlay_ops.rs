//! Structural overlay ops for the channel/group multi-tenant overlay system.
//!
//! The `overrides:` surface on a group/channel overlay is an **ordered list of
//! discrete, name-addressed ops** applied to a base pipeline's node list
//! *before* schema binding and compilation. This module defines the typed op
//! vocabulary ([`OverlayOp`]) and the engine ([`apply_overlay_ops`]) that folds
//! a stream of ops over a `Vec<Spanned<PipelineNode>>`.
//!
//! # Why explicit ops instead of a deep merge
//!
//! Every documented failure mode of layered config (strategic-merge
//! non-associativity, wholesale array replacement, silent key typos, "where did
//! this value come from?") traces back to *implicit* merging. Discrete
//! name-keyed ops applied in a total order are instead deterministic,
//! 1:1-provenance-traceable, and typo-detecting: a missing target is an error,
//! never a silent no-op.
//!
//! # Total order
//!
//! Ops carry a [`OverlayLayer`] precedence. [`apply_overlay_ops`] applies them
//! in `(layer precedence, declaration order)` order: it *stable*-sorts by layer,
//! so ops within one layer keep the caller-supplied declaration order while ops
//! across layers are ordered by fixed semantic precedence
//! (`pipeline-default < group(by priority) < channel-wide < channel-per-target`).
//! The applied result therefore depends only on the layer/declaration identity
//! of each op, never on the order the layers happened to be concatenated in.
//!
//! # Scope
//!
//! This module implements the structural ops — [`AddOp`], [`RemoveOp`],
//! [`ReplaceOp`] — plus the field-level ops [`SetOp`] (set a field within a
//! node by path, including `config.cxl` to replace a stage's logic wholesale),
//! [`BypassOp`] (sugar for removing and auto-rewiring a 1-in/1-out node), and
//! [`PatchSchemaOp`] (add / rename / retype / remove columns on a `Source`
//! node's declared schema via the column-keyed grammar shared with the channel
//! source-config patch pass). Each `op:` discriminant slots into
//! `RawOverlayOp::into_op` and [`apply_overlay_ops`].

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use indexmap::IndexMap;
use serde::{Deserialize, Deserializer};
use serde_saphyr::Location;

use clinker_core_types::span::Span;

use crate::config::node_header::{NodeHeader, NodeInput};
use crate::config::patch::apply_schema_ops;
use crate::config::{PipelineNode, SchemaColumnOp};
use crate::yaml::{CxlSource, Spanned};

// ---------------------------------------------------------------------
// Layer precedence
// ---------------------------------------------------------------------

/// Fixed semantic precedence of the layer an op came from.
///
/// Ordering is `PipelineDefault < Group{..} < ChannelWide < ChannelPerTarget`,
/// with groups ordered by ascending `priority` so a higher-priority group
/// applies *later* (and thus wins clobber contests). The `Ord` derive encodes
/// this directly: enum variants compare by declaration order first, and two
/// `Group` values compare by their `priority` field.
///
/// This is the ordering key [`apply_overlay_ops`] sorts on. Loading groups /
/// channels from disk and assigning these layers is a later concern in the
/// epic; this type is the seam those layers resolve to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OverlayLayer {
    /// The base pipeline's own `overrides:` (lowest precedence).
    PipelineDefault,
    /// A matching group's `overrides:`. Higher `priority` wins.
    Group { priority: i64 },
    /// A channel manifest's channel-wide `overrides:`.
    ChannelWide,
    /// A channel's per-target overlay `overrides:` (highest precedence).
    ChannelPerTarget,
}

// ---------------------------------------------------------------------
// Op vocabulary
// ---------------------------------------------------------------------

/// One structural override op.
///
/// Deserializes from the canonical `{ op: <kind>, ... }` YAML mapping via a
/// strict intermediate (`RawOverlayOp`) that rejects unknown keys and keys that
/// belong to a different op kind. Wrapping an op as `Spanned<OverlayOp>` (the
/// shape the `overrides:` list uses) preserves the op's source location even
/// though `OverlayOp` routes through a custom deserializer — that location is
/// what the engine stamps onto injected/replaced nodes so a later ill-typed-op
/// diagnostic points at the op, not the base pipeline.
#[derive(Debug, Clone)]
pub enum OverlayOp {
    /// Insert a new node (inline or a composition reference), spliced into the
    /// graph via `after` / `before` / explicit `input` wiring.
    Add(AddOp),
    /// Remove a node by name, repointing named consumers via an explicit
    /// `rewire` map so no dangling reference is left behind.
    Remove(RemoveOp),
    /// Replace a whole node by name, keeping its identity (and thus every
    /// consumer edge) intact.
    Replace(ReplaceOp),
    /// Set a single field within a named node by path — currently `config.cxl`,
    /// which replaces a stage's CXL logic wholesale.
    Set(SetOp),
    /// Remove a 1-in/1-out node and auto-rewire its sole consumer onto its sole
    /// upstream — sugar for a `remove` with a single-edge `rewire`.
    Bypass(BypassOp),
    /// Add / rename / retype / remove columns on a `Source` node's declared
    /// schema, keyed by column name.
    PatchSchema(PatchSchemaOp),
}

/// The `add` op: splice a new node into the base graph.
///
/// Exactly one of [`node`](Self::node) (an inline node) or
/// [`composition`](Self::composition) (a `.comp.yaml` reference, named by
/// [`alias`](Self::alias)) supplies the new node. The splice anchor is exactly
/// one of `after` / `before` / explicit `input`; an inline node with none of
/// those keeps its own declared `input:`.
#[derive(Debug, Clone)]
pub struct AddOp {
    /// Inline node definition. Mutually exclusive with `composition`.
    pub node: Option<PipelineNode>,
    /// Path to a composition to splice in. Requires `alias` and a splice
    /// anchor; mutually exclusive with `node`.
    pub composition: Option<PathBuf>,
    /// Node name for a `composition` add (the injected node's identity).
    pub alias: Option<String>,
    /// Config knobs forwarded to a `composition` add's node.
    pub config: IndexMap<String, serde_json::Value>,
    /// Splice anchor: read from this node, and repoint that node's former
    /// consumers to the new node.
    pub after: Option<String>,
    /// Splice anchor: feed this node, taking over its former upstream input.
    pub before: Option<String>,
    /// Explicit upstream wiring for the new node (no consumer rewiring).
    pub input: Option<NodeInput>,
}

/// The `remove` op: delete a node and rewire its consumers explicitly.
#[derive(Debug, Clone)]
pub struct RemoveOp {
    /// Name of the node to remove. Must exist.
    pub target: String,
    /// Consumer rewiring applied before removal. Each key is a `<node>.input`
    /// path; each value is the replacement upstream reference. Any consumer
    /// still referencing `target` after rewiring is an error.
    pub rewire: IndexMap<String, NodeInput>,
}

/// The `replace` op: swap a node's definition in place, keeping its name.
#[derive(Debug, Clone)]
pub struct ReplaceOp {
    /// Name of the node to replace. Must exist and must equal the
    /// replacement node's own name.
    pub target: String,
    /// The replacement node.
    pub node: PipelineNode,
}

/// The `set` op: set a single field within a named node by path.
///
/// The uniform-op-model reason "override the whole CXL" is *not* a special
/// case — CXL is just a node field, so replacing a stage's logic is a
/// `set <node> config.cxl: <program>`. The currently addressable path is
/// `config.cxl` (the primary CXL body of a `Transform` / `Aggregate` /
/// `Combine`); any other path is a hard [`OverlayOpError::UnknownField`] rather
/// than a silent no-op.
#[derive(Debug, Clone)]
pub struct SetOp {
    /// Name of the node whose field is being set. Must exist.
    pub target: String,
    /// Dotted field path within the node — currently only `config.cxl`.
    pub field: String,
    /// The new value. For `config.cxl` this must be a CXL source string.
    pub value: serde_json::Value,
}

/// The `bypass` op: sugar for removing a 1-in/1-out node and auto-rewiring.
///
/// Equivalent to a `remove` whose `rewire` repoints the node's sole consumer
/// onto the node's sole upstream input. The target must be a single-input
/// consumer node fed into by exactly one downstream consumer; anything else is
/// a hard [`OverlayOpError::BypassInvalid`] (a fan-in/fan-out node must use the
/// explicit `remove` op with a spelled-out `rewire` map).
#[derive(Debug, Clone)]
pub struct BypassOp {
    /// Name of the linear node to bypass. Must exist.
    pub target: String,
}

/// The `patch_schema` op: shape a `Source` node's declared columns.
///
/// The payload is the column-name-keyed grammar shared with the channel
/// source-config patch pass (`add` / `rename` / `retype` / `remove`), applied
/// by the same [`apply_schema_ops`] engine so both surfaces resolve columns and
/// their E231–E233 diagnostics identically. The target must be a `Source` node.
#[derive(Debug, Clone)]
pub struct PatchSchemaOp {
    /// Name of the `Source` node whose schema is patched. Must exist and be a
    /// `Source`.
    pub target: String,
    /// Column-name-keyed schema ops.
    pub schema: IndexMap<String, SchemaColumnOp>,
}

/// An op paired with the layer it came from — the unit [`apply_overlay_ops`]
/// consumes. The [`Spanned`] wrapper carries the op's source location.
#[derive(Debug, Clone)]
pub struct LayeredOp {
    /// Precedence layer this op belongs to.
    pub layer: OverlayLayer,
    /// The op plus its source location.
    pub op: Spanned<OverlayOp>,
}

impl LayeredOp {
    /// Convenience constructor.
    pub fn new(layer: OverlayLayer, op: Spanned<OverlayOp>) -> Self {
        Self { layer, op }
    }
}

// ---------------------------------------------------------------------
// Deserialization (strict, op-tag dispatch)
// ---------------------------------------------------------------------

/// The `op:` discriminant. An unmodeled tag surfaces as a clear "unknown
/// variant" error.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OverlayOpKind {
    Add,
    Remove,
    Replace,
    Set,
    Bypass,
    PatchSchema,
}

/// Flat, strict wire form. `deny_unknown_fields` rejects typo'd keys; the
/// per-kind routing in [`Self::into_op`] rejects keys that belong to a
/// different op kind, so each op stays a closed, self-describing shape.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawOverlayOp {
    op: OverlayOpKind,
    #[serde(default)]
    node: Option<PipelineNode>,
    #[serde(default)]
    composition: Option<PathBuf>,
    #[serde(default)]
    alias: Option<String>,
    #[serde(default)]
    config: IndexMap<String, serde_json::Value>,
    #[serde(default)]
    after: Option<String>,
    #[serde(default)]
    before: Option<String>,
    #[serde(default)]
    input: Option<NodeInput>,
    #[serde(default)]
    target: Option<String>,
    #[serde(default)]
    rewire: IndexMap<String, NodeInput>,
    #[serde(default)]
    field: Option<String>,
    #[serde(default)]
    value: Option<serde_json::Value>,
    #[serde(default)]
    schema: IndexMap<String, SchemaColumnOp>,
}

impl RawOverlayOp {
    fn into_op(self) -> Result<OverlayOp, String> {
        let RawOverlayOp {
            op,
            node,
            composition,
            alias,
            config,
            after,
            before,
            input,
            target,
            rewire,
            field,
            value,
            schema,
        } = self;

        match op {
            OverlayOpKind::Add => {
                reject(target.is_some(), "add", "target")?;
                reject(!rewire.is_empty(), "add", "rewire")?;
                reject(field.is_some(), "add", "field")?;
                reject(value.is_some(), "add", "value")?;
                reject(!schema.is_empty(), "add", "schema")?;
                Ok(OverlayOp::Add(AddOp {
                    node,
                    composition,
                    alias,
                    config,
                    after,
                    before,
                    input,
                }))
            }
            OverlayOpKind::Remove => {
                reject(node.is_some(), "remove", "node")?;
                reject(composition.is_some(), "remove", "composition")?;
                reject(alias.is_some(), "remove", "alias")?;
                reject(!config.is_empty(), "remove", "config")?;
                reject(after.is_some(), "remove", "after")?;
                reject(before.is_some(), "remove", "before")?;
                reject(input.is_some(), "remove", "input")?;
                reject(field.is_some(), "remove", "field")?;
                reject(value.is_some(), "remove", "value")?;
                reject(!schema.is_empty(), "remove", "schema")?;
                let target = target.ok_or("`remove` op requires a `target`")?;
                Ok(OverlayOp::Remove(RemoveOp { target, rewire }))
            }
            OverlayOpKind::Replace => {
                reject(composition.is_some(), "replace", "composition")?;
                reject(alias.is_some(), "replace", "alias")?;
                reject(!config.is_empty(), "replace", "config")?;
                reject(after.is_some(), "replace", "after")?;
                reject(before.is_some(), "replace", "before")?;
                reject(input.is_some(), "replace", "input")?;
                reject(!rewire.is_empty(), "replace", "rewire")?;
                reject(field.is_some(), "replace", "field")?;
                reject(value.is_some(), "replace", "value")?;
                reject(!schema.is_empty(), "replace", "schema")?;
                let target = target.ok_or("`replace` op requires a `target`")?;
                let node = node.ok_or("`replace` op requires a `node`")?;
                Ok(OverlayOp::Replace(ReplaceOp { target, node }))
            }
            OverlayOpKind::Set => {
                reject(node.is_some(), "set", "node")?;
                reject(composition.is_some(), "set", "composition")?;
                reject(alias.is_some(), "set", "alias")?;
                reject(!config.is_empty(), "set", "config")?;
                reject(after.is_some(), "set", "after")?;
                reject(before.is_some(), "set", "before")?;
                reject(input.is_some(), "set", "input")?;
                reject(!rewire.is_empty(), "set", "rewire")?;
                reject(!schema.is_empty(), "set", "schema")?;
                let target = target.ok_or("`set` op requires a `target`")?;
                let field = field.ok_or("`set` op requires a `field`")?;
                let value = value.ok_or("`set` op requires a `value`")?;
                Ok(OverlayOp::Set(SetOp {
                    target,
                    field,
                    value,
                }))
            }
            OverlayOpKind::Bypass => {
                reject(node.is_some(), "bypass", "node")?;
                reject(composition.is_some(), "bypass", "composition")?;
                reject(alias.is_some(), "bypass", "alias")?;
                reject(!config.is_empty(), "bypass", "config")?;
                reject(after.is_some(), "bypass", "after")?;
                reject(before.is_some(), "bypass", "before")?;
                reject(input.is_some(), "bypass", "input")?;
                reject(!rewire.is_empty(), "bypass", "rewire")?;
                reject(field.is_some(), "bypass", "field")?;
                reject(value.is_some(), "bypass", "value")?;
                reject(!schema.is_empty(), "bypass", "schema")?;
                let target = target.ok_or("`bypass` op requires a `target`")?;
                Ok(OverlayOp::Bypass(BypassOp { target }))
            }
            OverlayOpKind::PatchSchema => {
                reject(node.is_some(), "patch_schema", "node")?;
                reject(composition.is_some(), "patch_schema", "composition")?;
                reject(alias.is_some(), "patch_schema", "alias")?;
                reject(!config.is_empty(), "patch_schema", "config")?;
                reject(after.is_some(), "patch_schema", "after")?;
                reject(before.is_some(), "patch_schema", "before")?;
                reject(input.is_some(), "patch_schema", "input")?;
                reject(!rewire.is_empty(), "patch_schema", "rewire")?;
                reject(field.is_some(), "patch_schema", "field")?;
                reject(value.is_some(), "patch_schema", "value")?;
                let target = target.ok_or("`patch_schema` op requires a `target`")?;
                if schema.is_empty() {
                    return Err("`patch_schema` op requires a non-empty `schema`".to_string());
                }
                Ok(OverlayOp::PatchSchema(PatchSchemaOp { target, schema }))
            }
        }
    }
}

/// Reject a key that is set but not valid on the given op kind.
fn reject(is_set: bool, op: &str, field: &str) -> Result<(), String> {
    if is_set {
        Err(format!("`{field}` is not a valid field on a `{op}` op"))
    } else {
        Ok(())
    }
}

impl<'de> Deserialize<'de> for OverlayOp {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = RawOverlayOp::deserialize(deserializer)?;
        raw.into_op().map_err(serde::de::Error::custom)
    }
}

// ---------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------

/// A failure applying an overlay op. Each variant carries the op's source
/// [`Span`] (line-only, built from the op's YAML location) so callers can
/// surface a diagnostic anchored to the offending op.
#[derive(Debug, Clone)]
pub enum OverlayOpError {
    /// An op named a target node that is not present (never existed, or was
    /// removed by an earlier op).
    MissingTarget {
        /// The op kind (`add` splice anchor, `remove`, `replace`).
        op: &'static str,
        /// The absent node name.
        target: String,
        span: Span,
    },
    /// An `add` would introduce a node whose name already exists.
    DuplicateAdd { name: String, span: Span },
    /// An op's field combination is invalid (e.g. both `node` and
    /// `composition`, or more than one splice anchor).
    Malformed { message: String, span: Span },
    /// A splice / rewire / explicit-input reference names a node that does not
    /// exist in the current graph.
    UndeclaredReference {
        reference: String,
        context: String,
        span: Span,
    },
    /// Removing a node left a consumer referencing it with no rewire entry.
    DanglingReference {
        removed: String,
        consumer: String,
        span: Span,
    },
    /// A `replace` node's own name does not match the target it replaces.
    ReplaceNameMismatch {
        target: String,
        node_name: String,
        span: Span,
    },
    /// A splice / rewire targeted a variant that has no single primary input
    /// (a `Source`, `Merge`, `Combine`, or `Envelope`).
    NotSingleInput {
        node: String,
        context: String,
        span: Span,
    },
    /// A `set` op named a field path the target node does not expose.
    UnknownField {
        target: String,
        field: String,
        span: Span,
    },
    /// A `patch_schema` op targeted a node that is not a `Source`.
    NotSource {
        target: String,
        actual: &'static str,
        span: Span,
    },
    /// A `patch_schema` column op failed. `message` carries the reused
    /// column-op diagnostic (E231–E233) from the shared schema-op engine.
    PatchSchema { message: String, span: Span },
    /// A `bypass` target is not a 1-in/1-out linear node. `reason` states which
    /// precondition failed (no single upstream, or not exactly one consumer).
    BypassInvalid {
        target: String,
        reason: String,
        span: Span,
    },
}

impl OverlayOpError {
    /// The op-anchored span for this error.
    pub fn span(&self) -> Span {
        match self {
            OverlayOpError::MissingTarget { span, .. }
            | OverlayOpError::DuplicateAdd { span, .. }
            | OverlayOpError::Malformed { span, .. }
            | OverlayOpError::UndeclaredReference { span, .. }
            | OverlayOpError::DanglingReference { span, .. }
            | OverlayOpError::ReplaceNameMismatch { span, .. }
            | OverlayOpError::NotSingleInput { span, .. }
            | OverlayOpError::UnknownField { span, .. }
            | OverlayOpError::NotSource { span, .. }
            | OverlayOpError::PatchSchema { span, .. }
            | OverlayOpError::BypassInvalid { span, .. } => *span,
        }
    }
}

/// Render " (op at line N)" when the span carries a known line, else "".
fn at(span: Span) -> String {
    match span.synthetic_line_number() {
        Some(line) => format!(" (op at line {line})"),
        None => String::new(),
    }
}

impl std::fmt::Display for OverlayOpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OverlayOpError::MissingTarget { op, target, span } => write!(
                f,
                "`{op}` op targets node {target:?}, which does not exist{}",
                at(*span)
            ),
            OverlayOpError::DuplicateAdd { name, span } => write!(
                f,
                "`add` op would introduce node {name:?}, which already exists{}",
                at(*span)
            ),
            OverlayOpError::Malformed { message, span } => {
                write!(f, "malformed overlay op: {message}{}", at(*span))
            }
            OverlayOpError::UndeclaredReference {
                reference,
                context,
                span,
            } => write!(
                f,
                "{context} references undeclared node {reference:?}{}",
                at(*span)
            ),
            OverlayOpError::DanglingReference {
                removed,
                consumer,
                span,
            } => write!(
                f,
                "removing node {removed:?} leaves node {consumer:?} referencing it; \
                 add a `rewire` entry for {consumer:?}{}",
                at(*span)
            ),
            OverlayOpError::ReplaceNameMismatch {
                target,
                node_name,
                span,
            } => write!(
                f,
                "`replace` target {target:?} does not match the replacement node's \
                 own name {node_name:?}{}",
                at(*span)
            ),
            OverlayOpError::NotSingleInput {
                node,
                context,
                span,
            } => write!(
                f,
                "{context} requires node {node:?} to have a single input, but it does \
                 not{}",
                at(*span)
            ),
            OverlayOpError::UnknownField {
                target,
                field,
                span,
            } => write!(
                f,
                "`set` op targets field {field:?} on node {target:?}, which does not \
                 expose it (currently only `config.cxl` is settable){}",
                at(*span)
            ),
            OverlayOpError::NotSource {
                target,
                actual,
                span,
            } => write!(
                f,
                "`patch_schema` op targets node {target:?}, which is a {actual} node, \
                 not a source{}",
                at(*span)
            ),
            OverlayOpError::PatchSchema { message, span } => {
                write!(f, "`patch_schema` op failed: {message}{}", at(*span))
            }
            OverlayOpError::BypassInvalid {
                target,
                reason,
                span,
            } => write!(
                f,
                "`bypass` op cannot bypass node {target:?}: {reason}{}",
                at(*span)
            ),
        }
    }
}

impl std::error::Error for OverlayOpError {}

// ---------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------

/// Declared input port names for each composition an `add composition` op may
/// splice in, keyed by the op's raw `composition:` path (exactly as the op
/// carries it). The overlay driver builds this by scanning workspace
/// composition signatures before applying ops, so an injected composition's
/// `inputs:` port map can be populated the same way a hand-authored call
/// site's is. A path absent from the map — or one whose composition declares
/// zero or more than one input port — leaves the injected node's `inputs:`
/// empty, deferring the real port diagnostic to `bind_schema`.
pub type CompositionInputPorts = HashMap<PathBuf, Vec<String>>;

/// Apply a stream of layered overlay ops to a base node list, returning the
/// transformed list.
///
/// Ops are applied in `(layer precedence, declaration order)` order via a
/// stable sort on [`OverlayLayer`]; the result is therefore independent of the
/// order the caller concatenated the ops in. Every op must resolve against a
/// live node — a missing splice anchor, remove/replace target, or dangling
/// reference is a hard [`OverlayOpError`], never a silent no-op.
///
/// Injected compositions get empty `inputs:` port maps; use
/// [`apply_overlay_ops_with_ports`] to bind each splice anchor to the
/// composition's declared input port.
pub fn apply_overlay_ops(
    nodes: Vec<Spanned<PipelineNode>>,
    ops: Vec<LayeredOp>,
) -> Result<Vec<Spanned<PipelineNode>>, OverlayOpError> {
    apply_overlay_ops_with_ports(nodes, ops, &CompositionInputPorts::new())
}

/// [`apply_overlay_ops`] with composition input-port bindings.
///
/// For each `add composition` op whose `composition:` path resolves in `ports`
/// to a single declared input port, the injected node's `inputs:` map binds
/// that port to the splice anchor — the same call-site binding a hand-authored
/// composition node carries — so the injected node is fed by the top-level edge
/// wiring instead of being left a dead, unfed node.
pub fn apply_overlay_ops_with_ports(
    mut nodes: Vec<Spanned<PipelineNode>>,
    mut ops: Vec<LayeredOp>,
    ports: &CompositionInputPorts,
) -> Result<Vec<Spanned<PipelineNode>>, OverlayOpError> {
    // Stable sort by layer: cross-layer order follows fixed semantic
    // precedence, while ops sharing a layer keep the caller's declaration
    // order. This is the "(layer, declaration order)" total order.
    ops.sort_by(|a, b| a.layer.cmp(&b.layer));

    for layered in ops {
        let loc = layered.op.referenced;
        let span = span_from_loc(loc);
        match layered.op.value {
            OverlayOp::Add(add) => apply_add(&mut nodes, add, ports, loc, span)?,
            OverlayOp::Remove(remove) => apply_remove(&mut nodes, remove, span)?,
            OverlayOp::Replace(replace) => apply_replace(&mut nodes, replace, loc, span)?,
            OverlayOp::Set(set) => apply_set(&mut nodes, set, loc, span)?,
            OverlayOp::Bypass(bypass) => apply_bypass(&mut nodes, bypass, span)?,
            OverlayOp::PatchSchema(patch) => apply_patch_schema(&mut nodes, patch, span)?,
        }
    }

    Ok(nodes)
}

/// Line-only span from an op's YAML location (matches the compile path's
/// synthetic-span convention). Falls back to [`Span::SYNTHETIC`] when the
/// location is unknown.
fn span_from_loc(loc: Location) -> Span {
    let line = loc.line() as u32;
    if line > 0 {
        Span::line_only(line)
    } else {
        Span::SYNTHETIC
    }
}

fn index_of(nodes: &[Spanned<PipelineNode>], name: &str) -> Option<usize> {
    nodes.iter().position(|n| n.value.name() == name)
}

/// Resolved splice anchor for an [`AddOp`].
enum Splice {
    /// Read from the anchor; repoint the anchor's former consumers to the new
    /// node.
    After(String),
    /// Feed the anchor; take over the anchor's former upstream input.
    Before(String),
    /// Explicit op-level `input:` wiring; no consumer rewiring.
    ExplicitInput(NodeInput),
    /// No splice directive — an inline node keeps its own declared `input:`.
    NodeDeclared,
}

impl AddOp {
    /// Resolve the (at most one) splice anchor.
    fn resolve_splice(&self, span: Span) -> Result<Splice, OverlayOpError> {
        let selectors =
            self.after.is_some() as u8 + self.before.is_some() as u8 + self.input.is_some() as u8;
        if selectors > 1 {
            return Err(OverlayOpError::Malformed {
                message: "specify at most one of `after`, `before`, or `input`".to_string(),
                span,
            });
        }
        Ok(match (&self.after, &self.before, &self.input) {
            (Some(a), _, _) => Splice::After(a.clone()),
            (_, Some(b), _) => Splice::Before(b.clone()),
            (_, _, Some(i)) => Splice::ExplicitInput(i.clone()),
            _ => Splice::NodeDeclared,
        })
    }

    /// The name the new node will carry, validating node/composition
    /// exclusivity and the keys each source permits.
    fn new_node_name(&self, span: Span) -> Result<String, OverlayOpError> {
        match (&self.node, &self.composition) {
            (Some(node), None) => {
                if self.alias.is_some() {
                    return Err(OverlayOpError::Malformed {
                        message: "`alias` is only valid on a `composition` add".to_string(),
                        span,
                    });
                }
                if !self.config.is_empty() {
                    return Err(OverlayOpError::Malformed {
                        message: "`config` is only valid on a `composition` add".to_string(),
                        span,
                    });
                }
                Ok(node.name().to_string())
            }
            (None, Some(_)) => self.alias.clone().ok_or(OverlayOpError::Malformed {
                message: "a `composition` add requires an `alias`".to_string(),
                span,
            }),
            (Some(_), Some(_)) => Err(OverlayOpError::Malformed {
                message: "specify exactly one of `node` or `composition`".to_string(),
                span,
            }),
            (None, None) => Err(OverlayOpError::Malformed {
                message: "an `add` op requires a `node` or a `composition`".to_string(),
                span,
            }),
        }
    }

    /// Build the new node with its upstream input set. `input` is `None` only
    /// for the [`Splice::NodeDeclared`] case, where an inline node keeps its
    /// own declared `input:`.
    fn build_node(
        &self,
        input: Option<NodeInput>,
        ports: &CompositionInputPorts,
        loc: Location,
        span: Span,
    ) -> Result<PipelineNode, OverlayOpError> {
        match (&self.node, &self.composition) {
            (Some(node), None) => {
                let mut node = node.clone();
                if let Some(input) = input
                    && !node.set_primary_input(input)
                {
                    return Err(OverlayOpError::NotSingleInput {
                        node: node.name().to_string(),
                        context: "splicing an added node".to_string(),
                        span,
                    });
                }
                Ok(node)
            }
            (None, Some(path)) => {
                let input = input.ok_or(OverlayOpError::Malformed {
                    message: "a `composition` add requires a splice anchor (`after`, `before`, \
                              or `input`)"
                        .to_string(),
                    span,
                })?;
                let alias = self.alias.clone().ok_or(OverlayOpError::Malformed {
                    message: "a `composition` add requires an `alias`".to_string(),
                    span,
                })?;
                // Bind the splice anchor to the composition's sole declared
                // input port. A composition with zero or several input ports
                // can't be fed by a single splice anchor, so `inputs:` stays
                // empty and `bind_schema` reports any unbound required port
                // (E104) against the op's span.
                let input_port = match ports.get(path.as_path()).map(Vec::as_slice) {
                    Some([only]) => Some(only.clone()),
                    _ => None,
                };
                Ok(build_composition_node(
                    alias,
                    path.clone(),
                    self.config.clone(),
                    input,
                    input_port,
                    loc,
                ))
            }
            // Exclusivity was validated by `new_node_name`; treat any other
            // shape defensively rather than panicking.
            _ => Err(OverlayOpError::Malformed {
                message: "specify exactly one of `node` or `composition`".to_string(),
                span,
            }),
        }
    }
}

fn build_composition_node(
    alias: String,
    use_path: PathBuf,
    config: IndexMap<String, serde_json::Value>,
    input: NodeInput,
    input_port: Option<String>,
    loc: Location,
) -> PipelineNode {
    // A composition consumes through its named-port `inputs:` map — the
    // authoritative call-site binding the top-level edge wiring reads to feed
    // it. `header.input` is the YAML-shape obligation the shared `NodeHeader`
    // carries and drives consumer rewiring, but it produces no incoming edge on
    // its own. Binding the sole input port to the splice anchor here is what
    // makes the injected node wire up like a hand-authored call site instead of
    // sitting dead with no producer.
    let mut inputs = IndexMap::new();
    if let Some(port) = input_port {
        inputs.insert(port, node_input_ref(&input));
    }
    PipelineNode::Composition {
        header: NodeHeader {
            name: alias.clone(),
            description: None,
            input: Spanned::new(input, loc, loc),
            notes: None,
        },
        r#use: use_path,
        alias: Some(alias),
        inputs,
        outputs: IndexMap::new(),
        config,
        resources: IndexMap::new(),
        body: crate::plan::composition_body::CompositionBodyId::default(),
    }
}

/// Render a [`NodeInput`] as the plain upstream-reference string a composition
/// `inputs:` map stores: `Single("x")` → `"x"`, `Port { node, port }` →
/// `"node.port"` (the same shape the top-level edge wiring strips a port from).
fn node_input_ref(input: &NodeInput) -> String {
    match input {
        NodeInput::Single(name) => name.clone(),
        NodeInput::Port { node, port } => format!("{node}.{port}"),
    }
}

fn apply_add(
    nodes: &mut Vec<Spanned<PipelineNode>>,
    add: AddOp,
    ports: &CompositionInputPorts,
    loc: Location,
    span: Span,
) -> Result<(), OverlayOpError> {
    let splice = add.resolve_splice(span)?;
    let name = add.new_node_name(span)?;

    if index_of(nodes, &name).is_some() {
        return Err(OverlayOpError::DuplicateAdd { name, span });
    }

    match splice {
        Splice::After(anchor) => {
            let anchor_idx =
                index_of(nodes, &anchor).ok_or_else(|| OverlayOpError::MissingTarget {
                    op: "add",
                    target: anchor.clone(),
                    span,
                })?;
            // Every former consumer of the anchor now reads the spliced node.
            // The new node is not inserted yet, so this cannot touch it.
            for node in nodes.iter_mut() {
                reroute_single_ref(&mut node.value, &anchor, &name);
            }
            let node = add.build_node(Some(NodeInput::Single(anchor)), ports, loc, span)?;
            nodes.insert(anchor_idx + 1, Spanned::new(node, loc, loc));
        }
        Splice::Before(anchor) => {
            let anchor_idx =
                index_of(nodes, &anchor).ok_or_else(|| OverlayOpError::MissingTarget {
                    op: "add",
                    target: anchor.clone(),
                    span,
                })?;
            // The new node takes over the anchor's former upstream input.
            let anchor_input = nodes[anchor_idx]
                .value
                .primary_input()
                .cloned()
                .ok_or_else(|| OverlayOpError::NotSingleInput {
                    node: anchor.clone(),
                    context: "a `before` splice".to_string(),
                    span,
                })?;
            let former_upstream = node_input_ref(&anchor_input);
            nodes[anchor_idx]
                .value
                .set_primary_input(NodeInput::Single(name.clone()));
            // For a composition anchor, `set_primary_input` moves only
            // `header.input`; the real edge lives in its `inputs:` map, so
            // repoint the port bound to the former upstream onto the new node
            // too — otherwise the anchor keeps reading its old upstream and the
            // injected node is bypassed.
            reroute_composition_inputs(&mut nodes[anchor_idx].value, &former_upstream, &name);
            let node = add.build_node(Some(anchor_input), ports, loc, span)?;
            nodes.insert(anchor_idx, Spanned::new(node, loc, loc));
        }
        Splice::ExplicitInput(input) => {
            let node = add.build_node(Some(input), ports, loc, span)?;
            nodes.push(Spanned::new(node, loc, loc));
        }
        Splice::NodeDeclared => {
            let node = add.build_node(None, ports, loc, span)?;
            nodes.push(Spanned::new(node, loc, loc));
        }
    }

    validate_node_inputs(nodes, &name, span)
}

fn apply_remove(
    nodes: &mut Vec<Spanned<PipelineNode>>,
    remove: RemoveOp,
    span: Span,
) -> Result<(), OverlayOpError> {
    let idx = index_of(nodes, &remove.target).ok_or_else(|| OverlayOpError::MissingTarget {
        op: "remove",
        target: remove.target.clone(),
        span,
    })?;

    // Apply the explicit rewiring before removing the target.
    for (key, value) in &remove.rewire {
        let (consumer, field) = parse_rewire_key(key).ok_or_else(|| OverlayOpError::Malformed {
            message: format!("invalid rewire key {key:?}; expected `<node>.input`"),
            span,
        })?;
        if field != "input" {
            return Err(OverlayOpError::Malformed {
                message: format!(
                    "rewire only supports the `input` field; got `{field}` in {key:?}"
                ),
                span,
            });
        }
        let ci = index_of(nodes, consumer).ok_or_else(|| OverlayOpError::UndeclaredReference {
            reference: consumer.to_string(),
            context: format!("rewire key {key:?}"),
            span,
        })?;
        if !nodes[ci].value.set_primary_input(value.clone()) {
            return Err(OverlayOpError::NotSingleInput {
                node: consumer.to_string(),
                context: "a `rewire` entry".to_string(),
                span,
            });
        }
    }

    nodes.remove(idx);

    // Safety: after removal, no remaining node may still reference the target.
    for node in nodes.iter() {
        if node
            .value
            .direct_input_names()
            .iter()
            .any(|r| *r == remove.target)
        {
            return Err(OverlayOpError::DanglingReference {
                removed: remove.target.clone(),
                consumer: node.value.name().to_string(),
                span,
            });
        }
    }

    Ok(())
}

fn apply_replace(
    nodes: &mut [Spanned<PipelineNode>],
    replace: ReplaceOp,
    loc: Location,
    span: Span,
) -> Result<(), OverlayOpError> {
    let idx = index_of(nodes, &replace.target).ok_or_else(|| OverlayOpError::MissingTarget {
        op: "replace",
        target: replace.target.clone(),
        span,
    })?;

    let node_name = replace.node.name().to_string();
    if node_name != replace.target {
        return Err(OverlayOpError::ReplaceNameMismatch {
            target: replace.target,
            node_name,
            span,
        });
    }

    nodes[idx] = Spanned::new(replace.node, loc, loc);

    validate_node_inputs(nodes, &replace.target, span)
}

/// The only currently-settable `set` field path.
const SET_CXL_FIELD: &str = "config.cxl";

fn apply_set(
    nodes: &mut [Spanned<PipelineNode>],
    set: SetOp,
    loc: Location,
    span: Span,
) -> Result<(), OverlayOpError> {
    let idx = index_of(nodes, &set.target).ok_or_else(|| OverlayOpError::MissingTarget {
        op: "set",
        target: set.target.clone(),
        span,
    })?;

    if set.field != SET_CXL_FIELD {
        return Err(OverlayOpError::UnknownField {
            target: set.target,
            field: set.field,
            span,
        });
    }

    // `config.cxl` replaces the stage's CXL logic wholesale. The value must be
    // a CXL source string; the new `CxlSource` carries the op location so a
    // later typecheck error anchors to the override, not the base pipeline.
    let source = set
        .value
        .as_str()
        .ok_or_else(|| OverlayOpError::Malformed {
            message: format!(
                "`set` of `config.cxl` on node {:?} requires a string value",
                set.target
            ),
            span,
        })?;

    if !nodes[idx]
        .value
        .set_primary_cxl(CxlSource::new(source.to_string(), loc.span()))
    {
        // The field path is valid but this node kind has no primary `cxl:`
        // body (e.g. a `Source` or `Route`) — a missing field path, not a
        // silent no-op.
        return Err(OverlayOpError::UnknownField {
            target: set.target,
            field: set.field,
            span,
        });
    }

    Ok(())
}

fn apply_bypass(
    nodes: &mut Vec<Spanned<PipelineNode>>,
    bypass: BypassOp,
    span: Span,
) -> Result<(), OverlayOpError> {
    let idx = index_of(nodes, &bypass.target).ok_or_else(|| OverlayOpError::MissingTarget {
        op: "bypass",
        target: bypass.target.clone(),
        span,
    })?;

    // 1-in: the node must have exactly one upstream input to hand its consumer.
    let upstream =
        nodes[idx]
            .value
            .primary_input()
            .cloned()
            .ok_or_else(|| OverlayOpError::BypassInvalid {
                target: bypass.target.clone(),
                reason: "it has no single upstream input (bypass rewires a 1-in/1-out linear node)"
                    .to_string(),
                span,
            })?;

    // 1-out: exactly one downstream node may consume the target, or the
    // auto-rewire is ambiguous — spell it out with `remove` + `rewire` instead.
    let consumers: Vec<usize> = nodes
        .iter()
        .enumerate()
        .filter(|(i, node)| {
            *i != idx
                && node
                    .value
                    .direct_input_names()
                    .iter()
                    .any(|r| *r == bypass.target)
        })
        .map(|(i, _)| i)
        .collect();
    if consumers.len() != 1 {
        return Err(OverlayOpError::BypassInvalid {
            target: bypass.target.clone(),
            reason: format!(
                "it feeds {} consumers (bypass rewires exactly one; use `remove` with an \
                 explicit `rewire` for a fan-out)",
                consumers.len()
            ),
            span,
        });
    }
    let consumer_idx = consumers[0];

    // Repoint the sole consumer onto the target's former upstream, then drop
    // the target. A consumer without a single primary input (a `Merge` /
    // `Combine` fan-in) cannot be auto-rewired and must use `remove`.
    if !nodes[consumer_idx].value.set_primary_input(upstream) {
        let consumer = nodes[consumer_idx].value.name().to_string();
        return Err(OverlayOpError::BypassInvalid {
            target: bypass.target,
            reason: format!(
                "its consumer {consumer:?} has no single input to rewire; use `remove` with an \
                 explicit `rewire`"
            ),
            span,
        });
    }

    nodes.remove(idx);
    Ok(())
}

fn apply_patch_schema(
    nodes: &mut [Spanned<PipelineNode>],
    patch: PatchSchemaOp,
    span: Span,
) -> Result<(), OverlayOpError> {
    let idx = index_of(nodes, &patch.target).ok_or_else(|| OverlayOpError::MissingTarget {
        op: "patch_schema",
        target: patch.target.clone(),
        span,
    })?;

    match &mut nodes[idx].value {
        PipelineNode::Source { config, .. } => {
            // Reuse the shared column-op engine so the op surface and the
            // channel source-config patch surface resolve columns identically.
            apply_schema_ops(&mut config.schema, &patch.schema, &patch.target).map_err(|e| {
                OverlayOpError::PatchSchema {
                    message: e.to_string(),
                    span,
                }
            })
        }
        other => Err(OverlayOpError::NotSource {
            target: patch.target.clone(),
            actual: other.type_tag(),
            span,
        }),
    }
}

/// Repoint every plain `Single(from)` input reference on `node` to
/// `Single(to)`, across all of the variant's input positions.
///
/// Port-qualified references (`from.port`) are intentionally left untouched: a
/// linear `after` splice re-parents whole-node consumers, but a specific branch
/// port is a deliberate wiring choice the author must re-splice explicitly.
fn reroute_single_ref(node: &mut PipelineNode, from: &str, to: &str) {
    fn repoint(input: &mut NodeInput, from: &str, to: &str) {
        if let NodeInput::Single(name) = input
            && name == from
        {
            *name = to.to_string();
        }
    }

    // A composition consumes through its named-port `inputs:` map, not
    // `header.input` (which produces no incoming edge), so its real upstream
    // edge follows this repoint, not the `header.input` repoint below. Run it
    // first so the borrow ends before the `match` re-borrows `node`.
    reroute_composition_inputs(node, from, to);

    match node {
        PipelineNode::Source { .. } => {}
        PipelineNode::Transform { header, .. }
        | PipelineNode::Aggregate { header, .. }
        | PipelineNode::Route { header, .. }
        | PipelineNode::Output { header, .. }
        | PipelineNode::Reshape { header, .. }
        | PipelineNode::Cull { header, .. }
        | PipelineNode::Composition { header, .. } => repoint(&mut header.input.value, from, to),
        PipelineNode::Merge { header, .. } => {
            for input in &mut header.inputs {
                repoint(&mut input.value, from, to);
            }
        }
        PipelineNode::Combine { header, .. } => {
            for input in header.input.values_mut() {
                repoint(&mut input.value, from, to);
            }
        }
        PipelineNode::Envelope { header, .. } => {
            repoint(&mut header.body.value, from, to);
            if let Some(h) = header.header.as_mut() {
                repoint(&mut h.value, from, to);
            }
            if let Some(t) = header.trailer.as_mut() {
                repoint(&mut t.value, from, to);
            }
        }
    }
}

/// Repoint a composition's named-port `inputs:` map: any port bound to `from`
/// is rebound to `to`. No-op for non-composition nodes and for port-qualified
/// values (`from.port`), matching [`reroute_single_ref`]'s exact-match policy.
///
/// The `inputs:` map is a composition's authoritative call-site binding — the
/// map the top-level edge wiring reads to feed each port — whereas
/// `header.input` produces no edge. Both an `after` splice (via
/// [`reroute_single_ref`]) and a `before` splice must repoint it, or a
/// composition consumer keeps reading the spliced-out anchor directly and the
/// injected node is bypassed.
fn reroute_composition_inputs(node: &mut PipelineNode, from: &str, to: &str) {
    if let PipelineNode::Composition { inputs, .. } = node {
        for upstream in inputs.values_mut() {
            if upstream == from {
                *upstream = to.to_string();
            }
        }
    }
}

/// Every input reference of `node_name` must resolve to a node in the current
/// graph. Runs after an `add` / `replace` so a typo'd splice input is a hard
/// error rather than a deferred, base-anchored compile diagnostic.
fn validate_node_inputs(
    nodes: &[Spanned<PipelineNode>],
    node_name: &str,
    span: Span,
) -> Result<(), OverlayOpError> {
    let declared: HashSet<String> = nodes.iter().map(|n| n.value.name().to_string()).collect();
    if let Some(node) = nodes.iter().find(|n| n.value.name() == node_name) {
        for reference in node.value.direct_input_names() {
            if reference != node_name && !declared.contains(reference) {
                return Err(OverlayOpError::UndeclaredReference {
                    reference: reference.to_string(),
                    context: format!("input of node {node_name:?}"),
                    span,
                });
            }
        }
    }
    Ok(())
}

/// Split a rewire key `<node>.<field>` into its parts. Node names cannot
/// contain a dot, so the first dot is the separator; a key with zero or more
/// than one dot is rejected (returns `None`).
fn parse_rewire_key(key: &str) -> Option<(&str, &str)> {
    let (node, field) = key.split_once('.')?;
    if node.is_empty() || field.is_empty() || field.contains('.') {
        return None;
    }
    Some((node, field))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cxl::typecheck::Type;

    // ---- fixtures ----------------------------------------------------

    /// Parse a `nodes:` document into the base node list the engine mutates.
    fn parse_nodes(yaml: &str) -> Vec<Spanned<PipelineNode>> {
        #[derive(Deserialize)]
        struct Doc {
            nodes: Vec<Spanned<PipelineNode>>,
        }
        let doc: Doc = crate::yaml::from_str(yaml).expect("parse nodes");
        doc.nodes
    }

    /// Parse one `Spanned<OverlayOp>` from a bare op mapping.
    fn parse_op(yaml: &str) -> Spanned<OverlayOp> {
        crate::yaml::from_str::<Spanned<OverlayOp>>(yaml).expect("parse op")
    }

    /// A three-node linear pipeline: `orders -> normalize -> sink`.
    fn linear_base() -> Vec<Spanned<PipelineNode>> {
        parse_nodes(
            r#"
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
  - type: transform
    name: normalize
    input: orders
    config:
      cxl: "emit order_id = order_id"
  - type: transform
    name: sink
    input: normalize
    config:
      cxl: "emit order_id = order_id"
"#,
        )
    }

    /// (name, [direct input names]) fingerprint capturing order + wiring.
    fn fingerprint(nodes: &[Spanned<PipelineNode>]) -> Vec<(String, Vec<String>)> {
        nodes
            .iter()
            .map(|n| {
                (
                    n.value.name().to_string(),
                    n.value
                        .direct_input_names()
                        .iter()
                        .map(|s| s.to_string())
                        .collect(),
                )
            })
            .collect()
    }

    fn names(nodes: &[Spanned<PipelineNode>]) -> Vec<String> {
        nodes.iter().map(|n| n.value.name().to_string()).collect()
    }

    fn one(op: Spanned<OverlayOp>) -> Vec<LayeredOp> {
        vec![LayeredOp::new(OverlayLayer::ChannelPerTarget, op)]
    }

    // ---- add: splice wiring ------------------------------------------

    #[test]
    fn add_after_splices_between_anchor_and_its_consumers() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: add
node:
  type: transform
  name: fraud
  input: normalize
  config:
    cxl: "emit order_id = order_id"
after: normalize
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");

        // fraud is inserted directly after normalize.
        assert_eq!(names(&out), vec!["orders", "normalize", "fraud", "sink"]);
        // fraud reads normalize; sink now reads fraud.
        assert_eq!(
            fingerprint(&out),
            vec![
                ("orders".into(), vec![]),
                ("normalize".into(), vec!["orders".into()]),
                ("fraud".into(), vec!["normalize".into()]),
                ("sink".into(), vec!["fraud".into()]),
            ]
        );
    }

    #[test]
    fn add_before_takes_over_anchor_input() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: add
node:
  type: transform
  name: pre
  input: placeholder
  config:
    cxl: "emit order_id = order_id"
before: normalize
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");

        assert_eq!(names(&out), vec!["orders", "pre", "normalize", "sink"]);
        assert_eq!(
            fingerprint(&out),
            vec![
                ("orders".into(), vec![]),
                // pre takes normalize's former upstream (orders); the op-node's
                // own placeholder input is overridden by the splice.
                ("pre".into(), vec!["orders".into()]),
                ("normalize".into(), vec!["pre".into()]),
                ("sink".into(), vec!["normalize".into()]),
            ]
        );
    }

    #[test]
    fn add_explicit_input_appends_without_rewiring() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: add
node:
  type: transform
  name: branch
  input: placeholder
  config:
    cxl: "emit order_id = order_id"
input: orders
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");

        assert_eq!(names(&out), vec!["orders", "normalize", "sink", "branch"]);
        // Only `branch` is wired to orders; sink still reads normalize.
        assert_eq!(
            fingerprint(&out),
            vec![
                ("orders".into(), vec![]),
                ("normalize".into(), vec!["orders".into()]),
                ("sink".into(), vec!["normalize".into()]),
                ("branch".into(), vec!["orders".into()]),
            ]
        );
    }

    #[test]
    fn add_node_declared_input_stands_when_no_splice() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: add
node:
  type: transform
  name: branch
  input: orders
  config:
    cxl: "emit order_id = order_id"
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        assert_eq!(
            fingerprint(&out).last().unwrap(),
            &("branch".to_string(), vec!["orders".to_string()])
        );
    }

    #[test]
    fn add_composition_reference_builds_composition_node() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: add
composition: ../composition/fraud_check.comp.yaml
alias: fraud_check
after: normalize
config:
  threshold: 0.8
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");

        assert_eq!(
            names(&out),
            vec!["orders", "normalize", "fraud_check", "sink"]
        );
        let injected = &out[2].value;
        assert_eq!(injected.type_tag(), "composition");
        match injected {
            PipelineNode::Composition {
                header,
                r#use,
                alias,
                config,
                ..
            } => {
                assert_eq!(header.name, "fraud_check");
                assert_eq!(header.input.value, NodeInput::Single("normalize".into()));
                assert_eq!(
                    r#use,
                    &PathBuf::from("../composition/fraud_check.comp.yaml")
                );
                assert_eq!(alias.as_deref(), Some("fraud_check"));
                assert_eq!(config.get("threshold").and_then(|v| v.as_f64()), Some(0.8));
            }
            other => panic!("expected composition, got {}", other.type_tag()),
        }
        // sink was rewired onto the injected composition.
        assert_eq!(out[3].value.direct_input_names(), vec!["fraud_check"]);
    }

    /// A base whose downstream consumer of `normalize` is a composition, so the
    /// splice must both feed the injected composition and repoint the consumer's
    /// named-port `inputs:` map — the two halves of the issue #768 fix.
    fn composition_consumer_base() -> Vec<Spanned<PipelineNode>> {
        parse_nodes(
            r#"
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
  - type: transform
    name: normalize
    input: orders
    config:
      cxl: "emit order_id = order_id"
  - type: composition
    name: scorer
    input: normalize
    use: ../composition/score.comp.yaml
    inputs:
      inp: normalize
"#,
        )
    }

    /// The composition node named `name`'s `(header input, inputs map)`.
    fn composition_wiring(
        nodes: &[Spanned<PipelineNode>],
        name: &str,
    ) -> (NodeInput, IndexMap<String, String>) {
        match &nodes.iter().find(|n| n.value.name() == name).unwrap().value {
            PipelineNode::Composition { header, inputs, .. } => {
                (header.input.value.clone(), inputs.clone())
            }
            other => panic!("expected composition {name:?}, got {}", other.type_tag()),
        }
    }

    #[test]
    fn add_composition_binds_input_port_and_reroutes_downstream_composition() {
        let base = composition_consumer_base();
        let op = parse_op(
            r#"
op: add
composition: ../composition/screen.comp.yaml
alias: screen
after: normalize
"#,
        );
        let mut ports = CompositionInputPorts::new();
        ports.insert(
            PathBuf::from("../composition/screen.comp.yaml"),
            vec!["inp".to_string()],
        );

        let out = apply_overlay_ops_with_ports(base, one(op), &ports).expect("apply");

        assert_eq!(names(&out), vec!["orders", "normalize", "screen", "scorer"]);

        // The injected composition's sole input port is bound to the splice
        // anchor, so the top-level edge wiring feeds it (defect 1).
        let (screen_header, screen_inputs) = composition_wiring(&out, "screen");
        assert_eq!(screen_header, NodeInput::Single("normalize".into()));
        assert_eq!(
            screen_inputs.get("inp").map(String::as_str),
            Some("normalize")
        );
        assert_eq!(screen_inputs.len(), 1);

        // The downstream composition consumer's named-port `inputs:` map follows
        // the rewire onto the injected node — not just its header input (defect 2).
        let (scorer_header, scorer_inputs) = composition_wiring(&out, "scorer");
        assert_eq!(scorer_header, NodeInput::Single("screen".into()));
        assert_eq!(scorer_inputs.get("inp").map(String::as_str), Some("screen"));
    }

    #[test]
    fn add_composition_without_resolved_ports_leaves_inputs_empty() {
        // With no port info (the 2-arg entry point), the injected composition
        // keeps an empty `inputs:` map; `bind_schema` then surfaces the real
        // port diagnostic against the op span rather than the engine guessing.
        let base = composition_consumer_base();
        let op = parse_op(
            r#"
op: add
composition: ../composition/screen.comp.yaml
alias: screen
after: normalize
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        let (_, screen_inputs) = composition_wiring(&out, "screen");
        assert!(screen_inputs.is_empty());
    }

    #[test]
    fn add_composition_before_composition_anchor_reroutes_inputs_map() {
        // A `before` splice in front of a composition anchor must repoint the
        // anchor's named-port `inputs:` map (which carries the real edge), not
        // just its `header.input` — otherwise the anchor keeps reading its old
        // upstream directly and the injected composition is bypassed.
        let base = composition_consumer_base();
        let op = parse_op(
            r#"
op: add
composition: ../composition/screen.comp.yaml
alias: screen
before: scorer
"#,
        );
        let mut ports = CompositionInputPorts::new();
        ports.insert(
            PathBuf::from("../composition/screen.comp.yaml"),
            vec!["inp".to_string()],
        );

        let out = apply_overlay_ops_with_ports(base, one(op), &ports).expect("apply");

        assert_eq!(names(&out), vec!["orders", "normalize", "screen", "scorer"]);

        // The injected node takes over the anchor's former upstream (`normalize`).
        let (screen_header, screen_inputs) = composition_wiring(&out, "screen");
        assert_eq!(screen_header, NodeInput::Single("normalize".into()));
        assert_eq!(
            screen_inputs.get("inp").map(String::as_str),
            Some("normalize")
        );

        // The anchor now reads the injected node through both its header input
        // and its `inputs:` map — the map is what actually feeds it.
        let (scorer_header, scorer_inputs) = composition_wiring(&out, "scorer");
        assert_eq!(scorer_header, NodeInput::Single("screen".into()));
        assert_eq!(scorer_inputs.get("inp").map(String::as_str), Some("screen"));
    }

    #[test]
    fn add_inserted_node_carries_op_span() {
        let base = linear_base();
        // The op parses at line 2 of its document (line 1 is blank).
        let op = parse_op(
            r#"
op: add
node:
  type: transform
  name: fraud
  input: normalize
  config:
    cxl: "emit order_id = order_id"
after: normalize
"#,
        );
        let op_line = op.referenced.line();
        assert!(op_line > 0, "op should carry a real location");
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        let injected = out.iter().find(|n| n.value.name() == "fraud").unwrap();
        assert_eq!(
            injected.referenced.line(),
            op_line,
            "injected node span should anchor to the op, not the base"
        );
    }

    // ---- add: collisions ---------------------------------------------

    #[test]
    fn add_duplicate_name_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: add
node:
  type: transform
  name: normalize
  input: orders
  config:
    cxl: "emit order_id = order_id"
after: orders
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("duplicate add");
        assert!(
            matches!(err, OverlayOpError::DuplicateAdd { ref name, .. } if name == "normalize")
        );
    }

    #[test]
    fn add_missing_after_anchor_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: add
node:
  type: transform
  name: fraud
  input: nope
  config:
    cxl: "emit order_id = order_id"
after: nope
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("missing anchor");
        assert!(
            matches!(err, OverlayOpError::MissingTarget { op: "add", ref target, .. } if target == "nope")
        );
    }

    #[test]
    fn add_explicit_input_to_missing_node_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: add
node:
  type: transform
  name: branch
  input: placeholder
  config:
    cxl: "emit order_id = order_id"
input: ghost
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("dangling input");
        assert!(
            matches!(err, OverlayOpError::UndeclaredReference { ref reference, .. } if reference == "ghost")
        );
    }

    // ---- remove ------------------------------------------------------

    #[test]
    fn remove_with_rewire_repoints_consumer() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: remove
target: normalize
rewire:
  sink.input: orders
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        assert_eq!(names(&out), vec!["orders", "sink"]);
        assert_eq!(out[1].value.direct_input_names(), vec!["orders"]);
    }

    #[test]
    fn remove_missing_target_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: remove
target: ghost
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("missing target");
        assert!(
            matches!(err, OverlayOpError::MissingTarget { op: "remove", ref target, .. } if target == "ghost")
        );
    }

    #[test]
    fn remove_without_rewire_leaves_dangling_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: remove
target: normalize
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("dangling");
        assert!(
            matches!(err, OverlayOpError::DanglingReference { ref removed, ref consumer, .. }
                if removed == "normalize" && consumer == "sink")
        );
    }

    #[test]
    fn remove_rewire_to_missing_consumer_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: remove
target: normalize
rewire:
  ghost.input: orders
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("bad rewire consumer");
        assert!(
            matches!(err, OverlayOpError::UndeclaredReference { ref reference, .. } if reference == "ghost")
        );
    }

    #[test]
    fn remove_rewire_unsupported_field_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: remove
target: normalize
rewire:
  sink.config: orders
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("bad rewire field");
        assert!(matches!(err, OverlayOpError::Malformed { .. }));
    }

    // ---- replace -----------------------------------------------------

    #[test]
    fn replace_swaps_node_keeping_edges() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: replace
target: normalize
node:
  type: transform
  name: normalize
  input: orders
  config:
    cxl: "emit order_id = upper(order_id)"
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        // Position + name + downstream edge preserved.
        assert_eq!(names(&out), vec!["orders", "normalize", "sink"]);
        assert_eq!(out[2].value.direct_input_names(), vec!["normalize"]);
        // The definition changed.
        match &out[1].value {
            PipelineNode::Transform { config, .. } => {
                assert_eq!(config.cxl.source, "emit order_id = upper(order_id)");
            }
            other => panic!("expected transform, got {}", other.type_tag()),
        }
    }

    #[test]
    fn replace_name_mismatch_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: replace
target: normalize
node:
  type: transform
  name: renamed
  input: orders
  config:
    cxl: "emit order_id = order_id"
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("name mismatch");
        assert!(
            matches!(err, OverlayOpError::ReplaceNameMismatch { ref target, ref node_name, .. }
            if target == "normalize" && node_name == "renamed")
        );
    }

    #[test]
    fn replace_missing_target_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: replace
target: ghost
node:
  type: transform
  name: ghost
  input: orders
  config:
    cxl: "emit order_id = order_id"
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("missing target");
        assert!(
            matches!(err, OverlayOpError::MissingTarget { op: "replace", ref target, .. } if target == "ghost")
        );
    }

    // ---- total-order determinism -------------------------------------

    #[test]
    fn total_order_is_independent_of_construction_path() {
        // A pipeline-default add and a channel add both splice `after:
        // normalize`. The higher-precedence channel op must apply last
        // regardless of the order the ops are supplied in.
        let default_add = || {
            LayeredOp::new(
                OverlayLayer::PipelineDefault,
                parse_op(
                    r#"
op: add
node:
  type: transform
  name: p_stage
  input: normalize
  config:
    cxl: "emit order_id = order_id"
after: normalize
"#,
                ),
            )
        };
        let channel_add = || {
            LayeredOp::new(
                OverlayLayer::ChannelPerTarget,
                parse_op(
                    r#"
op: add
node:
  type: transform
  name: c_stage
  input: normalize
  config:
    cxl: "emit order_id = order_id"
after: normalize
"#,
                ),
            )
        };

        let out_a = apply_overlay_ops(linear_base(), vec![default_add(), channel_add()])
            .expect("apply order a");
        let out_b = apply_overlay_ops(linear_base(), vec![channel_add(), default_add()])
            .expect("apply order b");

        // Identical regardless of input order.
        assert_eq!(fingerprint(&out_a), fingerprint(&out_b));
        // And the channel op (higher precedence) applied last: it sits
        // directly after normalize, ahead of the pipeline-default stage.
        assert_eq!(
            names(&out_a),
            vec!["orders", "normalize", "c_stage", "p_stage", "sink"]
        );
    }

    #[test]
    fn within_layer_declaration_order_is_preserved() {
        // Two adds in the same layer keep their declaration order.
        let first = LayeredOp::new(
            OverlayLayer::ChannelPerTarget,
            parse_op(
                r#"
op: add
node:
  type: transform
  name: first
  input: normalize
  config:
    cxl: "emit order_id = order_id"
after: normalize
"#,
            ),
        );
        let second = LayeredOp::new(
            OverlayLayer::ChannelPerTarget,
            parse_op(
                r#"
op: add
node:
  type: transform
  name: second
  input: normalize
  config:
    cxl: "emit order_id = order_id"
after: normalize
"#,
            ),
        );
        let out = apply_overlay_ops(linear_base(), vec![first, second]).expect("apply");
        assert_eq!(
            names(&out),
            vec!["orders", "normalize", "second", "first", "sink"]
        );
    }

    #[test]
    fn group_priority_orders_within_group_layer() {
        // Higher-priority group applies later.
        let low = LayeredOp::new(
            OverlayLayer::Group { priority: 10 },
            parse_op(
                r#"
op: add
node:
  type: transform
  name: low
  input: normalize
  config:
    cxl: "emit order_id = order_id"
after: normalize
"#,
            ),
        );
        let high = LayeredOp::new(
            OverlayLayer::Group { priority: 20 },
            parse_op(
                r#"
op: add
node:
  type: transform
  name: high
  input: normalize
  config:
    cxl: "emit order_id = order_id"
after: normalize
"#,
            ),
        );
        let out = apply_overlay_ops(linear_base(), vec![high, low]).expect("apply");
        assert_eq!(
            names(&out),
            vec!["orders", "normalize", "high", "low", "sink"]
        );
    }

    // ---- deserialization ---------------------------------------------

    #[test]
    fn deserialize_add_node_shape() {
        let op = parse_op(
            r#"
op: add
node:
  type: transform
  name: stamp
  input: product_lookup
  config:
    cxl: "emit a = b"
after: product_lookup
"#,
        );
        match op.value {
            OverlayOp::Add(add) => {
                assert!(add.node.is_some());
                assert!(add.composition.is_none());
                assert_eq!(add.after.as_deref(), Some("product_lookup"));
            }
            _ => panic!("expected add"),
        }
    }

    #[test]
    fn deserialize_remove_shape() {
        let op = parse_op(
            r#"
op: remove
target: legacy_audit
rewire:
  route_priority.input: product_lookup
"#,
        );
        match op.value {
            OverlayOp::Remove(remove) => {
                assert_eq!(remove.target, "legacy_audit");
                assert_eq!(
                    remove.rewire.get("route_priority.input"),
                    Some(&NodeInput::Single("product_lookup".into()))
                );
            }
            _ => panic!("expected remove"),
        }
    }

    #[test]
    fn deserialize_replace_shape() {
        let op = parse_op(
            r#"
op: replace
target: route_priority
node:
  type: transform
  name: route_priority
  input: product_lookup
  config:
    cxl: "emit x = y"
"#,
        );
        match op.value {
            OverlayOp::Replace(replace) => {
                assert_eq!(replace.target, "route_priority");
                assert_eq!(replace.node.name(), "route_priority");
            }
            _ => panic!("expected replace"),
        }
    }

    #[test]
    fn deserialize_rejects_unknown_field() {
        let err = crate::yaml::from_str::<Spanned<OverlayOp>>(
            r#"
op: remove
target: x
bogus: 1
"#,
        )
        .expect_err("unknown field");
        assert!(
            err.to_string().contains("bogus"),
            "error should name the unknown field: {err}"
        );
    }

    #[test]
    fn deserialize_rejects_cross_variant_field() {
        let err = crate::yaml::from_str::<Spanned<OverlayOp>>(
            r#"
op: remove
target: x
after: y
"#,
        )
        .expect_err("cross-variant field");
        assert!(
            err.to_string().contains("after"),
            "error should name the misplaced field: {err}"
        );
    }

    #[test]
    fn deserialize_rejects_unmodeled_op_kind() {
        // An `op:` tag outside the modeled vocabulary surfaces as a clear
        // unknown-variant error.
        let err = crate::yaml::from_str::<Spanned<OverlayOp>>(
            r#"
op: frobnicate
target: x
"#,
        )
        .expect_err("unmodeled op");
        assert!(
            err.to_string().contains("frobnicate") || err.to_string().contains("variant"),
            "error should flag the unknown op kind: {err}"
        );
    }

    #[test]
    fn deserialize_add_requires_node_or_composition_at_apply() {
        // A bare `add` parses (shape is valid) but fails on application.
        let op = parse_op("op: add\nafter: normalize\n");
        let err = apply_overlay_ops(linear_base(), one(op)).expect_err("empty add");
        assert!(matches!(err, OverlayOpError::Malformed { .. }));
    }

    // ---- set: config.cxl ---------------------------------------------

    /// The CXL body source of a named transform in the current graph.
    fn transform_cxl(nodes: &[Spanned<PipelineNode>], name: &str) -> String {
        match &nodes.iter().find(|n| n.value.name() == name).unwrap().value {
            PipelineNode::Transform { config, .. } => config.cxl.source.clone(),
            other => panic!("expected transform, got {}", other.type_tag()),
        }
    }

    /// A three-node linear pipeline whose middle stage is an aggregate:
    /// `orders -> totals -> sink`.
    fn aggregate_base() -> Vec<Spanned<PipelineNode>> {
        parse_nodes(
            r#"
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: totals
    input: orders
    config:
      group_by: [order_id]
      cxl: "emit order_id = order_id"
  - type: transform
    name: sink
    input: totals
    config:
      cxl: "emit order_id = order_id"
"#,
        )
    }

    #[test]
    fn set_config_cxl_replaces_transform_logic() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: set
target: normalize
field: config.cxl
value: "emit order_id = upper(order_id)"
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        // Node identity, position, and wiring are untouched...
        assert_eq!(names(&out), vec!["orders", "normalize", "sink"]);
        assert_eq!(out[2].value.direct_input_names(), vec!["normalize"]);
        // ...only the CXL logic changed.
        assert_eq!(
            transform_cxl(&out, "normalize"),
            "emit order_id = upper(order_id)"
        );
    }

    #[test]
    fn set_config_cxl_replaces_aggregate_logic() {
        // `config.cxl` is a uniform field op: it addresses the primary CXL body
        // of any variant that has one, not only a transform.
        let base = aggregate_base();
        let op = parse_op(
            r#"
op: set
target: totals
field: config.cxl
value: "emit order_id = order_id, total = sum(amount)"
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        match &out
            .iter()
            .find(|n| n.value.name() == "totals")
            .unwrap()
            .value
        {
            PipelineNode::Aggregate { config, .. } => {
                assert_eq!(
                    config.cxl.source,
                    "emit order_id = order_id, total = sum(amount)"
                );
            }
            other => panic!("expected aggregate, got {}", other.type_tag()),
        }
    }

    #[test]
    fn set_new_cxl_carries_op_span() {
        // The replaced `CxlSource` takes the op's location, so a later CXL
        // typecheck error anchors to the override rather than the base pipeline.
        let base = linear_base();
        let op = parse_op(
            r#"
op: set
target: normalize
field: config.cxl
value: "emit order_id = order_id"
"#,
        );
        let op_span = op.referenced.span();
        assert_ne!(
            op_span,
            crate::yaml::Span::UNKNOWN,
            "op should carry a span"
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        match &out
            .iter()
            .find(|n| n.value.name() == "normalize")
            .unwrap()
            .value
        {
            PipelineNode::Transform { config, .. } => {
                assert_eq!(
                    config.cxl.span, op_span,
                    "replaced CXL span should anchor to the op"
                );
            }
            other => panic!("expected transform, got {}", other.type_tag()),
        }
    }

    #[test]
    fn set_missing_target_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: set
target: ghost
field: config.cxl
value: "emit x = y"
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("missing target");
        assert!(
            matches!(err, OverlayOpError::MissingTarget { op: "set", ref target, .. } if target == "ghost")
        );
    }

    #[test]
    fn set_unknown_field_path_errors() {
        // A field path other than the settable ones is a hard error, never a
        // silent no-op.
        let base = linear_base();
        let op = parse_op(
            r#"
op: set
target: normalize
field: config.batch_size
value: 512
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("unknown field");
        assert!(
            matches!(err, OverlayOpError::UnknownField { ref field, .. } if field == "config.batch_size")
        );
    }

    #[test]
    fn set_config_cxl_on_node_without_cxl_body_errors() {
        // `config.cxl` on a `Source` (no primary CXL body) is a missing field
        // path, not a silent no-op.
        let base = linear_base();
        let op = parse_op(
            r#"
op: set
target: orders
field: config.cxl
value: "emit x = y"
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("no cxl body");
        assert!(
            matches!(err, OverlayOpError::UnknownField { ref target, ref field, .. }
                if target == "orders" && field == "config.cxl")
        );
    }

    #[test]
    fn set_config_cxl_non_string_value_errors() {
        let base = linear_base();
        let op = parse_op(
            r#"
op: set
target: normalize
field: config.cxl
value: 42
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("non-string value");
        assert!(matches!(err, OverlayOpError::Malformed { .. }));
    }

    // ---- bypass ------------------------------------------------------

    #[test]
    fn bypass_rewires_linear_node() {
        // orders -> normalize -> sink; bypass normalize => orders -> sink.
        let base = linear_base();
        let op = parse_op("op: bypass\ntarget: normalize\n");
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        assert_eq!(names(&out), vec!["orders", "sink"]);
        assert_eq!(out[1].value.direct_input_names(), vec!["orders"]);
    }

    #[test]
    fn bypass_matches_equivalent_remove_rewire() {
        // bypass is exactly sugar for a remove + single-edge rewire.
        let bypassed = apply_overlay_ops(
            linear_base(),
            one(parse_op("op: bypass\ntarget: normalize\n")),
        )
        .expect("bypass");
        let removed = apply_overlay_ops(
            linear_base(),
            one(parse_op(
                "op: remove\ntarget: normalize\nrewire:\n  sink.input: orders\n",
            )),
        )
        .expect("remove");
        assert_eq!(fingerprint(&bypassed), fingerprint(&removed));
    }

    #[test]
    fn bypass_missing_target_errors() {
        let err = apply_overlay_ops(linear_base(), one(parse_op("op: bypass\ntarget: ghost\n")))
            .expect_err("missing target");
        assert!(
            matches!(err, OverlayOpError::MissingTarget { op: "bypass", ref target, .. } if target == "ghost")
        );
    }

    #[test]
    fn bypass_source_without_input_errors() {
        // A source has no upstream input to hand its consumer.
        let err = apply_overlay_ops(linear_base(), one(parse_op("op: bypass\ntarget: orders\n")))
            .expect_err("no input");
        assert!(
            matches!(err, OverlayOpError::BypassInvalid { ref target, .. } if target == "orders")
        );
    }

    #[test]
    fn bypass_terminal_node_without_consumer_errors() {
        // `sink` is fed by `normalize` but has no consumer to rewire.
        let err = apply_overlay_ops(linear_base(), one(parse_op("op: bypass\ntarget: sink\n")))
            .expect_err("no consumer");
        assert!(
            matches!(err, OverlayOpError::BypassInvalid { ref target, ref reason, .. }
                if target == "sink" && reason.contains("0 consumers"))
        );
    }

    #[test]
    fn bypass_fan_out_node_errors() {
        // `orders` feeds two consumers; auto-rewiring is ambiguous.
        let base = parse_nodes(
            r#"
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
  - type: transform
    name: mid
    input: orders
    config:
      cxl: "emit order_id = order_id"
  - type: transform
    name: a
    input: mid
    config:
      cxl: "emit order_id = order_id"
  - type: transform
    name: b
    input: mid
    config:
      cxl: "emit order_id = order_id"
"#,
        );
        let err = apply_overlay_ops(base, one(parse_op("op: bypass\ntarget: mid\n")))
            .expect_err("fan-out");
        assert!(
            matches!(err, OverlayOpError::BypassInvalid { ref target, ref reason, .. }
                if target == "mid" && reason.contains("2 consumers"))
        );
    }

    // ---- patch_schema ------------------------------------------------

    /// A source `orders` with three columns feeding a sink.
    fn schema_base() -> Vec<Spanned<PipelineNode>> {
        parse_nodes(
            r#"
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: amount, type: int }
        - { name: cust_id, type: string }
        - { name: order_notes, type: string }
  - type: output
    name: sink
    input: orders
    config:
      name: sink
      type: csv
      path: out.csv
"#,
        )
    }

    /// (name, type) pairs of a named source node's declared columns.
    fn source_columns(nodes: &[Spanned<PipelineNode>], name: &str) -> Vec<(String, Type)> {
        match &nodes.iter().find(|n| n.value.name() == name).unwrap().value {
            PipelineNode::Source { config, .. } => config
                .schema
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.ty.clone()))
                .collect(),
            other => panic!("expected source, got {}", other.type_tag()),
        }
    }

    #[test]
    fn patch_schema_add_drop_retype_reflected() {
        let base = schema_base();
        let op = parse_op(
            r#"
op: patch_schema
target: orders
schema:
  amount: { retype: float }
  order_notes: remove
  region: { add: { type: string } }
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        assert_eq!(
            source_columns(&out, "orders"),
            vec![
                ("amount".to_string(), Type::Float),
                ("cust_id".to_string(), Type::String),
                ("region".to_string(), Type::String),
            ]
        );
    }

    #[test]
    fn patch_schema_rename_sets_physical_alias() {
        let base = schema_base();
        let op = parse_op(
            r#"
op: patch_schema
target: orders
schema:
  cust_id: { rename: customer_id }
"#,
        );
        let out = apply_overlay_ops(base, one(op)).expect("apply");
        match &out
            .iter()
            .find(|n| n.value.name() == "orders")
            .unwrap()
            .value
        {
            PipelineNode::Source { config, .. } => {
                let col = config
                    .schema
                    .columns
                    .iter()
                    .find(|c| c.name == "customer_id")
                    .expect("renamed column");
                // Rename is a physical->logical alias: the reader still binds the
                // original source field.
                assert_eq!(col.source_name.as_deref(), Some("cust_id"));
            }
            other => panic!("expected source, got {}", other.type_tag()),
        }
    }

    #[test]
    fn patch_schema_unknown_column_errors() {
        // The reused column-op engine reports its E231 diagnostic.
        let base = schema_base();
        let op = parse_op(
            r#"
op: patch_schema
target: orders
schema:
  nope: { retype: float }
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("unknown column");
        assert!(
            matches!(err, OverlayOpError::PatchSchema { ref message, .. } if message.contains("E231")),
            "{err}"
        );
    }

    #[test]
    fn patch_schema_missing_target_errors() {
        let base = schema_base();
        let op = parse_op(
            r#"
op: patch_schema
target: ghost
schema:
  amount: remove
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("missing target");
        assert!(
            matches!(err, OverlayOpError::MissingTarget { op: "patch_schema", ref target, .. } if target == "ghost")
        );
    }

    #[test]
    fn patch_schema_non_source_target_errors() {
        // `sink` is an output node, not a source.
        let base = schema_base();
        let op = parse_op(
            r#"
op: patch_schema
target: sink
schema:
  amount: remove
"#,
        );
        let err = apply_overlay_ops(base, one(op)).expect_err("not a source");
        assert!(
            matches!(err, OverlayOpError::NotSource { ref target, actual, .. }
                if target == "sink" && actual == "output")
        );
    }

    // ---- deserialization: new ops ------------------------------------

    #[test]
    fn deserialize_set_shape() {
        let op = parse_op(
            r#"
op: set
target: route_priority
field: config.cxl
value: "emit _route = fulfilled"
"#,
        );
        match op.value {
            OverlayOp::Set(set) => {
                assert_eq!(set.target, "route_priority");
                assert_eq!(set.field, "config.cxl");
                assert_eq!(set.value.as_str(), Some("emit _route = fulfilled"));
            }
            _ => panic!("expected set"),
        }
    }

    #[test]
    fn deserialize_bypass_shape() {
        let op = parse_op("op: bypass\ntarget: legacy_audit\n");
        match op.value {
            OverlayOp::Bypass(bypass) => assert_eq!(bypass.target, "legacy_audit"),
            _ => panic!("expected bypass"),
        }
    }

    #[test]
    fn deserialize_patch_schema_shape() {
        let op = parse_op(
            r#"
op: patch_schema
target: orders
schema:
  tax_exempt: { add: { type: bool } }
  order_notes: remove
"#,
        );
        match op.value {
            OverlayOp::PatchSchema(patch) => {
                assert_eq!(patch.target, "orders");
                assert_eq!(patch.schema.len(), 2);
                assert!(matches!(
                    patch.schema.get("order_notes"),
                    Some(SchemaColumnOp::Remove)
                ));
            }
            _ => panic!("expected patch_schema"),
        }
    }

    #[test]
    fn set_requires_field_and_value() {
        let err = crate::yaml::from_str::<Spanned<OverlayOp>>("op: set\ntarget: x\n")
            .expect_err("missing field/value");
        assert!(
            err.to_string().contains("field") || err.to_string().contains("value"),
            "{err}"
        );
    }

    #[test]
    fn patch_schema_requires_schema() {
        let err = crate::yaml::from_str::<Spanned<OverlayOp>>("op: patch_schema\ntarget: x\n")
            .expect_err("missing schema");
        assert!(err.to_string().contains("schema"), "{err}");
    }

    #[test]
    fn deserialize_rejects_cross_variant_field_on_set() {
        // `node` is not a valid field on a `set` op.
        let err = crate::yaml::from_str::<Spanned<OverlayOp>>(
            r#"
op: set
target: x
field: config.cxl
value: "emit a = b"
node:
  type: transform
  name: x
  input: y
  config:
    cxl: "emit a = b"
"#,
        )
        .expect_err("cross-variant field");
        assert!(err.to_string().contains("node"), "{err}");
    }
}
