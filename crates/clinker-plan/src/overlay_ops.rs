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
//! This module implements the three **structural** ops: [`AddOp`], [`RemoveOp`],
//! and [`ReplaceOp`]. The value-level ops (`set`, `bypass`, `patch_schema`) are
//! a deliberate extension seam — see [`OverlayOp`] and the `op`-tag dispatch in
//! `RawOverlayOp::into_op` for where new variants slot in.

use std::collections::HashSet;
use std::path::PathBuf;

use indexmap::IndexMap;
use serde::{Deserialize, Deserializer};
use serde_saphyr::Location;

use clinker_core_types::span::Span;

use crate::config::PipelineNode;
use crate::config::node_header::{NodeHeader, NodeInput};
use crate::yaml::Spanned;

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
///
/// Extension seam: the `set` / `bypass` / `patch_schema` value-ops are not yet
/// modeled. They slot in as new variants here plus new arms in
/// `RawOverlayOp::into_op` and [`apply_overlay_ops`].
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

/// The `op:` discriminant. Only the structural ops are modeled; `op: set`
/// (etc.) surfaces as a clear "unknown variant" error until the value-ops land.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum OverlayOpKind {
    Add,
    Remove,
    Replace,
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
        } = self;

        match op {
            OverlayOpKind::Add => {
                reject(target.is_some(), "add", "target")?;
                reject(!rewire.is_empty(), "add", "rewire")?;
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
                let target = target.ok_or("`replace` op requires a `target`")?;
                let node = node.ok_or("`replace` op requires a `node`")?;
                Ok(OverlayOp::Replace(ReplaceOp { target, node }))
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
            | OverlayOpError::NotSingleInput { span, .. } => *span,
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
        }
    }
}

impl std::error::Error for OverlayOpError {}

// ---------------------------------------------------------------------
// Engine
// ---------------------------------------------------------------------

/// Apply a stream of layered overlay ops to a base node list, returning the
/// transformed list.
///
/// Ops are applied in `(layer precedence, declaration order)` order via a
/// stable sort on [`OverlayLayer`]; the result is therefore independent of the
/// order the caller concatenated the ops in. Every op must resolve against a
/// live node — a missing splice anchor, remove/replace target, or dangling
/// reference is a hard [`OverlayOpError`], never a silent no-op.
pub fn apply_overlay_ops(
    mut nodes: Vec<Spanned<PipelineNode>>,
    mut ops: Vec<LayeredOp>,
) -> Result<Vec<Spanned<PipelineNode>>, OverlayOpError> {
    // Stable sort by layer: cross-layer order follows fixed semantic
    // precedence, while ops sharing a layer keep the caller's declaration
    // order. This is the "(layer, declaration order)" total order.
    ops.sort_by(|a, b| a.layer.cmp(&b.layer));

    for layered in ops {
        let loc = layered.op.referenced;
        let span = span_from_loc(loc);
        match layered.op.value {
            OverlayOp::Add(add) => apply_add(&mut nodes, add, loc, span)?,
            OverlayOp::Remove(remove) => apply_remove(&mut nodes, remove, span)?,
            OverlayOp::Replace(replace) => apply_replace(&mut nodes, replace, loc, span)?,
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
                Ok(build_composition_node(
                    alias,
                    path.clone(),
                    self.config.clone(),
                    input,
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
    loc: Location,
) -> PipelineNode {
    PipelineNode::Composition {
        header: NodeHeader {
            name: alias.clone(),
            description: None,
            input: Spanned::new(input, loc, loc),
            notes: None,
        },
        r#use: use_path,
        alias: Some(alias),
        inputs: IndexMap::new(),
        outputs: IndexMap::new(),
        config,
        resources: IndexMap::new(),
        body: crate::plan::composition_body::CompositionBodyId::default(),
    }
}

fn apply_add(
    nodes: &mut Vec<Spanned<PipelineNode>>,
    add: AddOp,
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
            let node = add.build_node(Some(NodeInput::Single(anchor)), loc, span)?;
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
            nodes[anchor_idx]
                .value
                .set_primary_input(NodeInput::Single(name.clone()));
            let node = add.build_node(Some(anchor_input), loc, span)?;
            nodes.insert(anchor_idx, Spanned::new(node, loc, loc));
        }
        Splice::ExplicitInput(input) => {
            let node = add.build_node(Some(input), loc, span)?;
            nodes.push(Spanned::new(node, loc, loc));
        }
        Splice::NodeDeclared => {
            let node = add.build_node(None, loc, span)?;
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
        // `set` / `bypass` / `patch_schema` are the extension seam — until they
        // land, they surface as a clear unknown-variant error.
        let err = crate::yaml::from_str::<Spanned<OverlayOp>>(
            r#"
op: set
target: x
"#,
        )
        .expect_err("unmodeled op");
        assert!(
            err.to_string().contains("set") || err.to_string().contains("variant"),
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
}
