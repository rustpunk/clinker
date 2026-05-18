//! Window-index planning.
//!
//! Carries the typed [`AnalyticWindowSpec`] off pipeline nodes through
//! the deduplication step that produces `Vec<IndexSpec>`, combined with
//! `AnalysisReport` field sets harvested from each transform's CXL.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use clinker_record::Schema;
use petgraph::graph::NodeIndex;
use serde::{Deserialize, Serialize};

use crate::config::SortField;

/// Where a window's secondary index is rooted in the execution plan.
///
/// A windowed transform's arena+index pair is materialized from the
/// rows of some upstream operator. Every window — including one whose
/// immediate ancestor is a Source — anchors at that operator's
/// `NodeIndex`; the arena builds at the upstream operator's
/// dispatch-arm exit through `finalize_node_rooted_windows`.
///
/// The `Arc<Schema>` carried by both variants is the upstream
/// operator's `output_schema` at lowering time. It is **not** part of
/// equality / hashing — only `upstream: NodeIndex` is. Two
/// `IndexSpec`s with the same `upstream` always carry the same
/// `Arc<Schema>` instance from that upstream node, so dedup keyed on
/// `upstream` alone is correct.
pub enum PlanIndexRoot {
    /// Node-rooted in the current DAG: arena built at the upstream
    /// operator's dispatch-arm exit from `node_buffers[upstream]`.
    Node {
        upstream: NodeIndex,
        anchor_schema: Arc<Schema>,
    },
    /// Body-rooted at a parent-DAG node: a composition body's window
    /// references rows produced by the body's `input:` port, which in
    /// turn resolves to a parent-DAG operator. Lookup walks the body's
    /// runtime overlay first, then falls back to the parent's runtime.
    ParentNode {
        upstream: NodeIndex,
        anchor_schema: Arc<Schema>,
    },
}

impl Clone for PlanIndexRoot {
    fn clone(&self) -> Self {
        match self {
            Self::Node {
                upstream,
                anchor_schema,
            } => Self::Node {
                upstream: *upstream,
                anchor_schema: Arc::clone(anchor_schema),
            },
            Self::ParentNode {
                upstream,
                anchor_schema,
            } => Self::ParentNode {
                upstream: *upstream,
                anchor_schema: Arc::clone(anchor_schema),
            },
        }
    }
}

impl std::fmt::Debug for PlanIndexRoot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Node { upstream, .. } => f
                .debug_struct("Node")
                .field("upstream", upstream)
                .finish_non_exhaustive(),
            Self::ParentNode { upstream, .. } => f
                .debug_struct("ParentNode")
                .field("upstream", upstream)
                .finish_non_exhaustive(),
        }
    }
}

impl PartialEq for PlanIndexRoot {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Node { upstream: a, .. }, Self::Node { upstream: b, .. }) => a == b,
            (Self::ParentNode { upstream: a, .. }, Self::ParentNode { upstream: b, .. }) => a == b,
            _ => false,
        }
    }
}

impl Eq for PlanIndexRoot {}

impl Hash for PlanIndexRoot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Discriminant + payload: Node / ParentNode by upstream
        // NodeIndex. The Arc<Schema> is a side-channel — every
        // IndexSpec sharing an `upstream` carries the same Arc
        // instance (it's the upstream operator's output_schema), so
        // excluding it from the hash key keeps dedup correct.
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Node { upstream, .. } | Self::ParentNode { upstream, .. } => upstream.hash(state),
        }
    }
}

/// Typed representation of the `analytic_window` YAML block on a transform.
///
/// Deserialized directly by serde-saphyr off the YAML config; the same
/// struct is then carried forward through plan lowering. The
/// `requires_buffer_recompute` field is the lone exception — it is a
/// derived plan-time property, not a YAML-authored field; see its doc
/// comment for why `#[serde(default)]` is the correct shape there.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyticWindowSpec {
    /// Reference source name. If None or same as primary input, it's a same-source window.
    pub source: Option<String>,
    /// Fields to group the partition by.
    pub group_by: Vec<String>,
    /// Fields to sort the partition by (within each group).
    #[serde(default)]
    pub sort_by: Vec<SortField>,
    /// Expression to evaluate against the primary record for cross-source lookup.
    pub on: Option<String>,
    /// Optional row-frame attached to the window. `None` means "entire
    /// partition" and matches the historical default semantics. Range /
    /// Groups modes land in a later sprint; only `FrameMode::Rows` is
    /// accepted today.
    #[serde(default)]
    pub frame: Option<WindowFrame>,
    /// Window sits downstream of a relaxed-CK aggregate whose dropped CK
    /// fields overlap this window's `partition_by`. Set to `true` by the
    /// plan-time derivation walk in
    /// `derive_window_buffer_recompute_flags`; the executor's window arm
    /// switches from streaming-emit to per-partition buffered output so
    /// the orchestrator's commit phase can recompute affected partitions
    /// over `partition − retracted_rows`. `#[serde(default)]` is required
    /// because YAML never sets this — it is a derived plan-time property.
    #[serde(default)]
    pub requires_buffer_recompute: bool,
}

/// Row-frame attached to an [`AnalyticWindowSpec`]. Mirrors SQL window
/// frames (`ROWS BETWEEN <start> AND <end>`). `Range` / `Groups` modes
/// land in a later sprint.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct WindowFrame {
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: FrameBound,
}

/// Frame mode. Only `Rows` (positional offsets) is supported today.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FrameMode {
    Rows,
}

/// One bound of a [`WindowFrame`]. YAML form is externally tagged for
/// the offset-carrying variants and a bare string for the parameterless
/// ones. `Preceding`/`Following` carry a non-negative row offset.
///
/// ```yaml
/// start: unbounded_preceding
/// end:   current_row
/// # or
/// start: { preceding: 3 }
/// end:   { following: 0 }
/// ```
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum FrameBound {
    UnboundedPreceding,
    Preceding(u32),
    CurrentRow,
    Following(u32),
    UnboundedFollowing,
}

/// Specification for one secondary index to build during Phase 1.
#[derive(Debug, Clone)]
pub struct IndexSpec {
    /// Where the arena+index for this window is rooted. See [`PlanIndexRoot`].
    pub root: PlanIndexRoot,
    /// Fields to group partition keys by.
    pub group_by: Vec<String>,
    /// Fields to sort within each partition.
    pub sort_by: Vec<SortField>,
    /// All fields the Arena must store for this index: union of group_by + sort_by + window-referenced.
    pub arena_fields: Vec<String>,
    /// True if the source config declares a sort_order matching this index's sort_by.
    pub already_sorted: bool,
    /// Mirror of `AnalyticWindowSpec.requires_buffer_recompute`. Set during
    /// `deduplicate_indices` from the input `RawIndexRequest`s; the
    /// executor's window arm consults this to decide between streaming
    /// emit and buffered emit. When two transforms share a deduplicated
    /// IndexSpec and any one of them needs buffer-recompute, the merged
    /// spec inherits the flag.
    pub requires_buffer_recompute: bool,
}

/// Deduplicate index specs: two transforms with the same (root, group_by, sort_by)
/// share one IndexSpec. Arena fields are unioned.
pub fn deduplicate_indices(raw_specs: Vec<RawIndexRequest>) -> Vec<IndexSpec> {
    let mut deduped: Vec<IndexSpec> = Vec::new();

    for req in raw_specs {
        let existing = deduped.iter_mut().find(|spec| {
            spec.root == req.root
                && spec.group_by == req.group_by
                && sort_fields_equal(&spec.sort_by, &req.sort_by)
        });

        match existing {
            Some(spec) => {
                // Merge arena_fields
                for field in &req.arena_fields {
                    if !spec.arena_fields.contains(field) {
                        spec.arena_fields.push(field.clone());
                    }
                }
                // Lift the buffer-recompute flag if any contributing
                // transform requires it: a shared index that one
                // transform reads in buffered mode and another reads in
                // streaming mode collapses to buffered mode (the buffered
                // path's per-record output is a superset of the streaming
                // path's).
                spec.requires_buffer_recompute |= req.requires_buffer_recompute;
            }
            None => {
                deduped.push(IndexSpec {
                    root: req.root,
                    group_by: req.group_by,
                    sort_by: req.sort_by,
                    arena_fields: req.arena_fields,
                    already_sorted: req.already_sorted,
                    requires_buffer_recompute: req.requires_buffer_recompute,
                });
            }
        }
    }

    deduped
}

/// Raw index request before deduplication. One per (transform, analytic_window) pair.
#[derive(Debug, Clone)]
pub struct RawIndexRequest {
    pub root: PlanIndexRoot,
    pub group_by: Vec<String>,
    pub sort_by: Vec<SortField>,
    pub arena_fields: Vec<String>,
    pub already_sorted: bool,
    /// Index of the transform that requested this index.
    pub transform_index: usize,
    /// Mirror of `AnalyticWindowSpec.requires_buffer_recompute` for this
    /// transform. Carried through dedup so the merged `IndexSpec` lifts
    /// the flag when any contributing transform needs buffer recompute.
    pub requires_buffer_recompute: bool,
}

/// Collect the union of arena_fields across all IndexSpecs rooted at a
/// given upstream node. Used by node-rooted arena materialization at
/// the upstream operator's dispatch-arm exit — including Source
/// dispatch arms, whose emit-time finalize anchors windows that
/// previously root at `PlanIndexRoot::Source`.
pub fn collect_arena_fields_for_node(indices: &[IndexSpec], upstream: NodeIndex) -> Vec<String> {
    let mut fields = HashSet::new();
    for spec in indices {
        let matches = match &spec.root {
            PlanIndexRoot::Node { upstream: u, .. } => *u == upstream,
            PlanIndexRoot::ParentNode { upstream: u, .. } => *u == upstream,
        };
        if matches {
            for f in &spec.arena_fields {
                fields.insert(f.clone());
            }
        }
    }
    let mut result: Vec<String> = fields.into_iter().collect();
    result.sort();
    result
}

/// Find the index in `indices` that matches the given (root, group_by, sort_by).
pub fn find_index_for(
    indices: &[IndexSpec],
    root: &PlanIndexRoot,
    group_by: &[String],
    sort_by: &[SortField],
) -> Option<usize> {
    indices.iter().position(|spec| {
        &spec.root == root
            && spec.group_by == *group_by
            && sort_fields_equal(&spec.sort_by, sort_by)
    })
}

fn sort_fields_equal(a: &[SortField], b: &[SortField]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter()
        .zip(b.iter())
        .all(|(x, y)| x.field == y.field && x.order == y.order)
}
