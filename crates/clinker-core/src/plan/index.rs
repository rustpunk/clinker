//! Phase E: Index planning.
//!
//! Extracts `analytic_window` configs from pipeline nodes, combines with
//! `AnalysisReport` field sets, deduplicates into `Vec<IndexSpec>`.

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use clinker_record::Schema;
use petgraph::graph::NodeIndex;
use serde::Deserialize;

use crate::config::SortField;

/// Where a window's secondary index is rooted in the execution plan.
///
/// A windowed transform's arena+index pair is materialized from the rows
/// of some upstream operator. Today's source-rooted geometry is one
/// degenerate case; post-aggregate / post-combine / post-transform
/// windows are rooted at the upstream operator's emit buffer instead.
///
/// The `Arc<Schema>` carried by the `Node` and `ParentNode` variants is
/// the upstream operator's `output_schema` at lowering time. It is
/// **not** part of equality / hashing — only `upstream: NodeIndex` is.
/// Two `IndexSpec`s with the same `upstream` always carry the same
/// `Arc<Schema>` instance from that upstream node, so dedup keyed on
/// `upstream` alone is correct.
pub enum PlanIndexRoot {
    /// Source-rooted: arena built from the named source's record stream
    /// at Phase-0 setup.
    Source(String),
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
            Self::Source(s) => Self::Source(s.clone()),
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
            Self::Source(name) => f.debug_tuple("Source").field(name).finish(),
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
            (Self::Source(a), Self::Source(b)) => a == b,
            (Self::Node { upstream: a, .. }, Self::Node { upstream: b, .. }) => a == b,
            (Self::ParentNode { upstream: a, .. }, Self::ParentNode { upstream: b, .. }) => a == b,
            _ => false,
        }
    }
}

impl Eq for PlanIndexRoot {}

impl Hash for PlanIndexRoot {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Discriminant + payload: Source by string, Node/ParentNode by
        // upstream NodeIndex. The Arc<Schema> is a side-channel — every
        // IndexSpec sharing an `upstream` carries the same Arc instance
        // (it's the upstream operator's output_schema), so excluding it
        // from the hash key keeps dedup correct.
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Source(name) => name.hash(state),
            Self::Node { upstream, .. } | Self::ParentNode { upstream, .. } => upstream.hash(state),
        }
    }
}

/// Typed representation of the `analytic_window` YAML block on a transform.
#[derive(Debug, Clone, Deserialize)]
pub struct LocalWindowConfig {
    /// Reference source name. If None or same as primary input, it's a same-source window.
    pub source: Option<String>,
    /// Fields to group the partition by.
    pub group_by: Vec<String>,
    /// Fields to sort the partition by (within each group).
    #[serde(default)]
    pub sort_by: Vec<SortField>,
    /// Expression to evaluate against the primary record for cross-source lookup.
    pub on: Option<String>,
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

/// Parse a transform's `analytic_window` JSON value into a typed
/// [`LocalWindowConfig`]. `None` raw value yields `Ok(None)`. Used by
/// `PipelineConfig::compile_with_diagnostics` while building per-node
/// window configs in declaration order.
pub(crate) fn parse_analytic_window_value(
    raw: &Option<serde_json::Value>,
    transform_name: &str,
) -> Result<Option<LocalWindowConfig>, PlanIndexError> {
    match raw {
        None => Ok(None),
        Some(value) => {
            let config: LocalWindowConfig = serde_json::from_value(value.clone()).map_err(|e| {
                PlanIndexError::InvalidLocalWindow {
                    transform: transform_name.to_string(),
                    message: e.to_string(),
                }
            })?;
            Ok(Some(config))
        }
    }
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
    /// Mirror of `LocalWindowConfig.requires_buffer_recompute`. Set during
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

/// Raw index request before deduplication. One per (transform, local_window) pair.
#[derive(Debug, Clone)]
pub struct RawIndexRequest {
    pub root: PlanIndexRoot,
    pub group_by: Vec<String>,
    pub sort_by: Vec<SortField>,
    pub arena_fields: Vec<String>,
    pub already_sorted: bool,
    /// Index of the transform that requested this index.
    pub transform_index: usize,
    /// Mirror of `LocalWindowConfig.requires_buffer_recompute` for this
    /// transform. Carried through dedup so the merged `IndexSpec` lifts
    /// the flag when any contributing transform needs buffer recompute.
    pub requires_buffer_recompute: bool,
}

/// Collect the union of arena_fields across all IndexSpecs rooted at a
/// given source. Used by source-arena materialization paths
/// (`pipeline/ingestion.rs`, `executor/mod.rs`'s Phase-0 build).
pub fn collect_arena_fields_for_source(indices: &[IndexSpec], source: &str) -> Vec<String> {
    let mut fields = HashSet::new();
    for spec in indices {
        if let PlanIndexRoot::Source(name) = &spec.root
            && name == source
        {
            for f in &spec.arena_fields {
                fields.insert(f.clone());
            }
        }
    }
    let mut result: Vec<String> = fields.into_iter().collect();
    result.sort(); // deterministic order
    result
}

/// Collect the union of arena_fields across all IndexSpecs rooted at a
/// given upstream node. Used by node-rooted arena materialization at
/// the upstream operator's dispatch-arm exit.
pub fn collect_arena_fields_for_node(indices: &[IndexSpec], upstream: NodeIndex) -> Vec<String> {
    let mut fields = HashSet::new();
    for spec in indices {
        let matches = match &spec.root {
            PlanIndexRoot::Node { upstream: u, .. } => *u == upstream,
            PlanIndexRoot::ParentNode { upstream: u, .. } => *u == upstream,
            PlanIndexRoot::Source(_) => false,
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

/// Errors from index planning.
#[derive(Debug)]
pub enum PlanIndexError {
    InvalidLocalWindow { transform: String, message: String },
}

impl std::fmt::Display for PlanIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanIndexError::InvalidLocalWindow { transform, message } => {
                write!(
                    f,
                    "invalid local_window on transform '{}': {}",
                    transform, message
                )
            }
        }
    }
}

impl std::error::Error for PlanIndexError {}
