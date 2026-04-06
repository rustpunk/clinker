//! Physical properties of a plan node's output stream.
//!
//! `NodeProperties` is a side-table computed once after transform compilation
//! and stored on [`ExecutionPlanDag`](crate::plan::execution::ExecutionPlanDag).
//! It is consumed by:
//!
//! - streaming aggregation qualification (Phase 16)
//! - Phase 14 correlated DLQ sort-order lookup
//! - `--explain` text and JSON rendering
//! - Kiln canvas edge overlays
//!
//! The pass runs in topo order so each node's properties are derived from
//! its already-populated parents. Provenance is recorded on every node so
//! destructive operations can be explained to the user.

use crate::config::SortField;
use serde::{Deserialize, Serialize};

/// Physical properties of a node's output stream.
///
/// Computed once after transform compilation, stored on
/// [`ExecutionPlanDag`](crate::plan::execution::ExecutionPlanDag), consumed
/// by streaming-agg selection, Phase 14 correlated DLQ, `--explain`
/// rendering, and Kiln canvas edge overlays.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeProperties {
    pub ordering: Ordering,
    pub partitioning: Partitioning,
}

/// Effective output ordering of a node, together with a provenance chain
/// explaining how the ordering was derived or destroyed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ordering {
    /// Effective sort order at this node's output, if any.
    pub sort_order: Option<Vec<SortField>>,
    /// Provenance chain — how this ordering was derived or destroyed.
    pub provenance: OrderingProvenance,
}

/// Provenance explaining how a node's `Ordering` was derived.
///
/// Every variant identifies the node responsible so `--explain` and Kiln
/// canvas can render an intelligible chain back to the input declaration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OrderingProvenance {
    /// Declared on `InputConfig.sort_order` — the root of any propagation chain.
    DeclaredOnInput { input_name: String },
    /// Inherited unchanged from parent.
    Preserved { from_node: String },
    /// Destroyed by a transform writing to sort-key fields.
    DestroyedByTransformWriteSet {
        at_node: String,
        fields_written: Vec<String>,
        sort_fields_lost: Vec<String>,
    },
    /// Destroyed by a `distinct` statement inside a transform.
    DestroyedByDistinct { at_node: String },
    /// Destroyed by hash aggregation (output order is not guaranteed).
    DestroyedByHashAggregate { at_node: String },
    /// Merge reconciliation: parent orderings disagreed.
    DestroyedByMergeMismatch {
        at_node: String,
        parent_orderings: Vec<Option<Vec<SortField>>>,
    },
    /// Source has no declared sort order.
    NoOrdering,
}

/// Partitioning of a node's output stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Partitioning {
    pub kind: PartitioningKind,
    pub provenance: PartitioningProvenance,
}

/// Partitioning shape of a node's output.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitioningKind {
    /// Single logical stream. Every node in Clinker today is `Single` —
    /// Phase 6's in-place `par_iter_mut` preserves this because chunks are
    /// mutated at their original positions; workers share one `Vec`.
    Single,
    /// Records hash-partitioned across `num_partitions` partitions on `keys`.
    /// Each partition contains a disjoint subset of the key space; within a
    /// partition, order may hold independently. Reserved for future phases.
    HashPartitioned {
        keys: Vec<String>,
        num_partitions: usize,
    },
    /// Records distributed round-robin or by arrival-time across partitions.
    /// No key guarantee; records for the same key may appear in different
    /// partitions. Reserved for future phases (parallel multi-file ingest).
    RoundRobin { num_partitions: usize },
}

/// Provenance explaining how a node's `Partitioning` was derived.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PartitioningProvenance {
    /// Default for Source nodes and any stage that preserves a single stream.
    SingleStream,
    /// Inherited unchanged from parent.
    Preserved { from_node: String },
    /// Introduced by an explicit partitioning stage.
    IntroducedBy { at_node: String, reason: String },
}

impl NodeProperties {
    /// Construct a `NodeProperties` with no ordering and a single-stream
    /// partitioning. Convenience for tests and fallback cases.
    pub fn unordered_single() -> Self {
        Self {
            ordering: Ordering {
                sort_order: None,
                provenance: OrderingProvenance::NoOrdering,
            },
            partitioning: Partitioning {
                kind: PartitioningKind::Single,
                provenance: PartitioningProvenance::SingleStream,
            },
        }
    }
}
