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

/// Confidence level for a destructive provenance variant.
///
/// `Proven` is recorded when destruction is detected by a structural rule
/// (e.g., the transform's static write-set intersects the sort-key field
/// names). `Inferred` is reserved for any future case where destruction is
/// detected by a non-structural heuristic (e.g., an opaque UDF whose effect
/// on a sort-key field cannot be statically proven).
///
/// The `render_unordered_streaming_error` walker uses this field to choose
/// caret style (`^^^` for `Proven`, `~~~` for `Inferred`) and to insert a
/// "may have been" hedge in the rendered note text.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Confidence {
    Proven,
    Inferred,
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
        confidence: Confidence,
    },
    /// Destroyed by a `distinct` statement inside a transform.
    DestroyedByDistinct {
        at_node: String,
        confidence: Confidence,
    },
    /// Destroyed by hash aggregation (output order is not guaranteed).
    DestroyedByHashAggregate {
        at_node: String,
        confidence: Confidence,
    },
    /// Merge reconciliation: parent orderings disagreed.
    DestroyedByMergeMismatch {
        at_node: String,
        parent_orderings: Vec<Option<Vec<SortField>>>,
        confidence: Confidence,
    },
    /// Source has no declared sort order.
    NoOrdering,
    /// Ordering asserted by a streaming aggregate's group-by prefix at
    /// `at_node`, enabled by an upstream `Ordering` provenance chain
    /// (`enabled_by`). Phase 16 Task 16.4.0 introduces the variant; the
    /// property-pass walker that produces it lands in Task 16.4.9a.
    IntroducedByStreamingAggregate {
        at_node: String,
        enabled_by: Box<OrderingProvenance>,
    },
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

/// Format a rustc-style multi-line diagnostic explaining why an explicit
/// `strategy: streaming` aggregation cannot be honored: walk the parent's
/// `OrderingProvenance` chain hop-by-hop and emit one `note:` per
/// destruction site, plus a primary `help:` selected on the terminal hop.
///
/// The walker is also reused (Task 16.4.9) to populate
/// `PlanNode::Aggregation::fallback_reason` when `Auto` resolves to `Hash`
/// because eligibility was `HashFallback`, so Kiln canvas hover tooltips
/// share a single source of truth with compile errors.
///
/// Pure formatting — no I/O. Confidence-aware caret style is a
/// Clinker-original UX (no prior-art precedent; see drill-pass-10 Q4).
pub fn render_unordered_streaming_error(
    parent_props: &NodeProperties,
    group_by: &[String],
    agg_name: &str,
) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "error[CXL0419]: aggregate '{agg_name}' declared 'strategy: streaming' \
         but its input is not sorted on group key {group_by:?}\n"
    ));

    // Walk the chain: each hop is one note line. Emit hops in order from
    // immediate parent back to the terminal cause.
    let mut cur = &parent_props.ordering.provenance;
    let mut hops = 0usize;
    loop {
        match cur {
            OrderingProvenance::NoOrdering => {
                out.push_str("  note: input has no declared sort_order\n");
                break;
            }
            OrderingProvenance::DeclaredOnInput { input_name } => {
                out.push_str(&format!(
                    "  note: ordering declared on input `{input_name}`\n"
                ));
                break;
            }
            OrderingProvenance::Preserved { from_node } => {
                out.push_str(&format!("  note: preserved through `{from_node}`\n"));
                // `Preserved` does not carry a back-link to the upstream
                // provenance value (only a node name), so the chain
                // terminates here. Future work may thread the upstream
                // chain through `Preserved` to enable deeper walks.
                break;
            }
            OrderingProvenance::IntroducedByStreamingAggregate {
                at_node,
                enabled_by,
            } => {
                out.push_str(&format!(
                    "  note: ordering introduced by streaming aggregate `{at_node}`\n"
                ));
                cur = enabled_by;
            }
            OrderingProvenance::DestroyedByTransformWriteSet {
                at_node,
                sort_fields_lost,
                confidence,
                ..
            } => {
                let (caret, hedge) = caret_and_hedge(*confidence);
                out.push_str(&format!(
                    "  note: ordering {hedge}destroyed by transform `{at_node}` \
                     writing {sort_fields_lost:?}\n        {caret}\n"
                ));
                break;
            }
            OrderingProvenance::DestroyedByDistinct {
                at_node,
                confidence,
            } => {
                let (caret, hedge) = caret_and_hedge(*confidence);
                out.push_str(&format!(
                    "  note: ordering {hedge}destroyed by `distinct` in `{at_node}`\n        {caret}\n"
                ));
                break;
            }
            OrderingProvenance::DestroyedByHashAggregate {
                at_node,
                confidence,
            } => {
                let (caret, hedge) = caret_and_hedge(*confidence);
                out.push_str(&format!(
                    "  note: ordering {hedge}destroyed by hash aggregate `{at_node}`\n        {caret}\n"
                ));
                break;
            }
            OrderingProvenance::DestroyedByMergeMismatch {
                at_node,
                confidence,
                ..
            } => {
                let (caret, hedge) = caret_and_hedge(*confidence);
                out.push_str(&format!(
                    "  note: ordering {hedge}destroyed by merge mismatch at `{at_node}`\n        {caret}\n"
                ));
                break;
            }
        }
        hops += 1;
        if hops > 32 {
            // Defensive: bound the walk to avoid runaway recursion through
            // a malformed chain. 32 hops is far beyond any realistic plan.
            out.push_str("  note: (provenance chain truncated)\n");
            break;
        }
    }

    // Variant-specific primary `help:` selected on the terminal hop.
    let terminal = walk_to_terminal(&parent_props.ordering.provenance);
    match terminal {
        OrderingProvenance::NoOrdering => {
            out.push_str(&format!(
                "  help: declare `sort_order: {group_by:?}` on the upstream input\n"
            ));
        }
        OrderingProvenance::DeclaredOnInput { input_name } => {
            out.push_str(&format!(
                "  help: extend `sort_order` on input `{input_name}` to cover {group_by:?}\n"
            ));
        }
        OrderingProvenance::DestroyedByTransformWriteSet {
            at_node,
            sort_fields_lost,
            ..
        } => {
            out.push_str(&format!(
                "  help: stop writing field(s) {sort_fields_lost:?} in transform `{at_node}` \
                 or move the sort downstream of `{at_node}`\n"
            ));
        }
        OrderingProvenance::DestroyedByDistinct { at_node, .. } => {
            out.push_str(&format!(
                "  help: remove the `distinct` in transform `{at_node}` \
                 or place the aggregate upstream of it\n"
            ));
        }
        OrderingProvenance::DestroyedByHashAggregate { at_node, .. } => {
            out.push_str(&format!(
                "  help: hash aggregate `{at_node}` produces unordered output; \
                 add a sort step between `{at_node}` and `{agg_name}`\n"
            ));
        }
        OrderingProvenance::DestroyedByMergeMismatch { at_node, .. } => {
            out.push_str(&format!(
                "  help: parent orderings disagree at merge `{at_node}`; \
                 align them upstream or sort below the merge\n"
            ));
        }
        OrderingProvenance::Preserved { .. }
        | OrderingProvenance::IntroducedByStreamingAggregate { .. } => {
            // These are non-terminal in well-formed chains; nothing
            // variant-specific to suggest beyond the fallback help below.
        }
    }

    // Always-emitted fallback help lines.
    out.push_str(&format!(
        "  help: insert a sort step upstream of `{agg_name}`\n"
    ));
    out.push_str("  help: or relax to `strategy: auto` to allow hash fallback\n");
    out
}

fn caret_and_hedge(c: Confidence) -> (&'static str, &'static str) {
    match c {
        Confidence::Proven => ("^^^", ""),
        Confidence::Inferred => ("~~~", "may have been "),
    }
}

fn walk_to_terminal(p: &OrderingProvenance) -> &OrderingProvenance {
    let mut cur = p;
    let mut hops = 0usize;
    while let OrderingProvenance::IntroducedByStreamingAggregate { enabled_by, .. } = cur {
        cur = enabled_by;
        hops += 1;
        if hops > 32 {
            break;
        }
    }
    cur
}

#[cfg(test)]
mod render_tests {
    use super::*;

    fn props(p: OrderingProvenance) -> NodeProperties {
        NodeProperties {
            ordering: Ordering {
                sort_order: None,
                provenance: p,
            },
            partitioning: Partitioning {
                kind: PartitioningKind::Single,
                provenance: PartitioningProvenance::SingleStream,
            },
        }
    }

    #[test]
    fn test_render_no_sort_order_terminal_source_suggests_sort_order_declaration() {
        let s = render_unordered_streaming_error(
            &props(OrderingProvenance::NoOrdering),
            &["dept".to_string()],
            "agg",
        );
        assert!(s.contains("CXL0419"));
        assert!(s.contains("no declared sort_order"));
        assert!(s.contains("declare `sort_order"));
        assert!(s.contains("insert a sort step"));
        assert!(s.contains("relax to `strategy: auto`"));
    }

    #[test]
    fn test_render_destroyed_by_transform_write_set_suggests_field_or_move() {
        let s = render_unordered_streaming_error(
            &props(OrderingProvenance::DestroyedByTransformWriteSet {
                at_node: "t1".to_string(),
                fields_written: vec!["dept".to_string()],
                sort_fields_lost: vec!["dept".to_string()],
                confidence: Confidence::Proven,
            }),
            &["dept".to_string()],
            "agg",
        );
        assert!(s.contains("destroyed by transform `t1`"));
        assert!(s.contains("^^^"));
        assert!(s.contains("stop writing field(s)"));
        assert!(s.contains("move the sort downstream"));
    }

    #[test]
    fn test_render_destroyed_by_distinct_branch() {
        let s = render_unordered_streaming_error(
            &props(OrderingProvenance::DestroyedByDistinct {
                at_node: "td".to_string(),
                confidence: Confidence::Proven,
            }),
            &["k".to_string()],
            "agg",
        );
        assert!(s.contains("destroyed by `distinct` in `td`"));
        assert!(s.contains("remove the `distinct`"));
    }

    #[test]
    fn test_render_destroyed_by_hash_aggregate_branch() {
        let s = render_unordered_streaming_error(
            &props(OrderingProvenance::DestroyedByHashAggregate {
                at_node: "ha".to_string(),
                confidence: Confidence::Proven,
            }),
            &["k".to_string()],
            "agg",
        );
        assert!(s.contains("destroyed by hash aggregate `ha`"));
        assert!(s.contains("add a sort step between `ha` and `agg`"));
    }

    #[test]
    fn test_render_multi_hop_chain_emits_one_note_per_hop() {
        // streaming-agg → destroyed-by-write-set
        let chain = OrderingProvenance::IntroducedByStreamingAggregate {
            at_node: "sa".to_string(),
            enabled_by: Box::new(OrderingProvenance::DestroyedByTransformWriteSet {
                at_node: "t1".to_string(),
                fields_written: vec!["k".to_string()],
                sort_fields_lost: vec!["k".to_string()],
                confidence: Confidence::Proven,
            }),
        };
        let s = render_unordered_streaming_error(&props(chain), &["k".to_string()], "agg");
        assert!(s.contains("introduced by streaming aggregate `sa`"));
        assert!(s.contains("destroyed by transform `t1`"));
        // Two notes minimum
        assert!(s.matches("note:").count() >= 2);
    }

    #[test]
    fn test_render_inferred_confidence_uses_tilde_caret_and_hedge_text() {
        let s = render_unordered_streaming_error(
            &props(OrderingProvenance::DestroyedByDistinct {
                at_node: "td".to_string(),
                confidence: Confidence::Inferred,
            }),
            &["k".to_string()],
            "agg",
        );
        assert!(s.contains("~~~"));
        assert!(s.contains("may have been"));
    }

    #[test]
    fn test_render_always_includes_two_fallback_help_lines() {
        let s = render_unordered_streaming_error(
            &props(OrderingProvenance::DestroyedByDistinct {
                at_node: "td".to_string(),
                confidence: Confidence::Proven,
            }),
            &["k".to_string()],
            "agg",
        );
        assert!(s.contains("insert a sort step upstream of `agg`"));
        assert!(s.contains("relax to `strategy: auto`"));
    }
}
