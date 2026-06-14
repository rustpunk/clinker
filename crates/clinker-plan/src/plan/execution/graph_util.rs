//! Plan-graph lookups over a compiled [`ExecutionPlanDag`] shared by the
//! planner's strategy-selection passes and the runtime dispatch.

use super::{ExecutionPlanDag, PlanNode, matches_upstream_name};
use crate::error::PipelineError;
use petgraph::Direction;

/// Compute the init-phase ancestor closure for a compiled plan.
///
/// Walks every `PlanNode::Transform` whose payload phase is
/// [`crate::config::Phase::Init`] and unions their transitive
/// upstream ancestors via reverse-BFS along the graph's incoming
/// edges. The init-phase transforms themselves are included.
///
/// Returns an empty set when no init-phase transforms exist — the
/// caller then falls through to a single-pass topo walk.
///
/// E164 validation guarantees init-phase Transforms are terminal
/// (no runtime descendants), so the closure forms a well-bounded
/// init sub-DAG that Pass 1 can run to completion before Pass 2
/// starts.
pub fn compute_init_phase_node_set(
    plan: &ExecutionPlanDag,
) -> std::collections::HashSet<petgraph::graph::NodeIndex> {
    use crate::config::Phase as ConfPhase;
    let mut set: std::collections::HashSet<petgraph::graph::NodeIndex> =
        std::collections::HashSet::new();
    let mut stack: Vec<petgraph::graph::NodeIndex> = plan
        .graph
        .node_indices()
        .filter(|&idx| match &plan.graph[idx] {
            PlanNode::Transform {
                resolved: Some(p), ..
            } => p.phase == ConfPhase::Init,
            _ => false,
        })
        .collect();
    while let Some(idx) = stack.pop() {
        if !set.insert(idx) {
            continue;
        }
        for parent in plan
            .graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
        {
            stack.push(parent);
        }
    }
    set
}

/// Plan-invariant predecessor lookup for nodes that require exactly one
/// upstream input (currently `PlanNode::Aggregation`). Returns
/// `PipelineError::Internal` on misshapen plans — never panics. Mirrors
/// DataFusion's `internal_err!` macro.
pub fn single_predecessor(
    plan: &ExecutionPlanDag,
    node_idx: petgraph::graph::NodeIndex,
    op: &'static str,
    node_name: &str,
) -> Result<petgraph::graph::NodeIndex, PipelineError> {
    let preds: Vec<_> = plan
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    match preds.as_slice() {
        [p] => Ok(*p),
        _ => Err(PipelineError::Internal {
            op,
            node: node_name.to_string(),
            detail: format!("expected exactly 1 predecessor, got {}", preds.len()),
        }),
    }
}

/// Resolve every top-level `PlanNode::Envelope`'s wired `header:` port to its
/// predecessor `NodeIndex`, stamping `header_upstream` in place.
///
/// An Envelope with a wired header has TWO incoming neighbors (body + header),
/// so the executor cannot use [`single_predecessor`] to find the body — it must
/// know which predecessor carries the header. This post-pass matches the
/// lowered `header_input` producer name against the node's incoming neighbors,
/// mirroring the Combine driver resolution. The match strips the
/// [`CORRELATION_SORT_PREFIX`] so a header stream spliced behind an injected
/// correlation Sort still resolves to that Sort's index.
///
/// Runs after the DAG is fully enriched (all edges built, correlation Sorts
/// spliced). Envelope nodes with no wired header (`header_input` empty) are
/// skipped — `header_upstream` stays `None` and the executor frames with each
/// body grain's ambient envelope. A non-empty `header_input` that matches no
/// incoming neighbor also leaves `header_upstream` `None`; the executor then
/// fails fast with an `Internal` error, because a header named at lowering must
/// have produced an edge (the undeclared-producer case was already rejected at
/// E004 validation).
pub fn resolve_envelope_header_upstreams(plan: &mut ExecutionPlanDag) {
    resolve_envelope_header_upstreams_in_graph(&mut plan.graph);
}

/// Per-graph core of [`resolve_envelope_header_upstreams`].
///
/// Split out so the same resolution runs over the top-level
/// [`ExecutionPlanDag`] graph AND over each composition body's mini-DAG —
/// body graphs hold their own `PlanNode::Envelope` nodes the top-level pass
/// cannot reach, exactly like the Combine strategy-selection body sweep.
pub(crate) fn resolve_envelope_header_upstreams_in_graph(
    graph: &mut petgraph::graph::DiGraph<PlanNode, super::PlanEdge>,
) {
    let envelope_indices: Vec<petgraph::graph::NodeIndex> = graph
        .node_indices()
        .filter(|&idx| {
            matches!(
                &graph[idx],
                PlanNode::Envelope { header_input, .. } if !header_input.is_empty()
            )
        })
        .collect();

    for idx in envelope_indices {
        let PlanNode::Envelope { header_input, .. } = &graph[idx] else {
            continue;
        };
        // The lowered `header_input` retains any `.port` suffix, but DAG edges
        // connect to the producer node, so match against the bare producer name.
        let target = header_input
            .split('.')
            .next()
            .unwrap_or(header_input)
            .to_string();

        let resolved = graph
            .neighbors_directed(idx, Direction::Incoming)
            .find(|&p| matches_upstream_name(graph[p].name(), &target));

        if let PlanNode::Envelope {
            header_upstream, ..
        } = &mut graph[idx]
        {
            *header_upstream = resolved;
        }
    }
}
