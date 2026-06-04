//! Plan-graph lookups over a compiled [`ExecutionPlanDag`] shared by the
//! planner's strategy-selection passes and the runtime dispatch.

use super::{ExecutionPlanDag, PlanNode};
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
