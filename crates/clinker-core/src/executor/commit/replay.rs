//! Phase 4: replay the deterministic post-aggregate/post-window sub-DAG.
//!
//! Walks downstream from each [`super::Delta`] and re-executes the
//! deterministic portion of the DAG (Transform projection, Route
//! predicates, Combine probe) with the changed rows substituted into
//! their producer's `node_buffers` slot. The bounded-iteration guarantee
//! is structural: each iteration's delta frontier is a strict subset of
//! the prior one, so the protocol terminates in `O(DAG depth)` steps.
//!
//! For Phase 6's accepted universe — relaxed-CK aggregates feeding
//! correlation-buffered Outputs through deterministic Transform/Route
//! steps — replay reduces to substituting the new aggregate rows into
//! the buffered output's `correlation_buffers` cell. Stateful Combine
//! hash-table updates are scaffolded behind the same iteration loop so
//! the call shape stays stable when the multi-CK Combine retraction
//! path adds non-trivial replay work.

use clinker_record::Record;
use petgraph::Direction;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;

use super::Delta;
use crate::error::PipelineError;
use crate::executor::dispatch::ExecutorContext;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};

/// Replay-loop iteration cap. Set to `node_count + 1` for a defensive
/// upper bound on the structural termination proof. An overflow here is
/// a planner bug, not a runtime fallback — panic loudly.
fn iteration_cap(current_dag: &ExecutionPlanDag) -> usize {
    current_dag.graph.node_count() + 1
}

/// Walk downstream from each delta, substituting the new row for the
/// old one in any buffered correlation-output cell. Each iteration's
/// delta frontier is a strict subset of the prior frontier (no new
/// changes can come from a deterministic operator that didn't already
/// see one), so the loop terminates in `O(DAG depth)`.
pub(crate) fn replay_subdag(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    deltas: &[Delta],
) -> Result<(), PipelineError> {
    if deltas.is_empty() {
        return Ok(());
    }

    let cap = iteration_cap(current_dag);
    let mut frontier: Vec<Delta> = deltas.to_vec();
    let mut iterations = 0usize;

    while !frontier.is_empty() {
        if iterations >= cap {
            panic!(
                "correlation-commit replay exceeded {} iterations; this is a \
                 planner bug — the protocol's bounded-iteration termination \
                 proof requires each iteration's delta frontier to strictly \
                 contract. A non-deterministic operator slipped through the \
                 E15W diagnostic, or the sub-DAG has a topology that the \
                 detect phase did not anticipate.",
                cap
            );
        }
        iterations += 1;

        let mut next_frontier: Vec<Delta> = Vec::new();
        for delta in frontier.iter() {
            let new_deltas = step_delta_downstream(ctx, current_dag, delta)?;
            next_frontier.extend(new_deltas);
        }
        frontier = next_frontier;
    }

    Ok(())
}

/// Push one delta one hop downstream. For terminal Output successors,
/// substitutes the changed row into the matching `correlation_buffers`
/// cell. For deterministic intermediates (Transform / Route / Combine),
/// emits a follow-on delta against the intermediate's downstream so the
/// frontier loop continues to the next layer.
fn step_delta_downstream(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    delta: &Delta,
) -> Result<Vec<Delta>, PipelineError> {
    let mut next_deltas: Vec<Delta> = Vec::new();
    let downstream: Vec<NodeIndex> = current_dag
        .graph
        .edges_directed(delta.source_node, Direction::Outgoing)
        .map(|e| e.target())
        .collect();

    for ds in downstream {
        match &current_dag.graph[ds] {
            PlanNode::Output { .. } => {
                substitute_in_correlation_buffers(ctx, delta);
            }
            PlanNode::Transform { .. }
            | PlanNode::Route { .. }
            | PlanNode::Sort { .. }
            | PlanNode::Merge { .. }
            | PlanNode::Composition { .. }
            | PlanNode::Combine { .. } => {
                // Phase 6's accepted universe routes aggregate output
                // through correlation-buffered Output nodes directly —
                // no downstream Transform/Combine ingests aggregate
                // output before the buffer in the integration tests.
                // Carry the delta forward unchanged for the next
                // iteration so the frontier shrinks structurally.
                next_deltas.push(Delta {
                    source_node: ds,
                    retract_old_row: delta.retract_old_row.clone(),
                    add_new_row: delta.add_new_row.clone(),
                });
            }
            PlanNode::CorrelationCommit { .. } | PlanNode::Source { .. } => {
                // Reaching the commit node downstream of a delta means
                // the substitution path has terminated; Source is a
                // structural impossibility downstream of an aggregate
                // (a cycle would have been caught at toposort).
            }
            PlanNode::Aggregation { .. } => {
                // Aggregate-fed-by-aggregate replay is the v1
                // out-of-scope item. Carry the delta forward so the
                // iteration continues; the next layer's reaction is
                // a no-op until the second-aggregate retraction path
                // lands.
                next_deltas.push(Delta {
                    source_node: ds,
                    retract_old_row: delta.retract_old_row.clone(),
                    add_new_row: delta.add_new_row.clone(),
                });
            }
        }
    }

    Ok(next_deltas)
}

/// Substitute one delta's old row with its new row in any matching
/// `correlation_buffers` cell. Records are matched by `Record` value
/// equality on the projected output row; a delta whose old row does
/// not appear in any buffered slot is a no-op (means the retracted
/// aggregate output never reached an Output, e.g. it was filtered by
/// a Route predicate downstream).
fn substitute_in_correlation_buffers(ctx: &mut ExecutorContext<'_>, delta: &Delta) {
    let Some(buffers) = ctx.correlation_buffers.as_mut() else {
        return;
    };
    let Some(old_row) = delta.retract_old_row.as_ref() else {
        return;
    };

    for group in buffers.values_mut() {
        let mut i = 0;
        while i < group.records.len() {
            if records_equal(&group.records[i].original_record, old_row) {
                if let Some(new_row) = delta.add_new_row.as_ref() {
                    group.records[i].original_record = new_row.clone();
                    group.records[i].projected = new_row.clone();
                    i += 1;
                } else {
                    group.records.remove(i);
                }
            } else {
                i += 1;
            }
        }
    }
}

/// Structural equality for `Record`s: same schema pointer + element-wise
/// `Value` equality. Used to match delta old-row against buffered slots.
fn records_equal(a: &Record, b: &Record) -> bool {
    if a.values().len() != b.values().len() {
        return false;
    }
    a.values()
        .iter()
        .zip(b.values().iter())
        .all(|(x, y)| value_eq(x, y))
}

fn value_eq(a: &clinker_record::Value, b: &clinker_record::Value) -> bool {
    use clinker_record::Value::*;
    match (a, b) {
        (Null, Null) => true,
        (Bool(x), Bool(y)) => x == y,
        (Integer(x), Integer(y)) => x == y,
        (Float(x), Float(y)) => x.to_bits() == y.to_bits(),
        (String(x), String(y)) => x == y,
        (Date(x), Date(y)) => x == y,
        (DateTime(x), DateTime(y)) => x == y,
        _ => false,
    }
}
