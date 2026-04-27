//! Phase 1: detect retract scope.
//!
//! Walks `ctx.correlation_buffers` for groups carrying at least one
//! `error_rows` entry (the trigger set), then expands each to a set of
//! affected `(aggregate_node, group_key)` pairs by intersecting the
//! trigger group's CK fields with each downstream Aggregate's
//! `group_by`. The window-affected list mirrors the same shape but is
//! always empty under the Phase-6-accepted universe.

use std::collections::BTreeSet;

use clinker_record::GroupByKey;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::ExecutorContext;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};

/// Output of the detect phase. Consumed by `recompute_agg`,
/// `recompute_window`, and `flush`.
#[derive(Debug, Default)]
pub(crate) struct RetractScope {
    /// Affected aggregate node + group keys that need rerun. Each
    /// entry's `Vec<u32>` carries the runtime-DLQ-triggered input row
    /// IDs to retract from that aggregate's per-group state.
    pub(crate) aggregates: Vec<(NodeIndex, Vec<u32>)>,
    /// Trigger group keys (correlation-buffer keys) that drove the
    /// scope expansion. Used by `flush` to format DLQ trigger messages
    /// against the same group identifier the strict path emits. `Vec`
    /// rather than `BTreeSet` because `GroupByKey` does not implement
    /// `Ord`; the keys are pre-sorted by formatted-string before they
    /// land here so deterministic emission is preserved.
    pub(crate) trigger_group_keys: Vec<Vec<GroupByKey>>,
}

/// Compute the retract scope from `ctx.correlation_buffers` plus the
/// per-node `ck_set` lattice on `current_dag.node_properties`.
pub(crate) fn detect_retract_scope(
    ctx: &ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
) -> RetractScope {
    let mut scope = RetractScope::default();
    let Some(buffers) = ctx.correlation_buffers.as_ref() else {
        return scope;
    };

    // Trigger groups = every correlation buffer cell with at least one
    // error row. Sorted by formatted key string for deterministic
    // emission.
    let mut trigger_keys: Vec<Vec<GroupByKey>> = buffers
        .iter()
        .filter(|(_, g)| !g.error_rows.is_empty() || g.overflowed)
        .map(|(k, _)| k.clone())
        .collect();
    trigger_keys.sort_by_key(|k| format_group_key(k));
    scope.trigger_group_keys = trigger_keys.clone();

    // For each triggered group, walk every relaxed-CK Aggregate in the
    // DAG and add its node index to the scope. Today's Aggregate state
    // retention does not key per-row IDs by trigger-group identity (the
    // retained `HashAggregator` holds the entire input set), so the
    // affected per-aggregate row id list is the union of every error
    // row across every trigger group. The `recompute_agg` phase calls
    // `retract_row` for each id; the orchestrator's flush phase honors
    // the per-output fan-out policy when deciding which collateral
    // records to spare.
    let mut affected_row_ids: BTreeSet<u32> = BTreeSet::new();
    for key in &trigger_keys {
        if let Some(group) = buffers.get(key) {
            for &row in &group.error_rows {
                affected_row_ids.insert(row as u32);
            }
        }
    }

    if affected_row_ids.is_empty() {
        return scope;
    }

    // Aggregates: every relaxed-CK aggregate node in the current DAG.
    // We don't pre-filter against the trigger's CK set here because
    // `retract_row` is idempotent on row IDs that don't appear in the
    // aggregator's lineage — the lookup either succeeds or returns an
    // error which the recompute phase routes to the degrade-fallback.
    for idx in current_dag.graph.node_indices() {
        if let PlanNode::Aggregation {
            relaxed_correlation_key: true,
            ..
        } = &current_dag.graph[idx]
        {
            scope
                .aggregates
                .push((idx, affected_row_ids.iter().copied().collect()));
        }
    }

    scope
}

fn format_group_key(key: &[GroupByKey]) -> String {
    let parts: Vec<String> = key
        .iter()
        .map(|k| match k {
            GroupByKey::Null => "null".to_string(),
            GroupByKey::Bool(b) => b.to_string(),
            GroupByKey::Int(i) => i.to_string(),
            GroupByKey::Float(bits) => f64::from_bits(*bits).to_string(),
            GroupByKey::Str(s) => format!("{s:?}"),
            GroupByKey::Date(d) => d.to_string(),
            GroupByKey::DateTime(ts) => ts.to_string(),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}
