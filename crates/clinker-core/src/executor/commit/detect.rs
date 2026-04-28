//! Phase 1: detect retract scope.
//!
//! Walks `ctx.correlation_buffers` for groups carrying at least one
//! `error_rows` entry (the trigger set), then expands each to a set of
//! affected `(aggregate_node, group_key)` pairs by intersecting the
//! trigger group's CK fields with each downstream Aggregate's
//! `group_by`. The window-affected list runs the same expansion against
//! every Transform with a buffer-recompute IndexSpec, mapping retract
//! row IDs to the partitions they originally landed in.

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
    /// Affected window-bearing Transform nodes. Each entry pairs the
    /// Transform's NodeIndex with the per-partition retract row id
    /// list. Populated only when the planner flagged the IndexSpec
    /// for buffer-recompute and the dispatcher captured per-partition
    /// admission state on `ExecutorContext.relaxed_window_states`.
    /// `partition_retracts[i].0` is the partition tuple; the
    /// associated `Vec<u32>` is the set of arena positions to drop
    /// from that partition's surviving slice.
    pub(crate) windows: Vec<WindowRetractEntry>,
}

/// One affected window-bearing Transform plus the per-partition retract
/// row id lists. `partition_retracts` is keyed by the partition tuple
/// (the same `Vec<GroupByKey>` shape the dispatcher's window arm used
/// to admit rows into `RetainedWindowState.partition_outputs`).
#[derive(Debug)]
pub(crate) struct WindowRetractEntry {
    pub(crate) window_node: NodeIndex,
    pub(crate) partition_retracts: Vec<(Vec<GroupByKey>, Vec<u32>)>,
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

    // Aggregates: every aggregate node whose `group_by` omits a
    // correlation-key field — those activate the retraction protocol.
    // We don't pre-filter against the trigger's CK set here because
    // `retract_row` is idempotent on row IDs that don't appear in the
    // aggregator's lineage — the lookup either succeeds or returns an
    // error which the recompute phase routes to the degrade-fallback.
    let correlation_key = ctx.config.error_handling.correlation_key.as_ref();
    for idx in current_dag.graph.node_indices() {
        if let PlanNode::Aggregation { config, .. } = &current_dag.graph[idx]
            && crate::plan::execution::group_by_omits_any_ck_field(
                &config.group_by,
                correlation_key,
            )
        {
            scope
                .aggregates
                .push((idx, affected_row_ids.iter().copied().collect()));
        }
    }

    // Windows: every Transform whose IndexSpec is flagged for
    // buffer-recompute. Walk the captured per-partition admission
    // state and drop each retract row id into its origin partition.
    // The window operator admitted records using `record_pos = rn - 1`
    // as the arena position, mirroring the Aggregate path's row-id
    // lineage; `affected_row_ids` therefore directly maps onto the
    // arena positions stored on `RetainedWindowState.partition_outputs`.
    for (&window_node, retained) in &ctx.relaxed_window_states {
        let mut partition_retracts: Vec<(Vec<GroupByKey>, Vec<u32>)> = Vec::new();
        for (partition_key, rows) in &retained.partition_outputs {
            let drops: Vec<u32> = rows
                .iter()
                .filter_map(|(arena_pos, _, _)| {
                    if affected_row_ids.contains(arena_pos) {
                        Some(*arena_pos)
                    } else {
                        None
                    }
                })
                .collect();
            if !drops.is_empty() {
                partition_retracts.push((partition_key.clone(), drops));
            }
        }
        if !partition_retracts.is_empty() {
            scope.windows.push(WindowRetractEntry {
                window_node,
                partition_retracts,
            });
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
