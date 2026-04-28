//! Phase 1: detect retract scope.
//!
//! Walks `ctx.correlation_buffers` for groups carrying at least one
//! `error_rows` entry (the trigger set), then expands each to a set of
//! affected `(aggregate_node, group_key)` pairs by intersecting the
//! trigger group's CK fields with each downstream Aggregate's
//! `group_by`. The window-affected list runs the same expansion against
//! every Transform with a buffer-recompute IndexSpec, mapping retract
//! row IDs to the partitions they originally landed in.

use std::collections::{BTreeSet, HashMap};

use clinker_record::{FieldMetadata, GroupByKey, Schema};
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

    // Aggregate-name → DAG NodeIndex map, built once per detect call so
    // synthetic-CK column lookups below do not re-scan node weights for
    // every trigger key.
    let aggregate_idx_by_name: HashMap<&str, NodeIndex> = current_dag
        .graph
        .node_indices()
        .filter_map(|idx| match &current_dag.graph[idx] {
            PlanNode::Aggregation { name, .. } => Some((name.as_str(), idx)),
            _ => None,
        })
        .collect();

    // For each triggered group, classify its key columns by their
    // `FieldMetadata` to decide which row IDs feed `affected_row_ids`.
    //
    // - Source-CK column: the cell's `error_rows` are post-source row
    //   numbers and feed `retract_row` directly through the lineage path
    //   the aggregator built at ingest. Union them in.
    // - Synthetic-CK column (`AggregateGroupIndex`): the cell's
    //   `error_rows` are aggregate-output row numbers — NOT source row
    //   IDs — so they cannot drive `retract_row`. Resolve the encoded
    //   `group_index` back to the contributing source rows via the
    //   retained aggregator's `input_rows` table and union those.
    // - Mixed cell: both are processed independently; their row IDs
    //   union additively into `affected_row_ids`.
    //
    // When the aggregator is degraded (state spilled or never
    // instantiated), the synthetic-CK lookup misses and the existing
    // degrade-fallback in `recompute_agg.rs` routes the affected group
    // through strict-collateral DLQ.
    let mut affected_row_ids: BTreeSet<u32> = BTreeSet::new();
    for key in &trigger_keys {
        let Some(group) = buffers.get(key) else {
            continue;
        };
        // Probe the cell's schema for engine-stamped column lineage.
        // Every CorrelationErrorRecord in a given cell shares the same
        // schema (cells are keyed by the engine-stamped tuple), so the
        // first error_message is representative.
        let schema = group
            .error_messages
            .first()
            .map(|err| err.original_record.schema().clone());
        let mut has_source_ck = false;
        let mut had_synthetic_lookup = false;
        if let Some(schema) = schema.as_ref() {
            for col_idx in 0..schema.column_count() {
                match schema.field_metadata(col_idx) {
                    Some(FieldMetadata::SourceCorrelation { .. }) => {
                        has_source_ck = true;
                    }
                    Some(FieldMetadata::AggregateGroupIndex { aggregate_name }) => {
                        let Some(key_pos) = buffer_key_position_for_column(schema, col_idx) else {
                            continue;
                        };
                        let Some(&GroupByKey::Int(raw)) = key.get(key_pos) else {
                            // Synthetic CK should always encode as
                            // GroupByKey::Int (the aggregator stamps the
                            // u32 group_index as Value::Integer); a
                            // non-Int key here means the cell predates
                            // the synthetic-CK landing or the aggregator
                            // failed to populate the slot. Nothing to
                            // resolve back to source rows.
                            continue;
                        };
                        let Ok(group_idx) = u32::try_from(raw) else {
                            continue;
                        };
                        had_synthetic_lookup = true;
                        let Some(&node_idx) = aggregate_idx_by_name.get(aggregate_name.as_ref())
                        else {
                            continue;
                        };
                        let Some(retained) = ctx.relaxed_aggregator_states.get(&node_idx) else {
                            // Degrade-fallback: the aggregator's state
                            // is gone (spilled mid-run, or the relaxed
                            // dispatch arm never retained it because the
                            // node ran the strict path). The
                            // recompute-aggregates phase's
                            // degrade-fallback (`recompute_agg.rs`)
                            // routes the affected group through
                            // strict-collateral DLQ — no synthetic-CK
                            // expansion needed here.
                            continue;
                        };
                        if let Some(rows) = retained.aggregator.input_rows_by_group_index(group_idx)
                        {
                            for &r in rows {
                                affected_row_ids.insert(r);
                            }
                        }
                    }
                    None => {}
                }
            }
        }

        // Source-CK or no-CK cells: the cell's `error_rows` ARE source
        // row numbers (modulo the row-number-disambiguator path for
        // null-keyed cells), so they feed `retract_row` directly. A
        // pure-AggregateGroupIndex cell whose synthetic lookup landed
        // already covered its contributing source rows; including the
        // raw `error_rows` here would feed aggregate-output row numbers
        // into `retract_row` and exercise the not-found tolerance,
        // silently no-opping. Skip the raw union in that case.
        if has_source_ck || !had_synthetic_lookup {
            for &row in &group.error_rows {
                affected_row_ids.insert(row as u32);
            }
        }
    }

    if affected_row_ids.is_empty() {
        return scope;
    }

    // Aggregates: every aggregate node whose `group_by` omits a CK
    // field visible upstream — those activate the retraction protocol.
    // The lattice on `node_properties` already encodes the per-source
    // CK identity per node, so the classification below stays
    // consistent with the planner's assignment of relaxed-mode flags.
    // We don't pre-filter against the trigger's CK set here because
    // `retract_row` is idempotent on row IDs that don't appear in the
    // aggregator's lineage — the lookup either succeeds or returns an
    // error which the recompute phase routes to the degrade-fallback.
    for idx in current_dag.graph.node_indices() {
        if let PlanNode::Aggregation { config, .. } = &current_dag.graph[idx] {
            let parent_ck = current_dag
                .graph
                .neighbors_directed(idx, petgraph::Direction::Incoming)
                .next()
                .and_then(|p| current_dag.node_properties.get(&p))
                .map(|p| p.ck_set.clone())
                .unwrap_or_default();
            if crate::plan::execution::group_by_omits_any_ck_field(&config.group_by, &parent_ck) {
                scope
                    .aggregates
                    .push((idx, affected_row_ids.iter().copied().collect()));
            }
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

/// Position of `target_col_idx` within the buffer-key tuple emitted by
/// `buffer_key_for_record`. The buffer-key builder walks the schema in
/// column order and pushes one component per engine-stamped column, so
/// the position of any given engine-stamped column is the count of
/// engine-stamped columns that precede it. Returns `None` when
/// `target_col_idx` is itself not engine-stamped (a caller-side
/// invariant violation; the production callers all gate on
/// `field_metadata(idx)` first).
fn buffer_key_position_for_column(schema: &Schema, target_col_idx: usize) -> Option<usize> {
    if !schema
        .field_metadata(target_col_idx)
        .is_some_and(|m| m.is_engine_stamped())
    {
        return None;
    }
    Some(
        (0..target_col_idx)
            .filter(|&i| {
                schema
                    .field_metadata(i)
                    .is_some_and(|m| m.is_engine_stamped())
            })
            .count(),
    )
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
