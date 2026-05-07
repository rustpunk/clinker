//! Detect retract scope (orchestrator step 1 of 3).
//!
//! Walks `ctx.correlation_buffers` for groups carrying at least one
//! `error_rows` entry (the trigger set), then expands each to a set of
//! affected `(aggregate_node, group_key)` pairs by intersecting the
//! trigger group's CK fields with each downstream Aggregate's
//! `group_by`.

use std::collections::{BTreeSet, HashMap};

use clinker_record::{FieldMetadata, GroupByKey, Schema};
use petgraph::graph::NodeIndex;

use super::DlqEvent;
use crate::executor::dispatch::ExecutorContext;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};

/// Output of the detect phase. Consumed by `recompute_agg` and `flush`.
#[derive(Debug, Default)]
pub(crate) struct RetractScope {
    /// Affected aggregate node + group keys that need rerun. Each
    /// entry's `Vec<u32>` carries the runtime-DLQ-triggered input row
    /// IDs to retract from that aggregate's per-group state.
    pub(crate) aggregates: Vec<(NodeIndex, Vec<u64>)>,
    /// Trigger group keys (correlation-buffer keys) that drove the
    /// scope expansion. Used by `flush` to format DLQ trigger messages
    /// against the same group identifier the strict path emits. `Vec`
    /// rather than `BTreeSet` because `GroupByKey` does not implement
    /// `Ord`; the keys are pre-sorted by formatted-string before they
    /// land here so deterministic emission is preserved.
    pub(crate) trigger_group_keys: Vec<Vec<GroupByKey>>,
    /// Union of every source row id that has been folded into the
    /// scope across all iterations of the orchestrator's commit loop.
    /// Used by [`Self::expand_with_dlq_events`] to compute the
    /// per-iteration delta — a DLQ event whose `source_row` is already
    /// in this set is a no-op for the loop. Termination follows
    /// directly: source rows are bounded, so a strictly-monotone set
    /// caps the loop at `|source_rows|` iterations regardless of how
    /// many DLQ events each iteration produces.
    pub(crate) seen_source_rows: BTreeSet<u64>,
}

/// Compute the retract scope from `ctx.correlation_buffers` plus the
/// per-node `ck_set` lattice on `current_dag.node_properties`.
pub(crate) fn detect_retract_scope(
    ctx: &mut ExecutorContext<'_>,
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

    // Aggregate-name → NodeIndex map, built once per detect call so
    // synthetic-CK column lookups below do not re-scan node weights for
    // every trigger key. Spans the parent DAG plus every composition
    // body's graph so a parent-scope error cell carrying a body
    // aggregate's synthetic-CK column resolves back to the body's
    // (body-local) NodeIndex — the same key that addresses
    // ctx.relaxed_aggregator_states, populated by the body forward-pass
    // arm under the body-local index. Aggregate names are unique across
    // the pipeline because compile-time path qualification prepends
    // each body's call-site name, but `entry().or_insert` is used
    // defensively so a parent-scope name stays authoritative if that
    // qualification ever weakens.
    let mut aggregate_idx_by_name: HashMap<String, NodeIndex> = current_dag
        .graph
        .node_indices()
        .filter_map(|idx| match &current_dag.graph[idx] {
            PlanNode::Aggregation { name, .. } => Some((name.clone(), idx)),
            _ => None,
        })
        .collect();
    for body in ctx.artifacts.composition_bodies.values() {
        for idx in body.graph.node_indices() {
            if let PlanNode::Aggregation { name, .. } = &body.graph[idx] {
                aggregate_idx_by_name.entry(name.clone()).or_insert(idx);
            }
        }
    }

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
    let mut affected_row_ids: BTreeSet<u64> = BTreeSet::new();
    // Accumulate counter deltas locally so the immutable borrow of
    // `ctx.correlation_buffers` (and the immutable hand to
    // `ctx.relaxed_aggregator_states`) does not collide with the
    // mutable counter writes; we apply them below in one shot once the
    // buffer borrow ends.
    let mut synthetic_ck_lookups: u64 = 0;
    let mut synthetic_ck_rows_expanded: u64 = 0;
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
                        // The aggregator stamps `state.group_index` as
                        // `Value::Integer`, but `value_to_group_key`
                        // widens unpinned `Value::Integer` to
                        // `GroupByKey::Float` (the canonical numeric
                        // group-key shape used so int/float groups
                        // collide deterministically). Decode either
                        // shape back to a `u32` group index. A
                        // non-numeric key here means the cell predates
                        // the synthetic-CK landing or the aggregator
                        // failed to populate the slot.
                        let group_idx = match key.get(key_pos) {
                            Some(GroupByKey::Int(raw)) => match u32::try_from(*raw) {
                                Ok(idx) => idx,
                                Err(_) => continue,
                            },
                            Some(GroupByKey::Float(bits)) => {
                                let f = f64::from_bits(*bits);
                                if !f.is_finite() || f < 0.0 || f > u32::MAX as f64 {
                                    continue;
                                }
                                let rounded = f as u32;
                                if rounded as f64 != f {
                                    // Float carrying a non-integral
                                    // value; the aggregator never emits
                                    // such a key for the synthetic CK
                                    // slot, so skip rather than mask a
                                    // contract violation.
                                    continue;
                                }
                                rounded
                            }
                            _ => continue,
                        };
                        had_synthetic_lookup = true;
                        // One observation of the synthetic-CK column on
                        // a triggered cell — count regardless of whether
                        // the retained aggregator state is available, so
                        // a persistent gap between lookup and expansion
                        // counters surfaces a degrade-fallback pattern.
                        synthetic_ck_lookups += 1;
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
                            synthetic_ck_rows_expanded += rows.len() as u64;
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
                affected_row_ids.insert(row);
            }
        }
    }
    // The immutable borrow of `ctx.correlation_buffers` ends at the
    // last use above (NLL), so the mutable counter writes below
    // compile against fresh borrows.
    ctx.counters.retraction.synthetic_ck_fanout_lookups_total += synthetic_ck_lookups;
    ctx.counters
        .retraction
        .synthetic_ck_fanout_rows_expanded_total += synthetic_ck_rows_expanded;

    // Aggregates: every relaxed-CK aggregate in the DAG, populated
    // eagerly so the orchestrator's cascading-retraction loop can route
    // a source-row id discovered by deferred dispatch through every
    // aggregator's `retract_row` regardless of whether the initial
    // detect pass surfaced any triggers. `retract_row` is idempotent on
    // ids that don't appear in an aggregator's lineage — the lookup
    // succeeds or returns "not found" which the recompute phase
    // tolerates as a wide-fanout no-op. The lattice on
    // `node_properties` already encodes the per-source CK identity per
    // node, so the classification below stays consistent with the
    // planner's assignment of relaxed-mode flags.
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

    // Seed the cumulative source-row set with the initial trigger fan-in.
    // The orchestrator's commit loop folds each iteration's deferred-DLQ
    // events into this set via [`RetractScope::expand_with_dlq_events`];
    // a strictly-monotone set against a bounded source-row universe is
    // the structural termination proof for the loop.
    scope.seen_source_rows = affected_row_ids;

    scope
}

impl RetractScope {
    /// Fold a batch of deferred-dispatch DLQ events into this scope and
    /// return the entries whose `source_row` was not already covered.
    ///
    /// The orchestrator drives one extra commit-loop iteration per
    /// non-empty return value: the new triggers widen
    /// `aggregates[*].1` so the next `recompute_aggregates` call
    /// retracts the additional source rows from each relaxed-CK
    /// aggregator. Empty return signals convergence — every deferred
    /// failure observed on this iteration was already accounted for by
    /// a prior iteration's retract scope, so further looping cannot
    /// surface new state changes.
    ///
    /// Each appended row id flows through `retract_row`'s "not found"
    /// tolerance for aggregates whose lineage doesn't include it, so
    /// the wide-fanout shape (every relaxed-CK aggregate sees every
    /// new row id) stays correct without per-aggregate filtering.
    pub(crate) fn expand_with_dlq_events(&mut self, events: &[DlqEvent]) -> Vec<u64> {
        let mut new_rows: Vec<u64> = Vec::new();
        for event in events {
            let row = event.source_row;
            if !self.seen_source_rows.insert(row) {
                continue;
            }
            new_rows.push(row);
        }
        if !new_rows.is_empty() {
            for (_, retract_ids) in &mut self.aggregates {
                retract_ids.extend(new_rows.iter().copied());
            }
        }
        new_rows
    }
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
