//! Phase 2: recompute aggregates.
//!
//! For each affected `(aggregate_node, group_key)`, calls
//! `HashAggregator::retract_row` for every retracted input row id, then
//! re-finalizes the aggregator in place against the surviving
//! contributions. Emits commit-local [`super::Delta`] entries describing
//! the retract-old/add-new aggregate output rows so the replay phase can
//! propagate the change downstream.
//!
//! The retained `HashAggregator` per relaxed-CK aggregate node lives on
//! `ExecutorContext.relaxed_aggregator_states`. The Aggregate dispatch
//! arm in `dispatch.rs` parks the aggregator instance there before
//! finalize-into-buffer; the orchestrator drains and re-uses it here.

use super::{Delta, detect::RetractScope};
use crate::aggregation::HashAggError;
use crate::error::PipelineError;
use crate::executor::dispatch::ExecutorContext;

/// Drive each affected aggregate through retract + refinalize.
///
/// On per-aggregator failure (e.g. retract against a spilled aggregator),
/// the protocol degrades to "DLQ entire affected group" — the
/// orchestrator's flush phase emits collateral entries for every record
/// derived from the degraded aggregate, mirroring today's strict
/// E151-collateral semantics.
pub(crate) fn recompute_aggregates(
    ctx: &mut ExecutorContext<'_>,
    scope: &RetractScope,
) -> Result<Vec<Delta>, PipelineError> {
    if scope.aggregates.is_empty() {
        return Ok(Vec::new());
    }

    let mut deltas: Vec<Delta> = Vec::new();
    let mut degraded: Vec<petgraph::graph::NodeIndex> = Vec::new();

    let pending: Vec<(petgraph::graph::NodeIndex, Vec<u32>)> = scope.aggregates.clone();
    for (agg_idx, retract_ids) in pending {
        let Some(retained) = ctx.relaxed_aggregator_states.remove(&agg_idx) else {
            // No retained state — the aggregate either spilled (degrade
            // path) or was never instantiated. Mark for degrade-fallback
            // collateral handling in flush.
            degraded.push(agg_idx);
            continue;
        };

        match recompute_one_aggregate(ctx, agg_idx, retained, &retract_ids) {
            Ok(mut local_deltas) => {
                deltas.append(&mut local_deltas);
            }
            Err(_) => {
                degraded.push(agg_idx);
            }
        }
    }

    ctx.relaxed_aggregator_degrade.extend(degraded);
    Ok(deltas)
}

/// Per-aggregate retract loop, isolated for cleaner error attribution.
fn recompute_one_aggregate(
    ctx: &mut ExecutorContext<'_>,
    agg_idx: petgraph::graph::NodeIndex,
    mut retained: crate::executor::dispatch::RetainedAggregatorState,
    retract_ids: &[u32],
) -> Result<Vec<Delta>, HashAggError> {
    use cxl::eval::EvalContext;

    let mut deltas: Vec<Delta> = Vec::new();

    for &row_id in retract_ids {
        if let Err(e) = retained.aggregator.retract_row(row_id) {
            // Row id wasn't part of this aggregator's lineage. The
            // detect phase casts a wide net (every relaxed-CK aggregate
            // sees every triggered row id), so a not-found here is
            // expected for aggregates that weren't on the trigger's
            // path. Treat as no-op.
            if matches!(e, HashAggError::Spill(ref msg) if msg.contains("not found")) {
                continue;
            }
            return Err(e);
        }
    }

    // Finalize the post-retract state in place to produce updated
    // output rows. The pre-retract output rows came from the original
    // finalize call; we rebuild them from the retained
    // `pre_retract_output_rows` snapshot for the retract-old half of
    // each Delta.
    let mut new_rows = Vec::new();
    let finalize_ctx = EvalContext {
        stable: ctx.stable,
        source_file: ctx.source_file_arc,
        source_row: 0,
    };
    retained
        .aggregator
        .finalize_in_place(&finalize_ctx, &mut new_rows)?;

    // Pair pre-retract and post-retract rows by group-key prefix.
    // Aggregator output preserves the group_by columns at the head of
    // each row, which we use as the matching key here. Rows that
    // disappeared post-retract are paired against `None` (empty group);
    // rows that appear post-retract but were absent pre-retract are
    // emitted as add-only deltas (typically the empty case where every
    // row in a group was retracted).
    let group_by_indices = retained.group_by_indices.clone();
    for old in retained.pre_retract_output_rows.iter() {
        let key = extract_group_key_values(&old.0, &group_by_indices);
        let matching = new_rows
            .iter()
            .position(|new| extract_group_key_values(&new.0, &group_by_indices) == key);
        let new_row = matching.map(|i| new_rows[i].0.clone());
        deltas.push(Delta {
            source_node: agg_idx,
            retract_old_row: Some(old.0.clone()),
            add_new_row: new_row,
        });
    }
    // Catch new rows that didn't pair against any old row (no-op for
    // today's path — every group must have existed pre-retract because
    // retract is subtraction over the same group set).
    for new in new_rows.iter() {
        let key = extract_group_key_values(&new.0, &group_by_indices);
        let already_paired = retained
            .pre_retract_output_rows
            .iter()
            .any(|old| extract_group_key_values(&old.0, &group_by_indices) == key);
        if !already_paired {
            deltas.push(Delta {
                source_node: agg_idx,
                retract_old_row: None,
                add_new_row: Some(new.0.clone()),
            });
        }
    }

    Ok(deltas)
}

/// Extract the group-by column values from a finalized aggregate output
/// record. Used to pair pre-retract and post-retract output rows by
/// matching their group identity column-for-column.
fn extract_group_key_values(
    record: &clinker_record::Record,
    indices: &[u32],
) -> Vec<clinker_record::Value> {
    indices
        .iter()
        .map(|&i| {
            record
                .values()
                .get(i as usize)
                .cloned()
                .unwrap_or(clinker_record::Value::Null)
        })
        .collect()
}
