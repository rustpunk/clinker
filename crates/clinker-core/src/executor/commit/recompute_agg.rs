//! Recompute relaxed-CK aggregates and re-seed their producer slots.
//!
//! For each affected aggregate, calls `HashAggregator::retract_row` for
//! every triggered input row id, re-finalizes the aggregator in place
//! against the surviving contributions, then projects the post-retract
//! finalize output onto the producer's deferred-region `buffer_schema`
//! and writes it into `ctx.node_buffers[producer_idx]`. The
//! deferred-region dispatcher consumes that slot to re-feed the
//! downstream operator chain on the commit pass — one set of operator
//! arms covers both forward and commit dispatch.
//!
//! The retained `HashAggregator` per relaxed-CK aggregate node lives on
//! `ExecutorContext.relaxed_aggregator_states`. The Aggregate dispatch
//! arm in `dispatch.rs` parks the aggregator instance there before
//! finalize-into-buffer; this phase drains and re-uses it.

use super::detect::RetractScope;
use crate::aggregation::HashAggError;
use crate::error::PipelineError;
use crate::executor::dispatch::{
    ExecutorContext, finalize_node_rooted_windows, project_rows_to_buffer_schema,
};
use crate::plan::execution::ExecutionPlanDag;

/// Drive each affected aggregate through retract + refinalize, then
/// re-seed the producer's `node_buffers` slot with the post-recompute
/// rows projected to the deferred region's `buffer_schema`.
///
/// `new_retract_rows` carries only the row ids the orchestrator wants
/// applied THIS iteration. The retained `HashAggregator` lives across
/// iterations on `ctx.relaxed_aggregator_states` so each call retracts
/// only the per-iteration delta — re-feeding the cumulative set would
/// double-retract rows the previous iteration already removed and
/// `retract_row` is not idempotent on a row already taken out.
///
/// On per-aggregator failure (e.g. retract against a spilled aggregator),
/// the protocol degrades to "DLQ entire affected group" — the
/// orchestrator's flush phase emits collateral entries for every record
/// derived from the degraded aggregate, mirroring the strict-collateral
/// DLQ shape every aggregate uses on the strict path. Degraded
/// aggregates leave `node_buffers[agg_idx]` empty so the deferred
/// dispatcher emits nothing for the affected region on this iteration.
pub(crate) fn recompute_aggregates(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    scope: &RetractScope,
    new_retract_rows: &[u64],
) -> Result<(), PipelineError> {
    if scope.aggregates.is_empty() || new_retract_rows.is_empty() {
        return Ok(());
    }

    let mut degraded: Vec<petgraph::graph::NodeIndex> = Vec::new();

    let pending: Vec<petgraph::graph::NodeIndex> =
        scope.aggregates.iter().map(|(idx, _)| *idx).collect();
    for agg_idx in pending {
        // Apply the per-iteration retract delta in a borrow scope
        // that releases `&mut ctx.relaxed_aggregator_states` before
        // the emit pass needs `&mut ctx` for both the aggregator and
        // the buffer slot. A failed retract here marks the aggregate
        // degraded and skips the emit pass.
        let retract_ok = {
            let Some(retained) = ctx.relaxed_aggregator_states.get_mut(&agg_idx) else {
                // No retained state — the aggregate either spilled
                // (degrade path) or was never instantiated. Mark for
                // degrade-fallback collateral handling in flush.
                degraded.push(agg_idx);
                ctx.node_buffers.remove(&agg_idx);
                continue;
            };
            retract_and_refinalize(retained, new_retract_rows).is_ok()
        };
        if !retract_ok {
            degraded.push(agg_idx);
            ctx.node_buffers.remove(&agg_idx);
            ctx.relaxed_aggregator_states.remove(&agg_idx);
            continue;
        }

        match emit_post_recompute(ctx, current_dag, agg_idx) {
            Ok(emitted) => {
                ctx.counters.retraction.groups_recomputed += emitted as u64;
            }
            Err(_) => {
                degraded.push(agg_idx);
                ctx.node_buffers.remove(&agg_idx);
                ctx.relaxed_aggregator_states.remove(&agg_idx);
            }
        }
    }

    ctx.counters.retraction.degrade_fallback_count += degraded.len() as u64;
    ctx.relaxed_aggregator_degrade.extend(degraded);
    Ok(())
}

/// Apply the per-iteration retract delta to the retained aggregator
/// in place. Returns `Ok(())` on success, including the wide-net
/// "row not in this aggregator's lineage" case (treated as no-op).
fn retract_and_refinalize(
    retained: &mut crate::executor::dispatch::RetainedAggregatorState,
    retract_ids: &[u64],
) -> Result<(), HashAggError> {
    for &row_id in retract_ids {
        if let Err(e) = retained.aggregator.retract_row(row_id) {
            // Wide-net no-op: the orchestrator hands every relaxed-CK
            // aggregate every newly-failed source row, so an aggregate
            // that didn't ingest this row reports `not found` — that's
            // expected, not an error.
            if matches!(e, HashAggError::Spill(ref msg) if msg.contains("not found")) {
                continue;
            }
            return Err(e);
        }
    }
    Ok(())
}

/// Re-finalize the post-retract aggregator state in place and seed
/// the producer's `node_buffers` slot with the projected narrow rows.
/// Returns the number of rows emitted into the slot.
///
/// `pub(crate)` so the body-recursion helper in
/// [`super::dispatch::recurse_into_body`] can seed body-local
/// `node_buffers[producer]` from the body aggregator's retained state
/// — body forward passes drop body-local buffers on exit, so the
/// commit-pass dispatcher sees an empty slot until this seeds it.
pub(crate) fn emit_post_recompute(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    agg_idx: petgraph::graph::NodeIndex,
) -> Result<usize, HashAggError> {
    use cxl::eval::EvalContext;

    let retained = ctx
        .relaxed_aggregator_states
        .get_mut(&agg_idx)
        .expect("caller verified retained state present");
    let mut new_rows: Vec<crate::aggregation::SortRow> = Vec::new();
    let finalize_ctx = EvalContext {
        stable: ctx.stable,
        source_file: ctx.source_file_arc,
        source_row: 0,
        source_path: ctx.source_path_arc,
        source_count: ctx.source_count,
        source_batch: ctx.source_batch_arc,
        ingestion_timestamp: ctx.source_ingestion_timestamp,
    };
    let emits_synthetic = retained.aggregator.emits_synthetic_ck();
    retained
        .aggregator
        .finalize_in_place(&finalize_ctx, &mut new_rows)?;
    let emitted = new_rows.len();
    if emits_synthetic {
        ctx.counters.retraction.synthetic_ck_columns_emitted_total += emitted as u64;
    }

    // Project to the deferred region's narrow buffer schema — same
    // helper the forward-pass Aggregate arm uses for region producers
    // — so the deferred dispatcher's downstream operator chain sees
    // identically shaped rows regardless of which pass produced them.
    let buffer_schema = current_dag
        .deferred_region_at_producer(agg_idx)
        .map(|r| r.buffer_schema.clone());
    let projected: Vec<(clinker_record::Record, u64)> = match buffer_schema {
        Some(schema_cols) => project_rows_to_buffer_schema(new_rows, &schema_cols),
        // Missing region at a relaxed-CK aggregate is a planner
        // contract violation; surface it as the wide-row identity
        // emit rather than panic so the orchestrator's
        // degrade-fallback can still route the affected group
        // through strict-collateral DLQ on flush.
        None => new_rows,
    };

    // Re-materialize node-rooted window arenas before the deferred
    // dispatcher walks the region's downstream members. The forward
    // pass skips this for deferred-region producers (the windows are
    // consumed by deferred members, which do not run on the forward
    // pass), so the runtime registry is empty when the commit pass
    // re-enters the windowed Transform arm. Each iteration overwrites
    // the slot so the arena reflects the post-retract emit set.
    //
    // `arena_fields` ⊆ `buffer_schema` by construction
    // (`pass_b_column_prune` seeds windowed Transforms with their
    // `IndexSpec.arena_fields`), so the narrow `projected` rows carry
    // every column the arena projection requires. Maps the
    // `Compilation` shape `finalize_node_rooted_windows` raises into
    // `HashAggError::Spill` so the surrounding match arm handles it
    // through the same degrade-fallback path a retract failure takes.
    if let Err(e) = finalize_node_rooted_windows(ctx, current_dag, agg_idx, &projected) {
        return Err(HashAggError::Spill(format!(
            "node-rooted window rebuild during commit-pass recompute: {e}"
        )));
    }
    ctx.node_buffers.insert(agg_idx, projected);
    Ok(emitted)
}
