//! `PlanNode::Transform` dispatch arm.
//!
//! Holds the record-level CXL projection / filter / lookup body lifted out
//! of [`crate::executor::dispatch::dispatch_plan_node`], including the
//! streaming-fused fast path that drives per-record evaluation directly off
//! a Source receiver and the buffered `node_buffers` path. The dispatcher's
//! `Transform` arm is a single delegating call into [`dispatch_transform`].

use std::sync::Arc;

use clinker_record::Record;
use cxl::eval::{ProgramEvaluator, SkipReason};
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, advance_cursor, dispatch_transform_eval_error,
    drain_node_buffer_slot, finalize_node_rooted_windows, node_buffer_spill_allowed,
    source_file_arc_of, source_name_arc_of, tee_emit_to_region_input_buffers,
    transform_fused_consume,
};
use crate::executor::schema_check::check_input_schema;
use crate::executor::{
    WindowedEvalCtx, evaluate_single_transform, evaluate_single_transform_windowed,
};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Execute the `Transform` arm for `node_idx`: drive per-record CXL
/// evaluation (filter, projection, distinct, emit_each fan-out) over the
/// predecessor's records, taking the streaming-fused path off a Source
/// receiver when the pre-pass flagged this Transform eligible and the
/// buffered `node_buffers` path otherwise. Stateless and streaming.
pub(crate) fn dispatch_transform(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Transform {
        ref name,
        window_index,
        ref resolved,
        has_distinct,
        ..
    } = *node
    else {
        unreachable!("dispatch_transform called with non-Transform node");
    };
    // Streaming-fused path: when the pre-pass has flagged this
    // Transform as eligible (sole upstream is a Source whose
    // receiver lives in `ctx.source_records`, non-windowed,
    // non-init-phase, no upstream fan-out, and the Source is
    // not already claimed by Merge.interleave fusion), drive
    // per-record evaluation directly off the receiver instead
    // of consuming a Vec from `node_buffers`. See
    // https://github.com/rustpunk/clinker/issues/74.
    if ctx.should_fuse_transform(node_idx) {
        return transform_fused_consume(ctx, current_dag, node_idx, name);
    }
    // Get input events: first check own buffer (set by Route
    // node for branch dispatch), then fall back to predecessor.
    // Records flow into per-record evaluation; punctuations are
    // preserved verbatim and forwarded onto the output buffer
    // alongside the transformed records (Preserving behavior).
    let (input_records, input_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = if let Some(own_buf) = drain_node_buffer_slot(ctx, node_idx) {
        own_buf.drain_split_metered(&ctx.memory_budget, name.as_str())?
    } else {
        let predecessors: Vec<NodeIndex> = current_dag
            .graph
            .neighbors_directed(node_idx, Direction::Incoming)
            .collect();
        let pred_buf = if predecessors.len() == 1 {
            drain_node_buffer_slot(ctx, predecessors[0])
        } else {
            predecessors
                .iter()
                .find_map(|p| drain_node_buffer_slot(ctx, *p))
        };
        match pred_buf {
            // Meter the re-materialized drain so a spill-backed input cannot
            // re-inflate into RAM past the hard limit uncharged.
            Some(nb) => nb.drain_split_metered(&ctx.memory_budget, name.as_str())?,
            None => (Vec::new(), Vec::new()),
        }
    };

    // Read the typed program off the `PlanNode::Transform` payload. Every
    // lowered Transform carries a `Some(payload)` with a typechecked program;
    // an absent payload is the defensive pass-through (a node that failed to
    // lower has no program to run).
    let payload = match resolved {
        Some(p) => p,
        None => {
            tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &input_records)?;
            let nb = admit_node_buffer(
                ctx,
                name.as_str(),
                node_idx,
                input_records,
                input_puncts,
                node_buffer_spill_allowed(current_dag, node_idx),
            )?;
            ctx.node_buffers.insert(node_idx.into(), nb);
            return Ok(());
        }
    };

    // The compiled program is the per-record evaluator for the transform
    // hot loop: it lowers each statement to a closure once and skips the
    // per-record AST re-match a recursive tree-walk would pay. `has_distinct`
    // is read off the node — computed once at lowering, the single source of
    // truth `compute_node_properties` also reads — mirroring the aggregate
    // dispatch path rather than re-scanning the program per dispatch.
    let mut evaluator = ProgramEvaluator::with_max_expansion(
        Arc::clone(&payload.typed),
        has_distinct,
        payload.max_expansion,
    );

    let expected_input = current_dag.graph[node_idx]
        .expected_input_schema_in(current_dag)
        .cloned();
    let output_schema = current_dag.graph[node_idx].stored_output_schema().cloned();
    let upstream_name = current_dag
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .next()
        .map(|i| current_dag.graph[i].name().to_string())
        .unwrap_or_default();

    // Plan invariant: any Transform with `window_index: Some`
    // requires its WindowRuntime slot populated by either the
    // source-rooted Phase-0 build (Source root) or the upstream
    // operator's `finalize_node_rooted_windows` call (Node /
    // ParentNode roots). Fail loudly if the slot is missing —
    // the alternative (silently fall back to no-window eval)
    // corrupts `$window.*` results.
    if let Some(idx_num) = window_index {
        let spec =
            current_dag
                .indices_to_build
                .get(idx_num)
                .ok_or_else(|| PipelineError::Internal {
                    op: "executor",
                    node: name.clone(),
                    detail: format!(
                        "transform {name:?} declares window_index {idx_num} \
                             but plan.indices_to_build is too short"
                    ),
                })?;
        if ctx.window_runtime.resolve(&spec.root, idx_num).is_none() {
            return Err(PipelineError::Internal {
                op: "executor",
                node: name.clone(),
                detail: format!(
                    "transform {name:?} declares window_index {idx_num} \
                             but the runtime registry has no populated slot for that root; \
                             upstream operator did not finalize its node-rooted windows"
                ),
            });
        }
    }

    let mut output_records = Vec::with_capacity(input_records.len());

    for (i, (record, rn)) in input_records.into_iter().enumerate() {
        // Poll the shutdown flag every 1024 records so a long
        // Transform chain terminates promptly on SIGINT without
        // paying the atomic load per record.
        if i > 0 && i.is_multiple_of(1024) {
            ctx.check_shutdown()?;
        }
        if let Some(exp) = expected_input.as_ref() {
            check_input_schema(exp, record.schema(), name, "transform", &upstream_name)?;
        }
        let source_file_arc = source_file_arc_of(&record);
        let source_name_arc = source_name_arc_of(&record);
        let eval_ctx =
            ctx.eval_ctx_for_record(&source_file_arc, &source_name_arc, rn, record.doc_ctx());

        let target_schema = output_schema
            .as_ref()
            .cloned()
            .unwrap_or_else(|| Arc::clone(record.schema()));
        let eval_result = {
            let _guard = ctx.transform_timer.guard();
            if let Some(idx_num) = window_index {
                // Resolve the WindowRuntime via the spec's root.
                // Source-rooted windows root at Phase-0's source
                // arena; node-rooted windows root at the
                // upstream operator's finalize. The plan-time
                // invariant check above guarantees `resolve`
                // returns `Some`; reaching `None` here is an
                // upstream-arm bug (e.g. forgetting to call
                // `finalize_node_rooted_windows` after emit).
                let spec = &current_dag.indices_to_build[idx_num];
                let runtime = ctx
                    .window_runtime
                    .resolve(&spec.root, idx_num)
                    .ok_or_else(|| PipelineError::Internal {
                        op: "executor",
                        node: name.clone(),
                        detail: format!(
                            "transform {name:?} window_index {idx_num} \
                                         resolves to no runtime at per-record dispatch; \
                                         upstream finalize was skipped"
                        ),
                    })?;
                // record_pos: enumerate index `i`. Every arena
                // is node-rooted; it was built from the
                // upstream's emit buffer in iteration order,
                // and the per-record dispatch loop iterates
                // the same buffer, so `i` equals the row's
                // arena position by construction.
                let record_pos = i as u64;
                evaluate_single_transform_windowed(
                    &record,
                    name,
                    &mut evaluator,
                    &eval_ctx,
                    &WindowedEvalCtx {
                        plan: current_dag,
                        window_index: idx_num,
                        runtime,
                        record_pos,
                    },
                )
            } else {
                evaluate_single_transform(&record, name, &mut evaluator, &eval_ctx, &target_schema)
            }
        };
        match eval_result {
            Ok(records) => {
                if records.is_empty() {
                    advance_cursor(ctx, &source_name_arc_of(&record), rn);
                } else {
                    let mut emitted_any = false;
                    for (modified_record, status) in records {
                        match status {
                            Ok(()) => {
                                let advance_source = source_name_arc_of(&modified_record);
                                output_records.push((modified_record, rn));
                                advance_cursor(ctx, &advance_source, rn);
                                emitted_any = true;
                            }
                            Err(SkipReason::Filtered) => {
                                ctx.counters.filtered_count += 1;
                            }
                            Err(SkipReason::Duplicate) => {
                                ctx.counters.distinct_count += 1;
                            }
                        }
                    }
                    if !emitted_any {
                        advance_cursor(ctx, &source_name_arc_of(&record), rn);
                    }
                }
            }
            Err((transform_name, eval_err)) => {
                dispatch_transform_eval_error(ctx, record, rn, transform_name, eval_err)?;
            }
        }
    }

    // Materialize node-rooted window runtimes for any IndexSpec
    // rooted at THIS Transform. The schema-changing case is the
    // only one that drives node-rooted lookups here:
    // pure-passthrough Transforms feed downstream windows
    // through their predecessor's runtime (Sort/Route walk-
    // through at lowering time covers passthroughs that did not
    // change schema). When the lowering pass roots a window at
    // this Transform's `NodeIndex`, the call below installs the
    // matching runtime; otherwise the helper is a no-op.
    finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
    let nb = admit_node_buffer(
        ctx,
        name,
        node_idx,
        output_records,
        input_puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx.into(), nb);

    Ok(())
}
