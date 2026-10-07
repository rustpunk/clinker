//! `PlanNode::Transform` dispatch arm.
//!
//! Holds the record-level CXL projection / filter / lookup body lifted out
//! of [`crate::executor::dispatch::dispatch_plan_node`], including the
//! streaming-fused fast path that drives per-record evaluation directly off
//! a Source receiver and the materialized path that reads a `node_buffers`
//! slot. The dispatcher's `Transform` arm is a single delegating call into
//! [`dispatch_transform`].
//!
//! A Transform whose edge to its Sink the compiled plan certified as
//! streaming hands its rows to that Sink's writer thread in whichever arm
//! runs. The fused arm streams batches straight off its Source's channel.
//! The materialized arm sends its materialized rows through the same sender
//! and admits no node buffer; it is reached for such a Transform only in a
//! bounded preview, which runs the Transform apart from its Source so the
//! Sources drain in a fixed order.

use std::sync::Arc;

use clinker_record::Record;
use cxl::eval::{ProgramEvaluator, SkipReason};
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::batch_handoff::StreamingChargeHandle;
use crate::executor::dispatch::{
    ExecutorContext, NodeBufferKey, admit_node_buffer, advance_cursor,
    dispatch_transform_eval_error, finalize_node_rooted_windows, node_buffer_spill_allowed,
    require_node_buffer_input, source_file_arc_of, source_name_arc_of, stream_linear_producer_emit,
    tee_emit_to_region_input_buffers, transform_fused_consume,
};
use crate::executor::schema_check::check_input_schema;
use crate::executor::stream_event::{Punctuation, SourceRowId, StreamEvent};
use crate::executor::{
    WindowedEvalCtx, evaluate_single_transform, evaluate_single_transform_windowed,
};
use crate::log_dispatch::{LogDispatcher, TransformSignalContext};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Context carrier kept lazy until the node-kind guard has succeeded. Normal
/// dispatch passes the live executor context directly; the feature-gated
/// mismatch matrix uses the inert carrier to prove rejection precedes
/// evaluator and buffer access.
pub(crate) enum TransformDispatchContext<'borrow, 'plan> {
    Live(&'borrow mut ExecutorContext<'plan>),
    #[cfg(feature = "test-utils")]
    Inert,
}

impl<'borrow, 'plan> From<&'borrow mut ExecutorContext<'plan>>
    for TransformDispatchContext<'borrow, 'plan>
{
    fn from(ctx: &'borrow mut ExecutorContext<'plan>) -> Self {
        Self::Live(ctx)
    }
}

#[cfg(feature = "test-utils")]
impl crate::executor::dispatch::DispatchFaultGuard {
    /// Execute the real transform boundary with an inert context so tests can
    /// prove a wrong node returns before evaluator or buffer state is touched.
    #[doc(hidden)]
    pub fn dispatch_transform_mismatch_for_testing(
        current_dag: &ExecutionPlanDag,
        node_idx: NodeIndex,
        node: &PlanNode,
    ) -> Result<(), PipelineError> {
        dispatch_transform(TransformDispatchContext::Inert, current_dag, node_idx, node)
    }
}

/// The writer thread of a Sink whose edge from this Transform the compiled
/// plan certified as streaming, with the charge handle its batches cross.
struct CertifiedSinkHop {
    sender: crossbeam_channel::Sender<StreamEvent>,
    charge: StreamingChargeHandle,
}

impl CertifiedSinkHop {
    /// Send `rows`, then `puncts`, to the Sink's writer thread in batches of
    /// the Transform's batch size. Blocks on the writer's bounded channel;
    /// each batch is charged to the streaming slot until the writer drains
    /// it.
    fn send(
        &self,
        ctx: &ExecutorContext<'_>,
        name: &str,
        rows: Vec<(Record, SourceRowId)>,
        puncts: Vec<Punctuation>,
    ) -> Result<(), PipelineError> {
        stream_linear_producer_emit(
            &self.sender,
            ctx.batch_size_for(name),
            name,
            rows,
            puncts,
            &self.charge,
        )
    }
}

/// Take the streaming Sink hop installed for this Transform, if the compiled
/// plan certified one. Outside a bounded preview a certified Transform always
/// runs fused and takes its sender there, so the materialized arm finds one
/// only in a preview, and never inside a composition body.
///
/// A sender with no streaming charge consumer registered for it is an
/// executor invariant violation: both are installed together at executor
/// entry.
fn take_certified_sink_hop(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
) -> Result<Option<CertifiedSinkHop>, PipelineError> {
    let Some(sender) = ctx.take_streaming_sender(node_idx) else {
        return Ok(None);
    };
    let spill_allowed = node_buffer_spill_allowed(current_dag, node_idx);
    let charge = ctx
        .streaming_charge_handle(node_idx, name, spill_allowed)
        .ok_or_else(|| PipelineError::Internal {
            op: "executor",
            node: name.to_string(),
            detail: format!(
                "transform {name:?} holds its Sink's streaming sender but no streaming \
                 charge consumer is registered for it"
            ),
        })?;
    Ok(Some(CertifiedSinkHop { sender, charge }))
}

/// Execute the `Transform` arm for `node_idx`: drive per-record CXL
/// evaluation (filter, projection, distinct, emit_each fan-out) over the
/// predecessor's records, taking the streaming-fused path off a Source
/// receiver when the pre-pass flagged this Transform eligible and the
/// materialized path otherwise. Stateless.
///
/// When the compiled plan certified the Transform's edge to its Sink as
/// streaming, either arm hands its rows to the Sink's writer thread: the
/// fused arm batch by batch off the Source's channel, the materialized arm
/// (reached for such a Transform only in a bounded preview) by sending its
/// materialized rows, admitting no node buffer.
pub(crate) fn dispatch_transform<'borrow, 'plan>(
    ctx: impl Into<TransformDispatchContext<'borrow, 'plan>>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError>
where
    'plan: 'borrow,
{
    let PlanNode::Transform {
        ref name,
        window_index,
        ref resolved,
        has_distinct,
        ..
    } = *node
    else {
        return Err(crate::executor::invariant::dispatch_mismatch(
            "dispatch_transform",
            "transform",
            node.kind_name(),
            node.name(),
        ));
    };
    #[cfg(feature = "test-utils")]
    let TransformDispatchContext::Live(ctx) = ctx.into() else {
        panic!("transform dispatcher accessed inert context after accepting a transform node")
    };
    #[cfg(not(feature = "test-utils"))]
    let TransformDispatchContext::Live(ctx) = ctx.into();
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
    let predecessors: Vec<NodeIndex> = current_dag
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    let authored_input = ctx
        .current_body_node_input_refs
        .as_ref()
        .and_then(|refs| refs.get(name.as_str()))
        .and_then(|refs| refs.first())
        .map(String::as_str);
    let own_key = NodeBufferKey::from(node_idx);
    let (input_key, producer_name, producer_port): (NodeBufferKey, String, Option<String>) =
        if ctx.walk_reclaim.borrow().slots().contains_buffer(&own_key) {
            let (producer, port) = authored_input
                .and_then(|input| input.split_once('.'))
                .map_or(
                    (authored_input.unwrap_or("composition input"), None),
                    |(p, port)| (p, Some(port)),
                );
            (own_key, producer.to_string(), port.map(str::to_string))
        } else if let Some(&pred) = predecessors.first() {
            let port = current_dag
                .graph
                .find_edge(pred, node_idx)
                .and_then(|edge| current_dag.graph.edge_weight(edge))
                .and_then(|edge| edge.producer_port.as_deref());
            (
                NodeBufferKey::with_port(pred, port),
                current_dag.graph[pred].name().to_string(),
                port.map(str::to_string),
            )
        } else {
            (
                own_key,
                authored_input.unwrap_or("composition input").to_string(),
                None,
            )
        };
    let input_buffer = require_node_buffer_input(
        ctx,
        input_key,
        name,
        &producer_name,
        producer_port.as_deref(),
    )?;
    let (input_buffer, _input_reservation) =
        input_buffer.into_materialized_parts(&ctx.memory_budget, name)?;
    // The caller-explicit materialization reservation above covers the full
    // collected vector, including spill-backed reloads.
    let (input_records, input_puncts): (
        Vec<(Record, crate::executor::stream_event::SourceRowId)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = input_buffer.drain_split()?;

    // Read the typed program off the `PlanNode::Transform` payload. Every
    // lowered Transform carries a `Some(payload)` with a typechecked program;
    // an absent payload is the defensive pass-through (a node that failed to
    // lower has no program to run).
    let payload = match resolved {
        Some(p) => p,
        None => {
            // As in the fused arm's tail, a certified Transform tees to no
            // deferred region, so its streamed rows skip the tee.
            if let Some(hop) = take_certified_sink_hop(ctx, current_dag, node_idx, name)? {
                hop.send(ctx, name, input_records, input_puncts)?;
                return Ok(());
            }
            tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &input_records)?;
            admit_node_buffer(
                ctx,
                current_dag,
                name.as_str(),
                node_idx,
                input_records,
                input_puncts,
                node_buffer_spill_allowed(current_dag, node_idx),
            )?;
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
    // Inside a composition body `name` is body-local: two call sites of one
    // composition run the same names through the same telemetry producer, so
    // the exported identity is the call-site path, not the bare name.
    let logical_node = ctx.qualified_node_name(name).into_owned();
    // A commit-pass dispatch is one pass of a converge that may run this
    // transform again, so its signals belong to the converge rather than to the
    // pass. The state is taken out here and handed back below; the orchestrator
    // reports it once, after the loop stops.
    let signal_context = TransformSignalContext {
        execution_id: &ctx.stable.pipeline_execution_id,
        batch_id: &ctx.stable.pipeline_batch_id,
        pipeline_name: &ctx.stable.pipeline_name,
        logical_node: &logical_node,
    };
    let deferred_pass = ctx.in_deferred_dispatch;
    let mut signals = if deferred_pass {
        let carry = ctx.transform_signal_carry.take(&logical_node);
        LogDispatcher::deferred(
            ctx.telemetry_producer.clone(),
            &payload.log,
            &payload.log_conditions,
            signal_context,
            carry,
        )
    } else {
        LogDispatcher::new(
            ctx.telemetry_producer.clone(),
            &payload.log,
            &payload.log_conditions,
            signal_context,
        )
    };
    signals.fire_before_transform();

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
    // requires its WindowRuntime populated by the upstream operator's
    // `finalize_node_rooted_windows` call. Body transforms resolve only
    // through their compiled exact key; top-level transforms use their
    // top-level slot. Fail loudly if the runtime is missing —
    // the alternative (silently fall back to no-window eval)
    // corrupts `$window.*` results.
    let body_window_key = if let Some(idx_num) = window_index {
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
        let binding = if let Some(body_id) = ctx.window_runtime.active_stack.last().copied() {
            let body =
                ctx.composition_bodies
                    .get(&body_id)
                    .ok_or_else(|| PipelineError::Internal {
                        op: "executor",
                        node: name.clone(),
                        detail: format!(
                            "transform {name:?} executes in missing composition body {body_id:?}"
                        ),
                    })?;
            let binding = body.window_bindings.get(&node.id()).copied().ok_or_else(|| {
                PipelineError::Internal {
                    op: "executor",
                    node: name.clone(),
                    detail: format!(
                        "body transform {name:?} has window_index {idx_num} but no exact runtime binding"
                    ),
                }
            })?;
            if binding.index != idx_num {
                return Err(PipelineError::Internal {
                    op: "executor",
                    node: name.clone(),
                    detail: format!(
                        "body transform {name:?} binding points to slot {} but node declares {idx_num}",
                        binding.index
                    ),
                });
            }
            Some(binding.key)
        } else {
            None
        };
        let present = match binding {
            Some(key) => ctx.window_runtime.resolve_body(&key).is_some(),
            None => ctx.window_runtime.resolve_top(idx_num).is_some(),
        };
        if !present {
            return Err(PipelineError::Internal {
                op: "executor",
                node: name.clone(),
                detail: format!(
                    "transform {name:?} declares window_index {idx_num} \
                     but the runtime registry has no exact populated entry; \
                     upstream operator did not finalize its owning window root"
                ),
            });
        }
        binding
    } else {
        None
    };

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
        // Dispatch runs before the transform's program, so an authored gate
        // sees the record as it arrived — the input row the gate was
        // typechecked against.
        signals.fire_per_record(&record, &eval_ctx);

        let target_schema = output_schema
            .as_ref()
            .cloned()
            .unwrap_or_else(|| record.schema().clone());
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
                let runtime = match body_window_key {
                    Some(key) => ctx.window_runtime.resolve_body(&key),
                    None => ctx.window_runtime.resolve_top(idx_num),
                }
                .ok_or_else(|| PipelineError::Internal {
                    op: "executor",
                    node: name.clone(),
                    detail: format!(
                        "transform {name:?} window_index {idx_num} \
                                 resolves to no exact runtime at per-record dispatch; \
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
                signals.fire_on_error(&record);
                dispatch_transform_eval_error(ctx, record, rn, transform_name, eval_err)?;
            }
        }
    }

    // As in the fused arm's tail, a certified Transform roots no
    // node-anchored window and tees to no deferred region, so its rows go to
    // the Sink's writer thread in the order this loop produced them and skip
    // the window, tee and node-buffer steps below.
    if let Some(hop) = take_certified_sink_hop(ctx, current_dag, node_idx, name)? {
        hop.send(ctx, name, output_records, input_puncts)?;
        signals.finish();
        if let Some(carry) = signals.into_carry() {
            ctx.transform_signal_carry.park(logical_node, carry);
        }
        return Ok(());
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
    admit_node_buffer(
        ctx,
        current_dag,
        name,
        node_idx,
        output_records,
        input_puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    signals.finish();
    if let Some(carry) = signals.into_carry() {
        ctx.transform_signal_carry.park(logical_node, carry);
    }

    Ok(())
}
