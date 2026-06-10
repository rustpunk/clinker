//! `PlanNode::Aggregation` dispatch arm.
//!
//! Holds the hash / streaming GROUP BY executor body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`] — strategy selection,
//! per-record ingest, spill triggers, finalize, and the relaxed-CK
//! retraction handoff — together with the time-windowed aggregation
//! helpers it owns: window assignment, per-window add/error handling,
//! finalize, late-record DLQ, and the finalize-time DLQ emitter. The
//! dispatcher's `Aggregation` arm is a single delegating call into
//! [`dispatch_aggregation`].

use std::sync::Arc;

use clinker_record::{GroupByKey, Record, Schema, SchemaBuilder, Value};
use cxl::eval::ProgramEvaluator;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, RetainedAggregatorState, admit_node_buffer, advance_cursor,
    drain_node_buffer_slot, finalize_node_rooted_windows, node_buffer_spill_allowed,
    project_rows_to_buffer_schema, push_dlq, record_error_to_buffer_if_grouped, source_file_arc_of,
    source_name_arc_of, stream_linear_producer_emit, tee_emit_to_region_input_buffers,
};
use crate::executor::schema_check::check_input_schema;
use crate::executor::{DlqEntry, parse_memory_limit, stage_metrics};
use clinker_plan::config::ErrorStrategy;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};
use clinker_plan::plan::types::AggregateStrategy;

/// Execute the `Aggregation` arm for `node_idx`: select hash or streaming
/// strategy, ingest the predecessor's records (per-record for hash, sorted
/// for streaming), trip soft/hard spill thresholds inside the RSS budget,
/// finalize, and hand relaxed-CK aggregates to the retraction-mode commit
/// path. Blocking: accumulates group state before emitting any output row.
/// Time-windowed configs route through [`run_time_windowed_aggregate`].
pub(crate) fn dispatch_aggregation(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Aggregation {
        ref name,
        ref compiled,
        strategy: agg_strategy,
        ref output_schema,
        ref config,
        ..
    } = *node
    else {
        unreachable!("dispatch_aggregation called with non-Aggregation node");
    };
    // Whether this aggregate participates in retraction-mode
    // commit is fully determined by the planner-set
    // retraction-strategy flags on `compiled`: `requires_lineage`
    // for all-Reversible bindings, `requires_buffer_mode` for
    // any BufferRequired binding. Strict aggregates have both
    // flags `false` and bypass the retain-finalize-into-state
    // path entirely. Time-windowed aggregates always take the
    // strict path — relaxed-CK retraction over multi-window
    // emissions is not supported (see `apply_retraction_flags`).
    let is_time_windowed = config.time_window.is_some();
    let is_relaxed =
        !is_time_windowed && (compiled.requires_lineage || compiled.requires_buffer_mode);
    // Hash-aggregation dispatch arm.
    //
    // DataFusion PR #9241 / #12086 lesson: any
    // `PipelineError::Internal` raised here (e.g. via
    // `single_predecessor`, or via the wrapper enum's
    // Streaming-not-yet-implemented arm) MUST hard-abort
    // regardless of `error_strategy`. We achieve this
    // structurally by propagating those errors via `?`
    // before reaching the per-record FailFast/Continue
    // match — internal invariants always abort.
    use crate::aggregation::{HashAggError, SortRow as AggSortRow};

    let pred = clinker_plan::plan::execution::single_predecessor(
        current_dag,
        node_idx,
        "aggregation",
        name,
    )?;

    // Streaming-ingest fast path (issue #299): when `pred` is the certified
    // streaming producer for this Aggregate (top-level plan only — a
    // composition body shares the `NodeIndex` space but installs no ingest
    // edges), drive `add_record` off a bounded channel the producer fills
    // rather than pre-draining its whole output from a charged
    // `node_buffers` slot. Strict aggregates only: relaxed-CK and
    // time-windowed shapes were excluded by `certify_streaming_edge`, so
    // this never collides with the retain / multi-window branches below.
    let streaming_ingest_producer: Option<NodeIndex> = if ctx.current_body_node_input_refs.is_none()
        && !is_relaxed
        && !is_time_windowed
        && ctx
            .streaming_aggregate_ingest_edges
            .get(&pred)
            .is_some_and(|&agg| agg == node_idx)
    {
        Some(pred)
    } else {
        None
    };

    let agg_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);

    if let Some(producer_idx) = streaming_ingest_producer {
        let ingest = run_streaming_aggregate_ingest(
            ctx,
            current_dag,
            node_idx,
            producer_idx,
            name,
            agg_strategy,
            compiled,
            output_schema,
        )?;
        return finalize_aggregate_emit(
            ctx,
            current_dag,
            node_idx,
            name,
            agg_timer,
            ingest.input_count,
            ingest.out_rows,
            ingest.puncts,
        );
    }

    let (input, input_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = match drain_node_buffer_slot(ctx, pred) {
        Some(nb) => nb.drain_split()?,
        None => (Vec::new(), Vec::new()),
    };

    if let Some(expected) = current_dag.graph[node_idx]
        .expected_input_schema_in(current_dag)
        .cloned()
    {
        let upstream_name = current_dag.graph[pred].name().to_string();
        for (record, _) in &input {
            check_input_schema(
                &expected,
                record.schema(),
                name,
                "aggregation",
                &upstream_name,
            )?;
        }
    }

    // Build the per-aggregation runtime artifacts. The
    // executor owns the evaluator + spill metadata; the
    // wrapper enum owns the engine.
    let transform_idx = ctx.transform_by_name.get(name.as_str()).copied();
    let evaluator = match transform_idx {
        Some(idx) => ProgramEvaluator::with_max_expansion(
            Arc::clone(&ctx.compiled_transforms[idx].typed),
            ctx.compiled_transforms[idx].has_distinct(),
            ctx.compiled_transforms[idx].max_expansion,
        ),
        None => {
            return Err(PipelineError::Internal {
                op: "aggregation",
                node: name.clone(),
                detail: "no compiled transform found for aggregate node".to_string(),
            });
        }
    };

    // Spill schema follows the format used by
    // `HashAggregator::spill`: group-by columns ++
    // `__acc_state` ++ `__meta_tracker`.
    let spill_schema = compiled
        .group_by_fields
        .iter()
        .map(|s| Box::<str>::from(s.as_str()))
        .chain([
            Box::<str>::from("__acc_state"),
            Box::<str>::from("__meta_tracker"),
        ])
        .collect::<SchemaBuilder>()
        .build();

    let mem_limit = parse_memory_limit(ctx.config);

    // Resolve the spill compression mode against this aggregate's
    // output-schema width and the run's batch size, so spilled group state
    // matches what `--explain` projects for the operator. The explain line
    // for an Aggregation node resolves the same way against
    // `stored_output_schema().column_count()`, so resolving here against the
    // output schema keeps the reported mode and the on-disk format in
    // lockstep. `auto` skips LZ4 on narrow/short aggregates where the
    // per-frame fixed cost outweighs the savings.
    let spill_compress = ctx
        .spill_compress
        .resolve_for_schema(output_schema.column_count(), ctx.batch_size as u64);

    let input_count = input.len() as u64;

    let out_rows: Vec<AggSortRow> = if is_time_windowed {
        // Time-windowed path: per-(window) AggregateStream
        // instances dispatched in `run_time_windowed_aggregate`.
        // The single positional `stream` built above is unused
        // here — drop it so its spill files (if any) are cleaned
        // up promptly. Building a fresh evaluator + spill setup
        // per window keeps the time-windowed code self-
        // contained at the cost of one extra evaluator clone per
        // window (the heavy `TypedProgram` lives behind an Arc).
        drop(evaluator);
        let win_ctx = WindowedAggContext {
            name,
            compiled,
            strategy: agg_strategy,
            output_schema: Arc::clone(output_schema),
            spill_schema,
            mem_limit,
            transform_idx,
        };
        run_time_windowed_aggregate(
            ctx,
            current_dag,
            node_idx,
            &win_ctx,
            &input,
            config
                .time_window
                .as_ref()
                .expect("guarded by is_time_windowed"),
            config.allowed_lateness,
        )?
    } else {
        // Single ConsumerHandle shared between the
        // AggregateConsumer wrapper that the arbitrator's
        // policy registry holds and the HashAggregator that
        // mirrors its value_heap_bytes total into the
        // handle's counter on every admit / spill / reset.
        //
        // The returned `ConsumerId` is captured so the wrapper is
        // unregistered the moment its aggregator's state stops
        // being live. A strict aggregate's `finalize` consumes
        // the stream and emits every group, so the arm exit
        // unregisters it directly; without that the finalized
        // group state keeps contributing to `sum_consumer_usage`
        // for the rest of the run, inflating the reported peak
        // even though the bytes are gone. A relaxed aggregate
        // parks its aggregator on `relaxed_aggregator_states` and
        // carries this id with it, because the commit phase keeps
        // retracting and re-finalizing that instance — its bytes
        // stay live until the aggregator is dropped, at which
        // point the unregister fires from that state.
        let agg_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
        let agg_consumer_id = ctx.memory_budget.register_consumer(Arc::new(
            crate::aggregation::AggregateConsumer::new(agg_consumer_handle.clone()),
        ));
        let mut stream = crate::aggregation::AggregateStream::for_node(
            agg_strategy,
            crate::aggregation::AggregatorConfig {
                compiled: Arc::clone(compiled),
                evaluator,
                output_schema: Arc::clone(output_schema),
                spill_schema,
                memory_budget: mem_limit,
                spill_dir: Some(ctx.spill_root_path.to_path_buf()),
                spill_compress,
                transform_name: name.clone(),
                consumer_handle: agg_consumer_handle,
            },
        )?;

        // Per-record accumulator updates + spill I/O. The loop
        // threads `&mut ctx` (cursor advance, DLQ routing) per
        // record, so it stays on the dispatch thread rather than
        // crossing into the Rayon pool.
        let mut emitted_rows: Vec<AggSortRow> = Vec::with_capacity(64);
        (|| -> Result<(), PipelineError> {
            for (record, row_num) in &input {
                let source_file_arc = source_file_arc_of(record);
                let source_name_arc = source_name_arc_of(record);
                let eval_ctx = ctx.eval_ctx_for_record(
                    &source_file_arc,
                    &source_name_arc,
                    *row_num,
                    record.doc_ctx(),
                );
                let add_result = stream.add_record(record, *row_num, &eval_ctx, &mut emitted_rows);
                if add_result.is_ok() {
                    advance_cursor(ctx, &source_name_arc, *row_num);
                }
                if let Err(e) = add_result {
                    match ctx.config.error_handling.strategy {
                        ErrorStrategy::FailFast => return Err(e.into()),
                        ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                            let stage = Some(clinker_core_types::dlq::stage_aggregate(name));
                            let routed = record_error_to_buffer_if_grouped(
                                ctx,
                                record,
                                *row_num,
                                clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
                                format!("aggregate {name}: {e}"),
                                stage.clone(),
                                None,
                            );
                            if !routed {
                                let source_name = source_name_arc_of(record);
                                push_dlq(
                                    ctx,
                                    DlqEntry {
                                        source_row: *row_num,
                                        category: clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
                                        error_message: format!("aggregate {name}: {e}"),
                                        original_record: record.clone(),
                                        stage,
                                        route: None,
                                        trigger: true,
                                        source_name,
                                        triggering_field: None,
                                        triggering_value: None,
                                    },
                                )?;
                            }
                        }
                    }
                }
            }
            Ok(())
        })()?;

        // Finalize. Accumulator finalize errors get the
        // typed `PipelineError::Accumulator` mapping; under
        // `Continue` we route to the DLQ and emit zero rows
        // for the failed group. All other engine errors
        // propagate (Internal/Spill/Residual always abort).
        //
        // Relaxed-CK aggregates split the finalize path: instead
        // of consuming the wrapper, the Hash-arm boxed aggregator
        // is extracted and kept on `ExecutorContext.relaxed_aggregator_states`
        // so the correlation-commit orchestrator can call
        // `retract_row` + `finalize_in_place` against the same
        // instance that produced these rows. Strict aggregates
        // continue to consume-and-discard so non-relaxed
        // pipelines pay zero overhead.
        let finalize_ctx = ctx.merged_eval_ctx();
        let agg_out = if is_relaxed {
            let mut hash_box = match stream.into_retained_hash() {
                Some(b) => b,
                None => {
                    // Streaming + retraction-mode is rejected at
                    // compile time (E15Y); reaching this branch is
                    // a planner-pass bug.
                    return Err(PipelineError::Internal {
                        op: "aggregation",
                        node: name.clone(),
                        detail: "retraction-mode aggregate produced a non-Hash \
                                 stream — E15Y should have rejected this at compile time"
                            .to_string(),
                    });
                }
            };
            let emits_synthetic = hash_box.emits_synthetic_ck();
            let pre_finalize_len = emitted_rows.len();
            match hash_box.finalize_in_place(&finalize_ctx, &mut emitted_rows) {
                Ok(()) => {
                    if emits_synthetic {
                        ctx.counters.retraction.synthetic_ck_columns_emitted_total +=
                            (emitted_rows.len() - pre_finalize_len) as u64;
                    }
                    ctx.relaxed_aggregator_states.insert(
                        node_idx,
                        RetainedAggregatorState {
                            aggregator: hash_box,
                            consumer_id: agg_consumer_id,
                        },
                    );
                    emitted_rows
                }
                Err(HashAggError::Accumulator {
                    transform,
                    binding,
                    source,
                }) => match ctx.config.error_handling.strategy {
                    ErrorStrategy::FailFast => {
                        return Err(PipelineError::Accumulator {
                            transform,
                            binding,
                            source,
                        });
                    }
                    ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                        // Empty-input finalize failures have no real
                        // record to attribute to a Source, so stamp
                        // the aggregate node's own name as the
                        // source label. The DLQ reader sees a
                        // specific aggregate identifier instead of
                        // the generic `<merged>` fallthrough
                        // `source_name_arc_of` would yield for a
                        // schema-only synthetic record.
                        let (synthetic, source_name) = if let Some((rec, _)) = input.first() {
                            let sn = source_name_arc_of(rec);
                            (rec.clone(), sn)
                        } else {
                            (
                                Record::new(Arc::clone(output_schema), Vec::new()),
                                Arc::from(name.as_str()),
                            )
                        };
                        push_dlq(
                            ctx,
                            DlqEntry {
                                source_row: 0,
                                category:
                                    clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
                                error_message: format!(
                                    "aggregate {transform}.{binding}: {source:?}"
                                ),
                                original_record: synthetic,
                                stage: Some(clinker_core_types::dlq::stage_aggregate(name)),
                                route: None,
                                trigger: true,
                                source_name,
                                triggering_field: None,
                                triggering_value: None,
                            },
                        )?;
                        Vec::new()
                    }
                },
                Err(other) => return Err(other.into()),
            }
        } else {
            match stream.finalize(&finalize_ctx, &mut emitted_rows) {
                Ok(()) => emitted_rows,
                Err(HashAggError::Accumulator {
                    transform,
                    binding,
                    source,
                }) => match ctx.config.error_handling.strategy {
                    ErrorStrategy::FailFast => {
                        return Err(PipelineError::Accumulator {
                            transform,
                            binding,
                            source,
                        });
                    }
                    ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                        // Empty-input finalize failures have no real
                        // record to attribute to a Source, so stamp
                        // the aggregate node's own name as the
                        // source label. The DLQ reader sees a
                        // specific aggregate identifier instead of
                        // the generic `<merged>` fallthrough
                        // `source_name_arc_of` would yield for a
                        // schema-only synthetic record.
                        let (synthetic, source_name) = if let Some((rec, _)) = input.first() {
                            let sn = source_name_arc_of(rec);
                            (rec.clone(), sn)
                        } else {
                            (
                                Record::new(Arc::clone(output_schema), Vec::new()),
                                Arc::from(name.as_str()),
                            )
                        };
                        push_dlq(
                            ctx,
                            DlqEntry {
                                source_row: 0,
                                category:
                                    clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
                                error_message: format!(
                                    "aggregate {transform}.{binding}: {source:?}"
                                ),
                                original_record: synthetic,
                                stage: Some(clinker_core_types::dlq::stage_aggregate(name)),
                                route: None,
                                trigger: true,
                                source_name,
                                triggering_field: None,
                                triggering_value: None,
                            },
                        )?;
                        Vec::new()
                    }
                },
                Err(other) => return Err(other.into()),
            }
        };
        // The aggregate's working set is live exactly as long as
        // its aggregator is. A strict `finalize` above consumed
        // the stream and emitted every group, so that state is
        // now gone; unregister the wrapper here so the finalized
        // bytes leave `sum_consumer_usage` immediately and the
        // run peak reflects only concurrently-live state, making
        // it sensitive to the order aggregates complete in.
        //
        // A relaxed aggregate that finalized successfully parked
        // its boxed aggregator on `relaxed_aggregator_states`
        // (the key is present): that state stays live across the
        // commit phase's retract + re-finalize iterations, so its
        // wrapper must stay registered. Ownership of the
        // unregister transfers to `RetainedAggregatorState`,
        // which fires it when the aggregator is dropped — at the
        // commit-phase degrade `remove`, or at the top-scope
        // teardown that drains the surviving states once the walk
        // ends. A relaxed aggregate whose finalize failed and
        // routed to the DLQ dropped its aggregator without
        // parking it, so the key is absent and the wrapper is
        // unregistered here alongside the strict case — the
        // presence of the retained key, not `is_relaxed`, is the
        // exact predicate for "the state outlives this arm".
        if !ctx.relaxed_aggregator_states.contains_key(&node_idx) {
            ctx.memory_budget.unregister_consumer(agg_consumer_id);
        }
        agg_out
    };

    finalize_aggregate_emit(
        ctx,
        current_dag,
        node_idx,
        name,
        agg_timer,
        input_count,
        out_rows,
        input_puncts,
    )
}

/// Emit a strict aggregate's finalized rows: record the stage timer, then
/// route `out_rows` to the deferred-region slot, the streaming-Output
/// handoff, or a charged `node_buffers` slot, forwarding `input_puncts` at
/// the output tail. Shared by the materialized ingest path and the
/// streaming-ingest path ([`run_streaming_aggregate_ingest`]) — both
/// produce the same finalized `out_rows`, so the emit half is identical.
#[allow(clippy::too_many_arguments)]
fn finalize_aggregate_emit(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
    agg_timer: stage_metrics::StageTimer,
    input_count: u64,
    out_rows: Vec<crate::aggregation::SortRow>,
    input_puncts: Vec<crate::executor::stream_event::Punctuation>,
) -> Result<(), PipelineError> {
    ctx.collector
        .record(agg_timer.finish(input_count, out_rows.len() as u64));
    if let Some(region) = current_dag.deferred_region_at_producer(node_idx) {
        // Deferred-region producer. Project emits to the region's
        // buffer schema (the planner already pruned this to the
        // minimum columns the deferred operators reach via
        // `Expr::support_into` plus every windowed-Transform
        // member's `arena_fields`). Park narrow rows in
        // `node_buffers[node_idx]` and build any node-rooted
        // window runtimes against the same narrow projection so
        // a windowed-Transform member finds its slot populated
        // when the commit-time deferred dispatcher walks it —
        // the forward pass produces these emits exactly once and
        // every downstream member runs on the commit pass, so
        // pre-building here matches the strict path's window
        // lifecycle (forward-pass build, downstream consumption)
        // without an extra commit-time materialization for the
        // no-retraction case. Retraction iterations overwrite
        // the slot in `recompute_aggregates::emit_post_recompute`.
        // Aggregate forwards inbound punctuations to its
        // output buffer (Preserving). Document-scoped flush
        // semantics — "flush groups on `DocumentClose` before
        // forwarding the punctuation" — land as a follow-up
        // commit within this sprint; today the punctuation
        // travels at the tail of the aggregated output, which
        // matches the streaming contract for single-document
        // pipelines but does not yet split per-document
        // groups when multiple documents enter the same
        // aggregate.
        let buffer_schema = region.buffer_schema.clone();
        let projected = project_rows_to_buffer_schema(out_rows, &buffer_schema);
        finalize_node_rooted_windows(ctx, current_dag, node_idx, &projected)?;
        tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &projected)?;
        let nb = admit_node_buffer(
            ctx,
            name,
            node_idx,
            projected,
            input_puncts,
            node_buffer_spill_allowed(current_dag, node_idx),
        )?;
        ctx.node_buffers.insert(node_idx, nb);
    } else {
        // Streaming-Output handoff: a streaming-strategy
        // aggregate (the planner certified pre-sorted input)
        // whose sole downstream is a streaming-eligible Output
        // installed its sender under our `node_idx`. The finalized
        // group rows are already collected into `out_rows`, so
        // this does not shrink the aggregate's own working set;
        // what it saves is the second copy — hand `out_rows`
        // straight to the writer thread over the bounded channel
        // rather than admitting a charged `node_buffers` slot the
        // Output would re-drain, and overlap the writer with the
        // next topo node. Retraction-mode and time-windowed
        // aggregates take other branches above and never reach
        // this streaming handoff; the eligibility predicate
        // certified this aggregate roots no window and is not a
        // deferred-region producer, so the helper calls below are
        // correctly skipped. Dropping the sender disconnects the
        // writer's recv loop.
        if let Some(sender) = ctx.take_streaming_sender(node_idx) {
            let batch_size = ctx.batch_size;
            let spill_allowed = node_buffer_spill_allowed(current_dag, node_idx);
            let charge = ctx
                .streaming_charge_handle(node_idx, name, spill_allowed)
                .expect("streaming sender implies a registered charge consumer");
            stream_linear_producer_emit(
                &sender,
                batch_size,
                name,
                out_rows,
                input_puncts,
                &charge,
            )?;
            return Ok(());
        }

        // Materialize node-rooted window runtimes for any IndexSpec
        // rooted at this aggregate. The aggregate emits columns the
        // source arena cannot project (e.g. `total = sum(amount)`,
        // `$ck.aggregate.<name>`); a downstream window's IndexSpec
        // pins its `arena_fields` against the aggregate's
        // `output_schema`, so the arena materializes from
        // `out_rows` here, not from the source stream.
        finalize_node_rooted_windows(ctx, current_dag, node_idx, &out_rows)?;
        tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &out_rows)?;
        let nb = admit_node_buffer(
            ctx,
            name,
            node_idx,
            out_rows,
            input_puncts,
            node_buffer_spill_allowed(current_dag, node_idx),
        )?;
        ctx.node_buffers.insert(node_idx, nb);
    }

    Ok(())
}

/// Per-record side effects a streaming-ingest scoped thread cannot apply
/// directly (it holds no `&mut ExecutorContext`), replayed onto the
/// dispatcher after the scope joins. Ordering within each vector is the
/// channel arrival order, so the replay reproduces the materialized
/// path's cursor / DLQ sequencing exactly.
struct StreamingIngestEffects {
    /// `(source_name, row_num)` for each successfully ingested record —
    /// replayed via [`advance_cursor`] so rollback cursors and watermark
    /// liveness match the drain-to-`Vec` path.
    cursor_advances: Vec<(Arc<str>, u64)>,
    /// `(record, row_num, error_message)` for each `add_record` failure
    /// under `Continue` / `BestEffort`, replayed via the dispatcher's DLQ
    /// routing after join. `FailFast` surfaces the error eagerly instead.
    add_errors: Vec<(Record, u64, String)>,
}

/// Streaming-ingest result handed back to [`dispatch_aggregation`]: the
/// finalized group rows, the document-boundary punctuations forwarded at
/// the output tail, and the count of records the producer fed in.
struct StreamingIngestOutput {
    out_rows: Vec<crate::aggregation::SortRow>,
    puncts: Vec<crate::executor::stream_event::Punctuation>,
    input_count: u64,
}

/// Drive a strict (non-relaxed, non-time-windowed) Aggregate's ingest off
/// a bounded channel the producer fills, instead of pre-draining the
/// producer's whole output from a charged `node_buffers` slot.
///
/// Streaming, not blocking on its ingest half: the producer runs inside
/// this call on the dispatch thread (its own topo turn was short-circuited
/// by [`crate::executor::dispatch::dispatch_plan_node`]), streaming each
/// batch into the channel, while a scoped thread drives `add_record` off
/// the receiver. The bounded `send` is the back-pressure pivot — a slow
/// `add_record` (e.g. a hash aggregate spilling) stalls the producer and
/// lets the upstream Source channel fill. The finalize half stays blocking:
/// the scoped thread runs `finalize` once the channel disconnects (every
/// sender dropped at the producer arm's clean exit), then returns the
/// finalized rows. Document-boundary punctuations are collected and
/// forwarded at the output tail — byte-identical to the materialized hash
/// path, whose per-document group flush is a separate follow-up.
///
/// The scoped thread holds no `&mut ExecutorContext`; it accumulates cursor
/// advances and `add_record` DLQ errors into [`StreamingIngestEffects`],
/// replayed on the dispatch thread after the scope joins so rollback
/// cursors, watermark liveness, and DLQ sequencing match the drain-to-`Vec`
/// path exactly. Spill stays RSS-triggered inside `add_record` /
/// `finalize`, never channel-depth-triggered.
#[allow(clippy::too_many_arguments)]
fn run_streaming_aggregate_ingest(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    producer_idx: NodeIndex,
    name: &str,
    agg_strategy: AggregateStrategy,
    compiled: &Arc<cxl::plan::CompiledAggregate>,
    output_schema: &Arc<Schema>,
) -> Result<StreamingIngestOutput, PipelineError> {
    use crate::aggregation::SortRow as AggSortRow;
    use crate::executor::stream_event::StreamEvent;
    use cxl::eval::EvalContext;

    let expected_input = current_dag.graph[node_idx]
        .expected_input_schema_in(current_dag)
        .cloned();
    let upstream_name = current_dag.graph[producer_idx].name().to_string();

    // Build the per-aggregation runtime artifacts (mirrors the materialized
    // path): the executor owns the evaluator + spill metadata; the wrapper
    // enum owns the engine. Register exactly one `AggregateConsumer` so the
    // growing group state contributes to `sum_consumer_usage` while it is
    // live, and unregister it after `finalize` consumes the stream.
    let transform_idx = ctx.transform_by_name.get(name).copied();
    let evaluator = match transform_idx {
        Some(idx) => ProgramEvaluator::with_max_expansion(
            Arc::clone(&ctx.compiled_transforms[idx].typed),
            ctx.compiled_transforms[idx].has_distinct(),
            ctx.compiled_transforms[idx].max_expansion,
        ),
        None => {
            return Err(PipelineError::Internal {
                op: "aggregation",
                node: name.to_string(),
                detail: "no compiled transform found for aggregate node".to_string(),
            });
        }
    };
    let spill_schema = compiled
        .group_by_fields
        .iter()
        .map(|s| Box::<str>::from(s.as_str()))
        .chain([
            Box::<str>::from("__acc_state"),
            Box::<str>::from("__meta_tracker"),
        ])
        .collect::<SchemaBuilder>()
        .build();
    let mem_limit = parse_memory_limit(ctx.config);
    let spill_compress = ctx
        .spill_compress
        .resolve_for_schema(output_schema.column_count(), ctx.batch_size as u64);

    let agg_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
    let agg_consumer_id =
        ctx.memory_budget
            .register_consumer(Arc::new(crate::aggregation::AggregateConsumer::new(
                agg_consumer_handle.clone(),
            )));
    let mut stream = crate::aggregation::AggregateStream::for_node(
        agg_strategy,
        crate::aggregation::AggregatorConfig {
            compiled: Arc::clone(compiled),
            evaluator,
            output_schema: Arc::clone(output_schema),
            spill_schema,
            memory_budget: mem_limit,
            spill_dir: Some(ctx.spill_root_path.to_path_buf()),
            spill_compress,
            transform_name: name.to_string(),
            consumer_handle: agg_consumer_handle,
        },
    )?;

    // Install the producer's streaming sender + per-batch charge consumer
    // keyed by the producer's index, so its dispatch arm streams into our
    // channel with no producer-side change (the same `take_streaming_sender`
    // path the streaming-Output producers use). The charge handle's
    // per-batch `add_bytes` on flush is netted to zero by the scoped
    // thread's per-record `sub_bytes` discharge below; a 256-event bound
    // mirrors the Source ingest channel so back-pressure paces both ends.
    let (tx, rx) = crossbeam_channel::bounded::<StreamEvent>(256);
    let charge_handle = crate::pipeline::memory::ConsumerHandle::new();
    let charge_consumer_id = ctx.memory_budget.register_consumer(Arc::new(
        crate::executor::node_buffer::NodeBufferConsumer::new(charge_handle.clone(), false),
    ));
    ctx.streaming_output_senders.insert(producer_idx, tx);
    ctx.streaming_charge_consumers
        .insert(producer_idx, (charge_consumer_id, charge_handle.clone()));

    // Copy the stable-context reference out of `ctx` *before* the scope so
    // the scoped thread borrows `&'a StableEvalContext` directly (shared,
    // `Sync`) rather than through the `&mut ctx` the main thread holds for
    // the producer redispatch — the two borrows are disjoint.
    let stable = ctx.stable;
    let source_batch_arc = ctx.source_batch_arc;
    let ingestion_timestamp = ctx.source_ingestion_timestamp;
    let strategy = ctx.config.error_handling.strategy;

    let mut effects = StreamingIngestEffects {
        cursor_advances: Vec::new(),
        add_errors: Vec::new(),
    };
    let mut out_rows: Vec<AggSortRow> = Vec::with_capacity(64);
    let mut puncts: Vec<crate::executor::stream_event::Punctuation> = Vec::new();
    let mut input_count: u64 = 0;

    // Run the ingest recv loop and the producer concurrently. The scoped
    // thread owns the `AggregateStream` and drains the channel; the main
    // thread redispatches the producer (which streams into the channel and
    // drops its sender at clean exit, disconnecting the channel). The
    // producer's `?` error and the ingest thread's error both surface; the
    // ingest thread always drains to disconnect first so a producer `send`
    // can never deadlock on a dead consumer.
    let finalize_ctx = ctx.merged_eval_ctx();
    let ingest_result: Result<(), PipelineError> = std::thread::scope(|scope| {
        let handle = scope.spawn(|| -> Result<(), PipelineError> {
            while let Ok(event) = rx.recv() {
                let (record, rn) = match event {
                    StreamEvent::Record(r, rn) => (r, rn),
                    StreamEvent::Punctuation(p) => {
                        // Collect for the output tail (byte-identical to the
                        // materialized hash path; per-document flush is a
                        // separate follow-up). Punctuations carry zero
                        // charge, so no discharge here.
                        puncts.push(p);
                        continue;
                    }
                };
                // Discharge this record's per-row cost — the consume half of
                // the producer's per-batch admit. The formula matches the
                // producer's charge so a fully-drained stream nets to zero.
                charge_handle.sub_bytes(crate::executor::node_buffer::record_byte_cost(
                    record.schema().column_count(),
                ));
                input_count += 1;
                if let Some(exp) = expected_input.as_ref() {
                    check_input_schema(exp, record.schema(), name, "aggregation", &upstream_name)?;
                }
                let source_file_arc = source_file_arc_of(&record);
                let source_name_arc = source_name_arc_of(&record);
                // Mid-stream `$source.count` is `None` (defer-emit) — the
                // total is unknown until the source disconnects, exactly as
                // the per-record eval sites resolve it.
                let eval_ctx = EvalContext {
                    stable,
                    source_file: &source_file_arc,
                    source_row: rn,
                    source_path: &source_file_arc,
                    source_count: None,
                    source_batch: source_batch_arc,
                    ingestion_timestamp,
                    source_name: &source_name_arc,
                    doc_ctx: record.doc_ctx(),
                };
                match stream.add_record(&record, rn, &eval_ctx, &mut out_rows) {
                    Ok(()) => effects.cursor_advances.push((source_name_arc, rn)),
                    Err(e) => match strategy {
                        ErrorStrategy::FailFast => {
                            // Drain the rest so the producer's bounded `send`
                            // can't deadlock, then surface the error.
                            while rx.recv().is_ok() {}
                            return Err(e.into());
                        }
                        ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                            effects
                                .add_errors
                                .push((record, rn, format!("aggregate {name}: {e}")));
                        }
                    },
                }
            }
            // Channel disconnected — every sender dropped at the producer
            // arm's clean exit. Finalize the blocking half and return.
            stream
                .finalize(&finalize_ctx, &mut out_rows)
                .map_err(PipelineError::from)
        });

        // Redispatch the producer on the main thread. Clear its ingest-edge
        // entry first so the dispatcher's streaming-ingest short-circuit
        // (which made the producer's own topo turn a no-op) does not fire
        // again here — this is the one turn the producer must actually run.
        // It takes the sender we installed and streams into the channel,
        // dropping it at clean exit.
        ctx.streaming_aggregate_ingest_edges.remove(&producer_idx);
        let producer_result =
            crate::executor::dispatch::dispatch_plan_node(ctx, current_dag, producer_idx);
        // Belt-and-suspenders: ensure the channel disconnects even if a
        // producer error left a sender lingering on `ctx`, so the ingest
        // thread's `recv` returns `Err` and the join below cannot hang.
        ctx.streaming_output_senders.remove(&producer_idx);

        let ingest = handle.join().map_err(|_| PipelineError::Internal {
            op: "aggregation",
            node: name.to_string(),
            detail: "streaming aggregate ingest thread panicked".to_string(),
        })?;
        producer_result.and(ingest)
    });

    // Charge bookkeeping is complete: the producer charged each batch and
    // the ingest thread discharged each record. Pin to zero defensively (a
    // heuristic mismatch between batch charge and per-record discharge must
    // not leave a stale positive for the arbitrator), then unregister both
    // the per-edge charge consumer and the aggregate consumer — `finalize`
    // consumed the group state, so its bytes leave `sum_consumer_usage`.
    charge_handle.set_bytes(0);
    ctx.streaming_charge_consumers.remove(&producer_idx);
    ctx.memory_budget.unregister_consumer(charge_consumer_id);
    ctx.memory_budget.unregister_consumer(agg_consumer_id);

    ingest_result?;

    // Replay the per-record side effects the scoped thread accumulated. The
    // cursor advances keep rollback / watermark state consistent; the DLQ
    // errors route through the same `Continue` / `BestEffort` path the
    // materialized ingest loop uses.
    for (source_name_arc, rn) in std::mem::take(&mut effects.cursor_advances) {
        advance_cursor(ctx, &source_name_arc, rn);
    }
    for (record, rn, message) in std::mem::take(&mut effects.add_errors) {
        let stage = Some(clinker_core_types::dlq::stage_aggregate(name));
        let routed = record_error_to_buffer_if_grouped(
            ctx,
            &record,
            rn,
            clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
            message.clone(),
            stage.clone(),
            None,
        );
        if !routed {
            let source_name = source_name_arc_of(&record);
            push_dlq(
                ctx,
                DlqEntry {
                    source_row: rn,
                    category: clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
                    error_message: message,
                    original_record: record,
                    stage,
                    route: None,
                    trigger: true,
                    source_name,
                    triggering_field: None,
                    triggering_value: None,
                },
            )?;
        }
    }

    Ok(StreamingIngestOutput {
        out_rows,
        puncts,
        input_count,
    })
}

/// Immutable build configuration for a time-windowed aggregate, shared
/// by [`run_time_windowed_aggregate`] and its per-record [`add_to_window`]
/// helper. Bundling the seven aggregate-build fields keeps both functions
/// at their argument budget and gives the per-window stream factory one
/// home — [`Self::make_stream`] replaces the closure both call sites
/// previously threaded by reference.
struct WindowedAggContext<'a> {
    name: &'a str,
    compiled: &'a Arc<cxl::plan::CompiledAggregate>,
    strategy: AggregateStrategy,
    output_schema: Arc<Schema>,
    spill_schema: Arc<Schema>,
    mem_limit: usize,
    transform_idx: Option<usize>,
}

impl WindowedAggContext<'_> {
    /// Build a fresh per-window [`crate::aggregation::AggregateStream`]
    /// sharing this aggregate's compiled CXL, output schema, and spill
    /// schema, and register its consumer with the pipeline-scoped
    /// arbitrator. Returns the `ConsumerId` alongside the stream so the
    /// caller can unregister the wrapper once the window finalizes —
    /// leaving it registered would keep every finalized window's bytes in
    /// `sum_consumer_usage` for the rest of the run. Building a new
    /// `ProgramEvaluator` per window is cheap: the heavy CXL pipeline
    /// lives behind `Arc<TypedProgram>`.
    fn make_stream(
        &self,
        ctx: &ExecutorContext<'_>,
    ) -> Result<
        (
            crate::aggregation::AggregateStream,
            crate::pipeline::memory::ConsumerId,
        ),
        PipelineError,
    > {
        let evaluator = match self.transform_idx {
            Some(idx) => ProgramEvaluator::with_max_expansion(
                Arc::clone(&ctx.compiled_transforms[idx].typed),
                ctx.compiled_transforms[idx].has_distinct(),
                ctx.compiled_transforms[idx].max_expansion,
            ),
            None => {
                return Err(PipelineError::Internal {
                    op: "time-windowed-aggregation",
                    node: self.name.to_string(),
                    detail: "no compiled transform found for time-windowed aggregate node"
                        .to_string(),
                });
            }
        };
        let agg_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
        let agg_consumer_id = ctx.memory_budget.register_consumer(Arc::new(
            crate::aggregation::AggregateConsumer::new(agg_consumer_handle.clone()),
        ));
        // Resolve the spill compression mode against this aggregate's
        // output-schema width and the run's batch size, matching the
        // `--explain` projection for the Aggregation node so each window's
        // spill file's on-disk format agrees with the reported mode.
        let spill_compress = ctx
            .spill_compress
            .resolve_for_schema(self.output_schema.column_count(), ctx.batch_size as u64);
        let stream = crate::aggregation::AggregateStream::for_node(
            self.strategy,
            crate::aggregation::AggregatorConfig {
                compiled: Arc::clone(self.compiled),
                evaluator,
                output_schema: Arc::clone(&self.output_schema),
                spill_schema: Arc::clone(&self.spill_schema),
                memory_budget: self.mem_limit,
                spill_dir: Some(ctx.spill_root_path.to_path_buf()),
                spill_compress,
                transform_name: self.name.to_string(),
                consumer_handle: agg_consumer_handle,
            },
        )?;
        Ok((stream, agg_consumer_id))
    }
}

/// Dispatch arm for time-windowed aggregates. Branches the existing
/// strict `PlanNode::Aggregation` flow when `AggregateConfig.time_window`
/// is `Some(_)`. Each window (tumbling / hopping window or per-key
/// session) gets its own [`crate::aggregation::AggregateStream`]
/// instance so the existing per-group hash-aggregator machinery is
/// reused without duplication — windowing partitions records, not the
/// accumulator implementation.
///
/// Close-readiness is decided once at dispatch entry: every window
/// whose `end + allowed_lateness <= min_across_sources` over the
/// upstream Source set is treated as closed, and any record whose
/// assigned window falls in that closed set routes to the DLQ as
/// `DlqErrorCategory::LateRecord`. Open windows accumulate every
/// in-order record; at end-of-input (drain-to-Vec batch model) every
/// open window finalizes and contributes one emit row per
/// (group-by-key) group it observed.
fn run_time_windowed_aggregate(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    win_ctx: &WindowedAggContext<'_>,
    input: &[(Record, u64)],
    spec: &clinker_plan::config::pipeline_node::TimeWindowSpec,
    allowed_lateness: Option<std::time::Duration>,
) -> Result<Vec<crate::aggregation::SortRow>, PipelineError> {
    let name = win_ctx.name;
    let compiled = win_ctx.compiled;
    let output_schema = Arc::clone(&win_ctx.output_schema);
    use crate::aggregation::{AggregateStream, HashAggError, SortRow as AggSortRow};
    use crate::executor::time_window::{
        WindowBounds, duration_to_nanos, hopping_windows, partition_into_sessions,
        record_event_time_nanos, session_is_closed, tumbling_window, upstream_source_names,
        window_is_closed,
    };
    use clinker_plan::config::pipeline_node::TimeWindowSpec;
    use clinker_record::group_key::value_to_group_key;
    use std::collections::HashMap;

    let upstream_sources = upstream_source_names(current_dag, node_idx);
    let allowed_lateness_nanos = allowed_lateness.map(duration_to_nanos).unwrap_or(0);
    // Per-record streaming watermark: as we walk `input` in arrival
    // order, each record's `$source.event_time` advances its source's
    // running max; the running `min_across_sources` at the moment we
    // examine record N is the min over per-source maxes from records
    // {0..N-1}. A record at event-time `t` whose window
    // `[w_start, w_end)` already satisfies
    // `w_end + allowed_lateness <= running_min` was assigned to a
    // closed window and routes to the DLQ as `LateRecord`. This
    // mirrors Flink's per-record watermark advance under the
    // BoundedOutOfOrdernessWatermarks pattern and gives the dispatch
    // arm correct late-record semantics in batch mode without waiting
    // for the post-execute_dag `ctx.watermarks` fold (which lands
    // after dispatch returns).
    let mut running_per_source_max: std::collections::HashMap<String, i64> =
        std::collections::HashMap::with_capacity(upstream_sources.len());
    let running_min_across =
        |running: &std::collections::HashMap<String, i64>, sources: &[String]| -> Option<i64> {
            sources
                .iter()
                .filter_map(|s| running.get(s.as_str()).copied())
                .min()
        };

    let mut out_rows: Vec<AggSortRow> = Vec::new();
    // Per-(group_by) key extraction for session bucketing: mirrors
    // `HashAggregator::add_record`'s prefix walk so session assignment
    // here groups records identically to how the underlying aggregator
    // would have grouped them post-walk.
    let compute_group_key = |record: &Record,
                             row_num: u64|
     -> Result<Vec<GroupByKey>, PipelineError> {
        let mut key: Vec<GroupByKey> = Vec::with_capacity(compiled.group_by_indices.len());
        for (i, idx) in compiled.group_by_indices.iter().enumerate() {
            let field_name = compiled
                .group_by_fields
                .get(i)
                .map(String::as_str)
                .unwrap_or("");
            let val = record
                .values()
                .get(*idx as usize)
                .cloned()
                .unwrap_or(Value::Null);
            match value_to_group_key(&val, field_name, None, row_num) {
                Ok(Some(gk)) => key.push(gk),
                Ok(None) => key.push(GroupByKey::Null),
                Err(e) => {
                    return Err(PipelineError::Internal {
                        op: "time-windowed-aggregation",
                        node: name.to_string(),
                        detail: format!(
                            "group-by key extraction failed for field {field_name:?} at row {row_num}: {e}"
                        ),
                    });
                }
            }
        }
        Ok(key)
    };

    match spec {
        TimeWindowSpec::Tumbling { size } => {
            let size_nanos = duration_to_nanos(*size);
            if size_nanos <= 0 {
                return Err(PipelineError::Internal {
                    op: "time-windowed-aggregation",
                    node: name.to_string(),
                    detail: "tumbling window size must be > 0".to_string(),
                });
            }
            let mut per_window: HashMap<
                i64,
                (AggregateStream, crate::pipeline::memory::ConsumerId),
            > = HashMap::new();
            (|| -> Result<(), PipelineError> {
                for (record, row_num) in input {
                    let Some(t) = record_event_time_nanos(record) else {
                        continue;
                    };
                    let w = tumbling_window(t, size_nanos);
                    // Per-record running watermark BEFORE folding this
                    // record's event-time. Records with `event_time`
                    // older than the watermark by more than
                    // `allowed_lateness` past their window end route
                    // to the DLQ as `LateRecord`.
                    let running_min =
                        running_min_across(&running_per_source_max, &upstream_sources);
                    if window_is_closed(w.end, allowed_lateness_nanos, running_min) {
                        push_late_record(ctx, name, record, *row_num, w)?;
                        continue;
                    }
                    // Fold this record into the per-source running max
                    // AFTER the late check so a late record does not
                    // advance its source's watermark.
                    let src = source_name_arc_of(record).to_string();
                    running_per_source_max
                        .entry(src)
                        .and_modify(|v| {
                            if t > *v {
                                *v = t;
                            }
                        })
                        .or_insert(t);
                    add_to_window(
                        ctx,
                        win_ctx,
                        record,
                        *row_num,
                        w.start,
                        &mut per_window,
                        &mut out_rows,
                    )?;
                }
                Ok(())
            })()?;
            finalize_windows(ctx, name, per_window, &mut out_rows, &output_schema, input)?;
        }
        TimeWindowSpec::Hopping { size, slide } => {
            let size_nanos = duration_to_nanos(*size);
            let slide_nanos = duration_to_nanos(*slide);
            if size_nanos <= 0 || slide_nanos <= 0 {
                return Err(PipelineError::Internal {
                    op: "time-windowed-aggregation",
                    node: name.to_string(),
                    detail: "hopping window size and slide must both be > 0".to_string(),
                });
            }
            let mut per_window: HashMap<
                i64,
                (AggregateStream, crate::pipeline::memory::ConsumerId),
            > = HashMap::new();
            (|| -> Result<(), PipelineError> {
                for (record, row_num) in input {
                    let Some(t) = record_event_time_nanos(record) else {
                        continue;
                    };
                    let windows = hopping_windows(t, size_nanos, slide_nanos);
                    let running_min =
                        running_min_across(&running_per_source_max, &upstream_sources);
                    // A record is late iff EVERY window it would
                    // belong to is closed at the current watermark.
                    // For overlapping HOP windows, partial closure is
                    // possible (some closed, some still open) — route
                    // the record to its still-open windows and emit a
                    // single DLQ entry only when no window remains
                    // open. Mirrors Flink's late-event-on-sliding
                    // semantics.
                    let mut routed_to_any = false;
                    let mut first_closed: Option<crate::executor::time_window::WindowBounds> = None;
                    for w in windows {
                        if window_is_closed(w.end, allowed_lateness_nanos, running_min) {
                            if first_closed.is_none() {
                                first_closed = Some(w);
                            }
                            continue;
                        }
                        add_to_window(
                            ctx,
                            win_ctx,
                            record,
                            *row_num,
                            w.start,
                            &mut per_window,
                            &mut out_rows,
                        )?;
                        routed_to_any = true;
                    }
                    if !routed_to_any {
                        if let Some(w) = first_closed {
                            push_late_record(ctx, name, record, *row_num, w)?;
                            continue;
                        }
                        // No windows at all (slide > size gap) —
                        // record falls in no bucket; silently skip,
                        // matching the empty-window branch in
                        // `hopping_windows`.
                        continue;
                    }
                    let src = source_name_arc_of(record).to_string();
                    running_per_source_max
                        .entry(src)
                        .and_modify(|v| {
                            if t > *v {
                                *v = t;
                            }
                        })
                        .or_insert(t);
                }
                Ok(())
            })()?;
            finalize_windows(ctx, name, per_window, &mut out_rows, &output_schema, input)?;
        }
        TimeWindowSpec::Session { gap } => {
            let gap_nanos = duration_to_nanos(*gap);
            if gap_nanos <= 0 {
                return Err(PipelineError::Internal {
                    op: "time-windowed-aggregation",
                    node: name.to_string(),
                    detail: "session window gap must be > 0".to_string(),
                });
            }
            // First pass: bucket input indices by group-by key and
            // record each one's event-time. Records without
            // `$source.event_time` are skipped — session assignment
            // requires a per-record event-time anchor.
            let mut by_key: HashMap<Vec<GroupByKey>, Vec<(usize, i64)>> = HashMap::new();
            for (i, (record, row_num)) in input.iter().enumerate() {
                let Some(t) = record_event_time_nanos(record) else {
                    continue;
                };
                let key = compute_group_key(record, *row_num)?;
                by_key.entry(key).or_default().push((i, t));
            }
            // Second pass: per group, sort indices by event-time,
            // partition into sessions; remember the (group_key,
            // session_idx, session_bounds) assignment per input index.
            type SessionStreamKey = (Vec<GroupByKey>, usize);
            #[derive(Clone)]
            struct PerRecordSession {
                key: Vec<GroupByKey>,
                session_idx: usize,
                session: crate::executor::time_window::SessionInstance,
            }
            let mut record_session_info: Vec<Option<PerRecordSession>> = vec![None; input.len()];
            for (key, mut indexed_times) in by_key {
                indexed_times.sort_by_key(|(_, t)| *t);
                let sorted_times: Vec<i64> = indexed_times.iter().map(|(_, t)| *t).collect();
                let (assignments, sessions) = partition_into_sessions(&sorted_times, gap_nanos);
                for ((record_idx, _t), session_idx) in indexed_times.iter().zip(assignments.iter())
                {
                    record_session_info[*record_idx] = Some(PerRecordSession {
                        key: key.clone(),
                        session_idx: *session_idx,
                        session: sessions[*session_idx].clone(),
                    });
                }
            }
            // Third pass: walk input in arrival order so the
            // streaming running watermark advances monotonically with
            // the per-record check. Bucketing the AggregateStream by
            // (group_key, session_idx) separates session emits
            // without changing the underlying HashAggregator's
            // group-by contract.
            let mut session_streams: HashMap<
                SessionStreamKey,
                (AggregateStream, crate::pipeline::memory::ConsumerId),
            > = HashMap::new();
            (|| -> Result<(), PipelineError> {
                for (i, (rec, rn)) in input.iter().enumerate() {
                    let Some(info) = record_session_info[i].clone() else {
                        continue;
                    };
                    let running_min =
                        running_min_across(&running_per_source_max, &upstream_sources);
                    if session_is_closed(
                        &info.session,
                        gap_nanos,
                        allowed_lateness_nanos,
                        running_min,
                    ) {
                        let bounds = WindowBounds {
                            start: info.session.start,
                            end: info.session.last_event_time.saturating_add(gap_nanos),
                        };
                        push_late_record(ctx, name, rec, *rn, bounds)?;
                        continue;
                    }
                    if let Some(t) = record_event_time_nanos(rec) {
                        let src = source_name_arc_of(rec).to_string();
                        running_per_source_max
                            .entry(src)
                            .and_modify(|v| {
                                if t > *v {
                                    *v = t;
                                }
                            })
                            .or_insert(t);
                    }
                    let stream_key = (info.key, info.session_idx);
                    let entry = session_streams.entry(stream_key);
                    let (stream, _consumer_id) = match entry {
                        std::collections::hash_map::Entry::Occupied(o) => o.into_mut(),
                        std::collections::hash_map::Entry::Vacant(v) => {
                            let fresh = win_ctx.make_stream(ctx)?;
                            v.insert(fresh)
                        }
                    };
                    let source_file_arc = source_file_arc_of(rec);
                    let source_name_arc = source_name_arc_of(rec);
                    let eval_ctx = ctx.eval_ctx_for_record(
                        &source_file_arc,
                        &source_name_arc,
                        *rn,
                        rec.doc_ctx(),
                    );
                    let add_result = stream.add_record(rec, *rn, &eval_ctx, &mut out_rows);
                    if add_result.is_ok() {
                        advance_cursor(ctx, &source_name_arc, *rn);
                    }
                    if let Err(e) = add_result {
                        handle_aggregate_add_error(ctx, name, rec, *rn, e)?;
                    }
                }
                Ok(())
            })()?;
            // Finalize every (group, session) stream. Walk in
            // deterministic order (sorted by (group_key, session_idx))
            // so emit order is stable across runs.
            let mut entries: Vec<(
                SessionStreamKey,
                (AggregateStream, crate::pipeline::memory::ConsumerId),
            )> = session_streams.into_iter().collect();
            // `GroupByKey` does not implement `Ord`; fall back to a
            // Debug-formatted key for deterministic finalize order
            // across runs. The session_idx breaks ties for the same
            // group key.
            entries.sort_by(|a, b| {
                let ka: Vec<String> = a.0.0.iter().map(|k| format!("{k:?}")).collect();
                let kb: Vec<String> = b.0.0.iter().map(|k| format!("{k:?}")).collect();
                ka.cmp(&kb).then(a.0.1.cmp(&b.0.1))
            });
            let finalize_ctx = ctx.merged_eval_ctx();
            for (_, (stream, consumer_id)) in entries {
                // Finalize consumes this session's stream; unregister
                // its wrapper unconditionally afterward so the session's
                // bytes leave `sum_consumer_usage` whether finalize
                // emitted rows or routed a group failure to the DLQ.
                let result = stream.finalize(&finalize_ctx, &mut out_rows);
                ctx.memory_budget.unregister_consumer(consumer_id);
                match result {
                    Ok(()) => {}
                    Err(HashAggError::Accumulator {
                        transform,
                        binding,
                        source,
                    }) => match ctx.config.error_handling.strategy {
                        ErrorStrategy::FailFast => {
                            return Err(PipelineError::Accumulator {
                                transform,
                                binding,
                                source,
                            });
                        }
                        ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                            emit_aggregate_finalize_dlq(
                                ctx,
                                name,
                                input,
                                &output_schema,
                                &transform,
                                &binding,
                                &source,
                            )?;
                        }
                    },
                    Err(other) => return Err(other.into()),
                }
            }
        }
    }

    Ok(out_rows)
}

/// Route a per-window add for tumbling/hopping. Wraps
/// `AggregateStream::add_record` with the existing per-record DLQ /
/// failfast policy.
fn add_to_window(
    ctx: &mut ExecutorContext<'_>,
    win_ctx: &WindowedAggContext<'_>,
    record: &Record,
    row_num: u64,
    window_start: i64,
    per_window: &mut std::collections::HashMap<
        i64,
        (
            crate::aggregation::AggregateStream,
            crate::pipeline::memory::ConsumerId,
        ),
    >,
    out_rows: &mut Vec<crate::aggregation::SortRow>,
) -> Result<(), PipelineError> {
    let source_file_arc = source_file_arc_of(record);
    let source_name_arc = source_name_arc_of(record);
    let eval_ctx = ctx.eval_ctx_for_record(
        &source_file_arc,
        &source_name_arc,
        row_num,
        record.doc_ctx(),
    );
    let (stream, _consumer_id) = match per_window.entry(window_start) {
        std::collections::hash_map::Entry::Occupied(o) => o.into_mut(),
        std::collections::hash_map::Entry::Vacant(v) => {
            let fresh = win_ctx.make_stream(ctx)?;
            v.insert(fresh)
        }
    };
    let add_result = stream.add_record(record, row_num, &eval_ctx, out_rows);
    if add_result.is_ok() {
        advance_cursor(ctx, &source_name_arc, row_num);
        return Ok(());
    }
    let e = add_result.err().unwrap();
    handle_aggregate_add_error(ctx, win_ctx.name, record, row_num, e)
}

/// Shared per-record `add_record` error handler for both
/// tumbling/hopping and session arms. Mirrors the positional
/// aggregate arm's error-strategy switch.
fn handle_aggregate_add_error(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    record: &Record,
    row_num: u64,
    e: crate::aggregation::HashAggError,
) -> Result<(), PipelineError> {
    match ctx.config.error_handling.strategy {
        ErrorStrategy::FailFast => Err(e.into()),
        ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
            let stage = Some(clinker_core_types::dlq::stage_aggregate(name));
            let routed = record_error_to_buffer_if_grouped(
                ctx,
                record,
                row_num,
                clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
                format!("aggregate {name}: {e}"),
                stage.clone(),
                None,
            );
            if !routed {
                let source_name = source_name_arc_of(record);
                push_dlq(
                    ctx,
                    DlqEntry {
                        source_row: row_num,
                        category: clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
                        error_message: format!("aggregate {name}: {e}"),
                        original_record: record.clone(),
                        stage,
                        route: None,
                        trigger: true,
                        source_name,
                        triggering_field: None,
                        triggering_value: None,
                    },
                )?;
            }
            Ok(())
        }
    }
}

/// Finalize every per-window aggregator after the per-record walk.
/// Walks windows in ascending `window_start` order for deterministic
/// emit ordering. Accumulator failures route to the DLQ as
/// `AggregateFinalize`, mirroring the positional aggregate arm.
fn finalize_windows(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    per_window: std::collections::HashMap<
        i64,
        (
            crate::aggregation::AggregateStream,
            crate::pipeline::memory::ConsumerId,
        ),
    >,
    out_rows: &mut Vec<crate::aggregation::SortRow>,
    output_schema: &Arc<Schema>,
    input: &[(Record, u64)],
) -> Result<(), PipelineError> {
    use crate::aggregation::HashAggError;
    let mut entries: Vec<(
        i64,
        (
            crate::aggregation::AggregateStream,
            crate::pipeline::memory::ConsumerId,
        ),
    )> = per_window.into_iter().collect();
    entries.sort_by_key(|(start, _)| *start);
    let finalize_ctx = ctx.merged_eval_ctx();
    for (_, (stream, consumer_id)) in entries {
        // Finalize consumes this window's stream; unregister its
        // wrapper unconditionally afterward so the window's bytes leave
        // `sum_consumer_usage` whether finalize emitted rows or routed a
        // group failure to the DLQ. The state is gone either way.
        let result = stream.finalize(&finalize_ctx, out_rows);
        ctx.memory_budget.unregister_consumer(consumer_id);
        match result {
            Ok(()) => {}
            Err(HashAggError::Accumulator {
                transform,
                binding,
                source,
            }) => match ctx.config.error_handling.strategy {
                ErrorStrategy::FailFast => {
                    return Err(PipelineError::Accumulator {
                        transform,
                        binding,
                        source,
                    });
                }
                ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                    emit_aggregate_finalize_dlq(
                        ctx,
                        name,
                        input,
                        output_schema,
                        &transform,
                        &binding,
                        &source,
                    )?;
                }
            },
            Err(other) => return Err(other.into()),
        }
    }
    Ok(())
}

/// Route a late-arriving record to the DLQ as
/// `DlqErrorCategory::LateRecord`. The error detail carries the
/// window bounds in nanoseconds so a downstream reader can correlate
/// each late drop to the specific time bucket that had closed.
fn push_late_record(
    ctx: &mut ExecutorContext<'_>,
    transform: &str,
    record: &Record,
    row_num: u64,
    bounds: crate::executor::time_window::WindowBounds,
) -> Result<(), PipelineError> {
    let source_name = source_name_arc_of(record);
    push_dlq(
        ctx,
        DlqEntry {
            source_row: row_num,
            category: clinker_core_types::dlq::DlqErrorCategory::LateRecord,
            error_message: format!(
                "time-window {transform}: record at event-time inside window \
                 [{}, {}) (nanos) which had already closed",
                bounds.start, bounds.end
            ),
            original_record: record.clone(),
            stage: Some(clinker_core_types::dlq::stage_time_window(transform)),
            route: None,
            trigger: true,
            source_name,
            triggering_field: None,
            triggering_value: None,
        },
    )
}

/// Emit an `AggregateFinalize` DLQ entry for an accumulator failure at
/// finalize-time. Shared by tumbling/hopping `finalize_windows` and
/// the session arm; mirrors the positional aggregate arm's
/// synthetic-record fallback when input is empty.
fn emit_aggregate_finalize_dlq(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    input: &[(Record, u64)],
    output_schema: &Arc<Schema>,
    transform: &str,
    binding: &str,
    source: &clinker_record::accumulator::AccumulatorError,
) -> Result<(), PipelineError> {
    let (synthetic, source_name) = if let Some((rec, _)) = input.first() {
        let sn = source_name_arc_of(rec);
        (rec.clone(), sn)
    } else {
        (
            Record::new(Arc::clone(output_schema), Vec::new()),
            Arc::from(name),
        )
    };
    push_dlq(
        ctx,
        DlqEntry {
            source_row: 0,
            category: clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
            error_message: format!("aggregate {transform}.{binding}: {source:?}"),
            original_record: synthetic,
            stage: Some(clinker_core_types::dlq::stage_aggregate(name)),
            route: None,
            trigger: true,
            source_name,
            triggering_field: None,
            triggering_value: None,
        },
    )
}
