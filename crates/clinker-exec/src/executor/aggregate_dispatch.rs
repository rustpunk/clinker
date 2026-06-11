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

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{DocumentId, GroupByKey, Record, Schema, SchemaBuilder, Value};
use cxl::eval::{EvalContext, ProgramEvaluator};
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
    } else if is_relaxed {
        // Relaxed-CK retraction path: one retained `HashAggregator`
        // spanning the whole input, parked on
        // `relaxed_aggregator_states` so the correlation-commit
        // orchestrator can `retract_row` + `finalize_in_place` against
        // the same instance that produced these rows. The commit model
        // requires every group live across the retract iterations, so
        // per-document flush does not apply here — a document-aware
        // relaxed-CK aggregate keeps its single cross-document table.
        //
        // The single `ConsumerHandle` is shared with the
        // `AggregateConsumer` the arbitrator holds; its `ConsumerId` is
        // captured so the wrapper is unregistered the moment the
        // aggregator's state stops being live. Parking transfers
        // ownership of the unregister to `RetainedAggregatorState`,
        // which fires it when the aggregator drops.
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
                    handle_aggregate_add_error(ctx, name, record, *row_num, e)?;
                }
            }
            Ok(())
        })()?;

        // Finalize into the retained boxed aggregator (rather than
        // consuming it) so the commit phase can drive the same
        // instance. Accumulator finalize errors route to the DLQ under
        // `Continue` / `BestEffort`; all other engine errors propagate.
        let finalize_ctx = ctx.merged_eval_ctx();
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
        let agg_out = match hash_box.finalize_in_place(&finalize_ctx, &mut emitted_rows) {
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
                    emit_aggregate_finalize_dlq(
                        ctx,
                        name,
                        &input,
                        output_schema,
                        &transform,
                        &binding,
                        &source,
                    )?;
                    Vec::new()
                }
            },
            Err(other) => return Err(other.into()),
        };
        // A relaxed aggregate that finalized successfully parked its
        // boxed aggregator on `relaxed_aggregator_states` (the key is
        // present): that state stays live across the commit phase's
        // retract + re-finalize iterations, so its wrapper must stay
        // registered — `RetainedAggregatorState` owns the unregister and
        // fires it when the aggregator drops. A finalize that failed and
        // routed to the DLQ dropped its aggregator without parking it, so
        // the key is absent and the wrapper is unregistered here.
        if !ctx.relaxed_aggregator_states.contains_key(&node_idx) {
            ctx.memory_budget.unregister_consumer(agg_consumer_id);
        }
        agg_out
    } else {
        // Strict path with per-document group flush. The single
        // `evaluator` built above is unused — each document's table builds
        // its own behind an `Arc<TypedProgram>`. Drop it (and the
        // single-stream `spill_schema`, which the factory re-derives) so
        // any throwaway state releases promptly.
        drop(evaluator);
        drop(spill_schema);
        let factory =
            DocAggregatorFactory::from_ctx(ctx, name, agg_strategy, compiled, output_schema)?;
        run_strict_aggregate_per_document(ctx, factory, name, output_schema, &input, &input_puncts)?
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
        // `out_rows` already carries each closing document's groups
        // flushed at its `DocumentClose` boundary (the ingest arm
        // split them per document); the inbound punctuations forward
        // unchanged at the output buffer tail (Preserving), trailing
        // the document's flushed rows.
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

/// `ctx`-free factory for one document's [`crate::aggregation::AggregateStream`].
///
/// A document-aware source feeds an Aggregate one document per source
/// file, each carrying its own [`DocumentId`] and a `DocumentClose`
/// boundary at its tail. Per-document aggregation builds a fresh group
/// table for each document and flushes it at the document's close, so a
/// multi-document run produces one set of grouped rows per document rather
/// than a single cross-document aggregate.
///
/// This factory captures everything [`crate::aggregation::AggregateStream::for_node`]
/// needs without borrowing the [`ExecutorContext`], so it works on both
/// the dispatch thread (materialized path) and the streaming-ingest scoped
/// thread, which holds no `&mut ExecutorContext`. Each built table
/// registers its own [`crate::aggregation::AggregateConsumer`] with the
/// shared arbitrator, so the open document's group bytes count toward the
/// run's RSS accounting and the existing per-aggregator spill triggers
/// fire normally.
struct DocAggregatorFactory {
    strategy: AggregateStrategy,
    compiled: Arc<cxl::plan::CompiledAggregate>,
    typed: Arc<cxl::typecheck::TypedProgram>,
    has_distinct: bool,
    max_expansion: u64,
    output_schema: Arc<Schema>,
    spill_schema: Arc<Schema>,
    mem_limit: usize,
    spill_dir: std::path::PathBuf,
    spill_compress: bool,
    transform_name: String,
    arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>,
}

impl DocAggregatorFactory {
    /// Snapshot the build inputs out of `ctx` for an aggregate node.
    /// Resolves the spill schema, memory limit, and spill-compression mode
    /// from `ctx` itself — the same derivations the single-stream paths run
    /// inline — so the caller passes only the node identity.
    ///
    /// Returns an error when no compiled transform backs the node — the
    /// same hard-abort `PipelineError::Internal` the single-stream paths
    /// raise, surfaced here once at construction so [`Self::make`] only
    /// fails on the never-taken `for_node` arm.
    fn from_ctx(
        ctx: &ExecutorContext<'_>,
        node_name: &str,
        strategy: AggregateStrategy,
        compiled: &Arc<cxl::plan::CompiledAggregate>,
        output_schema: &Arc<Schema>,
    ) -> Result<Self, PipelineError> {
        let transform_idx = ctx
            .transform_by_name
            .get(node_name)
            .copied()
            .ok_or_else(|| PipelineError::Internal {
                op: "aggregation",
                node: node_name.to_string(),
                detail: "no compiled transform found for aggregate node".to_string(),
            })?;
        let compiled_transform = &ctx.compiled_transforms[transform_idx];
        // Spill schema mirrors `HashAggregator::spill`: group-by columns ++
        // `__acc_state` ++ `__meta_tracker`. The compression mode resolves
        // against the output-schema width and batch size so a spilled
        // table's on-disk format matches what `--explain` reports.
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
        let spill_compress = ctx
            .spill_compress
            .resolve_for_schema(output_schema.column_count(), ctx.batch_size as u64);
        Ok(Self {
            strategy,
            compiled: Arc::clone(compiled),
            typed: Arc::clone(&compiled_transform.typed),
            has_distinct: compiled_transform.has_distinct(),
            max_expansion: compiled_transform.max_expansion,
            output_schema: Arc::clone(output_schema),
            spill_schema,
            mem_limit: parse_memory_limit(ctx.config),
            spill_dir: ctx.spill_root_path.to_path_buf(),
            spill_compress,
            transform_name: node_name.to_string(),
            arbitrator: Arc::clone(&ctx.memory_budget),
        })
    }

    /// Build a fresh stream + register its arbitrator consumer. Building a
    /// new `ProgramEvaluator` per document is cheap: the heavy CXL
    /// pipeline lives behind `Arc<TypedProgram>`.
    fn make(
        &self,
    ) -> Result<
        (
            crate::aggregation::AggregateStream,
            crate::pipeline::memory::ConsumerId,
        ),
        PipelineError,
    > {
        let evaluator = ProgramEvaluator::with_max_expansion(
            Arc::clone(&self.typed),
            self.has_distinct,
            self.max_expansion,
        );
        let handle = crate::pipeline::memory::ConsumerHandle::new();
        let consumer_id = self.arbitrator.register_consumer(Arc::new(
            crate::aggregation::AggregateConsumer::new(handle.clone()),
        ));
        let stream = crate::aggregation::AggregateStream::for_node(
            self.strategy,
            crate::aggregation::AggregatorConfig {
                compiled: Arc::clone(&self.compiled),
                evaluator,
                output_schema: Arc::clone(&self.output_schema),
                spill_schema: Arc::clone(&self.spill_schema),
                memory_budget: self.mem_limit,
                spill_dir: Some(self.spill_dir.clone()),
                spill_compress: self.spill_compress,
                transform_name: self.transform_name.clone(),
                consumer_handle: handle,
            },
        )?;
        Ok((stream, consumer_id))
    }
}

/// Single live group table that flushes on each document-close boundary,
/// for the streaming-ingest path where boundaries arrive inline with
/// records.
///
/// Holds at most one [`crate::aggregation::AggregateStream`] at a time —
/// the open document's groups. On `DocumentClose` the current table
/// finalizes, emits, and resets, so a fused single-source stream (whose
/// boundaries arrive in document order) produces one set of grouped rows
/// per document. A non-fused Merge instead delivers its forwarded
/// punctuations after every record, so the lone table accumulates the
/// whole stream and flushes once at the tail — folding unreconciled
/// documents together exactly as a no-document run would.
///
/// Holding one table at a time keeps the peak footprint at a single
/// document's group state (plus its arbitrator consumer), so per-document
/// flush lowers peak memory rather than inflating it. The materialized
/// drain-to-`Vec` path keys tables by document instead (see
/// [`run_strict_aggregate_per_document`]) because its boundaries are split
/// out of arrival order.
struct DocumentFlushAggregator {
    factory: DocAggregatorFactory,
    current: Option<(
        crate::aggregation::AggregateStream,
        crate::pipeline::memory::ConsumerId,
    )>,
}

impl DocumentFlushAggregator {
    fn new(factory: DocAggregatorFactory) -> Self {
        Self {
            factory,
            current: None,
        }
    }

    /// Borrow the open table, building (and registering) a fresh one when
    /// none is open.
    fn stream(&mut self) -> Result<&mut crate::aggregation::AggregateStream, PipelineError> {
        if self.current.is_none() {
            self.current = Some(self.factory.make()?);
        }
        Ok(&mut self.current.as_mut().expect("just populated").0)
    }

    /// Finalize, emit, and reset the open table (if any). The next
    /// `stream` call builds a fresh one for the following document. A
    /// `DocumentClose` for a document that contributed no records finds no
    /// open table and is a no-op.
    fn flush_current(
        &mut self,
        finalize_ctx: &EvalContext,
        out: &mut Vec<crate::aggregation::SortRow>,
    ) -> Result<(), crate::aggregation::HashAggError> {
        let Some((stream, consumer_id)) = self.current.take() else {
            return Ok(());
        };
        let result = stream.finalize(finalize_ctx, out);
        // `finalize` consumed the stream — its group bytes are gone, so
        // unregister regardless of whether finalize succeeded.
        self.factory.arbitrator.unregister_consumer(consumer_id);
        result
    }

    /// Force an empty table into existence so an empty input still runs
    /// one finalize. A global fold (no group-by) over empty input emits its
    /// single defaulted row; an empty grouped fold emits nothing —
    /// reproducing the single-stream finalize this replaced. Idempotent.
    fn ensure_open(&mut self) -> Result<(), PipelineError> {
        if self.current.is_none() {
            self.current = Some(self.factory.make()?);
        }
        Ok(())
    }

    /// `true` when this aggregate is a global fold (no group-by columns).
    /// A global fold owes one defaulted row per closing document even when
    /// that document contributed zero records; a grouped fold owes nothing
    /// for an empty document.
    fn is_global_fold(&self) -> bool {
        self.factory.compiled.group_by_fields.is_empty()
    }
}

impl Drop for DocumentFlushAggregator {
    /// Release the open table's arbitrator consumer on every exit path.
    ///
    /// `flush_current` takes `current` and unregisters the consumer on the
    /// normal flush, so a cleanly-drained aggregator drops with
    /// `current == None` and this is a no-op. An error return from the
    /// ingest loop, by contrast, drops the aggregator with a table still
    /// open; without this the open table's `AggregateConsumer` would linger
    /// in the arbitrator's registry — the registry holds an independent
    /// `Arc` keyed by `ConsumerId`, so dropping the `AggregateStream` alone
    /// does not unregister it. `unregister_consumer` is idempotent, so even
    /// a redundant call is harmless.
    fn drop(&mut self) {
        if let Some((_, consumer_id)) = self.current.take() {
            self.factory.arbitrator.unregister_consumer(consumer_id);
        }
    }
}

/// Per-document group tables keyed by flush key, with a teardown that
/// unregisters any table still present from the shared arbitrator.
///
/// Each built [`crate::aggregation::AggregateStream`] registers an
/// [`crate::aggregation::AggregateConsumer`] keyed by a [`ConsumerId`]; the
/// arbitrator's registry holds an independent `Arc`, so dropping the stream
/// alone leaves its consumer charged against `sum_consumer_usage`. The
/// per-document flush loop `remove`s and finalizes each table, then
/// unregisters it explicitly — but an `add_record` `FailFast` error, a
/// non-`Accumulator` finalize error, or a `factory.make()` failure can exit
/// the routine with tables still live. This guard's `Drop` unregisters
/// every still-present consumer on *every* exit, so an early `?` cannot
/// strand a consumer in the registry. A normal completion `remove`s every
/// key as it flushes, so the guard drops an empty map and is a no-op; a
/// `remove`d-and-flushed table is gone from the map before drop, so no
/// consumer is unregistered twice.
struct RegisteredTables {
    tables: HashMap<
        DocumentId,
        (
            crate::aggregation::AggregateStream,
            crate::pipeline::memory::ConsumerId,
        ),
    >,
    arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>,
}

impl RegisteredTables {
    fn new(arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>) -> Self {
        Self {
            tables: HashMap::new(),
            arbitrator,
        }
    }

    /// Borrow the table for `key`, building (and registering) a fresh one
    /// via `factory` when none exists yet. The freshly built consumer is
    /// tracked by the guard the moment it lands in the map, so a later early
    /// return unregisters it.
    fn entry_or_make(
        &mut self,
        key: DocumentId,
        factory: &DocAggregatorFactory,
    ) -> Result<&mut crate::aggregation::AggregateStream, PipelineError> {
        let entry = match self.tables.entry(key) {
            std::collections::hash_map::Entry::Occupied(o) => o.into_mut(),
            std::collections::hash_map::Entry::Vacant(v) => v.insert(factory.make()?),
        };
        Ok(&mut entry.0)
    }

    /// Build (and register) a fresh table for `key` unconditionally. Used to
    /// force a global fold's defaulted row for an empty document.
    fn insert_fresh(
        &mut self,
        key: DocumentId,
        factory: &DocAggregatorFactory,
    ) -> Result<(), PipelineError> {
        self.tables.insert(key, factory.make()?);
        Ok(())
    }

    /// Take the table for `key` out of the guard, transferring its consumer
    /// to the caller, which must unregister it after finalizing. Removing it
    /// here is what keeps the guard's `Drop` from double-unregistering a
    /// flushed table.
    fn remove(
        &mut self,
        key: DocumentId,
    ) -> Option<(
        crate::aggregation::AggregateStream,
        crate::pipeline::memory::ConsumerId,
    )> {
        self.tables.remove(&key)
    }

    /// Flush keys in ascending [`DocumentId`] order for deterministic emit
    /// ordering across the leftover fold.
    fn sorted_keys(&self) -> Vec<DocumentId> {
        let mut keys: Vec<DocumentId> = self.tables.keys().copied().collect();
        keys.sort_unstable();
        keys
    }
}

impl Drop for RegisteredTables {
    fn drop(&mut self) {
        for (_, (_, consumer_id)) in self.tables.drain() {
            self.arbitrator.unregister_consumer(consumer_id);
        }
    }
}

/// Drive a strict (non-relaxed, non-time-windowed) Aggregate with
/// per-document group flush over a fully-drained input batch.
///
/// `drain_split` separated the predecessor's records from its boundaries,
/// so the closing-document set is read once from `input_puncts`, then each
/// record routes to a live group table keyed by a *flush key*: its own doc
/// id when that document's close was forwarded, or a shared sentinel
/// otherwise. On `DocumentClose(D)` the table for D finalizes, emits, and
/// drops; the sentinel table (every unreconciled / no-document record)
/// flushes at end-of-input. Keying by flush key — rather than tracking a
/// running document — is robust to arbitrary record interleaving (an
/// upstream Merge in `interleave` mode): a closing document's groups stay
/// isolated even when another document's records arrive between them, and
/// unreconciled documents fold together exactly as a no-document run would.
///
/// A no-document or single-document run keys every record to one table
/// (the file's single closing document, or the sentinel) flushed once —
/// byte-identical to a plain finalize.
///
/// Punctuations are not consumed here — the caller forwards the full
/// `input_puncts` at the output tail, preserving the boundary for
/// downstream operators while this arm flushes the closing document's
/// groups ahead of it. Peak footprint is the concurrently-open closing
/// documents plus the sentinel; each closing document drops at its close,
/// so per-document flush lowers peak rather than inflating it.
fn run_strict_aggregate_per_document(
    ctx: &mut ExecutorContext<'_>,
    factory: DocAggregatorFactory,
    name: &str,
    output_schema: &Arc<Schema>,
    input: &[(Record, u64)],
    input_puncts: &[crate::executor::stream_event::Punctuation],
) -> Result<Vec<crate::aggregation::SortRow>, PipelineError> {
    use crate::executor::stream_event::PunctuationKind;

    // Documents whose close was forwarded to this Aggregate. Only these key
    // their own table; every other record (unreconciled documents from a
    // Merge that swallowed their closes, and the synthetic no-document id)
    // shares the `SYNTHETIC` sentinel table and folds together. A real
    // closing document never collides with the sentinel — sources allocate
    // ids from a counter that starts past `DocumentId::SYNTHETIC`.
    let closing_docs: std::collections::HashSet<DocumentId> = input_puncts
        .iter()
        .filter(|p| p.kind() == PunctuationKind::DocumentClose)
        .map(|p| p.doc_id())
        .collect();
    let flush_key = |doc_id: DocumentId| -> DocumentId {
        if closing_docs.contains(&doc_id) {
            doc_id
        } else {
            DocumentId::SYNTHETIC
        }
    };

    // Hold the per-document tables behind a guard that unregisters any
    // still-present consumer on drop. A `FailFast` add error, a non-
    // `Accumulator` finalize error, or a `factory.make()` failure exits via
    // `?` with tables still live; without the guard each would strand its
    // arbitrator consumer in `sum_consumer_usage`. Normal completion removes
    // every key as it flushes, so the guard drops empty.
    let mut tables = RegisteredTables::new(Arc::clone(&factory.arbitrator));
    let mut out_rows: Vec<crate::aggregation::SortRow> = Vec::with_capacity(64);

    // A global fold (no group-by) owes one defaulted row per closing
    // document even when that document contributed zero records, matching a
    // standalone empty single-file run; a grouped fold owes nothing for an
    // empty document.
    let is_global_fold = factory.compiled.group_by_fields.is_empty();

    for (record, row_num) in input {
        let key = flush_key(record.doc_ctx().id());
        let source_file_arc = source_file_arc_of(record);
        let source_name_arc = source_name_arc_of(record);
        let eval_ctx = ctx.eval_ctx_for_record(
            &source_file_arc,
            &source_name_arc,
            *row_num,
            record.doc_ctx(),
        );
        let stream = tables.entry_or_make(key, &factory)?;
        let add_result = stream.add_record(record, *row_num, &eval_ctx, &mut out_rows);
        if add_result.is_ok() {
            advance_cursor(ctx, &source_name_arc, *row_num);
        }
        if let Err(e) = add_result {
            handle_aggregate_add_error(ctx, name, record, *row_num, e)?;
        }
    }

    // Flush each closing document at its boundary (in `input_puncts`
    // order), dropping its table. The merged-provenance finalize context
    // borrows only `'a`-lifetime data, so it survives the `&mut ctx` DLQ
    // routing between flushes.
    let finalize_ctx = ctx.merged_eval_ctx();
    // Documents already flushed in this loop, so a duplicate `DocumentClose`
    // for the same id does not synthesize a second global-fold row for a
    // document that already emitted.
    let mut flushed: std::collections::HashSet<DocumentId> = std::collections::HashSet::new();
    for p in input_puncts {
        if p.kind() != PunctuationKind::DocumentClose {
            continue;
        }
        let doc_id = p.doc_id();
        if !flushed.insert(doc_id) {
            continue;
        }
        // A closing document that contributed records owns a table built
        // lazily on its first record. One that contributed none has no table
        // — for a global fold, build a fresh table so its defaulted row
        // still emits; for a grouped fold, emit nothing.
        let entry = match tables.remove(doc_id) {
            Some(entry) => Some(entry),
            None if is_global_fold => Some(factory.make()?),
            None => None,
        };
        if let Some((stream, consumer_id)) = entry {
            let result = stream.finalize(&finalize_ctx, &mut out_rows);
            factory.arbitrator.unregister_consumer(consumer_id);
            // Attribute a finalize failure to a record from THIS document so
            // the DLQ entry points at the document's own source file, not the
            // batch's first document.
            let attribution = document_attribution_record(input, doc_id);
            route_document_flush_result(ctx, name, attribution, output_schema, result)?;
        }
    }

    // A wholly-empty input that flushed no closing document (the synthetic
    // no-document case — a fused single-source run whose only document never
    // forwarded a close) still owes its single defaulted row under a global
    // fold, so force the sentinel table open before the final flush. The
    // `flushed.is_empty()` guard keeps this from double-emitting when a
    // closing document already produced the global-fold row in the punct-loop
    // above (e.g. a global fold whose lone closing document flushed empty).
    if input.is_empty() && flushed.is_empty() {
        tables.insert_fresh(DocumentId::SYNTHETIC, &factory)?;
    }
    // Flush whatever is left in `DocumentId` order for deterministic emit
    // ordering: the sentinel (unreconciled / no-document remainder) and any
    // closing document whose close never arrived. This leftover fold spans
    // no single document, so its DLQ attribution keeps the batch-first-record
    // behavior.
    let finalize_ctx = ctx.merged_eval_ctx();
    for key in tables.sorted_keys() {
        let (stream, consumer_id) = tables.remove(key).expect("key from this map");
        let result = stream.finalize(&finalize_ctx, &mut out_rows);
        factory.arbitrator.unregister_consumer(consumer_id);
        route_document_flush_result(ctx, name, input, output_schema, result)?;
    }

    Ok(out_rows)
}

/// Pick a representative record for a flushing document so a finalize-time
/// DLQ entry is attributed to that document's source file, not the batch's
/// first document. Returns the input slice narrowed to the first record
/// whose `doc_ctx().id()` matches `doc_id`; when no record carries that id
/// (an empty closing document), returns the whole slice so
/// [`emit_aggregate_finalize_dlq`] keeps its existing first-record / node-
/// name fallback.
fn document_attribution_record(input: &[(Record, u64)], doc_id: DocumentId) -> &[(Record, u64)] {
    match input.iter().position(|(r, _)| r.doc_ctx().id() == doc_id) {
        Some(pos) => &input[pos..=pos],
        None => input,
    }
}

/// Route a document-flush finalize result: an `Accumulator` failure goes
/// to the DLQ under `Continue` / `BestEffort` (mirroring the single-stream
/// strict arm), `FailFast` and every other engine error propagate.
///
/// `attribution` is the input slice narrowed to the flushing document — a
/// single record from that document, or the whole batch for the cross-
/// document leftover fold — so a DLQ entry points at the document that
/// actually failed rather than the batch's first document.
fn route_document_flush_result(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    attribution: &[(Record, u64)],
    output_schema: &Arc<Schema>,
    result: Result<(), crate::aggregation::HashAggError>,
) -> Result<(), PipelineError> {
    use crate::aggregation::HashAggError;
    match result {
        Ok(_) => Ok(()),
        Err(HashAggError::Accumulator {
            transform,
            binding,
            source,
        }) => match ctx.config.error_handling.strategy {
            ErrorStrategy::FailFast => Err(PipelineError::Accumulator {
                transform,
                binding,
                source,
            }),
            ErrorStrategy::Continue | ErrorStrategy::BestEffort => emit_aggregate_finalize_dlq(
                ctx,
                name,
                attribution,
                output_schema,
                &transform,
                &binding,
                &source,
            ),
        },
        Err(other) => Err(other.into()),
    }
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
/// lets the upstream Source channel fill. Group state is bucketed per
/// [`DocumentId`]: a `DocumentClose(D)` arriving in stream order flushes
/// document D's groups before the boundary forwards at the output tail, so
/// a multi-document run emits one set of grouped rows per document. The
/// finalize half stays blocking: once the channel disconnects (every
/// sender dropped at the producer arm's clean exit), any document still
/// open (the synthetic doc id of a no-envelope pipeline, or a document
/// never closed) flushes and the finalized rows return. Document-boundary
/// punctuations are forwarded at the output tail unchanged.
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

    let expected_input = current_dag.graph[node_idx]
        .expected_input_schema_in(current_dag)
        .cloned();
    let upstream_name = current_dag.graph[producer_idx].name().to_string();

    // Build the `ctx`-free document-aggregator factory (same one the
    // materialized path uses). The scoped thread holds no
    // `&mut ExecutorContext`, so it builds and registers each document's
    // table through the factory's captured `Arc<MemoryArbitrator>`. Each
    // table's `AggregateConsumer` contributes its growing group state to
    // `sum_consumer_usage` while live, and is unregistered when that
    // document flushes (on its `DocumentClose`, or at disconnect for the
    // document left open).
    let factory = DocAggregatorFactory::from_ctx(ctx, name, agg_strategy, compiled, output_schema)?;

    // Install the bounded streaming-ingest channel keyed by the producer's
    // index, so its dispatch arm streams into it with no producer-side
    // change. The scoped thread's per-record `sub_bytes` discharge below
    // nets the producer's per-batch charge to zero.
    let (rx, charge_handle, charge_consumer_id) =
        ctx.install_streaming_ingest_channel(producer_idx);

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
    // Set once any `DocumentClose` flushes a table. The disconnect-time
    // empty-input flush owes a global fold its single defaulted row only
    // when no close already paid that debt — an all-empty-documents run
    // flushes one defaulted row per close and must not gain an extra
    // phantom row at the tail.
    let mut flushed_close = false;
    let mut agg = DocumentFlushAggregator::new(factory);

    // Run the ingest recv loop and the producer concurrently. The scoped
    // thread owns the document-flush aggregator and drains the channel; the
    // main thread redispatches the producer (which streams into the channel
    // and drops its sender at clean exit, disconnecting the channel). The
    // producer's `?` error and the ingest thread's error both surface; the
    // ingest thread always drains to disconnect first so a producer `send`
    // can never deadlock on a dead consumer.
    let finalize_ctx = ctx.merged_eval_ctx();
    let ingest_result: Result<(), PipelineError> = std::thread::scope(|scope| {
        let handle = scope.spawn(|| -> Result<(), PipelineError> {
            // The recv body is a single fallible closure so EVERY error exit
            // funnels through the one drain-then-return site below. `rx` lives
            // in the outer function frame, not in this closure, so an early
            // `?` return would NOT disconnect the channel — a producer blocked
            // on the bounded `send` would then deadlock and the join would
            // hang forever. Draining `rx` to disconnect before propagating any
            // error closes that gap structurally: no `?` site inside the
            // closure can reintroduce the non-draining asymmetry.
            let mut drive = || -> Result<(), PipelineError> {
                while let Ok(event) = rx.recv() {
                    let (record, rn) = match event {
                        StreamEvent::Record(r, rn) => (r, rn),
                        StreamEvent::Punctuation(p) => {
                            // A `DocumentClose` arriving in stream order means
                            // the open document is fully delivered: flush its
                            // groups now (its records arrived contiguously
                            // before this boundary), then forward the boundary
                            // at the output tail. A global fold owes one
                            // defaulted row per closing document even when that
                            // document contributed zero records, so force a
                            // table open before flushing an empty global-fold
                            // document — matching what a standalone empty run
                            // emits. A grouped fold owes nothing for an empty
                            // document, and a closing document that DID see
                            // records already holds an open table, so neither
                            // path gains a phantom row. Opens only forward.
                            // Punctuations carry zero charge, so no discharge
                            // here.
                            if p.kind()
                                == crate::executor::stream_event::PunctuationKind::DocumentClose
                            {
                                if agg.is_global_fold() {
                                    agg.ensure_open()?;
                                }
                                agg.flush_current(&finalize_ctx, &mut out_rows)?;
                                flushed_close = true;
                            }
                            puncts.push(p);
                            continue;
                        }
                    };
                    // Discharge this record's per-row cost — the consume half
                    // of the producer's per-batch admit. The formula matches
                    // the producer's charge so a fully-drained stream nets to
                    // zero.
                    charge_handle.sub_bytes(crate::executor::node_buffer::record_byte_cost(
                        record.schema().column_count(),
                    ));
                    input_count += 1;
                    if let Some(exp) = expected_input.as_ref() {
                        check_input_schema(
                            exp,
                            record.schema(),
                            name,
                            "aggregation",
                            &upstream_name,
                        )?;
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
                    let stream = agg.stream()?;
                    match stream.add_record(&record, rn, &eval_ctx, &mut out_rows) {
                        Ok(()) => effects.cursor_advances.push((source_name_arc, rn)),
                        Err(e) => match strategy {
                            ErrorStrategy::FailFast => return Err(e.into()),
                            ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                                effects.add_errors.push((
                                    record,
                                    rn,
                                    format!("aggregate {name}: {e}"),
                                ));
                            }
                        },
                    }
                }
                // Channel disconnected — every sender dropped at the producer
                // arm's clean exit. A completely empty stream (no records, no
                // forwarded close) opened no table; a global fold still owes
                // one defaulted row, so force one open before the final flush.
                // Gated on `!flushed_close` so an all-empty-documents run —
                // whose per-document closes already each flushed a defaulted
                // row — is not given an extra phantom row here. Then flush
                // whatever is still open (the trailing document, the synthetic
                // doc id of a no-envelope run, or the unreconciled-Merge
                // remainder) and return.
                if input_count == 0 && !flushed_close {
                    agg.ensure_open()?;
                }
                agg.flush_current(&finalize_ctx, &mut out_rows)
                    .map_err(PipelineError::from)
            };
            let result = drive();
            if result.is_err() {
                // Drain to disconnect before surfacing the error so a producer
                // blocked on the bounded `send` cannot deadlock the join. The
                // streaming-ingest arm aborts on any ingest error (schema
                // mismatch, FailFast `add_record`, finalize failure) with no
                // DLQ fallback, matching the prior single-stream behavior.
                while rx.recv().is_ok() {}
            }
            result
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
    // not leave a stale positive for the arbitrator), then unregister the
    // per-edge charge consumer. The per-document aggregate consumers are
    // released separately: a flushed document unregisters its own as it
    // finalizes, and `DocumentFlushAggregator`'s `Drop` releases any table
    // still open when `agg` falls out of scope below — including on the
    // error path, where `ingest_result?` returns before the success-path
    // replay. So no aggregate consumer survives this arm on any exit.
    charge_handle.set_bytes(0);
    ctx.streaming_charge_consumers.remove(&producer_idx);
    ctx.memory_budget.unregister_consumer(charge_consumer_id);

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

/// Shared per-record `add_record` error handler for every materialized
/// ingest arm — the strict per-document path, the relaxed-CK path, and
/// the tumbling/hopping/session windowed arms. `FailFast` surfaces the
/// error; `Continue` / `BestEffort` routes the failing record to the DLQ.
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
/// finalize-time. Shared by the strict per-document flush, the relaxed-CK
/// arm, and the tumbling/hopping/session windowed finalizers.
///
/// `records` carries the failure's source attribution: its first element
/// stamps the DLQ entry's `original_record` and `source_name`. The
/// per-document flush narrows it to a single record from the failing
/// document so the entry points at that document's file; the windowed and
/// relaxed-CK callers pass the whole batch (their failures span no single
/// document). Falls back to a synthetic record stamped with the node name
/// when `records` is empty (the failure has no real record to attribute to
/// a Source).
fn emit_aggregate_finalize_dlq(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    records: &[(Record, u64)],
    output_schema: &Arc<Schema>,
    transform: &str,
    binding: &str,
    source: &clinker_record::accumulator::AccumulatorError,
) -> Result<(), PipelineError> {
    let (synthetic, source_name) = if let Some((rec, _)) = records.first() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{DocumentContext, Value};
    use indexmap::IndexMap;

    fn record_in_document(doc_id: DocumentId, tag: &str) -> Record {
        let schema = Arc::new(Schema::new(vec!["tag".into()]));
        let mut record = Record::new(schema, vec![Value::from(tag)]);
        record.set_doc_ctx(Arc::new(DocumentContext::new(
            doc_id,
            Arc::from("file.csv"),
            IndexMap::new(),
        )));
        record
    }

    /// `document_attribution_record` narrows the batch to the first record of
    /// the named document so a finalize-time DLQ entry is stamped with that
    /// document's record, not the batch's first.
    #[test]
    fn attribution_narrows_to_the_named_document() {
        let doc_a = DocumentId::next();
        let doc_b = DocumentId::next();
        let input = vec![
            (record_in_document(doc_a, "a0"), 0),
            (record_in_document(doc_a, "a1"), 1),
            (record_in_document(doc_b, "b0"), 2),
            (record_in_document(doc_b, "b1"), 3),
        ];

        // Document B narrows to B's first record, never A's.
        let attr_b = document_attribution_record(&input, doc_b);
        assert_eq!(
            attr_b.len(),
            1,
            "narrowed to a single representative record"
        );
        assert_eq!(
            attr_b[0].0.get("tag"),
            Some(&Value::from("b0")),
            "attribution points at document B's first record",
        );

        // Document A narrows to A's first record.
        let attr_a = document_attribution_record(&input, doc_a);
        assert_eq!(
            attr_a[0].0.get("tag"),
            Some(&Value::from("a0")),
            "attribution points at document A's first record",
        );
    }

    /// A document id absent from the batch (an empty closing document, or the
    /// cross-document leftover fold) falls back to the whole slice so the DLQ
    /// emitter keeps its first-record / node-name fallback.
    #[test]
    fn attribution_falls_back_to_whole_slice_when_document_absent() {
        let doc_a = DocumentId::next();
        let absent = DocumentId::next();
        let input = vec![
            (record_in_document(doc_a, "a0"), 0),
            (record_in_document(doc_a, "a1"), 1),
        ];

        let attr = document_attribution_record(&input, absent);
        assert_eq!(
            attr.len(),
            input.len(),
            "an absent document keeps the whole slice for the existing fallback",
        );

        // The empty-input case yields an empty slice (no record to attribute).
        let empty: Vec<(Record, u64)> = Vec::new();
        assert!(document_attribution_record(&empty, absent).is_empty());
    }
}
