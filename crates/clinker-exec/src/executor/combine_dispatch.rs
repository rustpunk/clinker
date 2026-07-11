//! `PlanNode::Combine` dispatch arm.
//!
//! Holds the N-ary combine executor body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: strategy selection
//! (inline hash build/probe, grace-hash, sort-merge, IEJoin), build/probe
//! orchestration, the per-driver match/emit loop, and the recoverable
//! output-row error path. The dispatcher's `Combine` arm is a single
//! delegating call into [`dispatch_combine`].

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{Record, Value};
use cxl::eval::{EvalResult, ProgramEvaluator, SkipReason};
use cxl::typecheck::TypedProgram;
use indexmap::IndexMap;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, advance_cursor, drain_node_buffer_slot,
    finalize_node_rooted_windows, node_buffer_spill_allowed, push_dlq,
    record_error_to_buffer_if_grouped, source_file_arc_of, source_name_arc_of,
    stream_linear_producer_emit, tee_emit_to_region_input_buffers,
};
use crate::executor::schema_check::check_input_schema;
use crate::executor::{DlqEntry, NullStorage, stage_metrics, widen_record_to_schema};
use crate::pipeline::iejoin::RecordOrder;
use clinker_plan::BudgetCategory;
use clinker_plan::config::ErrorStrategy;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Cap on matches collected per driver row under `match: collect` before
/// truncation. 10K mirrors the module constants in `pipeline/combine.rs`
/// and aligns with DataFusion's collect-list bound.
const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// Execute the `Combine` arm for `node_idx`: dispatch on the planner-
/// selected [`clinker_plan::plan::combine::CombineStrategy`], drive the build and
/// probe sides, emit combined rows onto the node's widened output schema,
/// and route recoverable per-row failures through the DLQ. Blocking on the
/// build side; the probe side streams. Charges build-side state against the
/// memory arbitrator and unregisters its consumer on completion.
pub(crate) fn dispatch_combine(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Combine {
        ref name,
        ref strategy,
        ref driving_input,
        ref match_mode,
        ref on_miss,
        ref resolved_column_map,
        ref propagate_ck,
        // Typed `cxl:` body program carried on the node (like
        // `PlanTransformPayload.typed`). The runtime context carries no
        // typed-program table, so the program is read off this node
        // instance. A lowered combine with a body has `Some`; body-less
        // steps (`match: collect`, synthetic non-final N-ary steps) have
        // `None`.
        typed: ref body_typed_on_node,
        // Per-input metadata and the decomposed `where:` predicate, carried
        // on the node for the same reason as `typed`: a single name-keyed
        // side-table would be last-writer-wins across same-named combines in
        // sibling composition bodies, so reading them off this node instance
        // is what makes the runtime join scope-correct.
        combine_inputs: ref combine_inputs_on_node,
        decomposed_predicate: ref predicate_on_node,
        ..
    } = *node
    else {
        unreachable!("dispatch_combine called with non-Combine node");
    };
    use crate::executor::combine::CombineResolverMapping;
    use crate::pipeline::combine::{CombineHashTable, KeyExtractor};
    use crate::pipeline::grace_hash::{GraceHashExec, execute_combine_grace_hash};
    use crate::pipeline::iejoin::{IEJoinExec, IEJoinKernelOutput, execute_combine_iejoin};
    use crate::pipeline::sort_merge_join::{SortMergeExec, execute_combine_sort_merge};
    use clinker_plan::plan::combine::CombineStrategy;

    // Strategy dispatch up front. HashBuildProbe stays
    // inline below (the long-standing path);
    // HashPartitionIEJoin and pure-range IEJoin route
    // to the IEJoin executor; GraceHash routes to the
    // grace-hash executor; SortMerge routes to the
    // sort-merge executor; in-memory hash and the BNL
    // fallback are not yet wired.
    enum Dispatch {
        Inline,
        IEJoin(Option<u8>),
        Grace(u8),
        SortMerge,
    }
    let dispatch = match strategy {
        CombineStrategy::HashBuildProbe => Dispatch::Inline,
        CombineStrategy::HashPartitionIEJoin { partition_bits } => {
            Dispatch::IEJoin(Some(*partition_bits))
        }
        CombineStrategy::IEJoin => Dispatch::IEJoin(None),
        CombineStrategy::GraceHash { partition_bits } => Dispatch::Grace(*partition_bits),
        CombineStrategy::SortMerge => Dispatch::SortMerge,
        CombineStrategy::InMemoryHash | CombineStrategy::BlockNestedLoop => {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.clone(),
                detail: format!(
                    "combine executor does not yet implement strategy {:?}",
                    strategy
                ),
            });
        }
    };

    // Combine's widened output schema — every emitted
    // record lands on this `Arc<Schema>` so downstream
    // operators hit the ptr_eq fast path and
    // `Record::set` always addresses a known slot.
    let combine_output_schema = current_dag.graph[node_idx].stored_output_schema().cloned();

    let combine_inputs =
        combine_inputs_on_node
            .as_ref()
            .ok_or_else(|| PipelineError::Internal {
                op: "combine",
                node: name.clone(),
                detail: "no combine_inputs entry for combine node".to_string(),
            })?;
    let decomposed = predicate_on_node
        .as_ref()
        .ok_or_else(|| PipelineError::Internal {
            op: "combine",
            node: name.clone(),
            detail: "no combine_predicates entry for combine node".to_string(),
        })?;

    // E312 confines the executor to binary combines.
    // An escaped N>2 combine reaches the executor only
    // when the planner's post-pass skipped stamping —
    // that's a planner bug.
    if combine_inputs.len() != 2 {
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.clone(),
            detail: format!(
                "combine executor requires binary inputs (N=2); got {}",
                combine_inputs.len()
            ),
        });
    }
    if driving_input.is_empty() {
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.clone(),
            detail: "combine has no driving_input stamped (planner post-pass did not run)"
                .to_string(),
        });
    }

    // Identify the single build-side qualifier —
    // everything that is not the driver.
    let build_qualifier: String = combine_inputs
        .keys()
        .find(|q| q.as_str() != driving_input.as_str())
        .cloned()
        .ok_or_else(|| PipelineError::Internal {
            op: "combine",
            node: name.clone(),
            detail: "no build-side input found among combine inputs".to_string(),
        })?;
    let driver_upstream: &str = combine_inputs[driving_input.as_str()]
        .upstream_name
        .as_ref();
    let build_upstream: &str = combine_inputs[build_qualifier.as_str()]
        .upstream_name
        .as_ref();

    // Resolve predecessor buffers by upstream node name.
    // DAG edges run upstream_source -> combine (or via
    // an intermediate Transform chain). We search the
    // incoming neighbors and match by the node's name.
    //
    // `inject_correlation_sort` splices a synthetic
    // `__correlation_sort_<source>` Sort node between the
    // primary source and its downstream consumers. The
    // suffix carries the original source name; treating
    // any such Sort as an alias for its prefix-stripped
    // upstream lets the combine arm transparently resolve
    // through the splice. See `plan::execution::CORRELATION_SORT_PREFIX`.
    let predecessors: Vec<NodeIndex> = current_dag
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    let predecessor_matches = |p: NodeIndex, target: &str| -> bool {
        clinker_plan::plan::execution::matches_upstream_name(current_dag.graph[p].name(), target)
    };
    let driver_pred = predecessors
        .iter()
        .copied()
        .find(|p| predecessor_matches(*p, driver_upstream))
        .ok_or_else(|| PipelineError::Internal {
            op: "combine",
            node: name.clone(),
            detail: format!(
                "combine driver upstream {driver_upstream:?} is not an \
                         incoming neighbor in the DAG"
            ),
        })?;
    let build_pred = predecessors
        .iter()
        .copied()
        .find(|p| predecessor_matches(*p, build_upstream))
        .ok_or_else(|| PipelineError::Internal {
            op: "combine",
            node: name.clone(),
            detail: format!(
                "combine build upstream {build_upstream:?} is not an \
                         incoming neighbor in the DAG"
            ),
        })?;

    // Streaming-probe detection (issue #300). When the driver predecessor
    // is the certified probe-side streaming producer for this Combine
    // (`HashBuildProbe` only, top-level plan only — composition bodies
    // share the `NodeIndex` space but install no probe edges), the driver's
    // records are NOT pre-drained from a `node_buffers` slot. Instead the
    // build side is materialized first, then the driver producer streams
    // record-at-a-time into a bounded channel a probe thread drains. The
    // detection mirrors the Aggregate-ingest gate.
    let streaming_probe_driver: Option<NodeIndex> = if matches!(dispatch, Dispatch::Inline)
        && ctx.current_body_node_input_refs.is_none()
        && ctx
            .streaming_combine_probe_edges
            .get(&driver_pred)
            .is_some_and(|&combine| combine == node_idx)
    {
        Some(driver_pred)
    } else {
        None
    };

    // Combine collects puncts from both inputs and forwards a
    // reconciled set downstream. Like Merge, it folds the two
    // inputs' boundary signals so a document spanning both sides
    // emits one `DocumentOpen` and one `DocumentClose`; a document
    // carried by only one side forwards its single close. Reconciling
    // before forwarding keeps a downstream per-document Aggregate flush
    // from double-firing on a spanning document while still flushing
    // per side-local document.
    //
    // Under streaming-probe the driver slot is empty (the driver has not
    // dispatched yet — it streams into the probe channel during the redispatch
    // below), so this drains nothing and the driver records (and their
    // punctuations) arrive on the channel instead, to be reconciled with the
    // retained `build_puncts` after the probe joins.
    let (driver_buf, driver_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = if streaming_probe_driver.is_some() {
        (Vec::new(), Vec::new())
    } else {
        match drain_node_buffer_slot(ctx, driver_pred) {
            Some(nb) => nb.drain_split()?,
            None => (Vec::new(), Vec::new()),
        }
    };
    let (build_buf, build_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = match drain_node_buffer_slot(ctx, build_pred) {
        Some(nb) => nb.drain_split()?,
        None => (Vec::new(), Vec::new()),
    };
    // The empty-punctuation common case folds to an empty vector,
    // leaving the no-document path byte-identical to a bare union. For
    // the streaming-probe path the driver's punctuations have not arrived
    // yet (they stream over the channel below), so defer the reconcile:
    // retain `build_puncts` and rebuild `combined_puncts` once the probe
    // returns its `driver_puncts`. For every other path both sides are
    // already drained, so reconcile now.
    let mut combined_puncts: Vec<crate::executor::stream_event::Punctuation> = Vec::new();
    let mut build_puncts_retained: Vec<crate::executor::stream_event::Punctuation> = Vec::new();
    if streaming_probe_driver.is_some() {
        build_puncts_retained = build_puncts;
    } else {
        combined_puncts = crate::executor::stream_event::reconcile_document_boundaries(
            driver_puncts.into_iter().chain(build_puncts),
        );
    }

    // Operator-entry schema check per D4: every record
    // arriving on the probe and build channels is
    // validated against its upstream's compile-time
    // output schema. Arc::ptr_eq is the fast path; a
    // mismatch raises E314.
    let driver_expected = current_dag.graph[driver_pred]
        .output_schema_in(current_dag)
        .clone();
    let build_expected = current_dag.graph[build_pred]
        .output_schema_in(current_dag)
        .clone();
    for (record, _) in &driver_buf {
        check_input_schema(
            &driver_expected,
            record.schema(),
            name,
            "combine",
            driver_upstream,
        )?;
    }
    for (record, _) in &build_buf {
        check_input_schema(
            &build_expected,
            record.schema(),
            name,
            "combine",
            build_upstream,
        )?;
    }

    // Snapshot per-source cursors for every Source contributing
    // a record to this Combine's fold. Captured at fold start so
    // a Combine-output-row failure rewinds every contributing
    // source independently — driver and build sides treat
    // symmetrically. Cleared at every Combine exit (inline,
    // IEJoin, Grace, SortMerge); the recoverable-DLQ rewind
    // path restores from this entry before clearing. Setup-time
    // `PipelineError::Internal { op: "combine" }` invariant
    // violations still fail-fast and bypass the rewind.
    let mut combine_snapshot: HashMap<Arc<str>, u64> = HashMap::new();
    for (rec, _) in driver_buf.iter().chain(build_buf.iter()) {
        let sn = source_name_arc_of(rec);
        let cursor = ctx.rollback_cursors.get(&sn).copied().unwrap_or(0);
        combine_snapshot.entry(sn).or_insert(cursor);
    }
    ctx.combine_input_snapshots
        .insert(node_idx, combine_snapshot);

    // Build the KeyExtractor pair: one side aligned to
    // the build qualifier, the other to the probe. The
    // i-th equality conjunct contributes one key column
    // to each extractor — the side that matches the
    // build qualifier feeds the build extractor; the
    // OTHER side feeds the probe extractor. For an
    // N-ary chain step, the probe-side qualifier may be
    // a chain-buried original qualifier (e.g. `b` in a
    // step that joins `__combine_X_step_0` against
    // `c`); the executor accepts any non-build
    // qualifier as probe-side and routes its lookup
    // through the resolver mapping below.
    let mut build_progs: Vec<(Arc<TypedProgram>, cxl::ast::Expr)> = Vec::new();
    let mut probe_progs: Vec<(Arc<TypedProgram>, cxl::ast::Expr)> = Vec::new();
    for eq in &decomposed.equalities {
        let (build_expr, build_prog, probe_expr, probe_prog) =
            if eq.left_input.as_ref() == build_qualifier.as_str() {
                (
                    eq.left_expr.clone(),
                    Arc::clone(&eq.left_program),
                    eq.right_expr.clone(),
                    Arc::clone(&eq.right_program),
                )
            } else if eq.right_input.as_ref() == build_qualifier.as_str() {
                (
                    eq.right_expr.clone(),
                    Arc::clone(&eq.right_program),
                    eq.left_expr.clone(),
                    Arc::clone(&eq.left_program),
                )
            } else {
                // Neither side matches the build
                // qualifier — the plan-time
                // decomposition placed a foreign
                // conjunct into `equalities`. Planner
                // bug.
                return Err(PipelineError::Internal {
                    op: "combine",
                    node: name.clone(),
                    detail: format!(
                        "equality conjunct has qualifiers ({}, {}); \
                                 neither matches build qualifier {build_qualifier:?}",
                        eq.left_input, eq.right_input,
                    ),
                });
            };
        build_progs.push((build_prog, build_expr));
        probe_progs.push((probe_prog, probe_expr));
    }
    let build_extractor = KeyExtractor::new(build_progs);
    let probe_extractor = KeyExtractor::new(probe_progs);

    // Resolver mapping is built once and reused for
    // every probe iteration below. The `(side, u32)`
    // index pairs come from the CXL typechecker's
    // pre-resolved column map stashed on the PlanNode;
    // the `bare_to_side` fallback is derived here from
    // `combine_inputs` so unambiguous bare names keep
    // resolving after the resolver is constructed.
    let resolver_mapping =
        CombineResolverMapping::from_pre_resolved(resolved_column_map, combine_inputs);

    // IEJoin / HashPartitionIEJoin / GraceHash
    // dispatch: prep is identical to HashBuildProbe
    // up to this point (input fetch + schema check +
    // resolver mapping), but the matching kernel is
    // strategy-specific.
    match dispatch {
        Dispatch::IEJoin(partition_bits) => {
            // Register an IEJoin consumer with the pipeline-
            // scoped arbitrator under `SortConsumer` (priority
            // 20), sharing a `ConsumerHandle` with the kernel. The
            // kernel mirrors its live working-set bytes (the
            // block-band path's external-sort buffers and resident
            // blocks; the equi+range path's routed indices and
            // per-group L1/L2 / permutation / bit state) into this
            // handle, so the arbitrator's pull-mode `current_usage`
            // reads the live footprint while the join runs. The
            // pure-range block-band path is threshold-driven: it
            // spills its sorted runs and min/max-tagged blocks when
            // the buffer exceeds the soft limit, so the handle also
            // drives the kernel's `should_abort_local` gate, now a
            // genuine last resort for a single block-pair plus kernel
            // aux that still cannot fit.
            let ie_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
            let ie_consumer_id = ctx.memory_budget.register_consumer(Arc::new(
                crate::pipeline::sort_buffer::SortConsumer::new(ie_consumer_handle.clone()),
            ));
            // Every exit past this registration must unregister the
            // consumer, so the kernel-install-through-admit body runs
            // inside a closure whose Result is captured: the clean return
            // and every `?` early-return funnel through the single
            // `unregister_consumer` call below, so a finished IEJoin branch
            // never lingers in the arbitrator's registry.
            let result = (|| -> Result<(), PipelineError> {
                // Advance per-source `rollback_cursors` for every
                // build-side record before its `row_num` is dropped
                // in the `(r, _)` map. Source→Combine direct paths
                // (no intermediate Transform/Aggregate to advance the
                // cursor on the way through) would otherwise leave
                // the build source's cursor anchored at zero. Same
                // pass advances driver-side cursors for symmetry —
                // a Source→Combine direct driver has the same gap.
                for (rec, rn) in &driver_buf {
                    advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                }
                for (rec, rn) in &build_buf {
                    advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                }
                let build_records: Vec<Record> = build_buf.into_iter().map(|(r, _)| r).collect();
                let build_records_in = build_records.len() as u64;
                let build_timer =
                    stage_metrics::StageTimer::new(stage_metrics::StageName::CombineBuild {
                        name: name.clone(),
                    });
                let build_records_out = build_records.len() as u64;
                ctx.collector
                    .record(build_timer.finish(build_records_in, build_records_out));
                let probe_records_in = driver_buf.len() as u64;
                let probe_timer =
                    stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
                        name: name.clone(),
                    });
                let body_typed = body_typed_on_node.as_ref();
                let combine_output_schema_arc = combine_output_schema.clone();
                // Resolve the spill compression mode against the combine's output
                // schema width and the run's batch size, so the block-band path's
                // spill runs and block files match what `--explain` projects.
                // `auto` skips LZ4 on narrow combines where the per-frame fixed
                // cost outweighs the savings. The equi+range (`Some`) path never
                // spills, so this is inert there.
                let ie_column_count = combine_output_schema_arc
                    .as_ref()
                    .map(|s| s.column_count())
                    .unwrap_or(0);
                let ie_spill_compress = ctx
                    .spill_compress
                    .resolve_for_schema(ie_column_count, ctx.batch_size as u64);
                let iejoin_ctx = ctx.merged_eval_ctx();
                // CPU-bound IEJoin kernel — partition + range-walk +
                // materialize. The kernel owns its inputs
                // (`driver_buf`, `build_records`) and borrows only
                // `&ctx.memory_budget` + the local `iejoin_ctx`, so
                // it runs on the shared Rayon pool. Row order is
                // determined by the kernel's deterministic
                // partition-then-merge, not by pool scheduling.
                let kernel_out = ctx.kernel_pool.install(|| {
                    execute_combine_iejoin(IEJoinExec {
                        name,
                        build_qualifier: &build_qualifier,
                        driver_records: driver_buf,
                        build_records,
                        decomposed,
                        body_program: body_typed,
                        resolver_mapping: &resolver_mapping,
                        output_schema: combine_output_schema_arc.as_ref(),
                        match_mode: *match_mode,
                        on_miss: *on_miss,
                        partition_bits,
                        propagate_ck,
                        ctx: &iejoin_ctx,
                        budget: &ctx.memory_budget,
                        consumer: &ie_consumer_handle,
                        spill_dir: ctx.spill_root_path.as_ref(),
                        spill_compress: ie_spill_compress,
                        strategy: ctx.strategy,
                    })
                })?;
                // Route each deferred output-stage eval failure through the same
                // `combine_output_row` path the inline arm uses. This MUST run
                // before the snapshot is dropped below —
                // `dispatch_combine_output_error` reads the installed pre-fold
                // snapshot to rewind each contributing source's rollback cursor.
                // Both dispatch shapes carry the same failure vector, so it is
                // routed first, before the shape-specific row handling.
                let nb = match kernel_out {
                    IEJoinKernelOutput::Materialized(kernel) => {
                        // Equi+range: rows are already materialized in
                        // kernel/bucket visitation order; admit the whole vec.
                        dispatch_combine_output_errors(
                            ctx,
                            node_idx,
                            name,
                            kernel.output_eval_failures,
                        )?;
                        let output_records = kernel.records;
                        let probe_records_out = output_records.len() as u64;
                        ctx.collector
                            .record(probe_timer.finish(probe_records_in, probe_records_out));
                        finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
                        tee_emit_to_region_input_buffers(
                            ctx,
                            current_dag,
                            node_idx,
                            &output_records,
                        )?;
                        admit_node_buffer(
                            ctx,
                            name,
                            node_idx,
                            output_records,
                            combined_puncts,
                            node_buffer_spill_allowed(current_dag, node_idx),
                        )?
                    }
                    IEJoinKernelOutput::BlockBand(kernel) => {
                        // Pure-range: drain the bounded, payload-sorted output
                        // handle incrementally. When a downstream streaming
                        // Output certified this combine as its producer, the
                        // drain streams straight through the back-pressure sink
                        // so the output axis stays bounded end-to-end;
                        // otherwise it folds into a spillable node-buffer a
                        // blocking downstream drains.
                        dispatch_combine_output_errors(
                            ctx,
                            node_idx,
                            name,
                            kernel.output_eval_failures,
                        )?;
                        match drain_block_band_output(
                            ctx,
                            current_dag,
                            node_idx,
                            name,
                            kernel.sorted,
                            combined_puncts,
                        )? {
                            BlockBandDrain::Streamed(count) => {
                                // The rows already streamed to the writer
                                // thread, so there is no node-buffer to admit.
                                // Record the probe timer, drop the per-fold
                                // snapshot as every clean exit does, and return
                                // before the shared `node_buffers.insert` tail.
                                ctx.collector
                                    .record(probe_timer.finish(probe_records_in, count));
                                ctx.combine_input_snapshots.remove(&node_idx);
                                return Ok(());
                            }
                            BlockBandDrain::Buffered(nb) => {
                                let probe_records_out = nb.len_hint() as u64;
                                ctx.collector.record(
                                    probe_timer.finish(probe_records_in, probe_records_out),
                                );
                                nb
                            }
                        }
                    }
                };
                ctx.node_buffers.insert(node_idx, nb);
                // Combine arm clean-exit: drop the per-fold cursor
                // snapshot. Every emitted record has cleared into
                // `node_buffers` without rewind, so the snapshot is
                // no longer needed. Mirrors the inline arm's
                // post-emit clear.
                ctx.combine_input_snapshots.remove(&node_idx);
                Ok(())
            })();
            // The pre-output working set is freed once the kernel returns;
            // zero the handle before unregister so a policy poll in the
            // teardown window never reads a stale footprint for a consumer
            // that no longer holds any bytes.
            ie_consumer_handle.set_bytes(0);
            ctx.memory_budget.unregister_consumer(ie_consumer_id);
            return result;
        }
        Dispatch::Grace(partition_bits) => {
            // Single ConsumerHandle shared between the
            // GraceHashConsumer wrapper (Priority policy
            // priority 10) and the GraceHashExecutor that
            // mirrors its in-memory partition `bytes_estimated`
            // sum into the handle's counter on every admit /
            // spill_partition transition.
            let grace_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
            let grace_consumer_id = ctx.memory_budget.register_consumer(Arc::new(
                crate::pipeline::grace_hash::GraceHashConsumer::new(grace_consumer_handle.clone()),
            ));
            // Every exit past this registration must unregister the
            // consumer, so the kernel-install-through-admit body runs
            // inside a closure whose Result is captured: the clean return
            // and every `?` early-return funnel through the single
            // `unregister_consumer` call below, so a finished grace-hash
            // branch never lingers in the arbitrator's registry.
            let result = (|| -> Result<(), PipelineError> {
                // Same per-source cursor advance the IEJoin arm
                // performs; Source→Combine direct paths on either
                // side need the explicit walk because the build /
                // driver bufs flow directly out of `node_buffers`
                // with no operator in between to advance.
                for (rec, rn) in &driver_buf {
                    advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                }
                for (rec, rn) in &build_buf {
                    advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                }
                let build_records: Vec<Record> = build_buf.into_iter().map(|(r, _)| r).collect();
                let build_records_in = build_records.len() as u64;
                let build_timer =
                    stage_metrics::StageTimer::new(stage_metrics::StageName::CombineBuild {
                        name: name.clone(),
                    });
                let build_records_out = build_records.len() as u64;
                ctx.collector
                    .record(build_timer.finish(build_records_in, build_records_out));
                let probe_records_in = driver_buf.len() as u64;
                let probe_timer =
                    stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
                        name: name.clone(),
                    });
                let body_typed = body_typed_on_node.as_ref();
                let combine_output_schema_arc = combine_output_schema.clone();
                // Resolve the spill compression mode against the combine's output
                // schema width and the run's batch size, so the grace-hash
                // partition spill files match what `--explain` projects. `auto`
                // skips LZ4 on narrow combines where the per-frame fixed cost
                // outweighs the savings.
                let grace_column_count = combine_output_schema_arc
                    .as_ref()
                    .map(|s| s.column_count())
                    .unwrap_or(0);
                let grace_spill_compress = ctx
                    .spill_compress
                    .resolve_for_schema(grace_column_count, ctx.batch_size as u64);
                let grace_ctx = ctx.merged_eval_ctx();
                // CPU-bound grace-hash join kernel: partition build +
                // probe + spill I/O. The kernel owns its inputs and
                // borrows only `&ctx.memory_budget`, the local
                // `grace_ctx`, and the spill dir, so it runs on the
                // shared Rayon pool. Emitted-row order is fixed by the
                // kernel's deterministic partition-then-probe walk.
                let kernel_out = ctx.kernel_pool.install(|| {
                    execute_combine_grace_hash(GraceHashExec {
                        name,
                        build_qualifier: &build_qualifier,
                        driver_records: driver_buf,
                        build_records,
                        decomposed,
                        body_program: body_typed,
                        resolver_mapping: &resolver_mapping,
                        output_schema: combine_output_schema_arc.as_ref(),
                        match_mode: *match_mode,
                        on_miss: *on_miss,
                        partition_bits,
                        propagate_ck,
                        ctx: &grace_ctx,
                        budget: &ctx.memory_budget,
                        spill_dir: ctx.spill_root_path.as_ref(),
                        spill_compress: grace_spill_compress,
                        consumer_handle: grace_consumer_handle,
                        strategy: ctx.strategy,
                        stats_sink: crate::pipeline::grace_hash::GraceStatsSink {
                            catalog: std::sync::Arc::clone(&ctx.runtime_statistics),
                            node: build_upstream,
                            column: &build_qualifier,
                        },
                    })
                })?;
                let crate::pipeline::combine::CombineKernelOutput {
                    records: output_records,
                    output_eval_failures,
                } = kernel_out;
                // Route each deferred output-stage eval failure through
                // the `combine_output_row` path while the pre-fold
                // snapshot is still installed (the rewind reads it).
                for f in output_eval_failures {
                    dispatch_combine_output_error(
                        ctx,
                        node_idx,
                        &f.probe_record,
                        f.row,
                        f.matched_build.as_ref(),
                        name,
                        f.error,
                    )?;
                }
                let probe_records_out = output_records.len() as u64;
                ctx.collector
                    .record(probe_timer.finish(probe_records_in, probe_records_out));
                finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
                tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
                let nb = admit_node_buffer(
                    ctx,
                    name,
                    node_idx,
                    output_records,
                    combined_puncts,
                    node_buffer_spill_allowed(current_dag, node_idx),
                )?;
                ctx.node_buffers.insert(node_idx, nb);
                // Combine arm clean-exit: drop the per-fold cursor
                // snapshot. Mirrors the inline arm's post-emit
                // clear so non-inline strategies do not leak
                // stale snapshot entries across runs.
                ctx.combine_input_snapshots.remove(&node_idx);
                Ok(())
            })();
            ctx.memory_budget.unregister_consumer(grace_consumer_id);
            return result;
        }
        Dispatch::SortMerge => {
            // Single ConsumerHandle shared between the
            // SortMergeConsumer wrapper (Priority policy
            // priority 25) and the SortMergeExec kernel that
            // mirrors Phase A `SortBuffer.bytes_used` + Phase B
            // matching-run accumulator size into the handle's
            // counter at every push / spill transition.
            let sm_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
            let sm_consumer_id = ctx.memory_budget.register_consumer(Arc::new(
                crate::pipeline::sort_merge_join::SortMergeConsumer::new(
                    sm_consumer_handle.clone(),
                ),
            ));
            // Every exit past this registration must unregister the
            // consumer, so the kernel-install-through-admit body runs
            // inside a closure whose Result is captured: the clean return
            // and every `?` early-return funnel through the single
            // `unregister_consumer` call below, so a finished sort-merge
            // branch never lingers in the arbitrator's registry.
            let result = (|| -> Result<(), PipelineError> {
                // SortMerge is selected by the planner only
                // for pure-range predicates whose inputs
                // already arrive sorted on the range key
                // prefix. The kernel's `presorted: true`
                // path skips Phase A external sort and walks
                // the inputs in place via the two-cursor
                // merge.
                // Same per-source cursor advance the IEJoin /
                // Grace arms perform; Source→Combine direct paths
                // on either side need the explicit walk because
                // the build / driver bufs flow directly out of
                // `node_buffers` with no operator in between to
                // advance.
                for (rec, rn) in &driver_buf {
                    advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                }
                for (rec, rn) in &build_buf {
                    advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                }
                let build_records: Vec<Record> = build_buf.into_iter().map(|(r, _)| r).collect();
                let build_records_in = build_records.len() as u64;
                let build_timer =
                    stage_metrics::StageTimer::new(stage_metrics::StageName::CombineBuild {
                        name: name.clone(),
                    });
                let build_records_out = build_records.len() as u64;
                ctx.collector
                    .record(build_timer.finish(build_records_in, build_records_out));
                let probe_records_in = driver_buf.len() as u64;
                let probe_timer =
                    stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
                        name: name.clone(),
                    });
                let body_typed = body_typed_on_node.as_ref();
                let combine_output_schema_arc = combine_output_schema.clone();
                // Resolve the spill compression mode against the combine's output
                // schema width and the run's batch size, so Phase A spill runs
                // match what `--explain` projects. `auto` skips LZ4 on narrow
                // combines where the per-frame fixed cost outweighs the savings.
                let sm_column_count = combine_output_schema_arc
                    .as_ref()
                    .map(|s| s.column_count())
                    .unwrap_or(0);
                let sm_spill_compress = ctx
                    .spill_compress
                    .resolve_for_schema(sm_column_count, ctx.batch_size as u64);
                let sm_ctx = ctx.merged_eval_ctx();
                // CPU-bound sort-merge join kernel: two-cursor merge
                // over pre-sorted inputs. The kernel owns its inputs
                // and borrows only `&ctx.memory_budget`, the local
                // `sm_ctx`, and the spill dir, so it runs on the
                // shared Rayon pool. The two-cursor merge emits in a
                // deterministic order independent of pool scheduling.
                let kernel_out = ctx.kernel_pool.install(|| {
                    execute_combine_sort_merge(SortMergeExec {
                        name,
                        build_qualifier: &build_qualifier,
                        driver_records: driver_buf,
                        build_records,
                        decomposed,
                        body_program: body_typed,
                        resolver_mapping: &resolver_mapping,
                        output_schema: combine_output_schema_arc.as_ref(),
                        match_mode: *match_mode,
                        on_miss: *on_miss,
                        presorted: true,
                        propagate_ck,
                        ctx: &sm_ctx,
                        budget: &ctx.memory_budget,
                        spill_dir: ctx.spill_root_path.as_ref(),
                        spill_compress: sm_spill_compress,
                        consumer_handle: sm_consumer_handle,
                        strategy: ctx.strategy,
                    })
                })?;
                let crate::pipeline::combine::CombineKernelOutput {
                    records: output_records,
                    output_eval_failures,
                } = kernel_out;
                // Route each deferred output-stage eval failure through
                // the `combine_output_row` path while the pre-fold
                // snapshot is still installed (the rewind reads it).
                for f in output_eval_failures {
                    dispatch_combine_output_error(
                        ctx,
                        node_idx,
                        &f.probe_record,
                        f.row,
                        f.matched_build.as_ref(),
                        name,
                        f.error,
                    )?;
                }
                let probe_records_out = output_records.len() as u64;
                ctx.collector
                    .record(probe_timer.finish(probe_records_in, probe_records_out));
                finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
                tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
                let nb = admit_node_buffer(
                    ctx,
                    name,
                    node_idx,
                    output_records,
                    combined_puncts,
                    node_buffer_spill_allowed(current_dag, node_idx),
                )?;
                ctx.node_buffers.insert(node_idx, nb);
                // Combine arm clean-exit: drop the per-fold cursor
                // snapshot. Mirrors the inline arm's post-emit
                // clear.
                ctx.combine_input_snapshots.remove(&node_idx);
                Ok(())
            })();
            ctx.memory_budget.unregister_consumer(sm_consumer_id);
            return result;
        }
        Dispatch::Inline => {}
    }

    // Single ConsumerHandle shared between the
    // CombineHashConsumer wrapper (Priority policy
    // priority 30) and the post-build mirroring step below
    // that sets the handle's counter to the freshly-built
    // CombineHashTable's `memory_bytes()`. The wrapper's
    // ConsumerId is unregistered at arm exit so the
    // arbitrator's registry tracks live tables only.
    let inline_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
    let inline_consumer_id = ctx.memory_budget.register_consumer(Arc::new(
        crate::pipeline::combine::CombineHashConsumer::new(inline_consumer_handle.clone()),
    ));
    // Every exit past this registration must unregister the
    // consumer, so the hash-build-through-admit body runs inside a
    // closure whose Result is captured: the clean return, the
    // streaming-handoff early return, and every `?` early-return
    // funnel through the single `unregister_consumer` call below, so
    // a finished inline hash-combine never lingers in the arbitrator's
    // registry.
    let result = (|| -> Result<(), PipelineError> {
        // Hash-build phase — drain the build buffer into the
        // pipeline-scoped MemoryArbitrator-governed
        // CombineHashTable. The stage timer covers the full
        // build walk; on a budget abort the timer is dropped
        // without recording (matches the `StageTimer` "no
        // report on error" contract documented at its
        // definition). Arc-clone the pipeline-scoped handle
        // so subsequent `&mut ctx` borrows for cursor advance
        // are not blocked by an immutable borrow of `ctx`.
        let budget = ctx.memory_budget.clone();
        // Per-source cursor advance for both build and driver.
        // Source→Combine direct paths bypass the Transform /
        // Aggregate advance points so the cursor never moved off
        // zero for those records; this is the operator-entry
        // advance that catches the gap. The inline arm's driver
        // loop below does not advance per-row inline, so this is
        // the single advance point for driver and build alike.
        for (rec, rn) in &driver_buf {
            advance_cursor(ctx, &source_name_arc_of(rec), *rn);
        }
        for (rec, rn) in &build_buf {
            advance_cursor(ctx, &source_name_arc_of(rec), *rn);
        }
        let build_records: Vec<Record> = build_buf.into_iter().map(|(r, _)| r).collect();
        let build_records_in = build_records.len() as u64;
        let estimated_rows = Some(build_records.len());
        let hash_table_ctx = ctx.merged_eval_ctx();
        let build_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::CombineBuild {
            name: name.clone(),
        });
        let hash_table = CombineHashTable::build(
            build_records,
            &build_extractor,
            &hash_table_ctx,
            &budget,
            estimated_rows,
        )
        .map_err(|e| PipelineError::MemoryBudgetExceeded {
            node: name.clone(),
            used: budget.peak_rss().unwrap_or(0),
            limit: budget.hard_limit(),
            source: BudgetCategory::Arena,
            detail: Some(format!("combine build: {e}")),
        })?;
        let build_records_out = hash_table.len() as u64;
        // Mirror the freshly-built table's footprint into the
        // consumer handle so the arbitrator's pull-mode
        // `current_usage` reads the inline-combine's in-memory
        // bytes for the duration of the probe loop. The probe
        // loop is read-only on the table so no further updates
        // are needed; arm exit below unregisters the consumer.
        inline_consumer_handle.set_bytes(hash_table.memory_bytes() as u64);
        ctx.collector
            .record(build_timer.finish(build_records_in, build_records_out));

        // Body typed program (only used when the body is not empty —
        // `match: collect` and body-less synthetic N-ary steps leave it
        // `None`). Read off the node; shared with the streaming-probe thread
        // via the kernel.
        let body_program = body_typed_on_node.clone();

        // The probe matching kernel — owns the materialized hash table and
        // every per-combine artifact the probe reads, and holds no
        // `&mut ExecutorContext`. The materialized loop below and the
        // streaming-probe thread both drive it, so the two paths return a
        // byte-identical result set.
        let kernel = CombineProbeKernel {
            name,
            hash_table: &hash_table,
            resolver_mapping: &resolver_mapping,
            probe_extractor: &probe_extractor,
            decomposed,
            body_program,
            combine_output_schema: combine_output_schema.clone(),
            build_qualifier: &build_qualifier,
            match_mode: *match_mode,
            on_miss: *on_miss,
            propagate_ck,
            fail_fast: ctx.strategy == ErrorStrategy::FailFast,
        };

        // Per-driver probe loop. Stage timer covers the full per-driver
        // iteration; on early-return via the 10K-cadence E310 abort or a
        // residual/body eval error, the timer is dropped without recording.
        let mut probe_records_in = driver_buf.len() as u64;
        let probe_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
            name: name.clone(),
        });
        let output_records: Vec<(Record, u64)> =
            if let Some(driver_producer) = streaming_probe_driver {
                // Streaming-probe path: the build side is materialized into the
                // hash table above; now spawn the probe consumer on its own thread,
                // redispatch the driver producer to stream into the bounded channel,
                // and collect the probe output. The kernel + `&budget` move into the
                // thread; counter/cursor/DLQ effects replay onto `ctx` after the
                // join inside the helper.
                let streamed = run_streaming_combine_probe(
                    ctx,
                    current_dag,
                    node_idx,
                    driver_producer,
                    name,
                    &kernel,
                    &budget,
                )?;
                // The driver streamed its records over the channel, so the input
                // count comes from the probe thread rather than a pre-drained Vec.
                probe_records_in = streamed.input_count;
                // The driver's punctuations arrived over the channel during the
                // probe and were collected by the probe thread. Reconcile them now
                // with the retained build-side punctuations — same fold as the
                // materialized path, so a document spanning both inputs collapses
                // to one open + one close and a side-local document forwards its
                // close.
                combined_puncts = crate::executor::stream_event::reconcile_document_boundaries(
                    streamed
                        .driver_puncts
                        .into_iter()
                        .chain(std::mem::take(&mut build_puncts_retained)),
                );
                streamed.output_records
            } else {
                let mut output_records = Vec::with_capacity(driver_buf.len());
                let mut emitted_since_check: usize = 0;
                let mut probe_counters = ProbeCounters::default();
                // Reused across every probe iteration to avoid an allocation per
                // driver row. `KeyExtractor::extract_into` pushes onto the end; the
                // kernel clears it before each call.
                let mut probe_keys_buf: Vec<Value> = Vec::with_capacity(probe_extractor.len());

                for (probe_record, rn) in driver_buf {
                    let source_file_arc = source_file_arc_of(&probe_record);
                    let source_name_arc = source_name_arc_of(&probe_record);
                    let before = output_records.len();
                    let eval_ctx = ctx.eval_ctx_for_record(
                        &source_file_arc,
                        &source_name_arc,
                        rn,
                        probe_record.doc_ctx(),
                    );
                    let step = kernel.probe_row(
                        &eval_ctx,
                        &probe_record,
                        rn,
                        &mut probe_keys_buf,
                        &mut probe_counters,
                        &mut output_records,
                    )?;
                    if let ProbeRowStep::Deferred(f) = step {
                        let f = *f;
                        dispatch_combine_output_error(
                            ctx,
                            node_idx,
                            &f.probe_record,
                            f.rn,
                            f.matched_build.as_ref(),
                            name,
                            f.error,
                        )?;
                        // A deferred failure routes the whole driver row to the DLQ;
                        // no output rows survive for it.
                        output_records.truncate(before);
                        continue;
                    }
                    emitted_since_check += output_records.len() - before;

                    // Budget check every 10K emitted records to bound memory under
                    // fan-out. The build phase polls `should_abort` every 10K
                    // inserts inside `CombineHashTable::build`; this covers the
                    // symmetric probe-side risk where a small build × large driver
                    // fan-out can blow RSS even though the table itself is bounded.
                    if emitted_since_check >= 10_000 && budget.should_abort() {
                        return Err(PipelineError::MemoryBudgetExceeded {
                            node: name.clone(),
                            used: budget.peak_rss().unwrap_or(0),
                            limit: budget.hard_limit(),
                            source: BudgetCategory::Arena,
                            detail: Some("combine probe RSS abort".to_string()),
                        });
                    }
                    if emitted_since_check >= 10_000 {
                        emitted_since_check = 0;
                    }
                }

                // Fold the probe kernel's `distinct` / `filtered` skip counts into
                // the run counters — applied after the loop, the same place the
                // per-row `ctx.counters.*` increments used to live.
                ctx.counters.filtered_count += probe_counters.filtered;
                ctx.counters.distinct_count += probe_counters.distinct;
                output_records
            };

        let probe_records_out = output_records.len() as u64;
        ctx.collector
            .record(probe_timer.finish(probe_records_in, probe_records_out));

        // Streaming-Output handoff: an inline hash build-probe combine
        // whose sole downstream is a streaming-eligible Output
        // installed its sender under our `node_idx`. The build side is
        // already fully materialized in the hash table and the whole
        // probe emit is already collected into `output_records`, so
        // this does not shrink the combine's own working set; what it
        // saves is the second copy — hand `output_records` straight to
        // the writer thread over the bounded channel rather than
        // admitting a charged `node_buffers` slot the Output would
        // re-drain, and overlap the writer with the next topo node.
        // The eligibility predicate certified this combine roots no
        // window and tees to no deferred region, so the helper calls
        // below are correctly skipped. The reconciled document boundaries
        // are forwarded on the inline path just like the materialized arms,
        // so a per-document Aggregate reached through this streamed handoff
        // flushes per document; a terminal writer downstream consumes them
        // harmlessly. The per-fold snapshot is dropped here; the
        // hash-table consumer is released by the shared unregister
        // funnel every exit from this arm passes through.
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
                output_records,
                combined_puncts,
                &charge,
            )?;
            ctx.combine_input_snapshots.remove(&node_idx);
            return Ok(());
        }

        finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
        tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
        let nb = admit_node_buffer(
            ctx,
            name,
            node_idx,
            output_records,
            combined_puncts,
            node_buffer_spill_allowed(current_dag, node_idx),
        )?;
        ctx.node_buffers.insert(node_idx, nb);
        // Drop the per-fold cursor snapshot: every emitted record
        // has cleared into `node_buffers` without rewind, so the
        // snapshot is no longer needed. The rewind path on a
        // Combine-output-row failure reads and restores this entry
        // before clearing.
        ctx.combine_input_snapshots.remove(&node_idx);
        Ok(())
    })();
    ctx.memory_budget.unregister_consumer(inline_consumer_id);
    result
}

/// Result of a streaming-probe run handed back to [`dispatch_combine`]:
/// the probe output rows, the count of driver records the producer fed
/// over the channel (the streaming analogue of the materialized
/// `driver_buf.len()` probe input), and the driver's document-boundary
/// punctuations collected off the channel for post-join reconciliation
/// with the build side.
struct StreamingProbeOutput {
    output_records: Vec<(Record, u64)>,
    input_count: u64,
    driver_puncts: Vec<crate::executor::stream_event::Punctuation>,
}

/// Per-record side effects the streaming-probe thread cannot apply directly
/// (it holds no `&mut ExecutorContext`), replayed onto the dispatcher after
/// the probe scope joins. Ordering within each vector is channel-arrival
/// order, so the replay reproduces the materialized path's cursor / DLQ
/// sequencing exactly.
struct StreamingProbeEffects {
    /// `(source_name, row_num)` for each driver record the probe consumed —
    /// replayed via [`advance_cursor`], the streaming analogue of the
    /// materialized arm's operator-entry driver cursor advance.
    cursor_advances: Vec<(Arc<str>, u64)>,
    /// Distinct driver source names the probe consumed, in first-seen
    /// order. After the join — once the producer has fully advanced its
    /// sources but before the deferred combine advances replay — the
    /// dispatch thread reads each source's live cursor as its pre-fold
    /// floor, matching the materialized path's fold-start snapshot timing.
    driver_sources: Vec<Arc<str>>,
    /// Recoverable per-row failures, replayed via [`dispatch_combine_output_error`]
    /// after join (cursor rewind + DLQ) — matching the inline arm's per-row
    /// routing in arrival order. `FailFast` surfaces eagerly instead.
    failures: Vec<ProbeFailure>,
}

/// Drive an inline hash build-probe Combine's probe (driver) side off a
/// bounded channel the driver producer fills, instead of pre-draining the
/// driver's whole output from a charged `node_buffers` slot. The build-side
/// hash table is already materialized inside `kernel` before this runs.
///
/// Build-before-probe is the deadlock-free invariant this enforces: the
/// hash table is complete (it lives in `kernel`) before the driver producer
/// is redispatched, so the probe never reads an incomplete table. The probe
/// consumer runs on its OWN scoped thread and is live before the driver's
/// first `send`, so the bounded channel can never deadlock — a slow driver
/// stalls on a full channel and the probe paces it; a slow probe (large
/// fan-out) back-pressures the driver → Source. The finalize is trivial
/// (the hash table is read-only), so there is no blocking finalize half.
///
/// The probe thread holds no `&mut ExecutorContext`; it accumulates cursor
/// advances, driver-snapshot floors, and DLQ failures into
/// [`StreamingProbeEffects`], replayed on the dispatch thread after the
/// scope joins so rollback cursors, the per-fold rewind snapshot, and DLQ
/// sequencing match the drain-to-`Vec` path exactly. Mid-stream
/// `$source.count` is `None` (the driver total is unknown until disconnect),
/// the same defer-emit semantic the streaming Aggregate ingest uses.
#[allow(clippy::too_many_arguments)]
fn run_streaming_combine_probe(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    producer_idx: NodeIndex,
    name: &str,
    kernel: &CombineProbeKernel<'_>,
    budget: &crate::pipeline::memory::MemoryArbitrator,
) -> Result<StreamingProbeOutput, PipelineError> {
    use crate::executor::stream_event::StreamEvent;
    use cxl::eval::EvalContext;

    let expected_input = current_dag.graph[producer_idx]
        .output_schema_in(current_dag)
        .clone();
    let upstream_name = current_dag.graph[producer_idx].name().to_string();

    // Install the bounded streaming-ingest channel keyed by the driver
    // producer's index, so its dispatch arm streams into it with no
    // producer-side change. The probe thread's per-record `sub_bytes`
    // discharge nets the producer's per-batch charge to zero.
    let (rx, charge_handle, charge_consumer_id) =
        ctx.install_streaming_ingest_channel(producer_idx);

    // Copy the stable-context references out of `ctx` before the scope so
    // the probe thread borrows `&'a StableEvalContext` directly (shared,
    // `Sync`) rather than through the `&mut ctx` the main thread holds for
    // the driver redispatch — the two borrows are disjoint.
    let stable = ctx.stable;
    let source_batch_arc = ctx.source_batch_arc;
    let ingestion_timestamp = ctx.source_ingestion_timestamp;

    let mut effects = StreamingProbeEffects {
        cursor_advances: Vec::new(),
        driver_sources: Vec::new(),
        failures: Vec::new(),
    };
    let mut output_records: Vec<(Record, u64)> = Vec::new();
    let mut driver_puncts: Vec<crate::executor::stream_event::Punctuation> = Vec::new();
    let mut counters = ProbeCounters::default();
    let mut input_count: u64 = 0;

    // Run the probe recv loop and the driver producer concurrently. The
    // scoped thread owns the channel drain + probe; the main thread
    // redispatches the driver (which streams into the channel and drops its
    // sender at clean exit, disconnecting the channel). Both the driver's
    // `?` error and the probe thread's error surface; the probe thread
    // always drains to disconnect first so a driver `send` can never
    // deadlock on a dead consumer.
    let probe_result: Result<(), PipelineError> = std::thread::scope(|scope| {
        let handle = scope.spawn(|| -> Result<(), PipelineError> {
            let mut probe_keys_buf: Vec<Value> = Vec::with_capacity(kernel.probe_extractor.len());
            let mut budget_cadence: usize = 0;
            while let Ok(event) = rx.recv() {
                let (record, rn) = match event {
                    StreamEvent::Record(r, rn) => (r, rn),
                    // Punctuations carry zero record charge (no `sub_bytes`
                    // discharge, no `input_count` increment), so collect them
                    // for post-join reconciliation with the build side rather
                    // than dropping them. The driver's document boundaries
                    // must reach a downstream per-document consumer.
                    StreamEvent::Punctuation(p) => {
                        driver_puncts.push(p);
                        continue;
                    }
                };
                // Discharge this record's per-row cost — the consume half of
                // the driver's per-batch admit. The formula matches the
                // charge so a fully-drained stream nets to zero.
                charge_handle.sub_bytes(crate::executor::node_buffer::record_byte_cost(
                    record.schema().column_count(),
                ));
                input_count += 1;

                let source_file_arc = source_file_arc_of(&record);
                let source_name_arc = source_name_arc_of(&record);

                // Operator-entry schema check, mirroring the materialized
                // driver-side `check_input_schema` loop.
                if let Err(err) = check_input_schema(
                    &expected_input,
                    record.schema(),
                    name,
                    "combine",
                    &upstream_name,
                ) {
                    // A schema mismatch is a fatal E314 in both paths; drain
                    // to disconnect first so the driver `send` cannot
                    // deadlock, then surface.
                    while rx.recv().is_ok() {}
                    return Err(err);
                }

                // Track the driver source on first sight (for the post-join
                // pre-fold floor capture) and record the cursor advance for
                // replay.
                if !effects
                    .driver_sources
                    .iter()
                    .any(|s| Arc::ptr_eq(s, &source_name_arc))
                {
                    effects.driver_sources.push(Arc::clone(&source_name_arc));
                }
                effects
                    .cursor_advances
                    .push((Arc::clone(&source_name_arc), rn));

                // Mid-stream `$source.count` is `None` (the driver total is
                // unknown until disconnect) — the same defer-emit semantic
                // the streaming Aggregate ingest uses.
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

                let before = output_records.len();
                let step = match kernel.probe_row(
                    &eval_ctx,
                    &record,
                    rn,
                    &mut probe_keys_buf,
                    &mut counters,
                    &mut output_records,
                ) {
                    Ok(step) => step,
                    Err(e) => {
                        // Fatal (FailFast surfacing, on_miss::error,
                        // planner-invariant) — drain to disconnect, then
                        // surface.
                        while rx.recv().is_ok() {}
                        return Err(e);
                    }
                };
                if let ProbeRowStep::Deferred(f) = step {
                    // The whole driver row routes to the DLQ; no output rows
                    // survive for it.
                    output_records.truncate(before);
                    effects.failures.push(*f);
                    continue;
                }

                // Budget check every 10K emitted records, the same cadence
                // and abort the materialized loop uses.
                budget_cadence += output_records.len() - before;
                if budget_cadence >= 10_000 && budget.should_abort() {
                    while rx.recv().is_ok() {}
                    return Err(PipelineError::MemoryBudgetExceeded {
                        node: name.to_string(),
                        used: budget.peak_rss().unwrap_or(0),
                        limit: budget.hard_limit(),
                        source: BudgetCategory::Arena,
                        detail: Some("combine probe RSS abort".to_string()),
                    });
                }
                if budget_cadence >= 10_000 {
                    budget_cadence = 0;
                }
            }
            Ok(())
        });

        // Redispatch the driver producer on the main thread. Clear its
        // probe-edge entry first so the dispatcher's streaming-probe
        // short-circuit (which made the producer's own topo turn a no-op)
        // does not fire again here — this is the one turn the producer must
        // actually run. It takes the sender we installed and streams into
        // the channel, dropping it at clean exit.
        ctx.streaming_combine_probe_edges.remove(&producer_idx);
        let producer_result =
            crate::executor::dispatch::dispatch_plan_node(ctx, current_dag, producer_idx);
        // Belt-and-suspenders: ensure the channel disconnects even if a
        // producer error left a sender lingering on `ctx`, so the probe
        // thread's `recv` returns `Err` and the join below cannot hang.
        ctx.streaming_output_senders.remove(&producer_idx);

        let probe = handle.join().map_err(|_| PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: "streaming combine probe thread panicked".to_string(),
        })?;
        producer_result.and(probe)
    });

    // Charge bookkeeping is complete: the driver charged each batch and the
    // probe thread discharged each record. Pin to zero defensively (a
    // heuristic mismatch between batch charge and per-record discharge must
    // not leave a stale positive), then unregister the per-edge charge
    // consumer.
    charge_handle.set_bytes(0);
    ctx.streaming_charge_consumers.remove(&producer_idx);
    ctx.memory_budget.unregister_consumer(charge_consumer_id);

    probe_result?;

    // Capture each driver source's pre-fold floor from its LIVE cursor and
    // merge it into the installed combine snapshot, BEFORE the deferred
    // combine advances replay below. At this point the producer has fully
    // advanced its sources (it ran to completion on the main thread inside
    // the scope) but the combine's own operator-entry advance has not — so
    // the live cursor is exactly the floor the materialized path captures at
    // fold start. `or_insert` leaves any existing build-side entry (a source
    // that also drives) untouched.
    let driver_floors: Vec<(Arc<str>, u64)> = effects
        .driver_sources
        .iter()
        .map(|src| {
            let floor = ctx.rollback_cursors.get(src).copied().unwrap_or(0);
            (Arc::clone(src), floor)
        })
        .collect();
    if let Some(snapshot) = ctx.combine_input_snapshots.get_mut(&node_idx) {
        for (src, floor) in driver_floors {
            snapshot.entry(src).or_insert(floor);
        }
    }

    // Replay the per-record side effects the probe thread accumulated. The
    // cursor advances keep rollback / watermark state consistent; the DLQ
    // failures route through the same recoverable path the inline loop uses.
    for (source_name_arc, rn) in std::mem::take(&mut effects.cursor_advances) {
        advance_cursor(ctx, &source_name_arc, rn);
    }
    for f in std::mem::take(&mut effects.failures) {
        dispatch_combine_output_error(
            ctx,
            node_idx,
            &f.probe_record,
            f.rn,
            f.matched_build.as_ref(),
            name,
            f.error,
        )?;
    }

    ctx.counters.filtered_count += counters.filtered;
    ctx.counters.distinct_count += counters.distinct;

    Ok(StreamingProbeOutput {
        output_records,
        input_count,
        driver_puncts,
    })
}

/// Per-driver-row outcome the probe kernel returns to its caller. The
/// materialized inline loop and the streaming-probe thread both drive
/// [`CombineProbeKernel::probe_row`], which emits output rows directly into
/// the caller's buffer and signals row-level disposition here. Fatal errors
/// (FailFast surfacing, `on_miss: error`, planner-invariant violations,
/// `EmitMany` fan-out) short-circuit as `Err` instead.
enum ProbeRowStep {
    /// The row produced zero or more output records (already pushed) and
    /// the driver loop continues.
    Continue,
    /// A recoverable per-row eval failure under `Continue` / `BestEffort`.
    /// The caller routes it through `dispatch_combine_output_error` (cursor
    /// rewind + DLQ) — inline immediately, or after the streaming join.
    Deferred(Box<ProbeFailure>),
}

/// A recoverable combine output-row failure deferred for DLQ routing. The
/// streaming-probe thread cannot touch `&mut ExecutorContext`, so it
/// accumulates these and the dispatch thread replays each via
/// [`dispatch_combine_output_error`] after the probe scope joins — matching
/// the inline path's per-row routing in channel-arrival order.
struct ProbeFailure {
    probe_record: Record,
    rn: u64,
    matched_build: Option<Record>,
    error: cxl::eval::EvalError,
}

/// `distinct` / `filtered` skip counts the probe kernel accumulates so the
/// streaming thread (which holds no `&mut ExecutorContext`) can fold them
/// into `ctx.counters` after the join, exactly as the inline loop folds
/// them inline.
#[derive(Default)]
struct ProbeCounters {
    filtered: u64,
    distinct: u64,
}

/// The inline hash build-probe matching kernel for a single driver
/// (probe-side) record. Owns the fully-materialized build-side hash table
/// and every per-combine artifact the probe reads; holds no
/// `&mut ExecutorContext`, so it runs identically on the dispatch thread
/// (materialized path) and on a scoped probe thread (streaming path),
/// guaranteeing a byte-identical result set across the two paths.
///
/// The kernel borrows the hash table and the per-combine metadata for its
/// lifetime; the build side is complete before the first `probe_row` call,
/// so the table is read-only throughout.
struct CombineProbeKernel<'k> {
    name: &'k str,
    hash_table: &'k crate::pipeline::combine::CombineHashTable,
    resolver_mapping: &'k crate::executor::combine::CombineResolverMapping,
    probe_extractor: &'k crate::pipeline::combine::KeyExtractor,
    decomposed: &'k clinker_plan::plan::combine::DecomposedPredicate,
    /// Body typed program, `None` for `match: collect` (empty body) and
    /// body-less synthetic N-ary steps.
    body_program: Option<Arc<TypedProgram>>,
    combine_output_schema: Option<Arc<clinker_record::Schema>>,
    build_qualifier: &'k str,
    match_mode: clinker_plan::config::pipeline_node::MatchMode,
    on_miss: clinker_plan::config::pipeline_node::OnMiss,
    propagate_ck: &'k clinker_plan::config::pipeline_node::PropagateCkSpec,
    /// Whether `ErrorStrategy::FailFast` is active: surface a recoverable
    /// per-row eval failure as `Err` rather than deferring it to the DLQ.
    /// Constant for the whole probe, so it lives on the kernel rather than
    /// a per-call argument.
    fail_fast: bool,
}

impl CombineProbeKernel<'_> {
    /// Probe one driver record against the materialized hash table, pushing
    /// every emitted `(Record, row_num)` into `out` and accumulating
    /// `distinct` / `filtered` skips into `counters`. Returns
    /// [`ProbeRowStep::Continue`] on success (zero or more rows emitted) or
    /// [`ProbeRowStep::Deferred`] for a recoverable per-row failure the
    /// caller routes through the DLQ (or as `Err` when `self.fail_fast` is
    /// set). The signature is `&mut ExecutorContext`-free so the streaming
    /// probe thread can call it.
    fn probe_row(
        &self,
        eval_ctx: &cxl::eval::EvalContext<'_>,
        probe_record: &Record,
        rn: u64,
        probe_keys_buf: &mut Vec<Value>,
        counters: &mut ProbeCounters,
        out: &mut Vec<(Record, u64)>,
    ) -> Result<ProbeRowStep, PipelineError> {
        use crate::executor::combine::CombineResolver;
        use clinker_plan::config::pipeline_node::{MatchMode, OnMiss};

        let name = self.name;

        let probe_key_resolver = CombineResolver::new(self.resolver_mapping, probe_record, None);
        probe_keys_buf.clear();
        if let Err(e) =
            self.probe_extractor
                .extract_into(eval_ctx, &probe_key_resolver, probe_keys_buf)
        {
            if self.fail_fast {
                return Err(PipelineError::Compilation {
                    transform_name: name.to_string(),
                    messages: vec![format!("combine probe key eval error: {e}")],
                });
            }
            // No build candidate matched yet — the failure is on the probe
            // key itself, so only the driver source rewinds.
            return Ok(ProbeRowStep::Deferred(Box::new(ProbeFailure {
                probe_record: probe_record.clone(),
                rn,
                matched_build: None,
                error: e,
            })));
        }

        match self.match_mode {
            MatchMode::Collect => {
                let mut arr: Vec<Value> = Vec::new();
                let mut first_collected_build: Option<Record> = None;
                let mut truncated = false;
                let probe_iter = self.hash_table.probe(probe_keys_buf);
                for candidate in probe_iter {
                    if let Some(residual) = self.decomposed.residual.as_ref() {
                        let resolver = CombineResolver::new(
                            self.resolver_mapping,
                            probe_record,
                            Some(candidate.record),
                        );
                        let mut residual_eval = ProgramEvaluator::new(Arc::clone(residual), false);
                        match residual_eval.eval_record::<NullStorage>(eval_ctx, &resolver, None) {
                            Ok(EvalResult::Skip(SkipReason::Filtered)) => continue,
                            Ok(EvalResult::Emit { .. }) => {}
                            Ok(EvalResult::EmitMany { .. }) => {
                                return Err(PipelineError::Internal {
                                    op: "combine residual",
                                    node: name.to_string(),
                                    detail:
                                        "emit_each fan-out is not supported in a combine residual filter"
                                            .into(),
                                });
                            }
                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => continue,
                            Err(e) => {
                                if self.fail_fast {
                                    return Err(PipelineError::from(e));
                                }
                                return Ok(ProbeRowStep::Deferred(Box::new(ProbeFailure {
                                    probe_record: probe_record.clone(),
                                    rn,
                                    matched_build: Some(candidate.record.clone()),
                                    error: e,
                                })));
                            }
                        }
                    }
                    if arr.len() >= COLLECT_PER_GROUP_CAP {
                        truncated = true;
                        break;
                    }
                    if first_collected_build.is_none() {
                        first_collected_build = Some(candidate.record.clone());
                    }
                    // Build a `Value::Map` for every matched build record,
                    // preserving its own schema order. `iter_user_fields`
                    // filters engine-stamped columns (`$ck.*`, `$widened`)
                    // so a build record's sidecar Map payload never nests
                    // and reaches the writer as a nested Map.
                    let mut m: IndexMap<Box<str>, Value> = IndexMap::new();
                    for (fname, val) in candidate.record.iter_user_fields() {
                        m.insert(fname.into(), val.clone());
                    }
                    arr.push(Value::Map(Box::new(m)));
                }
                if truncated {
                    eprintln!(
                        "W: combine {:?} match: collect truncated at \
                         {COLLECT_PER_GROUP_CAP} matches for driver row {}",
                        name, rn
                    );
                }
                let mut rec = match self.combine_output_schema.as_ref() {
                    Some(s) => widen_record_to_schema(probe_record, s),
                    None => probe_record.clone(),
                };
                if let Some(first_build) = first_collected_build.as_ref() {
                    crate::executor::copy_build_ck_columns(
                        &mut rec,
                        first_build,
                        self.propagate_ck,
                    );
                }
                rec.set(self.build_qualifier, Value::Array(arr));
                out.push((rec, rn));
                Ok(ProbeRowStep::Continue)
            }

            MatchMode::First | MatchMode::All => {
                // Residual-filter + emit pass. Clone each surviving build
                // record before dropping the iterator so the evaluator
                // borrow doesn't alias the hash-table borrow.
                let matched_records: Vec<Record> = {
                    let probe_iter = self.hash_table.probe(probe_keys_buf);
                    let mut matched: Vec<Record> = Vec::new();
                    for candidate in probe_iter {
                        if let Some(residual) = self.decomposed.residual.as_ref() {
                            let resolver = CombineResolver::new(
                                self.resolver_mapping,
                                probe_record,
                                Some(candidate.record),
                            );
                            let mut residual_eval =
                                ProgramEvaluator::new(Arc::clone(residual), false);
                            match residual_eval
                                .eval_record::<NullStorage>(eval_ctx, &resolver, None)
                            {
                                Ok(EvalResult::Skip(_)) => continue,
                                Ok(EvalResult::Emit { .. }) => {}
                                Ok(EvalResult::EmitMany { .. }) => {
                                    return Err(PipelineError::Internal {
                                        op: "combine residual",
                                        node: name.to_string(),
                                        detail:
                                            "emit_each fan-out is not supported in a combine residual filter"
                                                .into(),
                                    });
                                }
                                Err(e) => {
                                    if self.fail_fast {
                                        return Err(PipelineError::from(e));
                                    }
                                    return Ok(ProbeRowStep::Deferred(Box::new(ProbeFailure {
                                        probe_record: probe_record.clone(),
                                        rn,
                                        matched_build: Some(candidate.record.clone()),
                                        error: e,
                                    })));
                                }
                            }
                        }
                        matched.push(candidate.record.clone());
                        if matches!(self.match_mode, MatchMode::First) {
                            break;
                        }
                    }
                    matched
                };

                if matched_records.is_empty() {
                    match self.on_miss {
                        OnMiss::Skip => Ok(ProbeRowStep::Continue),
                        OnMiss::Error => Err(PipelineError::CombineMissingMatch {
                            combine: name.to_string(),
                            driver_row: rn,
                        }),
                        OnMiss::NullFields => {
                            let resolver =
                                CombineResolver::new(self.resolver_mapping, probe_record, None);
                            let body = self.body_program.as_ref().ok_or_else(|| {
                                PipelineError::Internal {
                                    op: "combine",
                                    node: name.to_string(),
                                    detail: "combine body typed program missing for on_miss: null_fields"
                                        .to_string(),
                                }
                            })?;
                            let mut evaluator = ProgramEvaluator::new(Arc::clone(body), false);
                            match evaluator.eval_record::<NullStorage>(eval_ctx, &resolver, None) {
                                Ok(EvalResult::Emit {
                                    fields: emitted,
                                    record_vars,
                                    ..
                                }) => {
                                    let mut rec = match self.combine_output_schema.as_ref() {
                                        Some(s) => widen_record_to_schema(probe_record, s),
                                        None => probe_record.clone(),
                                    };
                                    for (n, v) in emitted {
                                        rec.set(&n, v);
                                    }
                                    for (k, v) in *record_vars {
                                        let _ = rec.set_record_var(&k, v);
                                    }
                                    out.push((rec, rn));
                                    Ok(ProbeRowStep::Continue)
                                }
                                Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                    counters.filtered += 1;
                                    Ok(ProbeRowStep::Continue)
                                }
                                Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                    counters.distinct += 1;
                                    Ok(ProbeRowStep::Continue)
                                }
                                Ok(EvalResult::EmitMany { .. }) => Err(PipelineError::Internal {
                                    op: "combine on_miss body",
                                    node: name.to_string(),
                                    detail: "emit_each fan-out is not supported in a combine body"
                                        .into(),
                                }),
                                Err(e) => {
                                    if self.fail_fast {
                                        return Err(PipelineError::from(e));
                                    }
                                    // on_miss path: no build row matched, so
                                    // only the driver source rewinds.
                                    Ok(ProbeRowStep::Deferred(Box::new(ProbeFailure {
                                        probe_record: probe_record.clone(),
                                        rn,
                                        matched_build: None,
                                        error: e,
                                    })))
                                }
                            }
                        }
                    }
                } else if let Some(body) = self.body_program.as_ref() {
                    let mut evaluator = ProgramEvaluator::new(Arc::clone(body), false);
                    for matched in &matched_records {
                        let resolver = CombineResolver::new(
                            self.resolver_mapping,
                            probe_record,
                            Some(matched),
                        );
                        match evaluator.eval_record::<NullStorage>(eval_ctx, &resolver, None) {
                            Ok(EvalResult::Emit {
                                fields: emitted,
                                record_vars,
                                ..
                            }) => {
                                let mut rec = match self.combine_output_schema.as_ref() {
                                    Some(s) => widen_record_to_schema(probe_record, s),
                                    None => probe_record.clone(),
                                };
                                for (n, v) in emitted {
                                    rec.set(&n, v);
                                }
                                for (k, v) in *record_vars {
                                    let _ = rec.set_record_var(&k, v);
                                }
                                crate::executor::copy_build_ck_columns(
                                    &mut rec,
                                    matched,
                                    self.propagate_ck,
                                );
                                out.push((rec, rn));
                            }
                            Ok(EvalResult::Skip(SkipReason::Filtered)) => counters.filtered += 1,
                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => counters.distinct += 1,
                            Ok(EvalResult::EmitMany { .. }) => {
                                return Err(PipelineError::Internal {
                                    op: "combine matched body",
                                    node: name.to_string(),
                                    detail: "emit_each fan-out is not supported in a combine body"
                                        .into(),
                                });
                            }
                            Err(e) => {
                                if self.fail_fast {
                                    return Err(PipelineError::from(e));
                                }
                                return Ok(ProbeRowStep::Deferred(Box::new(ProbeFailure {
                                    probe_record: probe_record.clone(),
                                    rn,
                                    matched_build: Some(matched.clone()),
                                    error: e,
                                })));
                            }
                        }
                    }
                    Ok(ProbeRowStep::Continue)
                } else {
                    // Body-less synthetic step from N-ary combine
                    // decomposition: the encoded output schema concatenates
                    // driver columns then build columns. Emit one record per
                    // match by concatenating value slices onto the encoded
                    // `Arc<Schema>`. Build-side `$ck.<field>` values are
                    // already in the concatenated tail under their encoded
                    // names, recovered later by `widen_record_to_schema`.
                    let target_schema =
                        self.combine_output_schema
                            .as_ref()
                            .ok_or_else(|| PipelineError::Internal {
                                op: "combine",
                                node: name.to_string(),
                                detail: "synthetic combine step has no output schema; decomposition pass did not run"
                                    .to_string(),
                            })?;
                    for matched in &matched_records {
                        let mut values: Vec<Value> =
                            Vec::with_capacity(target_schema.column_count());
                        values.extend(probe_record.values().iter().cloned());
                        values.extend(matched.values().iter().cloned());
                        if values.len() != target_schema.column_count() {
                            return Err(PipelineError::Internal {
                                op: "combine",
                                node: name.to_string(),
                                detail: format!(
                                    "synthetic combine step produced {} concatenated values; encoded schema has {} columns",
                                    values.len(),
                                    target_schema.column_count()
                                ),
                            });
                        }
                        let rec = Record::new(Arc::clone(target_schema), values);
                        out.push((rec, rn));
                    }
                    Ok(ProbeRowStep::Continue)
                }
            }
        }
    }
}

/// Route every deferred output-stage eval failure through the shared
/// `combine_output_row` dead-letter path, in visitation order. Both IEJoin
/// dispatch shapes hand back the same failure vector, so this runs once per
/// combine before the shape-specific row handling — and, like the singular
/// helper, MUST run while the pre-fold input snapshot is still installed so each
/// contributing source's rollback cursor rewinds to the right floor.
fn dispatch_combine_output_errors(
    ctx: &mut ExecutorContext<'_>,
    node_idx: NodeIndex,
    combine_name: &str,
    failures: Vec<crate::pipeline::combine::CombineOutputEvalFailure>,
) -> Result<(), PipelineError> {
    for f in failures {
        dispatch_combine_output_error(
            ctx,
            node_idx,
            &f.probe_record,
            f.row,
            f.matched_build.as_ref(),
            combine_name,
            f.error,
        )?;
    }
    Ok(())
}

/// Outcome of draining a pure-range block-band combine's payload-sorted output.
enum BlockBandDrain {
    /// The output streamed straight to a downstream streaming `Output` writer
    /// over the back-pressure sink; no `node_buffers` slot was admitted.
    /// Carries the emitted row count for the probe-stage timer.
    Streamed(u64),
    /// The output was admitted to a `node_buffers` slot (resident or spilled);
    /// the caller inserts it and reads its length.
    Buffered(crate::executor::node_buffer::NodeBuffer),
}

/// Drain a pure-range block-band combine's bounded, payload-sorted output,
/// preserving the deterministic `(driver order, driver_idx, build_idx)` order
/// the sort realized.
///
/// This is the single drain seam for the block-band output axis, with two
/// targets chosen by the downstream edge's streaming eligibility:
///
/// - **Streaming.** When a downstream streaming `Output` certified this combine
///   as its producer, a sender is installed under `node_idx`; the sorted rows
///   drain straight through the back-pressure sink to the writer thread. A
///   spilled result streams from the lazy k-way run merge, so at most one batch
///   plus the merge's per-run fronts is live — the output axis is bounded
///   end-to-end, not just at the operator. The certification declines a
///   producer that roots a node-anchored window, so no sender exists for that
///   surface; the materialization guard stays authoritative here to also
///   exclude a cross-region tee.
/// - **Buffered.** Otherwise the output is admitted to a `node_buffers` slot.
///   Resident output (the whole result fit the sort buffer's byte threshold) is
///   admitted through the shared `admit_node_buffer` path, which itself spills
///   the slot if RSS pressure warrants. Spilled output streams straight from the
///   k-way run merge into a single node-buffer spill chunk, never
///   re-materializing the full result — so a blocking downstream stays bounded.
///   When a downstream window is rooted on this node, a deferred region abuts
///   one of its out-edges, or the slot cannot spill (multi-consumer fan-out /
///   composition input port), the spilled result is materialized once — those
///   surfaces hold the whole set regardless — and admitted in memory.
fn drain_block_band_output(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    combine_name: &str,
    sorted: crate::pipeline::sort_buffer::SortedOutput<(RecordOrder, u64, u64)>,
    puncts: Vec<crate::executor::stream_event::Punctuation>,
) -> Result<BlockBandDrain, PipelineError> {
    use crate::pipeline::sort_buffer::SortedOutput;
    let spill_allowed = node_buffer_spill_allowed(current_dag, node_idx);
    // A window root, a cross-region tee, or a non-spillable slot needs the whole
    // slice materialized; the traversal that decides this is walked once and
    // reused by both the streaming guard and the spilled-buffer branch.
    let needs_materialization = block_band_output_needs_materialization(current_dag, node_idx);

    // Streaming graft: drain the SAME payload-sorted handle through the
    // back-pressure sink instead of admitting a node-buffer. Take the sender
    // only when the materialization guard is clear (no window root, no
    // cross-region tee — those consume the whole slice) AND a sender is
    // present: taking it without draining would strand the writer thread on
    // records that never arrive.
    if !needs_materialization && let Some(sender) = ctx.take_streaming_sender(node_idx) {
        let batch_size = ctx.batch_size;
        let charge = ctx
            .streaming_charge_handle(node_idx, combine_name, spill_allowed)
            .expect("streaming sender implies a registered charge consumer");
        // Both output shapes drain the identical `(order, driver_idx,
        // build_idx)` sort, so the streamed rows arrive in the same order the
        // buffered path admits — the determinism the cross-limit tests pin
        // holds regardless of which drain target is chosen.
        let count = match sorted {
            SortedOutput::InMemory(pairs) => stream_block_band_rows(
                &sender,
                batch_size,
                combine_name,
                pairs
                    .into_iter()
                    .map(|(record, (order, _, _))| Ok((record, order))),
                puncts,
                &charge,
            )?,
            SortedOutput::Spilled(files) => {
                // The k-way run merge is lazy — one resident record per open
                // run — so streaming its rows through the batcher keeps the
                // spilled result bounded, never re-materializing the slice.
                let merger = crate::pipeline::spill_merge::SortedRunMerger::new_payload_ordered(
                    files,
                    "iejoin block-band output merge",
                )?;
                stream_block_band_rows(
                    &sender,
                    batch_size,
                    combine_name,
                    merger.map(|item| item.map(|(record, (order, _, _))| (record, order))),
                    puncts,
                    &charge,
                )?
            }
        };
        return Ok(BlockBandDrain::Streamed(count));
    }

    let buffered = match sorted {
        SortedOutput::InMemory(pairs) => {
            // The result fit the buffer's byte threshold, so it is already
            // bounded. Strip the sort payload back to `(record, order)` and admit
            // as the equi+range path does, running the window-root and
            // cross-region tee side effects (no-ops when neither applies).
            let rows: Vec<(Record, u64)> = pairs
                .into_iter()
                .map(|(record, (order, _, _))| (record, order))
                .collect();
            finalize_node_rooted_windows(ctx, current_dag, node_idx, &rows)?;
            tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &rows)?;
            admit_node_buffer(ctx, combine_name, node_idx, rows, puncts, spill_allowed)?
        }
        SortedOutput::Spilled(files) => {
            let merger = crate::pipeline::spill_merge::SortedRunMerger::new_payload_ordered(
                files,
                "iejoin block-band output merge",
            )?;
            if spill_allowed && !needs_materialization {
                // Bounded path: fold the sorted runs straight into a node-buffer
                // spill chunk, one resident record per open run.
                drain_merger_to_spilled_node_buffer(ctx, node_idx, merger, puncts)?
            } else {
                // A window root, a cross-region tee, or a non-spillable slot needs
                // the whole slice; those surfaces are O(N) regardless, so
                // re-materialize the sorted stream once and admit it in memory.
                let mut rows: Vec<(Record, u64)> = Vec::new();
                for item in merger {
                    let (record, (order, _, _)) = item?;
                    rows.push((record, order));
                }
                finalize_node_rooted_windows(ctx, current_dag, node_idx, &rows)?;
                tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &rows)?;
                admit_node_buffer(ctx, combine_name, node_idx, rows, puncts, spill_allowed)?
            }
        }
    };
    Ok(BlockBandDrain::Buffered(buffered))
}

/// Stream a block-band combine's payload-sorted output rows straight to a
/// downstream streaming `Output` over the back-pressure sink, returning the
/// emitted row count.
///
/// Mirrors [`stream_linear_producer_emit`]'s batcher idiom but pulls from a lazy
/// sorted iterator — the resident `InMemory` pairs or the k-way
/// `SortedRunMerger` — so a spilled result never re-materializes: at most one
/// batch plus the merge's per-run fronts is live at once. Each `(record, order)`
/// pushes through the [`crate::executor::batch_handoff::EventBatcher`]; a full
/// batch is charged and routed through `charge` (which spills the batch under
/// memory pressure) and sent to the writer thread over the bounded channel, so
/// a slow writer back-pressures this drain. Punctuations follow the records,
/// matching the buffered path's forwarding to the terminal writer.
fn stream_block_band_rows(
    sender: &crossbeam_channel::Sender<crate::executor::stream_event::StreamEvent>,
    batch_size: usize,
    node_name: &str,
    rows: impl Iterator<Item = Result<(Record, u64), PipelineError>>,
    puncts: Vec<crate::executor::stream_event::Punctuation>,
    charge: &crate::executor::batch_handoff::StreamingChargeHandle,
) -> Result<u64, PipelineError> {
    let mut batcher = crate::executor::batch_handoff::EventBatcher::new(
        batch_size,
        |batch: crate::executor::batch_handoff::EventBatch| -> Result<(), PipelineError> {
            charge.charge_and_route(
                batch,
                |event: crate::executor::stream_event::StreamEvent| {
                    sender.send(event).map_err(|_| PipelineError::Internal {
                        op: "executor",
                        node: node_name.to_string(),
                        detail: String::from(
                            "streaming Output writer task dropped its receiver before \
                             the block-band output drain finished",
                        ),
                    })
                },
            )
        },
    );
    let mut count: u64 = 0;
    for item in rows {
        let (record, rn) = item?;
        batcher.push_record(record, rn)?;
        count += 1;
    }
    for punct in puncts {
        batcher.push_punctuation(punct)?;
    }
    batcher.finish()?;
    Ok(count)
}

/// Whether the block-band output must be fully materialized before admission: a
/// downstream window is rooted on this node, or a deferred region abuts one of
/// its out-edges. Both side effects (node-rooted arena build, cross-region
/// tee) consume the whole emitted slice, so the streamed-spill path cannot serve
/// them. Kept in sync with the surfaces `finalize_node_rooted_windows` and
/// `tee_emit_to_region_input_buffers` act on.
fn block_band_output_needs_materialization(
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
) -> bool {
    node_roots_window(current_dag, node_idx) || node_tees_to_deferred_region(current_dag, node_idx)
}

/// Whether any node-rooted window index is anchored on `node_idx` — the exact
/// condition under which `finalize_node_rooted_windows` builds an arena from the
/// node's full output.
fn node_roots_window(current_dag: &ExecutionPlanDag, node_idx: NodeIndex) -> bool {
    use clinker_plan::plan::index::PlanIndexRoot;
    current_dag.indices_to_build.iter().any(
        |spec| matches!(&spec.root, PlanIndexRoot::Node { upstream, .. } if *upstream == node_idx),
    )
}

/// Whether any out-edge of `node_idx` crosses into a deferred region — the exact
/// condition under which `tee_emit_to_region_input_buffers` parks the node's full
/// output into a region input buffer. Mirrors that function's crossing test, so
/// the two agree on which edges cross.
fn node_tees_to_deferred_region(current_dag: &ExecutionPlanDag, node_idx: NodeIndex) -> bool {
    use petgraph::visit::EdgeRef;
    let producer_region_producer = current_dag.deferred_region_at(node_idx).map(|r| r.producer);
    current_dag
        .graph
        .edges_directed(node_idx, Direction::Outgoing)
        .any(|edge_ref| {
            let target_region_producer = current_dag
                .deferred_region_at(edge_ref.target())
                .map(|r| r.producer);
            match (producer_region_producer, target_region_producer) {
                (None, Some(_)) => true,
                (Some(p), Some(t)) => p != t,
                _ => false,
            }
        })
}

/// Stream a payload-sorted run merge straight into a single spilled node-buffer
/// chunk, dropping the sort payload back to the `(record, order)` shape the slot
/// stores. Bounded: at most one resident record per open run plus the writer's
/// buffer, never the whole result. Charges the chunk's on-disk bytes against the
/// disk quota (E320) and registers a node-buffer consumer at zero resident bytes
/// — the records live on disk — mirroring `admit_node_buffer`'s spilled tail so
/// the slot's later drain unregisters through the same path.
fn drain_merger_to_spilled_node_buffer(
    ctx: &mut ExecutorContext<'_>,
    node_idx: NodeIndex,
    merger: crate::pipeline::spill_merge::SortedRunMerger<(RecordOrder, u64, u64)>,
    puncts: Vec<crate::executor::stream_event::Punctuation>,
) -> Result<crate::executor::node_buffer::NodeBuffer, PipelineError> {
    use crate::executor::node_buffer::{NodeBuffer, NodeBufferConsumer};
    let mut iter = merger;
    // Pull the first row to resolve the schema and compression mode; an empty
    // merge yields a punctuation-only memory slot.
    let (first_record, first_order) = match iter.next() {
        None => {
            return Ok(NodeBuffer::memory_from_records_and_puncts(
                Vec::new(),
                puncts,
            ));
        }
        Some(Ok((record, (order, _, _)))) => (record, order),
        Some(Err(e)) => return Err(e),
    };
    let schema = Arc::clone(first_record.schema());
    let column_count = schema.column_count();
    let compress = ctx
        .spill_compress
        .resolve_for_schema(column_count, ctx.batch_size as u64);
    let mut writer: crate::pipeline::spill::SpillWriter<u64> =
        crate::pipeline::spill::SpillWriter::new(
            schema,
            Some(ctx.spill_root_path.as_ref()),
            compress,
        )?;
    writer.write_pair(&first_record, &first_order)?;
    let mut count: u64 = 1;
    for item in iter {
        let (record, (order, _, _)) = item?;
        writer.write_pair(&record, &order)?;
        count += 1;
    }
    let (file, _written) = writer.finish_with_bytes()?;
    // The emit-phase output sort already charged this result against the disk
    // quota when it spilled its runs. This drain only re-serializes that same,
    // already-charged output into the node-buffer's single sorted chunk and then
    // frees the source runs, so charging the rewrite again would double-count one
    // logical result — making the disk-cap outcome depend on whether the
    // downstream buffers or streams (the streaming drain charges it only once).
    // Register the slot's node-buffer consumer at zero resident bytes so its
    // later drain unregisters through the same path `admit_node_buffer` sets up.
    // A prior registration at this slot is replaced first.
    if let Some((prev_id, _)) = ctx.node_buffer_consumer_ids.remove(&node_idx) {
        ctx.memory_budget.unregister_consumer(prev_id);
    }
    let handle = crate::pipeline::memory::ConsumerHandle::new();
    handle.set_bytes(0);
    let consumer_id = ctx
        .memory_budget
        .register_consumer(Arc::new(NodeBufferConsumer::new(handle.clone())));
    ctx.node_buffer_consumer_ids
        .insert(node_idx, (consumer_id, handle));
    ctx.memory_budget.sample_peak_consumer_usage();
    Ok(NodeBuffer::Spilled {
        chunks: vec![(file, count)],
        pending_puncts: puncts,
    })
}

fn dispatch_combine_output_error(
    ctx: &mut ExecutorContext<'_>,
    node_idx: NodeIndex,
    probe_record: &Record,
    row_num: u64,
    matched_build_record: Option<&Record>,
    combine_name: &str,
    eval_err: cxl::eval::EvalError,
) -> Result<(), PipelineError> {
    let category = clinker_core_types::dlq::DlqErrorCategory::CombineOutputRow;
    let stage = Some(DlqEntry::stage_combine(combine_name));
    let message = eval_err.to_string();

    let probe_source = source_name_arc_of(probe_record);
    let build_source = matched_build_record.map(source_name_arc_of);

    // Rewind every contributing source to its captured pre-fold floor
    // before admitting any DLQ entry. The snapshot was taken at fold
    // start (before this arm's operator-entry `advance_cursor` walk
    // raised each cursor to the highest contributed row), so restoring
    // it narrows the replay anchor back across this fold for the driver
    // and the matched build row alike. Only sources that fed THIS row
    // are rewound — co-folded sources that did not contribute keep their
    // forward progress. The snapshot is left installed: the arm's single
    // clear at fold exit is the only drop point.
    if let Some(snapshot) = ctx.combine_input_snapshots.get(&node_idx) {
        let mut floors: Vec<(Arc<str>, u64)> = Vec::new();
        if let Some(&floor) = snapshot.get(&probe_source) {
            floors.push((Arc::clone(&probe_source), floor));
        }
        if let Some(bsrc) = build_source.as_ref()
            && let Some(&floor) = snapshot.get(bsrc)
        {
            floors.push((Arc::clone(bsrc), floor));
        }
        for (sn, floor) in floors {
            ctx.rollback_cursors
                .entry(sn)
                .and_modify(|c| *c = (*c).min(floor))
                .or_insert(floor);
        }
    }

    // Trigger entry on the probe row's source. Park under the
    // correlation group cell when buffering is active so the group stays
    // atomic; otherwise push directly to the run-scoped DLQ.
    let triggering_field = eval_err.triggering_field.clone();
    let triggering_value = eval_err.triggering_value();
    let routed = record_error_to_buffer_if_grouped(
        ctx,
        probe_record,
        row_num,
        category,
        message.clone(),
        stage.clone(),
        None,
    );
    if !routed {
        push_dlq(
            ctx,
            DlqEntry {
                source_row: row_num,
                category,
                error_message: message.clone(),
                original_record: probe_record.clone(),
                stage: stage.clone(),
                route: None,
                trigger: true,
                source_name: Arc::clone(&probe_source),
                triggering_field,
                triggering_value,
            },
        )?;
    }

    // When a build row contributed, attribute a build-side entry too so
    // the contributing build lineage reaches the DLQ. It carries the
    // same category but is not the trigger. The build row's own source
    // row number is not threaded to the output stage (build-side row
    // numbers are consumed at the operator-entry cursor advance), so the
    // entry borrows the driver row number; the build record's
    // `original_record` still carries the build's real `$source.name`
    // stamp and `$ck.*` lineage, which is what attribution and
    // group-atomicity key off.
    if let Some(build_record) = matched_build_record {
        let build_routed = record_error_to_buffer_if_grouped(
            ctx,
            build_record,
            row_num,
            category,
            message.clone(),
            stage.clone(),
            None,
        );
        if !build_routed {
            let build_source_name = build_source
                .clone()
                .unwrap_or_else(|| source_name_arc_of(build_record));
            push_dlq(
                ctx,
                DlqEntry {
                    source_row: row_num,
                    category,
                    error_message: message,
                    original_record: build_record.clone(),
                    stage,
                    route: None,
                    trigger: false,
                    source_name: build_source_name,
                    triggering_field: None,
                    triggering_value: None,
                },
            )?;
        }
    }

    Ok(())
}
