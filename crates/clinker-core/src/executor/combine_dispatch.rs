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

use crate::config::ErrorStrategy;
use crate::error::PipelineError;
use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, advance_cursor, drain_node_buffer_slot,
    finalize_node_rooted_windows, node_buffer_spill_allowed, push_dlq,
    record_error_to_buffer_if_grouped, source_file_arc_of, source_name_arc_of,
    stream_linear_producer_emit, tee_emit_to_region_input_buffers,
};
use crate::executor::schema_check::check_input_schema;
use crate::executor::{DlqEntry, NullStorage, stage_metrics, widen_record_to_schema};
use crate::pipeline::memory::BudgetCategory;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};

/// Execute the `Combine` arm for `node_idx`: dispatch on the planner-
/// selected [`crate::plan::combine::CombineStrategy`], drive the build and
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
        ..
    } = *node
    else {
        unreachable!("dispatch_combine called with non-Combine node");
    };
    use crate::config::pipeline_node::{MatchMode, OnMiss};
    use crate::executor::combine::{CombineResolver, CombineResolverMapping};
    use crate::pipeline::combine::{CombineHashTable, KeyExtractor};
    use crate::pipeline::grace_hash::{GraceHashExec, execute_combine_grace_hash};
    use crate::pipeline::iejoin::{IEJoinExec, execute_combine_iejoin};
    use crate::pipeline::sort_merge_join::{SortMergeExec, execute_combine_sort_merge};
    use crate::plan::combine::CombineStrategy;

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

    // Cap on matches collected per driver under
    // `match: collect` before truncation. 10K mirrors
    // the module constants in pipeline/combine.rs and
    // aligns with DataFusion's collect-list bound.
    const COLLECT_PER_GROUP_CAP: usize = 10_000;

    // Combine's widened output schema — every emitted
    // record lands on this `Arc<Schema>` so downstream
    // operators hit the ptr_eq fast path and
    // `Record::set` always addresses a known slot.
    let combine_output_schema = current_dag.graph[node_idx].stored_output_schema().cloned();

    let combine_inputs =
        ctx.artifacts
            .combine_inputs
            .get(name)
            .ok_or_else(|| PipelineError::Internal {
                op: "combine",
                node: name.clone(),
                detail: "no combine_inputs entry for combine node".to_string(),
            })?;
    let decomposed =
        ctx.artifacts
            .combine_predicates
            .get(name)
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
        let pname = current_dag.graph[p].name();
        if pname == target {
            return true;
        }
        if let Some(stripped) = pname.strip_prefix(crate::plan::execution::CORRELATION_SORT_PREFIX)
        {
            return stripped == target;
        }
        false
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

    // Combine collects puncts from both inputs and forwards
    // them to the output buffer. Per-document dedup
    // (Merge-style barrier counter) for two-input Combine is
    // out of scope for #91 (deferred to follow-up); the
    // simple union here may emit `DocumentClose` twice
    // downstream for documents that span both inputs, which
    // any downstream Aggregate would handle idempotently.
    let (driver_buf, driver_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = match drain_node_buffer_slot(ctx, driver_pred) {
        Some(nb) => nb.drain_split()?,
        None => (Vec::new(), Vec::new()),
    };
    let (build_buf, build_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = match drain_node_buffer_slot(ctx, build_pred) {
        Some(nb) => nb.drain_split()?,
        None => (Vec::new(), Vec::new()),
    };
    let combined_puncts: Vec<crate::executor::stream_event::Punctuation> =
        driver_puncts.into_iter().chain(build_puncts).collect();

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
            // scoped arbitrator. IEJoin reuses the sort-buffer
            // spill machinery for its build-side bit array and
            // sort permutation, so it registers under
            // `SortConsumer` (priority 20); the handle's bytes
            // start at zero and gain wiring once IEJoin's build
            // path is plumbed.
            ctx.memory_budget.register_consumer(Arc::new(
                crate::pipeline::sort_buffer::SortConsumer::new(
                    crate::pipeline::memory::ConsumerHandle::new(),
                ),
            ));
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
            let body_typed = ctx.artifacts.typed.get(name);
            let combine_output_schema_arc = combine_output_schema.clone();
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
                    strategy: ctx.strategy,
                })
            })?;
            let crate::pipeline::combine::CombineKernelOutput {
                records: output_records,
                output_eval_failures,
            } = kernel_out;
            // Route each deferred output-stage eval failure through
            // the same `combine_output_row` path the inline arm
            // uses. This MUST run before the snapshot is dropped
            // below — `dispatch_combine_output_error` reads the
            // installed pre-fold snapshot to rewind each
            // contributing source's rollback cursor.
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
            // snapshot. Every emitted record has cleared into
            // `node_buffers` without rewind, so the snapshot is
            // no longer needed. Mirrors the inline arm's
            // post-emit clear.
            ctx.combine_input_snapshots.remove(&node_idx);
            return Ok(());
        }
        Dispatch::Grace(partition_bits) => {
            // Single ConsumerHandle shared between the
            // GraceHashConsumer wrapper (Priority policy
            // priority 10) and the GraceHashExecutor that
            // mirrors its in-memory partition `bytes_estimated`
            // sum into the handle's counter on every admit /
            // spill_partition transition.
            let grace_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
            ctx.memory_budget.register_consumer(Arc::new(
                crate::pipeline::grace_hash::GraceHashConsumer::new(grace_consumer_handle.clone()),
            ));
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
            let body_typed = ctx.artifacts.typed.get(name);
            let combine_output_schema_arc = combine_output_schema.clone();
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
                    consumer_handle: grace_consumer_handle,
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
            // clear so non-inline strategies do not leak
            // stale snapshot entries across runs.
            ctx.combine_input_snapshots.remove(&node_idx);
            return Ok(());
        }
        Dispatch::SortMerge => {
            // Single ConsumerHandle shared between the
            // SortMergeConsumer wrapper (Priority policy
            // priority 25) and the SortMergeExec kernel that
            // mirrors Phase A `SortBuffer.bytes_used` + Phase B
            // matching-run accumulator size into the handle's
            // counter at every push / spill transition.
            let sm_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
            ctx.memory_budget.register_consumer(Arc::new(
                crate::pipeline::sort_merge_join::SortMergeConsumer::new(
                    sm_consumer_handle.clone(),
                ),
            ));
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
            let body_typed = ctx.artifacts.typed.get(name);
            let combine_output_schema_arc = combine_output_schema.clone();
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
            return Ok(());
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

    // Body evaluator (only used when the body is not
    // empty — `match: collect` leaves it empty).
    let body_typed = ctx.artifacts.typed.get(name).cloned();
    let mut body_evaluator = body_typed
        .as_ref()
        .map(|bt| ProgramEvaluator::new(Arc::clone(bt), false));

    // Per-driver probe loop. Stage timer covers the
    // full per-driver iteration; on early-return via the
    // 10K-cadence E310 abort or a residual/body eval
    // error, the timer is dropped without recording.
    let probe_records_in = driver_buf.len() as u64;
    let probe_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
        name: name.clone(),
    });
    let mut output_records = Vec::with_capacity(driver_buf.len());
    let mut emitted_since_check: usize = 0;
    // Reused across every probe iteration to avoid an
    // allocation per driver row. `KeyExtractor::extract`
    // pushes into the end; we clear before each call.
    let mut probe_keys_buf: Vec<Value> = Vec::with_capacity(probe_extractor.len());

    'driver: for (probe_record, rn) in driver_buf {
        let source_file_arc = source_file_arc_of(&probe_record);
        let source_name_arc = source_name_arc_of(&probe_record);
        let eval_ctx = ctx.eval_ctx_for_record(
            &source_file_arc,
            &source_name_arc,
            rn,
            probe_record.doc_ctx(),
        );

        // Probe-side key extraction routes through the
        // shared `CombineResolver` so chain-buried
        // qualifiers (e.g. `b.id` against an N-ary
        // decomposition step's encoded intermediate
        // record) resolve via the resolved column map
        // rather than `Record::resolve_qualified`'s
        // bare-name fallback. For non-chain combines
        // the lookup goes through the same path with
        // the same answer (probe-side qualifier maps
        // to its native source-row position).
        let probe_key_resolver = CombineResolver::new(&resolver_mapping, &probe_record, None);
        probe_keys_buf.clear();
        if let Err(e) =
            probe_extractor.extract_into(&eval_ctx, &probe_key_resolver, &mut probe_keys_buf)
        {
            if ctx.strategy == ErrorStrategy::FailFast {
                return Err(PipelineError::Compilation {
                    transform_name: name.clone(),
                    messages: vec![format!("combine probe key eval error: {e}")],
                });
            }
            // No build candidate has matched yet — the failure is
            // on the probe key itself, so only the driver source
            // rewinds.
            dispatch_combine_output_error(ctx, node_idx, &probe_record, rn, None, name, e)?;
            continue 'driver;
        }

        match match_mode {
            MatchMode::Collect => {
                // Synthesize the output directly. No
                // body evaluator runs. On miss, emit
                // an empty array (E311-guarded: the
                // body is enforced empty at compile
                // time, so on_miss policy does not
                // bypass emission under Collect).
                let mut arr: Vec<Value> = Vec::new();
                let mut first_collected_build: Option<Record> = None;
                let mut truncated = false;
                let probe_iter = hash_table.probe(&probe_keys_buf);
                for candidate in probe_iter {
                    // Residual filter, if any.
                    if let Some(residual) = decomposed.residual.as_ref() {
                        let resolver = CombineResolver::new(
                            &resolver_mapping,
                            &probe_record,
                            Some(candidate.record),
                        );
                        let mut residual_eval = ProgramEvaluator::new(Arc::clone(residual), false);
                        match residual_eval.eval_record::<NullStorage>(&eval_ctx, &resolver, None) {
                            Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                continue;
                            }
                            Ok(EvalResult::Emit { .. }) => {}
                            Ok(EvalResult::EmitMany { .. }) => {
                                return Err(PipelineError::Internal {
                                            op: "combine residual",
                                            node: name.clone(),
                                            detail: "emit_each fan-out is not supported in a combine residual filter".into(),
                                        });
                            }
                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                continue;
                            }
                            Err(e) => {
                                if ctx.strategy == ErrorStrategy::FailFast {
                                    return Err(PipelineError::from(e));
                                }
                                dispatch_combine_output_error(
                                    ctx,
                                    node_idx,
                                    &probe_record,
                                    rn,
                                    Some(candidate.record),
                                    name,
                                    e,
                                )?;
                                continue 'driver;
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
                    // Build a Value::Map for every matched
                    // build record, preserving its own
                    // schema order. `iter_user_fields`
                    // filters every engine-stamped column
                    // — both `$ck.*` (correlation lineage;
                    // not meaningful nested inside a
                    // collect-array entry) and `$widened`
                    // (auto_widen sidecar; build-side
                    // sidecars drop at the join boundary
                    // by design, mirroring
                    // `propagate_ck: Driver`). Without
                    // this filter, a build record's
                    // `$widened` `Value::Map` payload
                    // nests inside the collect-mode
                    // `Value::Map` and reaches the writer
                    // as a nested Map, triggering
                    // `FormatError::UnserializableMapValue`.
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

                // Output record inherits the probe's
                // data re-projected onto the combine's
                // widened output_schema; the
                // `<build_qualifier>` column is
                // guaranteed to exist on it.
                let mut rec = match combine_output_schema.as_ref() {
                    Some(s) => widen_record_to_schema(&probe_record, s),
                    None => probe_record.clone(),
                };
                // Build-side `$ck.<field>` propagation under
                // collect mode is single-valued: the first
                // matched build's CK fills the slot. Every
                // matched build's full payload is still
                // preserved inside the array via the
                // per-row `Value::Map` encoding above, so
                // nothing is lost — the slot simply mirrors
                // the first match's identity. Skipped under
                // `propagate_ck: driver`.
                if let Some(first_build) = first_collected_build.as_ref() {
                    crate::executor::copy_build_ck_columns(&mut rec, first_build, propagate_ck);
                }
                rec.set(&build_qualifier, Value::Array(arr));
                output_records.push((rec, rn));
                emitted_since_check += 1;
            }

            MatchMode::First | MatchMode::All => {
                // Residual-filter + emit pass. We
                // clone each surviving build record
                // before dropping the iterator so the
                // evaluator borrow doesn't alias the
                // hash-table borrow.
                let matched_records: Vec<Record> = {
                    let probe_iter = hash_table.probe(&probe_keys_buf);
                    let mut matched: Vec<Record> = Vec::new();
                    for candidate in probe_iter {
                        if let Some(residual) = decomposed.residual.as_ref() {
                            let resolver = CombineResolver::new(
                                &resolver_mapping,
                                &probe_record,
                                Some(candidate.record),
                            );
                            let mut residual_eval =
                                ProgramEvaluator::new(Arc::clone(residual), false);
                            match residual_eval
                                .eval_record::<NullStorage>(&eval_ctx, &resolver, None)
                            {
                                Ok(EvalResult::Skip(_)) => continue,
                                Ok(EvalResult::Emit { .. }) => {}
                                Ok(EvalResult::EmitMany { .. }) => {
                                    return Err(PipelineError::Internal {
                                                op: "combine residual",
                                                node: name.clone(),
                                                detail: "emit_each fan-out is not supported in a combine residual filter".into(),
                                            });
                                }
                                Err(e) => {
                                    if ctx.strategy == ErrorStrategy::FailFast {
                                        return Err(PipelineError::from(e));
                                    }
                                    dispatch_combine_output_error(
                                        ctx,
                                        node_idx,
                                        &probe_record,
                                        rn,
                                        Some(candidate.record),
                                        name,
                                        e,
                                    )?;
                                    continue 'driver;
                                }
                            }
                        }
                        matched.push(candidate.record.clone());
                        if matches!(match_mode, MatchMode::First) {
                            break;
                        }
                    }
                    matched
                };

                if matched_records.is_empty() {
                    // On-miss dispatch.
                    match on_miss {
                        OnMiss::Skip => {
                            continue;
                        }
                        OnMiss::Error => {
                            return Err(PipelineError::CombineMissingMatch {
                                combine: name.clone(),
                                driver_row: rn,
                            });
                        }
                        OnMiss::NullFields => {
                            // Evaluate body against a
                            // resolver whose build
                            // slot is None — build-
                            // qualified fields return
                            // Value::Null.
                            let resolver =
                                CombineResolver::new(&resolver_mapping, &probe_record, None);
                            let evaluator =
                                body_evaluator
                                    .as_mut()
                                    .ok_or_else(|| PipelineError::Internal {
                                        op: "combine",
                                        node: name.clone(),
                                        detail: "combine body typed program \
                                                         missing for on_miss: null_fields"
                                            .to_string(),
                                    })?;
                            match evaluator.eval_record::<NullStorage>(&eval_ctx, &resolver, None) {
                                Ok(EvalResult::Emit {
                                    fields: emitted,
                                    record_vars,
                                    ..
                                }) => {
                                    let mut rec = match combine_output_schema.as_ref() {
                                        Some(s) => widen_record_to_schema(&probe_record, s),
                                        None => probe_record.clone(),
                                    };
                                    for (n, v) in emitted {
                                        rec.set(&n, v);
                                    }
                                    for (k, v) in *record_vars {
                                        let _ = rec.set_record_var(&k, v);
                                    }
                                    output_records.push((rec, rn));
                                    emitted_since_check += 1;
                                }
                                Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                    ctx.counters.filtered_count += 1;
                                }
                                Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                    ctx.counters.distinct_count += 1;
                                }
                                Ok(EvalResult::EmitMany { .. }) => {
                                    return Err(PipelineError::Internal {
                                        op: "combine on_miss body",
                                        node: name.clone(),
                                        detail:
                                            "emit_each fan-out is not supported in a combine body"
                                                .into(),
                                    });
                                }
                                Err(e) => {
                                    if ctx.strategy == ErrorStrategy::FailFast {
                                        return Err(PipelineError::from(e));
                                    }
                                    // on_miss path: no build row
                                    // matched, so only the driver
                                    // source rewinds.
                                    dispatch_combine_output_error(
                                        ctx,
                                        node_idx,
                                        &probe_record,
                                        rn,
                                        None,
                                        name,
                                        e,
                                    )?;
                                    continue 'driver;
                                }
                            }
                        }
                    }
                } else if let Some(evaluator) = body_evaluator.as_mut() {
                    for matched in &matched_records {
                        let resolver =
                            CombineResolver::new(&resolver_mapping, &probe_record, Some(matched));
                        match evaluator.eval_record::<NullStorage>(&eval_ctx, &resolver, None) {
                            Ok(EvalResult::Emit {
                                fields: emitted,
                                record_vars,
                                ..
                            }) => {
                                let mut rec = match combine_output_schema.as_ref() {
                                    Some(s) => widen_record_to_schema(&probe_record, s),
                                    None => probe_record.clone(),
                                };
                                for (n, v) in emitted {
                                    rec.set(&n, v);
                                }
                                for (k, v) in *record_vars {
                                    let _ = rec.set_record_var(&k, v);
                                }
                                // Build-side `$ck.<field>` propagation
                                // for this matched row. Driver-only
                                // pipelines short-circuit inside the
                                // helper; the call is uniform across
                                // every emit site so the policy is one
                                // code path for the whole engine.
                                crate::executor::copy_build_ck_columns(
                                    &mut rec,
                                    matched,
                                    propagate_ck,
                                );
                                output_records.push((rec, rn));
                                emitted_since_check += 1;
                            }
                            Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                ctx.counters.filtered_count += 1;
                            }
                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                ctx.counters.distinct_count += 1;
                            }
                            Ok(EvalResult::EmitMany { .. }) => {
                                return Err(PipelineError::Internal {
                                    op: "combine matched body",
                                    node: name.clone(),
                                    detail: "emit_each fan-out is not supported in a combine body"
                                        .into(),
                                });
                            }
                            Err(e) => {
                                if ctx.strategy == ErrorStrategy::FailFast {
                                    return Err(PipelineError::from(e));
                                }
                                dispatch_combine_output_error(
                                    ctx,
                                    node_idx,
                                    &probe_record,
                                    rn,
                                    Some(matched),
                                    name,
                                    e,
                                )?;
                                continue 'driver;
                            }
                        }
                    }
                } else {
                    // Body-less synthetic step from
                    // N-ary combine decomposition: the
                    // step's encoded output schema
                    // concatenates driver columns then
                    // build columns, both in the order
                    // their `intermediate_row.fields()`
                    // walk produces. Emit one record
                    // per match by concatenating value
                    // slices and constructing on the
                    // encoded `Arc<Schema>`.
                    //
                    // No `copy_build_ck_columns` call here:
                    // build-side `$ck.<field>` values are
                    // already in the concatenated tail under
                    // their encoded names (`__<qualifier>__$ck.<field>`).
                    // The chain's final step then resolves
                    // them via `widen_record_to_schema`'s
                    // engine-stamped fallback when projecting
                    // onto the original output schema.
                    let target_schema =
                        combine_output_schema
                            .as_ref()
                            .ok_or_else(|| PipelineError::Internal {
                                op: "combine",
                                node: name.clone(),
                                detail: "synthetic combine step has no output \
                                             schema; decomposition pass did not run"
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
                                node: name.clone(),
                                detail: format!(
                                    "synthetic combine step produced {} \
                                             concatenated values; encoded schema \
                                             has {} columns",
                                    values.len(),
                                    target_schema.column_count()
                                ),
                            });
                        }
                        let rec = Record::new(Arc::clone(target_schema), values);
                        output_records.push((rec, rn));
                        emitted_since_check += 1;
                    }
                }
            }
        }

        // Budget check every 10K emitted records to
        // bound memory under fan-out. The build phase
        // polls `should_abort` every 10K inserts inside
        // `CombineHashTable::build`; this loop covers
        // the symmetric probe-side risk where a small
        // build × large driver fan-out can blow RSS
        // even though the table itself is bounded.
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
    // below are correctly skipped. Punctuations are dropped on the
    // inline path (matching the materialized inline admit, which
    // forwards no puncts), and the terminal writer drops them
    // regardless. The per-fold snapshot and hash-table consumer
    // are still released before the streaming return.
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
            Vec::new(),
            &charge,
        )?;
        ctx.combine_input_snapshots.remove(&node_idx);
        ctx.memory_budget.unregister_consumer(inline_consumer_id);
        return Ok(());
    }

    finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
    let nb = admit_node_buffer(
        ctx,
        name,
        node_idx,
        output_records,
        Vec::new(),
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx, nb);
    // Drop the per-fold cursor snapshot: every emitted record
    // has cleared into `node_buffers` without rewind, so the
    // snapshot is no longer needed. The rewind path on a
    // Combine-output-row failure reads and restores this entry
    // before clearing.
    ctx.combine_input_snapshots.remove(&node_idx);
    // The hash table goes out of scope when this arm returns;
    // unregister its consumer so the arbitrator's registry
    // tracks live tables only.
    ctx.memory_budget.unregister_consumer(inline_consumer_id);

    Ok(())
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
