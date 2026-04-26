//! Top-level DAG dispatcher.
//!
//! [`dispatch_plan_node`] reads `current_dag.graph[node_idx]` and routes
//! the node to its executor arm — Source materialization, Transform projection,
//! Route fan-out, Merge concatenation, Sort enforcement, Aggregation,
//! Combine, Composition pass-through, and Output writing. Mutable per-walk
//! state (node buffers, counters, DLQ, timers, output writers, output errors,
//! the visited-source set for the dual-counter semantic) lives on
//! [`ExecutorContext`]; immutable plan-time state (config, artifacts, run
//! params, the current DAG) is borrowed.
//!
//! The free-function shape is the entry point composition body recursion
//! will reuse: a body walk constructs an `ExecutorContext` whose
//! `current_dag` borrows the body's mini-DAG and feeds nodes through the
//! same dispatcher, mirroring DataFusion's `RecursiveQueryExec` pattern
//! where `recursive_term.execute(partition, Arc::clone(&task_context))`
//! re-enters the same execution loop with a different plan.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;

use clinker_record::{PipelineCounters, Record, SchemaBuilder, Value};
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, SkipReason, StableEvalContext};
use cxl::typecheck::TypedProgram;
use indexmap::IndexMap;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::config::{ErrorStrategy, OutputConfig, PipelineConfig};
use crate::error::PipelineError;
use crate::executor::schema_check::check_input_schema;
use crate::executor::{
    CompiledRoute, CompiledTransform, DlqEntry, NullStorage, build_format_writer,
    evaluate_single_transform, parse_memory_limit, stage_metrics, widen_record_to_schema,
};
use crate::pipeline::memory::MemoryBudget;
use crate::plan::bind_schema::CompileArtifacts;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};
use crate::projection::project_output_from_record;

/// Mutable per-walk and borrowed plan-time state passed to
/// [`dispatch_plan_node`].
///
/// Borrowed (immutable for the entire walk):
/// * `config`      — the YAML pipeline config (e.g. error strategy).
/// * `artifacts`   — compile-time CXL typed programs and combine metadata.
/// * `current_dag` — the DAG being walked. Composition body recursion will
///   later swap this in place to re-enter the dispatcher on a body's
///   mini-DAG without duplicating arm logic.
/// * `output_configs` / `primary_output` — output sinks and the
///   declaration-order primary used as the fallback projection target.
/// * `transform_by_name` — name → index into `compiled_transforms`.
/// * `compiled_transforms` — precompiled CXL programs for Transform and
///   Aggregation arms.
/// * `stable` / `source_file_arc` / `strategy` — pipeline-stable scalars
///   reused at every per-record dispatch site.
///
/// Owned (mutated across the walk):
/// * `node_buffers` — `(Record, row_num)` queues threaded between arms.
/// * `combine_source_records` — pre-loaded build-side source streams the
///   Source arm consults before falling back to `all_records`.
/// * `all_records` — primary driving stream materialized before the walk.
/// * `writers` — output writer registry consumed lazily as Output arms fire.
/// * `compiled_route` — cached evaluator for Route arms.
/// * `counters` / `dlq_entries` — pipeline-wide accounting.
/// * `output_errors` — collected sink failures so siblings still attempt
///   their writes (DataFusion collection-pattern PR #14439).
/// * `ok_source_rows` — distinct source rows that have reached at least
///   one Output, backing the dual-counter `counters.ok_count` semantic.
/// * `records_emitted` — drives stage-metric reporting at end of walk.
/// * Cumulative timers (`transform_timer`, `route_timer`,
///   `projection_timer`, `write_timer`) — accumulated under match-arm
///   guards.
/// * `collector` — stage-metrics collector receiving per-arm timing.
pub(crate) struct ExecutorContext<'a> {
    // Borrowed plan-time state.
    pub(crate) config: &'a PipelineConfig,
    pub(crate) artifacts: &'a CompileArtifacts,
    pub(crate) output_configs: &'a [OutputConfig],
    pub(crate) primary_output: &'a OutputConfig,
    pub(crate) compiled_transforms: &'a [CompiledTransform],
    pub(crate) transform_by_name: HashMap<&'a str, usize>,
    pub(crate) stable: &'a StableEvalContext,
    pub(crate) source_file_arc: &'a Arc<str>,
    pub(crate) strategy: ErrorStrategy,

    // Owned mutable per-walk state.
    pub(crate) node_buffers: HashMap<NodeIndex, Vec<(Record, u64)>>,
    pub(crate) combine_source_records: HashMap<String, Vec<(Record, u64)>>,
    pub(crate) all_records: Vec<(Record, u64)>,
    pub(crate) writers: HashMap<String, Box<dyn Write + Send>>,
    pub(crate) compiled_route: Option<CompiledRoute>,
    /// Per-route compiled evaluators keyed by route node name.
    /// Populated for body Routes (and any Route whose conditions
    /// must survive an explicit name lookup). The Route dispatcher
    /// arm checks this map first; if absent, it falls back to the
    /// `compiled_route` singleton — the long-standing single-route
    /// path the top-level executor uses.
    pub(crate) compiled_routes_by_name: HashMap<String, CompiledRoute>,
    /// Body-scope `input:` reference table installed by the body
    /// executor before each body walk and cleared afterward. The
    /// Route dispatcher arm consults this map (when present) to
    /// resolve branch successors instead of walking `ctx.config.nodes`,
    /// which is top-level only and would not see body siblings.
    /// `None` at the top level — the existing config-walk path
    /// covers every top-level node.
    pub(crate) current_body_node_input_refs: Option<HashMap<String, Vec<String>>>,
    pub(crate) counters: PipelineCounters,
    pub(crate) dlq_entries: Vec<DlqEntry>,
    pub(crate) output_errors: Vec<PipelineError>,
    pub(crate) ok_source_rows: HashSet<u64>,
    pub(crate) records_emitted: u64,
    pub(crate) transform_timer: stage_metrics::CumulativeTimer,
    pub(crate) route_timer: stage_metrics::CumulativeTimer,
    pub(crate) projection_timer: stage_metrics::CumulativeTimer,
    pub(crate) write_timer: stage_metrics::CumulativeTimer,
    pub(crate) collector: &'a mut stage_metrics::StageCollector,

    /// Composition-body recursion depth. Incremented inside
    /// `execute_composition_body` before recursing on the body's
    /// mini-DAG and decremented at every exit path. Initialized to
    /// 0 at top-level `execute_dag_branching`; never read by
    /// non-Composition arms. The Composition arm in
    /// `dispatch_plan_node` checks this against
    /// `MAX_COMPOSITION_DEPTH` before recursing and emits E112 on
    /// overflow.
    pub(crate) recursion_depth: u32,
}

/// Execute one DAG node by routing it to its arm.
///
/// Reads the node by `node_idx` from `current_dag.graph` and dispatches
/// on `PlanNode` variant. Each arm reads from and writes to `ctx.node_buffers`
/// and updates the cumulative counters / timers. Errors short-circuit only
/// for invariant violations and `ErrorStrategy::FailFast` runtime failures;
/// per-record DLQ-able errors land in `ctx.dlq_entries` under
/// `Continue`/`BestEffort`. Output sink errors are collected into
/// `ctx.output_errors` instead of short-circuiting so sibling outputs still
/// get their chance to fail (and be reported) — the caller aggregates after
/// the walk.
pub(crate) fn dispatch_plan_node(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
) -> Result<(), PipelineError> {
    let node = current_dag.graph[node_idx].clone();
    match node {
        PlanNode::Source { ref name, .. } => {
            // Combine build-side sources get their own pre-loaded
            // records; every other source falls back to the
            // primary driving reader's stream. Records are
            // canonicalized onto the Source's plan-time
            // `Arc<Schema>` so every downstream operator hits
            // the `Arc::ptr_eq` fast path on the first record.
            // Structural equality holds by construction:
            // `CoercingReader` builds its Arc from the same
            // declared `schema:` block that `bind_schema` reads
            // to populate `PlanNode::Source.output_schema`.
            let source_schema = current_dag.graph[node_idx].stored_output_schema().cloned();
            let canonicalize = |r: &Record| -> Record {
                match source_schema.as_ref() {
                    Some(target) => {
                        if Arc::ptr_eq(r.schema(), target) {
                            r.clone()
                        } else {
                            debug_assert_eq!(
                                r.schema().columns(),
                                target.columns(),
                                "Source reader Arc must match plan-time declared columns",
                            );
                            let mut rebuilt = Record::new(Arc::clone(target), r.values().to_vec());
                            for (k, v) in r.iter_meta() {
                                let _ = rebuilt.set_meta(k, v.clone());
                            }
                            rebuilt
                        }
                    }
                    None => r.clone(),
                }
            };
            let records: Vec<_> =
                if let Some(src_recs) = ctx.combine_source_records.get(name.as_str()) {
                    src_recs
                        .iter()
                        .map(|(r, rn)| (canonicalize(r), *rn))
                        .collect()
                } else {
                    ctx.all_records
                        .iter()
                        .map(|(r, rn)| (canonicalize(r), *rn))
                        .collect()
                };
            ctx.node_buffers.insert(node_idx, records);
        }

        PlanNode::Transform { ref name, .. } => {
            // Get input records: first check own buffer (set by Route
            // node for branch dispatch), then fall back to predecessor.
            let input_records = if let Some(own_buf) = ctx.node_buffers.remove(&node_idx) {
                own_buf
            } else {
                let predecessors: Vec<NodeIndex> = current_dag
                    .graph
                    .neighbors_directed(node_idx, Direction::Incoming)
                    .collect();
                if predecessors.len() == 1 {
                    ctx.node_buffers
                        .remove(&predecessors[0])
                        .unwrap_or_default()
                } else {
                    predecessors
                        .iter()
                        .find_map(|p| ctx.node_buffers.remove(p))
                        .unwrap_or_default()
                }
            };

            // Find the CompiledTransform for this node
            let transform_idx = match ctx.transform_by_name.get(name.as_str()) {
                Some(&idx) => idx,
                None => {
                    // No transform found — pass through
                    ctx.node_buffers.insert(node_idx, input_records);
                    return Ok(());
                }
            };

            let mut evaluator = ProgramEvaluator::new(
                Arc::clone(&ctx.compiled_transforms[transform_idx].typed),
                ctx.compiled_transforms[transform_idx].has_distinct(),
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

            let mut output_records = Vec::with_capacity(input_records.len());

            for (record, rn) in input_records {
                if let Some(exp) = expected_input.as_ref() {
                    check_input_schema(exp, record.schema(), name, "transform", &upstream_name)?;
                }
                let eval_ctx = EvalContext {
                    stable: ctx.stable,
                    source_file: ctx.source_file_arc,
                    source_row: rn,
                };

                let target_schema = output_schema
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| Arc::clone(record.schema()));
                let eval_result = {
                    let _guard = ctx.transform_timer.guard();
                    evaluate_single_transform(
                        &record,
                        name,
                        &mut evaluator,
                        &eval_ctx,
                        &target_schema,
                    )
                };
                match eval_result {
                    Ok((modified_record, Ok(()))) => {
                        output_records.push((modified_record, rn));
                    }
                    Ok((_record, Err(SkipReason::Filtered))) => {
                        ctx.counters.filtered_count += 1;
                    }
                    Ok((_record, Err(SkipReason::Duplicate))) => {
                        ctx.counters.distinct_count += 1;
                    }
                    Err((transform_name, eval_err)) => {
                        if ctx.strategy == ErrorStrategy::FailFast {
                            return Err(eval_err.into());
                        }
                        ctx.counters.dlq_count += 1;
                        ctx.dlq_entries.push(DlqEntry {
                            source_row: rn,
                            category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                            error_message: eval_err.to_string(),
                            original_record: record,
                            stage: Some(DlqEntry::stage_transform(&transform_name)),
                            route: None,
                            trigger: true,
                        });
                    }
                }
            }

            ctx.node_buffers.insert(node_idx, output_records);
        }

        PlanNode::Route {
            ref name,
            mode,
            branches: _,
            default: _,
            ..
        } => {
            // Body-context Routes that consume an input port have no
            // predecessor in the body's mini-DAG — the records are
            // seeded into this node's own buffer at composition entry.
            // Check own buffer first, fall back to predecessor.
            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            let input_records = if let Some(own_buf) = ctx.node_buffers.remove(&node_idx) {
                own_buf
            } else {
                predecessors
                    .iter()
                    .find_map(|p| ctx.node_buffers.remove(p))
                    .unwrap_or_default()
            };

            if let Some(expected) = current_dag.graph[node_idx]
                .expected_input_schema_in(current_dag)
                .cloned()
            {
                let upstream_name = predecessors
                    .first()
                    .map(|&i| current_dag.graph[i].name().to_string())
                    .unwrap_or_default();
                for (record, _) in &input_records {
                    check_input_schema(&expected, record.schema(), name, "route", &upstream_name)?;
                }
            }

            // Get successor nodes (branch transform nodes)
            let successors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Outgoing)
                .collect();

            // Build branch_name -> successor NodeIndex mapping.
            //
            // Under the unified taxonomy, a
            // `PlanNode::Route` is a first-class node named as the
            // user wrote it in YAML (e.g. `classify_route`). Successors
            // may be Transforms, Aggregates, Outputs, or Compositions —
            // any node whose `input:` is declared as
            // `<route_name>.<branch>` (NodeInput::Port form). We
            // can't use `build_transform_specs` alone because it
            // excludes Outputs and Compositions.
            //
            // Walk `config.nodes` once, read each node's
            // `header.input`, match against `<route_parent>.<branch>`,
            // and map the branch to the successor's NodeIndex.
            let route_parent = name.strip_prefix("route_").unwrap_or(name);
            let mut succ_by_name: HashMap<String, NodeIndex> = HashMap::new();
            for &succ in &successors {
                succ_by_name.insert(current_dag.graph[succ].name().to_string(), succ);
            }
            let mut branch_to_succ: HashMap<String, NodeIndex> = HashMap::new();
            // Iterate either the body's input-ref table (when set
            // by the body executor for the current walk) or the
            // top-level config nodes. The shape is the same:
            // `(consumer_name, input_ref)` pairs.
            let mut name_input_pairs: Vec<(String, String)> = Vec::new();
            if let Some(body_refs) = ctx.current_body_node_input_refs.as_ref() {
                for (consumer, refs) in body_refs {
                    for r in refs {
                        name_input_pairs.push((consumer.clone(), r.clone()));
                    }
                }
            } else {
                use crate::config::PipelineNode;
                use crate::config::node_header::NodeInput;
                for spanned in &ctx.config.nodes {
                    let (node_name, input_ref) = match &spanned.value {
                        PipelineNode::Transform { header, .. }
                        | PipelineNode::Aggregate { header, .. }
                        | PipelineNode::Route { header, .. }
                        | PipelineNode::Output { header, .. } => {
                            let ir = match &header.input.value {
                                NodeInput::Single(s) => s.clone(),
                                NodeInput::Port { node, port } => format!("{node}.{port}"),
                            };
                            (header.name.clone(), ir)
                        }
                        _ => continue,
                    };
                    name_input_pairs.push((node_name, input_ref));
                }
            }
            for (node_name, input_ref) in name_input_pairs {
                // Port form: "<route>.<branch>" — branch name comes
                // from the port suffix. Standard for named branches
                // on routes with multiple downstream consumers per
                // branch.
                if let Some(branch) = input_ref.strip_prefix(&format!("{}.", route_parent))
                    && let Some(&succ) = succ_by_name.get(&node_name)
                {
                    branch_to_succ.insert(branch.to_string(), succ);
                    continue;
                }
                // Bare form: `input: <route>` on an Output whose
                // *name* matches a Route branch (or the Route
                // `default:`). Conventional shorthand when each
                // branch has exactly one downstream consumer and
                // the consumer's name is the branch name.
                if input_ref == route_parent
                    && let Some(&succ) = succ_by_name.get(&node_name)
                {
                    branch_to_succ.insert(node_name.clone(), succ);
                }
            }

            // Initialize per-successor buffers
            let mut branch_buffers: HashMap<NodeIndex, Vec<_>> = HashMap::new();
            for &succ in &successors {
                branch_buffers.insert(succ, Vec::new());
            }

            // Per-route compiled evaluator wins over the singleton
            // — `compiled_routes_by_name` is populated for body
            // Routes (and any Route whose conditions need an explicit
            // lookup), while the singleton remains the long-standing
            // path for the top-level Route. A body's Route arm
            // therefore finds its own conditions here instead of
            // inheriting the top-level Route's conditions, which
            // would have been the only ones available before.
            // `take` + restore keeps the route's `&mut`-mutated state
            // (regex caches, evaluator counters) consistent across
            // records hitting the same Route node within one walk.
            let from_map = ctx.compiled_routes_by_name.remove(name.as_str());
            let mut from_singleton_flag = false;
            let mut route_handle = match from_map {
                Some(r) => Some(r),
                None => {
                    let taken = ctx.compiled_route.take();
                    from_singleton_flag = taken.is_some();
                    taken
                }
            };

            if let Some(ref mut route) = route_handle {
                for (record, rn) in input_records {
                    let eval_ctx = EvalContext {
                        stable: ctx.stable,
                        source_file: ctx.source_file_arc,
                        source_row: rn,
                    };

                    let route_result = {
                        let _guard = ctx.route_timer.guard();
                        route.evaluate(&record, &eval_ctx)
                    };
                    match route_result {
                        Ok(targets) => {
                            for target in &targets {
                                if let Some(&succ) = branch_to_succ.get(target.as_str()) {
                                    branch_buffers
                                        .entry(succ)
                                        .or_default()
                                        .push((record.clone(), rn));
                                }
                                // Exclusive mode: stop after first match
                                if mode == crate::config::RouteMode::Exclusive {
                                    break;
                                }
                            }
                        }
                        Err(route_err) => {
                            if ctx.strategy == ErrorStrategy::FailFast {
                                return Err(route_err.into());
                            }
                            ctx.counters.dlq_count += 1;
                            ctx.dlq_entries.push(DlqEntry {
                                source_row: rn,
                                category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                error_message: route_err.to_string(),
                                original_record: record,
                                stage: Some(DlqEntry::stage_route_eval()),
                                route: None,
                                trigger: true,
                            });
                        }
                    }
                }
            }
            // Restore the route to whichever storage it came from.
            if let Some(r) = route_handle {
                if from_singleton_flag {
                    ctx.compiled_route = Some(r);
                } else {
                    ctx.compiled_routes_by_name.insert(name.to_string(), r);
                }
            }

            // Put branch buffers into node_buffers keyed by successor
            for (succ_idx, buf) in branch_buffers {
                ctx.node_buffers.insert(succ_idx, buf);
            }
        }

        PlanNode::Merge { ref name, .. } => {
            // Concatenate predecessor buffers in declaration order —
            // the order appearing in the Merge's `inputs:` YAML
            // array, which is stable across compile runs. Under
            // the unified taxonomy a Merge is a first-class node
            // (no "merge_<transform>" synthesis) so we read the
            // order straight off `PipelineNode::Merge.header.inputs`.
            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();

            let declaration_order: Vec<String> = {
                use crate::config::PipelineNode;
                use crate::config::node_header::NodeInput;
                ctx.config
                    .nodes
                    .iter()
                    .find_map(|spanned| match &spanned.value {
                        PipelineNode::Merge { header, .. } if header.name == *name => Some(
                            header
                                .inputs
                                .iter()
                                .map(|ni| match &ni.value {
                                    NodeInput::Single(s) => s.clone(),
                                    NodeInput::Port { node, port } => {
                                        format!("{node}.{port}")
                                    }
                                })
                                .collect(),
                        ),
                        _ => None,
                    })
                    .unwrap_or_default()
            };

            // Sort predecessors by declaration order
            let mut sorted_preds = predecessors.clone();
            sorted_preds.sort_by_key(|p| {
                let pred_name = current_dag.graph[*p].name();
                declaration_order
                    .iter()
                    .position(|d| d == pred_name)
                    .unwrap_or(usize::MAX)
            });

            let total: usize = sorted_preds
                .iter()
                .map(|p| ctx.node_buffers.get(p).map_or(0, |b| b.len()))
                .sum();
            let merge_output_schema = current_dag.graph[node_idx].stored_output_schema().cloned();
            let mut merged = Vec::with_capacity(total);
            for pred in &sorted_preds {
                let upstream_name = current_dag.graph[*pred].name().to_string();
                if let Some(buf) = ctx.node_buffers.remove(pred) {
                    for (mut record, rn) in buf {
                        if let Some(canonical) = merge_output_schema.as_ref() {
                            check_input_schema(
                                canonical,
                                record.schema(),
                                name,
                                "merge",
                                &upstream_name,
                            )?;
                            // Canonicalize: rebuild record with the
                            // Merge's `Arc<Schema>` so downstream
                            // operators hit the ptr_eq fast path
                            // regardless of which input the record
                            // originated from.
                            let values = record.values().to_vec();
                            let mut rebuilt = Record::new(Arc::clone(canonical), values);
                            for (k, v) in record.iter_meta() {
                                let _ = rebuilt.set_meta(k, v.clone());
                            }
                            record = rebuilt;
                        }
                        merged.push((record, rn));
                    }
                }
            }
            ctx.node_buffers.insert(node_idx, merged);
        }

        PlanNode::Sort {
            ref name,
            ref sort_fields,
            ..
        } => {
            // Enforcer-sort dispatch. Carries `row_num` through
            // the sort permutation as the `SortBuffer<u64>`
            // payload — the Record itself carries every field
            // value, emitted content, and metadata, so no
            // parallel bookkeeping map rides alongside.
            use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};

            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            let input_records: Vec<_> = predecessors
                .iter()
                .find_map(|p| ctx.node_buffers.remove(p))
                .unwrap_or_default();

            if input_records.is_empty() {
                ctx.node_buffers.insert(node_idx, Vec::new());
                return Ok(());
            }

            let schema = input_records[0].0.schema().clone();
            let memory_limit = parse_memory_limit(ctx.config);
            let mut buf: SortBuffer<u64> =
                SortBuffer::new(sort_fields.clone(), memory_limit, None, schema);

            let sort_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
            let sort_count = input_records.len() as u64;
            for (record, row_num) in input_records {
                buf.push(record, row_num);
                if buf.should_spill() {
                    buf.sort_and_spill().map_err(|e| {
                        PipelineError::Io(std::io::Error::other(format!(
                            "sort enforcer '{name}' spill failed: {e}"
                        )))
                    })?;
                }
            }

            let sorted = buf.finish().map_err(|e| {
                PipelineError::Io(std::io::Error::other(format!(
                    "sort enforcer '{name}' finish failed: {e}"
                )))
            })?;

            let mut out: Vec<(Record, u64)> = Vec::with_capacity(sort_count as usize);
            match sorted {
                SortedOutput::InMemory(pairs) => {
                    for (record, row_num) in pairs {
                        out.push((record, row_num));
                    }
                }
                SortedOutput::Spilled(files) => {
                    // Spill files are individually sorted but not
                    // globally merged. For correctness when multiple
                    // spill files exist, a k-way merge is required.
                    // Single-file fast path is exact.
                    if files.len() > 1 {
                        return Err(PipelineError::Io(std::io::Error::other(format!(
                            "sort enforcer '{name}' produced {} spill files; \
                             k-way merge for enforcer sort is not yet implemented \
                             (memory_limit too small for input)",
                            files.len()
                        ))));
                    }
                    for file in files {
                        let reader = file.reader().map_err(|e| {
                            PipelineError::Io(std::io::Error::other(format!(
                                "sort enforcer '{name}' spill read failed: {e}"
                            )))
                        })?;
                        for entry in reader {
                            let (record, row_num) = entry.map_err(|e| {
                                PipelineError::Io(std::io::Error::other(format!(
                                    "sort enforcer '{name}' spill decode failed: {e}"
                                )))
                            })?;
                            out.push((record, row_num));
                        }
                    }
                }
            }
            ctx.collector
                .record(sort_timer.finish(sort_count, sort_count));
            ctx.node_buffers.insert(node_idx, out);
        }

        PlanNode::Aggregation {
            ref name,
            ref compiled,
            strategy: agg_strategy,
            ref output_schema,
            ..
        } => {
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

            let pred =
                crate::executor::single_predecessor(current_dag, node_idx, "aggregation", name)?;
            let input = ctx.node_buffers.remove(&pred).unwrap_or_default();

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
                Some(idx) => ProgramEvaluator::new(
                    Arc::clone(&ctx.compiled_transforms[idx].typed),
                    ctx.compiled_transforms[idx].has_distinct(),
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

            let memory_limit = parse_memory_limit(ctx.config);

            let mut stream = crate::aggregation::AggregateStream::for_node(
                Arc::clone(compiled),
                evaluator,
                agg_strategy,
                Arc::clone(output_schema),
                spill_schema,
                memory_limit,
                None,
                name.clone(),
            )?;

            let agg_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
            let input_count = input.len() as u64;

            let mut emitted_rows: Vec<AggSortRow> = Vec::with_capacity(64);
            for (record, row_num) in &input {
                let eval_ctx = EvalContext {
                    stable: ctx.stable,
                    source_file: ctx.source_file_arc,
                    source_row: *row_num,
                };
                if let Err(e) = stream.add_record(record, *row_num, &eval_ctx, &mut emitted_rows) {
                    match ctx.config.error_handling.strategy {
                        ErrorStrategy::FailFast => return Err(e.into()),
                        ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                            ctx.counters.dlq_count += 1;
                            ctx.dlq_entries.push(DlqEntry {
                                source_row: *row_num,
                                category: crate::dlq::DlqErrorCategory::AggregateFinalize,
                                error_message: format!("aggregate {name}: {e}"),
                                original_record: record.clone(),
                                stage: Some(crate::dlq::stage_aggregate(name)),
                                route: None,
                                trigger: true,
                            });
                        }
                    }
                }
            }

            // Finalize. Accumulator finalize errors get the
            // typed `PipelineError::Accumulator` mapping; under
            // `Continue` we route to the DLQ and emit zero rows
            // for the failed group. All other engine errors
            // propagate (Internal/Spill/Residual always abort).
            let finalize_ctx = EvalContext {
                stable: ctx.stable,
                source_file: ctx.source_file_arc,
                source_row: 0,
            };
            let out_rows: Vec<AggSortRow> = match stream.finalize(&finalize_ctx, &mut emitted_rows)
            {
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
                        ctx.counters.dlq_count += 1;
                        let synthetic = if let Some((rec, _)) = input.first() {
                            rec.clone()
                        } else {
                            Record::new(Arc::clone(output_schema), Vec::new())
                        };
                        ctx.dlq_entries.push(DlqEntry {
                            source_row: 0,
                            category: crate::dlq::DlqErrorCategory::AggregateFinalize,
                            error_message: format!("aggregate {transform}.{binding}: {source:?}"),
                            original_record: synthetic,
                            stage: Some(crate::dlq::stage_aggregate(name)),
                            route: None,
                            trigger: true,
                        });
                        Vec::new()
                    }
                },
                Err(other) => return Err(other.into()),
            };

            ctx.collector
                .record(agg_timer.finish(input_count, out_rows.len() as u64));
            ctx.node_buffers.insert(node_idx, out_rows);
        }

        PlanNode::Output { ref name, .. } => {
            // Get input records: check own buffer first (Route
            // nodes store records at the successor's index), then
            // fall back to predecessor buffers.
            let input_records = if let Some(own_buf) = ctx.node_buffers.remove(&node_idx) {
                own_buf
            } else {
                let predecessors: Vec<NodeIndex> = current_dag
                    .graph
                    .neighbors_directed(node_idx, Direction::Incoming)
                    .collect();
                predecessors
                    .iter()
                    .find_map(|&p| {
                        // When multiple outputs share a predecessor,
                        // clone the buffer for all but the last
                        // consumer to avoid starving siblings.
                        let remaining_consumers = current_dag
                            .graph
                            .neighbors_directed(p, Direction::Outgoing)
                            .filter(|&succ| succ > node_idx)
                            .count();
                        if remaining_consumers == 0 {
                            ctx.node_buffers.remove(&p)
                        } else {
                            ctx.node_buffers.get(&p).cloned()
                        }
                    })
                    .unwrap_or_default()
            };

            if let Some(expected) = current_dag.graph[node_idx]
                .expected_input_schema_in(current_dag)
                .cloned()
            {
                let upstream_name = current_dag
                    .graph
                    .neighbors_directed(node_idx, Direction::Incoming)
                    .next()
                    .map(|i| current_dag.graph[i].name().to_string())
                    .unwrap_or_default();
                for (record, _) in &input_records {
                    check_input_schema(&expected, record.schema(), name, "output", &upstream_name)?;
                }
            }

            // Dual counters:
            //
            // * `records_written` increments per WRITE — under
            //   inclusive Route fan-out, one input matching N
            //   branches counts N (one per Output that received
            //   it). Aligns with per-Output throughput and the
            //   `records_emitted` local that drives stage-metric
            //   reporting.
            //
            // * `ok_count` increments by the number of DISTINCT
            //   source rows reaching this Output that haven't
            //   already been counted at another Output during
            //   the same DAG walk. Source identity is
            //   `row_num` (per-source counter), tracked across
            //   all Output arms via the `ok_source_rows` set
            //   declared at function scope.
            let output_record_count = input_records.len() as u64;
            let mut newly_ok: u64 = 0;
            for (_, row_num) in &input_records {
                if ctx.ok_source_rows.insert(*row_num) {
                    newly_ok += 1;
                }
            }
            ctx.counters.ok_count += newly_ok;
            ctx.counters.records_written += output_record_count;
            ctx.records_emitted += output_record_count;

            // Derive output schema from first emitted record.
            // The Record is authoritative post-rip; materialize
            // the output-projection's `emitted` / `metadata`
            // maps from it on demand at this boundary. That
            // pays the bucket-insert cost once per record
            // reaching the writer, not every intermediate node
            // transition (Invariant 3).
            let scan_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
            let out_cfg = ctx
                .output_configs
                .iter()
                .find(|o| o.name == *name)
                .unwrap_or(ctx.primary_output);

            let output_schema = if let Some((rec, _)) = input_records.first() {
                let projected = {
                    let _guard = ctx.projection_timer.guard();
                    project_output_from_record(rec, out_cfg)
                };
                Arc::clone(projected.schema())
            } else {
                // No records to write — use empty schema
                ctx.collector.record(scan_timer.finish(0, 0));
                return Ok(());
            };

            // Find and take the writer for this output. Errors
            // from build_format_writer / write_record / flush are
            // captured into `output_errors` instead of
            // short-circuiting via `?` so siblings still get
            // their chance to fail (and be reported).
            if let Some(raw_writer) = ctx.writers.remove(name) {
                match build_format_writer(out_cfg, raw_writer, Arc::clone(&output_schema)) {
                    Ok(mut csv_writer) => {
                        ctx.collector.record(scan_timer.finish(1, 1));
                        let mut write_failed = false;
                        for (record, _rn) in &input_records {
                            let projected = {
                                let _guard = ctx.projection_timer.guard();
                                project_output_from_record(record, out_cfg)
                            };
                            let write_result = {
                                let _guard = ctx.write_timer.guard();
                                csv_writer.write_record(&projected)
                            };
                            if let Err(e) = write_result {
                                ctx.output_errors.push(PipelineError::from(e));
                                write_failed = true;
                                break;
                            }
                        }
                        if !write_failed {
                            let flush_result = {
                                let _guard = ctx.write_timer.guard();
                                csv_writer.flush()
                            };
                            if let Err(e) = flush_result {
                                ctx.output_errors.push(PipelineError::from(e));
                            }
                        }
                    }
                    Err(e) => ctx.output_errors.push(e),
                }
            }
        }

        PlanNode::Composition { ref name, body, .. } => {
            // Recursive body execution: collect parent-scope records
            // per declared input port, swap `current_dag` to the body's
            // mini-DAG, walk the body's topo, then collect the body's
            // first declared output port and write it to this node's
            // buffer in the parent scope. The dispatcher arm logic
            // never diverged across body and top-level walks — both
            // run through `dispatch_plan_node` after a current_dag
            // swap, mirroring DataFusion's `RecursiveQueryExec` pattern
            // where the recursive term re-enters the same execution
            // loop with a different plan.
            debug_assert_ne!(
                body,
                crate::plan::composition_body::CompositionBodyId::SENTINEL,
                "composition {name:?}: body_id is sentinel — bind_composition did not run"
            );

            let bound_body = ctx
                .artifacts
                .body_of(body)
                .ok_or_else(|| PipelineError::compose_body_missing(name.clone()))?;

            // Schema-check parent records before stepping into the
            // body. Failures here surface with the parent-scope
            // upstream name, matching the diagnostic shape every
            // other arm emits at its own entry.
            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            if let Some(expected) = current_dag.graph[node_idx]
                .expected_input_schema_in(current_dag)
                .cloned()
            {
                let upstream_name = predecessors
                    .first()
                    .map(|&i| current_dag.graph[i].name().to_string())
                    .unwrap_or_default();
                // Peek-only schema check; the records are still owned
                // by their producer's buffer until `collect_port_records`
                // claims them below.
                if let Some(&first_pred) = predecessors.first()
                    && let Some(records) = ctx.node_buffers.get(&first_pred)
                {
                    for (record, _) in records {
                        check_input_schema(
                            &expected,
                            record.schema(),
                            name,
                            "composition",
                            &upstream_name,
                        )?;
                    }
                }
            }

            // Depth guard before recursion — same constant the
            // compile-time IsolatedFromAbove check uses, distinct
            // emission code for log greppability.
            if ctx.recursion_depth >= crate::plan::bind_schema::MAX_COMPOSITION_DEPTH {
                return Err(PipelineError::compose_depth_exceeded(
                    name.clone(),
                    ctx.recursion_depth,
                ));
            }

            let port_records = collect_port_records(ctx, current_dag, bound_body, node_idx)?;
            let composition_name = name.clone();
            let output_records =
                execute_composition_body(ctx, body, port_records, &composition_name)?;
            ctx.node_buffers.insert(node_idx, output_records);
        }

        PlanNode::Combine {
            ref name,
            ref strategy,
            ref driving_input,
            ref match_mode,
            ref on_miss,
            ref resolved_column_map,
            ..
        } => {
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
            let decomposed = ctx.artifacts.combine_predicates.get(name).ok_or_else(|| {
                PipelineError::Internal {
                    op: "combine",
                    node: name.clone(),
                    detail: "no combine_predicates entry for combine node".to_string(),
                }
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
            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            let driver_pred = predecessors
                .iter()
                .copied()
                .find(|p| current_dag.graph[*p].name() == driver_upstream)
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
                .find(|p| current_dag.graph[*p].name() == build_upstream)
                .ok_or_else(|| PipelineError::Internal {
                    op: "combine",
                    node: name.clone(),
                    detail: format!(
                        "combine build upstream {build_upstream:?} is not an \
                         incoming neighbor in the DAG"
                    ),
                })?;

            let driver_buf = ctx.node_buffers.remove(&driver_pred).unwrap_or_default();
            let build_buf = ctx.node_buffers.remove(&build_pred).unwrap_or_default();

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
                    let mut budget =
                        MemoryBudget::from_config(ctx.config.pipeline.memory_limit.as_deref());
                    let build_records: Vec<Record> =
                        build_buf.into_iter().map(|(r, _)| r).collect();
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
                    let iejoin_ctx = EvalContext {
                        stable: ctx.stable,
                        source_file: ctx.source_file_arc,
                        source_row: 0,
                    };
                    let output_records = execute_combine_iejoin(IEJoinExec {
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
                        ctx: &iejoin_ctx,
                        budget: &mut budget,
                    })?;
                    let probe_records_out = output_records.len() as u64;
                    ctx.collector
                        .record(probe_timer.finish(probe_records_in, probe_records_out));
                    ctx.node_buffers.insert(node_idx, output_records);
                    return Ok(());
                }
                Dispatch::Grace(partition_bits) => {
                    let mut budget =
                        MemoryBudget::from_config(ctx.config.pipeline.memory_limit.as_deref());
                    let build_records: Vec<Record> =
                        build_buf.into_iter().map(|(r, _)| r).collect();
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
                    let grace_ctx = EvalContext {
                        stable: ctx.stable,
                        source_file: ctx.source_file_arc,
                        source_row: 0,
                    };
                    let output_records = execute_combine_grace_hash(GraceHashExec {
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
                        ctx: &grace_ctx,
                        budget: &mut budget,
                    })?;
                    let probe_records_out = output_records.len() as u64;
                    ctx.collector
                        .record(probe_timer.finish(probe_records_in, probe_records_out));
                    ctx.node_buffers.insert(node_idx, output_records);
                    return Ok(());
                }
                Dispatch::SortMerge => {
                    // SortMerge is selected by the planner only
                    // for pure-range predicates whose inputs
                    // already arrive sorted on the range key
                    // prefix. The kernel's `presorted: true`
                    // path skips Phase A external sort and walks
                    // the inputs in place via the two-cursor
                    // merge.
                    let mut budget =
                        MemoryBudget::from_config(ctx.config.pipeline.memory_limit.as_deref());
                    let build_records: Vec<Record> =
                        build_buf.into_iter().map(|(r, _)| r).collect();
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
                    let sm_ctx = EvalContext {
                        stable: ctx.stable,
                        source_file: ctx.source_file_arc,
                        source_row: 0,
                    };
                    let output_records = execute_combine_sort_merge(SortMergeExec {
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
                        ctx: &sm_ctx,
                        budget: &mut budget,
                    })?;
                    let probe_records_out = output_records.len() as u64;
                    ctx.collector
                        .record(probe_timer.finish(probe_records_in, probe_records_out));
                    ctx.node_buffers.insert(node_idx, output_records);
                    return Ok(());
                }
                Dispatch::Inline => {}
            }

            // Hash-build phase — drain the build buffer into a
            // fresh MemoryBudget-governed CombineHashTable. The
            // stage timer covers the full build walk; on a
            // budget abort the timer is dropped without
            // recording (matches the `StageTimer` "no report on
            // error" contract documented at its definition).
            let mut budget = MemoryBudget::from_config(ctx.config.pipeline.memory_limit.as_deref());
            let build_records: Vec<Record> = build_buf.into_iter().map(|(r, _)| r).collect();
            let build_records_in = build_records.len() as u64;
            let estimated_rows = Some(build_records.len());
            let hash_table_ctx = EvalContext {
                stable: ctx.stable,
                source_file: ctx.source_file_arc,
                source_row: 0,
            };
            let build_timer =
                stage_metrics::StageTimer::new(stage_metrics::StageName::CombineBuild {
                    name: name.clone(),
                });
            let hash_table = CombineHashTable::build(
                build_records,
                &build_extractor,
                &hash_table_ctx,
                &mut budget,
                estimated_rows,
            )
            .map_err(|e| PipelineError::Compilation {
                transform_name: name.clone(),
                messages: vec![format!("E310 combine build: {e}")],
            })?;
            let build_records_out = hash_table.len() as u64;
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
            let probe_timer =
                stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
                    name: name.clone(),
                });
            let mut output_records = Vec::with_capacity(driver_buf.len());
            let mut emitted_since_check: usize = 0;
            // Reused across every probe iteration to avoid an
            // allocation per driver row. `KeyExtractor::extract`
            // pushes into the end; we clear before each call.
            let mut probe_keys_buf: Vec<Value> = Vec::with_capacity(probe_extractor.len());

            for (probe_record, rn) in driver_buf {
                let eval_ctx = EvalContext {
                    stable: ctx.stable,
                    source_file: ctx.source_file_arc,
                    source_row: rn,
                };

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
                let probe_key_resolver =
                    CombineResolver::new(&resolver_mapping, &probe_record, None);
                probe_keys_buf.clear();
                probe_extractor
                    .extract_into(&eval_ctx, &probe_key_resolver, &mut probe_keys_buf)
                    .map_err(|e| PipelineError::Compilation {
                        transform_name: name.clone(),
                        messages: vec![format!("combine probe key eval error: {e}")],
                    })?;

                match match_mode {
                    MatchMode::Collect => {
                        // Synthesize the output directly. No
                        // body evaluator runs. On miss, emit
                        // an empty array (E311-guarded: the
                        // body is enforced empty at compile
                        // time, so on_miss policy does not
                        // bypass emission under Collect).
                        let mut arr: Vec<Value> = Vec::new();
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
                                let mut residual_eval =
                                    ProgramEvaluator::new(Arc::clone(residual), false);
                                match residual_eval
                                    .eval_record::<NullStorage>(&eval_ctx, &resolver, None)
                                {
                                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                        continue;
                                    }
                                    Ok(EvalResult::Emit { .. }) => {}
                                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                        continue;
                                    }
                                    Err(e) => {
                                        return Err(PipelineError::from(e));
                                    }
                                }
                            }
                            if arr.len() >= COLLECT_PER_GROUP_CAP {
                                truncated = true;
                                break;
                            }
                            // Build a Value::Map for every
                            // matched build record, preserving
                            // its own schema order.
                            let mut m: IndexMap<Box<str>, Value> = IndexMap::new();
                            for (fname, val) in candidate.record.iter_all_fields() {
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
                                        Err(e) => {
                                            return Err(PipelineError::from(e));
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
                                    return Err(PipelineError::Compilation {
                                        transform_name: name.clone(),
                                        messages: vec![format!(
                                            "E310 combine on_miss: error — no \
                                             matching build row for driver row {rn}"
                                        )],
                                    });
                                }
                                OnMiss::NullFields => {
                                    // Evaluate body against a
                                    // resolver whose build
                                    // slot is None — build-
                                    // qualified fields return
                                    // Value::Null.
                                    let resolver = CombineResolver::new(
                                        &resolver_mapping,
                                        &probe_record,
                                        None,
                                    );
                                    let evaluator = body_evaluator.as_mut().ok_or_else(|| {
                                        PipelineError::Internal {
                                            op: "combine",
                                            node: name.clone(),
                                            detail: "combine body typed program \
                                                         missing for on_miss: null_fields"
                                                .to_string(),
                                        }
                                    })?;
                                    match evaluator
                                        .eval_record::<NullStorage>(&eval_ctx, &resolver, None)
                                    {
                                        Ok(EvalResult::Emit {
                                            fields: emitted,
                                            metadata,
                                        }) => {
                                            let mut rec = match combine_output_schema.as_ref() {
                                                Some(s) => widen_record_to_schema(&probe_record, s),
                                                None => probe_record.clone(),
                                            };
                                            for (n, v) in emitted {
                                                rec.set(&n, v);
                                            }
                                            for (k, v) in metadata {
                                                let _ = rec.set_meta(&k, v);
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
                                        Err(e) => {
                                            return Err(PipelineError::from(e));
                                        }
                                    }
                                }
                            }
                        } else if let Some(evaluator) = body_evaluator.as_mut() {
                            for matched in &matched_records {
                                let resolver = CombineResolver::new(
                                    &resolver_mapping,
                                    &probe_record,
                                    Some(matched),
                                );
                                match evaluator
                                    .eval_record::<NullStorage>(&eval_ctx, &resolver, None)
                                {
                                    Ok(EvalResult::Emit {
                                        fields: emitted,
                                        metadata,
                                    }) => {
                                        let mut rec = match combine_output_schema.as_ref() {
                                            Some(s) => widen_record_to_schema(&probe_record, s),
                                            None => probe_record.clone(),
                                        };
                                        for (n, v) in emitted {
                                            rec.set(&n, v);
                                        }
                                        for (k, v) in metadata {
                                            let _ = rec.set_meta(&k, v);
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
                                    Err(e) => {
                                        return Err(PipelineError::from(e));
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
                            let target_schema =
                                combine_output_schema.as_ref().ok_or_else(|| {
                                    PipelineError::Internal {
                                        op: "combine",
                                        node: name.clone(),
                                        detail: "synthetic combine step has no output \
                                             schema; decomposition pass did not run"
                                            .to_string(),
                                    }
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
                    return Err(PipelineError::Compilation {
                        transform_name: name.clone(),
                        messages: vec![format!(
                            "E310 combine probe memory limit exceeded: \
                             hard limit {}",
                            budget.hard_limit()
                        )],
                    });
                }
                if emitted_since_check >= 10_000 {
                    emitted_since_check = 0;
                }
            }

            let probe_records_out = output_records.len() as u64;
            ctx.collector
                .record(probe_timer.finish(probe_records_in, probe_records_out));
            ctx.node_buffers.insert(node_idx, output_records);
        }
    }

    Ok(())
}

/// Collect parent-scope records keyed by composition input port name.
///
/// Reads `bound_body.input_port_sources` to find each port's upstream
/// node in the PARENT scope's `current_dag`, looks up that node's
/// NodeIndex, and clones records out of `ctx.node_buffers` under that
/// index. Cloning rather than removing leaves the parent producer's
/// buffer intact for any sibling consumer that the parent walk has
/// not yet reached. The record stream is small relative to the
/// pipeline's working set; the clone is the same shape every Output
/// arm uses for fan-out today.
fn collect_port_records(
    ctx: &ExecutorContext<'_>,
    parent_dag: &ExecutionPlanDag,
    bound_body: &crate::plan::composition_body::BoundBody,
    composition_node_idx: NodeIndex,
) -> Result<IndexMap<String, Vec<(clinker_record::Record, u64)>>, PipelineError> {
    let mut result: IndexMap<String, Vec<(clinker_record::Record, u64)>> = IndexMap::new();
    for (port_name, parent_node_name) in &bound_body.input_port_sources {
        // The parent node may itself carry a port qualifier (e.g.
        // `route_node.branch`); strip to the bare name to match
        // `name()` on each parent-graph node.
        let bare = parent_node_name
            .split('.')
            .next()
            .unwrap_or(parent_node_name);
        let parent_idx = parent_dag
            .graph
            .node_indices()
            .find(|&i| parent_dag.graph[i].name() == bare);
        let records = match parent_idx {
            Some(idx) => ctx.node_buffers.get(&idx).cloned().unwrap_or_default(),
            None => {
                // Nested-composition case: the parent scope is
                // itself a body, the upstream name refers to one of
                // that body's own input ports (not a body-internal
                // node), so the records were seeded into THIS
                // composition node's own buffer at the enclosing
                // composition-arm entry. Fall through to that
                // buffer before declaring an internal error.
                ctx.node_buffers
                    .get(&composition_node_idx)
                    .cloned()
                    .unwrap_or_default()
            }
        };
        result.insert(port_name.clone(), records);
    }
    Ok(result)
}

/// Execute one composition body's mini-DAG.
///
/// Builds a transient body-scope `ExecutionPlanDag` and walks it
/// through `dispatch_plan_node` — the same dispatcher entry the
/// top-level walker uses. The body's `node_buffers` namespace
/// is swapped in via `mem::replace` so body NodeIndices index a
/// fresh space; the parent buffers are restored after the walk.
/// The depth-counter guard increments via RAII so `?`-bubbled
/// errors can't leak the counter.
fn execute_composition_body(
    ctx: &mut ExecutorContext<'_>,
    body_id: crate::plan::composition_body::CompositionBodyId,
    port_records: IndexMap<String, Vec<(clinker_record::Record, u64)>>,
    composition_name: &str,
) -> Result<Vec<(clinker_record::Record, u64)>, PipelineError> {
    // Resolve body and pre-compute everything that needs the
    // bound_body borrow before the swap so the body_dag clone is
    // independent of the artifacts borrow.
    let bound_body = ctx
        .artifacts
        .body_of(body_id)
        .ok_or_else(|| PipelineError::compose_body_missing(composition_name.to_string()))?;

    let body_dag = crate::plan::execution::ExecutionPlanDag::from_body(bound_body);

    // Seed body-scope buffers from parent records keyed by port.
    let mut body_buffers: HashMap<NodeIndex, Vec<(clinker_record::Record, u64)>> = HashMap::new();
    for (port_name, records) in port_records {
        let body_idx = bound_body
            .port_name_to_node_idx
            .get(port_name.as_str())
            .ok_or_else(|| PipelineError::compose_unknown_port(composition_name, &port_name))?;
        body_buffers.insert(*body_idx, records);
    }

    // Pick the body's terminal output node. The bind-time alias
    // resolution wrote the port → NodeIndex map onto BoundBody;
    // the first declared output port wins. Zero-output-port bodies
    // are legal (sink-only / side-effect bodies) and produce no
    // record stream back to the parent.
    let output_idx = bound_body.output_port_to_node_idx.values().next().copied();

    // Swap node_buffers to a body-local namespace so body NodeIndices
    // don't collide with the parent's. `combine_source_records` is
    // also swapped to an empty map: the body's Source arms (if any
    // — body-scope sources are unusual but legal) fall back to
    // `all_records`, not parent-scope combine sources.
    let saved_buffers = std::mem::replace(&mut ctx.node_buffers, body_buffers);
    let saved_combine = std::mem::take(&mut ctx.combine_source_records);
    // Install the body's `input:` reference table so the Route arm
    // can resolve `<route>.<branch>` references against body
    // siblings. Restored on exit.
    let saved_body_refs = ctx
        .current_body_node_input_refs
        .replace(bound_body.node_input_refs.clone());

    // Increment depth before recursing. Every exit path below
    // decrements before returning so the counter stays in sync —
    // including the `walk_result?` early-return at the end. The
    // dispatcher loop already collects errors into `walk_result`
    // rather than `?`-bubbling, so the only `?`-bubble that escapes
    // this function is on `walk_result?` itself, which fires AFTER
    // the decrement.
    ctx.recursion_depth += 1;

    // Walk the body's topo through the same dispatcher the top-level
    // walker uses. Errors from within the body are wrapped with the
    // composition's name for diagnosability — the user sees
    // "in composition '<name>': <inner>" instead of an opaque
    // inner-only message.
    let topo: Vec<NodeIndex> = body_dag.topo_order.clone();
    let mut walk_result: Result<(), PipelineError> = Ok(());
    for node_idx in topo {
        if let Err(inner) = dispatch_plan_node(ctx, &body_dag, node_idx) {
            walk_result = Err(PipelineError::compose_body_error(
                composition_name.to_string(),
                Box::new(inner),
            ));
            break;
        }
    }

    // Harvest output before restoring parent buffers.
    let output_records = match (&walk_result, output_idx) {
        (Ok(()), Some(idx)) => ctx.node_buffers.remove(&idx).unwrap_or_default(),
        _ => Vec::new(),
    };

    // Decrement depth and restore parent scope. `saturating_sub`
    // is defensive over the invariant; the inc/dec pairs are kept in
    // sync by hand on every exit path through this function.
    ctx.recursion_depth = ctx.recursion_depth.saturating_sub(1);
    ctx.node_buffers = saved_buffers;
    ctx.combine_source_records = saved_combine;
    ctx.current_body_node_input_refs = saved_body_refs;

    walk_result?;
    Ok(output_records)
}
