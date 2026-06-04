//! `PlanNode::Route` dispatch arm.
//!
//! Holds the predicate-based fan-out body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: per-branch predicate
//! evaluation, port emission to each matched successor, and default-branch
//! handling. The dispatcher's `Route` arm is a single delegating call into
//! [`dispatch_route`].

use std::collections::HashMap;

use clinker_record::{Record, Value};
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::DlqEntry;
use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, advance_cursor, drain_node_buffer_slot,
    node_buffer_spill_allowed, push_dlq, record_error_to_buffer_if_grouped, source_file_arc_of,
    source_name_arc_of, stream_linear_producer_emit,
};
use crate::executor::schema_check::check_input_schema;
use clinker_plan::BudgetCategory;
use clinker_plan::config::ErrorStrategy;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Execute the `Route` arm for `node_idx`: evaluate each branch predicate
/// against every input record and emit matching records onto the
/// corresponding successor port, falling back to the default branch.
/// Stateless and streaming — records route one at a time without per-record
/// state accumulation.
pub(crate) fn dispatch_route(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Route {
        ref name,
        mode,
        branches: _,
        default: _,
        ..
    } = *node
    else {
        unreachable!("dispatch_route called with non-Route node");
    };
    // Body-context Routes that consume an input port have no
    // predecessor in the body's mini-DAG — the records are
    // seeded into this node's own buffer at composition entry.
    // Check own buffer first, fall back to predecessor.
    let predecessors: Vec<NodeIndex> = current_dag
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    let (input_records, input_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = if let Some(own_buf) = drain_node_buffer_slot(ctx, node_idx) {
        own_buf.drain_split()?
    } else {
        match predecessors
            .iter()
            .find_map(|p| drain_node_buffer_slot(ctx, *p))
        {
            Some(nb) => nb.drain_split()?,
            None => (Vec::new(), Vec::new()),
        }
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
        use clinker_plan::config::PipelineNode;
        use clinker_plan::config::node_header::NodeInput;
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
        for (i, (record, rn)) in input_records.into_iter().enumerate() {
            // Poll the shutdown flag every 1024 records so a long
            // Route chain terminates promptly on SIGINT. Same
            // chunk-boundary cadence the Transform arm uses.
            if i > 0 && i.is_multiple_of(1024) {
                ctx.check_shutdown()?;
            }
            let source_file_arc = source_file_arc_of(&record);
            let source_name_arc = source_name_arc_of(&record);
            let eval_ctx =
                ctx.eval_ctx_for_record(&source_file_arc, &source_name_arc, rn, record.doc_ctx());

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
                        if mode == clinker_plan::config::RouteMode::Exclusive {
                            break;
                        }
                    }
                    advance_cursor(ctx, &source_name_arc, rn);
                }
                Err(route_err) => {
                    if ctx.strategy == ErrorStrategy::FailFast {
                        return Err(route_err.into());
                    }
                    let stage = Some(DlqEntry::stage_route_eval());
                    let routed = record_error_to_buffer_if_grouped(
                        ctx,
                        &record,
                        rn,
                        clinker_core_types::dlq::DlqErrorCategory::TypeCoercionFailure,
                        route_err.to_string(),
                        stage.clone(),
                        None,
                    );
                    if !routed {
                        let source_name = source_name_arc_of(&record);
                        let triggering_field = route_err.triggering_field.clone();
                        let triggering_value = route_err.triggering_value();
                        push_dlq(
                            ctx,
                            DlqEntry {
                                source_row: rn,
                                category:
                                    clinker_core_types::dlq::DlqErrorCategory::TypeCoercionFailure,
                                error_message: route_err.to_string(),
                                original_record: record,
                                stage,
                                route: None,
                                trigger: true,
                                source_name,
                                triggering_field,
                                triggering_value,
                            },
                        )?;
                    }
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

    // Streaming-Output handoff: a single-branch Route whose sole
    // successor is a streaming-eligible Output installed its
    // sender under our `node_idx` at executor entry. The one
    // branch's records are already collected into `merged`, so
    // this does not shrink the Route's own working set; what it
    // saves is the second copy — the records stream straight to
    // the writer thread rather than crossing a charged
    // `node_buffers` slot the Output would re-drain, and the
    // writer overlaps with the next topo node. The eligibility
    // predicate certified the single outgoing edge, so
    // `branch_buffers` holds exactly the one successor here, and a
    // streaming Route → terminal Output crosses no deferred
    // region, so the cross-region tee below is correctly skipped.
    // Dropping the sender disconnects the writer's recv.
    if let Some(sender) = ctx.take_streaming_sender(node_idx) {
        let batch_size = ctx.batch_size;
        let spill_allowed = node_buffer_spill_allowed(current_dag, node_idx);
        let charge = ctx
            .streaming_charge_handle(node_idx, name, spill_allowed)
            .expect("streaming sender implies a registered charge consumer");
        let merged: Vec<(Record, u64)> = branch_buffers.into_values().flatten().collect();
        stream_linear_producer_emit(&sender, batch_size, name, merged, input_puncts, &charge)?;
        return Ok(());
    }

    // Put branch buffers into node_buffers keyed by successor.
    // For successors that fall inside a deferred region while
    // this Route does not, also park the per-branch records on
    // the matching outgoing edge so the commit-time deferred
    // dispatcher receives the same records the forward branch
    // assignment selected. Internal-region edges and edges
    // between two non-deferred operators skip the tee — the
    // node_buffers entry already covers them.
    let route_region_producer = current_dag.deferred_region_at(node_idx).map(|r| r.producer);
    let active_body = ctx.window_runtime.active_stack.last().copied();
    for (succ_idx, buf) in branch_buffers {
        let succ_region_producer = current_dag.deferred_region_at(succ_idx).map(|r| r.producer);
        let crosses = match (route_region_producer, succ_region_producer) {
            (None, Some(_)) => true,
            (Some(p), Some(t)) if p != t => true,
            _ => false,
        };
        if crosses && let Some(edge) = current_dag.graph.find_edge(node_idx, succ_idx) {
            let row_bytes_each: u64 = buf
                .first()
                .map(|(rec, _)| {
                    (std::mem::size_of::<Value>() * rec.schema().column_count()
                        + std::mem::size_of::<(Record, u64)>()) as u64
                })
                .unwrap_or(0);
            for (record, rn) in &buf {
                if row_bytes_each > 0 && ctx.memory_budget.should_abort() {
                    return Err(PipelineError::MemoryBudgetExceeded {
                        node: name.to_string(),
                        used: ctx.memory_budget.peak_rss().unwrap_or(0),
                        limit: ctx.memory_budget.hard_limit(),
                        source: BudgetCategory::Arena,
                        detail: Some("Route cross-region tee admission".to_string()),
                    });
                }
                ctx.region_input_buffers
                    .entry((active_body, edge))
                    .or_default()
                    .push((record.clone(), *rn));
            }
        }
        // Route broadcasts punctuations to every branch — each
        // downstream subgraph needs the same document-boundary
        // signal to drive its own per-document accumulators
        // and writers.
        let nb = admit_node_buffer(
            ctx,
            name,
            succ_idx,
            buf,
            input_puncts.clone(),
            node_buffer_spill_allowed(current_dag, succ_idx),
        )?;
        ctx.node_buffers.insert(succ_idx, nb);
    }

    Ok(())
}
