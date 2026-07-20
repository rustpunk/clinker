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

use crate::executor::cull_dispatch::reads_predecessor_slot;
use crate::executor::dispatch::{
    ExecutorContext, NodeBufferKey, admit_node_buffer, advance_cursor, drain_node_buffer_slot,
    node_buffer_spill_allowed, push_dlq, record_error_to_buffer_if_grouped, source_file_arc_of,
    source_name_arc_of, stream_linear_producer_emit,
};
use crate::executor::schema_check::check_input_schema;
use crate::executor::{CompiledRoute, DlqEntry};
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
        ref branches,
        ref default,
        ref branch_programs,
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

    // Map each branch output port to the successor(s) that draw from it.
    //
    // The producer-port tag on each outgoing edge is the single source
    // of truth: the planner stamps `PlanEdge.producer_port` with the
    // branch (or `default`) name the consumer referenced as
    // `<route>.<branch>`, so resolution is a direct walk of the live
    // edge graph — no re-parsing of `config.nodes` input refs and no
    // dependence on the route-name spelling. A branch may fan to more
    // than one consumer, so each port maps to a `Vec` of successors.
    use petgraph::visit::EdgeRef;
    let mut branch_to_succ: HashMap<&str, Vec<NodeIndex>> = HashMap::new();
    for edge in current_dag
        .graph
        .edges_directed(node_idx, Direction::Outgoing)
    {
        if let Some(port) = edge.weight().producer_port.as_deref() {
            branch_to_succ.entry(port).or_default().push(edge.target());
        }
    }

    // Records matched to each consumer-bearing branch (producer output port),
    // keyed by branch name. Keeping records per port — rather than unioned per
    // successor — lets a predecessor-slot reader (Merge / Combine) that draws
    // several branches of this Route drain each port's slot separately.
    let mut branch_records: HashMap<String, Vec<(Record, u64)>> = HashMap::new();
    for branch in branch_to_succ.keys() {
        branch_records.insert((*branch).to_string(), Vec::new());
    }

    // Build the per-record evaluator bundle off the node's own typed branch
    // programs. Each Route node carries its own conditions, so a body Route and
    // a same-named top-level Route resolve independently — there is no shared
    // by-name lookup to collide in. Owning `route` locally for the record loop
    // keeps its `&mut`-mutated state (regex caches, evaluator counters)
    // consistent across records without a take/restore against shared context.
    let mut route = CompiledRoute::from_node(name, branches, branch_programs, default, mode)?;
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
                    if let Some(buf) = branch_records.get_mut(target.as_str()) {
                        buf.push((record.clone(), rn));
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
                let triggering_field = route_err.triggering_field.clone();
                let triggering_value = route_err.triggering_value();
                // Under `dlq_granularity: document`, a route-eval failure
                // condemns the whole document — mark it failed (capturing
                // this record as the root cause) so the Output emits the
                // trigger + collateral entries at the document's close.
                let marked = !routed
                    && crate::executor::document_dlq::record_error_to_document_buffer_if_doc_dlq(
                        ctx,
                        &record,
                        rn,
                        clinker_core_types::dlq::DlqErrorCategory::TypeCoercionFailure,
                        route_err.to_string(),
                        stage.clone(),
                        None,
                        triggering_field.clone(),
                        triggering_value.clone(),
                    );
                if !routed && !marked {
                    let source_name = source_name_arc_of(&record);
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
        let merged: Vec<(Record, u64)> = branch_records.into_values().flatten().collect();
        stream_linear_producer_emit(&sender, batch_size, name, merged, input_puncts, &charge)?;
        return Ok(());
    }

    // Deliver each branch's records to its consumers.
    //
    // An own-slot reader (Transform / Output / Reshape / …) checks its own
    // slot first, so its branch's records go into the successor's own slot
    // `(succ, None)`. A predecessor-slot reader (Merge / Combine) drains by
    // incoming edge, so each branch it draws lands in this Route's own slot
    // keyed `(node_idx, Some(branch))` — a Merge or Combine drawing several
    // branches of this Route keeps them separate rather than unioned.
    //
    // For successors that fall inside a deferred region while this Route does
    // not, also park the branch's records on the matching outgoing edge so the
    // commit-time deferred dispatcher receives the same records the forward
    // assignment selected. Internal-region edges and edges between two
    // non-deferred operators skip the tee — the `node_buffers` entry already
    // covers them.
    let route_region_producer = current_dag.deferred_region_at(node_idx).map(|r| r.producer);
    let active_body = ctx.window_runtime.active_stack.last().copied();
    // Collect outgoing (branch, successor, edge) triples before the mutable
    // admissions below so the immutable graph borrow does not overlap them.
    let outgoing: Vec<(String, NodeIndex, petgraph::graph::EdgeIndex)> = current_dag
        .graph
        .edges_directed(node_idx, Direction::Outgoing)
        .filter_map(|edge| {
            edge.weight()
                .producer_port
                .as_deref()
                .map(|branch| (branch.to_string(), edge.target(), edge.id()))
        })
        .collect();

    // Distinct branches whose records feed a predecessor-slot reader, admitted
    // last into this Route's own per-branch slots.
    let mut pred_branches: Vec<String> = Vec::new();
    for (branch, succ_idx, edge_id) in outgoing {
        let records = branch_records.get(&branch).cloned().unwrap_or_default();
        let succ_region_producer = current_dag.deferred_region_at(succ_idx).map(|r| r.producer);
        let crosses = match (route_region_producer, succ_region_producer) {
            (None, Some(_)) => true,
            (Some(p), Some(t)) if p != t => true,
            _ => false,
        };
        if crosses {
            let row_bytes_each: u64 = records
                .first()
                .map(|(rec, _)| {
                    (std::mem::size_of::<Value>() * rec.schema().column_count()
                        + std::mem::size_of::<(Record, u64)>()) as u64
                })
                .unwrap_or(0);
            for (record, rn) in &records {
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
                    .entry((active_body, edge_id))
                    .or_default()
                    .push((record.clone(), *rn));
            }
        }
        // Route broadcasts punctuations to every branch — each downstream
        // subgraph needs the same document-boundary signal to drive its own
        // per-document accumulators and writers.
        if reads_predecessor_slot(&current_dag.graph[succ_idx]) {
            if !pred_branches.contains(&branch) {
                pred_branches.push(branch);
            }
        } else {
            let nb = admit_node_buffer(
                ctx,
                name,
                succ_idx,
                records,
                input_puncts.clone(),
                node_buffer_spill_allowed(current_dag, succ_idx),
            )?;
            ctx.node_buffers.insert(succ_idx.into(), nb);
        }
    }
    let spill_allowed = node_buffer_spill_allowed(current_dag, node_idx);
    for branch in pred_branches {
        let records = branch_records.get(&branch).cloned().unwrap_or_default();
        let key = NodeBufferKey::with_port(node_idx, Some(branch.as_str()));
        let nb = admit_node_buffer(
            ctx,
            name,
            key.clone(),
            records,
            input_puncts.clone(),
            spill_allowed,
        )?;
        ctx.node_buffers.insert(key, nb);
    }

    Ok(())
}
