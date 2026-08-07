//! `PlanNode::Composition` dispatch arm.
//!
//! Holds the composition call-site body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: it collects
//! parent-scope records per declared input port, swaps the body's mini-DAG
//! in, walks it through the shared dispatcher, and forwards the body's
//! output port back into the parent scope. The dispatcher's `Composition`
//! arm is a single delegating call into [`dispatch_composition`]; the
//! port-collection and body-walk helpers (`collect_port_records`,
//! `execute_composition_body`) move with it.

use std::collections::{HashMap, HashSet};

use clinker_record::Record;
use indexmap::IndexMap;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, NodeBufferKey, NodeBufferReaderLedger, admit_node_buffer,
    admit_node_buffer_transferred, dispatch_plan_node, drain_node_buffer_slot,
    finalize_node_rooted_windows, node_buffer_spill_allowed, planned_materialized_reader_counts,
    require_node_buffer_input, require_node_buffer_input_transferred,
    tee_emit_to_region_input_buffers, validate_completed_node_buffer_scope,
};
use crate::executor::node_buffer::{
    NodeBuffer, TransientNodeBufferReservation, reserve_node_buffer_materialization,
};
use crate::executor::schema_check::check_input_schema;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Context carrier kept lazy until the node-kind guard has succeeded. Normal
/// dispatch passes the live executor context directly; the feature-gated
/// mismatch matrix uses the inert carrier to prove rejection precedes body
/// lookup, buffer scoping, and downstream access.
pub(crate) enum CompositionDispatchContext<'borrow, 'plan> {
    Live(&'borrow mut ExecutorContext<'plan>),
    #[cfg(feature = "test-utils")]
    Inert,
}

impl<'borrow, 'plan> From<&'borrow mut ExecutorContext<'plan>>
    for CompositionDispatchContext<'borrow, 'plan>
{
    fn from(ctx: &'borrow mut ExecutorContext<'plan>) -> Self {
        Self::Live(ctx)
    }
}

#[cfg(feature = "test-utils")]
impl crate::executor::dispatch::DispatchFaultGuard {
    /// Execute the real composition boundary with an inert context so tests
    /// can prove a wrong node returns before body or buffer state is touched.
    #[doc(hidden)]
    pub fn dispatch_composition_mismatch_for_testing(
        current_dag: &ExecutionPlanDag,
        node_idx: NodeIndex,
        node: &PlanNode,
    ) -> Result<(), PipelineError> {
        dispatch_composition(
            CompositionDispatchContext::Inert,
            current_dag,
            node_idx,
            node,
        )
    }
}

/// Execute the `Composition` arm for `node_idx`: collect parent-scope
/// records per declared input port, swap `current_dag` to the body's
/// mini-DAG, walk the body topo through the shared dispatcher, then collect
/// the body's first declared output port and write it to this node's buffer
/// in the parent scope.
pub(crate) fn dispatch_composition<'borrow, 'plan>(
    ctx: impl Into<CompositionDispatchContext<'borrow, 'plan>>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError>
where
    'plan: 'borrow,
{
    let PlanNode::Composition { ref name, body, .. } = *node else {
        return Err(crate::executor::invariant::dispatch_mismatch(
            "dispatch_composition",
            "composition",
            node.kind_name(),
            node.name(),
        ));
    };
    let ctx = match ctx.into() {
        CompositionDispatchContext::Live(ctx) => ctx,
        #[cfg(feature = "test-utils")]
        CompositionDispatchContext::Inert => {
            panic!(
                "composition dispatcher accessed inert context after accepting a composition node"
            )
        }
    };
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
        clinker_plan::plan::composition_body::CompositionBodyId::SENTINEL,
        "composition {name:?}: body_id is sentinel — bind_composition did not run"
    );

    // Verify the body exists; the value itself is no longer
    // needed at this scope — schema checks read the parent
    // graph directly and `collect_port_records` walks live
    // edges, so the body's lookup is deferred to
    // `execute_composition_body`.
    ctx.composition_bodies
        .get(&body)
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
            && let Some(edge) = current_dag.graph.find_edge(first_pred, node_idx)
            && let Some(records) = ctx.node_buffers.get(&NodeBufferKey::with_port(
                first_pred,
                current_dag.graph[edge].producer_port.as_deref(),
            ))
        {
            for (record, _) in records.peek_mem_records() {
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
    if ctx.recursion_depth >= clinker_plan::plan::bind_schema::MAX_COMPOSITION_DEPTH {
        return Err(PipelineError::compose_depth_exceeded(
            name.clone(),
            ctx.recursion_depth,
        ));
    }

    let composition_name = name.clone();
    let port_records = collect_port_records(ctx, current_dag, node_idx, &composition_name)?;
    // Direct sync recursion into the body executor. Depth is
    // bounded by MAX_COMPOSITION_DEPTH (compile-time) plus the
    // E112 runtime cap, so the recursion cannot overflow the
    // stack.
    let output = execute_composition_body(ctx, body, port_records, &composition_name)?;
    // Materialize node-rooted window runtimes for any IndexSpec
    // rooted at this composition's call-site NodeIndex. The
    // body executor returned with `active_stack` already
    // popped, so the install lands on `top` (parent scope).
    finalize_node_rooted_windows(ctx, current_dag, node_idx, &output.records)?;
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output.records)?;
    // Attribution rule: the body harvest's bytes are charged
    // under the parent composition's name so a budget-exceeded
    // diagnostic points the user at the user-visible operator,
    // not at the body's internal output port node name.
    //
    // Boundary admit (post-body): if `admit_node_buffer` returns
    // `MemoryBudgetExceeded` here, it surfaces bare with `node =
    // composition_name` — not wrapped in `CompositionBodyError`.
    // The wrapper at `execute_composition_body`'s topo walk has
    // already returned with `Ok` by the time we reach this admit;
    // only errors from inside that walk get the wrapper. The pre-body
    // port-records clone is separately preflighted under the call-site name
    // before entering the body, so its E310 also remains bare.
    match output.reservation {
        Some(reservation) => admit_node_buffer_transferred(
            ctx,
            current_dag,
            &composition_name,
            node_idx,
            output.records,
            Vec::new(),
            reservation,
        )?,
        None => admit_node_buffer(
            ctx,
            current_dag,
            &composition_name,
            node_idx,
            output.records,
            Vec::new(),
            node_buffer_spill_allowed(current_dag, node_idx),
        )?,
    }

    Ok(())
}

/// Collect parent-scope records keyed by composition input port name.
///
/// Resolves ports via the live edge graph: walks `parent_dag`'s incoming
/// edges into `composition_node_idx`, reads each edge's `port` tag, and
/// acquires that exact compiled consumer's sequential scan of the producer
/// port. The frozen
/// port-name snapshot kept by an earlier design drifted whenever a
/// planner pass spliced a node between the producer and the composition
/// (the synthetic `inject_correlation_sort` Sort being the canonical
/// trigger), silently emptying the producer's buffer one hop downstream.
/// Edge-walking the live graph is the same source-of-truth pattern every
/// other arm of the dispatcher uses (`Transform`, `Aggregate`, `Output`,
/// `Sort` all read predecessors via `neighbors_directed`).
///
/// Cloning rather than removing keeps the parent producer's buffer
/// intact for any sibling consumer the parent walk has not yet reached;
/// fan-out from a single producer to multiple ports is a normal case.
struct ReservedPortRecords {
    records: Vec<(Record, crate::executor::stream_event::SourceRowId)>,
    reservation: Option<TransientNodeBufferReservation>,
}

/// Discard forward-pass slots owned by deferred body regions.
///
/// Their commit-time replay is seeded from retained aggregate/continuation
/// state, not from these narrow forward emits. Leaving them in the transient
/// body namespace would both retain memory until scope teardown and violate
/// the successful-scope ledger invariant.
fn discard_deferred_body_residue(ctx: &mut ExecutorContext<'_>, body_dag: &ExecutionPlanDag) {
    let mut deferred_nodes = HashSet::new();
    for region in body_dag.deferred_regions.values() {
        deferred_nodes.insert(region.producer);
        deferred_nodes.extend(region.members.iter().copied());
        deferred_nodes.extend(region.outputs.iter().copied());
    }
    let stale_keys: Vec<NodeBufferKey> = ctx
        .node_buffers
        .keys()
        .filter(|key| deferred_nodes.contains(&key.node))
        .cloned()
        .collect();
    for key in stale_keys {
        drain_node_buffer_slot(ctx, key);
    }
}
fn collect_port_records(
    ctx: &mut ExecutorContext<'_>,
    parent_dag: &ExecutionPlanDag,
    composition_node_idx: NodeIndex,
    composition_name: &str,
) -> Result<IndexMap<String, ReservedPortRecords>, PipelineError> {
    let mut result: IndexMap<String, ReservedPortRecords> = IndexMap::new();
    use petgraph::visit::EdgeRef;
    for edge in parent_dag
        .graph
        .edges_directed(composition_node_idx, Direction::Incoming)
    {
        let Some(port_name) = edge.weight().port.as_ref() else {
            // Composition incoming edges are always port-tagged at
            // bind time; an untagged edge is a planner-pass bug
            // (likely a rewrite that forgot to preserve the tag) and
            // surfaces here as an internal error rather than a silent
            // record drop.
            return Err(PipelineError::Internal {
                op: "composition",
                node: composition_name.to_string(),
                detail: format!(
                    "untagged incoming edge from node {:?}; every composition input edge must \
                     carry a port name (planner-pass invariant — see PlanEdge.port)",
                    parent_dag.graph[edge.source()].name(),
                ),
            });
        };
        let producer_port = edge.weight().producer_port.as_deref();
        let input = require_node_buffer_input_transferred(
            ctx,
            NodeBufferKey::with_port(edge.source(), producer_port),
            composition_name,
            parent_dag.graph[edge.source()].name(),
            producer_port,
        )?;
        // Composition port seeding takes records only; the body operates in
        // its own document-boundary scope and re-emits at the call site.
        let (input, reservation) = input.into_parts();
        let materialized_bytes = input.estimated_materialized_bytes();
        let transferred_overlap_bytes = input.transferred_materialization_overlap_bytes();
        let reservation = match reservation {
            Some(reservation) => {
                reservation.reserve_additional(transferred_overlap_bytes, composition_name)?;
                reservation
            }
            None => reserve_node_buffer_materialization(
                materialized_bytes,
                &ctx.memory_budget,
                composition_name,
            )?,
        };
        let (records, _puncts) = input.drain_split()?;
        reservation.set_bytes(crate::executor::dispatch::estimate_node_buffer_bytes(
            &records,
        ));
        // Two parallel edges to the same port (e.g. `inputs: { p: a,
        // p: a }` — currently rejected at parse, but the runtime is
        // defensive) would overwrite; the wiring pass guarantees
        // unique port names per consumer.
        result.insert(
            port_name.clone(),
            ReservedPortRecords {
                records,
                reservation: Some(reservation),
            },
        );
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
/// Dispatch and output harvest are captured into one result so the single
/// restoration path runs before success or any error is propagated.
fn execute_composition_body(
    ctx: &mut ExecutorContext<'_>,
    body_id: clinker_plan::plan::composition_body::CompositionBodyId,
    port_records: IndexMap<String, ReservedPortRecords>,
    composition_name: &str,
) -> Result<ReservedPortRecords, PipelineError> {
    // Resolve body and pre-compute everything that needs the
    // bound_body borrow before the swap so the body_dag clone is
    // independent of the composition_bodies borrow.
    let bound_body = ctx
        .composition_bodies
        .get(&body_id)
        .ok_or_else(|| PipelineError::compose_body_missing(composition_name.to_string()))?;

    let body_dag = clinker_plan::plan::execution::ExecutionPlanDag::from_body(bound_body);

    // Resolve every body-local slot before transferring any reservation out of
    // its RAII guard. If port resolution fails, all still-owned guards drop and
    // unregister normally; after this pass the transfer loop is infallible.
    let mut resolved_inputs = Vec::with_capacity(port_records.len());
    for (port_name, input) in port_records {
        let body_idx = bound_body
            .port_name_to_node_idx
            .get(port_name.as_str())
            .ok_or_else(|| PipelineError::compose_unknown_port(composition_name, &port_name))?;
        resolved_inputs.push((NodeBufferKey::from(*body_idx), input));
    }

    // Seed body-scope buffers and transfer each materialization reservation's existing
    // consumer id/handle into the body-local registry. The wrapper stays
    // continuously registered: there is no unregister/register gap and no
    // second charge for the same allocation.
    let mut body_buffers: HashMap<NodeBufferKey, NodeBuffer> = HashMap::new();
    let mut body_consumer_ids = HashMap::new();
    let mut body_readers = NodeBufferReaderLedger::default();
    let seed_result = (|| {
        for (slot_key, input) in resolved_inputs {
            let Some(reservation) = input.reservation else {
                return Err(PipelineError::Internal {
                    op: "executor",
                    node: composition_name.to_string(),
                    detail: format!(
                        "composition body seed {slot_key:?} had no transferred memory registration"
                    ),
                });
            };
            if body_buffers.contains_key(&slot_key) || body_consumer_ids.contains_key(&slot_key) {
                return Err(PipelineError::Internal {
                    op: "executor",
                    node: composition_name.to_string(),
                    detail: format!(
                        "composition body seed {slot_key:?} was published more than once"
                    ),
                });
            }
            body_readers.publish(slot_key.clone(), 1, composition_name)?;
            body_buffers.insert(
                slot_key.clone(),
                NodeBuffer::memory_from_records(input.records),
            );
            body_consumer_ids.insert(slot_key, reservation.into_registration());
        }
        Ok::<(), PipelineError>(())
    })();
    if let Err(error) = seed_result {
        drop(body_buffers);
        for (_, (id, handle)) in body_consumer_ids {
            handle.set_bytes(0);
            ctx.memory_budget.unregister_consumer(id);
        }
        return Err(error);
    }

    // Pick the body's terminal output node. The bind-time alias
    // resolution wrote the port → NodeIndex map onto BoundBody;
    // the first declared output port wins. Zero-output-port bodies
    // are legal (sink-only / side-effect bodies) and produce no
    // record stream back to the parent.
    let output_idx = bound_body.output_port_to_node_idx.values().next().copied();

    // Swap node_buffers to a body-local namespace so body NodeIndices
    // don't collide with the parent's. `source_records` is also
    // swapped to an empty map so body-scope Source nodes resolve
    // through `node_buffers` (port seeding from parent scope), not
    // through parent-scope source ingestion — bodies declare ports,
    // not top-level sources. Any non-port-seeded body Source surfaces
    // as the defense-in-depth `Internal` error from the Source arm.
    let saved_buffers = std::mem::replace(&mut ctx.node_buffers, body_buffers);
    let saved_consumer_ids =
        std::mem::replace(&mut ctx.node_buffer_consumer_ids, body_consumer_ids);
    // Remaining-reader counts key by the body-local `NodeBufferKey` space, so
    // swap to a fresh ledger alongside `node_buffers`.
    let saved_readers = std::mem::replace(&mut ctx.node_buffer_readers, body_readers);
    let saved_planned_readers = std::mem::replace(
        &mut ctx.planned_node_buffer_readers,
        planned_materialized_reader_counts(&body_dag),
    );
    let saved_combine = std::mem::take(&mut ctx.source_records);
    // Window-arena consumer ids key by slot index, which the body
    // re-uses from zero alongside its window-runtime overlay. Swap to a
    // fresh map so a body arena registration at slot N does not clobber
    // the parent's slot N; body-local arenas drop when the body's
    // window-runtime overlay is popped on exit, so their wrappers are
    // unregistered there and the parent map is restored.
    let saved_arena_ids = std::mem::take(&mut ctx.window_arena_consumer_ids);
    // Install the body's `input:` reference table so the Route arm
    // can resolve `<route>.<branch>` references against body
    // siblings. Restored on exit.
    let saved_body_refs = ctx
        .current_body_node_input_refs
        .replace(bound_body.node_input_refs.clone());

    ctx.window_runtime.active_stack.push(body_id);

    // Increment depth before recursing. The walk and output harvest execute
    // inside one captured Result; parent-scope restoration below runs before
    // that Result is propagated on every success/error path.
    ctx.recursion_depth += 1;

    // Walk the body's topo through the same dispatcher the top-level
    // walker uses. Errors from within the body are wrapped with the
    // composition's name for diagnosability — the user sees
    // "in composition '<name>': <inner>" instead of an opaque
    // inner-only message.
    //
    // Body-interior E310 errors name a body-internal operator (for example
    // `stage_split`), so the wrapper supplies the user-visible call-site name.
    // The pre-body materialization gate and the parent's post-body output
    // admission already name the call-site and remain bare.
    let topo: Vec<NodeIndex> = body_dag.topo_order.clone();
    let walk_and_harvest: Result<ReservedPortRecords, PipelineError> = (|| {
        for node_idx in topo {
            if let Err(inner) = dispatch_plan_node(ctx, &body_dag, node_idx) {
                return Err(PipelineError::compose_body_error(
                    composition_name.to_string(),
                    Box::new(inner),
                ));
            }
        }

        // Harvest output before restoring parent buffers. When the output port
        // participates in a deferred region, the commit pass is the sole
        // source of parent records, so the optional forward slot is cleanup
        // only.
        let output = match output_idx {
            Some(idx) => {
                // Composition body output harvest — punctuations on the
                // body's output port belong to the composition's parent
                // pipeline scope; they re-emit when the parent's call
                // site re-introduces document context. The body harvest
                // takes records only.
                if body_dag.deferred_region_at(idx).is_some() {
                    // A deferred body terminal is harvested by the commit pass. Its
                    // forward slot is optional and is drained only as cleanup.
                    drain_node_buffer_slot(ctx, idx);
                    ReservedPortRecords {
                        records: Vec::new(),
                        reservation: None,
                    }
                } else {
                    let input = require_node_buffer_input(
                        ctx,
                        idx,
                        composition_name,
                        body_dag.graph[idx].name(),
                        None,
                    )?;
                    let (input, reservation) =
                        input.into_materialized_parts(&ctx.memory_budget, composition_name)?;
                    let (records, _puncts) = input.drain_split()?;
                    ReservedPortRecords {
                        records,
                        reservation,
                    }
                }
            }
            None => ReservedPortRecords {
                records: Vec::new(),
                reservation: None,
            },
        };
        discard_deferred_body_residue(ctx, &body_dag);
        validate_completed_node_buffer_scope(ctx, composition_name)?;
        Ok(output)
    })();

    // One restoration path for body dispatch, output harvest, and success.
    // Drop body-local buffers while their wrappers remain registered, then
    // unregister every residual body registration before restoring the parent
    // maps. Slots already drained by body operators removed their own entries,
    // so this sweep covers only early-return residue.
    ctx.recursion_depth = ctx.recursion_depth.saturating_sub(1);
    drop(std::mem::take(&mut ctx.node_buffers));
    for (_, (id, handle)) in std::mem::take(&mut ctx.node_buffer_consumer_ids) {
        handle.set_bytes(0);
        ctx.memory_budget.unregister_consumer(id);
    }
    ctx.node_buffers = saved_buffers;
    ctx.node_buffer_consumer_ids = saved_consumer_ids;
    ctx.node_buffer_readers = saved_readers;
    ctx.planned_node_buffer_readers = saved_planned_readers;
    ctx.source_records = saved_combine;
    ctx.current_body_node_input_refs = saved_body_refs;
    // Unregister body-local window-arena consumers and restore the
    // parent map. The body's node-rooted arenas drop when its window-
    // runtime overlay is popped below, so their wrappers must leave the
    // arbitrator's registry in lockstep.
    for (_, (id, _)) in std::mem::take(&mut ctx.window_arena_consumer_ids) {
        ctx.memory_budget.unregister_consumer(id);
    }
    ctx.window_arena_consumer_ids = saved_arena_ids;
    ctx.window_runtime.active_stack.pop();
    ctx.window_runtime.remove_body_scope(bound_body.body_scope);

    walk_and_harvest
}
