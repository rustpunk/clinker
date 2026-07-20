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

use std::collections::HashMap;

use clinker_record::Record;
use indexmap::IndexMap;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, NodeBufferKey, admit_node_buffer, dispatch_plan_node, drain_node_buffer_slot,
    finalize_node_rooted_windows, node_buffer_spill_allowed, tee_emit_to_region_input_buffers,
};
use crate::executor::node_buffer::NodeBuffer;
use crate::executor::schema_check::check_input_schema;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Execute the `Composition` arm for `node_idx`: collect parent-scope
/// records per declared input port, swap `current_dag` to the body's
/// mini-DAG, walk the body topo through the shared dispatcher, then collect
/// the body's first declared output port and write it to this node's buffer
/// in the parent scope.
pub(crate) fn dispatch_composition(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Composition { ref name, body, .. } = *node else {
        unreachable!("dispatch_composition called with non-Composition node");
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
            && let Some(records) = ctx.node_buffers.get(&first_pred.into())
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
    let output_records = execute_composition_body(ctx, body, port_records, &composition_name)?;
    // Materialize node-rooted window runtimes for any IndexSpec
    // rooted at this composition's call-site NodeIndex. The
    // body executor returned with `active_stack` already
    // popped, so the install lands on `top` (parent scope).
    finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
    // Attribution rule: the body harvest's bytes are charged
    // under the parent composition's name so a budget-exceeded
    // diagnostic points the user at the user-visible operator,
    // not at the body's internal output port node name.
    //
    // Boundary admit (post-body): same two-path model as the
    // pre-body port-records clone in `collect_port_records`.
    // If `admit_node_buffer` returns `MemoryBudgetExceeded`
    // here, it surfaces bare with `node = composition_name` —
    // not wrapped in `CompositionBodyError`. The wrapper at
    // `execute_composition_body`'s topo walk has already
    // returned with `Ok` by the time we reach this admit;
    // only errors from inside that walk get the wrapper.
    let nb = admit_node_buffer(
        ctx,
        &composition_name,
        node_idx,
        output_records,
        Vec::new(),
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx.into(), nb);

    Ok(())
}

/// Collect parent-scope records keyed by composition input port name.
///
/// Resolves ports via the live edge graph: walks `parent_dag`'s incoming
/// edges into `composition_node_idx`, reads each edge's `port` tag, and
/// clones records from the producer's `node_buffers` slot. The frozen
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
fn collect_port_records(
    ctx: &mut ExecutorContext<'_>,
    parent_dag: &ExecutionPlanDag,
    composition_node_idx: NodeIndex,
    composition_name: &str,
) -> Result<IndexMap<String, Vec<(clinker_record::Record, u64)>>, PipelineError> {
    let mut result: IndexMap<String, Vec<(clinker_record::Record, u64)>> = IndexMap::new();
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
        // Compute the heap-clone footprint while the immutable borrow
        // of `node_buffers` is live, then release it so the charge
        // path can take `&mut ctx.memory_budget`. The cloned records
        // are about to be seeded into the body-local `node_buffers`
        // namespace inside `execute_composition_body`; the body's
        // port-source Source arm discharges the slot when it claims
        // the seeded buffer, and the body-exit leak-discharge cleans
        // up any unconsumed remainder if a body operator errored.
        //
        // Boundary admit: the budget exceedance surfaces as a bare
        // `MemoryBudgetExceeded` with `node = composition_name`
        // (the user-visible call-site identifier). The body has not
        // executed yet, so there is no body-internal operator name
        // to disambiguate via `CompositionBodyError` — only inner
        // errors that bubble out of `execute_composition_body`'s
        // topo walk get that wrapper. Detail string discriminates
        // this site from the generic admit in `admit_node_buffer`.
        let cloned_with_bytes = ctx.node_buffers.get(&edge.source().into()).map(|nb| {
            // Composition port seeding clones records only; the
            // composition body operates inside its own document-
            // boundary scope and re-emits punctuations at the call-
            // site level on body exit, so the parent's puncts do not
            // forward through the body's port-source boundary.
            let cloned_records: Vec<(Record, u64)> = nb
                .clone_memory_only()
                .into_iter()
                .filter_map(|e| e.into_record())
                .collect();
            (cloned_records, nb.estimated_memory_bytes())
        });
        let records: Vec<(Record, u64)> = match cloned_with_bytes {
            Some((cloned, _bytes)) => cloned,
            None => Vec::new(),
        };
        // Two parallel edges to the same port (e.g. `inputs: { p: a,
        // p: a }` — currently rejected at parse, but the runtime is
        // defensive) would overwrite; the wiring pass guarantees
        // unique port names per consumer.
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
    body_id: clinker_plan::plan::composition_body::CompositionBodyId,
    port_records: IndexMap<String, Vec<(clinker_record::Record, u64)>>,
    composition_name: &str,
) -> Result<Vec<(clinker_record::Record, u64)>, PipelineError> {
    use clinker_plan::plan::index::PlanIndexRoot;

    // Resolve body and pre-compute everything that needs the
    // bound_body borrow before the swap so the body_dag clone is
    // independent of the composition_bodies borrow.
    let bound_body = ctx
        .composition_bodies
        .get(&body_id)
        .ok_or_else(|| PipelineError::compose_body_missing(composition_name.to_string()))?;

    let body_dag = clinker_plan::plan::execution::ExecutionPlanDag::from_body(bound_body);

    // Window runtime entry: install a fresh per-body vec sized to
    // `body_indices_to_build.len()`. ParentNode-rooted slots inherit
    // the parent's runtime via `Arc::clone` from `top` so the body's
    // windowed Transform arm can resolve through to it without re-
    // materializing the parent's arena. Node-rooted body slots stay
    // `None` here — they populate when the body's upstream operator
    // arm calls `finalize_node_rooted_windows` during the body walk.
    let body_index_count = bound_body.body_indices_to_build.len();
    let mut body_window_vec: Vec<Option<crate::executor::window_runtime::WindowRuntime>> =
        (0..body_index_count).map(|_| None).collect();
    for (idx, spec) in bound_body.body_indices_to_build.iter().enumerate() {
        if matches!(spec.root, PlanIndexRoot::ParentNode { .. })
            && let Some(parent_runtime) = ctx.window_runtime.resolve(&spec.root, idx)
        {
            // `resolve` for ParentNode in this position falls through
            // to top[idx] (no body is on the active_stack yet), which
            // is correct: the body inherits the parent operator's
            // runtime by cloning its `Arc<Arena>` and `Arc<SecondaryIndex>`.
            body_window_vec[idx] = Some(parent_runtime.clone());
        }
    }

    // Seed body-scope buffers from parent records keyed by port.
    let mut body_buffers: HashMap<NodeBufferKey, NodeBuffer> = HashMap::new();
    for (port_name, records) in port_records {
        let body_idx = bound_body
            .port_name_to_node_idx
            .get(port_name.as_str())
            .ok_or_else(|| PipelineError::compose_unknown_port(composition_name, &port_name))?;
        body_buffers.insert((*body_idx).into(), NodeBuffer::memory_from_records(records));
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

    // Push the body's window-runtime overlay onto the registry. The
    // ParentNode-rooted slots were populated above via `Arc::clone`
    // from the parent's `top` runtime; Node-rooted body slots stay
    // `None` and populate when the body's upstream operator arm
    // calls `finalize_node_rooted_windows` during the body walk.
    // `active_stack.push` makes `resolve` route through this overlay
    // for any window dispatched inside the body.
    ctx.window_runtime.bodies.insert(body_id, body_window_vec);
    ctx.window_runtime.active_stack.push(body_id);

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
    //
    // Body-interior path of the two-path admission model: the inner
    // error's `node` field names a body-internal operator (e.g.
    // `stage_split`) the user never wrote in their YAML, so the
    // wrapper supplies the user-visible call-site name on top. The
    // boundary admits in `collect_port_records` (pre-body input
    // clone) and the parent's Composition arm (post-body output
    // harvest) stay unwrapped because their `node` is already the
    // call-site composition name — wrapping them would duplicate
    // the same identifier in both layers with no information gain.
    // Consumers that want to catch every composition-involved
    // budget exceedance must match both shapes.
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

    // Harvest output before restoring parent buffers. When the output
    // port aliases a deferred-region producer (a relaxed-CK Aggregate
    // sitting at the body's terminal port), the producer's forward
    // emit is NOT a final stream — the commit-pass body→parent
    // harvest path re-emits the post-recompute narrow rows into the
    // parent's `node_buffers[composition_idx]`. Returning the forward
    // emit here would double-feed the parent's continuation: once
    // through the parent Composition arm's `node_buffers.insert`, then
    // again when `recurse_into_body` extends the slot with the
    // commit-pass harvest. Drop the forward emit so the commit pass
    // is the single source of records for that slot.
    let output_records: Vec<(Record, u64)> = match (&walk_result, output_idx) {
        (Ok(()), Some(idx)) => {
            // Composition body output harvest — punctuations on the
            // body's output port belong to the composition's parent
            // pipeline scope; they re-emit when the parent's call
            // site re-introduces document context. The body harvest
            // takes records only.
            let drained: Vec<(Record, u64)> = match drain_node_buffer_slot(ctx, idx) {
                Some(nb) => {
                    let (records, _puncts) = nb.drain_split()?;
                    records
                }
                None => Vec::new(),
            };
            if body_dag.deferred_region_at_producer(idx).is_some() {
                Vec::new()
            } else {
                drained
            }
        }
        _ => Vec::new(),
    };

    // Decrement depth and restore parent scope. `saturating_sub`
    // is defensive over the invariant; the inc/dec pairs are kept in
    // sync by hand on every exit path through this function.
    ctx.recursion_depth = ctx.recursion_depth.saturating_sub(1);
    ctx.node_buffers = saved_buffers;
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
    // Pop the window-runtime overlay so subsequent windows in the
    // parent scope route through `top` again. Removing the body's
    // entry releases the `Arc` clones (parent runtimes stay alive in
    // `top`; body-local node-rooted runtimes drop here).
    ctx.window_runtime.active_stack.pop();
    ctx.window_runtime.bodies.remove(&body_id);

    walk_result?;
    Ok(output_records)
}
