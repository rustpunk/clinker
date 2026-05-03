//! Commit-time deferred-region dispatcher.
//!
//! After `recompute_aggregates` has run `retract_row` + `finalize_in_place`
//! on every relaxed-CK aggregate and parked the post-recompute narrow
//! emits in `node_buffers[producer]` (projected to the region's
//! `buffer_schema`), this module walks each [`DeferredRegion`] in
//! topological order and drives the same operator arms in
//! [`crate::executor::dispatch::dispatch_plan_node`] that the forward
//! pass uses — only the deferred-region guard at the top of
//! `dispatch_plan_node` is inverted via
//! [`ExecutorContext::in_deferred_dispatch`].
//!
//! One evaluator everywhere: the per-arm CXL evaluation work
//! (Transform projection, Route predicates, Combine probe, Window /
//! Aggregate inner reductions, Output writer admission) lives in
//! `dispatch_plan_node`'s arms and runs identically on the forward and
//! commit passes. The forward pass parks narrow producer emits into
//! `node_buffers[producer]`, tees cross-region inputs into
//! `region_input_buffers`, and short-circuits members. The commit pass
//! seeds each region's producer slot with post-recompute narrow rows,
//! re-feeds cross-region inputs out of `region_input_buffers`, then
//! re-walks the same arms.
//!
//! Composition body recursion: when a region member is a
//! `PlanNode::Composition` whose body carries body-internal regions
//! (surfaced through [`ExecutionPlanDag::from_body`]'s
//! `deferred_regions` clone), the dispatcher pushes the body context
//! onto `window_runtime.active_stack` (mirroring
//! `execute_composition_body` in `dispatch.rs`), recursively dispatches
//! the body's regions on the body's transient DAG, then restores the
//! parent context. This keeps the EdgeIndex namespace
//! (`region_input_buffers` keys by `(Option<CompositionBodyId>,
//! EdgeIndex)`) consistent across nesting: the body id on the active
//! stack disambiguates body-local edge ids from parent edge ids that
//! happen to number the same.

use std::collections::{HashMap, HashSet};

use clinker_record::Record;
use petgraph::Direction;
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::visit::{EdgeRef, Topo};

use super::DlqEvent;
use super::detect::RetractScope;
use crate::error::PipelineError;
use crate::executor::dispatch::{ExecutorContext, dispatch_plan_node};
use crate::plan::CompositionBodyId;
use crate::plan::deferred_region::DeferredRegion;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};

/// Walk every [`DeferredRegion`] reachable from `current_dag` (top-level
/// regions plus body-internal regions surfaced through the
/// `Composition` arm's `from_body` clone) in topological order and
/// drive the same operator arms `dispatch_plan_node` runs on the
/// forward pass against the post-recompute aggregate emits parked in
/// each region's producer `node_buffers` slot.
///
/// Returns the per-member DLQ entries the deferred dispatch produced.
/// The orchestrator's outer loop feeds these back to
/// `RetractScope::expand_with_dlq_events` to widen the scope for the
/// next iteration when a deferred operator fails on a record that
/// previously did not trigger.
///
/// # Errors
///
/// Returns whatever the inner operator arms return on failure: schema
/// drift (`PipelineError::Compilation` from `check_input_schema`),
/// memory-budget overruns from `tee_emit_to_region_input_buffers`,
/// downstream Combine/Aggregate engine errors. `FailFast` strategy
/// short-circuits the walk; `Continue` / `BestEffort` route per-record
/// failures to `ctx.dlq_entries` (which the dispatcher captures into
/// the returned [`DlqEvent`] vec).
pub(crate) fn dispatch_deferred_subdag(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    scope: &RetractScope,
) -> Result<Vec<DlqEvent>, PipelineError> {
    // `scope` is currently consulted only by the orchestrator's outer
    // loop (which feeds it to `recompute_aggregates` BEFORE this
    // dispatcher runs); the regions to walk are read directly off
    // `current_dag.deferred_regions`. Held in the signature so future
    // refinements (per-scope filtering, partial-replay) can hook in
    // without an API break.
    let _ = scope;
    let mut events: Vec<DlqEvent> = Vec::new();

    // Flip the deferred-dispatch guard interpretation for the duration
    // of this call so the per-arm short-circuit at the top of
    // `dispatch_plan_node` becomes a no-op for region members. The
    // flag is restored explicitly on every exit path to keep the outer
    // forward pass observing its original value (currently always
    // `false`, but the contract is local).
    let saved_in_deferred = ctx.in_deferred_dispatch;
    ctx.in_deferred_dispatch = true;

    let result = dispatch_deferred_inner(ctx, current_dag, &mut events);

    ctx.in_deferred_dispatch = saved_in_deferred;
    result.map(|()| events)
}

/// Inner driver: factored out so the caller can balance
/// `in_deferred_dispatch` across `?`-bubble exits without a RAII
/// guard (an RAII guard holding `&mut ExecutorContext` would extend
/// the borrow through every recursive `dispatch_plan_node` call,
/// which the borrow-checker rejects).
fn dispatch_deferred_inner(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    events: &mut Vec<DlqEvent>,
) -> Result<(), PipelineError> {
    // Top-level region walk. `current_dag.deferred_regions` keys every
    // participating NodeIndex (producer + members + outputs) to a
    // shared `DeferredRegion`, so deduplicate by producer to walk each
    // region exactly once.
    let mut walked_producers: HashSet<NodeIndex> = HashSet::new();
    let region_keys: Vec<NodeIndex> = current_dag.deferred_regions.keys().copied().collect();
    for key in region_keys {
        let producer = match current_dag.deferred_regions.get(&key) {
            Some(r) => r.producer,
            None => continue,
        };
        if !walked_producers.insert(producer) {
            continue;
        }
        // Clone the region metadata so the borrow on `current_dag`
        // releases before each recursive `dispatch_plan_node` call.
        let region = current_dag
            .deferred_regions
            .get(&producer)
            .expect("deferred_regions keys producer to its own region")
            .clone();
        dispatch_one_region(ctx, current_dag, &region, events)?;
    }

    // Body-internal regions: when any Composition node's bound body
    // declares its own deferred regions, recurse into the body's
    // transient DAG. The dispatcher walks every Composition node in
    // the current DAG and recurses if the body has any deferred
    // regions, regardless of parent-region containment, so a body
    // whose internal Aggregate sits below a strict-CK parent operator
    // still gets its commit-time replay.
    let composition_members: Vec<(NodeIndex, CompositionBodyId)> = current_dag
        .graph
        .node_indices()
        .filter_map(|idx| match &current_dag.graph[idx] {
            PlanNode::Composition { body, .. } => Some((idx, *body)),
            _ => None,
        })
        .collect();
    for (composition_idx, body_id) in composition_members {
        // Recurse when the body OR any descendant body carries a
        // deferred region. A pure-passthrough wrapper body (no
        // deferred region of its own) still owns the parent path
        // through which a nested body's deferred dispatch must fire.
        let needs_recurse = ctx.artifacts.body_of(body_id).is_some_and(|b| {
            crate::plan::composition_body::body_or_descendants_have_deferred_region(
                ctx.artifacts,
                b,
            )
        });
        if !needs_recurse {
            continue;
        }
        recurse_into_body(ctx, current_dag, composition_idx, body_id, events)?;
    }

    Ok(())
}

/// Walk one region's members in topological order over a sub-graph
/// filter (`members ∪ {producer}`), seeding cross-region inputs from
/// `region_input_buffers` before each member dispatch and capturing
/// any DLQ entries the arm produced.
fn dispatch_one_region(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    region: &DeferredRegion,
    events: &mut Vec<DlqEvent>,
) -> Result<(), PipelineError> {
    // Sub-DAG walk constrained to the region's producer + members +
    // outputs. `Topo` over the parent DAG yields every node; we filter
    // to region participants and start from the producer so members
    // see seeded buffers in the right order.
    //
    // The producer itself is NOT redispatched — its `node_buffers`
    // slot is already populated by `recompute_aggregates` with the
    // post-recompute narrow rows. Members and outputs run.
    let in_region: HashSet<NodeIndex> = region
        .members
        .iter()
        .chain(region.outputs.iter())
        .chain(std::iter::once(&region.producer))
        .copied()
        .collect();

    let mut topo_walk = Topo::new(&current_dag.graph);
    let mut topo: Vec<NodeIndex> = Vec::new();
    while let Some(idx) = topo_walk.next(&current_dag.graph) {
        if in_region.contains(&idx) {
            topo.push(idx);
        }
    }

    // Clear stale forward-pass `node_buffers` entries for every
    // non-producer member. Route's forward-pass branch dispatch
    // (and similar own-buffer-write arms) parks records into
    // `node_buffers[succ_idx]` BEFORE the consumer arm gets a chance
    // to short-circuit, so the slot carries pre-recompute records the
    // commit pass must not re-read. The producer's slot is preserved
    // — `recompute_aggregates` populates it with the post-recompute
    // narrow rows.
    for &m in &region.members {
        ctx.node_buffers.remove(&m);
    }
    for &o in &region.outputs {
        ctx.node_buffers.remove(&o);
    }

    let active_body = ctx.window_runtime.active_stack.last().copied();

    for idx in topo {
        if idx == region.producer {
            // Producer's slot was seeded by `recompute_aggregates`;
            // do not redispatch.
            continue;
        }
        seed_cross_region_inputs_for(ctx, current_dag, idx, region, active_body)?;
        let dlq_len_before = ctx.dlq_entries.len();
        // Count one partition emit per windowed-Transform member
        // dispatched on the commit pass. Each such Transform
        // re-evaluates a partition slice over the post-recompute
        // upstream emits seeded into `node_buffers[producer]`; the
        // counter measures deferred-region partition activity, not
        // an ahead-of-time recompute walk.
        let is_windowed_transform = matches!(
            &current_dag.graph[idx],
            PlanNode::Transform {
                window_index: Some(_),
                ..
            }
        );
        dispatch_plan_node(ctx, current_dag, idx)?;
        if is_windowed_transform {
            ctx.counters.retraction.partitions_dispatched += 1;
        }
        // Aggregate-finalize and similar non-buffer-routed arms push
        // directly to `dlq_entries`; harvest those into events so the
        // orchestrator's loop sees them. Buffer-routed errors (per-
        // record Transform / Route eval failures) carry richer lineage
        // through the synthetic-CK column on the buffered cell — the
        // orchestrator re-runs `detect_retract_scope` against the live
        // buffer after each dispatch to recover the contributing source
        // rows from that lineage.
        if ctx.dlq_entries.len() > dlq_len_before {
            for entry in &ctx.dlq_entries[dlq_len_before..] {
                events.push(DlqEvent {
                    source_row: entry.source_row,
                });
            }
        }
    }

    Ok(())
}

/// For each in-edge of `consumer_idx` whose source is OUTSIDE the
/// region (or in a different region), drain
/// `region_input_buffers[(active_body, edge_idx)]` and append the
/// records onto the consumer's `node_buffers` slot. Internal-region
/// edges are skipped — `node_buffers` already carries those records
/// from the upstream member's emit on this same commit pass.
///
/// The Combine arm (and any other multi-input member) reads its inputs
/// by upstream-name lookup against incoming neighbors, so dropping the
/// records into the consumer's own `node_buffers` slot would not be
/// addressable by the Combine arm. Instead, the records are appended
/// onto the SOURCE of each crossing edge — the upstream node's slot —
/// so the consumer's existing predecessor-walk logic finds them.
fn seed_cross_region_inputs_for(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    consumer_idx: NodeIndex,
    region: &DeferredRegion,
    active_body: Option<CompositionBodyId>,
) -> Result<(), PipelineError> {
    let crossings: Vec<(NodeIndex, EdgeIndex)> = current_dag
        .graph
        .edges_directed(consumer_idx, Direction::Incoming)
        .filter_map(|e| {
            let source = e.source();
            // Skip in-region edges — node_buffers already carries the
            // upstream member's commit-pass emit.
            let in_region = region.members.contains(&source)
                || region.outputs.contains(&source)
                || source == region.producer;
            if in_region {
                None
            } else {
                Some((source, e.id()))
            }
        })
        .collect();

    for (source_idx, edge_id) in crossings {
        let key = (active_body, edge_id);
        let Some(parked) = ctx.region_input_buffers.remove(&key) else {
            continue;
        };
        // Append onto the source node's buffer so the consumer's
        // existing predecessor-walk logic resolves them.
        ctx.node_buffers
            .entry(source_idx)
            .or_default()
            .extend(parked);
    }

    Ok(())
}

/// Set up body-context (window-runtime overlay, body-local
/// `node_buffers` namespace, body input-ref table) the way
/// `execute_composition_body` does, run a body-scoped detect +
/// recompute against the body's relaxed-CK aggregates, recursively
/// dispatch the body's deferred regions on the body's transient DAG,
/// then restore parent scope.
///
/// Scope is per-DAG: the body's recompute fires only on body-internal
/// relaxed-CK aggregates (those whose `node_idx` lives in the body's
/// graph index space), and only against the source rows the body's
/// own [`detect_retract_scope`] folded in from the shared
/// `correlation_buffers`. The parent's retract scope does not flow
/// in — body and parent operate in disjoint NodeIndex namespaces, so
/// a parent-rooted aggregate's retract row id has no addressable
/// effect inside the body's `relaxed_aggregator_states` keyed by
/// body-local indices, and vice versa.
///
/// The composition's output port records are NOT re-harvested to the
/// parent's `node_buffers[composition_idx]` slot here. The parent
/// region (if any) does not re-execute the Composition arm on the
/// commit pass; the parent's downstream members already saw the
/// forward-pass composition emit. Cross-scope harvest of the body's
/// post-recompute output back to the parent's commit pass is a
/// follow-up — only relevant when a body-internal aggregate's
/// post-retract emit must propagate through a parent-side deferred
/// operator, which today's compose surface does not produce because
/// every parent region treats Composition as a passthrough for
/// `buffer_schema` propagation.
fn recurse_into_body(
    ctx: &mut ExecutorContext<'_>,
    parent_dag: &ExecutionPlanDag,
    composition_idx: NodeIndex,
    body_id: CompositionBodyId,
    events: &mut Vec<DlqEvent>,
) -> Result<(), PipelineError> {
    let _ = composition_idx;
    let _ = parent_dag;

    let bound_body = match ctx.artifacts.body_of(body_id) {
        Some(b) => b,
        None => return Ok(()),
    };
    let body_dag = ExecutionPlanDag::from_body(bound_body);

    // Swap to body-scope buffers so body NodeIndices index a fresh
    // namespace (parent-scope NodeIndex 0 and body-scope NodeIndex 0
    // collide numerically). Restore on every exit.
    let body_buffers: HashMap<NodeIndex, Vec<(Record, u64)>> = HashMap::new();
    let saved_buffers = std::mem::replace(&mut ctx.node_buffers, body_buffers);
    let saved_combine = std::mem::take(&mut ctx.combine_source_records);
    let saved_body_refs = ctx
        .current_body_node_input_refs
        .replace(bound_body.node_input_refs.clone());

    // Push body context onto the window-runtime stack so any window
    // dispatched inside the body resolves through the body's overlay
    // and `region_input_buffers` keys land under
    // `(Some(body_id), edge_id)` — the same key the forward-pass
    // body walker used when it tee'd cross-region inputs.
    let body_index_count = bound_body.body_indices_to_build.len();
    let body_window_vec: Vec<Option<crate::executor::window_runtime::WindowRuntime>> =
        (0..body_index_count).map(|_| None).collect();
    ctx.window_runtime.bodies.insert(body_id, body_window_vec);
    ctx.window_runtime.active_stack.push(body_id);

    let walk_result = (|| -> Result<(), PipelineError> {
        // Body-scoped detect + recompute: detect against the body's
        // own DAG so the producer/region walk uses body-local
        // NodeIndices; recompute against that body scope so each
        // body-internal relaxed-CK aggregate's
        // `relaxed_aggregator_states[body_local_idx]` is retracted
        // and its `node_buffers[body_local_idx]` is re-seeded with
        // the post-recompute narrow rows. The body's iteration
        // structure mirrors the parent's: the parent orchestrator
        // already drives the cascading-retract loop one level up;
        // for body-internal cascades that stay inside the body, the
        // outer loop's next iteration re-enters this recurse and
        // re-runs the body detect against the updated
        // `correlation_buffers`.
        let body_scope = super::detect::detect_retract_scope(ctx, &body_dag);
        let body_initial_rows: Vec<u32> = body_scope.seen_source_rows.iter().copied().collect();
        super::recompute_agg::recompute_aggregates(
            ctx,
            &body_dag,
            &body_scope,
            &body_initial_rows,
        )?;

        // Seed every body-region producer's `node_buffers[producer]`
        // from the retained body aggregator. Forward-pass body walks
        // drop body-local `node_buffers` on exit (the parent scope
        // restored its own map), so the body's projected emit is
        // gone by the time `recurse_into_body` swapped in a fresh
        // empty body-buffers map above. `recompute_aggregates`
        // populates the slot only when the per-iteration retract
        // delta is non-empty; the no-trigger initial pass would
        // otherwise leave the body's deferred members reading from
        // an empty input. Re-finalize via `emit_post_recompute` to
        // restore the body aggregator's emit before dispatching the
        // body region.
        for body_region in body_dag.deferred_regions.values() {
            let producer = body_region.producer;
            if ctx.node_buffers.contains_key(&producer) {
                continue;
            }
            if !ctx.relaxed_aggregator_states.contains_key(&producer) {
                continue;
            }
            if let Err(e) = super::recompute_agg::emit_post_recompute(ctx, &body_dag, producer) {
                return Err(PipelineError::Internal {
                    op: "executor",
                    node: body_dag.graph[producer].name().to_string(),
                    detail: format!("body-aggregate emit re-materialization failed: {e}"),
                });
            }
        }

        let inner_events = dispatch_deferred_subdag(ctx, &body_dag, &body_scope)?;
        events.extend(inner_events);
        Ok(())
    })();

    // Restore parent scope. The window-runtime body overlay is
    // popped first so subsequent parent-scope walks route through
    // `top` again, mirroring the forward-pass body executor's exit
    // ordering.
    ctx.window_runtime.active_stack.pop();
    ctx.window_runtime.bodies.remove(&body_id);
    ctx.current_body_node_input_refs = saved_body_refs;
    ctx.combine_source_records = saved_combine;
    ctx.node_buffers = saved_buffers;

    walk_result
}
