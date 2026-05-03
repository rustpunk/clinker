//! Deferred-region detection — plan-time analysis that identifies the
//! sub-graph of operators downstream of every relaxed-CK Aggregate up
//! to the next correlation-buffered Output (or composition body output
//! port).
//!
//! Operators inside a region eventually run on post-recompute aggregate
//! emits at commit time rather than on the forward pass. This module
//! computes the membership and the column-pruned buffer schema the
//! producing Aggregate must carry forward; consumer wiring lives in
//! later phases of the same sprint.
//!
//! Two passes:
//!
//! - **Pass A (forward BFS).** Seed at each relaxed-CK Aggregate; walk
//!   outgoing edges, tagging members and outputs until an Output halts
//!   recursion. Walks recurse into composition bodies via the body's
//!   `output_port_to_node_idx` map back to the parent's continuation.
//!
//! - **Pass B (reverse-topo column propagation).** From each Output's
//!   consumed columns, propagate `Expr::support_into` upstream through
//!   each member operator, terminating at the producer. The producer's
//!   `buffer_schema` filters the producer's `output_schema` columns to
//!   that consumed set, preserving the producer's own emit order.
//!   Producer-order matters because every downstream operator carries an
//!   `expected: Arc<Schema>` stamped at plan-compile time against the
//!   producer's `output_schema`; a buffer projection that reorders the
//!   columns trips the per-record `check_input_schema` at the first
//!   downstream consumer.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use cxl::ast::Statement;
use petgraph::Direction;
use petgraph::graph::{DiGraph, NodeIndex};

use crate::plan::bind_schema::CompileArtifacts;
use crate::plan::composition_body::{BoundBody, CompositionBodyId};
use crate::plan::execution::{PlanEdge, PlanNode, group_by_omits_any_ck_field};
use crate::plan::index::IndexSpec;
use crate::plan::properties::NodeProperties;

/// Plan-time metadata for one deferred region: a relaxed-CK Aggregate
/// (`producer`) plus the chain of operators between it and the
/// correlation-buffered Outputs at the region's exit boundary.
///
/// Operators in `members` are skipped on the forward pass and run at
/// commit time on post-recompute aggregate emits. The producer's emit
/// buffer carries `buffer_schema` — exactly the columns the deferred
/// operators reach via `Expr::support_into`.
#[derive(Clone, Debug)]
pub(crate) struct DeferredRegion {
    /// `NodeIndex` of the relaxed-CK Aggregate that seeds this region.
    /// For body-internal Aggregates, this is the body's local index;
    /// the parent-level flatten in `config/mod.rs` keys regions only
    /// by parent-graph indices, so consumers inside body executors
    /// must look up by their body-local map.
    pub(crate) producer: NodeIndex,
    /// Every non-producer, non-output operator inside the region.
    pub(crate) members: HashSet<NodeIndex>,
    /// Correlation-buffered Outputs at the region's exit boundary.
    pub(crate) outputs: HashSet<NodeIndex>,
    /// Minimum set of producer-emitted columns that the deferred
    /// operators consume, ordered to match the producer's
    /// `output_schema`. Producer-order is required because every
    /// downstream operator's `check_input_schema` compares the
    /// projected record's column list against the upstream's
    /// stamped `output_schema`; reordering the columns (e.g.
    /// alphabetical) trips a `SchemaMismatch` at the first
    /// downstream consumer.
    pub(crate) buffer_schema: Vec<String>,
}

/// Plan-time continuation metadata for a Composition node whose
/// (transitively) bound body carries a deferred region.
///
/// `members` are the operators reachable downstream of the Composition
/// in the containing DAG, up to but not including any strict-CK or
/// relaxed-CK Aggregation (which owns its own forward-pass emit or its
/// own DeferredRegion respectively). `outputs` are the correlation-
/// buffered Outputs at the continuation's exit boundary. The
/// commit-time dispatcher walks these in topological order via
/// `dispatch_plan_node` with `in_deferred_dispatch=true` after
/// harvesting body output records into `node_buffers[composition_idx]`.
///
/// Distinct from [`DeferredRegion`]: a region has a relaxed-CK
/// Aggregate as producer, while a continuation is rooted at a
/// Composition node whose post-harvest buffer slot acts as the
/// producer-equivalent. The two surfaces are disjoint by construction.
#[derive(Clone, Debug)]
pub(crate) struct ParentContinuation {
    /// `NodeIndex` of the Composition node in the containing DAG. The
    /// post-harvest seed lands in `node_buffers[composition_idx]`.
    pub(crate) composition_idx: NodeIndex,
    /// Operators between the Composition and the continuation's exit
    /// boundary. Excludes the seed Composition itself.
    pub(crate) members: HashSet<NodeIndex>,
    /// Outputs at the continuation's exit boundary.
    pub(crate) outputs: HashSet<NodeIndex>,
}

/// One call site of a composition body — a `PlanNode::Composition`
/// node that targets the body. Combined with the analysis's
/// continuation maps, this is enough to look up the parent operators
/// downstream of the call: `scope=None` →
/// `top_continuations[composition_idx]`; `scope=Some(bid)` →
/// `body_continuations[bid][composition_idx]`.
#[derive(Clone, Debug)]
pub(crate) struct CompositionCallSite {
    /// `None` for parent-scope calls; `Some(outer_body_id)` for
    /// body-internal calls.
    pub(crate) scope: Option<CompositionBodyId>,
    /// `NodeIndex` of the calling Composition in its containing graph.
    pub(crate) composition_idx: NodeIndex,
}

/// Aggregate output of [`detect_deferred_regions`]: every
/// relaxed-CK-Aggregate region (top-level + per-body) plus every
/// Composition continuation that the commit-time dispatcher must
/// re-execute after harvesting body output records.
///
/// Continuations are tracked separately from regions because they are
/// rooted at Composition nodes (no Aggregate producer), but the two
/// maps live alongside each other on the same scope: top-level
/// continuations key into the parent DAG, body continuations into the
/// outer body's mini-DAG.
pub(crate) struct DeferredRegionAnalysis {
    pub(crate) top_level: Vec<DeferredRegion>,
    pub(crate) body_regions: HashMap<CompositionBodyId, Vec<DeferredRegion>>,
    pub(crate) top_continuations: HashMap<NodeIndex, ParentContinuation>,
    pub(crate) body_continuations:
        HashMap<CompositionBodyId, HashMap<NodeIndex, ParentContinuation>>,
    /// Per-body call-site index — every parent-scope or
    /// outer-body-scope `PlanNode::Composition` that targets the body.
    /// `Vec`, not single entry, because the architectural contract is
    /// "buffer_schema is the union of every consumer's demand across
    /// all call sites": today's `bind_composition` allocates a fresh
    /// `CompositionBodyId` per call (Vec is always length 1) but
    /// encoding singular hardcodes an implementation accident; future
    /// allocator policies (memoization, body-sharing) require the
    /// union.
    pub(crate) body_call_sites: HashMap<CompositionBodyId, Vec<CompositionCallSite>>,
}

/// Walk every relaxed-CK Aggregate in the top-level DAG (recursing
/// through composition bodies via `artifacts.composition_bodies`) and
/// return one [`DeferredRegion`] per producer plus a
/// [`ParentContinuation`] entry for every Composition node whose body
/// carries a deferred region.
///
/// The four returned maps live at disjoint scopes:
///
/// - `top_level` — regions whose `producer` is a parent-graph
///   `NodeIndex`. The caller flattens these into
///   `ExecutionPlanDag.deferred_regions`.
/// - `body_regions` — regions seeded by body-internal Aggregates,
///   keyed by the body that owns them. The caller flattens each entry
///   into the matching `BoundBody.deferred_regions`. The two index
///   spaces are separated because a body-local NodeIndex can
///   numerically collide with a parent-graph NodeIndex (each graph
///   numbers from zero); the dispatcher consults the right map by
///   which scope it is currently executing.
/// - `top_continuations` — Composition nodes in the parent DAG whose
///   body carries a deferred region; populates
///   `ExecutionPlanDag.parent_continuations`.
/// - `body_continuations` — Composition nodes inside an outer body
///   whose nested body carries a deferred region; the caller flattens
///   each entry onto `BoundBody.parent_continuations` of the outer
///   body.
pub(crate) fn detect_deferred_regions(
    graph: &DiGraph<PlanNode, PlanEdge>,
    node_properties: &HashMap<NodeIndex, NodeProperties>,
    artifacts: &CompileArtifacts,
    indices_to_build: &[IndexSpec],
) -> DeferredRegionAnalysis {
    let mut top_level = Vec::new();
    let mut body_regions: HashMap<CompositionBodyId, Vec<DeferredRegion>> = HashMap::new();
    let mut top_continuations: HashMap<NodeIndex, ParentContinuation> = HashMap::new();
    let mut body_continuations: HashMap<CompositionBodyId, HashMap<NodeIndex, ParentContinuation>> =
        HashMap::new();
    let mut body_call_sites: HashMap<CompositionBodyId, Vec<CompositionCallSite>> = HashMap::new();

    // Top-level relaxed-CK aggregates.
    for idx in graph.node_indices() {
        let PlanNode::Aggregation { config, .. } = &graph[idx] else {
            continue;
        };
        let parent_ck = parent_ck_of(graph, node_properties, idx);
        if !group_by_omits_any_ck_field(&config.group_by, &parent_ck) {
            continue;
        }
        if let Some(region) = build_region(graph, artifacts, indices_to_build, idx) {
            top_level.push(region);
        }
    }

    // Body-internal relaxed-CK aggregates. Walk every body, derive the
    // body's parent-CK at each Aggregate from the upstream stored
    // schema (mirrors `apply_retraction_flags_in_body`), and seed a
    // region inside the body's mini-DAG.
    for (body_id, body) in &artifacts.composition_bodies {
        for idx in body.graph.node_indices() {
            let PlanNode::Aggregation { config, .. } = &body.graph[idx] else {
                continue;
            };
            let parent_ck = body_parent_ck_of(body, idx);
            if !group_by_omits_any_ck_field(&config.group_by, &parent_ck) {
                continue;
            }
            if let Some(region) = build_body_region(body, idx) {
                body_regions.entry(*body_id).or_default().push(region);
            }
        }
    }

    // Pre-compute which bodies carry (or transitively contain) a
    // deferred region. The continuation walker consults this set
    // instead of `BoundBody.deferred_regions`, which is still empty
    // here — the parent caller flattens the per-body regions back
    // into `body.deferred_regions` only AFTER detection returns. A
    // continuation walker that read the empty pre-flatten map would
    // miss every body region and silently produce no continuation.
    let mut bodies_with_region: HashSet<CompositionBodyId> = body_regions.keys().copied().collect();
    propagate_body_region_membership(artifacts, &mut bodies_with_region);

    // Top-level Composition continuations. Every Composition whose
    // body (or any nested body) carries a deferred region needs the
    // parent's downstream chain re-executed after the commit-time
    // harvest. The call-site index is populated unconditionally — the
    // continuation map filters by region presence, but a future
    // consumer may demand columns from a body that today carries no
    // region, so the index must be complete.
    for idx in graph.node_indices() {
        let PlanNode::Composition { body, .. } = &graph[idx] else {
            continue;
        };
        body_call_sites
            .entry(*body)
            .or_default()
            .push(CompositionCallSite {
                scope: None,
                composition_idx: idx,
            });
        if let Some(cont) = compute_continuation_from(idx, graph, &bodies_with_region) {
            top_continuations.insert(idx, cont);
        }
    }

    // Body-internal Composition continuations: nested Composition
    // nodes whose own body carries a deferred region. The outer body
    // dispatcher walks these once it has harvested the nested body's
    // output records. Call-site index population is unconditional, as
    // above.
    for (body_id, body) in &artifacts.composition_bodies {
        for idx in body.graph.node_indices() {
            let PlanNode::Composition { body: nested, .. } = &body.graph[idx] else {
                continue;
            };
            body_call_sites
                .entry(*nested)
                .or_default()
                .push(CompositionCallSite {
                    scope: Some(*body_id),
                    composition_idx: idx,
                });
            if let Some(cont) = compute_body_continuation_from(idx, body, &bodies_with_region) {
                body_continuations
                    .entry(*body_id)
                    .or_default()
                    .insert(idx, cont);
            }
        }
    }

    DeferredRegionAnalysis {
        top_level,
        body_regions,
        top_continuations,
        body_continuations,
        body_call_sites,
    }
}

/// Saturating walk that grows `bodies_with_region` to include every
/// body whose mini-DAG transitively reaches a body already in the set
/// via a nested `PlanNode::Composition`. Mirrors the runtime
/// `body_or_descendants_have_deferred_region` recursion but consults
/// the pre-built per-body region set instead of the not-yet-populated
/// `BoundBody.deferred_regions` field. Iterates to a fixed point so
/// arbitrary nesting depth converges in a single pass through every
/// body per round.
fn propagate_body_region_membership(
    artifacts: &CompileArtifacts,
    bodies_with_region: &mut HashSet<CompositionBodyId>,
) {
    loop {
        let mut grew = false;
        for (body_id, body) in &artifacts.composition_bodies {
            if bodies_with_region.contains(body_id) {
                continue;
            }
            let reaches = body.graph.node_indices().any(|idx| {
                let PlanNode::Composition { body: nested, .. } = &body.graph[idx] else {
                    return false;
                };
                bodies_with_region.contains(nested)
            });
            if reaches {
                bodies_with_region.insert(*body_id);
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }
}

/// Compute the parent-DAG continuation downstream of a Composition
/// node when the bound body carries a deferred region.
///
/// `None` when the body has no deferred region anywhere below, or when
/// the BFS produces neither a member nor an Output (a Composition
/// terminating in nothing reachable). Walks outgoing edges in the
/// parent DAG, halting recursion at every Output (added to `outputs`)
/// and at every Aggregation (excluded from both sets — strict
/// aggregates own their forward-pass emit; relaxed aggregates own a
/// DeferredRegion). The seed Composition itself is never added.
fn compute_continuation_from(
    c_idx: NodeIndex,
    graph: &DiGraph<PlanNode, PlanEdge>,
    bodies_with_region: &HashSet<CompositionBodyId>,
) -> Option<ParentContinuation> {
    let PlanNode::Composition { body, .. } = &graph[c_idx] else {
        return None;
    };
    if !bodies_with_region.contains(body) {
        return None;
    }
    let (members, outputs) = continuation_bfs(graph, c_idx);
    if members.is_empty() && outputs.is_empty() {
        return None;
    }
    let cont = ParentContinuation {
        composition_idx: c_idx,
        members,
        outputs,
    };
    debug_assert_eq!(
        cont.composition_idx, c_idx,
        "ParentContinuation seed must match its key in parent_continuations",
    );
    Some(cont)
}

/// Body-graph variant of [`compute_continuation_from`]. Walks the
/// outer body's mini-DAG starting at a nested Composition node,
/// returning `None` when the nested body carries no deferred region
/// or when the BFS yields no continuation work.
fn compute_body_continuation_from(
    c_idx: NodeIndex,
    body: &BoundBody,
    bodies_with_region: &HashSet<CompositionBodyId>,
) -> Option<ParentContinuation> {
    let PlanNode::Composition {
        body: nested_id, ..
    } = &body.graph[c_idx]
    else {
        return None;
    };
    if !bodies_with_region.contains(nested_id) {
        return None;
    }
    let (members, outputs) = continuation_bfs(&body.graph, c_idx);
    if members.is_empty() && outputs.is_empty() {
        return None;
    }
    let cont = ParentContinuation {
        composition_idx: c_idx,
        members,
        outputs,
    };
    debug_assert_eq!(
        cont.composition_idx, c_idx,
        "ParentContinuation seed must match its key in parent_continuations",
    );
    Some(cont)
}

/// Forward BFS over outgoing edges in `graph` from `seed`, collecting
/// reachable non-Aggregation nodes into `members` (Outputs go into
/// `outputs` and halt recursion).
///
/// The seed node is never added to either set. Aggregations halt
/// recursion AND are excluded from both sets — a strict aggregate
/// already runs on the forward pass; a relaxed aggregate already owns
/// a DeferredRegion. Either way, the continuation's responsibility
/// ends at the aggregate boundary.
fn continuation_bfs(
    graph: &DiGraph<PlanNode, PlanEdge>,
    seed: NodeIndex,
) -> (HashSet<NodeIndex>, HashSet<NodeIndex>) {
    let mut members: HashSet<NodeIndex> = HashSet::new();
    let mut outputs: HashSet<NodeIndex> = HashSet::new();
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut queue: VecDeque<NodeIndex> = VecDeque::new();
    visited.insert(seed);
    for succ in graph.neighbors_directed(seed, Direction::Outgoing) {
        queue.push_back(succ);
    }
    while let Some(node) = queue.pop_front() {
        if !visited.insert(node) {
            continue;
        }
        match &graph[node] {
            PlanNode::Output { .. } => {
                outputs.insert(node);
            }
            PlanNode::Aggregation { .. } => {
                // Halt; the aggregate is not part of the continuation.
            }
            _ => {
                members.insert(node);
                for succ in graph.neighbors_directed(node, Direction::Outgoing) {
                    queue.push_back(succ);
                }
            }
        }
    }
    (members, outputs)
}

/// Resolve the upstream node's CK set from `node_properties` for a
/// top-level Aggregate. Empty when the upstream lattice entry is
/// missing — same convention `apply_retraction_flags` uses.
fn parent_ck_of(
    graph: &DiGraph<PlanNode, PlanEdge>,
    node_properties: &HashMap<NodeIndex, NodeProperties>,
    idx: NodeIndex,
) -> BTreeSet<String> {
    graph
        .neighbors_directed(idx, Direction::Incoming)
        .next()
        .and_then(|p| node_properties.get(&p))
        .map(|p| p.ck_set.clone())
        .unwrap_or_default()
}

/// Body-graph variant: walk the nearest upstream node's stored output
/// schema for `$ck.*` shadow columns. Mirrors
/// [`crate::plan::execution::apply_retraction_flags_in_body`].
fn body_parent_ck_of(body: &BoundBody, idx: NodeIndex) -> BTreeSet<String> {
    use clinker_record::FieldMetadata;

    let mut ck: BTreeSet<String> = BTreeSet::new();
    let mut cursor = idx;
    while let Some(upstream) = body
        .graph
        .neighbors_directed(cursor, Direction::Incoming)
        .next()
    {
        if let Some(schema) = body.graph[upstream].stored_output_schema() {
            for (i, col) in schema.columns().iter().enumerate() {
                if matches!(
                    schema.field_metadata(i),
                    Some(FieldMetadata::SourceCorrelation { .. }),
                ) && let Some(field) = col.strip_prefix("$ck.")
                {
                    ck.insert(field.to_string());
                }
            }
            break;
        }
        cursor = upstream;
    }
    ck
}

/// Build a region rooted at a top-level relaxed-CK Aggregate. Pass A
/// fans out from the producer; Pass B prunes the producer's buffer
/// schema to the columns the deferred operators consume.
fn build_region(
    graph: &DiGraph<PlanNode, PlanEdge>,
    artifacts: &CompileArtifacts,
    indices_to_build: &[IndexSpec],
    producer: NodeIndex,
) -> Option<DeferredRegion> {
    let (members, outputs) = pass_a_forward_bfs(graph, artifacts, producer);
    let buffer_schema = pass_b_column_prune(
        graph,
        artifacts,
        indices_to_build,
        producer,
        &members,
        &outputs,
    );
    Some(DeferredRegion {
        producer,
        members,
        outputs,
        buffer_schema,
    })
}

/// Body-graph variant of `build_region`. Body Aggregates seed regions
/// confined to the body's mini-DAG; cross-body propagation back to the
/// parent is handled by the parent-level walk reaching the body via a
/// `Composition` node.
fn build_body_region(body: &BoundBody, producer: NodeIndex) -> Option<DeferredRegion> {
    let (members, outputs) = pass_a_body(body, producer);
    let buffer_schema = pass_b_body(body, &body.body_indices_to_build, producer, &members);
    Some(DeferredRegion {
        producer,
        members,
        outputs,
        buffer_schema,
    })
}

// ─── Pass A (forward BFS) ──────────────────────────────────────────────

/// Walk outgoing edges from `producer` in BFS order; collect every
/// non-producer node into `members` until an Output halts recursion
/// (added to `outputs` instead). Composition nodes recurse into their
/// body via `output_port_to_node_idx`, then bubble back up to the
/// composition's downstream successors.
fn pass_a_forward_bfs(
    graph: &DiGraph<PlanNode, PlanEdge>,
    artifacts: &CompileArtifacts,
    producer: NodeIndex,
) -> (HashSet<NodeIndex>, HashSet<NodeIndex>) {
    let mut members: HashSet<NodeIndex> = HashSet::new();
    let mut outputs: HashSet<NodeIndex> = HashSet::new();
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut queue: VecDeque<NodeIndex> = VecDeque::new();
    visited.insert(producer);
    for succ in graph.neighbors_directed(producer, Direction::Outgoing) {
        queue.push_back(succ);
    }

    while let Some(node) = queue.pop_front() {
        if !visited.insert(node) {
            continue;
        }
        match &graph[node] {
            PlanNode::Output { .. } => {
                outputs.insert(node);
                // Do not recurse past Output — region exit boundary.
            }
            PlanNode::Composition { body, .. } => {
                members.insert(node);
                // Recurse the body. Every body-internal node along the
                // path from each input port to each output port is
                // added as a member; the body's outputs flow back to
                // the Composition node's downstream successors via
                // standard outgoing-edge walks.
                if let Some(b) = artifacts.body_of(*body) {
                    walk_body_into_region(b, artifacts, &mut members, &mut outputs);
                }
                for succ in graph.neighbors_directed(node, Direction::Outgoing) {
                    queue.push_back(succ);
                }
            }
            _ => {
                members.insert(node);
                for succ in graph.neighbors_directed(node, Direction::Outgoing) {
                    queue.push_back(succ);
                }
            }
        }
    }

    (members, outputs)
}

/// Walk every node inside a composition body. Body internals do not
/// have their own Outputs (body Outputs aren't lowered into the body
/// graph), so every reachable body node is a member; nested
/// compositions recurse the same way.
fn walk_body_into_region(
    body: &BoundBody,
    artifacts: &CompileArtifacts,
    members: &mut HashSet<NodeIndex>,
    outputs: &mut HashSet<NodeIndex>,
) {
    for idx in body.graph.node_indices() {
        match &body.graph[idx] {
            PlanNode::Output { .. } => {
                outputs.insert(idx);
            }
            PlanNode::Composition { body: nested, .. } => {
                members.insert(idx);
                if let Some(n) = artifacts.body_of(*nested) {
                    walk_body_into_region(n, artifacts, members, outputs);
                }
            }
            _ => {
                members.insert(idx);
            }
        }
    }
}

/// Body-graph Pass A — walks outgoing edges from a body-internal
/// Aggregate; treats every reachable body node as a member (body has
/// no Outputs proper).
fn pass_a_body(body: &BoundBody, producer: NodeIndex) -> (HashSet<NodeIndex>, HashSet<NodeIndex>) {
    let mut members: HashSet<NodeIndex> = HashSet::new();
    let outputs: HashSet<NodeIndex> = HashSet::new();
    let mut visited: HashSet<NodeIndex> = HashSet::new();
    let mut queue: VecDeque<NodeIndex> = VecDeque::new();
    visited.insert(producer);
    for succ in body.graph.neighbors_directed(producer, Direction::Outgoing) {
        queue.push_back(succ);
    }
    while let Some(node) = queue.pop_front() {
        if !visited.insert(node) {
            continue;
        }
        members.insert(node);
        for succ in body.graph.neighbors_directed(node, Direction::Outgoing) {
            queue.push_back(succ);
        }
    }
    (members, outputs)
}

// ─── Pass B (reverse-topo column propagation) ──────────────────────────

/// Compute the producer's buffer schema by propagating consumed-column
/// sets backwards through every member operator, seeded at each Output
/// from its projection rule.
///
/// The walk terminates at the producer; the union of every set
/// reaching the producer's outgoing edges is the buffer schema. Final
/// column ordering matches the producer's own `stored_output_schema`
/// emit order via `project_in_producer_order`; an alphabetical
/// ordering broke the downstream Output's `SchemaMismatch` check
/// because `expected_input_schema` reads producer-emit order verbatim.
fn pass_b_column_prune(
    graph: &DiGraph<PlanNode, PlanEdge>,
    artifacts: &CompileArtifacts,
    indices_to_build: &[IndexSpec],
    producer: NodeIndex,
    members: &HashSet<NodeIndex>,
    outputs: &HashSet<NodeIndex>,
) -> Vec<String> {
    // Per-node consumed-column set. An entry exists once at least one
    // downstream propagation has reached the node; the propagation
    // pass merges into the entry as more downstream nodes report in.
    let mut consumed: HashMap<NodeIndex, HashSet<String>> = HashMap::new();

    // Seed each Output with the columns its projection rule reads.
    for &out_idx in outputs {
        let cols = output_consumed_columns(graph, out_idx);
        consumed.entry(out_idx).or_default().extend(cols);
    }
    // Seed each windowed-Transform member with its IndexSpec
    // `arena_fields`. Window `partition_by` / `sort_by` columns live
    // on the index spec, not the CXL program, so `support_into` alone
    // would miss them — at commit time the deferred dispatcher must
    // rebuild the node-rooted arena from the post-recompute narrow
    // rows, and the arena projection requires every `arena_fields`
    // entry to be present in the buffer schema.
    for &m in members {
        if let PlanNode::Transform {
            window_index: Some(wi),
            ..
        } = &graph[m]
            && let Some(spec) = indices_to_build.get(*wi)
        {
            let entry = consumed.entry(m).or_default();
            for f in &spec.arena_fields {
                entry.insert(f.clone());
            }
        }
    }

    // Reverse-topo walk constrained to the region. Use a worklist:
    // pop a node, propagate its consumed set onto every upstream edge
    // (member or producer); terminate when nothing changes.
    //
    // The region is a DAG embedded in the parent DAG; per-node
    // re-queueing on every consumer-side update converges in O(|E|).
    let mut worklist: VecDeque<NodeIndex> = outputs.iter().copied().collect();
    for &m in members {
        if matches!(
            &graph[m],
            PlanNode::Transform {
                window_index: Some(_),
                ..
            }
        ) {
            worklist.push_back(m);
        }
    }
    while let Some(node) = worklist.pop_front() {
        let downstream_set = consumed.get(&node).cloned().unwrap_or_default();
        let upstream_set = propagate_through(graph, artifacts, node, &downstream_set);

        for upstream in graph.neighbors_directed(node, Direction::Incoming) {
            if upstream != producer && !members.contains(&upstream) {
                continue;
            }
            let entry = consumed.entry(upstream).or_default();
            let mut changed = false;
            for col in &upstream_set {
                if entry.insert(col.clone()) {
                    changed = true;
                }
            }
            if changed {
                worklist.push_back(upstream);
            }
        }
    }

    // The producer's buffer schema filters its own `output_schema` to
    // the consumed set, preserving emit order. A downstream operator's
    // `check_input_schema` compares against the producer's stamped
    // `output_schema` column list; a reordered buffer trips
    // `SchemaMismatch` at the first consumer.
    let mut consumed_at_producer = consumed.get(&producer).cloned().unwrap_or_default();
    add_producer_engine_stamped_columns(
        graph[producer].stored_output_schema(),
        &mut consumed_at_producer,
    );
    project_in_producer_order(
        graph[producer].stored_output_schema(),
        &consumed_at_producer,
    )
}

/// Body-graph Pass B. Simpler: bodies don't have Outputs, so the
/// region's exit boundary is every leaf member. Seed each leaf with
/// the union of `support_into` over its expressions, then propagate.
fn pass_b_body(
    body: &BoundBody,
    body_indices: &[IndexSpec],
    producer: NodeIndex,
    members: &HashSet<NodeIndex>,
) -> Vec<String> {
    let mut consumed: HashMap<NodeIndex, HashSet<String>> = HashMap::new();
    let mut worklist: VecDeque<NodeIndex> = VecDeque::new();

    // Seed every leaf member (no outgoing edges into another member or
    // producer) with the columns its own expressions read. Windowed
    // Transforms additionally seed `arena_fields` so the post-recompute
    // arena rebuild has every `partition_by` / `sort_by` column.
    for &m in members {
        let has_member_successor = body
            .graph
            .neighbors_directed(m, Direction::Outgoing)
            .any(|s| members.contains(&s));
        let mut seed: HashSet<String> = HashSet::new();
        if let PlanNode::Transform {
            window_index: Some(wi),
            ..
        } = &body.graph[m]
            && let Some(spec) = body_indices.get(*wi)
        {
            for f in &spec.arena_fields {
                seed.insert(f.clone());
            }
        }
        if has_member_successor && seed.is_empty() {
            continue;
        }
        seed_from_node(&body.graph[m], &mut seed);
        consumed.insert(m, seed);
        worklist.push_back(m);
    }

    while let Some(node) = worklist.pop_front() {
        let downstream_set = consumed.get(&node).cloned().unwrap_or_default();
        let upstream_set = propagate_body_through(body, node, &downstream_set);

        for upstream in body.graph.neighbors_directed(node, Direction::Incoming) {
            if upstream != producer && !members.contains(&upstream) {
                continue;
            }
            let entry = consumed.entry(upstream).or_default();
            let mut changed = false;
            for col in &upstream_set {
                if entry.insert(col.clone()) {
                    changed = true;
                }
            }
            if changed {
                worklist.push_back(upstream);
            }
        }
    }

    let mut consumed_at_producer = consumed.get(&producer).cloned().unwrap_or_default();
    add_producer_engine_stamped_columns(
        body.graph[producer].stored_output_schema(),
        &mut consumed_at_producer,
    );
    project_in_producer_order(
        body.graph[producer].stored_output_schema(),
        &consumed_at_producer,
    )
}

/// Force the producer's engine-stamped columns (synthetic-CK and
/// source-CK shadows) into the consumed set so the narrow buffer
/// schema retains them. Downstream operators' `expected_input_schema`
/// is the producer's full `output_schema` — a buffer that drops these
/// columns trips `SchemaMismatch` at the first consumer regardless of
/// whether the operators' CXL programs reach them via `support_into`.
fn add_producer_engine_stamped_columns(
    producer_schema: Option<&std::sync::Arc<clinker_record::Schema>>,
    consumed: &mut HashSet<String>,
) {
    let Some(schema) = producer_schema else {
        return;
    };
    for i in 0..schema.column_count() {
        if schema
            .field_metadata(i)
            .is_some_and(|m| m.is_engine_stamped())
            && let Some(name) = schema.column_name(i)
        {
            consumed.insert(name.to_string());
        }
    }
}

/// Filter the producer's `output_schema` columns to the consumed set,
/// preserving the producer's emit order. Falls back to alphabetical
/// over the consumed set when the producer has no stored schema (a
/// planner contract violation in practice — every region producer is
/// an Aggregate, which always carries `output_schema` — but kept total
/// so a degraded plan does not panic the buffer build).
fn project_in_producer_order(
    producer_schema: Option<&std::sync::Arc<clinker_record::Schema>>,
    consumed: &HashSet<String>,
) -> Vec<String> {
    if let Some(schema) = producer_schema {
        let mut out: Vec<String> = Vec::with_capacity(consumed.len());
        for col in schema.columns() {
            let s = col.as_ref();
            if consumed.contains(s) {
                out.push(s.to_string());
            }
        }
        return out;
    }
    let mut sorted: BTreeSet<String> = BTreeSet::new();
    for col in consumed {
        sorted.insert(col.clone());
    }
    sorted.into_iter().collect()
}

// ─── Per-operator propagation rules ────────────────────────────────────

/// Propagate `downstream_set` through `node` to its incoming edges.
/// Adds columns the node itself reads (Transform `support_into`,
/// Route predicates, Aggregate group-by + accumulator inputs) and
/// removes columns the node defines locally (Transform emit names).
fn propagate_through(
    graph: &DiGraph<PlanNode, PlanEdge>,
    artifacts: &CompileArtifacts,
    node: NodeIndex,
    downstream_set: &HashSet<String>,
) -> HashSet<String> {
    let mut upstream: HashSet<String> = downstream_set.clone();
    propagate_through_node(&graph[node], artifacts, &mut upstream);
    upstream
}

fn propagate_body_through(
    body: &BoundBody,
    node: NodeIndex,
    downstream_set: &HashSet<String>,
) -> HashSet<String> {
    let mut upstream: HashSet<String> = downstream_set.clone();
    propagate_through_node_for_body(&body.graph[node], &mut upstream);
    upstream
}

/// Apply per-operator rules to `set` in place (used by both top-level
/// and body propagation walks). Accepts the parent `CompileArtifacts`
/// so combine resolved-column maps can split per-input columns.
fn propagate_through_node(
    node: &PlanNode,
    _artifacts: &CompileArtifacts,
    set: &mut HashSet<String>,
) {
    propagate_through_node_for_body(node, set);
}

/// Operator-by-operator column-propagation rules. The per-input split
/// through Combine and predicate-aware narrowing through Route are
/// conservative here — the downstream-consumed union flows back onto
/// every incoming edge, so the buffer schema is at worst slightly
/// wider than the per-edge minimum. Wider is sound (correctness is
/// preserved); narrower lands once the dispatcher consumer makes the
/// payoff observable.
fn propagate_through_node_for_body(node: &PlanNode, set: &mut HashSet<String>) {
    match node {
        PlanNode::Transform {
            resolved,
            write_set,
            ..
        } => {
            // Remove fields the transform defines locally; they're
            // not produced by the upstream.
            for w in write_set {
                set.remove(w);
            }
            // Add fields the transform reads.
            if let Some(payload) = resolved.as_deref() {
                accumulate_program_support(&payload.typed.program, set);
            }
        }
        PlanNode::Route { .. } => {
            // Route does not produce schema columns; predicates run
            // against the upstream row. The downstream-union flows
            // upstream unchanged — predicate-specific column reads
            // could narrow the buffer further, but the wider superset
            // is sound and the narrowing only pays off once a
            // dispatcher consumer reads the buffer schema.
        }
        PlanNode::Combine { .. } => {
            // Combine merges multiple inputs into a unified row. The
            // downstream-union flows backwards onto every input edge;
            // per-input narrowing via `resolved_column_map` would
            // tighten this but only matters once a runtime consumer
            // exists.
        }
        PlanNode::Merge { .. } | PlanNode::Sort { .. } | PlanNode::CorrelationCommit { .. } => {
            // Passthrough.
        }
        PlanNode::Aggregation {
            config, compiled, ..
        } => {
            // Inner aggregate inside the deferred region: needs its
            // group_by + every binding's input field.
            for g in &config.group_by {
                set.insert(g.clone());
            }
            for binding in &compiled.bindings {
                accumulate_binding_arg(&binding.arg, set);
            }
        }
        PlanNode::Composition { .. } => {
            // Composition body internals are walked separately by the
            // body-region pass; at the parent boundary the composition
            // node itself is treated as a passthrough for the buffer
            // schema.
        }
        PlanNode::Source { .. } | PlanNode::Output { .. } => {
            // Sources and Outputs are region boundaries; never visited
            // as upstream in the propagation walk.
        }
    }
}

/// Walk every statement in a TypedProgram and accumulate every column
/// reference reached by `Expr::support_into`.
fn accumulate_program_support(program: &cxl::ast::Program, set: &mut HashSet<String>) {
    for stmt in &program.statements {
        match stmt {
            Statement::Emit { expr, .. } | Statement::Let { expr, .. } => {
                expr.support_into(set);
            }
            Statement::Filter { predicate, .. } => predicate.support_into(set),
            Statement::Trace { guard, message, .. } => {
                if let Some(g) = guard.as_deref() {
                    g.support_into(set);
                }
                message.support_into(set);
            }
            Statement::ExprStmt { expr, .. } => expr.support_into(set),
            Statement::Distinct { .. } | Statement::UseStmt { .. } => {}
        }
    }
}

/// Pull column references out of an aggregate binding argument (the
/// runtime accumulator's input expression).
fn accumulate_binding_arg(arg: &cxl::plan::BindingArg, set: &mut HashSet<String>) {
    use cxl::plan::BindingArg;
    match arg {
        BindingArg::Field(_) | BindingArg::Wildcard => {
            // Field path goes by index into the upstream schema and is
            // not exposed as a name here. The aggregate's
            // `group_by_fields` plus the typed program's emit support
            // already cover the column set; leaving this branch silent
            // is correct because the seed for `pass_b_body` (the leaf
            // case) already pulls Aggregation columns from the
            // CompiledAggregate.
        }
        BindingArg::Expr(e) => e.support_into(set),
        BindingArg::Pair(a, b) => {
            accumulate_binding_arg(a, set);
            accumulate_binding_arg(b, set);
        }
    }
}

/// Seed a node's own consumed-column set from its expressions. Used
/// by body Pass B to seed every leaf member.
fn seed_from_node(node: &PlanNode, set: &mut HashSet<String>) {
    match node {
        PlanNode::Transform { resolved, .. } => {
            if let Some(payload) = resolved.as_deref() {
                accumulate_program_support(&payload.typed.program, set);
            }
        }
        PlanNode::Aggregation {
            config, compiled, ..
        } => {
            for g in &config.group_by {
                set.insert(g.clone());
            }
            for binding in &compiled.bindings {
                accumulate_binding_arg(&binding.arg, set);
            }
        }
        _ => {}
    }
}

/// Union of column reads demanded by every parent operator that
/// consumes records flowing out of `(body, port)`. Recurses through
/// `PlanNode::Composition` continuation members via
/// `analysis.body_call_sites` so demand chains across nested body
/// boundaries; memoizes per `(body, port)` so deeply nested chains
/// converge in O(call-graph edges).
///
/// `parent_graph` is the top-level DAG (used for call sites whose
/// `scope == None`); per-body graphs are reached through
/// `artifacts.body_of(scope)` for `scope == Some(_)` call sites.
///
/// # Panics
///
/// Panics if `(body, port)` is revisited while still on the active
/// walk. Cycles are impossible by construction — `bind_schema` rejects
/// recursive composition references at E107 — so a revisit signals a
/// planner-side regression rather than a malformed user input.
//
// Caller wires up alongside the seed-threading change to pass_b_body in
// the next commit; suppression is sprint-boundary acceptable per
// CLAUDE.md (transitional state cleared before sprint close).
#[allow(dead_code)]
fn continuation_support(
    body: CompositionBodyId,
    port: &str,
    analysis: &DeferredRegionAnalysis,
    parent_graph: &DiGraph<PlanNode, PlanEdge>,
    artifacts: &CompileArtifacts,
    memo: &mut HashMap<(CompositionBodyId, String), HashSet<String>>,
    active: &mut HashSet<(CompositionBodyId, String)>,
) -> HashSet<String> {
    let key = (body, port.to_string());
    if let Some(cached) = memo.get(&key) {
        return cached.clone();
    }
    if !active.insert(key.clone()) {
        panic!(
            "continuation_support revisited ({:?}, {:?}) during active walk; \
             composition recursion is rejected at bind time (E107), so this \
             indicates a planner-side regression in body_call_sites or the \
             continuation maps",
            body, port,
        );
    }

    let mut acc: HashSet<String> = HashSet::new();
    let call_sites: &[CompositionCallSite] = analysis
        .body_call_sites
        .get(&body)
        .map(|v| v.as_slice())
        .unwrap_or(&[]);

    for call_site in call_sites {
        let cont = match call_site.scope {
            None => analysis.top_continuations.get(&call_site.composition_idx),
            Some(s) => analysis
                .body_continuations
                .get(&s)
                .and_then(|m| m.get(&call_site.composition_idx)),
        };
        let Some(cont) = cont else {
            continue;
        };

        // Resolve the graph that contains this call site's continuation
        // members. `scope=None` lives in the parent DAG; `scope=Some(s)`
        // lives in the outer body's mini-DAG.
        let containing_graph: &DiGraph<PlanNode, PlanEdge> = match call_site.scope {
            None => parent_graph,
            Some(s) => match artifacts.body_of(s) {
                Some(b) => &b.graph,
                None => continue,
            },
        };

        for &member_idx in &cont.members {
            let member_node = &containing_graph[member_idx];
            if let PlanNode::Composition { body: inner, .. } = member_node {
                if let Some(inner_body) = artifacts.body_of(*inner) {
                    let inner_ports: Vec<String> =
                        inner_body.output_port_to_node_idx.keys().cloned().collect();
                    for inner_port in inner_ports {
                        let nested = continuation_support(
                            *inner,
                            &inner_port,
                            analysis,
                            parent_graph,
                            artifacts,
                            memo,
                            active,
                        );
                        acc.extend(nested);
                    }
                }
            } else {
                seed_from_node(member_node, &mut acc);
            }
        }
    }

    active.remove(&key);
    memo.insert(key, acc.clone());
    acc
}

/// Walk incoming edges from `start` until reaching an ancestor whose
/// `stored_output_schema()` is `Some`. Skips row-preserving variants
/// (Route / Sort / CorrelationCommit) that propagate their upstream's
/// schema by reference rather than carrying their own.
fn first_schema_bearing_ancestor(
    graph: &DiGraph<PlanNode, PlanEdge>,
    start: NodeIndex,
) -> Option<&std::sync::Arc<clinker_record::Schema>> {
    let mut cursor = graph
        .neighbors_directed(start, Direction::Incoming)
        .next()?;
    loop {
        if let Some(schema) = graph[cursor].stored_output_schema() {
            return Some(schema);
        }
        match graph.neighbors_directed(cursor, Direction::Incoming).next() {
            Some(next) => cursor = next,
            None => return None,
        }
    }
}

/// Resolve the columns an Output's projection rule reads from its
/// upstream's emitted shape.
fn output_consumed_columns(
    graph: &DiGraph<PlanNode, PlanEdge>,
    out_idx: NodeIndex,
) -> HashSet<String> {
    let mut set: HashSet<String> = HashSet::new();
    // Output projection: read the resolved payload's mapping if any,
    // else fall back to every column the upstream emits. Row-
    // preserving variants (Route / Sort) carry no `stored_output_schema`
    // of their own, so walk past them to the first ancestor that
    // does. Without this walk, an Output downstream of a Route inside
    // a deferred region would seed an empty consumed set, prune every
    // column out of the producer's `buffer_schema`, and trip
    // `SchemaMismatch` when the deferred dispatcher feeds the Route's
    // pre-stamped expected schema against the narrow buffer.
    let upstream_cols: Vec<String> = first_schema_bearing_ancestor(graph, out_idx)
        .map(|schema| {
            schema
                .columns()
                .iter()
                .map(|c| c.to_string())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    if let PlanNode::Output {
        resolved: Some(payload),
        ..
    } = &graph[out_idx]
    {
        let cfg = &payload.output;
        if let Some(mapping) = cfg.mapping.as_ref() {
            for src_col in mapping.values() {
                set.insert(src_col.clone());
            }
        }
        if cfg.include_unmapped {
            for col in &upstream_cols {
                set.insert(col.clone());
            }
        }
        if let Some(exclude) = cfg.exclude.as_ref() {
            for ex in exclude {
                set.remove(ex);
            }
        }
        if !cfg.include_unmapped && cfg.mapping.is_none() {
            // No mapping declared and unmapped not included — nothing
            // gets written, but the seed must not be empty: the buffer
            // schema invariant requires at least the producer emits to
            // flow upstream. Default to upstream columns; the executor
            // would otherwise drop everything anyway.
            for col in &upstream_cols {
                set.insert(col.clone());
            }
        }
    } else {
        // Output without a resolved payload: fall back to upstream
        // columns. Should not happen in compiled DAGs but keeps the
        // walker total.
        for col in &upstream_cols {
            set.insert(col.clone());
        }
    }
    set
}
