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
//!   `buffer_schema` is the union, sorted alphabetically for
//!   deterministic `--explain` rendering.

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use cxl::ast::Statement;
use petgraph::Direction;
use petgraph::graph::{DiGraph, NodeIndex};

use crate::plan::bind_schema::CompileArtifacts;
use crate::plan::composition_body::BoundBody;
use crate::plan::execution::{PlanEdge, PlanNode, group_by_omits_any_ck_field};
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
pub struct DeferredRegion {
    /// `NodeIndex` of the relaxed-CK Aggregate that seeds this region.
    /// For body-internal Aggregates, this is the body's local index;
    /// the parent-level flatten in `config/mod.rs` keys regions only
    /// by parent-graph indices, so consumers inside body executors
    /// must look up by their body-local map.
    pub producer: NodeIndex,
    /// Every non-producer, non-output operator inside the region.
    pub members: HashSet<NodeIndex>,
    /// Correlation-buffered Outputs at the region's exit boundary.
    pub outputs: HashSet<NodeIndex>,
    /// Minimum set of producer-emitted columns that the deferred
    /// operators consume, sorted alphabetically.
    pub buffer_schema: Vec<String>,
}

/// Walk every relaxed-CK Aggregate in the top-level DAG (recursing
/// through composition bodies via `artifacts.composition_bodies`) and
/// return one `DeferredRegion` per producer.
///
/// Body-internal Aggregates produce regions whose indices are body-
/// local; the caller is responsible for keying them appropriately.
pub(crate) fn detect_deferred_regions(
    graph: &DiGraph<PlanNode, PlanEdge>,
    node_properties: &HashMap<NodeIndex, NodeProperties>,
    artifacts: &CompileArtifacts,
) -> Vec<DeferredRegion> {
    let mut regions = Vec::new();

    // Top-level relaxed-CK aggregates.
    for idx in graph.node_indices() {
        let PlanNode::Aggregation { config, .. } = &graph[idx] else {
            continue;
        };
        let parent_ck = parent_ck_of(graph, node_properties, idx);
        if !group_by_omits_any_ck_field(&config.group_by, &parent_ck) {
            continue;
        }
        if let Some(region) = build_region(graph, artifacts, idx) {
            regions.push(region);
        }
    }

    // Body-internal relaxed-CK aggregates. Walk every body, derive the
    // body's parent-CK at each Aggregate from the upstream stored
    // schema (mirrors `apply_retraction_flags_in_body`), and seed a
    // region inside the body's mini-DAG.
    for body in artifacts.composition_bodies.values() {
        for idx in body.graph.node_indices() {
            let PlanNode::Aggregation { config, .. } = &body.graph[idx] else {
                continue;
            };
            let parent_ck = body_parent_ck_of(body, idx);
            if !group_by_omits_any_ck_field(&config.group_by, &parent_ck) {
                continue;
            }
            if let Some(region) = build_body_region(body, idx) {
                regions.push(region);
            }
        }
    }

    regions
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
    producer: NodeIndex,
) -> Option<DeferredRegion> {
    let (members, outputs) = pass_a_forward_bfs(graph, artifacts, producer);
    let buffer_schema = pass_b_column_prune(graph, artifacts, producer, &members, &outputs);
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
    let buffer_schema = pass_b_body(body, producer, &members);
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
/// reaching the producer's outgoing edges is the buffer schema. Sorted
/// alphabetically for deterministic snapshot output.
fn pass_b_column_prune(
    graph: &DiGraph<PlanNode, PlanEdge>,
    artifacts: &CompileArtifacts,
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

    // Reverse-topo walk constrained to the region. Use a worklist:
    // pop a node, propagate its consumed set onto every upstream edge
    // (member or producer); terminate when nothing changes.
    //
    // The region is a DAG embedded in the parent DAG; per-node
    // re-queueing on every consumer-side update converges in O(|E|).
    let mut worklist: VecDeque<NodeIndex> = outputs.iter().copied().collect();
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

    // The producer's buffer schema is the union of every consumer
    // edge's set landing on it.
    let mut buffer: BTreeSet<String> = BTreeSet::new();
    if let Some(set) = consumed.get(&producer) {
        for col in set {
            buffer.insert(col.clone());
        }
    }
    buffer.into_iter().collect()
}

/// Body-graph Pass B. Simpler: bodies don't have Outputs, so the
/// region's exit boundary is every leaf member. Seed each leaf with
/// the union of `support_into` over its expressions, then propagate.
fn pass_b_body(body: &BoundBody, producer: NodeIndex, members: &HashSet<NodeIndex>) -> Vec<String> {
    let mut consumed: HashMap<NodeIndex, HashSet<String>> = HashMap::new();
    let mut worklist: VecDeque<NodeIndex> = VecDeque::new();

    // Seed every leaf member (no outgoing edges into another member or
    // producer) with the columns its own expressions read.
    for &m in members {
        let has_member_successor = body
            .graph
            .neighbors_directed(m, Direction::Outgoing)
            .any(|s| members.contains(&s));
        if has_member_successor {
            continue;
        }
        let mut seed: HashSet<String> = HashSet::new();
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

    let mut buffer: BTreeSet<String> = BTreeSet::new();
    if let Some(set) = consumed.get(&producer) {
        for col in set {
            buffer.insert(col.clone());
        }
    }
    buffer.into_iter().collect()
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

/// Resolve the columns an Output's projection rule reads from its
/// upstream's emitted shape.
fn output_consumed_columns(
    graph: &DiGraph<PlanNode, PlanEdge>,
    out_idx: NodeIndex,
) -> HashSet<String> {
    let mut set: HashSet<String> = HashSet::new();
    // Output projection: read the resolved payload's mapping if any,
    // else fall back to every column the upstream emits. The Output's
    // upstream is the immediate predecessor on the parent graph.
    let upstream = graph
        .neighbors_directed(out_idx, Direction::Incoming)
        .next();
    let upstream_cols: Vec<String> = upstream
        .and_then(|u| graph[u].stored_output_schema())
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
