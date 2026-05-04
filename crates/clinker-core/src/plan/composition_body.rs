// Bound composition body types — populated by bind_schema when it
// recurses into a PipelineNode::Composition's body.

use std::collections::HashMap;
use std::path::PathBuf;

use cxl::typecheck::Row;
use indexmap::IndexMap;
use petgraph::graph::{DiGraph, NodeIndex};

use super::deferred_region::{DeferredRegion, ParentContinuation};
use super::execution::{PlanEdge, PlanNode};

/// Opaque handle into `CompileArtifacts.composition_bodies`. Each
/// `PipelineNode::Composition` in `CompiledPlan` carries one of these
/// pointing at its bound body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub struct CompositionBodyId(pub u32);

impl CompositionBodyId {
    /// Sentinel value used as serde-default before `bind_composition`
    /// runs. Any consumer reading this before bind_schema completes
    /// gets a clear debug-panic. ID 0 is a legitimate fresh body, so
    /// the sentinel must be `u32::MAX`.
    pub const SENTINEL: Self = Self(u32::MAX);
}

impl Default for CompositionBodyId {
    fn default() -> Self {
        Self::SENTINEL
    }
}

/// A bound composition body — one nested scope with its own NodeIndex
/// space inside `graph`, its own per-node row map, and its own nested
/// bodies (for composition-of-composition recursion).
///
/// The body is a fully-formed mini-DAG: `graph` holds the lowered
/// `PlanNode`s with their edges resolved from the body file's `input:`
/// references; `topo_order` is the executor walk order; `name_to_idx`
/// resolves body-internal node names; `port_name_to_node_idx` resolves
/// the signature's input port names to the body node(s) consuming them.
/// The two name tables are intentionally distinct — a port name can
/// legally collide with a body-internal node name because each lookup
/// is consulted in a separate context (port seeding consults the port
/// table; body-internal references consult the node table). NiFi uses
/// the same split between `findInputPort` and `findProcessor`; when
/// those tables were collapsed in older revisions, internal IDs
/// collided across nested process groups and the resolver picked
/// wrong, so the right fix is keeping the namespaces apart.
#[derive(Debug, Clone)]
pub struct BoundBody {
    /// The composition signature this body was bound against.
    /// Workspace-relative path used as the `CompositionSymbolTable` key.
    pub signature_path: PathBuf,

    /// Body's mini-DAG of lowered `PlanNode`s. NodeIndices here live
    /// in their own space — they do not collide with NodeIndices in
    /// the parent scope or in sibling bodies.
    pub graph: DiGraph<PlanNode, PlanEdge>,

    /// Body nodes in topological order. Walked by the body executor
    /// in the same shape as the top-level walker walks
    /// `ExecutionPlanDag.topo_order`.
    pub topo_order: Vec<NodeIndex>,

    /// Body-internal node-name → NodeIndex. Used to wire edges during
    /// bind_composition and (later) to look up the body's terminal
    /// output node by name.
    pub name_to_idx: HashMap<String, NodeIndex>,

    /// Signature input-port-name → NodeIndex of the body node that
    /// references the port as its input. The body executor seeds
    /// records into this NodeIndex's buffer at composition entry.
    /// Distinct from `name_to_idx` because a port name and a
    /// body-internal node name can legally collide; each lookup
    /// happens in a separate context (port seeding vs
    /// body-internal reference resolution).
    pub port_name_to_node_idx: HashMap<String, NodeIndex>,

    /// Per-node output row inside this body scope. Keyed by node name.
    /// Consumers that want a public accessor go through
    /// `CompiledPlan::typed_output_row`, which only sees top-level
    /// names; body-scope rows are Kiln-drill-in state.
    pub body_rows: HashMap<String, Row>,

    /// Per-body-node original `input:` references in declaration
    /// order. Keyed by consumer node name; values are the raw
    /// reference strings (port-qualified for Route branches:
    /// `route_name.branch_name`). The body Route arm uses this to
    /// resolve `<route>.<branch>` references to body successor
    /// NodeIndices — equivalent to the top-level walker reading
    /// `ctx.config.nodes`, but scoped to a body. Empty for nodes
    /// without inputs (Sources).
    pub node_input_refs: HashMap<String, Vec<String>>,

    /// Compiled `where:` predicates for body Route nodes, keyed by
    /// route node name. The body Route arm consults this map to
    /// evaluate per-record routing — without it, body Routes would
    /// silently inherit the top-level Route's conditions and
    /// mis-route every record. Empty for bodies with no Routes.
    /// Stored as `RouteBody` (the parsed source-of-truth) rather
    /// than a pre-compiled evaluator because compilation has to
    /// happen at executor entry alongside the top-level route to
    /// pick up the right `emitted_fields` set.
    pub route_bodies: HashMap<String, crate::config::pipeline_node::RouteBody>,

    /// Output row types at the body's declared output ports, keyed
    /// by port name. These are what the parent scope sees as the
    /// composition node's output row(s).
    pub output_port_rows: IndexMap<String, Row>,

    /// Per-output-port: the body NodeIndex whose buffer carries the
    /// records the port surfaces back to the parent scope. Resolved
    /// at bind time from `signature.outputs[port].internal_ref` —
    /// the executor consults this to harvest body records at end of
    /// the body walk. Distinct from `output_port_rows` (which carries
    /// the typed Row): the row drives schema propagation; this map
    /// drives runtime record movement.
    pub output_port_to_node_idx: IndexMap<String, NodeIndex>,

    /// Input row types at the body's declared input ports, with
    /// `tail: Open(fresh_tail)` — the row variables bound here are
    /// what carry pass-through columns through the body.
    pub input_port_rows: IndexMap<String, Row>,

    /// Nested composition bodies (composition-of-composition). Keyed
    /// by CompositionBodyId assigned from the parent
    /// CompileArtifacts counter.
    pub nested_body_ids: Vec<CompositionBodyId>,

    /// Body-scope analytic-window IndexSpecs. Populated by the post-
    /// parent-DAG-build pass that runs after `bind_composition` —
    /// body lowering happens before the parent DAG's NodeIndex space
    /// is allocated, so a body window whose first non-pass-through
    /// ancestor is the body's `input:` port (which resolves through to
    /// a parent-DAG operator) needs the parent's `name_to_idx` map
    /// to emit the right `PlanIndexRoot::ParentNode { upstream, .. }`.
    /// The body executor sizes its `window_runtime.bodies[body_id]`
    /// vec to this list at recursion entry. Empty for bodies whose
    /// transforms declare no `analytic_window:` config.
    pub body_indices_to_build: Vec<crate::plan::index::IndexSpec>,

    /// Per-Transform analytic-window configs captured at
    /// `bind_composition` time. Keyed by the body Transform's name;
    /// values are the typed `LocalWindowConfig` parsed from the
    /// body file's `analytic_window:` block. Empty entries are
    /// omitted. Consumed by the post-parent-DAG-build body-window
    /// pass to construct `body_indices_to_build` and backfill
    /// `window_index` onto each body Transform.
    pub body_window_configs: HashMap<String, crate::plan::index::LocalWindowConfig>,

    /// Deferred-region metadata for relaxed-CK Aggregates that live
    /// inside this body. Keys are NodeIndex values in this body's
    /// local mini-DAG (`graph`), distinct from
    /// `ExecutionPlanDag.deferred_regions` whose keys live in the
    /// parent graph's NodeIndex space — the two spaces collide
    /// numerically because each graph numbers from zero, so the
    /// dispatcher consults the right map by which scope it is
    /// currently executing.
    ///
    /// Every NodeIndex participating in a body-internal region
    /// (producer + members + outputs) is keyed to a clone of the
    /// region so dispatcher arms get O(1) lookup. Empty for bodies
    /// without a relaxed-CK Aggregate.
    pub(crate) deferred_regions: HashMap<NodeIndex, DeferredRegion>,

    /// Parent-continuation metadata for nested Composition nodes whose
    /// inner body (transitively) carries a deferred region. Keyed by
    /// the outer-body NodeIndex of the nested Composition node;
    /// cloned into the transient body DAG by
    /// [`crate::plan::execution::ExecutionPlanDag::from_body`] so the
    /// commit-time dispatcher walking the outer body sees the same
    /// continuation surface as the parent dispatcher does. Empty for
    /// bodies that contain no nested Composition with a deferred
    /// region somewhere below.
    pub(crate) parent_continuations: HashMap<NodeIndex, ParentContinuation>,
}

impl BoundBody {
    /// Empty body keyed by `signature_path`. Used by the artifact-
    /// registry tests (and out-of-crate fixtures) that need a `BoundBody`
    /// shell without populating its mini-DAG; the bind path constructs a
    /// fully-populated body via `bind_composition` and never calls this.
    pub fn empty(signature_path: PathBuf) -> Self {
        Self {
            signature_path,
            graph: DiGraph::new(),
            topo_order: Vec::new(),
            name_to_idx: HashMap::new(),
            port_name_to_node_idx: HashMap::new(),
            body_rows: HashMap::new(),
            node_input_refs: HashMap::new(),
            route_bodies: HashMap::new(),
            output_port_rows: IndexMap::new(),
            output_port_to_node_idx: IndexMap::new(),
            input_port_rows: IndexMap::new(),
            nested_body_ids: Vec::new(),
            body_indices_to_build: Vec::new(),
            body_window_configs: HashMap::new(),
            deferred_regions: HashMap::new(),
            parent_continuations: HashMap::new(),
        }
    }
}

/// True when `body` carries a deferred region directly OR any nested
/// composition body reachable through its mini-DAG does.
///
/// Plan-time and commit-time gates both consult this: the
/// orchestrator's `is_relaxed_pipeline` check and the dispatcher's
/// composition-recursion gate must agree on whether a body warrants
/// the relaxed path. The plan-time continuation walker uses the same
/// predicate to decide whether a given Composition node has a
/// continuation worth recording. Centralized here so the three sites
/// stay synchronized; a divergent answer between plan and runtime
/// would silently strand commit-time work.
pub(crate) fn body_or_descendants_have_deferred_region(
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
    body: &BoundBody,
) -> bool {
    if !body.deferred_regions.is_empty() {
        return true;
    }
    body.graph.node_indices().any(|idx| {
        let PlanNode::Composition { body: nested, .. } = &body.graph[idx] else {
            return false;
        };
        artifacts
            .body_of(*nested)
            .is_some_and(|b| body_or_descendants_have_deferred_region(artifacts, b))
    })
}
