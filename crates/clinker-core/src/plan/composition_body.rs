// Bound composition body types — populated by bind_schema when it
// recurses into a PipelineNode::Composition's body (16c.2).

use std::path::PathBuf;

use cxl::typecheck::Row;
use indexmap::IndexMap;

use super::bound_schemas::BoundSchemas;
use super::execution::PlanNode;

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

/// A bound composition body — one nested scope with its own NodeId
/// space, its own BoundSchemas, and its own nested bodies (for
/// composition-of-composition recursion).
#[derive(Debug, Clone)]
pub struct BoundBody {
    /// The composition signature this body was bound against.
    /// Workspace-relative path used as the `CompositionSymbolTable` key.
    pub signature_path: PathBuf,

    /// Body nodes in their own NodeId space (starting from 0 inside
    /// this scope). NodeIds here do NOT collide with NodeIds in the
    /// parent scope or in sibling bodies.
    pub nodes: Vec<PlanNode>,

    /// Per-node row types inside this body scope.
    pub bound_schemas: BoundSchemas,

    /// Output row types at the body's declared output ports, keyed
    /// by port name. These are what the parent scope sees as the
    /// composition node's output row(s).
    pub output_port_rows: IndexMap<String, Row>,

    /// Input row types at the body's declared input ports, with
    /// `tail: Open(fresh_tail)` — the row variables bound here are
    /// what carry pass-through columns through the body.
    pub input_port_rows: IndexMap<String, Row>,

    /// Nested composition bodies (composition-of-composition). Keyed
    /// by CompositionBodyId assigned from the parent
    /// CompileArtifacts counter.
    pub nested_body_ids: Vec<CompositionBodyId>,
}
