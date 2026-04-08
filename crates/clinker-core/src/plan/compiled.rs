//! Phase 16b Wave 2 — `CompiledPlan` newtype boundary.
//!
//! `CompiledPlan` is the typed-handle output of the new
//! [`crate::config::PipelineConfig::compile`] lowering path. It exists as a
//! private wrapper around an [`ExecutionPlanDag`] (plus a sidecar runtime
//! table) so that the plan→executor boundary is enforced at the type
//! level: the new compile path returns `CompiledPlan` exclusively, and
//! Wave 3 will flip the executor signature to consume `&CompiledPlan`
//! instead of `&PipelineConfig`.
//!
//! For Wave 2 the executor signature is unchanged — `CompiledPlan`
//! co-exists with the legacy [`ExecutionPlanDag::compile`] path for the
//! benefit of fixture-driven tests that have not yet migrated to the
//! unified `nodes:` shape.
//!
//! Architectural decisions:
//! - **Single fully-lowered IR.** `CompiledPlan` owns one DAG; there is
//!   no separate physical/logical split (DataFusion's two-IR pattern was
//!   considered and rejected for the Phase 16b scope; the
//!   [`PlanNodeRuntime`] sidecar exists as the deferral hook should we
//!   need it later).
//! - **Newtype, not type alias.** Forces callers to unwrap explicitly.
//! - **Sidecar runtime table.** [`PlanNodeRuntime`] is keyed by
//!   `petgraph::graph::NodeIndex` so per-node runtime state can grow
//!   without bloating the [`super::execution::PlanNode`] enum.

use std::collections::HashMap;

use petgraph::graph::NodeIndex;

use super::execution::ExecutionPlanDag;

/// Per-node runtime sidecar payload.
///
/// Wave 2 leaves this empty — the table is wired up so that future
/// passes (e.g. memory budgeting, parallelism heuristics, runtime
/// metrics handles) can add per-node state without churning the
/// [`super::execution::PlanNode`] enum.
#[derive(Debug, Default, Clone)]
pub struct PlanNodeRuntime {
    /// Reserved for Wave 3+. Empty payload shell — see module docs.
    _reserved: (),
}

/// Phase 16b Wave 2 typed handle returned by
/// [`crate::config::PipelineConfig::compile`]. Wraps the lowered
/// [`ExecutionPlanDag`] plus a sidecar runtime table.
#[derive(Debug)]
pub struct CompiledPlan {
    dag: ExecutionPlanDag,
    runtime: HashMap<NodeIndex, PlanNodeRuntime>,
}

impl CompiledPlan {
    /// Construct from a fully-built DAG. Used by the lowering path.
    pub(crate) fn new(dag: ExecutionPlanDag) -> Self {
        Self {
            dag,
            runtime: HashMap::new(),
        }
    }

    /// Borrow the lowered DAG.
    pub fn dag(&self) -> &ExecutionPlanDag {
        &self.dag
    }

    /// Borrow the per-node runtime sidecar table.
    pub fn runtime(&self) -> &HashMap<NodeIndex, PlanNodeRuntime> {
        &self.runtime
    }
}
