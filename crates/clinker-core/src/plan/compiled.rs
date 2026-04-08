//! Phase 16b Wave 2/4 — `CompiledPlan` newtype boundary.
//!
//! `CompiledPlan` is the typed-handle output of the new
//! [`crate::config::PipelineConfig::compile`] lowering path. It owns the
//! lowered [`ExecutionPlanDag`], the CXL-compiled transforms, and a
//! cloned copy of the source-of-truth [`PipelineConfig`]. The executor's
//! public entry point consumes `&CompiledPlan` exclusively; internal
//! helpers still read the legacy `PipelineConfig` subset via
//! [`CompiledPlan::config`].
//!
//! Wave 4ab: executor public surface flipped from `&PipelineConfig` to
//! `&CompiledPlan`. Legacy-type deletion (`TransformConfig`,
//! `TransformEntry`, `project_nodes_to_legacy`, free-function rewrite of
//! `ExecutionPlanDag::compile`) is deferred to a follow-up; the shim path
//! via `config()` keeps the executor's internal helpers unchanged while
//! the boundary at the top is strict.

use std::collections::HashMap;

use petgraph::graph::NodeIndex;

use super::execution::ExecutionPlanDag;
use crate::config::PipelineConfig;
use crate::executor::CompiledTransform;

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

/// Phase 16b typed handle returned by
/// [`crate::config::PipelineConfig::compile`]. Wraps the lowered
/// [`ExecutionPlanDag`], the compiled CXL transforms, and a cloned
/// [`PipelineConfig`] used by executor internals.
#[derive(Debug)]
pub struct CompiledPlan {
    dag: ExecutionPlanDag,
    runtime: HashMap<NodeIndex, PlanNodeRuntime>,
    config: PipelineConfig,
    compiled_transforms: Vec<CompiledTransform>,
}

impl CompiledPlan {
    /// Construct from a fully-built DAG plus its source PipelineConfig
    /// and compiled CXL transforms. Used by the lowering path.
    pub(crate) fn new(
        dag: ExecutionPlanDag,
        config: PipelineConfig,
        compiled_transforms: Vec<CompiledTransform>,
    ) -> Self {
        Self {
            dag,
            runtime: HashMap::new(),
            config,
            compiled_transforms,
        }
    }

    /// Borrow the lowered DAG.
    pub fn dag(&self) -> &ExecutionPlanDag {
        &self.dag
    }

    /// Mutably borrow the lowered DAG (plan-internal passes only).
    #[allow(dead_code)] // Wave 4 follow-up consumer
    pub(crate) fn dag_mut(&mut self) -> &mut ExecutionPlanDag {
        &mut self.dag
    }

    /// Borrow the per-node runtime sidecar table.
    pub fn runtime(&self) -> &HashMap<NodeIndex, PlanNodeRuntime> {
        &self.runtime
    }

    /// Borrow the source-of-truth pipeline config. Used by executor
    /// internals that have not been lifted off the legacy shape yet.
    pub(crate) fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Borrow the pre-compiled CXL transforms (declaration order).
    #[allow(dead_code)] // Wave 4 follow-up consumer
    pub(crate) fn compiled_transforms(&self) -> &[CompiledTransform] {
        &self.compiled_transforms
    }
}
