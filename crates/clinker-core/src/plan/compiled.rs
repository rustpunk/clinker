//! Phase 16b — `CompiledPlan` newtype boundary.
//!
//! `CompiledPlan` is the typed-handle output of the
//! [`crate::config::PipelineConfig::compile`] lowering path. It wraps
//! a validated `PipelineConfig`, its lowered DAG, and the
//! compile-time CXL typecheck artifacts produced by
//! [`crate::config::cxl_compile::run`].

use super::execution::ExecutionPlanDag;
use crate::config::PipelineConfig;
use crate::config::cxl_compile::CompileArtifacts;

#[derive(Debug)]
pub struct CompiledPlan {
    dag: ExecutionPlanDag,
    config: PipelineConfig,
    artifacts: CompileArtifacts,
}

impl CompiledPlan {
    pub(crate) fn new(
        dag: ExecutionPlanDag,
        config: PipelineConfig,
        artifacts: CompileArtifacts,
    ) -> Self {
        Self {
            dag,
            config,
            artifacts,
        }
    }

    pub fn dag(&self) -> &ExecutionPlanDag {
        &self.dag
    }

    pub(crate) fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Compile-time CXL typecheck artifacts — one entry per
    /// CXL-bearing node (Transform/Aggregate/Route) keyed by node name.
    /// The runtime executor reads this map directly to pull each
    /// transform's `Arc<TypedProgram>` instead of re-typechecking.
    pub(crate) fn artifacts(&self) -> &CompileArtifacts {
        &self.artifacts
    }
}
