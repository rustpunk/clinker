//! Phase 16b — `CompiledPlan` newtype boundary.
//!
//! `CompiledPlan` is the typed-handle output of the new
//! [`crate::config::PipelineConfig::compile`] lowering path.
//!
//! Task 16b.8 — the two-phase `bind_schema` scaffold and the dead
//! `compiled_transforms` storage field were deleted: the executor now
//! calls [`crate::executor::PipelineExecutor::compile_transforms_from_config`]
//! directly against the reader-derived schema after the readers open.
//! `CompiledPlan` is now a thin wrapper around a validated
//! `PipelineConfig` plus its lowered DAG.

use super::execution::ExecutionPlanDag;
use crate::config::PipelineConfig;

#[derive(Debug)]
pub struct CompiledPlan {
    dag: ExecutionPlanDag,
    config: PipelineConfig,
}

impl CompiledPlan {
    pub(crate) fn new(dag: ExecutionPlanDag, config: PipelineConfig) -> Self {
        Self { dag, config }
    }

    pub fn dag(&self) -> &ExecutionPlanDag {
        &self.dag
    }

    pub(crate) fn config(&self) -> &PipelineConfig {
        &self.config
    }
}
