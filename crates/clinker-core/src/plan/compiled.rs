//! Phase 16b — `CompiledPlan` newtype boundary.
//!
//! `CompiledPlan` is the typed-handle output of the
//! [`crate::config::PipelineConfig::compile`] lowering path. A thin
//! wrapper around a validated `PipelineConfig` plus its lowered DAG.

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
