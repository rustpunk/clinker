//! Phase 16b — `CompiledPlan` newtype boundary.
//!
//! `CompiledPlan` is the typed-handle output of the
//! [`crate::config::PipelineConfig::compile`] lowering path. It wraps
//! a validated `PipelineConfig`, its lowered DAG, and the
//! compile-time CXL typecheck artifacts produced by
//! [`crate::plan::bind_schema::bind_schema`].

use cxl::typecheck::Row;

use super::bind_schema::CompileArtifacts;
use super::execution::ExecutionPlanDag;
use crate::config::PipelineConfig;

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

    /// Look up the bound output row type for a node by name.
    ///
    /// Returns `None` for nodes that didn't participate in `bind_schema`
    /// (e.g. compositions whose binding failed).
    pub fn schema_for_node_name(&self, name: &str) -> Option<&Row> {
        self.artifacts.bound_schemas.output_of(name)
    }
}
