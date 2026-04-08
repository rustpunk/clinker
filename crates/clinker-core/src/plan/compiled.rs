//! Phase 16b — `CompiledPlan` newtype boundary.
//!
//! `CompiledPlan` is the typed-handle output of the new
//! [`crate::config::PipelineConfig::compile`] lowering path.

use super::execution::ExecutionPlanDag;
use crate::config::PipelineConfig;
use crate::executor::CompiledTransform;

#[derive(Debug)]
pub struct CompiledPlan {
    dag: ExecutionPlanDag,
    config: PipelineConfig,
    compiled_transforms: Vec<CompiledTransform>,
}

impl CompiledPlan {
    pub(crate) fn new(
        dag: ExecutionPlanDag,
        config: PipelineConfig,
        compiled_transforms: Vec<CompiledTransform>,
    ) -> Self {
        Self {
            dag,
            config,
            compiled_transforms,
        }
    }

    pub fn dag(&self) -> &ExecutionPlanDag {
        &self.dag
    }

    #[allow(dead_code)]
    pub(crate) fn dag_mut(&mut self) -> &mut ExecutionPlanDag {
        &mut self.dag
    }

    pub(crate) fn config(&self) -> &PipelineConfig {
        &self.config
    }

    #[allow(dead_code)]
    pub(crate) fn compiled_transforms(&self) -> &[CompiledTransform] {
        &self.compiled_transforms
    }

    pub(crate) fn take_compiled_transforms(&mut self) -> Vec<CompiledTransform> {
        std::mem::take(&mut self.compiled_transforms)
    }

    /// Two-phase CXL typecheck hook. Called by the executor after opening
    /// readers so reader-derived schema is available for CXL typechecking.
    pub(crate) fn bind_schema(
        &mut self,
        schema: &std::sync::Arc<clinker_record::Schema>,
    ) -> Result<(), crate::error::PipelineError> {
        let compiled = crate::executor::PipelineExecutor::compile_transforms_from_config(
            &self.config,
            schema,
        )?;

        {
            use crate::plan::execution::PlanNode;
            let by_name: std::collections::HashMap<&str, &CompiledTransform> =
                compiled.iter().map(|ct| (ct.name.as_str(), ct)).collect();
            for idx in self.dag.graph.node_indices().collect::<Vec<_>>() {
                if let PlanNode::Transform { name, resolved, .. } = &mut self.dag.graph[idx]
                    && let Some(payload) = resolved.as_mut()
                    && let Some(ct) = by_name.get(name.as_str())
                {
                    payload.typed = Some(std::sync::Arc::clone(&ct.typed));
                }
            }
        }

        self.compiled_transforms = compiled;
        Ok(())
    }
}
