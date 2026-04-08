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
//! `&CompiledPlan`. Legacy-type deletion (`LegacyTransformsBlock` and the
//! free-function rewrite of `ExecutionPlanDag::compile`) is deferred;
//! the shim path via `config()` keeps the executor's internal helpers
//! unchanged while the boundary at the top is strict.

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

    /// Wave 4ab test/bench helper: package a bare `PipelineConfig` into
    /// a `CompiledPlan` without running the lowering pipeline. The
    /// executor's internal helpers rebuild the DAG from `config()` at
    /// run time, so this is semantically equivalent to the pre-4ab
    /// direct `&PipelineConfig` executor entry point.
    pub fn from_config_for_run(config: PipelineConfig) -> Self {
        Self {
            dag: ExecutionPlanDag {
                graph: petgraph::graph::DiGraph::new(),
                topo_order: Vec::new(),
                source_dag: Vec::new(),
                indices_to_build: Vec::new(),
                output_projections: Vec::new(),
                parallelism: crate::plan::execution::ParallelismProfile {
                    per_transform: Vec::new(),
                    worker_threads: 4,
                },
                correlation_sort_note: None,
                node_properties: HashMap::new(),
            },
            runtime: HashMap::new(),
            config,
            compiled_transforms: Vec::new(),
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

    /// Move the compiled CXL transforms out of the plan. Used by the
    /// M5 executor entry point which binds schema on a locally-owned
    /// plan, then takes ownership of the resulting transforms for the
    /// downstream evaluator build.
    pub(crate) fn take_compiled_transforms(&mut self) -> Vec<CompiledTransform> {
        std::mem::take(&mut self.compiled_transforms)
    }

    /// Phase 16b Wave 4ab M4 — two-phase CXL typecheck hook.
    ///
    /// `PipelineConfig::compile()` builds topology without CXL
    /// typechecking because schema is a runtime artifact (reader-derived).
    /// The executor calls this method after opening readers to populate
    /// `compiled_transforms` and fill `PlanTransformPayload::typed` on
    /// every Transform node in the DAG.
    ///
    /// Idempotent: re-binding replaces prior state.
    pub(crate) fn bind_schema(
        &mut self,
        schema: &std::sync::Arc<clinker_record::Schema>,
    ) -> Result<(), crate::error::PipelineError> {
        let transforms_owned = self.config.transforms();
        let transforms: Vec<&crate::config::LegacyTransformsBlock> =
            transforms_owned.iter().collect();
        let compiled = crate::executor::PipelineExecutor::compile_transforms(&transforms, schema)?;

        // Fill PlanTransformPayload::typed on every Transform node whose
        // name matches a compiled transform. The DAG may be empty (for
        // `from_config_for_run` bridge callers), in which case this loop
        // is a no-op and only `compiled_transforms` is populated.
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
