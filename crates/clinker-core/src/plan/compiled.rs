//! Phase 16b — `CompiledPlan` newtype boundary.
//!
//! `CompiledPlan` is the typed-handle output of the
//! [`crate::config::PipelineConfig::compile`] lowering path. It wraps
//! a validated `PipelineConfig`, its lowered DAG, and the
//! compile-time CXL typecheck artifacts produced by
//! [`crate::plan::bind_schema::bind_schema`].

use cxl::typecheck::Row;

use super::bind_schema::CompileArtifacts;
use super::composition_body::{BoundBody, CompositionBodyId};
use super::execution::ExecutionPlanDag;
use crate::config::PipelineConfig;
use crate::config::composition::ProvenanceDb;

/// Content-hash identity for a compiled channel overlay (LD-16c-18).
///
/// Two plans with identical `ChannelIdentity` are byte-equivalent and
/// do not need re-materialization (SQLMesh Virtual Environments model).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ChannelIdentity {
    pub name: String,
    /// BLAKE3 hash of the raw `.channel.yaml` file bytes.
    pub content_hash: [u8; 32],
}

#[derive(Debug)]
pub struct CompiledPlan {
    dag: ExecutionPlanDag,
    config: PipelineConfig,
    artifacts: CompileArtifacts,
    channel_identity: Option<ChannelIdentity>,
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
            channel_identity: None,
        }
    }

    pub fn dag(&self) -> &ExecutionPlanDag {
        &self.dag
    }

    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Compile-time CXL typecheck artifacts — one entry per
    /// CXL-bearing node (Transform/Aggregate/Route) keyed by node name.
    /// The runtime executor reads this map directly to pull each
    /// transform's `Arc<TypedProgram>` instead of re-typechecking.
    pub fn artifacts(&self) -> &CompileArtifacts {
        &self.artifacts
    }

    /// Look up a composition body by its ID.
    ///
    /// Returns the `BoundBody` containing the composition's expanded nodes,
    /// bound schemas, and port rows. Used by Kiln for drill-in rendering.
    pub fn body_of(&self, id: CompositionBodyId) -> Option<&BoundBody> {
        self.artifacts.body_of(id)
    }

    /// Look up the bound output row type for a node by name.
    ///
    /// Returns `None` for nodes that didn't participate in `bind_schema`
    /// (e.g. compositions whose binding failed).
    pub fn schema_for_node_name(&self, name: &str) -> Option<&Row> {
        self.artifacts.bound_schemas.output_of(name)
    }

    /// Side-table of provenance-tracked config values for composition nodes.
    /// Populated during `bind_schema`; consumed by the Kiln inspector and
    /// channel overlay (16c.4).
    pub fn provenance(&self) -> &ProvenanceDb {
        &self.artifacts.provenance
    }

    /// Mutable access to the provenance side-table for channel overlay
    /// application. The overlay applies `ChannelDefault`/`ChannelFixed`
    /// layers to existing `ResolvedValue` entries.
    pub fn provenance_mut(&mut self) -> &mut ProvenanceDb {
        &mut self.artifacts.provenance
    }

    /// The channel identity stamped by [`apply_channel_overlay`], if any.
    /// `None` for base pipeline compilations (no channel applied).
    pub fn channel_identity(&self) -> Option<&ChannelIdentity> {
        self.channel_identity.as_ref()
    }

    /// Stamp a channel identity onto this compiled plan.
    pub fn set_channel_identity(&mut self, identity: ChannelIdentity) {
        self.channel_identity = Some(identity);
    }
}
