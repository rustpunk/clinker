//! `CompiledPlan` newtype boundary.
//!
//! `CompiledPlan` is the typed-handle output of the
//! [`crate::config::PipelineConfig::compile`] lowering path. It wraps
//! a validated `PipelineConfig`, its lowered DAG, and the
//! runtime-retained slice of the compile artifacts: the bound
//! composition bodies, the statistics catalog, and the provenance
//! side-table. The transient compile scratch (per-node typed CXL
//! programs, combine metadata, the `PlanNodeId` mint counters) lives on
//! [`CompileArtifacts`] only during compilation and is dropped when the
//! plan is built — it never enters `CompiledPlan`. Tests that need to
//! inspect that scratch call [`crate::plan::bind_schema::bind_schema`]
//! directly (the same entry point `compile` consumes) and read the
//! returned [`CompileArtifacts`]; post-lowering state (deferred regions,
//! body assignments) is reachable off the slim plan via [`Self::dag`] and
//! [`Self::body_of`].
//!
//! ## Deriving a node's scoped lineage (klinx export)
//!
//! `PlanNodeId` is the sole internal identity; it carries no scope. A
//! human-readable scoped path (`Outer/Inner/name`) is *derived*, never
//! keyed: a top-level node lives in [`Self::dag`]; a composition-body
//! node lives in the graph of the [`BoundBody`] whose
//! `CompositionBodyId` indexes [`Self::composition_bodies`]. Walk those
//! graphs to find the node carrying a given `PlanNodeId`, read its
//! `name`, and prefix the scope's body name(s). No materialized
//! `PlanNodeId → (scope, name)` map exists because the only consumer is
//! the out-of-tree klinx exporter; an in-tree map would be unused.

use super::bind_schema::CompileArtifacts;
use super::composition_body::{BoundBody, CompositionBodies, CompositionBodyId};
use super::execution::ExecutionPlanDag;
use super::statistics::StatisticsCatalog;
use crate::config::PipelineConfig;
use crate::config::composition::{ProvenanceDb, SchemaProvenanceDb};
use clinker_format::SourceSchema;
use indexmap::IndexMap;

/// Content-hash identity for a compiled channel overlay.
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
    /// Bound composition bodies the executor re-enters at runtime. The
    /// top-level pipeline is NOT here — it lives on `dag` directly.
    composition_bodies: CompositionBodies,
    /// Planner-wide column-statistics catalog, seeded into the runtime
    /// accumulator and consulted by `--explain` row estimates.
    statistics: StatisticsCatalog,
    /// Provenance-tracked config values, queried by the `--field`
    /// provenance path and mutated by channel overlay application.
    provenance: ProvenanceDb,
    channel_identity: Option<ChannelIdentity>,
    /// BLAKE3 of the post-env-var-interpolated source YAML, copied
    /// from `PipelineConfig.source_hash` at compile time. Zero array
    /// for in-memory configs that did not flow through file load.
    pipeline_hash: [u8; 32],
    /// Resolved unified [`SourceSchema`] per source node, keyed by node
    /// name. Every source's `schema:` — including an external
    /// `.schema.yaml` ([`SourceSchema::File`]) — is resolved to its inline
    /// form exactly once at compile time and retained here, so the ingest
    /// path (and `patch_schema` / per-attribute provenance) reads the schema
    /// from the plan rather than re-reading the file at runtime.
    bound_schemas: IndexMap<String, SourceSchema>,
    /// Per-attribute source-schema provenance keyed by `(source, column,
    /// attribute)`. Base-seeded from `bound_schemas`; the overlay path merges
    /// its layered attribution over that seed. Queried by `explain --field
    /// <source>.<column>.<attribute>`. Excluded from `pipeline_hash`; rebuilt
    /// each compile.
    schema_provenance: SchemaProvenanceDb,
}

impl CompiledPlan {
    /// Build a slim plan from a finished compile, moving the
    /// runtime-retained fields out of `artifacts` and dropping the
    /// transient compile scratch. Called once, at the tail of
    /// [`crate::config::PipelineConfig::compile_with_diagnostics`].
    pub(crate) fn from_compile(
        dag: ExecutionPlanDag,
        config: PipelineConfig,
        bound_schemas: IndexMap<String, SourceSchema>,
        artifacts: CompileArtifacts,
    ) -> Self {
        let pipeline_hash = config.source_hash;
        let CompileArtifacts {
            composition_bodies,
            statistics,
            provenance,
            ..
        } = artifacts;
        // Base-seed the schema provenance from the resolved schemas. The overlay
        // path overwrites this with fully layered attribution via
        // `merge_schema_provenance`; a plain compile keeps this Base-only seed.
        let mut schema_provenance = SchemaProvenanceDb::default();
        for (name, schema) in &bound_schemas {
            schema_provenance.seed_base(name, schema);
        }
        Self {
            dag,
            config,
            composition_bodies,
            statistics,
            provenance,
            channel_identity: None,
            pipeline_hash,
            bound_schemas,
            schema_provenance,
        }
    }

    pub fn dag(&self) -> &ExecutionPlanDag {
        &self.dag
    }

    pub fn config(&self) -> &PipelineConfig {
        &self.config
    }

    /// Bound composition bodies the executor re-enters. The executor
    /// reads this to step into each `PipelineNode::Composition`'s
    /// mini-DAG; tooling walks it for body drill-in.
    pub fn composition_bodies(&self) -> &CompositionBodies {
        &self.composition_bodies
    }

    /// Planner-wide column-statistics catalog. Cloned into the runtime
    /// accumulator at executor entry and read by `--explain` row
    /// estimates.
    pub fn statistics(&self) -> &StatisticsCatalog {
        &self.statistics
    }

    /// Look up a composition body by its ID.
    ///
    /// Returns the `BoundBody` containing the composition's expanded nodes,
    /// bound schemas, and port rows. Used by tooling for drill-in rendering.
    pub fn body_of(&self, id: CompositionBodyId) -> Option<&BoundBody> {
        self.composition_bodies.get(&id)
    }

    /// Side-table of provenance-tracked config values for composition nodes.
    /// Populated during `bind_schema`; consumed by tooling inspectors and
    /// channel overlay.
    pub fn provenance(&self) -> &ProvenanceDb {
        &self.provenance
    }

    /// Mutable access to the provenance side-table for channel overlay
    /// application. The overlay applies `ChannelWide`/`ChannelPerTarget`
    /// layers (optionally `fixed`) to existing `ResolvedValue` entries.
    pub fn provenance_mut(&mut self) -> &mut ProvenanceDb {
        &mut self.provenance
    }

    /// The channel identity stamped by the channel/group overlay resolution
    /// (`OverlayResolution::apply_config_and_vars`), if any. `None` for base
    /// pipeline compilations (no channel applied).
    pub fn channel_identity(&self) -> Option<&ChannelIdentity> {
        self.channel_identity.as_ref()
    }

    /// Stamp a channel identity onto this compiled plan.
    pub fn set_channel_identity(&mut self, identity: ChannelIdentity) {
        self.channel_identity = Some(identity);
    }

    /// BLAKE3 of the post-env-var-interpolated source YAML.
    ///
    /// Returns all-zeroes for in-memory configs constructed without a
    /// file load (e.g. `parse_config`-driven tests).
    pub fn pipeline_hash(&self) -> &[u8; 32] {
        &self.pipeline_hash
    }

    /// Resolved unified [`SourceSchema`] per source node, keyed by node name.
    ///
    /// Every source's `schema:` is resolved to its inline form (an external
    /// `.schema.yaml` [`SourceSchema::File`] is read once, at compile time)
    /// and retained here. Consumers read the schema from the plan rather than
    /// re-reading the file at runtime.
    pub fn bound_schemas(&self) -> &IndexMap<String, SourceSchema> {
        &self.bound_schemas
    }

    /// Per-attribute source-schema provenance, queried by `explain --field
    /// <source>.<column>.<attribute>`.
    pub fn schema_provenance(&self) -> &SchemaProvenanceDb {
        &self.schema_provenance
    }

    /// Lay layered schema provenance (recorded while structural overlay ops were
    /// applied) over the Base-only seed. The argument wins per leaf, so an
    /// overlay-shaped source keeps its full layer attribution while untouched
    /// sources keep their Base seed. Called once, from the overlay compile path.
    pub(crate) fn merge_schema_provenance(&mut self, overlay: SchemaProvenanceDb) {
        self.schema_provenance.merge_over(overlay);
    }
}
