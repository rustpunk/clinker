//! Execution plan emission.
//!
//! Compiles a `PipelineConfig` + CXL programs into an `ExecutionPlanDag`
//! that orchestrates the pipeline via a petgraph DAG.

// Decomposed into focused submodules; this hub holds the shared type layer
// (PlanNode, PlanEdge, ExecutionPlanDag) and re-exports each submodule's
// surface so `crate::plan::execution::*` paths resolve unchanged.

mod composition;
mod dag;
mod enforcer;
mod explain;
mod graph_util;
mod scheduling;
mod streaming_class;

pub use composition::*;
pub use dag::*;
pub use enforcer::*;
pub use explain::*;
pub(crate) use graph_util::resolve_envelope_header_upstreams_in_graph;
pub use graph_util::{
    compute_init_phase_node_set, resolve_envelope_header_upstreams, single_predecessor,
};
pub use streaming_class::{
    StreamClass, certify_streaming_edge, classify_stream_nodes,
    compute_merge_interleave_fused_sources, compute_streaming_aggregate_ingest_edges,
    compute_streaming_combine_probe_edges, compute_transform_fused_sources,
};

use std::collections::{BTreeSet, HashMap};

use petgraph::graph::{DiGraph, NodeIndex};
use serde::Serialize;

use std::sync::Arc;

use crate::config::{AggregateConfig, OutputConfig, RouteMode, SortField, SourceConfig};
use crate::plan::composition_body::CompositionBodyId;
use crate::plan::index::{AnalyticWindowSpec, IndexSpec};
use crate::plan::row_type::QualifiedField;
use crate::plan::types::{AggregateStrategy, JoinSide};
use clinker_core_types::span::Span;
use clinker_record::Schema;
use cxl::plan::CompiledAggregate;

use cxl::typecheck::pass::TypedProgram;

fn is_false(b: &bool) -> bool {
    !*b
}

/// Compact tag for a `CombineStrategy` variant, for `display_name()` /
/// `[combine:<tag>]` rendering. Pattern-matches `AggregateStrategy`'s
/// `hash` / `streaming` tag convention so both operators read alike in
/// `--explain` output.
pub(super) fn combine_strategy_tag(s: &crate::plan::combine::CombineStrategy) -> &'static str {
    use crate::plan::combine::CombineStrategy;
    match s {
        CombineStrategy::HashBuildProbe => "hash_build_probe",
        CombineStrategy::InMemoryHash => "in_memory_hash",
        CombineStrategy::HashPartitionIEJoin { .. } => "hash_partition_iejoin",
        CombineStrategy::IEJoin => "iejoin",
        CombineStrategy::SortMerge => "sort_merge",
        CombineStrategy::GraceHash { .. } => "grace_hash",
        CombineStrategy::BlockNestedLoop => "block_nested_loop",
    }
}

/// Per-node execution strategy — replaces global ExecutionMode.
///
/// Each transform declares what it needs from the executor.
/// The topo-walk applies the appropriate materialization at each node.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeExecutionReqs {
    /// Single-pass streaming — no arena, no sort.
    Streaming,
    /// Window functions present — build arena + indices first.
    RequiresArena,
    /// Correlation key — sorted input for group-boundary detection.
    RequiresSortedInput { sort_fields: Vec<SortField> },
}

/// Node in the execution plan DAG.
///
/// Self-contained — owns all display, topology, and execution-time data.
/// The DAG is the single source of truth for pipeline structure.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PlanNode {
    Source {
        name: String,
        /// Source-span of this node in the originating YAML document.
        /// `Span::SYNTHETIC` for planner-synthesized nodes and for nodes
        /// produced by the legacy `transformations:` planner path that
        /// has no span info to plumb through.
        #[serde(skip)]
        span: Span,
        /// Full resolved Source payload. Populated by
        /// `PipelineConfig::compile()` from the unified `nodes:` taxonomy.
        /// Boxed to keep the variant small.
        #[serde(skip)]
        resolved: Option<Box<PlanSourcePayload>>,
        /// Declared output schema. Populated by `bind_schema` from the
        /// source's author-declared `schema:` block; every record emitted
        /// by this source carries this exact `Arc` so downstream
        /// `Arc::ptr_eq` schema checks hit the fast path.
        #[serde(skip)]
        output_schema: Arc<Schema>,
    },
    Transform {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Full resolved Transform payload.
        #[serde(skip)]
        resolved: Option<Box<PlanTransformPayload>>,
        parallelism_class: ParallelismClass,
        tier: u32,
        execution_reqs: NodeExecutionReqs,
        /// Index into `ExecutionPlanDag.indices_to_build`, if this transform uses windows.
        #[serde(skip_serializing_if = "Option::is_none")]
        window_index: Option<usize>,
        /// How to look up the partition for this transform's window.
        #[serde(skip)]
        partition_lookup: Option<PartitionLookupKind>,
        /// Field names this transform writes (assigns to via `emit name = ...`).
        /// Populated at compile time from the CXL `TypedProgram`. Single source
        /// of truth for `compute_node_properties`'s
        /// `DestroyedByTransformWriteSet` rule — the property pass reads this
        /// directly off the node, no executor coupling.
        #[serde(skip_serializing_if = "BTreeSet::is_empty")]
        write_set: BTreeSet<String>,
        /// True iff the CXL transform contains a `distinct` statement. Populated
        /// at compile time alongside `write_set`. Consumed by
        /// `compute_node_properties` to emit `DestroyedByDistinct` for the
        /// downstream stream's ordering provenance.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        has_distinct: bool,
        /// Widened post-emit schema. Populated by `bind_schema` from the
        /// typechecked `TypedProgram`; includes every `emit`-written field
        /// on top of the upstream's schema so `Record::set` at emit sites
        /// always hits a known slot.
        #[serde(skip)]
        output_schema: Arc<Schema>,
    },
    Route {
        name: String,
        #[serde(skip)]
        span: Span,
        mode: RouteMode,
        branches: Vec<String>,
        default: String,
    },
    Merge {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Canonical output schema adopted from `input[0]`. All Merge inputs
        /// are structurally equal per the Merge contract (validated in
        /// `bind_schema`); picking one canonical `Arc` lets Merge emit every
        /// record via `Arc::clone(&output_schema)` so downstream operators
        /// always hit the `Arc::ptr_eq` fast path instead of structural
        /// fallback on input-switches.
        #[serde(skip)]
        output_schema: Arc<Schema>,
    },
    Output {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Full resolved Output payload.
        #[serde(skip)]
        resolved: Option<Box<PlanOutputPayload>>,
    },
    /// Per-correlation-group synthesis and trigger-row mutation.
    ///
    /// Blocking grouping operator: buffers each `partition_by` group,
    /// observes it, then applies each rule's trigger-row mutation and
    /// row synthesis. Single output. The executor compiles each rule's
    /// `when` / `set` / `overrides` CXL against the live input schema at
    /// dispatch time (mirroring the Route condition-compile seam), so the
    /// plan node carries only the parsed `config` and the widened output
    /// schema.
    Reshape {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Parsed Reshape configuration (`partition_by` / `order_by` /
        /// `rules`). Consumed by the executor's reshape dispatch arm.
        config: crate::config::pipeline_node::ReshapeBody,
        /// Widened output schema: the upstream columns plus the three
        /// `$meta.*` audit columns Reshape stamps. Populated by
        /// `bind_schema`.
        #[serde(skip)]
        output_schema: Arc<Schema>,
    },
    /// Per-correlation-group record removal with a side-output port.
    ///
    /// Blocking grouping operator: buffers each `partition_by` group,
    /// evaluates each rule's group-level `drop_group_when` predicate over
    /// the whole group, and routes every record of a dropped group to the
    /// `removed_to` producer-side output port while untriggered groups flow
    /// to the main output port. Cull does not widen — both ports carry the
    /// unchanged upstream schema. The executor compiles each rule's
    /// predicate against the live input schema at dispatch time (the Route
    /// condition-compile seam), so the plan node carries only the parsed
    /// `config` and the unchanged output schema.
    Cull {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Parsed Cull configuration (`partition_by` / `order_by` /
        /// `rules` / `removed_to`). Consumed by the executor's cull
        /// dispatch arm.
        config: crate::config::pipeline_node::CullBody,
        /// Output schema, equal to the upstream schema (Cull does not
        /// widen). Both the main and `removed_to` ports carry it.
        /// Populated by `bind_schema`.
        #[serde(skip)]
        output_schema: Arc<Schema>,
    },
    /// Frames a body stream into documents. Single-input, single-output.
    ///
    /// `Preserve` is a transparent framing stage: every body record flows
    /// through with its document context and grain unchanged and the
    /// document-boundary punctuations are forwarded verbatim, so a downstream
    /// Output frames byte-identically to today's per-document framing.
    /// `Concat` consolidates: it drains the whole body, folds the per-document
    /// headers to one, re-stamps every record onto a single consolidated
    /// document context, and re-frames with one open/close pair — so a
    /// multi-document body writes as one document. Neither strategy widens:
    /// the output schema is the body input's schema.
    ///
    /// A wired `header:` port replaces each body grain's ambient envelope with a
    /// matched header record carried on the same grain (transform-in-place header
    /// replacement). `header_input` names that port's producer (empty when no
    /// header is wired); `header_upstream` is its resolved predecessor
    /// `NodeIndex`, so the executor distinguishes the header predecessor from the
    /// body predecessor without re-deriving from input names. A wired `trailer:`
    /// port stays rejected at plan validation this release.
    Envelope {
        name: String,
        #[serde(skip)]
        span: Span,
        strategy: crate::config::pipeline_node::EnvelopeStrategy,
        /// Producer name of the wired `header:` port (with any `.port` suffix
        /// retained as authored). Empty string when no header is wired, in which
        /// case the node frames with each body grain's own ambient envelope.
        /// Populated at lowering from the node's `header:` reference.
        header_input: String,
        /// `NodeIndex` of the wired header predecessor, resolved from
        /// `header_input` against the Envelope's incoming neighbors during the
        /// `resolve_envelope_header_upstreams` post-pass (mirroring Combine's
        /// `driving_upstream` resolution, including the correlation-sort-prefix
        /// alias). `None` when no header is wired or before the post-pass runs;
        /// the executor reads it to drain the header stream separately from the
        /// body stream.
        #[serde(skip)]
        header_upstream: Option<petgraph::graph::NodeIndex>,
        /// Output schema, adopted verbatim from the body input (Envelope does
        /// not widen). Populated by `bind_schema`.
        #[serde(skip)]
        output_schema: Arc<Schema>,
        /// Compiled declarative header/footer synthesis. `Some` iff the node
        /// declared a `config.header:` or `config.footer:` map; the executor
        /// then stamps the synthesized sections into each output document's
        /// `EnvelopeRecord`. `None` when neither is declared. Holds non-serde
        /// CXL artifacts (`Expr` / `TypedProgram` / `CompiledAggregate`), so it
        /// is reconstructed at compile and never shipped through `--explain`.
        #[serde(skip)]
        synthesis: Option<Arc<crate::plan::envelope_synthesis::EnvelopeSynthesis>>,
    },
    /// Planner-synthesized sort enforcer.
    ///
    /// Inserted by `ExecutionPlanDag::insert_enforcer_sorts` on edges feeding
    /// transforms with `RequiresSortedInput` whose upstream `Source` does not
    /// already satisfy the requirement (per `source_ordering_satisfies`).
    ///
    /// Distinct from arena-local `sort_partition` (window-local, never lifted
    /// into the DAG) and user-declared Source/Output sorts. The variant name
    /// is reserved with the prefix `__sort_for_{consumer}`; user-declared node
    /// names starting with `__sort_for_` are rejected at compile time.
    Sort {
        name: String,
        #[serde(skip)]
        span: Span,
        sort_fields: Vec<SortField>,
    },
    /// Hash / streaming GROUP BY transform.
    ///
    /// Constructed by `ExecutionPlanDag::compile()` when a `TransformSpec`
    /// has its `aggregate` block set. The plan-time extraction artifact
    /// (`compiled`) and the realized output schema are not serializable —
    /// they live behind `Arc` and are reconstructed by the runtime, not
    /// shipped through the JSON `--explain` channel.
    Aggregation {
        name: String,
        #[serde(skip)]
        span: Span,
        config: AggregateConfig,
        #[serde(skip)]
        compiled: Arc<CompiledAggregate>,
        strategy: AggregateStrategy,
        #[serde(skip)]
        output_schema: Arc<Schema>,
        /// Reason streaming was not selected. Populated by the
        /// `select_aggregation_strategies` post-pass when
        /// `config.strategy == Auto` and eligibility was `HashFallback`.
        /// `None` for explicit Hash, explicit Streaming, or Auto-that-qualified.
        #[serde(skip_serializing_if = "Option::is_none")]
        fallback_reason: Option<String>,
        /// `true` when `config.strategy == Hash` AND eligibility was
        /// `Streaming` — surfaces in `--explain` so users notice missed
        /// performance opportunities at their own request.
        #[serde(default, skip_serializing_if = "is_false")]
        skipped_streaming_available: bool,
        /// Qualified sort order used for runtime `SortKeyEncoder`
        /// construction. `Some` iff resolved strategy is Streaming.
        #[serde(skip_serializing_if = "Option::is_none")]
        qualified_sort_order: Option<Vec<SortField>>,
    },
    /// Composition call-site node. Lowered to `PlanNode::Composition` in
    /// Stage 5; body nodes live in `CompileArtifacts.composition_bodies`
    /// keyed by the `body` handle.
    Composition {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Handle into `CompileArtifacts.composition_bodies`. Populated by
        /// `bind_composition` during `bind_schema`; sentinel
        /// `CompositionBodyId::SENTINEL` before binding runs.
        body: CompositionBodyId,
        /// Lowered output schema of the composition body. Populated by
        /// `bind_composition` from the body's terminal-node output row.
        #[serde(skip)]
        output_schema: Arc<Schema>,
    },
    /// Planner-synthesized terminal commit node for `correlation_key` pipelines.
    ///
    /// Inserted by [`ExecutionPlanDag::inject_correlation_commit`] downstream
    /// of every [`PlanNode::Output`] when at least one source declares a
    /// `correlation_key:`. The Output arm redirects projected records into
    /// `ExecutorContext.correlation_buffers` instead of writing to the
    /// FormatWriter; this arm walks the buffer at end-of-DAG and, per
    /// correlation group, either flushes records to the appropriate writer
    /// (clean group) or emits DLQ entries with `trigger`/collateral
    /// markings (any record in the group failed, or the group exceeded
    /// `max_group_buffer`). Reserved-prefix name guard: every
    /// `CorrelationCommit` node's name starts with
    /// [`CORRELATION_COMMIT_PREFIX`].
    CorrelationCommit {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Correlation key fields. The union of every source's
        /// declared `correlation_key:` field names, in declaration
        /// order. Read by the explain formatter for the
        /// `[correlation_commit] <name> key=[<fields>]` header.
        commit_group_by: Vec<String>,
        /// Per-group buffer cap. Mirrors
        /// `error_handling.max_group_buffer`; the runtime overflow
        /// check reads it via `ctx.correlation_max_group_buffer`,
        /// populated at executor construction.
        max_group_buffer: u64,
    },
    /// N-ary combine node.
    ///
    /// Carries the `--explain`-visible shape of the combine inline so the
    /// `ExecutionPlanDag` serializer — which does not see
    /// `CompileArtifacts` — can render JSON/text without a separate side
    /// table lookup. The heavy compile state (decomposed predicate
    /// programs, per-input schema rows, the typechecked `where`/`body`
    /// `TypedProgram`s) still lives in `CompileArtifacts` and is not
    /// duplicated here.
    Combine {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Planner-selected strategy. Defaults to `HashBuildProbe`;
        /// overwritten by the `select_combine_strategies` post-pass.
        /// Follows `PlanNode::Aggregation.strategy` precedent.
        strategy: crate::plan::combine::CombineStrategy,
        /// Driving (probe-side) input qualifier. Empty string until
        /// the `select_combine_strategies` post-pass selects it. A
        /// non-empty value implies `strategy` and `build_inputs` were
        /// also filled in the same pass.
        driving_input: String,
        /// Non-driving (build-side) input qualifiers. Populated by the
        /// `select_combine_strategies` post-pass alongside `driving_input`:
        /// every `CompileArtifacts.combine_inputs[name]` entry except
        /// the driver, in declaration order. Empty when the driver has
        /// not yet been selected.
        build_inputs: Vec<String>,
        /// `NodeIndex` of the driving (probe-side) predecessor in the DAG,
        /// resolved from `driving_input` against the combine's incoming
        /// neighbors during the `select_combine_strategies` post-pass.
        /// `None` until that pass runs (or when the driver predecessor
        /// could not be resolved — e.g. an N-ary decomposition step whose
        /// driver is a synthetic intermediate not present as a direct
        /// neighbor). The probe-side streaming-ingest predicate
        /// ([`certify_streaming_edge`]) reads this to identify the driver
        /// predecessor without `CompileArtifacts` access, because combine
        /// edges carry `port: None` and the predicate cannot map an input
        /// qualifier back to a node any other way.
        #[serde(skip)]
        driving_upstream: Option<petgraph::graph::NodeIndex>,
        /// Shape-only projection of the decomposed `where:` predicate
        /// (equalities count, ranges count, residual presence). Populated
        /// at lowering time from `CompileArtifacts.combine_predicates[name]`;
        /// zero-valued when the predicate decomposition produced no entry
        /// (E3xx combines).
        predicate_summary: crate::plan::combine::CombinePredicateSummary,
        match_mode: crate::config::pipeline_node::MatchMode,
        on_miss: crate::config::pipeline_node::OnMiss,
        /// Mirrors `CombineBody.propagate_ck`. Selects which correlation-key
        /// fields the combine's output rows carry — driver-only, union of
        /// all inputs, or an explicit named subset. Read by the per-node
        /// CK-set lattice in `compute_one`.
        propagate_ck: crate::config::pipeline_node::PropagateCkSpec,
        /// For synthetic binary combine nodes created by N-ary
        /// decomposition: name of the original N-ary combine node this
        /// was decomposed from. `None` for user-authored combine nodes.
        /// Consumed by `--explain` rendering for grouping.
        decomposed_from: Option<String>,
        /// Widened post-combine output schema. For `match: collect` this
        /// is the driver's schema plus one trailing column for the
        /// collected array; for `match: first | all` it is the body-emit
        /// widened schema. Populated by `bind_schema::bind_combine`.
        #[serde(skip)]
        output_schema: Arc<Schema>,
        /// Pre-resolved `(side, column-index)` for every qualified field
        /// reference in the combine body. Populated by the CXL typechecker
        /// walk over the body against the per-input schemas; consumed
        /// by `CombineResolverMapping::from_pre_resolved` at executor
        /// start so probe-time resolution is a direct `Vec<Value>`
        /// index read instead of a name-keyed hash lookup.
        #[serde(skip)]
        resolved_column_map: ResolvedColumnMap,
        /// Typed `cxl:` body program the combine emits, carried on the node like
        /// `PlanTransformPayload.typed`. `None` only in the pre-lowering window;
        /// populated for every lowered combine. Read off the node by the executor
        /// and by downstream lineage instead of re-fetching from a name-keyed map.
        #[serde(skip)]
        typed: Option<Arc<TypedProgram>>,
    },
}

/// Pre-resolved `(side, column-index)` map for combine body references.
///
/// Produced by the CXL typechecker during combine body typechecking,
/// consumed by the executor's combine resolver mapping at executor
/// start-up. One entry per qualified field the body reads.
pub type ResolvedColumnMap = Arc<HashMap<QualifiedField, (JoinSide, u32)>>;

/// Fully-resolved Source payload, populated by the
/// `PipelineConfig::compile()` lowering path. Wraps the parse-time
/// `SourceConfig` plus the `ValidatedPath` (proof of pre-pass success).
/// Stored behind `Box` on `PlanNode::Source` to keep the variant slim.
#[derive(Debug, Clone)]
pub struct PlanSourcePayload {
    pub source: SourceConfig,
    pub validated_path: Option<crate::security::ValidatedPath>,
}

/// Fully-resolved Transform payload. Holds the optional analytic-window
/// spec, the log directives, the validations sidebar, the DLQ NodeId for
/// downstream wiring, and the compile-time CXL `TypedProgram`.
#[derive(Debug, Clone)]
pub struct PlanTransformPayload {
    pub analytic_window: Option<AnalyticWindowSpec>,
    pub log: Vec<crate::config::LogDirective>,
    pub validations: Vec<crate::config::ValidationEntry>,
    pub dlq_node: Option<NodeIndex>,
    /// Compile-time-typechecked CXL program. Populated by
    /// `PipelineConfig::compile` via `bind_schema::bind_schema` and
    /// NEVER `None`: a transform whose CXL fails to typecheck surfaces
    /// as a compile-time E200 diagnostic and the enclosing `compile()`
    /// call returns `Err` before this payload is built.
    pub typed: Arc<TypedProgram>,
    /// Producer-declared scoped variables this transform writes via
    /// `emit $<scope>.<key>`. Empty list means no scope writes.
    pub declares: Vec<crate::config::pipeline_node::DeclareEntry>,
    /// `phase: init` runs the transform (and its transitive ancestors)
    /// to completion before any runtime-phase node sees a record.
    pub phase: crate::config::pipeline_node::Phase,
}

/// Fully-resolved Output payload.
#[derive(Debug, Clone)]
pub struct PlanOutputPayload {
    pub output: OutputConfig,
    pub validated_path: Option<crate::security::ValidatedPath>,
    /// Plan-time flag: this Output's path template uses a per-record
    /// token (`{source_file}` / `{source_path}`) AND its parent
    /// partitioning is `FilePartitioned`. The CLI pre-opens one writer
    /// per source file and the dispatcher routes each record to the
    /// right writer by `$source.file` Arc. Set by
    /// [`ExecutionPlanDag::populate_fan_out_flags`] after partition
    /// propagation. Defaults to `false` for single-file sources or
    /// templates without per-record tokens.
    pub fan_out_per_source_file: bool,
}

impl PlanNode {
    /// Get the name of this node regardless of variant.
    pub fn name(&self) -> &str {
        match self {
            PlanNode::Source { name, .. }
            | PlanNode::Transform { name, .. }
            | PlanNode::Route { name, .. }
            | PlanNode::Merge { name, .. }
            | PlanNode::Output { name, .. }
            | PlanNode::Reshape { name, .. }
            | PlanNode::Cull { name, .. }
            | PlanNode::Envelope { name, .. }
            | PlanNode::Sort { name, .. }
            | PlanNode::Aggregation { name, .. }
            | PlanNode::Composition { name, .. }
            | PlanNode::Combine { name, .. }
            | PlanNode::CorrelationCommit { name, .. } => name,
        }
    }

    /// Source-span of this node, or `Span::SYNTHETIC` for planner-synthesized
    /// nodes (sort enforcers, correlation sorts) and the legacy planner path.
    pub fn span(&self) -> Span {
        match self {
            PlanNode::Source { span, .. }
            | PlanNode::Transform { span, .. }
            | PlanNode::Route { span, .. }
            | PlanNode::Merge { span, .. }
            | PlanNode::Output { span, .. }
            | PlanNode::Reshape { span, .. }
            | PlanNode::Cull { span, .. }
            | PlanNode::Envelope { span, .. }
            | PlanNode::Sort { span, .. }
            | PlanNode::Aggregation { span, .. }
            | PlanNode::Composition { span, .. }
            | PlanNode::Combine { span, .. }
            | PlanNode::CorrelationCommit { span, .. } => *span,
        }
    }

    /// The stored `Arc<Schema>` for this node. For variants whose output
    /// row shape matches the upstream (Route/Output/Sort), callers must
    /// resolve via the graph (see [`PlanNode::output_schema_in`]).
    /// Names of CXL-emitted columns this node produces, for downstream
    /// `include_unmapped: false` projection at the Output boundary.
    ///
    /// - Source: every column is user-declared in the source schema, so
    ///   the full schema counts as "explicitly emitted".
    /// - Transform: `write_set` — just the `emit` LHS names, excluding
    ///   upstream passthroughs that the planner widened into the output
    ///   schema.
    /// - Aggregation / Combine / Composition / Merge: `output_schema`
    ///   columns — these variants don't widen; their schema IS the set
    ///   of explicitly produced columns.
    /// - Route / Sort / Output: walk to the immediate upstream and
    ///   inherit its emit names (these variants don't add their own).
    pub fn cxl_emit_names_in(&self, dag: &ExecutionPlanDag) -> Vec<String> {
        match self {
            PlanNode::Source { output_schema, .. } => output_schema
                .columns()
                .iter()
                .map(|c| c.to_string())
                .collect(),
            PlanNode::Transform {
                write_set,
                output_schema,
                ..
            } => output_schema
                .columns()
                .iter()
                .filter(|c| write_set.contains(c.as_ref()))
                .map(|c| c.to_string())
                .collect(),
            PlanNode::Aggregation { output_schema, .. }
            | PlanNode::Combine { output_schema, .. }
            | PlanNode::Composition { output_schema, .. }
            | PlanNode::Reshape { output_schema, .. }
            | PlanNode::Cull { output_schema, .. }
            | PlanNode::Envelope { output_schema, .. }
            | PlanNode::Merge { output_schema, .. } => output_schema
                .columns()
                .iter()
                .map(|c| c.to_string())
                .collect(),
            PlanNode::Route { .. }
            | PlanNode::Sort { .. }
            | PlanNode::Output { .. }
            | PlanNode::CorrelationCommit { .. } => {
                let name = self.name();
                let idx = match dag
                    .graph
                    .node_indices()
                    .find(|&i| dag.graph[i].name() == name)
                {
                    Some(i) => i,
                    None => return Vec::new(),
                };
                dag.graph
                    .neighbors_directed(idx, petgraph::Direction::Incoming)
                    .next()
                    .map(|upstream| dag.graph[upstream].cxl_emit_names_in(dag))
                    .unwrap_or_default()
            }
        }
    }

    pub fn stored_output_schema(&self) -> Option<&Arc<Schema>> {
        match self {
            PlanNode::Source { output_schema, .. }
            | PlanNode::Transform { output_schema, .. }
            | PlanNode::Aggregation { output_schema, .. }
            | PlanNode::Combine { output_schema, .. }
            | PlanNode::Composition { output_schema, .. }
            | PlanNode::Reshape { output_schema, .. }
            | PlanNode::Cull { output_schema, .. }
            | PlanNode::Envelope { output_schema, .. }
            | PlanNode::Merge { output_schema, .. } => Some(output_schema),
            PlanNode::Route { .. }
            | PlanNode::Output { .. }
            | PlanNode::Sort { .. }
            | PlanNode::CorrelationCommit { .. } => None,
        }
    }

    /// The `Arc<Schema>` this node emits. For row-preserving variants
    /// (Route/Output/Sort) this walks the graph to the sole upstream and
    /// returns its schema. At the top level the DAG invariant is that
    /// these variants always have exactly one incoming data edge; in
    /// composition-body context a Route at the body root may consume
    /// an input port that doesn't appear as a graph edge — for that
    /// case we fall back to a leaked empty `Arc<Schema>` so downstream
    /// schema checks see "no expected schema" (paired with the body
    /// arm that already handles this gracefully via the
    /// `expected_input_schema_in() -> Option<&Arc>` shape).
    pub fn output_schema_in<'a>(&'a self, dag: &'a ExecutionPlanDag) -> &'a Arc<Schema> {
        if let Some(s) = self.stored_output_schema() {
            return s;
        }
        let name = self.name();
        let idx = dag
            .graph
            .node_indices()
            .find(|&i| dag.graph[i].name() == name)
            .expect("PlanNode::output_schema_in: node not present in its own dag");
        if let Some(upstream) = dag
            .graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .next()
        {
            return dag.graph[upstream].output_schema_in(dag);
        }
        // Body-root row-preserving fallback. The empty schema is
        // structurally equal to nothing, so any downstream `column-list`
        // structural check will fail unless the consumer also short-
        // circuits on `expected.column_count() == 0` — which the
        // current dispatcher arms do via the `Option<&Arc<_>>` peek.
        // The Arc is leaked because `&'a Arc<Schema>` requires a
        // borrow that outlives this call; the leak is one-shot per
        // body Route at compile time.
        static EMPTY_SCHEMA: std::sync::OnceLock<Arc<Schema>> = std::sync::OnceLock::new();
        EMPTY_SCHEMA.get_or_init(|| Arc::new(Schema::new(Vec::new())))
    }

    /// The `Arc<Schema>` this node expects to see on incoming records.
    /// Equal to the sole upstream's `output_schema_in` for every variant
    /// with exactly one incoming edge (Transform/Aggregate/Route/Output/
    /// Sort/Composition). Returns `None` for Sources (no upstream) and
    /// for Merge/Combine (N>1 upstreams — callers check per input).
    pub fn expected_input_schema_in<'a>(
        &'a self,
        dag: &'a ExecutionPlanDag,
    ) -> Option<&'a Arc<Schema>> {
        if matches!(self, PlanNode::Source { .. }) {
            return None;
        }
        let name = self.name();
        let idx = dag
            .graph
            .node_indices()
            .find(|&i| dag.graph[i].name() == name)?;
        let mut incoming = dag
            .graph
            .neighbors_directed(idx, petgraph::Direction::Incoming);
        let first = incoming.next()?;
        if incoming.next().is_some() {
            // N>1 upstreams (Merge, Combine) — caller must walk per-input.
            return None;
        }
        Some(dag.graph[first].output_schema_in(dag))
    }

    /// Get the type tag string for id slug construction.
    pub fn type_tag(&self) -> &'static str {
        match self {
            PlanNode::Source { .. } => "source",
            PlanNode::Transform { .. } => "transform",
            PlanNode::Route { .. } => "route",
            PlanNode::Merge { .. } => "merge",
            PlanNode::Output { .. } => "output",
            PlanNode::Sort { .. } => "sort",
            PlanNode::Aggregation { .. } => "aggregation",
            PlanNode::Composition { .. } => "composition",
            PlanNode::Combine { .. } => "combine",
            PlanNode::CorrelationCommit { .. } => "correlation_commit",
            PlanNode::Reshape { .. } => "reshape",
            PlanNode::Cull { .. } => "cull",
            PlanNode::Envelope { .. } => "envelope",
        }
    }

    /// Build the id slug: `"{type}.{name}"`.
    pub fn id_slug(&self) -> String {
        format!("{}.{}", self.type_tag(), self.name())
    }

    /// The named output ports this node emits to, or `None` when the node
    /// has a single unnamed output.
    ///
    /// Producer-side counterpart to a node's named inputs. Two variants emit
    /// named ports: a `Route` emits one output per branch plus its `default`,
    /// and a `Cull` emits `main` (kept groups) plus its `removed_to` port
    /// (removed groups). The dispatcher tags each outgoing edge with the
    /// [`PlanEdge::producer_port`] it carries, so a record assigned to port
    /// `p` flows down exactly the edges tagged `p`. The Route set is
    /// deduplicated and preserves branch declaration order, appending
    /// `default` last when it is not already a branch name. Every other
    /// variant returns `None` (single output).
    pub fn output_ports(&self) -> Option<Vec<&str>> {
        match self {
            PlanNode::Route {
                branches, default, ..
            } => {
                let mut ports: Vec<&str> = branches.iter().map(String::as_str).collect();
                if !ports.contains(&default.as_str()) {
                    ports.push(default.as_str());
                }
                Some(ports)
            }
            // Cull is the second producer-side multi-output operator: the
            // main port carries kept groups, the `removed_to` port carries
            // removed groups. A downstream node draws kept rows by
            // referencing the node name (bare) and removed rows by
            // referencing `<cull>.<removed_to>`.
            PlanNode::Cull { config, .. } => Some(vec!["main", config.removed_to.as_str()]),
            _ => None,
        }
    }

    /// Human-readable label for DOT node annotations and text display.
    pub fn display_name(&self) -> String {
        match self {
            PlanNode::Source { name, .. } => format!("[source] {name}"),
            PlanNode::Transform { name, .. } => format!("[transform] {name}"),
            PlanNode::Route { name, mode, .. } => {
                let mode_str = match mode {
                    RouteMode::Exclusive => "exclusive",
                    RouteMode::Inclusive => "inclusive",
                };
                format!("[route:{mode_str}] {name}")
            }
            PlanNode::Merge { name, .. } => format!("[merge] {name}"),
            PlanNode::Output { name, .. } => format!("[output] {name}"),
            PlanNode::Reshape { name, config, .. } => {
                format!(
                    "[reshape] {name} partition_by=[{}] rules={}",
                    config.partition_by.join(", "),
                    config.rules.len()
                )
            }
            PlanNode::Cull { name, config, .. } => {
                format!(
                    "[cull] {name} partition_by=[{}] rules={} removed_to={}",
                    config.partition_by.join(", "),
                    config.rules.len(),
                    config.removed_to
                )
            }
            PlanNode::Sort { name, .. } => format!("[sort] {name}"),
            PlanNode::Envelope { name, strategy, .. } => {
                let s = match strategy {
                    crate::config::pipeline_node::EnvelopeStrategy::Preserve => "preserve",
                    crate::config::pipeline_node::EnvelopeStrategy::Concat => "concat",
                };
                format!("[envelope:{s}] {name}")
            }
            PlanNode::Aggregation { name, strategy, .. } => {
                let s = match strategy {
                    AggregateStrategy::Hash => "hash",
                    AggregateStrategy::Streaming => "streaming",
                };
                format!("[aggregation:{s}] {name}")
            }
            PlanNode::Composition { name, body, .. } => {
                format!("[composition:body={}] {name}", body.0)
            }
            PlanNode::CorrelationCommit {
                name,
                commit_group_by,
                ..
            } => {
                format!(
                    "[correlation_commit] {name} key=[{}]",
                    commit_group_by.join(", ")
                )
            }
            PlanNode::Combine {
                name,
                strategy,
                driving_input,
                build_inputs,
                predicate_summary,
                ..
            } => {
                // Mirror `PlanNode::Aggregation`'s `[aggregation:<strategy>]`
                // precedent for the strategy tag; append
                // `drive=<qualifier> build=[a, b]` plus the predicate shape
                // so the header line carries enough context for an
                // inspector to understand what the planner chose without
                // cracking the JSON payload.
                let strategy_tag = combine_strategy_tag(strategy);
                let drive = if driving_input.is_empty() {
                    "<unselected>"
                } else {
                    driving_input.as_str()
                };
                let build = if build_inputs.is_empty() {
                    "[]".to_string()
                } else {
                    format!("[{}]", build_inputs.join(", "))
                };
                let residual = if predicate_summary.has_residual {
                    " +residual"
                } else {
                    ""
                };
                format!(
                    "[combine:{strategy_tag}] {name} drive={drive} build={build} \
                     eq={eq} range={range}{residual}",
                    eq = predicate_summary.equalities,
                    range = predicate_summary.ranges,
                )
            }
        }
    }
}

/// Edge dependency type.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DependencyType {
    /// Record data flows along this edge.
    Data,
    /// Index/lookup dependency (no data flow — ordering constraint only).
    Index,
    /// Cross-source dependency.
    CrossSource,
}

impl DependencyType {
    /// String label for DOT edge annotations.
    pub fn as_str(&self) -> &'static str {
        match self {
            DependencyType::Data => "data",
            DependencyType::Index => "index",
            DependencyType::CrossSource => "cross_source",
        }
    }
}

/// Edge in the execution plan DAG.
///
/// An edge connects a producer output to a consumer input. Two
/// independent port tags name the endpoints when either side carries
/// more than one labelled stream:
///
/// - `producer_port` names which output of the producer this edge draws
///   from. A node that emits multiple named output streams (today:
///   `PlanNode::Route`, one output per branch plus the default) tags each
///   outgoing edge with the output it carries. `None` means the producer
///   has a single unnamed output (every other operator).
/// - `port` (consumer-side) names which input of the consumer this edge
///   feeds when the consumer takes named inputs (today:
///   `PlanNode::Composition`). `None` otherwise.
///
/// The dispatcher uses the live edge graph + these tags as the single
/// source of truth for runtime port resolution: planner-injected nodes
/// (e.g., `inject_correlation_sort`'s synthetic Sort) reroute edges and
/// carry both tags forward, so a downstream arm always reads its
/// producer/consumer port through the live graph rather than a frozen
/// name snapshot.
#[derive(Debug, Clone, Serialize)]
pub struct PlanEdge {
    pub dependency_type: DependencyType,
    /// Consumer-side port name when the consumer takes named inputs;
    /// `None` otherwise. Preserved across edge reroutes by every
    /// planner pass that splices intermediate nodes.
    pub port: Option<String>,
    /// Producer-side output port this edge draws from when the producer
    /// emits multiple named outputs (Route branches / default); `None`
    /// for single-output producers. Preserved across edge reroutes by
    /// every planner pass that splices an intermediate node: the spliced
    /// node is a single-output passthrough, so the tag stays on the hop
    /// leaving the original multi-output producer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_port: Option<String>,
}

/// Plan-time projection of an operator's runtime arbitration parameters.
///
/// `--explain` runs plan-only: no executor, no I/O, so no
/// `MemoryConsumer` wrappers are registered and the arbitrator's live
/// registry is empty. To still show the author which operator the
/// arbitrator would spill or pause first, this carries the same
/// `(spill_priority, can_back_pressure)` pair the runtime wrapper would
/// report once the stage runs.
///
/// `spill_priority` is `None` for stages that hold no spillable state —
/// a Source (its `try_spill` always frees zero; the `i32::MAX` sentinel
/// only keeps it last in the policy ordering) and a streaming Aggregate
/// (it emits per-group and never registers a spillable consumer). Those
/// render `N/A` rather than a misleading number; their back-pressure
/// flag still prints.
pub(super) struct ArbitrationClass {
    /// Lower = spilled first. `None` for non-spillable stages (rendered
    /// `N/A`).
    pub(super) spill_priority: Option<i32>,
    /// Whether the active policy can pause this stage's producer instead
    /// of forcing a spill.
    pub(super) can_back_pressure: bool,
}

/// Classify a plan node's runtime arbitration parameters for the
/// `--explain` annotation.
///
/// This is a plan-time mirror of the per-operator `MemoryConsumer`
/// impls — the runtime authority. Each constant below is the literal
/// the matching wrapper's `spill_priority()` / `can_back_pressure()`
/// returns; the mirror exists because `--explain` has no live consumers
/// to query. Keep the two in lock-step:
///
/// - `node_buffers` slot — `executor::node_buffer::NodeBufferConsumer`
///   (priority `0`; `can_back_pressure` is the slot's own flag, today
///   always `false` at the admit site).
/// - Source ingest — `executor::source_stream::SourceConsumer`
///   (priority `i32::MAX`, back-pressureable; rendered `N/A` here).
/// - hash Aggregate — `aggregation::AggregateConsumer` (priority `30`).
/// - grace-hash Combine — `pipeline::grace_hash::GraceHashConsumer`
///   (priority `10`).
/// - sort buffer / IEJoin build — `pipeline::sort_buffer::SortConsumer`
///   (priority `20`).
/// - sort-merge Combine — `pipeline::sort_merge_join::SortMergeConsumer`
///   (priority `25`).
/// - inline-hash Combine — `pipeline::combine::CombineHashConsumer`
///   (priority `30`).
///
/// The Combine arms follow the runtime dispatch in
/// `executor::dispatch`: each strategy registers exactly one of the
/// consumers above (grace-hash, sort-merge, IEJoin-via-sort-buffer, or
/// inline-hash) before draining its build side.
pub(super) fn arbitration_class(node: &PlanNode) -> ArbitrationClass {
    use crate::plan::combine::CombineStrategy;
    match node {
        // Source frees zero on spill; only its pause is a real lever.
        PlanNode::Source { .. } => ArbitrationClass {
            spill_priority: None,
            can_back_pressure: true,
        },
        // Streaming Aggregate holds at most one group's state and never
        // registers a spillable consumer; hash Aggregate accumulates the
        // full group table and spills it.
        PlanNode::Aggregation {
            strategy: AggregateStrategy::Streaming,
            ..
        } => ArbitrationClass {
            spill_priority: None,
            can_back_pressure: false,
        },
        PlanNode::Aggregation { .. } => ArbitrationClass {
            spill_priority: Some(30),
            can_back_pressure: false,
        },
        PlanNode::Sort { .. } => ArbitrationClass {
            spill_priority: Some(20),
            can_back_pressure: false,
        },
        PlanNode::Combine { strategy, .. } => {
            let spill_priority = match strategy {
                CombineStrategy::GraceHash { .. } => Some(10),
                CombineStrategy::SortMerge => Some(25),
                CombineStrategy::IEJoin | CombineStrategy::HashPartitionIEJoin { .. } => Some(20),
                CombineStrategy::HashBuildProbe => Some(30),
                // Never selected by `select_combine_strategies` for a
                // runnable plan — they error at dispatch. No consumer
                // registers, so there is no spillable state to rank.
                CombineStrategy::InMemoryHash | CombineStrategy::BlockNestedLoop => None,
            };
            ArbitrationClass {
                spill_priority,
                can_back_pressure: false,
            }
        }
        // Reshape buffers every input record per correlation group before
        // any rule fires (the no-cascade contract forbids incremental folding
        // — a rule must observe the whole group), then spills the raw input
        // records via a whole-record postcard+LZ4 round-trip and re-runs
        // synthesis on reload. Priority `15` sits between grace-hash (`10`)
        // and sort (`20`): a grouped record buffer that is costlier to evict
        // than grace partitions (it pays the re-synthesis CPU on reload) but
        // cheaper than an external-sort merge. No upstream channel to gate,
        // so it cannot back-pressure.
        PlanNode::Reshape { .. } => ArbitrationClass {
            spill_priority: Some(15),
            can_back_pressure: false,
        },
        // Cull buffers every input record per correlation group before any
        // group-level `drop_group_when` predicate can decide keep-vs-remove
        // (an aggregate property of the whole group cannot be folded
        // incrementally into a keep/remove decision), then spills the raw
        // input records via the same whole-record round-trip Reshape uses.
        // Priority `15` matches Reshape — a grouped record buffer costlier
        // to evict than grace partitions but cheaper than an external-sort
        // merge. No upstream channel to gate, so it cannot back-pressure.
        PlanNode::Cull { .. } => ArbitrationClass {
            spill_priority: Some(15),
            can_back_pressure: false,
        },
        // Stateless or sink stages register no spillable consumer:
        // Transform / Route / Merge stream record-at-a-time, Envelope passes
        // body records through unchanged (its NodeBuffer slot carries its own
        // arbitrator wrapper, not a stage consumer), Output is a sink,
        // Composition is a structural wrapper whose body nodes carry their own
        // classes, and CorrelationCommit's group buffer is bounded by
        // `max_group_buffer` rather than the arbitrator.
        PlanNode::Transform { .. }
        | PlanNode::Route { .. }
        | PlanNode::Merge { .. }
        | PlanNode::Envelope { .. }
        | PlanNode::Output { .. }
        | PlanNode::Composition { .. }
        | PlanNode::CorrelationCommit { .. } => ArbitrationClass {
            spill_priority: None,
            can_back_pressure: false,
        },
    }
}

/// `true` iff this operator backs its state with a real on-disk spill
/// writer — i.e. it actually writes spill files when the memory budget
/// trips, rather than merely carrying a `spill_priority` for memory
/// arbitration.
///
/// This is a strict subset of "has a non-`None` [`arbitration_class`]
/// spill priority". Some operators register a spillable `MemoryConsumer`
/// so the arbitrator can rank them, yet run their kernel entirely
/// in-memory and never open a spill file:
///
/// - inline hash Combine (`HashBuildProbe`) builds its hash table in RAM;
///   its consumer's `try_spill` requests a rebuild, it does not write a
///   spill file.
/// - `IEJoin` / `HashPartitionIEJoin` register under the sort-buffer
///   consumer for arbitration accounting but their range-join kernel
///   partitions and walks in memory — no spill writer is wired.
///
/// The operators that DO write spill files (`SpillWriter` /
/// `AggSpillWriter` / `GraceSpillWriter`):
///
/// - external sort (`PlanNode::Sort`),
/// - hash Aggregate (`PlanNode::Aggregation` with the hash strategy;
///   streaming aggregate holds one group and never spills),
/// - grace-hash and sort-merge Combine,
/// - Reshape (`PlanNode::Reshape`), which spills its raw per-group input
///   records and re-runs synthesis on reload.
///
/// Surfaces that describe what hits disk — the `--explain`
/// spill-compression projection, the per-stage estimated spill volume,
/// and the run-startup free-space / cap-headroom preflight — must filter
/// on this predicate, not on `spill_priority.is_some()`, so an in-memory
/// join never appears as a disk-spilling stage.
pub(super) fn writes_spill_files(node: &PlanNode) -> bool {
    use crate::plan::combine::CombineStrategy;
    match node {
        PlanNode::Sort { .. } => true,
        PlanNode::Aggregation {
            strategy: AggregateStrategy::Hash,
            ..
        } => true,
        PlanNode::Aggregation {
            strategy: AggregateStrategy::Streaming,
            ..
        } => false,
        PlanNode::Combine { strategy, .. } => matches!(
            strategy,
            CombineStrategy::GraceHash { .. } | CombineStrategy::SortMerge
        ),
        // Reshape spills its raw input records per group when the buffer trips
        // the budget and re-runs synthesis on reload, so it writes real spill
        // files and must appear in the disk-spill projection surfaces.
        PlanNode::Reshape { .. } => true,
        // Cull spills its raw per-group input records when the buffer trips
        // the budget and re-splits on reload, so it writes real spill files
        // and must appear in the disk-spill projection surfaces.
        PlanNode::Cull { .. } => true,
        // Envelope passes body records through unchanged and registers no
        // spillable consumer, so it never writes spill files.
        PlanNode::Source { .. }
        | PlanNode::Transform { .. }
        | PlanNode::Route { .. }
        | PlanNode::Merge { .. }
        | PlanNode::Envelope { .. }
        | PlanNode::Output { .. }
        | PlanNode::Composition { .. }
        | PlanNode::CorrelationCommit { .. } => false,
    }
}

/// DAG-based execution plan — replaces ExecutionPlan.
///
/// The single source of truth for pipeline topology and execution strategy.
/// Custom Serialize emits flat node-list JSON for tooling consumption.
#[derive(Debug, Clone)]
pub struct ExecutionPlanDag {
    /// petgraph DAG of PlanNode/PlanEdge.
    pub graph: DiGraph<PlanNode, PlanEdge>,
    /// Topologically sorted node indices.
    pub topo_order: Vec<NodeIndex>,
    /// Topologically sorted source tiers for Phase 1 ordering.
    pub source_dag: Vec<SourceTier>,
    /// Indices to build during Phase 1. Deduplicated.
    pub indices_to_build: Vec<IndexSpec>,
    /// Per-output projection rules.
    pub output_projections: Vec<OutputSpec>,
    /// Parallelism profile for the pipeline.
    pub parallelism: ParallelismProfile,
    /// Physical properties (ordering, partitioning) per node, keyed by
    /// `NodeIndex`. Populated by
    /// [`compute_node_properties`](ExecutionPlanDag::compute_node_properties)
    /// after transform compilation. Default-empty on construction.
    pub node_properties: HashMap<NodeIndex, crate::plan::properties::NodeProperties>,
    /// Deferred-region metadata per relaxed-CK aggregate. Empty for
    /// strict pipelines and CK-aligned aggregates. Populated by
    /// `crate::plan::deferred_region::detect_deferred_regions` after
    /// the window-buffer-recompute pass in compile Stage 5. Keyed by
    /// every `NodeIndex` participating in a region (producer + members
    /// + outputs) so dispatch-arm lookup is O(1).
    pub deferred_regions: HashMap<NodeIndex, crate::plan::deferred_region::DeferredRegion>,
    /// Parent-continuation metadata per Composition node whose body
    /// (transitively) carries a deferred region. Keyed by the
    /// Composition's `NodeIndex` in this DAG. Populated alongside
    /// `deferred_regions` in compile Stage 5; the commit-time
    /// dispatcher consults this map after harvesting body output
    /// records to drive the parent's downstream chain on
    /// post-recompute data.
    pub parent_continuations: HashMap<NodeIndex, crate::plan::deferred_region::ParentContinuation>,
}

/// Errors from execution plan compilation.
#[derive(Debug)]
pub enum PlanError {
    /// A Transform's CXL invokes `$window.*` but the YAML body declares
    /// no `analytic_window:` block.
    MissingAnalyticWindow {
        transform: String,
    },
    UnknownSource {
        name: String,
        transform: String,
    },
    /// Cycle detected in the DAG with path trace.
    CycleDetected {
        path: String,
    },
    /// An `input:` reference does not resolve to any declared transform or branch.
    InvalidInputReference {
        reference: String,
        transform: String,
        available: Vec<String>,
    },
    /// Two nodes produce the same id slug.
    DuplicateIdSlug {
        slug: String,
    },
    /// A transform's `input:` references itself.
    SelfReference {
        transform: String,
    },
    /// Enforcer-insertion or property-derivation pass failure.
    PropertyDerivation(String),
    /// `cxl::plan::extract_aggregates` rejected the typed program.
    AggregateExtractionFailed {
        transform: String,
        diagnostics: Vec<String>,
    },
    /// Aggregate transforms cannot have a `route:` block.
    AggregateWithRoute {
        transform: String,
    },
    /// Aggregate transforms cannot have a multi-input merge.
    AggregateWithMultipleInputs {
        transform: String,
    },
    /// E150 — a source's `correlation_key:` is incompatible with any
    /// Transform that uses analytic windows (`window_index: Some(_)`).
    /// Per-group arena construction across correlation boundaries is
    /// a separate concern; reject at compile time so users see the
    /// limitation up front instead of a confusing runtime path.
    CorrelationKeyWithArena {
        transform: String,
    },
    /// E152 — a `PlanNode::Composition` has an incoming edge with no
    /// `PlanEdge.port` tag. Compile-time guard for the dispatcher's
    /// runtime invariant: `collect_port_records` resolves composition
    /// inputs by reading the port tag off each incoming edge, and an
    /// untagged edge is a planner-pass bug (the rewrite spliced a
    /// node between the producer and the composition without
    /// preserving the tag). Surfaced here so it fails compile rather
    /// than silently dropping records at dispatch.
    CompositionUntaggedIncomingEdge {
        composition: String,
        producer: String,
        scope: String,
    },
}

impl std::fmt::Display for PlanError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanError::MissingAnalyticWindow { transform } => {
                write!(
                    f,
                    "transform '{}' uses window.* but has no analytic_window config",
                    transform
                )
            }
            PlanError::UnknownSource { name, transform } => {
                write!(
                    f,
                    "transform '{}' references unknown source '{}'",
                    transform, name
                )
            }
            PlanError::CycleDetected { path } => {
                write!(f, "cycle detected in transform DAG: {}", path)
            }
            PlanError::InvalidInputReference {
                reference,
                transform,
                available,
            } => {
                write!(
                    f,
                    "transform '{}' references unknown input '{}'. Available transforms: [{}]",
                    transform,
                    reference,
                    available.join(", ")
                )
            }
            PlanError::DuplicateIdSlug { slug } => {
                write!(f, "duplicate id slug in execution plan: '{}'", slug)
            }
            PlanError::SelfReference { transform } => {
                write!(
                    f,
                    "transform '{}' references itself in input: field",
                    transform
                )
            }
            PlanError::PropertyDerivation(msg) => {
                write!(f, "property derivation failed: {}", msg)
            }
            PlanError::AggregateExtractionFailed {
                transform,
                diagnostics,
            } => {
                write!(
                    f,
                    "aggregate extraction failed for transform '{}': {}",
                    transform,
                    diagnostics.join("; ")
                )
            }
            PlanError::AggregateWithRoute { transform } => {
                write!(
                    f,
                    "aggregate transform '{}' cannot also declare a `route:` block",
                    transform
                )
            }
            PlanError::AggregateWithMultipleInputs { transform } => {
                write!(
                    f,
                    "aggregate transform '{}' cannot consume multiple inputs",
                    transform
                )
            }
            PlanError::CorrelationKeyWithArena { transform } => {
                write!(
                    f,
                    "E150 transform '{transform}' uses analytic windows but at \
                     least one source declares a `correlation_key:`; per-group \
                     arena construction is not supported",
                )
            }
            PlanError::CompositionUntaggedIncomingEdge {
                composition,
                producer,
                scope,
            } => {
                write!(
                    f,
                    "E152 composition '{composition}' has untagged incoming edge from \
                     '{producer}' ({scope}); every composition input edge must carry a \
                     port name (planner-pass invariant — see PlanEdge.port)",
                )
            }
        }
    }
}

impl std::error::Error for PlanError {}
