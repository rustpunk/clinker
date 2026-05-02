//! Phase F: Execution plan emission.
//!
//! Compiles a `PipelineConfig` + CXL programs into an `ExecutionPlanDag`
//! that orchestrates the pipeline via a petgraph DAG.

use std::collections::{BTreeSet, HashMap, HashSet};

use indexmap::IndexMap;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{Control, DfsEvent, EdgeRef, Topo, depth_first_search};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

use std::sync::Arc;

use crate::aggregation::AggregateStrategy;
use crate::config::{
    AggregateConfig, OutputConfig, PipelineConfig, RouteMode, SortField, SourceConfig,
};
use crate::error::PipelineError;
use crate::executor::combine::JoinSide;
use crate::plan::composition_body::CompositionBodyId;
use crate::plan::index::{IndexSpec, LocalWindowConfig, PlanIndexError};
use crate::plan::properties::{
    NodeProperties, Ordering, OrderingProvenance, Partitioning, PartitioningKind,
    PartitioningProvenance,
};
use crate::plan::row_type::QualifiedField;
use crate::span::Span;
use clinker_record::Schema;
use cxl::plan::CompiledAggregate;

fn is_false(b: &bool) -> bool {
    !*b
}

/// Compact tag for a `CombineStrategy` variant, for `display_name()` /
/// `[combine:<tag>]` rendering. Pattern-matches `AggregateStrategy`'s
/// `hash` / `streaming` tag convention so both operators read alike in
/// `--explain` output.
fn combine_strategy_tag(s: &crate::plan::combine::CombineStrategy) -> &'static str {
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

/// Human-readable strategy label for the multi-line `--explain` block.
/// `HashPartitionIEJoin` and `GraceHash` append their `1 << partition_bits`
/// partition count so the planner's bucket choice surfaces without making
/// the reader compute it. The bare-tag form is used elsewhere (header
/// line, JSON tag), the spelled-out form here.
fn combine_strategy_display(s: &crate::plan::combine::CombineStrategy) -> String {
    use crate::plan::combine::CombineStrategy;
    match s {
        CombineStrategy::HashBuildProbe => "HashBuildProbe".to_string(),
        CombineStrategy::InMemoryHash => "InMemoryHash".to_string(),
        CombineStrategy::HashPartitionIEJoin { partition_bits } => {
            format!(
                "HashPartitionIEJoin ({} partitions)",
                1u32 << *partition_bits
            )
        }
        CombineStrategy::IEJoin => "IEJoin".to_string(),
        CombineStrategy::SortMerge => "SortMerge".to_string(),
        CombineStrategy::GraceHash { partition_bits } => {
            format!("GraceHash ({} partitions)", 1u32 << *partition_bits)
        }
        CombineStrategy::BlockNestedLoop => "BlockNestedLoop".to_string(),
    }
}

/// Compact predicate-shape label for N-ary step lines: `"equi"`,
/// `"range"`, `"mixed"`, or `"residual"`. Each step in a decomposed
/// chain renders one of these so the reader sees what kind of conjuncts
/// the planner peeled off at that depth without having to count the
/// summary fields by eye.
fn format_predicate_kind(s: &crate::plan::combine::CombinePredicateSummary) -> &'static str {
    match (s.equalities > 0, s.ranges > 0, s.has_residual) {
        (true, false, false) => "equi",
        (false, true, false) => "range",
        (true, true, _) => "mixed",
        (false, false, true) => "residual",
        (true, false, true) => "equi+residual",
        (false, true, true) => "range+residual",
        (false, false, false) => "<empty>",
    }
}

/// Human-readable label for `MatchMode`. Lowercase to match the YAML
/// surface syntax (`match: first`, `match: all`, `match: collect`).
fn format_match_mode(m: crate::config::pipeline_node::MatchMode) -> &'static str {
    use crate::config::pipeline_node::MatchMode;
    match m {
        MatchMode::First => "first",
        MatchMode::All => "all",
        MatchMode::Collect => "collect",
    }
}

/// Human-readable label for `OnMiss`. Lowercase to match the YAML
/// surface syntax (`on_miss: null_fields | skip | error`).
fn format_on_miss(o: crate::config::pipeline_node::OnMiss) -> &'static str {
    use crate::config::pipeline_node::OnMiss;
    match o {
        OnMiss::NullFields => "null_fields",
        OnMiss::Skip => "skip",
        OnMiss::Error => "error",
    }
}

/// Format a row count with comma thousands separators (e.g. `1,000,000`),
/// returning the literal string `"null"` when the cardinality is unknown.
/// Honest output (V-8-2): the planner only fills `estimated_cardinality`
/// at sources that carry an explicit hint, so most inputs render `null` —
/// matching DataFusion / Spark / DuckDB behavior when statistics are
/// absent.
fn format_estimated_rows(input: Option<&crate::plan::combine::CombineInput>) -> String {
    match input.and_then(|ci| ci.estimated_cardinality) {
        Some(n) => {
            // Comma thousands separator, manually formatted (no
            // dependency on `num-format` for one call site).
            let s = n.to_string();
            let mut out = String::with_capacity(s.len() + s.len() / 3);
            let bytes = s.as_bytes();
            for (i, &b) in bytes.iter().enumerate() {
                if i > 0 && (bytes.len() - i) % 3 == 0 {
                    out.push(',');
                }
                out.push(b as char);
            }
            out
        }
        None => "null".to_string(),
    }
}

/// Format a byte count with the largest binary-prefix unit that keeps
/// the magnitude under 1024. `64M` / `2G` style — matches the surface
/// syntax of `pipeline.memory_limit` so users see the value back in the
/// units they wrote.
fn format_bytes(n: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = 1024 * KIB;
    const GIB: u64 = 1024 * MIB;
    if n >= GIB && n.is_multiple_of(GIB) {
        format!("{}G", n / GIB)
    } else if n >= MIB && n.is_multiple_of(MIB) {
        format!("{}M", n / MIB)
    } else if n >= KIB && n.is_multiple_of(KIB) {
        format!("{}K", n / KIB)
    } else if n >= GIB {
        format!("{:.2}G", n as f64 / GIB as f64)
    } else if n >= MIB {
        format!("{:.2}M", n as f64 / MIB as f64)
    } else if n >= KIB {
        format!("{:.2}K", n as f64 / KIB as f64)
    } else {
        format!("{}B", n)
    }
}

/// Per-combine planned share of the pipeline-level memory limit. The
/// runtime executor uses one shared `MemoryBudget`; the rendering
/// divides the limit equally across the combine nodes visible in the
/// DAG so a reader can predict a single combine's working-set
/// allocation. `combine_count_total.max(1)` (V-8-1) guards against a
/// zero divisor if the renderer is ever called for a DAG with no
/// combines (e.g., a future API that surfaces this helper standalone).
fn memory_budget_per_combine(total_limit_bytes: u64, combine_count_total: usize) -> u64 {
    let combine_count = combine_count_total.max(1) as u64;
    total_limit_bytes / combine_count
}

/// Render the planned-share memory budget line for a combine node. The
/// `(planned share)` qualifier disambiguates the displayed value from a
/// per-operator ceiling — without it, users read the displayed budget
/// as a hard cap and report it as a bug when one combine's working set
/// consumes the full pipeline limit. Soft limit follows the dual-
/// threshold model on `MemoryBudget` (default 80%).
fn format_memory_budget_line(planned_bytes: u64) -> String {
    let soft_pct = 0.80_f64;
    let soft_bytes = (planned_bytes as f64 * soft_pct) as u64;
    format!(
        "{} (soft: {}, hard: {})",
        format_bytes(planned_bytes),
        format_bytes(soft_bytes),
        format_bytes(planned_bytes),
    )
}

/// Describe a build input's role under the chosen strategy. Most
/// strategies tag the build side as a plain `"build"`; sort-merge
/// inputs are symmetric (both sides scan in lock-step) so the label
/// reads `"build (sorted scan)"` to match the operator's own naming.
fn describe_build_role(
    s: &crate::plan::combine::CombineStrategy,
    _build_name: &str,
) -> &'static str {
    use crate::plan::combine::CombineStrategy;
    match s {
        CombineStrategy::SortMerge => "build (sorted scan)",
        _ => "build",
    }
}

use cxl::analyzer::ParallelismHint;
use cxl::ast::Statement;
use cxl::typecheck::pass::TypedProgram;

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
        /// Field names this transform writes (assigns to via `emit name = ...`,
        /// excluding `$meta.*` writes). Populated at compile time from the CXL
        /// `TypedProgram`. Single source of truth for
        /// `compute_node_properties`'s `DestroyedByTransformWriteSet` rule —
        /// the property pass reads this directly off the node, no executor
        /// coupling.
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
    },
}

/// Pre-resolved `(side, column-index)` map for combine body references.
///
/// Produced by the CXL typechecker during combine body typechecking,
/// consumed by [`crate::executor::combine::CombineResolverMapping::from_pre_resolved`]
/// at executor start-up. One entry per qualified field the body reads.
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
    pub analytic_window: Option<crate::config::pipeline_node::AnalyticWindowSpec>,
    pub log: Vec<crate::config::LogDirective>,
    pub validations: Vec<crate::config::ValidationEntry>,
    pub dlq_node: Option<NodeIndex>,
    /// Compile-time-typechecked CXL program. Populated by
    /// `PipelineConfig::compile` via `bind_schema::bind_schema` and
    /// NEVER `None`: a transform whose CXL fails to typecheck surfaces
    /// as a compile-time E200 diagnostic and the enclosing `compile()`
    /// call returns `Err` before this payload is built.
    pub typed: Arc<TypedProgram>,
}

/// Fully-resolved Output payload.
#[derive(Debug, Clone)]
pub struct PlanOutputPayload {
    pub output: OutputConfig,
    pub validated_path: Option<crate::security::ValidatedPath>,
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
        }
    }

    /// Build the id slug: `"{type}.{name}"`.
    pub fn id_slug(&self) -> String {
        format!("{}.{}", self.type_tag(), self.name())
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
            PlanNode::Sort { name, .. } => format!("[sort] {name}"),
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
/// `port` carries the consumer-side port name when the edge feeds a
/// node whose inputs are named (currently `PlanNode::Composition`). The
/// dispatcher uses the live edge graph + port tag as the single source
/// of truth for runtime port resolution: planner-injected nodes (e.g.,
/// `inject_correlation_sort`'s synthetic Sort) reroute edges and carry
/// the tag forward, so a downstream composition arm always reads its
/// producer through the live graph rather than a frozen name snapshot.
/// `None` for unnamed-input edges (every other consumer arm).
#[derive(Debug, Clone, Serialize)]
pub struct PlanEdge {
    pub dependency_type: DependencyType,
    /// Consumer-side port name when the consumer takes named inputs;
    /// `None` otherwise. Preserved across edge reroutes by every
    /// planner pass that splices intermediate nodes.
    pub port: Option<String>,
}

/// DAG-based execution plan — replaces ExecutionPlan.
///
/// The single source of truth for pipeline topology and execution strategy.
/// Custom Serialize emits flat node-list JSON for Kiln consumption.
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
    pub(crate) deferred_regions: HashMap<NodeIndex, crate::plan::deferred_region::DeferredRegion>,
}

impl ExecutionPlanDag {
    /// Construct from already-computed parts. The compile path supplies
    /// the populated graph + topology + per-transform plan metadata;
    /// `node_properties` and `deferred_regions` are populated by later
    /// passes against the returned DAG via `&mut`. Test fixtures that
    /// drive a single planner pass against a hand-built graph (e.g. the
    /// combine-strategy tests in `crates/clinker-core/tests/`) call this
    /// with empty topo / sources / output projections — every consumer
    /// they exercise reads only `graph` and the per-pass metadata.
    pub fn from_parts(
        graph: DiGraph<PlanNode, PlanEdge>,
        topo_order: Vec<NodeIndex>,
        source_dag: Vec<SourceTier>,
        indices_to_build: Vec<IndexSpec>,
        output_projections: Vec<OutputSpec>,
        parallelism: ParallelismProfile,
    ) -> Self {
        Self {
            graph,
            topo_order,
            source_dag,
            indices_to_build,
            output_projections,
            parallelism,
            node_properties: HashMap::new(),
            deferred_regions: HashMap::new(),
        }
    }

    /// Build a transient `ExecutionPlanDag` whose graph + topo_order
    /// alias a composition body's mini-DAG, with empty top-level
    /// fields (source_dag, indices_to_build, output_projections,
    /// parallelism, correlation sort, node_properties).
    ///
    /// Used by the body executor to swap the dispatcher's
    /// `current_dag` field while running a composition body. The
    /// dispatcher reads `graph` for node lookup and neighbor walks
    /// and `topo_order` for ordering — every other field is a
    /// top-level concern that body walks don't touch. The graph is
    /// cloned because the dispatcher needs to own its `current_dag`
    /// borrow target via a stack-local; the graph itself is small
    /// relative to the record stream.
    pub(crate) fn from_body(body: &crate::plan::composition_body::BoundBody) -> Self {
        Self {
            graph: body.graph.clone(),
            topo_order: body.topo_order.clone(),
            source_dag: Vec::new(),
            indices_to_build: body.body_indices_to_build.clone(),
            output_projections: Vec::new(),
            parallelism: ParallelismProfile {
                per_transform: Vec::new(),
                worker_threads: 1,
            },
            node_properties: HashMap::new(),
            // Body-local deferred regions ride along on the transient
            // DAG so dispatcher arms reading `current_dag.deferred_regions`
            // see body-internal producers without any arm-side branching.
            // Body NodeIndex space matches `body.graph`, so the keys
            // remain valid against the cloned graph above.
            deferred_regions: body.deferred_regions.clone(),
        }
    }

    /// `Some(region)` iff `idx` participates in any deferred region on
    /// this DAG (producer, member, or output). Dispatcher arms call
    /// this at the top of every operator branch to decide whether to
    /// short-circuit to the deferred buffer.
    pub(crate) fn deferred_region_at(
        &self,
        idx: NodeIndex,
    ) -> Option<&crate::plan::deferred_region::DeferredRegion> {
        self.deferred_regions.get(&idx)
    }

    /// `true` iff `idx` is a non-producer participant in some deferred
    /// region (member or output). Operator arms use this to skip work
    /// on the forward pass — the deferred dispatch at commit will run
    /// the operator on post-recompute aggregate emits.
    pub(crate) fn is_deferred_consumer(&self, idx: NodeIndex) -> bool {
        self.deferred_regions
            .get(&idx)
            .is_some_and(|r| r.producer != idx)
    }

    /// `Some(region)` iff `idx` is the producer of a deferred region.
    /// The Aggregation arm uses this to project emits to
    /// `region.buffer_schema` before parking them in `node_buffers`.
    pub(crate) fn deferred_region_at_producer(
        &self,
        idx: NodeIndex,
    ) -> Option<&crate::plan::deferred_region::DeferredRegion> {
        self.deferred_regions
            .get(&idx)
            .filter(|r| r.producer == idx)
    }

    /// Whether any node requires arena allocation (window functions).
    pub fn required_arena(&self) -> bool {
        !self.indices_to_build.is_empty()
    }

    /// Get transform nodes in topological order.
    ///
    /// Returns `(window_index, partition_lookup)` per transform in topo order.
    /// The executor uses this to look up per-transform window context.
    pub fn transform_window_info(&self) -> Vec<(Option<usize>, Option<PartitionLookupKind>)> {
        self.topo_order
            .iter()
            .filter_map(|&idx| match &self.graph[idx] {
                PlanNode::Transform {
                    window_index,
                    partition_lookup,
                    ..
                } => Some((*window_index, partition_lookup.clone())),
                _ => None,
            })
            .collect()
    }

    /// Get transform parallelism classes in topological order.
    pub fn transform_parallelism_classes(&self) -> Vec<ParallelismClass> {
        self.topo_order
            .iter()
            .filter_map(|&idx| match &self.graph[idx] {
                PlanNode::Transform {
                    parallelism_class, ..
                } => Some(*parallelism_class),
                _ => None,
            })
            .collect()
    }

    /// Whether the DAG has in-pipeline branching (Route nodes with outgoing
    /// edges to Transform nodes, or Merge nodes).
    ///
    /// Route nodes for multi-output dispatch (no outgoing edges) do NOT
    /// constitute branching — they're handled by the compiled_route path.
    pub fn has_branching(&self) -> bool {
        use petgraph::Direction;
        // Check for Merge nodes (always branching)
        if self
            .graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::Merge { .. }))
        {
            return true;
        }
        // Aggregation nodes require the DAG walk path so the dispatch
        // arm handles them; the single-input streaming path would
        // otherwise row-evaluate the aggregate program and raise
        // "row-level expression, got aggregate function call".
        if self
            .graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::Aggregation { .. }))
        {
            return true;
        }
        // Combine nodes are multi-input and require the DAG-walk
        // executor path. Without this, combine pipelines would route
        // through the single-input streaming path and silently skip
        // the combine dispatch arm.
        if self
            .graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::Combine { .. }))
        {
            return true;
        }
        // Composition nodes recurse into a body mini-DAG via the
        // dispatcher's Composition arm. A streaming single-input
        // walk would never enter that arm; without this branch the
        // body executor would silently no-op and authors would see
        // upstream records pass straight through.
        if self
            .graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::Composition { .. }))
        {
            return true;
        }
        // Check for Route nodes with outgoing edges (in-pipeline branching)
        for idx in self.graph.node_indices() {
            if matches!(self.graph[idx], PlanNode::Route { .. }) {
                let has_outgoing = self
                    .graph
                    .neighbors_directed(idx, Direction::Outgoing)
                    .next()
                    .is_some();
                if has_outgoing {
                    return true;
                }
            }
        }
        false
    }

    /// Human-readable execution summary replacing `ExecutionMode` debug format.
    pub fn execution_summary(&self) -> String {
        if self.required_arena() {
            "TwoPass".to_string()
        } else {
            "Streaming".to_string()
        }
    }

    /// Format the execution plan for `--explain` display.
    pub fn explain(&self) -> String {
        let mut out = String::new();
        out.push_str("=== Execution Plan ===\n\n");

        out.push_str(&format!("Mode: {}\n", self.execution_summary()));
        out.push_str(&format!(
            "Indices to build: {}\n",
            self.indices_to_build.len()
        ));
        let transform_count = self
            .graph
            .node_weights()
            .filter(|n| matches!(n, PlanNode::Transform { .. }))
            .count();
        out.push_str(&format!("Transforms: {}\n", transform_count));
        out.push_str(&format!(
            "Output projections: {}\n",
            self.output_projections.len()
        ));
        out.push_str(&format!("DAG nodes: {}\n\n", self.graph.node_count()));

        if !self.source_dag.is_empty() {
            out.push_str("Source DAG:\n");
            for (tier_idx, tier) in self.source_dag.iter().enumerate() {
                out.push_str(&format!(
                    "  Tier {}: {}\n",
                    tier_idx,
                    tier.sources.join(", ")
                ));
            }
            out.push('\n');
        }

        for (i, spec) in self.indices_to_build.iter().enumerate() {
            out.push_str(&format!("Index [{}]:\n", i));
            out.push_str(&format!("  Root: {}\n", format_index_root(&spec.root)));
            out.push_str(&format!("  Group by: {:?}\n", spec.group_by));
            out.push_str(&format!(
                "  Sort by: {:?}\n",
                spec.sort_by.iter().map(|s| &s.field).collect::<Vec<_>>()
            ));
            out.push_str(&format!("  Arena fields: {:?}\n", spec.arena_fields));
            out.push_str(&format!("  Already sorted: {}\n", spec.already_sorted));
            if spec.requires_buffer_recompute {
                out.push_str(
                    "  Buffer recompute on commit: yes (worst-case memory = O(largest partition × per-row-size))\n",
                );
            }
            out.push('\n');
        }

        for node in self.graph.node_weights() {
            if let PlanNode::Transform {
                name,
                parallelism_class,
                window_index,
                partition_lookup,
                ..
            } = node
            {
                out.push_str(&format!("Transform '{name}':\n"));
                out.push_str(&format!("  Parallelism: {parallelism_class:?}\n"));
                out.push_str(&format!("  Window index: {window_index:?}\n"));
                out.push_str(&format!("  Partition lookup: {partition_lookup:?}\n\n"));
            }
        }

        // Show planner-synthesized Sort nodes.
        for &idx in &self.topo_order {
            if let PlanNode::Sort {
                name, sort_fields, ..
            } = &self.graph[idx]
            {
                out.push_str(&format!("[sort] {name}\n"));
                out.push_str("  sort_fields:\n");
                for sf in sort_fields {
                    out.push_str(&format!(
                        "    - {} {:?} (nulls {:?})\n",
                        sf.field, sf.order, sf.null_order
                    ));
                }
                out.push('\n');
            }
        }

        // Physical Properties (NodeProperties side-table). Only emitted
        // when the property pass has run (post-compile DAGs).
        if !self.node_properties.is_empty() {
            out.push_str("=== Physical Properties ===\n\n");
            for &idx in &self.topo_order {
                let node = &self.graph[idx];
                let Some(props) = self.node_properties.get(&idx) else {
                    continue;
                };
                out.push_str(&format!("{}:\n", node.id_slug()));
                match &props.ordering.sort_order {
                    Some(order) => {
                        let fields: Vec<String> = order.iter().map(|s| s.field.clone()).collect();
                        out.push_str(&format!("  ordering: {}\n", fields.join(", ")));
                    }
                    None => out.push_str("  ordering: <none>\n"),
                }
                out.push_str(&format!(
                    "  ordering_provenance: {:?}\n",
                    props.ordering.provenance
                ));
                out.push_str(&format!("  partitioning: {:?}\n", props.partitioning.kind));
                out.push_str(&format!(
                    "  partitioning_provenance: {:?}\n\n",
                    props.partitioning.provenance
                ));
            }
        }

        // Show route info from graph nodes
        for node in self.graph.node_weights() {
            if let PlanNode::Route {
                name,
                mode,
                branches,
                default,
                ..
            } = node
            {
                out.push_str(&format!(
                    "Route '{}' (mode: {}):\n",
                    name,
                    match mode {
                        RouteMode::Exclusive => "exclusive",
                        RouteMode::Inclusive => "inclusive",
                    }
                ));
                for branch_name in branches {
                    out.push_str(&format!(
                        "  Branch '{}' → output '{}'\n",
                        branch_name, branch_name
                    ));
                }
                out.push_str(&format!(
                    "  Default: '{}' → output '{}'\n\n",
                    default, default
                ));
            }
        }

        // Combine blocks render only when artifacts are threaded through
        // (`explain_with_artifacts` / `explain_full_with_artifacts`).
        // Without artifacts, `combine_predicates` is unreachable and the
        // detail block degrades to the summary line embedded in
        // `display_name()`'s header. The memory-limit argument is ignored
        // when artifacts is `None`; pass `0` rather than re-reading the
        // default to avoid a config-coupling cycle here.
        self.render_combine_section(&mut out, None, 0);

        self.render_retraction_section(&mut out, None);

        out
    }

    /// Render the retraction-cost block for content-relaxed aggregates
    /// and buffer-mode windows.
    ///
    /// `config` is `Some` when the caller went through one of the
    /// artifacts-aware entry points and can read the pipeline-level
    /// `correlation_fanout_policy` default plus the `correlation_key`
    /// used to classify aggregates. Without it the block still emits
    /// per-window detail; aggregate detection requires the correlation
    /// key, so the per-aggregate section is silent on the no-config
    /// rendering and the policy line degrades to "unknown at this
    /// rendering".
    ///
    /// The block is silent on pipelines whose every aggregate has
    /// `group_by ⊇ correlation_key` so strict-correlation and
    /// non-correlated `--explain` output stays identical to today's
    /// text.
    fn render_retraction_section(&self, out: &mut String, config: Option<&PipelineConfig>) {
        let mut relaxed_aggregates: Vec<(&str, &PlanNode)> = Vec::new();
        for &idx in &self.topo_order {
            if let PlanNode::Aggregation {
                name,
                config: agg_cfg,
                ..
            } = &self.graph[idx]
            {
                let parent_ck = self
                    .graph
                    .neighbors_directed(idx, petgraph::Direction::Incoming)
                    .next()
                    .and_then(|p| self.node_properties.get(&p))
                    .map(|p| p.ck_set.clone())
                    .unwrap_or_default();
                if group_by_omits_any_ck_field(&agg_cfg.group_by, &parent_ck) {
                    relaxed_aggregates.push((name.as_str(), &self.graph[idx]));
                }
            }
        }
        let buffer_mode_index_count = self
            .indices_to_build
            .iter()
            .filter(|s| s.requires_buffer_recompute)
            .count();

        if relaxed_aggregates.is_empty() && buffer_mode_index_count == 0 {
            return;
        }

        out.push_str("=== Retraction ===\n\n");

        let policy_line = match config.and_then(|c| c.error_handling.correlation_fanout_policy) {
            Some(p) => format!("{p:?}").to_lowercase(),
            None => {
                if config.is_some() {
                    "any (default)".to_string()
                } else {
                    "unknown at this rendering (config not threaded)".to_string()
                }
            }
        };
        out.push_str(&format!(
            "retraction enabled — {} relaxed aggregates, {} buffer-mode windows, fanout policy: {}.\n\n",
            relaxed_aggregates.len(),
            buffer_mode_index_count,
            policy_line,
        ));

        for (name, node) in &relaxed_aggregates {
            let PlanNode::Aggregation { compiled, .. } = node else {
                continue;
            };
            let path_label = if compiled.requires_buffer_mode {
                "BufferRequired"
            } else if compiled.requires_lineage {
                "Reversible"
            } else {
                // Unreachable on relaxed aggregates because the planner
                // sets exactly one of the two flags; surface the
                // inconsistency rather than fall back silently.
                "unclassified"
            };
            // Cardinality estimate is honest: today's planner has no
            // group-cardinality side-table to consult before the run,
            // so the lineage memory ceiling is expressed as a per-row
            // cost rather than a total. The `(input_row_id → group_index)`
            // pair is `(u32, u32)` = 8 bytes; per-group `input_rows` Vec
            // overhead is tracked alongside but its sum scales with the
            // unknown group cardinality.
            let lineage_per_row_bytes = if compiled.requires_lineage {
                "~8 bytes/row"
            } else {
                "n/a (buffer-mode holds raw contributions)"
            };
            out.push_str(&format!("Aggregate '{name}':\n"));
            out.push_str(&format!(
                "  retraction: relaxed-CK enabled, {path_label} accumulator path, lineage memory {lineage_per_row_bytes} (cardinality unknown at plan time), worst-case degrade-fallback: DLQ entire affected group when retract precondition breaks at runtime.\n",
            ));
            // Per-output-row cost of the synthetic shadow column the
            // relaxed aggregate emits. ~16 B is the Value::Integer
            // discriminant + the 8-byte i64 payload + the Vec slot
            // overhead per row; the column lives at the tail of every
            // emitted record and lifts the post-aggregate retract
            // fan-out path for downstream failures.
            out.push_str(&format!(
                "  synthetic CK: $ck.aggregate.{name} (engine-stamped, +16 B/output-row, hidden from default writers)\n",
            ));
            out.push('\n');
        }

        for (i, spec) in self.indices_to_build.iter().enumerate() {
            if !spec.requires_buffer_recompute {
                continue;
            }
            // Per-row buffer cost = sum of arena-field value sizes; the
            // arena uses `Value` so worst case is the per-row union of
            // `arena_fields` sizes. Surface the field count and the
            // `Value`-size order without a fake byte total because
            // partition cardinality is also unknown at plan time. The
            // arena field set itself appears in the upstream `Index`
            // block above; not duplicated here because its ordering is
            // deduplication-pass dependent and would be a noisy
            // snapshot diff target.
            let row_field_count = spec.arena_fields.len();
            out.push_str(&format!(
                "Window index [{i}] (root {}, partition_by {:?}):\n",
                format_index_root(&spec.root),
                spec.group_by,
            ));
            out.push_str(&format!(
                "  retraction: window buffer recompute, partition cardinality unknown at plan time, per-row buffer ~{row_field_count}× sizeof(Value).\n",
            ));
            out.push_str(
                "  worst-case partition memory ceiling under degrade: O(largest partition × per-row-size); degrade-fallback drops the affected partition's recompute and DLQ's its rows.\n",
            );
            out.push('\n');
        }

        // Deferred regions: every relaxed-CK aggregate's commit-time
        // sub-DAG. Sorted by producer name for deterministic snapshot
        // output. Walk the per-producer view (each region keys every
        // participating NodeIndex back to the same struct, so dedup
        // by producer).
        if !self.deferred_regions.is_empty() {
            let mut by_producer: std::collections::BTreeMap<
                String,
                &crate::plan::deferred_region::DeferredRegion,
            > = std::collections::BTreeMap::new();
            for region in self.deferred_regions.values() {
                let producer_name = self.graph[region.producer].name().to_string();
                by_producer.entry(producer_name).or_insert(region);
            }
            for (producer_name, region) in by_producer {
                let mut output_names: Vec<&str> = region
                    .outputs
                    .iter()
                    .map(|idx| self.graph[*idx].name())
                    .collect();
                output_names.sort();
                out.push_str(&format!(
                    "Deferred Region: {producer_name} → [{}]\n",
                    output_names.join(", ")
                ));
                out.push_str(&format!("  buffer_schema: {:?}\n", region.buffer_schema,));
                out.push('\n');
            }
        }
    }

    /// `--explain` text with combine multi-line blocks. Identical to
    /// [`Self::explain`] except that combine nodes — when paired with
    /// the `CompileArtifacts` that produced this DAG — render a full
    /// per-node block (strategy, inputs + roles, predicate decomposition
    /// detail, match/on-miss policy, planned-share memory budget). N-ary
    /// chains (`decomposed_from = Some(_)`) group under their original
    /// user-declared name with numbered step lines.
    pub fn explain_with_artifacts(
        &self,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        total_memory_limit_bytes: u64,
    ) -> String {
        // Build the base block (everything except combines) by reusing
        // `explain()` and then overwriting the combine section with the
        // artifacts-aware render. `explain()` calls
        // `render_combine_section(.., None, ..)` which is a no-op for
        // every combine node when artifacts are absent — so the output
        // of `explain()` already has no combine block to dedupe.
        let mut out = self.explain();
        self.render_combine_section(&mut out, Some(artifacts), total_memory_limit_bytes);
        out
    }

    /// Render every combine node's multi-line block into `out`.
    ///
    /// Walks `topo_order` and groups nodes by `decomposed_from` so an
    /// N-ary chain — N>2 user inputs, decomposed at plan time into a
    /// chain of binary combines that share the original user-declared
    /// name in `decomposed_from` — emits one group header followed by
    /// numbered `Step N:` lines. Singleton combines render their own
    /// block. Without `artifacts`, the function returns immediately
    /// (predicate detail is unreachable; the header line on
    /// `display_name()` is the only signal).
    fn render_combine_section(
        &self,
        out: &mut String,
        artifacts: Option<&crate::plan::bind_schema::CompileArtifacts>,
        total_memory_limit_bytes: u64,
    ) {
        let Some(artifacts) = artifacts else {
            return;
        };

        // Group decomposed combines under the original user-declared
        // name; singleton combines key by their own name.
        let mut combine_groups: IndexMap<String, Vec<NodeIndex>> = IndexMap::new();
        for &idx in &self.topo_order {
            if let PlanNode::Combine {
                name,
                decomposed_from,
                ..
            } = &self.graph[idx]
            {
                let key = decomposed_from.as_deref().unwrap_or(name).to_string();
                combine_groups.entry(key).or_default().push(idx);
            }
        }

        if combine_groups.is_empty() {
            return;
        }

        let combine_count_total: usize = combine_groups.values().map(|v| v.len()).sum();
        let planned_share =
            memory_budget_per_combine(total_memory_limit_bytes, combine_count_total);

        for (group_name, indices) in &combine_groups {
            if indices.len() > 1 {
                self.render_combine_group(out, group_name, indices, artifacts, planned_share);
            } else {
                self.render_combine_single(out, indices[0], artifacts, planned_share);
            }
        }
    }

    /// Render a singleton combine block (1:1 between user declaration
    /// and plan node). The full multi-line block: strategy, driving
    /// input + estimated rows, build inputs + roles, predicate
    /// summary + per-bucket detail (PostgreSQL 3-tier), match mode,
    /// on-miss policy, planned-share memory budget. Defensive paths
    /// handle a combine whose `combine_predicates` entry is missing
    /// (E303 fired at compile time) by rendering `<unselected>` /
    /// summary-only — mirrors `display_name()`'s `<unselected>`
    /// fallback for an unset driving input.
    fn render_combine_single(
        &self,
        out: &mut String,
        idx: NodeIndex,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        planned_share: u64,
    ) {
        let PlanNode::Combine {
            name,
            strategy,
            driving_input,
            build_inputs,
            predicate_summary,
            match_mode,
            on_miss,
            ..
        } = &self.graph[idx]
        else {
            return;
        };

        let inputs_for_node = artifacts.combine_inputs.get(name);

        out.push_str(&format!("Combine '{name}':\n"));
        out.push_str(&format!(
            "  Strategy: {}\n",
            combine_strategy_display(strategy)
        ));

        let drive_label = if driving_input.is_empty() {
            "<unselected>"
        } else {
            driving_input.as_str()
        };
        let drive_rows = format_estimated_rows(inputs_for_node.and_then(|m| m.get(drive_label)));
        out.push_str(&format!(
            "  Driving input: {drive_label} (probe, est. {drive_rows} rows)\n",
        ));

        for build_name in build_inputs {
            let role = describe_build_role(strategy, build_name);
            let rows =
                format_estimated_rows(inputs_for_node.and_then(|m| m.get(build_name.as_str())));
            out.push_str(&format!(
                "  Build input: {build_name} ({role}, est. {rows} rows)\n",
            ));
        }

        out.push_str(&format!(
            "  Predicate: equalities={}, ranges={}, residual={}\n",
            predicate_summary.equalities,
            predicate_summary.ranges,
            if predicate_summary.has_residual {
                "yes"
            } else {
                "no"
            }
        ));
        if let Some(decomposed) = artifacts.combine_predicates.get(name) {
            out.push_str(&decomposed.format_text());
        }

        out.push_str(&format!("  Match: {}\n", format_match_mode(*match_mode)));
        out.push_str(&format!("  On miss: {}\n", format_on_miss(*on_miss)));
        out.push_str(&format!(
            "  Memory budget (planned share): {}\n",
            format_memory_budget_line(planned_share),
        ));
        out.push('\n');
    }

    /// Render an N-ary decomposition group. The header reads
    /// `Combine '<original>' (N inputs, binary decomposition):`
    /// followed by one `Step k:` line per binary node in the chain.
    /// The full input set is recovered by walking the first step's
    /// `combine_inputs` entry (driver + build_inputs); subsequent
    /// steps re-use the previous step's output as a virtual input,
    /// so the visible "user input count" is `nodes.len() + 1`.
    fn render_combine_group(
        &self,
        out: &mut String,
        group_name: &str,
        indices: &[NodeIndex],
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        planned_share: u64,
    ) {
        let nary_inputs = indices.len() + 1;
        out.push_str(&format!(
            "Combine '{group_name}' ({nary_inputs} inputs, binary decomposition):\n",
        ));

        for (i, &idx) in indices.iter().enumerate() {
            let PlanNode::Combine {
                name: step_name,
                strategy,
                driving_input,
                build_inputs,
                predicate_summary,
                ..
            } = &self.graph[idx]
            else {
                continue;
            };

            let drive = if driving_input.is_empty() {
                "<unselected>"
            } else {
                driving_input.as_str()
            };
            let builds = if build_inputs.is_empty() {
                "<none>".to_string()
            } else {
                build_inputs.join(", ")
            };
            out.push_str(&format!(
                "  Step {}: {} ({} x {} ON {}) -> {}\n",
                i + 1,
                combine_strategy_tag(strategy),
                drive,
                builds,
                format_predicate_kind(predicate_summary),
                step_name,
            ));

            // Per-step predicate detail under the step line. The
            // detail is indented one more level than a singleton
            // block because each step is already nested under the
            // group header.
            if let Some(decomposed) = artifacts.combine_predicates.get(step_name) {
                let detail = decomposed.format_text();
                for line in detail.lines() {
                    out.push_str("  ");
                    out.push_str(line);
                    out.push('\n');
                }
            }
        }

        // Shared planned-share memory budget for the group.
        out.push_str(&format!(
            "  Memory budget (planned share, per step): {}\n",
            format_memory_budget_line(planned_share),
        ));
        out.push('\n');
    }

    /// Like [`Self::explain_full`] but emits the artifacts-aware
    /// per-combine multi-line block alongside the existing transform /
    /// sort / route blocks. Intended for the CLI `--explain` path
    /// where the caller already holds the [`crate::plan::CompiledPlan`]
    /// that produced this DAG.
    pub fn explain_full_with_artifacts(
        &self,
        config: &PipelineConfig,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
    ) -> String {
        let total_limit = crate::pipeline::memory::parse_memory_limit_bytes(
            config.pipeline.memory_limit.as_deref(),
        );
        let mut out = self.explain_with_artifacts(artifacts, total_limit);
        // Re-render the retraction section with the pipeline config in
        // scope so the fanout-policy line resolves to the user-visible
        // setting. `explain()` (called inside `explain_with_artifacts`)
        // already emitted a config-less variant; strip it before
        // re-rendering to avoid duplicate blocks.
        if let Some(start) = out.find("=== Retraction ===\n") {
            out.truncate(start);
        }
        self.render_retraction_section(&mut out, Some(config));
        self.append_full_sections(&mut out, config);
        out
    }

    /// Like [`Self::explain_text`] but routes through the artifacts-
    /// aware text formatter so combine blocks render with full per-
    /// node detail.
    pub fn explain_text_with_artifacts(
        &self,
        config: &PipelineConfig,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
    ) -> String {
        let mut out = self.explain_full_with_artifacts(config, artifacts);
        self.append_topology_section(&mut out);
        out
    }

    /// Append the `CXL Expressions`, `Type Annotations`, `Memory Budget`
    /// trailing sections that `explain_full` adds onto a base
    /// `explain()` body. Factored so the artifacts-aware variant can
    /// reuse them.
    fn append_full_sections(&self, out: &mut String, config: &PipelineConfig) {
        // CXL AST (reformatted expressions from config)
        out.push_str("=== CXL Expressions ===\n\n");
        for t in crate::executor::build_transform_specs(config) {
            out.push_str(&format!("Transform '{}':\n", t.name));
            for line in t.cxl_source().lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    out.push_str(&format!("  {}\n", trimmed));
                }
            }
            out.push('\n');
        }

        // Type annotations (inferred types per output field)
        out.push_str("=== Type Annotations ===\n\n");
        for node in self.graph.node_weights() {
            if let PlanNode::Transform { name, .. } = node {
                out.push_str(&format!(
                    "Transform '{name}': (types inferred at compile time)\n"
                ));
            }
        }
        out.push('\n');

        // Memory budget
        out.push_str("=== Memory Budget ===\n\n");
        let memory_limit = config
            .pipeline
            .concurrency
            .as_ref()
            .and_then(|c| c.threads)
            .map(|_| "configured")
            .unwrap_or("default");
        out.push_str(&format!("Memory limit: {}\n", memory_limit));
        out.push_str(&format!(
            "Worker threads: {}\n",
            self.parallelism.worker_threads
        ));
        out.push('\n');
    }

    /// Append the `DAG Topology` section that `explain_text` adds onto
    /// a base `explain_full` body. Factored so the artifacts-aware
    /// variant can reuse it without duplicating the topology walk.
    fn append_topology_section(&self, out: &mut String) {
        out.push_str("=== DAG Topology ===\n\n");
        for &idx in &self.topo_order {
            let node = &self.graph[idx];
            let line_suffix = match node.span().synthetic_line_number() {
                Some(line) => format!(" (line:{line})"),
                None => String::new(),
            };
            match node {
                PlanNode::Route {
                    name,
                    mode,
                    branches,
                    default,
                    ..
                } => {
                    let mode_str = match mode {
                        RouteMode::Exclusive => "exclusive",
                        RouteMode::Inclusive => "inclusive",
                    };
                    out.push_str(&format!(
                        "  ◆ FORK [route:{mode_str}] '{name}'{line_suffix}\n"
                    ));
                    for branch in branches {
                        out.push_str(&format!("  ├──> {branch} → {branch}\n"));
                    }
                    out.push_str(&format!("  ├──> default → {default}\n"));
                }
                PlanNode::Merge { name, .. } => {
                    out.push_str(&format!("  └──< MERGE '{name}'{line_suffix}\n"));
                }
                PlanNode::Aggregation { .. } => {
                    out.push_str(&format!("  ◇ {}{line_suffix}\n", node.display_name()));
                }
                PlanNode::Combine { .. } => {
                    out.push_str(&format!("  ◈ {}{line_suffix}\n", node.display_name()));
                }
                PlanNode::Source { .. }
                | PlanNode::Transform { .. }
                | PlanNode::Output { .. }
                | PlanNode::Sort { .. }
                | PlanNode::Composition { .. }
                | PlanNode::CorrelationCommit { .. } => {
                    let deps: Vec<String> = self
                        .graph
                        .neighbors_directed(idx, petgraph::Direction::Incoming)
                        .map(|pred| self.graph[pred].name().to_string())
                        .collect();
                    if deps.is_empty() {
                        out.push_str(&format!("  ● {}{line_suffix}\n", node.display_name()));
                    } else {
                        out.push_str(&format!("  │ {}{line_suffix}\n", node.display_name()));
                    }
                }
            }
        }
        out.push('\n');
    }

    /// Full `--explain` output combining execution plan with config context.
    pub fn explain_full(&self, config: &PipelineConfig) -> String {
        let mut out = self.explain();
        if let Some(start) = out.find("=== Retraction ===\n") {
            out.truncate(start);
        }
        self.render_retraction_section(&mut out, Some(config));

        // CXL AST (reformatted expressions from config)
        out.push_str("=== CXL Expressions ===\n\n");
        for t in crate::executor::build_transform_specs(config) {
            out.push_str(&format!("Transform '{}':\n", t.name));
            for line in t.cxl_source().lines() {
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    out.push_str(&format!("  {}\n", trimmed));
                }
            }
            out.push('\n');
        }

        // Type annotations (inferred types per output field)
        out.push_str("=== Type Annotations ===\n\n");
        for node in self.graph.node_weights() {
            if let PlanNode::Transform { name, .. } = node {
                out.push_str(&format!(
                    "Transform '{name}': (types inferred at compile time)\n"
                ));
            }
        }
        out.push('\n');

        // Memory budget
        out.push_str("=== Memory Budget ===\n\n");
        let memory_limit = config
            .pipeline
            .concurrency
            .as_ref()
            .and_then(|c| c.threads)
            .map(|_| "configured")
            .unwrap_or("default");
        out.push_str(&format!("Memory limit: {}\n", memory_limit));
        out.push_str(&format!(
            "Worker threads: {}\n",
            self.parallelism.worker_threads
        ));
        out.push('\n');

        out
    }

    /// Get all transform nodes from the graph in topological order.
    pub fn transform_nodes(&self) -> Vec<&PlanNode> {
        self.topo_order
            .iter()
            .filter_map(|&idx| {
                let node = &self.graph[idx];
                if matches!(node, PlanNode::Transform { .. }) {
                    Some(node)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Enhanced text output with branch/merge ASCII indicators.
    ///
    /// Fork points show `├──>` per branch, merge points show `└──<`.
    /// Linear nodes show `│` continuation.
    ///
    /// Rendering:
    ///   - Route and Aggregation both render as their own sibling line
    ///     at their topo position (no visual nesting).
    ///   - Each line is annotated with `(line:N)` when the underlying
    ///     `PlanNode.span` carries a known source line (via
    ///     [`crate::span::Span::synthetic_line_number`]).
    ///   - Route forks emit one `├──> branch → target` line per branch
    ///     (branch name is the `RouteBody.conditions` key, which is
    ///     also the downstream consumer node name).
    pub fn explain_text(&self, config: &PipelineConfig) -> String {
        let mut out = self.explain_full(config);

        out.push_str("=== DAG Topology ===\n\n");
        for &idx in &self.topo_order {
            let node = &self.graph[idx];
            let line_suffix = match node.span().synthetic_line_number() {
                Some(line) => format!(" (line:{line})"),
                None => String::new(),
            };
            match node {
                PlanNode::Route {
                    name,
                    mode,
                    branches,
                    default,
                    ..
                } => {
                    let mode_str = match mode {
                        RouteMode::Exclusive => "exclusive",
                        RouteMode::Inclusive => "inclusive",
                    };
                    out.push_str(&format!(
                        "  ◆ FORK [route:{mode_str}] '{name}'{line_suffix}\n"
                    ));
                    for branch in branches {
                        out.push_str(&format!("  ├──> {branch} → {branch}\n"));
                    }
                    out.push_str(&format!("  ├──> default → {default}\n"));
                }
                PlanNode::Merge { name, .. } => {
                    out.push_str(&format!("  └──< MERGE '{name}'{line_suffix}\n"));
                }
                PlanNode::Aggregation { .. } => {
                    // Sibling rendering: Aggregation gets its own topo
                    // line, not nested inside an upstream Transform.
                    out.push_str(&format!("  ◇ {}{line_suffix}\n", node.display_name()));
                }
                PlanNode::Combine { .. } => {
                    // Sibling rendering: combine appears on its own topo
                    // line; the `◈` glyph distinguishes it from Aggregate
                    // (`◇`), Route fork (`◆`), and Merge collector (`└──<`).
                    // `display_name()` carries the strategy/drive/build
                    // and predicate-shape suffix.
                    out.push_str(&format!("  ◈ {}{line_suffix}\n", node.display_name()));
                }
                PlanNode::Source { .. }
                | PlanNode::Transform { .. }
                | PlanNode::Output { .. }
                | PlanNode::Sort { .. }
                | PlanNode::Composition { .. }
                | PlanNode::CorrelationCommit { .. } => {
                    let deps: Vec<String> = self
                        .graph
                        .neighbors_directed(idx, petgraph::Direction::Incoming)
                        .map(|pred| self.graph[pred].name().to_string())
                        .collect();
                    if deps.is_empty() {
                        out.push_str(&format!("  ● {}{line_suffix}\n", node.display_name()));
                    } else {
                        out.push_str(&format!("  │ {}{line_suffix}\n", node.display_name()));
                    }
                }
            }
        }
        out.push('\n');
        out
    }

    /// Render the DAG as Graphviz DOT.
    pub fn explain_dot(&self) -> String {
        format!(
            "{:?}",
            petgraph::dot::Dot::with_attr_getters(
                &self.graph,
                &[
                    petgraph::dot::Config::EdgeNoLabel,
                    petgraph::dot::Config::NodeNoLabel,
                ],
                &|_, edge| { format!(r#"label="{}""#, edge.weight().dependency_type.as_str()) },
                &|_, (_, node)| { format!(r#"label="{}""#, dot_escape(&node.display_name())) },
            )
        )
    }
}

/// Render a [`PlanIndexRoot`] for `--explain` output. Source-rooted
/// indices show as `source(<name>)`, node-rooted as `node(<idx>)`,
/// parent-node-rooted as `parent_node(<idx>)`.
fn format_index_root(root: &crate::plan::index::PlanIndexRoot) -> String {
    match root {
        crate::plan::index::PlanIndexRoot::Source(name) => format!("source({name})"),
        crate::plan::index::PlanIndexRoot::Node { upstream, .. } => {
            format!("node({})", upstream.index())
        }
        crate::plan::index::PlanIndexRoot::ParentNode { upstream, .. } => {
            format!("parent_node({})", upstream.index())
        }
    }
}

/// Escape a string for Graphviz DOT attribute values.
fn dot_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

/// Custom Serialize: flat node-list with schema_version, id slugs, depends_on.
///
/// Includes a `node_properties` map keyed by node *name* (not `NodeIndex`),
/// ordered by topo position. Keying by name keeps the JSON contract stable
/// for downstream consumers (Kiln canvas, debugger, third-party tooling)
/// across recompiles where `NodeIndex` values change.
impl Serialize for ExecutionPlanDag {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("schema_version", "1")?;

        // Build node list in topo order
        struct NodeList<'a>(&'a ExecutionPlanDag);
        impl<'a> Serialize for NodeList<'a> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let dag = self.0;
                let mut seq = serializer.serialize_seq(Some(dag.topo_order.len()))?;
                for &idx in &dag.topo_order {
                    let node = &dag.graph[idx];
                    // Collect depends_on from incoming edges
                    let depends_on: Vec<String> = dag
                        .graph
                        .neighbors_directed(idx, petgraph::Direction::Incoming)
                        .map(|pred| dag.graph[pred].id_slug())
                        .collect();

                    let entry = NodeEntry {
                        node,
                        depends_on: &depends_on,
                    };
                    seq.serialize_element(&entry)?;
                }
                seq.end()
            }
        }

        map.serialize_entry("nodes", &NodeList(self))?;

        // node_properties keyed by node name, in topo order. Built as an
        // IndexMap so the JSON object preserves topo iteration order.
        struct PropsMap<'a>(&'a ExecutionPlanDag);
        impl<'a> Serialize for PropsMap<'a> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let dag = self.0;
                let mut m = serializer.serialize_map(Some(dag.topo_order.len()))?;
                for &idx in &dag.topo_order {
                    let name = dag.graph[idx].name();
                    if let Some(props) = dag.node_properties.get(&idx) {
                        m.serialize_entry(name, props)?;
                    }
                }
                m.end()
            }
        }
        map.serialize_entry("node_properties", &PropsMap(self))?;
        map.end()
    }
}

/// Helper for JSON node serialization.
#[derive(Serialize)]
struct NodeEntry<'a> {
    #[serde(flatten)]
    node: &'a PlanNode,
    depends_on: &'a Vec<String>,
}

/// `--explain --format json` view: pairs an [`ExecutionPlanDag`] with
/// the [`crate::plan::bind_schema::CompileArtifacts`] that produced it
/// so each combine node serializes with full predicate detail
/// (`equalities[].left/.right`, `ranges[].left/.op/.right`, residual
/// flag), per-input role + estimated row count, and the planned-share
/// memory budget bytes. Round-trips strictly through `serde::Serialize`
/// (no `Deserialize`) — the JSON channel is consumer-only (Kiln canvas,
/// third-party tooling).
///
/// Keeping the wrapper out of `ExecutionPlanDag` itself preserves the
/// existing `serde_json::to_value(&dag)` / `to_string_pretty(&dag)`
/// callers (which neither receive nor want artifacts) without bumping
/// the JSON `schema_version`.
pub struct ExplainJson<'a> {
    dag: &'a ExecutionPlanDag,
    artifacts: &'a crate::plan::bind_schema::CompileArtifacts,
}

impl<'a> ExplainJson<'a> {
    /// Build an artifacts-aware JSON view of an execution plan.
    pub fn new(
        dag: &'a ExecutionPlanDag,
        artifacts: &'a crate::plan::bind_schema::CompileArtifacts,
    ) -> Self {
        Self { dag, artifacts }
    }
}

impl<'a> Serialize for ExplainJson<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(3))?;
        map.serialize_entry("schema_version", "1")?;

        // Combine-aware node list. Walks `topo_order` and emits a
        // `CombineNodeEntry` for combine variants (carrying the
        // artifacts-derived fields), falling back to the plain
        // `NodeEntry` for everything else.
        struct EnrichedNodeList<'a> {
            dag: &'a ExecutionPlanDag,
            artifacts: &'a crate::plan::bind_schema::CompileArtifacts,
            combine_count_total: usize,
            total_memory_limit: u64,
        }
        impl<'a> Serialize for EnrichedNodeList<'a> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let dag = self.dag;
                let mut seq = serializer.serialize_seq(Some(dag.topo_order.len()))?;
                let planned_share =
                    memory_budget_per_combine(self.total_memory_limit, self.combine_count_total);
                for &idx in &dag.topo_order {
                    let node = &dag.graph[idx];
                    let depends_on: Vec<String> = dag
                        .graph
                        .neighbors_directed(idx, petgraph::Direction::Incoming)
                        .map(|pred| dag.graph[pred].id_slug())
                        .collect();
                    if matches!(node, PlanNode::Combine { .. }) {
                        let entry = CombineNodeEntry {
                            node,
                            depends_on: &depends_on,
                            artifacts: self.artifacts,
                            memory_budget_bytes: planned_share,
                        };
                        seq.serialize_element(&entry)?;
                    } else {
                        let entry = NodeEntry {
                            node,
                            depends_on: &depends_on,
                        };
                        seq.serialize_element(&entry)?;
                    }
                }
                seq.end()
            }
        }

        let combine_count_total = self
            .dag
            .graph
            .node_weights()
            .filter(|n| matches!(n, PlanNode::Combine { .. }))
            .count();
        // The pipeline-level memory limit is not threaded into the
        // serializer; the JSON view always emits the per-combine
        // share derived from the global default (512MB) divided by
        // the combine count. Callers that have already overridden
        // the limit see the correct share through the text path.
        let total_memory_limit = crate::pipeline::memory::parse_memory_limit_bytes(None);

        map.serialize_entry(
            "nodes",
            &EnrichedNodeList {
                dag: self.dag,
                artifacts: self.artifacts,
                combine_count_total,
                total_memory_limit,
            },
        )?;

        // node_properties keyed by node name, in topo order. Mirrors
        // the plain `ExecutionPlanDag` Serialize body.
        struct PropsMap<'a>(&'a ExecutionPlanDag);
        impl<'a> Serialize for PropsMap<'a> {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                let dag = self.0;
                let mut m = serializer.serialize_map(Some(dag.topo_order.len()))?;
                for &idx in &dag.topo_order {
                    let name = dag.graph[idx].name();
                    if let Some(props) = dag.node_properties.get(&idx) {
                        m.serialize_entry(name, props)?;
                    }
                }
                m.end()
            }
        }
        map.serialize_entry("node_properties", &PropsMap(self.dag))?;
        map.end()
    }
}

/// Per-Combine-node JSON entry carrying the artifacts-derived
/// predicate detail (`equalities[].left/.right`, `ranges[]`,
/// `has_residual`), per-input role + cardinality, and the
/// planned-share `memory_budget_bytes`. Flattens the underlying
/// `PlanNode::Combine` shape to preserve every field the plain
/// `ExecutionPlanDag` Serialize emits (strategy, driving_input,
/// build_inputs, predicate_summary, match_mode, on_miss,
/// decomposed_from) — extension only, no replacement.
struct CombineNodeEntry<'a> {
    node: &'a PlanNode,
    depends_on: &'a Vec<String>,
    artifacts: &'a crate::plan::bind_schema::CompileArtifacts,
    memory_budget_bytes: u64,
}

impl<'a> Serialize for CombineNodeEntry<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // Re-serialize the underlying PlanNode to a serde_json::Value
        // so we can then layer the combine-specific extras on top.
        // Going through Value here is the only way to mix derived-
        // Serialize fields with manually-emitted ones without
        // duplicating the variant's field list (which would drift
        // the moment a new field is added to PlanNode::Combine).
        let mut base = serde_json::to_value(self.node).map_err(serde::ser::Error::custom)?;
        let obj = base.as_object_mut().ok_or_else(|| {
            serde::ser::Error::custom("PlanNode::Combine did not serialize as a JSON object")
        })?;

        // depends_on (matches NodeEntry behavior).
        obj.insert(
            "depends_on".to_string(),
            serde_json::Value::Array(
                self.depends_on
                    .iter()
                    .map(|s| serde_json::Value::String(s.clone()))
                    .collect(),
            ),
        );

        // Combine-specific extras. Pull from the variant fields plus
        // the artifacts side-table.
        if let PlanNode::Combine {
            name,
            strategy,
            driving_input,
            build_inputs,
            ..
        } = self.node
        {
            // Rich predicate detail.
            let predicate_value = build_predicate_json(self.artifacts.combine_predicates.get(name));
            obj.insert("predicate".to_string(), predicate_value);

            // Per-input role + estimated rows.
            let inputs_value = build_inputs_json(
                self.artifacts.combine_inputs.get(name),
                driving_input,
                build_inputs,
                strategy,
            );
            obj.insert("inputs".to_string(), inputs_value);

            // Memory budget (planned share).
            obj.insert(
                "memory_budget_bytes".to_string(),
                serde_json::Value::Number(self.memory_budget_bytes.into()),
            );
        }

        base.serialize(serializer)
    }
}

/// Build the rich `predicate` JSON object for a combine node:
/// `{ equalities: [{left, right}], ranges: [{left, op, right}], has_residual }`.
/// Returns the empty-shape object when `decomposed` is `None` (combine
/// whose decomposition failed at compile time — E303 / E308).
fn build_predicate_json(
    decomposed: Option<&crate::plan::combine::DecomposedPredicate>,
) -> serde_json::Value {
    use serde_json::{Map, Value};
    let mut obj = Map::new();
    let (eqs, ranges, has_residual) = match decomposed {
        Some(d) => {
            let eqs: Vec<Value> = d
                .equalities
                .iter()
                .map(|eq| {
                    let mut m = Map::new();
                    m.insert(
                        "left".to_string(),
                        Value::String(combine_operand_qualified(&eq.left_expr, &eq.left_input)),
                    );
                    m.insert(
                        "right".to_string(),
                        Value::String(combine_operand_qualified(&eq.right_expr, &eq.right_input)),
                    );
                    Value::Object(m)
                })
                .collect();
            let ranges: Vec<Value> = d
                .ranges
                .iter()
                .map(|r| {
                    let mut m = Map::new();
                    m.insert(
                        "left".to_string(),
                        Value::String(combine_operand_qualified(&r.left_expr, &r.left_input)),
                    );
                    m.insert(
                        "op".to_string(),
                        Value::String(range_op_label(r.op).to_string()),
                    );
                    m.insert(
                        "right".to_string(),
                        Value::String(combine_operand_qualified(&r.right_expr, &r.right_input)),
                    );
                    Value::Object(m)
                })
                .collect();
            (eqs, ranges, d.residual.is_some())
        }
        None => (Vec::new(), Vec::new(), false),
    };
    obj.insert("equalities".to_string(), Value::Array(eqs));
    obj.insert("ranges".to_string(), Value::Array(ranges));
    obj.insert("has_residual".to_string(), Value::Bool(has_residual));
    Value::Object(obj)
}

/// Build the `inputs` JSON map for a combine node:
/// `{ <qualifier>: { role, estimated_rows } }`. `role` is `"probe"`
/// for the driver, `"build"` (or `"build (sorted scan)"` for sort-merge)
/// for the rest. `estimated_rows` is JSON `null` when the planner has
/// no cardinality estimate — V-8-2 honest output, matching every
/// surveyed engine's behavior in absence of statistics.
fn build_inputs_json(
    inputs_for_node: Option<&IndexMap<String, crate::plan::combine::CombineInput>>,
    driving_input: &str,
    build_inputs: &[String],
    strategy: &crate::plan::combine::CombineStrategy,
) -> serde_json::Value {
    use serde_json::{Map, Value};
    let mut obj = Map::new();

    let mut emit_one = |name: &str, role: &str| {
        let est = inputs_for_node
            .and_then(|m| m.get(name))
            .and_then(|ci| ci.estimated_cardinality);
        let est_value = match est {
            Some(n) => Value::Number(n.into()),
            None => Value::Null,
        };
        let mut entry = Map::new();
        entry.insert("role".to_string(), Value::String(role.to_string()));
        entry.insert("estimated_rows".to_string(), est_value);
        obj.insert(name.to_string(), Value::Object(entry));
    };

    if !driving_input.is_empty() {
        emit_one(driving_input, "probe");
    }
    let build_role = describe_build_role(strategy, "");
    for build in build_inputs {
        emit_one(build.as_str(), build_role);
    }
    Value::Object(obj)
}

/// Stable string label for a `RangeOp` in JSON output.
fn range_op_label(op: crate::plan::combine::RangeOp) -> &'static str {
    use crate::plan::combine::RangeOp;
    match op {
        RangeOp::Lt => "lt",
        RangeOp::Le => "le",
        RangeOp::Gt => "gt",
        RangeOp::Ge => "ge",
    }
}

/// Render a conjunct operand `Expr` as a fully-qualified
/// `<input>.<field>` string for JSON output. Non-trivial expressions
/// (function calls, arithmetic) render as `"<input>.<expr>"` so the
/// consumer can still bucket the operand by its driving input even
/// when the underlying expression is opaque.
fn combine_operand_qualified(expr: &cxl::ast::Expr, input: &std::sync::Arc<str>) -> String {
    use cxl::ast::Expr;
    match expr {
        Expr::QualifiedFieldRef { parts, .. } => parts
            .iter()
            .map(|p| p.as_ref())
            .collect::<Vec<_>>()
            .join("."),
        _ => format!("{}.<expr>", input),
    }
}

/// Compile-time guard: every `PlanNode::Composition` must have all of
/// its incoming edges tagged with [`PlanEdge::port`]. The dispatcher's
/// `collect_port_records` resolves composition inputs by reading those
/// tags off the live edge graph; an untagged edge means a planner pass
/// spliced an intermediate node between a producer and a composition
/// without preserving the tag, and would silently drop records at
/// dispatch. Surfacing this at compile time lets users see E152 instead
/// of a confusing runtime `PipelineError::Internal`.
///
/// Walks `dag.graph` and every body's mini-DAG in
/// `artifacts.composition_bodies` (nested compositions live there).
/// Returns one diagnostic per offending edge.
pub(crate) fn diagnose_untagged_composition_edges(
    dag: &ExecutionPlanDag,
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
) -> Vec<crate::error::Diagnostic> {
    use crate::error::{Diagnostic, LabeledSpan};
    use petgraph::Direction;
    use petgraph::visit::EdgeRef;
    fn check(graph: &DiGraph<PlanNode, PlanEdge>, scope_label: &str, out: &mut Vec<Diagnostic>) {
        for idx in graph.node_indices() {
            let PlanNode::Composition {
                name: comp_name,
                span,
                ..
            } = &graph[idx]
            else {
                continue;
            };
            for edge in graph.edges_directed(idx, Direction::Incoming) {
                if edge.weight().port.is_some() {
                    continue;
                }
                let producer = graph[edge.source()].name().to_string();
                let err = PlanError::CompositionUntaggedIncomingEdge {
                    composition: comp_name.clone(),
                    producer,
                    scope: scope_label.to_string(),
                };
                out.push(Diagnostic::error(
                    "E152",
                    err.to_string(),
                    LabeledSpan::primary(*span, String::new()),
                ));
            }
        }
    }
    let mut out = Vec::new();
    check(&dag.graph, "top-level", &mut out);
    for (body_id, body) in &artifacts.composition_bodies {
        check(&body.graph, &format!("body {}", body_id.0), &mut out);
    }
    out
}

/// Extract the cycle path from a DFS back-edge detection.
///
/// Uses `depth_first_search` with `DfsEvent::BackEdge` + predecessor map
/// to extract the full cycle path. Formats as `"A" --> "B" --> "A"`.
pub(crate) fn extract_cycle_path(graph: &DiGraph<PlanNode, PlanEdge>, start: NodeIndex) -> String {
    let mut predecessors: HashMap<NodeIndex, NodeIndex> = HashMap::new();
    let mut cycle_edge: Option<(NodeIndex, NodeIndex)> = None;

    depth_first_search(graph, Some(start), |event| match event {
        DfsEvent::TreeEdge(u, v) => {
            predecessors.insert(v, u);
            Control::<()>::Continue
        }
        DfsEvent::BackEdge(u, v) => {
            cycle_edge = Some((u, v));
            Control::Break(())
        }
        _ => Control::Continue,
    });

    if let Some((from, to)) = cycle_edge {
        // Walk back from `from` to `to` to get the cycle path
        let mut path = vec![graph[from].name().to_string()];
        let mut current = from;
        while current != to {
            if let Some(&pred) = predecessors.get(&current) {
                current = pred;
                path.push(graph[current].name().to_string());
            } else {
                break;
            }
        }
        path.reverse();
        // Close the cycle
        path.push(path[0].clone());
        path.iter()
            .map(|n| format!("\"{}\"", n))
            .collect::<Vec<_>>()
            .join(" --> ")
    } else {
        format!("\"{}\"", graph[start].name())
    }
}

/// Assign tiers via BFS: each node's tier = max(predecessor tiers) + 1.
pub(crate) fn assign_tiers(graph: &mut DiGraph<PlanNode, PlanEdge>, topo_order: &[NodeIndex]) {
    let mut tiers: HashMap<NodeIndex, u32> = HashMap::new();

    for &idx in topo_order {
        let max_pred_tier = graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .filter_map(|pred| tiers.get(&pred))
            .max()
            .copied();

        let tier = match max_pred_tier {
            Some(t) => t + 1,
            None => 0, // Root node (source)
        };
        tiers.insert(idx, tier);

        // Update the tier field on Transform nodes
        if let PlanNode::Transform {
            tier: ref mut node_tier,
            ..
        } = graph[idx]
        {
            *node_tier = tier;
        }
    }
}

/// Derive ParallelismClass from analyzer output and window config.
pub(crate) fn derive_parallelism_class(
    analysis: &cxl::analyzer::TransformAnalysis,
    wc: &Option<LocalWindowConfig>,
    primary_source: &str,
) -> ParallelismClass {
    match analysis.parallelism_hint {
        ParallelismHint::Stateless => ParallelismClass::Stateless,
        ParallelismHint::IndexReading => {
            if let Some(wc) = wc {
                let source = wc
                    .source
                    .clone()
                    .unwrap_or_else(|| primary_source.to_string());
                if source != primary_source {
                    ParallelismClass::CrossSource
                } else {
                    ParallelismClass::IndexReading
                }
            } else {
                ParallelismClass::IndexReading
            }
        }
        ParallelismHint::Sequential => ParallelismClass::Sequential,
    }
}

/// One tier of the source dependency DAG. Sources within a tier are independent.
#[derive(Debug, Clone)]
pub struct SourceTier {
    pub sources: Vec<String>,
}

/// How to look up a record's partition during Phase 2.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionLookupKind {
    /// Same-source: extract group_by fields directly from the current record.
    SameSource,
    /// Cross-source: evaluate the `on` expression against the current record.
    CrossSource { on_expr: Option<String> },
}

/// AST compiler's parallelism classification per transform.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ParallelismClass {
    /// No window references — fully parallelizable.
    Stateless,
    /// Reads from immutable arena — parallelizable across chunks.
    IndexReading,
    /// Positional functions with ordering dependency — single-threaded.
    Sequential,
    /// References a different source's index.
    CrossSource,
}

/// Per-output projection specification.
#[derive(Debug, Clone)]
pub struct OutputSpec {
    pub name: String,
    pub mapping: IndexMap<String, String>,
    pub exclude: Vec<String>,
    pub include_unmapped: bool,
}

/// Pipeline-level parallelism configuration.
#[derive(Debug, Clone)]
pub struct ParallelismProfile {
    pub per_transform: Vec<ParallelismClass>,
    pub worker_threads: usize,
}

/// Build the source dependency DAG.
///
/// Reference sources (those targeted by cross-source windows) must be in
/// earlier tiers than the transforms that depend on them.
pub(crate) fn build_source_dag(
    sources: &[SourceConfig],
    window_configs: &[Option<LocalWindowConfig>],
    primary_source: &str,
) -> Result<Vec<SourceTier>, PlanError> {
    let all_sources: Vec<String> = sources.iter().map(|i| i.name.clone()).collect();

    if all_sources.len() <= 1 {
        // Single source — trivial DAG
        return Ok(vec![SourceTier {
            sources: all_sources,
        }]);
    }

    // Collect which sources are dependencies (referenced by cross-source windows)
    let mut dependencies: HashSet<String> = HashSet::new();
    for wc in window_configs.iter().flatten() {
        if let Some(source) = &wc.source
            && source != primary_source
        {
            dependencies.insert(source.clone());
        }
    }

    // Tier 0: reference sources (must be built first)
    // Tier 1: everything else (including primary)
    let tier0: Vec<String> = all_sources
        .iter()
        .filter(|s| dependencies.contains(s.as_str()))
        .cloned()
        .collect();

    let tier1: Vec<String> = all_sources
        .iter()
        .filter(|s| !dependencies.contains(s.as_str()))
        .cloned()
        .collect();

    let mut tiers = Vec::new();
    if !tier0.is_empty() {
        tiers.push(SourceTier { sources: tier0 });
    }
    if !tier1.is_empty() {
        tiers.push(SourceTier { sources: tier1 });
    }

    Ok(tiers)
}

/// Tier index of `source_name` in a `Vec<SourceTier>` (output of
/// [`build_source_dag`]), or `None` if the name is not present in any
/// tier. Used by E150c diagnostic emission to compare two sources'
/// ingestion ordering: lower index = earlier tier.
pub fn source_tier_index(tiers: &[SourceTier], source_name: &str) -> Option<usize> {
    tiers
        .iter()
        .position(|t| t.sources.iter().any(|s| s == source_name))
}

/// Walk back from a window-bearing Transform through the DAG and return
/// the name of the [`PlanNode::Source`] feeding its primary input chain.
///
/// Pass-through operators ([`PlanNode::Sort`], [`PlanNode::Route`]) and
/// schema-changing operators (Aggregation, Combine, Transform, Merge,
/// Composition) are walked through by following the first incoming
/// edge. Returns `None` if the walk reaches a node with no incoming
/// edges that is not a `Source` (disconnected node), or hits a
/// [`PlanNode::Merge`] whose inputs come from multiple distinct
/// source roots (no single primary source).
pub fn primary_input_source_for_transform(
    graph: &DiGraph<PlanNode, PlanEdge>,
    start: NodeIndex,
) -> Option<String> {
    let mut cursor = start;
    loop {
        if let PlanNode::Source { name, .. } = &graph[cursor] {
            return Some(name.clone());
        }
        let mut incoming = graph.neighbors_directed(cursor, petgraph::Direction::Incoming);
        let parent = incoming.next()?;
        cursor = parent;
    }
}

/// Walk one incoming edge per step from `start` past pass-through nodes
/// ([`PlanNode::Sort`], [`PlanNode::Route`]) and return the first ancestor
/// that actually changes the row stream's schema or row count.
///
/// Sort and Route are pass-through with respect to a windowed Transform's
/// rooting decision: a Sort merely reorders, a Route merely partitions a
/// shared row stream. The window's arena+index pair must root at whatever
/// upstream operator is actually producing the rows the window will see —
/// the Sort/Route is just an in-flight transform of those same rows.
///
/// Returns `start` itself if `start` has zero incoming edges (a Source
/// with no upstream, or a disconnected node). Returns the first
/// non-pass-through ancestor otherwise. If the walk encounters a
/// pass-through with multiple incoming edges (in-pipeline branching),
/// returns that pass-through node — the caller must treat that as a
/// rooting boundary because the row stream loses single-producer
/// identity past that point.
pub fn first_non_passthrough_ancestor(
    graph: &DiGraph<PlanNode, PlanEdge>,
    start: NodeIndex,
) -> NodeIndex {
    let mut current = start;
    loop {
        let is_passthrough = matches!(
            graph[current],
            PlanNode::Sort { .. } | PlanNode::Route { .. }
        );
        if !is_passthrough {
            return current;
        }
        let mut incoming = graph.neighbors_directed(current, petgraph::Direction::Incoming);
        let Some(parent) = incoming.next() else {
            return current;
        };
        if incoming.next().is_some() {
            // Multiple incoming edges into a pass-through — rooting
            // boundary. The window must root here, not past it.
            return current;
        }
        current = parent;
    }
}

/// Resolve composition-body analytic-window IndexSpecs against the
/// fully-built parent DAG.
///
/// Body lowering (`bind_composition`) runs before the parent DAG's
/// NodeIndex space is allocated, so it cannot stamp `window_index`
/// onto body Transform nodes whose first non-pass-through ancestor
/// reaches the body's `input:` port — that port resolves to a
/// parent-DAG operator only after Stage 5 has built the parent's
/// `name_to_idx`. This pass closes that gap: per body, per Transform
/// carrying a captured `body_window_configs` entry, walk
/// `first_non_passthrough_ancestor` through the body graph, classify
/// the rooting (`Source` / `Node` / `ParentNode`), construct the
/// `RawIndexRequest` set, deduplicate it, and backfill `window_index`
/// onto the body Transform.
///
/// Cross-body recursion (composition-of-composition) is handled by
/// running the pass per-body; nested bodies' parent context is the
/// enclosing body's mini-DAG plus any ancestor port resolutions
/// already baked into the encoding scope's index list.
pub(crate) fn resolve_composition_body_windows(
    parent_dag: &ExecutionPlanDag,
    artifacts: &mut crate::plan::bind_schema::CompileArtifacts,
    diags: &mut Vec<crate::error::Diagnostic>,
) {
    use crate::error::{Diagnostic, LabeledSpan};
    use crate::plan::index::{
        IndexSpec, PlanIndexRoot, RawIndexRequest, deduplicate_indices, find_index_for,
    };
    use crate::span::Span as PlanSpan;

    // Two-step ownership shuffle: (1) compute every body's
    // (idx_specs, window_index_assignments) without holding a mutable
    // borrow on `artifacts`, then (2) write the results back.
    // `artifacts.composition_bodies` holds `BoundBody` values keyed
    // by `CompositionBodyId`; the immutable borrow during compute
    // also covers parent_dag access.
    let body_ids: Vec<crate::plan::composition_body::CompositionBodyId> =
        artifacts.composition_bodies.keys().copied().collect();

    for body_id in body_ids {
        let Some(body) = artifacts.composition_bodies.get(&body_id) else {
            continue;
        };

        // Build a parent-DAG name → NodeIndex map once per body so
        // later port resolutions don't re-walk the parent graph for
        // every window. Top-level lookups walk parent_dag; nested-
        // body lookups would need the enclosing body's mini-DAG, but
        // for this pass the parent context is always the top-level
        // DAG — nested ParentNode rooting through multiple body
        // layers is an extension that the dispatcher's
        // `current_body_node_input_refs` plumbing already handles via
        // active_stack walks; the spec list emitted here points at the
        // most-immediate parent-DAG operator. Top-level pipelines
        // dominate the production geometry so this is the load-
        // bearing case.
        let mut parent_name_to_idx: std::collections::HashMap<&str, NodeIndex> =
            std::collections::HashMap::new();
        for idx in parent_dag.graph.node_indices() {
            parent_name_to_idx.insert(parent_dag.graph[idx].name(), idx);
        }

        // Find this body's call-site composition node in the parent
        // DAG. Multi-call-site signatures (the same .comp.yaml
        // referenced by N composition nodes) bind to N distinct
        // `CompositionBodyId`s — `composition_body_assignments` keys
        // by composition node name, not body file path, so each
        // body has exactly one composition node.
        let mut composition_idx: Option<NodeIndex> = None;
        for (comp_name, &assigned_id) in &artifacts.composition_body_assignments {
            if assigned_id == body_id
                && let Some(&idx) = parent_name_to_idx.get(comp_name.as_str())
            {
                composition_idx = Some(idx);
                break;
            }
        }
        let Some(composition_idx) = composition_idx else {
            // No call site — body is dead code. Skip.
            continue;
        };

        // Per-port → parent-DAG NodeIndex via the composition's
        // incoming port-tagged edges.
        let mut port_to_parent_idx: std::collections::HashMap<&str, NodeIndex> =
            std::collections::HashMap::new();
        use petgraph::visit::EdgeRef;
        for edge in parent_dag
            .graph
            .edges_directed(composition_idx, petgraph::Direction::Incoming)
        {
            if let Some(port_name) = edge.weight().port.as_deref() {
                port_to_parent_idx.insert(port_name, edge.source());
            }
        }

        // Per-Transform spec construction. Walk in declaration order
        // (HashMap iteration order is undefined but stable per
        // process; we collect names sorted for determinism into the
        // explain output downstream).
        let mut window_names: Vec<&str> = body
            .body_window_configs
            .keys()
            .map(|s| s.as_str())
            .collect();
        window_names.sort_unstable();

        let mut raw_requests: Vec<(RawIndexRequest, NodeIndex)> = Vec::new();

        for transform_name in &window_names {
            let Some(wc) = body.body_window_configs.get(*transform_name) else {
                continue;
            };
            let Some(&transform_idx) = body.name_to_idx.get(*transform_name) else {
                continue;
            };

            let mut arena_fields: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for gb in &wc.group_by {
                arena_fields.insert(gb.clone());
            }
            for sf in &wc.sort_by {
                arena_fields.insert(sf.field.clone());
            }
            // Pull every field a body Transform references through
            // window builtins or bare FieldRefs — e.g.
            // `$window.sum(amount)` adds `amount`, and `emit x = y` adds
            // `y`. The analyzer is the same one consumed at top-level
            // lowering (`config/mod.rs`); body Transforms register their
            // typed programs in `artifacts.typed` keyed by the body
            // node's name, so the analyzer call here mirrors the
            // top-level shape exactly.
            if let Some(typed) = artifacts.typed.get(*transform_name) {
                let analysis = cxl::analyzer::analyze_transform(transform_name, typed);
                for f in &analysis.accessed_fields {
                    arena_fields.insert(f.clone());
                }
            }
            let mut arena_fields_vec: Vec<String> = arena_fields.into_iter().collect();
            arena_fields_vec.sort();

            // Body-internal first non-pass-through ancestor.
            let pred_idx = match body
                .graph
                .neighbors_directed(transform_idx, petgraph::Direction::Incoming)
                .next()
            {
                Some(p) => p,
                None => continue,
            };
            let rooted_idx = first_non_passthrough_ancestor(&body.graph, pred_idx);
            let rooted_node = &body.graph[rooted_idx];

            // Classify the rooting:
            // - Body Source whose name matches a port → ParentNode.
            // - Body Source not a port (declared inside body) →
            //   Source(name).
            // - Other body operator → Node{ body_upstream, .. }.
            let root: PlanIndexRoot = match rooted_node {
                PlanNode::Source { name, .. } => {
                    if let Some(&parent_upstream) = port_to_parent_idx.get(name.as_str()) {
                        let anchor_schema = match parent_dag.graph[parent_upstream]
                            .stored_output_schema()
                            .cloned()
                        {
                            Some(s) => s,
                            None => {
                                diags.push(Diagnostic::error(
                                    "E003",
                                    format!(
                                        "composition body windowed transform {transform_name:?} \
                                         resolves through input port {name:?} to parent operator {:?} \
                                         which has no output schema",
                                        parent_dag.graph[parent_upstream].name()
                                    ),
                                    LabeledSpan::primary(PlanSpan::SYNTHETIC, String::new()),
                                ));
                                continue;
                            }
                        };
                        PlanIndexRoot::ParentNode {
                            upstream: parent_upstream,
                            anchor_schema,
                        }
                    } else {
                        // Body-declared Source — rare but legal.
                        PlanIndexRoot::Source(name.clone())
                    }
                }
                PlanNode::Merge { .. } => {
                    diags.push(Diagnostic::error(
                        "E150d",
                        format!(
                            "composition body windowed transform {transform_name:?} \
                             is rooted at a Merge node; Merge concatenates streams \
                             without a single producer identity, so a window cannot \
                             anchor to it"
                        ),
                        LabeledSpan::primary(PlanSpan::SYNTHETIC, String::new()),
                    ));
                    continue;
                }
                other => {
                    let Some(anchor_schema) = other.stored_output_schema().cloned() else {
                        continue;
                    };
                    // E150b — every arena field must be present in the
                    // upstream operator's output schema. The top-level
                    // lowering enforces this at `config/mod.rs`; body
                    // windows enforce it here so body-internal
                    // post-aggregate windows that reference a column
                    // the aggregate did not emit fail at compile rather
                    // than silently reading Null at runtime.
                    let mut e150b_fired = false;
                    for f in &arena_fields_vec {
                        if !anchor_schema.contains(f.as_str()) {
                            diags.push(Diagnostic::error(
                                "E150b",
                                format!(
                                    "composition body windowed transform {transform_name:?} \
                                     references field {f:?} that the upstream operator {:?} \
                                     does not emit; a node-rooted window can only see \
                                     columns produced by its rooted operator",
                                    other.name()
                                ),
                                LabeledSpan::primary(PlanSpan::SYNTHETIC, String::new()),
                            ));
                            e150b_fired = true;
                        }
                    }
                    if e150b_fired {
                        continue;
                    }
                    PlanIndexRoot::Node {
                        upstream: rooted_idx,
                        anchor_schema,
                    }
                }
            };

            // Body Sources do not declare top-level sort_order today;
            // node-rooted / parent-node-rooted arenas sort partitions
            // post-build at the upstream-arm exit anyway. Treat body
            // windows as `already_sorted = false` uniformly.
            let already_sorted = false;

            let req = RawIndexRequest {
                root,
                group_by: wc.group_by.clone(),
                sort_by: wc.sort_by.clone(),
                arena_fields: arena_fields_vec,
                already_sorted,
                // `transform_index` indexes into a per-body Vec; the
                // body has no top-level "transform list" alongside it,
                // so we record the body NodeIndex's underlying integer
                // for traceability. The dedup pass uses (root,
                // group_by, sort_by) regardless.
                transform_index: transform_idx.index(),
                requires_buffer_recompute: false,
            };
            raw_requests.push((req, transform_idx));
        }

        let request_only: Vec<RawIndexRequest> =
            raw_requests.iter().map(|(r, _)| r.clone()).collect();
        let body_indices: Vec<IndexSpec> = deduplicate_indices(request_only);

        // Backfill `window_index` onto each body Transform node. Re-
        // borrow `body` mutably now that we've finished consulting it
        // immutably. Only mutate `body.graph`, `body.body_indices_to_build`.
        let Some(body_mut) = artifacts.composition_bodies.get_mut(&body_id) else {
            continue;
        };
        for (req, transform_idx) in &raw_requests {
            let new_window_index =
                find_index_for(&body_indices, &req.root, &req.group_by, &req.sort_by);
            if let PlanNode::Transform {
                window_index,
                partition_lookup,
                ..
            } = &mut body_mut.graph[*transform_idx]
            {
                *window_index = new_window_index;
                // Body-internal partition lookup: cross-source
                // (`wc.source: <other>`) is not a body-level surface
                // today, so every body window resolves through the
                // same-source path. Stamping `SameSource` keeps
                // downstream consumers that branch on
                // `partition_lookup` uniform with the top-level
                // lowering arm in `lower_node_to_plan_node`.
                *partition_lookup = Some(PartitionLookupKind::SameSource);
            }
        }
        body_mut.body_indices_to_build = body_indices;
    }
}

/// Reserved name prefix for planner-synthesized [`PlanNode::Sort`] nodes
/// inserted by [`ExecutionPlanDag::insert_enforcer_sorts`] to satisfy a
/// transform's per-operator [`NodeExecutionReqs::RequiresSortedInput`].
pub const ENFORCER_SORT_PREFIX: &str = "__sort_for_";

/// Reserved name prefix for planner-synthesized [`PlanNode::Sort`] nodes
/// inserted by [`ExecutionPlanDag::inject_correlation_sort`] to materialize
/// each source's `correlation_key:` failure-domain grouping. Distinct
/// from [`ENFORCER_SORT_PREFIX`] because the two passes encode different
/// concerns: per-operator algorithm need vs. per-source identity.
pub const CORRELATION_SORT_PREFIX: &str = "__correlation_sort_";

/// Reserved name prefix for planner-synthesized [`PlanNode::CorrelationCommit`]
/// terminal nodes inserted by [`ExecutionPlanDag::inject_correlation_commit`].
/// User node names matching this prefix are rejected at compile time.
pub const CORRELATION_COMMIT_PREFIX: &str = "__correlation_commit_";

impl ExecutionPlanDag {
    /// Enforcer-sort insertion.
    ///
    /// Walks every [`PlanNode::Transform`] whose
    /// [`NodeExecutionReqs::RequiresSortedInput`] is unsatisfied by its
    /// upstream [`PlanNode::Source`]'s declared `sort_order`, and inserts a
    /// new [`PlanNode::Sort`] enforcer node on the connecting edge.
    ///
    /// # Errors
    /// Returns [`PipelineError::Compilation`] if any user-declared node name
    /// starts with the reserved [`ENFORCER_SORT_PREFIX`].
    ///
    /// # Idempotency
    /// A second call adds zero new nodes: enforcers inserted on the first
    /// pass are themselves [`PlanNode::Sort`], and the walk only operates on
    /// [`PlanNode::Source`] direct parents.
    pub fn insert_enforcer_sorts(
        &mut self,
        inputs: &HashMap<String, SourceConfig>,
    ) -> Result<(), PipelineError> {
        // Reserved-prefix guard: validate up-front so insertions cannot
        // collide with a user-declared name. Covers every reserved
        // planner prefix at once so a misnamed user node is rejected
        // even before the first synthesis pass runs.
        for idx in self.graph.node_indices() {
            let node = &self.graph[idx];
            if matches!(
                node,
                PlanNode::Sort { .. } | PlanNode::CorrelationCommit { .. }
            ) {
                continue;
            }
            let name = node.name();
            for prefix in [
                ENFORCER_SORT_PREFIX,
                CORRELATION_SORT_PREFIX,
                CORRELATION_COMMIT_PREFIX,
            ] {
                if name.starts_with(prefix) {
                    return Err(PipelineError::Compilation {
                        transform_name: name.to_string(),
                        messages: vec![format!(
                            "node name '{}' uses reserved prefix '{}' (planner-synthesized only)",
                            name, prefix
                        )],
                    });
                }
            }
        }

        // Snapshot consumer indices + their required ordering before mutating.
        let consumers: Vec<(NodeIndex, Vec<SortField>)> = self
            .graph
            .node_indices()
            .filter_map(|idx| match &self.graph[idx] {
                PlanNode::Transform {
                    execution_reqs: NodeExecutionReqs::RequiresSortedInput { sort_fields },
                    ..
                } => Some((idx, sort_fields.clone())),
                _ => None,
            })
            .collect();

        for (consumer_idx, required) in consumers {
            // The only requirement class today (correlated DLQ) consumes
            // a Source directly. Find the unique direct Source parent;
            // skip otherwise (future extensions will widen the walk).
            let source_idx = self
                .graph
                .neighbors_directed(consumer_idx, petgraph::Direction::Incoming)
                .find(|&p| matches!(self.graph[p], PlanNode::Source { .. }));
            let Some(source_idx) = source_idx else {
                continue;
            };

            let source_name = self.graph[source_idx].name().to_string();
            let declared: Vec<SortField> = inputs
                .get(&source_name)
                .and_then(|ic| ic.sort_order.as_ref())
                .map(|specs| specs.iter().cloned().map(|s| s.into_sort_field()).collect())
                .unwrap_or_default();

            if source_ordering_satisfies(&declared, &required) {
                continue;
            }

            // Locate and remove the direct Source→consumer edge, capturing
            // its dependency type and consumer-side port for re-use on
            // the rewritten edges. The port tag belongs to the
            // sort→consumer hop (the consumer is unchanged); the
            // source→sort hop has no port (Sort takes a single unnamed
            // input).
            let edge_id = self
                .graph
                .find_edge(source_idx, consumer_idx)
                .expect("source parent must have outgoing edge to consumer");
            let dep_type = self.graph[edge_id].dependency_type;
            let consumer_port = self.graph[edge_id].port.clone();
            self.graph.remove_edge(edge_id);

            // Planner-synthesized sort enforcer. No YAML node exists
            // for it; it is derived from the consumer's
            // `RequiresSortedInput` requirement and inserted on the
            // edge from the upstream source. `Span::SYNTHETIC` is
            // correct because the node has no author-written origin.
            let consumer_name = self.graph[consumer_idx].name().to_string();
            let sort_node = PlanNode::Sort {
                name: format!("{ENFORCER_SORT_PREFIX}{consumer_name}"),
                span: Span::SYNTHETIC,
                sort_fields: required,
            };
            let sort_idx = self.graph.add_node(sort_node);
            self.graph.add_edge(
                source_idx,
                sort_idx,
                PlanEdge {
                    dependency_type: dep_type,
                    port: None,
                },
            );
            self.graph.add_edge(
                sort_idx,
                consumer_idx,
                PlanEdge {
                    dependency_type: dep_type,
                    port: consumer_port,
                },
            );
        }

        Ok(())
    }

    /// Inject one [`PlanNode::Sort`] downstream of every source whose
    /// `correlation_key:` is declared, so the per-source failure-domain
    /// grouping has the deterministic ordering the commit phase needs.
    ///
    /// No-op for sources without a declared `correlation_key:`, and
    /// idempotent for sources whose declared `sort_order` already starts
    /// with the source's correlation-key fields. Each sort node carries
    /// the source's CK fields (ascending) as a prefix of any declared
    /// sort, mirroring the per-source identity downstream operators
    /// commit against.
    pub fn inject_correlation_sort(
        &mut self,
        sources: &[&crate::config::pipeline_node::SourceBody],
    ) -> Result<(), PipelineError> {
        for body in sources {
            let Some(correlation_key) = body.correlation_key.as_ref() else {
                continue;
            };
            let source_cfg = &body.source;

            let key_field_names: Vec<String> = correlation_key
                .fields()
                .into_iter()
                .map(String::from)
                .collect();
            let declared: Vec<SortField> = source_cfg
                .sort_order
                .as_ref()
                .map(|specs| specs.iter().cloned().map(|s| s.into_sort_field()).collect())
                .unwrap_or_default();

            // Idempotent satisfaction: declared sort already starts with
            // the correlation key fields. Direction-agnostic.
            let already_satisfied = key_field_names.len() <= declared.len()
                && key_field_names
                    .iter()
                    .zip(declared.iter())
                    .all(|(k, sf)| k == &sf.field);
            if already_satisfied {
                continue;
            }

            let source_name = &source_cfg.name;
            let source_idx = self.graph.node_indices().find(
                |&idx| matches!(&self.graph[idx], PlanNode::Source { name, .. } if name == source_name),
            );
            let Some(source_idx) = source_idx else {
                return Err(PipelineError::Compilation {
                    transform_name: source_name.clone(),
                    messages: vec![format!(
                        "inject_correlation_sort: source '{source_name}' not found in DAG"
                    )],
                });
            };

            let mut sort_fields: Vec<SortField> = key_field_names
                .iter()
                .map(|f| SortField {
                    field: f.clone(),
                    order: crate::config::SortOrder::Asc,
                    null_order: None,
                })
                .collect();
            for sf in declared.into_iter() {
                if !key_field_names.contains(&sf.field) {
                    sort_fields.push(sf);
                }
            }

            // Capture (target, dep_type, port) for every outgoing edge — the
            // port tag must survive the splice so a downstream
            // composition's named-input edge keeps its tag on the new
            // sort→target hop.
            let outgoing: Vec<(NodeIndex, DependencyType, Option<String>)> = self
                .graph
                .edges_directed(source_idx, petgraph::Direction::Outgoing)
                .map(|e| {
                    (
                        e.target(),
                        e.weight().dependency_type,
                        e.weight().port.clone(),
                    )
                })
                .collect();
            if outgoing.is_empty() {
                continue;
            }

            // Idempotent insertion guard: every outgoing edge already
            // lands on a CORRELATION_SORT_PREFIX Sort node — nothing to do.
            if outgoing.iter().all(|(t, _, _)| {
                matches!(&self.graph[*t], PlanNode::Sort { name, .. } if name.starts_with(CORRELATION_SORT_PREFIX))
            }) {
                continue;
            }

            let sort_node = PlanNode::Sort {
                name: format!("{CORRELATION_SORT_PREFIX}{source_name}"),
                span: Span::SYNTHETIC,
                sort_fields,
            };
            let sort_idx = self.graph.add_node(sort_node);
            for (target, dep_type, port) in outgoing {
                if target == sort_idx {
                    continue;
                }
                if let Some(edge_id) = self.graph.find_edge(source_idx, target) {
                    self.graph.remove_edge(edge_id);
                }
                self.graph.add_edge(
                    sort_idx,
                    target,
                    PlanEdge {
                        dependency_type: dep_type,
                        port,
                    },
                );
            }
            self.graph.add_edge(
                source_idx,
                sort_idx,
                PlanEdge {
                    dependency_type: DependencyType::Data,
                    port: None,
                },
            );
        }

        Ok(())
    }

    /// Inject the terminal [`PlanNode::CorrelationCommit`] when any
    /// source declares a `correlation_key:`.
    ///
    /// One commit node is created and every existing [`PlanNode::Output`]
    /// gains an outgoing edge to it. Output writes from the dispatcher
    /// arm route into [`crate::executor::dispatch::CorrelationGroupBuffer`]s
    /// keyed by group; the commit arm walks those buffers at end-of-DAG.
    /// Idempotent — calling twice with a commit already present is a
    /// no-op.
    ///
    /// `commit_group_by` carries the union of every source's CK field
    /// names. The runtime's `buffer_key_for_record` keys each record by
    /// every engine-stamped column it carries, so the commit-side
    /// `commit_group_by` is informational; the union is the most useful
    /// shape for `--explain` rendering.
    pub fn inject_correlation_commit(
        &mut self,
        sources: &[&crate::config::pipeline_node::SourceBody],
        max_group_buffer: u64,
    ) -> Result<(), PipelineError> {
        if sources.iter().all(|b| b.correlation_key.is_none()) {
            return Ok(());
        }

        // Idempotent: bail if a commit node already exists.
        let already = self
            .graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::CorrelationCommit { .. }));
        if already {
            return Ok(());
        }

        let mut commit_group_by: Vec<String> = Vec::new();
        for body in sources {
            if let Some(ck) = body.correlation_key.as_ref() {
                for field in ck.fields() {
                    let owned = field.to_string();
                    if !commit_group_by.contains(&owned) {
                        commit_group_by.push(owned);
                    }
                }
            }
        }

        let output_indices: Vec<NodeIndex> = self
            .graph
            .node_indices()
            .filter(|&idx| matches!(self.graph[idx], PlanNode::Output { .. }))
            .collect();
        if output_indices.is_empty() {
            return Ok(());
        }

        let commit_node = PlanNode::CorrelationCommit {
            name: format!("{CORRELATION_COMMIT_PREFIX}terminal"),
            span: Span::SYNTHETIC,
            commit_group_by,
            max_group_buffer,
        };
        let commit_idx = self.graph.add_node(commit_node);

        for output_idx in output_indices {
            self.graph.add_edge(
                output_idx,
                commit_idx,
                PlanEdge {
                    dependency_type: DependencyType::Data,
                    port: None,
                },
            );
        }

        Ok(())
    }

    /// Whether this DAG carries the correlation-key terminal commit
    /// node. Returns `true` iff [`Self::inject_correlation_commit`]
    /// has run on a pipeline with at least one source declaring a
    /// `correlation_key:` AND at least one [`PlanNode::Output`] was
    /// present.
    pub fn required_sorted_input(&self) -> bool {
        self.graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::CorrelationCommit { .. }))
    }

    /// Populate `node_properties` via topological walk.
    ///
    /// Computes [`NodeProperties`] for every node in the DAG, derived from
    /// already-populated parent properties plus declared per-node data
    /// (`SourceConfig.sort_order` for sources, `sort_fields` on
    /// [`PlanNode::Sort`], `write_set` and `has_distinct` on
    /// [`PlanNode::Transform`]). The pass never inspects operator
    /// implementations — it reads only what the plan node already declares,
    /// mirroring Trino's `PropertyDerivations.Visitor` and Spark's
    /// `AliasAwareOutputExpression`.
    ///
    /// # Panics / errors
    ///
    /// Asserts `self.node_properties.is_empty()` on entry as a double-call
    /// guard. Returns [`PipelineError::Compilation`] only if a future variant
    /// rule fails (currently infallible).
    pub fn compute_node_properties(
        &mut self,
        inputs: &HashMap<String, &crate::config::pipeline_node::SourceBody>,
    ) -> Result<(), PipelineError> {
        assert!(
            self.node_properties.is_empty(),
            "compute_node_properties called twice on the same ExecutionPlanDag"
        );

        let mut topo = Topo::new(&self.graph);
        while let Some(idx) = topo.next(&self.graph) {
            let props = {
                let parents: Vec<&NodeProperties> = self
                    .graph
                    .neighbors_directed(idx, petgraph::Direction::Incoming)
                    .filter_map(|p| self.node_properties.get(&p))
                    .collect();
                compute_one(&self.graph[idx], &parents, inputs)
            };
            self.node_properties.insert(idx, props);
        }

        Ok(())
    }

    /// Mark every [`IndexSpec`] whose owning window-bearing Transform
    /// participates in a relaxed-CK retraction pipeline AND whose
    /// `partition_by` does not cover the source-side correlation-key
    /// set. The flag flips the executor's window arm into buffered emit
    /// mode so the orchestrator's commit-time recompute can rerun the
    /// window over `partition − retracted_rows` and emit per-output
    /// Deltas.
    ///
    /// The trigger captures both directions of the geometry:
    ///
    /// * Window upstream of a relaxed-CK aggregate. The window operates
    ///   on source-side arena positions; a CK group can span multiple
    ///   partitions when `partition_by` is not a CK superset; the
    ///   relaxed aggregate downstream provides the retraction protocol
    ///   that needs the per-partition rollback.
    ///
    /// * Window downstream of a relaxed-CK aggregate. The aggregate's
    ///   dropped CK fields no longer appear in the partition's
    ///   downstream `ck_set`; if the window's `partition_by` references
    ///   one of those fields, partitions can span what would have been
    ///   strict CK boundaries.
    ///
    /// The unified rule: at the window's node, the visible `ck_set` has
    /// at least one field that is not part of the window's
    /// `partition_by` slice. Reads `node_properties.ck_set`, so callers
    /// must invoke `compute_node_properties` first. Idempotent.
    pub(crate) fn derive_window_buffer_recompute_flags(&mut self) {
        let ck_at = |idx: NodeIndex| -> BTreeSet<String> {
            self.node_properties
                .get(&idx)
                .map(|p| p.ck_set.clone())
                .unwrap_or_default()
        };
        derive_window_buffer_recompute_flags_for_graph(
            &self.graph,
            &mut self.indices_to_build,
            ck_at,
        );
    }

    /// Resolve `AggregateStrategyHint` on every `PlanNode::Aggregation`
    /// against upstream `OrderingProvenance`, rewrite the node's
    /// `strategy` field, populate the auxiliary `fallback_reason` /
    /// `skipped_streaming_available` / `qualified_sort_order` fields,
    /// and overwrite the side-table `node_properties[idx].ordering` to
    /// reflect the resolved strategy.
    ///
    /// Runs as a separate post-pass inside `compile()` immediately after
    /// `compute_node_properties()`. Hard-errors at compile time when a
    /// user explicitly requests `strategy: streaming` on an ineligible
    /// input, via the rustc-shaped walker
    /// `render_unordered_streaming_error`.
    pub(crate) fn select_aggregation_strategies(&mut self) -> Result<(), PipelineError> {
        use crate::aggregation::{
            AggregateStrategy, StreamingEligibility, qualifies_for_streaming,
        };
        use crate::config::AggregateStrategyHint;
        use crate::plan::properties::{Confidence, render_unordered_streaming_error};

        // Collect target indices first to avoid holding a borrow on `graph`
        // while mutating `node_properties`.
        let agg_indices: Vec<NodeIndex> = self
            .topo_order
            .iter()
            .copied()
            .filter(|idx| matches!(self.graph[*idx], PlanNode::Aggregation { .. }))
            .collect();

        for idx in agg_indices {
            let (name, hint, group_by) = match &self.graph[idx] {
                PlanNode::Aggregation { name, config, .. } => {
                    (name.clone(), config.strategy, config.group_by.clone())
                }
                _ => unreachable!(),
            };

            let parent_idx = crate::executor::single_predecessor(self, idx, "aggregation", &name)?;
            let parent_props = self
                .node_properties
                .get(&parent_idx)
                .cloned()
                .ok_or_else(|| PipelineError::Internal {
                    op: "aggregation",
                    node: name.clone(),
                    detail: "parent node has no computed NodeProperties".to_string(),
                })?;

            let eligibility = qualifies_for_streaming(&parent_props, &group_by);

            // Resolve hint → (strategy, fallback_reason, skipped_streaming_available,
            // qualified_sort_order). On explicit Streaming + ineligible, hard-error.
            let resolved: ResolvedStrategy = match hint {
                AggregateStrategyHint::Auto => match &eligibility {
                    StreamingEligibility::Streaming {
                        qualified_sort_order,
                        ..
                    } => ResolvedStrategy {
                        strategy: AggregateStrategy::Streaming,
                        fallback_reason: None,
                        skipped_streaming_available: false,
                        qualified_sort_order: Some(qualified_sort_order.clone()),
                    },
                    StreamingEligibility::HashFallback { reason } => ResolvedStrategy {
                        strategy: AggregateStrategy::Hash,
                        fallback_reason: Some(reason.clone()),
                        skipped_streaming_available: false,
                        qualified_sort_order: None,
                    },
                },
                AggregateStrategyHint::Hash => ResolvedStrategy {
                    strategy: AggregateStrategy::Hash,
                    fallback_reason: None,
                    skipped_streaming_available: matches!(
                        eligibility,
                        StreamingEligibility::Streaming { .. }
                    ),
                    qualified_sort_order: None,
                },
                AggregateStrategyHint::Streaming => match &eligibility {
                    StreamingEligibility::Streaming {
                        qualified_sort_order,
                        ..
                    } => ResolvedStrategy {
                        strategy: AggregateStrategy::Streaming,
                        fallback_reason: None,
                        skipped_streaming_available: false,
                        qualified_sort_order: Some(qualified_sort_order.clone()),
                    },
                    StreamingEligibility::HashFallback { .. } => {
                        let msg = render_unordered_streaming_error(&parent_props, &group_by, &name);
                        return Err(PipelineError::Compilation {
                            transform_name: name.clone(),
                            messages: vec![msg],
                        });
                    }
                },
            };

            // Capture parent provenance before mutating self.graph for the
            // streaming-output ordering chain.
            let parent_provenance = parent_props.ordering.provenance.clone();

            // Mutate the PlanNode in place.
            if let PlanNode::Aggregation {
                strategy,
                fallback_reason,
                skipped_streaming_available,
                qualified_sort_order,
                ..
            } = &mut self.graph[idx]
            {
                *strategy = resolved.strategy;
                *fallback_reason = resolved.fallback_reason.clone();
                *skipped_streaming_available = resolved.skipped_streaming_available;
                *qualified_sort_order = resolved.qualified_sort_order.clone();
            }

            // Preserve the CK set computed by `compute_one`. The post-pass
            // overwrites ordering/partitioning to reflect the resolved
            // strategy, but the CK lattice has already been computed and
            // must survive the rewrite — the resolved strategy does not
            // change which `$ck.<field>` columns this aggregate's output
            // carries.
            let preserved_ck_set = self
                .node_properties
                .get(&idx)
                .map(|p| p.ck_set.clone())
                .unwrap_or_default();

            // Overwrite the side-table ordering for this aggregation node
            // (D77 — single source of truth for aggregation ordering).
            let new_props = match resolved.strategy {
                AggregateStrategy::Streaming => NodeProperties {
                    ordering: Ordering {
                        sort_order: resolved.qualified_sort_order.clone(),
                        provenance: OrderingProvenance::IntroducedByStreamingAggregate {
                            at_node: name.clone(),
                            enabled_by: Box::new(parent_provenance),
                        },
                    },
                    partitioning: Partitioning {
                        kind: PartitioningKind::Single,
                        provenance: PartitioningProvenance::SingleStream,
                    },
                    ck_set: preserved_ck_set.clone(),
                },
                AggregateStrategy::Hash => NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::DestroyedByHashAggregate {
                            at_node: name.clone(),
                            confidence: Confidence::Proven,
                        },
                    },
                    partitioning: Partitioning {
                        kind: PartitioningKind::Single,
                        provenance: PartitioningProvenance::SingleStream,
                    },
                    ck_set: preserved_ck_set,
                },
            };
            self.node_properties.insert(idx, new_props);
        }

        Ok(())
    }
}

/// Internal carrier for `select_aggregation_strategies` resolution result.
struct ResolvedStrategy {
    strategy: crate::aggregation::AggregateStrategy,
    fallback_reason: Option<String>,
    skipped_streaming_available: bool,
    qualified_sort_order: Option<Vec<SortField>>,
}

/// Per-node property derivation rule. Pure function over the node and its
/// (already-computed) parent properties — never sees operator implementations.
///
/// Per Decision D46, the `Transform` arm MUST NOT advertise any ordering
/// produced by Phase 6 arena `sort_partition` — that sort is intra-partition
/// inside a `RequiresArena` materialization layer and is invisible at the
/// stream level. The rule below reads only `write_set` / `has_distinct` and
/// the parent's already-computed ordering, so arena-internal sorts cannot
/// leak in.
fn compute_one(
    node: &PlanNode,
    parents: &[&NodeProperties],
    inputs: &HashMap<String, &crate::config::pipeline_node::SourceBody>,
) -> NodeProperties {
    let single_stream_partitioning = || Partitioning {
        kind: PartitioningKind::Single,
        provenance: PartitioningProvenance::SingleStream,
    };
    let parent_partitioning = || {
        parents
            .first()
            .map(|p| p.partitioning.clone())
            .unwrap_or_else(single_stream_partitioning)
    };
    // Default lattice rule for nodes that do not transform CK visibility
    // (Transform, Sort, Route, Output, Composition): preserve the first
    // parent's CK set, or empty when there is no parent.
    let preserve_parent_ck_set = || {
        parents
            .first()
            .map(|p| p.ck_set.clone())
            .unwrap_or_default()
    };

    match node {
        PlanNode::Source { name, .. } => {
            let body = inputs.get(name).copied();
            let sort_order: Option<Vec<SortField>> = body
                .and_then(|b| b.source.sort_order.as_ref())
                .map(|specs| specs.iter().cloned().map(|s| s.into_sort_field()).collect());
            let provenance = if sort_order.is_some() {
                OrderingProvenance::DeclaredOnInput {
                    input_name: name.clone(),
                }
            } else {
                OrderingProvenance::NoOrdering
            };
            // Source observes every CK field declared on its own
            // `correlation_key:` — those columns are shadow-stamped at
            // ingest. Sources without a declared CK contribute an
            // empty set; multi-source pipelines rely on each source's
            // local declaration.
            let ck_set: BTreeSet<String> = body
                .and_then(|b| b.correlation_key.as_ref())
                .map(|ck| {
                    ck.fields()
                        .into_iter()
                        .map(String::from)
                        .collect::<BTreeSet<String>>()
                })
                .unwrap_or_default();
            NodeProperties {
                ordering: Ordering {
                    sort_order,
                    provenance,
                },
                partitioning: single_stream_partitioning(),
                ck_set,
            }
        }

        PlanNode::Sort {
            name, sort_fields, ..
        } => NodeProperties {
            ordering: Ordering {
                sort_order: Some(sort_fields.clone()),
                provenance: OrderingProvenance::Preserved {
                    from_node: name.clone(),
                },
            },
            partitioning: parent_partitioning(),
            ck_set: preserve_parent_ck_set(),
        },

        PlanNode::Transform {
            name,
            write_set,
            has_distinct,
            ..
        } => {
            let partitioning = parent_partitioning();
            let ck_set = preserve_parent_ck_set();

            // Distinct destroys ordering unconditionally.
            if *has_distinct {
                return NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::DestroyedByDistinct {
                            at_node: name.clone(),
                            confidence: crate::plan::properties::Confidence::Proven,
                        },
                    },
                    partitioning,
                    ck_set,
                };
            }

            // No parent or parent had no ordering — nothing to preserve.
            let parent = parents.first();
            let parent_sort = parent.and_then(|p| p.ordering.sort_order.as_ref());
            let Some(parent_sort) = parent_sort else {
                return NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: parent
                            .map(|p| p.ordering.provenance.clone())
                            .unwrap_or(OrderingProvenance::NoOrdering),
                    },
                    partitioning,
                    ck_set,
                };
            };

            // Intersect write_set with parent's sort-key field names. Note:
            // arena `sort_partition` (Phase 6) does not appear here — only
            // record fields written via `emit name = ...` enter `write_set`.
            let lost: Vec<String> = parent_sort
                .iter()
                .filter(|sf| write_set.contains(&sf.field))
                .map(|sf| sf.field.clone())
                .collect();
            if lost.is_empty() {
                NodeProperties {
                    ordering: Ordering {
                        sort_order: Some(parent_sort.clone()),
                        provenance: OrderingProvenance::Preserved {
                            from_node: name.clone(),
                        },
                    },
                    partitioning,
                    ck_set,
                }
            } else {
                NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::DestroyedByTransformWriteSet {
                            at_node: name.clone(),
                            fields_written: write_set.iter().cloned().collect(),
                            sort_fields_lost: lost,
                            confidence: crate::plan::properties::Confidence::Proven,
                        },
                    },
                    partitioning,
                    ck_set,
                }
            }
        }

        PlanNode::Route { name, .. } => {
            // Row-selection only — every branch inherits parent ordering and
            // partitioning unchanged. Provenance is rewritten to point at this
            // node so explain can chain through.
            let parent = match parents.first() {
                Some(p) => p,
                None => return NodeProperties::unordered_single(),
            };
            let provenance = if parent.ordering.sort_order.is_some() {
                OrderingProvenance::Preserved {
                    from_node: name.clone(),
                }
            } else {
                parent.ordering.provenance.clone()
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order: parent.ordering.sort_order.clone(),
                    provenance,
                },
                partitioning: parent.partitioning.clone(),
                ck_set: parent.ck_set.clone(),
            }
        }

        PlanNode::Merge { name, .. } => {
            if parents.is_empty() {
                return NodeProperties::unordered_single();
            }
            let first_so = parents[0].ordering.sort_order.clone();
            let all_match = parents
                .iter()
                .all(|p| sort_orders_equal(&p.ordering.sort_order, &first_so));
            let partitioning = if parents
                .iter()
                .all(|p| matches!(p.partitioning.kind, PartitioningKind::Single))
            {
                single_stream_partitioning()
            } else {
                parents[0].partitioning.clone()
            };
            // Intersect CK sets across all parents: a CK column is visible
            // post-merge only when every parent stream still carries it.
            // If a sibling branch has already passed through a relaxed
            // Aggregate that dropped a CK field, the smaller visible set
            // wins downstream.
            let ck_set: BTreeSet<String> = {
                let mut iter = parents.iter().map(|p| p.ck_set.clone());
                match iter.next() {
                    Some(first) => iter.fold(first, |acc, next| {
                        acc.intersection(&next).cloned().collect()
                    }),
                    None => BTreeSet::new(),
                }
            };
            if all_match {
                let provenance = if first_so.is_some() {
                    OrderingProvenance::Preserved {
                        from_node: name.clone(),
                    }
                } else {
                    OrderingProvenance::NoOrdering
                };
                NodeProperties {
                    ordering: Ordering {
                        sort_order: first_so,
                        provenance,
                    },
                    partitioning,
                    ck_set,
                }
            } else {
                NodeProperties {
                    ordering: Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::DestroyedByMergeMismatch {
                            at_node: name.clone(),
                            parent_orderings: parents
                                .iter()
                                .map(|p| p.ordering.sort_order.clone())
                                .collect(),
                            confidence: crate::plan::properties::Confidence::Proven,
                        },
                    },
                    partitioning,
                    ck_set,
                }
            }
        }

        PlanNode::Aggregation { name, config, .. } => {
            // Aggregation node ordering is the sole responsibility of
            // the `select_aggregation_strategies` post-pass, which runs
            // immediately after `compute_node_properties` and overwrites
            // this entry based on the resolved strategy. The defensive
            // default is "no ordering, single stream" so any bug that
            // bypasses the post-pass produces conservative
            // (correct-but-suboptimal) downstream eligibility decisions
            // rather than silently asserting a false ordering.
            //
            // CK lattice rule:
            //   - strict (`group_by ⊇ parent.ck_set`): preserves parent
            //     CK set unchanged.
            //   - relaxed (`group_by` omits any CK field visible in
            //     the parent lattice): intersects parent CK set with
            //     `group_by`, then injects a synthetic
            //     `$ck.aggregate.<name>` entry that the schema-widening
            //     pass already appended to the aggregate's
            //     `output_schema`. The synthetic entry keeps a relaxed
            //     aggregate's downstream consumers correlation-aware
            //     even when the original source CK field has dropped
            //     out of the lattice — detect-phase fan-out resolves
            //     it back to source rows via the aggregator's
            //     `input_rows` lineage.
            let parent_ck = preserve_parent_ck_set();
            let omits_ck = group_by_omits_any_ck_field(&config.group_by, &parent_ck);
            let mut ck_set: BTreeSet<String> = if omits_ck {
                let group_by_set: BTreeSet<&str> =
                    config.group_by.iter().map(String::as_str).collect();
                parent_ck
                    .into_iter()
                    .filter(|f| group_by_set.contains(f.as_str()))
                    .collect()
            } else {
                parent_ck
            };
            if omits_ck {
                ck_set.insert(format!("$ck.aggregate.{name}"));
            }
            NodeProperties {
                ordering: Ordering {
                    sort_order: None,
                    provenance: OrderingProvenance::NoOrdering,
                },
                partitioning: single_stream_partitioning(),
                ck_set,
            }
        }

        PlanNode::Output { name, .. } => {
            // Terminal — properties still computed for debugging. Inherit
            // parent, rewrite provenance to point at this node when ordering
            // is non-None so explain chains through. CK set is preserved at
            // Output so the inclusion-flag interaction (writer-default
            // strip vs `include_correlation_keys: true`) can consult the
            // surviving CK columns.
            let parent = match parents.first() {
                Some(p) => p,
                None => return NodeProperties::unordered_single(),
            };
            let provenance = if parent.ordering.sort_order.is_some() {
                OrderingProvenance::Preserved {
                    from_node: name.clone(),
                }
            } else {
                parent.ordering.provenance.clone()
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order: parent.ordering.sort_order.clone(),
                    provenance,
                },
                partitioning: parent.partitioning.clone(),
                ck_set: parent.ck_set.clone(),
            }
        }

        PlanNode::Composition { name, .. } => {
            // Composition is opaque at the top-level property pass.
            // Inherit parent ordering/partitioning — body-internal
            // properties live on the bound body's per-node rows, not
            // on this node.
            let parent = match parents.first() {
                Some(p) => p,
                None => return NodeProperties::unordered_single(),
            };
            let provenance = if parent.ordering.sort_order.is_some() {
                OrderingProvenance::Preserved {
                    from_node: name.clone(),
                }
            } else {
                parent.ordering.provenance.clone()
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order: parent.ordering.sort_order.clone(),
                    provenance,
                },
                partitioning: parent.partitioning.clone(),
                ck_set: parent.ck_set.clone(),
            }
        }

        PlanNode::Combine {
            name, propagate_ck, ..
        } => {
            // Combine always destroys parent ordering: hash-build/probe
            // (and IEJoin, grace hash) do not preserve driving-input
            // order. Emit `DestroyedByCombine { Proven }` so downstream
            // streaming-agg eligibility / `--explain` / Kiln overlays
            // can chain through and suggest "add a sort step between
            // `{combine}` and `{consumer}`". Resolves Phase Combine
            // §OQ-6 and drill D12.
            //
            // CK lattice rule reads `propagate_ck`. The Combine
            // post-pass `select_combine_strategies` runs AFTER
            // `compute_node_properties`, so `driving_input` is empty
            // here — declaration order (first parent = driver) is the
            // fallback, which matches today's runtime driver-resolution
            // behavior at dispatch.
            use crate::config::pipeline_node::PropagateCkSpec;
            let mut ck_set: BTreeSet<String> = match propagate_ck {
                PropagateCkSpec::Driver => parents
                    .first()
                    .map(|p| p.ck_set.clone())
                    .unwrap_or_default(),
                PropagateCkSpec::All => parents
                    .iter()
                    .flat_map(|p| p.ck_set.iter().cloned())
                    .collect(),
                PropagateCkSpec::Named(names) => {
                    let upstream_union: BTreeSet<String> = parents
                        .iter()
                        .flat_map(|p| p.ck_set.iter().cloned())
                        .collect();
                    names.intersection(&upstream_union).cloned().collect()
                }
            };
            // Synthetic CK (`$ck.aggregate.<name>`) is engine-managed
            // lineage from a relaxed aggregate to its source rows; the
            // user did not declare it, so `propagate_ck` has no
            // semantic meaning over it. Union it from every parent so
            // detect-phase fan-out keeps its bridge across the combine
            // boundary regardless of how user-declared source CK is
            // routed.
            let synthetic: BTreeSet<String> = parents
                .iter()
                .flat_map(|p| {
                    p.ck_set
                        .iter()
                        .filter(|f| f.starts_with("$ck.aggregate."))
                        .cloned()
                })
                .collect();
            ck_set.extend(synthetic);
            NodeProperties {
                ordering: Ordering {
                    sort_order: None,
                    provenance: OrderingProvenance::DestroyedByCombine {
                        at_node: name.clone(),
                        confidence: crate::plan::properties::Confidence::Proven,
                    },
                },
                partitioning: single_stream_partitioning(),
                ck_set,
            }
        }

        PlanNode::CorrelationCommit { .. } => {
            // Terminal node — no downstream ever consults its ordering.
            // Inherit upstream parent_partitioning() defensively in case
            // a future planner pass walks this slot.
            NodeProperties {
                ordering: parents
                    .first()
                    .map(|p| p.ordering.clone())
                    .unwrap_or(Ordering {
                        sort_order: None,
                        provenance: OrderingProvenance::NoOrdering,
                    }),
                partitioning: parent_partitioning(),
                ck_set: preserve_parent_ck_set(),
            }
        }
    }
}

/// Idempotency predicate for enforcer-sort insertion.
///
/// Returns true iff `declared` (the upstream source's actual ordering) is a
/// strict prefix of, or equal to, `required` viewed the other way around: the
/// required ordering must be a prefix of the declared ordering. Element-wise
/// equality is on `(field, order, null_order)`. Mirrors DataFusion's
/// `extract_common_sort_prefix` semantics.
///
/// An empty `required` is always satisfied. An empty `declared` only satisfies
/// an empty `required`.
pub fn source_ordering_satisfies(declared: &[SortField], required: &[SortField]) -> bool {
    if required.len() > declared.len() {
        return false;
    }
    declared
        .iter()
        .zip(required.iter())
        .all(|(d, r)| d.field == r.field && d.order == r.order && d.null_order == r.null_order)
}

/// Extract the set of record-field names a CXL transform writes.
///
/// Walks the `TypedProgram`'s top-level statements and collects the names of
/// every `emit name = ...` whose target is the record (not `$meta.*`). `let`
/// statements bind locals only and are ignored; `filter`, `distinct`, `trace`,
/// and bare expression statements do not write to fields.
///
/// Consumed by `compute_node_properties` to populate the
/// `DestroyedByTransformWriteSet` provenance variant. The write set
/// lives directly on `PlanNode::Transform` so the property pass never
/// has to reach into executor-private types.
pub(crate) fn extract_write_set(typed: &TypedProgram) -> BTreeSet<String> {
    let mut set = BTreeSet::new();
    for stmt in &typed.program.statements {
        if let Statement::Emit {
            name,
            is_meta: false,
            ..
        } = stmt
        {
            set.insert(name.to_string());
        }
    }
    set
}

/// Field-wise equality for `Option<Vec<SortField>>` — `SortField` itself does
/// not derive `PartialEq` and we deliberately avoid adding the derive in this
/// task. Used by the `Merge` arm of `compute_one` for parent-ordering
/// reconciliation.
fn sort_orders_equal(a: &Option<Vec<SortField>>, b: &Option<Vec<SortField>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => {
            x.len() == y.len()
                && x.iter().zip(y.iter()).all(|(p, q)| {
                    p.field == q.field && p.order == q.order && p.null_order == q.null_order
                })
        }
        _ => false,
    }
}

/// True when an aggregate's `group_by` omits at least one field of the
/// parent's visible CK set.
///
/// Selects between the strict-collateral two-phase commit (returns
/// `false`) and the relaxed lattice + five-phase retraction protocol
/// (returns `true`). Aggregates whose parent ck_set is empty always
/// return `false` — there is no CK to test against, so retraction is
/// not in play.
///
/// `parent_ck_set` is the lattice value computed at the aggregate's
/// upstream node (typed-stable across composition descent). `group_by`
/// is the aggregate's user-declared (or auto-extension-rewritten)
/// group-by list. Field-name comparison is strict equality; the
/// auto-extension pass appends `$ck.<field>` shadow columns whenever
/// the user already lists the corresponding bare field, so the
/// bare-name check below sees the post-extension shape and stays
/// stable across the rewrite.
pub(crate) fn group_by_omits_any_ck_field(
    group_by: &[String],
    parent_ck_set: &BTreeSet<String>,
) -> bool {
    parent_ck_set
        .iter()
        .any(|f| !group_by.iter().any(|g| g.as_str() == f.as_str()))
}

/// Walk the top-level DAG and re-stamp each aggregate's
/// `requires_lineage` / `requires_buffer_mode` flags from the lattice.
///
/// Lowering stamps the strict default; this pass flips the flags via
/// `set_retraction_flags(true)` for any aggregate whose `group_by`
/// does not cover its parent's `ck_set`. Runs after
/// `compute_node_properties`, so every aggregate's parent has a
/// populated lattice entry.
pub(crate) fn apply_retraction_flags(dag: &mut ExecutionPlanDag) {
    use std::sync::Arc;

    let plan: Vec<(petgraph::graph::NodeIndex, bool)> = dag
        .graph
        .node_indices()
        .filter_map(|idx| {
            let PlanNode::Aggregation { config, .. } = &dag.graph[idx] else {
                return None;
            };
            let parent_ck = dag
                .graph
                .neighbors_directed(idx, petgraph::Direction::Incoming)
                .next()
                .and_then(|p| dag.node_properties.get(&p))
                .map(|p| p.ck_set.clone())
                .unwrap_or_default();
            let is_relaxed = group_by_omits_any_ck_field(&config.group_by, &parent_ck);
            Some((idx, is_relaxed))
        })
        .collect();

    for (idx, is_relaxed) in plan {
        if let PlanNode::Aggregation { compiled, .. } = &mut dag.graph[idx] {
            Arc::make_mut(compiled).set_retraction_flags(is_relaxed);
        }
    }
}

/// Body-graph variant. Body mini-DAGs don't carry a `node_properties`
/// side table, so the parent's CK set is derived inline by walking
/// the upstream node's `output_schema` for `$ck.<field>` columns.
pub(crate) fn apply_retraction_flags_in_body(body: &mut crate::plan::composition_body::BoundBody) {
    use clinker_record::FieldMetadata;
    use std::sync::Arc;

    let plan: Vec<(petgraph::graph::NodeIndex, bool)> = body
        .graph
        .node_indices()
        .filter_map(|idx| {
            let PlanNode::Aggregation { config, .. } = &body.graph[idx] else {
                return None;
            };
            let mut ck: BTreeSet<String> = BTreeSet::new();
            let mut cursor = idx;
            while let Some(upstream) = body
                .graph
                .neighbors_directed(cursor, petgraph::Direction::Incoming)
                .next()
            {
                if let Some(schema) = body.graph[upstream].stored_output_schema() {
                    for (i, col) in schema.columns().iter().enumerate() {
                        if matches!(
                            schema.field_metadata(i),
                            Some(FieldMetadata::SourceCorrelation { .. }),
                        ) && let Some(field) = col.strip_prefix("$ck.")
                        {
                            ck.insert(field.to_string());
                        }
                    }
                    break;
                }
                cursor = upstream;
            }
            let is_relaxed = group_by_omits_any_ck_field(&config.group_by, &ck);
            Some((idx, is_relaxed))
        })
        .collect();

    for (idx, is_relaxed) in plan {
        if let PlanNode::Aggregation { compiled, .. } = &mut body.graph[idx] {
            Arc::make_mut(compiled).set_retraction_flags(is_relaxed);
        }
    }
}

/// Shared core for the buffer-recompute auto-flip walk over any
/// `(graph, indices_to_build)` pair.
///
/// `ck_at` returns the CK set visible at a given node — the top-level
/// dispatch reads `node_properties.ck_set`; the body dispatch derives it
/// inline by walking the nearest upstream `output_schema` for
/// `FieldMetadata::SourceCorrelation` columns.
///
/// The walk is the unified rule from
/// [`ExecutionPlanDag::derive_window_buffer_recompute_flags`]: when at
/// least one aggregate's `group_by` omits a parent-CK field (relaxed
/// retraction protocol fires), every windowed Transform whose
/// `partition_by` does not cover the visible CK set flips to
/// `requires_buffer_recompute = true`.
fn derive_window_buffer_recompute_flags_for_graph<F>(
    graph: &DiGraph<PlanNode, PlanEdge>,
    indices_to_build: &mut [crate::plan::index::IndexSpec],
    mut ck_at: F,
) where
    F: FnMut(NodeIndex) -> BTreeSet<String>,
{
    // Lattice-driven enabler: at least one aggregate whose parent's
    // `ck_set` is NOT a subset of `group_by`. Without one, the
    // retraction protocol does not fire and no window needs buffer
    // mode.
    let has_relaxed_aggregate = graph.node_indices().any(|idx| {
        let PlanNode::Aggregation { config, .. } = &graph[idx] else {
            return false;
        };
        let parent_ck = graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
            .next()
            .map(&mut ck_at)
            .unwrap_or_default();
        group_by_omits_any_ck_field(&config.group_by, &parent_ck)
    });
    if !has_relaxed_aggregate {
        return;
    }

    let mut to_flag: Vec<usize> = Vec::new();
    for idx in graph.node_indices() {
        if let PlanNode::Transform {
            window_index: Some(idx_num),
            ..
        } = &graph[idx]
        {
            let idx_num = *idx_num;
            let Some(spec) = indices_to_build.get(idx_num) else {
                continue;
            };
            let partition_set: BTreeSet<&str> = spec.group_by.iter().map(String::as_str).collect();
            let ck_set = ck_at(idx);
            let ck_outside_partition = ck_set.iter().any(|f| !partition_set.contains(f.as_str()));
            if ck_outside_partition {
                to_flag.push(idx_num);
            }
        }
    }
    for idx_num in to_flag {
        if let Some(spec) = indices_to_build.get_mut(idx_num) {
            spec.requires_buffer_recompute = true;
        }
    }
}

/// Body-graph variant of `derive_window_buffer_recompute_flags`.
///
/// Body mini-DAGs do not carry a `node_properties` side table, so the
/// CK set visible at any body node is derived inline by walking the
/// nearest upstream `output_schema` for `FieldMetadata::SourceCorrelation`
/// columns — the same shape `apply_retraction_flags_in_body` uses to
/// derive the relaxed-aggregate trigger. Composition-body windows
/// downstream of a body-internal relaxed-CK aggregate flip into
/// buffer-recompute mode the same way top-level windows do, so the
/// commit-phase recompute path can rerun the window over
/// `partition − retracted_rows`.
pub(crate) fn derive_window_buffer_recompute_flags_in_body(
    body: &mut crate::plan::composition_body::BoundBody,
) {
    use clinker_record::FieldMetadata;

    let ck_at = |start: NodeIndex| -> BTreeSet<String> {
        let mut ck: BTreeSet<String> = BTreeSet::new();
        let mut cursor = start;
        loop {
            if let Some(schema) = body.graph[cursor].stored_output_schema() {
                for (i, col) in schema.columns().iter().enumerate() {
                    if matches!(
                        schema.field_metadata(i),
                        Some(FieldMetadata::SourceCorrelation { .. }),
                    ) && let Some(field) = col.strip_prefix("$ck.")
                    {
                        ck.insert(field.to_string());
                    }
                }
                return ck;
            }
            match body
                .graph
                .neighbors_directed(cursor, petgraph::Direction::Incoming)
                .next()
            {
                Some(upstream) => cursor = upstream,
                None => return ck,
            }
        }
    };

    derive_window_buffer_recompute_flags_for_graph(
        &body.graph,
        &mut body.body_indices_to_build,
        ck_at,
    );
}

/// Detect whether a CXL transform contains any `distinct` statement.
///
/// Sibling of [`extract_write_set`] — sourced from the same `TypedProgram`
/// during plan compilation. Persisted as `has_distinct` on
/// [`PlanNode::Transform`] so the property pass can emit
/// [`OrderingProvenance::DestroyedByDistinct`] without reaching into
/// executor-private types.
pub(crate) fn extract_has_distinct(typed: &TypedProgram) -> bool {
    typed
        .program
        .statements
        .iter()
        .any(|s| matches!(s, Statement::Distinct { .. }))
}

/// Check if a source's declared sort_order matches the window's sort_by.
pub(crate) fn check_already_sorted(
    _sources: &[SourceConfig],
    _source: &str,
    _sort_by: &[SortField],
) -> bool {
    // SourceConfig doesn't have sort_order yet (to be added in Task 5.4.1)
    // For now, always return false — runtime pre-sorted detection is the fallback
    false
}

/// Errors from execution plan compilation.
#[derive(Debug)]
pub enum PlanError {
    IndexPlanning(PlanIndexError),
    MissingLocalWindow {
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
            PlanError::IndexPlanning(e) => write!(f, "index planning error: {}", e),
            PlanError::MissingLocalWindow { transform } => {
                write!(
                    f,
                    "transform '{}' uses window.* but has no local_window config",
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

#[cfg(test)]
mod port_tag_guard_tests {
    use super::*;
    use crate::plan::bind_schema::CompileArtifacts;
    use crate::plan::composition_body::CompositionBodyId;
    use clinker_record::SchemaBuilder;
    use std::sync::Arc;

    fn empty_dag() -> ExecutionPlanDag {
        ExecutionPlanDag {
            graph: DiGraph::new(),
            topo_order: Vec::new(),
            source_dag: Vec::new(),
            indices_to_build: Vec::new(),
            output_projections: Vec::new(),
            parallelism: ParallelismProfile {
                per_transform: Vec::new(),
                worker_threads: 1,
            },
            node_properties: HashMap::new(),
            deferred_regions: HashMap::new(),
        }
    }

    fn source_node(name: &str) -> PlanNode {
        PlanNode::Source {
            name: name.to_string(),
            span: Span::SYNTHETIC,
            resolved: None,
            output_schema: SchemaBuilder::new().build(),
        }
    }

    fn composition_node(name: &str) -> PlanNode {
        PlanNode::Composition {
            name: name.to_string(),
            span: Span::SYNTHETIC,
            body: CompositionBodyId::SENTINEL,
            output_schema: Arc::new(clinker_record::Schema::new(Vec::new())),
        }
    }

    #[test]
    fn diagnose_silent_when_every_composition_edge_is_port_tagged() {
        let mut dag = empty_dag();
        let src = dag.graph.add_node(source_node("src"));
        let comp = dag.graph.add_node(composition_node("comp"));
        dag.graph.add_edge(
            src,
            comp,
            PlanEdge {
                dependency_type: DependencyType::Data,
                port: Some("p".to_string()),
            },
        );
        let artifacts = CompileArtifacts::default();
        let diags = diagnose_untagged_composition_edges(&dag, &artifacts);
        assert!(
            diags.is_empty(),
            "expected no diagnostics, got: {:?}",
            diags
        );
    }

    #[test]
    fn diagnose_emits_e152_for_untagged_top_level_composition_edge() {
        let mut dag = empty_dag();
        let src = dag.graph.add_node(source_node("src"));
        let comp = dag.graph.add_node(composition_node("comp"));
        dag.graph.add_edge(
            src,
            comp,
            PlanEdge {
                dependency_type: DependencyType::Data,
                port: None,
            },
        );
        let artifacts = CompileArtifacts::default();
        let diags = diagnose_untagged_composition_edges(&dag, &artifacts);
        assert_eq!(diags.len(), 1, "expected one diagnostic, got {:?}", diags);
        assert_eq!(diags[0].code, "E152");
        assert!(
            diags[0].message.contains("comp"),
            "diag should name the composition: {}",
            diags[0].message
        );
        assert!(
            diags[0].message.contains("src"),
            "diag should name the producer: {}",
            diags[0].message
        );
        assert!(
            diags[0].message.contains("top-level"),
            "diag should label the scope: {}",
            diags[0].message
        );
    }

    #[test]
    fn diagnose_emits_e152_for_untagged_body_composition_edge() {
        let dag = empty_dag();
        let mut artifacts = CompileArtifacts::default();
        let body_id = artifacts.fresh_body_id();
        let mut body_graph = DiGraph::<PlanNode, PlanEdge>::new();
        let body_src = body_graph.add_node(source_node("body_src"));
        let body_comp = body_graph.add_node(composition_node("nested_comp"));
        body_graph.add_edge(
            body_src,
            body_comp,
            PlanEdge {
                dependency_type: DependencyType::Data,
                port: None,
            },
        );
        let body = crate::plan::composition_body::BoundBody {
            signature_path: std::path::PathBuf::from("compositions/test.comp.yaml"),
            graph: body_graph,
            topo_order: Vec::new(),
            name_to_idx: HashMap::new(),
            port_name_to_node_idx: HashMap::new(),
            body_rows: HashMap::new(),
            node_input_refs: HashMap::new(),
            route_bodies: HashMap::new(),
            output_port_rows: indexmap::IndexMap::new(),
            output_port_to_node_idx: indexmap::IndexMap::new(),
            input_port_rows: indexmap::IndexMap::new(),
            nested_body_ids: Vec::new(),
            body_indices_to_build: Vec::new(),
            body_window_configs: HashMap::new(),
            deferred_regions: HashMap::new(),
        };
        artifacts.insert_body(body_id, body);
        let diags = diagnose_untagged_composition_edges(&dag, &artifacts);
        assert_eq!(diags.len(), 1, "expected one diagnostic, got {:?}", diags);
        assert_eq!(diags[0].code, "E152");
        assert!(
            diags[0].message.contains("nested_comp"),
            "diag should name the nested composition: {}",
            diags[0].message
        );
        assert!(
            diags[0].message.contains(&format!("body {}", body_id.0)),
            "diag should label the body scope: {}",
            diags[0].message
        );
    }
}
