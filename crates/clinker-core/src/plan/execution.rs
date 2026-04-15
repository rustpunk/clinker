//! Phase F: Execution plan emission.
//!
//! Compiles a `PipelineConfig` + CXL programs into an `ExecutionPlanDag`
//! that orchestrates the pipeline via a petgraph DAG.

use std::collections::{BTreeSet, HashMap, HashSet};

use indexmap::IndexMap;
use petgraph::algo::toposort;
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
use crate::plan::composition_body::CompositionBodyId;
use crate::plan::index::{self, IndexSpec, LocalWindowConfig, PlanIndexError, RawIndexRequest};
use crate::plan::properties::{
    NodeProperties, Ordering, OrderingProvenance, Partitioning, PartitioningKind,
    PartitioningProvenance,
};
use crate::span::Span;
use clinker_record::Schema;
use cxl::plan::CompiledAggregate;

fn is_false(b: &bool) -> bool {
    !*b
}

use cxl::analyzer::{self, ParallelismHint};
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
        /// Phase 16b Wave 2 enrichment: full resolved Source payload.
        /// Populated only by the new `PipelineConfig::compile()` lowering
        /// from the unified `nodes:` taxonomy. Legacy planner leaves it
        /// `None`. Boxed to keep the variant small.
        #[serde(skip)]
        resolved: Option<Box<PlanSourcePayload>>,
    },
    Transform {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Phase 16b Wave 2 enrichment: full resolved Transform payload.
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
        /// coupling. See `docs/research/RESEARCH-property-derivation-layering.md`.
        #[serde(skip_serializing_if = "BTreeSet::is_empty")]
        write_set: BTreeSet<String>,
        /// True iff the CXL transform contains a `distinct` statement. Populated
        /// at compile time alongside `write_set`. Consumed by
        /// `compute_node_properties` to emit `DestroyedByDistinct` for the
        /// downstream stream's ordering provenance.
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        has_distinct: bool,
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
    },
    Output {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Phase 16b Wave 2 enrichment: full resolved Output payload.
        #[serde(skip)]
        resolved: Option<Box<PlanOutputPayload>>,
    },
    /// Planner-synthesized sort enforcer (drill pass 3, D46/D47).
    ///
    /// Inserted by `ExecutionPlanDag::insert_enforcer_sorts` on edges feeding
    /// transforms with `RequiresSortedInput` whose upstream `Source` does not
    /// already satisfy the requirement (per `source_ordering_satisfies`).
    ///
    /// Distinct from Phase 6 arena `sort_partition` (window-local, never lifted
    /// into the DAG) and Phase 8 declared Source/Output sorts. The variant name
    /// is reserved with the prefix `__sort_for_{consumer}`; user-declared node
    /// names starting with `__sort_for_` are rejected at compile time.
    Sort {
        name: String,
        #[serde(skip)]
        span: Span,
        sort_fields: Vec<SortField>,
    },
    /// Hash / streaming GROUP BY transform (Phase 16, Task 16.3.5).
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
        /// Reason streaming was not selected, populated by the
        /// `select_aggregation_strategies` post-pass (Task 16.4.9) when
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
    },
    /// N-ary combine node (Phase Combine, task C.0.2).
    ///
    /// V-1-1 side-table architecture: this variant carries ONLY --explain-
    /// visible and parse-time-available fields inline. The late-populated
    /// compile state lives in `CompileArtifacts` side-tables
    /// (`typed["{name}::where"]`, `typed["{name}::body"]`,
    /// `combine_predicates`, `combine_inputs`). See
    /// `crates/clinker-core/src/plan/combine.rs`.
    Combine {
        name: String,
        #[serde(skip)]
        span: Span,
        /// Planner-selected strategy. Defaults to `HashBuildProbe`;
        /// overwritten by `select_combine_strategies` post-pass (C.2).
        /// Follows `PlanNode::Aggregation.strategy` precedent.
        strategy: crate::plan::combine::CombineStrategy,
        /// Driving input qualifier. Empty string until the planner
        /// selects it (C.2).
        driving_input: String,
        match_mode: crate::config::pipeline_node::MatchMode,
        on_miss: crate::config::pipeline_node::OnMiss,
        /// For synthetic binary combine nodes created by N-ary
        /// decomposition (C.4): name of the original N-ary combine node
        /// this was decomposed from. `None` for user-authored combine
        /// nodes. Populated in C.4.0.3; consumed by C.5.1 `--explain`
        /// grouping (RESOLUTION W-1).
        decomposed_from: Option<String>,
    },
}

/// Phase 16b Wave 2 — fully-resolved Source payload, populated by the new
/// `PipelineConfig::compile()` lowering path. Wraps the parse-time
/// `SourceConfig` plus the `ValidatedPath` (proof of pre-pass success).
/// Stored behind `Box` on `PlanNode::Source` to keep the variant slim.
#[derive(Debug, Clone)]
pub struct PlanSourcePayload {
    pub source: SourceConfig,
    pub validated_path: Option<crate::security::ValidatedPath>,
}

/// Phase 16b Wave 2 — fully-resolved Transform payload. Holds the
/// optional analytic-window spec (renamed from local_window), the log
/// directives, the validations sidebar, the DLQ NodeId for downstream
/// wiring, and the compile-time CXL `TypedProgram` (Task 16b.9).
#[derive(Debug, Clone)]
pub struct PlanTransformPayload {
    pub analytic_window: Option<crate::config::pipeline_node::AnalyticWindowSpec>,
    pub log: Vec<crate::config::LogDirective>,
    pub validations: Vec<crate::config::ValidationEntry>,
    pub dlq_node: Option<NodeIndex>,
    /// Phase 16b Task 16b.9 — compile-time-typechecked CXL program.
    /// Populated by `PipelineConfig::compile` via `bind_schema::bind_schema`
    /// and NEVER `None`: a transform whose CXL fails to typecheck
    /// surfaces as a compile-time E200 diagnostic and the enclosing
    /// `compile()` call returns `Err` before this payload is built.
    pub typed: Arc<TypedProgram>,
}

/// Phase 16b Wave 2 — fully-resolved Output payload.
#[derive(Debug, Clone)]
pub struct PlanOutputPayload {
    pub output: OutputConfig,
    pub validated_path: Option<crate::security::ValidatedPath>,
}

/// Phase 16b Wave 4ab — post-lowering intermediate representation of a
/// single transform/aggregate/route node, fed into
/// [`build_plan_from_nodes`]. Decouples the planner from the legacy
/// `TransformSpec`/`PipelineConfig` shape so the executor public
/// surface can be cut over without dragging the legacy types into the
/// plan body.
///
/// The shape mirrors the field set the planner actually reads off
/// `TransformSpec` (cxl source, aggregate sidecar, route sidecar,
/// analytic-window block, input wiring) plus a promoted [`ResolvedInput`]
/// enum that distinguishes the three wiring modes that the legacy
/// `Option<TransformInput>` encoded implicitly (none = chain, single =
/// explicit, multiple = merge).
#[derive(Debug, Clone)]
pub(crate) struct PlanTransformSpec {
    pub name: String,
    pub cxl_source: Option<String>,
    pub aggregate: Option<crate::config::AggregateConfig>,
    pub route: Option<crate::config::RouteConfig>,
    pub analytic_window: Option<serde_json::Value>,
    pub input: ResolvedInput,
    /// Task 16b.8 — 1-based source line of this node's YAML
    /// declaration, threaded off `Spanned<PipelineNode>::referenced`.
    /// Zero = unknown (legacy fallback path or a saphyr location
    /// lost inside a tagged-enum/flatten edge case). Consumed by
    /// `ExecutionPlanDag::compile` to populate `PlanNode::*.span` via
    /// `crate::span::Span::line_only`.
    pub line: u32,
}

/// Resolved upstream wiring for a [`PlanTransformSpec`]. The legacy
/// `Option<TransformInput>` encoding mapped `None` to "chain to previous
/// declaration" and `Some(...)` to explicit single/multi wiring; this
/// enum makes that explicit so the planner does not have to peek at
/// declaration order to disambiguate.
#[derive(Debug, Clone)]
pub(crate) enum ResolvedInput {
    /// Implicit linear chain — wire to the previous transform (or the
    /// primary source for the first transform).
    Chain,
    /// Explicit single upstream by name (transform, aggregate, or
    /// dotted `route_name.branch_name`).
    Single(String),
    /// Multiple upstreams unioned through a synthesized merge node.
    Multi(Vec<String>),
}

/// Phase 16b Wave 4ab Checkpoint C: construct the planner's
/// `Vec<PlanTransformSpec>` from a `PipelineConfig`. Prefers the unified
/// `nodes:` enum when non-empty (walking Transform/Aggregate/Route
/// variants directly). Falls back to the legacy `crate::executor::build_transform_specs(config)`
/// projection when the config was authored under the legacy
/// `transformations:` schema — which is the path the legacy test
/// fixtures exercise. When the legacy types are deleted in a later
/// checkpoint, the fallback branch dies with them.
fn build_specs(config: &PipelineConfig) -> Vec<PlanTransformSpec> {
    use crate::config::PipelineNode;

    // New-shape authorship wins when `nodes:` carries at least one
    // Transform/Aggregate/Route variant. Fixtures that only populate
    // `PipelineNode::Source` (e.g. the ingestion tests) still go through
    // the legacy fallback for their transform list.
    let has_new_shape_transforms = config.nodes.iter().any(|n| {
        matches!(
            &n.value,
            PipelineNode::Transform { .. }
                | PipelineNode::Aggregate { .. }
                | PipelineNode::Route { .. }
        )
    });
    if has_new_shape_transforms {
        // Source names: header inputs that resolve to a source must be
        // projected as `ResolvedInput::Chain` so the legacy wire-up
        // hooks them into the primary source, matching the behaviour
        // of the legacy projection used by the executor read model.
        let source_names: std::collections::HashSet<String> = config
            .nodes
            .iter()
            .filter_map(|n| match &n.value {
                PipelineNode::Source { header, .. } => Some(header.name.clone()),
                _ => None,
            })
            .collect();
        // Pre-index Merge nodes by name so a downstream transform
        // whose `input:` points at a synthesized merge resolves to
        // `ResolvedInput::Multi(merge.inputs)` — the planner's merge-
        // aware path then materializes the merge PlanNode from the
        // downstream spec exactly as in the legacy path.
        let merge_inputs_by_name: std::collections::HashMap<String, Vec<String>> = config
            .nodes
            .iter()
            .filter_map(|s| match &s.value {
                PipelineNode::Merge { header, .. } => {
                    let inputs = header
                        .inputs
                        .iter()
                        .map(|n| match n {
                            crate::config::node_header::NodeInput::Single(s) => s.clone(),
                            crate::config::node_header::NodeInput::Port { node, port } => {
                                format!("{node}.{port}")
                            }
                        })
                        .collect();
                    Some((header.name.clone(), inputs))
                }
                _ => None,
            })
            .collect();
        let resolve_header_input = |n: &crate::config::node_header::NodeInput| -> ResolvedInput {
            use crate::config::node_header::NodeInput;
            let flat = match n {
                NodeInput::Single(s) => s.clone(),
                NodeInput::Port { node, port } => format!("{node}.{port}"),
            };
            let bare = flat.split('.').next().unwrap_or(&flat);
            if source_names.contains(bare) {
                ResolvedInput::Chain
            } else if let Some(upstreams) = merge_inputs_by_name.get(&flat) {
                ResolvedInput::Multi(upstreams.clone())
            } else {
                ResolvedInput::Single(flat)
            }
        };
        let mut specs = Vec::with_capacity(config.nodes.len());
        for spanned in &config.nodes {
            let line = spanned.referenced.line() as u32;
            match &spanned.value {
                PipelineNode::Transform {
                    header,
                    config: body,
                } => {
                    specs.push(PlanTransformSpec {
                        name: header.name.clone(),
                        cxl_source: Some(body.cxl.source.as_str().to_string()),
                        aggregate: None,
                        route: None,
                        analytic_window: body.analytic_window.clone(),
                        input: resolve_header_input(&header.input),
                        line,
                    });
                }
                PipelineNode::Aggregate {
                    header,
                    config: body,
                } => {
                    let agg = AggregateConfig {
                        group_by: body.group_by.clone(),
                        cxl: body.cxl.source.as_str().to_string(),
                        strategy: body.strategy,
                    };
                    specs.push(PlanTransformSpec {
                        name: header.name.clone(),
                        cxl_source: None,
                        aggregate: Some(agg),
                        route: None,
                        analytic_window: None,
                        input: resolve_header_input(&header.input),
                        line,
                    });
                }
                PipelineNode::Route {
                    header,
                    config: body,
                } => {
                    let branches: Vec<crate::config::RouteBranch> = body
                        .conditions
                        .iter()
                        .map(|(name, cxl)| crate::config::RouteBranch {
                            name: name.clone(),
                            condition: cxl.source.as_str().to_string(),
                        })
                        .collect();
                    let route = crate::config::RouteConfig {
                        mode: body.mode,
                        branches,
                        default: body.default.clone(),
                    };
                    specs.push(PlanTransformSpec {
                        name: header.name.clone(),
                        cxl_source: Some(String::new()),
                        aggregate: None,
                        route: Some(route),
                        analytic_window: None,
                        input: resolve_header_input(&header.input),
                        line,
                    });
                }
                PipelineNode::Source { .. }
                | PipelineNode::Output { .. }
                | PipelineNode::Merge { .. }
                | PipelineNode::Composition { .. }
                // Phase Combine C.0.1: Combine lowers to `PlanNode::Combine`
                // in C.0.2, not to a `PlanTransformSpec`. No-op here.
                | PipelineNode::Combine { .. } => {}
            }
        }
        return specs;
    }

    // D3a part2: the legacy fallback is dead. `lift_legacy_fields_into_nodes`
    // populates `config.nodes` from any authored legacy shape before the
    // planner runs, so reaching here means the pipeline has no
    // transform-like nodes at all — return an empty spec list.
    Vec::new()
}

impl PlanTransformSpec {
    /// CXL source text. For row-level transforms this is the top-level
    /// `cxl` field; for aggregates it is the nested `aggregate.cxl`.
    pub(crate) fn cxl_source(&self) -> &str {
        if let Some(agg) = &self.aggregate {
            agg.cxl.as_str()
        } else if let Some(s) = &self.cxl_source {
            s.as_str()
        } else {
            ""
        }
    }

    /// True iff this spec carries an aggregate sidecar.
    pub(crate) fn is_aggregate(&self) -> bool {
        self.aggregate.is_some()
    }
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
            | PlanNode::Combine { name, .. } => name,
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
            | PlanNode::Combine { span, .. } => *span,
        }
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
            PlanNode::Combine {
                name,
                strategy,
                driving_input,
                ..
            } => {
                // Minimal display for C.0; C.5.1 enriches --explain rendering
                // with strategy details and per-input annotations.
                format!("[combine strategy={strategy:?} drive={driving_input}] {name}")
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
#[derive(Debug, Clone, Serialize)]
pub struct PlanEdge {
    pub dependency_type: DependencyType,
}

/// DAG-based execution plan — replaces ExecutionPlan.
///
/// The single source of truth for pipeline topology and execution strategy.
/// Custom Serialize emits flat node-list JSON for Kiln consumption.
#[derive(Debug)]
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
    /// If correlation sort was auto-injected, describes what was prepended.
    pub correlation_sort_note: Option<String>,
    /// Physical properties (ordering, partitioning) per node, keyed by
    /// `NodeIndex`. Populated by
    /// [`compute_node_properties`](ExecutionPlanDag::compute_node_properties)
    /// after transform compilation. Default-empty on construction.
    pub node_properties: HashMap<NodeIndex, crate::plan::properties::NodeProperties>,
}

impl ExecutionPlanDag {
    /// Whether any node requires arena allocation (window functions).
    pub fn required_arena(&self) -> bool {
        !self.indices_to_build.is_empty()
    }

    /// Whether the executor must dispatch the sorted-streaming path
    /// (correlation-key DLQ failure-domain batching).
    ///
    /// Post-Task 16.0.5.11 (research-driven re-architecture): the signal is
    /// pipeline-level — the presence of a planner-synthesized correlation
    /// [`PlanNode::Sort`] inserted by [`Self::inject_correlation_sort`].
    /// Operator-intrinsic sort requirements (`RequiresSortedInput`) are an
    /// orthogonal concern reserved for future merge-join / streaming-agg.
    /// See `docs/research/RESEARCH-pipeline-correlation-key-placement.md`.
    pub fn required_sorted_input(&self) -> bool {
        if self.required_arena() {
            return false;
        }
        self.correlation_sort_node().is_some()
    }

    /// Locate the planner-synthesized correlation [`PlanNode::Sort`] node, if
    /// any, returning `(node_index, sort_fields)`.
    ///
    /// Used by the executor to retrieve the active correlation sort order
    /// without re-deriving it from `error_handling.correlation_key`.
    pub fn correlation_sort_node(&self) -> Option<(NodeIndex, &[SortField])> {
        self.graph
            .node_indices()
            .find_map(|idx| match &self.graph[idx] {
                PlanNode::Sort {
                    name, sort_fields, ..
                } if name.starts_with(CORRELATION_SORT_PREFIX) => {
                    Some((idx, sort_fields.as_slice()))
                }
                _ => None,
            })
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
        // Aggregation nodes require the DAG walk path so the dispatch arm
        // (Task 16.3.13) handles them; the legacy streaming path would
        // otherwise row-evaluate the aggregate program and raise
        // "row-level expression, got aggregate function call".
        if self
            .graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::Aggregation { .. }))
        {
            return true;
        }
        // Combine nodes are multi-input and require the DAG-walk executor
        // path (V-1-3 hard requirement). Without this, combine pipelines
        // would route through the single-input streaming path and silently
        // skip the combine dispatch arm.
        if self
            .graph
            .node_weights()
            .any(|n| matches!(n, PlanNode::Combine { .. }))
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
        } else if self.required_sorted_input() {
            "SortedStreaming".to_string()
        } else {
            "Streaming".to_string()
        }
    }

    /// Compile a PipelineConfig + pre-compiled transforms into a DAG plan.
    ///
    /// Validates all `input:` references with full config context.
    /// Derives `NodeExecutionReqs` per transform from analyzer output.
    /// Builds the petgraph DAG with source, transform, route, merge, and output nodes.
    pub fn compile(
        config: &PipelineConfig,
        compiled: &[(&str, &TypedProgram)],
    ) -> Result<Self, PlanError> {
        Self::compile_with_runtime_schema(config, compiled, None)
    }

    /// Variant of [`compile`] that accepts the actual runtime Arrow
    /// schema for aggregate index alignment. Phase 16b Task 16b.9 —
    /// `typed.field_types` now reflects the author-declared source
    /// schema (which may be a superset of the real file columns), so
    /// aggregate `group_by_indices` must be resolved against the
    /// runtime column layout instead of the declared one.
    pub fn compile_with_runtime_schema(
        config: &PipelineConfig,
        compiled: &[(&str, &TypedProgram)],
        runtime_input_schema: Option<&[String]>,
    ) -> Result<Self, PlanError> {
        // D3a part2: collect source/output configs from the unified
        // `nodes:` topology once, then use the owned Vecs throughout.
        // Walking `config.nodes` repeatedly would work but wastes
        // allocation on every helper call that takes a slice.
        let source_configs: Vec<SourceConfig> = config.source_configs().cloned().collect();
        let output_configs_owned: Vec<OutputConfig> = config.output_configs().cloned().collect();

        let primary_source = source_configs
            .first()
            .map(|i| i.name.clone())
            .unwrap_or_default();

        // Phase D: analyze all transforms
        let report = analyzer::analyze_all(compiled);

        // Phase 16b Wave 4ab Checkpoint C: build `PlanTransformSpec` directly
        // from the unified `nodes:` enum when present; fall back to the
        // legacy `crate::executor::build_transform_specs(config)` projection otherwise. The helpers
        // below consume `&[PlanTransformSpec]` exclusively, so the planner
        // body is agnostic to authorship shape.
        let specs: Vec<PlanTransformSpec> = build_specs(config);

        // Parse analytic_window configs via the spec-taking helper.
        let window_configs: Vec<Option<LocalWindowConfig>> = specs
            .iter()
            .map(|s| index::parse_analytic_window_spec(s).map_err(PlanError::IndexPlanning))
            .collect::<Result<Vec<_>, _>>()?;

        // Exercise spec accessors + ResolvedInput so the types are live end-to-end.
        let _spec_probe: Vec<(&str, bool, usize)> = specs
            .iter()
            .map(|s| {
                let fanin = match &s.input {
                    ResolvedInput::Chain => 0,
                    ResolvedInput::Single(name) => name.len().min(1),
                    ResolvedInput::Multi(names) => names.len(),
                };
                (s.cxl_source(), s.is_aggregate(), fanin)
            })
            .collect();

        // Validate: if a transform uses window.* but has no local_window, error
        for (i, analysis) in report.transforms.iter().enumerate() {
            if !analysis.window_calls.is_empty() && window_configs[i].is_none() {
                return Err(PlanError::MissingLocalWindow {
                    transform: analysis.name.clone(),
                });
            }
        }

        // Phase E: build raw index requests, then deduplicate
        let mut raw_requests = Vec::new();
        for (i, wc) in window_configs.iter().enumerate() {
            if let Some(wc) = wc {
                let source = wc.source.clone().unwrap_or_else(|| primary_source.clone());

                // Validate source exists
                if !source_configs.iter().any(|inp| inp.name == source) {
                    return Err(PlanError::UnknownSource {
                        name: source,
                        transform: report.transforms[i].name.clone(),
                    });
                }

                // Compute arena fields: group_by + sort_by field names + window-accessed fields
                let mut arena_fields: HashSet<String> = HashSet::new();
                for gb in &wc.group_by {
                    arena_fields.insert(gb.clone());
                }
                for sf in &wc.sort_by {
                    arena_fields.insert(sf.field.clone());
                }
                for f in &report.transforms[i].accessed_fields {
                    arena_fields.insert(f.clone());
                }

                // Check pre-sorted optimization
                let already_sorted = check_already_sorted(&source_configs, &source, &wc.sort_by);

                raw_requests.push(RawIndexRequest {
                    source,
                    group_by: wc.group_by.clone(),
                    sort_by: wc.sort_by.clone(),
                    arena_fields: arena_fields.into_iter().collect(),
                    already_sorted,
                    transform_index: i,
                });
            }
        }

        let indices = index::deduplicate_indices(raw_requests.clone());

        // Build source DAG
        let source_dag = build_source_dag(&source_configs, &window_configs, &primary_source)?;

        // Build output projections
        let output_projections = output_configs_owned
            .iter()
            .map(|o| OutputSpec {
                name: o.name.clone(),
                mapping: o.mapping.clone().unwrap_or_default(),
                exclude: o.exclude.clone().unwrap_or_default(),
                include_unmapped: o.include_unmapped,
            })
            .collect();

        // --- Build the petgraph DAG ---
        // Phase 1: Add all nodes. Phase 2: Wire all edges.
        // Two-phase approach allows forward references and cycle detection.
        let mut graph = DiGraph::<PlanNode, PlanEdge>::new();
        let mut node_by_name: HashMap<String, NodeIndex> = HashMap::new();
        let mut slug_set: HashSet<String> = HashSet::new();

        // Helper to insert a node with slug uniqueness check
        let add_node = |graph: &mut DiGraph<PlanNode, PlanEdge>,
                        node: PlanNode,
                        node_by_name: &mut HashMap<String, NodeIndex>,
                        slug_set: &mut HashSet<String>|
         -> Result<NodeIndex, PlanError> {
            let slug = node.id_slug();
            if !slug_set.insert(slug.clone()) {
                return Err(PlanError::DuplicateIdSlug { slug });
            }
            let key = format!("{}.{}", node.type_tag(), node.name());
            let idx = graph.add_node(node);
            node_by_name.insert(key, idx);
            Ok(idx)
        };

        // --- Phase 1: Add all nodes ---

        // Task 16b.8 — map node name → 1-based YAML source line, used
        // to populate `PlanNode.span` via `Span::line_only`. Built once
        // per compile() from `config.nodes`, which carries
        // `Spanned<PipelineNode>` entries whose `referenced.line()`
        // is the saphyr-reported source line.
        let line_by_name: HashMap<String, u32> = config
            .nodes
            .iter()
            .map(|s| (s.value.name().to_string(), s.referenced.line() as u32))
            .collect();
        let span_for = |name: &str| -> Span {
            line_by_name
                .get(name)
                .copied()
                .filter(|l| *l > 0)
                .map(Span::line_only)
                // (c) serde-saphyr loses the top-level Mapping line
                // when a `#[serde(tag = ...)] + #[serde(flatten)]` node
                // header is parsed; fall back to synthetic rather than
                // fabricating a bogus line.
                .unwrap_or(Span::SYNTHETIC)
        };

        // Source nodes
        for input in &source_configs {
            add_node(
                &mut graph,
                PlanNode::Source {
                    name: input.name.clone(),
                    span: span_for(&input.name),
                    resolved: None,
                },
                &mut node_by_name,
                &mut slug_set,
            )?;
        }

        // Transform nodes in declaration order (deterministic toposort per V-7-2)
        for (i, tc) in specs.iter().enumerate() {
            let analysis = &report.transforms[i];
            let wc = &window_configs[i];
            let parallelism_class = derive_parallelism_class(analysis, wc, &primary_source);

            let execution_reqs = if wc.is_some() {
                NodeExecutionReqs::RequiresArena
            } else {
                NodeExecutionReqs::Streaming
            };

            let window_index = if let Some(wc) = wc {
                let source = wc.source.clone().unwrap_or_else(|| primary_source.clone());
                index::find_index_for(&indices, &source, &wc.group_by, &wc.sort_by)
            } else {
                None
            };

            let partition_lookup = wc.as_ref().map(|wc| {
                let source = wc.source.clone().unwrap_or_else(|| primary_source.clone());
                if source == primary_source && wc.on.is_none() {
                    PartitionLookupKind::SameSource
                } else {
                    PartitionLookupKind::CrossSource {
                        on_expr: wc.on.clone(),
                    }
                }
            });

            // Extract the per-transform write set from the matching CXL
            // TypedProgram. The `compiled` slice is in the same declaration
            // order as `crate::executor::build_transform_specs(config)` (see analyzer::analyze_all).
            let write_set = extract_write_set(compiled[i].1);
            let has_distinct = extract_has_distinct(compiled[i].1);

            // ── Phase 16 Task 16.3.6: route aggregate transforms to
            //    `PlanNode::Aggregation` instead of `PlanNode::Transform`.
            //    D4: aggregates use a single dedicated plan node. D6:
            //    routes and multi-input merges are forbidden on aggregates.
            // Convert the spec's 1-based source line to a
            // `Span::line_only` so `--explain`'s annotation pass can
            // render `(line:N)` against this node.
            //
            // (c) Zero line means the spec was built from a saphyr
            // tagged-enum/flatten edge case where the YAML location is
            // UNKNOWN; no real span is recoverable at this layer.
            let spec_span = if tc.line > 0 {
                Span::line_only(tc.line)
            } else {
                Span::SYNTHETIC
            };
            if let Some(agg_cfg) = tc.aggregate.as_ref() {
                if tc.route.is_some() {
                    return Err(PlanError::AggregateWithRoute {
                        transform: tc.name.clone(),
                    });
                }
                if matches!(tc.input, ResolvedInput::Multi(_)) {
                    return Err(PlanError::AggregateWithMultipleInputs {
                        transform: tc.name.clone(),
                    });
                }

                let typed = compiled[i].1;
                // Synthetic input schema: deterministic ordering of every
                // field referenced by the CXL program. Task 16.3.13 will
                // reconcile this with the upstream node's true schema; for
                // unit-test scope the projection is the identity since the
                // executor projects records into this same order before
                // passing them into the hash aggregator.
                // Phase 16b Task 16b.9: prefer the runtime Arrow
                // schema when available. `typed.field_types` now comes
                // from the author-declared source schema which may
                // contain columns not present in the actual file; the
                // aggregator projects records by positional index, so
                // group_by_indices must be resolved against the
                // runtime layout, not the declared superset.
                let input_schema: Vec<String> = match runtime_input_schema {
                    Some(rt) => rt.to_vec(),
                    None => typed.field_types.keys().cloned().collect(),
                };

                let compiled_agg =
                    cxl::plan::extract_aggregates(typed, &agg_cfg.group_by, &input_schema)
                        .map_err(|diags| PlanError::AggregateExtractionFailed {
                            transform: tc.name.clone(),
                            diagnostics: diags.into_iter().map(|d| d.message).collect(),
                        })?;

                // Output schema: non-meta emit names in declaration order.
                let output_columns: Vec<Box<str>> = typed
                    .program
                    .statements
                    .iter()
                    .filter_map(|s| match s {
                        Statement::Emit {
                            name,
                            is_meta: false,
                            ..
                        } => Some(name.clone()),
                        _ => None,
                    })
                    .collect();
                let output_schema = Arc::new(Schema::new(output_columns));

                add_node(
                    &mut graph,
                    PlanNode::Aggregation {
                        name: tc.name.clone(),
                        span: spec_span,
                        config: agg_cfg.clone(),
                        compiled: Arc::new(compiled_agg),
                        strategy: AggregateStrategy::Hash,
                        output_schema,
                        fallback_reason: None,
                        skipped_streaming_available: false,
                        qualified_sort_order: None,
                    },
                    &mut node_by_name,
                    &mut slug_set,
                )?;

                continue;
            }

            add_node(
                &mut graph,
                PlanNode::Transform {
                    name: tc.name.clone(),
                    span: spec_span,
                    resolved: None,
                    parallelism_class,
                    tier: 0, // assigned later via BFS
                    execution_reqs,
                    window_index,
                    partition_lookup,
                    write_set,
                    has_distinct,
                },
                &mut node_by_name,
                &mut slug_set,
            )?;

            // Route node for this transform (if configured)
            if let Some(ref rc) = tc.route {
                add_node(
                    &mut graph,
                    PlanNode::Route {
                        name: format!("route_{}", tc.name),
                        span: spec_span,
                        mode: rc.mode,
                        branches: rc.branches.iter().map(|b| b.name.clone()).collect(),
                        default: rc.default.clone(),
                    },
                    &mut node_by_name,
                    &mut slug_set,
                )?;
            }

            // Merge node for multiple inputs. Planner-synthesized:
            // the merge PlanNode is emitted from the downstream
            // consumer's span, since there is no source-level Merge
            // declaration in this legacy code path (the `Multi`
            // `ResolvedInput` was derived from the consumer's
            // `inputs:` list upstream).
            if let ResolvedInput::Multi(_) = &tc.input {
                add_node(
                    &mut graph,
                    PlanNode::Merge {
                        name: format!("merge_{}", tc.name),
                        span: spec_span,
                    },
                    &mut node_by_name,
                    &mut slug_set,
                )?;
            }
        }

        // Output nodes
        for output in &output_configs_owned {
            add_node(
                &mut graph,
                PlanNode::Output {
                    name: output.name.clone(),
                    span: span_for(&output.name),
                    resolved: None,
                },
                &mut node_by_name,
                &mut slug_set,
            )?;
        }

        // --- Phase 2: Wire all edges ---
        let mut prev_transform_key: Option<String> = None;

        for (i, tc) in specs.iter().enumerate() {
            let wc = &window_configs[i];
            // Aggregate transforms register under `aggregation.{name}` instead
            // of `transform.{name}` (Phase 16 Task 16.3.6).
            let transform_key = if tc.aggregate.is_some() {
                format!("aggregation.{}", tc.name)
            } else {
                format!("transform.{}", tc.name)
            };
            let transform_idx = node_by_name[&transform_key];

            // Wire edges from input: or implicit linear chain
            match &tc.input {
                ResolvedInput::Single(upstream) => {
                    let upstream_idx =
                        resolve_input_reference(upstream, &node_by_name, &tc.name, &specs)?;
                    graph.add_edge(
                        upstream_idx,
                        transform_idx,
                        PlanEdge {
                            dependency_type: DependencyType::Data,
                        },
                    );
                }
                ResolvedInput::Multi(upstreams) => {
                    let merge_key = format!("merge.merge_{}", tc.name);
                    let merge_idx = node_by_name[&merge_key];
                    for upstream in upstreams {
                        let upstream_idx =
                            resolve_input_reference(upstream, &node_by_name, &tc.name, &specs)?;
                        graph.add_edge(
                            upstream_idx,
                            merge_idx,
                            PlanEdge {
                                dependency_type: DependencyType::Data,
                            },
                        );
                    }
                    graph.add_edge(
                        merge_idx,
                        transform_idx,
                        PlanEdge {
                            dependency_type: DependencyType::Data,
                        },
                    );
                }
                ResolvedInput::Chain => {
                    // Implicit linear chain: wire to previous transform or source
                    if let Some(ref prev_key) = prev_transform_key
                        && let Some(&prev_idx) = node_by_name.get(prev_key)
                    {
                        graph.add_edge(
                            prev_idx,
                            transform_idx,
                            PlanEdge {
                                dependency_type: DependencyType::Data,
                            },
                        );
                    } else if prev_transform_key.is_none() {
                        // First transform: wire to primary source
                        let source_key = format!("source.{}", primary_source);
                        if let Some(&source_idx) = node_by_name.get(&source_key) {
                            graph.add_edge(
                                source_idx,
                                transform_idx,
                                PlanEdge {
                                    dependency_type: DependencyType::Data,
                                },
                            );
                        }
                    }
                }
            }

            // Cross-source Index edges from window configs
            if let Some(wc) = wc {
                let source = wc.source.clone().unwrap_or_else(|| primary_source.clone());
                if source != primary_source {
                    let source_key = format!("source.{}", source);
                    if let Some(&source_idx) = node_by_name.get(&source_key) {
                        graph.add_edge(
                            source_idx,
                            transform_idx,
                            PlanEdge {
                                dependency_type: DependencyType::Index,
                            },
                        );
                    }
                }
            }

            // Wire transform → route if configured
            if tc.route.is_some() {
                let route_key = format!("route.route_{}", tc.name);
                let route_idx = node_by_name[&route_key];
                graph.add_edge(
                    transform_idx,
                    route_idx,
                    PlanEdge {
                        dependency_type: DependencyType::Data,
                    },
                );
            }

            prev_transform_key = Some(transform_key);
        }

        // Wire output nodes to last transform
        for output in &output_configs_owned {
            let output_key = format!("output.{}", output.name);
            let output_idx = node_by_name[&output_key];

            if let Some(ref prev_key) = prev_transform_key
                && let Some(&prev_idx) = node_by_name.get(prev_key)
            {
                graph.add_edge(
                    prev_idx,
                    output_idx,
                    PlanEdge {
                        dependency_type: DependencyType::Data,
                    },
                );
            }
        }

        // Topological sort with cycle detection
        let topo_order = toposort(&graph, None).map_err(|cycle| {
            // Extract cycle path using DFS
            let cycle_path = extract_cycle_path(&graph, cycle.node_id());
            PlanError::CycleDetected { path: cycle_path }
        })?;

        // Assign tiers via BFS: each node's tier = max(predecessor tiers) + 1
        assign_tiers(&mut graph, &topo_order);

        // Build parallelism profile from DAG graph nodes
        let parallelism = ParallelismProfile {
            per_transform: topo_order
                .iter()
                .filter_map(|&idx| match &graph[idx] {
                    PlanNode::Transform {
                        parallelism_class, ..
                    } => Some(*parallelism_class),
                    _ => None,
                })
                .collect(),
            worker_threads: config
                .pipeline
                .concurrency
                .as_ref()
                .and_then(|c| c.threads)
                .unwrap_or(4),
        };

        let mut dag = ExecutionPlanDag {
            graph,
            topo_order,
            source_dag,
            indices_to_build: indices,
            output_projections,
            parallelism,
            correlation_sort_note: None,
            node_properties: HashMap::new(),
        };

        // Task 16.0.5.10: enforcer-insertion → property derivation, in that
        // order, exactly once, before the executor sees the DAG. D50 mandates
        // a single sequential call chain with explicit intermediate binding
        // (not a type-state newtype). `insert_enforcer_sorts` is structurally
        // idempotent (prefix-guarded) and `compute_node_properties` asserts
        // its own double-call guard, so this pair is safe but must run in
        // order: properties derived from a DAG missing enforcer Sorts would
        // mis-attribute ordering provenance.
        let inputs_map: HashMap<String, SourceConfig> = source_configs
            .iter()
            .map(|i| (i.name.clone(), i.clone()))
            .collect();
        // Pipeline-level policy injection (correlation key for failure-domain
        // DLQ batching) runs first: it materializes a `PlanNode::Sort` from
        // declared config, distinct from per-operator algorithm requirements.
        // See docs/research/RESEARCH-pipeline-correlation-key-placement.md.
        dag.inject_correlation_sort(&config.error_handling, &source_configs)
            .map_err(|e| PlanError::PropertyDerivation(e.to_string()))?;
        dag.insert_enforcer_sorts(&inputs_map)
            .map_err(|e| PlanError::PropertyDerivation(e.to_string()))?;
        // Enforcer may have grown the graph; re-derive topo + tiers before
        // property derivation walks it.
        dag.topo_order = toposort(&dag.graph, None).map_err(|cycle| {
            let cycle_path = extract_cycle_path(&dag.graph, cycle.node_id());
            PlanError::CycleDetected { path: cycle_path }
        })?;
        assign_tiers(&mut dag.graph, &dag.topo_order);
        dag.compute_node_properties(&inputs_map)
            .map_err(|e| PlanError::PropertyDerivation(e.to_string()))?;
        // Task 16.4.9: post-pass that resolves the user `strategy` hint on
        // each `PlanNode::Aggregation` against upstream `OrderingProvenance`
        // and rewrites the node's stored ordering provenance accordingly.
        // DataFusion `PhysicalOptimizerRule` pattern: a frozen-plan walk
        // that mutates only the strategy + side-table ordering, never the
        // graph topology.
        dag.select_aggregation_strategies()
            .map_err(|e| PlanError::PropertyDerivation(e.to_string()))?;

        Ok(dag)
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

        if let Some(note) = &self.correlation_sort_note {
            out.push_str(&format!("{note}\n\n"));
        }

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
            out.push_str(&format!("  Source: {}\n", spec.source));
            out.push_str(&format!("  Group by: {:?}\n", spec.group_by));
            out.push_str(&format!(
                "  Sort by: {:?}\n",
                spec.sort_by.iter().map(|s| &s.field).collect::<Vec<_>>()
            ));
            out.push_str(&format!("  Arena fields: {:?}\n", spec.arena_fields));
            out.push_str(&format!("  Already sorted: {}\n\n", spec.already_sorted));
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

        // Show planner-synthesized Sort nodes (drill pass 3, 16.0.5.12).
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

        // Physical Properties (NodeProperties side-table) — task 16.0.5.12.
        // Only emitted when the property pass has run (post-compile DAGs).
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

        out
    }

    /// Full `--explain` output combining execution plan with config context.
    pub fn explain_full(&self, config: &PipelineConfig) -> String {
        let mut out = self.explain();

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
    /// Task 16b.8 polish:
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
                    // Combine renders as a sibling topo line. C.5.1 enriches
                    // this with per-input dependency trace and strategy
                    // details; C.0 keeps it minimal.
                    out.push_str(&format!("  ◈ {}{line_suffix}\n", node.display_name()));
                }
                PlanNode::Source { .. }
                | PlanNode::Transform { .. }
                | PlanNode::Output { .. }
                | PlanNode::Sort { .. }
                | PlanNode::Composition { .. } => {
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

/// Resolve an input reference to a NodeIndex.
///
/// Handles both plain transform names ("transform_name") and
/// dotted branch references ("categorize.high_value").
fn resolve_input_reference(
    reference: &str,
    node_by_name: &HashMap<String, NodeIndex>,
    current_transform: &str,
    transforms: &[PlanTransformSpec],
) -> Result<NodeIndex, PlanError> {
    // Self-reference check
    if reference == current_transform {
        return Err(PlanError::SelfReference {
            transform: current_transform.to_string(),
        });
    }

    // Try plain transform reference first
    let transform_key = format!("transform.{}", reference);
    if let Some(&idx) = node_by_name.get(&transform_key) {
        return Ok(idx);
    }

    // Try aggregate transform reference (Phase 16 Task 16.3.6)
    let aggregation_key = format!("aggregation.{}", reference);
    if let Some(&idx) = node_by_name.get(&aggregation_key) {
        return Ok(idx);
    }

    // Try dotted branch reference: "route_name.branch_name" -> route node
    if reference.contains('.') {
        let parts: Vec<&str> = reference.splitn(2, '.').collect();
        let route_key = format!("route.route_{}", parts[0]);
        if let Some(&idx) = node_by_name.get(&route_key) {
            return Ok(idx);
        }
    }

    // Collect available transforms and branches for error message
    let available: Vec<String> = transforms.iter().map(|t| t.name.clone()).collect();

    Err(PlanError::InvalidInputReference {
        reference: reference.to_string(),
        transform: current_transform.to_string(),
        available,
    })
}

/// Extract the cycle path from a DFS back-edge detection.
///
/// Uses `depth_first_search` with `DfsEvent::BackEdge` + predecessor map
/// to extract the full cycle path. Formats as `"A" --> "B" --> "A"`.
fn extract_cycle_path(graph: &DiGraph<PlanNode, PlanEdge>, start: NodeIndex) -> String {
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
fn assign_tiers(graph: &mut DiGraph<PlanNode, PlanEdge>, topo_order: &[NodeIndex]) {
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
fn derive_parallelism_class(
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
fn build_source_dag(
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

/// Reserved name prefix for planner-synthesized [`PlanNode::Sort`] nodes
/// inserted by [`ExecutionPlanDag::insert_enforcer_sorts`] to satisfy a
/// transform's per-operator [`NodeExecutionReqs::RequiresSortedInput`].
pub const ENFORCER_SORT_PREFIX: &str = "__sort_for_";

/// Reserved name prefix for planner-synthesized [`PlanNode::Sort`] nodes
/// inserted by [`ExecutionPlanDag::inject_correlation_sort`] to materialize
/// the pipeline-level `error_handling.correlation_key` failure-domain
/// grouping policy. Distinct from [`ENFORCER_SORT_PREFIX`] because the two
/// passes encode different concerns: operator-intrinsic algorithm needs vs.
/// declared pipeline-level policy. See
/// `docs/research/RESEARCH-pipeline-correlation-key-placement.md`.
pub const CORRELATION_SORT_PREFIX: &str = "__correlation_sort_";

impl ExecutionPlanDag {
    /// Inject a [`PlanNode::Sort`] for the pipeline-level
    /// `error_handling.correlation_key` failure-domain grouping policy.
    ///
    /// This is the *direct compile-time injection* path for pipeline-scoped
    /// ordering policy, distinct from
    /// [`insert_enforcer_sorts`](Self::insert_enforcer_sorts) which serves
    /// per-operator algorithm-intrinsic requirements. See
    /// `docs/research/RESEARCH-pipeline-correlation-key-placement.md` for
    /// the cross-ecosystem prior art motivating this split.
    ///
    /// # Behavior
    /// - No-op if `error_handling.correlation_key` is unset.
    /// - No-op if the primary source's declared `sort_order` already starts
    ///   with the correlation key fields (idempotent satisfaction).
    /// - Otherwise inserts one [`PlanNode::Sort`] node immediately downstream
    ///   of the primary source ([`PipelineConfig::inputs[0]`]) and reroutes
    ///   every existing data edge from that source through it. Sort fields =
    ///   `[correlation_key_fields..., declared_sort_order_remainder...]` to
    ///   match the deleted `maybe_inject_correlation_sort` semantics.
    ///
    /// # Idempotency
    /// A second call adds zero new nodes: the inserted [`PlanNode::Sort`]
    /// stands between the source and any consumer, so the source no longer
    /// has direct Transform consumers to reroute. The reserved-prefix guard
    /// (validated by [`insert_enforcer_sorts`]) prevents user-declared name
    /// collisions.
    ///
    /// # Errors
    /// Returns [`PipelineError::Compilation`] if `inputs[0]` is not present
    /// in the DAG (should be impossible after a successful node-building
    /// pass; treated as an internal invariant violation).
    pub fn inject_correlation_sort(
        &mut self,
        error_handling: &crate::config::ErrorHandlingConfig,
        sources: &[SourceConfig],
    ) -> Result<(), PipelineError> {
        let Some(correlation_key) = error_handling.correlation_key.as_ref() else {
            return Ok(());
        };
        let Some(primary_input) = sources.first() else {
            return Ok(());
        };

        // Build the desired sort_fields list: correlation key first, then any
        // user-declared trailing fields not already in the key prefix.
        let key_field_names: Vec<String> = correlation_key
            .fields()
            .into_iter()
            .map(String::from)
            .collect();
        let declared: Vec<SortField> = primary_input
            .sort_order
            .as_ref()
            .map(|specs| specs.iter().cloned().map(|s| s.into_sort_field()).collect())
            .unwrap_or_default();

        // Idempotent satisfaction: declared sort already starts with the
        // correlation key fields (direction-agnostic, matching the deleted
        // is_sorted_by_correlation_key behavior).
        let already_satisfied = key_field_names.len() <= declared.len()
            && key_field_names
                .iter()
                .zip(declared.iter())
                .all(|(k, sf)| k == &sf.field);
        if already_satisfied {
            return Ok(());
        }

        // Locate the primary source node by name.
        let primary_name = &primary_input.name;
        let source_idx = self.graph.node_indices().find(
            |&idx| matches!(&self.graph[idx], PlanNode::Source { name, .. } if name == primary_name),
        );
        let Some(source_idx) = source_idx else {
            return Err(PipelineError::Compilation {
                transform_name: primary_name.clone(),
                messages: vec![format!(
                    "inject_correlation_sort: primary source '{primary_name}' not found in DAG"
                )],
            });
        };

        // Build sort fields: key first, then any declared trailing fields not
        // already named in the key prefix.
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

        // Snapshot all outgoing data edges from the source. Reroute each
        // through a single inserted Sort node, preserving DependencyType.
        let outgoing: Vec<(NodeIndex, DependencyType)> = self
            .graph
            .edges_directed(source_idx, petgraph::Direction::Outgoing)
            .map(|e| (e.target(), e.weight().dependency_type))
            .collect();

        // Idempotency: if every outgoing edge already lands on a
        // CORRELATION_SORT_PREFIX Sort node, no work to do.
        if !outgoing.is_empty()
            && outgoing.iter().all(|(t, _)| {
                matches!(&self.graph[*t], PlanNode::Sort { name, .. } if name.starts_with(CORRELATION_SORT_PREFIX))
            })
        {
            return Ok(());
        }

        if outgoing.is_empty() {
            // Nothing downstream — nothing to enforce ordering for.
            return Ok(());
        }

        // Task 16b.8 — planner-synthesized correlation sort enforcer.
        // This node has no source-level declaration; it is derived
        // from the primary source's correlation key and inserted on
        // every outgoing edge.
        // (a) Planner-synthesized: correlation-sort nodes have no
        // author-written YAML origin; `Span::SYNTHETIC` is the correct span
        // here because no YAML node exists for it.
        let sort_node = PlanNode::Sort {
            name: format!("{CORRELATION_SORT_PREFIX}{primary_name}"),
            span: Span::SYNTHETIC,
            sort_fields,
        };
        let sort_idx = self.graph.add_node(sort_node);

        for (target, dep_type) in outgoing {
            // Skip rerouting if the target is already the correlation Sort
            // (defensive — outgoing was filtered above, but petgraph allows
            // multi-edges).
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
                },
            );
        }
        // Add the single Source -> Sort edge (Data dependency).
        self.graph.add_edge(
            source_idx,
            sort_idx,
            PlanEdge {
                dependency_type: DependencyType::Data,
            },
        );

        Ok(())
    }

    /// Phase 16.0.5 enforcer-sort insertion (drill pass 3, D46/D47).
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
        // collide with a user-declared name.
        for idx in self.graph.node_indices() {
            let node = &self.graph[idx];
            if matches!(node, PlanNode::Sort { .. }) {
                continue;
            }
            let name = node.name();
            for prefix in [ENFORCER_SORT_PREFIX, CORRELATION_SORT_PREFIX] {
                if name.starts_with(prefix) {
                    return Err(PipelineError::Compilation {
                        transform_name: name.to_string(),
                        messages: vec![format!(
                            "node name '{}' uses reserved prefix '{}' (planner-synthesized sort enforcers only)",
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
            // Today's only requirement class (Phase 14 correlated DLQ) consumes
            // a Source directly. Find the unique direct Source parent; skip
            // otherwise (later phases extending this will widen the walk).
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
            // its dependency type for re-use on the rewritten edges.
            let edge_id = self
                .graph
                .find_edge(source_idx, consumer_idx)
                .expect("source parent must have outgoing edge to consumer");
            let dep_type = self.graph[edge_id].dependency_type;
            self.graph.remove_edge(edge_id);

            // Task 16b.8 — planner-synthesized sort enforcer (D46/D47).
            // No YAML node exists for it; it is derived from the
            // consumer's `RequiresSortedInput` requirement and
            // inserted on the edge from the upstream source.
            // (a) Planner-synthesized: sort enforcer nodes have no
            // author-written YAML origin; `Span::SYNTHETIC` is correct.
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
                },
            );
            self.graph.add_edge(
                sort_idx,
                consumer_idx,
                PlanEdge {
                    dependency_type: dep_type,
                },
            );
        }

        Ok(())
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
    /// `AliasAwareOutputExpression` (see
    /// `docs/research/RESEARCH-property-derivation-layering.md`).
    ///
    /// # Panics / errors
    ///
    /// Asserts `self.node_properties.is_empty()` on entry as a double-call
    /// guard. Returns [`PipelineError::Compilation`] only if a future variant
    /// rule fails (currently infallible).
    pub fn compute_node_properties(
        &mut self,
        inputs: &HashMap<String, SourceConfig>,
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

    /// Task 16.4.9 — resolve `AggregateStrategyHint` on every
    /// `PlanNode::Aggregation` against upstream `OrderingProvenance`,
    /// rewrite the node's `strategy` field, populate the auxiliary
    /// `fallback_reason` / `skipped_streaming_available` /
    /// `qualified_sort_order` fields, and overwrite the side-table
    /// `node_properties[idx].ordering` to reflect the resolved strategy.
    ///
    /// Runs as a separate post-pass inside `compile()` immediately after
    /// `compute_node_properties()` (D75). Hard-errors at compile time
    /// when a user explicitly requests `strategy: streaming` on an
    /// ineligible input (D78), via the rustc-shaped walker
    /// `render_unordered_streaming_error` shipped in 16.4.9a.
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
    inputs: &HashMap<String, SourceConfig>,
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

    match node {
        PlanNode::Source { name, .. } => {
            let sort_order: Option<Vec<SortField>> = inputs
                .get(name)
                .and_then(|ic| ic.sort_order.as_ref())
                .map(|specs| specs.iter().cloned().map(|s| s.into_sort_field()).collect());
            let provenance = if sort_order.is_some() {
                OrderingProvenance::DeclaredOnInput {
                    input_name: name.clone(),
                }
            } else {
                OrderingProvenance::NoOrdering
            };
            NodeProperties {
                ordering: Ordering {
                    sort_order,
                    provenance,
                },
                partitioning: single_stream_partitioning(),
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
        },

        PlanNode::Transform {
            name,
            write_set,
            has_distinct,
            ..
        } => {
            let partitioning = parent_partitioning();

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
                }
            }
        }

        PlanNode::Aggregation { .. } => {
            // D77: aggregation node ordering is the sole responsibility of
            // the `select_aggregation_strategies` post-pass (Task 16.4.9),
            // which runs immediately after `compute_node_properties` and
            // overwrites this entry based on the resolved strategy. The
            // defensive default is "no ordering, single stream" so any
            // bug that bypasses the post-pass produces conservative
            // (correct-but-suboptimal) downstream eligibility decisions
            // rather than silently asserting a false ordering.
            NodeProperties {
                ordering: Ordering {
                    sort_order: None,
                    provenance: OrderingProvenance::NoOrdering,
                },
                partitioning: single_stream_partitioning(),
            }
        }

        PlanNode::Output { name, .. } => {
            // Terminal — properties still computed for debugging. Inherit
            // parent, rewrite provenance to point at this node when ordering
            // is non-None so explain chains through.
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
            }
        }

        PlanNode::Composition { name, .. } => {
            // Composition is opaque at the top-level property pass.
            // Inherit parent ordering/partitioning — body-internal
            // properties live in BoundBody.bound_schemas, not here.
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
            }
        }

        PlanNode::Combine { .. } => {
            // C.0: conservative default — combine destroys parent ordering
            // and yields a single stream. C.1 replaces this with a proper
            // `DestroyedByCombine { at_node, confidence: Proven }` ordering
            // provenance once the provenance enum gains that variant
            // (drill D20).
            // TODO(C.1): add `OrderingProvenance::DestroyedByCombine` and
            // emit it from here with `at_node = name.clone()`.
            NodeProperties::unordered_single()
        }
    }
}

/// Idempotency predicate for Phase 16.0.5 enforcer-sort insertion.
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
/// `DestroyedByTransformWriteSet` provenance variant in Phase 16.0.5.7. The
/// write set lives directly on `PlanNode::Transform` so the property pass
/// never has to reach into executor-private types
/// (see `docs/research/RESEARCH-property-derivation-layering.md`).
fn extract_write_set(typed: &TypedProgram) -> BTreeSet<String> {
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

/// Detect whether a CXL transform contains any `distinct` statement.
///
/// Sibling of [`extract_write_set`] — sourced from the same `TypedProgram`
/// during plan compilation. Persisted as `has_distinct` on
/// [`PlanNode::Transform`] so the property pass can emit
/// [`OrderingProvenance::DestroyedByDistinct`] without reaching into
/// executor-private types (Phase 16.0.5.7, see
/// `docs/research/RESEARCH-property-derivation-layering.md`).
fn extract_has_distinct(typed: &TypedProgram) -> bool {
    typed
        .program
        .statements
        .iter()
        .any(|s| matches!(s, Statement::Distinct { .. }))
}

/// Check if a source's declared sort_order matches the window's sort_by.
fn check_already_sorted(_sources: &[SourceConfig], _source: &str, _sort_by: &[SortField]) -> bool {
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
    /// `cxl::plan::extract_aggregates` rejected the typed program (Phase 16).
    AggregateExtractionFailed {
        transform: String,
        diagnostics: Vec<String>,
    },
    /// Aggregate transforms cannot have a `route:` block (D6).
    AggregateWithRoute {
        transform: String,
    },
    /// Aggregate transforms cannot have a multi-input merge (D6).
    AggregateWithMultipleInputs {
        transform: String,
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
        }
    }
}

impl std::error::Error for PlanError {}
