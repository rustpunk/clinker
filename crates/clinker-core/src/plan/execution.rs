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
    TransformInput,
};
use crate::error::PipelineError;
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
    /// Constructed by `ExecutionPlanDag::compile()` when a `LegacyTransformsBlock`
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
/// type-checked CXL program (Arc-shared with the analyzer), the optional
/// analytic-window spec (renamed from local_window), the log directives
/// the validations sidebar, and the DLQ NodeId for downstream wiring.
#[derive(Debug, Clone)]
pub struct PlanTransformPayload {
    /// Type-checked CXL program. Two-phase contract (Phase 16b Wave 4ab,
    /// M4): `None` after `PipelineConfig::compile()`, populated by
    /// `CompiledPlan::bind_schema()` which the executor invokes on entry
    /// once readers have produced a `Schema`. Schema is a runtime artifact
    /// (reader-derived), so CXL typecheck cannot run at compile() time.
    pub typed: Option<Arc<cxl::typecheck::pass::TypedProgram>>,
    pub analytic_window: Option<crate::config::pipeline_node::AnalyticWindowSpec>,
    pub log: Vec<crate::config::LogDirective>,
    pub validations: Vec<crate::config::ValidationEntry>,
    pub dlq_node: Option<NodeIndex>,
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
/// `LegacyTransformsBlock`/`PipelineConfig` shape so the executor public
/// surface can be cut over without dragging the legacy types into the
/// plan body.
///
/// The shape mirrors the field set the planner actually reads off
/// `LegacyTransformsBlock` (cxl source, aggregate sidecar, route sidecar, log
/// directives, validations, analytic-window block, input wiring) plus a
/// promoted [`ResolvedInput`] enum that distinguishes the three wiring
/// modes that the legacy `Option<TransformInput>` encoded implicitly
/// (none = chain, single = explicit, multiple = merge). The two
/// span fields exist for the eventual diagnostic surface; Wave 4ab fills
/// them with `Span::SYNTHETIC` from the legacy bridge constructor.
// Wave 4ab M2 WIP: remaining fields (route, log, validations, input_span,
// body_span) are consumed as Milestones 3-6 migrate the helpers off
// LegacyTransformsBlock. Keeping the full field set live avoids a second structural
// churn when those milestones land.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct PlanTransformSpec {
    pub name: String,
    pub cxl_source: Option<String>,
    pub aggregate: Option<crate::config::AggregateConfig>,
    pub route: Option<crate::config::RouteConfig>,
    pub analytic_window: Option<serde_json::Value>,
    pub log: Vec<crate::config::LogDirective>,
    pub validations: Vec<crate::config::ValidationEntry>,
    pub input: ResolvedInput,
    pub input_span: Span,
    pub body_span: Span,
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
/// variants directly). Falls back to the legacy `config.transforms()`
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
        // of `lower_nodes_into_legacy_fields::project_input`.
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
                        log: body.log.clone().unwrap_or_default(),
                        validations: body.validations.clone().unwrap_or_default(),
                        input: resolve_header_input(&header.input),
                        input_span: Span::SYNTHETIC,
                        body_span: Span::SYNTHETIC,
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
                        log: Vec::new(),
                        validations: Vec::new(),
                        input: resolve_header_input(&header.input),
                        input_span: Span::SYNTHETIC,
                        body_span: Span::SYNTHETIC,
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
                        log: Vec::new(),
                        validations: Vec::new(),
                        input: resolve_header_input(&header.input),
                        input_span: Span::SYNTHETIC,
                        body_span: Span::SYNTHETIC,
                    });
                }
                PipelineNode::Source { .. }
                | PipelineNode::Output { .. }
                | PipelineNode::Merge { .. }
                | PipelineNode::Composition { .. } => {}
            }
        }
        return specs;
    }

    // Legacy fallback: fixtures that still populate `config.transformations`
    // directly. Inlined what used to be `PlanTransformSpec::from_legacy`.
    config
        .transforms()
        .map(|tc| {
            let input = match &tc.input {
                None => ResolvedInput::Chain,
                Some(TransformInput::Single(s)) => ResolvedInput::Single(s.clone()),
                Some(TransformInput::Multiple(v)) => ResolvedInput::Multi(v.clone()),
            };
            PlanTransformSpec {
                name: tc.name.clone(),
                cxl_source: tc.cxl.clone(),
                aggregate: tc.aggregate.clone(),
                route: tc.route.clone(),
                analytic_window: tc.local_window.clone(),
                log: tc.log.clone().unwrap_or_default(),
                validations: tc.validations.clone().unwrap_or_default(),
                input,
                input_span: Span::SYNTHETIC,
                body_span: Span::SYNTHETIC,
            }
        })
        .collect()
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
            | PlanNode::Aggregation { name, .. } => name,
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
            | PlanNode::Aggregation { span, .. } => *span,
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
        let primary_source = config
            .inputs
            .first()
            .map(|i| i.name.clone())
            .unwrap_or_default();

        // Phase D: analyze all transforms
        let report = analyzer::analyze_all(compiled);

        // Phase 16b Wave 4ab Checkpoint C: build `PlanTransformSpec` directly
        // from the unified `nodes:` enum when present; fall back to the
        // legacy `config.transforms()` projection otherwise. The helpers
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
                if !config.inputs.iter().any(|inp| inp.name == source) {
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
                let already_sorted = check_already_sorted(&config.inputs, &source, &wc.sort_by);

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
        let source_dag = build_source_dag(&config.inputs, &window_configs, &primary_source)?;

        // Build output projections
        let output_projections = config
            .outputs
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

        // Source nodes
        for input in &config.inputs {
            add_node(
                &mut graph,
                PlanNode::Source {
                    name: input.name.clone(),
                    span: Span::SYNTHETIC,
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
            // order as `config.transforms()` (see analyzer::analyze_all).
            let write_set = extract_write_set(compiled[i].1);
            let has_distinct = extract_has_distinct(compiled[i].1);

            // ── Phase 16 Task 16.3.6: route aggregate transforms to
            //    `PlanNode::Aggregation` instead of `PlanNode::Transform`.
            //    D4: aggregates use a single dedicated plan node. D6:
            //    routes and multi-input merges are forbidden on aggregates.
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
                let input_schema: Vec<String> = typed.field_types.keys().cloned().collect();

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
                        span: Span::SYNTHETIC,
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
                    span: Span::SYNTHETIC,
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
                        span: Span::SYNTHETIC,
                        mode: rc.mode,
                        branches: rc.branches.iter().map(|b| b.name.clone()).collect(),
                        default: rc.default.clone(),
                    },
                    &mut node_by_name,
                    &mut slug_set,
                )?;
            }

            // Merge node for multiple inputs
            if let ResolvedInput::Multi(_) = &tc.input {
                add_node(
                    &mut graph,
                    PlanNode::Merge {
                        name: format!("merge_{}", tc.name),
                        span: Span::SYNTHETIC,
                    },
                    &mut node_by_name,
                    &mut slug_set,
                )?;
            }
        }

        // Output nodes
        for output in &config.outputs {
            add_node(
                &mut graph,
                PlanNode::Output {
                    name: output.name.clone(),
                    span: Span::SYNTHETIC,
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
        for output in &config.outputs {
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
        let inputs_map: HashMap<String, SourceConfig> = config
            .inputs
            .iter()
            .map(|i| (i.name.clone(), i.clone()))
            .collect();
        // Pipeline-level policy injection (correlation key for failure-domain
        // DLQ batching) runs first: it materializes a `PlanNode::Sort` from
        // declared config, distinct from per-operator algorithm requirements.
        // See docs/research/RESEARCH-pipeline-correlation-key-placement.md.
        dag.inject_correlation_sort(&config.error_handling, &config.inputs)
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
        for t in config.transforms() {
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
    pub fn explain_text(&self, config: &PipelineConfig) -> String {
        let mut out = self.explain_full(config);

        out.push_str("=== DAG Topology ===\n\n");
        for &idx in &self.topo_order {
            let node = &self.graph[idx];
            match node {
                PlanNode::Route {
                    name,
                    mode,
                    branches,
                    ..
                } => {
                    let mode_str = match mode {
                        RouteMode::Exclusive => "exclusive",
                        RouteMode::Inclusive => "inclusive",
                    };
                    out.push_str(&format!("  ◆ FORK [{mode_str}] '{name}'\n"));
                    for branch in branches {
                        out.push_str(&format!("  ├──> {branch}\n"));
                    }
                }
                PlanNode::Merge { name, .. } => {
                    out.push_str(&format!("  └──< MERGE '{name}'\n"));
                }
                PlanNode::Source { .. }
                | PlanNode::Transform { .. }
                | PlanNode::Output { .. }
                | PlanNode::Sort { .. }
                | PlanNode::Aggregation { .. } => {
                    let deps: Vec<String> = self
                        .graph
                        .neighbors_directed(idx, petgraph::Direction::Incoming)
                        .map(|pred| self.graph[pred].name().to_string())
                        .collect();
                    if deps.is_empty() {
                        out.push_str(&format!("  ● {}\n", node.display_name()));
                    } else {
                        out.push_str(&format!("  │ {}\n", node.display_name()));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use cxl::parser::Parser;
    use cxl::resolve::pass::resolve_program;
    use cxl::typecheck::pass::type_check;
    use std::collections::HashMap;

    /// Identity helper retained to keep test callsites compact.
    fn t(entry: &LegacyTransformsBlock) -> &LegacyTransformsBlock {
        entry
    }

    /// Build a minimal PipelineConfig for testing.
    fn test_config(
        inputs: Vec<(&str, &str)>,
        transforms: Vec<(&str, &str, Option<serde_json::Value>)>,
    ) -> PipelineConfig {
        PipelineConfig {
            pipeline: PipelineMeta {
                name: "test".into(),
                memory_limit: None,
                vars: None,
                date_formats: None,
                rules_path: None,
                concurrency: None,
                date_locale: None,
                log_rules: None,
                include_provenance: None,
                metrics: None,
            },
            nodes: Vec::new(),
            inputs: inputs
                .into_iter()
                .map(|(name, path)| SourceConfig {
                    name: name.into(),
                    path: path.into(),
                    schema: None,
                    schema_overrides: None,
                    array_paths: None,
                    sort_order: None,
                    format: InputFormat::Csv(None),
                    notes: None,
                })
                .collect(),
            outputs: vec![OutputConfig {
                name: "output".into(),
                path: "out.csv".into(),
                include_unmapped: true,
                include_header: None,
                mapping: None,
                exclude: None,
                sort_order: None,
                preserve_nulls: None,
                include_metadata: Default::default(),
                schema: None,
                split: None,
                format: OutputFormat::Csv(None),
                notes: None,
            }],
            transformations: transforms
                .into_iter()
                .map(|(name, cxl, local_window)| LegacyTransformsBlock {
                    name: name.into(),
                    description: None,
                    cxl: Some(cxl.into()),
                    aggregate: None,
                    local_window,
                    log: None,
                    validations: None,
                    route: None,
                    input: None,
                    notes: None,
                })
                .collect(),
            error_handling: ErrorHandlingConfig::default(),
            notes: None,
        }
    }

    /// Compile CXL source to TypedProgram for a given set of field names.
    fn compile_cxl(source: &str, fields: &[&str]) -> cxl::typecheck::pass::TypedProgram {
        let parsed = Parser::parse(source);
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors
        );
        let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
        let schema: IndexMap<String, cxl::typecheck::types::Type> = IndexMap::new();
        type_check(resolved, &schema).unwrap()
    }

    #[test]
    fn test_plan_stateless_only() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("calc", "let x = amount + 1\nemit result = x * 2", None)],
        );
        let fields = &["amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("calc", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        assert!(plan.indices_to_build.is_empty());
        let classes = plan.transform_parallelism_classes();
        assert_eq!(classes.len(), 1);
        assert_eq!(classes[0], ParallelismClass::Stateless);
        assert_eq!(plan.execution_summary(), "Streaming");
    }

    #[test]
    fn test_plan_single_window_index() {
        let window = serde_json::json!({
            "group_by": ["dept"]
        });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("agg", "emit total = $window.sum(amount)", Some(window))],
        );
        let fields = &["dept", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("agg", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        assert_eq!(plan.indices_to_build.len(), 1);
        assert_eq!(plan.indices_to_build[0].group_by, vec!["dept".to_string()]);
        assert_eq!(plan.execution_summary(), "TwoPass");
    }

    #[test]
    fn test_plan_dedup_shared_index() {
        let window = serde_json::json!({
            "group_by": ["dept"],
            "sort_by": [{"field": "date"}]
        });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![
                (
                    "agg1",
                    "emit total = $window.sum(amount)",
                    Some(window.clone()),
                ),
                ("agg2", "emit cnt = $window.count()", Some(window)),
            ],
        );
        let fields = &["dept", "amount", "date"];
        let typed1 = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let typed2 = compile_cxl(t(&config.transformations[1]).cxl_source(), fields);
        let compiled = vec![("agg1", &typed1), ("agg2", &typed2)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        assert_eq!(plan.indices_to_build.len(), 1, "should share one index");
        let wi = plan.transform_window_info();
        assert_eq!(wi[0].0, Some(0));
        assert_eq!(wi[1].0, Some(0));
    }

    #[test]
    fn test_plan_distinct_indices() {
        let window_a = serde_json::json!({ "group_by": ["dept"] });
        let window_b = serde_json::json!({ "group_by": ["region"] });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![
                (
                    "agg_dept",
                    "emit total = $window.sum(amount)",
                    Some(window_a),
                ),
                (
                    "agg_region",
                    "emit total = $window.sum(amount)",
                    Some(window_b),
                ),
            ],
        );
        let fields = &["dept", "region", "amount"];
        let typed1 = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let typed2 = compile_cxl(t(&config.transformations[1]).cxl_source(), fields);
        let compiled = vec![("agg_dept", &typed1), ("agg_region", &typed2)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        assert_eq!(plan.indices_to_build.len(), 2);
    }

    #[test]
    fn test_plan_parallelism_stateless() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("calc", "emit doubled = amount * 2", None)],
        );
        let fields = &["amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("calc", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        assert_eq!(
            plan.transform_parallelism_classes()[0],
            ParallelismClass::Stateless
        );
    }

    #[test]
    fn test_plan_parallelism_index_reading() {
        let window = serde_json::json!({ "group_by": ["dept"] });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("agg", "emit total = $window.sum(amount)", Some(window))],
        );
        let fields = &["dept", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("agg", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        assert_eq!(
            plan.transform_parallelism_classes()[0],
            ParallelismClass::IndexReading
        );
    }

    #[test]
    fn test_plan_parallelism_sequential() {
        let window = serde_json::json!({
            "group_by": ["dept"],
            "sort_by": [{"field": "date"}]
        });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("positional", "emit prev = $window.lag(1)", Some(window))],
        );
        let fields = &["dept", "amount", "date"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("positional", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        assert_eq!(
            plan.transform_parallelism_classes()[0],
            ParallelismClass::Sequential
        );
    }

    #[test]
    fn test_plan_explain_output() {
        let window = serde_json::json!({ "group_by": ["dept"] });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![
                ("calc", "emit doubled = amount * 2", None),
                ("agg", "emit total = $window.sum(amount)", Some(window)),
            ],
        );
        let fields = &["dept", "amount"];
        let typed1 = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let typed2 = compile_cxl(t(&config.transformations[1]).cxl_source(), fields);
        let compiled = vec![("calc", &typed1), ("agg", &typed2)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(explain.contains("Mode: TwoPass"));
        assert!(explain.contains("Indices to build: 1"));
        assert!(explain.contains("Transforms: 2"));
        assert!(explain.contains("Stateless"));
        assert!(explain.contains("IndexReading"));
    }

    #[test]
    fn test_plan_cross_source_dag() {
        let window = serde_json::json!({
            "source": "reference",
            "group_by": ["id"],
            "on": "ref_id"
        });
        let config = test_config(
            vec![("primary", "data.csv"), ("reference", "ref.csv")],
            vec![("lookup", "emit ref_val = $window.sum(amount)", Some(window))],
        );
        let fields = &["id", "ref_id", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("lookup", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        assert_eq!(plan.source_dag.len(), 2);
        assert!(
            plan.source_dag[0]
                .sources
                .contains(&"reference".to_string())
        );
        assert!(plan.source_dag[1].sources.contains(&"primary".to_string()));
    }

    #[test]
    fn test_plan_mode_two_pass() {
        let window = serde_json::json!({ "group_by": ["dept"] });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("agg", "emit total = $window.sum(amount)", Some(window))],
        );
        let fields = &["dept", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("agg", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        assert!(plan.required_arena());
        assert_eq!(plan.execution_summary(), "TwoPass");
    }

    #[test]
    fn test_plan_cross_source_missing_reference() {
        let window = serde_json::json!({
            "source": "nonexistent",
            "group_by": ["id"]
        });
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("lookup", "emit ref_val = $window.sum(amount)", Some(window))],
        );
        let fields = &["id", "amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("lookup", &typed)];

        let result = ExecutionPlanDag::compile(&config, &compiled);

        assert!(result.is_err());
        let err = result.unwrap_err();
        match err {
            PlanError::UnknownSource { name, .. } => assert_eq!(name, "nonexistent"),
            other => panic!("Expected UnknownSource, got: {:?}", other),
        }
    }

    /// Build a config with route configuration on the first transform.
    fn test_config_with_route(cxl: &str, route: crate::config::RouteConfig) -> PipelineConfig {
        PipelineConfig {
            pipeline: PipelineMeta {
                name: "route-explain-test".into(),
                memory_limit: None,
                vars: None,
                date_formats: None,
                rules_path: None,
                concurrency: None,
                date_locale: None,
                log_rules: None,
                include_provenance: None,
                metrics: None,
            },
            nodes: Vec::new(),
            inputs: vec![SourceConfig {
                name: "src".into(),
                path: "data.csv".into(),
                schema: None,
                schema_overrides: None,
                array_paths: None,
                sort_order: None,
                format: InputFormat::Csv(None),
                notes: None,
            }],
            outputs: vec![OutputConfig {
                name: "output".into(),
                path: "out.csv".into(),
                include_unmapped: true,
                include_header: None,
                mapping: None,
                exclude: None,
                sort_order: None,
                preserve_nulls: None,
                include_metadata: Default::default(),
                schema: None,
                split: None,
                format: OutputFormat::Csv(None),
                notes: None,
            }],
            transformations: vec![LegacyTransformsBlock {
                name: "router".into(),
                description: None,
                cxl: Some(cxl.into()),
                aggregate: None,
                local_window: None,
                log: None,
                validations: None,
                route: Some(route),
                input: None,
                notes: None,
            }],
            error_handling: ErrorHandlingConfig::default(),
            notes: None,
        }
    }

    #[test]
    fn test_explain_shows_route_branches() {
        let route = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Exclusive,
            branches: vec![
                crate::config::RouteBranch {
                    name: "high_value".into(),
                    condition: "amount > 1000".into(),
                },
                crate::config::RouteBranch {
                    name: "medium_value".into(),
                    condition: "amount > 100".into(),
                },
            ],
            default: "low_value".into(),
        };
        let config = test_config_with_route("emit result = amount", route);
        let fields = &["amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("router", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(
            explain.contains("high_value"),
            "should contain branch name 'high_value': {}",
            explain
        );
        assert!(
            explain.contains("medium_value"),
            "should contain branch name 'medium_value': {}",
            explain
        );
    }

    #[test]
    fn test_explain_shows_route_mode() {
        let route_exclusive = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Exclusive,
            branches: vec![crate::config::RouteBranch {
                name: "branch_a".into(),
                condition: "status == \"active\"".into(),
            }],
            default: "other".into(),
        };
        let config = test_config_with_route("emit result = status", route_exclusive);
        let fields = &["status"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("router", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(
            explain.contains("exclusive"),
            "should contain mode 'exclusive': {}",
            explain
        );

        // Also test inclusive
        let route_inclusive = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Inclusive,
            branches: vec![crate::config::RouteBranch {
                name: "branch_a".into(),
                condition: "status == \"active\"".into(),
            }],
            default: "other".into(),
        };
        let config2 = test_config_with_route("emit result = status", route_inclusive);
        let typed2 = compile_cxl(t(&config2.transformations[0]).cxl_source(), fields);
        let compiled2 = vec![("router", &typed2)];

        let plan2 = ExecutionPlanDag::compile(&config2, &compiled2).unwrap();
        let explain2 = plan2.explain();

        assert!(
            explain2.contains("inclusive"),
            "should contain mode 'inclusive': {}",
            explain2
        );
    }

    #[test]
    fn test_explain_shows_default() {
        let route = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Exclusive,
            branches: vec![crate::config::RouteBranch {
                name: "special".into(),
                condition: "flag == true".into(),
            }],
            default: "fallback".into(),
        };
        let config = test_config_with_route("emit result = flag", route);
        let fields = &["flag"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("router", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(
            explain.contains("Default: 'fallback'"),
            "should show default branch: {}",
            explain
        );
    }

    #[test]
    fn test_explain_no_route_unchanged() {
        // Pipeline without routes — explain output should not mention Route
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("calc", "emit doubled = amount * 2", None)],
        );
        let fields = &["amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("calc", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        assert!(
            !explain.contains("Route"),
            "should not contain Route section when no routes: {}",
            explain
        );
    }

    #[test]
    fn test_explain_shows_output_mapping() {
        let route = crate::config::RouteConfig {
            mode: crate::config::RouteMode::Exclusive,
            branches: vec![
                crate::config::RouteBranch {
                    name: "errors".into(),
                    condition: "status == \"error\"".into(),
                },
                crate::config::RouteBranch {
                    name: "warnings".into(),
                    condition: "status == \"warn\"".into(),
                },
            ],
            default: "normal".into(),
        };
        let config = test_config_with_route("emit result = status", route);
        let fields = &["status"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("router", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();
        let explain = plan.explain();

        // Each branch should show → output mapping
        assert!(
            explain.contains("→ output 'errors'"),
            "should show output mapping for 'errors': {}",
            explain
        );
        assert!(
            explain.contains("→ output 'warnings'"),
            "should show output mapping for 'warnings': {}",
            explain
        );
        assert!(
            explain.contains("→ output 'normal'"),
            "should show output mapping for default: {}",
            explain
        );
    }

    // --- DAG-specific tests ---

    #[test]
    fn test_dag_node_count_linear() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![
                ("step_one", "emit x = amount + 1", None),
                ("step_two", "emit y = amount * 2", None),
            ],
        );
        let fields = &["amount"];
        let typed1 = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let typed2 = compile_cxl(t(&config.transformations[1]).cxl_source(), fields);
        let compiled = vec![("step_one", &typed1), ("step_two", &typed2)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        // source + 2 transforms + output = 4
        assert_eq!(plan.graph.node_count(), 4);
        assert_eq!(plan.graph.edge_count(), 3);
    }

    #[test]
    fn test_dag_topo_order_linear() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![
                ("step_one", "emit x = amount + 1", None),
                ("step_two", "emit y = amount * 2", None),
            ],
        );
        let fields = &["amount"];
        let typed1 = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let typed2 = compile_cxl(t(&config.transformations[1]).cxl_source(), fields);
        let compiled = vec![("step_one", &typed1), ("step_two", &typed2)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();

        let names: Vec<&str> = plan
            .topo_order
            .iter()
            .map(|&idx| plan.graph[idx].name())
            .collect();
        assert_eq!(names, vec!["primary", "step_one", "step_two", "output"]);
    }

    #[test]
    fn test_dag_json_serialization() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("calc", "emit doubled = amount * 2", None)],
        );
        let fields = &["amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("calc", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();
        let json = serde_json::to_value(&plan).unwrap();

        assert_eq!(json["schema_version"], "1");
        let nodes = json["nodes"].as_array().unwrap();
        assert_eq!(nodes.len(), 3); // source + transform + output

        // Check first node is source
        assert_eq!(nodes[0]["type"], "source");
        assert_eq!(nodes[0]["name"], "primary");
        assert!(nodes[0]["depends_on"].as_array().unwrap().is_empty());

        // Check transform depends on source
        assert_eq!(nodes[1]["type"], "transform");
        assert_eq!(nodes[1]["name"], "calc");
        assert!(
            nodes[1]["depends_on"]
                .as_array()
                .unwrap()
                .contains(&serde_json::json!("source.primary"))
        );
    }

    // ----- Task 16.0.5.5: insert_enforcer_sorts -----

    /// Build a minimal 2-node DAG: Source -> Transform with the given
    /// `RequiresSortedInput` requirement. Returns the populated DAG.
    fn dag_with_sorted_consumer(
        source_name: &str,
        consumer_name: &str,
        required: Vec<SortField>,
    ) -> ExecutionPlanDag {
        let mut graph = DiGraph::<PlanNode, PlanEdge>::new();
        let s = graph.add_node(PlanNode::Source {
            name: source_name.into(),
            span: Span::SYNTHETIC,
            resolved: None,
        });
        let t = graph.add_node(PlanNode::Transform {
            name: consumer_name.into(),
            span: Span::SYNTHETIC,
            resolved: None,
            parallelism_class: ParallelismClass::Stateless,
            tier: 0,
            execution_reqs: NodeExecutionReqs::RequiresSortedInput {
                sort_fields: required,
            },
            window_index: None,
            partition_lookup: None,
            write_set: BTreeSet::new(),
            has_distinct: false,
        });
        graph.add_edge(
            s,
            t,
            PlanEdge {
                dependency_type: DependencyType::Data,
            },
        );
        let topo_order = vec![s, t];
        ExecutionPlanDag {
            graph,
            topo_order,
            source_dag: vec![],
            indices_to_build: vec![],
            output_projections: vec![],
            parallelism: ParallelismProfile {
                per_transform: vec![],
                worker_threads: 1,
            },
            correlation_sort_note: None,
            node_properties: HashMap::new(),
        }
    }

    fn input_with_sort(name: &str, sort: Option<Vec<SortField>>) -> SourceConfig {
        SourceConfig {
            name: name.into(),
            path: "x.csv".into(),
            schema: None,
            schema_overrides: None,
            array_paths: None,
            sort_order: sort.map(|v| v.into_iter().map(SortFieldSpec::Full).collect()),
            format: InputFormat::Csv(None),
            notes: None,
        }
    }

    fn count_sort_nodes(dag: &ExecutionPlanDag) -> usize {
        dag.graph
            .node_weights()
            .filter(|n| matches!(n, PlanNode::Sort { .. }))
            .count()
    }

    #[test]
    fn test_enforcer_noop_when_declared_prefix_satisfies() {
        let req = vec![sf("k", SortOrder::Asc, None)];
        let mut dag = dag_with_sorted_consumer("src", "consumer", req.clone());
        let inputs = HashMap::from([(
            "src".to_string(),
            input_with_sort(
                "src",
                Some(vec![
                    sf("k", SortOrder::Asc, None),
                    sf("extra", SortOrder::Asc, None),
                ]),
            ),
        )]);
        dag.insert_enforcer_sorts(&inputs).unwrap();
        assert_eq!(count_sort_nodes(&dag), 0);
    }

    #[test]
    fn test_enforcer_inserts_when_source_unordered() {
        let req = vec![sf("k", SortOrder::Asc, None)];
        let mut dag = dag_with_sorted_consumer("src", "consumer", req);
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", None))]);
        dag.insert_enforcer_sorts(&inputs).unwrap();
        assert_eq!(count_sort_nodes(&dag), 1);
        // Verify the inserted node has the reserved-prefix name.
        let sort = dag
            .graph
            .node_weights()
            .find(|n| matches!(n, PlanNode::Sort { .. }))
            .unwrap();
        assert_eq!(sort.name(), "__sort_for_consumer");
        // Verify topology: Source -> Sort -> Transform
        let src_idx = dag
            .graph
            .node_indices()
            .find(|&i| matches!(dag.graph[i], PlanNode::Source { .. }))
            .unwrap();
        let consumer_idx = dag
            .graph
            .node_indices()
            .find(|&i| matches!(dag.graph[i], PlanNode::Transform { .. }))
            .unwrap();
        assert!(dag.graph.find_edge(src_idx, consumer_idx).is_none());
    }

    #[test]
    fn test_enforcer_inserts_when_order_direction_mismatches() {
        let req = vec![sf("k", SortOrder::Asc, None)];
        let mut dag = dag_with_sorted_consumer("src", "consumer", req);
        let inputs = HashMap::from([(
            "src".to_string(),
            input_with_sort("src", Some(vec![sf("k", SortOrder::Desc, None)])),
        )]);
        dag.insert_enforcer_sorts(&inputs).unwrap();
        assert_eq!(count_sort_nodes(&dag), 1);
    }

    #[test]
    fn test_enforcer_idempotent_on_recompile() {
        let req = vec![sf("k", SortOrder::Asc, None)];
        let mut dag = dag_with_sorted_consumer("src", "consumer", req);
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", None))]);
        dag.insert_enforcer_sorts(&inputs).unwrap();
        let after_first = count_sort_nodes(&dag);
        dag.insert_enforcer_sorts(&inputs).unwrap();
        let after_second = count_sort_nodes(&dag);
        assert_eq!(after_first, 1);
        assert_eq!(after_second, 1);
    }

    #[test]
    fn test_enforcer_rejects_user_declared_reserved_prefix() {
        let req = vec![sf("k", SortOrder::Asc, None)];
        let mut dag = dag_with_sorted_consumer("src", "__sort_for_user", req);
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", None))]);
        let err = dag.insert_enforcer_sorts(&inputs).unwrap_err();
        match err {
            PipelineError::Compilation { transform_name, .. } => {
                assert_eq!(transform_name, "__sort_for_user");
            }
            other => panic!("expected Compilation error, got {other:?}"),
        }
    }

    // ----- Task 16.0.5.4: source_ordering_satisfies predicate -----

    fn sf(field: &str, order: SortOrder, null_order: Option<NullOrder>) -> SortField {
        SortField {
            field: field.into(),
            order,
            null_order,
        }
    }

    #[test]
    fn test_source_ordering_satisfies_prefix() {
        let declared = vec![
            sf("a", SortOrder::Asc, None),
            sf("b", SortOrder::Asc, None),
            sf("c", SortOrder::Asc, None),
        ];
        let required = vec![sf("a", SortOrder::Asc, None), sf("b", SortOrder::Asc, None)];
        assert!(source_ordering_satisfies(&declared, &required));
    }

    #[test]
    fn test_source_ordering_satisfies_equal_length() {
        let v = vec![
            sf("a", SortOrder::Asc, None),
            sf("b", SortOrder::Desc, None),
        ];
        assert!(source_ordering_satisfies(&v.clone(), &v));
    }

    #[test]
    fn test_source_ordering_satisfies_superset_required_fails() {
        let declared = vec![sf("a", SortOrder::Asc, None)];
        let required = vec![sf("a", SortOrder::Asc, None), sf("b", SortOrder::Asc, None)];
        assert!(!source_ordering_satisfies(&declared, &required));
    }

    #[test]
    fn test_source_ordering_satisfies_direction_mismatch_fails() {
        let declared = vec![sf("a", SortOrder::Asc, None)];
        let required = vec![sf("a", SortOrder::Desc, None)];
        assert!(!source_ordering_satisfies(&declared, &required));
    }

    #[test]
    fn test_source_ordering_satisfies_nulls_mismatch_fails() {
        let declared = vec![sf("a", SortOrder::Asc, Some(NullOrder::First))];
        let required = vec![sf("a", SortOrder::Asc, Some(NullOrder::Last))];
        assert!(!source_ordering_satisfies(&declared, &required));
    }

    #[test]
    fn test_source_ordering_satisfies_empty_required_always() {
        let declared = vec![sf("a", SortOrder::Asc, None)];
        assert!(source_ordering_satisfies(&declared, &[]));
        assert!(source_ordering_satisfies(&[], &[]));
    }

    #[test]
    fn test_source_ordering_satisfies_empty_declared_only_empty_required() {
        let required = vec![sf("a", SortOrder::Asc, None)];
        assert!(!source_ordering_satisfies(&[], &required));
    }

    // ----- Task 16.0.5.6: compute_node_properties -----

    /// Gate test for Task 16.0.5.6: a `PlanNode::Sort` node in a
    /// `Source -> Sort -> Transform` chain owns its declared `sort_fields`
    /// in `NodeProperties.ordering`, with `Preserved { from_node }` provenance
    /// pointing at itself. Confirms the topo walk runs, the double-call guard
    /// holds, and the Sort arm ignores parent ordering.
    #[test]
    fn test_node_properties_sort_node_owns_its_order() {
        let req = vec![sf("k", SortOrder::Asc, None)];
        let mut dag = dag_with_sorted_consumer("src", "consumer", req.clone());
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", None))]);

        // Insert the enforcer Sort first so the chain is Source -> Sort -> Transform.
        dag.insert_enforcer_sorts(&inputs).unwrap();
        assert_eq!(count_sort_nodes(&dag), 1);

        // Topo order may have grown — re-derive from petgraph.
        dag.topo_order = toposort(&dag.graph, None).unwrap();

        dag.compute_node_properties(&inputs).unwrap();

        // Every node has properties.
        assert_eq!(dag.node_properties.len(), dag.graph.node_count());

        // Locate the Sort node and verify ordering + provenance.
        let sort_idx = dag
            .graph
            .node_indices()
            .find(|&i| matches!(dag.graph[i], PlanNode::Sort { .. }))
            .unwrap();
        let sort_name = dag.graph[sort_idx].name().to_string();
        let props = dag.node_properties.get(&sort_idx).unwrap();
        let got = props.ordering.sort_order.as_ref().expect("sort_order set");
        assert_eq!(got.len(), req.len());
        for (g, r) in got.iter().zip(req.iter()) {
            assert_eq!(g.field, r.field);
            assert_eq!(g.order, r.order);
            assert_eq!(g.null_order, r.null_order);
        }
        match &props.ordering.provenance {
            OrderingProvenance::Preserved { from_node } => {
                assert_eq!(from_node, &sort_name);
            }
            other => panic!("expected Preserved {{ from_node }}, got {other:?}"),
        }
    }

    /// Sanity check: calling `compute_node_properties` twice panics.
    #[test]
    #[should_panic(expected = "compute_node_properties called twice")]
    fn test_compute_node_properties_double_call_panics() {
        let req = vec![sf("k", SortOrder::Asc, None)];
        let mut dag = dag_with_sorted_consumer("src", "consumer", req);
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", None))]);
        dag.compute_node_properties(&inputs).unwrap();
        dag.compute_node_properties(&inputs).unwrap();
    }

    // ----- Task 16.0.5.10: enforcer-then-properties wired into compile -----

    /// Gate: `ExecutionPlanDag::compile` runs `insert_enforcer_sorts`
    /// before `compute_node_properties`. We replicate the exact in-compile
    /// sequence on a hand-built DAG that has a `RequiresSortedInput`
    /// consumer (real CXL doesn't yet emit this — analyzer wiring is
    /// deferred — but the call sequence is what's under test). After the
    /// chain runs, the synthesized `PlanNode::Sort` must (a) exist (proves
    /// enforcer ran) and (b) carry `node_properties` whose ordering
    /// provenance is `Preserved { from_node: "__sort_for_..." }` (proves
    /// property derivation ran *after* the enforcer, on the rewritten DAG).
    #[test]
    fn test_compile_runs_enforcer_before_properties() {
        let req = vec![sf("k", SortOrder::Asc, None)];
        let mut dag = dag_with_sorted_consumer("src", "consumer", req);
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", None))]);

        // Same call chain as inside ExecutionPlanDag::compile (D50).
        dag.insert_enforcer_sorts(&inputs).unwrap();
        dag.topo_order = toposort(&dag.graph, None).unwrap();
        dag.compute_node_properties(&inputs).unwrap();

        // (a) Enforcer ran: a Sort node was inserted.
        let sort_idx = dag
            .graph
            .node_indices()
            .find(|&i| matches!(dag.graph[i], PlanNode::Sort { .. }))
            .expect("enforcer must insert a PlanNode::Sort");
        let sort_name = dag.graph[sort_idx].name().to_string();
        assert!(
            sort_name.starts_with(ENFORCER_SORT_PREFIX),
            "enforcer Sort node uses reserved prefix"
        );

        // (b) Property derivation ran *after*: Sort node has properties
        // with Preserved { from_node: <itself> }.
        let props = dag
            .node_properties
            .get(&sort_idx)
            .expect("Sort node must have NodeProperties (compute ran after enforcer)");
        assert!(props.ordering.sort_order.is_some());
        match &props.ordering.provenance {
            OrderingProvenance::Preserved { from_node } => {
                assert_eq!(from_node, &sort_name);
                assert!(from_node.starts_with(ENFORCER_SORT_PREFIX));
            }
            other => panic!("expected Preserved {{ from_node }}, got {other:?}"),
        }
    }

    /// Sanity: `ExecutionPlanDag::compile` populates `node_properties` for
    /// every node in the resulting DAG (proves the in-compile call chain
    /// actually fires on the production code path, even when no enforcer
    /// insertion is needed).
    #[test]
    fn test_compile_populates_node_properties_for_every_node() {
        let config = test_config(
            vec![("primary", "data.csv")],
            vec![("calc", "let x = amount + 1\nemit result = x * 2", None)],
        );
        let fields = &["amount"];
        let typed = compile_cxl(t(&config.transformations[0]).cxl_source(), fields);
        let compiled = vec![("calc", &typed)];

        let plan = ExecutionPlanDag::compile(&config, &compiled).unwrap();
        assert_eq!(
            plan.node_properties.len(),
            plan.graph.node_count(),
            "compute_node_properties must run inside compile"
        );
    }

    // ----- Task 16.0.5.7: per-operator preservation rules -----

    /// Build a minimal `ExecutionPlanDag` with just the supplied nodes and
    /// edges, computing `topo_order` from petgraph. Used by the per-operator
    /// rule tests below to construct surgical fixtures without going through
    /// `ExecutionPlanDag::compile`.
    fn dag_from_nodes(
        nodes: Vec<PlanNode>,
        edges: &[(usize, usize)],
    ) -> (ExecutionPlanDag, Vec<NodeIndex>) {
        let mut graph = DiGraph::<PlanNode, PlanEdge>::new();
        let idxs: Vec<NodeIndex> = nodes.into_iter().map(|n| graph.add_node(n)).collect();
        for &(a, b) in edges {
            graph.add_edge(
                idxs[a],
                idxs[b],
                PlanEdge {
                    dependency_type: DependencyType::Data,
                },
            );
        }
        let topo_order = toposort(&graph, None).unwrap();
        let dag = ExecutionPlanDag {
            graph,
            topo_order,
            source_dag: vec![],
            indices_to_build: vec![],
            output_projections: vec![],
            parallelism: ParallelismProfile {
                per_transform: vec![],
                worker_threads: 1,
            },
            correlation_sort_note: None,
            node_properties: HashMap::new(),
        };
        (dag, idxs)
    }

    fn make_transform(name: &str, write_set: BTreeSet<String>, has_distinct: bool) -> PlanNode {
        PlanNode::Transform {
            name: name.into(),
            span: Span::SYNTHETIC,
            resolved: None,
            parallelism_class: ParallelismClass::Stateless,
            tier: 0,
            execution_reqs: NodeExecutionReqs::Streaming,
            window_index: None,
            partition_lookup: None,
            write_set,
            has_distinct,
        }
    }

    fn make_arena_transform(name: &str) -> PlanNode {
        PlanNode::Transform {
            name: name.into(),
            span: Span::SYNTHETIC,
            resolved: None,
            parallelism_class: ParallelismClass::Stateless,
            tier: 0,
            execution_reqs: NodeExecutionReqs::RequiresArena,
            window_index: None,
            partition_lookup: None,
            write_set: BTreeSet::new(),
            has_distinct: false,
        }
    }

    #[test]
    fn test_node_properties_source_declared_input() {
        let (mut dag, idxs) = dag_from_nodes(
            vec![PlanNode::Source {
                name: "src".into(),
                span: Span::SYNTHETIC,
                resolved: None,
            }],
            &[],
        );
        let inputs = HashMap::from([(
            "src".to_string(),
            input_with_sort("src", Some(vec![sf("k", SortOrder::Asc, None)])),
        )]);
        dag.compute_node_properties(&inputs).unwrap();
        let props = dag.node_properties.get(&idxs[0]).unwrap();
        assert!(props.ordering.sort_order.is_some());
        match &props.ordering.provenance {
            OrderingProvenance::DeclaredOnInput { input_name } => {
                assert_eq!(input_name, "src");
            }
            other => panic!("expected DeclaredOnInput, got {other:?}"),
        }
        assert!(matches!(props.partitioning.kind, PartitioningKind::Single));
    }

    #[test]
    fn test_node_properties_transform_preserves_when_write_set_disjoint() {
        // Source(sort=k) -> Transform(write_set={x}) -> preserves k.
        let mut ws = BTreeSet::new();
        ws.insert("x".to_string());
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "src".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                make_transform("t", ws, false),
            ],
            &[(0, 1)],
        );
        let inputs = HashMap::from([(
            "src".to_string(),
            input_with_sort("src", Some(vec![sf("k", SortOrder::Asc, None)])),
        )]);
        dag.compute_node_properties(&inputs).unwrap();
        let props = dag.node_properties.get(&idxs[1]).unwrap();
        let so = props.ordering.sort_order.as_ref().expect("preserved");
        assert_eq!(so.len(), 1);
        assert_eq!(so[0].field, "k");
        match &props.ordering.provenance {
            OrderingProvenance::Preserved { from_node } => assert_eq!(from_node, "t"),
            other => panic!("expected Preserved, got {other:?}"),
        }
    }

    #[test]
    fn test_node_properties_transform_destroys_on_write_set_hit() {
        // Source(sort=k) -> Transform(write_set={k}) -> destroyed.
        let mut ws = BTreeSet::new();
        ws.insert("k".to_string());
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "src".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                make_transform("t", ws, false),
            ],
            &[(0, 1)],
        );
        let inputs = HashMap::from([(
            "src".to_string(),
            input_with_sort("src", Some(vec![sf("k", SortOrder::Asc, None)])),
        )]);
        dag.compute_node_properties(&inputs).unwrap();
        let props = dag.node_properties.get(&idxs[1]).unwrap();
        assert!(props.ordering.sort_order.is_none());
        match &props.ordering.provenance {
            OrderingProvenance::DestroyedByTransformWriteSet {
                at_node,
                fields_written,
                sort_fields_lost,
                ..
            } => {
                assert_eq!(at_node, "t");
                assert!(fields_written.contains(&"k".to_string()));
                assert_eq!(sort_fields_lost, &vec!["k".to_string()]);
            }
            other => panic!("expected DestroyedByTransformWriteSet, got {other:?}"),
        }
    }

    #[test]
    fn test_node_properties_transform_destroys_on_distinct() {
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "src".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                make_transform("t", BTreeSet::new(), true),
            ],
            &[(0, 1)],
        );
        let inputs = HashMap::from([(
            "src".to_string(),
            input_with_sort("src", Some(vec![sf("k", SortOrder::Asc, None)])),
        )]);
        dag.compute_node_properties(&inputs).unwrap();
        let props = dag.node_properties.get(&idxs[1]).unwrap();
        assert!(props.ordering.sort_order.is_none());
        assert!(matches!(
            props.ordering.provenance,
            OrderingProvenance::DestroyedByDistinct { .. }
        ));
    }

    #[test]
    fn test_node_properties_distinct_destroys_unconditionally() {
        // Even with an unsorted source, the Transform's provenance should
        // be `DestroyedByDistinct` (not `NoOrdering` propagated).
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "src".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                make_transform("t", BTreeSet::new(), true),
            ],
            &[(0, 1)],
        );
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", None))]);
        dag.compute_node_properties(&inputs).unwrap();
        let props = dag.node_properties.get(&idxs[1]).unwrap();
        match &props.ordering.provenance {
            OrderingProvenance::DestroyedByDistinct { at_node, .. } => {
                assert_eq!(at_node, "t");
            }
            other => panic!("expected DestroyedByDistinct, got {other:?}"),
        }
    }

    #[test]
    fn test_node_properties_route_fans_out_unchanged() {
        // Source(sort=k) -> Transform(disjoint) -> Route -> verify Route preserves.
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "src".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                make_transform("t", BTreeSet::new(), false),
                PlanNode::Route {
                    name: "r".into(),
                    span: Span::SYNTHETIC,
                    mode: RouteMode::Exclusive,
                    branches: vec![],
                    default: "x".into(),
                },
            ],
            &[(0, 1), (1, 2)],
        );
        let inputs = HashMap::from([(
            "src".to_string(),
            input_with_sort("src", Some(vec![sf("k", SortOrder::Asc, None)])),
        )]);
        dag.compute_node_properties(&inputs).unwrap();
        let route_props = dag.node_properties.get(&idxs[2]).unwrap();
        let so = route_props.ordering.sort_order.as_ref().expect("preserved");
        assert_eq!(so[0].field, "k");
        assert!(matches!(
            route_props.ordering.provenance,
            OrderingProvenance::Preserved { .. }
        ));
    }

    #[test]
    fn test_streaming_agg_after_route_each_branch_qualifies() {
        // D45: route fan-out preserves ordering on every outgoing edge so
        // streaming aggregation can qualify per-branch. We model this by
        // attaching two downstream Transforms (one per branch) and asserting
        // both inherit `Preserved`.
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "src".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                make_transform("t0", BTreeSet::new(), false),
                PlanNode::Route {
                    name: "r".into(),
                    span: Span::SYNTHETIC,
                    mode: RouteMode::Inclusive,
                    branches: vec![],
                    default: "x".into(),
                },
                make_transform("branch_a", BTreeSet::new(), false),
                make_transform("branch_b", BTreeSet::new(), false),
            ],
            &[(0, 1), (1, 2), (2, 3), (2, 4)],
        );
        let inputs = HashMap::from([(
            "src".to_string(),
            input_with_sort("src", Some(vec![sf("k", SortOrder::Asc, None)])),
        )]);
        dag.compute_node_properties(&inputs).unwrap();
        for branch_idx in [idxs[3], idxs[4]] {
            let p = dag.node_properties.get(&branch_idx).unwrap();
            assert!(
                p.ordering.sort_order.is_some(),
                "branch must inherit ordering from Route"
            );
            assert!(matches!(
                p.ordering.provenance,
                OrderingProvenance::Preserved { .. }
            ));
        }
    }

    #[test]
    fn test_node_properties_merge_identical_parents_preserves() {
        // Two Sources with the same sort_order -> Merge -> Preserved.
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "a".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                PlanNode::Source {
                    name: "b".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                PlanNode::Merge {
                    name: "m".into(),
                    span: Span::SYNTHETIC,
                },
            ],
            &[(0, 2), (1, 2)],
        );
        let so = vec![sf("k", SortOrder::Asc, None)];
        let inputs = HashMap::from([
            ("a".to_string(), input_with_sort("a", Some(so.clone()))),
            ("b".to_string(), input_with_sort("b", Some(so.clone()))),
        ]);
        dag.compute_node_properties(&inputs).unwrap();
        let props = dag.node_properties.get(&idxs[2]).unwrap();
        let got = props.ordering.sort_order.as_ref().expect("preserved");
        assert_eq!(got[0].field, "k");
        assert!(matches!(
            props.ordering.provenance,
            OrderingProvenance::Preserved { .. }
        ));
    }

    #[test]
    fn test_node_properties_merge_mismatch_destroys() {
        // Two Sources with different sort orders -> Merge -> destroyed.
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "a".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                PlanNode::Source {
                    name: "b".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                PlanNode::Merge {
                    name: "m".into(),
                    span: Span::SYNTHETIC,
                },
            ],
            &[(0, 2), (1, 2)],
        );
        let inputs = HashMap::from([
            (
                "a".to_string(),
                input_with_sort("a", Some(vec![sf("k", SortOrder::Asc, None)])),
            ),
            (
                "b".to_string(),
                input_with_sort("b", Some(vec![sf("j", SortOrder::Asc, None)])),
            ),
        ]);
        dag.compute_node_properties(&inputs).unwrap();
        let props = dag.node_properties.get(&idxs[2]).unwrap();
        assert!(props.ordering.sort_order.is_none());
        match &props.ordering.provenance {
            OrderingProvenance::DestroyedByMergeMismatch {
                at_node,
                parent_orderings,
                ..
            } => {
                assert_eq!(at_node, "m");
                assert_eq!(parent_orderings.len(), 2);
            }
            other => panic!("expected DestroyedByMergeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_arena_partition_sort_does_not_leak_into_node_properties() {
        // D46 boundary: a `RequiresArena` Transform with an empty `write_set`
        // and an unsorted Source must NOT advertise any ordering. The Phase 6
        // arena `sort_partition` runs intra-partition inside the
        // materialization layer and is invisible to NodeProperties.
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "src".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                make_arena_transform("arena_t"),
            ],
            &[(0, 1)],
        );
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", None))]);
        dag.compute_node_properties(&inputs).unwrap();
        let props = dag.node_properties.get(&idxs[1]).unwrap();
        assert!(
            props.ordering.sort_order.is_none(),
            "arena sort_partition must not surface as stream-level ordering"
        );
    }

    /// Gate test for Task 16.0.5.3: PlanNode::Sort variant exists,
    /// has correct serde tag, name/type_tag/display_name/id_slug arms,
    /// and round-trips through JSON.
    #[test]
    fn test_plan_node_sort_exhaustive_matches() {
        let node = PlanNode::Sort {
            name: "x".into(),
            span: Span::SYNTHETIC,
            sort_fields: vec![],
        };

        // Method arms exist for Sort.
        assert_eq!(node.name(), "x");
        assert_eq!(node.type_tag(), "sort");
        assert_eq!(node.id_slug(), "sort.x");
        assert_eq!(node.display_name(), "[sort] x");

        // Serde round-trip: tag is "sort".
        let json = serde_json::to_value(&node).unwrap();
        assert_eq!(json["type"], "sort");
        assert_eq!(json["name"], "x");
    }

    // ----- Task 16.0.5.12: --explain text + JSON for PlanNode::Sort -----

    /// Build a Source -> Sort -> Transform DAG via the same call chain
    /// `ExecutionPlanDag::compile` uses, returning the populated DAG.
    fn dag_with_enforcer_sort() -> ExecutionPlanDag {
        let req = vec![sf("k", SortOrder::Asc, None)];
        let mut dag = dag_with_sorted_consumer("src", "consumer", req);
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", None))]);
        dag.insert_enforcer_sorts(&inputs).unwrap();
        dag.topo_order = toposort(&dag.graph, None).unwrap();
        dag.compute_node_properties(&inputs).unwrap();
        dag
    }

    /// Gate: `--explain` text rendering shows the synthesized Sort node
    /// with its `sort_fields:` block and a Physical Properties section
    /// recording ordering + provenance for it.
    #[test]
    fn test_explain_text_shows_enforced_sort_node() {
        let dag = dag_with_enforcer_sort();
        let text = dag.explain();

        assert!(
            text.contains("[sort] __sort_for_consumer"),
            "explain text must show synthesized sort node:\n{text}"
        );
        assert!(
            text.contains("sort_fields:"),
            "explain text must show sort_fields block:\n{text}"
        );
        assert!(
            text.contains("=== Physical Properties ==="),
            "explain text must include Physical Properties section:\n{text}"
        );
        assert!(
            text.contains("sort.__sort_for_consumer"),
            "Physical Properties must list the sort node by id slug:\n{text}"
        );
        assert!(
            text.contains("ordering: k"),
            "Physical Properties must show the sort node's ordering:\n{text}"
        );
    }

    /// Gate: `--explain=json` includes the synthesized Sort node and a
    /// `node_properties` section keyed by node *name* with an entry for
    /// the sort node carrying its ordering.
    #[test]
    fn test_explain_json_roundtrip_includes_sort_node() {
        let dag = dag_with_enforcer_sort();
        let json = serde_json::to_value(&dag).unwrap();

        // Sort node appears in `nodes` with type=sort and a __sort_for_ name.
        let nodes = json["nodes"].as_array().unwrap();
        let sort_node = nodes
            .iter()
            .find(|n| n["type"] == "sort")
            .expect("sort node must appear in JSON nodes list");
        let sort_name = sort_node["name"].as_str().unwrap();
        assert!(
            sort_name.starts_with("__sort_for_"),
            "sort node name must use reserved prefix: {sort_name}"
        );

        // node_properties is keyed by node name and contains the sort node.
        let props = json["node_properties"]
            .as_object()
            .expect("node_properties must be an object keyed by node name");
        let sort_props = props
            .get(sort_name)
            .expect("node_properties must contain the synthesized sort node");
        let order = sort_props["ordering"]["sort_order"]
            .as_array()
            .expect("sort node ordering.sort_order must be present");
        assert_eq!(order.len(), 1);
        assert_eq!(order[0]["field"], "k");
    }

    // ── Phase 16 Task 16.4.9 — select_aggregation_strategies tests ─────────

    fn make_aggregation(
        name: &str,
        group_by: Vec<&str>,
        hint: crate::config::AggregateStrategyHint,
    ) -> PlanNode {
        use cxl::plan::CompiledAggregate;
        PlanNode::Aggregation {
            name: name.into(),
            span: Span::SYNTHETIC,
            config: AggregateConfig {
                group_by: group_by.into_iter().map(String::from).collect(),
                cxl: "emit n = count()".to_string(),
                strategy: hint,
            },
            compiled: Arc::new(CompiledAggregate {
                bindings: vec![],
                group_by_indices: vec![],
                group_by_fields: vec![],
                pre_agg_filter: None,
                emits: vec![],
            }),
            strategy: AggregateStrategy::Hash,
            output_schema: Arc::new(Schema::new(vec![])),
            fallback_reason: None,
            skipped_streaming_available: false,
            qualified_sort_order: None,
        }
    }

    /// Build a fixture: Source(sort? optional) -> Aggregation(group_by, hint).
    fn agg_fixture(
        sort_field: Option<&str>,
        group_by: Vec<&str>,
        hint: crate::config::AggregateStrategyHint,
    ) -> (ExecutionPlanDag, NodeIndex) {
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "src".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                make_aggregation("agg", group_by, hint),
            ],
            &[(0, 1)],
        );
        let so = sort_field.map(|f| vec![sf(f, SortOrder::Asc, None)]);
        let inputs = HashMap::from([("src".to_string(), input_with_sort("src", so))]);
        dag.compute_node_properties(&inputs).unwrap();
        (dag, idxs[1])
    }

    #[test]
    fn test_select_strategies_auto_qualifies_flips_to_streaming() {
        let (mut dag, agg_idx) = agg_fixture(
            Some("dept"),
            vec!["dept"],
            crate::config::AggregateStrategyHint::Auto,
        );
        dag.select_aggregation_strategies().unwrap();
        match &dag.graph[agg_idx] {
            PlanNode::Aggregation {
                strategy,
                fallback_reason,
                qualified_sort_order,
                ..
            } => {
                assert!(matches!(strategy, AggregateStrategy::Streaming));
                assert!(fallback_reason.is_none());
                assert!(qualified_sort_order.is_some());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_select_strategies_auto_ineligible_stays_hash_with_reason() {
        let (mut dag, agg_idx) = agg_fixture(
            None, // unsorted
            vec!["dept"],
            crate::config::AggregateStrategyHint::Auto,
        );
        dag.select_aggregation_strategies().unwrap();
        match &dag.graph[agg_idx] {
            PlanNode::Aggregation {
                strategy,
                fallback_reason,
                ..
            } => {
                assert!(matches!(strategy, AggregateStrategy::Hash));
                assert!(fallback_reason.is_some());
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_select_strategies_explicit_streaming_eligible_ok() {
        let (mut dag, agg_idx) = agg_fixture(
            Some("dept"),
            vec!["dept"],
            crate::config::AggregateStrategyHint::Streaming,
        );
        dag.select_aggregation_strategies().unwrap();
        match &dag.graph[agg_idx] {
            PlanNode::Aggregation { strategy, .. } => {
                assert!(matches!(strategy, AggregateStrategy::Streaming));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_select_strategies_explicit_streaming_ineligible_compile_error_message_quotes_reason() {
        let (mut dag, _) = agg_fixture(
            None,
            vec!["dept"],
            crate::config::AggregateStrategyHint::Streaming,
        );
        let err = dag.select_aggregation_strategies().unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("CXL0419") || msg.contains("agg"));
        // Compilation variant carries the rendered walker output.
        match err {
            PipelineError::Compilation { messages, .. } => {
                let joined = messages.join("\n");
                assert!(joined.contains("CXL0419"));
                assert!(joined.contains("strategy: streaming"));
                assert!(joined.contains("help:"));
            }
            other => panic!("expected Compilation, got {other:?}"),
        }
    }

    #[test]
    fn test_select_strategies_explicit_hash_skips_streaming_path_records_annotation() {
        let (mut dag, agg_idx) = agg_fixture(
            Some("dept"),
            vec!["dept"],
            crate::config::AggregateStrategyHint::Hash,
        );
        dag.select_aggregation_strategies().unwrap();
        match &dag.graph[agg_idx] {
            PlanNode::Aggregation {
                strategy,
                skipped_streaming_available,
                ..
            } => {
                assert!(matches!(strategy, AggregateStrategy::Hash));
                assert!(*skipped_streaming_available);
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_select_strategies_streaming_node_ordering_provenance_is_introduced() {
        let (mut dag, agg_idx) = agg_fixture(
            Some("dept"),
            vec!["dept"],
            crate::config::AggregateStrategyHint::Auto,
        );
        dag.select_aggregation_strategies().unwrap();
        let props = dag.node_properties.get(&agg_idx).unwrap();
        assert!(props.ordering.sort_order.is_some());
        assert!(matches!(
            props.ordering.provenance,
            OrderingProvenance::IntroducedByStreamingAggregate { .. }
        ));
    }

    #[test]
    fn test_select_strategies_streaming_node_provenance_enabled_by_points_to_upstream() {
        let (mut dag, agg_idx) = agg_fixture(
            Some("dept"),
            vec!["dept"],
            crate::config::AggregateStrategyHint::Auto,
        );
        dag.select_aggregation_strategies().unwrap();
        let props = dag.node_properties.get(&agg_idx).unwrap();
        match &props.ordering.provenance {
            OrderingProvenance::IntroducedByStreamingAggregate { enabled_by, .. } => {
                // Source declared the input directly.
                assert!(matches!(
                    **enabled_by,
                    OrderingProvenance::DeclaredOnInput { .. }
                ));
            }
            other => panic!("expected IntroducedByStreamingAggregate, got {other:?}"),
        }
    }

    #[test]
    fn test_select_strategies_hash_node_ordering_provenance_is_destroyed() {
        let (mut dag, agg_idx) = agg_fixture(
            None,
            vec!["dept"],
            crate::config::AggregateStrategyHint::Auto,
        );
        dag.select_aggregation_strategies().unwrap();
        let props = dag.node_properties.get(&agg_idx).unwrap();
        assert!(props.ordering.sort_order.is_none());
        assert!(matches!(
            props.ordering.provenance,
            OrderingProvenance::DestroyedByHashAggregate { .. }
        ));
    }

    #[test]
    fn test_select_strategies_global_fold_always_streams() {
        // Empty group_by → always Streaming, even with no upstream sort.
        let (mut dag, agg_idx) =
            agg_fixture(None, vec![], crate::config::AggregateStrategyHint::Auto);
        dag.select_aggregation_strategies().unwrap();
        match &dag.graph[agg_idx] {
            PlanNode::Aggregation { strategy, .. } => {
                assert!(matches!(strategy, AggregateStrategy::Streaming));
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_parallel_transforms_preserve_order_for_streaming_agg() {
        // Task 16.4.11 — D3 structural guard. Phase 6 `par_iter_mut`
        // preserves input order in-place because chunks are mutated at
        // their original positions; workers share one Vec. Therefore a
        // `Stateless` (parallel) upstream transform that does NOT write
        // sort-key fields must still let the downstream aggregation
        // qualify for streaming. If this ever fails, the whole D3 design
        // assumption is wrong.
        let mut ws = BTreeSet::new();
        ws.insert("x".to_string()); // disjoint from sort key `k`
        let (mut dag, idxs) = dag_from_nodes(
            vec![
                PlanNode::Source {
                    name: "src".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                },
                PlanNode::Transform {
                    name: "t".into(),
                    span: Span::SYNTHETIC,
                    resolved: None,
                    parallelism_class: ParallelismClass::Stateless,
                    tier: 0,
                    execution_reqs: NodeExecutionReqs::Streaming,
                    window_index: None,
                    partition_lookup: None,
                    write_set: ws,
                    has_distinct: false,
                },
                make_aggregation("agg", vec!["k"], crate::config::AggregateStrategyHint::Auto),
            ],
            &[(0, 1), (1, 2)],
        );
        let inputs = HashMap::from([(
            "src".to_string(),
            input_with_sort("src", Some(vec![sf("k", SortOrder::Asc, None)])),
        )]);
        dag.compute_node_properties(&inputs).unwrap();
        dag.select_aggregation_strategies().unwrap();
        match &dag.graph[idxs[2]] {
            PlanNode::Aggregation { strategy, .. } => {
                assert!(
                    matches!(strategy, AggregateStrategy::Streaming),
                    "parallel stateless transform must not destroy sort-key ordering"
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_explain_renders_strategy_hint_and_resolved_strategy_distinctly() {
        // After post-pass: config.strategy (hint) and PlanNode.strategy (resolved)
        // both appear in JSON, distinctly.
        let (mut dag, agg_idx) = agg_fixture(
            Some("dept"),
            vec!["dept"],
            crate::config::AggregateStrategyHint::Auto,
        );
        dag.select_aggregation_strategies().unwrap();
        let json = serde_json::to_value(&dag.graph[agg_idx]).unwrap();
        // Hint surfaces under config.strategy.
        assert_eq!(json["config"]["strategy"], "auto");
        // Resolved strategy is the top-level field.
        assert_eq!(json["strategy"], "streaming");
    }
}
