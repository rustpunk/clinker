//! Phase F: Execution plan emission.
//!
//! Compiles a `PipelineConfig` + CXL programs into an `ExecutionPlanDag`
//! that orchestrates the pipeline via a petgraph DAG.

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use petgraph::algo::toposort;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{Control, DfsEvent, depth_first_search};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

use crate::config::{PipelineConfig, RouteMode, SortField, TransformInput};
use crate::plan::index::{self, IndexSpec, LocalWindowConfig, PlanIndexError, RawIndexRequest};

use cxl::analyzer::{self, ParallelismHint};
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
    },
    Transform {
        name: String,
        parallelism_class: ParallelismClass,
        tier: u32,
        execution_reqs: NodeExecutionReqs,
        /// Index into `ExecutionPlanDag.indices_to_build`, if this transform uses windows.
        #[serde(skip_serializing_if = "Option::is_none")]
        window_index: Option<usize>,
        /// How to look up the partition for this transform's window.
        #[serde(skip)]
        partition_lookup: Option<PartitionLookupKind>,
    },
    Route {
        name: String,
        mode: RouteMode,
        branches: Vec<String>,
        default: String,
    },
    Merge {
        name: String,
    },
    Output {
        name: String,
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
        sort_fields: Vec<SortField>,
    },
}

impl PlanNode {
    /// Get the name of this node regardless of variant.
    pub fn name(&self) -> &str {
        match self {
            PlanNode::Source { name }
            | PlanNode::Transform { name, .. }
            | PlanNode::Route { name, .. }
            | PlanNode::Merge { name }
            | PlanNode::Output { name }
            | PlanNode::Sort { name, .. } => name,
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
        }
    }

    /// Build the id slug: `"{type}.{name}"`.
    pub fn id_slug(&self) -> String {
        format!("{}.{}", self.type_tag(), self.name())
    }

    /// Human-readable label for DOT node annotations and text display.
    pub fn display_name(&self) -> String {
        match self {
            PlanNode::Source { name } => format!("[source] {name}"),
            PlanNode::Transform { name, .. } => format!("[transform] {name}"),
            PlanNode::Route { name, mode, .. } => {
                let mode_str = match mode {
                    RouteMode::Exclusive => "exclusive",
                    RouteMode::Inclusive => "inclusive",
                };
                format!("[route:{mode_str}] {name}")
            }
            PlanNode::Merge { name } => format!("[merge] {name}"),
            PlanNode::Output { name } => format!("[output] {name}"),
            PlanNode::Sort { name, .. } => format!("[sort] {name}"),
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

    /// Whether any node requires sorted input (correlation key).
    pub fn required_sorted_input(&self) -> bool {
        self.correlation_sort_note.is_some() && !self.required_arena()
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

        // Parse local_window configs
        let window_configs: Vec<Option<LocalWindowConfig>> = config
            .transforms()
            .map(|t| index::parse_local_window(t).map_err(PlanError::IndexPlanning))
            .collect::<Result<Vec<_>, _>>()?;

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
                let already_sorted = check_already_sorted(config, &source, &wc.sort_by);

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
        let source_dag = build_source_dag(config, &window_configs, &primary_source)?;

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
                },
                &mut node_by_name,
                &mut slug_set,
            )?;
        }

        // Transform nodes in declaration order (deterministic toposort per V-7-2)
        let resolved_transforms: Vec<_> = config.transforms().collect();
        for (i, tc) in resolved_transforms.iter().enumerate() {
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

            add_node(
                &mut graph,
                PlanNode::Transform {
                    name: tc.name.clone(),
                    parallelism_class,
                    tier: 0, // assigned later via BFS
                    execution_reqs,
                    window_index,
                    partition_lookup,
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
                        mode: rc.mode,
                        branches: rc.branches.iter().map(|b| b.name.clone()).collect(),
                        default: rc.default.clone(),
                    },
                    &mut node_by_name,
                    &mut slug_set,
                )?;
            }

            // Merge node for multiple inputs
            if let Some(TransformInput::Multiple(_)) = &tc.input {
                add_node(
                    &mut graph,
                    PlanNode::Merge {
                        name: format!("merge_{}", tc.name),
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
                },
                &mut node_by_name,
                &mut slug_set,
            )?;
        }

        // --- Phase 2: Wire all edges ---
        let mut prev_transform_key: Option<String> = None;

        for (i, tc) in resolved_transforms.iter().enumerate() {
            let wc = &window_configs[i];
            let transform_key = format!("transform.{}", tc.name);
            let transform_idx = node_by_name[&transform_key];

            // Wire edges from input: or implicit linear chain
            match &tc.input {
                Some(TransformInput::Single(upstream)) => {
                    let upstream_idx =
                        resolve_input_reference(upstream, &node_by_name, &tc.name, config)?;
                    graph.add_edge(
                        upstream_idx,
                        transform_idx,
                        PlanEdge {
                            dependency_type: DependencyType::Data,
                        },
                    );
                }
                Some(TransformInput::Multiple(upstreams)) => {
                    let merge_key = format!("merge.merge_{}", tc.name);
                    let merge_idx = node_by_name[&merge_key];
                    for upstream in upstreams {
                        let upstream_idx =
                            resolve_input_reference(upstream, &node_by_name, &tc.name, config)?;
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
                None => {
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

        Ok(ExecutionPlanDag {
            graph,
            topo_order,
            source_dag,
            indices_to_build: indices,
            output_projections,
            parallelism,
            correlation_sort_note: None,
            node_properties: HashMap::new(),
        })
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

        // Show route info from graph nodes
        for node in self.graph.node_weights() {
            if let PlanNode::Route {
                name,
                mode,
                branches,
                default,
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
                PlanNode::Merge { name } => {
                    out.push_str(&format!("  └──< MERGE '{name}'\n"));
                }
                PlanNode::Source { .. }
                | PlanNode::Transform { .. }
                | PlanNode::Output { .. }
                | PlanNode::Sort { .. } => {
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
impl Serialize for ExecutionPlanDag {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut map = serializer.serialize_map(Some(2))?;
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
    config: &PipelineConfig,
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

    // Try dotted branch reference: "route_name.branch_name" -> route node
    if reference.contains('.') {
        let parts: Vec<&str> = reference.splitn(2, '.').collect();
        let route_key = format!("route.route_{}", parts[0]);
        if let Some(&idx) = node_by_name.get(&route_key) {
            return Ok(idx);
        }
    }

    // Collect available transforms and branches for error message
    let available: Vec<String> = config.transforms().map(|t| t.name.clone()).collect();

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
    config: &PipelineConfig,
    window_configs: &[Option<LocalWindowConfig>],
    primary_source: &str,
) -> Result<Vec<SourceTier>, PlanError> {
    let all_sources: Vec<String> = config.inputs.iter().map(|i| i.name.clone()).collect();

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

/// Check if a source's declared sort_order matches the window's sort_by.
fn check_already_sorted(_config: &PipelineConfig, _source: &str, _sort_by: &[SortField]) -> bool {
    // InputConfig doesn't have sort_order yet (to be added in Task 5.4.1)
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

    /// Extract resolved TransformConfig from TransformEntry (tests only).
    fn t(entry: &TransformEntry) -> &TransformConfig {
        match entry {
            TransformEntry::Transform(t) => t,
            _ => panic!("test expects resolved transform"),
        }
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
            inputs: inputs
                .into_iter()
                .map(|(name, path)| InputConfig {
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
                .map(|(name, cxl, local_window)| {
                    TransformEntry::Transform(TransformConfig {
                        name: name.into(),
                        description: None,
                        cxl: Some(cxl.into()),
                        aggregate: None,
                        local_window,
                        log: None,
                        validations: None,
                        route: None,
                        input: None,
                        parallelism: None,
                        notes: None,
                    })
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
        let schema: HashMap<String, cxl::typecheck::types::Type> = HashMap::new();
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
            inputs: vec![InputConfig {
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
            transformations: vec![TransformEntry::Transform(TransformConfig {
                name: "router".into(),
                description: None,
                cxl: Some(cxl.into()),
                aggregate: None,
                local_window: None,
                log: None,
                validations: None,
                route: Some(route),
                input: None,
                parallelism: None,
                notes: None,
            })],
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

    /// Gate test for Task 16.0.5.3: PlanNode::Sort variant exists,
    /// has correct serde tag, name/type_tag/display_name/id_slug arms,
    /// and round-trips through JSON.
    #[test]
    fn test_plan_node_sort_exhaustive_matches() {
        let node = PlanNode::Sort {
            name: "x".into(),
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
}
