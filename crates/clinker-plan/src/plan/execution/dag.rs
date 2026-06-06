//! DAG construction, topological tiering, cycle detection, and parallelism
//! derivation for the execution plan.

use super::*;

use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{Control, DfsEvent, depth_first_search};
use serde::Serialize;

use crate::config::SourceConfig;
use crate::plan::index::{AnalyticWindowSpec, IndexSpec};

use cxl::analyzer::ParallelismHint;

impl ExecutionPlanDag {
    /// Construct from already-computed parts. The compile path supplies
    /// the populated graph + topology + per-transform plan metadata;
    /// `node_properties` and `deferred_regions` are populated by later
    /// passes against the returned DAG via `&mut`. Test fixtures that
    /// drive a single planner pass against a hand-built graph (e.g. the
    /// combine-strategy tests in `crates/clinker-exec/tests/`) call this
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
            parent_continuations: HashMap::new(),
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
    pub fn from_body(body: &crate::plan::composition_body::BoundBody) -> Self {
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
            // Body-internal Composition continuations ride along the
            // same way: when the outer body's dispatcher walks a
            // nested Composition, it reads continuations from the same
            // surface as the parent dispatcher uses.
            parent_continuations: body.parent_continuations.clone(),
        }
    }

    /// `Some(region)` iff `idx` participates in any deferred region on
    /// this DAG (producer, member, or output). Dispatcher arms call
    /// this at the top of every operator branch to decide whether to
    /// short-circuit to the deferred buffer.
    pub fn deferred_region_at(
        &self,
        idx: NodeIndex,
    ) -> Option<&crate::plan::deferred_region::DeferredRegion> {
        self.deferred_regions.get(&idx)
    }

    /// `true` iff `idx` is a non-producer participant in some deferred
    /// region (member or output) OR a member/output of a Composition
    /// parent-continuation. Operator arms use this to skip work on the
    /// forward pass — the commit-time deferred dispatch will run the
    /// operator on post-recompute data (aggregate emits or harvested
    /// body output records, depending on the surface).
    pub fn is_deferred_consumer(&self, idx: NodeIndex) -> bool {
        self.deferred_regions
            .get(&idx)
            .is_some_and(|r| r.producer != idx)
            || self
                .parent_continuations
                .values()
                .any(|c| c.members.contains(&idx) || c.outputs.contains(&idx))
    }

    /// `Some(region)` iff `idx` is the producer of a deferred region.
    /// The Aggregation arm uses this to project emits to
    /// `region.buffer_schema` before parking them in `node_buffers`.
    pub fn deferred_region_at_producer(
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

    /// Coarse plan-time estimate, in bytes, of the disk volume the run could
    /// spill — the sum of every spilling operator's predicted peak live state.
    ///
    /// A spilling operator (hash Aggregate, sort, grace-hash / sort-merge /
    /// IEJoin / hash Combine) holds its whole accumulated input before it can
    /// emit, and spills that state when the memory budget trips; a streaming
    /// or sink stage spills nothing. Summing rather than taking the max is the
    /// conservative choice for a free-space preflight: two blocking operators
    /// can be live and spilled simultaneously, so their footprints add. The
    /// figure derives from the same `predicted_peak_bytes` estimates
    /// `--explain` surfaces, so a preflight warning lines up with the numbers
    /// a pipeline author already sees. Returns `0` when no node spills or when
    /// volume estimates are unknown (no on-disk file seed reached the plan).
    pub fn estimated_spill_bytes(&self) -> u64 {
        self.topo_order
            .iter()
            .filter(|&&idx| {
                super::arbitration_class(&self.graph[idx])
                    .spill_priority
                    .is_some()
            })
            .filter_map(|idx| self.node_properties.get(idx))
            .fold(0u64, |acc, props| {
                acc.saturating_add(props.predicted_peak_bytes)
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
) -> Vec<clinker_core_types::Diagnostic> {
    use clinker_core_types::{Diagnostic, LabeledSpan};
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
    wc: &Option<AnalyticWindowSpec>,
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
    window_configs: &[Option<AnalyticWindowSpec>],
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
            parent_continuations: HashMap::new(),
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
            parent_continuations: HashMap::new(),
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
