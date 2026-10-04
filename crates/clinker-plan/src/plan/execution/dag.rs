//! DAG construction, topological tiering, cycle detection, and parallelism
//! derivation for the execution plan.

use super::*;

use std::collections::{HashMap, HashSet};

use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{Control, DfsEvent, depth_first_search};
use serde::Serialize;

use crate::config::SourceConfig;
use crate::plan::index::{AnalyticWindowSpec, IndexSpec};

use cxl::analyzer::ParallelismHint;

/// One blocking operator's plan-time spill-volume estimate, surfaced in
/// `clinker run --explain` so an operator sees the per-stage footprint
/// before a run fills the spill volume.
///
/// Produced by [`ExecutionPlanDag::per_stage_spill_estimates`]. `estimate_bytes`
/// is `0` when the stage's volume is unknown at plan time (no on-disk file-size
/// seed reached it) — the renderer shows "unknown" rather than a misleading `0`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StageSpillEstimate {
    /// The spilling operator's pipeline node name (the key the runtime
    /// per-stage actual-spill breakdown also uses, so estimate and actual
    /// line up by name).
    pub node_name: String,
    /// The operator's `--explain` display name (strategy/role suffix and all).
    pub display_name: String,
    /// Coarse plan-time estimate, in bytes, of the live state this stage
    /// could spill. `0` means unknown — see the type-level note.
    pub estimate_bytes: u64,
}

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
        let mut dag = Self {
            consumer_registry: CompiledConsumerRegistry::compile(&graph),
            order_contract: ExecutionOrderContract::default(),
            source_activation: SourceActivationPlan::default(),
            graph,
            topo_order,
            source_dag,
            indices_to_build,
            output_projections,
            parallelism,
            node_properties: HashMap::new(),
            deferred_regions: HashMap::new(),
            parent_continuations: HashMap::new(),
            id_to_index: SecondaryMap::with_default(None),
        };
        dag.rebuild_id_index();
        dag
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
            consumer_registry: CompiledConsumerRegistry::compile(&body.graph),
            order_contract: ExecutionOrderContract::default(),
            source_activation: SourceActivationPlan::default(),
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
            // Runtime body DAGs never resolve id→index; the bridge is a
            // compile-time concern (composition window resolution runs
            // against the parent DAG at compile, not here), so leave it
            // empty rather than clone the body's.
            id_to_index: SecondaryMap::with_default(None),
        }
    }

    /// Rebuild the [`PlanNodeId`] → [`NodeIndex`] bridge from the current
    /// graph. Call after every toposort or structural mutation that
    /// finalizes the graph: the bridge must name every live node so a
    /// later id lookup resolves to the node's current storage position.
    /// Replaces the prior map wholesale so an id whose node was removed no
    /// longer resolves. See [`build_id_index`] for the coverage invariant.
    pub fn rebuild_id_index(&mut self) {
        self.id_to_index = build_id_index(&self.graph);
        self.consumer_registry = CompiledConsumerRegistry::compile(&self.graph);
    }

    /// Current storage position of the node with stable identity `id`, or
    /// `None` when no live node carries that id.
    pub fn index_of(&self, id: PlanNodeId) -> Option<NodeIndex> {
        self.id_to_index[id]
    }

    /// Storage position of `id`'s node, preferring the O(1) [`index_of`] bridge
    /// and falling back to a linear scan when the bridge is empty — as it is on
    /// body DAGs built by [`from_body`]. Resolving by id rather than name is
    /// load-bearing: a composition body can contain an authored node whose name
    /// collides with a synthesized input-port `Source` at a lower `NodeIndex`,
    /// which a name match would wrongly select.
    ///
    /// [`index_of`]: Self::index_of
    /// [`from_body`]: Self::from_body
    pub(crate) fn index_of_or_scan(&self, id: PlanNodeId) -> Option<NodeIndex> {
        self.index_of(id).or_else(|| {
            self.graph
                .node_indices()
                .find(|&i| self.graph[i].id() == id)
        })
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

    /// Return the recursively sealed external Source activation inventory.
    pub fn source_activation(&self) -> &SourceActivationPlan {
        &self.source_activation
    }

    /// Seal Source activation after the top-level DAG and every body mini-DAG
    /// have reached their final compiled topology.
    pub(crate) fn seal_source_activation(
        &mut self,
        bodies: &crate::plan::composition_body::CompositionBodies,
    ) -> Result<(), SourceActivationPlanError> {
        let compiled = compile_source_activation_plan(self, bodies)?;
        self.source_activation = compiled;
        Ok(())
    }

    /// Coarse plan-time estimate, in bytes, of the disk volume the run could
    /// spill — the sum of every spill-writing operator's predicted peak live
    /// state.
    ///
    /// A spill-writing operator (external sort, hash Aggregate, grace-hash /
    /// sort-merge Combine, block-band `IEJoin` / `HashPartitionIEJoin` — see
    /// [`super::writes_spill_files`]) holds its whole accumulated input before
    /// it can emit, and writes that state to a spill file when the memory budget
    /// trips; the block-band path writes it about twice (sort runs plus block
    /// files), reflected by [`super::spill_volume_multiplier`]. The in-memory
    /// inline hash build/probe join carries an arbitration `spill_priority` but
    /// runs its kernel entirely in RAM, so it contributes nothing to disk volume
    /// and is excluded — counting it would over-state the free-space a run
    /// needs. Summing rather than taking the max is the conservative
    /// choice for a free-space preflight: two blocking operators can be live
    /// and spilled simultaneously, so their footprints add. The figure derives
    /// from the same `predicted_peak_bytes` estimates `--explain` surfaces, so
    /// a preflight warning lines up with the numbers a pipeline author already
    /// sees. Returns `0` when no node spills or when volume estimates are
    /// unknown (no on-disk file seed reached the plan).
    pub fn estimated_spill_bytes(&self) -> u64 {
        self.topo_order
            .iter()
            .filter(|&&idx| super::writes_spill_files(&self.graph[idx]))
            .filter_map(|&idx| {
                self.node_properties
                    .get(&idx)
                    .map(|props| (idx, props.predicted_peak_bytes))
            })
            .fold(0u64, |acc, (idx, peak)| {
                acc.saturating_add(
                    peak.saturating_mul(super::spill_volume_multiplier(&self.graph[idx])),
                )
            })
    }

    /// Per-blocking-stage spill-volume estimate, one entry per spill-writing
    /// operator (external sort, hash Aggregate, grace-hash / sort-merge Combine,
    /// block-band `IEJoin` / `HashPartitionIEJoin` — see
    /// [`super::writes_spill_files`]) in topological order.
    ///
    /// The in-memory inline hash build/probe join carries an arbitration
    /// `spill_priority` but never writes a spill file, so it does not appear
    /// here — the per-stage estimate describes what reaches disk. Each
    /// [`StageSpillEstimate`] carries the operator's node name, its `--explain`
    /// display name, and its `predicted_peak_bytes` scaled by
    /// [`super::spill_volume_multiplier`] (about 2× for the block-band path,
    /// which writes sort runs then block files). `estimate_bytes`
    /// is `0` (rendered "unknown") when no on-disk file-size seed reached the
    /// node: a multi-file source matcher (glob/regex/paths) whose discovered
    /// sizes were not summed at plan time, a missing/unreadable input, or a
    /// network source. Summing the entries equals [`Self::estimated_spill_bytes`].
    /// The figures are the same `predicted_peak_bytes` the Physical Properties
    /// arbitration line surfaces, so the per-stage estimate and the cap-headroom
    /// preflight line up with what an operator already reads.
    pub fn per_stage_spill_estimates(&self) -> Vec<StageSpillEstimate> {
        self.topo_order
            .iter()
            .filter(|&&idx| super::writes_spill_files(&self.graph[idx]))
            .map(|&idx| {
                let node = &self.graph[idx];
                StageSpillEstimate {
                    node_name: node.name().to_string(),
                    display_name: node.display_name(),
                    estimate_bytes: self
                        .node_properties
                        .get(&idx)
                        .map_or(0, |p| p.predicted_peak_bytes)
                        .saturating_mul(super::spill_volume_multiplier(node)),
                }
            })
            .collect()
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
    /// constitute branching — they're handled by the Route dispatch arm.
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

/// E378: refuse a Sink inside any composition body while a Source declares
/// `dlq_granularity: document`.
///
/// Document granularity guarantees that no Sink writes a record of a
/// document that is rejected. The plan and the scheduler hold that
/// guarantee by running every Sink after every other node, so each
/// document's verdict is final before any Sink writes. A body Sink would run
/// inside its composition's dispatch, in the middle of the top-level walk,
/// where that ordering cannot reach it. Body Sinks do not write in a run yet
/// (#1242); the refusal keeps the guarantee for when they do, rather than
/// guaranteeing it for some Sinks only.
///
/// The help gives one fix. Where moving the Sink to the pipeline through a
/// new composition output port runs on the engine as it is, the help is
/// that move as numbered fragments the author can paste, the pipeline Sink
/// block carrying the body Sink's compiled configuration rendered back to
/// YAML, with the columns of a Sink that writes only emitted columns written
/// out as its `mapping:`. A call carries only the rows of its composition's
/// first declared output port, and only when nothing else in the body reads
/// that port's node (#1315), so the move is offered only for a
/// pipeline-level call, made once, whose body Sink reads the plain node
/// behind that first port and shares it with no other body node, and whose
/// columns a `mapping:` can state. Everywhere else the help is one next step,
/// the explain page, with the reason.
///
/// Every help is written as if every movable Sink moves, so applying them
/// all gives one consistent pipeline: new ports and pipeline Sink names take
/// the Sink's name, or that name with the first free `_2`, `_3`, ... when
/// it is already used (by an existing output or pipeline node, or by an
/// earlier move), and a port count is the count after every move.
///
/// The caller runs this only when a Source declares document granularity;
/// `source` names the first such Source in declaration order. The walk
/// starts at the pipeline's composition calls and descends through each
/// body's nested calls, so a Sink at any depth is reached through the call
/// that owns it. Returns one diagnostic per body Sink, in that walk's order.
pub(crate) fn diagnose_document_dlq_body_sinks(
    dag: &ExecutionPlanDag,
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
    signatures: &crate::config::composition::CompositionSymbolTable,
    source: &str,
) -> Vec<clinker_core_types::Diagnostic> {
    use clinker_core_types::QuoteName;
    use clinker_core_types::{Diagnostic, LabeledSpan};
    use petgraph::Direction;

    type BodyId = crate::plan::composition_body::CompositionBodyId;
    /// A body Sink: the body that declares it and its node there.
    type BodySink = (BodyId, NodeIndex);

    /// The composition node that binds a body: its name and span, and the
    /// body that contains it (`None` for a call in the pipeline itself).
    struct Call<'a> {
        name: &'a str,
        span: clinker_core_types::Span,
        parent: Option<BodyId>,
    }

    /// A movable Sink's names: its new port, the node that port reads, and
    /// its name at pipeline level.
    struct Move<'a> {
        port: String,
        port_node: &'a str,
        pipeline_name: String,
    }

    fn collect_calls<'a>(
        graph: &'a DiGraph<PlanNode, PlanEdge>,
        parent: Option<BodyId>,
        out: &mut HashMap<BodyId, Call<'a>>,
    ) {
        for node in graph.node_weights() {
            if let PlanNode::Composition {
                name, span, body, ..
            } = node
            {
                out.insert(
                    *body,
                    Call {
                        name,
                        span: *span,
                        parent,
                    },
                );
            }
        }
    }

    /// Every Sink under `body_id`: its own Sinks and those of the
    /// compositions it calls, in body node order, a nested call's Sinks at
    /// the call's position.
    fn body_sinks(
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        body_id: BodyId,
        out: &mut Vec<BodySink>,
    ) {
        let Some(body) = artifacts.composition_bodies.get(&body_id) else {
            return;
        };
        for idx in body.graph.node_indices() {
            match &body.graph[idx] {
                PlanNode::Sink { .. } => out.push((body_id, idx)),
                PlanNode::Composition { body: nested, .. } => body_sinks(artifacts, *nested, out),
                _ => {}
            }
        }
    }

    /// `name`, or `name` with the first free numeric suffix, recorded as
    /// taken. One rule for ports and pipeline Sink names.
    fn free_name(name: &str, taken: &mut HashSet<String>) -> String {
        let mut candidate = name.to_owned();
        let mut suffix = 1;
        while taken.contains(&candidate) {
            suffix += 1;
            candidate = format!("{name}_{suffix}");
        }
        taken.insert(candidate.clone());
        candidate
    }

    let mut calls = HashMap::new();
    collect_calls(&dag.graph, None, &mut calls);
    for (body_id, body) in &artifacts.composition_bodies {
        collect_calls(&body.graph, Some(*body_id), &mut calls);
    }

    // Every body Sink, reached from the pipeline's calls in node order.
    let mut sinks = Vec::new();
    for node in dag.graph.node_weights() {
        if let PlanNode::Composition { body, .. } = node {
            body_sinks(artifacts, *body, &mut sinks);
        }
    }

    // How many calls bind each composition file.
    let mut uses: HashMap<&std::path::Path, usize> = HashMap::new();
    for (body_id, body) in &artifacts.composition_bodies {
        if calls.contains_key(body_id) {
            *uses.entry(body.signature_path.as_path()).or_default() += 1;
        }
    }

    // Why a body Sink cannot be moved through a port today: the node behind
    // the first port when it can, the reason when it cannot.
    let movable = |(body_id, idx): BodySink| -> Result<&str, String> {
        let body = &artifacts.composition_bodies[&body_id];
        let call = &calls[&body_id];
        let quoted_call = call.name.quoted_name();
        if call.parent.is_some() {
            return Err(format!(
                "composition {quoted_call} is called inside another composition"
            ));
        }
        let count = uses
            .get(body.signature_path.as_path())
            .copied()
            .unwrap_or(0);
        if count > 1 {
            return Err(format!(
                "`{file}` is used by {count} composition calls, so the moved Sink would be \
                 declared once per call, each writing the same output",
                file = composition_file_label(&body.signature_path),
            ));
        }
        let Some((port, &port_node)) = body.output_port_to_node_idx.first() else {
            return Err(format!(
                "composition {quoted_call} declares no output port to carry the Sink's rows"
            ));
        };
        let port_node_name = body.graph[port_node].name();
        let reads = body
            .graph
            .neighbors_directed(idx, Direction::Incoming)
            .next();
        if reads != Some(port_node) {
            let reads = reads.map_or("", |pred| body.graph[pred].name());
            return Err(format!(
                "Sink {quoted_sink} reads {quoted_reads}, and only the first output port of \
                 composition {quoted_call}, `{port}` from {quoted_port_node}, carries rows to \
                 the pipeline",
                quoted_sink = body.graph[idx].name().quoted_name(),
                quoted_reads = reads.quoted_name(),
                quoted_port_node = port_node_name.quoted_name(),
            ));
        }
        if let Some(input_port) = body
            .port_name_to_node_idx
            .iter()
            .find_map(|(name, &node)| (node == port_node).then_some(name))
        {
            return Err(format!(
                "the first output port of composition {quoted_call}, `{port}`, reads input \
                 port {quoted_input} rather than a node of the composition",
                quoted_input = input_port.quoted_name(),
            ));
        }
        if matches!(body.graph[port_node], PlanNode::Composition { .. })
            || body.graph[port_node].output_ports().is_some()
        {
            return Err(format!(
                "the first output port of composition {quoted_call}, `{port}`, reads \
                 {quoted_port_node}, which sends rows to output ports of its own, and a \
                 composition output port cannot yet carry rows from one of those",
                quoted_port_node = port_node_name.quoted_name(),
            ));
        }
        if let Some(reader) = body
            .graph
            .neighbors_directed(port_node, Direction::Outgoing)
            .find(|reader| !matches!(body.graph[*reader], PlanNode::Sink { .. }))
        {
            return Err(format!(
                "{quoted_port_node} also feeds {quoted_reader} inside composition \
                 {quoted_call}, and a composition output port cannot yet carry rows from a \
                 node that another node in the composition reads",
                quoted_port_node = port_node_name.quoted_name(),
                quoted_reader = body.graph[reader].name().quoted_name(),
            ));
        }
        if projected_columns(&body.graph, idx).is_some_and(|kept| kept.is_empty()) {
            return Err(format!(
                "Sink {quoted_sink} sets `include_unmapped: false` and its `exclude:` removes \
                 every column {quoted_port_node} emits, so at pipeline level it would write \
                 the columns composition {quoted_call} passes through instead",
                quoted_sink = body.graph[idx].name().quoted_name(),
                quoted_port_node = port_node_name.quoted_name(),
            ));
        }
        Ok(port_node_name)
    };

    // Names for every move, in walk order: a port per composition and a
    // pipeline Sink name, each free of what is already there and of earlier
    // moves. A composition's taken set ends as every output its file declares
    // after every move, so its size is the port count step 3 states.
    let mut port_taken: HashMap<BodyId, HashSet<String>> = HashMap::new();
    let mut sink_taken: HashSet<String> = dag
        .graph
        .node_weights()
        .map(|node| node.name().to_owned())
        .collect();
    let mut decisions: Vec<(BodySink, Result<Move<'_>, String>)> = Vec::new();
    for (body_id, idx) in sinks {
        let decision = movable((body_id, idx)).map(|port_node| {
            let body = &artifacts.composition_bodies[&body_id];
            let sink = body.graph[idx].name();
            // Every declared output name is taken, including one whose alias
            // names no body node: bind drops that output, but its key stays
            // under `_compose.outputs:`.
            let ports = port_taken.entry(body_id).or_insert_with(|| {
                body.output_port_to_node_idx
                    .keys()
                    .chain(
                        signatures
                            .get(&body.signature_path)
                            .into_iter()
                            .flat_map(|signature| signature.outputs.keys()),
                    )
                    .cloned()
                    .collect()
            });
            Move {
                port: free_name(sink, ports),
                port_node,
                pipeline_name: free_name(sink, &mut sink_taken),
            }
        });
        decisions.push(((body_id, idx), decision));
    }

    let mut out = Vec::new();
    for ((body_id, idx), decision) in decisions {
        let body = &artifacts.composition_bodies[&body_id];
        let call = &calls[&body_id];
        let PlanNode::Sink {
            name: sink,
            span: sink_span,
            resolved,
            ..
        } = &body.graph[idx]
        else {
            continue;
        };
        let quoted_sink = sink.quoted_name();
        let quoted_call = call.name.quoted_name();
        let help = match decision {
            Err(reason) => format!(
                "run `clinker explain --code E378` and follow its steps for declaring Sink \
                 {quoted_sink} at pipeline level: moving it through a composition output port \
                 does not work here, because {reason}"
            ),
            Ok(Move {
                port,
                port_node,
                pipeline_name,
            }) => {
                let file = composition_file_label(&body.signature_path);
                // The reference as authored; the port node is the same node.
                let feeding = body
                    .node_input_refs
                    .get(sink)
                    .and_then(|refs| refs.first().map(String::as_str))
                    .unwrap_or(port_node);
                let mut steps = vec![
                    format!("in `{file}`, under `_compose.outputs:`, add:\n    {port}: {feeding}"),
                    format!("in `{file}`, remove Sink {quoted_sink} from `nodes:`"),
                ];
                if let [only_port] = body
                    .output_port_to_node_idx
                    .keys()
                    .collect::<Vec<_>>()
                    .as_slice()
                {
                    steps.push(format!(
                        "composition {quoted_call} then has {count} output ports, so read \
                         `{call}.{only_port}` wherever the pipeline reads `{call}` without a port",
                        count = port_taken.get(&body_id).map_or(0, HashSet::len),
                        call = call.name,
                    ));
                }
                // The Sink's own configuration, under its pipeline name. A
                // Sink that writes only emitted columns gets them written out
                // as its `mapping:`: in the body its input is the port node,
                // which emits only those columns, but at pipeline level its
                // input is the call, whose emitted columns are its whole
                // output, pass-through columns included. A mapping under
                // `include_unmapped: false` writes exactly the listed columns
                // in the listed order, which is what the body Sink wrote.
                let config = resolved.as_deref().map(|payload| {
                    let mut config = payload.sink.clone();
                    config.name.clone_from(&pipeline_name);
                    if let Some(columns) = projected_columns(&body.graph, idx) {
                        config.mapping = Some(crate::config::OutputMapping::new(
                            columns
                                .into_iter()
                                .map(crate::config::MappingEntry::passthrough)
                                .collect(),
                        ));
                    }
                    config
                });
                steps.push(format!(
                    "under the pipeline's `nodes:`, add:\n  - type: sink\n    name: \
                     {pipeline_name}\n    input: {call}.{port}\n{config}",
                    call = call.name,
                    config = render_sink_config(config.as_ref(), sink),
                ));
                let renamed = if pipeline_name == *sink {
                    String::new()
                } else {
                    format!(
                        ", as Sink {quoted} because {quoted_sink} is taken there,",
                        quoted = pipeline_name.quoted_name()
                    )
                };
                let mut help = format!(
                    "move Sink {quoted_sink} to the pipeline{renamed} and feed it through a new \
                     output port of composition {quoted_call}, so it writes only after every \
                     document's verdict is final",
                );
                for (number, step) in steps.iter().enumerate() {
                    help.push_str(&format!("\n{}. {step}", number + 1));
                }
                help
            }
        };
        let message = format!(
            "composition {quoted_call} declares Sink {quoted_sink} in its body, but \
             source {quoted_source} declares `dlq_granularity: document`, which needs \
             every Sink declared at pipeline level",
            quoted_source = source.quoted_name(),
        );
        out.push(
            Diagnostic::error(
                "E378",
                message,
                LabeledSpan::primary(*sink_span, "Sink declared inside a composition body"),
            )
            .with_secondary(LabeledSpan::primary(call.span, "composition invoked here"))
            .with_help(help),
        );
    }
    out
}

/// A composition file as the help names it: its workspace-relative path with
/// `/` between components, the form a `use:` line spells on every platform.
fn composition_file_label(path: &std::path::Path) -> String {
    path.components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

/// The columns a body Sink restricts its output to, where the moved Sink
/// would not restrict itself to the same ones, in the order it writes them;
/// `None` when the moved Sink, with the body Sink's configuration unchanged,
/// writes what the body Sink wrote.
///
/// A Sink with `include_unmapped: false` and no `mapping:` writes only the
/// columns its input emits ([`cxl_emit_walk`], the walk the runtime
/// projection reads), less the names its `exclude:` lists, in the order the
/// record carries them, which is the emit walk's schema order. Only a walk
/// that ends at a Transform reports fewer columns than the row the
/// composition call passes on: the call's own emit names are its whole
/// output schema, so the moved Sink would also write the columns the
/// Transform passes through. Every other end already reports the whole row
/// the port carries, engine-stamped columns included, which the runtime
/// projection never writes, so the unchanged configuration writes the same
/// columns and no `mapping:` is needed (one would name those engine columns,
/// written as empty cells). An input that emits no named column applies no
/// restriction at runtime, so it is `None` here too. `Some` of an empty list
/// is a Sink whose `exclude:` removes every column a Transform emits.
fn projected_columns(graph: &DiGraph<PlanNode, PlanEdge>, idx: NodeIndex) -> Option<Vec<String>> {
    let PlanNode::Sink { resolved, .. } = &graph[idx] else {
        return None;
    };
    let sink = &resolved.as_deref()?.sink;
    if sink.include_unmapped || sink.mapping.is_some() {
        return None;
    }
    let (end, emitted) = cxl_emit_walk(graph, idx)?;
    if !matches!(graph[end], PlanNode::Transform { .. }) || emitted.is_empty() {
        return None;
    }
    let excluded = sink.exclude.as_deref().unwrap_or_default();
    Some(
        emitted
            .into_iter()
            .filter(|column| !excluded.contains(column))
            .collect(),
    )
}

/// The `config:` lines of a pipeline Sink block: the body Sink's compiled
/// configuration rendered back to YAML, indented under `config:`.
///
/// Every lowered Sink carries its configuration and a `SinkConfig` always
/// serializes, so the fallback is reached only if lowering changes shape; it
/// says in words where to copy the block from rather than print YAML that
/// would not compile.
fn render_sink_config(sink: Option<&crate::config::SinkConfig>, name: &str) -> String {
    use clinker_core_types::QuoteName;
    let Some(Ok(rendered)) = sink.map(crate::yaml::to_string) else {
        return format!(
            "    config: copy the `config:` block of Sink {quoted} from the composition file \
             unchanged",
            quoted = name.quoted_name()
        );
    };
    let mut block = String::from("    config:");
    for line in rendered.lines().filter(|line| !line.trim().is_empty()) {
        block.push_str("\n      ");
        block.push_str(line);
    }
    block
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
///
/// A projection-shaped summary of each Sink node, carried on the execution
/// plan alongside the graph. The executor does not read it — it projects from
/// the `SinkConfig` on the node's resolved payload — so the fields here exist
/// to describe the plan, not to drive it; `clinker --explain` reports the count.
/// `mapping` mirrors [`crate::config::OutputMapping::entries`] so the summary
/// cannot disagree with the config about the block's shape or direction.
#[derive(Debug, Clone)]
pub struct OutputSpec {
    pub name: String,
    pub mapping: Vec<crate::config::MappingEntry>,
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
    use crate::plan::{EntityRef, PlanNodeId, SecondaryMap};
    use clinker_record::SchemaBuilder;
    use std::sync::Arc;

    fn empty_dag() -> ExecutionPlanDag {
        ExecutionPlanDag {
            consumer_registry: CompiledConsumerRegistry::default(),
            order_contract: ExecutionOrderContract::default(),
            source_activation: SourceActivationPlan::default(),
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
            id_to_index: SecondaryMap::with_default(None),
        }
    }

    fn source_node(name: &str, id: usize) -> PlanNode {
        PlanNode::Source {
            name: name.to_string(),
            id: PlanNodeId::new(id),
            span: Span::SYNTHETIC,
            resolved: None,
            output_schema: SchemaBuilder::new().build(),
        }
    }

    fn composition_node(name: &str, id: usize) -> PlanNode {
        PlanNode::Composition {
            name: name.to_string(),
            id: PlanNodeId::new(id),
            span: Span::SYNTHETIC,
            body: CompositionBodyId::SENTINEL,
            output_schema: SharedStorage::from_arc(Arc::new(clinker_record::Schema::new(
                Vec::new(),
            ))),
        }
    }

    #[test]
    fn diagnose_silent_when_every_composition_edge_is_port_tagged() {
        let mut dag = empty_dag();
        let src = dag.graph.add_node(source_node("src", 0));
        let comp = dag.graph.add_node(composition_node("comp", 1));
        dag.graph.add_edge(
            src,
            comp,
            PlanEdge {
                dependency_type: DependencyType::Data,
                port: Some("p".to_string()),
                producer_port: None,
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
        let src = dag.graph.add_node(source_node("src", 0));
        let comp = dag.graph.add_node(composition_node("comp", 1));
        dag.graph.add_edge(
            src,
            comp,
            PlanEdge {
                dependency_type: DependencyType::Data,
                port: None,
                producer_port: None,
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
        let body_src = body_graph.add_node(source_node("body_src", 0));
        let body_comp = body_graph.add_node(composition_node("nested_comp", 1));
        body_graph.add_edge(
            body_src,
            body_comp,
            PlanEdge {
                dependency_type: DependencyType::Data,
                port: None,
                producer_port: None,
            },
        );
        let body = crate::plan::composition_body::BoundBody {
            body_scope: body_id.into(),
            signature_path: std::path::PathBuf::from("compositions/test.comp.yaml"),
            semantic_name: "test".to_string(),
            semantic_digest: [0; 32],
            resource_bindings: indexmap::IndexMap::new(),
            source_instances: Vec::new(),
            graph: body_graph,
            topo_order: Vec::new(),
            name_to_idx: HashMap::new(),
            port_name_to_node_idx: HashMap::new(),
            body_rows: HashMap::new(),
            node_input_refs: HashMap::new(),
            output_port_rows: indexmap::IndexMap::new(),
            output_port_to_node_idx: indexmap::IndexMap::new(),
            input_port_rows: indexmap::IndexMap::new(),
            nested_body_ids: Vec::new(),
            body_indices_to_build: Vec::new(),
            window_bindings: HashMap::new(),
            body_window_configs: HashMap::new(),
            deferred_regions: HashMap::new(),
            parent_continuations: HashMap::new(),
            id_to_index: SecondaryMap::with_default(None),
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

// Schema-less, row-preserving nodes (Route/Output/Sort/CorrelationCommit)
// carry no stored schema, so `output_schema_in`, `expected_input_schema_in`,
// and `cxl_emit_names_in` locate the node in its DAG and walk to its sole
// upstream. In a composition body an authored node can legally share a name
// with a synthesized input-port `Source` (which sits at a lower `NodeIndex`),
// so resolving by name would select the port and report the wrong schema.
// These tests pin resolution by stable `PlanNodeId`.
#[cfg(test)]
mod schema_resolution_by_id_tests {
    use super::*;
    use crate::plan::{EntityRef, PlanNodeId, SecondaryMap};
    use clinker_record::SchemaBuilder;

    fn source_named(name: &str, id: usize, field: &str) -> PlanNode {
        PlanNode::Source {
            name: name.to_string(),
            id: PlanNodeId::new(id),
            span: Span::SYNTHETIC,
            resolved: None,
            output_schema: SchemaBuilder::new().with_field(field).build(),
        }
    }

    fn sort_named(name: &str, id: usize) -> PlanNode {
        PlanNode::Sort {
            name: name.to_string(),
            id: PlanNodeId::new(id),
            span: Span::SYNTHETIC,
            sort_fields: Vec::new(),
        }
    }

    fn data_edge() -> PlanEdge {
        PlanEdge {
            dependency_type: DependencyType::Data,
            port: None,
            producer_port: None,
        }
    }

    fn dag_from(graph: DiGraph<PlanNode, PlanEdge>) -> ExecutionPlanDag {
        ExecutionPlanDag::from_parts(
            graph,
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            ParallelismProfile {
                per_transform: Vec::new(),
                worker_threads: 1,
            },
        )
    }

    fn names_of(schema: &clinker_record::Schema) -> Vec<String> {
        schema.columns().iter().map(|c| c.to_string()).collect()
    }

    /// Body-shaped collision: a port-`Source` named `collide` (inserted
    /// first, so lowest `NodeIndex`) + the real upstream + an authored `Sort`
    /// also named `collide` wired only to the real upstream. Returns the
    /// authored Sort's index.
    fn collision_dag() -> (ExecutionPlanDag, petgraph::graph::NodeIndex) {
        let mut graph = DiGraph::new();
        let _port = graph.add_node(source_named("collide", 0, "port_col"));
        let upstream = graph.add_node(source_named("real_upstream", 1, "x"));
        let sort = graph.add_node(sort_named("collide", 2));
        graph.add_edge(upstream, sort, data_edge());
        let mut dag = dag_from(graph);
        // Mimic a composition-body DAG: `from_body` leaves the id->index bridge
        // empty, so resolution falls through to the by-id graph scan — the path
        // that must defeat the port/authored-node name collision.
        dag.id_to_index = SecondaryMap::with_default(None);
        (dag, sort)
    }

    #[test]
    fn output_schema_in_resolves_colliding_node_by_id_not_name() {
        let (dag, sort) = collision_dag();
        // Must walk the authored Sort's real upstream (`x`), not the
        // same-named port-Source (which has no upstream, so a name match
        // would fall back to the empty body-root schema).
        assert_eq!(
            names_of(dag.graph[sort].output_schema_in(&dag)),
            vec!["x".to_string()],
            "output_schema_in must resolve the authored node, not the port-Source",
        );
    }

    #[test]
    fn expected_input_schema_in_resolves_colliding_node_by_id_not_name() {
        let (dag, sort) = collision_dag();
        let schema = dag.graph[sort]
            .expected_input_schema_in(&dag)
            .expect("the authored Sort has exactly one upstream");
        assert_eq!(names_of(schema), vec!["x".to_string()]);
    }

    #[test]
    fn cxl_emit_names_in_resolves_colliding_node_by_id_not_name() {
        let (dag, sort) = collision_dag();
        assert_eq!(
            dag.graph[sort].cxl_emit_names_in(&dag),
            vec!["x".to_string()],
            "cxl_emit_names_in must inherit the authored upstream's emit names",
        );
    }

    /// Control: without a name collision, resolution is unchanged.
    #[test]
    fn resolution_unchanged_without_name_collision() {
        let mut graph = DiGraph::new();
        let upstream = graph.add_node(source_named("up", 0, "x"));
        let sort = graph.add_node(sort_named("sorter", 1));
        graph.add_edge(upstream, sort, data_edge());
        let dag = dag_from(graph);
        assert_eq!(
            names_of(dag.graph[sort].output_schema_in(&dag)),
            vec!["x".to_string()],
        );
    }
}
