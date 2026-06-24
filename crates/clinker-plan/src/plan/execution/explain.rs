//! `--explain` rendering and JSON serialization for the execution plan DAG.
//!
//! Text/DOT/JSON views over [`ExecutionPlanDag`], plus the combine-aware
//! [`ExplainJson`] wrapper. Plan-only: no executor, no I/O.

use super::*;

use std::collections::HashMap;

use indexmap::IndexMap;
use petgraph::graph::NodeIndex;
use petgraph::visit::EdgeRef;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

use crate::config::{PipelineConfig, RouteMode};

/// Walk `config.nodes` in declaration order and yield each
/// expression-bearing node's `(name, cxl_source)` pair for the
/// `--explain` "CXL Expressions" section. Transform nodes contribute
/// their projection CXL, Aggregate nodes their reduction CXL, and Route
/// nodes an empty body (their predicates render in the plan block, not
/// here). Other node kinds carry no CXL source and are skipped.
fn transform_cxl_sources(config: &PipelineConfig) -> Vec<(String, String)> {
    use crate::config::PipelineNode;
    let mut out = Vec::new();
    for spanned in &config.nodes {
        match &spanned.value {
            PipelineNode::Transform { header, config } => {
                out.push((header.name.clone(), config.cxl.as_ref().to_string()));
            }
            PipelineNode::Aggregate { header, config } => {
                out.push((header.name.clone(), config.cxl.as_ref().to_string()));
            }
            PipelineNode::Route { header, .. } => {
                out.push((header.name.clone(), String::new()));
            }
            _ => {}
        }
    }
    out
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

/// Render the `=== Statistics ===` section: the planner-wide statistics
/// catalog's contents and the decision each figure informs.
///
/// Emits one line per node row count (with provenance) and one per column
/// that carries an exec-time sketch result (distinct count, heavy hitters,
/// membership). A row count tagged `file metadata` is the metadata-derived
/// estimate that drives the combine build/probe and partition-bit choices;
/// an `exec sketch` figure was measured during a run and supersedes it.
/// The section is omitted entirely when the catalog is empty, preserving
/// the honest-null floor — a plan with no sized sources adds no section.
fn render_statistics_section(
    out: &mut String,
    statistics: &crate::plan::statistics::StatisticsCatalog,
) {
    if statistics.is_empty() {
        return;
    }
    out.push_str("=== Statistics ===\n\n");

    let row_counts = statistics.row_counts_sorted();
    if !row_counts.is_empty() {
        out.push_str("Row counts:\n");
        for (node, rc) in row_counts {
            // A file-metadata row count is what the combine planner sizes
            // its build/probe and partition-bit choices against; an exec
            // sketch figure is the measured count that supersedes it.
            let informs = match rc.source {
                crate::plan::statistics::StatSource::FileMetadata
                | crate::plan::statistics::StatSource::SchemaHint => {
                    "informs combine build/probe + partition bits"
                }
                crate::plan::statistics::StatSource::ExecSketch => "measured during run",
            };
            out.push_str(&format!(
                "  {node}: ≈{} rows [{}] ({informs})\n",
                format_thousands(rc.rows),
                rc.source.label()
            ));
        }
    }

    let columns = statistics.columns_sorted();
    let has_column_stats = columns
        .iter()
        .any(|(_, s)| s.distinct.is_some() || s.heavy_hitters.is_some() || s.bloom.is_some());
    if has_column_stats {
        out.push('\n');
        out.push_str("Column sketches:\n");
        for (key, stats) in columns {
            if let Some((distinct, source)) = stats.distinct {
                out.push_str(&format!(
                    "  {}.{}: {} distinct [{}]\n",
                    key.node,
                    key.column,
                    format_thousands(distinct),
                    source.label()
                ));
            }
            if let Some(hitters) = &stats.heavy_hitters
                && !hitters.is_empty()
            {
                // Show the top few `value=count` pairs inline; the counts
                // are Misra-Gries lower bounds, so the line is the floor on
                // each listed key's frequency, never an exclusion of others.
                let preview = hitters
                    .iter()
                    .take(3)
                    .map(|(v, c)| format!("{v}={}", format_thousands(*c)))
                    .collect::<Vec<_>>()
                    .join(", ");
                let more = if hitters.len() > 3 {
                    format!(", +{} more", hitters.len() - 3)
                } else {
                    String::new()
                };
                out.push_str(&format!(
                    "  {}.{}: heavy hitters [exec sketch, lower bound]: {preview}{more}\n",
                    key.node, key.column,
                ));
            }
            if let Some(bloom) = &stats.bloom {
                let sizing = if bloom.sized_from_estimate {
                    "sized from estimate"
                } else {
                    "sized from exact count"
                };
                out.push_str(&format!(
                    "  {}.{}: membership filter, {} bits / {} probes [exec sketch, {sizing}]\n",
                    key.node, key.column, bloom.bit_count, bloom.hash_count
                ));
            }
        }
    }
    out.push('\n');
}

/// Render a `u64` with comma thousands separators (e.g. `1,000,000`),
/// manually formatted to avoid a dependency on `num-format` for one
/// call site.
fn format_thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}

/// Format a combine input's row count for `--explain`, tagging it with
/// the statistic's provenance — e.g. `1,000,000 [file metadata]`.
///
/// Returns the literal string `"null"` when no row count is known. Honest
/// output: a row count exists only for a source whose on-disk size could
/// be read (or one an exec sketch has measured), so most inputs render
/// `null` — matching DataFusion / Spark / DuckDB behavior when statistics
/// are absent.
fn format_estimated_rows(
    input: Option<&crate::plan::combine::CombineInput>,
    statistics: &crate::plan::statistics::StatisticsCatalog,
) -> String {
    match input.and_then(|ci| statistics.row_count_with_source(&ci.upstream_name)) {
        Some(rc) => format!("{} [{}]", format_thousands(rc.rows), rc.source.label()),
        None => "null".to_string(),
    }
}

/// Format a byte count with the largest binary-prefix unit that keeps
/// the magnitude under 1024. `64M` / `2G` style — matches the surface
/// syntax of `pipeline.memory.limit` so users see the value back in the
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
/// runtime executor uses one shared `MemoryArbitrator`; the rendering
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
/// threshold model on `MemoryArbitrator` (default 80%).
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

///
/// Mirrors the runtime distinction the executor's dispatch makes: nodes
/// whose output bypasses `ctx.node_buffers` (fused Source / Transform
/// chains and sink Outputs) charge no inter-stage memory against the
/// memory arbitrator; every other node materializes an intermediate
/// buffer and is spill-eligible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BufferClass {
    /// No `ctx.node_buffers` slot is admitted for this stage's output —
    /// no `MemoryArbitrator` charge for this stage, no spill eligibility.
    /// Today this covers fused Sources (their receiver is consumed
    /// directly by the downstream fused arm) and every Output (sinks
    /// write to their configured writer and never admit a buffer).
    Streaming,
    /// Records pass through a `ctx.node_buffers` slot between dispatch
    /// arms. Each admission registers a `NodeBufferConsumer` whose
    /// pull-mode `current_usage` reflects the slot's live footprint;
    /// a soft-threshold trip spills the slot to disk.
    Materialized,
}

impl BufferClass {
    fn label(self) -> &'static str {
        match self {
            Self::Streaming => "streaming",
            Self::Materialized => "materialized",
        }
    }
}

impl ExecutionPlanDag {
    fn classify_node_buffers(&self, config: &PipelineConfig) -> HashMap<NodeIndex, BufferClass> {
        // Delegate to the fusion classifier whose streaming verdict is
        // decided by the same `certify_streaming_edge` predicate the
        // runtime sender install consults, so the `--explain` annotation
        // can never drift from what the dispatcher actually streams.
        // `StreamClass` is the verdict; `BufferClass` is this surface's
        // render-time wrapper.
        crate::plan::execution::classify_stream_nodes(self, config)
            .into_iter()
            .map(|(idx, class)| {
                let buffer_class = match class {
                    crate::plan::execution::StreamClass::Streaming => BufferClass::Streaming,
                    crate::plan::execution::StreamClass::Materialized => BufferClass::Materialized,
                };
                (idx, buffer_class)
            })
            .collect()
    }

    /// Format the execution plan for `--explain` display.
    ///
    /// `buffer_classes` carries the `streaming` / `materialized`
    /// verdict per node so the **Physical Properties** section can
    /// append a `buffer:` line — the pre-runtime signal that pipeline
    /// authors use to see which stages will charge `MemoryArbitrator`.
    /// Build it with [`Self::classify_node_buffers`] when a
    /// `PipelineConfig` is in hand.
    fn explain(
        &self,
        buffer_classes: &HashMap<NodeIndex, BufferClass>,
        arbitration_policy_name: &str,
    ) -> String {
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
        out.push_str(&format!("DAG nodes: {}\n", self.graph.node_count()));
        // Pipeline-level arbitration policy. The active policy is the
        // one `pipeline.memory.backpressure` selects (default `pause`
        // → `BackPressurePreferred -> Priority`); rendering it here
        // lets pipeline authors see the spill/pause posture before
        // runtime.
        out.push_str(&format!("arbitration: {}\n\n", arbitration_policy_name));

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
                    "  partitioning_provenance: {:?}\n",
                    props.partitioning.provenance
                ));
                if let Some(class) = buffer_classes.get(&idx) {
                    out.push_str(&format!("  buffer: {}\n", class.label()));
                }
                // Per-node arbitration parameters, derived at plan time
                // from the runtime `MemoryConsumer` impls (see
                // `arbitration_class`). Lower `spill_priority` = elected
                // for spill first; `N/A` = the stage holds no spillable
                // state. The three `predicted_*` values are the scheduler's
                // inputs: `predicted_peak` is the live volume this node is
                // expected to hold at its peak, `predicted_freed` is what it
                // returns to the budget the instant it drains, and
                // `predicted_subtree_reclaim` is the largest reclaim its
                // whole downstream subtree eventually unlocks — the same
                // numbers `next_runnable` weighs (headroom-fit on peak, then
                // immediate-freed, then subtree-reclaim tiebreaks). All
                // render `0B` when no file-size seed reached this node.
                // Two-space indent so the buffer-class slicers (which stop at
                // the first non-indented line) keep reading the stanza.
                let ac = arbitration_class(node);
                out.push_str(&format!(
                    "  arbitration: spill_priority={}, can_back_pressure={}, \
                     predicted_peak={}, predicted_freed={}, predicted_subtree_reclaim={}\n",
                    ac.spill_priority
                        .map_or_else(|| "N/A".to_string(), |p| p.to_string()),
                    ac.can_back_pressure,
                    format_bytes(props.predicted_peak_bytes),
                    format_bytes(props.predicted_freed_bytes_on_complete),
                    format_bytes(props.predicted_subtree_reclaim_bytes),
                ));
                out.push('\n');
            }

            // Buffer-edge pseudo-nodes: one entry per `node_buffers` slot
            // between non-fused stages. The runtime keys `ctx.node_buffers`
            // by the producer's `NodeIndex`, so the slot number printed
            // here is that index — stable and identical to the slot the
            // executor admits into. Every slot registers a
            // `NodeBufferConsumer` (priority 0, the cheapest spill victim);
            // its `can_back_pressure` is the slot's own flag, today always
            // false at the admit site, so the producer-variant suffix is
            // informational context only — it does not flip the flag.
            let materialized_edges: Vec<(NodeIndex, NodeIndex)> = self
                .topo_order
                .iter()
                .filter(|&&idx| buffer_classes.get(&idx) == Some(&BufferClass::Materialized))
                .flat_map(|&producer| {
                    self.graph
                        .edges(producer)
                        .map(move |edge| (producer, edge.target()))
                })
                .collect();
            if !materialized_edges.is_empty() {
                out.push_str("=== Buffer Edges ===\n\n");
                for (producer, target) in materialized_edges {
                    let producer_node = &self.graph[producer];
                    let target_node = &self.graph[target];
                    out.push_str(&format!(
                        "edge {} -> {}:\n",
                        producer_node.id_slug(),
                        target_node.id_slug()
                    ));
                    out.push_str(&format!(
                        "  buffer: node_buffer (slot={})\n",
                        producer.index()
                    ));
                    // A node_buffer slot holds the producer's materialized
                    // output, so its predicted peak is the producer's own
                    // predicted volume; it frees that whole buffer once the
                    // consumer has drained it. Both render `0B` when the
                    // producer carried no volume seed.
                    let producer_peak = self
                        .node_properties
                        .get(&producer)
                        .map_or(0, |p| p.predicted_peak_bytes);
                    out.push_str(&format!(
                        "  arbitration: spill_priority=0, can_back_pressure=false, \
                         predicted_peak={}, predicted_freed={} (producer: {})\n",
                        format_bytes(producer_peak),
                        format_bytes(producer_peak),
                        producer_node.type_tag()
                    ));
                    out.push('\n');
                }
            }
        }

        // Estimated spill volume per blocking stage. Plan-only (reads
        // `predicted_peak_bytes` already computed by the volume-estimate pass),
        // so it renders on every `--explain` path. A streaming-only pipeline
        // has no spilling operator and adds nothing here.
        let spill_estimate = self.spill_estimate_explain();
        if !spill_estimate.is_empty() {
            out.push_str("=== Estimated Spill Volume ===\n\n");
            out.push_str(&spill_estimate);
            out.push('\n');
        }

        // Show route info from graph nodes. Each branch (and the default)
        // is an output port; the consumers it feeds are resolved from the
        // producer-port-tagged outgoing edges, so the rendering reflects
        // the live topology rather than assuming one consumer per port.
        for idx in self.graph.node_indices() {
            if let PlanNode::Route {
                name,
                mode,
                branches,
                default,
                ..
            } = &self.graph[idx]
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
                        "  Branch '{}' → {}\n",
                        branch_name,
                        self.render_route_port_targets(idx, branch_name),
                    ));
                }
                out.push_str(&format!(
                    "  Default: '{}' → {}\n\n",
                    default,
                    self.render_route_port_targets(idx, default),
                ));
            }
        }

        // Combine blocks render only when the statistics catalog is
        // threaded through (`explain_with_statistics` /
        // `explain_full_with_statistics`). The per-node combine metadata
        // is read off the node, but the row-estimate formatting needs the
        // catalog; without it the detail block degrades to the summary
        // line embedded in `display_name()`'s header. The memory-limit
        // argument is ignored when the catalog is `None`; pass `0` rather
        // than re-reading the default to avoid a config-coupling cycle here.
        self.render_combine_section(&mut out, None, 0);

        self.render_retraction_section(&mut out, None);

        out
    }

    /// Render the `--explain` target list for one Route output port.
    ///
    /// Walks the route node's outgoing edges, selecting those whose
    /// [`PlanEdge::producer_port`] equals `port`, and lists each target
    /// node's id slug. A port with no consumer renders `(unconsumed)` so
    /// an author who wired a branch to nothing sees it. Multiple consumers
    /// of one port render comma-separated in graph-edge order.
    fn render_route_port_targets(&self, route_idx: NodeIndex, port: &str) -> String {
        use petgraph::visit::EdgeRef;
        let targets: Vec<String> = self
            .graph
            .edges_directed(route_idx, petgraph::Direction::Outgoing)
            .filter(|e| e.weight().producer_port.as_deref() == Some(port))
            .map(|e| self.graph[e.target()].id_slug())
            .collect();
        if targets.is_empty() {
            "(unconsumed)".to_string()
        } else {
            targets.join(", ")
        }
    }

    /// Render the retraction-cost block for content-relaxed aggregates
    /// and buffer-mode windows.
    ///
    /// `config` is `Some` when the caller went through one of the
    /// statistics-aware entry points and can read the pipeline-level
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
    /// [`Self::explain`] except that combine nodes — when paired with the
    /// statistics catalog that backs this DAG's row estimates — render a
    /// full per-node block (strategy, inputs + roles, predicate
    /// decomposition detail, match/on-miss policy, planned-share memory
    /// budget). N-ary chains (`decomposed_from = Some(_)`) group under
    /// their original user-declared name with numbered step lines.
    pub fn explain_with_statistics(
        &self,
        config: &PipelineConfig,
        statistics: &crate::plan::statistics::StatisticsCatalog,
        total_memory_limit_bytes: u64,
    ) -> String {
        // Build the base block (everything except combines) by reusing
        // `explain()` and then overwriting the combine section with the
        // statistics-aware render. `explain()` calls
        // `render_combine_section(.., None, ..)` which is a no-op for
        // every combine node when the catalog is absent — so the output
        // of `explain()` already has no combine block to dedupe.
        let classes = self.classify_node_buffers(config);
        let policy_name = config.pipeline.memory.backpressure.policy_name();
        let mut out = self.explain(&classes, policy_name);
        self.render_combine_section(&mut out, Some(statistics), total_memory_limit_bytes);
        render_statistics_section(&mut out, statistics);
        out
    }

    /// Render every combine node's multi-line block into `out`.
    ///
    /// Walks `topo_order` and groups nodes by `decomposed_from` so an
    /// N-ary chain — N>2 user inputs, decomposed at plan time into a
    /// chain of binary combines that share the original user-declared
    /// name in `decomposed_from` — emits one group header followed by
    /// numbered `Step N:` lines. Singleton combines render their own
    /// block. Without the statistics catalog, the function returns
    /// immediately (row-estimate detail is unreachable; the header line on
    /// `display_name()` is the only signal).
    fn render_combine_section(
        &self,
        out: &mut String,
        statistics: Option<&crate::plan::statistics::StatisticsCatalog>,
        total_memory_limit_bytes: u64,
    ) {
        let Some(statistics) = statistics else {
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
                self.render_combine_group(out, group_name, indices, planned_share);
            } else {
                self.render_combine_single(out, indices[0], statistics, planned_share);
            }
        }
    }

    /// Render a singleton combine block (1:1 between user declaration
    /// and plan node). The full multi-line block: strategy, driving
    /// input + estimated rows, build inputs + roles, predicate
    /// summary + per-bucket detail (PostgreSQL 3-tier), match mode,
    /// on-miss policy, planned-share memory budget. Defensive paths
    /// handle a combine whose on-node `decomposed_predicate` is `None`
    /// (a body-less `match: collect` step, or E303 fired at compile
    /// time) by rendering the count summary only and skipping the detail
    /// buckets — mirrors `display_name()`'s `<unselected>` fallback for
    /// an unset driving input.
    fn render_combine_single(
        &self,
        out: &mut String,
        idx: NodeIndex,
        statistics: &crate::plan::statistics::StatisticsCatalog,
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
            combine_inputs,
            decomposed_predicate,
            ..
        } = &self.graph[idx]
        else {
            return;
        };

        // Per-input metadata + decomposed predicate are read off the node
        // (the runtime source), not the bare-name `CompileArtifacts` maps.
        let inputs_for_node = combine_inputs.as_ref();

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
        let drive_rows =
            format_estimated_rows(inputs_for_node.and_then(|m| m.get(drive_label)), statistics);
        out.push_str(&format!(
            "  Driving input: {drive_label} (probe, est. {drive_rows} rows)\n",
        ));

        for build_name in build_inputs {
            let role = describe_build_role(strategy, build_name);
            let rows = format_estimated_rows(
                inputs_for_node.and_then(|m| m.get(build_name.as_str())),
                statistics,
            );
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
        if let Some(decomposed) = decomposed_predicate {
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
                decomposed_predicate,
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

            // Per-step predicate detail under the step line, read off the
            // step node. The detail is indented one more level than a
            // singleton block because each step is already nested under the
            // group header.
            if let Some(decomposed) = decomposed_predicate {
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

    /// Like [`Self::explain_full`] but emits the statistics-aware
    /// per-combine multi-line block alongside the existing transform /
    /// sort / route blocks. Intended for the CLI `--explain` path
    /// where the caller already holds the [`crate::plan::CompiledPlan`]
    /// that produced this DAG and its statistics catalog.
    pub fn explain_full_with_statistics(
        &self,
        config: &PipelineConfig,
        statistics: &crate::plan::statistics::StatisticsCatalog,
    ) -> String {
        let total_limit =
            crate::config::utils::parse_memory_limit_bytes(config.pipeline.memory.limit.as_deref());
        let mut out = self.explain_with_statistics(config, statistics, total_limit);
        // Re-render the retraction section with the pipeline config in
        // scope so the fanout-policy line resolves to the user-visible
        // setting. `explain()` (called inside `explain_with_statistics`)
        // already emitted a config-less variant; strip it before
        // re-rendering to avoid duplicate blocks.
        if let Some(start) = out.find("=== Retraction ===\n") {
            out.truncate(start);
        }
        self.render_retraction_section(&mut out, Some(config));
        self.append_full_sections(&mut out, config);
        out
    }

    /// Like [`Self::explain_text`] but routes through the statistics-
    /// aware text formatter so combine blocks render with full per-
    /// node detail.
    pub fn explain_text_with_statistics(
        &self,
        config: &PipelineConfig,
        statistics: &crate::plan::statistics::StatisticsCatalog,
    ) -> String {
        let mut out = self.explain_full_with_statistics(config, statistics);
        self.append_topology_section(&mut out);
        out
    }

    /// Render the per-blocking-stage estimated spill volume for `--explain`.
    ///
    /// One line per spill-writing operator (external sort, hash Aggregate,
    /// grace-hash / sort-merge Combine) giving its plan-time
    /// `predicted_peak_bytes` estimate, followed by a total. A stage whose
    /// volume is unknown at plan time (no on-disk file-size seed reached it —
    /// a `glob`/`regex` multi-file source, a network source, or a
    /// missing/unreadable input) renders `unknown` instead of a misleading
    /// `0B`, and the total notes that unknown stages are excluded. Returns an
    /// empty string when the plan has no spill-eligible operator, so a
    /// streaming-only pipeline adds no section.
    ///
    /// Bytes render in binary units (`G`/`M`/`K` = GiB/MiB/KiB), matching the
    /// Physical Properties `predicted_peak` line these figures derive from.
    pub fn spill_estimate_explain(&self) -> String {
        let estimates = self.per_stage_spill_estimates();
        if estimates.is_empty() {
            return String::new();
        }
        let mut out = String::from("Estimated spill volume (per blocking stage):\n");
        let mut total: u64 = 0;
        let mut any_unknown = false;
        for est in &estimates {
            if est.estimate_bytes == 0 {
                any_unknown = true;
                out.push_str(&format!("  {} → unknown\n", est.display_name));
            } else {
                total = total.saturating_add(est.estimate_bytes);
                out.push_str(&format!(
                    "  {} → {}\n",
                    est.display_name,
                    format_bytes(est.estimate_bytes)
                ));
            }
        }
        if any_unknown {
            out.push_str(&format!(
                "  Total (known stages): {} (excludes stages whose volume is unknown at plan time — \
                 a network source, a missing or unreadable input, or a glob/regex matcher whose \
                 discovery fails)\n",
                format_bytes(total),
            ));
        } else {
            out.push_str(&format!("  Total: {}\n", format_bytes(total)));
        }
        out
    }

    /// Column count the spill-compression decision resolves against for the
    /// node at `idx` — the same width the runtime spill writer sees.
    ///
    /// Returns the node's *effective* output-schema column count via
    /// [`PlanNode::output_schema_in`], which for a row-preserving operator
    /// (an enforcer [`PlanNode::Sort`], whose own `stored_output_schema` is
    /// `None`) walks to its sole upstream and reports that schema's width.
    /// This is exactly the width the runtime sort buffer reads off its first
    /// input record (`sort_dispatch`), so the `--explain` projection and the
    /// on-disk spill file agree on the column count the `auto` heuristic
    /// weighs. Hash Aggregate and grace-hash / sort-merge Combine carry a
    /// stored output schema, so `output_schema_in` returns it directly — the
    /// same `Arc<Schema>` their runtime dispatch arms resolve compression
    /// against.
    fn spill_decision_column_count(&self, idx: NodeIndex) -> usize {
        self.graph[idx].output_schema_in(self).column_count()
    }

    /// Render the per-blocking-operator spill-compression decision for the
    /// `--explain` text output.
    ///
    /// Each operator that actually writes spill files (hash Aggregate,
    /// external sort, grace-hash / sort-merge Combine — see
    /// [`writes_spill_files`]) gets one line resolving `compress` against the
    /// column count its runtime spill writer sees
    /// ([`Self::spill_decision_column_count`]) and the run's `batch_size`, so
    /// the projected mode equals the on-disk format the run produces. In-memory
    /// join strategies (inline hash build/probe, IEJoin) carry a
    /// `spill_priority` for memory arbitration but never open a spill writer,
    /// so they are excluded — spill compression has no meaning for state that
    /// never reaches disk. Under [`CompressMode::Auto`] the decision varies
    /// per operator because a wide operator clears the per-batch byte
    /// threshold a narrow one does not; `On` / `Off` report the same forced
    /// choice for every operator. The header line names the configured mode so
    /// an operator can confirm both the policy and its resolved effect before
    /// a run. Returns an empty string when the plan has no spill-writing
    /// operator.
    pub fn spill_compression_explain(
        &self,
        compress: crate::config::CompressMode,
        batch_size: usize,
    ) -> String {
        let blocking: Vec<NodeIndex> = self
            .topo_order
            .iter()
            .copied()
            .filter(|&idx| writes_spill_files(&self.graph[idx]))
            .collect();
        if blocking.is_empty() {
            return String::new();
        }
        let mut out = format!("Spill compression: {compress:?} [storage.spill.compress]\n");
        for idx in blocking {
            let node = &self.graph[idx];
            let column_count = self.spill_decision_column_count(idx);
            let chosen = if compress.resolve_for_schema(column_count, batch_size as u64) {
                "lz4"
            } else {
                "off"
            };
            out.push_str(&format!("  {} → {chosen}\n", node.display_name()));
        }
        out
    }

    /// Structured per-stage spill estimate for the JSON `--explain` output.
    ///
    /// Mirrors [`Self::spill_estimate_explain`] field-for-field — the same
    /// spill-writing operators, the same `unknown`-vs-bytes distinction
    /// (rendered here as `None` vs `Some`), the same known-stage total — so
    /// the JSON and text storage summaries cannot drift.
    pub fn estimated_spill_json(&self) -> EstimatedSpillJson {
        let mut per_stage = Vec::new();
        let mut total_known_bytes: u64 = 0;
        let mut any_unknown = false;
        for est in self.per_stage_spill_estimates() {
            let estimate_bytes = if est.estimate_bytes == 0 {
                any_unknown = true;
                None
            } else {
                total_known_bytes = total_known_bytes.saturating_add(est.estimate_bytes);
                Some(est.estimate_bytes)
            };
            per_stage.push(StageSpillEstimateJson {
                node_name: est.node_name,
                display_name: est.display_name,
                estimate_bytes,
            });
        }
        EstimatedSpillJson {
            per_stage,
            total_known_bytes,
            any_unknown,
        }
    }

    /// Structured per-operator spill-compression decision for the JSON
    /// `--explain` output.
    ///
    /// Mirrors [`Self::spill_compression_explain`]: the same spill-writing
    /// operators (in-memory joins excluded), the same per-operator `lz4` /
    /// `off` resolution against the runtime spill writer's column count
    /// ([`Self::spill_decision_column_count`]) and `batch_size`, under the
    /// same configured mode.
    pub fn spill_compression_json(
        &self,
        compress: crate::config::CompressMode,
        batch_size: usize,
    ) -> SpillCompressionJson {
        let per_operator = self
            .topo_order
            .iter()
            .copied()
            .filter(|&idx| writes_spill_files(&self.graph[idx]))
            .map(|idx| {
                let node = &self.graph[idx];
                let column_count = self.spill_decision_column_count(idx);
                let compression = if compress.resolve_for_schema(column_count, batch_size as u64) {
                    "lz4"
                } else {
                    "off"
                };
                OperatorCompressionJson {
                    node_name: node.name().to_string(),
                    display_name: node.display_name(),
                    compression: compression.to_string(),
                }
            })
            .collect();
        SpillCompressionJson {
            mode: compress.json_label().to_string(),
            per_operator,
        }
    }

    /// Append the `CXL Expressions`, `Type Annotations`, `Memory Budget`
    /// trailing sections that `explain_full` adds onto a base
    /// `explain()` body. Factored so the statistics-aware variant can
    /// reuse them.
    fn append_full_sections(&self, out: &mut String, config: &PipelineConfig) {
        // CXL AST (reformatted expressions from config)
        out.push_str("=== CXL Expressions ===\n\n");
        for (name, cxl) in transform_cxl_sources(config) {
            out.push_str(&format!("Transform '{}':\n", name));
            for line in cxl.lines() {
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
        let limit_status = config
            .pipeline
            .concurrency
            .as_ref()
            .and_then(|c| c.threads)
            .map(|_| "configured")
            .unwrap_or("default");
        out.push_str(&format!("Memory limit: {}\n", limit_status));
        out.push_str(&format!(
            "Worker threads: {}\n",
            self.parallelism.worker_threads
        ));
        out.push('\n');
    }

    /// Append the `DAG Topology` section that `explain_text` adds onto
    /// a base `explain_full` body. Factored so the statistics-aware
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
                PlanNode::Reshape { .. } => {
                    out.push_str(&format!("  ◇ {}{line_suffix}\n", node.display_name()));
                }
                PlanNode::Cull { config, name, .. } => {
                    // Fork rendering: Cull is a two-port producer like Route.
                    // Show the main and side-output ports so the two-output
                    // topology is visible in the plan.
                    out.push_str(&format!("  ◆ FORK [cull] '{name}'{line_suffix}\n"));
                    out.push_str(&format!("  ├──> main → {name}\n"));
                    out.push_str(&format!(
                        "  ├──> {removed} → {name}.{removed}\n",
                        removed = config.removed_to
                    ));
                }
                PlanNode::Source { .. }
                | PlanNode::Transform { .. }
                | PlanNode::Envelope { .. }
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
        let classes = self.classify_node_buffers(config);
        let policy_name = config.pipeline.memory.backpressure.policy_name();
        let mut out = self.explain(&classes, policy_name);
        if let Some(start) = out.find("=== Retraction ===\n") {
            out.truncate(start);
        }
        self.render_retraction_section(&mut out, Some(config));

        // CXL AST (reformatted expressions from config)
        out.push_str("=== CXL Expressions ===\n\n");
        for (name, cxl) in transform_cxl_sources(config) {
            out.push_str(&format!("Transform '{}':\n", name));
            for line in cxl.lines() {
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
        let limit_status = config
            .pipeline
            .concurrency
            .as_ref()
            .and_then(|c| c.threads)
            .map(|_| "configured")
            .unwrap_or("default");
        out.push_str(&format!("Memory limit: {}\n", limit_status));
        out.push_str(&format!(
            "Worker threads: {}\n",
            self.parallelism.worker_threads
        ));
        out.push('\n');

        out
    }

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
                PlanNode::Reshape { .. } => {
                    // Sibling rendering: Reshape is a grouping operator;
                    // it shares the `◇` glyph family with Aggregate.
                    out.push_str(&format!("  ◇ {}{line_suffix}\n", node.display_name()));
                }
                PlanNode::Cull { config, name, .. } => {
                    // Fork rendering: Cull is a two-port producer like Route,
                    // so it gets the `◆ FORK` glyph and one line per output
                    // port (main + the `removed_to` side output).
                    out.push_str(&format!("  ◆ FORK [cull] '{name}'{line_suffix}\n"));
                    out.push_str(&format!("  ├──> main → {name}\n"));
                    out.push_str(&format!(
                        "  ├──> {removed} → {name}.{removed}\n",
                        removed = config.removed_to
                    ));
                }
                PlanNode::Source { .. }
                | PlanNode::Transform { .. }
                | PlanNode::Envelope { .. }
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
                &|_, edge| {
                    // Append the producer output port (Route branch /
                    // default) to the dependency-type label so the graph
                    // shows which branch each edge carries.
                    let dep = edge.weight().dependency_type.as_str();
                    match edge.weight().producer_port.as_deref() {
                        Some(port) => format!(r#"label="{}:{}""#, dep, dot_escape(port)),
                        None => format!(r#"label="{}""#, dep),
                    }
                },
                &|_, (_, node)| { format!(r#"label="{}""#, dot_escape(&node.display_name())) },
            )
        )
    }
}

/// Render a [`PlanIndexRoot`] for `--explain` output. Source-rooted
/// Node-rooted indices show as `node(<idx>)`, parent-node-rooted as
/// `parent_node(<idx>)`. Windows whose upstream is a Source render
/// as `node(<source_node_idx>)` — there is no separate
/// source-rooted category at runtime.
fn format_index_root(root: &crate::plan::index::PlanIndexRoot) -> String {
    match root {
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
/// for downstream consumers (debuggers and third-party tooling)
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
/// (no `Deserialize`) — the JSON channel is consumer-only (debuggers,
/// third-party tooling).
///
/// Keeping the wrapper out of `ExecutionPlanDag` itself preserves the
/// existing `serde_json::to_value(&dag)` / `to_string_pretty(&dag)`
/// callers (which neither receive nor want the statistics catalog)
/// without bumping the JSON `schema_version`.
pub struct ExplainJson<'a> {
    dag: &'a ExecutionPlanDag,
    statistics: &'a crate::plan::statistics::StatisticsCatalog,
    /// Storage observability at parity with the text `--explain` output:
    /// per-stage spill estimates, the resolved spill root / disk cap, the
    /// per-operator compression decision, cap headroom, and the staging
    /// plan. `None` when the JSON view is built without storage context
    /// (e.g. the field-provenance path), in which case the
    /// `storage_summary` key is omitted entirely rather than emitted as
    /// `null`.
    storage_summary: Option<StorageSummaryJson>,
}

impl<'a> ExplainJson<'a> {
    /// Build a statistics-aware JSON view of an execution plan.
    pub fn new(
        dag: &'a ExecutionPlanDag,
        statistics: &'a crate::plan::statistics::StatisticsCatalog,
    ) -> Self {
        Self {
            dag,
            statistics,
            storage_summary: None,
        }
    }

    /// Attach the storage observability summary so the JSON `--explain`
    /// output carries the same per-stage spill estimates, spill root /
    /// disk cap, compression decision, cap headroom, and staging plan the
    /// text output renders. The CLI assembles the summary because it holds
    /// the resolved storage config, spill root, batch size, and staging
    /// policy the plan layer does not.
    pub fn with_storage_summary(mut self, summary: StorageSummaryJson) -> Self {
        self.storage_summary = Some(summary);
        self
    }
}

/// Structured storage observability for the JSON `--explain` output, at
/// parity with the text path's per-stage spill estimate, spill root,
/// spill disk cap, spill compression, cap headroom, and staging plan.
///
/// Every field is structured rather than a stringified blob so downstream
/// tooling and third-party consumers can read per-stage spill
/// estimates and the cap / staging summary without re-parsing prose. The
/// CLI assembles it from [`ExecutionPlanDag::estimated_spill_json`] and
/// [`ExecutionPlanDag::spill_compression_json`] (the plan-derivable parts)
/// plus the CLI-resolved spill root / cap / staging
/// fields.
#[derive(Debug, Clone, Serialize)]
pub struct StorageSummaryJson {
    /// Resolved spill root: where the per-run spill directory is created.
    pub spill_root: SpillRootJson,
    /// Resolved cumulative on-disk spill cap in bytes; `None` is unlimited
    /// (`storage.spill.disk_cap_bytes` unset).
    pub spill_disk_cap_bytes: Option<u64>,
    /// Per-blocking-stage estimated spill volume, plus a total over the
    /// stages whose volume is known at plan time.
    pub estimated_spill: EstimatedSpillJson,
    /// Per-spill-writing-operator compression decision under the resolved
    /// `storage.spill.compress` mode.
    pub spill_compression: SpillCompressionJson,
    /// Cap headroom: cap minus estimated spill volume. `None` when no cap
    /// is configured or the estimate is unknown (`0`) — mirrors the text
    /// path, which omits the line in those cases.
    pub cap_headroom: Option<CapHeadroomJson>,
    /// Per-source staging plan: whether each source (or each discovered
    /// file) would be staged, the resolved staged path, and the
    /// reuse-if-fresh decision.
    pub staging: StagingPlanJson,
}

/// Resolved spill root for the JSON storage summary.
#[derive(Debug, Clone, Serialize)]
pub struct SpillRootJson {
    /// The directory under which the per-run spill directory is created.
    pub path: String,
    /// Where the value came from: `storage.spill.dir` or the OS temp-dir
    /// default.
    pub source: String,
}

/// Per-stage estimated spill volume for the JSON storage summary.
#[derive(Debug, Clone, Serialize)]
pub struct EstimatedSpillJson {
    /// One entry per spill-writing blocking stage, in topological order.
    pub per_stage: Vec<StageSpillEstimateJson>,
    /// Sum over stages whose estimate is known; excludes `unknown` stages.
    pub total_known_bytes: u64,
    /// `true` when at least one stage's volume is unknown at plan time, so
    /// the total understates the real footprint.
    pub any_unknown: bool,
}

/// One blocking stage's spill estimate for the JSON storage summary.
/// `estimate_bytes` is `None` when the stage's volume is unknown at plan
/// time (no on-disk file-size seed reached it), matching the text path's
/// `unknown` rendering rather than a misleading `0`.
#[derive(Debug, Clone, Serialize)]
pub struct StageSpillEstimateJson {
    pub node_name: String,
    pub display_name: String,
    pub estimate_bytes: Option<u64>,
}

/// Per-operator spill-compression decision for the JSON storage summary.
#[derive(Debug, Clone, Serialize)]
pub struct SpillCompressionJson {
    /// The configured `storage.spill.compress` mode (`auto` / `on` / `off`).
    pub mode: String,
    /// One entry per spill-writing operator, in topological order.
    pub per_operator: Vec<OperatorCompressionJson>,
}

/// One operator's resolved spill-compression choice (`lz4` / `off`).
#[derive(Debug, Clone, Serialize)]
pub struct OperatorCompressionJson {
    pub node_name: String,
    pub display_name: String,
    pub compression: String,
}

/// Cap-headroom figures for the JSON storage summary.
#[derive(Debug, Clone, Serialize)]
pub struct CapHeadroomJson {
    /// Cap minus estimated spill volume, in bytes.
    pub headroom_bytes: u64,
    /// The run's estimated spill volume, in bytes.
    pub estimated_bytes: u64,
    /// The configured disk cap, in bytes.
    pub cap_bytes: u64,
    /// Estimated volume as a percentage of the cap.
    pub pct_of_cap: f64,
    /// `true` when the estimate is at or above 80% of the cap, so a real
    /// run may abort with a spill-cap error (E320).
    pub over_threshold: bool,
}

/// Per-source staging plan for the JSON storage summary.
#[derive(Debug, Clone, Serialize)]
pub struct StagingPlanJson {
    /// Whether source staging is enabled. When `false`, `sources` is empty
    /// and every source reads in place.
    pub enabled: bool,
    /// One entry per source, in declaration order.
    pub sources: Vec<StagingSourceJson>,
}

/// One source's staging plan for the JSON storage summary.
#[derive(Debug, Clone, Serialize)]
pub struct StagingSourceJson {
    pub name: String,
    /// `false` for network sources (not stagable; they read in place).
    pub stagable: bool,
    /// Per-file staging decision; empty for an unstagable source or one
    /// whose matcher resolved to no files.
    pub files: Vec<StagingFileJson>,
    /// Present when matcher resolution failed during the explain; mirrors
    /// the text path's inline `(discovery failed: …)` note.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub discovery_error: Option<String>,
}

/// One staged file's decision for the JSON storage summary.
#[derive(Debug, Clone, Serialize)]
pub struct StagingFileJson {
    /// The matched source file path.
    pub source_path: String,
    /// `true` when the file matches a staging pattern and would be copied.
    pub staged: bool,
    /// The resolved staged path; `None` when the file reads in place.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub staged_path: Option<String>,
    /// The reuse-if-fresh decision label (`hit` / `miss`); `None` when the
    /// file is not staged.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reuse: Option<String>,
}

impl<'a> Serialize for ExplainJson<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // schema_version + nodes + node_properties, plus storage_summary
        // when the CLI threaded the storage context through.
        let entry_count = 3 + usize::from(self.storage_summary.is_some());
        let mut map = serializer.serialize_map(Some(entry_count))?;
        map.serialize_entry("schema_version", "1")?;

        // Combine-aware node list. Walks `topo_order` and emits a
        // `CombineNodeEntry` for combine variants (whose row estimates
        // read the statistics catalog), falling back to the plain
        // `NodeEntry` for everything else.
        struct EnrichedNodeList<'a> {
            dag: &'a ExecutionPlanDag,
            statistics: &'a crate::plan::statistics::StatisticsCatalog,
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
                            statistics: self.statistics,
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
        let total_memory_limit = crate::config::utils::parse_memory_limit_bytes(None);

        map.serialize_entry(
            "nodes",
            &EnrichedNodeList {
                dag: self.dag,
                statistics: self.statistics,
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

        // Storage observability at parity with the text `--explain`
        // output. Omitted (not `null`) when the JSON view was built
        // without storage context.
        if let Some(summary) = &self.storage_summary {
            map.serialize_entry("storage_summary", summary)?;
        }
        map.end()
    }
}

/// Per-Combine-node JSON entry carrying the node-derived predicate
/// detail (`equalities[].left/.right`, `ranges[]`,
/// `has_residual`), per-input role + cardinality, and the
/// planned-share `memory_budget_bytes`. Flattens the underlying
/// `PlanNode::Combine` shape to preserve every field the plain
/// `ExecutionPlanDag` Serialize emits (strategy, driving_input,
/// build_inputs, predicate_summary, match_mode, on_miss,
/// decomposed_from) — extension only, no replacement.
struct CombineNodeEntry<'a> {
    node: &'a PlanNode,
    depends_on: &'a Vec<String>,
    statistics: &'a crate::plan::statistics::StatisticsCatalog,
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

        // Combine-specific extras. The per-input metadata and decomposed
        // predicate come off the node (the runtime source); only the
        // row-estimate figures consult the statistics catalog.
        if let PlanNode::Combine {
            strategy,
            driving_input,
            build_inputs,
            combine_inputs,
            decomposed_predicate,
            ..
        } = self.node
        {
            // Rich predicate detail.
            let predicate_value = build_predicate_json(decomposed_predicate.as_ref());
            obj.insert("predicate".to_string(), predicate_value);

            // Per-input role + estimated rows.
            let inputs_value = build_inputs_json(
                combine_inputs.as_ref(),
                self.statistics,
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
/// `{ <qualifier>: { role, estimated_rows, estimated_rows_source } }`.
/// `role` is `"probe"` for the driver, `"build"` (or `"build (sorted
/// scan)"` for sort-merge) for the rest. `estimated_rows` is JSON `null`
/// when no row count is known, with `estimated_rows_source` carrying the
/// statistic's provenance label when present — honest output matching
/// every surveyed engine's behavior in the absence of statistics.
fn build_inputs_json(
    inputs_for_node: Option<&IndexMap<String, crate::plan::combine::CombineInput>>,
    statistics: &crate::plan::statistics::StatisticsCatalog,
    driving_input: &str,
    build_inputs: &[String],
    strategy: &crate::plan::combine::CombineStrategy,
) -> serde_json::Value {
    use serde_json::{Map, Value};
    let mut obj = Map::new();

    let mut emit_one = |name: &str, role: &str| {
        let rc = inputs_for_node
            .and_then(|m| m.get(name))
            .and_then(|ci| statistics.row_count_with_source(&ci.upstream_name));
        let (est_value, source_value) = match rc {
            Some(rc) => (
                Value::Number(rc.rows.into()),
                Value::String(rc.source.label().to_string()),
            ),
            None => (Value::Null, Value::Null),
        };
        let mut entry = Map::new();
        entry.insert("role".to_string(), Value::String(role.to_string()));
        entry.insert("estimated_rows".to_string(), est_value);
        entry.insert("estimated_rows_source".to_string(), source_value);
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

#[cfg(test)]
mod spill_projection_tests {
    use super::*;
    use crate::plan::combine::{CombinePredicateSummary, CombineStrategy};
    use crate::plan::properties::NodeProperties;
    use crate::plan::{EntityRef, PlanNodeId};
    use petgraph::graph::DiGraph;
    use std::sync::Arc;

    fn empty_dag() -> ExecutionPlanDag {
        ExecutionPlanDag::from_parts(
            DiGraph::new(),
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

    fn combine_node(name: &str, id: usize, strategy: CombineStrategy) -> PlanNode {
        PlanNode::Combine {
            name: name.to_string(),
            id: PlanNodeId::new(id),
            span: Span::SYNTHETIC,
            strategy,
            driving_input: "a".to_string(),
            build_inputs: vec!["b".to_string()],
            driving_upstream: None,
            predicate_summary: CombinePredicateSummary::default(),
            match_mode: crate::config::pipeline_node::MatchMode::First,
            on_miss: crate::config::pipeline_node::OnMiss::NullFields,
            propagate_ck: crate::config::pipeline_node::PropagateCkSpec::Driver,
            decomposed_from: None,
            output_schema: Arc::new(clinker_record::Schema::new(Vec::new())),
            resolved_column_map: Arc::new(std::collections::HashMap::new()),
            typed: None,
            combine_inputs: None,
            decomposed_predicate: None,
            combine_driving: None,
        }
    }

    fn sort_node(name: &str, id: usize) -> PlanNode {
        PlanNode::Sort {
            name: name.to_string(),
            id: PlanNodeId::new(id),
            span: Span::SYNTHETIC,
            sort_fields: Vec::new(),
        }
    }

    /// A Source carrying an explicit output schema of `column_names`. The
    /// schema is the exact `Arc` every record this Source emits carries, so a
    /// downstream node's runtime input-record width equals this column count.
    fn source_node(name: &str, id: usize, column_names: &[&str]) -> PlanNode {
        let schema = clinker_record::Schema::new(
            column_names.iter().map(|c| Box::<str>::from(*c)).collect(),
        );
        PlanNode::Source {
            name: name.to_string(),
            id: PlanNodeId::new(id),
            span: Span::SYNTHETIC,
            resolved: None,
            output_schema: Arc::new(schema),
        }
    }

    /// Push a node onto the DAG, register it in `topo_order`, and seed a
    /// `predicted_peak_bytes` so the per-stage estimate has a non-zero
    /// figure to report for the spill-writing variants.
    fn add_node(dag: &mut ExecutionPlanDag, node: PlanNode, peak_bytes: u64) {
        let idx = dag.graph.add_node(node);
        dag.topo_order.push(idx);
        let mut props = NodeProperties::unordered_single();
        props.predicted_peak_bytes = peak_bytes;
        dag.node_properties.insert(idx, props);
    }

    /// The `--explain` spill-compression projection lists only operators
    /// backed by a real spill writer. In-memory join strategies
    /// (`HashBuildProbe`, `IEJoin`, `HashPartitionIEJoin`) carry a
    /// `spill_priority` for memory arbitration but never open a spill file,
    /// so they must not appear; the genuinely-spilling external sort and
    /// grace-hash / sort-merge Combine must.
    #[test]
    fn spill_compression_lists_only_real_spill_writers() {
        let mut dag = empty_dag();
        add_node(
            &mut dag,
            combine_node("inline_join", 0, CombineStrategy::HashBuildProbe),
            1_000,
        );
        add_node(
            &mut dag,
            combine_node("range_join", 1, CombineStrategy::IEJoin),
            1_000,
        );
        add_node(
            &mut dag,
            combine_node(
                "hashpart_join",
                2,
                CombineStrategy::HashPartitionIEJoin { partition_bits: 4 },
            ),
            1_000,
        );
        add_node(
            &mut dag,
            combine_node(
                "grace_join",
                3,
                CombineStrategy::GraceHash { partition_bits: 4 },
            ),
            1_000,
        );
        add_node(
            &mut dag,
            combine_node("merge_join", 4, CombineStrategy::SortMerge),
            1_000,
        );
        add_node(&mut dag, sort_node("external_sort", 5), 1_000);

        let out = dag.spill_compression_explain(crate::config::CompressMode::Auto, 1_024);

        // In-memory joins are excluded: their state never reaches disk.
        assert!(
            !out.contains("inline_join"),
            "HashBuildProbe must not appear in the spill-compression projection:\n{out}"
        );
        assert!(
            !out.contains("range_join"),
            "IEJoin must not appear in the spill-compression projection:\n{out}"
        );
        assert!(
            !out.contains("hashpart_join"),
            "HashPartitionIEJoin must not appear in the spill-compression projection:\n{out}"
        );

        // Genuine spill writers are included.
        assert!(
            out.contains("grace_join"),
            "grace-hash Combine must appear in the spill-compression projection:\n{out}"
        );
        assert!(
            out.contains("merge_join"),
            "sort-merge Combine must appear in the spill-compression projection:\n{out}"
        );
        assert!(
            out.contains("external_sort"),
            "external sort must appear in the spill-compression projection:\n{out}"
        );
    }

    /// The per-stage estimated-spill-volume projection applies the same
    /// spill-writer predicate, so an in-memory join contributes nothing to
    /// the disk estimate while a real spiller does.
    #[test]
    fn per_stage_estimate_excludes_in_memory_joins() {
        let mut dag = empty_dag();
        add_node(
            &mut dag,
            combine_node("inline_join", 0, CombineStrategy::HashBuildProbe),
            5_000,
        );
        add_node(
            &mut dag,
            combine_node(
                "grace_join",
                1,
                CombineStrategy::GraceHash { partition_bits: 4 },
            ),
            7_000,
        );

        let estimates = dag.per_stage_spill_estimates();
        let names: Vec<&str> = estimates.iter().map(|e| e.node_name.as_str()).collect();
        assert_eq!(
            names,
            vec!["grace_join"],
            "only the grace-hash Combine writes spill files"
        );
        // The disk-volume total counts the real spiller only.
        assert_eq!(dag.estimated_spill_bytes(), 7_000);
    }

    /// The spill-compression projection resolves an enforcer `Sort`'s column
    /// count against the records that actually flow into it — its upstream's
    /// output-schema width — not the Sort's own `stored_output_schema`, which
    /// is `None` for a row-preserving operator.
    ///
    /// At runtime `sort_dispatch` reads the column count off its first input
    /// record's schema (the upstream's emitted schema), so the basis the
    /// `--explain` projection prints must equal that width or the reported
    /// compression mode can disagree with the on-disk spill file. A Source
    /// emits records widened with engine-stamped tail columns, so a five-wide
    /// upstream makes the Sort's runtime spilled-batch five columns wide —
    /// `spill_decision_column_count` must report exactly that, never `0`.
    #[test]
    fn sort_spill_column_count_matches_runtime_input_width() {
        let mut dag = empty_dag();
        // Five-wide upstream: three declared columns plus two engine-stamped
        // tail columns, the shape ingest hands every record at runtime.
        let upstream_columns = ["sku", "price", "qty", "$source.file", "$source.name"];
        let src_idx = dag
            .graph
            .add_node(source_node("orders", 0, &upstream_columns));
        let sort_idx = dag.graph.add_node(sort_node("__sort__orders", 1));
        dag.graph.add_edge(
            src_idx,
            sort_idx,
            PlanEdge {
                dependency_type: DependencyType::Data,
                port: None,
                producer_port: None,
            },
        );
        dag.topo_order = vec![src_idx, sort_idx];
        dag.node_properties
            .insert(src_idx, NodeProperties::unordered_single());
        dag.node_properties
            .insert(sort_idx, NodeProperties::unordered_single());

        // The width the runtime sort buffer reads off its first input record
        // is exactly the upstream Source's emitted-schema width.
        let runtime_input_width = dag.graph[src_idx]
            .stored_output_schema()
            .expect("a Source carries an output schema")
            .column_count();
        assert_eq!(runtime_input_width, upstream_columns.len());

        // `--explain` must resolve the Sort's compression decision against the
        // same width — not the Sort's own `stored_output_schema` (which is
        // `None`, and would otherwise collapse the projected width to `0`).
        assert!(dag.graph[sort_idx].stored_output_schema().is_none());
        assert_eq!(
            dag.spill_decision_column_count(sort_idx),
            runtime_input_width,
            "the --explain Sort column count must equal the runtime spilled-batch width"
        );
    }

    /// The Statistics section renders all three exec-sketch results — a
    /// distinct count, a top-k heavy-hitter list with counts, and a
    /// membership-filter summary — each tagged with its `exec sketch`
    /// provenance, exactly as a catalog populated during a grace-hash run
    /// would surface them.
    #[test]
    fn statistics_section_renders_all_three_sketch_classes() {
        use crate::plan::statistics::{BloomSummary, StatKey, StatisticsCatalog};
        use clinker_record::Value;

        let mut catalog = StatisticsCatalog::new();
        catalog.seed_row_count_from_bytes("orders", Some(1024 * 1200));
        let key = StatKey::new("orders", "product_id");
        catalog.record_distinct(key.clone(), 12_431);
        catalog.record_heavy_hitters(
            key.clone(),
            vec![
                (Value::from("widget"), 9_000),
                (Value::from("gadget"), 3_200),
                (Value::from("gizmo"), 1_100),
                (Value::from("sprocket"), 800),
            ],
        );
        catalog.record_bloom(
            key,
            BloomSummary {
                bit_count: 119_048,
                hash_count: 7,
                sized_from_estimate: true,
            },
        );

        let mut out = String::new();
        render_statistics_section(&mut out, &catalog);

        let expected = "\
=== Statistics ===

Row counts:
  orders: ≈1,200 rows [file metadata] (informs combine build/probe + partition bits)

Column sketches:
  orders.product_id: 12,431 distinct [exec sketch]
  orders.product_id: heavy hitters [exec sketch, lower bound]: widget=9,000, gadget=3,200, gizmo=1,100, +1 more
  orders.product_id: membership filter, 119048 bits / 7 probes [exec sketch, sized from estimate]

";
        assert_eq!(out, expected, "rendered:\n{out}");
    }

    /// An empty catalog adds no Statistics section, preserving the
    /// honest-null floor for a plan with no sized sources or sketches.
    #[test]
    fn statistics_section_omitted_when_catalog_empty() {
        use crate::plan::statistics::StatisticsCatalog;
        let mut out = String::new();
        render_statistics_section(&mut out, &StatisticsCatalog::new());
        assert!(
            out.is_empty(),
            "empty catalog must add nothing; got {out:?}"
        );
    }
}
