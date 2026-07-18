//! Streaming fused-path analysis over a compiled plan.
//!
//! These predicates decide which `producer → streaming-consumer` edges
//! bypass a `node_buffers` slot and stream record-by-record over a bounded
//! channel. The consumer side is parameterized over consumer kind, with
//! `Output` (a file-writer thread) as the sole certified consumer kind
//! today. Both `--explain` (plan-only) and the runtime dispatcher consult
//! the same predicates so the buffer-class annotation can never drift from
//! what the dispatcher actually does.

use std::collections::{HashMap, HashSet};

use crate::config::PipelineConfig;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};

use super::graph_util::compute_init_phase_node_set;

/// Reports whether `idx` has exactly one outgoing edge in `plan`. The
/// streaming-fusion classifiers reject fan-out at the upstream side —
/// a Source feeding two consumers cannot move its live receiver into
/// one of them, and a Merge whose merged stream is read by siblings
/// must stay on the buffered path so those siblings see the records
/// via `node_buffers`.
fn has_single_outgoing(plan: &ExecutionPlanDag, idx: petgraph::graph::NodeIndex) -> bool {
    plan.graph
        .neighbors_directed(idx, petgraph::Direction::Outgoing)
        .count()
        == 1
}

/// Identify Source-node names whose receivers are claimed by a
/// downstream `Merge.mode: interleave` whose every direct predecessor
/// is a `PlanNode::Source`. The Merge arm consumes those receivers
/// via `crossbeam_channel::Select` so a slow Source no longer blocks
/// peer Sources' channels from filling. Per-Source dispatch arms whose
/// name is in this set return cleanly without touching
/// `ctx.source_records`.
///
/// Only triggers when ALL predecessors are Sources. Mixed
/// predecessors (Source + Transform + ...) keep today's per-arm
/// drain path — the Merge arm reads from `node_buffers` for those.
/// Concat-mode Merges keep the per-Source drain too: concat drains
/// predecessors in declaration order, which the per-arm path
/// already delivers.
pub fn compute_merge_interleave_fused_sources(
    plan: &ExecutionPlanDag,
    config: &PipelineConfig,
) -> HashSet<String> {
    let mut fused: HashSet<String> = HashSet::new();
    for node_idx in plan.graph.node_indices() {
        let PlanNode::Merge {
            name: merge_name, ..
        } = &plan.graph[node_idx]
        else {
            continue;
        };
        let predecessors: Vec<_> = plan
            .graph
            .neighbors_directed(node_idx, petgraph::Direction::Incoming)
            .collect();
        if predecessors.is_empty() {
            continue;
        }
        let all_sources = predecessors
            .iter()
            .all(|p| matches!(&plan.graph[*p], PlanNode::Source { .. }));
        if !all_sources {
            continue;
        }
        // Look up the Merge's mode + seed in the config side-table
        // by name. Missing entry (defaulted MergeBody) is Concat.
        // Seeded interleaves (`interleave_seed: Some(_)`) take the
        // non-fused path so determinism survives — the fastrand
        // schedule over a pre-buffered Vec yields the same sequence
        // across runs, whereas a live-channel `Select` depends on
        // arrival order. Unseeded interleaves get fusion +
        // back-pressure.
        let (mode, seed) = config
            .nodes
            .iter()
            .find_map(|spanned| match &spanned.value {
                crate::config::PipelineNode::Merge { header, config }
                    if header.name == *merge_name =>
                {
                    Some((config.mode, config.interleave_seed))
                }
                _ => None,
            })
            .unwrap_or((crate::config::MergeMode::Concat, None));
        if !matches!(mode, crate::config::MergeMode::Interleave) || seed.is_some() {
            continue;
        }
        for pred in predecessors {
            if let PlanNode::Source { name, .. } = &plan.graph[pred] {
                fused.insert(name.clone());
            }
        }
    }
    fused
}

/// Identify `PlanNode::Transform` nodes whose sole upstream is a
/// `PlanNode::Source` with a parked receiver in `ctx.source_records`,
/// and whose evaluation can stream directly off that receiver instead
/// of a pre-drained Vec. Returns the set of fused Transform
/// `NodeIndex`es plus the set of Source names whose receivers move
/// into the fused Transform's hands (those names extend
/// [`compute_merge_interleave_fused_sources`]'s output so the Source
/// dispatch arm short-circuits via the same membership check).
///
/// Eligibility (all must hold):
///
/// - Exactly one incoming edge.
/// - That predecessor is a `PlanNode::Source`.
/// - The upstream Source has exactly one outgoing edge (no Merge or
///   sibling fan-out — fanning would require cloning the live channel).
/// - `window_index` is `None`. Windowed Transforms drive
///   `evaluate_single_transform_windowed`, which indexes records into
///   the upstream operator's arena via the row's enumerate position;
///   that arena is built upstream and must already be materialized.
/// - The Transform is not in the init-phase ancestor closure.
/// - The upstream Source's name is not already claimed by
///   `merge_fused`. Merge.interleave fusion wins because the Merge
///   needs every predecessor's receiver concurrently.
///
/// See https://github.com/rustpunk/clinker/issues/74 for the rationale
/// (extending #67's pattern from Merge to Transform unblocks
/// `[slow Source → Transform → out]` topologies from sitting idle on
/// the upstream Source's drain).
pub fn compute_transform_fused_sources(
    plan: &ExecutionPlanDag,
    merge_fused: &HashSet<String>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
) -> (HashSet<String>, HashSet<petgraph::graph::NodeIndex>) {
    let mut extra_fused_sources: HashSet<String> = HashSet::new();
    let mut fused_transforms: HashSet<petgraph::graph::NodeIndex> = HashSet::new();
    for node_idx in plan.graph.node_indices() {
        let PlanNode::Transform { window_index, .. } = &plan.graph[node_idx] else {
            continue;
        };
        if window_index.is_some() {
            continue;
        }
        if init_phase_set.contains(&node_idx) {
            continue;
        }
        let mut incoming = plan
            .graph
            .neighbors_directed(node_idx, petgraph::Direction::Incoming);
        let Some(pred_idx) = incoming.next() else {
            continue;
        };
        if incoming.next().is_some() {
            continue;
        }
        let PlanNode::Source {
            name: source_name, ..
        } = &plan.graph[pred_idx]
        else {
            continue;
        };
        if merge_fused.contains(source_name) {
            continue;
        }
        if !has_single_outgoing(plan, pred_idx) {
            continue;
        }
        extra_fused_sources.insert(source_name.clone());
        fused_transforms.insert(node_idx);
    }
    (extra_fused_sources, fused_transforms)
}

/// Per-node inter-stage materialization class.
///
/// A `Streaming` stage's output bypasses a `ctx.node_buffers` slot: a
/// sink (`Output`) that never admits a buffer, a fused `Source` whose
/// receiver the downstream consumer takes directly, or a fused
/// `Transform` that hands each bounded batch to a streaming Output thread
/// over a back-pressured channel without admitting a slot. A
/// `Materialized` stage's output crosses a `node_buffers` slot that
/// registers a `NodeBufferConsumer` and is spill-eligible.
///
/// The non-trivial verdict — whether a fused Transform streams — is
/// decided by [`certify_streaming_edge`], the one predicate both
/// [`classify_stream_nodes`] (the `--explain` annotation) and the runtime
/// sender install consult. A Transform is `Streaming` here iff a streaming
/// sender is installed for it at runtime, so the explain annotation can
/// never drift from what the dispatcher does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamClass {
    /// No `node_buffers` slot, no arbitrator charge, no spill.
    Streaming,
    /// Output crosses a `node_buffers` slot; spill-eligible.
    Materialized,
}

/// Classify every node in `plan` as [`StreamClass::Streaming`] or
/// [`StreamClass::Materialized`], derived from the same fusion analysis
/// the runtime dispatch consumes.
///
/// Streaming nodes:
///
/// - **Outputs**, unconditionally — sinks that write to their writer and
///   never admit a `node_buffers` slot.
/// - **Sources** whose name lands in either fused-source set
///   (Merge.interleave fusion or single-Transform fusion). The Source
///   dispatch arm returns without admitting a buffer; the downstream
///   fused consumer takes the receiver directly.
/// - A fused **Transform** whose single downstream is a streaming-
///   eligible Output, as decided by [`certify_streaming_edge`]. Such a
///   Transform hands each bounded batch straight to the streaming Output
///   thread over a back-pressured channel and never admits a
///   `node_buffers` slot, so its peak inter-stage footprint is one batch
///   rather than the whole stage. A fused Transform that feeds anything
///   other than a streaming Output (a fan-out, a window root, a blocking
///   operator) stays `Materialized`: it accumulates and admits its full
///   output.
///
/// Everything else materializes.
///
/// Both `--explain` (plan-only, no live consumers) and the runtime
/// dispatch call this — the dispatcher reads the same `Streaming` verdict
/// for a fused Transform to decide whether to install a streaming sender,
/// so the explain annotation can never disagree with the dispatcher. The
/// returned map covers every `node_indices()` slot so callers can index
/// it by `NodeIndex` directly.
pub fn classify_stream_nodes(
    plan: &ExecutionPlanDag,
    config: &PipelineConfig,
) -> HashMap<petgraph::graph::NodeIndex, StreamClass> {
    let init_phase = compute_init_phase_node_set(plan);
    let mut fused_sources = compute_merge_interleave_fused_sources(plan, config);
    let (extra_fused_sources, fused_transforms) =
        compute_transform_fused_sources(plan, &fused_sources, &init_phase);
    fused_sources.extend(extra_fused_sources);

    // Pipeline-wide correlation buffering routes every write through the
    // CorrelationCommit terminal, so no Output streams. Mirrors the same
    // short-circuit in the runtime spec computation so the explain
    // annotation matches.
    let correlation_active = config.any_source_has_correlation_key();

    // A producer streams iff some streaming-eligible consumer (Output,
    // Aggregate ingest, or Combine probe) certifies it as its producer.
    // `certify_streaming_edge` returns the producer index for every
    // certified consumer kind, so collecting it across all node indices
    // yields every streaming producer in one pass.
    let streaming_producers: HashSet<petgraph::graph::NodeIndex> = if correlation_active {
        HashSet::new()
    } else {
        plan.graph
            .node_indices()
            .filter_map(|output_idx| {
                certify_streaming_edge(plan, output_idx, &fused_transforms, &init_phase)
            })
            .collect()
    };

    plan.graph
        .node_indices()
        .map(|idx| {
            let class = match &plan.graph[idx] {
                PlanNode::Output { .. } => StreamClass::Streaming,
                PlanNode::Source { name, .. } if fused_sources.contains(name.as_str()) => {
                    StreamClass::Streaming
                }
                // Any producer the eligibility predicate accepts as
                // feeding a streaming consumer (Output writer, Aggregate
                // ingest, or Combine probe) — fused Transform, non-fused
                // Merge, single-branch Route, streaming Aggregate, inline
                // Combine probe, pure-range block-band Combine output drain
                // — streams its output over the bounded channel and admits
                // no `node_buffers` slot, so it renders `streaming`. The
                // predicate already excluded blocking strategies (hash
                // Aggregate; equi+range / sort-merge / grace-hash Combine)
                // and window roots, so this branch only fires for genuinely
                // streaming producers.
                PlanNode::Transform { .. }
                | PlanNode::Merge { .. }
                | PlanNode::Route { .. }
                | PlanNode::Aggregation { .. }
                | PlanNode::Combine { .. }
                    if streaming_producers.contains(&idx) =>
                {
                    StreamClass::Streaming
                }
                _ => StreamClass::Materialized,
            };
            (idx, class)
        })
        .collect()
}

/// Identify every `producer → Aggregate` edge whose ingest streams:
/// the producer feeds `add_record` per record over a bounded channel
/// rather than the Aggregate pre-draining the producer's whole output
/// from a charged `node_buffers` slot. Returns a map from the producer's
/// `NodeIndex` to the consuming Aggregate's `NodeIndex`.
///
/// This is the [`certify_streaming_edge`] predicate applied with each
/// `Aggregation` node as the consumer; the producer-kind half of the
/// predicate (fused `Source → Transform`, non-fused `Merge`, single-branch
/// `Route`, streaming-strategy `Aggregate`) is shared with the `Output`
/// consumer, so the same fusion analysis decides both. The runtime
/// installs one bounded channel per returned edge: the producer arm
/// streams into it during its own dispatch turn, and the Aggregate arm
/// drains it via a back-pressured recv loop — so a slow producer paces
/// the Aggregate's ingest and no inter-stage slot is charged for the edge.
///
/// Correlation buffering disables streaming pipeline-wide (the
/// `CorrelationCommit` terminal owns the writes), so the caller passes a
/// fused-transform set computed only when correlation is inactive; an
/// empty fused set leaves the map empty for those topologies.
pub fn compute_streaming_aggregate_ingest_edges(
    plan: &ExecutionPlanDag,
    fused_transforms: &HashSet<petgraph::graph::NodeIndex>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
) -> HashMap<petgraph::graph::NodeIndex, petgraph::graph::NodeIndex> {
    let mut edges = HashMap::new();
    for agg_idx in plan.graph.node_indices() {
        if !matches!(&plan.graph[agg_idx], PlanNode::Aggregation { .. }) {
            continue;
        }
        if let Some(producer_idx) =
            certify_streaming_edge(plan, agg_idx, fused_transforms, init_phase_set)
        {
            edges.insert(producer_idx, agg_idx);
        }
    }
    edges
}

/// Identify every `producer → Combine(driver)` edge whose probe-side
/// ingest streams: the driver (probe-side) producer feeds the hash-probe
/// kernel per record over a bounded channel rather than the Combine arm
/// pre-draining the driver's whole output from a charged `node_buffers`
/// slot. Returns a map from the driver producer's `NodeIndex` to the
/// consuming `Combine`'s `NodeIndex`.
///
/// This is the [`certify_streaming_edge`] predicate applied with each
/// `Combine` node as the consumer. The build side stays fully
/// materialized inside the arm — it is the hash table the probe reads —
/// so only the probe (driver) ingest lifts, and only for the
/// `HashBuildProbe` strategy (range / sort-merge / grace-hash re-sort or
/// re-scan the driver and stay blocking). The predicate resolves the
/// driver predecessor from the plan-stamped `driving_upstream` index, so a
/// Combine's build edge is never mistaken for the streaming edge.
///
/// The runtime installs one bounded channel per returned edge: the Combine
/// arm materializes the build-side hash table on the main thread, spawns
/// the probe consumer on its own thread, then redispatches the driver
/// producer (which streams into the channel during the Combine's dispatch
/// turn). Build-before-probe holds because the hash table is complete
/// before the driver producer's first send; a slow driver paces the probe
/// over the bounded channel and no inter-stage slot is charged for the
/// edge.
///
/// Correlation buffering disables streaming pipeline-wide (the
/// `CorrelationCommit` terminal owns the writes), so the caller passes a
/// fused-transform set computed only when correlation is inactive.
pub fn compute_streaming_combine_probe_edges(
    plan: &ExecutionPlanDag,
    fused_transforms: &HashSet<petgraph::graph::NodeIndex>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
) -> HashMap<petgraph::graph::NodeIndex, petgraph::graph::NodeIndex> {
    let mut edges = HashMap::new();
    for combine_idx in plan.graph.node_indices() {
        if !matches!(&plan.graph[combine_idx], PlanNode::Combine { .. }) {
            continue;
        }
        if let Some(producer_idx) =
            certify_streaming_edge(plan, combine_idx, fused_transforms, init_phase_set)
        {
            edges.insert(producer_idx, combine_idx);
        }
    }
    edges
}

/// Certify a `producer → consumer` edge for the streaming substrate,
/// returning the producer's `NodeIndex` when the edge streams, or `None`
/// when it stays materialized.
///
/// `consumer_idx` names the downstream streaming consumer. Three consumer
/// kinds are certified: `Output` (the file-writer sink), `Aggregation`
/// (the per-record ingest half of a strict GROUP BY), and `Combine` (the
/// probe-side ingest half of a hash build-probe join). A streaming
/// consumer's work moves into a `std::thread` that consumes a bounded
/// crossbeam channel; the producer arm sends records into that channel as
/// it produces them, so back-pressure flows consumer → producer → Source
/// and no inter-stage `node_buffers` slot is charged. This function is the
/// single plan-derived eligibility predicate both the runtime spawn path
/// and the `--explain` buffer-class annotation ([`classify_stream_nodes`])
/// consult, so the explain annotation can never disagree with what the
/// dispatcher does.
///
/// Common eligibility (independent of consumer or producer kind):
///
/// - The consumer is not in the init-phase ancestor closure.
///
/// Consumer-specific eligibility, and how each resolves its producer:
///
/// - `Output`: not a per-source-file fan-out writer and no `split:` block
///   (both own their own writer lifecycle, incompatible with the single
///   streaming writer task). Producer = its sole incoming edge.
/// - `Aggregation`: strict only — not relaxed-CK (`requires_lineage` /
///   `requires_buffer_mode`, which retains state for the correlation
///   commit) and not time-windowed (multi-pass over the whole batch).
///   Producer = its sole incoming edge.
/// - `Combine`: strategy `HashBuildProbe` only (range / sort-merge /
///   grace-hash re-sort or re-scan the driver and stay blocking). A
///   Combine is fan-in (build edge + driver edge), so its producer is the
///   plan-stamped `driving_upstream` (probe-side) predecessor, not a sole
///   incoming edge; `None` when the driver could not be resolved to a
///   direct predecessor.
///
/// Every other consumer node kind returns `None`.
///
/// The resolved producer is then certified by [`certify_linear_producer`],
/// which admits these producer kinds (each must have exactly one outgoing
/// edge — into this consumer — and root no node-anchored window arena,
/// because a window rooted at the producer needs the producer's full
/// output materialized and streaming would starve it):
///
/// - fused `Merge.interleave` (issue #72): a `PlanNode::Merge` in
///   `interleave` mode whose every direct predecessor is a fused Source.
/// - non-fused `Merge` (concat, or interleave with non-Source inputs):
///   the arm drains its predecessors' `node_buffers` slots and forwards
///   each record to the consumer rather than admitting its own slot.
/// - fused `Source → Transform`: a `PlanNode::Transform` that
///   `compute_transform_fused_sources` classified as fused.
/// - single-branch `Route`: exactly one outgoing edge into this consumer.
/// - streaming-strategy `Aggregate`
///   ([`crate::plan::types::AggregateStrategy::Streaming`]): emits one
///   group per sort-key boundary; hash aggregation stays blocking.
/// - hash build-probe `Combine`
///   ([`CombineStrategy::HashBuildProbe`]): only the probe-side emit
///   streams; the build side stays materialized inside the arm.
/// - block-band `Combine` ([`CombineStrategy::IEJoin`] pure-range and
///   [`CombineStrategy::HashPartitionIEJoin`] equi+range, which share the
///   one path): blocking on its input (both sides drained and sorted), but
///   its bounded payload-sorted output drain streams to the consumer. The
///   sort-merge and grace-hash joins keep their output materialized and
///   stay blocking.
///
/// Correlation buffering disables streaming pipeline-wide — the
/// `CorrelationCommit` terminal owns the writes — so the caller short-
/// circuits before calling this.
pub fn certify_streaming_edge(
    plan: &ExecutionPlanDag,
    consumer_idx: petgraph::graph::NodeIndex,
    fused_transforms: &HashSet<petgraph::graph::NodeIndex>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
) -> Option<petgraph::graph::NodeIndex> {
    use crate::plan::combine::CombineStrategy;

    // Consumer-generic eligibility, hoisted ahead of the consumer-kind
    // dispatch because every certifying arm shares it: an init-phase
    // consumer drives the bootstrap DAG (no streaming writer task), and a
    // consumer with fan-in cannot move a single upstream producer's live
    // channel into itself.
    if init_phase_set.contains(&consumer_idx) {
        return None;
    }

    // Consumer-kind dispatch resolves the producer (probe-side) index per
    // consumer kind, then defers to `certify_linear_producer` for the
    // shared producer-kind half. `Output` (file-writer sink), `Aggregation`
    // (the per-record ingest half of a GROUP BY), and `Combine` (the
    // probe-side ingest half of a hash build-probe join) are the certified
    // consumer kinds; every other kind falls through to `None`.
    //
    // `Output` and `Aggregation` are linear consumers with exactly one
    // incoming edge, so they share the generic single-incoming guard.
    // `Combine` is fan-in by construction (build edge + driver edge), so it
    // resolves its driver predecessor directly from the plan-stamped
    // `driving_upstream` index instead — the single-incoming guard would
    // reject it.
    let producer_idx: petgraph::graph::NodeIndex = match &plan.graph[consumer_idx] {
        PlanNode::Output { resolved, .. } => {
            let payload = resolved.as_deref()?;
            // Per-source-file fan-out and split writers own their own file
            // rotation lifecycle and cannot share the single streaming
            // writer task. Both are plan-derived so the explain and
            // runtime surfaces agree without consulting the runtime writer
            // registry.
            if payload.fan_out_per_source_file || payload.output.split.is_some() {
                return None;
            }
            single_incoming_producer(plan, consumer_idx)?
        }
        PlanNode::Aggregation {
            config, compiled, ..
        } => {
            // The ingest half streams: an eligible producer feeds
            // `add_record` per record over the bounded channel rather than
            // pre-draining the producer's whole output from a charged
            // `node_buffers` slot. The finalize half stays blocking (it
            // accumulates every group before emitting) — that is the
            // operator's nature, not a buffer this lifts.
            //
            // Two aggregate shapes keep the materialized ingest:
            //
            // - Relaxed-CK aggregates (`requires_lineage` /
            //   `requires_buffer_mode`) retain their hash state for the
            //   correlation-commit phase, which re-`retract` + re-finalize
            //   the same instance; streaming ingest would have to hand that
            //   live state across a thread boundary and back, so the commit
            //   path keeps draining a materialized slot.
            // - Time-windowed aggregates run a multi-pass per-window (and,
            //   for sessions, global-sort) algorithm over the whole input
            //   batch, which is incompatible with a single forward
            //   record-at-a-time pass.
            //
            // Both are plan-derived flags, so the explain buffer-class
            // annotation and the runtime ingest path agree without
            // inspecting runtime state.
            let is_relaxed = compiled.requires_lineage || compiled.requires_buffer_mode;
            if is_relaxed || config.time_window.is_some() {
                return None;
            }
            single_incoming_producer(plan, consumer_idx)?
        }
        PlanNode::Combine {
            strategy,
            driving_upstream,
            ..
        } => {
            // The probe (driver) ingest half streams: the driver producer
            // feeds the probe kernel per record over the bounded channel
            // rather than the arm pre-draining the driver's whole output
            // from a charged `node_buffers` slot. The build side stays
            // fully materialized inside the arm — it is the hash table the
            // probe reads, complete before the first probe — so only the
            // probe-side ingest lifts. Range / sort-merge / grace-hash
            // joins re-sort or re-scan the driver and stay blocking.
            if !matches!(strategy, CombineStrategy::HashBuildProbe) {
                return None;
            }
            // Resolve the driver predecessor from the plan-stamped index
            // rather than the incoming edges: a Combine has both a build
            // edge and a driver edge, so it never has exactly one incoming
            // neighbor. `driving_upstream` is `None` when the strategy
            // post-pass could not resolve the driver to a direct
            // predecessor (e.g. an N-ary decomposition step whose driver is
            // a synthetic intermediate) — decline streaming for those.
            (*driving_upstream)?
        }
        _ => return None,
    };

    certify_linear_producer(plan, producer_idx, fused_transforms)
}

/// Resolve the sole incoming producer of a linear (single-fan-in)
/// streaming consumer, or `None` when the consumer has zero or more than
/// one incoming edge. Used by the `Output` / `Aggregation` consumer arms,
/// whose streaming model moves a single upstream producer's live channel
/// into the consumer; a consumer with fan-in cannot do that.
fn single_incoming_producer(
    plan: &ExecutionPlanDag,
    consumer_idx: petgraph::graph::NodeIndex,
) -> Option<petgraph::graph::NodeIndex> {
    let mut incoming = plan
        .graph
        .neighbors_directed(consumer_idx, petgraph::Direction::Incoming);
    let producer_idx = incoming.next()?;
    if incoming.next().is_some() {
        return None;
    }
    Some(producer_idx)
}

/// Certify the producer-kind half of a streaming edge: given the resolved
/// producer index, return `Some(producer_idx)` when the producer is a
/// linear streaming source (fused `Merge.interleave`, non-fused `Merge`,
/// fused `Source → Transform`, single-branch `Route`, streaming-strategy
/// `Aggregate`, hash build-probe `Combine`, or pure-range block-band
/// `Combine`) feeding only the consumer and rooting no node-anchored window
/// arena, or `None` otherwise.
///
/// Shared by every consumer arm of [`certify_streaming_edge`] so the
/// producer eligibility rules are derived once. A window rooted at the
/// producer needs the producer's full output materialized to build the
/// arena, so a streaming producer must root no node-anchored window —
/// enforced uniformly here.
fn certify_linear_producer(
    plan: &ExecutionPlanDag,
    producer_idx: petgraph::graph::NodeIndex,
    fused_transforms: &HashSet<petgraph::graph::NodeIndex>,
) -> Option<petgraph::graph::NodeIndex> {
    use crate::plan::combine::CombineStrategy;
    use crate::plan::types::AggregateStrategy;

    // A window rooted at the producer needs the producer's full output
    // materialized to build the arena, so a streaming producer must root
    // no node-anchored window. Shared by every linear-producer arm below.
    let roots_window = |idx: petgraph::graph::NodeIndex| -> bool {
        plan.indices_to_build.iter().any(|spec| {
            matches!(
                &spec.root,
                crate::plan::index::PlanIndexRoot::Node { upstream, .. }
                    if *upstream == idx
            )
        })
    };

    match &plan.graph[producer_idx] {
        PlanNode::Merge { .. } => {
            // Fused Merge.interleave streams off the live Source channels
            // directly inside `merge_fused_interleave`; the non-fused
            // Merge (concat, or interleave with non-Source inputs) drains
            // its predecessors' `node_buffers` slots and forwards each
            // record to the writer thread. Both route their emit through
            // the streaming sender — the runtime arm picks the path — so
            // every Merge with a single downstream consumer and no window
            // root is eligible regardless of fusion.
            let has_predecessor = plan
                .graph
                .neighbors_directed(producer_idx, petgraph::Direction::Incoming)
                .next()
                .is_some();
            if !has_predecessor
                || !has_single_outgoing(plan, producer_idx)
                || roots_window(producer_idx)
            {
                return None;
            }
            Some(producer_idx)
        }
        PlanNode::Transform { .. } => {
            // The Transform must be a fused Source→Transform that feeds
            // only this consumer and roots no node-anchored window arena.
            if !fused_transforms.contains(&producer_idx)
                || !has_single_outgoing(plan, producer_idx)
                || roots_window(producer_idx)
            {
                return None;
            }
            Some(producer_idx)
        }
        PlanNode::Route { .. } => {
            // Single-branch Route: exactly one outgoing edge (this
            // consumer). A multi-branch Route forks across successor slots
            // and stays materialized.
            if !has_single_outgoing(plan, producer_idx) || roots_window(producer_idx) {
                return None;
            }
            Some(producer_idx)
        }
        PlanNode::Aggregation { strategy, .. } => {
            // Streaming aggregation emits one group per sort-key
            // boundary, so it can stream to the consumer; hash aggregation
            // must accumulate every group before emitting and stays
            // blocking.
            if !matches!(strategy, AggregateStrategy::Streaming) {
                return None;
            }
            if !has_single_outgoing(plan, producer_idx) || roots_window(producer_idx) {
                return None;
            }
            Some(producer_idx)
        }
        PlanNode::Combine { strategy, .. } => {
            // Two combine shapes stream their output over the sink, for
            // different reasons:
            //
            // - `HashBuildProbe`: the probe-side emit streams row-by-row
            //   while the build side stays materialized in the hash table.
            // - `IEJoin` / `HashPartitionIEJoin` (the one block-band path,
            //   pure-range and equi+range): fully blocking on its INPUT —
            //   both sides are drained, sorted, and the output buffer
            //   filled before anything emits — but the bounded,
            //   payload-sorted output DRAIN streams straight to the sink
            //   instead of admitting a node-buffer. From this predicate's
            //   view it is a linear producer with one output edge, same as
            //   the others.
            //
            // The sort-merge and grace-hash joins keep their output
            // materialized (their output axis is a separate task), so they
            // stay blocking here.
            if !matches!(
                strategy,
                CombineStrategy::HashBuildProbe
                    | CombineStrategy::IEJoin
                    | CombineStrategy::HashPartitionIEJoin { .. }
            ) {
                return None;
            }
            if !has_single_outgoing(plan, producer_idx) || roots_window(producer_idx) {
                return None;
            }
            Some(producer_idx)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{CompileContext, parse_config};
    use crate::plan::combine::CombineStrategy;

    /// A pure-range combine (`amount >= lo and amount < hi`, no equality
    /// conjunct) whose sole downstream is a single `Output`. The planner
    /// selects `CombineStrategy::IEJoin` (the block-band path), and the
    /// combine has exactly one out-edge into the Output.
    const PURE_RANGE_TO_OUTPUT: &str = r#"
pipeline:
  name: cert_pure_range
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: string }
      - { name: amount, type: int }
- type: source
  name: bands
  config:
    name: bands
    type: csv
    path: bands.csv
    schema:
      - { name: band_id, type: string }
      - { name: lo, type: int }
      - { name: hi, type: int }
- type: combine
  name: banded
  input:
    orders: orders
    bands: bands
  config:
    where: "orders.amount >= bands.lo and orders.amount < bands.hi"
    match: all
    on_miss: skip
    cxl: |
      emit order_id = orders.order_id
      emit band_id = bands.band_id
    propagate_ck: driver
- type: output
  name: out
  input: banded
  config:
    name: out
    type: csv
    path: out.csv
"#;

    /// The same pure-range combine, but its output fans out to two
    /// `Output` sinks — two out-edges, so the single-outgoing constraint
    /// must reject streaming.
    const PURE_RANGE_FAN_OUT: &str = r#"
pipeline:
  name: cert_pure_range_fanout
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: string }
      - { name: amount, type: int }
- type: source
  name: bands
  config:
    name: bands
    type: csv
    path: bands.csv
    schema:
      - { name: band_id, type: string }
      - { name: lo, type: int }
      - { name: hi, type: int }
- type: combine
  name: banded
  input:
    orders: orders
    bands: bands
  config:
    where: "orders.amount >= bands.lo and orders.amount < bands.hi"
    match: all
    on_miss: skip
    cxl: |
      emit order_id = orders.order_id
      emit band_id = bands.band_id
    propagate_ck: driver
- type: output
  name: out_a
  input: banded
  config:
    name: out_a
    type: csv
    path: out_a.csv
- type: output
  name: out_b
  input: banded
  config:
    name: out_b
    type: csv
    path: out_b.csv
"#;

    /// An equi+range combine (`region == region and amount >= lo`) whose
    /// sole downstream is a single `Output`. The planner selects
    /// `CombineStrategy::HashPartitionIEJoin`, which now runs the same bounded
    /// block-band path as pure-range `IEJoin`, so its payload-sorted output
    /// drain streams to the sink and the producer arm certifies it.
    const EQUI_RANGE_TO_OUTPUT: &str = r#"
pipeline:
  name: cert_equi_range
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: region, type: string }
      - { name: amount, type: int }
- type: source
  name: bands
  config:
    name: bands
    type: csv
    path: bands.csv
    schema:
      - { name: region, type: string }
      - { name: lo, type: int }
- type: combine
  name: banded
  input:
    orders: orders
    bands: bands
  config:
    where: "orders.region == bands.region and orders.amount >= bands.lo"
    match: all
    on_miss: skip
    cxl: |
      emit region = orders.region
      emit amount = orders.amount
      emit lo = bands.lo
    propagate_ck: driver
- type: output
  name: out
  input: banded
  config:
    name: out
    type: csv
    path: out.csv
"#;

    /// Compile `yaml`, then run the exact fused-set + init-phase setup that
    /// [`classify_stream_nodes`] uses at runtime, invoking the closure with
    /// the compiled DAG plus the two derived sets so a test can assert
    /// [`certify_streaming_edge`] verdicts against live plan indices.
    fn with_certification_inputs<R>(
        yaml: &str,
        f: impl FnOnce(
            &ExecutionPlanDag,
            &HashSet<petgraph::graph::NodeIndex>,
            &HashSet<petgraph::graph::NodeIndex>,
        ) -> R,
    ) -> R {
        let config = parse_config(yaml).expect("parse pipeline YAML");
        let validated = config
            .compile(&CompileContext::default())
            .expect("compile pipeline");
        let dag = validated.dag();
        let init_phase = compute_init_phase_node_set(dag);
        let merge_fused = compute_merge_interleave_fused_sources(dag, &config);
        let (_extra_sources, fused_transforms) =
            compute_transform_fused_sources(dag, &merge_fused, &init_phase);
        f(dag, &fused_transforms, &init_phase)
    }

    /// The `NodeIndex` of the first node matching `pred`, panicking with
    /// `label` when absent.
    fn find_node(
        dag: &ExecutionPlanDag,
        label: &str,
        pred: impl Fn(&PlanNode) -> bool,
    ) -> petgraph::graph::NodeIndex {
        dag.graph
            .node_indices()
            .find(|idx| pred(&dag.graph[*idx]))
            .unwrap_or_else(|| panic!("plan is missing a {label} node"))
    }

    fn combine_strategy(dag: &ExecutionPlanDag) -> CombineStrategy {
        let idx = find_node(dag, "combine", |n| matches!(n, PlanNode::Combine { .. }));
        match &dag.graph[idx] {
            PlanNode::Combine { strategy, .. } => strategy.clone(),
            _ => unreachable!("find_node matched a Combine"),
        }
    }

    #[test]
    fn pure_range_block_band_combine_certifies_streaming_output_edge() {
        with_certification_inputs(PURE_RANGE_TO_OUTPUT, |dag, fused, init| {
            // Honest fixture: the predicate is pure-range, so the block-band
            // strategy is selected — the exact producer this task admits.
            assert!(
                matches!(combine_strategy(dag), CombineStrategy::IEJoin),
                "the pure-range predicate must select the block-band IEJoin strategy, got {:?}",
                combine_strategy(dag)
            );
            let combine = find_node(dag, "combine", |n| matches!(n, PlanNode::Combine { .. }));
            let output = find_node(dag, "output", |n| matches!(n, PlanNode::Output { .. }));
            // The combine→Output edge certifies as a streaming output edge,
            // resolving the block-band combine as the producer.
            assert_eq!(
                certify_streaming_edge(dag, output, fused, init),
                Some(combine),
                "combine(IEJoin) -> single Output must certify the combine as the streaming producer"
            );
            // The runtime-facing classification agrees: the combine renders
            // `Streaming` (no node-buffers slot), so the dispatcher installs
            // a sender for it.
            let classes = classify_stream_nodes(
                dag,
                &parse_config(PURE_RANGE_TO_OUTPUT).expect("re-parse for config"),
            );
            assert_eq!(classes[&combine], StreamClass::Streaming);
        });
    }

    #[test]
    fn pure_range_block_band_combine_with_fan_out_is_not_certified() {
        with_certification_inputs(PURE_RANGE_FAN_OUT, |dag, fused, init| {
            assert!(
                matches!(combine_strategy(dag), CombineStrategy::IEJoin),
                "the fan-out fixture must still select the block-band strategy"
            );
            let combine = find_node(dag, "combine", |n| matches!(n, PlanNode::Combine { .. }));
            // Two out-edges: every Output's incoming edge fails the
            // single-outgoing constraint, so none certifies.
            for output in dag
                .graph
                .node_indices()
                .filter(|idx| matches!(&dag.graph[*idx], PlanNode::Output { .. }))
            {
                assert_eq!(
                    certify_streaming_edge(dag, output, fused, init),
                    None,
                    "a combine with >1 outgoing edge must not certify a streaming output edge"
                );
            }
            let classes = classify_stream_nodes(
                dag,
                &parse_config(PURE_RANGE_FAN_OUT).expect("re-parse for config"),
            );
            assert_eq!(
                classes[&combine],
                StreamClass::Materialized,
                "a fan-out combine must materialize its output"
            );
        });
    }

    #[test]
    fn equi_range_hash_partition_combine_certifies_streaming_output_edge() {
        with_certification_inputs(EQUI_RANGE_TO_OUTPUT, |dag, fused, init| {
            // The equi+range HashPartitionIEJoin path now runs the same bounded
            // block-band machinery as pure-range IEJoin, so its payload-sorted
            // output drain streams to the sink exactly like the pure-range case.
            assert!(
                matches!(
                    combine_strategy(dag),
                    CombineStrategy::HashPartitionIEJoin { .. }
                ),
                "the equi+range predicate must select the hash-partitioned IEJoin strategy, got {:?}",
                combine_strategy(dag)
            );
            let combine = find_node(dag, "combine", |n| matches!(n, PlanNode::Combine { .. }));
            let output = find_node(dag, "output", |n| matches!(n, PlanNode::Output { .. }));
            // The combine→Output edge certifies, resolving the equi+range
            // block-band combine as the streaming producer, and the runtime
            // classification agrees.
            assert_eq!(
                certify_streaming_edge(dag, output, fused, init),
                Some(combine),
                "equi+range block-band combine -> single Output must certify the combine as the \
                 streaming producer"
            );
            let classes = classify_stream_nodes(
                dag,
                &parse_config(EQUI_RANGE_TO_OUTPUT).expect("re-parse for config"),
            );
            assert_eq!(classes[&combine], StreamClass::Streaming);
        });
    }
}
