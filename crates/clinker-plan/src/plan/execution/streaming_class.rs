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

    // A fused Transform streams iff some streaming-eligible Output names
    // it as its producer. Collect those producer indices once.
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
                // feeding a streaming Output — fused Transform, non-fused
                // Merge, single-branch Route, streaming Aggregate, inline
                // Combine probe — streams its output to the writer thread
                // and admits no `node_buffers` slot, so it renders
                // `streaming`. The predicate already excluded blocking
                // strategies (hash Aggregate, range/sort-merge/grace
                // Combine) and window roots, so this branch only fires for
                // genuinely streaming producers.
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

/// Certify a `producer → consumer` edge for the streaming substrate,
/// returning the producer's `NodeIndex` when the edge streams, or `None`
/// when it stays materialized.
///
/// `consumer_idx` names the downstream streaming consumer; today the only
/// certified consumer kind is `PlanNode::Output`, but the eligibility
/// shell is consumer-generic so future streaming consumers (a streaming
/// `Aggregate` ingest, a `Combine` probe) plug in as additional certifying
/// arms without re-deriving the producer analysis. A streaming consumer's
/// work moves into a `std::thread` that consumes a bounded crossbeam
/// channel; the producer arm sends records into that channel as it
/// produces them, so back-pressure flows consumer → producer → Source and
/// no inter-stage `node_buffers` slot is charged. This function is the
/// single plan-derived eligibility predicate both the runtime spawn path
/// and the `--explain` buffer-class annotation ([`classify_stream_nodes`])
/// consult, so the explain annotation can never disagree with what the
/// dispatcher does.
///
/// Common eligibility (independent of producer kind):
///
/// - The consumer is not in the init-phase ancestor closure.
/// - The consumer has exactly one incoming edge.
///
/// Consumer = `Output` (the sole certified consumer kind today):
///
/// - The Output is not a per-source-file fan-out writer and its config
///   declares no `split:` block — both own their own writer lifecycle
///   and are incompatible with the single streaming writer task.
///
/// Every other consumer node kind returns `None`: the consumer-specific
/// guards above are conjunctive, so no producer edge into a non-`Output`
/// consumer is certified until that consumer kind grows its own arm.
///
/// Producer = fused `Merge.interleave` (issue #72):
///
/// - The predecessor is a `PlanNode::Merge` in `interleave` mode whose
///   every direct predecessor is a fused Source, with exactly one
///   outgoing edge (this Output).
///
/// Producer = fused `Source → Transform` (the bounded per-batch
/// inter-stage handoff):
///
/// - The predecessor is a `PlanNode::Transform` that
///   `compute_transform_fused_sources` classified as fused (its sole
///   upstream is a single-outgoing Source it streams off directly), with
///   exactly one outgoing edge (this Output).
/// - The Transform roots no node-anchored window arena. A window rooted
///   at this Transform needs the Transform's full output materialized to
///   build the arena, so streaming would starve it; such a Transform
///   stays on the buffered path.
///
/// Producer = non-fused `Merge` (concat, or interleave with non-Source
/// inputs):
///
/// - The Merge feeds only this Output and roots no window arena. The
///   arm drains its predecessors' `node_buffers` slots in declaration
///   order (concat) or round-robin (interleave) into the merged result,
///   then streams that result to the writer thread rather than admitting
///   it to its own charged `node_buffers` slot. (The fused
///   Merge.interleave path streams record-by-record off the live Source
///   channels instead and is the only Merge whose footprint is one
///   batch.)
///
/// Producer = single-branch `Route`:
///
/// - The Route has exactly one outgoing edge (its sole branch) and that
///   edge feeds this Output, and the Route roots no window arena. A
///   multi-branch Route forks records across several successor slots and
///   stays on the materialized fan-out path so each branch keeps its own
///   slot — only a Route with a single downstream consumer is a linear
///   producer the streaming writer can drain.
///
/// Producer = streaming-strategy `Aggregate`:
///
/// - The aggregate resolved to [`crate::plan::types::AggregateStrategy::Streaming`] (the
///   planner certified pre-sorted input), feeds only this Output, and
///   roots no window arena. Hash aggregation stays blocking: it must
///   accumulate every group before emitting, so it cannot stream.
///
/// Producer = `Combine` probe-side (`HashBuildProbe`):
///
/// - The combine resolved to [`CombineStrategy::HashBuildProbe`], feeds
///   only this Output, and roots no window arena. The build side stays
///   fully materialized inside the arm; only the probe-side emit streams.
///   Range / sort-merge / grace-hash strategies are blocking and stay
///   materialized.
///
/// Common to every non-`Output` producer kind below: the producer must
/// have exactly one outgoing edge (this Output) and root no node-anchored
/// window arena, because a window rooted at the producer needs the
/// producer's full output materialized to build the arena and streaming
/// would starve it.
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
    use crate::plan::types::AggregateStrategy;

    // Consumer-generic eligibility, hoisted ahead of the consumer-kind
    // dispatch because every certifying arm shares it: an init-phase
    // consumer drives the bootstrap DAG (no streaming writer task), and a
    // consumer with fan-in cannot move a single upstream producer's live
    // channel into itself.
    if init_phase_set.contains(&consumer_idx) {
        return None;
    }

    // Consumer-kind dispatch. `Output` is the sole certified consumer kind
    // today; every other kind falls through to `None`. The guards here are
    // the consumer-specific half of the predicate (the producer-kind half
    // is the `match` on `producer_idx` below).
    match &plan.graph[consumer_idx] {
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
        }
        _ => return None,
    }

    // Exactly one incoming edge.
    let mut incoming = plan
        .graph
        .neighbors_directed(consumer_idx, petgraph::Direction::Incoming);
    let producer_idx = incoming.next()?;
    if incoming.next().is_some() {
        return None;
    }

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
            // every Merge with a single downstream Output and no window
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
            // only this Output and roots no node-anchored window arena.
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
            // Output). A multi-branch Route forks across successor slots
            // and stays materialized.
            if !has_single_outgoing(plan, producer_idx) || roots_window(producer_idx) {
                return None;
            }
            Some(producer_idx)
        }
        PlanNode::Aggregation { strategy, .. } => {
            // Streaming aggregation emits one group per sort-key
            // boundary, so it can stream to the writer; hash aggregation
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
            // Only the inline hash build-probe streams its probe-side
            // emit; the build side stays fully materialized inside the
            // arm. Range / sort-merge / grace-hash joins are blocking.
            if !matches!(strategy, CombineStrategy::HashBuildProbe) {
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
