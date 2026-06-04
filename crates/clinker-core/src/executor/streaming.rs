//! Streaming fused-path analysis and the streaming-output writer thread.
//!
//! These functions decide which producer → single-`Output` chains bypass a
//! `node_buffers` slot and stream record-by-record to a writer thread, and
//! implement that writer thread. Both `--explain` (plan-only) and the
//! runtime dispatcher consult the same predicates here so the buffer-class
//! annotation can never drift from what the dispatcher actually does.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::Arc;

use clinker_record::Schema;

use crate::config::{OutputConfig, PipelineConfig};
use crate::error::PipelineError;
use clinker_format::traits::FormatWriter;

use super::{
    WriterRegistry, build_format_writer, compute_init_phase_node_set, dispatch, stage_metrics,
};

/// Reports whether `idx` has exactly one outgoing edge in `plan`. The
/// streaming-fusion classifiers reject fan-out at the upstream side —
/// a Source feeding two consumers cannot move its live receiver into
/// one of them, and a Merge whose merged stream is read by siblings
/// must stay on the buffered path so those siblings see the records
/// via `node_buffers`.
fn has_single_outgoing(
    plan: &crate::plan::execution::ExecutionPlanDag,
    idx: petgraph::graph::NodeIndex,
) -> bool {
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
pub(crate) fn compute_merge_interleave_fused_sources(
    plan: &crate::plan::execution::ExecutionPlanDag,
    config: &PipelineConfig,
) -> HashSet<String> {
    use crate::plan::execution::PlanNode;
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
pub(crate) fn compute_transform_fused_sources(
    plan: &crate::plan::execution::ExecutionPlanDag,
    merge_fused: &HashSet<String>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
) -> (HashSet<String>, HashSet<petgraph::graph::NodeIndex>) {
    use crate::plan::execution::PlanNode;
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
/// decided by [`streaming_output_producer`], the one predicate both
/// [`classify_stream_nodes`] (the `--explain` annotation) and
/// [`compute_streaming_output_specs`] (the runtime sender install)
/// consult. A Transform is `Streaming` here iff a streaming sender is
/// installed for it at runtime, so the explain annotation can never drift
/// from what the dispatcher does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamClass {
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
///   eligible Output, as decided by [`streaming_output_producer`]. Such a
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
pub(crate) fn classify_stream_nodes(
    plan: &crate::plan::execution::ExecutionPlanDag,
    config: &PipelineConfig,
) -> HashMap<petgraph::graph::NodeIndex, StreamClass> {
    use crate::plan::execution::PlanNode;
    let init_phase = compute_init_phase_node_set(plan);
    let mut fused_sources = compute_merge_interleave_fused_sources(plan, config);
    let (extra_fused_sources, fused_transforms) =
        compute_transform_fused_sources(plan, &fused_sources, &init_phase);
    fused_sources.extend(extra_fused_sources);

    // Pipeline-wide correlation buffering routes every write through the
    // CorrelationCommit terminal, so no Output streams. Mirrors the same
    // short-circuit in `compute_streaming_output_specs` so the explain
    // annotation matches the runtime spec computation.
    let correlation_active = config.any_source_has_correlation_key();

    // A fused Transform streams iff some streaming-eligible Output names
    // it as its producer. Collect those producer indices once.
    let streaming_producers: HashSet<petgraph::graph::NodeIndex> = if correlation_active {
        HashSet::new()
    } else {
        plan.graph
            .node_indices()
            .filter_map(|output_idx| {
                streaming_output_producer(plan, output_idx, &fused_transforms, &init_phase)
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

/// Resolve the streaming producer for an `Output` node, or `None` when
/// the Output is not streaming-eligible.
///
/// A streaming Output's writer moves into a `std::thread` that consumes a
/// bounded crossbeam channel; the producer arm sends records into that
/// channel as it produces them, so back-pressure flows writer → producer
/// → Source and no inter-stage `node_buffers` slot is charged. Two
/// producer topologies qualify, and this function is the single
/// plan-derived eligibility predicate both the runtime spawn path
/// ([`compute_streaming_output_specs`]) and the `--explain` buffer-class
/// annotation ([`classify_stream_nodes`]) consult, so the explain
/// annotation can never disagree with what the dispatcher does.
///
/// Common eligibility (independent of producer kind):
///
/// - The Output has exactly one incoming edge.
/// - The Output is not in the init-phase ancestor closure.
/// - The Output is not a per-source-file fan-out writer and its config
///   declares no `split:` block — both own their own writer lifecycle
///   and are incompatible with the single streaming writer task.
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
fn streaming_output_producer(
    plan: &crate::plan::execution::ExecutionPlanDag,
    output_idx: petgraph::graph::NodeIndex,
    fused_transforms: &HashSet<petgraph::graph::NodeIndex>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
) -> Option<petgraph::graph::NodeIndex> {
    use crate::plan::combine::CombineStrategy;
    use crate::plan::execution::PlanNode;
    use crate::plan::types::AggregateStrategy;

    let PlanNode::Output { resolved, .. } = &plan.graph[output_idx] else {
        return None;
    };
    let payload = resolved.as_deref()?;
    if init_phase_set.contains(&output_idx) {
        return None;
    }
    // Per-source-file fan-out and split writers own their own file
    // rotation lifecycle and cannot share the single streaming writer
    // task. Both are plan-derived so the explain and runtime surfaces
    // agree without consulting the runtime writer registry.
    if payload.fan_out_per_source_file || payload.output.split.is_some() {
        return None;
    }

    // Exactly one incoming edge.
    let mut incoming = plan
        .graph
        .neighbors_directed(output_idx, petgraph::Direction::Incoming);
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

/// Identify fused producer → single `Output` chains eligible for
/// streaming writes and build a [`StreamingOutputSpec`] for each.
///
/// The buffered Output arm waits until its producer has emitted every
/// record before invoking the writer; under a slow upstream Source this
/// defeats the live back-pressure the Source ingest channel delivers,
/// because every record sits in the producer's `node_buffers` slot until
/// the producer finishes. The streaming path moves the Output's writer
/// into a `std::thread` that consumes a bounded crossbeam channel the
/// producer arm fills as it produces; per-record `Writer::write_record`
/// fires concurrently with production and back-pressure flows writer →
/// producer → Source. The producer's output never crosses a
/// `node_buffers` slot, so peak inter-stage memory for that stage is one
/// bounded batch rather than its whole output.
///
/// Eligibility is decided by [`streaming_output_producer`] (the shared
/// plan-derived predicate); this function adds the runtime writer-
/// registry checks (the writer must be a registered single writer, not a
/// fan-out) and packages the owned per-task metadata.
///
/// Returns the list of streaming specs (one per qualifying Output). Empty
/// for pipelines that don't match the topology, leaving every Output on
/// the existing buffered path. See
/// https://github.com/rustpunk/clinker/issues/72 for the rationale.
pub(super) fn compute_streaming_output_specs(
    plan: &crate::plan::execution::ExecutionPlanDag,
    config: &PipelineConfig,
    fused_transforms: &HashSet<petgraph::graph::NodeIndex>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
    output_configs: &[OutputConfig],
    writers: &WriterRegistry,
) -> Vec<StreamingOutputSpec> {
    use crate::plan::execution::PlanNode;

    // Pipeline-wide correlation buffering disables streaming for every
    // Output — the CorrelationCommit terminal owns the actual writes.
    if config.any_source_has_correlation_key() {
        return Vec::new();
    }

    let mut specs: Vec<StreamingOutputSpec> = Vec::new();
    for output_idx in plan.graph.node_indices() {
        let PlanNode::Output {
            name: output_name, ..
        } = &plan.graph[output_idx]
        else {
            continue;
        };
        let Some(producer_idx) =
            streaming_output_producer(plan, output_idx, fused_transforms, init_phase_set)
        else {
            continue;
        };

        // Runtime writer-registry checks: the writer must be registered
        // as a single writer, never a fan-out. (The plan-derived
        // fan-out / split exclusions live in `streaming_output_producer`
        // so the explain surface agrees; this is the runtime-only
        // registry confirmation.)
        if !writers.single.contains_key(output_name) || writers.fan_out.contains_key(output_name) {
            continue;
        }
        let Some(out_cfg) = output_configs.iter().find(|o| &o.name == output_name) else {
            continue;
        };

        // Pre-compute the schema-check + projection metadata so the
        // spawned task carries owned `Arc<Schema>` / `Vec<String>` / the
        // upstream node name for E314 diagnostics, with no borrow back
        // into the plan.
        let expected_input_schema = plan.graph[output_idx]
            .expected_input_schema_in(plan)
            .cloned();
        let cxl_emit_names: Vec<String> = plan.graph[output_idx].cxl_emit_names_in(plan);

        specs.push(StreamingOutputSpec {
            producer_idx,
            output_idx,
            output_name: output_name.clone(),
            producer_name: plan.graph[producer_idx].name().to_string(),
            out_cfg: out_cfg.clone(),
            expected_input_schema,
            cxl_emit_names,
        });
    }
    specs
}

/// Plan-time metadata for a streaming `Output` thread spawned at executor
/// entry. Carries every field the thread needs in owned form so it can run
/// independently of the borrowed `&PipelineConfig` / `&ExecutionPlanDag`
/// references that anchor the dispatcher's `ExecutorContext`.
pub(crate) struct StreamingOutputSpec {
    /// `NodeIndex` of the upstream producer node whose arm writes records
    /// into the streaming channel — either a fused `Merge.interleave` or
    /// a fused `Source → Transform`. The producer arm looks up its sender
    /// in [`dispatch::ExecutorContext::streaming_output_senders`] keyed by
    /// this index.
    pub(crate) producer_idx: petgraph::graph::NodeIndex,
    /// `NodeIndex` of the downstream `Output` node. The Output arm
    /// short-circuits when its index appears in
    /// [`dispatch::ExecutorContext::streaming_output_nodes`].
    pub(crate) output_idx: petgraph::graph::NodeIndex,
    pub(crate) output_name: String,
    /// Name of the upstream producer (`Merge` or fused `Transform`), used
    /// as the upstream-node label in the streaming task's E314
    /// schema-mismatch diagnostics.
    pub(crate) producer_name: String,
    pub(crate) out_cfg: OutputConfig,
    /// Compile-time input schema for the Output; used by the streaming
    /// task to run the same `check_input_schema` invariant the buffered
    /// path enforces (E314 SchemaMismatch diagnostics).
    pub(crate) expected_input_schema: Option<Arc<Schema>>,
    /// Upstream `cxl_emit_names_in` result — passed to
    /// `project_output_from_record` so the streaming projection drops
    /// passthroughs the user didn't explicitly emit (matching the
    /// buffered path's `include_unmapped: false` semantic).
    pub(crate) cxl_emit_names: Vec<String>,
}

/// Per-thread return shape merged into the dispatcher's
/// [`dispatch::ExecutorContext`] after `JoinHandle::join`. Mirrors the
/// counter / timer / error accounting the buffered Output arm performs
/// inline; the streaming thread accumulates these locally and the
/// dispatcher folds them back into `ctx.counters`, `ctx.records_emitted`,
/// `ctx.write_timer`, `ctx.projection_timer`, `ctx.ok_source_rows`, and
/// `ctx.output_errors`.
pub(crate) struct StreamingOutputTaskOutput {
    pub(crate) records_written: u64,
    pub(crate) records_emitted: u64,
    pub(crate) seen_row_nums: HashSet<u64>,
    pub(crate) write_timer: stage_metrics::CumulativeTimer,
    pub(crate) projection_timer: stage_metrics::CumulativeTimer,
    pub(crate) errors: Vec<PipelineError>,
    pub(crate) stage_metrics: Vec<stage_metrics::StageMetrics>,
}

impl StreamingOutputTaskOutput {
    /// Fold the thread-local counters / timers / errors / stage metrics
    /// back into the dispatcher's [`dispatch::ExecutorContext`] after
    /// `JoinHandle::join`. Mirrors the buffered Output arm's inline
    /// counter accounting: `ok_count` counts distinct source rows
    /// (deduplicated against `ctx.ok_source_rows`), `records_written`
    /// counts every write, and the per-task timers accumulate via
    /// [`stage_metrics::CumulativeTimer::add`].
    pub(super) fn fold_into(self, ctx: &mut dispatch::ExecutorContext<'_>) {
        let mut newly_ok: u64 = 0;
        for rn in self.seen_row_nums {
            if ctx.ok_source_rows.insert(rn) {
                newly_ok += 1;
            }
        }
        ctx.counters.ok_count += newly_ok;
        ctx.counters.records_written += self.records_written;
        ctx.records_emitted += self.records_emitted;
        ctx.write_timer.add(self.write_timer);
        ctx.projection_timer.add(self.projection_timer);
        ctx.output_errors.extend(self.errors);
        for sm in self.stage_metrics {
            ctx.collector.record(sm);
        }
    }
}

/// Streaming-output writer thread body. Drains the crossbeam `Receiver`
/// populated by the fused `Merge.interleave` arm, projects each record
/// through `project_output_from_record`, lazily constructs the writer on
/// the first record, calls `Writer::write_record` per record, and
/// flushes at channel close. Errors are accumulated rather than aborting
/// the loop so the dispatcher can surface them alongside any sibling
/// `output_errors`. See [`StreamingOutputSpec`] for the eligibility
/// predicate and https://github.com/rustpunk/clinker/issues/72 for the
/// rationale.
pub(super) fn streaming_output(
    rx: crossbeam_channel::Receiver<crate::executor::stream_event::StreamEvent>,
    raw_writer: Box<dyn Write + Send>,
    spec: StreamingOutputSpec,
    charge_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
) -> StreamingOutputTaskOutput {
    use crate::projection::project_output_from_record;

    let mut out = StreamingOutputTaskOutput {
        records_written: 0,
        records_emitted: 0,
        seen_row_nums: HashSet::new(),
        write_timer: stage_metrics::CumulativeTimer::new(),
        projection_timer: stage_metrics::CumulativeTimer::new(),
        errors: Vec::new(),
        stage_metrics: Vec::new(),
    };
    let cxl_emit_names_opt: Option<&[String]> = if spec.cxl_emit_names.is_empty() {
        None
    } else {
        Some(&spec.cxl_emit_names)
    };

    let mut scan_timer_slot: Option<stage_metrics::StageTimer> = Some(
        stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan),
    );
    let mut writer: Option<Box<dyn FormatWriter>> = None;
    let mut raw_writer_slot: Option<Box<dyn Write + Send>> = Some(raw_writer);

    while let Ok(event) = rx.recv() {
        // Streaming output is terminal — it writes records straight to
        // disk. Punctuations are consumed here; per-document writer
        // finalization (envelope header/footer streaming reconstruction
        // on `DocumentClose`) is a follow-up filed under #91 backlog.
        let (record, rn) = match event {
            crate::executor::stream_event::StreamEvent::Record(r, rn) => (r, rn),
            crate::executor::stream_event::StreamEvent::Punctuation(_) => continue,
        };
        // Discharge this record's per-row cost from the shared charge
        // handle — the consume half of the per-batch admit/discharge
        // model. The producer charged the whole batch's
        // `EventBatch::estimated_bytes` on flush; subtracting each
        // record's `record_byte_cost` as it drains keeps the slot's live
        // count tracking exactly what is still buffered between producer
        // and writer. The formula matches the producer's charge, so a
        // fully-drained stream nets the counter back to zero.
        charge_handle.sub_bytes(crate::executor::node_buffer::record_byte_cost(
            record.schema().column_count(),
        ));
        if let Some(expected) = spec.expected_input_schema.as_ref()
            && let Err(err) = crate::executor::schema_check::check_input_schema(
                expected,
                record.schema(),
                &spec.output_name,
                "output",
                &spec.producer_name,
            )
        {
            out.errors.push(err);
            continue;
        }

        let projected = {
            let _guard = out.projection_timer.guard();
            project_output_from_record(&record, &spec.out_cfg, cxl_emit_names_opt)
        };

        // Lazy writer construction: defer until we have the first
        // record's projected schema so the writer's column list matches
        // what `project_output_from_record` actually emits (same source
        // of truth as the buffered Output arm at dispatch.rs's
        // `output_schema = Arc::clone(projected.schema())`).
        if writer.is_none() {
            let raw = raw_writer_slot
                .take()
                .expect("raw_writer_slot is Some until first record arrives");
            let schema = Arc::clone(projected.schema());
            match build_format_writer(&spec.out_cfg, raw, schema) {
                Ok(w) => {
                    writer = Some(w);
                    if let Some(timer) = scan_timer_slot.take() {
                        out.stage_metrics.push(timer.finish(1, 1));
                    }
                }
                Err(e) => {
                    out.errors.push(e);
                    if let Some(timer) = scan_timer_slot.take() {
                        out.stage_metrics.push(timer.finish(0, 0));
                    }
                    // Drain the rest of the channel so the Merge arm's
                    // bounded `send` doesn't deadlock — the streaming
                    // thread must keep consuming until every sender drops
                    // even when the writer is dead, otherwise the Merge
                    // producer blocks forever on a full bounded channel.
                    while rx.recv().is_ok() {}
                    // Nothing is buffered downstream once the channel
                    // drains, so zero the slot's charge unconditionally —
                    // the error path never discharged the records it
                    // dropped on the floor above.
                    charge_handle.set_bytes(0);
                    return out;
                }
            }
        }

        let write_result = {
            let _guard = out.write_timer.guard();
            writer
                .as_mut()
                .expect("writer is Some after lazy construction")
                .write_record(&projected)
        };
        match write_result {
            Ok(()) => {
                out.records_written += 1;
                out.records_emitted += 1;
                out.seen_row_nums.insert(rn);
            }
            Err(e) => {
                out.errors.push(PipelineError::from(e));
                while rx.recv().is_ok() {}
                charge_handle.set_bytes(0);
                return out;
            }
        }
    }

    // Channel closed — every sender dropped (`recv` returned `Err`),
    // so no more records will arrive. Flush whatever the writer has
    // buffered. `writer.is_none()` is the empty-stream case (zero
    // records arrived); matches the buffered path's
    // `if unbuffered.is_empty() { return Ok(()); }` short-circuit.
    if let Some(timer) = scan_timer_slot.take() {
        out.stage_metrics.push(timer.finish(0, 0));
    }
    if let Some(mut w) = writer {
        let flush_result = {
            let _guard = out.write_timer.guard();
            w.flush()
        };
        if let Err(e) = flush_result {
            out.errors.push(PipelineError::from(e));
        }
    }
    // The channel is fully drained, so nothing is in flight; the
    // per-record discharge above should have zeroed the counter already.
    // Pin it to zero defensively so a heuristic mismatch between the
    // batch charge and the per-record discharge can never leave a stale
    // positive charge for the post-join arbitrator read.
    charge_handle.set_bytes(0);
    out
}
