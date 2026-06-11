//! `PlanNode::Merge` dispatch arm.
//!
//! Holds the streamwise-concatenation body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: declaration-order
//! concat, round-robin interleave, and the fused all-Source interleave fast
//! path that takes ownership of the parked source receivers and runs a fair
//! `crossbeam_channel::Select` for end-to-end back-pressure. The
//! dispatcher's `Merge` arm is a single delegating call into
//! [`dispatch_merge`].

use std::sync::Arc;

use clinker_record::Record;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, MergeStreamHandoff, admit_node_buffer, drain_node_buffer_slot,
    finalize_node_rooted_windows, merge_fused_interleave, node_buffer_spill_allowed,
    stream_linear_producer_emit, tee_emit_to_region_input_buffers,
};
use crate::executor::schema_check::check_input_schema;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Execute the `Merge` arm for `node_idx`: concatenate predecessor buffers
/// in declaration order (Concat), round-robin across them (Interleave), or —
/// when every predecessor is a Source under `mode: interleave` — drive the
/// fused `crossbeam_channel::Select` path. Streaming concatenation; emits
/// every record onto the canonical merge output schema for `Arc::ptr_eq`.
pub(crate) fn dispatch_merge(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Merge { ref name, .. } = *node else {
        unreachable!("dispatch_merge called with non-Merge node");
    };
    // Two architectural modes for a Merge arm:
    //
    // 1. **Fused** — every direct predecessor is a Source and
    //    `mode: interleave`. The pre-pass at executor entry
    //    marked each predecessor's source name in
    //    `ctx.fused_sources`, the Source dispatch arms
    //    returned cleanly without consuming, and the receivers
    //    are still parked in `ctx.source_records`. This arm
    //    takes ownership of those receivers and runs a fair
    //    `crossbeam_channel::Select` over them so a slow Source
    //    no longer blocks peers — back-pressure flows
    //    end-to-end.
    //
    // 2. **Non-fused** — concat mode, or a mix of Source and
    //    non-Source predecessors. Predecessor arms have
    //    already populated `ctx.node_buffers`; this arm
    //    consumes those buffers in declaration order (Concat)
    //    or round-robins across them (Interleave).
    //
    // Per-source FIFO survives both modes; per-record schema
    // canonicalization runs at consume time so the canonical
    // `Arc<Schema>` reaches downstream operators uniformly.
    let predecessors: Vec<NodeIndex> = current_dag
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();

    let (declaration_order, mode, interleave_seed): (
        Vec<String>,
        clinker_plan::config::MergeMode,
        Option<u64>,
    ) = {
        use clinker_plan::config::PipelineNode;
        use clinker_plan::config::node_header::NodeInput;
        ctx.config
            .nodes
            .iter()
            .find_map(|spanned| match &spanned.value {
                PipelineNode::Merge { header, config, .. } if header.name == *name => {
                    let order = header
                        .inputs
                        .iter()
                        .map(|ni| match &ni.value {
                            NodeInput::Single(s) => s.clone(),
                            NodeInput::Port { node, port } => {
                                format!("{node}.{port}")
                            }
                        })
                        .collect();
                    Some((order, config.mode, config.interleave_seed))
                }
                _ => None,
            })
            .unwrap_or_else(|| (Vec::new(), clinker_plan::config::MergeMode::Concat, None))
    };

    // Sort predecessors by declaration order
    let mut sorted_preds = predecessors.clone();
    sorted_preds.sort_by_key(|p| {
        let pred_name = current_dag.graph[*p].name();
        declaration_order
            .iter()
            .position(|d| d == pred_name)
            .unwrap_or(usize::MAX)
    });

    let merge_output_schema = current_dag.graph[node_idx].stored_output_schema().cloned();
    // Detect fused mode: every predecessor is a Source whose
    // name is in `ctx.fused_sources`. The pre-pass at executor
    // entry already validated mode == Interleave for these,
    // but we double-check here defensively.
    let fused_mode = matches!(mode, clinker_plan::config::MergeMode::Interleave)
        && sorted_preds.iter().all(|p| match &current_dag.graph[*p] {
            PlanNode::Source { name: src_name, .. } => {
                ctx.fused_sources.contains(src_name.as_str())
            }
            _ => false,
        });

    // Punctuation collection state hoisted above the if-else so
    // the dedup pass below the merge body can read it. The
    // fused-mode merge_fused_interleave path does not yet
    // forward punctuations (a follow-up will route them
    // through merge_fused_interleave's recv loop); the else
    // branch's drain logic fills `all_puncts` from each input
    // NodeBuffer.
    let mut all_puncts: Vec<crate::executor::stream_event::Punctuation> = Vec::new();

    // Streaming-Output handoff: if a single Output downstream of
    // this Merge passed the eligibility predicate at executor
    // entry, its crossbeam `Sender` was installed under our
    // `node_idx`. Take it here so the Merge arm streams every
    // record through the channel instead of accumulating, and so
    // dropping the sender at clean exit disconnects the streaming
    // thread's `recv` loop. The fused-interleave path streams
    // inside `merge_fused_interleave` (the sender moves there);
    // the non-fused path (concat, or interleave with non-Source
    // inputs) accumulates `merged` then streams it through
    // `stream_linear_producer_emit` below, skipping the
    // materialized `admit_node_buffer` slot.
    let streaming_sender = ctx.take_streaming_sender(node_idx);
    // The per-batch charge handle for the streaming slot, shared by
    // both fused and non-fused streaming paths. `None` when this
    // Merge is materialized (no streaming sender installed).
    let merge_charge = streaming_sender.as_ref().and_then(|_| {
        let spill_allowed = node_buffer_spill_allowed(current_dag, node_idx);
        ctx.streaming_charge_handle(node_idx, name, spill_allowed)
    });
    let merge_batch_size = ctx.batch_size;
    // Held only on the non-fused streaming path; the fused path
    // consumes its sender inside `merge_fused_interleave`.
    let mut nonfused_sender: Option<
        crossbeam_channel::Sender<crate::executor::stream_event::StreamEvent>,
    > = None;
    let merged: Vec<(Record, u64)> = if fused_mode {
        let handoff = match (streaming_sender, merge_charge.as_ref()) {
            (Some(sender), Some(charge)) => Some(MergeStreamHandoff {
                sender,
                charge,
                batch_size: merge_batch_size,
            }),
            _ => None,
        };
        merge_fused_interleave(
            ctx,
            current_dag,
            name,
            &sorted_preds,
            merge_output_schema.as_ref(),
            handoff,
        )?
    } else {
        nonfused_sender = streaming_sender;
        let total: usize = sorted_preds
            .iter()
            .map(|p| ctx.node_buffers.get(p).map_or(0, |b| b.len_hint()))
            .sum();
        let mut merged = Vec::with_capacity(total);
        let emit = |merged: &mut Vec<(Record, u64)>,
                    upstream_name: &str,
                    mut record: Record,
                    rn: u64|
         -> Result<(), PipelineError> {
            if let Some(canonical) = merge_output_schema.as_ref() {
                check_input_schema(canonical, record.schema(), name, "merge", upstream_name)?;
                // Rebuild record with the Merge's
                // `Arc<Schema>` so downstream operators hit
                // the ptr_eq fast path regardless of which
                // input the record originated from. Carry the
                // per-record envelope context across the
                // rebuild — each merged row keeps the document
                // it came from.
                let doc_ctx = Arc::clone(record.doc_ctx());
                let values = record.values().to_vec();
                record = Record::new(Arc::clone(canonical), values);
                record.set_doc_ctx(doc_ctx);
            }
            merged.push((record, rn));
            Ok(())
        };

        // Every input's punctuations accumulate in the outer
        // `all_puncts`; the post-merge dedup pass collapses
        // multiple-input duplicates so each `DocumentOpen` and
        // `DocumentClose` traverses downstream exactly once.
        match mode {
            clinker_plan::config::MergeMode::Concat => {
                for pred in &sorted_preds {
                    let upstream_name = current_dag.graph[*pred].name().to_string();
                    if let Some(buf) = drain_node_buffer_slot(ctx, *pred) {
                        for event in buf.drain() {
                            match event? {
                                crate::executor::stream_event::StreamEvent::Record(record, rn) => {
                                    emit(&mut merged, &upstream_name, record, rn)?;
                                }
                                crate::executor::stream_event::StreamEvent::Punctuation(p) => {
                                    all_puncts.push(p);
                                }
                            }
                        }
                    }
                }
            }
            clinker_plan::config::MergeMode::Interleave => {
                // Round-robin across pre-buffered predecessor
                // outputs. Reached only when predecessors are
                // not all Sources (the fused path took
                // those); the inputs are produced by upstream
                // operator arms that wrote to `node_buffers`.
                use std::collections::VecDeque;
                let upstream_names: Vec<String> = sorted_preds
                    .iter()
                    .map(|p| current_dag.graph[*p].name().to_string())
                    .collect();
                let mut deques: Vec<VecDeque<(Record, u64)>> = sorted_preds
                    .iter()
                    .map(|pred| -> Result<VecDeque<(Record, u64)>, PipelineError> {
                        match drain_node_buffer_slot(ctx, *pred) {
                            Some(nb) => {
                                let (records, puncts) = nb.drain_split()?;
                                all_puncts.extend(puncts);
                                Ok(VecDeque::from(records))
                            }
                            None => Ok(VecDeque::new()),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut rng = interleave_seed.map(fastrand::Rng::with_seed);
                let mut cursor = 0usize;
                loop {
                    let ready: Vec<usize> = deques
                        .iter()
                        .enumerate()
                        .filter_map(|(i, d)| (!d.is_empty()).then_some(i))
                        .collect();
                    if ready.is_empty() {
                        break;
                    }
                    let pick_idx = match rng.as_mut() {
                        Some(r) => ready[r.usize(0..ready.len())],
                        None => {
                            let mut chosen = ready[0];
                            for _ in 0..deques.len() {
                                let candidate = cursor % deques.len();
                                cursor = cursor.wrapping_add(1);
                                if !deques[candidate].is_empty() {
                                    chosen = candidate;
                                    break;
                                }
                            }
                            chosen
                        }
                    };
                    if let Some((record, rn)) = deques[pick_idx].pop_front() {
                        emit(&mut merged, &upstream_names[pick_idx], record, rn)?;
                    }
                }
            }
        }
        merged
    };

    // Reconcile boundaries across every Merge input: one `DocumentOpen`
    // per document (first sighting wins) and one `DocumentClose` once
    // every input that opened a document has closed it. A document
    // carried by a single input branch forwards its close after that
    // branch's close, so a Merge of distinct single-document sources
    // lets each document flush independently downstream.
    let deduped_puncts = crate::executor::stream_event::reconcile_document_boundaries(all_puncts);

    // Non-fused streaming path: the predecessors' slots are
    // already drained into the full `merged` Vec, so this does not
    // shrink the Merge's own working set; what it saves is the
    // second copy — hand `merged` straight to the downstream
    // Output thread over the bounded channel rather than admitting
    // a charged `node_buffers` slot the Output would re-drain, and
    // overlap the writer with the next topo node. (The fused
    // Merge.interleave path is the true one-batch streamer; it
    // forwards records off the live Source channels inside
    // `merge_fused_interleave` and returns an empty `merged`, its
    // sender already dropped there.) The eligibility predicate
    // certified this Merge roots no window and tees to no deferred
    // region, so the helper calls below are correctly skipped.
    // Dropping `nonfused_sender` at the end of this branch
    // disconnects the writer thread's `recv` loop.
    if let Some(sender) = nonfused_sender {
        let charge = merge_charge
            .as_ref()
            .expect("streaming sender implies a registered charge handle");
        stream_linear_producer_emit(
            &sender,
            merge_batch_size,
            name,
            merged,
            deduped_puncts,
            charge,
        )?;
        return Ok(());
    }

    // Merge is rejected as a window root at compile time
    // (E150d) because the multi-producer concatenation has no
    // single producer identity for record_pos to anchor
    // against. The call below is a defense-in-depth no-op:
    // the helper iterates plan.indices_to_build and matches
    // nothing because no spec roots at a Merge NodeIndex.
    finalize_node_rooted_windows(ctx, current_dag, node_idx, &merged)?;
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &merged)?;
    let nb = admit_node_buffer(
        ctx,
        name,
        node_idx,
        merged,
        deduped_puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx, nb);

    Ok(())
}
