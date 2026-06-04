//! `PlanNode::Source` dispatch arm.
//!
//! Holds the input-reader body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: it consumes the
//! source's live crossbeam receiver (or body-port seed buffer),
//! canonicalizes each record onto the plan-time schema, seeds `$record.*`
//! and `$source.*` defaults, advances the per-source counter, and finalizes
//! node-rooted window arenas at EOF. The dispatcher's `Source` arm is a
//! single delegating call into [`dispatch_source`].

use std::sync::Arc;

use clinker_record::Record;
use petgraph::graph::NodeIndex;

use crate::error::PipelineError;
use crate::executor::dispatch::{
    ExecutorContext, MERGED_SOURCE_FILE, admit_node_buffer, build_engine_stamped_tail,
    canonicalize_to_source_schema, drain_node_buffer_slot, finalize_node_rooted_windows,
    node_buffer_spill_allowed, seed_source_vars_for_record, source_file_arc_of,
    tee_emit_to_region_input_buffers,
};
use crate::plan::execution::{ExecutionPlanDag, PlanNode};

/// Execute the `Source` arm for `node_idx`: drain the source's records from
/// its live receiver, body-port seed buffer, or fail loudly when the ingest
/// pass missed it; canonicalize onto the plan-time `Arc<Schema>`; seed
/// `$record.*` / `$source.*` defaults; advance the per-source counter; and
/// finalize every window arena rooted at this Source at EOF. Streaming:
/// records are consumed one at a time so back-pressure engages.
pub(crate) fn dispatch_source(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Source { ref name, .. } = *node else {
        unreachable!("dispatch_source called with non-Source node");
    };
    // Three input paths feed a Source's emit:
    //
    // 1. Source name in `ctx.fused_sources` — a downstream
    //    `Merge.interleave` arm has taken ownership of this
    //    Source's crossbeam `Receiver` and is consuming records
    //    directly via `crossbeam_channel::Select`. This arm
    //    returns cleanly without emitting; the fused Merge
    //    populates the merge node's buffer.
    // 2. Records already seeded into `ctx.node_buffers[node_idx]`
    //    by the body executor at composition entry —
    //    composition input ports surface as synthetic Source
    //    nodes owning the records the parent scope harvested.
    // 3. The live crossbeam `Receiver` in `source_records[name]`,
    //    consumed via `recv` per record until the paired ingest
    //    thread drops its sender. Per record: canonicalize onto
    //    the source's plan-time schema, seed `$record.<key>`
    //    defaults, seed `$source.<key>` defaults per
    //    `(source, file_arc)`, advance the per-source running
    //    counter. On `recv` returning `Err` (channel
    //    disconnected), stamp the finalized per-source count and
    //    call `finalize_node_rooted_windows` so every spec rooted
    //    at this Source's `NodeIndex` lands its arena.
    //
    // Records are canonicalized onto the Source's plan-time
    // `Arc<Schema>` so every downstream operator hits the
    // `Arc::ptr_eq` fast path on the first record. Structural
    // equality holds by construction.
    if ctx.fused_sources.contains(name.as_str()) {
        return Ok(());
    }
    let source_schema = current_dag.graph[node_idx].stored_output_schema().cloned();
    let engine_stamped: Vec<(usize, Box<str>)> = source_schema
        .as_ref()
        .map(build_engine_stamped_tail)
        .unwrap_or_default();
    let canonicalize = |r: &Record| -> Record {
        match source_schema.as_ref() {
            Some(target) => canonicalize_to_source_schema(r, target, &engine_stamped),
            None => r.clone(),
        }
    };

    let (records, source_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = if let Some(seeded) = drain_node_buffer_slot(ctx, node_idx) {
        // Body-context port source — records were seeded by
        // `execute_composition_body` from parent-scope
        // output. The seeded records still carry the parent
        // producer's `Arc<Schema>`, so canonicalize them
        // onto this port source's schema before downstream
        // consumers run. Apply per-record seeding for body-
        // declared record_vars (parent's writes survive via
        // `seed_record_vars`'s preserve-existing semantics).
        // Punctuations on the seeded port forward through to
        // the Source's output buffer so downstream operators
        // see the original document boundaries.
        let mut out_records: Vec<(Record, u64)> = Vec::with_capacity(seeded.len_hint());
        let mut out_puncts: Vec<crate::executor::stream_event::Punctuation> = Vec::new();
        let has_record_seed = !ctx.record_var_seed.is_empty();
        for event in seeded.drain() {
            match event? {
                crate::executor::stream_event::StreamEvent::Record(r, rn) => {
                    let mut rec = canonicalize(&r);
                    if has_record_seed {
                        rec.seed_record_vars(ctx.record_var_seed);
                    }
                    seed_source_vars_for_record(ctx, name.as_str(), &rec)?;
                    out_records.push((rec, rn));
                }
                crate::executor::stream_event::StreamEvent::Punctuation(p) => {
                    out_puncts.push(p);
                }
            }
        }
        (out_records, out_puncts)
    } else if let Some(rx) = ctx.source_records.remove(name.as_str()) {
        // Live channel: consume per record so back-pressure
        // engages — a slow upstream Source no longer blocks
        // peers' channels from filling, and watermark
        // observations on a different source's records
        // flow through the dispatcher in the meantime
        // wherever the executor task scheduler interleaves.
        let timeout = ctx.idle_timeouts.get(name.as_str()).copied();
        let has_record_seed = !ctx.record_var_seed.is_empty();
        let source_name_arc: Arc<str> = Arc::from(name.as_str());
        let mut drained: Vec<(Record, u64)> = Vec::new();
        let mut drained_puncts: Vec<crate::executor::stream_event::Punctuation> = Vec::new();
        // Tracked so an idle-timeout flips THAT file's
        // partition to `Idle`. Before any record arrives the
        // consumer uses the synthetic [`MERGED_SOURCE_FILE`]
        // Arc, matching the engine-stamp path for record-
        // less source contexts.
        let mut last_file: Arc<str> = Arc::clone(&MERGED_SOURCE_FILE);
        let mut count: u64 = 0;
        let mut records_since_check: u32 = 0;
        loop {
            let item: Option<crate::executor::stream_event::StreamEvent> = match timeout {
                Some(t) => match rx.recv_timeout(t) {
                    Ok(item) => Some(item),
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        // Quiet for longer than
                        // `idle_timeout` — flip the
                        // partition tracked by `last_file`
                        // to `Idle`. The next record un-
                        // idles via `observe`. Idempotent on
                        // repeat timeouts.
                        ctx.watermarks.mark_idle(name.as_str(), &last_file);
                        continue;
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => None,
                },
                None => rx.recv().ok(),
            };
            match item {
                Some(crate::executor::stream_event::StreamEvent::Record(record, rn)) => {
                    last_file = source_file_arc_of(&record);
                    let mut rec = canonicalize(&record);
                    if has_record_seed {
                        rec.seed_record_vars(ctx.record_var_seed);
                    }
                    seed_source_vars_for_record(ctx, name.as_str(), &rec)?;
                    if let Some(slot) = ctx.total_per_source.get_mut(&source_name_arc) {
                        *slot += 1;
                    }
                    count += 1;
                    records_since_check += 1;
                    if records_since_check >= 1024 {
                        records_since_check = 0;
                        ctx.check_shutdown()?;
                    }
                    drained.push((rec, rn));
                }
                Some(crate::executor::stream_event::StreamEvent::Punctuation(p)) => {
                    drained_puncts.push(p);
                }
                None => break,
            }
        }
        ctx.finalize_source_count(&source_name_arc, count);
        if timeout.is_some() && ctx.watermarks.is_idle(name.as_str()) {
            tracing::debug!(
                target: "clinker::watermark",
                source = %name,
                "source consumer ended with all partitions in WatermarkStatus::Idle"
            );
        }
        (drained, drained_puncts)
    } else {
        // Defense-in-depth: a Source reaching this arm with
        // neither a body-port seed, nor an entry in
        // `source_records`, nor fused-bit set means the
        // executor's unified ingest pass missed it. Every
        // declared Source ingests through the same
        // `source_records` map (no primary fallthrough), so
        // any miss surfaces here as a loud internal error
        // rather than the silent-corruption surface at the
        // root of #47.
        return Err(PipelineError::Internal {
            op: "executor",
            node: name.clone(),
            detail: format!(
                "Source '{name}' has no ingested records; \
                         the executor's source-ingest pass missed this Source — \
                         likely a planner regression introducing a Source topology \
                         the ingest pass doesn't enumerate.",
            ),
        });
    };
    // Build node-rooted arenas anchored at this Source's
    // `NodeIndex`. Replaces the prologue's Phase-0 build:
    // every spec previously rooted at `PlanIndexRoot::Source`
    // now anchors here.
    finalize_node_rooted_windows(ctx, current_dag, node_idx, &records)?;
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &records)?;
    let nb = admit_node_buffer(
        ctx,
        name,
        node_idx,
        records,
        source_puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx, nb);

    Ok(())
}
