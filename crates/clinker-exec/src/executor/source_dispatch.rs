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

use crate::executor::dispatch::{
    ExecutorContext, MERGED_SOURCE_FILE, NodeBufferKey, admit_node_buffer,
    admit_node_buffer_transferred, build_engine_stamped_tail, canonicalize_to_source_schema,
    estimate_node_buffer_bytes, finalize_node_rooted_windows, node_buffer_spill_allowed,
    require_node_buffer_input_transferred, seed_source_vars_for_record, source_file_arc_of,
    tee_emit_to_region_input_buffers,
};
use crate::executor::node_buffer::TransientNodeBufferReservation;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Context carrier kept lazy until the node-kind guard has succeeded. Normal
/// dispatch passes the live executor context directly; the feature-gated
/// mismatch matrix uses the inert carrier to prove rejection precedes source
/// receiver, cursor, and buffer access.
pub(crate) enum SourceDispatchContext<'borrow, 'plan> {
    Live(&'borrow mut ExecutorContext<'plan>),
    #[cfg(feature = "test-utils")]
    Inert,
}

impl<'borrow, 'plan> From<&'borrow mut ExecutorContext<'plan>>
    for SourceDispatchContext<'borrow, 'plan>
{
    fn from(ctx: &'borrow mut ExecutorContext<'plan>) -> Self {
        Self::Live(ctx)
    }
}

#[cfg(feature = "test-utils")]
impl crate::executor::dispatch::DispatchFaultGuard {
    /// Execute the real source boundary with an inert context so tests can
    /// prove a wrong node returns before source state is touched.
    #[doc(hidden)]
    pub fn dispatch_source_mismatch_for_testing(
        current_dag: &ExecutionPlanDag,
        node_idx: NodeIndex,
        node: &PlanNode,
    ) -> Result<(), PipelineError> {
        dispatch_source(SourceDispatchContext::Inert, current_dag, node_idx, node)
    }
}

/// Execute the `Source` arm for `node_idx`: drain the source's records from
/// its live receiver, body-port seed buffer, or fail loudly when the ingest
/// pass missed it; canonicalize onto the plan-time `Arc<Schema>`; seed
/// `$record.*` / `$source.*` defaults; advance the per-source counter; and
/// finalize every window arena rooted at this Source at EOF. Streaming:
/// records are consumed one at a time so back-pressure engages.
pub(crate) fn dispatch_source<'borrow, 'plan>(
    ctx: impl Into<SourceDispatchContext<'borrow, 'plan>>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError>
where
    'plan: 'borrow,
{
    let PlanNode::Source {
        ref name,
        id,
        ref resolved,
        ..
    } = *node
    else {
        return Err(crate::executor::invariant::dispatch_mismatch(
            "dispatch_source",
            "source",
            node.kind_name(),
            node.name(),
        ));
    };
    #[cfg(feature = "test-utils")]
    let SourceDispatchContext::Live(ctx) = ctx.into() else {
        panic!("source dispatcher accessed inert context after accepting a source node")
    };
    #[cfg(not(feature = "test-utils"))]
    let SourceDispatchContext::Live(ctx) = ctx.into();

    // An authored body Source carries a resolved reader contract; a synthetic
    // input-port Source does not. Activate the exact sealed group on the first
    // authored member encountered. Group activation may install receivers for
    // several fused members, but it opens every session before publishing any
    // receiver to this body scope.
    if resolved.is_some()
        && let Some(body_id) = ctx.window_runtime.active_stack.last().copied()
    {
        let body_scope = ctx
            .composition_bodies
            .get(&body_id)
            .ok_or_else(|| PipelineError::compose_body_missing(name.clone()))?
            .body_scope;
        let instance = clinker_plan::plan::execution::CompiledSourceInstanceId {
            scope: clinker_plan::plan::execution::CompiledSourceScope::CompositionBody(body_scope),
            source_node: id,
        };
        let mut controller =
            ctx.source_activation
                .take()
                .ok_or_else(|| PipelineError::Internal {
                    op: "source-activation",
                    node: ctx.qualified_node_name(name).into_owned(),
                    detail: "body Source reached execution without admitted capabilities"
                        .to_string(),
                })?;
        let logical_prefix = ctx.composition_call_sites.join(".");
        let activated = controller.activate(
            instance,
            current_dag,
            &logical_prefix,
            &ctx.memory_budget,
            ctx.shutdown_token.clone(),
            ctx.telemetry_producer.as_ref(),
        );
        ctx.source_activation = Some(controller);
        if let Some(activated) = activated? {
            for (source_name, receiver) in activated.receivers {
                if ctx
                    .source_records
                    .insert(source_name.clone(), receiver)
                    .is_some()
                {
                    return Err(PipelineError::Internal {
                        op: "source-activation",
                        node: source_name,
                        detail: "body Source receiver was activated more than once".to_string(),
                    });
                }
            }
            for (source_name, registration) in activated.consumers {
                if let Some((id, handle)) = ctx
                    .source_consumers
                    .insert(source_name.clone(), registration)
                {
                    handle.resume();
                    handle.set_bytes(0);
                    ctx.memory_budget.unregister_consumer(id);
                    return Err(PipelineError::Internal {
                        op: "source-activation",
                        node: source_name,
                        detail: "body Source memory consumer was activated more than once"
                            .to_string(),
                    });
                }
            }
        }
    }
    let source_identity: Arc<str> = if !ctx.window_runtime.active_stack.is_empty() {
        Arc::from(ctx.qualified_node_name(name).into_owned())
    } else {
        Arc::from(name.as_str())
    };
    ctx.source_count_per_source
        .entry(Arc::clone(&source_identity))
        .or_insert(None);
    ctx.total_per_source
        .entry(Arc::clone(&source_identity))
        .or_insert(0);
    ctx.dlq_per_source
        .entry(Arc::clone(&source_identity))
        .or_insert(0);
    // Three input paths feed a Source's emit:
    //
    // 1. Records already seeded into `ctx.node_buffers[node_idx]`
    //    by the body executor at composition entry —
    //    composition input ports surface as synthetic Source
    //    nodes owning the records the parent scope harvested.
    // 2. Source name in `ctx.fused_sources` with no seeded own
    //    slot — a downstream top-level `Merge.interleave` or
    //    Transform arm has taken ownership of this Source's
    //    crossbeam `Receiver` and consumes it directly. This arm
    //    returns cleanly without emitting; the fused consumer
    //    owns the Source boundary work and downstream emission.
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
    let source_slot_key = NodeBufferKey::from(node_idx);
    let has_seeded_own_slot = ctx.node_buffers.contains_key(&source_slot_key);
    if !has_seeded_own_slot && ctx.fused_sources.contains(name.as_str()) {
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

    let (records, source_puncts, transferred_reservation): (
        Vec<(Record, crate::executor::stream_event::SourceRowId)>,
        Vec<crate::executor::stream_event::Punctuation>,
        Option<TransientNodeBufferReservation>,
    ) = if has_seeded_own_slot {
        let seeded = require_node_buffer_input_transferred(
            ctx,
            source_slot_key.clone(),
            name,
            "composition input",
            None,
        )?;
        let (seeded, reservation) = seeded.into_parts();
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
        //
        // Remove the body-local registration from the slot map without
        // unregistering it, then resume RAII ownership locally. The seeded
        // event vector and canonicalized records vector overlap during this
        // loop, so reserve the prospective canonicalized footprint before
        // allocating the output. Once the old vector drops, reduce the same
        // handle to the actual canonicalized footprint; admission later
        // atomically changes only the wrapper while retaining this id and
        // handle.
        let Some(reservation) = reservation else {
            return Err(PipelineError::Internal {
                op: "executor",
                node: name.clone(),
                detail: "composition port Source had a seeded node buffer without its memory reservation"
                    .to_string(),
            });
        };
        let prospective_bytes = source_schema
            .as_ref()
            .map(|schema| seeded.estimated_materialized_bytes_for_columns(schema.column_count()))
            .unwrap_or_else(|| seeded.estimated_materialized_bytes());
        reservation.reserve_additional(prospective_bytes, name)?;
        let mut out_records: Vec<(Record, crate::executor::stream_event::SourceRowId)> =
            Vec::with_capacity(seeded.len_hint());
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
        reservation.set_bytes(estimate_node_buffer_bytes(&out_records));
        (out_records, out_puncts, Some(reservation))
    } else if let Some(rx) = ctx.source_records.remove(name.as_str()) {
        // Live channel: consume per record so back-pressure
        // engages — a slow upstream Source no longer blocks
        // peers' channels from filling, and watermark
        // observations on a different source's records
        // flow through the dispatcher in the meantime
        // wherever the executor task scheduler interleaves.
        let timeout = ctx.idle_timeouts.get(name.as_str()).copied();
        let has_record_seed = !ctx.record_var_seed.is_empty();
        let source_name_arc = Arc::clone(&source_identity);
        let mut drained: Vec<(Record, crate::executor::stream_event::SourceRowId)> = Vec::new();
        let mut drained_puncts: Vec<crate::executor::stream_event::Punctuation> = Vec::new();
        // Tracked so an idle-timeout flips THAT file's
        // partition to `Idle`. Before any record arrives the
        // consumer uses the synthetic [`MERGED_SOURCE_FILE`]
        // Arc, matching the engine-stamp path for record-
        // less source contexts.
        let mut last_file: Arc<str> = Arc::clone(&MERGED_SOURCE_FILE);
        let mut count: u64 = 0;
        let mut records_since_check: u32 = 0;
        // Resume-on-entry + active-exemption: unpark this source if a prior
        // arbitration round paused it, and mark it active so the resume
        // controller never pauses the producer feeding the `recv()` below.
        // `release_source_consumer` at drain end clears the flag and resumes.
        ctx.activate_source_for_drain(name.as_str());
        loop {
            // About to wait on an empty channel: publish the staged tail
            // first. A source that trickles never reaches the 1024-record
            // boundary below, and a count frozen at zero while the run is
            // merely slow is the reading a supervisor must not be given.
            // Costs nothing while records are flowing, because the channel
            // is not empty then.
            if rx.is_empty() {
                crate::executor::dispatch::publish_record_progress(ctx);
            }
            let item: Option<crate::executor::source_stream::SourceStreamEvent> = match timeout {
                Some(t) => match rx.recv_timeout(t) {
                    Ok(item) => Some(item),
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        // Quiet for longer than
                        // `idle_timeout` — flip the
                        // partition tracked by `last_file`
                        // to `Idle`. The next record un-
                        // idles via `observe`. Idempotent on
                        // repeat timeouts.
                        ctx.watermarks
                            .mark_idle(source_identity.as_ref(), &last_file);
                        continue;
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => None,
                },
                None => rx.recv().ok(),
            };
            let consumed = item
                .map(|event| {
                    crate::executor::dispatch::consume_source_event(ctx, &source_name_arc, event)
                })
                .transpose()?;
            match consumed {
                Some(crate::executor::dispatch::ConsumedSourceEvent::Record(record, rn)) => {
                    last_file = source_file_arc_of(&record);
                    let mut rec = canonicalize(&record);
                    if has_record_seed {
                        rec.seed_record_vars(ctx.record_var_seed);
                    }
                    seed_source_vars_for_record(ctx, name.as_str(), &rec)?;
                    count += 1;
                    records_since_check += 1;
                    if records_since_check >= 1024 {
                        records_since_check = 0;
                        ctx.check_shutdown()?;
                    }
                    drained.push((rec, rn));
                }
                Some(crate::executor::dispatch::ConsumedSourceEvent::Rejected) => {
                    count += 1;
                    records_since_check += 1;
                    if records_since_check >= 1024 {
                        records_since_check = 0;
                        ctx.check_shutdown()?;
                    }
                }
                Some(crate::executor::dispatch::ConsumedSourceEvent::Punctuation(p)) => {
                    // Mark a structural-count close failed BEFORE forwarding it,
                    // so the Output arm's per-file buffer rejects the file at
                    // this close rather than flushing it.
                    crate::executor::document_dlq::mark_structural_reject_if_present(ctx, &p);
                    drained_puncts.push(p);
                }
                Some(crate::executor::dispatch::ConsumedSourceEvent::Population) => {}
                None => break,
            }
        }
        ctx.finalize_source_count(&source_name_arc, count);
        ctx.release_source_consumer(name.as_str());
        if timeout.is_some() && ctx.watermarks.is_idle(source_identity.as_ref()) {
            tracing::debug!(
                target: "clinker::watermark",
                source = %name,
                "source consumer ended with all partitions in WatermarkStatus::Idle"
            );
        }
        (drained, drained_puncts, None)
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
    let spill_allowed = node_buffer_spill_allowed(current_dag, node_idx);
    if let Some(reservation) = transferred_reservation {
        admit_node_buffer_transferred(
            ctx,
            current_dag,
            name,
            node_idx,
            records,
            source_puncts,
            reservation,
        )?;
    } else {
        admit_node_buffer(
            ctx,
            current_dag,
            name,
            node_idx,
            records,
            source_puncts,
            spill_allowed,
        )?;
    }

    Ok(())
}
