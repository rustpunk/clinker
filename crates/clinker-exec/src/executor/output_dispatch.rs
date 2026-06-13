//! `PlanNode::Output` dispatch arm.
//!
//! Holds the sink-writer body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: writer init, output
//! schema mapping, `include_unmapped` passthrough, the per-record fan-out
//! to source-file-keyed writers, the correlation-buffer capture path, and
//! the streaming-fused output short-circuit. The dispatcher's `Output` arm
//! is a single delegating call into [`dispatch_output`].

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use clinker_record::{GroupByKey, Record};
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    CorrelationRecordSlot, ExecutorContext, buffer_key_for_record, drain_node_buffer_slot,
    push_write_error, source_file_path_of,
};
use crate::executor::schema_check::check_input_schema;
use crate::executor::{build_format_writer, stage_metrics};
use crate::projection::project_output_from_record;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Resolve the [`OutputConfig`](clinker_plan::config::OutputConfig) for the
/// Output named `name`, falling back to the pipeline's primary output when no
/// per-name entry exists. Borrows `ctx` immutably, so the caller resolves it
/// before taking any `&mut ctx`.
fn resolve_out_cfg<'a>(
    ctx: &'a ExecutorContext<'_>,
    name: &str,
) -> &'a clinker_plan::config::OutputConfig {
    ctx.output_configs
        .iter()
        .find(|o| o.name == *name)
        .unwrap_or(ctx.primary_output)
}

/// The per-Output input-binding values all three Output dispatch arms derive
/// from the plan before writing: the expected input schema (for the
/// schema-check), the upstream node name (for E314 diagnostics), and the CXL
/// emit names (for `include_unmapped: false` projection). Owned so the caller
/// can hold them across the `&mut ctx` write phase.
struct OutputInputs {
    expected_input_schema: Option<Arc<clinker_record::Schema>>,
    upstream_name: String,
    cxl_emit_names: Vec<String>,
}

/// Derive the [`OutputInputs`] for `node_idx` from the plan. Shared by the
/// records-only, document-DLQ, and envelope Output arms so the input-binding
/// preamble lives in one place.
fn resolve_output_inputs(current_dag: &ExecutionPlanDag, node_idx: NodeIndex) -> OutputInputs {
    OutputInputs {
        expected_input_schema: current_dag.graph[node_idx]
            .expected_input_schema_in(current_dag)
            .cloned(),
        upstream_name: current_dag
            .graph
            .neighbors_directed(node_idx, Direction::Incoming)
            .next()
            .map(|i| current_dag.graph[i].name().to_string())
            .unwrap_or_default(),
        cxl_emit_names: current_dag.graph[node_idx].cxl_emit_names_in(current_dag),
    }
}

/// Execute the `Output` arm for `node_idx`: open the writer(s), map records
/// onto the declared output schema (passing unmapped fields through when
/// configured), and write — taking the per-record fan-out path for
/// source-file-keyed outputs, the correlation-buffer capture path under a
/// correlation-key pipeline, and the streaming-fused short-circuit when a
/// streaming sender was installed. Stateless and streaming per record.
pub(crate) fn dispatch_output(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Output { ref name, .. } = *node else {
        unreachable!("dispatch_output called with non-Output node");
    };
    // Streaming-Output short-circuit (issue #72). The executor
    // entry already moved this Output's writer into a
    // `std::thread` that drained records from a bounded crossbeam
    // channel populated by the fused Merge arm. Per-record
    // `write_record` already fired concurrently with Merge
    // production; the dispatcher's end-of-DAG join surface joins
    // the thread and folds its counters / timers / errors into
    // the context. The Output's topo turn here is a no-op.
    if ctx.streaming_output_nodes.contains(&node_idx) {
        return Ok(());
    }
    // Document-level DLQ short-circuit. When any source declares
    // `dlq_granularity: document`, this Output's records are decided
    // per-document: buffered until each `DocumentClose`, then flushed clean
    // to the writer or rejected (trigger + collateral) to the DLQ. The
    // driver consumes the INTERLEAVED event stream (records + closes in
    // order), so this path reads the boundary the records-only `drain_split`
    // below discards — an additive read of the same buffer, not a change to
    // the records-vs-puncts split contract every other operator relies on.
    if ctx.document_dlq.is_some() {
        return dispatch_output_document_dlq(ctx, current_dag, node_idx, name);
    }
    // Envelope-reconstruction short-circuit. When this Output declares
    // `reconstruct_envelope: true`, its writer's `begin_document` /
    // `end_document` framing must fire around each document's records. This
    // arm detects document boundaries from each record's `doc_ctx`
    // (a change in the per-frame `grain()` between consecutive records) rather
    // than the records-only path's boundary-blind write loop. It buffers
    // nothing: each body record streams straight through `write_record`
    // between the header and footer, so a 1 GiB document flows at O(1-record).
    if resolve_out_cfg(ctx, name).reconstruct_envelope {
        return dispatch_output_envelope(ctx, current_dag, node_idx, name);
    }
    // Get input records: check own buffer first (Route
    // nodes store records at the successor's index), then
    // fall back to predecessor buffers.
    //
    // Output is terminal — it writes to disk, so punctuations
    // are consumed at this stage rather than forwarded. The
    // input drain still uses `drain_split` for symmetry with
    // every other operator, but the puncts vector goes unused
    // for non-streaming outputs. Streaming Outputs (#72) take
    // the early-return path above and forward puncts through
    // the streaming channel separately.
    let input_records: Vec<(Record, u64)> =
        if let Some(own_buf) = drain_node_buffer_slot(ctx, node_idx) {
            let (records, _puncts) = own_buf.drain_split()?;
            records
        } else {
            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            let mut found: Option<Vec<(Record, u64)>> = None;
            for &p in &predecessors {
                // When multiple outputs share a predecessor,
                // clone the buffer for all but the last
                // consumer to avoid starving siblings.
                let remaining_consumers = current_dag
                    .graph
                    .neighbors_directed(p, Direction::Outgoing)
                    .filter(|&succ| succ > node_idx)
                    .count();
                if remaining_consumers == 0 {
                    if let Some(nb) = drain_node_buffer_slot(ctx, p) {
                        let (records, _puncts) = nb.drain_split()?;
                        found = Some(records);
                        break;
                    }
                } else {
                    // Multi-consumer fanout: keep the producer's
                    // buffer alive for remaining siblings via a
                    // heap clone. The clone's footprint flows
                    // through pull-mode attribution at the
                    // arbitrator's next poll; the producer slot's
                    // registered consumer keeps reporting the live
                    // buffer until the last consumer drains it.
                    //
                    // Output is terminal — it writes to disk and
                    // has no downstream node_buffer to receive
                    // forwarded punctuations. The fan-out clone
                    // path takes records only; sibling consumers
                    // that need the document boundary read it from
                    // the non-cloned (last-consumer) drain through
                    // their own arm.
                    let cloned = ctx.node_buffers.get(&p).map(|nb| {
                        nb.clone_memory_only()
                            .into_iter()
                            .filter_map(|e| e.into_record())
                            .collect::<Vec<(Record, u64)>>()
                    });
                    if let Some(cloned) = cloned {
                        found = Some(cloned);
                        break;
                    }
                }
            }
            found.unwrap_or_default()
        };

    let OutputInputs {
        expected_input_schema,
        upstream_name,
        cxl_emit_names,
    } = resolve_output_inputs(current_dag, node_idx);
    if let Some(expected) = expected_input_schema.as_ref() {
        for (record, _) in &input_records {
            check_input_schema(expected, record.schema(), name, "output", &upstream_name)?;
        }
    }

    // When correlation buffering is active, every record
    // routed to this Output goes through the per-group buffer
    // — `CorrelationCommit` decides at end-of-DAG whether to
    // flush the group to the writer or DLQ it. Null-keyed
    // records get a row-disambiguated buffer cell each so
    // they retain per-record-rejection semantics without
    // splitting the writer path.
    let buffered: Vec<(Record, u64, Vec<GroupByKey>)>;
    let unbuffered: Vec<(Record, u64)>;
    if ctx.correlation_buffers.is_some() {
        buffered = input_records
            .into_iter()
            .map(|(rec, rn)| {
                let key = buffer_key_for_record(&rec, rn);
                (rec, rn, key)
            })
            .collect();
        unbuffered = Vec::new();
    } else {
        buffered = Vec::new();
        unbuffered = input_records;
    }
    // Counter semantics:
    //
    // * `records_written` increments per WRITE — under
    //   inclusive Route fan-out, one input matching N
    //   branches counts N (one per Output that received
    //   it). Aligns with per-Output throughput and the
    //   `records_emitted` local that drives stage-metric
    //   reporting.
    //
    // * `ok_count` increments by the number of DISTINCT
    //   source rows reaching this Output that haven't
    //   already been counted at another Output during
    //   the same DAG walk. Source identity is
    //   `row_num` (per-source counter), tracked across
    //   all Output arms via the `ok_source_rows` set
    //   declared at function scope.
    //
    // Buffered records DEFER counter increments to the
    // `CorrelationCommit` arm — clean groups bump
    // counters at flush time; dirty groups never count
    // toward `ok_count`.
    let unbuffered_record_count = unbuffered.len() as u64;
    let mut newly_ok: u64 = 0;
    for (_, row_num) in &unbuffered {
        if ctx.ok_source_rows.insert(*row_num) {
            newly_ok += 1;
        }
    }
    ctx.counters.ok_count += newly_ok;
    ctx.counters.records_written += unbuffered_record_count;
    ctx.records_emitted += unbuffered_record_count;

    // Derive output schema from first emitted record.
    // The Record is authoritative post-rip; materialize
    // the output-projection's `emitted` / `metadata`
    // maps from it on demand at this boundary. That
    // pays the bucket-insert cost once per record
    // reaching the writer, not every intermediate node
    // transition (Invariant 3).
    let scan_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
    // Inline field access (not `resolve_out_cfg`) so the borrow is scoped to
    // `output_configs` / `primary_output`: this arm interleaves `out_cfg`'s
    // borrow with `&mut ctx` on other fields (correlation buffers, writers,
    // timers), which disjoint sub-field borrows permit but a whole-`ctx`
    // helper borrow would not.
    let out_cfg = ctx
        .output_configs
        .iter()
        .find(|o| o.name == *name)
        .unwrap_or(ctx.primary_output);

    // `include_unmapped: false` consults the upstream CXL emit names (resolved
    // in the preamble above) to drop upstream passthroughs the user did not
    // explicitly emit.
    let cxl_emit_names_opt: Option<&[String]> = if cxl_emit_names.is_empty() {
        None
    } else {
        Some(&cxl_emit_names)
    };

    // Buffer non-null-key records. Project once, push slot.
    // Overflow check fires the moment a group's record count
    // exceeds the configured cap; subsequent records of the
    // same group are still admitted so they can become
    // collateral entries when `CorrelationCommit` drains the
    // group, but admission flips the overflow flag so the
    // commit arm emits a `GroupSizeExceeded` trigger.
    if !buffered.is_empty() {
        let max_buf = ctx.correlation_max_group_buffer;
        let buffers = ctx
            .correlation_buffers
            .as_mut()
            .expect("correlation_buffers is Some — we just checked above");
        for (record, rn, group_key) in buffered.iter() {
            let projected = project_output_from_record(record, out_cfg, cxl_emit_names_opt);
            let entry = buffers.entry(group_key.clone()).or_default();
            entry.total_records += 1;
            if max_buf > 0 && entry.total_records > max_buf {
                entry.overflowed = true;
            }
            entry.records.push(CorrelationRecordSlot {
                row_num: *rn,
                original_record: record.clone(),
                projected,
                output_name: name.clone(),
            });
        }
    }

    if unbuffered.is_empty() {
        ctx.collector.record(scan_timer.finish(0, 0));
        return Ok(());
    }

    let output_schema = {
        let projected = {
            let _guard = ctx.projection_timer.guard();
            project_output_from_record(&unbuffered[0].0, out_cfg, cxl_emit_names_opt)
        };
        Arc::clone(projected.schema())
    };

    // Find and take the writer for this output. Errors from
    // build_format_writer / write_record / flush are captured
    // into `output_errors` instead of short-circuiting via `?`
    // so siblings still get their chance to fail.
    //
    // Fan-out path: when the plan flagged this Output for
    // per-source-file routing, each record's source_file Arc
    // selects the right writer; the registry holds N writers
    // (one per discovered file).
    let fan_out_writers = ctx.fan_out_writers.remove(name);
    let single_writer = if fan_out_writers.is_none() {
        ctx.writers.remove(name)
    } else {
        None
    };
    let mut fan_ctx = FanOutContext {
        name,
        out_cfg,
        cxl_emit_names_opt,
        output_schema: &output_schema,
        output_errors: &mut ctx.output_errors,
        write_timer: &mut ctx.write_timer,
        projection_timer: &mut ctx.projection_timer,
        collector: &mut *ctx.collector,
    };
    if let Some(per_file) = fan_out_writers {
        emit_fan_out(&mut fan_ctx, &unbuffered, per_file, scan_timer);
    } else if let Some(raw_writer) = single_writer {
        emit_single_writer(&mut fan_ctx, raw_writer, &unbuffered, scan_timer);
    }

    Ok(())
}

/// Execute the `Output` arm under document-level DLQ. Drains this Output's
/// input as the INTERLEAVED record + `DocumentClose` event stream and hands
/// it to a [`crate::executor::document_dlq::DocumentDlqDriver`], which
/// buffers each record per document and, at each close, flushes the
/// document clean to the writer or rejects it (trigger + collateral) to the
/// DLQ. Blocking at the document grain — a buffered record is not written
/// until its close decides the document clean; peak memory is the
/// concurrently-open documents, spillable under budget.
fn dispatch_output_document_dlq(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
) -> Result<(), PipelineError> {
    let events = drain_output_input_events(ctx, current_dag, node_idx)?;

    let OutputInputs {
        expected_input_schema,
        upstream_name,
        cxl_emit_names,
    } = resolve_output_inputs(current_dag, node_idx);
    if let Some(expected) = expected_input_schema.as_ref() {
        for event in &events {
            if let crate::executor::stream_event::StreamEvent::Record(record, _) = event {
                check_input_schema(expected, record.schema(), name, "output", &upstream_name)?;
            }
        }
    }

    // Inline field access (not `resolve_out_cfg`) so the borrow is scoped to
    // `output_configs` / `primary_output`: the driver holds `out_cfg` across
    // `run`, which takes `&mut ctx` — a whole-`ctx` helper borrow would
    // conflict, but disjoint sub-field borrows coexist.
    let out_cfg = ctx
        .output_configs
        .iter()
        .find(|o| o.name == *name)
        .unwrap_or(ctx.primary_output);
    let driver =
        crate::executor::document_dlq::DocumentDlqDriver::new(ctx, name, out_cfg, cxl_emit_names);
    driver.run(ctx, events)
}

/// Drain this Output's input buffer as the INTERLEAVED `StreamEvent`
/// stream, preserving record/`DocumentClose` ordering — the boundary the
/// records-only `drain_split` path discards — and materialize it into a
/// `Vec` for the document-DLQ driver (which buffers per document anyway, so
/// has no use for the lazy form). Delegates the predecessor-selection walk to
/// [`drain_output_input_event_iter`] so the two boundary-aware Output arms
/// share one mechanism.
fn drain_output_input_events(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
) -> Result<Vec<crate::executor::stream_event::StreamEvent>, PipelineError> {
    drain_output_input_event_iter(ctx, current_dag, node_idx).collect()
}

/// Execute the `Output` arm under envelope reconstruction
/// (`reconstruct_envelope: true`). Replays this Output's records through the
/// writer with per-document framing: the writer's `begin_document` fires on
/// the first record of each document, every body record streams straight
/// through `write_record`, and `end_document` fires when the document ends.
///
/// Boundary detection is RECORD-driven, not punctuation-driven: every
/// `Record` carries its `Arc<DocumentContext>`, and a document boundary is a
/// change in the record's `doc_ctx().grain()` between consecutive records.
/// Punctuations cannot drive this — the executor's buffers tail-clump all
/// `DocumentClose` events after all records, so an interleaved boundary stream
/// never reaches a terminal Output.
///
/// The grain is the per-document FRAME ([`clinker_record::DocumentGrain`]). An
/// X12 `GS`/`ST` inherits the interchange grain, so a whole `ISA..IEA`
/// interchange frames as one envelope; an HL7 `MSH` opens a fresh grain, so a
/// multi-message file frames once PER message; a flat file
/// (CSV/JSON/XML/fixed-width/EDIFACT) is one grain per file.
///
/// This per-frame grain is DELIBERATELY distinct from the document-DLQ's
/// keying, which stays at the file grain (`source_file`): an HL7 `BTS`/`FTS`
/// batch/file count mismatch is a whole-file structural failure, so the DLQ
/// must condemn the whole file, not one message. Framing and dead-lettering
/// therefore use different grains and never co-execute — `reconstruct_envelope`
/// and `dlq_granularity: document` are mutually exclusive (rejected by E347),
/// so no record is ever both DLQ-bucketed and envelope-framed.
///
/// Bounded-memory: this path buffers no records. It holds only the current
/// document's context, so a multi-gigabyte document streams at O(1-record).
/// The input drain is itself lazy — a spilled predecessor buffer streams from
/// disk one record at a time rather than materializing.
///
/// Records with a non-concrete source file (the `<merged>` sentinel or an
/// empty stamp — an in-pipeline synthesis or fan-in row that belongs to no
/// document) stream through unframed, matching the document-DLQ arm's
/// `is_concrete_file` guard.
///
/// # Errors
///
/// Surfaces input-drain (spill-read), schema-check, writer-construction,
/// `write_record`, framing, and flush errors as a [`PipelineError`]. A
/// schema-check failure fails fast like the sibling arms, but first surfaces
/// any already-accumulated framing/write errors and flushes the open document
/// so nothing in flight is lost.
fn dispatch_output_envelope(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
) -> Result<(), PipelineError> {
    use crate::executor::stream_event::StreamEvent;

    let OutputInputs {
        expected_input_schema,
        upstream_name,
        cxl_emit_names,
    } = resolve_output_inputs(current_dag, node_idx);
    // Owned clone: the writer-factory closure and the projection both borrow
    // `out_cfg` across the loop's `&mut ctx` phases, so it cannot stay a
    // borrow of `ctx.output_configs`.
    let out_cfg = resolve_out_cfg(ctx, name).clone();
    let cxl_emit_names_opt: Option<&[String]> = if cxl_emit_names.is_empty() {
        None
    } else {
        Some(&cxl_emit_names)
    };

    // Emit the same `SchemaScan` stage metric the records-only arm does, so an
    // envelope Output is not invisible to per-stage reporting.
    let scan_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
    let mut any_record = false;

    let events = drain_output_input_event_iter(ctx, current_dag, node_idx);
    let mut driver = EnvelopeWriterDriver::default();

    for event in events {
        // Boundaries come from records, so punctuations are irrelevant here —
        // drop them exactly as the records-only `drain_split` path does.
        let StreamEvent::Record(record, row_num) = event? else {
            continue;
        };
        any_record = true;
        if let Some(expected) = expected_input_schema.as_ref()
            && let Err(e) =
                check_input_schema(expected, record.schema(), name, "output", &upstream_name)
        {
            // Fail fast like the records-only / DLQ arms, but first close the
            // open document and surface any framing/write errors accumulated
            // so far — the in-flight footer and prior errors are not lost.
            {
                let _guard = ctx.write_timer.guard();
                driver.finish();
            }
            ctx.output_errors.append(&mut driver.errors);
            return Err(e);
        }
        let projected = {
            let _guard = ctx.projection_timer.guard();
            project_output_from_record(&record, &out_cfg, cxl_emit_names_opt)
        };
        {
            let _guard = ctx.write_timer.guard();
            // The borrowed `ctx.writers` registry is consulted only when the
            // writer opens (first record); passing the raw-writer source as a
            // closure keeps the driver free of `ExecutorContext` so its
            // boundary logic is unit-testable against a probe writer.
            driver.on_record(record.doc_ctx(), &projected, &mut |schema| {
                let raw_writer = ctx.writers.remove(name)?;
                Some(build_format_writer(&out_cfg, raw_writer, schema))
            });
        }
        // Count exactly as the records-only arm does: both counters bump
        // unconditionally per record, independent of whether a writer is open
        // or even registered. That keeps flag-on (with no-op hooks) invariant
        // against flag-off — same input yields the same `records_written` /
        // `ok_count` even on the no-writer / dry-run path where no byte is
        // emitted.
        ctx.counters.records_written += 1;
        ctx.records_emitted += 1;
        if ctx.ok_source_rows.insert(row_num) {
            ctx.counters.ok_count += 1;
        }
    }
    {
        let _guard = ctx.write_timer.guard();
        driver.finish();
    }

    if any_record {
        ctx.collector.record(scan_timer.finish(1, 1));
    } else {
        ctx.collector.record(scan_timer.finish(0, 0));
    }
    ctx.output_errors.append(&mut driver.errors);
    Ok(())
}

/// Lazy writer-open source the [`EnvelopeWriterDriver`] calls on the first
/// record, passing the projected output schema. `None` means no writer is
/// registered for the Output (a sibling already took it, or a dry run);
/// `Some(Err(_))` carries a writer-construction failure. Threaded as a
/// closure so the driver stays free of [`ExecutorContext`] — production
/// builds it from `ctx.writers`, the unit test from a probe writer.
type WriterFactory<'a> = dyn FnMut(
        Arc<clinker_record::Schema>,
    ) -> Option<Result<Box<dyn clinker_format::FormatWriter>, PipelineError>>
    + 'a;

/// Per-Output state for the envelope-reconstruction arm. Holds the single
/// writer (opened lazily on the first record) and the currently-open
/// document — never any body records, so the arm's footprint is O(1), not
/// O(document size). Free of [`ExecutorContext`] so its boundary logic is
/// unit-testable against a probe writer; the caller supplies the raw-writer
/// source as a closure, counts records (so the per-record counters stay
/// identical to the records-only arm regardless of writer state), and folds
/// the accumulated errors back into the run context.
#[derive(Default)]
struct EnvelopeWriterDriver {
    writer: Option<Box<dyn clinker_format::FormatWriter>>,
    /// The currently-open document's context, set on its first record's
    /// `begin_document` and cleared on its `end_document`. `None` before the
    /// first concrete-file record and between documents. Held so the
    /// matching `end_document` (at the next boundary or at `finish`) carries
    /// the same context `begin_document` opened with.
    open_doc: Option<Arc<clinker_record::DocumentContext>>,
    /// Writer-construction / framing / write / flush errors, appended to the
    /// run's error sink by the caller rather than short-circuiting, matching
    /// the records-only Output path.
    errors: Vec<PipelineError>,
}

impl EnvelopeWriterDriver {
    /// Write one already-projected body record, framing per document. The
    /// record's own `doc_ctx` drives boundary detection: when its frame
    /// `grain` differs from the currently-open document's, the prior document
    /// ends (`end_document`) and the new one begins (`begin_document`) before
    /// the record is written. Records whose `source_file` is non-concrete (an
    /// in-pipeline synthesis / fan-in row) stream through unframed.
    ///
    /// Opens the writer lazily on the first record (via `open_writer`,
    /// deriving the schema from it). A `None` from `open_writer` means no
    /// writer is registered (a sibling already took it, or a dry run): the
    /// record is dropped, matching the records-only Output path's behavior
    /// when its writer registry slot is empty.
    ///
    /// Record counting is the caller's job (it bumps `records_written` /
    /// `ok_count` unconditionally per record, exactly as the records-only
    /// arm does), so a dropped record here still counts identically — that
    /// is what keeps flag-on invariant against flag-off.
    fn on_record(
        &mut self,
        doc_ctx: &Arc<clinker_record::DocumentContext>,
        projected: &Record,
        open_writer: &mut WriterFactory<'_>,
    ) {
        if self.writer.is_none() {
            match open_writer(Arc::clone(projected.schema())) {
                Some(Ok(w)) => self.writer = Some(w),
                Some(Err(e)) => {
                    self.errors.push(e);
                    return;
                }
                None => return,
            }
        }
        self.maybe_cross_boundary(doc_ctx);
        let writer = self.writer.as_mut().expect("writer opened above");
        if let Err(e) = writer.write_record(projected) {
            self.errors.push(e.into());
        }
    }

    /// Fire `end_document` / `begin_document` when this record's document
    /// differs from the currently-open one. A non-concrete source file
    /// belongs to no document, so it neither closes the open document nor
    /// opens one — it streams through inside whatever framing is current.
    ///
    /// The same-document test compares the record's
    /// [`grain`](clinker_record::DocumentContext::grain) against the open
    /// document's. The grain is a `Copy` value identity (not a pointer), so it
    /// is correct across an input-buffer spill boundary for free: a frame whose
    /// records span two spill chunks rebuilds a fresh `source_file` Arc per
    /// chunk, but the grain round-trips verbatim, so the frame is not
    /// spuriously split mid-stream. Keying on grain rather than `source_file`
    /// is what makes a multi-message HL7 file frame once per message (each
    /// `MSH` is its own grain) while a nested X12 interchange still frames once
    /// (its `GS`/`ST` levels inherit the interchange grain).
    fn maybe_cross_boundary(&mut self, doc_ctx: &Arc<clinker_record::DocumentContext>) {
        if !crate::executor::document_dlq::is_concrete_file(doc_ctx.source_file()) {
            return;
        }
        let grain = doc_ctx.grain();
        let same_doc = self
            .open_doc
            .as_ref()
            .is_some_and(|open| open.grain() == grain);
        if same_doc {
            return;
        }
        self.fire_end();
        self.fire_begin(doc_ctx);
        self.open_doc = Some(Arc::clone(doc_ctx));
    }

    /// Emit the open document's closing framing, if a document is open.
    fn fire_end(&mut self) {
        if let (Some(writer), Some(doc_ctx)) = (self.writer.as_mut(), self.open_doc.take())
            && let Err(e) = writer.end_document(&doc_ctx)
        {
            self.errors.push(e.into());
        }
    }

    /// Emit a document's opening framing through the open writer.
    fn fire_begin(&mut self, doc_ctx: &clinker_record::DocumentContext) {
        if let Some(writer) = self.writer.as_mut()
            && let Err(e) = writer.begin_document(doc_ctx)
        {
            self.errors.push(e.into());
        }
    }

    /// Close the last open document and flush at end of stream.
    fn finish(&mut self) {
        self.fire_end();
        if let Some(writer) = self.writer.as_mut()
            && let Err(e) = writer.flush()
        {
            self.errors.push(e.into());
        }
    }
}

/// Lazily drain this Output's input as the INTERLEAVED `StreamEvent` stream,
/// preserving record/boundary ordering — the envelope-reconstruction analog
/// of [`drain_output_input_events`], but yielding an iterator rather than a
/// `Vec` so a spilled predecessor buffer streams from disk one event at a
/// time instead of materializing. Mirrors the predecessor-selection logic of
/// the per-record path (own slot first, then the last-consumer predecessor
/// drain, then a memory clone for multi-consumer fan-out). Multi-consumer
/// fan-out slots are never spilled, so the in-memory clone branch never hides
/// a spilled buffer.
fn drain_output_input_event_iter(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
) -> Box<dyn Iterator<Item = Result<crate::executor::stream_event::StreamEvent, PipelineError>>> {
    use crate::executor::stream_event::StreamEvent;

    if let Some(own_buf) = drain_node_buffer_slot(ctx, node_idx) {
        return Box::new(own_buf.drain());
    }
    let predecessors: Vec<NodeIndex> = current_dag
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    for p in predecessors {
        let remaining_consumers = current_dag
            .graph
            .neighbors_directed(p, Direction::Outgoing)
            .filter(|&succ| succ > node_idx)
            .count();
        if remaining_consumers == 0 {
            if let Some(nb) = drain_node_buffer_slot(ctx, p) {
                return Box::new(nb.drain());
            }
        } else if let Some(cloned) = ctx.node_buffers.get(&p).map(|nb| nb.clone_memory_only()) {
            return Box::new(cloned.into_iter().map(Ok));
        }
    }
    Box::new(std::iter::empty::<Result<StreamEvent, PipelineError>>())
}

/// The Output node's resolved write target plus the run-scoped writer
/// state both write paths share. Bundling the four cross-branch borrows
/// (error sink, the write / projection cumulative timers, and the
/// stage-metric collector) with the per-call write descriptor (output
/// name, resolved [`OutputConfig`](clinker_plan::config::OutputConfig), explicit
/// CXL emit names, and the projected output schema) keeps the fan-out and
/// single-writer helpers below clippy's argument threshold and gives them
/// one shared shape — a change to how an Output write is attributed (e.g.
/// a new metric guard) lands on the struct, not on two signatures.
struct FanOutContext<'a> {
    name: &'a str,
    out_cfg: &'a clinker_plan::config::OutputConfig,
    cxl_emit_names_opt: Option<&'a [String]>,
    output_schema: &'a Arc<clinker_record::Schema>,
    output_errors: &'a mut Vec<PipelineError>,
    write_timer: &'a mut crate::executor::stage_metrics::CumulativeTimer,
    projection_timer: &'a mut crate::executor::stage_metrics::CumulativeTimer,
    collector: &'a mut crate::executor::stage_metrics::StageCollector,
}

/// Write `unbuffered` through a single pre-opened writer. Errors from
/// writer construction / `write_record` / `flush` land in the context's
/// error sink rather than short-circuiting, so sibling Outputs still get
/// their chance to fail.
fn emit_single_writer(
    fan_ctx: &mut FanOutContext<'_>,
    raw_writer: Box<dyn Write + Send>,
    unbuffered: &[(Record, u64)],
    scan_timer: crate::executor::stage_metrics::StageTimer,
) {
    match build_format_writer(
        fan_ctx.out_cfg,
        raw_writer,
        Arc::clone(fan_ctx.output_schema),
    ) {
        Ok(mut csv_writer) => {
            fan_ctx.collector.record(scan_timer.finish(1, 1));
            let mut write_failed = false;
            for (record, _rn) in unbuffered {
                let projected = {
                    let _guard = fan_ctx.projection_timer.guard();
                    project_output_from_record(record, fan_ctx.out_cfg, fan_ctx.cxl_emit_names_opt)
                };
                let write_result = {
                    let _guard = fan_ctx.write_timer.guard();
                    csv_writer.write_record(&projected)
                };
                if let Err(e) = write_result {
                    push_write_error(fan_ctx.output_errors, e);
                    write_failed = true;
                    break;
                }
            }
            if !write_failed {
                let flush_result = {
                    let _guard = fan_ctx.write_timer.guard();
                    csv_writer.flush()
                };
                if let Err(e) = flush_result {
                    push_write_error(fan_ctx.output_errors, e);
                }
            }
        }
        Err(e) => fan_ctx.output_errors.push(e),
    }
}

/// Emit a buffered record stream to a fan-out output: one writer per
/// source-file `Arc<str>`, route each record to the writer keyed by
/// its `$source.file` Arc. Writers without any matched records still
/// flush an empty file (preserving header and any per-file framing).
///
/// All errors land in the context's error sink rather than
/// short-circuiting so sibling writers in the same Output still get
/// their chance to flush or report.
fn emit_fan_out(
    fan_ctx: &mut FanOutContext<'_>,
    unbuffered: &[(Record, u64)],
    per_file: HashMap<Arc<str>, Box<dyn Write + Send>>,
    scan_timer: crate::executor::stage_metrics::StageTimer,
) {
    use std::collections::HashMap as Hm;

    // Build one format writer per pre-opened raw writer. Failed
    // construction for one file does NOT abort the whole output —
    // siblings still get their chance.
    let mut format_writers: Hm<Arc<str>, Box<dyn clinker_format::FormatWriter>> = Hm::new();
    for (file_arc, raw) in per_file {
        match build_format_writer(fan_ctx.out_cfg, raw, Arc::clone(fan_ctx.output_schema)) {
            Ok(fw) => {
                format_writers.insert(file_arc, fw);
            }
            Err(e) => fan_ctx.output_errors.push(e),
        }
    }
    fan_ctx.collector.record(scan_timer.finish(1, 1));

    for (record, rn) in unbuffered {
        let Some(file_path) = source_file_path_of(record) else {
            fan_ctx.output_errors.push(PipelineError::Internal {
                op: "fan_out",
                node: fan_ctx.name.to_string(),
                detail: format!(
                    "row {rn} has no `$source.file` stamp; fan-out output requires per-record source-file lineage",
                ),
            });
            continue;
        };
        // Look up the writer by path; the registry keys by Arc<str>
        // so we need to find by string equality. Build a probing Arc
        // once per record (cheap relative to the write itself).
        let file_arc: Arc<str> = Arc::from(file_path);
        let Some(fw) = format_writers.get_mut(&file_arc) else {
            // Record's file isn't in the fan-out registry — typically
            // means the CLI's writer setup didn't pre-open one for
            // this file. Surface but keep going.
            fan_ctx.output_errors.push(PipelineError::Internal {
                op: "fan_out",
                node: fan_ctx.name.to_string(),
                detail: format!(
                    "no fan-out writer registered for source file {:?}",
                    file_arc
                ),
            });
            continue;
        };
        let projected = {
            let _guard = fan_ctx.projection_timer.guard();
            project_output_from_record(record, fan_ctx.out_cfg, fan_ctx.cxl_emit_names_opt)
        };
        let write_result = {
            let _guard = fan_ctx.write_timer.guard();
            fw.write_record(&projected)
        };
        if let Err(e) = write_result {
            push_write_error(fan_ctx.output_errors, e);
        }
    }

    // Flush every writer regardless of per-record errors so partial
    // outputs land on disk for inspection.
    for (_arc, mut fw) in format_writers {
        let flush_result = {
            let _guard = fan_ctx.write_timer.guard();
            fw.flush()
        };
        if let Err(e) = flush_result {
            push_write_error(fan_ctx.output_errors, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_format::FormatWriter;
    use clinker_format::error::FormatError;
    use clinker_record::{DocumentContext, DocumentId, FieldResolver, Schema, Value};
    use std::sync::Mutex;

    /// A [`FormatWriter`] that records every hook invocation as an ordered
    /// string log, so a test can assert the exact boundary sequence the
    /// envelope arm drove. The log is shared via `Arc<Mutex<_>>` because the
    /// writer is boxed and moved into the driver.
    struct ProbeWriter {
        log: Arc<Mutex<Vec<String>>>,
    }

    impl FormatWriter for ProbeWriter {
        fn write_record(&mut self, record: &Record) -> Result<(), FormatError> {
            let id = match record.resolve("id") {
                Some(Value::Integer(n)) => *n,
                _ => -1,
            };
            self.log.lock().unwrap().push(format!("write:{id}"));
            Ok(())
        }
        fn flush(&mut self) -> Result<(), FormatError> {
            self.log.lock().unwrap().push("flush".to_string());
            Ok(())
        }
        fn begin_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
            self.log
                .lock()
                .unwrap()
                .push(format!("begin:{}", doc.source_file()));
            Ok(())
        }
        fn end_document(&mut self, doc: &DocumentContext) -> Result<(), FormatError> {
            self.log
                .lock()
                .unwrap()
                .push(format!("end:{}", doc.source_file()));
            Ok(())
        }
    }

    fn doc(file: &str) -> Arc<DocumentContext> {
        Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from(file),
            clinker_record::EnvelopeRecord::empty(),
        ))
    }

    fn record(id: i64, doc_ctx: &Arc<DocumentContext>) -> Record {
        let schema = Arc::new(Schema::new(vec!["id".into()]));
        let mut rec = Record::new(schema, vec![Value::Integer(id)]);
        rec.set_doc_ctx(Arc::clone(doc_ctx));
        rec
    }

    /// Drive the driver over a probe writer with a record-driven stream
    /// (boundaries come from each record's `doc_ctx`, as in production),
    /// returning its hook-call log and the count of records that reached
    /// `write_record`. Records are already in output shape, so the dispatch
    /// loop's projection step is elided.
    fn run_log(records: &[Record]) -> (Vec<String>, u64) {
        let log = Arc::new(Mutex::new(Vec::new()));
        let mut driver = EnvelopeWriterDriver::default();
        for rec in records {
            let log = Arc::clone(&log);
            driver.on_record(rec.doc_ctx(), rec, &mut |_schema| {
                Some(Ok(Box::new(ProbeWriter {
                    log: Arc::clone(&log),
                }) as Box<dyn FormatWriter>))
            });
        }
        driver.finish();
        assert!(driver.errors.is_empty(), "probe writer never errors");
        // Drop the driver (and the probe writer it owns, which holds a clone
        // of `log`) before reclaiming the sole `Arc`.
        drop(driver);
        let log = Arc::try_unwrap(log).unwrap().into_inner().unwrap();
        // The driver no longer counts records (the dispatch caller does, to
        // stay aligned with the records-only arm); derive the write count
        // from the probe log instead.
        let written = log.iter().filter(|l| l.starts_with("write:")).count() as u64;
        (log, written)
    }

    #[test]
    fn fires_begin_and_end_once_per_document_at_boundaries() {
        let a = doc("a.csv");
        let b = doc("b.csv");
        // Two documents' records, back to back; the boundary is the
        // `source_file` change between record 2 and record 3.
        let records = vec![record(1, &a), record(2, &a), record(3, &b)];
        let (log, written) = run_log(&records);
        assert_eq!(
            log,
            vec![
                "begin:a.csv",
                "write:1",
                "write:2",
                "end:a.csv",
                "begin:b.csv",
                "write:3",
                "end:b.csv",
                "flush",
            ],
        );
        assert_eq!(written, 3, "every body record streamed through");
    }

    #[test]
    fn nested_x12_interchange_fires_only_at_the_interchange_pair() {
        // One X12 interchange, two nested levels: the inner level mints a
        // fresh `DocumentId` via `child` but INHERITS the interchange grain.
        // Keying boundary detection on the grain frames at the interchange, so
        // a record carrying the inner (GS/ST) context still belongs to the one
        // interchange document — begin/end fire exactly once for the whole
        // `ISA..IEA`, not once per transaction set.
        let outer = doc("multi.x12");
        let inner =
            Arc::new(outer.child(DocumentId::next(), clinker_record::EnvelopeRecord::empty()));
        let records = vec![record(1, &outer), record(2, &inner), record(3, &outer)];
        let (log, written) = run_log(&records);
        assert_eq!(
            log,
            vec![
                "begin:multi.x12",
                "write:1",
                "write:2",
                "write:3",
                "end:multi.x12",
                "flush",
            ],
            "begin/end fire once for the interchange, not per nested level",
        );
        assert_eq!(written, 3);
    }

    #[test]
    fn multi_message_hl7_file_frames_once_per_message() {
        // One HL7 file, two messages: each `MSH` opens a fresh frame via
        // `child_frame`, so the two message contexts share the file's
        // `source_file` Arc but carry DISTINCT grains. Keying on grain frames
        // ONCE PER MESSAGE — begin/end fire around each message's records —
        // even though both messages live in one file. (Keying on `source_file`
        // would collapse them into a single frame, the bug this fixes.)
        let file: Arc<str> = Arc::from("messages.hl7");
        let file_doc = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::clone(&file),
            clinker_record::EnvelopeRecord::empty(),
        ));
        let msg1 = Arc::new(
            file_doc.child_frame(DocumentId::next(), clinker_record::EnvelopeRecord::empty()),
        );
        let msg2 = Arc::new(
            file_doc.child_frame(DocumentId::next(), clinker_record::EnvelopeRecord::empty()),
        );
        let records = vec![record(1, &msg1), record(2, &msg1), record(3, &msg2)];
        let (log, written) = run_log(&records);
        assert_eq!(
            log,
            vec![
                "begin:messages.hl7",
                "write:1",
                "write:2",
                "end:messages.hl7",
                "begin:messages.hl7",
                "write:3",
                "end:messages.hl7",
                "flush",
            ],
            "begin/end fire once per HL7 message, not once for the whole file",
        );
        assert_eq!(written, 3);
    }

    #[test]
    fn single_document_frames_once_and_ends_at_finish() {
        // A document's `end_document` fires at `finish()` when no later
        // boundary closes it — the EOF-with-open-document case.
        let a = doc("solo.csv");
        let records = vec![record(1, &a), record(2, &a)];
        let (log, _written) = run_log(&records);
        assert_eq!(
            log,
            vec![
                "begin:solo.csv",
                "write:1",
                "write:2",
                "end:solo.csv",
                "flush"
            ],
            "the last open document is closed at finish()",
        );
    }

    #[test]
    fn spilled_chunk_rebuild_does_not_split_a_document() {
        // A document whose records span two input spill chunks has its
        // `DocumentContext` rebuilt per chunk by the spill codec, producing a
        // fresh `source_file` Arc but the SAME grain (the codec carries the
        // grain verbatim). Keying boundary detection on the grain therefore
        // keeps the frame intact across the spill boundary — a pure
        // `Arc::ptr_eq` on `source_file` would spuriously split it. The
        // postcard round-trip is exactly what the spill path does.
        let chunk1 = doc("split.csv");
        let bytes = postcard::to_stdvec(chunk1.as_ref()).unwrap();
        let rebuilt: DocumentContext = postcard::from_bytes(&bytes).unwrap();
        let chunk2 = Arc::new(rebuilt);
        assert!(
            !Arc::ptr_eq(chunk1.source_file(), chunk2.source_file()),
            "the rebuilt context must hold a distinct `source_file` Arc to model the spill",
        );
        assert_eq!(
            chunk1.grain(),
            chunk2.grain(),
            "but the grain survives the spill round-trip verbatim",
        );
        let records = vec![record(1, &chunk1), record(2, &chunk2)];
        let (log, _written) = run_log(&records);
        assert_eq!(
            log,
            vec![
                "begin:split.csv",
                "write:1",
                "write:2",
                "end:split.csv",
                "flush"
            ],
            "a document split across spill chunks frames once, not once per chunk",
        );
    }

    #[test]
    fn non_concrete_source_file_streams_unframed() {
        // A record whose source file is the `<merged>` sentinel (a fan-in /
        // synthesis row) belongs to no document: it neither opens nor closes
        // framing, streaming through whatever document is current. Here the
        // whole stream is non-concrete, so no begin/end ever fires.
        let merged = doc("<merged>");
        let records = vec![record(1, &merged), record(2, &merged)];
        let (log, written) = run_log(&records);
        assert_eq!(
            log,
            vec!["write:1", "write:2", "flush"],
            "non-concrete-file records stream through unframed",
        );
        assert_eq!(written, 2);
    }

    #[test]
    fn no_writer_slot_drops_records_without_error_and_never_counts() {
        // The empty-writer-slot path (a dry run, or a sibling Output that
        // already took the writer): `open_writer` yields `None`. The driver
        // must drop each record silently — no hook, no write, no error — and
        // carry no per-record counter of its own, so the dispatch caller's
        // unconditional `records_written` / `ok_count` increments produce the
        // exact same counts whether or not a writer materializes. That is what
        // keeps the `reconstruct_envelope` flag transparent on the no-writer
        // path: counters match the records-only arm.
        let a = doc("dry-run.csv");
        let records = [record(1, &a), record(2, &a)];
        let mut driver = EnvelopeWriterDriver::default();
        for rec in &records {
            // No writer is ever registered for this Output.
            driver.on_record(rec.doc_ctx(), rec, &mut |_schema| None);
        }
        driver.finish();
        assert!(
            driver.errors.is_empty(),
            "a missing writer slot is not an error — the records-only arm drops too",
        );
        assert!(
            driver.writer.is_none(),
            "no writer ever opened, so no framing or write was attempted",
        );
    }
}
