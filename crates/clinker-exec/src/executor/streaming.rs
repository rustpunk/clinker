//! The streaming-consumer substrate and its sole `Output` instantiation.
//!
//! Installs the fused producer → single streaming-consumer chains that the
//! plan's streaming-fusion analysis flagged as bypassing a `node_buffers`
//! slot, moving each such consumer onto its own thread fed by a bounded
//! channel. The channel-drain + per-record-discharge skeleton
//! ([`drain_streaming_channel`]) is parameterized over a
//! [`StreamingConsumer`], with [`OutputStreamConsumer`] (a file-writer)
//! as the sole instantiation today. The eligibility verdict comes from the
//! plan layer's `certify_streaming_edge`, so this runtime install can never
//! disagree with the `--explain` buffer-class annotation.

use std::collections::HashSet;
use std::io::Write;
use std::ops::ControlFlow;
use std::sync::Arc;

use clinker_record::{Record, Schema};

use clinker_format::traits::FormatWriter;
use clinker_plan::config::{OutputConfig, PipelineConfig};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::certify_streaming_edge;

use super::stream_event::{Punctuation, StreamEvent};
use super::{WriterRegistry, build_format_writer, dispatch, stage_metrics};

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
/// Eligibility is decided by [`certify_streaming_edge`] (the shared
/// plan-derived predicate); this function adds the runtime writer-
/// registry checks (the writer must be a registered single writer, not a
/// fan-out) and packages the owned per-task metadata. It is the `Output`
/// instantiation of the generalized substrate: the writer-registry guard,
/// `out_cfg`, and projection metadata are all Output-specific.
///
/// Returns the list of streaming specs (one per qualifying Output). Empty
/// for pipelines that don't match the topology, leaving every Output on
/// the existing buffered path. See
/// https://github.com/rustpunk/clinker/issues/72 for the rationale.
pub(super) fn compute_streaming_output_specs(
    plan: &clinker_plan::plan::execution::ExecutionPlanDag,
    config: &PipelineConfig,
    fused_transforms: &HashSet<petgraph::graph::NodeIndex>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
    output_configs: &[OutputConfig],
    writers: &WriterRegistry,
) -> Vec<StreamingOutputSpec> {
    use clinker_plan::plan::execution::PlanNode;

    // Pipeline-wide correlation buffering disables streaming for every
    // Output — the CorrelationCommit terminal owns the actual writes.
    // Document-level DLQ disables it for the same structural reason: the
    // per-document Output buffer flushes or rejects each document at its
    // materialized `DocumentClose`, which a streaming-Output thread would
    // consume out of band before the buffer could decide the document.
    // Envelope reconstruction disables it on the same axis: the
    // punctuation-aware Output arm fires the writer's `begin_document` /
    // `end_document` at the materialized boundaries, so a streaming-Output
    // thread consuming the `DocumentClose` out of band would strand the
    // footer.
    if config.any_source_has_correlation_key()
        || config.any_source_has_document_dlq()
        || config.any_output_reconstructs_envelope()
    {
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
            certify_streaming_edge(plan, output_idx, fused_transforms, init_phase_set)
        else {
            continue;
        };

        // Runtime writer-registry checks: the writer must be registered
        // as a single writer, never a fan-out. (The plan-derived
        // fan-out / split exclusions live in `certify_streaming_edge`'s
        // Output arm so the explain surface agrees; this is the
        // runtime-only registry confirmation.)
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

/// A streaming consumer plugged into [`drain_streaming_channel`].
///
/// The skeleton owns the channel recv loop, the per-record memory
/// discharge, the back-pressure semantics, and the deadlock-safe
/// drain-on-fatal; the consumer owns only what to do with each event.
/// Consumers are single-threaded — the skeleton drives one consumer per
/// streaming thread, so `&mut self` is the only mutation path and the
/// peak inter-stage footprint is one bounded batch.
///
/// `Output` (file writer) is the sole implementation today; future
/// streaming consumers (an `Aggregate` ingest, a `Combine` probe) plug in
/// as additional implementations without re-deriving the recv loop.
pub(super) trait StreamingConsumer {
    /// Handle one body record (already discharged from the shared charge
    /// handle by the skeleton). Return [`ControlFlow::Continue`] to keep
    /// draining or [`ControlFlow::Break`] on a fatal consumer error; on
    /// `Break` the skeleton drains the rest of the channel (so the bounded
    /// producer `send` never deadlocks), zeroes the charge, and returns
    /// without calling [`StreamingConsumer::on_close`].
    fn on_record(&mut self, record: Record, row_num: u64) -> ControlFlow<()>;

    /// Handle one document-boundary punctuation. Carries no memory
    /// discharge — punctuations contribute zero to
    /// `EventBatch::estimated_bytes`, so the record-only discharge stays
    /// symmetric.
    fn on_punctuation(&mut self, punct: Punctuation);

    /// Finalize at channel disconnect (every sender dropped). Called
    /// exactly once on the clean-drain path, never after an `on_record`
    /// `Break`.
    fn on_close(&mut self);
}

/// Drain a streaming consumer's bounded channel to disconnect, applying
/// the per-record memory discharge and deadlock-safe fatal handling that
/// every streaming consumer shares.
///
/// Streaming, not blocking: back-pressure flows consumer → producer →
/// Source through the bounded channel, and the per-batch admit/discharge
/// model keeps the slot's live byte count tracking only the batches in
/// flight, never the whole stage. For each body record the skeleton
/// discharges the record's `record_byte_cost` from `charge_handle` *before*
/// handing it to [`StreamingConsumer::on_record`] — the consume half of the
/// producer's per-batch `EventBatch::estimated_bytes` charge, so a fully
/// drained stream nets the counter back to zero. On an `on_record`
/// `Break`, the skeleton drains the rest of the channel (so the producer's
/// bounded `send` never blocks forever on a dead consumer) and pins the
/// charge to zero without finalizing the consumer. On clean disconnect it
/// finalizes the consumer, then pins the charge to zero defensively so a
/// heuristic mismatch between the batch charge and the per-record discharge
/// can never leave a stale positive charge for the post-join arbitrator
/// read.
pub(super) fn drain_streaming_channel<C: StreamingConsumer>(
    rx: &crossbeam_channel::Receiver<StreamEvent>,
    charge_handle: &Arc<crate::pipeline::memory::ConsumerHandle>,
    consumer: &mut C,
) {
    while let Ok(event) = rx.recv() {
        let (record, rn) = match event {
            StreamEvent::Record(r, rn) => (r, rn),
            StreamEvent::Punctuation(p) => {
                consumer.on_punctuation(p);
                continue;
            }
        };
        // Discharge this record's per-row cost from the shared charge
        // handle — the consume half of the per-batch admit/discharge
        // model. The producer charged the whole batch's
        // `EventBatch::estimated_bytes` on flush; subtracting each
        // record's `record_byte_cost` as it drains keeps the slot's live
        // count tracking exactly what is still buffered between producer
        // and consumer. The formula matches the producer's charge, so a
        // fully-drained stream nets the counter back to zero. It runs
        // before `on_record` so a consumer that drops the record still
        // accounts for it.
        charge_handle.sub_bytes(crate::executor::node_buffer::record_byte_cost(
            record.schema().column_count(),
        ));
        if consumer.on_record(record, rn).is_break() {
            // Drain the rest of the channel so the producer's bounded
            // `send` doesn't deadlock — the skeleton must keep consuming
            // until every sender drops even when the consumer is dead,
            // otherwise the producer blocks forever on a full bounded
            // channel. The consumer is not finalized: a fatal `on_record`
            // already abandoned its work.
            while rx.recv().is_ok() {}
            // Nothing is buffered downstream once the channel drains, so
            // zero the slot's charge unconditionally — the fatal path never
            // discharged the records it dropped on the floor above.
            charge_handle.set_bytes(0);
            return;
        }
    }

    // Channel closed — every sender dropped (`recv` returned `Err`), so no
    // more records will arrive. Let the consumer finalize (flush a writer,
    // emit a final group), then pin the charge to zero.
    consumer.on_close();
    charge_handle.set_bytes(0);
}

/// The `Output` instantiation of [`StreamingConsumer`]: projects each
/// record through `project_output_from_record`, lazily constructs the
/// format writer on the first record, calls `Writer::write_record` per
/// record, and flushes at channel close. Errors are accumulated into
/// `out` rather than aborting so the dispatcher can surface them alongside
/// any sibling `output_errors`.
struct OutputStreamConsumer {
    spec: StreamingOutputSpec,
    out: StreamingOutputTaskOutput,
    /// Pending `SchemaScan` timer, finished on the first writer build (or
    /// on close for the empty-stream case).
    scan_timer_slot: Option<stage_metrics::StageTimer>,
    /// Lazily built on the first record's projected schema.
    writer: Option<Box<dyn FormatWriter>>,
    /// Holds the raw sink until the first record triggers the lazy build.
    raw_writer_slot: Option<Box<dyn Write + Send>>,
}

impl OutputStreamConsumer {
    fn new(raw_writer: Box<dyn Write + Send>, spec: StreamingOutputSpec) -> Self {
        Self {
            spec,
            out: StreamingOutputTaskOutput {
                records_written: 0,
                records_emitted: 0,
                seen_row_nums: HashSet::new(),
                write_timer: stage_metrics::CumulativeTimer::new(),
                projection_timer: stage_metrics::CumulativeTimer::new(),
                errors: Vec::new(),
                stage_metrics: Vec::new(),
            },
            scan_timer_slot: Some(stage_metrics::StageTimer::new(
                stage_metrics::StageName::SchemaScan,
            )),
            writer: None,
            raw_writer_slot: Some(raw_writer),
        }
    }
}

impl StreamingConsumer for OutputStreamConsumer {
    fn on_record(&mut self, record: Record, row_num: u64) -> ControlFlow<()> {
        use crate::projection::project_output_from_record;

        let cxl_emit_names_opt: Option<&[String]> = if self.spec.cxl_emit_names.is_empty() {
            None
        } else {
            Some(&self.spec.cxl_emit_names)
        };

        if let Some(expected) = self.spec.expected_input_schema.as_ref()
            && let Err(err) = crate::executor::schema_check::check_input_schema(
                expected,
                record.schema(),
                &self.spec.output_name,
                "output",
                &self.spec.producer_name,
            )
        {
            self.out.errors.push(err);
            return ControlFlow::Continue(());
        }

        let projected = {
            let _guard = self.out.projection_timer.guard();
            project_output_from_record(&record, &self.spec.out_cfg, cxl_emit_names_opt)
        };

        // Lazy writer construction: defer until we have the first record's
        // projected schema so the writer's column list matches what
        // `project_output_from_record` actually emits (same source of truth
        // as the buffered Output arm at dispatch.rs's `output_schema =
        // Arc::clone(projected.schema())`).
        if self.writer.is_none() {
            let raw = self
                .raw_writer_slot
                .take()
                .expect("raw_writer_slot is Some until first record arrives");
            let schema = Arc::clone(projected.schema());
            match build_format_writer(&self.spec.out_cfg, raw, schema) {
                Ok(w) => {
                    self.writer = Some(w);
                    if let Some(timer) = self.scan_timer_slot.take() {
                        self.out.stage_metrics.push(timer.finish(1, 1));
                    }
                }
                Err(e) => {
                    self.out.errors.push(e);
                    if let Some(timer) = self.scan_timer_slot.take() {
                        self.out.stage_metrics.push(timer.finish(0, 0));
                    }
                    return ControlFlow::Break(());
                }
            }
        }

        let write_result = {
            let _guard = self.out.write_timer.guard();
            self.writer
                .as_mut()
                .expect("writer is Some after lazy construction")
                .write_record(&projected)
        };
        match write_result {
            Ok(()) => {
                self.out.records_written += 1;
                self.out.records_emitted += 1;
                self.out.seen_row_nums.insert(row_num);
                ControlFlow::Continue(())
            }
            Err(e) => {
                self.out.errors.push(PipelineError::from(e));
                ControlFlow::Break(())
            }
        }
    }

    fn on_punctuation(&mut self, _punct: Punctuation) {
        // Streaming output is terminal — it writes records straight to disk,
        // so a document boundary needs no writer action here. An Output that
        // declares `reconstruct_envelope: true` (where the boundary DOES drive
        // a writer's `begin_document` / `end_document`) is excluded from this
        // fused path at spec-build time, so no envelope-reconstructing Output
        // ever reaches this consumer; the boundaries it would discard belong
        // only to plain writers that render no framing, leaving the streamed
        // output byte-identical to the boundary-unaware buffered path.
    }

    fn on_close(&mut self) {
        // Flush whatever the writer has buffered. `writer.is_none()` is the
        // empty-stream case (zero records arrived); matches the buffered
        // path's `if unbuffered.is_empty() { return Ok(()); }` short-circuit
        // — the pending scan timer is still finished so the stage metric is
        // emitted with zero rows.
        if let Some(timer) = self.scan_timer_slot.take() {
            self.out.stage_metrics.push(timer.finish(0, 0));
        }
        if let Some(w) = self.writer.as_mut() {
            let flush_result = {
                let _guard = self.out.write_timer.guard();
                w.flush()
            };
            if let Err(e) = flush_result {
                self.out.errors.push(PipelineError::from(e));
            }
        }
    }
}

/// Streaming-output writer thread body — the `Output` instantiation of the
/// generalized streaming-consumer substrate. Builds an
/// [`OutputStreamConsumer`] over the writer lifecycle and drives it with
/// [`drain_streaming_channel`], returning the folded-back per-task
/// counters. See [`StreamingOutputSpec`] for the eligibility predicate and
/// https://github.com/rustpunk/clinker/issues/72 for the rationale.
pub(super) fn streaming_output(
    rx: crossbeam_channel::Receiver<StreamEvent>,
    raw_writer: Box<dyn Write + Send>,
    spec: StreamingOutputSpec,
    charge_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
) -> StreamingOutputTaskOutput {
    let mut consumer = OutputStreamConsumer::new(raw_writer, spec);
    drain_streaming_channel(&rx, &charge_handle, &mut consumer);
    consumer.out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::node_buffer::record_byte_cost;
    use crate::executor::stream_event::PunctuationKind;
    use crate::pipeline::memory::ConsumerHandle;
    use clinker_record::{Schema, Value, synthetic_document_context};
    use std::sync::Arc;

    /// One-column record used to size the per-record discharge against
    /// `record_byte_cost(1)`.
    fn rec(id: i64) -> Record {
        Record::new(
            Arc::new(Schema::new(vec!["id".into()])),
            vec![Value::Integer(id)],
        )
    }

    /// Records every event the skeleton routes, so a test can assert
    /// arrival order, `on_close` invocation count, and the `Break` cutoff.
    /// `break_at_row` makes `on_record` return `Break` on the matching row
    /// number, exercising the deadlock-safe drain-on-fatal path.
    struct Recorder {
        records: Vec<u64>,
        puncts: Vec<PunctuationKind>,
        closes: usize,
        break_at_row: Option<u64>,
    }

    impl Recorder {
        fn new(break_at_row: Option<u64>) -> Self {
            Self {
                records: Vec::new(),
                puncts: Vec::new(),
                closes: 0,
                break_at_row,
            }
        }
    }

    impl StreamingConsumer for Recorder {
        fn on_record(&mut self, _record: Record, row_num: u64) -> ControlFlow<()> {
            self.records.push(row_num);
            if self.break_at_row == Some(row_num) {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }

        fn on_punctuation(&mut self, punct: Punctuation) {
            self.puncts.push(punct.kind());
        }

        fn on_close(&mut self) {
            self.closes += 1;
        }
    }

    #[test]
    fn full_drain_nets_charge_to_zero_and_preserves_order() {
        let (tx, rx) = crossbeam_channel::unbounded::<StreamEvent>();
        let charge = ConsumerHandle::new();
        let n = 4u64;
        // Mirror the producer's per-batch admit: charge the whole stream
        // up front, then let the skeleton discharge each record as it
        // drains.
        charge.add_bytes(record_byte_cost(1) * n);
        for rn in 0..n {
            tx.send(StreamEvent::record(rec(rn as i64), rn)).unwrap();
        }
        drop(tx);

        let mut consumer = Recorder::new(None);
        drain_streaming_channel(&rx, &charge, &mut consumer);

        assert_eq!(consumer.records, vec![0, 1, 2, 3], "records seen in order");
        assert_eq!(consumer.closes, 1, "on_close fires once on clean drain");
        assert_eq!(charge.bytes(), 0, "full drain nets the charge to zero");
    }

    #[test]
    fn break_drains_channel_zeroes_charge_and_skips_close() {
        let (tx, rx) = crossbeam_channel::unbounded::<StreamEvent>();
        let charge = ConsumerHandle::new();
        let n = 5u64;
        charge.add_bytes(record_byte_cost(1) * n);
        for rn in 0..n {
            tx.send(StreamEvent::record(rec(rn as i64), rn)).unwrap();
        }
        drop(tx);

        // Break on row 1: rows 2..5 remain in the channel and must be
        // drained so a bounded producer `send` could not deadlock.
        let mut consumer = Recorder::new(Some(1));
        drain_streaming_channel(&rx, &charge, &mut consumer);

        assert_eq!(
            consumer.records,
            vec![0, 1],
            "stops handing records at Break"
        );
        assert_eq!(
            consumer.closes, 0,
            "on_close is skipped after a fatal Break"
        );
        assert_eq!(charge.bytes(), 0, "charge is zeroed even on the fatal path");
        assert!(rx.try_recv().is_err(), "remaining events are drained");
    }

    #[test]
    fn punctuations_route_without_discharge_in_arrival_order() {
        let (tx, rx) = crossbeam_channel::unbounded::<StreamEvent>();
        let charge = ConsumerHandle::new();
        // Charge only the single record; punctuations contribute nothing.
        charge.add_bytes(record_byte_cost(1));
        let doc = synthetic_document_context();
        tx.send(StreamEvent::punctuation(Punctuation::document_open(
            Arc::clone(&doc),
        )))
        .unwrap();
        tx.send(StreamEvent::record(rec(0), 0)).unwrap();
        tx.send(StreamEvent::punctuation(Punctuation::document_close(
            Arc::clone(&doc),
        )))
        .unwrap();
        drop(tx);

        let mut consumer = Recorder::new(None);
        drain_streaming_channel(&rx, &charge, &mut consumer);

        assert_eq!(
            consumer.puncts,
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ],
            "punctuations routed in arrival order"
        );
        assert_eq!(consumer.records, vec![0]);
        assert_eq!(
            charge.bytes(),
            0,
            "only the record discharged — punctuations carry no cost"
        );
    }

    #[test]
    fn disconnect_fires_close_once_then_zeroes_charge() {
        let (tx, rx) = crossbeam_channel::unbounded::<StreamEvent>();
        let charge = ConsumerHandle::new();
        // Empty stream: no records charged, immediate disconnect.
        drop(tx);

        let mut consumer = Recorder::new(None);
        drain_streaming_channel(&rx, &charge, &mut consumer);

        assert!(consumer.records.is_empty(), "no records on an empty stream");
        assert_eq!(
            consumer.closes, 1,
            "on_close fires exactly once on disconnect"
        );
        assert_eq!(charge.bytes(), 0);
    }
}
