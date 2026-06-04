//! The streaming-output writer thread and its per-Output spec.
//!
//! Installs the fused producer → single-`Output` chains that the plan's
//! streaming-fusion analysis flagged as bypassing a `node_buffers` slot,
//! moving each such Output's writer onto its own thread fed by a bounded
//! channel. The eligibility verdict comes from the plan layer's
//! `streaming_output_producer`, so this runtime install can never disagree
//! with the `--explain` buffer-class annotation.

use std::collections::HashSet;
use std::io::Write;
use std::sync::Arc;

use clinker_record::Schema;

use clinker_format::traits::FormatWriter;
use clinker_plan::config::{OutputConfig, PipelineConfig};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::streaming_output_producer;

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
