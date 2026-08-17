//! `PlanNode::Sink` dispatch arm.
//!
//! Holds the sink-writer body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: writer init, output
//! schema mapping, `include_unmapped` passthrough, the per-record fan-out
//! to source-file-keyed writers, the correlation-buffer capture path, and
//! the streaming-fused output short-circuit. The dispatcher's `Sink` arm
//! is a single delegating call into [`dispatch_sink`].

use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

use clinker_record::{GroupByKey, Record, Schema, Value};
use indexmap::IndexSet;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    CorrelationRecordSlot, ExecutorContext, buffer_key_for_record, mapping_probe,
    missing_node_buffer_input_error, push_dlq, push_write_error, require_node_buffer_input,
    single_input_node_buffer_key, sink_collision_dlq_entry, source_file_path_of,
};
use crate::executor::node_buffer::TransientNodeBufferReservation;
use crate::executor::schema_check::check_input_schema;
use crate::executor::structured_output_guard::{
    StructuredOutputDocumentGuard, structured_output_format,
};
use crate::executor::{DlqEntry, OutputDeliveryId, build_format_writer, stage_metrics};
use crate::projection::project_output_from_record;
use clinker_plan::config::ErrorStrategy;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{
    ExecutionPlanDag, OrderGuarantee, PhysicalWriterBoundary, PlanNode, WriterBoundaryMode,
    WriterOrderDisposition,
};

/// Runtime adapter for one topology-derived physical writer boundary.
///
/// The adapter is constructed from the frozen planner contract and validated
/// against the writer implementation that is about to emit bytes. Deferred
/// boundaries reuse the executor's shared stable authored-key sorter; preserve
/// and proven-terminal-sort boundaries retain FIFO without consulting raw
/// Output configuration or adding a comparison key.
#[derive(Clone)]
pub(crate) struct OrderedWriterBoundary {
    boundary: PhysicalWriterBoundary,
}

type OrderedRecord = Result<(Record, crate::executor::stream_event::SourceRowId), PipelineError>;
type OrderedRecordStream<'a> = Box<dyn Iterator<Item = OrderedRecord> + 'a>;

impl OrderedWriterBoundary {
    /// Resolve the one compiled boundary for `output_id` and assert that the
    /// runtime path matches its planner-selected mode.
    pub(crate) fn for_sink(
        current_dag: &ExecutionPlanDag,
        output_id: clinker_plan::plan::PlanNodeId,
        expected_mode: WriterBoundaryMode,
    ) -> Result<Self, PipelineError> {
        let mut matches = current_dag
            .order_contract()
            .writer_boundaries
            .iter()
            .filter(|boundary| boundary.output_id == output_id);
        let boundary = matches.next().ok_or_else(|| PipelineError::Internal {
            op: "writer_boundary",
            node: format!("{output_id:?}"),
            detail: "compiled Sink has no physical writer boundary".to_string(),
        })?;
        if matches.next().is_some() {
            return Err(PipelineError::Internal {
                op: "writer_boundary",
                node: boundary.output_name.clone(),
                detail: "compiled Sink has more than one physical writer boundary template"
                    .to_string(),
            });
        }
        if boundary.mode != expected_mode {
            return Err(PipelineError::Internal {
                op: "writer_boundary",
                node: boundary.output_name.clone(),
                detail: format!(
                    "compiled writer mode {:?} reached the {:?} byte-emission path",
                    boundary.mode, expected_mode
                ),
            });
        }
        validate_writer_disposition(boundary)?;
        if expected_mode == WriterBoundaryMode::Streaming
            && !matches!(
                boundary.disposition,
                WriterOrderDisposition::Preserve | WriterOrderDisposition::Unordered
            )
        {
            return Err(PipelineError::Internal {
                op: "writer_boundary",
                node: boundary.output_name.clone(),
                detail:
                    "streaming writer boundary cannot consume a complete-population disposition"
                        .to_string(),
            });
        }
        Ok(Self {
            boundary: boundary.clone(),
        })
    }

    pub(crate) fn is_incremental_streaming(&self) -> bool {
        self.boundary.mode == WriterBoundaryMode::Streaming
            && matches!(
                self.boundary.disposition,
                WriterOrderDisposition::Preserve | WriterOrderDisposition::Unordered
            )
    }

    /// Apply the compiled complete-population disposition to records carrying
    /// their real source identity as inert payload. `SourceRowId` never joins
    /// the comparator, so equal authored keys retain arrival order.
    pub(crate) fn order_records(
        &self,
        ctx: &mut ExecutorContext<'_>,
        records: Vec<(Record, crate::executor::stream_event::SourceRowId)>,
    ) -> Result<Vec<(Record, crate::executor::stream_event::SourceRowId)>, PipelineError> {
        self.order_record_stream(ctx, records.into_iter().map(Ok))?
            .collect()
    }

    /// Apply this boundary to a complete population while leaving a spilled
    /// merge lazy. Preserving and unordered boundaries forward their input
    /// iterator untouched; deferred boundaries delegate to the shared stable
    /// sorter and bounded-fan-in merger.
    pub(crate) fn order_record_stream<'a, I>(
        &self,
        ctx: &mut ExecutorContext<'_>,
        records: I,
    ) -> Result<OrderedRecordStream<'a>, PipelineError>
    where
        I: IntoIterator<
                Item = Result<(Record, crate::executor::stream_event::SourceRowId), PipelineError>,
            > + 'a,
        I::IntoIter: 'a,
    {
        let WriterOrderDisposition::DeferredSort { fields } = &self.boundary.disposition else {
            return Ok(Box::new(records.into_iter()));
        };
        let fields: Vec<clinker_plan::config::SortField> = fields
            .iter()
            .map(|field| clinker_plan::config::SortField {
                field: field.field.clone(),
                order: field.order,
                null_order: Some(field.null_order),
            })
            .collect();
        Ok(Box::new(
            crate::executor::sort_dispatch::sort_record_stream_by_authored_fields(
                ctx,
                &self.boundary.output_name,
                &fields,
                records,
            )?,
        ))
    }

    /// Sort records carrying a stable zero-based slot index. The index is
    /// payload only and lets deferred correlation/document commits restore
    /// their record metadata after the shared sorter permutes the population.
    pub(crate) fn order_indexed_records(
        &self,
        ctx: &mut ExecutorContext<'_>,
        records: Vec<(Record, usize)>,
    ) -> Result<Vec<(Record, usize)>, PipelineError> {
        let indexed = records
            .into_iter()
            .map(|(record, index)| {
                let ordinal = u64::try_from(index).map_err(|_| PipelineError::Internal {
                    op: "writer_boundary",
                    node: self.boundary.output_name.clone(),
                    detail: "writer population index exceeds u64".to_string(),
                })?;
                Ok((
                    record,
                    crate::executor::stream_event::SourceRowId::new(
                        self.boundary.output_id,
                        ordinal,
                    ),
                ))
            })
            .collect::<Result<Vec<_>, PipelineError>>()?;
        self.order_records(ctx, indexed)?
            .into_iter()
            .map(|(record, index)| {
                usize::try_from(index.ordinal())
                    .map(|index| (record, index))
                    .map_err(|_| PipelineError::Internal {
                        op: "writer_boundary",
                        node: self.boundary.output_name.clone(),
                        detail: "writer population index does not fit usize".to_string(),
                    })
            })
            .collect()
    }
}

fn validate_writer_disposition(boundary: &PhysicalWriterBoundary) -> Result<(), PipelineError> {
    let valid = match (&boundary.guarantee, &boundary.disposition) {
        (OrderGuarantee::Unordered, WriterOrderDisposition::Unordered)
        | (OrderGuarantee::StableArrival, WriterOrderDisposition::Preserve) => true,
        (
            OrderGuarantee::Sorted(expected),
            WriterOrderDisposition::DeferredSort { fields }
            | WriterOrderDisposition::ProvenTerminalSort { fields, .. },
        ) => expected == fields,
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(PipelineError::Internal {
            op: "writer_boundary",
            node: boundary.output_name.clone(),
            detail: format!(
                "compiled guarantee {:?} is incompatible with writer disposition {:?}",
                boundary.guarantee, boundary.disposition
            ),
        })
    }
}

/// Resolve the [`SinkConfig`](clinker_plan::config::SinkConfig) for the
/// Output named `name`, falling back to the pipeline's primary output when no
/// per-name entry exists. Borrows `ctx` immutably, so the caller resolves it
/// before taking any `&mut ctx`.
fn resolve_out_cfg<'a>(
    ctx: &'a ExecutorContext<'_>,
    name: &str,
) -> &'a clinker_plan::config::SinkConfig {
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
struct SinkInputs {
    expected_input_schema: Option<Arc<clinker_record::Schema>>,
    upstream_name: String,
    cxl_emit_names: Vec<String>,
}

/// Context carrier kept lazy until the node-kind guard has succeeded. Normal
/// dispatch passes the live executor context directly; the feature-gated
/// mismatch matrix uses the inert carrier to prove rejection precedes writer
/// lookup, publication, correlation buffering, and input-buffer access.
pub(crate) enum SinkDispatchContext<'borrow, 'plan> {
    Live(&'borrow mut ExecutorContext<'plan>),
    #[cfg(feature = "test-utils")]
    Inert,
}

impl<'borrow, 'plan> From<&'borrow mut ExecutorContext<'plan>>
    for SinkDispatchContext<'borrow, 'plan>
{
    fn from(ctx: &'borrow mut ExecutorContext<'plan>) -> Self {
        Self::Live(ctx)
    }
}

#[cfg(feature = "test-utils")]
impl crate::executor::dispatch::DispatchFaultGuard {
    /// Execute the real output boundary with an inert context so tests can
    /// prove a wrong node returns before writer or publication state is read.
    #[doc(hidden)]
    pub fn dispatch_sink_mismatch_for_testing(
        current_dag: &ExecutionPlanDag,
        node_idx: NodeIndex,
        node: &PlanNode,
    ) -> Result<(), PipelineError> {
        dispatch_sink(SinkDispatchContext::Inert, current_dag, node_idx, node)
    }
}

/// Derive the [`SinkInputs`] for `node_idx` from the plan. Shared by the
/// records-only, document-DLQ, and envelope Sink arms so the input-binding
/// preamble lives in one place.
fn resolve_sink_inputs(current_dag: &ExecutionPlanDag, node_idx: NodeIndex) -> SinkInputs {
    SinkInputs {
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

/// Execute the `Sink` arm for `node_idx`: open the writer(s), map records
/// onto the declared output schema (passing unmapped fields through when
/// configured), and write — taking the per-record fan-out path for
/// source-file-keyed outputs, the correlation-buffer capture path under a
/// correlation-key pipeline, and the streaming-fused short-circuit when a
/// streaming sender was installed. Deferred physical boundaries block only at
/// their compiled population grain and spill through the shared sorter.
pub(crate) fn dispatch_sink<'borrow, 'plan>(
    ctx: impl Into<SinkDispatchContext<'borrow, 'plan>>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError>
where
    'plan: 'borrow,
{
    let PlanNode::Sink { ref name, .. } = *node else {
        return Err(crate::executor::invariant::dispatch_mismatch(
            "dispatch_sink",
            "sink",
            node.kind_name(),
            node.name(),
        ));
    };
    #[cfg(feature = "test-utils")]
    let SinkDispatchContext::Live(ctx) = ctx.into() else {
        panic!("sink dispatcher accessed inert context after accepting a sink node")
    };
    #[cfg(not(feature = "test-utils"))]
    let SinkDispatchContext::Live(ctx) = ctx.into();

    // The streaming writer thread and correlation commit own their actual
    // terminal work and close their Sink observations there. The topo-order
    // Sink turn is respectively a no-op or a projection/buffering handoff, so
    // reporting it here would emit a pre-write zero-record span and then miss
    // the outcome that establishes the external bytes.
    if ctx.streaming_sink_nodes.contains(&node_idx) || ctx.correlation_buffers.is_some() {
        return dispatch_sink_work(ctx, current_dag, node_idx, node);
    }

    let mut signal = ctx.telemetry_producer.clone().map(|producer| {
        let logical_node = ctx.qualified_node_name(name);
        crate::telemetry::SinkSignal::new(producer, logical_node.into_owned())
    });
    let sink_byte_counter = signal
        .as_ref()
        .map(|_| clinker_format::SharedByteCounter::new());
    debug_assert!(ctx.sink_byte_counter.is_none());
    ctx.sink_byte_counter = sink_byte_counter.clone();
    let records_before = ctx.counters.records_written;
    let errors_before = ctx.output_errors.len();
    let result = dispatch_sink_work(ctx, current_dag, node_idx, node);
    ctx.sink_byte_counter = None;
    if let Some(mut signal) = signal.take() {
        signal.record_records(ctx.counters.records_written.saturating_sub(records_before));
        signal.record_bytes(
            sink_byte_counter
                .as_ref()
                .map_or(0, clinker_format::SharedByteCounter::bytes_written),
        );
        let new_errors = ctx.output_errors.len().saturating_sub(errors_before);
        signal.record_errors(u64::try_from(new_errors).unwrap_or(u64::MAX));
        let interrupted_error = matches!(&result, Err(PipelineError::Interrupted));
        if (result.is_err() && !interrupted_error) || new_errors > 0 {
            signal.fail();
        } else if interrupted_error
            || ctx
                .shutdown_token
                .as_ref()
                .is_some_and(crate::pipeline::shutdown::ShutdownToken::is_requested)
        {
            signal.interrupt();
        } else {
            signal.complete();
        }
    }
    result
}

fn dispatch_sink_work(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Sink {
        ref name,
        ref resolved,
        ..
    } = *node
    else {
        return Err(crate::executor::invariant::dispatch_mismatch(
            "dispatch_sink",
            "sink",
            node.kind_name(),
            node.name(),
        ));
    };
    let output_id = node.id();
    // Streaming-Sink short-circuit (issue #72). The executor
    // entry already moved this Sink's writer into a
    // `std::thread` that drained records from a bounded crossbeam
    // channel populated by the fused Merge arm. Per-record
    // `write_record` already fired concurrently with Merge
    // production; the dispatcher's end-of-DAG join surface joins
    // the thread and folds its counters / timers / errors into
    // the context. The Sink's topo turn here is a no-op.
    if ctx.streaming_sink_nodes.contains(&node_idx) {
        return Ok(());
    }
    // Document-level DLQ short-circuit. When any source declares
    // `dlq_granularity: document`, this Sink's records are decided
    // per-document: buffered until each `DocumentClose`, then flushed clean
    // to the writer or rejected (trigger + collateral) to the DLQ. The
    // driver consumes the INTERLEAVED event stream (records + closes in
    // order), so this path reads the boundary the records-only `drain_split`
    // below discards — an additive read of the same buffer, not a change to
    // the records-vs-puncts split contract every other operator relies on.
    if ctx.document_dlq.is_some() {
        let writer_boundary = OrderedWriterBoundary::for_sink(
            current_dag,
            output_id,
            WriterBoundaryMode::DocumentDlq,
        )?;
        return dispatch_sink_document_dlq(ctx, current_dag, node_idx, name, writer_boundary);
    }
    // Envelope-reconstruction short-circuit. When this Output declares
    // `reconstruct_envelope: true`, its writer's `begin_document` /
    // `end_document` framing must fire around each document's records. This
    // arm detects document boundaries from each record's `doc_ctx`
    // (a change in the per-frame `grain()` between consecutive records) rather
    // than the records-only path's boundary-blind write loop. Preserving
    // boundaries stream each body record; deferred boundaries sort one
    // document through the bounded shared spill path before framing its body.
    if resolve_out_cfg(ctx, name).reconstruct_envelope {
        let writer_boundary =
            OrderedWriterBoundary::for_sink(current_dag, output_id, WriterBoundaryMode::Envelope)?;
        return dispatch_sink_envelope(ctx, current_dag, node_idx, name, writer_boundary);
    }
    let correlation_deferred = ctx.correlation_buffers.is_some();
    let expected_mode = if correlation_deferred {
        WriterBoundaryMode::CorrelationDeferred
    } else if resolved
        .as_deref()
        .is_some_and(|payload| payload.fan_out_per_source_file)
    {
        WriterBoundaryMode::PerSourceFile
    } else {
        WriterBoundaryMode::RecordsOnly
    };
    let writer_boundary = OrderedWriterBoundary::for_sink(current_dag, output_id, expected_mode)?;
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
    use petgraph::visit::EdgeRef;
    let edge = current_dag
        .graph
        .edges_directed(node_idx, Direction::Incoming)
        .next()
        .ok_or_else(|| missing_sink_input_error(current_dag, node_idx, name))?;
    let producer = edge.source();
    let producer_port = edge.weight().producer_port.as_deref();
    let input_key =
        single_input_node_buffer_key(&ctx.node_buffers, node_idx, producer, producer_port);
    let input = require_node_buffer_input(
        ctx,
        input_key,
        name,
        current_dag.graph[producer].name(),
        producer_port,
    )?;
    let (input, input_materialization_reservation) =
        input.into_materialized_parts(&ctx.memory_budget, name)?;
    let (mut input_records, _input_puncts) = input.drain_split()?;

    // Correlation queues commit only after every group has reached its final
    // clean/dirty disposition. Sorting here would be destroyed by the group
    // walk, so that complete population consumes the same boundary in
    // `commit_correlation_buffers`. Ordinary and per-file paths are complete
    // now and can enforce their disposition before projection or writing.
    if !correlation_deferred {
        input_records = writer_boundary.order_records(ctx, input_records)?;
    }

    let SinkInputs {
        expected_input_schema,
        upstream_name,
        cxl_emit_names,
    } = resolve_sink_inputs(current_dag, node_idx);
    if let Some(expected) = expected_input_schema.as_ref() {
        for (record, _) in &input_records {
            check_input_schema(expected, record.schema(), name, "sink", &upstream_name)?;
        }
    }

    // When correlation buffering is active, every record
    // routed to this Output goes through the per-group buffer
    // — `CorrelationCommit` decides at end-of-DAG whether to
    // flush the group to the writer or DLQ it. Null-keyed
    // records get a row-disambiguated buffer cell each so
    // they retain per-record-rejection semantics without
    // splitting the writer path.
    let buffered: Vec<(
        Record,
        crate::executor::stream_event::SourceRowId,
        Vec<GroupByKey>,
    )>;
    let unbuffered: Vec<(Record, crate::executor::stream_event::SourceRowId)>;
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
    if !unbuffered.is_empty() && structured_output_format(&out_cfg.format).is_some() {
        if ctx.fan_out_writers.contains_key(name) {
            let mut guards: HashMap<Arc<str>, StructuredOutputDocumentGuard> = HashMap::new();
            for (record, _) in &unbuffered {
                let Some(file_path) = source_file_path_of(record) else {
                    continue;
                };
                let file_arc: Arc<str> = Arc::from(file_path);
                let guard = guards
                    .entry(file_arc)
                    .or_insert_with(|| StructuredOutputDocumentGuard::new(&out_cfg.format));
                if let Err(err) = guard.observe(name, record.doc_ctx()) {
                    ctx.output_errors.push(err);
                    return Ok(());
                }
            }
        } else {
            let mut guard = StructuredOutputDocumentGuard::new(&out_cfg.format);
            for (record, _) in &unbuffered {
                if let Err(err) = guard.observe(name, record.doc_ctx()) {
                    ctx.output_errors.push(err);
                    return Ok(());
                }
            }
        }
    }

    // `include_unmapped: false` consults the upstream CXL emit names (resolved
    // in the preamble above) to drop upstream passthroughs the user did not
    // explicitly emit.
    let cxl_emit_names_opt: Option<&[String]> = if cxl_emit_names.is_empty() {
        None
    } else {
        Some(&cxl_emit_names)
    };

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
    //   all Sink arms via the `ok_source_rows` set
    //   declared at function scope.
    //
    // Buffered records DEFER counter increments to the
    // `CorrelationCommit` arm — clean groups bump
    // counters at flush time; dirty groups never count
    // toward `ok_count`. The unbuffered records' ok / written / emitted counts
    // are applied AFTER the write below, so a record a `join_values` collision
    // dead-letters is counted once (as DLQ) rather than as both written and DLQ
    // — and `ok_source_rows` is only ever inserted into on a successful write,
    // never removed, so a row written OK at one Output and dead-lettered at
    // another still counts as ok exactly once regardless of Output order.

    // Derive output schema from first emitted record.
    // The Record is authoritative post-rip; materialize
    // the output-projection's `emitted` / `metadata`
    // maps from it on demand at this boundary. That
    // pays the bucket-insert cost once per record
    // reaching the writer, not every intermediate node
    // transition (Invariant 3).
    let scan_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
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
            // Do not feed the run-wide mapping probe yet: a dirty correlation
            // group is rejected wholesale, so its fields did not populate the
            // delivered file. Clean groups contribute their evidence at the
            // commit boundary without rebuilding this projection.
            let projected =
                crate::projection::project_output_from_record(record, out_cfg, cxl_emit_names_opt);
            let entry = buffers.entry(group_key.clone()).or_default();
            entry.total_records += 1;
            if max_buf > 0 && entry.total_records > max_buf {
                entry.overflowed = true;
            }
            entry.records.push(CorrelationRecordSlot {
                row_num: *rn,
                consumer: output_id,
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

    // CSV under `include_unmapped` is the one writer whose column header can
    // widen record-to-record: `auto_widen` surfaces a `$widened` sidecar
    // column on a later record that the first record lacked. Pinning the
    // header to the first record's projection would silently drop that column
    // (issue #805). This buffered records-only arm has the whole batch
    // materialized in RAM, so it can pre-scan for the UNION of every record's
    // projected columns in first-seen order and write a lossless widened
    // header. Every other CSV path lacks that materialization — the streaming
    // fused arm (Merge / Transform → Output) and the envelope arm both write
    // record-at-a-time under a bounded-memory budget — so a union is
    // impossible there and drift surfaces loudly as `FormatError::SchemaDrift`
    // at the writer (the CSV writer's `error_on_undeclared_columns` guard,
    // and the fixed-width writer's unconditional guard). JSON / XML need
    // neither: they self-describe each record.
    let output_schema = if matches!(out_cfg.format, clinker_plan::config::OutputFormat::Csv(_))
        && out_cfg.include_unmapped
    {
        let _guard = ctx.projection_timer.guard();
        build_csv_union_schema(&unbuffered, out_cfg, cxl_emit_names_opt)
    } else {
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
    let fan_out_paths = ctx.fan_out_paths.remove(name).unwrap_or_default();
    let single_writer = if fan_out_writers.is_none() {
        ctx.writers.remove(name)
    } else {
        None
    };
    // Whether this Output actually has a writer to emit through. When it does
    // not — a dry-run, or an Output whose writer a sibling already took — no
    // record is written, but the counters still bump per record so a dry-run
    // reports what WOULD be written and flag-on (no-op envelope hooks) stays
    // count-identical to flag-off (the invariant the envelope arm documents).
    // With a writer present, only the records actually written are counted, so a
    // `join_values` collision that dead-letters a record does not also count it
    // as written.
    let had_writer = fan_out_writers.is_some() || single_writer.is_some();
    let strategy = ctx.strategy;
    // Collected inside the writer helpers (which hold only partial `ctx`
    // borrows) and applied below once `ctx` is free again: `dlq_pending` drains
    // through `push_dlq`, `written_rows` drives the ok/written/emitted counts.
    let mut dlq_pending: Vec<DlqEntry> = Vec::new();
    let mut written_rows: Vec<crate::executor::stream_event::SourceRowId> = Vec::new();
    let output_staging = ctx.output_staging.clone();
    {
        let mut fan_ctx = FanOutContext {
            name,
            out_cfg,
            cxl_emit_names_opt,
            output_schema: &output_schema,
            output_errors: &mut ctx.output_errors,
            write_timer: &mut ctx.write_timer,
            projection_timer: &mut ctx.projection_timer,
            collector: &mut *ctx.collector,
            strategy,
            dlq_pending: &mut dlq_pending,
            written_rows: &mut written_rows,
            output_staging: &output_staging,
            sink_byte_counter: ctx.sink_byte_counter.clone(),
            mapping_probe: out_cfg
                .mapping
                .as_ref()
                .map(|_| mapping_probe(&mut ctx.mapping_probes, name, out_cfg)),
        };
        if let Some(per_file) = fan_out_writers {
            emit_fan_out(
                &mut fan_ctx,
                &unbuffered,
                per_file,
                fan_out_paths,
                scan_timer,
            );
        } else if let Some(raw_writer) = single_writer {
            emit_single_writer(&mut fan_ctx, raw_writer, &unbuffered, scan_timer);
        }
    }
    // With a writer present, count ok / written / emitted from the records
    // actually written — one entry in `written_rows` per successful
    // `write_record`. Insert each into the run-shared `ok_source_rows` on success
    // only (never remove), matching the streaming arm: a record a collision
    // dead-letters is never in `written_rows`, yet a written sibling that merely
    // shares its source row_num (a `combine match: all` fan-out) still counts,
    // and a source row written OK at one Output and dead-lettered at another
    // counts as ok exactly once regardless of Output order. Without a writer, no
    // record was written, so `written_rows` is empty; fall back to the per-record
    // bump over `unbuffered` to preserve the dry-run / flag-parity count.
    let counted: &[(Record, crate::executor::stream_event::SourceRowId)] =
        if had_writer { &[] } else { &unbuffered };
    let mut newly_ok: u64 = 0;
    let mut written_total: u64 = written_rows.len() as u64;
    for row_num in &written_rows {
        ctx.ok_deliveries
            .insert(OutputDeliveryId::new(*row_num, output_id));
        if ctx.ok_source_rows.insert(*row_num) {
            newly_ok += 1;
        }
    }
    for (_, row_num) in counted {
        written_total += 1;
        ctx.ok_deliveries
            .insert(OutputDeliveryId::new(*row_num, output_id));
        if ctx.ok_source_rows.insert(*row_num) {
            newly_ok += 1;
        }
    }
    ctx.counters.ok_count += newly_ok;
    ctx.counters.records_written += written_total;
    ctx.records_emitted += written_total;

    // Drain the collected collision entries; `push_dlq` enforces the DLQ rate
    // ceiling (E315/E316), which can still abort the run.
    for entry in dlq_pending {
        push_dlq(ctx, entry)?;
    }

    // The duplicate records have completed their synchronous Output use.
    drop(input_materialization_reservation);
    Ok(())
}

/// Whether `record` carries auto-widened extra columns in its `$widened`
/// sidecar — a non-empty `Value::Map`. Absent / `Null` / empty means the
/// record contributes only the shared declared columns (E315 guarantees one
/// declared schema across a batch), so the CSV union pre-scan can skip
/// re-projecting it.
fn record_has_widened_extras(record: &Record) -> bool {
    matches!(
        record.get(clinker_plan::config::pipeline_node::WIDENED_SIDECAR_COLUMN),
        Some(Value::Map(map)) if !map.is_empty()
    )
}

/// Build the CSV writer's column header as the UNION of every record's
/// projected columns, in first-seen order, so a column that `auto_widen`
/// surfaces only on a later record still gets a header slot (records that
/// lack it emit an empty cell there) rather than being silently dropped.
///
/// Seeds the order from the first record's projection — the shared declared
/// columns plus whatever `auto_widen` already surfaced on it — then merges in
/// the extra columns of every LATER record that carries a non-empty
/// `$widened` sidecar. Records without widened extras contribute only the
/// shared declared columns, already in the set, so the second projection pass
/// touches only the drifters: 1x record memory (records are already
/// materialized at this arm), and a second projection only on the drifting
/// minority.
fn build_csv_union_schema(
    unbuffered: &[(Record, crate::executor::stream_event::SourceRowId)],
    out_cfg: &clinker_plan::config::SinkConfig,
    cxl_emit_names_opt: Option<&[String]>,
) -> Arc<Schema> {
    let mut union: IndexSet<Box<str>> = IndexSet::new();
    let first = project_output_from_record(&unbuffered[0].0, out_cfg, cxl_emit_names_opt);
    for col in first.schema().columns() {
        union.insert(col.clone());
    }
    for (record, _) in &unbuffered[1..] {
        if !record_has_widened_extras(record) {
            continue;
        }
        let projected = project_output_from_record(record, out_cfg, cxl_emit_names_opt);
        for col in projected.schema().columns() {
            union.insert(col.clone());
        }
    }
    Arc::new(Schema::new(union.into_iter().collect()))
}

/// Execute the `Output` arm under document-level DLQ. Drains this Output's
/// input as the INTERLEAVED record + `DocumentClose` event stream and hands
/// it to a [`crate::executor::document_dlq::DocumentDlqDriver`], which
/// buffers each record per document and, at each close, flushes the
/// document clean to the writer or rejects it (trigger + collateral) to the
/// DLQ. Blocking at the document grain — a buffered record is not written
/// until its close decides the document clean; peak memory is the
/// concurrently-open documents, spillable under budget.
fn dispatch_sink_document_dlq(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
    writer_boundary: OrderedWriterBoundary,
) -> Result<(), PipelineError> {
    let (events, _input_clone_reservation) =
        drain_sink_input_events(ctx, current_dag, node_idx, name)?;

    let SinkInputs {
        expected_input_schema,
        upstream_name,
        cxl_emit_names,
    } = resolve_sink_inputs(current_dag, node_idx);
    if let Some(expected) = expected_input_schema.as_ref() {
        for event in &events {
            if let crate::executor::stream_event::StreamEvent::Record(record, _) = event {
                check_input_schema(expected, record.schema(), name, "sink", &upstream_name)?;
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
    let driver = crate::executor::document_dlq::DocumentDlqDriver::new(
        ctx,
        name,
        out_cfg,
        cxl_emit_names,
        writer_boundary,
    );
    driver.run(ctx, events)
}

/// Drain this Sink's input buffer as the INTERLEAVED `StreamEvent`
/// stream, preserving record/`DocumentClose` ordering — the boundary the
/// records-only `drain_split` path discards — and materialize it into a
/// `Vec` for the document-DLQ driver (which buffers per document anyway, so
/// has no use for the lazy form). Delegates the predecessor-selection walk to
/// [`drain_sink_input_event_iter`] so the two boundary-aware Sink arms
/// share one mechanism.
fn drain_sink_input_events(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
) -> Result<
    (
        Vec<crate::executor::stream_event::StreamEvent>,
        Option<TransientNodeBufferReservation>,
    ),
    PipelineError,
> {
    let input = drain_sink_input_event_iter(ctx, current_dag, node_idx, name, true)?;
    let (events, reservation) = input.into_parts();
    Ok((events.collect::<Result<Vec<_>, _>>()?, reservation))
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
/// Bounded-memory: a preserving boundary holds only the current document's
/// context. A deferred boundary feeds one document into the shared spillable
/// sorter and drains its bounded-fan-in merge lazily, so it never materializes
/// the sorted document in a second collection. The input drain is itself lazy.
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
fn dispatch_sink_envelope(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
    writer_boundary: OrderedWriterBoundary,
) -> Result<(), PipelineError> {
    let SinkInputs {
        expected_input_schema,
        upstream_name,
        cxl_emit_names,
    } = resolve_sink_inputs(current_dag, node_idx);
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

    let mut events = drain_sink_input_event_iter(ctx, current_dag, node_idx, name, false)?;
    let mut driver = EnvelopeWriterDriver::default();
    let mut structured_guard = StructuredOutputDocumentGuard::new(&out_cfg.format);
    let mut pending = None;
    let processing_result = (|| {
        loop {
            let first = match pending.take() {
                Some(record) => record,
                None => match next_envelope_record(&mut events)? {
                    Some(record) => record,
                    None => break,
                },
            };
            any_record = true;
            let grain =
                crate::executor::document_dlq::is_concrete_file(first.0.doc_ctx().source_file())
                    .then(|| first.0.doc_ctx().grain());
            let mut first = Some(first);
            let mut population_done = false;
            let population = std::iter::from_fn(|| {
                if let Some(record) = first.take() {
                    return Some(Ok(record));
                }
                if population_done {
                    return None;
                }
                match next_envelope_record(&mut events) {
                    Err(error) => {
                        population_done = true;
                        Some(Err(error))
                    }
                    Ok(None) => {
                        population_done = true;
                        None
                    }
                    Ok(Some(record)) => {
                        let same_document = grain.is_some_and(|grain| {
                            crate::executor::document_dlq::is_concrete_file(
                                record.0.doc_ctx().source_file(),
                            ) && record.0.doc_ctx().grain() == grain
                        });
                        if same_document {
                            Some(Ok(record))
                        } else {
                            pending = Some(record);
                            population_done = true;
                            None
                        }
                    }
                }
            });
            let checked = population.map(|item| {
                let (record, row_num) = item?;
                if let Some(expected) = expected_input_schema.as_ref() {
                    check_input_schema(expected, record.schema(), name, "sink", &upstream_name)?;
                }
                Ok((record, row_num))
            });
            let ordered = writer_boundary.order_record_stream(ctx, checked)?;
            for item in ordered {
                let (record, row_num) = item?;
                if let Err(err) = structured_guard.observe(name, record.doc_ctx()) {
                    ctx.output_errors.push(err);
                    return Ok::<(), PipelineError>(());
                }
                let projected = {
                    let _guard = ctx.projection_timer.guard();
                    match out_cfg.mapping.as_ref() {
                        Some(_) => {
                            let probe = mapping_probe(&mut ctx.mapping_probes, name, &out_cfg);
                            crate::projection::project_output_probed(
                                &record,
                                &out_cfg,
                                cxl_emit_names_opt,
                                Some(probe),
                            )
                        }
                        None => project_output_from_record(&record, &out_cfg, cxl_emit_names_opt),
                    }
                };
                {
                    let _guard = ctx.write_timer.guard();
                    driver.on_record(record.doc_ctx(), &projected, &mut |schema| {
                        let raw_writer = ctx.writers.remove(name)?;
                        Some(build_format_writer(
                            &out_cfg,
                            raw_writer,
                            schema,
                            ctx.output_staging.clone(),
                            ctx.sink_byte_counter.clone(),
                        ))
                    });
                }
                if !driver.errors.is_empty() {
                    return Ok(());
                }
                ctx.counters.records_written += 1;
                ctx.records_emitted += 1;
                if ctx.ok_source_rows.insert(row_num) {
                    ctx.counters.ok_count += 1;
                }
            }
        }
        Ok(())
    })();
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
    processing_result
}

fn next_envelope_record(
    events: &mut dyn Iterator<
        Item = Result<crate::executor::stream_event::StreamEvent, PipelineError>,
    >,
) -> Result<Option<(Record, crate::executor::stream_event::SourceRowId)>, PipelineError> {
    for event in events {
        if let crate::executor::stream_event::StreamEvent::Record(record, row_num) = event? {
            return Ok(Some((record, row_num)));
        }
    }
    Ok(None)
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
            // A `join_values` `on_conflict: error` collision is routed to the DLQ
            // on the record-granularity Sink arms (buffered + streaming). This
            // envelope-reconstruction arm holds only the projected record and no
            // `ctx` here, and a collision interacts with the per-document framing
            // it drives, so — like the correlation-commit and document-DLQ arms —
            // it keeps the existing fatal disposition, tracked as a follow-up
            // rather than approximated here.
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

/// Lazily drain this Sink's input as the INTERLEAVED `StreamEvent` stream,
/// preserving record/boundary ordering — the envelope-reconstruction analog
/// of [`drain_sink_input_events`], but yielding an iterator rather than a
/// `Vec` so a spilled predecessor buffer streams from disk one event at a
/// time instead of materializing. Mirrors the per-record path's own-slot-first
/// selection, then lets the producer-declared reader ledger choose a shared
/// sequential scan or transfer the authoritative predecessor generation to
/// its final reader. A spill-backed scan opens one chunk at a time.
#[must_use = "a Sink input must retain its scan reservation"]
struct SinkInputEventIter {
    events:
        Box<dyn Iterator<Item = Result<crate::executor::stream_event::StreamEvent, PipelineError>>>,
    reservation: Option<TransientNodeBufferReservation>,
}

impl SinkInputEventIter {
    fn into_parts(
        self,
    ) -> (
        Box<dyn Iterator<Item = Result<crate::executor::stream_event::StreamEvent, PipelineError>>>,
        Option<TransientNodeBufferReservation>,
    ) {
        (self.events, self.reservation)
    }
}

impl Iterator for SinkInputEventIter {
    type Item = Result<crate::executor::stream_event::StreamEvent, PipelineError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.events.next()
    }
}

fn drain_sink_input_event_iter(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
    materializes: bool,
) -> Result<SinkInputEventIter, PipelineError> {
    use petgraph::visit::EdgeRef;
    let edge = current_dag
        .graph
        .edges_directed(node_idx, Direction::Incoming)
        .next()
        .ok_or_else(|| missing_sink_input_error(current_dag, node_idx, name))?;
    let producer = edge.source();
    let producer_port = edge.weight().producer_port.as_deref();
    let input_key =
        single_input_node_buffer_key(&ctx.node_buffers, node_idx, producer, producer_port);
    let input = require_node_buffer_input(
        ctx,
        input_key,
        name,
        current_dag.graph[producer].name(),
        producer_port,
    )?;
    let (input, reservation) = if materializes {
        input.into_materialized_parts(&ctx.memory_budget, name)?
    } else {
        input.into_parts()
    };
    Ok(SinkInputEventIter {
        events: Box::new(input.drain()),
        reservation,
    })
}

/// Build the fail-loud diagnostic only after every valid Sink input location
/// (own slot, predecessor drain, or predecessor shared scan) has been checked.
#[cold]
fn missing_sink_input_error(
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
) -> PipelineError {
    use petgraph::visit::EdgeRef;

    let incoming = current_dag
        .graph
        .edges_directed(node_idx, Direction::Incoming)
        .next();
    match incoming {
        Some(edge) => missing_node_buffer_input_error(
            name,
            current_dag.graph[edge.source()].name(),
            edge.weight().producer_port.as_deref(),
        ),
        None => missing_node_buffer_input_error(name, "<missing predecessor>", None),
    }
}

/// The Sink node's resolved write target plus the run-scoped writer
/// state both write paths share. Bundling the four cross-branch borrows
/// (error sink, the write / projection cumulative timers, and the
/// stage-metric collector) with the per-call write descriptor (output
/// name, resolved [`SinkConfig`](clinker_plan::config::SinkConfig), explicit
/// CXL emit names, and the projected output schema) keeps the fan-out and
/// single-writer helpers below clippy's argument threshold and gives them
/// one shared shape — a change to how a Sink write is attributed (e.g.
/// a new metric guard) lands on the struct, not on two signatures.
struct FanOutContext<'a> {
    name: &'a str,
    out_cfg: &'a clinker_plan::config::SinkConfig,
    cxl_emit_names_opt: Option<&'a [String]>,
    output_schema: &'a Arc<clinker_record::Schema>,
    output_errors: &'a mut Vec<PipelineError>,
    write_timer: &'a mut crate::executor::stage_metrics::CumulativeTimer,
    projection_timer: &'a mut crate::executor::stage_metrics::CumulativeTimer,
    collector: &'a mut crate::executor::stage_metrics::StageCollector,
    /// This Sink's `mapping:` resolution evidence, carried in so the per-record
    /// projections below feed the same probe the rest of the run's arms do.
    mapping_probe: Option<&'a mut crate::projection::MappingProbe>,
    /// The run's error strategy, so a `join_values` `on_conflict: error`
    /// collision dead-letters under `Continue` but still aborts under
    /// `FailFast` — the same disposition every other per-record failure gets.
    strategy: ErrorStrategy,
    /// Collision DLQ entries gathered during the write. Drained through
    /// [`push_dlq`] by the caller once the full `ctx` borrow is free (the
    /// writer helpers hold only partial borrows of it).
    dlq_pending: &'a mut Vec<DlqEntry>,
    /// The source `row_num` of every record actually written, in write order —
    /// one entry per successful `write_record`, so a `combine match: all`
    /// fan-out that emits several output records for one driver row contributes
    /// several entries. The caller counts `records_written`/`records_emitted`
    /// from its length and inserts each into `ok_source_rows`, matching the
    /// streaming arm; a record a collision dead-letters is simply never pushed
    /// here, so a written sibling sharing its row_num still counts.
    written_rows: &'a mut Vec<crate::executor::stream_event::SourceRowId>,
    output_staging: &'a crate::output::staging::OutputStagingRegistry,
    /// Optional fixed-size counter for this Sink work unit. Fan-out writers
    /// share it so the terminal metric is the sum of physical bytes.
    sink_byte_counter: Option<clinker_format::SharedByteCounter>,
}

/// Write `unbuffered` through a single pre-opened writer. Errors from
/// writer construction / `write_record` / `flush` land in the context's
/// error sink rather than short-circuiting, so sibling Outputs still get
/// their chance to fail.
fn emit_single_writer(
    fan_ctx: &mut FanOutContext<'_>,
    raw_writer: Box<dyn Write + Send>,
    unbuffered: &[(Record, crate::executor::stream_event::SourceRowId)],
    scan_timer: crate::executor::stage_metrics::StageTimer,
) {
    match build_format_writer(
        fan_ctx.out_cfg,
        raw_writer,
        Arc::clone(fan_ctx.output_schema),
        fan_ctx.output_staging.clone(),
        fan_ctx.sink_byte_counter.clone(),
    ) {
        Ok(mut csv_writer) => {
            fan_ctx.collector.record(scan_timer.finish(1, 1));
            let mut write_failed = false;
            for (record, rn) in unbuffered {
                let projected = {
                    let _guard = fan_ctx.projection_timer.guard();
                    match fan_ctx.mapping_probe.as_deref_mut() {
                        Some(probe) => crate::projection::project_output_staged(
                            record,
                            fan_ctx.out_cfg,
                            fan_ctx.cxl_emit_names_opt,
                            probe,
                        ),
                        None => crate::projection::project_output_from_record(
                            record,
                            fan_ctx.out_cfg,
                            fan_ctx.cxl_emit_names_opt,
                        ),
                    }
                };
                let write_result = {
                    let _guard = fan_ctx.write_timer.guard();
                    csv_writer.write_record(&projected)
                };
                if let Err(e) = write_result {
                    if let Some(probe) = fan_ctx.mapping_probe.as_deref_mut() {
                        probe.discard_staged_record();
                    }
                    // A `join_values` `on_conflict: error` collision dead-letters
                    // the one offending record and keeps writing the rest (unless
                    // FailFast); any other write error is fatal for this writer.
                    if fan_ctx.strategy != ErrorStrategy::FailFast
                        && let Some(entry) = sink_collision_dlq_entry(record, *rn, fan_ctx.name, &e)
                    {
                        fan_ctx.dlq_pending.push(entry);
                        continue;
                    }
                    push_write_error(fan_ctx.output_errors, e);
                    write_failed = true;
                    break;
                }
                // Advisories describe the delivered file. A record rejected by
                // `join_values on_conflict: error` above must not contribute
                // mapping evidence or hide an all-empty written column.
                if let Some(probe) = fan_ctx.mapping_probe.as_deref_mut() {
                    probe.commit_staged_record();
                }
                fan_ctx.written_rows.push(*rn);
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
    unbuffered: &[(Record, crate::executor::stream_event::SourceRowId)],
    per_file: HashMap<Arc<str>, Box<dyn Write + Send>>,
    mut resolved_paths: HashMap<Arc<str>, String>,
    scan_timer: crate::executor::stage_metrics::StageTimer,
) {
    use std::collections::HashMap as Hm;

    // Build one format writer per pre-opened raw writer. Failed
    // construction for one file does NOT abort the whole output —
    // siblings still get their chance.
    let mut format_writers: Hm<Arc<str>, Box<dyn clinker_format::FormatWriter>> = Hm::new();
    for (file_arc, raw) in per_file {
        let mut resolved_config = fan_ctx.out_cfg.clone();
        if let Some(path) = resolved_paths.remove(&file_arc) {
            resolved_config.path = path;
            resolved_config.resolved_path_template = None;
        } else if resolved_config.split.is_some() {
            fan_ctx.output_errors.push(PipelineError::Internal {
                op: "fan_out",
                node: fan_ctx.name.to_string(),
                detail: format!(
                    "split fan-out writer for source file {file_arc:?} has no resolved base path"
                ),
            });
            continue;
        }
        match build_format_writer(
            &resolved_config,
            raw,
            Arc::clone(fan_ctx.output_schema),
            fan_ctx.output_staging.clone(),
            fan_ctx.sink_byte_counter.clone(),
        ) {
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
            match fan_ctx.mapping_probe.as_deref_mut() {
                Some(probe) => crate::projection::project_output_staged(
                    record,
                    fan_ctx.out_cfg,
                    fan_ctx.cxl_emit_names_opt,
                    probe,
                ),
                None => crate::projection::project_output_from_record(
                    record,
                    fan_ctx.out_cfg,
                    fan_ctx.cxl_emit_names_opt,
                ),
            }
        };
        let write_result = {
            let _guard = fan_ctx.write_timer.guard();
            fw.write_record(&projected)
        };
        if let Err(e) = write_result {
            if let Some(probe) = fan_ctx.mapping_probe.as_deref_mut() {
                probe.discard_staged_record();
            }
            if fan_ctx.strategy != ErrorStrategy::FailFast
                && let Some(entry) = sink_collision_dlq_entry(record, *rn, fan_ctx.name, &e)
            {
                fan_ctx.dlq_pending.push(entry);
                continue;
            }
            push_write_error(fan_ctx.output_errors, e);
            continue;
        }
        if let Some(probe) = fan_ctx.mapping_probe.as_deref_mut() {
            probe.commit_staged_record();
        }
        fan_ctx.written_rows.push(*rn);
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
    use crate::executor::node_buffer::{NodeBuffer, reserve_node_buffer_materialization};
    use clinker_format::FormatWriter;
    use clinker_format::error::FormatError;
    use clinker_record::{DocumentContext, DocumentId, FieldResolver, Schema, Value};
    use std::sync::Mutex;

    struct FixedUsage(u64);

    impl crate::pipeline::memory::MemoryConsumer for FixedUsage {
        fn current_usage(&self) -> u64 {
            self.0
        }

        fn spill_priority(&self) -> i32 {
            i32::MAX
        }

        fn try_spill(
            &self,
            target_bytes: u64,
        ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
            Err(crate::pipeline::memory::ConsumerSpillError::BelowTarget {
                target: target_bytes,
                freed: 0,
            })
        }

        fn can_back_pressure(&self) -> bool {
            false
        }
    }

    fn sink_input_clone_fixture() -> NodeBuffer {
        let schema = Arc::new(Schema::new(vec!["id".into()]));
        NodeBuffer::memory_from_records(vec![(Record::new(schema, vec![Value::Integer(1)]), 1)])
    }

    #[test]
    fn ordinary_sink_materialization_preflight_returns_node_buffer_e310_at_baseline() {
        let input = sink_input_clone_fixture();
        let clone_bytes = input.estimated_memory_bytes();
        let hard_limit = 100 * 1024 * 1024 * 1024;
        let baseline_usage = hard_limit - clone_bytes + 1;
        let budget = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            hard_limit,
            0.80,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        let baseline_id = budget.register_consumer(Arc::new(FixedUsage(baseline_usage)));

        match reserve_node_buffer_materialization(clone_bytes, &budget, "ordinary_out") {
            Err(PipelineError::MemoryBudgetExceeded {
                node,
                used,
                limit,
                source,
                ..
            }) => {
                assert_eq!(node, "ordinary_out");
                assert_eq!(used, hard_limit + 1);
                assert_eq!(limit, hard_limit);
                assert_eq!(source, clinker_plan::BudgetCategory::NodeBuffer);
            }
            Ok(_) => panic!("Output materialization must be rejected before allocation"),
            Err(other) => panic!("expected Output E310 NodeBuffer; got {other:?}"),
        }
        assert_eq!(budget.consumer_count(), 1);
        assert_eq!(budget.sum_consumer_usage(), baseline_usage);
        budget.unregister_consumer(baseline_id);
    }

    #[test]
    fn envelope_sink_iterator_holds_materialization_charge_through_iteration() {
        let mut input = sink_input_clone_fixture();
        let clone_bytes = input.estimated_memory_bytes();
        let budget = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            100 * 1024 * 1024 * 1024,
            0.80,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        let reservation = reserve_node_buffer_materialization(clone_bytes, &budget, "envelope_out")
            .expect("roomy Output materialization");
        let reread = input.reread().expect("re-readable Output input");
        let mut events = SinkInputEventIter {
            events: Box::new(reread.drain()),
            reservation: Some(reservation),
        };

        assert_eq!(budget.sum_consumer_usage(), clone_bytes);
        assert!(events.next().expect("one event").is_ok());
        assert_eq!(
            budget.sum_consumer_usage(),
            clone_bytes,
            "the lazy envelope iterator owns the reservation until it drops"
        );
        drop(events);
        assert_eq!(budget.consumer_count(), 0);
        assert_eq!(budget.sum_consumer_usage(), 0);
    }

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
