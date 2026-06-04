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

use crate::error::PipelineError;
use crate::executor::dispatch::{
    CorrelationRecordSlot, ExecutorContext, buffer_key_for_record, drain_node_buffer_slot,
    source_file_path_of,
};
use crate::executor::schema_check::check_input_schema;
use crate::executor::{build_format_writer, stage_metrics};
use crate::plan::execution::{ExecutionPlanDag, PlanNode};
use crate::projection::project_output_from_record;

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

    if let Some(expected) = current_dag.graph[node_idx]
        .expected_input_schema_in(current_dag)
        .cloned()
    {
        let upstream_name = current_dag
            .graph
            .neighbors_directed(node_idx, Direction::Incoming)
            .next()
            .map(|i| current_dag.graph[i].name().to_string())
            .unwrap_or_default();
        for (record, _) in &input_records {
            check_input_schema(&expected, record.schema(), name, "output", &upstream_name)?;
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
    let out_cfg = ctx
        .output_configs
        .iter()
        .find(|o| o.name == *name)
        .unwrap_or(ctx.primary_output);

    // Compute the upstream node's CXL emit names once.
    // `include_unmapped: false` consults this to drop upstream
    // passthroughs the user did not explicitly emit.
    let cxl_emit_names = current_dag.graph[node_idx].cxl_emit_names_in(current_dag);
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
    if let Some(per_file) = ctx.fan_out_writers.remove(name) {
        emit_fan_out(
            name,
            out_cfg,
            cxl_emit_names_opt,
            &output_schema,
            &unbuffered,
            per_file,
            &mut ctx.output_errors,
            &mut ctx.write_timer,
            &mut ctx.projection_timer,
            ctx.collector,
            scan_timer,
        );
    } else if let Some(raw_writer) = ctx.writers.remove(name) {
        match build_format_writer(out_cfg, raw_writer, Arc::clone(&output_schema)) {
            Ok(mut csv_writer) => {
                ctx.collector.record(scan_timer.finish(1, 1));
                let mut write_failed = false;
                for (record, _rn) in &unbuffered {
                    let projected = {
                        let _guard = ctx.projection_timer.guard();
                        project_output_from_record(record, out_cfg, cxl_emit_names_opt)
                    };
                    let write_result = {
                        let _guard = ctx.write_timer.guard();
                        csv_writer.write_record(&projected)
                    };
                    if let Err(e) = write_result {
                        ctx.output_errors.push(PipelineError::from(e));
                        write_failed = true;
                        break;
                    }
                }
                if !write_failed {
                    let flush_result = {
                        let _guard = ctx.write_timer.guard();
                        csv_writer.flush()
                    };
                    if let Err(e) = flush_result {
                        ctx.output_errors.push(PipelineError::from(e));
                    }
                }
            }
            Err(e) => ctx.output_errors.push(e),
        }
    }

    Ok(())
}

/// Project every record in `rows` onto the column set in
/// `buffer_schema`, returning narrow `Record` instances on a fresh,
/// per-call `Arc<Schema>` shared by every emitted Record.
///
/// Mirrors the column-pruning pattern at
/// `pipeline::arena::project_records_into_minimal`. Invoked once at the
/// deferred-region producer (a relaxed-CK Aggregate) so the emit buffer
/// retains exactly the columns the deferred operators reach via
/// `Expr::support_into`. Source row numbers carry through unchanged.
///
/// `pub(crate)` so the commit-time `recompute_aggregates` phase can
/// Emit a buffered record stream to a fan-out output: one writer per
/// source-file `Arc<str>`, route each record to the writer keyed by
/// its `$source.file` Arc. Writers without any matched records still
/// flush an empty file (preserving header and any per-file framing).
///
/// All errors land in `output_errors` rather than short-circuiting so
/// sibling writers in the same Output still get their chance to flush
/// or report.
#[allow(clippy::too_many_arguments)]
fn emit_fan_out(
    name: &str,
    out_cfg: &crate::config::OutputConfig,
    cxl_emit_names_opt: Option<&[String]>,
    output_schema: &Arc<clinker_record::Schema>,
    unbuffered: &[(Record, u64)],
    per_file: HashMap<Arc<str>, Box<dyn Write + Send>>,
    output_errors: &mut Vec<PipelineError>,
    write_timer: &mut crate::executor::stage_metrics::CumulativeTimer,
    projection_timer: &mut crate::executor::stage_metrics::CumulativeTimer,
    collector: &mut crate::executor::stage_metrics::StageCollector,
    scan_timer: crate::executor::stage_metrics::StageTimer,
) {
    use std::collections::HashMap as Hm;

    // Build one format writer per pre-opened raw writer. Failed
    // construction for one file does NOT abort the whole output —
    // siblings still get their chance.
    let mut format_writers: Hm<Arc<str>, Box<dyn clinker_format::FormatWriter>> = Hm::new();
    for (file_arc, raw) in per_file {
        match build_format_writer(out_cfg, raw, Arc::clone(output_schema)) {
            Ok(fw) => {
                format_writers.insert(file_arc, fw);
            }
            Err(e) => output_errors.push(e),
        }
    }
    collector.record(scan_timer.finish(1, 1));

    for (record, rn) in unbuffered {
        let Some(file_path) = source_file_path_of(record) else {
            output_errors.push(PipelineError::Internal {
                op: "fan_out",
                node: name.to_string(),
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
            output_errors.push(PipelineError::Internal {
                op: "fan_out",
                node: name.to_string(),
                detail: format!(
                    "no fan-out writer registered for source file {:?}",
                    file_arc
                ),
            });
            continue;
        };
        let projected = {
            let _guard = projection_timer.guard();
            project_output_from_record(record, out_cfg, cxl_emit_names_opt)
        };
        let write_result = {
            let _guard = write_timer.guard();
            fw.write_record(&projected)
        };
        if let Err(e) = write_result {
            output_errors.push(PipelineError::from(e));
        }
    }

    // Flush every writer regardless of per-record errors so partial
    // outputs land on disk for inspection.
    for (_arc, mut fw) in format_writers {
        let flush_result = {
            let _guard = write_timer.guard();
            fw.flush()
        };
        if let Err(e) = flush_result {
            output_errors.push(PipelineError::from(e));
        }
    }
}
