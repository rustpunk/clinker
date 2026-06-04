//! `PlanNode::Sort` dispatch arm.
//!
//! Holds the planner-synthesized enforcer-sort body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: it materializes the
//! predecessor's records, sorts them by the enforced key carrying `row_num`
//! through the permutation, and emits the ordered run. The dispatcher's
//! `Sort` arm is a single delegating call into [`dispatch_sort`].

use clinker_record::Record;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, drain_node_buffer_slot, node_buffer_spill_allowed,
    tee_emit_to_region_input_buffers,
};
use crate::executor::{parse_memory_limit, stage_metrics};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};

/// Execute the `Sort` arm for `node_idx`: buffer the predecessor's records,
/// sort by the enforced `sort_fields` key (carrying each record's `row_num`
/// through the permutation), and emit the ordered run. Blocking: the full
/// input run materializes before the first sorted record leaves.
pub(crate) fn dispatch_sort(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError> {
    let PlanNode::Sort {
        ref name,
        ref sort_fields,
        ..
    } = *node
    else {
        unreachable!("dispatch_sort called with non-Sort node");
    };
    // Enforcer-sort dispatch. Carries `row_num` through
    // the sort permutation as the `SortBuffer<u64>`
    // payload — the Record itself carries every field
    // value, emitted content, and metadata, so no
    // parallel bookkeeping map rides alongside.
    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};

    let predecessors: Vec<NodeIndex> = current_dag
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    let (input_records, input_puncts): (
        Vec<(Record, u64)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = match predecessors
        .iter()
        .find_map(|p| drain_node_buffer_slot(ctx, *p))
    {
        Some(nb) => nb.drain_split()?,
        None => (Vec::new(), Vec::new()),
    };

    if input_records.is_empty() {
        tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &[])?;
        // An empty input still registers a (zero-byte) consumer
        // via `admit_node_buffer` for symmetry with the non-empty
        // path, so the arbitrator's pull-mode registry treats
        // every Sort insert uniformly.
        let nb = admit_node_buffer(
            ctx,
            name,
            node_idx,
            Vec::new(),
            input_puncts,
            node_buffer_spill_allowed(current_dag, node_idx),
        )?;
        ctx.node_buffers.insert(node_idx, nb);
        return Ok(());
    }

    let schema = input_records[0].0.schema().clone();
    let mem_limit = parse_memory_limit(ctx.config);
    let mut buf: SortBuffer<u64> = SortBuffer::new(
        sort_fields.clone(),
        mem_limit,
        Some(ctx.spill_root_path.to_path_buf()),
        schema,
    );

    let sort_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
    let sort_count = input_records.len() as u64;
    // CPU-bound: sort buffer push + per-batch comparison + spill
    // I/O. The kernel owns its input (`input_records`, `buf`) and
    // borrows nothing from `ctx`, so it runs on the shared Rayon
    // pool; output order is fully determined by the sort itself,
    // so pool scheduling cannot perturb it.
    let sorted = ctx.kernel_pool.install(|| {
        for (record, row_num) in input_records {
            buf.push(record, row_num);
            if buf.should_spill() {
                buf.sort_and_spill().map_err(|e| {
                    PipelineError::Io(std::io::Error::other(format!(
                        "sort enforcer '{name}' spill failed: {e}"
                    )))
                })?;
            }
        }
        buf.finish().map_err(|e| {
            PipelineError::Io(std::io::Error::other(format!(
                "sort enforcer '{name}' finish failed: {e}"
            )))
        })
    })?;

    let mut out: Vec<(Record, u64)> = Vec::with_capacity(sort_count as usize);
    match sorted {
        SortedOutput::InMemory(pairs) => {
            for (record, row_num) in pairs {
                out.push((record, row_num));
            }
        }
        SortedOutput::Spilled(files) => {
            // Spill files are individually sorted but not
            // globally merged. For correctness when multiple
            // spill files exist, a k-way merge is required.
            // Single-file fast path is exact.
            if files.len() > 1 {
                return Err(PipelineError::Io(std::io::Error::other(format!(
                    "sort enforcer '{name}' produced {} spill files; \
                             k-way merge for enforcer sort is not yet implemented \
                             (memory.limit too small for input)",
                    files.len()
                ))));
            }
            for file in files {
                let reader = file.reader().map_err(|e| {
                    PipelineError::Io(std::io::Error::other(format!(
                        "sort enforcer '{name}' spill read failed: {e}"
                    )))
                })?;
                for entry in reader {
                    let (record, row_num) = entry.map_err(|e| {
                        PipelineError::Io(std::io::Error::other(format!(
                            "sort enforcer '{name}' spill decode failed: {e}"
                        )))
                    })?;
                    out.push((record, row_num));
                }
            }
        }
    }
    ctx.collector
        .record(sort_timer.finish(sort_count, sort_count));
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &out)?;
    let nb = admit_node_buffer(
        ctx,
        name,
        node_idx,
        out,
        input_puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx, nb);

    Ok(())
}
