//! `PlanNode::Sort` dispatch arm.
//!
//! Holds the planner-synthesized enforcer-sort body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: it materializes the
//! predecessor's records, sorts them by the enforced key carrying `row_num`
//! through the permutation, and emits the ordered run. The dispatcher's
//! `Sort` arm is a single delegating call into [`dispatch_sort`].

use std::cmp::Ordering;
use std::sync::Arc;

use clinker_record::Record;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, drain_node_buffer_slot, node_buffer_spill_allowed,
    tee_emit_to_region_input_buffers,
};
use crate::executor::{parse_memory_limit, stage_metrics};
use crate::pipeline::loser_tree::LoserTree;
use crate::pipeline::sort::compare_records_by_fields;
use crate::pipeline::spill::{SpillFile, SpillReader};
use clinker_plan::config::SortField;
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
    // Resolve the spill compression mode against this sort's schema width and
    // the run's batch size, so spilled runs match what `--explain` projects.
    let spill_compress = ctx
        .spill_compress
        .resolve_for_schema(schema.column_count(), ctx.batch_size as u64);
    let buf: SortBuffer<u64> = SortBuffer::new(
        sort_fields.clone(),
        mem_limit,
        Some(ctx.spill_root_path.to_path_buf()),
        spill_compress,
        schema,
    );

    let sort_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
    let sort_count = input_records.len() as u64;
    // Cloned out of `ctx` so the kernel closure owns it: every spilled run
    // (including the residue `finish` flushes) is charged against the disk
    // quota, aborting with E320 the moment the cumulative total crosses
    // `storage.spill.disk_cap_bytes`.
    let budget = std::sync::Arc::clone(&ctx.memory_budget);
    // CPU-bound: sort buffer push + per-batch comparison + spill I/O. The
    // kernel owns its input (`input_records`, `buf`, `budget`) and borrows
    // nothing from `ctx`, so it runs on the shared Rayon pool; output order is
    // fully determined by the sort itself, so pool scheduling cannot perturb it.
    let sorted = ctx
        .kernel_pool
        .install(move || drain_into_sort_buffer(buf, input_records, name, &budget))?;

    // Individually-sorted runs from `SortBuffer` are folded into one globally
    // ordered run through the same LoserTree k-way merge that aggregation spill
    // uses; a single in-memory run is already globally ordered and needs no
    // merge.
    let out: Vec<(Record, u64)> = match sorted {
        SortedOutput::InMemory(pairs) => pairs,
        SortedOutput::Spilled(files) => merge_sorted_runs(files, sort_fields)?,
    };
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

/// Drain `input` into `buf`, spilling sorted runs when the buffer exceeds its
/// budget and charging every spilled run — including the residue flushed by
/// `finish` — against the arbitrator's disk quota. Returns the buffer's sorted
/// output, or [`PipelineError::SpillCapExceeded`] (E320) when a spilled run
/// crosses the configured `storage.spill.disk_cap_bytes`.
///
/// CPU-bound: the per-run sort runs on the caller's Rayon pool.
fn drain_into_sort_buffer(
    mut buf: crate::pipeline::sort_buffer::SortBuffer<u64>,
    input: Vec<(Record, u64)>,
    node_name: &str,
    budget: &crate::pipeline::memory::MemoryArbitrator,
) -> Result<crate::pipeline::sort_buffer::SortedOutput<u64>, PipelineError> {
    for (record, row_num) in input {
        buf.push(record, row_num);
        if buf.should_spill() {
            let written = buf.sort_and_spill().map_err(|e| {
                PipelineError::Io(std::io::Error::other(format!(
                    "sort enforcer '{node_name}' spill failed: {e}"
                )))
            })?;
            charge_enforcer_spill(budget, node_name, written)?;
        }
    }
    let (sorted, residue) = buf.finish().map_err(|e| {
        PipelineError::Io(std::io::Error::other(format!(
            "sort enforcer '{node_name}' finish failed: {e}"
        )))
    })?;
    charge_enforcer_spill(budget, node_name, residue)?;
    Ok(sorted)
}

/// Charge `written` spilled bytes for `node_name` against the disk quota,
/// returning [`PipelineError::SpillCapExceeded`] (E320) once the running
/// cumulative total crosses `storage.spill.disk_cap_bytes`. A zero-byte write
/// (nothing flushed) charges nothing.
fn charge_enforcer_spill(
    budget: &crate::pipeline::memory::MemoryArbitrator,
    node_name: &str,
    written: u64,
) -> Result<(), PipelineError> {
    if written > 0 && budget.record_spill_bytes(node_name, written) {
        return Err(PipelineError::spill_cap_exceeded(
            node_name.to_string(),
            budget.max_spill_bytes(),
            written,
            budget.cumulative_spill_bytes(),
        ));
    }
    Ok(())
}

/// One entry in the enforcer-sort k-way merge: a spilled record and the
/// `row_num` payload carried through the sort permutation. `Ord` delegates to
/// [`compare_records_by_fields`] — the exact field comparator `SortBuffer`
/// formed each run with — so the merged order is byte-identical to a single
/// in-memory sort. The memcmp key on
/// [`crate::pipeline::loser_tree::MergeEntry`] is deliberately not used here:
/// it is not provably equal to the field comparator across Integer/Decimal or
/// float ordering, so merging on it could silently mis-order cross-type keys.
struct Run {
    sort_by: Arc<[SortField]>,
    record: Record,
    row_num: u64,
}

impl PartialEq for Run {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Run {}

impl PartialOrd for Run {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Run {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_records_by_fields(&self.record, &other.record, &self.sort_by)
    }
}

/// Pull the next `(record, row_num)` from one spilled run, wrapping it as a
/// [`Run`] merge entry, or `None` at end of stream.
fn next_run(
    reader: &mut SpillReader<u64>,
    sort_by: &Arc<[SortField]>,
) -> Result<Option<Run>, PipelineError> {
    match reader.next() {
        None => Ok(None),
        Some(Ok((record, row_num))) => Ok(Some(Run {
            sort_by: Arc::clone(sort_by),
            record,
            row_num,
        })),
        Some(Err(e)) => Err(PipelineError::Io(std::io::Error::other(format!(
            "sort enforcer spill decode failed: {e}"
        )))),
    }
}

/// K-way merge the individually-sorted spill runs into one globally-ordered
/// run via a [`LoserTree`], preserving each record's `row_num` payload.
///
/// `SortBuffer` spills runs in input order (run 0 earliest) with a stable
/// per-run sort, and the loser tree breaks ties by lower stream index, so
/// equal keys emit in input order — the merged run is byte-identical to the
/// single in-memory sort the buffer would have produced without spilling.
///
/// Memory model: holds one resident record per open run plus the fully
/// materialized output, matching the already-accepted single-run spill path.
fn merge_sorted_runs(
    files: Vec<SpillFile<u64>>,
    sort_by: &[SortField],
) -> Result<Vec<(Record, u64)>, PipelineError> {
    let sort_by: Arc<[SortField]> = Arc::from(sort_by.to_vec());
    // Each reader owns its own file handle; `files` is held for the whole
    // merge so the spill temp files outlive every read.
    let mut readers: Vec<SpillReader<u64>> = files
        .iter()
        .map(|f| f.reader())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            PipelineError::Io(std::io::Error::other(format!(
                "sort enforcer spill read failed: {e}"
            )))
        })?;

    // Seed the tree with one entry per run.
    let mut initial: Vec<Option<Run>> = Vec::with_capacity(readers.len());
    for reader in &mut readers {
        initial.push(next_run(reader, &sort_by)?);
    }
    let mut tree = LoserTree::new(initial);

    let mut out: Vec<(Record, u64)> = Vec::new();
    while tree.winner().is_some() {
        let idx = tree.winner_index();
        // The loser tree exposes only `&T`; clone the winning record and copy
        // its `row_num` out before refilling from the winning run. This
        // per-record clone mirrors the aggregation spill merge.
        let (record, row_num) = {
            let winner = tree.winner().expect("winner present under loop guard");
            (winner.record.clone(), winner.row_num)
        };
        out.push((record, row_num));
        let next = next_run(&mut readers[idx], &sort_by)?;
        tree.replace_winner(next);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
    use clinker_plan::config::SortOrder;
    use clinker_record::{Schema, Value};
    use rust_decimal::Decimal;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["k".into(), "id".into()]))
    }

    /// A record whose sort key `k` is a `Decimal` (mantissa/10) and whose
    /// `id` mirrors the carried `row_num` for readback.
    fn rec(schema: &Arc<Schema>, k_mantissa: i64, id: i64) -> Record {
        Record::new(
            schema.clone(),
            vec![
                Value::Decimal(Decimal::new(k_mantissa, 1)),
                Value::Integer(id),
            ],
        )
    }

    fn sort_by_k_asc() -> Vec<SortField> {
        vec![SortField {
            field: "k".into(),
            order: SortOrder::Asc,
            null_order: None,
        }]
    }

    /// Multiple individually-sorted spill runs — with a `Decimal` sort key,
    /// duplicate keys spanning different runs, and unsorted input within each
    /// run — must k-way merge into one globally-ordered run that preserves
    /// every record's `row_num` payload and emits equal keys in input order
    /// (stable). This is the correctness the enforcer sort lost when it
    /// rejected multi-run spills instead of merging them.
    #[test]
    fn merge_sorted_runs_globally_orders_stably_and_preserves_row_num() {
        let schema = schema();
        let sort_by = sort_by_k_asc();
        // budget=1 is the spill-everything threshold; runs are flushed
        // explicitly below so each carries several unsorted records.
        let mut buf: SortBuffer<u64> =
            SortBuffer::new(sort_by.clone(), 1, None, true, schema.clone());

        // Run A: keys 3.0, 1.0, 2.0 (row_num 0,1,2).
        buf.push(rec(&schema, 30, 0), 0);
        buf.push(rec(&schema, 10, 1), 1);
        buf.push(rec(&schema, 20, 2), 2);
        buf.sort_and_spill().unwrap();
        // Run B: keys 2.0, 1.0 (row_num 3,4) — duplicates 2.0 and 1.0 from A.
        buf.push(rec(&schema, 20, 3), 3);
        buf.push(rec(&schema, 10, 4), 4);
        buf.sort_and_spill().unwrap();
        // Run C: keys 2.0, 4.0 (row_num 5,6) — duplicate 2.0 from A and B.
        buf.push(rec(&schema, 20, 5), 5);
        buf.push(rec(&schema, 40, 6), 6);
        buf.sort_and_spill().unwrap();

        let files = match buf.finish().unwrap().0 {
            SortedOutput::Spilled(files) => {
                assert_eq!(files.len(), 3, "three explicit flushes → three runs");
                files
            }
            SortedOutput::InMemory(_) => panic!("expected Spilled after three flushes"),
        };

        let merged = merge_sorted_runs(files, &sort_by).unwrap();

        // Every input record survives the merge exactly once.
        assert_eq!(merged.len(), 7);

        // Globally ordered on the Decimal key, ascending.
        let keys: Vec<Decimal> = merged
            .iter()
            .map(|(r, _)| match r.get("k") {
                Some(Value::Decimal(d)) => *d,
                other => panic!("expected Decimal key, got {other:?}"),
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                Decimal::new(10, 1),
                Decimal::new(10, 1),
                Decimal::new(20, 1),
                Decimal::new(20, 1),
                Decimal::new(20, 1),
                Decimal::new(30, 1),
                Decimal::new(40, 1),
            ],
            "runs must merge into ascending Decimal order"
        );

        // Stable: for equal keys, records emit in input order (run 0 earliest,
        // then input order within a run). The carried row_num payload proves it
        // and that no record picked up another's payload through the merge.
        let row_nums: Vec<u64> = merged.iter().map(|(_, rn)| *rn).collect();
        assert_eq!(
            row_nums,
            vec![1, 4, 2, 3, 5, 0, 6],
            "equal keys must emit in input order and carry their own row_num"
        );

        // The payload identity matches the record's `id` column for every row,
        // confirming the pairing survived both the spill envelope and the merge.
        for (record, row_num) in &merged {
            assert_eq!(record.get("id"), Some(&Value::Integer(*row_num as i64)));
        }
    }

    /// The enforcer sort must abort the spill instead of writing past
    /// `storage.spill.disk_cap_bytes`: each spilled run is charged against the
    /// arbitrator's disk quota, and the first run that crosses a one-byte cap
    /// surfaces E320. Before the fix the enforcer sort never charged the
    /// arbitrator, so it could spill unbounded regardless of the configured cap.
    #[test]
    fn enforcer_sort_spill_past_disk_cap_fails_with_spill_cap_exceeded() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};

        let schema = schema();
        let budget = MemoryArbitrator::with_policy(64 * 1024, 0.80, 0.70, Box::new(NoOpPolicy));
        budget.set_max_spill_bytes(1);
        let spill_root = tempfile::tempdir().unwrap();
        // threshold=1 → every push spills, so the first run already crosses the
        // one-byte disk cap.
        let buf: SortBuffer<u64> = SortBuffer::new(
            sort_by_k_asc(),
            1,
            Some(spill_root.path().to_path_buf()),
            true,
            schema.clone(),
        );
        let input: Vec<(Record, u64)> = (0..6)
            .map(|i| (rec(&schema, (i as i64 + 1) * 10, i as i64), i))
            .collect();

        // `SortedOutput` is not `Debug`, so match rather than `expect_err`.
        let err = match drain_into_sort_buffer(buf, input, "enforce", &budget) {
            Ok(_) => panic!("a one-byte disk cap must abort the enforcer sort spill"),
            Err(e) => e,
        };
        match err {
            PipelineError::SpillCapExceeded {
                node,
                cap,
                attempted,
                current,
            } => {
                assert_eq!(node, "enforce");
                assert_eq!(cap, 1, "reported cap equals the configured quota");
                assert!(attempted > 0, "the overflowing run reports its size");
                assert!(
                    current > cap,
                    "cumulative spilled ({current}) must exceed the cap ({cap})"
                );
            }
            other => panic!("disk-cap overflow must surface SpillCapExceeded; got {other:?}"),
        }
        assert!(
            budget.cumulative_spill_bytes() > 1,
            "cumulative_spill_bytes must reflect the overflowing write"
        );
    }
}
