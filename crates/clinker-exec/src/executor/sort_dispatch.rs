//! `PlanNode::Sort` dispatch arm.
//!
//! Holds the planner-synthesized enforcer-sort body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`]: it materializes the
//! predecessor's records, sorts them by the enforced key while carrying each
//! record's [`SourceRowId`](crate::executor::stream_event::SourceRowId) through
//! the permutation, and emits the ordered run. The dispatcher's
//! `Sort` arm is a single delegating call into [`dispatch_sort`].

use clinker_record::Record;
use petgraph::graph::NodeIndex;

use crate::executor::dispatch::{
    ExecutorContext, admit_node_buffer, node_buffer_spill_allowed,
    require_single_input_node_buffer_slot, tee_emit_to_region_input_buffers,
};
use crate::executor::{parse_memory_limit, stage_metrics};
use crate::pipeline::spill_merge::merge_sorted_runs;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode, single_predecessor};

/// Context carrier kept lazy until the node-kind guard has succeeded. Normal
/// dispatch passes the live executor context directly; the feature-gated
/// mismatch matrix uses the inert carrier to prove rejection precedes input
/// drainage, sorting, spill, and buffer access.
pub(crate) enum SortDispatchContext<'borrow, 'plan> {
    Live(&'borrow mut ExecutorContext<'plan>),
    #[cfg(feature = "test-utils")]
    Inert,
}

impl<'borrow, 'plan> From<&'borrow mut ExecutorContext<'plan>>
    for SortDispatchContext<'borrow, 'plan>
{
    fn from(ctx: &'borrow mut ExecutorContext<'plan>) -> Self {
        Self::Live(ctx)
    }
}

#[cfg(feature = "test-utils")]
impl crate::executor::dispatch::DispatchFaultGuard {
    /// Execute the real sort boundary with an inert context so tests can prove
    /// a wrong node returns before input or spill state is touched.
    #[doc(hidden)]
    pub fn dispatch_sort_mismatch_for_testing(
        current_dag: &ExecutionPlanDag,
        node_idx: NodeIndex,
        node: &PlanNode,
    ) -> Result<(), PipelineError> {
        dispatch_sort(SortDispatchContext::Inert, current_dag, node_idx, node)
    }
}

/// Lazily drained result of a stable authored-key sort. Resident populations
/// iterate their already-sorted vector; spilled populations retain the shared
/// bounded-fan-in merger so records reach a terminal writer without
/// rematerializing the merged run.
pub(super) enum AuthoredSortStream {
    InMemory(std::vec::IntoIter<(Record, crate::executor::stream_event::SourceRowId)>),
    Spilled {
        merger: crate::pipeline::spill_merge::SortedRunMerger<
            crate::executor::stream_event::SourceRowId,
        >,
        _charge: crate::pipeline::spill_merge::SpillChargeGuard,
    },
}

impl Iterator for AuthoredSortStream {
    type Item = Result<(Record, crate::executor::stream_event::SourceRowId), PipelineError>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = match self {
            Self::InMemory(records) => records.next().map(Ok),
            Self::Spilled { merger, .. } => merger.next(),
        };
        if matches!(result, None | Some(Err(_))) && matches!(self, Self::Spilled { .. }) {
            // Dropping both the merger and its charge guard at exhaustion (or
            // decode failure) unlinks the final files and releases their exact
            // live-byte charge at the same boundary. An early caller drop gets
            // the same behavior from the enum fields' Drop implementations.
            *self = Self::InMemory(Vec::new().into_iter());
        }
        result
    }
}

/// Execute the `Sort` arm for `node_idx`: buffer the predecessor's records,
/// sort by the enforced `sort_fields` key (carrying each record's
/// [`SourceRowId`](crate::executor::stream_event::SourceRowId) through the
/// permutation), and emit the ordered run. Blocking: the full
/// input run materializes before the first sorted record leaves.
pub(crate) fn dispatch_sort<'borrow, 'plan>(
    ctx: impl Into<SortDispatchContext<'borrow, 'plan>>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    node: &PlanNode,
) -> Result<(), PipelineError>
where
    'plan: 'borrow,
{
    let PlanNode::Sort {
        ref name,
        ref sort_fields,
        ..
    } = *node
    else {
        return Err(crate::executor::invariant::dispatch_mismatch(
            "dispatch_sort",
            "sort",
            node.kind_name(),
            node.name(),
        ));
    };
    #[cfg(feature = "test-utils")]
    let SortDispatchContext::Live(ctx) = ctx.into() else {
        panic!("sort dispatcher accessed inert context after accepting a sort node")
    };
    #[cfg(not(feature = "test-utils"))]
    let SortDispatchContext::Live(ctx) = ctx.into();
    // Enforcer-sort dispatch. Carries `row_num` through
    // the sort permutation as the `SortBuffer<SourceRowId>`
    // payload — the Record itself carries every field
    // value, emitted content, and metadata, so no
    // parallel bookkeeping map rides alongside.
    let pred = single_predecessor(current_dag, node_idx, "sort", name)?;
    let producer_port = current_dag
        .graph
        .find_edge(pred, node_idx)
        .and_then(|edge| current_dag.graph.edge_weight(edge))
        .and_then(|edge| edge.producer_port.as_deref());
    let input_buffer = require_single_input_node_buffer_slot(
        ctx,
        node_idx,
        pred,
        name,
        current_dag.graph[pred].name(),
        producer_port,
    )?;
    let (input_buffer, _input_reservation) =
        input_buffer.into_materialized_parts(&ctx.memory_budget, name)?;
    let (input_records, input_puncts): (
        Vec<(Record, crate::executor::stream_event::SourceRowId)>,
        Vec<crate::executor::stream_event::Punctuation>,
    ) = input_buffer.drain_split()?;

    if input_records.is_empty() {
        tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &[])?;
        // An empty input still registers a (zero-byte) consumer
        // via `admit_node_buffer` for symmetry with the non-empty
        // path, so the arbitrator's pull-mode registry treats
        // every Sort insert uniformly.
        admit_node_buffer(
            ctx,
            current_dag,
            name,
            node_idx,
            Vec::new(),
            input_puncts,
            node_buffer_spill_allowed(current_dag, node_idx),
        )?;
        return Ok(());
    }

    let out = sort_records_by_authored_fields(ctx, name, sort_fields, input_records)?;
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &out)?;
    admit_node_buffer(
        ctx,
        current_dag,
        name,
        node_idx,
        out,
        input_puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;

    Ok(())
}

/// Whether a record is excluded by an authored `null_order: drop` field.
///
/// A record qualifies when *any* dropping field's key is absent from the
/// record or present and null — an absent column and an explicit null are
/// the same missing key as far as ordering is concerned.
///
/// The buffered and streaming sorters share this rather than each spelling
/// the test out: the two paths must exclude the same records and report the
/// same count, and the cheapest way to guarantee that is to leave them no
/// way to disagree.
fn dropped_for_null_key(record: &Record, sort_fields: &[clinker_plan::config::SortField]) -> bool {
    sort_fields.iter().any(|field| {
        field.null_order == Some(clinker_plan::config::NullOrder::Drop)
            && record
                .get(&field.field)
                .is_none_or(clinker_record::Value::is_null)
    })
}

/// Apply the shared stable authored-key sort to a materialized record stream.
///
/// `SourceRowId` remains payload throughout resident sorting and spill merge;
/// it is never appended to the comparison key. This is shared by synthesized
/// Sort nodes and terminal Output declarations so both paths have identical
/// null, direction, stable-tie, memory, and spill behavior.
pub(super) fn sort_records_by_authored_fields(
    ctx: &mut ExecutorContext<'_>,
    node_name: &str,
    sort_fields: &[clinker_plan::config::SortField],
    mut input_records: Vec<(Record, crate::executor::stream_event::SourceRowId)>,
) -> Result<Vec<(Record, crate::executor::stream_event::SourceRowId)>, PipelineError> {
    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};

    // Opened before the null-key filter so the stage's elapsed time covers
    // the work that removes records, and `records_in` is the population the
    // stage was handed rather than what survived it.
    let sort_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
    let received = input_records.len() as u64;
    input_records.retain(|(record, _)| !dropped_for_null_key(record, sort_fields));
    ctx.counters
        .increment_null_dropped(received - input_records.len() as u64);
    if input_records.is_empty() {
        // Report even here. Returning without recording would hide the drop
        // on exactly the run that dropped everything.
        ctx.collector.record(sort_timer.finish(received, 0));
        return Ok(input_records);
    }
    let schema = input_records[0].0.schema().clone();
    let mem_limit = parse_memory_limit(ctx.config);
    let spill_compress = ctx
        .spill_compress
        .resolve_for_schema(schema.column_count(), ctx.batch_size as u64);
    let buf: SortBuffer<crate::executor::stream_event::SourceRowId> = SortBuffer::new(
        sort_fields.to_vec(),
        mem_limit,
        Some(ctx.spill_root_path.to_path_buf()),
        spill_compress,
        schema,
    );

    let sort_count = input_records.len() as u64;
    let charge = crate::pipeline::spill_merge::SpillChargeGuard::new(
        std::sync::Arc::clone(&ctx.memory_budget),
        node_name,
    );
    let sorted = ctx
        .kernel_pool
        .install(|| drain_into_sort_buffer(buf, input_records, node_name, &charge))?;
    let out = match sorted {
        SortedOutput::InMemory(pairs) => pairs,
        SortedOutput::Spilled(files) => merge_sorted_runs(
            files,
            sort_fields,
            "authored sort",
            crate::pipeline::spill_merge::MergeBudget {
                budget: &ctx.memory_budget,
                node: node_name,
                compress: spill_compress,
                charge_owner: Some(&charge),
            },
        )?,
    };
    ctx.collector
        .record(sort_timer.finish(received, sort_count));
    Ok(out)
}

/// Apply the shared stable authored-key sorter to a record iterator and leave
/// a spilled result lazy. This is the terminal-writer variant of
/// [`sort_records_by_authored_fields`]: it performs the same null-drop filter,
/// stable run formation, spill charging, and bounded fan-in merge, while
/// allowing document/envelope commit paths to write one merged record at a
/// time instead of collecting the complete result in a second `Vec`.
pub(super) fn sort_record_stream_by_authored_fields<I>(
    ctx: &mut ExecutorContext<'_>,
    node_name: &str,
    sort_fields: &[clinker_plan::config::SortField],
    input: I,
) -> Result<AuthoredSortStream, PipelineError>
where
    I: IntoIterator<
        Item = Result<(Record, crate::executor::stream_event::SourceRowId), PipelineError>,
    >,
{
    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
    use crate::pipeline::spill_merge::{MergeBudget, SortedRunMerger, SpillChargeGuard};

    let mem_limit = parse_memory_limit(ctx.config);
    let mut buffer: Option<SortBuffer<crate::executor::stream_event::SourceRowId>> = None;
    let mut spill_compress = false;
    let mut sort_count = 0u64;
    let mut dropped = 0u64;
    let sort_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
    let charge = SpillChargeGuard::new(std::sync::Arc::clone(&ctx.memory_budget), node_name);

    for item in input {
        let (record, source_row) = item?;
        if dropped_for_null_key(&record, sort_fields) {
            dropped = dropped.saturating_add(1);
            continue;
        }
        let buf = buffer.get_or_insert_with(|| {
            spill_compress = ctx
                .spill_compress
                .resolve_for_schema(record.schema().column_count(), ctx.batch_size as u64);
            SortBuffer::new(
                sort_fields.to_vec(),
                mem_limit,
                Some(ctx.spill_root_path.to_path_buf()),
                spill_compress,
                record.schema().clone(),
            )
        });
        buf.push(record, source_row);
        sort_count = sort_count.saturating_add(1);
        if buf.should_spill() {
            let written = buf.sort_and_spill().map_err(|error| {
                PipelineError::Io(std::io::Error::other(format!(
                    "sort enforcer '{node_name}' spill failed: {error}"
                )))
            })?;
            charge_enforcer_spill(&charge, node_name, written)?;
        }
    }

    ctx.counters.increment_null_dropped(dropped);
    let received = sort_count.saturating_add(dropped);

    let Some(buf) = buffer else {
        // No record survived to open a buffer. Report anyway: a stream that
        // dropped every record it saw is the case an operator most needs to
        // see, and it is the one an early return would hide.
        ctx.collector.record(sort_timer.finish(received, 0));
        return Ok(AuthoredSortStream::InMemory(Vec::new().into_iter()));
    };
    let (sorted, residue) = buf.finish().map_err(|error| {
        PipelineError::Io(std::io::Error::other(format!(
            "sort enforcer '{node_name}' finish failed: {error}"
        )))
    })?;
    charge_enforcer_spill(&charge, node_name, residue)?;
    ctx.collector
        .record(sort_timer.finish(received, sort_count));

    match sorted {
        SortedOutput::InMemory(records) => Ok(AuthoredSortStream::InMemory(records.into_iter())),
        SortedOutput::Spilled(files) => {
            let merger = SortedRunMerger::new(
                files,
                sort_fields,
                "authored sort",
                MergeBudget {
                    budget: &ctx.memory_budget,
                    node: node_name,
                    compress: spill_compress,
                    charge_owner: Some(&charge),
                },
            )?;
            Ok(AuthoredSortStream::Spilled {
                merger,
                _charge: charge,
            })
        }
    }
}

/// Drain `input` into `buf`, spilling sorted runs when the buffer exceeds its
/// budget and charging every spilled run — including the residue flushed by
/// `finish` — against the arbitrator's disk quota. Returns the buffer's sorted
/// output, or [`PipelineError::SpillCapExceeded`] (E320) when a spilled run
/// crosses the configured `storage.spill.disk_cap_bytes`.
///
/// CPU-bound: the per-run sort runs on the caller's Rayon pool.
fn drain_into_sort_buffer(
    mut buf: crate::pipeline::sort_buffer::SortBuffer<crate::executor::stream_event::SourceRowId>,
    input: Vec<(Record, crate::executor::stream_event::SourceRowId)>,
    node_name: &str,
    charge: &crate::pipeline::spill_merge::SpillChargeGuard,
) -> Result<
    crate::pipeline::sort_buffer::SortedOutput<crate::executor::stream_event::SourceRowId>,
    PipelineError,
> {
    for (record, source_row) in input {
        buf.push(record, source_row);
        if buf.should_spill() {
            let written = buf.sort_and_spill().map_err(|e| {
                PipelineError::Io(std::io::Error::other(format!(
                    "sort enforcer '{node_name}' spill failed: {e}"
                )))
            })?;
            charge_enforcer_spill(charge, node_name, written)?;
        }
    }
    let (sorted, residue) = buf.finish().map_err(|e| {
        PipelineError::Io(std::io::Error::other(format!(
            "sort enforcer '{node_name}' finish failed: {e}"
        )))
    })?;
    charge_enforcer_spill(charge, node_name, residue)?;
    Ok(sorted)
}

/// Charge `written` spilled bytes for `node_name` against the disk quota,
/// returning [`PipelineError::SpillCapExceeded`] (E320) once the running
/// cumulative total crosses `storage.spill.disk_cap_bytes`. A zero-byte write
/// (nothing flushed) charges nothing.
fn charge_enforcer_spill(
    charge: &crate::pipeline::spill_merge::SpillChargeGuard,
    node_name: &str,
    written: u64,
) -> Result<(), PipelineError> {
    if written > 0 && charge.record(written) {
        return Err(PipelineError::spill_cap_exceeded(
            node_name.to_string(),
            charge.max_spill_bytes(),
            written,
            charge.current_spill_bytes(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::executor::dispatch::{NodeBufferKey, single_input_node_buffer_key};
    use crate::executor::node_buffer::NodeBuffer;
    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
    use clinker_plan::config::{SortField, SortOrder};
    use clinker_plan::plan::{EntityRef, PlanNodeId};
    use clinker_record::{Schema, Value};
    use rust_decimal::Decimal;

    #[test]
    fn sort_prefers_its_successor_local_slot() {
        let producer_idx = NodeIndex::new(1);
        let sort_idx = NodeIndex::new(2);
        let mut buffers = HashMap::new();
        buffers.insert(
            NodeBufferKey::with_port(producer_idx, Some("selected")),
            NodeBuffer::Memory(Vec::new()),
        );
        buffers.insert(
            NodeBufferKey::from(sort_idx),
            NodeBuffer::Memory(Vec::new()),
        );

        let selected =
            single_input_node_buffer_key(&buffers, sort_idx, producer_idx, Some("selected"));

        assert_eq!(selected, NodeBufferKey::from(sort_idx));
    }

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["k".into(), "id".into()]))
    }

    /// A record whose sort key `k` is a `Decimal` (mantissa/10) and whose
    /// `id` mirrors the carried source ordinal for readback.
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

    /// The enforcer sort must abort the spill instead of writing past
    /// `storage.spill.disk_cap_bytes`: each spilled run is charged against the
    /// arbitrator's disk quota, and the first run that crosses a one-byte cap
    /// surfaces E320. Before the fix the enforcer sort never charged the
    /// arbitrator, so it could spill unbounded regardless of the configured cap.
    #[test]
    fn enforcer_sort_spill_past_disk_cap_fails_with_spill_cap_exceeded() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};

        let schema = schema();
        let budget = Arc::new(MemoryArbitrator::with_policy(
            64 * 1024,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        budget.set_max_spill_bytes(1);
        let charge =
            crate::pipeline::spill_merge::SpillChargeGuard::new(Arc::clone(&budget), "enforce");
        let spill_root = tempfile::tempdir().unwrap();
        // threshold=1 → every push spills, so the first run already crosses the
        // one-byte disk cap.
        let buf: SortBuffer<crate::executor::stream_event::SourceRowId> = SortBuffer::new(
            sort_by_k_asc(),
            1,
            Some(spill_root.path().to_path_buf()),
            true,
            schema.clone(),
        );
        let input: Vec<(Record, crate::executor::stream_event::SourceRowId)> = (0..6)
            .map(|i| {
                (
                    rec(&schema, (i as i64 + 1) * 10, i as i64),
                    crate::executor::stream_event::SourceRowId::new(PlanNodeId::new(0), i as u64),
                )
            })
            .collect();

        // `SortedOutput` is not `Debug`, so match rather than `expect_err`.
        let err = match drain_into_sort_buffer(buf, input, "enforce", &charge) {
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
        drop(charge);
        assert_eq!(
            budget.cumulative_spill_bytes(),
            0,
            "the failed sort drops its files and must release their charges"
        );
    }

    #[test]
    fn sequential_spilled_sorts_release_live_bytes_between_drains() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use crate::pipeline::spill_merge::{MergeBudget, SpillChargeGuard, merge_sorted_runs};

        fn run_sort(
            budget: &Arc<MemoryArbitrator>,
            schema: &Arc<Schema>,
        ) -> Result<u64, PipelineError> {
            let spill_root = tempfile::tempdir().unwrap();
            let buf = SortBuffer::new(
                sort_by_k_asc(),
                1,
                Some(spill_root.path().to_path_buf()),
                true,
                Arc::clone(schema),
            );
            let input = (0..6)
                .map(|i| {
                    (
                        rec(schema, (6 - i) * 10, i),
                        crate::executor::stream_event::SourceRowId::new(
                            PlanNodeId::new(0),
                            i as u64,
                        ),
                    )
                })
                .collect();
            let charge = SpillChargeGuard::new(Arc::clone(budget), "sequential-sort");
            let sorted = drain_into_sort_buffer(buf, input, "sequential-sort", &charge)?;
            let charged = charge.bytes();
            let SortedOutput::Spilled(files) = sorted else {
                panic!("one-byte threshold must spill")
            };
            let rows = merge_sorted_runs(
                files,
                &sort_by_k_asc(),
                "authored sort",
                MergeBudget {
                    budget,
                    node: "sequential-sort",
                    compress: true,
                    charge_owner: Some(&charge),
                },
            )?;
            assert_eq!(rows.len(), 6);
            drop(charge);
            Ok(charged)
        }

        let schema = schema();
        let budget = Arc::new(MemoryArbitrator::with_policy(
            64 * 1024,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        let first_bytes = run_sort(&budget, &schema).unwrap();
        assert!(first_bytes > 0);
        assert_eq!(budget.cumulative_spill_bytes(), 0);

        // Exactly one sort population fits. A stale first charge would make
        // the second spill exceed this cap.
        budget.set_max_spill_bytes(first_bytes);
        run_sort(&budget, &schema).expect("the second sort must not see stale first-sort bytes");
        assert_eq!(budget.cumulative_spill_bytes(), 0);
        assert_eq!(
            budget
                .per_stage_spill_bytes()
                .get("sequential-sort")
                .copied()
                .unwrap_or(0),
            0
        );
    }

    #[test]
    fn lazy_sort_releases_on_completion_drop_and_constructor_error() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};
        use crate::pipeline::spill::SpillFile;
        use crate::pipeline::spill_merge::{MergeBudget, SortedRunMerger, SpillChargeGuard};

        fn runs(
            schema: &Arc<Schema>,
            dir: &std::path::Path,
        ) -> Vec<SpillFile<crate::executor::stream_event::SourceRowId>> {
            let mut buffer = SortBuffer::new(
                sort_by_k_asc(),
                1,
                Some(dir.to_path_buf()),
                true,
                Arc::clone(schema),
            );
            for i in 0..3 {
                buffer.push(
                    rec(schema, (3 - i) * 10, i),
                    crate::executor::stream_event::SourceRowId::new(PlanNodeId::new(0), i as u64),
                );
                buffer.sort_and_spill().unwrap();
            }
            match buffer.finish().unwrap().0 {
                SortedOutput::Spilled(files) => files,
                SortedOutput::InMemory(_) => panic!("expected spilled runs"),
            }
        }

        fn charged_stream(
            budget: &Arc<MemoryArbitrator>,
            files: Vec<SpillFile<crate::executor::stream_event::SourceRowId>>,
        ) -> AuthoredSortStream {
            let charge = SpillChargeGuard::new(Arc::clone(budget), "lazy-sort");
            let bytes: u64 = files.iter().map(SpillFile::bytes).sum();
            assert!(!charge.record(bytes));
            let merger = SortedRunMerger::new(
                files,
                &sort_by_k_asc(),
                "authored sort",
                MergeBudget {
                    budget,
                    node: "lazy-sort",
                    compress: true,
                    charge_owner: Some(&charge),
                },
            )
            .unwrap();
            AuthoredSortStream::Spilled {
                merger,
                _charge: charge,
            }
        }

        let budget = Arc::new(MemoryArbitrator::with_policy(
            64 * 1024,
            0.80,
            0.70,
            Box::new(NoOpPolicy),
        ));
        let schema = schema();

        let dir = tempfile::tempdir().unwrap();
        let stream = charged_stream(&budget, runs(&schema, dir.path()));
        assert!(budget.cumulative_spill_bytes() > 0);
        assert_eq!(stream.collect::<Result<Vec<_>, _>>().unwrap().len(), 3);
        assert_eq!(budget.cumulative_spill_bytes(), 0);

        let dir = tempfile::tempdir().unwrap();
        let stream = charged_stream(&budget, runs(&schema, dir.path()));
        assert!(budget.cumulative_spill_bytes() > 0);
        drop(stream);
        assert_eq!(budget.cumulative_spill_bytes(), 0);

        let dir = tempfile::tempdir().unwrap();
        let files = runs(&schema, dir.path());
        let bytes: u64 = files.iter().map(SpillFile::bytes).sum();
        std::fs::write(files[0].path(), b"invalid spill header").unwrap();
        let charge = SpillChargeGuard::new(Arc::clone(&budget), "lazy-sort");
        assert!(!charge.record(bytes));
        let result = SortedRunMerger::new(
            files,
            &sort_by_k_asc(),
            "authored sort",
            MergeBudget {
                budget: &budget,
                node: "lazy-sort",
                compress: true,
                charge_owner: Some(&charge),
            },
        );
        assert!(
            result.is_err(),
            "corrupt spill must fail during construction"
        );
        drop(charge);
        assert_eq!(budget.cumulative_spill_bytes(), 0);
    }
}
