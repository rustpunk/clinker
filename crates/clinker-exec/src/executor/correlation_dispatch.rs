//! `PlanNode::CorrelationCommit` dispatch arm and correlation-buffer commit.
//!
//! Holds the terminal correlation-commit body lifted out of
//! [`crate::executor::dispatch::dispatch_plan_node`] together with the
//! per-group commit machinery it drives: [`commit_correlation_buffers`]
//! walks every correlation buffer and emits, per group, either a clean
//! flush-to-writer or DLQ entries (trigger + collateral) for dirty /
//! overflowed groups. The buffer entry types (`CorrelationGroupBuffer`
//! and friends) stay in [`crate::executor::dispatch`] because they are part
//! of `ExecutorContext`'s surface and the shared error-routing path. The
//! dispatcher's `CorrelationCommit` arm is a single delegating call into
//! [`dispatch_correlation_commit`].

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clinker_record::GroupByKey;

use crate::executor::dispatch::{
    CorrelationGroupBuffer, CorrelationRecordSlot, ExecutorContext, MERGED_SOURCE_NAME, push_dlq,
    push_write_error, source_name_arc_of,
};
use crate::executor::structured_output_guard::StructuredOutputDocumentGuard;
use crate::executor::{DlqEntry, build_format_writer};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::ExecutionPlanDag;

/// Execute the `CorrelationCommit` arm: drive the relaxed-CK cascading
/// retraction orchestrator (which, for strict pipelines, runs a single
/// forward pass) and flush every correlation group at end-of-DAG via
/// [`commit_correlation_buffers`].
pub(crate) fn dispatch_correlation_commit(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
) -> Result<(), PipelineError> {
    crate::executor::commit::orchestrate(ctx, current_dag)?;
    Ok(())
}

/// Walk every correlation buffer and emit the per-group commit:
/// flush-to-writer for clean groups, DLQ-with-trigger for dirty groups,
/// overflow-aware DLQ for groups that tripped `max_group_buffer`.
///
/// Two-phase: phase 1 walks every group once, accumulating clean
/// records into a per-output queue and emitting DLQ entries for
/// dirty/overflow groups. Phase 2 opens each writer ONCE, writes the
/// accumulated queue, and flushes — one writer take per Output, not
/// one per group. The phasing is necessary because every group flush
/// would otherwise contend for `ctx.writers.remove(name)`, leaving
/// subsequent groups silently dropped.
///
/// Single source of truth for both strict and relaxed pipelines. The
/// relaxed-CK orchestrator runs its cascading-retraction loop, then
/// delegates here once the post-recompute aggregate output rows have
/// been substituted into the buffer. The per-group emission shape is
/// identical; only the path that populates the buffer differs.
pub(crate) fn commit_correlation_buffers(
    ctx: &mut ExecutorContext<'_>,
) -> Result<(), PipelineError> {
    use std::collections::BTreeMap;

    let Some(mut buffers) = ctx.correlation_buffers.take() else {
        return Ok(());
    };

    // Stable iteration order — sort by formatted group key so DLQ
    // emission and writer flush are deterministic across runs.
    let mut group_keys: Vec<Vec<GroupByKey>> = buffers.keys().cloned().collect();
    group_keys.sort_by_key(|k| format_group_key(k));

    // Phase 1: per-group disposition. Clean groups' records flow
    // into `clean_per_output` keyed by output_name; the BTreeMap key
    // ordering is the writer-flush order. Dirty groups land in the
    // DLQ inline.
    let mut clean_per_output: BTreeMap<String, Vec<CorrelationRecordSlot>> = BTreeMap::new();
    for group_key in group_keys {
        let Some(group) = buffers.remove(&group_key) else {
            continue;
        };
        commit_one_group(ctx, &group_key, group, &mut clean_per_output)?;
    }

    // Phase 2: drain the clean per-output queues to writers.
    flush_clean_records_to_writers(ctx, clean_per_output)?;

    Ok(())
}

fn format_group_key(key: &[GroupByKey]) -> String {
    let parts: Vec<String> = key
        .iter()
        .map(|k| match k {
            GroupByKey::Null => "null".to_string(),
            GroupByKey::Bool(b) => b.to_string(),
            GroupByKey::Int(i) => i.to_string(),
            GroupByKey::Float(bits) => f64::from_bits(*bits).to_string(),
            GroupByKey::Decimal(_) => k.to_value().to_string(),
            GroupByKey::Str(s) => format!("{s:?}"),
            GroupByKey::Date(d) => d.to_string(),
            GroupByKey::DateTime(ts) => ts.to_string(),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}

fn commit_one_group(
    ctx: &mut ExecutorContext<'_>,
    group_key: &[GroupByKey],
    group: CorrelationGroupBuffer,
    clean_per_output: &mut std::collections::BTreeMap<String, Vec<CorrelationRecordSlot>>,
) -> Result<(), PipelineError> {
    let CorrelationGroupBuffer {
        records,
        error_rows,
        error_messages,
        total_records,
        overflowed,
    } = group;

    if overflowed {
        // Overflow disposition: one root-cause DLQ entry with
        // category=GroupSizeExceeded and trigger=true; every other
        // buffered record of the group becomes a collateral with
        // category=Correlated and trigger=false. Per-record original
        // records come from `records` (the projected buffer holds the
        // un-projected original alongside).
        let mut emitted_trigger = false;
        let group_repr = format_group_key(group_key);
        let overflow_msg = PipelineError::CorrelationGroupOverflow {
            group_key: group_repr.clone(),
            count: total_records,
        }
        .to_string();
        // Dedup distinct rows by (source, row_num) so Route fan-out
        // (one row to N outputs) emits one DLQ entry, not N. Pairing on
        // `source_name` matters under multi-source ingest where row_num
        // is per-source.
        let mut seen_rows: HashSet<(Arc<str>, u64)> = HashSet::new();
        for slot in &records {
            let source_name = source_name_arc_of(&slot.original_record);
            if !seen_rows.insert((Arc::clone(&source_name), slot.row_num)) {
                continue;
            }
            let (category, trigger, error_message) = if !emitted_trigger {
                emitted_trigger = true;
                (
                    clinker_core_types::dlq::DlqErrorCategory::GroupSizeExceeded,
                    true,
                    overflow_msg.clone(),
                )
            } else {
                (
                    clinker_core_types::dlq::DlqErrorCategory::Correlated,
                    false,
                    format!("correlated with failure in group: {overflow_msg}"),
                )
            };
            push_dlq(
                ctx,
                DlqEntry {
                    source_row: slot.row_num,
                    category,
                    error_message,
                    original_record: slot.original_record.clone(),
                    stage: Some("correlation_commit".to_string()),
                    route: None,
                    trigger,
                    source_name,
                    triggering_field: None,
                    triggering_value: None,
                },
            )?;
        }
        // Per-source rollback rewind: each contributing source rewinds
        // its `rollback_cursors` entry to the lowest `row_num` of any
        // group member from that source. The cursor narrows the replay
        // anchor so a downstream resume reprocesses every record that
        // contributed to the overflowing group, including those whose
        // forward operators had already advanced the cursor past them.
        // No causal-source attribution is required for an overflow —
        // every contributing source shared blame proportionally — so
        // every source contributing a slot rewinds independently.
        let mut per_source_min: HashMap<Arc<str>, u64> = HashMap::new();
        for slot in &records {
            let sn = source_name_arc_of(&slot.original_record);
            per_source_min
                .entry(sn)
                .and_modify(|m| *m = (*m).min(slot.row_num))
                .or_insert(slot.row_num);
        }
        for (sn, min_rn) in per_source_min {
            ctx.rollback_cursors
                .entry(sn)
                .and_modify(|c| *c = (*c).min(min_rn))
                .or_insert(min_rn);
        }
        return Ok(());
    }

    let group_dirty = !error_rows.is_empty();
    if !group_dirty {
        // Clean group → drop the records into the per-output queue
        // for batched flush after every group has been visited.
        for slot in records {
            clean_per_output
                .entry(slot.output_name.clone())
                .or_default()
                .push(slot);
        }
        return Ok(());
    }

    // Dirty group → drop projected records, emit DLQ entries for every
    // distinct row_num touched by the group. Triggers come from
    // `error_messages`; collaterals come from `records` (rows that
    // succeeded their leg but get rolled back because the group failed).
    let first_err_message = error_messages
        .first()
        .map(|e| e.error_message.clone())
        .unwrap_or_else(|| "unknown".to_string());

    // Per-source narrowing: a collateral slot is spared whenever its
    // originating Source did not contribute any trigger error to this
    // group. With multi-source ingest, a single trigger from `src_b`
    // would otherwise DLQ every co-grouped `src_a` slot purely by
    // sharing the correlation key — `src_a` had no causal role in the
    // failure. The set is built from `error_messages` before the
    // trigger loop so each trigger's `$source.name` stamp drives its
    // own narrowing of the collateral walk below. Empty in a
    // single-source pipeline by construction: every co-grouped slot
    // shares the failing source, so the wider behavior is
    // bit-identical to today's pipeline-wide collateral DLQ.
    let failing_sources: HashSet<Arc<str>> = error_messages
        .iter()
        .map(|err| source_name_arc_of(&err.original_record))
        .collect();

    // Dedup distinct rows by (source, row_num) so Route fan-out (one
    // row to N outputs) emits one DLQ entry, not N. Pairing on
    // `source_name` matters under multi-source ingest where row_num is
    // per-source — pre-sprint-7 `HashSet<u64>` collided across sources
    // and silently dropped co-rowed slots from the collateral walk.
    let mut seen_rows: HashSet<(Arc<str>, u64)> = HashSet::new();
    // Trigger entries: one per distinct erroring row. Multiple errors
    // per row (different branches failing) collapse to a single trigger
    // entry carrying the first error.
    for err in &error_messages {
        let source_name = source_name_arc_of(&err.original_record);
        if !seen_rows.insert((Arc::clone(&source_name), err.row_num)) {
            continue;
        }
        push_dlq(
            ctx,
            DlqEntry {
                source_row: err.row_num,
                category: err.category,
                error_message: err.error_message.clone(),
                original_record: err.original_record.clone(),
                stage: err.stage.clone(),
                route: err.route.clone(),
                trigger: true,
                source_name,
                triggering_field: None,
                triggering_value: None,
            },
        )?;
    }
    // Collateral entries: every other distinct row that flowed through
    // the group's Output buffers but didn't itself error. Two sparing
    // axes apply, in order:
    //   1. Per-source narrowing — a slot from a non-failing source is
    //      always spared. Falls back to today's `Any` semantics within
    //      single-source pipelines.
    //   2. CorrelationFanoutPolicy interpretation — Any/All/Primary
    //      operate WITHIN the failing-source's records.
    // The resolved policy is read per-Output (per-Combine / per-Output
    // overrides win against the pipeline default). Today the strict
    // path always resolves to `Any` so axis 2 sparing is a no-op; the
    // wire is in place for the relaxed-CK orchestrator to substitute a
    // different policy.
    for slot in &records {
        let slot_source = source_name_arc_of(&slot.original_record);
        if seen_rows.contains(&(Arc::clone(&slot_source), slot.row_num)) {
            continue;
        }
        // Per-source spare: slot's Source did not contribute a trigger
        // to this group, so it never had a causal role in the failure.
        // Slots stamped as `MERGED_SOURCE_NAME` carry no single-source
        // attribution — Combine outputs (multi-input fold) and
        // synthetic aggregate emits both reach this branch. Treating
        // them as ambiguous, they stay on the collateral path so a
        // downstream failure on a Combine-derived row still flushes
        // the upstream-trigger correlation under today's
        // pipeline-wide-equivalent semantics. Per-source narrowing
        // only spares slots whose stamp identifies a distinct
        // non-failing Source.
        let slot_attributable = slot_source.as_ref() != MERGED_SOURCE_NAME.as_ref();
        if slot_attributable && !failing_sources.contains(&slot_source) {
            clean_per_output
                .entry(slot.output_name.clone())
                .or_default()
                .push(slot.clone());
            continue;
        }
        let policy = crate::executor::commit::output_fanout_policy(ctx, &slot.output_name);
        // Strict path: every slot's CK identity already equals the
        // group's CK identity by construction (the buffer key is
        // shared). `is_full_tuple_match` is `true`; `is_primary_match`
        // is also `true`. Under `Any` the slot DLQs; under `All` the
        // slot DLQs (full match); under `Primary` the slot DLQs
        // (primary match). The relaxed orchestrator's flush path
        // populates these flags differently per slot when multi-CK
        // fan-out makes the match partial.
        let spare = crate::executor::commit::should_spare_collateral(policy, true, true);
        if spare {
            // Spared collateral lands in the per-output clean queue
            // alongside the originally-clean records.
            clean_per_output
                .entry(slot.output_name.clone())
                .or_default()
                .push(slot.clone());
            continue;
        }
        if !seen_rows.insert((Arc::clone(&slot_source), slot.row_num)) {
            continue;
        }
        push_dlq(
            ctx,
            DlqEntry {
                source_row: slot.row_num,
                category: clinker_core_types::dlq::DlqErrorCategory::Correlated,
                error_message: format!("correlated with failure in group: {first_err_message}"),
                original_record: slot.original_record.clone(),
                stage: Some("correlation_commit".to_string()),
                route: None,
                trigger: false,
                source_name: slot_source,
                triggering_field: None,
                triggering_value: None,
            },
        )?;
    }

    Ok(())
}

fn flush_clean_records_to_writers(
    ctx: &mut ExecutorContext<'_>,
    per_output: std::collections::BTreeMap<String, Vec<CorrelationRecordSlot>>,
) -> Result<(), PipelineError> {
    'outputs: for (output_name, slots) in per_output {
        if slots.is_empty() {
            continue;
        }
        let out_cfg = ctx
            .output_configs
            .iter()
            .find(|o| o.name == output_name)
            .unwrap_or(ctx.primary_output);
        let mut structured_guard = StructuredOutputDocumentGuard::new(&out_cfg.format);
        for slot in &slots {
            if let Err(err) = structured_guard.observe(&output_name, slot.projected.doc_ctx()) {
                ctx.output_errors.push(err);
                continue 'outputs;
            }
        }
        let output_schema = Arc::clone(slots[0].projected.schema());

        let Some(raw_writer) = ctx.writers.remove(&output_name) else {
            continue;
        };
        match build_format_writer(out_cfg, raw_writer, Arc::clone(&output_schema)) {
            Ok(mut writer) => {
                let mut write_failed = false;
                for slot in &slots {
                    let write_result = {
                        let _guard = ctx.write_timer.guard();
                        writer.write_record(&slot.projected)
                    };
                    if let Err(e) = write_result {
                        push_write_error(&mut ctx.output_errors, e);
                        write_failed = true;
                        break;
                    }
                }
                if !write_failed {
                    let flush_result = {
                        let _guard = ctx.write_timer.guard();
                        writer.flush()
                    };
                    if let Err(e) = flush_result {
                        push_write_error(&mut ctx.output_errors, e);
                    }
                }
            }
            Err(e) => ctx.output_errors.push(e),
        }

        // Counter accounting: per-row newly-ok bookkeeping was
        // deferred at Output arm time when buffering. Apply it here
        // so records never count toward `ok_count` if their group
        // rolled back.
        let mut newly_ok: u64 = 0;
        for slot in &slots {
            if ctx.ok_source_rows.insert(slot.row_num) {
                newly_ok += 1;
            }
        }
        ctx.counters.ok_count += newly_ok;
        ctx.counters.records_written += slots.len() as u64;
        ctx.records_emitted += slots.len() as u64;
    }
    Ok(())
}
