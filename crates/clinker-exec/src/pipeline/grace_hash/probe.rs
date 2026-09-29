//! Probe-side match emission for the grace hash join. The same
//! per-probe emit routine serves all three streams that reach a built
//! hash table — the in-memory probe phase, the spilled-partition
//! reload, and the BNL fallback — so match-mode and on-miss semantics
//! stay identical across spill boundaries.

use std::sync::Arc;

use clinker_record::owned_storage::{OwnedKey, OwnedMap, OwnedValues, SharedStorage};
use clinker_record::{Record, Schema, Value};
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, SkipReason};

use super::RecordOrder;
use crate::executor::combine::{CombineResolver, CombineResolverMapping};
use crate::executor::widen_record_to_schema;
use crate::pipeline::combine::BuildSeq;
use crate::pipeline::combine::{CombineOutputEvalFailure, MatchedBuildFailure, ProbeIter};
use crate::pipeline::combine_verdict::{
    Admit, DriverScan, DriverVerdict, MissToken, PredicateOutcome, eval_predicate,
};
use clinker_plan::config::pipeline_node::{MatchMode, OnMiss};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::combine::DecomposedPredicate;

/// Cap on matches collected per driver under [`MatchMode::Collect`].
/// Mirrors the constant in `pipeline::combine` and `pipeline::iejoin` so
/// every code path truncates at the same threshold.
const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// Outcome of [`super::GraceHashExecutor::probe_record`]. Either the
/// in-memory matches (caller walks them inline) or a marker that the
/// record was written to a probe-side spill file.
pub(crate) enum ProbeOutcome<'a> {
    InMemory(ProbeMatches<'a>),
    Spilled,
}

/// One probe's candidates against a built hash table, with the row id and
/// [`BuildSeq`] of every build row that table was built from.
///
/// Invariant: `build_ids[i]` is the row id its Source minted and the
/// arrival position of the build record the table reports at
/// `ProbeCandidate.index == i`, and a probe yields a key's candidates in
/// ascending arrival position. [`CandidateOrder`] checks both, so a table
/// that reported an index with no build row, or walked its candidates out of
/// arrival order, fails as `PipelineError::Internal` instead of reporting or
/// picking another row.
pub(crate) struct ProbeMatches<'a> {
    pub(crate) candidates: ProbeIter<'a>,
    pub(crate) build_ids: &'a [(RecordOrder, BuildSeq)],
}

/// The candidate order of one probe: each candidate's row id and
/// [`BuildSeq`], with the positions checked to rise strictly, so `first`,
/// `all` and `collect` follow build arrival order by construction rather
/// than by the table's layout.
struct CandidateOrder<'a> {
    build_ids: &'a [(RecordOrder, BuildSeq)],
    last: Option<BuildSeq>,
    name: &'a str,
}

impl<'a> CandidateOrder<'a> {
    fn new(build_ids: &'a [(RecordOrder, BuildSeq)], name: &'a str) -> Self {
        Self {
            build_ids,
            last: None,
            name,
        }
    }

    /// The row id and arrival position of the candidate at `index`, after
    /// checking its position follows the previous candidate's.
    fn next(&mut self, index: usize) -> Result<(RecordOrder, BuildSeq), PipelineError> {
        let (row, seq) =
            self.build_ids
                .get(index)
                .copied()
                .ok_or_else(|| PipelineError::Internal {
                    op: "grace_hash probe",
                    node: self.name.to_string(),
                    detail: format!(
                        "hash table candidate index {index} has no build row; the partition \
                         holds {}",
                        self.build_ids.len()
                    ),
                })?;
        if self.last.is_some_and(|last| seq <= last) {
            return Err(PipelineError::Internal {
                op: "grace_hash probe",
                node: self.name.to_string(),
                detail: format!(
                    "build candidates out of arrival order: position {} after {}",
                    seq.0,
                    self.last.map_or(0, |last| last.0)
                ),
            });
        }
        self.last = Some(seq);
        Ok((row, seq))
    }
}

/// Mutable emission targets threaded through every grace-hash emit path
/// (in-memory probe, spilled reload, BNL fallback). `records` accumulates
/// emitted output rows; `failures` accumulates recoverable output-stage
/// eval failures the dispatcher routes to the dead-letter queue. Bundled
/// so the emit helpers stay under clippy's too-many-arguments cap.
pub(super) struct GraceEmitSink<'a> {
    pub(super) records: &'a mut Vec<(Record, RecordOrder)>,
    pub(super) failures: &'a mut Vec<CombineOutputEvalFailure>,
    /// Combine node name, for the E325 runaway-cap diagnostic.
    pub(super) name: &'a str,
    /// Opt-in per-combine output-row cap (E325); `None` is unlimited. `records`
    /// is the single output vec threaded across every phase (in-memory probe,
    /// spilled reload, BNL), so its length is the combine's cumulative row count.
    pub(super) max_output_rows: Option<u64>,
}

impl GraceEmitSink<'_> {
    /// Push one emitted output row, failing loud (E325) the moment it would carry
    /// the cumulative count past `max_output_rows` rather than truncating.
    /// Checked before the push, so exactly `cap` rows land and the first over-cap
    /// row aborts — a result-size ceiling independent of the memory budget.
    pub(super) fn push_row(
        &mut self,
        record: Record,
        rn: RecordOrder,
    ) -> Result<(), PipelineError> {
        if let Some(cap) = self.max_output_rows
            && self.records.len() as u64 >= cap
        {
            return Err(PipelineError::CombineOutputCapExceeded {
                combine: self.name.to_string(),
                cap,
            });
        }
        self.records.push((record, rn));
        Ok(())
    }
}

/// Shape-stable bundle for [`emit_for_probe`]. Bundling the per-call
/// arguments keeps the function signature under clippy's
/// too-many-arguments cap and lets call sites update one field
/// without rewriting the call site.
pub(super) struct EmitArgs<'a> {
    pub(super) name: &'a str,
    pub(super) decomposed: &'a DecomposedPredicate,
    pub(super) resolver_mapping: &'a CombineResolverMapping,
    pub(super) output_schema: Option<&'a SharedStorage<Schema>>,
    pub(super) match_mode: MatchMode,
    pub(super) on_miss: OnMiss,
    pub(super) build_qualifier: &'a str,
    pub(super) propagate_ck: &'a clinker_plan::config::pipeline_node::PropagateCkSpec,
    pub(super) strategy: clinker_plan::config::ErrorStrategy,
}

/// Per-probe emission. Walks the probe iterator in build arrival order,
/// feeds each candidate's residual outcome to the driver's [`DriverScan`],
/// and emits records under its verdict: the configured match mode, and the
/// on_miss policy only for a driver with no true and no failed candidate.
/// Mirrors the inline HashBuildProbe arm so the grace path stays
/// behavior-compatible across spill boundaries.
pub(super) fn emit_for_probe<'a>(
    args: &EmitArgs<'_>,
    probe_record: &Record,
    rn: RecordOrder,
    matches: ProbeMatches<'a>,
    body_evaluator: Option<&mut ProgramEvaluator>,
    ctx: &EvalContext<'_>,
    sink: &mut GraceEmitSink<'_>,
) -> Result<(), PipelineError> {
    let EmitArgs {
        name,
        decomposed,
        resolver_mapping,
        output_schema,
        match_mode,
        build_qualifier,
        propagate_ck,
        strategy,
        ..
    } = *args;
    let ProbeMatches {
        candidates: probe_iter,
        build_ids,
    } = matches;
    let mut order = CandidateOrder::new(build_ids, name);
    let mut residual_eval = decomposed
        .residual
        .as_ref()
        .map(|residual| ProgramEvaluator::new(Arc::clone(residual), false));
    let fail_fast = strategy == clinker_plan::config::ErrorStrategy::FailFast;
    let failure_for = |record: &Record, row: RecordOrder, error| CombineOutputEvalFailure {
        probe_record: probe_record.clone(),
        row: rn,
        matched_build: Some(MatchedBuildFailure {
            record: record.clone(),
            row,
        }),
        error,
        failed_at: crate::executor::DlqFailureStamp::now(),
    };
    match match_mode {
        MatchMode::Collect => {
            let mut scan: DriverScan<(), CombineOutputEvalFailure> =
                DriverScan::new(MatchMode::Collect);
            let mut arr: Vec<Value> = Vec::new();
            let mut first_build: Option<Record> = None;
            let mut truncated = false;
            for cand in probe_iter {
                let (row, seq) = order.next(cand.index)?;
                let admit = match residual_outcome(
                    residual_eval.as_mut(),
                    resolver_mapping,
                    probe_record,
                    cand.record,
                    ctx,
                    name,
                )? {
                    PredicateOutcome::True => scan.observe_true(seq.0, || ()),
                    PredicateOutcome::NotTrue => continue,
                    PredicateOutcome::Failed(e) => {
                        scan.observe_failed(seq.0, || failure_for(cand.record, row, e))
                    }
                };
                match admit {
                    Admit::Take => {
                        if arr.len() >= COLLECT_PER_GROUP_CAP {
                            // Past the cap no element is kept, but every later
                            // candidate is still evaluated so each failure
                            // among them is written.
                            truncated = true;
                            continue;
                        }
                        if first_build.is_none() {
                            first_build = Some(cand.record.clone());
                        }
                        // Build-side records contribute only their
                        // user-declared field values to the collect array.
                        // `iter_user_fields` filters every engine-stamped
                        // column — both `$ck.*` (correlation lineage) and
                        // `$widened` (auto_widen sidecar; build-side
                        // sidecars drop at the join boundary by design,
                        // mirroring `propagate_ck: Driver`). Without this
                        // filter, a build record's `$widened` `Value::Map`
                        // payload nests inside the collect-mode
                        // `Value::Map` and reaches the writer as a nested
                        // Map, triggering
                        // `FormatError::UnserializableMapValue`.
                        let mut m: indexmap::IndexMap<OwnedKey, Value> = indexmap::IndexMap::new();
                        for (fname, val) in cand.record.iter_user_fields() {
                            m.insert(fname.into(), val.clone());
                        }
                        arr.push(Value::Map(OwnedMap::from_map(m)));
                    }
                    Admit::Fail(failure) => {
                        if fail_fast {
                            return Err(PipelineError::from(failure.error));
                        }
                        sink.failures.push(failure);
                        // The array is unknown now; release what it held.
                        arr = Vec::new();
                        first_build = None;
                    }
                    Admit::Decides { .. } | Admit::Ignore => {}
                }
            }
            match scan.finish() {
                DriverVerdict::Collected => {}
                DriverVerdict::CollectFailed => return Ok(()),
                other => return Err(other.mode_mismatch("grace_hash probe", name)),
            }
            if truncated {
                eprintln!(
                    "W: combine {:?} match: collect truncated at \
                     {COLLECT_PER_GROUP_CAP} matches for driver row {rn}",
                    name
                );
            }
            let mut rec = match output_schema {
                Some(s) => widen_record_to_schema(probe_record, s),
                None => probe_record.clone(),
            };
            if let Some(b) = first_build.as_ref() {
                crate::executor::copy_build_ck_columns(&mut rec, b, propagate_ck);
            }
            rec.set(build_qualifier, Value::Array(OwnedValues::from_vec(arr)));
            sink.push_row(rec, rn)?;
        }
        MatchMode::First | MatchMode::All => {
            let mut scan: DriverScan<(Record, RecordOrder), CombineOutputEvalFailure> =
                DriverScan::new(match_mode);
            let mut taken: Vec<(Record, RecordOrder)> = Vec::new();
            // Candidates arrive in build arrival order ([`CandidateOrder`]
            // checks it), so a `first` driver stops at its deciding one.
            for cand in probe_iter {
                if scan.settled() {
                    break;
                }
                let (row, seq) = order.next(cand.index)?;
                match residual_outcome(
                    residual_eval.as_mut(),
                    resolver_mapping,
                    probe_record,
                    cand.record,
                    ctx,
                    name,
                )? {
                    PredicateOutcome::True => {
                        if let Admit::Take = scan.observe_true(seq.0, || (cand.record.clone(), row))
                        {
                            taken.push((cand.record.clone(), row));
                        }
                    }
                    PredicateOutcome::NotTrue => {}
                    PredicateOutcome::Failed(e) => {
                        if let Admit::Fail(failure) =
                            scan.observe_failed(seq.0, || failure_for(cand.record, row, e))
                        {
                            if fail_fast {
                                return Err(PipelineError::from(failure.error));
                            }
                            sink.failures.push(failure);
                        }
                    }
                }
            }
            let matched = match scan.finish() {
                DriverVerdict::Selected(pick) => vec![pick],
                DriverVerdict::Pairs => taken,
                DriverVerdict::FailedFirst(failure) => {
                    if fail_fast {
                        return Err(PipelineError::from(failure.error));
                    }
                    sink.failures.push(failure);
                    return Ok(());
                }
                DriverVerdict::Miss(miss) => {
                    return apply_on_miss(args, miss, probe_record, rn, body_evaluator, ctx, sink);
                }
                other => return Err(other.mode_mismatch("grace_hash probe", name)),
            };
            if let Some(evaluator) = body_evaluator {
                for (m, build_row) in &matched {
                    let resolver = CombineResolver::new(resolver_mapping, probe_record, Some(m));
                    match evaluator.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Emit {
                            fields: emitted,
                            record_vars,
                            ..
                        }) => {
                            let mut rec = match output_schema {
                                Some(s) => widen_record_to_schema(probe_record, s),
                                None => probe_record.clone(),
                            };
                            for (n, v) in emitted {
                                rec.set(&n, v);
                            }
                            for (k, v) in *record_vars {
                                let _ = rec.set_record_var(&k, v);
                            }
                            crate::executor::copy_build_ck_columns(&mut rec, m, propagate_ck);
                            sink.push_row(rec, rn)?;
                        }
                        Ok(EvalResult::Skip(_)) => {}
                        Ok(EvalResult::EmitMany { .. }) => {
                            return Err(PipelineError::Internal {
                                op: "grace_hash body",
                                node: name.to_string(),
                                detail: "emit_each fan-out is not supported in a combine body"
                                    .into(),
                            });
                        }
                        Err(e) => {
                            if strategy == clinker_plan::config::ErrorStrategy::FailFast {
                                return Err(PipelineError::from(e));
                            }
                            sink.failures.push(CombineOutputEvalFailure {
                                probe_record: probe_record.clone(),
                                row: rn,
                                matched_build: Some(MatchedBuildFailure {
                                    record: m.clone(),
                                    row: *build_row,
                                }),
                                error: e,
                                failed_at: crate::executor::DlqFailureStamp::now(),
                            });
                            continue;
                        }
                    }
                }
            } else {
                // Body-less synthetic chain step: concatenate probe and
                // build values onto the encoded output schema. Mirrors
                // the executor's HashBuildProbe synthetic-step branch.
                let target_schema = output_schema.ok_or_else(|| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: "synthetic grace hash step has no output schema".to_string(),
                })?;
                for (m, _) in &matched {
                    let mut values: Vec<Value> = Vec::with_capacity(target_schema.column_count());
                    values.extend(probe_record.values().iter().cloned());
                    values.extend(m.values().iter().cloned());
                    if values.len() != target_schema.column_count() {
                        return Err(PipelineError::Internal {
                            op: "combine",
                            node: name.to_string(),
                            detail: format!(
                                "synthetic grace hash step produced {} values; encoded schema \
                                 has {} columns",
                                values.len(),
                                target_schema.column_count()
                            ),
                        });
                    }
                    let rec = Record::new(target_schema.clone(), values);
                    sink.push_row(rec, rn)?;
                }
            }
        }
    }
    Ok(())
}

/// The residual's outcome for one `(driver, build)` pair, or `True` when the
/// predicate has no residual beyond its equality keys.
fn residual_outcome(
    residual_eval: Option<&mut ProgramEvaluator>,
    resolver_mapping: &CombineResolverMapping,
    probe_record: &Record,
    build_record: &Record,
    ctx: &EvalContext<'_>,
    name: &str,
) -> Result<PredicateOutcome, PipelineError> {
    let Some(residual_eval) = residual_eval else {
        return Ok(PredicateOutcome::True);
    };
    let resolver = CombineResolver::new(resolver_mapping, probe_record, Some(build_record));
    eval_predicate::<NullStorage>(residual_eval, ctx, &resolver, "grace_hash residual", name)
}

/// Apply `on_miss` to a driver whose scan found no true and no failed
/// candidate: `skip` drops it, `error` raises E319, `null_fields` runs the
/// body over the driver alone. `_miss` is the scan's proof that the driver is
/// a miss.
fn apply_on_miss(
    args: &EmitArgs<'_>,
    _miss: MissToken,
    probe_record: &Record,
    rn: RecordOrder,
    body_evaluator: Option<&mut ProgramEvaluator>,
    ctx: &EvalContext<'_>,
    sink: &mut GraceEmitSink<'_>,
) -> Result<(), PipelineError> {
    let EmitArgs {
        name,
        resolver_mapping,
        output_schema,
        on_miss,
        strategy,
        ..
    } = *args;
    match on_miss {
        OnMiss::Skip => Ok(()),
        OnMiss::Error => Err(PipelineError::CombineMissingMatch {
            combine: name.to_string(),
            driver_row: rn.ordinal(),
        }),
        OnMiss::NullFields => {
            let resolver = CombineResolver::new(resolver_mapping, probe_record, None);
            let evaluator = body_evaluator.ok_or_else(|| PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: "grace hash on_miss: null_fields with no body program".to_string(),
            })?;
            match evaluator.eval_record::<NullStorage>(ctx, &resolver, None) {
                Ok(EvalResult::Emit {
                    fields: emitted,
                    record_vars,
                    ..
                }) => {
                    let mut rec = match output_schema {
                        Some(s) => widen_record_to_schema(probe_record, s),
                        None => probe_record.clone(),
                    };
                    for (n, v) in emitted {
                        rec.set(&n, v);
                    }
                    for (k, v) in *record_vars {
                        let _ = rec.set_record_var(&k, v);
                    }
                    sink.push_row(rec, rn)
                }
                Ok(EvalResult::Skip(SkipReason::Filtered | SkipReason::Duplicate)) => Ok(()),
                Ok(EvalResult::EmitMany { .. }) => Err(PipelineError::Internal {
                    op: "grace_hash on_miss body",
                    node: name.to_string(),
                    detail: "emit_each fan-out is not supported in a combine body".into(),
                }),
                Err(e) => {
                    if strategy == clinker_plan::config::ErrorStrategy::FailFast {
                        return Err(PipelineError::from(e));
                    }
                    sink.failures.push(CombineOutputEvalFailure {
                        probe_record: probe_record.clone(),
                        row: rn,
                        matched_build: None,
                        error: e,
                        failed_at: crate::executor::DlqFailureStamp::now(),
                    });
                    Ok(())
                }
            }
        }
    }
}

/// Placeholder `RecordStorage` for windowless expression evaluation.
/// Mirrors `pipeline::combine::NullStorage`; the grace hash path runs
/// outside any window and never queries this storage.
struct NullStorage;

impl clinker_record::RecordStorage for NullStorage {
    fn resolve_field(&self, _: u64, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _: u64) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u64 {
        0
    }
}
