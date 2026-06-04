//! Probe-side match emission for the grace hash join. The same
//! per-probe emit routine serves all three streams that reach a built
//! hash table — the in-memory probe phase, the spilled-partition
//! reload, and the BNL fallback — so match-mode and on-miss semantics
//! stay identical across spill boundaries.

use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, SkipReason};

use super::RecordOrder;
use crate::executor::combine::{CombineResolver, CombineResolverMapping};
use crate::executor::widen_record_to_schema;
use crate::pipeline::combine::{CombineOutputEvalFailure, ProbeIter};
use clinker_plan::config::pipeline_node::{MatchMode, OnMiss};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::combine::DecomposedPredicate;

/// Cap on matches collected per driver under [`MatchMode::Collect`].
/// Mirrors the constant in `pipeline::combine` and `pipeline::iejoin` so
/// every code path truncates at the same threshold.
const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// Outcome of [`super::GraceHashExecutor::probe_record`]. Either an
/// in-memory probe iterator (caller walks matches inline) or a marker
/// that the record was written to a probe-side spill file.
pub(crate) enum ProbeOutcome<'a> {
    InMemory(ProbeIter<'a>),
    Spilled,
}

/// Mutable emission targets threaded through every grace-hash emit path
/// (in-memory probe, spilled reload, BNL fallback). `records` accumulates
/// emitted output rows; `failures` accumulates recoverable output-stage
/// eval failures the dispatcher routes to the dead-letter queue. Bundled
/// so the emit helpers stay under clippy's too-many-arguments cap.
pub(super) struct GraceEmitSink<'a> {
    pub(super) records: &'a mut Vec<(Record, RecordOrder)>,
    pub(super) failures: &'a mut Vec<CombineOutputEvalFailure>,
}

/// Shape-stable bundle for [`emit_for_probe`]. Bundling the per-call
/// arguments keeps the function signature under clippy's
/// too-many-arguments cap and lets call sites update one field
/// without rewriting the call site.
pub(super) struct EmitArgs<'a> {
    pub(super) name: &'a str,
    pub(super) decomposed: &'a DecomposedPredicate,
    pub(super) resolver_mapping: &'a CombineResolverMapping,
    pub(super) output_schema: Option<&'a Arc<Schema>>,
    pub(super) match_mode: MatchMode,
    pub(super) on_miss: OnMiss,
    pub(super) build_qualifier: &'a str,
    pub(super) propagate_ck: &'a clinker_plan::config::pipeline_node::PropagateCkSpec,
    pub(super) strategy: clinker_plan::config::ErrorStrategy,
}

/// Per-probe emission. Walks the probe iterator, applies the residual
/// filter, and emits records under the configured match mode and on_miss
/// policy. Mirrors the inline HashBuildProbe arm so the grace path stays
/// behavior-compatible across spill boundaries.
pub(super) fn emit_for_probe<'a>(
    args: &EmitArgs<'_>,
    probe_record: &Record,
    rn: RecordOrder,
    probe_iter: ProbeIter<'a>,
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
        on_miss,
        build_qualifier,
        propagate_ck,
        strategy,
    } = *args;
    match match_mode {
        MatchMode::Collect => {
            let mut arr: Vec<Value> = Vec::new();
            let mut first_build: Option<Record> = None;
            let mut truncated = false;
            for cand in probe_iter {
                if let Some(residual) = decomposed.residual.as_ref() {
                    let resolver =
                        CombineResolver::new(resolver_mapping, probe_record, Some(cand.record));
                    let mut residual_eval = ProgramEvaluator::new(Arc::clone(residual), false);
                    match residual_eval.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Skip(_)) => continue,
                        Ok(EvalResult::Emit { .. }) => {}
                        Ok(EvalResult::EmitMany { .. }) => {
                            return Err(PipelineError::Internal {
                                op: "grace_hash residual",
                                node: name.to_string(),
                                detail: "emit_each fan-out is not supported in a combine residual filter".into(),
                            });
                        }
                        Err(e) => {
                            if strategy == clinker_plan::config::ErrorStrategy::FailFast {
                                return Err(PipelineError::from(e));
                            }
                            sink.failures.push(CombineOutputEvalFailure {
                                probe_record: probe_record.clone(),
                                row: rn,
                                matched_build: Some(cand.record.clone()),
                                error: e,
                            });
                            continue;
                        }
                    }
                }
                if arr.len() >= COLLECT_PER_GROUP_CAP {
                    truncated = true;
                    break;
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
                let mut m: indexmap::IndexMap<Box<str>, Value> = indexmap::IndexMap::new();
                for (fname, val) in cand.record.iter_user_fields() {
                    m.insert(fname.into(), val.clone());
                }
                arr.push(Value::Map(Box::new(m)));
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
            rec.set(build_qualifier, Value::Array(arr));
            sink.records.push((rec, rn));
        }
        MatchMode::First | MatchMode::All => {
            let matched: Vec<Record> = {
                let mut acc: Vec<Record> = Vec::new();
                for cand in probe_iter {
                    if let Some(residual) = decomposed.residual.as_ref() {
                        let resolver =
                            CombineResolver::new(resolver_mapping, probe_record, Some(cand.record));
                        let mut residual_eval = ProgramEvaluator::new(Arc::clone(residual), false);
                        match residual_eval.eval_record::<NullStorage>(ctx, &resolver, None) {
                            Ok(EvalResult::Skip(_)) => continue,
                            Ok(EvalResult::Emit { .. }) => {}
                            Ok(EvalResult::EmitMany { .. }) => {
                                return Err(PipelineError::Internal {
                                    op: "grace_hash residual",
                                    node: name.to_string(),
                                    detail: "emit_each fan-out is not supported in a combine residual filter".into(),
                                });
                            }
                            Err(e) => {
                                if strategy == clinker_plan::config::ErrorStrategy::FailFast {
                                    return Err(PipelineError::from(e));
                                }
                                sink.failures.push(CombineOutputEvalFailure {
                                    probe_record: probe_record.clone(),
                                    row: rn,
                                    matched_build: Some(cand.record.clone()),
                                    error: e,
                                });
                                continue;
                            }
                        }
                    }
                    acc.push(cand.record.clone());
                    if matches!(match_mode, MatchMode::First) {
                        break;
                    }
                }
                acc
            };
            if matched.is_empty() {
                match on_miss {
                    OnMiss::Skip => {}
                    OnMiss::Error => {
                        return Err(PipelineError::CombineMissingMatch {
                            combine: name.to_string(),
                            driver_row: rn,
                        });
                    }
                    OnMiss::NullFields => {
                        let resolver = CombineResolver::new(resolver_mapping, probe_record, None);
                        let evaluator = body_evaluator.ok_or_else(|| PipelineError::Internal {
                            op: "combine",
                            node: name.to_string(),
                            detail: "grace hash on_miss: null_fields with no body program"
                                .to_string(),
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
                                sink.records.push((rec, rn));
                            }
                            Ok(EvalResult::Skip(SkipReason::Filtered)) => {}
                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => {}
                            Ok(EvalResult::EmitMany { .. }) => {
                                return Err(PipelineError::Internal {
                                    op: "grace_hash on_miss body",
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
                                    matched_build: None,
                                    error: e,
                                });
                            }
                        }
                    }
                }
            } else if let Some(evaluator) = body_evaluator {
                for m in &matched {
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
                            sink.records.push((rec, rn));
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
                                matched_build: Some(m.clone()),
                                error: e,
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
                for m in &matched {
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
                    let rec = Record::new(Arc::clone(target_schema), values);
                    sink.records.push((rec, rn));
                }
            }
        }
    }
    Ok(())
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
