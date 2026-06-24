//! Executor-internal transform spec and the per-record transform
//! evaluation helpers the dispatch arms drive.

use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::pipeline::arena::Arena;
use crate::pipeline::index::{GroupByKey, value_to_group_key};
use crate::pipeline::window_context::PartitionWindowContext;
use clinker_plan::plan::execution::ExecutionPlanDag;
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, SkipReason};

use super::{NullStorage, record_with_emitted_fields};

/// Single emitted (or skipped) output of a per-record transform
/// evaluation. The `Result<(), SkipReason>` distinguishes a real emit
/// (`Ok(())`) from a `filter`/`distinct` skip; the leading `Record` is
/// the output record (or the original record on Skip). One entry per
/// emitted record under `emit each` fan-out.
pub(crate) type TransformOutput = (Record, Result<(), SkipReason>);

/// Error shape returned by [`evaluate_single_transform`] and
/// [`evaluate_single_transform_windowed`] — pairs the transform name
/// with the underlying [`cxl::eval::EvalError`] for DLQ classification.
pub(crate) type TransformEvalError = (String, cxl::eval::EvalError);

/// Evaluate a single transform against a record, returning the
/// modified record with emitted fields and metadata merged in.
///
/// Used by the DAG-walking executor for per-node evaluation. The
/// record schema is widened in-place (rebuilt onto a schema that
/// includes every emitted field) so `Record::set` always hits a
/// known slot. Downstream positional-index consumers (aggregation
/// `group_by_indices`) rely on the upstream layout, so the widened
/// schema preserves every upstream column at its original index.
///
/// Returns a `Vec` of `(record, Ok | Skip)` entries — typically one
/// entry per call, but `emit each` fan-out produces N entries from a
/// single input record. On error, returns `(transform_name, EvalError)`.
#[allow(clippy::result_large_err)]
pub(crate) fn evaluate_single_transform(
    record: &Record,
    transform_name: &str,
    evaluator: &mut ProgramEvaluator,
    ctx: &EvalContext,
    _output_schema: &Arc<Schema>,
) -> Result<Vec<TransformOutput>, TransformEvalError> {
    let input = record;
    let result = evaluator
        .eval_record::<NullStorage>(ctx, input, None)
        .map_err(|e| (transform_name.to_string(), e))?;
    Ok(materialize_eval_result(input, result))
}

/// Convert an [`EvalResult`] into the per-record dispatch shape used
/// by the executor: one `(Record, Ok)` per emitted body record, or a
/// single `(Record, Skip)` entry on Filtered/Duplicate. The fan-out
/// case applies emitted fields and record-var writes on a fresh
/// `record_with_emitted_fields` projection of `input` once per
/// `EmitOne`; downstream cursor advancement happens once per emitted
/// record at the call site.
fn materialize_eval_result(input: &Record, result: EvalResult) -> Vec<TransformOutput> {
    match result {
        EvalResult::Emit {
            fields: emitted,
            record_vars,
            ..
        } => {
            let mut out = record_with_emitted_fields(input, &emitted);
            for (name, value) in &emitted {
                out.set(name, value.clone());
            }
            for (key, value) in *record_vars {
                let _ = out.set_record_var(&key, value);
            }
            vec![(out, Ok(()))]
        }
        EvalResult::EmitMany { records } => {
            let mut acc = Vec::with_capacity(records.len());
            for rec in records {
                let cxl::eval::EmitOne {
                    fields: emitted,
                    record_vars,
                } = rec;
                let mut out = record_with_emitted_fields(input, &emitted);
                for (name, value) in &emitted {
                    out.set(name, value.clone());
                }
                for (key, value) in *record_vars {
                    let _ = out.set_record_var(&key, value);
                }
                acc.push((out, Ok(())));
            }
            acc
        }
        EvalResult::Skip(reason) => vec![(input.clone(), Err(reason))],
    }
}

/// Window-resolution inputs for [`evaluate_single_transform_windowed`]:
/// the plan (for the analytic-window spec at `window_index`), the
/// resolved per-window arena + partition index, and this record's
/// position within that arena. Grouped so the windowed evaluator takes
/// one `&` argument for the window context, keeping the per-record
/// values (`record`, `transform_name`, `evaluator`, `ctx`) it shares
/// with [`evaluate_single_transform`] in the same positions.
pub(crate) struct WindowedEvalCtx<'a> {
    /// Execution DAG, indexed by `window_index` to fetch the
    /// transform's analytic-window spec (`group_by` / `sort_by`).
    pub(crate) plan: &'a ExecutionPlanDag,
    /// Index into `plan.indices_to_build` selecting this transform's
    /// window spec.
    pub(crate) window_index: usize,
    /// Resolved window runtime: the materialized arena plus its
    /// partition index, against which `$window.*` expressions evaluate.
    pub(crate) runtime: &'a crate::executor::window_runtime::WindowRuntime,
    /// This record's position within `runtime.arena`, used to locate
    /// its slot inside the resolved partition.
    pub(crate) record_pos: u64,
}

/// Same as [`evaluate_single_transform`] but threads a
/// [`PartitionWindowContext`] derived from the resolved
/// [`crate::executor::window_runtime::WindowRuntime`] so `$window.*`
/// expressions resolve against the transform's analytic window.
/// `window_index` indexes `plan.indices_to_build`.
///
/// The caller resolves `runtime` via
/// `ctx.window_runtime.resolve(&spec.root, window_index)` and is
/// responsible for surfacing `PipelineError::Internal` if `resolve`
/// returns `None` — at this point in the dispatch arm the upstream
/// operator has already finalized its emit, so a missing runtime is
/// always an invariant violation.
///
/// `record_pos` is the record's position in the arena, derived by the
/// caller from the spec's root: `(rn - 1)` for `Source(_)` roots
/// (Phase-0 materializes source records in row-number order), or the
/// per-record enumerate index for `Node{..}` and `ParentNode{..}` roots
/// (the upstream operator's emit order is the arena's order).
///
/// If the record's group-by values do not resolve to a partition (null
/// in any group key, or no matching partition), the evaluator runs
/// without a window context — matching the legacy inline arena path's
/// behavior.
#[allow(clippy::result_large_err)]
pub(crate) fn evaluate_single_transform_windowed(
    record: &Record,
    transform_name: &str,
    evaluator: &mut ProgramEvaluator,
    ctx: &EvalContext,
    window: &WindowedEvalCtx<'_>,
) -> Result<Vec<TransformOutput>, TransformEvalError> {
    let &WindowedEvalCtx {
        plan,
        window_index,
        runtime,
        record_pos,
    } = window;
    let spec = &plan.indices_to_build[window_index];
    let arena = runtime.arena.as_ref();
    let index = runtime.index.as_ref();

    let key: Option<Vec<GroupByKey>> = spec
        .group_by
        .iter()
        .map(|field| {
            let val = record.get(field).cloned().unwrap_or(Value::Null);
            value_to_group_key(&val, field, None, record_pos)
                .ok()
                .flatten()
        })
        .collect();

    let sort_fields: Vec<&str> = spec.sort_by.iter().map(|s| s.field.as_str()).collect();
    let result = if let Some(key) = key {
        if let Some(partition) = index.get(&key) {
            let pos_in_partition = partition.iter().position(|&p| p == record_pos).unwrap_or(0);
            let wctx =
                PartitionWindowContext::new(arena, partition, pos_in_partition, &sort_fields);
            evaluator
                .eval_record(ctx, record, Some(&wctx))
                .map_err(|e| (transform_name.to_string(), e))?
        } else {
            evaluator
                .eval_record::<Arena>(ctx, record, None)
                .map_err(|e| (transform_name.to_string(), e))?
        }
    } else {
        evaluator
            .eval_record::<Arena>(ctx, record, None)
            .map_err(|e| (transform_name.to_string(), e))?
    };

    Ok(materialize_eval_result(record, result))
}
