//! Executor-internal transform spec + compiled transform, and the
//! per-record transform evaluation helpers the dispatch arms drive.

use std::sync::Arc;

use clinker_record::{Record, Schema, Value};

use crate::config::PipelineConfig;
use crate::pipeline::arena::Arena;
use crate::pipeline::index::{GroupByKey, value_to_group_key};
use crate::pipeline::window_context::PartitionWindowContext;
use crate::plan::execution::ExecutionPlanDag;
use cxl::ast::Statement;
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, SkipReason};
use cxl::typecheck::TypedProgram;

use super::{NullStorage, record_with_emitted_fields};

/// Executor-internal transform spec.
///
/// Produced by walking `PipelineConfig::nodes` directly and matching on
/// `PipelineNode::{Transform, Aggregate, Route}` variants. Used by
/// executor read paths that still consume a flat Vec-of-transforms view.
/// The fields are the superset that executor sites read from; see
/// `build_transform_specs`.
#[derive(Debug, Clone)]
pub struct TransformSpec {
    pub name: String,
    pub cxl: Option<String>,
    pub aggregate: Option<crate::config::AggregateConfig>,
    pub route: Option<crate::config::RouteConfig>,
    pub input: Option<crate::config::TransformInput>,
    /// Per-record fan-out ceiling for `emit each` blocks inside this
    /// transform. Threaded to the evaluator at construction time and
    /// surfaces as a typed DLQ entry on overflow.
    pub max_expansion: u64,
}

impl TransformSpec {
    pub fn cxl_source(&self) -> &str {
        if let Some(agg) = &self.aggregate {
            agg.cxl.as_str()
        } else if let Some(s) = &self.cxl {
            s.as_str()
        } else {
            ""
        }
    }
}

/// Walk `PipelineConfig::nodes` and materialize a flat `Vec<TransformSpec>`
/// from the Transform/Aggregate/Route variants in declaration order. Merge
/// nodes referenced by a transform's input are expanded back into
/// `TransformInput::Multiple(list)` to match the legacy executor wire shape.
pub fn build_transform_specs(config: &PipelineConfig) -> Vec<TransformSpec> {
    use crate::config::node_header::NodeInput;
    use crate::config::{AggregateConfig, PipelineNode, RouteBranch, RouteConfig, TransformInput};

    let merge_by_name: std::collections::HashMap<&str, Vec<String>> = config
        .nodes
        .iter()
        .filter_map(|n| match &n.value {
            PipelineNode::Merge { header, .. } => {
                let upstreams: Vec<String> = header
                    .inputs
                    .iter()
                    .map(|spanned_ni| match &spanned_ni.value {
                        NodeInput::Single(s) => s.clone(),
                        NodeInput::Port { node, port } => format!("{node}.{port}"),
                    })
                    .collect();
                Some((header.name.as_str(), upstreams))
            }
            _ => None,
        })
        .collect();

    let project_input = |ni: &NodeInput| -> Option<TransformInput> {
        match ni {
            NodeInput::Single(s) => {
                if let Some(upstreams) = merge_by_name.get(s.as_str()) {
                    Some(TransformInput::Multiple(upstreams.clone()))
                } else {
                    Some(TransformInput::Single(s.clone()))
                }
            }
            NodeInput::Port { node, port } => {
                Some(TransformInput::Single(format!("{node}.{port}")))
            }
        }
    };

    let mut out = Vec::new();
    for spanned in &config.nodes {
        match &spanned.value {
            PipelineNode::Transform {
                header,
                config: body,
            } => {
                out.push(TransformSpec {
                    name: header.name.clone(),
                    cxl: Some(body.cxl.as_ref().to_string()),
                    aggregate: None,
                    route: None,
                    input: project_input(&header.input.value),
                    max_expansion: body.max_expansion,
                });
            }
            PipelineNode::Aggregate {
                header,
                config: body,
            } => {
                out.push(TransformSpec {
                    name: header.name.clone(),
                    cxl: None,
                    aggregate: Some(AggregateConfig {
                        group_by: body.group_by.clone(),
                        cxl: body.cxl.as_ref().to_string(),
                        strategy: body.strategy,
                        time_window: body.time_window.clone(),
                        allowed_lateness: body.allowed_lateness,
                    }),
                    route: None,
                    input: project_input(&header.input.value),
                    max_expansion: cxl::eval::DEFAULT_MAX_EXPANSION,
                });
            }
            PipelineNode::Route {
                header,
                config: body,
            } => {
                let branches: Vec<RouteBranch> = body
                    .conditions
                    .iter()
                    .map(|(name, cxl)| RouteBranch {
                        name: name.clone(),
                        condition: cxl.as_ref().to_string(),
                    })
                    .collect();
                out.push(TransformSpec {
                    name: header.name.clone(),
                    cxl: Some(String::new()),
                    aggregate: None,
                    route: Some(RouteConfig {
                        mode: body.mode,
                        branches,
                        default: body.default.clone(),
                    }),
                    input: project_input(&header.input.value),
                    max_expansion: cxl::eval::DEFAULT_MAX_EXPANSION,
                });
            }
            _ => {}
        }
    }
    out
}

/// Compiled transform: CXL source compiled once, evaluated per record.
#[derive(Debug)]
pub struct CompiledTransform {
    pub(crate) name: String,
    pub(crate) typed: Arc<TypedProgram>,
    pub(crate) max_expansion: u64,
}

impl CompiledTransform {
    pub(crate) fn has_distinct(&self) -> bool {
        self.typed
            .program
            .statements
            .iter()
            .any(|s| matches!(s, Statement::Distinct { .. }))
    }
}

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
#[allow(clippy::too_many_arguments, clippy::result_large_err)]
pub(crate) fn evaluate_single_transform_windowed(
    record: &Record,
    transform_name: &str,
    evaluator: &mut ProgramEvaluator,
    ctx: &EvalContext,
    plan: &ExecutionPlanDag,
    window_index: usize,
    runtime: &crate::executor::window_runtime::WindowRuntime,
    record_pos: u64,
) -> Result<Vec<TransformOutput>, TransformEvalError> {
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
