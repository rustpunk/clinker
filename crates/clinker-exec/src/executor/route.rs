//! Compiled route node: branch predicates evaluated per record.

use std::sync::Arc;

use clinker_plan::error::PipelineError;
use clinker_record::Record;
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator};
use cxl::typecheck::TypedProgram;

use super::NullStorage;

/// Compiled route branch: a named CXL boolean condition evaluator.
pub(crate) struct CompiledRouteBranch {
    pub(super) name: String,
    pub(super) evaluator: ProgramEvaluator,
}

/// Compiled route configuration for multi-output dispatch.
pub(crate) struct CompiledRoute {
    pub(super) branches: Vec<CompiledRouteBranch>,
    pub(super) default: String,
    pub(super) mode: clinker_plan::config::RouteMode,
}

impl CompiledRoute {
    /// Build the per-record evaluator bundle directly from a `PlanNode::Route`'s
    /// on-node fields.
    ///
    /// The branch programs were typechecked once at bind time and carried on the
    /// node (`PlanNode::Route.branch_programs`), so this only wraps each in a
    /// [`ProgramEvaluator`] — there is no re-parse or re-typecheck of condition
    /// source. `branches` and `branch_programs` are populated together at
    /// lowering and are therefore 1:1 in declaration order; a length mismatch is
    /// a plan-time invariant violation surfaced as [`PipelineError::Internal`].
    pub(crate) fn from_node(
        node_name: &str,
        branches: &[String],
        branch_programs: &[Arc<TypedProgram>],
        default: &str,
        mode: clinker_plan::config::RouteMode,
    ) -> Result<Self, PipelineError> {
        if branches.len() != branch_programs.len() {
            return Err(PipelineError::Internal {
                op: "route::from_node",
                node: node_name.to_string(),
                detail: format!(
                    "route carries {} branch name(s) but {} typed program(s); \
                     lowering must stamp one program per branch",
                    branches.len(),
                    branch_programs.len()
                ),
            });
        }
        let branches = branches
            .iter()
            .zip(branch_programs.iter())
            .map(|(name, program)| CompiledRouteBranch {
                name: name.clone(),
                evaluator: ProgramEvaluator::new(Arc::clone(program), false),
            })
            .collect();
        Ok(CompiledRoute {
            branches,
            default: default.to_string(),
            mode,
        })
    }

    /// Evaluate route conditions against the authoritative Record.
    ///
    /// Returns the list of output names the record should be dispatched to.
    /// In Exclusive mode: first matching branch (or default).
    /// In Inclusive mode: all matching branches (or default if none match).
    ///
    /// Branch predicates resolve field references through the Record's
    /// own [`FieldResolver`] impl — schema + overflow for bare names.
    /// No parallel bookkeeping map is required (Invariant 3).
    pub(crate) fn evaluate(
        &mut self,
        record: &Record,
        ctx: &EvalContext,
    ) -> Result<Vec<String>, cxl::eval::EvalError> {
        match self.mode {
            clinker_plan::config::RouteMode::Exclusive => {
                for branch in &mut self.branches {
                    match branch
                        .evaluator
                        .eval_record::<NullStorage>(ctx, record, None)?
                    {
                        EvalResult::Emit { .. } | EvalResult::EmitMany { .. } => {
                            return Ok(vec![branch.name.clone()]);
                        }
                        EvalResult::Skip(_) => continue,
                    }
                }
                Ok(vec![self.default.clone()])
            }
            clinker_plan::config::RouteMode::Inclusive => {
                let mut matched = Vec::new();
                for branch in &mut self.branches {
                    match branch
                        .evaluator
                        .eval_record::<NullStorage>(ctx, record, None)?
                    {
                        EvalResult::Emit { .. } | EvalResult::EmitMany { .. } => {
                            matched.push(branch.name.clone());
                        }
                        EvalResult::Skip(_) => {}
                    }
                }
                if matched.is_empty() {
                    matched.push(self.default.clone());
                }
                Ok(matched)
            }
        }
    }
}
