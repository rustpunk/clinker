//! Compiled route node: branch predicates evaluated per record.

use clinker_record::Record;
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator};

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
    pub(super) mode: crate::config::RouteMode,
}

impl CompiledRoute {
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
            crate::config::RouteMode::Exclusive => {
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
            crate::config::RouteMode::Inclusive => {
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
