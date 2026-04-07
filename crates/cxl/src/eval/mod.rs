pub mod builtins_impl;
pub mod context;
pub mod error;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clinker_record::{GroupByKey, GroupKeyError, Value, value_to_group_key};

use crate::ast::{BinOp, Expr, LiteralValue, Statement, UnaryOp};
use crate::lexer::Span;
use crate::resolve::traits::{FieldResolver, RecordStorage, WindowContext};
use crate::typecheck::pass::TypedProgram;

pub use context::{Clock, EvalContext, FixedClock, WallClock};
pub use error::{EvalError, EvalErrorKind};

/// Row disposition signal from CXL evaluation.
///
/// Returned by `ProgramEvaluator::eval_record()` and consumed by
/// the executor to decide whether to emit, skip, or route a record.
#[derive(Debug)]
pub enum EvalResult {
    /// Record passed all filters and distinct checks — emit to output.
    /// `fields` = output field values, `metadata` = `$meta.*` writes.
    Emit {
        fields: indexmap::IndexMap<String, Value>,
        metadata: indexmap::IndexMap<String, Value>,
    },
    /// Record should be excluded from output.
    Skip(SkipReason),
}

impl EvalResult {
    /// Convenience: unwrap the emitted fields (panics on Skip).
    pub fn into_fields(self) -> indexmap::IndexMap<String, Value> {
        match self {
            EvalResult::Emit { fields, .. } => fields,
            EvalResult::Skip(_) => panic!("called into_fields on Skip"),
        }
    }
}

/// Why a record was skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// A `filter` predicate evaluated to non-true.
    Filtered,
    /// A `distinct` statement found a duplicate value.
    Duplicate,
}

/// Stateful CXL evaluator wrapping a compiled program.
///
/// Owns the `TypedProgram` and optional distinct dedup state. One evaluator
/// per transform per partition/thread — never Clone, use factory construction.
/// Follows DataFusion Accumulator / Polars GroupedReduction pattern.
pub struct ProgramEvaluator {
    typed: Arc<TypedProgram>,
    /// HashSet for distinct dedup. None when no distinct statement present.
    distinct_seen: Option<HashSet<Vec<GroupByKey>>>,
    /// Current partition key for window-scoped distinct.
    current_partition_key: Option<Vec<GroupByKey>>,
    /// Whether this program has any distinct statements.
    has_distinct: bool,
}

impl ProgramEvaluator {
    /// Create a new evaluator for the given program.
    /// `has_distinct` controls whether the distinct HashSet is allocated.
    pub fn new(typed: Arc<TypedProgram>, has_distinct: bool) -> Self {
        Self {
            typed,
            distinct_seen: if has_distinct {
                Some(HashSet::new())
            } else {
                None
            },
            current_partition_key: None,
            has_distinct,
        }
    }

    /// Called before eval_record when transform has windows.
    /// Clears distinct set on partition change.
    pub fn set_partition(&mut self, key: &[GroupByKey]) {
        if self.current_partition_key.as_deref() != Some(key) {
            if let Some(seen) = &mut self.distinct_seen {
                seen.clear();
            }
            self.current_partition_key = Some(key.to_vec());
        }
    }

    /// Reset distinct state (between files or when reusing evaluator).
    pub fn reset_distinct(&mut self) {
        if let Some(seen) = &mut self.distinct_seen {
            seen.clear();
        }
        self.current_partition_key = None;
    }

    /// Whether this program has any distinct statements.
    pub fn has_distinct(&self) -> bool {
        self.has_distinct
    }

    /// Borrow the underlying typed program. Used by the aggregation
    /// engine to evaluate `BindingArg::Expr` arguments through the free
    /// `eval_expr` entry point during the hash aggregator hot loop.
    pub fn typed(&self) -> &Arc<TypedProgram> {
        &self.typed
    }

    /// Evaluate a record through the full program, returning Emit or Skip.
    pub fn eval_record<'w, S: RecordStorage + 'w>(
        &mut self,
        ctx: &EvalContext,
        resolver: &dyn FieldResolver,
        window: Option<&dyn WindowContext<'w, S>>,
    ) -> Result<EvalResult, EvalError> {
        let mut env: HashMap<String, Value> = HashMap::new();
        let mut output = indexmap::IndexMap::new();
        let mut meta_output = indexmap::IndexMap::new();

        for stmt in &self.typed.program.statements {
            match stmt {
                Statement::Filter { predicate, .. } => {
                    let val = eval_expr(
                        predicate,
                        &self.typed,
                        ctx,
                        resolver,
                        window,
                        &env,
                        &meta_output,
                    )?;
                    if val != Value::Bool(true) {
                        return Ok(EvalResult::Skip(SkipReason::Filtered));
                    }
                }
                Statement::Distinct { field, .. } => {
                    let key = self.build_distinct_key(field.as_deref(), &env, resolver)?;
                    let seen = self
                        .distinct_seen
                        .as_mut()
                        .expect("distinct statement requires ProgramEvaluator with distinct state");
                    if !seen.insert(key) {
                        return Ok(EvalResult::Skip(SkipReason::Duplicate));
                    }
                }
                Statement::Let { name, expr, .. } => {
                    let val =
                        eval_expr(expr, &self.typed, ctx, resolver, window, &env, &meta_output)?;
                    env.insert(name.to_string(), val);
                }
                Statement::Emit {
                    name,
                    expr,
                    is_meta,
                    ..
                } => {
                    let val =
                        eval_expr(expr, &self.typed, ctx, resolver, window, &env, &meta_output)?;
                    if *is_meta {
                        meta_output.insert(name.to_string(), val);
                    } else {
                        output.insert(name.to_string(), val);
                    }
                }
                Statement::Trace {
                    level,
                    guard,
                    message,
                    ..
                } => {
                    let should_trace = if let Some(g) = guard {
                        matches!(
                            eval_expr(g, &self.typed, ctx, resolver, window, &env, &meta_output)?,
                            Value::Bool(true)
                        )
                    } else {
                        true
                    };
                    if should_trace {
                        let msg = eval_expr(
                            message,
                            &self.typed,
                            ctx,
                            resolver,
                            window,
                            &env,
                            &meta_output,
                        )?;
                        let msg_str = match &msg {
                            Value::String(s) => s.to_string(),
                            other => format!("{:?}", other),
                        };
                        match level.unwrap_or(crate::ast::TraceLevel::Trace) {
                            crate::ast::TraceLevel::Trace => tracing::trace!(
                                source_row = ctx.source_row,
                                source_file = %ctx.source_file,
                                "{}", msg_str
                            ),
                            crate::ast::TraceLevel::Debug => tracing::debug!(
                                source_row = ctx.source_row,
                                source_file = %ctx.source_file,
                                "{}", msg_str
                            ),
                            crate::ast::TraceLevel::Info => tracing::info!(
                                source_row = ctx.source_row,
                                source_file = %ctx.source_file,
                                "{}", msg_str
                            ),
                            crate::ast::TraceLevel::Warn => tracing::warn!(
                                source_row = ctx.source_row,
                                source_file = %ctx.source_file,
                                "{}", msg_str
                            ),
                            crate::ast::TraceLevel::Error => tracing::error!(
                                source_row = ctx.source_row,
                                source_file = %ctx.source_file,
                                "{}", msg_str
                            ),
                        }
                    }
                }
                Statement::UseStmt { .. } => {}
                Statement::ExprStmt { expr, .. } => {
                    eval_expr(expr, &self.typed, ctx, resolver, window, &env, &meta_output)?;
                }
            }
        }
        Ok(EvalResult::Emit {
            fields: output,
            metadata: meta_output,
        })
    }

    /// Build the distinct key for a record.
    fn build_distinct_key(
        &self,
        field: Option<&str>,
        env: &HashMap<String, Value>,
        resolver: &dyn FieldResolver,
    ) -> Result<Vec<GroupByKey>, EvalError> {
        match field {
            Some(name) => {
                // Resolve from let-bindings first, then input fields
                let val = env
                    .get(name)
                    .cloned()
                    .or_else(|| resolver.resolve(name))
                    .unwrap_or(Value::Null);
                match value_to_group_key(&val, name, None, 0) {
                    Ok(Some(gk)) => Ok(vec![gk]),
                    Ok(None) => Ok(vec![GroupByKey::Null]),
                    Err(e) => Err(group_key_error_to_eval_error(e)),
                }
            }
            None => {
                // Bare distinct — hash all input fields
                let mut key = Vec::new();
                for (name, val) in resolver.iter_fields() {
                    match value_to_group_key(&val, &name, None, 0) {
                        Ok(Some(gk)) => key.push(gk),
                        Ok(None) => key.push(GroupByKey::Null),
                        Err(e) => return Err(group_key_error_to_eval_error(e)),
                    }
                }
                Ok(key)
            }
        }
    }
}

/// Convert a GroupKeyError to an EvalError.
fn group_key_error_to_eval_error(e: GroupKeyError) -> EvalError {
    let got = match &e {
        GroupKeyError::NanInGroupBy { .. } => "NaN",
        GroupKeyError::TypeMismatch { got, .. } => got,
        GroupKeyError::UnsupportedType { type_name, .. } => type_name,
    };
    EvalError::new(
        EvalErrorKind::TypeMismatch {
            expected: "hashable value",
            got,
        },
        Span::new(0, 0),
    )
}

/// Evaluate a full CXL program against a record. Returns the output field map.
pub fn eval_program<'w, S: RecordStorage + 'w>(
    typed: &TypedProgram,
    ctx: &EvalContext,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext<'w, S>>,
) -> Result<indexmap::IndexMap<String, Value>, EvalError> {
    let mut env: HashMap<String, Value> = HashMap::new();
    let mut output = indexmap::IndexMap::new();
    let meta_state = indexmap::IndexMap::new();

    for stmt in &typed.program.statements {
        match stmt {
            Statement::Let { name, expr, .. } => {
                let val = eval_expr(expr, typed, ctx, resolver, window, &env, &meta_state)?;
                env.insert(name.to_string(), val);
            }
            Statement::Emit { name, expr, .. } => {
                let val = eval_expr(expr, typed, ctx, resolver, window, &env, &meta_state)?;
                output.insert(name.to_string(), val);
            }
            Statement::Trace {
                level,
                guard,
                message,
                ..
            } => {
                let should_trace = if let Some(g) = guard {
                    matches!(
                        eval_expr(g, typed, ctx, resolver, window, &env, &meta_state)?,
                        Value::Bool(true)
                    )
                } else {
                    true
                };
                if should_trace {
                    let msg = eval_expr(message, typed, ctx, resolver, window, &env, &meta_state)?;
                    let msg_str = match &msg {
                        Value::String(s) => s.to_string(),
                        other => format!("{:?}", other),
                    };
                    match level.unwrap_or(crate::ast::TraceLevel::Trace) {
                        crate::ast::TraceLevel::Trace => tracing::trace!(
                            source_row = ctx.source_row,
                            source_file = %ctx.source_file,
                            "{}", msg_str
                        ),
                        crate::ast::TraceLevel::Debug => tracing::debug!(
                            source_row = ctx.source_row,
                            source_file = %ctx.source_file,
                            "{}", msg_str
                        ),
                        crate::ast::TraceLevel::Info => tracing::info!(
                            source_row = ctx.source_row,
                            source_file = %ctx.source_file,
                            "{}", msg_str
                        ),
                        crate::ast::TraceLevel::Warn => tracing::warn!(
                            source_row = ctx.source_row,
                            source_file = %ctx.source_file,
                            "{}", msg_str
                        ),
                        crate::ast::TraceLevel::Error => tracing::error!(
                            source_row = ctx.source_row,
                            source_file = %ctx.source_file,
                            "{}", msg_str
                        ),
                    }
                }
            }
            Statement::UseStmt { .. } => {} // Module imports handled at compile time
            Statement::ExprStmt { expr, .. } => {
                eval_expr(expr, typed, ctx, resolver, window, &env, &meta_state)?;
            }
            Statement::Filter { .. } | Statement::Distinct { .. } => {
                // Handled by ProgramEvaluator::eval_record() (Phase 12.2.5+)
                // eval_program() is the legacy path — these statements are no-ops here.
            }
        }
    }

    Ok(output)
}

/// Evaluate a single expression.
pub fn eval_expr<'w, S: RecordStorage + 'w>(
    expr: &Expr,
    typed: &TypedProgram,
    ctx: &EvalContext,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext<'w, S>>,
    env: &HashMap<String, Value>,
    meta_state: &indexmap::IndexMap<String, Value>,
) -> Result<Value, EvalError> {
    match expr {
        Expr::Literal { value, .. } => Ok(literal_to_value(value)),

        Expr::FieldRef { name, .. } => {
            // Check let-bound env first
            if let Some(val) = env.get(&**name) {
                return Ok(val.clone());
            }
            // Then field resolver
            Ok(resolver.resolve(name).unwrap_or(Value::Null))
        }

        Expr::QualifiedFieldRef { parts, .. } => {
            match parts.len() {
                2 => Ok(resolver
                    .resolve_qualified(&parts[0], &parts[1])
                    .unwrap_or(Value::Null)),
                3 => {
                    // Three-part path: source.record_type.field
                    // Join first two parts as the compound source key
                    let compound = format!("{}.{}", &parts[0], &parts[1]);
                    Ok(resolver
                        .resolve_qualified(&compound, &parts[2])
                        .unwrap_or(Value::Null))
                }
                _ => Ok(Value::Null),
            }
        }

        Expr::PipelineAccess { field, .. } => {
            Ok(ctx.resolve_pipeline(field).unwrap_or(Value::Null))
        }

        Expr::MetaAccess { field, .. } => {
            // Check locally-emitted metadata first (same transform), then resolver
            if let Some(val) = meta_state.get(&**field) {
                return Ok(val.clone());
            }
            // Fall back to Record metadata (set by earlier transforms)
            Ok(resolver
                .resolve(&format!("$meta.{field}"))
                .unwrap_or(Value::Null))
        }

        Expr::Now { .. } => Ok(Value::DateTime(ctx.clock.now())),

        Expr::Wildcard { .. } => Ok(Value::Bool(true)), // Wildcard in match = always matches

        Expr::Binary {
            op, lhs, rhs, span, ..
        } => eval_binary(
            *op, lhs, rhs, *span, typed, ctx, resolver, window, env, meta_state,
        ),

        Expr::Unary {
            op, operand, span, ..
        } => {
            let val = eval_expr(operand, typed, ctx, resolver, window, env, meta_state)?;
            match op {
                UnaryOp::Neg => match val {
                    Value::Integer(n) => n
                        .checked_neg()
                        .map(Value::Integer)
                        .ok_or_else(|| EvalError::integer_overflow("negation", *span)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Null),
                },
                UnaryOp::Not => match val {
                    Value::Bool(b) => Ok(Value::Bool(!b)),
                    Value::Null => Ok(Value::Null),
                    _ => Ok(Value::Null),
                },
            }
        }

        Expr::Coalesce { lhs, rhs, .. } => {
            let left = eval_expr(lhs, typed, ctx, resolver, window, env, meta_state)?;
            if left.is_null() {
                eval_expr(rhs, typed, ctx, resolver, window, env, meta_state)
            } else {
                Ok(left) // Short-circuit: RHS not evaluated
            }
        }

        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            let cond = eval_expr(condition, typed, ctx, resolver, window, env, meta_state)?;
            match cond {
                Value::Bool(true) => {
                    eval_expr(then_branch, typed, ctx, resolver, window, env, meta_state)
                }
                _ => {
                    if let Some(eb) = else_branch {
                        eval_expr(eb, typed, ctx, resolver, window, env, meta_state)
                    } else {
                        Ok(Value::Null)
                    }
                }
            }
        }

        Expr::Match { subject, arms, .. } => {
            if let Some(scrutinee) = subject {
                // Value-form match
                let scrutinee_val =
                    eval_expr(scrutinee, typed, ctx, resolver, window, env, meta_state)?;
                for arm in arms {
                    if matches!(arm.pattern, Expr::Wildcard { .. }) {
                        return eval_expr(&arm.body, typed, ctx, resolver, window, env, meta_state);
                    }
                    let pat_val =
                        eval_expr(&arm.pattern, typed, ctx, resolver, window, env, meta_state)?;
                    if values_equal(&scrutinee_val, &pat_val) {
                        return eval_expr(&arm.body, typed, ctx, resolver, window, env, meta_state);
                    }
                }
                Ok(Value::Null)
            } else {
                // Condition-form match
                for arm in arms {
                    if matches!(arm.pattern, Expr::Wildcard { .. }) {
                        return eval_expr(&arm.body, typed, ctx, resolver, window, env, meta_state);
                    }
                    let cond =
                        eval_expr(&arm.pattern, typed, ctx, resolver, window, env, meta_state)?;
                    if matches!(cond, Value::Bool(true)) {
                        return eval_expr(&arm.body, typed, ctx, resolver, window, env, meta_state);
                    }
                }
                Ok(Value::Null)
            }
        }

        Expr::MethodCall {
            node_id,
            receiver,
            method,
            args,
            span,
        } => {
            let recv_val = eval_expr(receiver, typed, ctx, resolver, window, env, meta_state)?;
            let mut arg_vals = Vec::with_capacity(args.len());
            for arg in args {
                arg_vals.push(eval_expr(
                    arg, typed, ctx, resolver, window, env, meta_state,
                )?);
            }

            // Get pre-compiled regex if available
            let regex = typed
                .regexes
                .get(node_id.0 as usize)
                .and_then(|r| r.as_ref());

            match builtins_impl::dispatch_method(&recv_val, method, &arg_vals, regex, *span, ctx)? {
                Some(val) => Ok(val),
                None => Err(EvalError::new(
                    EvalErrorKind::TypeMismatch {
                        expected: "known method",
                        got: "unknown",
                    },
                    *span,
                )),
            }
        }

        Expr::WindowCall {
            function,
            args,
            span,
            ..
        } => {
            let w = window.ok_or_else(|| {
                EvalError::new(
                    EvalErrorKind::TypeMismatch {
                        expected: "window context",
                        got: "none",
                    },
                    *span,
                )
            })?;

            match &**function {
                "count" => Ok(Value::Integer(w.count())),
                "sum" => {
                    if let Some(Expr::FieldRef { name, .. }) = args.first() {
                        Ok(w.sum(name))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "avg" => {
                    if let Some(Expr::FieldRef { name, .. }) = args.first() {
                        Ok(w.avg(name))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "min" => {
                    if let Some(Expr::FieldRef { name, .. }) = args.first() {
                        Ok(w.min(name))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "max" => {
                    if let Some(Expr::FieldRef { name, .. }) = args.first() {
                        Ok(w.max(name))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "first" => {
                    // Positional: returns RecordView. Field access via postfix chain
                    // is handled by the MethodCall evaluator on the receiver.
                    // For now, return Null — full field chain resolution in Task 5.4.
                    Ok(w.first().map(|_r| Value::Null).unwrap_or(Value::Null))
                }
                "last" => Ok(w.last().map(|_r| Value::Null).unwrap_or(Value::Null)),
                "lag" => {
                    let offset = args
                        .first()
                        .map(|a| eval_expr(a, typed, ctx, resolver, window, env, meta_state))
                        .transpose()?
                        .and_then(|v| {
                            if let Value::Integer(n) = v {
                                Some(n as usize)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(1);
                    Ok(w.lag(offset).map(|_r| Value::Null).unwrap_or(Value::Null))
                }
                "lead" => {
                    let offset = args
                        .first()
                        .map(|a| eval_expr(a, typed, ctx, resolver, window, env, meta_state))
                        .transpose()?
                        .and_then(|v| {
                            if let Value::Integer(n) = v {
                                Some(n as usize)
                            } else {
                                None
                            }
                        })
                        .unwrap_or(1);
                    Ok(w.lead(offset).map(|_r| Value::Null).unwrap_or(Value::Null))
                }
                "collect" => {
                    if let Some(Expr::FieldRef { name, .. }) = args.first() {
                        Ok(w.collect(name))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "distinct" => {
                    if let Some(Expr::FieldRef { name, .. }) = args.first() {
                        Ok(w.distinct(name))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "any" | "all" => Ok(Value::Null), // Evaluator-driven: implemented in Task 5.4
                _ => Ok(Value::Null),
            }
        }

        // AggCall is handled by the hash/streaming aggregator, not the row-level
        // evaluator. Reaching here means the typecheck pass failed to reject an
        // aggregate call in a row-level context.
        Expr::AggCall { name, span, .. } => {
            let _ = name;
            Err(EvalError::new(
                EvalErrorKind::TypeMismatch {
                    expected: "row-level expression",
                    got: "aggregate function call",
                },
                *span,
            ))
        }

        // Extractor-produced leaves. Reaching the row-level evaluator means a
        // post-extraction residual was evaluated without an aggregate scope —
        // the aggregate finalize path has its own evaluator (Task 16.3.12).
        Expr::AggSlot { span, .. } => Err(EvalError::new(
            EvalErrorKind::TypeMismatch {
                expected: "row-level expression",
                got: "aggregate slot reference",
            },
            *span,
        )),
        Expr::GroupKey { span, .. } => Err(EvalError::new(
            EvalErrorKind::TypeMismatch {
                expected: "row-level expression",
                got: "group-by key reference",
            },
            *span,
        )),
    }
}

#[allow(clippy::too_many_arguments)]
fn eval_binary<'w, S: RecordStorage + 'w>(
    op: BinOp,
    lhs: &Expr,
    rhs: &Expr,
    span: Span,
    typed: &TypedProgram,
    ctx: &EvalContext,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext<'w, S>>,
    env: &HashMap<String, Value>,
    meta_state: &indexmap::IndexMap<String, Value>,
) -> Result<Value, EvalError> {
    // Three-valued AND/OR: short-circuit before evaluating RHS
    match op {
        BinOp::And => {
            let left = eval_expr(lhs, typed, ctx, resolver, window, env, meta_state)?;
            return match left {
                Value::Bool(false) => Ok(Value::Bool(false)), // false && anything = false
                Value::Bool(true) => eval_expr(rhs, typed, ctx, resolver, window, env, meta_state),
                Value::Null => {
                    let right = eval_expr(rhs, typed, ctx, resolver, window, env, meta_state)?;
                    match right {
                        Value::Bool(false) => Ok(Value::Bool(false)), // null && false = false
                        _ => Ok(Value::Null),
                    }
                }
                _ => Ok(Value::Null),
            };
        }
        BinOp::Or => {
            let left = eval_expr(lhs, typed, ctx, resolver, window, env, meta_state)?;
            return match left {
                Value::Bool(true) => Ok(Value::Bool(true)), // true || anything = true
                Value::Bool(false) => eval_expr(rhs, typed, ctx, resolver, window, env, meta_state),
                Value::Null => {
                    let right = eval_expr(rhs, typed, ctx, resolver, window, env, meta_state)?;
                    match right {
                        Value::Bool(true) => Ok(Value::Bool(true)), // null || true = true
                        _ => Ok(Value::Null),
                    }
                }
                _ => Ok(Value::Null),
            };
        }
        _ => {}
    }

    let left = eval_expr(lhs, typed, ctx, resolver, window, env, meta_state)?;
    let right = eval_expr(rhs, typed, ctx, resolver, window, env, meta_state)?;

    // Equality: never null (per spec)
    match op {
        BinOp::Eq => return Ok(Value::Bool(values_equal(&left, &right))),
        BinOp::Neq => return Ok(Value::Bool(!values_equal(&left, &right))),
        _ => {}
    }

    // Null propagation for everything else
    if left.is_null() || right.is_null() {
        return Ok(Value::Null);
    }

    match op {
        BinOp::Add => eval_add(&left, &right, span),
        BinOp::Sub => eval_arith(
            &left,
            &right,
            span,
            "subtraction",
            |a, b| a.checked_sub(b),
            |a, b| a - b,
        ),
        BinOp::Mul => eval_arith(
            &left,
            &right,
            span,
            "multiplication",
            |a, b| a.checked_mul(b),
            |a, b| a * b,
        ),
        BinOp::Div => {
            // Check division by zero
            match (&left, &right) {
                (_, Value::Integer(0)) => Err(EvalError::division_by_zero(span)),
                (_, Value::Float(f)) if *f == 0.0 => Err(EvalError::division_by_zero(span)),
                _ => eval_arith(
                    &left,
                    &right,
                    span,
                    "division",
                    |a, b| a.checked_div(b),
                    |a, b| a / b,
                ),
            }
        }
        BinOp::Mod => match (&left, &right) {
            (_, Value::Integer(0)) => Err(EvalError::division_by_zero(span)),
            _ => eval_arith(
                &left,
                &right,
                span,
                "modulo",
                |a, b| a.checked_rem(b),
                |a, b| a % b,
            ),
        },
        BinOp::Gt => Ok(Value::Bool(
            compare_values(&left, &right) == Some(std::cmp::Ordering::Greater),
        )),
        BinOp::Lt => Ok(Value::Bool(
            compare_values(&left, &right) == Some(std::cmp::Ordering::Less),
        )),
        BinOp::Gte => Ok(Value::Bool(matches!(
            compare_values(&left, &right),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ))),
        BinOp::Lte => Ok(Value::Bool(matches!(
            compare_values(&left, &right),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ))),
        BinOp::Eq | BinOp::Neq | BinOp::And | BinOp::Or => unreachable!("handled above"),
    }
}

fn eval_add(left: &Value, right: &Value, span: Span) -> Result<Value, EvalError> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => a
            .checked_add(*b)
            .map(Value::Integer)
            .ok_or_else(|| EvalError::integer_overflow("addition", span)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(a + *b as f64)),
        (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b).into())),
        _ => Ok(Value::Null),
    }
}

fn eval_arith(
    left: &Value,
    right: &Value,
    span: Span,
    op_name: &'static str,
    int_op: impl FnOnce(i64, i64) -> Option<i64>,
    float_op: impl FnOnce(f64, f64) -> f64,
) -> Result<Value, EvalError> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => int_op(*a, *b)
            .map(Value::Integer)
            .ok_or_else(|| EvalError::integer_overflow(op_name, span)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(float_op(*a, *b))),
        (Value::Integer(a), Value::Float(b)) => Ok(Value::Float(float_op(*a as f64, *b))),
        (Value::Float(a), Value::Integer(b)) => Ok(Value::Float(float_op(*a, *b as f64))),
        _ => Ok(Value::Null),
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    // Per spec: null == null is true, null == anything_else is false
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x == y,
        (Value::Integer(x), Value::Float(y)) => (*x as f64) == *y,
        (Value::Float(x), Value::Integer(y)) => *x == (*y as f64),
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Date(x), Value::Date(y)) => x == y,
        (Value::DateTime(x), Value::DateTime(y)) => x == y,
        _ => false,
    }
}

fn compare_values(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => Some(x.cmp(y)),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y),
        (Value::Integer(x), Value::Float(y)) => (*x as f64).partial_cmp(y),
        (Value::Float(x), Value::Integer(y)) => x.partial_cmp(&(*y as f64)),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Date(x), Value::Date(y)) => Some(x.cmp(y)),
        (Value::DateTime(x), Value::DateTime(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

fn literal_to_value(lit: &LiteralValue) -> Value {
    match lit {
        LiteralValue::Int(n) => Value::Integer(*n),
        LiteralValue::Float(f) => Value::Float(*f),
        LiteralValue::String(s) => Value::String(s.clone()),
        LiteralValue::Date(d) => Value::Date(*d),
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Null => Value::Null,
    }
}
