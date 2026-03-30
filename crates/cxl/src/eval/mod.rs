pub mod error;
pub mod context;
pub mod builtins_impl;

#[cfg(test)]
mod tests;

use std::collections::HashMap;

use clinker_record::Value;

use crate::ast::{BinOp, Expr, LiteralValue, Statement, UnaryOp};
use crate::lexer::Span;
use crate::resolve::traits::{FieldResolver, WindowContext};
use crate::typecheck::pass::TypedProgram;

pub use error::{EvalError, EvalErrorKind};
pub use context::{Clock, WallClock, FixedClock, EvalContext};

/// Evaluate a full CXL program against a record. Returns the output field map.
pub fn eval_program(
    typed: &TypedProgram,
    ctx: &EvalContext,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext>,
) -> Result<indexmap::IndexMap<String, Value>, EvalError> {
    let mut env: HashMap<String, Value> = HashMap::new();
    let mut output = indexmap::IndexMap::new();

    for stmt in &typed.program.statements {
        match stmt {
            Statement::Let { name, expr, .. } => {
                let val = eval_expr(expr, typed, ctx, resolver, window, &env)?;
                env.insert(name.to_string(), val);
            }
            Statement::Emit { name, expr, .. } => {
                let val = eval_expr(expr, typed, ctx, resolver, window, &env)?;
                output.insert(name.to_string(), val);
            }
            Statement::Trace { guard, message, .. } => {
                let should_trace = if let Some(g) = guard {
                    matches!(eval_expr(g, typed, ctx, resolver, window, &env)?, Value::Bool(true))
                } else {
                    true
                };
                if should_trace {
                    let msg = eval_expr(message, typed, ctx, resolver, window, &env)?;
                    tracing::debug!("trace: {:?}", msg);
                }
            }
            Statement::UseStmt { .. } => {} // Module imports handled at compile time
            Statement::ExprStmt { expr, .. } => {
                eval_expr(expr, typed, ctx, resolver, window, &env)?;
            }
        }
    }

    Ok(output)
}

/// Evaluate a single expression.
pub fn eval_expr(
    expr: &Expr,
    typed: &TypedProgram,
    ctx: &EvalContext,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext>,
    env: &HashMap<String, Value>,
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

        Expr::QualifiedFieldRef { source, field, .. } => {
            Ok(resolver.resolve_qualified(source, field).unwrap_or(Value::Null))
        }

        Expr::PipelineAccess { field, .. } => {
            Ok(ctx.resolve_pipeline(field).unwrap_or(Value::Null))
        }

        Expr::Now { .. } => {
            Ok(Value::DateTime(ctx.clock.now()))
        }

        Expr::Wildcard { .. } => Ok(Value::Bool(true)), // Wildcard in match = always matches

        Expr::Binary { op, lhs, rhs, span, .. } => {
            eval_binary(*op, lhs, rhs, *span, typed, ctx, resolver, window, env)
        }

        Expr::Unary { op, operand, span, .. } => {
            let val = eval_expr(operand, typed, ctx, resolver, window, env)?;
            match op {
                UnaryOp::Neg => match val {
                    Value::Integer(n) => n.checked_neg()
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
            let left = eval_expr(lhs, typed, ctx, resolver, window, env)?;
            if left.is_null() {
                eval_expr(rhs, typed, ctx, resolver, window, env)
            } else {
                Ok(left) // Short-circuit: RHS not evaluated
            }
        }

        Expr::IfThenElse { condition, then_branch, else_branch, .. } => {
            let cond = eval_expr(condition, typed, ctx, resolver, window, env)?;
            match cond {
                Value::Bool(true) => eval_expr(then_branch, typed, ctx, resolver, window, env),
                _ => {
                    if let Some(eb) = else_branch {
                        eval_expr(eb, typed, ctx, resolver, window, env)
                    } else {
                        Ok(Value::Null)
                    }
                }
            }
        }

        Expr::Match { subject, arms, .. } => {
            if let Some(scrutinee) = subject {
                // Value-form match
                let scrutinee_val = eval_expr(scrutinee, typed, ctx, resolver, window, env)?;
                for arm in arms {
                    if matches!(arm.pattern, Expr::Wildcard { .. }) {
                        return eval_expr(&arm.body, typed, ctx, resolver, window, env);
                    }
                    let pat_val = eval_expr(&arm.pattern, typed, ctx, resolver, window, env)?;
                    if values_equal(&scrutinee_val, &pat_val) {
                        return eval_expr(&arm.body, typed, ctx, resolver, window, env);
                    }
                }
                Ok(Value::Null)
            } else {
                // Condition-form match
                for arm in arms {
                    if matches!(arm.pattern, Expr::Wildcard { .. }) {
                        return eval_expr(&arm.body, typed, ctx, resolver, window, env);
                    }
                    let cond = eval_expr(&arm.pattern, typed, ctx, resolver, window, env)?;
                    if matches!(cond, Value::Bool(true)) {
                        return eval_expr(&arm.body, typed, ctx, resolver, window, env);
                    }
                }
                Ok(Value::Null)
            }
        }

        Expr::MethodCall { node_id, receiver, method, args, span } => {
            let recv_val = eval_expr(receiver, typed, ctx, resolver, window, env)?;
            let mut arg_vals = Vec::with_capacity(args.len());
            for arg in args {
                arg_vals.push(eval_expr(arg, typed, ctx, resolver, window, env)?);
            }

            // Get pre-compiled regex if available
            let regex = typed.regexes.get(node_id.0 as usize).and_then(|r| r.as_ref());

            match builtins_impl::dispatch_method(&recv_val, method, &arg_vals, regex, *span)? {
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

        Expr::WindowCall { function, args, span, .. } => {
            let w = window.ok_or_else(|| EvalError::new(
                EvalErrorKind::TypeMismatch { expected: "window context", got: "none" },
                *span,
            ))?;

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
                    Ok(w.first().and_then(|_r| {
                        // Return the resolver as-is; CXL accesses fields via method chain
                        // e.g., window.first().field_name
                        Some(Value::Null) // Simplified: full impl needs field access chain
                    }).unwrap_or(Value::Null))
                }
                "last" | "lag" | "lead" => Ok(Value::Null), // Simplified: full impl in Phase 5
                "any" | "all" => Ok(Value::Null), // Simplified: full impl needs closure evaluation
                _ => Ok(Value::Null),
            }
        }
    }
}

fn eval_binary(
    op: BinOp,
    lhs: &Expr,
    rhs: &Expr,
    span: Span,
    typed: &TypedProgram,
    ctx: &EvalContext,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext>,
    env: &HashMap<String, Value>,
) -> Result<Value, EvalError> {
    // Three-valued AND/OR: short-circuit before evaluating RHS
    match op {
        BinOp::And => {
            let left = eval_expr(lhs, typed, ctx, resolver, window, env)?;
            return match left {
                Value::Bool(false) => Ok(Value::Bool(false)), // false && anything = false
                Value::Bool(true) => eval_expr(rhs, typed, ctx, resolver, window, env),
                Value::Null => {
                    let right = eval_expr(rhs, typed, ctx, resolver, window, env)?;
                    match right {
                        Value::Bool(false) => Ok(Value::Bool(false)), // null && false = false
                        _ => Ok(Value::Null),
                    }
                }
                _ => Ok(Value::Null),
            };
        }
        BinOp::Or => {
            let left = eval_expr(lhs, typed, ctx, resolver, window, env)?;
            return match left {
                Value::Bool(true) => Ok(Value::Bool(true)), // true || anything = true
                Value::Bool(false) => eval_expr(rhs, typed, ctx, resolver, window, env),
                Value::Null => {
                    let right = eval_expr(rhs, typed, ctx, resolver, window, env)?;
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

    let left = eval_expr(lhs, typed, ctx, resolver, window, env)?;
    let right = eval_expr(rhs, typed, ctx, resolver, window, env)?;

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
        BinOp::Sub => eval_arith(&left, &right, span, "subtraction", |a, b| a.checked_sub(b), |a, b| a - b),
        BinOp::Mul => eval_arith(&left, &right, span, "multiplication", |a, b| a.checked_mul(b), |a, b| a * b),
        BinOp::Div => {
            // Check division by zero
            match (&left, &right) {
                (_, Value::Integer(0)) => Err(EvalError::division_by_zero(span)),
                (_, Value::Float(f)) if *f == 0.0 => Err(EvalError::division_by_zero(span)),
                _ => eval_arith(&left, &right, span, "division", |a, b| a.checked_div(b), |a, b| a / b),
            }
        }
        BinOp::Mod => {
            match (&left, &right) {
                (_, Value::Integer(0)) => Err(EvalError::division_by_zero(span)),
                _ => eval_arith(&left, &right, span, "modulo", |a, b| a.checked_rem(b), |a, b| a % b),
            }
        }
        BinOp::Gt => Ok(Value::Bool(compare_values(&left, &right) == Some(std::cmp::Ordering::Greater))),
        BinOp::Lt => Ok(Value::Bool(compare_values(&left, &right) == Some(std::cmp::Ordering::Less))),
        BinOp::Gte => Ok(Value::Bool(matches!(compare_values(&left, &right), Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)))),
        BinOp::Lte => Ok(Value::Bool(matches!(compare_values(&left, &right), Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)))),
        BinOp::Eq | BinOp::Neq | BinOp::And | BinOp::Or => unreachable!("handled above"),
    }
}

fn eval_add(left: &Value, right: &Value, span: Span) -> Result<Value, EvalError> {
    match (left, right) {
        (Value::Integer(a), Value::Integer(b)) => a.checked_add(*b)
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
    left: &Value, right: &Value, span: Span, op_name: &'static str,
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
