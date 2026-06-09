//! Compile-once-to-closures evaluator for the CXL scalar core.
//!
//! Where the tree-walk in [`super`] re-matches every `Expr`/`Statement`
//! variant on every record, this module lowers a [`TypedProgram`] a
//! single time into a tree of boxed closures. Each closure captures its
//! already-resolved children and any node-keyed side data (span, baked
//! literal) *by value* at lowering time, so per-record evaluation is a
//! sequence of vtable calls with no central dispatch `match` and no
//! re-indexing of the typed program's side tables.
//!
//! This slice covers the **scalar core** only — literals, field and
//! system access, binary/unary arithmetic and comparison, the
//! three-valued Kleene `and`/`or`, conditionals (`if`/`match`/
//! coalesce), and the statement forms that do not fan out or call
//! methods (`let`, `emit`, `filter`, `trace`, `use`, expression
//! statements). Method calls, closure builtins, window access,
//! `distinct`, and `emit each` are not lowered here; their lowering
//! returns an [`EvalError`] rather than a silently-wrong value, so a
//! program reaching one of those nodes fails loudly instead of
//! diverging from the tree-walk.
//!
//! The produced [`CompiledProgram`] is parameterized by the record
//! storage backend `S` because window reads (a later slice) borrow the
//! arena through `WindowContext<'w, S>`; carrying `S` on the compiled
//! tree keeps that lifetime path honest when it lands. The scalar-core
//! closures never touch the window, but the type threads through so the
//! frame shape does not change when window nodes are added.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{RecordStorage, Value};

use crate::ast::{BinOp, EmitTarget, Expr, LiteralValue, Statement, TraceLevel, UnaryOp};
use crate::lexer::Span;
use crate::resolve::traits::{FieldResolver, WindowContext};
use crate::typecheck::pass::TypedProgram;

use super::context::EvalContext;
use super::error::{EvalError, EvalErrorKind};
use super::{EvalResult, SkipReason};

/// Per-record evaluation frame threaded into every compiled closure.
///
/// Holds the immutable references the scalar core reads (the typed
/// program, the per-record context, the field resolver, the optional
/// window context) plus the mutable `env` scratch map for let-bindings.
/// Mirrors the argument bundle the tree-walk's `eval_expr` threads, so
/// a compiled node's body is a direct transcription of the matching
/// tree-walk arm.
///
/// `typed` and `window` are retained even though the scalar core does
/// not read them: the deferred-node lowering paths (method calls with
/// `typed.regexes`, window access through `window`) will read them when
/// they land, and keeping the fields on the frame avoids reshaping it
/// then.
pub struct Frame<'a, 'w, S: RecordStorage + 'w> {
    #[allow(dead_code)]
    typed: &'a TypedProgram,
    ctx: &'a EvalContext<'a>,
    resolver: &'a dyn FieldResolver,
    #[allow(dead_code)]
    window: Option<&'a dyn WindowContext<'w, S>>,
    env: HashMap<String, Value>,
}

/// A lowered expression node: a closure producing a [`Value`] from a
/// [`Frame`]. Children and node-keyed side data are captured by value
/// at lowering, so the body re-runs per record without re-walking the
/// AST.
type CompiledExpr<S> = Box<dyn for<'a, 'w> Fn(&mut Frame<'a, 'w, S>) -> Result<Value, EvalError>>;

/// Control-flow outcome of running one compiled statement.
enum StmtFlow {
    /// Statement completed; continue to the next one.
    Continue,
    /// Statement short-circuited the whole record (a `filter` predicate
    /// that did not evaluate to `true`). The carried reason matches the
    /// tree-walk's [`SkipReason`].
    Skip(SkipReason),
}

/// Mutable output channels a statement writes into.
///
/// Separated from the read-only [`Frame`] fields so the two-channel
/// shape of [`EvalResult`] (field output vs `$record.*` writes) is
/// explicit, and so `$pipeline.*` / `$source.*` writes route straight
/// through the shared context exactly as the tree-walk does.
struct StmtOut {
    fields: indexmap::IndexMap<String, Value>,
    record_vars: indexmap::IndexMap<String, Value>,
}

/// A lowered statement: a closure that mutates the output channels
/// and/or the frame's env and reports whether the record should
/// continue or be skipped.
type CompiledStmt<S> =
    Box<dyn for<'a, 'w> Fn(&mut Frame<'a, 'w, S>, &mut StmtOut) -> Result<StmtFlow, EvalError>>;

/// A CXL program lowered to a sequence of compiled statements.
///
/// Built once via [`compile`] and run per record via
/// [`Self::eval_record`], which reproduces the tree-walk's
/// `eval_record_inner` control flow: statements run in order, a
/// `filter` short-circuit returns `Skip`, and the accumulated field /
/// record-var channels become an [`EvalResult::Emit`].
pub struct CompiledProgram<S: RecordStorage + 'static> {
    statements: Vec<CompiledStmt<S>>,
}

impl<S: RecordStorage + 'static> CompiledProgram<S> {
    /// Evaluate one record through the compiled program.
    ///
    /// Reproduces `ProgramEvaluator::eval_record_inner` for the scalar
    /// core: a fresh env and output channels per record, in-order
    /// statement execution, `filter` short-circuit to
    /// [`EvalResult::Skip`], and an [`EvalResult::Emit`] carrying the
    /// field and `$record.*` channels. `emit each` fan-out is not
    /// lowered in this slice, so this entry never produces
    /// [`EvalResult::EmitMany`].
    ///
    /// The error boundary (filling `source_row` / `source_expr` on a
    /// surfaced [`EvalError`]) stays with the caller, matching the
    /// tree-walk where `eval_record` wraps `eval_record_inner`.
    pub fn eval_record<'a, 'w>(
        &self,
        typed: &'a TypedProgram,
        ctx: &'a EvalContext<'a>,
        resolver: &'a dyn FieldResolver,
        window: Option<&'a dyn WindowContext<'w, S>>,
    ) -> Result<EvalResult, EvalError> {
        let mut frame = Frame {
            typed,
            ctx,
            resolver,
            window,
            env: HashMap::new(),
        };
        let mut out = StmtOut {
            fields: indexmap::IndexMap::new(),
            record_vars: indexmap::IndexMap::new(),
        };
        for stmt in &self.statements {
            match stmt(&mut frame, &mut out)? {
                StmtFlow::Continue => {}
                StmtFlow::Skip(reason) => return Ok(EvalResult::Skip(reason)),
            }
        }
        Ok(EvalResult::Emit {
            fields: out.fields,
            record_vars: Box::new(out.record_vars),
        })
    }
}

/// Lower a typed program's scalar core into a [`CompiledProgram`].
///
/// Each statement is lowered exactly once. Method calls, closure
/// builtins, window access, `distinct`, and `emit each` are out of
/// scope for this slice: lowering one of those produces a statement /
/// expression closure that returns an [`EvalError`] at run time rather
/// than a silently-wrong value, so a divergence from the tree-walk
/// surfaces as a hard error instead of corrupt output.
pub fn compile<S: RecordStorage + 'static>(typed: &TypedProgram) -> CompiledProgram<S> {
    let statements = typed.program.statements.iter().map(compile_stmt).collect();
    CompiledProgram { statements }
}

/// Construct the closure that signals an unsupported node, anchored on
/// the node's span. Used for every deferred node so reaching one fails
/// loudly with a precise location instead of diverging silently.
fn unsupported_expr<S: RecordStorage + 'static>(what: &'static str, span: Span) -> CompiledExpr<S> {
    Box::new(move |_frame| Err(unsupported_error(what, span)))
}

fn unsupported_error(what: &'static str, span: Span) -> EvalError {
    EvalError::new(
        EvalErrorKind::TypeMismatch {
            expected: "scalar-core node",
            got: what,
        },
        span,
    )
}

fn compile_stmt<S: RecordStorage + 'static>(stmt: &Statement) -> CompiledStmt<S> {
    match stmt {
        Statement::Let { name, expr, .. } => {
            let name = name.to_string();
            let value = compile_expr(expr);
            Box::new(move |frame, _out| {
                let v = value(frame)?;
                frame.env.insert(name.clone(), v);
                Ok(StmtFlow::Continue)
            })
        }
        Statement::Emit {
            name, expr, target, ..
        } => {
            let name: Arc<str> = Arc::from(&**name);
            let target = *target;
            let value = compile_expr(expr);
            Box::new(move |frame, out| {
                // Mirror the tree-walk: stamp the emit's field name onto
                // any surfaced error so DLQ entries name the column under
                // computation.
                let v = value(frame).map_err(|mut e| {
                    if e.triggering_field.is_none() {
                        e.triggering_field = Some(Arc::clone(&name));
                    }
                    e
                })?;
                match target {
                    EmitTarget::Field => {
                        out.fields.insert(name.to_string(), v);
                    }
                    EmitTarget::Pipeline => {
                        frame.ctx.stable.set_pipeline_var(&name, v);
                    }
                    EmitTarget::Source => {
                        frame
                            .ctx
                            .stable
                            .set_source_var(frame.ctx.source_file, &name, v);
                    }
                    EmitTarget::Record => {
                        out.record_vars.insert(name.to_string(), v);
                    }
                }
                Ok(StmtFlow::Continue)
            })
        }
        Statement::Filter { predicate, .. } => {
            let pred = compile_expr(predicate);
            Box::new(move |frame, _out| {
                // A predicate passes only when it is exactly `true`;
                // `null` (and any non-bool) is not true, so it skips.
                if pred(frame)? != Value::Bool(true) {
                    Ok(StmtFlow::Skip(SkipReason::Filtered))
                } else {
                    Ok(StmtFlow::Continue)
                }
            })
        }
        Statement::Trace {
            level,
            guard,
            message,
            ..
        } => {
            let level = level.unwrap_or(TraceLevel::Trace);
            let guard = guard.as_ref().map(|g| compile_expr(g));
            let message = compile_expr(message);
            Box::new(move |frame, _out| {
                let should_trace = match &guard {
                    Some(g) => matches!(g(frame)?, Value::Bool(true)),
                    None => true,
                };
                if should_trace {
                    let msg = message(frame)?;
                    let msg_str = match &msg {
                        Value::String(s) => s.to_string(),
                        other => format!("{:?}", other),
                    };
                    emit_trace(level, frame.ctx, &msg_str);
                }
                Ok(StmtFlow::Continue)
            })
        }
        Statement::UseStmt { .. } => Box::new(|_frame, _out| Ok(StmtFlow::Continue)),
        Statement::ExprStmt { expr, .. } => {
            let value = compile_expr(expr);
            Box::new(move |frame, _out| {
                value(frame)?;
                Ok(StmtFlow::Continue)
            })
        }
        // Deferred to a later slice: `distinct` needs the evaluator's
        // dedup state and `emit each` needs the fan-out accumulator,
        // neither of which the scalar-core frame carries. Fail loudly
        // rather than treat them as no-ops (which would diverge from the
        // tree-walk's actual behavior).
        Statement::Distinct { span, .. } => {
            let span = *span;
            Box::new(move |_frame, _out| Err(unsupported_error("distinct statement", span)))
        }
        Statement::EmitEach { span, .. } => {
            let span = *span;
            Box::new(move |_frame, _out| Err(unsupported_error("emit each statement", span)))
        }
    }
}

fn compile_expr<S: RecordStorage + 'static>(expr: &Expr) -> CompiledExpr<S> {
    match expr {
        Expr::Literal { value, .. } => {
            // Bake the literal's Value once at lowering; cloning a baked
            // Value per record is the same clone the tree-walk pays in
            // `literal_to_value`, but without re-matching the variant.
            let baked = literal_to_value(value);
            Box::new(move |_frame| Ok(baked.clone()))
        }
        Expr::FieldRef { name, .. } => {
            let name = name.to_string();
            Box::new(move |frame| {
                if let Some(v) = frame.env.get(&name) {
                    return Ok(v.clone());
                }
                Ok(frame
                    .resolver
                    .resolve(&name)
                    .cloned()
                    .unwrap_or(Value::Null))
            })
        }
        Expr::QualifiedFieldRef { parts, .. } => {
            let parts: Vec<String> = parts.iter().map(|p| p.to_string()).collect();
            Box::new(move |frame| match parts.len() {
                2 => Ok(frame
                    .resolver
                    .resolve_qualified(&parts[0], &parts[1])
                    .cloned()
                    .unwrap_or(Value::Null)),
                3 => {
                    let compound = format!("{}.{}", &parts[0], &parts[1]);
                    Ok(frame
                        .resolver
                        .resolve_qualified(&compound, &parts[2])
                        .cloned()
                        .unwrap_or(Value::Null))
                }
                _ => Ok(Value::Null),
            })
        }
        Expr::PipelineAccess { field, .. } => {
            let field = field.to_string();
            Box::new(move |frame| Ok(frame.ctx.resolve_pipeline(&field).unwrap_or(Value::Null)))
        }
        Expr::VarsAccess { key, .. } => {
            let key = key.to_string();
            Box::new(move |frame| {
                Ok(frame
                    .ctx
                    .stable
                    .static_vars
                    .get(&key)
                    .cloned()
                    .unwrap_or(Value::Null))
            })
        }
        Expr::SourceAccess { field, .. } => {
            let field = field.to_string();
            Box::new(move |frame| Ok(frame.ctx.resolve_source(&field).unwrap_or(Value::Null)))
        }
        Expr::DocAccess { section, field, .. } => {
            let section = section.to_string();
            let field = field.to_string();
            Box::new(move |frame| {
                Ok(frame
                    .ctx
                    .resolve_doc(&section, &field)
                    .unwrap_or(Value::Null))
            })
        }
        Expr::QualifiedSourceAccess {
            input_name, field, ..
        } => {
            let input_name = input_name.to_string();
            let field = field.to_string();
            Box::new(move |frame| {
                if let Some(arcs) = frame.ctx.stable.source_input_arcs.get(&input_name) {
                    for arc in arcs {
                        if let Some(v) = frame.ctx.stable.resolve_source_var(arc, &field) {
                            return Ok(v);
                        }
                    }
                }
                Ok(Value::Null)
            })
        }
        Expr::RecordAccess { field, .. } => {
            // `$record.<key>` reads route through the resolver under the
            // `$record.` prefix, matching the tree-walk so the dedicated
            // record-vars channel resolves identically.
            let key = format!("$record.{field}");
            Box::new(move |frame| Ok(frame.resolver.resolve(&key).cloned().unwrap_or(Value::Null)))
        }
        Expr::Now { .. } => Box::new(|frame| Ok(Value::DateTime(frame.ctx.stable.clock.now()))),
        Expr::Wildcard { .. } => Box::new(|_frame| Ok(Value::Bool(true))),
        Expr::Binary {
            op, lhs, rhs, span, ..
        } => compile_binary(*op, lhs, rhs, *span),
        Expr::Unary {
            op, operand, span, ..
        } => {
            let op = *op;
            let span = *span;
            let operand = compile_expr(operand);
            Box::new(move |frame| {
                let val = operand(frame)?;
                apply_unary(op, val, span)
            })
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            let lhs = compile_expr(lhs);
            let rhs = compile_expr(rhs);
            Box::new(move |frame| {
                let left = lhs(frame)?;
                if left.is_null() { rhs(frame) } else { Ok(left) }
            })
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            let condition = compile_expr(condition);
            let then_branch = compile_expr(then_branch);
            let else_branch = else_branch.as_ref().map(|e| compile_expr(e));
            Box::new(move |frame| {
                let cond = condition(frame)?;
                if matches!(cond, Value::Bool(true)) {
                    then_branch(frame)
                } else if let Some(eb) = &else_branch {
                    eb(frame)
                } else {
                    Ok(Value::Null)
                }
            })
        }
        Expr::Match { subject, arms, .. } => compile_match(subject.as_deref(), arms),
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            let receiver = compile_expr(receiver);
            let index = compile_expr(index);
            Box::new(move |frame| {
                let recv = receiver(frame)?;
                if recv.is_null() {
                    return Ok(Value::Null);
                }
                let idx_val = index(frame)?;
                Ok(match (&recv, &idx_val) {
                    (Value::Array(arr), Value::Integer(i)) => {
                        if *i < 0 {
                            Value::Null
                        } else {
                            arr.get(*i as usize).cloned().unwrap_or(Value::Null)
                        }
                    }
                    (Value::Map(m), Value::String(key)) => {
                        m.get(&**key).cloned().unwrap_or(Value::Null)
                    }
                    _ => Value::Null,
                })
            })
        }
        // Deferred to a later slice. These compile to a loud-error leaf
        // so a program reaching one fails at its precise span rather
        // than producing a value that silently diverges from the
        // tree-walk.
        Expr::MethodCall { span, .. } => unsupported_expr("method call", *span),
        Expr::WindowCall { span, .. } => unsupported_expr("window call", *span),
        Expr::Closure { span, .. } => unsupported_expr("closure", *span),
        Expr::AggCall { span, .. } => unsupported_expr("aggregate call", *span),
        Expr::AggSlot { span, .. } => unsupported_expr("aggregate slot", *span),
        Expr::GroupKey { span, .. } => unsupported_expr("group-by key", *span),
    }
}

/// Lower a binary operator. `and`/`or` become conditional nodes that
/// evaluate the right operand only when the three-valued Kleene table
/// demands it; every other operator evaluates both operands eagerly.
fn compile_binary<S: RecordStorage + 'static>(
    op: BinOp,
    lhs: &Expr,
    rhs: &Expr,
    span: Span,
) -> CompiledExpr<S> {
    let lhs = compile_expr::<S>(lhs);
    let rhs = compile_expr::<S>(rhs);
    match op {
        // false && _ = false (RHS unread); true && x = x; null && false =
        // false, else null.
        BinOp::And => Box::new(move |frame| {
            let left = lhs(frame)?;
            match left {
                Value::Bool(false) => Ok(Value::Bool(false)),
                Value::Bool(true) => rhs(frame),
                Value::Null => match rhs(frame)? {
                    Value::Bool(false) => Ok(Value::Bool(false)),
                    _ => Ok(Value::Null),
                },
                _ => Ok(Value::Null),
            }
        }),
        // true || _ = true (RHS unread); false || x = x; null || true =
        // true, else null.
        BinOp::Or => Box::new(move |frame| {
            let left = lhs(frame)?;
            match left {
                Value::Bool(true) => Ok(Value::Bool(true)),
                Value::Bool(false) => rhs(frame),
                Value::Null => match rhs(frame)? {
                    Value::Bool(true) => Ok(Value::Bool(true)),
                    _ => Ok(Value::Null),
                },
                _ => Ok(Value::Null),
            }
        }),
        // Equality never nulls (per spec): a null operand compares, it
        // does not poison the result.
        BinOp::Eq => Box::new(move |frame| {
            let l = lhs(frame)?;
            let r = rhs(frame)?;
            Ok(Value::Bool(values_equal(&l, &r)))
        }),
        BinOp::Neq => Box::new(move |frame| {
            let l = lhs(frame)?;
            let r = rhs(frame)?;
            Ok(Value::Bool(!values_equal(&l, &r)))
        }),
        // Every remaining operator null-propagates: a null operand
        // yields null before the operator runs.
        _ => Box::new(move |frame| {
            let left = lhs(frame)?;
            let right = rhs(frame)?;
            if left.is_null() || right.is_null() {
                return Ok(Value::Null);
            }
            apply_arith_or_cmp(op, &left, &right, span)
        }),
    }
}

/// Lower a `match` expression to a guarded chain. The value form
/// compares each arm pattern against the scrutinee with `values_equal`;
/// the condition form runs each arm pattern as a boolean guard. Both
/// fall through to `Null` when no arm matches, matching the tree-walk.
fn compile_match<S: RecordStorage + 'static>(
    subject: Option<&Expr>,
    arms: &[crate::ast::MatchArm],
) -> CompiledExpr<S> {
    enum CompiledArm<S: RecordStorage + 'static> {
        Wildcard {
            body: CompiledExpr<S>,
        },
        Guard {
            pattern: CompiledExpr<S>,
            body: CompiledExpr<S>,
        },
    }

    let compiled_arms: Vec<CompiledArm<S>> = arms
        .iter()
        .map(|arm| {
            if matches!(arm.pattern, Expr::Wildcard { .. }) {
                CompiledArm::Wildcard {
                    body: compile_expr(&arm.body),
                }
            } else {
                CompiledArm::Guard {
                    pattern: compile_expr(&arm.pattern),
                    body: compile_expr(&arm.body),
                }
            }
        })
        .collect();

    match subject {
        Some(scrutinee) => {
            let scrutinee = compile_expr::<S>(scrutinee);
            Box::new(move |frame| {
                let scrutinee_val = scrutinee(frame)?;
                for arm in &compiled_arms {
                    match arm {
                        CompiledArm::Wildcard { body } => return body(frame),
                        CompiledArm::Guard { pattern, body } => {
                            let pat_val = pattern(frame)?;
                            if values_equal(&scrutinee_val, &pat_val) {
                                return body(frame);
                            }
                        }
                    }
                }
                Ok(Value::Null)
            })
        }
        None => Box::new(move |frame| {
            for arm in &compiled_arms {
                match arm {
                    CompiledArm::Wildcard { body } => return body(frame),
                    CompiledArm::Guard { pattern, body } => {
                        if matches!(pattern(frame)?, Value::Bool(true)) {
                            return body(frame);
                        }
                    }
                }
            }
            Ok(Value::Null)
        }),
    }
}

fn apply_unary(op: UnaryOp, val: Value, span: Span) -> Result<Value, EvalError> {
    match op {
        UnaryOp::Neg => match val {
            Value::Integer(n) => n
                .checked_neg()
                .map(Value::Integer)
                .ok_or_else(|| EvalError::integer_overflow("negation", span)),
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

/// Apply an arithmetic or comparison operator to two non-null operands.
/// `and`/`or`/`eq`/`neq` never reach here — they are handled as
/// dedicated nodes in [`compile_binary`].
fn apply_arith_or_cmp(
    op: BinOp,
    left: &Value,
    right: &Value,
    span: Span,
) -> Result<Value, EvalError> {
    match op {
        BinOp::Add => eval_add(left, right, span),
        BinOp::Sub => eval_arith(
            left,
            right,
            span,
            "subtraction",
            i64::checked_sub,
            |a, b| a - b,
        ),
        BinOp::Mul => eval_arith(
            left,
            right,
            span,
            "multiplication",
            i64::checked_mul,
            |a, b| a * b,
        ),
        BinOp::Div => match (left, right) {
            (_, Value::Integer(0)) => Err(EvalError::division_by_zero(span)),
            (_, Value::Float(f)) if *f == 0.0 => Err(EvalError::division_by_zero(span)),
            _ => eval_arith(left, right, span, "division", i64::checked_div, |a, b| {
                a / b
            }),
        },
        BinOp::Mod => match (left, right) {
            (_, Value::Integer(0)) => Err(EvalError::division_by_zero(span)),
            _ => eval_arith(left, right, span, "modulo", i64::checked_rem, |a, b| a % b),
        },
        BinOp::Gt => Ok(Value::Bool(
            compare_values(left, right) == Some(std::cmp::Ordering::Greater),
        )),
        BinOp::Lt => Ok(Value::Bool(
            compare_values(left, right) == Some(std::cmp::Ordering::Less),
        )),
        BinOp::Gte => Ok(Value::Bool(matches!(
            compare_values(left, right),
            Some(std::cmp::Ordering::Greater | std::cmp::Ordering::Equal)
        ))),
        BinOp::Lte => Ok(Value::Bool(matches!(
            compare_values(left, right),
            Some(std::cmp::Ordering::Less | std::cmp::Ordering::Equal)
        ))),
        BinOp::Eq | BinOp::Neq | BinOp::And | BinOp::Or => {
            unreachable!("handled by dedicated compiled nodes")
        }
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
    int_op: impl Fn(i64, i64) -> Option<i64>,
    float_op: impl Fn(f64, f64) -> f64,
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
        LiteralValue::String(s) => Value::String(s.as_ref().into()),
        LiteralValue::Date(d) => Value::Date(*d),
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Null => Value::Null,
    }
}

/// Emit a `trace` line at the resolved level with the record's source
/// provenance attached, matching the tree-walk's tracing call shape.
fn emit_trace(level: TraceLevel, ctx: &EvalContext<'_>, msg: &str) {
    match level {
        TraceLevel::Trace => tracing::trace!(
            source_row = ctx.source_row,
            source_file = %ctx.source_file,
            "{}", msg
        ),
        TraceLevel::Debug => tracing::debug!(
            source_row = ctx.source_row,
            source_file = %ctx.source_file,
            "{}", msg
        ),
        TraceLevel::Info => tracing::info!(
            source_row = ctx.source_row,
            source_file = %ctx.source_file,
            "{}", msg
        ),
        TraceLevel::Warn => tracing::warn!(
            source_row = ctx.source_row,
            source_file = %ctx.source_file,
            "{}", msg
        ),
        TraceLevel::Error => tracing::error!(
            source_row = ctx.source_row,
            source_file = %ctx.source_file,
            "{}", msg
        ),
    }
}

#[cfg(test)]
mod tests;
