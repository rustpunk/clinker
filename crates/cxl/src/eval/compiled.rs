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
//! This slice covers the scalar core plus the record-shaped method
//! surface: literals, field and system access, binary/unary arithmetic
//! and comparison, the three-valued Kleene `and`/`or`, conditionals
//! (`if`/`match`/coalesce), the non-fan-out statement forms (`let`,
//! `emit`, `filter`, `trace`, `use`, expression statements), the
//! record-level builtins reached through `dispatch_method`, and the
//! closure-bearing array builtins (`filter`/`map`/`find`/`any`/
//! `flat_map`). Window access, `distinct`, and `emit each` are not
//! lowered here; their lowering returns an [`EvalError`] rather than a
//! silently-wrong value, so a program reaching one of those nodes fails
//! loudly instead of diverging from the tree-walk.
//!
//! Builtins are not reimplemented: a `MethodCall` node lowers to a call
//! into the same `dispatch_method` the tree-walk uses, so the
//! null-receiver gate (and its four exceptions `is_null` / `type_of` /
//! `is_empty` / `catch`), regex methods, and every string / numeric /
//! date method share one implementation. The pre-compiled regex for a
//! method node is captured by value at lowering from
//! `typed.regexes[node_id]`, never recompiled per record. A closure
//! builtin compiles its closure body to a *separate* compiled
//! sub-program and runs the host loop in Rust, binding the closure
//! parameter through the shared `env` with the same save / insert /
//! eval / remove / restore sequence the tree-walk uses (including the
//! remove on the error path).
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
use regex::Regex;

use crate::ast::{BinOp, EmitTarget, Expr, LiteralValue, Statement, TraceLevel, UnaryOp};
use crate::lexer::Span;
use crate::resolve::traits::{FieldResolver, WindowContext};
use crate::typecheck::pass::TypedProgram;

use super::builtins_impl::dispatch_method;
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

/// Lower a typed program into a [`CompiledProgram`].
///
/// Each statement is lowered exactly once. The typed program is threaded
/// through lowering (not run time) so that node-keyed side data — the
/// pre-compiled regex in `typed.regexes[node_id]` — is captured by value
/// into the method node's closure at compile time, never re-indexed per
/// record. Window access, `distinct`, and `emit each` remain out of
/// scope for this slice: lowering one of those produces a statement /
/// expression closure that returns an [`EvalError`] at run time rather
/// than a silently-wrong value, so a divergence from the tree-walk
/// surfaces as a hard error instead of corrupt output.
pub fn compile<S: RecordStorage + 'static>(typed: &TypedProgram) -> CompiledProgram<S> {
    let statements = typed
        .program
        .statements
        .iter()
        .map(|stmt| compile_stmt(typed, stmt))
        .collect();
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

fn compile_stmt<S: RecordStorage + 'static>(
    typed: &TypedProgram,
    stmt: &Statement,
) -> CompiledStmt<S> {
    match stmt {
        Statement::Let { name, expr, .. } => {
            let name = name.to_string();
            let value = compile_expr(typed, expr);
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
            let value = compile_expr(typed, expr);
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
            let pred = compile_expr(typed, predicate);
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
            let guard = guard.as_ref().map(|g| compile_expr(typed, g));
            let message = compile_expr(typed, message);
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
            let value = compile_expr(typed, expr);
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

fn compile_expr<S: RecordStorage + 'static>(typed: &TypedProgram, expr: &Expr) -> CompiledExpr<S> {
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
        } => compile_binary(typed, *op, lhs, rhs, *span),
        Expr::Unary {
            op, operand, span, ..
        } => {
            let op = *op;
            let span = *span;
            let operand = compile_expr(typed, operand);
            Box::new(move |frame| {
                let val = operand(frame)?;
                apply_unary(op, val, span)
            })
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            let lhs = compile_expr(typed, lhs);
            let rhs = compile_expr(typed, rhs);
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
            let condition = compile_expr(typed, condition);
            let then_branch = compile_expr(typed, then_branch);
            let else_branch = else_branch.as_ref().map(|e| compile_expr(typed, e));
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
        Expr::Match { subject, arms, .. } => compile_match(typed, subject.as_deref(), arms),
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            let receiver = compile_expr(typed, receiver);
            let index = compile_expr(typed, index);
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
        Expr::MethodCall {
            node_id,
            receiver,
            method,
            args,
            span,
        } => compile_method_call(typed, *node_id, receiver, method, args, *span),
        // A free-standing closure is never a value — it is only valid as
        // the argument of a closure-bearing array builtin, where
        // `compile_method_call` consumes it directly rather than routing
        // through here. Reaching this arm means the closure escaped that
        // position; surface the same error the tree-walk does.
        Expr::Closure { span, .. } => {
            let span = *span;
            Box::new(move |_frame| {
                Err(EvalError::new(
                    EvalErrorKind::TypeMismatch {
                        expected: "closure-bearing method argument",
                        got: "free-standing closure",
                    },
                    span,
                ))
            })
        }
        // Deferred to a later slice. These compile to a loud-error leaf
        // so a program reaching one fails at its precise span rather
        // than producing a value that silently diverges from the
        // tree-walk.
        Expr::WindowCall { span, .. } => unsupported_expr("window call", *span),
        Expr::AggCall { span, .. } => unsupported_expr("aggregate call", *span),
        Expr::AggSlot { span, .. } => unsupported_expr("aggregate slot", *span),
        Expr::GroupKey { span, .. } => unsupported_expr("group-by key", *span),
    }
}

/// Lower a method call. Three shapes dispatch here:
///
/// - `$window.<fn>(..).<field>` chains (an `args`-less method on a
///   `first`/`last`/`lag`/`lead` window call) read the window context
///   and are deferred to the window slice — they lower to a loud-error
///   leaf rather than route through `dispatch_method`.
/// - The closure-bearing array builtins (`filter`/`map`/`find`/`any`/
///   `flat_map` whose first argument is a closure) lower to a
///   [`compile_maplike`] host loop over a separately-compiled closure
///   body.
/// - Every other method lowers to an eager evaluation of the receiver
///   and all arguments followed by a single call into the shared
///   `dispatch_method`, which owns the null-receiver gate (and its four
///   exceptions) and the full builtin table. The pre-compiled regex for
///   this node is captured by value here at lowering and handed to
///   `dispatch_method` per record, never recompiled.
fn compile_method_call<S: RecordStorage + 'static>(
    typed: &TypedProgram,
    node_id: crate::ast::NodeId,
    receiver: &Expr,
    method: &str,
    args: &[Expr],
    span: Span,
) -> CompiledExpr<S> {
    // `$window.<fn>(..).<field>` chains resolve a field off a positional
    // window row without a `Value` round-trip; they need the live window
    // borrow and so belong to the window slice. Detect the exact shape
    // the tree-walk intercepts and defer it loudly.
    if args.is_empty()
        && let Expr::WindowCall { function, .. } = receiver
        && matches!(&**function, "first" | "last" | "lag" | "lead")
    {
        return unsupported_expr("window chain access", span);
    }

    // Closure-bearing array builtins: dispatch by name + closure-shaped
    // first argument, mirroring the tree-walk so a string-regex
    // `.find("pattern")` still routes to the regex builtin below.
    if matches!(method, "filter" | "map" | "find" | "any" | "flat_map")
        && matches!(args.first(), Some(Expr::Closure { .. }))
    {
        return compile_maplike(typed, receiver, method, args, span);
    }

    let receiver = compile_expr::<S>(typed, receiver);
    let arg_exprs: Vec<CompiledExpr<S>> =
        args.iter().map(|a| compile_expr::<S>(typed, a)).collect();
    let method: Box<str> = method.into();
    // Capture the node's pre-compiled regex by value once, at lowering.
    // The tree-walk re-indexes `typed.regexes[node_id]` on every record;
    // baking it into the closure makes that a one-time cost.
    let regex: Option<Regex> = typed
        .regexes
        .get(node_id.0 as usize)
        .and_then(|r| r.clone());

    Box::new(move |frame| {
        let recv_val = receiver(frame)?;
        let mut arg_vals = Vec::with_capacity(arg_exprs.len());
        for arg in &arg_exprs {
            arg_vals.push(arg(frame)?);
        }
        match dispatch_method(
            &recv_val,
            &method,
            &arg_vals,
            regex.as_ref(),
            span,
            frame.ctx,
        )? {
            Some(val) => Ok(val),
            None => Err(EvalError::new(
                EvalErrorKind::TypeMismatch {
                    expected: "known method",
                    got: "unknown",
                },
                span,
            )),
        }
    })
}

/// Lower a closure-bearing array builtin (`filter`/`map`/`find`/`any`/
/// `flat_map`) to a host loop in Rust over a separately-compiled closure
/// body.
///
/// The closure body is a distinct [`CompiledExpr`] — not inlined into
/// the receiver's node — so it re-runs per element with the closure
/// parameter freshly bound. Binding goes through the shared `env` with
/// the exact save / insert / eval / remove / restore sequence the
/// tree-walk's `dispatch_closure_method` uses, including the remove on
/// the error path so a failing body never leaves the parameter bound.
/// A null receiver short-circuits to `Null` and the body never runs,
/// matching the tree-walk's null-propagation for these builtins.
fn compile_maplike<S: RecordStorage + 'static>(
    typed: &TypedProgram,
    receiver: &Expr,
    method: &str,
    args: &[Expr],
    span: Span,
) -> CompiledExpr<S> {
    let receiver = compile_expr::<S>(typed, receiver);
    // The closure-shaped first argument is guaranteed by the caller; its
    // param and body compile once here.
    let (param, body): (Box<str>, CompiledExpr<S>) = match args.first() {
        Some(Expr::Closure { param, body, .. }) => (param.clone(), compile_expr::<S>(typed, body)),
        // Unreachable in practice (the caller gates on a closure-shaped
        // first arg), but surface the same shape error the tree-walk
        // raises rather than panic if the invariant is ever violated.
        _ => {
            return Box::new(move |_frame| {
                Err(EvalError::new(
                    EvalErrorKind::TypeMismatch {
                        expected: "closure argument (`it => ...`)",
                        got: "non-closure argument",
                    },
                    span,
                ))
            });
        }
    };
    let kind = MapLikeKind::from_name(method);

    Box::new(move |frame| {
        let recv_val = receiver(frame)?;
        if recv_val.is_null() {
            return Ok(Value::Null);
        }
        let Value::Array(elements) = recv_val else {
            return Ok(Value::Null);
        };

        // Save any shadowed binding so the closure param does not leak a
        // record-scope value, restoring it after the loop.
        let previous = frame.env.remove(param.as_ref());
        let mut output: Vec<Value> = Vec::new();
        let mut found: Option<Value> = None;
        let mut found_any = false;

        for element in &elements {
            frame.env.insert(param.to_string(), element.clone());
            let body_val = body(frame);
            // Always remove the per-iteration binding, then propagate any
            // body error. The shadowed `previous` is restored only after
            // the loop completes — matching the tree-walk, which on the
            // error path leaves the env with the param unbound (the env
            // is per-record scratch, discarded once the error surfaces).
            frame.env.remove(param.as_ref());
            let body_val = body_val?;
            match kind {
                MapLikeKind::Filter => {
                    if matches!(body_val, Value::Bool(true)) {
                        output.push(element.clone());
                    }
                }
                MapLikeKind::Map => output.push(body_val),
                MapLikeKind::Find => {
                    if matches!(body_val, Value::Bool(true)) {
                        found = Some(element.clone());
                        break;
                    }
                }
                MapLikeKind::Any => {
                    if matches!(body_val, Value::Bool(true)) {
                        found_any = true;
                        break;
                    }
                }
                MapLikeKind::FlatMap => match body_val {
                    Value::Array(inner) => output.extend(inner),
                    Value::Null => {}
                    other => output.push(other),
                },
            }
        }

        if let Some(prev) = previous {
            frame.env.insert(param.to_string(), prev);
        }

        Ok(match kind {
            MapLikeKind::Filter | MapLikeKind::Map | MapLikeKind::FlatMap => Value::Array(output),
            MapLikeKind::Find => found.unwrap_or(Value::Null),
            MapLikeKind::Any => Value::Bool(found_any),
        })
    })
}

/// The five closure-bearing array builtins, resolved from the method
/// name once at lowering so the host loop branches on a dense enum
/// rather than re-comparing the method string per element.
#[derive(Clone, Copy)]
enum MapLikeKind {
    Filter,
    Map,
    Find,
    Any,
    FlatMap,
}

impl MapLikeKind {
    fn from_name(method: &str) -> Self {
        match method {
            "filter" => MapLikeKind::Filter,
            "map" => MapLikeKind::Map,
            "find" => MapLikeKind::Find,
            "any" => MapLikeKind::Any,
            "flat_map" => MapLikeKind::FlatMap,
            _ => unreachable!("compile_maplike gated by method name"),
        }
    }
}

/// Lower a binary operator. `and`/`or` become conditional nodes that
/// evaluate the right operand only when the three-valued Kleene table
/// demands it; every other operator evaluates both operands eagerly.
fn compile_binary<S: RecordStorage + 'static>(
    typed: &TypedProgram,
    op: BinOp,
    lhs: &Expr,
    rhs: &Expr,
    span: Span,
) -> CompiledExpr<S> {
    let lhs = compile_expr::<S>(typed, lhs);
    let rhs = compile_expr::<S>(typed, rhs);
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
    typed: &TypedProgram,
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
                    body: compile_expr(typed, &arm.body),
                }
            } else {
                CompiledArm::Guard {
                    pattern: compile_expr(typed, &arm.pattern),
                    body: compile_expr(typed, &arm.body),
                }
            }
        })
        .collect();

    match subject {
        Some(scrutinee) => {
            let scrutinee = compile_expr::<S>(typed, scrutinee);
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
