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
//! The module lowers the full row-level node vocabulary: literals, field
//! and system access, binary/unary arithmetic and comparison, the
//! three-valued Kleene `and`/`or`, conditionals (`if`/`match`/coalesce),
//! the record-level builtins reached through `dispatch_method`, the
//! closure-bearing array builtins (`filter`/`map`/`find`/`any`/
//! `flat_map`), window reads (aggregate / positional-chain / rank /
//! predicate-fold leaves), and every statement form (`let`, `emit` to all
//! four targets, `filter`, `distinct`, `trace`, `use`, expression
//! statements, and `emit each` fan-out). The only nodes that do not
//! lower are `AggCall` / `AggSlot` / `GroupKey`: an `AggCall` is rewritten
//! away before a `TypedProgram` exists, and `AggSlot` / `GroupKey` live
//! only in the finalize residual (which has its own evaluator), so none
//! has a row node and reaching one is a loud error rather than a silent
//! value.
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
//! Window reads stay leaf loads from the captured
//! `Option<&dyn WindowContext<'w, S>>`, evaluated synchronously while the
//! `'w` borrow is live — never lowered to a value-producing op whose
//! result is stored and re-read. The `$window.<fn>().<field>` chain
//! compiles to a single node that resolves the field off the positional
//! `RecordView` with no intermediate `Value` for the row, preserving the
//! zero-copy lifetime path. `distinct`'s dedup set and the partition key
//! it is scoped to are *not* part of the immutable compiled program: they
//! live in a separate [`EvalState`] the caller threads by `&mut`, so one
//! compiled program can be shared across records and threads.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clinker_record::{GroupByKey, RecordStorage, Value, value_to_group_key};
use regex::Regex;

use crate::ast::{BinOp, EmitTarget, Expr, LiteralValue, Statement, TraceLevel, UnaryOp};
use crate::lexer::Span;
use crate::resolve::traits::{FieldResolver, WindowContext};
use crate::typecheck::pass::TypedProgram;

use super::builtins_impl::dispatch_method;
use super::context::EvalContext;
use super::error::{EvalError, EvalErrorKind};
use super::{EvalResult, SkipReason, group_key_error_to_eval_error};

/// Per-record evaluation frame threaded into every compiled closure.
///
/// Holds the immutable references the scalar core and window leaves read
/// (the per-record context, the field resolver, the optional window
/// context) plus the mutable `env` scratch map for let-bindings. Mirrors
/// the argument bundle the tree-walk's `eval_expr` threads, so a compiled
/// node's body is a direct transcription of the matching tree-walk arm.
///
/// The typed program is not carried: every node-keyed side-table read
/// (the pre-compiled regex) is resolved by value at lowering, so nothing
/// at run time re-indexes the typed program. `window` is read by the
/// window-leaf nodes; it stays an `Option` because a transform without
/// analytic windows evaluates with `None`, exactly as the tree-walk does.
struct Frame<'a, 'w, S: RecordStorage + 'w> {
    ctx: &'a EvalContext<'a>,
    resolver: &'a dyn FieldResolver,
    window: Option<&'a dyn WindowContext<'w, S>>,
    env: HashMap<String, Value>,
}

/// Mutable cross-record evaluation state threaded into `eval_record`.
///
/// The compiled program is immutable (it is shared, potentially behind an
/// `Arc`, across records and threads), so `distinct`'s dedup set and the
/// window-partition key it is scoped to live here rather than on the
/// program. The caller owns one of these per evaluator instance and hands
/// it to `eval_record` by `&mut`, mirroring how the tree-walk keeps
/// `distinct_seen` / `current_partition_key` on `ProgramEvaluator`.
pub(crate) struct EvalState {
    /// Seen distinct keys for the current partition. `None` when the
    /// program has no `distinct` statement, matching the tree-walk's
    /// allocation-gated set.
    distinct_seen: Option<HashSet<Vec<GroupByKey>>>,
    /// The partition key the dedup set is currently scoped to. A change
    /// clears `distinct_seen` (window-scoped distinct), so a new
    /// partition starts deduping from empty.
    current_partition_key: Option<Vec<GroupByKey>>,
    /// Per-record `emit each` fan-out ceiling. When a fan-out body's
    /// running emit count reaches this, the originating record errors
    /// with `ExpansionLimitExceeded` rather than expanding unboundedly.
    max_expansion: u64,
}

impl EvalState {
    /// Build the mutable state for a program. `has_distinct` gates the
    /// dedup-set allocation so a program without `distinct` pays nothing;
    /// `max_expansion` is the `emit each` per-record fan-out ceiling.
    pub(crate) fn new(has_distinct: bool, max_expansion: u64) -> Self {
        Self {
            distinct_seen: has_distinct.then(HashSet::new),
            current_partition_key: None,
            max_expansion,
        }
    }

    /// Make the partition key the dedup set is scoped to `key`, clearing
    /// the set on a change. Mirrors `ProgramEvaluator::set_partition`:
    /// distinct is window-partition-scoped, so crossing a partition
    /// boundary resets dedup rather than carrying it across partitions.
    pub(crate) fn set_partition(&mut self, key: &[GroupByKey]) {
        if self.current_partition_key.as_deref() != Some(key) {
            if let Some(seen) = &mut self.distinct_seen {
                seen.clear();
            }
            self.current_partition_key = Some(key.to_vec());
        }
    }

    /// Clear the dedup set and forget the current partition, returning the
    /// state to the shape it had at construction. Mirrors
    /// `ProgramEvaluator::reset_distinct` for evaluator reuse across files.
    pub(crate) fn reset(&mut self) {
        if let Some(seen) = &mut self.distinct_seen {
            seen.clear();
        }
        self.current_partition_key = None;
    }

    /// The per-record `emit each` fan-out ceiling. The tree-walk reads this
    /// through the shared state so both evaluation paths enforce one
    /// ceiling, set once at construction.
    pub(super) fn max_expansion(&self) -> u64 {
        self.max_expansion
    }

    /// Record `key` as seen for the current partition, returning whether it
    /// was newly inserted (`true`) or a duplicate (`false`). The tree-walk
    /// and the compiled `Distinct` node both dedup through this one set, so
    /// switching paths cannot change which rows a `distinct` keeps.
    ///
    /// Panics if the state was built with `has_distinct = false`; a program
    /// reaching a `distinct` statement is always built with the set
    /// allocated, mirroring the tree-walk's `expect`.
    pub(super) fn distinct_insert(&mut self, key: Vec<GroupByKey>) -> bool {
        self.distinct_seen
            .as_mut()
            .expect("distinct statement requires EvalState allocated with has_distinct = true")
            .insert(key)
    }
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
    /// Statement short-circuited the whole record. The carried reason
    /// distinguishes a `filter` non-true predicate ([`SkipReason::Filtered`])
    /// from a `distinct` duplicate ([`SkipReason::Duplicate`]), matching
    /// the tree-walk's two short-circuit points.
    Skip(SkipReason),
}

/// Mutable per-record run state a statement mutates.
///
/// Bundles the two output channels (field output vs `$record.*` writes —
/// the two-channel shape of [`EvalResult`]), the `emit each` fan-out
/// accumulator, and a `&mut` borrow of the cross-record [`EvalState`]
/// (distinct dedup). `$pipeline.*` / `$source.*` writes route straight
/// through the frame's context, so they never appear here. Threading one
/// struct keeps every compiled-statement closure a uniform two-argument
/// shape regardless of which channel it writes.
struct StmtOut<'s> {
    fields: indexmap::IndexMap<String, Value>,
    record_vars: indexmap::IndexMap<String, Value>,
    /// Records produced by `emit each` blocks this record. Non-empty at
    /// end-of-program promotes the result to [`EvalResult::EmitMany`].
    fan_out: Vec<super::EmitOne>,
    /// Running fan-out cardinality across all `emit each` blocks for this
    /// record, checked against [`EvalState::max_expansion`].
    expansion_count: u64,
    /// Cross-record dedup state, borrowed for the duration of the record.
    state: &'s mut EvalState,
}

/// A lowered statement: a closure that mutates the per-record run state
/// and/or the frame's env and reports whether the record should continue
/// or be skipped.
type CompiledStmt<S> = Box<
    dyn for<'a, 'w, 's> Fn(&mut Frame<'a, 'w, S>, &mut StmtOut<'s>) -> Result<StmtFlow, EvalError>,
>;

/// A CXL program lowered to a sequence of compiled statements.
///
/// Built once via [`compile`] and run per record via
/// [`Self::eval_record`], which reproduces the tree-walk's
/// `eval_record_inner` control flow: statements run in order, a `filter`
/// or `distinct` short-circuit returns `Skip`, an `emit each` block
/// fans out into [`EvalResult::EmitMany`], and otherwise the accumulated
/// field / record-var channels become an [`EvalResult::Emit`].
///
/// The program is immutable; the per-evaluator dedup state lives in a
/// separate [`EvalState`] the caller threads by `&mut`, so one compiled
/// program can be shared across records and threads while each has its
/// own distinct set.
pub(crate) struct CompiledProgram<S: RecordStorage + 'static> {
    statements: Vec<CompiledStmt<S>>,
}

impl<S: RecordStorage + 'static> CompiledProgram<S> {
    /// Evaluate one record through the compiled program.
    ///
    /// Reproduces `ProgramEvaluator::eval_record_inner`: a fresh env and
    /// output channels per record, in-order statement execution, `filter`
    /// / `distinct` short-circuit to [`EvalResult::Skip`], `emit each`
    /// fan-out to [`EvalResult::EmitMany`], and otherwise an
    /// [`EvalResult::Emit`] carrying the field and `$record.*` channels.
    ///
    /// `state` carries the cross-record dedup set; a `distinct` program
    /// must be evaluated with the same `EvalState` across the records of
    /// a partition (and `EvalState::set_partition` called at each
    /// partition boundary) for dedup to be correct, mirroring the
    /// tree-walk's `ProgramEvaluator`.
    ///
    /// The error boundary (filling `source_row` / `source_expr` on a
    /// surfaced [`EvalError`]) stays with the caller, matching the
    /// tree-walk where `eval_record` wraps `eval_record_inner`.
    pub(crate) fn eval_record<'a, 'w>(
        &self,
        ctx: &'a EvalContext<'a>,
        resolver: &'a dyn FieldResolver,
        window: Option<&'a dyn WindowContext<'w, S>>,
        state: &mut EvalState,
    ) -> Result<EvalResult, EvalError> {
        let mut frame = Frame {
            ctx,
            resolver,
            window,
            env: HashMap::new(),
        };
        let mut out = StmtOut {
            fields: indexmap::IndexMap::new(),
            record_vars: indexmap::IndexMap::new(),
            fan_out: Vec::new(),
            expansion_count: 0,
            state,
        };
        for stmt in &self.statements {
            match stmt(&mut frame, &mut out)? {
                StmtFlow::Continue => {}
                StmtFlow::Skip(reason) => return Ok(EvalResult::Skip(reason)),
            }
        }
        if out.fan_out.is_empty() {
            Ok(EvalResult::Emit {
                fields: out.fields,
                record_vars: Box::new(out.record_vars),
            })
        } else {
            Ok(EvalResult::EmitMany {
                records: out.fan_out,
            })
        }
    }
}

/// Lower a typed program into a [`CompiledProgram`].
///
/// Each statement is lowered exactly once. The typed program is threaded
/// through lowering (not run time) so that node-keyed side data — the
/// pre-compiled regex in `typed.regexes[node_id]` — is captured by value
/// into the method node's closure at compile time, never re-indexed per
/// record. The resulting program is immutable; `distinct`'s cross-record
/// dedup state lives in a separate [`EvalState`] the caller threads into
/// [`CompiledProgram::eval_record`].
pub(crate) fn compile<S: RecordStorage + 'static>(typed: &TypedProgram) -> CompiledProgram<S> {
    let statements = typed
        .program
        .statements
        .iter()
        .map(|stmt| compile_stmt(typed, stmt))
        .collect();
    CompiledProgram { statements }
}

/// Whether a typed program contains a top-level `distinct` statement.
///
/// Drives the differential harness's [`EvalState`] dedup-set allocation:
/// it derives `has_distinct` the same way production callers do (a flat
/// scan for a top-level `distinct` — the parser rejects `distinct` inside
/// `emit each`, so the scan is exhaustive) so both evaluators dedup over
/// the same set. Production code threads `has_distinct` from the
/// transform's compiled metadata rather than rescanning, so this helper
/// exists only for the test harness.
#[cfg(test)]
pub fn program_has_distinct(typed: &TypedProgram) -> bool {
    typed
        .program
        .statements
        .iter()
        .any(|s| matches!(s, Statement::Distinct { .. }))
}

/// Reachability error for a node that cannot appear in a compiled
/// program. `AggCall` / `AggSlot` / `GroupKey` are rewritten away by
/// `extract_aggregates` before a `TypedProgram` exists (or live only in
/// the finalize residual, which has its own evaluator), so reaching one
/// here means a malformed program slipped past typecheck; surface a loud
/// typed error at its span rather than a silent value.
fn unreachable_node_error(what: &'static str, span: Span) -> EvalError {
    EvalError::new(
        EvalErrorKind::TypeMismatch {
            expected: "row-level node",
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
        Statement::Distinct { field, .. } => {
            // The dedup key is computed exactly as the tree-walk's
            // `build_distinct_key`: a named `distinct by <field>` keys on
            // that one field (env-bound value preferred over the input
            // field), a bare `distinct` keys on every input field in
            // resolver order. Conversion goes through `value_to_group_key`
            // (never a raw `Value`) so `f64::to_bits` / NaN handling and
            // the `GroupKeyError` surface match the tree-walk bit for bit.
            let field: Option<String> = field.as_ref().map(|f| f.to_string());
            Box::new(move |frame, out| {
                let key = build_distinct_key(field.as_deref(), &frame.env, frame.resolver)?;
                let seen = out.state.distinct_seen.as_mut().expect(
                    "distinct statement requires EvalState allocated with has_distinct = true",
                );
                // Check membership before any emit — `distinct` never
                // reaches a downstream `Emit` for a duplicate.
                if seen.insert(key) {
                    Ok(StmtFlow::Continue)
                } else {
                    Ok(StmtFlow::Skip(SkipReason::Duplicate))
                }
            })
        }
        Statement::EmitEach {
            binding,
            source,
            body,
            span,
            ..
        } => {
            let binding = binding.to_string();
            let source = compile_expr::<S>(typed, source);
            let span = *span;
            // The body is a restricted sub-program: only let / emit /
            // trace / use / expression statements. `filter` / `distinct` /
            // nested `emit each` are rejected at compile time with the
            // same span-anchored error the tree-walk raises at run time,
            // so an ill-formed body fails identically.
            let body: Vec<CompiledStmt<S>> = body
                .iter()
                .map(|s| compile_emit_each_body_stmt(typed, s))
                .collect();
            Box::new(move |frame, out| {
                let source_val = source(frame)?;
                let elements = match source_val {
                    Value::Array(arr) => arr,
                    // A null source fans out zero records — the block is a
                    // no-op, continuing to the next statement.
                    Value::Null => return Ok(StmtFlow::Continue),
                    other => {
                        return Err(EvalError::new(
                            EvalErrorKind::TypeMismatch {
                                expected: "Array",
                                got: other.type_name(),
                            },
                            span,
                        ));
                    }
                };
                for element in elements {
                    // Ceiling check before binding/evaluating the element:
                    // an oversized fan-out errors before unbounded work,
                    // matching the tree-walk's pre-body check.
                    if out.expansion_count >= out.state.max_expansion {
                        return Err(EvalError::new(
                            EvalErrorKind::ExpansionLimitExceeded {
                                limit: out.state.max_expansion,
                            },
                            span,
                        ));
                    }
                    frame.env.insert(binding.clone(), element);
                    // Seed each emitted record with the field / record-var
                    // writes already produced before this block — a
                    // top-level `emit name = ...` preceding the fan-out
                    // applies to every record it produces.
                    let mut iter = StmtOut {
                        fields: out.fields.clone(),
                        record_vars: out.record_vars.clone(),
                        fan_out: Vec::new(),
                        expansion_count: 0,
                        state: &mut *out.state,
                    };
                    for stmt in &body {
                        // A restricted body never skips or fans out, so the
                        // flow result is always `Continue`; an error
                        // propagates with the per-element binding still in
                        // env (per-record scratch, discarded on error).
                        stmt(frame, &mut iter)?;
                    }
                    frame.env.remove(binding.as_str());
                    out.fan_out.push(super::EmitOne {
                        fields: iter.fields,
                        record_vars: Box::new(iter.record_vars),
                    });
                    out.expansion_count += 1;
                }
                Ok(StmtFlow::Continue)
            })
        }
    }
}

/// Compile one statement of an `emit each` body. The body surface is
/// restricted to let / emit / trace / use / expression statements;
/// `filter` / `distinct` / nested `emit each` are rejected here at
/// compile time with the same span-anchored type error the tree-walk
/// raises at run time, so an ill-formed body diverges from neither
/// evaluator. The returned closure shares the same `StmtOut` shape as a
/// top-level statement but only ever reports `Continue`.
///
/// `trace` and `use` are compiled to bare `Continue` no-ops here, *not*
/// routed through `compile_stmt`. The tree-walk's `eval_emit_each_body`
/// matches both as no-ops and never evaluates the trace guard or message,
/// so routing them through the full top-level `Trace` closure — which
/// evaluates both and could surface an error the tree-walk never sees —
/// would diverge.
fn compile_emit_each_body_stmt<S: RecordStorage + 'static>(
    typed: &TypedProgram,
    stmt: &Statement,
) -> CompiledStmt<S> {
    match stmt {
        Statement::Trace { .. } | Statement::UseStmt { .. } => {
            Box::new(|_frame, _out| Ok(StmtFlow::Continue))
        }
        Statement::Let { .. } | Statement::Emit { .. } | Statement::ExprStmt { .. } => {
            compile_stmt(typed, stmt)
        }
        Statement::Filter { span, .. }
        | Statement::Distinct { span, .. }
        | Statement::EmitEach { span, .. } => {
            let span = *span;
            Box::new(move |_frame, _out| {
                Err(EvalError::new(
                    EvalErrorKind::TypeMismatch {
                        expected: "let / emit / trace inside emit each body",
                        got: "filter / distinct / nested emit_each",
                    },
                    span,
                ))
            })
        }
    }
}

/// Build the dedup key for a `distinct` statement, identically to the
/// tree-walk's `ProgramEvaluator::build_distinct_key`.
///
/// A named field resolves from the let-bound env first, then the input
/// record (null when absent); a bare `distinct` keys on every input field
/// in resolver iteration order. Each value goes through
/// `value_to_group_key` so NaN / `f64::to_bits` canonicalization and the
/// `GroupKeyError → EvalError` mapping match the tree-walk.
fn build_distinct_key(
    field: Option<&str>,
    env: &HashMap<String, Value>,
    resolver: &dyn FieldResolver,
) -> Result<Vec<GroupByKey>, EvalError> {
    match field {
        Some(name) => {
            let val: &Value = match env.get(name) {
                Some(v) => v,
                None => resolver.resolve(name).unwrap_or(&clinker_record::NULL),
            };
            match value_to_group_key(val, name, None, 0) {
                Ok(Some(gk)) => Ok(vec![gk]),
                Ok(None) => Ok(vec![GroupByKey::Null]),
                Err(e) => Err(group_key_error_to_eval_error(e)),
            }
        }
        None => {
            let mut key = Vec::new();
            for (name, val) in resolver.iter_fields() {
                match value_to_group_key(val, name, None, 0) {
                    Ok(Some(gk)) => key.push(gk),
                    Ok(None) => key.push(GroupByKey::Null),
                    Err(e) => return Err(group_key_error_to_eval_error(e)),
                }
            }
            Ok(key)
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
        // A bare `$window.<fn>(..)` is a leaf load from the captured
        // window context. Aggregate / positional / rank / predicate-fold
        // builtins all lower here; the postfix `.field` chain form is
        // intercepted one level up in `compile_method_call`.
        Expr::WindowCall {
            function,
            args,
            span,
            ..
        } => compile_window_call(typed, function, args, *span),
        // `AggCall` is rewritten away by `extract_aggregates` before a
        // `TypedProgram` exists, and `AggSlot` / `GroupKey` live only in
        // the finalize residual (which has its own evaluator) — none has a
        // row node. Reaching one means a malformed program slipped past
        // typecheck; surface a loud error at its span, matching the
        // tree-walk's row-level rejection.
        Expr::AggCall { span, .. } => {
            let span = *span;
            Box::new(move |_frame| Err(unreachable_node_error("aggregate function call", span)))
        }
        Expr::AggSlot { span, .. } => {
            let span = *span;
            Box::new(move |_frame| Err(unreachable_node_error("aggregate slot reference", span)))
        }
        Expr::GroupKey { span, .. } => {
            let span = *span;
            Box::new(move |_frame| Err(unreachable_node_error("group-by key reference", span)))
        }
    }
}

/// Lower a method call. Three shapes dispatch here:
///
/// - `$window.<fn>(..).<field>` chains (an `args`-less method on a
///   `first`/`last`/`lag`/`lead` window call) lower to a single
///   [`compile_window_chain_field`] leaf that reads the positional window
///   row and resolves `<field>` off it directly — no intermediate
///   `Value` for the row, preserving the zero-copy lifetime path.
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
    // window row without a `Value` round-trip. Detect the exact shape the
    // tree-walk intercepts and lower it to a single window-leaf node.
    if args.is_empty()
        && let Expr::WindowCall {
            function,
            args: window_args,
            ..
        } = receiver
        && matches!(&**function, "first" | "last" | "lag" | "lead")
    {
        return compile_window_chain_field(typed, function, window_args, method, span);
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

/// Lower a `$window.<fn>(..).<field>` chain to a single window-leaf node.
///
/// The positional window function (`first`/`last`/`lag`/`lead`) returns a
/// `RecordView` borrowing the arena; the chain resolves `<field>` off that
/// view directly, with no intermediate `Value` for the row (a `Value`
/// cannot carry the arena borrow). `lag`/`lead` take an integer offset
/// argument — non-integer or absent defaults to `1`, matching the
/// tree-walk. A missing window context surfaces the same typed error the
/// tree-walk raises. The result is the resolved field value, or `Null`
/// when the positional row is out of range or the field is absent.
fn compile_window_chain_field<S: RecordStorage + 'static>(
    typed: &TypedProgram,
    function: &str,
    window_args: &[Expr],
    field: &str,
    span: Span,
) -> CompiledExpr<S> {
    let function: Box<str> = function.into();
    let field: Box<str> = field.into();
    // `lag`/`lead` read their offset from the first window arg; compile it
    // once so the per-record path only evaluates, never re-walks.
    let offset_arg: Option<CompiledExpr<S>> =
        window_args.first().map(|a| compile_expr::<S>(typed, a));
    Box::new(move |frame| {
        let w = frame.window.ok_or_else(|| {
            EvalError::new(
                EvalErrorKind::TypeMismatch {
                    expected: "window context",
                    got: "none",
                },
                span,
            )
        })?;
        let view = match &*function {
            "first" => w.first(),
            "last" => w.last(),
            "lag" | "lead" => {
                let offset = match &offset_arg {
                    Some(arg) => match arg(frame)? {
                        Value::Integer(n) => n.max(0) as usize,
                        _ => 1,
                    },
                    None => 1,
                };
                if &*function == "lag" {
                    w.lag(offset)
                } else {
                    w.lead(offset)
                }
            }
            _ => unreachable!("compile_window_chain_field gated by function name"),
        };
        // Resolve the field directly off the borrowed view — no `Value`
        // round-trip for the row.
        Ok(view
            .and_then(|row| row.resolve(&field).cloned())
            .unwrap_or(Value::Null))
    })
}

/// Lower a bare `$window.<fn>(..)` call to a leaf load from the captured
/// window context. Mirrors the tree-walk's `WindowCall` arm exactly:
///
/// - Aggregate builtins (`sum`/`avg`/`min`/`max`/`cumulative_sum`/
///   `collect`/`distinct`) read the field name from a `FieldRef` first
///   argument (anything else → `Null`).
/// - Cardinality / rank builtins (`count`/`row_number`/`rank`/
///   `dense_rank`) take no field.
/// - `first_value`/`last_value` read a field name like the aggregates.
/// - Positional builtins (`first`/`last`/`lag`/`lead`) without a `.field`
///   chain return `Null` — the meaningful form is the chain, handled by
///   [`compile_window_chain_field`].
/// - Predicate folds (`any`/`every`/`exists`/`not_exists`) run a compiled
///   predicate sub-program over each partition record with the same
///   three-valued Kleene collapse the tree-walk uses.
///
/// A missing window context surfaces the same typed error as the
/// tree-walk; an unimplemented registered name is a loud error rather
/// than a silent `Null`.
fn compile_window_call<S: RecordStorage + 'static>(
    typed: &TypedProgram,
    function: &str,
    args: &[Expr],
    span: Span,
) -> CompiledExpr<S> {
    /// The field name of a `FieldRef` first argument, captured at
    /// lowering. Aggregate window builtins key on this; a non-`FieldRef`
    /// argument resolves to `None` (→ `Null` at run time), matching the
    /// tree-walk's `if let Some(FieldRef) = args.first()` guard.
    fn field_arg(args: &[Expr]) -> Option<Box<str>> {
        match args.first() {
            Some(Expr::FieldRef { name, .. }) => Some(name.clone()),
            _ => None,
        }
    }

    let function: Box<str> = function.into();
    let field = field_arg(args);

    // Predicate folds compile their predicate sub-program once and run it
    // per partition record; everything else is a direct trait call.
    if matches!(&*function, "any" | "every" | "exists" | "not_exists") {
        let predicate = args.first().map(|p| compile_expr::<S>(typed, p));
        let invert = &*function == "not_exists";
        let want_any = matches!(&*function, "any" | "exists");
        return Box::new(move |frame| {
            let w = frame.window.ok_or_else(|| window_missing_error(span))?;
            let predicate = predicate.as_ref().ok_or_else(|| {
                EvalError::new(
                    EvalErrorKind::ArityMismatch {
                        name: function.to_string(),
                        expected: 1,
                        got: 0,
                    },
                    span,
                )
            })?;
            window_predicate_fold(frame, w, predicate, invert, want_any)
        });
    }

    Box::new(move |frame| {
        let w = frame.window.ok_or_else(|| window_missing_error(span))?;
        let agg = |f: &dyn Fn(&str) -> Value| match &field {
            Some(name) => f(name),
            None => Value::Null,
        };
        match &*function {
            "count" => Ok(Value::Integer(w.count())),
            "sum" => Ok(agg(&|n| w.sum(n))),
            "cumulative_sum" => Ok(agg(&|n| w.cumulative_sum(n))),
            "avg" => Ok(agg(&|n| w.avg(n))),
            "min" => Ok(agg(&|n| w.min(n))),
            "max" => Ok(agg(&|n| w.max(n))),
            // A bare positional builtin (no `.field`) is a row reference
            // `Value` cannot carry; the chain form is intercepted upstream.
            "first" | "last" | "lag" | "lead" => Ok(Value::Null),
            "collect" => Ok(agg(&|n| w.collect(n))),
            "distinct" => Ok(agg(&|n| w.distinct(n))),
            "row_number" => Ok(Value::Integer(w.row_number())),
            "rank" => Ok(Value::Integer(w.rank())),
            "dense_rank" => Ok(Value::Integer(w.dense_rank())),
            "first_value" => Ok(agg(&|n| w.first_value(n))),
            "last_value" => Ok(agg(&|n| w.last_value(n))),
            // A registered name with no evaluator arm is a hard error, not
            // a silent Null — matching the tree-walk's catch-all.
            _ => Err(EvalError::new(
                EvalErrorKind::TypeMismatch {
                    expected: "registered $window function",
                    got: "unimplemented evaluator arm",
                },
                span,
            )),
        }
    })
}

/// The "no window context available" error, shared by the window-leaf
/// nodes so the message and span match the tree-walk's.
fn window_missing_error(span: Span) -> EvalError {
    EvalError::new(
        EvalErrorKind::TypeMismatch {
            expected: "window context",
            got: "none",
        },
        span,
    )
}

/// Run a `$window` predicate fold (`any`/`every`/`exists`/`not_exists`)
/// over every record in the partition, collapsing the per-row predicate
/// results with the same three-valued Kleene logic as `BinOp::And/Or`.
///
/// The predicate is evaluated against each partition record as the field
/// resolver, sharing the outer frame's env and window — exactly the
/// tree-walk's `eval_expr(predicate, .., &row, window, env)` shape. A
/// `true` short-circuits `any`/`exists`; a `false` short-circuits
/// `every`/`not_exists`; any null / non-bool row result taints the fold
/// to `Null` unless a short-circuit already fired. With no taint the
/// result is the iterated-operator identity (`false` for any/exists,
/// `true` for every/not_exists).
fn window_predicate_fold<'a, 'w, S: RecordStorage + 'w>(
    frame: &mut Frame<'a, 'w, S>,
    w: &dyn WindowContext<'w, S>,
    predicate: &CompiledExpr<S>,
    invert: bool,
    want_any: bool,
) -> Result<Value, EvalError> {
    // Move env out of the outer frame so each partition-record sub-frame
    // can share and mutate it, then restore it after the fold — the env is
    // threaded by value through `Frame`, so this mirrors the tree-walk's
    // single shared `&mut env` without a per-row clone.
    let mut env = std::mem::take(&mut frame.env);
    let ctx = frame.ctx;
    let window = frame.window;
    let mut found_null = false;
    let mut short_circuit: Option<Value> = None;
    for i in 0..w.partition_len() {
        let row = w.partition_record(i);
        let mut sub = Frame {
            ctx,
            resolver: &row,
            window,
            env,
        };
        let raw = predicate(&mut sub);
        env = std::mem::take(&mut sub.env);
        let raw = match raw {
            Ok(v) => v,
            Err(e) => {
                frame.env = env;
                return Err(e);
            }
        };
        let v = match (invert, raw) {
            (true, Value::Bool(b)) => Value::Bool(!b),
            (_, other) => other,
        };
        match v {
            Value::Bool(true) if want_any => {
                short_circuit = Some(Value::Bool(true));
                break;
            }
            Value::Bool(false) if !want_any => {
                short_circuit = Some(Value::Bool(false));
                break;
            }
            Value::Bool(_) => {}
            _ => found_null = true,
        }
    }
    frame.env = env;
    Ok(match short_circuit {
        Some(v) => v,
        None if found_null => Value::Null,
        None => Value::Bool(!want_any),
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
