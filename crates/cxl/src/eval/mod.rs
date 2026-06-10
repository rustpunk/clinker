pub mod builtins_impl;
pub mod context;
pub mod error;

// Compile-once-to-closures evaluator. `ProgramEvaluator` compiles a
// program into a `CompiledProgram` and can run either this path or the
// tree-walk per record, selected by a runtime flag. Both paths coexist
// only while the compiled path is being proven byte-identical to the
// tree-walk; once it is the sole evaluator the tree-walk and the flag go
// away, leaving this module the only scalar core.
pub(crate) mod compiled;

#[cfg(test)]
mod tests;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{GroupByKey, GroupKeyError, Value, value_to_group_key};

use crate::ast::{BinOp, EmitTarget, Expr, LiteralValue, Statement, UnaryOp};
use crate::lexer::Span;
use crate::resolve::traits::{FieldResolver, RecordStorage, WindowContext};
use crate::typecheck::pass::TypedProgram;

use compiled::{CompiledProgram, EvalState};

pub use context::{Clock, EvalContext, FixedClock, StableEvalContext, WallClock};
pub use error::{EvalError, EvalErrorKind, extract_triggering_value};

/// Default per-record fan-out ceiling for `emit each` blocks when the
/// transform's YAML config does not set one explicitly. Matches the
/// default in `clinker_plan::config::pipeline_node::TransformBody`.
pub const DEFAULT_MAX_EXPANSION: u64 = 10_000;

/// Row disposition signal from CXL evaluation.
///
/// Returned by `ProgramEvaluator::eval_record()` and consumed by
/// the executor to decide whether to emit, skip, or route a record.
#[derive(Debug)]
pub enum EvalResult {
    /// Record passed all filters and distinct checks — emit to output.
    /// `fields` = output field values; `record_vars` = `$record.<key>`
    /// writes the caller applies to the record's record_vars channel
    /// (`$pipeline.*` and `$source.*` writes go directly through
    /// `StableEvalContext` during eval and are not surfaced here).
    /// `record_vars` is boxed to keep the enum's stack footprint
    /// under clippy's `large_enum_variant` threshold; the typical
    /// record path with no `$record.<key>` writes still produces an
    /// empty boxed map.
    Emit {
        fields: indexmap::IndexMap<String, Value>,
        record_vars: Box<indexmap::IndexMap<String, Value>>,
    },
    /// Multiple records emitted from a single input via `emit each`
    /// fan-out. Each record carries its own field map and record-var
    /// writes, mirroring `Emit`'s per-record shape. Consumers iterate
    /// over `records` and push each onto their output buffer.
    EmitMany { records: Vec<EmitOne> },
    /// Record should be excluded from output.
    Skip(SkipReason),
}

/// One emitted record produced by an `emit each` body iteration.
/// Carries the same field map / record-var slot shape as a plain
/// `EvalResult::Emit`. Boxed inside `EmitMany` to keep that variant's
/// stack size small while still allowing the fan-out path to gather
/// many records before the consumer unpacks.
#[derive(Debug)]
pub struct EmitOne {
    pub fields: indexmap::IndexMap<String, Value>,
    pub record_vars: Box<indexmap::IndexMap<String, Value>>,
}

impl EvalResult {
    /// Convenience: unwrap the emitted fields (panics on Skip / EmitMany).
    ///
    /// # Panics
    ///
    /// Panics on [`EvalResult::Skip`] or [`EvalResult::EmitMany`].
    /// `EmitMany` callers must unpack the multi-record fan-out before
    /// reducing to a single field map.
    pub fn into_fields(self) -> indexmap::IndexMap<String, Value> {
        match self {
            EvalResult::Emit { fields, .. } => fields,
            EvalResult::EmitMany { .. } => {
                panic!("called into_fields on EmitMany; unpack the records first")
            }
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
/// Bundle of the immutable references the evaluator threads through
/// every recursive descent: the typed program (for node-id-keyed type
/// and regex tables), the per-record `EvalContext` (source/row/vars),
/// the field resolver (record-field lookup), and the optional window
/// context (analytic-window-scoped reads). The mutable `env`
/// (let-binding scratch) stays separate so the mutation contract is
/// explicit at every call site.
#[derive(Copy, Clone)]
struct EvalEnv<'a, 'w, S: RecordStorage + 'w> {
    typed: &'a TypedProgram,
    ctx: &'a EvalContext<'a>,
    resolver: &'a dyn FieldResolver,
    window: Option<&'a dyn WindowContext<'w, S>>,
}

/// Owns the `TypedProgram` and optional distinct dedup state. One evaluator
/// per transform per partition/thread — never Clone, use factory construction.
/// Follows DataFusion Accumulator / Polars GroupedReduction pattern.
pub struct ProgramEvaluator {
    typed: Arc<TypedProgram>,
    /// Distinct dedup set, partition key, and `emit each` ceiling, shared
    /// by both evaluation paths. Threading one state object means
    /// `set_partition` and per-record dedup behave identically whether a
    /// record runs through the tree-walk or the compiled program.
    state: EvalState,
    /// Whether this program has any distinct statements. Gates the dedup
    /// set's allocation inside `state`.
    has_distinct: bool,
    /// Selects the per-record evaluator: `true` runs the compiled program,
    /// `false` runs the tree-walk. Transitional — both evaluators are kept
    /// only while the compiled path is being proven byte-identical to the
    /// tree-walk; the tree-walk (and this flag) are removed once the
    /// compiled path is the sole evaluator. Default `false` keeps every
    /// existing caller on the tree-walk until the cutover.
    use_compiled: bool,
    /// Compiled programs, one per record-storage type the evaluator is
    /// called with. `eval_record` is generic over `S` and a single
    /// evaluator instance is called with several `S` (e.g. `NullStorage`
    /// for non-windowed records, the window arena's storage for windowed
    /// ones), so the compiled program — whose window leaves are
    /// monomorphized over `S` — is built lazily on first use of each `S`
    /// and cached by its `TypeId`. The boxed value is a `CompiledProgram<S>`
    /// downcast back to the concrete type at the call site.
    compiled_cache: HashMap<TypeId, Box<dyn Any>>,
}

impl ProgramEvaluator {
    /// Create a new evaluator for the given program.
    /// `has_distinct` controls whether the distinct HashSet is allocated.
    pub fn new(typed: Arc<TypedProgram>, has_distinct: bool) -> Self {
        Self::with_max_expansion(typed, has_distinct, DEFAULT_MAX_EXPANSION)
    }

    /// Create a new evaluator with an explicit `max_expansion` ceiling.
    /// Callers that thread the transform's YAML-configured cap (typically
    /// `clinker-plan::config::pipeline_node::TransformBody::max_expansion`)
    /// use this constructor; tests and code paths that never use
    /// `emit each` may keep the default via [`Self::new`].
    pub fn with_max_expansion(
        typed: Arc<TypedProgram>,
        has_distinct: bool,
        max_expansion: u64,
    ) -> Self {
        Self {
            typed,
            state: EvalState::new(has_distinct, max_expansion),
            has_distinct,
            use_compiled: false,
            compiled_cache: HashMap::new(),
        }
    }

    /// Switch this evaluator between the tree-walk and the compiled
    /// program. Transitional: the A/B switch the differential test and the
    /// bench parity gate flip, removed with the tree-walk once the compiled
    /// path is the sole evaluator.
    pub fn set_use_compiled(&mut self, use_compiled: bool) {
        self.use_compiled = use_compiled;
    }

    /// Called before eval_record when transform has windows.
    /// Clears distinct set on partition change.
    pub fn set_partition(&mut self, key: &[GroupByKey]) {
        self.state.set_partition(key);
    }

    /// Reset distinct state (between files or when reusing evaluator).
    pub fn reset_distinct(&mut self) {
        self.state.reset();
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
    ///
    /// Wraps the inner walk in a boundary `map_err` that attaches the
    /// current `source_row` and the program's `source` text to any
    /// [`EvalError`] surfaced from a subexpression, so DLQ entries and
    /// miette diagnostics downstream carry "row N: ..." context and an
    /// underlined source span without every internal constructor having
    /// to thread those fields manually. The boundary is identical whether
    /// the record runs through the compiled program or the tree-walk, so a
    /// surfaced error carries the same provenance through either path.
    ///
    /// `S` is `'static` because the compiled path caches one
    /// `CompiledProgram<S>` per storage type keyed by `TypeId`; every real
    /// storage (`NullStorage`, the window arena) is already `'static`.
    pub fn eval_record<'w, S: RecordStorage + 'static>(
        &mut self,
        ctx: &EvalContext<'_>,
        resolver: &dyn FieldResolver,
        window: Option<&dyn WindowContext<'w, S>>,
    ) -> Result<EvalResult, EvalError> {
        let source_row = ctx.source_row;
        let source_expr = self.typed.source.clone();
        let inner = if self.use_compiled {
            self.eval_record_compiled(ctx, resolver, window)
        } else {
            self.eval_record_inner(ctx, resolver, window)
        };
        inner.map_err(move |mut e| {
            if e.source_row.is_none() {
                e.source_row = Some(source_row);
            }
            if e.source_expr.is_none()
                && let Some(s) = source_expr
            {
                e.source_expr = Some(s);
            }
            e
        })
    }

    /// Run one record through the compiled program for storage `S`,
    /// building and caching that program on first use of each `S`.
    ///
    /// The cross-record dedup state lives in `self.state`, the same object
    /// the tree-walk and `set_partition` use, so a `distinct` program
    /// dedups identically through either path. No boundary stamping here —
    /// the shared boundary in `eval_record` wraps this call, matching the
    /// tree-walk's `eval_record_inner`.
    fn eval_record_compiled<'w, S: RecordStorage + 'static>(
        &mut self,
        ctx: &EvalContext<'_>,
        resolver: &dyn FieldResolver,
        window: Option<&dyn WindowContext<'w, S>>,
    ) -> Result<EvalResult, EvalError> {
        let program = self
            .compiled_cache
            .entry(TypeId::of::<S>())
            .or_insert_with(|| Box::new(compiled::compile::<S>(&self.typed)))
            .downcast_ref::<CompiledProgram<S>>()
            .expect("compiled cache keyed by TypeId<S> always holds a CompiledProgram<S>");
        let ctx_ref: &EvalContext<'_> = ctx;
        program.eval_record(ctx_ref, resolver, window, &mut self.state)
    }

    fn eval_record_inner<'w, S: RecordStorage + 'w>(
        &mut self,
        ctx: &EvalContext<'_>,
        resolver: &dyn FieldResolver,
        window: Option<&dyn WindowContext<'w, S>>,
    ) -> Result<EvalResult, EvalError> {
        let mut env: HashMap<String, Value> = HashMap::new();
        let mut output = indexmap::IndexMap::new();
        let mut record_var_writes: indexmap::IndexMap<String, Value> = indexmap::IndexMap::new();
        // Fan-out accumulator: populated by `emit each` blocks. When
        // non-empty at end-of-program, the evaluator returns
        // `EvalResult::EmitMany` with these records (plus any
        // trailing top-level `emit name = ...` writes folded into
        // each emitted record's `fields` map). `expansion_count`
        // tracks the running fan-out cardinality so the per-record
        // `max_expansion` ceiling can route oversized expansions to
        // DLQ before unbounded work happens.
        let mut fan_out: Vec<EmitOne> = Vec::new();
        let mut expansion_count: u64 = 0;
        // Read the fan-out ceiling once: it is fixed for the evaluator's
        // lifetime, and the shared `state` it lives on is borrowed mutably
        // by the distinct arm, so capturing it up front keeps the borrow
        // local to the one statement that needs it.
        let max_expansion = self.state.max_expansion();

        // Index-loop over statements rather than `for stmt in &...` —
        // the `emit each` arm needs `&mut self` to call its body
        // walker, which conflicts with the immutable iterator borrow.
        // Cloning the Arc<TypedProgram> bumps a refcount once but lets
        // each arm reborrow from the cloned handle without contending
        // for the inner borrow on `self.typed`.
        let typed = Arc::clone(&self.typed);
        for stmt in &typed.program.statements {
            match stmt {
                Statement::Filter { predicate, .. } => {
                    let val = eval_expr(predicate, &self.typed, ctx, resolver, window, &mut env)?;
                    if val != Value::Bool(true) {
                        return Ok(EvalResult::Skip(SkipReason::Filtered));
                    }
                }
                Statement::Distinct { field, .. } => {
                    let key = self.build_distinct_key(field.as_deref(), &env, resolver)?;
                    if !self.state.distinct_insert(key) {
                        return Ok(EvalResult::Skip(SkipReason::Duplicate));
                    }
                }
                Statement::Let { name, expr, .. } => {
                    let val = eval_expr(expr, &self.typed, ctx, resolver, window, &mut env)?;
                    env.insert(name.to_string(), val);
                }
                Statement::Emit {
                    name, expr, target, ..
                } => {
                    let val = eval_expr(expr, &self.typed, ctx, resolver, window, &mut env)
                        .map_err(|mut e| {
                            if e.triggering_field.is_none() {
                                e.triggering_field = Some(Arc::from(&**name));
                            }
                            e
                        })?;
                    match target {
                        EmitTarget::Field => {
                            output.insert(name.to_string(), val);
                        }
                        EmitTarget::Pipeline => {
                            ctx.stable.set_pipeline_var(name, val);
                        }
                        EmitTarget::Source => {
                            ctx.stable.set_source_var(ctx.source_file, name, val);
                        }
                        EmitTarget::Record => {
                            record_var_writes.insert(name.to_string(), val);
                        }
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
                            eval_expr(g, &self.typed, ctx, resolver, window, &mut env)?,
                            Value::Bool(true)
                        )
                    } else {
                        true
                    };
                    if should_trace {
                        let msg = eval_expr(message, &self.typed, ctx, resolver, window, &mut env)?;
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
                    eval_expr(expr, &self.typed, ctx, resolver, window, &mut env)?;
                }
                Statement::EmitEach { .. } => {
                    // Statement borrow into self.typed.program would conflict
                    // with `&mut self` inside the body walker; the body is
                    // hoisted out by pointer here so the iterator can be
                    // dropped before the body walker re-borrows self.
                    let (binding_name, source_expr, body_stmts, span_val) = match stmt {
                        Statement::EmitEach {
                            binding,
                            source,
                            body,
                            span,
                            ..
                        } => (binding.clone(), source.clone(), body.clone(), *span),
                        _ => unreachable!(),
                    };
                    let source_val =
                        eval_expr(&source_expr, &self.typed, ctx, resolver, window, &mut env)?;
                    let elements = match source_val {
                        Value::Array(arr) => arr,
                        Value::Null => continue,
                        other => {
                            return Err(EvalError::new(
                                EvalErrorKind::TypeMismatch {
                                    expected: "Array",
                                    got: other.type_name(),
                                },
                                span_val,
                            ));
                        }
                    };
                    for element in elements {
                        if expansion_count >= max_expansion {
                            return Err(EvalError::new(
                                EvalErrorKind::ExpansionLimitExceeded {
                                    limit: max_expansion,
                                },
                                span_val,
                            ));
                        }
                        env.insert(binding_name.to_string(), element);
                        let mut iter_fields = indexmap::IndexMap::new();
                        let mut iter_record_vars: indexmap::IndexMap<String, Value> =
                            indexmap::IndexMap::new();
                        // Seed each iteration with any field-target
                        // emits already produced before this fan-out:
                        // top-level `emit name = ...` writes prior to
                        // the `emit each` block apply to every emitted
                        // record.
                        for (k, v) in &output {
                            iter_fields.insert(k.clone(), v.clone());
                        }
                        for (k, v) in &record_var_writes {
                            iter_record_vars.insert(k.clone(), v.clone());
                        }
                        let eval_env = EvalEnv {
                            typed: &self.typed,
                            ctx,
                            resolver,
                            window,
                        };
                        Self::eval_emit_each_body(
                            &body_stmts,
                            eval_env,
                            &mut env,
                            &mut iter_fields,
                            &mut iter_record_vars,
                        )?;
                        env.remove(binding_name.as_ref());
                        fan_out.push(EmitOne {
                            fields: iter_fields,
                            record_vars: Box::new(iter_record_vars),
                        });
                        expansion_count += 1;
                    }
                }
            }
        }
        if !fan_out.is_empty() {
            Ok(EvalResult::EmitMany { records: fan_out })
        } else {
            Ok(EvalResult::Emit {
                fields: output,
                record_vars: Box::new(record_var_writes),
            })
        }
    }

    /// Evaluate the body statements of an `emit each` block once with
    /// the binding already inserted into `env`. Mirrors a stripped-down
    /// version of [`Self::eval_record_inner`] — body statements may
    /// not be `EmitEach` (parser enforces no nesting) or `Filter` /
    /// `Distinct` (a fan-out block applies once per outer record, so a
    /// body filter/distinct would silently divide work between
    /// branches the engine can't represent; reject at runtime as a
    /// type-mismatch surfaced through the boundary `map_err`).
    fn eval_emit_each_body<'w, S: RecordStorage + 'w>(
        body: &[Statement],
        eval_env: EvalEnv<'_, 'w, S>,
        env: &mut HashMap<String, Value>,
        fields: &mut indexmap::IndexMap<String, Value>,
        record_vars: &mut indexmap::IndexMap<String, Value>,
    ) -> Result<(), EvalError> {
        let EvalEnv {
            typed,
            ctx,
            resolver,
            window,
        } = eval_env;
        for stmt in body {
            match stmt {
                Statement::Let { name, expr, .. } => {
                    let val = eval_expr(expr, typed, ctx, resolver, window, env)?;
                    env.insert(name.to_string(), val);
                }
                Statement::Emit {
                    name, expr, target, ..
                } => {
                    let val =
                        eval_expr(expr, typed, ctx, resolver, window, env).map_err(|mut e| {
                            if e.triggering_field.is_none() {
                                e.triggering_field = Some(Arc::from(&**name));
                            }
                            e
                        })?;
                    match target {
                        EmitTarget::Field => {
                            fields.insert(name.to_string(), val);
                        }
                        EmitTarget::Pipeline => {
                            ctx.stable.set_pipeline_var(name, val);
                        }
                        EmitTarget::Source => {
                            ctx.stable.set_source_var(ctx.source_file, name, val);
                        }
                        EmitTarget::Record => {
                            record_vars.insert(name.to_string(), val);
                        }
                    }
                }
                Statement::Trace { .. } | Statement::UseStmt { .. } => {}
                Statement::ExprStmt { expr, .. } => {
                    eval_expr(expr, typed, ctx, resolver, window, env)?;
                }
                Statement::Filter { .. }
                | Statement::Distinct { .. }
                | Statement::EmitEach { .. } => {
                    return Err(EvalError::new(
                        EvalErrorKind::TypeMismatch {
                            expected: "let / emit / trace inside emit each body",
                            got: "filter / distinct / nested emit_each",
                        },
                        stmt_span(stmt),
                    ));
                }
            }
        }
        Ok(())
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
                // Resolve from let-bindings first, then input fields.
                // `value_to_group_key` takes `&Value`, so the env and
                // resolver hits can feed it by borrow without cloning;
                // the null fallback hands out a reference to the
                // shared `clinker_record::NULL` sentinel.
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
                // Bare distinct — hash all input fields
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
}

/// Dispatch a closure-bearing array method (`filter`/`map`/`find`/
/// `any`/`flat_map`). The closure argument's body is evaluated once
/// per array element with the closure parameter inserted into `env`
/// (and removed after each iteration so the env is left undisturbed
/// for the caller). Non-array receivers fall through to `Null`.
fn dispatch_closure_method<'w, S: RecordStorage + 'w>(
    receiver: &Value,
    method: &str,
    args: &[Expr],
    span: Span,
    eval_env: EvalEnv<'_, 'w, S>,
    env: &mut HashMap<String, Value>,
) -> Result<Value, EvalError> {
    let EvalEnv {
        typed,
        ctx,
        resolver,
        window,
    } = eval_env;
    // Null receivers short-circuit to Null for every closure-bearing
    // builtin — mirrors the null-propagation policy in
    // `dispatch_method`.
    if receiver.is_null() {
        return Ok(Value::Null);
    }
    let elements = match receiver {
        Value::Array(arr) => arr,
        _ => return Ok(Value::Null),
    };
    let closure = match args.first() {
        Some(Expr::Closure { param, body, .. }) => (param.clone(), body.as_ref()),
        _ => {
            return Err(EvalError::new(
                EvalErrorKind::TypeMismatch {
                    expected: "closure argument (`it => ...`)",
                    got: "non-closure argument",
                },
                span,
            ));
        }
    };
    let (param_name, body) = closure;

    // Per-iteration insert+remove on the SHARED `env` keeps the
    // closure-body eval allocation-free on the closure side — the
    // body's own subexpressions still allocate their results as
    // before.
    let previous = env.remove(param_name.as_ref());
    let mut output: Vec<Value> = Vec::new();
    let mut found: Option<Value> = None;
    let mut found_any: bool = false;

    for element in elements {
        env.insert(param_name.to_string(), element.clone());
        let body_val = eval_expr(body, typed, ctx, resolver, window, env);
        // Always remove the binding to keep env clean even on error.
        env.remove(param_name.as_ref());
        let body_val = body_val?;
        match method {
            "filter" => {
                if matches!(body_val, Value::Bool(true)) {
                    output.push(element.clone());
                }
            }
            "map" => {
                output.push(body_val);
            }
            "find" => {
                if matches!(body_val, Value::Bool(true)) {
                    found = Some(element.clone());
                    break;
                }
            }
            "any" => {
                if matches!(body_val, Value::Bool(true)) {
                    found_any = true;
                    break;
                }
            }
            "flat_map" => match body_val {
                Value::Array(inner) => output.extend(inner),
                Value::Null => {}
                other => output.push(other),
            },
            _ => unreachable!("dispatch_closure_method gated by method name"),
        }
    }

    if let Some(prev) = previous {
        env.insert(param_name.to_string(), prev);
    }

    Ok(match method {
        "filter" | "map" | "flat_map" => Value::Array(output),
        "find" => found.unwrap_or(Value::Null),
        "any" => Value::Bool(found_any),
        _ => unreachable!(),
    })
}

/// Recover the span of a [`Statement`] variant. Used inside
/// `emit each` body validation to anchor a runtime error on the
/// offending body statement when its variant is forbidden inside the
/// block.
fn stmt_span(stmt: &Statement) -> Span {
    match stmt {
        Statement::Let { span, .. }
        | Statement::Emit { span, .. }
        | Statement::Trace { span, .. }
        | Statement::UseStmt { span, .. }
        | Statement::ExprStmt { span, .. }
        | Statement::Filter { span, .. }
        | Statement::Distinct { span, .. }
        | Statement::EmitEach { span, .. } => *span,
    }
}

/// Convert a GroupKeyError to an EvalError.
pub(super) fn group_key_error_to_eval_error(e: GroupKeyError) -> EvalError {
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

/// Evaluate a single expression.
///
/// `env` is `&mut HashMap<String, Value>` so the closure-bearing
/// builtin dispatcher can insert the closure parameter for the body
/// walk and remove it afterward — the design's no-clone invariant
/// requires direct mutation rather than per-iteration env copies.
/// Non-closure call sites pass the same env unchanged.
pub fn eval_expr<'w, S: RecordStorage + 'w>(
    expr: &Expr,
    typed: &TypedProgram,
    ctx: &EvalContext<'_>,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext<'w, S>>,
    env: &mut HashMap<String, Value>,
) -> Result<Value, EvalError> {
    match expr {
        Expr::Literal { value, .. } => Ok(literal_to_value(value)),

        Expr::FieldRef { name, .. } => {
            // Check let-bound env first
            if let Some(val) = env.get(&**name) {
                return Ok(val.clone());
            }
            // Then field resolver — clone only at the eval-site that
            // keeps the value past the next resolve() call. Short-circuits
            // inside coalesce / filter that borrow the return don't pay
            // the clone (they're gone by the time the owned `Value` is
            // constructed here).
            Ok(resolver.resolve(name).cloned().unwrap_or(Value::Null))
        }

        Expr::QualifiedFieldRef { parts, .. } => {
            match parts.len() {
                2 => Ok(resolver
                    .resolve_qualified(&parts[0], &parts[1])
                    .cloned()
                    .unwrap_or(Value::Null)),
                3 => {
                    // Three-part path: source.record_type.field
                    // Join first two parts as the compound source key
                    let compound = format!("{}.{}", &parts[0], &parts[1]);
                    Ok(resolver
                        .resolve_qualified(&compound, &parts[2])
                        .cloned()
                        .unwrap_or(Value::Null))
                }
                _ => Ok(Value::Null),
            }
        }

        Expr::PipelineAccess { field, .. } => {
            Ok(ctx.resolve_pipeline(field).unwrap_or(Value::Null))
        }

        Expr::VarsAccess { key, .. } => Ok(ctx
            .stable
            .static_vars
            .get(key.as_ref())
            .cloned()
            .unwrap_or(Value::Null)),

        Expr::SourceAccess { field, .. } => Ok(ctx.resolve_source(field).unwrap_or(Value::Null)),

        Expr::DocAccess { section, field, .. } => {
            Ok(ctx.resolve_doc(section, field).unwrap_or(Value::Null))
        }

        Expr::QualifiedSourceAccess {
            input_name, field, ..
        } => {
            // `$source.<input_name>.<field>` looks up the value an
            // upstream Transform writer set for records flowing
            // through Source `<input_name>`. The plan-time
            // `source_input_arcs` map (built by clinker-exec's
            // `build_stable_eval_context`) gives the source-file Arcs
            // for each input name; we scan them and return the first
            // matching `resolve_source_var` hit.
            //
            // Per-input single-writer (E170) + same-program-eval
            // invariants make values across multi-file Sources of the
            // same input trivially agree, so first-match semantics is
            // well-defined.
            if let Some(arcs) = ctx.stable.source_input_arcs.get(input_name.as_ref()) {
                for arc in arcs {
                    if let Some(v) = ctx.stable.resolve_source_var(arc, field) {
                        return Ok(v);
                    }
                }
            }
            Ok(Value::Null)
        }

        Expr::RecordAccess { field, .. } => {
            // `$record.<key>` reads delegate to the resolver, which
            // (for `Record`) strips the `$record.` prefix and looks
            // up the value in the dedicated record-vars channel.
            // The state-node arm with `scope: record` writes there.
            Ok(resolver
                .resolve(&format!("$record.{field}"))
                .cloned()
                .unwrap_or(Value::Null))
        }

        Expr::Now { .. } => Ok(Value::DateTime(ctx.stable.clock.now())),

        Expr::Wildcard { .. } => Ok(Value::Bool(true)), // Wildcard in match = always matches

        Expr::Binary {
            op, lhs, rhs, span, ..
        } => eval_binary(*op, lhs, rhs, *span, typed, ctx, resolver, window, env),

        Expr::Unary {
            op, operand, span, ..
        } => {
            let val = eval_expr(operand, typed, ctx, resolver, window, env)?;
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
            let left = eval_expr(lhs, typed, ctx, resolver, window, env)?;
            if left.is_null() {
                eval_expr(rhs, typed, ctx, resolver, window, env)
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

        Expr::MethodCall {
            node_id,
            receiver,
            method,
            args,
            span,
        } => {
            // `$window.lag(n).<field>` style chains: parser emits this as
            // `MethodCall { receiver: WindowCall(lag/lead/first/last), method: <field>, args: [] }`.
            // Resolve the field directly off the positional record without
            // round-tripping through `Value` — `RecordView` carries a
            // borrow into the arena that would not survive the lifetime of
            // a `Value` round-trip without inventing a lifetime-carrying
            // variant on the `Value` enum (which the postcard/serde wire
            // format cannot accommodate).
            if args.is_empty()
                && let Expr::WindowCall {
                    function,
                    args: window_args,
                    ..
                } = &**receiver
                && matches!(&**function, "first" | "last" | "lag" | "lead")
            {
                let w = window.ok_or_else(|| {
                    EvalError::new(
                        EvalErrorKind::TypeMismatch {
                            expected: "window context",
                            got: "none",
                        },
                        *span,
                    )
                })?;
                let view = match &**function {
                    "first" => w.first(),
                    "last" => w.last(),
                    "lag" | "lead" => {
                        let offset = match window_args.first() {
                            Some(arg) => {
                                let v = eval_expr(arg, typed, ctx, resolver, window, env)?;
                                if let Value::Integer(n) = v {
                                    n.max(0) as usize
                                } else {
                                    1
                                }
                            }
                            None => 1,
                        };
                        if &**function == "lag" {
                            w.lag(offset)
                        } else {
                            w.lead(offset)
                        }
                    }
                    _ => unreachable!("matches! pre-filtered the function name"),
                };
                return Ok(view
                    .and_then(|row| row.resolve(method).cloned())
                    .unwrap_or(Value::Null));
            }

            let recv_val = eval_expr(receiver, typed, ctx, resolver, window, env)?;

            // Closure-bearing array builtins: dispatch with the raw
            // argument exprs so the closure body can re-evaluate per
            // iteration with the closure parameter bound. Per-iteration
            // binding goes through the shared env via insert+remove —
            // no per-iteration env clone. `find` overloads with the
            // string-regex variant: dispatch by receiver-Closure-arg
            // shape so a `.find("pattern")` on a string still routes
            // through the regex form below.
            if matches!(
                method.as_ref(),
                "filter" | "map" | "find" | "any" | "flat_map"
            ) && args
                .first()
                .map(|a| matches!(a, Expr::Closure { .. }))
                .unwrap_or(false)
            {
                let eval_env = EvalEnv {
                    typed,
                    ctx,
                    resolver,
                    window,
                };
                return dispatch_closure_method(&recv_val, method, args, *span, eval_env, env);
            }

            let mut arg_vals = Vec::with_capacity(args.len());
            for arg in args {
                arg_vals.push(eval_expr(arg, typed, ctx, resolver, window, env)?);
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
                "cumulative_sum" => {
                    if let Some(Expr::FieldRef { name, .. }) = args.first() {
                        Ok(w.cumulative_sum(name))
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
                // Positional builtins return `RecordView` per the trait;
                // a bare `$window.first()` (without `.field`) is semantically
                // a row reference, which `Value` cannot carry — `Value`
                // is `'static` and `RecordView<'a, S>` borrows the arena.
                // The meaningful surface is the postfix chain
                // `$window.first().<field>`, which the `MethodCall` arm
                // intercepts upstream. Reaching this arm means the user
                // wrote a positional builtin without a field accessor —
                // return Null so downstream coalesce / filter sees a
                // well-typed nullable.
                "first" | "last" | "lag" | "lead" => Ok(Value::Null),
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
                "row_number" => Ok(Value::Integer(w.row_number())),
                "rank" => Ok(Value::Integer(w.rank())),
                "dense_rank" => Ok(Value::Integer(w.dense_rank())),
                "first_value" => {
                    if let Some(Expr::FieldRef { name, .. }) = args.first() {
                        Ok(w.first_value(name))
                    } else {
                        Ok(Value::Null)
                    }
                }
                "last_value" => {
                    if let Some(Expr::FieldRef { name, .. }) = args.first() {
                        Ok(w.last_value(name))
                    } else {
                        Ok(Value::Null)
                    }
                }
                // Predicate-fold builtins share a three-valued Kleene
                // shape: `any` and `exists` collapse to iterated OR;
                // `every` and `not_exists` collapse to iterated AND
                // (with `not_exists` inverting the predicate result).
                // The fold must agree with BinOp::And/Or, which are
                // Kleene three-valued and silently treat non-Bool /
                // non-Null operands as Null. Mirroring both properties
                // keeps the identity `any([a, b, c]) ≡ a or b or c`
                // correctness-preserving under rewrites.
                //
                // `exists(p)` is a semantic alias of `any(p)` — they
                // produce the same value over the same partition.
                // `not_exists(p)` is `every(not p)` and is implemented
                // as a primitive (`every` fold over an inverted Bool)
                // for clarity and to share short-circuit semantics with
                // the other folds.
                //
                // Typecheck (typecheck/pass.rs) rejects non-Bool argument
                // types and nested $window.* inside the predicate before
                // we reach this arm.
                "any" | "every" | "exists" | "not_exists" => {
                    let predicate = args.first().ok_or_else(|| {
                        EvalError::new(
                            EvalErrorKind::ArityMismatch {
                                name: function.to_string(),
                                expected: 1,
                                got: 0,
                            },
                            *span,
                        )
                    })?;
                    let invert = &**function == "not_exists";
                    let want_any = matches!(&**function, "any" | "exists");
                    let mut found_null = false;
                    for i in 0..w.partition_len() {
                        let row = w.partition_record(i);
                        let raw = eval_expr(predicate, typed, ctx, &row, window, env)?;
                        let v = match (invert, raw) {
                            (true, Value::Bool(b)) => Value::Bool(!b),
                            (_, other) => other,
                        };
                        match v {
                            Value::Bool(true) if want_any => return Ok(Value::Bool(true)),
                            Value::Bool(false) if !want_any => return Ok(Value::Bool(false)),
                            Value::Bool(_) => {}
                            // Null and non-Bool runtime values both
                            // collapse to "null seen" — same catch-all
                            // as BinOp::And/Or.
                            _ => found_null = true,
                        }
                    }
                    Ok(if found_null {
                        Value::Null
                    } else {
                        // Identity for the iterated operator:
                        //   any/exists (iterated or) → false
                        //   every/not_exists (iterated and) → true
                        Value::Bool(!want_any)
                    })
                }
                // Unregistered names cannot reach here: the typecheck
                // pass rejects unknown $window.* identifiers before
                // evaluation. Reaching this arm means a registered name
                // has no evaluator implementation — surface a typed
                // error rather than silently emitting Null.
                _ => Err(EvalError::new(
                    EvalErrorKind::TypeMismatch {
                        expected: "registered $window function",
                        got: "unimplemented evaluator arm",
                    },
                    *span,
                )),
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
        // the aggregate finalize path has its own evaluator.
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

        // Bracket-index access dispatches by receiver value type:
        // integer indices apply to `Value::Array`; string indices to
        // `Value::Map`. Out-of-bounds / type mismatch return Null so
        // downstream coalesce / filter sees a well-typed nullable.
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            let recv = eval_expr(receiver, typed, ctx, resolver, window, env)?;
            if recv.is_null() {
                return Ok(Value::Null);
            }
            let idx_val = eval_expr(index, typed, ctx, resolver, window, env)?;
            Ok(match (&recv, &idx_val) {
                (Value::Array(arr), Value::Integer(i)) => {
                    if *i < 0 {
                        Value::Null
                    } else {
                        arr.get(*i as usize).cloned().unwrap_or(Value::Null)
                    }
                }
                (Value::Map(m), Value::String(key)) => {
                    m.get(key.as_str()).cloned().unwrap_or(Value::Null)
                }
                _ => Value::Null,
            })
        }

        // Closures only evaluate inside method-call dispatch (`filter`,
        // `map`, etc.) — the call site introduces the closure
        // parameter into env and recursively evaluates the body. A
        // closure reaching here means it appeared in a context other
        // than a closure-bearing builtin, which the resolve pass
        // rejects upstream; surface a typed error to keep the data
        // path strict.
        Expr::Closure { span, .. } => Err(EvalError::new(
            EvalErrorKind::TypeMismatch {
                expected: "closure-bearing method argument",
                got: "free-standing closure",
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
    ctx: &EvalContext<'_>,
    resolver: &dyn FieldResolver,
    window: Option<&dyn WindowContext<'w, S>>,
    env: &mut HashMap<String, Value>,
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
        LiteralValue::String(s) => Value::String(s.as_ref().into()),
        LiteralValue::Date(d) => Value::Date(*d),
        LiteralValue::Bool(b) => Value::Bool(*b),
        LiteralValue::Null => Value::Null,
    }
}
