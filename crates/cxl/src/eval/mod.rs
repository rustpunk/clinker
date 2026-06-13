pub mod builtins_impl;
pub mod context;
pub mod error;

// Compile-once-to-closures evaluator. `ProgramEvaluator` compiles a
// program into a `CompiledProgram` on first use and runs it per record;
// this module is the sole per-record scalar core.
pub(crate) mod compiled;

#[cfg(test)]
mod tests;

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{GroupByKey, GroupKeyError, Value};

use crate::ast::Expr;
use crate::lexer::Span;
use crate::resolve::traits::{FieldResolver, RecordStorage, WindowContext};
use crate::typecheck::pass::TypedProgram;

use compiled::{CompiledProgram, EvalState};

pub use compiled::{CompiledScalar, compare_values, compile_scalar};
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

/// Owns the `TypedProgram` and optional distinct dedup state. One evaluator
/// per transform per partition/thread — never Clone, use factory construction.
/// Follows DataFusion Accumulator / Polars GroupedReduction pattern.
pub struct ProgramEvaluator {
    typed: Arc<TypedProgram>,
    /// Distinct dedup set, partition key, and `emit each` ceiling. The
    /// compiled program is immutable and threads this mutable state by
    /// `&mut` so `set_partition` and per-record dedup live on the
    /// evaluator, not the program.
    state: EvalState,
    /// Whether this program has any distinct statements. Gates the dedup
    /// set's allocation inside `state`.
    has_distinct: bool,
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
            compiled_cache: HashMap::new(),
        }
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

    /// Borrow the underlying typed program.
    pub fn typed(&self) -> &Arc<TypedProgram> {
        &self.typed
    }

    /// Lower a single scalar expression against this evaluator's typed
    /// program to a reusable [`CompiledScalar`].
    ///
    /// The aggregation engine compiles each pre-aggregation filter and
    /// `BindingArg::Expr` argument once at construction through this factory
    /// (the regex cache and literal baking are paid once), then evaluates the
    /// resulting closure per record — replacing the per-record expression
    /// re-walk the recursive matcher performed.
    pub fn compile_scalar(&self, expr: &Expr) -> CompiledScalar {
        compile_scalar(&self.typed, expr)
    }

    /// Evaluate a record through the full program, returning Emit or Skip.
    ///
    /// Compiles the program into a `CompiledProgram<S>` on first use of
    /// each storage type `S`, caches it by `TypeId`, and runs it; the
    /// cross-record dedup state in `self.state` (the same object
    /// `set_partition` mutates) is threaded by `&mut` so a `distinct`
    /// program dedups across the record stream.
    ///
    /// Wraps the run in a boundary `map_err` that attaches the current
    /// `source_row` and the program's `source` text to any [`EvalError`]
    /// surfaced from a subexpression, so DLQ entries and miette diagnostics
    /// downstream carry "row N: ..." context and an underlined source span
    /// without every internal constructor having to thread those fields
    /// manually.
    ///
    /// `S` is `'static` because the cache holds one `CompiledProgram<S>`
    /// per storage type keyed by `TypeId`; every real storage
    /// (`NullStorage`, the window arena) is already `'static`.
    pub fn eval_record<'w, S: RecordStorage + 'static>(
        &mut self,
        ctx: &EvalContext<'_>,
        resolver: &dyn FieldResolver,
        window: Option<&dyn WindowContext<'w, S>>,
    ) -> Result<EvalResult, EvalError> {
        let source_row = ctx.source_row;
        let source_expr = self.typed.source.clone();
        let program = self
            .compiled_cache
            .entry(TypeId::of::<S>())
            .or_insert_with(|| Box::new(compiled::compile::<S>(&self.typed)))
            .downcast_ref::<CompiledProgram<S>>()
            .expect("compiled cache keyed by TypeId<S> always holds a CompiledProgram<S>");
        program
            .eval_record(ctx, resolver, window, &mut self.state)
            .map_err(move |mut e| {
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
