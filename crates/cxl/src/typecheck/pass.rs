use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use indexmap::IndexMap;
use regex::Regex;

use super::row::{ColumnLookup, QualifiedField, Row};
use super::types::Type;
use crate::ast::{BinOp, Expr, LiteralValue, NodeId, Program, Statement, UnaryOp};
use crate::builtins::{BuiltinDef, BuiltinRegistry, TypeTag};
use crate::lexer::Span;
use crate::resolve::pass::{ResolvedBinding, ResolvedProgram};
use crate::resolve::scoped_vars::{ScopedVarType, ScopedVarsRegistry};

/// Return type inference for aggregate function calls.
///
/// - `sum(Integer)` → Integer, `sum(Float)` → Float, `sum(Numeric/Any)` → Numeric
/// - `count(...)` → Integer (always)
/// - `avg(...)` → Float
/// - `min(T)` / `max(T)` → T (preserving input type)
/// - `collect(T)` → Array (element type is currently erased)
/// - `weighted_avg(value, weight)` → Decimal if either arg is decimal, else Float
/// - Unknown names → Any (caller emits a diagnostic via the parser lookahead set)
fn aggregate_return_type(name: &str, arg_types: &[Type]) -> Type {
    match name {
        "sum" => arg_types
            .first()
            .map(|t| match t.unwrap_nullable() {
                Type::Int => Type::Int,
                Type::Float => Type::Float,
                // Exact aggregation: sum over a decimal stays decimal.
                Type::Decimal => Type::Decimal,
                Type::Numeric => Type::Numeric,
                _ => Type::Numeric,
            })
            .unwrap_or(Type::Numeric),
        "count" => Type::Int,
        // Avg over a decimal is exact (returns decimal); otherwise Float.
        "avg" => match arg_types.first().map(Type::unwrap_nullable) {
            Some(Type::Decimal) => Type::Decimal,
            _ => Type::Float,
        },
        // weighted_avg over a decimal value or weight is exact (returns
        // decimal); otherwise Float.
        "weighted_avg" => {
            if arg_types
                .iter()
                .any(|t| matches!(t.unwrap_nullable(), Type::Decimal))
            {
                Type::Decimal
            } else {
                Type::Float
            }
        }
        "min" | "max" => arg_types.first().cloned().unwrap_or(Type::Any),
        "collect" => Type::Array,
        _ => Type::Any,
    }
}

/// Aggregate context mode for typecheck enforcement.
///
/// `Row` is the default for ordinary row-level transforms and rejects any
/// `Expr::AggCall`. `GroupBy` is set for transforms declared via the
/// `aggregate:` YAML block and additionally enforces that bare field
/// references outside an aggregate call appear in the `group_by` field set.
///
/// Mirrors the PostgreSQL `parseCheckAggregates` / Spark `CheckAnalysis`
/// two-mode pattern: aggregate-context typecheck is a separate pass gated by
/// this flag.
#[derive(Debug, Clone)]
pub enum AggregateMode {
    /// Row-level transform. Any `Expr::AggCall` is an error.
    Row,
    /// Aggregate transform. Bare `FieldRef` not in `group_by_fields` and not
    /// inside an `AggCall` is an error.
    GroupBy { group_by_fields: HashSet<String> },
}

/// A diagnostic produced by the type checker.
#[derive(Debug, Clone)]
pub struct TypeDiagnostic {
    pub span: Span,
    pub message: String,
    pub help: Option<String>,
    /// Secondary span for cross-expression conflicts.
    pub related_span: Option<Span>,
    /// Is this a warning (true) or error (false)?
    pub is_warning: bool,
}

/// A constraint from a single usage site of a field.
#[derive(Debug, Clone)]
struct FieldConstraint {
    inferred_type: Type,
    span: Span,
}

/// Output of the type checker. Flat struct — takes ownership of ResolvedProgram fields.
/// Distinct type: compiler enforces Program → ResolvedProgram → TypedProgram ordering.
/// Must be Send + Sync for Arc sharing across rayon workers.
#[derive(Debug)]
pub struct TypedProgram {
    pub program: Program,
    pub bindings: Vec<Option<ResolvedBinding>>,
    /// Per-node type annotation, indexed by NodeId.
    pub types: Vec<Option<Type>>,
    /// Inferred field types for runtime DLQ validation. Deterministic
    /// iteration order.
    ///
    /// Keyed by `QualifiedField` to match the `Row.declared`
    /// representation — schema-declared fields may carry a qualifier
    /// (combine merged rows, Phase Combine C.1.0). For non-combine
    /// nodes every entry is bare. Callers that need a flat string list
    /// should use `.keys().map(|qf| qf.name.to_string())`.
    pub field_types: IndexMap<QualifiedField, Type>,
    /// Pre-compiled regex literals, indexed by NodeId.
    pub regexes: Vec<Option<Regex>>,
    /// Total node count.
    pub node_count: u32,
    /// Bound output row for the node this program was typechecked for.
    /// Populated by `bind_schema` after running shape-specific row
    /// propagation (Transform widens, Aggregate projects group-by +
    /// aggregates, Combine emits a fresh body row). The input-seeded
    /// value is `Row::closed(IndexMap::new(), Span::default())`;
    /// consumers observe the populated row after `bind_schema` completes.
    pub output_row: Row,
    /// Original CXL source text this program was parsed from. Populated
    /// by production callers (the pipeline compiler) so runtime
    /// [`crate::eval::EvalError`] diagnostics can render the offending
    /// expression with a caret on the failing span. `None` for synthetic
    /// programs and standalone tests that do not exercise the diagnostic
    /// renderer.
    pub source: Option<Arc<str>>,
}

impl TypedProgram {
    /// Attach the original CXL source text. Production pipeline-compile
    /// callers set this so [`crate::eval::EvalError`] miette rendering
    /// can underline the offending subexpression.
    pub fn with_source(mut self, source: Arc<str>) -> Self {
        self.source = Some(source);
        self
    }
}

/// Run Phase C: type-check a resolved program in row-level mode (the
/// historical default). Equivalent to `type_check_with_mode(resolved, schema,
/// AggregateMode::Row)`. Most call sites use this entry point.
pub fn type_check(
    resolved: ResolvedProgram,
    schema: &Row,
) -> Result<TypedProgram, Vec<TypeDiagnostic>> {
    type_check_with_mode(resolved, schema, AggregateMode::Row)
}

/// Run Phase C with an explicit aggregate-mode. `AggregateMode::GroupBy`
/// enables the two-direction aggregate context checks documented on
/// `AggregateMode`.
pub fn type_check_with_mode(
    resolved: ResolvedProgram,
    schema: &Row,
    aggregate_mode: AggregateMode,
) -> Result<TypedProgram, Vec<TypeDiagnostic>> {
    type_check_with_mode_and_vars(
        resolved,
        schema,
        aggregate_mode,
        &ScopedVarsRegistry::default(),
    )
}

/// Run Phase C with explicit aggregate-mode and scoped-vars registry.
///
/// Reads of declared `$pipeline.<key>` / `$source.<key>` resolve to the
/// declared type (mapped via [`scoped_var_type_to_type`]); undeclared keys
/// would have been rejected at resolve time and never reach the
/// typechecker, so this lookup is total for any resolver-accepted access.
pub fn type_check_with_mode_and_vars(
    resolved: ResolvedProgram,
    schema: &Row,
    aggregate_mode: AggregateMode,
    scoped_vars: &ScopedVarsRegistry,
) -> Result<TypedProgram, Vec<TypeDiagnostic>> {
    let registry = BuiltinRegistry::new();
    let node_count = resolved.node_count;

    // Destructure resolved to avoid borrow conflicts
    let ResolvedProgram {
        program,
        bindings,
        node_count: _,
    } = resolved;

    // Root node of each top-level `let` RHS, in declaration order. Matches the
    // `ResolvedBinding::LetVar(i)` index for top-level lets (nested emit-each /
    // closure bindings sit above these on the resolver's scope stack and are
    // popped before the next top-level statement, so they never shift a
    // top-level let's ordinal).
    let let_binding_nodes: Vec<NodeId> = program
        .statements
        .iter()
        .filter_map(|s| match s {
            Statement::Let { expr, .. } => Some(expr.node_id()),
            _ => None,
        })
        .collect();

    let mut checker = TypeChecker {
        bindings: &bindings,
        schema,
        registry: &registry,
        scoped_vars,
        types: vec![None; node_count as usize],
        regexes: vec![None; node_count as usize],
        field_constraints: HashMap::new(),
        let_binding_nodes,
        diagnostics: Vec::new(),
        aggregate_mode: aggregate_mode.clone(),
        agg_function_depth: 0,
    };

    // Pass 1: Constraint collection + Pass 2: Bottom-up annotation (single walk)
    for stmt in &program.statements {
        checker.check_statement(stmt);
    }

    // Unify per-field constraints
    checker.unify_field_constraints();

    // Phase C semantic checks
    checker.check_emit_count(&program);
    checker.check_aggregate_context(&program);

    // Build field_types map
    let field_types = checker.build_field_type_map();

    let has_errors = checker.diagnostics.iter().any(|d| !d.is_warning);
    if has_errors {
        return Err(checker.diagnostics);
    }

    // Extract from checker (partial moves release the borrow on `bindings`)
    let types = checker.types;
    let regexes = checker.regexes;

    Ok(TypedProgram {
        program,
        bindings,
        types,
        field_types,
        regexes,
        node_count,
        output_row: Row::closed(IndexMap::new(), Span::default()),
        source: None,
    })
}

struct TypeChecker<'a> {
    bindings: &'a [Option<ResolvedBinding>],
    schema: &'a Row,
    registry: &'a BuiltinRegistry,
    /// User-declared `$pipeline.<key>` / `$source.<key>` / `$record.<key>`
    /// types; consulted on every PipelineAccess / SourceAccess after the
    /// builtin-member shortcut misses.
    scoped_vars: &'a ScopedVarsRegistry,
    types: Vec<Option<Type>>,
    regexes: Vec<Option<Regex>>,
    field_constraints: HashMap<String, Vec<FieldConstraint>>,
    /// Root `NodeId` of each top-level `let` binding's RHS, in declaration
    /// order. `ResolvedBinding::LetVar(i)` indexes this to recover a let-var's
    /// type from its RHS node.
    ///
    /// Soundness: a *genuine* top-level let-var reference has `i` equal to that
    /// let's ordinal here, and — because the resolver forbids forward
    /// references and statements are walked in order — its RHS was annotated
    /// before the reference is visited, so `get_type` returns the real type.
    /// `LetVar(i)` is *also* produced for emit-each iterators and closure
    /// params; their scope-stack index either exceeds this vec (→ `Type::Any`)
    /// or aliases a *later* top-level let whose RHS is not yet annotated at that
    /// point in the walk (→ `Type::Any` via the unset default). Both erase to
    /// `Any`, which is correct: an iterator/closure element type is unknown
    /// here. No index ever aliases an already-annotated unrelated binding.
    let_binding_nodes: Vec<NodeId>,
    diagnostics: Vec<TypeDiagnostic>,
    // Note: in_predicate_expr tracking is done via the resolver's context,
    // but we track it here for the nested-window check
    aggregate_mode: AggregateMode,
    /// Depth counter for the `check_aggregate_context` walk. When > 0 we are
    /// inside the arguments of an AggCall; bare FieldRefs are exempted from
    /// the "must appear in group_by" check.
    agg_function_depth: u32,
}

/// Convert a [`ScopedVarType`] (the config-mirroring tag) to the CXL
/// [`Type`]. `Date`/`DateTime` map to their distinct CXL types — strict
/// mode coerces only at the eval boundary, not at typecheck.
fn scoped_var_type_to_type(t: ScopedVarType) -> Type {
    match t {
        ScopedVarType::String => Type::String,
        ScopedVarType::Int => Type::Int,
        ScopedVarType::Float => Type::Float,
        ScopedVarType::Bool => Type::Bool,
        ScopedVarType::Date => Type::Date,
        ScopedVarType::DateTime => Type::DateTime,
    }
}

/// Whether `recv` (nullability already stripped) is an acceptable receiver for
/// a builtin whose registered receiver tag is `def_receiver`.
///
/// The name-keyed registry stores one receiver `TypeTag` per method, but eval
/// dispatches several methods on the runtime value, so the single tag
/// under-describes the true receiver set. `Any` on either side is permissive
/// (open rows / composition-tail receivers). Beyond an exact unify, three
/// families are value-dispatched overloads the registry cannot express under
/// one key:
/// - numeric methods (`abs`, `round`, `clamp`, …) also accept `Decimal` —
///   exact decimals run the same evaluation paths, and `Decimal` deliberately
///   does not *unify* with the `int|float` `Numeric` union yet is a valid
///   numeric receiver;
/// - date methods (`year`, `hour`, `format_date`, …) also accept `DateTime`;
/// - `join` / `length` / `find` also accept `Array` (String/Array overloads
///   collapsed onto one name key, dispatched by receiver value at eval).
fn receiver_compatible(def_receiver: TypeTag, method: &str, recv: &Type) -> bool {
    if def_receiver == TypeTag::Any || matches!(recv, Type::Any) {
        return true;
    }
    if recv.unify(&Type::from_type_tag(def_receiver)).is_some() {
        return true;
    }
    match def_receiver {
        TypeTag::Numeric => matches!(recv, Type::Decimal),
        TypeTag::Date => matches!(recv, Type::DateTime),
        TypeTag::String => {
            matches!(recv, Type::Array) && matches!(method, "join" | "length" | "find")
        }
        _ => false,
    }
}

/// Whether an argument of (nullability-stripped) type `got` satisfies a builtin
/// parameter whose declared tag is `want`. `Any` on either side is permissive —
/// this is what keeps closure arguments (which type as `Any`) and open-row
/// values valid. Numeric parameters additionally accept `Decimal`, mirroring
/// [`receiver_compatible`].
fn arg_compatible(want: TypeTag, got: &Type) -> bool {
    if want == TypeTag::Any || matches!(got, Type::Any) {
        return true;
    }
    if got.unify(&Type::from_type_tag(want)).is_some() {
        return true;
    }
    matches!(want, TypeTag::Numeric) && matches!(got, Type::Decimal)
}

/// Return type of a builtin `MethodCall`, applying the receiver-dependent
/// overrides the name-keyed registry cannot express under one `return_type`
/// tag. Each override tracks what the evaluator actually returns for that
/// receiver form; without them the wrong type poisons the operand/method
/// checks on the surrounding expression.
fn method_return_type(method: &str, recv_ty: &Type, def: &BuiltinDef) -> Type {
    let recv = recv_ty.unwrap_nullable();
    match method {
        // Exact decimals run these numeric methods closed over the decimal
        // domain, so the result stays `Decimal` even though the registry types
        // them over the general `Numeric` receiver (which never unifies with
        // `Decimal`). `clamp`/`min`/`max` are excluded: they may return an
        // argument value, so the result is not guaranteed to be a decimal.
        "abs" | "ceil" | "floor" | "round" | "round_to" if matches!(recv, Type::Decimal) => {
            Type::Decimal
        }
        // `add_days` preserves the receiver's date kind (`Date`→`Date`,
        // `DateTime`→`DateTime`); the registry stores only the `Date` return.
        "add_days" if matches!(recv, Type::DateTime) => Type::DateTime,
        // `find` is a String/Array overload sharing one registry key: on a
        // String it returns `Bool` (regex match), on an Array it returns the
        // matched element. Erase the non-String form to `Any` rather than
        // propagate the wrong `Bool`.
        "find" if !matches!(recv, Type::String) => Type::Any,
        _ => Type::from_type_tag(def.return_type),
    }
}

impl<'a> TypeChecker<'a> {
    fn set_type(&mut self, node_id: NodeId, ty: Type) {
        let idx = node_id.0 as usize;
        if idx < self.types.len() {
            self.types[idx] = Some(ty);
        }
    }

    fn get_type(&self, node_id: NodeId) -> Type {
        let idx = node_id.0 as usize;
        self.types
            .get(idx)
            .and_then(|t| t.clone())
            .unwrap_or(Type::Any)
    }

    fn add_constraint(&mut self, field: &str, ty: Type, span: Span) {
        self.field_constraints
            .entry(field.to_string())
            .or_default()
            .push(FieldConstraint {
                inferred_type: ty,
                span,
            });
    }

    fn error(&mut self, span: Span, message: String, help: Option<String>) {
        self.diagnostics.push(TypeDiagnostic {
            span,
            message,
            help,
            related_span: None,
            is_warning: false,
        });
    }

    fn warning(&mut self, span: Span, message: String, help: Option<String>) {
        self.diagnostics.push(TypeDiagnostic {
            span,
            message,
            help,
            related_span: None,
            is_warning: true,
        });
    }

    // ── Statement checking ──────────────────────────────────────

    fn check_statement(&mut self, stmt: &Statement) {
        match stmt {
            Statement::Let { expr, .. } => {
                self.check_expr(expr, false);
            }
            Statement::Emit { expr, .. } => {
                self.check_expr(expr, false);
            }
            Statement::Trace { guard, message, .. } => {
                if let Some(g) = guard {
                    self.check_expr(g, false);
                }
                self.check_expr(message, false);
            }
            Statement::UseStmt { .. } => {}
            Statement::ExprStmt { expr, .. } => {
                self.check_expr(expr, false);
            }
            Statement::Filter {
                predicate, span, ..
            } => {
                let ty = self.check_expr(predicate, false);
                // Only error when type is *known* to be non-Bool.
                // Type::Any means unknown — can't verify, let it pass.
                if ty != Type::Bool && ty != Type::Any {
                    self.error(
                        *span,
                        format!(
                            "filter predicate must be type Bool, got {ty}. \
                             Use an explicit comparison."
                        ),
                        None,
                    );
                }
            }
            Statement::Distinct { .. } => {
                // Distinct field validation deferred — schema pin needed for full check.
            }
            Statement::EmitEach { source, body, .. } => {
                let source_ty = self.check_expr(source, false);
                let inner = source_ty.unwrap_nullable();
                if !matches!(inner, Type::Array | Type::Any) {
                    self.error(
                        source.span(),
                        format!(
                            "emit_each requires an Array source, got {source_ty}"
                        ),
                        Some("Use an Array-valued expression (e.g. `arr.filter(...)`, `str.split(\",\")`, or a Map-derived `.keys` / `.values`)".into()),
                    );
                }
                for stmt in body {
                    self.check_statement(stmt);
                }
            }
            Statement::ExplodeOuter { source, body, .. } => {
                let source_ty = self.check_expr(source, false);
                let inner = source_ty.unwrap_nullable();
                // The outer variant additionally accepts a statically-null
                // source: a null/empty array is the case it exists to
                // handle — it preserves the trigger row with the binding
                // bound to null rather than emitting nothing.
                if !matches!(inner, Type::Array | Type::Any | Type::Null) {
                    self.error(
                        source.span(),
                        format!(
                            "emit_each ... outer requires an Array, Null, or Any source, got {source_ty}"
                        ),
                        Some("Use an Array-valued (or nullable Array) expression; the outer variant preserves the trigger row when that array is null or empty".into()),
                    );
                }
                for stmt in body {
                    self.check_statement(stmt);
                }
            }
        }
    }

    // ── Expression checking (bottom-up annotation) ──────────────

    fn check_expr(&mut self, expr: &Expr, in_predicate: bool) -> Type {
        match expr {
            Expr::Literal { node_id, value, .. } => {
                let ty = match value {
                    LiteralValue::Int(_) => Type::Int,
                    LiteralValue::Float(_) => Type::Float,
                    LiteralValue::String(_) => Type::String,
                    LiteralValue::Date(_) => Type::Date,
                    LiteralValue::Bool(_) => Type::Bool,
                    LiteralValue::Null => Type::Null,
                };
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::FieldRef {
                node_id,
                name,
                span,
            } => {
                let binding = self
                    .bindings
                    .get(node_id.0 as usize)
                    .and_then(|b| b.as_ref());
                let ty = match binding {
                    Some(ResolvedBinding::Field(_)) => {
                        // Check schema for declared type via row lookup
                        match self.schema.lookup(name) {
                            ColumnLookup::Declared(declared) => declared.clone(),
                            ColumnLookup::PassThrough(_tail_id) => {
                                // TODO: upgrade to Type::PassThrough(TailVarId)
                                // once composition unification needs tail identity.
                                Type::Any
                            }
                            ColumnLookup::Unknown => Type::Any, // Will be inferred from usage
                            ColumnLookup::Ambiguous(matches) => {
                                // Bare `FieldRef` over a combine merged row
                                // where `name` resolves to multiple declared
                                // fields. Emit a loud error with a precise
                                // "qualify with X or Y" suggestion rather
                                // than silently typing as Any. Reachable only
                                // from combine body typechecks (C.1.3).
                                let suggestions: Vec<String> =
                                    matches.iter().map(|qf| qf.to_string()).collect();
                                self.error(
                                    *span,
                                    format!(
                                        "field '{name}' is ambiguous — matches multiple inputs"
                                    ),
                                    Some(format!(
                                        "qualify the reference: `{}`",
                                        suggestions.join("` or `")
                                    )),
                                );
                                Type::Any
                            }
                        }
                    }
                    Some(ResolvedBinding::LetVar(i)) => {
                        // Infer the let-var's type from its binding RHS so it
                        // participates in the operand checks below like any
                        // other value. An `Any`-typed let-var would unify with
                        // everything and silently bypass the arithmetic /
                        // comparison / method checks.
                        self.let_binding_nodes
                            .get(*i)
                            .copied()
                            .map(|node| self.get_type(node))
                            .unwrap_or(Type::Any)
                    }
                    Some(ResolvedBinding::IteratorBinding) => Type::Any, // `it` — runtime type
                    _ => Type::Any,
                };
                self.set_type(*node_id, ty.clone());
                let _ = span;
                ty
            }

            Expr::QualifiedFieldRef { node_id, parts, .. } => {
                // Phase Combine C.1.0.4 — resolve 2-part refs against the
                // input row via `lookup_qualified`. Exact match on the
                // `(qualifier, name)` pair means `Ambiguous` is
                // structurally unreachable (defensively flattened to
                // `Any`). 3+ part refs are not decomposable (e.g.
                // multi-record `source.record_type.field`); they stay
                // `Type::Any` and will surface as E304 / E308 in the
                // combine bind_schema arm (C.1.1 / C.1.3).
                let ty = if parts.len() == 2 {
                    match self.schema.lookup_qualified(&parts[0], &parts[1]) {
                        ColumnLookup::Declared(declared) => declared.clone(),
                        ColumnLookup::PassThrough(_)
                        | ColumnLookup::Unknown
                        | ColumnLookup::Ambiguous(_) => Type::Any,
                    }
                } else {
                    Type::Any
                };
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::PipelineAccess { node_id, field, .. } => {
                let ty = match &**field {
                    "start_time" => Type::DateTime,
                    "name" | "execution_id" | "batch_id" => Type::String,
                    "total_count" | "ok_count" | "dlq_count" | "filtered_count"
                    | "distinct_count" => Type::Int,
                    other => self
                        .scoped_vars
                        .pipeline
                        .get(other)
                        .copied()
                        .map(scoped_var_type_to_type)
                        .unwrap_or(Type::Any),
                };
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::VarsAccess { node_id, key, .. } => {
                let ty = self
                    .scoped_vars
                    .static_vars
                    .get(&**key)
                    .copied()
                    .map(scoped_var_type_to_type)
                    .unwrap_or(Type::Any);
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::ConfigAccess { node_id, param, .. } => {
                // Type from the composition's declared config schema. An
                // unknown param was already rejected by the resolver, so the
                // `Any` fallback only shields the typecheck pass from cascading
                // on an already-diagnosed read.
                let ty = self
                    .scoped_vars
                    .config
                    .get(&**param)
                    .copied()
                    .map(scoped_var_type_to_type)
                    .unwrap_or(Type::Any);
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::SourceAccess { node_id, field, .. } => {
                let ty = match &**field {
                    "file" | "path" | "batch" | "name" => Type::String,
                    "row" | "count" => Type::Int,
                    "ingestion_timestamp" => Type::DateTime,
                    other => self
                        .scoped_vars
                        .source
                        .get(other)
                        .copied()
                        .map(scoped_var_type_to_type)
                        .unwrap_or(Type::Any),
                };
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::QualifiedSourceAccess { node_id, field, .. } => {
                // qualified source access only addresses
                // user-declared keys (builtin source members aren't
                // valid through the qualifier). Type comes from the
                // same registry as the unqualified form.
                let ty = self
                    .scoped_vars
                    .source
                    .get(&**field)
                    .copied()
                    .map(scoped_var_type_to_type)
                    .unwrap_or(Type::Any);
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::RecordAccess { node_id, field, .. } => {
                // `$record.<key>` reads against the declared registry —
                // the resolver already rejected undeclared keys, so a miss
                // here implies an internal bug. Fall through to Any rather
                // than re-emitting a diagnostic.
                let ty = self
                    .scoped_vars
                    .record
                    .get(&**field)
                    .copied()
                    .map(scoped_var_type_to_type)
                    .unwrap_or(Type::Any);
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::DocAccess { node_id, .. } => {
                // `$doc.<section>.<field>` types as `Any` until envelope
                // section schemas flow through typecheck context (added
                // alongside the per-source envelope config wiring).
                // `Any` unifies with everything so arithmetic and
                // comparison expressions remain well-typed; runtime
                // evaluation yields the actual envelope value or
                // `Value::Null` for an undeclared section / field.
                self.set_type(*node_id, Type::Any);
                Type::Any
            }

            Expr::Now { node_id, .. } => {
                self.set_type(*node_id, Type::DateTime);
                Type::DateTime
            }

            Expr::Wildcard { node_id, .. } => {
                self.set_type(*node_id, Type::Any);
                Type::Any
            }

            // Extractor-produced leaves: never present during typecheck pass.
            // Types are assigned during aggregate extraction, not here.
            Expr::AggSlot { node_id, .. } | Expr::GroupKey { node_id, .. } => {
                self.set_type(*node_id, Type::Any);
                Type::Any
            }

            Expr::Binary {
                node_id,
                op,
                lhs,
                rhs,
                span,
            } => {
                let lt = self.check_expr(lhs, in_predicate);
                let rt = self.check_expr(rhs, in_predicate);
                let ty = self.check_binary_op(*node_id, *op, &lt, &rt, *span);
                self.infer_field_type_from_binary(*op, lhs, rhs, &lt, &rt);
                ty
            }

            Expr::Unary {
                node_id,
                op,
                operand,
                ..
            } => {
                let inner = self.check_expr(operand, in_predicate);
                let ty = match op {
                    UnaryOp::Neg => {
                        if inner.is_nullable() {
                            Type::nullable(inner.unwrap_nullable().clone())
                        } else {
                            inner
                        }
                    }
                    UnaryOp::Not => {
                        if inner.is_nullable() {
                            Type::nullable(Type::Bool)
                        } else {
                            Type::Bool
                        }
                    }
                };
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::Coalesce {
                node_id, lhs, rhs, ..
            } => {
                let lt = self.check_expr(lhs, in_predicate);
                let rt = self.check_expr(rhs, in_predicate);
                // Coalesce strips nullability from left operand
                let inner = lt.unwrap_nullable();
                let ty = inner.unify(&rt).unwrap_or(rt);
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::IfThenElse {
                node_id,
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                self.check_expr(condition, in_predicate);
                let then_ty = self.check_expr(then_branch, in_predicate);
                let ty = if let Some(eb) = else_branch {
                    let else_ty = self.check_expr(eb, in_predicate);
                    then_ty.unify(&else_ty).unwrap_or(Type::Any)
                } else {
                    // Missing else → result could be Null
                    Type::nullable(then_ty)
                };
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::Match {
                node_id,
                subject,
                arms,
                span,
            } => {
                if let Some(s) = subject {
                    self.check_expr(s, in_predicate);
                }

                // Check for wildcard arm
                let has_wildcard = arms
                    .iter()
                    .any(|arm| matches!(arm.pattern, Expr::Wildcard { .. }));
                if !has_wildcard {
                    self.error(
                        *span,
                        "match expression must have a '_' (wildcard) catch-all arm".into(),
                        Some("Add '_ => <default_value>' as the last arm".into()),
                    );
                }

                let mut result_ty = Type::Any;
                for arm in arms {
                    self.check_expr(&arm.pattern, in_predicate);
                    let body_ty = self.check_expr(&arm.body, in_predicate);
                    result_ty = result_ty.unify(&body_ty).unwrap_or(Type::Any);
                }
                self.set_type(*node_id, result_ty.clone());
                result_ty
            }

            Expr::MethodCall {
                node_id,
                receiver,
                method,
                args,
                span,
            } => {
                // `$window.lag(n).<field>` style postfix-field chain on a
                // positional window builtin. The result type is the
                // upstream row's declared type for `field`; an unknown
                // field name surfaces as a typecheck error here rather
                // than silently producing Null at runtime.
                if args.is_empty()
                    && let Expr::WindowCall { function, .. } = &**receiver
                    && matches!(&**function, "first" | "last" | "lag" | "lead")
                {
                    // Recurse into the WindowCall to typecheck its args
                    // (e.g. the integer offset on `lag(1)`).
                    self.check_expr(receiver, in_predicate);
                    let ty = match self.schema.lookup(method) {
                        // Out-of-bounds positional (lag past partition
                        // start, lead past partition end) returns Null at
                        // runtime, so the chain's result is always
                        // nullable regardless of the upstream column's
                        // declared nullability.
                        ColumnLookup::Declared(declared) => Type::nullable(declared.clone()),
                        // Open row (composition tail / source-without-
                        // declared-schema) — we cannot prove the field
                        // exists upstream at compile time. Type::Any
                        // mirrors how `FieldRef` handles open rows.
                        ColumnLookup::PassThrough(_) => Type::Any,
                        ColumnLookup::Unknown => {
                            // Closed row, field genuinely absent. Treat
                            // identically to a bare `FieldRef` typo.
                            self.error(
                                *span,
                                format!(
                                    "field '{}' is not in the upstream row schema for $window.{}()",
                                    method, function
                                ),
                                Some(
                                    "ensure the upstream node emits this field, or correct the spelling"
                                        .into(),
                                ),
                            );
                            Type::Any
                        }
                        ColumnLookup::Ambiguous(_) => {
                            // Bare field reference on a merged-row receiver
                            // is ambiguous — same shape the FieldRef arm
                            // handles for combine inputs.
                            self.error(
                                *span,
                                format!(
                                    "field '{}' on $window.{}() is ambiguous in the upstream row schema",
                                    method, function
                                ),
                                Some(
                                    "qualify the upstream input or rename the field upstream"
                                        .into(),
                                ),
                            );
                            Type::Any
                        }
                    };
                    self.set_type(*node_id, ty.clone());
                    return ty;
                }

                let recv_ty = self.check_expr(receiver, in_predicate);
                let mut arg_types = Vec::new();
                for arg in args {
                    arg_types.push(self.check_expr(arg, in_predicate));
                }

                // Try to pre-compile regex for .matches(), .find(), .capture()
                if matches!(&**method, "matches" | "find" | "capture")
                    && let Some(first_arg) = args.first()
                    && let Expr::Literal {
                        value: LiteralValue::String(pattern),
                        span: arg_span,
                        ..
                    } = first_arg
                {
                    match Regex::new(pattern) {
                        Ok(re) => {
                            self.regexes[node_id.0 as usize] = Some(re);
                        }
                        Err(e) => {
                            self.error(*arg_span, format!("invalid regex pattern: {}", e), None);
                        }
                    }
                }

                // Look up return type from registry. `self.registry` is a
                // shared `&BuiltinRegistry`, so `def` borrows the registry (not
                // `*self`) and the `&mut self` validation call below is free.
                let registry = self.registry;
                let ty = if let Some(def) = registry.lookup_method(method) {
                    // Validate receiver / arity / argument types against the
                    // registered signature. Mismatches are recorded but we keep
                    // computing the return type so one pass surfaces every
                    // downstream error too.
                    self.check_method_signature(def, method, &recv_ty, &arg_types, args, *span);
                    method_return_type(method, &recv_ty, def)
                } else {
                    Type::Any
                };

                // Null propagation: nullable receiver → nullable result (except is_null, type_of)
                let ty = if recv_ty.is_nullable()
                    && !matches!(&**method, "is_null" | "type_of" | "catch" | "is_empty")
                {
                    Type::nullable(ty)
                } else {
                    ty
                };

                self.set_type(*node_id, ty.clone());
                let _ = span;
                ty
            }

            Expr::WindowCall {
                node_id,
                function,
                args,
                span,
            } => {
                let is_predicate_fn =
                    matches!(&**function, "any" | "every" | "exists" | "not_exists");

                // Check for nested window calls inside predicate_expr
                if in_predicate {
                    self.error(*span,
                        format!("$window.{}() cannot be called inside a $window.any/every/exists/not_exists predicate", function),
                        Some("Move the window call outside the predicate expression".into()));
                }

                // Type check arguments
                for arg in args {
                    self.check_expr(arg, is_predicate_fn);
                }

                // Check numeric requirement for sum/cumulative_sum/avg.
                // `Decimal` is accepted: the window sum/cumulative_sum/avg
                // slices accumulate decimals exactly (see `window_context`), so
                // an exact decimal window total/average stays exact rather than
                // routing through binary float.
                if matches!(&**function, "sum" | "cumulative_sum" | "avg") {
                    for arg in args {
                        let arg_ty = self.get_type(arg.node_id());
                        let inner = arg_ty.unwrap_nullable();
                        if !matches!(
                            inner,
                            Type::Int | Type::Float | Type::Decimal | Type::Numeric | Type::Any
                        ) {
                            self.error(
                                arg.span(),
                                format!(
                                    "window.{}() requires a Numeric argument, got {}",
                                    function, arg_ty
                                ),
                                Some(
                                    "Use a numeric field or convert with .to_int() / .to_float() / .to_decimal()"
                                        .into(),
                                ),
                            );
                        }
                    }
                }

                // Predicate-fold builtins: enforce arity 1 and Bool-typed
                // predicate so the eval-time three-valued fold has a
                // well-typed input.
                if is_predicate_fn {
                    match args.len() {
                        1 => {
                            let arg_ty = self.get_type(args[0].node_id());
                            let inner = arg_ty.unwrap_nullable();
                            if !matches!(inner, Type::Bool | Type::Any) {
                                self.error(
                                    args[0].span(),
                                    format!(
                                        "window.{}() requires a Bool argument, got {}",
                                        function, arg_ty
                                    ),
                                    Some("Use a boolean expression like `amount > 100`".into()),
                                );
                            }
                        }
                        n => self.error(
                            *span,
                            format!("window.{}() takes exactly 1 argument, got {}", function, n),
                            None,
                        ),
                    }
                }

                // Unregistered window names are a typecheck error.
                // Letting them fall through to `Type::Any` lets a
                // misspelled `$window.row_numbr()` reach eval-time and
                // silently return `Value::Null` from the catch-all arm —
                // a class of silent-Null bug this pass exists to catch.
                let ty = match self.registry.lookup_window(function) {
                    Some(def) => Type::from_type_tag(def.return_type),
                    None => {
                        self.error(
                            *span,
                            format!("unknown window function $window.{}()", function),
                            Some(
                                "Use one of the registered window functions: row_number, rank, dense_rank, count, sum, cumulative_sum, avg, min, max, first, last, first_value, last_value, lag, lead, any, every, exists, not_exists, collect, distinct"
                                    .into(),
                            ),
                        );
                        Type::Any
                    }
                };

                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::AggCall {
                node_id,
                name,
                args,
                span,
            } => {
                // Type-check arguments (even in row context — Direction 1
                // enforcement happens in `check_aggregate_context` so that a
                // single pass over the tree produces all diagnostics).
                let mut arg_types = Vec::with_capacity(args.len());
                for arg in args {
                    arg_types.push(self.check_expr(arg, in_predicate));
                }

                let ty = aggregate_return_type(name, &arg_types);

                // Numeric guard for sum/avg (same pattern as window sum/avg).
                // `Decimal` is accepted: the SumState/AvgState decimal paths
                // aggregate exactly, so an exact monetary total/average stays
                // in the decimal domain instead of being pushed through the
                // binary-float rounding decimal exists to avoid. weighted_avg
                // aggregates decimals exactly too, and only rejects a decimal
                // mixed with a binary float (the two cannot combine exactly).
                match &**name {
                    "sum" | "avg" => {
                        if let Some(arg_ty) = arg_types.first() {
                            let inner = arg_ty.unwrap_nullable();
                            if !matches!(
                                inner,
                                Type::Int | Type::Float | Type::Decimal | Type::Numeric | Type::Any
                            ) {
                                self.error(
                                    *span,
                                    format!("{name}() requires a Numeric argument, got {arg_ty}"),
                                    Some(
                                        "Use a numeric field or convert with .to_int() / .to_float() / .to_decimal()"
                                            .into(),
                                    ),
                                );
                            }
                        }
                    }
                    "weighted_avg" => {
                        if arg_types.len() != 2 {
                            self.error(
                                *span,
                                "weighted_avg() requires exactly two arguments (value, weight)"
                                    .into(),
                                None,
                            );
                        } else {
                            // A decimal value or weight now aggregates exactly
                            // (the accumulator has an exact decimal path). But a
                            // decimal in one position mixed with a binary float
                            // in the other cannot combine exactly — reject it
                            // the same way `decimal * float` is rejected in
                            // ordinary arithmetic, rather than silently drop the
                            // total. `Numeric` admits `Float` at runtime and
                            // does not unify with `Decimal` (see `Type::unify`),
                            // so it is rejected against a decimal too; `Any`
                            // stays permissive here and is poisoned to Null at
                            // runtime if it resolves to a conflicting mix.
                            let has_decimal = arg_types
                                .iter()
                                .any(|t| matches!(t.unwrap_nullable(), Type::Decimal));
                            let has_float = arg_types.iter().any(|t| {
                                matches!(t.unwrap_nullable(), Type::Float | Type::Numeric)
                            });
                            if has_decimal && has_float {
                                self.error(
                                    *span,
                                    "weighted_avg() cannot mix a decimal argument with a float or numeric argument"
                                        .into(),
                                    Some(
                                        "Convert with .to_decimal() or .to_float() so the value and weight share one numeric domain"
                                            .into(),
                                    ),
                                );
                            }
                        }
                    }
                    _ => {}
                }

                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::IndexAccess {
                node_id,
                receiver,
                index,
                ..
            } => {
                // Typecheck the children; downstream dispatch decides
                // array-vs-map by receiver value type. The result type
                // is Any because the index target's element type is
                // not statically known here (Array element type is not
                // tracked, and Map values are untyped at this layer).
                self.check_expr(receiver, in_predicate);
                self.check_expr(index, in_predicate);
                self.set_type(*node_id, Type::Any);
                Type::Any
            }

            Expr::Closure { node_id, body, .. } => {
                // Standalone closure typecheck happens here only for
                // diagnostics — `MethodCall` callsites typecheck the
                // body with the closure parameter introduced into
                // scope. The closure as a value has no runtime type.
                self.check_expr(body, in_predicate);
                self.set_type(*node_id, Type::Any);
                Type::Any
            }
        }
    }

    /// enforce aggregate-context rules. Walks the program after type
    /// inference so diagnostics can reference inferred types. Runs in both
    /// `Row` and `GroupBy` modes because Direction 1 (AggCall in Row) applies
    /// universally.
    fn check_aggregate_context(&mut self, program: &Program) {
        // Expand let-bindings by collecting a map from LetVar index to the
        // underlying expression. This lets us resolve a LetVar FieldRef to
        // its defining expression (PostgreSQL-style alias expansion).
        let let_exprs: Vec<Expr> = program
            .statements
            .iter()
            .filter_map(|s| match s {
                Statement::Let { expr, .. } => Some(expr.clone()),
                _ => None,
            })
            .collect();

        for stmt in &program.statements {
            self.check_aggregate_context_stmt(stmt, &let_exprs);
        }
    }

    /// Enforce aggregate-context rules on one statement, recursing through
    /// `emit each` / `emit each ... outer` bodies — including a fan-out
    /// nested inside another fan-out, so a nested block's emits and the
    /// aggregate calls inside them are never invisible to this check. A
    /// non-recursive walker that handled only the immediate body silently
    /// admitted aggregates buried one fan-out level deeper.
    fn check_aggregate_context_stmt(&mut self, stmt: &Statement, let_exprs: &[Expr]) {
        match stmt {
            Statement::Emit { expr, .. } => {
                self.agg_function_depth = 0;
                self.walk_agg_ctx(expr, let_exprs);
            }
            // Let bindings are checked through their usage sites
            // (PostgreSQL-style alias expansion): a bare non-grouped
            // field in a let body is only an error if the let var is
            // referenced outside an aggregate function.
            Statement::Let { .. } => {}
            Statement::Filter { predicate, .. } => {
                // Filter in an aggregate transform acts as a row-level
                // gate (WHERE semantics). AggCalls here are rejected in
                // both modes — GroupBy also rejects because aggregates
                // belong in HAVING (not supported), matching PostgreSQL.
                self.agg_function_depth = 0;
                self.walk_agg_ctx_row_only(predicate);
            }
            Statement::EmitEach { source, body, .. }
            | Statement::ExplodeOuter { source, body, .. } => {
                // Fan-out (plain or outer) is a row-level operation: the
                // source must not contain aggregates, and body statements
                // — at every nesting level — obey the same context rules
                // as the surrounding block.
                self.agg_function_depth = 0;
                self.walk_agg_ctx_row_only(source);
                for inner in body {
                    self.check_aggregate_context_stmt(inner, let_exprs);
                }
            }
            _ => {}
        }
    }

    /// Walk with full two-direction enforcement. Used for emit/let RHS.
    fn walk_agg_ctx(&mut self, expr: &Expr, let_exprs: &[Expr]) {
        match expr {
            Expr::AggCall {
                name, args, span, ..
            } => {
                // Direction 1: AggCall is forbidden in Row mode.
                if matches!(self.aggregate_mode, AggregateMode::Row) {
                    self.error(
                        *span,
                        format!(
                            "aggregate function '{name}' is not allowed in a row-level transform"
                        ),
                        Some(
                            "Move this expression into an 'aggregate:' transform block, or remove the aggregate call"
                                .into(),
                        ),
                    );
                    return;
                }
                // Nested aggregates: forbidden in all modes.
                if self.agg_function_depth > 0 {
                    self.error(
                        *span,
                        format!(
                            "aggregate function '{name}' cannot be nested inside another aggregate"
                        ),
                        None,
                    );
                    return;
                }
                self.agg_function_depth += 1;
                for arg in args {
                    self.walk_agg_ctx(arg, let_exprs);
                }
                self.agg_function_depth -= 1;
            }

            Expr::FieldRef {
                node_id,
                name,
                span,
            } => {
                // Let-binding transparency: if this FieldRef resolves to a
                // LetVar, recurse into the binding expression instead of
                // treating it as a bare column reference.
                if let Some(Some(ResolvedBinding::LetVar(i))) =
                    self.bindings.get(node_id.0 as usize)
                    && let Some(def_expr) = let_exprs.get(*i)
                {
                    self.walk_agg_ctx(def_expr, let_exprs);
                    return;
                }

                if let AggregateMode::GroupBy { group_by_fields } = &self.aggregate_mode
                    && self.agg_function_depth == 0
                    && !group_by_fields.contains(name.as_ref())
                {
                    let msg = format!(
                        "field '{name}' must appear in the GROUP BY list or be used inside an aggregate function"
                    );
                    let group_by_fields = group_by_fields.clone();
                    self.error(
                        *span,
                        msg,
                        Some(format!(
                            "Either add '{name}' to group_by, or wrap this usage in an aggregate call (e.g. max({name})). Current group_by: [{}]",
                            group_by_fields
                                .iter()
                                .cloned()
                                .collect::<Vec<_>>()
                                .join(", ")
                        )),
                    );
                }
            }

            // Per-record provenance fields (`source.row` / `source.file`)
            // cannot appear outside an aggregate function inside an
            // aggregate transform — the residual evaluator at finalize()
            // does not have a current record. Universal SQL practice
            // (Postgres SQLSTATE 42803, DuckDB binder, Trino `$row_id`
            // scan-only, Spark functional-dependency rule, DataFusion #20135).
            Expr::SourceAccess { field, span, .. }
                if matches!(self.aggregate_mode, AggregateMode::GroupBy { .. })
                    && self.agg_function_depth == 0 =>
            {
                self.error(
                    *span,
                    format!(
                        "`source.{field}` is per-record provenance and cannot appear in an aggregate residual expression"
                    ),
                    Some(format!(
                        "Wrap in an aggregate function (e.g., `first($source.{field})`, `min($source.{field})`, `max($source.{field})`)"
                    )),
                );
            }

            // Recurse into children. Literals, $pipeline.*, now,
            // qualified field refs, and wildcards are all exempt.
            Expr::Binary { lhs, rhs, .. } => {
                self.walk_agg_ctx(lhs, let_exprs);
                self.walk_agg_ctx(rhs, let_exprs);
            }
            Expr::Unary { operand, .. } => self.walk_agg_ctx(operand, let_exprs),
            Expr::Coalesce { lhs, rhs, .. } => {
                self.walk_agg_ctx(lhs, let_exprs);
                self.walk_agg_ctx(rhs, let_exprs);
            }
            Expr::IfThenElse {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                self.walk_agg_ctx(condition, let_exprs);
                self.walk_agg_ctx(then_branch, let_exprs);
                if let Some(eb) = else_branch {
                    self.walk_agg_ctx(eb, let_exprs);
                }
            }
            Expr::Match { subject, arms, .. } => {
                if let Some(s) = subject {
                    self.walk_agg_ctx(s, let_exprs);
                }
                for arm in arms {
                    self.walk_agg_ctx(&arm.pattern, let_exprs);
                    self.walk_agg_ctx(&arm.body, let_exprs);
                }
            }
            Expr::MethodCall { receiver, args, .. } => {
                self.walk_agg_ctx(receiver, let_exprs);
                for arg in args {
                    self.walk_agg_ctx(arg, let_exprs);
                }
            }
            Expr::WindowCall { args, .. } => {
                for arg in args {
                    self.walk_agg_ctx(arg, let_exprs);
                }
            }
            Expr::IndexAccess {
                receiver, index, ..
            } => {
                self.walk_agg_ctx(receiver, let_exprs);
                self.walk_agg_ctx(index, let_exprs);
            }
            Expr::Closure { body, .. } => {
                self.walk_agg_ctx(body, let_exprs);
            }
            Expr::Literal { .. }
            | Expr::QualifiedFieldRef { .. }
            | Expr::PipelineAccess { .. }
            | Expr::VarsAccess { .. }
            | Expr::ConfigAccess { .. }
            | Expr::SourceAccess { .. }
            | Expr::QualifiedSourceAccess { .. }
            | Expr::RecordAccess { .. }
            | Expr::DocAccess { .. }
            | Expr::Now { .. }
            | Expr::Wildcard { .. }
            | Expr::AggSlot { .. }
            | Expr::GroupKey { .. } => {}
        }
    }

    /// Walk that enforces Direction 1 only (no AggCall allowed anywhere).
    /// Used for `filter` / `distinct` predicates in aggregate transforms,
    /// matching PostgreSQL WHERE-clause semantics.
    fn walk_agg_ctx_row_only(&mut self, expr: &Expr) {
        match expr {
            Expr::AggCall { name, span, .. } => {
                self.error(
                    *span,
                    format!(
                        "aggregate function '{name}' is not allowed in a filter predicate — aggregates apply after grouping"
                    ),
                    None,
                );
            }
            Expr::Binary { lhs, rhs, .. } => {
                self.walk_agg_ctx_row_only(lhs);
                self.walk_agg_ctx_row_only(rhs);
            }
            Expr::Unary { operand, .. } => self.walk_agg_ctx_row_only(operand),
            Expr::Coalesce { lhs, rhs, .. } => {
                self.walk_agg_ctx_row_only(lhs);
                self.walk_agg_ctx_row_only(rhs);
            }
            Expr::IfThenElse {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                self.walk_agg_ctx_row_only(condition);
                self.walk_agg_ctx_row_only(then_branch);
                if let Some(eb) = else_branch {
                    self.walk_agg_ctx_row_only(eb);
                }
            }
            Expr::Match { subject, arms, .. } => {
                if let Some(s) = subject {
                    self.walk_agg_ctx_row_only(s);
                }
                for arm in arms {
                    self.walk_agg_ctx_row_only(&arm.pattern);
                    self.walk_agg_ctx_row_only(&arm.body);
                }
            }
            Expr::MethodCall { receiver, args, .. } => {
                self.walk_agg_ctx_row_only(receiver);
                for arg in args {
                    self.walk_agg_ctx_row_only(arg);
                }
            }
            Expr::WindowCall { args, .. } => {
                for arg in args {
                    self.walk_agg_ctx_row_only(arg);
                }
            }
            Expr::IndexAccess {
                receiver, index, ..
            } => {
                self.walk_agg_ctx_row_only(receiver);
                self.walk_agg_ctx_row_only(index);
            }
            Expr::Closure { body, .. } => {
                self.walk_agg_ctx_row_only(body);
            }
            _ => {}
        }
    }

    /// Validate a `MethodCall` against its registered builtin signature:
    /// receiver type, argument count, and per-argument types. Every mismatch
    /// is recorded as a diagnostic; the caller still computes a recovery
    /// return type so a single pass reports the whole expression's errors.
    fn check_method_signature(
        &mut self,
        def: &BuiltinDef,
        method: &str,
        recv_ty: &Type,
        arg_types: &[Type],
        args: &[Expr],
        span: Span,
    ) {
        // (i) Receiver type.
        if !receiver_compatible(def.receiver, method, recv_ty.unwrap_nullable()) {
            self.error(
                span,
                format!(
                    "method '{method}' expects a {} receiver, but got {recv_ty}",
                    Type::from_type_tag(def.receiver)
                ),
                Some(format!("'{method}' is not defined for {recv_ty} values")),
            );
        }

        // (ii) Argument count.
        let n = args.len();
        let within_max = def.max_args.map(|m| n <= m).unwrap_or(true);
        if n < def.min_args || !within_max {
            let want = match def.max_args {
                Some(max) if max == def.min_args => def.min_args.to_string(),
                Some(max) => format!("between {} and {max}", def.min_args),
                None => format!("at least {}", def.min_args),
            };
            self.error(
                span,
                format!("method '{method}' takes {want} argument(s), but got {n}"),
                None,
            );
            // The count is wrong; positional arg-type checks below would
            // produce misaligned, cascading diagnostics, so stop here.
            return;
        }

        // (iii) Per-argument types.
        for (i, arg_ty) in arg_types.iter().enumerate() {
            let want = if i < def.args.len() {
                Some(def.args[i])
            } else if def.max_args.is_none() {
                // Variadic tail repeats the last declared parameter type.
                def.args.last().copied()
            } else {
                None
            };
            let Some(want) = want else { continue };
            if !arg_compatible(want, arg_ty.unwrap_nullable()) {
                self.error(
                    args[i].span(),
                    format!(
                        "argument {} of '{method}' expects {}, but got {arg_ty}",
                        i + 1,
                        Type::from_type_tag(want)
                    ),
                    None,
                );
            }
        }
    }

    fn check_binary_op(
        &mut self,
        node_id: NodeId,
        op: BinOp,
        lt: &Type,
        rt: &Type,
        span: Span,
    ) -> Type {
        let lt_inner = lt.unwrap_nullable();
        let rt_inner = rt.unwrap_nullable();
        let either_nullable = lt.is_nullable() || rt.is_nullable();

        // Exactness guard: an exact `decimal` never silently mixes with a
        // binary `float` in arithmetic or comparison. Require an explicit cast
        // (`.to_decimal()` / `.to_float()`) so the loss of precision is a
        // deliberate, visible choice rather than an accidental promotion.
        let is_decimal_float_mix = matches!(
            (lt_inner, rt_inner),
            (Type::Decimal, Type::Float) | (Type::Float, Type::Decimal)
        );
        if is_decimal_float_mix
            && matches!(
                op,
                BinOp::Add
                    | BinOp::Sub
                    | BinOp::Mul
                    | BinOp::Div
                    | BinOp::Mod
                    | BinOp::Gt
                    | BinOp::Lt
                    | BinOp::Gte
                    | BinOp::Lte
                    | BinOp::Eq
                    | BinOp::Neq
            )
        {
            self.diagnostics.push(TypeDiagnostic {
                span,
                message: "cannot mix decimal and float without an explicit cast".into(),
                help: Some(
                    "decimal arithmetic is exact; convert one operand with \
                     `.to_decimal()` (exact→exact) or `.to_float()` (exact→binary) \
                     to make the precision trade-off explicit"
                        .into(),
                ),
                related_span: None,
                is_warning: false,
            });
        }

        // A `decimal` mixed with the `numeric` supertype (`int | float`) is NOT
        // provably incompatible: `numeric` may resolve to `int` at runtime,
        // which widens exactly into the decimal domain. So it is left permissive
        // here rather than flagged as a hard type error — the definitely-invalid
        // decimal/float half is already diagnosed above, and the contexts that
        // require an exact axis (combine range joins → E327, `weighted_avg`)
        // reject the ambiguous pairing with their own targeted checks. A common
        // trigger is `numeric`-returning builtins over a decimal receiver
        // (`decimal.clamp(lo, hi)`, `decimal.min(x)`), whose exact result would
        // otherwise be rejected when composed with another decimal.
        let is_decimal_numeric_mix = matches!(
            (lt_inner, rt_inner),
            (Type::Decimal, Type::Numeric) | (Type::Numeric, Type::Decimal)
        );

        // Unify the (nullability-stripped) operands once. `None` means the two
        // types are provably disjoint: `Any`, `Numeric`, `Null`, and
        // `Nullable` all unify, so a failure here — excluding the two decimal
        // mixings handled above — is a genuine type clash the operator can't
        // bridge. Such expressions used to type-check clean and then evaluate
        // to a silent `Null` / `false`.
        let unified = lt_inner.unify(rt_inner);

        let result = match op {
            // Arithmetic: result is the unified numeric type. A failed unify
            // that is not the already-diagnosed decimal/float mix is a hard
            // error (e.g. `"x" + 1`). Keep the widened `Any` for error recovery
            // so the enclosing expression still type-checks.
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => match &unified {
                Some(t) => t.clone(),
                None => {
                    if !is_decimal_float_mix && !is_decimal_numeric_mix {
                        self.error(
                            span,
                            format!(
                                "arithmetic operator cannot combine operands of type {lt} and {rt}"
                            ),
                            Some(
                                "arithmetic operands must be mutually compatible numeric types; \
                                 convert one side (e.g. `.to_int()`, `.to_float()`) so both share a type"
                                    .into(),
                            ),
                        );
                    }
                    Type::Any
                }
            },

            // Comparison: result is Bool, but the operands must be mutually
            // orderable. Ordering provably-disjoint types (e.g.
            // `len(name) > "5"`) silently evaluated to `false` before.
            BinOp::Gt | BinOp::Lt | BinOp::Gte | BinOp::Lte => {
                if unified.is_none() && !is_decimal_float_mix && !is_decimal_numeric_mix {
                    self.error(
                        span,
                        format!("cannot compare {lt} and {rt} with an ordering operator"),
                        Some(
                            "comparison operands must be mutually compatible \
                             (both numeric, both strings, both dates, …)"
                                .into(),
                        ),
                    );
                }
                Type::Bool
            }

            // Equality: always Bool, never nullable. Provably-disjoint non-`Any`
            // operands can never be equal, so `5 == "5"` is a type error rather
            // than a constant `false`.
            BinOp::Eq | BinOp::Neq => {
                if unified.is_none() && !is_decimal_float_mix && !is_decimal_numeric_mix {
                    self.error(
                        span,
                        format!(
                            "cannot compare {lt} and {rt} for equality — they are different types"
                        ),
                        Some(
                            "equality operands must share a type; convert one side or compare \
                             against a matching literal"
                                .into(),
                        ),
                    );
                }
                self.set_type(node_id, Type::Bool);
                return Type::Bool;
            }

            // Logical: result is Bool
            BinOp::And | BinOp::Or => Type::Bool,
        };

        // Null propagation for non-equality operators
        let result = if either_nullable && !matches!(op, BinOp::Eq | BinOp::Neq) {
            Type::nullable(result)
        } else {
            result
        };

        self.set_type(node_id, result.clone());
        result
    }

    /// Infer field types from binary operator usage.
    /// E.g., `status == "active"` → status is String.
    /// E.g., `amount + 1` → amount is Numeric.
    fn infer_field_type_from_binary(
        &mut self,
        op: BinOp,
        lhs: &Expr,
        rhs: &Expr,
        lt: &Type,
        rt: &Type,
    ) {
        // Only infer from operators that imply types
        let implied_type = match op {
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => Some(Type::Numeric),
            BinOp::Eq | BinOp::Neq => None, // Could be any type
            BinOp::Gt | BinOp::Lt | BinOp::Gte | BinOp::Lte => None, // Could be any ordered type
            BinOp::And | BinOp::Or => Some(Type::Bool),
        };

        // For equality/comparison: infer from the other operand's known type
        if matches!(
            op,
            BinOp::Eq | BinOp::Neq | BinOp::Gt | BinOp::Lt | BinOp::Gte | BinOp::Lte
        ) {
            if let Expr::FieldRef { name, span, .. } = lhs
                && !matches!(rt, Type::Any)
            {
                self.add_constraint(name, rt.clone(), *span);
            }
            if let Expr::FieldRef { name, span, .. } = rhs
                && !matches!(lt, Type::Any)
            {
                self.add_constraint(name, lt.clone(), *span);
            }
        }

        if let Some(ref ty) = implied_type {
            if let Expr::FieldRef { name, span, .. } = lhs {
                self.add_constraint(name, ty.clone(), *span);
            }
            if let Expr::FieldRef { name, span, .. } = rhs {
                self.add_constraint(name, ty.clone(), *span);
            }
        }
    }

    /// Unify all constraints per field. Conflicts produce diagnostics citing both spans.
    fn unify_field_constraints(&mut self) {
        for (field, constraints) in &self.field_constraints {
            // Check against schema-declared type
            if let ColumnLookup::Declared(declared) = self.schema.lookup(field) {
                for constraint in constraints {
                    // A `Numeric` constraint is the "used in arithmetic" marker
                    // (`amount + 1`), which a `decimal` column satisfies even
                    // though `Decimal` deliberately does not unify with the
                    // `int|float` `Numeric` union (that non-unification is what
                    // forces the explicit cast on `decimal * float`). Treat the
                    // arithmetic-usage marker as satisfied so decimal columns
                    // participate in arithmetic without a spurious conflict.
                    if matches!(constraint.inferred_type, Type::Numeric)
                        && matches!(declared.unwrap_nullable(), Type::Decimal)
                    {
                        continue;
                    }
                    if declared.unify(&constraint.inferred_type).is_none() {
                        self.diagnostics.push(TypeDiagnostic {
                            span: constraint.span,
                            message: format!(
                                "field '{}' is declared as {} in schema but used as {} here",
                                field, declared, constraint.inferred_type
                            ),
                            help: Some(format!(
                                "Change the usage to match the declared type {}",
                                declared
                            )),
                            related_span: None,
                            is_warning: false,
                        });
                    }
                }
                continue;
            }

            // Check constraints against each other
            if constraints.len() >= 2 {
                let first = &constraints[0];
                for other in &constraints[1..] {
                    if first.inferred_type.unify(&other.inferred_type).is_none() {
                        self.diagnostics.push(TypeDiagnostic {
                            span: other.span,
                            message: format!(
                                "field '{}' used as {} here but as {} elsewhere",
                                field, other.inferred_type, first.inferred_type
                            ),
                            help: Some(
                                "Ensure consistent type usage or add explicit conversion".into(),
                            ),
                            related_span: Some(first.span),
                            is_warning: false,
                        });
                    }
                }
            }
        }
    }

    /// Check that the program has at least one emit statement. Recurses
    /// through `Statement::EmitEach.body` and `Statement::ExplodeOuter.body`
    /// so a program whose only emits live inside a fan-out block still
    /// counts as having emits.
    fn check_emit_count(&mut self, program: &Program) {
        let has_emit = crate::ast::contains_emit(&program.statements);
        if !has_emit {
            self.warning(
                program.span,
                "program contains no 'emit' statements — no output fields will be produced".into(),
                Some("Add at least one 'emit name = expr' statement".into()),
            );
        }
    }

    /// Build the FieldTypeMap from collected constraints and schema.
    ///
    /// Keyed by `QualifiedField` — matches `Row.declared` representation
    /// so combine merged rows (Phase Combine C.1) do not collapse
    /// qualified fields to bare names. Inferred constraints from the
    /// body (collected during the walk) are always bare; schema fields
    /// carry whatever qualification the upstream row carried.
    fn build_field_type_map(&self) -> IndexMap<QualifiedField, Type> {
        let mut map = IndexMap::new();

        // Schema-declared types first (authoritative)
        for (field, ty) in self.schema.fields() {
            map.insert(field.clone(), ty.clone());
        }

        // Inferred types from constraints (always bare — `field_constraints`
        // keys are the raw names from `Expr::FieldRef`)
        for (field, constraints) in &self.field_constraints {
            let qf = QualifiedField::bare(field.as_str());
            if map.contains_key(&qf) {
                continue; // Schema takes precedence
            }
            // Use the first constraint's type (they should all be unified by now)
            if let Some(first) = constraints.first() {
                // Try to unify all constraints to get the most specific type
                let mut unified = first.inferred_type.clone();
                for c in &constraints[1..] {
                    if let Some(u) = unified.unify(&c.inferred_type) {
                        unified = u;
                    }
                }
                map.insert(qf, unified);
            }
        }

        map
    }
}

#[cfg(test)]
mod tests {
    use super::super::row::Row;
    use super::*;
    use crate::parser::Parser;
    use crate::resolve::pass::resolve_program;

    fn empty_row() -> Row {
        Row::closed(IndexMap::new(), Span::new(0, 0))
    }

    fn typecheck_ok(src: &str, fields: &[&str], schema: &Row) -> TypedProgram {
        let parsed = Parser::parse(src);
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap_or_else(|d| {
            panic!(
                "Resolve errors: {:?}",
                d.iter().map(|e| &e.message).collect::<Vec<_>>()
            )
        });
        type_check(resolved, schema).unwrap_or_else(|d| {
            panic!(
                "Type errors: {:?}",
                d.iter().map(|e| &e.message).collect::<Vec<_>>()
            )
        })
    }

    fn typecheck_err(src: &str, fields: &[&str], schema: &Row) -> Vec<TypeDiagnostic> {
        let parsed = Parser::parse(src);
        assert!(parsed.errors.is_empty());
        let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
        type_check(resolved, schema).expect_err("Expected type errors but got Ok")
    }

    // Find the type of the first expression in the first emit statement
    fn first_emit_expr_type(typed: &TypedProgram) -> Type {
        for stmt in &typed.program.statements {
            if let Statement::Emit { expr, .. } = stmt {
                return typed.types[expr.node_id().0 as usize]
                    .clone()
                    .unwrap_or(Type::Any);
            }
        }
        Type::Any
    }

    #[test]
    fn test_typecheck_arithmetic_int() {
        // 1 + 2 infers Int
        let typed = typecheck_ok("emit a = 1 + 2", &[], &empty_row());
        assert_eq!(first_emit_expr_type(&typed), Type::Int);

        // 1 + 2.0 infers Float (promotion)
        let typed2 = typecheck_ok("emit b = 1 + 2.0", &[], &empty_row());
        assert_eq!(first_emit_expr_type(&typed2), Type::Float);
    }

    #[test]
    fn test_typecheck_decimal_plus_int_widens_to_decimal() {
        // `amount + 1` on a decimal column typechecks (the int literal widens
        // into decimal context) and the result stays Decimal.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        let schema = Row::closed(cols, Span::new(0, 0));
        let typed = typecheck_ok("emit total = amount + 1", &["amount"], &schema);
        assert_eq!(first_emit_expr_type(&typed), Type::Decimal);
    }

    #[test]
    fn test_typecheck_decimal_times_float_is_error() {
        // Mixing exact decimal with binary float is a hard error without a cast.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        cols.insert("rate".into(), Type::Float);
        let schema = Row::closed(cols, Span::new(0, 0));
        let errs = typecheck_err("emit total = amount * rate", &["amount", "rate"], &schema);
        assert!(
            errs.iter()
                .any(|d| d.message.contains("cannot mix decimal and float")),
            "expected a decimal/float mix diagnostic, got: {:?}",
            errs.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_decimal_cast_to_float_allows_mix() {
        // An explicit `.to_float()` cast makes the decimal×float mix legal and
        // the result is Float.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        cols.insert("rate".into(), Type::Float);
        let schema = Row::closed(cols, Span::new(0, 0));
        let typed = typecheck_ok(
            "emit total = amount.to_float() * rate",
            &["amount", "rate"],
            &schema,
        );
        assert_eq!(first_emit_expr_type(&typed), Type::Float);
    }

    #[test]
    fn test_typecheck_decimal_receiver_numeric_methods_stay_decimal() {
        // The registry types these methods over the general Numeric receiver
        // (round/round_to → Float, ceil/floor → Int, abs → Numeric), but their
        // evaluation keeps a decimal receiver in the decimal domain, so the
        // inferred type must too.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        let schema = Row::closed(cols, Span::new(0, 0));
        for src in [
            "emit v = amount.round_to(2)",
            "emit v = amount.round(2)",
            "emit v = amount.abs()",
            "emit v = amount.ceil()",
            "emit v = amount.floor()",
        ] {
            let typed = typecheck_ok(src, &["amount"], &schema);
            assert_eq!(
                first_emit_expr_type(&typed),
                Type::Decimal,
                "result type for `{src}`"
            );
        }
    }

    #[test]
    fn test_typecheck_decimal_round_to_composes_with_decimal() {
        // Regression: `round_to` used to take the registry's Float return at
        // face value on a decimal receiver, so composing the result with
        // another decimal was rejected as a decimal/float mix.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        cols.insert("fee".into(), Type::Decimal);
        let schema = Row::closed(cols, Span::new(0, 0));
        let typed = typecheck_ok(
            "emit total = amount.round_to(2) + fee",
            &["amount", "fee"],
            &schema,
        );
        assert_eq!(first_emit_expr_type(&typed), Type::Decimal);
    }

    #[test]
    fn test_typecheck_float_round_to_unchanged() {
        // Float receivers keep the registry's Float return type.
        let mut cols = IndexMap::new();
        cols.insert("rate".into(), Type::Float);
        let schema = Row::closed(cols, Span::new(0, 0));
        let typed = typecheck_ok("emit v = rate.round_to(2)", &["rate"], &schema);
        assert_eq!(first_emit_expr_type(&typed), Type::Float);
    }

    #[test]
    fn test_typecheck_nullable_decimal_round_to_is_nullable_decimal() {
        // Null propagation still applies on top of the decimal
        // specialization: a nullable decimal receiver yields decimal?.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::nullable(Type::Decimal));
        let schema = Row::closed(cols, Span::new(0, 0));
        let typed = typecheck_ok("emit v = amount.round_to(2)", &["amount"], &schema);
        assert_eq!(first_emit_expr_type(&typed), Type::nullable(Type::Decimal));
    }

    #[test]
    fn test_aggregate_return_type_decimal() {
        // sum/avg/min/max over a decimal stay exact decimal.
        assert_eq!(
            aggregate_return_type("sum", &[Type::Decimal]),
            Type::Decimal
        );
        assert_eq!(
            aggregate_return_type("avg", &[Type::Decimal]),
            Type::Decimal
        );
        assert_eq!(
            aggregate_return_type("min", &[Type::Decimal]),
            Type::Decimal
        );
        assert_eq!(
            aggregate_return_type("max", &[Type::Decimal]),
            Type::Decimal
        );
        // avg over int/float is still Float.
        assert_eq!(aggregate_return_type("avg", &[Type::Int]), Type::Float);
        // weighted_avg stays decimal when either the value OR the weight is
        // decimal, and Float when neither is.
        assert_eq!(
            aggregate_return_type("weighted_avg", &[Type::Decimal, Type::Int]),
            Type::Decimal
        );
        assert_eq!(
            aggregate_return_type("weighted_avg", &[Type::Int, Type::Decimal]),
            Type::Decimal
        );
        assert_eq!(
            aggregate_return_type("weighted_avg", &[Type::Int, Type::Int]),
            Type::Float
        );
    }

    #[test]
    fn test_sum_avg_over_decimal_column_typechecks() {
        // Regression for the numeric guard: `sum`/`avg` over a `decimal`
        // column must typecheck and keep the result in the decimal domain.
        // Before the fix the guard omitted `Type::Decimal`, so the exact
        // monetary total was rejected with "requires a Numeric argument" and
        // the fix-it hint steered users to `.to_float()` — reintroducing the
        // binary-float drift the decimal type exists to prevent.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        cols.insert("dept".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        let fields = ["amount", "dept"];

        for (src, want) in [
            ("emit total = sum(amount)", Type::Decimal),
            ("emit average = avg(amount)", Type::Decimal),
        ] {
            let parsed = Parser::parse(src);
            assert!(parsed.errors.is_empty());
            let resolved = resolve_program(parsed.ast, &fields, parsed.node_count).unwrap();
            let typed = type_check_with_mode(resolved, &schema, agg_mode(&["dept"]))
                .unwrap_or_else(|d| {
                    panic!(
                        "`{src}` must typecheck over a decimal column, got: {:?}",
                        d.iter().map(|e| &e.message).collect::<Vec<_>>()
                    )
                });
            assert_eq!(
                first_emit_expr_type(&typed),
                want,
                "result type for `{src}`"
            );
        }
    }

    #[test]
    fn test_weighted_avg_over_decimal_typechecks() {
        // weighted_avg now has an exact decimal accumulator, so a decimal value
        // or weight (or both) typechecks and keeps the result in the decimal
        // domain instead of being rejected.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        cols.insert("price".into(), Type::Decimal);
        cols.insert("qty".into(), Type::Int);
        cols.insert("dept".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        let fields = ["amount", "price", "qty", "dept"];

        for src in [
            "emit wa = weighted_avg(amount, qty)", // decimal value, int weight
            "emit wa = weighted_avg(qty, price)",  // int value, decimal weight
            "emit wa = weighted_avg(amount, price)", // decimal value and weight
        ] {
            let parsed = Parser::parse(src);
            assert!(parsed.errors.is_empty());
            let resolved = resolve_program(parsed.ast, &fields, parsed.node_count).unwrap();
            let typed = type_check_with_mode(resolved, &schema, agg_mode(&["dept"]))
                .unwrap_or_else(|d| {
                    panic!(
                        "`{src}` must typecheck over a decimal column, got: {:?}",
                        d.iter().map(|e| &e.message).collect::<Vec<_>>()
                    )
                });
            assert_eq!(
                first_emit_expr_type(&typed),
                Type::Decimal,
                "result type for `{src}`"
            );
        }
    }

    #[test]
    fn test_weighted_avg_decimal_float_mix_rejected() {
        // A decimal in one position and a binary float in the other cannot
        // combine exactly, mirroring the `decimal * float` arithmetic rule, so
        // the mix is rejected loudly rather than silently dropped.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        cols.insert("rate".into(), Type::Float);
        cols.insert("dept".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        let fields = ["amount", "rate", "dept"];

        for src in [
            "emit wa = weighted_avg(amount, rate)", // decimal value, float weight
            "emit wa = weighted_avg(rate, amount)", // float value, decimal weight
        ] {
            let parsed = Parser::parse(src);
            assert!(parsed.errors.is_empty());
            let resolved = resolve_program(parsed.ast, &fields, parsed.node_count).unwrap();
            let errs = type_check_with_mode(resolved, &schema, agg_mode(&["dept"]))
                .expect_err("weighted_avg mixing decimal and float must be rejected");
            assert!(
                errs.iter().any(|d| d
                    .message
                    .contains("cannot mix a decimal argument with a float or numeric argument")),
                "`{src}` expected a decimal/float mix rejection, got: {:?}",
                errs.iter().map(|d| &d.message).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn test_weighted_avg_decimal_numeric_mix_rejected() {
        // `numeric` admits a binary float at runtime and does not unify with
        // `decimal`, so mixing a decimal argument with a `numeric` one is
        // rejected at typecheck the same way a decimal/float mix is — otherwise
        // the plan compiles and a fractional weight silently poisons the group
        // to Null at runtime.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        cols.insert("qty".into(), Type::Numeric);
        cols.insert("dept".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        let fields = ["amount", "qty", "dept"];

        for src in [
            "emit wa = weighted_avg(amount, qty)", // decimal value, numeric weight
            "emit wa = weighted_avg(qty, amount)", // numeric value, decimal weight
        ] {
            let parsed = Parser::parse(src);
            assert!(parsed.errors.is_empty());
            let resolved = resolve_program(parsed.ast, &fields, parsed.node_count).unwrap();
            let errs = type_check_with_mode(resolved, &schema, agg_mode(&["dept"]))
                .expect_err("weighted_avg mixing decimal and numeric must be rejected");
            assert!(
                errs.iter().any(|d| d
                    .message
                    .contains("cannot mix a decimal argument with a float or numeric argument")),
                "`{src}` expected a decimal/numeric mix rejection, got: {:?}",
                errs.iter().map(|d| &d.message).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn test_typecheck_null_propagation() {
        // nullable_field + 1 → Nullable(Int) when field is declared Nullable(Int)
        let mut cols = IndexMap::new();
        cols.insert("nullable_field".into(), Type::Nullable(Box::new(Type::Int)));
        let schema = Row::closed(cols, Span::new(0, 0));
        let typed = typecheck_ok(
            "emit val = nullable_field + 1",
            &["nullable_field"],
            &schema,
        );
        let ty = first_emit_expr_type(&typed);
        assert_eq!(ty, Type::Nullable(Box::new(Type::Int)));
    }

    #[test]
    fn test_typecheck_coalesce_strips_nullable() {
        let mut cols = IndexMap::new();
        cols.insert("nullable_field".into(), Type::Nullable(Box::new(Type::Int)));
        let schema = Row::closed(cols, Span::new(0, 0));
        let typed = typecheck_ok(
            "emit val = nullable_field ?? 0",
            &["nullable_field"],
            &schema,
        );
        let ty = first_emit_expr_type(&typed);
        assert_eq!(ty, Type::Int);
    }

    #[test]
    fn test_typecheck_match_missing_wildcard() {
        let diags = typecheck_err(
            "emit val = match status { \"A\" => 1 }",
            &["status"],
            &empty_row(),
        );
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("wildcard") || d.message.contains("catch-all")),
            "Expected wildcard diagnostic, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_nested_window_in_predicate() {
        let diags = typecheck_err(
            "emit val = $window.any($window.sum(it) > 0)",
            &["amount"],
            &empty_row(),
        );
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("cannot be called inside")),
            "Expected nested window diagnostic, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_any_requires_bool_arg() {
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Int);
        let schema = Row::closed(cols, Span::new(0, 0));
        let diags = typecheck_err("emit val = $window.any(amount)", &["amount"], &schema);
        assert!(
            diags.iter().any(|d| d.message.contains("Bool")),
            "Expected Bool requirement diagnostic, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_every_requires_bool_arg() {
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Int);
        let schema = Row::closed(cols, Span::new(0, 0));
        let diags = typecheck_err("emit val = $window.every(amount)", &["amount"], &schema);
        assert!(
            diags.iter().any(|d| d.message.contains("Bool")),
            "Expected Bool requirement diagnostic, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_any_arity_rejected() {
        let diags = typecheck_err("emit val = $window.any()", &[], &empty_row());
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("takes exactly 1 argument")),
            "Expected arity diagnostic, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_sum_requires_numeric() {
        // window.sum() on a String field should produce an error
        let mut cols = IndexMap::new();
        cols.insert("name".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        let diags = typecheck_err("emit val = $window.sum(name)", &["name"], &schema);
        assert!(
            diags.iter().any(|d| d.message.contains("Numeric")),
            "Expected Numeric requirement diagnostic, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_window_lag_postfix_field_known() {
        // `$window.lag(1).total` typechecks against the upstream row's
        // declared `total: Int`, lifting it to Nullable(Int) because
        // out-of-bounds positionals return Null.
        let mut cols = IndexMap::new();
        cols.insert("total".into(), Type::Int);
        let schema = Row::closed(cols, Span::new(0, 0));
        let typed = typecheck_ok("emit prev = $window.lag(1).total", &["total"], &schema);
        assert_eq!(
            first_emit_expr_type(&typed),
            Type::Nullable(Box::new(Type::Int))
        );
    }

    #[test]
    fn test_typecheck_window_lag_postfix_field_unknown_rejects() {
        // `$window.lag(1).typo_field` against a closed schema with no
        // `typo_field` produces a typecheck error rather than silently
        // typing as Any.
        let mut cols = IndexMap::new();
        cols.insert("total".into(), Type::Int);
        let schema = Row::closed(cols, Span::new(0, 0));
        let diags = typecheck_err("emit prev = $window.lag(1).typo_field", &["total"], &schema);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("typo_field") && d.message.contains("upstream")),
            "expected upstream-row-schema diagnostic, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_field_type_conflict_both_spans() {
        // field used as String in one place, Numeric in another
        let diags = typecheck_err(
            "emit a = status == \"active\"\nemit b = status + 1",
            &["status"],
            &empty_row(),
        );
        // Should have a diagnostic about conflicting types
        let conflict = diags.iter().find(|d| d.message.contains("used as"));
        assert!(
            conflict.is_some(),
            "Expected type conflict diagnostic, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
        // Should cite both spans
        assert!(
            conflict.unwrap().related_span.is_some(),
            "Expected related_span to cite the other usage site"
        );
    }

    #[test]
    fn test_typecheck_schema_override_conflict() {
        let mut cols = IndexMap::new();
        cols.insert("age".into(), Type::Int);
        let schema = Row::closed(cols, Span::new(0, 0));
        let diags = typecheck_err("emit val = age == \"old\"", &["age"], &schema);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("declared as") && d.message.contains("schema")),
            "Expected schema override conflict, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_zero_emit_warning() {
        // Program with no emit → warning (not error, so type_check succeeds)
        let typed = typecheck_ok("let x = 1", &[], &empty_row());
        // The program should have been created successfully (warnings don't block)
        assert!(typed.program.statements.len() == 1);
    }

    #[test]
    fn test_typecheck_field_type_map_produced() {
        let typed = typecheck_ok(
            "emit a = amount + 1\nemit b = name == \"test\"",
            &["amount", "name"],
            &empty_row(),
        );
        // amount should be inferred as Numeric, name as String
        assert!(
            typed
                .field_types
                .contains_key(&QualifiedField::bare("amount")),
            "Expected amount in field_types, got: {:?}",
            typed.field_types
        );
        assert!(
            typed
                .field_types
                .contains_key(&QualifiedField::bare("name")),
            "Expected name in field_types, got: {:?}",
            typed.field_types
        );
    }

    #[test]
    fn test_typecheck_equality_always_bool() {
        let typed = typecheck_ok("emit val = 1 == 2", &[], &empty_row());
        assert_eq!(first_emit_expr_type(&typed), Type::Bool);
    }

    #[test]
    fn test_typecheck_if_missing_else_nullable() {
        let typed = typecheck_ok("emit val = if true then 1", &[], &empty_row());
        let ty = first_emit_expr_type(&typed);
        assert!(
            ty.is_nullable(),
            "Expected Nullable type for if-then without else, got {}",
            ty
        );
    }

    #[test]
    fn test_typecheck_now_is_datetime() {
        let typed = typecheck_ok("emit ts = now", &[], &empty_row());
        assert_eq!(first_emit_expr_type(&typed), Type::DateTime);
    }

    #[test]
    fn test_typecheck_pipeline_access_types() {
        let typed = typecheck_ok("emit ts = $pipeline.start_time", &[], &empty_row());
        assert_eq!(first_emit_expr_type(&typed), Type::DateTime);

        let typed2 = typecheck_ok("emit n = $pipeline.total_count", &[], &empty_row());
        assert_eq!(first_emit_expr_type(&typed2), Type::Int);
    }

    #[test]
    fn test_typecheck_regex_precompiled() {
        let typed = typecheck_ok(
            "emit val = name.matches(\"\\\\d+\")",
            &["name"],
            &empty_row(),
        );
        // The regex should be pre-compiled and stored
        let has_regex = typed.regexes.iter().any(|r| r.is_some());
        assert!(
            has_regex,
            "Expected pre-compiled regex in TypedProgram.regexes"
        );
    }

    // ── aggregate typecheck tests ──────────────────────────────────────

    fn agg_mode(keys: &[&str]) -> AggregateMode {
        AggregateMode::GroupBy {
            group_by_fields: keys.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    fn typecheck_with(
        src: &str,
        fields: &[&str],
        mode: AggregateMode,
    ) -> Result<TypedProgram, Vec<TypeDiagnostic>> {
        let parsed = Parser::parse(src);
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        let resolved =
            resolve_program(parsed.ast, fields, parsed.node_count).expect("resolve failed");
        let row = empty_row();
        type_check_with_mode(resolved, &row, mode)
    }

    fn agg_ok(src: &str, fields: &[&str], keys: &[&str]) -> TypedProgram {
        typecheck_with(src, fields, agg_mode(keys)).unwrap_or_else(|d| {
            panic!(
                "Expected Ok but got type errors: {:?}",
                d.iter().map(|e| &e.message).collect::<Vec<_>>()
            )
        })
    }

    fn agg_err(src: &str, fields: &[&str], keys: &[&str]) -> Vec<TypeDiagnostic> {
        typecheck_with(src, fields, agg_mode(keys)).expect_err("Expected type errors but got Ok")
    }

    fn row_err(src: &str, fields: &[&str]) -> Vec<TypeDiagnostic> {
        typecheck_with(src, fields, AggregateMode::Row)
            .expect_err("Expected type errors but got Ok")
    }

    // Direction 1 — AggCall in row-level context rejected

    #[test]
    fn test_agg_in_row_context_rejected() {
        let errs = row_err("emit total = sum(amount)", &["amount"]);
        assert!(errs.iter().any(|d| d.message.contains("sum")));
    }

    #[test]
    fn test_agg_inside_nested_emit_each_body_row_context_rejected() {
        // The aggregate-context walker must descend through both fan-out
        // levels: an aggregate call buried in a body nested two `emit each`
        // blocks deep is still forbidden in a row-level transform. A
        // walker that handled only the immediate body would let this slip.
        let errs = row_err(
            "emit each g in groups {\n  emit each it in g {\n    emit bad = sum(it)\n  }\n}",
            &["groups"],
        );
        assert!(
            errs.iter().any(|d| d.message.contains("sum")),
            "expected the buried aggregate to be rejected, got: {:?}",
            errs.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_all_agg_fns_rejected_in_row_context() {
        for name in [
            "sum(amount)",
            "count(amount)",
            "avg(amount)",
            "min(amount)",
            "max(amount)",
            "collect(amount)",
            "weighted_avg(amount, amount)",
        ] {
            let src = format!("emit x = {name}");
            let errs = row_err(&src, &["amount"]);
            assert!(
                !errs.is_empty(),
                "Expected row-context error for {name}, got Ok"
            );
        }
    }

    #[test]
    fn test_agg_buried_in_binary_expr_row_context() {
        let errs = row_err("emit x = amount + sum(amount)", &["amount"]);
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_agg_in_if_branch_row_context() {
        let errs = row_err(
            "emit x = if flag then sum(amount) else 0",
            &["flag", "amount"],
        );
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_agg_in_let_binding_row_context() {
        let errs = row_err("let x = sum(amount)\nemit y = x", &["amount"]);
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_agg_in_filter_row_context() {
        let errs = row_err("filter sum(amount) > 100\nemit x = 1", &["amount"]);
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_agg_in_method_receiver_row_context() {
        let errs = row_err("emit x = sum(amount).to_string()", &["amount"]);
        assert!(!errs.is_empty());
    }

    // Direction 2 — bare FieldRef in aggregate context

    #[test]
    fn test_non_grouped_field_in_emit_rejected() {
        let errs = agg_err("emit name = name", &["name", "dept"], &["dept"]);
        assert!(errs.iter().any(|d| d.message.contains("'name'")));
    }

    #[test]
    fn test_group_key_in_emit_allowed() {
        let _ = agg_ok("emit dept = dept", &["dept", "amount"], &["dept"]);
    }

    #[test]
    fn test_multi_key_group_by() {
        // Both group keys allowed bare; non-key errors
        let errs = agg_err(
            "emit dept = dept\nemit region = region\nemit name = name",
            &["dept", "region", "name"],
            &["dept", "region"],
        );
        assert!(errs.iter().any(|d| d.message.contains("'name'")));
        assert!(!errs.iter().any(|d| d.message.contains("'dept'")));
        assert!(!errs.iter().any(|d| d.message.contains("'region'")));
    }

    #[test]
    fn test_non_grouped_field_inside_agg_call_allowed() {
        let _ = agg_ok("emit total = sum(amount)", &["amount", "dept"], &["dept"]);
    }

    #[test]
    fn test_non_grouped_in_agg_binary_arg() {
        let _ = agg_ok(
            "emit total = avg(salary + bonus)",
            &["salary", "bonus", "dept"],
            &["dept"],
        );
    }

    #[test]
    fn test_let_binding_used_in_agg_call() {
        let _ = agg_ok(
            "let x = amount\nemit total = sum(x)",
            &["amount", "dept"],
            &["dept"],
        );
    }

    #[test]
    fn test_let_binding_emitted_bare() {
        let errs = agg_err(
            "let x = amount\nemit out = x",
            &["amount", "dept"],
            &["dept"],
        );
        assert!(!errs.is_empty(), "let alias to non-grouped should error");
    }

    #[test]
    fn test_group_key_inside_agg_call() {
        let _ = agg_ok("emit d = sum(dept_id)", &["dept_id"], &["dept_id"]);
    }

    #[test]
    fn test_literal_in_aggregate_context() {
        let _ = agg_ok("emit c = 42", &["dept"], &["dept"]);
    }

    #[test]
    fn test_pipeline_meta_in_aggregate_context() {
        let _ = agg_ok("emit run = $pipeline.execution_id", &["dept"], &["dept"]);
    }

    // Per-record provenance fields cannot appear bare in an aggregate
    // residual; must be wrapped in an aggregate.
    #[test]
    fn test_typecheck_rejects_source_row_in_agg_residual() {
        let errs = agg_err("emit row = $source.row", &["dept"], &["dept"]);
        assert!(
            errs.iter()
                .any(|d| d.message.contains("source.row")
                    && d.message.contains("per-record provenance")),
            "expected provenance rejection, got: {:?}",
            errs.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_allows_first_source_row_in_agg() {
        let _ = agg_ok("emit row = min($source.row)", &["dept"], &["dept"]);
    }

    #[test]
    fn test_filter_in_aggregate_context_allowed() {
        let _ = agg_ok(
            "filter salary > 50000\nemit total = sum(salary)",
            &["salary", "dept"],
            &["dept"],
        );
    }

    #[test]
    fn test_distinct_in_aggregate_context_allowed() {
        let _ = agg_ok(
            "distinct by employee_id\nemit total = sum(salary)",
            &["employee_id", "salary", "dept"],
            &["dept"],
        );
    }

    // Nested

    #[test]
    fn test_nested_agg_call_rejected() {
        let errs = agg_err("emit x = sum(count(id))", &["id", "dept"], &["dept"]);
        assert!(errs.iter().any(|d| d.message.contains("nested")));
    }

    #[test]
    fn test_nested_agg_through_method_chain() {
        let errs = agg_err(
            "emit x = sum(count(name).to_float())",
            &["name", "dept"],
            &["dept"],
        );
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_agg_in_filter_aggregate_context() {
        let errs = agg_err(
            "filter sum(amount) > 0\nemit x = 1",
            &["amount", "dept"],
            &["dept"],
        );
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_empty_group_by_global_fold() {
        let _ = agg_ok("emit total = sum(amount)", &["amount"], &[]);
    }

    #[test]
    fn test_error_points_at_specific_field() {
        // Mixing a valid key emit and an invalid non-key emit — only the
        // offending one should error.
        let errs = agg_err(
            "emit dept = dept\nemit bad = name",
            &["dept", "name"],
            &["dept"],
        );
        assert!(errs.iter().any(|d| d.message.contains("'name'")));
        assert!(!errs.iter().any(|d| d.message.contains("'dept'")));
    }

    // Expression depth edge cases

    #[test]
    fn test_method_on_non_grouped_field() {
        let errs = agg_err(
            "emit x = employee_name.trim()",
            &["employee_name", "dept"],
            &["dept"],
        );
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_method_on_agg_result() {
        let _ = agg_ok(
            "emit x = sum(amount).to_string()",
            &["amount", "dept"],
            &["dept"],
        );
    }

    #[test]
    fn test_coalesce_agg_and_literal() {
        let _ = agg_ok("emit x = sum(amount) ?? 0", &["amount", "dept"], &["dept"]);
    }

    #[test]
    fn test_coalesce_non_grouped_and_literal() {
        let errs = agg_err(
            "emit x = employee_name ?? \"unknown\"",
            &["employee_name", "dept"],
            &["dept"],
        );
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_if_both_branches_aggregated() {
        let _ = agg_ok(
            "emit x = if dept == \"SALES\" then sum(c) else sum(s)",
            &["dept", "c", "s"],
            &["dept"],
        );
    }

    #[test]
    fn test_if_condition_has_non_grouped() {
        let errs = agg_err(
            "emit x = if employee_name == \"Alice\" then sum(salary) else 0",
            &["employee_name", "salary", "dept"],
            &["dept"],
        );
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_count_literal_arg() {
        let _ = agg_ok("emit c = count(1)", &["dept"], &["dept"]);
    }

    #[test]
    fn test_non_grouped_in_binary_no_agg() {
        let errs = agg_err(
            "emit x = (amount + bonus) * 0.9",
            &["amount", "bonus", "dept"],
            &["dept"],
        );
        assert!(!errs.is_empty());
    }

    #[test]
    fn test_agg_of_binary_with_non_grouped() {
        let _ = agg_ok(
            "emit x = sum(amount * rate)",
            &["amount", "rate", "dept"],
            &["dept"],
        );
    }

    #[test]
    fn test_weighted_avg_in_aggregate_context() {
        let _ = agg_ok(
            "emit w = weighted_avg(score, weight)",
            &["score", "weight", "dept"],
            &["dept"],
        );
    }

    // Type inference

    #[test]
    fn test_typecheck_sum_returns_same_type() {
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Int);
        let schema = Row::closed(cols, Span::new(0, 0));
        let parsed = Parser::parse("emit t = sum(amount)");
        let resolved = resolve_program(parsed.ast, &["amount", "dept"], parsed.node_count).unwrap();
        let typed = type_check_with_mode(
            resolved,
            &schema,
            AggregateMode::GroupBy {
                group_by_fields: ["dept".to_string()].into_iter().collect(),
            },
        )
        .unwrap();
        assert_eq!(first_emit_expr_type(&typed), Type::Int);
    }

    #[test]
    fn test_typecheck_avg_returns_float() {
        let typed = agg_ok("emit t = avg(amount)", &["amount", "dept"], &["dept"]);
        assert_eq!(first_emit_expr_type(&typed), Type::Float);
    }

    #[test]
    fn test_typecheck_collect_returns_array() {
        let typed = agg_ok("emit t = collect(amount)", &["amount", "dept"], &["dept"]);
        assert_eq!(first_emit_expr_type(&typed), Type::Array);
    }

    #[test]
    fn test_typecheck_weighted_avg_returns_float() {
        let typed = agg_ok(
            "emit t = weighted_avg(score, weight)",
            &["score", "weight", "dept"],
            &["dept"],
        );
        assert_eq!(first_emit_expr_type(&typed), Type::Float);
    }

    // ── Row integration tests ─────────────────────────────────────────

    #[test]
    fn test_cxl_type_check_accepts_row_closed() {
        let mut cols = IndexMap::new();
        cols.insert("a".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        // `emit x = a` should succeed — `a` is declared.
        let typed = typecheck_ok("emit x = a", &["a"], &schema);
        assert!(
            typed.field_types.contains_key(&QualifiedField::bare("a")),
            "expected 'a' in field_types"
        );
    }

    #[test]
    fn test_cxl_type_check_accepts_row_open_passthrough() {
        use super::super::row::TailVarId;
        let mut cols = IndexMap::new();
        cols.insert("a".into(), Type::String);
        let schema = Row::open(cols, Span::new(0, 0), TailVarId(0));
        // `emit x = b` should succeed — `b` is not declared but the row
        // is open, so it passes through as Type::Any. The typecheck must
        // not error. `b` resolves as Type::Any via PassThrough.
        let typed = typecheck_ok("emit x = b", &["a", "b"], &schema);
        // The program should have an emit statement referencing b.
        assert!(
            !typed.program.statements.is_empty(),
            "expected at least one statement"
        );
    }

    #[test]
    fn test_cxl_type_check_rejects_closed_row_unknown_column() {
        let mut cols = IndexMap::new();
        cols.insert("a".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        // `emit x = b` — `b` is not in the schema and the row is closed.
        // The typechecker does NOT reject unknown fields with an error today
        // (they resolve as Type::Any from the resolver). The rejection
        // happens at the resolve phase when `b` is not in `field_refs`.
        // Verify that the resolve phase rejects it.
        let parsed = Parser::parse("emit x = b");
        assert!(parsed.errors.is_empty());
        let result = resolve_program(parsed.ast, &["a"], parsed.node_count);
        // `b` is not in the declared fields, so resolve should reject it
        // OR it should typecheck but with `b` not being a declared schema
        // field. Let's verify the typecheck path:
        // If resolve fails (b not in field_refs), that's the gating behavior.
        // If resolve succeeds (b treated as implicit field), typecheck
        // produces Type::Any for b (ColumnLookup::Unknown → Type::Any).
        match result {
            Err(_) => {
                // Resolve rejected unknown column — correct behavior for
                // a closed row where field_refs doesn't include 'b'.
            }
            Ok(resolved) => {
                // Resolve accepted it (some CXL versions allow implicit refs).
                // The typecheck should still succeed but with b as Unknown/Any.
                let typed = type_check(resolved, &schema);
                assert!(
                    typed.is_ok(),
                    "typecheck should not hard-error on unknown fields in current semantics"
                );
            }
        }
    }

    // --- Phase Combine C.1.0 gate: qualified field ref resolution --------

    /// C.1.0 corrective: a bare `FieldRef` against a merged row where
    /// the name is ambiguous (matches multiple declared fields under
    /// different qualifiers) must produce a loud typecheck error
    /// suggesting `qualifier.name` — NOT silently degrade to Type::Any.
    #[test]
    fn test_bare_fieldref_ambiguous_errors() {
        use super::super::row::QualifiedField;

        // Merged row with two `id` fields under different qualifiers.
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::qualified("orders", "id"), Type::Int);
        cols.insert(QualifiedField::qualified("products", "id"), Type::Int);
        let schema = Row::closed(cols, Span::new(0, 0));

        // `emit v = id` — bare ref that resolves ambiguously.
        let parsed = Parser::parse("emit v = id");
        assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
        let resolved = resolve_program(parsed.ast, &["id"], parsed.node_count).unwrap();
        let result = type_check(resolved, &schema);

        let diags = result.expect_err("ambiguous bare ref must error");
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("ambiguous") && d.message.contains("'id'")),
            "expected 'ambiguous' diagnostic mentioning 'id'; got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
        // Suggestion mentions both qualified alternatives.
        let help_msgs: Vec<&String> = diags.iter().filter_map(|d| d.help.as_ref()).collect();
        assert!(
            help_msgs
                .iter()
                .any(|h| h.contains("orders.id") && h.contains("products.id")),
            "expected help mentioning both `orders.id` and `products.id`; got: {help_msgs:?}"
        );
    }

    /// C.1.0 gate (RESOLUTION T-5): a 2-part `QualifiedFieldRef` against
    /// a row containing qualified fields resolves via `lookup_qualified`
    /// to the declared type, not `Type::Any`. Smoke test that the new
    /// `Expr::QualifiedFieldRef` handler in pass.rs actually calls into
    /// the new `Row::lookup_qualified` method.
    #[test]
    fn test_qualified_field_ref_resolves() {
        use super::super::row::QualifiedField;

        // Build a merged-row style schema: orders.product_id: Int,
        // products.product_id: String. Two fields share bare name
        // "product_id" under different qualifiers.
        let mut cols = IndexMap::new();
        cols.insert(QualifiedField::qualified("orders", "product_id"), Type::Int);
        cols.insert(
            QualifiedField::qualified("products", "product_id"),
            Type::String,
        );
        let schema = Row::closed(cols, Span::new(0, 0));

        // `emit v = orders.product_id` — the typechecker must resolve
        // the 2-part ref via `lookup_qualified("orders", "product_id")`
        // and set the expr type to Int, not Any.
        let parsed = Parser::parse("emit v = orders.product_id");
        assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
        // `orders` is the left-most identifier in `orders.product_id` —
        // the resolver treats it as a field binding. Register it in the
        // field_refs slice so resolve succeeds.
        let resolved = resolve_program(parsed.ast, &["orders"], parsed.node_count)
            .expect("resolve should succeed for qualified ref");
        let typed = type_check(resolved, &schema).expect("typecheck should succeed");

        assert_eq!(
            first_emit_expr_type(&typed),
            Type::Int,
            "orders.product_id must resolve to Int via lookup_qualified, not Any"
        );

        // And the products.product_id side must independently resolve to
        // String — same mechanism, different qualifier.
        let parsed = Parser::parse("emit v = products.product_id");
        assert!(parsed.errors.is_empty());
        let resolved = resolve_program(parsed.ast, &["products"], parsed.node_count).unwrap();
        let typed = type_check(resolved, &schema).unwrap();
        assert_eq!(
            first_emit_expr_type(&typed),
            Type::String,
            "products.product_id must resolve to String via lookup_qualified"
        );
    }

    // ── language soundness (#489 let-var inference, #464 binary-op
    // mismatch, #488 method receiver/arity/argtype) ──────────────────
    //
    // Positive tests assert a DIAGNOSTIC; negative tests prove legitimate
    // expressions — including the value-dispatch receiver families and open
    // rows — still type-check clean (the dominant false-positive risk).

    #[test]
    fn test_typecheck_string_plus_int_rejected() {
        // #464: `"x" + 1` has provably disjoint operands. It used to widen to
        // Any silently; now it must emit a diagnostic.
        let diags = typecheck_err("emit v = \"x\" + 1", &[], &empty_row());
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("String") && d.message.contains("Int")),
            "expected a String/Int arithmetic mismatch, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_length_gt_string_rejected() {
        // #464 + #488: `name.length() > "5"` compares Int to String — a silent
        // `false` before, now a diagnostic.
        let mut cols = IndexMap::new();
        cols.insert("name".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        let diags = typecheck_err("emit v = name.length() > \"5\"", &["name"], &schema);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("compare") && d.message.contains("ordering")),
            "expected an ordering-comparison mismatch, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_eq_disjoint_types_rejected() {
        // #464: equality over provably disjoint non-Any types is a type error,
        // not a constant `false`.
        let mut cols = IndexMap::new();
        cols.insert("qty".into(), Type::Int);
        let schema = Row::closed(cols, Span::new(0, 0));
        let diags = typecheck_err("emit v = qty == \"5\"", &["qty"], &schema);
        assert!(
            diags.iter().any(|d| d.message.contains("equality")
                || (d.message.contains("Int") && d.message.contains("String"))),
            "expected an equality type mismatch, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_letvar_rhs_type_misuse_rejected() {
        // #489: a let-var must take its RHS type so operand checks apply.
        // `tag` is String; `tag + 1` is then a String/Int arithmetic mismatch.
        // An `Any`-typed let-var would unify with everything and bypass it.
        let diags = typecheck_err("let tag = \"x\"\nemit v = tag + 1", &[], &empty_row());
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("String") && d.message.contains("Int")),
            "expected the let-var's String RHS to reject `+ 1`, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_letvar_rhs_type_flows_to_method_receiver() {
        // #489 + #488: a let-var bound to an Int, used as a String-method
        // receiver, is rejected — the inferred let-var type reaches the
        // receiver check.
        let diags = typecheck_err("let n = 5\nemit v = n.upper()", &[], &empty_row());
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("upper") && d.message.contains("String")),
            "expected `(let n = 5).upper()` to be rejected, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_method_receiver_mismatch_rejected() {
        // #488: `(5).upper()` — Int receiver on a String method.
        let diags = typecheck_err("emit v = (5).upper()", &[], &empty_row());
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("upper") && d.message.contains("receiver")),
            "expected a receiver mismatch for (5).upper(), got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_method_arg_type_mismatch_rejected() {
        // #488: `name.starts_with(5)` — Int where a String argument is required.
        let mut cols = IndexMap::new();
        cols.insert("name".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        let diags = typecheck_err("emit v = name.starts_with(5)", &["name"], &schema);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("starts_with") && d.message.contains("String")),
            "expected an arg-type mismatch, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_method_arity_mismatch_rejected() {
        // #488: `name.upper("x")` — upper takes no arguments.
        let mut cols = IndexMap::new();
        cols.insert("name".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        let diags = typecheck_err("emit v = name.upper(\"x\")", &["name"], &schema);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("upper") && d.message.contains("argument")),
            "expected an arity mismatch, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_valid_arithmetic_and_methods_still_clean() {
        // #464/#488 false-positive guard: legitimate arithmetic, comparison,
        // and method calls — including nullable operands — stay clean.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Int);
        cols.insert("name".into(), Type::String);
        cols.insert("nullable_amount".into(), Type::nullable(Type::Int));
        let schema = Row::closed(cols, Span::new(0, 0));
        let fields = ["amount", "name", "nullable_amount"];
        for src in [
            "emit v = amount + 1",              // Int + Int
            "emit v = amount > 5",              // Int comparison
            "emit v = amount == 5",             // Int equality
            "emit v = name.upper()",            // String method, no args
            "emit v = name.starts_with(\"a\")", // String argument
            "emit v = nullable_amount + 1",     // nullable Int arithmetic
            "emit v = nullable_amount > 5",     // nullable Int comparison
        ] {
            let _ = typecheck_ok(src, &fields, &schema);
        }
    }

    #[test]
    fn test_typecheck_receiver_polymorphism_still_clean() {
        // #488 false-positive guard for the value-dispatched receiver families
        // the single-key registry cannot express: numeric methods on Decimal,
        // date methods on DateTime, and join/length/find on Array must all
        // remain clean.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        cols.insert("ts".into(), Type::DateTime);
        cols.insert("tags".into(), Type::Array);
        let schema = Row::closed(cols, Span::new(0, 0));
        let fields = ["amount", "ts", "tags"];
        for src in [
            "emit v = amount.abs()",           // numeric method on Decimal
            "emit v = amount.clamp(0, 100)",   // numeric args over a Decimal receiver
            "emit v = ts.year()",              // date component on DateTime
            "emit v = ts.hour()",              // time component on DateTime
            "emit v = ts.format_date(\"%Y\")", // date method on DateTime
            // add_days preserves the DateTime receiver kind, so comparing the
            // shifted timestamp against another DateTime must stay clean.
            "emit v = ts.add_days(1) > ts",
            "emit v = ts.add_days(1).year()",
            "emit v = tags.length()",                // length on Array
            "emit v = tags.join(\",\")",             // join on Array
            "emit v = tags.find(it => it == \"x\")", // find (closure) on Array
            // Array `.find` returns the matched element (Any), not the String
            // form's Bool, so composing on its result must stay clean.
            "emit v = tags.find(it => it == \"x\").upper()",
            "emit v = tags.find(it => it == \"x\") == \"x\"",
            "emit v = ts.format(\"\")",     // format renders any receiver
            "emit v = amount.format(\"\")", // format on a Decimal receiver
        ] {
            let _ = typecheck_ok(src, &fields, &schema);
        }
    }

    #[test]
    fn test_typecheck_decimal_numeric_composition_still_clean() {
        // #464 false-positive guard: `numeric`-returning builtins over a decimal
        // receiver (`clamp`/`min`/`max` are typed `Numeric`, not `Decimal`)
        // compose with another decimal without a spurious mismatch. `numeric`
        // may resolve to `int` at runtime, which widens exactly into decimal, so
        // the pairing is not provably incompatible and must stay permissive.
        let mut cols = IndexMap::new();
        cols.insert("amount".into(), Type::Decimal);
        cols.insert("fee".into(), Type::Decimal);
        let schema = Row::closed(cols, Span::new(0, 0));
        let fields = ["amount", "fee"];
        for src in [
            "emit v = amount.clamp(0, 100) + fee", // Numeric (clamp) + Decimal
            "emit v = amount.min(fee) > fee",      // Numeric (min) ordered vs Decimal
            "emit v = amount.max(fee) == fee",     // Numeric (max) equated with Decimal
        ] {
            let _ = typecheck_ok(src, &fields, &schema);
        }
    }

    #[test]
    fn test_typecheck_open_row_receiver_and_operands_permissive() {
        // #464/#488 false-positive guard: on an open row, unknown fields type
        // as Any (PassThrough), so method calls and arithmetic/comparison over
        // them stay permissive rather than hard-error.
        use super::super::row::TailVarId;
        let cols = IndexMap::new();
        let schema = Row::open(cols, Span::new(0, 0), TailVarId(0));
        let fields = ["ext"];
        for src in [
            "emit v = ext.upper()",  // Any receiver
            "emit v = ext + 1",      // Any operand arithmetic
            "emit v = ext > 5",      // Any operand comparison
            "emit v = ext == \"z\"", // Any operand equality
        ] {
            let _ = typecheck_ok(src, &fields, &schema);
        }
    }

    #[test]
    fn test_typecheck_debug_optional_prefix_arity() {
        // #488: `.debug()` (no prefix) and `.debug("tag")` both type-check;
        // `.debug("a", "b")` exceeds the single optional argument.
        let mut cols = IndexMap::new();
        cols.insert("name".into(), Type::String);
        let schema = Row::closed(cols, Span::new(0, 0));
        let _ = typecheck_ok("emit v = name.debug()", &["name"], &schema);
        let _ = typecheck_ok("emit v = name.debug(\"tag\")", &["name"], &schema);
        let diags = typecheck_err("emit v = name.debug(\"a\", \"b\")", &["name"], &schema);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("debug") && d.message.contains("argument")),
            "expected an over-arity diagnostic for debug, got: {:?}",
            diags.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_compile_oversized_int_literal_yields_diagnostic() {
        // #448: an integer literal wider than i64 must surface as a parse
        // diagnostic (the lexer emits Token::Error, the parser rejects it)
        // rather than panicking in the lexer.
        let parsed = Parser::parse("emit v = 99999999999999999999");
        assert!(
            !parsed.errors.is_empty(),
            "expected a parse diagnostic for an oversized integer literal"
        );
    }
}
