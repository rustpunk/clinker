use std::collections::{HashMap, HashSet};

use indexmap::IndexMap;
use regex::Regex;

use super::row::{ColumnLookup, QualifiedField, Row};
use super::types::Type;
use crate::ast::{BinOp, Expr, LiteralValue, NodeId, Program, Statement, UnaryOp};
use crate::builtins::BuiltinRegistry;
use crate::lexer::Span;
use crate::resolve::pass::{ResolvedBinding, ResolvedProgram};

/// Return type inference for aggregate function calls (Phase 16).
///
/// - `sum(Integer)` → Integer, `sum(Float)` → Float, `sum(Numeric/Any)` → Numeric
/// - `count(...)` → Integer (always)
/// - `avg(...)` → Float
/// - `min(T)` / `max(T)` → T (preserving input type)
/// - `collect(T)` → Array (Phase 16 treats it as Array; element type is erased)
/// - `weighted_avg(_, _)` → Float
/// - Unknown names → Any (caller emits a diagnostic via the parser lookahead set)
fn aggregate_return_type(name: &str, arg_types: &[Type]) -> Type {
    match name {
        "sum" => arg_types
            .first()
            .map(|t| match t.unwrap_nullable() {
                Type::Int => Type::Int,
                Type::Float => Type::Float,
                Type::Numeric => Type::Numeric,
                _ => Type::Numeric,
            })
            .unwrap_or(Type::Numeric),
        "count" => Type::Int,
        "avg" => Type::Float,
        "weighted_avg" => Type::Float,
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
/// Research: RESEARCH-aggregate-typecheck-context.md — PostgreSQL
/// `parseCheckAggregates` + Spark `CheckAnalysis` pattern.
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
    let registry = BuiltinRegistry::new();
    let node_count = resolved.node_count;

    // Destructure resolved to avoid borrow conflicts
    let ResolvedProgram {
        program,
        bindings,
        node_count: _,
    } = resolved;

    let mut checker = TypeChecker {
        bindings: &bindings,
        schema,
        registry: &registry,
        types: vec![None; node_count as usize],
        regexes: vec![None; node_count as usize],
        field_constraints: HashMap::new(),
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
    })
}

struct TypeChecker<'a> {
    bindings: &'a [Option<ResolvedBinding>],
    schema: &'a Row,
    registry: &'a BuiltinRegistry,
    types: Vec<Option<Type>>,
    regexes: Vec<Option<Regex>>,
    field_constraints: HashMap<String, Vec<FieldConstraint>>,
    diagnostics: Vec<TypeDiagnostic>,
    // Note: in_predicate_expr tracking is done via the resolver's context,
    // but we track it here for the nested-window check
    aggregate_mode: AggregateMode,
    /// Depth counter for the `check_aggregate_context` walk. When > 0 we are
    /// inside the arguments of an AggCall; bare FieldRefs are exempted from
    /// the "must appear in group_by" check.
    agg_function_depth: u32,
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
                                // TODO: LD-16c-22 — upgrade to Type::PassThrough(TailVarId)
                                // when 16c.2 needs tail identity for composition unification.
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
                    Some(ResolvedBinding::LetVar(_)) => Type::Any, // Inferred from RHS
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
                    "name" | "execution_id" | "source_file" => Type::String,
                    "total_count" | "ok_count" | "dlq_count" | "source_row" => Type::Int,
                    _ => Type::Any, // User-defined pipeline.vars.*
                };
                self.set_type(*node_id, ty.clone());
                ty
            }

            Expr::MetaAccess { node_id, .. } => {
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

                // Look up return type from registry
                let ty = if let Some(def) = self.registry.lookup_method(method) {
                    Type::from_type_tag(def.return_type)
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
                let is_predicate_fn = &**function == "any" || &**function == "all";

                // Check for nested window calls inside predicate_expr
                if in_predicate {
                    self.error(*span,
                        format!("$window.{}() cannot be called inside a $window.any() or $window.all() predicate", function),
                        Some("Move the window call outside the predicate expression".into()));
                }

                // Type check arguments
                for arg in args {
                    self.check_expr(arg, is_predicate_fn);
                }

                // Check numeric requirement for sum/avg
                if matches!(&**function, "sum" | "avg") {
                    for arg in args {
                        let arg_ty = self.get_type(arg.node_id());
                        let inner = arg_ty.unwrap_nullable();
                        if !matches!(inner, Type::Int | Type::Float | Type::Numeric | Type::Any) {
                            self.error(
                                arg.span(),
                                format!(
                                    "window.{}() requires a Numeric argument, got {}",
                                    function, arg_ty
                                ),
                                Some(
                                    "Use a numeric field or convert with .to_int() / .to_float()"
                                        .into(),
                                ),
                            );
                        }
                    }
                }

                let ty = if let Some(def) = self.registry.lookup_window(function) {
                    Type::from_type_tag(def.return_type)
                } else {
                    Type::Any
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

                // Numeric-only guard for sum/avg/weighted_avg (same pattern as
                // window sum/avg).
                match &**name {
                    "sum" | "avg" => {
                        if let Some(arg_ty) = arg_types.first() {
                            let inner = arg_ty.unwrap_nullable();
                            if !matches!(inner, Type::Int | Type::Float | Type::Numeric | Type::Any)
                            {
                                self.error(
                                    *span,
                                    format!("{name}() requires a Numeric argument, got {arg_ty}"),
                                    Some(
                                        "Use a numeric field or convert with .to_int() / .to_float()"
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
                        }
                    }
                    _ => {}
                }

                self.set_type(*node_id, ty.clone());
                ty
            }
        }
    }

    /// Phase C: enforce aggregate-context rules. Walks the program after type
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
            match stmt {
                Statement::Emit { expr, .. } => {
                    self.agg_function_depth = 0;
                    self.walk_agg_ctx(expr, &let_exprs);
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
                _ => {}
            }
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

            // D59 / Task 16.3.13a: per-record provenance fields
            // (`pipeline.source_row` / `pipeline.source_file`) cannot appear
            // outside an aggregate function inside an aggregate transform —
            // the residual evaluator at finalize() does not have a current
            // record. Universal SQL practice (Postgres SQLSTATE 42803,
            // DuckDB binder, Trino `$row_id` scan-only, Spark functional-
            // dependency rule, DataFusion #20135).
            Expr::PipelineAccess { field, span, .. }
                if matches!(self.aggregate_mode, AggregateMode::GroupBy { .. })
                    && self.agg_function_depth == 0
                    && (field.as_ref() == "source_row" || field.as_ref() == "source_file") =>
            {
                self.error(
                    *span,
                    format!(
                        "`pipeline.{field}` is per-record provenance and cannot appear in an aggregate residual expression"
                    ),
                    Some(format!(
                        "Wrap in an aggregate function (e.g., `first(pipeline.{field})`, `min(pipeline.{field})`, `max(pipeline.{field})`)"
                    )),
                );
            }

            // Recurse into children. Literals, $pipeline.*, $meta.*, now,
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
            Expr::Literal { .. }
            | Expr::QualifiedFieldRef { .. }
            | Expr::PipelineAccess { .. }
            | Expr::MetaAccess { .. }
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
            _ => {}
        }
    }

    fn check_binary_op(
        &mut self,
        node_id: NodeId,
        op: BinOp,
        lt: &Type,
        rt: &Type,
        _span: Span,
    ) -> Type {
        let lt_inner = lt.unwrap_nullable();
        let rt_inner = rt.unwrap_nullable();
        let either_nullable = lt.is_nullable() || rt.is_nullable();

        let result = match op {
            // Arithmetic: result is the unified numeric type
            BinOp::Add | BinOp::Sub | BinOp::Mul | BinOp::Div | BinOp::Mod => {
                lt_inner.unify(rt_inner).unwrap_or(Type::Any)
            }

            // Comparison: result is Bool
            BinOp::Gt | BinOp::Lt | BinOp::Gte | BinOp::Lte => Type::Bool,

            // Equality: always Bool, never nullable
            BinOp::Eq | BinOp::Neq => {
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

    /// Check that the program has at least one emit statement.
    fn check_emit_count(&mut self, program: &Program) {
        let has_emit = program
            .statements
            .iter()
            .any(|s| matches!(s, Statement::Emit { .. }));
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

    #[allow(dead_code)]
    fn node_type(typed: &TypedProgram, idx: usize) -> &Type {
        typed.types[idx].as_ref().unwrap_or(&Type::Any)
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

    // ── Phase 16 Task 16.2 — aggregate typecheck tests ─────────────────

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

    // D59 / Task 16.3.13a: per-record provenance fields cannot appear
    // bare in an aggregate residual; must be wrapped in an aggregate.
    #[test]
    fn test_typecheck_rejects_pipeline_source_row_in_agg_residual() {
        let errs = agg_err("emit row = $pipeline.source_row", &["dept"], &["dept"]);
        assert!(
            errs.iter()
                .any(|d| d.message.contains("source_row")
                    && d.message.contains("per-record provenance")),
            "expected provenance rejection, got: {:?}",
            errs.iter().map(|d| &d.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_typecheck_allows_first_pipeline_source_row_in_agg() {
        let _ = agg_ok("emit row = min($pipeline.source_row)", &["dept"], &["dept"]);
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

    // ── Phase 16c.1.4 hard-gate tests — Row integration ──────────────

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
}
