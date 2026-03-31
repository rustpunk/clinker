use std::collections::HashMap;

use indexmap::IndexMap;
use regex::Regex;

use super::types::Type;
use crate::ast::{BinOp, Expr, LiteralValue, NodeId, Program, Statement, UnaryOp};
use crate::builtins::BuiltinRegistry;
use crate::lexer::Span;
use crate::resolve::pass::{ResolvedBinding, ResolvedProgram};

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
    /// Inferred field types for runtime DLQ validation. Deterministic iteration order.
    pub field_types: IndexMap<String, Type>,
    /// Pre-compiled regex literals, indexed by NodeId.
    pub regexes: Vec<Option<Regex>>,
    /// Total node count.
    pub node_count: u32,
}

/// Run Phase C: type-check a resolved program.
/// `schema` contains explicitly declared field types from YAML — these override inference.
pub fn type_check(
    resolved: ResolvedProgram,
    schema: &HashMap<String, Type>,
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
    };

    // Pass 1: Constraint collection + Pass 2: Bottom-up annotation (single walk)
    for stmt in &program.statements {
        checker.check_statement(stmt);
    }

    // Unify per-field constraints
    checker.unify_field_constraints();

    // Phase C semantic checks
    checker.check_emit_count(&program);

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
    schema: &'a HashMap<String, Type>,
    registry: &'a BuiltinRegistry,
    types: Vec<Option<Type>>,
    regexes: Vec<Option<Regex>>,
    field_constraints: HashMap<String, Vec<FieldConstraint>>,
    diagnostics: Vec<TypeDiagnostic>,
    // Note: in_predicate_expr tracking is done via the resolver's context,
    // but we track it here for the nested-window check
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

    #[allow(dead_code)]
    fn error_with_related(
        &mut self,
        span: Span,
        message: String,
        help: Option<String>,
        related: Span,
    ) {
        self.diagnostics.push(TypeDiagnostic {
            span,
            message,
            help,
            related_span: Some(related),
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
                        // Check schema for declared type
                        if let Some(declared) = self.schema.get(&**name) {
                            declared.clone()
                        } else {
                            Type::Any // Will be inferred from usage
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

            Expr::QualifiedFieldRef { node_id, .. } => {
                self.set_type(*node_id, Type::Any);
                Type::Any
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

            Expr::Now { node_id, .. } => {
                self.set_type(*node_id, Type::DateTime);
                Type::DateTime
            }

            Expr::Wildcard { node_id, .. } => {
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
                        format!("window.{}() cannot be called inside a window.any() or window.all() predicate", function),
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
            if let Some(declared) = self.schema.get(field) {
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
    fn build_field_type_map(&self) -> IndexMap<String, Type> {
        let mut map = IndexMap::new();

        // Schema-declared types first (authoritative)
        for (field, ty) in self.schema {
            map.insert(field.clone(), ty.clone());
        }

        // Inferred types from constraints
        for (field, constraints) in &self.field_constraints {
            if map.contains_key(field) {
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
                map.insert(field.clone(), unified);
            }
        }

        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;
    use crate::resolve::pass::resolve_program;

    fn typecheck_ok(src: &str, fields: &[&str], schema: &HashMap<String, Type>) -> TypedProgram {
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

    fn typecheck_err(
        src: &str,
        fields: &[&str],
        schema: &HashMap<String, Type>,
    ) -> Vec<TypeDiagnostic> {
        let parsed = Parser::parse(src);
        assert!(parsed.errors.is_empty());
        let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap();
        type_check(resolved, schema).expect_err("Expected type errors but got Ok")
    }

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
        let typed = typecheck_ok("emit a = 1 + 2", &[], &HashMap::new());
        assert_eq!(first_emit_expr_type(&typed), Type::Int);

        // 1 + 2.0 infers Float (promotion)
        let typed2 = typecheck_ok("emit b = 1 + 2.0", &[], &HashMap::new());
        assert_eq!(first_emit_expr_type(&typed2), Type::Float);
    }

    #[test]
    fn test_typecheck_null_propagation() {
        // nullable_field + 1 → Nullable(Int) when field is declared Nullable(Int)
        let mut schema = HashMap::new();
        schema.insert("nullable_field".into(), Type::Nullable(Box::new(Type::Int)));
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
        let mut schema = HashMap::new();
        schema.insert("nullable_field".into(), Type::Nullable(Box::new(Type::Int)));
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
            &HashMap::new(),
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
            "emit val = window.any(window.sum(it) > 0)",
            &["amount"],
            &HashMap::new(),
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
        let mut schema = HashMap::new();
        schema.insert("name".into(), Type::String);
        let diags = typecheck_err("emit val = window.sum(name)", &["name"], &schema);
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
            &HashMap::new(),
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
        let mut schema = HashMap::new();
        schema.insert("age".into(), Type::Int);
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
        let typed = typecheck_ok("let x = 1", &[], &HashMap::new());
        // The program should have been created successfully (warnings don't block)
        assert!(typed.program.statements.len() == 1);
    }

    #[test]
    fn test_typecheck_field_type_map_produced() {
        let typed = typecheck_ok(
            "emit a = amount + 1\nemit b = name == \"test\"",
            &["amount", "name"],
            &HashMap::new(),
        );
        // amount should be inferred as Numeric, name as String
        assert!(
            typed.field_types.contains_key("amount"),
            "Expected amount in field_types, got: {:?}",
            typed.field_types
        );
        assert!(
            typed.field_types.contains_key("name"),
            "Expected name in field_types, got: {:?}",
            typed.field_types
        );
    }

    #[test]
    fn test_typecheck_equality_always_bool() {
        let typed = typecheck_ok("emit val = 1 == 2", &[], &HashMap::new());
        assert_eq!(first_emit_expr_type(&typed), Type::Bool);
    }

    #[test]
    fn test_typecheck_if_missing_else_nullable() {
        let typed = typecheck_ok("emit val = if true then 1", &[], &HashMap::new());
        let ty = first_emit_expr_type(&typed);
        assert!(
            ty.is_nullable(),
            "Expected Nullable type for if-then without else, got {}",
            ty
        );
    }

    #[test]
    fn test_typecheck_now_is_datetime() {
        let typed = typecheck_ok("emit ts = now", &[], &HashMap::new());
        assert_eq!(first_emit_expr_type(&typed), Type::DateTime);
    }

    #[test]
    fn test_typecheck_pipeline_access_types() {
        let typed = typecheck_ok("emit ts = pipeline.start_time", &[], &HashMap::new());
        assert_eq!(first_emit_expr_type(&typed), Type::DateTime);

        let typed2 = typecheck_ok("emit n = pipeline.total_count", &[], &HashMap::new());
        assert_eq!(first_emit_expr_type(&typed2), Type::Int);
    }

    #[test]
    fn test_typecheck_regex_precompiled() {
        let typed = typecheck_ok(
            "emit val = name.matches(\"\\\\d+\")",
            &["name"],
            &HashMap::new(),
        );
        // The regex should be pre-compiled and stored
        let has_regex = typed.regexes.iter().any(|r| r.is_some());
        assert!(
            has_regex,
            "Expected pre-compiled regex in TypedProgram.regexes"
        );
    }
}
