use crate::ast::{Expr, MatchArm, NodeId, Program, Statement};
use crate::lexer::Span;
use super::levenshtein::best_match;

/// What a resolved identifier binds to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolvedBinding {
    /// A field from the input record schema. Index into the field list.
    Field(usize),
    /// A let-bound variable. Index into the let-binding order.
    LetVar(usize),
    /// A pipeline.* member (start_time, name, execution_id, counters, etc.).
    PipelineMember,
    /// A built-in or module function.
    Function,
    /// The `it` binding inside a predicate_expr (window.any/all argument).
    IteratorBinding,
}

/// Tracks whether we are resolving inside a predicate_expr (window.any/all argument).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResolveContext {
    Primary,
    PredicateExpr,
}

/// A diagnostic produced by the resolver pass.
#[derive(Debug, Clone)]
pub struct ResolveDiagnostic {
    pub span: Span,
    pub message: String,
    pub help: Option<String>,
}

/// Output of the resolver pass. Distinct type from Program — compiler enforces
/// that unresolved ASTs cannot be passed to the type checker.
#[derive(Debug)]
pub struct ResolvedProgram {
    pub program: Program,
    /// Side-table mapping NodeId → ResolvedBinding. Indexed by NodeId.0.
    /// Only populated for identifier-bearing nodes (FieldRef, QualifiedFieldRef, PipelineAccess).
    pub bindings: Vec<Option<ResolvedBinding>>,
    /// Total node count (for pre-sizing downstream side-tables).
    pub node_count: u32,
}

/// The known pipeline.* member names.
const PIPELINE_MEMBERS: &[&str] = &[
    "start_time",
    "name",
    "execution_id",
    "total_count",
    "ok_count",
    "dlq_count",
    "source_file",
    "source_row",
];

/// Run Phase B: resolve all identifiers in the program.
pub fn resolve_program(
    program: Program,
    fields: &[&str],
    node_count: u32,
) -> Result<ResolvedProgram, Vec<ResolveDiagnostic>> {
    let mut resolver = Resolver {
        fields,
        let_vars: Vec::new(),
        bindings: vec![None; node_count as usize],
        diagnostics: Vec::new(),
        context: ResolveContext::Primary,
    };

    for stmt in &program.statements {
        resolver.resolve_statement(stmt);
    }

    if resolver.diagnostics.is_empty() {
        Ok(ResolvedProgram {
            program,
            bindings: resolver.bindings,
            node_count,
        })
    } else {
        Err(resolver.diagnostics)
    }
}

struct Resolver<'a> {
    fields: &'a [&'a str],
    let_vars: Vec<String>,
    bindings: Vec<Option<ResolvedBinding>>,
    diagnostics: Vec<ResolveDiagnostic>,
    context: ResolveContext,
}

impl<'a> Resolver<'a> {
    fn bind(&mut self, node_id: NodeId, binding: ResolvedBinding) {
        let idx = node_id.0 as usize;
        if idx < self.bindings.len() {
            self.bindings[idx] = Some(binding);
        }
    }

    fn resolve_statement(&mut self, stmt: &Statement) {
        match stmt {
            Statement::Let { expr, name, .. } => {
                self.resolve_expr(expr);
                self.let_vars.push(name.to_string());
            }
            Statement::Emit { expr, .. } => {
                self.resolve_expr(expr);
            }
            Statement::Trace { guard, message, .. } => {
                if let Some(g) = guard {
                    self.resolve_expr(g);
                }
                self.resolve_expr(message);
            }
            Statement::UseStmt { .. } => {
                // Module resolution is Phase 10 scope
            }
            Statement::ExprStmt { expr, .. } => {
                self.resolve_expr(expr);
            }
        }
    }

    fn resolve_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::FieldRef { node_id, name, span } => {
                self.resolve_field_ref(*node_id, name, *span);
            }
            Expr::QualifiedFieldRef { node_id, source, field, span } => {
                // For now, treat source.field as a qualified field lookup
                // The runtime FieldResolver handles the actual lookup
                self.bind(*node_id, ResolvedBinding::Field(0));
                // Also try to resolve the source as a known identifier
                let _ = (source, field, span);
            }
            Expr::PipelineAccess { node_id, field, span } => {
                if PIPELINE_MEMBERS.contains(&&**field) {
                    self.bind(*node_id, ResolvedBinding::PipelineMember);
                } else {
                    // Could be a user-defined pipeline.vars.* variable
                    // For now, accept anything under pipeline.* and let runtime resolve
                    self.bind(*node_id, ResolvedBinding::PipelineMember);
                    let _ = span;
                }
            }
            Expr::Now { .. } => {
                // `now` is a keyword, no binding needed — evaluator handles directly
            }
            Expr::Wildcard { .. } => {
                // Wildcards don't resolve to anything
            }
            Expr::Literal { .. } => {
                // Literals don't need resolution
            }
            Expr::Binary { lhs, rhs, .. } => {
                self.resolve_expr(lhs);
                self.resolve_expr(rhs);
            }
            Expr::Unary { operand, .. } => {
                self.resolve_expr(operand);
            }
            Expr::Coalesce { lhs, rhs, .. } => {
                self.resolve_expr(lhs);
                self.resolve_expr(rhs);
            }
            Expr::IfThenElse { condition, then_branch, else_branch, .. } => {
                self.resolve_expr(condition);
                self.resolve_expr(then_branch);
                if let Some(eb) = else_branch {
                    self.resolve_expr(eb);
                }
            }
            Expr::Match { subject, arms, .. } => {
                if let Some(s) = subject {
                    self.resolve_expr(s);
                }
                for arm in arms {
                    self.resolve_match_arm(arm);
                }
            }
            Expr::MethodCall { receiver, args, .. } => {
                self.resolve_expr(receiver);
                for arg in args {
                    self.resolve_expr(arg);
                }
            }
            Expr::WindowCall { node_id, function, args, span } => {
                // Check if this is any/all — their arguments are predicate_expr context
                let is_predicate = &**function == "any" || &**function == "all";
                if is_predicate {
                    let prev_context = self.context;
                    self.context = ResolveContext::PredicateExpr;
                    for arg in args {
                        self.resolve_expr(arg);
                    }
                    self.context = prev_context;
                } else {
                    for arg in args {
                        self.resolve_expr(arg);
                    }
                }
                let _ = (node_id, span);
            }
        }
    }

    fn resolve_match_arm(&mut self, arm: &MatchArm) {
        self.resolve_expr(&arm.pattern);
        self.resolve_expr(&arm.body);
    }

    fn resolve_field_ref(&mut self, node_id: NodeId, name: &str, span: Span) {
        // Check for `it` binding
        if name == "it" {
            if self.context == ResolveContext::PredicateExpr {
                self.bind(node_id, ResolvedBinding::IteratorBinding);
                return;
            } else {
                self.diagnostics.push(ResolveDiagnostic {
                    span,
                    message: "'it' is only valid inside window.any() or window.all() predicates".into(),
                    help: Some("Move this expression inside a window.any() or window.all() call".into()),
                });
                return;
            }
        }

        // Check let-bound variables (search most recent first for shadowing)
        for (i, var) in self.let_vars.iter().enumerate().rev() {
            if var == name {
                self.bind(node_id, ResolvedBinding::LetVar(i));
                return;
            }
        }

        // Check field references
        for (i, field) in self.fields.iter().enumerate() {
            if *field == name {
                self.bind(node_id, ResolvedBinding::Field(i));
                return;
            }
        }

        // Unresolved — produce diagnostic with fuzzy match suggestion
        let all_names: Vec<&str> = self.let_vars.iter().map(|s| s.as_str())
            .chain(self.fields.iter().copied())
            .collect();

        let help = best_match(name, &all_names, 3)
            .map(|suggestion| format!("did you mean '{}'?", suggestion));

        self.diagnostics.push(ResolveDiagnostic {
            span,
            message: format!("unresolved identifier '{}'", name),
            help,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    fn resolve_ok(src: &str, fields: &[&str]) -> ResolvedProgram {
        let parsed = Parser::parse(src);
        assert!(parsed.errors.is_empty(), "Parse errors: {:?}", parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>());
        resolve_program(parsed.ast, fields, parsed.node_count)
            .unwrap_or_else(|diags| panic!("Resolve errors: {:?}", diags.iter().map(|d| &d.message).collect::<Vec<_>>()))
    }

    fn resolve_err(src: &str, fields: &[&str]) -> Vec<ResolveDiagnostic> {
        let parsed = Parser::parse(src);
        assert!(parsed.errors.is_empty(), "Parse errors: {:?}", parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>());
        resolve_program(parsed.ast, fields, parsed.node_count)
            .expect_err("Expected resolve errors but got Ok")
    }

    #[test]
    fn test_resolve_simple_field_ref() {
        let resolved = resolve_ok("emit name = first_name", &["first_name"]);
        // Find the FieldRef node's binding
        let has_field_binding = resolved.bindings.iter().any(|b| matches!(b, Some(ResolvedBinding::Field(0))));
        assert!(has_field_binding, "Expected Field(0) binding for first_name");
    }

    #[test]
    fn test_resolve_let_binding() {
        let resolved = resolve_ok("let x = 1\nemit val = x", &[]);
        // x should resolve to LetVar(0)
        let has_let_binding = resolved.bindings.iter().any(|b| matches!(b, Some(ResolvedBinding::LetVar(0))));
        assert!(has_let_binding, "Expected LetVar(0) binding for x");
    }

    #[test]
    fn test_resolve_pipeline_member() {
        let resolved = resolve_ok("emit ts = pipeline.start_time", &[]);
        let has_pipeline = resolved.bindings.iter().any(|b| matches!(b, Some(ResolvedBinding::PipelineMember)));
        assert!(has_pipeline, "Expected PipelineMember binding for pipeline.start_time");
    }

    #[test]
    fn test_resolve_unresolved_with_suggestion() {
        let diags = resolve_err("emit val = naem", &["name"]);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("unresolved identifier 'naem'"));
        assert_eq!(diags[0].help.as_deref(), Some("did you mean 'name'?"));
    }

    #[test]
    fn test_resolve_it_outside_predicate_error() {
        let diags = resolve_err("emit val = it", &[]);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("'it' is only valid inside window.any() or window.all()"));
    }

    #[test]
    fn test_resolve_it_inside_predicate_ok() {
        let resolved = resolve_ok("emit has_high = window.any(it > 100000)", &["salary"]);
        let has_it = resolved.bindings.iter().any(|b| matches!(b, Some(ResolvedBinding::IteratorBinding)));
        assert!(has_it, "Expected IteratorBinding for `it` inside window.any()");
    }

    #[test]
    fn test_resolve_let_shadows_field() {
        let resolved = resolve_ok("let name = \"override\"\nemit val = name", &["name"]);
        // The second `name` should resolve to LetVar, not Field
        let has_let = resolved.bindings.iter().any(|b| matches!(b, Some(ResolvedBinding::LetVar(0))));
        assert!(has_let, "Expected LetVar(0) — let should shadow field");
    }

    #[test]
    fn test_resolve_now_keyword_no_binding() {
        // `now` is a keyword, not a field — no binding needed
        let resolved = resolve_ok("emit ts = now", &[]);
        // Should succeed without errors — `now` is an Expr::Now, not a FieldRef
        assert!(resolved.program.statements.len() == 1);
    }

    #[test]
    fn test_resolve_multiple_fields() {
        let resolved = resolve_ok("emit a = x\nemit b = y", &["x", "y"]);
        let field_bindings: Vec<_> = resolved.bindings.iter()
            .filter_map(|b| match b {
                Some(ResolvedBinding::Field(i)) => Some(*i),
                _ => None,
            })
            .collect();
        assert!(field_bindings.contains(&0), "Expected Field(0) for x");
        assert!(field_bindings.contains(&1), "Expected Field(1) for y");
    }
}
