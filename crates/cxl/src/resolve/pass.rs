use std::collections::{HashMap, HashSet};

use super::levenshtein::best_match;
use super::scoped_vars::ScopedVarsRegistry;
use crate::ast::{Expr, MatchArm, NodeId, Program, Statement};
use crate::lexer::Span;

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
    /// A qualified module function call: module_path, fn_name.
    ModuleFunction(Box<str>, Box<str>),
    /// A qualified module constant access: module_path, const_name.
    ModuleConstant(Box<str>, Box<str>),
}

/// Describes what a module exports (functions and constants).
#[derive(Debug, Clone, Default)]
pub struct ModuleExports {
    pub functions: HashSet<String>,
    pub constants: HashSet<String>,
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
    "batch_id",
    "total_count",
    "ok_count",
    "dlq_count",
];

/// The known source.* member names — per-record / per-source provenance.
const SOURCE_MEMBERS: &[&str] = &[
    "file",
    "row",
    "path",
    "count",
    "batch",
    "ingestion_timestamp",
];

/// Run Phase B: resolve all identifiers in the program.
pub fn resolve_program(
    program: Program,
    fields: &[&str],
    node_count: u32,
) -> Result<ResolvedProgram, Vec<ResolveDiagnostic>> {
    resolve_program_with_modules(program, fields, node_count, &HashMap::new())
}

/// Run Phase B with module awareness: resolve all identifiers in the program.
pub fn resolve_program_with_modules(
    program: Program,
    fields: &[&str],
    node_count: u32,
    module_exports: &HashMap<String, ModuleExports>,
) -> Result<ResolvedProgram, Vec<ResolveDiagnostic>> {
    resolve_program_with_modules_and_vars(
        program,
        fields,
        node_count,
        module_exports,
        &ScopedVarsRegistry::default(),
    )
}

/// Run Phase B with module + scoped-vars awareness.
///
/// `scoped_vars` carries the pipeline's declared `$pipeline.<key>` /
/// `$source.<key>` / `$row.<key>` registry. Empty registry preserves the
/// pre-Phase-B behavior — only builtin members of each namespace resolve.
/// Production call sites build the registry from
/// `clinker_core::config::ScopedVarsDecl`.
pub fn resolve_program_with_modules_and_vars(
    program: Program,
    fields: &[&str],
    node_count: u32,
    module_exports: &HashMap<String, ModuleExports>,
    scoped_vars: &ScopedVarsRegistry,
) -> Result<ResolvedProgram, Vec<ResolveDiagnostic>> {
    let mut resolver = Resolver {
        fields,
        let_vars: Vec::new(),
        bindings: vec![None; node_count as usize],
        diagnostics: Vec::new(),
        context: ResolveContext::Primary,
        module_aliases: HashMap::new(),
        module_exports,
        scoped_vars,
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
    /// Maps alias name → module path key (e.g. "dates" → "shared.date_helpers")
    module_aliases: HashMap<String, String>,
    /// Available module exports, keyed by module path
    module_exports: &'a HashMap<String, ModuleExports>,
    /// User-declared `$pipeline.<key>` / `$source.<key>` / `$row.<key>`
    /// registry consulted alongside the builtin member sets.
    scoped_vars: &'a ScopedVarsRegistry,
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
            Statement::UseStmt { path, alias, .. } => {
                let module_key = path.iter().map(|s| &**s).collect::<Vec<_>>().join(".");
                // Alias or last segment of path as the local name
                let local_name = alias
                    .as_ref()
                    .map(|a| a.to_string())
                    .unwrap_or_else(|| path.last().map(|s| s.to_string()).unwrap_or_default());
                self.module_aliases.insert(local_name, module_key);
            }
            Statement::ExprStmt { expr, .. } => {
                self.resolve_expr(expr);
            }
            Statement::Filter { predicate, .. } => {
                self.resolve_expr(predicate);
            }
            Statement::Distinct { .. } => {
                // Distinct references a field name directly, not an expression.
                // No expression resolution needed.
            }
        }
    }

    fn resolve_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::FieldRef {
                node_id,
                name,
                span,
            } => {
                self.resolve_field_ref(*node_id, name, *span);
            }
            Expr::QualifiedFieldRef {
                node_id,
                parts,
                span,
            } => {
                // Check if first part is a module alias → module constant access
                if parts.len() == 2 {
                    let module_lookup = self.module_aliases.get(&*parts[0]).cloned();
                    if let Some(module_key) = module_lookup {
                        let const_name = &*parts[1];
                        if let Some(exports) = self.module_exports.get(&module_key) {
                            if exports.constants.contains(const_name) {
                                self.bind(
                                    *node_id,
                                    ResolvedBinding::ModuleConstant(
                                        module_key.clone().into(),
                                        const_name.into(),
                                    ),
                                );
                                return;
                            }
                            // Not a constant — check if it's a function (missing parens)
                            if exports.functions.contains(const_name) {
                                self.diagnostics.push(ResolveDiagnostic {
                                    span: *span,
                                    message: format!(
                                        "'{}' is a function in module '{}', not a constant",
                                        const_name, parts[0]
                                    ),
                                    help: Some(format!(
                                        "use '{}.{}()' with parentheses to call it",
                                        parts[0], const_name
                                    )),
                                });
                                return;
                            }
                            // Suggest similar names
                            let all_members: Vec<&str> = exports
                                .constants
                                .iter()
                                .map(|s| s.as_str())
                                .chain(exports.functions.iter().map(|s| s.as_str()))
                                .collect();
                            let help = best_match(const_name, &all_members, 3)
                                .map(|s| format!("did you mean '{}'?", s));
                            self.diagnostics.push(ResolveDiagnostic {
                                span: *span,
                                message: format!(
                                    "module '{}' has no member '{}'",
                                    parts[0], const_name
                                ),
                                help,
                            });
                            return;
                        }
                    }
                }
                // Fall through: qualified field lookup — runtime FieldResolver handles resolution.
                self.bind(*node_id, ResolvedBinding::Field(0));
            }
            Expr::PipelineAccess {
                node_id,
                field,
                span,
            } => {
                let is_builtin = PIPELINE_MEMBERS.contains(&&**field);
                let is_declared = self.scoped_vars.pipeline.contains_key(&**field);
                if is_builtin || is_declared {
                    self.bind(*node_id, ResolvedBinding::PipelineMember);
                } else {
                    let declared: Vec<&str> = self
                        .scoped_vars
                        .pipeline
                        .keys()
                        .map(|s| s.as_str())
                        .collect();
                    let candidates: Vec<&str> = PIPELINE_MEMBERS
                        .iter()
                        .copied()
                        .chain(declared.iter().copied())
                        .collect();
                    self.diagnostics.push(ResolveDiagnostic {
                        span: *span,
                        message: format!("unknown pipeline member '$pipeline.{field}'"),
                        help: best_match(field, &candidates, 3)
                            .map(|s| format!("did you mean '$pipeline.{s}'?"))
                            .or_else(|| {
                                Some("declare it in the pipeline `vars.pipeline` block".into())
                            }),
                    });
                }
            }
            Expr::SourceAccess {
                node_id,
                field,
                span,
            } => {
                let is_builtin = SOURCE_MEMBERS.contains(&&**field);
                let is_declared = self.scoped_vars.source.contains_key(&**field);
                if is_builtin || is_declared {
                    self.bind(*node_id, ResolvedBinding::PipelineMember);
                } else {
                    let declared: Vec<&str> =
                        self.scoped_vars.source.keys().map(|s| s.as_str()).collect();
                    let candidates: Vec<&str> = SOURCE_MEMBERS
                        .iter()
                        .copied()
                        .chain(declared.iter().copied())
                        .collect();
                    self.diagnostics.push(ResolveDiagnostic {
                        span: *span,
                        message: format!("unknown source member '$source.{field}'"),
                        help: best_match(field, &candidates, 3)
                            .map(|s| format!("did you mean '$source.{s}'?"))
                            .or_else(|| {
                                Some("declare it in the pipeline `vars.source` block".into())
                            }),
                    });
                }
            }
            Expr::MetaAccess { node_id, .. } => {
                // Metadata keys are runtime-resolved — accept any field name
                self.bind(*node_id, ResolvedBinding::PipelineMember);
            }
            Expr::Now { .. } => {
                // `now` is a keyword, no binding needed — evaluator handles directly
            }
            Expr::Wildcard { .. } => {
                // Wildcards don't resolve to anything
            }
            Expr::AggSlot { .. } | Expr::GroupKey { .. } => {
                // Extractor-produced leaves; never present during resolve pass.
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
            Expr::IfThenElse {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
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
            Expr::MethodCall {
                node_id,
                receiver,
                method,
                args,
                span,
            } => {
                // Check if this is a qualified module function call: module.fn(args)
                if let Expr::FieldRef {
                    name: ref recv_name,
                    ..
                } = **receiver
                {
                    let module_lookup = self.module_aliases.get(&**recv_name).cloned();
                    if let Some(module_key) = module_lookup
                        && let Some(exports) = self.module_exports.get(&module_key)
                    {
                        if exports.functions.contains(&**method) {
                            // Resolve args normally
                            for arg in args {
                                self.resolve_expr(arg);
                            }
                            self.bind(
                                *node_id,
                                ResolvedBinding::ModuleFunction(module_key.into(), method.clone()),
                            );
                            return;
                        }
                        // Not a function — check if it's a constant
                        if exports.constants.contains(&**method) {
                            self.diagnostics.push(ResolveDiagnostic {
                                span: *span,
                                message: format!(
                                    "'{}' is a constant in module '{}', not a function",
                                    method, recv_name
                                ),
                                help: Some(format!(
                                    "use '{}.{}' without parentheses",
                                    recv_name, method
                                )),
                            });
                            return;
                        }
                        let all_members: Vec<&str> = exports
                            .functions
                            .iter()
                            .map(|s| s.as_str())
                            .chain(exports.constants.iter().map(|s| s.as_str()))
                            .collect();
                        let help = best_match(method, &all_members, 3)
                            .map(|s| format!("did you mean '{}'?", s));
                        self.diagnostics.push(ResolveDiagnostic {
                            span: *span,
                            message: format!("module '{}' has no function '{}'", recv_name, method),
                            help,
                        });
                        return;
                    }
                }
                // Normal method call
                self.resolve_expr(receiver);
                for arg in args {
                    self.resolve_expr(arg);
                }
            }
            Expr::WindowCall {
                node_id,
                function,
                args,
                span,
            } => {
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
            Expr::AggCall { args, .. } => {
                for arg in args {
                    self.resolve_expr(arg);
                }
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
                    message: "'it' is only valid inside $window.any() or $window.all() predicates"
                        .into(),
                    help: Some(
                        "Move this expression inside a $window.any() or $window.all() call".into(),
                    ),
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
        let all_names: Vec<&str> = self
            .let_vars
            .iter()
            .map(|s| s.as_str())
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
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        resolve_program(parsed.ast, fields, parsed.node_count).unwrap_or_else(|diags| {
            panic!(
                "Resolve errors: {:?}",
                diags.iter().map(|d| &d.message).collect::<Vec<_>>()
            )
        })
    }

    fn resolve_err(src: &str, fields: &[&str]) -> Vec<ResolveDiagnostic> {
        let parsed = Parser::parse(src);
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        resolve_program(parsed.ast, fields, parsed.node_count)
            .expect_err("Expected resolve errors but got Ok")
    }

    #[test]
    fn test_resolve_simple_field_ref() {
        let resolved = resolve_ok("emit name = first_name", &["first_name"]);
        // Find the FieldRef node's binding
        let has_field_binding = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::Field(0))));
        assert!(
            has_field_binding,
            "Expected Field(0) binding for first_name"
        );
    }

    #[test]
    fn test_resolve_let_binding() {
        let resolved = resolve_ok("let x = 1\nemit val = x", &[]);
        // x should resolve to LetVar(0)
        let has_let_binding = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::LetVar(0))));
        assert!(has_let_binding, "Expected LetVar(0) binding for x");
    }

    #[test]
    fn test_resolve_undeclared_pipeline_member() {
        // After Phase B, undeclared $pipeline.<key> is a hard error;
        // pre-Phase-B it silently resolved to a runtime null. Empty
        // registry (the resolve_ok / resolve_err helpers' default) means
        // the user has declared no pipeline-scope variables.
        let diags = resolve_err("emit val = $pipeline.cutoff_date", &[]);
        assert!(
            diags
                .iter()
                .any(|d| d.message.contains("$pipeline.cutoff_date")),
            "expected diagnostic for undeclared $pipeline member: {diags:?}"
        );
    }

    #[test]
    fn test_resolve_declared_pipeline_member() {
        // Build a registry that declares `cutoff_date`; the resolver
        // should accept the read and bind it as PipelineMember.
        use crate::resolve::scoped_vars::ScopedVarType;
        let mut registry = ScopedVarsRegistry::default();
        registry
            .pipeline
            .insert("cutoff_date".to_string(), ScopedVarType::Date);
        let parsed = Parser::parse("emit ok = $pipeline.cutoff_date");
        assert!(parsed.errors.is_empty());
        let resolved = resolve_program_with_modules_and_vars(
            parsed.ast,
            &[],
            parsed.node_count,
            &HashMap::new(),
            &registry,
        )
        .expect("declared pipeline var should resolve");
        let has_pipeline = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::PipelineMember)));
        assert!(
            has_pipeline,
            "Expected PipelineMember binding for declared cutoff_date"
        );
    }

    #[test]
    fn test_resolve_pipeline_member() {
        let resolved = resolve_ok("emit ts = $pipeline.start_time", &[]);
        let has_pipeline = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::PipelineMember)));
        assert!(
            has_pipeline,
            "Expected PipelineMember binding for pipeline.start_time"
        );
    }

    #[test]
    fn test_resolve_source_member() {
        let resolved = resolve_ok("emit f = $source.file", &[]);
        let has_binding = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::PipelineMember)));
        assert!(has_binding, "Expected binding for $source.file");
    }

    #[test]
    fn test_resolve_unknown_source_member() {
        let diags = resolve_err("emit f = $source.unknown", &[]);
        assert!(
            diags.iter().any(|d| d.message.contains("$source.unknown")),
            "expected diagnostic for unknown $source member: {diags:?}"
        );
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
        assert!(
            diags[0]
                .message
                .contains("'it' is only valid inside $window.any() or $window.all()")
        );
    }

    #[test]
    fn test_resolve_it_inside_predicate_ok() {
        let resolved = resolve_ok("emit has_high = $window.any(it > 100000)", &["salary"]);
        let has_it = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::IteratorBinding)));
        assert!(
            has_it,
            "Expected IteratorBinding for `it` inside window.any()"
        );
    }

    #[test]
    fn test_resolve_let_shadows_field() {
        let resolved = resolve_ok("let name = \"override\"\nemit val = name", &["name"]);
        // The second `name` should resolve to LetVar, not Field
        let has_let = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::LetVar(0))));
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
        let field_bindings: Vec<_> = resolved
            .bindings
            .iter()
            .filter_map(|b| match b {
                Some(ResolvedBinding::Field(i)) => Some(*i),
                _ => None,
            })
            .collect();
        assert!(field_bindings.contains(&0), "Expected Field(0) for x");
        assert!(field_bindings.contains(&1), "Expected Field(1) for y");
    }

    // ── Module resolution tests ───────────────────────────────────

    fn make_module_exports(fns: &[&str], consts: &[&str]) -> ModuleExports {
        ModuleExports {
            functions: fns.iter().map(|s| s.to_string()).collect(),
            constants: consts.iter().map(|s| s.to_string()).collect(),
        }
    }

    fn resolve_with_modules_ok(
        src: &str,
        fields: &[&str],
        modules: &HashMap<String, ModuleExports>,
    ) -> ResolvedProgram {
        let parsed = Parser::parse(src);
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        resolve_program_with_modules(parsed.ast, fields, parsed.node_count, modules).unwrap_or_else(
            |diags| {
                panic!(
                    "Resolve errors: {:?}",
                    diags.iter().map(|d| &d.message).collect::<Vec<_>>()
                )
            },
        )
    }

    fn resolve_with_modules_err(
        src: &str,
        fields: &[&str],
        modules: &HashMap<String, ModuleExports>,
    ) -> Vec<ResolveDiagnostic> {
        let parsed = Parser::parse(src);
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        resolve_program_with_modules(parsed.ast, fields, parsed.node_count, modules)
            .expect_err("Expected resolve errors but got Ok")
    }

    #[test]
    fn test_module_qualified_fn_call() {
        let mut modules = HashMap::new();
        modules.insert(
            "validators".to_string(),
            make_module_exports(&["is_valid_email"], &["MAX_SALARY"]),
        );

        let resolved = resolve_with_modules_ok(
            "use validators\nemit valid = validators.is_valid_email(Email)",
            &["Email"],
            &modules,
        );
        let has_module_fn = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::ModuleFunction(_, _))));
        assert!(has_module_fn, "Expected ModuleFunction binding");
    }

    #[test]
    fn test_module_qualified_constant() {
        let mut modules = HashMap::new();
        modules.insert(
            "validators".to_string(),
            make_module_exports(&["is_valid"], &["MAX_SALARY"]),
        );

        let resolved = resolve_with_modules_ok(
            "use validators\nemit max = validators.MAX_SALARY",
            &[],
            &modules,
        );
        let has_module_const = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::ModuleConstant(_, _))));
        assert!(has_module_const, "Expected ModuleConstant binding");
    }

    #[test]
    fn test_module_qualified_nonexistent_constant() {
        // "MAX" has no member "MAZ" — close enough for levenshtein suggestion
        let mut modules = HashMap::new();
        modules.insert("validators".to_string(), make_module_exports(&[], &["MAX"]));

        let diags =
            resolve_with_modules_err("use validators\nemit val = validators.MAZ", &[], &modules);
        assert_eq!(diags.len(), 1);
        assert!(diags[0].message.contains("has no member 'MAZ'"));
        assert!(
            diags[0].help.as_ref().map_or(false, |h| h.contains("MAX")),
            "Expected suggestion 'MAX', got: {:?}",
            diags[0].help
        );
    }

    #[test]
    fn test_module_use_alias() {
        let mut modules = HashMap::new();
        modules.insert(
            "shared.date_helpers".to_string(),
            make_module_exports(&["parse_date"], &[]),
        );

        let resolved = resolve_with_modules_ok(
            "use shared.date_helpers as dates\nemit d = dates.parse_date(raw)",
            &["raw"],
            &modules,
        );
        let has_module_fn = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::ModuleFunction(_, _))));
        assert!(has_module_fn, "Expected ModuleFunction binding via alias");
    }

    #[test]
    fn test_module_fn_name_shadows_builtin() {
        // Module fn `upper` should resolve to module function, not builtin method
        let mut modules = HashMap::new();
        modules.insert(
            "validators".to_string(),
            make_module_exports(&["upper"], &[]),
        );

        let resolved = resolve_with_modules_ok(
            "use validators\nemit u = validators.upper(Name)",
            &["Name"],
            &modules,
        );
        let has_module_fn = resolved
            .bindings
            .iter()
            .any(|b| matches!(b, Some(ResolvedBinding::ModuleFunction(_, _))));
        assert!(
            has_module_fn,
            "Expected ModuleFunction binding for shadowed name"
        );
    }
}
