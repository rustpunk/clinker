//! Phase D: AST dependency analysis.
//!
//! Walks a `TypedProgram` to extract window usage, field access patterns,
//! and parallelism hints. Produces an `AnalysisReport` consumed by
//! `clinker-core::plan` for Phases E and F.

use std::collections::HashSet;

use crate::ast::{Expr, MatchArm, Statement};
use crate::typecheck::pass::TypedProgram;

/// Which category of window function was called.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFunction {
    /// first, last, lag, lead — return a record view
    Positional,
    /// count, sum, avg, min, max — return a scalar
    Aggregate,
    /// any, all — iterate with a predicate
    Iterable,
    /// collect, distinct — return an array
    Collector,
}

/// Information about a single `window.*` call site in the AST.
#[derive(Debug, Clone)]
pub struct WindowCallInfo {
    /// The function name (e.g., "sum", "first", "lag")
    pub function_name: Box<str>,
    /// Which category this function belongs to
    pub category: WindowFunction,
    /// Field names passed as arguments (e.g., "amount" in `window.sum(amount)`)
    pub field_args: Vec<String>,
    /// Fields accessed on positional results via postfix chains
    /// (e.g., "name" from `window.first().name`)
    pub postfix_fields: Vec<String>,
}

/// AST-level parallelism hint derived from window usage patterns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParallelismHint {
    /// No `window.*` references — fully parallelizable
    Stateless,
    /// References `window.*` but only reads from immutable arena
    IndexReading,
    /// Contains positional functions with ordering dependency (lag/lead)
    Sequential,
}

/// Per-transform analysis results from Phase D.
#[derive(Debug, Clone)]
pub struct TransformAnalysis {
    /// Transform name from config
    pub name: String,
    /// All window function call sites found in this transform's CXL
    pub window_calls: Vec<WindowCallInfo>,
    /// All field names accessed through window expressions
    /// (union of field_args + postfix_fields across all window calls)
    pub accessed_fields: HashSet<String>,
    /// Parallelism hint derived from window usage pattern
    pub parallelism_hint: ParallelismHint,
}

/// Complete analysis report for all transforms in a pipeline.
#[derive(Debug, Clone)]
pub struct AnalysisReport {
    pub transforms: Vec<TransformAnalysis>,
}

/// Analyze a single compiled CXL program, producing a `TransformAnalysis`.
///
/// `name`: the transform's name from config.
/// `typed`: the compiled (parsed → resolved → type-checked) CXL program.
pub fn analyze_transform(name: &str, typed: &TypedProgram) -> TransformAnalysis {
    let mut window_calls = Vec::new();
    let mut accessed_fields = HashSet::new();

    let has_distinct = typed
        .program
        .statements
        .iter()
        .any(|s| matches!(s, Statement::Distinct { .. }));

    for stmt in &typed.program.statements {
        walk_statement(stmt, &mut window_calls, &mut accessed_fields);
    }

    // Distinct forces Sequential — mutable HashSet state cannot be parallelized.
    let parallelism_hint = if has_distinct {
        ParallelismHint::Sequential
    } else {
        classify_parallelism(&window_calls)
    };

    TransformAnalysis {
        name: name.to_string(),
        window_calls,
        accessed_fields,
        parallelism_hint,
    }
}

/// Analyze all transforms, producing a complete `AnalysisReport`.
pub fn analyze_all(transforms: &[(&str, &TypedProgram)]) -> AnalysisReport {
    let transforms = transforms
        .iter()
        .map(|(name, typed)| analyze_transform(name, typed))
        .collect();
    AnalysisReport { transforms }
}

/// Classify parallelism from window call patterns.
fn classify_parallelism(calls: &[WindowCallInfo]) -> ParallelismHint {
    if calls.is_empty() {
        return ParallelismHint::Stateless;
    }

    // If any call is positional (lag/lead/first/last), treat as Sequential
    // because positional access depends on partition ordering
    let has_positional = calls
        .iter()
        .any(|c| c.category == WindowFunction::Positional);
    if has_positional {
        return ParallelismHint::Sequential;
    }

    // Pure aggregates and iterables are IndexReading — they read immutable arena data
    ParallelismHint::IndexReading
}

// --- AST walkers ---

fn walk_statement(stmt: &Statement, calls: &mut Vec<WindowCallInfo>, fields: &mut HashSet<String>) {
    match stmt {
        Statement::Let { expr, .. } => walk_expr(expr, calls, fields, None),
        Statement::Emit { expr, .. } => walk_expr(expr, calls, fields, None),
        Statement::Trace { guard, message, .. } => {
            if let Some(g) = guard {
                walk_expr(g, calls, fields, None);
            }
            walk_expr(message, calls, fields, None);
        }
        Statement::ExprStmt { expr, .. } => walk_expr(expr, calls, fields, None),
        Statement::UseStmt { .. } => {}
        Statement::Filter { predicate, .. } => walk_expr(predicate, calls, fields, None),
        Statement::Distinct { .. } => {}
    }
}

/// Walk an expression tree, collecting window calls and accessed fields.
///
/// `postfix_target`: when we're walking a postfix chain on a WindowCall result,
/// this collects the field names accessed (e.g., `window.first().name` → "name").
fn walk_expr(
    expr: &Expr,
    calls: &mut Vec<WindowCallInfo>,
    fields: &mut HashSet<String>,
    postfix_target: Option<&mut Vec<String>>,
) {
    match expr {
        Expr::WindowCall { function, args, .. } => {
            let category = match function.as_ref() {
                "first" | "last" | "lag" | "lead" => WindowFunction::Positional,
                "count" | "sum" | "avg" | "min" | "max" => WindowFunction::Aggregate,
                "any" | "all" => WindowFunction::Iterable,
                "collect" | "distinct" => WindowFunction::Collector,
                _ => WindowFunction::Aggregate, // fallback
            };

            // Extract field name arguments
            let field_args: Vec<String> = args
                .iter()
                .filter_map(|arg| {
                    if let Expr::FieldRef { name, .. } = arg {
                        Some(name.to_string())
                    } else {
                        None
                    }
                })
                .collect();

            // Add field args to the accessed set
            for f in &field_args {
                fields.insert(f.clone());
            }

            // Walk args for nested expressions
            for arg in args {
                walk_expr(arg, calls, fields, None);
            }

            let call_info = WindowCallInfo {
                function_name: function.clone(),
                category,
                field_args,
                postfix_fields: Vec::new(), // filled by parent MethodCall if any
            };
            calls.push(call_info);
        }

        // MethodCall on a WindowCall result: `window.first().name` appears as
        // MethodCall { receiver: WindowCall("first"), method: "name", args: [] }
        // We need to detect when the receiver is a WindowCall and collect the method
        // as a postfix field.
        Expr::MethodCall {
            receiver,
            method,
            args,
            ..
        } => {
            // Check if receiver is a WindowCall (or chain leading to one)
            if is_window_call_chain(receiver) {
                // This method name is a field access on the window result
                let field_name = method.to_string();
                fields.insert(field_name.clone());

                // Walk the receiver to collect the WindowCall itself
                let mut postfix_fields = vec![field_name];
                walk_expr(receiver, calls, fields, Some(&mut postfix_fields));

                // Attach postfix fields to the last WindowCall we found
                if let Some(last_call) = calls.last_mut() {
                    last_call.postfix_fields.extend(postfix_fields);
                }
            } else {
                // Normal method call — walk receiver and args
                walk_expr(receiver, calls, fields, None);
            }

            for arg in args {
                walk_expr(arg, calls, fields, None);
            }
        }

        // Field access. Any bare `FieldRef` means this transform needs
        // the field from the upstream record at runtime — Option W's
        // arena projection must include it, or the evaluator will read
        // Null from a missing slot. This was a latent bug before the
        // arena/DAG unification: the analyzer tracked only
        // window-argument fields, so plain `emit f1 = f1` inside a
        // window-using transform silently dropped f1.
        //
        // `postfix_target` is additionally collected for
        // `window.first().name`-style access (so we can attach the
        // postfix to the preceding WindowCall for aggregate-op dispatch).
        Expr::FieldRef { name, .. } => {
            fields.insert(name.to_string());
            if let Some(postfix) = postfix_target {
                postfix.push(name.to_string());
            }
        }

        // Recurse into all other expression types
        Expr::Binary { lhs, rhs, .. } => {
            walk_expr(lhs, calls, fields, None);
            walk_expr(rhs, calls, fields, None);
        }
        Expr::Unary { operand, .. } => {
            walk_expr(operand, calls, fields, None);
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            walk_expr(condition, calls, fields, None);
            walk_expr(then_branch, calls, fields, None);
            if let Some(e) = else_branch {
                walk_expr(e, calls, fields, None);
            }
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            walk_expr(lhs, calls, fields, None);
            walk_expr(rhs, calls, fields, None);
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                walk_expr(s, calls, fields, None);
            }
            for arm in arms {
                walk_match_arm(arm, calls, fields);
            }
        }
        Expr::AggCall { args, .. } => {
            for arg in args {
                walk_expr(arg, calls, fields, None);
            }
        }
        Expr::Literal { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::PipelineAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::MetaAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }
}

fn walk_match_arm(arm: &MatchArm, calls: &mut Vec<WindowCallInfo>, fields: &mut HashSet<String>) {
    walk_expr(&arm.pattern, calls, fields, None);
    walk_expr(&arm.body, calls, fields, None);
}

/// Check if an expression is a WindowCall or a chain of method calls ending at a WindowCall.
fn is_window_call_chain(expr: &Expr) -> bool {
    match expr {
        Expr::WindowCall { .. } => true,
        Expr::MethodCall { receiver, .. } => is_window_call_chain(receiver),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Span;
    use crate::parser::Parser;
    use crate::resolve::pass::resolve_program;
    use crate::typecheck::pass::type_check;
    use crate::typecheck::row::Row;

    /// Helper: compile CXL source to TypedProgram for testing the analyzer.
    fn compile(source: &str) -> TypedProgram {
        let parsed = Parser::parse(source);
        assert!(
            parsed.errors.is_empty(),
            "Parse errors: {:?}",
            parsed.errors
        );
        let fields: Vec<&str> = vec!["dept", "amount", "name", "region", "score", "date"];
        let resolved = resolve_program(parsed.ast, &fields, parsed.node_count).unwrap();
        let schema = Row::closed(indexmap::IndexMap::new(), Span::new(0, 0));
        type_check(resolved, &schema).unwrap()
    }

    #[test]
    fn test_analyzer_no_windows() {
        let typed = compile("let x = amount + 1\nemit result = x * 2");
        let analysis = analyze_transform("test", &typed);
        assert!(analysis.window_calls.is_empty());
        // Under Option W, `accessed_fields` tracks every FieldRef the
        // transform reads (not just fields reached via window calls) so
        // the arena projection covers every field needed at runtime.
        // `amount` is referenced in the `let` expression.
        assert!(
            analysis.accessed_fields.contains("amount"),
            "amount is referenced in the let binding"
        );
        assert_eq!(analysis.parallelism_hint, ParallelismHint::Stateless);
    }

    #[test]
    fn test_analyzer_aggregate_window() {
        let typed = compile("emit total = $window.sum(amount)");
        let analysis = analyze_transform("test", &typed);
        assert_eq!(analysis.window_calls.len(), 1);
        assert_eq!(analysis.window_calls[0].function_name.as_ref(), "sum");
        assert_eq!(analysis.window_calls[0].category, WindowFunction::Aggregate);
        assert!(analysis.accessed_fields.contains("amount"));
        assert_eq!(analysis.parallelism_hint, ParallelismHint::IndexReading);
    }

    #[test]
    fn test_analyzer_positional_window() {
        let typed = compile("emit prev = $window.lag(1)");
        let analysis = analyze_transform("test", &typed);
        assert_eq!(analysis.window_calls.len(), 1);
        assert_eq!(
            analysis.window_calls[0].category,
            WindowFunction::Positional
        );
        assert_eq!(analysis.parallelism_hint, ParallelismHint::Sequential);
    }

    #[test]
    fn test_analyzer_multiple_windows() {
        let typed = compile("emit total = $window.sum(amount)\nemit cnt = $window.count()");
        let analysis = analyze_transform("test", &typed);
        assert_eq!(analysis.window_calls.len(), 2);
        assert_eq!(analysis.parallelism_hint, ParallelismHint::IndexReading);
    }

    #[test]
    fn test_analyzer_mixed_stateless_and_window() {
        let typed =
            compile("let x = amount + 1\nemit total = $window.sum(amount)\nemit doubled = x * 2");
        let analysis = analyze_transform("test", &typed);
        assert_eq!(analysis.window_calls.len(), 1);
        // Still IndexReading because there IS a window call
        assert_eq!(analysis.parallelism_hint, ParallelismHint::IndexReading);
    }
}
