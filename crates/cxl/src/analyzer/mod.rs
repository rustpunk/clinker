//! Phase D: AST dependency analysis.
//!
//! Walks a `TypedProgram` to extract window usage, field access patterns,
//! and parallelism hints. Produces an `AnalysisReport` consumed by
//! `clinker-plan::plan` for Phases E and F.

use std::collections::HashSet;

use crate::ast::{Expr, Statement};
use crate::typecheck::pass::TypedProgram;

pub mod doc_paths;
pub mod visitor;

use visitor::{Visitor, walk_expr};

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
    /// Field names accessed through window expressions (union of
    /// `field_args` + `postfix_fields` across all window calls).
    ///
    /// Window-analysis scoped: it records only the bare `FieldRef`s reached
    /// while walking window-call argument and postfix chains, and does not
    /// record `QualifiedFieldRef`s. It is therefore NOT the lineage support
    /// set. For the columns an expression or program *reads* — qualified
    /// `input.field` references included — use
    /// [`crate::ast::Expr::support_into`] / [`crate::ast::program_support_into`].
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
    let has_distinct = typed
        .program
        .statements
        .iter()
        .any(|s| matches!(s, Statement::Distinct { .. }));

    let mut collector = WindowCollector::default();
    for stmt in &typed.program.statements {
        collector.visit_statement(stmt);
    }

    // Distinct forces Sequential — mutable HashSet state cannot be parallelized.
    let parallelism_hint = if has_distinct {
        ParallelismHint::Sequential
    } else {
        classify_parallelism(&collector.window_calls)
    };

    TransformAnalysis {
        name: name.to_string(),
        window_calls: collector.window_calls,
        accessed_fields: collector.accessed_fields,
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

/// Collects every `window.*` call site and every field reference a
/// transform's CXL reads, driving the shared typed-AST [`Visitor`] walk.
///
/// Only the nodes that carry analysis meaning are overridden — `WindowCall`
/// (records the call and its argument fields), `MethodCall` (detects a
/// postfix field access on a window result, e.g. `window.first().name`),
/// and `FieldRef` (every bare column the transform needs from the upstream
/// record). Every other node falls through to the default descent.
#[derive(Default)]
struct WindowCollector {
    window_calls: Vec<WindowCallInfo>,
    accessed_fields: HashSet<String>,
    /// Set while walking the receiver of a window-result method chain so a
    /// field access in that chain attaches to the originating
    /// `WindowCall`'s `postfix_fields`. `None` outside such a walk.
    postfix_target: Option<Vec<String>>,
}

impl Visitor for WindowCollector {
    fn visit_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::WindowCall { function, args, .. } => {
                let category = match function.as_ref() {
                    "first" | "last" | "lag" | "lead" => WindowFunction::Positional,
                    "count" | "sum" | "avg" | "min" | "max" => WindowFunction::Aggregate,
                    "any" | "all" => WindowFunction::Iterable,
                    "collect" | "distinct" => WindowFunction::Collector,
                    _ => WindowFunction::Aggregate, // fallback
                };

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

                for f in &field_args {
                    self.accessed_fields.insert(f.clone());
                }

                // Argument sub-expressions never belong to the enclosing
                // window-result postfix chain, so suspend any active target
                // while descending into them.
                let saved = self.postfix_target.take();
                for arg in args {
                    self.visit_expr(arg);
                }
                self.postfix_target = saved;

                self.window_calls.push(WindowCallInfo {
                    function_name: function.clone(),
                    category,
                    field_args,
                    postfix_fields: Vec::new(), // filled by parent MethodCall if any
                });
            }

            // `window.first().name` parses as
            // MethodCall { receiver: WindowCall("first"), method: "name" }.
            // When the receiver bottoms out at a window call, the method
            // name is a field access on the window result — collect it and
            // attach it to that call's postfix fields.
            Expr::MethodCall {
                receiver,
                method,
                args,
                ..
            } => {
                if is_window_call_chain(receiver) {
                    let field_name = method.to_string();
                    self.accessed_fields.insert(field_name.clone());

                    let saved = self.postfix_target.replace(vec![field_name]);
                    self.visit_expr(receiver);
                    let postfix_fields = self.postfix_target.take().unwrap_or_default();
                    self.postfix_target = saved;

                    if let Some(last_call) = self.window_calls.last_mut() {
                        last_call.postfix_fields.extend(postfix_fields);
                    }
                } else {
                    self.visit_expr(receiver);
                }

                let saved = self.postfix_target.take();
                for arg in args {
                    self.visit_expr(arg);
                }
                self.postfix_target = saved;
            }

            // Any bare `FieldRef` means this transform needs the field from
            // the upstream record at runtime — the arena projection must
            // include it, or the evaluator reads Null from a missing slot.
            // The analyzer therefore tracks every FieldRef, not just
            // window-argument fields, so plain `emit f1 = f1` inside a
            // window-using transform does not silently drop f1.
            //
            // While walking a window-result method chain, the field also
            // attaches to that call's postfix fields for aggregate-op
            // dispatch (`window.first().name`).
            Expr::FieldRef { name, .. } => {
                self.accessed_fields.insert(name.to_string());
                if let Some(postfix) = self.postfix_target.as_mut() {
                    postfix.push(name.to_string());
                }
            }

            _ => walk_expr(self, expr),
        }
    }
}

/// True when `expr` is a window call or a method-call chain bottoming out
/// at one.
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
