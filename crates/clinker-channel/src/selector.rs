//! CXL selector evaluation over channel labels.
//!
//! A group's `match:` string (e.g. `region == "west" and tier == "enterprise"`)
//! is a bare CXL boolean expression evaluated against a channel's identity
//! [`labels`](crate::ChannelManifest::labels). Selectors run in a **restricted
//! label-only context**: the only names in scope are the channel's labels.
//! `$record` / `$source` / `$pipeline` / `$vars` / `$doc`, window and aggregate
//! calls, `now`, wildcards, and qualified references are all rejected, so a
//! selector is a pure, deterministic predicate over labels.
//!
//! ## Reuse, not reinvention
//!
//! The evaluation pipeline is the one the Combine `where:` predicate uses —
//! [`cxl::parser::Parser::parse`] → [`cxl::resolve::resolve_program`] →
//! [`cxl::typecheck::type_check`] → [`ProgramEvaluator::eval_record`] — with a
//! label-backed [`HashMapResolver`] in place of a record resolver. No CXL
//! machinery is duplicated here; the match string is wrapped as a `filter`
//! statement so the parser yields a [`Statement::Filter`] whose predicate is
//! the selector.
//!
//! ## Two-phase: compile once, match per channel
//!
//! A group's selector is [`compile`](LabelSelector::compile)d once (parse +
//! label-only validation, both label-independent), then
//! [`matches`](LabelSelector::matches)ed against each channel's label set
//! (name resolution + typecheck + evaluation against that channel's labels).
//!
//! ## Labels are typed from their JSON values
//!
//! Each scalar label is typed by its JSON kind (string → `String`, bool →
//! `Bool`, integer → `Int`, other number → `Float`) so the typechecker rejects
//! label/literal type mismatches. Non-scalar labels (null / array / object)
//! are not comparable in a boolean predicate and are treated as out of scope —
//! a selector that references one fails name resolution, exactly like a typo.
//!
//! ## A misspelled label is a hard error, never a silent `false`
//!
//! Name resolution rejects any bare identifier that is not a declared label
//! ("unresolved identifier"), so a typo surfaces as [`SelectorError::Resolve`]
//! rather than quietly evaluating to `false`. This check must live at resolve
//! time: the typechecker types an unknown field as `Any` and lets it through.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use indexmap::IndexMap;
use serde_json::Value as JsonValue;

use clinker_record::{
    HashMapResolver, PipelineCounters, RecordStorage, Value, synthetic_document_context_ref,
};
use cxl::ast::{Expr, MatchArm, Program, Statement};
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, StableEvalContext, WallClock};
use cxl::lexer::Span;
use cxl::parser::Parser;
use cxl::resolve::resolve_program;
use cxl::typecheck::{QualifiedField, Row, Type, type_check};

/// Errors from compiling or evaluating a channel-label selector.
#[derive(Debug, thiserror::Error)]
pub enum SelectorError {
    /// The `match:` string is not valid CXL.
    #[error("selector parse error: {0}")]
    Parse(String),
    /// The `match:` string is not a single boolean expression (e.g. it parsed
    /// to multiple statements, or to something other than a bare predicate).
    #[error("selector must be a single boolean expression")]
    NotSingleBoolean,
    /// The selector reads outside the channel's labels or is non-deterministic.
    #[error("selector may only reference channel labels, found {0}")]
    ForbiddenReference(&'static str),
    /// Name resolution failed — typically a label referenced by the selector
    /// is not declared on the channel (a typo, or a non-scalar label).
    #[error("selector references an unknown label: {0}")]
    Resolve(String),
    /// The selector is ill-typed — a non-boolean predicate, or a label/literal
    /// type mismatch.
    #[error("selector type error: {0}")]
    Type(String),
    /// Evaluation of a well-typed selector failed at runtime.
    #[error("selector evaluation error: {0}")]
    Eval(String),
}

/// A compiled channel-label selector: a bare CXL boolean over channel `labels`.
///
/// Construct with [`LabelSelector::compile`]; evaluate against a channel's
/// labels with [`LabelSelector::matches`].
#[derive(Debug, Clone)]
pub struct LabelSelector {
    /// Parsed program — a single [`Statement::Filter`] whose predicate is the
    /// selector expression. Cloned per [`matches`](Self::matches) so name
    /// resolution (which consumes the program) can run against each channel's
    /// label set.
    program: Program,
    /// Total AST node count, used to size the resolver's binding table.
    node_count: u32,
    /// The wrapped (`filter …`) source the program was parsed from, kept for
    /// evaluation-error rendering.
    source: Arc<str>,
}

impl LabelSelector {
    /// Compile a group's `match:` string as a bare CXL boolean.
    ///
    /// Performs the label-independent work: parse the expression and reject
    /// every construct that would read outside the channel's labels. Label
    /// name resolution and typechecking happen later, per channel, in
    /// [`matches`](Self::matches).
    pub fn compile(source: &str) -> Result<Self, SelectorError> {
        // Wrap as a `filter` statement so the parser produces a
        // `Statement::Filter` whose predicate is the selector — the same
        // wrapping the Combine `where:` predicate path uses.
        let wrapped = format!("filter {source}");
        let parsed = Parser::parse(&wrapped);
        if !parsed.errors.is_empty() {
            let msg = parsed
                .errors
                .iter()
                .map(|e| e.message.clone())
                .collect::<Vec<_>>()
                .join("; ");
            return Err(SelectorError::Parse(msg));
        }

        // A selector is exactly one boolean predicate. Anything else — multiple
        // statements, an `emit`, a `distinct` — is not a selector.
        let predicate = match parsed.ast.statements.as_slice() {
            [Statement::Filter { predicate, .. }] => predicate,
            _ => return Err(SelectorError::NotSingleBoolean),
        };

        // Enforce the label-only context.
        reject_non_label(predicate)?;

        let node_count = parsed.node_count;
        Ok(Self {
            program: parsed.ast,
            node_count,
            source: Arc::from(wrapped.as_str()),
        })
    }

    /// Evaluate the selector against a channel's `labels`.
    ///
    /// Returns `Ok(true)` when the channel matches, `Ok(false)` when it does
    /// not, and an error when the selector references an undeclared label
    /// (typo / non-scalar), is ill-typed, or fails at runtime — never a silent
    /// `false` for a bad reference.
    pub fn matches(&self, labels: &IndexMap<String, JsonValue>) -> Result<bool, SelectorError> {
        // Build the label schema (name → CXL type) and the runtime label
        // values. Only scalar labels participate; non-scalar labels are out of
        // scope (see the module docs).
        let mut declared: IndexMap<QualifiedField, Type> = IndexMap::new();
        let mut values: HashMap<String, Value> = HashMap::new();
        for (name, json) in labels {
            if let Some((ty, val)) = scalar_label(json) {
                declared.insert(QualifiedField::bare(name.as_str()), ty);
                values.insert(name.clone(), val);
            }
        }
        let field_names: Vec<&str> = declared.keys().map(|qf| qf.name.as_ref()).collect();

        // Phase B — name resolution. A bare identifier that is not a declared
        // label is rejected here, so a misspelled label is a hard error rather
        // than a silent `false`.
        let resolved = resolve_program(self.program.clone(), &field_names, self.node_count)
            .map_err(|diags| {
                SelectorError::Resolve(
                    diags
                        .into_iter()
                        .map(|d| d.message)
                        .collect::<Vec<_>>()
                        .join("; "),
                )
            })?;

        // Phase C — typecheck against the label row. Catches a non-boolean
        // predicate and label/literal type mismatches. The row is closed: the
        // channel's labels are the complete set of names in scope.
        let row = Row::closed(declared, Span::default());
        let typed = type_check(resolved, &row)
            .map_err(|diags| {
                SelectorError::Type(
                    diags
                        .into_iter()
                        .filter(|d| !d.is_warning)
                        .map(|d| d.message)
                        .collect::<Vec<_>>()
                        .join("; "),
                )
            })?
            .with_source(Arc::clone(&self.source));

        // Evaluate the filter predicate against the label-backed resolver. The
        // label-only validation guarantees the evaluator never reads the
        // pipeline/source/doc context, so an inert context is sufficient.
        let resolver = HashMapResolver::new(values);
        let stable = inert_stable_context();
        let placeholder: Arc<str> = Arc::from("");
        let ctx = EvalContext {
            stable: &stable,
            source_file: &placeholder,
            source_row: 0,
            source_path: &placeholder,
            source_count: None,
            source_batch: &placeholder,
            ingestion_timestamp: inert_datetime(),
            source_name: &placeholder,
            doc_ctx: synthetic_document_context_ref(),
        };
        let mut evaluator = ProgramEvaluator::new(Arc::new(typed), false);
        match evaluator.eval_record::<NullStorage>(&ctx, &resolver, None) {
            // A bare `filter` program emits (passes through) on a true
            // predicate and skips on a false one; there is no `emit each`, so
            // `EmitMany` cannot arise, but it is treated as a match defensively.
            Ok(EvalResult::Emit { .. } | EvalResult::EmitMany { .. }) => Ok(true),
            Ok(EvalResult::Skip(_)) => Ok(false),
            Err(e) => Err(SelectorError::Eval(e.to_string())),
        }
    }
}

/// Enforce the label-only context: recurse the selector expression and reject
/// any construct that reads outside the channel's labels or is
/// non-deterministic. Literals and bare [`Expr::FieldRef`]s are the only leaves
/// a selector may use; a field ref that names a non-label is caught later at
/// name-resolution time (see [`LabelSelector::matches`]).
///
/// The match is exhaustive on purpose: a new [`Expr`] variant must be
/// classified here rather than silently admitted into the restricted context.
fn reject_non_label(expr: &Expr) -> Result<(), SelectorError> {
    match expr {
        Expr::Literal { .. } | Expr::FieldRef { .. } => Ok(()),
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            reject_non_label(lhs)?;
            reject_non_label(rhs)
        }
        Expr::Unary { operand, .. } => reject_non_label(operand),
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            reject_non_label(condition)?;
            reject_non_label(then_branch)?;
            if let Some(e) = else_branch {
                reject_non_label(e)?;
            }
            Ok(())
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                reject_non_label(s)?;
            }
            for MatchArm { pattern, body, .. } in arms {
                reject_non_label(pattern)?;
                reject_non_label(body)?;
            }
            Ok(())
        }
        Expr::MethodCall { receiver, args, .. } => {
            reject_non_label(receiver)?;
            for a in args {
                reject_non_label(a)?;
            }
            Ok(())
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            reject_non_label(receiver)?;
            reject_non_label(index)
        }
        // Closures appear only as arguments to closure-bearing builtins (e.g.
        // `.any(it => …)`); the bound `it` is not a label reference.
        Expr::Closure { body, .. } => reject_non_label(body),
        // System namespaces, window/aggregate calls, `now`, wildcards, and
        // qualified references all read outside the label set (or are
        // non-deterministic) and have no place in a label selector.
        Expr::PipelineAccess { .. } => Err(SelectorError::ForbiddenReference("$pipeline")),
        Expr::VarsAccess { .. } => Err(SelectorError::ForbiddenReference("$vars")),
        Expr::SourceAccess { .. } | Expr::QualifiedSourceAccess { .. } => {
            Err(SelectorError::ForbiddenReference("$source"))
        }
        Expr::RecordAccess { .. } => Err(SelectorError::ForbiddenReference("$record")),
        Expr::DocAccess { .. } => Err(SelectorError::ForbiddenReference("$doc")),
        Expr::Now { .. } => Err(SelectorError::ForbiddenReference("now")),
        Expr::WindowCall { .. } => Err(SelectorError::ForbiddenReference("a $window function")),
        Expr::AggCall { .. } | Expr::AggSlot { .. } => {
            Err(SelectorError::ForbiddenReference("an aggregate function"))
        }
        Expr::GroupKey { .. } => Err(SelectorError::ForbiddenReference("a group-by key")),
        Expr::Wildcard { .. } => Err(SelectorError::ForbiddenReference("a wildcard")),
        Expr::QualifiedFieldRef { .. } => {
            Err(SelectorError::ForbiddenReference("a qualified reference"))
        }
    }
}

/// Map a scalar JSON label value to its CXL type and runtime value. Returns
/// `None` for non-scalar labels (null / array / object), which are not
/// comparable in a boolean selector and so are out of the selector's scope.
fn scalar_label(json: &JsonValue) -> Option<(Type, Value)> {
    match json {
        JsonValue::String(s) => Some((Type::String, Value::String(s.as_str().into()))),
        JsonValue::Bool(b) => Some((Type::Bool, Value::Bool(*b))),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some((Type::Int, Value::Integer(i)))
            } else {
                // A u64 beyond i64::MAX or a fractional number resolves to Float.
                n.as_f64().map(|f| (Type::Float, Value::Float(f)))
            }
        }
        JsonValue::Null | JsonValue::Array(_) | JsonValue::Object(_) => None,
    }
}

/// The Unix epoch, used as an inert placeholder timestamp (see
/// [`inert_stable_context`]). A compile-time constant with no fallible path.
fn inert_datetime() -> chrono::NaiveDateTime {
    chrono::DateTime::<chrono::Utc>::UNIX_EPOCH.naive_utc()
}

/// A windowless [`RecordStorage`]. Selectors never reference `$window`
/// functions (rejected at compile time), so no record is ever stored or read;
/// every method returns the empty/absent result.
struct NullStorage;

impl RecordStorage for NullStorage {
    fn resolve_field(&self, _index: u64, _name: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _index: u64, _source: &str, _field: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _index: u64) -> Vec<&str> {
        Vec::new()
    }
    fn record_count(&self) -> u64 {
        0
    }
}

/// Build the inert pipeline-stable context selector evaluation runs against.
///
/// [`LabelSelector::compile`] rejects every `$pipeline` / `$source` /
/// `$record` / `$vars` / `$doc` reference, `now`, and window/aggregate call, so
/// the evaluator never reads any field of this context. These values are
/// placeholders that exist only to satisfy the shared evaluator API and are
/// never observed during selector evaluation.
fn inert_stable_context() -> StableEvalContext {
    let empty: Arc<str> = Arc::from("");
    StableEvalContext {
        clock: Box::new(WallClock),
        pipeline_start_time: inert_datetime(),
        pipeline_name: Arc::clone(&empty),
        pipeline_execution_id: Arc::clone(&empty),
        pipeline_batch_id: empty,
        pipeline_counters: PipelineCounters::default(),
        pipeline_vars: Arc::new(RwLock::new(IndexMap::new())),
        source_vars: Arc::new(RwLock::new(HashMap::new())),
        source_input_arcs: Arc::new(HashMap::new()),
        static_vars: Arc::new(IndexMap::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a label set from `(name, json)` pairs.
    fn labels(pairs: &[(&str, JsonValue)]) -> IndexMap<String, JsonValue> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn matches_all_conjuncts() {
        let sel = LabelSelector::compile(r#"region == "west" and tier == "enterprise""#).unwrap();
        let ls = labels(&[
            ("region", JsonValue::from("west")),
            ("tier", JsonValue::from("enterprise")),
        ]);
        assert!(sel.matches(&ls).unwrap());
    }

    #[test]
    fn no_match_when_a_conjunct_is_false() {
        let sel = LabelSelector::compile(r#"region == "west" and tier == "enterprise""#).unwrap();
        // One conjunct false.
        let east = labels(&[
            ("region", JsonValue::from("east")),
            ("tier", JsonValue::from("enterprise")),
        ]);
        assert!(!sel.matches(&east).unwrap());
        let basic = labels(&[
            ("region", JsonValue::from("west")),
            ("tier", JsonValue::from("basic")),
        ]);
        assert!(!sel.matches(&basic).unwrap());
    }

    #[test]
    fn disjunction_matches_either_branch() {
        let sel = LabelSelector::compile(r#"region == "west" or region == "east""#).unwrap();
        assert!(
            sel.matches(&labels(&[("region", JsonValue::from("east"))]))
                .unwrap()
        );
        assert!(
            !sel.matches(&labels(&[("region", JsonValue::from("south"))]))
                .unwrap()
        );
    }

    #[test]
    fn label_typo_is_error_not_silent_false() {
        // `regon` is a typo for `region`; the channel declares `region`.
        let sel = LabelSelector::compile(r#"regon == "west""#).unwrap();
        let ls = labels(&[("region", JsonValue::from("west"))]);
        let result = sel.matches(&ls);
        // A hard error — crucially NOT a silent `Ok(false)`.
        assert!(
            matches!(result, Err(SelectorError::Resolve(_))),
            "expected a resolve error for the misspelled label, got {result:?}"
        );
    }

    #[test]
    fn constant_selector_is_deterministic_over_any_label_set() {
        // A label-free selector evaluates the same regardless of labels,
        // including against an empty/standalone label set.
        let always = LabelSelector::compile("true").unwrap();
        assert!(always.matches(&labels(&[])).unwrap());
        assert!(
            always
                .matches(&labels(&[("region", JsonValue::from("west"))]))
                .unwrap()
        );
        let never = LabelSelector::compile("false").unwrap();
        assert!(!never.matches(&labels(&[])).unwrap());
    }

    #[test]
    fn selector_over_missing_label_errors_on_empty_set() {
        // Referencing a label absent from an empty set is a hard error, not a
        // silent `false` — deterministic across repeated evaluation.
        let sel = LabelSelector::compile(r#"region == "west""#).unwrap();
        let empty = labels(&[]);
        assert!(matches!(
            sel.matches(&empty),
            Err(SelectorError::Resolve(_))
        ));
        assert!(matches!(
            sel.matches(&empty),
            Err(SelectorError::Resolve(_))
        ));
    }

    #[test]
    fn numeric_labels_compare_by_value() {
        let sel = LabelSelector::compile("tier == 3").unwrap();
        assert!(
            sel.matches(&labels(&[("tier", JsonValue::from(3))]))
                .unwrap()
        );
        assert!(
            !sel.matches(&labels(&[("tier", JsonValue::from(4))]))
                .unwrap()
        );
    }

    #[test]
    fn bool_labels_compare_by_value() {
        let sel = LabelSelector::compile("beta == true").unwrap();
        assert!(
            sel.matches(&labels(&[("beta", JsonValue::from(true))]))
                .unwrap()
        );
        assert!(
            !sel.matches(&labels(&[("beta", JsonValue::from(false))]))
                .unwrap()
        );
    }

    #[test]
    fn string_method_call_over_a_label() {
        // Method calls over labels are permitted; the label-only walk admits
        // them and the typechecker validates the method.
        let sel = LabelSelector::compile(r#"region.contains("es")"#).unwrap();
        assert!(
            sel.matches(&labels(&[("region", JsonValue::from("west"))]))
                .unwrap()
        );
        assert!(
            !sel.matches(&labels(&[("region", JsonValue::from("north"))]))
                .unwrap()
        );
    }

    #[test]
    fn non_boolean_selector_is_type_error() {
        // A bare field is a valid expression but not a boolean predicate.
        let sel = LabelSelector::compile("region").unwrap();
        let ls = labels(&[("region", JsonValue::from("west"))]);
        assert!(matches!(sel.matches(&ls), Err(SelectorError::Type(_))));
    }

    #[test]
    fn type_mismatch_between_label_and_literal_is_error() {
        // `tier` is an integer label; comparing it to a string literal is
        // ill-typed and must not silently evaluate to `false`.
        let sel = LabelSelector::compile(r#"tier == "enterprise""#).unwrap();
        let ls = labels(&[("tier", JsonValue::from(3))]);
        let result = sel.matches(&ls);
        assert!(
            matches!(result, Err(SelectorError::Type(_))),
            "expected a type error, got {result:?}"
        );
    }

    #[test]
    fn system_namespaces_are_rejected_at_compile() {
        for src in [
            r#"$source.file == "x""#,
            r#"$source.row == 1"#,
            r#"$record.tenant == "acme""#,
            r#"$pipeline.name == "p""#,
            r#"$vars.currency == "USD""#,
            r#"$doc.head.batch == "b""#,
        ] {
            let err = LabelSelector::compile(src).unwrap_err();
            assert!(
                matches!(err, SelectorError::ForbiddenReference(_)),
                "expected {src:?} to be rejected as a forbidden reference, got {err:?}"
            );
        }
    }

    #[test]
    fn qualified_reference_is_rejected_at_compile() {
        let err = LabelSelector::compile(r#"orders.region == "west""#).unwrap_err();
        assert!(matches!(err, SelectorError::ForbiddenReference(_)));
    }

    #[test]
    fn nested_forbidden_reference_is_rejected() {
        // The forbidden construct is buried inside a boolean tree; the
        // recursive walk must still find it.
        let err = LabelSelector::compile(r#"region == "west" and $source.row == 1"#).unwrap_err();
        assert!(matches!(err, SelectorError::ForbiddenReference(_)));
    }

    #[test]
    fn empty_match_string_is_parse_error() {
        assert!(matches!(
            LabelSelector::compile(""),
            Err(SelectorError::Parse(_))
        ));
    }

    #[test]
    fn non_scalar_label_is_out_of_scope() {
        // An array-valued label is not part of the selector schema, so a
        // selector referencing it fails resolution rather than matching it.
        let sel = LabelSelector::compile(r#"tags == "west""#).unwrap();
        let ls = labels(&[("tags", JsonValue::from(vec!["west", "east"]))]);
        assert!(matches!(sel.matches(&ls), Err(SelectorError::Resolve(_))));

        // A non-scalar label that the selector does NOT reference is harmless.
        let sel2 = LabelSelector::compile(r#"region == "west""#).unwrap();
        let ls2 = labels(&[
            ("region", JsonValue::from("west")),
            ("tags", JsonValue::from(vec!["a", "b"])),
        ]);
        assert!(sel2.matches(&ls2).unwrap());
    }
}
