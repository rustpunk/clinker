//! Differential and property tests proving the compiled scalar-core
//! evaluator produces byte-identical [`EvalResult`]s to the tree-walk.
//!
//! Every program here stays inside the scalar core (no method calls,
//! closures, window access, `distinct`, or `emit each`) so both
//! evaluators are defined on it. The fidelity assertion compares the
//! field map, the `$record.*` channel, the skip reason, and — on the
//! error path — the error kind, span, and boundary provenance fields,
//! byte for byte. On any divergence the compiled node is the thing to
//! fix; the assertion is never weakened.

use std::collections::HashMap;
use std::sync::Arc;

use clinker_record::{RecordStorage, Value};
use proptest::prelude::*;

use super::{CompiledProgram, compile};
use crate::eval::context::{EvalContext, StableEvalContext};
use crate::eval::error::EvalError;
use crate::eval::{EvalResult, ProgramEvaluator, SkipReason};
use crate::parser::Parser;
use crate::resolve::HashMapResolver;
use crate::resolve::pass::resolve_program;
use crate::typecheck::pass::{TypedProgram, type_check};
use crate::typecheck::row::Row;

/// Window storage stand-in for the no-window scalar-core programs under
/// test. Every method is unreachable here; the scalar core never reads
/// the window, so the differential harness always passes `None`.
struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u64, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _: u64) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u64 {
        0
    }
}

fn empty_row() -> Row {
    Row::closed(indexmap::IndexMap::new(), crate::lexer::Span::new(0, 0))
}

/// Parse → resolve → typecheck a CXL program against the given input
/// field names, attaching the source text so error spans render
/// identically through both evaluators. Returns `None` when the program
/// fails to parse, resolve, or typecheck — the proptest uses this to
/// discard generated programs that are not well-typed CXL (e.g. a field
/// used as both numeric and string), since neither evaluator is defined
/// on them.
fn try_type_program(src: &str, fields: &[&str]) -> Option<TypedProgram> {
    let parsed = Parser::parse(src);
    if !parsed.errors.is_empty() {
        return None;
    }
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count).ok()?;
    Some(
        type_check(resolved, &empty_row())
            .ok()?
            .with_source(Arc::from(src)),
    )
}

/// Like [`try_type_program`] but panics on any failure. Used by the
/// hand-written corpus, where every program is valid CXL by
/// construction.
fn type_program(src: &str, fields: &[&str]) -> TypedProgram {
    let parsed = Parser::parse(src);
    assert!(
        parsed.errors.is_empty(),
        "parse errors: {:?}",
        parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count).unwrap_or_else(|d| {
        panic!(
            "resolve errors: {:?}",
            d.iter().map(|e| &e.message).collect::<Vec<_>>()
        )
    });
    type_check(resolved, &empty_row())
        .unwrap_or_else(|d| {
            panic!(
                "type errors: {:?}",
                d.iter().map(|e| &e.message).collect::<Vec<_>>()
            )
        })
        .with_source(Arc::from(src))
}

/// Run the compiled program through the same outermost error boundary
/// the tree-walk's `ProgramEvaluator::eval_record` applies — stamping
/// `source_row` / `source_expr` onto a surfaced error — so the two
/// evaluators are compared on identical boundary provenance.
fn compiled_eval_record(
    program: &CompiledProgram<NullStorage>,
    typed: &TypedProgram,
    ctx: &EvalContext<'_>,
    resolver: &dyn crate::resolve::traits::FieldResolver,
) -> Result<EvalResult, EvalError> {
    let source_row = ctx.source_row;
    let source_expr = typed.source.clone();
    let window: Option<&dyn crate::resolve::traits::WindowContext<'_, NullStorage>> = None;
    program
        .eval_record(typed, ctx, resolver, window)
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

/// Ordered `(key, value)` pairs from an emit channel, captured for
/// equality comparison.
type Pairs = Vec<(String, Value)>;

/// One emitted record's two channels (fields, record-vars) in a
/// comparable form.
type EmitRecordProjection = (Pairs, Pairs);

/// Canonical, comparable projection of an [`EvalResult`] (or the error
/// it surfaced). Reduces both evaluators' outputs to plain owned data so
/// a single `assert_eq!` covers fields, record-vars, skip reason, and —
/// on the error path — the error kind string, span, and boundary
/// provenance fields.
#[derive(Debug, PartialEq)]
enum ResultProjection {
    Emit {
        fields: Pairs,
        record_vars: Pairs,
    },
    EmitMany {
        records: Vec<EmitRecordProjection>,
    },
    Skip(SkipReason),
    Err {
        kind: String,
        span: crate::lexer::Span,
        source_row: Option<u64>,
        source_expr: Option<String>,
        triggering_field: Option<String>,
    },
}

fn project(result: &Result<EvalResult, EvalError>) -> ResultProjection {
    fn pairs(m: &indexmap::IndexMap<String, Value>) -> Pairs {
        m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }
    match result {
        Ok(EvalResult::Emit {
            fields,
            record_vars,
        }) => ResultProjection::Emit {
            fields: pairs(fields),
            record_vars: pairs(record_vars),
        },
        Ok(EvalResult::EmitMany { records }) => ResultProjection::EmitMany {
            records: records
                .iter()
                .map(|r| (pairs(&r.fields), pairs(&r.record_vars)))
                .collect(),
        },
        Ok(EvalResult::Skip(reason)) => ResultProjection::Skip(*reason),
        Err(e) => ResultProjection::Err {
            // The kind enum is not PartialEq; its Debug form captures
            // every payload field deterministically, so comparing the
            // rendered string is an exact-equality check on the kind.
            kind: format!("{:?}", e.kind),
            span: e.span,
            source_row: e.source_row,
            source_expr: e.source_expr.as_ref().map(|s| s.to_string()),
            triggering_field: e.triggering_field.as_ref().map(|s| s.to_string()),
        },
    }
}

/// Assert the tree-walk and the compiled evaluator agree byte-for-byte
/// on `src` evaluated against `record`. Returns the shared projection so
/// callers can additionally assert the concrete outcome.
///
/// `TypedProgram` is not `Clone` and `ProgramEvaluator` needs an owned
/// `Arc<TypedProgram>`, so the program is type-checked twice from the
/// same source with the same input field set — the two are identical by
/// construction.
fn assert_agree(src: &str, fields: &[&str], record: HashMap<String, Value>) -> ResultProjection {
    let typed = type_program(src, fields);

    let stable = StableEvalContext::test_default();
    let resolver = HashMapResolver::new(record);

    // Tree-walk path through the public, boundary-wrapped entry.
    let ctx_tw = EvalContext::test_default_borrowed(&stable);
    let mut evaluator = ProgramEvaluator::new(Arc::new(type_program(src, fields)), false);
    let tree = evaluator.eval_record::<NullStorage>(&ctx_tw, &resolver, None);

    // Compiled path through the same boundary wrapper.
    let ctx_c = EvalContext::test_default_borrowed(&stable);
    let program = compile::<NullStorage>(&typed);
    let comp = compiled_eval_record(&program, &typed, &ctx_c, &resolver);

    let tree_proj = project(&tree);
    let comp_proj = project(&comp);
    assert_eq!(
        tree_proj, comp_proj,
        "tree-walk and compiled diverged on `{src}`",
    );
    tree_proj
}

// ── Differential corpus: the scalar core ───────────────────────────────

/// The full three-valued Kleene truth table for `and` / `or`. Each
/// operand is forced to true / false / null via a let-bound field so the
/// generated program references no method calls.
#[test]
fn kleene_truth_table() {
    let states = [
        ("true", Value::Bool(true)),
        ("false", Value::Bool(false)),
        ("nullv", Value::Null),
    ];
    for (lname, lval) in &states {
        for (rname, rval) in &states {
            for op in ["and", "or"] {
                let src = format!("emit out = a {op} b");
                let record = HashMap::from([
                    ("a".to_string(), lval.clone()),
                    ("b".to_string(), rval.clone()),
                ]);
                assert_agree(&src, &["a", "b"], record);
                let _ = (lname, rname);
            }
        }
    }
}

/// `and` / `or` must short-circuit identically: the right operand is a
/// division-by-zero that errors if evaluated. `false && X` and
/// `true || X` must NOT evaluate `X`, so both paths agree on a clean
/// emit; the other states must evaluate `X` and both must surface the
/// same error.
#[test]
fn kleene_short_circuit_side_effect() {
    // false && (1/0) → false, RHS unread, no error.
    assert_agree(
        "emit out = a and (1 / z)",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Bool(false)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
    // true || (1/0) → true, RHS unread, no error.
    assert_agree(
        "emit out = a or (1 / z)",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Bool(true)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
    // true && (1/0) → RHS evaluated → both must surface the same error.
    assert_agree(
        "emit out = a and (1 / z)",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Bool(true)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
}

/// Arithmetic and comparison null-propagation: a null operand yields
/// null, except equality which never nulls.
#[test]
fn null_propagation() {
    let with_null = |op: &str| {
        assert_agree(
            &format!("emit out = a {op} b"),
            &["a", "b"],
            HashMap::from([
                ("a".to_string(), Value::Null),
                ("b".to_string(), Value::Integer(3)),
            ]),
        )
    };
    for op in ["+", "-", "*", "/", "%", ">", "<", ">=", "<="] {
        let proj = with_null(op);
        assert_eq!(
            proj,
            ResultProjection::Emit {
                fields: vec![("out".to_string(), Value::Null)],
                record_vars: vec![],
            },
        );
    }
}

/// Eq / Neq never null: a null operand still produces a boolean.
#[test]
fn eq_neq_on_null() {
    let proj = assert_agree(
        "emit out = a == b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(3)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(false))],
            record_vars: vec![],
        },
    );
    // null == null → true
    assert_agree(
        "emit out = a == b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Null),
        ]),
    );
    // null != 3 → true
    let proj = assert_agree(
        "emit out = a != b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(3)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Bool(true))],
            record_vars: vec![],
        },
    );
}

/// Coalesce short-circuits on the first non-null operand and never
/// evaluates the RHS in that case.
#[test]
fn coalesce_short_circuit() {
    // a is non-null → RHS (1/0) unread, no error.
    assert_agree(
        "emit out = a ?? (1 / z)",
        &["a", "z"],
        HashMap::from([
            ("a".to_string(), Value::Integer(7)),
            ("z".to_string(), Value::Integer(0)),
        ]),
    );
    // a is null → fall through to b.
    let proj = assert_agree(
        "emit out = a ?? b",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(9)),
        ]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(9))],
            record_vars: vec![],
        },
    );
}

/// `if`/`then`/`else` — true branch, false branch, and missing else.
#[test]
fn if_then_else() {
    assert_agree(
        "emit out = if a > 0 then \"pos\" else \"nonpos\"",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
    assert_agree(
        "emit out = if a > 0 then \"pos\" else \"nonpos\"",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(-5))]),
    );
    // Null condition → not true → else branch.
    assert_agree(
        "emit out = if a > 0 then \"pos\" else \"nonpos\"",
        &["a"],
        HashMap::from([("a".to_string(), Value::Null)]),
    );
}

/// Value-form and condition-form `match`, including wildcard fall-through.
#[test]
fn match_forms() {
    // Value form, matching a non-wildcard arm.
    assert_agree(
        "emit out = match status { \"a\" => 1, \"b\" => 2, _ => 0 }",
        &["status"],
        HashMap::from([("status".to_string(), Value::String("b".into()))]),
    );
    // Value form, no non-wildcard arm matches → wildcard fall-through.
    // (The typechecker requires the `_` catch-all, so an unmatched
    // scrutinee always lands on it rather than the implicit null.)
    let proj = assert_agree(
        "emit out = match status { \"a\" => 1, \"b\" => 2, _ => 0 }",
        &["status"],
        HashMap::from([("status".to_string(), Value::String("z".into()))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(0))],
            record_vars: vec![],
        },
    );
    // Condition form.
    assert_agree(
        "emit out = match { a > 10 => \"big\", a > 0 => \"small\", _ => \"neg\" }",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
}

/// Unary negation and logical not, including null-propagation.
#[test]
fn unary_ops() {
    assert_agree(
        "emit out = -a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(4))]),
    );
    assert_agree(
        "emit out = not a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Bool(true))]),
    );
    assert_agree(
        "emit out = not a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Null)]),
    );
}

/// All four emit targets land in the right channel: Field → output map,
/// Record → record_vars, Pipeline / Source → context (neither surfaces
/// in the result), with both evaluators agreeing on the surfaced shape.
#[test]
fn emit_targets() {
    // Field.
    let proj = assert_agree("emit out = 1", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(1))],
            record_vars: vec![],
        },
    );
    // Record.
    let proj = assert_agree("emit $record.tag = 7", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![("tag".to_string(), Value::Integer(7))],
        },
    );
    // Pipeline / Source write through the context — neither appears in
    // the surfaced fields or record_vars. Both evaluators must agree on
    // the empty emit.
    let proj = assert_agree("emit $pipeline.seen = 1", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![],
        },
    );
    let proj = assert_agree("emit $source.seen = 1", &[], HashMap::new());
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![],
            record_vars: vec![],
        },
    );
}

/// `filter` short-circuits the record to `Skip(Filtered)` on a non-true
/// predicate (null included), and passes on `true`.
#[test]
fn filter_skip() {
    let proj = assert_agree(
        "filter a > 10\nemit out = a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
    assert_eq!(proj, ResultProjection::Skip(SkipReason::Filtered));

    // Null predicate is not true → skip.
    let proj = assert_agree(
        "filter a > b\nemit out = a",
        &["a", "b"],
        HashMap::from([
            ("a".to_string(), Value::Null),
            ("b".to_string(), Value::Integer(1)),
        ]),
    );
    assert_eq!(proj, ResultProjection::Skip(SkipReason::Filtered));

    // Passing predicate → emit.
    assert_agree(
        "filter a > 0\nemit out = a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(5))]),
    );
}

/// `let` writes the shared env and a later field-ref reads it; field
/// resolution prefers env over the input record.
#[test]
fn let_binding_and_env_shadow() {
    // Env value shadows the input field of the same name.
    let proj = assert_agree(
        "let a = 100\nemit out = a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(1))]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(100))],
            record_vars: vec![],
        },
    );
}

/// System-access leaves: `$source.*`, `$pipeline.*`, `now`. Both
/// evaluators resolve them against the same context and agree.
#[test]
fn system_access() {
    assert_agree("emit out = $source.row", &[], HashMap::new());
    assert_agree("emit out = $pipeline.name", &[], HashMap::new());
    assert_agree("emit out = now", &[], HashMap::new());
}

/// Index access on an array-valued field: in-bounds, out-of-bounds
/// (→ null), and negative (→ null). Both evaluators agree, including
/// the OOB null.
#[test]
fn index_access() {
    let nums = || {
        Value::Array(vec![
            Value::Integer(10),
            Value::Integer(20),
            Value::Integer(30),
        ])
    };
    let proj = assert_agree(
        "emit out = nums[1]",
        &["nums"],
        HashMap::from([("nums".to_string(), nums())]),
    );
    assert_eq!(
        proj,
        ResultProjection::Emit {
            fields: vec![("out".to_string(), Value::Integer(20))],
            record_vars: vec![],
        },
    );
    // Out of bounds → null.
    assert_agree(
        "emit out = nums[9]",
        &["nums"],
        HashMap::from([("nums".to_string(), nums())]),
    );
    // Negative index → null.
    assert_agree(
        "emit out = nums[0 - 1]",
        &["nums"],
        HashMap::from([("nums".to_string(), nums())]),
    );
}

/// Error path: division by zero must surface the same error kind, span,
/// boundary `source_row`, `source_expr`, and `triggering_field` from
/// both evaluators.
#[test]
fn error_span_fidelity_division_by_zero() {
    let proj = assert_agree(
        "emit out = 1 / z",
        &["z"],
        HashMap::from([("z".to_string(), Value::Integer(0))]),
    );
    match proj {
        ResultProjection::Err {
            kind,
            source_row,
            source_expr,
            triggering_field,
            ..
        } => {
            assert_eq!(kind, "DivisionByZero");
            assert_eq!(source_row, Some(1));
            assert!(source_expr.is_some());
            assert_eq!(triggering_field.as_deref(), Some("out"));
        }
        other => panic!("expected error projection, got {other:?}"),
    }
}

/// Error path: integer overflow on negation surfaces identically.
#[test]
fn error_span_fidelity_overflow() {
    let proj = assert_agree(
        "emit out = -a",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(i64::MIN))]),
    );
    match proj {
        ResultProjection::Err { kind, .. } => assert!(
            kind.contains("IntegerOverflow"),
            "expected IntegerOverflow, got {kind}"
        ),
        other => panic!("expected error projection, got {other:?}"),
    }
}

/// Multiple statements and emits accumulate in order; trace / use / expr
/// statements run without altering the field channel.
#[test]
fn multi_statement_ordering() {
    assert_agree(
        "let x = a + 1\nemit p = x\nemit q = x * 2\ntrace \"hi\"\nx + 1",
        &["a"],
        HashMap::from([("a".to_string(), Value::Integer(4))]),
    );
}

// ── Property test: random scalar-core expressions ──────────────────────

/// A generated scalar-core expression. Lowers to CXL source text and is
/// evaluated by both paths. The grammar covers literals (including
/// nulls), the three input field refs, nested binary / unary / coalesce
/// / if, with real depth — but no method calls, closures, windows, or
/// other deferred nodes.
#[derive(Debug, Clone)]
enum GenExpr {
    IntLit(i64),
    BoolLit(bool),
    NullLit,
    StrLit(String),
    Field(&'static str),
    Binary(&'static str, Box<GenExpr>, Box<GenExpr>),
    Not(Box<GenExpr>),
    Neg(Box<GenExpr>),
    Coalesce(Box<GenExpr>, Box<GenExpr>),
    If(Box<GenExpr>, Box<GenExpr>, Box<GenExpr>),
}

impl GenExpr {
    /// Render to CXL source text. Every binary/unary form is fully
    /// parenthesized so the generated precedence is unambiguous and the
    /// parser reconstructs exactly the generated tree.
    fn render(&self) -> String {
        match self {
            GenExpr::IntLit(n) => n.to_string(),
            GenExpr::BoolLit(b) => b.to_string(),
            GenExpr::NullLit => "null".to_string(),
            GenExpr::StrLit(s) => format!("\"{s}\""),
            GenExpr::Field(name) => name.to_string(),
            GenExpr::Binary(op, l, r) => format!("({} {} {})", l.render(), op, r.render()),
            GenExpr::Not(e) => format!("(not {})", e.render()),
            GenExpr::Neg(e) => format!("(-{})", e.render()),
            GenExpr::Coalesce(l, r) => format!("({} ?? {})", l.render(), r.render()),
            GenExpr::If(c, t, e) => {
                format!(
                    "(if {} then {} else {})",
                    c.render(),
                    t.render(),
                    e.render()
                )
            }
        }
    }
}

/// Leaf generator: literals (with nulls and strings) and the three input
/// field refs `a` / `b` / `c`.
fn leaf_strategy() -> impl Strategy<Value = GenExpr> {
    prop_oneof![
        (-1000i64..1000).prop_map(GenExpr::IntLit),
        any::<bool>().prop_map(GenExpr::BoolLit),
        Just(GenExpr::NullLit),
        "[a-z]{0,4}".prop_map(GenExpr::StrLit),
        Just(GenExpr::Field("a")),
        Just(GenExpr::Field("b")),
        Just(GenExpr::Field("c")),
    ]
}

/// Recursive expression generator with real nesting: up to 4 levels
/// deep, branching into binary / unary / coalesce / if forms over the
/// full operator set the scalar core supports.
fn expr_strategy() -> impl Strategy<Value = GenExpr> {
    leaf_strategy().prop_recursive(4, 48, 4, |inner| {
        prop_oneof![
            (
                prop_oneof![
                    Just("+"),
                    Just("-"),
                    Just("*"),
                    Just("/"),
                    Just("%"),
                    Just("=="),
                    Just("!="),
                    Just(">"),
                    Just("<"),
                    Just(">="),
                    Just("<="),
                    Just("and"),
                    Just("or"),
                ],
                inner.clone(),
                inner.clone(),
            )
                .prop_map(|(op, l, r)| GenExpr::Binary(op, Box::new(l), Box::new(r))),
            inner.clone().prop_map(|e| GenExpr::Not(Box::new(e))),
            inner.clone().prop_map(|e| GenExpr::Neg(Box::new(e))),
            (inner.clone(), inner.clone())
                .prop_map(|(l, r)| GenExpr::Coalesce(Box::new(l), Box::new(r))),
            (inner.clone(), inner.clone(), inner.clone()).prop_map(|(c, t, e)| GenExpr::If(
                Box::new(c),
                Box::new(t),
                Box::new(e)
            )),
        ]
    })
}

/// Random input values for the `a` / `b` / `c` fields, spanning every
/// scalar type plus null so generated expressions hit mixed-type and
/// null-propagation paths.
fn value_strategy() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        (-1000i64..1000).prop_map(Value::Integer),
        (-1000.0f64..1000.0).prop_map(Value::Float),
        "[a-z]{0,4}".prop_map(|s| Value::String(s.into())),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    /// For any generated scalar-core expression and any random a/b/c
    /// inputs, the tree-walk and the compiled evaluator must produce
    /// byte-identical results — value, skip, or error (kind + span +
    /// boundary provenance).
    #[test]
    fn compiled_matches_tree_walk(
        expr in expr_strategy(),
        a in value_strategy(),
        b in value_strategy(),
        c in value_strategy(),
    ) {
        let src = format!("emit out = {}", expr.render());
        // Discard programs that are not well-typed CXL (e.g. a field
        // used as both numeric and string): neither evaluator is defined
        // on them, so they are out of scope for the fidelity contract.
        let typed = match try_type_program(&src, &["a", "b", "c"]) {
            Some(t) => t,
            None => return Err(TestCaseError::reject("not well-typed")),
        };
        let typed_tw = try_type_program(&src, &["a", "b", "c"]).expect("second type-check agrees");
        let record = HashMap::from([
            ("a".to_string(), a),
            ("b".to_string(), b),
            ("c".to_string(), c),
        ]);

        let stable = StableEvalContext::test_default();
        let resolver = HashMapResolver::new(record);

        let ctx_tw = EvalContext::test_default_borrowed(&stable);
        let mut evaluator = ProgramEvaluator::new(Arc::new(typed_tw), false);
        let tree = evaluator.eval_record::<NullStorage>(&ctx_tw, &resolver, None);

        let ctx_c = EvalContext::test_default_borrowed(&stable);
        let program = compile::<NullStorage>(&typed);
        let comp = compiled_eval_record(&program, &typed, &ctx_c, &resolver);

        prop_assert_eq!(project(&tree), project(&comp), "diverged on `{}`", src);
    }
}
