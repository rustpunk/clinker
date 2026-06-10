use std::collections::HashMap;

use chrono::Datelike;
use clinker_record::{RecordStorage, Value};

use super::*;
use crate::lexer::Span;
use crate::parser::Parser;
use crate::resolve::HashMapResolver;
use crate::resolve::pass::resolve_program;
use crate::typecheck::pass::type_check;
use crate::typecheck::row::Row;

fn empty_row() -> Row {
    Row::closed(indexmap::IndexMap::new(), Span::new(0, 0))
}

/// Run a typed program through the canonical [`ProgramEvaluator`] and
/// return its field-target emit map. The test corpus uses only
/// `emit name = expr` statements, so the result is exactly the
/// `output` map [`ProgramEvaluator::eval_record`] builds for the
/// `EvalResult::Emit` variant — the single statement-level evaluator
/// is now the only expression-evaluation path the tests exercise.
fn run_fields<'w, S: RecordStorage + 'static>(
    typed: TypedProgram,
    ctx: &EvalContext<'_>,
    resolver: &dyn crate::resolve::FieldResolver,
    window: Option<&dyn clinker_record::WindowContext<'w, S>>,
) -> Result<indexmap::IndexMap<String, Value>, EvalError> {
    let has_distinct = typed
        .program
        .statements
        .iter()
        .any(|s| matches!(s, crate::ast::Statement::Distinct { .. }));
    let mut evaluator = ProgramEvaluator::new(std::sync::Arc::new(typed), has_distinct);
    match evaluator.eval_record(ctx, resolver, window)? {
        EvalResult::Emit { fields, .. } => Ok(fields),
        EvalResult::EmitMany { records } => Ok(records
            .into_iter()
            .next()
            .map(|r| r.fields)
            .unwrap_or_default()),
        EvalResult::Skip(_) => Ok(indexmap::IndexMap::new()),
    }
}

/// Dummy storage for no-window evaluation tests.
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

fn eval_ok(
    src: &str,
    fields: &[&str],
    record: HashMap<String, Value>,
) -> indexmap::IndexMap<String, Value> {
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
    let typed = type_check(resolved, &empty_row()).unwrap_or_else(|d| {
        panic!(
            "Type errors: {:?}",
            d.iter().map(|e| &e.message).collect::<Vec<_>>()
        )
    });
    let stable = StableEvalContext::test_default();
    let ctx = EvalContext::test_default_borrowed(&stable);
    let resolver = HashMapResolver::new(record);
    run_fields::<NullStorage>(typed, &ctx, &resolver, None)
        .unwrap_or_else(|e| panic!("Eval error: {}", e))
}

fn eval_single(src: &str, fields: &[&str], record: HashMap<String, Value>) -> Value {
    let output = eval_ok(src, fields, record);
    output.into_values().next().unwrap_or(Value::Null)
}

#[test]
fn test_eval_arithmetic_null_propagation() {
    // Null + 1 → Null
    let v = eval_single(
        "emit val = x + 1",
        &["x"],
        HashMap::from([("x".into(), Value::Null)]),
    );
    assert_eq!(v, Value::Null);
    // 2 + 3 → 5
    let v2 = eval_single("emit val = 2 + 3", &[], HashMap::new());
    assert_eq!(v2, Value::Integer(5));
}

#[test]
fn test_eval_comparison_null_semantics() {
    // Null == Null → true
    let v = eval_single(
        "emit val = x == null",
        &["x"],
        HashMap::from([("x".into(), Value::Null)]),
    );
    assert_eq!(v, Value::Bool(true));
    // Null != 1 → true
    let v2 = eval_single(
        "emit val = x != 1",
        &["x"],
        HashMap::from([("x".into(), Value::Null)]),
    );
    assert_eq!(v2, Value::Bool(true));
    // Null > 1 → Null
    let v3 = eval_single(
        "emit val = x > 1",
        &["x"],
        HashMap::from([("x".into(), Value::Null)]),
    );
    assert_eq!(v3, Value::Null);
}

#[test]
fn test_eval_coalesce_short_circuit() {
    // "hello" ?? <anything> → "hello" without evaluating RHS
    // We use a valid RHS to avoid errors, but the point is LHS wins
    let v = eval_single("emit val = \"hello\" ?? \"world\"", &[], HashMap::new());
    assert_eq!(v, Value::String("hello".into()));
}

#[test]
fn test_eval_let_binding_scope() {
    let output = eval_ok("let x = 10\nemit val = x + 1", &[], HashMap::new());
    assert_eq!(output.get("val"), Some(&Value::Integer(11)));
}

#[test]
fn test_eval_match_condition_form() {
    let v = eval_single(
        "emit val = match { age > 18 => \"adult\", _ => \"minor\" }",
        &["age"],
        HashMap::from([("age".into(), Value::Integer(25))]),
    );
    assert_eq!(v, Value::String("adult".into()));
}

#[test]
fn test_eval_match_value_form() {
    let v = eval_single(
        "emit val = match status { \"A\" => \"active\", \"I\" => \"inactive\", _ => \"unknown\" }",
        &["status"],
        HashMap::from([("status".into(), Value::String("A".into()))]),
    );
    assert_eq!(v, Value::String("active".into()));
}

#[test]
fn test_eval_if_then_else_missing_else() {
    let v = eval_single("emit val = if false then 1", &[], HashMap::new());
    assert_eq!(v, Value::Null);
}

#[test]
fn test_eval_string_methods_core() {
    assert_eq!(
        eval_single("emit val = \" hello \".trim()", &[], HashMap::new()),
        Value::String("hello".into())
    );
    assert_eq!(
        eval_single("emit val = \"hi\".upper()", &[], HashMap::new()),
        Value::String("HI".into())
    );
    assert_eq!(
        eval_single("emit val = \"HI\".lower()", &[], HashMap::new()),
        Value::String("hi".into())
    );
    assert_eq!(
        eval_single("emit val = \"hello\".length()", &[], HashMap::new()),
        Value::Integer(5)
    );
    assert_eq!(
        eval_single(
            "emit val = \"hello world\".replace(\"world\", \"rust\")",
            &[],
            HashMap::new()
        ),
        Value::String("hello rust".into())
    );
    assert_eq!(
        eval_single("emit val = \"hello\".substring(1, 3)", &[], HashMap::new()),
        Value::String("ell".into())
    );
}

#[test]
fn test_eval_string_methods_regex() {
    // .matches uses pre-compiled regex — full-match semantics
    assert_eq!(
        eval_single(
            "emit val = \"123\".matches(\"\\\\d+\")",
            &[],
            HashMap::new()
        ),
        Value::Bool(true)
    );
    // .capture extracts a match
    assert_eq!(
        eval_single(
            "emit val = \"abc123def\".capture(\"(\\\\d+)\")",
            &[],
            HashMap::new()
        ),
        Value::String("123".into())
    );
}

#[test]
fn test_eval_string_methods_path() {
    assert_eq!(
        eval_single(
            "emit val = \"data/orders.csv\".file_name()",
            &[],
            HashMap::new()
        ),
        Value::String("orders.csv".into())
    );
    assert_eq!(
        eval_single(
            "emit val = \"data/orders.csv\".file_stem()",
            &[],
            HashMap::new()
        ),
        Value::String("orders".into())
    );
    assert_eq!(
        eval_single(
            "emit val = \"data/orders.csv\".extension()",
            &[],
            HashMap::new()
        ),
        Value::String("csv".into())
    );
    assert_eq!(
        eval_single(
            "emit val = \"data/batch/orders.csv\".parent()",
            &[],
            HashMap::new()
        ),
        Value::String("data/batch".into())
    );
    assert_eq!(
        eval_single(
            "emit val = \"data/batch/orders.csv\".parent_name()",
            &[],
            HashMap::new()
        ),
        Value::String("batch".into())
    );
}

#[test]
fn test_eval_numeric_methods() {
    assert_eq!(
        eval_single("emit val = (-5).abs()", &[], HashMap::new()),
        Value::Integer(5)
    );
    assert_eq!(
        eval_single("emit val = 3.7.ceil()", &[], HashMap::new()),
        Value::Integer(4)
    );
    assert_eq!(
        eval_single("emit val = 3.2.floor()", &[], HashMap::new()),
        Value::Integer(3)
    );
    assert_eq!(
        eval_single("emit val = 3.456.round()", &[], HashMap::new()),
        Value::Float(3.0)
    );
    assert_eq!(
        eval_single("emit val = 3.456.round_to(2)", &[], HashMap::new()),
        Value::Float(3.46)
    );
    // clamp
    assert_eq!(
        eval_single(
            "emit val = x.clamp(0, 100)",
            &["x"],
            HashMap::from([("x".into(), Value::Integer(150))])
        ),
        Value::Integer(100)
    );
}

#[test]
fn test_eval_date_methods() {
    let record = HashMap::from([(
        "d".into(),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2026, 3, 15).unwrap()),
    )]);
    assert_eq!(
        eval_single("emit val = d.year()", &["d"], record.clone()),
        Value::Integer(2026)
    );
    assert_eq!(
        eval_single("emit val = d.month()", &["d"], record.clone()),
        Value::Integer(3)
    );
    assert_eq!(
        eval_single("emit val = d.day()", &["d"], record.clone()),
        Value::Integer(15)
    );
    // add_days
    let v = eval_single("emit val = d.add_days(7)", &["d"], record.clone());
    assert_eq!(
        v,
        Value::Date(chrono::NaiveDate::from_ymd_opt(2026, 3, 22).unwrap())
    );
    // format_date
    let v2 = eval_single("emit val = d.format_date(\"%Y-%m-%d\")", &["d"], record);
    assert_eq!(v2, Value::String("2026-03-15".into()));
}

#[test]
fn test_eval_conversion_strict() {
    assert_eq!(
        eval_single("emit val = \"42\".to_int()", &[], HashMap::new()),
        Value::Integer(42)
    );
    // to_int on "abc" → error
    let parsed = Parser::parse("emit val = \"abc\".to_int()");
    let resolved = resolve_program(parsed.ast, &[], parsed.node_count).unwrap();
    let typed = type_check(resolved, &empty_row()).unwrap();
    let stable = StableEvalContext::test_default();
    let ctx = EvalContext::test_default_borrowed(&stable);
    let resolver = HashMapResolver::new(HashMap::new());
    let result = run_fields::<NullStorage>(typed, &ctx, &resolver, None);
    assert!(
        result.is_err(),
        "Expected conversion error for \"abc\".to_int()"
    );
}

#[test]
fn test_eval_conversion_lenient() {
    assert_eq!(
        eval_single("emit val = \"abc\".try_int()", &[], HashMap::new()),
        Value::Null
    );
    assert_eq!(
        eval_single("emit val = \"2.5\".try_float()", &[], HashMap::new()),
        Value::Float(2.5)
    );
}

#[test]
fn test_eval_introspection() {
    assert_eq!(
        eval_single("emit val = \"hello\".type_of()", &[], HashMap::new()),
        Value::String("string".into())
    );
    assert_eq!(
        eval_single("emit val = 42.type_of()", &[], HashMap::new()),
        Value::String("int".into())
    );
    assert_eq!(
        eval_single("emit val = null.is_null()", &[], HashMap::new()),
        Value::Bool(true)
    );
    assert_eq!(
        eval_single("emit val = 42.is_null()", &[], HashMap::new()),
        Value::Bool(false)
    );
}

#[test]
fn test_eval_regex_precompiled() {
    // Regex should come from TypedProgram.regexes, pre-compiled during type check
    let parsed = Parser::parse("emit val = \"123\".matches(\"\\\\d+\")");
    let resolved = resolve_program(parsed.ast, &[], parsed.node_count).unwrap();
    let typed = type_check(resolved, &empty_row()).unwrap();
    // Verify regex was pre-compiled
    let has_regex = typed.regexes.iter().any(|r| r.is_some());
    assert!(has_regex, "Expected pre-compiled regex in TypedProgram");
    // And evaluation uses it
    let stable = StableEvalContext::test_default();
    let ctx = EvalContext::test_default_borrowed(&stable);
    let resolver = HashMapResolver::new(HashMap::new());
    let output = run_fields::<NullStorage>(typed, &ctx, &resolver, None).unwrap();
    assert_eq!(output.get("val"), Some(&Value::Bool(true)));
}

#[test]
fn test_eval_emit_output_map() {
    let output = eval_ok("emit a = 1\nemit b = 2\nemit c = 3", &[], HashMap::new());
    assert_eq!(output.len(), 3);
    assert_eq!(output.get("a"), Some(&Value::Integer(1)));
    assert_eq!(output.get("b"), Some(&Value::Integer(2)));
    assert_eq!(output.get("c"), Some(&Value::Integer(3)));
}

#[test]
fn test_eval_now_with_fixed_clock() {
    let v = eval_single("emit ts = now", &[], HashMap::new());
    // FixedClock in test_default returns 2026-01-15T12:00:00
    assert!(matches!(v, Value::DateTime(_)));
    if let Value::DateTime(dt) = v {
        assert_eq!(dt.year(), 2026);
    }
}

#[test]
fn test_eval_three_valued_and() {
    // false && null → false (not null)
    let v = eval_single(
        "emit val = false and x",
        &["x"],
        HashMap::from([("x".into(), Value::Null)]),
    );
    assert_eq!(v, Value::Bool(false));
}

#[test]
fn test_eval_three_valued_or() {
    // true || null → true (not null)
    let v = eval_single(
        "emit val = true or x",
        &["x"],
        HashMap::from([("x".into(), Value::Null)]),
    );
    assert_eq!(v, Value::Bool(true));
}

// ── Level 3: .debug() passthrough tests ───────────────────────

#[test]
fn test_log_level3_passthrough_value() {
    // .debug("check") returns the value unchanged
    let v = eval_single(
        "emit val = Name.debug(\"check\")",
        &["Name"],
        HashMap::from([("Name".into(), Value::String("Alice".into()))]),
    );
    assert_eq!(v, Value::String("Alice".into()));
}

#[test]
fn test_log_level3_emits_trace() {
    // .debug() should not error — trace event emitted but we can't capture it in unit tests
    let v = eval_single(
        "emit val = Name.debug(\"dbg\")",
        &["Name"],
        HashMap::from([("Name".into(), Value::String("Bob".into()))]),
    );
    assert_eq!(v, Value::String("Bob".into()));
}

#[test]
fn test_log_level3_debug_no_prefix() {
    // .debug() with no args
    let v = eval_single(
        "emit val = Name.debug()",
        &["Name"],
        HashMap::from([("Name".into(), Value::String("Carol".into()))]),
    );
    assert_eq!(v, Value::String("Carol".into()));
}

#[test]
fn test_log_level3_debug_null() {
    // null.debug("check") → null (null propagation)
    let v = eval_single(
        "emit val = Name.debug(\"check\")",
        &["Name"],
        HashMap::from([("Name".into(), Value::Null)]),
    );
    assert_eq!(v, Value::Null);
}

#[test]
fn test_log_level3_debug_chained() {
    // .debug("a").debug("b") → 2 events, value unchanged
    let v = eval_single(
        "emit val = Name.debug(\"first\").debug(\"second\")",
        &["Name"],
        HashMap::from([("Name".into(), Value::Integer(42))]),
    );
    assert_eq!(v, Value::Integer(42));
}

// ── Level 4: trace statement tests ────────────────────────────

#[test]
fn test_log_level4_trace_basic() {
    // trace "msg" — should not error
    let output = eval_ok("trace \"saw record\"\nemit val = 1", &[], HashMap::new());
    assert_eq!(output.get("val"), Some(&Value::Integer(1)));
}

#[test]
fn test_log_level4_trace_when_guard() {
    // trace if false "msg" — guard prevents trace
    let output = eval_ok(
        "trace if false \"should not fire\"\nemit val = 1",
        &[],
        HashMap::new(),
    );
    assert_eq!(output.get("val"), Some(&Value::Integer(1)));
}

#[test]
fn test_log_level4_trace_level_override() {
    // trace warn "alert" — uses warn level
    let output = eval_ok(
        "trace warn \"alert message\"\nemit val = 1",
        &[],
        HashMap::new(),
    );
    assert_eq!(output.get("val"), Some(&Value::Integer(1)));
}

#[test]
fn test_log_level4_multi_field_interpolation() {
    // trace with string containing field references — fields resolve to values
    let record = HashMap::from([
        ("a".into(), Value::Integer(1)),
        ("b".into(), Value::Integer(2)),
        ("c".into(), Value::Integer(3)),
    ]);
    let output = eval_ok(
        "let msg = a + b + c\ntrace msg\nemit total = msg",
        &["a", "b", "c"],
        record,
    );
    assert_eq!(output.get("total"), Some(&Value::Integer(6)));
}

#[test]
fn test_log_level4_guard_short_circuits() {
    // trace if Amount > 1000 "high" — guard is false, message expr not evaluated for side effects
    let output = eval_ok(
        "trace if false \"never\"\nemit val = 42",
        &[],
        HashMap::new(),
    );
    assert_eq!(output.get("val"), Some(&Value::Integer(42)));
}

// ---------------------------------------------------------------------------
// CompiledScalar: the single-expression path used by aggregation + Combine
//
// The aggregation and Combine engines evaluate one scalar `Expr` per record
// outside any statement program or analytic window via
// `compile_scalar(...).eval(...)`. This corpus pins that path's result for
// the windowless scalar surface those engines reach — literals, field refs,
// three-valued Kleene `and`/`or`, comparison / arithmetic with null
// propagation and overflow, coalesce, `if` / `match`, method calls
// (including a pre-compiled regex), and the closure-bearing array builtins.
// The retract / binding path needs these exact: a cached `retract_values`
// row must replay byte-for-byte, so every case asserts a hard expected
// value or error rather than a snapshot.
// ---------------------------------------------------------------------------

/// Expected outcome of evaluating one corpus case.
enum Expected {
    /// Exact value the compiled scalar must produce.
    Yields(Value),
    /// Substring the surfaced error's `Display` must contain.
    FailsWith(&'static str),
}

/// Parse → resolve → typecheck `emit val = <expr>` and hand back the typed
/// program plus the lone emit's expression, so a single scalar `Expr` can
/// be lowered through `compile_scalar` with the node-id-keyed side tables
/// (regex cache) the compiled path bakes in.
fn typed_emit_expr(src: &str, fields: &[&str]) -> (TypedProgram, crate::ast::Expr) {
    let full = format!("emit val = {src}");
    let parsed = Parser::parse(&full);
    assert!(
        parsed.errors.is_empty(),
        "parse errors for {full:?}: {:?}",
        parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
    );
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count)
        .unwrap_or_else(|d| panic!("resolve errors for {full:?}: {d:?}"));
    let typed = type_check(resolved, &empty_row())
        .unwrap_or_else(|d| panic!("type errors for {full:?}: {d:?}"))
        .with_source(std::sync::Arc::from(full.as_str()));
    let expr = match typed.program.statements.first() {
        Some(crate::ast::Statement::Emit { expr, .. }) => expr.clone(),
        other => panic!("expected a single emit statement, got {other:?}"),
    };
    (typed, expr)
}

/// One corpus case: expression source, declared input field names, the
/// record's `(field, value)` pairs, and the expected outcome. The record
/// is an owned `Vec` because `Value` payloads (`String`, `Array`) are not
/// const-promotable, so they cannot live in a `'static` slice literal.
type ScalarCase = (
    &'static str,
    &'static [&'static str],
    Vec<(&'static str, Value)>,
    Expected,
);

#[test]
fn compiled_scalar_evaluates_windowless_surface() {
    // Each case exercises a distinct lowering arm the aggregation / Combine
    // scalar expressions can reach.
    let cases: Vec<ScalarCase> = vec![
        // Literals.
        ("42", &[], vec![], Expected::Yields(Value::Integer(42))),
        ("3.5", &[], vec![], Expected::Yields(Value::Float(3.5))),
        (
            "\"hi\"",
            &[],
            vec![],
            Expected::Yields(Value::String("hi".into())),
        ),
        ("true", &[], vec![], Expected::Yields(Value::Bool(true))),
        ("null", &[], vec![], Expected::Yields(Value::Null)),
        // Field refs (present + absent → null). `missing` is declared so
        // resolution succeeds, but the record omits it so the eval takes
        // the absent-field → null path.
        (
            "amount",
            &["amount"],
            vec![("amount", Value::Integer(7))],
            Expected::Yields(Value::Integer(7)),
        ),
        (
            "missing",
            &["amount", "missing"],
            vec![("amount", Value::Integer(7))],
            Expected::Yields(Value::Null),
        ),
        // Arithmetic with null propagation + string concat.
        (
            "amount + 1",
            &["amount"],
            vec![("amount", Value::Integer(41))],
            Expected::Yields(Value::Integer(42)),
        ),
        (
            "amount + 1",
            &["amount"],
            vec![("amount", Value::Null)],
            Expected::Yields(Value::Null),
        ),
        (
            "a + b",
            &["a", "b"],
            vec![("a", Value::Float(1.5)), ("b", Value::Integer(2))],
            Expected::Yields(Value::Float(3.5)),
        ),
        (
            "first + last",
            &["first", "last"],
            vec![
                ("first", Value::String("ab".into())),
                ("last", Value::String("cd".into())),
            ],
            Expected::Yields(Value::String("abcd".into())),
        ),
        // Comparison + Kleene and/or with a null operand.
        (
            "amount > 10",
            &["amount"],
            vec![("amount", Value::Integer(5))],
            Expected::Yields(Value::Bool(false)),
        ),
        // null and true → null (Kleene: null && true is unknown).
        (
            "flag and amount > 0",
            &["flag", "amount"],
            vec![("flag", Value::Null), ("amount", Value::Integer(3))],
            Expected::Yields(Value::Null),
        ),
        // false or true → true.
        (
            "flag or amount > 0",
            &["flag", "amount"],
            vec![("flag", Value::Bool(false)), ("amount", Value::Integer(3))],
            Expected::Yields(Value::Bool(true)),
        ),
        // null == null → true (equality never nulls).
        (
            "amount == null",
            &["amount"],
            vec![("amount", Value::Null)],
            Expected::Yields(Value::Bool(true)),
        ),
        // Coalesce short-circuit (LHS resolves but is absent → null → RHS).
        (
            "missing ?? amount",
            &["amount", "missing"],
            vec![("amount", Value::Integer(9))],
            Expected::Yields(Value::Integer(9)),
        ),
        // Conditional + match.
        (
            "if amount > 0 then \"pos\" else \"nonpos\"",
            &["amount"],
            vec![("amount", Value::Integer(-1))],
            Expected::Yields(Value::String("nonpos".into())),
        ),
        (
            "match status { \"A\" => \"active\", _ => \"other\" }",
            &["status"],
            vec![("status", Value::String("A".into()))],
            Expected::Yields(Value::String("active".into())),
        ),
        // Unary negation.
        (
            "-amount",
            &["amount"],
            vec![("amount", Value::Integer(4))],
            Expected::Yields(Value::Integer(-4)),
        ),
        // Method call (string).
        (
            "name.upper()",
            &["name"],
            vec![("name", Value::String("abc".into()))],
            Expected::Yields(Value::String("ABC".into())),
        ),
        // Method call backed by a pre-compiled regex (the side-table the
        // compiled path captures by value at lowering).
        (
            "code.matches(\"\\\\d+\")",
            &["code"],
            vec![("code", Value::String("123".into()))],
            Expected::Yields(Value::Bool(true)),
        ),
        // Closure-bearing array builtin (env-threaded closure param).
        (
            "items.filter(it => it > 1)",
            &["items"],
            vec![(
                "items",
                Value::Array(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(3),
                ]),
            )],
            Expected::Yields(Value::Array(vec![Value::Integer(2), Value::Integer(3)])),
        ),
        // Division by zero surfaces a runtime error.
        (
            "amount / 0",
            &["amount"],
            vec![("amount", Value::Integer(8))],
            Expected::FailsWith("zero"),
        ),
    ];

    for (src, fields, record, expected) in cases {
        let (typed, expr) = typed_emit_expr(src, fields);
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        let resolver = HashMapResolver::new(
            record
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        );

        let got = compile_scalar(&typed, &expr).eval(&ctx, &resolver);

        match (expected, got) {
            (Expected::Yields(want), Ok(actual)) => {
                assert_eq!(actual, want, "value mismatch for {src:?}")
            }
            (Expected::FailsWith(needle), Err(e)) => assert!(
                e.to_string().contains(needle),
                "error for {src:?} did not contain {needle:?}: {e}"
            ),
            (Expected::Yields(_), Err(e)) => panic!("expected a value for {src:?}, got error: {e}"),
            (Expected::FailsWith(_), Ok(actual)) => {
                panic!("expected an error for {src:?}, got value: {actual:?}")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// $window.any / $window.every — three-valued logic
//
// Mirrors BinOp::And/Or in eval/mod.rs by construction. Tests use a
// minimal in-memory `RowsStorage` + `RowsWindow` so each test row can hold
// explicit `Value::Bool` / `Value::Null` predicate inputs without going
// through CSV/coercion.
// ---------------------------------------------------------------------------

mod any_every {
    use super::*;
    use crate::typecheck::row::QualifiedField;
    use clinker_record::{RecordView, WindowContext};

    /// Storage that returns the i-th row's `flag` field by index.
    struct RowsStorage {
        rows: Vec<Value>,
    }

    impl RecordStorage for RowsStorage {
        fn resolve_field(&self, index: u64, name: &str) -> Option<&Value> {
            if name == "flag" {
                self.rows.get(index as usize)
            } else {
                None
            }
        }
        fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
            None
        }
        fn available_fields(&self, _: u64) -> Vec<&str> {
            vec!["flag"]
        }
        fn record_count(&self) -> u64 {
            self.rows.len() as u64
        }
    }

    /// Window over the entire RowsStorage — every row is in the partition.
    struct RowsWindow<'a> {
        storage: &'a RowsStorage,
    }

    impl<'a> WindowContext<'a, RowsStorage> for RowsWindow<'a> {
        fn first(&self) -> Option<RecordView<'a, RowsStorage>> {
            (!self.storage.rows.is_empty()).then(|| RecordView::new(self.storage, 0))
        }
        fn last(&self) -> Option<RecordView<'a, RowsStorage>> {
            self.storage
                .rows
                .len()
                .checked_sub(1)
                .map(|i| RecordView::new(self.storage, i as u64))
        }
        fn lag(&self, _: usize) -> Option<RecordView<'a, RowsStorage>> {
            None
        }
        fn lead(&self, _: usize) -> Option<RecordView<'a, RowsStorage>> {
            None
        }
        fn count(&self) -> i64 {
            self.storage.rows.len() as i64
        }
        fn sum(&self, _: &str) -> Value {
            Value::Null
        }
        fn cumulative_sum(&self, _: &str) -> Value {
            Value::Null
        }
        fn avg(&self, _: &str) -> Value {
            Value::Null
        }
        fn min(&self, _: &str) -> Value {
            Value::Null
        }
        fn max(&self, _: &str) -> Value {
            Value::Null
        }
        fn partition_len(&self) -> usize {
            self.storage.rows.len()
        }
        fn partition_record(&self, index: usize) -> RecordView<'a, RowsStorage> {
            RecordView::new(self.storage, index as u64)
        }
        fn collect(&self, _: &str) -> Value {
            Value::Null
        }
        fn distinct(&self, _: &str) -> Value {
            Value::Null
        }
        fn row_number(&self) -> i64 {
            1
        }
        fn rank(&self) -> i64 {
            1
        }
        fn dense_rank(&self) -> i64 {
            1
        }
        fn first_value(&self, _: &str) -> Value {
            Value::Null
        }
        fn last_value(&self, _: &str) -> Value {
            Value::Null
        }
    }

    fn eval_window(src: &str, partition: Vec<Value>) -> Value {
        let parsed = Parser::parse(src);
        assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
        let resolved = resolve_program(parsed.ast, &["flag"], parsed.node_count)
            .unwrap_or_else(|d| panic!("resolve: {:?}", d));
        let row = Row::closed(
            indexmap::IndexMap::from([(
                QualifiedField::bare("flag"),
                crate::typecheck::types::Type::Bool,
            )]),
            Span::new(0, 0),
        );
        let typed = type_check(resolved, &row).unwrap_or_else(|d| panic!("type: {:?}", d));
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        let storage = RowsStorage { rows: partition };
        let resolver = HashMapResolver::new(HashMap::new());
        let window = RowsWindow { storage: &storage };
        run_fields::<RowsStorage>(typed, &ctx, &resolver, Some(&window))
            .unwrap_or_else(|e| panic!("eval: {}", e))
            .into_values()
            .next()
            .unwrap_or(Value::Null)
    }

    fn b(v: bool) -> Value {
        Value::Bool(v)
    }
    fn n() -> Value {
        Value::Null
    }

    #[test]
    fn any_short_circuits_on_true() {
        assert_eq!(
            eval_window(
                "emit r = $window.any(flag)",
                vec![b(true), b(false), b(false)]
            ),
            Value::Bool(true)
        );
        // True before null → still true (short-circuit fires before null is seen).
        assert_eq!(
            eval_window("emit r = $window.any(flag)", vec![b(true), n(), b(false)]),
            Value::Bool(true)
        );
    }

    #[test]
    fn any_every_false_returns_false() {
        assert_eq!(
            eval_window(
                "emit r = $window.any(flag)",
                vec![b(false), b(false), b(false)]
            ),
            Value::Bool(false)
        );
    }

    #[test]
    fn any_null_with_no_true_returns_null() {
        assert_eq!(
            eval_window("emit r = $window.any(flag)", vec![n(), b(false), b(false)]),
            Value::Null
        );
        assert_eq!(
            eval_window("emit r = $window.any(flag)", vec![b(false), n(), b(false)]),
            Value::Null
        );
    }

    #[test]
    fn every_short_circuits_on_false() {
        assert_eq!(
            eval_window(
                "emit r = $window.every(flag)",
                vec![b(true), b(false), b(true)]
            ),
            Value::Bool(false)
        );
        // False before null → still false (short-circuit fires before null is seen).
        assert_eq!(
            eval_window("emit r = $window.every(flag)", vec![b(false), n(), b(true)]),
            Value::Bool(false)
        );
    }

    #[test]
    fn every_all_true_returns_true() {
        assert_eq!(
            eval_window(
                "emit r = $window.every(flag)",
                vec![b(true), b(true), b(true)]
            ),
            Value::Bool(true)
        );
    }

    #[test]
    fn every_null_with_no_false_returns_null() {
        assert_eq!(
            eval_window("emit r = $window.every(flag)", vec![n(), b(true), b(true)]),
            Value::Null
        );
        assert_eq!(
            eval_window("emit r = $window.every(flag)", vec![b(true), n(), b(true)]),
            Value::Null
        );
    }

    #[test]
    fn empty_partition_returns_identity() {
        // Defensive — unreachable in practice (current row is always in
        // partition), but identity for iterated or/and:
        // any → false, every → true.
        assert_eq!(
            eval_window("emit r = $window.any(flag)", vec![]),
            Value::Bool(false)
        );
        assert_eq!(
            eval_window("emit r = $window.every(flag)", vec![]),
            Value::Bool(true)
        );
    }

    #[test]
    fn agrees_with_iterated_or_and_two_rows() {
        // Algebraic identity: any([a, b]) ≡ a or b; every([a, b]) ≡ a and b.
        // BinOp::And/Or in eval/mod.rs are the source of truth;
        // any/every over the same inputs must produce the same Value.
        let pairs = [
            (b(true), b(true)),
            (b(true), b(false)),
            (b(false), b(true)),
            (b(false), b(false)),
            (b(true), n()),
            (n(), b(true)),
            (b(false), n()),
            (n(), b(false)),
            (n(), n()),
        ];
        for (a, b_val) in pairs {
            let from_any =
                eval_window("emit r = $window.any(flag)", vec![a.clone(), b_val.clone()]);
            let expected_any = match (&a, &b_val) {
                (Value::Bool(true), _) | (_, Value::Bool(true)) => Value::Bool(true),
                (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
                _ => Value::Null,
            };
            assert_eq!(
                from_any, expected_any,
                "any({a:?}, {b_val:?}) must match iterated or"
            );

            let from_every = eval_window(
                "emit r = $window.every(flag)",
                vec![a.clone(), b_val.clone()],
            );
            let expected_every = match (&a, &b_val) {
                (Value::Bool(false), _) | (_, Value::Bool(false)) => Value::Bool(false),
                (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
                _ => Value::Null,
            };
            assert_eq!(
                from_every, expected_every,
                "every({a:?}, {b_val:?}) must match iterated and"
            );
        }
    }

    #[test]
    fn exists_aliases_any() {
        // exists(p) and any(p) collapse to iterated OR over identical
        // inputs and must match Value-for-Value.
        assert_eq!(
            eval_window(
                "emit r = $window.exists(flag)",
                vec![b(false), b(true), b(false)]
            ),
            Value::Bool(true)
        );
        assert_eq!(
            eval_window(
                "emit r = $window.exists(flag)",
                vec![b(false), b(false), b(false)]
            ),
            Value::Bool(false)
        );
        // Empty partition: same identity as any.
        assert_eq!(
            eval_window("emit r = $window.exists(flag)", vec![]),
            Value::Bool(false)
        );
    }

    #[test]
    fn not_exists_inverts_predicate() {
        // not_exists(p) ≡ every(not p): true iff no row satisfies p.
        assert_eq!(
            eval_window(
                "emit r = $window.not_exists(flag)",
                vec![b(false), b(false), b(false)]
            ),
            Value::Bool(true)
        );
        assert_eq!(
            eval_window(
                "emit r = $window.not_exists(flag)",
                vec![b(false), b(true), b(false)]
            ),
            Value::Bool(false)
        );
        // Empty partition: vacuously true (matches every's identity).
        assert_eq!(
            eval_window("emit r = $window.not_exists(flag)", vec![]),
            Value::Bool(true)
        );
    }
}

// ---------------------------------------------------------------------------
// Typecheck: unknown $window.* functions are an error.
// Catches the silent-Null bug where $window.row_numbr() previously
// fell through the registry's None branch and inherited Type::Any.
// ---------------------------------------------------------------------------

mod window_typecheck {
    use super::*;

    fn typecheck_errors(src: &str) -> Vec<String> {
        let parsed = Parser::parse(src);
        assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
        let resolved = resolve_program(parsed.ast, &[], parsed.node_count)
            .unwrap_or_else(|d| panic!("resolve: {:?}", d));
        let row = empty_row();
        match type_check(resolved, &row) {
            Ok(_) => vec![],
            Err(diags) => diags.into_iter().map(|d| d.message).collect(),
        }
    }

    #[test]
    fn unknown_window_function_errors() {
        let errs = typecheck_errors("emit r = $window.row_numbr()");
        assert!(
            errs.iter().any(|m| m.contains("unknown window function")),
            "expected unknown-window-function diagnostic, got: {:?}",
            errs
        );
    }

    #[test]
    fn registered_window_function_typechecks() {
        // row_number is registered with return type Int; should not error.
        let errs = typecheck_errors("emit r = $window.row_number()");
        assert!(errs.is_empty(), "unexpected errors: {:?}", errs);
    }
}

// ---------------------------------------------------------------------------
// $window.row_number / rank / dense_rank / first_value / last_value.
//
// Unit-level shape tests against a minimal in-memory implementation that
// keys ranks off a synthetic `k` column. End-to-end coverage with an
// Arena partition lives in clinker-exec's
// `tests/post_aggregate_window_ranking.rs` integration test.
// ---------------------------------------------------------------------------

mod ranking_and_value {
    use super::*;
    use crate::typecheck::row::QualifiedField;
    use clinker_record::{RecordView, WindowContext};

    /// Storage with two columns: `k` (sort key) and `v` (value).
    struct KeyedRows {
        keys: Vec<Value>,
        values: Vec<Value>,
    }

    impl RecordStorage for KeyedRows {
        fn resolve_field(&self, index: u64, name: &str) -> Option<&Value> {
            match name {
                "k" => self.keys.get(index as usize),
                "v" => self.values.get(index as usize),
                _ => None,
            }
        }
        fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
            None
        }
        fn available_fields(&self, _: u64) -> Vec<&str> {
            vec!["k", "v"]
        }
        fn record_count(&self) -> u64 {
            self.keys.len() as u64
        }
    }

    /// Window over the full storage. `current_pos` selects which row the
    /// ranking functions report; rank/dense_rank consult the `k` column
    /// for tie detection.
    struct RankWindow<'a> {
        storage: &'a KeyedRows,
        current_pos: usize,
    }

    impl<'a> WindowContext<'a, KeyedRows> for RankWindow<'a> {
        fn first(&self) -> Option<RecordView<'a, KeyedRows>> {
            (!self.storage.keys.is_empty()).then(|| RecordView::new(self.storage, 0))
        }
        fn last(&self) -> Option<RecordView<'a, KeyedRows>> {
            self.storage
                .keys
                .len()
                .checked_sub(1)
                .map(|i| RecordView::new(self.storage, i as u64))
        }
        fn lag(&self, _: usize) -> Option<RecordView<'a, KeyedRows>> {
            None
        }
        fn lead(&self, _: usize) -> Option<RecordView<'a, KeyedRows>> {
            None
        }
        fn count(&self) -> i64 {
            self.storage.keys.len() as i64
        }
        fn sum(&self, _: &str) -> Value {
            Value::Null
        }
        fn cumulative_sum(&self, _: &str) -> Value {
            Value::Null
        }
        fn avg(&self, _: &str) -> Value {
            Value::Null
        }
        fn min(&self, _: &str) -> Value {
            Value::Null
        }
        fn max(&self, _: &str) -> Value {
            Value::Null
        }
        fn partition_len(&self) -> usize {
            self.storage.keys.len()
        }
        fn partition_record(&self, index: usize) -> RecordView<'a, KeyedRows> {
            RecordView::new(self.storage, index as u64)
        }
        fn collect(&self, _: &str) -> Value {
            Value::Null
        }
        fn distinct(&self, _: &str) -> Value {
            Value::Null
        }
        fn row_number(&self) -> i64 {
            self.current_pos as i64 + 1
        }
        fn rank(&self) -> i64 {
            if self.storage.keys.is_empty() {
                return 1;
            }
            let mut rank = 1i64;
            for i in 1..=self.current_pos {
                if self.storage.keys[i] != self.storage.keys[i - 1] {
                    rank = (i as i64) + 1;
                }
            }
            rank
        }
        fn dense_rank(&self) -> i64 {
            if self.storage.keys.is_empty() {
                return 1;
            }
            let mut rank = 1i64;
            for i in 1..=self.current_pos {
                if self.storage.keys[i] != self.storage.keys[i - 1] {
                    rank += 1;
                }
            }
            rank
        }
        fn first_value(&self, field: &str) -> Value {
            match field {
                "k" => self.storage.keys.first().cloned().unwrap_or(Value::Null),
                "v" => self.storage.values.first().cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }
        fn last_value(&self, field: &str) -> Value {
            match field {
                "k" => self.storage.keys.last().cloned().unwrap_or(Value::Null),
                "v" => self.storage.values.last().cloned().unwrap_or(Value::Null),
                _ => Value::Null,
            }
        }
    }

    fn eval_with(
        src: &str,
        keys: Vec<Value>,
        values: Vec<Value>,
        current_pos: usize,
    ) -> indexmap::IndexMap<String, Value> {
        let parsed = Parser::parse(src);
        assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
        let resolved = resolve_program(parsed.ast, &["k", "v"], parsed.node_count)
            .unwrap_or_else(|d| panic!("resolve: {:?}", d));
        let mut cols = indexmap::IndexMap::new();
        cols.insert(
            QualifiedField::bare("k"),
            crate::typecheck::types::Type::Int,
        );
        cols.insert(
            QualifiedField::bare("v"),
            crate::typecheck::types::Type::Int,
        );
        let row = Row::closed(cols, Span::new(0, 0));
        let typed = type_check(resolved, &row).unwrap_or_else(|d| panic!("type: {:?}", d));
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        let storage = KeyedRows { keys, values };
        let resolver = HashMapResolver::new(HashMap::new());
        let window = RankWindow {
            storage: &storage,
            current_pos,
        };
        run_fields::<KeyedRows>(typed, &ctx, &resolver, Some(&window))
            .unwrap_or_else(|e| panic!("eval: {}", e))
    }

    fn i(n: i64) -> Value {
        Value::Integer(n)
    }

    #[test]
    fn row_number_is_1_indexed() {
        let out = eval_with(
            "emit n = $window.row_number()",
            vec![i(1), i(1), i(2), i(3)],
            vec![i(10), i(20), i(30), i(40)],
            0,
        );
        assert_eq!(out.get("n"), Some(&Value::Integer(1)));
        let out = eval_with(
            "emit n = $window.row_number()",
            vec![i(1), i(1), i(2), i(3)],
            vec![i(10), i(20), i(30), i(40)],
            3,
        );
        assert_eq!(out.get("n"), Some(&Value::Integer(4)));
    }

    #[test]
    fn rank_ties_share_value_then_gap() {
        // Keys: 1 1 2 2 2 5 → rank: 1 1 3 3 3 6 (gap of two after the
        // first tie pair, gap of two after the second tie triple).
        let keys = vec![i(1), i(1), i(2), i(2), i(2), i(5)];
        let vals = vec![i(0); 6];
        let expected = [1, 1, 3, 3, 3, 6];
        for (pos, exp) in expected.iter().enumerate() {
            let out = eval_with("emit r = $window.rank()", keys.clone(), vals.clone(), pos);
            assert_eq!(
                out.get("r"),
                Some(&Value::Integer(*exp)),
                "rank at pos {pos}"
            );
        }
    }

    #[test]
    fn dense_rank_no_gaps() {
        // Keys: 1 1 2 2 2 5 → dense_rank: 1 1 2 2 2 3 (each distinct
        // key bumps the rank by exactly one).
        let keys = vec![i(1), i(1), i(2), i(2), i(2), i(5)];
        let vals = vec![i(0); 6];
        let expected = [1, 1, 2, 2, 2, 3];
        for (pos, exp) in expected.iter().enumerate() {
            let out = eval_with(
                "emit r = $window.dense_rank()",
                keys.clone(),
                vals.clone(),
                pos,
            );
            assert_eq!(
                out.get("r"),
                Some(&Value::Integer(*exp)),
                "dense_rank at pos {pos}"
            );
        }
    }

    #[test]
    fn first_value_last_value_project_field() {
        let out = eval_with(
            "emit fv = $window.first_value(v)\nemit lv = $window.last_value(v)",
            vec![i(1), i(2), i(3)],
            vec![i(100), i(200), i(300)],
            1,
        );
        assert_eq!(out.get("fv"), Some(&Value::Integer(100)));
        assert_eq!(out.get("lv"), Some(&Value::Integer(300)));
    }
}

// ---------------------------------------------------------------------------
// Intra-record document support: closures, bracket-index, emit_each
// ---------------------------------------------------------------------------

mod intra_record {
    use super::*;
    use indexmap::IndexMap;
    use std::sync::Arc;

    /// Compile `src` against the given input fields and run a single
    /// record through the stateful `ProgramEvaluator`. Returns the
    /// `EvalResult` so tests can inspect fan-out vs single-emit.
    fn run(
        src: &str,
        fields: &[&str],
        record: HashMap<String, Value>,
    ) -> Result<EvalResult, crate::eval::EvalError> {
        run_with_max_expansion(src, fields, record, DEFAULT_MAX_EXPANSION)
    }

    fn run_with_max_expansion(
        src: &str,
        fields: &[&str],
        record: HashMap<String, Value>,
        max_expansion: u64,
    ) -> Result<EvalResult, crate::eval::EvalError> {
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
        let typed = type_check(resolved, &empty_row()).unwrap_or_else(|d| {
            panic!(
                "Type errors: {:?}",
                d.iter().map(|e| &e.message).collect::<Vec<_>>()
            )
        });
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        let resolver = HashMapResolver::new(record);
        let has_distinct = typed
            .program
            .statements
            .iter()
            .any(|s| matches!(s, crate::ast::Statement::Distinct { .. }));
        let mut evaluator = crate::eval::ProgramEvaluator::with_max_expansion(
            Arc::new(typed),
            has_distinct,
            max_expansion,
        );
        evaluator.eval_record::<NullStorage>(&ctx, &resolver, None)
    }

    fn arr(values: Vec<Value>) -> Value {
        Value::Array(values)
    }

    fn map(entries: Vec<(&str, Value)>) -> Value {
        let mut m = IndexMap::new();
        for (k, v) in entries {
            m.insert(k.into(), v);
        }
        Value::Map(Box::new(m))
    }

    #[test]
    fn closure_filter_returns_matching_elements() {
        let out = run(
            "emit val = nums.filter(it => it > 1)",
            &["nums"],
            HashMap::from([(
                "nums".into(),
                arr(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(3),
                ]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(
                    fields.get("val"),
                    Some(&arr(vec![Value::Integer(2), Value::Integer(3)]))
                );
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn closure_map_transforms_elements() {
        let out = run(
            "emit val = nums.map(it => it + 10)",
            &["nums"],
            HashMap::from([(
                "nums".into(),
                arr(vec![Value::Integer(1), Value::Integer(2)]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(
                    fields.get("val"),
                    Some(&arr(vec![Value::Integer(11), Value::Integer(12)]))
                );
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn closure_any_returns_bool() {
        let out = run(
            "emit val = nums.any(it => it > 5)",
            &["nums"],
            HashMap::from([(
                "nums".into(),
                arr(vec![Value::Integer(1), Value::Integer(7)]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(fields.get("val"), Some(&Value::Bool(true)));
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn bracket_index_array() {
        let out = run(
            "emit val = nums[1]",
            &["nums"],
            HashMap::from([(
                "nums".into(),
                arr(vec![
                    Value::Integer(10),
                    Value::Integer(20),
                    Value::Integer(30),
                ]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(fields.get("val"), Some(&Value::Integer(20)));
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn bracket_index_map_string_key() {
        let out = run(
            "emit val = m[\"k1\"]",
            &["m"],
            HashMap::from([("m".into(), map(vec![("k1", Value::Integer(42))]))]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(fields.get("val"), Some(&Value::Integer(42)));
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn map_keys_and_values() {
        let out = run(
            "emit ks = m.keys()\nemit vs = m.values()",
            &["m"],
            HashMap::from([(
                "m".into(),
                map(vec![("a", Value::Integer(1)), ("b", Value::Integer(2))]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(
                    fields.get("ks"),
                    Some(&arr(vec![
                        Value::String("a".into()),
                        Value::String("b".into()),
                    ]))
                );
                assert_eq!(
                    fields.get("vs"),
                    Some(&arr(vec![Value::Integer(1), Value::Integer(2)]))
                );
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn map_merge_and_set_and_remove_field() {
        let out = run(
            "emit merged = a.merge(b)\nemit with_x = a.set(\"x\", 99)\nemit without_a = a.remove_field(\"a\")",
            &["a", "b"],
            HashMap::from([
                ("a".into(), map(vec![("a", Value::Integer(1))])),
                ("b".into(), map(vec![("b", Value::Integer(2))])),
            ]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(
                    fields.get("merged"),
                    Some(&map(vec![
                        ("a", Value::Integer(1)),
                        ("b", Value::Integer(2))
                    ]))
                );
                assert_eq!(
                    fields.get("with_x"),
                    Some(&map(vec![
                        ("a", Value::Integer(1)),
                        ("x", Value::Integer(99))
                    ]))
                );
                assert_eq!(fields.get("without_a"), Some(&map(vec![])));
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn nested_set_replaces_existing_leaf() {
        let out = run(
            "emit updated = doc.set(\"address.city\", \"NYC\")",
            &["doc"],
            HashMap::from([(
                "doc".into(),
                map(vec![(
                    "address",
                    map(vec![
                        ("city", Value::String("LA".into())),
                        ("zip", Value::String("90001".into())),
                    ]),
                )]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(
                    fields.get("updated"),
                    Some(&map(vec![(
                        "address",
                        map(vec![
                            ("city", Value::String("NYC".into())),
                            ("zip", Value::String("90001".into())),
                        ]),
                    )]))
                );
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn nested_set_auto_creates_missing_intermediates() {
        let out = run(
            "emit built = doc.set(\"a.b.c\", 7)",
            &["doc"],
            HashMap::from([("doc".into(), map(vec![]))]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(
                    fields.get("built"),
                    Some(&map(vec![(
                        "a",
                        map(vec![("b", map(vec![("c", Value::Integer(7))]))]),
                    )]))
                );
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn nested_set_type_conflict_returns_null() {
        // `a` exists but is a scalar, so descending into `a.b` is a hard
        // conflict: the whole op returns Null.
        let out = run(
            "emit r = doc.set(\"a.b\", 1)",
            &["doc"],
            HashMap::from([(
                "doc".into(),
                map(vec![("a", Value::String("scalar".into()))]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(fields.get("r"), Some(&Value::Null));
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn nested_set_into_existing_array_element() {
        let out = run(
            "emit r = doc.set(\"items[1].name\", \"X\")",
            &["doc"],
            HashMap::from([(
                "doc".into(),
                map(vec![(
                    "items",
                    arr(vec![
                        map(vec![("name", Value::String("a".into()))]),
                        map(vec![("name", Value::String("b".into()))]),
                    ]),
                )]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(
                    fields.get("r"),
                    Some(&map(vec![(
                        "items",
                        arr(vec![
                            map(vec![("name", Value::String("a".into()))]),
                            map(vec![("name", Value::String("X".into()))]),
                        ]),
                    )]))
                );
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn nested_set_array_index_past_end_returns_null() {
        // Index 5 is past the 2-element array → Null for the whole op, with no
        // silent auto-grow.
        let out = run(
            "emit r = doc.set(\"items[5].name\", \"X\")",
            &["doc"],
            HashMap::from([(
                "doc".into(),
                map(vec![(
                    "items",
                    arr(vec![
                        map(vec![("name", Value::String("a".into()))]),
                        map(vec![("name", Value::String("b".into()))]),
                    ]),
                )]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(fields.get("r"), Some(&Value::Null));
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn nested_set_field_into_array_returns_null() {
        // `items` is an array; a field segment against it is a type conflict.
        let out = run(
            "emit r = doc.set(\"items.name\", \"X\")",
            &["doc"],
            HashMap::from([(
                "doc".into(),
                map(vec![("items", arr(vec![Value::Integer(1)]))]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(fields.get("r"), Some(&Value::Null));
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn nested_set_does_not_mutate_receiver() {
        // `.set` is copy-on-write: emitting both the original and the mutated
        // path must show the original untouched.
        let out = run(
            "emit before = doc\nemit after = doc.set(\"a.b\", 9)",
            &["doc"],
            HashMap::from([(
                "doc".into(),
                map(vec![("a", map(vec![("b", Value::Integer(1))]))]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(
                    fields.get("before"),
                    Some(&map(vec![("a", map(vec![("b", Value::Integer(1))]))]))
                );
                assert_eq!(
                    fields.get("after"),
                    Some(&map(vec![("a", map(vec![("b", Value::Integer(9))]))]))
                );
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn single_key_set_still_inserts_at_top_level() {
        // The bare-key fast path must be unchanged by the nested-path work.
        let out = run(
            "emit r = doc.set(\"k\", 1)",
            &["doc"],
            HashMap::from([("doc".into(), map(vec![]))]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(fields.get("r"), Some(&map(vec![("k", Value::Integer(1))])));
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn array_remove_returns_new_array_without_index() {
        let out = run(
            "emit val = nums.remove(1)",
            &["nums"],
            HashMap::from([(
                "nums".into(),
                arr(vec![
                    Value::Integer(10),
                    Value::Integer(20),
                    Value::Integer(30),
                ]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::Emit { fields, .. } => {
                assert_eq!(
                    fields.get("val"),
                    Some(&arr(vec![Value::Integer(10), Value::Integer(30)]))
                );
            }
            other => panic!("expected Emit, got {other:?}"),
        }
    }

    #[test]
    fn emit_each_fans_out_one_per_element() {
        let out = run(
            "emit each it in nums {\n  emit val = it\n}",
            &["nums"],
            HashMap::from([(
                "nums".into(),
                arr(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(3),
                ]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::EmitMany { records } => {
                assert_eq!(records.len(), 3);
                assert_eq!(records[0].fields.get("val"), Some(&Value::Integer(1)));
                assert_eq!(records[1].fields.get("val"), Some(&Value::Integer(2)));
                assert_eq!(records[2].fields.get("val"), Some(&Value::Integer(3)));
            }
            other => panic!("expected EmitMany, got {other:?}"),
        }
    }

    #[test]
    fn emit_each_null_source_emits_nothing() {
        let out = run(
            "emit each it in nums {\n  emit val = it\n}",
            &["nums"],
            HashMap::from([("nums".into(), Value::Null)]),
        )
        .unwrap();
        // Null source produces no fan-out and no single Emit either —
        // the evaluator falls through to an empty Emit (output map is
        // empty).
        match out {
            EvalResult::Emit { fields, .. } => {
                assert!(fields.is_empty(), "expected empty Emit, got {fields:?}");
            }
            other => panic!("expected empty Emit on null source, got {other:?}"),
        }
    }

    #[test]
    fn emit_each_max_expansion_overflow_dlqs() {
        let big: Vec<Value> = (0..20).map(Value::Integer).collect();
        let err = run_with_max_expansion(
            "emit each it in nums {\n  emit val = it\n}",
            &["nums"],
            HashMap::from([("nums".into(), arr(big))]),
            5,
        )
        .expect_err("expected ExpansionLimitExceeded");
        match err.kind {
            crate::eval::EvalErrorKind::ExpansionLimitExceeded { limit } => {
                assert_eq!(limit, 5);
            }
            other => panic!("expected ExpansionLimitExceeded, got {other:?}"),
        }
    }

    #[test]
    fn nested_emit_each_rejected_by_parser() {
        let result =
            Parser::parse("emit each it in nums {\n  emit each x in others { emit val = x }\n}");
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.message.contains("emit_each cannot be nested")),
            "expected nested emit_each parse error, got: {:?}",
            result.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    #[test]
    fn free_standing_closure_rejected_at_parse() {
        // The parser only accepts a closure inside a method-call
        // argument list, so a let-binding to a closure does not parse:
        // the lhs (`it`) parses as a FieldRef, then the `=>` token is
        // unexpected because no led handler consumes it outside a
        // closure context.
        let parsed = Parser::parse("let f = it => it + 1\nemit val = 1");
        assert!(
            !parsed.errors.is_empty(),
            "expected parse error for free-standing closure"
        );
        assert!(
            parsed
                .errors
                .iter()
                .any(|e| e.message.contains("FatArrow") || e.message.contains("unexpected")),
            "expected FatArrow / unexpected-token diagnostic, got: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }

    // ── explode_outer (`emit each ... outer`) ──────────────────────
    // The outer variant matches `emit each` on a non-empty array but
    // preserves the trigger row (binding bound to null) when the source
    // is null or empty.

    #[test]
    fn explode_outer_fans_out_one_per_element_when_non_empty() {
        let out = run(
            "emit each it in nums outer {\n  emit val = it\n}",
            &["nums"],
            HashMap::from([(
                "nums".into(),
                arr(vec![
                    Value::Integer(1),
                    Value::Integer(2),
                    Value::Integer(3),
                ]),
            )]),
        )
        .unwrap();
        match out {
            EvalResult::EmitMany { records } => {
                assert_eq!(records.len(), 3);
                assert_eq!(records[0].fields.get("val"), Some(&Value::Integer(1)));
                assert_eq!(records[1].fields.get("val"), Some(&Value::Integer(2)));
                assert_eq!(records[2].fields.get("val"), Some(&Value::Integer(3)));
            }
            other => panic!("expected EmitMany, got {other:?}"),
        }
    }

    #[test]
    fn explode_outer_null_source_preserves_trigger_row_with_null_binding() {
        let out = run(
            "emit each it in nums outer {\n  emit val = it\n}",
            &["nums"],
            HashMap::from([("nums".into(), Value::Null)]),
        )
        .unwrap();
        // Unlike `emit each` (which emits nothing on a null source), the
        // outer variant emits exactly one record with the binding bound
        // to null — the LATERAL VIEW OUTER EXPLODE shape.
        match out {
            EvalResult::EmitMany { records } => {
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].fields.get("val"), Some(&Value::Null));
            }
            other => panic!("expected single EmitMany record on null source, got {other:?}"),
        }
    }

    #[test]
    fn explode_outer_empty_array_preserves_trigger_row_with_null_binding() {
        let out = run(
            "emit each it in nums outer {\n  emit val = it\n}",
            &["nums"],
            HashMap::from([("nums".into(), arr(vec![]))]),
        )
        .unwrap();
        match out {
            EvalResult::EmitMany { records } => {
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].fields.get("val"), Some(&Value::Null));
            }
            other => panic!("expected single EmitMany record on empty array, got {other:?}"),
        }
    }

    #[test]
    fn explode_outer_trigger_row_carries_pre_block_emits() {
        // A top-level emit preceding the fan-out applies to the preserved
        // trigger row too, so the outer-join row is never bare.
        let out = run(
            "emit kept = 7\nemit each it in nums outer {\n  emit val = it\n}",
            &["nums"],
            HashMap::from([("nums".into(), Value::Null)]),
        )
        .unwrap();
        match out {
            EvalResult::EmitMany { records } => {
                assert_eq!(records.len(), 1);
                assert_eq!(records[0].fields.get("kept"), Some(&Value::Integer(7)));
                assert_eq!(records[0].fields.get("val"), Some(&Value::Null));
            }
            other => {
                panic!("expected single EmitMany record carrying pre-block emit, got {other:?}")
            }
        }
    }

    #[test]
    fn explode_outer_max_expansion_governs_non_empty_fan_out() {
        let big: Vec<Value> = (0..20).map(Value::Integer).collect();
        let err = run_with_max_expansion(
            "emit each it in nums outer {\n  emit val = it\n}",
            &["nums"],
            HashMap::from([("nums".into(), arr(big))]),
            5,
        )
        .expect_err("expected ExpansionLimitExceeded");
        match err.kind {
            crate::eval::EvalErrorKind::ExpansionLimitExceeded { limit } => {
                assert_eq!(limit, 5);
            }
            other => panic!("expected ExpansionLimitExceeded, got {other:?}"),
        }
    }

    #[test]
    fn nested_explode_outer_rejected_by_parser() {
        let result = Parser::parse(
            "emit each it in nums outer {\n  emit each x in others outer { emit val = x }\n}",
        );
        assert!(
            result
                .errors
                .iter()
                .any(|e| e.message.contains("emit_each cannot be nested")),
            "expected nested fan-out parse error, got: {:?}",
            result.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
    }
}
