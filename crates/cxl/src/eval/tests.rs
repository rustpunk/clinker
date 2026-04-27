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

/// Dummy storage for no-window evaluation tests.
struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _: u32) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u32 {
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
    eval_program::<NullStorage>(&typed, &ctx, &resolver, None)
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
    let result = eval_program::<NullStorage>(&typed, &ctx, &resolver, None);
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
        eval_single("emit val = \"3.14\".try_float()", &[], HashMap::new()),
        Value::Float(3.14)
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
    let output = eval_program::<NullStorage>(&typed, &ctx, &resolver, None).unwrap();
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
