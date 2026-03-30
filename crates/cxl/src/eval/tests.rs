use std::collections::HashMap;

use chrono::Datelike;
use clinker_record::Value;

use crate::parser::Parser;
use crate::resolve::pass::resolve_program;
use crate::resolve::HashMapResolver;
use crate::typecheck::pass::type_check;
use crate::typecheck::Type;
use super::*;

fn eval_ok(src: &str, fields: &[&str], record: HashMap<String, Value>) -> indexmap::IndexMap<String, Value> {
    let parsed = Parser::parse(src);
    assert!(parsed.errors.is_empty(), "Parse errors: {:?}",
        parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>());
    let resolved = resolve_program(parsed.ast, fields, parsed.node_count)
        .unwrap_or_else(|d| panic!("Resolve errors: {:?}", d.iter().map(|e| &e.message).collect::<Vec<_>>()));
    let typed = type_check(resolved, &HashMap::new())
        .unwrap_or_else(|d| panic!("Type errors: {:?}", d.iter().map(|e| &e.message).collect::<Vec<_>>()));
    let ctx = EvalContext::test_default();
    let resolver = HashMapResolver::new(record);
    eval_program(&typed, &ctx, &resolver, None)
        .unwrap_or_else(|e| panic!("Eval error: {}", e))
}

fn eval_single(src: &str, fields: &[&str], record: HashMap<String, Value>) -> Value {
    let output = eval_ok(src, fields, record);
    output.into_values().next().unwrap_or(Value::Null)
}

#[test]
fn test_eval_arithmetic_null_propagation() {
    // Null + 1 → Null
    let v = eval_single("emit val = x + 1", &["x"], HashMap::from([("x".into(), Value::Null)]));
    assert_eq!(v, Value::Null);
    // 2 + 3 → 5
    let v2 = eval_single("emit val = 2 + 3", &[], HashMap::new());
    assert_eq!(v2, Value::Integer(5));
}

#[test]
fn test_eval_comparison_null_semantics() {
    // Null == Null → true
    let v = eval_single("emit val = x == null", &["x"], HashMap::from([("x".into(), Value::Null)]));
    assert_eq!(v, Value::Bool(true));
    // Null != 1 → true
    let v2 = eval_single("emit val = x != 1", &["x"], HashMap::from([("x".into(), Value::Null)]));
    assert_eq!(v2, Value::Bool(true));
    // Null > 1 → Null
    let v3 = eval_single("emit val = x > 1", &["x"], HashMap::from([("x".into(), Value::Null)]));
    assert_eq!(v3, Value::Null);
}

#[test]
fn test_eval_coalesce_short_circuit() {
    // "hello" ?? <anything> → "hello" without evaluating RHS
    // We use a valid RHS to avoid errors, but the point is LHS wins
    let v = eval_single(
        "emit val = \"hello\" ?? \"world\"",
        &[],
        HashMap::new(),
    );
    assert_eq!(v, Value::String("hello".into()));
}

#[test]
fn test_eval_let_binding_scope() {
    let output = eval_ok(
        "let x = 10\nemit val = x + 1",
        &[],
        HashMap::new(),
    );
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
        eval_single("emit val = \"hello world\".replace(\"world\", \"rust\")", &[], HashMap::new()),
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
        eval_single("emit val = \"123\".matches(\"\\\\d+\")", &[], HashMap::new()),
        Value::Bool(true)
    );
    // .capture extracts a match
    assert_eq!(
        eval_single("emit val = \"abc123def\".capture(\"(\\\\d+)\")", &[], HashMap::new()),
        Value::String("123".into())
    );
}

#[test]
fn test_eval_string_methods_path() {
    assert_eq!(
        eval_single("emit val = \"data/orders.csv\".file_name()", &[], HashMap::new()),
        Value::String("orders.csv".into())
    );
    assert_eq!(
        eval_single("emit val = \"data/orders.csv\".file_stem()", &[], HashMap::new()),
        Value::String("orders".into())
    );
    assert_eq!(
        eval_single("emit val = \"data/orders.csv\".extension()", &[], HashMap::new()),
        Value::String("csv".into())
    );
    assert_eq!(
        eval_single("emit val = \"data/batch/orders.csv\".parent()", &[], HashMap::new()),
        Value::String("data/batch".into())
    );
    assert_eq!(
        eval_single("emit val = \"data/batch/orders.csv\".parent_name()", &[], HashMap::new()),
        Value::String("batch".into())
    );
}

#[test]
fn test_eval_numeric_methods() {
    assert_eq!(eval_single("emit val = (-5).abs()", &[], HashMap::new()), Value::Integer(5));
    assert_eq!(eval_single("emit val = 3.7.ceil()", &[], HashMap::new()), Value::Integer(4));
    assert_eq!(eval_single("emit val = 3.2.floor()", &[], HashMap::new()), Value::Integer(3));
    assert_eq!(eval_single("emit val = 3.456.round()", &[], HashMap::new()), Value::Float(3.0));
    assert_eq!(eval_single("emit val = 3.456.round_to(2)", &[], HashMap::new()), Value::Float(3.46));
    // clamp
    assert_eq!(
        eval_single("emit val = x.clamp(0, 100)", &["x"], HashMap::from([("x".into(), Value::Integer(150))])),
        Value::Integer(100)
    );
}

#[test]
fn test_eval_date_methods() {
    let record = HashMap::from([("d".into(), Value::Date(chrono::NaiveDate::from_ymd_opt(2026, 3, 15).unwrap()))]);
    assert_eq!(eval_single("emit val = d.year()", &["d"], record.clone()), Value::Integer(2026));
    assert_eq!(eval_single("emit val = d.month()", &["d"], record.clone()), Value::Integer(3));
    assert_eq!(eval_single("emit val = d.day()", &["d"], record.clone()), Value::Integer(15));
    // add_days
    let v = eval_single("emit val = d.add_days(7)", &["d"], record.clone());
    assert_eq!(v, Value::Date(chrono::NaiveDate::from_ymd_opt(2026, 3, 22).unwrap()));
    // format_date
    let v2 = eval_single("emit val = d.format_date(\"%Y-%m-%d\")", &["d"], record);
    assert_eq!(v2, Value::String("2026-03-15".into()));
}

#[test]
fn test_eval_conversion_strict() {
    assert_eq!(eval_single("emit val = \"42\".to_int()", &[], HashMap::new()), Value::Integer(42));
    // to_int on "abc" → error
    let parsed = Parser::parse("emit val = \"abc\".to_int()");
    let resolved = resolve_program(parsed.ast, &[], parsed.node_count).unwrap();
    let typed = type_check(resolved, &HashMap::new()).unwrap();
    let ctx = EvalContext::test_default();
    let resolver = HashMapResolver::new(HashMap::new());
    let result = eval_program(&typed, &ctx, &resolver, None);
    assert!(result.is_err(), "Expected conversion error for \"abc\".to_int()");
}

#[test]
fn test_eval_conversion_lenient() {
    assert_eq!(eval_single("emit val = \"abc\".try_int()", &[], HashMap::new()), Value::Null);
    assert_eq!(eval_single("emit val = \"3.14\".try_float()", &[], HashMap::new()), Value::Float(3.14));
}

#[test]
fn test_eval_introspection() {
    assert_eq!(eval_single("emit val = \"hello\".type_of()", &[], HashMap::new()), Value::String("string".into()));
    assert_eq!(eval_single("emit val = 42.type_of()", &[], HashMap::new()), Value::String("int".into()));
    assert_eq!(eval_single("emit val = null.is_null()", &[], HashMap::new()), Value::Bool(true));
    assert_eq!(eval_single("emit val = 42.is_null()", &[], HashMap::new()), Value::Bool(false));
}

#[test]
fn test_eval_regex_precompiled() {
    // Regex should come from TypedProgram.regexes, pre-compiled during type check
    let parsed = Parser::parse("emit val = \"123\".matches(\"\\\\d+\")");
    let resolved = resolve_program(parsed.ast, &[], parsed.node_count).unwrap();
    let typed = type_check(resolved, &HashMap::new()).unwrap();
    // Verify regex was pre-compiled
    let has_regex = typed.regexes.iter().any(|r| r.is_some());
    assert!(has_regex, "Expected pre-compiled regex in TypedProgram");
    // And evaluation uses it
    let ctx = EvalContext::test_default();
    let resolver = HashMapResolver::new(HashMap::new());
    let output = eval_program(&typed, &ctx, &resolver, None).unwrap();
    assert_eq!(output.get("val"), Some(&Value::Bool(true)));
}

#[test]
fn test_eval_emit_output_map() {
    let output = eval_ok(
        "emit a = 1\nemit b = 2\nemit c = 3",
        &[],
        HashMap::new(),
    );
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
    let v = eval_single("emit val = false and x", &["x"], HashMap::from([("x".into(), Value::Null)]));
    assert_eq!(v, Value::Bool(false));
}

#[test]
fn test_eval_three_valued_or() {
    // true || null → true (not null)
    let v = eval_single("emit val = true or x", &["x"], HashMap::from([("x".into(), Value::Null)]));
    assert_eq!(v, Value::Bool(true));
}
