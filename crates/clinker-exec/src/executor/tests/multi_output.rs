//! Multi-output route-evaluation white-box tests (inline half).
//!
//! These build and evaluate routes directly through the crate-private
//! `CompiledRoute::from_node` / `CompiledRoute::evaluate` seam, so they
//! cannot move to the `tests/` directory. The multi-writer integration
//! tests (channel fan-out, DLQ staging, backward-compat shims) run through
//! the public entry point in `crates/clinker-exec/tests/multi_output.rs`.

use super::*;

/// Helper: build a [`CompiledRoute`] for a route config against a set of
/// field names.
///
/// Mirrors the binder: each branch condition is typechecked as a
/// `filter <condition>` program against an all-`Any` row of `fields`, and the
/// resulting typed programs are handed to the production
/// [`CompiledRoute::from_node`] constructor — so the evaluation tests exercise
/// the same on-node evaluator-build path the executor uses at dispatch.
fn compile_test_route(route_yaml: &str, fields: &[&str]) -> CompiledRoute {
    use std::sync::Arc;
    let route_config: clinker_plan::config::RouteConfig =
        clinker_plan::yaml::from_str(route_yaml).unwrap();
    let branches: Vec<String> = route_config
        .branches
        .iter()
        .map(|b| b.name.clone())
        .collect();
    let programs: Vec<Arc<cxl::typecheck::TypedProgram>> = route_config
        .branches
        .iter()
        .map(|b| Arc::new(typecheck_test_condition(&b.condition, fields)))
        .collect();
    CompiledRoute::from_node(
        "test_route",
        &branches,
        &programs,
        &route_config.default,
        route_config.mode,
    )
    .unwrap()
}

/// Typecheck a single route branch condition into the `filter <condition>`
/// program the binder produces, against an all-`Any` row of `fields`.
/// Panics on parse / resolve / typecheck failure (the caller asserts success).
fn typecheck_test_condition(condition: &str, fields: &[&str]) -> cxl::typecheck::TypedProgram {
    let type_cols: indexmap::IndexMap<cxl::typecheck::QualifiedField, cxl::typecheck::Type> =
        fields
            .iter()
            .map(|f| {
                (
                    cxl::typecheck::QualifiedField::bare(*f),
                    cxl::typecheck::Type::Any,
                )
            })
            .collect();
    let type_schema = cxl::typecheck::Row::closed(type_cols, cxl::lexer::Span::new(0, 0));
    let source = format!("filter {condition}");
    let parsed = cxl::parser::Parser::parse(&source);
    assert!(parsed.errors.is_empty(), "parse failed for {source:?}");
    let resolved = cxl::resolve::resolve_program_with_modules_and_vars(
        parsed.ast,
        fields,
        parsed.node_count,
        &std::collections::HashMap::new(),
        &Default::default(),
    )
    .expect("resolve failed");
    cxl::typecheck::pass::type_check_with_mode_and_vars(
        resolved,
        &type_schema,
        cxl::typecheck::pass::AggregateMode::Row,
        &Default::default(),
    )
    .expect("typecheck failed")
}

/// Helper: build an EvalContext for route evaluation tests.
fn test_eval_context() -> cxl::eval::EvalContext<'static> {
    use std::sync::{Arc, OnceLock};
    static STABLE: OnceLock<cxl::eval::StableEvalContext> = OnceLock::new();
    static SOURCE_FILE: OnceLock<Arc<str>> = OnceLock::new();
    cxl::eval::EvalContext::test_with_file(
        STABLE.get_or_init(cxl::eval::StableEvalContext::test_default),
        SOURCE_FILE.get_or_init(|| Arc::from("test.csv")),
        1,
    )
}

/// Helper: assemble a Record from an `emitted` map for the
/// `CompiledRoute::evaluate(&Record, ...)` shape. Schema columns
/// take insertion order of `emitted`.
fn test_record(emitted: &indexmap::IndexMap<String, Value>) -> clinker_record::Record {
    use std::sync::Arc;
    let columns: Vec<Box<str>> = emitted.keys().map(|k| k.as_str().into()).collect();
    let schema = Arc::new(clinker_record::Schema::new(columns));
    let values: Vec<Value> = emitted.values().cloned().collect();
    clinker_record::Record::new(schema, values)
}

// --- Route evaluation unit tests ---

#[test]
fn test_route_eval_exclusive_first_match() {
    let mut route = compile_test_route(
        r#"
mode: exclusive
branches:
  - name: high
    condition: "amount > 10000"
  - name: medium
    condition: "amount > 1000"
default: low
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(50000))]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["high"]);
}

#[test]
fn test_route_eval_exclusive_second_match() {
    let mut route = compile_test_route(
        r#"
mode: exclusive
branches:
  - name: high
    condition: "amount > 10000"
  - name: medium
    condition: "amount > 1000"
default: low
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(5000))]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["medium"]);
}

#[test]
fn test_route_eval_exclusive_default() {
    let mut route = compile_test_route(
        r#"
mode: exclusive
branches:
  - name: high
    condition: "amount > 10000"
  - name: medium
    condition: "amount > 1000"
default: low
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(500))]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["low"]);
}

#[test]
fn test_route_eval_inclusive_all_match() {
    let mut route = compile_test_route(
        r#"
mode: inclusive
branches:
  - name: audit
    condition: "amount > 1000"
  - name: report
    condition: "amount > 500"
default: standard
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(5000))]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["audit", "report"]);
}

#[test]
fn test_route_eval_inclusive_some_match() {
    let mut route = compile_test_route(
        r#"
mode: inclusive
branches:
  - name: audit
    condition: "amount > 10000"
  - name: report
    condition: "amount > 500"
  - name: archive
    condition: "amount > 50000"
default: standard
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(5000))]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["report"]);
}

#[test]
fn test_route_eval_inclusive_none_match() {
    let mut route = compile_test_route(
        r#"
mode: inclusive
branches:
  - name: audit
    condition: "amount > 10000"
  - name: report
    condition: "amount > 5000"
default: standard
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(100))]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["standard"]);
}

#[test]
fn test_route_eval_complex_and_or() {
    let mut route = compile_test_route(
        r#"
branches:
  - name: intl_high
    condition: "amount > 10000 and country != 'US'"
default: standard
"#,
        &["amount", "country"],
    );

    let ctx = test_eval_context();

    // Matches: high amount AND non-US
    let emitted = indexmap::IndexMap::from([
        ("amount".to_string(), Value::Integer(50000)),
        ("country".to_string(), Value::String("UK".into())),
    ]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["intl_high"]);

    // Does not match: high amount but US
    let emitted = indexmap::IndexMap::from([
        ("amount".to_string(), Value::Integer(50000)),
        ("country".to_string(), Value::String("US".into())),
    ]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["standard"]);
}

#[test]
fn test_route_eval_null_field_in_condition() {
    let mut route = compile_test_route(
        r#"
branches:
  - name: high
    condition: "amount > 10000"
default: standard
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Null)]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    // Null > 10000 is not true → default
    assert_eq!(targets, vec!["standard"]);
}

#[test]
fn test_route_eval_emitted_field_in_condition() {
    // Route condition references a field that was emitted by a transform (not original input)
    let mut route = compile_test_route(
        r#"
branches:
  - name: high
    condition: "computed_score > 90"
default: standard
"#,
        &["computed_score"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("computed_score".to_string(), Value::Integer(95))]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["high"]);
}

#[test]
fn test_route_eval_let_binding_in_condition() {
    // Route conditions don't have let bindings (they're filter-only),
    // but they can reference fields that were created by let+emit in transforms.
    // This test verifies that emitted fields are accessible.
    let mut route = compile_test_route(
        r#"
branches:
  - name: vip
    condition: "tier == 'gold'"
default: regular
"#,
        &["tier"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("tier".to_string(), Value::String("gold".into()))]);
    let targets = route.evaluate(&test_record(&emitted), &ctx).unwrap();
    assert_eq!(targets, vec!["vip"]);
}
