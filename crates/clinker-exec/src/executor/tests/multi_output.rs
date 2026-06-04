//! Multi-output route-evaluation white-box tests (inline half).
//!
//! These compile and evaluate routes directly through the crate-private
//! `PipelineExecutor::compile_route` / `CompiledRoute::evaluate` seam, so
//! they cannot move to the `tests/` directory. The multi-writer
//! integration tests (channel fan-out, DLQ staging, backward-compat
//! shims) run through the public entry point in
//! `crates/clinker-exec/tests/multi_output.rs`.

use super::*;

/// Helper: compile a route config against a set of field names.
fn compile_test_route(route_yaml: &str, fields: &[&str]) -> CompiledRoute {
    let route_config: clinker_plan::config::RouteConfig =
        clinker_plan::yaml::from_str(route_yaml).unwrap();
    let emitted_fields: Vec<String> = fields.iter().map(|s| s.to_string()).collect();
    PipelineExecutor::compile_route(&route_config, &emitted_fields, &Default::default()).unwrap()
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

#[test]
fn test_route_config_non_boolean_condition_typecheck_error() {
    // "amount + 1" returns Int, not Bool — compile_route should reject it
    let route_yaml = r#"
mode: exclusive
branches:
  - name: bad
    condition: "amount + 1"
default: fallback
"#;
    let route_config: clinker_plan::config::RouteConfig =
        clinker_plan::yaml::from_str(route_yaml).unwrap();
    let emitted_fields = vec!["amount".to_string()];
    let result =
        PipelineExecutor::compile_route(&route_config, &emitted_fields, &Default::default());
    match result {
        Err(e) => {
            let err = e.to_string();
            assert!(
                err.contains("Bool"),
                "error should mention Bool type requirement: {err}"
            );
        }
        Ok(_) => panic!("non-boolean route condition should fail typecheck"),
    }
}

#[test]
fn test_route_config_boolean_condition_passes() {
    // "amount > 10000" returns Bool — compile_route should succeed
    let route_yaml = r#"
mode: exclusive
branches:
  - name: high
    condition: "amount > 10000"
default: low
"#;
    let route_config: clinker_plan::config::RouteConfig =
        clinker_plan::yaml::from_str(route_yaml).unwrap();
    let emitted_fields = vec!["amount".to_string()];
    let result =
        PipelineExecutor::compile_route(&route_config, &emitted_fields, &Default::default());
    assert!(
        result.is_ok(),
        "boolean route condition should pass typecheck: {}",
        result.err().map(|e| e.to_string()).unwrap_or_default()
    );
}
