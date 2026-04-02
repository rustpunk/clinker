//! Multi-output routing tests for Phase 13.
//!
//! Tests in this module exercise the multi-writer registry, route condition
//! evaluation, per-output channels, and DLQ stage/route extensions.

use super::*;
use crate::test_helpers::SharedBuffer;
use std::collections::HashMap;

/// Build a multi-output test fixture with the given YAML config.
///
/// Returns the parsed `PipelineConfig` and a `HashMap<String, SharedBuffer>`
/// with one entry per output defined in the config. The buffer names match
/// the output `name` fields in the YAML.
fn multi_output_fixture(
    yaml: &str,
) -> (crate::config::PipelineConfig, HashMap<String, SharedBuffer>) {
    let config = crate::config::parse_config(yaml).unwrap();
    let buffers: HashMap<String, SharedBuffer> = config
        .outputs
        .iter()
        .map(|o| (o.name.clone(), SharedBuffer::new()))
        .collect();
    (config, buffers)
}

/// Build default `PipelineRunParams` for tests.
fn test_params(config: &crate::config::PipelineConfig) -> PipelineRunParams {
    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(|v| crate::config::convert_pipeline_vars(v))
        .unwrap_or_default();
    PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars,
    }
}

/// Helper: compile a route config against a set of field names.
fn compile_test_route(route_yaml: &str, fields: &[&str]) -> CompiledRoute {
    let route_config: crate::config::RouteConfig = serde_saphyr::from_str(route_yaml).unwrap();
    let emitted_fields: Vec<String> = fields.iter().map(|s| s.to_string()).collect();
    PipelineExecutor::compile_route(&route_config, &emitted_fields).unwrap()
}

/// Helper: build an EvalContext for route evaluation tests.
fn test_eval_context() -> cxl::eval::EvalContext {
    cxl::eval::EvalContext::test_default()
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
    assert_eq!(targets, vec!["intl_high"]);

    // Does not match: high amount but US
    let emitted = indexmap::IndexMap::from([
        ("amount".to_string(), Value::Integer(50000)),
        ("country".to_string(), Value::String("US".into())),
    ]);
    let targets = route.evaluate(&emitted, &ctx).unwrap();
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
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
    let targets = route.evaluate(&emitted, &ctx).unwrap();
    assert_eq!(targets, vec!["vip"]);
}
