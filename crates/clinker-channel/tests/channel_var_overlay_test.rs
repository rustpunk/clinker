//! Channel-overlay var resolution tests for issue #45.
//!
//! Covers the four registries (`$vars.*`, `$pipeline.*`, `$source.*`,
//! `$record.*`) crossed with override + add semantics, plus the
//! reserved-name guard, the unknown-source guard, the type-mismatch
//! check, and the composition-target guard.

use std::path::PathBuf;

use clinker_channel::binding::ChannelBinding;
use clinker_channel::overlay::apply_channel_overlay;
use clinker_core_types::Severity;
use clinker_record::Value;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("clinker-exec/tests/fixtures")
}

/// Compile the var-overlay fixture pipeline into (config, plan).
fn compile_vars_fixture() -> (
    clinker_plan::config::PipelineConfig,
    clinker_plan::plan::CompiledPlan,
) {
    let root = fixtures_dir();
    let yaml_path = root.join("pipelines/channel_vars_pipeline.yaml");
    let yaml = std::fs::read_to_string(&yaml_path).unwrap();
    let config: clinker_plan::config::PipelineConfig =
        clinker_plan::yaml::from_str(&yaml).expect("parse vars fixture");
    let ctx =
        clinker_plan::config::CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = clinker_plan::config::PipelineConfig::compile(&config, &ctx).expect("compile");
    (config, plan)
}

fn binding(yaml: &[u8]) -> ChannelBinding {
    ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("test.channel.yaml"))
        .expect("channel YAML parses")
}

fn errors_with_code<'a>(
    diags: &'a [clinker_core_types::Diagnostic],
    code: &str,
) -> Vec<&'a clinker_core_types::Diagnostic> {
    diags
        .iter()
        .filter(|d| matches!(d.severity, Severity::Error) && d.code == code)
        .collect()
}

// ── $vars (static) ─────────────────────────────────────────────────────

#[test]
fn static_var_override_applied() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  static:
    fuzzy_threshold:
      type: float
      default: 0.92
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
    assert_eq!(
        result.static_vars.get("fuzzy_threshold"),
        Some(&Value::Float(0.92)),
    );
}

#[test]
fn static_var_add_applied() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  static:
    new_static_knob:
      type: int
      default: 7
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
    assert_eq!(
        result.static_vars.get("new_static_knob"),
        Some(&Value::Integer(7)),
    );
}

#[test]
fn static_var_type_mismatch_emits_e107() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  static:
    fuzzy_threshold:
      type: string
      default: "wrong"
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    let errs = errors_with_code(&result.diagnostics, "E107");
    assert_eq!(
        errs.len(),
        1,
        "expected one E107, got {:?}",
        result.diagnostics
    );
}

// ── $pipeline ──────────────────────────────────────────────────────────

#[test]
fn pipeline_var_override_applied() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  pipeline:
    cutoff_date:
      type: date
      default: "2026-01-01"
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
    assert_eq!(
        result.pipeline_vars.get("cutoff_date"),
        Some(&Value::from("2026-01-01")),
    );
}

#[test]
fn pipeline_var_add_applied() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  pipeline:
    extra_pipeline_var:
      type: bool
      default: true
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
    assert_eq!(
        result.pipeline_vars.get("extra_pipeline_var"),
        Some(&Value::Bool(true)),
    );
}

#[test]
fn pipeline_reserved_name_emits_e110() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  pipeline:
    execution_id:
      type: string
      default: "x"
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    let errs = errors_with_code(&result.diagnostics, "E110");
    assert_eq!(errs.len(), 1);
}

// ── $source ────────────────────────────────────────────────────────────

#[test]
fn source_var_override_applied_per_source_name() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  source:
    orders:
      ingest_label:
        type: string
        default: "staging"
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
    let inner = result
        .source_vars
        .get("orders")
        .expect("orders source has overrides");
    assert_eq!(inner.get("ingest_label"), Some(&Value::from("staging")),);
}

#[test]
fn source_var_add_applied_per_source_name() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  source:
    orders:
      added_per_src:
        type: int
        default: 99
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
    let inner = result.source_vars.get("orders").unwrap();
    assert_eq!(inner.get("added_per_src"), Some(&Value::Integer(99)));
}

#[test]
fn source_unknown_name_emits_e111() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  source:
    not_a_source:
      x:
        type: int
        default: 1
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    let errs = errors_with_code(&result.diagnostics, "E111");
    assert_eq!(errs.len(), 1);
}

#[test]
fn source_reserved_name_emits_e110() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  source:
    orders:
      path:
        type: string
        default: "/tmp/x"
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    let errs = errors_with_code(&result.diagnostics, "E110");
    assert_eq!(errs.len(), 1);
}

// ── $record ────────────────────────────────────────────────────────────

#[test]
fn record_var_override_applied() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  record:
    tier:
      type: string
      default: "platinum"
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
    assert_eq!(
        result.record_vars.get("tier"),
        Some(&Value::from("platinum")),
    );
}

#[test]
fn record_var_add_applied() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: ch
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  record:
    region:
      type: string
      default: "west"
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
    assert_eq!(result.record_vars.get("region"), Some(&Value::from("west")),);
}

// ── Cross-cutting ──────────────────────────────────────────────────────

#[test]
fn missing_channel_falls_back_to_declared_defaults() {
    // Without a channel binding, no overlay runs; static_vars/pipeline_vars
    // /source_vars/record_vars come purely from declared defaults via the
    // collect_*_var_defaults helpers (covered by executor seeding tests).
    // Here we assert the apply_channel_overlay shape: without a channel
    // binding, the function isn't called and the result type stays empty
    // by definition. Confirm that an empty channel produces empty maps.
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: empty
  target: ./pipelines/channel_vars_pipeline.yaml
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(result.static_vars.is_empty());
    assert!(result.pipeline_vars.is_empty());
    assert!(result.source_vars.is_empty());
    assert!(result.record_vars.is_empty());
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
}

#[test]
fn multiple_overrides_in_one_channel() {
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: kitchen_sink
  target: ./pipelines/channel_vars_pipeline.yaml
vars:
  static:
    fuzzy_threshold:
      type: float
      default: 0.99
  pipeline:
    cutoff_date:
      type: date
      default: "2030-01-01"
  source:
    orders:
      ingest_label:
        type: string
        default: "yolo"
  record:
    tier:
      type: string
      default: "diamond"
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    assert!(
        result
            .diagnostics
            .iter()
            .all(|d| !matches!(d.severity, Severity::Error))
    );
    assert_eq!(result.static_vars.len(), 1);
    assert_eq!(result.pipeline_vars.len(), 1);
    assert_eq!(result.source_vars.get("orders").unwrap().len(), 1);
    assert_eq!(result.record_vars.len(), 1);
}

#[test]
fn composition_target_with_vars_emits_e109() {
    // Use an existing composition fixture as the target.
    let (config, mut plan) = compile_vars_fixture();
    let b = binding(
        br#"
channel:
  name: comp_with_vars
  target: ./compositions/customer_enrich.comp.yaml
vars:
  static:
    foo:
      type: int
      default: 1
"#,
    );
    let result = apply_channel_overlay(&mut plan, &b, &config);
    let errs = errors_with_code(&result.diagnostics, "E109");
    assert_eq!(errs.len(), 1);
}
