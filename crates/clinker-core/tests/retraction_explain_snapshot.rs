//! Snapshot capture for the `--explain` retraction surface.
//!
//! Locks in the per-aggregate / per-window block shape that operators
//! authoring relaxed-CK pipelines rely on for capacity sizing. The
//! committed snapshot is the canonical text; any change to the explain
//! body that flows through here is reviewed line-by-line in the
//! `cargo insta review` diff before being accepted into the tree.

use clinker_core::config::{CompileContext, PipelineConfig, parse_config};

fn render_explain(yaml: &str) -> String {
    let config: PipelineConfig = parse_config(yaml).expect("parse_config");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let dag = plan.dag();
    let artifacts = plan.artifacts();
    dag.explain_text_with_artifacts(&config, artifacts)
}

/// Mixed Reversible + BufferRequired bindings in one relaxed Aggregate
/// plus an upstream analytic-window Transform whose `partition_by`
/// drops the pipeline-level CK, so the planner flips the window into
/// buffer-recompute mode. The fixture exercises every retraction
/// surface: top-of-explain summary, per-aggregate path + lineage
/// memory, per-window buffer cost.
#[test]
fn explain_renders_retraction_section_for_relaxed_pipeline() {
    let yaml = r#"
pipeline:
  name: retract_explain_demo
error_handling:
  strategy: continue
  correlation_key: order_id
  correlation_fanout_policy: any
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: int }
- type: transform
  name: windowed
  input: orders
  config:
    cxl: 'emit order_id = order_id

      emit department = department

      emit amount = amount

      emit running_total = $window.sum(amount)

      '
    analytic_window:
      group_by: [department]
- type: aggregate
  name: dept_totals
  input: windowed
  config:
    group_by: [department]
    relaxed_correlation_key: true
    cxl: 'emit department = department

      emit total = sum(amount)

      emit lo = min(amount)

      emit n = count(*)

      '
- type: output
  name: out
  input: dept_totals
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let text = render_explain(yaml);
    insta::assert_snapshot!("explain_relaxed_aggregate_buffer_mode", text);
}

/// Strict pipeline: identical-looking text otherwise, but the
/// `=== Retraction ===` block must be absent so non-relaxed pipelines
/// pay zero plan-time observability noise.
#[test]
fn explain_omits_retraction_section_when_no_relaxed_aggregate() {
    let yaml = r#"
pipeline:
  name: strict_explain
error_handling:
  strategy: continue
  correlation_key: order_id
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: orders
  config:
    group_by: [order_id, department]
    cxl: 'emit department = department

      emit total = sum(amount)

      '
- type: output
  name: out
  input: dept_totals
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    let text = render_explain(yaml);
    assert!(
        !text.contains("=== Retraction ==="),
        "strict pipeline must not emit retraction section, got:\n{text}"
    );
    assert!(
        !text.contains("retraction enabled"),
        "strict pipeline must not emit retraction summary, got:\n{text}"
    );
}
