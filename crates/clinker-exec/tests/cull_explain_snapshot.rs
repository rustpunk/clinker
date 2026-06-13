//! Snapshot capture for the Cull node's `--explain` surface.
//!
//! Locks in the two-port topology a Cull renders: the main output port and
//! the `removed_to` side-output port, plus the operator's blocking/spill
//! arbitration annotation. The committed snapshot is the canonical text; any
//! change to the explain body that flows through here is reviewed line-by-line
//! in the `cargo insta review` diff before being accepted into the tree.

use clinker_plan::config::{CompileContext, PipelineConfig, parse_config};

fn render_explain(yaml: &str) -> String {
    let config: PipelineConfig = parse_config(yaml).expect("parse_config");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let dag = plan.dag();
    let artifacts = plan.artifacts();
    dag.explain_text_with_artifacts(&config, artifacts)
}

/// A source → cull → (main output + audit side output) pipeline. The Cull
/// removes any account group containing an `error` row, routing the whole
/// group to the `removed` side output. `--explain` must render both output
/// ports so the two-output topology is visible.
#[test]
fn explain_renders_cull_two_output_ports() {
    let yaml = r#"
pipeline:
  name: cull_explain_demo
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      schema:
        - { name: account, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: cull
    name: drop_bad
    input: events
    config:
      partition_by: [account]
      removed_to: removed
      rules:
        - name: drop_error_groups
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
  - type: output
    name: kept
    input: drop_bad
    config:
      name: kept
      type: csv
      path: kept.csv
  - type: output
    name: audit
    input: drop_bad.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;
    let text = render_explain(yaml);

    // Sanity asserts on the two-port topology, independent of the snapshot:
    // both the main and `removed` ports must appear, and the Cull's blocking
    // spill annotation must be present.
    assert!(
        text.contains("FORK [cull] 'drop_bad'"),
        "Cull must render as a two-port fork: {text}"
    );
    assert!(
        text.contains("main → drop_bad"),
        "the main output port must be rendered: {text}"
    );
    assert!(
        text.contains("removed → drop_bad.removed"),
        "the removed side-output port must be rendered: {text}"
    );

    insta::assert_snapshot!("explain_cull_two_output_ports", text);
}
