//! Compile-time constant folding of `$config.<param>` in composition bodies
//! (issue #765).
//!
//! `$config.<param>` reads in a composition body are folded to the
//! instantiation's resolved config value after typecheck, so two call sites of
//! the same composition with different `config:` compile to different bodies,
//! and an undeclared `$config.<param>` is a compile error.

use std::path::PathBuf;

use super::deferred_region::bind_schema_with_dir;
use crate::config::{CompileContext, parse_config};

const GATE_COMP: &str = r#"_compose:
  name: gate
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: val, type: float }
  outputs:
    out: gated
  config_schema:
    min_amount:
      type: float
      default: 0.0
nodes:
  - type: transform
    name: gated
    input: inp
    config:
      cxl: |
        filter val >= $config.min_amount
        emit id = id
        emit val = val
"#;

fn write_gate(workspace: &std::path::Path) {
    let comp_dir = workspace.join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(comp_dir.join("gate.comp.yaml"), GATE_COMP).expect("write comp");
    std::fs::create_dir_all(workspace.join("pipelines")).expect("mkdir pipelines");
}

/// The folded `filter` literal of a composition instantiation's `gated` body
/// transform, as a debug string of the typed program.
fn body_program_debug(
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
    config: &crate::config::PipelineConfig,
    comp_node: &str,
) -> String {
    let comp_id = artifacts
        .top_level_id(&config.nodes, comp_node)
        .unwrap_or_else(|| panic!("composition id for {comp_node:?}"));
    let body_id = artifacts
        .composition_body_assignments
        .get(&comp_id)
        .copied()
        .unwrap_or_else(|| panic!("body assignment for {comp_node:?}"));
    let body = artifacts
        .body_of(body_id)
        .unwrap_or_else(|| panic!("bound body for {comp_node:?}"));
    let idx = body
        .name_to_idx
        .get("gated")
        .copied()
        .expect("body `gated` node index");
    let node_id = body.graph[idx].id();
    let prog = artifacts
        .typed_get(node_id)
        .expect("body `gated` typed program");
    format!("{:?}", prog.program)
}

#[test]
fn two_instantiations_fold_to_different_bodies() {
    let workspace = tempfile::tempdir().expect("tempdir");
    write_gate(workspace.path());

    // The same composition used twice with different call-site `config:`.
    let yaml = r#"
pipeline:
  name: two_gates
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: id, type: string }
        - { name: val, type: float }
  - type: composition
    name: gate_low
    input: src
    use: ../compositions/gate.comp.yaml
    inputs:
      inp: src
    config:
      min_amount: 10.0
  - type: composition
    name: gate_high
    input: src
    use: ../compositions/gate.comp.yaml
    inputs:
      inp: src
    config:
      min_amount: 100.0
  - type: output
    name: out_low
    input: gate_low
    config:
      name: out_low
      type: csv
      path: out_low.csv
  - type: output
    name: out_high
    input: gate_high
    config:
      name: out_high
      type: csv
      path: out_high.csv
"#;
    let (artifacts, config) = bind_schema_with_dir(yaml, workspace.path());

    let low = body_program_debug(&artifacts, &config, "gate_low");
    let high = body_program_debug(&artifacts, &config, "gate_high");

    // No `$config` survives — it is folded to a literal before storage.
    assert!(
        !low.contains("ConfigAccess") && !high.contains("ConfigAccess"),
        "no ConfigAccess may remain after folding.\nlow: {low}\nhigh: {high}"
    );
    // Each instantiation baked in its own resolved threshold.
    assert!(
        low.contains("Float(10.0)"),
        "gate_low must fold $config.min_amount to 10.0.\nlow: {low}"
    );
    assert!(
        high.contains("Float(100.0)"),
        "gate_high must fold $config.min_amount to 100.0.\nhigh: {high}"
    );
    assert_ne!(
        low, high,
        "two instantiations with different config must compile to different bodies"
    );
}

#[test]
fn unknown_config_param_is_a_compile_error() {
    let workspace = tempfile::tempdir().expect("tempdir");
    // A gate whose body reads a param NOT in its config schema.
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("gate.comp.yaml"),
        r#"_compose:
  name: gate
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: val, type: float }
  outputs:
    out: gated
  config_schema:
    min_amount:
      type: float
      default: 0.0
nodes:
  - type: transform
    name: gated
    input: inp
    config:
      cxl: |
        filter val >= $config.cutoff
        emit id = id
"#,
    )
    .expect("write comp");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: bad_config
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: id, type: string }
        - { name: val, type: float }
  - type: composition
    name: gate
    input: src
    use: ../compositions/gate.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: gate
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let result = config.compile(&ctx);
    let diags = result.expect_err("an undeclared $config param must fail compilation");
    assert!(
        diags.iter().any(|d| d.message.contains("$config.cutoff")),
        "compile error must name the unknown $config param: {:?}",
        diags.iter().map(|d| &d.message).collect::<Vec<_>>()
    );
}
