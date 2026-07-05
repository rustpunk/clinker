//! Composition-binding errors (E102–E109) are fatal at compile.
//!
//! A composition whose `use:` cannot be resolved — or whose call site is
//! otherwise ill-bound — drops the composition node from the lowered DAG.
//! Compile used to still succeed there, so a run wrote zero records to the
//! downstream output with no diagnostic: a silent-empty-output failure the
//! caller could not tell apart from an empty input. `compile` now returns
//! the binding diagnostic instead, so the pipeline fails loudly. A body
//! that binds cleanly is unaffected and still compiles.

use std::path::PathBuf;

use crate::config::{CompileContext, PipelineConfig, parse_config};

/// Build a workspace with one composition body and a pipeline whose
/// single composition node invokes `use_path`. The output reads from the
/// composition, so an omitted composition node is what produces the
/// silent-empty-output symptom this gate closes.
fn workspace_with_use(use_path: &str) -> (tempfile::TempDir, PipelineConfig) {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("passthrough.comp.yaml"),
        r#"_compose:
  name: passthrough
  inputs:
    inp:
      schema:
        - { name: id, type: string }
  outputs:
    out: shape
  config_schema: {}

nodes:
  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit id = id
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = format!(
        r#"
pipeline:
  name: composition_binding_fatal_demo
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - {{ name: id, type: string }}
  - type: composition
    name: body
    input: src
    use: {use_path}
    inputs:
      inp: src
  - type: output
    name: out_body
    input: body
    config:
      name: out_body
      type: csv
      path: out_body.csv
"#
    );

    let config: PipelineConfig = parse_config(&yaml).expect("parse top-level pipeline");
    (workspace, config)
}

/// An unresolvable `use:` path fails compile with a spanned E103 instead
/// of silently omitting the composition and letting the run write nothing.
#[test]
fn unresolvable_use_path_fails_compile_with_e103() {
    let (workspace, config) = workspace_with_use("../compositions/nonexistent.comp.yaml");

    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let err = config
        .compile(&ctx)
        .expect_err("an unresolvable composition `use:` must fail to compile");

    let diag = err.iter().find(|d| d.code == "E103").unwrap_or_else(|| {
        panic!("expected an E103 diagnostic for the unresolvable `use:` path; got: {err:?}")
    });
    assert!(
        diag.message.contains("body"),
        "the E103 message must name the offending composition node: {:?}",
        diag.message
    );
}

/// A composition whose `use:` resolves to a body that binds cleanly still
/// compiles — the fatal gate rejects only error-severity diagnostics.
#[test]
fn resolvable_use_path_still_compiles() {
    let (workspace, config) = workspace_with_use("../compositions/passthrough.comp.yaml");

    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    config
        .compile(&ctx)
        .expect("a composition whose body binds cleanly must compile");
}
