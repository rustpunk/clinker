//! Composition-body nodes must pass the same node-scoped config checks
//! as top-level nodes (E115).
//!
//! Top-level `validate_config` runs before compile on the call-site
//! pipeline's own nodes only; composition bodies are re-read and parsed
//! during binding on a path that historically skipped those checks. A
//! body Envelope with a wired `trailer:` therefore sailed past the
//! top-level "not yet supported" rejection and reached the executor,
//! where it died as an internal error (the envelope dispatch expects a
//! single body predecessor and finds two). `bind_composition` now runs
//! the shared `validate_node_configs` pass over body nodes and rejects
//! each violation with an E115 diagnostic at compile time.

use std::path::PathBuf;

use crate::config::{CompileContext, PipelineConfig, parse_config};

/// Write a `.comp.yaml` body and a pipeline invoking it, and return the
/// compile-ready pieces. The body's `nodes:` section is caller-supplied
/// so each test exercises a different body-node violation.
fn workspace_with_body(body_nodes: &str, output_node: &str) -> (tempfile::TempDir, PipelineConfig) {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("framed_body.comp.yaml"),
        format!(
            r#"_compose:
  name: framed_body
  inputs:
    inp:
      schema:
        - {{ name: id, type: string }}
        - {{ name: val, type: int }}
  outputs:
    out: {output_node}
  config_schema: {{}}

nodes:
{body_nodes}"#
        ),
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: body_node_config_demo
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: id, type: string }
        - { name: val, type: int }
  - type: composition
    name: body
    input: src
    use: ../compositions/framed_body.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out_body
    input: body
    config:
      name: out_body
      type: csv
      path: out_body.csv
      include_unmapped: true
"#;

    let config: PipelineConfig = parse_config(yaml).expect("parse top-level pipeline");
    (workspace, config)
}

/// A body Envelope wiring the `trailer:` port fails compile with an
/// E115 diagnostic carrying the same "not yet supported" wording the
/// top-level rejection uses — instead of reaching the executor and
/// dying as an internal error.
#[test]
fn body_envelope_wired_trailer_rejected_with_e115() {
    let (workspace, config) = workspace_with_body(
        r#"  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit id = id
        emit val = val
  - type: envelope
    name: framed
    body: shape
    trailer: shape
    config:
      strategy: preserve
"#,
        "framed",
    );

    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let err = config
        .compile(&ctx)
        .expect_err("a body envelope with a wired trailer must fail to compile");

    let diag = err.iter().find(|d| d.code == "E115").unwrap_or_else(|| {
        panic!("expected an E115 diagnostic for the wired body trailer; got: {err:?}")
    });
    assert!(
        diag.message.contains("envelope node 'framed'")
            && diag.message.contains("`trailer`")
            && diag.message.contains("not yet supported"),
        "the E115 message must carry the top-level not-yet-supported trailer wording: {:?}",
        diag.message
    );
    assert!(
        diag.message.contains("framed_body.comp.yaml"),
        "the E115 message must name the offending body file: {:?}",
        diag.message
    );
}

/// A body Envelope wiring only the `header:` port still compiles — a
/// wired header is a real, supported port, so body binding must not
/// reject more than the top level does.
#[test]
fn body_envelope_wired_header_still_compiles() {
    let (workspace, config) = workspace_with_body(
        r#"  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit id = id
        emit val = val
  - type: transform
    name: hdr
    input: inp
    config:
      cxl: |
        emit id = id
  - type: envelope
    name: framed
    body: shape
    header: hdr
    config:
      strategy: preserve
"#,
        "framed",
    );

    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    config
        .compile(&ctx)
        .expect("a body envelope with only a wired header must compile");
}

/// The shared pass covers every node-scoped check, not just the
/// Envelope trailer: a body Transform with `batch_size: 0` is rejected
/// with the same message top-level validation produces.
#[test]
fn body_transform_zero_batch_size_rejected_with_e115() {
    let (workspace, config) = workspace_with_body(
        r#"  - type: transform
    name: shape
    input: inp
    config:
      batch_size: 0
      cxl: |
        emit id = id
        emit val = val
"#,
        "shape",
    );

    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let err = config
        .compile(&ctx)
        .expect_err("a body transform with batch_size 0 must fail to compile");

    let diag = err.iter().find(|d| d.code == "E115").unwrap_or_else(|| {
        panic!("expected an E115 diagnostic for the zero body batch_size; got: {err:?}")
    });
    assert!(
        diag.message
            .contains("transform 'shape': batch_size must be >= 1"),
        "the E115 message must carry the top-level batch_size wording: {:?}",
        diag.message
    );
}
