//! Composition-body nodes must pass the same node-scoped config checks
//! and external-schema resolution as top-level nodes.
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
//!
//! Two more parity gaps close on the same body walk: a body Source/Output
//! CSV `delimiter` / `quote_char` is validated with the same one-ASCII-byte
//! rule top-level nodes get (so a bad byte fails at compile, not first at
//! run), and a body Source/Output external `.schema.yaml` reference is
//! resolved to its inline columns relative to the composition file's own
//! directory (so a body Output's declared-decimal `scale` reaches the write
//! boundary and a body Source's file schema resolves from the right base).

use std::path::PathBuf;

use crate::config::{CompileContext, PipelineConfig, parse_config};
use crate::plan::execution::PlanNode;

use super::deferred_region::{body_id_for, compile_with_dir_full};

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

/// A body Output whose CSV `delimiter` is not a single ASCII byte is rejected
/// at compile with an E115 carrying the same "one ASCII byte" wording the
/// top-level output check produces — instead of first failing when the writer
/// is constructed mid-run.
#[test]
fn body_output_bad_csv_delimiter_rejected_at_compile() {
    let (workspace, config) = workspace_with_body(
        r#"  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit id = id
        emit val = val
  - type: output
    name: body_sink
    input: shape
    config:
      name: body_sink
      type: csv
      path: body_sink.csv
      options:
        delimiter: "||"
"#,
        "shape",
    );

    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let err = config
        .compile(&ctx)
        .expect_err("a body output with a multi-character delimiter must fail to compile");

    let diag = err
        .iter()
        .find(|d| d.code == "E115" && d.message.contains("one ASCII byte"))
        .unwrap_or_else(|| {
            panic!(
                "expected an E115 'one ASCII byte' diagnostic for the body delimiter; got: {err:?}"
            )
        });
    assert!(
        diag.message.contains("output 'body_sink'"),
        "the diagnostic must name the offending body output: {:?}",
        diag.message
    );
    assert!(
        diag.message.contains("framed_body.comp.yaml"),
        "the diagnostic must name the body file: {:?}",
        diag.message
    );
}

/// A body Output with a valid single-byte delimiter compiles — the new body
/// check must not reject more than the top level does, and body Output nodes
/// remain a supported shape.
#[test]
fn body_output_valid_csv_delimiter_compiles() {
    let (workspace, config) = workspace_with_body(
        r#"  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit id = id
        emit val = val
  - type: output
    name: body_sink
    input: shape
    config:
      name: body_sink
      type: csv
      path: body_sink.csv
      options:
        delimiter: "|"
"#,
        "shape",
    );

    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    config
        .compile(&ctx)
        .expect("a body output with a single-byte delimiter must compile");
}

/// The body CSV check covers the source side too: a body Source `quote_char`
/// that is not a single ASCII byte is rejected at compile, mirroring the
/// top-level source `quote_char` validation.
#[test]
fn body_source_bad_csv_quote_char_rejected_at_compile() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("src_body.comp.yaml"),
        r#"_compose:
  name: src_body
  inputs:
    driver:
      schema:
        - { name: x, type: int }
  outputs:
    out: shape
  config_schema: {}

nodes:
  - type: source
    name: body_src
    config:
      name: body_src
      type: csv
      path: body_src.csv
      schema:
        - { name: code, type: string }
      options:
        quote_char: "→"
  - type: transform
    name: shape
    input: body_src
    config:
      cxl: |
        emit label = code
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: body_source_quote_demo
nodes:
  - type: source
    name: drv
    config:
      name: drv
      type: csv
      path: drv.csv
      schema:
        - { name: x, type: int }
  - type: composition
    name: enrich
    input: drv
    use: ../compositions/src_body.comp.yaml
    inputs:
      driver: drv
  - type: output
    name: out
    input: enrich
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config: PipelineConfig = parse_config(yaml).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let err = config
        .compile(&ctx)
        .expect_err("a body source with a non-ASCII quote_char must fail to compile");

    assert!(
        err.iter().any(|d| d.code == "E115"
            && d.message.contains("source 'body_src'")
            && d.message.contains("quote_char")
            && d.message.contains("one ASCII byte")),
        "expected an E115 naming the body source, quote_char, and the one-byte rule; got: {err:?}"
    );
}

/// A body Output whose `schema:` names an external `.schema.yaml` with a
/// scaled `decimal` column resolves to inline columns at bind time. The
/// resolved schema yields `as_columns() == Some`, which is the exact
/// condition that arms the write boundary's decimal-scale rounding — so a
/// body Output now rounds declared decimals like a top-level Output. The path
/// resolves relative to the composition file's directory, not the pipeline's.
#[test]
fn body_output_external_decimal_schema_resolves_to_inline() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    // External schema lives beside the composition file; the body Output
    // references it by a comp-relative path.
    std::fs::write(
        comp_dir.join("amounts.schema.yaml"),
        "- { name: id, type: string }\n- { name: amount, type: decimal, scale: 2 }\n",
    )
    .expect("write schema");
    std::fs::write(
        comp_dir.join("decimal_body.comp.yaml"),
        r#"_compose:
  name: decimal_body
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: amount, type: decimal }
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
        emit amount = amount
  - type: output
    name: body_sink
    input: shape
    config:
      name: body_sink
      type: csv
      path: body_sink.csv
      schema: amounts.schema.yaml
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: body_decimal_demo
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: decimal }
  - type: composition
    name: body
    input: src
    use: ../compositions/decimal_body.comp.yaml
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
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let body_id = body_id_for(&compiled, "body");
    let bound = compiled.body_of(body_id).expect("bound body");
    let idx = bound
        .graph
        .node_indices()
        .find(|&i| bound.graph[i].name() == "body_sink")
        .expect("body output `body_sink` present in the bound body");
    let PlanNode::Output { resolved, .. } = &bound.graph[idx] else {
        panic!("node `body_sink` is not an Output");
    };
    let payload = resolved
        .as_ref()
        .expect("the full plan carries the body Output's resolved payload");
    let columns = payload
        .output
        .schema
        .as_ref()
        .and_then(|s| s.as_columns())
        .expect("the external body-output schema resolved to inline columns");
    let amount = columns
        .iter()
        .find(|c| c.name.as_str() == "amount")
        .expect("resolved schema carries the `amount` column");
    assert_eq!(
        amount.scale,
        Some(2),
        "the resolved decimal column keeps its declared scale, arming write-boundary rounding"
    );
    assert!(
        matches!(amount.ty.unwrap_nullable(), cxl::typecheck::Type::Decimal),
        "the resolved column is a decimal, got {:?}",
        amount.ty
    );
}

/// A body Source whose `schema:` names an external `.schema.yaml` resolves
/// that path relative to the composition file's directory. The referenced
/// schema declares the `code` column the body transform reads, so resolving
/// from the correct base lets the body bind; resolving from the process
/// working directory (the pre-fix behavior for body sources) would fail to
/// find the file and abort the body.
#[test]
fn body_source_external_schema_resolves_relative_to_comp_dir() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("ref.schema.yaml"),
        "- { name: code, type: string }\n",
    )
    .expect("write schema");
    std::fs::write(
        comp_dir.join("src_ref_body.comp.yaml"),
        r#"_compose:
  name: src_ref_body
  inputs:
    driver:
      schema:
        - { name: x, type: int }
  outputs:
    out: shape
  config_schema: {}

nodes:
  - type: source
    name: body_src
    config:
      name: body_src
      type: csv
      path: body_src.csv
      schema: ref.schema.yaml
  - type: transform
    name: shape
    input: body_src
    config:
      cxl: |
        emit label = code
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: body_source_schema_demo
nodes:
  - type: source
    name: drv
    config:
      name: drv
      type: csv
      path: drv.csv
      schema:
        - { name: x, type: int }
  - type: composition
    name: enrich
    input: drv
    use: ../compositions/src_ref_body.comp.yaml
    inputs:
      driver: drv
  - type: output
    name: out
    input: enrich
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config: PipelineConfig = parse_config(yaml).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    config
        .compile(&ctx)
        .expect("the body source's comp-relative external schema must resolve and the body bind");
}
