//! Duplicate node names inside a composition body must be rejected (E001).
//!
//! The top-level E001 duplicate-name pass never sees composition body
//! files, so without a body-local check two body nodes sharing a name would
//! collapse to ONE `PlanNodeId` at the `bind_composition` pre-mint
//! (`HashMap::entry().or_insert_with`) — tripping the `rebuild_id_index`
//! coverage assert in debug builds and silently aliasing the two nodes'
//! compiled programs in release. `bind_composition` detects the duplicate
//! before minting and emits a clear bind-time diagnostic instead.

use std::path::PathBuf;

use crate::config::{CompileContext, PipelineConfig, parse_config};

/// A composition body that declares two transforms both named `dup`
/// fails to compile with an E001 diagnostic — and does not panic on the
/// `rebuild_id_index` coverage assert.
#[test]
fn duplicate_body_node_name_aborts_with_e001() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    // Body composition with TWO transforms named `dup`. Under the old
    // bare-name pre-mint these two would share a single `PlanNodeId`.
    std::fs::write(
        comp_dir.join("dup_body.comp.yaml"),
        r#"_compose:
  name: dup_body
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: val, type: int }
  outputs:
    out: dup
  config_schema: {}

nodes:
  - type: transform
    name: dup
    input: inp
    config:
      cxl: |
        emit id = id
        emit a = val
  - type: transform
    name: dup
    input: dup
    config:
      cxl: |
        emit id = id
        emit b = a
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: dup_body_demo
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      correlation_key: id
      schema:
        - { name: id, type: string }
        - { name: val, type: int }
  - type: composition
    name: body
    input: src
    use: ../compositions/dup_body.comp.yaml
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

    let config: PipelineConfig = parse_config(yaml).expect("parse");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    // The compile must fail loudly with E001 rather than panic on the
    // `rebuild_id_index` coverage assert or silently alias the two nodes.
    let err = config
        .compile(&ctx)
        .expect_err("a composition body with duplicate node names must fail to compile");

    let dup_diag = err.iter().find(|d| d.code == "E001").unwrap_or_else(|| {
        panic!("expected an E001 diagnostic for the duplicate body node; got: {err:?}")
    });
    assert!(
        dup_diag.message.contains("body") && dup_diag.message.contains("dup"),
        "the E001 message must name the composition body and the duplicated name: {:?}",
        dup_diag.message
    );
}
