//! Scope-qualified keying of the shared `CompileArtifacts.typed`
//! typed-program table (the transform / aggregate / combine-body surface).
//!
//! Every CXL-bearing node's typed program lands in `artifacts.typed` keyed
//! by [`ScopedNodeId`]. Before scope-qualified keying it was keyed by the
//! bare node name, so a node named the same at top level and inside a
//! composition body overwrote one entry with the other — the surviving
//! program was whichever the bind pass visited last. That fed the executor
//! transform startup table, the `$doc`-attribution walk, and lowering's
//! per-node program stamp from the wrong scope.
//!
//! This test compiles a pipeline where a TOP-LEVEL transform and a BODY
//! transform share the SAME node name (`shape`) but emit DIFFERENT fields,
//! and asserts both scope-keyed entries survive with their own distinct
//! program.

use crate::NodeScope;

use super::deferred_region::compile_with_dir_full;

/// A top-level transform and a composition-body transform both named
/// `shape`, each emitting a distinct field, keep separate `typed` entries
/// under their scope-qualified keys.
#[test]
fn typed_table_scoped_key_does_not_collide_across_scopes() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    // Body composition: a transform named `shape` that emits `body_marker`
    // — distinct from the top-level `shape` below, which emits `top_marker`.
    std::fs::write(
        comp_dir.join("scoped_transform.comp.yaml"),
        r#"_compose:
  name: scoped_transform
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: val, type: int }
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
        emit body_marker = val
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    // Top-level pipeline: a transform ALSO named `shape`, emitting
    // `top_marker`, plus a composition whose body holds the body `shape`.
    let yaml = r#"
pipeline:
  name: typed_scoped
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
  - type: transform
    name: shape
    input: src
    config:
      cxl: |
        emit id = id
        emit top_marker = val
  - type: composition
    name: body
    input: src
    use: ../compositions/scoped_transform.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out_top
    input: shape
    config:
      name: out_top
      type: csv
      path: out_top.csv
      include_unmapped: true
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
    let artifacts = compiled.artifacts();

    // The body's scope id comes from the composition-body assignment.
    let body_id = artifacts
        .composition_body_assignments
        .get("body")
        .copied()
        .expect("composition body assignment for `body`");

    // Both scope-qualified keys resolve — with bare-name keying only one of
    // these would have been populated (last writer wins).
    let top = artifacts
        .typed_get(NodeScope::TopLevel, "shape")
        .expect("top-level `shape` program present under TopLevel scope key");
    let body = artifacts
        .typed_get(NodeScope::Body(body_id), "shape")
        .expect("body `shape` program present under Body scope key");

    let emits = |tp: &cxl::typecheck::TypedProgram, field: &str| -> bool {
        tp.output_row
            .fields()
            .any(|(qf, _)| qf.name.as_ref() == field)
    };

    // Each scope's program carries its OWN emit and not the other's. With
    // bare-name keying the two `shape` nodes shared one table slot, so the
    // last writer's marker would appear under both keys (or one key would be
    // absent entirely, already caught by the `expect`s above).
    assert!(
        emits(top, "top_marker") && !emits(top, "body_marker"),
        "top-level `shape` must carry `top_marker` and not `body_marker`; \
         got output row {:?}",
        top.output_row
    );
    assert!(
        emits(body, "body_marker") && !emits(body, "top_marker"),
        "body `shape` must carry `body_marker` and not `top_marker`; \
         got output row {:?}",
        body.output_row
    );
}
