//! Per-node-id keying of the shared `CompileArtifacts.typed`
//! typed-program table (the transform / aggregate / combine-body surface).
//!
//! Every CXL-bearing node's typed program lands in `artifacts.typed` keyed
//! by its [`PlanNodeId`]. Before id keying it was keyed by the bare node
//! name, so a node named the same at top level and inside a composition
//! body overwrote one entry with the other — the surviving program was
//! whichever the bind pass visited last. That fed the executor transform
//! startup table, the `$doc`-attribution walk, and lowering's per-node
//! program stamp from the wrong scope.
//!
//! This test compiles a pipeline where a TOP-LEVEL transform and a BODY
//! transform share the SAME node name (`shape`) but emit DIFFERENT fields,
//! and asserts the two nodes get DISTINCT `PlanNodeId`s with their own
//! independent typed entry.

use super::deferred_region::bind_schema_with_dir;

/// A top-level transform and a composition-body transform both named
/// `shape`, each emitting a distinct field, get distinct `PlanNodeId`s and
/// keep separate `typed` entries under those ids.
#[test]
fn typed_table_node_id_does_not_collide_across_scopes() {
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
    let (artifacts, config) = bind_schema_with_dir(yaml, workspace.path());

    // The body's scope id comes from the composition-body assignment,
    // keyed by the `body` composition call-site node's own id — resolved
    // through the production `top_level_id` resolver.
    let body_comp_id = artifacts
        .top_level_id(&config.nodes, "body")
        .expect("top-level `body` composition id");
    let body_id = artifacts
        .composition_body_assignments
        .get(&body_comp_id)
        .copied()
        .expect("composition body assignment for `body`");

    // Resolve the top-level `shape` node's id (top-level names are unique),
    // and the body `shape` node's id off the bound body's mini-DAG. The two
    // same-named nodes live in disjoint scopes and so must get DISTINCT ids.
    let top_id = artifacts
        .top_level_id(&config.nodes, "shape")
        .expect("top-level `shape` id");
    let body = artifacts.body_of(body_id).expect("bound body for `body`");
    let body_idx = body
        .name_to_idx
        .get("shape")
        .copied()
        .expect("body `shape` node index");
    let body_id_for_shape = body.graph[body_idx].id();

    assert_ne!(
        top_id, body_id_for_shape,
        "a top-level node and a body node sharing the name `shape` must mint \
         distinct PlanNodeIds; got top {top_id} and body {body_id_for_shape}"
    );

    // Both id-keyed entries resolve — with bare-name keying only one of
    // these would have been populated (last writer wins).
    let top = artifacts
        .typed_get(top_id)
        .expect("top-level `shape` program present under its id");
    let body_prog = artifacts
        .typed_get(body_id_for_shape)
        .expect("body `shape` program present under its id");

    let emits = |tp: &cxl::typecheck::TypedProgram, field: &str| -> bool {
        tp.output_row
            .fields()
            .any(|(qf, _)| qf.name.as_ref() == field)
    };

    // Each id's program carries its OWN emit and not the other's. With
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
        emits(body_prog, "body_marker") && !emits(body_prog, "top_marker"),
        "body `shape` must carry `body_marker` and not `top_marker`; \
         got output row {:?}",
        body_prog.output_row
    );
}
