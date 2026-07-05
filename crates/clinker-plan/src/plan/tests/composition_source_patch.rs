//! Channel `sources:` patches addressed at a source declared inside a
//! composition body via a qualified `<composition>.<source>` key.
//!
//! A body-declared source is not in `config.nodes` at patch time (the body is
//! expanded only during compile), so `apply_source_patches` validates the
//! composition alias and defers the patch; `bind_composition` applies it to the
//! freshly re-read body before the body binds. These tests drive the full
//! parse -> apply_source_patches -> compile flow and observe the patch at the
//! level a body source is observable: its schema seeds the body's binding, so a
//! patch that fixes the body source's schema makes an otherwise-failing body
//! bind. (A body source binds but does not run today; a data run through a body
//! source awaits body-source runtime support.)

use std::path::PathBuf;

use indexmap::IndexMap;

use crate::config::{CompileContext, PipelineConfig, SourceConfigPatch, apply_source_patches};

/// Build a workspace with a composition body that declares its own source
/// `ref` and a transform reading it (`emit label = code`), plus the invoking
/// pipeline whose composition node is named `enrich`. `ref_schema` is the body
/// source's declared schema block so a test can start it wrong and let a patch
/// fix it. Returns the temp workspace and the parsed pipeline config.
fn workspace_with_body_source(ref_schema: &str) -> (tempfile::TempDir, PipelineConfig) {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("with_ref.comp.yaml"),
        format!(
            r#"_compose:
  name: with_ref
  inputs:
    driver:
      schema:
        - {{ name: x, type: int }}
  outputs:
    out: shape
  config_schema: {{}}

nodes:
  - type: source
    name: ref
    config:
      name: ref
      type: csv
      path: ref.csv
      schema:
{ref_schema}
  - type: transform
    name: shape
    input: ref
    config:
      cxl: |
        emit label = code
"#
        ),
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: composition_source_patch_demo
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
    use: ../compositions/with_ref.comp.yaml
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
    let config: PipelineConfig = crate::config::parse_config(yaml).expect("parse pipeline");
    (workspace, config)
}

fn patch_from_yaml(yaml: &str) -> SourceConfigPatch {
    crate::yaml::from_str(yaml).expect("parse patch")
}

fn qualified(key: &str, patch_yaml: &str) -> IndexMap<String, SourceConfigPatch> {
    let mut patches = IndexMap::new();
    patches.insert(key.to_string(), patch_from_yaml(patch_yaml));
    patches
}

/// A channel `sources:` patch qualified `enrich.ref` reaches the source `ref`
/// declared inside composition `enrich`'s body and applies before the body
/// binds: the body fails to bind without it (the transform reads `code`, which
/// the base source schema lacks) and binds with it (the patch renames the
/// column to `code`).
#[test]
fn body_source_patch_reaches_and_fixes_binding() {
    // Base: source `ref` declares `raw`; the body transform `emit label = code`
    // references a column the body source does not carry, so the body fails.
    let (base_ws, base_config) =
        workspace_with_body_source("        - { name: raw, type: string }");
    let base_ctx = CompileContext::with_pipeline_dir(base_ws.path(), PathBuf::from("pipelines"));
    assert!(
        base_config.compile(&base_ctx).is_err(),
        "the body must fail to bind before the patch renames the source column"
    );

    // Patched: rename the body source column `raw` -> `code`, so the transform
    // resolves and the body binds.
    let (ws, mut config) = workspace_with_body_source("        - { name: raw, type: string }");
    apply_source_patches(
        &mut config,
        &qualified("enrich.ref", "schema:\n  raw: { rename: code }\n"),
    )
    .expect("apply defers the qualified patch");
    assert!(
        config
            .body_source_patches
            .get("enrich")
            .is_some_and(|m| m.contains_key("ref")),
        "the qualified patch is deferred under the composition node name"
    );
    let ctx = CompileContext::with_pipeline_dir(ws.path(), PathBuf::from("pipelines"));
    config
        .compile(&ctx)
        .unwrap_or_else(|d| panic!("the patched body source must let the body bind: {d:?}"));
}

/// A qualified key whose composition exists but whose inner source name does
/// not fails compile with a spanned E230 that names the missing source, the
/// composition, and the body file — instead of silently ignoring the patch.
#[test]
fn unknown_inner_source_fails_at_bind_with_e230() {
    let (ws, mut config) = workspace_with_body_source("        - { name: code, type: string }");
    apply_source_patches(
        &mut config,
        &qualified("enrich.nonexistent", "schema:\n  code: remove\n"),
    )
    .expect("apply defers (enrich is a composition; inner name is checked at bind)");
    let ctx = CompileContext::with_pipeline_dir(ws.path(), PathBuf::from("pipelines"));
    let diags = config
        .compile(&ctx)
        .expect_err("an unknown inner source must fail compile");
    let e230 = diags
        .iter()
        .find(|d| d.code == "E230")
        .unwrap_or_else(|| panic!("expected E230 for the unknown body source; got: {diags:?}"));
    assert!(
        e230.message.contains("nonexistent") && e230.message.contains("with_ref.comp.yaml"),
        "E230 must name the missing source and the body file: {}",
        e230.message
    );
}

/// Build a workspace whose pipeline has a top-level composition `enrich` (body
/// source `ref`, patch target) alongside a top-level composition `wrap` whose
/// body contains a *nested* composition node that also happens to be named
/// `enrich` but resolves to a different body with no source `ref`. Exercises the
/// name-collision path: a patch keyed `enrich.ref` must reach only the top-level
/// `enrich` body, never the same-named nested node inside `wrap`'s body.
fn workspace_with_shadowed_nested_enrich() -> (tempfile::TempDir, PipelineConfig) {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");

    // Top-level `enrich` target: body source `ref` (schema `raw`) feeds a
    // transform that reads `code`, so the body binds only once a patch renames
    // `raw` -> `code`.
    std::fs::write(
        comp_dir.join("with_ref.comp.yaml"),
        r#"_compose:
  name: with_ref
  inputs:
    driver:
      schema:
        - { name: x, type: int }
  outputs:
    out: shape
  config_schema: {}

nodes:
  - type: source
    name: ref
    config:
      name: ref
      type: csv
      path: ref.csv
      schema:
        - { name: raw, type: string }
  - type: transform
    name: shape
    input: ref
    config:
      cxl: |
        emit label = code
"#,
    )
    .expect("write with_ref");

    // `wrap`'s body holds a nested composition node named `enrich` that resolves
    // to `passthru.comp.yaml` — a body with NO source `ref`. If the deferred
    // `enrich.ref` patch leaked into this nested node it would fail to find `ref`
    // and abort the compile with E230.
    std::fs::write(
        comp_dir.join("wrap.comp.yaml"),
        r#"_compose:
  name: wrap
  inputs:
    driver:
      schema:
        - { name: x, type: int }
  outputs:
    out: wrap_tag
  config_schema: {}

nodes:
  - type: composition
    name: enrich
    input: driver
    use: ./passthru.comp.yaml
    inputs:
      driver: driver
  - type: transform
    name: wrap_tag
    input: enrich
    config:
      cxl: |
        emit y2 = y
"#,
    )
    .expect("write wrap");

    std::fs::write(
        comp_dir.join("passthru.comp.yaml"),
        r#"_compose:
  name: passthru
  inputs:
    driver:
      schema:
        - { name: x, type: int }
  outputs:
    out: proj
  config_schema: {}

nodes:
  - type: transform
    name: proj
    input: driver
    config:
      cxl: |
        emit y = x
"#,
    )
    .expect("write passthru");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: shadowed_nested_enrich_demo
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
    use: ../compositions/with_ref.comp.yaml
    inputs:
      driver: drv
  - type: composition
    name: wrap
    input: drv
    use: ../compositions/wrap.comp.yaml
    inputs:
      driver: drv
  - type: output
    name: out_enrich
    input: enrich
    config:
      name: out_enrich
      type: csv
      path: out_enrich.csv
  - type: output
    name: out_wrap
    input: wrap
    config:
      name: out_wrap
      type: csv
      path: out_wrap.csv
"#;
    let config: PipelineConfig = crate::config::parse_config(yaml).expect("parse pipeline");
    (workspace, config)
}

/// A patch keyed `enrich.ref` must reach only the top-level composition `enrich`,
/// not a same-named composition node nested inside another body. Without the
/// top-level gate, `bind_composition` matches the patch on the nested `enrich`
/// (bound at depth >= 1) and applies it to a body that declares no source `ref`,
/// aborting the compile with a spurious E230. With the gate, only the top-level
/// `enrich` body is patched and the whole pipeline compiles.
#[test]
fn body_source_patch_does_not_leak_into_shadowed_nested_composition() {
    let (ws, mut config) = workspace_with_shadowed_nested_enrich();
    apply_source_patches(
        &mut config,
        &qualified("enrich.ref", "schema:\n  raw: { rename: code }\n"),
    )
    .expect("apply defers the qualified patch");
    let ctx = CompileContext::with_pipeline_dir(ws.path(), PathBuf::from("pipelines"));
    config.compile(&ctx).unwrap_or_else(|d| {
        panic!(
            "the `enrich.ref` patch must target only the top-level `enrich` body \
             and leave the same-named nested composition untouched: {d:?}"
        )
    });
}

/// A schema op that is ill-formed against the body source (removing a column
/// that does not exist) fails compile with the same stable op code (E231) a
/// top-level source patch would produce, re-anchored to the composition.
#[test]
fn body_source_patch_op_error_carries_stable_code() {
    let (ws, mut config) = workspace_with_body_source("        - { name: code, type: string }");
    apply_source_patches(
        &mut config,
        &qualified("enrich.ref", "schema:\n  missing: remove\n"),
    )
    .expect("apply defers");
    let ctx = CompileContext::with_pipeline_dir(ws.path(), PathBuf::from("pipelines"));
    let diags = config
        .compile(&ctx)
        .expect_err("removing an unknown body-source column must fail compile");
    assert!(
        diags.iter().any(|d| d.code == "E231"),
        "expected the E231 unknown-column code, got: {diags:?}"
    );
}
