//! Insta snapshot tests for the scoped-variable / state-node
//! diagnostics. Each test compiles a fixture YAML that triggers a
//! specific E-code and snapshots the rendered output, so future
//! changes that shift the wording or span trip the snapshot diff.
//!
//! Diagnostics covered:
//!   * E164 — init-phase state node has a runtime descendant
//!   * E170 — multiple writers for the same scoped variable
//!   * E171 — runtime reader is not a transitive descendant of writer
//!   * E172 — bare `$source.<custom>` read downstream of Merge/Combine
//!   * E173 — composition body reads parent scoped var without opting in
//!   * E174 — composition `_compose.scoped_vars` schema mismatch
//!   * E175 — init-phase reader observes a runtime-only writer

use clinker_core::config::{CompileContext, parse_config};
use clinker_core::error::Diagnostic;
use std::path::PathBuf;

fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn render_diags(diags: &[Diagnostic]) -> String {
    let mut buf = String::new();
    for d in diags {
        buf.push_str(&format!("{}: {}\n", d.code, d.message));
        if let Some(help) = &d.help {
            buf.push_str(&format!("  help: {help}\n"));
        }
    }
    buf
}

fn compile_err_diags(yaml: &str) -> Vec<Diagnostic> {
    let config = parse_config(yaml).expect("parse_config");
    let ctx = CompileContext::default();
    match config.compile(&ctx) {
        Ok(_) => panic!("expected compile error, got success"),
        Err(diags) => diags,
    }
}

fn compile_err_with_ctx(yaml: &str) -> Vec<Diagnostic> {
    let config = parse_config(yaml).expect("parse_config");
    let root = fixture_root();
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    match config.compile(&ctx) {
        Ok(_) => panic!("expected compile error, got success"),
        Err(diags) => diags,
    }
}

#[test]
fn snapshot_e164_init_terminal() {
    let yaml = r#"
pipeline:
  name: e164_init_terminal
  vars:
    pipeline:
      x:
        type: int
        default: 0
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: int }
  - type: transform
    name: init_writer
    input: src
    config:
      declares:
        - { name: x, scope: pipeline, type: int }
      phase: init
      cxl: |
        emit a = a
        emit $pipeline.x = a
  - type: transform
    name: runtime_reader
    input: init_writer
    config:
      cxl: |
        emit a = a
        emit b = $pipeline.x
  - type: output
    name: out
    input: runtime_reader
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let diags = compile_err_diags(yaml);
    insta::assert_snapshot!(render_diags(&diags));
}

#[test]
fn snapshot_e170_multi_writer() {
    let yaml = r#"
pipeline:
  name: e170_multi_writer
  vars:
    pipeline:
      x:
        type: int
        default: 0
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: int }
  - type: transform
    name: w1
    input: src
    config:
      declares:
        - { name: x, scope: pipeline, type: int }
      cxl: |
        emit a = a
        emit $pipeline.x = a
  - type: transform
    name: w2
    input: w1
    config:
      declares:
        - { name: x, scope: pipeline, type: int }
      cxl: |
        emit a = a
        emit $pipeline.x = a + 1
  - type: output
    name: out
    input: w2
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let diags = compile_err_diags(yaml);
    insta::assert_snapshot!(render_diags(&diags));
}

#[test]
fn snapshot_e171_non_descendant_reader() {
    let yaml = r#"
pipeline:
  name: e171_non_descendant
  vars:
    pipeline:
      x:
        type: int
        default: 0
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: int }
  - type: transform
    name: writer
    input: src
    config:
      declares:
        - { name: x, scope: pipeline, type: int }
      cxl: |
        emit a = a
        emit $pipeline.x = a
  - type: transform
    name: sibling_reader
    input: src
    config:
      cxl: |
        emit a = a
        emit b = $pipeline.x
  - type: output
    name: out
    input: sibling_reader
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let diags = compile_err_diags(yaml);
    insta::assert_snapshot!(render_diags(&diags));
}

#[test]
fn snapshot_e172_post_merge_unqualified() {
    let yaml = r#"
pipeline:
  name: e172_post_merge_unqualified
  vars:
    source:
      tag:
        type: string
nodes:
  - type: source
    name: l
    config:
      name: l
      type: csv
      path: l.csv
      schema:
        - { name: id, type: int }
        - { name: t, type: string }
  - type: source
    name: r
    config:
      name: r
      type: csv
      path: r.csv
      schema:
        - { name: id, type: int }
        - { name: t, type: string }
  - type: transform
    name: writer
    input: l
    config:
      declares:
        - { name: tag, scope: source, type: string }
      cxl: |
        emit id = id
        emit t = t
        emit $source.tag = t
  - type: merge
    name: m
    inputs: [writer, r]
  - type: transform
    name: reader
    input: m
    config:
      cxl: |
        emit id = id
        emit t = $source.tag
  - type: output
    name: out
    input: reader
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let diags = compile_err_diags(yaml);
    insta::assert_snapshot!(render_diags(&diags));
}

#[test]
fn snapshot_e173_composition_body_reads_undeclared_parent_var() {
    // Uses the existing fixture composition that reads
    // `$pipeline.cutoff` without declaring it in
    // `_compose.scoped_vars`. The parent declares `cutoff` so the
    // body's read would resolve through the hidden tier — and the
    // body's resolver must emit E173 with the composition-aware help
    // ("declare it in _compose.scoped_vars").
    let yaml = r#"
pipeline:
  name: e173_undeclared_parent_var
  vars:
    pipeline:
      cutoff:
        type: int
        default: 0
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
  - type: composition
    name: body
    input: src
    use: ../compositions/state_uses_undeclared.comp.yaml
    inputs:
      data: src
  - type: output
    name: out
    input: body
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let diags = compile_err_with_ctx(yaml);
    insta::assert_snapshot!(render_diags(&diags));
}

#[test]
fn snapshot_e174_composition_scoped_vars_type_mismatch() {
    let yaml = r#"
pipeline:
  name: e174_type_mismatch
  vars:
    pipeline:
      cutoff:
        type: int
        default: 0
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
  - type: composition
    name: body
    input: src
    use: ../compositions/state_type_mismatch.comp.yaml
    inputs:
      inp: src
  - type: output
    name: out
    input: body
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let diags = compile_err_with_ctx(yaml);
    insta::assert_snapshot!(render_diags(&diags));
}

#[test]
fn snapshot_e175_init_reads_runtime_var() {
    let yaml = r#"
pipeline:
  name: e175_init_reads_runtime
  vars:
    pipeline:
      runtime_v:
        type: int
        default: 0
      init_v:
        type: int
        default: 0
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: a, type: int }
  - type: transform
    name: runtime_w
    input: src
    config:
      declares:
        - { name: runtime_v, scope: pipeline, type: int }
      cxl: |
        emit a = a
        emit $pipeline.runtime_v = a
  - type: source
    name: init_src
    config:
      name: init_src
      type: csv
      path: init.csv
      schema:
        - { name: b, type: int }
  - type: transform
    name: init_w
    input: init_src
    config:
      declares:
        - { name: init_v, scope: pipeline, type: int }
      phase: init
      cxl: |
        emit b = b
        emit $pipeline.init_v = b + $pipeline.runtime_v
  - type: output
    name: out
    input: runtime_w
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let diags = compile_err_diags(yaml);
    insta::assert_snapshot!(render_diags(&diags));
}
