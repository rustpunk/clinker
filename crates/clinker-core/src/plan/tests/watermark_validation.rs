//! Plan-time validation for `SourceConfig.watermark`.
//!
//! Two negative paths surface as compile-time diagnostics:
//! - E154 — the named watermark column is not present in the source's
//!   declared `schema:` block.
//! - E155 — the named watermark column exists but its declared CXL
//!   type is not `date_time` or `date` (not event-time-coercible).

use crate::config::{CompileContext, parse_config};

fn compile_diagnostics(yaml: &str) -> Vec<(String, String)> {
    let config = parse_config(yaml).expect("parse_config");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("compile should fail");
    diags
        .iter()
        .map(|d| (d.code.clone(), d.message.clone()))
        .collect()
}

#[test]
fn watermark_column_not_in_schema_emits_e154() {
    let yaml = r#"
pipeline:
  name: watermark_missing_column
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: a.csv
    watermark:
      column: nonexistent_column
    schema:
      - { name: id, type: int }
      - { name: event_time, type: date_time }
- type: output
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let diags = compile_diagnostics(yaml);
    let hit = diags
        .iter()
        .find(|(code, _)| code == "E154")
        .unwrap_or_else(|| panic!("expected E154, got {diags:?}"));
    assert!(
        hit.1.contains("nonexistent_column"),
        "E154 message should name the missing column: {}",
        hit.1
    );
}

#[test]
fn watermark_column_wrong_type_emits_e155() {
    let yaml = r#"
pipeline:
  name: watermark_wrong_type
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: a.csv
    watermark:
      column: id
    schema:
      - { name: id, type: int }
      - { name: event_time, type: date_time }
- type: output
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let diags = compile_diagnostics(yaml);
    let hit = diags
        .iter()
        .find(|(code, _)| code == "E155")
        .unwrap_or_else(|| panic!("expected E155, got {diags:?}"));
    assert!(
        hit.1.contains("id"),
        "E155 message should name the column: {}",
        hit.1
    );
}

#[test]
fn watermark_on_date_column_compiles_clean() {
    let yaml = r#"
pipeline:
  name: watermark_date_ok
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: a.csv
    watermark:
      column: day
    schema:
      - { name: id, type: int }
      - { name: day, type: date }
- type: output
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    config
        .compile(&CompileContext::default())
        .expect("Date-typed watermark column compiles clean");
}

#[test]
fn watermark_absent_compiles_clean() {
    let yaml = r#"
pipeline:
  name: no_watermark
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: a.csv
    schema:
      - { name: id, type: int }
      - { name: event_time, type: date_time }
- type: output
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    config
        .compile(&CompileContext::default())
        .expect("absent watermark compiles clean");
}
