//! E378: a composition body may not declare a Sink while a Source declares
//! `dlq_granularity: document`.
//!
//! Document granularity runs every Sink after every other node so each
//! document's verdict is final before any Sink writes. A body Sink runs
//! inside its composition's dispatch, where that ordering cannot reach it,
//! so the combination is refused at compile time. The same pipeline under
//! `dlq_granularity: record` compiles.

use std::path::PathBuf;

use clinker_core_types::{Diagnostic, Span};

use crate::config::{CompileContext, parse_config};

/// Compile a pipeline whose Source `events` declares `dlq_granularity:
/// {granularity}` and whose composition call `enrich` binds a body holding a
/// Transform `shape` and a Sink `audit` reading it.
fn compile_with_body_sink(granularity: &str) -> Result<(), Vec<Diagnostic>> {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("audited.comp.yaml"),
        r#"_compose:
  name: audited
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
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
        emit value = value
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
"#,
    )
    .expect("write comp");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");

    let yaml = format!(
        r#"
pipeline:
  name: document_dlq_body_sink
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      dlq_granularity: {granularity}
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
  - type: composition
    name: enrich
    input: events
    use: ../compositions/audited.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: out
    input: enrich
    config:
      name: out
      type: csv
      path: out.csv
"#
    );
    let config = parse_config(&yaml).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    config.compile(&ctx).map(|_| ())
}

#[test]
fn document_granularity_rejects_a_composition_body_sink() {
    let diags = compile_with_body_sink("document")
        .expect_err("a body Sink under document granularity must fail compilation");
    let e378: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == "E378").collect();
    assert_eq!(
        e378.len(),
        1,
        "exactly one E378 diagnostic, got {:?}",
        diags
            .iter()
            .map(|d| format!("{}: {}", d.code, d.message))
            .collect::<Vec<_>>()
    );
    let diag = e378[0];

    for fragment in [
        "composition 'enrich'",
        "Sink 'audit'",
        "source 'events'",
        "`dlq_granularity: document`",
        "pipeline level",
    ] {
        assert!(
            diag.message.contains(fragment),
            "the message names {fragment:?}: {}",
            diag.message
        );
    }
    let help = diag.help.as_deref().expect("E378 carries a help text");
    for fragment in [
        "input: enrich.audit",
        "audit: shape",
        "dlq_granularity: record",
        "#1242",
    ] {
        assert!(
            help.contains(fragment),
            "the help carries {fragment:?}: {help}"
        );
    }
    assert_ne!(
        diag.primary.span,
        Span::SYNTHETIC,
        "the diagnostic is located at the body Sink"
    );
    assert_eq!(
        diag.secondary.len(),
        1,
        "the composition call site is labelled"
    );
}

#[test]
fn record_granularity_keeps_composition_body_sinks() {
    if let Err(diags) = compile_with_body_sink("record") {
        panic!(
            "a body Sink under record granularity compiles, got {:?}",
            diags
                .iter()
                .map(|d| format!("{}: {}", d.code, d.message))
                .collect::<Vec<_>>()
        );
    }
}
