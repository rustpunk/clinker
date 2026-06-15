//! Structured output writers must not silently merge multiple documents into
//! one envelope.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

fn slot(name: &str, csv: &str) -> FileSlot {
    FileSlot::new(
        PathBuf::from(format!("{name}.csv")),
        Box::new(Cursor::new(csv.as_bytes().to_vec())),
    )
}

fn run_yaml(yaml: &str, readers: SourceReaders) -> Result<String, String> {
    let config = parse_config(yaml).map_err(|e| format!("parse: {e:?}"))?;
    let plan = config
        .compile(&CompileContext::default())
        .map_err(|e| format!("compile: {e:?}"))?;

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf) as Box<dyn std::io::Write + Send>,
    )]);

    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
        .map(|_| "ok".to_string())
        .map_err(|e| format!("{e}"))
}

fn single_source_yaml(output_format: &str) -> String {
    format!(
        r#"
pipeline:
  name: structured_output_multidoc
nodes:
  - type: source
    name: body
    config:
      name: body
      type: csv
      glob: ./*.csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: tag, type: string }}
  - type: output
    name: out
    input: body
    config:
      name: out
      type: {output_format}
      path: out.{output_format}
"#
    )
}

#[test]
fn structured_single_writer_outputs_reject_multi_file_bodies() {
    for output_format in ["swift", "x12", "edifact", "hl7"] {
        let readers: SourceReaders = HashMap::from([(
            "body".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![
                slot("a", "id,tag\n1,A\n"),
                slot("b", "id,tag\n2,B\n"),
            ]),
        )]);

        let err = run_yaml(&single_source_yaml(output_format), readers)
            .expect_err("structured output must reject multi-document body");
        assert!(
            err.contains("multi-document body"),
            "{output_format} error should explain document-cardinality violation: {err}"
        );
        assert!(
            err.contains("out"),
            "{output_format} error should name the output: {err}"
        );
    }
}

fn streaming_merge_yaml() -> &'static str {
    r#"
pipeline:
  name: structured_output_streaming_multidoc
nodes:
  - type: source
    name: left
    config:
      name: left
      type: csv
      path: left.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: right
    config:
      name: right
      type: csv
      path: right.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: merge
    name: merged
    inputs: [left, right]
    config:
      mode: interleave
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: swift
      path: out.swift
"#
}

#[test]
fn streaming_structured_output_rejects_multi_document_merge_body() {
    let readers: SourceReaders = HashMap::from([
        (
            "left".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot("left", "id,tag\n1,L\n")]),
        ),
        (
            "right".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot("right", "id,tag\n2,R\n")]),
        ),
    ]);

    let err = run_yaml(streaming_merge_yaml(), readers)
        .expect_err("streaming structured output must reject multi-document body");
    assert!(err.contains("SWIFT error"), "{err}");
    assert!(err.contains("multi-document body"), "{err}");
}
