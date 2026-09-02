//! End-to-end proof that a `multiple:` field reaches an XML sink and is written
//! as repeated child elements, and that a `join_values` `repeat_as` / `wrap_in`
//! override renames the item and adds a container — the central acceptance
//! criterion of #916.
//!
//! The repeated-element emission is unit-tested in `clinker-format`; this guards
//! the executor wiring the unit tests cannot see: the writer factory threading
//! the output's `join_values` into the XML writer config.

use crate::common;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{ExecutionReport, PipelineRunParams, SourceReaders};
use clinker_plan::error::PipelineError;

fn params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// A JSON source (native arrays) with a two-value `tags` column on one record.
const JSON_INPUT: &str = r#"[{"order_id":"1","tags":["a","b"]}]"#;

fn json_reader() -> SourceReaders {
    HashMap::from([(
        "orders".to_string(),
        clinker_exec::executor::single_file_reader(
            "in.json",
            Box::new(std::io::Cursor::new(JSON_INPUT.as_bytes().to_vec())),
        ),
    )])
}

fn run_input(
    pipeline: &str,
    source: &str,
    filename: &str,
    input: &[u8],
) -> (Result<ExecutionReport, PipelineError>, SharedBuffer) {
    let config = clinker_plan::config::parse_config(pipeline).expect("pipeline parses");
    let readers = HashMap::from([(
        source.to_string(),
        clinker_exec::executor::single_file_reader(
            filename,
            Box::new(std::io::Cursor::new(input.to_vec())),
        ),
    )]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    (
        common::run_config(&config, readers, writers, &params()),
        output,
    )
}

fn run(pipeline: &str) -> String {
    let config = clinker_plan::config::parse_config(pipeline).expect("pipeline parses");
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    common::run_config(&config, json_reader(), writers, &params())
        .expect("run succeeds — the array encodes as repeated elements");
    String::from_utf8(buf.contents()).expect("utf-8 output")
}

#[test]
fn xml_declared_multiple_read_write_round_trip() {
    const PIPELINE: &str = r#"
pipeline:
  name: xml_multi_value_round_trip
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: xml
      path: ./in.xml
      options:
        record_path: Root/Record
      schema:
        - { name: order_id, type: int }
        - { name: tags, type: string, multiple: true }
  - type: sink
    name: out
    input: orders
    config:
      name: out
      type: xml
      path: ./out.xml
"#;
    const INPUT: &[u8] =
        b"<Root><Record><order_id>1</order_id><tags>a</tags><tags>b</tags></Record></Root>";
    let (result, output) = run_input(PIPELINE, "orders", "in.xml", INPUT);
    result.expect("declared multi-value XML round-trip succeeds");
    assert_eq!(output.contents(), INPUT);
}

#[test]
fn xml_undeclared_array_fails_on_buffered_output() {
    const PIPELINE: &str = r#"
pipeline:
  name: xml_array_backstop_buffered
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./in.json
      schema:
        - { name: payload, type: any }
  - type: sink
    name: out
    input: orders
    config:
      name: out
      type: xml
      path: ./out.xml
"#;
    let (result, _output) = run_input(PIPELINE, "orders", "in.json", br#"[{"payload":["a","b"]}]"#);
    let error = result.expect_err("an undeclared array must fail the XML write");
    let message = error.to_string();
    assert!(
        message.contains("XML") && message.contains("payload"),
        "{message}"
    );
}

#[test]
fn xml_undeclared_array_fails_on_streaming_output() {
    const PIPELINE: &str = r#"
pipeline:
  name: xml_array_backstop_streaming
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./in.json
      schema:
        - { name: order_id, type: string }
  - type: transform
    name: collect
    input: orders
    config:
      cxl: |
        emit payload = [order_id]
  - type: sink
    name: out
    input: collect
    config:
      name: out
      type: xml
      path: ./out.xml
"#;
    let (result, _output) = run_input(PIPELINE, "orders", "in.json", br#"[{"order_id":"1"}]"#);
    let error = result.expect_err("an undeclared array must fail the fused XML write");
    let message = error.to_string();
    assert!(
        message.contains("XML") && message.contains("payload"),
        "{message}"
    );
}

/// With no `join_values` config the values emit as bare repeated elements named
/// after the field — the write side of the read-side `multiple: true` collect.
#[test]
fn xml_multi_value_default_emits_repeated_elements() {
    const PIPELINE: &str = r#"
pipeline:
  name: xml_multi_value_default
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./in.json
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: sink
    name: out
    input: orders
    config:
      name: out
      type: xml
      path: ./out.xml
"#;
    let output = run(PIPELINE);
    assert_eq!(
        output, "<Root><Record><order_id>1</order_id><tags>a</tags><tags>b</tags></Record></Root>",
        "got: {output}"
    );
}

/// A `join_values` entry with `repeat_as` and `wrap_in` renames the item element
/// and wraps the run in a container — proving the override flows from YAML,
/// through the writer factory, into the XML writer config.
#[test]
fn xml_multi_value_repeat_as_and_wrap_in_flow_through_the_factory() {
    const PIPELINE: &str = r#"
pipeline:
  name: xml_multi_value_wrapped
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./in.json
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: sink
    name: out
    input: orders
    config:
      name: out
      type: xml
      path: ./out.xml
      join_values:
        - { field: tags, repeat_as: Tag, wrap_in: Tags }
"#;
    let output = run(PIPELINE);
    assert_eq!(
        output,
        "<Root><Record><order_id>1</order_id><Tags><Tag>a</Tag><Tag>b</Tag></Tags></Record></Root>",
        "got: {output}"
    );
}
