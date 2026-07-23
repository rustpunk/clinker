//! End-to-end proof that a `multiple:` field reaches an XML sink and is written
//! as repeated child elements, and that a `join_values` `repeat_as` / `wrap_in`
//! override renames the item and adds a container — the central acceptance
//! criterion of #916.
//!
//! The repeated-element emission is unit-tested in `clinker-format`; this guards
//! the executor wiring the unit tests cannot see: the writer factory threading
//! the output's `join_values` into the XML writer config.

mod common;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineRunParams, SourceReaders};

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
  - type: output
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
  - type: output
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
