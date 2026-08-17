//! End-to-end proof that a JSON source read with nested objects and written
//! back with no intervening transform reproduces the input nesting.
//!
//! The expansion itself is unit-tested in `clinker-format`; what this guards is
//! the executor wiring those tests cannot see — a real pipeline routing a
//! source's inferred dotted columns through the writer factory into the JSON
//! writer.

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

fn json_reader(input: &str) -> SourceReaders {
    HashMap::from([(
        "orders".to_string(),
        clinker_exec::executor::single_file_reader(
            "in.json",
            Box::new(std::io::Cursor::new(input.as_bytes().to_vec())),
        ),
    )])
}

fn run(pipeline: &str, input: &str) -> String {
    let config = clinker_plan::config::parse_config(pipeline).expect("pipeline parses");
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    common::run_config(&config, json_reader(input), writers, &params()).expect("run succeeds");
    String::from_utf8(buf.contents()).expect("utf-8 output")
}

const PIPELINE: &str = r#"
pipeline:
  name: json_nested_passthrough
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./in.json
      schema:
        - { name: order_id, type: string }
        - { name: customer.name, type: string }
        - { name: customer.email, type: string }
        - { name: customer.address.city, type: string }
  - type: sink
    name: out
    input: orders
    config:
      name: out
      type: json
      path: ./out.json
      options:
        format: ndjson
"#;

#[test]
fn nested_json_read_and_written_back_keeps_its_shape() {
    const INPUT: &str = r#"[{"order_id":"1","customer":{"name":"Ada","email":"ada@example.com","address":{"city":"Boston"}}}]"#;
    let output = run(PIPELINE, INPUT);
    let written: serde_json::Value =
        serde_json::from_str(output.trim_end()).expect("valid JSON output");
    let source: serde_json::Value = serde_json::from_str(INPUT).expect("valid JSON input");
    assert_eq!(
        written,
        source.as_array().expect("input array")[0],
        "got: {output}"
    );
}
