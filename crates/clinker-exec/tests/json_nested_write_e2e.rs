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
use clinker_plan::config::CompileContext;
use clinker_plan::error::PipelineError;
use clinker_record::nested_key::MAX_NESTED_VALUE_DEPTH;

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

fn transformed_pipeline(cxl: &str, preserve_nulls: Option<bool>) -> String {
    let cxl = cxl
        .lines()
        .map(|line| format!("        {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    let preserve_nulls = preserve_nulls
        .map(|value| format!("      preserve_nulls: {value}\n"))
        .unwrap_or_default();
    r#"
pipeline:
  name: json_nested_cxl
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: ./in.csv
      options:
        has_header: true
      schema:
        - { name: kind, type: string }
        - { name: key, type: string }
  - type: transform
    name: construct
    input: rows
    config:
      cxl: |
        __CXL__
  - type: sink
    name: out
    input: construct
    config:
      name: out
      type: json
      path: ./out.json
      include_unmapped: false
__PRESERVE_NULLS__
      options:
        format: ndjson
"#
    .replace("        __CXL__", &cxl)
    .replace("__PRESERVE_NULLS__", &preserve_nulls)
}

fn run_transformed(
    cxl: &str,
    input: &str,
    preserve_nulls: Option<bool>,
) -> (Result<(), PipelineError>, SharedBuffer) {
    let pipeline = transformed_pipeline(cxl, preserve_nulls);
    let config = clinker_plan::config::parse_config(&pipeline).expect("pipeline parses");
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let result =
        common::run_config(&config, json_reader_for_rows(input), writers, &params()).map(|_| ());
    (result, output)
}

fn json_reader_for_rows(input: &str) -> SourceReaders {
    HashMap::from([(
        "rows".to_string(),
        clinker_exec::executor::single_file_reader(
            "in.csv",
            Box::new(std::io::Cursor::new(input.as_bytes().to_vec())),
        ),
    )])
}

fn nested_map_expression(depth: usize, leaf: &str) -> String {
    format!("{}{leaf}{}", "{n: ".repeat(depth), "}".repeat(depth))
}

fn nested_json_value(depth: usize, leaf: &str) -> String {
    format!("{}{leaf}{}", r#"{"n":"#.repeat(depth), "}".repeat(depth))
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

#[test]
fn cxl_created_json_value_at_the_shared_depth_cap_is_byte_exact() {
    let expression = nested_map_expression(MAX_NESTED_VALUE_DEPTH, r#""leaf""#);
    let cxl = format!("emit payload = {expression}");

    let (result, output) = run_transformed(&cxl, "kind,key\nok,n\n", None);

    result.expect("depth-cap value writes");
    assert_eq!(
        output.as_string(),
        format!(
            r#"{{"payload":{}}}"#,
            nested_json_value(MAX_NESTED_VALUE_DEPTH, r#""leaf""#)
        )
    );
}

#[test]
fn cxl_created_json_value_over_the_depth_cap_leaves_no_record_bytes() {
    let expression = nested_map_expression(MAX_NESTED_VALUE_DEPTH + 1, "null");
    let cxl = format!("emit payload = {expression}");

    let (result, output) = run_transformed(&cxl, "kind,key\ntoo-deep,n\n", None);

    let error = result.expect_err("cap plus one must fail");
    assert!(
        matches!(
            error,
            PipelineError::Eval(ref source)
                if matches!(
                    source.kind,
                    cxl::eval::EvalErrorKind::ConstructionDepthExceeded { limit }
                        if limit == MAX_NESTED_VALUE_DEPTH
                )
        ),
        "unexpected error: {error:?}"
    );
    assert!(
        output.contents().is_empty(),
        "a rejected value cannot leave partial JSON"
    );
}

#[test]
fn json_decodes_reserved_looking_literal_keys_exactly_once() {
    let cxl = r#"emit payload = {"\\@literal": 1, "\\#text": "body", "\\\\name": true}"#;

    let (result, output) = run_transformed(cxl, "kind,key\nok,unused\n", None);

    result.expect("escaped literal keys write");
    assert_eq!(
        output.as_string(),
        r##"{"payload":{"@literal":1,"#text":"body","\\name":true}}"##
    );
}

#[test]
fn static_and_computed_json_key_collisions_both_fail_without_output() {
    let static_cxl = r#"emit payload = {"@id": 1, "\\@id": 2}"#;
    let pipeline = transformed_pipeline(static_cxl, None);
    let config = clinker_plan::config::parse_config(&pipeline).expect("pipeline YAML parses");

    let diagnostics = config
        .compile(&CompileContext::default())
        .expect_err("static logical-key collision must fail compilation");
    assert!(
        diagnostics
            .iter()
            .any(|diagnostic| diagnostic.message.contains("duplicate map key \"@id\"")),
        "unexpected diagnostics: {diagnostics:#?}"
    );

    let computed_cxl = r#"emit payload = {"@id": 1, [key]: 2}"#;
    let (result, output) = run_transformed(computed_cxl, "kind,key\ndynamic,@id\n", None);
    let error = result.expect_err("computed logical-key collision must fail evaluation");
    assert!(
        matches!(
            error,
            PipelineError::Eval(ref source)
                if matches!(
                    &source.kind,
                    cxl::eval::EvalErrorKind::DuplicateMapKey { key } if key == "@id"
                )
        ),
        "unexpected error: {error:?}"
    );
    assert!(
        output.contents().is_empty(),
        "the computed collision cannot reach the JSON writer"
    );
}

#[test]
fn json_null_policy_defaults_to_omit_and_preserves_nested_null_values() {
    let cxl = "emit payload = {missing: null, items: [null, \"x\"]}\n\
               emit absent = if kind == \"ok\" then null else \"present\"";

    let (default_result, defaulted) = run_transformed(cxl, "kind,key\nok,unused\n", None);
    let (drop_result, dropped) = run_transformed(cxl, "kind,key\nok,unused\n", Some(false));
    let (keep_result, kept) = run_transformed(cxl, "kind,key\nok,unused\n", Some(true));

    default_result.expect("default null policy writes");
    drop_result.expect("drop-null JSON writes");
    keep_result.expect("preserve-null JSON writes");
    assert_eq!(
        defaulted.contents(),
        dropped.contents(),
        "the omitted option must retain the false default"
    );
    assert_eq!(
        dropped.as_string(),
        r#"{"payload":{"missing":null,"items":[null,"x"]}}"#
    );
    assert_eq!(
        kept.as_string(),
        r#"{"payload":{"missing":null,"items":[null,"x"]},"absent":null}"#
    );
}
