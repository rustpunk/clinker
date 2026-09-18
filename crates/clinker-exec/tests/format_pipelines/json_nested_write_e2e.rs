//! End-to-end proof that a JSON source read with nested objects and written
//! back with no intervening transform reproduces the input nesting.
//!
//! The expansion itself is unit-tested in `clinker-format`; what this guards is
//! the executor wiring those tests cannot see — a real pipeline routing a
//! source's inferred dotted columns through the writer factory into the JSON
//! writer.

use crate::common;

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
    assert_eq!(output, concat!(r#"{"order_id":"1","customer":{"name":"Ada","email":"ada@example.com","address":{"city":"Boston"}}}"#, "\n"));
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
            concat!(r#"{{"payload":{}}}"#, "\n"),
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
        concat!(r##"{"payload":{"@literal":1,"#text":"body","\\name":true}}"##, "\n")
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
        concat!(r#"{"payload":{"missing":null,"items":[null,"x"]}}"#, "\n")
    );
    assert_eq!(
        kept.as_string(),
        concat!(r#"{"payload":{"missing":null,"items":[null,"x"]},"absent":null}"#, "\n")
    );
}

#[test]
fn compiled_json_array_ndjson_pretty_and_envelope_bytes() {
    for mode in ["array", "ndjson"] {
        for pretty in [false, true] {
            for envelope in [false, true] {
                let source_options = if envelope { r#"
      options:
        record_path: records
      envelope:
        sections:
          opening:
            extract: { json_pointer: "/opening" }
            fields: { tag: int }
          closing:
            extract: { json_pointer: "/closing" }
            fields: { status: string }
"# } else { "\n" };
                let sink_envelope = if envelope { r#"
        envelope:
          header_from_doc: opening
          footer_from_doc: closing
          footer_record_count_field: rows
"# } else { "\n" };
                let pipeline = format!(r#"
pipeline:
  name: json_framing
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: in.json
      schema: [{{ name: v, type: int }}]
{source_options}
  - type: sink
    name: out
    input: orders
    config:
      name: out
      type: json
      path: out.json
      reconstruct_envelope: {envelope}
      options:
        format: {mode}
        pretty: {pretty}
{sink_envelope}
"#);
                let input = if envelope {
                    r#"{"opening":{"tag":7},"closing":{"status":"done"},"records":[{"v":1},{"v":2}]}"#
                } else { r#"[{"v":1},{"v":2}]"# };
                let expected = match (envelope, pretty, mode) {
                    (false, _, "ndjson") => "{\"v\":1}\n{\"v\":2}\n".to_owned(),
                    (false, false, "array") => "[\n{\"v\":1},\n{\"v\":2}\n]\n".to_owned(),
                    (false, true, "array") => "[\n{\n  \"v\": 1\n},\n{\n  \"v\": 2\n}\n]\n".to_owned(),
                    (true, pretty, mode) => {
                        let doc = if pretty {
                            "{\"header\":{\n  \"tag\": 7\n},\"body\":[{\n  \"v\": 1\n},{\n  \"v\": 2\n}],\"footer\":{\n  \"status\": \"done\",\n  \"rows\": 2\n}}"
                        } else {
                            "{\"header\":{\"tag\":7},\"body\":[{\"v\":1},{\"v\":2}],\"footer\":{\"status\":\"done\",\"rows\":2}}"
                        };
                        if mode == "array" { format!("[\n{doc}\n]\n") } else { doc.to_owned() }
                    }
                    _ => unreachable!(),
                };
                assert_eq!(run(&pipeline, input), expected, "{mode} pretty={pretty} envelope={envelope}");
            }
        }
    }
}

#[test]
fn nested_modes_physical_files_reset_utf8_and_selected_row_cardinality() {
    use clinker_exec::source::{SourceInput, multi_file::FileSlot};
    const CASES: &[(&str, &str, &str)] = &[
        ("json-array", "json", ""),
        ("json-ndjson", "json", "      options: { format: ndjson }\n"),
        ("json-body", "json", "      options: { record_path: items }\n"),
        ("json-envelope", "json", "      options: { record_path: items }\n"),
        ("json-document", "json", "      options: { record_path: items }\n      dlq_granularity: document\n"),
        ("xml-selected", "xml", "      options: { record_path: Root/items/row }\n"),
        ("xml-envelope", "xml", "      options: { record_path: Root/items/row }\n"),
        ("xml-document", "xml", "      options: { record_path: Root/items/row }\n      dlq_granularity: document\n"),
    ];
    for &(name, format, options) in CASES {
        let root = tempfile::tempdir().unwrap();
        let envelope = if name.ends_with("envelope") {
            format!("      envelope:\n        sections:\n          manifest:\n            extract: {{ {}: \"{}\" }}\n            fields:\n              batch: string\n", if format == "json" { "json_pointer" } else { "xml_path" }, if format == "json" { "/manifest" } else { "/Root/manifest" })
        } else { String::new() };
        let yaml = format!(r#"
pipeline:
  name: selected_native_rows
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: {format}
      path: input.{format}
{options}{envelope}      schema:
        - {{ name: id, type: {{ nullable: int }} }}
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: json
      path: output.json
      options: {{ format: ndjson }}
"#);
        let config = clinker_plan::config::parse_config(&yaml).unwrap();
        let files = (1..=2).map(|id| {
            let text = match name {
                "json-array" => format!(r#"[{{"id":{id}}},{{}}]"#),
                "json-ndjson" => format!("{{\"id\":{id}}}\n{{}}\n"),
                "json-body" | "json-envelope" | "json-document" => format!(r#"{{"metadata":{{"id":99}},"items":[{{"id":{id}}},{{}}],"manifest":{{"batch":"batch-{id}"}},"trailing":{{"id":98}}}}"#),
                _ => format!(r#"<?xml version="1.0" encoding="UTF-8"?><Root><metadata><id>99</id></metadata><items><row><id>{id}</id></row></items><between><id>97</id></between><items><row/></items><manifest><batch>batch-{id}</batch></manifest><trailing><id>98</id></trailing></Root>"#),
            };
            let path = root.path().join(format!("input-{id}.{format}"));
            std::fs::write(&path, [b"\xef\xbb\xbf".as_slice(), text.as_bytes()].concat()).unwrap();
            FileSlot::new(path.clone(), Box::new(std::fs::File::open(path).unwrap()))
        }).collect();
        let readers = HashMap::from([("rows".to_owned(), SourceInput::Files(files))]);
        let output = SharedBuffer::new();
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = [("out".into(), Box::new(output.clone()) as Box<dyn std::io::Write + Send>)].into();
        let report = common::run_config(&config, readers, writers, &params()).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(output.contents(), b"{\"id\":1}\n{}\n{\"id\":2}\n{}\n", "{name}");
        assert_eq!(report.counters.total_count, 4, "{name}");
        assert_eq!(report.counters.records_written, 4, "{name}");
        assert_eq!(report.counters.dlq_count, 0, "{name}");
        assert_eq!(report.per_source_record_counts["rows"], 4, "{name}");
    }
}

#[test]
fn nested_modes_invalid_second_physical_file_preserves_first_file_prefix() {
    use clinker_exec::source::{SourceInput, multi_file::FileSlot};
    for format in ["json", "xml"] {
        for failure in ["bom", "utf8", "declaration-or-bom32"] {
            let root = tempfile::tempdir().unwrap();
            let options = if format == "xml" { "      options: { record_path: Root/row }\n" } else { "" };
            let yaml = format!(r#"
pipeline:
  name: rejected_native_file
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: {format}
      path: input.{format}
{options}      schema:
        - {{ name: id, type: int }}
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: json
      path: output.json
      options: {{ format: ndjson }}
"#);
            let config = clinker_plan::config::parse_config(&yaml).unwrap();
            let first = if format == "json" { b"\xef\xbb\xbf[{\"id\":1}]".as_slice() } else { b"\xef\xbb\xbf<?xml version=\"1.0\" encoding=\"UTF-8\"?><Root><row><id>1</id></row></Root>".as_slice() };
            let second: &[u8] = match (format, failure) {
                (_, "bom") => b"\xff\xfe{\0}\0",
                ("json", "utf8") => b"[{\"id\":\"\xff\"}]",
                ("xml", "utf8") => b"<Root><row><id>\xff</id></row></Root>",
                ("xml", _) => b"<?xml version=\"1.0\" encoding=\"UTF-16\"?><Root><row><id>2</id></row></Root>",
                _ => b"\0\0\xfe\xff[\0\0\0",
            };
            let files = [first, second].into_iter().enumerate().map(|(i, bytes)| {
                let path = root.path().join(format!("part-{i}.{format}"));
                std::fs::write(&path, bytes).unwrap();
                FileSlot::new(path.clone(), Box::new(std::fs::File::open(path).unwrap()))
            }).collect();
            let output = SharedBuffer::new();
            let writers: HashMap<String, Box<dyn std::io::Write + Send>> = [("out".into(), Box::new(output.clone()) as Box<dyn std::io::Write + Send>)].into();
            let result = common::run_config(&config, [("rows".into(), SourceInput::Files(files))].into(), writers, &params());
            let error = result.expect_err("invalid second input must fail");
            assert!(error.to_string().contains("UTF"), "{format}/{failure}: {error}");
            assert_eq!(output.contents(), b"{\"id\":1}\n", "{format}/{failure}: only the first physical file is valid");
        }
    }
}
