//! Executable-boundary coverage for CXL-created values written as native XML.
//!
//! These tests deliberately pass through YAML parsing, CXL compilation and
//! evaluation, executor dispatch, and the real XML writer factory. Assertions
//! use exact bytes so wrapper, repetition, null, and record-atomicity behavior
//! cannot drift independently between those layers.

mod common;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineRunParams, SourceReaders};
use clinker_plan::config::CompileContext;
use clinker_plan::error::PipelineError;
use clinker_record::nested_key::MAX_NESTED_VALUE_DEPTH;

fn params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "xml-nested-e2e".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

fn transformed_pipeline(cxl: &str, preserve_nulls: Option<bool>, rename_repeats: bool) -> String {
    let cxl = cxl
        .lines()
        .map(|line| format!("        {line}"))
        .collect::<Vec<_>>()
        .join("\n");
    let join_values = if rename_repeats {
        "      join_values:\n        - { field: tags, repeat_as: Tag, wrap_in: Tags }\n"
    } else {
        ""
    };
    let output_schema = if rename_repeats {
        "      schema:\n        - { name: tags, type: string, multiple: true }\n"
    } else {
        ""
    };
    let preserve_nulls = preserve_nulls
        .map(|value| format!("      preserve_nulls: {value}\n"))
        .unwrap_or_default();
    r#"
pipeline:
  name: xml_nested_cxl
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
      type: xml
      path: ./out.xml
      include_unmapped: false
__OUTPUT_SCHEMA__
__PRESERVE_NULLS__
__JOIN_VALUES__      options:
        root_element: Feed
        record_element: Entry
        attribute_prefix: "@"
"#
    .replace("        __CXL__", &cxl)
    .replace("__OUTPUT_SCHEMA__", output_schema)
    .replace("__PRESERVE_NULLS__", &preserve_nulls)
    .replace("__JOIN_VALUES__", join_values)
}

fn reader(input: &str) -> SourceReaders {
    HashMap::from([(
        "rows".to_string(),
        clinker_exec::executor::single_file_reader(
            "in.csv",
            Box::new(std::io::Cursor::new(input.as_bytes().to_vec())),
        ),
    )])
}

fn run_transformed(
    cxl: &str,
    input: &str,
    preserve_nulls: Option<bool>,
    rename_repeats: bool,
) -> (Result<(), PipelineError>, SharedBuffer) {
    let pipeline = transformed_pipeline(cxl, preserve_nulls, rename_repeats);
    let config = clinker_plan::config::parse_config(&pipeline).expect("pipeline parses");
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let result = common::run_config(&config, reader(input), writers, &params()).map(|_| ());
    (result, output)
}

fn nested_map_expression(depth: usize, leaf: &str) -> String {
    format!("{}{leaf}{}", "{n: ".repeat(depth), "}".repeat(depth))
}

fn nested_xml_value(depth: usize, leaf: &str) -> String {
    format!("{}{leaf}{}", "<n>".repeat(depth), "</n>".repeat(depth))
}

fn contains_format_error(error: &PipelineError) -> bool {
    match error {
        PipelineError::Format(_) => true,
        PipelineError::Multiple(errors) => errors.iter().any(contains_format_error),
        _ => false,
    }
}

#[test]
fn cxl_created_xml_value_at_the_shared_depth_cap_is_byte_exact() {
    let expression = nested_map_expression(MAX_NESTED_VALUE_DEPTH, r#""leaf""#);
    let cxl = format!("emit payload = {expression}");

    let (result, output) = run_transformed(&cxl, "kind,key\nok,n\n", None, false);

    result.expect("depth-cap value writes");
    assert_eq!(
        output.as_string(),
        format!(
            "<Feed><Entry><payload>{}</payload></Entry></Feed>",
            nested_xml_value(MAX_NESTED_VALUE_DEPTH, "leaf")
        )
    );
}

#[test]
fn cxl_created_xml_value_over_the_depth_cap_leaves_no_record_bytes() {
    let expression = nested_map_expression(MAX_NESTED_VALUE_DEPTH + 1, "null");
    let cxl = format!("emit payload = {expression}");

    let (result, output) = run_transformed(&cxl, "kind,key\ntoo-deep,n\n", None, false);

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
        "a rejected value cannot leave partial XML"
    );
}

#[test]
fn xml_repeats_native_children_and_applies_explicit_wrapper_overrides() {
    let cxl = r##"emit payload = {"@kind": "event", "#text": "before", item: [{"@id": 1, "#text": "alpha"}, {"@id": 2, "#text": "beta"}], tail: "after"}
emit tags = ["a", "b"]"##;

    let (result, output) = run_transformed(cxl, "kind,key\nok,unused\n", None, true);

    result.expect("recursive and overridden repeats write");
    assert_eq!(
        output.as_string(),
        r#"<Feed><Entry><payload kind="event">before<item id="1">alpha</item><item id="2">beta</item><tail>after</tail></payload><Tags><Tag>a</Tag><Tag>b</Tag></Tags></Entry></Feed>"#
    );
}

#[test]
fn static_and_computed_xml_reserved_key_collisions_have_parity() {
    let collision_cases = [
        (
            r#"emit payload = {"@id": 1, "\\@id": 2}"#,
            r#"emit payload = {"@id": 1, [key]: 2}"#,
            r"\@id",
            "@id",
        ),
        (
            r##"emit payload = {"#text": "one", "\\#text": "two"}"##,
            r##"emit payload = {"#text": "one", [key]: "two"}"##,
            r"\#text",
            "#text",
        ),
    ];

    for (static_cxl, computed_cxl, computed_key, logical_key) in collision_cases {
        let pipeline = transformed_pipeline(static_cxl, None, false);
        let config = clinker_plan::config::parse_config(&pipeline).expect("pipeline YAML parses");
        let diagnostics = config
            .compile(&CompileContext::default())
            .expect_err("static logical-key collision must fail compilation");
        assert!(
            diagnostics
                .iter()
                .any(|diagnostic| diagnostic.message.contains("duplicate map key")),
            "unexpected diagnostics for {logical_key:?}: {diagnostics:#?}"
        );

        let input = format!("kind,key\ndynamic,{computed_key}\n");
        let (result, output) = run_transformed(computed_cxl, &input, None, false);
        let error = result.expect_err("computed logical-key collision must fail evaluation");
        assert!(
            matches!(
                error,
                PipelineError::Eval(ref source)
                    if matches!(
                        &source.kind,
                        cxl::eval::EvalErrorKind::DuplicateMapKey { key }
                            if key == logical_key
                    )
            ),
            "unexpected error for {logical_key:?}: {error:?}"
        );
        assert!(
            output.contents().is_empty(),
            "a computed collision cannot reach the XML writer"
        );
    }
}

#[test]
fn value_dependent_unsupported_xml_shapes_fail_before_record_bytes() {
    let cases = [
        (
            "array inside array",
            "emit payload = if kind == \"bad\" then [[1]] else [1]",
            "unused",
        ),
        (
            "collection-valued attribute",
            r#"emit payload = if kind == "bad" then {"@ids": [1]} else {"@ids": 1}"#,
            "unused",
        ),
        (
            "collection-valued text",
            r##"emit payload = if kind == "bad" then {"#text": [1]} else {"#text": "ok"}"##,
            "unused",
        ),
        (
            "computed invalid element name",
            r#"emit payload = {[key]: 1}"#,
            "1bad",
        ),
    ];

    for (label, cxl, key) in cases {
        let input = format!("kind,key\nbad,{key}\n");
        let (result, output) = run_transformed(cxl, &input, None, false);
        let error = result.expect_err(label);
        assert!(
            contains_format_error(&error),
            "{label} must surface as a format error: {error:?}"
        );
        assert!(
            output.contents().is_empty(),
            "{label} cannot leave partial XML"
        );
    }
}

#[test]
fn a_rejected_second_xml_record_leaves_the_first_record_complete() {
    let cxl = r#"emit payload = if kind == "good" then {item: [1, 2]} else {"@ids": [1]}"#;

    let (result, output) = run_transformed(cxl, "kind,key\ngood,unused\nbad,unused\n", None, false);

    let error = result.expect_err("the second record has an unsupported XML shape");
    assert!(contains_format_error(&error), "unexpected error: {error:?}");
    assert_eq!(
        output.as_string(),
        "<Feed><Entry><payload><item>1</item><item>2</item></payload></Entry>",
        "the rejected second record must add no start tag or body bytes"
    );
}

#[test]
fn xml_null_policy_defaults_to_omit_and_never_emits_null_attributes() {
    let cxl = r#"emit payload = {"@missing": null, empty: null, items: [null, "x"]}
emit absent = if kind == "ok" then null else "present""#;

    let (default_result, defaulted) = run_transformed(cxl, "kind,key\nok,unused\n", None, false);
    let (drop_result, dropped) = run_transformed(cxl, "kind,key\nok,unused\n", Some(false), false);
    let (keep_result, kept) = run_transformed(cxl, "kind,key\nok,unused\n", Some(true), false);

    default_result.expect("default null policy writes");
    drop_result.expect("drop-null XML writes");
    keep_result.expect("preserve-null XML writes");
    assert_eq!(
        defaulted.contents(),
        dropped.contents(),
        "the omitted option must retain the false default"
    );
    assert_eq!(
        dropped.as_string(),
        "<Feed><Entry><payload><items>x</items></payload></Entry></Feed>"
    );
    assert_eq!(
        kept.as_string(),
        "<Feed><Entry><payload><empty/><items/><items>x</items></payload><absent/></Entry></Feed>"
    );
}
