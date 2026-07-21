//! Plan-time gates on the multi-value surface.
//!
//! E358 rejects a malformed `split_to_rows` / `split_values` declaration on a
//! source; E359 rejects a `multiple: true` column reaching an output whose
//! format has no multi-value encoding. Both used to be either a runtime string
//! error at reader construction (XML only) or nothing at all, so these pin the
//! move to compile time with a diagnostic code and a source span.

use clinker_core_types::Diagnostic;

use crate::config::{CompileContext, parse_config};

/// A single-source pipeline: `source_body` is spliced under the source's
/// `config:` block, and the output writes `output_format`.
fn pipeline(source_body: &str, output_format: &str) -> String {
    format!(
        r#"
pipeline:
  name: multi_value_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: xml
      path: ./in.xml
      options:
        record_path: Orders/Order
{source_body}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: {output_format}
      path: out.txt
"#
    )
}

fn compile_err(yaml: &str) -> Vec<Diagnostic> {
    let config = parse_config(yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .expect_err("compile must fail")
}

fn compile_ok(yaml: &str) {
    let config = parse_config(yaml).expect("pipeline parses");
    config
        .compile(&CompileContext::default())
        .unwrap_or_else(|d| panic!("compile must succeed, got: {d:?}"));
}

/// Every diagnostic carrying `code`, as `(message, has_span)`.
fn coded(diags: &[Diagnostic], code: &str) -> Vec<(String, bool)> {
    diags
        .iter()
        .filter(|d| d.code == code)
        .map(|d| {
            (
                d.message.clone(),
                d.primary.span != clinker_core_types::span::Span::SYNTHETIC,
            )
        })
        .collect()
}

#[test]
fn duplicate_split_to_rows_field_is_rejected_with_a_span() {
    let yaml = pipeline(
        r#"      split_to_rows:
        - Item
        - Item
      schema:
        - { name: "Item.name", type: string }"#,
        "csv",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("more than once"), "{}", found[0].0);
    assert!(found[0].1, "E358 must carry a source span");
}

#[test]
fn nested_split_to_rows_fields_are_rejected() {
    // This ran at XML reader construction as an unspanned string error; it
    // now fails at compile, before any input is opened.
    let yaml = pipeline(
        r#"      split_to_rows:
        - Item
        - Item.part
      schema:
        - { name: "Item.part.code", type: string }"#,
        "csv",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("nest"), "{}", found[0].0);
}

#[test]
fn disjoint_split_to_rows_fields_compile() {
    let yaml = pipeline(
        r#"      split_to_rows:
        - Item
        - Tag
      schema:
        - { name: "Item.name", type: string }
        - { name: Tag, type: string }"#,
        "csv",
    );
    compile_ok(&yaml);
}

#[test]
fn split_values_on_a_single_valued_column_is_rejected() {
    // Splitting text on a delimiter produces several values, which only a
    // `multiple: true` column can hold.
    let yaml = pipeline(
        r#"      split_values:
        - field: tags
      schema:
        - { name: tags, type: string }"#,
        "csv",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("multiple: true"), "{}", found[0].0);
}

#[test]
fn duplicate_split_values_field_is_rejected() {
    let yaml = pipeline(
        r#"      split_values:
        - tags
        - field: tags
          delimiter: "|"
      schema:
        - { name: tags, type: string, multiple: true }"#,
        "json",
    );
    let found = coded(&compile_err(&yaml), "E358");
    assert_eq!(found.len(), 1, "expected one E358");
    assert!(found[0].0.contains("more than once"), "{}", found[0].0);
}

#[test]
fn multi_value_column_into_a_json_output_compiles() {
    // JSON is the one output format that can encode a multi-value field
    // today, so the E359 capability table lets it through.
    let yaml = pipeline(
        r#"      schema:
        - { name: tags, type: string, multiple: true }"#,
        "json",
    );
    compile_ok(&yaml);
}

#[test]
fn multi_value_column_into_a_non_encoding_output_is_rejected_with_a_span() {
    for format in ["csv", "xml", "fixed_width"] {
        let yaml = pipeline(
            r#"      schema:
        - { name: tags, type: string, multiple: true }"#,
            format,
        );
        let found = coded(&compile_err(&yaml), "E359");
        assert_eq!(found.len(), 1, "expected one E359 for {format}");
        assert!(
            found[0].0.contains("'tags'") && found[0].0.contains(format),
            "{format}: {}",
            found[0].0
        );
        assert!(found[0].1, "E359 must carry a source span for {format}");
    }
}

#[test]
fn a_source_with_no_multi_value_column_is_unaffected() {
    let yaml = pipeline(
        r#"      schema:
        - { name: tags, type: string }"#,
        "csv",
    );
    compile_ok(&yaml);
}
