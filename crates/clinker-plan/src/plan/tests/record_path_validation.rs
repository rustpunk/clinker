//! The plan-time `record_path` grammar gate (E363).
//!
//! `record_path` used to be an unvalidated `Option<String>` copied verbatim
//! into reader config. An XPath-shaped value (`//product`) split into a segment
//! list whose first entry no element name can equal, so the run finished with
//! zero records and reported success. These tests pin every rejected form to a
//! spanned compile-time diagnostic, and pin the accepted forms so the grammar
//! does not over-reject.

use clinker_core_types::Diagnostic;

use crate::config::{CompileContext, parse_config};

/// A single-source XML pipeline declaring `record_path: raw`. `raw` is written
/// through `{:?}` so a value containing YAML-significant characters (`*`, a
/// leading `/`, the empty string) reaches the parser intact.
fn xml_pipeline(raw: &str) -> String {
    xml_pipeline_with(raw, "")
}

/// [`xml_pipeline`] plus extra lines spliced into the source's `options:`
/// block, for the namespace-qualified case.
fn xml_pipeline_with(raw: &str, extra_options: &str) -> String {
    format!(
        r#"
pipeline:
  name: record_path_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: xml
      path: ./in.xml
      options:
        record_path: {raw:?}
{extra_options}
      schema:
        - {{ name: id, type: int }}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// An XML pipeline that declares no `record_path` at all — the supported form
/// an empty string is NOT equivalent to.
fn xml_pipeline_without_record_path() -> String {
    r#"
pipeline:
  name: record_path_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: xml
      path: ./in.xml
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#
    .to_string()
}

/// The JSON twin of [`xml_pipeline`].
fn json_pipeline(raw: &str) -> String {
    format!(
        r#"
pipeline:
  name: record_path_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: ./in.json
      options:
        record_path: {raw:?}
      schema:
        - {{ name: id, type: int }}
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

fn json_pipeline_without_record_path() -> String {
    r#"
pipeline:
  name: record_path_gate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: ./in.json
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#
    .to_string()
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

/// The single E363 a pipeline must produce, asserting it carries a span.
fn sole_e363(yaml: &str) -> String {
    let diags = compile_err(yaml);
    let found = coded(&diags, "E363");
    assert_eq!(found.len(), 1, "expected exactly one E363, got: {diags:?}");
    assert!(found[0].1, "E363 must carry a source span: {}", found[0].0);
    found[0].0.clone()
}

#[test]
fn xpath_descendant_step_is_rejected() {
    let msg = sole_e363(&xml_pipeline("//product"));
    assert!(msg.contains("src"), "{msg}");
    assert!(msg.contains("XPath"), "{msg}");
    // The message must show the shape the author should have written.
    assert!(msg.contains("Write \"product\" instead"), "{msg}");
}

#[test]
fn rooted_xml_path_is_rejected() {
    let msg = sole_e363(&xml_pipeline("/Orders/Order"));
    assert!(msg.contains("already anchored"), "{msg}");
    assert!(msg.contains("Write \"Orders/Order\" instead"), "{msg}");
}

#[test]
fn doubled_separator_inside_an_xml_path_is_rejected() {
    let msg = sole_e363(&xml_pipeline("Orders//Order"));
    assert!(msg.contains("Write \"Orders/Order\" instead"), "{msg}");
}

#[test]
fn trailing_separator_on_an_xml_path_is_rejected() {
    let msg = sole_e363(&xml_pipeline("Orders/"));
    assert!(msg.contains("empty segment"), "{msg}");
    assert!(msg.contains("Write \"Orders\" instead"), "{msg}");
}

#[test]
fn an_empty_xml_record_path_is_rejected_though_omitting_it_is_fine() {
    // `record_path: ""` is not the same as leaving the key out: the empty
    // string is a one-segment path naming an element called "", which reads
    // zero records. Omitting the key makes every top-level element a record.
    let msg = sole_e363(&xml_pipeline(""));
    assert!(msg.contains("empty"), "{msg}");
    assert!(
        msg.contains("every top-level element becomes one record"),
        "{msg}"
    );
    compile_ok(&xml_pipeline_without_record_path());
}

#[test]
fn xpath_predicates_and_axes_are_rejected() {
    for (raw, needle) in [
        ("//product[@id]", "XPath"),
        ("product[@id]", "not an XML element name"),
        ("child::product", "not an XML element name"),
        ("*", "not an XML element name"),
    ] {
        let msg = sole_e363(&xml_pipeline(raw));
        assert!(msg.contains(needle), "{raw:?}: {msg}");
    }
}

#[test]
fn jsonpath_root_marker_is_rejected_on_a_json_source() {
    let msg = sole_e363(&json_pipeline("$.data"));
    assert!(msg.contains("JSONPath"), "{msg}");
    assert!(msg.contains("Write \"data\" instead"), "{msg}");
}

#[test]
fn rooted_and_empty_segment_json_paths_are_rejected() {
    for (raw, needle) in [
        ("/data", "already anchored"),
        ("//data", "already anchored"),
        (".data", "empty segment"),
        ("data..rows", "empty segment"),
        ("data.", "empty segment"),
    ] {
        let msg = sole_e363(&json_pipeline(raw));
        assert!(msg.contains(needle), "{raw:?}: {msg}");
    }
}

#[test]
fn an_empty_json_record_path_is_rejected_though_omitting_it_is_fine() {
    let msg = sole_e363(&json_pipeline(""));
    assert!(msg.contains("empty"), "{msg}");
    compile_ok(&json_pipeline_without_record_path());
}

#[test]
fn accepted_xml_paths_still_compile() {
    for raw in [
        "records/record",
        "Orders/Order",
        "doc/records/record",
        "PurchaseOrders/Order",
        "root/data/record",
        "records",
    ] {
        compile_ok(&xml_pipeline(raw));
    }
}

#[test]
fn a_namespace_qualified_xml_path_still_compiles() {
    // `:` is a legal XML NameChar and `namespace_handling: qualify` keeps the
    // prefix on every element name, so a qualified path must survive the
    // XML-name segment rule.
    compile_ok(&xml_pipeline_with(
        "ns:Orders/ns:Order",
        "        namespace_handling: qualify",
    ));
}

#[test]
fn accepted_json_paths_still_compile() {
    for raw in [
        "data.rows",
        "data.records",
        "batch_records",
        "records",
        // Only the exact `$.` prefix is a JSONPath marker, so a `$`-prefixed
        // key stays addressable.
        "$schema.rows",
        "$",
    ] {
        compile_ok(&json_pipeline(raw));
    }
}
