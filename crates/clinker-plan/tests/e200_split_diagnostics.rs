//! Boundary tests for the CXL-compile diagnostic split (#461).
//!
//! `typecheck_cxl` once collapsed CXL parse, name-resolution, and type
//! failures under one `E200` code. Each class now carries its own code:
//! parse → E202, name-resolution → E203, type → E200. These tests compile a
//! pipeline through the public `PipelineConfig::compile` boundary and pin the
//! code each failure class produces, so the three stay distinguishable by
//! code (not just by message prefix).

use clinker_plan::config::{CompileContext, parse_config};

/// A source → transform → output pipeline whose transform body is `cxl`.
/// The upstream schema is `amount: int, label: string`, so a well-formed
/// body has real columns to reference and mistype.
fn pipeline_with_transform_cxl(cxl: &str) -> String {
    format!(
        r#"
pipeline:
  name: e200_split
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: amount, type: int }}
        - {{ name: label, type: string }}
  - type: transform
    name: t
    input: src
    config:
      cxl: "{cxl}"
  - type: output
    name: out
    input: t
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// Compile the fixture, expect failure, and return every `(code, message)`.
fn compile_diagnostics(yaml: &str) -> Vec<(String, String)> {
    let config = parse_config(yaml).expect("fixture must parse as YAML");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("fixture is expected to fail compilation");
    diags
        .into_iter()
        .map(|d| (d.code.clone(), d.message.clone()))
        .collect()
}

fn codes(diags: &[(String, String)]) -> Vec<&str> {
    diags.iter().map(|(c, _)| c.as_str()).collect()
}

#[test]
fn cxl_parse_error_is_e202() {
    // A trailing binary operator with no right operand is a syntax error.
    let diags = compile_diagnostics(&pipeline_with_transform_cxl("emit total = amount *"));
    assert!(
        diags.iter().any(|(code, _)| code == "E202"),
        "expected a parse error coded E202, got {:?}",
        codes(&diags)
    );
    // A parse failure short-circuits resolution and type checking, so it
    // must not masquerade as a name-resolution (E203) or type (E200) error.
    assert!(
        !diags
            .iter()
            .any(|(code, _)| code == "E200" || code == "E203"),
        "a CXL parse error must not surface as E200/E203, got {:?}",
        codes(&diags)
    );
}

#[test]
fn cxl_name_resolution_error_is_e203() {
    // `subtotal` is not a column of the upstream schema.
    let diags = compile_diagnostics(&pipeline_with_transform_cxl("emit total = subtotal + 1"));
    assert!(
        diags
            .iter()
            .any(|(code, msg)| code == "E203" && msg.contains("subtotal")),
        "expected a name-resolution error coded E203 naming `subtotal`, got {diags:?}"
    );
    assert!(
        !diags
            .iter()
            .any(|(code, _)| code == "E200" || code == "E202"),
        "a CXL name-resolution error must not surface as E200/E202, got {:?}",
        codes(&diags)
    );
}

#[test]
fn cxl_type_error_is_e200() {
    // `amount` is Int and `label` is String; adding them does not type-check.
    let diags = compile_diagnostics(&pipeline_with_transform_cxl("emit total = amount + label"));
    assert!(
        diags.iter().any(|(code, _)| code == "E200"),
        "expected a type error coded E200, got {:?}",
        codes(&diags)
    );
    assert!(
        !diags
            .iter()
            .any(|(code, _)| code == "E202" || code == "E203"),
        "a CXL type error must not surface as E202/E203, got {:?}",
        codes(&diags)
    );
}
