//! Compile-time validation of the Envelope node's declarative header/footer
//! synthesis.
//!
//! Header expressions are evaluated at document open, before the body streams,
//! so a body-column reference is rejected with E353. Footer aggregates fold the
//! body as O(1) accumulators, so an aggregate outside the streaming allow-list —
//! or one not a CXL function at all — is rejected (E354 for the former, an
//! unknown-function typecheck error for the latter).

use crate::config::{CompileContext, parse_config};

/// Build a pipeline whose body Transform feeds an Envelope node, with the
/// Envelope's `config:` block supplied verbatim by the caller. The body carries
/// an `amount` column so footer aggregates have a real field to fold.
fn pipeline_with_envelope_config(config_block: &str) -> String {
    let mut yaml = String::from(
        r#"
pipeline:
  name: envelope_synth
  vars:
    sender_id: { type: string, default: "ACME" }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: transform
    name: body
    input: src
    config:
      cxl: |
        emit id = id
        emit amount = amount
  - type: envelope
    name: framed
    body: body
    config:
"#,
    );
    for line in config_block.lines() {
        yaml.push_str(&format!("      {line}\n"));
    }
    yaml.push_str(
        "  - type: output\n    name: out\n    input: framed\n    config:\n      name: out\n      type: csv\n      path: out.csv\n",
    );
    yaml
}

#[test]
fn footer_count_and_sum_compile_clean() {
    // The streaming allow-list (count / sum) over bare-field / `*` arguments
    // compiles without diagnostics — the happy path the rejections guard.
    let yaml = pipeline_with_envelope_config(
        "strategy: concat\nfooter:\n  interchange:\n    n: count()\n    total: sum(amount)",
    );
    let config = parse_config(&yaml).expect("parse footer pipeline");
    config
        .compile(&CompileContext::default())
        .expect("count / sum footer aggregates compile clean");
}

#[test]
fn header_referencing_a_body_field_is_e353() {
    // A header field reads the body column `amount`. The header is emitted
    // before the body streams, so this is rejected with E353 — pin the code.
    let yaml = pipeline_with_envelope_config(
        "strategy: concat\nheader:\n  interchange:\n    total: amount",
    );
    let config = parse_config(&yaml).expect("parse header pipeline");
    let err = config
        .compile(&CompileContext::default())
        .expect_err("a body-field reference in a header must fail to compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E353")
        .unwrap_or_else(|| panic!("expected an E353 diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("amount"),
        "E353 must name the offending body column: {}",
        diag.message
    );
}

#[test]
fn header_reading_vars_compiles_clean() {
    // A header reading only `$vars` (a document-open-knowable input) is the
    // intended header shape and compiles without diagnostics — the control
    // proving E353 fires on the body reference, not on header synthesis itself.
    let yaml = pipeline_with_envelope_config(
        "strategy: preserve\nheader:\n  interchange:\n    sender: $vars.sender_id",
    );
    let config = parse_config(&yaml).expect("parse header-vars pipeline");
    config
        .compile(&CompileContext::default())
        .expect("a $vars-only header compiles clean");
}

#[test]
fn footer_collect_is_e354() {
    // `collect(amount)` is a holistic aggregate — it accumulates every value,
    // not an O(1) fold — so it is rejected as a non-streaming footer aggregate.
    let yaml = pipeline_with_envelope_config(
        "strategy: concat\nfooter:\n  interchange:\n    items: collect(amount)",
    );
    let config = parse_config(&yaml).expect("parse collect-footer pipeline");
    let err = config
        .compile(&CompileContext::default())
        .expect_err("a holistic footer aggregate must fail to compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E354")
        .unwrap_or_else(|| panic!("expected an E354 diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("count / sum / avg / min / max"),
        "E354 must name the streaming allow-list: {}",
        diag.message
    );
}

#[test]
fn footer_median_is_a_compile_error() {
    // `median` is not a CXL aggregate function at all, so it fails typechecking
    // as an unknown function (surfaced as E200) — a compile error either way,
    // satisfying the "median(x) is a compile error" acceptance without a
    // special case in the allow-list.
    let yaml = pipeline_with_envelope_config(
        "strategy: concat\nfooter:\n  interchange:\n    mid: median(amount)",
    );
    let config = parse_config(&yaml).expect("parse median-footer pipeline");
    let err = config
        .compile(&CompileContext::default())
        .expect_err("median is not a CXL aggregate and must fail to compile");
    assert!(
        err.iter().any(|d| d.code == "E200" || d.code == "E354"),
        "median must surface a compile diagnostic (unknown function), got: {err:?}"
    );
}

#[test]
fn footer_composed_argument_is_e354() {
    // `sum(amount * 1.1)` is a composed argument — the footer fold supports only
    // a bare field or `*`, so the composed argument is rejected with E354.
    let yaml = pipeline_with_envelope_config(
        "strategy: concat\nfooter:\n  interchange:\n    gross: sum(amount * 2)",
    );
    let config = parse_config(&yaml).expect("parse composed-arg footer pipeline");
    let err = config
        .compile(&CompileContext::default())
        .expect_err("a composed aggregate argument must fail to compile");
    assert!(
        err.iter().any(|d| d.code == "E354"),
        "a composed footer argument must surface E354, got: {err:?}"
    );
}
