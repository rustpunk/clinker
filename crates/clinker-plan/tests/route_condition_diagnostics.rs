//! Bind-time diagnostics for malformed Route branch conditions.
//!
//! A Route branch condition is a CXL boolean expression. It is type-checked
//! during schema binding — the same stage that validates a Combine `where:`
//! predicate — so a malformed condition surfaces as a span-anchored compile
//! diagnostic at the Route node, before any I/O, rather than as a late
//! runtime `PipelineError::Compilation` at run start. These tests pin that
//! parity across the three failure classes the binder distinguishes (parse,
//! name resolution, type) plus the non-literal-`$doc`-index case, and confirm
//! a well-typed condition still compiles unchanged.
//!
//! The executor does not re-compile route conditions at startup, so plan-time
//! binding is the sole place these are diagnosed: a regression that dropped
//! the bind-time check would let a malformed condition slip to run start with
//! no source span.

use clinker_core_types::Diagnostic;
use clinker_core_types::span::Span;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::plan::CompiledPlan;

/// Compile a single-branch Route pipeline whose branch condition is supplied
/// by the caller. The upstream CSV source declares an `int amount`, so a
/// condition referencing `amount` is well-formed; anything else the caller
/// passes exercises a specific failure class. Returns the compile result so a
/// test can assert success or inspect the diagnostics.
///
/// The condition is embedded in a single-quoted YAML scalar so a CXL string
/// literal (double quotes) inside it survives the YAML parse and reaches the
/// binder verbatim.
fn compile_route_condition(condition: &str) -> Result<CompiledPlan, Vec<Diagnostic>> {
    let yaml = format!(
        r#"
pipeline:
  name: route_cond_diag
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: amount, type: int }}
  - type: route
    name: split
    input: orders
    config:
      mode: exclusive
      conditions:
        big: '{condition}'
      default: small
  - type: output
    name: big_out
    input: split.big
    config:
      name: big_out
      type: csv
      path: big.csv
  - type: output
    name: small_out
    input: split.small
    config:
      name: small_out
      type: csv
      path: small.csv
"#
    );
    let config = parse_config(&yaml).expect("parse route-condition pipeline");
    config.compile(&CompileContext::default())
}

/// Like [`compile_route_condition`], but the upstream XML source declares an
/// envelope section `Head { total: int }`, so a branch condition may read
/// `$doc.Head.total`. This variant exercises the `$doc`-path walk that a route
/// condition feeds — the same walk that rejects a non-literal envelope index.
fn compile_route_condition_with_doc(condition: &str) -> Result<CompiledPlan, Vec<Diagnostic>> {
    let yaml = format!(
        r#"
pipeline:
  name: route_cond_doc
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          Head:
            extract: {{ xml_path: "/doc/Head" }}
            fields:
              total: int
      schema:
        - {{ name: amount, type: int }}
  - type: route
    name: split
    input: payments
    config:
      mode: exclusive
      conditions:
        big: '{condition}'
      default: small
  - type: output
    name: big_out
    input: split.big
    config:
      name: big_out
      type: csv
      path: big.csv
  - type: output
    name: small_out
    input: split.small
    config:
      name: small_out
      type: csv
      path: small.csv
"#
    );
    let config = parse_config(&yaml).expect("parse route-condition doc pipeline");
    config.compile(&CompileContext::default())
}

#[test]
fn route_condition_parse_error_is_diagnosed_at_bind_time() {
    // A syntactically invalid condition (`amount >` has no right operand) is a
    // valid YAML scalar but not a valid CXL predicate. The binder must reject
    // it at compile time with a branch-qualified E200 anchored at the Route
    // node's source line — not defer it to run start.
    let err = compile_route_condition("amount >")
        .expect_err("a route condition that fails to parse must fail the compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E200" && d.message.contains("parse error"))
        .unwrap_or_else(|| panic!("expected an E200 parse-error diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("branch big"),
        "the diagnostic must name the offending branch, got: {}",
        diag.message
    );
    assert!(
        diag.primary.span.synthetic_line_number().is_some(),
        "the diagnostic must anchor at the Route node's source line, not a synthetic span"
    );
}

#[test]
fn route_condition_unknown_field_is_diagnosed_at_bind_time() {
    // A condition that references a field the upstream schema does not declare
    // fails name resolution. The binder must surface that as a branch-qualified
    // E200 naming the unresolved identifier, anchored at the Route node.
    let err = compile_route_condition("nonexistent_field > 100")
        .expect_err("a route condition referencing an unknown field must fail the compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E200" && d.message.contains("name resolution"))
        .unwrap_or_else(|| panic!("expected an E200 name-resolution diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("nonexistent_field") && diag.message.contains("branch big"),
        "the diagnostic must name the unresolved field and the offending branch, got: {}",
        diag.message
    );
    assert!(
        diag.primary.span.synthetic_line_number().is_some(),
        "the diagnostic must anchor at the Route node's source line, not a synthetic span"
    );
}

#[test]
fn route_condition_type_error_is_diagnosed_at_bind_time() {
    // A well-formed, resolvable condition that is ill-typed (comparing the
    // `int` field `amount` against a string literal) fails type checking. The
    // binder must surface a branch-qualified E200 anchored at the Route node.
    let err = compile_route_condition("amount > \"text\"")
        .expect_err("an ill-typed route condition must fail the compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E200" && d.message.contains("type error"))
        .unwrap_or_else(|| panic!("expected an E200 type-error diagnostic, got: {err:?}"));
    assert!(
        diag.message.contains("branch big"),
        "the diagnostic must name the offending branch, got: {}",
        diag.message
    );
    assert!(
        diag.primary.span.synthetic_line_number().is_some(),
        "the diagnostic must anchor at the Route node's source line, not a synthetic span"
    );
}

#[test]
fn route_condition_non_literal_doc_index_aborts_with_e340() {
    // Indexing a `$doc` access by a record field (`$doc.Head.total[amount]`)
    // leaves the declared document path unresolvable at compile time. A route
    // condition must abort the compile with the same E340 fail-fast — carrying
    // help text and anchored at a real source line — that a Transform body or
    // Combine predicate already uses for the equivalent case.
    let err = compile_route_condition_with_doc("amount > $doc.Head.total[amount]")
        .expect_err("a non-literal `$doc` index in a route condition must fail the compile");
    let diag = err
        .iter()
        .find(|d| d.code == "E340")
        .unwrap_or_else(|| panic!("expected an E340 diagnostic, got: {err:?}"));
    assert_ne!(
        diag.primary.span,
        Span::SYNTHETIC,
        "E340 primary span must not be the synthetic sentinel"
    );
    assert!(
        diag.primary.span.synthetic_line_number().is_some(),
        "E340 primary span must carry the offending node's source line"
    );
    assert!(diag.help.is_some(), "E340 must carry help text");
}

#[test]
fn well_typed_route_condition_compiles() {
    // Control: a well-typed condition against the declared schema compiles
    // cleanly — the bind-time check rejects only malformed conditions.
    compile_route_condition("amount > 100").expect("a well-typed route condition must compile");
}

#[test]
fn well_typed_doc_route_condition_compiles() {
    // Control: a route condition reading a fully-declared, literally-indexed
    // envelope path compiles — the E340 guard fires on the non-literal index
    // alone, never on a legitimate `$doc` read.
    compile_route_condition_with_doc("amount > $doc.Head.total")
        .expect("a well-typed `$doc` route condition must compile");
}
