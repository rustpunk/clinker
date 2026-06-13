//! Plan-time validation for the Cull node's `bind_schema` guards.
//!
//! Each guard rejects a malformed Cull at compile time with an `E200`
//! diagnostic. Because every guard shares the `E200` code, these tests pin
//! the distinctive message substring each one produces, so a future edit
//! that swaps two messages — or drops a guard entirely — fails here rather
//! than letting a malformed pipeline plan silently.
//!
//! Guards covered (one negative path each):
//! - `partition_by` names a field absent from the upstream schema.
//! - `order_by` names a field absent from the upstream schema.
//! - `removed_to` is empty.
//! - `removed_to` collides with the node's own name (the main output port).
//! - `drop_group_when` references a field absent from the upstream schema.

use crate::config::{CompileContext, parse_config};

/// Compile `yaml`, expecting failure, and return `(code, message)` pairs.
fn compile_diagnostics(yaml: &str) -> Vec<(String, String)> {
    let config = parse_config(yaml).expect("parse_config");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("compile should fail");
    diags
        .iter()
        .map(|d| (d.code.clone(), d.message.clone()))
        .collect()
}

/// Assert that some emitted `E200` carries every substring in `needles`.
fn assert_e200_contains(diags: &[(String, String)], needles: &[&str]) {
    let found = diags
        .iter()
        .filter(|(code, _)| code == "E200")
        .any(|(_, msg)| needles.iter().all(|n| msg.contains(n)));
    assert!(
        found,
        "expected an E200 containing all of {needles:?}, got {diags:?}"
    );
}

/// Wrap a cull `config:` block in a source → cull → output pipeline whose
/// upstream schema is `id: string, amount: int, status: string` and whose
/// partition key is `id`. The main output draws from `cd` (bare) and the
/// side output from `cd.removed` — but malformed fixtures here fail at
/// bind time before the side-output wiring matters.
fn pipeline_with_cull_config(cull_config: &str) -> String {
    format!(
        r#"
pipeline:
  name: cull_validation
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: amount, type: int }}
        - {{ name: status, type: string }}
  - type: cull
    name: cd
    input: src
    config:
{cull_config}
  - type: output
    name: out
    input: cd
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: cd.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#
    )
}

#[test]
fn partition_by_unknown_field_rejected() {
    let yaml = pipeline_with_cull_config(
        r#"      partition_by: [nonexistent]
      removed_to: removed
      rules:
        - name: drop_errors
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0""#,
    );
    let diags = compile_diagnostics(&yaml);
    assert_e200_contains(
        &diags,
        &[
            "partition_by field",
            "nonexistent",
            "not present in the upstream schema",
        ],
    );
}

#[test]
fn order_by_unknown_field_rejected() {
    let yaml = pipeline_with_cull_config(
        r#"      partition_by: [id]
      order_by:
        - { field: nonexistent, order: asc }
      removed_to: removed
      rules:
        - name: drop_errors
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0""#,
    );
    let diags = compile_diagnostics(&yaml);
    assert_e200_contains(
        &diags,
        &[
            "order_by field",
            "nonexistent",
            "not present in the upstream schema",
        ],
    );
}

#[test]
fn empty_removed_to_rejected() {
    let yaml = pipeline_with_cull_config(
        r#"      partition_by: [id]
      removed_to: ""
      rules:
        - name: drop_errors
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0""#,
    );
    let diags = compile_diagnostics(&yaml);
    assert_e200_contains(
        &diags,
        &["removed_to must name a non-empty side-output port"],
    );
}

#[test]
fn removed_to_colliding_with_node_name_rejected() {
    // `removed_to: cd` collides with the node's own name (the main output
    // port), so the two output ports would be indistinguishable.
    let yaml = pipeline_with_cull_config(
        r#"      partition_by: [id]
      removed_to: cd
      rules:
        - name: drop_errors
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0""#,
    );
    let diags = compile_diagnostics(&yaml);
    assert_e200_contains(
        &diags,
        &[
            "removed_to",
            "collides with the node's own name",
            "must be distinguishable",
        ],
    );
}

#[test]
fn drop_group_when_unknown_field_rejected() {
    // The predicate references a column absent from the upstream schema, so
    // the aggregate-context typecheck fails.
    let yaml = pipeline_with_cull_config(
        r#"      partition_by: [id]
      removed_to: removed
      rules:
        - name: drop_errors
          drop_group_when: "sum(nonexistent) > 0""#,
    );
    let diags = compile_diagnostics(&yaml);
    // The typecheck reports the unknown field; the exact wording comes from
    // the CXL resolver, so pin the field name and the node label.
    let found = diags
        .iter()
        .any(|(code, msg)| code == "E200" && msg.contains("nonexistent"));
    assert!(
        found,
        "expected an E200 naming the unknown field `nonexistent`, got {diags:?}"
    );
}

#[test]
fn empty_rules_rejected() {
    // A Cull with no removal rule never removes anything — a no-op two-port
    // operator, almost certainly an authoring mistake — so it is rejected.
    let yaml = pipeline_with_cull_config(
        r#"      partition_by: [id]
      removed_to: removed
      rules: []"#,
    );
    let diags = compile_diagnostics(&yaml);
    assert_e200_contains(&diags, &["rules must contain at least one removal rule"]);
}

#[test]
fn well_formed_cull_compiles() {
    // The happy path: a valid Cull with an aggregate `drop_group_when`
    // predicate compiles, and both output ports wire (main → `out`, removed
    // → `audit`).
    let yaml = pipeline_with_cull_config(
        r#"      partition_by: [id]
      removed_to: removed
      rules:
        - name: drop_errors
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0""#,
    );
    let config = parse_config(&yaml).expect("parse_config");
    config
        .compile(&CompileContext::default())
        .expect("a well-formed Cull pipeline must compile");
}

#[test]
fn string_and_count_predicates_typecheck() {
    // Ordered aggregate-boolean predicates over non-numeric aggregates
    // (`max(status) > 'error'`) and over `count(*)` both typecheck — the
    // aggregate residual evaluator orders strings the same way the row
    // evaluator does, so these are well-formed group-level predicates.
    let yaml = pipeline_with_cull_config(
        r#"      partition_by: [id]
      removed_to: removed
      rules:
        - name: lexical
          drop_group_when: "max(status) > 'error'"
        - name: big
          drop_group_when: "count(*) > 3""#,
    );
    let config = parse_config(&yaml).expect("parse_config");
    config
        .compile(&CompileContext::default())
        .expect("string and count aggregate predicates must typecheck");
}
