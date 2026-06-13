//! Plan-time validation for the Reshape node's `bind_schema` guards.
//!
//! Each guard rejects a malformed rule at compile time with an `E200`
//! diagnostic. Because every guard shares the `E200` code, these tests
//! pin the distinctive message substring each one produces, so a future
//! edit that swaps two messages — or drops a guard entirely — fails here
//! rather than letting a malformed pipeline plan silently.
//!
//! Guards covered (one negative path each):
//! - `partition_by` names a field absent from the upstream schema.
//! - `order_by` names a field absent from the upstream schema.
//! - `mutate.set` writes a `partition_by` field (group identity must
//!   survive Reshape).
//! - `mutate.set` writes an engine-stamped `$`-prefixed column.
//! - `mutate.set` targets a column absent from the upstream schema
//!   (Reshape mutates existing columns, never adds new ones).
//! - `synthesize` with `copy_from: none` fails to override every column.

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

/// Wrap a reshape `config:` block in a source → reshape → output pipeline
/// whose upstream schema is `id: string, amount: int, label: string` and
/// whose partition key is `id`.
fn pipeline_with_reshape_config(reshape_config: &str) -> String {
    format!(
        r#"
pipeline:
  name: reshape_validation
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
        - {{ name: label, type: string }}
  - type: reshape
    name: rs
    input: src
    config:
{reshape_config}
  - type: output
    name: out
    input: rs
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

#[test]
fn partition_by_unknown_field_rejected() {
    let yaml = pipeline_with_reshape_config(
        r#"      partition_by: [nonexistent]
      rules:
        - name: r
          when: "amount > 0"
          mutate:
            set:
              label: "'x'""#,
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
    let yaml = pipeline_with_reshape_config(
        r#"      partition_by: [id]
      order_by:
        - { field: nonexistent, order: asc }
      rules:
        - name: r
          when: "amount > 0"
          mutate:
            set:
              label: "'x'""#,
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
fn mutate_set_partition_key_rejected() {
    let yaml = pipeline_with_reshape_config(
        r#"      partition_by: [id]
      rules:
        - name: r
          when: "amount > 0"
          mutate:
            set:
              id: "'reassigned'""#,
    );
    let diags = compile_diagnostics(&yaml);
    assert_e200_contains(
        &diags,
        &[
            "mutate.set may not write",
            "partition_by field",
            "group identity must survive",
        ],
    );
}

#[test]
fn mutate_set_engine_stamped_column_rejected() {
    let yaml = pipeline_with_reshape_config(
        r#"      partition_by: [id]
      rules:
        - name: r
          when: "amount > 0"
          mutate:
            set:
              $meta.synthetic: "true""#,
    );
    let diags = compile_diagnostics(&yaml);
    assert_e200_contains(
        &diags,
        &["mutate.set may not write the engine-stamped column"],
    );
}

#[test]
fn mutate_set_unknown_target_rejected() {
    let yaml = pipeline_with_reshape_config(
        r#"      partition_by: [id]
      rules:
        - name: r
          when: "amount > 0"
          mutate:
            set:
              nonexistent: "'x'""#,
    );
    let diags = compile_diagnostics(&yaml);
    assert_e200_contains(
        &diags,
        &["mutate.set target", "nonexistent", "does not add new ones"],
    );
}

#[test]
fn synthesize_copy_from_none_missing_override_rejected() {
    // `copy_from: none` starts all-null, so every user column (`amount`,
    // `label`) must appear in `overrides`. Here only `amount` is supplied,
    // so `label` is reported missing.
    let yaml = pipeline_with_reshape_config(
        r#"      partition_by: [id]
      rules:
        - name: r
          when: "amount > 0"
          synthesize:
            copy_from: none
            overrides:
              id: "id"
              amount: "amount""#,
    );
    let diags = compile_diagnostics(&yaml);
    assert_e200_contains(
        &diags,
        &["copy_from: none", "must override every column", "label"],
    );
}

#[test]
fn doc_context_reference_in_rule_rejected() {
    // Reshape spills per-group input records to disk under memory pressure and
    // re-runs the rules on reload, but the spill round-trip does not carry
    // document envelope context, so a `$doc` reference would resolve to null
    // for a spilled group and to the real envelope for a resident one —
    // output that depends on the memory budget. The bind-time guard rejects
    // any `$doc` reference in a `when` / `mutate.set` / `synthesize` fragment
    // until envelope context survives the spill round-trip.
    //
    // The source declares an XML envelope so `$doc.BatchInfo.batch_id`
    // type-resolves; the guard fires on the resolved reference, not a parse
    // error.
    let yaml = r#"
pipeline:
  name: reshape_doc_guard
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
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
            fields:
              batch_id: string
      schema:
        - { name: account, type: string }
        - { name: amount, type: int }
        - { name: note, type: string }
  - type: reshape
    name: rs
    input: payments
    config:
      partition_by: [account]
      rules:
        - name: stamp_batch
          when: "amount > 0"
          mutate:
            set:
              note: "$doc.BatchInfo.batch_id"
  - type: output
    name: out
    input: rs
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let diags = compile_diagnostics(yaml);
    assert_e200_contains(
        &diags,
        &[
            "references `$doc` document context",
            "Reshape spills per-group state",
            "current limitation",
        ],
    );
}
