//! Parse coverage for the channel `sources:` config-patch block (issue #550).
//!
//! These tests pin the on-the-wire YAML surface for the per-source patch: the
//! schema column ops (`add` / `rename` / `retype` / `remove`), the
//! `array_paths` ops (set-by-path / `remove`), and the scalar `options` map.
//! Applying the parsed patch to a pipeline is covered in `clinker-plan`; here
//! we only assert that every documented form deserializes into the typed
//! [`SourceConfigPatch`] carried on the binding.

use std::path::PathBuf;

use clinker_channel::binding::ChannelBinding;
use clinker_plan::config::{ArrayPathOp, SchemaColumnOp};

fn binding(yaml: &[u8]) -> ChannelBinding {
    ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("test.channel.yaml"))
        .expect("channel YAML parses")
}

#[test]
fn parses_full_sources_patch_block() {
    let b = binding(
        br#"
channel:
  name: acme
  target: ./pipeline.yaml
sources:
  transactions:
    schema:
      amount:      { retype: float }
      cust_id:     { rename: customer_id }
      order_notes: remove
      region:      { add: { type: string } }
    array_paths:
      items:      { mode: join, separator: ";" }
      line_items: remove
    options:
      record_path: batch_records
"#,
    );

    let patch = b
        .source_patches
        .get("transactions")
        .expect("transactions patch present");

    // Schema ops: one of each form, in declared order.
    let schema: Vec<(&String, &SchemaColumnOp)> = patch.schema.iter().collect();
    assert_eq!(schema.len(), 4);
    assert!(matches!(schema[0].1, SchemaColumnOp::Retype(_)));
    assert!(matches!(schema[1].1, SchemaColumnOp::Rename(to) if to == "customer_id"));
    assert!(matches!(schema[2].1, SchemaColumnOp::Remove));
    assert!(matches!(schema[3].1, SchemaColumnOp::Add(_)));

    // array_paths ops: a set-by-path and a remove.
    assert!(matches!(
        patch.array_paths.get("items"),
        Some(ArrayPathOp::Set { separator: Some(s), .. }) if s == ";"
    ));
    assert!(matches!(
        patch.array_paths.get("line_items"),
        Some(ArrayPathOp::Remove)
    ));

    // options: one scalar key.
    assert_eq!(
        patch.options.get("record_path").and_then(|v| v.as_str()),
        Some("batch_records")
    );
}

#[test]
fn absent_sources_block_is_empty() {
    let b = binding(
        br#"
channel:
  name: plain
  target: ./pipeline.yaml
"#,
    );
    assert!(b.source_patches.is_empty());
}

#[test]
fn multiple_sources_each_carry_their_patch() {
    let b = binding(
        br#"
channel:
  name: multi
  target: ./pipeline.yaml
sources:
  orders:
    schema:
      id: { retype: int }
  customers:
    options:
      delimiter: "|"
"#,
    );
    assert_eq!(b.source_patches.len(), 2);
    assert!(b.source_patches.contains_key("orders"));
    assert!(b.source_patches.contains_key("customers"));
}

#[test]
fn schema_add_carries_long_unique_flag() {
    let b = binding(
        br#"
channel:
  name: lu
  target: ./pipeline.yaml
sources:
  src:
    schema:
      uuid: { add: { type: string, long_unique: true } }
"#,
    );
    match b.source_patches["src"].schema.get("uuid") {
        Some(SchemaColumnOp::Add(add)) => assert!(add.long_unique),
        other => panic!("expected add op, got {other:?}"),
    }
}

#[test]
fn unknown_op_key_is_a_parse_error() {
    // A schema op map with an unrecognized key is rejected by the binding
    // parser (deny_unknown_fields on the op payload).
    let err = ChannelBinding::from_yaml_bytes(
        br#"
channel:
  name: bad
  target: ./pipeline.yaml
sources:
  src:
    schema:
      amount: { bogus: 1 }
"#,
        PathBuf::from("bad.channel.yaml"),
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("YAML parse error"), "{msg}");
}
