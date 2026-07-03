//! Parse coverage for the per-target overlay `sources:` config-patch block.
//!
//! These tests pin the on-the-wire YAML surface for the per-source patch a
//! channel's per-target overlay carries: the schema column ops (`add` /
//! `rename` / `modify` / `remove`), the `array_paths` ops (set-by-path /
//! `remove`), and the scalar `options` map. Applying the parsed patch to a
//! pipeline is covered in `clinker-plan`; here we only assert that every
//! documented form deserializes into the typed [`SourceConfigPatch`] carried on
//! the overlay.

use std::path::PathBuf;

use clinker_channel::OverlayFile;
use clinker_plan::config::{ArrayPathOp, SchemaColumnOp};

fn overlay(yaml: &[u8]) -> OverlayFile {
    OverlayFile::from_yaml_bytes(yaml, PathBuf::from("base.channel.yaml"))
        .expect("overlay YAML parses")
}

#[test]
fn parses_full_sources_patch_block() {
    let o = overlay(
        br#"
channel:
  target: ../../pipeline/base.yaml
sources:
  transactions:
    schema:
      amount:      { type: float }
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

    let patch = o
        .sources
        .get("transactions")
        .expect("transactions patch present");

    // Schema ops: one of each form, in declared order.
    let schema: Vec<(&String, &SchemaColumnOp)> = patch.schema.iter().collect();
    assert_eq!(schema.len(), 4);
    assert!(matches!(schema[0].1, SchemaColumnOp::Modify(_)));
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
    let o = overlay(
        br#"
channel:
  target: ../../pipeline/base.yaml
"#,
    );
    assert!(o.sources.is_empty());
}

#[test]
fn multiple_sources_each_carry_their_patch() {
    let o = overlay(
        br#"
channel:
  target: ../../pipeline/base.yaml
sources:
  orders:
    schema:
      id: { type: int }
  customers:
    options:
      delimiter: "|"
"#,
    );
    assert_eq!(o.sources.len(), 2);
    assert!(o.sources.contains_key("orders"));
    assert!(o.sources.contains_key("customers"));
}

#[test]
fn schema_add_carries_long_unique_flag() {
    let o = overlay(
        br#"
channel:
  target: ../../pipeline/base.yaml
sources:
  src:
    schema:
      uuid: { add: { type: string, long_unique: true } }
"#,
    );
    match o.sources["src"].schema.get("uuid") {
        Some(SchemaColumnOp::Add(add)) => assert_eq!(add.long_unique, Some(true)),
        other => panic!("expected add op, got {other:?}"),
    }
}

#[test]
fn unknown_op_key_is_a_parse_error() {
    // A schema op map with an unrecognized key is rejected by the overlay
    // parser (deny_unknown_fields on the op payload).
    let err = OverlayFile::from_yaml_bytes(
        br#"
channel:
  target: ../../pipeline/base.yaml
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
