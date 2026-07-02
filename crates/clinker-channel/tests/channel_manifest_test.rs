//! Parse coverage for the channel-centric overlay serde types (issue #517).
//!
//! Pins the on-the-wire YAML surface for `channel.cfg.yaml` manifests
//! ([`ChannelManifest`]) and per-target overlay files ([`OverlayFile`]).
//! These types are parse-only; layer resolution and override application
//! live in later stages, so the assertions here stay at the deserialized
//! shape: header fields, label order/values, the four var scopes, and the
//! opaque `overrides` list.

use std::path::PathBuf;

use clinker_channel::{ChannelManifest, OverlayFile};
use clinker_plan::config::ScopedVarType;

fn manifest(yaml: &[u8]) -> ChannelManifest {
    ChannelManifest::from_yaml_bytes(yaml, PathBuf::from("channel.cfg.yaml"))
        .expect("manifest YAML parses")
}

fn overlay(yaml: &[u8], name: &str) -> OverlayFile {
    OverlayFile::from_yaml_bytes(yaml, PathBuf::from(name)).expect("overlay YAML parses")
}

#[test]
fn parses_full_manifest() {
    let m = manifest(
        br#"
channel:
  name: globex
labels: { region: west, tier: enterprise }
config:
  fraud_check.threshold: 0.9
vars:
  static:
    currency: { type: string, default: "USD" }
  pipeline:
    retries: { type: int }
  source:
    orders:
      cutoff: { type: int, default: 100 }
  record:
    tag: { type: string, default: "batch" }
overrides:
  - { op: add, composition: ./composition/fraud.comp.yaml, alias: fraud }
"#,
    );

    assert_eq!(m.channel.name, "globex");

    // Labels preserve declared order and scalar values.
    let labels: Vec<(&str, &serde_json::Value)> =
        m.labels.iter().map(|(k, v)| (k.as_str(), v)).collect();
    assert_eq!(labels[0].0, "region");
    assert_eq!(labels[0].1, &serde_json::json!("west"));
    assert_eq!(labels[1].0, "tier");
    assert_eq!(labels[1].1, &serde_json::json!("enterprise"));

    // Channel-wide config keeps raw dotted-path keys.
    assert_eq!(
        m.config.get("fraud_check.threshold"),
        Some(&serde_json::json!(0.9))
    );

    // All four var scopes populate.
    assert_eq!(
        m.vars.static_scope["currency"].var_type,
        ScopedVarType::String
    );
    assert_eq!(
        m.vars.static_scope["currency"].default,
        Some(serde_json::json!("USD"))
    );
    assert_eq!(m.vars.pipeline["retries"].var_type, ScopedVarType::Int);
    assert!(m.vars.pipeline["retries"].default.is_none());
    assert_eq!(
        m.vars.source["orders"]["cutoff"].var_type,
        ScopedVarType::Int
    );
    assert_eq!(m.vars.record["tag"].var_type, ScopedVarType::String);

    // `overrides` stays opaque — a list of maps, uninterpreted here.
    assert_eq!(m.overrides.len(), 1);
    assert_eq!(m.overrides[0]["op"], serde_json::json!("add"));
    assert_eq!(m.overrides[0]["alias"], serde_json::json!("fraud"));
}

#[test]
fn parses_minimal_manifest() {
    // Only the header is required; every other block defaults to empty.
    let m = manifest(
        br#"
channel:
  name: acme
"#,
    );
    assert_eq!(m.channel.name, "acme");
    assert!(m.labels.is_empty());
    assert!(m.config.is_empty());
    assert!(m.vars.static_scope.is_empty());
    assert!(m.vars.pipeline.is_empty());
    assert!(m.vars.source.is_empty());
    assert!(m.vars.record.is_empty());
    assert!(m.overrides.is_empty());
}

#[test]
fn manifest_rejects_unknown_top_level_field() {
    let err = ChannelManifest::from_yaml_bytes(
        br#"
channel:
  name: acme
lables: { region: west }
"#,
        PathBuf::from("channel.cfg.yaml"),
    );
    assert!(err.is_err(), "typo'd top-level key must be rejected");
}

#[test]
fn manifest_labels_round_trip() {
    let m = manifest(
        br#"
channel:
  name: globex
labels: { region: west, tier: enterprise, shard: "07" }
"#,
    );

    // Serialize the parsed manifest and read it back; labels must survive
    // with identical order and values.
    let json = serde_json::to_string(&m).expect("manifest serializes");
    let round: ChannelManifest = serde_json::from_str(&json).expect("manifest re-parses");

    let before: Vec<(&String, &serde_json::Value)> = m.labels.iter().collect();
    let after: Vec<(&String, &serde_json::Value)> = round.labels.iter().collect();
    assert_eq!(before, after);
    assert_eq!(
        after[2],
        (&"shard".to_string(), &serde_json::json!("07")),
        "quoted scalar label round-trips as a string, order preserved"
    );
}

#[test]
fn parses_full_overlay() {
    let o = overlay(
        br#"
channel:
  target: ../../pipeline/order_fulfillment.yaml
config:
  fraud_check.threshold: 0.95
vars:
  static:
    currency: { type: string, default: "EUR" }
overrides:
  - { op: set, target: route_priority, field: config.cxl, value: "emit _route = a" }
"#,
        "order_fulfillment.channel.yaml",
    );

    assert_eq!(o.channel.target, "../../pipeline/order_fulfillment.yaml");
    assert_eq!(
        o.config.get("fraud_check.threshold"),
        Some(&serde_json::json!(0.95))
    );
    assert_eq!(
        o.vars.static_scope["currency"].var_type,
        ScopedVarType::String
    );
    assert_eq!(o.overrides.len(), 1);
    assert_eq!(o.overrides[0]["op"], serde_json::json!("set"));
}

#[test]
fn overlay_target_is_authoritative_over_filename() {
    // Identical body under each of the three filename forms; the parsed
    // target comes from the YAML, so it is identical across all of them.
    let body = br#"
channel:
  target: ../../pipeline/order_fulfillment.yaml
config: {}
"#;

    let as_channel = overlay(body, "anything.channel.yaml");
    let as_comp = overlay(body, "anything.comp.yaml");
    let as_bare = overlay(body, "anything.yaml");

    assert_eq!(
        as_channel.channel.target,
        "../../pipeline/order_fulfillment.yaml"
    );
    assert_eq!(as_comp.channel.target, as_channel.channel.target);
    assert_eq!(as_bare.channel.target, as_channel.channel.target);

    // Even when the filename stem disagrees with the target stem, the YAML
    // wins: parsing does not derive the target from the filename.
    let mismatched = overlay(
        br#"
channel:
  target: ../../composition/tax_calc.comp.yaml
"#,
        "order_fulfillment.channel.yaml",
    );
    assert_eq!(
        mismatched.channel.target,
        "../../composition/tax_calc.comp.yaml"
    );
}

#[test]
fn overlay_rejects_unknown_top_level_field() {
    let err = OverlayFile::from_yaml_bytes(
        br#"
channel:
  target: ./pipeline.yaml
labels: { region: west }
"#,
        PathBuf::from("pipeline.channel.yaml"),
    );
    assert!(
        err.is_err(),
        "overlay files carry no `labels:` block — labels live on the manifest"
    );
}

#[test]
fn overlay_overrides_stay_opaque() {
    // A structurally rich op list parses as opaque values without this layer
    // asserting any op vocabulary (that is CH-10's job).
    let o = overlay(
        br#"
channel:
  target: ./pipeline.yaml
overrides:
  - { op: add, node: { type: transform, name: stamp, input: src }, after: src }
  - { op: patch_schema, target: orders, add: [ { name: tax_exempt, type: bool } ] }
  - { op: bypass, target: legacy_audit }
"#,
        "pipeline.channel.yaml",
    );

    assert_eq!(o.overrides.len(), 3);
    assert!(o.overrides[0].is_object());
    assert_eq!(o.overrides[1]["op"], serde_json::json!("patch_schema"));
    assert_eq!(o.overrides[2]["target"], serde_json::json!("legacy_audit"));
}
