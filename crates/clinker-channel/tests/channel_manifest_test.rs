//! Wire-format contract for catalog-scoped channel resources.

use std::path::PathBuf;

use clinker_channel::{ChannelManifest, OverlayFile};
use clinker_plan::config::ScopedVarType;

fn parse_manifest(yaml: &[u8]) -> ChannelManifest {
    ChannelManifest::from_yaml_bytes(yaml, PathBuf::from("channel.cfg.yaml"))
        .expect("manifest parses")
}

#[test]
fn fixed_leaf_values_parse_with_authored_spans_and_default_false() {
    let manifest = parse_manifest(
        br#"
channel:
  name: tenant.acme
  targets: [sales.orders]
config:
  fraud.threshold: { value: 0.9, fixed: true }
  fraud.mode: { value: strict }
vars:
  static:
    currency: { type: string, default: USD, fixed: true }
  pipeline:
    retries: { type: int, default: 3 }
"#,
    );

    assert_eq!(manifest.channel.name, "tenant.acme");
    assert_eq!(manifest.channel.targets[0].value, "sales.orders");

    let threshold = &manifest.config["fraud.threshold"];
    assert_eq!(threshold.value, serde_json::json!(0.9));
    assert!(threshold.fixed);
    assert_eq!(threshold.value_span.line(), 6);
    assert_eq!(threshold.fixed_span.expect("fixed span").line(), 6);

    let mode = &manifest.config["fraud.mode"];
    assert_eq!(mode.value, serde_json::json!("strict"));
    assert!(!mode.fixed, "fixed defaults to false at every leaf");

    let currency = &manifest.vars.static_scope["currency"];
    assert_eq!(currency.var_type, ScopedVarType::String);
    assert_eq!(currency.default, Some(serde_json::json!("USD")));
    assert!(currency.fixed);
    assert_eq!(currency.fixed_span.expect("fixed span").line(), 10);

    assert!(!manifest.vars.pipeline["retries"].fixed);
}

#[test]
fn fixed_unknown_or_misplaced_keys_fail_at_the_authored_input() {
    let unknown = ChannelManifest::from_yaml_bytes(
        br#"
channel:
  name: tenant.acme
  targets: [sales.orders]
config:
  fraud.threshold: { value: 0.9, fiexed: true }
"#,
        PathBuf::from("channel.cfg.yaml"),
    )
    .expect_err("unknown leaf keys must fail closed");
    let unknown = unknown.to_string();
    assert!(unknown.contains("fiexed"), "{unknown}");
    assert!(
        unknown.contains("fixed"),
        "diagnostic should name the corrected key: {unknown}"
    );

    let misplaced = ChannelManifest::from_yaml_bytes(
        br#"
channel:
  name: tenant.acme
  targets: [sales.orders]
config:
  fraud.threshold: 0.9
fixed:
  fraud.threshold: true
"#,
        PathBuf::from("channel.cfg.yaml"),
    )
    .expect_err("a sibling fixed block is not a second public syntax");
    let misplaced = misplaced.to_string();
    assert!(misplaced.contains("fixed"), "{misplaced}");
    assert!(
        misplaced.contains("value"),
        "diagnostic should show the leaf form: {misplaced}"
    );
}

#[test]
fn channel_manifest_rejects_empty_or_omitted_targets() {
    for yaml in [
        br#"channel: { name: tenant.acme, targets: [] }"#.as_slice(),
        br#"channel: { name: tenant.acme }"#.as_slice(),
    ] {
        let error = ChannelManifest::from_yaml_bytes(yaml, PathBuf::from("channel.cfg.yaml"))
            .expect_err("target scope is mandatory and non-empty")
            .to_string();
        assert!(error.contains("targets"), "{error}");
        assert!(error.contains("pipeline"), "{error}");
    }
}

#[test]
fn channel_wide_manifest_rejects_graph_and_source_operations() {
    for (field, body) in [
        ("overrides", "overrides: [{ op: bypass, target: audit }]"),
        (
            "sources",
            "sources: { orders: { options: { delimiter: '|' } } }",
        ),
    ] {
        let yaml = format!("channel:\n  name: tenant.acme\n  targets: [sales.orders]\n{body}\n");
        let error =
            ChannelManifest::from_yaml_bytes(yaml.as_bytes(), PathBuf::from("channel.cfg.yaml"))
                .expect_err("channel-wide graph/source operations are forbidden")
                .to_string();
        assert!(error.contains(field), "{error}");
        assert!(
            error.contains("target"),
            "diagnostic should direct the author to a target file: {error}"
        );
    }
}

#[test]
fn target_file_uses_a_logical_pipeline_identity_not_a_path() {
    let overlay = OverlayFile::from_yaml_bytes(
        br#"
channel:
  target: sales.orders
config:
  fraud.threshold: { value: 0.95 }
overrides:
  - { op: bypass, target: legacy_audit }
"#,
        PathBuf::from("orders.yaml"),
    )
    .expect("target overlay parses");

    assert_eq!(overlay.channel.target, "sales.orders");
    assert_eq!(
        overlay.config["fraud.threshold"].value,
        serde_json::json!(0.95)
    );
    assert_eq!(overlay.overrides.len(), 1);
}
