//! Parse coverage for the `group/*.group.yaml` serde model (issue #518, CH-3).
//!
//! These tests pin the on-the-wire YAML surface for a group: the
//! `group.name` / `group.match` / `group.priority` header, the value-clobber
//! `config:` / `vars:` surfaces, and the verbatim `overrides:` op list. The
//! `match:` selector is a CXL boolean string (compiled elsewhere) and the
//! `overrides:` entries are preserved raw here — this stage is parse only.
//!
//! The four-scope `vars:` surface reuses the channel overlay's `ChannelVars`
//! type, so the static scope is read via `vars.static_scope`.

use std::path::PathBuf;

use clinker_channel::Group;
use clinker_plan::config::ScopedVarType;

fn group(yaml: &[u8]) -> Group {
    Group::from_yaml_bytes(yaml, PathBuf::from("test.group.yaml")).expect("group YAML parses")
}

#[test]
fn no_selector_group_defaults_priority_and_has_no_match() {
    // A selector-less group is explicit-only: `match` is absent, and with
    // `priority` also absent the default (0) is applied.
    let g = group(
        br#"
group:
  name: nightly
"#,
    );

    assert_eq!(g.name, "nightly");
    assert_eq!(g.selector, None, "absent `match` parses as None");
    assert_eq!(g.priority, 0, "absent `priority` defaults to 0");
    assert!(g.config.is_empty());
    assert!(g.overrides.is_empty());
    assert!(g.vars.static_scope.is_empty());
    assert!(g.vars.pipeline.is_empty());
    assert!(g.vars.source.is_empty());
    assert!(g.vars.record.is_empty());
}

#[test]
fn multi_key_match_selector_is_preserved_verbatim() {
    // A `match` referencing several label keys is stored as the raw CXL string.
    let expr = r#"tier == "enterprise" && region == "west""#;
    let g = group(
        br#"
group:
  name: enterprise_west
  match: 'tier == "enterprise" && region == "west"'
  priority: 30
"#,
    );

    assert_eq!(g.selector.as_deref(), Some(expr));
    assert_eq!(g.priority, 30);
}

#[test]
fn explicit_priority_overrides_default() {
    let g = group(
        br#"
group:
  name: high
  priority: 100
"#,
    );
    assert_eq!(g.priority, 100);
    assert_eq!(g.selector, None);
}

#[test]
fn negative_priority_is_accepted() {
    // `priority` is a signed i64; a group can deliberately sit below the
    // unprioritized baseline.
    let g = group(
        br#"
group:
  name: fallback
  priority: -5
"#,
    );
    assert_eq!(g.priority, -5);
}

#[test]
fn full_group_parses_all_surfaces() {
    let g = group(
        br#"
group:
  name: enterprise
  match: 'tier == "enterprise"'
  priority: 20
config:
  fraud_check.threshold: 0.9
  region: west
vars:
  static:
    currency: { type: string, default: "USD" }
  pipeline:
    batch_size: { type: int, default: 500 }
  source:
    orders:
      cutoff: { type: date }
  record:
    seen: { type: bool, default: false }
overrides:
  - op: add
    composition: ../composition/fraud_check.comp.yaml
    alias: fraud_check
    after: normalize_fields
    config: { threshold: 0.80 }
  - op: set
    target: route_priority
    field: config.cxl
    value: "emit x = 1"
"#,
    );

    assert_eq!(g.name, "enterprise");
    assert_eq!(g.selector.as_deref(), Some(r#"tier == "enterprise""#));
    assert_eq!(g.priority, 20);

    // Value-clobber config keeps dotted keys as raw strings.
    assert_eq!(
        g.config
            .get("fraud_check.threshold")
            .and_then(|v| v.as_f64()),
        Some(0.9)
    );
    assert_eq!(
        g.config.get("region").and_then(|v| v.as_str()),
        Some("west")
    );

    // Vars parse into ScopedVarDecl across all four scopes.
    let currency = &g.vars.static_scope["currency"];
    assert_eq!(currency.var_type, ScopedVarType::String);
    assert_eq!(
        currency.default.as_ref().and_then(|v| v.as_str()),
        Some("USD")
    );
    assert_eq!(g.vars.pipeline["batch_size"].var_type, ScopedVarType::Int);
    assert_eq!(
        g.vars.source["orders"]["cutoff"].var_type,
        ScopedVarType::Date
    );
    assert_eq!(g.vars.record["seen"].var_type, ScopedVarType::Bool);

    // Overrides are preserved verbatim, in declaration order.
    assert_eq!(g.overrides.len(), 2);
    assert_eq!(
        g.overrides[0].get("op").and_then(|v| v.as_str()),
        Some("add")
    );
    assert_eq!(
        g.overrides[0].get("alias").and_then(|v| v.as_str()),
        Some("fraud_check")
    );
    assert_eq!(
        g.overrides[1].get("op").and_then(|v| v.as_str()),
        Some("set")
    );
}

#[test]
fn unknown_top_level_key_is_a_parse_error() {
    // deny_unknown_fields rejects a stray top-level key (e.g. a typo of
    // `overrides`), rather than silently dropping it.
    let err = Group::from_yaml_bytes(
        br#"
group:
  name: typo
overides: []
"#,
        PathBuf::from("bad.group.yaml"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("YAML parse error"), "{err}");
}

#[test]
fn unknown_group_header_key_is_a_parse_error() {
    let err = Group::from_yaml_bytes(
        br#"
group:
  name: bad
  weight: 5
"#,
        PathBuf::from("bad.group.yaml"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("YAML parse error"), "{err}");
}

#[test]
fn missing_name_is_a_parse_error() {
    let err = Group::from_yaml_bytes(
        br#"
group:
  priority: 10
"#,
        PathBuf::from("bad.group.yaml"),
    )
    .unwrap_err();
    assert!(err.to_string().contains("YAML parse error"), "{err}");
}
