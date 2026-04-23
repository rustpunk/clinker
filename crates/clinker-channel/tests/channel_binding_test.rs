use clinker_channel::binding::validate_channel_bindings;
use clinker_channel::{ChannelBinding, ChannelTarget, DottedPath};
use std::path::PathBuf;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("clinker-core/tests/fixtures")
}

fn channel_fixtures_dir() -> PathBuf {
    fixtures_dir().join("channels")
}

/// Build a symbol table from the test fixtures workspace.
fn build_symbol_table() -> clinker_core::config::composition::CompositionSymbolTable {
    use clinker_core::config::composition::scan_workspace_signatures;
    let ws_root = fixtures_dir();
    let table = scan_workspace_signatures(&ws_root).expect("fixture scan should succeed");
    std::sync::Arc::try_unwrap(table).unwrap_or_else(|arc| (*arc).clone())
}

// ── Channel binding parse tests ─────────────────────────────────────────

#[test]
fn test_channel_binding_deserializes_pipeline_target() {
    let yaml = br#"
channel:
  name: acme_prod
  target: ./pipelines/channel_target_pipeline.yaml
config:
  default:
    enrich1.fuzzy_threshold: 0.85
  fixed:
    enrich1.lookup_table: "s3://acme/lookups/prod.csv"
"#;
    let binding =
        ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("test.channel.yaml")).unwrap();
    assert_eq!(binding.name, "acme_prod");
    assert!(matches!(binding.target, ChannelTarget::Pipeline(_)));
    if let ChannelTarget::Pipeline(p) = &binding.target {
        assert_eq!(
            p,
            &PathBuf::from("./pipelines/channel_target_pipeline.yaml")
        );
    }
    assert_eq!(binding.config_default.len(), 1);
    assert_eq!(binding.config_fixed.len(), 1);
    assert!(
        binding
            .config_default
            .contains_key(&DottedPath::try_from("enrich1.fuzzy_threshold").unwrap())
    );
    assert!(
        binding
            .config_fixed
            .contains_key(&DottedPath::try_from("enrich1.lookup_table").unwrap())
    );
}

#[test]
fn test_channel_binding_deserializes_composition_target() {
    let yaml = br#"
channel:
  name: comp_direct
  target: ./compositions/customer_enrich.comp.yaml
config:
  fixed:
    fuzzy_threshold: 0.95
"#;
    let binding =
        ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("comp.channel.yaml")).unwrap();
    assert_eq!(binding.name, "comp_direct");
    assert!(matches!(binding.target, ChannelTarget::Composition(_)));
    if let ChannelTarget::Composition(p) = &binding.target {
        assert_eq!(
            p,
            &PathBuf::from("./compositions/customer_enrich.comp.yaml")
        );
    }
    assert!(binding.config_default.is_empty());
    assert_eq!(binding.config_fixed.len(), 1);
    assert!(
        binding
            .config_fixed
            .contains_key(&DottedPath::try_from("fuzzy_threshold").unwrap())
    );
}

#[test]
fn test_channel_binding_rejects_multi_segment_dotted_path() {
    let yaml = br#"
channel:
  name: bad
  target: ./pipelines/main.yaml
config:
  default:
    enrich1.fuzzy.nested: 0.5
"#;
    let result = ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("bad.channel.yaml"));
    assert!(result.is_err(), "3-segment dotted path should be rejected");
    let err = result.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("at most 2 segments"),
        "error should mention segment limit, got: {msg}"
    );
}

#[test]
fn test_channel_hash_is_deterministic() {
    let yaml = br#"
channel:
  name: test
  target: ./pipelines/main.yaml
config:
  default:
    threshold: 0.5
"#;
    let b1 = ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("a.channel.yaml")).unwrap();
    let b2 = ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("b.channel.yaml")).unwrap();
    assert_eq!(b1.channel_hash, b2.channel_hash);
    assert_ne!(b1.channel_hash, [0u8; 32], "hash must not be zero");
}

// ── DottedPath validation tests ─────────────────────────────────────────

#[test]
fn test_dotted_path_rejects_empty() {
    assert!(DottedPath::try_from("").is_err());
}

#[test]
fn test_dotted_path_rejects_leading_dot() {
    assert!(DottedPath::try_from(".foo").is_err());
}

#[test]
fn test_dotted_path_rejects_trailing_dot() {
    assert!(DottedPath::try_from("foo.").is_err());
}

#[test]
fn test_dotted_path_rejects_consecutive_dots() {
    assert!(DottedPath::try_from("foo..bar").is_err());
}

#[test]
fn test_dotted_path_accepts_single_segment() {
    let p = DottedPath::try_from("threshold").unwrap();
    assert_eq!(p.segments(), (None, "threshold"));
}

#[test]
fn test_dotted_path_accepts_two_segments() {
    let p = DottedPath::try_from("enrich1.fuzzy_threshold").unwrap();
    assert_eq!(p.segments(), (Some("enrich1"), "fuzzy_threshold"));
}

// ── Fixture parse tests ────────────────────────────────────────────────

#[test]
fn test_all_six_fixtures_parse() {
    let expected = [
        "acme_prod.channel.yaml",
        "acme_staging.channel.yaml",
        "beta_prod.channel.yaml",
        "beta_staging.channel.yaml",
        "comp_direct.channel.yaml",
        "empty_defaults.channel.yaml",
    ];

    for name in &expected {
        let path = channel_fixtures_dir().join(name);
        let binding = ChannelBinding::load(&path)
            .unwrap_or_else(|e| panic!("fixture {name} should parse: {e}"));
        assert!(!binding.name.is_empty(), "{name}: name should not be empty");
        assert_ne!(
            binding.channel_hash, [0u8; 32],
            "{name}: hash must not be zero"
        );
    }
}

#[test]
fn test_empty_defaults_fixture_has_no_fixed() {
    let path = channel_fixtures_dir().join("empty_defaults.channel.yaml");
    let binding = ChannelBinding::load(&path).unwrap();
    assert!(binding.config_fixed.is_empty());
    assert!(!binding.config_default.is_empty());
}

#[test]
fn test_comp_direct_fixture_has_no_default() {
    let path = channel_fixtures_dir().join("comp_direct.channel.yaml");
    let binding = ChannelBinding::load(&path).unwrap();
    assert!(binding.config_default.is_empty());
    assert!(!binding.config_fixed.is_empty());
    assert!(matches!(binding.target, ChannelTarget::Composition(_)));
}

// ── Channel validation tests ────────────────────────────────────────────

#[test]
fn test_channel_validation_emits_e105_for_undeclared_config_key() {
    let yaml = br#"
channel:
  name: bad_keys
  target: ./compositions/customer_enrich.comp.yaml
config:
  default:
    nonexistent_param: 0.5
"#;
    let binding = ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("bad.channel.yaml")).unwrap();
    let symbol_table = build_symbol_table();
    let ws_root = fixtures_dir();

    let diags = validate_channel_bindings(&[binding], &symbol_table, &ws_root);
    assert!(!diags.is_empty(), "should emit diagnostics");
    assert!(
        diags.iter().any(|d| d.code == "E105"),
        "should contain E105 diagnostic, got: {diags:?}"
    );
}

#[test]
fn test_channel_validation_emits_e103_for_unknown_target() {
    let yaml = br#"
channel:
  name: missing_target
  target: ./compositions/does_not_exist.comp.yaml
config:
  default:
    foo: bar
"#;
    let binding = ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("bad.channel.yaml")).unwrap();
    let symbol_table = build_symbol_table();
    let ws_root = fixtures_dir();

    let diags = validate_channel_bindings(&[binding], &symbol_table, &ws_root);
    assert!(!diags.is_empty(), "should emit diagnostics");
    assert!(
        diags.iter().any(|d| d.code == "E103"),
        "should contain E103 diagnostic, got: {diags:?}"
    );
}

#[test]
fn test_channel_validation_accepts_all_six_fixtures() {
    let symbol_table = build_symbol_table();
    let ws_root = fixtures_dir();

    // Load only the composition-targeting fixture (comp_direct) since
    // pipeline-targeting fixtures can't be validated against the symbol
    // table. Pipeline-target path validation requires the actual files
    // to exist on disk, which they do in the fixtures dir.
    let mut bindings = Vec::new();
    let expected = [
        "acme_prod.channel.yaml",
        "acme_staging.channel.yaml",
        "beta_prod.channel.yaml",
        "beta_staging.channel.yaml",
        "comp_direct.channel.yaml",
        "empty_defaults.channel.yaml",
    ];
    for name in &expected {
        let path = channel_fixtures_dir().join(name);
        bindings.push(ChannelBinding::load(&path).unwrap());
    }

    let diags = validate_channel_bindings(&bindings, &symbol_table, &ws_root);
    assert!(
        diags.is_empty(),
        "all 6 fixtures should validate cleanly, got: {diags:?}"
    );
}
