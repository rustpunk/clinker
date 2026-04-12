use std::path::PathBuf;

use clinker_channel::binding::ChannelBinding;
use clinker_channel::overlay::apply_channel_overlay;
use clinker_core::config::composition::LayerKind;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("clinker-core/tests/fixtures")
}

fn channel_fixtures_dir() -> PathBuf {
    fixtures_dir().join("channels")
}

/// Compile a pipeline fixture into a CompiledPlan for overlay testing.
fn compile_fixture(rel_pipeline_path: &str) -> clinker_core::plan::CompiledPlan {
    let root = fixtures_dir();
    let yaml_path = root.join(rel_pipeline_path);
    let yaml = std::fs::read_to_string(&yaml_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", yaml_path.display()));
    let config: clinker_core::config::PipelineConfig =
        clinker_core::yaml::from_str(&yaml).expect("parse pipeline YAML");
    let pipeline_dir = PathBuf::from(rel_pipeline_path)
        .parent()
        .unwrap_or(std::path::Path::new(""))
        .to_path_buf();
    let ctx = clinker_core::config::CompileContext::with_pipeline_dir(&root, pipeline_dir);
    clinker_core::config::PipelineConfig::compile(&config, &ctx).expect("compile fixture")
}

// ── Gate tests for Task 16c.4.3 ─────────────────────────────────────────

#[test]
fn test_channel_overlay_applies_fixed_binding_wins() {
    // nested_composition_pipeline has composition `nested_process` with
    // `strict_mode: false` at the call site (CompositionDefault).
    let mut plan = compile_fixture("pipelines/nested_composition_pipeline.yaml");

    // Verify provenance exists before overlay
    assert!(
        plan.provenance()
            .get("nested_process", "strict_mode")
            .is_some(),
        "nested_process.strict_mode should exist in provenance before overlay"
    );

    let yaml = br#"
channel:
  name: test_fixed
  target: ./pipelines/nested_composition_pipeline.yaml
config:
  fixed:
    nested_process.strict_mode: true
"#;
    let binding =
        ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("test.channel.yaml")).unwrap();

    let _diags = apply_channel_overlay(&mut plan, &binding);

    let resolved = plan
        .provenance()
        .get("nested_process", "strict_mode")
        .expect("nested_process.strict_mode should exist in provenance");

    let winner = resolved.winning_layer().expect("should have a winner");
    assert_eq!(
        winner.kind,
        LayerKind::ChannelFixed,
        "ChannelFixed should win over CompositionDefault"
    );
    assert_eq!(
        resolved.value,
        serde_json::json!(true),
        "value should be updated to channel fixed value"
    );
}

#[test]
fn test_channel_overlay_default_does_not_override_fixed() {
    let mut plan = compile_fixture("pipelines/nested_composition_pipeline.yaml");

    // First apply a ChannelFixed layer
    let fixed_yaml = br#"
channel:
  name: test_fixed_first
  target: ./pipelines/nested_composition_pipeline.yaml
config:
  fixed:
    nested_process.strict_mode: true
"#;
    let fixed_binding =
        ChannelBinding::from_yaml_bytes(fixed_yaml, PathBuf::from("fixed.channel.yaml")).unwrap();
    apply_channel_overlay(&mut plan, &fixed_binding);

    // Now apply a ChannelDefault layer — it should NOT override the fixed winner
    let default_yaml = br#"
channel:
  name: test_default_after
  target: ./pipelines/nested_composition_pipeline.yaml
config:
  default:
    nested_process.strict_mode: false
"#;
    let default_binding =
        ChannelBinding::from_yaml_bytes(default_yaml, PathBuf::from("default.channel.yaml"))
            .unwrap();
    apply_channel_overlay(&mut plan, &default_binding);

    let resolved = plan
        .provenance()
        .get("nested_process", "strict_mode")
        .expect("nested_process.strict_mode should exist");

    let winner = resolved.winning_layer().expect("should have a winner");
    assert_eq!(
        winner.kind,
        LayerKind::ChannelFixed,
        "ChannelFixed should still win after ChannelDefault applied"
    );
    assert_eq!(
        resolved.value,
        serde_json::json!(true),
        "value should remain at the ChannelFixed value"
    );
}

#[test]
fn test_channel_identity_stable_across_reruns() {
    let mut plan1 = compile_fixture("pipelines/nested_composition_pipeline.yaml");
    let mut plan2 = compile_fixture("pipelines/nested_composition_pipeline.yaml");

    let yaml = br#"
channel:
  name: stable_test
  target: ./pipelines/nested_composition_pipeline.yaml
config:
  default:
    nested_process.strict_mode: true
"#;
    let binding1 = ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("a.channel.yaml")).unwrap();
    let binding2 = ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("b.channel.yaml")).unwrap();

    apply_channel_overlay(&mut plan1, &binding1);
    apply_channel_overlay(&mut plan2, &binding2);

    let id1 = plan1
        .channel_identity()
        .expect("should have channel identity after overlay");
    let id2 = plan2
        .channel_identity()
        .expect("should have channel identity after overlay");

    assert_eq!(id1.content_hash, id2.content_hash);
    assert_eq!(id1.name, id2.name);
    assert_ne!(id1.content_hash, [0u8; 32]);
}

#[test]
fn test_channel_overlay_applies_all_six_fixtures_without_errors() {
    // The 6 channel fixtures target various pipelines. For overlay testing,
    // we apply each to a compiled plan. Some fixture keys may not match
    // provenance entries (warnings are OK), but no errors should occur.
    //
    // Pipeline targets:
    // - acme_prod/staging/empty_defaults → channel_target_pipeline.yaml
    //   (no compositions, so no provenance entries — all keys emit warnings)
    // - beta_prod/staging → composition_pipeline.yaml
    //   (enrich1 has validation errors so no provenance for it; addr_norm has provenance)
    // - comp_direct → targets composition directly, not a pipeline
    //
    // We apply each to nested_composition_pipeline which has provenance for
    // nested_process.strict_mode. Keys won't match but that's OK — no errors.

    let all_fixtures = [
        "acme_prod.channel.yaml",
        "acme_staging.channel.yaml",
        "beta_prod.channel.yaml",
        "beta_staging.channel.yaml",
        "comp_direct.channel.yaml",
        "empty_defaults.channel.yaml",
    ];

    for channel_name in &all_fixtures {
        let channel_path = channel_fixtures_dir().join(channel_name);
        let binding = ChannelBinding::load(&channel_path)
            .unwrap_or_else(|e| panic!("load {channel_name}: {e}"));

        let mut plan = compile_fixture("pipelines/nested_composition_pipeline.yaml");
        let diags = apply_channel_overlay(&mut plan, &binding);

        // Channel identity should be stamped
        assert!(
            plan.channel_identity().is_some(),
            "{channel_name}: should have channel identity after overlay"
        );

        // No error-severity diagnostics
        let errors: Vec<_> = diags
            .iter()
            .filter(|d| matches!(d.severity, clinker_core::error::Severity::Error))
            .collect();
        assert!(
            errors.is_empty(),
            "{channel_name}: should have no errors, got: {errors:?}"
        );
    }
}

// ── Additional overlay tests ────────────────────────────────────────────

#[test]
fn test_channel_identity_is_none_before_overlay() {
    let plan = compile_fixture("pipelines/nested_composition_pipeline.yaml");
    assert!(
        plan.channel_identity().is_none(),
        "channel_identity should be None for base compiled plan"
    );
}
