use std::path::PathBuf;

use clinker_channel::binding::ChannelBinding;
use clinker_channel::overlay::apply_channel_overlay;
use clinker_plan::config::composition::LayerKind;

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("clinker-exec/tests/fixtures")
}

fn channel_fixtures_dir() -> PathBuf {
    fixtures_dir().join("channels")
}

/// Compile a pipeline fixture into (config, plan) for overlay testing.
fn compile_fixture(
    rel_pipeline_path: &str,
) -> (
    clinker_plan::config::PipelineConfig,
    clinker_plan::plan::CompiledPlan,
) {
    let root = fixtures_dir();
    let yaml_path = root.join(rel_pipeline_path);
    let yaml = std::fs::read_to_string(&yaml_path)
        .unwrap_or_else(|e| panic!("read {}: {e}", yaml_path.display()));
    let config: clinker_plan::config::PipelineConfig =
        clinker_plan::yaml::from_str(&yaml).expect("parse pipeline YAML");
    let pipeline_dir = PathBuf::from(rel_pipeline_path)
        .parent()
        .unwrap_or(std::path::Path::new(""))
        .to_path_buf();
    let ctx = clinker_plan::config::CompileContext::with_pipeline_dir(&root, pipeline_dir);
    let plan =
        clinker_plan::config::PipelineConfig::compile(&config, &ctx).expect("compile fixture");
    (config, plan)
}

// ── Channel overlay merge tests ─────────────────────────────────────────

#[test]
fn test_channel_overlay_applies_fixed_binding_wins() {
    // nested_composition_pipeline has composition `nested_process` with
    // `strict_mode: false` at the call site (PipelineDefault).
    let (config, mut plan) = compile_fixture("pipelines/nested_composition_pipeline.yaml");

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

    let _result = apply_channel_overlay(&mut plan, &binding, &config);

    let resolved = plan
        .provenance()
        .get("nested_process", "strict_mode")
        .expect("nested_process.strict_mode should exist in provenance");

    let winner = resolved.winning_layer().expect("should have a winner");
    assert_eq!(
        winner.kind,
        LayerKind::ChannelPerTarget,
        "fixed ChannelPerTarget should win over PipelineDefault"
    );
    assert!(
        winner.fixed,
        "the winning layer should carry the fixed lock from config.fixed"
    );
    assert_eq!(
        resolved.value,
        serde_json::json!(true),
        "value should be updated to channel fixed value"
    );
}

#[test]
fn test_channel_overlay_default_does_not_override_fixed() {
    // `config.default` and `config.fixed` land on the same ChannelPerTarget
    // layer; the split maps to the layer's `fixed` lock. When a key appears in
    // both, the fixed value wins — it locks against the default within the
    // layer, regardless of which was authored first.
    let (config, mut plan) = compile_fixture("pipelines/nested_composition_pipeline.yaml");

    let yaml = br#"
channel:
  name: test_default_and_fixed
  target: ./pipelines/nested_composition_pipeline.yaml
config:
  default:
    nested_process.strict_mode: false
  fixed:
    nested_process.strict_mode: true
"#;
    let binding =
        ChannelBinding::from_yaml_bytes(yaml, PathBuf::from("both.channel.yaml")).unwrap();
    apply_channel_overlay(&mut plan, &binding, &config);

    let resolved = plan
        .provenance()
        .get("nested_process", "strict_mode")
        .expect("nested_process.strict_mode should exist");

    let winner = resolved.winning_layer().expect("should have a winner");
    assert_eq!(
        winner.kind,
        LayerKind::ChannelPerTarget,
        "the fixed ChannelPerTarget value should win over the default"
    );
    assert!(
        winner.fixed,
        "the winning layer should carry the fixed lock"
    );
    assert_eq!(
        resolved.value,
        serde_json::json!(true),
        "value should remain at the fixed value, not the default"
    );
}

#[test]
fn test_channel_identity_stable_across_reruns() {
    let (config, mut plan1) = compile_fixture("pipelines/nested_composition_pipeline.yaml");
    let (_, mut plan2) = compile_fixture("pipelines/nested_composition_pipeline.yaml");

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

    apply_channel_overlay(&mut plan1, &binding1, &config);
    apply_channel_overlay(&mut plan2, &binding2, &config);

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
fn test_channel_overlay_unmatched_keys_are_hard_errors() {
    // The 6 channel fixtures each carry `config` keys targeting other pipelines.
    // Applied to nested_composition_pipeline — whose only tracked parameter is
    // `nested_process.strict_mode` — none of their keys match. Promoting the
    // former W103 warning to the E113 hard error means every unmatched key now
    // aborts the run instead of silently no-op'ing. Identity is still stamped so
    // downstream diagnostics can name the channel.

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

        let (config, mut plan) = compile_fixture("pipelines/nested_composition_pipeline.yaml");
        let result = apply_channel_overlay(&mut plan, &binding, &config);

        // Channel identity is stamped regardless of overlay diagnostics.
        assert!(
            plan.channel_identity().is_some(),
            "{channel_name}: should have channel identity after overlay"
        );

        // Every unmatched config key is an E113 hard error; no other error
        // codes are expected from these config-only, pipeline-target channels.
        let errors: Vec<_> = result
            .diagnostics
            .iter()
            .filter(|d| matches!(d.severity, clinker_core_types::Severity::Error))
            .collect();
        assert!(
            !errors.is_empty(),
            "{channel_name}: unmatched config keys must produce E113 errors"
        );
        assert!(
            errors.iter().all(|d| d.code == "E113"),
            "{channel_name}: config-key mismatches must all be E113, got: {errors:?}"
        );
    }
}

// ── Additional overlay tests ────────────────────────────────────────────

#[test]
fn test_channel_identity_is_none_before_overlay() {
    let (_, plan) = compile_fixture("pipelines/nested_composition_pipeline.yaml");
    assert!(
        plan.channel_identity().is_none(),
        "channel_identity should be None for base compiled plan"
    );
}
