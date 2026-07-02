//! Integration tests for the provenance and resource subsystems.

use std::path::PathBuf;

use clinker_core_types::span::Span;
use clinker_plan::config::{CompileContext, LayerKind, ProvenanceLayer, ResolvedValue};

fn test_span() -> Span {
    Span::SYNTHETIC
}

fn fixture_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn parse_pipeline(yaml: &str) -> clinker_plan::config::PipelineConfig {
    clinker_plan::yaml::from_str(yaml).expect("parse PipelineConfig")
}

// ---------------------------------------------------------------------
// ResolvedValue + ProvenanceLayer tests
// ---------------------------------------------------------------------

#[test]
fn test_resolved_value_initial_layer_wins() {
    let rv = ResolvedValue::new(42, LayerKind::PipelineDefault, test_span());
    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::PipelineDefault);
    assert!(winner.won);
    assert_eq!(rv.value, 42);
}

#[test]
fn test_apply_layer_fixed_channel_wins_over_pipeline_default() {
    let mut rv = ResolvedValue::new(10, LayerKind::PipelineDefault, test_span());
    rv.apply_layer_fixed(99, LayerKind::ChannelPerTarget, test_span());

    assert_eq!(rv.value, 99, "fixed ChannelPerTarget value should win");
    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::ChannelPerTarget);

    let base = rv
        .provenance
        .iter()
        .find(|l| l.kind == LayerKind::PipelineDefault)
        .expect("PipelineDefault layer must still exist");
    assert!(!base.won);
}

#[test]
fn test_non_fixed_higher_does_not_override_fixed_lower() {
    // A `fixed` lower layer locks its value against every higher-precedence
    // layer applied afterwards, regardless of application order.
    let mut rv = ResolvedValue::new_fixed(10, LayerKind::PipelineDefault, test_span());
    rv.apply_layer(20, LayerKind::ChannelPerTarget, test_span());

    assert_eq!(
        rv.value, 10,
        "fixed PipelineDefault value should be retained"
    );
    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::PipelineDefault);

    let higher = rv
        .provenance
        .iter()
        .find(|l| l.kind == LayerKind::ChannelPerTarget)
        .expect("ChannelPerTarget layer must exist");
    assert!(!higher.won);
}

#[test]
fn test_provenance_chain_accumulates_distinct_layers() {
    // The chain is no longer capped at a fixed count: it holds one entry per
    // distinct layer, and a same-kind re-apply replaces in place.
    let mut rv = ResolvedValue::new(1, LayerKind::PipelineDefault, test_span());
    rv.apply_layer(
        2,
        LayerKind::Group {
            priority: 10,
            seq: 0,
        },
        test_span(),
    );
    rv.apply_layer(3, LayerKind::ChannelWide, test_span());
    rv.apply_layer(4, LayerKind::ChannelPerTarget, test_span());

    assert_eq!(
        rv.provenance.len(),
        4,
        "four distinct layers, one entry each"
    );
    assert_eq!(
        rv.value, 4,
        "highest-precedence ChannelPerTarget value wins"
    );

    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::ChannelPerTarget);

    // Re-apply same kind: should replace in place, not push.
    rv.apply_layer(5, LayerKind::ChannelPerTarget, test_span());
    assert_eq!(
        rv.provenance.len(),
        4,
        "still four layers after same-kind re-apply"
    );
    assert_eq!(rv.value, 5, "replaced value should be updated");
}

#[test]
fn test_channel_per_target_wins_over_channel_wide() {
    let mut rv = ResolvedValue::new(10, LayerKind::ChannelWide, test_span());
    rv.apply_layer(99, LayerKind::ChannelPerTarget, test_span());

    assert_eq!(
        rv.value, 99,
        "ChannelPerTarget value should win over ChannelWide"
    );
    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::ChannelPerTarget);
}

// ---------------------------------------------------------------------
// bind_schema integration + size-budget tests
// ---------------------------------------------------------------------

#[test]
fn test_compiled_plan_composition_params_carry_provenance() {
    let root = fixture_workspace_root();
    let yaml_path = root.join("pipelines/nested_composition_pipeline.yaml");
    let yaml = std::fs::read_to_string(&yaml_path).expect("read fixture");
    let cfg = parse_pipeline(&yaml);
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));

    let plan = cfg
        .compile(&ctx)
        .expect("nested composition pipeline should compile");

    let prov = plan.provenance();

    // The fixture has composition node "nested_process" calling nested_caller
    // with config: strict_mode: false. The signature declares strict_mode
    // (type: bool, default: false).
    let entry = prov
        .get("nested_process", "strict_mode")
        .expect("nested_process.strict_mode must have provenance");

    assert_eq!(entry.value, serde_json::json!(false));
    let winner = entry.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::PipelineDefault);
    assert!(winner.won);

    // Verify provenance is non-empty overall.
    assert!(
        !prov.is_empty(),
        "provenance table should have entries for composition config params"
    );
}

#[test]
fn test_resolved_value_size_within_budget() {
    // Belt-and-suspenders runtime check (mirrors compile-time assert in provenance.rs).
    assert!(
        std::mem::size_of::<ProvenanceLayer>() <= 64,
        "ProvenanceLayer must fit in 64 bytes (cache-line budget), got {}",
        std::mem::size_of::<ProvenanceLayer>()
    );
}
