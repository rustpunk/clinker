//! Integration tests for the provenance and resource subsystems (Phase 16c.3).

use std::path::PathBuf;

use clinker_core::config::{CompileContext, LayerKind, ProvenanceLayer, ResolvedValue};
use clinker_core::span::Span;

fn test_span() -> Span {
    Span::SYNTHETIC
}

fn fixture_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

fn parse_pipeline(yaml: &str) -> clinker_core::config::PipelineConfig {
    clinker_core::yaml::from_str(yaml).expect("parse PipelineConfig")
}

#[test]
fn scaffold_compiles() {
    assert!(true);
}

// ---------------------------------------------------------------------
// Task 16c.3.2 gate tests — ResolvedValue + ProvenanceLayer
// ---------------------------------------------------------------------

#[test]
fn test_resolved_value_initial_layer_wins() {
    let rv = ResolvedValue::new(42, LayerKind::CompositionDefault, test_span());
    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::CompositionDefault);
    assert!(winner.won);
    assert_eq!(rv.value, 42);
}

#[test]
fn test_apply_layer_channel_fixed_wins_over_composition_default() {
    let mut rv = ResolvedValue::new(10, LayerKind::CompositionDefault, test_span());
    rv.apply_layer(99, LayerKind::ChannelFixed, test_span());

    assert_eq!(rv.value, 99, "ChannelFixed value should win");
    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::ChannelFixed);

    let comp = rv
        .provenance
        .iter()
        .find(|l| l.kind == LayerKind::CompositionDefault)
        .expect("CompositionDefault layer must still exist");
    assert!(!comp.won);
}

#[test]
fn test_apply_layer_channel_default_does_not_win_over_fixed() {
    let mut rv = ResolvedValue::new(10, LayerKind::ChannelFixed, test_span());
    rv.apply_layer(20, LayerKind::ChannelDefault, test_span());

    assert_eq!(rv.value, 10, "ChannelFixed value should be retained");
    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::ChannelFixed);

    let ch_default = rv
        .provenance
        .iter()
        .find(|l| l.kind == LayerKind::ChannelDefault)
        .expect("ChannelDefault layer must exist");
    assert!(!ch_default.won);
}

#[test]
fn test_resolved_value_provenance_chain_bounded_at_4() {
    let mut rv = ResolvedValue::new(1, LayerKind::CompositionDefault, test_span());
    rv.apply_layer(2, LayerKind::ChannelDefault, test_span());
    rv.apply_layer(3, LayerKind::ChannelFixed, test_span());
    rv.apply_layer(4, LayerKind::InspectorEdit, test_span());

    assert_eq!(rv.provenance.len(), 4, "exactly 4 layers, one per kind");
    assert_eq!(rv.value, 4, "InspectorEdit value should win");

    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::InspectorEdit);

    // Re-apply same kind: should replace in place, not push.
    rv.apply_layer(5, LayerKind::InspectorEdit, test_span());
    assert_eq!(
        rv.provenance.len(),
        4,
        "still 4 layers after same-kind re-apply"
    );
    assert_eq!(rv.value, 5, "replaced value should be updated");
}

#[test]
fn test_apply_layer_inspector_edit_wins_over_channel_fixed() {
    let mut rv = ResolvedValue::new(10, LayerKind::ChannelFixed, test_span());
    rv.apply_layer(99, LayerKind::InspectorEdit, test_span());

    assert_eq!(
        rv.value, 99,
        "InspectorEdit value should win over ChannelFixed"
    );
    let winner = rv.winning_layer().expect("must have a winner");
    assert_eq!(winner.kind, LayerKind::InspectorEdit);
}

// ---------------------------------------------------------------------
// Task 16c.3.3 gate tests — bind_schema integration + size budget
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
    assert_eq!(winner.kind, LayerKind::CompositionDefault);
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
        "ProvenanceLayer must fit in 64 bytes (D-H.5 / LD-16c-11), got {}",
        std::mem::size_of::<ProvenanceLayer>()
    );
}
