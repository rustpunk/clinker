//! Integration tests for the provenance and resource subsystems (Phase 16c.3).

use clinker_core::config::{LayerKind, ResolvedValue};
use clinker_core::span::Span;

fn test_span() -> Span {
    Span::SYNTHETIC
}

#[test]
fn scaffold_compiles() {
    // Gate test for Task 16c.3.0: proves the test harness links against clinker-core.
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

    // CompositionDefault should not be the winner.
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

    // ChannelDefault should exist but not be the winner.
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
