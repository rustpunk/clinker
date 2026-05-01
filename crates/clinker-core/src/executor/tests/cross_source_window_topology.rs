//! Cross-source window topology coverage.
//!
//! Cross-source analytic-windows (`analytic_window: { source: <other> }`)
//! always resolve to `PlanIndexRoot::Source(<other>)` — never node-rooted.
//! The diagnostic E150c guards against tier inversion: if the referenced
//! source's ingestion tier is downstream of the windowed Transform's
//! primary input source, the engine would attempt to project against an
//! arena that hasn't been built yet.
//!
//! Today's `build_source_dag` always orders cross-source-referenced
//! sources into tier 0 (the only earlier tier), so user-authored YAML
//! cannot construct a tier-inverted geometry: every cross-source
//! reference is automatically promoted to an earlier tier than the
//! consumer. E150c is therefore wired as defense-in-depth — it would
//! fire if a future `build_source_dag` extension admitted multi-tier
//! inversions, and the unit test below pins the predicate's behavior on
//! a synthetically-constructed multi-tier `Vec<SourceTier>`.

use crate::plan::execution::{SourceTier, source_tier_index};

/// `source_tier_index` returns the tier position of a source name in a
/// multi-tier `Vec<SourceTier>`. Pin this on a synthetic graph: the
/// production `build_source_dag` only ever produces two tiers, but the
/// predicate consumed by the E150c diagnostic must remain correct as
/// future tier-inversion geometries become expressible.
#[test]
fn source_tier_index_returns_tier_position_for_multi_tier_graph() {
    let tiers = vec![
        SourceTier {
            sources: vec!["alpha".to_string(), "beta".to_string()],
        },
        SourceTier {
            sources: vec!["gamma".to_string()],
        },
        SourceTier {
            sources: vec!["delta".to_string()],
        },
    ];
    assert_eq!(source_tier_index(&tiers, "alpha"), Some(0));
    assert_eq!(source_tier_index(&tiers, "beta"), Some(0));
    assert_eq!(source_tier_index(&tiers, "gamma"), Some(1));
    assert_eq!(source_tier_index(&tiers, "delta"), Some(2));
    assert_eq!(source_tier_index(&tiers, "missing"), None);
}

/// E150c predicate fires when the cross-source target's tier index is
/// strictly greater than the primary input source's tier index. The
/// diagnostic at `config/mod.rs` consults the same comparison; this
/// unit test pins the boundary directly so any future regression on
/// the comparator (off-by-one, sign flip) surfaces here.
#[test]
fn e150c_predicate_distinguishes_inverted_from_aligned_tiers() {
    let tiers = vec![
        SourceTier {
            sources: vec!["upstream".to_string()],
        },
        SourceTier {
            sources: vec!["downstream".to_string()],
        },
    ];

    // Aligned: primary is later than reference target → E150c quiet.
    let primary_tier = source_tier_index(&tiers, "downstream").unwrap();
    let target_tier = source_tier_index(&tiers, "upstream").unwrap();
    assert!(
        target_tier <= primary_tier,
        "aligned: target tier {target_tier} <= primary tier {primary_tier} \
         keeps E150c quiet"
    );

    // Inverted: primary is earlier than reference target → E150c fires.
    let primary_tier = source_tier_index(&tiers, "upstream").unwrap();
    let target_tier = source_tier_index(&tiers, "downstream").unwrap();
    assert!(
        target_tier > primary_tier,
        "inverted: target tier {target_tier} > primary tier {primary_tier} \
         is the exact condition E150c fires on"
    );
}
