//! Projection of the per-source count maps onto the report-facing view.
//!
//! `project_declared_source_dlq_counts` and
//! `project_declared_source_record_counts` are the single point where the
//! internal per-source counters (which include the synthetic `<merged>` rollup
//! slot) are filtered down to the declared-source view the `ExecutionReport`
//! surfaces. These white-box tests seed the raw maps directly so the
//! `<merged>` filter and the zero-handling asymmetry are pinned without
//! standing up a full pipeline — the integration counterparts in
//! `tests/per_source_merged_dlq.rs` and `tests/per_source_record_counts.rs`
//! prove the same contract end-to-end.

use super::*;

use crate::executor::dispatch::MERGED_SOURCE_NAME;
use std::collections::HashMap;
use std::sync::Arc;

/// A DLQ counter map keyed by the synthetic `<merged>` slot plus a declared
/// source projects to the declared source alone: `<merged>` is dropped, so the
/// surfaced sum is strictly below the aggregate that includes it.
#[test]
fn dlq_projection_drops_the_merged_rollup_slot() {
    let merged_key: &Arc<str> = &MERGED_SOURCE_NAME;
    let mut dlq_per_source: HashMap<Arc<str>, u64> = HashMap::new();
    dlq_per_source.insert(Arc::from("src_a"), 3);
    dlq_per_source.insert(Arc::clone(merged_key), 5);

    let projected = project_declared_source_dlq_counts(&dlq_per_source, merged_key);

    assert_eq!(
        projected,
        BTreeMap::from([("src_a".to_string(), 3)]),
        "only the declared source surfaces; the <merged> slot is filtered out"
    );
    assert!(
        !projected.contains_key("<merged>"),
        "the synthetic rollup key never leaks into the report map"
    );
    let surfaced: u64 = projected.values().sum();
    let total: u64 = dlq_per_source.values().sum();
    assert!(
        surfaced < total,
        "the surfaced per-source sum ({surfaced}) is below the aggregate ({total}) \
         once the <merged>-attributed entries are excluded"
    );
}

/// A source with zero DLQ entries is absent from the projected DLQ map,
/// matching the "absent = none landed" precedent.
#[test]
fn dlq_projection_drops_zero_count_sources() {
    let merged_key: &Arc<str> = &MERGED_SOURCE_NAME;
    let mut dlq_per_source: HashMap<Arc<str>, u64> = HashMap::new();
    dlq_per_source.insert(Arc::from("src_clean"), 0);
    dlq_per_source.insert(Arc::from("src_bad"), 2);

    let projected = project_declared_source_dlq_counts(&dlq_per_source, merged_key);

    assert_eq!(
        projected,
        BTreeMap::from([("src_bad".to_string(), 2)]),
        "a zero-entry source is omitted from the DLQ map"
    );
}

/// The record-count projection also drops `<merged>`, but keeps a source that
/// finalized with zero records as `Some(0)` (distinct from "never finalized",
/// which is `None` and omitted).
#[test]
fn record_projection_drops_merged_keeps_zero_and_omits_unfinalized() {
    let merged_key: &Arc<str> = &MERGED_SOURCE_NAME;
    let mut source_counts: HashMap<Arc<str>, Option<u64>> = HashMap::new();
    source_counts.insert(Arc::from("src_rows"), Some(4));
    source_counts.insert(Arc::from("src_empty"), Some(0));
    source_counts.insert(Arc::from("src_unfinalized"), None);
    source_counts.insert(Arc::clone(merged_key), Some(4));

    let projected = project_declared_source_record_counts(&source_counts, merged_key);

    assert_eq!(
        projected,
        BTreeMap::from([("src_rows".to_string(), 4), ("src_empty".to_string(), 0)]),
        "the <merged> rollup and the unfinalized source are dropped; a source \
         that finalized empty is kept as 0"
    );
}
