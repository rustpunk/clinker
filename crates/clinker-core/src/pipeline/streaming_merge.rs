//! Shared streaming-merge boundary detector (Phase 16 Task 16.4.0).
//!
//! Factors out the group-boundary emission loop that was previously
//! inlined inside `HashAggregator::finalize_with_spill` and was about to
//! be duplicated inside `StreamingAggregator<AddRaw>`. Both call sites
//! consume a monotonically non-decreasing stream of
//! `(key, AggregatorGroupState)` pairs and need to:
//!
//! 1. Keep exactly one "currently open" group.
//! 2. Merge incoming state into the open state when keys are equal.
//! 3. Finalize the open group and yield one `SortRow` when a strictly
//!    greater key arrives.
//! 4. Flush the last open group on end-of-stream.
//!
//! The two call sites differ in how they derive `(key, state)` —
//! `finalize_with_spill` decodes a spilled `Record`, whereas
//! `StreamingAggregator<AddRaw>` folds a raw upstream `Record` into a
//! fresh prototype state. The boundary-detection state machine is
//! identical. Per DataFusion PR #4301, centralizing this loop avoids
//! subtle merge/emission skew between the two paths.

use clinker_record::{GroupByKey, Value, accumulator::AccumulatorEnum};
use indexmap::IndexMap;

use crate::aggregation::{AggregatorGroupState, HashAggError, SortRow};

/// Group-boundary emission state machine.
///
/// Holds the currently open `(key, state)` pair and, on each call to
/// [`GroupBoundary::push`], either merges the incoming partial into the
/// open group (equal keys) or finalizes the open group into a `SortRow`
/// and replaces it with the new one (strictly greater key). Strictly
/// lesser keys violate the caller's monotonic contract and are reported
/// as [`HashAggError::SortOrderViolation`].
///
/// The emission closure `F` is injected by the caller so this module
/// stays decoupled from the aggregation finalize path's compiled-emits
/// machinery.
pub(crate) struct GroupBoundary {
    current: Option<(Vec<GroupByKey>, AggregatorGroupState)>,
}

impl GroupBoundary {
    pub(crate) fn new() -> Self {
        Self { current: None }
    }

    /// Push one `(key, state)` partial. On a key boundary the previous
    /// group is finalized via `finalize` and returned as a `SortRow`. On
    /// an equal key the two per-group states are merged. On a strictly
    /// lesser key an error is returned — callers treat this as a hard
    /// abort (`PipelineError::SortOrderViolation` / `Internal`).
    ///
    /// The `sidecar` triple — `(row_num, emitted, accumulated)` — is
    /// the executor's per-record metadata. For the in-memory and
    /// streaming-raw paths it comes from the input record's SortRow; for
    /// the spill-recovery path the sidecars were lost at spill time and
    /// the caller supplies identity values (`0`, empty, empty).
    pub(crate) fn push<F>(
        &mut self,
        key: Vec<GroupByKey>,
        state: AggregatorGroupState,
        sidecar: (u64, IndexMap<String, Value>, IndexMap<String, Value>),
        finalize: &F,
    ) -> Result<Option<SortRow>, HashAggError>
    where
        F: Fn(&[GroupByKey], &AggregatorGroupState) -> Result<clinker_record::Record, HashAggError>,
    {
        use std::cmp::Ordering;

        if let Some((cur_key, cur_state)) = self.current.as_mut() {
            match crate::aggregation::compare_group_keys(cur_key, &key) {
                Ordering::Equal => {
                    // Row-by-row accumulator merge. Sidecars + tracker
                    // fold via the shared `merge_group_sidecars` helper
                    // so the raw-streaming path and the spill-recovery
                    // path produce byte-identical reductions (Task
                    // 16.4.2 audit fix Gap B).
                    for (a, b) in cur_state.row.iter_mut().zip(state.row.iter()) {
                        AccumulatorEnum::merge(a, b);
                    }
                    // Ensure `state` carries the current call's sidecar
                    // triple in its own fields — the AddRaw path seeds
                    // these from the per-record sidecar, the spill-merge
                    // path has them populated from the spilled envelope
                    // (or at identity values in the 16.3.13 fallback).
                    let mut src = state;
                    if src.min_row_num == u64::MAX {
                        src.min_row_num = sidecar.0;
                    }
                    if src.common_emitted.is_none() && !sidecar.1.is_empty() {
                        src.common_emitted = Some(sidecar.1.clone());
                    }
                    if src.union_accumulated.is_empty() {
                        src.union_accumulated = sidecar.2.clone();
                    }
                    // Accumulator rows were already merged above; zero
                    // `src.row` so `merge_group_sidecars` does not
                    // double-count anything (the helper only touches
                    // sidecars + tracker, but keeping `src.row` empty
                    // makes that contract obvious to future readers).
                    src.row.clear();
                    crate::aggregation::merge_group_sidecars(cur_state, src);
                    let _ = &sidecar; // consumed into src above
                    Ok(None)
                }
                Ordering::Less => {
                    // cur_key < incoming key → boundary; finalize cur.
                    let (prev_key, prev_state) = self.current.take().unwrap();
                    let record = finalize(&prev_key, &prev_state)?;
                    let row_num = if prev_state.min_row_num == u64::MAX {
                        0
                    } else {
                        prev_state.min_row_num
                    };
                    let emitted = prev_state.common_emitted.unwrap_or_default();
                    let accumulated = prev_state.union_accumulated;
                    // Install the new open group with its sidecar triple
                    // already seeded (the caller passes the incoming
                    // per-record sidecars; for spill-recovery they are
                    // identity values).
                    let mut new_state = state;
                    if new_state.min_row_num == u64::MAX {
                        new_state.min_row_num = sidecar.0;
                    }
                    if new_state.common_emitted.is_none() && !sidecar.1.is_empty() {
                        new_state.common_emitted = Some(sidecar.1);
                    }
                    if new_state.union_accumulated.is_empty() {
                        new_state.union_accumulated = sidecar.2;
                    }
                    self.current = Some((key, new_state));
                    Ok(Some((record, row_num, emitted, accumulated)))
                }
                // cur_key > incoming → caller violated monotonic
                // contract (upstream sort is wrong).
                Ordering::Greater => Err(HashAggError::SortOrderViolation {
                    message: format!(
                        "streaming-merge received key that sorts before the currently-open group \
                         (cur={cur_key:?}, incoming={key:?}); upstream ordering contract violated"
                    ),
                }),
            }
        } else {
            let mut new_state = state;
            if new_state.min_row_num == u64::MAX {
                new_state.min_row_num = sidecar.0;
            }
            if new_state.common_emitted.is_none() && !sidecar.1.is_empty() {
                new_state.common_emitted = Some(sidecar.1);
            }
            if new_state.union_accumulated.is_empty() {
                new_state.union_accumulated = sidecar.2;
            }
            self.current = Some((key, new_state));
            Ok(None)
        }
    }

    /// Finalize and emit the last open group, if any.
    pub(crate) fn flush<F>(mut self, finalize: &F) -> Result<Option<SortRow>, HashAggError>
    where
        F: Fn(&[GroupByKey], &AggregatorGroupState) -> Result<clinker_record::Record, HashAggError>,
    {
        if let Some((key, state)) = self.current.take() {
            let record = finalize(&key, &state)?;
            let row_num = if state.min_row_num == u64::MAX {
                0
            } else {
                state.min_row_num
            };
            let emitted = state.common_emitted.unwrap_or_default();
            let accumulated = state.union_accumulated;
            Ok(Some((record, row_num, emitted, accumulated)))
        } else {
            Ok(None)
        }
    }
}
