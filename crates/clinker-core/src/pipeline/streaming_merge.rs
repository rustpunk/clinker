//! Shared streaming-merge boundary detector — "Single-Encoder Two-Phase
//! Bytes" architecture (Phase 16 Task 16.4.3).
//!
//! The boundary detector owns one [`SortKeyEncoder`] and two scratch
//! `Vec<u8>` buffers (`current`, `last`). On every call to
//! [`GroupBoundary::push`] the caller has already encoded the incoming
//! record's group-by columns into `boundary.current` (via
//! `SortKeyEncoder::encode_into`). The boundary then memcmps `current`
//! against `last`:
//!
//! * `current == last` → merge incoming partial state into the open group.
//! * `current >  last` → finalize the open group, install the incoming
//!   group as the new open group, swap `last <- current` (so the next
//!   call's `current` reuses what was previously `last`'s capacity).
//! * `current <  last` → caller violated the monotonic contract.
//!   `mode == UserInput` → `HashAggError::SortOrderViolation`;
//!   `mode == SpillMerge` → `HashAggError::MergeSortOrderViolation`.
//!
//! Steady-state allocation is zero: `encode_into` clears+reuses the
//! incoming buffer's capacity, and `mem::swap` rotates the two buffers
//! without copying. This is the DataFusion `GroupValuesFullyOrdered`
//! (PR #9662) + Polars streaming sorted group-by pattern, validated by
//! the drill pass 9 audit (`RESEARCH-phase-16.4.3-spill-write-unification.md`).

use clinker_record::{Value, accumulator::AccumulatorEnum};
use indexmap::IndexMap;

use crate::aggregation::{AggregatorGroupState, HashAggError, SortRow};
use crate::pipeline::sort_key::SortKeyEncoder;

/// Whether the boundary is being driven from the user-input path
/// (`StreamingAggregator<AddRaw>`) or the spill-merge recovery path
/// (`HashAggregator::finalize_with_spill`). Determines which error
/// variant is produced when the monotonic contract is violated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamingErrorMode {
    /// User-supplied input was not actually sorted on the declared
    /// group-by prefix → user-visible DLQ-styled error.
    UserInput,
    /// LoserTree produced out-of-order keys → internal Clinker bug.
    SpillMerge,
}

/// Group-boundary emission state machine, byte-keyed.
///
/// Owns the [`SortKeyEncoder`], the two scratch byte buffers, and the
/// currently-open `AggregatorGroupState`. Callers encode each incoming
/// record's group-by columns directly into [`Self::current`] via
/// [`SortKeyEncoder::encode_into`] and then call [`Self::push`].
pub(crate) struct GroupBoundary {
    encoder: SortKeyEncoder,
    /// Scratch buffer for the incoming record's encoded group key.
    /// Caller writes into this via `boundary.encoder.encode_into(rec, &mut boundary.current)`
    /// before calling `push`.
    pub(crate) current: Vec<u8>,
    /// Encoded group key of the currently-open group. Empty when no
    /// group is open.
    last: Vec<u8>,
    /// Currently-open per-group state. `None` when no group is open.
    open_state: Option<AggregatorGroupState>,
    mode: StreamingErrorMode,
}

impl GroupBoundary {
    pub(crate) fn new(encoder: SortKeyEncoder, mode: StreamingErrorMode) -> Self {
        Self {
            encoder,
            current: Vec::new(),
            last: Vec::new(),
            open_state: None,
            mode,
        }
    }

    /// Borrow the owned encoder. Used by callers that need to encode a
    /// record into [`Self::current`] before calling [`Self::push`].
    pub(crate) fn encoder(&self) -> &SortKeyEncoder {
        &self.encoder
    }

    /// Push one `(state)` partial. The caller has already populated
    /// `self.current` with the encoded group key for the incoming
    /// record. On a key boundary the previous group is finalized via
    /// `finalize` and pushed into `out`.
    ///
    /// `sidecar` — `(row_num, emitted, accumulated)` — is the executor's
    /// per-record metadata. For the streaming-raw path it comes from the
    /// input record's SortRow; for the spill-recovery path the sidecars
    /// were lost at spill time and the caller supplies identity values
    /// (`0`, empty, empty).
    pub(crate) fn push<F>(
        &mut self,
        state: AggregatorGroupState,
        sidecar: (u64, IndexMap<String, Value>, IndexMap<String, Value>),
        finalize: &F,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError>
    where
        F: Fn(&AggregatorGroupState) -> Result<clinker_record::Record, HashAggError>,
    {
        use std::cmp::Ordering;

        if self.open_state.is_none() {
            // First record — install as the open group and swap buffers.
            let mut new_state = state;
            seed_sidecars(&mut new_state, sidecar);
            self.open_state = Some(new_state);
            std::mem::swap(&mut self.last, &mut self.current);
            self.current.clear();
            return Ok(());
        }

        match self.current.as_slice().cmp(self.last.as_slice()) {
            Ordering::Equal => {
                let cur_state = self.open_state.as_mut().unwrap();
                // Row-by-row accumulator merge.
                for (a, b) in cur_state.row.iter_mut().zip(state.row.iter()) {
                    AccumulatorEnum::merge(a, b);
                }
                let mut src = state;
                seed_sidecars(&mut src, sidecar);
                src.row.clear(); // already merged above
                crate::aggregation::merge_group_sidecars(cur_state, src);
                self.current.clear();
                Ok(())
            }
            Ordering::Greater => {
                // Boundary: finalize the open group.
                let prev_state = self.open_state.take().unwrap();
                let record = finalize(&prev_state)?;
                let row_num = if prev_state.min_row_num == u64::MAX {
                    0
                } else {
                    prev_state.min_row_num
                };
                let emitted = prev_state.common_emitted.unwrap_or_default();
                let accumulated = prev_state.union_accumulated;
                out.push((record, row_num, emitted, accumulated));

                // Install the new open group.
                let mut new_state = state;
                seed_sidecars(&mut new_state, sidecar);
                self.open_state = Some(new_state);
                std::mem::swap(&mut self.last, &mut self.current);
                self.current.clear();
                Ok(())
            }
            Ordering::Less => {
                let msg = self.encoder.debug_decode_pair(&self.last, &self.current);
                let prev = format!("0x{}", hex(&self.last));
                let next = format!("0x{}", hex(&self.current));
                match self.mode {
                    StreamingErrorMode::UserInput => Err(HashAggError::SortOrderViolation {
                        prev_key_debug: prev,
                        next_key_debug: format!("{next} ({msg})"),
                    }),
                    StreamingErrorMode::SpillMerge => Err(HashAggError::MergeSortOrderViolation {
                        prev_key_debug: prev,
                        next_key_debug: format!("{next} ({msg})"),
                    }),
                }
            }
        }
    }

    /// Finalize and emit the last open group, if any.
    pub(crate) fn flush<F>(
        mut self,
        finalize: &F,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError>
    where
        F: Fn(&AggregatorGroupState) -> Result<clinker_record::Record, HashAggError>,
    {
        if let Some(state) = self.open_state.take() {
            let record = finalize(&state)?;
            let row_num = if state.min_row_num == u64::MAX {
                0
            } else {
                state.min_row_num
            };
            let emitted = state.common_emitted.unwrap_or_default();
            let accumulated = state.union_accumulated;
            out.push((record, row_num, emitted, accumulated));
        }
        Ok(())
    }
}

fn seed_sidecars(
    state: &mut AggregatorGroupState,
    sidecar: (u64, IndexMap<String, Value>, IndexMap<String, Value>),
) {
    if state.min_row_num == u64::MAX {
        state.min_row_num = sidecar.0;
    }
    if state.common_emitted.is_none() && !sidecar.1.is_empty() {
        state.common_emitted = Some(sidecar.1);
    } else if state.common_emitted.is_none() {
        state.common_emitted = Some(IndexMap::new());
    }
    if state.union_accumulated.is_empty() {
        state.union_accumulated = sidecar.2;
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}
