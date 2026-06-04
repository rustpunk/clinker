//! Shared streaming-merge boundary detector — "Single-Encoder Two-Phase
//! Bytes" architecture.
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
//! (PR #9662) + Polars streaming sorted group-by pattern.

use clinker_record::{Record, accumulator::AccumulatorEnum};

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
    /// Record that opened the currently-open group. Held so the finalize
    /// closure can receive it (for semantic group-key extraction) without
    /// the caller needing a parallel `RefCell` shadow of the key.
    open_record: Option<Record>,
    mode: StreamingErrorMode,
}

impl GroupBoundary {
    pub(crate) fn new(encoder: SortKeyEncoder, mode: StreamingErrorMode) -> Self {
        Self {
            encoder,
            current: Vec::new(),
            last: Vec::new(),
            open_state: None,
            open_record: None,
            mode,
        }
    }

    /// Borrow the owned encoder. Used by callers that need to encode a
    /// record into [`Self::current`] before calling [`Self::push`].
    pub(crate) fn encoder(&self) -> &SortKeyEncoder {
        &self.encoder
    }

    /// Whether a per-group state is currently open (i.e. at least one
    /// record has been pushed and the group has not yet been flushed).
    /// Powers `StreamingAggregator::current_row_count()`.
    pub(crate) fn is_group_open(&self) -> bool {
        self.open_state.is_some()
    }

    /// Push one `(state)` partial. The caller has already populated
    /// `self.current` with the encoded group key for the incoming
    /// record. On a key boundary the previous group is finalized via
    /// `finalize` and pushed into `out`.
    ///
    /// `row_num` is the incoming record's row number; the boundary
    /// tracks the minimum across all records folded into a group so
    /// downstream sort-stable operators preserve the earliest input
    /// row's position.
    ///
    /// **Sort-order verification is always on in release builds (Task
    /// 16.4.5).** The `Ordering::Less` arm below uses an unconditional
    /// `Err` return, NOT `debug_assert!`. A user whose declared sort
    /// order is wrong, or a Clinker bug that produces an out-of-order
    /// LoserTree, will hard-abort with `SortOrderViolation` /
    /// `MergeSortOrderViolation` regardless of build profile. The cost
    /// is one `Vec<u8>::cmp` per group boundary (memcmp on the encoded
    /// key bytes) — O(group_count), not O(record_count) — so the
    /// always-on contract is free in steady state.
    pub(crate) fn push<F>(
        &mut self,
        mut state: AggregatorGroupState,
        record: Record,
        row_num: u64,
        finalize: &F,
        out: &mut Vec<SortRow>,
    ) -> Result<(), HashAggError>
    where
        F: Fn(&Record, &AggregatorGroupState) -> Result<Record, HashAggError>,
    {
        use std::cmp::Ordering;

        if row_num < state.min_row_num {
            state.min_row_num = row_num;
        }

        if self.open_state.is_none() {
            // First record — install as the open group and swap buffers.
            self.open_state = Some(state);
            self.open_record = Some(record);
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
                src.row.clear(); // already merged above
                crate::aggregation::merge_group_sidecars(cur_state, src);
                self.current.clear();
                let _ = record; // same group — incoming record not retained
                Ok(())
            }
            Ordering::Greater => {
                // Boundary: finalize the open group.
                let prev_state = self.open_state.take().unwrap();
                let prev_record = self
                    .open_record
                    .take()
                    .expect("open_record must be Some whenever open_state is Some");
                let out_record = finalize(&prev_record, &prev_state)?;
                let prev_row_num = if prev_state.min_row_num == u64::MAX {
                    0
                } else {
                    prev_state.min_row_num
                };
                out.push((out_record, prev_row_num));

                // Install the new open group.
                self.open_state = Some(state);
                self.open_record = Some(record);
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
        F: Fn(&Record, &AggregatorGroupState) -> Result<Record, HashAggError>,
    {
        if let Some(state) = self.open_state.take() {
            let prev_record = self
                .open_record
                .take()
                .expect("open_record must be Some whenever open_state is Some");
            let out_record = finalize(&prev_record, &state)?;
            let row_num = if state.min_row_num == u64::MAX {
                0
            } else {
                state.min_row_num
            };
            out.push((out_record, row_num));
        }
        Ok(())
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
