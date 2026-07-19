//! IEJoin algorithm for combine nodes with range predicates.
//!
//! Implements the Union Arrays variant from Khayyat et al., VLDB Journal
//! 2017 ("Lightning Fast and Space Efficient Inequality Joins"). The
//! algorithm builds two sorted arrays L1 (sorted on the first range
//! attribute) and L2 (sorted on the second), with a permutation P
//! mapping L2 positions into L1, and a bit-array that records which L2
//! positions have been visited. Scanning L2 in order and consulting the
//! bit-array yields every pair `(left, right)` for which both range
//! predicates hold — without the O(N*M) nested-loop blow-up.
//!
//! Two corrections in the 2017 errata are load-bearing here. The
//! original 2015 paper grouped `(>, <=)` and `(<, >=)` for sort
//! direction; the corrected grouping is `(>, >=)` descending and
//! `(<, <=)` ascending, with L2 sorted in the OPPOSITE direction from
//! L1. Implementing the 2015 sort rules silently produces wrong results
//! for half of all operator-pair combinations. The proptest below pins
//! the corrected rules against a nested-loop oracle on all sixteen
//! `(op1, op2)` pairs.
//!
//! Errata point 4 (also missing from the original paper) requires a
//! secondary tiebreaker on the OTHER axis attribute when primary keys
//! collide, plus a tertiary tiebreaker on record index. Without it,
//! duplicate values produce missing or duplicated matches. Both DuckDB
//! and Polars implement this; we follow the same rule.
//!
//! NULL handling matches Polars (drill D33): records with NULL in any
//! range field are partitioned out before sorting and routed through
//! the combine's `on_miss` policy. CXL ternary semantics return false
//! for NULL comparisons, and the pre-filter avoids the entire DuckDB
//! `#10122` ValidityMask corruption class.
//!
//! **Memory model.** One bounded dispatch shape — the block-band path in the
//! [`block`] child module — serves every combine, whether pure-range or
//! equi+range, so the INPUT axis is bounded by the budget rather than the input
//! size in both cases. Each side is external-sorted (spilling its overflow to
//! disk) on `(equality-hash, primary-range-key, …)` — the equality hash leads
//! the sort so equal-keyed records cluster — the sorted stream is sliced into
//! contiguous min/max-tagged blocks that never straddle an equality-hash
//! boundary, non-overlapping block-pairs are pruned on the equality hash and the
//! range bounds, and the numeric kernel runs per surviving same-hash pair. At
//! most one driver block and one matching build block plus that pair's L1/L2
//! sort arrays are resident at once.
//!
//! Equality is an additional PRUNE axis on top of the range prune, exactly the
//! sort-based range-partitioned band join DeWitt's partitioned band join and
//! DuckDB's IEJoin block-spill use: partition/group by equality, range within.
//! The hash is only the partition/prune filter; because hashes collide, each
//! surviving block pair re-verifies the CANONICAL equality-key bytes per
//! candidate pair before emitting — the way a hash join rechecks the key after a
//! bucket hit. A single hot equality value produces many same-hash, range-
//! ordered blocks that the block-pair scheduler pairs and scans one bounded pair
//! at a time, so it degrades to a pure-range band join over its own blocks with
//! no separate skew path (the irreducible single-value block-nested-loop
//! fallback is tracked separately, see #863). A pure-range combine carries no
//! equality keys, so every record hashes to one constant group and the path is
//! byte-for-byte the prior pure-range behavior.
//!
//! The path mirrors its live footprint into a registered [`ConsumerHandle`] so
//! the arbitrator's pull-mode `current_usage` sees it — its input drain buffers,
//! resident blocks, AND its output sort buffer, so both axes are visible. The
//! OUTPUT axis is spill-bounded too: emitted rows accumulate in a payload-
//! ordered sort buffer that spills on its own byte threshold (charging resident
//! bytes through the handle) and the dispatcher drains it incrementally, so the
//! O(N·M) result never sits in RAM. The pre-output abort returns a typed
//! `PipelineError::MemoryBudgetExceeded` carrying the combine node's name and
//! `BudgetCategory::Arena`; it is a strictly LOCAL last resort — a single
//! block-pair plus kernel aux exceeding the hard limit even alone — so a
//! spilling, bounded-residency run never aborts merely because process RSS sits
//! above a tight budget. Global pressure on either axis is answered by spilling,
//! not by aborting.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use ahash::RandomState;
use clinker_record::{Record, Schema, Value};
use cxl::ast::Expr;
use cxl::eval::{EvalContext, EvalError, EvalResult, ProgramEvaluator};
use cxl::typecheck::TypedProgram;
use indexmap::IndexMap;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::executor::combine::{CombineResolver, CombineResolverMapping};
use crate::executor::widen_record_to_schema;
use crate::pipeline::combine::{CombineOutputEvalFailure, KeyExtractor, canonical_key_bytes};
use crate::pipeline::memory::{ConsumerHandle, MemoryArbitrator};
use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
use clinker_plan::BudgetCategory;
use clinker_plan::config::pipeline_node::{MatchMode, OnMiss};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::combine::{DecomposedPredicate, RangeKeyType, RangeOp};

mod block;

/// Cap on matches collected per driver under [`MatchMode::Collect`].
/// Mirrors `pipeline::combine::COLLECT_PER_GROUP_CAP` so both code paths
/// truncate at the same threshold.
const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// Period (matched pairs emitted) between [`MemoryArbitrator::should_abort`]
/// polls during the IEJoin scan. Same cadence as the hash probe loop.
const MEMORY_CHECK_INTERVAL: usize = 10_000;

/// Order-tracking sidecar carried alongside every record in the
/// executor's `node_buffers`. Spelled out here so the IEJoin entry
/// point's signature stays self-documenting.
pub(crate) type RecordOrder = u64;

/// The block-band combine's bounded output: a payload-ordered sorted handle over
/// the emitted rows — resident when the whole result fit the output sort
/// buffer's byte threshold, spilled to sorted runs otherwise — keyed on
/// `(driver order, driver_idx, build_idx)`, the exact key the pre-spill kernel
/// globally sorted its in-RAM output vec by. The dispatcher drains this handle
/// incrementally, so the output axis spills instead of holding every matched row
/// in RAM. `output_eval_failures` carries the deferred output-stage eval failures
/// the dispatcher routes through the DLQ (bounded by driver count, so it stays a
/// separate resident sorted pass).
pub(crate) struct BlockBandOutput {
    pub sorted: SortedOutput<(RecordOrder, u64, u64)>,
    /// Exact count of emitted rows across every sorted run (== the output sort
    /// buffer's total pushes). Lets the dispatcher size a spilled drain in O(1),
    /// and — when the runs are adopted whole into a merge-on-drain node buffer —
    /// serves as that slot's `len_hint` without a disk scan.
    pub row_count: u64,
    pub output_eval_failures: Vec<CombineOutputEvalFailure>,
}

// Summarizes shape and counts without recursing into records or spill files, so
// a test's `expect_err` panic renders the handle without requiring `Debug` on
// `SortedOutput` / `SpillFile`.
impl std::fmt::Debug for BlockBandOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let sorted = match &self.sorted {
            SortedOutput::InMemory(rows) => format!("InMemory({} rows)", rows.len()),
            SortedOutput::Spilled(files) => format!("Spilled({} runs)", files.len()),
        };
        f.debug_struct("BlockBandOutput")
            .field("sorted", &sorted)
            .field("row_count", &self.row_count)
            .field("output_eval_failures", &self.output_eval_failures.len())
            .finish()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Bit array with coarse 1:1024 Bloom filter
// ──────────────────────────────────────────────────────────────────────────

/// One-bit-per-position bit array with a 1:1024 coarse Bloom filter.
///
/// `bits[i]` encodes whether L1 position `i` has been visited. The
/// coarse filter `coarse[i/1024]` lets the IEJoin scan skip empty
/// 1024-entry regions entirely — DuckDB's `NextValid` pattern. Because
/// the IEJoin scan walks ALL set bits at-or-after a given start
/// position (not just the next one), the iterator-based interface
/// below is the cleanest fit; a `next_set_from(pos) -> Option<usize>`
/// shape would re-walk the same coarse-filter logic on every call.
struct IEJoinBitArray {
    bits: Vec<u64>,
    coarse: Vec<u64>,
    len: usize,
}

impl IEJoinBitArray {
    /// Allocate a fresh array of `len` bits, all cleared. Backing
    /// vecs are sized so out-of-bounds words are zero rather than
    /// short-allocations the scan would have to special-case.
    fn new(len: usize) -> Self {
        let words = len.div_ceil(64);
        let regions = len.div_ceil(1024);
        Self {
            bits: vec![0u64; words.max(1)],
            coarse: vec![0u64; regions.div_ceil(64).max(1)],
            len,
        }
    }

    /// Set the bit at position `pos`. Also updates the coarse filter
    /// so the iterator can skip empty regions later.
    #[inline]
    fn set(&mut self, pos: usize) {
        debug_assert!(pos < self.len, "IEJoinBitArray::set out of range");
        let word = pos / 64;
        let bit = pos % 64;
        self.bits[word] |= 1u64 << bit;

        let region = pos / 1024;
        let coarse_word = region / 64;
        let coarse_bit = region % 64;
        self.coarse[coarse_word] |= 1u64 << coarse_bit;
    }

    /// Iterate every set bit at or after `start`, in ascending order.
    fn iter_set_from(&self, start: usize) -> IterSetFrom<'_> {
        IterSetFrom {
            bits: self,
            cursor: start,
        }
    }
}

/// Iterator over set bits at or after a starting position, ascending.
///
/// Skips empty 1024-entry regions via the coarse filter. Inside a
/// non-empty region, scans 64-bit words with `trailing_zeros()` to
/// jump to the next set bit. `cursor` tracks the next unexamined
/// position; the iterator advances past it on every successful yield.
struct IterSetFrom<'a> {
    bits: &'a IEJoinBitArray,
    cursor: usize,
}

impl<'a> Iterator for IterSetFrom<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<usize> {
        let len = self.bits.len;
        while self.cursor < len {
            let region = self.cursor / 1024;
            let coarse_word = region / 64;
            let coarse_bit = region % 64;
            // If the entire 1024-entry region is empty, jump past it.
            if (self.bits.coarse[coarse_word] >> coarse_bit) & 1 == 0 {
                let next_region_start = (region + 1) * 1024;
                self.cursor = next_region_start.max(self.cursor + 1);
                continue;
            }
            // Region is non-empty; scan the current word.
            let word_idx = self.cursor / 64;
            let bit_idx = self.cursor % 64;
            // Mask off bits BEFORE the cursor in the current word.
            let masked = self.bits.bits[word_idx] & (!0u64 << bit_idx);
            if masked != 0 {
                let bit = masked.trailing_zeros() as usize;
                let pos = word_idx * 64 + bit;
                if pos >= len {
                    return None;
                }
                self.cursor = pos + 1;
                return Some(pos);
            }
            // Current word exhausted — advance to the next word.
            // Region boundaries are re-checked by the outer loop.
            self.cursor = (word_idx + 1) * 64;
        }
        None
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Numeric IEJoin / PWMJ kernels
// ──────────────────────────────────────────────────────────────────────────

/// Direction the corrected errata assigns to a range operator's primary
/// sort. `Asc` for `<`/`<=`, `Desc` for `>`/`>=`. L2 is sorted in the
/// opposite direction to L1's same-axis operator.
#[derive(Clone, Copy)]
enum SortDir {
    Asc,
    Desc,
}

fn primary_dir(op: RangeOp) -> SortDir {
    match op {
        RangeOp::Lt | RangeOp::Le => SortDir::Asc,
        RangeOp::Gt | RangeOp::Ge => SortDir::Desc,
    }
}

/// Whether op1 AND op2 both include equality. Used to compute `eqOff`:
/// `0` when both inclusive, `1` otherwise. The offset adjusts the L1
/// scan start so equal-key entries match only when both operators
/// admit equality.
fn both_inclusive(op1: RangeOp, op2: RangeOp) -> bool {
    matches!(op1, RangeOp::Le | RangeOp::Ge) && matches!(op2, RangeOp::Le | RangeOp::Ge)
}

/// Compare two order-preserving keys under a sort direction. Generic so the
/// range axis (widened to `i128` to carry the decimal fixed-point grid) and the
/// `i64` signed record index share one comparator.
#[inline]
fn cmp_dir<T: Ord>(a: T, b: T, dir: SortDir) -> std::cmp::Ordering {
    match dir {
        SortDir::Asc => a.cmp(&b),
        SortDir::Desc => b.cmp(&a),
    }
}

/// Numeric IEJoin (Union Arrays variant) for two range predicates.
///
/// Returns the set of `(left_idx, right_idx)` pairs `(li, ri)` for
/// which `op1.apply(left[li].0, right[ri].0)` AND
/// `op2.apply(left[li].1, right[ri].1)` hold. Output order is not
/// specified — callers that care must sort or hash before comparing.
/// Uncapped convenience wrapper, used only by the kernel's own equivalence
/// tests; production always goes through [`iejoin_numeric_capped`] with a
/// budget-derived cap.
#[cfg(test)]
fn iejoin_numeric(
    left_keys: &[(i128, i128)],
    right_keys: &[(i128, i128)],
    op1: RangeOp,
    op2: RangeOp,
) -> Vec<(usize, usize)> {
    iejoin_numeric_capped(left_keys, right_keys, op1, op2, usize::MAX)
        .expect("an uncapped iejoin_numeric (max_pairs == usize::MAX) never overflows")
}

/// Cap-aware IEJoin. Identical to [`iejoin_numeric`] except it stops and returns
/// `None` the moment the match count would exceed `max_pairs`, WITHOUT
/// materializing an over-budget output vector — so a caller under a tight memory
/// budget can detect a dense pair and fall back to a bounded streaming pass
/// rather than allocating a huge pair vector. `Some(pairs)` is the complete
/// result whenever `pairs.len() <= max_pairs`. `max_pairs == usize::MAX` is
/// effectively uncapped (a `usize` length can never exceed it).
fn iejoin_numeric_capped(
    left_keys: &[(i128, i128)],
    right_keys: &[(i128, i128)],
    op1: RangeOp,
    op2: RangeOp,
    max_pairs: usize,
) -> Option<Vec<(usize, usize)>> {
    let n_left = left_keys.len();
    let n_right = right_keys.len();
    let n_total = n_left + n_right;
    if n_left == 0 || n_right == 0 {
        return Some(Vec::new());
    }

    // Encode left record `i` as +(i+1), right record `j` as -(j+1) in the
    // signed-index slot (i64); the two order-preserving range keys ride as i128.
    let mut l1: Vec<(i64, i128, i128)> = Vec::with_capacity(n_total);
    for (i, (k1, k2)) in left_keys.iter().enumerate() {
        l1.push(((i + 1) as i64, *k1, *k2));
    }
    for (j, (k1, k2)) in right_keys.iter().enumerate() {
        l1.push((-((j + 1) as i64), *k1, *k2));
    }
    let mut l2 = l1.clone();

    let dir1 = primary_dir(op1);
    let dir2 = primary_dir(op2);

    // Strict-vs-inclusive secondary tiebreaker (errata point 4).
    //
    // When primary keys tie on either axis, the algorithm needs a
    // tiebreak that produces a consistent left/right interleaving:
    //
    // L1 axis: when op1_keys tie, all right entries must precede all
    // left entries if op1 is STRICT (so the eqOff=1 scan from
    // `l1_pos + 1` misses them — equal op1_keys violate Lt/Gt). For
    // INCLUSIVE op1, left precedes right so the eq scan finds them
    // and emits the equal-key match. signed_idx is positive for
    // left, negative for right; Asc puts right first, Desc puts left
    // first.
    //
    // L2 axis: when op2_keys tie, the left entry must process BEFORE
    // the right's bit is set if op2 is STRICT (so equal op2_keys
    // never match). For INCLUSIVE op2, right precedes left so the
    // bit IS set when left scans. signed_idx Desc puts left first
    // (positive > negative); Asc puts right first.
    let op1_strict = matches!(op1, RangeOp::Lt | RangeOp::Gt);
    let op2_strict = matches!(op2, RangeOp::Lt | RangeOp::Gt);
    let l1_idx_dir = if op1_strict {
        SortDir::Asc
    } else {
        SortDir::Desc
    };
    let l2_idx_dir = if op2_strict {
        SortDir::Desc
    } else {
        SortDir::Asc
    };

    // L1: primary by op1_key in dir1, secondary by signed_idx in the
    // op1-strictness direction. The op2_key axis is NOT a secondary
    // tiebreaker on L1 — within an equal-op1 group, only the
    // left/right interleaving matters; op2 filtering happens via the
    // L2 scan and bit-array intersection.
    l1.sort_by(|a, b| cmp_dir(a.1, b.1, dir1).then(cmp_dir(a.0, b.0, l1_idx_dir)));
    // L2: primary by op2_key in the OPPOSITE direction to op2's
    // primary (errata correction — paper's grouping was wrong).
    // Secondary by signed_idx in the op2-strictness direction.
    let dir2_l2 = match dir2 {
        SortDir::Asc => SortDir::Desc,
        SortDir::Desc => SortDir::Asc,
    };
    l2.sort_by(|a, b| cmp_dir(a.2, b.2, dir2_l2).then(cmp_dir(a.0, b.0, l2_idx_dir)));

    // Permutation P: P[l2_pos] = l1_pos for the same signed_idx.
    // Map signed_idx (in -(n_right) .. -1 ∪ 1 .. n_left) to a dense
    // 0..n_total index for the auxiliary table.
    let dense_of = |signed: i64| -> usize {
        if signed > 0 {
            (signed - 1) as usize
        } else {
            n_left + ((-signed - 1) as usize)
        }
    };
    let mut l1_pos_of_dense = vec![0usize; n_total];
    for (l1_pos, entry) in l1.iter().enumerate() {
        l1_pos_of_dense[dense_of(entry.0)] = l1_pos;
    }
    let mut p = vec![0usize; n_total];
    for (l2_pos, entry) in l2.iter().enumerate() {
        p[l2_pos] = l1_pos_of_dense[dense_of(entry.0)];
    }

    let eq_off: usize = if both_inclusive(op1, op2) { 0 } else { 1 };
    let mut bit = IEJoinBitArray::new(n_total);
    let mut out = Vec::new();

    for l2_pos in 0..n_total {
        let entry = &l2[l2_pos];
        if entry.0 > 0 {
            // LEFT side: scan L1 forward from l1_pos + eq_off for
            // every set bit (right-side entries already processed).
            let l1_pos = p[l2_pos];
            let scan_start = l1_pos.saturating_add(eq_off);
            for set_l1_pos in bit.iter_set_from(scan_start) {
                let right_signed = l1[set_l1_pos].0;
                debug_assert!(
                    right_signed < 0,
                    "bit array recorded a non-right L1 position"
                );
                let li = (entry.0 - 1) as usize;
                let ri = ((-right_signed) - 1) as usize;
                out.push((li, ri));
                // Stop before the vector grows past what the caller's budget
                // admits; the caller re-runs the pair through a bounded pass.
                if out.len() > max_pairs {
                    return None;
                }
            }
        } else {
            // RIGHT side: mark its L1 position as visited.
            let l1_pos = p[l2_pos];
            bit.set(l1_pos);
        }
    }
    Some(out)
}

/// Single-inequality piecewise merge join — universal across DuckDB,
/// Polars, and DataFusion (drill D34). Sorts both sides by the
/// condition key, then enumerates pairs that satisfy the predicate.
/// Uncapped convenience wrapper, used only by the kernel's own equivalence
/// tests; production always goes through [`pwmj_numeric_capped`].
#[cfg(test)]
fn pwmj_numeric(left_keys: &[i128], right_keys: &[i128], op: RangeOp) -> Vec<(usize, usize)> {
    pwmj_numeric_capped(left_keys, right_keys, op, usize::MAX)
        .expect("an uncapped pwmj_numeric (max_pairs == usize::MAX) never overflows")
}

/// Cap-aware single-inequality PWMJ. Mirrors [`iejoin_numeric_capped`]: returns
/// `None` the moment the match count would exceed `max_pairs`, stopping without
/// materializing an over-budget vector, else `Some(pairs)` with
/// `pairs.len() <= max_pairs`. `max_pairs == usize::MAX` is uncapped.
fn pwmj_numeric_capped(
    left_keys: &[i128],
    right_keys: &[i128],
    op: RangeOp,
    max_pairs: usize,
) -> Option<Vec<(usize, usize)>> {
    let n_left = left_keys.len();
    let n_right = right_keys.len();
    if n_left == 0 || n_right == 0 {
        return Some(Vec::new());
    }
    let dir = primary_dir(op);
    let mut left_idx: Vec<usize> = (0..n_left).collect();
    let mut right_idx: Vec<usize> = (0..n_right).collect();
    left_idx.sort_by(|&a, &b| cmp_dir(left_keys[a], left_keys[b], dir).then(a.cmp(&b)));
    right_idx.sort_by(|&a, &b| cmp_dir(right_keys[a], right_keys[b], dir).then(a.cmp(&b)));

    let apply = |a: i128, b: i128| -> bool {
        match op {
            RangeOp::Lt => a < b,
            RangeOp::Le => a <= b,
            RangeOp::Gt => a > b,
            RangeOp::Ge => a >= b,
        }
    };

    // Direct enumeration: produces the same pair set as the more
    // intricate two-pointer scan but avoids the duplicate-key fenceposts
    // that the spec calls out as a hot bug class. The PWMJ entry point
    // exists so the planner can pick the simpler kernel for the
    // single-inequality case; correctness, not raw throughput, is the
    // load-bearing guarantee at this layer.
    let mut out = Vec::new();
    for &li in &left_idx {
        let lv = left_keys[li];
        for &ri in &right_idx {
            if apply(lv, right_keys[ri]) {
                out.push((li, ri));
                // Stop before the vector grows past the caller's budget.
                if out.len() > max_pairs {
                    return None;
                }
            }
        }
    }
    Some(out)
}

// ──────────────────────────────────────────────────────────────────────────
// Public entry point: execute_combine_iejoin
// ──────────────────────────────────────────────────────────────────────────

/// Execute a combine node via the bounded block-band IEJoin path (or PWMJ for
/// single-inequality predicates). Equality conjuncts, when present, become an
/// additional prune axis inside the same block-band machinery — there is no
/// separate resident hash-partition path — so every combine, pure-range or
/// equi+range, is bounded on both the input and output axes.
///
/// The caller (executor's Combine arm) is responsible for input schema
/// validation and predecessor buffer routing; this function trusts its
/// inputs and constructs records on the combine's `output_schema`.
///
/// Output `RecordOrder` carries the driver record's original ordering
/// tag — same convention HashBuildProbe uses. The block-band output sort
/// returns rows in the deterministic `(driver order, driver_idx, build_idx)`
/// order, independent of the memory-derived block layout.
/// Inputs to [`execute_combine_iejoin`]. Grouped into a struct so the
/// function signature stays under clippy's `too_many_arguments` cap and
/// callers can update one field without rewriting the call site.
pub(crate) struct IEJoinExec<'a> {
    pub name: &'a str,
    pub build_qualifier: &'a str,
    pub driver_records: Vec<(Record, RecordOrder)>,
    pub build_records: Vec<Record>,
    pub decomposed: &'a DecomposedPredicate,
    pub body_program: Option<&'a Arc<TypedProgram>>,
    pub resolver_mapping: &'a CombineResolverMapping,
    pub output_schema: Option<&'a Arc<Schema>>,
    pub match_mode: MatchMode,
    pub on_miss: OnMiss,
    /// Opt-in runtime cap on the combine's emitted-row count (E325 on breach);
    /// `None` is unlimited. Enforced at the shared output-emit chokepoint, so it
    /// covers every match mode and both the materialized and nested-loop paths.
    pub max_output_rows: Option<u64>,
    /// Build-side `$ck.<field>` propagation policy. Mirrors the
    /// HashBuildProbe arm — every strategy threads the same spec to
    /// the shared `copy_build_ck_columns` helper at every emit site.
    pub propagate_ck: &'a clinker_plan::config::pipeline_node::PropagateCkSpec,
    pub ctx: &'a EvalContext<'a>,
    pub budget: &'a MemoryArbitrator,
    /// Shared handle for the IEJoin's registered `MemoryConsumer` wrapper.
    /// The block-band path mirrors its live working-set bytes into this handle
    /// (its drain buffers, resident blocks, and output sort buffer — so both
    /// its axes are visible), so the arbitrator's pull-mode `current_usage`
    /// reads the footprint while the join runs. It does not read the handle's
    /// spill / pause flags: the path spills both its input drains and its output
    /// sort on their own byte thresholds, so the handle carries the byte
    /// estimate for attribution and pressure on either axis is answered by
    /// spilling, never by a global-pressure abort.
    pub consumer: &'a Arc<ConsumerHandle>,
    /// Pipeline-scoped spill directory borrowed from
    /// `ExecutorContext::spill_root_path`. The block-band path external-sorts
    /// each side and writes its min/max-tagged blocks and output-sort runs here.
    pub spill_dir: &'a Path,
    /// Whether the block-band path's spill runs and block files are
    /// LZ4-compressed. Resolved by the dispatcher from the workspace
    /// `[storage.spill] compress` knob against the combine's schema width and
    /// batch size, so the on-disk format matches what `--explain` reports.
    pub spill_compress: bool,
    /// Error strategy governing output-stage eval failures. Under
    /// `FailFast` a residual / body eval error propagates immediately;
    /// under `Continue` / `BestEffort` the failing row is deferred to the
    /// dispatcher via [`BlockBandOutput::output_eval_failures`].
    pub strategy: clinker_plan::config::ErrorStrategy,
}

pub(crate) fn execute_combine_iejoin(
    args: IEJoinExec<'_>,
) -> Result<BlockBandOutput, PipelineError> {
    let IEJoinExec {
        name,
        build_qualifier,
        driver_records,
        build_records,
        decomposed,
        body_program,
        resolver_mapping,
        output_schema,
        match_mode,
        on_miss,
        max_output_rows,
        propagate_ck,
        ctx,
        budget,
        consumer,
        spill_dir,
        spill_compress,
        strategy,
    } = args;
    if decomposed.ranges.is_empty() {
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: "iejoin executor invoked with zero range conjuncts; planner bug".to_string(),
        });
    }

    // The shared TypedProgram for range-expression evaluation. Every
    // EqualityConjunct points at the original where-clause TypedProgram,
    // and the residual is rebuilt over the merged row when ranges or
    // residuals exist (see `decompose_predicate`). The residual is
    // always populated when ranges are present (decompose folds every
    // range into the residual program), so the unwrap below is
    // guaranteed by that invariant.
    let range_program: Arc<TypedProgram> = if let Some(eq) = decomposed.equalities.first() {
        Arc::clone(&eq.left_program)
    } else {
        Arc::clone(decomposed.residual.as_ref().ok_or_else(|| {
            PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: "decomposed has range conjuncts but no equality program or residual; \
                         decompose_predicate invariant violated"
                    .to_string(),
            }
        })?)
    };

    // Build extractors aligned to the driver and build qualifiers, one
    // per equality conjunct. Mirrors the alignment loop in
    // executor::mod.rs's HashBuildProbe arm — the side that matches the
    // build qualifier feeds the build extractor; the other side feeds
    // the driver extractor regardless of its qualifier (chain-buried
    // qualifiers from N-ary decomposition route through the resolver
    // mapping at extract time).
    let mut driver_eq_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
    let mut build_eq_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
    for eq in &decomposed.equalities {
        let (driver_expr, driver_prog, build_expr, build_prog) =
            if eq.left_input.as_ref() == build_qualifier {
                (
                    eq.right_expr.clone(),
                    Arc::clone(&eq.right_program),
                    eq.left_expr.clone(),
                    Arc::clone(&eq.left_program),
                )
            } else if eq.right_input.as_ref() == build_qualifier {
                (
                    eq.left_expr.clone(),
                    Arc::clone(&eq.left_program),
                    eq.right_expr.clone(),
                    Arc::clone(&eq.right_program),
                )
            } else {
                return Err(PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!(
                        "equality conjunct has qualifiers ({}, {}); neither matches \
                         build qualifier {build_qualifier:?}",
                        eq.left_input, eq.right_input
                    ),
                });
            };
        driver_eq_progs.push((driver_prog, driver_expr));
        build_eq_progs.push((build_prog, build_expr));
    }
    let driver_extractor = KeyExtractor::new(driver_eq_progs);
    let build_extractor = KeyExtractor::new(build_eq_progs);

    // Range-key extractors, one per range conjunct, aligned to driver
    // and build sides. The range conjunct does NOT carry a per-side
    // TypedProgram (range expressions share the where-clause program),
    // so we clone `range_program` for each side. The build qualifier
    // selects which side is build; the OTHER side feeds the driver
    // extractor regardless of its qualifier (chain-buried qualifiers
    // from N-ary decomposition route through the resolver mapping at
    // extract time).
    let mut driver_range_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
    let mut build_range_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
    let mut range_ops: Vec<RangeOp> = Vec::new();
    // The per-axis numeric flavor (fixed at plan time from the unified operand
    // type) tells the scan how to reduce each side to one comparable i128. It is
    // symmetric under the driver/build flip, so it rides `r` directly.
    let mut range_kinds: Vec<RangeKeyType> = Vec::new();
    for r in &decomposed.ranges {
        let (driver_expr, build_expr, op) = if r.right_input.as_ref() == build_qualifier {
            (r.left_expr.clone(), r.right_expr.clone(), r.op)
        } else if r.left_input.as_ref() == build_qualifier {
            // Flip the operator so the predicate reads as
            // `driver_key OP build_key`.
            let flipped = match r.op {
                RangeOp::Lt => RangeOp::Gt,
                RangeOp::Le => RangeOp::Ge,
                RangeOp::Gt => RangeOp::Lt,
                RangeOp::Ge => RangeOp::Le,
            };
            (r.right_expr.clone(), r.left_expr.clone(), flipped)
        } else {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!(
                    "range conjunct has qualifiers ({}, {}); neither matches build \
                     qualifier {build_qualifier:?}",
                    r.left_input, r.right_input
                ),
            });
        };
        driver_range_progs.push((Arc::clone(&range_program), driver_expr));
        build_range_progs.push((Arc::clone(&range_program), build_expr));
        range_ops.push(op);
        range_kinds.push(r.key_type);
    }
    let driver_range_extractor = KeyExtractor::new(driver_range_progs);
    let build_range_extractor = KeyExtractor::new(build_range_progs);

    // Residual evaluator (for 3+ range conjuncts; the first two run
    // through the IEJoin/PWMJ kernel and the rest re-check via the
    // residual). The residual lives over the merged row, so it needs
    // the full CombineResolver.
    let n_ranges = range_ops.len();
    let residual_eval: Option<ProgramEvaluator> = if n_ranges > 2 {
        decomposed
            .residual
            .as_ref()
            .map(|r| ProgramEvaluator::new(Arc::clone(r), false))
    } else {
        None
    };
    let body_eval = body_program.map(|p| ProgramEvaluator::new(Arc::clone(p), false));

    // Pre-scan: extract each record's canonical equality-key bytes and its range
    // keys in parallel, deriving the equality hash from those bytes so the prune
    // hash and the re-verify bytes agree by construction. A record with a NULL in
    // any equality key (3VL) or an unrepresentable range key can never match and
    // is routed off the range walk. A pure-range combine carries no equality
    // extractor, so every record hashes to one constant group and the path is
    // byte-for-byte the prior pure-range behavior; an equi+range combine hashes
    // on its equality keys so the block-band sort clusters equal-keyed records
    // and prunes block-pairs on the equality hash before the range walk.

    // Per-record key extraction is the expensive part of the pre-scan: every
    // record runs the compiled CXL eq-key and range-key closures. The
    // extractors are stateless (`&self`) and `EvalContext` is read-only, so the
    // extraction parallelizes across the shared kernel pool. Each record yields
    // an independent `RecordScan`; the block-band drain replays those outcomes
    // in ascending index order into the external-sort buffers, so the sliced
    // blocks are a pure function of the data, not of pool scheduling.
    let driver_scans: Vec<RecordScan> = driver_records
        .par_iter()
        .map(|(rec, _rn)| {
            // Driver-side key extraction routes through `CombineResolver` so
            // chain-buried qualifiers (e.g. `b.id` against an N-ary
            // decomposition step's encoded intermediate record) resolve via the
            // resolved column map rather than `Record`'s bare-name fallback.
            let driver_resolver = CombineResolver::new(resolver_mapping, rec, None);
            scan_record(
                &driver_extractor,
                &driver_range_extractor,
                &range_kinds,
                ctx,
                &driver_resolver,
            )
            .map_err(|e| scan_key_error(name, "driving", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let build_scans: Vec<RecordScan> = build_records
        .par_iter()
        .map(|rec| {
            scan_record(
                &build_extractor,
                &build_range_extractor,
                &range_kinds,
                ctx,
                rec,
            )
            .map_err(|e| scan_key_error(name, "build", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let op1 = range_ops[0];
    let op2 = if n_ranges >= 2 {
        Some(range_ops[1])
    } else {
        None
    };

    // One bounded block-band path serves every combine. Equality conjuncts,
    // when present, ride the scan as a per-record equality hash (the leading
    // sort key and the block-pair prune axis) plus canonical equality bytes
    // (re-verified per candidate pair, since hashes collide); a pure-range
    // combine carries none, so every record shares one group and the path is
    // byte-for-byte the prior pure-range behavior. Both the input and output
    // axes spill, so the join completes within the budget instead of aborting —
    // a hot equality value degrades to a pure-range band join over its own
    // same-hash blocks rather than materializing that group resident.
    block::execute_block_band(block::BlockBandExec {
        name,
        build_qualifier,
        driver_records,
        driver_scans,
        build_records,
        build_scans,
        op1,
        op2,
        residual_eval,
        body_eval,
        resolver_mapping,
        output_schema,
        match_mode,
        on_miss,
        max_output_rows,
        propagate_ck,
        ctx,
        budget,
        consumer,
        spill_dir,
        spill_compress,
        strategy,
        options: block::BlockBandOptions::default(),
    })
}

// ──────────────────────────────────────────────────────────────────────────
// Shared emit path (equi+range groups and pure-range blocks)
// ──────────────────────────────────────────────────────────────────────────

/// Immutable per-combine configuration threaded through the shared emit
/// path. Both the equi+range group loop and the pure-range block scheduler
/// build one and reuse it across every emit call, so the two strategies
/// synthesize output rows identically. Private to this module and its
/// `block` child.
struct EmitConfig<'a> {
    name: &'a str,
    /// Build-side qualifier (the combine input name the build rows carry), under
    /// which a collect row nests its build array. Combine-wide, so it rides on
    /// the config rather than threading through each finalize call.
    build_qualifier: &'a str,
    ctx: &'a EvalContext<'a>,
    resolver_mapping: &'a CombineResolverMapping,
    output_schema: Option<&'a Arc<Schema>>,
    match_mode: MatchMode,
    propagate_ck: &'a clinker_plan::config::pipeline_node::PropagateCkSpec,
    budget: &'a MemoryArbitrator,
    strategy: clinker_plan::config::ErrorStrategy,
}

/// The residual (3+ range conjuncts) and body evaluators. Both are stateful
/// (`eval_record` takes `&mut self`), so they are owned here and borrowed
/// mutably by the emit path.
struct Evaluators {
    residual: Option<ProgramEvaluator>,
    body: Option<ProgramEvaluator>,
}

/// One driver row presented to [`emit_pairs`]: its record, its order tag, the
/// key under which its match state is tracked in [`MatchState`], and its unique
/// global input index for deterministic output tagging.
///
/// The equi+range path keys by global driver index (each driver lands in
/// exactly one bucket-and-eq-group); the block path keys by the driver's
/// local index within its driver block (each driver lands in exactly one
/// block). Either way a driver's match state lives under one stable key.
///
/// `driver_idx` is the driver's position in the combine's driver input, always
/// unique even when a chained upstream stamps duplicate `order` tags. The block
/// path tags each emitted row with it so the final sort is total; the equi+range
/// path leaves it unread (it emits no row tags).
struct DriverRef<'a> {
    record: &'a Record,
    order: RecordOrder,
    key: usize,
    driver_idx: u64,
}

/// The per-call inputs to [`emit_pairs`]: the kernel's local pair indices, the
/// driver and build slices they index, and each local build's original input
/// index, which drives the block-band's deterministic `First` selection,
/// `Collect` ordering, and final output sort.
struct EmitBatch<'a> {
    pairs: &'a [(usize, usize)],
    driver_slice: &'a [DriverRef<'a>],
    build_slice: &'a [&'a Record],
    build_idx: &'a [u64],
}

/// One accumulated collect-array element, ordered by a build-order key so a
/// bounded max-heap can retain the smallest-key [`COLLECT_PER_GROUP_CAP`]
/// matches deterministically. `order_key` is the build's original input index
/// on the pure-range block path (making the kept set and its order a pure
/// function of the data) and a per-driver insertion counter on the equi+range
/// path (which preserves visitation order and the first-`CAP` truncation the
/// path had before). Ordering ignores `value`, which is not `Ord`.
struct CollectEntry {
    order_key: u64,
    value: Value,
}

impl PartialEq for CollectEntry {
    fn eq(&self, other: &Self) -> bool {
        self.order_key == other.order_key
    }
}
impl Eq for CollectEntry {}
impl PartialOrd for CollectEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for CollectEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.order_key.cmp(&other.order_key)
    }
}

/// Resident byte cost of one cloned build record held in match state — the
/// same shape the sort buffer and block sizing charge, so the block path's
/// held-state gate tracks a comparable figure.
fn record_held_cost(record: &Record) -> u64 {
    (std::mem::size_of::<Record>() + record.estimated_heap_size()) as u64
}

/// Resident byte cost of one accumulated collect-array entry: the entry struct
/// plus the heap its extracted build-field map holds.
fn collect_entry_cost(entry: &CollectEntry) -> u64 {
    (std::mem::size_of::<CollectEntry>() + entry.value.heap_size()) as u64
}

/// Keep the `(idx, record)` with the smallest `idx` for `key` in `map`,
/// adjusting `held_bytes` for whichever record is now resident. Shared by the
/// `First`-mode candidate and the collect `$ck` build so the min-selection rule
/// (determinism-critical) lives in one place. A free function so the caller can
/// pass two disjoint `MatchState` fields (`&mut self.first_match`,
/// `&mut self.held_bytes`) without aliasing.
fn keep_min(
    map: &mut HashMap<usize, (u64, Record)>,
    held_bytes: &mut u64,
    key: usize,
    idx: u64,
    record: &Record,
) {
    match map.entry(key) {
        std::collections::hash_map::Entry::Occupied(mut e) if idx < e.get().0 => {
            *held_bytes = held_bytes.saturating_sub(record_held_cost(&e.get().1));
            *held_bytes = held_bytes.saturating_add(record_held_cost(record));
            e.insert((idx, record.clone()));
        }
        std::collections::hash_map::Entry::Occupied(_) => {}
        std::collections::hash_map::Entry::Vacant(e) => {
            *held_bytes = held_bytes.saturating_add(record_held_cost(record));
            e.insert((idx, record.clone()));
        }
    }
}

/// Per-driver match tracking shared by the pair-emit loop and the collect /
/// on_miss finalizers, keyed by [`DriverRef::key`].
struct MatchState {
    /// `matched[key]` is set once a driver has emitted at least one match.
    /// Drives the `First`-mode dedup (equi+range path) and, for the `All`
    /// mode, the end-of-join unmatched sweep. The block path's `First`
    /// selection uses `first_match` instead, so it never reads this for
    /// `First`.
    matched: Vec<bool>,
    /// Bounded max-heap per driver of the smallest-`order_key`
    /// [`COLLECT_PER_GROUP_CAP`] collect matches. Draining it sorted yields
    /// the deterministic collect array.
    collect_accum: HashMap<usize, BinaryHeap<CollectEntry>>,
    collect_truncated: HashMap<usize, ()>,
    /// The matched build with the smallest `order_key`, kept for `$ck`
    /// propagation onto the collect row. Min-keyed so the propagated build is
    /// a pure function of the data on the block path (min build input index),
    /// and the first-visited build on the equi+range path (min insertion
    /// counter) — matching that path's prior behavior.
    first_collected_builds: HashMap<usize, (u64, Record)>,
    /// The block path's `First`-mode candidate per driver: the residual-passing
    /// match with the smallest build input index, held until the driver block
    /// finalizes and emits it. Empty on the equi+range path, which emits the
    /// first-visited match immediately. Bounded by one build record per driver.
    first_match: HashMap<usize, (u64, Record)>,
    /// Running byte total of every cloned build record and collect entry held
    /// above (`first_match`, `first_collected_builds`, and the `collect_accum`
    /// heaps). Both paths fold it into their pre-output budget gates so a
    /// `match: first` / `collect` join over wide build records aborts with the
    /// typed budget error instead of growing this residency unbounded: the block
    /// path via its per-pair pre-output gate (and it drains collect per driver
    /// block), the equi+range path via each group's pre-output peak plus the
    /// every-10K accumulation poll — its per-driver heaps and min-order `$ck`
    /// build drain only in the single end-of-join flush loop, after every bucket
    /// and group completes. It stays zero for both paths' `first` / `all` modes,
    /// which emit each match immediately and hold no build records across pairs.
    held_bytes: u64,
}

impl MatchState {
    fn new(driver_count: usize) -> Self {
        Self {
            matched: vec![false; driver_count],
            collect_accum: HashMap::new(),
            collect_truncated: HashMap::new(),
            first_collected_builds: HashMap::new(),
            first_match: HashMap::new(),
            held_bytes: 0,
        }
    }

    /// The live bytes of held `First` / collect candidates, folded into the
    /// pre-output budget gates on both the block path (per-pair) and the
    /// equi+range path (per-group peak plus the every-10K accumulation poll).
    fn held_bytes(&self) -> u64 {
        self.held_bytes
    }

    /// Record one residual-passing collect match for `key`: extract the build's
    /// user fields, keep the bounded smallest-`order_key` set, and track the
    /// min-`order_key` build for `$ck`. Marking a driver truncated once its
    /// heap is full and a further match cannot displace a kept element.
    fn record_collect_match(&mut self, key: usize, order_key: u64, build_record: &Record) {
        keep_min(
            &mut self.first_collected_builds,
            &mut self.held_bytes,
            key,
            order_key,
            build_record,
        );
        // Build-side records contribute only their user-declared field values:
        // `iter_user_fields` filters every engine-stamped column (`$ck.*`
        // correlation lineage and the `$widened` auto-widen sidecar) so neither
        // nests inside a collect-array entry and trips the writer's
        // `UnserializableMapValue` guard.
        let mut m: IndexMap<Box<str>, Value> = IndexMap::new();
        for (fname, val) in build_record.iter_user_fields() {
            m.insert(fname.into(), val.clone());
        }
        let entry = CollectEntry {
            order_key,
            value: Value::Map(Box::new(m)),
        };
        let entry_cost = collect_entry_cost(&entry);
        let heap = self.collect_accum.entry(key).or_default();
        if heap.len() < COLLECT_PER_GROUP_CAP {
            heap.push(entry);
            self.held_bytes = self.held_bytes.saturating_add(entry_cost);
        } else {
            // Heap is full: keep the smallest-`order_key` CAP entries. Displace
            // the current largest only when this match is smaller.
            self.collect_truncated.insert(key, ());
            if entry.order_key < heap.peek().expect("full heap is non-empty").order_key {
                if let Some(popped) = heap.pop() {
                    self.held_bytes = self.held_bytes.saturating_sub(collect_entry_cost(&popped));
                }
                heap.push(entry);
                self.held_bytes = self.held_bytes.saturating_add(entry_cost);
            }
        }
    }

    /// Drain the collect state for `key` into the deterministic flush inputs:
    /// the array sorted ascending by `order_key`, the truncation flag, and the
    /// min-`order_key` build for `$ck`. Releases the drained entries' held-byte
    /// charge.
    fn take_collect(&mut self, key: usize) -> CollectFlush {
        let arr = match self.collect_accum.remove(&key) {
            Some(heap) => {
                let sorted = heap.into_sorted_vec();
                for e in &sorted {
                    self.held_bytes = self.held_bytes.saturating_sub(collect_entry_cost(e));
                }
                sorted.into_iter().map(|e| e.value).collect()
            }
            None => Vec::new(),
        };
        let truncated = self.collect_truncated.remove(&key).is_some();
        let first_build = match self.first_collected_builds.remove(&key) {
            Some((_, r)) => {
                self.held_bytes = self.held_bytes.saturating_sub(record_held_cost(&r));
                Some(r)
            }
            None => None,
        };
        CollectFlush {
            arr,
            truncated,
            first_build,
        }
    }

    /// Record one residual-passing `First`-mode candidate for the block path:
    /// keep the match with the smallest build input index, held until the
    /// driver block finalizes.
    fn note_first_candidate(&mut self, key: usize, build_idx: u64, build_record: &Record) {
        keep_min(
            &mut self.first_match,
            &mut self.held_bytes,
            key,
            build_idx,
            build_record,
        );
    }

    /// Remove and return this driver's held `First` candidate, releasing its
    /// held-byte charge. Used by the block path's driver-block finalize.
    fn take_first_candidate(&mut self, key: usize) -> Option<(u64, Record)> {
        let taken = self.first_match.remove(&key);
        if let Some((_, ref r)) = taken {
            self.held_bytes = self.held_bytes.saturating_sub(record_held_cost(r));
        }
        taken
    }
}

/// Mutable output sink for the shared emit path: the block-band payload-ordered
/// output sort buffer plus the deferred output-eval failures and their sort
/// tags. Each row folds its `(driver order, driver_idx, build_idx)` tiebreak in
/// as the sort payload — so the deterministic output order is realized by the
/// (possibly external) sort rather than by a trailing global sort of an in-RAM
/// vec, and the output axis spills on its own byte threshold instead of holding
/// every matched row.
struct EmitSink<'a> {
    /// The payload-ordered output sort buffer. `budget` / `name` charge each
    /// spilled run against the disk quota (E320); `consumer` mirrors the
    /// buffer's resident bytes into the IEJoin handle, so the output sort
    /// self-bounds by spilling on its own threshold while its live footprint
    /// stays visible to the arbitrator — the same charge+spill contract the
    /// input-side drain honors.
    buf: &'a mut SortBuffer<(RecordOrder, u64, u64)>,
    budget: &'a MemoryArbitrator,
    name: &'a str,
    consumer: &'a Arc<ConsumerHandle>,
    /// Opt-in cap on the combine's total emitted rows; `None` is unlimited. The
    /// running count is the output buffer's own `total_rows()` (cumulative pushes
    /// across every spilled run), checked before each push so the run fails loud
    /// (E325) rather than truncating once the output would exceed it.
    max_output_rows: Option<u64>,
    output_eval_failures: &'a mut Vec<CombineOutputEvalFailure>,
    /// Parallel `(driver order, driver_idx, build_idx)` sort key per deferred
    /// output-eval failure, so dead-letter rows re-order into the same
    /// layout-independent order as the emitted rows.
    failure_tags: Option<&'a mut Vec<(RecordOrder, u64, u64)>>,
}

impl EmitSink<'_> {
    /// Push one emitted row: fold `(order, driver_idx, build_idx)` in as the
    /// sort payload and spill a run once the buffer crosses its byte threshold
    /// (charging the run against the disk quota, E320) — so the output axis is
    /// bounded rather than holding every matched row in RAM. `build_idx` is
    /// `u64::MAX` for collect / on_miss rows, which sort after a driver's match
    /// rows in driver input order.
    fn push_row(
        &mut self,
        record: Record,
        order: RecordOrder,
        driver_idx: u64,
        build_idx: u64,
    ) -> Result<(), PipelineError> {
        // Opt-in output-size runaway guard: refuse to push the row that would
        // carry the emitted count past the configured cap, failing loud (E325)
        // rather than truncating to a partial result. `total_rows()` is the
        // buffer's cumulative push count, so checking it before the push makes
        // exactly `cap` rows land and the first over-cap row abort. Independent
        // of the memory budget — this is a result-size ceiling, not memory
        // pressure (the output sort spills to stay within memory).
        if let Some(cap) = self.max_output_rows
            && self.buf.total_rows() as u64 >= cap
        {
            return Err(PipelineError::CombineOutputCapExceeded {
                combine: self.name.to_string(),
                cap,
            });
        }
        // Mirror the byte delta into the consumer handle the moment the row
        // lands — the same charge the input-side drain applies — so the
        // arbitrator's pull-mode view reflects the output sort's live footprint.
        // Spilling on the buffer's own threshold, not global pressure, is what
        // bounds this axis; the charge releases the bytes it moves to disk.
        let pre = self.buf.bytes_used();
        self.buf.push(record, (order, driver_idx, build_idx));
        self.consumer
            .add_bytes(self.buf.bytes_used().saturating_sub(pre) as u64);
        if self.buf.should_spill() {
            let pre_spill = self.buf.bytes_used() as u64;
            let written = self.buf.sort_and_spill().map_err(|e| {
                PipelineError::Io(std::io::Error::other(format!(
                    "iejoin block-band output spill failed: {e}"
                )))
            })?;
            self.consumer.sub_bytes(pre_spill);
            if written > 0 && self.budget.record_spill_bytes(self.name, written) {
                return Err(PipelineError::spill_cap_exceeded(
                    self.name,
                    self.budget.disk_quota(),
                    written,
                    self.budget.cumulative_spill_bytes(),
                ));
            }
        }
        Ok(())
    }

    /// The output sort buffer's in-RAM bytes (bounded by its spill threshold),
    /// folded into the block-band per-pair consumer charge so the arbitrator's
    /// pull-mode view reflects the output axis alongside the input.
    fn output_buffer_bytes(&self) -> u64 {
        self.buf.bytes_used() as u64
    }

    /// Defer one recoverable output-eval failure, recording its
    /// `(order, driver_idx, build_idx)` sort key when the block path asked for
    /// reordered dead-letter output.
    fn push_failure(
        &mut self,
        failure: CombineOutputEvalFailure,
        order: RecordOrder,
        driver_idx: u64,
        build_idx: u64,
    ) {
        self.output_eval_failures.push(failure);
        if let Some(tags) = self.failure_tags.as_deref_mut() {
            tags.push((order, driver_idx, build_idx));
        }
    }
}

/// Inputs to [`flush_collect_row`] for one driver's collect-mode row.
struct CollectFlush {
    arr: Vec<Value>,
    truncated: bool,
    first_build: Option<Record>,
}

/// The strictly-local memory budget for one [`emit_pairs`] call, so a
/// `match: collect` join's per-driver accumulators cannot grow past the hard
/// limit while a pair emits.
///
/// `floor` is this pair's fixed working set — everything EXCEPT the live
/// [`MatchState`] held bytes: the resident baseline, the driver/build held
/// bytes, the materialized pair vector or nested-loop tile, the in-block pile,
/// and the reserved output-sort cap. After each collect match `emit_pairs` polls
/// `floor + state.held_bytes()` against `hard` and aborts with the typed
/// strictly-local error rather than growing collect residency unbounded — the
/// block-nested-loop fallback (and, for a many-to-one pair, the materialized
/// path) would otherwise turn a hot `match: collect` value into an OOM instead of
/// the clean pre-fallback abort. `First` / `All` hold O(1) per-driver state, so
/// the poll is a cheap no-op for them.
struct PairBudget<'a> {
    floor: u64,
    budget: &'a MemoryArbitrator,
    name: &'a str,
    consumer: &'a Arc<ConsumerHandle>,
}

/// Emit every qualifying `(driver, build)` pair in `pairs` under the
/// combine's match mode, applying the residual filter (3+ range conjuncts) and
/// the `First`-mode selection. `pairs` carries local indices into
/// `driver_slice` / `build_slice`; per-driver state is tracked in `state` under
/// [`DriverRef::key`].
///
/// The block scheduler's one emit path. `build_idx` supplies each local build's
/// original input index, so `First` selects the minimum-index match (deferred
/// to the driver block's finalize), `Collect` orders and truncates by that
/// index, and every emitted row is tagged for the final deterministic output
/// sort — making the output a pure function of the data rather than of the
/// memory-derived block layout.
///
/// Streaming into the sink's output sort buffer, which spills on its own byte
/// threshold and charges through the consumer handle, so the output axis is
/// spill-bounded and strictly-local.
fn emit_pairs(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    batch: &EmitBatch<'_>,
    state: &mut MatchState,
    sink: &mut EmitSink<'_>,
    pair_budget: &PairBudget<'_>,
) -> Result<(), PipelineError> {
    for &(di_local, bi_local) in batch.pairs {
        let dref = &batch.driver_slice[di_local];
        let driver_record = dref.record;
        let driver_order = dref.order;
        let key = dref.key;
        let build_record = batch.build_slice[bi_local];
        // The build's original input index, so `First` selects the minimum-index
        // match, `Collect` orders and truncates by it, and every row is tagged
        // for the final deterministic output sort — all pure functions of the
        // data rather than the memory-derived block layout.
        let bidx = batch.build_idx[bi_local];

        // 3+ range conjuncts: the residual re-checks the full predicate over
        // the merged row (the kernel verified only the first two axes). See
        // the module doc.
        if let Some(residual) = evals.residual.as_mut() {
            let resolver =
                CombineResolver::new(cfg.resolver_mapping, driver_record, Some(build_record));
            match residual.eval_record::<NullStorage>(cfg.ctx, &resolver, None) {
                Ok(EvalResult::Skip(_)) => continue,
                Ok(EvalResult::Emit { .. }) => {}
                Ok(EvalResult::EmitMany { .. }) => {
                    return Err(PipelineError::Internal {
                        op: "iejoin residual",
                        node: cfg.name.to_string(),
                        detail: "emit_each fan-out is not supported in a combine residual filter"
                            .into(),
                    });
                }
                Err(e) => {
                    if cfg.strategy == clinker_plan::config::ErrorStrategy::FailFast {
                        return Err(PipelineError::from(e));
                    }
                    sink.push_failure(
                        CombineOutputEvalFailure {
                            probe_record: driver_record.clone(),
                            row: driver_order,
                            matched_build: Some(build_record.clone()),
                            error: e,
                        },
                        driver_order,
                        dref.driver_idx,
                        bidx,
                    );
                    continue;
                }
            }
        }

        match cfg.match_mode {
            MatchMode::Collect => {
                // Order key: the build's input index, so the kept set and array
                // order are a deterministic function of the data.
                state.record_collect_match(key, bidx, build_record);
                // Bound the collect accumulators as they grow: they hold up to
                // COLLECT_PER_GROUP_CAP cloned build records per driver, are never
                // spilled, and drain only at the driver-block finalize — so a hot
                // equality value (the case the block-nested-loop fallback is
                // entered for) could otherwise grow them past the budget and OOM.
                // Poll the strictly-local peak after each match and abort typed,
                // matching the clean pre-fallback behavior for this shape.
                let peak = pair_budget.floor.saturating_add(state.held_bytes());
                if peak > pair_budget.budget.hard_limit() {
                    pair_budget.consumer.set_bytes(0);
                    return Err(pre_output_budget_error(
                        pair_budget.name,
                        peak,
                        pair_budget.budget.hard_limit(),
                    ));
                }
            }
            MatchMode::First => {
                // Hold the minimum-build-index candidate; emit at the driver
                // block's finalize, so the selection is the same regardless of
                // which block the winning build landed in.
                state.note_first_candidate(key, bidx, build_record);
            }
            MatchMode::All => {
                if emit_match_row(cfg, evals, dref, build_record, bidx, sink)? {
                    state.matched[key] = true;
                }
            }
        }
    }
    Ok(())
}

/// Materialize and push one `(driver, build)` output row for `First` / `All`:
/// the body eval (or, for a body-less synthetic decomposition step, the
/// driver-then-build value concat) and `$ck` propagation. The row is tagged
/// with `build_idx` for the final deterministic output sort. Returns `true` if
/// a row was pushed, `false` if the body eval skipped it or a recoverable eval
/// failure was deferred.
fn emit_match_row(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    driver: &DriverRef<'_>,
    build_record: &Record,
    build_idx: u64,
    sink: &mut EmitSink<'_>,
) -> Result<bool, PipelineError> {
    let driver_record = driver.record;
    let driver_order = driver.order;
    let driver_idx = driver.driver_idx;
    if let Some(evaluator) = evals.body.as_mut() {
        let resolver =
            CombineResolver::new(cfg.resolver_mapping, driver_record, Some(build_record));
        match evaluator.eval_record::<NullStorage>(cfg.ctx, &resolver, None) {
            Ok(EvalResult::Emit {
                fields,
                record_vars,
                ..
            }) => {
                let mut rec = match cfg.output_schema {
                    Some(s) => widen_record_to_schema(driver_record, s),
                    None => driver_record.clone(),
                };
                for (n, v) in fields {
                    rec.set(&n, v);
                }
                for (k, v) in *record_vars {
                    let _ = rec.set_record_var(&k, v);
                }
                crate::executor::copy_build_ck_columns(&mut rec, build_record, cfg.propagate_ck);
                sink.push_row(rec, driver_order, driver_idx, build_idx)?;
                Ok(true)
            }
            Ok(EvalResult::Skip(_)) => Ok(false),
            Ok(EvalResult::EmitMany { .. }) => Err(PipelineError::Internal {
                op: "iejoin body",
                node: cfg.name.to_string(),
                detail: "emit_each fan-out is not supported in a combine body".into(),
            }),
            Err(e) => {
                if cfg.strategy == clinker_plan::config::ErrorStrategy::FailFast {
                    return Err(PipelineError::from(e));
                }
                sink.push_failure(
                    CombineOutputEvalFailure {
                        probe_record: driver_record.clone(),
                        row: driver_order,
                        matched_build: Some(build_record.clone()),
                        error: e,
                    },
                    driver_order,
                    driver_idx,
                    build_idx,
                );
                Ok(false)
            }
        }
    } else {
        // Body-less synthetic intermediate step from N-ary decomposition:
        // concatenate driver then build values onto the step's encoded schema.
        // Mirrors the HashBuildProbe passthrough; the downstream chain step
        // reads the encoded columns at their declared positions.
        let target_schema = cfg.output_schema.ok_or_else(|| PipelineError::Internal {
            op: "combine",
            node: cfg.name.to_string(),
            detail: "synthetic combine step has no output schema; \
                     decomposition pass did not run"
                .to_string(),
        })?;
        let mut values: Vec<Value> = Vec::with_capacity(target_schema.column_count());
        values.extend(driver_record.values().iter().cloned());
        values.extend(build_record.values().iter().cloned());
        if values.len() != target_schema.column_count() {
            return Err(PipelineError::Internal {
                op: "combine",
                node: cfg.name.to_string(),
                detail: format!(
                    "synthetic combine step produced {} concatenated \
                     values; encoded schema has {} columns",
                    values.len(),
                    target_schema.column_count()
                ),
            });
        }
        let rec = Record::new(Arc::clone(target_schema), values);
        sink.push_row(rec, driver_order, driver_idx, build_idx)?;
        Ok(true)
    }
}

/// Flush one driver's collect-mode row: even a zero-match driver emits, with
/// an empty array. Propagates the first matched build's `$ck.<field>` values
/// onto the synthesized row, then sets the collect array under the build
/// qualifier.
fn flush_collect_row(
    cfg: &EmitConfig<'_>,
    driver_record: &Record,
    driver_order: RecordOrder,
    driver_idx: u64,
    flush: CollectFlush,
    sink: &mut EmitSink<'_>,
) -> Result<(), PipelineError> {
    let CollectFlush {
        arr,
        truncated,
        first_build,
    } = flush;
    if truncated {
        eprintln!(
            "W: combine {:?} match: collect truncated at \
             {COLLECT_PER_GROUP_CAP} matches for driver row {driver_order}",
            cfg.name
        );
    }
    let mut rec = match cfg.output_schema {
        Some(s) => widen_record_to_schema(driver_record, s),
        None => driver_record.clone(),
    };
    if let Some(first_build) = first_build {
        crate::executor::copy_build_ck_columns(&mut rec, &first_build, cfg.propagate_ck);
    }
    rec.set(cfg.build_qualifier, Value::Array(arr));
    // A collect row carries no single build index; tag it `MAX` so the block
    // path's final sort places it after that driver's match rows (of which a
    // collect driver has none) and orders collect rows by driver input order.
    sink.push_row(rec, driver_order, driver_idx, u64::MAX)
}

/// Dispatch one zero-match driver through its `on_miss` policy: `Skip` drops
/// it, `Error` surfaces the missing-match error, `NullFields` runs the body
/// over the driver alone and emits the widened row (recoverable eval failures
/// route to `sink.output_eval_failures`).
fn dispatch_on_miss(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    driver_record: &Record,
    driver_order: RecordOrder,
    driver_idx: u64,
    on_miss: OnMiss,
    sink: &mut EmitSink<'_>,
) -> Result<(), PipelineError> {
    match on_miss {
        OnMiss::Skip => Ok(()),
        OnMiss::Error => Err(PipelineError::CombineMissingMatch {
            combine: cfg.name.to_string(),
            driver_row: driver_order,
        }),
        OnMiss::NullFields => {
            let resolver = CombineResolver::new(cfg.resolver_mapping, driver_record, None);
            let evaluator = evals.body.as_mut().ok_or_else(|| PipelineError::Internal {
                op: "combine",
                node: cfg.name.to_string(),
                detail: "combine body typed program missing for on_miss: null_fields".to_string(),
            })?;
            match evaluator.eval_record::<NullStorage>(cfg.ctx, &resolver, None) {
                Ok(EvalResult::Emit {
                    fields,
                    record_vars,
                    ..
                }) => {
                    let mut rec = match cfg.output_schema {
                        Some(s) => widen_record_to_schema(driver_record, s),
                        None => driver_record.clone(),
                    };
                    for (n, v) in fields {
                        rec.set(&n, v);
                    }
                    for (k, v) in *record_vars {
                        let _ = rec.set_record_var(&k, v);
                    }
                    // An on_miss row has no matched build; tag it `MAX` so the
                    // block path's final sort keeps it in driver input order.
                    sink.push_row(rec, driver_order, driver_idx, u64::MAX)
                }
                Ok(EvalResult::Skip(_)) => Ok(()),
                Ok(EvalResult::EmitMany { .. }) => Err(PipelineError::Internal {
                    op: "iejoin on_miss body",
                    node: cfg.name.to_string(),
                    detail: "emit_each fan-out is not supported in a combine body".into(),
                }),
                Err(e) => {
                    if cfg.strategy == clinker_plan::config::ErrorStrategy::FailFast {
                        return Err(PipelineError::from(e));
                    }
                    sink.push_failure(
                        CombineOutputEvalFailure {
                            probe_record: driver_record.clone(),
                            row: driver_order,
                            matched_build: None,
                            error: e,
                        },
                        driver_order,
                        driver_idx,
                        u64::MAX,
                    );
                    Ok(())
                }
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────

/// Outcome of extracting one record's join keys in the pre-scan. Produced in
/// parallel (per record, independently); the block-band drain replays them in
/// ascending index order into its external-sort buffers, so the sliced blocks
/// stay a pure function of the data rather than of pool scheduling.
enum RecordScan {
    /// Record has a NULL equality key (3VL, recursively — see
    /// [`canonical_key_bytes`]) or an unrepresentable range key, so it can never
    /// match: driver records route to the `on_miss` pile, build records drop.
    Unmatched,
    /// Record carries valid keys: `eq_hash` is the equality hash (the
    /// block-band's leading sort key and block-pair prune axis), `eq` the
    /// canonical equality-key bytes (the per-pair re-verify token, since hashes
    /// collide), and `range_key` the `(k1, k2)` inequality keys.
    Matched {
        eq_hash: u64,
        eq: Vec<u8>,
        range_key: (i128, i128),
    },
}

/// Why [`scan_record`] could not reduce a record's keys. `Eval` is a CXL
/// key-expression failure (routed like any other eval error); `RangeOutOfRange`
/// is a decimal range value the fixed-point axis cannot represent exactly
/// (overflow or truncation) — a fail-loud data error, never a silent drop.
#[derive(Debug)]
enum ScanKeyError {
    Eval(EvalError),
    RangeOutOfRange { value: String },
}

impl From<EvalError> for ScanKeyError {
    fn from(e: EvalError) -> Self {
        ScanKeyError::Eval(e)
    }
}

/// Extract one record's equality + range keys into a [`RecordScan`]. Pure over
/// `&self`-borrowed extractors and a read-only `EvalContext`, so it runs
/// concurrently across records without shared mutable state. Returns
/// `Unmatched` on a NULL equality key (recursively — [`canonical_key_bytes`]
/// rejects any null so the record can never join, matching
/// `keys_equal_canonicalized`'s 3VL) or an unrepresentable range key. A
/// pure-range combine has an empty equality extractor, so `eq` is empty and
/// every record hashes to one constant group — the prior pure-range behavior.
fn scan_record(
    eq_extractor: &KeyExtractor,
    range_extractor: &KeyExtractor,
    range_kinds: &[RangeKeyType],
    ctx: &EvalContext<'_>,
    resolver: &dyn cxl::resolve::traits::FieldResolver,
) -> Result<RecordScan, ScanKeyError> {
    let mut eq_buf: Vec<Value> = Vec::new();
    if !eq_extractor.is_empty() {
        eq_extractor.extract_into(ctx, resolver, &mut eq_buf)?;
    }
    // A null anywhere in the equality key makes it never-joinable (SQL 3VL), so
    // route the record off the range walk rather than carrying a hash for it.
    let Some(eq) = canonical_key_bytes(&eq_buf) else {
        return Ok(RecordScan::Unmatched);
    };
    let mut range_buf: Vec<Value> = Vec::new();
    range_extractor.extract_into(ctx, resolver, &mut range_buf)?;
    let Some(range_key) = range_keys_to_i128_pair(&range_buf, range_kinds)? else {
        return Ok(RecordScan::Unmatched);
    };
    // Derive the prune hash from the canonical bytes: equal equality keys have
    // equal bytes and so hash identically (they cluster under the block-band
    // sort), and this agrees with the per-pair re-verify by construction while
    // avoiding a second walk of the key Values. Distinct keys may collide, which
    // the re-verify catches.
    let eq_hash = eq_hash_of_bytes(&eq);
    Ok(RecordScan::Matched {
        eq_hash,
        eq,
        range_key,
    })
}

/// Hash the canonical equality-key bytes with a FIXED seed. Block-band-local (it
/// does not touch the shared [`hash_composite_key`] the equi-hash join and other
/// strategies depend on): the hash only groups and prunes and never reaches
/// output, so any stable, well-distributed hash of the canonical bytes suffices.
/// A fixed seed makes the sliced block layout identical run to run rather than
/// varying with a process-random seed.
fn eq_hash_of_bytes(bytes: &[u8]) -> u64 {
    use std::hash::{BuildHasher, Hasher};
    use std::sync::OnceLock;
    static STATE: OnceLock<RandomState> = OnceLock::new();
    let state = STATE.get_or_init(|| {
        RandomState::with_seeds(
            0x9e37_79b9_7f4a_7c15,
            0xc2b2_ae3d_27d4_eb4f,
            0x1656_67b1_9e37_79f9,
            0x2545_f491_4f6c_dd1d,
        )
    });
    let mut hasher = state.build_hasher();
    hasher.write(bytes);
    hasher.finish()
}

/// Convert a 1-or-2-element vector of range key Values to an order-preserving
/// `(i128, i128)` pair, one axis kind per element (the axis's static numeric
/// flavor from the typechecker). Returns `Ok(None)` when any element is NULL or
/// not reducible to the numeric axis (routed to the unmatched pile as before),
/// and `Err` when a decimal element is outside the exact fixed-point range —
/// fail-loud rather than a silent drop. Single-axis callers (PWMJ) ignore the
/// second slot; the placeholder zero keeps the layout uniform across kernels.
fn range_keys_to_i128_pair(
    values: &[Value],
    kinds: &[RangeKeyType],
) -> Result<Option<(i128, i128)>, ScanKeyError> {
    let n = values.len();
    if n == 0 {
        return Ok(None);
    }
    let kind0 = kinds.first().copied().unwrap_or(RangeKeyType::Unsupported);
    let Some(v1) = value_to_i128(kind0, &values[0])? else {
        return Ok(None);
    };
    if n == 1 {
        return Ok(Some((v1, 0)));
    }
    let kind1 = kinds.get(1).copied().unwrap_or(RangeKeyType::Unsupported);
    let Some(v2) = value_to_i128(kind1, &values[1])? else {
        return Ok(None);
    };
    Ok(Some((v1, v2)))
}

/// Reduce one range-axis Value to the order-preserving `i128` its axis kind
/// dictates, so both cross-input sides land in one comparable space. The kind is
/// fixed at plan time from the concrete operand types, matching the runtime
/// `compare_values` semantics: an integer on a mixed int/float axis coerces
/// through `f64`, and an integer on a decimal axis widens exactly onto the same
/// fixed-point grid.
///
/// Only the reducible kinds reach here — `Numeric` and `Unsupported` are
/// rejected at plan time (E327), so their arms are defensively `None`. `Ok(None)`
/// otherwise means a NULL / off-type value routed to the unmatched pile; `Err`
/// only when a decimal value cannot be placed on the fixed-point grid exactly.
#[inline]
fn value_to_i128(kind: RangeKeyType, v: &Value) -> Result<Option<i128>, ScanKeyError> {
    use crate::pipeline::sort_key::{
        decimal_to_orderable_i128, float_to_orderable_i128, integer_on_decimal_grid,
    };
    use chrono::Datelike;
    let out = match kind {
        RangeKeyType::Integer => match v {
            Value::Integer(i) => Some(*i as i128),
            _ => None,
        },
        // A pure-float axis sees only floats; a mixed int/float axis coerces the
        // integer side through `f64`, the same widening `compare_values` applies
        // to `int <=> float`. Both reduce through the orderable-float transform.
        RangeKeyType::Float | RangeKeyType::MixedNumeric => match v {
            Value::Float(f) if f.is_finite() => Some(float_to_orderable_i128(*f)),
            Value::Integer(i) => Some(float_to_orderable_i128(*i as f64)),
            _ => None,
        },
        RangeKeyType::Decimal => match v {
            Value::Decimal(d) => match decimal_to_orderable_i128(*d) {
                Some(k) => Some(k),
                None => {
                    return Err(ScanKeyError::RangeOutOfRange {
                        value: v.to_string(),
                    });
                }
            },
            // Integer operand widened exactly into the decimal context.
            Value::Integer(i) => match integer_on_decimal_grid(*i) {
                Some(k) => Some(k),
                None => {
                    return Err(ScanKeyError::RangeOutOfRange {
                        value: v.to_string(),
                    });
                }
            },
            _ => None,
        },
        RangeKeyType::Date => match v {
            Value::Date(d) => Some(d.num_days_from_ce() as i128),
            _ => None,
        },
        RangeKeyType::DateTime => match v {
            Value::DateTime(dt) => Some(dt.and_utc().timestamp_micros() as i128),
            _ => None,
        },
        // Plan-rejected (E327) before runtime; defensively route out.
        RangeKeyType::Numeric | RangeKeyType::Unsupported => None,
    };
    Ok(out)
}

/// Estimated bytes of the numeric-IEJoin internal working set for a group
/// of `n_left` driver and `n_right` build range keys: the two
/// `(signed_idx, op1_key, op2_key)` sort arrays (L1 and L2), the two
/// `usize` position/permutation vectors, and the visited bit array (one
/// `u64` word per 64 positions plus one coarse `u64` word per 1024). This
/// is the state [`iejoin_numeric`] allocates before it materializes any
/// output pair, so charging it lets the budget abort a doomed band join
/// before the O(n) sort arrays are even built — the RSS-independent gate
/// the wasm build and other hosts without a `rss_bytes()` reading rely on.
/// Saturating arithmetic keeps a pathological count from wrapping the
/// estimate back under the budget.
fn iejoin_numeric_state_bytes(n_left: usize, n_right: usize) -> usize {
    let n_total = n_left.saturating_add(n_right);
    // L1 + L2: two `Vec<(i64, i128, i128)>` of length n_total (signed index +
    // the two i128 range keys).
    let sort_arrays = n_total
        .saturating_mul(std::mem::size_of::<(i64, i128, i128)>())
        .saturating_mul(2);
    // l1_pos_of_dense + p: two `Vec<usize>` of length n_total.
    let perm = n_total
        .saturating_mul(std::mem::size_of::<usize>())
        .saturating_mul(2);
    // Bit array: one u64 word per 64 positions plus one coarse u64 word per
    // 1024 positions — mirrors `IEJoinBitArray::new`.
    let bit = n_total
        .div_ceil(64)
        .saturating_add(n_total.div_ceil(1024).div_ceil(64))
        .saturating_mul(std::mem::size_of::<u64>());
    sort_arrays.saturating_add(perm).saturating_add(bit)
}

/// Estimated bytes of the single-inequality PWMJ working set for a group of
/// `n_left` driver and `n_right` build keys: the two `Vec<usize>` sorted-index
/// arrays [`pwmj_numeric`] builds (one per side), plus the merge scratch the
/// stable `slice::sort_by` transiently allocates while sorting each of them —
/// the sides sort sequentially, so the larger side bounds that transient peak.
/// Far smaller than [`iejoin_numeric_state_bytes`] — no L1/L2 key-triple
/// arrays, permutation vector, or visited bit array — so the block-band gate
/// charges the PWMJ kernel its real footprint instead of the dual-conjunct
/// estimate. Saturating arithmetic keeps a pathological count from wrapping
/// the estimate back under the budget.
fn pwmj_numeric_state_bytes(n_left: usize, n_right: usize) -> usize {
    let index_arrays = n_left
        .saturating_add(n_right)
        .saturating_mul(std::mem::size_of::<usize>());
    let sort_scratch = n_left
        .max(n_right)
        .saturating_mul(std::mem::size_of::<usize>());
    index_arrays.saturating_add(sort_scratch)
}

/// Build the typed pre-output budget-abort error, shared by both dispatch
/// shapes. On the equi+range path it fires when the resident partition and
/// per-group sort arrays exceed the budget (gated through the arbitrator's
/// `should_abort_local`, since that path holds its inputs resident with no
/// spill). On the block-band path it is a strictly LOCAL last resort — the one
/// loaded block-pair's resident bytes plus kernel aux exceed the hard limit
/// even alone — gated by a direct `peak > hard_limit` comparison, never by
/// global process pressure, because that path answers pressure by spilling.
/// Either way it surfaces `MemoryBudgetExceeded` with `BudgetCategory::Arena`,
/// the same shape the output-buffer poll and every other budget-checked
/// operator surface use.
fn pre_output_budget_error(name: &str, used: u64, limit: u64) -> PipelineError {
    PipelineError::MemoryBudgetExceeded {
        node: name.to_string(),
        used,
        limit,
        source: BudgetCategory::Arena,
        detail: Some("iejoin pre-output state exceeded budget".to_string()),
    }
}

fn key_eval_error(name: &str, side: &'static str, err: EvalError) -> PipelineError {
    PipelineError::Compilation {
        transform_name: name.to_string(),
        messages: vec![format!("combine {side}-side range/key eval error: {err}")],
    }
}

/// Map a [`ScanKeyError`] to the typed pipeline error for its class: a CXL
/// key-expression failure through [`key_eval_error`], and a decimal range value
/// beyond the fixed-point axis to the fail-loud E326 abort (never a silent
/// drop). `side` names the combine input for the diagnostic.
fn scan_key_error(name: &str, side: &'static str, err: ScanKeyError) -> PipelineError {
    match err {
        ScanKeyError::Eval(e) => key_eval_error(name, side, e),
        ScanKeyError::RangeOutOfRange { value } => PipelineError::CombineRangeKeyOutOfRange {
            combine: name.to_string(),
            value,
        },
    }
}

/// Placeholder `RecordStorage` for windowless body / residual
/// evaluation. Same role as `executor::mod::NullStorage`.
struct NullStorage;

impl clinker_record::RecordStorage for NullStorage {
    fn resolve_field(&self, _: u64, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _: u64) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u64 {
        0
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::HashSet;

    #[test]
    fn test_bit_array_set_and_scan() {
        let mut ba = IEJoinBitArray::new(2048);
        ba.set(0);
        ba.set(1023);
        ba.set(1024);
        let from_0: Vec<usize> = ba.iter_set_from(0).collect();
        assert_eq!(from_0, vec![0, 1023, 1024]);
        let from_1: Vec<usize> = ba.iter_set_from(1).collect();
        assert_eq!(from_1, vec![1023, 1024]);
        let from_1024: Vec<usize> = ba.iter_set_from(1024).collect();
        assert_eq!(from_1024, vec![1024]);
        let from_1025: Vec<usize> = ba.iter_set_from(1025).collect();
        assert!(from_1025.is_empty());
    }

    #[test]
    fn test_bit_array_coarse_filter_skip() {
        let mut ba = IEJoinBitArray::new(100_000);
        ba.set(50_000);
        let found: Vec<usize> = ba.iter_set_from(0).collect();
        assert_eq!(found, vec![50_000]);
    }

    #[test]
    fn test_iejoin_numeric_state_bytes_counts_and_grows() {
        // Empty input allocates no sort arrays or permutation vectors, and
        // the two backing bit-array words `IEJoinBitArray::new` always keeps
        // (`words.max(1)` + `regions.div_ceil(64).max(1)`) are only sized on
        // a non-empty run — so a zero-length group estimates zero bytes.
        assert_eq!(iejoin_numeric_state_bytes(0, 0), 0);

        // A concrete group: the estimate must equal the exact per-component
        // sum the kernel allocates so it tracks real footprint, not a fudge
        // factor. n_total = 300.
        let n_total = 300usize;
        let expected = n_total * std::mem::size_of::<(i64, i128, i128)>() * 2 // L1 + L2
            + n_total * std::mem::size_of::<usize>() * 2 // l1_pos_of_dense + p
            + (n_total.div_ceil(64) + n_total.div_ceil(1024).div_ceil(64))
                * std::mem::size_of::<u64>(); // bit array
        assert_eq!(iejoin_numeric_state_bytes(100, 200), expected);

        // Strictly monotonic in the total group size — a bigger band join
        // can only estimate a larger working set, which is what makes the
        // pre-output abort trip earlier on wider inputs.
        assert!(iejoin_numeric_state_bytes(1_000, 1_000) > iejoin_numeric_state_bytes(100, 100));

        // The estimate for a non-trivial group must exceed a 1-byte budget,
        // so `should_abort_local` short-circuits on the local arm WITHOUT
        // consulting RSS — the RSS-blind backstop the gate exists to
        // provide. Uses `NoOpPolicy` so no victim selection interferes.
        let budget = MemoryArbitrator::with_policy(
            1,
            0.8,
            0.70,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        );
        let est = iejoin_numeric_state_bytes(500, 500) as u64;
        assert!(est > 1);
        assert!(
            budget.should_abort_local(est),
            "a {est}-byte working-set estimate must trip a 1-byte budget on the \
             local arm regardless of RSS availability"
        );

        // Per-pair charge cadence (block-band path). The block scheduler
        // charges this estimator with BLOCK-sized arguments once per surviving
        // block-pair, not with the whole-input counts, and pruning drops most
        // pairs before they are charged at all. So the estimate paid per pair is
        // bounded by the block size and is a small fraction of the whole-input
        // estimate — the property that keeps the input axis bounded. Two
        // block-sized pairs (64 records each) must together stay well under the
        // whole-input estimate for a 4000×4000 join.
        let whole_input = iejoin_numeric_state_bytes(4_000, 4_000);
        let per_pair = iejoin_numeric_state_bytes(64, 64);
        assert!(
            per_pair.saturating_mul(2) < whole_input / 20,
            "two block-sized per-pair charges ({} bytes) must stay far below the \
             whole-input estimate ({whole_input} bytes); the scheduler pays the \
             estimator per surviving pair with block-sized counts, so it never \
             materializes the whole-input working set at once",
            per_pair.saturating_mul(2)
        );
    }

    #[derive(Clone, Copy, Debug)]
    enum TOp {
        Lt,
        Le,
        Gt,
        Ge,
    }

    impl TOp {
        fn apply(self, a: i128, b: i128) -> bool {
            match self {
                TOp::Lt => a < b,
                TOp::Le => a <= b,
                TOp::Gt => a > b,
                TOp::Ge => a >= b,
            }
        }
        fn apply_f64(self, a: f64, b: f64) -> bool {
            match self {
                TOp::Lt => a < b,
                TOp::Le => a <= b,
                TOp::Gt => a > b,
                TOp::Ge => a >= b,
            }
        }
        fn to_range(self) -> RangeOp {
            match self {
                TOp::Lt => RangeOp::Lt,
                TOp::Le => RangeOp::Le,
                TOp::Gt => RangeOp::Gt,
                TOp::Ge => RangeOp::Ge,
            }
        }
    }

    /// One proptest case: left rows, right rows, and the two range
    /// operators forming the band predicate.
    type JoinCase = (Vec<(i128, i128)>, Vec<(i128, i128)>, TOp, TOp);

    fn nested_loop(
        left: &[(i128, i128)],
        right: &[(i128, i128)],
        op1: TOp,
        op2: TOp,
    ) -> HashSet<(usize, usize)> {
        let mut out = HashSet::new();
        for (li, l) in left.iter().enumerate() {
            for (ri, r) in right.iter().enumerate() {
                if op1.apply(l.0, r.0) && op2.apply(l.1, r.1) {
                    out.insert((li, ri));
                }
            }
        }
        out
    }

    #[test]
    fn test_iejoin_numeric_basic() {
        // Tax-bracket-shaped 3 left × 3 right.
        // Predicate: left.0 <= right.0 AND left.1 >= right.1.
        let left = vec![(10_i128, 100_i128), (20, 50), (5, 200)];
        let right = vec![(15_i128, 80_i128), (25, 150), (8, 60)];
        let actual: HashSet<(usize, usize)> =
            iejoin_numeric(&left, &right, RangeOp::Le, RangeOp::Ge)
                .into_iter()
                .collect();
        let expected = nested_loop(&left, &right, TOp::Le, TOp::Ge);
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_iejoin_numeric_all_16_op_pairs() {
        let left = vec![(1, 10), (3, 30), (5, 50), (2, 20), (4, 40)];
        let right = vec![(2, 15), (3, 25), (4, 35), (1, 5)];
        let ops = [TOp::Lt, TOp::Le, TOp::Gt, TOp::Ge];
        for op1 in ops {
            for op2 in ops {
                let actual: HashSet<(usize, usize)> =
                    iejoin_numeric(&left, &right, op1.to_range(), op2.to_range())
                        .into_iter()
                        .collect();
                let expected = nested_loop(&left, &right, op1, op2);
                assert_eq!(actual, expected, "mismatch for ({:?}, {:?})", op1, op2);
            }
        }
    }

    #[test]
    fn test_iejoin_numeric_empty_inputs() {
        let empty: Vec<(i128, i128)> = Vec::new();
        let some = vec![(1_i128, 2_i128)];
        assert!(iejoin_numeric(&empty, &some, RangeOp::Lt, RangeOp::Lt).is_empty());
        assert!(iejoin_numeric(&some, &empty, RangeOp::Lt, RangeOp::Lt).is_empty());
        assert!(iejoin_numeric(&empty, &empty, RangeOp::Lt, RangeOp::Lt).is_empty());
    }

    #[test]
    fn test_iejoin_numeric_single_row() {
        let left = vec![(5_i128, 10_i128)];
        let right = vec![(3_i128, 8_i128)];
        // 5 > 3 AND 10 > 8 → match
        let m: HashSet<_> = iejoin_numeric(&left, &right, RangeOp::Gt, RangeOp::Gt)
            .into_iter()
            .collect();
        assert_eq!(m, HashSet::from([(0, 0)]));
        // 5 < 3 AND 10 > 8 → no match (left.0 < right.0 fails)
        let n: HashSet<_> = iejoin_numeric(&left, &right, RangeOp::Lt, RangeOp::Gt)
            .into_iter()
            .collect();
        assert!(n.is_empty());
    }

    #[test]
    fn test_iejoin_numeric_all_duplicates() {
        let left: Vec<(i128, i128)> = (0..10).map(|_| (5_i128, 5_i128)).collect();
        let right: Vec<(i128, i128)> = (0..10).map(|_| (5_i128, 5_i128)).collect();
        // Both ops inclusive — full Cartesian = 100 pairs.
        let actual: HashSet<(usize, usize)> =
            iejoin_numeric(&left, &right, RangeOp::Le, RangeOp::Ge)
                .into_iter()
                .collect();
        assert_eq!(actual.len(), 100);
        // Strict on one axis — zero pairs (left.0 < right.0 fails for
        // equal values).
        let strict: HashSet<(usize, usize)> =
            iejoin_numeric(&left, &right, RangeOp::Lt, RangeOp::Ge)
                .into_iter()
                .collect();
        assert!(strict.is_empty());
    }

    #[test]
    fn test_pwmj_numeric_basic() {
        let left = vec![1_i128, 3, 5, 7];
        let right = vec![2_i128, 4, 6, 8];
        for op in [RangeOp::Lt, RangeOp::Le, RangeOp::Gt, RangeOp::Ge] {
            let actual: HashSet<(usize, usize)> =
                pwmj_numeric(&left, &right, op).into_iter().collect();
            let mut expected = HashSet::new();
            for (li, &lv) in left.iter().enumerate() {
                for (ri, &rv) in right.iter().enumerate() {
                    let hit = match op {
                        RangeOp::Lt => lv < rv,
                        RangeOp::Le => lv <= rv,
                        RangeOp::Gt => lv > rv,
                        RangeOp::Ge => lv >= rv,
                    };
                    if hit {
                        expected.insert((li, ri));
                    }
                }
            }
            assert_eq!(actual, expected, "pwmj mismatch under {:?}", op);
        }
    }

    #[test]
    fn test_pwmj_numeric_empty() {
        let empty: Vec<i128> = Vec::new();
        let some = vec![1_i128];
        assert!(pwmj_numeric(&empty, &some, RangeOp::Lt).is_empty());
        assert!(pwmj_numeric(&some, &empty, RangeOp::Lt).is_empty());
        assert!(pwmj_numeric(&empty, &empty, RangeOp::Lt).is_empty());
    }

    /// A range-key value skewed toward small clustered integers (so duplicate
    /// keys and boundary equality get exercised) but also drawing values that
    /// straddle the `i64` limits and span the full `i128` range — the widened
    /// axis must compare those correctly, which a `-50..50` generator alone
    /// (all within `i64`) could never catch.
    fn arb_i128_key() -> impl Strategy<Value = i128> {
        prop_oneof![
            4 => -50i128..50,
            1 => (i64::MAX as i128 - 5)..(i64::MAX as i128 + 5),
            1 => (i64::MIN as i128 - 5)..(i64::MIN as i128 + 5),
            1 => any::<i128>(),
        ]
    }

    fn arb_inputs() -> impl Strategy<Value = JoinCase> {
        let op = prop_oneof![Just(TOp::Lt), Just(TOp::Le), Just(TOp::Gt), Just(TOp::Ge)];
        (
            prop::collection::vec((arb_i128_key(), arb_i128_key()), 0..50),
            prop::collection::vec((arb_i128_key(), arb_i128_key()), 0..50),
            op.clone(),
            op,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        #[test]
        fn proptest_iejoin_matches_nested_loop(
            (left, right, op1, op2) in arb_inputs()
        ) {
            let actual: HashSet<(usize, usize)> =
                iejoin_numeric(&left, &right, op1.to_range(), op2.to_range())
                    .into_iter()
                    .collect();
            let expected = nested_loop(&left, &right, op1, op2);
            prop_assert_eq!(actual, expected);
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        /// The cap-aware kernel returns the identical full result whenever the
        /// actual match count fits `max_pairs`, and overflows (`None`) exactly
        /// when it does not — so a caller can trust `None` to mean "dense pair,
        /// fall back" and `Some` to be the complete, materialization-safe vector.
        #[test]
        fn proptest_iejoin_capped_equals_uncapped_or_overflows(
            (left, right, op1, op2) in arb_inputs(),
            cap in 0usize..600,
        ) {
            let uncapped = iejoin_numeric(&left, &right, op1.to_range(), op2.to_range());
            let capped =
                iejoin_numeric_capped(&left, &right, op1.to_range(), op2.to_range(), cap);
            if uncapped.len() <= cap {
                // Non-overflowing: byte-identical to the uncapped kernel, order
                // and all — the capped path shares the exact enumeration.
                prop_assert_eq!(capped, Some(uncapped));
            } else {
                // Overflow fires precisely when the true count exceeds the cap.
                prop_assert_eq!(capped, None);
            }
        }

        /// Same contract for the single-inequality PWMJ kernel.
        #[test]
        fn proptest_pwmj_capped_equals_uncapped_or_overflows(
            left in prop::collection::vec(arb_i128_key(), 0..40),
            right in prop::collection::vec(arb_i128_key(), 0..40),
            op_idx in 0usize..4,
            cap in 0usize..400,
        ) {
            let op = [RangeOp::Lt, RangeOp::Le, RangeOp::Gt, RangeOp::Ge][op_idx];
            let uncapped = pwmj_numeric(&left, &right, op);
            let capped = pwmj_numeric_capped(&left, &right, op, cap);
            if uncapped.len() <= cap {
                prop_assert_eq!(capped, Some(uncapped));
            } else {
                prop_assert_eq!(capped, None);
            }
        }
    }

    /// One float proptest case: left/right float rows and the two range
    /// operators of the band predicate.
    type FloatJoinCase = (Vec<(f64, f64)>, Vec<(f64, f64)>, TOp, TOp);

    /// Brute-force nested-loop join over the ORIGINAL float values. The
    /// kernel compares `value_to_i64`-encoded keys, so agreement with this
    /// oracle is exactly the order-preservation property under test.
    fn nested_loop_f64(
        left: &[(f64, f64)],
        right: &[(f64, f64)],
        op1: TOp,
        op2: TOp,
    ) -> HashSet<(usize, usize)> {
        let mut out = HashSet::new();
        for (li, l) in left.iter().enumerate() {
            for (ri, r) in right.iter().enumerate() {
                if op1.apply_f64(l.0, r.0) && op2.apply_f64(l.1, r.1) {
                    out.insert((li, ri));
                }
            }
        }
        out
    }

    /// Finite `f64` sample skewed toward small recurring integers (so
    /// duplicate keys and boundary equality get exercised), the signed
    /// zeros, subnormals, and a wide continuous spread reaching large
    /// magnitudes of both signs. The negative values are the whole point:
    /// a raw `to_bits() as i64` encoding is only non-monotone once the
    /// sign bit is set, so a strategy without negatives cannot catch it.
    fn arb_float_key() -> impl Strategy<Value = f64> {
        prop_oneof![
            4 => (-8i64..8).prop_map(|n| n as f64),
            1 => Just(0.0f64),
            1 => Just(-0.0f64),
            1 => Just(f64::MIN_POSITIVE),
            1 => Just(-f64::MIN_POSITIVE),
            2 => -1.0e6f64..1.0e6,
        ]
    }

    fn arb_float_inputs() -> impl Strategy<Value = FloatJoinCase> {
        let op = prop_oneof![Just(TOp::Lt), Just(TOp::Le), Just(TOp::Gt), Just(TOp::Ge)];
        (
            prop::collection::vec((arb_float_key(), arb_float_key()), 0..40),
            prop::collection::vec((arb_float_key(), arb_float_key()), 0..40),
            op.clone(),
            op,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        /// The kernel result over `value_to_i64`-encoded float keys must
        /// equal the float nested-loop oracle for every operator pair,
        /// including negatives and signed zeros. Under the old
        /// `f.to_bits() as i64` arm this diverged: among negative floats a
        /// larger magnitude encodes to a larger i64, inverting their order,
        /// and `-0.0` encodes to `i64::MIN` rather than `0` — so range
        /// matches over negative keys went wrong.
        #[test]
        fn proptest_iejoin_float_keys_match_nested_loop(
            (left, right, op1, op2) in arb_float_inputs()
        ) {
            // Every generated value is finite, so the float axis reduction is
            // `Ok(Some(..))`.
            let enc = |rows: &[(f64, f64)]| -> Vec<(i128, i128)> {
                rows.iter()
                    .map(|&(a, b)| {
                        (
                            value_to_i128(RangeKeyType::Float, &Value::Float(a))
                                .unwrap()
                                .unwrap(),
                            value_to_i128(RangeKeyType::Float, &Value::Float(b))
                                .unwrap()
                                .unwrap(),
                        )
                    })
                    .collect()
            };
            let left_i = enc(&left);
            let right_i = enc(&right);
            let actual: HashSet<(usize, usize)> =
                iejoin_numeric(&left_i, &right_i, op1.to_range(), op2.to_range())
                    .into_iter()
                    .collect();
            let expected = nested_loop_f64(&left, &right, op1, op2);
            prop_assert_eq!(actual, expected);
        }
    }

    /// Locks `iejoin_numeric` against a nested-loop oracle at the 1K × 1K
    /// scale used by `combine_iejoin.rs`'s correctness gate. The proptest
    /// above runs against random inputs up to 50 × 50; this test pins the
    /// tax-bracket-shaped workload (`>=` lo and `<` hi) at a thousand rows
    /// per side so a regression in the larger-array code path — coarse
    /// filter striding, permutation indexing — surfaces in CI even though
    /// the bench harness itself is not part of `cargo test --workspace`.
    #[test]
    fn test_iejoin_against_nested_loop_at_scale_1k() {
        // 50 brackets tiling [0, 100_000) on the lo axis with hi = lo +
        // step; 1000 employees with incomes spread uniformly across the
        // covered range. `iejoin_numeric` does NOT see the entity_id
        // equality — the executor applies that as a partition key before
        // calling this kernel — so this test models a single equality
        // partition with 1000 drivers × 50 builds. The remaining 950
        // builds + drivers (to reach 1K × 1K) come from a second
        // partition modelled by appending another tile sequence with a
        // disjoint income range, exercising the same kernel on a wider
        // input.
        const SPAN: i64 = 100_000;
        let n_brackets_per_tile = 50usize;
        let n_employees_per_tile = 500usize;
        let step = SPAN / n_brackets_per_tile as i64;

        let mut left: Vec<(i128, i128)> = Vec::with_capacity(2 * n_employees_per_tile);
        let mut right: Vec<(i128, i128)> = Vec::with_capacity(2 * n_brackets_per_tile);
        for tile in 0usize..2 {
            let base = tile as i64 * SPAN;
            for b in 0..n_brackets_per_tile {
                let lo = base + b as i64 * step;
                let hi = lo + step;
                // For driver.income >= build.bracket_lo AND driver.income
                // < build.bracket_hi we need build.0 = bracket_lo (Le
                // when flipped to driver-first) and build.1 = bracket_hi
                // (Gt when flipped). The kernel sees the predicate as
                // `left OP right` so the build is on the right; we pass
                // (bracket_lo, bracket_hi) as the right tuple.
                right.push((lo as i128, hi as i128));
            }
            for e in 0..n_employees_per_tile {
                // Mix per-row so income lands on different bracket
                // boundaries than a strict modulo would produce.
                let mix =
                    ((tile * n_employees_per_tile + e) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                let income = base + (mix as i64).rem_euclid(SPAN);
                left.push((income as i128, income as i128));
            }
        }

        // Predicate driver.income >= build.bracket_lo AND driver.income
        // < build.bracket_hi. With the kernel's `left OP right`
        // convention, axis-1 is `Ge` (driver_income >= bracket_lo) and
        // axis-2 is `Lt` (driver_income < bracket_hi).
        let actual: HashSet<(usize, usize)> =
            iejoin_numeric(&left, &right, RangeOp::Ge, RangeOp::Lt)
                .into_iter()
                .collect();

        // Oracle: same predicate via direct nested loop on the same
        // (i128, i128) keys.
        let mut expected: HashSet<(usize, usize)> = HashSet::new();
        for (li, l) in left.iter().enumerate() {
            for (ri, r) in right.iter().enumerate() {
                if l.0 >= r.0 && l.1 < r.1 {
                    expected.insert((li, ri));
                }
            }
        }
        assert_eq!(
            actual,
            expected,
            "1K-scale tax-bracket workload diverged: iejoin {} pairs, \
             nested-loop {} pairs",
            actual.len(),
            expected.len()
        );
    }
}
