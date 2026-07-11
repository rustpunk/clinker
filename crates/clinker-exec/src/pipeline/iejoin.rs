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
//! **Memory model.** Two dispatch shapes with different input-axis bounds:
//!
//! - **Pure-range (`partition_bits: None`)** runs the bounded block-band path
//!   in the [`block`] child module. Each side is external-sorted on the
//!   primary inequality key (spilling its overflow to disk), the merged stream
//!   is sliced into contiguous min/max-tagged blocks, non-overlapping
//!   block-pairs are pruned, and the numeric kernel runs per surviving pair. At
//!   most one driver block and one matching build block plus that pair's L1/L2
//!   sort arrays are resident at once, so the INPUT axis is bounded by the
//!   budget rather than the input size.
//! - **Equi+range (`partition_bits: Some(bits)`)** hash-partitions on the
//!   equality keys and holds each partition's records, range-key arrays, and
//!   the per-group L1/L2 / permutation / bit-array state resident. This side
//!   has no spill path yet (a follow-up bounds it); its pre-output working set
//!   is estimated and gated through [`MemoryArbitrator::should_abort_local`] at
//!   partition-build completion and again per equality group before the range
//!   walk — an RSS-independent check that aborts a doomed join before the sort
//!   arrays are built, on hosts where `rss_bytes()` is unavailable.
//!
//! Both shapes mirror their live footprint into a registered
//! [`ConsumerHandle`] so the arbitrator's pull-mode `current_usage` sees it,
//! and both poll the accumulated OUTPUT buffer every 10K emitted matched pairs
//! through the arbitrator's global-aware `should_abort_local` — the output
//! axis (`output_records`, O(N·M) worst case) is un-spillable until a later
//! phase streams it, so reacting to global pressure there is correct. The
//! pre-output abort returns a typed `PipelineError::MemoryBudgetExceeded`
//! carrying the combine node's name and `BudgetCategory::Arena`. On the
//! equi+range path it fires when the resident partition and per-group sort
//! arrays exceed the budget; on the block-band path it is a strictly LOCAL
//! last resort — a single block-pair plus kernel aux exceeding the hard limit
//! even alone — so a spilling, bounded-residency run never aborts merely
//! because process RSS sits above a tight budget. Global pressure on the input
//! axis is answered by spilling, not by aborting.

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
use crate::pipeline::combine::{
    CombineKernelOutput, CombineOutputEvalFailure, KeyExtractor, hash_composite_key,
    keys_equal_canonicalized,
};
use crate::pipeline::memory::{ConsumerHandle, MemoryArbitrator};
use clinker_plan::BudgetCategory;
use clinker_plan::config::pipeline_node::{MatchMode, OnMiss};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::combine::{DecomposedPredicate, RangeOp};

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

/// Compare two `i64` keys under a sort direction.
#[inline]
fn cmp_dir(a: i64, b: i64, dir: SortDir) -> std::cmp::Ordering {
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
fn iejoin_numeric(
    left_keys: &[(i64, i64)],
    right_keys: &[(i64, i64)],
    op1: RangeOp,
    op2: RangeOp,
) -> Vec<(usize, usize)> {
    let n_left = left_keys.len();
    let n_right = right_keys.len();
    let n_total = n_left + n_right;
    if n_left == 0 || n_right == 0 {
        return Vec::new();
    }

    // Encode left record `i` as +(i+1), right record `j` as -(j+1).
    // i64 fits both signs without `-0` ambiguity.
    let mut l1: Vec<(i64, i64, i64)> = Vec::with_capacity(n_total);
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
            }
        } else {
            // RIGHT side: mark its L1 position as visited.
            let l1_pos = p[l2_pos];
            bit.set(l1_pos);
        }
    }
    out
}

/// Single-inequality piecewise merge join — universal across DuckDB,
/// Polars, and DataFusion (drill D34). Sorts both sides by the
/// condition key, then enumerates pairs that satisfy the predicate.
fn pwmj_numeric(left_keys: &[i64], right_keys: &[i64], op: RangeOp) -> Vec<(usize, usize)> {
    let n_left = left_keys.len();
    let n_right = right_keys.len();
    if n_left == 0 || n_right == 0 {
        return Vec::new();
    }
    let dir = primary_dir(op);
    let mut left_idx: Vec<usize> = (0..n_left).collect();
    let mut right_idx: Vec<usize> = (0..n_right).collect();
    left_idx.sort_by(|&a, &b| cmp_dir(left_keys[a], left_keys[b], dir).then(a.cmp(&b)));
    right_idx.sort_by(|&a, &b| cmp_dir(right_keys[a], right_keys[b], dir).then(a.cmp(&b)));

    let apply = |a: i64, b: i64| -> bool {
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
            }
        }
    }
    out
}

// ──────────────────────────────────────────────────────────────────────────
// Public entry point: execute_combine_iejoin
// ──────────────────────────────────────────────────────────────────────────

/// Execute a combine node via IEJoin (or PWMJ for single-inequality
/// predicates). Hash-partitions on equality keys when `partition_bits`
/// is `Some`; runs as a single virtual partition when `None`
/// (pure-range combines).
///
/// The caller (executor's Combine arm) is responsible for input schema
/// validation and predecessor buffer routing; this function trusts its
/// inputs and constructs records on the combine's `output_schema`.
///
/// Output `RecordOrder` carries the driver record's original ordering
/// tag — same convention HashBuildProbe uses. Within a single driver,
/// matches are emitted in the IEJoin scan order; downstream sort is
/// the caller's responsibility.
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
    /// `Some(bits)` → hash-partition driver and build by their
    /// equality-key tuple into `2^bits` buckets before the IEJoin
    /// scan. `None` → run as a single virtual partition (pure-range
    /// case, where there are no equality keys to partition on).
    pub partition_bits: Option<u8>,
    /// Build-side `$ck.<field>` propagation policy. Mirrors the
    /// HashBuildProbe arm — every strategy threads the same spec to
    /// the shared `copy_build_ck_columns` helper at every emit site.
    pub propagate_ck: &'a clinker_plan::config::pipeline_node::PropagateCkSpec,
    pub ctx: &'a EvalContext<'a>,
    pub budget: &'a MemoryArbitrator,
    /// Shared handle for the IEJoin's registered `MemoryConsumer` wrapper.
    /// Both dispatch shapes mirror their live working-set bytes into this
    /// handle (the block-band path its drain buffers and resident blocks; the
    /// equi+range path its partition state and per-group sort arrays), so the
    /// arbitrator's pull-mode `current_usage` reads the footprint while the
    /// join runs. Neither shape reads the handle's spill / pause flags — the
    /// block-band path spills on its own byte threshold and the equi+range
    /// path holds its inputs resident — so the handle carries the byte
    /// estimate for attribution and the `should_abort_local` gate only.
    pub consumer: &'a Arc<ConsumerHandle>,
    /// Pipeline-scoped spill directory borrowed from
    /// `ExecutorContext::spill_root_path`. The pure-range (`partition_bits:
    /// None`) block-band path external-sorts each side and writes its
    /// min/max-tagged blocks here; the equi+range (`Some`) path holds its
    /// inputs resident and never touches it.
    pub spill_dir: &'a Path,
    /// Whether the block-band path's spill runs and block files are
    /// LZ4-compressed. Resolved by the dispatcher from the workspace
    /// `[storage.spill] compress` knob against the combine's schema width and
    /// batch size, so the on-disk format matches what `--explain` reports.
    pub spill_compress: bool,
    /// Error strategy governing output-stage eval failures. Under
    /// `FailFast` a residual / body eval error propagates immediately;
    /// under `Continue` / `BestEffort` the failing row is deferred to the
    /// dispatcher via [`CombineKernelOutput::output_eval_failures`].
    pub strategy: clinker_plan::config::ErrorStrategy,
}

pub(crate) fn execute_combine_iejoin(
    args: IEJoinExec<'_>,
) -> Result<CombineKernelOutput, PipelineError> {
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
        partition_bits,
        propagate_ck,
        ctx,
        budget,
        consumer,
        spill_dir,
        spill_compress,
        strategy,
    } = args;
    // Recoverable output-stage eval failures deferred to the dispatcher.
    // Always empty under `FailFast` (those errors return immediately).
    let mut output_eval_failures: Vec<CombineOutputEvalFailure> = Vec::new();
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

    // Pre-scan: extract eq keys + range keys for every record in parallel.
    // A record with a NULL in any eq key (3VL) or any range key can never
    // match and is handled off the range walk. Pure-range case
    // (`partition_bits: None`): no eq key, n_buckets=1, and the block-band
    // path drains the scan outcomes into external-sort buffers instead of
    // hash buckets.
    let n_buckets: usize = match partition_bits {
        Some(bits) => 1usize << bits.min(16),
        None => 1,
    };
    let hash_state = RandomState::new();

    // Per-record key extraction is the expensive part of the pre-scan:
    // every record runs the compiled CXL eq-key and range-key closures.
    // The extractors are stateless (`&self`) and `EvalContext` is
    // read-only, so the extraction parallelizes across the shared kernel
    // pool. Each record yields an
    // independent `RecordScan`; the routing loops below replay those
    // outcomes in strict ascending index order, so the partition vectors
    // and `unmatched_drivers` end up byte-identical to a sequential scan.
    let driver_scans: Vec<RecordScan> = driver_records
        .par_iter()
        .map(|(rec, _rn)| {
            // Driver-side key extraction routes through `CombineResolver`
            // so chain-buried qualifiers (e.g. `b.id` against an N-ary
            // decomposition step's encoded intermediate record) resolve
            // via the resolved column map rather than `Record`'s
            // bare-name fallback.
            let driver_resolver = CombineResolver::new(resolver_mapping, rec, None);
            scan_record(
                &driver_extractor,
                &driver_range_extractor,
                ctx,
                &driver_resolver,
                n_buckets,
                &hash_state,
            )
            .map_err(|e| key_eval_error(name, "driving", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let build_scans: Vec<RecordScan> = build_records
        .par_iter()
        .map(|rec| {
            scan_record(
                &build_extractor,
                &build_range_extractor,
                ctx,
                rec,
                n_buckets,
                &hash_state,
            )
            .map_err(|e| key_eval_error(name, "build", e))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let op1 = range_ops[0];
    let op2 = if n_ranges >= 2 {
        Some(range_ops[1])
    } else {
        None
    };

    // Pure-range predicates (`partition_bits: None`) run the bounded
    // block-band path: external-sort each side on the primary inequality
    // key, slice the merged stream into min/max-tagged blocks, prune the
    // non-overlapping block-pairs, and run the kernel per surviving pair.
    // The equi+range (`Some(bits)`) path below hash-partitions and holds its
    // inputs resident. Both share `emit_pairs`, so they agree row-for-row.
    if partition_bits.is_none() {
        // `IEJoin(None)` is selected only for pure-range predicates, which
        // carry no equality conjuncts. A non-empty eq extractor here means
        // the planner routed an equi+range combine to the pure-range
        // strategy — a planner-shape violation the block path cannot honor.
        if !driver_extractor.is_empty() {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: "iejoin block-band path invoked with equality conjuncts; \
                         the pure-range strategy requires none"
                    .to_string(),
            });
        }
        return block::execute_block_band(block::BlockBandExec {
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
            propagate_ck,
            ctx,
            budget,
            consumer,
            spill_dir,
            spill_compress,
            strategy,
            options: block::BlockBandOptions::default(),
        });
    }

    // ── Equi+range path (`partition_bits: Some(bits)`) ───────────────────
    let mut driver_partitions: Vec<Vec<usize>> = vec![Vec::new(); n_buckets];
    let mut build_partitions: Vec<Vec<usize>> = vec![Vec::new(); n_buckets];
    let mut driver_eq_keys: Vec<Option<Vec<Value>>> = vec![None; driver_records.len()];
    let mut build_eq_keys: Vec<Option<Vec<Value>>> = vec![None; build_records.len()];
    let mut driver_range_keys: Vec<Option<(i64, i64)>> = vec![None; driver_records.len()];
    let mut build_range_keys: Vec<Option<(i64, i64)>> = vec![None; build_records.len()];
    let mut unmatched_drivers: Vec<usize> = Vec::new();

    // Replay the parallel scan outcomes in strict ascending index order, so
    // the partition vectors and `unmatched_drivers` end up byte-identical to
    // a sequential scan.
    for (i, scan) in driver_scans.into_iter().enumerate() {
        match scan {
            RecordScan::Unmatched => unmatched_drivers.push(i),
            RecordScan::Matched {
                eq_keys,
                range_key,
                bucket,
            } => {
                driver_partitions[bucket].push(i);
                driver_eq_keys[i] = Some(eq_keys);
                driver_range_keys[i] = Some(range_key);
            }
        }
    }
    for (j, scan) in build_scans.into_iter().enumerate() {
        // NULL build eq keys (3VL) and NULL / non-integer range keys can
        // never match, so the build side drops them — there is no
        // build-side `on_miss` bucket the driver side fills.
        if let RecordScan::Matched {
            eq_keys,
            range_key,
            bucket,
        } = scan
        {
            build_partitions[bucket].push(j);
            build_eq_keys[j] = Some(eq_keys);
            build_range_keys[j] = Some(range_key);
        }
    }

    // Pre-output charge #1 — the pre-scan working set. The routed
    // partition index vectors and the per-record range-key slots are the
    // state now held on the heap before any range walk starts. Estimate
    // their bytes, mirror them into the registered consumer handle, and
    // abort before the first group if they already exceed budget. The
    // equi+range path holds its inputs resident with no spill, so an
    // over-budget pre-scan can only be resolved by a clean abort —
    // `should_abort_local` short-circuits on the local estimate, so the gate
    // still fires on a host where `rss_bytes()` returns `None`.
    let routed_index_count = driver_partitions.iter().map(Vec::len).sum::<usize>()
        + build_partitions.iter().map(Vec::len).sum::<usize>();
    let partition_state_bytes = routed_index_count
        .saturating_mul(std::mem::size_of::<usize>())
        .saturating_add(
            driver_range_keys
                .len()
                .saturating_add(build_range_keys.len())
                .saturating_mul(std::mem::size_of::<Option<(i64, i64)>>()),
        );
    consumer.set_bytes(partition_state_bytes as u64);
    if budget.should_abort_local(partition_state_bytes as u64) {
        return Err(pre_output_budget_error(
            name,
            partition_state_bytes as u64,
            budget.hard_limit(),
        ));
    }

    let mut output_records: Vec<(Record, RecordOrder)> = Vec::new();
    let mut emitted_since_check: usize = 0;

    // Match state is global on this path: each driver lands in exactly one
    // bucket and, within it, one eq-key group, so a per-driver flag /
    // accumulator keyed by global index is correct across the whole join.
    // Shared with the emit loop and the collect / on_miss finalizers below.
    let mut state = MatchState::new(driver_records.len());
    let emit_cfg = EmitConfig {
        name,
        build_qualifier,
        ctx,
        resolver_mapping,
        output_schema,
        match_mode,
        propagate_ck,
        budget,
        strategy,
    };
    let mut evals = Evaluators {
        residual: residual_eval,
        body: body_eval,
    };

    for bucket in 0..n_buckets {
        let drivers = &driver_partitions[bucket];
        let builds = &build_partitions[bucket];
        if drivers.is_empty() || builds.is_empty() {
            continue;
        }

        // Group records by their full equality-key tuple. With
        // partition_bits=8 hash collisions can land distinct eq tuples in the
        // same bucket; the eq verification per group filters them. This path
        // is reached only for equi+range predicates (pure-range runs the
        // block-band path), so every record carries an eq key.
        let groups: Vec<(Vec<usize>, Vec<usize>)> =
            group_by_eq_keys(drivers, builds, &driver_eq_keys, &build_eq_keys);

        for (group_drivers, group_builds) in groups {
            if group_drivers.is_empty() || group_builds.is_empty() {
                continue;
            }
            let dkeys: Vec<(i64, i64)> = group_drivers
                .iter()
                .map(|&i| driver_range_keys[i].expect("driver range keys populated"))
                .collect();
            let bkeys: Vec<(i64, i64)> = group_builds
                .iter()
                .map(|&j| build_range_keys[j].expect("build range keys populated"))
                .collect();

            // Pre-output charge #2 — this group's transient IEJoin working
            // set on top of the retained pre-scan state: the two range-key
            // arrays plus the kernel's L1/L2 sort arrays, permutation
            // vectors, and visited bit array. Gate it before the range walk
            // so a group large enough to overflow the budget aborts before
            // the sort arrays are materialized. The peak is the retained
            // partition state plus this one group's kernel state (prior
            // groups' state is already dropped), so mirror that sum into the
            // handle for the arbitrator's pull-mode view.
            let group_state_bytes = dkeys
                .len()
                .saturating_add(bkeys.len())
                .saturating_mul(std::mem::size_of::<(i64, i64)>())
                .saturating_add(iejoin_numeric_state_bytes(dkeys.len(), bkeys.len()));
            let group_peak_bytes = partition_state_bytes.saturating_add(group_state_bytes);
            consumer.set_bytes(group_peak_bytes as u64);
            if budget.should_abort_local(group_peak_bytes as u64) {
                return Err(pre_output_budget_error(
                    name,
                    group_peak_bytes as u64,
                    budget.hard_limit(),
                ));
            }

            let pairs: Vec<(usize, usize)> = match op2 {
                Some(op2v) => iejoin_numeric(&dkeys, &bkeys, op1, op2v),
                None => {
                    // Single-inequality: PWMJ on the sole axis.
                    let dl: Vec<i64> = dkeys.iter().map(|(a, _)| *a).collect();
                    let bl: Vec<i64> = bkeys.iter().map(|(a, _)| *a).collect();
                    pwmj_numeric(&dl, &bl, op1)
                }
            };

            // Fold the materialized pair index vector into the group peak —
            // it is the last pre-output allocation before per-pair emit, so a
            // high-fan-out band join aborts here before building any output
            // records.
            let pairs_peak_bytes = group_peak_bytes.saturating_add(
                pairs
                    .len()
                    .saturating_mul(std::mem::size_of::<(usize, usize)>()),
            );
            consumer.set_bytes(pairs_peak_bytes as u64);
            if budget.should_abort_local(pairs_peak_bytes as u64) {
                return Err(pre_output_budget_error(
                    name,
                    pairs_peak_bytes as u64,
                    budget.hard_limit(),
                ));
            }

            // The kernel returns local pair indices into the group's driver
            // and build slices; present them to the shared emit loop keyed by
            // global driver index so match state spans the whole join.
            let driver_slice: Vec<DriverRef<'_>> = group_drivers
                .iter()
                .map(|&gi| DriverRef {
                    record: &driver_records[gi].0,
                    order: driver_records[gi].1,
                    key: gi,
                    driver_idx: gi as u64,
                })
                .collect();
            let build_slice: Vec<&Record> =
                group_builds.iter().map(|&gj| &build_records[gj]).collect();
            let mut sink = EmitSink {
                output_records: &mut output_records,
                emitted_since_check: &mut emitted_since_check,
                output_eval_failures: &mut output_eval_failures,
                // Equi+range keeps kernel/bucket visitation order; no final
                // deterministic re-sort, so no per-row tags or failure keys.
                row_tags: None,
                failure_tags: None,
            };
            let batch = EmitBatch {
                pairs: &pairs,
                driver_slice: &driver_slice,
                build_slice: &build_slice,
                build_idx: None,
            };
            emit_pairs(&emit_cfg, &mut evals, &batch, &mut state, &mut sink)?;
        }
    }

    // Collect mode flushes one row per driver — even drivers that saw
    // zero matches emit, with an empty array (matches the
    // HashBuildProbe collect-path shape).
    if matches!(match_mode, MatchMode::Collect) {
        let mut sink = EmitSink {
            output_records: &mut output_records,
            emitted_since_check: &mut emitted_since_check,
            output_eval_failures: &mut output_eval_failures,
            row_tags: None,
            failure_tags: None,
        };
        for (i, (driver_record, driver_order)) in driver_records.iter().enumerate() {
            let flush = state.take_collect(i);
            flush_collect_row(&emit_cfg, driver_record, *driver_order, i as u64, flush, &mut sink);
        }
        // Collect mode never runs the body eval, so no recoverable
        // output-stage failure can accrue on this path.
        return Ok(CombineKernelOutput {
            records: output_records,
            output_eval_failures,
        });
    }

    // First/All on_miss dispatch — every driver that saw zero matches
    // (NULL-eq, NULL-range, and the unmatched pile from the range walk)
    // gets the configured handling.
    let mut all_unmatched: Vec<usize> = unmatched_drivers;
    for (i, hit) in state.matched.iter().enumerate() {
        if !*hit && !all_unmatched.contains(&i) {
            all_unmatched.push(i);
        }
    }
    all_unmatched.sort_unstable();
    all_unmatched.dedup();

    let mut sink = EmitSink {
        output_records: &mut output_records,
        emitted_since_check: &mut emitted_since_check,
        output_eval_failures: &mut output_eval_failures,
        row_tags: None,
        failure_tags: None,
    };
    for i in all_unmatched {
        let (driver_record, driver_order) = (&driver_records[i].0, driver_records[i].1);
        dispatch_on_miss(
            &emit_cfg,
            &mut evals,
            driver_record,
            driver_order,
            i as u64,
            on_miss,
            &mut sink,
        )?;
    }

    Ok(CombineKernelOutput {
        records: output_records,
        output_eval_failures,
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
/// driver and build slices they index, and — on the block path only — each
/// local build's original input index for deterministic selection and ordering.
struct EmitBatch<'a> {
    pairs: &'a [(usize, usize)],
    driver_slice: &'a [DriverRef<'a>],
    build_slice: &'a [&'a Record],
    build_idx: Option<&'a [u64]>,
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
    /// heaps). The block path folds it into its per-pair pre-output gate so a
    /// `match: first` / `collect` join over wide build records aborts with the
    /// typed budget error instead of growing this residency unbounded. Zero on
    /// the equi+range path, which emits matches immediately and holds no build
    /// records across pairs.
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

    /// The bytes of held `First` / collect candidates accumulated so far in
    /// this driver block, for the block path's pre-output gate.
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

/// Mutable output sink for the shared emit path: the accumulating output
/// buffer, the every-10K poll counter, and the deferred output-eval failures.
struct EmitSink<'a> {
    output_records: &'a mut Vec<(Record, RecordOrder)>,
    emitted_since_check: &'a mut usize,
    output_eval_failures: &'a mut Vec<CombineOutputEvalFailure>,
    /// Block-path only: parallel `(driver input index, build input index)` tag
    /// per pushed row — `(driver_idx, u64::MAX)` for collect / on_miss rows.
    /// Zipped with `output_records` and stable-sorted by `(driver order,
    /// driver_idx, build_idx)` after the pass, so block output order is a pure
    /// function of the data even when a chained upstream stamps duplicate driver
    /// orders. `None` on the equi+range path, which keeps kernel/bucket order.
    row_tags: Option<&'a mut Vec<(u64, u64)>>,
    /// Block-path only: parallel `(driver order, driver_idx, build_idx)` sort key
    /// per deferred output-eval failure, so dead-letter rows are re-ordered into
    /// the same layout-independent order as the emitted rows. `None` on the
    /// equi+range path, which dispatches failures in visitation order.
    failure_tags: Option<&'a mut Vec<(RecordOrder, u64, u64)>>,
}

impl EmitSink<'_> {
    /// Push one emitted row, recording its `(driver_idx, build_idx)` tag when the
    /// block path asked for deterministic ordering.
    fn push_row(&mut self, record: Record, order: RecordOrder, driver_idx: u64, build_idx: u64) {
        self.output_records.push((record, order));
        if let Some(tags) = self.row_tags.as_deref_mut() {
            tags.push((driver_idx, build_idx));
        }
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

/// Emit every qualifying `(driver, build)` pair in `pairs` under the
/// combine's match mode, applying the residual filter (3+ range conjuncts),
/// the `First`-mode selection, and the every-10K output-buffer poll. `pairs`
/// carries local indices into `driver_slice` / `build_slice`; per-driver
/// state is tracked in `state` under [`DriverRef::key`].
///
/// Shared by the equi+range group loop and the pure-range block scheduler so
/// both synthesize rows identically. `build_idx` distinguishes the two: `None`
/// on the equi+range path keeps that path's kernel/bucket visitation order and
/// its immediate first-visited `First`-mode emit; `Some(idx)` on the block path
/// supplies each local build's original input index, so `First` selects the
/// minimum-index match (deferred to the driver block's finalize), `Collect`
/// orders and truncates by that index, and every emitted row is tagged for the
/// block path's final deterministic sort. Both make block output a pure
/// function of the data rather than of the memory-derived block layout.
///
/// Streaming into `sink.output_records`; the output axis is bounded only by the
/// every-10K poll (see [`poll_output_buffer`]).
fn emit_pairs(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    batch: &EmitBatch<'_>,
    state: &mut MatchState,
    sink: &mut EmitSink<'_>,
) -> Result<(), PipelineError> {
    for &(di_local, bi_local) in batch.pairs {
        let dref = &batch.driver_slice[di_local];
        let driver_record = dref.record;
        let driver_order = dref.order;
        let key = dref.key;
        let build_record = batch.build_slice[bi_local];
        // Original build input index on the block path; a per-driver insertion
        // counter stand-in (the next collect-array position) on the equi+range
        // path, which preserves its visitation order.
        let bidx = batch.build_idx.map(|idx| idx[bi_local]);

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
                        bidx.unwrap_or(u64::MAX),
                    );
                    continue;
                }
            }
        }

        match cfg.match_mode {
            MatchMode::Collect => {
                // Order key: the build input index on the block path (kept set
                // and array order become deterministic); an insertion counter
                // on the equi+range path (preserves visitation order and the
                // first-`CAP` truncation). The counter is the driver's current
                // collect-array length, which strictly increases per push.
                let order_key = bidx.unwrap_or_else(|| {
                    state.collect_accum.get(&key).map_or(0, BinaryHeap::len) as u64
                });
                state.record_collect_match(key, order_key, build_record);
            }
            MatchMode::First => match bidx {
                // Block path: hold the minimum-build-index candidate; emit at
                // the driver block's finalize.
                Some(idx) => state.note_first_candidate(key, idx, build_record),
                // Equi+range path: first-visited wins. Emit immediately and
                // dedup; a body eval that skips leaves the driver open to the
                // next pair.
                None => {
                    if state.matched[key] {
                        continue;
                    }
                    if emit_match_row(
                        cfg,
                        evals,
                        driver_record,
                        driver_order,
                        dref.driver_idx,
                        build_record,
                        0,
                        sink,
                    )? {
                        state.matched[key] = true;
                    }
                }
            },
            MatchMode::All => {
                if emit_match_row(
                    cfg,
                    evals,
                    driver_record,
                    driver_order,
                    dref.driver_idx,
                    build_record,
                    bidx.unwrap_or(0),
                    sink,
                )? {
                    state.matched[key] = true;
                }
            }
        }
    }
    Ok(())
}

/// Materialize and push one `(driver, build)` output row for `First` / `All`:
/// the body eval (or, for a body-less synthetic decomposition step, the
/// driver-then-build value concat), `$ck` propagation, and the every-10K poll.
/// The row is tagged with `build_idx` for the block path's final deterministic
/// sort (ignored on the equi+range path, whose sink records no tags). Returns
/// `true` if a row was pushed, `false` if the body eval skipped it or a
/// recoverable eval failure was deferred.
fn emit_match_row(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    driver_record: &Record,
    driver_order: RecordOrder,
    driver_idx: u64,
    build_record: &Record,
    build_idx: u64,
    sink: &mut EmitSink<'_>,
) -> Result<bool, PipelineError> {
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
                sink.push_row(rec, driver_order, driver_idx, build_idx);
                poll_output_buffer(cfg, sink)?;
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
        sink.push_row(rec, driver_order, driver_idx, build_idx);
        poll_output_buffer(cfg, sink)?;
        Ok(true)
    }
}

/// Poll the accumulated output buffer every [`MEMORY_CHECK_INTERVAL`] emitted
/// rows and abort if it has grown past the hard limit. The output axis is
/// unbounded (O(N·M) worst case) until a later phase streams it, so this
/// cadence is the backstop that keeps a high-fan-out band join from blowing
/// the ceiling between charges.
fn poll_output_buffer(cfg: &EmitConfig<'_>, sink: &mut EmitSink<'_>) -> Result<(), PipelineError> {
    *sink.emitted_since_check += 1;
    if *sink.emitted_since_check >= MEMORY_CHECK_INTERVAL {
        let used =
            crate::pipeline::combine::combine_output_buffer_bytes(sink.output_records) as u64;
        if cfg.budget.should_abort_local(used) {
            return Err(PipelineError::MemoryBudgetExceeded {
                node: cfg.name.to_string(),
                used,
                limit: cfg.budget.hard_limit(),
                source: BudgetCategory::Arena,
                detail: Some("iejoin output buffer exceeded budget".to_string()),
            });
        }
        *sink.emitted_since_check = 0;
    }
    Ok(())
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
) {
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
    sink.push_row(rec, driver_order, driver_idx, u64::MAX);
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
                    sink.push_row(rec, driver_order, driver_idx, u64::MAX);
                    poll_output_buffer(cfg, sink)
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

/// Outcome of extracting one record's join keys in the pre-scan. Produced
/// in parallel (per record, independently) and replayed sequentially in
/// index order, so the order-sensitive partition fill stays deterministic.
enum RecordScan {
    /// Record has a NULL eq key (3VL) or a NULL / non-integer range key —
    /// it can never match. Driver records route to the `on_miss` bucket;
    /// build records drop.
    Unmatched,
    /// Record carries valid eq + range keys and lands in `bucket`.
    Matched {
        eq_keys: Vec<Value>,
        range_key: (i64, i64),
        bucket: usize,
    },
}

/// Extract one record's eq + range keys and assign its hash bucket.
/// Pure over `&self`-borrowed extractors and a read-only `EvalContext`,
/// so it runs concurrently across records without shared mutable state.
/// Returns `Unmatched` on a NULL eq key or an unrepresentable range key,
/// matching the sequential pre-scan's 3VL routing exactly.
fn scan_record(
    eq_extractor: &KeyExtractor,
    range_extractor: &KeyExtractor,
    ctx: &EvalContext<'_>,
    resolver: &dyn cxl::resolve::traits::FieldResolver,
    n_buckets: usize,
    hash_state: &RandomState,
) -> Result<RecordScan, EvalError> {
    let mut eq_buf: Vec<Value> = Vec::new();
    if !eq_extractor.is_empty() {
        eq_extractor.extract_into(ctx, resolver, &mut eq_buf)?;
    }
    if eq_buf.iter().any(|v| matches!(v, Value::Null)) {
        return Ok(RecordScan::Unmatched);
    }
    let mut range_buf: Vec<Value> = Vec::new();
    range_extractor.extract_into(ctx, resolver, &mut range_buf)?;
    let Some(range_key) = range_keys_to_i64_pair(&range_buf) else {
        return Ok(RecordScan::Unmatched);
    };
    let bucket = if n_buckets > 1 {
        (hash_composite_key(&eq_buf, hash_state) as usize) & (n_buckets - 1)
    } else {
        0
    };
    Ok(RecordScan::Matched {
        eq_keys: eq_buf,
        range_key,
        bucket,
    })
}

/// Convert a 1-or-2-element vector of range key Values to an
/// `(i64, i64)` pair, returning `None` if any element is NULL or
/// non-orderable. Single-axis callers (PWMJ) ignore the second slot;
/// the placeholder zero keeps the layout uniform across kernels.
fn range_keys_to_i64_pair(values: &[Value]) -> Option<(i64, i64)> {
    let n = values.len();
    if n == 0 {
        return None;
    }
    let v1 = value_to_i64(&values[0])?;
    if n == 1 {
        return Some((v1, 0));
    }
    let v2 = value_to_i64(&values[1])?;
    Some((v1, v2))
}

#[inline]
fn value_to_i64(v: &Value) -> Option<i64> {
    use chrono::Datelike;
    match v {
        Value::Integer(i) => Some(*i),
        Value::Float(f) => {
            if f.is_finite() {
                Some(crate::pipeline::sort_key::float_to_orderable_i64(*f))
            } else {
                None
            }
        }
        Value::Date(d) => Some(d.num_days_from_ce() as i64),
        Value::DateTime(dt) => Some(dt.and_utc().timestamp_micros()),
        _ => None,
    }
}

/// Group drivers and builds by their equality-key tuple within a
/// single hash partition. Necessary when partition_bits hashing lands
/// distinct eq tuples in the same bucket; the per-group range scan
/// then sees only records that actually share an eq key.
fn group_by_eq_keys(
    drivers: &[usize],
    builds: &[usize],
    driver_eq_keys: &[Option<Vec<Value>>],
    build_eq_keys: &[Option<Vec<Value>>],
) -> Vec<(Vec<usize>, Vec<usize>)> {
    let mut driver_groups: Vec<(Vec<Value>, Vec<usize>)> = Vec::new();
    'driver: for &i in drivers {
        let keys = driver_eq_keys[i]
            .as_ref()
            .expect("driver eq keys populated in pre-scan");
        for (existing_keys, group) in driver_groups.iter_mut() {
            if keys_equal_canonicalized(keys, existing_keys) {
                group.push(i);
                continue 'driver;
            }
        }
        driver_groups.push((keys.clone(), vec![i]));
    }
    let mut out: Vec<(Vec<usize>, Vec<usize>)> = Vec::new();
    for (driver_keys, driver_group) in driver_groups {
        let mut build_group: Vec<usize> = Vec::new();
        for &j in builds {
            let keys = build_eq_keys[j]
                .as_ref()
                .expect("build eq keys populated in pre-scan");
            if keys_equal_canonicalized(keys, &driver_keys) {
                build_group.push(j);
            }
        }
        out.push((driver_group, build_group));
    }
    out
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
    // L1 + L2: two `Vec<(i64, i64, i64)>` of length n_total.
    let sort_arrays = n_total
        .saturating_mul(std::mem::size_of::<(i64, i64, i64)>())
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
        let expected = n_total * std::mem::size_of::<(i64, i64, i64)>() * 2 // L1 + L2
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
        fn apply(self, a: i64, b: i64) -> bool {
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
    type JoinCase = (Vec<(i64, i64)>, Vec<(i64, i64)>, TOp, TOp);

    fn nested_loop(
        left: &[(i64, i64)],
        right: &[(i64, i64)],
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
        let left = vec![(10_i64, 100_i64), (20, 50), (5, 200)];
        let right = vec![(15_i64, 80_i64), (25, 150), (8, 60)];
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
        let empty: Vec<(i64, i64)> = Vec::new();
        let some = vec![(1_i64, 2_i64)];
        assert!(iejoin_numeric(&empty, &some, RangeOp::Lt, RangeOp::Lt).is_empty());
        assert!(iejoin_numeric(&some, &empty, RangeOp::Lt, RangeOp::Lt).is_empty());
        assert!(iejoin_numeric(&empty, &empty, RangeOp::Lt, RangeOp::Lt).is_empty());
    }

    #[test]
    fn test_iejoin_numeric_single_row() {
        let left = vec![(5_i64, 10_i64)];
        let right = vec![(3_i64, 8_i64)];
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
        let left: Vec<(i64, i64)> = (0..10).map(|_| (5_i64, 5_i64)).collect();
        let right: Vec<(i64, i64)> = (0..10).map(|_| (5_i64, 5_i64)).collect();
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
        let left = vec![1_i64, 3, 5, 7];
        let right = vec![2_i64, 4, 6, 8];
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
        let empty: Vec<i64> = Vec::new();
        let some = vec![1_i64];
        assert!(pwmj_numeric(&empty, &some, RangeOp::Lt).is_empty());
        assert!(pwmj_numeric(&some, &empty, RangeOp::Lt).is_empty());
        assert!(pwmj_numeric(&empty, &empty, RangeOp::Lt).is_empty());
    }

    fn arb_inputs() -> impl Strategy<Value = JoinCase> {
        let op = prop_oneof![Just(TOp::Lt), Just(TOp::Le), Just(TOp::Gt), Just(TOp::Ge)];
        (
            prop::collection::vec((-50i64..50, -50i64..50), 0..50),
            prop::collection::vec((-50i64..50, -50i64..50), 0..50),
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
            // Every generated value is finite, so `value_to_i64` is `Some`.
            let enc = |rows: &[(f64, f64)]| -> Vec<(i64, i64)> {
                rows.iter()
                    .map(|&(a, b)| {
                        (
                            value_to_i64(&Value::Float(a)).unwrap(),
                            value_to_i64(&Value::Float(b)).unwrap(),
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

        let mut left: Vec<(i64, i64)> = Vec::with_capacity(2 * n_employees_per_tile);
        let mut right: Vec<(i64, i64)> = Vec::with_capacity(2 * n_brackets_per_tile);
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
                right.push((lo, hi));
            }
            for e in 0..n_employees_per_tile {
                // Mix per-row so income lands on different bracket
                // boundaries than a strict modulo would produce.
                let mix =
                    ((tile * n_employees_per_tile + e) as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
                let income = base + (mix as i64).rem_euclid(SPAN);
                left.push((income, income));
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
        // (i64, i64) keys.
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
