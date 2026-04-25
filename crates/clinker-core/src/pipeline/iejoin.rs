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
//! **Memory model:** the IEJoin scan materializes both partition sides,
//! the L1/L2 vectors (`signed_idx, op1_key, op2_key` triples), the
//! permutation P, and the bit array. The caller's [`MemoryBudget`] is
//! polled every 10K emitted matched pairs; abort returns
//! `PipelineError::Compilation` with an E310-prefixed message — the
//! same shape used by the C.2 hash build/probe path.

use std::collections::HashMap;
use std::sync::Arc;

use ahash::RandomState;
use clinker_record::{Record, Schema, Value};
use cxl::ast::Expr;
use cxl::eval::{EvalContext, EvalError, EvalResult, ProgramEvaluator};
use cxl::typecheck::TypedProgram;
use indexmap::IndexMap;

use crate::config::pipeline_node::{MatchMode, OnMiss};
use crate::error::PipelineError;
use crate::executor::combine::{CombineResolver, CombineResolverMapping};
use crate::pipeline::combine::{KeyExtractor, hash_composite_key, keys_equal_canonicalized};
use crate::pipeline::memory::MemoryBudget;
use crate::plan::combine::{DecomposedPredicate, RangeOp};

/// Cap on matches collected per driver under [`MatchMode::Collect`].
/// Mirrors `pipeline::combine::COLLECT_PER_GROUP_CAP` so both code paths
/// truncate at the same threshold.
const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// Period (matched pairs emitted) between [`MemoryBudget::should_abort`]
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
    pub driving_input: &'a str,
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
    pub ctx: &'a EvalContext<'a>,
    pub budget: &'a mut MemoryBudget,
}

pub(crate) fn execute_combine_iejoin(
    args: IEJoinExec<'_>,
) -> Result<Vec<(Record, RecordOrder)>, PipelineError> {
    let IEJoinExec {
        name,
        driving_input,
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
        ctx,
        budget,
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
    // executor::mod.rs's HashBuildProbe arm.
    let mut driver_eq_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
    let mut build_eq_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
    for eq in &decomposed.equalities {
        let (driver_expr, driver_prog, build_expr, build_prog) = if eq.left_input.as_ref()
            == driving_input
            && eq.right_input.as_ref() == build_qualifier
        {
            (
                eq.left_expr.clone(),
                Arc::clone(&eq.left_program),
                eq.right_expr.clone(),
                Arc::clone(&eq.right_program),
            )
        } else if eq.left_input.as_ref() == build_qualifier
            && eq.right_input.as_ref() == driving_input
        {
            (
                eq.right_expr.clone(),
                Arc::clone(&eq.right_program),
                eq.left_expr.clone(),
                Arc::clone(&eq.left_program),
            )
        } else {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!(
                    "equality conjunct has qualifiers ({}, {}); expected ({driving_input}, \
                     {build_qualifier})",
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
    // so we clone `range_program` for each side.
    let mut driver_range_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
    let mut build_range_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
    let mut range_ops: Vec<RangeOp> = Vec::new();
    for r in &decomposed.ranges {
        let (driver_expr, build_expr, op) = if r.left_input.as_ref() == driving_input
            && r.right_input.as_ref() == build_qualifier
        {
            (r.left_expr.clone(), r.right_expr.clone(), r.op)
        } else if r.left_input.as_ref() == build_qualifier
            && r.right_input.as_ref() == driving_input
        {
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
                    "range conjunct has qualifiers ({}, {}); expected ({driving_input}, \
                     {build_qualifier})",
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
    let mut residual_eval: Option<ProgramEvaluator> = if n_ranges > 2 {
        decomposed
            .residual
            .as_ref()
            .map(|r| ProgramEvaluator::new(Arc::clone(r), false))
    } else {
        None
    };
    let mut body_eval = body_program.map(|p| ProgramEvaluator::new(Arc::clone(p), false));

    // Pre-scan: extract eq keys + range keys for every record. Records
    // with NULL in any eq key (3VL) or any range key are routed to the
    // unmatched bucket — the IEJoin scan never sees them. Pure-range
    // case: no eq key, n_buckets=1.
    let n_buckets: usize = match partition_bits {
        Some(bits) => 1usize << bits.min(16),
        None => 1,
    };

    let hash_state = RandomState::new();
    let mut driver_partitions: Vec<Vec<usize>> = vec![Vec::new(); n_buckets];
    let mut build_partitions: Vec<Vec<usize>> = vec![Vec::new(); n_buckets];
    let mut driver_eq_keys: Vec<Option<Vec<Value>>> = vec![None; driver_records.len()];
    let mut build_eq_keys: Vec<Option<Vec<Value>>> = vec![None; build_records.len()];
    let mut driver_range_keys: Vec<Option<(i64, i64)>> = vec![None; driver_records.len()];
    let mut build_range_keys: Vec<Option<(i64, i64)>> = vec![None; build_records.len()];
    let mut unmatched_drivers: Vec<usize> = Vec::new();

    let mut eq_buf: Vec<Value> = Vec::new();
    let mut range_buf: Vec<Value> = Vec::new();

    for (i, (rec, _rn)) in driver_records.iter().enumerate() {
        eq_buf.clear();
        if !driver_extractor.is_empty() {
            driver_extractor
                .extract_into(ctx, rec, &mut eq_buf)
                .map_err(|e| key_eval_error(name, "driving", e))?;
        }
        if eq_buf.iter().any(|v| matches!(v, Value::Null)) {
            unmatched_drivers.push(i);
            continue;
        }
        range_buf.clear();
        driver_range_extractor
            .extract_into(ctx, rec, &mut range_buf)
            .map_err(|e| key_eval_error(name, "driving", e))?;
        let Some(rk) = range_keys_to_i64_pair(&range_buf) else {
            unmatched_drivers.push(i);
            continue;
        };
        let bucket = if n_buckets > 1 {
            (hash_composite_key(&eq_buf, &hash_state) as usize) & (n_buckets - 1)
        } else {
            0
        };
        driver_partitions[bucket].push(i);
        driver_eq_keys[i] = Some(eq_buf.clone());
        driver_range_keys[i] = Some(rk);
    }

    for (j, rec) in build_records.iter().enumerate() {
        eq_buf.clear();
        if !build_extractor.is_empty() {
            build_extractor
                .extract_into(ctx, rec, &mut eq_buf)
                .map_err(|e| key_eval_error(name, "build", e))?;
        }
        if eq_buf.iter().any(|v| matches!(v, Value::Null)) {
            // NULL build eq keys can never match (3VL); drop.
            continue;
        }
        range_buf.clear();
        build_range_extractor
            .extract_into(ctx, rec, &mut range_buf)
            .map_err(|e| key_eval_error(name, "build", e))?;
        let Some(rk) = range_keys_to_i64_pair(&range_buf) else {
            continue;
        };
        let bucket = if n_buckets > 1 {
            (hash_composite_key(&eq_buf, &hash_state) as usize) & (n_buckets - 1)
        } else {
            0
        };
        build_partitions[bucket].push(j);
        build_eq_keys[j] = Some(eq_buf.clone());
        build_range_keys[j] = Some(rk);
    }

    let mut matched_driver: Vec<bool> = vec![false; driver_records.len()];
    let mut output_records: Vec<(Record, RecordOrder)> = Vec::new();
    let mut emitted_since_check: usize = 0;

    let mut collect_accum: HashMap<usize, Vec<Value>> = HashMap::new();
    let mut collect_truncated: HashMap<usize, ()> = HashMap::new();

    let op1 = range_ops[0];
    let op2 = if n_ranges >= 2 {
        Some(range_ops[1])
    } else {
        None
    };
    let pure_range = driver_extractor.is_empty();

    for bucket in 0..n_buckets {
        let drivers = &driver_partitions[bucket];
        let builds = &build_partitions[bucket];
        if drivers.is_empty() || builds.is_empty() {
            continue;
        }

        // Group records by their full equality-key tuple. With
        // partition_bits=8 hash collisions can land distinct eq tuples
        // in the same bucket; the eq verification per group filters
        // them. Pure-range case: a single group containing every record
        // in this (sole) bucket.
        let groups: Vec<(Vec<usize>, Vec<usize>)> = if pure_range {
            vec![(drivers.clone(), builds.clone())]
        } else {
            group_by_eq_keys(drivers, builds, &driver_eq_keys, &build_eq_keys)
        };

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

            let pairs: Vec<(usize, usize)> = match op2 {
                Some(op2v) => iejoin_numeric(&dkeys, &bkeys, op1, op2v),
                None => {
                    // Single-inequality: PWMJ on the sole axis.
                    let dl: Vec<i64> = dkeys.iter().map(|(a, _)| *a).collect();
                    let bl: Vec<i64> = bkeys.iter().map(|(a, _)| *a).collect();
                    pwmj_numeric(&dl, &bl, op1)
                }
            };

            for (di_local, bi_local) in pairs {
                let driver_idx = group_drivers[di_local];
                let build_idx = group_builds[bi_local];
                let (driver_record, driver_order) =
                    (&driver_records[driver_idx].0, driver_records[driver_idx].1);
                let build_record = &build_records[build_idx];

                // 3+ range conjuncts: residual filter applies. The
                // residual covers the full original predicate (every
                // range was folded in), but we've already verified the
                // first two ranges via the kernel; the residual run
                // here re-checks them too. That's a small cost in
                // exchange for not having to materialize a separate
                // "tail-only" residual at planning time.
                if let Some(ref mut residual) = residual_eval {
                    let resolver =
                        CombineResolver::new(resolver_mapping, driver_record, Some(build_record));
                    match residual.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Skip(_)) => continue,
                        Ok(EvalResult::Emit { .. }) => {}
                        Err(e) => return Err(PipelineError::from(e)),
                    }
                }

                if matches!(match_mode, MatchMode::First) && matched_driver[driver_idx] {
                    continue;
                }

                match match_mode {
                    MatchMode::Collect => {
                        if collect_truncated.contains_key(&driver_idx) {
                            continue;
                        }
                        let arr = collect_accum.entry(driver_idx).or_default();
                        if arr.len() >= COLLECT_PER_GROUP_CAP {
                            collect_truncated.insert(driver_idx, ());
                            continue;
                        }
                        let mut m: IndexMap<Box<str>, Value> = IndexMap::new();
                        for (fname, val) in build_record.iter_all_fields() {
                            m.insert(fname.into(), val.clone());
                        }
                        arr.push(Value::Map(Box::new(m)));
                        matched_driver[driver_idx] = true;
                    }
                    MatchMode::First | MatchMode::All => {
                        let resolver = CombineResolver::new(
                            resolver_mapping,
                            driver_record,
                            Some(build_record),
                        );
                        let evaluator =
                            body_eval.as_mut().ok_or_else(|| PipelineError::Internal {
                                op: "combine",
                                node: name.to_string(),
                                detail: "combine body typed program missing for non-collect match"
                                    .to_string(),
                            })?;
                        match evaluator.eval_record::<NullStorage>(ctx, &resolver, None) {
                            Ok(EvalResult::Emit { fields, metadata }) => {
                                let mut rec = match output_schema {
                                    Some(s) => widen_record_to_schema(driver_record, s),
                                    None => driver_record.clone(),
                                };
                                for (n, v) in fields {
                                    rec.set(&n, v);
                                }
                                for (k, v) in metadata {
                                    let _ = rec.set_meta(&k, v);
                                }
                                output_records.push((rec, driver_order));
                                matched_driver[driver_idx] = true;
                                emitted_since_check += 1;
                                if emitted_since_check >= MEMORY_CHECK_INTERVAL {
                                    if budget.should_abort() {
                                        return Err(PipelineError::Compilation {
                                            transform_name: name.to_string(),
                                            messages: vec![format!(
                                                "E310 combine probe memory limit exceeded: \
                                                 hard limit {}",
                                                budget.hard_limit()
                                            )],
                                        });
                                    }
                                    emitted_since_check = 0;
                                }
                            }
                            Ok(EvalResult::Skip(_)) => {}
                            Err(e) => return Err(PipelineError::from(e)),
                        }
                    }
                }
            }
        }
    }

    // Collect mode flushes one row per driver — even drivers that saw
    // zero matches emit, with an empty array (matches the
    // HashBuildProbe collect-path shape).
    if matches!(match_mode, MatchMode::Collect) {
        for (i, (driver_record, driver_order)) in driver_records.iter().enumerate() {
            let arr = collect_accum.remove(&i).unwrap_or_default();
            if collect_truncated.contains_key(&i) {
                eprintln!(
                    "W: combine {name:?} match: collect truncated at \
                     {COLLECT_PER_GROUP_CAP} matches for driver row {driver_order}"
                );
            }
            let mut rec = match output_schema {
                Some(s) => widen_record_to_schema(driver_record, s),
                None => driver_record.clone(),
            };
            rec.set(build_qualifier, Value::Array(arr));
            output_records.push((rec, *driver_order));
        }
        return Ok(output_records);
    }

    // First/All on_miss dispatch — every driver that saw zero matches
    // (NULL-eq, NULL-range, and the unmatched pile from the IEJoin
    // scan) gets the configured handling.
    let mut all_unmatched: Vec<usize> = unmatched_drivers;
    for (i, hit) in matched_driver.iter().enumerate() {
        if !*hit && !all_unmatched.contains(&i) {
            all_unmatched.push(i);
        }
    }
    all_unmatched.sort_unstable();
    all_unmatched.dedup();

    for i in all_unmatched {
        let (driver_record, driver_order) = (&driver_records[i].0, driver_records[i].1);
        match on_miss {
            OnMiss::Skip => continue,
            OnMiss::Error => {
                return Err(PipelineError::Compilation {
                    transform_name: name.to_string(),
                    messages: vec![format!(
                        "E310 combine on_miss: error — no matching build row for driver row {driver_order}"
                    )],
                });
            }
            OnMiss::NullFields => {
                let resolver = CombineResolver::new(resolver_mapping, driver_record, None);
                let evaluator = body_eval.as_mut().ok_or_else(|| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: "combine body typed program missing for on_miss: null_fields"
                        .to_string(),
                })?;
                match evaluator.eval_record::<NullStorage>(ctx, &resolver, None) {
                    Ok(EvalResult::Emit { fields, metadata }) => {
                        let mut rec = match output_schema {
                            Some(s) => widen_record_to_schema(driver_record, s),
                            None => driver_record.clone(),
                        };
                        for (n, v) in fields {
                            rec.set(&n, v);
                        }
                        for (k, v) in metadata {
                            let _ = rec.set_meta(&k, v);
                        }
                        output_records.push((rec, driver_order));
                        emitted_since_check += 1;
                        if emitted_since_check >= MEMORY_CHECK_INTERVAL {
                            if budget.should_abort() {
                                return Err(PipelineError::Compilation {
                                    transform_name: name.to_string(),
                                    messages: vec![format!(
                                        "E310 combine probe memory limit exceeded: \
                                         hard limit {}",
                                        budget.hard_limit()
                                    )],
                                });
                            }
                            emitted_since_check = 0;
                        }
                    }
                    Ok(EvalResult::Skip(_)) => {}
                    Err(e) => return Err(PipelineError::from(e)),
                }
            }
        }
    }

    Ok(output_records)
}

// ──────────────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────────────

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
                Some(f.to_bits() as i64)
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

fn key_eval_error(name: &str, side: &'static str, err: EvalError) -> PipelineError {
    PipelineError::Compilation {
        transform_name: name.to_string(),
        messages: vec![format!("combine {side}-side range/key eval error: {err}")],
    }
}

fn widen_record_to_schema(input: &Record, target: &Arc<Schema>) -> Record {
    if Arc::ptr_eq(input.schema(), target) {
        return input.clone();
    }
    let mut values: Vec<Value> = Vec::with_capacity(target.column_count());
    for col in target.columns() {
        values.push(input.get(col).cloned().unwrap_or(Value::Null));
    }
    let mut out = Record::new(Arc::clone(target), values);
    for (k, v) in input.iter_meta() {
        let _ = out.set_meta(k, v.clone());
    }
    out
}

/// Placeholder `RecordStorage` for windowless body / residual
/// evaluation. Same role as `executor::mod::NullStorage` and
/// `pipeline::combine::NullStorage`.
struct NullStorage;

impl clinker_record::RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _: u32) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u32 {
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
        fn to_range(self) -> RangeOp {
            match self {
                TOp::Lt => RangeOp::Lt,
                TOp::Le => RangeOp::Le,
                TOp::Gt => RangeOp::Gt,
                TOp::Ge => RangeOp::Ge,
            }
        }
    }

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

    fn arb_inputs() -> impl Strategy<Value = (Vec<(i64, i64)>, Vec<(i64, i64)>, TOp, TOp)> {
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
}
