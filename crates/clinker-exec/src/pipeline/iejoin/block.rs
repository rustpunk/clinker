//! Block-band execution for pure-range IEJoin combines (`CombineStrategy::IEJoin`,
//! dispatched as `IEJoin(None)`).
//!
//! The whole-input IEJoin kernel materializes both sides plus its L1/L2 sort
//! arrays, so a pure-range band join over inputs larger than the memory budget
//! has nowhere to spill. This module bounds the INPUT axis by partitioning
//! each side into contiguous, key-sorted, min/max-tagged blocks that live on
//! disk, then running the unchanged kernel per surviving block-pair:
//!
//!   1. **Drain** each side into a payload-ordered
//!      [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer) keyed on the
//!      primary inequality key (payload `(k1, k2, driver_idx, order)` for the
//!      driver, `(k1, k2, build_idx)` for the build). The buffer spills its
//!      overflow to disk.
//!   2. **Slice** the sorted stream (in-memory vec or the k-way merge over the
//!      spilled runs — never collected) into contiguous blocks of at most
//!      `block_target_bytes`, tagging each with its per-axis key bounds. Blocks
//!      stay resident under a shared `soft / 2` budget across both sides, so a
//!      side (or both sides together) that fits it never touches disk;
//!      remaining blocks are written to their own spill files.
//!   3. **Schedule** driver blocks as the outer loop and build blocks as the
//!      inner loop, strictly sequentially. A block-pair whose key bounds prove
//!      the predicate empty (see [`possible`]) is pruned before any load. Each
//!      surviving pair loads its two blocks (a resident block is borrowed, a
//!      spilled block read into scratch), runs the numeric kernel, and emits
//!      through the shared [`emit_pairs`](super::emit_pairs) loop.
//!
//! Determinism: the emitted rows — and the deferred output-eval (dead-letter)
//! rows — are a pure function of the input data and the pipeline config, never
//! of `pipeline.memory.limit`. Build payloads carry the build record's original
//! input index, so `match: first` selects the minimum such index and `collect`
//! orders and truncates by it. Driver payloads carry the driver's unique global
//! input index, so the whole output is sorted by
//! `(driver order, driver_idx, build_idx)` before return — a total key even when
//! a chained upstream stamps duplicate driver orders — and the dead-letter rows
//! are sorted the same way, both independent of the memory-derived block layout.
//! `on_miss: error` is dispatched after the full pass and names the
//! lowest-input-index unmatched driver. The one documented exception is the
//! FailFast eval-error identity: FailFast surfaces the first eval error in
//! visitation order and returns immediately, since determinizing an error path
//! would defeat fail-fast.
//!
//! Memory model: bounded on the input axis — the resident-block budget plus at
//! most one loaded spilled block per side, the kernel's O(n) sort arrays, and
//! the held `First` / `collect` candidates for the current driver block. Both
//! deferred on-miss piles are bounded the same way: the scan-phase pile (drivers
//! with a NULL / non-orderable range key, routed out before any block) AND the
//! in-block zero-match pile (drivers that entered a block but matched nothing)
//! each drain into their own spillable, driver-index-ordered sort buffer rather
//! than a resident vec, and stream back through a two-way merge at finalize — so
//! an input dominated by unmatched drivers, whether NULL-keyed or
//! orderable-but-out-of-range, stays bounded on the driver side too.
//! The scan/sort and slice phases respond to memory pressure by SPILLING (own
//! byte threshold + disk-quota E320), never by consulting global process
//! pressure.
//! The per-pair pre-output abort
//! ([`pre_output_budget_error`](super::pre_output_budget_error)) is a strictly
//! LOCAL last resort: it measures this pair's exact live footprint — the
//! resident baseline, the scratch for any spilled block loaded here, the
//! per-mode range-key columns, the emit-side driver/build slice and build-index
//! vectors, the per-mode kernel-aux arrays (two-conjunct IEJoin or the leaner
//! single-inequality PWMJ), the materialized pair-index vector, and the held
//! candidates accumulated so far — directly against the hard limit, never
//! against the arbitrator's global process pressure. So a bounded-residency run
//! stays host-independent and aborts only when one pair's own footprint cannot
//! fit `hard`, not because the process floor sits above a tight budget. The
//! OUTPUT axis is spill-bounded on the same strictly-local terms: emitted rows
//! accumulate in a payload-ordered
//! [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer) keyed on
//! `(driver order, driver_idx, build_idx)` that spills on its own byte threshold
//! (disk-quota E320) instead of holding the O(N·M) result in RAM, and the
//! dispatcher drains that sorted handle incrementally. The buffer's resident
//! bytes ARE charged through the consumer handle — mirrored per row exactly as
//! the input drains charge, so the arbitrator's pull-mode view reflects every
//! axis — while spilling on the buffer's own threshold, not global pressure, is
//! what bounds it. The gates additionally reserve the deferred sort buffers'
//! spill caps, but PER PHASE, since the buffers are not all co-resident at their
//! caps at once. The output sort grows across the whole emit phase, so every gate
//! reserves its constant cap. The in-block pile is phase-distinct: it takes no
//! pushes during a driver block's inner loop (its misses drain only in that
//! block's finalize, after the loop), so the per-pair gates fold in its CURRENT
//! live bytes — zero for a fully-matching run, which therefore reserves one cap,
//! not two — while the driver-load gate covers that later finalize growth with a
//! bounded term: the pile's current live bytes plus at most the driver block's own
//! record footprint (the misses it can clone), capped at the pile's spill cap. A
//! fully-matching block routes nothing to the pile, so that term collapses to the
//! block footprint (≤ `block_target`) rather than a full second cap, which is what
//! lets a run whose true finalize peak is one cap plus a block complete instead of
//! aborting for a cap it never uses. At end-of-join the piles convert to k-way
//! mergers and stack with the output sort and the still-resident blocks; that
//! finalize footprint is bounded by construction. Each pile's merger first folds
//! its spilled runs down to a fixed merge fan-in before streaming (the bounded-k
//! cascaded multi-pass merge, #868), so its open-run residency — loser-tree
//! records and open readers — is capped by that fan-in no matter how many runs a
//! pathologically fragmented pile produced, rather than scaling with the run
//! count. The two finalize mergers plus the output sort (≤ its cap) and the
//! resident sides (≤ the shared block budget) thus sum to a bounded stack under the
//! derived caps. Each per-pair gate proves `phase_working_set + reservation ≤
//! hard`; because every reserved buffer spills at its cap, the true phase-peak
//! stays within `hard`, a provable bound rather than the earlier ~1.4×hard worst
//! case. The per-pair EMIT loop keeps no global-pressure abort: it polls no
//! `should_abort`, so a handle transiently summing the input footprint plus the
//! output sort's resident bytes is only ever an attribution reading, never an abort
//! trigger. The deferred-miss dispatch does run one `should_abort` backstop
//! ([`poll_finalize_backstop`]) every
//! [`MEMORY_CHECK_INTERVAL`](super::MEMORY_CHECK_INTERVAL) dispatched rows, but as
//! defense-in-depth against a cross-consumer or host-RSS overrun surfacing during
//! finalize — consistent with the global-pressure polls the engine's other spill
//! operators run — NOT as the guard for the mergers' open-run residency, which the
//! bounded-k fold-down already caps.

use std::io;
use std::path::Path;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use clinker_record::{Record, Schema};

use cxl::eval::{EvalContext, ProgramEvaluator};

use clinker_plan::BudgetCategory;
use clinker_plan::config::ErrorStrategy;
use clinker_plan::config::pipeline_node::{MatchMode, OnMiss, PropagateCkSpec};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::combine::RangeOp;

use crate::executor::combine::CombineResolverMapping;
use crate::pipeline::memory::{ConsumerHandle, MemoryArbitrator};
use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
use crate::pipeline::spill::{SpillFile, SpillWriter};
use crate::pipeline::spill_merge::{MergeBudget, SortedRunMerger};

use super::{
    BlockBandOutput, CollectFlush, DriverRef, EmitBatch, EmitConfig, EmitSink, Evaluators,
    MatchState, RecordOrder, RecordScan, RowSink, dispatch_on_miss, emit_match_row, emit_pairs,
    flush_collect_row, iejoin_numeric, iejoin_numeric_state_bytes, pre_output_budget_error,
    pwmj_numeric, pwmj_numeric_state_bytes,
};

/// Soft-limit divisor for the per-block byte target. Targeting `soft / 8` per
/// block, against a `soft / 4` resident-block budget shared by both sides
/// ([`RESIDENT_BUDGET_DIVISOR`]), lets up to ~two blocks stay resident before
/// spill kicks in, and keeps one surviving pair's working set (two blocks plus
/// the kernel's O(n) L1/L2 sort arrays) comfortably under the hard limit the
/// strictly-local pre-output gate measures against.
const BLOCK_TARGET_DIVISOR: u64 = 8;

/// Floor for the per-block byte target, so a tiny configured budget still
/// exercises the multi-block path rather than collapsing every side to one
/// resident block. Mirrors the sort-merge matching-run floor. A single record
/// larger than the target still forms a block of one — the target is soft.
const BLOCK_TARGET_FLOOR: usize = 16 * 1024;

/// Per-side external-sort spill threshold: a quarter of the soft RSS limit,
/// floored at 16 KiB so tests with tiny budgets still exercise the drain spill
/// path. Mirrors the sort-merge Phase-A threshold.
const SORT_SPILL_FLOOR: usize = 16 * 1024;

/// Soft-limit divisor for the output/drain external-sort spill threshold: a
/// quarter of the soft RSS limit, floored at 16 KiB. Sized so the per-pair
/// gate can statically reserve this cap and still admit a worst-case pair
/// (see the memory-model note above).
const SORT_SPILL_DIVISOR: u64 = 4;

/// Soft-limit divisor for the resident-block budget shared across both sides.
/// A completed block stays in RAM until the two sides' resident blocks together
/// would exceed `soft / 4`; further blocks spill to their own files. A side (or
/// both sides) whose sliced blocks fit `soft / 4` never touches disk, restoring
/// the in-RAM path for small and mid-size inputs. Worst case a draining side's
/// sort buffer (≤ `soft / 4` before it spills) coexists with the other side's
/// resident blocks (≤ `soft / 4`), summing to `soft / 2`, which is below the hard
/// limit (`soft` is a fraction of `hard`).
const RESIDENT_BUDGET_DIVISOR: u64 = 4;

/// Per-block byte target derived from the arbitrator soft limit, or the
/// test-only override.
fn block_target_bytes(budget: &MemoryArbitrator) -> usize {
    std::cmp::max(
        BLOCK_TARGET_FLOOR,
        (budget.soft_limit() / BLOCK_TARGET_DIVISOR) as usize,
    )
}

/// Drain-side external-sort spill threshold.
fn sort_spill_threshold_bytes(budget: &MemoryArbitrator) -> usize {
    std::cmp::max(
        SORT_SPILL_FLOOR,
        (budget.soft_limit() / SORT_SPILL_DIVISOR) as usize,
    )
}

/// Resident-block budget shared across both sides, or the test-only override.
fn resident_budget_total(budget: &MemoryArbitrator) -> u64 {
    (budget.soft_limit() / RESIDENT_BUDGET_DIVISOR).max(BLOCK_TARGET_FLOOR as u64)
}

/// Tracks the resident bytes both sides' kept-in-RAM blocks hold against the
/// shared [`resident_budget_total`]. Admission is per-block and fit-based: a
/// completed block stays resident whenever it still fits the remaining budget
/// and spills otherwise, so a later, smaller block can still be admitted after
/// an earlier, larger one spilled — it is not a strict resident prefix. The
/// build side (reloaded once per driver block) drains first, so the budget
/// favors the side whose residency saves the most reloads.
struct ResidentBudget {
    total: u64,
    used: u64,
}

impl ResidentBudget {
    fn new(total: u64) -> Self {
        Self { total, used: 0 }
    }

    /// Admit a just-completed block of `bytes` as resident if it still fits the
    /// shared budget, charging it; return whether it was admitted.
    fn admit(&mut self, bytes: u64) -> bool {
        if self.used.saturating_add(bytes) <= self.total {
            self.used = self.used.saturating_add(bytes);
            true
        } else {
            false
        }
    }
}

/// Estimated resident byte cost of one `(record, payload)` pair — the same
/// formula the [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer)
/// charges, so block sizing and the pre-output charge track its accounting.
fn pair_bytes<P>(record: &Record) -> usize {
    std::mem::size_of::<Record>() + record.estimated_heap_size() + std::mem::size_of::<P>()
}

/// Per-record byte width of a hoisted range-key column: the `(k1, k2)` tuple a
/// two-conjunct band feeds to `iejoin_numeric`, or the bare `k1` a
/// single-inequality PWMJ reads. The pre-output gate charges both sides' key
/// columns at this width, so `Single`'s dead second axis is never counted.
fn range_key_width(op2: Option<RangeOp>) -> usize {
    match op2 {
        Some(_) => std::mem::size_of::<(i64, i64)>(),
        None => std::mem::size_of::<i64>(),
    }
}

/// Zero the consumer handle and build the typed strictly-local pre-output
/// abort for a gate whose measured `peak` exceeds `hard`. Every block-path
/// gate (driver load, pre-load pair, post-kernel pairs) returns through here,
/// so "no stale charge survives an abort" is a property of one function rather
/// than of copy discipline repeated at each gate.
fn abort_over_budget(
    consumer: &Arc<ConsumerHandle>,
    name: &str,
    peak: u64,
    hard: u64,
) -> PipelineError {
    consumer.set_bytes(0);
    pre_output_budget_error(name, peak, hard)
}

/// The kernel dispatch for one driver block, carrying the driver-side key
/// column hoisted out of the build loop (it depends only on the driver block).
/// `Dual` runs the two-conjunct IEJoin over both key axes and holds the
/// `(k1, k2)` column; `Single` runs the single-inequality PWMJ over the primary
/// axis and holds the bare `k1` column — no dead `k2` slot. Bundling the mode
/// with its column lets one value stand for "op2 present" vs "single-axis keys
/// hoisted", which the two are never both.
enum RangeMode {
    Dual {
        op2: RangeOp,
        driver_keys: Vec<(i64, i64)>,
    },
    Single {
        driver_keys: Vec<i64>,
    },
}

/// Per-axis key bounds accumulated while a block fills.
#[derive(Clone, Copy)]
struct Bounds {
    k1_min: i64,
    k1_max: i64,
    k2_min: i64,
    k2_max: i64,
}

impl Bounds {
    /// Empty bounds: `min`/`max` inverted so the first `update` seeds both and
    /// an unfilled block (never updated) prunes against every operator.
    fn empty() -> Self {
        Self {
            k1_min: i64::MAX,
            k1_max: i64::MIN,
            k2_min: i64::MAX,
            k2_max: i64::MIN,
        }
    }

    fn update(&mut self, k1: i64, k2: i64) {
        self.k1_min = self.k1_min.min(k1);
        self.k1_max = self.k1_max.max(k1);
        self.k2_min = self.k2_min.min(k2);
        self.k2_max = self.k2_max.max(k2);
    }
}

/// One contiguous, key-sorted run of `(record, payload)` pairs with its
/// per-axis key bounds, ready for the block-band prune. Either resident (kept
/// in RAM under the shared resident-block budget) or on disk in its own spill
/// file.
struct Block<P> {
    storage: BlockStorage<P>,
    bounds: Bounds,
    /// Estimated resident byte cost when loaded: the figure charged against the
    /// budget and, for a resident block, the RAM it actually holds. Known at
    /// slice time so the per-pair pre-output gate can evaluate before any load.
    resident_bytes: u64,
    /// Record count, also known at slice time, so the kernel-aux estimate feeds
    /// the pre-output gate before the block's pairs are materialized.
    len: usize,
}

enum BlockStorage<P> {
    Resident(Vec<(Record, P)>),
    /// The spill file is held for the block's lifetime so its backing temp
    /// file (deleted on `SpillFile` drop) survives every reload in the
    /// scheduler's inner loop.
    Spilled(SpillFile<P>),
}

/// A loaded block's pairs: a borrow of the resident vec (no copy — a resident
/// build block is revisited once per driver-block pass) or an owned scratch vec
/// read fresh from disk for a spilled block. Both deref to `&[(Record, P)]`, so
/// the scheduler reads either uniformly. Borrowing the resident case is what
/// keeps a resident block's footprint at one copy, not two — the gate charges
/// `resident_bytes` exactly once.
enum Loaded<'a, P> {
    Borrowed(&'a [(Record, P)]),
    Owned(Vec<(Record, P)>),
}

impl<P> std::ops::Deref for Loaded<'_, P> {
    type Target = [(Record, P)];
    fn deref(&self) -> &Self::Target {
        match self {
            Loaded::Borrowed(pairs) => pairs,
            Loaded::Owned(pairs) => pairs,
        }
    }
}

impl<P: DeserializeOwned> Block<P> {
    /// Materialize the block's pairs. A resident block borrows its held vec (no
    /// copy — a build block is revisited once per driver-block pass); a spilled
    /// block reads its file into a fresh scratch vec (`resident_bytes`, already
    /// charged). `context` localizes a decode failure.
    fn load(&self, context: &'static str) -> Result<Loaded<'_, P>, PipelineError> {
        match &self.storage {
            BlockStorage::Resident(pairs) => Ok(Loaded::Borrowed(pairs)),
            BlockStorage::Spilled(file) => {
                let reader = file.reader().map_err(|e| {
                    PipelineError::Io(io::Error::other(format!(
                        "{context}: block reload open failed: {e}"
                    )))
                })?;
                let pairs = reader
                    .map(|r| {
                        r.map_err(|e| {
                            PipelineError::Io(io::Error::other(format!(
                                "{context}: block reload decode failed: {e}"
                            )))
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Loaded::Owned(pairs))
            }
        }
    }
}

/// Whether a range predicate `d OP b` can hold for *some* `d` in a driver
/// block's `[dmin, dmax]` and *some* `b` in a build block's `[bmin, bmax]`.
/// Sound and conservative: it drops a block-pair only when the predicate is
/// provably empty on that axis, so no qualifying pair is ever pruned.
///
/// `op` is already driver-vs-build oriented (the executor flips the raw
/// predicate before the kernel), matching `iejoin_numeric`'s `left OP right`
/// convention.
fn possible(op: RangeOp, dmin: i64, dmax: i64, bmin: i64, bmax: i64) -> bool {
    match op {
        // ∃ d,b: d < b  ⇔  min d < max b
        RangeOp::Lt => dmin < bmax,
        RangeOp::Le => dmin <= bmax,
        // ∃ d,b: d > b  ⇔  max d > min b
        RangeOp::Gt => dmax > bmin,
        RangeOp::Ge => dmax >= bmin,
    }
}

/// A block-pair survives the prune iff the primary axis is possible and — for
/// a two-conjunct band — the secondary axis is possible too. Each axis is an
/// independent existence check, so a conjunction needs both.
fn block_pair_possible<PA, PB>(
    op1: RangeOp,
    op2: Option<RangeOp>,
    driver: &Block<PA>,
    build: &Block<PB>,
) -> bool {
    if !possible(
        op1,
        driver.bounds.k1_min,
        driver.bounds.k1_max,
        build.bounds.k1_min,
        build.bounds.k1_max,
    ) {
        return false;
    }
    if let Some(op2) = op2
        && !possible(
            op2,
            driver.bounds.k2_min,
            driver.bounds.k2_max,
            build.bounds.k2_min,
            build.bounds.k2_max,
        )
    {
        return false;
    }
    true
}

/// A payload-sorted stream over one drained side, unifying the in-memory and
/// spilled cases so the slicer never materializes the merged run. `InMemory`
/// yields an already-sorted vec; `Spilled` folds the sorted runs through the
/// k-way [`SortedRunMerger`], one resident record per open run.
enum SortedStream<P: Ord + DeserializeOwned> {
    InMemory(std::vec::IntoIter<(Record, P)>),
    Spilled(SortedRunMerger<P>),
}

impl<P: Ord + DeserializeOwned> Iterator for SortedStream<P> {
    type Item = Result<(Record, P), PipelineError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SortedStream::InMemory(it) => it.next().map(Ok),
            SortedStream::Spilled(merger) => merger.next(),
        }
    }
}

/// Wrap a finished sort buffer's [`SortedOutput`] in a [`SortedStream`]: an
/// in-memory buffer yields its already-sorted vec, a spilled one folds its runs
/// through the k-way [`SortedRunMerger`] (one resident record per open run).
/// `context` localizes a merge-open failure. `budget` charges any intermediate
/// runs the bounded-k cascade spills when a pile fragments past the merge fan-in;
/// callers pass the same node / arbitrator / compression they charge their own
/// spills under, so the cascade's re-writes attribute to this stage and balance
/// against the input runs' charges. The matched-side slice, the deferred-pile
/// finish, and the test drain all stream a `SortedOutput` the same way, so the
/// `InMemory` / `Spilled` dispatch lives here once. Residue charging is the
/// caller's concern — it happens on the `finish` that produced `sorted`.
fn sorted_output_stream<P: Serialize + Ord + DeserializeOwned>(
    sorted: SortedOutput<P>,
    context: &'static str,
    budget: MergeBudget<'_>,
) -> Result<SortedStream<P>, PipelineError> {
    Ok(match sorted {
        SortedOutput::InMemory(pairs) => SortedStream::InMemory(pairs.into_iter()),
        SortedOutput::Spilled(files) => SortedStream::Spilled(
            SortedRunMerger::new_payload_ordered(files, context, budget)?,
        ),
    })
}

/// Shared spill / charge context for the drain and slice helpers. Bundled so
/// each helper stays under clippy's argument cap.
struct DrainCtx<'a> {
    name: &'a str,
    budget: &'a MemoryArbitrator,
    consumer: &'a Arc<ConsumerHandle>,
    spill_dir: &'a Path,
    spill_compress: bool,
    block_target: usize,
    /// Sort-buffer spill threshold. A tiny value forces the drain-spill →
    /// k-way-merge path even under a budget above process RSS, so a test can
    /// exercise it while the run still completes.
    sort_threshold: usize,
}

/// Test-only overrides for the block-band path. Not user-facing config: the
/// production dispatch always constructs the default and derives every
/// threshold from the arbitrator soft limit.
#[derive(Default)]
pub(super) struct BlockBandOptions {
    /// Forces the per-block byte target, so a proptest can drive the
    /// multi-block path on tiny inputs. `None` derives it from the soft limit.
    pub(super) block_target_override: Option<usize>,
    /// Forces the sort-buffer spill threshold, so a test can drive the
    /// drain-spill → k-way-merge → block-reload path under a budget above RSS.
    /// `None` derives it from the soft limit.
    pub(super) sort_spill_override: Option<usize>,
    /// Forces the shared resident-block budget. `Some(0)` spills every
    /// completed block so a test can exercise the per-block spill → reload path
    /// under a roomy budget; `None` derives it from the soft limit.
    pub(super) resident_budget_override: Option<u64>,
}

/// Inputs to [`execute_block_band`]. Grouped into a struct so the entry point
/// takes one argument and the parent can update one field without rewriting
/// the call site.
pub(super) struct BlockBandExec<'a> {
    pub(super) name: &'a str,
    pub(super) build_qualifier: &'a str,
    pub(super) driver_records: Vec<(Record, RecordOrder)>,
    pub(super) driver_scans: Vec<RecordScan>,
    pub(super) build_records: Vec<Record>,
    pub(super) build_scans: Vec<RecordScan>,
    pub(super) op1: RangeOp,
    pub(super) op2: Option<RangeOp>,
    pub(super) residual_eval: Option<ProgramEvaluator>,
    pub(super) body_eval: Option<ProgramEvaluator>,
    pub(super) resolver_mapping: &'a CombineResolverMapping,
    pub(super) output_schema: Option<&'a Arc<Schema>>,
    pub(super) match_mode: MatchMode,
    pub(super) on_miss: OnMiss,
    pub(super) propagate_ck: &'a PropagateCkSpec,
    pub(super) ctx: &'a EvalContext<'a>,
    pub(super) budget: &'a MemoryArbitrator,
    pub(super) consumer: &'a Arc<ConsumerHandle>,
    pub(super) spill_dir: &'a Path,
    pub(super) spill_compress: bool,
    pub(super) strategy: ErrorStrategy,
    pub(super) options: BlockBandOptions,
}

/// Execute a pure-range combine via the block-band path. Blocking on both
/// sides (each is drained and sliced before the range walk); bounded on the
/// input axis AND, now, the output axis: emitted rows accumulate in a
/// payload-ordered, spillable sort buffer rather than an in-RAM vec. Returns a
/// bounded [`BlockBandOutput`] — a sorted output handle the dispatcher drains
/// incrementally — plus any deferred output-eval failures for the DLQ.
pub(super) fn execute_block_band(
    exec: BlockBandExec<'_>,
) -> Result<BlockBandOutput, PipelineError> {
    let BlockBandExec {
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
        options,
    } = exec;

    let block_target = options
        .block_target_override
        .unwrap_or_else(|| block_target_bytes(budget));
    let sort_threshold = options
        .sort_spill_override
        .unwrap_or_else(|| sort_spill_threshold_bytes(budget));
    // Constant per-buffer spill cap: each deferred sort buffer self-bounds by
    // spilling once its resident bytes reach this. Derived from the budget, NOT
    // `sort_threshold`, which honors the test-only `sort_spill_override`.
    let deferred_cap = sort_spill_threshold_bytes(budget) as u64;
    // The in-block zero-match pile can hold live bytes only under a First / All
    // run with a non-skip on_miss; Collect and Skip never route to it. The
    // driver-load gate below folds a bounded finalize term for it (computed per
    // driver block); the per-pair gates fold its constant live bytes.
    let inblock_can_fill =
        matches!(match_mode, MatchMode::First | MatchMode::All) && !matches!(on_miss, OnMiss::Skip);
    let drain_ctx = DrainCtx {
        name,
        budget,
        consumer,
        spill_dir,
        spill_compress,
        block_target,
        sort_threshold,
    };
    // Shared across both sides so their resident blocks compete for one budget:
    // a side, or both together, whose sliced blocks fit `soft / 2` stays entirely
    // in RAM. `Some(0)` forces every block to spill for the test path.
    let resident_total = options
        .resident_budget_override
        .unwrap_or_else(|| resident_budget_total(budget));
    let mut resident = ResidentBudget::new(resident_total);

    // Driver record schema, captured before the drain consumes `driver_records`
    // and shared with it: the scan-phase and in-block zero-match buffers both hold
    // driver records and must decode them on reload exactly as the matched driver
    // buffer does. An empty driver side yields an empty schema — it holds nothing,
    // so no reload occurs.
    let driver_row_schema: Arc<Schema> = match driver_records.first() {
        Some((record, _)) => Arc::clone(record.schema()),
        None => Arc::new(Schema::new(Vec::new())),
    };

    // Schema the emitted output records carry, resolved before the drains
    // consume `driver_records`: the combine's output schema when present, else
    // the driver schema (the shape the body-less / no-output-schema synthetic
    // step re-emits). The empty / first-driver fallback reuses `driver_row_schema`
    // so the derivation lives in one place and the two cannot silently diverge.
    // Spilling output rows only happens on high-fan-out combines, which always
    // carry an output schema, so the fallback backs only the never-spilling small
    // cases and the header always matches the rows written.
    let output_row_schema: Arc<Schema> = output_schema
        .map(Arc::clone)
        .unwrap_or_else(|| Arc::clone(&driver_row_schema));

    // Scan-phase unmatched drivers (NULL / non-orderable range key) are retained
    // — spilled to their own buffer during the drain — only when the run will
    // consume them: Collect emits an empty-array row for each, and any non-Skip
    // on_miss dispatches them. Under First / All + Skip they produce no output,
    // so the drain drops them outright rather than spilling and re-reading a pile
    // nothing observes.
    let retain_unmatched =
        matches!(match_mode, MatchMode::Collect) || !matches!(on_miss, OnMiss::Skip);

    // Drain + slice: pull each matched side into a payload-ordered sort buffer
    // and slice the sorted stream into min/max-tagged blocks. Scan-phase
    // unmatched drivers drain into their own spillable, driver-index-ordered
    // buffer for the deferred on_miss dispatch; unmatched builds are dropped
    // (they can never match). The drain/slice mirror each resident block's bytes
    // into the consumer handle and leave that baseline in place — it stays
    // charged through the other side's drain and all of the pruning and range
    // walk, so the arbitrator's pull-mode view never under-reads this operator's
    // live footprint.
    //
    // Drain the build side first so it claims the shared resident budget: each
    // build block is reloaded once per driver block (the inner loop), while a
    // driver block loads once, so keeping the build side in RAM saves the most
    // reloads for a fixed budget.
    let build_blocks = drain_build_side(build_records, build_scans, &drain_ctx, &mut resident)?;
    let (driver_blocks, scan_unmatched_buf) = drain_driver_side(
        driver_records,
        driver_scans,
        retain_unmatched,
        &driver_row_schema,
        &drain_ctx,
        &mut resident,
    )?;

    // Baseline resident footprint the consumer holds through the schedule/emit
    // stage: both sides' kept-in-RAM blocks plus the scan-phase unmatched
    // buffer's resident tail. That tail is static across the block loop (the
    // buffer takes no further pushes after the drain and spills its overflow to
    // disk), so folding it into the baseline lets the per-pair gate account for
    // it exactly — like a resident block — rather than leave it an uncharged
    // residency. Spilled blocks hold no RAM until their per-pair load, charged
    // transiently below.
    let baseline_resident = resident_bytes_of(&driver_blocks)
        + resident_bytes_of(&build_blocks)
        + scan_unmatched_buf.bytes_used() as u64;

    // Schedule + emit: prune block pairs, run the kernel per surviving pair, and
    // emit through the shared loop. Each emitted row folds its
    // `(driver order, driver input index, build input index)` sort key in as the
    // payload of a payload-ordered sort buffer — `(order, driver_idx, u64::MAX)`
    // for collect / on_miss rows — so the deterministic output order is realized
    // by the (possibly external) sort, a pure function of the data and
    // independent of the memory-derived block layout even when a chained upstream
    // repeats orders. The buffer spills on its own byte threshold, so the output
    // axis is bounded rather than holding every matched row in RAM.
    let mut output_buf: SortBuffer<(RecordOrder, u64, u64)> = SortBuffer::new_payload_ordered(
        sort_threshold,
        Some(spill_dir.to_path_buf()),
        spill_compress,
        output_row_schema,
    );
    let mut emitted_since_check: usize = 0;
    let mut output_eval_failures = Vec::new();
    // Parallel `(order, driver_idx, build_idx)` sort key per deferred output-eval
    // failure, so the dead-letter rows re-order into the same layout-independent
    // order as the emitted rows.
    let mut failure_tags: Vec<(RecordOrder, u64, u64)> = Vec::new();
    // In-block zero-match drivers (a driver that entered a block but matched no
    // build) drain into their own payload-ordered, spillable buffer keyed on
    // `(driver_idx, order)`, on the same spill substrate and disk-quota accounting
    // (E320) as the scan-phase pile — so an input dominated by
    // orderable-but-out-of-range drivers stays bounded on the driver side too,
    // streamed back through the deferred on-miss merge rather than held resident
    // until dispatch. At finalize this pile merges with the streamed scan-phase
    // pile in driver-index order, so on_miss:error names the globally
    // lowest-input-index unmatched driver (not whichever range-key-ordered block
    // finalized first). Empty under on_miss:skip and Collect (nothing routes
    // here).
    let mut inblock_buf: SortBuffer<ScanUnmatchedPayload> = SortBuffer::new_payload_ordered(
        sort_threshold,
        Some(spill_dir.to_path_buf()),
        spill_compress,
        driver_row_schema,
    );
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
    // One sink threads the whole emit phase (driver loop, driver-block finalize,
    // scan-unmatched sweep, on_miss dispatch); its borrows of the output buffer
    // and failure-tag vec end at its last use, before the output buffer is
    // finished and the failures re-sorted.
    let mut sink = EmitSink {
        rows: RowSink::Sorted {
            buf: &mut output_buf,
            budget,
            name,
            consumer,
        },
        emitted_since_check: &mut emitted_since_check,
        output_eval_failures: &mut output_eval_failures,
        failure_tags: Some(&mut failure_tags),
    };

    for driver_block in &driver_blocks {
        // Bytes this driver block holds for its whole inner loop: the scratch
        // copy of a spilled block (a resident block is already in the baseline
        // and only borrowed), the hoisted range-key column — `(k1, k2)` for a
        // two-conjunct band, the bare `k1` for the single-inequality PWMJ, with
        // no dead second axis — and the `DriverRef` slice the emit consumes.
        let driver_key_bytes = range_key_width(op2);
        let driver_vecs_bytes =
            (driver_block.len * (driver_key_bytes + std::mem::size_of::<DriverRef<'_>>())) as u64;
        let driver_scratch = spilled_scratch_bytes(driver_block);
        let driver_held = driver_scratch.saturating_add(driver_vecs_bytes);
        // Gate the driver block's own load from metadata, symmetric with the
        // per-pair build gate below: a spilled driver block whose scratch plus
        // the resident baseline and the reserved deferred terms cannot fit the
        // hard limit aborts before it is materialized rather than after. A
        // resident (borrowed) block adds zero scratch, so it trips only if the
        // resident baseline plus those terms already exceeds `hard`.
        //
        // The output sort grows across the whole emit phase, so its constant cap
        // is always reserved. The in-block miss pile is phase-distinct: it takes
        // no pushes during the inner build loop (the per-pair gates below fold its
        // constant live bytes), and grows only in THIS block's finalize, where it
        // clones this block's own zero-match drivers. So its finalize residency is
        // bounded by its current live bytes plus at most this block's record
        // footprint (`resident_bytes`, the most those clones can add), and it
        // never exceeds its spill cap (it spills at that cap regardless). Reserving
        // that bounded term rather than a second full cap is the tightening: a
        // fully-matching block routes nothing to the pile, so its live bytes are
        // zero and the term collapses to the block footprint (≤ `block_target`)
        // instead of a whole cap — so a run whose true finalize peak is one cap
        // plus a block completes instead of aborting for a cap it never uses. A
        // miss-heavy run whose pile has already accumulated toward its cap folds
        // that live figure in, so the bound stays sound: it degrades to the full
        // cap exactly when the pile is genuinely near it.
        let inblock_finalize_term = if inblock_can_fill {
            deferred_cap
                .min((inblock_buf.bytes_used() as u64).saturating_add(driver_block.resident_bytes))
        } else {
            0
        };
        let driver_peak = baseline_resident
            .saturating_add(driver_held)
            .saturating_add(deferred_cap)
            .saturating_add(inblock_finalize_term);
        if driver_peak > budget.hard_limit() {
            return Err(abort_over_budget(
                consumer,
                name,
                driver_peak,
                budget.hard_limit(),
            ));
        }

        let driver_loaded = driver_block.load("iejoin block-band driver block")?;
        if driver_loaded.is_empty() {
            continue;
        }
        // Per-driver-block match state, keyed by the driver's local index in
        // this block. Each driver lands in exactly one driver block, so the
        // `First`-mode selection and the collect / on_miss finalization below
        // are globally correct even though the state is block-local.
        let mut state = MatchState::new(driver_loaded.len());
        let driver_slice: Vec<DriverRef<'_>> = driver_loaded
            .iter()
            .enumerate()
            .map(|(di, (record, payload))| DriverRef {
                record,
                order: payload.3,
                key: di,
                driver_idx: payload.2,
            })
            .collect();
        // Fix the kernel dispatch for this driver block, hoisting the driver key
        // column (which depends only on this block) out of the build loop. The
        // single-inequality (PWMJ) mode carries the bare `k1` column directly —
        // no `(k1, k2)` tuple with a dead second axis — so it allocates and the
        // gate charges only what PWMJ reads.
        let range_mode = match op2 {
            Some(op2v) => RangeMode::Dual {
                op2: op2v,
                driver_keys: driver_loaded.iter().map(|(_, p)| (p.0, p.1)).collect(),
            },
            None => RangeMode::Single {
                driver_keys: driver_loaded.iter().map(|(_, p)| p.0).collect(),
            },
        };
        // Each absolute set includes the output sort's and the in-block pile's
        // current resident bytes, so the per-row `add_bytes` charges the emit and
        // finalize paths apply are not clobbered back to an input-only figure —
        // the handle reflects the input, output, and deferred in-block footprints
        // throughout.
        consumer.set_bytes(
            baseline_resident
                .saturating_add(driver_held)
                .saturating_add(sink.output_buffer_bytes())
                .saturating_add(inblock_buf.bytes_used() as u64),
        );

        for build_block in &build_blocks {
            if !block_pair_possible(op1, op2, driver_block, build_block) {
                continue;
            }

            // Pre-output charge, from block metadata BEFORE the build side is
            // loaded: the exact live footprint this pair charges the consumer —
            // the resident baseline (kept-in-RAM blocks, counted once), this
            // pair's scratch for any spilled block loaded here (zero for a
            // borrowed resident block, already in the baseline), the build-side
            // vectors held across the kernel and emit (its range-key column
            // sized per mode, the `build_idx` and `build_slice` vecs), the
            // kernel's O(n) sort state (the two-conjunct IEJoin arrays or the
            // leaner PWMJ index vecs), the First / collect candidates held so far
            // in this driver block, and the in-block miss pile's CURRENT resident
            // bytes. The pile is folded in at its live size rather than its cap
            // because it takes no pushes during this inner loop — its misses are
            // drained only in the driver-block finalize below, after the loop — so
            // its residency is exactly constant across every pair this gate
            // guards; the driver-load gate reserves its full cap to cover that
            // later growth. The output sort buffer's LIVE bytes are bounded
            // independently by its own spill threshold, so they are deliberately
            // not summed here — folding the live output bytes in would abort a pair
            // whose input and output both legitimately spill. What the gate DOES
            // reserve is the output sort's CONSTANT spill cap (`deferred_cap`):
            // reserving that fixed cap up front bounds the working set to
            // `hard - reservation`, so once the output buffer reaches the same cap
            // the simultaneous peak still fits `hard`. The abort is a strictly
            // LOCAL last resort: it compares the working set plus that reserved cap
            // directly to the hard limit and never consults the arbitrator's global
            // pressure, because both the input and output axes answer memory
            // pressure by spilling.
            let build_vecs_bytes = (build_block.len
                * (range_key_width(op2)
                    + std::mem::size_of::<u64>()
                    + std::mem::size_of::<&Record>())) as u64;
            let aux_kernel = match op2 {
                Some(_) => iejoin_numeric_state_bytes(driver_block.len, build_block.len),
                None => pwmj_numeric_state_bytes(driver_block.len, build_block.len),
            } as u64;
            let build_held = spilled_scratch_bytes(build_block)
                .saturating_add(build_vecs_bytes)
                .saturating_add(aux_kernel);
            let pair_peak = baseline_resident
                .saturating_add(driver_held)
                .saturating_add(build_held)
                .saturating_add(state.held_bytes())
                .saturating_add(inblock_buf.bytes_used() as u64);
            // Reserve only the output sort's constant cap on top of the measured
            // working set (`pair_peak` already carries the in-block pile's live
            // bytes and stays otherwise stable, so the post-kernel gate below can
            // add `pairs_bytes` and reserve exactly once, never double-counting).
            let pair_peak_reserved = pair_peak.saturating_add(deferred_cap);
            if pair_peak_reserved > budget.hard_limit() {
                return Err(abort_over_budget(
                    consumer,
                    name,
                    pair_peak_reserved,
                    budget.hard_limit(),
                ));
            }

            let build_loaded = build_block.load("iejoin block-band build block")?;
            if build_loaded.is_empty() {
                continue;
            }
            let build_idx: Vec<u64> = build_loaded.iter().map(|(_, p)| p.2).collect();
            consumer.set_bytes(
                baseline_resident
                    .saturating_add(driver_held)
                    .saturating_add(build_held)
                    .saturating_add(state.held_bytes())
                    .saturating_add(sink.output_buffer_bytes())
                    .saturating_add(inblock_buf.bytes_used() as u64),
            );

            // Build the build-side key column at the width the chosen kernel
            // reads: the `(k1, k2)` pairs for the two-conjunct IEJoin, or the
            // bare `k1` column for PWMJ (one pass, no dead second axis).
            let pairs: Vec<(usize, usize)> = match &range_mode {
                RangeMode::Dual { op2, driver_keys } => {
                    let bkeys: Vec<(i64, i64)> =
                        build_loaded.iter().map(|(_, p)| (p.0, p.1)).collect();
                    iejoin_numeric(driver_keys, &bkeys, op1, *op2)
                }
                RangeMode::Single { driver_keys } => {
                    let bl: Vec<i64> = build_loaded.iter().map(|(_, p)| p.0).collect();
                    pwmj_numeric(driver_keys, &bl, op1)
                }
            };

            // Fold the materialized pair-index vector into the peak: it is the
            // last pre-output allocation before per-pair emit. This is a
            // check-AFTER-allocation — the vector is already built — so it is a
            // typed abort for an over-budget-but-allocatable pair, not a
            // guarantee against an allocation larger than free RAM (that is the
            // block-nested-loop follow-up). It mirrors the equi+range path's
            // post-kernel pairs gate.
            let pairs_bytes = (pairs.len() * std::mem::size_of::<(usize, usize)>()) as u64;
            let pair_peak = pair_peak.saturating_add(pairs_bytes);
            let pair_peak_reserved = pair_peak.saturating_add(deferred_cap);
            if pair_peak_reserved > budget.hard_limit() {
                return Err(abort_over_budget(
                    consumer,
                    name,
                    pair_peak_reserved,
                    budget.hard_limit(),
                ));
            }
            consumer.set_bytes(
                baseline_resident
                    .saturating_add(driver_held)
                    .saturating_add(build_held)
                    .saturating_add(state.held_bytes())
                    .saturating_add(pairs_bytes)
                    .saturating_add(sink.output_buffer_bytes())
                    .saturating_add(inblock_buf.bytes_used() as u64),
            );

            let build_slice: Vec<&Record> = build_loaded.iter().map(|(r, _)| r).collect();
            let batch = EmitBatch {
                pairs: &pairs,
                driver_slice: &driver_slice,
                build_slice: &build_slice,
                build_idx: Some(&build_idx),
            };
            emit_pairs(&emit_cfg, &mut evals, &batch, &mut state, &mut sink)?;

            // The build scratch and key/pair arrays for this pair are dropped;
            // fall back to the driver block's held state plus the First / collect
            // candidates accumulated so far, still carrying the output sort's and
            // the in-block pile's resident bytes so those charges persist.
            consumer.set_bytes(
                baseline_resident
                    .saturating_add(driver_held)
                    .saturating_add(state.held_bytes())
                    .saturating_add(sink.output_buffer_bytes())
                    .saturating_add(inblock_buf.bytes_used() as u64),
            );
        }

        // This driver block's inner loop is complete: flush collect
        // accumulators / emit deferred First candidates, and drain its zero-match
        // drivers into the spillable in-block pile for the end-of-join on_miss
        // dispatch. The temporary pile borrows `inblock_buf` only for this call,
        // so the set_bytes below can read its resident bytes.
        finalize_driver_block(
            &emit_cfg,
            &mut evals,
            &driver_loaded,
            &mut state,
            &mut InblockPile {
                buf: &mut inblock_buf,
                ctx: &drain_ctx,
                on_miss,
            },
            &mut sink,
        )?;

        // The driver block's scratch, key array, and now-drained held state are
        // released; leave the resident baseline plus the output sort's and the
        // in-block pile's resident bytes charged.
        consumer.set_bytes(
            baseline_resident
                .saturating_add(sink.output_buffer_bytes())
                .saturating_add(inblock_buf.bytes_used() as u64),
        );
    }

    // Deferred zero-match handling. Both on-miss piles finish to their own
    // driver-input-index-ordered streams — the scan-phase pile (NULL /
    // non-orderable range key, never entered a block) and the in-block pile
    // (entered a block, matched nothing) — each streamed one record resident at a
    // time, never the whole pile. Under Collect each scan-phase driver emits one
    // empty-array row (the output buffer re-sorts, so the stream order is
    // immaterial to the result). Under First / All the two streams two-way merge
    // by ascending driver-input-index and dispatch through on_miss in that order
    // — the same total order the prior single sorted vec produced — so
    // on_miss:error names the globally lowest-input-index miss and
    // on_miss:null_fields visits misses in input order (its rows re-sort through
    // the output buffer, so that order only fixes the FailFast eval-error
    // identity). The residue run each finish spills is charged against the disk
    // quota (E320).
    //
    // Finalize is its own memory phase, distinct from the emit loop the per-pair
    // gates bound, and it is bounded here by construction rather than by a gate.
    // The per-pair input working set is released, and each pile converts from a
    // resident buffer (≤ one spill cap) to a k-way merger over its spilled runs.
    // That merger is the same accepted external-merge primitive the drain phase
    // and the downstream output drain already open un-gated — one resident record
    // per open run — so the two deferred mergers stack the same order of residency
    // finish_and_slice's single drain merger does, on top of the still-resident
    // sides and the output sort (≤ its cap). With the resident sides bounded by
    // the shared block budget and each cap derived at `soft / 4` (16 KiB floored),
    // that stack fits under `soft < hard`. It is not gated by a reservation because
    // the bounded-k cascade (#868) already caps each merger's open-run residency at
    // a fixed fan-in, folding a pathologically fragmented pile's runs down to that
    // many before streaming: a static cap reservation would over-state a merger
    // holding at most that many open runs and false-abort a tight-budget run whose
    // actual finalize residency is a fraction of a cap. The deferred-miss dispatch
    // still polls a global backstop (see `poll_finalize_backstop`) as
    // defense-in-depth against pressure this operator does not itself own.
    let scan_stream = finish_unmatched_stream(scan_unmatched_buf, &drain_ctx)?;
    match match_mode {
        MatchMode::Collect => {
            debug_assert_eq!(
                inblock_buf.total_rows(),
                0,
                "Collect emits a row for every driver, so none routes to the in-block on_miss pile"
            );
            emit_scan_unmatched_collect(&emit_cfg, scan_stream, &mut sink)?;
        }
        MatchMode::First | MatchMode::All => {
            // Each buffer is payload-ordered on `(driver_idx, order)`, so both
            // streams arrive in ascending driver-input-index order and the merge
            // is a total order. `driver_idx` is unique per driver, so the key is
            // total even when a chained upstream repeats `order`.
            let inblock_stream = finish_unmatched_stream(inblock_buf, &drain_ctx)?;
            dispatch_deferred_misses(
                &emit_cfg,
                &mut evals,
                scan_stream,
                inblock_stream,
                on_miss,
                &mut sink,
            )?;
        }
    }

    // Final deterministic order: the payload-ordered output buffer sorts by
    // `(driver order, driver input index, build input index)` — resident when it
    // fit its byte threshold, spilled to sorted runs otherwise. `driver_idx` is
    // unique per driver, so the key is total even when a chained upstream stamps
    // duplicate orders; collect / on_miss rows carry `build_idx == u64::MAX`, so
    // they sort after that driver's match rows (a collect / miss driver has
    // none). The `sink` borrow of the buffer ends at its last use above, so it
    // can be finished here. A final spill of the residue run is charged against
    // the disk quota (E320).
    // Exact emitted-row count, captured before `finish` consumes the buffer.
    // Equals the total rows the output sort will emit across every run, so a
    // downstream that adopts the spilled runs whole can report `len_hint` in
    // O(1) without scanning them off disk.
    let row_count = output_buf.total_rows() as u64;
    let (sorted, residue) = output_buf.finish().map_err(|e| {
        PipelineError::Io(io::Error::other(format!(
            "iejoin block-band output finish failed: {e}"
        )))
    })?;
    if residue > 0 && budget.record_spill_bytes(name, residue) {
        return Err(PipelineError::spill_cap_exceeded(
            name,
            budget.disk_quota(),
            residue,
            budget.cumulative_spill_bytes(),
        ));
    }

    // Re-order the deferred output-eval failures into the same
    // layout-independent order, so the dead-letter output is a pure function of
    // the data too. FailFast surfaces the first-encountered eval error and
    // returns before reaching here; that identity stays visitation-ordered by
    // design — determinizing an error path would defeat fail-fast.
    debug_assert_eq!(output_eval_failures.len(), failure_tags.len());
    let mut failures: Vec<_> = output_eval_failures.into_iter().zip(failure_tags).collect();
    failures.sort_by_key(|(_, key)| *key);
    let output_eval_failures = failures.into_iter().map(|(f, _)| f).collect();

    // The resident blocks are freed as this function returns and the finished
    // output sort is handed to the dispatcher, so discharge the whole handle now
    // — releasing the output sort's mirrored bytes along with the input baseline
    // — and the node-buffer admission that follows never sums a stale footprint.
    // Charging the output sort through this handle during emit never aborted a
    // completing run: the buffer self-bounds by spilling on its own threshold and
    // the emit path polls no global-pressure gate, so a handle transiently above
    // the hard limit is only ever an attribution reading, never an abort trigger.
    consumer.set_bytes(0);

    Ok(BlockBandOutput {
        sorted,
        row_count,
        output_eval_failures,
    })
}

/// Sum the resident (kept-in-RAM) blocks' byte footprints; spilled blocks hold
/// no RAM until their per-pair load.
fn resident_bytes_of<P>(blocks: &[Block<P>]) -> u64 {
    blocks
        .iter()
        .filter_map(|b| match &b.storage {
            BlockStorage::Resident(_) => Some(b.resident_bytes),
            BlockStorage::Spilled(_) => None,
        })
        .sum()
}

/// The transient scratch bytes loading `block` allocates: `resident_bytes` for
/// a spilled block read fresh into a vec, zero for a resident block (already in
/// the baseline and only borrowed).
fn spilled_scratch_bytes<P>(block: &Block<P>) -> u64 {
    match &block.storage {
        BlockStorage::Resident(_) => 0,
        BlockStorage::Spilled(_) => block.resident_bytes,
    }
}

/// Build-side blocks, keyed on `(k1, k2, build_idx)`.
type BuildBlocks = Vec<Block<(i64, i64, u64)>>;

/// Driver payload: primary/secondary range keys, the driver's unique global
/// input index, and its `RecordOrder` tag. `driver_idx` is the sort tiebreak
/// after the keys (`order` may repeat when a chained upstream fans one input
/// row into several) and disambiguates the final output sort.
type DriverPayload = (i64, i64, u64, RecordOrder);

/// Scan-phase unmatched-driver payload: the driver's unique global input index
/// and its `RecordOrder` tag. Sorting by `driver_idx` (unique) yields the
/// deterministic driver-input-index order the deferred on_miss dispatch walks;
/// `order` rides along for the dispatch tag.
type ScanUnmatchedPayload = (u64, RecordOrder);

/// Driver-side drain result: the sliced driver blocks and the scan-phase
/// unmatched drivers held in their own spillable, driver-index-ordered sort
/// buffer for the deferred on_miss dispatch.
type DriverSide = (Vec<Block<DriverPayload>>, SortBuffer<ScanUnmatchedPayload>);

/// Drain the driver side: matched records go into a payload-ordered sort
/// buffer keyed on `(k1, k2, driver_idx, order)` — `driver_idx` (the driver's
/// unique global input index) breaks key ties ahead of the possibly-repeated
/// `order`. Scan-phase unmatched drivers (NULL / non-orderable range key) drain
/// into a second payload-ordered buffer keyed on `(driver_idx, order)` — spilled
/// on the same substrate and quota (E320) as the matched sides, so an input
/// dominated by unmatched drivers stays bounded rather than resident — unless
/// `retain_unmatched` is false (First / All + Skip), where they produce no
/// output and are dropped outright. Returns the sliced blocks and the scan-phase
/// buffer.
fn drain_driver_side(
    driver_records: Vec<(Record, RecordOrder)>,
    driver_scans: Vec<RecordScan>,
    retain_unmatched: bool,
    schema: &Arc<Schema>,
    ctx: &DrainCtx<'_>,
    resident: &mut ResidentBudget,
) -> Result<DriverSide, PipelineError> {
    // The matched, scan-phase, and in-block buffers all carry the driver schema
    // (`schema`, derived once by the caller), so every reload decodes identically.
    let mut scan_buf: SortBuffer<ScanUnmatchedPayload> = SortBuffer::new_payload_ordered(
        ctx.sort_threshold,
        Some(ctx.spill_dir.to_path_buf()),
        ctx.spill_compress,
        Arc::clone(schema),
    );
    if driver_records.is_empty() {
        return Ok((Vec::new(), scan_buf));
    }
    let mut buf: SortBuffer<DriverPayload> = SortBuffer::new_payload_ordered(
        ctx.sort_threshold,
        Some(ctx.spill_dir.to_path_buf()),
        ctx.spill_compress,
        Arc::clone(schema),
    );
    for (driver_idx, ((record, order), scan)) in
        driver_records.into_iter().zip(driver_scans).enumerate()
    {
        let driver_idx = driver_idx as u64;
        match scan {
            RecordScan::Unmatched => {
                if retain_unmatched {
                    push_charge_spill(&mut scan_buf, record, (driver_idx, order), ctx)?;
                }
            }
            RecordScan::Matched {
                range_key: (k1, k2),
                ..
            } => push_charge_spill(&mut buf, record, (k1, k2, driver_idx, order), ctx)?,
        }
    }
    let blocks = finish_and_slice(buf, schema, ctx, resident, &|p: &DriverPayload| (p.0, p.1))?;
    Ok((blocks, scan_buf))
}

/// Drain the build side: matched records go into a payload-ordered sort buffer
/// keyed on `(k1, k2, build_idx)` — the build's original input index, so the
/// downstream First / Collect selection and the output ordering are a pure
/// function of the data. Unmatched builds are dropped (no build-side on_miss).
fn drain_build_side(
    build_records: Vec<Record>,
    build_scans: Vec<RecordScan>,
    ctx: &DrainCtx<'_>,
    resident: &mut ResidentBudget,
) -> Result<BuildBlocks, PipelineError> {
    let Some(first) = build_records.first() else {
        return Ok(Vec::new());
    };
    let schema = Arc::clone(first.schema());
    let mut buf: SortBuffer<(i64, i64, u64)> = SortBuffer::new_payload_ordered(
        ctx.sort_threshold,
        Some(ctx.spill_dir.to_path_buf()),
        ctx.spill_compress,
        Arc::clone(&schema),
    );
    for (build_idx, (record, scan)) in build_records.into_iter().zip(build_scans).enumerate() {
        if let RecordScan::Matched {
            range_key: (k1, k2),
            ..
        } = scan
        {
            push_charge_spill(&mut buf, record, (k1, k2, build_idx as u64), ctx)?;
        }
    }
    finish_and_slice(buf, &schema, ctx, resident, &|p: &(i64, i64, u64)| {
        (p.0, p.1)
    })
}

/// Charge `written` on-disk bytes against the run's spill quota, returning the
/// typed E320 error if this write pushed cumulative spill past the cap. Shared
/// by the drain-spill, finish-residue, and per-block spill sites.
fn charge_block_spill(ctx: &DrainCtx<'_>, written: u64) -> Result<(), PipelineError> {
    if written > 0 && ctx.budget.record_spill_bytes(ctx.name, written) {
        return Err(PipelineError::spill_cap_exceeded(
            ctx.name,
            ctx.budget.disk_quota(),
            written,
            ctx.budget.cumulative_spill_bytes(),
        ));
    }
    Ok(())
}

/// Push one matched pair into the sort buffer, mirror the byte delta into the
/// consumer handle, and spill (charging the run against the disk quota, E320)
/// when the buffer crosses its threshold.
fn push_charge_spill<P: Serialize + DeserializeOwned + Send + Ord>(
    buf: &mut SortBuffer<P>,
    record: Record,
    payload: P,
    ctx: &DrainCtx<'_>,
) -> Result<(), PipelineError> {
    let pre = buf.bytes_used();
    buf.push(record, payload);
    ctx.consumer
        .add_bytes(buf.bytes_used().saturating_sub(pre) as u64);
    if buf.should_spill() {
        let pre_spill = buf.bytes_used() as u64;
        let written = buf.sort_and_spill().map_err(|e| {
            PipelineError::Io(io::Error::other(format!(
                "iejoin block-band drain spill failed: {e}"
            )))
        })?;
        ctx.consumer.sub_bytes(pre_spill);
        charge_block_spill(ctx, written)?;
    }
    Ok(())
}

/// Finish the sort buffer and slice its sorted output into blocks. Every block
/// is admitted resident while the shared budget allows and spilled to its own
/// file otherwise, so a side (or both sides together) that fits the resident
/// budget stays entirely in RAM. The buffer's charge is released so the
/// consumer reflects the resident blocks that replace it — but for the
/// in-memory case that release is deferred until the sorted vec is actually
/// consumed into blocks, so the still-resident vec is never charged out from
/// under the consumer mid-slice.
fn finish_and_slice<P: Serialize + DeserializeOwned + Send + Ord>(
    buf: SortBuffer<P>,
    schema: &Arc<Schema>,
    ctx: &DrainCtx<'_>,
    resident: &mut ResidentBudget,
    key_of: &impl Fn(&P) -> (i64, i64),
) -> Result<Vec<Block<P>>, PipelineError> {
    let pre_finish = buf.bytes_used() as u64;
    let (sorted, residue) = buf.finish().map_err(|e| {
        PipelineError::Io(io::Error::other(format!(
            "iejoin block-band drain finish failed: {e}"
        )))
    })?;
    charge_block_spill(ctx, residue)?;

    // `defer_release` keeps the buffer charge live across slicing for the
    // in-memory case, where the sorted vec is still resident and each admitted
    // block re-charges its own bytes (a transient over-count that never
    // under-reports). The spilled case has already written its runs to disk, so
    // its charge is dropped now — the k-way merge holds only one record per run.
    // An empty in-memory buffer forms no blocks, so release and return early.
    let defer_release = match &sorted {
        SortedOutput::InMemory(pairs) => {
            if pairs.is_empty() {
                ctx.consumer.sub_bytes(pre_finish);
                return Ok(Vec::new());
            }
            true
        }
        SortedOutput::Spilled(_) => {
            ctx.consumer.sub_bytes(pre_finish);
            false
        }
    };
    let stream = sorted_output_stream(
        sorted,
        "iejoin block-band merge",
        MergeBudget {
            budget: ctx.budget,
            node: ctx.name,
            compress: ctx.spill_compress,
        },
    )?;
    let blocks = slice_side(stream, schema, ctx, resident, key_of)?;
    if defer_release {
        // The in-memory sorted vec is now fully consumed into blocks; release
        // its buffer charge, leaving only the admitted resident blocks charged.
        ctx.consumer.sub_bytes(pre_finish);
    }
    Ok(blocks)
}

/// Slice a sorted stream into contiguous `block_target`-sized blocks, admitting
/// each into RAM under the shared resident budget or spilling it otherwise.
/// Streaming over the input: at most one forming block is resident beyond the
/// admitted set, so the spilled-run merge is never collected.
fn slice_side<P: Serialize + DeserializeOwned + Send + Ord>(
    stream: SortedStream<P>,
    schema: &Arc<Schema>,
    ctx: &DrainCtx<'_>,
    resident: &mut ResidentBudget,
    key_of: &impl Fn(&P) -> (i64, i64),
) -> Result<Vec<Block<P>>, PipelineError> {
    let mut blocks: Vec<Block<P>> = Vec::new();
    let mut cur: Vec<(Record, P)> = Vec::new();
    let mut cur_bytes: usize = 0;
    let mut bounds = Bounds::empty();
    for item in stream {
        let (record, payload) = item?;
        let (k1, k2) = key_of(&payload);
        bounds.update(k1, k2);
        cur_bytes += pair_bytes::<P>(&record);
        cur.push((record, payload));
        // A single record larger than the target still forms a block of one —
        // the target is soft.
        if cur_bytes >= ctx.block_target {
            blocks.push(make_block(
                std::mem::take(&mut cur),
                bounds,
                cur_bytes as u64,
                schema,
                ctx,
                resident,
            )?);
            cur_bytes = 0;
            bounds = Bounds::empty();
        }
    }
    if !cur.is_empty() {
        blocks.push(make_block(
            cur,
            bounds,
            cur_bytes as u64,
            schema,
            ctx,
            resident,
        )?);
    }
    Ok(blocks)
}

/// Build one completed block, keeping it resident if the shared budget still
/// admits its bytes (charged into the consumer, since it holds that RAM through
/// the rest of the join) or spilling it to its own file otherwise.
/// `resident_bytes` is the block's estimated footprint, already accumulated by
/// the slicer as it filled the block, so it is not re-summed here.
fn make_block<P: Serialize + DeserializeOwned + Send + Ord>(
    pairs: Vec<(Record, P)>,
    bounds: Bounds,
    resident_bytes: u64,
    schema: &Arc<Schema>,
    ctx: &DrainCtx<'_>,
    resident: &mut ResidentBudget,
) -> Result<Block<P>, PipelineError> {
    let len = pairs.len();
    if resident.admit(resident_bytes) {
        ctx.consumer.add_bytes(resident_bytes);
        Ok(Block {
            storage: BlockStorage::Resident(pairs),
            bounds,
            resident_bytes,
            len,
        })
    } else {
        spill_block(pairs, bounds, len, resident_bytes, schema, ctx)
    }
}

/// Write one block's pairs to a fresh spill file and return the on-disk
/// [`Block`], charging the exact bytes written against the disk quota (E320 on
/// cap). The writer reports its own on-disk byte total, so a block that reaches
/// disk is always charged — no post-hoc `stat` that could transiently fail and
/// silently under-charge the cap.
fn spill_block<P: Serialize>(
    pairs: Vec<(Record, P)>,
    bounds: Bounds,
    len: usize,
    resident_bytes: u64,
    schema: &Arc<Schema>,
    ctx: &DrainCtx<'_>,
) -> Result<Block<P>, PipelineError> {
    let mut writer: SpillWriter<P> =
        SpillWriter::new(Arc::clone(schema), Some(ctx.spill_dir), ctx.spill_compress).map_err(
            |e| {
                PipelineError::Io(io::Error::other(format!(
                    "iejoin block-band block spill open failed: {e}"
                )))
            },
        )?;
    for (record, payload) in &pairs {
        writer.write_pair(record, payload).map_err(|e| {
            PipelineError::Io(io::Error::other(format!(
                "iejoin block-band block spill write failed: {e}"
            )))
        })?;
    }
    let (file, written) = writer.finish_with_bytes().map_err(|e| {
        PipelineError::Io(io::Error::other(format!(
            "iejoin block-band block spill finish failed: {e}"
        )))
    })?;
    charge_block_spill(ctx, written)?;
    Ok(Block {
        storage: BlockStorage::Spilled(file),
        bounds,
        resident_bytes,
        len,
    })
}

/// The deferred in-block-miss drain target handed to the driver-block finalizer:
/// the spillable pile, the spill/charge context it drains through, and the
/// `on_miss` policy that decides whether a miss is recorded at all. Bundled so
/// the finalizer stays within the argument budget.
struct InblockPile<'a, 'ctx> {
    buf: &'a mut SortBuffer<ScanUnmatchedPayload>,
    ctx: &'a DrainCtx<'ctx>,
    on_miss: OnMiss,
}

/// Finalize one driver block after its inner loop over build blocks. Under
/// `Collect`, flush one row per driver (even zero-match drivers, with an empty
/// array). Under `First`, emit each driver's held minimum-build-index candidate
/// — the selection is a pure function of the data, not of the block layout —
/// and gather any driver with no emitting candidate. Under `All`, gather every
/// zero-match driver. Gathered drivers drain into the spillable in-block pile
/// (keyed on `(driver_idx, order)`, charged against the disk quota, E320) for the
/// deferred end-of-join on_miss dispatch, so on_miss:error names the
/// lowest-input-index unmatched driver rather than whichever block finalized
/// first and the pile stays bounded rather than resident. Each driver's global
/// input index (`payload.2`) is carried through so its rows tag deterministically.
fn finalize_driver_block(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    driver_loaded: &[(Record, DriverPayload)],
    state: &mut MatchState,
    pile: &mut InblockPile<'_, '_>,
    sink: &mut EmitSink<'_>,
) -> Result<(), PipelineError> {
    match cfg.match_mode {
        MatchMode::Collect => {
            for (di, (record, payload)) in driver_loaded.iter().enumerate() {
                let flush = state.take_collect(di);
                flush_collect_row(cfg, record, payload.3, payload.2, flush, sink)?;
            }
        }
        MatchMode::First => {
            for (di, (record, payload)) in driver_loaded.iter().enumerate() {
                match state.take_first_candidate(di) {
                    // Emit the minimum-build-index candidate. A body eval that
                    // skips it leaves the driver a miss (no other candidate is
                    // held), routed to the deferred on_miss dispatch.
                    Some((bidx, build)) => {
                        let dref = DriverRef {
                            record,
                            order: payload.3,
                            key: di,
                            driver_idx: payload.2,
                        };
                        if !emit_match_row(cfg, evals, &dref, &build, bidx, sink)? {
                            note_unmatched(pile, record, payload.3, payload.2)?;
                        }
                    }
                    None => note_unmatched(pile, record, payload.3, payload.2)?,
                }
            }
        }
        MatchMode::All => {
            for (di, (record, payload)) in driver_loaded.iter().enumerate() {
                if !state.matched[di] {
                    note_unmatched(pile, record, payload.3, payload.2)?;
                }
            }
        }
    }
    Ok(())
}

/// Finish an unmatched-driver buffer (scan-phase or in-block) into a
/// driver-input-index-ordered stream, charging any residue run this call spills
/// against the disk quota (E320). Mirrors the matched-side finish: an in-memory
/// buffer yields its sorted vec; a spilled one folds its runs through the k-way
/// merger, one resident record per open run — so the stream is bounded
/// regardless of how many drivers were unmatched.
fn finish_unmatched_stream(
    buf: SortBuffer<ScanUnmatchedPayload>,
    ctx: &DrainCtx<'_>,
) -> Result<SortedStream<ScanUnmatchedPayload>, PipelineError> {
    let (sorted, residue) = buf.finish().map_err(|e| {
        PipelineError::Io(io::Error::other(format!(
            "iejoin block-band unmatched finish failed: {e}"
        )))
    })?;
    charge_block_spill(ctx, residue)?;
    sorted_output_stream(
        sorted,
        "iejoin block-band unmatched merge",
        MergeBudget {
            budget: ctx.budget,
            node: ctx.name,
            compress: ctx.spill_compress,
        },
    )
}

/// Stream the scan-phase unmatched drivers back and emit one empty-array
/// `Collect` row each, in driver-input-index order. Bounded: one record resident
/// at a time (the merger holds one record per open run), never the whole pile.
/// The output buffer re-sorts, so this stream order is immaterial to the result.
fn emit_scan_unmatched_collect(
    cfg: &EmitConfig<'_>,
    scan_stream: SortedStream<ScanUnmatchedPayload>,
    sink: &mut EmitSink<'_>,
) -> Result<(), PipelineError> {
    for item in scan_stream {
        let (record, (driver_idx, order)) = item?;
        flush_collect_row(
            cfg,
            &record,
            order,
            driver_idx,
            CollectFlush {
                arr: Vec::new(),
                truncated: false,
                first_build: None,
            },
            sink,
        )?;
    }
    Ok(())
}

/// Dispatch every zero-match driver through its `on_miss` policy in ascending
/// driver-input-index order, two-way merging the streamed scan-phase pile (NULL /
/// non-orderable range key) with the streamed in-block zero-match pile (entered a
/// block, matched nothing). Both streams are payload-ordered on
/// `(driver_idx, order)`. A driver is in exactly one pile, so their driver
/// indices are disjoint and the merge is a total order: on_miss:error names the
/// globally lowest-input-index miss regardless of which pile it came from, and
/// on_miss:null_fields visits misses in that same order (its emitted / dead-letter
/// rows re-sort through the output buffer, so the order only fixes the FailFast
/// eval-error identity). Bounded on both sides — one streamed record resident per
/// open run.
fn dispatch_deferred_misses(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    mut scan_stream: SortedStream<ScanUnmatchedPayload>,
    mut inblock_stream: SortedStream<ScanUnmatchedPayload>,
    on_miss: OnMiss,
    sink: &mut EmitSink<'_>,
) -> Result<(), PipelineError> {
    let mut scan_cur = scan_stream.next().transpose()?;
    let mut inblock_cur = inblock_stream.next().transpose()?;
    let mut emitted_since_check = 0usize;
    loop {
        // Advance the front with the smaller driver index. Disjoint indices, so
        // the `<=` tie branch never actually ties an in-block index; consuming
        // the chosen front by move keeps the merge panic-free.
        let take_scan = match (&scan_cur, &inblock_cur) {
            (Some((_, (scan_idx, _))), Some((_, (inblock_idx, _)))) => scan_idx <= inblock_idx,
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        if take_scan {
            if let Some((record, (driver_idx, order))) = scan_cur {
                dispatch_on_miss(cfg, evals, &record, order, driver_idx, on_miss, sink)?;
            }
            scan_cur = scan_stream.next().transpose()?;
        } else {
            if let Some((record, (driver_idx, order))) = inblock_cur {
                dispatch_on_miss(cfg, evals, &record, order, driver_idx, on_miss, sink)?;
            }
            inblock_cur = inblock_stream.next().transpose()?;
        }
        poll_finalize_backstop(cfg, &mut emitted_since_check)?;
    }
    Ok(())
}

/// Backstop the deferred-miss finalize against a global-pressure overshoot.
///
/// This finalize opens per-pile k-way mergers — two at once under First / All. The
/// bounded-k cascade (#868) folds each pile's spilled runs down to a fixed fan-in
/// before streaming, so a merger's own open-run residency (its loser-tree records
/// and open readers) is capped by construction regardless of how fragmented the
/// pile is; the local per-pair gates plus that fold-down already bound the
/// operator's finalize footprint. This poll is not that bound — it is
/// defense-in-depth. The rest of the block-band path answers pressure by spilling on
/// its own byte thresholds and runs no global poll, but finalize dispatch can still
/// meet memory pressure the operator does not itself own — a co-resident consumer's
/// residency, or host RSS the byte-counted view does not fully capture — so poll the
/// global ceiling here every [`MEMORY_CHECK_INTERVAL`](super::MEMORY_CHECK_INTERVAL)
/// dispatched rows, matching the cadence the engine's other spill operators poll at.
/// An overshoot then aborts cleanly with the typed diagnostic instead of allocating
/// past the hard limit in silence. [`should_abort`](MemoryArbitrator::should_abort)
/// trips on RSS OR the byte-counted consumer sum, so the guard still fires on a host
/// where RSS cannot be measured.
fn poll_finalize_backstop(
    cfg: &EmitConfig<'_>,
    emitted_since_check: &mut usize,
) -> Result<(), PipelineError> {
    *emitted_since_check += 1;
    if *emitted_since_check >= super::MEMORY_CHECK_INTERVAL {
        if cfg.budget.should_abort() {
            return Err(PipelineError::MemoryBudgetExceeded {
                node: cfg.name.to_string(),
                used: cfg.budget.current_pressure(),
                limit: cfg.budget.hard_limit(),
                source: BudgetCategory::Arena,
                detail: Some(
                    "iejoin block-band deferred-miss finalize exceeded budget".to_string(),
                ),
            });
        }
        *emitted_since_check = 0;
    }
    Ok(())
}

/// Drain one zero-match driver into the spillable in-block pile for the deferred
/// end-of-join on_miss dispatch. `Skip` needs nothing (the driver emits no row);
/// `Error` and `NullFields` need the driver held until the full pass completes,
/// so the error names the lowest input index and the null-field rows emit in
/// input order. The buffer spills on its own byte threshold (charging each run
/// against the disk quota, E320), so the pile stays bounded even when most
/// drivers miss. The payload `(driver_idx, order)` keys the buffer on the unique
/// global input index, so the finished stream is already in dispatch order.
fn note_unmatched(
    pile: &mut InblockPile<'_, '_>,
    record: &Record,
    order: RecordOrder,
    driver_idx: u64,
) -> Result<(), PipelineError> {
    if !matches!(pile.on_miss, OnMiss::Skip) {
        push_charge_spill(pile.buf, record.clone(), (driver_idx, order), pile.ctx)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipeline::memory::NoOpPolicy;
    use clinker_plan::config::pipeline_node::PropagateCkSpec;
    use clinker_plan::plan::combine::CombineInput;
    use clinker_plan::plan::execution::ResolvedColumnMap;
    use clinker_record::{Schema, Value};
    use cxl::eval::StableEvalContext;
    use indexmap::IndexMap;
    use proptest::prelude::*;
    use std::collections::{HashMap, HashSet};

    /// A test operator, mirroring `RangeOp` with a direct `apply` for oracles.
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

    const OPS: [TOp; 4] = [TOp::Lt, TOp::Le, TOp::Gt, TOp::Ge];

    /// One test-side record: `Some((k1, k2))` is a matched record with those
    /// range keys; `None` models a NULL / non-orderable range key that the
    /// scan routes to `Unmatched`. `id` tags the row for readback.
    type Side = Vec<(Option<(i64, i64)>, i64)>;

    fn driver_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["k1".into(), "k2".into(), "id".into()]))
    }

    fn build_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["k1".into(), "k2".into(), "id".into()]))
    }

    fn make_rec(schema: &Arc<Schema>, k1: i64, k2: i64, id: i64) -> Record {
        Record::new(
            Arc::clone(schema),
            vec![Value::Integer(k1), Value::Integer(k2), Value::Integer(id)],
        )
    }

    /// Empty resolver mapping. The body-less synthetic emit path never
    /// consults it (no residual, no body), so an empty map suffices.
    fn empty_resolver() -> CombineResolverMapping {
        let map: ResolvedColumnMap = Arc::new(HashMap::new());
        let inputs: IndexMap<String, CombineInput> = IndexMap::new();
        CombineResolverMapping::from_pre_resolved(&map, &inputs)
    }

    fn arbitrator(hard_limit: u64) -> MemoryArbitrator {
        MemoryArbitrator::with_policy(hard_limit, 0.80, 0.70, Box::new(NoOpPolicy))
    }

    fn to_records_scans(side: &Side, schema: &Arc<Schema>) -> (Vec<Record>, Vec<RecordScan>) {
        let records = side
            .iter()
            .map(|(key, id)| {
                let (k1, k2) = key.unwrap_or((0, 0));
                make_rec(schema, k1, k2, *id)
            })
            .collect();
        let scans = side
            .iter()
            .map(|(key, _)| match key {
                Some((k1, k2)) => RecordScan::Matched {
                    eq_keys: Vec::new(),
                    range_key: (*k1, *k2),
                    bucket: 0,
                },
                None => RecordScan::Unmatched,
            })
            .collect();
        (records, scans)
    }

    /// Materialize a `BlockBandOutput`'s payload-ordered sorted handle back into
    /// the flat `(record, order)` sequence the pre-spill kernel returned, so the
    /// assertions can read emitted rows in their final deterministic order. In
    /// memory the buffer's sorted vec is already ordered; a spilled buffer folds
    /// its runs through the k-way merger.
    fn drain_sorted(
        sorted: SortedOutput<(RecordOrder, u64, u64)>,
    ) -> Result<Vec<(Record, RecordOrder)>, PipelineError> {
        // The cascade only charges intermediate runs, which this small fixture
        // never produces; a roomy unlimited-quota arbitrator keeps the test's
        // focus on emitted order.
        let arb = arbitrator(u64::MAX);
        let mut rows = Vec::new();
        for item in sorted_output_stream(
            sorted,
            "block-band test drain",
            MergeBudget {
                budget: &arb,
                node: "block-band test drain",
                compress: true,
            },
        )? {
            let (record, (order, _, _)) = item?;
            rows.push((record, order));
        }
        Ok(rows)
    }

    struct RunCfg {
        op1: RangeOp,
        op2: Option<RangeOp>,
        match_mode: MatchMode,
        on_miss: OnMiss,
        block_target: usize,
        hard_limit: u64,
        max_spill_bytes: Option<u64>,
        /// Test-only sort-buffer spill threshold override. `None` derives it
        /// from the budget (16 KiB floor); a tiny value forces the drain-spill
        /// → merge path under a roomy budget.
        sort_spill: Option<usize>,
        /// Test-only resident-block budget override. `None` derives it from the
        /// budget (`soft / 2`); `Some(0)` spills every block so a test exercises
        /// the per-block spill → reload path under a roomy budget.
        resident_budget: Option<u64>,
    }

    /// Drive the block-band path over synthetic inputs, returning the emitted
    /// records. Constructs the arbitrator from `cfg` and delegates.
    fn run_block(driver: &Side, build: &Side, cfg: &RunCfg) -> Result<Vec<Record>, PipelineError> {
        let budget = arbitrator(cfg.hard_limit);
        if let Some(cap) = cfg.max_spill_bytes {
            budget.set_max_spill_bytes(cap);
        }
        run_block_on(driver, build, cfg, &budget)
    }

    /// Drive the block-band path over synthetic inputs against a caller-owned
    /// arbitrator (so a test can inspect its spill accounting afterwards). Uses
    /// the body-less concat emit path, so `All`/`First` rows are
    /// `driver_values ++ build_values` and `Collect` rows are the driver
    /// widened with a `b` array column.
    fn run_block_on(
        driver: &Side,
        build: &Side,
        cfg: &RunCfg,
        budget: &MemoryArbitrator,
    ) -> Result<Vec<Record>, PipelineError> {
        let d_schema = driver_schema();
        let b_schema = build_schema();
        let out_schema = match cfg.match_mode {
            MatchMode::Collect => Arc::new(Schema::new(vec![
                "k1".into(),
                "k2".into(),
                "id".into(),
                "b".into(),
            ])),
            MatchMode::First | MatchMode::All => Arc::new(Schema::new(vec![
                "d_k1".into(),
                "d_k2".into(),
                "d_id".into(),
                "b_k1".into(),
                "b_k2".into(),
                "b_id".into(),
            ])),
        };

        let (driver_records_bare, driver_scans) = to_records_scans(driver, &d_schema);
        let driver_records: Vec<(Record, RecordOrder)> = driver_records_bare
            .into_iter()
            .enumerate()
            .map(|(i, r)| (r, i as RecordOrder))
            .collect();
        let (build_records, build_scans) = to_records_scans(build, &b_schema);

        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        let resolver = empty_resolver();
        let consumer = ConsumerHandle::new();
        let tmp = tempfile::Builder::new()
            .prefix("iejoin-block-test-")
            .tempdir()
            .expect("temp dir");
        let propagate = PropagateCkSpec::Driver;

        let out = execute_block_band(BlockBandExec {
            name: "block_test",
            build_qualifier: "b",
            driver_records,
            driver_scans,
            build_records,
            build_scans,
            op1: cfg.op1,
            op2: cfg.op2,
            residual_eval: None,
            body_eval: None,
            resolver_mapping: &resolver,
            output_schema: Some(&out_schema),
            match_mode: cfg.match_mode,
            on_miss: cfg.on_miss,
            propagate_ck: &propagate,
            ctx: &ctx,
            budget,
            consumer: &consumer,
            spill_dir: tmp.path(),
            spill_compress: false,
            strategy: ErrorStrategy::FailFast,
            options: BlockBandOptions {
                block_target_override: Some(cfg.block_target),
                sort_spill_override: cfg.sort_spill,
                resident_budget_override: cfg.resident_budget,
            },
        })?;
        assert!(
            out.output_eval_failures.is_empty(),
            "the synthetic emit path never defers an eval failure"
        );
        Ok(drain_sorted(out.sorted)?
            .into_iter()
            .map(|(r, _)| r)
            .collect())
    }

    fn int(v: Option<&Value>) -> i64 {
        match v {
            Some(Value::Integer(n)) => *n,
            other => panic!("expected Integer, got {other:?}"),
        }
    }

    /// Reconstruct the emitted `(driver_id, build_id)` pair set from `All`-mode
    /// concat rows (`d_id` at column 2, `b_id` at column 5).
    fn all_pairs(records: &[Record]) -> HashSet<(i64, i64)> {
        records
            .iter()
            .map(|r| (int(r.get("d_id")), int(r.get("b_id"))))
            .collect()
    }

    /// Nested-loop oracle over the matched (`Some`-keyed) rows.
    fn oracle_pairs(
        driver: &Side,
        build: &Side,
        op1: TOp,
        op2: Option<TOp>,
    ) -> HashSet<(i64, i64)> {
        let mut out = HashSet::new();
        for (dk, did) in driver {
            let Some((dk1, dk2)) = dk else { continue };
            for (bk, bid) in build {
                let Some((bk1, bk2)) = bk else { continue };
                let m1 = op1.apply(*dk1, *bk1);
                let m2 = op2.is_none_or(|o2| o2.apply(*dk2, *bk2));
                if m1 && m2 {
                    out.insert((*did, *bid));
                }
            }
        }
        out
    }

    // ── possible() prune truth table ─────────────────────────────────────

    #[test]
    fn possible_truth_table_over_four_range_configs() {
        // Driver bounds fixed at [10, 20]; build bounds chosen so each axis
        // configuration is disjoint-below, touching-equal, overlapping, or
        // disjoint-above relative to the driver.
        let (dmin, dmax) = (10i64, 20i64);
        // (bmin, bmax, label) fixtures.
        let disjoint_below = (0i64, 5i64); // build strictly below driver
        let touching_equal = (20i64, 30i64); // build min == driver max
        let overlapping = (15i64, 25i64); // ranges overlap
        let disjoint_above = (25i64, 40i64); // build strictly above driver

        // Lt: possible ⇔ dmin < bmax.
        assert!(!possible(
            RangeOp::Lt,
            dmin,
            dmax,
            disjoint_below.0,
            disjoint_below.1
        ));
        assert!(possible(
            RangeOp::Lt,
            dmin,
            dmax,
            touching_equal.0,
            touching_equal.1
        ));
        assert!(possible(
            RangeOp::Lt,
            dmin,
            dmax,
            overlapping.0,
            overlapping.1
        ));
        assert!(possible(
            RangeOp::Lt,
            dmin,
            dmax,
            disjoint_above.0,
            disjoint_above.1
        ));

        // Le: possible ⇔ dmin <= bmax.
        assert!(!possible(
            RangeOp::Le,
            dmin,
            dmax,
            disjoint_below.0,
            disjoint_below.1
        ));
        assert!(possible(
            RangeOp::Le,
            dmin,
            dmax,
            touching_equal.0,
            touching_equal.1
        ));
        assert!(possible(
            RangeOp::Le,
            dmin,
            dmax,
            overlapping.0,
            overlapping.1
        ));
        assert!(possible(
            RangeOp::Le,
            dmin,
            dmax,
            disjoint_above.0,
            disjoint_above.1
        ));
        // A build max exactly at the driver min: Lt cannot match (10 < 10
        // false), Le can (10 <= 10 true).
        assert!(!possible(RangeOp::Lt, dmin, dmax, 0, 10));
        assert!(possible(RangeOp::Le, dmin, dmax, 0, 10));

        // Gt: possible ⇔ dmax > bmin.
        assert!(possible(
            RangeOp::Gt,
            dmin,
            dmax,
            disjoint_below.0,
            disjoint_below.1
        ));
        assert!(possible(
            RangeOp::Gt,
            dmin,
            dmax,
            overlapping.0,
            overlapping.1
        ));
        assert!(!possible(
            RangeOp::Gt,
            dmin,
            dmax,
            disjoint_above.0,
            disjoint_above.1
        ));
        // Build min exactly at driver max: Gt cannot match (20 > 20 false),
        // Ge can (20 >= 20 true).
        assert!(!possible(RangeOp::Gt, dmin, dmax, 20, 30));
        assert!(possible(RangeOp::Ge, dmin, dmax, 20, 30));

        // Ge: possible ⇔ dmax >= bmin.
        assert!(possible(
            RangeOp::Ge,
            dmin,
            dmax,
            disjoint_below.0,
            disjoint_below.1
        ));
        assert!(possible(
            RangeOp::Ge,
            dmin,
            dmax,
            touching_equal.0,
            touching_equal.1
        ));
        assert!(possible(
            RangeOp::Ge,
            dmin,
            dmax,
            overlapping.0,
            overlapping.1
        ));
        assert!(!possible(
            RangeOp::Ge,
            dmin,
            dmax,
            disjoint_above.0,
            disjoint_above.1
        ));
    }

    // ── two-conjunct equivalence to the nested-loop oracle ───────────────

    /// Every prunable/keeper block-pair decision plus the kernel must
    /// reproduce the nested-loop oracle, under a block target of 1 byte that
    /// forces one record per block (maximal pruning + scheduling coverage).
    fn assert_all_equivalent(driver: &Side, build: &Side, op1: TOp, op2: Option<TOp>) {
        let cfg = RunCfg {
            op1: op1.to_range(),
            op2: op2.map(TOp::to_range),
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            block_target: 1,
            hard_limit: 1 << 30,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: None,
        };
        let records = run_block(driver, build, &cfg).expect("block-band run");
        let actual = all_pairs(&records);
        let expected = oracle_pairs(driver, build, op1, op2);
        assert_eq!(
            actual, expected,
            "block-band pair set diverged for ({op1:?}, {op2:?})"
        );
    }

    fn arb_side(len: std::ops::Range<usize>) -> impl Strategy<Value = Side> {
        prop::collection::vec(
            (
                prop_oneof![
                    9 => (-15i64..15, -15i64..15).prop_map(Some),
                    1 => Just(None),
                ],
                0i64..1_000,
            ),
            len,
        )
        .prop_map(|mut rows| {
            // Assign unique ids so the reconstructed pair set is unambiguous.
            for (i, (_, id)) in rows.iter_mut().enumerate() {
                *id = i as i64;
            }
            rows
        })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(96))]
        #[test]
        fn proptest_block_band_two_conjunct_matches_oracle(
            driver in arb_side(0..14),
            build in arb_side(0..14),
            op1_idx in 0usize..4,
            op2_idx in 0usize..4,
        ) {
            assert_all_equivalent(&driver, &build, OPS[op1_idx], Some(OPS[op2_idx]));
        }
    }

    #[test]
    fn two_conjunct_unit_cases() {
        // Duplicates on both axes.
        let dups: Side = (0..8).map(|i| (Some((5, 5)), i)).collect();
        assert_all_equivalent(&dups, &dups.clone(), TOp::Le, Some(TOp::Ge));
        assert_all_equivalent(&dups, &dups.clone(), TOp::Lt, Some(TOp::Ge));

        // NULL keys on each side (Unmatched) alongside valid rows.
        let driver: Side = vec![(Some((3, 3)), 0), (None, 1), (Some((7, 1)), 2)];
        let build: Side = vec![(Some((5, 5)), 0), (None, 1), (Some((9, 0)), 2)];
        assert_all_equivalent(&driver, &build, TOp::Lt, Some(TOp::Le));
        assert_all_equivalent(&driver, &build, TOp::Ge, Some(TOp::Gt));

        // Empty side(s).
        let empty: Side = Vec::new();
        let one: Side = vec![(Some((1, 2)), 0)];
        assert_all_equivalent(&empty, &one, TOp::Lt, Some(TOp::Lt));
        assert_all_equivalent(&one, &empty, TOp::Lt, Some(TOp::Lt));
        assert_all_equivalent(&empty, &empty, TOp::Ge, Some(TOp::Ge));

        // Single row per side.
        let d1: Side = vec![(Some((5, 10)), 0)];
        let b1: Side = vec![(Some((3, 8)), 0)];
        assert_all_equivalent(&d1, &b1, TOp::Gt, Some(TOp::Gt));
        assert_all_equivalent(&d1, &b1, TOp::Lt, Some(TOp::Gt));
    }

    #[test]
    fn degenerate_single_block_matches_multi_block() {
        // A large block target keeps each side resident in one block (the
        // degenerate path); the pair set must match the 1-byte-target run.
        let driver: Side = (0..10).map(|i| (Some((i % 4, i % 3)), i)).collect();
        let build: Side = (0..10).map(|i| (Some((i % 5, i % 2)), i)).collect();
        for op1 in OPS {
            for op2 in OPS {
                let mut resident = RunCfg {
                    op1: op1.to_range(),
                    op2: Some(op2.to_range()),
                    match_mode: MatchMode::All,
                    on_miss: OnMiss::Skip,
                    block_target: 1 << 20,
                    hard_limit: 1 << 30,
                    max_spill_bytes: None,
                    sort_spill: None,
                    resident_budget: None,
                };
                let single = all_pairs(&run_block(&driver, &build, &resident).unwrap());
                resident.block_target = 1;
                let multi = all_pairs(&run_block(&driver, &build, &resident).unwrap());
                let oracle = oracle_pairs(&driver, &build, op1, Some(op2));
                assert_eq!(single, oracle, "degenerate diverged for ({op1:?},{op2:?})");
                assert_eq!(multi, oracle, "multi-block diverged for ({op1:?},{op2:?})");
            }
        }
    }

    /// Force the drain-spill → k-way-merge → block-spill path on both sides,
    /// then prune every block-pair so no pair reaches the pre-output charge.
    /// Verifies the merge + slice + prune machinery runs end-to-end against real
    /// spilled runs and that pruning is correct (empty output), and that spill
    /// actually occurred. `resident_budget: Some(0)` spills every block, so the
    /// merged stream round-trips through per-block files.
    #[test]
    fn drain_spill_merge_and_slice_path_runs_and_prunes() {
        // Driver keys strictly below build keys; op1 = Gt (driver > build) is
        // impossible for every block-pair, so the schedule prunes all of them.
        let driver: Side = (0..400i64).map(|i| (Some((i % 500, 0)), i)).collect();
        let build: Side = (0..400i64)
            .map(|i| (Some((10_000 + (i % 500), 0)), i + 1_000))
            .collect();
        // Small hard limit makes the 16 KiB sort floor bind so the 400-row
        // sides spill their runs; a tiny block target plus a zero resident
        // budget forces per-block spill of the merged stream too.
        let cfg = RunCfg {
            op1: RangeOp::Gt,
            op2: None,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            block_target: 256,
            hard_limit: 32 * 1024,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: Some(0),
        };
        let budget = arbitrator(cfg.hard_limit);
        let records = run_block_on(&driver, &build, &cfg, &budget).expect("drain-spill run");
        assert!(
            records.is_empty(),
            "every block-pair is disjoint under Gt, so nothing survives the prune"
        );
        assert!(
            budget.cumulative_spill_bytes() > 0,
            "the 400-row sides and per-block writes must have spilled to disk"
        );
    }

    /// Force the sort buffer itself to spill (tiny `sort_spill` override) and
    /// spill every block (`resident_budget: Some(0)`) under a roomy budget, so
    /// the full `SortBuffer spill → k-way merge → slice → per-block spill →
    /// reload → kernel → emit` path runs with SURVIVING pairs and the emitted
    /// result matches the oracle. The overrides decouple spilling from the
    /// budget, so the strictly-local pre-output gate never fires under the
    /// roomy 1 GiB limit.
    #[test]
    fn sort_buffer_spill_merge_reload_and_emit_matches_oracle() {
        // 300 drivers over 30 overlapping bands: each driver falls in one band,
        // so the output (~300 rows) stays under the 10K poll while the sides are
        // large enough that a 256-byte sort threshold forces multiple runs.
        let span = 30_000i64;
        let n_bands = 30i64;
        let step = span / n_bands;
        let build: Side = (0..n_bands)
            .map(|b| (Some((b * step, b * step + step)), b))
            .collect();
        let driver: Side = (0..300i64)
            .map(|i| {
                let income = (i.wrapping_mul(101)).rem_euclid(span);
                (Some((income, income)), i)
            })
            .collect();
        // driver.income >= band.lo AND driver.income < band.hi → axis-1 Ge (lo),
        // axis-2 Lt (hi); build tuple is (lo, hi).
        let cfg = RunCfg {
            op1: RangeOp::Ge,
            op2: Some(RangeOp::Lt),
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            block_target: 256,
            hard_limit: 1 << 30,
            max_spill_bytes: None,
            sort_spill: Some(256),
            resident_budget: Some(0),
        };
        let budget = arbitrator(cfg.hard_limit);
        let records = run_block_on(&driver, &build, &cfg, &budget).expect("sort-spill run");
        let actual = all_pairs(&records);
        let expected = oracle_pairs(&driver, &build, TOp::Ge, Some(TOp::Lt));
        assert_eq!(
            actual, expected,
            "sort-buffer spill → merge → reload → emit diverged from the oracle"
        );
        assert!(!expected.is_empty(), "the fixture must produce matches");
        assert!(
            budget.cumulative_spill_bytes() > 0,
            "the tiny sort threshold must have spilled sort runs and blocks"
        );
    }

    // ── single-conjunct (PWMJ) equivalence ───────────────────────────────

    #[test]
    fn pwmj_single_conjunct_matches_oracle() {
        let driver: Side = vec![
            (Some((1, 0)), 0),
            (Some((3, 0)), 1),
            (Some((5, 0)), 2),
            (Some((3, 0)), 3),
            (None, 4),
        ];
        let build: Side = vec![
            (Some((2, 0)), 0),
            (Some((4, 0)), 1),
            (Some((3, 0)), 2),
            (None, 3),
        ];
        for op in OPS {
            let cfg = RunCfg {
                op1: op.to_range(),
                op2: None,
                match_mode: MatchMode::All,
                on_miss: OnMiss::Skip,
                block_target: 1,
                hard_limit: 1 << 30,
                max_spill_bytes: None,
                sort_spill: None,
                resident_budget: None,
            };
            let actual = all_pairs(&run_block(&driver, &build, &cfg).unwrap());
            let expected = oracle_pairs(&driver, &build, op, None);
            assert_eq!(actual, expected, "PWMJ diverged for {op:?}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(64))]
        #[test]
        fn proptest_pwmj_matches_oracle(
            driver in arb_side(0..12),
            build in arb_side(0..12),
            op_idx in 0usize..4,
        ) {
            // Zero the secondary axis so the single-conjunct filler is honored.
            let z = |s: &Side| -> Side {
                s.iter().map(|(k, id)| (k.map(|(a, _)| (a, 0)), *id)).collect()
            };
            assert_all_equivalent_pwmj(&z(&driver), &z(&build), OPS[op_idx]);
        }
    }

    fn assert_all_equivalent_pwmj(driver: &Side, build: &Side, op: TOp) {
        let cfg = RunCfg {
            op1: op.to_range(),
            op2: None,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            block_target: 1,
            hard_limit: 1 << 30,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: None,
        };
        let actual = all_pairs(&run_block(driver, build, &cfg).unwrap());
        let expected = oracle_pairs(driver, build, op, None);
        assert_eq!(actual, expected, "PWMJ diverged for {op:?}");
    }

    // ── match-mode × on_miss, multi-block vs single-block ────────────────

    /// Set of driver ids that emitted at least one row (`First`-mode readback:
    /// the driver id is column 2 of the concat row).
    fn emitted_driver_ids(records: &[Record]) -> Vec<i64> {
        let mut ids: Vec<i64> = records.iter().map(|r| int(r.get("d_id"))).collect();
        ids.sort_unstable();
        ids
    }

    /// Per-driver set of collected build ids (`Collect`-mode readback: the `b`
    /// column holds an array of build user-field maps).
    fn collect_map(records: &[Record]) -> HashMap<i64, HashSet<i64>> {
        let mut out: HashMap<i64, HashSet<i64>> = HashMap::new();
        for r in records {
            let did = int(r.get("id"));
            let entry = out.entry(did).or_default();
            match r.get("b") {
                Some(Value::Array(items)) => {
                    for item in items {
                        match item {
                            Value::Map(m) => {
                                entry.insert(int(m.get("id")));
                            }
                            other => panic!("expected build map, got {other:?}"),
                        }
                    }
                }
                other => panic!("expected build array, got {other:?}"),
            }
        }
        out
    }

    fn run_modes(
        driver: &Side,
        build: &Side,
        mode: MatchMode,
        on_miss: OnMiss,
        target: usize,
    ) -> Vec<Record> {
        let cfg = RunCfg {
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            match_mode: mode,
            on_miss,
            block_target: target,
            hard_limit: 1 << 30,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: None,
        };
        run_block(driver, build, &cfg).expect("mode run")
    }

    #[test]
    fn match_modes_agree_across_block_layouts() {
        // Every driver matches at least one build so on_miss never fires here.
        let driver: Side = vec![
            (Some((1, 9)), 0),
            (Some((2, 8)), 1),
            (Some((3, 7)), 2),
            (Some((1, 9)), 3),
        ];
        let build: Side = vec![
            (Some((5, 5)), 100),
            (Some((4, 6)), 101),
            (Some((9, 9)), 102),
        ];

        // All: full pair set identical.
        let all_single = all_pairs(&run_modes(
            &driver,
            &build,
            MatchMode::All,
            OnMiss::Skip,
            1 << 20,
        ));
        let all_multi = all_pairs(&run_modes(&driver, &build, MatchMode::All, OnMiss::Skip, 1));
        assert_eq!(all_single, all_multi, "All diverged across layouts");

        // First: exactly one row per matched driver; the emitted driver-id set
        // is layout-independent (the chosen build may differ).
        let first_single = emitted_driver_ids(&run_modes(
            &driver,
            &build,
            MatchMode::First,
            OnMiss::Skip,
            1 << 20,
        ));
        let first_multi = emitted_driver_ids(&run_modes(
            &driver,
            &build,
            MatchMode::First,
            OnMiss::Skip,
            1,
        ));
        assert_eq!(first_single, first_multi, "First driver set diverged");
        assert_eq!(
            first_single.len(),
            4,
            "every driver matched exactly one row"
        );

        // Collect: per-driver build-id set is layout-independent.
        let collect_single = collect_map(&run_modes(
            &driver,
            &build,
            MatchMode::Collect,
            OnMiss::Skip,
            1 << 20,
        ));
        let collect_multi = collect_map(&run_modes(
            &driver,
            &build,
            MatchMode::Collect,
            OnMiss::Skip,
            1,
        ));
        assert_eq!(collect_single, collect_multi, "Collect sets diverged");
    }

    #[test]
    fn on_miss_skip_and_collect_handle_zero_match_drivers() {
        // Driver id 2 matches nothing; a NULL-key driver (id 3) is also a miss.
        let driver: Side = vec![
            (Some((1, 9)), 0),
            (Some((2, 8)), 1),
            (Some((100, 0)), 2),
            (None, 3),
        ];
        let build: Side = vec![(Some((5, 5)), 100), (Some((4, 6)), 101)];

        // Skip: only matched drivers emit; layout-independent.
        let skip_single = emitted_driver_ids(&run_modes(
            &driver,
            &build,
            MatchMode::First,
            OnMiss::Skip,
            1 << 20,
        ));
        let skip_multi = emitted_driver_ids(&run_modes(
            &driver,
            &build,
            MatchMode::First,
            OnMiss::Skip,
            1,
        ));
        assert_eq!(skip_single, skip_multi);
        assert_eq!(
            skip_single,
            vec![0, 1],
            "only matched drivers emit under Skip"
        );

        // Collect: every driver emits exactly one row, even zero-match ones
        // (with an empty array), across layouts.
        let collect_single = collect_map(&run_modes(
            &driver,
            &build,
            MatchMode::Collect,
            OnMiss::Skip,
            1 << 20,
        ));
        let collect_multi = collect_map(&run_modes(
            &driver,
            &build,
            MatchMode::Collect,
            OnMiss::Skip,
            1,
        ));
        assert_eq!(
            collect_single, collect_multi,
            "Collect across layouts diverged"
        );
        assert_eq!(
            collect_single.len(),
            4,
            "every driver emits one collect row"
        );
        assert!(
            collect_single[&2].is_empty(),
            "zero-match driver has empty array"
        );
        assert!(
            collect_single[&3].is_empty(),
            "NULL-key driver has empty array"
        );
    }

    #[test]
    fn on_miss_error_fires_consistently_across_layouts() {
        let build: Side = vec![(Some((5, 5)), 100)];

        // A zero-match driver present → Error under every layout.
        let miss_driver: Side = vec![(Some((1, 9)), 0), (Some((100, 0)), 1)];
        for target in [1usize, 1 << 20] {
            let cfg = RunCfg {
                op1: RangeOp::Le,
                op2: Some(RangeOp::Ge),
                match_mode: MatchMode::All,
                on_miss: OnMiss::Error,
                block_target: target,
                hard_limit: 1 << 30,
                max_spill_bytes: None,
                sort_spill: None,
                resident_budget: None,
            };
            let err = run_block(&miss_driver, &build, &cfg).unwrap_err();
            assert!(
                matches!(err, PipelineError::CombineMissingMatch { .. }),
                "expected missing-match error at target {target}, got {err:?}"
            );
        }

        // All drivers match → no error under every layout, identical output.
        let ok_driver: Side = vec![(Some((1, 9)), 0), (Some((2, 8)), 1)];
        let single = all_pairs(
            &run_block(
                &ok_driver,
                &build,
                &RunCfg {
                    op1: RangeOp::Le,
                    op2: Some(RangeOp::Ge),
                    match_mode: MatchMode::All,
                    on_miss: OnMiss::Error,
                    block_target: 1 << 20,
                    hard_limit: 1 << 30,
                    max_spill_bytes: None,
                    sort_spill: None,
                    resident_budget: None,
                },
            )
            .unwrap(),
        );
        let multi = all_pairs(
            &run_block(
                &ok_driver,
                &build,
                &RunCfg {
                    op1: RangeOp::Le,
                    op2: Some(RangeOp::Ge),
                    match_mode: MatchMode::All,
                    on_miss: OnMiss::Error,
                    block_target: 1,
                    hard_limit: 1 << 30,
                    max_spill_bytes: None,
                    sort_spill: None,
                    resident_budget: None,
                },
            )
            .unwrap(),
        );
        assert_eq!(single, multi);
    }

    // ── E320 spill-cap enforcement ─────────────────────────────────────────

    #[test]
    fn block_spill_over_tiny_cap_raises_e320() {
        // Enough rows and a tiny block target that every block is written to
        // disk; a 1-byte spill cap trips E320 at the first block (or sort-run)
        // write.
        let driver: Side = (0..200i64).map(|i| (Some((i, i)), i)).collect();
        let build: Side = (0..200i64).map(|i| (Some((i, i)), i + 1_000)).collect();
        let cfg = RunCfg {
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            block_target: 128,
            // Small enough that the 16 KiB sort floor binds; a zero resident
            // budget spills every block, and the 1-byte cap forbids any spilled
            // byte, so the first sort-run or block write trips E320.
            hard_limit: 32 * 1024,
            max_spill_bytes: Some(1),
            sort_spill: None,
            resident_budget: Some(0),
        };
        let err = run_block(&driver, &build, &cfg).expect_err("tiny spill cap must abort");
        assert!(
            matches!(err, PipelineError::SpillCapExceeded { .. }),
            "expected E320 spill-cap error, got {err:?}"
        );
    }

    // ── mid-size sides stay resident (no spill) ──────────────────────────

    #[test]
    fn mid_size_multi_block_sides_stay_resident_without_spill() {
        // Each side is multi-block (many records past a tiny block target) yet
        // both together fit a generous resident budget, so nothing
        // spills — the in-RAM path the previous spill-everything-when-multi-block
        // policy lost for mid-size inputs. A roomy hard limit keeps the sort
        // threshold high so no sort run spills either; the only possible spill is
        // a block, which the budget forbids.
        let driver: Side = (0..60).map(|i| (Some((i % 7, i % 5)), i)).collect();
        let build: Side = (0..60).map(|i| (Some((i % 6, i % 4)), i + 1_000)).collect();
        let cfg = RunCfg {
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            // Tiny target → each 60-row side slices into many blocks.
            block_target: 512,
            hard_limit: 1 << 30,
            max_spill_bytes: None,
            sort_spill: None,
            // Far above both sides combined → every block is admitted resident.
            resident_budget: Some(1 << 20),
        };
        let budget = arbitrator(cfg.hard_limit);
        let records = run_block_on(&driver, &build, &cfg, &budget).expect("mid-size run");
        let actual = all_pairs(&records);
        let expected = oracle_pairs(&driver, &build, TOp::Le, Some(TOp::Ge));
        assert_eq!(
            actual, expected,
            "mid-size resident run diverged from the oracle"
        );
        assert!(!expected.is_empty(), "the fixture must produce matches");
        assert_eq!(
            budget.cumulative_spill_bytes(),
            0,
            "multi-block sides fitting the resident budget must not spill; spilled {} bytes",
            budget.cumulative_spill_bytes()
        );
    }

    // ── dense block-pair typed abort ────────────────────────────────────

    #[test]
    fn dense_block_pair_aborts_with_typed_error_not_oom() {
        // Every driver matches every build (all keys equal, `<= AND >=`), so a
        // single block-pair materializes an N*M pair-index vector. A budget that
        // admits both blocks and the kernel aux but not the pair vector must
        // abort with the typed MemoryBudgetExceeded (the same allocation the
        // equi+range path gates), not OOM the process.
        let n = 200i64;
        let driver: Side = (0..n).map(|i| (Some((5, 5)), i)).collect();
        let build: Side = (0..n).map(|i| (Some((5, 5)), i + 10_000)).collect();
        let cfg = RunCfg {
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            // One block per side (all rows resident together), so the surviving
            // pair is a single near-cross-product.
            block_target: 1 << 20,
            // Both ~small blocks plus the kernel aux fit, but the 200×200 pair
            // vector (~640 KiB) pushes the local peak over this limit.
            hard_limit: 256 * 1024,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: None,
        };
        let err = run_block(&driver, &build, &cfg).expect_err("dense pair must abort");
        match err {
            PipelineError::MemoryBudgetExceeded { detail, source, .. } => {
                assert_eq!(source, clinker_plan::BudgetCategory::Arena);
                assert!(
                    detail
                        .as_deref()
                        .unwrap_or("")
                        .contains("iejoin pre-output"),
                    "abort must come from the pre-output gate; got {detail:?}"
                );
            }
            other => {
                panic!("expected MemoryBudgetExceeded from the pairs-vector charge; got {other:?}")
            }
        }
    }

    // ── output determinism across block layouts ──────────────────────────

    /// Ordered `(driver_id, build_id)` sequence from `All`/`First` concat rows.
    fn ordered_pairs(records: &[Record]) -> Vec<(i64, i64)> {
        records
            .iter()
            .map(|r| (int(r.get("d_id")), int(r.get("b_id"))))
            .collect()
    }

    /// Ordered `(driver_id, [build_ids in array order])` sequence from `Collect`
    /// rows, preserving both the row order and the within-row array order.
    fn ordered_collect(records: &[Record]) -> Vec<(i64, Vec<i64>)> {
        records
            .iter()
            .map(|r| {
                let did = int(r.get("id"));
                let arr = match r.get("b") {
                    Some(Value::Array(items)) => items
                        .iter()
                        .map(|item| match item {
                            Value::Map(m) => int(m.get("id")),
                            other => panic!("expected build map, got {other:?}"),
                        })
                        .collect(),
                    other => panic!("expected build array, got {other:?}"),
                };
                (did, arr)
            })
            .collect()
    }

    /// Overlapping-band fixture: driver `i` has key `(10i, 10i)`; build `j` is
    /// `(0, 50(j+1))` = lo 0, hi 50(j+1). Under `Ge` (amount ≥ lo) `AND` `Lt`
    /// (amount < hi), driver `i` matches every build `j` with `50(j+1) > 10i`,
    /// so most drivers match several builds — the case where First selection and
    /// array order actually depend on which build is seen first.
    fn overlapping_fixture() -> (Side, Side) {
        let driver: Side = (0..40i64).map(|i| (Some((10 * i, 10 * i)), i)).collect();
        let build: Side = (0..20i64).map(|j| (Some((0, 50 * (j + 1))), j)).collect();
        (driver, build)
    }

    fn det_cfg(
        mode: MatchMode,
        on_miss: OnMiss,
        block_target: usize,
        resident: Option<u64>,
    ) -> RunCfg {
        RunCfg {
            op1: RangeOp::Ge,
            op2: Some(RangeOp::Lt),
            match_mode: mode,
            on_miss,
            block_target,
            hard_limit: 1 << 30,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: resident,
        }
    }

    #[test]
    fn output_is_byte_identical_across_block_layouts() {
        // Two layouts of the same data: one resident single block per side, one
        // one-record-per-block forced fully to spill. Under the determinism
        // invariant the ordered output must be identical between them (a proxy
        // for "identical across pipeline.memory.limit").
        let (driver, build) = overlapping_fixture();
        let resident_layout = |mode, on_miss| det_cfg(mode, on_miss, 1 << 20, None);
        let spilled_layout = |mode, on_miss| det_cfg(mode, on_miss, 1, Some(0));

        // All: full ordered pair sequence identical.
        let all_resident = ordered_pairs(
            &run_block(
                &driver,
                &build,
                &resident_layout(MatchMode::All, OnMiss::Skip),
            )
            .unwrap(),
        );
        let all_spilled = ordered_pairs(
            &run_block(
                &driver,
                &build,
                &spilled_layout(MatchMode::All, OnMiss::Skip),
            )
            .unwrap(),
        );
        assert_eq!(
            all_resident, all_spilled,
            "All output order changed with block layout"
        );
        assert!(!all_resident.is_empty(), "the fixture must produce matches");

        // First: the selected build per driver is identical.
        let first_resident = ordered_pairs(
            &run_block(
                &driver,
                &build,
                &resident_layout(MatchMode::First, OnMiss::Skip),
            )
            .unwrap(),
        );
        let first_spilled = ordered_pairs(
            &run_block(
                &driver,
                &build,
                &spilled_layout(MatchMode::First, OnMiss::Skip),
            )
            .unwrap(),
        );
        assert_eq!(
            first_resident, first_spilled,
            "First selection changed with block layout"
        );
        // Every First driver picks the minimum build index among its matches.
        for (did, bid) in &first_resident {
            let min_match = build
                .iter()
                .filter(|(k, _)| k.map(|(_, hi)| 10 * did < hi).unwrap_or(false))
                .map(|(_, j)| *j)
                .min()
                .expect("a First-emitting driver has at least one match");
            assert_eq!(*bid, min_match, "driver {did} must pick min build index");
        }

        // Collect: row order and within-row array order identical.
        let collect_resident = ordered_collect(
            &run_block(
                &driver,
                &build,
                &resident_layout(MatchMode::Collect, OnMiss::Skip),
            )
            .unwrap(),
        );
        let collect_spilled = ordered_collect(
            &run_block(
                &driver,
                &build,
                &spilled_layout(MatchMode::Collect, OnMiss::Skip),
            )
            .unwrap(),
        );
        assert_eq!(
            collect_resident, collect_spilled,
            "Collect order changed with block layout"
        );
    }

    #[test]
    fn on_miss_error_names_same_lowest_index_driver_across_layouts() {
        // Two drivers match nothing: id 3 (higher range key) and id 7 (lower
        // range key). on_miss:error must name the lowest INPUT index (3) at both
        // block layouts, not whichever range-key-ordered block finalizes first.
        let mut driver: Side = (0..10i64).map(|i| (Some((10 * i, 10 * i)), i)).collect();
        // Push ids 3 and 7 above every band's hi so they never match.
        driver[3] = (Some((100_000, 100_000)), 3);
        driver[7] = (Some((90_000, 90_000)), 7);
        let (_, build) = overlapping_fixture();

        let resident = run_block(
            &driver,
            &build,
            &det_cfg(MatchMode::All, OnMiss::Error, 1 << 20, None),
        )
        .expect_err("a zero-match driver must error");
        let spilled = run_block(
            &driver,
            &build,
            &det_cfg(MatchMode::All, OnMiss::Error, 1, Some(0)),
        )
        .expect_err("a zero-match driver must error");
        for err in [&resident, &spilled] {
            match err {
                PipelineError::CombineMissingMatch { driver_row, .. } => {
                    assert_eq!(
                        *driver_row, 3,
                        "on_miss:error must name the lowest input index"
                    );
                }
                other => panic!("expected CombineMissingMatch; got {other:?}"),
            }
        }
    }

    #[test]
    fn on_miss_error_names_lowest_input_index_when_order_differs_from_index() {
        // A chained upstream can stamp RecordOrder out of step with input
        // position. Two zero-match drivers: input index 1 carries order 30,
        // input index 2 carries the SMALLER order 10. on_miss:error must name
        // the lowest INPUT INDEX miss (index 1, whose order is 30) — the identity
        // the equi+range arm enforces by sorting unmatched drivers on index
        // alone — not the lowest-order miss (index 2, order 10) that an
        // (order, driver_idx) sort would surface. `run_block` stamps order ==
        // index, so it cannot tell the two apart; this fixture sets them apart.
        let d_schema = driver_schema();
        let b_schema = build_schema();
        let out_schema = Arc::new(Schema::new(vec![
            "d_k1".into(),
            "d_k2".into(),
            "d_id".into(),
            "b_k1".into(),
            "b_k2".into(),
            "b_id".into(),
        ]));
        // (k1, k2, id, order). The two matches (key 5,5) keep the miss set from
        // being the whole input; the two misses use an unreachable key.
        let driver_keyed: [(i64, i64, i64, RecordOrder); 4] = [
            (5, 5, 0, 50),
            (100_000, 100_000, 1, 30),
            (100_000, 100_000, 2, 10),
            (5, 5, 3, 40),
        ];
        let build: Side = (0..3).map(|i| (Some((5, 5)), 100 + i)).collect();

        let run = |block_target: usize, resident: Option<u64>| -> PipelineError {
            let driver_records: Vec<(Record, RecordOrder)> = driver_keyed
                .iter()
                .map(|(k1, k2, id, order)| (make_rec(&d_schema, *k1, *k2, *id), *order))
                .collect();
            let driver_scans: Vec<RecordScan> = driver_keyed
                .iter()
                .map(|(k1, k2, _, _)| RecordScan::Matched {
                    eq_keys: Vec::new(),
                    range_key: (*k1, *k2),
                    bucket: 0,
                })
                .collect();
            let (build_records, build_scans) = to_records_scans(&build, &b_schema);
            let budget = arbitrator(1 << 30);
            let stable = StableEvalContext::test_default();
            let ctx = EvalContext::test_default_borrowed(&stable);
            let resolver = empty_resolver();
            let consumer = ConsumerHandle::new();
            let tmp = tempfile::Builder::new()
                .prefix("iejoin-miss-order-")
                .tempdir()
                .expect("temp dir");
            let propagate = PropagateCkSpec::Driver;
            execute_block_band(BlockBandExec {
                name: "miss_order",
                build_qualifier: "b",
                driver_records,
                driver_scans,
                build_records,
                build_scans,
                op1: RangeOp::Le,
                op2: Some(RangeOp::Ge),
                residual_eval: None,
                body_eval: None,
                resolver_mapping: &resolver,
                output_schema: Some(&out_schema),
                match_mode: MatchMode::All,
                on_miss: OnMiss::Error,
                propagate_ck: &propagate,
                ctx: &ctx,
                budget: &budget,
                consumer: &consumer,
                spill_dir: tmp.path(),
                spill_compress: false,
                strategy: ErrorStrategy::FailFast,
                options: BlockBandOptions {
                    block_target_override: Some(block_target),
                    sort_spill_override: None,
                    resident_budget_override: resident,
                },
            })
            .expect_err("a zero-match driver must error under on_miss: error")
        };

        for err in [run(1 << 20, None), run(1, Some(0))] {
            match err {
                PipelineError::CombineMissingMatch { driver_row, .. } => {
                    assert_eq!(
                        driver_row, 30,
                        "on_miss:error must name the lowest INPUT INDEX miss \
                         (index 1, order 30), not the lowest-order miss (index 2, order 10)"
                    );
                }
                other => panic!("expected CombineMissingMatch; got {other:?}"),
            }
        }
    }

    // ── duplicate driver RecordOrder stays deterministic ──────────────────

    #[test]
    fn duplicate_record_order_output_is_deterministic_across_layouts() {
        // A chained upstream can stamp several driver rows with the same
        // RecordOrder (one input row fanned into many). When two such rows match
        // the same builds, the (order, build_idx) key collides; the driver's
        // unique global input index breaks the tie, so the emitted order is a
        // pure function of the data — identical across block layouts.
        let d_schema = driver_schema();
        let b_schema = build_schema();
        let out_schema = Arc::new(Schema::new(vec![
            "d_k1".into(),
            "d_k2".into(),
            "d_id".into(),
            "b_k1".into(),
            "b_k2".into(),
            "b_id".into(),
        ]));
        // Four drivers, all key (5, 5); ids 1 and 2 share the duplicate order 7.
        let driver_keyed: [(i64, i64, i64, RecordOrder); 4] =
            [(5, 5, 0, 0), (5, 5, 1, 7), (5, 5, 2, 7), (5, 5, 3, 9)];
        let build: Side = (0..4).map(|i| (Some((5, 5)), 100 + i)).collect();

        let run = |block_target: usize, resident: Option<u64>| -> Vec<(i64, i64)> {
            let driver_records: Vec<(Record, RecordOrder)> = driver_keyed
                .iter()
                .map(|(k1, k2, id, order)| (make_rec(&d_schema, *k1, *k2, *id), *order))
                .collect();
            let driver_scans: Vec<RecordScan> = driver_keyed
                .iter()
                .map(|(k1, k2, _, _)| RecordScan::Matched {
                    eq_keys: Vec::new(),
                    range_key: (*k1, *k2),
                    bucket: 0,
                })
                .collect();
            let (build_records, build_scans) = to_records_scans(&build, &b_schema);
            let budget = arbitrator(1 << 30);
            let stable = StableEvalContext::test_default();
            let ctx = EvalContext::test_default_borrowed(&stable);
            let resolver = empty_resolver();
            let consumer = ConsumerHandle::new();
            let tmp = tempfile::Builder::new()
                .prefix("iejoin-dup-order-")
                .tempdir()
                .expect("temp dir");
            let propagate = PropagateCkSpec::Driver;
            let out = execute_block_band(BlockBandExec {
                name: "dup_order",
                build_qualifier: "b",
                driver_records,
                driver_scans,
                build_records,
                build_scans,
                op1: RangeOp::Le,
                op2: Some(RangeOp::Ge),
                residual_eval: None,
                body_eval: None,
                resolver_mapping: &resolver,
                output_schema: Some(&out_schema),
                match_mode: MatchMode::All,
                on_miss: OnMiss::Skip,
                propagate_ck: &propagate,
                ctx: &ctx,
                budget: &budget,
                consumer: &consumer,
                spill_dir: tmp.path(),
                spill_compress: false,
                strategy: ErrorStrategy::FailFast,
                options: BlockBandOptions {
                    block_target_override: Some(block_target),
                    sort_spill_override: None,
                    resident_budget_override: resident,
                },
            })
            .expect("dup-order run");
            drain_sorted(out.sorted)
                .expect("drain dup-order output")
                .iter()
                .map(|(r, _)| (int(r.get("d_id")), int(r.get("b_id"))))
                .collect()
        };

        let resident_layout = run(1 << 20, None);
        let spilled_layout = run(1, Some(0));
        assert_eq!(
            resident_layout, spilled_layout,
            "output order changed with block layout despite duplicate RecordOrder"
        );
        // Every one of the four drivers matches every one of the four builds.
        assert_eq!(
            resident_layout.len(),
            16,
            "the 4x4 match set must be complete"
        );
    }

    // ── held First candidates over budget abort with the typed error ─────

    #[test]
    fn held_first_candidates_over_budget_abort_typed() {
        // match:first holds one build record per matched driver until the driver
        // block finalizes. With wide build records and many small drivers in a
        // single driver block, that held residency grows across build blocks; the
        // per-pair pre-output gate must fold it in and abort with the typed
        // budget error rather than let it grow unbounded.
        let d_schema = driver_schema();
        let b_schema = Arc::new(Schema::new(vec![
            "k1".into(),
            "k2".into(),
            "id".into(),
            "pad".into(),
        ]));
        let out_schema = Arc::new(Schema::new(vec![
            "d_k1".into(),
            "d_k2".into(),
            "d_id".into(),
            "b_k1".into(),
            "b_k2".into(),
            "b_id".into(),
            "b_pad".into(),
        ]));
        let n = 300i64;
        let pad = "w".repeat(2048);
        // Driver i has key (i, i); build i has key (i, i) plus a wide pad. Under
        // `Le AND Ge` driver i matches only build i, so match:first holds build
        // i's wide record — one held candidate per driver across the block.
        let driver_records: Vec<(Record, RecordOrder)> = (0..n)
            .map(|i| (make_rec(&d_schema, i, i, i), i as RecordOrder))
            .collect();
        let driver_scans: Vec<RecordScan> = (0..n)
            .map(|i| RecordScan::Matched {
                eq_keys: Vec::new(),
                range_key: (i, i),
                bucket: 0,
            })
            .collect();
        let build_records: Vec<Record> = (0..n)
            .map(|i| {
                Record::new(
                    Arc::clone(&b_schema),
                    vec![
                        Value::Integer(i),
                        Value::Integer(i),
                        Value::Integer(i + 10_000),
                        Value::string_unique(&pad),
                    ],
                )
            })
            .collect();
        let build_scans: Vec<RecordScan> = (0..n)
            .map(|i| RecordScan::Matched {
                eq_keys: Vec::new(),
                range_key: (i, i),
                bucket: 0,
            })
            .collect();

        let budget = arbitrator(256 * 1024);
        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        let resolver = empty_resolver();
        let consumer = ConsumerHandle::new();
        let tmp = tempfile::Builder::new()
            .prefix("iejoin-held-")
            .tempdir()
            .expect("temp dir");
        let propagate = PropagateCkSpec::Driver;
        let err = execute_block_band(BlockBandExec {
            name: "held_test",
            build_qualifier: "b",
            driver_records,
            driver_scans,
            build_records,
            build_scans,
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            residual_eval: None,
            body_eval: None,
            resolver_mapping: &resolver,
            output_schema: Some(&out_schema),
            match_mode: MatchMode::First,
            on_miss: OnMiss::Skip,
            propagate_ck: &propagate,
            ctx: &ctx,
            budget: &budget,
            consumer: &consumer,
            spill_dir: tmp.path(),
            spill_compress: false,
            strategy: ErrorStrategy::FailFast,
            options: BlockBandOptions {
                // All 300 narrow drivers fit one block; each build block holds
                // only a few wide builds, so the driver block iterates many build
                // blocks and the held candidates accumulate across them.
                block_target_override: Some(64 * 1024),
                sort_spill_override: Some(1 << 30),
                // Spill every block so the resident baseline stays ~zero and the
                // growing held candidates are the term that crosses the budget.
                resident_budget_override: Some(0),
            },
        })
        .expect_err("accumulated held candidates over the budget must abort");
        match err {
            PipelineError::MemoryBudgetExceeded { detail, source, .. } => {
                assert_eq!(source, clinker_plan::BudgetCategory::Arena);
                assert!(
                    detail
                        .as_deref()
                        .unwrap_or("")
                        .contains("iejoin pre-output"),
                    "abort must come from the pre-output gate; got {detail:?}"
                );
            }
            other => {
                panic!("expected MemoryBudgetExceeded from the held-state gate; got {other:?}")
            }
        }
    }

    // ── spilled driver block gated before its load ────────────────────────

    #[test]
    fn spilled_driver_block_over_hard_limit_aborts_before_load() {
        // Driver keys strictly below build keys under Gt → every block-pair is
        // pruned, so no per-pair build gate ever runs. The driver side is one
        // spilled block whose scratch alone exceeds the hard limit; without a
        // driver-load gate the run would materialize it over budget and then
        // complete (everything pruned). The metadata gate must abort at the
        // driver load instead.
        let driver: Side = (0..500i64).map(|i| (Some((i % 100, 0)), i)).collect();
        let build: Side = (0..10i64)
            .map(|i| (Some((10_000 + i, 0)), i + 1_000))
            .collect();
        let cfg = RunCfg {
            op1: RangeOp::Gt,
            op2: None,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            // One block holds all 500 drivers (~80 KB); a zero resident budget
            // spills it, so its load would allocate over-hard scratch.
            block_target: 1 << 30,
            hard_limit: 16 * 1024,
            max_spill_bytes: None,
            // Keep the sort buffer in memory so the whole side forms one block.
            sort_spill: Some(1 << 30),
            resident_budget: Some(0),
        };
        let err = run_block(&driver, &build, &cfg)
            .expect_err("an over-hard spilled driver block must abort at its load");
        match err {
            PipelineError::MemoryBudgetExceeded { detail, source, .. } => {
                assert_eq!(source, clinker_plan::BudgetCategory::Arena);
                assert!(
                    detail
                        .as_deref()
                        .unwrap_or("")
                        .contains("iejoin pre-output"),
                    "abort must come from the driver-load gate; got {detail:?}"
                );
            }
            other => {
                panic!("expected MemoryBudgetExceeded from the driver-load gate; got {other:?}")
            }
        }
    }

    // ── scan-phase unmatched-driver residency is bounded ─────────────────────

    #[test]
    fn all_unmatched_driver_heavy_scan_phase_spills_under_tight_budget() {
        // The pathological all-unmatched shape: every driver has a NULL /
        // non-orderable range key, so none enters a block and all route to the scan-phase
        // pile. Under on_miss:error the run drains and spills that whole pile,
        // then streams it back and errors on the lowest-input-index miss. With no
        // driver blocks and no emitted rows, the only structure that can reach
        // disk is the scan-phase buffer — so a non-zero cumulative spill proves
        // that pile was bounded to disk rather than held resident (the old code
        // held a resident Vec and spilled nothing here).
        let driver: Side = (0..800i64).map(|i| (None, i)).collect();
        let build: Side = vec![(Some((5, 5)), 100), (Some((6, 4)), 101)];
        let cfg = RunCfg {
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            match_mode: MatchMode::All,
            on_miss: OnMiss::Error,
            block_target: 256,
            // Small enough that the 16 KiB sort floor binds, so the 800-driver
            // scan-phase buffer overflows and spills its runs.
            hard_limit: 32 * 1024,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: None,
        };
        let budget = arbitrator(cfg.hard_limit);
        let err = run_block_on(&driver, &build, &cfg, &budget)
            .expect_err("an all-unmatched input under on_miss:error must error");
        match err {
            PipelineError::CombineMissingMatch { driver_row, .. } => assert_eq!(
                driver_row, 0,
                "on_miss:error must name the lowest-input-index miss"
            ),
            other => panic!("expected CombineMissingMatch; got {other:?}"),
        }
        assert!(
            budget.cumulative_spill_bytes() > 0,
            "the 800-driver scan-phase pile must have spilled to disk, not stayed resident"
        );
    }

    /// Fixed-usage consumer: reports `bytes` every arbitration round and never
    /// spills, so a test can pin `sum_consumer_usage` above the hard limit
    /// deterministically — the byte-counted abort arm, independent of host RSS.
    struct PinnedConsumer {
        bytes: u64,
    }

    impl crate::pipeline::memory::MemoryConsumer for PinnedConsumer {
        fn current_usage(&self) -> u64 {
            self.bytes
        }
        fn spill_priority(&self) -> i32 {
            0
        }
        fn try_spill(
            &self,
            _target: u64,
        ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
            Ok(0)
        }
        fn can_back_pressure(&self) -> bool {
            false
        }
    }

    /// Compile a constant-emit combine body into a `ProgramEvaluator`, so an
    /// on_miss:null_fields dispatch emits a row (advancing the finalize poll
    /// counter) without consulting the resolver.
    fn constant_body() -> ProgramEvaluator {
        use cxl::lexer::Span;
        use cxl::parser::Parser;
        use cxl::resolve::pass::resolve_program;
        use cxl::typecheck::pass::type_check;
        use cxl::typecheck::row::Row;
        let parsed = Parser::parse("emit d_id = 0");
        assert!(
            parsed.errors.is_empty(),
            "parse errors: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        let resolved = resolve_program(parsed.ast, &["k1", "k2", "id"], parsed.node_count)
            .unwrap_or_else(|d| panic!("resolve errors: {d:?}"));
        let row = Row::closed(IndexMap::new(), Span::new(0, 0));
        let typed = type_check(resolved, &row).unwrap_or_else(|d| panic!("type errors: {d:?}"));
        ProgramEvaluator::new(Arc::new(typed), false)
    }

    #[test]
    fn deferred_miss_finalize_backstop_aborts_cleanly_on_budget_breach() {
        // The deferred-miss finalize opens a k-way merger per on-miss pile — two
        // at once under First/All — whose open-run residency the strictly-local
        // per-pair gates never reserve, so a fragmented pile can lift it toward the
        // hard limit. This drives the exact driver-heavy-unmatched shape (every
        // driver unmatched, so all route through the finalize dispatch) with the
        // byte-counted abort arm already breached by a co-resident pinned consumer,
        // and asserts the finalize backstop surfaces a clean typed
        // MemoryBudgetExceeded rather than allocating past the ceiling in silence.
        // The pinned charge drives `sum_consumer_usage` past `hard` with no real
        // allocation, so the trigger is host-independent: it does not rely on the
        // process RSS the other arm reads.

        // 1 GiB hard limit sits above any host's test RSS, so the RSS arm of
        // should_abort cannot trip; the registered pinned consumer alone pushes
        // sum_consumer_usage past the ceiling.
        let hard = 1u64 << 30;
        let budget = arbitrator(hard);
        budget.register_consumer(Arc::new(PinnedConsumer { bytes: hard + 1 }));
        assert!(
            budget.sum_consumer_usage() > budget.hard_limit(),
            "test invariant: the pinned consumer must arm the byte-counted abort arm"
        );
        assert!(
            budget.peak_rss().is_none_or(|rss| rss < hard),
            "test invariant: host RSS must stay under the 1 GiB ceiling so only the \
             byte-counted arm can trip the finalize backstop"
        );

        // Every driver is scan-phase unmatched (NULL range key); one more than a
        // full MEMORY_CHECK_INTERVAL of them, so the finalize dispatch reaches its
        // first poll. The build is immaterial — no driver enters a block.
        let interval = crate::pipeline::iejoin::MEMORY_CHECK_INTERVAL;
        let d_schema = driver_schema();
        let b_schema = build_schema();
        let out_schema = Arc::new(Schema::new(vec![
            "d_k1".into(),
            "d_k2".into(),
            "d_id".into(),
            "b_k1".into(),
            "b_k2".into(),
            "b_id".into(),
        ]));
        let driver: Side = (0..(interval as i64 + 1)).map(|i| (None, i)).collect();
        let build: Side = vec![(Some((5, 5)), 100)];
        let (driver_bare, driver_scans) = to_records_scans(&driver, &d_schema);
        let driver_records: Vec<(Record, RecordOrder)> = driver_bare
            .into_iter()
            .enumerate()
            .map(|(i, r)| (r, i as RecordOrder))
            .collect();
        let (build_records, build_scans) = to_records_scans(&build, &b_schema);

        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        let resolver = empty_resolver();
        // The operator's own handle is unregistered, so only the pinned consumer
        // feeds `sum_consumer_usage`; the trigger stays deterministic.
        let consumer = ConsumerHandle::new();
        let tmp = tempfile::Builder::new()
            .prefix("iejoin-finalize-backstop-")
            .tempdir()
            .expect("temp dir");
        let propagate = PropagateCkSpec::Driver;

        let err = execute_block_band(BlockBandExec {
            name: "finalize_backstop",
            build_qualifier: "b",
            driver_records,
            driver_scans,
            build_records,
            build_scans,
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            residual_eval: None,
            body_eval: Some(constant_body()),
            resolver_mapping: &resolver,
            output_schema: Some(&out_schema),
            match_mode: MatchMode::All,
            on_miss: OnMiss::NullFields,
            propagate_ck: &propagate,
            ctx: &ctx,
            budget: &budget,
            consumer: &consumer,
            spill_dir: tmp.path(),
            spill_compress: false,
            strategy: ErrorStrategy::FailFast,
            options: BlockBandOptions::default(),
        })
        .expect_err("the finalize backstop must abort when the ceiling is already breached");

        match err {
            PipelineError::MemoryBudgetExceeded {
                detail,
                source,
                limit,
                ..
            } => {
                assert_eq!(source, clinker_plan::BudgetCategory::Arena);
                assert_eq!(limit, hard);
                assert!(
                    detail.as_deref().unwrap_or("").contains("finalize"),
                    "abort must come from the deferred-miss finalize backstop; got {detail:?}"
                );
            }
            other => {
                panic!("expected MemoryBudgetExceeded from the finalize backstop; got {other:?}")
            }
        }
    }

    #[test]
    fn all_unmatched_collect_streams_scan_phase_identically_across_budgets() {
        // Every driver is scan-phase unmatched; under Collect each emits one
        // empty-array row streamed back from the scan-phase buffer. The emitted
        // rows must be identical whether that pile stayed resident (roomy budget)
        // or spilled and round-tripped through the k-way merge (tight budget) —
        // the determinism-across-budgets contract, now covering the bounded
        // scan-phase path.
        let driver: Side = (0..600i64).map(|i| (None, i)).collect();
        let build: Side = vec![(Some((5, 5)), 100)];
        let make_cfg = |hard: u64| RunCfg {
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            match_mode: MatchMode::Collect,
            on_miss: OnMiss::Skip,
            block_target: 256,
            hard_limit: hard,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: None,
        };
        let tight_budget = arbitrator(32 * 1024);
        let tight = ordered_collect(
            &run_block_on(&driver, &build, &make_cfg(32 * 1024), &tight_budget)
                .expect("tight collect run"),
        );
        let roomy =
            ordered_collect(&run_block(&driver, &build, &make_cfg(1 << 30)).expect("roomy run"));
        assert_eq!(
            tight, roomy,
            "Collect scan-phase output changed between spilled and resident budgets"
        );
        assert_eq!(
            tight.len(),
            600,
            "every unmatched driver emits exactly one collect row"
        );
        assert!(
            tight.iter().all(|(_, arr)| arr.is_empty()),
            "an unmatched driver collects an empty build array"
        );
        assert!(
            tight_budget.cumulative_spill_bytes() > 0,
            "the tight budget must spill the scan-phase pile"
        );
    }

    #[test]
    fn mixed_scan_and_inblock_misses_dispatch_in_global_index_order() {
        // Interleave scan-phase misses (NULL keys) with in-block misses
        // (orderable keys that match nothing) so the deferred dispatch must merge
        // the two piles. on_miss:error must name the globally lowest-input-index
        // miss regardless of which pile holds it, identically across block
        // layouts. Index 0 is a scan-phase (NULL) miss, so it wins the merge.
        let mut driver: Side = (0..12i64).map(|i| (Some((10 * i, 10 * i)), i)).collect();
        driver[0] = (None, 0); // scan-phase miss at the lowest index
        driver[5] = (Some((100_000, 100_000)), 5); // in-block miss
        driver[9] = (None, 9); // scan-phase miss at a higher index
        let build: Side = vec![(Some((0, 500)), 100), (Some((0, 300)), 101)];
        for (target, resident) in [(1usize << 20, None), (1usize, Some(0u64))] {
            let cfg = RunCfg {
                op1: RangeOp::Ge,
                op2: Some(RangeOp::Lt),
                match_mode: MatchMode::All,
                on_miss: OnMiss::Error,
                block_target: target,
                hard_limit: 1 << 30,
                max_spill_bytes: None,
                sort_spill: None,
                resident_budget: resident,
            };
            let err = run_block(&driver, &build, &cfg)
                .expect_err("a zero-match driver must error under on_miss:error");
            match err {
                PipelineError::CombineMissingMatch { driver_row, .. } => assert_eq!(
                    driver_row, 0,
                    "the merged dispatch must name the globally lowest-input-index miss \
                     (the scan-phase miss at index 0), not the lowest in-block miss"
                ),
                other => panic!("expected CombineMissingMatch; got {other:?}"),
            }
        }
    }

    #[test]
    fn all_inblock_miss_driver_heavy_spills_under_tight_budget() {
        // The pathological all-in-block-miss shape: every driver carries an
        // orderable range key that falls outside every build range, so each enters
        // a block (it is not scan-phase unmatched) but matches nothing and routes
        // to the DEFERRED in-block on-miss pile. Under on_miss:error that whole
        // pile drains into its spillable buffer, spills its runs, then streams back
        // through the two-way merge and errors on the lowest input index. The old
        // code held this pile in a resident O(N) Vec until dispatch; the buffer
        // bounds it to disk instead. Here the scan-phase pile and the output buffer
        // are both empty, so the in-block pile IS the entire deferred-miss
        // residency — the structure that must not stay resident. (The valid-key
        // drivers also form spilled driver blocks under this budget, so the
        // cumulative spill is not attributed to the in-block buffer alone; the
        // isolating facts are that the run reaches the merge and errors on index 0,
        // and that a 900-driver deferred pile completes under a sub-100 KiB working
        // budget without a resident Vec.)
        let driver: Side = (0..900i64)
            .map(|i| (Some((1_000 + i, 1_000 + i)), i))
            .collect();
        let build: Side = vec![(Some((5, 5)), 100), (Some((6, 4)), 101)];
        let cfg = RunCfg {
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            match_mode: MatchMode::All,
            on_miss: OnMiss::Error,
            // Small blocks, all spilled, so the driver-load gate stays well under
            // the hard limit while the in-block buffer overflows its 16 KiB-floored
            // threshold and spills its runs.
            block_target: 256,
            hard_limit: 128 * 1024,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: Some(0),
        };
        let budget = arbitrator(cfg.hard_limit);
        let err = run_block_on(&driver, &build, &cfg, &budget)
            .expect_err("an all-in-block-miss input under on_miss:error must error");
        match err {
            PipelineError::CombineMissingMatch { driver_row, .. } => assert_eq!(
                driver_row, 0,
                "on_miss:error must name the lowest-input-index in-block miss"
            ),
            other => panic!("expected CombineMissingMatch; got {other:?}"),
        }
        assert!(
            budget.cumulative_spill_bytes() > 0,
            "the 900-driver deferred pile must have reached disk, not stayed resident"
        );
    }

    /// Compile a constant-marker combine body (`emit m = 1`) so an
    /// on_miss:null_fields dispatch emits one row per unmatched driver while the
    /// driver's own columns pass through `widen_record_to_schema` — letting a
    /// test read each miss row's driver `id` back in emitted order.
    fn marker_body() -> ProgramEvaluator {
        use cxl::lexer::Span;
        use cxl::parser::Parser;
        use cxl::resolve::pass::resolve_program;
        use cxl::typecheck::pass::type_check;
        use cxl::typecheck::row::Row;
        let parsed = Parser::parse("emit m = 1");
        assert!(
            parsed.errors.is_empty(),
            "parse errors: {:?}",
            parsed.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        let resolved = resolve_program(parsed.ast, &["k1", "k2", "id"], parsed.node_count)
            .unwrap_or_else(|d| panic!("resolve errors: {d:?}"));
        let row = Row::closed(IndexMap::new(), Span::new(0, 0));
        let typed = type_check(resolved, &row).unwrap_or_else(|d| panic!("type errors: {d:?}"));
        ProgramEvaluator::new(Arc::new(typed), false)
    }

    /// Drive an all-unmatched null_fields shape and return the emitted rows plus
    /// the arbitrator's cumulative spill. `sort_spill` fragments every sort buffer
    /// (both on-miss piles and the output) into runs; a tiny value fans each pile
    /// past the merge fan-in so the finalize mergers must fold down.
    fn run_null_fields_frag(
        driver: &Side,
        build: &Side,
        out_schema: &Arc<Schema>,
        sort_spill: Option<usize>,
        hard_limit: u64,
    ) -> (Vec<Record>, u64) {
        let d_schema = driver_schema();
        let b_schema = build_schema();
        let (driver_bare, driver_scans) = to_records_scans(driver, &d_schema);
        let driver_records: Vec<(Record, RecordOrder)> = driver_bare
            .into_iter()
            .enumerate()
            .map(|(i, r)| (r, i as RecordOrder))
            .collect();
        let (build_records, build_scans) = to_records_scans(build, &b_schema);

        let stable = StableEvalContext::test_default();
        let ctx = EvalContext::test_default_borrowed(&stable);
        let resolver = empty_resolver();
        let consumer = ConsumerHandle::new();
        let tmp = tempfile::Builder::new()
            .prefix("iejoin-cascade-frag-")
            .tempdir()
            .expect("temp dir");
        let propagate = PropagateCkSpec::Driver;
        let budget = arbitrator(hard_limit);

        let out = execute_block_band(BlockBandExec {
            name: "cascade_frag",
            build_qualifier: "b",
            driver_records,
            driver_scans,
            build_records,
            build_scans,
            op1: RangeOp::Le,
            op2: Some(RangeOp::Ge),
            residual_eval: None,
            body_eval: Some(marker_body()),
            resolver_mapping: &resolver,
            output_schema: Some(out_schema),
            match_mode: MatchMode::All,
            on_miss: OnMiss::NullFields,
            propagate_ck: &propagate,
            ctx: &ctx,
            budget: &budget,
            consumer: &consumer,
            spill_dir: tmp.path(),
            spill_compress: false,
            strategy: ErrorStrategy::FailFast,
            options: BlockBandOptions {
                block_target_override: Some(256),
                sort_spill_override: sort_spill,
                resident_budget_override: None,
            },
        })
        .expect("an all-unmatched null_fields shape must complete within budget");
        assert!(
            out.output_eval_failures.is_empty(),
            "the constant-marker body never defers an eval failure"
        );
        let rows: Vec<Record> = drain_sorted(out.sorted)
            .expect("drain the completed output")
            .into_iter()
            .map(|(r, _)| r)
            .collect();
        (rows, budget.cumulative_spill_bytes())
    }

    #[test]
    fn driver_heavy_both_piles_fragment_and_complete_via_cascade() {
        // The driver-heavy-unmatched payoff for the bounded-k finalize merge
        // (#868). Even-index drivers carry a NULL range key (the scan-phase pile);
        // odd-index drivers carry an orderable key outside every build range (the
        // in-block pile). Under match:all + on_miss:null_fields BOTH piles fill,
        // and at finalize the two convert to k-way mergers streamed in one
        // interleaved driver-index order. A 64-byte sort threshold fans each pile
        // into hundreds of spilled runs — far past the merge fan-in
        // (`MERGE_FAN_IN` = 64) — so opening every run at once (the pre-cascade
        // behavior) would exhaust file descriptors or overshoot memory. The cascade
        // folds each pile down to the fan-in before streaming, so the run
        // COMPLETES; its output must be byte-identical to the same run under a
        // roomy, non-spilling budget — the determinism-across-budgets contract,
        // here across the fold-down.
        const N: i64 = 600;
        let driver: Side = (0..N)
            .map(|i| {
                if i % 2 == 0 {
                    (None, i) // scan-phase pile: NULL range key, never enters a block
                } else {
                    // in-block pile: orderable, but out of every build range
                    (Some((1_000_000 + i, 1_000_000 + i)), i)
                }
            })
            .collect();
        // Build ranges every orderable driver misses on the primary axis (Le:
        // driver k1 ≤ 5 is false for the million-scale keys). No driver matches, so
        // all N route through the deferred on-miss finalize.
        let build: Side = vec![(Some((5, 5)), 100), (Some((6, 4)), 101)];
        assert!(
            oracle_pairs(&driver, &build, TOp::Le, Some(TOp::Ge)).is_empty(),
            "the fixture must be all-unmatched so every driver reaches the finalize dispatch"
        );
        // Driver columns pass through `widen_record_to_schema`, so keep `id` in the
        // output schema to read each miss row's driver identity back; `m` carries
        // the constant marker the null_fields body stamps.
        let out_schema = Arc::new(Schema::new(vec![
            "k1".into(),
            "k2".into(),
            "id".into(),
            "m".into(),
        ]));

        // Reference: a roomy budget with no spill override keeps every pile
        // resident.
        let (resident_rows, resident_spill) =
            run_null_fields_frag(&driver, &build, &out_schema, None, 1 << 30);
        // Fragmented: the tiny threshold spills each pile into hundreds of runs
        // under the same roomy hard limit, so completion is the cascade fold-down's
        // doing, not a looser budget.
        let (frag_rows, frag_spill) =
            run_null_fields_frag(&driver, &build, &out_schema, Some(64), 1 << 30);

        assert_eq!(resident_spill, 0, "the roomy reference must not spill");
        assert!(
            frag_spill > 0,
            "the fragmented run must spill both piles and the output to disk"
        );

        let ids = |rows: &[Record]| -> Vec<i64> {
            rows.iter()
                .map(|r| {
                    assert_eq!(
                        int(r.get("m")),
                        1,
                        "every emitted row must be a null_fields miss row"
                    );
                    int(r.get("id"))
                })
                .collect()
        };
        let resident_ids = ids(&resident_rows);
        let frag_ids = ids(&frag_rows);
        assert_eq!(
            resident_ids.len(),
            N as usize,
            "match:all null_fields emits exactly one row per unmatched driver"
        );
        assert_eq!(
            resident_ids,
            (0..N).collect::<Vec<_>>(),
            "misses must emit in ascending driver-input-index order"
        );
        assert_eq!(
            resident_ids, frag_ids,
            "the fold-down through the bounded-k cascade must not change the emitted \
             order or drop / duplicate any miss row"
        );
    }

    // ── deferred reservation is phase-split, not a naive per-buffer sum ───────

    #[test]
    fn fully_matching_run_reserves_one_cap_not_two_for_the_empty_inblock_pile() {
        // Every driver matches, so the in-block zero-match pile stays empty for the
        // whole run. Under a First / All run with a non-skip on_miss the pile COULD
        // fill, but this workload never routes to it, so the per-pair gate must
        // reserve only the output sort's cap — the pile's live bytes are zero — not
        // a second cap for a pile that never grows. The budget is sized into the
        // band where one reserved cap fits and two do not, so the earlier
        // unconditional two-cap reservation would abort a run that genuinely fits:
        // the regression this guards.
        let d_schema = driver_schema();
        let b_schema = build_schema();
        // A small build side every driver clears on both axes: build k1 = 0 (Ge
        // passes for any non-negative driver k1) and k2 huge (Lt passes for any
        // in-range driver k2). A large driver side makes the resident block and the
        // two-conjunct kernel aux the dominant working-set terms.
        let driver: Side = (0..600i64).map(|i| (Some((i + 1, i + 1)), i)).collect();
        let build: Side = (0..3i64)
            .map(|j| (Some((0, 1_000_000_000)), 100 + j))
            .collect();
        let op2 = Some(RangeOp::Lt);

        // The exact per-pair working set the single-block layout presents at its
        // binding (post-kernel) gate: both sides in one resident block, the
        // hoisted driver key column and `DriverRef` slice, the build-side vectors,
        // the two-conjunct kernel aux, and the materialized pair-index vector for
        // the one surviving pair (every driver-build combination matches). A
        // resident block is borrowed (no scratch) and All holds no candidates, so
        // these are the whole measured working set the gate sees.
        let (driver_records, _) = to_records_scans(&driver, &d_schema);
        let (build_records, _) = to_records_scans(&build, &b_schema);
        let baseline_blocks: u64 = driver_records
            .iter()
            .map(|r| pair_bytes::<DriverPayload>(r) as u64)
            .sum::<u64>()
            + build_records
                .iter()
                .map(|r| pair_bytes::<(i64, i64, u64)>(r) as u64)
                .sum::<u64>();
        let driver_vecs =
            (driver.len() * (range_key_width(op2) + std::mem::size_of::<DriverRef<'_>>())) as u64;
        let build_vecs = (build.len()
            * (range_key_width(op2) + std::mem::size_of::<u64>() + std::mem::size_of::<&Record>()))
            as u64;
        let aux = iejoin_numeric_state_bytes(driver.len(), build.len()) as u64;
        let pairs_bytes =
            (driver.len() * build.len() * std::mem::size_of::<(usize, usize)>()) as u64;
        let peak = baseline_blocks + driver_vecs + build_vecs + aux + pairs_bytes;

        // Land the budget in the (hard - 2cap, hard - cap] band: hard = 1.4 * peak
        // puts peak at ~0.71 * hard, and cap = soft / 4 = 0.2 * hard, so peak fits
        // with one reserved cap but not two.
        let hard = peak * 14 / 10;
        let budget = arbitrator(hard);
        let cap = sort_spill_threshold_bytes(&budget) as u64;
        assert!(
            peak + cap <= budget.hard_limit() && budget.hard_limit() < peak + 2 * cap,
            "test must sit in the one-cap-fits / two-caps-abort band: \
             peak={peak} cap={cap} hard={}",
            budget.hard_limit()
        );

        let cfg = |on_miss| RunCfg {
            op1: RangeOp::Ge,
            op2,
            match_mode: MatchMode::All,
            on_miss,
            // One block per side (the layout the peak was computed for); the sort
            // buffers spill on the real threshold, so the output axis stays bounded
            // while the resident blocks anchor the working set.
            block_target: 1 << 30,
            hard_limit: hard,
            max_spill_bytes: None,
            sort_spill: None,
            resident_budget: Some(1 << 30),
        };

        // Skip reserves one cap before and after the phase split, so it completes
        // and fixes the expected matched output.
        let skip = all_pairs(&run_block(&driver, &build, &cfg(OnMiss::Skip)).expect("skip run"));
        // Error over a fully-matching input dispatches no miss, so it must also
        // complete — but only when the per-pair gate reserves one cap, not two.
        let error =
            all_pairs(&run_block(&driver, &build, &cfg(OnMiss::Error)).expect(
                "a fully-matching Error run must not be aborted by the in-block reservation",
            ));
        assert_eq!(error, oracle_pairs(&driver, &build, TOp::Ge, Some(TOp::Lt)));
        assert_eq!(
            skip, error,
            "a fully-matching run's output must not depend on on_miss"
        );
        assert!(!error.is_empty(), "the fixture must produce matches");
    }

    #[test]
    fn fully_matching_spilled_driver_block_reserves_bounded_finalize_term_not_two_caps() {
        // The DRIVER-LOAD gate's over-reservation: it reserved the output cap PLUS
        // the in-block pile's FULL cap for every First / All + non-skip on_miss
        // run, even one that never routes a driver to that pile. This fixture is
        // exactly that shape — every driver matches, so the pile stays empty the
        // whole run — AND the driver blocks SPILL (a large resident build claims
        // the entire resident budget), so `driver_held` carries each block's
        // scratch and the gate's driver-side term is substantial. The budget sits
        // in the band where the resident baseline plus a spilled driver block plus
        // ONE output cap fits under hard, but a SECOND full cap for the empty pile
        // does not. The pile takes no pushes during the inner loop and, at
        // finalize, clones only this block's own drivers, so the correct finalize
        // term is bounded by the block footprint — not a whole cap — and a
        // fully-matching block's term collapses toward zero. The old two-cap
        // reservation aborted this run at the driver-load gate; the bounded term
        // admits it.
        let d_schema = driver_schema();
        let b_schema = build_schema();
        // Every driver clears the single matching build on the primary axis
        // (`Ge`, single-conjunct PWMJ): driver k1 = 0 ≥ build k1 = 0. The build is
        // dominated by high-key fillers whose block-pairs prune away (0 ≥ 1e9 is
        // false), so they inflate the resident baseline without adding surviving
        // pairs — which keeps the per-pair working set small so the DRIVER-LOAD
        // gate, not a per-pair gate, is the binding reservation.
        let driver: Side = (0..200i64).map(|i| (Some((0, 0)), i)).collect();
        let mut build: Side = vec![(Some((0, 0)), 900_000)];
        build.extend((0..800i64).map(|j| (Some((1_000_000_000, 0)), j)));
        let op2: Option<RangeOp> = None;

        let (driver_records, _) = to_records_scans(&driver, &d_schema);
        let (build_records, _) = to_records_scans(&build, &b_schema);

        // The whole resident build side is the baseline: the resident budget is
        // set to exactly its bytes, so every build block stays resident and every
        // driver block spills (the build drains first and claims the budget).
        let baseline: u64 = build_records
            .iter()
            .map(|r| pair_bytes::<(i64, i64, u64)>(r) as u64)
            .sum();

        // One full spilled driver block's footprint at the gate: its record
        // scratch (`resident_bytes`, charged into `driver_held` on load) plus its
        // hoisted key column and `DriverRef` slice. `block_target` is set so a full
        // block holds exactly `RECORDS_PER_BLOCK` records, matching the slicer's
        // fill rule (emit once cumulative bytes reach the target).
        const RECORDS_PER_BLOCK: u64 = 12;
        let r_d = pair_bytes::<DriverPayload>(&driver_records[0]) as u64;
        let block_target = (RECORDS_PER_BLOCK * r_d) as usize;
        let block_bytes = RECORDS_PER_BLOCK * r_d;
        let driver_vecs = RECORDS_PER_BLOCK
            * (range_key_width(op2) as u64 + std::mem::size_of::<DriverRef<'_>>() as u64);
        let driver_held = block_bytes + driver_vecs;
        // `s` is the co-resident input footprint the gate measures for a full,
        // spilled driver block: the resident baseline plus that block's held bytes.
        let s = baseline + driver_held;

        // Land hard in the band where `s + one output cap + the bounded finalize
        // term (≈ one block)` fits but `s + two caps` does not. hard = s·10/7
        // (~1.43·s) puts s at ~0.70·hard and cap = soft/4 = 0.2·hard, so the second
        // cap overshoots while one cap plus a block-sized term fits.
        let hard = s * 10 / 7;
        let budget = arbitrator(hard);
        let cap = sort_spill_threshold_bytes(&budget) as u64;
        // The tightened finalize term for a fully-matching (empty-pile) block: its
        // pile is empty, so it collapses to the block footprint, capped at the
        // pile's spill cap.
        let inblock_term = cap.min(block_bytes);
        assert!(
            block_bytes < cap,
            "the block footprint must sit below the pile cap so the finalize term \
             is a fraction of a cap: block_bytes={block_bytes} cap={cap}"
        );
        assert!(
            s + cap + inblock_term <= budget.hard_limit(),
            "the tightened driver-load reservation (one output cap + bounded \
             finalize term) must fit: s={s} cap={cap} term={inblock_term} hard={}",
            budget.hard_limit()
        );
        assert!(
            budget.hard_limit() < s + 2 * cap,
            "the old two-cap driver-load reservation must NOT fit — this is the \
             over-reservation the fix removes: s={s} cap={cap} hard={}",
            budget.hard_limit()
        );

        let cfg = |on_miss| RunCfg {
            op1: RangeOp::Ge,
            op2,
            match_mode: MatchMode::All,
            on_miss,
            block_target,
            hard_limit: hard,
            max_spill_bytes: None,
            // Keep the drain buffers in memory so each side slices cleanly into
            // blocks; the driver blocks then spill because the resident budget is
            // fully claimed by the build side.
            sort_spill: Some(1 << 30),
            resident_budget: Some(baseline),
        };

        // Skip never routes a driver to the in-block pile, so it reserves no pile
        // term at the driver-load gate under either the old or the new code — it
        // completes and fixes the expected fully-matching output.
        let skip = all_pairs(&run_block(&driver, &build, &cfg(OnMiss::Skip)).expect("skip run"));
        // Error CAN route to the in-block pile, so the old gate reserved a second
        // full cap for it up front and aborted this run at the driver-load gate.
        // With the bounded finalize term it completes — the pile stays empty
        // because every driver matches.
        let error = all_pairs(&run_block(&driver, &build, &cfg(OnMiss::Error)).expect(
            "a fully-matching Error run over a spilled driver block must not be \
                 aborted by an over-reserved in-block pile cap",
        ));
        assert_eq!(error, oracle_pairs(&driver, &build, TOp::Ge, None));
        assert_eq!(
            skip, error,
            "a fully-matching run's output must not depend on on_miss"
        );
        assert!(!error.is_empty(), "the fixture must produce matches");
    }
}
