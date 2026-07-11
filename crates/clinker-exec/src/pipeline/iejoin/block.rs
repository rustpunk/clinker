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
//! the held `First` / `collect` candidates for the current driver block. The
//! scan/sort and slice phases respond to memory pressure by SPILLING (own byte
//! threshold + disk-quota E320), never by consulting global process pressure.
//! The per-pair pre-output abort
//! ([`pre_output_budget_error`](super::pre_output_budget_error)) is a strictly
//! LOCAL last resort: it measures this pair's exact live footprint — the
//! resident baseline, the scratch for any spilled block loaded here, the
//! range-key and kernel-aux arrays, the materialized pair-index vector, and the
//! held candidates accumulated so far — directly against the hard limit, never
//! against the arbitrator's global process pressure. So a bounded-residency run
//! stays host-independent and aborts only when one pair's own footprint cannot
//! fit `hard`, not because the process floor sits above a tight budget. The
//! OUTPUT axis (`output_records`, O(N·M) worst case) is un-spillable until a
//! later phase streams it, so it keeps the every-10K poll on the arbitrator's
//! global-aware `should_abort_local`: reacting to global pressure is the
//! arbitrator's job, on that axis only.

use std::io;
use std::path::Path;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;

use clinker_record::{Record, Schema};

use cxl::eval::{EvalContext, ProgramEvaluator};

use clinker_plan::config::ErrorStrategy;
use clinker_plan::config::pipeline_node::{MatchMode, OnMiss, PropagateCkSpec};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::combine::RangeOp;

use crate::executor::combine::CombineResolverMapping;
use crate::pipeline::combine::CombineKernelOutput;
use crate::pipeline::memory::{ConsumerHandle, MemoryArbitrator};
use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
use crate::pipeline::spill::{SpillFile, SpillWriter};
use crate::pipeline::spill_merge::SortedRunMerger;

use super::{
    CollectFlush, DriverRef, EmitBatch, EmitConfig, EmitSink, Evaluators, MatchState, RecordOrder,
    RecordScan, dispatch_on_miss, emit_match_row, emit_pairs, flush_collect_row, iejoin_numeric,
    iejoin_numeric_state_bytes, pre_output_budget_error, pwmj_numeric,
};

/// Soft-limit divisor for the per-block byte target. Targeting `soft / 8` per
/// block, against a `soft / 2` resident-block budget shared by both sides
/// ([`RESIDENT_BUDGET_DIVISOR`]), lets up to ~four blocks stay resident before
/// spill kicks in, and keeps one surviving pair's working set (two blocks plus
/// the kernel's O(n) L1/L2 sort arrays) comfortably under the hard limit the
/// strictly-local pre-output gate measures against.
const BLOCK_TARGET_DIVISOR: u64 = 8;

/// Floor for the per-block byte target, so a tiny configured budget still
/// exercises the multi-block path rather than collapsing every side to one
/// resident block. Mirrors the sort-merge matching-run floor. A single record
/// larger than the target still forms a block of one — the target is soft.
const BLOCK_TARGET_FLOOR: usize = 16 * 1024;

/// Per-side external-sort spill threshold: half the soft RSS limit, floored at
/// 16 KiB so tests with tiny budgets still exercise the drain spill path.
/// Mirrors the sort-merge Phase-A threshold.
const SORT_SPILL_FLOOR: usize = 16 * 1024;

/// Soft-limit divisor for the resident-block budget shared across both sides.
/// A completed block stays in RAM until the two sides' resident blocks together
/// would exceed `soft / 2`; further blocks spill to their own files. A side (or
/// both sides) whose sliced blocks fit `soft / 2` never touches disk, restoring
/// the in-RAM path for small and mid-size inputs. Worst case a draining side's
/// sort buffer (≤ `soft / 2` before it spills) coexists with the other side's
/// resident blocks (≤ `soft / 2`), summing to `soft`, which is below the hard
/// limit (`soft` is a fraction of `hard`).
const RESIDENT_BUDGET_DIVISOR: u64 = 2;

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
    std::cmp::max(SORT_SPILL_FLOOR, (budget.soft_limit() / 2) as usize)
}

/// Resident-block budget shared across both sides, or the test-only override.
fn resident_budget_total(budget: &MemoryArbitrator) -> u64 {
    (budget.soft_limit() / RESIDENT_BUDGET_DIVISOR).max(BLOCK_TARGET_FLOOR as u64)
}

/// Tracks the resident bytes both sides' kept-in-RAM blocks hold against the
/// shared [`resident_budget_total`]. A completed block is admitted resident
/// only while the running total stays within budget; otherwise it spills.
/// Because blocks complete in slice order and the scheduler visits the earliest
/// blocks first, admitting a resident prefix and spilling the rest serves the
/// most reloads for a fixed budget.
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
/// input axis. Returns the combined rows and any deferred output-eval failures
/// for the dispatcher to route through the DLQ.
pub(super) fn execute_block_band(
    exec: BlockBandExec<'_>,
) -> Result<CombineKernelOutput, PipelineError> {
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
    let drain_ctx = DrainCtx {
        name,
        budget,
        consumer,
        spill_dir,
        spill_compress,
        block_target,
        sort_threshold,
    };
    // Shared across both sides so their resident blocks compete for one budget
    // (B1): a side, or both together, whose sliced blocks fit `soft / 2` stays
    // entirely in RAM. `Some(0)` forces every block to spill for the test path.
    let resident_total = options
        .resident_budget_override
        .unwrap_or_else(|| resident_budget_total(budget));
    let mut resident = ResidentBudget::new(resident_total);

    // Drain + slice: pull each matched side into a payload-ordered sort buffer
    // and slice the sorted stream into min/max-tagged blocks. Scan-phase
    // unmatched drivers are retained for on_miss dispatch; unmatched builds
    // are dropped (they can never match). The drain/slice mirror each resident
    // block's bytes into the consumer handle and leave that baseline in place —
    // it stays charged through the other side's drain and all of the pruning
    // and range walk, so the arbitrator's pull-mode view never under-reads this
    // operator's live footprint.
    let (driver_blocks, unmatched_driver_records) =
        drain_driver_side(driver_records, driver_scans, &drain_ctx, &mut resident)?;
    let build_blocks = drain_build_side(build_records, build_scans, &drain_ctx, &mut resident)?;

    // Baseline resident footprint the consumer holds through the schedule/emit
    // stage: the sum of both sides' kept-in-RAM blocks. Spilled blocks hold no
    // RAM until their per-pair load, charged transiently below.
    let baseline_resident = resident_bytes_of(&driver_blocks) + resident_bytes_of(&build_blocks);

    // Schedule + emit: prune block pairs, run the kernel per surviving pair, and
    // emit through the shared loop. `row_tags` records each emitted row's
    // `(driver input index, build input index)` — `(driver_idx, u64::MAX)` for
    // collect / on_miss rows — so the output can be sorted into a deterministic
    // order at the end, a pure function of the data and independent of the
    // memory-derived block layout even when a chained upstream repeats orders.
    let mut output_records: Vec<(Record, RecordOrder)> = Vec::new();
    let mut row_tags: Vec<(u64, u64)> = Vec::new();
    let mut emitted_since_check: usize = 0;
    let mut output_eval_failures = Vec::new();
    // Parallel `(order, driver_idx, build_idx)` sort key per deferred output-eval
    // failure, so the dead-letter rows re-order into the same layout-independent
    // order as the emitted rows.
    let mut failure_tags: Vec<(RecordOrder, u64, u64)> = Vec::new();
    // Zero-match drivers whose on_miss handling is deferred to after the full
    // pass — record, order, and global input index — so on_miss:error names the
    // lowest-input-index unmatched driver (not whichever range-key-ordered block
    // finalized first). Empty under on_miss:skip and Collect (every driver emits).
    let mut unmatched: Vec<(Record, RecordOrder, u64)> = Vec::new();
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
    // scan-unmatched sweep, on_miss dispatch); its borrows of the output and tag
    // buffers end at its last use, before the final deterministic sort consumes
    // them.
    let mut sink = EmitSink {
        output_records: &mut output_records,
        emitted_since_check: &mut emitted_since_check,
        output_eval_failures: &mut output_eval_failures,
        row_tags: Some(&mut row_tags),
        failure_tags: Some(&mut failure_tags),
    };

    for driver_block in &driver_blocks {
        // Transient bytes this driver block holds for its whole inner loop: the
        // scratch copy of a spilled block (a resident block is already in the
        // baseline and only borrowed) plus its range-key array.
        let dkeys_bytes = (driver_block.len * std::mem::size_of::<(i64, i64)>()) as u64;
        let driver_scratch = spilled_scratch_bytes(driver_block);
        let driver_held = driver_scratch.saturating_add(dkeys_bytes);
        // Gate the driver block's own load from metadata, symmetric with the
        // per-pair build gate below: a spilled driver block whose scratch plus
        // the resident baseline cannot fit the hard limit aborts before it is
        // materialized rather than after. A resident (borrowed) block adds zero
        // scratch and never trips this.
        let driver_peak = baseline_resident.saturating_add(driver_held);
        if driver_peak > budget.hard_limit() {
            consumer.set_bytes(0);
            return Err(pre_output_budget_error(name, driver_peak, budget.hard_limit()));
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
        let dkeys: Vec<(i64, i64)> = driver_loaded.iter().map(|(_, p)| (p.0, p.1)).collect();
        // Single-inequality (PWMJ) uses only the primary driver key column; it
        // is fixed for the whole inner loop, so build it once here instead of
        // per surviving build pair.
        let driver_pwmj_keys: Option<Vec<i64>> = match op2 {
            None => Some(dkeys.iter().map(|(a, _)| *a).collect()),
            Some(_) => None,
        };
        consumer.set_bytes(baseline_resident.saturating_add(driver_held));

        for build_block in &build_blocks {
            if !block_pair_possible(op1, op2, driver_block, build_block) {
                continue;
            }

            // Pre-output charge, from block metadata BEFORE the build side is
            // loaded: the exact live footprint this pair charges the consumer —
            // the resident baseline (kept-in-RAM blocks, counted once), this
            // pair's scratch for any spilled block loaded here (zero for a
            // borrowed resident block, already in the baseline), the two
            // range-key arrays, the kernel's O(n) sort state, and the First /
            // collect candidates held so far in this driver block. The abort is
            // a strictly LOCAL last resort: it compares that sum directly to the
            // hard limit and never consults the arbitrator's global pressure,
            // because this path answers pressure by spilling. Global pressure is
            // the arbitrator's job, on the un-spillable output axis polled inside
            // the emit loop.
            let bkeys_bytes = (build_block.len * std::mem::size_of::<(i64, i64)>()) as u64;
            let aux_kernel = iejoin_numeric_state_bytes(driver_block.len, build_block.len) as u64;
            let build_held = spilled_scratch_bytes(build_block)
                .saturating_add(bkeys_bytes)
                .saturating_add(aux_kernel);
            let pair_peak = baseline_resident
                .saturating_add(driver_held)
                .saturating_add(build_held)
                .saturating_add(state.held_bytes());
            if pair_peak > budget.hard_limit() {
                consumer.set_bytes(0);
                return Err(pre_output_budget_error(name, pair_peak, budget.hard_limit()));
            }

            let build_loaded = build_block.load("iejoin block-band build block")?;
            if build_loaded.is_empty() {
                continue;
            }
            let bkeys: Vec<(i64, i64)> = build_loaded.iter().map(|(_, p)| (p.0, p.1)).collect();
            let build_idx: Vec<u64> = build_loaded.iter().map(|(_, p)| p.2).collect();
            consumer.set_bytes(
                baseline_resident
                    .saturating_add(driver_held)
                    .saturating_add(build_held)
                    .saturating_add(state.held_bytes()),
            );

            let pairs: Vec<(usize, usize)> = match (op2, &driver_pwmj_keys) {
                (Some(op2v), _) => iejoin_numeric(&dkeys, &bkeys, op1, op2v),
                (None, Some(dl)) => {
                    let bl: Vec<i64> = bkeys.iter().map(|(a, _)| *a).collect();
                    pwmj_numeric(dl, &bl, op1)
                }
                (None, None) => unreachable!("driver_pwmj_keys is Some whenever op2 is None"),
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
            if pair_peak > budget.hard_limit() {
                consumer.set_bytes(0);
                return Err(pre_output_budget_error(name, pair_peak, budget.hard_limit()));
            }
            consumer.set_bytes(
                baseline_resident
                    .saturating_add(driver_held)
                    .saturating_add(build_held)
                    .saturating_add(state.held_bytes())
                    .saturating_add(pairs_bytes),
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
            // candidates accumulated so far.
            consumer.set_bytes(
                baseline_resident
                    .saturating_add(driver_held)
                    .saturating_add(state.held_bytes()),
            );
        }

        // This driver block's inner loop is complete: flush collect
        // accumulators / emit deferred First candidates, and gather its
        // zero-match drivers for the end-of-join on_miss dispatch.
        finalize_driver_block(
            &emit_cfg,
            &mut evals,
            &driver_loaded,
            on_miss,
            &mut state,
            &mut unmatched,
            &mut sink,
        )?;

        // The driver block's scratch, key array, and now-drained held state are
        // released; leave only the resident baseline charged.
        consumer.set_bytes(baseline_resident);
    }

    // Scan-phase unmatched drivers (NULL / non-orderable range key) never
    // entered a block. Under Collect each emits an empty-array row; under
    // First / All each joins the deferred on_miss pile.
    finalize_scan_unmatched(
        &emit_cfg,
        &unmatched_driver_records,
        on_miss,
        &mut unmatched,
        &mut sink,
    );

    // End-of-join on_miss dispatch over every zero-match driver, in ascending
    // input order: on_miss:error names the lowest-input-index driver, and
    // on_miss:null_fields emits its rows in input order. `driver_idx` breaks
    // ties when a chained upstream repeats orders, so the dispatched-driver
    // identity is deterministic. Skip / Collect leave `unmatched` empty.
    unmatched.sort_by_key(|(_, order, driver_idx)| (*order, *driver_idx));
    for (record, order, driver_idx) in &unmatched {
        dispatch_on_miss(
            &emit_cfg,
            &mut evals,
            record,
            *order,
            *driver_idx,
            on_miss,
            &mut sink,
        )?;
    }

    // Final deterministic order: sort emitted rows by
    // `(driver order, driver input index, build input index)`. `driver_idx` is
    // unique per driver, so the key is total even when a chained upstream stamps
    // duplicate orders; collect / on_miss rows carry `build_idx == u64::MAX`, so
    // they sort after that driver's match rows (a collect / miss driver has
    // none). The `sink` borrow of these buffers ends at its last use above.
    debug_assert_eq!(output_records.len(), row_tags.len());
    let mut rows: Vec<((Record, RecordOrder), (u64, u64))> =
        output_records.into_iter().zip(row_tags).collect();
    rows.sort_by_key(|((_, order), (driver_idx, build_idx))| (*order, *driver_idx, *build_idx));
    let output_records: Vec<(Record, RecordOrder)> =
        rows.into_iter().map(|(row, _)| row).collect();

    // Re-order the deferred output-eval failures into the same
    // layout-independent order, so the dead-letter output is a pure function of
    // the data too. FailFast surfaces the first-encountered eval error and
    // returns before reaching here; that identity stays visitation-ordered by
    // design — determinizing an error path would defeat fail-fast.
    debug_assert_eq!(output_eval_failures.len(), failure_tags.len());
    let mut failures: Vec<_> = output_eval_failures.into_iter().zip(failure_tags).collect();
    failures.sort_by_key(|(_, key)| *key);
    let output_eval_failures = failures.into_iter().map(|(f, _)| f).collect();

    // The resident blocks are freed as this function returns; discharge the
    // consumer now so the node-buffer admission that follows never sums a stale
    // baseline that no longer holds any RAM.
    consumer.set_bytes(0);

    Ok(CombineKernelOutput {
        records: output_records,
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

/// Driver-side drain result: the sliced driver blocks and the retained
/// scan-phase unmatched drivers (record, order, and global input index), held
/// for end-of-join on_miss dispatch.
type DriverSide = (Vec<Block<DriverPayload>>, Vec<(Record, RecordOrder, u64)>);

/// Drain the driver side: matched records go into a payload-ordered sort
/// buffer keyed on `(k1, k2, order)`; unmatched drivers are retained for
/// on_miss dispatch. Returns the sliced blocks and the retained unmatched
/// records.
fn drain_driver_side(
    driver_records: Vec<(Record, RecordOrder)>,
    driver_scans: Vec<RecordScan>,
    ctx: &DrainCtx<'_>,
    resident: &mut ResidentBudget,
) -> Result<DriverSide, PipelineError> {
    let Some((first, _)) = driver_records.first() else {
        return Ok((Vec::new(), Vec::new()));
    };
    let schema = Arc::clone(first.schema());
    let mut buf: SortBuffer<DriverPayload> = SortBuffer::new_payload_ordered(
        ctx.sort_threshold,
        Some(ctx.spill_dir.to_path_buf()),
        ctx.spill_compress,
        Arc::clone(&schema),
    );
    let mut unmatched: Vec<(Record, RecordOrder, u64)> = Vec::new();
    for (driver_idx, ((record, order), scan)) in
        driver_records.into_iter().zip(driver_scans).enumerate()
    {
        let driver_idx = driver_idx as u64;
        match scan {
            RecordScan::Unmatched => unmatched.push((record, order, driver_idx)),
            RecordScan::Matched {
                range_key: (k1, k2),
                ..
            } => push_charge_spill(&mut buf, record, (k1, k2, driver_idx, order), ctx)?,
        }
    }
    let blocks = finish_and_slice(buf, &schema, ctx, resident, &|p: &DriverPayload| (p.0, p.1))?;
    Ok((blocks, unmatched))
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
        if written > 0 && ctx.budget.record_spill_bytes(ctx.name, written) {
            return Err(PipelineError::spill_cap_exceeded(
                ctx.name,
                ctx.budget.disk_quota(),
                written,
                ctx.budget.cumulative_spill_bytes(),
            ));
        }
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
    if residue > 0 && ctx.budget.record_spill_bytes(ctx.name, residue) {
        return Err(PipelineError::spill_cap_exceeded(
            ctx.name,
            ctx.budget.disk_quota(),
            residue,
            ctx.budget.cumulative_spill_bytes(),
        ));
    }

    // `defer_release` keeps the buffer charge live across slicing for the
    // in-memory case, where the sorted vec is still resident and each admitted
    // block re-charges its own bytes (a transient over-count that never
    // under-reports). The spilled case has already written its runs to disk, so
    // its charge is dropped now — the k-way merge holds only one record per run.
    let (stream, defer_release) = match sorted {
        SortedOutput::InMemory(pairs) => {
            if pairs.is_empty() {
                ctx.consumer.sub_bytes(pre_finish);
                return Ok(Vec::new());
            }
            (SortedStream::InMemory(pairs.into_iter()), true)
        }
        SortedOutput::Spilled(files) => {
            ctx.consumer.sub_bytes(pre_finish);
            let merger = SortedRunMerger::new_payload_ordered(files, "iejoin block-band merge")?;
            (SortedStream::Spilled(merger), false)
        }
    };
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
                schema,
                ctx,
                resident,
            )?);
            cur_bytes = 0;
            bounds = Bounds::empty();
        }
    }
    if !cur.is_empty() {
        blocks.push(make_block(cur, bounds, schema, ctx, resident)?);
    }
    Ok(blocks)
}

/// Build one completed block, keeping it resident if the shared budget still
/// admits its bytes (charged into the consumer, since it holds that RAM through
/// the rest of the join) or spilling it to its own file otherwise.
fn make_block<P: Serialize + DeserializeOwned + Send + Ord>(
    pairs: Vec<(Record, P)>,
    bounds: Bounds,
    schema: &Arc<Schema>,
    ctx: &DrainCtx<'_>,
    resident: &mut ResidentBudget,
) -> Result<Block<P>, PipelineError> {
    let len = pairs.len();
    let resident_bytes = pairs.iter().map(|(r, _)| pair_bytes::<P>(r)).sum::<usize>() as u64;
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
/// silently under-charge the cap (A5).
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
    if written > 0 && ctx.budget.record_spill_bytes(ctx.name, written) {
        return Err(PipelineError::spill_cap_exceeded(
            ctx.name,
            ctx.budget.disk_quota(),
            written,
            ctx.budget.cumulative_spill_bytes(),
        ));
    }
    Ok(Block {
        storage: BlockStorage::Spilled(file),
        bounds,
        resident_bytes,
        len,
    })
}

/// Finalize one driver block after its inner loop over build blocks. Under
/// `Collect`, flush one row per driver (even zero-match drivers, with an empty
/// array). Under `First`, emit each driver's held minimum-build-index candidate
/// — the selection is a pure function of the data, not of the block layout —
/// and gather any driver with no emitting candidate. Under `All`, gather every
/// zero-match driver. Gathered drivers are appended to `unmatched` for the
/// deferred end-of-join on_miss dispatch, so on_miss:error names the
/// lowest-input-index unmatched driver rather than whichever block finalized
/// first. Each driver's global input index (`payload.2`) is carried through so
/// its rows tag deterministically.
fn finalize_driver_block(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    driver_loaded: &[(Record, DriverPayload)],
    on_miss: OnMiss,
    state: &mut MatchState,
    unmatched: &mut Vec<(Record, RecordOrder, u64)>,
    sink: &mut EmitSink<'_>,
) -> Result<(), PipelineError> {
    match cfg.match_mode {
        MatchMode::Collect => {
            for (di, (record, payload)) in driver_loaded.iter().enumerate() {
                let flush = state.take_collect(di);
                flush_collect_row(cfg, record, payload.3, payload.2, flush, sink);
            }
        }
        MatchMode::First => {
            for (di, (record, payload)) in driver_loaded.iter().enumerate() {
                match state.take_first_candidate(di) {
                    // Emit the minimum-build-index candidate. A body eval that
                    // skips it leaves the driver a miss (no other candidate is
                    // held), routed to the deferred on_miss dispatch.
                    Some((bidx, build)) => {
                        if !emit_match_row(
                            cfg, evals, record, payload.3, payload.2, &build, bidx, sink,
                        )? {
                            note_unmatched(on_miss, record, payload.3, payload.2, unmatched);
                        }
                    }
                    None => note_unmatched(on_miss, record, payload.3, payload.2, unmatched),
                }
            }
        }
        MatchMode::All => {
            for (di, (record, payload)) in driver_loaded.iter().enumerate() {
                if !state.matched[di] {
                    note_unmatched(on_miss, record, payload.3, payload.2, unmatched);
                }
            }
        }
    }
    Ok(())
}

/// Finalize the scan-phase unmatched drivers (NULL / non-orderable range key).
/// Under `Collect` each emits one empty-array row; under `First` / `All` each
/// joins the deferred on_miss pile.
fn finalize_scan_unmatched(
    cfg: &EmitConfig<'_>,
    scan_unmatched: &[(Record, RecordOrder, u64)],
    on_miss: OnMiss,
    unmatched: &mut Vec<(Record, RecordOrder, u64)>,
    sink: &mut EmitSink<'_>,
) {
    match cfg.match_mode {
        MatchMode::Collect => {
            for (record, order, driver_idx) in scan_unmatched {
                flush_collect_row(
                    cfg,
                    record,
                    *order,
                    *driver_idx,
                    CollectFlush {
                        arr: Vec::new(),
                        truncated: false,
                        first_build: None,
                    },
                    sink,
                );
            }
        }
        MatchMode::First | MatchMode::All => {
            for (record, order, driver_idx) in scan_unmatched {
                note_unmatched(on_miss, record, *order, *driver_idx, unmatched);
            }
        }
    }
}

/// Record one zero-match driver for the deferred end-of-join on_miss dispatch.
/// `Skip` needs nothing (the driver emits no row); `Error` and `NullFields`
/// need the driver held until the full pass completes, so the error names the
/// lowest input index and the null-field rows emit in input order. The driver's
/// global input index is retained for the deterministic row tag.
fn note_unmatched(
    on_miss: OnMiss,
    record: &Record,
    order: RecordOrder,
    driver_idx: u64,
    unmatched: &mut Vec<(Record, RecordOrder, u64)>,
) {
    if !matches!(on_miss, OnMiss::Skip) {
        unmatched.push((record.clone(), order, driver_idx));
    }
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
        Ok(out.records.into_iter().map(|(r, _)| r).collect())
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

    // ── T6: possible() prune truth table ─────────────────────────────────

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

    // ── T2: two-conjunct equivalence to the nested-loop oracle ───────────

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

    // ── T3: single-conjunct (PWMJ) equivalence ───────────────────────────

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

    // ── T4: match-mode × on_miss, multi-block vs single-block ────────────

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

    // ── T5: E320 spill-cap enforcement ───────────────────────────────────

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

    // ── B2: mid-size sides stay resident (no spill) ──────────────────────

    #[test]
    fn mid_size_multi_block_sides_stay_resident_without_spill() {
        // Each side is multi-block (many records past a tiny block target) yet
        // both together fit a generous resident budget, so under B1 nothing
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

    // ── F3: dense block-pair typed abort (A1) ────────────────────────────

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
                panic!("expected MemoryBudgetExceeded from the A1 pairs charge; got {other:?}")
            }
        }
    }

    // ── F2: output determinism across block layouts ──────────────────────

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

    // ── G4: duplicate driver RecordOrder stays deterministic ─────────────

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
            out.records
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
        assert_eq!(resident_layout.len(), 16, "the 4x4 match set must be complete");
    }

    // ── G1: held First candidates over budget abort with the typed error ──

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
                    detail.as_deref().unwrap_or("").contains("iejoin pre-output"),
                    "abort must come from the pre-output gate; got {detail:?}"
                );
            }
            other => {
                panic!("expected MemoryBudgetExceeded from the held-state gate; got {other:?}")
            }
        }
    }

    // ── G2: spilled driver block gated before its load ───────────────────

    #[test]
    fn spilled_driver_block_over_hard_limit_aborts_before_load() {
        // Driver keys strictly below build keys under Gt → every block-pair is
        // pruned, so no per-pair build gate ever runs. The driver side is one
        // spilled block whose scratch alone exceeds the hard limit; without a
        // driver-load gate the run would materialize it over budget and then
        // complete (everything pruned). The metadata gate must abort at the
        // driver load instead.
        let driver: Side = (0..500i64).map(|i| (Some((i % 100, 0)), i)).collect();
        let build: Side = (0..10i64).map(|i| (Some((10_000 + i, 0)), i + 1_000)).collect();
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
                    detail.as_deref().unwrap_or("").contains("iejoin pre-output"),
                    "abort must come from the driver-load gate; got {detail:?}"
                );
            }
            other => {
                panic!("expected MemoryBudgetExceeded from the driver-load gate; got {other:?}")
            }
        }
    }
}
