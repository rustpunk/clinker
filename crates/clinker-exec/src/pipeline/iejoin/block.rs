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
//!      primary inequality key (payload `(k1, k2, order)` for the driver,
//!      `(k1, k2)` for the build). The buffer spills its overflow to disk.
//!   2. **Slice** the sorted stream (in-memory vec or the k-way merge over the
//!      spilled runs — never collected) into contiguous blocks of at most
//!      `block_target_bytes`, tagging each with its per-axis key bounds. A side
//!      that fits one block and never spilled stays resident (the degenerate
//!      case = the old whole-input cost); otherwise every block is written to
//!      its own spill file.
//!   3. **Schedule** driver blocks as the outer loop and build blocks as the
//!      inner loop, strictly sequentially. A block-pair whose key bounds prove
//!      the predicate empty (see [`possible`]) is pruned before any load. Each
//!      surviving pair loads its two blocks, runs the numeric kernel, and emits
//!      through the shared [`emit_pairs`](super::emit_pairs) loop.
//!
//! Memory model: bounded on the input axis — at most one driver block and one
//! matching build block are resident at once, plus the kernel's O(n) sort
//! arrays for that pair. The OUTPUT axis (`output_records`) still accumulates
//! in memory and is bounded only by the every-10K poll; a later phase streams
//! it. The pre-output abort (`should_abort_local` →
//! [`pre_output_budget_error`](super::pre_output_budget_error)) is the genuine
//! last resort: a single block-pair plus kernel aux that still cannot fit.

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
    CollectFlush, DriverRef, EmitConfig, EmitSink, Evaluators, MatchState, RecordOrder, RecordScan,
    dispatch_on_miss, emit_pairs, flush_collect_row, iejoin_numeric, iejoin_numeric_state_bytes,
    pre_output_budget_error, pwmj_numeric,
};

/// Soft-limit divisor for the per-block byte target. The scheduler holds at
/// most one driver block and one matching build block resident at once, plus
/// the kernel's O(n) L1/L2 sort arrays for that pair; targeting `soft / 8` per
/// block keeps that working set (~2 blocks + aux, which for small records can
/// be several times the block bytes) comfortably under the soft limit even
/// when `should_abort_local` is measured against the hard limit.
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

/// Estimated resident byte cost of one `(record, payload)` pair — the same
/// formula the [`SortBuffer`](crate::pipeline::sort_buffer::SortBuffer)
/// charges, so block sizing and the pre-output charge track its accounting.
fn pair_bytes<P>(record: &Record) -> usize {
    std::mem::size_of::<Record>() + record.estimated_heap_size() + std::mem::size_of::<P>()
}

/// The primary and secondary inequality keys carried in a block payload, read
/// for slicing (block bounds) and pruning. Implemented for the driver payload
/// `(k1, k2, order)` and the build payload `(k1, k2)`.
trait BlockKey {
    fn k1(&self) -> i64;
    fn k2(&self) -> i64;
}

impl BlockKey for (i64, i64, RecordOrder) {
    fn k1(&self) -> i64 {
        self.0
    }
    fn k2(&self) -> i64 {
        self.1
    }
}

impl BlockKey for (i64, i64) {
    fn k1(&self) -> i64 {
        self.0
    }
    fn k2(&self) -> i64 {
        self.1
    }
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
/// per-axis key bounds, ready for the block-band prune. Either resident (the
/// degenerate single-block-per-side, no-spill case) or on disk in its own
/// spill file.
struct Block<P> {
    storage: BlockStorage<P>,
    bounds: Bounds,
    /// Estimated resident byte cost when loaded, charged against the budget
    /// before the kernel runs for a pair that includes this block.
    resident_bytes: u64,
}

enum BlockStorage<P> {
    Resident(Vec<(Record, P)>),
    /// The spill file is held for the block's lifetime so its backing temp
    /// file (deleted on `SpillFile` drop) survives every reload in the
    /// scheduler's inner loop.
    Spilled(SpillFile<P>),
}

impl<P: DeserializeOwned + Clone> Block<P> {
    /// Materialize the block's pairs. A resident block clones its held vec (a
    /// build block is reloaded once per driver-block pass); a spilled block
    /// reads its file fresh. `context` localizes a decode failure.
    fn load(&self, context: &'static str) -> Result<Vec<(Record, P)>, PipelineError> {
        match &self.storage {
            BlockStorage::Resident(pairs) => Ok(pairs.clone()),
            BlockStorage::Spilled(file) => {
                let reader = file.reader().map_err(|e| {
                    PipelineError::Io(io::Error::other(format!(
                        "{context}: block reload open failed: {e}"
                    )))
                })?;
                reader
                    .map(|r| {
                        r.map_err(|e| {
                            PipelineError::Io(io::Error::other(format!(
                                "{context}: block reload decode failed: {e}"
                            )))
                        })
                    })
                    .collect()
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
/// production dispatch always constructs the default and derives both
/// thresholds from the arbitrator soft limit.
#[derive(Default)]
pub(super) struct BlockBandOptions {
    /// Forces the per-block byte target, so a proptest can drive the
    /// multi-block path on tiny inputs. `None` derives it from the soft limit.
    pub(super) block_target_override: Option<usize>,
    /// Forces the sort-buffer spill threshold, so a test can drive the
    /// drain-spill → k-way-merge → block-reload path under a budget above RSS
    /// (which the pre-output gate needs to complete). `None` derives it from
    /// the soft limit.
    pub(super) sort_spill_override: Option<usize>,
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

    // Phase 1 + 2: drain each matched side into a payload-ordered sort buffer
    // and slice the sorted stream into min/max-tagged blocks. Scan-phase
    // unmatched drivers are retained for on_miss dispatch; unmatched builds
    // are dropped (they can never match).
    let (driver_blocks, unmatched_driver_records) =
        drain_driver_side(driver_records, driver_scans, &drain_ctx)?;
    let build_blocks = drain_build_side(build_records, build_scans, &drain_ctx)?;

    // Phase 3: schedule blocks, prune, run the kernel per surviving pair, and
    // emit through the shared loop.
    let mut output_records: Vec<(Record, RecordOrder)> = Vec::new();
    let mut emitted_since_check: usize = 0;
    let mut output_eval_failures = Vec::new();
    let emit_cfg = EmitConfig {
        name,
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

    for driver_block in &driver_blocks {
        let driver_loaded = driver_block.load("iejoin block-band driver block")?;
        if driver_loaded.is_empty() {
            continue;
        }
        // Per-driver-block match state, keyed by the driver's local index in
        // this block. Each driver lands in exactly one driver block, so the
        // `First`-mode dedup and the collect/on_miss finalization below are
        // globally correct even though the state is block-local.
        let mut state = MatchState::new(driver_loaded.len());
        let driver_slice: Vec<DriverRef<'_>> = driver_loaded
            .iter()
            .enumerate()
            .map(|(di, (record, payload))| DriverRef {
                record,
                order: payload.2,
                key: di,
            })
            .collect();
        let dkeys: Vec<(i64, i64)> = driver_loaded.iter().map(|(_, p)| (p.0, p.1)).collect();

        for build_block in &build_blocks {
            if !block_pair_possible(op1, op2, driver_block, build_block) {
                continue;
            }
            let build_loaded = build_block.load("iejoin block-band build block")?;
            if build_loaded.is_empty() {
                continue;
            }
            let bkeys: Vec<(i64, i64)> = build_loaded.iter().map(|(_, p)| *p).collect();

            // Pre-output charge: the two resident blocks plus the kernel's
            // O(n) sort arrays for this pair. This is the genuine last-resort
            // abort — a single block-pair plus aux that cannot fit.
            let aux = iejoin_numeric_state_bytes(dkeys.len(), bkeys.len()) as u64;
            let peak = driver_block
                .resident_bytes
                .saturating_add(build_block.resident_bytes)
                .saturating_add(aux);
            consumer.set_bytes(peak);
            if budget.should_abort_local(peak) {
                return Err(pre_output_budget_error(name, peak, budget.hard_limit()));
            }

            let pairs: Vec<(usize, usize)> = match op2 {
                Some(op2v) => iejoin_numeric(&dkeys, &bkeys, op1, op2v),
                None => {
                    // Single-inequality: PWMJ on the sole (primary) axis.
                    let dl: Vec<i64> = dkeys.iter().map(|(a, _)| *a).collect();
                    let bl: Vec<i64> = bkeys.iter().map(|(a, _)| *a).collect();
                    pwmj_numeric(&dl, &bl, op1)
                }
            };

            let build_slice: Vec<&Record> = build_loaded.iter().map(|(r, _)| r).collect();
            let mut sink = EmitSink {
                output_records: &mut output_records,
                emitted_since_check: &mut emitted_since_check,
                output_eval_failures: &mut output_eval_failures,
            };
            emit_pairs(
                &emit_cfg,
                &mut evals,
                &pairs,
                &driver_slice,
                &build_slice,
                &mut state,
                &mut sink,
            )?;
        }

        // This driver block's inner loop is complete: flush collect
        // accumulators / dispatch on_miss for its zero-match drivers.
        let mut sink = EmitSink {
            output_records: &mut output_records,
            emitted_since_check: &mut emitted_since_check,
            output_eval_failures: &mut output_eval_failures,
        };
        finalize_driver_block(
            &emit_cfg,
            &mut evals,
            &driver_loaded,
            build_qualifier,
            on_miss,
            state,
            &mut sink,
        )?;
    }

    // Scan-phase unmatched drivers (NULL / non-orderable range key) never
    // entered a block; handle them at the very end, exactly as the whole-input
    // path does.
    let mut sink = EmitSink {
        output_records: &mut output_records,
        emitted_since_check: &mut emitted_since_check,
        output_eval_failures: &mut output_eval_failures,
    };
    finalize_unmatched(
        &emit_cfg,
        &mut evals,
        &unmatched_driver_records,
        build_qualifier,
        on_miss,
        &mut sink,
    )?;

    Ok(CombineKernelOutput {
        records: output_records,
        output_eval_failures,
    })
}

/// Driver-side drain result: the sliced driver blocks and the retained
/// scan-phase unmatched drivers (held for end-of-join on_miss dispatch).
type DriverSide = (
    Vec<Block<(i64, i64, RecordOrder)>>,
    Vec<(Record, RecordOrder)>,
);

/// Drain the driver side: matched records go into a payload-ordered sort
/// buffer keyed on `(k1, k2, order)`; unmatched drivers are retained for
/// on_miss dispatch. Returns the sliced blocks and the retained unmatched
/// records.
fn drain_driver_side(
    driver_records: Vec<(Record, RecordOrder)>,
    driver_scans: Vec<RecordScan>,
    ctx: &DrainCtx<'_>,
) -> Result<DriverSide, PipelineError> {
    let Some((first, _)) = driver_records.first() else {
        return Ok((Vec::new(), Vec::new()));
    };
    let schema = Arc::clone(first.schema());
    let mut buf: SortBuffer<(i64, i64, RecordOrder)> = SortBuffer::new_payload_ordered(
        ctx.sort_threshold,
        Some(ctx.spill_dir.to_path_buf()),
        ctx.spill_compress,
        Arc::clone(&schema),
    );
    let mut unmatched: Vec<(Record, RecordOrder)> = Vec::new();
    for ((record, order), scan) in driver_records.into_iter().zip(driver_scans) {
        match scan {
            RecordScan::Unmatched => unmatched.push((record, order)),
            RecordScan::Matched {
                range_key: (k1, k2),
                ..
            } => push_charge_spill(&mut buf, record, (k1, k2, order), ctx)?,
        }
    }
    let blocks = finish_and_slice(buf, &schema, ctx)?;
    Ok((blocks, unmatched))
}

/// Drain the build side: matched records go into a payload-ordered sort buffer
/// keyed on `(k1, k2)`; unmatched builds are dropped (no build-side on_miss).
fn drain_build_side(
    build_records: Vec<Record>,
    build_scans: Vec<RecordScan>,
    ctx: &DrainCtx<'_>,
) -> Result<Vec<Block<(i64, i64)>>, PipelineError> {
    let Some(first) = build_records.first() else {
        return Ok(Vec::new());
    };
    let schema = Arc::clone(first.schema());
    let mut buf: SortBuffer<(i64, i64)> = SortBuffer::new_payload_ordered(
        ctx.sort_threshold,
        Some(ctx.spill_dir.to_path_buf()),
        ctx.spill_compress,
        Arc::clone(&schema),
    );
    for (record, scan) in build_records.into_iter().zip(build_scans) {
        if let RecordScan::Matched {
            range_key: (k1, k2),
            ..
        } = scan
        {
            push_charge_spill(&mut buf, record, (k1, k2), ctx)?;
        }
    }
    finish_and_slice(buf, &schema, ctx)
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

/// Finish the sort buffer and slice its sorted output into blocks. A side that
/// fits one block and never spilled stays resident (the degenerate case);
/// otherwise the sorted stream (in-memory vec or the spilled-run merge) is
/// sliced into per-block spill files.
fn finish_and_slice<P: Serialize + DeserializeOwned + Send + Ord + Clone + BlockKey>(
    buf: SortBuffer<P>,
    schema: &Arc<Schema>,
    ctx: &DrainCtx<'_>,
) -> Result<Vec<Block<P>>, PipelineError> {
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
    // The buffer's records are now off the handle (spilled, or about to move
    // into blocks). Resident blocks re-charge at load time in Phase 3.
    ctx.consumer.set_bytes(0);

    match sorted {
        SortedOutput::InMemory(pairs) => {
            if pairs.is_empty() {
                return Ok(Vec::new());
            }
            let total: usize = pairs.iter().map(|(r, _)| pair_bytes::<P>(r)).sum();
            if total <= ctx.block_target {
                // Degenerate: the whole side fits one block and the sort never
                // spilled → keep it resident, matching the old whole-input
                // cost. No block file is written.
                let mut bounds = Bounds::empty();
                for (_, p) in &pairs {
                    bounds.update(p.k1(), p.k2());
                }
                Ok(vec![Block {
                    storage: BlockStorage::Resident(pairs),
                    bounds,
                    resident_bytes: total as u64,
                }])
            } else {
                slice_and_spill(SortedStream::InMemory(pairs.into_iter()), schema, ctx)
            }
        }
        SortedOutput::Spilled(files) => {
            let merger = SortedRunMerger::new_payload_ordered(files, "iejoin block-band merge")?;
            slice_and_spill(SortedStream::Spilled(merger), schema, ctx)
        }
    }
}

/// Slice a sorted stream into contiguous `block_target`-sized blocks, writing
/// each to its own spill file (charged against the disk quota, E320). Streaming
/// over the input: at most one forming block is resident at a time, so the
/// spilled-run merge is never collected.
fn slice_and_spill<P: Serialize + DeserializeOwned + Send + Ord + Clone + BlockKey>(
    stream: SortedStream<P>,
    schema: &Arc<Schema>,
    ctx: &DrainCtx<'_>,
) -> Result<Vec<Block<P>>, PipelineError> {
    let mut blocks: Vec<Block<P>> = Vec::new();
    let mut cur: Vec<(Record, P)> = Vec::new();
    let mut cur_bytes: usize = 0;
    let mut bounds = Bounds::empty();
    for item in stream {
        let (record, payload) = item?;
        bounds.update(payload.k1(), payload.k2());
        cur_bytes += pair_bytes::<P>(&record);
        cur.push((record, payload));
        // A single record larger than the target still forms a block of one —
        // the target is soft.
        if cur_bytes >= ctx.block_target {
            blocks.push(spill_block(std::mem::take(&mut cur), bounds, schema, ctx)?);
            cur_bytes = 0;
            bounds = Bounds::empty();
        }
    }
    if !cur.is_empty() {
        blocks.push(spill_block(cur, bounds, schema, ctx)?);
    }
    Ok(blocks)
}

/// Write one block's pairs to a fresh spill file and return the on-disk
/// [`Block`], charging the written bytes against the disk quota (E320 on cap).
fn spill_block<P: Serialize>(
    pairs: Vec<(Record, P)>,
    bounds: Bounds,
    schema: &Arc<Schema>,
    ctx: &DrainCtx<'_>,
) -> Result<Block<P>, PipelineError> {
    let resident_bytes = pairs.iter().map(|(r, _)| pair_bytes::<P>(r)).sum::<usize>() as u64;
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
    let file = writer.finish().map_err(|e| {
        PipelineError::Io(io::Error::other(format!(
            "iejoin block-band block spill finish failed: {e}"
        )))
    })?;
    let written = std::fs::metadata(file.path()).map(|m| m.len()).unwrap_or(0);
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
    })
}

/// Finalize one driver block after its inner loop over build blocks: under
/// `Collect`, flush one row per driver (even zero-match drivers, with an empty
/// array); under `First` / `All`, dispatch on_miss for every zero-match driver
/// in the block.
fn finalize_driver_block(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    driver_loaded: &[(Record, (i64, i64, RecordOrder))],
    build_qualifier: &str,
    on_miss: OnMiss,
    mut state: MatchState,
    sink: &mut EmitSink<'_>,
) -> Result<(), PipelineError> {
    match cfg.match_mode {
        MatchMode::Collect => {
            for (di, (record, payload)) in driver_loaded.iter().enumerate() {
                let arr = state.collect_accum.remove(&di).unwrap_or_default();
                let truncated = state.collect_truncated.contains_key(&di);
                let first_build = state.first_collected_builds.remove(&di);
                flush_collect_row(
                    cfg,
                    record,
                    payload.2,
                    build_qualifier,
                    CollectFlush {
                        arr,
                        truncated,
                        first_build,
                    },
                    sink.output_records,
                );
            }
        }
        MatchMode::First | MatchMode::All => {
            for (di, (record, payload)) in driver_loaded.iter().enumerate() {
                if !state.matched[di] {
                    dispatch_on_miss(cfg, evals, record, payload.2, on_miss, sink)?;
                }
            }
        }
    }
    Ok(())
}

/// Finalize the scan-phase unmatched drivers: under `Collect` each emits one
/// row with an empty array; under `First` / `All` each is dispatched through
/// on_miss.
fn finalize_unmatched(
    cfg: &EmitConfig<'_>,
    evals: &mut Evaluators,
    unmatched: &[(Record, RecordOrder)],
    build_qualifier: &str,
    on_miss: OnMiss,
    sink: &mut EmitSink<'_>,
) -> Result<(), PipelineError> {
    match cfg.match_mode {
        MatchMode::Collect => {
            for (record, order) in unmatched {
                flush_collect_row(
                    cfg,
                    record,
                    *order,
                    build_qualifier,
                    CollectFlush {
                        arr: Vec::new(),
                        truncated: false,
                        first_build: None,
                    },
                    sink.output_records,
                );
            }
        }
        MatchMode::First | MatchMode::All => {
            for (record, order) in unmatched {
                dispatch_on_miss(cfg, evals, record, *order, on_miss, sink)?;
            }
        }
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
    /// then prune every block-pair so the pre-output charge (which reads
    /// process RSS and would trip against the tiny test budget) is never
    /// reached. Verifies the merge + slice + prune machinery runs end-to-end
    /// against real spilled runs and that pruning is correct (empty output),
    /// and that spill actually occurred.
    #[test]
    fn drain_spill_merge_and_slice_path_runs_and_prunes() {
        // Driver keys strictly below build keys; op1 = Gt (driver > build) is
        // impossible for every block-pair, so the schedule prunes all of them.
        let driver: Side = (0..400i64).map(|i| (Some((i % 500, 0)), i)).collect();
        let build: Side = (0..400i64)
            .map(|i| (Some((10_000 + (i % 500), 0)), i + 1_000))
            .collect();
        // Small hard limit makes the 16 KiB sort floor bind so the 400-row
        // sides spill their runs; a tiny block target forces per-block spill of
        // the merged stream too.
        let cfg = RunCfg {
            op1: RangeOp::Gt,
            op2: None,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            block_target: 256,
            hard_limit: 32 * 1024,
            max_spill_bytes: None,
            sort_spill: None,
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

    /// Force the sort buffer itself to spill (tiny `sort_spill` override) under
    /// a budget above process RSS, so the full `SortBuffer spill → k-way merge
    /// → slice → per-block spill → reload → kernel → emit` path runs with
    /// SURVIVING pairs and the emitted result matches the oracle. The tiny sort
    /// threshold decouples spilling from the budget, so the pre-output gate is
    /// never reached.
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
            // Small enough that the 16 KiB sort floor and the 128-byte block
            // target both bind; the cap forbids any spilled byte.
            hard_limit: 32 * 1024,
            max_spill_bytes: Some(1),
            sort_spill: None,
        };
        let err = run_block(&driver, &build, &cfg).expect_err("tiny spill cap must abort");
        assert!(
            matches!(err, PipelineError::SpillCapExceeded { .. }),
            "expected E320 spill-cap error, got {err:?}"
        );
    }
}
