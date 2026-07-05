//! Grace hash join executor.
//!
//! Dynamic hybrid hash join: in-memory hash for partitions that fit,
//! disk spill for the largest partition once the soft memory limit is
//! crossed, recursive repartition for reloaded partitions that still
//! exceed the budget.
//!
//! ## Per-partition lifecycle
//!
//! ```text
//! Building   — accumulating build records in memory
//! OnDisk     — spilled to disk; probe records that hash here are also
//!              spilled (lazy probe-side spilling)
//! Ready      — hash table built, ready to probe in-memory
//! Done       — fully processed, resources released
//! ```
//!
//! ## Partitioning
//!
//! The shared [`PartitionAssigner`] uses the *upper* bits of the 64-bit
//! hash so the lower bits stay free for hashbrown's bucket placement.
//! Bucket placement remains stable across partition splits, so a
//! reloaded partition's hash table doesn't need to be re-keyed when
//! `assigner.double()` produces a finer assigner during recursive
//! repartition.
//!
//! ## Spill victim policy
//!
//! When the [`MemoryArbitrator`] reports `should_spill`, the largest
//! `Building` partition (by accumulated bytes) transitions to `OnDisk`.
//! AsterixDB's "Largest-Size" policy: maximizes bytes evicted per
//! spill cycle, minimizes how often we cross the budget threshold.
//!
//! ## Recursive repartition
//!
//! A reloaded partition that still exceeds the soft limit after its
//! hash table is built is split via `assigner.double()`. Only the
//! oversize partition is split — the rest of the dataset is unaffected.
//! Capped at 12 bits (4096 partitions); a partition that survives to
//! the cap falls through to the in-memory probe path with the budget
//! check still active.
//!
//! ## Cleanup
//!
//! The executor borrows a path inside the pipeline-scoped
//! `Arc<tempfile::TempDir>` carried on `ExecutorContext`. Each
//! committed spill file (a [`tempfile::TempPath`]) deletes itself on
//! Drop; the pipeline-scoped TempDir Drop is the secondary sweep that
//! collects files leaked by an operator panic.
//!
//! ## Submodules
//!
//! - [`build`] — partition assignment, the distinct-key sketch, and the
//!   byte-bounded build-chunk iterator.
//! - [`probe`] — per-probe match emission shared by every join path.
//! - [`spill`] — spilled-partition reload, recursive repartition, and
//!   the block-nested-loop fallback.

mod build;
mod probe;
mod spill;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::RandomState;
use clinker_record::{Record, Schema, Value};
use cxl::ast::Expr;
use cxl::eval::{EvalContext, ProgramEvaluator};
use cxl::typecheck::TypedProgram;
use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::executor::combine::{CombineResolver, CombineResolverMapping};
use crate::pipeline::combine::{
    CombineHashTable, CombineKernelOutput, CombineOutputEvalFailure, KeyExtractor,
    hash_composite_key,
};
use crate::pipeline::grace_spill::{
    GraceSpillError, GraceSpillWriter, SpillFilePath, grace_spill_error,
};
use crate::pipeline::memory::MemoryArbitrator;
#[cfg(test)]
use crate::pipeline::memory::NoOpPolicy;
use clinker_plan::BudgetCategory;
use clinker_plan::config::pipeline_node::{MatchMode, OnMiss};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::combine::DecomposedPredicate;

use build::{GraceHll, PartitionAssigner, estimated_record_bytes};
use probe::{EmitArgs, GraceEmitSink, ProbeOutcome, emit_for_probe};
use spill::{ReloadContext, SpilledPartition, process_spilled_partition};

/// Period (matches emitted) between [`MemoryArbitrator::should_abort`] polls
/// during the probe loop. Same cadence as the inline hash probe.
const MEMORY_CHECK_INTERVAL: usize = 10_000;

/// Render a composite join key into a single representative [`Value`] for
/// heavy-hitter reporting. A single-component key keeps its own value so a
/// string or integer key reports unchanged; a multi-component key renders
/// its components joined by `|` into a string, since the catalog records
/// one value per heavy hitter.
fn representative_key_value(keys: &[Value]) -> Value {
    match keys {
        [single] => single.clone(),
        _ => {
            let joined = keys
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("|");
            Value::from(joined.as_str())
        }
    }
}

/// Order-tracking sidecar carried alongside every record in the
/// executor's `node_buffers`. Mirrors the alias in `iejoin`; the grace
/// path emits matches in driver-order-then-build-walk-order.
pub(crate) type RecordOrder = u64;

// ──────────────────────────────────────────────────────────────────────────
// Partition state
// ──────────────────────────────────────────────────────────────────────────

/// One partition's state inside [`GraceHashExecutor`].
enum PartitionState {
    /// Build records accumulating in memory. `bytes_estimated` is a
    /// running sum used to pick a spill victim (Largest-Size policy).
    /// `distinct_sketch` is fed on every insert; it survives the
    /// Building → OnDisk transition so the BNL fallback can report
    /// approximate cardinality if a partition trips E310.
    Building {
        records: Vec<Record>,
        bytes_estimated: usize,
        distinct_sketch: GraceHll,
    },
    /// Build side spilled. `build_files` carries one file per
    /// spill flush of this partition (the initial bulk spill plus
    /// any per-record late arrivals once the partition is on disk).
    /// Lazy-allocated `probe_writer` captures probe records that hash
    /// to this partition; finalized into `probe_files` before reload.
    /// `hash_bits` records the assigner width at the time of writing —
    /// important for the reload path's recursive split.
    /// `distinct_sketch` carries the build-side HLL across the spill
    /// boundary so the reload path's BNL branch has a cardinality
    /// estimate without re-scanning.
    ///
    /// `probe_writer` is boxed so the enum variants stay near the same
    /// stack footprint (the LZ4 frame encoder + buffered file handle
    /// inside `GraceSpillWriter` runs ~250 bytes; boxing it keeps the
    /// `Building` and `Ready` variants from paying that overhead per
    /// partition slot).
    OnDisk {
        build_files: Vec<SpillFilePath>,
        probe_writer: Option<Box<GraceSpillWriter>>,
        probe_files: Vec<SpillFilePath>,
        build_count: u64,
        probe_count: u64,
        hash_bits: u8,
        distinct_sketch: GraceHll,
    },
    /// In-memory hash table built; ready for probe.
    Ready { hash_table: CombineHashTable },
    /// Fully processed; resources released.
    Done,
}

impl PartitionState {
    fn building_bytes(&self) -> usize {
        match self {
            PartitionState::Building {
                bytes_estimated, ..
            } => *bytes_estimated,
            _ => 0,
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Inputs to execute_combine_grace_hash
// ──────────────────────────────────────────────────────────────────────────

/// Inputs to [`execute_combine_grace_hash`]. Mirrors the [`crate::pipeline::iejoin::IEJoinExec`]
/// shape so the executor's combine arm has a uniform dispatch surface.
pub(crate) struct GraceHashExec<'a> {
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
    /// Initial partition bit width supplied by the planner.
    pub partition_bits: u8,
    /// Build-side `$ck.<field>` propagation policy. Threaded uniformly
    /// across every combine strategy and consumed by
    /// `copy_build_ck_columns` at each emit site.
    pub propagate_ck: &'a clinker_plan::config::pipeline_node::PropagateCkSpec,
    pub ctx: &'a EvalContext<'a>,
    pub budget: &'a MemoryArbitrator,
    /// Pipeline-scoped spill directory. Owned by the executor's
    /// `Arc<TempDir>` on `ExecutorContext`; this borrow lives for one
    /// combine invocation. Cleanup of individual spill files runs on
    /// `tempfile::TempPath` Drop; the pipeline-scoped TempDir Drop
    /// closes the panic-leak hole for files committed mid-combine.
    pub spill_dir: &'a Path,
    /// Whether partition spill files are LZ4-compressed. Resolved by the
    /// dispatcher from the workspace `[storage.spill] compress` knob against
    /// this combine's output-schema width and the run's batch size, so the
    /// build/probe/repartition spill files match what `--explain` reports.
    pub spill_compress: bool,
    /// Shared with the registered `GraceHashConsumer` wrapper.
    /// Passed through to `GraceHashExecutor::new` so the operator's
    /// partition bytes mirror into the arbitrator's pull-mode
    /// `current_usage` surface.
    pub consumer_handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
    /// Error strategy governing output-stage eval failures. Under
    /// `FailFast` a residual / body eval error propagates immediately;
    /// under `Continue` / `BestEffort` the failing row is deferred to the
    /// dispatcher via [`CombineKernelOutput::output_eval_failures`].
    pub strategy: clinker_plan::config::ErrorStrategy,
    /// Exec-time statistics accumulator (Plane B) and the `(node, column)`
    /// the join's build-side sketches are recorded under at completion. The
    /// kernel maintains three sketches over the build-key hashes it already
    /// computes — a planner-grade HyperLogLog (distinct count), a
    /// Misra-Gries tracker (heavy hitters), and a Bloom filter (membership)
    /// — then folds the finalized results into the catalog so a downstream
    /// node, or the run's own reporting, sees measured figures that
    /// supersede the plan-time row-count guess.
    pub stats_sink: GraceStatsSink<'a>,
}

/// Where the grace-hash join records its build-side sketch results: the
/// shared exec-time catalog plus the `(node, column)` key identifying the
/// build join key.
pub(crate) struct GraceStatsSink<'a> {
    pub catalog:
        std::sync::Arc<std::sync::Mutex<clinker_plan::plan::statistics::StatisticsCatalog>>,
    pub node: &'a str,
    pub column: &'a str,
}

// ──────────────────────────────────────────────────────────────────────────
// GraceHashExecutor
// ──────────────────────────────────────────────────────────────────────────

/// Stateful grace hash executor. Owns the partition table and the
/// memory budget; the spill directory is supplied by the caller and
/// owned outside the executor (the pipeline-scoped `Arc<TempDir>`
/// lives on `ExecutorContext` so cleanup spans the whole run rather
/// than each operator). Public surface is constructed and driven by
/// [`execute_combine_grace_hash`]; the type itself is `pub(crate)` so
/// unit tests in this module can assert on the transition lifecycle.
pub(crate) struct GraceHashExecutor {
    assigner: PartitionAssigner,
    partitions: Vec<PartitionState>,
    spill_dir: PathBuf,
    hash_state: RandomState,
    /// Combine node name, used to attribute each spill commit's bytes to
    /// the right stage in [`MemoryArbitrator::record_spill_bytes`] and to
    /// tag the E320 `SpillCapExceeded` surface raised when a commit crosses
    /// the disk quota.
    name: String,
    /// Shared with the arbitrator's `GraceHashConsumer` wrapper.
    /// `add_build_record` adds the admitted record's bytes;
    /// `spill_partition` subtracts the evicted partition's
    /// `bytes_estimated`. On-disk partitions don't count against
    /// `handle.bytes` — Velox's "reclaimable ≠ held" point.
    consumer_handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
    /// Whether partition spill files are LZ4-compressed. Resolved by the
    /// dispatcher from the workspace `[storage.spill] compress` knob against
    /// this combine's output-schema width and the run's batch size, so the
    /// on-disk format matches what `--explain` reports for the operator.
    spill_compress: bool,
}

/// Charge one finished spill file's `written` bytes against the run's
/// disk quota at the moment the file is committed, attributing them to
/// stage `name`. Returns [`GraceSpillError::CapExceeded`] when this commit
/// pushed the cumulative on-disk total past the configured cap so the
/// caller aborts here rather than after the whole build or probe phase has
/// finished writing — the per-commit enforcement the inter-partition
/// repartition path already applies. A free function rather than a method
/// so callers holding a `&mut self.partitions` iterator can charge through
/// a disjoint borrow of `self.name`.
fn charge_grace_spill(
    budget: &MemoryArbitrator,
    name: &str,
    written: u64,
) -> Result<(), GraceSpillError> {
    if budget.record_spill_bytes(name, written) {
        return Err(GraceSpillError::CapExceeded {
            attempted: written,
            cap: budget.disk_quota(),
            cumulative: budget.cumulative_spill_bytes(),
        });
    }
    Ok(())
}

impl GraceHashExecutor {
    /// Build a fresh executor sized for `partition_bits`. The caller
    /// supplies the spill directory — a path inside the pipeline-scoped
    /// `Arc<TempDir>` from `ExecutorContext::spill_root_path`. Cleanup
    /// of individual files runs on `tempfile::TempPath` Drop; the
    /// pipeline-scoped TempDir provides the secondary panic-safe sweep.
    pub(crate) fn new(
        partition_bits: u8,
        spill_dir: &Path,
        consumer_handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
        spill_compress: bool,
        name: &str,
    ) -> std::io::Result<Self> {
        let assigner = PartitionAssigner::new(partition_bits.max(1));
        let n = assigner.num_partitions();
        let mut partitions = Vec::with_capacity(n);
        for _ in 0..n {
            partitions.push(PartitionState::Building {
                records: Vec::new(),
                bytes_estimated: 0,
                distinct_sketch: GraceHll::new(),
            });
        }
        Ok(Self {
            assigner,
            partitions,
            spill_dir: spill_dir.to_path_buf(),
            hash_state: RandomState::new(),
            name: name.to_string(),
            consumer_handle,
            spill_compress,
        })
    }

    /// Path of the spill directory hosting per-partition files.
    /// Borrowed reference into the caller-owned `Arc<TempDir>`.
    pub(crate) fn spill_dir_path(&self) -> &Path {
        &self.spill_dir
    }

    /// Hash state shared by every partition. Build and probe must hash
    /// composite keys with the same state; the executor exposes it so
    /// the caller's KeyExtractor stream and the partition assigner
    /// agree.
    pub(crate) fn hash_state(&self) -> &RandomState {
        &self.hash_state
    }

    /// Insert a build record into its partition.
    ///
    /// When the partition is already `OnDisk` (a prior batch already
    /// triggered spill), the record streams directly to a fresh
    /// per-partition spill file inline; the reload phase reads every
    /// build file in sequence. After every insert, the budget is
    /// consulted; on `should_spill` the largest Building partition is
    /// evicted (Largest-Size policy).
    pub(crate) fn add_build_record(
        &mut self,
        record: Record,
        hash: u64,
        budget: &MemoryArbitrator,
    ) -> Result<(), GraceSpillError> {
        let p = self.assigner.partition_for(hash) as usize;
        let bytes = estimated_record_bytes(&record);

        // First peek at partition state without taking ownership of
        // the record. The OnDisk path needs the partition's hash_bits
        // but mustn't hold the mutable borrow across the spill writer
        // creation (which also borrows `self.spill_dir`).
        let on_disk_bits = match &self.partitions[p] {
            PartitionState::Building { .. } => None,
            PartitionState::OnDisk { hash_bits, .. } => Some(*hash_bits),
            PartitionState::Ready { .. } | PartitionState::Done => {
                // Build phase only adds to Building or OnDisk; arriving
                // in Ready/Done indicates the caller invoked
                // `finish_build` before the build stream completed.
                return Err(GraceSpillError::Io(std::io::Error::other(format!(
                    "grace hash: add_build_record on partition {p} in finished state",
                ))));
            }
        };

        match on_disk_bits {
            None => {
                if let PartitionState::Building {
                    records,
                    bytes_estimated,
                    distinct_sketch,
                } = &mut self.partitions[p]
                {
                    records.push(record);
                    *bytes_estimated += bytes;
                    distinct_sketch.add(hash);
                    // Mirror the admitted bytes into the consumer
                    // handle so the arbitrator's policy sees this
                    // partition's contribution at poll time.
                    self.consumer_handle.add_bytes(bytes as u64);
                }
            }
            Some(hash_bits) => {
                let mut w = GraceSpillWriter::new(
                    &self.spill_dir,
                    hash_bits,
                    p as u16,
                    self.spill_compress,
                )?;
                w.write_record(&record)?;
                let (new_path, written) = w.finish()?;
                if let PartitionState::OnDisk {
                    build_files,
                    build_count,
                    distinct_sketch,
                    ..
                } = &mut self.partitions[p]
                {
                    build_files.push(new_path);
                    *build_count += 1;
                    distinct_sketch.add(hash);
                }
                charge_grace_spill(budget, &self.name, written)?;
            }
        }

        if budget.should_spill() || self.consumer_handle.take_spill_request() {
            self.spill_largest_building(budget)?;
        }
        Ok(())
    }

    /// Force-spill the largest Building partition. Returns Ok(()) when
    /// no Building partition remains (everything is already on disk).
    fn spill_largest_building(&mut self, budget: &MemoryArbitrator) -> Result<(), GraceSpillError> {
        // Iterate until RSS drops below soft limit OR no Building
        // partition is left to evict. The soft limit is checked through
        // `should_spill` rather than `should_abort`: we want to catch
        // overshoots before they breach the hard limit.
        loop {
            // Scan once to find the largest Building partition.
            let mut victim: Option<(usize, usize)> = None;
            for (i, p) in self.partitions.iter().enumerate() {
                let bytes = p.building_bytes();
                if bytes > 0 && victim.map(|(_, b)| bytes > b).unwrap_or(true) {
                    victim = Some((i, bytes));
                }
            }
            let Some((idx, _)) = victim else {
                return Ok(());
            };
            self.spill_partition(idx, budget)?;
            if !budget.should_spill() {
                return Ok(());
            }
        }
    }

    /// Drain partition `idx` from Building → OnDisk by writing every
    /// in-memory record to a fresh spill file. The HLL sketch is
    /// preserved verbatim across the transition so the reload-phase
    /// BNL branch can read partition cardinality without rebuilding.
    fn spill_partition(
        &mut self,
        idx: usize,
        budget: &MemoryArbitrator,
    ) -> Result<(), GraceSpillError> {
        let assigner_bits = self.assigner.hash_bits();
        let hash_bits = assigner_bits;
        let partition_id = idx as u16;
        let prev = std::mem::replace(&mut self.partitions[idx], PartitionState::Done);
        let (records, distinct_sketch, bytes_estimated) = match prev {
            PartitionState::Building {
                records,
                distinct_sketch,
                bytes_estimated,
            } => (records, distinct_sketch, bytes_estimated),
            other => {
                // Restore and bail.
                self.partitions[idx] = other;
                return Ok(());
            }
        };
        let mut writer = GraceSpillWriter::new(
            &self.spill_dir,
            hash_bits,
            partition_id,
            self.spill_compress,
        )?;
        let count = records.len() as u64;
        for r in records {
            writer.write_record(&r)?;
        }
        let (path, written) = writer.finish()?;
        self.partitions[idx] = PartitionState::OnDisk {
            build_files: vec![path],
            probe_writer: None,
            probe_files: Vec::new(),
            build_count: count,
            probe_count: 0,
            hash_bits,
            distinct_sketch,
        };
        // Building → OnDisk: in-memory bytes are now off-process.
        // Saturating-sub keeps the counter aligned with the
        // operator's live state without wrapping if estimate drift
        // ever exceeds the running total.
        self.consumer_handle.sub_bytes(bytes_estimated as u64);
        charge_grace_spill(budget, &self.name, written)?;
        Ok(())
    }

    /// Transition every Building partition → Ready by constructing
    /// its `CombineHashTable`. After this call, only `Ready` and
    /// `OnDisk` states remain. The Building variant's HLL is dropped
    /// at this transition: in-memory partitions complete probing
    /// against the live `CombineHashTable` and never reach the BNL
    /// branch where the sketch would be consulted.
    pub(crate) fn finish_build(
        &mut self,
        extractor: &KeyExtractor,
        ctx: &EvalContext<'_>,
        budget: &MemoryArbitrator,
        combine_name: &str,
    ) -> Result<(), PipelineError> {
        for i in 0..self.partitions.len() {
            let prev = std::mem::replace(&mut self.partitions[i], PartitionState::Done);
            let new_state = match prev {
                PartitionState::Building { records, .. } => {
                    if records.is_empty() {
                        // Empty partition fast-path: still construct an
                        // empty hash table so probe lookups hit the
                        // Ready branch and emit zero matches uniformly.
                        let table =
                            CombineHashTable::build(records, extractor, ctx, budget, Some(0))
                                .map_err(|e| PipelineError::MemoryBudgetExceeded {
                                    node: combine_name.to_string(),
                                    used: budget.peak_rss().unwrap_or(0),
                                    limit: budget.hard_limit(),
                                    source: BudgetCategory::Arena,
                                    detail: Some(format!("grace hash build: {e}")),
                                })?;
                        PartitionState::Ready { hash_table: table }
                    } else {
                        let estimated = Some(records.len());
                        let table =
                            CombineHashTable::build(records, extractor, ctx, budget, estimated)
                                .map_err(|e| PipelineError::MemoryBudgetExceeded {
                                    node: combine_name.to_string(),
                                    used: budget.peak_rss().unwrap_or(0),
                                    limit: budget.hard_limit(),
                                    source: BudgetCategory::Arena,
                                    detail: Some(format!("grace hash build: {e}")),
                                })?;
                        PartitionState::Ready { hash_table: table }
                    }
                }
                other => other,
            };
            self.partitions[i] = new_state;
        }
        Ok(())
    }

    /// Probe one record. Returns matches collected from the partition's
    /// hash table when `Ready`, or routes the record to the partition's
    /// probe-side spill file when `OnDisk` and returns an empty Vec.
    ///
    /// Caller must have invoked [`Self::finish_build`] before this.
    pub(crate) fn probe_record<'a>(
        &'a mut self,
        record: &Record,
        probe_keys: &'a [Value],
        hash: u64,
    ) -> Result<ProbeOutcome<'a>, GraceSpillError> {
        let p = self.assigner.partition_for(hash) as usize;
        match &mut self.partitions[p] {
            PartitionState::Ready { hash_table } => {
                Ok(ProbeOutcome::InMemory(hash_table.probe(probe_keys)))
            }
            PartitionState::OnDisk {
                probe_writer,
                probe_files,
                probe_count,
                hash_bits,
                ..
            } => {
                if probe_writer.is_none() {
                    *probe_writer = Some(Box::new(GraceSpillWriter::new(
                        &self.spill_dir,
                        *hash_bits,
                        // Probe-side files share the partition_id but
                        // are distinguishable by adding 0x8000 — the
                        // top bit is unused by partition assignment so
                        // it serves as a probe-vs-build tag at the
                        // file-name level for diagnostic clarity.
                        (p as u16) | 0x8000,
                        self.spill_compress,
                    )?));
                }
                let w = probe_writer.as_mut().unwrap();
                w.write_record(record)?;
                *probe_count += 1;
                let _ = probe_files; // bookkeeping in the reload path
                Ok(ProbeOutcome::Spilled)
            }
            PartitionState::Building { .. } | PartitionState::Done => {
                // Building means finish_build was not called; Done
                // means a partition was already reloaded. Both are
                // executor-side bugs, not user-visible failures.
                Err(GraceSpillError::Io(std::io::Error::other(format!(
                    "grace hash probe_record on partition {p} in invalid state"
                ))))
            }
        }
    }

    /// Finalize any open probe writers so their LZ4 frames are valid
    /// before the reload phase reopens them. Build-side writers were
    /// already finalized in `spill_partition`. Each finished file is
    /// charged against the disk quota as it is committed, so a probe phase
    /// that overshoots aborts at the crossing partition rather than after
    /// every open writer has been flushed.
    pub(crate) fn finalize_probe_spills(
        &mut self,
        budget: &MemoryArbitrator,
    ) -> Result<(), GraceSpillError> {
        for state in &mut self.partitions {
            if let PartitionState::OnDisk {
                probe_writer,
                probe_files,
                ..
            } = state
                && let Some(w) = probe_writer.take()
            {
                let (path, written) = (*w).finish()?;
                probe_files.push(path);
                charge_grace_spill(budget, &self.name, written)?;
            }
        }
        Ok(())
    }

    /// Iterate spilled partitions, returning their reload payloads in
    /// partition order. Drains each as it yields. The HLL sketch
    /// transfers ownership from the partition state to the
    /// `SpilledPartition` so the reload path can fold cardinality
    /// estimates into the BNL branch's E310 diagnostic.
    pub(crate) fn drain_spilled(&mut self) -> Vec<SpilledPartition> {
        let mut out = Vec::new();
        for (idx, state) in self.partitions.iter_mut().enumerate() {
            let prev = std::mem::replace(state, PartitionState::Done);
            if let PartitionState::OnDisk {
                build_files,
                probe_files,
                build_count,
                hash_bits,
                distinct_sketch,
                ..
            } = prev
            {
                out.push(SpilledPartition {
                    partition_id: idx as u16,
                    build_files,
                    probe_files,
                    build_count,
                    hash_bits,
                    distinct_sketch,
                });
            }
        }
        out
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Public entry point: execute_combine_grace_hash
// ──────────────────────────────────────────────────────────────────────────

/// Run a combine via grace hash. Mirrors `execute_combine_iejoin` in
/// shape: the caller provides full driver and build buffers; this
/// function constructs the grace executor, partitions inputs, runs the
/// probe phase, and reloads spilled partition pairs.
///
/// Output preserves driver order across the in-memory probe phase and
/// emits reloaded matches after the in-memory matches; downstream sort
/// is the caller's responsibility (matches the IEJoin contract).
pub(crate) fn execute_combine_grace_hash(
    args: GraceHashExec<'_>,
) -> Result<CombineKernelOutput, PipelineError> {
    let GraceHashExec {
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
        spill_dir,
        spill_compress,
        consumer_handle,
        strategy,
        stats_sink,
    } = args;

    if decomposed.equalities.is_empty() {
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: "grace hash executor invoked without equality conjuncts; planner bug"
                .to_string(),
        });
    }

    // Build extractors: one side aligned to the build qualifier, the
    // other to the probe. Same alignment loop as HashBuildProbe and
    // IEJoin paths use; chain-buried qualifiers route through the
    // resolver mapping at extract time.
    let mut driver_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
    let mut build_progs: Vec<(Arc<TypedProgram>, Expr)> = Vec::new();
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
                        "equality conjunct has qualifiers ({}, {}); neither matches build \
                         qualifier {build_qualifier:?}",
                        eq.left_input, eq.right_input
                    ),
                });
            };
        driver_progs.push((driver_prog, driver_expr));
        build_progs.push((build_prog, build_expr));
    }
    let driver_extractor = KeyExtractor::new(driver_progs);
    let build_extractor = KeyExtractor::new(build_progs);

    // Determine the build-side and driver-side schemas. Each is
    // recovered from the first record on its side, falling back to
    // the output schema so the spill reader has something to attach
    // even on empty inputs.
    let build_schema: Arc<Schema> = build_records
        .first()
        .map(|r| Arc::clone(r.schema()))
        .or_else(|| output_schema.cloned())
        .unwrap_or_else(|| Arc::new(Schema::new(Vec::new())));
    let driver_schema: Arc<Schema> = driver_records
        .first()
        .map(|(r, _)| Arc::clone(r.schema()))
        .or_else(|| output_schema.cloned())
        .unwrap_or_else(|| Arc::new(Schema::new(Vec::new())));

    let mut executor = GraceHashExecutor::new(
        partition_bits,
        spill_dir,
        consumer_handle,
        spill_compress,
        name,
    )
    .map_err(|e| PipelineError::Internal {
        op: "combine",
        node: name.to_string(),
        detail: format!("grace hash spill dir bind failed: {e}"),
    })?;

    // ── Build phase ────────────────────────────────────────────────────
    //
    // Partition placement, the byte-driven spill decisions, and the
    // arbitrator mirroring inside `add_build_record` are order-sensitive
    // and must stay sequential. The expensive part — running the compiled
    // CXL build-key closures per record and hashing the
    // composite key — is independent per record, so it parallelizes
    // across the shared kernel pool. The hashed records are then fed into
    // `add_build_record` in input order, so the resulting partition table
    // and spill timing are byte-identical to a sequential build. The
    // hash seed is cloned out of the executor first; `RandomState::clone`
    // preserves the seed, so every worker hashes identically.
    let build_hash_state = executor.hash_state().clone();
    let hashed_build: Vec<(Record, u64)> = build_records
        .into_par_iter()
        .map(|record| {
            let keys =
                build_extractor
                    .extract(ctx, &record)
                    .map_err(|e| PipelineError::Compilation {
                        transform_name: name.to_string(),
                        messages: vec![format!("grace hash build key eval error: {e}")],
                    })?;
            let hash = hash_composite_key(&keys, &build_hash_state);
            Ok::<_, PipelineError>((record, hash))
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Maintain three planner-grade sketches over the build-side join keys
    // in the single build pass, reusing the composite-key hashes already
    // computed for partition placement. [`BuildKeySketches`] is
    // allocation-bounded by a fixed constant — never by build size — so it
    // is invisible to the arbitrator: the HyperLogLog is fixed-register, the
    // Misra-Gries holds at most its counter budget (and one representative
    // value per live counter, materialized only when a counter opens), and
    // the Bloom is sized UP FRONT from the build node's plan-time row-count
    // estimate (an upper bound on distinct keys, so the filter is
    // conservatively sized — FP rate ≤ target, just possibly more bits).
    // When no plan-time estimate exists the filter is skipped rather than
    // buffered. The feed runs outside the order-sensitive `add_build_record`
    // loop, so the partition table and spill timing are unchanged; the
    // per-partition diagnostic sketches stay at 64 registers.
    //
    // The build node's row count is normally the plan-time file-metadata
    // estimate; if the build source already drained and recorded its exact
    // count, that supersedes the estimate and the membership summary records
    // the sizing as exact rather than estimated.
    let build_row_count = stats_sink
        .catalog
        .lock()
        .ok()
        .and_then(|catalog| catalog.row_count_with_source(stats_sink.node));
    let bloom_sized_from_estimate = !matches!(
        build_row_count.map(|rc| rc.source),
        Some(clinker_plan::plan::statistics::StatSource::ExecSketch)
    );
    let mut build_sketches =
        crate::sketch::BuildKeySketches::new(build_row_count.map(|rc| rc.rows));
    for (record, hash) in hashed_build {
        // Re-derive the representative key value only when Misra-Gries opens
        // a counter (a bounded number of times across the whole build), so
        // the per-row cost stays a single hash and no value is retained per
        // record. The extract here repeats work the parallel hash pass did,
        // but only for the handful of keys that become tracked counters.
        build_sketches.observe(hash, || {
            build_extractor
                .extract(ctx, &record)
                .map(|keys| representative_key_value(&keys))
                .unwrap_or(Value::Null)
        });
        executor
            .add_build_record(record, hash, budget)
            .map_err(|e| grace_spill_error(e, name, "build add failed"))?;
    }

    let crate::sketch::BuildKeySketchSummary {
        distinct,
        heavy_hitters,
        bloom,
    } = build_sketches.finalize();

    if let Ok(mut catalog) = stats_sink.catalog.lock() {
        use clinker_plan::plan::statistics::{BloomSummary, StatKey};
        let key = StatKey::new(stats_sink.node, stats_sink.column);
        catalog.record_distinct(key.clone(), distinct);
        catalog.record_heavy_hitters(key.clone(), heavy_hitters);
        if let Some((bit_count, hash_count)) = bloom {
            catalog.record_bloom(
                key,
                BloomSummary {
                    bit_count,
                    hash_count,
                    sized_from_estimate: bloom_sized_from_estimate,
                },
            );
        }
    }
    executor.finish_build(&build_extractor, ctx, budget, name)?;

    // ── Probe phase ───────────────────────────────────────────────────
    let mut output_records: Vec<(Record, RecordOrder)> = Vec::new();
    // Recoverable output-stage eval failures deferred to the dispatcher.
    // Threaded by `&mut` through every emit path (in-memory, spilled
    // reload, and BNL fallback) so a failure on any path is routed
    // uniformly. Always empty under `FailFast`.
    let mut output_eval_failures: Vec<CombineOutputEvalFailure> = Vec::new();
    let mut body_evaluator = body_program.map(|bp| ProgramEvaluator::new(Arc::clone(bp), false));
    let mut probe_keys_buf: Vec<Value> = Vec::with_capacity(driver_extractor.len());
    let mut emitted_since_check = 0usize;

    let emit_args = EmitArgs {
        name,
        decomposed,
        resolver_mapping,
        output_schema,
        match_mode,
        on_miss,
        build_qualifier,
        propagate_ck,
        strategy,
    };

    for (probe_record, rn) in driver_records {
        let row_ctx = ctx.with_row(rn);
        let probe_resolver = CombineResolver::new(resolver_mapping, &probe_record, None);
        probe_keys_buf.clear();
        driver_extractor
            .extract_into(&row_ctx, &probe_resolver, &mut probe_keys_buf)
            .map_err(|e| PipelineError::Compilation {
                transform_name: name.to_string(),
                messages: vec![format!("grace hash probe key eval error: {e}")],
            })?;
        let hash = hash_composite_key(&probe_keys_buf, executor.hash_state());

        let outcome = executor
            .probe_record(&probe_record, &probe_keys_buf, hash)
            .map_err(|e| grace_spill_error(e, name, "probe failed"))?;

        match outcome {
            ProbeOutcome::InMemory(probe_iter) => {
                emit_for_probe(
                    &emit_args,
                    &probe_record,
                    rn,
                    probe_iter,
                    body_evaluator.as_mut(),
                    &row_ctx,
                    &mut GraceEmitSink {
                        records: &mut output_records,
                        failures: &mut output_eval_failures,
                    },
                )?;
            }
            ProbeOutcome::Spilled => {
                // The executor wrote the probe record to the
                // partition's probe-side spill file. The reload phase
                // will re-read it under a fresh row sequence, since
                // the original row number was consumed at write time.
                let _ = (probe_record, rn);
            }
        }
        emitted_since_check += 1;
        if emitted_since_check >= MEMORY_CHECK_INTERVAL {
            emitted_since_check = 0;
            if budget.should_abort() {
                return Err(PipelineError::MemoryBudgetExceeded {
                    node: name.to_string(),
                    used: budget.peak_rss().unwrap_or(0),
                    limit: budget.hard_limit(),
                    source: BudgetCategory::Arena,
                    detail: Some("grace hash probe RSS abort".to_string()),
                });
            }
        }
    }

    executor
        .finalize_probe_spills(budget)
        .map_err(|e| grace_spill_error(e, name, "probe finalize failed"))?;

    // ── Reload phase ──────────────────────────────────────────────────
    // Process every spilled partition pair. A reloaded partition that
    // still exceeds soft_limit after its hash table is built triggers
    // recursive repartition via PartitionAssigner::double.
    let spill_dir_path = executor.spill_dir_path().to_path_buf();
    let hash_state = executor.hash_state().clone();
    let spilled = executor.drain_spilled();
    let rc = ReloadContext {
        name,
        build_extractor: &build_extractor,
        driver_extractor: &driver_extractor,
        emit: &emit_args,
        ctx,
        build_schema: Arc::clone(&build_schema),
        driver_schema: Arc::clone(&driver_schema),
        spill_dir: &spill_dir_path,
        spill_compress,
        hash_state: &hash_state,
    };
    for sp in spilled {
        process_spilled_partition(
            &rc,
            sp,
            &mut body_evaluator,
            budget,
            &mut GraceEmitSink {
                records: &mut output_records,
                failures: &mut output_eval_failures,
            },
        )?;
    }

    // Keep the executor alive until reload finishes — its TempDir owns
    // every spill file path threaded through the reload loop.
    drop(executor);

    Ok(CombineKernelOutput {
        records: output_records,
        output_eval_failures,
    })
}

/// `MemoryConsumer` wrapper for a `GraceHashExecutor`. Holds an
/// `Arc<ConsumerHandle>` shared with the executor: live in-memory
/// `Building` partition `bytes_estimated` is summed into
/// `handle.bytes`; on-disk partitions are excluded because they're
/// no longer reclaimable (Velox "reclaimable ≠ held" point).
/// `try_spill` flips the handle's spill-request flag; the executor's
/// `add_build_record` polls and elects the largest building partition
/// to spill via `GraceSpillWriter`.
///
/// `spill_priority = 10`: grace-hash partition spill is cheaper than
/// sort (each partition writes through `GraceSpillWriter` without
/// run-merge fixup) and far cheaper than hash-aggregation rebuilds.
/// Preferred early victim alongside `node_buffers`.
/// `can_back_pressure = false`: the executor reads its driver
/// stream-to-completion before probing, so there's no upstream channel
/// to pause once the build phase has started.
pub struct GraceHashConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl GraceHashConsumer {
    pub fn new(handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl crate::pipeline::memory::MemoryConsumer for GraceHashConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        10
    }

    fn try_spill(
        &self,
        target_bytes: u64,
    ) -> Result<u64, crate::pipeline::memory::ConsumerSpillError> {
        self.handle.request_spill();
        let bytes = self.handle.bytes();
        if bytes >= target_bytes {
            Ok(bytes)
        } else {
            Err(crate::pipeline::memory::ConsumerSpillError::BelowTarget {
                target: target_bytes,
                freed: bytes,
            })
        }
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests;
