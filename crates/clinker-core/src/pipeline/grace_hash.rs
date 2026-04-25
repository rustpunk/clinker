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
//! When the [`MemoryBudget`] reports `should_spill`, the largest
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
//! The executor owns a [`tempfile::TempDir`]. Drop removes the
//! directory on normal exit and on panic unwind.

use std::path::Path;
use std::sync::Arc;

use ahash::RandomState;
use clinker_record::{Record, Schema, Value};
use cxl::ast::Expr;
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, SkipReason};
use cxl::typecheck::TypedProgram;
use tempfile::TempDir;

use crate::config::pipeline_node::{MatchMode, OnMiss};
use crate::error::PipelineError;
use crate::executor::combine::{CombineResolver, CombineResolverMapping};
use crate::pipeline::combine::{CombineHashTable, KeyExtractor, hash_composite_key};
use crate::pipeline::grace_spill::{GraceSpillReader, GraceSpillWriter, SpillFilePath};
use crate::pipeline::memory::MemoryBudget;
use crate::plan::combine::DecomposedPredicate;

/// Cap on matches collected per driver under [`MatchMode::Collect`].
/// Mirrors the constant in `pipeline::combine` and `pipeline::iejoin` so
/// every code path truncates at the same threshold.
const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// Period (matches emitted) between [`MemoryBudget::should_abort`] polls
/// during the probe loop. Same cadence as the inline hash probe.
const MEMORY_CHECK_INTERVAL: usize = 10_000;

/// Maximum partition bit width. 12 bits = 4096 partitions; beyond this,
/// per-partition overhead (file handles, postcard headers, hashbrown
/// allocations) outweighs further skew reduction. AsterixDB and DuckDB
/// converge on the same cap.
const MAX_HASH_BITS: u8 = 12;

/// Conservative per-record byte estimate when computing whether to fire
/// GraceHash strategy. Underestimating biases toward HashBuildProbe;
/// overestimating biases toward GraceHash. 1 KiB matches the
/// production-record size we observe on enrich pipelines.
pub(crate) const GRACE_RECORD_BYTES_ESTIMATE: u64 = 1024;

/// Order-tracking sidecar carried alongside every record in the
/// executor's `node_buffers`. Mirrors the alias in `iejoin`; the grace
/// path emits matches in driver-order-then-build-walk-order.
pub(crate) type RecordOrder = u64;

// ──────────────────────────────────────────────────────────────────────────
// PartitionAssigner
// ──────────────────────────────────────────────────────────────────────────

/// Assigns 64-bit hash values to one of `2^hash_bits` partitions using
/// the *upper* bits of the hash. Independence from the lower bits
/// (which hashbrown uses for bucket placement) is the property that
/// lets a partition's hash table survive an `assigner.double()` split
/// without re-bucketing.
///
/// Capped at 12 bits (4096 partitions); larger fan-outs amortize per-
/// partition overhead poorly and trade memory savings for handle and
/// header overhead.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PartitionAssigner {
    hash_bits: u8,
    mask: u64,
    shift: u8,
}

impl PartitionAssigner {
    /// Construct an assigner for `hash_bits` partition bits, clamped to
    /// `[1, MAX_HASH_BITS]`.
    pub(crate) fn new(hash_bits: u8) -> Self {
        let hash_bits = hash_bits.clamp(1, MAX_HASH_BITS);
        let mask = if hash_bits == 64 {
            !0u64
        } else {
            (1u64 << hash_bits) - 1
        };
        let shift = 64 - hash_bits;
        Self {
            hash_bits,
            mask,
            shift,
        }
    }

    /// Map a 64-bit hash to a partition index in `[0, 2^hash_bits)`.
    #[inline]
    pub(crate) fn partition_for(&self, hash: u64) -> u16 {
        ((hash >> self.shift) & self.mask) as u16
    }

    pub(crate) fn num_partitions(&self) -> usize {
        1usize << self.hash_bits
    }

    pub(crate) fn hash_bits(&self) -> u8 {
        self.hash_bits
    }

    /// Returns the assigner with one additional bit, or `None` when at
    /// the 12-bit cap. The doubled assigner refines partition boundaries:
    /// every record that mapped to partition `p` under the parent now
    /// maps to either `2p` or `2p + 1` under the child.
    pub(crate) fn double(&self) -> Option<Self> {
        if self.hash_bits >= MAX_HASH_BITS {
            None
        } else {
            Some(Self::new(self.hash_bits + 1))
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Partition state
// ──────────────────────────────────────────────────────────────────────────

/// One partition's state inside [`GraceHashExecutor`].
enum PartitionState {
    /// Build records accumulating in memory. `bytes_estimated` is a
    /// running sum used to pick a spill victim (Largest-Size policy).
    Building {
        records: Vec<Record>,
        bytes_estimated: usize,
    },
    /// Build side spilled. `build_files` carries one file per
    /// spill flush of this partition (the initial bulk spill plus
    /// any per-record late arrivals once the partition is on disk).
    /// Lazy-allocated `probe_writer` captures probe records that hash
    /// to this partition; finalized into `probe_files` before reload.
    /// `hash_bits` records the assigner width at the time of writing —
    /// important for the reload path's recursive split.
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
    pub ctx: &'a EvalContext<'a>,
    pub budget: &'a mut MemoryBudget,
}

// ──────────────────────────────────────────────────────────────────────────
// GraceHashExecutor
// ──────────────────────────────────────────────────────────────────────────

/// Stateful grace hash executor. Owns the temp directory, the partition
/// table, and the memory budget. Public surface is constructed and
/// driven by [`execute_combine_grace_hash`]; the type itself is
/// `pub(crate)` so unit tests in this module can assert on the
/// transition lifecycle.
pub(crate) struct GraceHashExecutor {
    assigner: PartitionAssigner,
    partitions: Vec<PartitionState>,
    spill_dir: TempDir,
    hash_state: RandomState,
}

impl GraceHashExecutor {
    /// Build a fresh executor sized for `partition_bits`. Allocates a
    /// fresh temp directory; cleanup runs on Drop.
    pub(crate) fn new(partition_bits: u8) -> std::io::Result<Self> {
        let assigner = PartitionAssigner::new(partition_bits.max(1));
        let n = assigner.num_partitions();
        let mut partitions = Vec::with_capacity(n);
        for _ in 0..n {
            partitions.push(PartitionState::Building {
                records: Vec::new(),
                bytes_estimated: 0,
            });
        }
        let spill_dir = tempfile::Builder::new().prefix("grace-hash-").tempdir()?;
        Ok(Self {
            assigner,
            partitions,
            spill_dir,
            hash_state: RandomState::new(),
        })
    }

    /// Path of the temp directory hosting per-partition spill files.
    /// Exposed for cleanup-on-Drop tests.
    pub(crate) fn spill_dir_path(&self) -> &Path {
        self.spill_dir.path()
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
        budget: &mut MemoryBudget,
    ) -> std::io::Result<()> {
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
                return Err(std::io::Error::other(format!(
                    "grace hash: add_build_record on partition {p} in finished state",
                )));
            }
        };

        match on_disk_bits {
            None => {
                if let PartitionState::Building {
                    records,
                    bytes_estimated,
                } = &mut self.partitions[p]
                {
                    records.push(record);
                    *bytes_estimated += bytes;
                }
            }
            Some(hash_bits) => {
                let mut w = GraceSpillWriter::new(self.spill_dir.path(), hash_bits, p as u16)?;
                w.write_record(&record)?;
                let new_path = w.finish()?;
                if let PartitionState::OnDisk {
                    build_files,
                    build_count,
                    ..
                } = &mut self.partitions[p]
                {
                    build_files.push(new_path);
                    *build_count += 1;
                }
            }
        }

        if budget.should_spill() {
            self.spill_largest_building(budget)?;
        }
        Ok(())
    }

    /// Force-spill the largest Building partition. Returns Ok(()) when
    /// no Building partition remains (everything is already on disk).
    fn spill_largest_building(&mut self, budget: &mut MemoryBudget) -> std::io::Result<()> {
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
            self.spill_partition(idx)?;
            if !budget.should_spill() {
                return Ok(());
            }
        }
    }

    /// Drain partition `idx` from Building → OnDisk by writing every
    /// in-memory record to a fresh spill file.
    fn spill_partition(&mut self, idx: usize) -> std::io::Result<()> {
        let assigner_bits = self.assigner.hash_bits();
        let hash_bits = assigner_bits;
        let partition_id = idx as u16;
        let prev = std::mem::replace(&mut self.partitions[idx], PartitionState::Done);
        let records = match prev {
            PartitionState::Building { records, .. } => records,
            other => {
                // Restore and bail.
                self.partitions[idx] = other;
                return Ok(());
            }
        };
        let mut writer = GraceSpillWriter::new(self.spill_dir.path(), hash_bits, partition_id)?;
        let count = records.len() as u64;
        for r in records {
            writer.write_record(&r)?;
        }
        let path = writer.finish()?;
        self.partitions[idx] = PartitionState::OnDisk {
            build_files: vec![path],
            probe_writer: None,
            probe_files: Vec::new(),
            build_count: count,
            probe_count: 0,
            hash_bits,
        };
        Ok(())
    }

    /// Transition every Building partition → Ready by constructing
    /// its `CombineHashTable`. After this call, only `Ready` and
    /// `OnDisk` states remain.
    pub(crate) fn finish_build(
        &mut self,
        extractor: &KeyExtractor,
        ctx: &EvalContext<'_>,
        budget: &mut MemoryBudget,
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
                                .map_err(|e| PipelineError::Compilation {
                                    transform_name: String::new(),
                                    messages: vec![format!("E310 grace hash build: {e}")],
                                })?;
                        PartitionState::Ready { hash_table: table }
                    } else {
                        let estimated = Some(records.len());
                        let table =
                            CombineHashTable::build(records, extractor, ctx, budget, estimated)
                                .map_err(|e| PipelineError::Compilation {
                                    transform_name: String::new(),
                                    messages: vec![format!("E310 grace hash build: {e}")],
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
    ) -> std::io::Result<ProbeOutcome<'a>> {
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
                        self.spill_dir.path(),
                        *hash_bits,
                        // Probe-side files share the partition_id but
                        // are distinguishable by adding 0x8000 — the
                        // top bit is unused by partition assignment so
                        // it serves as a probe-vs-build tag at the
                        // file-name level for diagnostic clarity.
                        (p as u16) | 0x8000,
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
                Err(std::io::Error::other(format!(
                    "grace hash probe_record on partition {p} in invalid state"
                )))
            }
        }
    }

    /// Finalize any open probe writers so their LZ4 frames are valid
    /// before the reload phase reopens them. Build-side writers were
    /// already finalized in `spill_partition`.
    pub(crate) fn finalize_probe_spills(&mut self) -> std::io::Result<()> {
        for state in &mut self.partitions {
            if let PartitionState::OnDisk {
                probe_writer,
                probe_files,
                ..
            } = state
                && let Some(w) = probe_writer.take()
            {
                let path = (*w).finish()?;
                probe_files.push(path);
            }
        }
        Ok(())
    }

    /// Iterate spilled partitions, returning their reload payloads in
    /// partition order. Drains each as it yields.
    pub(crate) fn drain_spilled(&mut self) -> Vec<SpilledPartition> {
        let mut out = Vec::new();
        for (idx, state) in self.partitions.iter_mut().enumerate() {
            let prev = std::mem::replace(state, PartitionState::Done);
            if let PartitionState::OnDisk {
                build_files,
                probe_files,
                build_count,
                hash_bits,
                ..
            } = prev
            {
                out.push(SpilledPartition {
                    partition_id: idx as u16,
                    build_files,
                    probe_files,
                    build_count,
                    hash_bits,
                });
            }
        }
        out
    }
}

/// Outcome of [`GraceHashExecutor::probe_record`]. Either an in-memory
/// probe iterator (caller walks matches inline) or a marker that the
/// record was written to a probe-side spill file.
pub(crate) enum ProbeOutcome<'a> {
    InMemory(crate::pipeline::combine::ProbeIter<'a>),
    Spilled,
}

/// One spilled partition's reload payload, drained from the executor
/// after the probe phase.
pub(crate) struct SpilledPartition {
    pub partition_id: u16,
    pub build_files: Vec<SpillFilePath>,
    pub probe_files: Vec<SpillFilePath>,
    pub build_count: u64,
    pub hash_bits: u8,
}

/// Estimate one record's heap footprint plus header overhead. Used as
/// the unit input to spill-victim selection. Conservative; over-counts
/// favor earlier spills over budget overshoots.
fn estimated_record_bytes(record: &Record) -> usize {
    record.estimated_heap_size() + std::mem::size_of::<Record>()
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
) -> Result<Vec<(Record, RecordOrder)>, PipelineError> {
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
        ctx,
        budget,
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

    let mut executor =
        GraceHashExecutor::new(partition_bits).map_err(|e| PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: format!("grace hash temp dir alloc failed: {e}"),
        })?;

    // ── Build phase ────────────────────────────────────────────────────
    for record in build_records {
        let keys =
            build_extractor
                .extract(ctx, &record)
                .map_err(|e| PipelineError::Compilation {
                    transform_name: name.to_string(),
                    messages: vec![format!("grace hash build key eval error: {e}")],
                })?;
        let hash = hash_composite_key(&keys, executor.hash_state());
        executor
            .add_build_record(record, hash, budget)
            .map_err(|e| PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!("grace hash build add failed: {e}"),
            })?;
    }
    executor.finish_build(&build_extractor, ctx, budget)?;

    // ── Probe phase ───────────────────────────────────────────────────
    let mut output_records: Vec<(Record, RecordOrder)> = Vec::new();
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
    };

    for (probe_record, rn) in driver_records {
        let row_ctx = EvalContext {
            stable: ctx.stable,
            source_file: ctx.source_file,
            source_row: rn,
        };
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
            .map_err(|e| PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!("grace hash probe failed: {e}"),
            })?;

        match outcome {
            ProbeOutcome::InMemory(probe_iter) => {
                emit_for_probe(
                    &emit_args,
                    &probe_record,
                    rn,
                    probe_iter,
                    body_evaluator.as_mut(),
                    &row_ctx,
                    &mut output_records,
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
                return Err(PipelineError::Compilation {
                    transform_name: name.to_string(),
                    messages: vec![format!(
                        "E310 grace hash probe memory limit exceeded: hard limit {}",
                        budget.hard_limit()
                    )],
                });
            }
        }
    }

    executor
        .finalize_probe_spills()
        .map_err(|e| PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: format!("grace hash probe finalize failed: {e}"),
        })?;

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
        hash_state: &hash_state,
    };
    for sp in spilled {
        process_spilled_partition(&rc, sp, &mut body_evaluator, budget, &mut output_records)?;
    }

    // Keep the executor alive until reload finishes — its TempDir owns
    // every spill file path threaded through the reload loop.
    drop(executor);

    Ok(output_records)
}

/// Bundle of reload-phase context shared across recursive
/// [`process_spilled_partition`] calls. Lifetimes track the executor's
/// owned data: `build_schema` is owned (Arc-cloned at every recursive
/// step) so the spill reader can attach it to each rehydrated record.
struct ReloadContext<'a> {
    name: &'a str,
    build_extractor: &'a KeyExtractor,
    driver_extractor: &'a KeyExtractor,
    emit: &'a EmitArgs<'a>,
    ctx: &'a EvalContext<'a>,
    build_schema: Arc<Schema>,
    driver_schema: Arc<Schema>,
    spill_dir: &'a Path,
    hash_state: &'a RandomState,
}

/// Reload one spilled partition. If it fits in memory, build its hash
/// table and probe the spilled probe-side records. If it exceeds the
/// budget, repartition into 2 child partitions via `assigner.double()`
/// and recurse.
fn process_spilled_partition(
    rc: &ReloadContext<'_>,
    sp: SpilledPartition,
    body_evaluator: &mut Option<ProgramEvaluator>,
    budget: &mut MemoryBudget,
    output: &mut Vec<(Record, RecordOrder)>,
) -> Result<(), PipelineError> {
    let name = rc.name;
    let build_extractor = rc.build_extractor;
    let driver_extractor = rc.driver_extractor;
    let ctx = rc.ctx;
    let build_schema = Arc::clone(&rc.build_schema);
    let driver_schema = Arc::clone(&rc.driver_schema);
    let spill_dir = rc.spill_dir;
    let hash_state = rc.hash_state;
    // Reload build records (every build_files entry concatenated).
    // The reader's footer is cross-checked against the SpilledPartition
    // metadata so a misrouted file (e.g. probe slot vs. build slot)
    // surfaces as an error rather than a silent join miscompute.
    let mut build_records: Vec<Record> = Vec::with_capacity(sp.build_count as usize);
    for path in &sp.build_files {
        let reader = GraceSpillReader::open(path, Arc::clone(&build_schema)).map_err(|e| {
            PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!("grace hash reload open build failed: {e}"),
            }
        })?;
        let h = reader.header();
        if h.hash_bits != sp.hash_bits {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!(
                    "grace hash reload: build file hash_bits {} disagrees with SpilledPartition {}",
                    h.hash_bits, sp.hash_bits
                ),
            });
        }
        // Build-side spill files carry the raw partition id; probe-side
        // files OR the high bit on (0x8000) for diagnostic distinction.
        // A build path tagged with the high bit indicates a misrouted
        // file — surface it before any join correctness assumption is
        // violated.
        if h.partition_id & 0x8000 != 0 {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!(
                    "grace hash reload: build path file has probe-side partition tag {:#x}",
                    h.partition_id
                ),
            });
        }
        for r in reader {
            let r = r.map_err(|e| PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!("grace hash reload build read failed: {e}"),
            })?;
            build_records.push(r);
        }
    }

    // Decide: split or build?
    let parent_assigner = PartitionAssigner::new(sp.hash_bits);
    let needs_split = budget.should_spill() && parent_assigner.double().is_some();
    if needs_split && sp.build_count > 1 {
        let child_assigner = parent_assigner.double().unwrap();
        // Repartition build_records into 2 child partitions by
        // re-hashing under the wider assigner. The parent's partition
        // id is `sp.partition_id`; under the child assigner the parent
        // id equals `child_id >> 1`, so children are `2*p` and `2*p+1`.
        let parent_id = sp.partition_id as u64;
        let mut child_a: Vec<Record> = Vec::new();
        let mut child_b: Vec<Record> = Vec::new();
        for r in build_records {
            let keys =
                build_extractor
                    .extract(ctx, &r)
                    .map_err(|e| PipelineError::Compilation {
                        transform_name: name.to_string(),
                        messages: vec![format!("grace hash repartition key eval: {e}")],
                    })?;
            let h = hash_composite_key(&keys, hash_state);
            let cp = child_assigner.partition_for(h) as u64;
            if cp == parent_id * 2 {
                child_a.push(r);
            } else {
                child_b.push(r);
            }
        }
        // Repartition probe records similarly.
        let mut child_a_probe: Vec<Record> = Vec::new();
        let mut child_b_probe: Vec<Record> = Vec::new();
        for path in &sp.probe_files {
            let preader =
                GraceSpillReader::open(path, Arc::clone(&driver_schema)).map_err(|e| {
                    PipelineError::Internal {
                        op: "combine",
                        node: name.to_string(),
                        detail: format!("grace hash reload open probe failed: {e}"),
                    }
                })?;
            for r in preader {
                let r = r.map_err(|e| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!("grace hash reload probe read failed: {e}"),
                })?;
                let keys =
                    driver_extractor
                        .extract(ctx, &r)
                        .map_err(|e| PipelineError::Compilation {
                            transform_name: name.to_string(),
                            messages: vec![format!("grace hash repartition probe key eval: {e}")],
                        })?;
                let h = hash_composite_key(&keys, hash_state);
                let cp = child_assigner.partition_for(h) as u64;
                if cp == parent_id * 2 {
                    child_a_probe.push(r);
                } else {
                    child_b_probe.push(r);
                }
            }
        }
        // Re-spill each child to its own pair of files and recurse.
        for (child_id, child_build, child_probe) in [
            (parent_id * 2, child_a, child_a_probe),
            (parent_id * 2 + 1, child_b, child_b_probe),
        ] {
            let bcount = child_build.len() as u64;
            let mut bw =
                GraceSpillWriter::new(spill_dir, child_assigner.hash_bits(), child_id as u16)
                    .map_err(|e| PipelineError::Internal {
                        op: "combine",
                        node: name.to_string(),
                        detail: format!("grace hash repartition build writer: {e}"),
                    })?;
            for r in &child_build {
                bw.write_record(r).map_err(|e| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!("grace hash repartition build write: {e}"),
                })?;
            }
            let bpath = bw.finish().map_err(|e| PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!("grace hash repartition build finalize: {e}"),
            })?;
            let mut probe_files: Vec<SpillFilePath> = Vec::new();
            if !child_probe.is_empty() {
                let mut pw = GraceSpillWriter::new(
                    spill_dir,
                    child_assigner.hash_bits(),
                    (child_id as u16) | 0x8000,
                )
                .map_err(|e| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!("grace hash repartition probe writer: {e}"),
                })?;
                for r in &child_probe {
                    pw.write_record(r).map_err(|e| PipelineError::Internal {
                        op: "combine",
                        node: name.to_string(),
                        detail: format!("grace hash repartition probe write: {e}"),
                    })?;
                }
                let p = pw.finish().map_err(|e| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!("grace hash repartition probe finalize: {e}"),
                })?;
                probe_files.push(p);
            }
            let child_sp = SpilledPartition {
                partition_id: child_id as u16,
                build_files: vec![bpath],
                probe_files,
                build_count: bcount,
                hash_bits: child_assigner.hash_bits(),
            };
            process_spilled_partition(rc, child_sp, body_evaluator, budget, output)?;
        }
        return Ok(());
    }

    // Build the in-memory hash table for the reloaded partition.
    let hash_table = CombineHashTable::build(
        build_records,
        build_extractor,
        ctx,
        budget,
        Some(sp.build_count as usize),
    )
    .map_err(|e| PipelineError::Compilation {
        transform_name: name.to_string(),
        messages: vec![format!("E310 grace hash reload build: {e}")],
    })?;

    // Walk every probe-side spill file and emit matches.
    let mut probe_keys_buf: Vec<Value> = Vec::with_capacity(driver_extractor.len());
    let mut row_seq: u64 = 0;
    for path in &sp.probe_files {
        let reader = GraceSpillReader::open(path, Arc::clone(&driver_schema)).map_err(|e| {
            PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!("grace hash reload open probe failed: {e}"),
            }
        })?;
        for r in reader {
            let probe_record = r.map_err(|e| PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!("grace hash reload probe read failed: {e}"),
            })?;
            let row_ctx = EvalContext {
                stable: ctx.stable,
                source_file: ctx.source_file,
                source_row: row_seq,
            };
            let resolver = CombineResolver::new(rc.emit.resolver_mapping, &probe_record, None);
            probe_keys_buf.clear();
            driver_extractor
                .extract_into(&row_ctx, &resolver, &mut probe_keys_buf)
                .map_err(|e| PipelineError::Compilation {
                    transform_name: name.to_string(),
                    messages: vec![format!("grace hash reload probe key eval: {e}")],
                })?;
            let probe_iter = hash_table.probe(&probe_keys_buf);
            emit_for_probe(
                rc.emit,
                &probe_record,
                row_seq,
                probe_iter,
                body_evaluator.as_mut(),
                &row_ctx,
                output,
            )?;
            row_seq += 1;
        }
    }
    Ok(())
}

/// Shape-stable bundle for [`emit_for_probe`]. Bundling the per-call
/// arguments keeps the function signature under clippy's
/// too-many-arguments cap and lets call sites update one field
/// without rewriting the call site.
struct EmitArgs<'a> {
    name: &'a str,
    decomposed: &'a DecomposedPredicate,
    resolver_mapping: &'a CombineResolverMapping,
    output_schema: Option<&'a Arc<Schema>>,
    match_mode: MatchMode,
    on_miss: OnMiss,
    build_qualifier: &'a str,
}

/// Per-probe emission. Walks the probe iterator, applies the residual
/// filter, and emits records under the configured match mode and on_miss
/// policy. Mirrors the inline HashBuildProbe arm so the grace path stays
/// behavior-compatible across spill boundaries.
fn emit_for_probe<'a>(
    args: &EmitArgs<'_>,
    probe_record: &Record,
    rn: RecordOrder,
    probe_iter: crate::pipeline::combine::ProbeIter<'a>,
    body_evaluator: Option<&mut ProgramEvaluator>,
    ctx: &EvalContext<'_>,
    output: &mut Vec<(Record, RecordOrder)>,
) -> Result<(), PipelineError> {
    let EmitArgs {
        name,
        decomposed,
        resolver_mapping,
        output_schema,
        match_mode,
        on_miss,
        build_qualifier,
    } = *args;
    match match_mode {
        MatchMode::Collect => {
            let mut arr: Vec<Value> = Vec::new();
            let mut truncated = false;
            for cand in probe_iter {
                if let Some(residual) = decomposed.residual.as_ref() {
                    let resolver =
                        CombineResolver::new(resolver_mapping, probe_record, Some(cand.record));
                    let mut residual_eval = ProgramEvaluator::new(Arc::clone(residual), false);
                    match residual_eval.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Skip(_)) => continue,
                        Ok(EvalResult::Emit { .. }) => {}
                        Err(e) => return Err(PipelineError::from(e)),
                    }
                }
                if arr.len() >= COLLECT_PER_GROUP_CAP {
                    truncated = true;
                    break;
                }
                let mut m: indexmap::IndexMap<Box<str>, Value> = indexmap::IndexMap::new();
                for (fname, val) in cand.record.iter_all_fields() {
                    m.insert(fname.into(), val.clone());
                }
                arr.push(Value::Map(Box::new(m)));
            }
            if truncated {
                eprintln!(
                    "W: combine {:?} match: collect truncated at \
                     {COLLECT_PER_GROUP_CAP} matches for driver row {rn}",
                    name
                );
            }
            let mut rec = match output_schema {
                Some(s) => widen(probe_record, s),
                None => probe_record.clone(),
            };
            rec.set(build_qualifier, Value::Array(arr));
            output.push((rec, rn));
        }
        MatchMode::First | MatchMode::All => {
            let matched: Vec<Record> = {
                let mut acc: Vec<Record> = Vec::new();
                for cand in probe_iter {
                    if let Some(residual) = decomposed.residual.as_ref() {
                        let resolver =
                            CombineResolver::new(resolver_mapping, probe_record, Some(cand.record));
                        let mut residual_eval = ProgramEvaluator::new(Arc::clone(residual), false);
                        match residual_eval.eval_record::<NullStorage>(ctx, &resolver, None) {
                            Ok(EvalResult::Skip(_)) => continue,
                            Ok(EvalResult::Emit { .. }) => {}
                            Err(e) => return Err(PipelineError::from(e)),
                        }
                    }
                    acc.push(cand.record.clone());
                    if matches!(match_mode, MatchMode::First) {
                        break;
                    }
                }
                acc
            };
            if matched.is_empty() {
                match on_miss {
                    OnMiss::Skip => {}
                    OnMiss::Error => {
                        return Err(PipelineError::Compilation {
                            transform_name: name.to_string(),
                            messages: vec![format!(
                                "E310 combine on_miss: error — no matching build row for driver row {rn}"
                            )],
                        });
                    }
                    OnMiss::NullFields => {
                        let resolver = CombineResolver::new(resolver_mapping, probe_record, None);
                        let evaluator = body_evaluator.ok_or_else(|| PipelineError::Internal {
                            op: "combine",
                            node: name.to_string(),
                            detail: "grace hash on_miss: null_fields with no body program"
                                .to_string(),
                        })?;
                        match evaluator.eval_record::<NullStorage>(ctx, &resolver, None) {
                            Ok(EvalResult::Emit {
                                fields: emitted,
                                metadata,
                            }) => {
                                let mut rec = match output_schema {
                                    Some(s) => widen(probe_record, s),
                                    None => probe_record.clone(),
                                };
                                for (n, v) in emitted {
                                    rec.set(&n, v);
                                }
                                for (k, v) in metadata {
                                    let _ = rec.set_meta(&k, v);
                                }
                                output.push((rec, rn));
                            }
                            Ok(EvalResult::Skip(SkipReason::Filtered)) => {}
                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => {}
                            Err(e) => return Err(PipelineError::from(e)),
                        }
                    }
                }
            } else if let Some(evaluator) = body_evaluator {
                for m in &matched {
                    let resolver = CombineResolver::new(resolver_mapping, probe_record, Some(m));
                    match evaluator.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Emit {
                            fields: emitted,
                            metadata,
                        }) => {
                            let mut rec = match output_schema {
                                Some(s) => widen(probe_record, s),
                                None => probe_record.clone(),
                            };
                            for (n, v) in emitted {
                                rec.set(&n, v);
                            }
                            for (k, v) in metadata {
                                let _ = rec.set_meta(&k, v);
                            }
                            output.push((rec, rn));
                        }
                        Ok(EvalResult::Skip(_)) => {}
                        Err(e) => return Err(PipelineError::from(e)),
                    }
                }
            } else {
                // Body-less synthetic chain step: concatenate probe and
                // build values onto the encoded output schema. Mirrors
                // the executor's HashBuildProbe synthetic-step branch.
                let target_schema = output_schema.ok_or_else(|| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: "synthetic grace hash step has no output schema".to_string(),
                })?;
                for m in &matched {
                    let mut values: Vec<Value> = Vec::with_capacity(target_schema.column_count());
                    values.extend(probe_record.values().iter().cloned());
                    values.extend(m.values().iter().cloned());
                    if values.len() != target_schema.column_count() {
                        return Err(PipelineError::Internal {
                            op: "combine",
                            node: name.to_string(),
                            detail: format!(
                                "synthetic grace hash step produced {} values; encoded schema \
                                 has {} columns",
                                values.len(),
                                target_schema.column_count()
                            ),
                        });
                    }
                    output.push((Record::new(Arc::clone(target_schema), values), rn));
                }
            }
        }
    }
    Ok(())
}

fn widen(input: &Record, target: &Arc<Schema>) -> Record {
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

/// Placeholder `RecordStorage` for windowless expression evaluation.
/// Mirrors `pipeline::combine::NullStorage`; the grace hash path runs
/// outside any window and never queries this storage.
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

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::SchemaBuilder;
    use cxl::ast::Statement;
    use cxl::lexer::Span as CxlSpan;
    use cxl::parser::Parser;
    use cxl::resolve::pass::resolve_program;
    use cxl::typecheck::pass::type_check;
    use cxl::typecheck::row::{QualifiedField, Row};

    fn schema_with(cols: &[&str]) -> Arc<Schema> {
        let mut b = SchemaBuilder::with_capacity(cols.len());
        for c in cols {
            b = b.with_field(*c);
        }
        b.build()
    }

    fn record_for(schema: &Arc<Schema>, values: Vec<Value>) -> Record {
        Record::new(Arc::clone(schema), values)
    }

    /// Compile a single CXL key expression into the (typed_program,
    /// expression) pair that `KeyExtractor::new` consumes. Mirrors
    /// `pipeline::combine::tests::compile_key`.
    fn compile_key(
        src: &str,
        fields: &[&str],
        row_fields: &[(&str, cxl::typecheck::Type)],
    ) -> (Arc<TypedProgram>, cxl::ast::Expr) {
        let parsed = Parser::parse(src);
        assert!(parsed.errors.is_empty(), "parse: {:?}", parsed.errors);
        let resolved = resolve_program(parsed.ast, fields, parsed.node_count)
            .unwrap_or_else(|d| panic!("resolve: {d:?}"));
        let mut cols: indexmap::IndexMap<QualifiedField, cxl::typecheck::Type> =
            indexmap::IndexMap::new();
        for (n, t) in row_fields {
            cols.insert(QualifiedField::bare(*n), t.clone());
        }
        let row = Row::closed(cols, CxlSpan::new(0, 0));
        let typed = type_check(resolved, &row).unwrap_or_else(|d| panic!("typecheck: {d:?}"));
        let expr = match &typed.program.statements[0] {
            Statement::Emit { expr, .. } => expr.clone(),
            _ => panic!("expected emit stmt"),
        };
        (Arc::new(typed), expr)
    }

    /// Budget calibrated to fire `should_spill` continuously (so the
    /// largest-Building eviction loop takes effect) without firing
    /// `should_abort` (which would short-circuit the build phase).
    /// `limit` is 10 GiB so RSS-vs-hard-limit always falls inside;
    /// `spill_threshold_pct` is set so soft limit = 1 KiB, well below
    /// any host's resident set.
    fn tiny_budget() -> MemoryBudget {
        MemoryBudget::new(10 * 1024 * 1024 * 1024, 0.000_001)
    }

    #[test]
    fn partition_assigner_alignment() {
        let a = PartitionAssigner::new(4);
        // Same hash → same partition (deterministic).
        for h in [0u64, 1, 0xDEAD_BEEF, !0] {
            assert_eq!(a.partition_for(h), a.partition_for(h));
        }
        assert_eq!(a.num_partitions(), 16);
        assert_eq!(a.hash_bits(), 4);
    }

    #[test]
    fn partition_assigner_double_refines_uniformly() {
        // Doubling the partition count refines: every parent partition
        // splits into 2*p and 2*p + 1.
        let parent = PartitionAssigner::new(4);
        let child = parent.double().unwrap();
        assert_eq!(child.hash_bits(), 5);
        for h in (0..1024u64).map(|i| i.wrapping_mul(0x9E37_79B9_7F4A_7C15)) {
            let pp = parent.partition_for(h) as u32;
            let cp = child.partition_for(h) as u32;
            assert!(
                cp == pp * 2 || cp == pp * 2 + 1,
                "child partition {cp} must be one of {{{}, {}}} for parent {pp}",
                pp * 2,
                pp * 2 + 1
            );
        }
    }

    #[test]
    fn partition_assigner_caps_at_max_bits() {
        let mut a = PartitionAssigner::new(MAX_HASH_BITS);
        assert!(a.double().is_none());
        a = PartitionAssigner::new(20); // clamped down
        assert_eq!(a.hash_bits(), MAX_HASH_BITS);
    }

    #[test]
    fn temp_dir_cleaned_on_drop() {
        let exec = GraceHashExecutor::new(4).unwrap();
        let path = exec.spill_dir_path().to_path_buf();
        assert!(path.exists());
        drop(exec);
        assert!(!path.exists(), "spill dir must be removed on Drop");
    }

    #[test]
    fn temp_dir_cleaned_on_panic() {
        // Capture the path via a wrapper; on panic the TempDir's Drop
        // still runs during unwinding.
        let captured: std::sync::Mutex<Option<std::path::PathBuf>> = std::sync::Mutex::new(None);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let exec = GraceHashExecutor::new(4).unwrap();
            *captured.lock().unwrap() = Some(exec.spill_dir_path().to_path_buf());
            panic!("simulated executor panic");
        }));
        assert!(result.is_err(), "panic must propagate out");
        let path = captured.lock().unwrap().clone().unwrap();
        assert!(!path.exists(), "spill dir must be removed on panic unwind");
    }

    /// Add records into a low-budget executor and assert that at least
    /// one partition transitions to `OnDisk` before `finish_build` runs.
    #[test]
    fn spill_activates_under_tiny_budget() {
        let schema = schema_with(&["k", "v"]);
        let mut exec = GraceHashExecutor::new(4).unwrap();
        let mut budget = tiny_budget();
        for i in 0..256i64 {
            let rec = record_for(
                &schema,
                vec![Value::Integer(i), Value::String(format!("row-{i}").into())],
            );
            // Synthetic hash: distribute uniformly across 16 partitions.
            let hash = (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            exec.add_build_record(rec, hash, &mut budget).unwrap();
        }
        let on_disk = exec
            .partitions
            .iter()
            .filter(|p| matches!(p, PartitionState::OnDisk { .. }))
            .count();
        assert!(
            on_disk >= 1,
            "expected ≥1 partition spilled under tiny budget; got {on_disk}"
        );
    }

    /// Lazy probe spill: a partition pre-spilled during build receives a
    /// probe record and writes it to its probe-side spill file rather
    /// than dropping it.
    #[test]
    fn lazy_probe_spill_routes_to_partition_file() {
        let schema = schema_with(&["k"]);
        let mut exec = GraceHashExecutor::new(4).unwrap();
        let mut budget = tiny_budget();

        // Send 64 records all to partition 0 (top 4 bits = 0). Use a
        // hash with high-bits zero.
        let probe_partition_hash: u64 = 0x0000_0000_0000_1234;
        for i in 0..64i64 {
            let rec = record_for(&schema, vec![Value::Integer(i)]);
            exec.add_build_record(rec, probe_partition_hash, &mut budget)
                .unwrap();
        }
        // Force spill of partition 0.
        exec.spill_largest_building(&mut budget).unwrap();
        let p0_disk = matches!(&exec.partitions[0], PartitionState::OnDisk { .. });
        assert!(p0_disk, "partition 0 must be on disk after force-spill");

        // Probe a record into partition 0; it should write to the
        // partition's probe-side file.
        let probe = record_for(&schema, vec![Value::Integer(999)]);
        let outcome = exec
            .probe_record(&probe, &[Value::Integer(999)], probe_partition_hash)
            .unwrap();
        assert!(matches!(outcome, ProbeOutcome::Spilled));

        exec.finalize_probe_spills().unwrap();
        match &exec.partitions[0] {
            PartitionState::OnDisk {
                probe_files,
                probe_count,
                ..
            } => {
                assert_eq!(probe_files.len(), 1);
                assert_eq!(*probe_count, 1);
                assert!(probe_files[0].exists());
            }
            _ => panic!("partition 0 must remain OnDisk after probe spill"),
        }
    }

    /// End-to-end correctness via `execute_combine_grace_hash` with a
    /// hand-built `DecomposedPredicate` and `KeyExtractor` aligned to a
    /// pure-equi join `orders.k == products.k`. The output set must
    /// match the cross-product filtered by the predicate, regardless
    /// of which partitions get spilled.
    #[test]
    fn execute_grace_hash_partition_pair_correct() {
        use crate::executor::combine::{CombineResolverMapping, JoinSide};
        use crate::plan::combine::{DecomposedPredicate, EqualityConjunct};
        use cxl::eval::{EvalContext, StableEvalContext};

        // Driver and build schemas use distinct bare names for the
        // join key (`dk` and `bk`) so the bare-name CombineResolver
        // mapping is unambiguous.
        let driver_schema = schema_with(&["dk", "v"]);
        let build_schema = schema_with(&["bk", "name"]);

        let drivers: Vec<(Record, RecordOrder)> = (0..10i64)
            .map(|i| {
                (
                    Record::new(
                        Arc::clone(&driver_schema),
                        vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
                    ),
                    i as u64,
                )
            })
            .collect();
        let builds: Vec<Record> = (0..10i64)
            .map(|i| {
                Record::new(
                    Arc::clone(&build_schema),
                    vec![Value::Integer(i), Value::String(format!("b-{i}").into())],
                )
            })
            .collect();

        // Compose typed programs for left (driver) and right (build)
        // key expressions. Each side runs against its own row.
        let (left_tp, left_expr) =
            compile_key("emit k = dk", &["dk"], &[("dk", cxl::typecheck::Type::Int)]);
        let (right_tp, right_expr) =
            compile_key("emit k = bk", &["bk"], &[("bk", cxl::typecheck::Type::Int)]);

        let decomposed = DecomposedPredicate {
            equalities: vec![EqualityConjunct {
                left_expr,
                left_input: Arc::from("orders"),
                left_program: left_tp,
                right_expr,
                right_input: Arc::from("products"),
                right_program: right_tp,
            }],
            ranges: Vec::new(),
            residual: None,
        };

        // Resolver mapping: orders.dk → driver col 0, orders.v →
        // driver col 1, products.bk → build col 0, products.name →
        // build col 1. Bare names `dk`, `v`, `bk`, `name` are
        // unambiguous because each appears on exactly one side.
        let mut mapping_q: std::collections::HashMap<
            crate::plan::row_type::QualifiedField,
            (JoinSide, u32),
        > = std::collections::HashMap::new();
        mapping_q.insert(
            crate::plan::row_type::QualifiedField::qualified("orders", "dk"),
            (JoinSide::Probe, 0),
        );
        mapping_q.insert(
            crate::plan::row_type::QualifiedField::qualified("orders", "v"),
            (JoinSide::Probe, 1),
        );
        mapping_q.insert(
            crate::plan::row_type::QualifiedField::qualified("products", "bk"),
            (JoinSide::Build, 0),
        );
        mapping_q.insert(
            crate::plan::row_type::QualifiedField::qualified("products", "name"),
            (JoinSide::Build, 1),
        );

        let mut combine_inputs: indexmap::IndexMap<String, crate::plan::combine::CombineInput> =
            indexmap::IndexMap::new();
        let mut driver_row_cols: indexmap::IndexMap<
            crate::plan::row_type::QualifiedField,
            cxl::typecheck::Type,
        > = indexmap::IndexMap::new();
        driver_row_cols.insert(
            crate::plan::row_type::QualifiedField::bare("dk"),
            cxl::typecheck::Type::Int,
        );
        driver_row_cols.insert(
            crate::plan::row_type::QualifiedField::bare("v"),
            cxl::typecheck::Type::String,
        );
        let mut build_row_cols: indexmap::IndexMap<
            crate::plan::row_type::QualifiedField,
            cxl::typecheck::Type,
        > = indexmap::IndexMap::new();
        build_row_cols.insert(
            crate::plan::row_type::QualifiedField::bare("bk"),
            cxl::typecheck::Type::Int,
        );
        build_row_cols.insert(
            crate::plan::row_type::QualifiedField::bare("name"),
            cxl::typecheck::Type::String,
        );
        combine_inputs.insert(
            "orders".to_string(),
            crate::plan::combine::CombineInput {
                upstream_name: Arc::from("orders"),
                row: crate::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
                estimated_cardinality: None,
            },
        );
        combine_inputs.insert(
            "products".to_string(),
            crate::plan::combine::CombineInput {
                upstream_name: Arc::from("products"),
                row: crate::plan::row_type::Row::closed(build_row_cols, CxlSpan::new(0, 0)),
                estimated_cardinality: None,
            },
        );
        let resolver_mapping =
            CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

        let stable = StableEvalContext::test_default();
        let source_file: Arc<str> = Arc::from("test.csv");
        let ctx = EvalContext {
            stable: &stable,
            source_file: &source_file,
            source_row: 0,
        };
        let mut budget = MemoryBudget::new(u64::MAX, 0.80);

        // Drive everything through grace hash. body_program=None so
        // the synthetic-step concatenation path is exercised; that's
        // fine for correctness because we only assert on join
        // membership, not on the body shape.
        let mut combined_schema_builder = clinker_record::SchemaBuilder::new();
        combined_schema_builder = combined_schema_builder.with_field("dk");
        combined_schema_builder = combined_schema_builder.with_field("v");
        combined_schema_builder = combined_schema_builder.with_field("bk");
        combined_schema_builder = combined_schema_builder.with_field("name");
        let combined_schema = combined_schema_builder.build();

        let result = execute_combine_grace_hash(GraceHashExec {
            name: "grace_test",
            build_qualifier: "products",
            driver_records: drivers,
            build_records: builds,
            decomposed: &decomposed,
            body_program: None,
            resolver_mapping: &resolver_mapping,
            output_schema: Some(&combined_schema),
            match_mode: crate::config::pipeline_node::MatchMode::All,
            on_miss: crate::config::pipeline_node::OnMiss::Skip,
            partition_bits: 4,
            ctx: &ctx,
            budget: &mut budget,
        })
        .expect("grace hash E2E");

        assert_eq!(result.len(), 10, "every driver matches one build by k");
        // Verify membership: each (k, v, k, name) tuple is present.
        let mut seen: Vec<(i64, String, String)> = result
            .iter()
            .map(|(rec, _)| {
                let k = match rec.values()[0] {
                    Value::Integer(i) => i,
                    _ => panic!("k not int"),
                };
                let v = match &rec.values()[1] {
                    Value::String(s) => s.to_string(),
                    _ => panic!("v not str"),
                };
                // After widen: build columns occupy slots 2 & 3 (k_b,
                // name) but the synthetic concat writes raw values
                // positionally.
                let name = match &rec.values()[3] {
                    Value::String(s) => s.to_string(),
                    _ => panic!("name not str"),
                };
                (k, v, name)
            })
            .collect();
        seen.sort_by_key(|(k, _, _)| *k);
        let expected: Vec<(i64, String, String)> = (0..10)
            .map(|i| (i, format!("d-{i}"), format!("b-{i}")))
            .collect();
        assert_eq!(seen, expected);
    }

    /// End-to-end: a tiny memory budget forces partition spill during
    /// build. The reload phase rehydrates the spilled partitions and
    /// emits the same join membership the in-memory path would.
    #[test]
    fn execute_grace_hash_spill_then_reload_correct() {
        use crate::executor::combine::{CombineResolverMapping, JoinSide};
        use crate::plan::combine::{DecomposedPredicate, EqualityConjunct};
        use cxl::eval::{EvalContext, StableEvalContext};

        let driver_schema = schema_with(&["dk", "v"]);
        let build_schema = schema_with(&["bk", "name"]);

        let drivers: Vec<(Record, RecordOrder)> = (0..32i64)
            .map(|i| {
                (
                    Record::new(
                        Arc::clone(&driver_schema),
                        vec![Value::Integer(i), Value::String(format!("d-{i}").into())],
                    ),
                    i as u64,
                )
            })
            .collect();
        let builds: Vec<Record> = (0..32i64)
            .map(|i| {
                Record::new(
                    Arc::clone(&build_schema),
                    vec![Value::Integer(i), Value::String(format!("b-{i}").into())],
                )
            })
            .collect();

        let (left_tp, left_expr) =
            compile_key("emit k = dk", &["dk"], &[("dk", cxl::typecheck::Type::Int)]);
        let (right_tp, right_expr) =
            compile_key("emit k = bk", &["bk"], &[("bk", cxl::typecheck::Type::Int)]);
        let decomposed = DecomposedPredicate {
            equalities: vec![EqualityConjunct {
                left_expr,
                left_input: Arc::from("orders"),
                left_program: left_tp,
                right_expr,
                right_input: Arc::from("products"),
                right_program: right_tp,
            }],
            ranges: Vec::new(),
            residual: None,
        };

        let mut mapping_q: std::collections::HashMap<
            crate::plan::row_type::QualifiedField,
            (JoinSide, u32),
        > = std::collections::HashMap::new();
        mapping_q.insert(
            crate::plan::row_type::QualifiedField::qualified("orders", "dk"),
            (JoinSide::Probe, 0),
        );
        mapping_q.insert(
            crate::plan::row_type::QualifiedField::qualified("orders", "v"),
            (JoinSide::Probe, 1),
        );
        mapping_q.insert(
            crate::plan::row_type::QualifiedField::qualified("products", "bk"),
            (JoinSide::Build, 0),
        );
        mapping_q.insert(
            crate::plan::row_type::QualifiedField::qualified("products", "name"),
            (JoinSide::Build, 1),
        );

        let mut combine_inputs: indexmap::IndexMap<String, crate::plan::combine::CombineInput> =
            indexmap::IndexMap::new();
        let mut driver_row_cols: indexmap::IndexMap<
            crate::plan::row_type::QualifiedField,
            cxl::typecheck::Type,
        > = indexmap::IndexMap::new();
        driver_row_cols.insert(
            crate::plan::row_type::QualifiedField::bare("dk"),
            cxl::typecheck::Type::Int,
        );
        driver_row_cols.insert(
            crate::plan::row_type::QualifiedField::bare("v"),
            cxl::typecheck::Type::String,
        );
        let mut build_row_cols: indexmap::IndexMap<
            crate::plan::row_type::QualifiedField,
            cxl::typecheck::Type,
        > = indexmap::IndexMap::new();
        build_row_cols.insert(
            crate::plan::row_type::QualifiedField::bare("bk"),
            cxl::typecheck::Type::Int,
        );
        build_row_cols.insert(
            crate::plan::row_type::QualifiedField::bare("name"),
            cxl::typecheck::Type::String,
        );
        combine_inputs.insert(
            "orders".to_string(),
            crate::plan::combine::CombineInput {
                upstream_name: Arc::from("orders"),
                row: crate::plan::row_type::Row::closed(driver_row_cols, CxlSpan::new(0, 0)),
                estimated_cardinality: None,
            },
        );
        combine_inputs.insert(
            "products".to_string(),
            crate::plan::combine::CombineInput {
                upstream_name: Arc::from("products"),
                row: crate::plan::row_type::Row::closed(build_row_cols, CxlSpan::new(0, 0)),
                estimated_cardinality: None,
            },
        );
        let resolver_mapping =
            CombineResolverMapping::from_pre_resolved(&Arc::new(mapping_q), &combine_inputs);

        let stable = StableEvalContext::test_default();
        let source_file: Arc<str> = Arc::from("test.csv");
        let ctx = EvalContext {
            stable: &stable,
            source_file: &source_file,
            source_row: 0,
        };

        // Big hard limit so should_abort never fires; tiny spill
        // threshold so should_spill fires immediately (process RSS
        // far exceeds 1 KiB on any host). This decouples spill
        // activation from build abort.
        let mut budget = MemoryBudget::new(10 * 1024 * 1024 * 1024, 0.000_001);

        let mut combined_schema_builder = clinker_record::SchemaBuilder::new();
        combined_schema_builder = combined_schema_builder.with_field("dk");
        combined_schema_builder = combined_schema_builder.with_field("v");
        combined_schema_builder = combined_schema_builder.with_field("bk");
        combined_schema_builder = combined_schema_builder.with_field("name");
        let combined_schema = combined_schema_builder.build();

        let result = execute_combine_grace_hash(GraceHashExec {
            name: "grace_spill_test",
            build_qualifier: "products",
            driver_records: drivers,
            build_records: builds,
            decomposed: &decomposed,
            body_program: None,
            resolver_mapping: &resolver_mapping,
            output_schema: Some(&combined_schema),
            match_mode: crate::config::pipeline_node::MatchMode::All,
            on_miss: crate::config::pipeline_node::OnMiss::Skip,
            partition_bits: 4,
            ctx: &ctx,
            budget: &mut budget,
        })
        .expect("grace hash spill E2E");

        assert_eq!(
            result.len(),
            32,
            "every driver matches one build under spill"
        );
        let mut keys: Vec<i64> = result
            .iter()
            .map(|(rec, _)| match rec.values()[0] {
                Value::Integer(i) => i,
                _ => panic!(),
            })
            .collect();
        keys.sort();
        assert_eq!(keys, (0..32).collect::<Vec<_>>());
    }

    /// Round-trip records through the spill writer/reader by calling
    /// `add_build_record`, `spill_largest_building`, then reloading via
    /// `drain_spilled` + `GraceSpillReader`.
    #[test]
    fn build_spill_reload_records_match() {
        let schema = schema_with(&["k", "v"]);
        let mut exec = GraceHashExecutor::new(2).unwrap();
        let mut budget = MemoryBudget::new(u64::MAX, 0.80); // never spills via budget
        let originals: Vec<Record> = (0..16i64)
            .map(|i| {
                record_for(
                    &schema,
                    vec![Value::Integer(i), Value::String(format!("v-{i}").into())],
                )
            })
            .collect();
        for (i, r) in originals.iter().enumerate() {
            exec.add_build_record(r.clone(), i as u64, &mut budget)
                .unwrap();
        }
        // Force-spill every partition.
        for idx in 0..exec.partitions.len() {
            let _ = exec.spill_partition(idx);
        }
        let spilled = exec.drain_spilled();
        let mut reloaded: Vec<Record> = Vec::new();
        for sp in spilled {
            for path in &sp.build_files {
                let reader = GraceSpillReader::open(path, Arc::clone(&schema)).unwrap();
                for r in reader {
                    reloaded.push(r.unwrap());
                }
            }
        }
        // Sort both by k to compare independent of partition ordering.
        let mut by_k: Vec<(i64, String)> = reloaded
            .iter()
            .map(|r| {
                let k = match r.get("k") {
                    Some(Value::Integer(i)) => *i,
                    _ => panic!("missing k"),
                };
                let v = match r.get("v") {
                    Some(Value::String(s)) => s.to_string(),
                    _ => panic!("missing v"),
                };
                (k, v)
            })
            .collect();
        by_k.sort_by_key(|(k, _)| *k);
        let expected: Vec<(i64, String)> = (0..16).map(|i| (i, format!("v-{i}"))).collect();
        assert_eq!(by_k, expected);
    }
}
