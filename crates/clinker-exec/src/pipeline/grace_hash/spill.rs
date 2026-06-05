//! Spill reload and the block-nested-loop fallback for the grace hash
//! join. After the in-memory probe phase finishes, every partition that
//! overflowed to disk is reloaded here: it is either rebuilt in memory,
//! recursively repartitioned via `assigner.double()`, or — when a
//! dominant key makes further splitting useless — joined through the
//! bounded-memory BNL path.

use std::path::Path;
use std::sync::Arc;

use ahash::RandomState;
use clinker_record::{Record, Schema, Value};
use cxl::eval::{EvalContext, ProgramEvaluator};

use super::build::{BuildChunkIter, Hll, PartitionAssigner};
use super::probe::{EmitArgs, GraceEmitSink, emit_for_probe};
use crate::executor::combine::CombineResolver;
use crate::pipeline::combine::{CombineHashTable, KeyExtractor, hash_composite_key};
use crate::pipeline::grace_spill::{
    GraceSpillReader, GraceSpillWriter, SpillFilePath, grace_spill_error,
};
use crate::pipeline::memory::MemoryArbitrator;
use clinker_plan::BudgetCategory;
use clinker_plan::error::PipelineError;

/// Number of result records the BNL fallback emits per output batch
/// before polling [`MemoryArbitrator::should_abort`]. Output amplification
/// from a hot key joining against many probe-side records can produce
/// `M * N` rows for a single equivalence class; periodic polling at the
/// 10 K boundary keeps the abort signal responsive without paying an
/// RSS read per emit.
pub(crate) const RESULT_BATCH_SIZE: usize = 10_000;

/// Bytes reserved for the probe-side spill stream during BNL chunked
/// scans. Subtracted from the soft-limit budget when sizing each build
/// chunk; the remainder is then halved to leave headroom for hashbrown
/// table expansion (StarRocks #56491). 4 MiB is the LZ4 frame
/// decompression buffer cost plus a small slack for the postcard
/// payload buffer.
pub(crate) const PROBE_BUFFER_RESERVATION: usize = 4 * 1024 * 1024;

/// Threshold below which a recursive partition split is judged
/// irreducible. If the largest child holds more than `1 -
/// SKEW_REDUCTION_THRESHOLD` (i.e., 80%) of the parent's record count,
/// doubling the partition count again can't separate the dominant key
/// — bail out of recursion and fall back to BNL on the parent.
/// AsterixDB VLDB 2022 settles on the same fraction.
pub(crate) const SKEW_REDUCTION_THRESHOLD: f64 = 0.20;

/// One spilled partition's reload payload, drained from the executor
/// after the probe phase. The HLL sketch travels alongside so the
/// BNL fallback path can fold a cardinality estimate into the E310
/// diagnostic without re-walking the spill files.
pub(crate) struct SpilledPartition {
    pub partition_id: u16,
    pub build_files: Vec<SpillFilePath>,
    pub probe_files: Vec<SpillFilePath>,
    pub build_count: u64,
    pub hash_bits: u8,
    pub distinct_sketch: Hll,
}

/// Bundle of reload-phase context shared across recursive
/// [`process_spilled_partition`] calls. Lifetimes track the executor's
/// owned data: `build_schema` is owned (Arc-cloned at every recursive
/// step) so the spill reader can attach it to each rehydrated record.
pub(super) struct ReloadContext<'a> {
    pub(super) name: &'a str,
    pub(super) build_extractor: &'a KeyExtractor,
    pub(super) driver_extractor: &'a KeyExtractor,
    pub(super) emit: &'a EmitArgs<'a>,
    pub(super) ctx: &'a EvalContext<'a>,
    pub(super) build_schema: Arc<Schema>,
    pub(super) driver_schema: Arc<Schema>,
    pub(super) spill_dir: &'a Path,
    /// Whether repartition spill files written during reload are
    /// LZ4-compressed. Carried from the dispatcher's resolved
    /// `[storage.spill] compress` decision so recursively-split partition
    /// files honor the same knob the build/probe spill files did.
    pub(super) spill_compress: bool,
    pub(super) hash_state: &'a RandomState,
}

/// Reload one spilled partition. If it fits in memory, build its hash
/// table and probe the spilled probe-side records. If it exceeds the
/// budget, repartition into 2 child partitions via `assigner.double()`
/// and recurse.
pub(super) fn process_spilled_partition(
    rc: &ReloadContext<'_>,
    sp: SpilledPartition,
    body_evaluator: &mut Option<ProgramEvaluator>,
    budget: &MemoryArbitrator,
    sink: &mut GraceEmitSink<'_>,
) -> Result<(), PipelineError> {
    let name = rc.name;
    let build_extractor = rc.build_extractor;
    let driver_extractor = rc.driver_extractor;
    let ctx = rc.ctx;
    let build_schema = Arc::clone(&rc.build_schema);
    let driver_schema = Arc::clone(&rc.driver_schema);
    let spill_dir = rc.spill_dir;
    let spill_compress = rc.spill_compress;
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

    // Decide: in-memory build, recursive split, or BNL fallback.
    //
    // The reload path tries three tiers in order:
    //  1. If the partition fits under the soft limit, build the hash
    //     table directly and probe.
    //  2. Otherwise, tentatively classify records under the doubled
    //     assigner and check whether the largest child still holds
    //     more than `1 - SKEW_REDUCTION_THRESHOLD` of the parent's
    //     records. A non-trivial reduction means split-and-recurse
    //     can make progress.
    //  3. If the reduction is below threshold, or the assigner is
    //     already at the partition-bit cap, the partition is
    //     irreducible — fall back to block-nested-loop. BNL holds
    //     bounded memory by chunking the build side into pieces sized
    //     for `(soft_limit - PROBE_BUFFER_RESERVATION) / 2`.
    let parent_assigner = PartitionAssigner::new(sp.hash_bits);
    let child_assigner_opt = parent_assigner.double();
    let needs_split = budget.should_spill() && child_assigner_opt.is_some();
    if needs_split && sp.build_count > 1 {
        let child_assigner = child_assigner_opt.unwrap();
        // Repartition build_records into 2 child partitions by
        // re-hashing under the wider assigner. The parent's partition
        // id is `sp.partition_id`; under the child assigner the parent
        // id equals `child_id >> 1`, so children are `2*p` and `2*p+1`.
        // Each child accumulates its own HLL during this pass — the
        // sketch is invariant under hash-bit width (the underlying
        // keys are unchanged), but rebuilding from the rehashed
        // stream keeps the child sketch in step with its records
        // rather than carrying parent-level state forward.
        let parent_id = sp.partition_id as u64;
        let mut child_a: Vec<Record> = Vec::new();
        let mut child_b: Vec<Record> = Vec::new();
        let mut child_a_sketch = Hll::new();
        let mut child_b_sketch = Hll::new();
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
                child_a_sketch.add(h);
                child_a.push(r);
            } else {
                child_b_sketch.add(h);
                child_b.push(r);
            }
        }

        // Skew detection: a recursive split that fails to reduce the
        // largest child by at least SKEW_REDUCTION_THRESHOLD is wasted
        // work. The dominant key hashes are concentrated at one parent
        // bucket regardless of bit width; doubling the assigner can't
        // separate them. Bail out of recursion and route the parent
        // into BNL fallback. Reassemble the parent's records from the
        // (now-bisected) child halves so BNL sees the unsplit input.
        let parent_count = sp.build_count;
        let max_child = child_a.len().max(child_b.len()) as u64;
        let irreducible = parent_count > 0
            && (max_child as f64) > (1.0 - SKEW_REDUCTION_THRESHOLD) * (parent_count as f64);

        if irreducible {
            let mut combined = child_a;
            combined.append(&mut child_b);
            return bnl_fallback(
                rc,
                &sp,
                combined,
                body_evaluator,
                budget,
                sink,
                &mut BnlStats::default(),
            );
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
        for (child_id, child_build, child_probe, child_sketch) in [
            (parent_id * 2, child_a, child_a_probe, child_a_sketch),
            (parent_id * 2 + 1, child_b, child_b_probe, child_b_sketch),
        ] {
            let bcount = child_build.len() as u64;
            let mut bw = GraceSpillWriter::new(
                spill_dir,
                child_assigner.hash_bits(),
                child_id as u16,
                spill_compress,
            )
            .map_err(|e| grace_spill_error(e, name, "repartition build writer"))?;
            for r in &child_build {
                bw.write_record(r)
                    .map_err(|e| grace_spill_error(e, name, "repartition build write"))?;
            }
            let (bpath, b_written) = bw
                .finish()
                .map_err(|e| grace_spill_error(e, name, "repartition build finalize"))?;
            if budget.record_spill_bytes(b_written) {
                return Err(PipelineError::spill_cap_exceeded(
                    name,
                    budget.disk_quota(),
                    b_written,
                    budget.cumulative_spill_bytes(),
                ));
            }
            let mut probe_files: Vec<SpillFilePath> = Vec::new();
            if !child_probe.is_empty() {
                let mut pw = GraceSpillWriter::new(
                    spill_dir,
                    child_assigner.hash_bits(),
                    (child_id as u16) | 0x8000,
                    spill_compress,
                )
                .map_err(|e| grace_spill_error(e, name, "repartition probe writer"))?;
                for r in &child_probe {
                    pw.write_record(r)
                        .map_err(|e| grace_spill_error(e, name, "repartition probe write"))?;
                }
                let (p, p_written) = pw
                    .finish()
                    .map_err(|e| grace_spill_error(e, name, "repartition probe finalize"))?;
                if budget.record_spill_bytes(p_written) {
                    return Err(PipelineError::spill_cap_exceeded(
                        name,
                        budget.disk_quota(),
                        p_written,
                        budget.cumulative_spill_bytes(),
                    ));
                }
                probe_files.push(p);
            }
            let child_sp = SpilledPartition {
                partition_id: child_id as u16,
                build_files: vec![bpath],
                probe_files,
                build_count: bcount,
                hash_bits: child_assigner.hash_bits(),
                distinct_sketch: child_sketch,
            };
            process_spilled_partition(rc, child_sp, body_evaluator, budget, sink)?;
        }
        return Ok(());
    }

    // No further splitting is possible (assigner cap reached), but the
    // budget still says we're over: BNL is the only safe choice. The
    // in-memory hash-table branch below would build a table the size
    // of the entire partition.
    if budget.should_spill() && child_assigner_opt.is_none() {
        return bnl_fallback(
            rc,
            &sp,
            build_records,
            body_evaluator,
            budget,
            sink,
            &mut BnlStats::default(),
        );
    }

    // Build the in-memory hash table for the reloaded partition.
    let hash_table = CombineHashTable::build(
        build_records,
        build_extractor,
        ctx,
        budget,
        Some(sp.build_count as usize),
    )
    .map_err(|e| PipelineError::MemoryBudgetExceeded {
        node: name.to_string(),
        used: budget.peak_rss().unwrap_or(0),
        limit: budget.hard_limit(),
        source: BudgetCategory::Arena,
        detail: Some(format!("grace hash reload build: {e}")),
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
            let row_ctx = ctx.with_row(row_seq);
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
                sink,
            )?;
            row_seq += 1;
        }
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────────
// Block-nested-loop fallback
// ──────────────────────────────────────────────────────────────────────────

/// Counters captured during one BNL execution so tests can assert on
/// chunking and batching behavior without instrumenting the budget. The
/// fields are intentionally crate-public so the unit tests in this
/// module can read them; production callers ignore the value.
#[derive(Debug, Default, Clone)]
pub(crate) struct BnlStats {
    /// Number of build-side chunks the fallback materialized.
    pub chunks_processed: usize,
    /// Largest hash-table memory footprint observed across all
    /// chunks. Sums every byte the table allocates per
    /// [`CombineHashTable::memory_bytes`].
    pub peak_chunk_table_bytes: usize,
    /// Number of times the fallback hit a 10 K-record output batch
    /// boundary and polled [`MemoryArbitrator::should_abort`].
    pub batches_emitted: usize,
    /// Largest single-chunk record count.
    pub peak_chunk_records: usize,
    /// Bytes the per-chunk sizing formula resolved to. Diagnostic
    /// hook for the bounded-memory test.
    pub chunk_byte_budget: usize,
}

/// Block-nested-loop fallback for irreducible partitions.
///
/// Chunks the build side into pieces sized for `(soft_limit -
/// PROBE_BUFFER_RESERVATION) / 2`. For each chunk: builds a
/// [`CombineHashTable`], scans every probe-side spill file in turn,
/// emits matches via [`emit_for_probe`], drops the chunk's hash table,
/// advances. The `/2` factor reserves headroom for hashbrown's resize
/// (per StarRocks #56491); the `PROBE_BUFFER_RESERVATION` reserves
/// space for the LZ4 frame decoder + postcard buffer.
///
/// Memory model:
///   - In-flight: one chunk's hash table + the probe spill file's
///     decode buffer + the per-record emit batch. Bounded by
///     construction.
///   - Out-of-loop: `output` accumulates results. Output amplification
///     for a single hot key against M probe records is `M * 1`; per
///     chunk it's `M * chunk_keys`, so the result-batch poll happens
///     between every 10 K matches.
///
/// On [`MemoryArbitrator::should_abort`] returning true at any tier
/// (chunk-table construction, between batches), the function returns
/// an E310 [`PipelineError::Compilation`] carrying the partition_id
/// and the HLL-derived approximate distinct-key count.
pub(super) fn bnl_fallback(
    rc: &ReloadContext<'_>,
    sp: &SpilledPartition,
    build_records: Vec<Record>,
    body_evaluator: &mut Option<ProgramEvaluator>,
    budget: &MemoryArbitrator,
    sink: &mut GraceEmitSink<'_>,
    stats: &mut BnlStats,
) -> Result<(), PipelineError> {
    let name = rc.name;
    let driver_schema = Arc::clone(&rc.driver_schema);
    let approx_distinct = sp.distinct_sketch.estimate();

    // Per-chunk byte budget. Soft-limit minus probe reservation, then
    // halved to leave headroom for the hash table's bucket array resize
    // (StarRocks #56491). `max(1)` floor protects against soft_limit <
    // PROBE_BUFFER_RESERVATION (e.g., test budgets) — yields one-record
    // chunks, which still terminate.
    let soft = budget.soft_limit() as usize;
    let chunk_budget = soft
        .saturating_sub(PROBE_BUFFER_RESERVATION)
        .saturating_div(2)
        .max(1);
    stats.chunk_byte_budget = chunk_budget;

    // If hard limit is already breached before any work, fail fast.
    if budget.should_abort() {
        return Err(combine_e310_partition_aborted(
            name,
            sp.partition_id,
            approx_distinct,
            budget,
        ));
    }

    let chunks = BuildChunkIter::new(build_records, chunk_budget);
    let mut probe_keys_buf: Vec<Value> = Vec::with_capacity(rc.driver_extractor.len());
    for chunk in chunks {
        stats.chunks_processed += 1;
        stats.peak_chunk_records = stats.peak_chunk_records.max(chunk.len());
        let chunk_len = chunk.len();
        let table =
            CombineHashTable::build(chunk, rc.build_extractor, rc.ctx, budget, Some(chunk_len))
                .map_err(|e| PipelineError::MemoryBudgetExceeded {
                    node: name.to_string(),
                    used: budget.peak_rss().unwrap_or(0),
                    limit: budget.hard_limit(),
                    source: BudgetCategory::Arena,
                    detail: Some(format!(
                        "grace hash bnl chunk build (partition {}, ~{} distinct): {e}",
                        sp.partition_id, approx_distinct
                    )),
                })?;
        stats.peak_chunk_table_bytes = stats.peak_chunk_table_bytes.max(table.memory_bytes());

        // Emit matches in 10 K-record batches against this chunk's
        // table. After every batch boundary, poll should_abort so a
        // runaway output amplification (one hot key joined against M
        // probe records → M outputs per probe) gets caught.
        let mut emitted_in_batch = 0usize;

        // Per-chunk row-sequence baseline so each probe stream sees a
        // contiguous numbering. The probe-side spill files were
        // written with the original probe order discarded; we reissue
        // monotonic row numbers for downstream sort stability.
        let mut row_seq: u64 = 0;
        for path in &sp.probe_files {
            let reader = GraceSpillReader::open(path, Arc::clone(&driver_schema)).map_err(|e| {
                PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!("grace hash bnl probe open failed: {e}"),
                }
            })?;
            for r in reader {
                let probe_record = r.map_err(|e| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!("grace hash bnl probe read failed: {e}"),
                })?;
                let row_ctx = rc.ctx.with_row(row_seq);
                let resolver = CombineResolver::new(rc.emit.resolver_mapping, &probe_record, None);
                probe_keys_buf.clear();
                rc.driver_extractor
                    .extract_into(&row_ctx, &resolver, &mut probe_keys_buf)
                    .map_err(|e| PipelineError::Compilation {
                        transform_name: name.to_string(),
                        messages: vec![format!("grace hash bnl probe key eval: {e}")],
                    })?;
                let probe_iter = table.probe(&probe_keys_buf);
                let pre = sink.records.len();
                emit_for_probe(
                    rc.emit,
                    &probe_record,
                    row_seq,
                    probe_iter,
                    body_evaluator.as_mut(),
                    &row_ctx,
                    sink,
                )?;
                let added = sink.records.len() - pre;
                emitted_in_batch += added;
                row_seq += 1;

                while emitted_in_batch >= RESULT_BATCH_SIZE {
                    stats.batches_emitted += 1;
                    emitted_in_batch -= RESULT_BATCH_SIZE;
                    if budget.should_abort() {
                        return Err(combine_e310_partition_aborted(
                            name,
                            sp.partition_id,
                            approx_distinct,
                            budget,
                        ));
                    }
                }
            }
        }
        // Drop the chunk's hash table before reading the next chunk;
        // bounded-memory invariant relies on this happening eagerly.
        drop(table);

        // Final between-chunk abort check. The chunked build alone
        // cannot exceed the budget by construction (chunk_budget is
        // sized for it), but cumulative `output` growth could.
        if budget.should_abort() {
            return Err(combine_e310_partition_aborted(
                name,
                sp.partition_id,
                approx_distinct,
                budget,
            ));
        }
    }
    Ok(())
}

/// E310 — runtime partition aborted because the chunked BNL fallback
/// could not bring memory under the hard limit. Carries the partition
/// index and an HLL-approximated distinct-key count in `detail` so the
/// operator can size `partition_bits` upstream or reroute the dominant
/// key before retrying.
fn combine_e310_partition_aborted(
    transform: &str,
    partition_id: u16,
    approx_distinct: u64,
    budget: &MemoryArbitrator,
) -> PipelineError {
    PipelineError::MemoryBudgetExceeded {
        node: transform.to_string(),
        used: budget.peak_rss().unwrap_or(0),
        limit: budget.hard_limit(),
        source: BudgetCategory::Arena,
        detail: Some(format!(
            "combine grace hash exceeded memory limit on partition {partition_id}; \
             approximate distinct key count {approx_distinct}"
        )),
    }
}
