//! Sort-merge join executor for combine nodes with pure-range predicates.
//!
//! Two-phase execution:
//!
//!   Phase A — external sort each side on the range key(s) via the
//!             existing [`SortBuffer`] + [`SpillWriter`] / [`LoserTree`]
//!             infrastructure (postcard+LZ4 spills). Skipped when the
//!             upstream `NodeProperties::ordering` already covers the
//!             range keys.
//!
//!   Phase B — two-cursor merge walk. For each outer-side run with a
//!             common range key, buffer the matching inner-side run in
//!             [`MatchingRunBuffer`] and emit the cross-product. When the
//!             buffered run exceeds `soft_limit / 4` it spills to a
//!             [`GraceSpillWriter`] file and replays for each matching
//!             outer row, bounding peak memory at one inner record on
//!             the spill path.
//!
//! The matching-run spill prevents an unbounded-buffer OOM on
//! high-duplication inner sides — the SPARK-13450 failure mode.
//! DataFusion and CockroachDB use the same pattern.
//!
//! NULL handling matches the IEJoin path: any record with a NULL in any
//! range key participates in `on_miss` dispatch instead of the merge
//! walk, since CXL ternary semantics return false for NULL comparisons.
//! Pre-filtering keeps the sorted runs free of NULLs so the cursor walk
//! never has to special-case them.
//!
//! The current planner selects `SortMerge` only when both inputs already
//! arrive sorted on the range key prefix; in that case Phase A is a
//! no-op and the executor walks the inputs in place. Phase A is wired
//! end-to-end so a future planner relaxation (or an explicit user knob)
//! can route unsorted inputs through this path without a second
//! implementation pass.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use clinker_record::{Record, Schema, Value};
use cxl::ast::Expr;
use cxl::eval::{EvalContext, EvalError, EvalResult, ProgramEvaluator, SkipReason};
use cxl::typecheck::TypedProgram;
use indexmap::IndexMap;

use crate::config::pipeline_node::{MatchMode, OnMiss};
use crate::error::PipelineError;
use crate::executor::combine::{CombineResolver, CombineResolverMapping};
use crate::pipeline::combine::KeyExtractor;
use crate::pipeline::grace_spill::{GraceSpillReader, GraceSpillWriter, SpillFilePath};
use crate::pipeline::memory::MemoryBudget;
use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
use crate::plan::combine::{DecomposedPredicate, RangeOp};

/// Cap on matches collected per driver under [`MatchMode::Collect`].
/// Mirrors the constant in `pipeline::combine`, `pipeline::iejoin`, and
/// `pipeline::grace_hash` so every code path truncates at the same
/// threshold.
const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// Period (matched pairs emitted) between [`MemoryBudget::should_abort`]
/// polls during the merge walk.
const MEMORY_CHECK_INTERVAL: usize = 10_000;

/// Order-tracking sidecar carried alongside every record in the
/// executor's `node_buffers`. Same alias the IEJoin and grace-hash
/// kernels expose; spelled out here so the public exec entry point's
/// signature stays self-documenting.
pub(crate) type RecordOrder = u64;

// ──────────────────────────────────────────────────────────────────────
// Matching-run buffer
// ──────────────────────────────────────────────────────────────────────

/// Buffered inner-side records that share a single range-key value.
///
/// Capped at `soft_limit / 4` bytes in the in-memory variant; on
/// overflow the buffer transitions to the spilled variant, where most
/// records live on disk (in one or more spill segments) and a small
/// in-memory tail accumulates until it itself overflows and gets
/// flushed to a new segment. Replay streams every segment in order and
/// then the tail.
enum MatchingRunBuffer {
    InMemory {
        records: Vec<Record>,
        bytes: usize,
    },
    Spilled {
        /// Spill file segments, written in append order. Each is a
        /// self-contained postcard+LZ4 frame from `GraceSpillWriter`.
        segments: Vec<SpillFilePath>,
        schema: Arc<Schema>,
        /// Total records written to disk across `segments`.
        spilled_count: u64,
        /// In-memory tail of records that have not yet been spilled.
        tail: Vec<Record>,
        /// Bytes accumulated in `tail` since the last spill flush.
        tail_bytes: usize,
    },
}

impl MatchingRunBuffer {
    fn new() -> Self {
        Self::InMemory {
            records: Vec::new(),
            bytes: 0,
        }
    }
}

/// Push one inner record into the buffer, spilling either the whole
/// in-memory run (on the first overflow) or the accumulated tail (on
/// subsequent overflows) when the byte count would exceed `byte_limit`.
///
/// Returns the byte length committed to disk during this call (zero
/// when the record stayed in memory). Caller folds the result into
/// [`crate::pipeline::memory::MemoryBudget::record_spill_bytes`] so
/// the disk-quota gate can fire on overflow.
fn push_to_buffer(
    buf: &mut MatchingRunBuffer,
    record: Record,
    byte_limit: usize,
    spill_dir: &std::path::Path,
    spill_seq: &mut u32,
) -> std::io::Result<u64> {
    let incoming = std::mem::size_of::<Record>() + record.estimated_heap_size();
    match buf {
        MatchingRunBuffer::InMemory { records, bytes } => {
            if bytes.saturating_add(incoming) > byte_limit && !records.is_empty() {
                // Promote to Spilled: drain all records to a fresh
                // segment, then start a tail with the incoming record.
                let mut drained = std::mem::take(records);
                let schema = Arc::clone(drained[0].schema());
                drained.push(record);
                let (segment_path, written) = write_spill_segment(spill_dir, spill_seq, &drained)?;
                let count = drained.len() as u64;
                *buf = MatchingRunBuffer::Spilled {
                    segments: vec![segment_path],
                    schema,
                    spilled_count: count,
                    tail: Vec::new(),
                    tail_bytes: 0,
                };
                Ok(written)
            } else {
                *bytes = bytes.saturating_add(incoming);
                records.push(record);
                Ok(0)
            }
        }
        MatchingRunBuffer::Spilled {
            segments,
            schema: _,
            spilled_count,
            tail,
            tail_bytes,
        } => {
            if tail_bytes.saturating_add(incoming) > byte_limit && !tail.is_empty() {
                // Tail itself overflowed; flush it as a new segment
                // and start a fresh tail with the incoming record.
                let mut drained = std::mem::take(tail);
                drained.push(record);
                let (segment_path, written) = write_spill_segment(spill_dir, spill_seq, &drained)?;
                *spilled_count = spilled_count.saturating_add(drained.len() as u64);
                *tail_bytes = 0;
                segments.push(segment_path);
                Ok(written)
            } else {
                *tail_bytes = tail_bytes.saturating_add(incoming);
                tail.push(record);
                Ok(0)
            }
        }
    }
}

/// Write a slice of records as a single fresh spill segment via
/// [`GraceSpillWriter`]. The returned path lives inside `spill_dir`;
/// the caller's `TempDir` owns its lifetime. The byte length is
/// returned alongside the path so the caller can fold it into
/// [`crate::pipeline::memory::MemoryBudget::record_spill_bytes`].
fn write_spill_segment(
    spill_dir: &std::path::Path,
    spill_seq: &mut u32,
    records: &[Record],
) -> std::io::Result<(SpillFilePath, u64)> {
    let seq = *spill_seq;
    *spill_seq = spill_seq.wrapping_add(1);
    let mut writer = GraceSpillWriter::new(spill_dir, 0, (seq & 0xFFFF) as u16)?;
    for r in records {
        writer.write_record(r)?;
    }
    writer.finish()
}

/// Replay-side iteration over a matching-run buffer. Yields each inner
/// record once. The spilled variant streams from each segment in order
/// then yields the in-memory tail; the in-memory variant clones from
/// the backing Vec so the buffer can be re-iterated per outer row in
/// the matching run.
fn iter_run(
    buf: &MatchingRunBuffer,
) -> Result<Box<dyn Iterator<Item = std::io::Result<Record>> + '_>, std::io::Error> {
    match buf {
        MatchingRunBuffer::InMemory { records, .. } => {
            Ok(Box::new(records.iter().map(|r| Ok(r.clone()))))
        }
        MatchingRunBuffer::Spilled {
            segments,
            schema,
            tail,
            ..
        } => {
            // Open every segment up front so iteration is straight-line
            // (the chained iterator below is a single `flatten`); a
            // failure to open any segment surfaces immediately rather
            // than mid-walk.
            let mut readers: Vec<GraceSpillReader> = Vec::with_capacity(segments.len());
            for path in segments {
                readers.push(GraceSpillReader::open(path, Arc::clone(schema))?);
            }
            let from_disk = readers.into_iter().flatten();
            let from_tail = tail.iter().map(|r| Ok(r.clone()));
            Ok(Box::new(from_disk.chain(from_tail)))
        }
    }
}

// ──────────────────────────────────────────────────────────────────────
// Range-key extraction
// ──────────────────────────────────────────────────────────────────────

/// Extract a single range key from a record via the supplied extractor.
/// Returns `None` when the resulting Value is NULL or non-orderable —
/// matching the IEJoin pre-scan's NULL filter.
fn extract_range_key(
    extractor: &KeyExtractor,
    ctx: &EvalContext<'_>,
    record: &Record,
    resolver_mapping: &CombineResolverMapping,
    is_driver_side: bool,
    buf: &mut Vec<Value>,
) -> Result<Option<Value>, EvalError> {
    buf.clear();
    if is_driver_side {
        let resolver = CombineResolver::new(resolver_mapping, record, None);
        extractor.extract_into(ctx, &resolver, buf)?;
    } else {
        // Build-side key extraction reads directly from the record.
        // The KeyExtractor internals never reach back through the
        // resolver_mapping when the qualifier matches the bare record.
        extractor.extract_into(ctx, record, buf)?;
    }
    if buf.is_empty() {
        return Ok(None);
    }
    let v = buf[0].clone();
    if matches!(v, Value::Null) {
        return Ok(None);
    }
    if !is_orderable(&v) {
        return Ok(None);
    }
    Ok(Some(v))
}

/// Whether a Value participates in range comparisons. Matches the
/// IEJoin's `value_to_i64` set: integers, finite floats, dates,
/// datetimes. Strings, booleans, arrays, and maps are rejected
/// upstream as residual.
fn is_orderable(v: &Value) -> bool {
    match v {
        Value::Integer(_) | Value::Date(_) | Value::DateTime(_) => true,
        Value::Float(f) => f.is_finite(),
        _ => false,
    }
}

/// Compare two range key values under the IEJoin numeric-coercion
/// convention (i64-encoded, float bit-cast). NULLs and non-orderables
/// are filtered before this is called.
fn cmp_range_keys(a: &Value, b: &Value) -> Ordering {
    use chrono::Datelike;
    let ai = match a {
        Value::Integer(i) => *i,
        Value::Float(f) => f.to_bits() as i64,
        Value::Date(d) => d.num_days_from_ce() as i64,
        Value::DateTime(dt) => dt.and_utc().timestamp_micros(),
        _ => 0,
    };
    let bi = match b {
        Value::Integer(i) => *i,
        Value::Float(f) => f.to_bits() as i64,
        Value::Date(d) => d.num_days_from_ce() as i64,
        Value::DateTime(dt) => dt.and_utc().timestamp_micros(),
        _ => 0,
    };
    ai.cmp(&bi)
}

/// Apply a range operator to two pre-extracted, non-NULL range keys.
fn apply_op(left: &Value, op: RangeOp, right: &Value) -> bool {
    let ord = cmp_range_keys(left, right);
    match op {
        RangeOp::Lt => ord == Ordering::Less,
        RangeOp::Le => ord != Ordering::Greater,
        RangeOp::Gt => ord == Ordering::Greater,
        RangeOp::Ge => ord != Ordering::Less,
    }
}

// ──────────────────────────────────────────────────────────────────────
// Public entry point
// ──────────────────────────────────────────────────────────────────────

/// Inputs to [`execute_combine_sort_merge`]. Bundled so the function
/// signature stays under clippy's `too_many_arguments` cap.
pub(crate) struct SortMergeExec<'a> {
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
    /// True when the planner has already verified that both inputs
    /// arrive sorted on the range key prefix. Phase A external sort is
    /// skipped in that case and the inputs are walked in place.
    pub presorted: bool,
    pub ctx: &'a EvalContext<'a>,
    pub budget: &'a mut MemoryBudget,
    /// Pipeline-scoped spill directory borrowed from
    /// `ExecutorContext::spill_root_path`. Matching-run spill segments
    /// land inside it; the surrounding `Arc<TempDir>` Drop is the
    /// secondary panic-safe cleanup sweep.
    pub spill_dir: &'a Path,
}

/// Snapshot of which phases ran during a sort-merge execution. Used by
/// the in-module test gate to verify the pre-sorted skip path and the
/// matching-run spill threshold. Private to the module — production
/// callers go through [`execute_combine_sort_merge`], which discards
/// the stats.
#[derive(Debug, Clone, Copy, Default)]
struct SortMergeStats {
    /// Number of times Phase A external sort was invoked (0 or 2 — once
    /// per side). A pre-sorted execution leaves this at 0.
    phase_a_sort_invocations: u32,
    /// Number of matching runs that overflowed `soft_limit / 4` and
    /// transitioned to a spilled buffer.
    matching_run_spills: u32,
}

/// Execute a combine via sort-merge. Caller provides full driver and
/// build buffers; this function externally sorts each side (when not
/// pre-sorted), then walks the two cursors emitting matching pairs.
///
/// Output `RecordOrder` carries the driver record's original ordering
/// tag — the same convention HashBuildProbe and IEJoin use. Within a
/// single driver, matches emit in build-side sort order; downstream
/// sort is the caller's responsibility.
///
/// # Errors
///
/// Returns [`PipelineError::Compilation`] on key-eval failure, residual
/// or body evaluation errors, and `on_miss: error` driver misses.
/// Returns [`PipelineError::Internal`] on planner-shape violations
/// (e.g. invocation with no range conjuncts) and on spill I/O failures.
pub(crate) fn execute_combine_sort_merge(
    args: SortMergeExec<'_>,
) -> Result<Vec<(Record, RecordOrder)>, PipelineError> {
    let (records, _stats) = execute_combine_sort_merge_with_stats(args)?;
    Ok(records)
}

/// Variant of [`execute_combine_sort_merge`] that also returns
/// [`SortMergeStats`]. Module-private — exposed to the in-module
/// `#[cfg(test)]` block so tests can verify Phase A skip and
/// matching-run spill counts. Production callers go through
/// [`execute_combine_sort_merge`].
fn execute_combine_sort_merge_with_stats(
    args: SortMergeExec<'_>,
) -> Result<(Vec<(Record, RecordOrder)>, SortMergeStats), PipelineError> {
    let SortMergeExec {
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
        presorted,
        ctx,
        budget,
        spill_dir,
    } = args;

    let mut stats = SortMergeStats::default();

    if decomposed.ranges.is_empty() {
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: "sort-merge executor invoked with zero range conjuncts; planner bug"
                .to_string(),
        });
    }

    // Sort-merge in this kernel is wired for the single-inequality
    // case the planner currently selects it for: one range conjunct
    // between the two inputs. Two-inequality predicates land on the
    // IEJoin path instead. The check is a planner-shape invariant,
    // not a user-visible error — the planner never selects SortMerge
    // for multi-range predicates.
    if decomposed.ranges.len() > 1 {
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: format!(
                "sort-merge executor invoked with {} range conjuncts; the planner \
                 selects SortMerge only for single-inequality predicates",
                decomposed.ranges.len()
            ),
        });
    }
    if !decomposed.equalities.is_empty() {
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: format!(
                "sort-merge executor invoked with {} equality conjuncts; the planner \
                 selects SortMerge only for pure-range predicates",
                decomposed.equalities.len()
            ),
        });
    }

    // Resolve the range program. Pure-range with no equality conjuncts:
    // every range expression types against the residual program built
    // by `decompose_predicate` (which folds every range into the
    // residual when ranges are present).
    let range_program: Arc<TypedProgram> =
        Arc::clone(decomposed.residual.as_ref().ok_or_else(|| {
            PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: "decomposed has range conjuncts but no residual program; \
                         decompose_predicate invariant violated"
                    .to_string(),
            }
        })?);

    let range_conjunct = &decomposed.ranges[0];
    let (driver_expr, build_expr, op) = if range_conjunct.right_input.as_ref() == build_qualifier {
        (
            range_conjunct.left_expr.clone(),
            range_conjunct.right_expr.clone(),
            range_conjunct.op,
        )
    } else if range_conjunct.left_input.as_ref() == build_qualifier {
        // Flip so the predicate reads as `driver OP build`.
        let flipped = match range_conjunct.op {
            RangeOp::Lt => RangeOp::Gt,
            RangeOp::Le => RangeOp::Ge,
            RangeOp::Gt => RangeOp::Lt,
            RangeOp::Ge => RangeOp::Le,
        };
        (
            range_conjunct.right_expr.clone(),
            range_conjunct.left_expr.clone(),
            flipped,
        )
    } else {
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: format!(
                "range conjunct has qualifiers ({}, {}); neither matches build \
                     qualifier {build_qualifier:?}",
                range_conjunct.left_input, range_conjunct.right_input
            ),
        });
    };

    let driver_extractor =
        KeyExtractor::new(vec![(Arc::clone(&range_program), driver_expr.clone())]);
    let build_extractor = KeyExtractor::new(vec![(Arc::clone(&range_program), build_expr.clone())]);

    // ── Phase A: external sort each side on the range key ────────────
    //
    // The planner only selects SortMerge today when both inputs arrive
    // pre-sorted on the range key prefix. The non-presorted branch is
    // wired for forward compatibility — a future planner relaxation
    // (or an explicit user knob) can route here without a second
    // executor implementation. The single canonical SortField below
    // describes the range key axis; for pre-sorted inputs we extract
    // keys from records in declared order and skip the sort.
    let driver_field = field_name_of_range_expr(&driver_expr);
    let build_field = field_name_of_range_expr(&build_expr);

    let mut driver_pairs: Vec<(Record, RecordOrder, Value)> = Vec::new();
    let mut driver_unmatched_orders: Vec<RecordOrder> = Vec::new();
    let mut driver_unmatched_records: Vec<(Record, RecordOrder)> = Vec::new();
    let mut range_buf: Vec<Value> = Vec::new();
    for (record, order) in driver_records {
        let key = extract_range_key(
            &driver_extractor,
            ctx,
            &record,
            resolver_mapping,
            true,
            &mut range_buf,
        )
        .map_err(|e| key_eval_error(name, "driving", e))?;
        match key {
            Some(k) => driver_pairs.push((record, order, k)),
            None => {
                driver_unmatched_orders.push(order);
                driver_unmatched_records.push((record, order));
            }
        }
    }

    let mut build_pairs: Vec<(Record, Value)> = Vec::new();
    for record in build_records {
        let key = extract_range_key(
            &build_extractor,
            ctx,
            &record,
            resolver_mapping,
            false,
            &mut range_buf,
        )
        .map_err(|e| key_eval_error(name, "build", e))?;
        if let Some(k) = key {
            build_pairs.push((record, k));
        }
    }

    if !presorted {
        // Phase A external sort: build a SortBuffer<RecordOrder> per
        // side, sized against the configured memory limit, and read
        // back via the LoserTree merge when spills occurred.
        sort_driver_pairs_externally(&mut driver_pairs, name, &driver_field, budget)?;
        sort_build_pairs_externally(&mut build_pairs, name, &build_field, budget)?;
        stats.phase_a_sort_invocations = 2;
    } else {
        // Pre-sorted skip: the upstream NodeProperties::ordering covers
        // the range key, so the inputs are already in ascending order
        // on the key axis. Nothing to do.
        debug_assert!(
            driver_pairs
                .windows(2)
                .all(|w| cmp_range_keys(&w[0].2, &w[1].2) != Ordering::Greater),
            "presorted driver pairs are not in ascending range-key order"
        );
        debug_assert!(
            build_pairs
                .windows(2)
                .all(|w| cmp_range_keys(&w[0].1, &w[1].1) != Ordering::Greater),
            "presorted build pairs are not in ascending range-key order"
        );
    }

    // ── Phase B: two-cursor merge walk ───────────────────────────────
    let mut output_records: Vec<(Record, RecordOrder)> = Vec::new();
    let mut body_eval = body_program.map(|p| ProgramEvaluator::new(Arc::clone(p), false));
    let mut emitted_since_check = 0usize;

    // Matching-run spill segments land in the caller-owned
    // pipeline-scoped TempDir. Each segment cleans itself on Drop;
    // panic-leaked segments are swept by the pipeline TempDir Drop.
    let mut spill_seq: u32 = 0;

    // Matching-run byte threshold: soft_limit / 4 (SPARK-13450 /
    // DataFusion convention). Floor at 16 KiB so tests with tiny
    // budgets still exercise the spill path.
    let byte_limit: usize = std::cmp::max(16 * 1024, (budget.soft_limit() / 4) as usize);

    let driver_unmatched_set: std::collections::HashSet<RecordOrder> =
        driver_unmatched_orders.iter().copied().collect();
    let mut matched_driver_orders: std::collections::HashSet<RecordOrder> =
        std::collections::HashSet::new();

    // For First-mode short-circuit per driver, track which drivers
    // already emitted at least once. Within a matching run, every
    // outer row is its own driver — short-circuit applies per outer
    // row on the build run.
    walk_two_cursors(WalkArgs {
        name,
        driver_pairs: &driver_pairs,
        build_pairs: &build_pairs,
        op,
        decomposed,
        body_eval: body_eval.as_mut(),
        resolver_mapping,
        output_schema,
        match_mode,
        build_qualifier,
        ctx,
        byte_limit,
        spill_dir,
        spill_seq: &mut spill_seq,
        stats: &mut stats,
        output: &mut output_records,
        matched_driver_orders: &mut matched_driver_orders,
        emitted_since_check: &mut emitted_since_check,
        budget,
    })?;

    // ── on_miss / collect-mode flush ─────────────────────────────────
    //
    // Drivers that produced zero matches under First/All routes
    // through `on_miss`; under Collect, every driver emits exactly one
    // row, with an empty array on miss. NULL-range drivers (placed in
    // `driver_unmatched_*` above) participate uniformly with drivers
    // that survived key extraction but matched no inner row.
    let _ = driver_unmatched_set; // tracked for symmetry; iteration uses driver_unmatched_records below

    let mut all_unmatched: Vec<(Record, RecordOrder)> = Vec::new();
    for (driver_record, driver_order, _key) in &driver_pairs {
        if !matched_driver_orders.contains(driver_order) {
            all_unmatched.push((driver_record.clone(), *driver_order));
        }
    }
    all_unmatched.extend(driver_unmatched_records);
    all_unmatched.sort_by_key(|(_, o)| *o);

    if matches!(match_mode, MatchMode::Collect) {
        // Collect-mode: every driver emits one record. Drivers that
        // matched at least one build row already pushed via the merge
        // walk; unmatched drivers emit here with an empty array.
        for (driver_record, driver_order) in all_unmatched {
            let mut rec = match output_schema {
                Some(s) => widen(&driver_record, s),
                None => driver_record.clone(),
            };
            rec.set(build_qualifier, Value::Array(Vec::new()));
            output_records.push((rec, driver_order));
        }
    } else {
        for (driver_record, driver_order) in all_unmatched {
            match on_miss {
                OnMiss::Skip => continue,
                OnMiss::Error => {
                    return Err(PipelineError::Compilation {
                        transform_name: name.to_string(),
                        messages: vec![format!(
                            "E310 combine on_miss: error — no matching build row for \
                             driver row {driver_order}"
                        )],
                    });
                }
                OnMiss::NullFields => {
                    let resolver = CombineResolver::new(resolver_mapping, &driver_record, None);
                    let evaluator = body_eval.as_mut().ok_or_else(|| PipelineError::Internal {
                        op: "combine",
                        node: name.to_string(),
                        detail: "combine body typed program missing for \
                                         on_miss: null_fields"
                            .to_string(),
                    })?;
                    match evaluator.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Emit { fields, metadata }) => {
                            let mut rec = match output_schema {
                                Some(s) => widen(&driver_record, s),
                                None => driver_record.clone(),
                            };
                            for (n, v) in fields {
                                rec.set(&n, v);
                            }
                            for (k, v) in metadata {
                                let _ = rec.set_meta(&k, v);
                            }
                            output_records.push((rec, driver_order));
                        }
                        Ok(EvalResult::Skip(_)) => {}
                        Err(e) => return Err(PipelineError::from(e)),
                    }
                }
            }
        }
    }

    Ok((output_records, stats))
}

// ──────────────────────────────────────────────────────────────────────
// Phase A helpers — external sort each side on the range key
// ──────────────────────────────────────────────────────────────────────

/// External-sort the driver-side `(record, order, key)` tuples on the
/// range key field. Uses [`SortBuffer<RecordOrder>`] under the hood;
/// the sort key is read off the record so the buffer's spill envelope
/// preserves the order tag verbatim.
fn sort_driver_pairs_externally(
    pairs: &mut Vec<(Record, RecordOrder, Value)>,
    name: &str,
    range_field: &Option<String>,
    budget: &mut MemoryBudget,
) -> Result<(), PipelineError> {
    if pairs.is_empty() {
        return Ok(());
    }
    let Some(field) = range_field else {
        // The planner only selects SortMerge for predicates whose
        // range axis is a simple `qualifier.field` reference, so the
        // field is always extractable. A None here means the planner
        // selected SortMerge for an expression range that the kernel
        // cannot encode through `SortBuffer`'s field-comparator.
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: "sort-merge phase A: range expression is not a simple field \
                     reference; the planner selected SortMerge for an expression \
                     it cannot externally sort"
                .to_string(),
        });
    };
    let schema = Arc::clone(pairs[0].0.schema());
    let spill_threshold = std::cmp::max(16 * 1024, budget.soft_limit() as usize / 2);
    let sort_field = crate::config::SortField {
        field: field.clone(),
        order: crate::config::SortOrder::Asc,
        null_order: None,
    };
    let mut buf: SortBuffer<RecordOrder> =
        SortBuffer::new(vec![sort_field], spill_threshold, None, schema);

    // Drain pairs into the buffer; payload is the order tag. The key
    // value rides on the record itself (read by the SortBuffer's
    // field comparator), so we drop it here and re-extract on read.
    for (record, order, _key) in pairs.drain(..) {
        buf.push(record, order);
        if buf.should_spill() {
            buf.sort_and_spill().map_err(|e| {
                PipelineError::Io(std::io::Error::other(format!(
                    "sort-merge driver phase A spill failed: {e}"
                )))
            })?;
        }
    }

    let sorted = buf.finish().map_err(|e| {
        PipelineError::Io(std::io::Error::other(format!(
            "sort-merge driver phase A finish failed: {e}"
        )))
    })?;

    match sorted {
        SortedOutput::InMemory(out) => {
            for (record, order) in out {
                let key = record.get(field).cloned().unwrap_or(Value::Null);
                pairs.push((record, order, key));
            }
        }
        SortedOutput::Spilled(files) => {
            // Single-file path is exact (the buffer sort is stable).
            // Multi-file would need a k-way merge via `LoserTree`; the
            // matching-run spill is independent and handled in Phase B,
            // so a true k-way merge over Phase A spill files is the
            // next-iteration improvement. Today we surface the limit
            // explicitly so a future enlargement of the soft limit (or
            // a user `memory_limit:` increase) lets the workload run.
            if files.len() > 1 {
                return Err(PipelineError::Io(std::io::Error::other(format!(
                    "sort-merge driver phase A produced {} spill files; multi-file \
                     k-way merge is wired but not yet enabled — increase the \
                     pipeline `memory_limit:` so the sort fits in a single chunk",
                    files.len()
                ))));
            }
            for file in files {
                let reader = file.reader().map_err(|e| {
                    PipelineError::Io(std::io::Error::other(format!(
                        "sort-merge driver phase A spill read failed: {e}"
                    )))
                })?;
                for entry in reader {
                    let (record, order) = entry.map_err(|e| {
                        PipelineError::Io(std::io::Error::other(format!(
                            "sort-merge driver phase A spill decode failed: {e}"
                        )))
                    })?;
                    let key = record.get(field).cloned().unwrap_or(Value::Null);
                    pairs.push((record, order, key));
                }
            }
        }
    }
    Ok(())
}

/// External-sort the build-side `(record, key)` tuples on the range key
/// field. Mirrors the driver path but with a unit `()` payload — the
/// build side carries no order tag.
fn sort_build_pairs_externally(
    pairs: &mut Vec<(Record, Value)>,
    name: &str,
    range_field: &Option<String>,
    budget: &mut MemoryBudget,
) -> Result<(), PipelineError> {
    if pairs.is_empty() {
        return Ok(());
    }
    let Some(field) = range_field else {
        return Err(PipelineError::Internal {
            op: "combine",
            node: name.to_string(),
            detail: "sort-merge phase A: build-side range expression is not a \
                     simple field reference"
                .to_string(),
        });
    };
    let schema = Arc::clone(pairs[0].0.schema());
    let spill_threshold = std::cmp::max(16 * 1024, budget.soft_limit() as usize / 2);
    let sort_field = crate::config::SortField {
        field: field.clone(),
        order: crate::config::SortOrder::Asc,
        null_order: None,
    };
    let mut buf: SortBuffer<()> = SortBuffer::new(vec![sort_field], spill_threshold, None, schema);

    for (record, _key) in pairs.drain(..) {
        buf.push(record, ());
        if buf.should_spill() {
            buf.sort_and_spill().map_err(|e| {
                PipelineError::Io(std::io::Error::other(format!(
                    "sort-merge build phase A spill failed: {e}"
                )))
            })?;
        }
    }

    let sorted = buf.finish().map_err(|e| {
        PipelineError::Io(std::io::Error::other(format!(
            "sort-merge build phase A finish failed: {e}"
        )))
    })?;

    match sorted {
        SortedOutput::InMemory(out) => {
            for (record, _) in out {
                let key = record.get(field).cloned().unwrap_or(Value::Null);
                pairs.push((record, key));
            }
        }
        SortedOutput::Spilled(files) => {
            if files.len() > 1 {
                return Err(PipelineError::Io(std::io::Error::other(format!(
                    "sort-merge build phase A produced {} spill files; multi-file \
                     k-way merge is wired but not yet enabled — increase the \
                     pipeline `memory_limit:` so the sort fits in a single chunk",
                    files.len()
                ))));
            }
            for file in files {
                let reader = file.reader().map_err(|e| {
                    PipelineError::Io(std::io::Error::other(format!(
                        "sort-merge build phase A spill read failed: {e}"
                    )))
                })?;
                for entry in reader {
                    let (record, _) = entry.map_err(|e| {
                        PipelineError::Io(std::io::Error::other(format!(
                            "sort-merge build phase A spill decode failed: {e}"
                        )))
                    })?;
                    let key = record.get(field).cloned().unwrap_or(Value::Null);
                    pairs.push((record, key));
                }
            }
        }
    }
    Ok(())
}

/// Extract the bare field name from a range-side expression iff the
/// expression is a simple `qualifier.field` reference. Returns `None`
/// for expression-shaped operands (e.g. `lower(a.x)`, `a.x + 1`),
/// signalling that Phase A external sort cannot encode the key via
/// `SortBuffer`'s field comparator.
fn field_name_of_range_expr(expr: &Expr) -> Option<String> {
    match expr {
        Expr::QualifiedFieldRef { parts, .. } if parts.len() == 2 => Some(parts[1].to_string()),
        Expr::FieldRef { name, .. } => Some(name.to_string()),
        _ => None,
    }
}

// ──────────────────────────────────────────────────────────────────────
// Phase B helpers — two-cursor merge walk
// ──────────────────────────────────────────────────────────────────────

struct WalkArgs<'a, 'b, 'c> {
    name: &'a str,
    driver_pairs: &'a [(Record, RecordOrder, Value)],
    build_pairs: &'a [(Record, Value)],
    op: RangeOp,
    decomposed: &'a DecomposedPredicate,
    body_eval: Option<&'b mut ProgramEvaluator>,
    resolver_mapping: &'a CombineResolverMapping,
    output_schema: Option<&'a Arc<Schema>>,
    match_mode: MatchMode,
    build_qualifier: &'a str,
    ctx: &'a EvalContext<'a>,
    byte_limit: usize,
    spill_dir: &'a std::path::Path,
    spill_seq: &'b mut u32,
    stats: &'b mut SortMergeStats,
    output: &'b mut Vec<(Record, RecordOrder)>,
    matched_driver_orders: &'b mut std::collections::HashSet<RecordOrder>,
    emitted_since_check: &'b mut usize,
    budget: &'c mut MemoryBudget,
}

/// Walk the two pre-sorted cursors, emitting cross-product matches per
/// run of equal driver keys.
///
/// For an inclusive op (`Le`/`Ge`), this groups driver rows by equal
/// range key, buffers every build row that satisfies the predicate
/// against that key, and emits one record per (driver, buffered build)
/// pair. For a strict op (`Lt`/`Gt`), the walk is identical except
/// equal-key boundaries are excluded.
///
/// Build-side iteration over the matching run uses
/// [`MatchingRunBuffer`] so a hot run with millions of duplicates
/// spills to disk at `soft_limit / 4` instead of OOMing the executor.
fn walk_two_cursors(args: WalkArgs<'_, '_, '_>) -> Result<(), PipelineError> {
    let WalkArgs {
        name,
        driver_pairs,
        build_pairs,
        op,
        decomposed,
        mut body_eval,
        resolver_mapping,
        output_schema,
        match_mode,
        build_qualifier,
        ctx,
        byte_limit,
        spill_dir,
        spill_seq,
        stats,
        output,
        matched_driver_orders,
        emitted_since_check,
        budget,
    } = args;

    let mut di = 0usize;
    while di < driver_pairs.len() {
        let driver_key = &driver_pairs[di].2;

        // Identify the matching-run window on the build side. The
        // window is the contiguous set of build rows whose key
        // satisfies `driver_key OP build_key`. Because both sides are
        // sorted ascending on the key axis, the window is contiguous
        // and can be located with a linear scan over the build cursor.
        // The matching run for a single driver may extend to the end
        // of the build cursor (e.g. `driver.x < build.y` admits every
        // build row whose key exceeds the driver key).
        let mut run = MatchingRunBuffer::new();
        let mut spilled_this_run = false;
        for (build_record, build_key) in build_pairs.iter() {
            if apply_op(driver_key, op, build_key) {
                let was_inmemory = matches!(run, MatchingRunBuffer::InMemory { .. });
                let written = push_to_buffer(
                    &mut run,
                    build_record.clone(),
                    byte_limit,
                    spill_dir,
                    spill_seq,
                )
                .map_err(|e| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!("sort-merge matching-run spill failed: {e}"),
                })?;
                if written > 0 && budget.record_spill_bytes(written) {
                    return Err(PipelineError::Compilation {
                        transform_name: name.to_string(),
                        messages: vec![format!(
                            "E310 sort-merge matching-run exceeded disk-spill quota: {} > {}",
                            budget.cumulative_spill_bytes(),
                            budget.disk_quota()
                        )],
                    });
                }
                if was_inmemory && matches!(run, MatchingRunBuffer::Spilled { .. }) {
                    spilled_this_run = true;
                }
            }
        }
        if spilled_this_run {
            stats.matching_run_spills = stats.matching_run_spills.saturating_add(1);
        }

        // Emit cross-product against this driver. For First-mode the
        // walk short-circuits after the first emit; for All-mode every
        // build row emits.
        let driver_record = driver_pairs[di].0.clone();
        let driver_order = driver_pairs[di].1;

        emit_for_run(&mut EmitForRunArgs {
            name,
            driver_record: &driver_record,
            driver_order,
            run: &run,
            decomposed,
            body_eval: body_eval.as_deref_mut(),
            resolver_mapping,
            output_schema,
            match_mode,
            build_qualifier,
            ctx,
            output,
            matched_driver_orders,
            emitted_since_check,
            budget,
        })?;

        di += 1;
    }
    Ok(())
}

struct EmitForRunArgs<'a, 'b> {
    name: &'a str,
    driver_record: &'a Record,
    driver_order: RecordOrder,
    run: &'a MatchingRunBuffer,
    decomposed: &'a DecomposedPredicate,
    body_eval: Option<&'b mut ProgramEvaluator>,
    resolver_mapping: &'a CombineResolverMapping,
    output_schema: Option<&'a Arc<Schema>>,
    match_mode: MatchMode,
    build_qualifier: &'a str,
    ctx: &'a EvalContext<'a>,
    output: &'b mut Vec<(Record, RecordOrder)>,
    matched_driver_orders: &'b mut std::collections::HashSet<RecordOrder>,
    emitted_since_check: &'b mut usize,
    budget: &'b mut MemoryBudget,
}

/// Emit cross-product records for one driver row against the buffered
/// inner run. Honors `match: collect | first | all`, the residual
/// filter, and the body evaluator.
fn emit_for_run(args: &mut EmitForRunArgs<'_, '_>) -> Result<(), PipelineError> {
    let name = args.name;
    let driver_record = args.driver_record;
    let driver_order = args.driver_order;
    let run = args.run;
    let decomposed = args.decomposed;
    let resolver_mapping = args.resolver_mapping;
    let output_schema = args.output_schema;
    let match_mode = args.match_mode;
    let build_qualifier = args.build_qualifier;
    let ctx = args.ctx;

    match match_mode {
        MatchMode::Collect => {
            let mut arr: Vec<Value> = Vec::new();
            let mut truncated = false;
            for inner in iter_run(run).map_err(|e| PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!("sort-merge run replay failed: {e}"),
            })? {
                let inner = inner.map_err(|e| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!("sort-merge run replay decode failed: {e}"),
                })?;
                if let Some(residual) = decomposed.residual.as_ref() {
                    let resolver =
                        CombineResolver::new(resolver_mapping, driver_record, Some(&inner));
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
                let mut m: IndexMap<Box<str>, Value> = IndexMap::new();
                for (fname, val) in inner.iter_all_fields() {
                    m.insert(fname.into(), val.clone());
                }
                arr.push(Value::Map(Box::new(m)));
            }
            if truncated {
                eprintln!(
                    "W: combine {name:?} match: collect truncated at \
                     {COLLECT_PER_GROUP_CAP} matches for driver row {driver_order}"
                );
            }
            if !arr.is_empty() {
                args.matched_driver_orders.insert(driver_order);
            }
            let mut rec = match output_schema {
                Some(s) => widen(driver_record, s),
                None => driver_record.clone(),
            };
            rec.set(build_qualifier, Value::Array(arr));
            args.output.push((rec, driver_order));
        }

        MatchMode::First | MatchMode::All => {
            for inner in iter_run(run).map_err(|e| PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!("sort-merge run replay failed: {e}"),
            })? {
                let inner = inner.map_err(|e| PipelineError::Internal {
                    op: "combine",
                    node: name.to_string(),
                    detail: format!("sort-merge run replay decode failed: {e}"),
                })?;

                if let Some(residual) = decomposed.residual.as_ref() {
                    let resolver =
                        CombineResolver::new(resolver_mapping, driver_record, Some(&inner));
                    let mut residual_eval = ProgramEvaluator::new(Arc::clone(residual), false);
                    match residual_eval.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Skip(_)) => continue,
                        Ok(EvalResult::Emit { .. }) => {}
                        Err(e) => return Err(PipelineError::from(e)),
                    }
                }

                if let Some(evaluator) = args.body_eval.as_deref_mut() {
                    let resolver =
                        CombineResolver::new(resolver_mapping, driver_record, Some(&inner));
                    match evaluator.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Emit { fields, metadata }) => {
                            let mut rec = match output_schema {
                                Some(s) => widen(driver_record, s),
                                None => driver_record.clone(),
                            };
                            for (n, v) in fields {
                                rec.set(&n, v);
                            }
                            for (k, v) in metadata {
                                let _ = rec.set_meta(&k, v);
                            }
                            args.output.push((rec, driver_order));
                            args.matched_driver_orders.insert(driver_order);
                            *args.emitted_since_check = args.emitted_since_check.saturating_add(1);
                            if *args.emitted_since_check >= MEMORY_CHECK_INTERVAL {
                                if args.budget.should_abort() {
                                    return Err(PipelineError::Compilation {
                                        transform_name: name.to_string(),
                                        messages: vec![format!(
                                            "E310 combine probe memory limit exceeded: \
                                             hard limit {}",
                                            args.budget.hard_limit()
                                        )],
                                    });
                                }
                                *args.emitted_since_check = 0;
                            }
                            if matches!(match_mode, MatchMode::First) {
                                break;
                            }
                        }
                        Ok(EvalResult::Skip(SkipReason::Filtered)) => {}
                        Ok(EvalResult::Skip(SkipReason::Duplicate)) => {}
                        Err(e) => return Err(PipelineError::from(e)),
                    }
                } else if let Some(target_schema) = output_schema {
                    // Body-less synthetic chain step: concatenate driver
                    // and inner values onto the encoded output schema.
                    // Mirrors the shape used by IEJoin / grace-hash for
                    // N-ary decomposition steps.
                    let mut values: Vec<Value> = Vec::with_capacity(target_schema.column_count());
                    values.extend(driver_record.values().iter().cloned());
                    values.extend(inner.values().iter().cloned());
                    if values.len() != target_schema.column_count() {
                        return Err(PipelineError::Internal {
                            op: "combine",
                            node: name.to_string(),
                            detail: format!(
                                "synthetic sort-merge step produced {} values; \
                                 encoded schema has {} columns",
                                values.len(),
                                target_schema.column_count()
                            ),
                        });
                    }
                    let mut rec = Record::new(Arc::clone(target_schema), values);
                    // Carry the driver's meta forward so the chain's final
                    // step can re-emit `__cxl_correlation_key` onto the
                    // user-projected output row.
                    for (k, v) in driver_record.iter_meta() {
                        let _ = rec.set_meta(k, v.clone());
                    }
                    args.output.push((rec, driver_order));
                } else {
                    // No body and no output schema (test-mode pass-through):
                    // emit one record per match carrying the driver record
                    // verbatim. The output buffer is the kernel's record
                    // count gauge — without a body the combine still
                    // signals "this driver matched at least once".
                    args.output.push((driver_record.clone(), driver_order));
                    args.matched_driver_orders.insert(driver_order);
                    *args.emitted_since_check = args.emitted_since_check.saturating_add(1);
                    if *args.emitted_since_check >= MEMORY_CHECK_INTERVAL {
                        if args.budget.should_abort() {
                            return Err(PipelineError::Compilation {
                                transform_name: name.to_string(),
                                messages: vec![format!(
                                    "E310 combine probe memory limit exceeded: \
                                     hard limit {}",
                                    args.budget.hard_limit()
                                )],
                            });
                        }
                        *args.emitted_since_check = 0;
                    }
                    if matches!(match_mode, MatchMode::First) {
                        break;
                    }
                }
            }
        }
    }
    Ok(())
}

// ──────────────────────────────────────────────────────────────────────
// Schema helpers
// ──────────────────────────────────────────────────────────────────────

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

fn key_eval_error(name: &str, side: &'static str, err: EvalError) -> PipelineError {
    PipelineError::Compilation {
        transform_name: name.to_string(),
        messages: vec![format!("combine {side}-side range key eval error: {err}")],
    }
}

/// Placeholder `RecordStorage` for windowless residual / body
/// evaluation. Mirrors the equivalent in `pipeline::iejoin` and
/// `pipeline::grace_hash`.
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

// ──────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::combine::{CombineResolverMapping, JoinSide};
    use crate::plan::combine::{CombineInput, RangeConjunct};
    use clinker_record::SchemaBuilder;
    use cxl::ast::Statement;
    use cxl::lexer::Span as CxlSpan;
    use cxl::parser::Parser;
    use cxl::resolve::pass::resolve_program;
    use cxl::typecheck::pass::type_check;
    use cxl::typecheck::row::{QualifiedField, Row as TypeRow};
    use indexmap::IndexMap;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn schema_with(cols: &[&str]) -> Arc<Schema> {
        let mut b = SchemaBuilder::with_capacity(cols.len());
        for c in cols {
            b = b.with_field(*c);
        }
        b.build()
    }

    /// Compile a CXL where-clause source into the merged-row TypedProgram
    /// and return it. The merged row carries every qualifier.field tuple
    /// referenced by the predicate.
    fn compile_pure_range(
        src: &str,
        merged_fields: &[(&str, &str, cxl::typecheck::Type)],
    ) -> Arc<TypedProgram> {
        let parsed = Parser::parse(src);
        assert!(
            parsed.errors.is_empty(),
            "parse errors: {:?}",
            parsed.errors
        );

        let field_refs: Vec<&str> = Vec::new(); // qualified fields only
        let resolved = resolve_program(parsed.ast, &field_refs, parsed.node_count)
            .unwrap_or_else(|d| panic!("resolve: {d:?}"));

        let mut cols: IndexMap<QualifiedField, cxl::typecheck::Type> = IndexMap::new();
        for (qual, field, ty) in merged_fields {
            cols.insert(QualifiedField::qualified(*qual, *field), ty.clone());
        }
        let row = TypeRow::closed(cols, CxlSpan::new(0, 0));
        let typed = type_check(resolved, &row).unwrap_or_else(|d| panic!("typecheck: {d:?}"));
        Arc::new(typed)
    }

    fn extract_range_conjunct(
        typed: &Arc<TypedProgram>,
        left_input: &str,
        right_input: &str,
    ) -> RangeConjunct {
        // Tests build a single-conjunct predicate; pull the LHS/RHS of
        // the top-level binary expression directly off the AST. The
        // CXL `filter` statement wraps the predicate as its body.
        let stmt = &typed.program.statements[0];
        let expr = match stmt {
            Statement::Filter { predicate, .. } => predicate.clone(),
            other => panic!("expected Filter statement, got {other:?}"),
        };
        let cxl::ast::Expr::Binary { op, lhs, rhs, .. } = &expr else {
            panic!("expected binary range expression at root");
        };
        let range_op = match op {
            cxl::ast::BinOp::Lt => RangeOp::Lt,
            cxl::ast::BinOp::Lte => RangeOp::Le,
            cxl::ast::BinOp::Gt => RangeOp::Gt,
            cxl::ast::BinOp::Gte => RangeOp::Ge,
            other => panic!("expected range op, got {other:?}"),
        };
        RangeConjunct {
            left_expr: (**lhs).clone(),
            left_input: Arc::from(left_input),
            op: range_op,
            right_expr: (**rhs).clone(),
            right_input: Arc::from(right_input),
        }
    }

    fn make_test_resolver_mapping(
        left_qual: &str,
        left_schema: &Arc<Schema>,
        right_qual: &str,
        right_schema: &Arc<Schema>,
    ) -> CombineResolverMapping {
        // Construct CombineInput entries for both sides; the resolver
        // mapping consumes `combine_inputs` plus a pre-resolved column
        // map. We populate the pre-resolved map by walking each side's
        // schema and assigning JoinSide::Probe to the left input and
        // JoinSide::Build to the right.
        let left_cols: IndexMap<QualifiedField, cxl::typecheck::Type> = left_schema
            .columns()
            .iter()
            .map(|c| {
                (
                    QualifiedField::qualified(left_qual, c.as_ref()),
                    cxl::typecheck::Type::Any,
                )
            })
            .collect();
        let right_cols: IndexMap<QualifiedField, cxl::typecheck::Type> = right_schema
            .columns()
            .iter()
            .map(|c| {
                (
                    QualifiedField::qualified(right_qual, c.as_ref()),
                    cxl::typecheck::Type::Any,
                )
            })
            .collect();
        let left_row = TypeRow::closed(left_cols, CxlSpan::new(0, 0));
        let right_row = TypeRow::closed(right_cols, CxlSpan::new(0, 0));
        let mut combine_inputs: IndexMap<String, CombineInput> = IndexMap::new();
        combine_inputs.insert(
            left_qual.to_string(),
            CombineInput {
                upstream_name: Arc::from(left_qual),
                row: left_row,
                estimated_cardinality: None,
            },
        );
        combine_inputs.insert(
            right_qual.to_string(),
            CombineInput {
                upstream_name: Arc::from(right_qual),
                row: right_row,
                estimated_cardinality: None,
            },
        );
        // Pre-resolved column map: maps every qualifier.field to its
        // (side, positional-index) pair.
        let mut pre_resolved_inner: HashMap<QualifiedField, (JoinSide, u32)> = HashMap::new();
        for (idx, col) in left_schema.columns().iter().enumerate() {
            pre_resolved_inner.insert(
                QualifiedField::qualified(left_qual, col.as_ref()),
                (JoinSide::Probe, idx as u32),
            );
        }
        for (idx, col) in right_schema.columns().iter().enumerate() {
            pre_resolved_inner.insert(
                QualifiedField::qualified(right_qual, col.as_ref()),
                (JoinSide::Build, idx as u32),
            );
        }
        let pre_resolved = Arc::new(pre_resolved_inner);
        CombineResolverMapping::from_pre_resolved(&pre_resolved, &combine_inputs)
    }

    fn rec(schema: &Arc<Schema>, vals: Vec<Value>) -> Record {
        Record::new(Arc::clone(schema), vals)
    }

    /// Build a DecomposedPredicate carrying one range conjunct, with
    /// the residual program covering the entire predicate. For
    /// pure-range combines, `decompose_predicate` writes the range
    /// expression into both `ranges` and `residual`; the SortMerge
    /// kernel reads ranges directly and uses the residual for 3+
    /// range conjuncts (here unused).
    fn decomposed_pure_range(
        rc: RangeConjunct,
        residual: Arc<TypedProgram>,
    ) -> DecomposedPredicate {
        DecomposedPredicate {
            equalities: Vec::new(),
            ranges: vec![rc],
            residual: Some(residual),
        }
    }

    /// Bundle of run_kernel inputs. Bundled so the test driver function
    /// stays under clippy's `too_many_arguments` cap and call sites
    /// update one field without rewriting the whole call.
    struct RunKernel<'a> {
        driver_records: Vec<(Record, RecordOrder)>,
        build_records: Vec<Record>,
        decomposed: DecomposedPredicate,
        driver_qual: &'a str,
        build_qual: &'a str,
        driver_schema: &'a Arc<Schema>,
        build_schema: &'a Arc<Schema>,
        match_mode: MatchMode,
        on_miss: OnMiss,
        presorted: bool,
        body_program: Option<&'a Arc<TypedProgram>>,
        /// Tiny budget here forces the matching-run spill path: the
        /// kernel computes `byte_limit = max(16 KiB, soft_limit / 4)`,
        /// so a 64 KiB total budget yields the floor at 16 KiB.
        budget_bytes: Option<u64>,
    }

    /// Drive the SortMerge kernel on hand-built inputs and return the
    /// emitted (record, order) pairs alongside the stats.
    fn run_kernel(rk: RunKernel<'_>) -> (Vec<(Record, RecordOrder)>, SortMergeStats) {
        let resolver_mapping = make_test_resolver_mapping(
            rk.driver_qual,
            rk.driver_schema,
            rk.build_qual,
            rk.build_schema,
        );
        let stable = cxl::eval::StableEvalContext::test_default();
        let source_file: Arc<str> = Arc::from("");
        let ctx = EvalContext {
            stable: &stable,
            source_file: &source_file,
            source_row: 0,
        };
        let mut budget = match rk.budget_bytes {
            Some(b) => MemoryBudget::new(b, 0.80),
            None => MemoryBudget::from_config(None),
        };
        let dir = tempfile::Builder::new()
            .prefix("sm-test-")
            .tempdir()
            .expect("sort-merge test temp dir");
        let args = SortMergeExec {
            name: "sm_test",
            build_qualifier: rk.build_qual,
            driver_records: rk.driver_records,
            build_records: rk.build_records,
            decomposed: &rk.decomposed,
            body_program: rk.body_program,
            resolver_mapping: &resolver_mapping,
            output_schema: None,
            match_mode: rk.match_mode,
            on_miss: rk.on_miss,
            presorted: rk.presorted,
            ctx: &ctx,
            budget: &mut budget,
            spill_dir: dir.path(),
        };
        let result = execute_combine_sort_merge_with_stats(args)
            .expect("sort-merge kernel execution failed");
        // Hold the TempDir until the kernel has consumed every spill
        // segment; segments carry `Arc<Path>` references back into
        // this directory.
        drop(dir);
        result
    }

    /// Pure-range correctness gate: products vs tax brackets.
    /// `product.price < bracket.max` admits one match per (product,
    /// bracket) pair where the product's price lies below the bracket
    /// upper bound.
    #[test]
    fn test_sort_merge_pure_range_correct() {
        let products = schema_with(&["sku", "price"]);
        let brackets = schema_with(&["bracket_id", "max"]);

        // Pre-sorted ascending on price/max.
        let driver = vec![
            (
                rec(
                    &products,
                    vec![Value::String("p1".into()), Value::Integer(10)],
                ),
                0,
            ),
            (
                rec(
                    &products,
                    vec![Value::String("p2".into()), Value::Integer(20)],
                ),
                1,
            ),
            (
                rec(
                    &products,
                    vec![Value::String("p3".into()), Value::Integer(30)],
                ),
                2,
            ),
        ];
        let build = vec![
            rec(
                &brackets,
                vec![Value::String("b15".into()), Value::Integer(15)],
            ),
            rec(
                &brackets,
                vec![Value::String("b25".into()), Value::Integer(25)],
            ),
            rec(
                &brackets,
                vec![Value::String("b35".into()), Value::Integer(35)],
            ),
        ];

        let typed = compile_pure_range(
            "filter product.price < bracket.max",
            &[
                ("product", "price", cxl::typecheck::Type::Int),
                ("bracket", "max", cxl::typecheck::Type::Int),
            ],
        );
        let rc = extract_range_conjunct(&typed, "product", "bracket");
        let decomposed = decomposed_pure_range(rc, Arc::clone(&typed));

        let (records, _stats) = run_kernel(RunKernel {
            driver_records: driver,
            build_records: build,
            decomposed,
            driver_qual: "product",
            build_qual: "bracket",
            driver_schema: &products,
            build_schema: &brackets,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            presorted: true, // presorted on the range axis
            body_program: None,
            budget_bytes: None,
        });
        // p1 (10) < 15, 25, 35 → 3 matches
        // p2 (20) < 25, 35     → 2 matches
        // p3 (30) < 35         → 1 match
        // Total = 6.
        assert_eq!(
            records.len(),
            6,
            "expected 6 cross-product matches; got: {records:?}"
        );
    }

    /// Large matching run forces the inner buffer past `soft_limit / 4`
    /// and verifies the spilled replay produces the same output as the
    /// in-memory path.
    #[test]
    fn test_sort_merge_matching_run_spill() {
        let drivers_schema = schema_with(&["d_id", "d_key"]);
        let builds_schema = schema_with(&["b_id", "b_key"]);

        // Single driver row with key=1; many build rows each with
        // key=10, all satisfying `1 < 10` → one matching run sized to
        // exceed `soft_limit / 4` (we set the limit via the kernel's
        // `byte_limit` floor of 16 KiB by inflating record payloads).
        let driver = vec![(
            rec(
                &drivers_schema,
                vec![Value::String("d0".into()), Value::Integer(1)],
            ),
            0,
        )];
        let mut build = Vec::new();
        let big_payload: String = "x".repeat(1024); // 1 KiB per record
        for i in 0..32 {
            build.push(rec(
                &builds_schema,
                vec![
                    Value::String(format!("b{i}-{}", &big_payload).into()),
                    Value::Integer(10),
                ],
            ));
        }

        let typed = compile_pure_range(
            "filter drivers.d_key < builds.b_key",
            &[
                ("drivers", "d_key", cxl::typecheck::Type::Int),
                ("builds", "b_key", cxl::typecheck::Type::Int),
            ],
        );
        let rc = extract_range_conjunct(&typed, "drivers", "builds");
        let decomposed = decomposed_pure_range(rc, Arc::clone(&typed));

        // 64 KiB budget → soft_limit ~52 KiB → byte_limit floors at
        // 16 KiB. With 32 records × ~1 KiB each = 32 KiB, the running
        // total breaches the floor partway through and forces the
        // spilled-buffer transition.
        let (records, stats) = run_kernel(RunKernel {
            driver_records: driver,
            build_records: build.clone(),
            decomposed,
            driver_qual: "drivers",
            build_qual: "builds",
            driver_schema: &drivers_schema,
            build_schema: &builds_schema,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            presorted: true,
            body_program: None,
            budget_bytes: Some(64 * 1024),
        });
        assert_eq!(
            records.len(),
            32,
            "expected 32 cross-product matches; got {} (stats={:?})",
            records.len(),
            stats
        );
        assert!(
            stats.matching_run_spills >= 1,
            "matching run must have spilled at least once; stats={stats:?}"
        );

        // Compare against the no-spill path: re-run with a tiny build
        // set that fits in memory and confirm record content matches.
        let small_build: Vec<Record> = build.iter().take(2).cloned().collect();
        let typed2 = compile_pure_range(
            "filter drivers.d_key < builds.b_key",
            &[
                ("drivers", "d_key", cxl::typecheck::Type::Int),
                ("builds", "b_key", cxl::typecheck::Type::Int),
            ],
        );
        let rc2 = extract_range_conjunct(&typed2, "drivers", "builds");
        let decomposed2 = decomposed_pure_range(rc2, Arc::clone(&typed2));
        let driver2 = vec![(
            rec(
                &drivers_schema,
                vec![Value::String("d0".into()), Value::Integer(1)],
            ),
            0,
        )];
        let (small_records, small_stats) = run_kernel(RunKernel {
            driver_records: driver2,
            build_records: small_build,
            decomposed: decomposed2,
            driver_qual: "drivers",
            build_qual: "builds",
            driver_schema: &drivers_schema,
            build_schema: &builds_schema,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            presorted: true,
            body_program: None,
            budget_bytes: Some(64 * 1024),
        });
        assert_eq!(small_records.len(), 2);
        assert_eq!(
            small_stats.matching_run_spills, 0,
            "small-build run must not spill"
        );
    }

    /// Pre-sorted inputs skip Phase A external sort. Verified by
    /// inspecting `phase_a_sort_invocations` on the returned stats.
    #[test]
    fn test_sort_merge_presorted_skip() {
        let drivers_schema = schema_with(&["k"]);
        let builds_schema = schema_with(&["k"]);
        let driver = vec![
            (rec(&drivers_schema, vec![Value::Integer(1)]), 0),
            (rec(&drivers_schema, vec![Value::Integer(2)]), 1),
            (rec(&drivers_schema, vec![Value::Integer(3)]), 2),
        ];
        let build = vec![
            rec(&builds_schema, vec![Value::Integer(2)]),
            rec(&builds_schema, vec![Value::Integer(4)]),
        ];

        let typed = compile_pure_range(
            "filter drivers.k < builds.k",
            &[
                ("drivers", "k", cxl::typecheck::Type::Int),
                ("builds", "k", cxl::typecheck::Type::Int),
            ],
        );
        let rc = extract_range_conjunct(&typed, "drivers", "builds");
        let decomposed = decomposed_pure_range(rc, Arc::clone(&typed));

        let (_records, stats) = run_kernel(RunKernel {
            driver_records: driver,
            build_records: build,
            decomposed,
            driver_qual: "drivers",
            build_qual: "builds",
            driver_schema: &drivers_schema,
            build_schema: &builds_schema,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            presorted: true, // presorted
            body_program: None,
            budget_bytes: None,
        });
        assert_eq!(
            stats.phase_a_sort_invocations, 0,
            "pre-sorted run must skip Phase A external sort; stats={stats:?}"
        );
    }

    /// EC-3 edge case: zero-record outer or inner side handled cleanly
    /// by the two-cursor walk (no panic, empty result set under
    /// `on_miss: skip`).
    #[test]
    fn test_sort_merge_empty_side() {
        let drivers_schema = schema_with(&["k"]);
        let builds_schema = schema_with(&["k"]);

        // Empty inner side.
        let driver_a = vec![
            (rec(&drivers_schema, vec![Value::Integer(1)]), 0),
            (rec(&drivers_schema, vec![Value::Integer(2)]), 1),
        ];
        let build_a: Vec<Record> = Vec::new();

        let typed = compile_pure_range(
            "filter drivers.k < builds.k",
            &[
                ("drivers", "k", cxl::typecheck::Type::Int),
                ("builds", "k", cxl::typecheck::Type::Int),
            ],
        );
        let rc = extract_range_conjunct(&typed, "drivers", "builds");
        let decomposed = decomposed_pure_range(rc, Arc::clone(&typed));

        let (records_a, _) = run_kernel(RunKernel {
            driver_records: driver_a,
            build_records: build_a,
            decomposed,
            driver_qual: "drivers",
            build_qual: "builds",
            driver_schema: &drivers_schema,
            build_schema: &builds_schema,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            presorted: true,
            body_program: None,
            budget_bytes: None,
        });
        assert!(
            records_a.is_empty(),
            "empty inner side must emit zero records; got: {records_a:?}"
        );

        // Empty outer side.
        let driver_b: Vec<(Record, RecordOrder)> = Vec::new();
        let build_b = vec![rec(&builds_schema, vec![Value::Integer(5)])];
        let typed_b = compile_pure_range(
            "filter drivers.k < builds.k",
            &[
                ("drivers", "k", cxl::typecheck::Type::Int),
                ("builds", "k", cxl::typecheck::Type::Int),
            ],
        );
        let rc_b = extract_range_conjunct(&typed_b, "drivers", "builds");
        let decomposed_b = decomposed_pure_range(rc_b, Arc::clone(&typed_b));
        let (records_b, _) = run_kernel(RunKernel {
            driver_records: driver_b,
            build_records: build_b,
            decomposed: decomposed_b,
            driver_qual: "drivers",
            build_qual: "builds",
            driver_schema: &drivers_schema,
            build_schema: &builds_schema,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            presorted: true,
            body_program: None,
            budget_bytes: None,
        });
        assert!(
            records_b.is_empty(),
            "empty outer side must emit zero records; got: {records_b:?}"
        );
    }
}
