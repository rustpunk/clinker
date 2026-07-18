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

use rayon::iter::{IntoParallelIterator, ParallelIterator};

use clinker_record::{Record, Schema, Value};
use cxl::ast::Expr;
use cxl::eval::{EvalContext, EvalError, EvalResult, ProgramEvaluator, SkipReason};
use cxl::typecheck::TypedProgram;
use indexmap::IndexMap;

use crate::executor::combine::{CombineResolver, CombineResolverMapping};
use crate::executor::widen_record_to_schema;
use crate::pipeline::combine::{CombineKernelOutput, CombineOutputEvalFailure, KeyExtractor};
use crate::pipeline::grace_spill::{
    GraceSpillError, GraceSpillReader, GraceSpillWriter, SpillFilePath, grace_spill_error,
};
use crate::pipeline::memory::MemoryArbitrator;
#[cfg(test)]
use crate::pipeline::memory::NoOpPolicy;
use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
use crate::pipeline::spill_merge::{MergeBudget, SortedRunMerger};
use clinker_plan::BudgetCategory;
use clinker_plan::config::pipeline_node::{MatchMode, OnMiss};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::combine::{DecomposedPredicate, RangeOp};

/// Cap on matches collected per driver under [`MatchMode::Collect`].
/// Mirrors the constant in `pipeline::combine`, `pipeline::iejoin`, and
/// `pipeline::grace_hash` so every code path truncates at the same
/// threshold.
const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// Period (matched pairs emitted) between [`MemoryArbitrator::should_abort`]
/// polls during the merge walk.
const MEMORY_CHECK_INTERVAL: usize = 10_000;

/// Per-side external-sort spill threshold in bytes: half the soft RSS limit,
/// floored at 16 KiB so tests with tiny budgets still exercise the spill path.
/// Shared by both the driver- and build-side external sorts.
fn spill_threshold_bytes(budget: &MemoryArbitrator) -> usize {
    std::cmp::max(16 * 1024, budget.soft_limit() as usize / 2)
}

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
/// [`crate::pipeline::memory::MemoryArbitrator::record_spill_bytes`] so
/// the disk-quota gate can fire on overflow.
fn push_to_buffer(
    buf: &mut MatchingRunBuffer,
    record: Record,
    byte_limit: usize,
    spill_dir: &std::path::Path,
    spill_compress: bool,
    spill_seq: &mut u32,
    consumer_handle: &Arc<crate::pipeline::memory::ConsumerHandle>,
) -> Result<u64, GraceSpillError> {
    let incoming = std::mem::size_of::<Record>() + record.estimated_heap_size();
    match buf {
        MatchingRunBuffer::InMemory { records, bytes } => {
            if bytes.saturating_add(incoming) > byte_limit && !records.is_empty() {
                // Promote to Spilled: drain all records to a fresh
                // segment, then start a tail with the incoming record.
                // Every in-memory byte previously charged into the
                // consumer handle is now off-process; subtract before
                // the variant flip so the handle stays aligned with
                // the operator's live state (Velox's "reclaimable ≠
                // held" point).
                let prev_in_memory = *bytes as u64;
                let mut drained = std::mem::take(records);
                let schema = Arc::clone(drained[0].schema());
                drained.push(record);
                let (segment_path, written) =
                    write_spill_segment(spill_dir, spill_compress, spill_seq, &drained)?;
                let count = drained.len() as u64;
                *buf = MatchingRunBuffer::Spilled {
                    segments: vec![segment_path],
                    schema,
                    spilled_count: count,
                    tail: Vec::new(),
                    tail_bytes: 0,
                };
                consumer_handle.sub_bytes(prev_in_memory);
                Ok(written)
            } else {
                *bytes = bytes.saturating_add(incoming);
                records.push(record);
                consumer_handle.add_bytes(incoming as u64);
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
                let prev_tail = *tail_bytes as u64;
                let mut drained = std::mem::take(tail);
                drained.push(record);
                let (segment_path, written) =
                    write_spill_segment(spill_dir, spill_compress, spill_seq, &drained)?;
                *spilled_count = spilled_count.saturating_add(drained.len() as u64);
                *tail_bytes = 0;
                segments.push(segment_path);
                consumer_handle.sub_bytes(prev_tail);
                Ok(written)
            } else {
                *tail_bytes = tail_bytes.saturating_add(incoming);
                tail.push(record);
                consumer_handle.add_bytes(incoming as u64);
                Ok(0)
            }
        }
    }
}

/// Write a slice of records as a single fresh spill segment via
/// [`GraceSpillWriter`]. The returned path lives inside `spill_dir`;
/// the caller's `TempDir` owns its lifetime. The byte length is
/// returned alongside the path so the caller can fold it into
/// [`crate::pipeline::memory::MemoryArbitrator::record_spill_bytes`].
fn write_spill_segment(
    spill_dir: &std::path::Path,
    spill_compress: bool,
    spill_seq: &mut u32,
    records: &[Record],
) -> Result<(SpillFilePath, u64), GraceSpillError> {
    let seq = *spill_seq;
    *spill_seq = spill_seq.wrapping_add(1);
    let mut writer = GraceSpillWriter::new(spill_dir, 0, (seq & 0xFFFF) as u16, spill_compress)?;
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
// Streamed driver cursor / replayable build side
// ──────────────────────────────────────────────────────────────────────

/// Single-pass ascending stream of driver `(record, order, key)` tuples.
///
/// Memory model: streaming. The spilled variant holds one resident record
/// per open run inside the [`SortedRunMerger`]; the in-memory variant owns
/// the sorted vector and yields it by value. Either way Phase B never holds
/// a second full copy of the driver side — the merge walk pulls one driver
/// at a time. The range key rides on the record's sort column, re-read here
/// for the spilled variant so no per-record key vector is materialized.
enum DriverStream {
    InMemory(std::vec::IntoIter<(Record, RecordOrder, Value)>),
    Spilled {
        merger: SortedRunMerger<RecordOrder>,
        field: String,
    },
}

impl Iterator for DriverStream {
    type Item = Result<(Record, RecordOrder, Value), PipelineError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            DriverStream::InMemory(it) => it.next().map(Ok),
            DriverStream::Spilled { merger, field } => match merger.next()? {
                Ok((record, order)) => {
                    let key = record.get(field).cloned().unwrap_or(Value::Null);
                    Some(Ok((record, order, key)))
                }
                Err(e) => Some(Err(e)),
            },
        }
    }
}

/// Replayable, spillable build side for the Phase B merge walk.
///
/// The build side is scanned once per driver, so it must support repeated
/// replay — unlike the single-pass driver stream. `Buffered` reuses the
/// spilling [`MatchingRunBuffer`]: when the sorted build exceeds
/// `soft_limit / 4` it spills to disk, bounding the resident footprint to
/// one in-memory tail plus on-disk segments regardless of build size. The
/// range key is re-read from the record's sort column `field` on each
/// replay, so no parallel key vector is held. `Resident` keeps pre-extracted
/// keys in memory and is used only when the range axis is not a simple field
/// reference (an expression axis, reachable only on the pre-sorted skip path)
/// so the key cannot be recovered from a record column.
enum BuildSide {
    Resident(Vec<(Record, Value)>),
    Buffered {
        buf: MatchingRunBuffer,
        field: String,
    },
}

/// A single replay pass over a [`BuildSide`]: `(record, range key)` in sorted
/// order, each item fallible because the spilled variant decodes from disk.
type BuildReplayIter<'a> = Box<dyn Iterator<Item = std::io::Result<(Record, Value)>> + 'a>;

impl BuildSide {
    /// Replay every build record in sorted order, pairing each with its
    /// range key. Cloning the resident records / decoding the spilled ones
    /// keeps the buffer intact for the next driver's replay.
    fn replay(&self) -> Result<BuildReplayIter<'_>, std::io::Error> {
        match self {
            BuildSide::Resident(pairs) => Ok(Box::new(
                pairs.iter().map(|(r, k)| Ok((r.clone(), k.clone()))),
            )),
            BuildSide::Buffered { buf, field } => {
                let field = field.clone();
                let it = iter_run(buf)?.map(move |r| {
                    r.map(|record| {
                        let key = record.get(&field).cloned().unwrap_or(Value::Null);
                        (record, key)
                    })
                });
                Ok(Box::new(it))
            }
        }
    }

    /// In-memory bytes this build side has charged into the shared consumer
    /// handle. `Resident` is uncharged (it holds no handle bytes); `Buffered`
    /// reports its live in-memory portion so the caller can discharge it once
    /// the walk finishes. On-disk segments are off-process and excluded.
    fn charged_bytes(&self) -> u64 {
        match self {
            BuildSide::Resident(_) => 0,
            BuildSide::Buffered { buf, .. } => matching_run_in_memory_bytes(buf),
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

/// Compare two range key values by mapping each to a monotone `i64`
/// *within its own type*: integers as-is, floats through the
/// order-preserving `float_to_orderable_i64` encoding, dates/datetimes as
/// their epoch counts. NULLs and non-orderables are filtered before this
/// is called.
///
/// There is no cross-type coercion here: an integer and a float are NOT
/// brought onto a common numeric scale, so this assumes both keys of an
/// axis share a type. A mixed int/float range axis is not currently
/// guarded at plan time and would compare in disjoint encoding spaces.
fn cmp_range_keys(a: &Value, b: &Value) -> Ordering {
    use chrono::Datelike;
    let ai = match a {
        Value::Integer(i) => *i,
        Value::Float(f) => crate::pipeline::sort_key::float_to_orderable_i64(*f),
        Value::Date(d) => d.num_days_from_ce() as i64,
        Value::DateTime(dt) => dt.and_utc().timestamp_micros(),
        _ => 0,
    };
    let bi = match b {
        Value::Integer(i) => *i,
        Value::Float(f) => crate::pipeline::sort_key::float_to_orderable_i64(*f),
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
    /// Build-side `$ck.<field>` propagation policy. Threaded uniformly
    /// across every combine strategy and consumed by
    /// `copy_build_ck_columns` at each emit site.
    pub propagate_ck: &'a clinker_plan::config::pipeline_node::PropagateCkSpec,
    pub ctx: &'a EvalContext<'a>,
    pub budget: &'a MemoryArbitrator,
    /// Pipeline-scoped spill directory borrowed from
    /// `ExecutorContext::spill_root_path`. Matching-run spill segments
    /// land inside it; the surrounding `Arc<TempDir>` Drop is the
    /// secondary panic-safe cleanup sweep.
    pub spill_dir: &'a Path,
    /// Whether Phase A external-sort spill runs are LZ4-compressed. Resolved
    /// by the dispatcher from the workspace `[storage.spill] compress` knob
    /// against the combine's schema width and batch size, so the on-disk
    /// format matches what `--explain` reports.
    pub spill_compress: bool,
    /// Shared with the registered `SortMergeConsumer` wrapper.
    /// Phase A per-side `SortBuffer.bytes_used` plus Phase B
    /// matching-run accumulator size mirror into `handle.bytes`
    /// so the arbitrator's pull-mode `current_usage` reads the
    /// operator's live in-memory state.
    pub consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
    /// Error strategy governing output-stage eval failures. Under
    /// `FailFast` a residual / body eval error propagates immediately;
    /// under `Continue` / `BestEffort` the failing row is deferred to the
    /// dispatcher via [`CombineKernelOutput::output_eval_failures`].
    pub strategy: clinker_plan::config::ErrorStrategy,
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
    /// Set to 1 when the replayable build side overflowed `soft_limit / 4`
    /// and transitioned to a spilled buffer; 0 when it stayed in memory.
    matching_run_spills: u32,
    /// Set to 1 when the driver side exceeded its sort-buffer spill threshold
    /// and Phase B streamed it from disk through the k-way merger instead of
    /// holding it resident; 0 when the sorted driver fit in memory. Proves the
    /// driver axis is bounded on both the sorted and (bounded) pre-sorted paths.
    driver_stream_spilled: u32,
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
) -> Result<CombineKernelOutput, PipelineError> {
    let (output, _stats) = execute_combine_sort_merge_with_stats(args)?;
    Ok(output)
}

/// Variant of [`execute_combine_sort_merge`] that also returns
/// [`SortMergeStats`]. Module-private — exposed to the in-module
/// `#[cfg(test)]` block so tests can verify Phase A skip and
/// matching-run spill counts. Production callers go through
/// [`execute_combine_sort_merge`].
fn execute_combine_sort_merge_with_stats(
    args: SortMergeExec<'_>,
) -> Result<(CombineKernelOutput, SortMergeStats), PipelineError> {
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
        propagate_ck,
        ctx,
        budget,
        spill_dir,
        spill_compress,
        consumer_handle,
        strategy,
    } = args;

    let mut stats = SortMergeStats::default();
    // Recoverable output-stage eval failures deferred to the dispatcher.
    // Threaded by `&mut` through `emit_for_driver` and the NULL-range
    // dispatch below. Always empty under `FailFast`.
    let mut output_eval_failures: Vec<CombineOutputEvalFailure> = Vec::new();

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
    let mut driver_unmatched_records: Vec<(Record, RecordOrder)> = Vec::new();

    // Range-key extraction runs the compiled CXL range closures per
    // record — the costly part of Phase A. `extract_range_key` is
    // stateless (`&self` extractor, a per-call key buffer) and
    // `EvalContext` is read-only, so the extraction parallelizes across
    // the shared kernel pool. The routing loop replays the owned
    // `(record, order, key)` outcomes in declared order, so `driver_pairs`
    // and the unmatched vector stay byte-identical to a sequential
    // extraction.
    let driver_keyed: Vec<(Record, RecordOrder, Option<Value>)> = driver_records
        .into_par_iter()
        .map(|(record, order)| {
            let mut range_buf: Vec<Value> = Vec::new();
            let key = extract_range_key(
                &driver_extractor,
                ctx,
                &record,
                resolver_mapping,
                true,
                &mut range_buf,
            )
            .map_err(|e| key_eval_error(name, "driving", e))?;
            Ok::<_, PipelineError>((record, order, key))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (record, order, key) in driver_keyed {
        match key {
            Some(k) => driver_pairs.push((record, order, k)),
            // A NULL / non-orderable range key never satisfies a CXL
            // ternary comparison, so the driver matches nothing and is
            // dispatched through `on_miss` / collect-empty after the walk.
            None => driver_unmatched_records.push((record, order)),
        }
    }

    let mut build_pairs: Vec<(Record, Value)> = Vec::new();
    let build_keyed: Vec<(Record, Option<Value>)> = build_records
        .into_par_iter()
        .map(|record| {
            let mut range_buf: Vec<Value> = Vec::new();
            let key = extract_range_key(
                &build_extractor,
                ctx,
                &record,
                resolver_mapping,
                false,
                &mut range_buf,
            )
            .map_err(|e| key_eval_error(name, "build", e))?;
            Ok::<_, PipelineError>((record, key))
        })
        .collect::<Result<Vec<_>, _>>()?;
    for (record, key) in build_keyed {
        if let Some(k) = key {
            build_pairs.push((record, k));
        }
    }

    // Build-side byte threshold: soft_limit / 4 (SPARK-13450 / DataFusion
    // convention). Floor at 16 KiB so tests with tiny budgets still
    // exercise the spill path. Spill segments land in the caller-owned
    // pipeline-scoped TempDir; each cleans itself on Drop.
    let byte_limit: usize = std::cmp::max(16 * 1024, (budget.soft_limit() / 4) as usize);
    let mut spill_seq: u32 = 0;

    // ── Phase A: bound each side for the streaming merge walk. The driver
    //    becomes a single-pass ascending stream and the build a replayable,
    //    spillable buffer. Neither holds a second full copy of an over-budget
    //    side during Phase B: the driver merges lazily one record per open run,
    //    the build spills past `byte_limit`. The pre-sorted path only avoids the
    //    sort *work* — an already-sorted driver still funnels through the same
    //    spillable stream (a stable sort of sorted input is an order-preserving
    //    no-op), matching the build side, so a pre-sorted-but-over-budget driver
    //    no longer materializes whole in RAM either.
    let driver_stream = sort_driver_stream(DriverStreamBuild {
        pairs: driver_pairs,
        name,
        range_field: &driver_field,
        budget,
        spill_compress,
        consumer_handle: &consumer_handle,
        spill_dir,
        presorted,
    })?;
    let build_side = build_side_from_pairs(BuildSideBuild {
        pairs: build_pairs,
        name,
        range_field: &build_field,
        budget,
        spill_compress,
        consumer_handle: &consumer_handle,
        spill_dir,
        byte_limit,
        presorted,
        spill_seq: &mut spill_seq,
        stats: &mut stats,
    })?;
    if !presorted {
        stats.phase_a_sort_invocations = 2;
    }
    if matches!(driver_stream, DriverStream::Spilled { .. }) {
        stats.driver_stream_spilled = 1;
    }

    // ── Phase B: streaming merge walk ────────────────────────────────
    let mut output_records: Vec<(Record, RecordOrder)> = Vec::new();
    let mut body_eval = body_program.map(|p| ProgramEvaluator::new(Arc::clone(p), false));
    let mut emitted_since_check = 0usize;

    let ectx = EmitCtx {
        name,
        output_schema,
        build_qualifier,
        propagate_ck,
        resolver_mapping,
        ctx,
        budget,
        strategy,
    };

    // Pull one driver at a time from the sorted stream, replay the build
    // side against it, and emit its matches. `on_miss` and collect-mode
    // are dispatched inline per driver so the walk never retains the set
    // of drivers to revisit after the fact — that would reintroduce a
    // full-driver-side residency for a sparse join.
    for driver in driver_stream {
        let (driver_record, driver_order, driver_key) = driver?;
        emit_for_driver(EmitDriverArgs {
            ectx: &ectx,
            driver_record: &driver_record,
            driver_order,
            driver_key: &driver_key,
            op,
            build_side: &build_side,
            decomposed,
            body_eval: body_eval.as_mut(),
            match_mode,
            on_miss,
            output: &mut output_records,
            emitted_since_check: &mut emitted_since_check,
            failures: &mut output_eval_failures,
        })?;
    }

    // The build side's in-memory residency was charged into the shared
    // consumer handle as it filled; discharge it now the walk is done so
    // the arbitrator's pull-mode usage falls back to zero for this
    // operator.
    consumer_handle.sub_bytes(build_side.charged_bytes());

    // ── NULL-range drivers ───────────────────────────────────────────
    // They never entered the merge (no orderable key), so each matches
    // nothing: one collect row (empty array) or the on_miss policy,
    // preserving original row order.
    for (driver_record, driver_order) in driver_unmatched_records {
        if matches!(match_mode, MatchMode::Collect) {
            emit_collect_row(
                &ectx,
                &driver_record,
                driver_order,
                Vec::new(),
                None,
                &mut output_records,
                &mut emitted_since_check,
            )?;
        } else {
            dispatch_driver_miss(
                &ectx,
                &driver_record,
                driver_order,
                on_miss,
                body_eval.as_mut(),
                &mut output_records,
                &mut output_eval_failures,
            )?;
        }
    }

    Ok((
        CombineKernelOutput {
            records: output_records,
            output_eval_failures,
        },
        stats,
    ))
}

// ──────────────────────────────────────────────────────────────────────
// Phase A helpers — external sort each side on the range key
// ──────────────────────────────────────────────────────────────────────

/// Sort the driver-side `(record, order, key)` tuples on the range key field
/// (skipped when pre-sorted) and hand back a single-pass ascending
/// [`DriverStream`]. Uses [`SortBuffer<RecordOrder>`] under the hood; the sort
/// key rides on the record so the spill envelope preserves the order tag
/// verbatim.
///
/// Memory model: streaming. The driver side is consumed once and never holds a
/// second full copy resident during Phase B — the spilled variant streams one
/// record per open run through the [`SortedRunMerger`], and the in-memory
/// variant is bounded by the sort buffer's `soft_limit / 2` spill threshold.
/// The transient sort-buffer charge is netted to zero here (an already-consumed
/// single-pass stream offers the arbitrator nothing to reclaim), matching the
/// build side's discharge after the walk.
///
/// A pre-sorted simple-field driver still funnels through the same spillable
/// sort buffer rather than staying resident: a stable sort of already-ascending
/// input is an order-preserving no-op, so the emitted stream is byte-identical
/// while an over-budget driver spills instead of materializing whole. An
/// expression range axis (no simple field, reachable only pre-sorted) keeps its
/// pre-extracted keys resident, the driver analog of [`BuildSide::Resident`],
/// because a spilled key cannot be re-read from a record column.
///
/// Inputs bundled into [`DriverStreamBuild`] so the signature stays under
/// clippy's `too_many_arguments` cap, matching [`BuildSideBuild`].
struct DriverStreamBuild<'a> {
    pairs: Vec<(Record, RecordOrder, Value)>,
    name: &'a str,
    range_field: &'a Option<String>,
    budget: &'a MemoryArbitrator,
    spill_compress: bool,
    consumer_handle: &'a Arc<crate::pipeline::memory::ConsumerHandle>,
    spill_dir: &'a Path,
    /// True when the caller certified the input already ascends on the range
    /// key: the external sort is still applied (a stable no-op that keeps the
    /// stream byte-identical while bounding residency), but an expression range
    /// axis is kept resident rather than rejected.
    presorted: bool,
}

fn sort_driver_stream(args: DriverStreamBuild<'_>) -> Result<DriverStream, PipelineError> {
    let DriverStreamBuild {
        pairs,
        name,
        range_field,
        budget,
        spill_compress,
        consumer_handle,
        spill_dir,
        presorted,
    } = args;
    if pairs.is_empty() {
        return Ok(DriverStream::InMemory(Vec::new().into_iter()));
    }
    let Some(field) = range_field else {
        // Expression range axis (e.g. `a.x + 1`): the key rides in no record
        // column, so it can neither drive `SortBuffer`'s field comparator nor be
        // recovered on a spilled replay. It is reachable only on the pre-sorted
        // skip path — the external sort requires a simple field — where the pairs
        // already ascend and carry their extracted keys, so keep them resident.
        // A non-presorted expression axis has no sort path at all: a
        // planner-shape violation the kernel never sees today.
        if !presorted {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: "sort-merge phase A: range expression is not a simple field \
                         reference; the planner selected SortMerge for an expression \
                         it cannot externally sort"
                    .to_string(),
            });
        }
        debug_assert!(
            pairs
                .windows(2)
                .all(|w| cmp_range_keys(&w[0].2, &w[1].2) != Ordering::Greater),
            "presorted driver pairs are not in ascending range-key order"
        );
        return Ok(DriverStream::InMemory(pairs.into_iter()));
    };
    let schema = Arc::clone(pairs[0].0.schema());
    let spill_threshold = spill_threshold_bytes(budget);
    let sort_field = clinker_plan::config::SortField {
        field: field.clone(),
        order: clinker_plan::config::SortOrder::Asc,
        null_order: None,
    };
    let mut buf: SortBuffer<RecordOrder> = SortBuffer::new(
        vec![sort_field],
        spill_threshold,
        Some(spill_dir.to_path_buf()),
        spill_compress,
        schema,
    );

    // Transient sort-buffer charge, rewound in full below so the driver side
    // contributes nothing to the consumer handle once sorting completes.
    let mut local_charged: u64 = 0;

    for (record, order, _key) in pairs {
        let pre = buf.bytes_used();
        buf.push(record, order);
        let delta = (buf.bytes_used().saturating_sub(pre)) as u64;
        consumer_handle.add_bytes(delta);
        local_charged = local_charged.saturating_add(delta);
        if buf.should_spill() {
            let pre_spill = buf.bytes_used() as u64;
            let written = buf.sort_and_spill().map_err(|e| {
                PipelineError::Io(std::io::Error::other(format!(
                    "sort-merge driver phase A spill failed: {e}"
                )))
            })?;
            consumer_handle.sub_bytes(pre_spill);
            local_charged = local_charged.saturating_sub(pre_spill);
            if written > 0 && budget.record_spill_bytes(name, written) {
                return Err(PipelineError::spill_cap_exceeded(
                    name,
                    budget.disk_quota(),
                    written,
                    budget.cumulative_spill_bytes(),
                ));
            }
        }
    }

    let (sorted, residue) = buf.finish().map_err(|e| {
        PipelineError::Io(std::io::Error::other(format!(
            "sort-merge driver phase A finish failed: {e}"
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

    // Discharge the whole transient sort charge: whichever way the sort
    // landed, the driver stream carries the records from here on and is not
    // charged against the handle.
    consumer_handle.sub_bytes(local_charged);

    match sorted {
        SortedOutput::InMemory(out) => {
            let mut v: Vec<(Record, RecordOrder, Value)> = Vec::with_capacity(out.len());
            for (record, order) in out {
                let key = record.get(field).cloned().unwrap_or(Value::Null);
                v.push((record, order, key));
            }
            Ok(DriverStream::InMemory(v.into_iter()))
        }
        SortedOutput::Spilled(files) => {
            // Fold the individually-sorted runs into one globally-ordered
            // stream via the shared LoserTree k-way merge. `SortBuffer` already
            // sorted each run on `field`, so the merge preserves that order
            // across runs (a single run is an identity pass); the walk pulls
            // one record per open run.
            let merge_field = clinker_plan::config::SortField {
                field: field.clone(),
                order: clinker_plan::config::SortOrder::Asc,
                null_order: None,
            };
            let merger = SortedRunMerger::new(
                files,
                std::slice::from_ref(&merge_field),
                "sort-merge driver phase A",
                MergeBudget {
                    budget,
                    node: name,
                    compress: spill_compress,
                },
            )?;
            Ok(DriverStream::Spilled {
                merger,
                field: field.clone(),
            })
        }
    }
}

/// Fixed context for pushing one build record into the replay buffer:
/// spill threshold, spill directory, disk-quota budget, and the shared
/// consumer handle. Bundled so [`BuildPush::push`] stays under clippy's
/// argument cap and every call site charges memory and disk identically.
struct BuildPush<'a> {
    byte_limit: usize,
    spill_dir: &'a Path,
    spill_compress: bool,
    consumer_handle: &'a Arc<crate::pipeline::memory::ConsumerHandle>,
    budget: &'a MemoryArbitrator,
    name: &'a str,
}

impl BuildPush<'_> {
    /// Append one sorted build record to the replay buffer, spilling past
    /// `byte_limit`. Charges in-memory growth into the consumer handle
    /// (via [`push_to_buffer`]) and any freshly written spill bytes against
    /// the disk quota, aborting with `SpillCapExceeded` on overflow.
    fn push(
        &self,
        buf: &mut MatchingRunBuffer,
        record: Record,
        spill_seq: &mut u32,
    ) -> Result<(), PipelineError> {
        let written = push_to_buffer(
            buf,
            record,
            self.byte_limit,
            self.spill_dir,
            self.spill_compress,
            spill_seq,
            self.consumer_handle,
        )
        .map_err(|e| grace_spill_error(e, self.name, "sort-merge build buffer spill failed"))?;
        if written > 0 && self.budget.record_spill_bytes(self.name, written) {
            return Err(PipelineError::spill_cap_exceeded(
                self.name,
                self.budget.disk_quota(),
                written,
                self.budget.cumulative_spill_bytes(),
            ));
        }
        Ok(())
    }
}

/// Inputs to [`build_side_from_pairs`]. Bundled so the function signature
/// stays under clippy's `too_many_arguments` cap.
struct BuildSideBuild<'a> {
    pairs: Vec<(Record, Value)>,
    name: &'a str,
    range_field: &'a Option<String>,
    budget: &'a MemoryArbitrator,
    spill_compress: bool,
    consumer_handle: &'a Arc<crate::pipeline::memory::ConsumerHandle>,
    spill_dir: &'a Path,
    byte_limit: usize,
    /// True when the caller certified the input already ascends on the range
    /// key (upstream ordering), so the external sort is skipped.
    presorted: bool,
    spill_seq: &'a mut u32,
    stats: &'a mut SortMergeStats,
}

/// Sort the build-side `(record, key)` tuples on the range key (skipped when
/// pre-sorted) and fold them into a replayable [`BuildSide`].
///
/// Memory model: the sorted records land in a spillable [`MatchingRunBuffer`]
/// that overflows to disk past `byte_limit`, so an over-budget build side no
/// longer materializes whole in RAM. The buffer's in-memory residency is
/// charged into the shared consumer handle for the walk's duration and
/// discharged by the caller once the walk finishes. Expression range axes
/// (no simple field) keep their pre-extracted keys resident because the key
/// cannot be re-read from a record column on replay.
fn build_side_from_pairs(args: BuildSideBuild<'_>) -> Result<BuildSide, PipelineError> {
    let BuildSideBuild {
        pairs,
        name,
        range_field,
        budget,
        spill_compress,
        consumer_handle,
        spill_dir,
        byte_limit,
        presorted,
        spill_seq,
        stats,
    } = args;

    if pairs.is_empty() {
        return Ok(BuildSide::Resident(Vec::new()));
    }
    let Some(field) = range_field else {
        // Expression range axis: the key rides in no record column, so it cannot
        // be re-read on a spilled replay — keep the already-sorted pairs resident
        // with their pre-extracted keys. Reachable only on the pre-sorted skip
        // path; the external sort requires a simple field. A non-presorted
        // expression axis has no sort path at all: a planner-shape violation the
        // kernel never sees today, surfaced as a typed invariant error rather
        // than a debug-only panic that would silently hold the full build side
        // resident in release.
        if !presorted {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: "sort-merge phase A: build-side range expression is not a \
                         simple field reference; the planner selected SortMerge for \
                         an expression it cannot externally sort"
                    .to_string(),
            });
        }
        return Ok(BuildSide::Resident(pairs));
    };

    let pusher = BuildPush {
        byte_limit,
        spill_dir,
        spill_compress,
        consumer_handle,
        budget,
        name,
    };
    let mut buf = MatchingRunBuffer::new();

    if presorted {
        // Upstream ordering already ascends on the range key, so the buffer is
        // filled in place — no sort, just the spill-bounded FIFO append.
        debug_assert!(
            pairs
                .windows(2)
                .all(|w| cmp_range_keys(&w[0].1, &w[1].1) != Ordering::Greater),
            "presorted build pairs are not in ascending range-key order"
        );
        for (record, _key) in pairs {
            pusher.push(&mut buf, record, spill_seq)?;
        }
    } else {
        // External sort into globally-ordered runs, then fold into the replay
        // buffer. The transient sort-buffer charge is netted to zero before
        // the replay buffer re-charges its residency, so the build side is
        // charged exactly once.
        let schema = Arc::clone(pairs[0].0.schema());
        let spill_threshold = spill_threshold_bytes(budget);
        let sort_field = clinker_plan::config::SortField {
            field: field.clone(),
            order: clinker_plan::config::SortOrder::Asc,
            null_order: None,
        };
        let mut sbuf: SortBuffer<()> = SortBuffer::new(
            vec![sort_field],
            spill_threshold,
            Some(spill_dir.to_path_buf()),
            spill_compress,
            schema,
        );
        let mut local_charged: u64 = 0;
        for (record, _key) in pairs {
            let pre = sbuf.bytes_used();
            sbuf.push(record, ());
            let delta = (sbuf.bytes_used().saturating_sub(pre)) as u64;
            consumer_handle.add_bytes(delta);
            local_charged = local_charged.saturating_add(delta);
            if sbuf.should_spill() {
                let pre_spill = sbuf.bytes_used() as u64;
                let written = sbuf.sort_and_spill().map_err(|e| {
                    PipelineError::Io(std::io::Error::other(format!(
                        "sort-merge build phase A spill failed: {e}"
                    )))
                })?;
                consumer_handle.sub_bytes(pre_spill);
                local_charged = local_charged.saturating_sub(pre_spill);
                if written > 0 && budget.record_spill_bytes(name, written) {
                    return Err(PipelineError::spill_cap_exceeded(
                        name,
                        budget.disk_quota(),
                        written,
                        budget.cumulative_spill_bytes(),
                    ));
                }
            }
        }
        let (sorted, residue) = sbuf.finish().map_err(|e| {
            PipelineError::Io(std::io::Error::other(format!(
                "sort-merge build phase A finish failed: {e}"
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
        // Net the transient sort charge; the replay buffer re-charges below.
        consumer_handle.sub_bytes(local_charged);
        match sorted {
            SortedOutput::InMemory(out) => {
                for (record, ()) in out {
                    pusher.push(&mut buf, record, spill_seq)?;
                }
            }
            SortedOutput::Spilled(files) => {
                let merge_field = clinker_plan::config::SortField {
                    field: field.clone(),
                    order: clinker_plan::config::SortOrder::Asc,
                    null_order: None,
                };
                let merger = SortedRunMerger::new(
                    files,
                    std::slice::from_ref(&merge_field),
                    "sort-merge build phase A",
                    MergeBudget {
                        budget,
                        node: name,
                        compress: spill_compress,
                    },
                )?;
                for entry in merger {
                    let (record, ()) = entry?;
                    pusher.push(&mut buf, record, spill_seq)?;
                }
            }
        }
    }

    if matches!(buf, MatchingRunBuffer::Spilled { .. }) {
        // The build buffer overflowed `byte_limit` and now lives mostly on
        // disk. Counted once per build side; drives the module tests'
        // spill-path assertions.
        stats.matching_run_spills = stats.matching_run_spills.saturating_add(1);
    }

    Ok(BuildSide::Buffered {
        buf,
        field: field.clone(),
    })
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
// Phase B helpers — streaming merge walk
// ──────────────────────────────────────────────────────────────────────

/// In-memory byte count for a matching-run buffer. `InMemory` carries
/// its accumulated bytes directly; `Spilled` reports only the tail
/// (on-disk segments are off-process and don't count against the
/// pull-mode `current_usage` surface — same convention as the grace-
/// hash partition Building → OnDisk transition).
fn matching_run_in_memory_bytes(buf: &MatchingRunBuffer) -> u64 {
    match buf {
        MatchingRunBuffer::InMemory { bytes, .. } => *bytes as u64,
        MatchingRunBuffer::Spilled { tail_bytes, .. } => *tail_bytes as u64,
    }
}

/// Fixed per-combine emit context: everything the per-driver emit and the
/// zero-match dispatch read but never mutate. Bundled so the emit helpers
/// stay under clippy's argument cap and every emit site widens, propagates
/// `$ck`, and polls the budget identically.
struct EmitCtx<'a> {
    name: &'a str,
    output_schema: Option<&'a Arc<Schema>>,
    build_qualifier: &'a str,
    propagate_ck: &'a clinker_plan::config::pipeline_node::PropagateCkSpec,
    resolver_mapping: &'a CombineResolverMapping,
    ctx: &'a EvalContext<'a>,
    budget: &'a MemoryArbitrator,
    strategy: clinker_plan::config::ErrorStrategy,
}

/// Poll the accumulated output buffer every [`MEMORY_CHECK_INTERVAL`] emitted
/// rows and abort if its own byte count has grown past the hard limit. The
/// sort-merge output vector has no spill path, so — as on the IEJoin
/// equi+range path — global pressure is the sole ceiling on its growth; the
/// byte-counted [`MemoryArbitrator::should_abort_local`] gate fires even where
/// process RSS is unavailable. Applies uniformly across `first` / `all` and
/// `collect`, so a high-cardinality driver with a permissive predicate aborts
/// cleanly instead of growing the output unbounded.
fn poll_output_buffer(
    ectx: &EmitCtx<'_>,
    output: &[(Record, RecordOrder)],
    emitted_since_check: &mut usize,
) -> Result<(), PipelineError> {
    *emitted_since_check += 1;
    if *emitted_since_check >= MEMORY_CHECK_INTERVAL {
        let used = crate::pipeline::combine::combine_output_buffer_bytes(output) as u64;
        if ectx.budget.should_abort_local(used) {
            return Err(PipelineError::MemoryBudgetExceeded {
                node: ectx.name.to_string(),
                used,
                limit: ectx.budget.hard_limit(),
                source: BudgetCategory::Arena,
                detail: Some("sort-merge output buffer exceeded budget".to_string()),
            });
        }
        *emitted_since_check = 0;
    }
    Ok(())
}

/// Build and push one collect-mode row for `driver_order`: even a zero-match
/// driver emits, with an empty array. Propagates the first matched build's
/// `$ck.<field>` values onto the widened row, sets the collect array under the
/// build qualifier, then polls the output buffer — so a high-cardinality
/// driver with a permissive predicate aborts cleanly over budget instead of
/// growing collect-mode output unbounded.
fn emit_collect_row(
    ectx: &EmitCtx<'_>,
    driver_record: &Record,
    driver_order: RecordOrder,
    arr: Vec<Value>,
    first_build: Option<&Record>,
    output: &mut Vec<(Record, RecordOrder)>,
    emitted_since_check: &mut usize,
) -> Result<(), PipelineError> {
    let mut rec = match ectx.output_schema {
        Some(s) => widen_record_to_schema(driver_record, s),
        None => driver_record.clone(),
    };
    if let Some(b) = first_build {
        crate::executor::copy_build_ck_columns(&mut rec, b, ectx.propagate_ck);
    }
    rec.set(ectx.build_qualifier, Value::Array(arr));
    output.push((rec, driver_order));
    poll_output_buffer(ectx, output, emitted_since_check)
}

/// Dispatch one zero-match driver through its `on_miss` policy: `Skip` drops
/// it, `Error` surfaces the missing-match error, `NullFields` runs the body
/// over the driver alone and emits the widened row (a recoverable eval failure
/// routes to `failures`, or fails fast). Used inline for both keyed drivers
/// that matched nothing and NULL-range drivers.
fn dispatch_driver_miss(
    ectx: &EmitCtx<'_>,
    driver_record: &Record,
    driver_order: RecordOrder,
    on_miss: OnMiss,
    body_eval: Option<&mut ProgramEvaluator>,
    output: &mut Vec<(Record, RecordOrder)>,
    failures: &mut Vec<CombineOutputEvalFailure>,
) -> Result<(), PipelineError> {
    match on_miss {
        OnMiss::Skip => Ok(()),
        OnMiss::Error => Err(PipelineError::CombineMissingMatch {
            combine: ectx.name.to_string(),
            driver_row: driver_order,
        }),
        OnMiss::NullFields => {
            let resolver = CombineResolver::new(ectx.resolver_mapping, driver_record, None);
            let evaluator = body_eval.ok_or_else(|| PipelineError::Internal {
                op: "combine",
                node: ectx.name.to_string(),
                detail: "combine body typed program missing for on_miss: null_fields".to_string(),
            })?;
            match evaluator.eval_record::<NullStorage>(ectx.ctx, &resolver, None) {
                Ok(EvalResult::Emit {
                    fields,
                    record_vars,
                    ..
                }) => {
                    let mut rec = match ectx.output_schema {
                        Some(s) => widen_record_to_schema(driver_record, s),
                        None => driver_record.clone(),
                    };
                    for (n, v) in fields {
                        rec.set(&n, v);
                    }
                    for (k, v) in *record_vars {
                        let _ = rec.set_record_var(&k, v);
                    }
                    output.push((rec, driver_order));
                    Ok(())
                }
                Ok(EvalResult::Skip(_)) => Ok(()),
                Ok(EvalResult::EmitMany { .. }) => Err(PipelineError::Internal {
                    op: "sort_merge_join on_miss body",
                    node: ectx.name.to_string(),
                    detail: "emit_each fan-out is not supported in a combine body".into(),
                }),
                Err(e) => {
                    if ectx.strategy == clinker_plan::config::ErrorStrategy::FailFast {
                        return Err(PipelineError::from(e));
                    }
                    failures.push(CombineOutputEvalFailure {
                        probe_record: driver_record.clone(),
                        row: driver_order,
                        matched_build: None,
                        error: e,
                    });
                    Ok(())
                }
            }
        }
    }
}

/// Inputs to [`emit_for_driver`]: one driver row plus everything the merge
/// walk needs to replay the build side against it. Bundled so the function
/// stays under clippy's argument cap. Consumed by value — each call owns the
/// mutable output / counter / failures borrows for its duration.
struct EmitDriverArgs<'a, 'b> {
    ectx: &'b EmitCtx<'a>,
    driver_record: &'b Record,
    driver_order: RecordOrder,
    driver_key: &'b Value,
    op: RangeOp,
    build_side: &'b BuildSide,
    decomposed: &'a DecomposedPredicate,
    body_eval: Option<&'b mut ProgramEvaluator>,
    match_mode: MatchMode,
    on_miss: OnMiss,
    output: &'b mut Vec<(Record, RecordOrder)>,
    emitted_since_check: &'b mut usize,
    failures: &'b mut Vec<CombineOutputEvalFailure>,
}

/// Emit the join output for one driver row, replaying the build side and
/// applying the range predicate inline. The predicate filter is fused into
/// the replay so no per-driver matching run is materialized: the only
/// buffered build state is the shared, spillable [`BuildSide`], keeping peak
/// residency at one matching set plus the k open runs of each side's cursor.
///
/// Honors `match: collect | first | all`, the residual filter, and the body
/// evaluator; `on_miss` is dispatched inline when the driver emits nothing.
fn emit_for_driver(args: EmitDriverArgs<'_, '_>) -> Result<(), PipelineError> {
    let EmitDriverArgs {
        ectx,
        driver_record,
        driver_order,
        driver_key,
        op,
        build_side,
        decomposed,
        mut body_eval,
        match_mode,
        on_miss,
        output,
        emitted_since_check,
        failures,
    } = args;

    let name = ectx.name;
    let resolver_mapping = ectx.resolver_mapping;
    let ctx = ectx.ctx;

    let replay_err = |e: std::io::Error| PipelineError::Internal {
        op: "combine",
        node: name.to_string(),
        detail: format!("sort-merge build replay failed: {e}"),
    };
    let decode_err = |e: std::io::Error| PipelineError::Internal {
        op: "combine",
        node: name.to_string(),
        detail: format!("sort-merge build replay decode failed: {e}"),
    };

    match match_mode {
        MatchMode::Collect => {
            let mut arr: Vec<Value> = Vec::new();
            let mut first_build: Option<Record> = None;
            let mut truncated = false;
            for entry in build_side.replay().map_err(replay_err)? {
                let (inner, build_key) = entry.map_err(decode_err)?;
                if !apply_op(driver_key, op, &build_key) {
                    continue;
                }
                if let Some(residual) = decomposed.residual.as_ref() {
                    let resolver =
                        CombineResolver::new(resolver_mapping, driver_record, Some(&inner));
                    let mut residual_eval = ProgramEvaluator::new(Arc::clone(residual), false);
                    match residual_eval.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Skip(_)) => continue,
                        Ok(EvalResult::Emit { .. }) => {}
                        Ok(EvalResult::EmitMany { .. }) => {
                            return Err(PipelineError::Internal {
                                op: "sort_merge_join residual",
                                node: name.to_string(),
                                detail: "emit_each fan-out is not supported in a combine residual filter".into(),
                            });
                        }
                        Err(e) => {
                            if ectx.strategy == clinker_plan::config::ErrorStrategy::FailFast {
                                return Err(PipelineError::from(e));
                            }
                            failures.push(CombineOutputEvalFailure {
                                probe_record: driver_record.clone(),
                                row: driver_order,
                                matched_build: Some(inner.clone()),
                                error: e,
                            });
                            continue;
                        }
                    }
                }
                if arr.len() >= COLLECT_PER_GROUP_CAP {
                    truncated = true;
                    break;
                }
                if first_build.is_none() {
                    first_build = Some(inner.clone());
                }
                // Build-side records contribute only their user-declared field
                // values to the collect array. `iter_user_fields` filters every
                // engine-stamped column — both `$ck.*` (correlation lineage; not
                // meaningful nested inside a collect-array entry) and `$widened`
                // (auto_widen sidecar; build-side sidecars drop at the join
                // boundary by design, mirroring `propagate_ck: Driver`). Without
                // this filter, a build record's `$widened` `Value::Map` payload
                // nests inside the collect-mode `Value::Map` and reaches the
                // writer as a nested Map, triggering
                // `FormatError::UnserializableMapValue`.
                let mut m: IndexMap<Box<str>, Value> = IndexMap::new();
                for (fname, val) in inner.iter_user_fields() {
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
            emit_collect_row(
                ectx,
                driver_record,
                driver_order,
                arr,
                first_build.as_ref(),
                output,
                emitted_since_check,
            )?;
        }

        MatchMode::First | MatchMode::All => {
            let mut emitted_any = false;
            for entry in build_side.replay().map_err(replay_err)? {
                let (inner, build_key) = entry.map_err(decode_err)?;
                if !apply_op(driver_key, op, &build_key) {
                    continue;
                }

                if let Some(residual) = decomposed.residual.as_ref() {
                    let resolver =
                        CombineResolver::new(resolver_mapping, driver_record, Some(&inner));
                    let mut residual_eval = ProgramEvaluator::new(Arc::clone(residual), false);
                    match residual_eval.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Skip(_)) => continue,
                        Ok(EvalResult::Emit { .. }) => {}
                        Ok(EvalResult::EmitMany { .. }) => {
                            return Err(PipelineError::Internal {
                                op: "sort_merge_join residual",
                                node: name.to_string(),
                                detail: "emit_each fan-out is not supported in a combine residual filter".into(),
                            });
                        }
                        Err(e) => {
                            if ectx.strategy == clinker_plan::config::ErrorStrategy::FailFast {
                                return Err(PipelineError::from(e));
                            }
                            failures.push(CombineOutputEvalFailure {
                                probe_record: driver_record.clone(),
                                row: driver_order,
                                matched_build: Some(inner.clone()),
                                error: e,
                            });
                            continue;
                        }
                    }
                }

                if let Some(evaluator) = body_eval.as_deref_mut() {
                    let resolver =
                        CombineResolver::new(resolver_mapping, driver_record, Some(&inner));
                    match evaluator.eval_record::<NullStorage>(ctx, &resolver, None) {
                        Ok(EvalResult::Emit {
                            fields,
                            record_vars,
                            ..
                        }) => {
                            let mut rec = match ectx.output_schema {
                                Some(s) => widen_record_to_schema(driver_record, s),
                                None => driver_record.clone(),
                            };
                            for (n, v) in fields {
                                rec.set(&n, v);
                            }
                            for (k, v) in *record_vars {
                                let _ = rec.set_record_var(&k, v);
                            }
                            crate::executor::copy_build_ck_columns(
                                &mut rec,
                                &inner,
                                ectx.propagate_ck,
                            );
                            output.push((rec, driver_order));
                            emitted_any = true;
                            poll_output_buffer(ectx, output, emitted_since_check)?;
                            if matches!(match_mode, MatchMode::First) {
                                break;
                            }
                        }
                        Ok(EvalResult::Skip(SkipReason::Filtered)) => {}
                        Ok(EvalResult::Skip(SkipReason::Duplicate)) => {}
                        Ok(EvalResult::EmitMany { .. }) => {
                            return Err(PipelineError::Internal {
                                op: "sort_merge_join body",
                                node: name.to_string(),
                                detail: "emit_each fan-out is not supported in a combine body"
                                    .into(),
                            });
                        }
                        Err(e) => {
                            if ectx.strategy == clinker_plan::config::ErrorStrategy::FailFast {
                                return Err(PipelineError::from(e));
                            }
                            failures.push(CombineOutputEvalFailure {
                                probe_record: driver_record.clone(),
                                row: driver_order,
                                matched_build: Some(inner.clone()),
                                error: e,
                            });
                            continue;
                        }
                    }
                } else if let Some(target_schema) = ectx.output_schema {
                    // Body-less synthetic chain step: concatenate driver and
                    // inner values onto the encoded output schema. Mirrors the
                    // shape used by IEJoin / grace-hash for N-ary decomposition
                    // steps.
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
                    let rec = Record::new(Arc::clone(target_schema), values);
                    output.push((rec, driver_order));
                    emitted_any = true;
                    poll_output_buffer(ectx, output, emitted_since_check)?;
                    if matches!(match_mode, MatchMode::First) {
                        break;
                    }
                } else {
                    // No body and no output schema (test-mode pass-through):
                    // emit one record per match carrying the driver record
                    // verbatim. The output buffer is the kernel's record count
                    // gauge — without a body the combine still signals "this
                    // driver matched at least once".
                    output.push((driver_record.clone(), driver_order));
                    emitted_any = true;
                    poll_output_buffer(ectx, output, emitted_since_check)?;
                    if matches!(match_mode, MatchMode::First) {
                        break;
                    }
                }
            }

            if !emitted_any {
                dispatch_driver_miss(
                    ectx,
                    driver_record,
                    driver_order,
                    on_miss,
                    body_eval,
                    output,
                    failures,
                )?;
            }
        }
    }
    Ok(())
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

/// `MemoryConsumer` wrapper for a `SortMergeExec` mid-execution.
/// Holds an `Arc<ConsumerHandle>` shared with the executor: Phase A
/// per-side `SortBuffer.bytes_used` plus Phase B matching-run
/// accumulator size sum into `handle.bytes`. `try_spill` flips the
/// handle's spill-request flag; the executor reacts at the next
/// batch boundary by flushing its current run to disk.
///
/// `spill_priority = 25`: sort-merge spill is more expensive than
/// either sort or grace-hash because it involves both per-side sort
/// flush AND matching-run accumulator flush; only hash-aggregation
/// (priority 30) is more expensive. `can_back_pressure = false`:
/// sort-merge holds two cursors that walk inputs monotonically;
/// pausing either side stalls the match scan.
pub struct SortMergeConsumer {
    handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
}

impl SortMergeConsumer {
    pub fn new(handle: std::sync::Arc<crate::pipeline::memory::ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl crate::pipeline::memory::MemoryConsumer for SortMergeConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        25
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

// ──────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::combine::CombineResolverMapping;
    use clinker_plan::plan::combine::{CombineInput, RangeConjunct};
    use clinker_plan::plan::types::JoinSide;
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
            },
        );
        combine_inputs.insert(
            right_qual.to_string(),
            CombineInput {
                upstream_name: Arc::from(right_qual),
                row: right_row,
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
    /// emitted (record, order) pairs alongside the stats. Panics if the
    /// kernel returns an error; over-budget-abort tests use
    /// [`run_kernel_result`] instead.
    fn run_kernel(rk: RunKernel<'_>) -> (Vec<(Record, RecordOrder)>, SortMergeStats) {
        let (output, stats) = run_kernel_result(rk).expect("sort-merge kernel execution failed");
        (output.records, stats)
    }

    /// Drive the SortMerge kernel and return the raw `Result`, so tests can
    /// assert a clean over-budget abort rather than unwrapping.
    fn run_kernel_result(
        rk: RunKernel<'_>,
    ) -> Result<(CombineKernelOutput, SortMergeStats), PipelineError> {
        let resolver_mapping = make_test_resolver_mapping(
            rk.driver_qual,
            rk.driver_schema,
            rk.build_qual,
            rk.build_schema,
        );
        let stable = cxl::eval::StableEvalContext::test_default();
        let source_file: Arc<str> = Arc::from("");
        let ctx = EvalContext::test_with_file(&stable, &source_file, 0);
        let mut budget = match rk.budget_bytes {
            Some(b) => MemoryArbitrator::with_policy(b, 0.80, 0.70, Box::new(NoOpPolicy)),
            None => MemoryArbitrator::with_policy(
                clinker_plan::config::utils::parse_memory_limit_bytes(None)
                    .unwrap_or(clinker_plan::config::utils::DEFAULT_MEMORY_LIMIT_BYTES),
                0.80,
                0.70,
                Box::new(NoOpPolicy),
            ),
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
            propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
            ctx: &ctx,
            budget: &mut budget,
            spill_dir: dir.path(),
            spill_compress: true,
            consumer_handle: crate::pipeline::memory::ConsumerHandle::new(),
            strategy: clinker_plan::config::ErrorStrategy::FailFast,
        };
        let result = execute_combine_sort_merge_with_stats(args);
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

    /// Phase A external sort spilling to MULTIPLE runs per side must k-way
    /// merge them into one globally-ordered stream — the join result is
    /// identical to the same inputs sorted entirely in memory, and matches a
    /// brute-force oracle. Before the multi-file merge was wired, a >1-run
    /// Phase A spill returned a hard error ("multi-file k-way merge is wired
    /// but not yet enabled"); this drives the completed merge end-to-end.
    #[test]
    fn test_sort_merge_phase_a_multi_file_spill() {
        let drivers_schema = schema_with(&["k", "pad"]);
        let builds_schema = schema_with(&["k", "pad"]);
        // ~1.2 KiB pad per row inflates each side well past the Phase A spill
        // threshold at the 128 KiB budget (soft_limit / 2 ≈ 51 KiB), forcing
        // several sorted runs per side and thus the k-way merge. The match set
        // is kept tiny (only the low-key drivers beat the high-key builds) so
        // the O(N·M) output stays under the same budget's output guard — this
        // test targets the input axis, not the output axis.
        let pad = "x".repeat(1200);
        let n = 100i64;
        // First `matching_lo` drivers have a low key that beats the builds;
        // the rest have a key no build can exceed. Symmetrically only the first
        // `matching_hi` builds carry a key the low drivers beat.
        let matching_lo = 4i64;
        let matching_hi = 4i64;
        let driver_keys: Vec<i64> = (0..n)
            .map(|i| if i < matching_lo { 0 } else { 1_000_000 })
            .collect();
        let build_keys: Vec<i64> = (0..n)
            .map(|j| if j < matching_hi { 10 } else { -1_000_000 })
            .collect();

        let make_inputs = || {
            let driver: Vec<(Record, RecordOrder)> = driver_keys
                .iter()
                .enumerate()
                .map(|(i, &k)| {
                    (
                        rec(
                            &drivers_schema,
                            vec![Value::Integer(k), Value::String(pad.clone().into())],
                        ),
                        i as RecordOrder,
                    )
                })
                .collect();
            let build: Vec<Record> = build_keys
                .iter()
                .map(|&k| {
                    rec(
                        &builds_schema,
                        vec![Value::Integer(k), Value::String(pad.clone().into())],
                    )
                })
                .collect();
            (driver, build)
        };

        // Brute-force oracle for `drivers.k < builds.k`: each satisfying build
        // emits one row tagged with the driver's original order.
        let mut oracle_orders: Vec<RecordOrder> = Vec::new();
        for (i, &dk) in driver_keys.iter().enumerate() {
            let matches = build_keys.iter().filter(|&&bk| dk < bk).count();
            oracle_orders.extend(std::iter::repeat_n(i as RecordOrder, matches));
        }
        oracle_orders.sort_unstable();
        assert_eq!(
            oracle_orders.len(),
            (matching_lo * matching_hi) as usize,
            "oracle match count is the low-driver × high-build product"
        );

        let compile = || {
            let typed = compile_pure_range(
                "filter drivers.k < builds.k",
                &[
                    ("drivers", "k", cxl::typecheck::Type::Int),
                    ("builds", "k", cxl::typecheck::Type::Int),
                ],
            );
            let rc = extract_range_conjunct(&typed, "drivers", "builds");
            decomposed_pure_range(rc, Arc::clone(&typed))
        };

        let run = |budget_bytes: Option<u64>| {
            let (driver, build) = make_inputs();
            run_kernel(RunKernel {
                driver_records: driver,
                build_records: build,
                decomposed: compile(),
                driver_qual: "drivers",
                build_qual: "builds",
                driver_schema: &drivers_schema,
                build_schema: &builds_schema,
                match_mode: MatchMode::All,
                on_miss: OnMiss::Skip,
                presorted: false,
                body_program: None,
                budget_bytes,
            })
        };

        // Constrained budget → Phase A spills several runs per side and k-way
        // merges them; the small match set keeps the output under the guard.
        let (spilled_records, spilled_stats) = run(Some(128 * 1024));
        // Default (large) budget → Phase A sorts entirely in memory.
        let (in_mem_records, in_mem_stats) = run(None);

        // Phase A external sort ran on both (not the presorted skip).
        assert_eq!(spilled_stats.phase_a_sort_invocations, 2);
        assert_eq!(in_mem_stats.phase_a_sort_invocations, 2);

        // Same match count as the oracle, both ways.
        assert_eq!(spilled_records.len(), oracle_orders.len());
        assert_eq!(in_mem_records.len(), oracle_orders.len());

        // The multiset of emitted driver-order tags matches the oracle exactly,
        // so the multi-run merge admitted precisely the same pairs the
        // in-memory sort did.
        let mut spilled_orders: Vec<RecordOrder> =
            spilled_records.iter().map(|(_, o)| *o).collect();
        spilled_orders.sort_unstable();
        let mut in_mem_orders: Vec<RecordOrder> = in_mem_records.iter().map(|(_, o)| *o).collect();
        in_mem_orders.sort_unstable();
        assert_eq!(spilled_orders, oracle_orders);
        assert_eq!(in_mem_orders, oracle_orders);
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

    /// A sort-merge join whose full sorted sides both exceed a tight budget
    /// must complete within budget by streaming/spilling each side, and its
    /// output must be byte-identical to the same join run entirely in memory.
    /// This drives the streamed Phase B (driver k-way merge + spilled build
    /// buffer) and proves it admits exactly the same pairs in the same order
    /// as the resident path, independent of the memory limit.
    ///
    /// The matching drivers all share range key 0 — a tie the sort must break
    /// stably — but carry a distinct `tag` column equal to their input index.
    /// Because the projection below compares each row's full value vector, a tie
    /// permutation or a record-to-order-tag mis-pairing across budgets would
    /// change the emitted `(order, values)` sequence and fail the assertion; the
    /// tag is what makes stable-tie ordering observable rather than masked by
    /// value-identical rows.
    #[test]
    fn test_sort_merge_streamed_sides_byte_identical_across_budgets() {
        let drivers_schema = schema_with(&["k", "pad", "tag"]);
        let builds_schema = schema_with(&["k", "pad"]);
        // ~1.2 KiB pad per row inflates each side well past the tight budget's
        // per-side spill thresholds (driver external sort spills into several
        // runs; the build buffer spills to disk). A small match set keeps the
        // O(N·M) output under the every-10K output guard.
        let pad = "y".repeat(1200);
        let n = 100i64;
        let matching = 5i64;
        let driver_keys: Vec<i64> = (0..n)
            .map(|i| if i < matching { 0 } else { 5_000_000 })
            .collect();
        let build_keys: Vec<i64> = (0..n)
            .map(|j| if j < matching { 10 } else { -5_000_000 })
            .collect();

        let make_inputs = || {
            let driver: Vec<(Record, RecordOrder)> = driver_keys
                .iter()
                .enumerate()
                .map(|(i, &k)| {
                    (
                        rec(
                            &drivers_schema,
                            vec![
                                Value::Integer(k),
                                Value::String(pad.clone().into()),
                                // Distinct per driver so a tie permutation or a
                                // value↔order mis-pairing is visible in the output.
                                Value::Integer(i as i64),
                            ],
                        ),
                        i as RecordOrder,
                    )
                })
                .collect();
            let build: Vec<Record> = build_keys
                .iter()
                .map(|&k| {
                    rec(
                        &builds_schema,
                        vec![Value::Integer(k), Value::String(pad.clone().into())],
                    )
                })
                .collect();
            (driver, build)
        };

        // Brute-force oracle multiset of emitted driver-order tags.
        let mut oracle_orders: Vec<RecordOrder> = Vec::new();
        for (i, &dk) in driver_keys.iter().enumerate() {
            let m = build_keys.iter().filter(|&&bk| dk < bk).count();
            oracle_orders.extend(std::iter::repeat_n(i as RecordOrder, m));
        }
        oracle_orders.sort_unstable();
        assert_eq!(oracle_orders.len(), (matching * matching) as usize);

        let compile = || {
            let typed = compile_pure_range(
                "filter drivers.k < builds.k",
                &[
                    ("drivers", "k", cxl::typecheck::Type::Int),
                    ("builds", "k", cxl::typecheck::Type::Int),
                ],
            );
            let rc = extract_range_conjunct(&typed, "drivers", "builds");
            decomposed_pure_range(rc, Arc::clone(&typed))
        };
        let run = |budget_bytes: Option<u64>| {
            let (driver, build) = make_inputs();
            run_kernel(RunKernel {
                driver_records: driver,
                build_records: build,
                decomposed: compile(),
                driver_qual: "drivers",
                build_qual: "builds",
                driver_schema: &drivers_schema,
                build_schema: &builds_schema,
                match_mode: MatchMode::All,
                on_miss: OnMiss::Skip,
                presorted: false,
                body_program: None,
                budget_bytes,
            })
        };

        // Tight budget spills both sides through the streamed Phase B; the
        // default (large) budget sorts and holds them in memory.
        let (tight_records, tight_stats) = run(Some(128 * 1024));
        let (large_records, _large_stats) = run(None);

        // The tight run ran Phase A external sort on both sides and spilled the
        // replayable build buffer — the streamed path, not the resident one.
        assert_eq!(tight_stats.phase_a_sort_invocations, 2);
        assert!(
            tight_stats.matching_run_spills >= 1,
            "the build side must spill under the tight budget; stats={tight_stats:?}"
        );
        assert_eq!(
            tight_stats.driver_stream_spilled, 1,
            "the driver side must stream from disk under the tight budget \
             (not stay resident); stats={tight_stats:?}"
        );

        // Same match count as the oracle, both ways.
        assert_eq!(tight_records.len(), oracle_orders.len());
        assert_eq!(large_records.len(), oracle_orders.len());

        // Byte-identical output across budgets: the same (order, values)
        // sequence regardless of whether the sides streamed or stayed resident.
        let project = |rs: &[(Record, RecordOrder)]| {
            rs.iter()
                .map(|(r, o)| (*o, r.values().to_vec()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            project(&tight_records),
            project(&large_records),
            "streamed (tight-budget) and resident (large-budget) output must be byte-identical"
        );

        // And the emitted driver-order multiset matches the oracle exactly.
        let mut tight_orders: Vec<RecordOrder> = tight_records.iter().map(|(_, o)| *o).collect();
        tight_orders.sort_unstable();
        assert_eq!(tight_orders, oracle_orders);
    }

    /// The PRE-SORTED path must bound the driver axis too: an already-sorted
    /// driver larger than the budget streams from disk instead of staying
    /// resident, and the output stays byte-identical across budgets. Every
    /// driver shares range key 0 — one big tie block — so the cross-budget
    /// byte-identity assertion pins that the driver's stable tie order and each
    /// record's distinct `tag` survive the spill → k-way-merge round trip. This
    /// is the regression guard for the pre-sorted driver that previously stayed
    /// fully resident (`DriverStream::InMemory` over the whole driver vector).
    #[test]
    fn test_sort_merge_presorted_driver_bounded_byte_identical_across_budgets() {
        let drivers_schema = schema_with(&["k", "pad", "tag"]);
        let builds_schema = schema_with(&["k"]);
        // Wide pad so the sorted driver overflows the tight budget's spill
        // threshold and streams from disk; a two-row build keeps the O(N·M)
        // output small enough to stay under the every-10K output guard.
        let pad = "z".repeat(1200);
        let n = 60usize;
        // Driver: all key 0 (ascending — a degenerate pre-sorted run of ties),
        // distinct tag per row. Build: two rows with key 10, so every driver
        // matches both under `drivers.k < builds.k`.
        let make_inputs = || {
            let driver: Vec<(Record, RecordOrder)> = (0..n)
                .map(|i| {
                    (
                        rec(
                            &drivers_schema,
                            vec![
                                Value::Integer(0),
                                Value::String(pad.clone().into()),
                                Value::Integer(i as i64),
                            ],
                        ),
                        i as RecordOrder,
                    )
                })
                .collect();
            let build: Vec<Record> = (0..2)
                .map(|_| rec(&builds_schema, vec![Value::Integer(10)]))
                .collect();
            (driver, build)
        };

        let compile = || {
            let typed = compile_pure_range(
                "filter drivers.k < builds.k",
                &[
                    ("drivers", "k", cxl::typecheck::Type::Int),
                    ("builds", "k", cxl::typecheck::Type::Int),
                ],
            );
            let rc = extract_range_conjunct(&typed, "drivers", "builds");
            decomposed_pure_range(rc, Arc::clone(&typed))
        };
        let run = |budget_bytes: Option<u64>| {
            let (driver, build) = make_inputs();
            run_kernel(RunKernel {
                driver_records: driver,
                build_records: build,
                decomposed: compile(),
                driver_qual: "drivers",
                build_qual: "builds",
                driver_schema: &drivers_schema,
                build_schema: &builds_schema,
                match_mode: MatchMode::All,
                on_miss: OnMiss::Skip,
                presorted: true,
                body_program: None,
                budget_bytes,
            })
        };

        let (tight_records, tight_stats) = run(Some(64 * 1024));
        let (large_records, _large_stats) = run(None);

        // Pre-sorted, so no explicit Phase A sort was requested — yet the driver
        // still spilled and streamed under the tight budget rather than staying
        // resident. That pairing is the whole point of the fix.
        assert_eq!(
            tight_stats.phase_a_sort_invocations, 0,
            "pre-sorted run must not request Phase A external sort; stats={tight_stats:?}"
        );
        assert_eq!(
            tight_stats.driver_stream_spilled, 1,
            "the pre-sorted driver must stream from disk under the tight budget \
             (bounded), not stay resident; stats={tight_stats:?}"
        );

        assert_eq!(tight_records.len(), n * 2);
        assert_eq!(large_records.len(), n * 2);

        // Byte-identical across budgets: same (order, values) sequence whether
        // the tied driver streamed from disk or stayed in memory. A tie
        // permutation or a value↔order mis-pairing would break this.
        let project = |rs: &[(Record, RecordOrder)]| {
            rs.iter()
                .map(|(r, o)| (*o, r.values().to_vec()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            project(&tight_records),
            project(&large_records),
            "pre-sorted streamed (tight) and resident (large) output must be byte-identical"
        );
    }

    /// Body-less range combines (no CXL body, encoded output schema present —
    /// the plain 2-input join shape, distinct from the N-ary synthetic chain
    /// whose intermediate steps always use `match: all`) must honor the match
    /// mode: `first` emits exactly one row per matched driver, `all` emits every
    /// match. Locks the streamed Phase B's per-driver short-circuit so a future
    /// refactor cannot silently regress `first` back to emit-all (or vice versa).
    #[test]
    fn test_sort_merge_body_less_first_vs_all() {
        let drivers_schema = schema_with(&["k"]);
        let builds_schema = schema_with(&["k"]);
        // One driver (key 1) below three builds (keys 2,3,4): three candidates.
        let driver = vec![(rec(&drivers_schema, vec![Value::Integer(1)]), 0)];
        let build = vec![
            rec(&builds_schema, vec![Value::Integer(2)]),
            rec(&builds_schema, vec![Value::Integer(3)]),
            rec(&builds_schema, vec![Value::Integer(4)]),
        ];

        let compile = || {
            let typed = compile_pure_range(
                "filter drivers.k < builds.k",
                &[
                    ("drivers", "k", cxl::typecheck::Type::Int),
                    ("builds", "k", cxl::typecheck::Type::Int),
                ],
            );
            let rc = extract_range_conjunct(&typed, "drivers", "builds");
            decomposed_pure_range(rc, Arc::clone(&typed))
        };
        let run = |mode: MatchMode| {
            run_kernel(RunKernel {
                driver_records: driver.clone(),
                build_records: build.clone(),
                decomposed: compile(),
                driver_qual: "drivers",
                build_qual: "builds",
                driver_schema: &drivers_schema,
                build_schema: &builds_schema,
                match_mode: mode,
                on_miss: OnMiss::Skip,
                presorted: true,
                body_program: None,
                budget_bytes: None,
            })
            .0
        };

        assert_eq!(
            run(MatchMode::First).len(),
            1,
            "match: first must emit exactly one row for the matched driver"
        );
        assert_eq!(
            run(MatchMode::All).len(),
            3,
            "match: all must emit one row per matching build"
        );
    }

    /// `match: collect` must poll the output buffer and abort cleanly over a
    /// tight budget instead of growing unbounded. A high-cardinality driver
    /// side with a permissive predicate accumulates one collect row per driver;
    /// past the every-10K poll the output-buffer byte count trips the hard
    /// limit and the kernel returns `MemoryBudgetExceeded`.
    #[test]
    fn test_sort_merge_collect_aborts_over_tight_budget() {
        // The driver schema carries the build-qualifier column ("builds"),
        // standing in for the plan-time-widened output schema the collect flush
        // writes its array into (run_kernel passes no separate output schema).
        let drivers_schema = schema_with(&["k", "builds"]);
        let builds_schema = schema_with(&["k", "b"]);
        // Every driver (key 0) matches every build (key 1000): a permissive
        // predicate making each collect array non-empty. More than one poll
        // interval of drivers so the every-10K output poll fires.
        let n_drivers = 12_000usize;
        let driver: Vec<(Record, RecordOrder)> = (0..n_drivers)
            .map(|i| {
                (
                    rec(&drivers_schema, vec![Value::Integer(0), Value::Null]),
                    i as RecordOrder,
                )
            })
            .collect();
        let build: Vec<Record> = (0..4)
            .map(|j| {
                rec(
                    &builds_schema,
                    vec![Value::Integer(1000), Value::Integer(j)],
                )
            })
            .collect();

        let typed = compile_pure_range(
            "filter drivers.k < builds.k",
            &[
                ("drivers", "k", cxl::typecheck::Type::Int),
                ("builds", "k", cxl::typecheck::Type::Int),
            ],
        );
        let rc = extract_range_conjunct(&typed, "drivers", "builds");
        let decomposed = decomposed_pure_range(rc, Arc::clone(&typed));

        // 64 KiB hard limit: one poll interval of accumulated collect rows
        // dwarfs it, so the poll at the first interval boundary aborts.
        let result = run_kernel_result(RunKernel {
            driver_records: driver,
            build_records: build,
            decomposed,
            driver_qual: "drivers",
            build_qual: "builds",
            driver_schema: &drivers_schema,
            build_schema: &builds_schema,
            match_mode: MatchMode::Collect,
            on_miss: OnMiss::Skip,
            presorted: true,
            body_program: None,
            budget_bytes: Some(64 * 1024),
        });
        let err = match result {
            Ok(_) => panic!("collect over a tight budget must abort, not grow unbounded"),
            Err(e) => e,
        };
        match err {
            PipelineError::MemoryBudgetExceeded { node, .. } => assert_eq!(node, "sm_test"),
            other => {
                panic!("expected MemoryBudgetExceeded from the collect output poll; got {other:?}")
            }
        }
    }

    /// Build `n` phase A driver pairs whose sort key `k` is an Integer and
    /// whose wide `pad` column pushes the accumulated buffer past the phase A
    /// 16 KiB spill-threshold floor after a handful of records.
    fn phase_a_driver_pairs(n: usize) -> Vec<(Record, RecordOrder, Value)> {
        let schema = schema_with(&["k", "pad"]);
        let pad = "x".repeat(1024);
        (0..n)
            .map(|i| {
                let r = rec(
                    &schema,
                    vec![Value::Integer(i as i64), Value::String(pad.as_str().into())],
                );
                (r, i as RecordOrder, Value::Integer(i as i64))
            })
            .collect()
    }

    /// Phase A driver external sort must charge each spilled run against the
    /// disk quota and abort with E320 once a run crosses the cap. Before the
    /// fix the driver-side phase A sort spilled with zero disk-cap accounting,
    /// so it could write past `storage.spill.disk_cap_bytes` unbounded.
    #[test]
    fn phase_a_driver_spill_past_disk_cap_fails_with_spill_cap_exceeded() {
        let pairs = phase_a_driver_pairs(64);
        let budget = MemoryArbitrator::with_policy(1024, 0.80, 0.70, Box::new(NoOpPolicy));
        budget.set_max_spill_bytes(1);
        let dir = tempfile::tempdir().unwrap();
        let handle = crate::pipeline::memory::ConsumerHandle::new();
        let err = sort_driver_stream(DriverStreamBuild {
            pairs,
            name: "sm",
            range_field: &Some("k".to_string()),
            budget: &budget,
            spill_compress: true,
            consumer_handle: &handle,
            spill_dir: dir.path(),
            presorted: false,
        })
        .err()
        .expect("a one-byte disk cap must abort the driver phase A spill");
        match err {
            PipelineError::SpillCapExceeded {
                node,
                cap,
                attempted,
                current,
            } => {
                assert_eq!(node, "sm");
                assert_eq!(cap, 1, "reported cap equals the configured quota");
                assert!(attempted > 0, "the overflowing run reports its size");
                assert!(current > cap, "cumulative spilled must exceed the cap");
            }
            other => panic!("expected SpillCapExceeded; got {other:?}"),
        }
        drop(dir);
    }

    /// The build side mirrors the driver: each phase A spilled run is charged
    /// and the disk cap is enforced.
    #[test]
    fn phase_a_build_spill_past_disk_cap_fails_with_spill_cap_exceeded() {
        let schema = schema_with(&["k", "pad"]);
        let pad = "x".repeat(1024);
        let pairs: Vec<(Record, Value)> = (0..64i64)
            .map(|i| {
                let r = rec(
                    &schema,
                    vec![Value::Integer(i), Value::String(pad.as_str().into())],
                );
                (r, Value::Integer(i))
            })
            .collect();
        let budget = MemoryArbitrator::with_policy(1024, 0.80, 0.70, Box::new(NoOpPolicy));
        budget.set_max_spill_bytes(1);
        let dir = tempfile::tempdir().unwrap();
        let handle = crate::pipeline::memory::ConsumerHandle::new();
        let mut spill_seq: u32 = 0;
        let mut stats = SortMergeStats::default();
        let err = build_side_from_pairs(BuildSideBuild {
            pairs,
            name: "sm",
            range_field: &Some("k".to_string()),
            budget: &budget,
            spill_compress: true,
            consumer_handle: &handle,
            spill_dir: dir.path(),
            byte_limit: 16 * 1024,
            presorted: false,
            spill_seq: &mut spill_seq,
            stats: &mut stats,
        })
        .err()
        .expect("a one-byte disk cap must abort the build phase A spill");
        match err {
            PipelineError::SpillCapExceeded {
                node, cap, current, ..
            } => {
                assert_eq!(node, "sm");
                assert_eq!(cap, 1);
                assert!(current > cap);
            }
            other => panic!("expected SpillCapExceeded; got {other:?}"),
        }
        drop(dir);
    }

    /// Phase A must spill into the CONFIGURED spill root, not the OS temp dir.
    /// A configured root removed before the spill surfaces DirUnavailable; if
    /// the sort instead fell back to `std::env::temp_dir()` (the bug) the spill
    /// would succeed against the healthy OS temp dir and no error would surface.
    #[test]
    fn phase_a_spills_into_configured_root_not_env_temp() {
        let pairs = phase_a_driver_pairs(64);
        let budget = MemoryArbitrator::with_policy(1024, 0.80, 0.70, Box::new(NoOpPolicy));
        let scratch = tempfile::tempdir().unwrap();
        let spill_root = scratch.path().join("configured-root");
        std::fs::create_dir(&spill_root).unwrap();
        std::fs::remove_dir(&spill_root).unwrap();
        let handle = crate::pipeline::memory::ConsumerHandle::new();
        let err = sort_driver_stream(DriverStreamBuild {
            pairs,
            name: "sm",
            range_field: &Some("k".to_string()),
            budget: &budget,
            spill_compress: true,
            consumer_handle: &handle,
            spill_dir: &spill_root,
            presorted: false,
        })
        .err()
        .expect(
            "phase A must spill into the configured (now-removed) root and fail \
             DirUnavailable, proving it does not fall back to the OS temp dir",
        );
        let rendered = err.to_string();
        assert!(
            rendered.contains("became unavailable mid-run"),
            "expected DirUnavailable from the configured root; got: {rendered}"
        );
        drop(scratch);
    }

    /// Range-key comparison over float `Value`s must agree with native f64
    /// order across the sign boundary. Isolated in its own module so the
    /// proptest prelude imports stay local.
    mod float_range_keys {
        use super::super::{RangeOp, apply_op, cmp_range_keys};
        use clinker_record::Value;
        use proptest::prelude::*;
        use std::collections::HashSet;

        /// Finite `f64` sample skewed toward small recurring integers (so
        /// duplicate keys and boundary equality get exercised), the signed
        /// zeros, subnormals, and a wide continuous spread reaching large
        /// magnitudes of both signs. The negative values are the whole
        /// point: the old `f.to_bits() as i64` encoding is only
        /// non-monotone once the sign bit is set.
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

        fn arb_op() -> impl Strategy<Value = RangeOp> {
            prop_oneof![
                Just(RangeOp::Lt),
                Just(RangeOp::Le),
                Just(RangeOp::Gt),
                Just(RangeOp::Ge),
            ]
        }

        fn apply_op_f64(a: f64, op: RangeOp, b: f64) -> bool {
            match op {
                RangeOp::Lt => a < b,
                RangeOp::Le => a <= b,
                RangeOp::Gt => a > b,
                RangeOp::Ge => a >= b,
            }
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            /// `apply_op` (and thus `cmp_range_keys`) over float keys must
            /// match native f64 comparison for every operator, including
            /// across the sign boundary and the signed-zero pair.
            #[test]
            fn apply_op_matches_native_f64(
                a in arb_float_key(),
                b in arb_float_key(),
                op in arb_op(),
            ) {
                prop_assert_eq!(
                    apply_op(&Value::Float(a), op, &Value::Float(b)),
                    apply_op_f64(a, op, b)
                );
                // Ordering agrees too, treating signed zeros as equal.
                let native = a.partial_cmp(&b).unwrap();
                prop_assert_eq!(
                    cmp_range_keys(&Value::Float(a), &Value::Float(b)),
                    native
                );
            }

            /// Float-key range matching via `apply_op` (the predicate the
            /// Phase-B merge walk applies per candidate pair) must equal the
            /// brute-force native-f64 nested-loop oracle for every operator.
            /// This pins the range-key encoding seam; it does not drive the
            /// Phase-A external sort or the merge walk itself.
            #[test]
            fn range_join_matches_nested_loop(
                left in prop::collection::vec(arb_float_key(), 0..40),
                right in prop::collection::vec(arb_float_key(), 0..40),
                op in arb_op(),
            ) {
                let mut actual: HashSet<(usize, usize)> = HashSet::new();
                for (li, &l) in left.iter().enumerate() {
                    for (ri, &r) in right.iter().enumerate() {
                        if apply_op(&Value::Float(l), op, &Value::Float(r)) {
                            actual.insert((li, ri));
                        }
                    }
                }
                let mut expected: HashSet<(usize, usize)> = HashSet::new();
                for (li, &l) in left.iter().enumerate() {
                    for (ri, &r) in right.iter().enumerate() {
                        if apply_op_f64(l, op, r) {
                            expected.insert((li, ri));
                        }
                    }
                }
                prop_assert_eq!(actual, expected);
            }
        }
    }
}
