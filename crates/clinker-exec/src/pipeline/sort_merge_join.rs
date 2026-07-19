//! Sort-merge join executor for combine nodes with pure-range predicates.
//!
//! Bounded on all three axes — driver input, build input, and output — so a
//! join whose sides or result exceed the memory budget completes by spilling
//! rather than materializing in RAM. Two-phase execution:
//!
//!   Phase A — bound each side into a single-pass ascending cursor over the
//!             range key, via the shared [`SortBuffer`] + [`SortedRunMerger`]
//!             external sort (postcard+LZ4 spills). A pre-sorted side that fits
//!             budget is streamed in place (no redundant stable sort); an
//!             over-budget side spills regardless.
//!
//!   Phase B — streaming windowed merge. Drivers are pulled one at a time from
//!             the sorted driver stream. The matching build rows for one driver
//!             form a contiguous prefix (`>=` / `>`) or suffix (`<=` / `<`) of
//!             the sorted build side; the walk advances a monotonic build cursor
//!             into a spillable [`MatchWindow`], appending newly-matching builds
//!             and front-dropping ones that fall below the current driver. Each
//!             build is decoded from its sorted input exactly once, never once
//!             per driver — the difference between a merge and a nested loop.
//!             The window spills past `soft_limit / 4` (the SPARK-13450 spill
//!             fallback for a single high-duplication run).
//!
//! Output order and axis: emitted rows route through a payload-ordered,
//! spillable [`SortBuffer`] keyed on `(driver order, driver input index, build
//! input index)` — the engine-canonical order the IEJoin block-band path also
//! realizes. So the emitted order is a pure function of the data, byte-identical
//! across memory limits and independent of the merge's internal emit sequence,
//! and the output axis spills instead of holding every matched row in RAM. The
//! dispatcher drains the resulting sorted handle through the shared payload-
//! sorted drain (streaming to a downstream Output, adopting spilled runs whole,
//! or admitting resident).
//!
//! NULL handling matches the IEJoin path: any record with a NULL in any range
//! key matches nothing (CXL ternary semantics return false for NULL
//! comparisons) and routes through `on_miss` / collect-empty. `on_miss: error`
//! cites the globally lowest `RecordOrder` miss, tracked as a single running
//! minimum so a sparse left join stays bounded.
//!
//! The current planner selects `SortMerge` only when both inputs already arrive
//! sorted on the range key prefix; the non-pre-sorted branch is wired end-to-end
//! so a future planner relaxation (or an explicit user knob) can route unsorted
//! inputs here without a second implementation pass.

use std::cmp::Ordering;
use std::path::Path;
use std::sync::Arc;

use rayon::iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator};
use serde::{Serialize, de::DeserializeOwned};

use clinker_record::{Record, Schema, Value};
use cxl::ast::Expr;
use cxl::eval::{EvalContext, EvalError, EvalResult, ProgramEvaluator, SkipReason};
use cxl::typecheck::TypedProgram;
use indexmap::IndexMap;

use crate::executor::combine::{CombineResolver, CombineResolverMapping};
use crate::executor::widen_record_to_schema;
use crate::pipeline::combine::{CombineOutputEvalFailure, KeyExtractor};
use crate::pipeline::memory::MemoryArbitrator;
#[cfg(test)]
use crate::pipeline::memory::NoOpPolicy;
use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};
use crate::pipeline::spill::{SpillFile, SpillWriter};
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

/// Emitted-row period between global [`MemoryArbitrator::should_abort`] polls
/// during the merge walk. The self-spilling output sort is the primary bound on
/// this operator's own residency; this poll is the last-resort defense that
/// catches cross-consumer or under-counted global pressure the output sort
/// cannot see — the same cadence `pipeline::grace_hash` and `pipeline::combine`
/// use. `should_abort` trips on either RSS or the byte-counted consumer sum, so
/// the backstop still fires on targets where RSS is unmeasurable.
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
// Match window
// ──────────────────────────────────────────────────────────────────────

/// One build row buffered in the [`MatchWindow`]: the record, its range key,
/// and its unique global input index.
type WindowEntry = (Record, Value, u64);

/// Shared spill / charge context for a [`MatchWindow`] mutation and the output
/// sink. Bundled so the window methods stay under clippy's argument cap and
/// every spill charges disk and memory identically.
struct MergeSpill<'a> {
    name: &'a str,
    spill_dir: &'a Path,
    spill_compress: bool,
    budget: &'a MemoryArbitrator,
    consumer: &'a Arc<crate::pipeline::memory::ConsumerHandle>,
    /// Emitted rows since the last global memory backstop poll. Interior
    /// mutability because the sink borrows `MergeSpill` shared; the walk is
    /// single-threaded so a `Cell` suffices.
    emitted_since_check: std::cell::Cell<usize>,
    /// Opt-in per-combine output-row cap (E325); `None` is unlimited. Checked
    /// against the output buffer's cumulative `total_rows()` before each push, so
    /// the combine fails loud rather than emitting past the configured ceiling.
    max_output_rows: Option<u64>,
}

/// A sealed [`MatchWindow`] segment: resident entries, or a spilled
/// postcard+LZ4 run re-decoded on replay. The spill carries each entry's
/// `(key, build_idx)` as the payload so both survive off-process without a
/// synthetic sort column — the key need not be re-derivable from a record field
/// (an expression range axis has none).
enum WindowSegment {
    Resident(Vec<WindowEntry>),
    Spilled(SpillFile<(Value, u64)>),
}

/// Spillable, front-droppable, replayable buffer holding the *current matching
/// run* of the streaming range merge — the contiguous prefix (for `>=` / `>`)
/// or suffix (for `<=` / `<`) of the sorted build side that satisfies the range
/// predicate against the current driver.
///
/// Streaming/blocking + memory model: the merge walk back-appends builds as
/// they enter the run from the sorted cursor ([`push_back`]), front-drops builds
/// that permanently stop matching as drivers advance ([`drop_front_while`]), and
/// replays the run front-to-back (build-index ascending) once per driver
/// ([`replay`]). Resident content is held to roughly `byte_limit`; when it would
/// exceed that, the most-recently-sealed resident segment spills to disk —
/// keeping the front resident so front-drop stays a cheap pop, and the newest
/// (least-soon-dropped) segment on disk so a shrinking suffix does not
/// spill-then-immediately-drop. Spilled segments re-decode on each replay (the
/// SPARK-13450 fallback, reached only when one matching run itself exceeds
/// budget), and a fully-dropped front segment is released so an over-budget
/// build is never re-decoded past the run's lower bound. The live in-memory
/// footprint mirrors into the shared consumer handle for the walk's duration
/// and is discharged when the window drops.
///
/// [`push_back`]: MatchWindow::push_back
/// [`drop_front_while`]: MatchWindow::drop_front_while
/// [`replay`]: MatchWindow::replay
struct MatchWindow {
    /// Sealed segments, front-to-back. A partially front-dropped head is always
    /// `Resident` (a spilled head is decoded to resident before it is dropped
    /// into), so `head_off` indexes a resident vec.
    segments: std::collections::VecDeque<WindowSegment>,
    /// Records already dropped from the front of `segments.front()`.
    head_off: usize,
    /// Open resident tail receiving appends; sealed into a segment past
    /// `seg_target`.
    tail: Vec<WindowEntry>,
    tail_bytes: usize,
    /// Live in-memory bytes charged into the consumer handle: the resident
    /// segments' undropped entries plus the tail.
    charged: u64,
    schema: Option<Arc<Schema>>,
    /// Seal the tail into a segment once it reaches this many bytes.
    seg_target: usize,
    /// Spill sealed segments once resident content would exceed this.
    byte_limit: usize,
    /// Set once any segment spilled — surfaces the `matching_run_spills` stat.
    spilled: bool,
}

impl MatchWindow {
    fn new(byte_limit: usize) -> Self {
        // Seal at half the resident cap so ~two sealed segments plus the open
        // tail span the budget; a smaller segment also bounds the re-decode
        // wasted skipping a spilled head's dropped prefix.
        let seg_target = std::cmp::max(4 * 1024, byte_limit / 2);
        Self {
            segments: std::collections::VecDeque::new(),
            head_off: 0,
            tail: Vec::new(),
            tail_bytes: 0,
            charged: 0,
            schema: None,
            seg_target,
            byte_limit,
            spilled: false,
        }
    }

    fn entry_bytes(entry: &WindowEntry) -> usize {
        std::mem::size_of::<Record>()
            + entry.0.estimated_heap_size()
            + std::mem::size_of::<Value>()
            + std::mem::size_of::<u64>()
    }

    /// Append one matching build row, charging its in-memory growth into the
    /// consumer handle. Seals the tail past `seg_target`; when resident content
    /// would exceed `byte_limit`, spills the most-recently-sealed resident
    /// segment to disk and charges the written bytes against the disk quota,
    /// aborting with `SpillCapExceeded` on overflow.
    fn push_back(&mut self, entry: WindowEntry, ctx: &MergeSpill<'_>) -> Result<(), PipelineError> {
        if self.schema.is_none() {
            self.schema = Some(Arc::clone(entry.0.schema()));
        }
        let bytes = Self::entry_bytes(&entry) as u64;
        self.tail.push(entry);
        self.tail_bytes += bytes as usize;
        self.charged += bytes;
        ctx.consumer.add_bytes(bytes);

        if self.tail_bytes >= self.seg_target {
            let sealed = std::mem::take(&mut self.tail);
            self.tail_bytes = 0;
            self.segments.push_back(WindowSegment::Resident(sealed));
        }
        while self.charged > self.byte_limit as u64 {
            if !self.spill_one_resident(ctx)? {
                break;
            }
        }
        Ok(())
    }

    /// Spill the most-recently-sealed resident segment to disk, returning false
    /// when no resident segment remains. Writes only the segment's undropped
    /// entries (past `head_off` for a resident head) and rewinds `head_off`,
    /// since the spilled run holds only live entries.
    fn spill_one_resident(&mut self, ctx: &MergeSpill<'_>) -> Result<bool, PipelineError> {
        let Some(idx) = self
            .segments
            .iter()
            .rposition(|s| matches!(s, WindowSegment::Resident(_)))
        else {
            return Ok(false);
        };
        let is_head = idx == 0;
        let start = if is_head { self.head_off } else { 0 };
        // Take ownership of the resident vec, leaving an empty placeholder in the
        // slot, so the writer iterates its entries by reference — no clone on the
        // hot spill path. The placeholder is overwritten with the sealed `Spilled`
        // segment below (or the slot is removed when nothing live remains).
        let owned =
            match std::mem::replace(&mut self.segments[idx], WindowSegment::Resident(Vec::new())) {
                WindowSegment::Resident(v) => v,
                WindowSegment::Spilled(_) => unreachable!("rposition selected a Resident segment"),
            };
        let live = &owned[start..];
        if live.is_empty() {
            self.segments.remove(idx);
            if is_head {
                self.head_off = 0;
            }
            return Ok(true);
        }
        let schema = self
            .schema
            .clone()
            .expect("a non-empty window recorded its schema on first push");
        let mut writer: SpillWriter<(Value, u64)> =
            SpillWriter::new(schema, Some(ctx.spill_dir), ctx.spill_compress)
                .map_err(|e| spill_io_error(ctx.name, "match-window spill open failed", e))?;
        for (record, key, build_idx) in live {
            writer
                .write_pair(record, &(key.clone(), *build_idx))
                .map_err(|e| spill_io_error(ctx.name, "match-window spill write failed", e))?;
        }
        let (file, written) = writer
            .finish_with_bytes()
            .map_err(|e| spill_io_error(ctx.name, "match-window spill finish failed", e))?;
        let live_bytes: u64 = live.iter().map(|e| Self::entry_bytes(e) as u64).sum();
        self.segments[idx] = WindowSegment::Spilled(file);
        if is_head {
            self.head_off = 0;
        }
        self.charged -= live_bytes;
        ctx.consumer.sub_bytes(live_bytes);
        self.spilled = true;
        if written > 0 && ctx.budget.record_spill_bytes(ctx.name, written) {
            return Err(PipelineError::spill_cap_exceeded(
                ctx.name,
                ctx.budget.disk_quota(),
                written,
                ctx.budget.cumulative_spill_bytes(),
            ));
        }
        Ok(true)
    }

    /// Decode the spilled front segment to resident so it can be peeked and
    /// front-dropped by index. A no-op when the front is already resident.
    fn decode_head_to_resident(&mut self, ctx: &MergeSpill<'_>) -> Result<(), PipelineError> {
        if !matches!(self.segments.front(), Some(WindowSegment::Spilled(_))) {
            return Ok(());
        }
        let WindowSegment::Spilled(file) = self.segments.pop_front().unwrap() else {
            unreachable!("matched Spilled above")
        };
        let reader = file
            .reader()
            .map_err(|e| spill_io_error(ctx.name, "match-window replay open failed", e))?;
        let mut v: Vec<WindowEntry> = Vec::new();
        let mut bytes: u64 = 0;
        for item in reader {
            let (record, (key, build_idx)) =
                item.map_err(|e| spill_io_error(ctx.name, "match-window replay decode failed", e))?;
            let entry = (record, key, build_idx);
            bytes += Self::entry_bytes(&entry) as u64;
            v.push(entry);
        }
        self.charged += bytes;
        ctx.consumer.add_bytes(bytes);
        self.segments.push_front(WindowSegment::Resident(v));
        self.head_off = 0;
        Ok(())
    }

    /// Front-drop every leading build whose key satisfies `should_drop` — the
    /// builds that no longer match the current driver and, because both sides
    /// ascend, never match a later one. Releases fully-dropped segments so an
    /// over-budget build is not re-decoded past the run's lower bound.
    fn drop_front_while(
        &mut self,
        should_drop: impl Fn(&Value) -> bool,
        ctx: &MergeSpill<'_>,
    ) -> Result<(), PipelineError> {
        loop {
            if self.segments.is_empty() {
                if self.tail.is_empty() {
                    return Ok(());
                }
                // Fold the open tail into a segment so the drop walks one path.
                let sealed = std::mem::take(&mut self.tail);
                self.tail_bytes = 0;
                self.segments.push_back(WindowSegment::Resident(sealed));
                self.head_off = 0;
            }
            self.decode_head_to_resident(ctx)?;
            let mut head_off = self.head_off;
            let mut dropped_bytes: u64 = 0;
            let seg_len;
            {
                let WindowSegment::Resident(v) = self.segments.front().unwrap() else {
                    unreachable!("decode_head_to_resident left a resident head")
                };
                seg_len = v.len();
                while head_off < v.len() && should_drop(&v[head_off].1) {
                    dropped_bytes += Self::entry_bytes(&v[head_off]) as u64;
                    head_off += 1;
                }
            }
            self.head_off = head_off;
            self.charged -= dropped_bytes;
            ctx.consumer.sub_bytes(dropped_bytes);
            if head_off >= seg_len {
                self.segments.pop_front();
                self.head_off = 0;
                // Advance to the next segment; the run may keep dropping.
                continue;
            }
            return Ok(());
        }
    }

    /// Replay the current run front-to-back (build-index ascending) as
    /// `(record, key, build_idx)`. Resident segments clone; a spilled segment
    /// opens its reader lazily as the walk reaches it, so at most one spill
    /// reader is live and the run is never re-materialized whole.
    fn replay<'a>(
        &'a self,
        ctx: &'a MergeSpill<'a>,
    ) -> impl Iterator<Item = Result<WindowEntry, PipelineError>> + 'a {
        let head_off = self.head_off;
        let name = ctx.name;
        let seg_iter = (0..self.segments.len()).flat_map(
            move |i| -> Box<dyn Iterator<Item = Result<WindowEntry, PipelineError>> + 'a> {
                let start = if i == 0 { head_off } else { 0 };
                match &self.segments[i] {
                    WindowSegment::Resident(v) => Box::new(v[start..].iter().cloned().map(Ok)),
                    WindowSegment::Spilled(file) => match file.reader() {
                        Ok(reader) => Box::new(reader.map(move |item| {
                            item.map(|(record, (key, build_idx))| (record, key, build_idx))
                                .map_err(|e| {
                                    spill_io_error(name, "match-window replay decode failed", e)
                                })
                        })),
                        Err(e) => Box::new(std::iter::once(Err(spill_io_error(
                            name,
                            "match-window replay open failed",
                            e,
                        )))),
                    },
                }
            },
        );
        let tail_iter = self.tail.iter().cloned().map(Ok);
        seg_iter.chain(tail_iter)
    }

    /// Live in-memory bytes this window has charged into the consumer handle, so
    /// the caller discharges exactly once the walk finishes.
    fn charged_bytes(&self) -> u64 {
        self.charged
    }
}

/// Lift a spill error into a `PipelineError::Io` localized to the combine node —
/// the same shape the external-sort spill errors take. Generic over the error
/// type so it accepts both the sort-buffer and spill-writer error enums.
fn spill_io_error<E: std::fmt::Display>(name: &str, detail: &str, err: E) -> PipelineError {
    PipelineError::Io(std::io::Error::other(format!(
        "sort-merge {name}: {detail}: {err}"
    )))
}

// ──────────────────────────────────────────────────────────────────────
// Streamed side cursor
// ──────────────────────────────────────────────────────────────────────

/// Single-pass ascending stream of `(record, key, payload)` tuples for one join
/// side. `payload` is the side's spill-envelope companion carried verbatim
/// through Phase A: `(RecordOrder, driver_idx)` for the driver side — its order
/// tag plus its unique global input index (the deterministic tie-break under a
/// repeated `RecordOrder`, since a chained upstream can fan one input row into
/// several sharing one order) — and `build_idx` for the build side (carried so
/// the output sort breaks ties among one driver's matches deterministically).
/// Both sides walk this one cursor; the build side is consumed exactly once by
/// the merge walk (each build pulled into the match window a single time), so
/// each build decodes from its sorted input O(1) times regardless of driver
/// count — unlike the replay-per-driver predecessor.
///
/// Memory model: streaming. `Spilled` holds one resident record per open run in
/// the [`SortedRunMerger`] and re-reads the key from the sort column `field`;
/// `InMemory` owns the sorted vector with pre-extracted keys (also the carrier
/// for an expression range axis, whose key rides in no record column — reachable
/// only on the pre-sorted skip path, where the axis never spills). Either way
/// Phase B never holds a second full copy of the side — the merge walk pulls one
/// record at a time.
enum SideStream<P: Ord> {
    InMemory(std::vec::IntoIter<(Record, Value, P)>),
    Spilled {
        merger: SortedRunMerger<P>,
        field: String,
    },
}

impl<P: DeserializeOwned + Ord> Iterator for SideStream<P> {
    type Item = Result<(Record, Value, P), PipelineError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SideStream::InMemory(it) => it.next().map(Ok),
            SideStream::Spilled { merger, field } => match merger.next()? {
                Ok((record, payload)) => {
                    let key = record.get(field).cloned().unwrap_or(Value::Null);
                    Some(Ok((record, key, payload)))
                }
                Err(e) => Some(Err(e)),
            },
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
    /// Opt-in per-combine output-row cap (E325); `None` is unlimited. Enforced at
    /// the shared `push_output_row` chokepoint against the output sort's
    /// cumulative row count.
    pub max_output_rows: Option<u64>,
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
    /// Shared with the registered `SortMergeConsumer` wrapper. The operator
    /// mirrors its live in-memory footprint into `handle.bytes` so the
    /// arbitrator's pull-mode `current_usage` reads it: a resident (in-memory)
    /// sorted driver stays charged for the whole walk, the [`MatchWindow`]'s
    /// resident matching run is charged as it fills and discharged when the walk
    /// ends, and the payload-ordered output sort's resident bytes are charged as
    /// rows accumulate and released as they spill. Each side's transient Phase A
    /// sort-buffer charge nets to zero once that side lands (spilled records are
    /// off-process; a resident driver's charge is retained, not double-counted).
    pub consumer_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
    /// Error strategy governing output-stage eval failures. Under
    /// `FailFast` a residual / body eval error propagates immediately;
    /// under `Continue` / `BestEffort` the failing row is deferred to the
    /// dispatcher via [`SortMergeOutput::output_eval_failures`].
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
    /// Set to 1 when the build side exceeded its sort-buffer spill threshold and
    /// Phase B streamed the build cursor from disk through the k-way merger
    /// instead of holding it resident; 0 when the sorted build fit in memory.
    /// Proves the build input axis is bounded independently of the window spill.
    build_stream_spilled: u32,
    /// Count of build records pulled once from the sorted build cursor into the
    /// monotonically-advancing match window. Equals the number of orderable
    /// build rows regardless of driver count — the structural proof that the
    /// merge walk decodes each build from its sorted input O(1) times, never
    /// once per driver (the nested-loop failure mode).
    build_pulls: u64,
    /// Set to 1 when the payload-ordered output sort exceeded its byte threshold
    /// and spilled to disk instead of holding every emitted row resident. Proves
    /// the output axis is bounded — including the collect and on_miss paths,
    /// which route through the same sort.
    output_spilled: u32,
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
) -> Result<SortMergeOutput, PipelineError> {
    let (output, _stats) = execute_combine_sort_merge_with_stats(args)?;
    Ok(output)
}

/// A sort-merge combine's bounded output: a payload-ordered sorted handle over
/// the emitted rows — resident when the whole result fit the output sort
/// buffer's byte threshold, spilled to sorted runs otherwise — keyed on
/// `(driver order, driver_idx, build_idx)`, the engine-canonical, memory-limit
/// -independent order. The dispatcher drains it incrementally through the shared
/// payload-sorted drain, so the output axis spills instead of holding every
/// matched row in RAM. Mirrors the block-band IEJoin's `BlockBandOutput` so both
/// pure-range paths realize the same output order through the same sink.
pub(crate) struct SortMergeOutput {
    pub sorted: SortedOutput<(RecordOrder, u64, u64)>,
    /// Exact emitted-row count across every sorted run (== the output sort
    /// buffer's total pushes). Lets the dispatcher size a spilled drain in O(1)
    /// and serve a merge-on-drain node buffer's `len_hint` without a disk scan.
    pub row_count: u64,
    pub output_eval_failures: Vec<CombineOutputEvalFailure>,
}

/// Variant of [`execute_combine_sort_merge`] that also returns
/// [`SortMergeStats`]. Module-private — exposed to the in-module
/// `#[cfg(test)]` block so tests can verify Phase A skip, the windowed build
/// pulls, and the matching-run / output spill counts. Production callers go
/// through [`execute_combine_sort_merge`].
fn execute_combine_sort_merge_with_stats(
    args: SortMergeExec<'_>,
) -> Result<(SortMergeOutput, SortMergeStats), PipelineError> {
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
        max_output_rows,
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

    // Output rows carry the combine's output schema when present, else the
    // driver schema (the shape the body-less synthetic step and the
    // no-output-schema test path re-emit). Captured before the drains consume
    // `driver_records`; it backs the output sort's spill header. Spilling output
    // only happens on high-fan-out combines, which always carry an output
    // schema, so the fallback backs only the never-spilling small cases.
    let driver_schema_hint: Option<Arc<Schema>> =
        driver_records.first().map(|(r, _)| Arc::clone(r.schema()));

    // Range-key extraction runs the compiled CXL range closures per record — the
    // costly part of Phase A — in parallel across the shared kernel pool. Each
    // driver keeps its unique global input index (`driver_idx`) as the
    // deterministic output-order tie-break under a repeated `RecordOrder` (a
    // chained upstream can fan one input row into several sharing one order).
    let driver_keyed: Vec<(Record, RecordOrder, u64, Option<Value>)> = driver_records
        .into_par_iter()
        .enumerate()
        .map(|(driver_idx, (record, order))| {
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
            Ok::<_, PipelineError>((record, order, driver_idx as u64, key))
        })
        .collect::<Result<Vec<_>, _>>()?;
    // `(record, key, (order, driver_idx))`: the range key drives Phase A's sort;
    // `(order, driver_idx)` is the payload carried verbatim through the spill
    // envelope — the order tag plus the deterministic tie-break under a repeated
    // `RecordOrder`.
    let mut driver_pairs: Vec<(Record, Value, (RecordOrder, u64))> = Vec::new();
    // NULL / non-orderable-key drivers never satisfy a CXL ternary comparison,
    // so they match nothing and route through the miss dispatch after the walk.
    let mut driver_unmatched: Vec<(Record, RecordOrder, u64)> = Vec::new();
    for (record, order, driver_idx, key) in driver_keyed {
        match key {
            Some(k) => driver_pairs.push((record, k, (order, driver_idx))),
            None => driver_unmatched.push((record, order, driver_idx)),
        }
    }

    let build_keyed: Vec<(Record, u64, Option<Value>)> = build_records
        .into_par_iter()
        .enumerate()
        .map(|(build_idx, record)| {
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
            Ok::<_, PipelineError>((record, build_idx as u64, key))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut build_pairs: Vec<(Record, Value, u64)> = Vec::new();
    for (record, build_idx, key) in build_keyed {
        if let Some(k) = key {
            build_pairs.push((record, k, build_idx));
        }
    }

    // Match-window byte threshold: soft_limit / 4 (SPARK-13450 / DataFusion
    // convention), floored at 16 KiB so tiny-budget tests still exercise spill.
    let byte_limit: usize = std::cmp::max(16 * 1024, (budget.soft_limit() / 4) as usize);

    // ── Phase A: bound each side for the streaming windowed merge. The driver
    //    becomes a single-pass ascending stream and the build a single-pass
    //    ascending cursor pulled once into the match window. Neither holds a
    //    second full copy of an over-budget side during Phase B: driver and
    //    build merge lazily one record per open run, the window spills past
    //    `byte_limit`, and the output spills past its own threshold. The
    //    pre-sorted path only avoids the sort *work*.
    let (driver_stream, driver_charge) = sort_side_stream(SideStreamBuild {
        pairs: driver_pairs,
        name,
        range_field: &driver_field,
        budget,
        spill_compress,
        consumer_handle: &consumer_handle,
        spill_dir,
        presorted,
        side: "driver",
    })?;
    let (build_cursor, build_resident_charge) = sort_side_stream(SideStreamBuild {
        pairs: build_pairs,
        name,
        range_field: &build_field,
        budget,
        spill_compress,
        consumer_handle: &consumer_handle,
        spill_dir,
        presorted,
        side: "build",
    })?;
    if !presorted {
        stats.phase_a_sort_invocations = 2;
    }
    if matches!(driver_stream, SideStream::Spilled { .. }) {
        stats.driver_stream_spilled = 1;
    }
    if matches!(build_cursor, SideStream::Spilled { .. }) {
        stats.build_stream_spilled = 1;
    }

    // ── Phase B: streaming windowed merge ────────────────────────────
    //
    // Emitted rows accumulate in a payload-ordered, spillable sort keyed on
    // `(driver order, driver_idx, build_idx)` — `(order, driver_idx, u64::MAX)`
    // for collect / on_miss rows — so the emitted order is the engine-canonical,
    // memory-limit-independent order regardless of the merge's emit sequence, and
    // the output axis spills rather than holding every matched row in RAM.
    let output_row_schema: Arc<Schema> = match output_schema {
        Some(s) => Arc::clone(s),
        None => driver_schema_hint.unwrap_or_else(|| Arc::new(Schema::new(Vec::new()))),
    };
    let sort_threshold = spill_threshold_bytes(budget);
    let mut output_buf: SortBuffer<(RecordOrder, u64, u64)> = SortBuffer::new_payload_ordered(
        sort_threshold,
        Some(spill_dir.to_path_buf()),
        spill_compress,
        output_row_schema,
    );
    // Parallel `(order, driver_idx, build_idx)` sort key per deferred output-eval
    // failure, so the dead-letter rows re-order into the same layout-independent
    // order as the emitted rows.
    let mut failure_tags: Vec<(RecordOrder, u64, u64)> = Vec::new();

    let mut body_eval = body_program.map(|p| ProgramEvaluator::new(Arc::clone(p), false));
    let ectx = EmitCtx {
        name,
        output_schema,
        build_qualifier,
        propagate_ck,
        resolver_mapping,
        ctx,
        strategy,
    };
    let mspill = MergeSpill {
        name,
        spill_dir,
        spill_compress,
        budget,
        max_output_rows,
        consumer: &consumer_handle,
        emitted_since_check: std::cell::Cell::new(0),
    };

    // The matching set for one driver is a contiguous prefix (`>=` / `>`: build
    // keys at/below the driver) or suffix (`<=` / `<`: build keys at/above the
    // driver) of the ascending build side, whose boundary advances monotonically
    // as drivers ascend. Prefix ops only pull new builds into the window; suffix
    // ops also front-drop builds that fall below the current driver.
    let is_suffix = matches!(op, RangeOp::Lt | RangeOp::Le);
    let mut window = MatchWindow::new(byte_limit);
    // A resident (`InMemory`) build cursor holds every build row in RAM upfront,
    // charged as `build_resident_charge`; a spilled cursor streams one record per
    // open run and is not pre-charged. For the resident case the charge is handed
    // off row by row: each pull discharges the pulled build here and the window
    // re-charges it in `push_back`, so a byte-counting arbitrator always sees the
    // build exactly once — in the cursor before the pull, in the window after —
    // never twice and never zero. `build_charge` tracks the still-resident,
    // not-yet-pulled remainder, discharged once the walk ends.
    let build_charge_tracked = matches!(build_cursor, SideStream::InMemory(_));
    let mut build_charge = build_resident_charge;
    let mut build_iter = build_cursor;
    // One-record lookahead: a build that does not yet match the current driver
    // (prefix ops) waits here for a later, larger-key driver rather than dropping.
    let mut pending_build: Option<(Record, Value, u64)> = None;
    // Lowest `RecordOrder` among zero-match drivers, for `on_miss: error`. A
    // single value, so a sparse left join records misses in O(1) resident state
    // (never a resident vector of every unmatched driver).
    let mut min_miss: Option<RecordOrder> = None;

    for driver in driver_stream {
        let (driver_record, driver_key, (driver_order, driver_idx)) = driver?;

        // Front-drop builds that no longer match this driver (suffix ops only;
        // a prefix build, once matched, matches every later driver).
        if is_suffix {
            window.drop_front_while(|bk| !apply_op(&driver_key, op, bk), &mspill)?;
        }
        // Pull newly-matching builds from the cursor into the window. Each build
        // is read from the cursor exactly once (`build_pulls`), so a build is
        // decoded from its sorted input O(1) times regardless of driver count.
        loop {
            if pending_build.is_none() {
                match build_iter.next() {
                    Some(Ok(b)) => {
                        if build_charge_tracked {
                            // Ownership of this build's bytes moves from the
                            // resident cursor to the window (or is freed on a
                            // non-match); discharge the cursor's share here.
                            let b_bytes = MatchWindow::entry_bytes(&b) as u64;
                            consumer_handle.sub_bytes(b_bytes);
                            build_charge = build_charge.saturating_sub(b_bytes);
                        }
                        pending_build = Some(b);
                        stats.build_pulls += 1;
                    }
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }
            let matches = {
                let (_, bk, _) = pending_build.as_ref().unwrap();
                apply_op(&driver_key, op, bk)
            };
            if matches {
                window.push_back(pending_build.take().unwrap(), &mspill)?;
            } else if is_suffix {
                // Below the suffix: never matches this or any later driver — skip.
                pending_build = None;
            } else {
                // Above the prefix: may match a later, larger-key driver — hold.
                break;
            }
        }

        emit_for_driver(EmitDriverArgs {
            ectx: &ectx,
            mspill: &mspill,
            driver_record: &driver_record,
            driver_order,
            driver_idx,
            window: &window,
            body_eval: body_eval.as_mut(),
            decomposed,
            match_mode,
            on_miss,
            output: &mut output_buf,
            failures: &mut output_eval_failures,
            failure_tags: &mut failure_tags,
            min_miss: &mut min_miss,
        })?;
    }

    // The window's in-memory residency, any resident-driver charge, and the
    // still-resident (never-pulled) tail of a resident build cursor were mirrored
    // into the shared consumer handle for the walk's duration; discharge them all
    // now the walk is done. `build_charge` is zero for a spilled or exhausted
    // cursor.
    consumer_handle.sub_bytes(window.charged_bytes());
    consumer_handle.sub_bytes(driver_charge);
    consumer_handle.sub_bytes(build_charge);
    if window.spilled {
        stats.matching_run_spills = 1;
    }

    // ── Zero-key (NULL-range) drivers ────────────────────────────────
    // They never entered the merge, so each matches nothing: one collect row
    // (empty array) or the on_miss policy, routed through the same output sort so
    // their canonical order is realized alongside the walk's matched rows.
    for (driver_record, driver_order, driver_idx) in driver_unmatched {
        dispatch_driver_miss(DispatchMiss {
            ectx: &ectx,
            mspill: &mspill,
            driver_record: &driver_record,
            driver_order,
            driver_idx,
            match_mode,
            on_miss,
            body_eval: body_eval.as_mut(),
            output: &mut output_buf,
            failures: &mut output_eval_failures,
            failure_tags: &mut failure_tags,
            min_miss: &mut min_miss,
        })?;
    }

    // `on_miss: error` cites the globally lowest-RecordOrder miss, independent of
    // the walk order and the memory limit — identical to the pre-streaming flush.
    if matches!(on_miss, OnMiss::Error)
        && !matches!(match_mode, MatchMode::Collect)
        && let Some(row) = min_miss
    {
        return Err(PipelineError::CombineMissingMatch {
            combine: name.to_string(),
            driver_row: row,
        });
    }

    let row_count = output_buf.total_rows() as u64;
    // The output sort's mirrored bytes are freed as the finished handle passes to
    // the dispatcher; discharge them so the node-buffer admission never sums a
    // stale footprint. Charging the output sort during emit never aborts a
    // completing run — it self-bounds by spilling.
    let output_sort_charged = output_buf.bytes_used() as u64;
    let (sorted, residue) = output_buf.finish().map_err(|e| {
        PipelineError::Io(std::io::Error::other(format!(
            "sort-merge output finish failed: {e}"
        )))
    })?;
    consumer_handle.sub_bytes(output_sort_charged);
    if matches!(sorted, SortedOutput::Spilled(_)) {
        stats.output_spilled = 1;
    }
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
    // the data too. FailFast surfaces the first eval error and returns before
    // reaching here.
    debug_assert_eq!(output_eval_failures.len(), failure_tags.len());
    let mut failures: Vec<_> = output_eval_failures.into_iter().zip(failure_tags).collect();
    failures.sort_by_key(|(_, key)| *key);
    let output_eval_failures = failures.into_iter().map(|(f, _)| f).collect();

    Ok((
        SortMergeOutput {
            sorted,
            row_count,
            output_eval_failures,
        },
        stats,
    ))
}

// ──────────────────────────────────────────────────────────────────────
// Phase A helpers — external sort each side on the range key
// ──────────────────────────────────────────────────────────────────────

/// Inputs to [`sort_side_stream`], bundled so the signature stays under
/// clippy's `too_many_arguments` cap. Generic over the spill-envelope payload
/// `P` the side carries verbatim — `(RecordOrder, driver_idx)` for the driver
/// side, `build_idx` for the build side.
struct SideStreamBuild<'a, P> {
    /// `(record, key, payload)` tuples for the side.
    pairs: Vec<(Record, Value, P)>,
    name: &'a str,
    range_field: &'a Option<String>,
    budget: &'a MemoryArbitrator,
    spill_compress: bool,
    consumer_handle: &'a Arc<crate::pipeline::memory::ConsumerHandle>,
    spill_dir: &'a Path,
    /// True when the caller certified the input already ascends on the range
    /// key. A pre-sorted side that fits budget is walked in place; only an
    /// over-budget pre-sorted side funnels through the spillable sort.
    presorted: bool,
    /// `"driver"` or `"build"` — labels the side in spill / invariant messages.
    side: &'static str,
}

/// Resident byte charge of a `(record, key, payload)` entry once it lives in a
/// [`SideStream::InMemory`] vector: the record, its heap, and the inline key and
/// payload words. Folds the driver- and build-side per-record estimators into
/// one — they differed only by payload width, captured here by `size_of::<P>()`.
/// For the build side (`P = u64`) this equals [`MatchWindow::entry_bytes`], the
/// metric the window re-charges each pulled build with, keeping the
/// cursor→window hand-off netting exact.
fn side_entry_bytes<P>(record: &Record) -> u64 {
    (std::mem::size_of::<Record>()
        + record.estimated_heap_size()
        + std::mem::size_of::<Value>()
        + std::mem::size_of::<P>()) as u64
}

/// Sum the resident byte charge of pre-sorted `pairs` in the single pass that
/// also verifies the caller's ascending-range-key certification. A pre-sorted
/// walk-in-place trusts that order literally: a mis-certified (actually-
/// unsorted) input would otherwise diverge silently from the over-budget path,
/// which re-sorts through the spillable sort — the same input then yields
/// different output at different budgets. On the first out-of-order key this
/// returns a typed `Internal` naming the node, failing loud on the violated
/// precondition rather than panicking (debug-only `debug_assert`) or emitting
/// wrong rows (release). The check compares each key to the previous during the
/// byte-sum pass the caller already makes, so it adds no extra scan.
fn checked_presorted_charge<P>(
    pairs: &[(Record, Value, P)],
    name: &str,
    side: &'static str,
) -> Result<u64, PipelineError> {
    let mut total: u64 = 0;
    let mut prev: Option<&Value> = None;
    for (record, key, _payload) in pairs {
        if let Some(prev_key) = prev
            && cmp_range_keys(prev_key, key) == Ordering::Greater
        {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!(
                    "sort-merge phase A: pre-sorted {side} input is not ascending on \
                     the range key; the planner certified an out-of-order input as \
                     pre-sorted"
                ),
            });
        }
        total = total.saturating_add(side_entry_bytes::<P>(record));
        prev = Some(key);
    }
    Ok(total)
}

/// Sort one side's `(record, key, payload)` tuples on the range key field
/// (skipped when pre-sorted) and hand back a single-pass ascending
/// [`SideStream`] plus the in-memory bytes it charged into the consumer handle.
/// One helper for both sides — the driver and build Phase A sorts differ only in
/// the payload width they carry through the spill envelope.
///
/// Memory model: streaming. A spilled side streams one record per open run
/// through the [`SortedRunMerger`] and returns a zero charge — its transient
/// sort-buffer charge is netted here, and the records are off-process. A resident
/// (in-memory) side — sorted-but-fits, pre-sorted-and-fits, or an expression
/// range axis — stays charged through the walk and returns that charge for the
/// caller to discharge afterward, so a byte-counting arbitrator (RSS unavailable,
/// concurrent consumers) sees it. The resident charge uses the per-entry metric
/// [`side_entry_bytes`]: the build side hands it off record by record as the walk
/// pulls into the match window (which re-charges under the same metric), while a
/// resident driver is discharged wholesale once the walk ends.
///
/// A pre-sorted simple-field side that fits budget is walked in place, with no
/// redundant stable sort and Vec rebuild; only an over-budget pre-sorted side
/// funnels through the spillable sort (a stable sort of already-ascending input
/// is an order-preserving no-op, so the stream stays byte-identical while
/// spilling). An expression range axis (no simple field, reachable only
/// pre-sorted) keeps its pre-extracted keys resident, because a spilled key
/// cannot be re-read from a record column. On every walk-in-place path the
/// caller's pre-sorted certification is verified in release (not merely
/// `debug_assert`ed): a mis-certified input fails loud rather than diverging
/// across budgets.
fn sort_side_stream<P>(args: SideStreamBuild<'_, P>) -> Result<(SideStream<P>, u64), PipelineError>
where
    P: Serialize + DeserializeOwned + Send + Ord + crate::pipeline::sort_buffer::HeapBytes,
{
    let SideStreamBuild {
        pairs,
        name,
        range_field,
        budget,
        spill_compress,
        consumer_handle,
        spill_dir,
        presorted,
        side,
    } = args;
    if pairs.is_empty() {
        return Ok((SideStream::InMemory(Vec::new().into_iter()), 0));
    }
    let spill_threshold = spill_threshold_bytes(budget);

    let Some(field) = range_field else {
        // Expression range axis (e.g. `a.x + 1`): the key rides in no record
        // column, so it can neither drive `SortBuffer`'s field comparator nor be
        // recovered on a spilled replay. Reachable only pre-sorted (the sort
        // requires a simple field); keep resident with pre-extracted keys,
        // charged so the arbitrator sees it.
        if !presorted {
            return Err(PipelineError::Internal {
                op: "combine",
                node: name.to_string(),
                detail: format!(
                    "sort-merge phase A: {side}-side range expression is not a simple \
                     field reference; the planner selected SortMerge for an expression \
                     it cannot externally sort"
                ),
            });
        }
        let charged = checked_presorted_charge(&pairs, name, side)?;
        consumer_handle.add_bytes(charged);
        return Ok((SideStream::InMemory(pairs.into_iter()), charged));
    };

    if presorted {
        let total = checked_presorted_charge(&pairs, name, side)?;
        if total <= spill_threshold as u64 {
            // Fits budget: walk in place — no redundant stable sort + rebuild.
            consumer_handle.add_bytes(total);
            return Ok((SideStream::InMemory(pairs.into_iter()), total));
        }
        // Over budget: fall through to the spillable sort (stable no-op on
        // already-ascending input) so the side spills instead of holding whole.
    }

    let schema = Arc::clone(pairs[0].0.schema());
    let sort_field = clinker_plan::config::SortField {
        field: field.clone(),
        order: clinker_plan::config::SortOrder::Asc,
        null_order: None,
    };
    // The payload rides the spill envelope verbatim; the range key rides on the
    // record's sort column.
    let mut buf: SortBuffer<P> = SortBuffer::new(
        vec![sort_field],
        spill_threshold,
        Some(spill_dir.to_path_buf()),
        spill_compress,
        schema,
    );

    let mut local_charged: u64 = 0;
    for (record, _key, payload) in pairs {
        let pre = buf.bytes_used();
        buf.push(record, payload);
        let delta = (buf.bytes_used().saturating_sub(pre)) as u64;
        consumer_handle.add_bytes(delta);
        local_charged = local_charged.saturating_add(delta);
        if buf.should_spill() {
            let pre_spill = buf.bytes_used() as u64;
            let written = buf
                .sort_and_spill()
                .map_err(|e| spill_io_error(name, &format!("{side} phase A spill failed"), e))?;
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

    let (sorted, residue) = buf
        .finish()
        .map_err(|e| spill_io_error(name, &format!("{side} phase A finish failed"), e))?;
    if residue > 0 && budget.record_spill_bytes(name, residue) {
        return Err(PipelineError::spill_cap_exceeded(
            name,
            budget.disk_quota(),
            residue,
            budget.cumulative_spill_bytes(),
        ));
    }
    // Net the transient sort-buffer charge to zero; each terminal branch below
    // then charges only what it actually holds resident (nothing, for a spilled
    // cursor whose records live on disk).
    consumer_handle.sub_bytes(local_charged);
    match sorted {
        SortedOutput::InMemory(out) => {
            // Records stay resident in `v` until the walk consumes/pulls them; the
            // merger sorted each run and preserved that order across runs. Re-derive
            // each key from the sort column so the InMemory tuple matches the
            // spilled path's re-read, and charge under the per-entry metric so the
            // build side's row-by-row hand-off (and the driver's wholesale
            // discharge) net clean.
            let mut v: Vec<(Record, Value, P)> = Vec::with_capacity(out.len());
            for (record, payload) in out {
                let key = record.get(field).cloned().unwrap_or(Value::Null);
                v.push((record, key, payload));
            }
            let charged: u64 = v.iter().map(|(r, _, _)| side_entry_bytes::<P>(r)).sum();
            consumer_handle.add_bytes(charged);
            Ok((SideStream::InMemory(v.into_iter()), charged))
        }
        SortedOutput::Spilled(files) => {
            // Records are off-process; the transient sort charge was discharged
            // above. The merger holds one record per open run and the walk pulls
            // one at a time (a single run is an identity pass).
            let merge_field = clinker_plan::config::SortField {
                field: field.clone(),
                order: clinker_plan::config::SortOrder::Asc,
                null_order: None,
            };
            let merger = SortedRunMerger::new(
                files,
                std::slice::from_ref(&merge_field),
                if side == "build" {
                    "sort-merge build phase A"
                } else {
                    "sort-merge driver phase A"
                },
                MergeBudget {
                    budget,
                    node: name,
                    compress: spill_compress,
                },
            )?;
            Ok((
                SideStream::Spilled {
                    merger,
                    field: field.clone(),
                },
                0,
            ))
        }
    }
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

/// Fixed per-combine emit context: everything the per-driver emit and the
/// zero-match dispatch read but never mutate. Bundled so the emit helpers
/// stay under clippy's argument cap and every emit site widens and propagates
/// `$ck` identically.
struct EmitCtx<'a> {
    name: &'a str,
    output_schema: Option<&'a Arc<Schema>>,
    build_qualifier: &'a str,
    propagate_ck: &'a clinker_plan::config::pipeline_node::PropagateCkSpec,
    resolver_mapping: &'a CombineResolverMapping,
    ctx: &'a EvalContext<'a>,
    strategy: clinker_plan::config::ErrorStrategy,
}

/// Build slot of a collect / on_miss row's `(order, driver_idx, build)` sort
/// key. Such a row carries no build, so `u64::MAX` fixes the slot
/// deterministically — a driver emits either matched rows or a single
/// miss/collect row, never both, so it never collides with a real build index.
const MISS_BUILD_TAG: u64 = u64::MAX;

/// Push one emitted row into the payload-ordered output sort, charging its
/// in-memory growth into the consumer handle and spilling (charging the run
/// against the disk quota) when the buffer crosses its threshold. Every emit
/// path — matched, on_miss null_fields, and collect — routes through here, so
/// the output axis is bounded uniformly rather than growing an in-RAM vec.
fn push_output_row(
    output: &mut SortBuffer<(RecordOrder, u64, u64)>,
    record: Record,
    key: (RecordOrder, u64, u64),
    mspill: &MergeSpill<'_>,
) -> Result<(), PipelineError> {
    // Opt-in output-size runaway guard (E325): refuse the row that would carry
    // the cumulative count past the cap, failing loud rather than truncating.
    // `total_rows()` counts every prior push across spilled runs, so checking it
    // before the push makes exactly `cap` rows land and the first over-cap row
    // abort — a result-size ceiling, orthogonal to the memory backstop below.
    if let Some(cap) = mspill.max_output_rows
        && output.total_rows() as u64 >= cap
    {
        return Err(PipelineError::CombineOutputCapExceeded {
            combine: mspill.name.to_string(),
            cap,
        });
    }
    let pre = output.bytes_used();
    output.push(record, key);
    mspill
        .consumer
        .add_bytes(output.bytes_used().saturating_sub(pre) as u64);
    if output.should_spill() {
        let pre_spill = output.bytes_used() as u64;
        let written = output
            .sort_and_spill()
            .map_err(|e| spill_io_error(mspill.name, "output sort spill failed", e))?;
        mspill.consumer.sub_bytes(pre_spill);
        if written > 0 && mspill.budget.record_spill_bytes(mspill.name, written) {
            return Err(PipelineError::spill_cap_exceeded(
                mspill.name,
                mspill.budget.disk_quota(),
                written,
                mspill.budget.cumulative_spill_bytes(),
            ));
        }
    }

    // Global memory backstop. The self-spilling output sort above bounds this
    // operator's own residency, but it cannot see memory other consumers hold
    // nor a Record heap the estimator under-counts. Every `MEMORY_CHECK_INTERVAL`
    // emitted rows, poll the arbitrator's global hard-limit gate so a genuine
    // over-budget condition surfaces as a typed abort rather than an OS OOM.
    let emitted = mspill.emitted_since_check.get() + 1;
    if emitted >= MEMORY_CHECK_INTERVAL {
        mspill.emitted_since_check.set(0);
        if mspill.budget.should_abort() {
            return Err(PipelineError::MemoryBudgetExceeded {
                node: mspill.name.to_string(),
                used: mspill.budget.current_pressure(),
                limit: mspill.budget.hard_limit(),
                source: BudgetCategory::Arena,
                detail: Some("sort-merge output-axis global memory backstop".to_string()),
            });
        }
    } else {
        mspill.emitted_since_check.set(emitted);
    }
    Ok(())
}

/// One driver row's identity for the emit helpers: record, order tag, and
/// unique global input index. Bundled so the emit helpers stay under clippy's
/// argument cap.
struct DriverRow<'a> {
    record: &'a Record,
    order: RecordOrder,
    idx: u64,
}

/// Deferred-failure and miss trackers threaded through the emit helpers: the
/// dead-letter rows, their parallel canonical sort tags, and the running lowest
/// `RecordOrder` miss for `on_miss: error`. Bundled to stay under the argument
/// cap.
struct FailSink<'a> {
    failures: &'a mut Vec<CombineOutputEvalFailure>,
    failure_tags: &'a mut Vec<(RecordOrder, u64, u64)>,
    min_miss: &'a mut Option<RecordOrder>,
}

/// Build and push one collect-mode row for `driver` into the output sort: even a
/// zero-match driver emits, with an empty array. Propagates the first matched
/// build's `$ck.<field>` values onto the widened row and sets the collect array
/// under the build qualifier.
fn emit_collect_row(
    ectx: &EmitCtx<'_>,
    mspill: &MergeSpill<'_>,
    driver: DriverRow<'_>,
    arr: Vec<Value>,
    first_build: Option<&Record>,
    output: &mut SortBuffer<(RecordOrder, u64, u64)>,
) -> Result<(), PipelineError> {
    let mut rec = match ectx.output_schema {
        Some(s) => widen_record_to_schema(driver.record, s),
        None => driver.record.clone(),
    };
    if let Some(b) = first_build {
        crate::executor::copy_build_ck_columns(&mut rec, b, ectx.propagate_ck);
    }
    rec.set(ectx.build_qualifier, Value::Array(arr));
    push_output_row(
        output,
        rec,
        (driver.order, driver.idx, MISS_BUILD_TAG),
        mspill,
    )
}

/// Handle one zero-match driver under `first` / `all`: `Skip` drops it, `Error`
/// records it so the walk can cite the globally lowest-`RecordOrder` miss
/// afterward (a single running minimum, not a resident vector of every
/// unmatched driver), and `NullFields` runs the body over the driver alone and
/// routes the widened row through the output sort — a recoverable eval failure
/// defers to `fail.failures` with its canonical sort tag, or fails fast.
fn handle_on_miss(
    ectx: &EmitCtx<'_>,
    mspill: &MergeSpill<'_>,
    driver: DriverRow<'_>,
    on_miss: OnMiss,
    body_eval: Option<&mut ProgramEvaluator>,
    output: &mut SortBuffer<(RecordOrder, u64, u64)>,
    fail: &mut FailSink<'_>,
) -> Result<(), PipelineError> {
    match on_miss {
        OnMiss::Skip => Ok(()),
        OnMiss::Error => {
            let m = (*fail.min_miss).map_or(driver.order, |cur| cur.min(driver.order));
            *fail.min_miss = Some(m);
            Ok(())
        }
        OnMiss::NullFields => {
            let resolver = CombineResolver::new(ectx.resolver_mapping, driver.record, None);
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
                        Some(s) => widen_record_to_schema(driver.record, s),
                        None => driver.record.clone(),
                    };
                    for (n, v) in fields {
                        rec.set(&n, v);
                    }
                    for (k, v) in *record_vars {
                        let _ = rec.set_record_var(&k, v);
                    }
                    push_output_row(
                        output,
                        rec,
                        (driver.order, driver.idx, MISS_BUILD_TAG),
                        mspill,
                    )
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
                    fail.failures.push(CombineOutputEvalFailure {
                        probe_record: driver.record.clone(),
                        row: driver.order,
                        matched_build: None,
                        error: e,
                    });
                    fail.failure_tags
                        .push((driver.order, driver.idx, MISS_BUILD_TAG));
                    Ok(())
                }
            }
        }
    }
}

/// Inputs to [`dispatch_driver_miss`], for a driver that matched nothing.
struct DispatchMiss<'a, 'b> {
    ectx: &'b EmitCtx<'a>,
    mspill: &'b MergeSpill<'b>,
    driver_record: &'b Record,
    driver_order: RecordOrder,
    driver_idx: u64,
    match_mode: MatchMode,
    on_miss: OnMiss,
    body_eval: Option<&'b mut ProgramEvaluator>,
    output: &'b mut SortBuffer<(RecordOrder, u64, u64)>,
    failures: &'b mut Vec<CombineOutputEvalFailure>,
    failure_tags: &'b mut Vec<(RecordOrder, u64, u64)>,
    min_miss: &'b mut Option<RecordOrder>,
}

/// Dispatch a driver that matched nothing (a NULL-range driver): under collect
/// it emits one empty-array row, otherwise it routes through `on_miss`.
fn dispatch_driver_miss(args: DispatchMiss<'_, '_>) -> Result<(), PipelineError> {
    let DispatchMiss {
        ectx,
        mspill,
        driver_record,
        driver_order,
        driver_idx,
        match_mode,
        on_miss,
        body_eval,
        output,
        failures,
        failure_tags,
        min_miss,
    } = args;
    let driver = DriverRow {
        record: driver_record,
        order: driver_order,
        idx: driver_idx,
    };
    if matches!(match_mode, MatchMode::Collect) {
        emit_collect_row(ectx, mspill, driver, Vec::new(), None, output)
    } else {
        let mut fail = FailSink {
            failures,
            failure_tags,
            min_miss,
        };
        handle_on_miss(ectx, mspill, driver, on_miss, body_eval, output, &mut fail)
    }
}

/// Inputs to [`emit_for_driver`]: one driver row plus its match window and the
/// output sort. Bundled so the function stays under clippy's argument cap.
struct EmitDriverArgs<'a, 'b> {
    ectx: &'b EmitCtx<'a>,
    mspill: &'b MergeSpill<'b>,
    driver_record: &'b Record,
    driver_order: RecordOrder,
    driver_idx: u64,
    window: &'b MatchWindow,
    body_eval: Option<&'b mut ProgramEvaluator>,
    decomposed: &'a DecomposedPredicate,
    match_mode: MatchMode,
    on_miss: OnMiss,
    output: &'b mut SortBuffer<(RecordOrder, u64, u64)>,
    failures: &'b mut Vec<CombineOutputEvalFailure>,
    failure_tags: &'b mut Vec<(RecordOrder, u64, u64)>,
    min_miss: &'b mut Option<RecordOrder>,
}

/// Emit the join output for one driver row from its match window. Every window
/// entry already satisfies the range predicate — window membership *is* the
/// merge walk's pull / front-drop invariant — so the residual filter and body
/// run directly, with no per-pair `apply_op`. Rows route through the
/// payload-ordered output sort, so the emit order is free: the canonical
/// `(order, driver_idx, build_idx)` order is realized by the sort regardless of
/// this walk's emit sequence. Honors `match: collect | first | all` and the
/// residual filter / body evaluator; a zero-match driver falls through to the
/// on_miss dispatch.
fn emit_for_driver(args: EmitDriverArgs<'_, '_>) -> Result<(), PipelineError> {
    let EmitDriverArgs {
        ectx,
        mspill,
        driver_record,
        driver_order,
        driver_idx,
        window,
        mut body_eval,
        decomposed,
        match_mode,
        on_miss,
        output,
        failures,
        failure_tags,
        min_miss,
    } = args;

    let name = ectx.name;
    let resolver_mapping = ectx.resolver_mapping;
    let ctx = ectx.ctx;

    match match_mode {
        MatchMode::Collect => {
            let mut arr: Vec<Value> = Vec::new();
            let mut first_build: Option<Record> = None;
            let mut truncated = false;
            for entry in window.replay(mspill) {
                let (inner, _build_key, build_idx) = entry?;
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
                            failure_tags.push((driver_order, driver_idx, build_idx));
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
                mspill,
                DriverRow {
                    record: driver_record,
                    order: driver_order,
                    idx: driver_idx,
                },
                arr,
                first_build.as_ref(),
                output,
            )
        }

        MatchMode::First | MatchMode::All => {
            let mut emitted_any = false;
            // Loop-invariant: First commits to one build and treats the body as a
            // post-match projection; All scans every window entry.
            let select_first = matches!(match_mode, MatchMode::First);
            for entry in window.replay(mspill) {
                let (inner, _build_key, build_idx) = entry?;
                let out_key = (driver_order, driver_idx, build_idx);

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
                            failure_tags.push(out_key);
                            continue;
                        }
                    }
                }

                if let Some(evaluator) = body_eval.as_deref_mut() {
                    // First selects this residual-passing build and treats the
                    // body as a post-match projection: record the predicate
                    // match up front so a body skip drops only this row instead
                    // of falling through to on_miss, then stop after this one
                    // build rather than retrying a later match.
                    if select_first {
                        emitted_any = true;
                    }
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
                            push_output_row(output, rec, out_key, mspill)?;
                            emitted_any = true;
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
                            failure_tags.push(out_key);
                            // First already committed to this build; the deferred
                            // dead-letter above is its only output and the trailing
                            // break stops the scan. All falls through to the next
                            // window entry.
                        }
                    }
                    if select_first {
                        break;
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
                    push_output_row(output, rec, out_key, mspill)?;
                    emitted_any = true;
                    if select_first {
                        break;
                    }
                } else {
                    // No body and no output schema (test-mode pass-through):
                    // emit one record per match carrying the driver record
                    // verbatim.
                    push_output_row(output, driver_record.clone(), out_key, mspill)?;
                    emitted_any = true;
                    if select_first {
                        break;
                    }
                }
            }

            if !emitted_any {
                let mut fail = FailSink {
                    failures,
                    failure_tags,
                    min_miss,
                };
                handle_on_miss(
                    ectx,
                    mspill,
                    DriverRow {
                        record: driver_record,
                        order: driver_order,
                        idx: driver_idx,
                    },
                    on_miss,
                    body_eval,
                    output,
                    &mut fail,
                )?;
            }
            Ok(())
        }
    }
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

    /// Drain a finished output handle into `(record, order)` pairs in the
    /// canonical `(order, driver_idx, build_idx)` order — the same projection
    /// the dispatcher's drain applies — so a test observes the emitted order
    /// identically whether the output stayed resident or spilled.
    fn drain_sorted(
        sorted: SortedOutput<(RecordOrder, u64, u64)>,
    ) -> Result<Vec<(Record, RecordOrder)>, PipelineError> {
        match sorted {
            SortedOutput::InMemory(pairs) => {
                Ok(pairs.into_iter().map(|(r, (o, _, _))| (r, o)).collect())
            }
            SortedOutput::Spilled(files) => {
                // Order-only drain: an unbounded-quota arbitrator suffices because
                // the test asserts the merged order, not disk accounting.
                let arbitrator =
                    MemoryArbitrator::with_policy(u64::MAX, 0.80, 0.70, Box::new(NoOpPolicy));
                let merger = SortedRunMerger::new_payload_ordered(
                    files,
                    "sort-merge test drain",
                    MergeBudget {
                        budget: &arbitrator,
                        node: "sort-merge test drain",
                        compress: true,
                    },
                )?;
                let mut v = Vec::new();
                for item in merger {
                    let (record, (order, _, _)) = item?;
                    v.push((record, order));
                }
                Ok(v)
            }
        }
    }

    /// Drive the SortMerge kernel on hand-built inputs and return the emitted
    /// (record, order) pairs alongside the stats. Panics if the kernel returns
    /// an error; over-budget-abort tests use [`run_kernel_result`] instead.
    fn run_kernel(rk: RunKernel<'_>) -> (Vec<(Record, RecordOrder)>, SortMergeStats) {
        run_kernel_result(rk).expect("sort-merge kernel execution failed")
    }

    /// Drive the SortMerge kernel, drain its bounded output handle into the
    /// emitted pairs, and return the raw `Result` so tests can assert a clean
    /// over-budget abort rather than unwrapping.
    fn run_kernel_result(
        rk: RunKernel<'_>,
    ) -> Result<(Vec<(Record, RecordOrder)>, SortMergeStats), PipelineError> {
        run_kernel_result_pinned(rk, None, None)
    }

    /// [`run_kernel_result`] with an optional encoded output schema (present, the
    /// emitted row concatenates the driver and matched-build values through the
    /// body-less synthetic path, so a test can observe the build's identity and
    /// pin tie ordering) and an optional co-resident consumer pinned to a fixed
    /// byte count and registered with the arbitrator, so the global memory
    /// backstop can be driven through the host-independent byte-counted arm of
    /// `should_abort` rather than the test process's real RSS.
    fn run_kernel_result_pinned(
        rk: RunKernel<'_>,
        output_schema: Option<&Arc<Schema>>,
        pinned_consumer_bytes: Option<u64>,
    ) -> Result<(Vec<(Record, RecordOrder)>, SortMergeStats), PipelineError> {
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
        // A co-resident consumer pinned to a fixed byte count, registered so its
        // usage sums into `should_abort`'s byte-counted arm. The arbitrator keeps
        // the `Arc` alive in its consumer list for the run's duration.
        if let Some(bytes) = pinned_consumer_bytes {
            let pinned = crate::pipeline::memory::ConsumerHandle::new();
            pinned.add_bytes(bytes);
            budget.register_consumer(Arc::new(SortMergeConsumer::new(pinned)));
        }
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
            output_schema,
            match_mode: rk.match_mode,
            on_miss: rk.on_miss,
            max_output_rows: None,
            presorted: rk.presorted,
            propagate_ck: &clinker_plan::config::pipeline_node::PropagateCkSpec::Driver,
            ctx: &ctx,
            budget: &mut budget,
            spill_dir: dir.path(),
            spill_compress: true,
            consumer_handle: crate::pipeline::memory::ConsumerHandle::new(),
            strategy: clinker_plan::config::ErrorStrategy::FailFast,
        };
        // Drain the output handle while the TempDir is still alive: a spilled
        // handle's sorted runs live inside it, so the drain must precede the drop.
        let out = match execute_combine_sort_merge_with_stats(args) {
            Ok((output, stats)) => drain_sorted(output.sorted).map(|records| (records, stats)),
            Err(e) => Err(e),
        };
        drop(dir);
        out
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

        // The tight run ran Phase A external sort on both sides and streamed
        // both from disk — the bounded path, not the resident one. Each build is
        // pulled from the sorted cursor exactly once across the whole walk (the
        // structural no-O(N*M) proof), regardless of driver count.
        assert_eq!(tight_stats.phase_a_sort_invocations, 2);
        assert_eq!(
            tight_stats.driver_stream_spilled, 1,
            "the driver side must stream from disk under the tight budget \
             (not stay resident); stats={tight_stats:?}"
        );
        assert_eq!(
            tight_stats.build_stream_spilled, 1,
            "the build side must stream from disk under the tight budget \
             (not stay resident); stats={tight_stats:?}"
        );
        assert_eq!(
            tight_stats.build_pulls, n as u64,
            "each orderable build is pulled from the sorted cursor exactly once, \
             not once per driver; stats={tight_stats:?}"
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
    /// fully resident (`SideStream::InMemory` over the whole driver vector).
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

    /// Determinism across budgets for every non-body match mode: the emitted
    /// `(order, values)` sequence must be byte-identical between a tight
    /// (spilling) budget and a roomy (resident) one, for `first`, `all`, and
    /// `collect`. Matching drivers share range key 0 — a tie — but carry a
    /// distinct `tag` equal to their input index, and matching builds a distinct
    /// `b`, so a tie permutation or a value↔order mis-pairing across budgets would
    /// change the projected sequence and fail the assertion. The payload-ordered
    /// output sort is the single sink for all three modes, so this pins that its
    /// canonical `(order, driver_idx, build_idx)` order is realized regardless of
    /// which axis spilled. (`on_miss: null_fields` routes through the identical
    /// `push_output_row` sink, so its cross-budget determinism follows by
    /// construction; the `error` path is pinned separately below.)
    #[test]
    fn test_sort_merge_determinism_across_budgets_all_modes() {
        let drivers_schema = schema_with(&["k", "pad", "tag", "builds"]);
        let builds_schema = schema_with(&["k", "pad", "b"]);
        let pad = "z".repeat(1024);
        let n = 30i64;
        let m = 30i64;
        let make_inputs = || {
            let driver: Vec<(Record, RecordOrder)> = (0..n)
                .map(|i| {
                    (
                        rec(
                            &drivers_schema,
                            vec![
                                Value::Integer(0),
                                Value::String(pad.clone().into()),
                                Value::Integer(i),
                                Value::Null,
                            ],
                        ),
                        i as RecordOrder,
                    )
                })
                .collect();
            let build: Vec<Record> = (0..m)
                .map(|j| {
                    rec(
                        &builds_schema,
                        vec![
                            Value::Integer(10),
                            Value::String(pad.clone().into()),
                            Value::Integer(j),
                        ],
                    )
                })
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
        let run = |mode: MatchMode, budget_bytes: Option<u64>| {
            let (driver, build) = make_inputs();
            run_kernel(RunKernel {
                driver_records: driver,
                build_records: build,
                decomposed: compile(),
                driver_qual: "drivers",
                build_qual: "builds",
                driver_schema: &drivers_schema,
                build_schema: &builds_schema,
                match_mode: mode,
                on_miss: OnMiss::Skip,
                presorted: false,
                body_program: None,
                budget_bytes,
            })
        };
        let project = |rs: &[(Record, RecordOrder)]| {
            rs.iter()
                .map(|(r, o)| (*o, r.values().to_vec()))
                .collect::<Vec<_>>()
        };
        for mode in [MatchMode::First, MatchMode::All, MatchMode::Collect] {
            let (tight, tight_stats) = run(mode, Some(48 * 1024));
            let (roomy, _) = run(mode, None);
            assert!(
                tight_stats.driver_stream_spilled == 1 || tight_stats.output_spilled == 1,
                "the tight budget must exercise a spill path for {mode:?}; \
                 stats={tight_stats:?}"
            );
            assert_eq!(
                project(&tight),
                project(&roomy),
                "{mode:?}: tight (spilling) and roomy (resident) output must be \
                 byte-identical"
            );
        }
    }

    /// `on_miss: error` must cite the globally lowest `RecordOrder` miss,
    /// identical across budgets and independent of the walk order — the
    /// pre-streaming flush semantics. A chained upstream stamps `RecordOrder` out
    /// of step with range-key order, and the lowest-order miss is not the
    /// first-encountered one, so an inline in-stream-order dispatch would cite the
    /// wrong row.
    #[test]
    fn test_sort_merge_on_miss_error_cites_lowest_record_order() {
        let drivers_schema = schema_with(&["k", "pad"]);
        let builds_schema = schema_with(&["k"]);
        let pad = "q".repeat(1024);
        // Drivers with range keys 100, 50, 10, 30 and RecordOrders 7, 3, 9, 2.
        // The single build has key 20, so only the key-10 driver matches
        // (10 < 20). The three misses carry orders 7, 3, 2 — lowest is 2, the
        // key-30 driver, which the range-sorted walk visits last.
        let driver_spec: [(i64, RecordOrder); 4] = [(100, 7), (50, 3), (10, 9), (30, 2)];
        let make_driver = || {
            driver_spec
                .iter()
                .map(|&(k, order)| {
                    (
                        rec(
                            &drivers_schema,
                            vec![Value::Integer(k), Value::String(pad.clone().into())],
                        ),
                        order,
                    )
                })
                .collect::<Vec<_>>()
        };
        let build = vec![rec(&builds_schema, vec![Value::Integer(20)])];
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
            run_kernel_result(RunKernel {
                driver_records: make_driver(),
                build_records: build.clone(),
                decomposed: compile(),
                driver_qual: "drivers",
                build_qual: "builds",
                driver_schema: &drivers_schema,
                build_schema: &builds_schema,
                match_mode: MatchMode::All,
                on_miss: OnMiss::Error,
                presorted: false,
                body_program: None,
                budget_bytes,
            })
        };
        for budget in [Some(20 * 1024), None] {
            match run(budget) {
                Err(PipelineError::CombineMissingMatch { driver_row, .. }) => {
                    assert_eq!(
                        driver_row, 2,
                        "on_miss: error must cite the lowest-RecordOrder miss (2), \
                         not the first visited; budget={budget:?}"
                    );
                }
                other => panic!("expected CombineMissingMatch citing row 2; got {other:?}"),
            }
        }
    }

    /// No O(N*M): a spilled build with many drivers must decode each build from
    /// its sorted input O(1) times — `build_pulls` equals the build row count,
    /// never `build_count * driver_count`. This is the structural line between a
    /// windowed merge and the per-driver full-build replay it replaces.
    #[test]
    fn test_sort_merge_no_nested_loop_pulls_each_build_once() {
        let drivers_schema = schema_with(&["k"]);
        let builds_schema = schema_with(&["k", "pad"]);
        let pad = "w".repeat(256);
        // Many drivers relative to builds is the point — `build_pulls` must equal
        // `n_build`, not `n_build * n_driver`. `n_build` stays high enough (wide
        // `pad`) to overflow the phase A spill threshold, yet the `i < j` match
        // count `(n_build-1)*n_build/2` = 7140 sits under the `MEMORY_CHECK_INTERVAL`
        // emitted-row backstop, so the tight budget exercises the spilled merge
        // without tripping the global abort on the test process's baseline RSS.
        let n_driver = 400i64;
        let n_build = 120i64;
        // Interleaved keys keep the result neither empty nor a full cross-product.
        let driver: Vec<(Record, RecordOrder)> = (0..n_driver)
            .map(|i| {
                (
                    rec(&drivers_schema, vec![Value::Integer(i)]),
                    i as RecordOrder,
                )
            })
            .collect();
        let build: Vec<Record> = (0..n_build)
            .map(|j| {
                rec(
                    &builds_schema,
                    vec![Value::Integer(j), Value::String(pad.clone().into())],
                )
            })
            .collect();
        // Brute-force oracle: driver i (key i) matches build j (key j) iff i < j.
        let mut oracle: Vec<RecordOrder> = Vec::new();
        for i in 0..n_driver {
            for j in 0..n_build {
                if i < j {
                    oracle.push(i as RecordOrder);
                }
            }
        }
        oracle.sort_unstable();

        let typed = compile_pure_range(
            "filter drivers.k < builds.k",
            &[
                ("drivers", "k", cxl::typecheck::Type::Int),
                ("builds", "k", cxl::typecheck::Type::Int),
            ],
        );
        let rc = extract_range_conjunct(&typed, "drivers", "builds");
        let decomposed = decomposed_pure_range(rc, Arc::clone(&typed));
        // Tight budget so the build cursor spills — the case where the old
        // per-driver replay re-decoded the whole build from disk every driver.
        let (records, stats) = run_kernel(RunKernel {
            driver_records: driver,
            build_records: build,
            decomposed,
            driver_qual: "drivers",
            build_qual: "builds",
            driver_schema: &drivers_schema,
            build_schema: &builds_schema,
            match_mode: MatchMode::All,
            on_miss: OnMiss::Skip,
            presorted: false,
            body_program: None,
            budget_bytes: Some(32 * 1024),
        });
        assert_eq!(
            stats.build_stream_spilled, 1,
            "the build must spill so the assertion exercises the on-disk path; \
             stats={stats:?}"
        );
        assert_eq!(
            stats.build_pulls,
            n_build as u64,
            "each build is pulled from the sorted cursor exactly once ({n_build}), \
             not once per driver ({}); stats={stats:?}",
            n_build * n_driver
        );
        let mut orders: Vec<RecordOrder> = records.iter().map(|(_, o)| *o).collect();
        orders.sort_unstable();
        assert_eq!(
            orders, oracle,
            "windowed merge must match the brute-force oracle"
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

    /// `match: collect` over a tight budget must stay bounded by *spilling* the
    /// output — not by aborting, and not by growing unbounded. A high-cardinality
    /// driver side with a permissive predicate accumulates one collect row per
    /// driver; the payload-ordered output sort spills past its byte threshold, so
    /// the join completes within budget and emits exactly one row per driver.
    #[test]
    fn test_sort_merge_collect_stays_bounded_by_spilling() {
        // The driver schema carries the build-qualifier column ("builds"),
        // standing in for the plan-time-widened output schema the collect flush
        // writes its array into (run_kernel passes no separate output schema).
        let drivers_schema = schema_with(&["k", "builds"]);
        let builds_schema = schema_with(&["k", "b"]);
        // Every driver (key 0) matches every build (key 1000): a permissive
        // predicate making each collect array non-empty. Kept under the
        // `MEMORY_CHECK_INTERVAL` emitted-row backstop so this tight-budget run
        // demonstrates the output self-spill in isolation — the global abort
        // backstop (which trips on the test process's own baseline RSS at a
        // sub-baseline budget) is exercised separately by the backstop test.
        let n_drivers = 8_000usize;
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

        // 64 KiB budget: the accumulated collect output dwarfs it, so the output
        // sort spills and the join completes bounded instead of aborting.
        let (records, stats) = run_kernel_result(RunKernel {
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
        })
        .expect("collect over a tight budget must complete by spilling, not abort");
        assert_eq!(
            records.len(),
            n_drivers,
            "collect emits exactly one row per driver"
        );
        assert_eq!(
            stats.output_spilled, 1,
            "the collect output must spill under the tight budget rather than \
             growing unbounded in RAM; stats={stats:?}"
        );
        // Every collect array holds all four matching builds (0 < 1000).
        for (rec, _order) in &records {
            match rec.get("builds") {
                Some(Value::Array(a)) => {
                    assert_eq!(a.len(), 4, "each driver collects all 4 builds")
                }
                other => panic!("collect row must carry a 4-element array; got {other:?}"),
            }
        }
    }

    /// The self-spilling output sort bounds this operator's own residency, but a
    /// global over-budget condition driven by *another* consumer's memory (or a
    /// Record heap the estimator under-counts) is invisible to it. The merge walk
    /// polls the arbitrator's global hard-limit gate every `MEMORY_CHECK_INTERVAL`
    /// emitted rows so such a breach surfaces as a clean typed
    /// `MemoryBudgetExceeded`, never an OS OOM. A co-resident consumer pinned
    /// above the hard limit trips the byte-counted arm of `should_abort` — host
    /// independent, with no reliance on the test process's real RSS — while an
    /// otherwise-identical run without it completes, proving the poll neither
    /// spuriously aborts nor misses the breach.
    #[test]
    fn test_sort_merge_output_backstop_aborts_on_global_pressure() {
        let drivers_schema = schema_with(&["k"]);
        let builds_schema = schema_with(&["k"]);
        // One build above every driver key: under `drivers.k < builds.k` every
        // driver matches the single build, so `match: all` emits one row per
        // driver. More than `MEMORY_CHECK_INTERVAL` drivers so the walk polls the
        // backstop at least once.
        let n_drivers = MEMORY_CHECK_INTERVAL + 1_000;
        let build = vec![rec(&builds_schema, vec![Value::Integer(5_000_000)])];
        let make_drivers = || {
            (0..n_drivers)
                .map(|i| {
                    (
                        rec(&drivers_schema, vec![Value::Integer(i as i64)]),
                        i as RecordOrder,
                    )
                })
                .collect::<Vec<_>>()
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
        // A hard limit far above any plausible test-process RSS, so `should_abort`
        // can only trip through the byte-counted consumer sum, never real RSS.
        let hard = 8u64 * 1024 * 1024 * 1024;
        let run = |pinned: Option<u64>| {
            run_kernel_result_pinned(
                RunKernel {
                    driver_records: make_drivers(),
                    build_records: build.clone(),
                    decomposed: compile(),
                    driver_qual: "drivers",
                    build_qual: "builds",
                    driver_schema: &drivers_schema,
                    build_schema: &builds_schema,
                    match_mode: MatchMode::All,
                    on_miss: OnMiss::Skip,
                    presorted: true,
                    body_program: None,
                    budget_bytes: Some(hard),
                },
                None,
                pinned,
            )
        };

        // Control: no external pressure — the walk polls the backstop but the
        // arbitrator is under budget, so it completes and emits one row per driver.
        let (records, _stats) =
            run(None).expect("without external pressure the backstop must not abort");
        assert_eq!(
            records.len(),
            n_drivers,
            "the unpressured walk emits one row per driver"
        );

        // Pin a consumer above the hard limit: the byte-counted arm of
        // `should_abort` trips at the first poll and the walk aborts cleanly.
        let err = run(Some(hard + 64 * 1024))
            .expect_err("a co-resident consumer over the hard limit must abort the walk");
        match err {
            PipelineError::MemoryBudgetExceeded {
                node,
                limit,
                source,
                ..
            } => {
                assert_eq!(node, "sm_test");
                assert_eq!(limit, hard);
                assert_eq!(source, BudgetCategory::Arena);
            }
            other => {
                panic!("the global backstop must surface MemoryBudgetExceeded; got {other:?}")
            }
        }
    }

    /// Build `n` phase A driver pairs whose sort key `k` is an Integer and
    /// whose wide `pad` column pushes the accumulated buffer past the phase A
    /// 16 KiB spill-threshold floor after a handful of records. Shape is the
    /// side cursor's `(record, key, payload)` with the driver payload
    /// `(order, driver_idx)`.
    fn phase_a_driver_pairs(n: usize) -> Vec<(Record, Value, (RecordOrder, u64))> {
        let schema = schema_with(&["k", "pad"]);
        let pad = "x".repeat(1024);
        (0..n)
            .map(|i| {
                let r = rec(
                    &schema,
                    vec![Value::Integer(i as i64), Value::String(pad.as_str().into())],
                );
                (r, Value::Integer(i as i64), (i as RecordOrder, i as u64))
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
        let err = sort_side_stream(SideStreamBuild {
            pairs,
            name: "sm",
            range_field: &Some("k".to_string()),
            budget: &budget,
            spill_compress: true,
            consumer_handle: &handle,
            spill_dir: dir.path(),
            presorted: false,
            side: "driver",
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
        let pairs: Vec<(Record, Value, u64)> = (0..64i64)
            .map(|i| {
                let r = rec(
                    &schema,
                    vec![Value::Integer(i), Value::String(pad.as_str().into())],
                );
                (r, Value::Integer(i), i as u64)
            })
            .collect();
        let budget = MemoryArbitrator::with_policy(1024, 0.80, 0.70, Box::new(NoOpPolicy));
        budget.set_max_spill_bytes(1);
        let dir = tempfile::tempdir().unwrap();
        let handle = crate::pipeline::memory::ConsumerHandle::new();
        let err = sort_side_stream(SideStreamBuild {
            pairs,
            name: "sm",
            range_field: &Some("k".to_string()),
            budget: &budget,
            spill_compress: true,
            consumer_handle: &handle,
            spill_dir: dir.path(),
            presorted: false,
            side: "build",
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
        let err = sort_side_stream(SideStreamBuild {
            pairs,
            name: "sm",
            range_field: &Some("k".to_string()),
            budget: &budget,
            spill_compress: true,
            consumer_handle: &handle,
            spill_dir: &spill_root,
            presorted: false,
            side: "driver",
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

    /// Drive the Phase-B windowed merge for one `predicate` (whose operands are
    /// the driver `drivers` and build `builds` in either textual order — a
    /// flipped orientation exercises the range-op flip in the kernel) against a
    /// brute-force native oracle, resident and pre-sorted, at a spilling and a
    /// roomy budget. Every operator routes through here, not just `<`.
    ///
    /// The fixed dataset carries duplicate range keys on both sides (tie
    /// handling), boundary equality (an inclusive op admits a pair an exclusive
    /// op rejects), and driver `RecordOrder`s scrambled relative to range order
    /// with one deliberately repeated order — so the canonical
    /// `(order, driver_idx, build_idx)` output ordering is genuinely pinned. The
    /// emitted row concatenates driver+build values under an encoded output
    /// schema, so a distinct per-side tag column identifies each matched pair: a
    /// dropped, duplicated, or mis-ordered join row fails the assertion, not just
    /// a wrong row count.
    fn assert_range_walk_matches_oracle(
        predicate: &str,
        pred_left: &str,
        pred_right: &str,
        native: impl Fn(i64, i64) -> bool + Copy,
    ) {
        let drivers_schema = schema_with(&["k", "dtag", "pad"]);
        let builds_schema = schema_with(&["k", "btag", "pad"]);
        let output_schema = schema_with(&["dk", "dtag", "dpad", "bk", "btag", "bpad"]);
        let pad = "p".repeat(1024);

        // Driver range keys 0..=5 (4 of each → ties), build keys 0..=4 (4 of each
        // → ties). Equal keys 0..4 exist on both sides, so an inclusive op admits a
        // boundary pair an exclusive op rejects; driver key 5 sits above every
        // build, and no build key 5 sits above every driver — both open ends.
        let n_driver = 24usize;
        let n_build = 20usize;
        let driver_key = |i: usize| (i % 6) as i64;
        let build_key = |j: usize| (j % 5) as i64;
        // Scrambled orders (a permutation of 0..n_driver), then force driver 7 to
        // share driver 19's order so the driver-idx tie-break under a repeated
        // `RecordOrder` is exercised.
        let driver_order = |i: usize| -> RecordOrder {
            if i == 7 {
                ((19 * 5) % n_driver) as RecordOrder
            } else {
                ((i * 5) % n_driver) as RecordOrder
            }
        };

        for presorted in [false, true] {
            // Pre-sorted requires an ascending range key (stable, so equal keys
            // keep index order); the resident path feeds a descending arrangement
            // so Phase A's external sort actually reorders. `dtag`/`btag` are the
            // final input position == the kernel's `driver_idx`/`build_idx`, so the
            // oracle's canonical `(order, driver_idx, build_idx)` maps directly onto
            // the emitted `(order, dtag, btag)`.
            let mut driver_layout: Vec<usize> = (0..n_driver).collect();
            let mut build_layout: Vec<usize> = (0..n_build).collect();
            if presorted {
                driver_layout.sort_by_key(|&i| driver_key(i));
                build_layout.sort_by_key(|&j| build_key(j));
            } else {
                driver_layout.sort_by_key(|&i| std::cmp::Reverse(driver_key(i)));
                build_layout.sort_by_key(|&j| std::cmp::Reverse(build_key(j)));
            }
            let driver_recs: Vec<(Record, RecordOrder)> = driver_layout
                .iter()
                .enumerate()
                .map(|(pos, &i)| {
                    (
                        rec(
                            &drivers_schema,
                            vec![
                                Value::Integer(driver_key(i)),
                                Value::Integer(pos as i64),
                                Value::String(pad.as_str().into()),
                            ],
                        ),
                        driver_order(i),
                    )
                })
                .collect();
            let build_recs: Vec<Record> = build_layout
                .iter()
                .enumerate()
                .map(|(pos, &j)| {
                    rec(
                        &builds_schema,
                        vec![
                            Value::Integer(build_key(j)),
                            Value::Integer(pos as i64),
                            Value::String(pad.as_str().into()),
                        ],
                    )
                })
                .collect();

            // Brute-force oracle over the positioned inputs: driver `di` matches
            // build `bi` iff the native predicate holds; canonical order is
            // `(driver_order, di, bi)`.
            let mut oracle: Vec<(RecordOrder, i64, i64)> = Vec::new();
            for (di, (drec, dorder)) in driver_recs.iter().enumerate() {
                let dk = match &drec.values()[0] {
                    Value::Integer(v) => *v,
                    other => panic!("driver key not int: {other:?}"),
                };
                for (bi, brec) in build_recs.iter().enumerate() {
                    let bk = match &brec.values()[0] {
                        Value::Integer(v) => *v,
                        other => panic!("build key not int: {other:?}"),
                    };
                    if native(dk, bk) {
                        oracle.push((*dorder, di as i64, bi as i64));
                    }
                }
            }
            oracle.sort_unstable();

            let compile = || {
                let typed = compile_pure_range(
                    predicate,
                    &[
                        (pred_left, "k", cxl::typecheck::Type::Int),
                        (pred_right, "k", cxl::typecheck::Type::Int),
                    ],
                );
                let rc = extract_range_conjunct(&typed, pred_left, pred_right);
                decomposed_pure_range(rc, Arc::clone(&typed))
            };
            let run = |budget_bytes: Option<u64>| {
                run_kernel_result_pinned(
                    RunKernel {
                        driver_records: driver_recs.clone(),
                        build_records: build_recs.clone(),
                        decomposed: compile(),
                        driver_qual: "drivers",
                        build_qual: "builds",
                        driver_schema: &drivers_schema,
                        build_schema: &builds_schema,
                        match_mode: MatchMode::All,
                        on_miss: OnMiss::Skip,
                        presorted,
                        body_program: None,
                        budget_bytes,
                    },
                    Some(&output_schema),
                    None,
                )
                .expect("sort-merge kernel execution failed")
            };
            // Emitted row = driver.values() ++ build.values(): dtag at index 1,
            // btag at index 4.
            let project = |records: &[(Record, RecordOrder)]| -> Vec<(RecordOrder, i64, i64)> {
                records
                    .iter()
                    .map(|(r, o)| {
                        let dtag = match &r.values()[1] {
                            Value::Integer(v) => *v,
                            other => panic!("dtag not int: {other:?}"),
                        };
                        let btag = match &r.values()[4] {
                            Value::Integer(v) => *v,
                            other => panic!("btag not int: {other:?}"),
                        };
                        (*o, dtag, btag)
                    })
                    .collect()
            };

            let (tight, tight_stats) = run(Some(32 * 1024));
            let (roomy, _roomy_stats) = run(None);

            assert!(
                tight_stats.driver_stream_spilled == 1
                    || tight_stats.build_stream_spilled == 1
                    || tight_stats.output_spilled == 1
                    || tight_stats.matching_run_spills == 1,
                "{predicate:?} (presorted={presorted}): the tight budget must exercise \
                 a spill path; stats={tight_stats:?}"
            );
            assert_eq!(
                project(&tight),
                oracle,
                "{predicate:?} (presorted={presorted}): tight (spilling) windowed merge \
                 must equal the brute-force nested-loop oracle"
            );
            assert_eq!(
                project(&roomy),
                oracle,
                "{predicate:?} (presorted={presorted}): roomy (resident) output must \
                 equal the oracle — determinism across budgets"
            );
        }
    }

    /// Direct orientation (`drivers OP builds`): the windowed merge must equal the
    /// nested-loop oracle for every one of the four range operators, not just the
    /// `<` the rest of the suite covers. `<`/`<=` walk the suffix front-drop path,
    /// `>`/`>=` the prefix pending-hold/growing-window path; `<=`/`>=` exercise
    /// inclusive-boundary handling.
    #[test]
    fn test_sort_merge_walk_all_ops_direct() {
        assert_range_walk_matches_oracle(
            "filter drivers.k < builds.k",
            "drivers",
            "builds",
            |d, b| d < b,
        );
        assert_range_walk_matches_oracle(
            "filter drivers.k <= builds.k",
            "drivers",
            "builds",
            |d, b| d <= b,
        );
        assert_range_walk_matches_oracle(
            "filter drivers.k > builds.k",
            "drivers",
            "builds",
            |d, b| d > b,
        );
        assert_range_walk_matches_oracle(
            "filter drivers.k >= builds.k",
            "drivers",
            "builds",
            |d, b| d >= b,
        );
    }

    /// Flipped orientation (`builds OP drivers`, build on the left): the kernel
    /// flips the operator so the predicate reads `driver OP' build`. Each of the
    /// four operators flips to a different one, so this covers the flip path for
    /// all of them and re-confirms the resulting walk against the oracle.
    #[test]
    fn test_sort_merge_walk_all_ops_flipped() {
        // builds.k < drivers.k  ⇒ driver > build (prefix)
        assert_range_walk_matches_oracle(
            "filter builds.k < drivers.k",
            "builds",
            "drivers",
            |d, b| b < d,
        );
        // builds.k <= drivers.k ⇒ driver >= build (prefix, inclusive)
        assert_range_walk_matches_oracle(
            "filter builds.k <= drivers.k",
            "builds",
            "drivers",
            |d, b| b <= d,
        );
        // builds.k > drivers.k  ⇒ driver < build (suffix)
        assert_range_walk_matches_oracle(
            "filter builds.k > drivers.k",
            "builds",
            "drivers",
            |d, b| b > d,
        );
        // builds.k >= drivers.k ⇒ driver <= build (suffix, inclusive)
        assert_range_walk_matches_oracle(
            "filter builds.k >= drivers.k",
            "builds",
            "drivers",
            |d, b| b >= d,
        );
    }

    /// Boundary (equal keys) explicitly: one driver key equal to the middle of
    /// three straddling build keys, so only the equal-key build separates the
    /// inclusive ops (`<=`, `>=`) from the exclusive ones (`<`, `>`). Pins that
    /// `apply_op` includes/excludes the tie exactly as CXL comparison does.
    #[test]
    fn test_sort_merge_walk_boundary_inclusive_vs_exclusive() {
        let drivers_schema = schema_with(&["k", "dtag"]);
        let builds_schema = schema_with(&["k", "btag"]);
        let output_schema = schema_with(&["dk", "dtag", "bk", "btag"]);
        // Driver key 5; builds keys 4,5,6 with btags 0,1,2 — btag 1 is the equal
        // key, admitted only by inclusive ops.
        let driver = || {
            vec![(
                rec(&drivers_schema, vec![Value::Integer(5), Value::Integer(0)]),
                0,
            )]
        };
        let build = || {
            vec![
                rec(&builds_schema, vec![Value::Integer(4), Value::Integer(0)]),
                rec(&builds_schema, vec![Value::Integer(5), Value::Integer(1)]),
                rec(&builds_schema, vec![Value::Integer(6), Value::Integer(2)]),
            ]
        };
        let run_op = |predicate: &str| -> Vec<i64> {
            let typed = compile_pure_range(
                predicate,
                &[
                    ("drivers", "k", cxl::typecheck::Type::Int),
                    ("builds", "k", cxl::typecheck::Type::Int),
                ],
            );
            let rc = extract_range_conjunct(&typed, "drivers", "builds");
            let decomposed = decomposed_pure_range(rc, Arc::clone(&typed));
            let (records, _) = run_kernel_result_pinned(
                RunKernel {
                    driver_records: driver(),
                    build_records: build(),
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
                },
                Some(&output_schema),
                None,
            )
            .expect("boundary walk failed");
            // btag rides at concat index 3 (driver 2 cols + build[1]).
            let mut btags: Vec<i64> = records
                .iter()
                .map(|(r, _)| match &r.values()[3] {
                    Value::Integer(v) => *v,
                    other => panic!("btag not int: {other:?}"),
                })
                .collect();
            btags.sort_unstable();
            btags
        };
        assert_eq!(
            run_op("filter drivers.k < builds.k"),
            vec![2],
            "Lt excludes the equal key: only build > 5"
        );
        assert_eq!(
            run_op("filter drivers.k <= builds.k"),
            vec![1, 2],
            "Le includes the equal key: build >= 5"
        );
        assert_eq!(
            run_op("filter drivers.k > builds.k"),
            vec![0],
            "Gt excludes the equal key: only build < 5"
        );
        assert_eq!(
            run_op("filter drivers.k >= builds.k"),
            vec![0, 1],
            "Ge includes the equal key: build <= 5"
        );
    }

    /// A pre-sorted certification that is actually out of order must fail loud in
    /// release, not silently walk in place and diverge from the spilling path
    /// (which would re-sort). The ascending-order guard on the driver-side
    /// walk-in-place path returns a typed `Internal` for a descending driver.
    #[test]
    fn test_sort_merge_presorted_unsorted_driver_fails() {
        let drivers_schema = schema_with(&["k"]);
        let builds_schema = schema_with(&["k"]);
        // Range keys 3,1,2 — out of ascending order despite the presorted claim.
        let driver = vec![
            (rec(&drivers_schema, vec![Value::Integer(3)]), 0),
            (rec(&drivers_schema, vec![Value::Integer(1)]), 1),
            (rec(&drivers_schema, vec![Value::Integer(2)]), 2),
        ];
        let build = vec![rec(&builds_schema, vec![Value::Integer(10)])];
        let typed = compile_pure_range(
            "filter drivers.k < builds.k",
            &[
                ("drivers", "k", cxl::typecheck::Type::Int),
                ("builds", "k", cxl::typecheck::Type::Int),
            ],
        );
        let rc = extract_range_conjunct(&typed, "drivers", "builds");
        let decomposed = decomposed_pure_range(rc, Arc::clone(&typed));
        let err = run_kernel_result(RunKernel {
            driver_records: driver,
            build_records: build,
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
        })
        .expect_err("a mis-certified pre-sorted (descending) driver must fail loud");
        match err {
            PipelineError::Internal { op, detail, .. } => {
                assert_eq!(op, "combine");
                assert!(
                    detail.contains("not ascending") && detail.contains("driver"),
                    "expected the driver ascending-order guard message; got: {detail}"
                );
            }
            other => panic!("expected Internal from the pre-sorted order guard; got {other:?}"),
        }
    }

    /// The build side mirrors the driver: a mis-certified pre-sorted build (the
    /// driver ascends cleanly, the build does not) fails loud through the same
    /// release-mode guard rather than diverging across budgets.
    #[test]
    fn test_sort_merge_presorted_unsorted_build_fails() {
        let drivers_schema = schema_with(&["k"]);
        let builds_schema = schema_with(&["k"]);
        let driver = vec![
            (rec(&drivers_schema, vec![Value::Integer(1)]), 0),
            (rec(&drivers_schema, vec![Value::Integer(2)]), 1),
        ];
        // Build range keys 10,5 — descending despite the presorted claim.
        let build = vec![
            rec(&builds_schema, vec![Value::Integer(10)]),
            rec(&builds_schema, vec![Value::Integer(5)]),
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
        let err = run_kernel_result(RunKernel {
            driver_records: driver,
            build_records: build,
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
        })
        .expect_err("a mis-certified pre-sorted (descending) build must fail loud");
        match err {
            PipelineError::Internal { op, detail, .. } => {
                assert_eq!(op, "combine");
                assert!(
                    detail.contains("not ascending") && detail.contains("build"),
                    "expected the build ascending-order guard message; got: {detail}"
                );
            }
            other => panic!("expected Internal from the pre-sorted order guard; got {other:?}"),
        }
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
