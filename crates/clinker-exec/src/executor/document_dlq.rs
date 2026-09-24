//! Document-level dead-lettering for sources declaring
//! `dlq_granularity: document`.
//!
//! Under the default `record` granularity a record failure dead-letters
//! only that record; under `document` it dead-letters the entire document
//! the failing record belongs to. The shape mirrors the correlation-group
//! commit (buffer-until-boundary, flush clean / DLQ dirty, trigger /
//! collateral split) but the memory model is the per-document Aggregate
//! flush: records buffer per document and each document's bucket DROPS at
//! its boundary, so a closed document's records leave RAM before the next
//! file opens. A buffered bucket SPILLS to disk under the shared
//! [`crate::pipeline::memory::MemoryArbitrator`] budget rather than OOM-ing,
//! every bucket registering its own consumer the same way the Aggregate
//! per-document tables do.
//!
//! ## Document grain
//!
//! The document is the OUTERMOST envelope level — the source file (an EDI
//! interchange, a batch file with header/trailer). A nested-envelope format
//! (X12 ISA → GS → ST) stamps each record with its innermost level's id,
//! but every level of one file shares the file's `source_file` Arc, so the
//! buffer keys on `source_file`: a failure anywhere in the file rejects the
//! whole file, and a record arriving between two nested-level boundaries
//! still belongs to its file's bucket. A flat single-level file (CSV, JSON,
//! plain XML) has innermost == outermost == the file, so the grain is the
//! file there too. The file's bucket decides when its OUTERMOST close
//! arrives — tracked by per-file envelope depth returning to zero (nested
//! closes leave the file open) — or at end-of-input for an unterminated
//! file.
//!
//! ## Streaming
//!
//! Streaming is disabled pipeline-wide when any source declares the
//! `document` granularity (see `PipelineConfig::any_source_has_document_dlq`):
//! per-document buffering at the Output arm needs the materialized
//! `DocumentClose` punctuation the streaming-Output / streaming-ingest
//! short-circuits would otherwise consume out of band.
//!
//! ## DLQ rate
//!
//! The rate counts per row: a rejected N-record document emits one
//! `trigger: true` root cause plus N-1 `trigger: false` collateral entries,
//! each counting toward the rate denominator — so a rejected 1000-record
//! document contributes 1000 to the DLQ rate, matching the correlation
//! collateral precedent. A row held by several Sinks is written, and
//! counted, once: each failed document keeps a ledger of the rows already
//! written (see `EmittedRows`), and a later Sink writes only rows no earlier
//! rejection wrote.
//!
//! ## Spill correctness
//!
//! Document identity survives spilling end to end. The driver's own
//! per-document bucket keys records by `source_file` BEFORE spilling them,
//! and appends only its in-memory tail to a new chunk so repeated spills are
//! O(total records), not O(records²). Upstream is covered too: a record's
//! document context — including the `source_file` the grain keys on — now
//! rides through the shared record-spill format, so a record arriving via an
//! UPSTREAM node-buffer that spilled keeps its file identity and is still
//! governed by the policy.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clinker_record::{DocumentId, Record};

use crate::executor::dispatch::{
    ExecutorContext, MERGED_SOURCE_FILE, mapping_probe, push_dlq, source_file_arc_of,
    source_name_arc_of,
};
use crate::executor::node_buffer::NodeBuffer;
use crate::executor::sink_dispatch::OrderedWriterBoundary;
use crate::executor::stream_event::{SourceRowId, StreamEvent};
use crate::executor::structured_output_guard::StructuredOutputDocumentGuard;
use crate::executor::{DlqEntry, DlqFailureStamp, build_format_writer};
use crate::pipeline::memory::{
    ConsumerHandle, ConsumerId, ConsumerSpillError, MemoryArbitrator, MemoryConsumer,
};
use clinker_plan::config::SinkConfig;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::PlanNodeId;
use roaring::RoaringTreemap;

/// Identity of one document the policy operates on: its source file (the
/// outermost envelope level). Every record and boundary of a file shares
/// this `Arc<str>`, so it is the grain a failure rejects at.
type DocKey = Arc<str>;

/// Root-cause failure captured for a document the first time one of its
/// records (or an out-of-band validation) fails. Replayed as the single
/// `trigger: true` DLQ entry when the document is rejected.
struct DocTrigger {
    source_row: SourceRowId,
    category: clinker_core_types::dlq::DlqErrorCategory,
    error_message: String,
    original_record: Record,
    stage: Option<String>,
    route: Option<String>,
    source_name: Arc<str>,
    triggering_field: Option<Arc<str>>,
    triggering_value: Option<clinker_record::Value>,
    /// Taken when the failure marked the document; the trigger entry
    /// emitted at the document's reject carries it.
    failed_at: DlqFailureStamp,
}

/// A document's run-wide failed verdict. Once a document is marked failed it
/// stays failed until the run ends: every Sink holding any of its records
/// rejects it, however many Sinks read them.
struct FailedDocument {
    /// The stamp of the document's first failure. Every collateral of the
    /// document, at any Sink and at any time, is condemned by it.
    cause: DlqFailureStamp,
    /// The first failure, until its trigger entry is written. The first
    /// rejection takes it; a document still holding it at the end of the
    /// run never reached a Sink and is rejected by the end sweep.
    trigger: Option<DocTrigger>,
    /// The rows of this document already written to the dead-letter output,
    /// so each is written once however many Sinks hold it.
    emitted: EmittedRows,
}

// The ledger's charge model. Each figure is derived from the roaring 0.11.5
// source (paths under its `src/`) and is an upper bound on the heap the
// structure holds, so the charge stays at or above the ledger's heap between
// settles. Vectors there grow by doubling, so a vector holds at most
// `max(4, 2 * len)` elements; the model charges that capacity.

/// Heap of one ordered-map node in a treemap: `RoaringTreemap` is a
/// `BTreeMap<u32, RoaringBitmap>` (`treemap/mod.rs:40-42`). An internal node
/// holds a parent pointer, two `u16` counters, 11 keys, 11 values and 12 edge
/// pointers; a leaf holds less. A B-tree has no more nodes than keys, so one
/// node per upper-32-bit key bounds the map.
const BTREE_NODE_BYTES: u64 =
    (8 + 4 + 11 * 4 + 11 * std::mem::size_of::<roaring::RoaringBitmap>() + 12 * 8) as u64;

/// One container slot in a bitmap's `Vec<Container>` (`bitmap/mod.rs:48-50`):
/// a `u16` key and a `Store` (`bitmap/container.rs:17-20`), whose largest
/// variant is one vector (`bitmap/store/mod.rs:28-32`), plus a tag the
/// compiler may not fold into a niche, rounded to 8-byte alignment.
const CONTAINER_BYTES: u64 = (2 + std::mem::size_of::<Vec<u16>>() + 8).next_multiple_of(8) as u64;

/// A bitmap container's fixed `Box<[u64; 1024]>` (`bitmap/store/bitmap_store.rs:15-22`).
/// An array container becomes one past 4,096 values (`bitmap/container.rs:9`,
/// `:225-241`), when its 4,096 recorded rows were already charged more.
const BITMAP_CONTAINER_BYTES: u64 = 8192;

/// Growth charged for an admitted row. An array container stores a `u16` per
/// value (`bitmap/store/array_store/mod.rs:24-26`, inserted at `:127-129`); a
/// run container adds at most one 4-byte interval per insert
/// (`bitmap/store/interval_store.rs:63-103`, `:907-910`). Doubled for vector
/// capacity, the larger is 8 bytes.
const ROW_ADMISSION_BYTES: u64 = 8;

/// Growth charged for a row that may open a new 65,536-value container
/// (`bitmap/inherent.rs:187-197`): one container slot, doubled for the
/// containers vector's capacity.
const NEW_CONTAINER_BYTES: u64 = 2 * CONTAINER_BYTES;

/// Growth charged for a row that may open a new upper-32-bit bitmap
/// (`treemap/inherent.rs:51-54`): one map node and the new bitmap's first
/// containers vector, which holds four slots.
const NEW_HIGH_KEY_BYTES: u64 = BTREE_NODE_BYTES + 4 * CONTAINER_BYTES;

/// Growth charged for a row whose two neighbours in its container are
/// already recorded while the treemap holds a run container. In a run
/// container that insert merges two intervals and removes one without
/// shrinking the vector (`bitmap/store/interval_store.rs:85`), so the freed
/// interval's capacity stays charged until a settle finds no run container,
/// or finds this slack larger than the rest of the ledger and rebuilds the
/// treemap with exact capacities (a clone of a vector keeps only its length).
const MERGED_INTERVAL_BYTES: u64 = 8;

/// One `(Source, rows)` slot of [`EmittedRows::sources`].
const SOURCE_ENTRY_BYTES: u64 = std::mem::size_of::<(PlanNodeId, SourceEmittedRows)>() as u64;

/// The most one admission can add to the ledger's charge: a merging row that
/// opens a new Source entry, bitmap and container. Every admission
/// preflights its own growth, which never exceeds this.
const MAX_ADMISSION_BYTES: u64 = ROW_ADMISSION_BYTES
    + MERGED_INTERVAL_BYTES
    + NEW_CONTAINER_BYTES
    + NEW_HIGH_KEY_BYTES
    + 4 * SOURCE_ENTRY_BYTES;

/// A ledger settles after this many admissions within one rejection pass, as
/// well as at the end of every pass, so the per-admission charges a long pass
/// accumulates are replaced by the compressed size.
const SETTLE_EVERY_ADMISSIONS: u64 = 65_536;

/// Fixed charge for each failed document's map slot, taken when the document
/// is first marked failed and released with the state. A hash map holds at
/// most about 2.3 slots per entry after growing, so three are charged.
const FAILED_DOCUMENT_BYTES: u64 = 3 * (std::mem::size_of::<(DocKey, FailedDocument)>() as u64 + 1);

/// Which rows of one rejected document have been dead-lettered, as one
/// [`RoaringTreemap`] per Source keyed by the row's absolute
/// [`SourceRowId`] ordinal.
///
/// Absolute ordinals need no record of where the document starts: a Source
/// mints one ordinal per row across every file it reads, so one document's
/// rows are one contiguous run of its Source's ordinals, whichever Sink sees
/// them first. The ordinals are `u64`, so the 64-bit treemap rather than the
/// 32-bit bitmap holds them.
///
/// Roaring splits the ordinals into 65,536-value containers and keeps each as
/// a sorted array (2 bytes per value), a bitmap (8 KiB) or a list of runs,
/// whichever is smaller once the treemap is optimized. A contiguous document
/// settles to one run per container, a few dozen bytes. No settled container
/// is charged more than about 16 KiB for its 65,536 ordinals (an array of
/// 4,096 values with vector growth), which is under a quarter of a byte per
/// row of the document, plus a header of a few dozen bytes; before a settle
/// each admission is charged at most [`MAX_ADMISSION_BYTES`].
///
/// The ledger is exact dedup state and does not spill. Its charge is an upper
/// bound on its heap: each admission is charged its worst-case growth (at
/// most [`MAX_ADMISSION_BYTES`]) and preflighted against the hard limit, and
/// [`EmittedRows::settle`] replaces the accumulated admissions with the
/// compressed structure's bound. Growth past the hard limit fails the run
/// with E310 rather than spilling.
///
/// There is more than one entry only when two Sources read the same file
/// path, which then names one document (#1233).
struct EmittedRows {
    sources: Vec<(PlanNodeId, SourceEmittedRows)>,
    /// Bytes charged for this ledger to the document state's consumer.
    charged: u64,
    /// Admissions since the last settle.
    unsettled: u64,
}

/// One Source's recorded rows of a rejected document.
struct SourceEmittedRows {
    rows: RoaringTreemap,
    /// `ordinal >> 16` of the last admitted row. Its container exists, so the
    /// next row in the same container cannot open a new one.
    last_container: Option<u64>,
    /// Interval capacity merges may have left behind in run containers.
    merged_slack: u64,
    /// Whether the last settle left a run container. Containers become runs
    /// only when a settle optimizes them, so until then no merge can leave
    /// interval capacity behind.
    has_runs: bool,
}

/// What admitting one row costs.
struct Admission {
    growth: u64,
    merges: bool,
}

impl EmittedRows {
    fn new() -> Self {
        Self {
            sources: Vec::new(),
            charged: 0,
            unsettled: 0,
        }
    }

    /// What admitting `row` may add to the ledger, or `None` when the row is
    /// already recorded and admitting it is a no-op.
    fn admission(&self, row: SourceRowId) -> Option<Admission> {
        let ordinal = row.ordinal();
        let container = ordinal >> 16;
        let Some((_, source)) = self.sources.iter().find(|(id, _)| *id == row.source()) else {
            let entry = if self.sources.is_empty() {
                4 * SOURCE_ENTRY_BYTES
            } else {
                2 * SOURCE_ENTRY_BYTES
            };
            return Some(Admission {
                growth: ROW_ADMISSION_BYTES + NEW_CONTAINER_BYTES + NEW_HIGH_KEY_BYTES + entry,
                merges: false,
            });
        };
        if source.rows.contains(ordinal) {
            return None;
        }
        let opens = match source.last_container {
            Some(last) if last == container => 0,
            Some(last) if last >> 16 == container >> 16 => NEW_CONTAINER_BYTES,
            _ => NEW_CONTAINER_BYTES + NEW_HIGH_KEY_BYTES,
        };
        // Neighbours only merge within one container.
        let low = ordinal & 0xFFFF;
        let merges = source.has_runs
            && low != 0
            && low != 0xFFFF
            && source.rows.contains(ordinal - 1)
            && source.rows.contains(ordinal + 1);
        let merged = if merges { MERGED_INTERVAL_BYTES } else { 0 };
        Some(Admission {
            growth: ROW_ADMISSION_BYTES + opens + merged,
            merges,
        })
    }

    /// Record `row`, whose admission was charged as `admission`.
    fn record(&mut self, row: SourceRowId, admission: &Admission) {
        let index = match self.sources.iter().position(|(id, _)| *id == row.source()) {
            Some(index) => index,
            None => {
                self.sources.push((
                    row.source(),
                    SourceEmittedRows {
                        rows: RoaringTreemap::new(),
                        last_container: None,
                        merged_slack: 0,
                        has_runs: false,
                    },
                ));
                self.sources.len() - 1
            }
        };
        let source = &mut self.sources[index].1;
        source.rows.insert(row.ordinal());
        source.last_container = Some(row.ordinal() >> 16);
        if admission.merges {
            source.merged_slack += MERGED_INTERVAL_BYTES;
        }
        self.charged = self.charged.saturating_add(admission.growth);
        self.unsettled += 1;
    }

    /// Compress every treemap and replace the charge with a bound on the
    /// compressed heap. Returns the new charge. Optimizing converts a
    /// container to runs only when that is smaller, but the new run vector
    /// may keep doubling slack, so the new charge can exceed the old one by
    /// that slack; it is reported, and the next admission's preflight sees
    /// it.
    ///
    /// Merge slack larger than the treemap's own bound is shed by rebuilding
    /// the treemap as a clone, whose vectors hold exactly their length. The
    /// rebuild briefly holds both copies, which the slack being dropped
    /// already covers.
    fn settle(&mut self) -> u64 {
        let slots = if self.sources.is_empty() {
            0
        } else {
            (2 * self.sources.len() as u64).max(4)
        };
        let mut charged = slots * SOURCE_ENTRY_BYTES;
        for (_, source) in &mut self.sources {
            source.rows.optimize();
            let (heap, has_runs) = treemap_heap_bound(&source.rows);
            source.has_runs = has_runs;
            if !has_runs {
                source.merged_slack = 0;
            } else if source.merged_slack > heap {
                source.rows = source.rows.clone();
                source.merged_slack = 0;
            }
            charged += heap + source.merged_slack;
        }
        self.charged = charged;
        self.unsettled = 0;
        charged
    }
}

/// An upper bound on `rows`' heap from its containers' statistics
/// (`bitmap/statistics.rs`), and whether it holds any run container.
fn treemap_heap_bound(rows: &RoaringTreemap) -> (u64, bool) {
    let mut bytes = 0;
    let mut has_runs = false;
    for (_, bitmap) in rows.bitmaps() {
        let stats = bitmap.statistics();
        let containers = u64::from(stats.n_containers);
        let arrays = u64::from(stats.n_array_containers);
        let runs = u64::from(stats.n_run_containers);
        let bitmaps = u64::from(stats.n_bitset_containers);
        // A run container's statistic is its serialized size, 2 bytes plus 4
        // per interval (`bitmap/store/interval_store.rs:35-41`).
        let intervals = stats.n_bytes_run_containers.saturating_sub(2 * runs) / 4;
        bytes += BTREE_NODE_BYTES
            + CONTAINER_BYTES * (2 * containers).max(4)
            // An array of `len` values holds at most max(4, 2 * len) `u16`s,
            // at most 4 * len + 4 bytes.
            + 4 * arrays
            + 4 * u64::from(stats.n_values_array_containers)
            + BITMAP_CONTAINER_BYTES * bitmaps
            // A run container's vector: at most max(4, 2 * intervals) intervals.
            + 16 * runs
            + 8 * intervals;
        has_runs |= runs > 0;
    }
    (bytes, has_runs)
}

/// Run-scoped document-DLQ state: which sources opt into the policy, the
/// run-wide failed verdict of each document with its captured root-cause
/// trigger, and the ledger of rows each failed document has already written
/// to the dead-letter output.
///
/// `Some(..)` on [`ExecutorContext::document_dlq`] iff at least one source
/// declares `dlq_granularity: document`; `None` otherwise (the dominant
/// per-record path, zero overhead). The per-document RECORD buffers live in
/// the Output arm's single invocation (a local driver), not here. Only the
/// cross-stage failure marks and the emitted-row ledgers are run-scoped,
/// because an upstream Transform / Route failure must be visible to the
/// Output at the document's close and a later Sink must see which rows an
/// earlier one wrote.
///
/// The ledgers are charged to the run's arbitrator through one consumer the
/// state registers at construction and unregisters on drop.
pub(crate) struct DocumentDlqState {
    /// Source-node names declaring `dlq_granularity: document`. A record is
    /// governed by the policy only when its originating source is in this
    /// set; records from `record`-granularity sources in the same run
    /// stream through untouched.
    doc_sources: HashSet<Arc<str>>,
    /// Documents marked failed, keyed by source file, with the root-cause
    /// trigger captured at the FIRST failure. Run-scoped so an upstream
    /// failure (Transform / Route, before any Output) is visible to every
    /// Output at the document's close, and never cleared: a document's
    /// first rejection takes its trigger but leaves the verdict, so every
    /// later Sink holding the document rejects it too.
    failed: HashMap<DocKey, FailedDocument>,
    /// Records of a failed document that ALSO failed (the 2nd, 3rd, … failure
    /// in the same document), keyed by source file. Only the first failure
    /// becomes the trigger; a later failing record never reaches an Output
    /// (it was suppressed at its Transform / Route failure site), so it is
    /// captured here and emitted as a `DocumentRejected` collateral at the
    /// document's reject — preserving the invariant that every record of a
    /// rejected N-record document contributes exactly one DLQ entry.
    extra_collaterals: HashMap<DocKey, Vec<(Record, SourceRowId)>>,
    arbitrator: Arc<MemoryArbitrator>,
    consumer_id: ConsumerId,
    /// The bytes charged for every ledger in `failed`.
    handle: Arc<ConsumerHandle>,
}

impl DocumentDlqState {
    /// Build the run-scoped state from the set of document-granularity
    /// source names, registering the one consumer its ledgers are charged
    /// through with `arbitrator`. Empty marks — the first marked failure
    /// populates them.
    pub(crate) fn new(doc_sources: HashSet<Arc<str>>, arbitrator: Arc<MemoryArbitrator>) -> Self {
        let handle = ConsumerHandle::new();
        let consumer_id =
            arbitrator.register_consumer(Arc::new(DocumentDlqConsumer::new(Arc::clone(&handle))));
        Self {
            doc_sources,
            failed: HashMap::new(),
            extra_collaterals: HashMap::new(),
            arbitrator,
            consumer_id,
            handle,
        }
    }

    /// The document key (source file) `record` is governed by under the
    /// `document` policy, or `None` when it is not governed: its originating
    /// source declares the policy, it carries a real (non-synthetic)
    /// document id, AND it carries a concrete source file (the key). A
    /// `SYNTHETIC`-id or file-less record (a no-document source, an
    /// in-pipeline synthesis, a post-Combine merged-lineage row) has no
    /// document to reject, so it falls back to per-record DLQ semantics.
    /// Returning the key — rather than a bool plus a second
    /// `source_file_arc_of` at the call site — builds the file Arc once per
    /// governed record.
    fn governing_key(&self, record: &Record) -> Option<DocKey> {
        if record.doc_ctx().id() == DocumentId::SYNTHETIC
            || !self.doc_sources.contains(&source_name_arc_of(record))
        {
            return None;
        }
        let file = source_file_arc_of(record);
        is_concrete_file(&file).then_some(file)
    }

    /// Record that `row` of failed document `key` is about to be written to
    /// the dead-letter output. Returns `Ok(false)` when an earlier rejection
    /// already wrote it, so the caller writes nothing; `Ok(true)` when the
    /// caller must write it now.
    ///
    /// The ledger's growth is preflighted against the arbitrator's hard limit
    /// before the row is recorded, as a node-buffer reservation's is: the
    /// ledger cannot spill (it is exact dedup state), so growth that would
    /// pass the limit fails the run with E310 instead, naming `node`.
    ///
    /// # Errors
    ///
    /// [`PipelineError::MemoryBudgetExceeded`] with
    /// [`clinker_plan::BudgetCategory::Arena`] when the growth would pass
    /// the hard limit; [`PipelineError::Internal`] when `key` is not a failed
    /// document.
    fn admit_emitted(
        &mut self,
        key: &DocKey,
        row: SourceRowId,
        node: &str,
    ) -> Result<bool, PipelineError> {
        let failed = self
            .failed
            .get_mut(key)
            .ok_or_else(|| PipelineError::Internal {
                op: "document dead-letter",
                node: node.to_string(),
                detail: format!("document {key:?} is rejected but was never marked failed"),
            })?;
        let Some(admission) = failed.emitted.admission(row) else {
            return Ok(false);
        };
        let growth = admission.growth;
        debug_assert!(growth <= MAX_ADMISSION_BYTES);
        let charged_pressure = self.arbitrator.sum_consumer_usage();
        let projected_pressure = charged_pressure.saturating_add(growth);
        let hard_limit = self.arbitrator.hard_limit();
        if hard_limit != 0 && projected_pressure > hard_limit {
            return Err(PipelineError::MemoryBudgetExceeded {
                node: node.to_string(),
                used: projected_pressure,
                limit: hard_limit,
                source: clinker_plan::BudgetCategory::Arena,
                detail: Some(format!(
                    "the document dead-letter ledger of {key:?} projected {projected_pressure} bytes from charged pressure {charged_pressure} plus {growth} bytes for one more row"
                )),
            });
        }
        failed.emitted.record(row, &admission);
        self.handle.add_bytes(growth);
        if failed.emitted.unsettled >= SETTLE_EVERY_ADMISSIONS {
            settle_ledger(&self.handle, &mut failed.emitted);
        }
        self.arbitrator.sample_peak_consumer_usage();
        Ok(true)
    }

    /// Settle document `key`'s ledger after a rejection pass: compress it and
    /// charge its compressed size in place of the pass's admissions.
    fn settle_emitted(&mut self, key: &DocKey) {
        if let Some(failed) = self.failed.get_mut(key) {
            settle_ledger(&self.handle, &mut failed.emitted);
        }
        self.arbitrator.sample_peak_consumer_usage();
    }

    /// Mark document `key` failed with its first failure `trigger`, charging
    /// the document's fixed map slot.
    fn insert_failed(&mut self, key: DocKey, trigger: DocTrigger) {
        self.handle.add_bytes(FAILED_DOCUMENT_BYTES);
        self.failed.insert(
            key,
            FailedDocument {
                cause: trigger.failed_at,
                trigger: Some(trigger),
                emitted: EmittedRows::new(),
            },
        );
    }

    /// Bytes charged to the arbitrator for every ledger.
    #[cfg(test)]
    fn charged_bytes(&self) -> u64 {
        self.handle.bytes()
    }
}

/// Settle `emitted` and move `handle` by the change in its charge.
fn settle_ledger(handle: &ConsumerHandle, emitted: &mut EmittedRows) {
    let before = emitted.charged;
    let after = emitted.settle();
    if after >= before {
        handle.add_bytes(after - before);
    } else {
        handle.sub_bytes(before - after);
    }
}

impl Drop for DocumentDlqState {
    fn drop(&mut self) {
        self.handle.set_bytes(0);
        self.arbitrator.unregister_consumer(self.consumer_id);
    }
}

/// The arbitrator's view of the document-DLQ ledgers.
///
/// The ledgers are exact dedup state and cannot spill, and no producer feeds
/// them that the arbitrator could pause, so `try_spill` frees nothing and
/// `can_back_pressure` is false. Their growth is refused at the hard limit
/// instead (see [`DocumentDlqState::admit_emitted`]). `spill_priority` is
/// `i32::MAX`, the value other non-reclaimable consumers use: the `Priority`
/// policy elects the lowest priority first, so it elects this consumer only
/// when every registered consumer is equally non-reclaimable, and a
/// best-effort spill sweep in priority order reaches it last.
struct DocumentDlqConsumer {
    handle: Arc<ConsumerHandle>,
}

impl DocumentDlqConsumer {
    fn new(handle: Arc<ConsumerHandle>) -> Self {
        Self { handle }
    }
}

impl MemoryConsumer for DocumentDlqConsumer {
    fn current_usage(&self) -> u64 {
        self.handle.bytes()
    }

    fn spill_priority(&self) -> i32 {
        i32::MAX
    }

    fn try_spill(&self, target_bytes: u64) -> Result<u64, ConsumerSpillError> {
        Err(ConsumerSpillError::BelowTarget {
            target: target_bytes,
            freed: 0,
        })
    }

    fn can_back_pressure(&self) -> bool {
        false
    }
}

/// A source-file Arc identifies a real document only when it names a
/// concrete file — not the empty stamp or the `<merged>` sentinel a
/// fan-in (Combine / post-aggregate) record carries.
pub(crate) fn is_concrete_file(file: &Arc<str>) -> bool {
    !file.is_empty() && file.as_ref() != MERGED_SOURCE_FILE.as_ref()
}

/// Mark a document failed and capture its root-cause trigger, returning
/// `true` iff the failure was absorbed into the document-DLQ state (the
/// caller must NOT also push a per-record DLQ entry). Returns `false` when
/// the buffer is inactive or the record is not under the `document` policy,
/// signaling the caller to take its per-record DLQ path.
///
/// The first failure for a document wins the trigger slot; later failures
/// of the same document are swallowed (the document is already doomed), so
/// the trigger always names the earliest root cause. Mirrors
/// `held_failure::hold_failure_if_grouped`, the correlation-buffer sibling.
///
/// This is the routable reject-document seam: `category` is parameterized,
/// so a non-record-eval validator (an envelope / checksum check that
/// condemns the whole document without any one record being at fault)
/// passes its own category and a representative document record here to mark
/// the document failed — it need not be a CXL-eval failure. The engine
/// reserves [`clinker_core_types::dlq::DlqErrorCategory::DocumentRejected`]
/// for the `trigger: false` collateral siblings; the trigger carries
/// whatever `category` the caller supplies.
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_error_to_document_buffer_if_doc_dlq(
    ctx: &mut ExecutorContext<'_>,
    record: &Record,
    source_row: SourceRowId,
    category: clinker_core_types::dlq::DlqErrorCategory,
    error_message: String,
    stage: Option<String>,
    route: Option<String>,
    triggering_field: Option<Arc<str>>,
    triggering_value: Option<clinker_record::Value>,
    failed_at: DlqFailureStamp,
) -> bool {
    let Some(state) = ctx.document_dlq.as_ref() else {
        return false;
    };
    let Some(key) = state.governing_key(record) else {
        return false;
    };
    let source_name = source_name_arc_of(record);
    mark_document_failed(
        ctx,
        key,
        DocTrigger {
            source_row,
            category,
            error_message,
            original_record: record.clone(),
            stage,
            route,
            source_name,
            triggering_field,
            triggering_value,
            failed_at,
        },
    );
    true
}

/// Mark the document containing a source declared-type failure. The rejected
/// row has its document context, but it has not passed the ordinary source
/// stamping path, so source and file identity must come from the
/// [`crate::executor::dlq::SourceRejectionEvent`] itself.
pub(crate) fn record_source_rejection_to_document_buffer_if_doc_dlq(
    ctx: &mut ExecutorContext<'_>,
    event: &crate::executor::dlq::SourceRejectionEvent,
    diagnostic: String,
) -> bool {
    let Some(state) = ctx.document_dlq.as_ref() else {
        return false;
    };
    if event.original_record.doc_ctx().id() == DocumentId::SYNTHETIC
        || !state.doc_sources.contains(&event.source_name)
        || !is_concrete_file(&event.source_file)
    {
        return false;
    }
    mark_document_failed(
        ctx,
        Arc::clone(&event.source_file),
        DocTrigger {
            source_row: event.source_row,
            category: event.category(),
            error_message: diagnostic,
            original_record: event.original_record.clone(),
            stage: Some(DlqEntry::stage_source()),
            route: None,
            source_name: Arc::clone(&event.source_name),
            triggering_field: Some(Arc::from(event.triggering_field.as_ref())),
            triggering_value: Some(event.triggering_value.clone()),
            failed_at: event.failed_at,
        },
    );
    true
}

/// Mark a document failed for an envelope structural-count failure if `p`
/// is a file-level close carrying a [`crate::executor::stream_event::StructuralReject`]
/// payload. A no-op for every ordinary boundary.
///
/// Called at every raw source-channel punctuation-drain site that holds
/// `ctx` — the non-fused Source arm, the fused Source→Transform arm, and the
/// fused Merge.interleave arm — so a structural-count failure condemns the
/// whole file regardless of which fusion claimed the source receiver. The
/// reject is keyed by the representative record's `$source.file` stamp, so
/// the file grain is correct independent of the carrying close's level. The
/// close still forwards downstream unchanged; #97's per-file Output buffer
/// rejects every already-streamed record of the file at that close.
pub(crate) fn mark_structural_reject_if_present(
    ctx: &mut ExecutorContext<'_>,
    p: &crate::executor::stream_event::Punctuation,
) {
    let Some(reject) = p.structural_reject() else {
        return;
    };
    record_error_to_document_buffer_if_doc_dlq(
        ctx,
        &reject.record,
        reject.row_num,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation,
        reject.message.clone(),
        Some("structural_validation".to_string()),
        None,
        None,
        None,
        reject.failed_at,
    );
}

/// Mark document `key` failed by `trigger`. The FIRST failure becomes the
/// document's root-cause trigger; a later failure of an already-failed
/// document stashes its record as an extra collateral (the trigger slot is
/// taken), so that record still contributes a `DocumentRejected` entry at
/// the document's reject rather than vanishing — every record of a rejected
/// document is accounted for as the trigger or a collateral.
fn mark_document_failed(ctx: &mut ExecutorContext<'_>, key: DocKey, trigger: DocTrigger) {
    let Some(state) = ctx.document_dlq.as_mut() else {
        return;
    };
    if state.failed.contains_key(&key) {
        state
            .extra_collaterals
            .entry(key)
            .or_default()
            .push((trigger.original_record, trigger.source_row));
    } else {
        state.insert_failed(key, trigger);
    }
}

/// One per-document record bucket inside the Output-arm driver: a spillable
/// [`NodeBuffer`] charged against the shared arbitrator through its own
/// consumer, plus the live envelope depth for the file. The bucket drops at
/// the file's outermost close (depth back to zero) or at end-of-input.
struct DocBucket {
    buffer: NodeBuffer,
    consumer_id: crate::pipeline::memory::ConsumerId,
    handle: Arc<crate::pipeline::memory::ConsumerHandle>,
    /// Envelope nesting depth for this file: incremented on each
    /// `DocumentOpen` carrying the file, decremented on each
    /// `DocumentClose`. The file's outermost close is the one that returns
    /// this to zero; a nested-level close leaves it positive.
    depth: i64,
}

/// What one rejection pass takes out of a failed document before it streams
/// the document's rows: the trigger if no earlier pass wrote it, the other
/// failing records captured at their failure sites, and the stamp every
/// collateral is condemned by.
struct RejectionParts {
    trigger: Option<DocTrigger>,
    extra_collaterals: Vec<(Record, SourceRowId)>,
    cause: DlqFailureStamp,
}

/// Claim the one decision slot for `key` in this Output invocation.
fn claim_document_decision(decided: &mut HashSet<DocKey>, key: &DocKey) -> bool {
    decided.insert(Arc::clone(key))
}

/// Apply one close punctuation to its live bucket and return the document key
/// when this is its outermost close. A close without a live bucket is still a
/// terminal decision candidate; the invocation's decided set and the run's
/// emitted-row ledger deduplicate it.
fn closing_document_key(buckets: &mut HashMap<DocKey, DocBucket>, file: &DocKey) -> Option<DocKey> {
    let outermost = match buckets.get_mut(file) {
        Some(bucket) => {
            bucket.depth -= 1;
            bucket.depth <= 0
        }
        None => true,
    };
    outermost.then(|| Arc::clone(file))
}

/// Stable end-of-input decision order for documents whose outermost close
/// never arrived.
fn remaining_document_keys(buckets: &HashMap<DocKey, DocBucket>) -> Vec<DocKey> {
    let mut remaining: Vec<DocKey> = buckets.keys().cloned().collect();
    remaining.sort_unstable();
    remaining
}

/// Per-Output-invocation driver for the `document` granularity: buffers
/// each record into its file's spillable bucket and, when the file's
/// outermost close arrives (envelope depth back to zero) or at end-of-input,
/// flushes the file clean to this Output's writer or rejects it to the DLQ.
/// The bucket drops on decision, so per-document peak memory falls — peak is
/// the concurrently-open files.
///
/// Blocking at the document grain: a buffered record is not written until
/// its file's outermost close decides the file clean. The writer is opened
/// lazily on the first clean record and reused across this arm's document
/// decisions, then flushed at the driver's end.
pub(crate) struct DocumentDlqDriver<'cfg> {
    allocation_resources: clinker_record::owned_storage::AllocationResources,
    output_name: String,
    out_cfg: &'cfg SinkConfig,
    cxl_emit_names: Option<Vec<String>>,
    /// Per-file spillable buckets, dropped at each file's outermost close.
    buckets: HashMap<DocKey, DocBucket>,
    /// Files already flushed clean / rejected this invocation, so a record
    /// or close arriving after a file is decided does not silently vanish:
    /// a late record for a decided-clean file writes through (it would have
    /// flushed); a late record for a decided-failed file dead-letters as a
    /// collateral; a duplicate close is a no-op.
    decided: HashSet<DocKey>,
    /// The Output's writer, opened on the first clean record and reused
    /// across this arm's document decisions. `None` until then.
    writer: Option<clinker_format::FormatWriterHandle>,
    arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>,
    spill_root: Arc<std::path::Path>,
    spill_compress: clinker_plan::config::CompressMode,
    batch_size: usize,
    ok_count: u64,
    records_written: u64,
    structured_guard: StructuredOutputDocumentGuard,
    writer_boundary: OrderedWriterBoundary,
}

impl<'cfg> DocumentDlqDriver<'cfg> {
    /// Build the driver for one Output arm invocation. `cxl_emit_names`
    /// drives `include_unmapped: false` projection; `None`/empty keeps the
    /// upstream passthrough.
    pub(crate) fn new(
        ctx: &ExecutorContext<'_>,
        output_name: &str,
        out_cfg: &'cfg SinkConfig,
        cxl_emit_names: Vec<String>,
        writer_boundary: OrderedWriterBoundary,
    ) -> Self {
        let cxl_emit_names = if cxl_emit_names.is_empty() {
            None
        } else {
            Some(cxl_emit_names)
        };
        Self {
            allocation_resources: ctx.allocation_resources.clone(),
            output_name: output_name.to_string(),
            out_cfg,
            cxl_emit_names,
            buckets: HashMap::new(),
            decided: HashSet::new(),
            writer: None,
            arbitrator: Arc::clone(&ctx.memory_budget),
            spill_root: Arc::clone(&ctx.spill_root_path),
            spill_compress: ctx.spill_compress,
            batch_size: ctx.batch_size,
            ok_count: 0,
            records_written: 0,
            structured_guard: StructuredOutputDocumentGuard::new(&out_cfg.format),
            writer_boundary,
        }
    }

    /// Borrow (building on first sight) the bucket for file `key`. The
    /// arbitrator is passed in so the caller's other `&self` fields stay
    /// free of the `&mut self.buckets` borrow this returns. A new bucket's
    /// consumer is registered under `output_name`, the name its spill is
    /// recorded under.
    fn bucket_for<'a>(
        buckets: &'a mut HashMap<DocKey, DocBucket>,
        arbitrator: &crate::pipeline::memory::MemoryArbitrator,
        output_name: &str,
        key: &DocKey,
    ) -> &'a mut DocBucket {
        buckets.entry(Arc::clone(key)).or_insert_with(|| {
            let handle = crate::pipeline::memory::ConsumerHandle::new();
            let consumer_id = arbitrator.register_node_consumer(
                output_name,
                Arc::new(crate::executor::node_buffer::NodeBufferConsumer::new(
                    handle.clone(),
                )),
            );
            DocBucket {
                buffer: NodeBuffer::Memory(Vec::new()),
                consumer_id,
                handle,
                depth: 0,
            }
        })
    }

    /// Buffer one record into its file's bucket, charging and spilling the
    /// bucket under budget pressure.
    ///
    /// # Errors
    ///
    /// Surfaces a spill-cap-exceeded [`PipelineError`] when admitting the
    /// bucket would push cumulative spill past the configured ceiling.
    fn buffer_record(
        &mut self,
        key: &DocKey,
        record: Record,
        source_row: SourceRowId,
    ) -> Result<(), PipelineError> {
        let column_count = record.schema().column_count();
        let bucket = Self::bucket_for(&mut self.buckets, &self.arbitrator, &self.output_name, key);
        bucket.buffer.push(record, source_row);
        bucket.handle.set_bytes(
            bucket
                .buffer
                .unaccounted_memory_bytes(&self.allocation_resources),
        );
        self.arbitrator.sample_peak_consumer_usage();
        if self.arbitrator.should_spill() {
            spill_bucket_in_place(
                bucket,
                &self.arbitrator,
                &self.output_name,
                self.spill_root.as_ref(),
                self.spill_compress,
                self.batch_size,
                column_count,
            )?;
        }
        Ok(())
    }

    /// Decide file `key`: flush it clean to the writer or, if its run-scoped
    /// failed mark is set, reject it. Drops the bucket and marks the file
    /// decided. Idempotent — a second decision for the same file is a no-op.
    ///
    /// # Errors
    ///
    /// Surfaces writer-build / write / flush failures, spill-drain decode
    /// errors, and any DLQ-rate error from a reject as a [`PipelineError`].
    fn decide_document(
        &mut self,
        ctx: &mut ExecutorContext<'_>,
        key: &DocKey,
    ) -> Result<(), PipelineError> {
        if !claim_document_decision(&mut self.decided, key) {
            return Ok(());
        }
        let bucket = self.buckets.remove(key);
        let is_failed = ctx
            .document_dlq
            .as_ref()
            .is_some_and(|s| s.failed.contains_key(key));
        if is_failed {
            reject_document_now(ctx, key, bucket, &self.output_name)
        } else {
            self.flush_clean(ctx, bucket)
        }
    }

    /// Write a clean document's buffered (possibly spilled) records through
    /// this Output's writer, opening it on first use. Unregisters the
    /// bucket's arbitrator consumer.
    fn flush_clean(
        &mut self,
        ctx: &mut ExecutorContext<'_>,
        bucket: Option<DocBucket>,
    ) -> Result<(), PipelineError> {
        let Some(bucket) = bucket else {
            return Ok(());
        };
        let DocBucket {
            buffer,
            consumer_id,
            handle,
            ..
        } = bucket;
        handle.set_bytes(0);
        let result = (|| {
            // Drain in ARRIVAL order. The success sink is order-sensitive, and
            // a bucket that spilled and then kept a resident mem tail has its
            // newest records in the mem tail. Feed the restored arrival stream
            // into the compiled boundary; a deferred boundary drains its
            // bounded spill merge lazily into the writer.
            let ordered = self
                .writer_boundary
                .order_record_stream(ctx, drain_records_in_arrival_order(buffer))?;
            for item in ordered {
                let (record, source_row) = item?;
                if let Err(err) = self
                    .structured_guard
                    .observe(&self.output_name, record.doc_ctx())
                {
                    ctx.output_errors.push(err);
                    return Ok(());
                }
                let projected = match self.out_cfg.mapping.as_ref() {
                    Some(_) => {
                        let probe =
                            mapping_probe(&mut ctx.mapping_probes, &self.output_name, self.out_cfg);
                        crate::projection::project_output_probed(
                            &record,
                            self.out_cfg,
                            self.cxl_emit_names.as_deref(),
                            Some(probe),
                        )
                    }
                    None => crate::projection::project_output_from_record(
                        &record,
                        self.out_cfg,
                        self.cxl_emit_names.as_deref(),
                    ),
                };
                let prior_errors = ctx.output_errors.len();
                self.write_projected(ctx, std::slice::from_ref(&projected));
                if ctx.output_errors.len() != prior_errors {
                    break;
                }
                if ctx.ok_source_rows.insert(source_row) {
                    self.ok_count += 1;
                }
                self.records_written += 1;
            }
            Ok(())
        })();
        self.arbitrator.unregister_consumer(consumer_id);
        result
    }

    /// Write a non-governed record straight through to this Output's writer,
    /// counting it toward `ok_count` / `records_written` exactly as the
    /// per-record Output path does. Used for records a `record`-policy
    /// source (or in-pipeline synthesis) routes to a document-DLQ Output,
    /// and for a late record arriving after its file already flushed clean.
    fn write_through(
        &mut self,
        ctx: &mut ExecutorContext<'_>,
        record: Record,
        source_row: SourceRowId,
    ) -> Result<(), PipelineError> {
        let mut ordered = self
            .writer_boundary
            .order_record_stream(ctx, std::iter::once(Ok((record, source_row))))?;
        let Some(item) = ordered.next() else {
            return Ok(());
        };
        let (record, source_row) = item?;
        if let Err(err) = self
            .structured_guard
            .observe(&self.output_name, record.doc_ctx())
        {
            ctx.output_errors.push(err);
            return Ok(());
        }
        let projected = match self.out_cfg.mapping.as_ref() {
            Some(_) => {
                let probe = mapping_probe(&mut ctx.mapping_probes, &self.output_name, self.out_cfg);
                crate::projection::project_output_probed(
                    &record,
                    self.out_cfg,
                    self.cxl_emit_names.as_deref(),
                    Some(probe),
                )
            }
            None => crate::projection::project_output_from_record(
                &record,
                self.out_cfg,
                self.cxl_emit_names.as_deref(),
            ),
        };
        if ctx.ok_source_rows.insert(source_row) {
            self.ok_count += 1;
        }
        self.records_written += 1;
        self.write_projected(ctx, std::slice::from_ref(&projected));
        Ok(())
    }

    /// Write a batch of already-projected records through this Output's
    /// writer, opening it lazily on first use. Errors land in the context's
    /// error sink rather than short-circuiting, matching the per-record
    /// Output path.
    fn write_projected(&mut self, ctx: &mut ExecutorContext<'_>, projected: &[Record]) {
        if projected.is_empty() {
            return;
        }
        if self.writer.is_none() {
            let Some(raw_writer) = ctx.writers.remove(&self.output_name) else {
                // No writer registered for this Output (a dry-run or a
                // sibling already took it): nothing to write to.
                return;
            };
            let output_schema = projected[0].schema().clone();
            match build_format_writer(
                self.out_cfg,
                raw_writer,
                output_schema,
                ctx.output_staging.clone(),
                ctx.sink_byte_counter.clone(),
                ctx.writer_resources.clone(),
                &ctx.truncation_ledger,
            ) {
                Ok(w) => self.writer = Some(w),
                Err(e) => {
                    ctx.output_errors.push(e);
                    return;
                }
            }
        }
        let writer = self.writer.as_mut().expect("writer opened above");
        for record in projected {
            let write_result = {
                let _guard = ctx.write_timer.guard();
                writer.write_record(record)
            };
            if let Err(e) = write_result {
                // A `join_values` `on_conflict: error` collision routes to the
                // DLQ on the record-granularity Output arms (buffered +
                // streaming). This document-DLQ arm writes projected records and
                // does not hold each original record here, and a collision
                // interacts with the document's own accept/reject verdict, so
                // that combination keeps the existing disposition and is tracked
                // as a follow-up rather than approximated here.
                ctx.output_errors.push(e.into());
                break;
            }
        }
    }

    /// Handle one governed record: buffer it into its open file's bucket, or
    /// — when its file was already decided (a record arriving after its
    /// file's close, e.g. an upstream interleave) — route it to match the
    /// file's verdict so it never silently vanishes. A late record for a
    /// clean file writes through; a late record for a failed file
    /// dead-letters as a `DocumentRejected` collateral.
    ///
    /// # Errors
    ///
    /// Surfaces buffering spill errors and DLQ-rate errors as a [`PipelineError`].
    fn admit_governed(
        &mut self,
        ctx: &mut ExecutorContext<'_>,
        key: DocKey,
        record: Record,
        source_row: SourceRowId,
    ) -> Result<(), PipelineError> {
        if self.decided.contains(&key) {
            let cause = ctx
                .document_dlq
                .as_ref()
                .and_then(|s| s.failed.get(&key))
                .map(|failed| failed.cause);
            match cause {
                Some(cause) => {
                    if document_state(ctx, &self.output_name)?.admit_emitted(
                        &key,
                        source_row,
                        &self.output_name,
                    )? {
                        push_document_collateral(ctx, &key, record, source_row, &cause)?;
                    }
                }
                None => self.write_through(ctx, record, source_row)?,
            }
            return Ok(());
        }
        self.buffer_record(&key, record, source_row)
    }

    /// Drive the Output arm's full drained event stream: buffer each record
    /// into its file's bucket and, on each file's outermost close (envelope
    /// depth back to zero), flush-or-reject it; then decide any file whose
    /// outermost close never arrived (malformed / unterminated input) on the
    /// same clean-vs-failed axis. Finishes by flushing the writer and
    /// folding the run counters back into `ctx`.
    ///
    /// # Errors
    ///
    /// Surfaces buffering, flushing, and reject errors as a [`PipelineError`].
    pub(crate) fn run(
        mut self,
        ctx: &mut ExecutorContext<'_>,
        events: impl IntoIterator<Item = StreamEvent>,
    ) -> Result<(), PipelineError> {
        use crate::executor::stream_event::PunctuationKind;
        for event in events {
            match event {
                StreamEvent::Record(record, source_row) => {
                    // A governed record buffers under its file's policy; a
                    // non-governed one (synthetic id, file-less, or a
                    // `record`-policy source feeding the same Output) writes
                    // straight through, exactly as a per-record Output does.
                    let key = ctx
                        .document_dlq
                        .as_ref()
                        .and_then(|s| s.governing_key(&record));
                    match key {
                        Some(key) => self.admit_governed(ctx, key, record, source_row)?,
                        None => self.write_through(ctx, record, source_row)?,
                    }
                }
                StreamEvent::Punctuation(p) => {
                    let file = Arc::clone(p.source_file());
                    if !is_concrete_file(&file) {
                        continue;
                    }
                    match p.kind() {
                        PunctuationKind::DocumentOpen => {
                            // Track envelope nesting per file so only the
                            // OUTERMOST close (depth back to zero) decides
                            // the file. A bucket may not exist yet for a
                            // header-only file that has emitted no record;
                            // create it so the open/close balance is counted.
                            Self::bucket_for(
                                &mut self.buckets,
                                &self.arbitrator,
                                &self.output_name,
                                &file,
                            )
                            .depth += 1;
                        }
                        PunctuationKind::DocumentClose => {
                            if let Some(key) = closing_document_key(&mut self.buckets, &file) {
                                self.decide_document(ctx, &key)?;
                            }
                        }
                    }
                }
            }
        }
        // End-of-input drain: any file whose outermost close never arrived
        // (a malformed / unterminated document) decides on the same
        // clean-vs-failed axis as one closed in stream — a failed one
        // rejects with its collaterals, a clean one flushes. Deterministic
        // order keeps emit / write ordering stable across runs.
        for key in remaining_document_keys(&self.buckets) {
            self.decide_document(ctx, &key)?;
        }

        if let Some(mut writer) = self.writer.take() {
            let flush_result = {
                let _guard = ctx.write_timer.guard();
                writer.flush()
            };
            if let Err(e) = flush_result {
                ctx.output_errors.push(e.into());
            }
        }
        ctx.counters.ok_count += self.ok_count;
        ctx.counters.records_written += self.records_written;
        ctx.records_emitted += self.records_written;
        Ok(())
    }
}

impl Drop for DocumentDlqDriver<'_> {
    fn drop(&mut self) {
        // A `?`-early-return out of `run` leaves buckets live; unregister
        // every surviving consumer so an error exit cannot strand a charge
        // in the arbitrator's registry. Mirrors `RegisteredTables`' guard.
        for (_, bucket) in self.buckets.drain() {
            self.arbitrator.unregister_consumer(bucket.consumer_id);
        }
    }
}

/// End-of-DAG sweep: reject every failed document the run never emitted —
/// a document marked failed upstream whose close never arrived AND whose
/// records were all suppressed before any Output (so no Output-arm bucket
/// carried them). Emits the captured root-cause trigger with no collaterals,
/// once each. Runs once after every Output arm; a document whose records DID
/// reach an Output was rejected there, which took its trigger, so it is
/// skipped. A no-op when the document-DLQ buffer is inactive.
///
/// # Errors
///
/// Surfaces any DLQ-rate error a trigger push trips as a [`PipelineError`].
pub(crate) fn reject_unclosed_failed_documents(
    ctx: &mut ExecutorContext<'_>,
) -> Result<(), PipelineError> {
    let Some(state) = ctx.document_dlq.as_ref() else {
        return Ok(());
    };
    let mut pending: Vec<DocKey> = state
        .failed
        .iter()
        .filter(|(_, failed)| failed.trigger.is_some())
        .map(|(key, _)| Arc::clone(key))
        .collect();
    pending.sort_unstable();
    for key in pending {
        reject_document_now(ctx, &key, None, "document_dlq")?;
    }
    Ok(())
}

/// Drain a clean document's bucket records in ARRIVAL order — spill chunks
/// (older) first, then the resident in-memory tail (newer).
///
/// [`NodeBuffer::drain`] yields the in-memory events BEFORE the spill chunks.
/// For a `Mixed` bucket that is arrival-INVERTED: `NodeBuffer::Mixed` is only
/// ever produced by `push_event` after a spill, so its mem tail holds the
/// document's NEWEST records, which `drain` would emit ahead of the older
/// spilled body. (There is no `Mixed` whose mem is a pre-spill head — an
/// inter-stage slot never reaches `Mixed` at all.) The success sink is
/// order-sensitive, so this splits the bucket and re-orders to arrival
/// sequence: spill chunks (older) first, then the resident tail (newer).
/// Spill rows stream from disk lazily; a decode failure surfaces as a
/// `PipelineError` item.
fn drain_records_in_arrival_order(
    buffer: NodeBuffer,
) -> impl Iterator<Item = Result<(Record, SourceRowId), PipelineError>> {
    let SpilledRemainder {
        mem_records,
        chunks,
        ..
    } = peel_mem_tail(buffer);
    // The chunks alone re-form a `Spilled` buffer whose drain yields each
    // chunk's records in chunk (= arrival) order; the resident tail follows.
    let chunk_records = NodeBuffer::Spilled {
        chunks,
        pending_puncts: Vec::new(),
    }
    .drain()
    .filter_map(|event| match event {
        Ok(StreamEvent::Record(r, rn)) => Some(Ok((r, rn))),
        Ok(StreamEvent::Punctuation(_)) => None,
        Err(e) => Some(Err(e)),
    });
    chunk_records.chain(mem_records.into_iter().map(Ok))
}

/// A [`NodeBuffer`] split into its in-memory records, its already-on-disk
/// spill chunks, and its trailing punctuations — the shape
/// [`spill_bucket_in_place`] needs to append a new chunk without reading any
/// existing chunk back from disk.
struct SpilledRemainder {
    mem_records: Vec<(Record, SourceRowId)>,
    chunks: Vec<(crate::pipeline::spill::SpillFile<SourceRowId>, u64)>,
    puncts: Vec<crate::executor::stream_event::Punctuation>,
}

/// Split `buffer` into its in-memory records, its on-disk spill chunks, and
/// its trailing punctuations. Records and puncts in the mem tail separate
/// (puncts never spill); spill chunks pass through untouched.
fn peel_mem_tail(buffer: NodeBuffer) -> SpilledRemainder {
    let (mem_events, chunks, mut puncts) = match buffer {
        NodeBuffer::Memory(events) => (events, Vec::new(), Vec::new()),
        NodeBuffer::Spilled {
            chunks,
            pending_puncts,
        } => (Vec::new(), chunks, pending_puncts),
        NodeBuffer::Mixed {
            mem,
            spills,
            pending_puncts,
        } => (mem, spills, pending_puncts),
        NodeBuffer::MergeSpilled { .. } | NodeBuffer::ReReadable(_) => {
            unreachable!("document DLQ buckets are only ever mutable Memory/Spilled/Mixed buffers")
        }
    };
    let mut mem_records: Vec<(Record, SourceRowId)> = Vec::with_capacity(mem_events.len());
    for event in mem_events {
        match event {
            StreamEvent::Record(r, rn) => mem_records.push((r, rn)),
            StreamEvent::Punctuation(p) => puncts.push(p),
        }
    }
    SpilledRemainder {
        mem_records,
        chunks,
        puncts,
    }
}

/// Flush only a bucket's in-memory tail to a NEW spill chunk, APPENDING it
/// to any chunks the bucket already holds, and charge the file against the
/// arbitrator's spill quota.
///
/// Peeling off only the mem tail (rather than draining the whole buffer and
/// re-spilling every prior chunk) keeps repeated spills under sustained
/// memory pressure O(total records), not O(records²): existing on-disk
/// chunks stay on disk untouched. Punctuations never spill — they ride in
/// `pending_puncts` and drain at the document tail.
fn spill_bucket_in_place(
    bucket: &mut DocBucket,
    arbitrator: &crate::pipeline::memory::MemoryArbitrator,
    output_name: &str,
    spill_root: &std::path::Path,
    spill_compress: clinker_plan::config::CompressMode,
    batch_size: usize,
    column_count: usize,
) -> Result<(), PipelineError> {
    // Peel only the in-memory portion off the bucket, leaving any existing
    // spill chunks on disk untouched (no read-back). The mem-tail records
    // become a new chunk appended after them.
    let SpilledRemainder {
        mem_records,
        mut chunks,
        puncts,
    } = peel_mem_tail(std::mem::replace(
        &mut bucket.buffer,
        NodeBuffer::Memory(Vec::new()),
    ));
    if mem_records.is_empty() {
        // Nothing new to flush — restore the buffer unchanged.
        bucket.buffer = NodeBuffer::Spilled {
            chunks,
            pending_puncts: puncts,
        };
        return Ok(());
    }

    let compress = spill_compress.resolve_for_schema(column_count, batch_size as u64);
    if let Some((file, count)) = crate::executor::node_buffer_spill::spill_node_buffer(
        mem_records,
        Some(spill_root),
        compress,
    )? {
        let file_bytes = std::fs::metadata(file.path()).map(|m| m.len()).unwrap_or(0);
        if arbitrator.record_spill_bytes(output_name, file_bytes) {
            return Err(PipelineError::spill_cap_exceeded(
                output_name,
                arbitrator.max_spill_bytes(),
                file_bytes,
                arbitrator.cumulative_spill_bytes(),
            ));
        }
        chunks.push((file, count));
    }
    // The mem tail is now on disk; the bucket's live in-memory bytes are
    // zero (only spill chunks remain).
    bucket.handle.set_bytes(0);
    bucket.buffer = NodeBuffer::Spilled {
        chunks,
        pending_puncts: puncts,
    };
    Ok(())
}

/// Emit one `DocumentRejected` collateral for a record of failed document
/// `key` that the emitted-row ledger admitted. Counts toward the DLQ rate.
/// `cause` is the document trigger's stamp: the record is condemned now, by
/// that failure, and carries its failure id.
fn push_document_collateral(
    ctx: &mut ExecutorContext<'_>,
    key: &DocKey,
    record: Record,
    source_row: SourceRowId,
    cause: &DlqFailureStamp,
) -> Result<(), PipelineError> {
    let source_name = source_name_arc_of(&record);
    push_dlq(
        ctx,
        DlqEntry {
            source_row,
            category: clinker_core_types::dlq::DlqErrorCategory::DocumentRejected,
            error_message: format!("document {key:?} rejected: a sibling record failed"),
            original_record: record,
            stage: Some("document_dlq".to_string()),
            route: None,
            trigger: false,
            source_name,
            triggering_field: None,
            triggering_value: None,
            failed_at: DlqFailureStamp::condemned_by(cause),
        },
    )
}

/// The run's document-DLQ state, which every caller here has already
/// established is active.
fn document_state<'a>(
    ctx: &'a mut ExecutorContext<'_>,
    node: &str,
) -> Result<&'a mut DocumentDlqState, PipelineError> {
    ctx.document_dlq
        .as_mut()
        .ok_or_else(|| PipelineError::Internal {
            op: "document dead-letter",
            node: node.to_string(),
            detail: "a document rejection ran without document dead-letter state".to_string(),
        })
}

/// Take what one rejection pass over failed document `key` writes before its
/// rows: the trigger (only the document's first pass finds it), the extra
/// failing records, and the stamp that condemns every collateral. The
/// verdict itself stays for the run.
///
/// # Errors
///
/// [`PipelineError::Internal`] when `key` is not a failed document.
fn take_rejection_parts(
    state: &mut DocumentDlqState,
    key: &DocKey,
    node: &str,
) -> Result<RejectionParts, PipelineError> {
    let failed = state
        .failed
        .get_mut(key)
        .ok_or_else(|| PipelineError::Internal {
            op: "document dead-letter",
            node: node.to_string(),
            detail: format!("document {key:?} is rejected but was never marked failed"),
        })?;
    let trigger = failed.trigger.take();
    let cause = failed.cause;
    let extra_collaterals = state.extra_collaterals.remove(key).unwrap_or_default();
    Ok(RejectionParts {
        trigger,
        extra_collaterals,
        cause,
    })
}

/// Reject failed document `key` at `node`: write each of its rows that no
/// earlier rejection wrote, then release `bucket`'s arbitrator consumer on
/// every exit.
///
/// # Errors
///
/// Surfaces spill-drain decode errors, E310 from the emitted-row ledger and
/// DLQ-rate errors as a [`PipelineError`].
fn reject_document_now(
    ctx: &mut ExecutorContext<'_>,
    key: &DocKey,
    bucket: Option<DocBucket>,
    node: &str,
) -> Result<(), PipelineError> {
    let arbitrator = Arc::clone(&ctx.memory_budget);
    let (buffer, consumer_id) = match bucket {
        Some(DocBucket {
            buffer,
            consumer_id,
            handle,
            ..
        }) => {
            handle.set_bytes(0);
            (Some(buffer), Some(consumer_id))
        }
        None => (None, None),
    };
    let result = stream_document_rejection(ctx, key, buffer, node);
    if let Some(consumer_id) = consumer_id {
        arbitrator.unregister_consumer(consumer_id);
    }
    result
}

/// One streaming rejection pass over failed document `key`, in this order:
/// the root-cause trigger (`trigger: true`, its own category) if no earlier
/// pass wrote it, then a `DocumentRejected` collateral (`trigger: false`) for
/// each extra failing record and each record of `buffer`. Every row is
/// admitted to the document's emitted-row ledger before it is written, and
/// only a row no earlier rejection wrote is written, so each row of the
/// document reaches the dead-letter output once across every Sink. Records
/// are drained one at a time; nothing is collected first. The ledger settles
/// on every exit.
fn stream_document_rejection(
    ctx: &mut ExecutorContext<'_>,
    key: &DocKey,
    buffer: Option<NodeBuffer>,
    node: &str,
) -> Result<(), PipelineError> {
    let RejectionParts {
        trigger,
        extra_collaterals,
        cause,
    } = take_rejection_parts(document_state(ctx, node)?, key, node)?;
    let result = (|| {
        if let Some(t) = trigger
            && document_state(ctx, node)?.admit_emitted(key, t.source_row, node)?
        {
            push_dlq(
                ctx,
                DlqEntry {
                    source_row: t.source_row,
                    category: t.category,
                    error_message: t.error_message,
                    original_record: t.original_record,
                    stage: t.stage,
                    route: t.route,
                    trigger: true,
                    source_name: t.source_name,
                    triggering_field: t.triggering_field,
                    triggering_value: t.triggering_value,
                    failed_at: t.failed_at,
                },
            )?;
        }
        // A non-first failing record was suppressed at its failure site, so
        // it never reached a Sink's bucket; it is written here.
        for (record, source_row) in extra_collaterals {
            if document_state(ctx, node)?.admit_emitted(key, source_row, node)? {
                push_document_collateral(ctx, key, record, source_row, &cause)?;
            }
        }
        if let Some(buffer) = buffer {
            for event in buffer.drain() {
                match event? {
                    StreamEvent::Record(record, source_row) => {
                        if document_state(ctx, node)?.admit_emitted(key, source_row, node)? {
                            push_document_collateral(ctx, key, record, source_row, &cause)?;
                        }
                    }
                    StreamEvent::Punctuation(_) => {}
                }
            }
        }
        Ok(())
    })();
    if let Some(state) = ctx.document_dlq.as_mut() {
        state.settle_emitted(key);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::owned_storage::SharedStorage;
    use clinker_record::{
        DocumentContext, DocumentId, EnvelopeRecord, FieldMetadata, Schema, SchemaBuilder, Value,
    };

    fn schema() -> SharedStorage<Schema> {
        SharedStorage::from_arc(Arc::new(Schema::new(vec!["id".into(), "value".into()])))
    }

    fn rec(s: &SharedStorage<Schema>, id: i64, value: i64) -> Record {
        Record::new(s.clone(), vec![Value::Integer(id), Value::Integer(value)])
    }

    fn rejected_document_fixture() -> (
        Arc<MemoryArbitrator>,
        DocumentDlqState,
        HashMap<DocKey, DocBucket>,
        SharedStorage<DocumentContext>,
    ) {
        let key: DocKey = Arc::from("broken.x12");
        let source_name: Arc<str> = Arc::from("orders");
        let doc = SharedStorage::from_arc(Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::clone(&key),
            EnvelopeRecord::empty(),
        )));
        let schema = SchemaBuilder::with_capacity(4)
            .with_field("id")
            .with_field("value")
            .with_field_meta("$source.file", FieldMetadata::SourceFile)
            .with_field_meta("$source.name", FieldMetadata::SourceName)
            .build();
        let record = |id: i64| {
            let mut record = Record::new(
                schema.clone(),
                vec![
                    Value::Integer(id),
                    Value::Integer(id * 10),
                    Value::from(key.as_ref()),
                    Value::from(source_name.as_ref()),
                ],
            );
            record.set_doc_ctx(doc.clone());
            record
        };
        let trigger_record = record(1);
        let collateral_record = record(2);
        let trigger_row = SourceRowId::from(1);
        let collateral_row = SourceRowId::from(2);

        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            1024 * 1024,
            0.8,
            0.6,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        let mut state = DocumentDlqState::new(
            HashSet::from([Arc::clone(&source_name)]),
            Arc::clone(&arbitrator),
        );
        state.insert_failed(
            Arc::clone(&key),
            DocTrigger {
                source_row: trigger_row,
                category: clinker_core_types::dlq::DlqErrorCategory::ValidationFailure,
                error_message: "document validation failed".to_string(),
                original_record: trigger_record.clone(),
                stage: Some("validate".to_string()),
                route: None,
                source_name,
                triggering_field: None,
                triggering_value: None,
                failed_at: DlqFailureStamp::now(),
            },
        );
        assert_eq!(
            state.charged_bytes(),
            FAILED_DOCUMENT_BYTES,
            "marking a document failed charges its map slot"
        );

        let handle = ConsumerHandle::new();
        let consumer_id = arbitrator.register_consumer(Arc::new(
            crate::executor::node_buffer::NodeBufferConsumer::new(handle.clone()),
        ));
        let mut buffer = NodeBuffer::Memory(Vec::new());
        buffer.push(trigger_record, trigger_row);
        buffer.push(collateral_record, collateral_row);
        let bucket = DocBucket {
            buffer,
            consumer_id,
            handle,
            depth: 1,
        };
        (arbitrator, state, HashMap::from([(key, bucket)]), doc)
    }

    /// The rows one rejection pass over `key` writes, decided at the same
    /// seams the executor pass uses but without an executor context: whether
    /// the trigger is written, then each collateral row the ledger admits.
    /// Releases `bucket`'s consumer and settles the ledger, as the pass does.
    fn rejection_pass_rows(
        state: &mut DocumentDlqState,
        arbitrator: &MemoryArbitrator,
        key: &DocKey,
        bucket: Option<DocBucket>,
    ) -> (bool, Vec<SourceRowId>) {
        let RejectionParts {
            trigger,
            extra_collaterals,
            ..
        } = take_rejection_parts(state, key, "out").expect("a failed document");
        let trigger_written = trigger.is_some_and(|t| {
            state
                .admit_emitted(key, t.source_row, "out")
                .expect("trigger admission")
        });
        let mut collaterals = Vec::new();
        for (_, row) in extra_collaterals {
            if state.admit_emitted(key, row, "out").expect("admission") {
                collaterals.push(row);
            }
        }
        if let Some(bucket) = bucket {
            bucket.handle.set_bytes(0);
            for event in bucket.buffer.drain() {
                if let StreamEvent::Record(_, row) = event.expect("drain")
                    && state.admit_emitted(key, row, "out").expect("admission")
                {
                    collaterals.push(row);
                }
            }
            arbitrator.unregister_consumer(bucket.consumer_id);
        }
        state.settle_emitted(key);
        (trigger_written, collaterals)
    }

    #[test]
    fn duplicate_document_close_claims_one_rejection() {
        let (arbitrator, mut state, mut buckets, doc) = rejected_document_fixture();
        let key = Arc::clone(doc.source_file());
        let closes = [
            crate::executor::stream_event::Punctuation::document_close(doc.clone()),
            crate::executor::stream_event::Punctuation::document_close(doc.clone()),
        ];
        let mut decided = HashSet::new();
        let mut rejections = Vec::new();

        for close in closes {
            if let Some(decision_key) = closing_document_key(&mut buckets, close.source_file())
                && claim_document_decision(&mut decided, &decision_key)
            {
                let bucket = buckets.remove(&decision_key);
                rejections.push(rejection_pass_rows(
                    &mut state,
                    &arbitrator,
                    &decision_key,
                    bucket,
                ));
            }
        }

        assert!(
            state
                .failed
                .get(&key)
                .is_some_and(|failed| failed.trigger.is_none()),
            "the rejection takes the trigger and keeps the run-wide verdict"
        );
        assert_eq!(decided, HashSet::from([key.clone()]));
        assert_eq!(rejections.len(), 1, "a duplicate close cannot reject twice");
        assert!(rejections[0].0, "one root-cause entry");
        assert_eq!(
            rejections[0].1,
            [SourceRowId::from(2)],
            "the sibling record is emitted once; the trigger's own row is not a collateral"
        );
        assert_eq!(
            arbitrator.consumer_count(),
            1,
            "the bucket's consumer is released; only the state's remains"
        );

        // A second rejection of the same document, as a later Sink holding
        // the same rows runs, writes nothing more.
        assert_eq!(
            rejection_pass_rows(&mut state, &arbitrator, &key, None),
            (false, Vec::new())
        );
    }

    #[test]
    fn unterminated_dirty_document_is_rejected_at_drain() {
        let (arbitrator, mut state, mut buckets, _doc) = rejected_document_fixture();
        let mut decided = HashSet::new();
        let mut rejections = Vec::new();

        for key in remaining_document_keys(&buckets) {
            if claim_document_decision(&mut decided, &key) {
                let bucket = buckets.remove(&key);
                rejections.push(rejection_pass_rows(&mut state, &arbitrator, &key, bucket));
            }
        }

        assert_eq!(rejections.len(), 1, "drain decides the open document");
        assert!(rejections[0].0, "one root-cause entry");
        assert_eq!(
            rejections[0].1.len(),
            1,
            "the buffered sibling becomes one collateral"
        );
        assert!(buckets.is_empty(), "the drained bucket is released");
        assert_eq!(
            arbitrator.consumer_count(),
            1,
            "the bucket's consumer is released; only the state's remains"
        );
    }

    /// Row `ordinal` of Source node `source`.
    fn row(source: usize, ordinal: u64) -> SourceRowId {
        SourceRowId::new(
            <PlanNodeId as clinker_plan::plan::EntityRef>::new(source),
            ordinal,
        )
    }

    /// An arbitrator with hard limit `limit` that never elects a victim.
    fn ledger_arbitrator(limit: u64) -> Arc<MemoryArbitrator> {
        Arc::new(MemoryArbitrator::with_policy(
            limit,
            0.8,
            0.6,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ))
    }

    /// A document state charged to `arbitrator` holding one failed document.
    fn ledger_state(arbitrator: &Arc<MemoryArbitrator>) -> (DocumentDlqState, DocKey) {
        let key: DocKey = Arc::from("orders.csv");
        let mut state =
            DocumentDlqState::new(HashSet::from([Arc::from("orders")]), Arc::clone(arbitrator));
        state.failed.insert(
            Arc::clone(&key),
            FailedDocument {
                cause: DlqFailureStamp::now(),
                trigger: None,
                emitted: EmittedRows::new(),
            },
        );
        (state, key)
    }

    /// The ordinals document `key` has recorded for Source node `source`.
    fn recorded(state: &DocumentDlqState, key: &DocKey, source: usize) -> Vec<u64> {
        let source = <PlanNodeId as clinker_plan::plan::EntityRef>::new(source);
        state.failed[key]
            .emitted
            .sources
            .iter()
            .find(|(id, _)| *id == source)
            .map(|(_, rows)| rows.rows.iter().collect())
            .unwrap_or_default()
    }

    #[test]
    fn ledger_admission_is_idempotent_in_any_order() {
        let arbitrator = ledger_arbitrator(1 << 30);
        let orders: [Vec<u64>; 3] = [
            (1..=300).collect(),
            (1..=300).rev().collect(),
            vec![70_000, 5, 1 << 33, 6, 65_535, 65_536, 4, 1 << 40],
        ];
        for order in orders {
            let (mut state, key) = ledger_state(&arbitrator);
            for &ordinal in &order {
                assert!(
                    state
                        .admit_emitted(&key, row(1, ordinal), "out")
                        .expect("admission"),
                    "the first admission of {ordinal} is new"
                );
            }
            state.settle_emitted(&key);
            for &ordinal in &order {
                assert!(
                    !state
                        .admit_emitted(&key, row(1, ordinal), "out")
                        .expect("admission"),
                    "a repeated admission of {ordinal} is a no-op"
                );
            }
            let mut admitted = order.clone();
            admitted.sort_unstable();
            assert_eq!(recorded(&state, &key, 1), admitted);
        }
    }

    #[test]
    fn a_later_sink_admits_only_rows_no_earlier_rejection_wrote() {
        let arbitrator = ledger_arbitrator(1 << 30);
        let (mut state, key) = ledger_state(&arbitrator);
        let mut admit = |source: usize, ordinal: u64| {
            state
                .admit_emitted(&key, row(source, ordinal), "out")
                .expect("admission")
        };

        let first_sink: Vec<bool> = (1..=3).map(|ordinal| admit(1, ordinal)).collect();
        assert_eq!(first_sink, [true, true, true]);
        let later_sink: Vec<bool> = (2..=4).map(|ordinal| admit(1, ordinal)).collect();
        assert_eq!(
            later_sink,
            [false, false, true],
            "a later Sink writes only the row no earlier rejection wrote"
        );
        assert!(
            admit(2, 2),
            "the same ordinal under another Source is another row"
        );
    }

    #[test]
    fn emitted_row_ledger_is_charged_to_the_arbitrator() {
        let arbitrator = ledger_arbitrator(1 << 30);
        let consumers_before = arbitrator.consumer_count();
        let usage_before = arbitrator.sum_consumer_usage();

        let (mut state, key) = ledger_state(&arbitrator);
        let baseline_usage = arbitrator.sum_consumer_usage();
        let baseline_charge = state.charged_bytes();
        for ordinal in 1..=10_000 {
            assert!(
                state
                    .admit_emitted(&key, row(1, ordinal), "out")
                    .expect("admission")
            );
        }
        state.settle_emitted(&key);
        let charged = state.charged_bytes() - baseline_charge;
        assert_eq!(
            arbitrator.sum_consumer_usage() - baseline_usage,
            charged,
            "the arbitrator sees exactly the ledger's charge"
        );
        let serialized = state.failed[&key].emitted.sources[0]
            .1
            .rows
            .serialized_size() as u64;
        assert!(
            charged >= serialized,
            "the charge {charged} covers at least the serialized {serialized} bytes"
        );
        assert!(
            charged <= 1024,
            "10,000 contiguous rows settle to at most 1 KiB, charged {charged}"
        );
        for ordinal in 1..=10_000 {
            assert!(
                !state
                    .admit_emitted(&key, row(1, ordinal), "out")
                    .expect("admission")
            );
        }
        assert_eq!(
            arbitrator.sum_consumer_usage() - baseline_usage,
            charged,
            "re-admitting recorded rows charges nothing"
        );

        let (mut sparse, sparse_key) = ledger_state(&arbitrator);
        let sparse_baseline = sparse.charged_bytes();
        for ordinal in (1..131_072).step_by(2) {
            assert!(
                sparse
                    .admit_emitted(&sparse_key, row(1, ordinal), "out")
                    .expect("admission")
            );
        }
        sparse.settle_emitted(&sparse_key);
        let sparse_charged = sparse.charged_bytes() - sparse_baseline;
        assert!(
            sparse_charged <= 131_072 / 8 + 2048,
            "every other row over 131,072 settles to one bit per row plus headers, charged {sparse_charged}"
        );

        drop(sparse);
        drop(state);
        assert_eq!(arbitrator.consumer_count(), consumers_before);
        assert_eq!(arbitrator.sum_consumer_usage(), usage_before);
    }

    #[test]
    fn emitted_row_ledger_growth_past_the_hard_limit_is_e310() {
        let arbitrator = ledger_arbitrator(1024);
        let (mut state, key) = ledger_state(&arbitrator);
        let mut refused = None;
        for step in 0..64u64 {
            let ordinal = 1 + step * (1 << 20);
            let charged_before = state.charged_bytes();
            match state.admit_emitted(&key, row(1, ordinal), "orders_out") {
                Ok(admitted) => assert!(admitted, "each scattered row is new"),
                Err(error) => {
                    assert_eq!(
                        state.charged_bytes(),
                        charged_before,
                        "the charge excludes the refused growth"
                    );
                    assert!(
                        !recorded(&state, &key, 1).contains(&ordinal),
                        "a refused row is not recorded"
                    );
                    refused = Some(error);
                    break;
                }
            }
        }
        match refused.expect("scattered rows reach the 1 KiB hard limit") {
            PipelineError::MemoryBudgetExceeded {
                node,
                limit,
                source,
                detail,
                ..
            } => {
                assert_eq!(node, "orders_out");
                assert_eq!(limit, 1024);
                assert_eq!(source, clinker_plan::BudgetCategory::Arena);
                assert!(
                    detail.is_some_and(|d| d.contains("dead-letter ledger")),
                    "the detail names the ledger"
                );
            }
            other => panic!("expected E310, got {other:?}"),
        }
    }

    /// Rows split between two Sinks by a Route, every other row to each,
    /// meet in the ledger as the second Sink fills every gap the first left.
    /// Each fill merges two runs; the settles shed the capacity the merges
    /// leave, so the charge stays near the compressed size.
    #[test]
    fn interleaved_rows_from_two_sinks_settle_compactly() {
        let arbitrator = ledger_arbitrator(1 << 30);
        let (mut state, key) = ledger_state(&arbitrator);
        let baseline = state.charged_bytes();
        for first in [1, 2] {
            for ordinal in (first..=400_000).step_by(2) {
                assert!(
                    state
                        .admit_emitted(&key, row(1, ordinal), "out")
                        .expect("admission")
                );
            }
            state.settle_emitted(&key);
        }
        assert_eq!(recorded(&state, &key, 1).len(), 400_000);
        let charged = state.charged_bytes() - baseline;
        assert!(
            charged <= 2048,
            "400,000 contiguous rows settle to a few runs, charged {charged}"
        );
    }

    /// The ledger's consumer frees nothing, so the default policy never
    /// elects it while a reclaimable consumer is registered, however much
    /// the ledger holds; asked to spill, it reports zero bytes freed.
    #[test]
    fn ledger_consumer_is_elected_only_after_every_reclaimable_consumer() {
        use crate::pipeline::memory::{ArbitrationPolicy, BackPressurePreferred, Priority};

        let arbitrator = ledger_arbitrator(1 << 30);
        let node_handle = ConsumerHandle::new();
        node_handle.set_bytes(16);
        let node_consumer = crate::executor::node_buffer::NodeBufferConsumer::new(node_handle);
        let node_id = arbitrator.register_consumer(Arc::new(
            crate::executor::node_buffer::NodeBufferConsumer::new(ConsumerHandle::new()),
        ));
        let (state, _key) = ledger_state(&arbitrator);
        state.handle.set_bytes(1 << 20);
        let ledger_consumer = DocumentDlqConsumer::new(Arc::clone(&state.handle));

        let snapshot: [(ConsumerId, &dyn MemoryConsumer); 2] = [
            (state.consumer_id, &ledger_consumer),
            (node_id, &node_consumer),
        ];
        assert_eq!(Priority.select_victim(&snapshot, 1 << 20), Some(node_id));
        assert_eq!(
            BackPressurePreferred::wrapping(Priority).select_victim(&snapshot, 1 << 20),
            Some(node_id)
        );
        assert!(!ledger_consumer.can_back_pressure());
        match ledger_consumer.try_spill(1 << 20) {
            Err(ConsumerSpillError::BelowTarget { freed, .. }) => assert_eq!(freed, 0),
            other => panic!("the ledger frees nothing: {other:?}"),
        }
    }

    /// A per-document bucket that spills to disk round-trips every record
    /// and row number through the drain — the path both the clean flush and
    /// the reject collateral walk take over a spilled bucket. Drives
    /// [`spill_bucket_in_place`] directly so the bucket's OWN spill is
    /// exercised in isolation from any upstream node-buffer spill (the
    /// upstream-spill document-identity gap is tracked at
    /// <https://github.com/rustpunk/clinker/issues/195>).
    #[test]
    fn bucket_spill_round_trips_records_and_row_numbers() {
        let s = schema();
        // A tiny budget so the registered consumer's reported bytes cross
        // the soft limit; the bucket then spills to disk.
        let arbitrator = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            64,
            0.5,
            0.4,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        let tmp = tempfile::tempdir().expect("tempdir");

        let handle = crate::pipeline::memory::ConsumerHandle::new();
        let consumer_id = arbitrator.register_consumer(Arc::new(
            crate::executor::node_buffer::NodeBufferConsumer::new(handle.clone()),
        ));
        let mut bucket = DocBucket {
            buffer: NodeBuffer::Memory(Vec::new()),
            consumer_id,
            handle,
            depth: 1,
        };

        let n: u64 = 64;
        for i in 0..n {
            bucket
                .buffer
                .push(rec(&s, i as i64, (i * 10) as i64), 1000 + i);
        }

        spill_bucket_in_place(
            &mut bucket,
            &arbitrator,
            "out",
            tmp.path(),
            clinker_plan::config::CompressMode::default(),
            8,
            s.column_count(),
        )
        .expect("bucket spills cleanly");

        // The bucket is now disk-backed: its in-memory tail is empty.
        assert!(
            matches!(bucket.buffer, NodeBuffer::Spilled { .. }),
            "the over-budget bucket promoted to a spilled buffer"
        );

        // Drain reproduces every record and row number in order — the exact
        // loop the reject / flush paths run over a spilled bucket.
        let mut drained: Vec<(i64, i64, u64)> = Vec::new();
        for event in bucket.buffer.drain() {
            if let StreamEvent::Record(record, row_num) = event.expect("spill drains cleanly") {
                let id = match &record.values()[0] {
                    Value::Integer(v) => *v,
                    other => panic!("unexpected id value: {other:?}"),
                };
                let value = match &record.values()[1] {
                    Value::Integer(v) => *v,
                    other => panic!("unexpected value: {other:?}"),
                };
                drained.push((id, value, row_num.ordinal()));
            }
        }
        arbitrator.unregister_consumer(consumer_id);

        let expected: Vec<(i64, i64, u64)> = (0..n)
            .map(|i| (i as i64, (i * 10) as i64, 1000 + i))
            .collect();
        assert_eq!(
            drained, expected,
            "every spilled record and row number round-trips in order"
        );
    }

    /// A per-document bucket whose spill file crosses the arbitrator's disk
    /// quota aborts with the structured `SpillCapExceeded` (E320) surface
    /// rather than continuing to fill the disk. Drives [`spill_bucket_in_place`]
    /// directly with a one-byte cap so the mem-tail flush overflows on its
    /// first write — the DLQ bucket path's own disk-cap guard, exercised in
    /// isolation from any upstream node-buffer spill.
    #[test]
    fn bucket_spill_past_disk_cap_aborts_with_e320() {
        use crate::pipeline::memory::assert_spill_cap_overflow;

        let s = schema();
        let arbitrator = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            64,
            0.5,
            0.4,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        // A one-byte disk quota: the mem-tail flush overflows it on the first
        // write, regardless of how the soft memory limit is set.
        arbitrator.set_max_spill_bytes(1).unwrap();
        let tmp = tempfile::tempdir().expect("tempdir");

        let handle = crate::pipeline::memory::ConsumerHandle::new();
        let consumer_id = arbitrator.register_consumer(Arc::new(
            crate::executor::node_buffer::NodeBufferConsumer::new(handle.clone()),
        ));
        let mut bucket = DocBucket {
            buffer: NodeBuffer::Memory(Vec::new()),
            consumer_id,
            handle,
            depth: 1,
        };

        for i in 0..64u64 {
            bucket
                .buffer
                .push(rec(&s, i as i64, (i * 10) as i64), 1000 + i);
        }

        let result = spill_bucket_in_place(
            &mut bucket,
            &arbitrator,
            "out",
            tmp.path(),
            clinker_plan::config::CompressMode::default(),
            8,
            s.column_count(),
        );

        assert_spill_cap_overflow(result, &arbitrator, "out", 1, tmp.path());

        arbitrator.unregister_consumer(consumer_id);
    }

    /// A clean document whose bucket SPILLED and then accumulated a resident
    /// in-memory tail (a `Mixed` buffer — memory pressure relaxed mid-
    /// document) is drained to the success sink in ARRIVAL order: the spilled
    /// head first, then the resident tail. `NodeBuffer::drain` alone would
    /// invert this (mem tail first), corrupting intra-document output order;
    /// `drain_records_in_arrival_order` restores it.
    #[test]
    fn clean_mixed_bucket_drains_in_arrival_order() {
        let s = schema();
        let arbitrator = Arc::new(crate::pipeline::memory::MemoryArbitrator::with_policy(
            64,
            0.5,
            0.4,
            Box::new(crate::pipeline::memory::NoOpPolicy),
        ));
        let tmp = tempfile::tempdir().expect("tempdir");
        let handle = crate::pipeline::memory::ConsumerHandle::new();
        let consumer_id = arbitrator.register_consumer(Arc::new(
            crate::executor::node_buffer::NodeBufferConsumer::new(handle.clone()),
        ));
        let mut bucket = DocBucket {
            buffer: NodeBuffer::Memory(Vec::new()),
            consumer_id,
            handle,
            depth: 1,
        };

        // Arrival order: rows 0..32 (the head) then rows 32..40 (the tail).
        // Push and spill the head, then push the tail so the bucket ends in
        // `Mixed` (spill chunk + resident mem tail) — the state a spilled
        // document whose pressure relaxed reaches.
        for i in 0..32u64 {
            bucket
                .buffer
                .push(rec(&s, i as i64, (i * 10) as i64), 100 + i);
        }
        spill_bucket_in_place(
            &mut bucket,
            &arbitrator,
            "out",
            tmp.path(),
            clinker_plan::config::CompressMode::default(),
            8,
            s.column_count(),
        )
        .expect("head spills");
        for i in 32..40u64 {
            bucket
                .buffer
                .push(rec(&s, i as i64, (i * 10) as i64), 100 + i);
        }
        assert!(
            matches!(bucket.buffer, NodeBuffer::Mixed { .. }),
            "after spilling then pushing more, the bucket is Mixed"
        );

        // The naive `NodeBuffer::drain` would yield the tail (mem) first.
        // Arrival-order drain must yield the head (spilled) first.
        let DocBucket {
            buffer,
            consumer_id,
            ..
        } = bucket;
        let rows: Vec<u64> = drain_records_in_arrival_order(buffer)
            .map(|item| item.expect("drains cleanly").1.ordinal())
            .collect();
        arbitrator.unregister_consumer(consumer_id);

        let expected: Vec<u64> = (100..140).collect();
        assert_eq!(
            rows, expected,
            "the clean Mixed bucket drains in arrival order (spilled head, then resident tail)"
        );
    }
}
