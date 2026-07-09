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
//! The rate counts per entry: a rejected N-record document emits one
//! `trigger: true` root cause plus N-1 `trigger: false` collateral entries,
//! each counting toward the rate denominator — so a rejected 1000-record
//! document contributes 1000 to the DLQ rate, matching the correlation
//! collateral precedent.
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
    ExecutorContext, MERGED_SOURCE_FILE, push_dlq, source_file_arc_of, source_name_arc_of,
};
use crate::executor::node_buffer::NodeBuffer;
use crate::executor::stream_event::StreamEvent;
use crate::executor::structured_output_guard::StructuredOutputDocumentGuard;
use crate::executor::{DlqEntry, build_format_writer};
use crate::projection::project_output_from_record;
use clinker_plan::config::OutputConfig;
use clinker_plan::error::PipelineError;

/// Identity of one document the policy operates on: its source file (the
/// outermost envelope level). Every record and boundary of a file shares
/// this `Arc<str>`, so it is the grain a failure rejects at.
type DocKey = Arc<str>;

/// Root-cause failure captured for a document the first time one of its
/// records (or an out-of-band validation) fails. Replayed as the single
/// `trigger: true` DLQ entry when the document is rejected.
struct DocTrigger {
    source_row: u64,
    category: clinker_core_types::dlq::DlqErrorCategory,
    error_message: String,
    original_record: Record,
    stage: Option<String>,
    route: Option<String>,
    source_name: Arc<str>,
    triggering_field: Option<Arc<str>>,
    triggering_value: Option<clinker_record::Value>,
}

/// Run-scoped document-DLQ state: which sources opt into the policy, the
/// failed-document set with each document's captured root-cause trigger,
/// and the run-scoped dedup set that keeps a duplicate close from
/// re-emitting a document's reject entries.
///
/// `Some(..)` on [`ExecutorContext::document_dlq`] iff at least one source
/// declares `dlq_granularity: document`; `None` otherwise (the dominant
/// per-record path, zero overhead). The per-document RECORD buffers live in
/// the Output arm's single invocation (a local driver), not here. Only the
/// cross-stage failure marks and the reject dedup are run-scoped, because an
/// upstream Transform / Route failure must be visible to the Output at the
/// document's close.
pub(crate) struct DocumentDlqState {
    /// Source-node names declaring `dlq_granularity: document`. A record is
    /// governed by the policy only when its originating source is in this
    /// set; records from `record`-granularity sources in the same run
    /// stream through untouched.
    doc_sources: HashSet<Arc<str>>,
    /// Documents marked failed, keyed by source file, with the root-cause
    /// trigger captured at the FIRST failure. Run-scoped so an upstream
    /// failure (Transform / Route, before any Output) is visible to every
    /// Output at the document's close.
    failed: HashMap<DocKey, DocTrigger>,
    /// Records of a failed document that ALSO failed (the 2nd, 3rd, … failure
    /// in the same document), keyed by source file. Only the first failure
    /// becomes the trigger; a later failing record never reaches an Output
    /// (it was suppressed at its Transform / Route failure site), so it is
    /// captured here and emitted as a `DocumentRejected` collateral at the
    /// document's reject — preserving the invariant that every record of a
    /// rejected N-record document contributes exactly one DLQ entry.
    extra_collaterals: HashMap<DocKey, Vec<(Record, u64)>>,
    /// Documents whose reject DLQ entries have already been emitted. A
    /// duplicate close (fan-in dedup escapee, malformed input) finds the key
    /// here and emits nothing more.
    rejected: HashSet<DocKey>,
}

impl DocumentDlqState {
    /// Build the run-scoped state from the set of document-granularity
    /// source names. Empty marks / dedup — the first marked failure
    /// populates them.
    pub(crate) fn new(doc_sources: HashSet<Arc<str>>) -> Self {
        Self {
            doc_sources,
            failed: HashMap::new(),
            extra_collaterals: HashMap::new(),
            rejected: HashSet::new(),
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
/// `record_error_to_buffer_if_grouped`, the correlation-buffer sibling.
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
    row_num: u64,
    category: clinker_core_types::dlq::DlqErrorCategory,
    error_message: String,
    stage: Option<String>,
    route: Option<String>,
    triggering_field: Option<Arc<str>>,
    triggering_value: Option<clinker_record::Value>,
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
            source_row: row_num,
            category,
            error_message,
            original_record: record.clone(),
            stage,
            route,
            source_name,
            triggering_field,
            triggering_value,
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
    match state.failed.entry(key.clone()) {
        std::collections::hash_map::Entry::Vacant(v) => {
            v.insert(trigger);
        }
        std::collections::hash_map::Entry::Occupied(_) => {
            state
                .extra_collaterals
                .entry(key)
                .or_default()
                .push((trigger.original_record, trigger.source_row));
        }
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
    output_name: String,
    out_cfg: &'cfg OutputConfig,
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
    writer: Option<Box<dyn clinker_format::FormatWriter>>,
    arbitrator: Arc<crate::pipeline::memory::MemoryArbitrator>,
    spill_root: Arc<std::path::Path>,
    spill_compress: clinker_plan::config::CompressMode,
    batch_size: usize,
    ok_count: u64,
    records_written: u64,
    structured_guard: StructuredOutputDocumentGuard,
}

impl<'cfg> DocumentDlqDriver<'cfg> {
    /// Build the driver for one Output arm invocation. `cxl_emit_names`
    /// drives `include_unmapped: false` projection; `None`/empty keeps the
    /// upstream passthrough.
    pub(crate) fn new(
        ctx: &ExecutorContext<'_>,
        output_name: &str,
        out_cfg: &'cfg OutputConfig,
        cxl_emit_names: Vec<String>,
    ) -> Self {
        let cxl_emit_names = if cxl_emit_names.is_empty() {
            None
        } else {
            Some(cxl_emit_names)
        };
        Self {
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
        }
    }

    /// Borrow (building on first sight) the bucket for file `key`. The
    /// arbitrator is passed in so the caller's other `&self` fields stay
    /// free of the `&mut self.buckets` borrow this returns.
    fn bucket_for<'a>(
        buckets: &'a mut HashMap<DocKey, DocBucket>,
        arbitrator: &crate::pipeline::memory::MemoryArbitrator,
        key: &DocKey,
    ) -> &'a mut DocBucket {
        buckets.entry(Arc::clone(key)).or_insert_with(|| {
            let handle = crate::pipeline::memory::ConsumerHandle::new();
            let consumer_id = arbitrator.register_consumer(Arc::new(
                crate::executor::node_buffer::NodeBufferConsumer::new(handle.clone()),
            ));
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
        row_num: u64,
    ) -> Result<(), PipelineError> {
        let column_count = record.schema().column_count();
        let bucket = Self::bucket_for(&mut self.buckets, &self.arbitrator, key);
        bucket.buffer.push(record, row_num);
        bucket
            .handle
            .set_bytes(bucket.buffer.estimated_memory_bytes());
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
        if !self.decided.insert(Arc::clone(key)) {
            return Ok(());
        }
        let bucket = self.buckets.remove(key);
        let is_failed = ctx
            .document_dlq
            .as_ref()
            .is_some_and(|s| s.failed.contains_key(key));
        if is_failed {
            reject_document_now(ctx, key, bucket)
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
        let cxl_emit_names_opt: Option<&[String]> = self.cxl_emit_names.as_deref();

        let mut projected: Vec<Record> = Vec::with_capacity(buffer.len_hint());
        // Drain in ARRIVAL order. The success sink is order-sensitive, and a
        // bucket that spilled and then kept a resident mem tail
        // (`NodeBuffer::Mixed`) has its NEWEST records in the mem tail —
        // `NodeBuffer::drain` yields mem-first, which would write the
        // post-spill tail ahead of the pre-spill body. Emitting the spill
        // chunks (older) before the resident tail (newer) restores arrival
        // order. (The reject path's DLQ order is not contractual, so it
        // drains directly.)
        for item in drain_records_in_arrival_order(buffer) {
            let (record, row_num) = item?;
            if let Err(err) = self
                .structured_guard
                .observe(&self.output_name, record.doc_ctx())
            {
                ctx.output_errors.push(err);
                self.arbitrator.unregister_consumer(consumer_id);
                return Ok(());
            }
            projected.push(project_output_from_record(
                &record,
                self.out_cfg,
                cxl_emit_names_opt,
            ));
            if ctx.ok_source_rows.insert(row_num) {
                self.ok_count += 1;
            }
            self.records_written += 1;
        }
        self.arbitrator.unregister_consumer(consumer_id);

        self.write_projected(ctx, &projected);
        Ok(())
    }

    /// Write a non-governed record straight through to this Output's writer,
    /// counting it toward `ok_count` / `records_written` exactly as the
    /// per-record Output path does. Used for records a `record`-policy
    /// source (or in-pipeline synthesis) routes to a document-DLQ Output,
    /// and for a late record arriving after its file already flushed clean.
    fn write_through(&mut self, ctx: &mut ExecutorContext<'_>, record: Record, row_num: u64) {
        if let Err(err) = self
            .structured_guard
            .observe(&self.output_name, record.doc_ctx())
        {
            ctx.output_errors.push(err);
            return;
        }
        let projected =
            project_output_from_record(&record, self.out_cfg, self.cxl_emit_names.as_deref());
        if ctx.ok_source_rows.insert(row_num) {
            self.ok_count += 1;
        }
        self.records_written += 1;
        self.write_projected(ctx, std::slice::from_ref(&projected));
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
            let output_schema = Arc::clone(projected[0].schema());
            match build_format_writer(self.out_cfg, raw_writer, output_schema) {
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
        row_num: u64,
    ) -> Result<(), PipelineError> {
        if self.decided.contains(&key) {
            let is_failed = ctx
                .document_dlq
                .as_ref()
                .is_some_and(|s| s.failed.contains_key(&key));
            if is_failed {
                push_document_collateral(ctx, &key, record, row_num)?;
            } else {
                self.write_through(ctx, record, row_num);
            }
            return Ok(());
        }
        self.buffer_record(&key, record, row_num)
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
                StreamEvent::Record(record, row_num) => {
                    // A governed record buffers under its file's policy; a
                    // non-governed one (synthetic id, file-less, or a
                    // `record`-policy source feeding the same Output) writes
                    // straight through, exactly as a per-record Output does.
                    let key = ctx
                        .document_dlq
                        .as_ref()
                        .and_then(|s| s.governing_key(&record));
                    match key {
                        Some(key) => self.admit_governed(ctx, key, record, row_num)?,
                        None => self.write_through(ctx, record, row_num),
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
                            Self::bucket_for(&mut self.buckets, &self.arbitrator, &file).depth += 1;
                        }
                        PunctuationKind::DocumentClose => {
                            let outermost = match self.buckets.get_mut(&file) {
                                Some(b) => {
                                    b.depth -= 1;
                                    b.depth <= 0
                                }
                                // No live bucket: either already decided, or
                                // a close with no tracked open (malformed) —
                                // treat as the file's terminal close.
                                None => true,
                            };
                            if outermost {
                                self.decide_document(ctx, &file)?;
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
        let mut remaining: Vec<DocKey> = self.buckets.keys().cloned().collect();
        remaining.sort_unstable();
        for key in remaining {
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
/// once each. Runs once after every Output arm so a document whose records
/// DID reach an Output (and was rejected there with its collaterals) is
/// already in `rejected` and is skipped. A no-op when the document-DLQ
/// buffer is inactive.
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
        .keys()
        .filter(|k| !state.rejected.contains(*k))
        .cloned()
        .collect();
    pending.sort_unstable();
    for key in pending {
        reject_document_now(ctx, &key, None)?;
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
) -> impl Iterator<Item = Result<(Record, u64), PipelineError>> {
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
    mem_records: Vec<(Record, u64)>,
    chunks: Vec<(crate::pipeline::spill::SpillFile<u64>, u64)>,
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
    };
    let mut mem_records: Vec<(Record, u64)> = Vec::with_capacity(mem_events.len());
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

/// Emit one `DocumentRejected` collateral for a single late record that
/// arrived after its document was already decided-failed. Counts toward the
/// DLQ rate like every other collateral.
fn push_document_collateral(
    ctx: &mut ExecutorContext<'_>,
    key: &DocKey,
    record: Record,
    row_num: u64,
) -> Result<(), PipelineError> {
    let source_name = source_name_arc_of(&record);
    push_dlq(
        ctx,
        DlqEntry {
            source_row: row_num,
            category: clinker_core_types::dlq::DlqErrorCategory::DocumentRejected,
            error_message: format!("document {key:?} rejected: a sibling record failed"),
            original_record: record,
            stage: Some("document_dlq".to_string()),
            route: None,
            trigger: false,
            source_name,
            triggering_field: None,
            triggering_value: None,
        },
    )
}

/// Emit the reject DLQ entries for failed document `key`, once per document
/// run-scoped: the captured root-cause trigger (`trigger: true`, its
/// original category) followed by a `DocumentRejected` collateral
/// (`trigger: false`) for every other buffered record of the document.
/// Every emitted entry counts toward the DLQ rate. Drops the document's
/// trigger and its bucket.
fn reject_document_now(
    ctx: &mut ExecutorContext<'_>,
    key: &DocKey,
    bucket: Option<DocBucket>,
) -> Result<(), PipelineError> {
    let already_rejected = {
        let state = ctx
            .document_dlq
            .as_mut()
            .expect("document_dlq is Some — checked by caller");
        !state.rejected.insert(Arc::clone(key))
    };
    if already_rejected {
        // Already rejected (duplicate close): release any bucket this caller
        // handed in so its consumer does not leak, and emit nothing more.
        if let Some(bucket) = bucket {
            bucket.handle.set_bytes(0);
            ctx.memory_budget.unregister_consumer(bucket.consumer_id);
        }
        return Ok(());
    }
    let (trigger, extra_collaterals) = {
        let state = ctx
            .document_dlq
            .as_mut()
            .expect("document_dlq is Some — checked above");
        (
            state.failed.remove(key),
            state.extra_collaterals.remove(key).unwrap_or_default(),
        )
    };

    // Collect this document's collateral rows out of the bucket before any
    // `push_dlq` borrows `ctx` mutably. Dedup collateral by `(source_name,
    // row_num)` so a fan-out that routed one source row to several Outputs
    // emits one collateral, not N — the same identity the correlation
    // collateral walk dedups on. The trigger's own row is seeded into the
    // seen set so the root-cause record is never also a collateral.
    let mut seen: HashSet<(Arc<str>, u64)> = HashSet::new();
    if let Some(t) = trigger.as_ref() {
        seen.insert((Arc::clone(&t.source_name), t.source_row));
    }
    let mut collaterals: Vec<(Record, u64, Arc<str>)> = Vec::new();
    // A non-first failing record of this document was suppressed at its
    // failure site (it never reached an Output bucket), so emit it as a
    // collateral here. Added before the bucket walk so a record that
    // somehow appears in both is deduped to one entry.
    for (record, row_num) in extra_collaterals {
        let source_name = source_name_arc_of(&record);
        if seen.insert((Arc::clone(&source_name), row_num)) {
            collaterals.push((record, row_num, source_name));
        }
    }
    if let Some(bucket) = bucket {
        let DocBucket {
            buffer,
            consumer_id,
            handle,
            ..
        } = bucket;
        handle.set_bytes(0);
        for event in buffer.drain() {
            if let StreamEvent::Record(record, row_num) = event? {
                let source_name = source_name_arc_of(&record);
                if seen.insert((Arc::clone(&source_name), row_num)) {
                    collaterals.push((record, row_num, source_name));
                }
            }
        }
        ctx.memory_budget.unregister_consumer(consumer_id);
    }

    if let Some(t) = trigger {
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
            },
        )?;
    }
    for (record, row_num, source_name) in collaterals {
        push_dlq(
            ctx,
            DlqEntry {
                source_row: row_num,
                category: clinker_core_types::dlq::DlqErrorCategory::DocumentRejected,
                error_message: format!("document {key:?} rejected: a sibling record failed"),
                original_record: record,
                stage: Some("document_dlq".to_string()),
                route: None,
                trigger: false,
                source_name,
                triggering_field: None,
                triggering_value: None,
            },
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{Schema, Value};

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec!["id".into(), "value".into()]))
    }

    fn rec(s: &Arc<Schema>, id: i64, value: i64) -> Record {
        Record::new(
            Arc::clone(s),
            vec![Value::Integer(id), Value::Integer(value)],
        )
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
                drained.push((id, value, row_num));
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
        arbitrator.set_max_spill_bytes(1);
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
            .map(|item| item.expect("drains cleanly").1)
            .collect();
        arbitrator.unregister_consumer(consumer_id);

        let expected: Vec<u64> = (100..140).collect();
        assert_eq!(
            rows, expected,
            "the clean Mixed bucket drains in arrival order (spilled head, then resident tail)"
        );
    }
}
