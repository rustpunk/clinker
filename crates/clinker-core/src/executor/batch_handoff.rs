//! Bounded-batch inter-stage handoff carrying `StreamEvent` batches.
//!
//! [`EventBatch`] is the streaming counterpart of
//! [`crate::executor::node_buffer::NodeBuffer`]: where a `NodeBuffer`
//! holds a stage's *entire* output before the consumer drains it (full
//! materialization, charged as one slot against the arbitrator), an
//! `EventBatch` is one bounded slice of that output. A streaming stage
//! fills one batch, hands it off to its downstream consumer, and starts
//! a fresh one — it never accumulates or admits a whole-stage
//! `node_buffers` slot. When the consumer is a streaming `Output` thread
//! fed over a bounded channel (the fused `Source → Transform → Output`
//! chain), the channel's blocking `send` paces the producer, so peak
//! inter-stage memory for the stage is one batch plus the channel's
//! in-flight bound rather than the whole stage.
//!
//! The events inside a batch — records and `DocumentOpen` /
//! `DocumentClose` punctuations — stay in strict arrival order, and that
//! order is preserved *across* batch boundaries: a document whose records
//! are split over N batches keeps its trailing `DocumentClose` after the
//! last record of that document, even when the close lands in a later
//! batch than some of the document's records. This is the documented
//! anti-requirement — the punctuation-drop in the existing fused
//! Transform path (`transform_fused_consume`) is exactly what the batch
//! substrate must not inherit.
//!
//! Batches are accumulated through an [`EventBatcher`], which fills the
//! current batch up to a configured `batch_size` and flushes a full
//! batch to a sink closure. Because a batch's cut point is purely a
//! count threshold and the batcher never reorders events relative to the
//! order they were pushed, the trailing-punctuation invariant holds by
//! construction: a `DocumentClose` pushed after a document's last record
//! is appended after that record in whichever batch it lands in, and no
//! earlier batch can contain it.

use std::path::Path;
use std::sync::Arc;

use crate::error::PipelineError;
use crate::executor::stream_event::{Punctuation, StreamEvent};
use crate::pipeline::memory::{BudgetCategory, ConsumerHandle, MemoryArbitrator};

/// Default per-batch event count when no `pipeline.batch_size` knob and
/// no per-Transform override is set.
///
/// 2048 events balances the in-flight footprint of one batch against the
/// per-flush bookkeeping cost: small enough that one batch (plus the
/// streaming channel's in-flight bound) is a negligible fraction of the
/// default 512 MiB budget for typical record widths, large enough that
/// the per-flush handoff cost is amortized over thousands of records.
pub(crate) const DEFAULT_BATCH_SIZE: usize = 2048;

/// One bounded slice of a streaming stage's output.
///
/// Holds records and document-boundary punctuations interleaved in the
/// order they were produced. A batch is the unit the streaming stage
/// hands off to its downstream consumer; its event count is bounded by
/// the owning [`EventBatcher`]'s `batch_size`, so a single in-flight
/// batch caps the stage's own working set for one flush cycle.
#[derive(Debug, Default)]
pub(crate) struct EventBatch {
    events: Vec<StreamEvent>,
}

impl EventBatch {
    /// Construct an empty batch with capacity reserved for `cap` events.
    pub(crate) fn with_capacity(cap: usize) -> Self {
        Self {
            events: Vec::with_capacity(cap),
        }
    }

    /// Number of events (records and punctuations) in the batch.
    pub(crate) fn len(&self) -> usize {
        self.events.len()
    }

    /// `true` when the batch carries no events.
    pub(crate) fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Consume the batch into its event vector in arrival order.
    pub(crate) fn into_events(self) -> Vec<StreamEvent> {
        self.events
    }

    /// Heuristic in-memory footprint of the batch's records, mirroring
    /// [`crate::executor::node_buffer::NodeBuffer::estimated_memory_bytes`]:
    /// records contribute the shared
    /// [`crate::executor::node_buffer::record_byte_cost`] per row,
    /// punctuations contribute `0` (they are O(1) per document and never
    /// spill). The streaming per-batch charge `add_bytes` this value into
    /// the slot's [`crate::pipeline::memory::ConsumerHandle`] on flush so
    /// the arbitrator sees "batches in flight," and the writer
    /// `sub_bytes` the same amount on drain — the per-batch counterpart of
    /// the full-stage `estimate_node_buffer_bytes` charge.
    pub(crate) fn estimated_bytes(&self) -> u64 {
        let column_count = self.events.iter().find_map(|e| match e {
            StreamEvent::Record(r, _) => Some(r.schema().column_count()),
            StreamEvent::Punctuation(_) => None,
        });
        let Some(column_count) = column_count else {
            return 0;
        };
        let record_count = self.events.iter().filter(|e| e.is_record()).count();
        crate::executor::node_buffer::record_byte_cost(column_count)
            .saturating_mul(record_count as u64)
    }

    /// Append a record event, preserving arrival order.
    pub(crate) fn push_record(&mut self, record: clinker_record::Record, row_num: u64) {
        self.events.push(StreamEvent::record(record, row_num));
    }

    /// Append a punctuation event, preserving arrival order.
    ///
    /// A `DocumentClose` pushed after the document's last record lands
    /// after that record in the event stream and therefore in whichever
    /// batch the flush boundary places it — the trailing-punctuation
    /// invariant the substrate guarantees across batch splits.
    pub(crate) fn push_punctuation(&mut self, punct: Punctuation) {
        self.events.push(StreamEvent::punctuation(punct));
    }
}

/// Accumulates [`StreamEvent`]s into bounded [`EventBatch`]es and flushes
/// a full batch to a sink as soon as it reaches `batch_size`.
///
/// The batcher never reorders events: each `push_*` appends to the
/// current batch, and a flush hands off the whole current batch and
/// starts a fresh one. Because the cut point is a pure count threshold
/// applied in arrival order, punctuation ordering is preserved across
/// batch boundaries — a document's `DocumentClose` cannot be flushed
/// before a record that was pushed earlier, and records pushed after a
/// `DocumentOpen` cannot precede it. The owner calls [`Self::finish`]
/// once the upstream is exhausted to flush any partial trailing batch.
///
/// The sink is a fallible closure so a downstream send error (a closed
/// channel, a spill-quota overrun) propagates out of the push that
/// triggered the flush rather than being swallowed.
pub(crate) struct EventBatcher<F, E>
where
    F: FnMut(EventBatch) -> Result<(), E>,
{
    current: EventBatch,
    batch_size: usize,
    sink: F,
}

impl<F, E> EventBatcher<F, E>
where
    F: FnMut(EventBatch) -> Result<(), E>,
{
    /// Construct a batcher that flushes full `batch_size`-event batches
    /// to `sink`. A `batch_size` of zero is clamped to one so every
    /// event still flushes (a zero threshold would never flush and would
    /// accumulate the whole stage in memory — the opposite of the
    /// substrate's purpose).
    pub(crate) fn new(batch_size: usize, sink: F) -> Self {
        let batch_size = batch_size.max(1);
        Self {
            current: EventBatch::with_capacity(batch_size),
            batch_size,
            sink,
        }
    }

    /// Push a record, flushing the current batch first if it is full.
    pub(crate) fn push_record(
        &mut self,
        record: clinker_record::Record,
        row_num: u64,
    ) -> Result<(), E> {
        self.flush_if_full()?;
        self.current.push_record(record, row_num);
        Ok(())
    }

    /// Push a punctuation, flushing the current batch first if it is
    /// full. Pushing the punctuation after the flush keeps it ordered
    /// after every record already accumulated in the prior batch.
    pub(crate) fn push_punctuation(&mut self, punct: Punctuation) -> Result<(), E> {
        self.flush_if_full()?;
        self.current.push_punctuation(punct);
        Ok(())
    }

    /// Flush the current batch when it has reached `batch_size` events.
    fn flush_if_full(&mut self) -> Result<(), E> {
        if self.current.len() >= self.batch_size {
            self.flush_current()?;
        }
        Ok(())
    }

    /// Hand the current batch to the sink and start a fresh one. A no-op
    /// on an empty current batch so a flush at an exact boundary does not
    /// emit a spurious empty batch.
    fn flush_current(&mut self) -> Result<(), E> {
        if self.current.is_empty() {
            return Ok(());
        }
        let batch = std::mem::replace(
            &mut self.current,
            EventBatch::with_capacity(self.batch_size),
        );
        (self.sink)(batch)
    }

    /// Flush any partial trailing batch. Called once the upstream is
    /// exhausted so the final under-full batch reaches the sink.
    pub(crate) fn finish(mut self) -> Result<(), E> {
        self.flush_current()
    }
}

/// Per-batch arbitrator accounting for a streaming inter-stage handoff.
///
/// Where [`crate::executor::dispatch::admit_node_buffer`] charges a
/// blocking stage's *entire* output as one `NodeBufferConsumer` slot, a
/// streaming stage registers exactly one consumer wrapper for its logical
/// slot and then drives this handle once per flushed [`EventBatch`]:
///
/// - **Charge on flush.** [`Self::charge_and_route`] adds the batch's
///   [`EventBatch::estimated_bytes`] to the shared
///   [`ConsumerHandle`] before the events leave the producer, so the
///   arbitrator's `current_usage` for the slot reflects "batches in
///   flight," never the whole stage.
/// - **Discharge on consume.** The downstream writer thread holds a clone
///   of the same `ConsumerHandle` and subtracts each consumed record's
///   [`crate::executor::node_buffer::record_byte_cost`] as it drains, so
///   the live count tracks exactly what is still buffered between the
///   producer and the writer.
///
/// On a soft-threshold trip (`MemoryArbitrator::should_spill_self()`,
/// which observes RSS without driving the pausing arbitration round) —
/// and only when `spill_allowed` certifies the slot has a single drain
/// consumer — the flushed batch's records round-trip through a
/// `SpillFile<u64>` on disk instead of being held in the producer's
/// working set: each maximal run of consecutive records is written out,
/// re-read, and forwarded to the writer one at a time, relieving the
/// in-memory peak for that batch. Punctuations are forwarded in place
/// between the runs they separate, so the records-and-punctuations
/// interleaving the producer emitted survives the spill round-trip
/// byte-for-byte in arrival order — a `DocumentOpen` that frames a run
/// still precedes that run's records, and a `DocumentClose` still trails
/// them, exactly as on the in-memory path. `spill_allowed` is `false` for
/// any slot whose consumer would reach `NodeBuffer::clone_memory_only`
/// (multi-consumer fan-out / composition input-port edge), because that
/// method panics on spill-backed variants.
///
/// One `StreamingChargeHandle` lives per producer slot for the slot's
/// whole lifetime; the wrapper it pairs with is unregistered when the
/// stream finishes.
pub(crate) struct StreamingChargeHandle {
    handle: Arc<ConsumerHandle>,
    arbitrator: Arc<MemoryArbitrator>,
    spill_root_path: Arc<Path>,
    node_name: String,
    spill_allowed: bool,
}

impl StreamingChargeHandle {
    /// Construct a charge handle for one streaming producer slot.
    ///
    /// `handle` is the shared [`ConsumerHandle`] already registered with
    /// the arbitrator as a `NodeBufferConsumer`; the same `Arc` is cloned
    /// into the writer thread so the discharge side subtracts from the
    /// counter this side adds to. `spill_allowed` mirrors
    /// `dispatch::node_buffer_spill_allowed` for the slot.
    pub(crate) fn new(
        handle: Arc<ConsumerHandle>,
        arbitrator: Arc<MemoryArbitrator>,
        spill_root_path: Arc<Path>,
        node_name: String,
        spill_allowed: bool,
    ) -> Self {
        Self {
            handle,
            arbitrator,
            spill_root_path,
            node_name,
            spill_allowed,
        }
    }

    /// Charge one flushed batch against the arbitrator and route its
    /// events downstream through `send`.
    ///
    /// Adds the batch's estimated bytes to the slot's handle and samples
    /// the arbitrator's peak charged usage. When `spill_allowed` and the
    /// soft RSS threshold has tripped, each maximal run of consecutive
    /// records is spilled to a `SpillFile<u64>` and streamed back out from
    /// disk one at a time (relieving the producer's in-memory peak for the
    /// batch) with its spill bytes recorded against the disk quota; an
    /// over-quota total surfaces the structured `MemoryBudgetExceeded`
    /// shape with `detail: "spill quota exceeded"`. Punctuations are
    /// forwarded in place between the runs they separate, so the spill path
    /// emits the batch's events in the same arrival order the in-memory
    /// path does — the records-and-punctuations interleaving the producer
    /// built is preserved across the disk round-trip. Otherwise the events
    /// go straight to `send` in memory.
    ///
    /// `send` is the producer's bounded-channel send; its blocking
    /// back-pressure paces the producer when the writer falls behind.
    pub(crate) fn charge_and_route(
        &self,
        batch: EventBatch,
        mut send: impl FnMut(StreamEvent) -> Result<(), PipelineError>,
    ) -> Result<(), PipelineError> {
        let bytes = batch.estimated_bytes();
        self.handle.add_bytes(bytes);
        self.arbitrator.sample_peak_consumer_usage();

        if self.spill_allowed && self.arbitrator.should_spill_self() {
            // Spill record *runs* in place rather than partitioning the
            // batch into all-records-then-all-punctuations: a punctuation
            // flushes the run that preceded it through disk and is then
            // forwarded at its own offset, so a `[DocumentOpen, r0, r1,
            // DocumentClose]` batch re-emits in that exact order instead of
            // moving the open after the records it frames.
            let mut run: Vec<(clinker_record::Record, u64)> = Vec::new();
            for event in batch.into_events() {
                match event {
                    StreamEvent::Record(record, rn) => run.push((record, rn)),
                    StreamEvent::Punctuation(p) => {
                        self.spill_and_forward_run(std::mem::take(&mut run), &mut send)?;
                        send(StreamEvent::punctuation(p))?;
                    }
                }
            }
            self.spill_and_forward_run(run, &mut send)?;
            return Ok(());
        }

        for event in batch.into_events() {
            send(event)?;
        }
        Ok(())
    }

    /// Spill one run of consecutive records to disk, then re-read and
    /// forward them in order through `send`.
    ///
    /// The writer's discharge path stays uniform — it always sees
    /// `StreamEvent::Record`s and subtracts their per-record cost
    /// regardless of whether the batch round-tripped through disk. An
    /// empty run is a no-op (no spill file). Records the spilled file's
    /// on-disk size against the arbitrator's disk quota and surfaces
    /// `MemoryBudgetExceeded` when the cumulative total exceeds the cap.
    fn spill_and_forward_run(
        &self,
        run: Vec<(clinker_record::Record, u64)>,
        send: &mut impl FnMut(StreamEvent) -> Result<(), PipelineError>,
    ) -> Result<(), PipelineError> {
        let Some((file, _count)) = crate::executor::node_buffer_spill::spill_node_buffer(
            run,
            Some(self.spill_root_path.as_ref()),
        )?
        else {
            return Ok(());
        };
        let file_bytes = std::fs::metadata(file.path()).map(|m| m.len()).unwrap_or(0);
        if self.arbitrator.record_spill_bytes(file_bytes) {
            return Err(PipelineError::MemoryBudgetExceeded {
                node: self.node_name.clone(),
                used: self.arbitrator.cumulative_spill_bytes(),
                limit: self.arbitrator.max_spill_bytes(),
                source: BudgetCategory::NodeBuffer,
                detail: Some("spill quota exceeded".to_string()),
            });
        }
        for item in file.reader()? {
            let (record, rn) = item?;
            send(StreamEvent::record(record, rn))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use clinker_record::{
        DocumentContext, DocumentId, Record, Schema, Value, synthetic_document_context,
    };
    use indexmap::IndexMap;

    use crate::executor::stream_event::PunctuationKind;

    fn rec(id: i64) -> Record {
        Record::new(
            Arc::new(Schema::new(vec!["id".into()])),
            vec![Value::Integer(id)],
        )
    }

    /// Build a document context with a fresh, distinct id so two
    /// documents in one test compare unequal (the process-wide synthetic
    /// singleton shares one id and cannot distinguish documents).
    fn distinct_doc() -> Arc<DocumentContext> {
        Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from(""),
            IndexMap::new(),
        ))
    }

    /// Sink closure type the test driver hands each batcher. Records
    /// every flushed batch's length and flattens its events so a test
    /// can assert both the per-batch cut points and the flattened order.
    type TestSink = Box<dyn FnMut(EventBatch) -> Result<(), ()>>;

    /// Drive a batcher to completion and return `(flattened events,
    /// per-batch sizes)`. The shared `Mutex` cells let the sink closure
    /// (which the batcher owns) write back into the test's collectors.
    fn collect(
        batch_size: usize,
        push: impl FnOnce(&mut EventBatcher<TestSink, ()>) -> Result<(), ()>,
    ) -> (Vec<StreamEvent>, Vec<usize>) {
        let flat: Arc<std::sync::Mutex<Vec<StreamEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let sizes: Arc<std::sync::Mutex<Vec<usize>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let flat_sink = Arc::clone(&flat);
        let sizes_sink = Arc::clone(&sizes);
        let sink: TestSink = Box::new(move |batch: EventBatch| {
            sizes_sink.lock().unwrap().push(batch.len());
            flat_sink.lock().unwrap().extend(batch.into_events());
            Ok(())
        });
        let mut batcher = EventBatcher::new(batch_size, sink);
        push(&mut batcher).unwrap();
        batcher.finish().unwrap();
        let flat_out = Arc::try_unwrap(flat).unwrap().into_inner().unwrap();
        let sizes_out = Arc::try_unwrap(sizes).unwrap().into_inner().unwrap();
        (flat_out, sizes_out)
    }

    #[test]
    fn flushes_full_batches_and_a_partial_tail() {
        let (flat, sizes) = collect(2, |b| {
            for i in 0..5 {
                b.push_record(rec(i), i as u64)?;
            }
            Ok(())
        });
        // 5 records at batch_size 2 → batches of 2, 2, 1.
        assert_eq!(sizes, vec![2, 2, 1]);
        assert_eq!(flat.len(), 5);
        for (i, ev) in flat.iter().enumerate() {
            match ev {
                StreamEvent::Record(_, rn) => assert_eq!(*rn, i as u64),
                StreamEvent::Punctuation(_) => panic!("expected only records"),
            }
        }
    }

    #[test]
    fn trailing_close_stays_after_last_record_across_a_split() {
        let ctx = synthetic_document_context();
        let doc_id = ctx.id();
        // Open, three records, close — at batch_size 2 the document's
        // records split across batches and the close lands in a later
        // batch than the open and the first records.
        let (flat, sizes) = collect(2, |b| {
            b.push_punctuation(Punctuation::document_open(Arc::clone(&ctx)))?;
            for i in 0..3 {
                b.push_record(rec(i), i as u64)?;
            }
            b.push_punctuation(Punctuation::document_close(Arc::clone(&ctx)))?;
            Ok(())
        });
        // open, r0 | r1, r2 | close  → batches of 2, 2, 1.
        assert_eq!(sizes, vec![2, 2, 1]);

        // The open precedes every record; the close trails every record.
        let open_pos = flat
            .iter()
            .position(|e| matches!(e, StreamEvent::Punctuation(p) if p.kind() == PunctuationKind::DocumentOpen))
            .unwrap();
        let close_pos = flat
            .iter()
            .position(|e| matches!(e, StreamEvent::Punctuation(p) if p.kind() == PunctuationKind::DocumentClose))
            .unwrap();
        let record_positions: Vec<usize> = flat
            .iter()
            .enumerate()
            .filter_map(|(i, e)| e.is_record().then_some(i))
            .collect();
        assert!(record_positions.iter().all(|&p| open_pos < p));
        assert!(record_positions.iter().all(|&p| close_pos > p));

        // Both punctuations carry the same document identity.
        for e in &flat {
            if let StreamEvent::Punctuation(p) = e {
                assert_eq!(p.doc_id(), doc_id);
            }
        }
    }

    #[test]
    fn two_documents_keep_their_boundaries_separate_across_splits() {
        let doc_a = distinct_doc();
        let doc_b = distinct_doc();
        let (flat, _sizes) = collect(2, |b| {
            b.push_punctuation(Punctuation::document_open(Arc::clone(&doc_a)))?;
            b.push_record(rec(0), 0)?;
            b.push_record(rec(1), 1)?;
            b.push_punctuation(Punctuation::document_close(Arc::clone(&doc_a)))?;
            b.push_punctuation(Punctuation::document_open(Arc::clone(&doc_b)))?;
            b.push_record(rec(2), 2)?;
            b.push_punctuation(Punctuation::document_close(Arc::clone(&doc_b)))?;
            Ok(())
        });
        // doc_a's close must precede doc_b's open (no interleaving of
        // boundaries across documents).
        let a_close = flat
            .iter()
            .position(|e| {
                matches!(e, StreamEvent::Punctuation(p)
                if p.kind() == PunctuationKind::DocumentClose && p.doc_id() == doc_a.id())
            })
            .unwrap();
        let b_open = flat
            .iter()
            .position(|e| {
                matches!(e, StreamEvent::Punctuation(p)
                if p.kind() == PunctuationKind::DocumentOpen && p.doc_id() == doc_b.id())
            })
            .unwrap();
        assert!(a_close < b_open);
    }

    #[test]
    fn empty_batcher_finish_emits_nothing() {
        let (flat, sizes) = collect(4, |_b| Ok(()));
        assert!(flat.is_empty());
        assert!(sizes.is_empty());
    }

    #[test]
    fn zero_batch_size_clamps_to_one() {
        let (flat, sizes) = collect(0, |b| {
            b.push_record(rec(0), 0)?;
            b.push_record(rec(1), 1)?;
            Ok(())
        });
        // Clamped to 1 → one record per batch.
        assert_eq!(sizes, vec![1, 1]);
        assert_eq!(flat.len(), 2);
    }

    #[test]
    fn estimated_bytes_counts_records_only() {
        let ctx = synthetic_document_context();
        let mut batch = EventBatch::with_capacity(4);
        // Empty batch and punctuation-only batch both cost zero.
        assert_eq!(EventBatch::with_capacity(0).estimated_bytes(), 0);
        batch.push_punctuation(Punctuation::document_open(Arc::clone(&ctx)));
        assert_eq!(batch.estimated_bytes(), 0, "puncts contribute zero");
        batch.push_record(rec(1), 1);
        batch.push_record(rec(2), 2);
        // Two records of a 1-column schema; punctuations still contribute 0.
        let per_row = crate::executor::node_buffer::record_byte_cost(1);
        assert_eq!(batch.estimated_bytes(), per_row * 2);
    }

    /// The streaming per-batch spill path round-trips a batch's records
    /// through disk while preserving the full records-and-punctuations
    /// interleaving — the leading `DocumentOpen` still precedes the run it
    /// frames and the trailing `DocumentClose` still follows it — and
    /// records the spill bytes against the arbitrator. Forces a
    /// deterministic soft-threshold trip by pinning the arbitrator's limit
    /// and peak RSS so `should_spill()` fires on every platform regardless
    /// of `rss_bytes()` availability.
    #[test]
    fn charge_handle_spills_a_batch_preserving_order_and_close() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};

        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            64 * 1024 * 1024,
            0.80,
            Box::new(NoOpPolicy),
        ));
        // Force the soft threshold to trip unconditionally. The streaming
        // charge path polls `should_spill_self` (no pausing round), so the
        // precondition asserts that variant.
        arbitrator.set_limit(1);
        arbitrator.set_peak_rss_for_test(u64::MAX);
        assert!(arbitrator.should_spill_self());

        let spill_dir = tempfile::tempdir().expect("temp dir");
        let spill_root: Arc<std::path::Path> = Arc::from(spill_dir.path());
        let handle = ConsumerHandle::new();
        let charge = StreamingChargeHandle::new(
            handle.clone(),
            Arc::clone(&arbitrator),
            spill_root,
            "stream-spill-test".to_string(),
            true,
        );

        // One document: open, three records, close — at this batch size
        // the whole document is one batch, so the close trails its records
        // even after the records round-trip through the spill file.
        let ctx = synthetic_document_context();
        let mut batch = EventBatch::with_capacity(8);
        batch.push_punctuation(Punctuation::document_open(Arc::clone(&ctx)));
        for i in 0..3 {
            batch.push_record(rec(i), i as u64);
        }
        batch.push_punctuation(Punctuation::document_close(Arc::clone(&ctx)));

        let routed: Arc<std::sync::Mutex<Vec<StreamEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let routed_sink = Arc::clone(&routed);
        charge
            .charge_and_route(batch, move |event| {
                routed_sink.lock().unwrap().push(event);
                Ok(())
            })
            .expect("charge_and_route on the spill path");

        let out = Arc::try_unwrap(routed).unwrap().into_inner().unwrap();
        // The spill path emits the same arrival order the in-memory path
        // would: leading open, then the three records (re-read from the
        // spill file in order), then the trailing close. The open frames
        // the run it precedes and the close trails it — the substrate's
        // strict-arrival-order invariant, preserved across the disk
        // round-trip rather than collapsed to records-then-punctuations.
        let record_rns: Vec<u64> = out
            .iter()
            .filter_map(|e| match e {
                StreamEvent::Record(_, rn) => Some(*rn),
                StreamEvent::Punctuation(_) => None,
            })
            .collect();
        assert_eq!(
            record_rns,
            vec![0, 1, 2],
            "spill round-trip preserved order"
        );
        let open_pos = out
            .iter()
            .position(|e| matches!(e, StreamEvent::Punctuation(p) if p.kind() == PunctuationKind::DocumentOpen))
            .expect("leading open forwarded at its arrival offset");
        let close_pos = out
            .iter()
            .position(|e| matches!(e, StreamEvent::Punctuation(p) if p.kind() == PunctuationKind::DocumentClose))
            .expect("close forwarded after the spilled records");
        let first_record_pos = out
            .iter()
            .position(|e| e.is_record())
            .expect("records present");
        let last_record_pos = out
            .iter()
            .rposition(|e| e.is_record())
            .expect("records present");
        assert!(
            open_pos < first_record_pos,
            "DocumentOpen must precede the records it frames across the spill round-trip"
        );
        assert!(
            close_pos > last_record_pos,
            "DocumentClose must trail every record across the spill round-trip"
        );

        // The arbitrator recorded the batch's spill to disk.
        assert!(
            arbitrator.cumulative_spill_bytes() > 0,
            "the spilled batch must have recorded spill bytes"
        );
    }

    /// A batch whose records are split into two runs by an interior
    /// `Close`/`Open` pair re-emits each run in place across the spill
    /// path: the two documents stay distinct and every event keeps its
    /// arrival offset, proving the spill path spills record *runs* rather
    /// than collapsing the batch to all-records-then-all-punctuations.
    #[test]
    fn charge_handle_spill_preserves_two_interleaved_document_runs() {
        use crate::pipeline::memory::{MemoryArbitrator, NoOpPolicy};

        let arbitrator = Arc::new(MemoryArbitrator::with_policy(
            64 * 1024 * 1024,
            0.80,
            Box::new(NoOpPolicy),
        ));
        arbitrator.set_limit(1);
        arbitrator.set_peak_rss_for_test(u64::MAX);
        assert!(arbitrator.should_spill_self());

        let spill_dir = tempfile::tempdir().expect("temp dir");
        let spill_root: Arc<std::path::Path> = Arc::from(spill_dir.path());
        let handle = ConsumerHandle::new();
        let charge = StreamingChargeHandle::new(
            handle.clone(),
            Arc::clone(&arbitrator),
            spill_root,
            "stream-spill-two-docs".to_string(),
            true,
        );

        // [Open(A), r0, r1, Close(A), Open(B), r2, Close(B)] — two record
        // runs separated by the A-close / B-open boundary.
        let doc_a = distinct_doc();
        let doc_b = distinct_doc();
        let mut batch = EventBatch::with_capacity(8);
        batch.push_punctuation(Punctuation::document_open(Arc::clone(&doc_a)));
        batch.push_record(rec(0), 0);
        batch.push_record(rec(1), 1);
        batch.push_punctuation(Punctuation::document_close(Arc::clone(&doc_a)));
        batch.push_punctuation(Punctuation::document_open(Arc::clone(&doc_b)));
        batch.push_record(rec(2), 2);
        batch.push_punctuation(Punctuation::document_close(Arc::clone(&doc_b)));

        let routed: Arc<std::sync::Mutex<Vec<StreamEvent>>> =
            Arc::new(std::sync::Mutex::new(Vec::new()));
        let routed_sink = Arc::clone(&routed);
        charge
            .charge_and_route(batch, move |event| {
                routed_sink.lock().unwrap().push(event);
                Ok(())
            })
            .expect("charge_and_route on the spill path");

        let out = Arc::try_unwrap(routed).unwrap().into_inner().unwrap();
        // Flatten to a comparable shape: punctuations as (kind, doc_id),
        // records as their row number, then assert the exact arrival order.
        #[derive(Debug, PartialEq)]
        enum Shape {
            Open(DocumentId),
            Close(DocumentId),
            Rec(u64),
        }
        let shapes: Vec<Shape> = out
            .iter()
            .map(|e| match e {
                StreamEvent::Record(_, rn) => Shape::Rec(*rn),
                StreamEvent::Punctuation(p) => match p.kind() {
                    PunctuationKind::DocumentOpen => Shape::Open(p.doc_id()),
                    PunctuationKind::DocumentClose => Shape::Close(p.doc_id()),
                },
            })
            .collect();
        assert_eq!(
            shapes,
            vec![
                Shape::Open(doc_a.id()),
                Shape::Rec(0),
                Shape::Rec(1),
                Shape::Close(doc_a.id()),
                Shape::Open(doc_b.id()),
                Shape::Rec(2),
                Shape::Close(doc_b.id()),
            ],
            "both record runs re-emit in place with their framing punctuations"
        );
    }

    #[test]
    fn batch_len_counts_records_and_punctuations() {
        let ctx = synthetic_document_context();
        let mut batch = EventBatch::with_capacity(4);
        batch.push_punctuation(Punctuation::document_open(Arc::clone(&ctx)));
        batch.push_record(rec(0), 0);
        batch.push_record(rec(1), 1);
        batch.push_punctuation(Punctuation::document_close(ctx));
        assert_eq!(batch.len(), 4);
        let events = batch.into_events();
        assert_eq!(events.iter().filter(|e| e.is_record()).count(), 2);
    }
}
