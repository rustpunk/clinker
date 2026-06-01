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

use crate::executor::stream_event::{Punctuation, StreamEvent};

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
