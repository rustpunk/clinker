//! Inline punctuation events on the executor's record stream.
//!
//! Every channel and buffer between executor stages carries
//! [`StreamEvent`], a two-variant enum:
//!
//! - [`StreamEvent::Record`] — the existing `(Record, u64)` payload
//!   (record + row number within the source file).
//! - [`StreamEvent::Punctuation`] — a document-boundary signal carrying
//!   the `Arc<DocumentContext>` whose boundary is being marked.
//!
//! Source ingest emits one `DocumentOpen` punctuation before the first
//! body record of each source file and one `DocumentClose` after the
//! last. Operators preserve, transform, or consume punctuations
//! according to their punctuation discipline (Transform / Route =
//! Preserving; Merge / Combine = Reconciling — emits one downstream
//! `Close` per document once every input that opened that document has
//! also closed it; Aggregate / Output = WindowBound — flush
//! document-scoped state on `Close` then forward).
//!
//! Tucker 2003-style semantics: punctuations flow inline with records,
//! preserving strict ordering. Out-of-band side-channels (Vector
//! `SignalTo`, Logstash atomic boolean) are the FLINK-4329 failure
//! mode and explicitly rejected.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use clinker_record::{DocumentContext, DocumentId, Record};

/// Discriminator for [`Punctuation`] — what the punctuation is signaling.
///
/// Defined exhaustively from day one so adding a future variant is an
/// additive change with no `#[serde(default)]` workaround on existing
/// callers. `DocumentOpen` arrives before the first body record of a
/// document; `DocumentClose` arrives after the last.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PunctuationKind {
    DocumentOpen,
    DocumentClose,
}

/// Structural-integrity failure a source reader detected at an envelope
/// trailer (an X12 `SE`/`GE`/`IEA`, EDIFACT `UNT`/`UNZ`, or HL7 `BTS`/`FTS`
/// count mismatch). Rides on the file-level [`PunctuationKind::DocumentClose`]
/// the ingest thread emits for the condemned file.
///
/// The count is only known mid-stream — after every body record it counts
/// has already streamed — so the ingest thread cannot reject the document
/// before its first record. Instead it tags the document's close with this
/// payload; the consumer-side Source dispatch arm reads it and marks the
/// whole file failed through the document-DLQ reject seam, so #97's
/// per-file Output buffer rejects every already-streamed record of the file
/// at its close. The `record` is a representative body row of the document
/// (widened, doc-context-stamped) that becomes the captured `trigger: true`
/// root cause.
#[derive(Debug, Clone)]
pub struct StructuralReject {
    /// A representative record of the condemned document — the engine-widened,
    /// document-context-stamped row the reject seam captures as the
    /// `trigger: true` root cause for the file's reject.
    pub record: Record,
    /// Source row number of the representative record, for DLQ attribution.
    pub row_num: u64,
    /// The precise count-mismatch message the reader built at the trailer.
    pub message: String,
}

/// Document-boundary punctuation on the executor's record stream.
///
/// Carries an `Arc<DocumentContext>` so operators reading the event can
/// (a) identify which document is opening/closing via `doc_ctx.id()`
/// and (b) access the document's envelope sections at the boundary
/// (e.g. trailer-validation operators reading `$doc.<section>.<field>`
/// from the same Arc that body records carried). The Arc clone is a
/// refcount bump only; punctuations are O(1) per document, not per
/// record.
///
/// A file-level `DocumentClose` may additionally carry a
/// [`StructuralReject`] payload when the ingest thread condemned the file
/// for an envelope count mismatch under `dlq_granularity: document`; every
/// other punctuation leaves it `None`, so the structural-validation path
/// adds zero cost to the dominant boundary-marking flow and stays invisible
/// to operators that ignore it (they treat the close as an ordinary
/// boundary).
#[derive(Debug, Clone)]
pub struct Punctuation {
    doc_ctx: Arc<DocumentContext>,
    kind: PunctuationKind,
    structural_reject: Option<Box<StructuralReject>>,
}

impl Punctuation {
    /// Construct a punctuation for a given document context and kind.
    pub fn new(doc_ctx: Arc<DocumentContext>, kind: PunctuationKind) -> Self {
        Self {
            doc_ctx,
            kind,
            structural_reject: None,
        }
    }

    /// Convenience: build a `DocumentOpen` punctuation.
    pub fn document_open(doc_ctx: Arc<DocumentContext>) -> Self {
        Self::new(doc_ctx, PunctuationKind::DocumentOpen)
    }

    /// Convenience: build a `DocumentClose` punctuation.
    pub fn document_close(doc_ctx: Arc<DocumentContext>) -> Self {
        Self::new(doc_ctx, PunctuationKind::DocumentClose)
    }

    /// Build a `DocumentClose` punctuation that condemns the document for an
    /// envelope structural-count failure. The consumer-side Source dispatch
    /// arm reads the [`StructuralReject`] payload and marks the file failed
    /// via the document-DLQ reject seam before forwarding the close
    /// downstream.
    pub fn structural_reject_close(
        doc_ctx: Arc<DocumentContext>,
        reject: StructuralReject,
    ) -> Self {
        Self {
            doc_ctx,
            kind: PunctuationKind::DocumentClose,
            structural_reject: Some(Box::new(reject)),
        }
    }

    /// The structural-count reject payload this close carries, if any. `None`
    /// for every normal boundary; `Some` only on the file-level close of a
    /// document the ingest thread condemned for an envelope count mismatch.
    pub fn structural_reject(&self) -> Option<&StructuralReject> {
        self.structural_reject.as_deref()
    }

    /// Identity of the document this punctuation marks. Equality on
    /// this value is the Merge-dedup discriminator.
    pub fn doc_id(&self) -> DocumentId {
        self.doc_ctx.id()
    }

    /// Source file the marked document belongs to. Every envelope level of
    /// one file (the file-level document and each nested level) shares the
    /// same `source_file` Arc, so this identifies the OUTERMOST (file /
    /// interchange) document a nested-level boundary sits inside — the grain
    /// the per-document DLQ rejects at.
    pub fn source_file(&self) -> &Arc<str> {
        self.doc_ctx.source_file()
    }

    /// What boundary the punctuation marks.
    pub fn kind(&self) -> PunctuationKind {
        self.kind
    }
}

/// Inline channel/buffer payload — either a body record or a document
/// boundary signal.
///
/// Replaces the executor's previous `(Record, u64)` channel and
/// `NodeBuffer` payload. Operators that don't care about boundaries
/// pattern-match `StreamEvent::Record(rec, rn)` and forward
/// `StreamEvent::Punctuation(_)` unchanged. Operators that do care
/// (Aggregate flush, Output finalize, Merge dedup) intercept the
/// punctuation branch.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    Record(Record, u64),
    Punctuation(Punctuation),
}

impl StreamEvent {
    /// Construct a `Record` variant.
    pub fn record(record: Record, row_num: u64) -> Self {
        Self::Record(record, row_num)
    }

    /// Construct a `Punctuation` variant.
    pub fn punctuation(p: Punctuation) -> Self {
        Self::Punctuation(p)
    }

    /// `true` if this event is a record; `false` for any punctuation.
    /// Drives `NodeBuffer::len_hint`'s record-only count.
    pub fn is_record(&self) -> bool {
        matches!(self, Self::Record(..))
    }

    /// Consume into the `(Record, u64)` pair if this event is a record,
    /// otherwise discard the punctuation and return `None`. Used by the
    /// Output fan-out clone path and composition port seeding, which
    /// take records only.
    pub fn into_record(self) -> Option<(Record, u64)> {
        match self {
            Self::Record(r, rn) => Some((r, rn)),
            Self::Punctuation(_) => None,
        }
    }
}

/// Fold a fan-in's document-boundary punctuations down to one downstream
/// signal per document, the way a multi-input operator must.
///
/// A document whose records arrive across several inputs (a forked
/// document stream rejoining at a Merge, or a document spanning both
/// sides of a binary Combine) contributes its `DocumentOpen` /
/// `DocumentClose` boundary once per input that carries it. Forwarding
/// that union unchanged would open the document several times and —
/// worse — close it several times, double-firing any downstream
/// consumer that flushes on close (per-document Aggregate finalize,
/// Output finalize).
///
/// The reconciliation forwards each document's `DocumentOpen` on first
/// sighting (later duplicates are dropped) and its `DocumentClose`
/// exactly once — when every input that opened the document has also
/// closed it, i.e. when the per-document close count reaches the
/// per-document open count. A document carried by a single input
/// (distinct source files with distinct [`DocumentId`]s, a join whose
/// two sides carry different documents) has open count 1, so its close
/// forwards after that one input's close; a document genuinely spanning
/// N inputs forwards its close only after all N closes. Either way
/// exactly one downstream open and one downstream close survive per
/// document. Input order is preserved for the events that survive.
///
/// Malformed input is tolerated without breaking the single-close
/// guarantee: a document opened but never closed forwards its open and
/// no close (the downstream document stays open, and a per-document
/// consumer's end-of-stream tail flush handles it); a duplicate close
/// beyond the open count is dropped (the `==` test holds for exactly
/// one close); a close with no matching open is dropped.
pub(crate) fn reconcile_document_boundaries(
    puncts: impl IntoIterator<Item = Punctuation>,
) -> Vec<Punctuation> {
    let events: Vec<Punctuation> = puncts.into_iter().collect();

    // Pass 1 tallies how many inputs opened each document; that total is
    // the coverage target the document's close count must reach. The
    // pre-tally is load-bearing: a document spanning two inputs yields
    // the union `[open, close, open, close]`, so the first close arrives
    // with only one open seen so far. A single-pass "close == opens seen
    // so far" test would fire that close early, before the second input
    // closes. Counting all opens up front makes the close fire only on
    // the last input's close.
    let mut open_counts: HashMap<DocumentId, usize> = HashMap::new();
    for p in &events {
        if p.kind() == PunctuationKind::DocumentOpen {
            *open_counts.entry(p.doc_id()).or_insert(0) += 1;
        }
    }

    let mut deduped: Vec<Punctuation> = Vec::new();
    let mut open_seen: HashSet<DocumentId> = HashSet::new();
    let mut close_counts: HashMap<DocumentId, usize> = HashMap::new();
    for p in events {
        let id = p.doc_id();
        match p.kind() {
            PunctuationKind::DocumentOpen => {
                if open_seen.insert(id) {
                    deduped.push(p);
                }
            }
            PunctuationKind::DocumentClose => {
                let count = close_counts.entry(id).or_insert(0);
                *count += 1;
                if *count == open_counts.get(&id).copied().unwrap_or(0) {
                    deduped.push(p);
                }
            }
        }
    }
    deduped
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{DocumentContext, Schema, Value, synthetic_document_context};

    fn doc_ctx() -> Arc<DocumentContext> {
        Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("doc.x12"),
            clinker_record::EnvelopeRecord::empty(),
        ))
    }

    fn rec(id: i64) -> Record {
        Record::new(
            Arc::new(Schema::new(vec!["id".into()])),
            vec![Value::Integer(id)],
        )
    }

    #[test]
    fn record_event_carries_row_number() {
        let ev = StreamEvent::record(rec(1), 42);
        assert!(ev.is_record());
        let (r, rn) = match ev {
            StreamEvent::Record(r, rn) => (r, rn),
            StreamEvent::Punctuation(_) => panic!("expected Record"),
        };
        assert_eq!(rn, 42);
        assert_eq!(r.values()[0], Value::Integer(1));
    }

    #[test]
    fn ordinary_close_carries_no_structural_reject() {
        // The dominant boundary path leaves the structural-reject payload
        // empty, so a structural-validation check on any close is a cheap None.
        let ctx = doc_ctx();
        let close = Punctuation::document_close(Arc::clone(&ctx));
        assert!(close.structural_reject().is_none());
        let open = Punctuation::document_open(ctx);
        assert!(open.structural_reject().is_none());
    }

    #[test]
    fn structural_reject_close_carries_payload_and_is_a_close() {
        // A structural-reject close is a `DocumentClose` (so downstream
        // per-document consumers treat it as an ordinary boundary) that
        // additionally exposes its representative record, row, and message
        // for the consumer-side reject seam.
        let ctx = doc_ctx();
        let reject = StructuralReject {
            record: rec(7),
            row_num: 42,
            message: "SE segment count mismatch".to_string(),
        };
        let close = Punctuation::structural_reject_close(Arc::clone(&ctx), reject);
        assert_eq!(close.kind(), PunctuationKind::DocumentClose);
        let payload = close
            .structural_reject()
            .expect("a structural-reject close carries its payload");
        assert_eq!(payload.row_num, 42);
        assert_eq!(payload.record.values()[0], Value::Integer(7));
        assert!(payload.message.contains("SE segment count mismatch"));
    }

    #[test]
    fn punctuation_event_carries_doc_id_and_kind() {
        let ctx = synthetic_document_context();
        let ev = StreamEvent::punctuation(Punctuation::document_open(Arc::clone(&ctx)));
        assert!(!ev.is_record());
        let p = match ev {
            StreamEvent::Punctuation(p) => p,
            StreamEvent::Record(..) => panic!("expected Punctuation"),
        };
        assert_eq!(p.kind(), PunctuationKind::DocumentOpen);
        assert_eq!(p.doc_id(), ctx.id());
    }

    #[test]
    fn into_record_discards_punctuation() {
        let ctx = synthetic_document_context();
        let p_event = StreamEvent::punctuation(Punctuation::document_close(ctx));
        assert!(p_event.into_record().is_none());

        let r_event = StreamEvent::record(rec(5), 7);
        let (_, rn) = r_event.into_record().unwrap();
        assert_eq!(rn, 7);
    }

    fn kinds_for(puncts: &[Punctuation], id: DocumentId) -> Vec<PunctuationKind> {
        puncts
            .iter()
            .filter(|p| p.doc_id() == id)
            .map(|p| p.kind())
            .collect()
    }

    #[test]
    fn reconcile_collapses_duplicate_boundaries_for_spanning_document() {
        // One document whose open+close arrives on each of two inputs
        // collapses to a single open and a single close downstream — no
        // double-close to double-fire a downstream per-document flush.
        let ctx = doc_ctx();
        let id = ctx.id();
        let union = vec![
            Punctuation::document_open(Arc::clone(&ctx)),
            Punctuation::document_open(Arc::clone(&ctx)),
            Punctuation::document_close(Arc::clone(&ctx)),
            Punctuation::document_close(Arc::clone(&ctx)),
        ];
        let out = reconcile_document_boundaries(union);
        assert_eq!(
            kinds_for(&out, id),
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ]
        );
    }

    #[test]
    fn reconcile_holds_close_under_interleaved_spanning_order() {
        // The two-pass tally is load-bearing for the INTERLEAVED union order
        // `[open, close, open, close]` — the real ordering when each input's
        // `drain_split` yields its own document's `[open, close]` pair and the
        // two pairs chain together for a same-id document spanning both inputs.
        // Here the FIRST close arrives with only one open seen so far; a naive
        // single-pass "close == opens-seen-so-far" test would fire it early and
        // emit two downstream closes. Pre-tallying the total open count (2) in
        // pass 1 forces the close to fire only on the LAST close, so exactly
        // one open and one close survive.
        let ctx = doc_ctx();
        let id = ctx.id();
        let union = vec![
            Punctuation::document_open(Arc::clone(&ctx)),
            Punctuation::document_close(Arc::clone(&ctx)),
            Punctuation::document_open(Arc::clone(&ctx)),
            Punctuation::document_close(Arc::clone(&ctx)),
        ];
        let out = reconcile_document_boundaries(union);
        assert_eq!(
            kinds_for(&out, id),
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ],
            "interleaved spanning order must still yield exactly one open + one \
             close (a single-pass regression would fire the first close early)"
        );
    }

    #[test]
    fn reconcile_forwards_close_for_one_sided_document() {
        // A document that opens+closes on only ONE input (open count == 1)
        // forwards BOTH its open and its close — the common join case where
        // each side carries its own document, so each document's close must
        // reach a downstream per-document flush.
        let ctx = doc_ctx();
        let id = ctx.id();
        let union = vec![
            Punctuation::document_open(Arc::clone(&ctx)),
            Punctuation::document_close(Arc::clone(&ctx)),
        ];
        let out = reconcile_document_boundaries(union);
        assert_eq!(
            kinds_for(&out, id),
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ]
        );
    }

    #[test]
    fn reconcile_forwards_both_one_sided_documents() {
        // The join case: a driver document D and a build document B, each
        // carried by one input only. Both forward open+close independently.
        let driver = doc_ctx();
        let build = doc_ctx();
        let union = vec![
            Punctuation::document_open(Arc::clone(&driver)),
            Punctuation::document_close(Arc::clone(&driver)),
            Punctuation::document_open(Arc::clone(&build)),
            Punctuation::document_close(Arc::clone(&build)),
        ];
        let out = reconcile_document_boundaries(union);
        assert_eq!(
            kinds_for(&out, driver.id()),
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ]
        );
        assert_eq!(
            kinds_for(&out, build.id()),
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ]
        );
    }

    #[test]
    fn reconcile_emits_single_close_on_duplicate_close() {
        // A malformed input that double-closes a one-sided document
        // (open count 1, close count 2) still forwards exactly one close:
        // the close at which close-count == open-count forwards, every
        // later close is dropped. The single-close guarantee survives.
        let ctx = doc_ctx();
        let id = ctx.id();
        let union = vec![
            Punctuation::document_open(Arc::clone(&ctx)),
            Punctuation::document_close(Arc::clone(&ctx)),
            Punctuation::document_close(Arc::clone(&ctx)),
        ];
        let out = reconcile_document_boundaries(union);
        assert_eq!(
            kinds_for(&out, id),
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ]
        );
    }

    #[test]
    fn reconcile_is_per_document_independent() {
        // Two distinct documents reconcile independently: a fully-covered
        // spanning one (open count 2) emits open+close, a genuinely
        // never-closed one (open with no close) emits open only — covering
        // the unterminated-document branch.
        let spanning = doc_ctx();
        let unterminated = doc_ctx();
        let union = vec![
            Punctuation::document_open(Arc::clone(&spanning)),
            Punctuation::document_open(Arc::clone(&unterminated)),
            Punctuation::document_open(Arc::clone(&spanning)),
            Punctuation::document_close(Arc::clone(&spanning)),
            Punctuation::document_close(Arc::clone(&spanning)),
        ];
        let out = reconcile_document_boundaries(union);
        assert_eq!(
            kinds_for(&out, spanning.id()),
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ]
        );
        assert_eq!(
            kinds_for(&out, unterminated.id()),
            vec![PunctuationKind::DocumentOpen]
        );
    }

    #[test]
    fn reconcile_empty_input_yields_empty_output() {
        // The common case — no punctuations on either input — passes
        // through byte-identically to an empty vector.
        let out = reconcile_document_boundaries(Vec::new());
        assert!(out.is_empty());
    }
}
