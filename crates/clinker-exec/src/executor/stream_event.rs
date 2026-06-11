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
//! Preserving; Merge = Reconciling — emits one downstream `Close` only
//! when every input has closed the same document; Aggregate /
//! Output = WindowBound — flush document-scoped state on `Close` then
//! forward).
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

/// Document-boundary punctuation on the executor's record stream.
///
/// Carries an `Arc<DocumentContext>` so operators reading the event can
/// (a) identify which document is opening/closing via `doc_ctx.id()`
/// and (b) access the document's envelope sections at the boundary
/// (e.g. trailer-validation operators reading `$doc.<section>.<field>`
/// from the same Arc that body records carried). The Arc clone is a
/// refcount bump only; punctuations are O(1) per document, not per
/// record.
#[derive(Debug, Clone)]
pub struct Punctuation {
    doc_ctx: Arc<DocumentContext>,
    kind: PunctuationKind,
}

impl Punctuation {
    /// Construct a punctuation for a given document context and kind.
    pub fn new(doc_ctx: Arc<DocumentContext>, kind: PunctuationKind) -> Self {
        Self { doc_ctx, kind }
    }

    /// Convenience: build a `DocumentOpen` punctuation.
    pub fn document_open(doc_ctx: Arc<DocumentContext>) -> Self {
        Self::new(doc_ctx, PunctuationKind::DocumentOpen)
    }

    /// Convenience: build a `DocumentClose` punctuation.
    pub fn document_close(doc_ctx: Arc<DocumentContext>) -> Self {
        Self::new(doc_ctx, PunctuationKind::DocumentClose)
    }

    /// Identity of the document this punctuation marks. Equality on
    /// this value is the Merge-dedup discriminator.
    pub fn doc_id(&self) -> DocumentId {
        self.doc_ctx.id()
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
/// `DocumentClose` boundary once per input. Forwarding that union
/// unchanged would open the document several times and — worse — close
/// it several times, double-firing any downstream consumer that flushes
/// on close (per-document Aggregate finalize, Output finalize).
///
/// The reconciliation emits `DocumentOpen` on first sighting (later
/// duplicates are dropped) and `DocumentClose` only once the close count
/// for a document reaches `fan_in_degree` — i.e. every input has closed
/// it. A document that traverses fewer than `fan_in_degree` inputs (e.g.
/// distinct source files with distinct [`DocumentId`]s, each appearing on
/// one input only) never reaches the threshold, so its close is withheld;
/// this is the intended fan-in semantics, identical across every
/// multi-input operator that calls this function. Input order is
/// preserved for the events that survive.
pub(crate) fn reconcile_document_boundaries(
    puncts: impl IntoIterator<Item = Punctuation>,
    fan_in_degree: usize,
) -> Vec<Punctuation> {
    let mut deduped: Vec<Punctuation> = Vec::new();
    let mut open_seen: HashSet<DocumentId> = HashSet::new();
    let mut close_counts: HashMap<DocumentId, usize> = HashMap::new();
    for p in puncts {
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
                if *count == fan_in_degree {
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
    use indexmap::IndexMap;

    fn doc_ctx() -> Arc<DocumentContext> {
        Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("doc.x12"),
            IndexMap::new(),
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
        let out = reconcile_document_boundaries(union, 2);
        assert_eq!(
            kinds_for(&out, id),
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ]
        );
    }

    #[test]
    fn reconcile_withholds_close_for_partial_coverage_document() {
        // A document that opens+closes on only ONE input never reaches the
        // fan-in degree, so its open is forwarded but its close is withheld
        // — the shared fan-in semantics across every multi-input operator.
        let ctx = doc_ctx();
        let id = ctx.id();
        let union = vec![
            Punctuation::document_open(Arc::clone(&ctx)),
            Punctuation::document_close(Arc::clone(&ctx)),
        ];
        let out = reconcile_document_boundaries(union, 2);
        assert_eq!(kinds_for(&out, id), vec![PunctuationKind::DocumentOpen]);
    }

    #[test]
    fn reconcile_is_per_document_independent() {
        // Two distinct documents reconcile independently: a fully-covered
        // one emits open+close, a partially-covered one emits open only.
        let spanning = doc_ctx();
        let partial = doc_ctx();
        let union = vec![
            Punctuation::document_open(Arc::clone(&spanning)),
            Punctuation::document_open(Arc::clone(&partial)),
            Punctuation::document_open(Arc::clone(&spanning)),
            Punctuation::document_close(Arc::clone(&spanning)),
            Punctuation::document_close(Arc::clone(&partial)),
            Punctuation::document_close(Arc::clone(&spanning)),
        ];
        let out = reconcile_document_boundaries(union, 2);
        assert_eq!(
            kinds_for(&out, spanning.id()),
            vec![
                PunctuationKind::DocumentOpen,
                PunctuationKind::DocumentClose
            ]
        );
        assert_eq!(
            kinds_for(&out, partial.id()),
            vec![PunctuationKind::DocumentOpen]
        );
    }

    #[test]
    fn reconcile_empty_input_yields_empty_output() {
        // The common case — no punctuations on either input — passes
        // through byte-identically to an empty vector.
        let out = reconcile_document_boundaries(Vec::new(), 2);
        assert!(out.is_empty());
    }
}
