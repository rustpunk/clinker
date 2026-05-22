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

    /// Borrow the underlying document context.
    pub fn doc_ctx(&self) -> &Arc<DocumentContext> {
        &self.doc_ctx
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
    pub fn is_record(&self) -> bool {
        matches!(self, Self::Record(..))
    }

    /// `true` if this event is a punctuation.
    pub fn is_punctuation(&self) -> bool {
        matches!(self, Self::Punctuation(_))
    }

    /// Borrow the contained record, if this event is a `Record`.
    pub fn as_record(&self) -> Option<(&Record, u64)> {
        match self {
            Self::Record(r, rn) => Some((r, *rn)),
            Self::Punctuation(_) => None,
        }
    }

    /// Consume into the `(Record, u64)` pair if this event is a record,
    /// otherwise discard the punctuation and return `None`.
    pub fn into_record(self) -> Option<(Record, u64)> {
        match self {
            Self::Record(r, rn) => Some((r, rn)),
            Self::Punctuation(_) => None,
        }
    }

    /// Borrow the contained punctuation, if any.
    pub fn as_punctuation(&self) -> Option<&Punctuation> {
        match self {
            Self::Punctuation(p) => Some(p),
            Self::Record(..) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{Schema, Value, synthetic_document_context};

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
        assert!(!ev.is_punctuation());
        let (r, rn) = ev.as_record().unwrap();
        assert_eq!(rn, 42);
        assert_eq!(r.values()[0], Value::Integer(1));
    }

    #[test]
    fn punctuation_event_carries_doc_id_and_kind() {
        let ctx = synthetic_document_context();
        let ev = StreamEvent::punctuation(Punctuation::document_open(Arc::clone(&ctx)));
        assert!(ev.is_punctuation());
        assert!(!ev.is_record());
        let p = ev.as_punctuation().unwrap();
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
}
