//! Document-level envelope context shared per source file.
//!
//! A [`DocumentContext`] is built once per file (per document) by the
//! source reader's envelope pre-scan, then attached as `Arc<DocumentContext>`
//! to every body record emitted from that document. CXL `$doc.<section>.<field>`
//! expressions resolve against the sections map held on this struct.
//!
//! Section names are arbitrary user-chosen identifiers declared in the
//! source's envelope config (no reserved names — `Head`, `Foot`,
//! `batch_metadata`, `preamble`, `eob_summary` are all equally valid).
//! The pre-scan extracts every declared section before any body record
//! streams, so all `$doc.*` values are available on every body record
//! throughout the body stream.

use crate::value::Value;
use indexmap::IndexMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// Opaque identity for a single document instance within a pipeline run.
///
/// Monotonic per-process. Sources allocate via [`DocumentId::next`] when
/// constructing a new context per file. Used by `Merge` punctuation dedup
/// (keys a per-document `HashMap` / `HashSet`) to fold a document's
/// boundary down to one downstream emission.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DocumentId(u64);

impl DocumentId {
    /// Allocate the next document id from a process-wide monotonic counter.
    /// Cheap: one atomic increment.
    pub fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Sentinel id reserved for the synthetic context returned by
    /// [`synthetic_document_context`]. Records produced in-pipeline
    /// (Transform synthesis, test fixtures) all share this id.
    pub const SYNTHETIC: Self = Self(0);
}

/// Envelope context for one document.
///
/// One per source file. The `sections` map holds every envelope section
/// declared in the source's YAML envelope config, populated upfront by the
/// reader's pre-scan pass; each section's payload is a [`Value::Map`] of
/// field name → typed value. CXL `$doc.<section>.<field>` resolves by
/// looking up `<section>` in this map, then `<field>` in the inner map.
///
/// Cloned per record as `Arc<DocumentContext>` — refcount bump only,
/// no data duplication.
#[derive(Debug)]
pub struct DocumentContext {
    id: DocumentId,
    source_file: Arc<str>,
    sections: IndexMap<Box<str>, Value>,
}

impl DocumentContext {
    /// Build a populated document context for a single source file.
    /// Called once per file by the source ingest path after the reader's
    /// envelope pre-scan returns the section map.
    pub fn new(id: DocumentId, source_file: Arc<str>, sections: IndexMap<Box<str>, Value>) -> Self {
        Self {
            id,
            source_file,
            sections,
        }
    }

    /// Opaque identity for dedup and per-document bucketing.
    pub fn id(&self) -> DocumentId {
        self.id
    }

    /// Originating source file path. Equal to `$source.file` for records
    /// from this document; the source ingest path compares it by
    /// `Arc::ptr_eq` to detect file transitions in a multi-file source.
    pub fn source_file(&self) -> &Arc<str> {
        &self.source_file
    }

    /// Resolve `$doc.<section>.<field>` by chained lookup. Returns
    /// `None` if either the section is undeclared on this document or
    /// the field is missing from the section's payload. CXL eval maps
    /// `None` to [`Value::Null`] per the streaming-resolver convention.
    pub fn get_section_field(&self, section: &str, field: &str) -> Option<Value> {
        match self.sections.get(section)? {
            Value::Map(m) => m.get(field).cloned(),
            // A section declared but not a Map (e.g., scalar / array
            // payload) is structurally invalid for field access; the
            // envelope config schema is fields:-shaped, so reaching here
            // means the reader emitted a non-Map payload.
            _ => None,
        }
    }
}

fn synthetic_storage() -> &'static Arc<DocumentContext> {
    static SYNTHETIC: OnceLock<Arc<DocumentContext>> = OnceLock::new();
    SYNTHETIC.get_or_init(|| {
        Arc::new(DocumentContext {
            id: DocumentId::SYNTHETIC,
            source_file: Arc::from(""),
            sections: IndexMap::new(),
        })
    })
}

/// Owned `Arc` clone of the process-wide synthetic [`DocumentContext`].
///
/// Used for every record that isn't tied to a real source file:
/// Transform-synthesized records, test fixtures, and any internal-path
/// record assembly that pre-dates the envelope system. Single
/// allocation backing the singleton; this function bumps the refcount.
/// The sections map is empty, so `$doc.<section>.<field>` against this
/// context always returns `None` (callers map to `Value::Null`).
pub fn synthetic_document_context() -> Arc<DocumentContext> {
    synthetic_storage().clone()
}

/// `'static` reference to the synthetic context's [`Arc`], for sites
/// that need a `&Arc<DocumentContext>` borrow (e.g. constructing an
/// `EvalContext<'a>` in a record-free path) without producing a
/// short-lived owned clone. No refcount bump.
pub fn synthetic_document_context_ref() -> &'static Arc<DocumentContext> {
    synthetic_storage()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_section(fields: &[(&str, Value)]) -> Value {
        let mut m = IndexMap::new();
        for (k, v) in fields {
            m.insert(Box::from(*k), v.clone());
        }
        Value::Map(Box::new(m))
    }

    #[test]
    fn document_id_next_is_monotonic_and_distinct() {
        let a = DocumentId::next();
        let b = DocumentId::next();
        assert_ne!(a, b);
        assert_ne!(a, DocumentId::SYNTHETIC);
    }

    #[test]
    fn synthetic_context_is_shared() {
        let a = synthetic_document_context();
        let b = synthetic_document_context();
        assert!(Arc::ptr_eq(&a, &b));
        assert_eq!(a.id(), DocumentId::SYNTHETIC);
        // Synthetic context carries no sections — every $doc.* misses.
        assert!(a.get_section_field("anything", "x").is_none());
    }

    #[test]
    fn synthetic_ref_matches_owned_singleton() {
        let owned = synthetic_document_context();
        assert!(Arc::ptr_eq(synthetic_document_context_ref(), &owned));
    }

    #[test]
    fn get_section_field_resolves_map_payload() {
        let mut sections = IndexMap::new();
        sections.insert(
            Box::from("Head"),
            make_section(&[
                ("batch_id", Value::String("RUN-001".into())),
                ("run_date", Value::String("2026-05-22".into())),
            ]),
        );
        sections.insert(
            Box::from("Foot"),
            make_section(&[("record_count", Value::Integer(42))]),
        );
        let ctx = DocumentContext::new(DocumentId::next(), Arc::from("payments.xml"), sections);

        assert_eq!(
            ctx.get_section_field("Head", "batch_id"),
            Some(Value::String("RUN-001".into()))
        );
        assert_eq!(
            ctx.get_section_field("Foot", "record_count"),
            Some(Value::Integer(42))
        );
        // Unknown section.
        assert!(ctx.get_section_field("Middle", "x").is_none());
        // Known section, unknown field.
        assert!(ctx.get_section_field("Head", "missing").is_none());
    }

    #[test]
    fn source_file_is_borrowable_for_ptr_eq() {
        let file: Arc<str> = Arc::from("doc.xml");
        let ctx = DocumentContext::new(DocumentId::next(), Arc::clone(&file), IndexMap::new());
        assert!(Arc::ptr_eq(ctx.source_file(), &file));
    }

    #[test]
    fn document_context_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DocumentContext>();
        assert_send_sync::<Arc<DocumentContext>>();
    }
}
