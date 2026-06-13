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
use serde::de::{self, SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

/// Opaque identity for a single document instance within a pipeline run.
///
/// Monotonic per-process. Sources allocate via [`DocumentId::next`] when
/// constructing a new context per file. Used by `Merge` punctuation dedup
/// (keys a per-document `HashMap` / `HashSet`) to fold a document's
/// boundary down to one downstream emission, and by the Aggregate's
/// per-document group flush to walk its open documents in a stable order.
/// `Ord` follows allocation order: a document opened earlier sorts first.
///
/// The `Serialize`/`Deserialize` impls are transparent over the inner
/// `u64`, so a spilled record's `doc_id` keys the per-file interning
/// table by the original numeric identity — never re-minted on read.
/// [`DocumentId::SYNTHETIC`] (`0`) round-trips as `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
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

    /// Open a nested envelope level beneath this one, flattening the
    /// ancestry into a single sibling sections map.
    ///
    /// Multi-level envelope formats (EDI X12 ISA → GS → ST) nest
    /// envelopes inside a file. Rather than chain `DocumentContext`s,
    /// each inner level mints a fresh context that carries every section
    /// the enclosing levels declared *plus* its own — all siblings in one
    /// map. A record streamed inside the ST level therefore resolves the
    /// ISA's `$doc.interchange.*`, the GS's `$doc.group.*`, and its own
    /// `$doc.transaction.*` through the same two-level lookup, with no CXL
    /// syntax change. Per-level section *names* keep the levels distinct;
    /// a child section name that collides with an ancestor's shadows the
    /// ancestor (innermost wins), matching the lexical-scope intuition.
    ///
    /// The new level gets a distinct [`DocumentId`] (so Merge dedup and
    /// any per-document operator treat each level as its own document
    /// frame) and inherits the parent's `source_file` (every level of one
    /// file shares the file identity). Sections are cloned by `Arc`/`Value`
    /// — envelope payloads are small (a few fields per level), so the copy
    /// is O(declared sections), not O(body).
    pub fn child(&self, id: DocumentId, sections: IndexMap<Box<str>, Value>) -> Self {
        let mut merged = self.sections.clone();
        for (name, payload) in sections {
            merged.insert(name, payload);
        }
        Self {
            id,
            source_file: Arc::clone(&self.source_file),
            sections: merged,
        }
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

// ── Spill codec ─────────────────────────────────────────────────────────────
//
// A `DocumentContext` is interned once per spill file (one frame per distinct
// `DocumentId`), so its codec is hand-written rather than derived for two
// reasons: `IndexMap` has no insertion-order-preserving serde impl, and the
// `Arc<str>` source file must collapse to its bytes on the wire and rebuild as
// a fresh `Arc<str>` on read (the in-memory `Arc` identity does not — and need
// not — survive the spill boundary; see the module-level note on per-document
// re-hydration). The shape is a 3-tuple mirroring `Value::Map`'s pair-vec
// encoding so section insertion order is preserved exactly:
//   (DocumentId, source_file: &str, sections: Vec<(&str, &Value)>)
// On read each `(String, Value)` pair re-inserts in order into a fresh
// `IndexMap`, and the source file rebuilds via `Arc::from`.

impl Serialize for DocumentContext {
    /// Serializes this context as `(id, source_file, ordered section pairs)`.
    ///
    /// Used only by the spill interning path, which writes one context frame
    /// per distinct document per file (`O(distinct documents)`, never
    /// `O(records)`). Section order is preserved by emitting the `IndexMap`
    /// as an ordered pair vector, matching the `Value::Map` codec.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let pairs: Vec<(&str, &Value)> =
            self.sections.iter().map(|(k, v)| (k.as_ref(), v)).collect();
        let mut tup = serializer.serialize_tuple(3)?;
        tup.serialize_element(&self.id)?;
        tup.serialize_element(self.source_file.as_ref())?;
        tup.serialize_element(&pairs)?;
        tup.end()
    }
}

impl<'de> Deserialize<'de> for DocumentContext {
    /// Reconstructs a context from `(id, source_file, ordered section pairs)`.
    ///
    /// Rebuilds the `source_file` as a fresh `Arc<str>` and re-inserts the
    /// section pairs into a new `IndexMap` in wire order, so a document's
    /// section ordering survives a spill round-trip. The rebuilt `Arc<str>`
    /// is a distinct allocation from the live ingest stream's — equality is
    /// by content, not pointer identity.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ContextVisitor;

        impl<'de> Visitor<'de> for ContextVisitor {
            type Value = DocumentContext;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a (DocumentId, source_file, sections) tuple")
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<DocumentContext, A::Error> {
                let id: DocumentId = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let source_file: std::string::String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let pairs: Vec<(std::string::String, Value)> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let mut sections = IndexMap::with_capacity(pairs.len());
                for (k, v) in pairs {
                    sections.insert(k.into_boxed_str(), v);
                }
                Ok(DocumentContext {
                    id,
                    source_file: Arc::from(source_file.as_str()),
                    sections,
                })
            }
        }

        deserializer.deserialize_tuple(3, ContextVisitor)
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
    fn child_layers_sibling_sections_and_inherits_file() {
        let mut isa = IndexMap::new();
        isa.insert(
            Box::from("interchange"),
            make_section(&[("control_ref", Value::String("000000001".into()))]),
        );
        let file: Arc<str> = Arc::from("claim.x12");
        let parent = DocumentContext::new(DocumentId::next(), Arc::clone(&file), isa);

        let mut gs = IndexMap::new();
        gs.insert(
            Box::from("group"),
            make_section(&[("functional_id", Value::String("HC".into()))]),
        );
        let group_id = DocumentId::next();
        let child = parent.child(group_id, gs);

        // Distinct identity per level, shared file.
        assert_ne!(child.id(), parent.id());
        assert_eq!(child.id(), group_id);
        assert!(Arc::ptr_eq(child.source_file(), &file));

        // The child resolves BOTH its own section and the inherited
        // ancestor section through the same two-level lookup — the
        // flattened-sibling representation that lets nested levels read
        // via distinct names with no $doc syntax change.
        assert_eq!(
            child.get_section_field("interchange", "control_ref"),
            Some(Value::String("000000001".into()))
        );
        assert_eq!(
            child.get_section_field("group", "functional_id"),
            Some(Value::String("HC".into()))
        );
        // The parent never gained the child's section.
        assert!(parent.get_section_field("group", "functional_id").is_none());
    }

    #[test]
    fn child_section_name_collision_shadows_ancestor() {
        let mut outer = IndexMap::new();
        outer.insert(
            Box::from("meta"),
            make_section(&[("level", Value::String("interchange".into()))]),
        );
        let parent = DocumentContext::new(DocumentId::next(), Arc::from("f.x12"), outer);

        let mut inner = IndexMap::new();
        inner.insert(
            Box::from("meta"),
            make_section(&[("level", Value::String("transaction".into()))]),
        );
        let child = parent.child(DocumentId::next(), inner);

        // Innermost wins on a name collision — lexical-scope intuition.
        assert_eq!(
            child.get_section_field("meta", "level"),
            Some(Value::String("transaction".into()))
        );
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

    #[test]
    fn document_id_serde_is_transparent_over_u64() {
        // The id is the interning-table key on a spill round-trip; it must
        // encode as the bare inner u64 so a record's doc_id reloads to the
        // same numeric identity it spilled with.
        let id = DocumentId::next();
        let bytes = postcard::to_stdvec(&id).unwrap();
        let raw = postcard::to_stdvec(&id.0).unwrap();
        assert_eq!(bytes, raw, "DocumentId must encode identically to its u64");
        let back: DocumentId = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(back, id);

        // SYNTHETIC (0) round-trips as 0, never re-minted.
        let syn = postcard::to_stdvec(&DocumentId::SYNTHETIC).unwrap();
        let syn_back: DocumentId = postcard::from_bytes(&syn).unwrap();
        assert_eq!(syn_back, DocumentId::SYNTHETIC);
    }

    #[test]
    fn document_context_serde_preserves_id_file_and_section_order() {
        // Sections inserted z, a, m — insertion order must survive the wire,
        // mirroring the Value::Map pair-vec codec. A nested Value::Map inside
        // a section exercises the recursive Value path through the frame.
        let mut sections = IndexMap::new();
        sections.insert(
            Box::from("z_section"),
            make_section(&[("k", Value::String("zv".into()))]),
        );
        sections.insert(
            Box::from("a_section"),
            make_section(&[
                ("count", Value::Integer(7)),
                (
                    "nested",
                    Value::Map(Box::new({
                        let mut inner = IndexMap::new();
                        inner.insert(Box::from("deep"), Value::Bool(true));
                        inner
                    })),
                ),
            ]),
        );
        sections.insert(Box::from("m_section"), make_section(&[]));
        let id = DocumentId::next();
        let ctx = DocumentContext::new(id, Arc::from("payments/run-001.xml"), sections);

        let bytes = postcard::to_stdvec(&ctx).unwrap();
        let back: DocumentContext = postcard::from_bytes(&bytes).unwrap();

        assert_eq!(back.id(), id);
        assert_eq!(back.source_file().as_ref(), "payments/run-001.xml");
        // Section names survive in insertion order, not sorted order.
        let names: Vec<&str> = back.sections.keys().map(|k| k.as_ref()).collect();
        assert_eq!(names, vec!["z_section", "a_section", "m_section"]);
        // Field values, including a nested Value::Map, survive intact.
        assert_eq!(
            back.get_section_field("z_section", "k"),
            Some(Value::String("zv".into()))
        );
        assert_eq!(
            back.get_section_field("a_section", "count"),
            Some(Value::Integer(7))
        );
        assert_eq!(
            back.get_section_field("a_section", "nested"),
            Some(Value::Map(Box::new({
                let mut inner = IndexMap::new();
                inner.insert(Box::from("deep"), Value::Bool(true));
                inner
            })))
        );
    }
}
