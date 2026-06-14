//! Document-level envelope context shared per source file.
//!
//! A [`DocumentContext`] is built once per file (per document) by the
//! source reader's envelope pre-scan, then attached as `Arc<DocumentContext>`
//! to every body record emitted from that document. CXL `$doc.<section>.<field>`
//! expressions resolve against the [`EnvelopeRecord`] held on this struct.
//!
//! The envelope is modeled with the same [`Schema`] + [`Value`] machinery as a
//! body record: one column per declared section, each value the section's
//! `Value::Map` payload. One [`Arc<Schema>`] backs the envelope, so the ambient
//! `$doc` view (this module) and any node-input view a downstream consolidation
//! node takes borrow the SAME record rather than a re-encoding.
//!
//! Section names are arbitrary user-chosen identifiers declared in the
//! source's envelope config (no reserved names — `Head`, `Foot`,
//! `batch_metadata`, `preamble`, `eob_summary` are all equally valid).
//! The pre-scan extracts every declared section before any body record
//! streams, so all `$doc.*` values are available on every body record
//! throughout the body stream.

use crate::schema::Schema;
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

/// Identity of the per-outermost-logical-document **frame** a record belongs
/// to — the grain at which the engine reconstructs an output envelope.
///
/// A frame is NOT always the innermost or the outermost envelope level: it is
/// whichever level the source format treats as one logical document.
///
/// - X12 (ISA → GS → ST): the frame is the **interchange** (the file-level
///   `ISA`). The `GS` functional group and `ST` transaction set
///   [`DocumentContext::child`] levels INHERIT the interchange grain, so a
///   whole interchange frames as one document.
/// - HL7 (FHS → BHS → MSH): the frame is the **message**. Each `MSH` opens a
///   fresh grain via [`DocumentContext::child_frame`], so a multi-message file
///   frames once per message.
/// - EDIFACT, CSV, JSON, XML, fixed-width: the file is the only level, so the
///   frame is the file (one grain per file).
///
/// `Copy`/`Eq`/`Hash` so the envelope-writer boundary detector can key on it
/// directly. Two contexts of the same frame (e.g. the `GS` and `ST` of one X12
/// interchange) compare equal even though their own [`DocumentId`]s differ.
///
/// One grain is one output document: the Envelope node's consolidation
/// strategies group a materialized body by grain to derive its set of headers
/// (the first record of each grain carries that document's header), so the
/// grain is the join key between "a record" and "the document it frames into".
///
/// This is distinct from the document-DLQ's keying: the DLQ keys on
/// `source_file` (the file grain), because a structural-count failure condemns
/// a whole file. The two never co-execute — `reconstruct_envelope` and
/// `dlq_granularity: document` are mutually exclusive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct DocumentGrain(DocumentId);

impl DocumentGrain {
    /// The grain shared by every record produced outside a real document
    /// (Transform synthesis, fan-in merge rows, test fixtures) — the grain of
    /// the [`DocumentId::SYNTHETIC`] context. Such records belong to no frame,
    /// so callers treat this grain as "unframed".
    pub const SYNTHETIC: Self = Self(DocumentId::SYNTHETIC);

    /// The inner [`DocumentId`] this grain is identified by — the id of the
    /// framing level (the X12 interchange, the HL7 message, or the file).
    pub fn document_id(self) -> DocumentId {
        self.0
    }
}

/// A document's envelope, modeled with the same [`Schema`] + [`Value`]
/// machinery as a body record.
///
/// One column per declared section (insertion-ordered, ancestor-flattened);
/// each section payload is an opaque [`Value::Map`], so any engine-internal
/// keys a reader injects for lossless writer round-trip (X12 / EDIFACT / HL7
/// `$raw`, the SWIFT synthetic `body`) ride along untouched. The schema column
/// list is the section *name* list — a `Schema` column carries only a name and
/// optional engine-stamp metadata, never a per-column value type, so the
/// section payload's `Value::Map` shape is implicit in the value, not the
/// schema.
///
/// The whole record is held behind ONE [`Arc<Schema>`]: the ambient `$doc`
/// resolver reads it through [`DocumentContext::get_section_field`], and a
/// downstream consolidation node borrows the same record (via a crate-internal
/// accessor that goes public with the Envelope node) — neither re-encodes it.
///
/// Two equality relations apply, for two different questions. [`PartialEq`] is
/// literal/structural — engine keys included — and backs the spill round-trip.
/// [`Self::same_header`] is semantic header identity: it excludes the
/// `$`-prefixed engine keys so two headers that agree on every user-visible
/// field fold even when their preserved raw bytes differ (e.g. an X12 `ISA`
/// control number).
#[derive(Debug, Clone)]
pub struct EnvelopeRecord {
    schema: Arc<Schema>,
    /// Section payloads, positional and parallel to `schema.columns()`. Each
    /// is the section's `Value::Map` (or, for a malformed reader emission, some
    /// other `Value`, which `$doc` field access treats as a miss).
    sections: Vec<Value>,
}

impl EnvelopeRecord {
    /// Build an envelope record from the reader/driver's ordered section map.
    ///
    /// The schema columns are the section names in insertion order; the values
    /// are the section payloads positionally. Keeps reader/driver population a
    /// one-line wrap over the `IndexMap` the pre-scan already produces.
    pub fn from_sections(sections: IndexMap<Box<str>, Value>) -> Self {
        let mut columns: Vec<Box<str>> = Vec::with_capacity(sections.len());
        let mut values: Vec<Value> = Vec::with_capacity(sections.len());
        for (name, payload) in sections {
            columns.push(name);
            values.push(payload);
        }
        Self {
            schema: Arc::new(Schema::new(columns)),
            sections: values,
        }
    }

    /// The empty envelope: no sections, so every `$doc.*` resolves `None`.
    /// Used for synthetic / zero-body contexts.
    pub fn empty() -> Self {
        Self {
            schema: Arc::new(Schema::new(Vec::new())),
            sections: Vec::new(),
        }
    }

    /// `true` when this envelope declares no sections — the headerless
    /// envelope an empty-body document or a synthetic context carries.
    /// Concat treats a headerless document as carrying no common header,
    /// so empties coexist with a single real header without conflicting.
    pub fn is_empty(&self) -> bool {
        self.sections.is_empty()
    }

    /// The payload `Value` for a named section, or `None` when the section is
    /// undeclared on this envelope.
    fn section_value(&self, section: &str) -> Option<&Value> {
        let idx = self.schema.index(section)?;
        self.sections.get(idx)
    }

    /// Resolve a section's field by chained lookup: section payload `Value::Map`
    /// then `field`. Returns `None` when the section is undeclared, the payload
    /// is not a map, or the field is absent. The value is cloned owned.
    fn get_field(&self, section: &str, field: &str) -> Option<Value> {
        match self.section_value(section)? {
            Value::Map(m) => m.get(field).cloned(),
            _ => None,
        }
    }

    /// Borrow a section's inner field map in declared order, or `None` when the
    /// section is undeclared or its payload is not a [`Value::Map`].
    fn field_map(&self, section: &str) -> Option<&IndexMap<Box<str>, Value>> {
        match self.section_value(section)? {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Two envelopes name the same sections (positionally) and every section's
    /// payload agrees on all *user-visible* fields — the semantic
    /// header-identity relation, as distinct from the literal structural
    /// [`PartialEq`].
    ///
    /// Why this differs from `PartialEq`: readers stash engine-internal keys
    /// alongside the user-declared fields inside a section payload so a writer
    /// can reconstruct the original bytes losslessly. The X12 / EDIFACT / HL7
    /// readers carry a `$raw` element list under each header section, and that
    /// list embeds interchange control numbers and timestamps that legitimately
    /// vary between two files from the same trading partner. Under `PartialEq`
    /// those two headers compare distinct — the bytes differ — so concat's
    /// consolidation guard would reject them as a multi-header conflict even
    /// though every addressable header field matches. `same_header` excludes
    /// those engine keys so the two fold to one header.
    ///
    /// Exclusion rule: a `$`-prefixed key in a section payload is engine-
    /// injected (the `$`-sigil system-namespace convention, matching CXL's
    /// `$pipeline.*` / `$doc.*` and the readers' `$raw`) and is ignored on both
    /// sides. User-declared section fields never start with `$`, so the rule
    /// excludes exactly the reader-internal shadow and nothing a user authored.
    ///
    /// Section names are still compared positionally (the same sections in a
    /// different declared order are distinct headers) and section fields by key
    /// (field order within a section does not matter), exactly as `PartialEq`
    /// does — `same_header` differs from `PartialEq` only in dropping the
    /// `$`-prefixed keys before comparing each section's payload.
    ///
    /// When two headers are `same_header`-equal but differ in an excluded key,
    /// the caller keeps ONE of the two full [`EnvelopeRecord`]s (engine keys
    /// included) as the consolidated document's representative — concat keeps
    /// the first document's, so the consolidated output reconstructs from the
    /// first document's `$raw`.
    pub fn same_header(&self, other: &EnvelopeRecord) -> bool {
        if self.schema.columns() != other.schema.columns() {
            return false;
        }
        self.sections
            .iter()
            .zip(other.sections.iter())
            .all(|(a, b)| Self::section_payload_eq(a, b))
    }

    /// Compare two section payloads ignoring engine-injected `$`-prefixed keys.
    ///
    /// Two `Value::Map` payloads agree when their non-`$` top-level entries are
    /// equal as a key→value set: the same set of non-`$` keys, each mapping to
    /// an equal [`Value`]. The comparison is by keyed lookup, so it is
    /// order-independent and allocation-free — no clone or sort. Only top-level
    /// keys are filtered; a `$`-prefixed key nested inside a field value is
    /// compared structurally, which is sound because readers inject their
    /// engine shadow (`$raw`) at the section-payload root, never nested. A
    /// non-map payload (a malformed reader emission) is compared by strict
    /// equality — there are no keys to filter.
    fn section_payload_eq(a: &Value, b: &Value) -> bool {
        match (a, b) {
            (Value::Map(am), Value::Map(bm)) => {
                let user_key_count = |m: &IndexMap<Box<str>, Value>| {
                    m.keys().filter(|k| !k.starts_with('$')).count()
                };
                if user_key_count(am) != user_key_count(bm) {
                    return false;
                }
                am.iter()
                    .filter(|(k, _)| !k.starts_with('$'))
                    .all(|(k, v)| bm.get(&**k) == Some(v))
            }
            (a, b) => a == b,
        }
    }

    /// Build a child envelope: this record's columns unioned with `child`'s,
    /// `child` shadowing on a same-name collision (innermost-wins). A colliding
    /// section keeps the ancestor's column position but takes the child's
    /// payload; a fresh child section appends after the ancestors — matching the
    /// `IndexMap::insert` order the flattened side-table used.
    fn merged_with(&self, child: EnvelopeRecord) -> EnvelopeRecord {
        let mut merged: IndexMap<Box<str>, Value> =
            IndexMap::with_capacity(self.sections.len() + child.sections.len());
        for (name, payload) in self.schema.columns().iter().zip(self.sections.iter()) {
            merged.insert(name.clone(), payload.clone());
        }
        for (name, payload) in child
            .schema
            .columns()
            .iter()
            .zip(child.sections.into_iter())
        {
            merged.insert(name.clone(), payload);
        }
        EnvelopeRecord::from_sections(merged)
    }
}

/// Literal, structural equality: two envelopes are equal when they declare the
/// same section names in the same order and each section's payload compares
/// byte-for-byte equal — engine-injected keys included.
///
/// The section-name list (`schema.columns()`, a `[Box<str>]`) is compared
/// positionally, so the same sections in a different declared order are
/// *distinct* headers. Each section's payload is a [`Value::Map`], whose
/// equality is by key — field order within a section does not matter — but the
/// payload includes any engine-injected keys (`$raw`, the SWIFT synthetic
/// `body`) the readers carry, so two headers that agree on every user-visible
/// field but differ in preserved raw bytes compare distinct.
///
/// This is deliberately strict — it includes the engine-injected keys, so a
/// spill round-trip that altered the preserved `$raw` bytes would be observable
/// under whole-record equality. The strict-vs-semantic distinction is pinned by
/// the `same_header` unit tests, which assert a record pair that `same_header`
/// folds is still `!=` here. For the *semantic* "do these name the same header"
/// question — which must ignore the engine keys so two files differing only in
/// a control number fold — use [`EnvelopeRecord::same_header`] instead.
///
/// Hand-written rather than derived because [`Schema`] has no `PartialEq`.
impl PartialEq for EnvelopeRecord {
    fn eq(&self, other: &Self) -> bool {
        self.schema.columns() == other.schema.columns() && self.sections == other.sections
    }
}

/// Envelope context for one document.
///
/// One per source file. The [`EnvelopeRecord`] holds every envelope section
/// declared in the source's YAML envelope config, populated upfront by the
/// reader's pre-scan pass; each section's payload is a [`Value::Map`] of
/// field name → typed value. CXL `$doc.<section>.<field>` resolves by
/// looking up `<section>` in the record, then `<field>` in the inner map.
///
/// Cloned per record as `Arc<DocumentContext>` — refcount bump only,
/// no data duplication.
#[derive(Debug)]
pub struct DocumentContext {
    id: DocumentId,
    /// The frame this context belongs to. A file-level context's grain is its
    /// own [`DocumentId`]; a [`Self::child`] level inherits its parent's
    /// grain; a [`Self::child_frame`] level mints a new grain from its own id.
    /// See [`DocumentGrain`] for the per-format mapping.
    grain: DocumentGrain,
    source_file: Arc<str>,
    /// The envelope as a schema-described record: one column per declared
    /// section (insertion-ordered, ancestor-flattened); each value is the
    /// section's `Value::Map` payload, carrying any engine-internal keys
    /// (`$raw`, `body`) the readers inject. ONE `Arc<Schema>` — the ambient
    /// `$doc` view and any consolidation-input view borrow THIS, never a
    /// re-encoding.
    envelope: EnvelopeRecord,
}

impl DocumentContext {
    /// Build a populated document context for a single source file.
    /// Called once per file by the source ingest path after the reader's
    /// envelope pre-scan returns the section map (wrapped in an
    /// [`EnvelopeRecord`] at the driver seam).
    ///
    /// The file-level document is its own frame: its [`grain`](Self::grain)
    /// is derived from `id`. Nested levels inherit or re-mint this grain via
    /// [`Self::child`] / [`Self::child_frame`].
    pub fn new(id: DocumentId, source_file: Arc<str>, envelope: EnvelopeRecord) -> Self {
        Self {
            id,
            grain: DocumentGrain(id),
            source_file,
            envelope,
        }
    }

    /// Opaque identity for dedup and per-document bucketing.
    pub fn id(&self) -> DocumentId {
        self.id
    }

    /// The frame this record belongs to — the grain at which output-envelope
    /// reconstruction operates (the document-DLQ keys on `source_file`
    /// instead; see [`DocumentGrain`]). Equal across every level of one frame
    /// (e.g. an X12 interchange's `ISA`/`GS`/`ST` contexts all report the
    /// interchange grain); distinct per HL7 message.
    pub fn grain(&self) -> DocumentGrain {
        self.grain
    }

    /// Originating source file path. Equal to `$source.file` for records
    /// from this document; the source ingest path compares it by
    /// `Arc::ptr_eq` to detect file transitions in a multi-file source.
    pub fn source_file(&self) -> &Arc<str> {
        &self.source_file
    }

    /// The document's envelope as a schema-described record — the node-input
    /// view a downstream consolidation node reduces over.
    ///
    /// The same single [`Arc<Schema>`] backs both this borrow and the ambient
    /// `$doc` resolution path ([`Self::get_section_field`] reads through it),
    /// so there is exactly one in-memory encoding of the envelope per document.
    ///
    /// The cross-crate consumer is the Envelope node's `concat` strategy: it
    /// reads each incoming document's envelope to fold a body's per-document
    /// headers down to one consolidated header (or reject a clash).
    pub fn envelope_record(&self) -> &EnvelopeRecord {
        &self.envelope
    }

    /// Open a nested envelope level beneath this one, flattening the
    /// ancestry into a single sibling envelope record.
    ///
    /// Multi-level envelope formats (EDI X12 ISA → GS → ST) nest
    /// envelopes inside a file. Rather than chain `DocumentContext`s,
    /// each inner level mints a fresh context that carries every section
    /// the enclosing levels declared *plus* its own — all siblings in one
    /// record. A record streamed inside the ST level therefore resolves the
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
    pub fn child(&self, id: DocumentId, sections: EnvelopeRecord) -> Self {
        self.child_inner(id, sections, self.grain)
    }

    /// Open a nested level that begins a NEW document frame.
    ///
    /// Identical to [`Self::child`] in section flattening and `source_file`
    /// inheritance, but the new level's [`grain`](Self::grain) is minted from
    /// its own `id` rather than inherited from the parent. Used for formats
    /// whose framing document is a nested level repeated within one file:
    /// each HL7 `MSH` message opens a fresh frame, so a multi-message file
    /// reconstructs one output envelope per message, while still resolving the
    /// enclosing `FHS`/`BHS` `$doc.*` sections through the flattened record.
    pub fn child_frame(&self, id: DocumentId, sections: EnvelopeRecord) -> Self {
        self.child_inner(id, sections, DocumentGrain(id))
    }

    fn child_inner(&self, id: DocumentId, sections: EnvelopeRecord, grain: DocumentGrain) -> Self {
        Self {
            id,
            grain,
            source_file: Arc::clone(&self.source_file),
            envelope: self.envelope.merged_with(sections),
        }
    }

    /// Resolve `$doc.<section>.<field>` by chained lookup. Returns
    /// `None` if either the section is undeclared on this document or
    /// the field is missing from the section's payload. CXL eval maps
    /// `None` to [`Value::Null`] per the streaming-resolver convention.
    ///
    /// Reads through [`Self::envelope_record`], so the ambient `$doc` view and
    /// the node-input view share one encoding.
    pub fn get_section_field(&self, section: &str, field: &str) -> Option<Value> {
        self.envelope_record().get_field(section, field)
    }

    /// Borrow a whole section's ordered field map by name, for an output
    /// writer reconstructing a per-document header/footer that echoes every
    /// field of a `$doc` section in declared order.
    ///
    /// Returns `None` when the section is undeclared on this document or its
    /// payload is not a [`Value::Map`] (the envelope config schema is always
    /// `fields:`-shaped, so a non-Map payload means the reader emitted an
    /// unexpected shape). The borrow is O(1) — no field is cloned; the writer
    /// iterates the returned map in insertion order. Reads through
    /// [`Self::envelope_record`].
    pub fn section_fields(&self, section: &str) -> Option<&IndexMap<Box<str>, Value>> {
        self.envelope_record().field_map(section)
    }
}

// ── Spill codec ─────────────────────────────────────────────────────────────
//
// A `DocumentContext` is interned once per spill file (one frame per distinct
// `DocumentId`), so its codec is hand-written rather than derived for two
// reasons: the envelope's section order must be preserved on the wire (it has
// no derive that guarantees ordering), and the `Arc<str>` source file must
// collapse to its bytes on the wire and rebuild as a fresh `Arc<str>` on read
// (the in-memory `Arc` identity does not — and need not — survive the spill
// boundary; see the module-level note on per-document re-hydration). The shape
// is a 4-tuple mirroring `Value::Map`'s pair-vec encoding so section insertion
// order is preserved exactly:
//   (DocumentId, DocumentGrain, source_file: &str, sections: Vec<(&str, &Value)>)
// The `grain` rides the wire so a spilled record stays in the SAME envelope
// frame it carried before spilling — it is not re-derived from `id` on read,
// because a nested HL7 message frame's grain differs from its own id. On read
// each `(String, Value)` pair re-inserts in order into a fresh `EnvelopeRecord`,
// and the source file rebuilds via `Arc::from`.

impl Serialize for DocumentContext {
    /// Serializes this context as `(id, grain, source_file, ordered section pairs)`.
    ///
    /// Used only by the spill interning path, which writes one context frame
    /// per distinct document per file (`O(distinct documents)`, never
    /// `O(records)`). Section order is preserved by emitting the envelope's
    /// `(name, value)` columns as an ordered pair vector, matching the
    /// `Value::Map` codec.
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let pairs: Vec<(&str, &Value)> = self
            .envelope
            .schema
            .columns()
            .iter()
            .map(|c| c.as_ref())
            .zip(self.envelope.sections.iter())
            .collect();
        let mut tup = serializer.serialize_tuple(4)?;
        tup.serialize_element(&self.id)?;
        tup.serialize_element(&self.grain)?;
        tup.serialize_element(self.source_file.as_ref())?;
        tup.serialize_element(&pairs)?;
        tup.end()
    }
}

impl<'de> Deserialize<'de> for DocumentContext {
    /// Reconstructs a context from `(id, grain, source_file, ordered section pairs)`.
    ///
    /// Rebuilds the `source_file` as a fresh `Arc<str>` and re-inserts the
    /// section pairs into a new [`EnvelopeRecord`] in wire order, so a
    /// document's section ordering survives a spill round-trip. The rebuilt
    /// `Arc<str>` is a distinct allocation from the live ingest stream's —
    /// equality is by content, not pointer identity. The `grain` round-trips
    /// verbatim so the record stays governed by the frame it spilled with.
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct ContextVisitor;

        impl<'de> Visitor<'de> for ContextVisitor {
            type Value = DocumentContext;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(
                    f,
                    "a (DocumentId, DocumentGrain, source_file, sections) tuple"
                )
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<DocumentContext, A::Error> {
                let id: DocumentId = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let grain: DocumentGrain = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let source_file: std::string::String = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                let pairs: Vec<(std::string::String, Value)> = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(3, &self))?;
                let mut sections = IndexMap::with_capacity(pairs.len());
                for (k, v) in pairs {
                    sections.insert(k.into_boxed_str(), v);
                }
                Ok(DocumentContext {
                    id,
                    grain,
                    source_file: Arc::from(source_file.as_str()),
                    envelope: EnvelopeRecord::from_sections(sections),
                })
            }
        }

        deserializer.deserialize_tuple(4, ContextVisitor)
    }
}

fn synthetic_storage() -> &'static Arc<DocumentContext> {
    static SYNTHETIC: OnceLock<Arc<DocumentContext>> = OnceLock::new();
    SYNTHETIC.get_or_init(|| {
        Arc::new(DocumentContext {
            id: DocumentId::SYNTHETIC,
            grain: DocumentGrain::SYNTHETIC,
            source_file: Arc::from(""),
            envelope: EnvelopeRecord::empty(),
        })
    })
}

/// Owned `Arc` clone of the process-wide synthetic [`DocumentContext`].
///
/// Used for every record that isn't tied to a real source file:
/// Transform-synthesized records, test fixtures, and any internal-path
/// record assembly that pre-dates the envelope system. Single
/// allocation backing the singleton; this function bumps the refcount.
/// The envelope is empty, so `$doc.<section>.<field>` against this
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

    fn envelope(sections: IndexMap<Box<str>, Value>) -> EnvelopeRecord {
        EnvelopeRecord::from_sections(sections)
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
        let ctx = DocumentContext::new(
            DocumentId::next(),
            Arc::from("payments.xml"),
            envelope(sections),
        );

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
        let parent = DocumentContext::new(DocumentId::next(), Arc::clone(&file), envelope(isa));

        let mut gs = IndexMap::new();
        gs.insert(
            Box::from("group"),
            make_section(&[("functional_id", Value::String("HC".into()))]),
        );
        let group_id = DocumentId::next();
        let child = parent.child(group_id, envelope(gs));

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

        // X12 grain: a `child` level INHERITS the parent's frame grain, so a
        // GS/ST level reports the interchange (ISA file-level) grain — one
        // frame per interchange, not per transaction set.
        assert_eq!(child.grain(), parent.grain());
        assert_eq!(parent.grain().document_id(), parent.id());
    }

    #[test]
    fn file_level_grain_is_its_own_id() {
        // A file-level document (CSV/JSON/XML/fixed-width/EDIFACT and the X12
        // ISA / HL7 FHS) is its own frame: grain == id.
        let ctx = DocumentContext::new(
            DocumentId::next(),
            Arc::from("a.csv"),
            EnvelopeRecord::empty(),
        );
        assert_eq!(ctx.grain().document_id(), ctx.id());
    }

    #[test]
    fn child_frame_mints_a_fresh_grain_per_level() {
        // HL7 grain: each `MSH` message opens a fresh frame via `child_frame`,
        // so two messages of one file get DISTINCT grains (one envelope frame
        // per message) while still inheriting the file's `source_file` and any
        // enclosing `$doc.*` sections.
        let file: Arc<str> = Arc::from("messages.hl7");
        let mut fhs = IndexMap::new();
        fhs.insert(
            Box::from("file_header"),
            make_section(&[("sender", Value::String("LAB".into()))]),
        );
        let file_doc = DocumentContext::new(DocumentId::next(), Arc::clone(&file), envelope(fhs));

        let msg1 = file_doc.child_frame(DocumentId::next(), EnvelopeRecord::empty());
        let msg2 = file_doc.child_frame(DocumentId::next(), EnvelopeRecord::empty());

        // Each message is its own frame.
        assert_ne!(msg1.grain(), msg2.grain());
        assert_eq!(msg1.grain().document_id(), msg1.id());
        assert_eq!(msg2.grain().document_id(), msg2.id());
        // And neither shares the file-level frame.
        assert_ne!(msg1.grain(), file_doc.grain());
        // Both still resolve the enclosing FHS section and share the file.
        assert_eq!(
            msg1.get_section_field("file_header", "sender"),
            Some(Value::String("LAB".into()))
        );
        assert!(Arc::ptr_eq(msg1.source_file(), &file));
        assert!(Arc::ptr_eq(msg2.source_file(), &file));
    }

    #[test]
    fn grandchild_inherits_the_nearest_frame_grain() {
        // A `child_frame` (HL7 message) followed by an ordinary `child`
        // (a hypothetical sub-level) keeps the message's grain — `child`
        // always inherits its parent's grain, wherever that frame began.
        let file = DocumentContext::new(
            DocumentId::next(),
            Arc::from("f.hl7"),
            EnvelopeRecord::empty(),
        );
        let message = file.child_frame(DocumentId::next(), EnvelopeRecord::empty());
        let sub = message.child(DocumentId::next(), EnvelopeRecord::empty());
        assert_eq!(sub.grain(), message.grain());
        assert_ne!(sub.grain(), file.grain());
    }

    #[test]
    fn child_section_name_collision_shadows_ancestor() {
        let mut outer = IndexMap::new();
        outer.insert(
            Box::from("meta"),
            make_section(&[("level", Value::String("interchange".into()))]),
        );
        let parent = DocumentContext::new(DocumentId::next(), Arc::from("f.x12"), envelope(outer));

        let mut inner = IndexMap::new();
        inner.insert(
            Box::from("meta"),
            make_section(&[("level", Value::String("transaction".into()))]),
        );
        let child = parent.child(DocumentId::next(), envelope(inner));

        // Innermost wins on a name collision — lexical-scope intuition.
        assert_eq!(
            child.get_section_field("meta", "level"),
            Some(Value::String("transaction".into()))
        );
    }

    #[test]
    fn source_file_is_borrowable_for_ptr_eq() {
        let file: Arc<str> = Arc::from("doc.xml");
        let ctx = DocumentContext::new(
            DocumentId::next(),
            Arc::clone(&file),
            EnvelopeRecord::empty(),
        );
        assert!(Arc::ptr_eq(ctx.source_file(), &file));
    }

    #[test]
    fn document_context_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<DocumentContext>();
        assert_send_sync::<Arc<DocumentContext>>();
    }

    #[test]
    fn envelope_record_is_single_arc_backing_both_views() {
        // The same single `Arc<Schema>` backs the ambient `$doc` resolution
        // path and the node-input borrow — proving one encoding, not two.
        let mut sections = IndexMap::new();
        sections.insert(
            Box::from("Head"),
            make_section(&[("batch_id", Value::String("RUN-001".into()))]),
        );
        let ctx = Arc::new(DocumentContext::new(
            DocumentId::next(),
            Arc::from("payments.xml"),
            envelope(sections),
        ));

        // Cloning the per-record `Arc<DocumentContext>` (as the executor does)
        // shares the underlying envelope — the schema Arc is one allocation,
        // pointer-identical across the clone.
        let clone = Arc::clone(&ctx);
        assert!(Arc::ptr_eq(&ctx, &clone));
        assert!(Arc::ptr_eq(
            &ctx.envelope_record().schema,
            &clone.envelope_record().schema
        ));

        // The ambient `$doc` reader and the node-input view resolve the SAME
        // record: `get_section_field` reads through `envelope_record`, so a
        // value the ambient view resolves is the same value the node-input
        // borrow exposes positionally — no parallel encoding.
        assert_eq!(
            ctx.get_section_field("Head", "batch_id"),
            Some(Value::String("RUN-001".into()))
        );
        let node_view = ctx.envelope_record();
        let head_idx = node_view.schema.index("Head").expect("Head column");
        match &node_view.sections[head_idx] {
            Value::Map(m) => assert_eq!(
                m.get("batch_id"),
                Some(&Value::String("RUN-001".into())),
                "node-input view sees the same payload the ambient view resolves"
            ),
            other => panic!("expected Value::Map, got {other:?}"),
        }
    }

    fn section_with(fields: &[(&str, Value)]) -> Value {
        make_section(fields)
    }

    #[test]
    fn same_header_ignores_dollar_prefixed_engine_keys() {
        // Two X12-style headers identical in every user field but differing in
        // the engine-preserved `$raw` element list (one carries an interchange
        // control number, the other does not). `same_header` must fold them;
        // `PartialEq` must still split them — the spill round-trip depends on
        // the strict relation.
        let mut a = IndexMap::new();
        a.insert(
            Box::from("interchange"),
            section_with(&[
                ("sender", Value::String("ACME".into())),
                (
                    "$raw",
                    Value::Array(vec![Value::String("ISA*00*...*000000001".into())]),
                ),
            ]),
        );
        let mut b = IndexMap::new();
        b.insert(
            Box::from("interchange"),
            section_with(&[
                ("sender", Value::String("ACME".into())),
                (
                    "$raw",
                    Value::Array(vec![Value::String("ISA*00*...*000000002".into())]),
                ),
            ]),
        );
        let ea = envelope(a);
        let eb = envelope(b);

        assert!(
            ea.same_header(&eb),
            "headers agreeing on every user field fold despite differing $raw"
        );
        assert!(
            eb.same_header(&ea),
            "same_header is symmetric over the $raw difference"
        );
        assert_ne!(
            ea, eb,
            "PartialEq stays strict: the differing $raw makes the records structurally distinct"
        );
    }

    #[test]
    fn same_header_distinguishes_sections_in_different_declared_order() {
        // Same two sections, declared in a different order — positionally
        // distinct headers, so `same_header` is FALSE.
        let mut a = IndexMap::new();
        a.insert(Box::from("head"), section_with(&[("k", Value::Integer(1))]));
        a.insert(Box::from("foot"), section_with(&[("k", Value::Integer(2))]));
        let mut b = IndexMap::new();
        b.insert(Box::from("foot"), section_with(&[("k", Value::Integer(2))]));
        b.insert(Box::from("head"), section_with(&[("k", Value::Integer(1))]));

        assert!(
            !envelope(a).same_header(&envelope(b)),
            "the same sections in a different declared order are distinct headers"
        );
    }

    #[test]
    fn same_header_ignores_field_order_within_a_section() {
        // Same section with the SAME user fields in a different order →
        // `same_header` TRUE (field order within a section does not matter).
        let mut a = IndexMap::new();
        a.insert(
            Box::from("head"),
            section_with(&[("x", Value::Integer(1)), ("y", Value::String("v".into()))]),
        );
        let mut b = IndexMap::new();
        b.insert(
            Box::from("head"),
            section_with(&[("y", Value::String("v".into())), ("x", Value::Integer(1))]),
        );

        assert!(
            envelope(a).same_header(&envelope(b)),
            "field order within a section is irrelevant to header identity"
        );
    }

    #[test]
    fn same_header_distinguishes_differing_user_fields() {
        // A real user-field difference (not an engine key) must split headers,
        // proving the exclusion rule does not over-fold.
        let mut a = IndexMap::new();
        a.insert(
            Box::from("head"),
            section_with(&[("sender", Value::String("ACME".into()))]),
        );
        let mut b = IndexMap::new();
        b.insert(
            Box::from("head"),
            section_with(&[("sender", Value::String("OTHER".into()))]),
        );

        assert!(
            !envelope(a).same_header(&envelope(b)),
            "a differing user field makes two headers distinct"
        );
    }

    #[test]
    fn same_header_swift_body_field_is_compared_not_excluded() {
        // The SWIFT `body` key is NOT `$`-prefixed: it is the sole, user-
        // addressable field of a service-block section, so it is compared, not
        // excluded. Two SWIFT-style headers with different `body` content are
        // distinct — the exclusion rule covers only the `$raw` shadow.
        let mut a = IndexMap::new();
        a.insert(
            Box::from("basic_header"),
            section_with(&[("body", Value::String("F01BANKBEBB".into()))]),
        );
        let mut b = IndexMap::new();
        b.insert(
            Box::from("basic_header"),
            section_with(&[("body", Value::String("F01OTHERBANK".into()))]),
        );

        assert!(
            !envelope(a).same_header(&envelope(b)),
            "the user-addressable SWIFT `body` field is compared, not excluded"
        );
    }

    #[test]
    fn same_header_empty_envelopes_are_equal() {
        assert!(EnvelopeRecord::empty().same_header(&EnvelopeRecord::empty()));
        let mut a = IndexMap::new();
        a.insert(Box::from("head"), section_with(&[("k", Value::Integer(1))]));
        assert!(
            !envelope(a).same_header(&EnvelopeRecord::empty()),
            "a non-empty header is not the same header as the empty envelope"
        );
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
        // Model an HL7 message frame: build a file-level context, then a
        // `child_frame` whose grain differs from its own id, so the round-trip
        // proves the grain is carried verbatim (not re-derived from id).
        let file_doc = DocumentContext::new(
            id,
            Arc::from("payments/run-001.xml"),
            EnvelopeRecord::empty(),
        );
        let ctx = file_doc.child_frame(DocumentId::next(), envelope(sections));
        let expected_grain = ctx.grain();
        let expected_id = ctx.id();
        assert_ne!(
            expected_grain.document_id(),
            file_doc.id(),
            "a child_frame's grain is its own id, distinct from the parent's"
        );

        let bytes = postcard::to_stdvec(&ctx).unwrap();
        let back: DocumentContext = postcard::from_bytes(&bytes).unwrap();

        assert_eq!(back.id(), expected_id);
        assert_eq!(
            back.grain(),
            expected_grain,
            "grain round-trips verbatim, not re-derived from id"
        );
        assert_eq!(back.source_file().as_ref(), "payments/run-001.xml");
        // Section names survive in insertion order, not sorted order.
        let names: Vec<&str> = back
            .envelope_record()
            .schema
            .columns()
            .iter()
            .map(|c| c.as_ref())
            .collect();
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
