//! Envelope section configuration shared across format readers.
//!
//! An [`EnvelopeConfig`] describes the document-level sections a
//! reader should extract during the one-time pre-scan that precedes
//! body record streaming. Each section has a user-chosen name (no
//! engine-reserved labels), an extract rule that tells the reader how
//! to locate it in the source, and a typed field schema for CXL
//! `$doc.<section>.<field>` typechecking.
//!
//! Per-format readers consume the relevant `Extract` variant during
//! [`crate::traits::FormatReader::prepare_document`]; unsupported
//! variants surface as a format error so a config-wrong-for-format
//! mistake fails fast.

use clinker_record::{
    DEFAULT_DATE_FORMATS, DEFAULT_DATETIME_FORMATS, Value, coerce_to_bool, coerce_to_date,
    coerce_to_datetime, coerce_to_float, coerce_to_int, coerce_to_string,
};
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

/// A reader-driven envelope-nesting signal, queued by a multi-level
/// format reader and drained by the source ingest driver after each
/// `next_record`.
///
/// Single-level envelope formats (XML, JSON, EDIFACT) never produce
/// these — their whole document is one `prepare_document` pre-scan, so
/// the driver opens exactly one document per file. Multi-level formats
/// (EDI X12 ISA → GS → ST) cross envelope boundaries *mid-file*: the
/// reader emits `OpenLevel` when it enters a nested envelope and
/// `CloseLevel` when it leaves one. The driver maps each `OpenLevel` to a
/// child [`clinker_record::DocumentContext`] (carrying the level's
/// extracted sections, layered as siblings over the enclosing levels) and
/// a `DocumentOpen` punctuation, and each `CloseLevel` to the matching
/// `DocumentClose`.
///
/// Events are inline with the record stream, not an out-of-band channel:
/// the driver drains them in arrival order relative to records, so a
/// level opens before the records inside it and closes after them. A
/// trailing `OpenLevel`/`CloseLevel` pair with no records between (a
/// header-only inner envelope, or a header-only interchange) is a
/// legitimate, balanced shape the driver applies in full — it must not be
/// skipped at end-of-input.
#[derive(Debug, Clone)]
pub enum EnvelopeEvent {
    /// Enter a nested envelope level, carrying the sections the reader
    /// extracted for it (already coerced to their declared field types,
    /// keyed by user-declared section name). The driver opens a child
    /// document context whose sections layer over the enclosing levels.
    ///
    /// `frame` tells the driver whether this level begins a new
    /// output-envelope / document-DLQ frame (see [`FrameRole`]). Most nested
    /// levels are [`FrameRole::Inherit`] (an X12 `GS`/`ST` stays inside the
    /// interchange frame); a level the format treats as a self-contained
    /// document — each HL7 `MSH` message — is [`FrameRole::NewFrame`].
    OpenLevel {
        sections: IndexMap<Box<str>, Value>,
        frame: FrameRole,
    },
    /// Leave the innermost open nested envelope level. The driver closes
    /// the matching document context.
    CloseLevel,
}

/// Whether a nested envelope level begins a fresh per-document frame — the
/// grain at which the engine reconstructs an output envelope and dead-letters
/// a whole document.
///
/// The frame is whichever level the format treats as one logical document:
/// the X12 interchange (so its nested `GS`/`ST` levels `Inherit`), or each HL7
/// message (so each `MSH` is a `NewFrame`). The driver maps `Inherit` to
/// [`clinker_record::DocumentContext::child`] and `NewFrame` to
/// [`clinker_record::DocumentContext::child_frame`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameRole {
    /// Stay inside the enclosing frame — this level's records share the
    /// enclosing document's grain (X12 `GS`/`ST` under one `ISA`).
    Inherit,
    /// Begin a new frame — this level is itself a logical document (an HL7
    /// `MSH` message), so its records get a distinct grain.
    NewFrame,
}

/// Configuration for one source's envelope sections.
///
/// Section keys are user-chosen identifiers — the engine reserves
/// none. Examples that all parse the same way: `Head` / `Foot`,
/// `batch_metadata` / `eob_summary`, `preamble` / `trailer`.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EnvelopeConfig {
    /// Map from user-chosen section name to its extract + schema.
    /// Insertion-ordered so diagnostics list sections in the order the
    /// author declared them.
    #[serde(default)]
    pub sections: IndexMap<String, EnvelopeSection>,
}

impl EnvelopeConfig {
    /// `true` when no sections are declared — the reader skips the
    /// pre-scan entirely.
    pub fn is_empty(&self) -> bool {
        self.sections.is_empty()
    }
}

/// One declared envelope section.
///
/// `extract` tells the reader how to locate the section's payload in
/// the source file (an XPath in XML, a JSON pointer in JSON; flat-file
/// discriminator support lands with the multi-body issue). `fields`
/// gives the typed schema the CXL typechecker consults when binding
/// `$doc.<section>.<field>` references.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvelopeSection {
    pub extract: EnvelopeExtract,
    #[serde(default)]
    pub fields: IndexMap<String, EnvelopeFieldType>,
}

/// Format-specific extraction rule. The reader picks the variant
/// matching its format; an other-variant arrival surfaces as a
/// format-mismatch error.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeExtract {
    /// XPath-style descent into the parsed XML tree. The matched node's
    /// children become the section's `fields` map keyed by child element
    /// name.
    XmlPath(String),
    /// JSON pointer (RFC 6901) into the source JSON. The matched value
    /// must be a JSON object; its top-level keys become the section's
    /// `fields` map.
    JsonPointer(String),
    /// Flat-file service segment tag (e.g. `UNB` for an EDIFACT
    /// interchange header). The named segment's positional data elements
    /// become the section's `fields` map keyed `e01`, `e02`, … . Only
    /// segments the reader can resolve from its bounded header pre-scan
    /// are extractable; trailer segments that arrive after body streaming
    /// are validated by the reader, not exposed as envelope sections.
    Segment(String),
    /// Multi-record flat-file record-type tag (CSV / fixed-width). The
    /// named tag (e.g. `H` for a header row) identifies which of the
    /// source's declared `records:` types carries this section's payload;
    /// the multi-record reader resolves it from the file's bounded header
    /// pre-scan and the matched row's named columns become the section's
    /// `fields` map. Only a header-type tag the reader can reach before
    /// body streaming is extractable as a `$doc` section; a trailer-type
    /// tag arrives after the body it closes, so it is validated as it
    /// streams (its declared count against the actual body count), not
    /// surfaced here. YAML form: `extract: { record_type: H }`.
    RecordType(String),
}

/// A user-declared section for a *nested* envelope level that has no
/// pre-scan declaration point.
///
/// The file-level envelope (X12 `ISA`, EDIFACT `UNB`, HL7 `FHS`) is
/// declared through [`EnvelopeSection`] before body streaming, because a
/// bounded header pre-scan resolves it. The nested tiers — X12's `GS`
/// functional group and `ST` transaction set — exist only mid-file, so a
/// reader cannot resolve them at pre-scan time and instead names them when
/// it crosses each boundary. This carries the author's chosen section
/// name and the typed `fields` schema the reader coerces the level's
/// positional elements into, so `$doc.<name>.<field>` exposes typed,
/// addressable fields instead of untyped positional `eNN` strings. The
/// `extract` rule is implied by the level (the GS or ST segment), so only
/// the name and field schema are declared.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NestedEnvelopeSection {
    /// The section name `$doc.<name>.<field>` resolves under for this
    /// level. User-chosen — the engine reserves none.
    pub name: String,
    /// Typed field schema, keyed by the level's positional element name
    /// (`e01`, `e02`, …). Drives the runtime coercion of each declared
    /// element to its type; undeclared elements are dropped from the
    /// typed view (the section schema is the contract).
    #[serde(default)]
    pub fields: IndexMap<String, EnvelopeFieldType>,
}

/// Field type vocabulary mirrored from CXL's typechecker — small
/// closed set sufficient for envelope-section authoring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvelopeFieldType {
    String,
    Int,
    Float,
    Bool,
    Date,
    DateTime,
}

/// Coerce raw `(name, string)` pairs extracted from a source's envelope
/// into the section's declared field schema.
///
/// Shared by every reader that extracts envelope sections (XML element
/// payloads, EDIFACT positional service-segment elements). Unknown raw
/// fields are dropped silently — the section schema is the contract.
/// Missing declared fields are not reported here; they evaluate to
/// [`Value::Null`] via the `DocumentContext::get_section_field`
/// `Option`-to-`Null` mapping at CXL eval time. The first non-empty
/// observation wins when a raw name repeats.
///
/// # Errors
///
/// Returns the coercion-failure message (section, field, declared type,
/// observed value) as a `String` so each caller can wrap it in its own
/// format-specific [`FormatError`] variant.
pub(crate) fn coerce_section_fields(
    raw: Vec<(String, String)>,
    schema: &IndexMap<String, EnvelopeFieldType>,
) -> Result<IndexMap<Box<str>, Value>, String> {
    let mut out: IndexMap<Box<str>, Value> = IndexMap::with_capacity(schema.len());
    let mut by_name: IndexMap<&str, &str> = IndexMap::with_capacity(raw.len());
    for (k, v) in &raw {
        by_name.entry(k.as_str()).or_insert(v.as_str());
    }
    for (field, ty) in schema {
        let raw_str = match by_name.get(field.as_str()) {
            Some(s) if !s.is_empty() => *s,
            _ => continue,
        };
        let value = Value::String(raw_str.into());
        let coerced = match ty {
            EnvelopeFieldType::String => coerce_to_string(&value),
            EnvelopeFieldType::Int => coerce_to_int(&value),
            EnvelopeFieldType::Float => coerce_to_float(&value),
            EnvelopeFieldType::Bool => coerce_to_bool(&value),
            EnvelopeFieldType::Date => coerce_to_date(&value, DEFAULT_DATE_FORMATS),
            EnvelopeFieldType::DateTime => coerce_to_datetime(&value, DEFAULT_DATETIME_FORMATS),
        }
        .map_err(|e| {
            format!(
                "envelope section field {field:?} (declared type {ty:?}): \
                 cannot coerce value {raw_str:?}: {e}"
            )
        })?;
        out.insert(Box::from(field.as_str()), coerced);
    }
    Ok(out)
}
