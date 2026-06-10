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
    OpenLevel { sections: IndexMap<Box<str>, Value> },
    /// Leave the innermost open nested envelope level. The driver closes
    /// the matching document context.
    CloseLevel,
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
