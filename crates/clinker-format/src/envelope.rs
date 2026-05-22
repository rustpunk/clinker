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

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

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
