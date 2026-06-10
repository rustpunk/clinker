//! UN/EDIFACT reader/writer pair.
//!
//! EDIFACT is a flat, delimiter-structured interchange format. An
//! interchange opens with an optional `UNA` service-string advice and a
//! mandatory `UNB` header, wraps one or more `UNH..UNT` messages, and
//! closes with a `UNZ` trailer. Within a segment, data elements split on
//! the element separator (default `+`), components on the component
//! separator (default `:`); a release char (default `?`) escapes a
//! delimiter that occurs as literal data.
//!
//! This module maps one body segment to one [`crate::traits::FormatReader`]
//! record under a static positional schema (`seg_id`, `msg_ref`,
//! `msg_type`, `e01..`). Service segments are consumed by the reader to
//! validate `UNT`/`UNZ` control counts and control-reference echoes
//! inline; they are never emitted as body records. The writer mirrors
//! this, reconstructing the envelope around emitted records and
//! recomputing every control count.
//!
//! Memory model: only the interchange header is pre-scanned and retained
//! (so `$doc` envelope sections over `UNB` cost O(UNB)); the body streams
//! one segment at a time. The whole interchange is never buffered.

pub mod reader;
mod tokenizer;
pub mod writer;

pub use reader::{EdifactReader, EdifactReaderConfig};
pub use writer::{EdifactWriter, EdifactWriterConfig};

/// Engine-internal key under which the reader stashes the complete,
/// ordered `UNB` data-element list (empty middle elements included) in a
/// `$doc` envelope section, for lossless writer reconstruction via
/// `interchange_from_doc`. The `$` prefix marks it as engine-namespaced
/// so it is not a user-declarable envelope field and never surfaces in
/// CXL `$doc.<section>.<field>` typechecking.
pub(crate) const RAW_ELEMENTS_KEY: &str = "$raw";

/// Count of mandatory composite data elements that precede the
/// interchange control reference in a `UNB` segment, per ISO 9735:
/// S001 (syntax identifier), S002 (interchange sender), S003
/// (interchange recipient), and S004 (date/time of preparation). The
/// control reference (data element 0020) is the first *simple* element
/// after this leading composite group, so its canonical wire index is
/// this count.
const UNB_LEADING_COMPOSITES: usize = 4;

/// Locate the interchange control reference (UNB data element 0020) by
/// its defined position in the segment structure rather than a fixed
/// absolute index.
///
/// The control reference follows the four mandatory leading composite
/// elements S001–S004 (syntax identifier, sender, recipient,
/// date/time). In a conformant `UNB` those occupy wire indices 0–3 and
/// the control reference sits at index 4. Real interchanges, however,
/// carry an empty optional or placeholder element ahead of the control
/// reference — e.g. an empty recipient-reference slot — which shifts it
/// to a later index. Locating it at a fixed index 4 then extracts the
/// empty padding instead, so the reader spuriously rejects an
/// internally consistent interchange and the writer echoes the wrong
/// element into `UNZ`.
///
/// Both the reader and the writer call this so the located reference is
/// identical on read and on re-emit, keeping a round-tripped header and
/// trailer self-consistent. The control reference 0020 is a mandatory,
/// non-empty element, so the rule is: the first non-empty element at or
/// after the canonical position (the count of leading composites).
/// Returns `None` only for a degenerate header with no such element,
/// which disables the `UNZ`-echo check rather than failing the parse.
pub(crate) fn unb_control_reference(elements: &[String]) -> Option<&str> {
    elements
        .iter()
        .skip(UNB_LEADING_COMPOSITES)
        .map(String::as_str)
        .find(|e| !e.is_empty())
}
