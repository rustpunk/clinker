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

/// Wire index of the canonical S004 (date/time of preparation) slot in a
/// `UNB` segment: the fourth and last of the mandatory leading
/// composites, immediately ahead of the control reference. An empty
/// element here is the structural marker that the date/time has been
/// padded, which is what shifts the control reference past its canonical
/// slot in a doubly-degenerate header (see [`unb_control_candidates`]).
const UNB_S004_INDEX: usize = UNB_LEADING_COMPOSITES - 1;

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
/// Returns the primary positional candidate: the first non-empty element
/// at or after the canonical position (the count of leading composites).
/// The writer uses this as the reference it echoes into `UNZ` when it
/// reconstructs a header from a `$doc` section, since at write time there
/// is no trailer to consult. `None` only for a degenerate header with no
/// such element, which disables the writer's echo rather than fabricating
/// one.
pub(crate) fn unb_control_reference(elements: &[String]) -> Option<&str> {
    unb_control_candidates(elements).next()
}

/// Yield the structurally-plausible positions for the interchange control
/// reference (0020), in order of decreasing likelihood, so the reader can
/// confirm which one the `UNZ` trailer echoes.
///
/// The first candidate is the canonical one — the first non-empty element
/// after the four mandatory leading composites. A second candidate exists
/// only for the *doubly-degenerate* header that the first-non-empty rule
/// cannot resolve on its own: when the S004 (date/time) slot is empty, a
/// producer that also emits a date-only S004 places it where 0020 would
/// normally sit, so the date occupies the canonical slot and the true
/// control reference is the next non-empty element. This shape is
/// byte-indistinguishable from a conformant header whose optional S005
/// recipient reference is present, so position alone cannot choose
/// between them — only the `UNZ` echo can. The reader therefore treats
/// both as candidates and accepts whichever the trailer echoes,
/// validating against the segment structure rather than guessing from the
/// data shape of the candidate (which would risk misreading a reference
/// that merely looks like a date).
///
/// At most two candidates are yielded: the canonical slot and the
/// single-position shift the documented degeneracies can produce. An
/// echo matching neither is a genuine mismatch.
fn unb_control_candidates(elements: &[String]) -> impl Iterator<Item = &str> {
    let s004_padded = elements.get(UNB_S004_INDEX).is_some_and(|e| e.is_empty());
    elements
        .iter()
        .enumerate()
        .skip(UNB_LEADING_COMPOSITES)
        .filter(|(_, e)| !e.is_empty())
        .map(|(i, e)| (i, e.as_str()))
        // The primary candidate is the first non-empty element after the
        // leading composites; the shifted candidate is the next one, and
        // only when the empty S004 slot marks the doubly-degenerate shape.
        .take(if s004_padded { 2 } else { 1 })
        .map(|(_, e)| e)
}

/// Whether a `UNZ` control-reference echo matches the control reference
/// the `UNB` declares, validating structurally against the segment.
///
/// The `UNB` header is parsed before the trailer is available, so the
/// reader cannot resolve a doubly-degenerate header's control reference
/// from the header alone (the canonical slot and the next slot are both
/// plausible). The `UNZ` echo is the structural disambiguator: a valid
/// interchange echoes its true control reference, so the echo confirms
/// which candidate slot holds 0020. The check passes when the echo equals
/// any structurally-plausible candidate (see [`unb_control_candidates`])
/// and fails otherwise — a corrupt or out-of-sync trailer.
///
/// A header with no candidate at all (a degenerate `UNB` with no non-empty
/// element after the leading composites) has no control reference to echo,
/// so any echo passes — there is nothing to contradict.
pub(crate) fn unz_echo_matches_unb(unb_elements: &[String], unz_echo: &str) -> bool {
    let mut candidates = unb_control_candidates(unb_elements).peekable();
    // A header with no control-reference candidate has nothing for the
    // trailer to contradict, so the echo is vacuously valid.
    candidates.peek().is_none() || candidates.any(|c| c == unz_echo)
}
