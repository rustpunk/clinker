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

mod charset;
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
/// composites, immediately ahead of the control reference. Its content is
/// the structural marker for the two header divergences that shift the
/// control reference past its canonical slot: an empty element here marks
/// the doubly-degenerate padded shape, and a bare date token here (with a
/// bare time in the next slot) marks a split date/time (see
/// [`unb_control_candidates`]).
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
/// The reference is the first non-empty element after the leading
/// composite group. Two producer divergences shift it past its canonical
/// slot, and each is resolved before the first-non-empty rule runs:
///
/// - **Split S004 date/time.** A header that transmits S004 as two
///   element-separated parts (a bare date, then a bare time) instead of
///   the conformant single `date:time` composite pushes 0020 one slot
///   later. [`s004_date_time_is_split`] recognises that shape
///   deterministically, so the time part is consumed as part of the
///   leading group rather than mistaken for the control reference — a
///   single, unambiguous location, not an extra candidate.
/// - **Empty padding ahead of a date-only S004.** A *doubly-degenerate*
///   header that leaves the S004 slot empty and places a date-only S004
///   where 0020 would sit is byte-indistinguishable from a conformant
///   header whose optional S005 recipient reference is present. Position
///   alone cannot choose between the date and the next element, and
///   sniffing the candidate's data shape would risk misreading a
///   reference that merely looks like a date, so both are offered as
///   candidates and the `UNZ` echo disambiguates.
///
/// At most two candidates are yielded: the canonical slot and the
/// single-position shift the empty-padding degeneracy can produce. An
/// echo matching neither is a genuine mismatch.
fn unb_control_candidates(elements: &[String]) -> impl Iterator<Item = &str> {
    // A split date/time consumes one extra leading element (the time
    // part), so the reference sits one slot later. Recombining it shifts
    // the whole leading group rather than offering an extra candidate,
    // since the recombination is deterministic.
    let leading = if s004_date_time_is_split(elements) {
        UNB_LEADING_COMPOSITES + 1
    } else {
        UNB_LEADING_COMPOSITES
    };
    // An empty S004 slot marks the doubly-degenerate shape, where the
    // canonical slot and the slot after it are both plausible and only the
    // trailer can choose. A split date/time is mutually exclusive with it:
    // the S004 slot then holds a bare date, not an empty pad.
    let s004_padded = elements.get(UNB_S004_INDEX).is_some_and(|e| e.is_empty());
    elements
        .iter()
        .skip(leading)
        .map(String::as_str)
        .filter(|e| !e.is_empty())
        // The primary candidate is the first non-empty element after the
        // leading composites; the shifted candidate is the next one, and
        // only when the empty S004 slot marks the doubly-degenerate shape.
        .take(if s004_padded { 2 } else { 1 })
}

/// Whether the `UNB` S004 date/time of preparation is transmitted as two
/// separate data elements — a bare date in the S004 slot, then a bare
/// time in the next slot — rather than the conformant single composite
/// element `date:time`.
///
/// The split pushes the interchange control reference one slot later, so
/// recognising it is a prerequisite for counting element positions
/// correctly. The rule is deterministic and content-grounded rather than
/// a loose "looks like data" sniff: it fires only when the S004 slot holds
/// a bare date token (all digits, `YYMMDD` or `CCYYMMDD`) carrying no
/// component separator — so it is *not* already a `date:time` composite —
/// and the immediately following slot holds a bare EDIFACT time token (all
/// digits, `HHMM` or `HHMMSS`). A genuine control reference whose value
/// happens to be all digits is not mistaken for a split time unless the
/// S004 slot is itself a bare date of the matching width, and even then
/// the `UNZ` echo check still rejects a trailer that contradicts the
/// recombined header.
fn s004_date_time_is_split(elements: &[String]) -> bool {
    let Some(date) = elements.get(UNB_S004_INDEX) else {
        return false;
    };
    // The bare-date-token test also excludes a conformant composite: a
    // joined `date:time` carries a component separator, so it is neither
    // all-digits nor of date-token width and never matches.
    if !is_edifact_date_token(date) {
        return false;
    }
    elements
        .get(UNB_S004_INDEX + 1)
        .is_some_and(|time| is_edifact_time_token(time))
}

/// Whether `s` is a bare EDIFACT date token: all ASCII digits and either
/// six (`YYMMDD`) or eight (`CCYYMMDD`) characters wide, the two date
/// forms the S004 date component takes.
fn is_edifact_date_token(s: &str) -> bool {
    matches!(s.len(), 6 | 8) && s.bytes().all(|b| b.is_ascii_digit())
}

/// Whether `s` is a bare EDIFACT time token: all ASCII digits and either
/// four (`HHMM`) or six (`HHMMSS`) characters wide, the two time forms the
/// S004 time component takes.
fn is_edifact_time_token(s: &str) -> bool {
    matches!(s.len(), 4 | 6) && s.bytes().all(|b| b.is_ascii_digit())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn els(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn conformant_header_locates_reference_at_canonical_slot() {
        let e = els(&["UNOA:1", "SENDER", "RECEIVER", "240101:1200", "REF1"]);
        assert_eq!(unb_control_reference(&e), Some("REF1"));
        assert!(unz_echo_matches_unb(&e, "REF1"));
        assert!(!unz_echo_matches_unb(&e, "1200"));
    }

    #[test]
    fn empty_padding_shifts_reference_one_slot() {
        // S004 is populated; an empty optional element sits ahead of the
        // reference, shifting it to index 5.
        let e = els(&["UNOA:1", "S", "R", "240101:1200", "", "REF1"]);
        assert_eq!(unb_control_reference(&e), Some("REF1"));
        assert!(unz_echo_matches_unb(&e, "REF1"));
    }

    #[test]
    fn split_date_time_recombines_before_locating_reference() {
        // S004 split across two elements (date, then time) pushes the true
        // reference REF1 to index 5; the time part 1200 must not be taken
        // as the control reference.
        let e = els(&["UNOA:1", "SENDER", "RECEIVER", "240101", "1200", "REF1"]);
        assert!(s004_date_time_is_split(&e));
        assert_eq!(unb_control_reference(&e), Some("REF1"));
        assert!(unz_echo_matches_unb(&e, "REF1"));
        // A trailer echoing the time part is a genuine mismatch — the split
        // recombination is deterministic, not a candidate-widening.
        assert!(!unz_echo_matches_unb(&e, "1200"));
    }

    #[test]
    fn split_date_time_with_eight_digit_date_and_six_digit_time() {
        // CCYYMMDD date plus HHMMSS time — the wider S004 forms still split.
        let e = els(&["UNOA:1", "S", "R", "20240101", "120000", "REF1"]);
        assert!(s004_date_time_is_split(&e));
        assert_eq!(unb_control_reference(&e), Some("REF1"));
    }

    #[test]
    fn conformant_joined_date_time_is_not_treated_as_split() {
        // The composite carries a component separator, so it is already one
        // element and nothing recombines.
        let e = els(&["UNOA:1", "S", "R", "240101:1200", "REF1"]);
        assert!(!s004_date_time_is_split(&e));
        assert_eq!(unb_control_reference(&e), Some("REF1"));
    }

    #[test]
    fn date_only_s004_without_a_following_time_is_not_a_split() {
        // A bare date in S004 followed by a non-time reference is a
        // date-only S004, not a split; the reference is the next element.
        let e = els(&["UNOA:1", "S", "R", "240101", "REF1"]);
        assert!(!s004_date_time_is_split(&e));
        assert_eq!(unb_control_reference(&e), Some("REF1"));
    }

    #[test]
    fn non_date_s004_followed_by_four_digit_reference_is_not_a_split() {
        // A populated non-date S004 (already a conformant single date here
        // would carry a colon; a genuinely odd S004 that is not a bare date
        // token) must not trigger recombination even when the next element
        // is four digits.
        let e = els(&["UNOA:1", "S", "R", "240101:1200", "1234"]);
        assert!(!s004_date_time_is_split(&e));
        assert_eq!(unb_control_reference(&e), Some("1234"));
    }

    #[test]
    fn doubly_degenerate_padded_header_is_not_misread_as_split() {
        // Empty S004 pad plus a date-only S004 at the canonical slot: the
        // empty-padding path owns this shape, not the split path. Both the
        // date and the next element are candidates so the UNZ echo chooses.
        let e = els(&["UNOA:1", "SENDER", "RECEIVER", "", "240101", "REF9"]);
        assert!(!s004_date_time_is_split(&e));
        assert!(unz_echo_matches_unb(&e, "REF9"));
        assert!(unz_echo_matches_unb(&e, "240101"));
        assert!(!unz_echo_matches_unb(&e, "WRONG"));
    }

    #[test]
    fn date_token_widths_are_six_or_eight_digits_only() {
        assert!(is_edifact_date_token("240101"));
        assert!(is_edifact_date_token("20240101"));
        assert!(!is_edifact_date_token("2401")); // too short
        assert!(!is_edifact_date_token("2401011")); // odd width
        assert!(!is_edifact_date_token("24010a")); // non-digit
    }

    #[test]
    fn time_token_widths_are_four_or_six_digits_only() {
        assert!(is_edifact_time_token("1200"));
        assert!(is_edifact_time_token("120000"));
        assert!(!is_edifact_time_token("12")); // too short
        assert!(!is_edifact_time_token("12000")); // odd width
        assert!(!is_edifact_time_token("12o0")); // non-digit
    }
}
