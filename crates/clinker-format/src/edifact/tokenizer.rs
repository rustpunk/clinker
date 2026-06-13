//! UN/EDIFACT segment tokenizer: delimiter discovery and
//! release-char-aware splitting.
//!
//! An EDIFACT interchange is a flat byte stream of segments terminated
//! by the segment terminator (default `'`). Each segment splits into a
//! tag and data elements on the element separator (default `+`); each
//! element splits into components on the component separator (default
//! `:`). A leading 9-byte `UNA` service-string-advice overrides those
//! delimiters; when `UNA` is absent the syntax Level-A defaults apply.
//!
//! Memory model: the tokenizer reads one segment at a time from a
//! buffered source into a bounded scratch buffer. A hard per-segment
//! byte cap turns a missing terminator (truncated / corrupt input)
//! into an error rather than letting the buffer grow without limit.

use std::io::{BufRead, Read};

use crate::edifact::charset::Charset;
use crate::error::FormatError;
use crate::segment_tokenizer::{
    SegmentFraming, TrailingSegment, read_raw_segment, skip_inter_segment_whitespace,
};

/// Hard ceiling on a single segment's raw byte length. A real EDIFACT
/// segment is well under a kilobyte; this cap exists only so a stream
/// with no terminator byte fails fast instead of buffering the whole
/// file into one "segment".
pub(crate) const MAX_SEGMENT_BYTES: usize = 64 * 1024;

/// The six EDIFACT service characters discovered from `UNA` (or the
/// Level-A defaults when `UNA` is absent).
///
/// The `repetition` separator is stored but never split on: element
/// text is kept verbatim, so a repeating element rides inside one
/// element string intact (no first-only data loss). The Level-A default
/// is space, which syntax version 3 reserves as unused — splitting on it
/// would corrupt ordinary text data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Delimiters {
    pub component: u8,
    pub element: u8,
    pub decimal: u8,
    pub release: u8,
    pub repetition: u8,
    pub terminator: u8,
}

impl Delimiters {
    /// Syntax Level-A defaults, used when no `UNA` prefix is present.
    pub(crate) fn level_a() -> Self {
        Self {
            component: b':',
            element: b'+',
            decimal: b'.',
            release: b'?',
            repetition: b' ',
            terminator: b'\'',
        }
    }
}

/// Streaming EDIFACT segment reader.
///
/// Wraps a buffered source and yields one decoded segment string at a time
/// (terminator stripped, inter-segment CR/LF whitespace stripped). The
/// delimiters are discovered once from the optional `UNA` prefix on the
/// first read. Element text is decoded through the active [`Charset`], which
/// starts on the byte-total bootstrap repertoire so the `UNB` decodes before
/// its in-band syntax identifier is known; the reader re-arms it via
/// [`Self::set_charset`] once that identifier is parsed.
pub(crate) struct SegmentTokenizer<R: Read> {
    reader: R,
    delimiters: Delimiters,
    charset: Charset,
    initialized: bool,
}

impl<R: BufRead> SegmentTokenizer<R> {
    /// Build a tokenizer over a buffered source. Delimiter discovery is
    /// deferred to the first [`Self::next_segment`] call so the `UNA`
    /// scan and the first segment read share one pass. Decoding starts on
    /// the bootstrap charset so the `UNB` — which names the body repertoire
    /// in-band — can itself be decoded before that repertoire is known.
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            delimiters: Delimiters::level_a(),
            charset: Charset::Bootstrap,
            initialized: false,
        }
    }

    /// The active delimiter set (Level-A until the first read resolves
    /// any `UNA` prefix).
    pub(crate) fn delimiters(&self) -> Delimiters {
        self.delimiters
    }

    /// Re-arm the decoding charset, called by the reader once it has parsed
    /// the `UNB` syntax identifier so every subsequent segment decodes
    /// through the negotiated repertoire.
    pub(crate) fn set_charset(&mut self, charset: Charset) {
        self.charset = charset;
    }

    /// Resolve the optional `UNA` prefix. Peeks the first bytes of the
    /// stream: a `UNA` tag consumes the 9-byte service string and the
    /// (optional) single separator byte that follows it; otherwise the
    /// Level-A defaults stand and no bytes are consumed.
    fn initialize(&mut self) -> Result<(), FormatError> {
        if self.initialized {
            return Ok(());
        }
        self.initialized = true;

        let head = self.reader.fill_buf()?;
        if head.starts_with(b"UNA") {
            if head.len() < 9 {
                // A truncated UNA is unrecoverable — the delimiter set is
                // undefined, so nothing downstream can be tokenized.
                return Err(FormatError::Edifact(
                    "truncated UNA service string: need 9 bytes after the stream start, \
                     the interchange is corrupt or truncated"
                        .into(),
                ));
            }
            // UNA layout: bytes 3..9 are component, element, decimal,
            // release, repetition, terminator in that fixed order.
            let una = &head[3..9];
            let delimiters = Delimiters {
                component: una[0],
                element: una[1],
                decimal: una[2],
                release: una[3],
                repetition: una[4],
                terminator: una[5],
            };
            self.delimiters = delimiters;
            self.reader.consume(9);
            skip_inter_segment_whitespace(&mut self.reader)?;
        }
        Ok(())
    }

    /// EDIFACT segment framing: the terminator discovered from `UNA` (or the
    /// Level-A default), the release character that escapes an embedded
    /// terminator, and a terminator-less final segment rejected as a
    /// truncated interchange.
    fn framing(&self) -> SegmentFraming {
        SegmentFraming {
            terminator: self.delimiters.terminator,
            release: Some(self.delimiters.release),
            max_segment_bytes: MAX_SEGMENT_BYTES,
            trailing: TrailingSegment::RejectUnterminated,
        }
    }

    /// Read the next segment (terminator-delimited) and decode it through
    /// the active [`Charset`], or `None` at clean end of stream.
    ///
    /// The terminator byte is consumed but not returned. A release char
    /// immediately before a terminator escapes it (the terminator is
    /// then literal data, not a boundary). CR/LF between segments is
    /// dropped; CR/LF inside an element is preserved. The raw segment bytes
    /// are decoded once here (the shared framing layer that read them never
    /// interprets bytes), so the in-band repertoire is honored without
    /// touching segment framing.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Edifact`] when the segment bytes are not valid
    /// in the active charset — a high byte under an ASCII repertoire, or
    /// invalid UTF-8 under UNOY.
    pub(crate) fn next_segment(&mut self) -> Result<Option<String>, FormatError> {
        self.initialize()?;
        skip_inter_segment_whitespace(&mut self.reader)?;

        let framing = self.framing();
        let raw = match read_raw_segment(
            &mut self.reader,
            &framing,
            || {
                FormatError::Edifact(format!(
                    "segment exceeded the {MAX_SEGMENT_BYTES}-byte cap without a terminator; \
                     the interchange is malformed or the segment terminator is misconfigured"
                ))
            },
            |read| {
                FormatError::Edifact(format!(
                    "unterminated final segment ({read} bytes read with no segment terminator); \
                     the interchange is truncated"
                ))
            },
        )? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let text = self.charset.decode(raw)?;
        Ok(Some(text))
    }
}

/// A parsed segment: its tag plus its decoded data elements.
///
/// Release-escape sequences are decoded into their literal data byte
/// here, so an element value carries clean data (a `?'` on the wire
/// becomes a literal `'` in the value, a `?+` becomes a literal `+`).
/// Downstream consumers — CSV/JSON output, CXL string predicates,
/// `$doc` fields — therefore see the data the EDIFACT producer meant,
/// never the wire escapes. The writer re-escapes on output, so the
/// reader → writer → reader round-trip is byte-faithful.
///
/// Component and repetition separators are *not* decoded or split:
/// element text keeps them verbatim, so a composite element `A:B:C`
/// stays one element string `"A:B:C"` and a repeating element rides
/// inside one string intact (no first-only data loss). The positional
/// element model deliberately works above the component/repetition
/// resolution — only the release mechanism is interpreted, because that
/// is what distinguishes a delimiter byte from literal data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedSegment {
    pub tag: String,
    pub elements: Vec<String>,
}

/// Split a raw segment string into its tag and decoded element values on
/// the element separator, honoring the release char.
///
/// The tag is the first element; the remainder are data elements. A
/// release char escapes the following byte, which is emitted as literal
/// data with the release char dropped (so `?+` decodes to `+`, `?'` to
/// `'`, `??` to `?`). An unescaped element separator splits; an
/// unescaped component separator is kept verbatim (it is part of the
/// element's internal composite structure, not a data byte to decode).
/// Repetition splitting is not performed — repetitions ride inside the
/// element text verbatim.
pub(crate) fn split_segment(raw: &str, delims: &Delimiters) -> ParsedSegment {
    let element = delims.element;
    let release = delims.release;

    let bytes = raw.as_bytes();
    let mut parts: Vec<String> = Vec::new();
    let mut current: Vec<u8> = Vec::new();
    let mut pending_release = false;

    for &b in bytes {
        if pending_release {
            // The previous byte was a release char: this byte is literal
            // data regardless of its delimiter role. Emit it decoded —
            // the release char is consumed, not retained.
            current.push(b);
            pending_release = false;
            continue;
        }
        if b == release {
            pending_release = true;
            continue;
        }
        if b == element {
            parts.push(bytes_to_string(&current));
            current.clear();
            continue;
        }
        current.push(b);
    }
    if pending_release {
        // A dangling release char at end of segment has nothing to
        // escape; keep it as literal data rather than dropping it.
        current.push(release);
    }
    parts.push(bytes_to_string(&current));

    let mut iter = parts.into_iter();
    let tag = iter.next().unwrap_or_default();
    let elements = iter.collect();
    ParsedSegment { tag, elements }
}

/// Reassemble interior `current` bytes into a `String`. The input is a
/// UTF-8-validated segment slice, so this never fails.
fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn tok(data: &[u8]) -> SegmentTokenizer<Cursor<Vec<u8>>> {
        SegmentTokenizer::new(Cursor::new(data.to_vec()))
    }

    #[test]
    fn level_a_defaults_when_una_absent() {
        let mut t = tok(b"UNB+UNOA:1'");
        let first = t.next_segment().unwrap().unwrap();
        assert_eq!(first, "UNB+UNOA:1");
        assert_eq!(t.delimiters(), Delimiters::level_a());
    }

    #[test]
    fn una_overrides_delimiters() {
        // UNA declares component '|', element '*', decimal ',',
        // release '#', repetition ' ', terminator '~'.
        let mut t = tok(b"UNA|*,# ~UNB*UNOA|1~");
        let first = t.next_segment().unwrap().unwrap();
        let d = t.delimiters();
        assert_ne!(d, Delimiters::level_a());
        assert_eq!(d.component, b'|');
        assert_eq!(d.element, b'*');
        assert_eq!(d.terminator, b'~');
        assert_eq!(first, "UNB*UNOA|1");
        let parsed = split_segment(&first, &d);
        assert_eq!(parsed.tag, "UNB");
        assert_eq!(parsed.elements, vec!["UNOA|1".to_string()]);
    }

    #[test]
    fn release_char_decodes_escaped_element_separator() {
        // BGM+1?+2+done' — the ?+ is an escaped (literal) '+' inside the
        // first data element; it decodes to a clean '+' in the value. The
        // unescaped '+' before "done" is the real element boundary, so
        // the segment has two elements: "1+2" and "done".
        let mut t = tok(b"BGM+1?+2+done'");
        let raw = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&raw, &t.delimiters());
        assert_eq!(parsed.tag, "BGM");
        assert_eq!(parsed.elements, vec!["1+2".to_string(), "done".to_string()]);
    }

    #[test]
    fn release_char_decodes_escaped_terminator() {
        // FTX+a?'b' — the ?' is a literal apostrophe inside the element
        // (not a terminator); the unescaped ' terminates the segment. The
        // element decodes to clean "a'b".
        let mut t = tok(b"FTX+a?'b'");
        let raw = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&raw, &t.delimiters());
        assert_eq!(parsed.tag, "FTX");
        assert_eq!(parsed.elements, vec!["a'b".to_string()]);
    }

    #[test]
    fn double_release_decodes_to_literal_release_char() {
        // FTX+a??b' — ?? is an escaped (literal) '?'; it decodes to a
        // single clean '?' in the element value.
        let mut t = tok(b"FTX+a??b'");
        let raw = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&raw, &t.delimiters());
        assert_eq!(parsed.elements, vec!["a?b".to_string()]);
    }

    #[test]
    fn unescaped_component_separator_kept_verbatim() {
        // NAD+UNOA:1 — the ':' is a real component separator, part of the
        // element's composite structure. It is kept verbatim in the
        // element value, not decoded, so a composite element round-trips.
        let d = Delimiters::level_a();
        let parsed = split_segment("UNB+UNOA:1", &d);
        assert_eq!(parsed.elements, vec!["UNOA:1".to_string()]);
    }

    #[test]
    fn repetition_separator_space_does_not_split_element_text() {
        // Element text is kept verbatim and never split on the
        // repetition separator, so a space inside an element is ordinary
        // text — `"ACME WIDGETS"` is one element, not two.
        let d = Delimiters::level_a();
        let parsed = split_segment("NAD+BY+ACME WIDGETS", &d);
        assert_eq!(
            parsed.elements,
            vec!["BY".to_string(), "ACME WIDGETS".to_string()]
        );
    }

    #[test]
    fn crlf_between_segments_stripped_not_inside_elements() {
        let mut t = tok(b"UNH+1+ORDERS'\r\nBGM+220'\r\n");
        let s1 = t.next_segment().unwrap().unwrap();
        assert_eq!(s1, "UNH+1+ORDERS");
        let s2 = t.next_segment().unwrap().unwrap();
        assert_eq!(s2, "BGM+220");
        assert!(t.next_segment().unwrap().is_none());
    }

    #[test]
    fn unterminated_segment_hits_byte_cap_error() {
        let data = vec![b'A'; MAX_SEGMENT_BYTES + 10];
        let mut t = tok(&data);
        let err = t.next_segment().unwrap_err();
        assert!(matches!(err, FormatError::Edifact(msg) if msg.contains("cap")));
    }

    #[test]
    fn unterminated_final_segment_errors() {
        let mut t = tok(b"UNH+1+ORDERS'BGM+220");
        let _ = t.next_segment().unwrap().unwrap();
        let err = t.next_segment().unwrap_err();
        assert!(matches!(err, FormatError::Edifact(msg) if msg.contains("truncated")));
    }

    #[test]
    fn truncated_una_errors() {
        let mut t = tok(b"UNA:+");
        let err = t.next_segment().unwrap_err();
        assert!(matches!(err, FormatError::Edifact(msg) if msg.contains("UNA")));
    }

    #[test]
    fn bootstrap_decodes_unb_with_high_byte_in_a_later_element() {
        // The bootstrap charset must not break on a high byte before the
        // syntax identifier is known: a UNB carrying a Latin-1 byte 0xE9 in
        // a sender field still decodes (the tag/syntax id are ASCII).
        let mut data = b"UNB+UNOC:3+Caf".to_vec();
        data.push(0xE9);
        data.push(b'\'');
        let mut t = SegmentTokenizer::new(Cursor::new(data));
        let first = t.next_segment().unwrap().unwrap();
        assert_eq!(first, "UNB+UNOC:3+Café");
    }

    #[test]
    fn rearmed_latin1_decodes_high_byte_body_segment() {
        // After the UNB is decoded under the bootstrap charset, re-arming to
        // Latin-1 (as the reader does once it parses UNOC) lets a body
        // segment carrying a Latin-1 high byte decode correctly.
        let mut body = b"NAD+BY+Caf".to_vec();
        body.push(0xE9);
        body.push(b'\'');
        let mut full = b"UNB+UNOC:3+S+R'".to_vec();
        full.extend_from_slice(&body);
        let mut t = SegmentTokenizer::new(Cursor::new(full));
        let unb = t.next_segment().unwrap().unwrap();
        assert_eq!(unb, "UNB+UNOC:3+S+R");
        // The reader re-arms here, between segment 1 (UNB) and segment 2.
        t.set_charset(Charset::Latin1);
        let nad = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&nad, &t.delimiters());
        assert_eq!(parsed.tag, "NAD");
        assert_eq!(parsed.elements, vec!["BY".to_string(), "Café".to_string()]);
    }

    #[test]
    fn high_byte_body_segment_rejected_under_ascii_charset() {
        // The same body bytes under a UNOA/UNOB ASCII repertoire fail loud
        // rather than silently reinterpreting the high byte.
        let mut body = b"NAD+BY+Caf".to_vec();
        body.push(0xE9);
        body.push(b'\'');
        let mut full = b"UNB+UNOA:1+S+R'".to_vec();
        full.extend_from_slice(&body);
        let mut t = SegmentTokenizer::new(Cursor::new(full));
        let _ = t.next_segment().unwrap().unwrap(); // UNB under bootstrap
        t.set_charset(Charset::Ascii);
        let err = t.next_segment().unwrap_err();
        assert!(matches!(err, FormatError::Edifact(m) if m.contains("ASCII")));
    }
}
