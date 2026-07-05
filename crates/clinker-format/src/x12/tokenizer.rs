//! ANSI ASC X12 segment tokenizer: ISA-driven delimiter discovery and
//! delimiter-aware splitting.
//!
//! An X12 interchange is a flat byte stream of segments terminated by the
//! segment terminator. Each segment splits into a tag and data elements on
//! the element (data) separator; an element splits into components on the
//! sub-element (component) separator. Unlike EDIFACT, X12 declares its
//! delimiters in a fixed-length 106-byte `ISA` header rather than an
//! optional service-string advice, and X12 has **no** release/escape
//! character — a data value that contains a delimiter byte is
//! unrepresentable, so the writer rejects it rather than corrupt the
//! interchange.
//!
//! Delimiter discovery reads three bytes at fixed structural positions of
//! the ISA: the element separator is the byte immediately after the `ISA`
//! tag (index 3), the sub-element separator is the `ISA16` byte (index
//! 104), and the segment terminator is the byte immediately after `ISA16`
//! (index 105). Splitting the 106-byte ISA on the discovered element
//! separator yields the 16 ISA elements, so downstream code locates
//! `ISA13` (the interchange control number) as the 13th element rather
//! than by an absolute byte offset.
//!
//! Memory model: the tokenizer reads one segment at a time from a buffered
//! source into a bounded scratch buffer. A hard per-segment byte cap turns
//! a missing terminator (truncated / corrupt input) into an error rather
//! than letting the buffer grow without limit.

use std::io::{BufRead, Read};

use clinker_record::Value;

use crate::charset::Charset;
use crate::error::FormatError;
use crate::segment_tokenizer::{
    SegmentFraming, TrailingSegment, read_raw_segment, skip_inter_segment_whitespace,
};

/// Hard ceiling on a single segment's raw byte length. A real X12 segment
/// is well under a kilobyte; this cap exists only so a stream with no
/// terminator byte fails fast instead of buffering the whole file into one
/// "segment".
pub(crate) const MAX_SEGMENT_BYTES: usize = 64 * 1024;

/// The fixed byte length of an `ISA` interchange header, terminator
/// included. The header is read as one 106-byte slice; the three
/// delimiters live at structural positions within it.
pub(crate) const ISA_LEN: usize = 106;

/// 0-based index of the element (data) separator within the ISA — the
/// byte immediately following the three-character `ISA` tag.
const ISA_ELEMENT_SEP_IDX: usize = 3;

/// 0-based index of the `ISA16` sub-element (component) separator within
/// the ISA — the last single-byte element of the header.
const ISA_SUBELEMENT_SEP_IDX: usize = 104;

/// 0-based index of the segment terminator within the ISA — the byte
/// immediately following `ISA16`.
const ISA_TERMINATOR_IDX: usize = 105;

/// The three X12 service delimiters discovered from the fixed `ISA`
/// header.
///
/// X12 has no release/escape character, so a delimiter byte appearing in
/// data is unrepresentable; the writer rejects such a value rather than
/// emit a structurally corrupt interchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Delimiters {
    /// Data-element separator (between elements of a segment).
    pub element: u8,
    /// Sub-element / component separator (within a composite element).
    /// Stored but never split on: element text is kept verbatim, so a
    /// composite element rides inside one element string intact.
    pub subelement: u8,
    /// Segment terminator.
    pub terminator: u8,
}

impl Delimiters {
    /// Encode the delimiter set as its `$doc` carrier value: a
    /// three-element array of integer bytes `[element, subelement,
    /// terminator]`. Bytes, not characters — a delimiter byte (a `\n`
    /// terminator, say) need not be printable text.
    pub(crate) fn to_doc_value(self) -> Value {
        Value::Array(vec![
            Value::Integer(i64::from(self.element)),
            Value::Integer(i64::from(self.subelement)),
            Value::Integer(i64::from(self.terminator)),
        ])
    }

    /// Decode a delimiter set from its `$doc` carrier value. The key is
    /// engine-namespaced and only ever written by [`Self::to_doc_value`],
    /// so any other shape signals a corrupted document context; the error
    /// message describes the malformation for the caller to wrap.
    pub(crate) fn from_doc_value(value: &Value) -> Result<Self, String> {
        let Value::Array(items) = value else {
            return Err(format!(
                "expected a three-element byte array, found {value:?}"
            ));
        };
        let byte = |i: usize, role: &str| -> Result<u8, String> {
            match items.get(i) {
                Some(Value::Integer(n)) => u8::try_from(*n)
                    .map_err(|_| format!("{role} delimiter {n} is not a byte (0..=255)")),
                other => Err(format!("{role} delimiter is {other:?}, expected a byte")),
            }
        };
        if items.len() != 3 {
            return Err(format!(
                "expected a three-element byte array, found {} elements",
                items.len()
            ));
        }
        Ok(Self {
            element: byte(0, "element")?,
            subelement: byte(1, "sub-element")?,
            terminator: byte(2, "segment-terminator")?,
        })
    }
}

/// Streaming X12 segment reader.
///
/// Wraps a buffered source and yields one decoded segment string at a time
/// (terminator stripped, inter-segment CR/LF whitespace stripped). The
/// delimiters are discovered once from the fixed 106-byte `ISA` header on
/// the first read; element text is decoded through the configured
/// [`Charset`] (the shared framing layer never inspects bytes, so the
/// charset policy stays X12-local here).
pub(crate) struct SegmentTokenizer<R: Read> {
    reader: R,
    delimiters: Delimiters,
    charset: Charset,
    initialized: bool,
}

impl<R: BufRead> SegmentTokenizer<R> {
    /// Build a tokenizer over a buffered source decoding element text
    /// through `charset`. Delimiter discovery is deferred to the first
    /// [`Self::next_segment`] call so the `ISA` header scan and the first
    /// segment read share one pass.
    pub(crate) fn new(reader: R, charset: Charset) -> Self {
        Self {
            reader,
            // A placeholder until the ISA is read; never used because the
            // first read initializes from the header before splitting.
            delimiters: Delimiters {
                element: b'*',
                subelement: b':',
                terminator: b'~',
            },
            charset,
            initialized: false,
        }
    }

    /// The active delimiter set, valid only after the first read has
    /// resolved the `ISA` header.
    pub(crate) fn delimiters(&self) -> Delimiters {
        self.delimiters
    }

    /// The charset element text is decoded through.
    pub(crate) fn charset(&self) -> Charset {
        self.charset
    }

    /// Read and consume the fixed 106-byte `ISA` header, returning its raw
    /// bytes and resolving the delimiter set from it.
    ///
    /// A 106-byte `ISA` segment declares the element separator (index 3),
    /// the sub-element separator (`ISA16`, index 104), and the segment
    /// terminator (index 105). The header bytes are consumed, and any CR/LF
    /// the producer wrote after the terminator is skipped so the next read
    /// starts cleanly at the `GS`. The reader's `ensure_initialized` guards
    /// against a second call, so this runs exactly once before any
    /// [`Self::next_segment`].
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::X12`] when the stream does not begin with a
    /// full 106-byte `ISA` header (a truncated or non-X12 stream leaves the
    /// delimiter set undefined, so nothing downstream can be tokenized).
    pub(crate) fn read_isa_header(&mut self) -> Result<Vec<u8>, FormatError> {
        // Read exactly ISA_LEN bytes. `fill_buf` may return a short slice
        // when the source delivers the header across multiple reads, so
        // accumulate until the header is whole or the stream ends.
        let mut header: Vec<u8> = Vec::with_capacity(ISA_LEN);
        while header.len() < ISA_LEN {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                return Err(FormatError::X12(format!(
                    "truncated ISA header: need {ISA_LEN} bytes at the stream start, \
                     read {}; the interchange is empty, corrupt, or not X12",
                    header.len()
                )));
            }
            let want = ISA_LEN - header.len();
            let take = want.min(buf.len());
            header.extend_from_slice(&buf[..take]);
            self.reader.consume(take);
        }

        if !header.starts_with(b"ISA") {
            return Err(FormatError::X12(format!(
                "interchange must begin with an ISA header, found {:?}",
                String::from_utf8_lossy(&header[..3.min(header.len())])
            )));
        }

        self.delimiters = Delimiters {
            element: header[ISA_ELEMENT_SEP_IDX],
            subelement: header[ISA_SUBELEMENT_SEP_IDX],
            terminator: header[ISA_TERMINATOR_IDX],
        };
        self.initialized = true;
        // Drop the trailing terminator (and any CR/LF a producer wrote
        // after it) so the next read starts cleanly at GS.
        skip_inter_segment_whitespace(&mut self.reader)?;
        Ok(header)
    }

    /// X12 segment framing: the terminator discovered from the `ISA` header,
    /// no release character (X12 has no escape mechanism), and a
    /// terminator-less final segment rejected as a truncated interchange.
    fn framing(&self) -> SegmentFraming {
        SegmentFraming {
            terminator: self.delimiters.terminator,
            release: None,
            max_segment_bytes: MAX_SEGMENT_BYTES,
            trailing: TrailingSegment::RejectUnterminated,
        }
    }

    /// Read the next segment (terminator-delimited) and decode it through
    /// the configured [`Charset`], or `None` at clean end of stream.
    ///
    /// The terminator byte is consumed but not returned. X12 has no
    /// release/escape mechanism, so every terminator byte is a segment
    /// boundary. CR/LF between segments is dropped; CR/LF inside an
    /// element is preserved. The raw segment bytes are decoded once here
    /// (the shared framing layer that read them never interprets bytes), so
    /// a non-UTF-8 charset is honored without touching segment framing.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Charset`] when the segment bytes are not valid
    /// in the configured charset (only possible under [`Charset::Utf8`]).
    pub(crate) fn next_segment(&mut self) -> Result<Option<String>, FormatError> {
        debug_assert!(
            self.initialized,
            "read_isa_header must run before next_segment"
        );
        skip_inter_segment_whitespace(&mut self.reader)?;

        let framing = self.framing();
        let raw = match read_raw_segment(
            &mut self.reader,
            &framing,
            || {
                FormatError::X12(format!(
                    "segment exceeded the {MAX_SEGMENT_BYTES}-byte cap without a terminator; \
                     the interchange is malformed or the segment terminator is misconfigured"
                ))
            },
            |read| {
                FormatError::X12(format!(
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

/// A parsed segment: its tag plus its data elements.
///
/// The sub-element (component) separator is *not* split or decoded:
/// element text keeps it verbatim, so a composite element `A:B:C` stays
/// one element string `"A:B:C"`. The positional element model
/// deliberately works above component resolution. X12 has no release
/// mechanism, so no escape decoding occurs — element text is the raw bytes
/// between element separators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedSegment {
    pub tag: String,
    pub elements: Vec<String>,
}

/// Split a raw segment string into its tag and element values on the
/// element separator.
///
/// The tag is the first part; the remainder are data elements. An
/// unescaped sub-element separator is kept verbatim (it is part of the
/// element's internal composite structure). X12 has no release character,
/// so every element-separator byte splits.
pub(crate) fn split_segment(raw: &str, delims: &Delimiters) -> ParsedSegment {
    let element = delims.element;
    let bytes = raw.as_bytes();
    let mut parts: Vec<String> = Vec::new();
    let mut current: Vec<u8> = Vec::new();

    for &b in bytes {
        if b == element {
            parts.push(bytes_to_string(&current));
            current.clear();
        } else {
            current.push(b);
        }
    }
    parts.push(bytes_to_string(&current));

    let mut iter = parts.into_iter();
    let tag = iter.next().unwrap_or_default();
    let elements = iter.collect();
    ParsedSegment { tag, elements }
}

/// Split the fixed `ISA` header bytes into the `ISA` tag plus its 16 data
/// elements on the discovered element separator, decoding the header text
/// through `charset`.
///
/// The header is split structurally (on the element separator), not by
/// absolute byte offset, so a consumer reads `ISA13` (the interchange
/// control number) as element index 12 regardless of any producer
/// padding quirk — mirroring the rest of the parser's
/// locate-by-structure discipline.
///
/// # Errors
///
/// Returns [`FormatError::Charset`] when the header bytes are not valid in
/// `charset` (only possible under [`Charset::Utf8`]).
pub(crate) fn split_isa(
    header: &[u8],
    delims: &Delimiters,
    charset: Charset,
) -> Result<ParsedSegment, FormatError> {
    // The header's final byte is the segment terminator; trim it so the
    // last element (ISA16) is not contaminated by it.
    let body = if header.last() == Some(&delims.terminator) {
        &header[..header.len() - 1]
    } else {
        header
    };
    let text = charset.decode(body.to_vec())?;
    Ok(split_segment(&text, delims))
}

/// Reassemble interior `current` bytes into a `String`. The input is a
/// slice of an already-decoded (valid UTF-8) segment string, split on the
/// single-byte element separator, so this reconstruction is lossless.
fn bytes_to_string(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A minimal valid ISA header (default `*` element, `:` sub-element,
    /// `~` terminator), 106 bytes, followed by a GS segment.
    fn isa_header() -> String {
        // Build the canonical 106-byte ISA with `*` separators and `~`
        // terminator; the control number is "000000001".
        let isa = "ISA*00*          *00*          *ZZ*SENDER         \
                   *ZZ*RECEIVER       *240101*1200*U*00401*000000001*0*P*:~";
        assert_eq!(isa.len(), ISA_LEN, "ISA fixture must be 106 bytes");
        isa.to_string()
    }

    fn tok(data: &[u8]) -> SegmentTokenizer<Cursor<Vec<u8>>> {
        SegmentTokenizer::new(Cursor::new(data.to_vec()), Charset::Utf8)
    }

    #[test]
    fn isa_header_declares_delimiters() {
        let data = format!("{}GS*PO*S*R*20240101*1200*1*X*004010~", isa_header());
        let mut t = tok(data.as_bytes());
        let header = t.read_isa_header().unwrap();
        let d = t.delimiters();
        assert_eq!(d.element, b'*');
        assert_eq!(d.subelement, b':');
        assert_eq!(d.terminator, b'~');
        let parsed = split_isa(&header, &d, t.charset()).unwrap();
        assert_eq!(parsed.tag, "ISA");
        assert_eq!(parsed.elements.len(), 16, "ISA carries 16 elements");
        // ISA13 (interchange control number) is element index 12.
        assert_eq!(parsed.elements[12], "000000001");
        // ISA16 (the sub-element separator) is the last element.
        assert_eq!(parsed.elements[15], ":");
    }

    #[test]
    fn custom_delimiters_discovered_from_isa() {
        // Element separator '|', sub-element '^', terminator '\n'. Build a
        // 106-byte ISA using those bytes.
        let isa = "ISA|00|          |00|          |ZZ|SENDER         \
                   |ZZ|RECEIVER       |240101|1200|U|00401|000000001|0|P|^\n";
        assert_eq!(isa.len(), ISA_LEN);
        let mut t = tok(isa.as_bytes());
        let _ = t.read_isa_header().unwrap();
        let d = t.delimiters();
        assert_eq!(d.element, b'|');
        assert_eq!(d.subelement, b'^');
        assert_eq!(d.terminator, b'\n');
    }

    #[test]
    fn next_segment_after_isa_reads_gs() {
        let data = format!("{}GS*PO*S*R*20240101*1200*1*X*004010~", isa_header());
        let mut t = tok(data.as_bytes());
        let _ = t.read_isa_header().unwrap();
        let gs = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&gs, &t.delimiters());
        assert_eq!(parsed.tag, "GS");
        assert_eq!(parsed.elements[0], "PO");
    }

    #[test]
    fn composite_subelement_kept_verbatim() {
        let data = format!("{}REF*ZZ*A:B:C~", isa_header());
        let mut t = tok(data.as_bytes());
        let _ = t.read_isa_header().unwrap();
        let seg = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&seg, &t.delimiters());
        assert_eq!(parsed.tag, "REF");
        assert_eq!(parsed.elements, vec!["ZZ".to_string(), "A:B:C".to_string()]);
    }

    #[test]
    fn latin1_high_byte_element_decoded() {
        // A REF segment whose element carries the Latin-1 byte 0xE9 (é)
        // decodes under the Latin-1 charset but would be rejected as
        // invalid UTF-8 under the default.
        let mut data = isa_header().into_bytes();
        data.extend_from_slice(b"REF*ZZ*Caf");
        data.push(0xE9);
        data.push(b'~');
        let mut t = SegmentTokenizer::new(Cursor::new(data), Charset::Latin1);
        let _ = t.read_isa_header().unwrap();
        let seg = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&seg, &t.delimiters());
        assert_eq!(parsed.tag, "REF");
        assert_eq!(parsed.elements, vec!["ZZ".to_string(), "Café".to_string()]);
    }

    #[test]
    fn latin1_isa_decode_consistent_with_segments() {
        // A Latin-1 tokenizer decodes both the ISA header and the body
        // through the same repertoire; the ASCII control header is
        // unaffected and the high-byte body element decodes.
        let mut data = isa_header().into_bytes();
        data.extend_from_slice(b"REF*N1*A");
        data.push(0xF1);
        data.push(b'o');
        data.push(b'~');
        let mut t = SegmentTokenizer::new(Cursor::new(data), Charset::Latin1);
        let header = t.read_isa_header().unwrap();
        let isa = split_isa(&header, &t.delimiters(), t.charset()).unwrap();
        assert_eq!(isa.elements[12], "000000001");
        let seg = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&seg, &t.delimiters());
        assert_eq!(parsed.elements, vec!["N1".to_string(), "Año".to_string()]);
    }

    #[test]
    fn high_byte_element_rejected_under_utf8() {
        let mut data = isa_header().into_bytes();
        data.extend_from_slice(b"REF*ZZ*Caf");
        data.push(0xE9);
        data.push(b'~');
        let mut t = SegmentTokenizer::new(Cursor::new(data), Charset::Utf8);
        let _ = t.read_isa_header().unwrap();
        let err = t.next_segment().unwrap_err();
        assert!(matches!(err, FormatError::Charset(m) if m.contains("not valid UTF-8")));
    }

    #[test]
    fn crlf_between_segments_stripped() {
        let data = format!(
            "{}GS*PO*S*R*20240101*1200*1*X*004010~\r\nST*850*0001~\r\n",
            isa_header()
        );
        let mut t = tok(data.as_bytes());
        let _ = t.read_isa_header().unwrap();
        let gs = t.next_segment().unwrap().unwrap();
        assert!(gs.starts_with("GS*"));
        let st = t.next_segment().unwrap().unwrap();
        assert_eq!(st, "ST*850*0001");
        assert!(t.next_segment().unwrap().is_none());
    }

    #[test]
    fn truncated_isa_errors() {
        let mut t = tok(b"ISA*00*  ");
        let err = t.read_isa_header().unwrap_err();
        assert!(matches!(err, FormatError::X12(m) if m.contains("truncated ISA")));
    }

    #[test]
    fn non_isa_start_errors() {
        // 106 bytes that do not begin with ISA.
        let data = "X".repeat(ISA_LEN);
        let mut t = tok(data.as_bytes());
        let err = t.read_isa_header().unwrap_err();
        assert!(matches!(err, FormatError::X12(m) if m.contains("must begin with an ISA")));
    }

    #[test]
    fn unterminated_final_segment_errors() {
        let data = format!("{}GS*PO*S*R*20240101*1200*1*X*004010", isa_header());
        let mut t = tok(data.as_bytes());
        let _ = t.read_isa_header().unwrap();
        let err = t.next_segment().unwrap_err();
        assert!(matches!(err, FormatError::X12(m) if m.contains("truncated")));
    }

    #[test]
    fn unterminated_segment_hits_byte_cap() {
        let data = format!("{}{}", isa_header(), "A".repeat(MAX_SEGMENT_BYTES + 10));
        let mut t = tok(data.as_bytes());
        let _ = t.read_isa_header().unwrap();
        let err = t.next_segment().unwrap_err();
        assert!(matches!(err, FormatError::X12(m) if m.contains("cap")));
    }

    #[test]
    fn delimiter_doc_value_round_trips() {
        let d = Delimiters {
            element: b'|',
            subelement: b'^',
            terminator: b'\n',
        };
        assert_eq!(
            d.to_doc_value(),
            Value::Array(vec![
                Value::Integer(124),
                Value::Integer(94),
                Value::Integer(10),
            ])
        );
        assert_eq!(Delimiters::from_doc_value(&d.to_doc_value()).unwrap(), d);
    }

    #[test]
    fn delimiter_doc_value_rejects_malformed_shapes() {
        // Non-array carrier.
        assert!(Delimiters::from_doc_value(&Value::String("junk".into())).is_err());
        // Wrong arity.
        assert!(
            Delimiters::from_doc_value(&Value::Array(
                vec![Value::Integer(42), Value::Integer(58),]
            ))
            .is_err()
        );
        // Out-of-byte-range integer.
        assert!(
            Delimiters::from_doc_value(&Value::Array(vec![
                Value::Integer(300),
                Value::Integer(58),
                Value::Integer(126),
            ]))
            .is_err()
        );
        // Non-integer element.
        assert!(
            Delimiters::from_doc_value(&Value::Array(vec![
                Value::String("*".into()),
                Value::Integer(58),
                Value::Integer(126),
            ]))
            .is_err()
        );
    }

    #[test]
    fn isa_split_across_buffer_boundaries() {
        // A BufReader with a tiny capacity forces fill_buf to deliver the
        // 106-byte header in fragments; initialization must still
        // assemble the whole header.
        use std::io::BufReader;
        let data = format!("{}GS*PO*S*R*20240101*1200*1*X*004010~", isa_header());
        let inner = Cursor::new(data.into_bytes());
        let buffered = BufReader::with_capacity(16, inner);
        let mut t = SegmentTokenizer::new(buffered, Charset::Utf8);
        let header = t.read_isa_header().unwrap();
        assert_eq!(header.len(), ISA_LEN);
        assert_eq!(t.delimiters().element, b'*');
    }
}
