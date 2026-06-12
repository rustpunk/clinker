//! HL7 v2 segment tokenizer: MSH-driven delimiter discovery and
//! escape-aware splitting.
//!
//! An HL7 v2 file is a flat byte stream of segments terminated by a
//! carriage return. Each segment splits into a tag and data fields on the
//! field separator; a field splits into components on the component
//! separator, repetitions on the repetition separator, and sub-components
//! on the sub-component separator. The delimiters are declared in the
//! `MSH` header rather than assumed: the field separator is the single
//! byte immediately after the `MSH` tag (`MSH-1`), and the four encoding
//! characters (`MSH-2`) are the bytes that follow — component, repetition,
//! escape, sub-component — up to the next field separator.
//!
//! The escape character introduces an escape sequence `\X\`, where `X`
//! names a delimiter (`F` field, `S` component, `T` sub-component, `R`
//! repetition, `E` escape itself). The reader decodes these into their
//! literal data byte so downstream consumers see clean data; the writer
//! re-escapes on output, so a reader → writer → reader round-trip is
//! byte-faithful. Component, repetition, and sub-component separators are
//! *not* split or decoded — field text keeps them verbatim, so a composite
//! field `A^B^C` stays one field string. The positional field model
//! deliberately works above component resolution.
//!
//! Memory model: the tokenizer reads one segment at a time from a buffered
//! source into a bounded scratch buffer. A hard per-segment byte cap turns
//! a missing terminator (truncated / corrupt input) into an error rather
//! than letting the buffer grow without limit.

use std::io::{BufRead, Read};

use crate::error::FormatError;

/// Hard ceiling on a single segment's raw byte length. A real HL7 segment
/// is well under a few kilobytes; this cap exists only so a stream with no
/// carriage-return terminator fails fast instead of buffering the whole
/// file into one "segment".
pub(crate) const MAX_SEGMENT_BYTES: usize = 256 * 1024;

/// The HL7 segment terminator. Always a carriage return — never
/// producer-chosen, unlike the field/encoding delimiters.
const SEGMENT_TERMINATOR: u8 = b'\r';

/// The four HL7 service delimiters discovered from the `MSH` header (the
/// field separator plus the four encoding characters).
///
/// The component, repetition, and sub-component separators are stored but
/// never split on at the tokenizer level: field text is kept verbatim, so
/// a composite or repeating field rides inside one field string intact.
/// Only the escape character is interpreted, because that is what
/// distinguishes a delimiter byte from literal data.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Delimiters {
    /// Field separator (`MSH-1`), between fields of a segment.
    pub field: u8,
    /// Component separator (first encoding char), within a field.
    pub component: u8,
    /// Repetition separator (second encoding char), within a field.
    pub repetition: u8,
    /// Escape character (third encoding char), introducing a `\X\`
    /// escape sequence.
    pub escape: u8,
    /// Sub-component separator (fourth encoding char), within a component.
    pub subcomponent: u8,
}

impl Delimiters {
    /// The conventional HL7 v2 default delimiter set — field `|`,
    /// component `^`, repetition `~`, escape `\`, sub-component `&`. Used
    /// as a placeholder before the `MSH` header is read and as the writer's
    /// default when no header dictates otherwise.
    pub(crate) fn default_set() -> Self {
        Self {
            field: b'|',
            component: b'^',
            repetition: b'~',
            escape: b'\\',
            subcomponent: b'&',
        }
    }
}

/// Streaming HL7 segment reader.
///
/// Wraps a buffered source and yields one raw segment string at a time
/// (terminator stripped, inter-segment newline whitespace stripped). The
/// delimiters are discovered once from the `MSH` header on the first read.
pub(crate) struct SegmentTokenizer<R: Read> {
    reader: R,
    delimiters: Delimiters,
    initialized: bool,
}

impl<R: BufRead> SegmentTokenizer<R> {
    /// Build a tokenizer over a buffered source. Delimiter discovery is
    /// deferred to the first [`Self::read_first_segment`] call so the `MSH`
    /// header scan and the first segment read share one pass.
    pub(crate) fn new(reader: R) -> Self {
        Self {
            reader,
            delimiters: Delimiters::default_set(),
            initialized: false,
        }
    }

    /// The active delimiter set, valid only after the first read has
    /// resolved the `MSH` header.
    pub(crate) fn delimiters(&self) -> Delimiters {
        self.delimiters
    }

    /// Consume any inter-segment newline whitespace (`\r`/`\n`) that some
    /// producers add between segments for readability. Stops at the first
    /// byte that could begin a segment tag.
    fn skip_inter_segment_whitespace(&mut self) -> Result<(), FormatError> {
        loop {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                return Ok(());
            }
            let mut consumed = 0;
            for &b in buf {
                if b == b'\r' || b == b'\n' {
                    consumed += 1;
                } else {
                    break;
                }
            }
            if consumed == 0 {
                return Ok(());
            }
            self.reader.consume(consumed);
        }
    }

    /// Read the first segment — an `MSH` message header, or an `FHS` file
    /// header or `BHS` batch header that wraps it — resolving the delimiter
    /// set from it and returning the raw segment text.
    ///
    /// All three header segments share the same delimiter-declaring layout:
    /// the field separator is the single byte after the three-character tag,
    /// and the four encoding characters follow it up to the next field
    /// separator. HL7 requires a file's `FHS`/`BHS` encoding characters to
    /// match its messages' `MSH`, so reading them from whichever header
    /// leads the file yields the same delimiter set. The reader's
    /// `ensure_initialized` guards against a second call, so this runs
    /// exactly once before any [`Self::next_segment`].
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Hl7`] when the stream does not begin with an
    /// `MSH`/`FHS`/`BHS` header carrying at least the field separator and
    /// four encoding characters (a non-HL7 or truncated stream leaves the
    /// delimiter set undefined, so nothing downstream can be tokenized).
    pub(crate) fn read_first_segment(&mut self) -> Result<String, FormatError> {
        self.skip_inter_segment_whitespace()?;

        // The header's delimiters are not yet known, so the first segment is
        // read up to the fixed carriage-return terminator without any escape
        // interpretation: the escape character itself is declared inside
        // this very segment.
        let raw = self.read_raw_until_terminator()?.ok_or_else(|| {
            FormatError::Hl7(
                "empty input: no header segment found; the file is empty or not HL7 v2".into(),
            )
        })?;

        let starts_with_header =
            raw.starts_with(b"MSH") || raw.starts_with(b"FHS") || raw.starts_with(b"BHS");
        if !starts_with_header {
            return Err(FormatError::Hl7(format!(
                "HL7 v2 file must begin with an MSH, FHS, or BHS header segment, found {:?}",
                String::from_utf8_lossy(&raw[..3.min(raw.len())])
            )));
        }
        // After the tag: byte 3 is the field separator, bytes 4..8 are the
        // encoding characters (component, repetition, escape, sub-component).
        // The encoding-characters field ends at the next field separator, so
        // it must be at least four bytes wide.
        if raw.len() < 8 {
            return Err(FormatError::Hl7(format!(
                "truncated HL7 header: need a three-character tag plus a field separator and four \
                 encoding characters (8 bytes), read {}",
                raw.len()
            )));
        }
        let field = raw[3];
        let encoding = &raw[4..8];
        // The five service delimiters must be mutually distinct. A short
        // MSH-2 (e.g. `MSH|^~\|...`, which omits the sub-component char)
        // leaves `raw[7]` pointing at the next field separator, so a naive
        // read would set `subcomponent == field` and silently corrupt every
        // split. The byte after the four encoding characters (`raw[8]`, when
        // present) must be the field separator that ends MSH-2; reject a
        // header where it is not, or where any two delimiters collide.
        let delims = [field, encoding[0], encoding[1], encoding[2], encoding[3]];
        for (a, &da) in delims.iter().enumerate() {
            for &db in &delims[a + 1..] {
                if da == db {
                    return Err(FormatError::Hl7(format!(
                        "HL7 header declares colliding delimiters: the field separator and the \
                         four encoding characters (component, repetition, escape, sub-component) \
                         must be five distinct bytes, found {:?}. A short MSH-2 that omits an \
                         encoding character is the usual cause.",
                        String::from_utf8_lossy(&raw[3..8])
                    )));
                }
            }
        }
        // A conformant MSH-2 ends at the next field separator. If the byte
        // after the four encoding characters is present and is not the field
        // separator, the encoding-characters field is longer than four bytes
        // (an unsupported extended encoding set), which would misalign every
        // later field.
        if let Some(&after) = raw.get(8)
            && after != field
        {
            return Err(FormatError::Hl7(format!(
                "HL7 header MSH-2 encoding-characters field is not the expected four bytes \
                 terminated by the field separator; found {:?} after the four encoding \
                 characters. Extended encoding sets are not supported.",
                after as char
            )));
        }
        self.delimiters = Delimiters {
            field,
            component: encoding[0],
            repetition: encoding[1],
            escape: encoding[2],
            subcomponent: encoding[3],
        };
        self.initialized = true;

        let text = String::from_utf8(raw).map_err(|e| {
            FormatError::Hl7(format!(
                "MSH header is not valid UTF-8: {e}. Non-UTF-8 charsets are not supported"
            ))
        })?;
        Ok(text)
    }

    /// Read the next raw segment (carriage-return-delimited), or `None` at
    /// clean end of stream.
    ///
    /// The terminator byte is consumed but not returned. An escape sequence
    /// `\X\` is passed through verbatim (the escape char is not a segment
    /// boundary); decoding into literal data happens in [`split_segment`].
    /// Inter-segment newlines are dropped; an embedded escape sequence
    /// inside a field is preserved.
    pub(crate) fn next_segment(&mut self) -> Result<Option<String>, FormatError> {
        debug_assert!(
            self.initialized,
            "read_first_segment must run before next_segment"
        );
        self.skip_inter_segment_whitespace()?;
        let raw = match self.read_raw_until_terminator()? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };
        let text = String::from_utf8(raw).map_err(|e| {
            FormatError::Hl7(format!(
                "segment is not valid UTF-8: {e}. Non-UTF-8 charsets are not supported"
            ))
        })?;
        Ok(Some(text))
    }

    /// Read raw bytes up to (and consuming) the next carriage-return
    /// terminator. Returns `None` at a clean end of stream (empty pending
    /// buffer). A non-empty buffer at EOF is treated as a final segment
    /// whose trailing terminator the producer omitted — accepted rather
    /// than rejected, because HL7 producers commonly drop the last CR.
    ///
    /// The escape character is never interpreted here: a `\X\` sequence
    /// passes through verbatim because the terminator is the fixed carriage
    /// return, and HL7 escape decoding happens later in [`split_segment`].
    fn read_raw_until_terminator(&mut self) -> Result<Option<Vec<u8>>, FormatError> {
        let mut raw: Vec<u8> = Vec::new();
        loop {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                if raw.is_empty() {
                    return Ok(None);
                }
                // A final segment with no trailing CR is the common HL7
                // shape; strip surrounding newlines and accept it.
                return Ok(Some(strip_edge_whitespace(&raw)));
            }

            let mut consumed = 0;
            let mut finished = false;
            for &b in buf {
                consumed += 1;
                if b == SEGMENT_TERMINATOR {
                    finished = true;
                    break;
                }
                raw.push(b);
            }
            self.reader.consume(consumed);

            if raw.len() > MAX_SEGMENT_BYTES {
                return Err(FormatError::Hl7(format!(
                    "segment exceeded the {MAX_SEGMENT_BYTES}-byte cap without a carriage-return \
                     terminator; the file is malformed or not HL7 v2"
                )));
            }

            if finished {
                return Ok(Some(strip_edge_whitespace(&raw)));
            }
        }
    }
}

/// Drop a line-feed that brackets a raw segment body. HL7 segments end in
/// a bare carriage return, but producers (and CRLF-normalizing transports)
/// often add a line feed; that whitespace is insignificant, so it is
/// trimmed from the segment edges. Interior bytes are left intact.
fn strip_edge_whitespace(raw: &[u8]) -> Vec<u8> {
    let start = raw
        .iter()
        .position(|&b| b != b'\r' && b != b'\n')
        .unwrap_or(raw.len());
    let end = raw
        .iter()
        .rposition(|&b| b != b'\r' && b != b'\n')
        .map(|p| p + 1)
        .unwrap_or(start);
    raw[start..end].to_vec()
}

/// A parsed segment: its tag plus its decoded data fields.
///
/// Escape sequences are decoded into their literal data byte here, so a
/// field value carries clean data (a `\F\` on the wire becomes a literal
/// field-separator byte in the value). Downstream consumers — CSV/JSON
/// output, CXL string predicates, `$doc` fields — therefore see the data
/// the HL7 producer meant, never the wire escapes. The writer re-escapes
/// on output, so the reader → writer → reader round-trip is byte-faithful.
///
/// Component, repetition, and sub-component separators are *not* decoded or
/// split: field text keeps them verbatim, so a composite field `A^B^C`
/// stays one field string and a repeating field rides inside one string
/// intact. Only the escape mechanism is interpreted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedSegment {
    pub tag: String,
    pub fields: Vec<String>,
}

/// Split a raw segment string into its tag and decoded field values on the
/// field separator, honoring the escape character.
///
/// The tag is the first part; the remainder are data fields. The header
/// segments (`MSH`/`FHS`/`BHS`) are special: their field-separator-1 *is*
/// the delimiter, and their encoding-characters field (field index 0)
/// carries the component/repetition/escape/sub-component bytes verbatim —
/// those bytes are not interpreted as escapes there, so the
/// encoding-characters field round-trips intact. For every other field, an
/// escape sequence `\X\` decodes to the literal delimiter byte it names
/// (`\F\` → field, `\S\` → component, `\T\` → sub-component, `\R\` →
/// repetition, `\E\` → escape); an unrecognized or unterminated sequence is
/// kept verbatim so no data is lost.
pub(crate) fn split_segment(raw: &str, delims: &Delimiters) -> ParsedSegment {
    let field = delims.field;
    let bytes = raw.as_bytes();

    // Split on the field separator first (the field separator never appears
    // unescaped inside field data — a literal one is written `\F\`).
    let mut raw_parts: Vec<&[u8]> = Vec::new();
    let mut start = 0;
    for (i, &b) in bytes.iter().enumerate() {
        if b == field {
            raw_parts.push(&bytes[start..i]);
            start = i + 1;
        }
    }
    raw_parts.push(&bytes[start..]);

    // A delimiter-declaring header carries the escape character as literal
    // data in its encoding-characters field rather than as an escape
    // introducer, so that field is kept verbatim.
    let is_header = raw.starts_with("MSH") || raw.starts_with("FHS") || raw.starts_with("BHS");
    let mut parts = raw_parts.into_iter();
    let tag = parts
        .next()
        .map(|t| String::from_utf8_lossy(t).into_owned())
        .unwrap_or_default();
    let fields: Vec<String> = parts
        .enumerate()
        .map(|(idx, part)| {
            if is_header && idx == 0 {
                String::from_utf8_lossy(part).into_owned()
            } else {
                decode_escapes(part, delims)
            }
        })
        .collect();

    ParsedSegment { tag, fields }
}

/// Decode HL7 `\X\` escape sequences in a field's bytes into their literal
/// delimiter byte. A recognized sequence (`\F\ \S\ \T\ \R\ \E\`) is
/// replaced with the byte it names; an unrecognized or unterminated escape
/// run is kept verbatim, so application escapes (`\.br\`, `\Xdd..\`) and
/// malformed input survive rather than silently dropping data.
fn decode_escapes(bytes: &[u8], delims: &Delimiters) -> String {
    let escape = delims.escape;
    if !bytes.contains(&escape) {
        return String::from_utf8_lossy(bytes).into_owned();
    }
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != escape {
            out.push(bytes[i]);
            i += 1;
            continue;
        }
        // Look for a closing escape byte to delimit the sequence.
        if let Some(end_rel) = bytes[i + 1..].iter().position(|&b| b == escape) {
            let inner = &bytes[i + 1..i + 1 + end_rel];
            if let Some(byte) = escape_to_delimiter(inner, delims) {
                out.push(byte);
                i = i + 1 + end_rel + 1;
                continue;
            }
            // A `\Xhh..\` hexadecimal escape encodes raw data bytes (the
            // form the writer uses to escape an embedded carriage return so
            // it cannot split a segment). Decode it back to those bytes so
            // the round-trip is faithful.
            if let Some(mut decoded) = decode_hex_escape(inner) {
                out.append(&mut decoded);
                i = i + 1 + end_rel + 1;
                continue;
            }
            // Recognized escape framing but an application sequence we do
            // not decode (e.g. `\.br\`): keep the whole run verbatim.
            out.push(escape);
            i += 1;
            continue;
        }
        // No closing escape byte: a dangling escape char is literal data.
        out.push(escape);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Map a delimiter-escape sequence body (the bytes between the two escape
/// characters) to the literal delimiter byte it names, or `None` for any
/// sequence that is not one of the five standard delimiter escapes.
fn escape_to_delimiter(inner: &[u8], delims: &Delimiters) -> Option<u8> {
    match inner {
        b"F" => Some(delims.field),
        b"S" => Some(delims.component),
        b"T" => Some(delims.subcomponent),
        b"R" => Some(delims.repetition),
        b"E" => Some(delims.escape),
        _ => None,
    }
}

/// Decode an HL7 `\Xhh..\` hexadecimal escape body (the bytes between the
/// two escape characters, beginning with `X`) to the raw bytes it encodes.
/// Returns `None` when the body is not an `X` followed by an even-length run
/// of ASCII hex digits, so a non-hex application escape keeps its
/// verbatim-passthrough behavior.
fn decode_hex_escape(inner: &[u8]) -> Option<Vec<u8>> {
    let hex = inner.strip_prefix(b"X")?;
    if hex.is_empty() || !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for pair in hex.chunks_exact(2) {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out.push((hi * 16 + lo) as u8);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// A minimal MSH header with the conventional `|^~\&` delimiters.
    const MSH: &str =
        "MSH|^~\\&|SENDAPP|SENDFAC|RCVAPP|RCVFAC|20240101120000||ADT^A01|MSG001|P|2.5";

    fn tok(data: &[u8]) -> SegmentTokenizer<Cursor<Vec<u8>>> {
        SegmentTokenizer::new(Cursor::new(data.to_vec()))
    }

    #[test]
    fn msh_header_declares_delimiters() {
        let data = format!("{MSH}\rPID|1||PATID");
        let mut t = tok(data.as_bytes());
        let header = t.read_first_segment().unwrap();
        let d = t.delimiters();
        assert_eq!(d.field, b'|');
        assert_eq!(d.component, b'^');
        assert_eq!(d.repetition, b'~');
        assert_eq!(d.escape, b'\\');
        assert_eq!(d.subcomponent, b'&');
        let parsed = split_segment(&header, &d);
        assert_eq!(parsed.tag, "MSH");
        // MSH-2 (encoding chars) is field index 0, kept verbatim.
        assert_eq!(parsed.fields[0], "^~\\&");
        // MSH-9 (message type) is field index 7 (the MSH off-by-one).
        assert_eq!(parsed.fields[7], "ADT^A01");
        // MSH-10 (message control id) is field index 8.
        assert_eq!(parsed.fields[8], "MSG001");
    }

    #[test]
    fn custom_delimiters_discovered_from_msh() {
        // Field '#', component '@', repetition '*', escape '/', subcomponent '$'.
        let msh = "MSH#@*/$#SENDAPP#SENDFAC#RCVAPP#RCVFAC#202401##ADT@A01#M1#P#2.5";
        let data = format!("{msh}\r");
        let mut t = tok(data.as_bytes());
        let _ = t.read_first_segment().unwrap();
        let d = t.delimiters();
        assert_eq!(d.field, b'#');
        assert_eq!(d.component, b'@');
        assert_eq!(d.repetition, b'*');
        assert_eq!(d.escape, b'/');
        assert_eq!(d.subcomponent, b'$');
    }

    #[test]
    fn next_segment_after_msh_reads_pid() {
        let data = format!("{MSH}\rPID|1||PATID^^^MRN");
        let mut t = tok(data.as_bytes());
        let _ = t.read_first_segment().unwrap();
        let pid = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&pid, &t.delimiters());
        assert_eq!(parsed.tag, "PID");
        assert_eq!(parsed.fields[0], "1");
        // Composite field kept verbatim above component resolution.
        assert_eq!(parsed.fields[2], "PATID^^^MRN");
    }

    #[test]
    fn escape_sequences_decode_into_clean_values() {
        // OBX field carries an escaped field separator, escaped component
        // separator, and an escaped escape character.
        let data = format!("{MSH}\rOBX|1|TX|note||a\\F\\b\\S\\c\\E\\d");
        let mut t = tok(data.as_bytes());
        let _ = t.read_first_segment().unwrap();
        let obx = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&obx, &t.delimiters());
        assert_eq!(parsed.tag, "OBX");
        // \F\ -> '|', \S\ -> '^', \E\ -> '\'.
        assert_eq!(parsed.fields[4], "a|b^c\\d");
    }

    #[test]
    fn unrecognized_escape_kept_verbatim() {
        // \.br\ is a formatting escape this positional reader does not
        // decode; it must survive verbatim, not be dropped.
        let data = format!("{MSH}\rNTE|1||line one\\.br\\line two");
        let mut t = tok(data.as_bytes());
        let _ = t.read_first_segment().unwrap();
        let nte = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&nte, &t.delimiters());
        assert_eq!(parsed.fields[2], "line one\\.br\\line two");
    }

    #[test]
    fn newlines_between_segments_stripped() {
        let data = format!("{MSH}\r\nPID|1\r\nNTE|2\r\n");
        let mut t = tok(data.as_bytes());
        let _ = t.read_first_segment().unwrap();
        let pid = t.next_segment().unwrap().unwrap();
        assert_eq!(pid, "PID|1");
        let nte = t.next_segment().unwrap().unwrap();
        assert_eq!(nte, "NTE|2");
        assert!(t.next_segment().unwrap().is_none());
    }

    #[test]
    fn final_segment_without_terminator_accepted() {
        // HL7 producers commonly omit the trailing CR on the last segment.
        let data = format!("{MSH}\rPID|1");
        let mut t = tok(data.as_bytes());
        let _ = t.read_first_segment().unwrap();
        let pid = t.next_segment().unwrap().unwrap();
        assert_eq!(pid, "PID|1");
        assert!(t.next_segment().unwrap().is_none());
    }

    #[test]
    fn empty_input_errors() {
        let mut t = tok(b"");
        let err = t.read_first_segment().unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("empty input")));
    }

    #[test]
    fn non_msh_start_errors() {
        let mut t = tok(b"PID|1||X\r");
        let err = t.read_first_segment().unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("must begin with an MSH")));
    }

    #[test]
    fn truncated_msh_errors() {
        let mut t = tok(b"MSH|^\r");
        let err = t.read_first_segment().unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("truncated HL7 header")));
    }

    #[test]
    fn unterminated_segment_hits_byte_cap() {
        let data = format!("{MSH}\r{}", "A".repeat(MAX_SEGMENT_BYTES + 10));
        let mut t = tok(data.as_bytes());
        let _ = t.read_first_segment().unwrap();
        let err = t.next_segment().unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("cap")));
    }

    #[test]
    fn msh_split_across_buffer_boundaries() {
        use std::io::BufReader;
        let data = format!("{MSH}\rPID|1");
        let inner = Cursor::new(data.into_bytes());
        let buffered = BufReader::with_capacity(8, inner);
        let mut t = SegmentTokenizer::new(buffered);
        let header = t.read_first_segment().unwrap();
        assert!(header.starts_with("MSH"));
        assert_eq!(t.delimiters().field, b'|');
    }

    #[test]
    fn short_msh2_collides_with_field_separator_and_is_rejected() {
        // `MSH|^~\|...` has a three-character MSH-2 (`^~\`, no sub-component)
        // followed by the field separator. A naive `raw[4..8]` read would set
        // the sub-component separator to the field separator `|`, silently
        // corrupting every split. The collision must be rejected.
        let mut t = tok(b"MSH|^~\\|SENDAPP|SENDFAC|RCVAPP|RCVFAC|202401||ADT^A01|M1|P|2.5\r");
        let err = t.read_first_segment().unwrap_err();
        assert!(
            matches!(err, FormatError::Hl7(m) if m.contains("colliding delimiters")),
            "expected a colliding-delimiter rejection"
        );
    }

    #[test]
    fn duplicate_encoding_chars_rejected() {
        // Two encoding characters equal to each other (`^` repeated) is also
        // a collision the parser must reject.
        let mut t = tok(b"MSH|^^\\&|SENDAPP|SENDFAC|R|R|202401||ADT^A01|M1|P|2.5\r");
        let err = t.read_first_segment().unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("colliding delimiters")));
    }

    #[test]
    fn extended_msh2_beyond_four_chars_rejected() {
        // A MSH-2 wider than four bytes before the field separator is an
        // unsupported extended encoding set; rejecting it avoids silently
        // misaligning every later field.
        let mut t = tok(b"MSH|^~\\&#|SENDAPP|SENDFAC|R|R|202401||ADT^A01|M1|P|2.5\r");
        let err = t.read_first_segment().unwrap_err();
        assert!(matches!(err, FormatError::Hl7(m) if m.contains("not the expected four bytes")));
    }

    #[test]
    fn hex_escape_decodes_to_raw_byte() {
        // A `\X0D\` hex escape decodes back to a literal carriage return —
        // the form the writer uses to escape an embedded CR so it cannot
        // split a segment.
        let data = format!("{MSH}\rNTE|1||a\\X0D\\b");
        let mut t = tok(data.as_bytes());
        let _ = t.read_first_segment().unwrap();
        let nte = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&nte, &t.delimiters());
        assert_eq!(parsed.fields[2], "a\rb");
    }

    #[test]
    fn malformed_hex_escape_kept_verbatim() {
        // A non-hex `\X..\` body is not a valid hex escape and is preserved
        // verbatim rather than dropped.
        let data = format!("{MSH}\rNTE|1||a\\XZZ\\b");
        let mut t = tok(data.as_bytes());
        let _ = t.read_first_segment().unwrap();
        let nte = t.next_segment().unwrap().unwrap();
        let parsed = split_segment(&nte, &t.delimiters());
        assert_eq!(parsed.fields[2], "a\\XZZ\\b");
    }
}
