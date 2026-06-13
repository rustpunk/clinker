//! SWIFT MT block framer and block-4 tag-line splitter.
//!
//! A SWIFT MT message is a sequence of brace-balanced blocks
//! `{1:...}{2:...}{3:...}{4:...-}{5:...}`. Each top-level block opens with
//! `{`, carries a numeric block id and a colon, then a body, and closes with
//! the matching `}`. The body of blocks 3 and 5 may itself contain nested
//! `{tag:value}` sub-blocks (e.g. `{3:{108:REF}}`), so the framer tracks
//! brace depth rather than scanning for the first `}`. Block 4 — the message
//! text block — is special: its body is a run of `:tag:value` lines and it is
//! closed by the literal `-}` trailer rather than a bare brace.
//!
//! Unlike the flat EDI tokenizers (HL7/X12/EDIFACT) this framing is not
//! terminator-delimited, so the block framer is hand-rolled on brace depth.
//! Only the block-4 tag-line layer resembles a terminator scan: it splits the
//! block-4 body into [`ParsedBlock4Line`] entries on the `\r\n` / `\n` line
//! breaks that separate `:tag:value` lines.
//!
//! Structural separators (braces, the leading `:`, the `-}` trailer, the line
//! breaks) are kept out of the stored field values so a record carries clean
//! tag/value data; the writer re-frames them, keeping the reader → writer →
//! reader round-trip byte-faithful.
//!
//! Memory model: the framer reads one top-level block at a time into a
//! bounded scratch buffer. A hard per-block byte cap turns an unbalanced or
//! missing-brace stream (truncated / corrupt input) into an error rather than
//! letting the buffer grow without limit.

use std::io::BufRead;

use crate::error::FormatError;

/// Hard ceiling on a single top-level block's raw byte length. A real SWIFT
/// block 4 (the largest) is well under a few hundred kilobytes; this cap
/// exists only so a stream with an unbalanced brace fails fast instead of
/// buffering the whole input into one "block".
pub(crate) const MAX_BLOCK_BYTES: usize = 1024 * 1024;

/// The SWIFT block-4 trailer. Block 4 carries free `:tag:value` text whose
/// values may include a bare `}`, so it is closed by this two-byte sequence
/// rather than a single brace.
const BLOCK4_TRAILER: &[u8] = b"-}";

/// The numeric id of the message-text block (block 4), the only block whose
/// body is split into per-line records.
pub(crate) const TEXT_BLOCK_ID: u8 = 4;

/// One top-level SWIFT block: its numeric id and its raw body text.
///
/// `body` is the content between the `{n:` header and the closing `}` (or the
/// `-}` trailer for block 4), with the structural braces and the `-}` trailer
/// stripped but every interior byte — including the nested `{tag:value}`
/// sub-blocks of blocks 3 and 5 — kept verbatim. The block-4 body still
/// carries its `:tag:value` line structure, which [`split_block4`] resolves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedBlock {
    /// The numeric block id (`1`, `2`, `3`, `4`, `5`).
    pub id: u8,
    /// The block body with the framing braces / `-}` trailer removed.
    pub body: String,
}

/// One `:tag:value` line of a block-4 message-text body.
///
/// `tag` is the SWIFT field tag without its surrounding colons (`20`, `32A`,
/// `61`); `value` is the field text, with any continuation lines that do not
/// begin a new `:tag:` joined back in verbatim (multi-line `:86:` narrative,
/// for instance, keeps its embedded line breaks). The writer re-frames the
/// `:tag:value\r\n` wire shape from these, so the round-trip is byte-faithful.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedBlock4Line {
    /// The field tag without its surrounding colons.
    pub tag: String,
    /// The field value, continuation lines folded in verbatim.
    pub value: String,
}

/// Streaming SWIFT MT block framer.
///
/// Wraps a buffered source and yields one top-level [`ParsedBlock`] at a time
/// by tracking brace depth (and the block-4 `-}` trailer). The whole message
/// is never buffered — only the block currently being framed.
pub(crate) struct BlockTokenizer<R: BufRead> {
    reader: R,
}

impl<R: BufRead> BlockTokenizer<R> {
    /// Build a tokenizer over a buffered source.
    pub(crate) fn new(reader: R) -> Self {
        Self { reader }
    }

    /// Read the next top-level block, or `None` at a clean end of stream.
    ///
    /// Inter-block whitespace (`\r`/`\n`/spaces producers add for
    /// readability) is skipped before the opening `{`. The block id is the
    /// digit run between `{` and the first `:`; the body is the brace-balanced
    /// (or `-}`-terminated, for block 4) content that follows.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Swift`] when the next non-whitespace byte is not
    /// an opening brace, the block id is missing or non-numeric, the body
    /// exceeds [`MAX_BLOCK_BYTES`] without closing, or the stream ends before
    /// the block's closing brace / `-}` trailer (an unbalanced or truncated
    /// message).
    pub(crate) fn read_block(&mut self) -> Result<Option<ParsedBlock>, FormatError> {
        if !self.skip_inter_block_whitespace()? {
            return Ok(None);
        }
        let raw = self.read_balanced_block()?;
        parse_block(&raw)
    }

    /// Consume inter-block whitespace up to the next byte that could open a
    /// block. Returns `false` at a clean end of stream (no further block),
    /// `true` when a non-whitespace byte is waiting.
    fn skip_inter_block_whitespace(&mut self) -> Result<bool, FormatError> {
        loop {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                return Ok(false);
            }
            let consumed = buf
                .iter()
                .take_while(|&&b| b == b'\r' || b == b'\n' || b == b' ' || b == b'\t')
                .count();
            if consumed == 0 {
                // A non-whitespace byte is at the front of the buffer.
                return Ok(true);
            }
            self.reader.consume(consumed);
        }
    }

    /// Read one top-level block's raw bytes including the framing braces.
    ///
    /// The first byte must be `{`. A non-text block (1/2/3/5) closes on the
    /// brace that returns depth to zero, so its nested `{tag:value}`
    /// sub-blocks are tracked by depth. The message-text block (`{4:`) is
    /// different: its body is opaque line-structured free text — a SWIFT
    /// field value (a `:77E:` envelope, a `:79:` narrative, an `:86:`
    /// information line) legitimately carries `{`, `}`, and even `-}` as
    /// data. So once the `{4:` header is seen, brace counting stops entirely
    /// and the block closes only on a line-anchored `-}` trailer: a `-}` that
    /// begins a line (preceded by `\r\n`/`\n`, or at the very start of the
    /// body). Every interior `{`/`}`/non-anchored `-}` byte is data.
    fn read_balanced_block(&mut self) -> Result<Vec<u8>, FormatError> {
        let mut raw: Vec<u8> = Vec::new();
        let mut depth: usize = 0;
        let mut is_text_block = false;
        let mut header_seen = false;

        loop {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                return Err(FormatError::Swift(format!(
                    "SWIFT block truncated: reached end of input before the block's closing \
                     brace (read {} bytes, brace depth {depth})",
                    raw.len()
                )));
            }
            let mut consumed = 0;
            let mut finished = false;
            for &b in buf {
                consumed += 1;
                raw.push(b);

                if !header_seen {
                    if raw.len() == 1 && b != b'{' {
                        return Err(FormatError::Swift(format!(
                            "SWIFT message must begin with an opening brace '{{', found {:?}",
                            b as char
                        )));
                    }
                    if b == b':' {
                        // The `{4:` header marks the message-text block, whose
                        // free-text body is opaque from here on.
                        is_text_block = raw.starts_with(b"{4:");
                        header_seen = true;
                        // Block 4's opening `{` is the only structural brace;
                        // it counted toward depth, and the line-anchored `-}`
                        // trailer is what closes it.
                        continue;
                    }
                }

                if is_text_block {
                    // Opaque free text: close only on a line-anchored `-}`
                    // trailer. Interior braces and non-anchored `-}` are data.
                    if b == b'}' && ends_with_line_anchored_trailer(&raw) {
                        finished = true;
                        break;
                    }
                    continue;
                }

                match b {
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            finished = true;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            self.reader.consume(consumed);

            if raw.len() > MAX_BLOCK_BYTES {
                return Err(FormatError::Swift(format!(
                    "SWIFT block exceeded the {MAX_BLOCK_BYTES}-byte cap without a closing \
                     brace; the message is malformed or has an unbalanced brace"
                )));
            }
            if finished {
                return Ok(raw);
            }
        }
    }
}

/// Whether the accumulated block-4 bytes end in a line-anchored `-}` trailer:
/// a `-}` that begins a line. The trailer is line-anchored when the byte
/// immediately before the `-` is a line break (`\n` after a CRLF, or a lone
/// `\r`) or the `-}` sits at the very start of the block body (right after
/// the `{4:` header — an empty message-text block). A `-}` mid-line
/// (e.g. `:79:SEE-}NOTE`) is data, not a frame boundary, so it is not
/// anchored.
fn ends_with_line_anchored_trailer(raw: &[u8]) -> bool {
    let Some(before_trailer) = raw.len().checked_sub(BLOCK4_TRAILER.len()) else {
        return false;
    };
    if &raw[before_trailer..] != BLOCK4_TRAILER {
        return false;
    }
    // The trailer begins its own line when the byte before the `-` is a line
    // break, or the `-}` sits at the body start (right after the three-byte
    // `{4:` header — an empty message-text block).
    const HEADER_LEN: usize = "{4:".len();
    if before_trailer == HEADER_LEN {
        return true;
    }
    matches!(raw.get(before_trailer - 1), Some(b'\n') | Some(b'\r'))
}

/// Parse one raw top-level block (`{n:body}` or `{4:body-}`) into its id and
/// body, stripping the framing braces and the block-4 `-}` trailer.
///
/// # Errors
///
/// Returns [`FormatError::Swift`] when the block is not brace-framed, has no
/// `:` after the id, or carries a missing/non-numeric/out-of-range block id.
fn parse_block(raw: &[u8]) -> Result<Option<ParsedBlock>, FormatError> {
    if raw.first() != Some(&b'{') || raw.last() != Some(&b'}') {
        return Err(FormatError::Swift(
            "SWIFT block is not enclosed in braces; expected '{n:...}'".into(),
        ));
    }
    let inner = &raw[1..raw.len() - 1];
    let colon = inner.iter().position(|&b| b == b':').ok_or_else(|| {
        FormatError::Swift("SWIFT block has no ':' after the block id; expected '{n:...}'".into())
    })?;
    let id_bytes = &inner[..colon];
    if id_bytes.is_empty() || !id_bytes.iter().all(u8::is_ascii_digit) {
        return Err(FormatError::Swift(format!(
            "SWIFT block id {:?} is not a number; expected a numeric block id (1-5)",
            String::from_utf8_lossy(id_bytes)
        )));
    }
    let id: u8 = std::str::from_utf8(id_bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| {
            FormatError::Swift(format!(
                "SWIFT block id {:?} is out of range; expected 1-5",
                String::from_utf8_lossy(id_bytes)
            ))
        })?;
    let mut body_bytes = &inner[colon + 1..];
    // Block 4's body keeps its `:tag:value` lines but loses the `-}` trailer.
    if id == TEXT_BLOCK_ID && body_bytes.ends_with(b"-") {
        body_bytes = &body_bytes[..body_bytes.len() - 1];
    }
    let body = String::from_utf8(body_bytes.to_vec()).map_err(|e| {
        FormatError::Swift(format!(
            "SWIFT block {id} body is not valid UTF-8: {e}. Non-UTF-8 SWIFT messages are not \
             supported"
        ))
    })?;
    Ok(Some(ParsedBlock { id, body }))
}

/// Split a block-4 body into its `:tag:value` lines.
///
/// Each line of the form `:tag:value` begins a new field; any line that does
/// not begin with `:` continues the current field's value, **including a
/// blank line** — a blank line inside a multi-line narrative (`:77E:`,
/// `:79:`) is part of the value and is folded in verbatim so the
/// read → write → read round-trip is byte-faithful. The single line break
/// that frames the body (the `\r\n` between the last field and the `-}`
/// trailer) is structural and dropped; leading blank lines before the first
/// field are likewise insignificant framing.
///
/// # Errors
///
/// Returns [`FormatError::Swift`] when a `:tag:` opener has no second colon
/// closing the tag, or carries an empty tag — a malformed message-text line.
pub(crate) fn split_block4(body: &str) -> Result<Vec<ParsedBlock4Line>, FormatError> {
    // The body ends with the structural line break that precedes the `-}`
    // trailer. Drop exactly that one break so its trailing empty segment is
    // not folded as a spurious blank continuation onto the final field; an
    // interior blank line inside a field still survives the split below.
    let body = body
        .strip_suffix('\n')
        .map(|b| b.strip_suffix('\r').unwrap_or(b))
        .unwrap_or(body);

    let mut lines: Vec<ParsedBlock4Line> = Vec::new();
    for segment in body.split('\n') {
        let segment = segment.strip_suffix('\r').unwrap_or(segment);
        if let Some(rest) = segment.strip_prefix(':') {
            let tag_end = rest.find(':').ok_or_else(|| {
                FormatError::Swift(format!(
                    "SWIFT block-4 line {segment:?} has no ':' closing the field tag; expected \
                     ':tag:value'"
                ))
            })?;
            let tag = &rest[..tag_end];
            if tag.is_empty() {
                return Err(FormatError::Swift(format!(
                    "SWIFT block-4 line {segment:?} has an empty field tag; expected ':tag:value'"
                )));
            }
            let value = &rest[tag_end + 1..];
            lines.push(ParsedBlock4Line {
                tag: tag.to_string(),
                value: value.to_string(),
            });
        } else if let Some(last) = lines.last_mut() {
            // A continuation line of the current field's value (blank or not).
            // Re-join the line break the split removed so the value — and any
            // interior blank line within it — is byte-faithful.
            last.value.push('\n');
            last.value.push_str(segment);
        } else if !segment.is_empty() {
            return Err(FormatError::Swift(format!(
                "SWIFT block-4 body opens with a continuation line {segment:?} before any \
                 ':tag:value' field"
            )));
        }
        // A leading blank segment before the first field is structural
        // whitespace (the `\r\n` after the `{4:` opener) — skipped silently.
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;
    use std::io::Cursor;

    /// A minimal single-customer-credit-transfer MT103: basic header,
    /// application header, user header with a nested service-id sub-block, the
    /// message text block, and a trailer block.
    const MT103: &str = "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}\
        {3:{108:MSGREF12345}}\
        {4:\r\n:20:REFERENCE12345\r\n:23B:CRED\r\n:32A:240101USD1000,00\r\n\
        :50K:/12345678\r\nJOHN DOE\r\n:59:/98765432\r\nJANE SMITH\r\n-}\
        {5:{CHK:1234567890AB}}";

    fn tok(data: &str) -> BlockTokenizer<BufReader<Cursor<Vec<u8>>>> {
        BlockTokenizer::new(BufReader::new(Cursor::new(data.as_bytes().to_vec())))
    }

    fn collect_blocks(data: &str) -> Vec<ParsedBlock> {
        let mut t = tok(data);
        let mut out = Vec::new();
        while let Some(block) = t.read_block().unwrap() {
            out.push(block);
        }
        out
    }

    #[test]
    fn frames_all_five_blocks_of_an_mt103() {
        let blocks = collect_blocks(MT103);
        let ids: Vec<u8> = blocks.iter().map(|b| b.id).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
        assert_eq!(blocks[0].body, "F01BANKBEBBAXXX0000000000");
        assert_eq!(blocks[1].body, "I103BANKDEFFXXXXN");
    }

    #[test]
    fn nested_subblock_in_block3_stays_intact() {
        let blocks = collect_blocks(MT103);
        // Block 3 keeps its nested {108:...} sub-block verbatim.
        assert_eq!(blocks[2].id, 3);
        assert_eq!(blocks[2].body, "{108:MSGREF12345}");
    }

    #[test]
    fn nested_subblock_in_block5_stays_intact() {
        let blocks = collect_blocks(MT103);
        assert_eq!(blocks[4].id, 5);
        assert_eq!(blocks[4].body, "{CHK:1234567890AB}");
    }

    #[test]
    fn block4_terminated_by_dash_brace_not_bare_brace() {
        let blocks = collect_blocks(MT103);
        let block4 = &blocks[3];
        assert_eq!(block4.id, 4);
        // The `-}` trailer is stripped; the `:tag:value` lines remain.
        assert!(block4.body.contains(":20:REFERENCE12345"));
        assert!(!block4.body.contains("-}"));
    }

    #[test]
    fn block4_split_yields_one_line_per_tag() {
        let blocks = collect_blocks(MT103);
        let lines = split_block4(&blocks[3].body).unwrap();
        let tags: Vec<&str> = lines.iter().map(|l| l.tag.as_str()).collect();
        assert_eq!(tags, vec!["20", "23B", "32A", "50K", "59"]);
        assert_eq!(lines[0].value, "REFERENCE12345");
        assert_eq!(lines[2].value, "240101USD1000,00");
    }

    #[test]
    fn block4_folds_multiline_continuation_values() {
        let blocks = collect_blocks(MT103);
        let lines = split_block4(&blocks[3].body).unwrap();
        // :50K: carries an account line then a name continuation line.
        let f50k = lines.iter().find(|l| l.tag == "50K").unwrap();
        assert_eq!(f50k.value, "/12345678\nJOHN DOE");
        let f59 = lines.iter().find(|l| l.tag == "59").unwrap();
        assert_eq!(f59.value, "/98765432\nJANE SMITH");
    }

    #[test]
    fn missing_trailing_newline_still_frames() {
        // No trailing whitespace after the final block.
        let data = "{1:F01X}{4:\r\n:20:REF\r\n-}";
        let blocks = collect_blocks(data);
        assert_eq!(blocks.len(), 2);
        let lines = split_block4(&blocks[1].body).unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].tag, "20");
        assert_eq!(lines[0].value, "REF");
    }

    #[test]
    fn truncated_open_brace_errors() {
        let data = "{1:F01X}{2:I103";
        let mut t = tok(data);
        let _ = t.read_block().unwrap(); // block 1
        let err = t.read_block().unwrap_err();
        assert!(matches!(err, FormatError::Swift(m) if m.contains("truncated")));
    }

    #[test]
    fn missing_block4_trailer_errors() {
        // Block 4 with no `-}` trailer runs to EOF unbalanced.
        let data = "{1:F01X}{4:\r\n:20:REF\r\n";
        let mut t = tok(data);
        let _ = t.read_block().unwrap();
        let err = t.read_block().unwrap_err();
        assert!(matches!(err, FormatError::Swift(m) if m.contains("truncated")));
    }

    #[test]
    fn non_brace_start_errors() {
        let data = "garbage";
        let mut t = tok(data);
        let err = t.read_block().unwrap_err();
        assert!(matches!(err, FormatError::Swift(m) if m.contains("opening brace")));
    }

    #[test]
    fn non_numeric_block_id_errors() {
        let data = "{X:body}";
        let mut t = tok(data);
        let err = t.read_block().unwrap_err();
        assert!(matches!(err, FormatError::Swift(m) if m.contains("not a number")));
    }

    #[test]
    fn block4_bare_brace_in_value_is_data_not_a_close() {
        // A `}` inside block-4 free text must not close the block early; only
        // a line-anchored `-}` does. Here a value carries a literal `}`.
        let data = "{4:\r\n:20:REF}WITHBRACE\r\n-}";
        let blocks = collect_blocks(data);
        assert_eq!(blocks.len(), 1);
        let lines = split_block4(&blocks[0].body).unwrap();
        assert_eq!(lines[0].value, "REF}WITHBRACE");
    }

    #[test]
    fn block4_open_brace_in_value_is_data_not_counted() {
        // A `{` inside a block-4 field value (legitimate in `:79:`/`:77E:`
        // narrative) must NOT inflate brace depth — block 4 is opaque free
        // text, so the real `-}` still closes it. A `{`-counting framer would
        // reject this whole message as truncated.
        let data = "{1:F01X}{4:\r\n:79:A{B\r\n:86:C{D{E\r\n-}";
        let blocks = collect_blocks(data);
        assert_eq!(blocks.len(), 2);
        let lines = split_block4(&blocks[1].body).unwrap();
        assert_eq!(lines[0].tag, "79");
        assert_eq!(lines[0].value, "A{B");
        assert_eq!(lines[1].tag, "86");
        assert_eq!(lines[1].value, "C{D{E");
    }

    #[test]
    fn block4_mid_line_dash_brace_is_data_not_a_close() {
        // A `-}` in the MIDDLE of a value (not beginning a line) is data, not
        // the trailer; only a line-anchored `-}` closes the block. The block
        // must continue past the mid-line `-}` to its real line-anchored one.
        let data = "{4:\r\n:79:SEE-}NOTE\r\n:86:MORE\r\n-}";
        let blocks = collect_blocks(data);
        assert_eq!(blocks.len(), 1);
        let lines = split_block4(&blocks[0].body).unwrap();
        assert_eq!(lines.len(), 2, "block must not close at a mid-line trailer");
        assert_eq!(lines[0].tag, "79");
        assert_eq!(lines[0].value, "SEE-}NOTE");
        assert_eq!(lines[1].tag, "86");
        assert_eq!(lines[1].value, "MORE");
    }

    #[test]
    fn block4_interior_blank_line_in_value_survives() {
        // A blank line WITHIN a multi-line narrative value is part of the
        // value, not framing — it must fold in so the round-trip is faithful.
        let data = "{4:\r\n:77E:LINE1\r\n\r\nLINE3\r\n-}";
        let blocks = collect_blocks(data);
        let lines = split_block4(&blocks[0].body).unwrap();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].tag, "77E");
        assert_eq!(lines[0].value, "LINE1\n\nLINE3");
    }

    #[test]
    fn block4_line_without_closing_tag_colon_errors() {
        let err = split_block4(":20").unwrap_err();
        assert!(matches!(err, FormatError::Swift(m) if m.contains("closing the field tag")));
    }

    #[test]
    fn block4_empty_tag_errors() {
        let err = split_block4("::value").unwrap_err();
        assert!(matches!(err, FormatError::Swift(m) if m.contains("empty field tag")));
    }
}
