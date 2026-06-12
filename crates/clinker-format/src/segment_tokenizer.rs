//! Shared segment-framing scaffolding for the flat EDI readers (HL7 v2,
//! ANSI ASC X12, UN/EDIFACT).
//!
//! All three formats are byte streams of terminator-delimited segments with
//! the same low-level framing concerns: producers sprinkle inter-segment
//! `\r`/`\n` for readability, a single segment must not grow without bound
//! when a terminator goes missing, and the readability newlines that bracket
//! a raw segment body are insignificant. Only the *policy* differs between
//! formats — the terminator byte, whether a release character escapes an
//! embedded terminator, the byte cap, and what an EOF with bytes still
//! pending means (a producer that dropped the final terminator versus a
//! truncated stream). This module owns the framing loop and lets each format
//! supply its policy, so a fix or a cap change lands in one place instead of
//! drifting across three copies.
//!
//! Escape/release *decoding* and delimiter *discovery* stay in each format's
//! own tokenizer: this layer never inspects field structure, it only finds
//! segment boundaries. HL7 composite (`^`), repetition (`~`), and
//! sub-component (`&`) separators therefore pass through framing untouched.

use std::io::BufRead;

use crate::error::FormatError;

/// What a clean end-of-stream with bytes still pending means for a format.
///
/// HL7 producers commonly drop the trailing carriage return on the final
/// segment, so a non-empty pending buffer at EOF is a legitimate last
/// segment. X12 and EDIFACT require every segment to carry its terminator,
/// so the same condition is a truncated interchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TrailingSegment {
    /// Accept a terminator-less final segment as the producer's last record.
    AcceptUnterminated,
    /// Reject a terminator-less final segment as truncation.
    RejectUnterminated,
}

/// The per-format framing policy consumed by [`read_raw_segment`].
///
/// Carries the resolved terminator byte (fixed for HL7, discovered from the
/// header for X12/EDIFACT), the optional release character that escapes an
/// embedded terminator (EDIFACT only), the per-segment byte cap, and how to
/// treat an unterminated final segment.
#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentFraming {
    /// The byte that ends a segment.
    pub terminator: u8,
    /// A release character that, immediately preceding the terminator (or any
    /// byte), makes that byte literal data rather than a boundary. `None` for
    /// formats with no release mechanism (HL7, X12).
    pub release: Option<u8>,
    /// Hard ceiling on a single segment's raw byte length, so a missing
    /// terminator fails fast instead of buffering the whole stream.
    pub max_segment_bytes: usize,
    /// Whether a terminator-less final segment is the producer's last record
    /// or a truncation error.
    pub trailing: TrailingSegment,
}

/// Consume any inter-segment newline whitespace (`\r`/`\n`) that producers
/// add between segments for readability. Stops at the first byte that could
/// begin a segment tag.
///
/// # Errors
///
/// Propagates an I/O error from the underlying buffered reader as
/// [`FormatError::Io`].
pub(crate) fn skip_inter_segment_whitespace<R: BufRead>(reader: &mut R) -> Result<(), FormatError> {
    loop {
        let buf = reader.fill_buf()?;
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
        reader.consume(consumed);
    }
}

/// Drop the line-feed and carriage-return bytes that bracket a raw segment
/// body. EDI segments end in a single terminator byte, but producers (and
/// CRLF-normalizing transports) wrap readability newlines around it; that
/// whitespace is insignificant, so it is trimmed from the segment edges.
/// Interior bytes are left intact.
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

/// Read raw bytes up to (and consuming) the next unescaped terminator,
/// returning the edge-trimmed segment body or `None` at a clean end of
/// stream.
///
/// The terminator byte is consumed but not returned. When the framing policy
/// names a release character, a release byte makes the byte that follows it
/// literal data — both bytes are retained verbatim — so an escaped terminator
/// does not split the segment; the release mechanism is therefore preserved
/// for the format's own splitter to decode later. A release with nothing to
/// escape at the segment boundary is kept as literal data.
///
/// The caller supplies two error constructors so each format raises its own
/// [`FormatError`] variant with a precise message: `cap_error` for a segment
/// that exceeds the byte cap with no terminator, and `truncation_error` for
/// an unterminated final segment under [`TrailingSegment::RejectUnterminated`].
/// `truncation_error` receives the count of bytes read so far.
///
/// # Errors
///
/// Returns the `cap_error` result when a segment exceeds
/// [`SegmentFraming::max_segment_bytes`] without a terminator, the
/// `truncation_error` result on an unterminated final segment when the policy
/// rejects one, or [`FormatError::Io`] on an underlying read failure.
pub(crate) fn read_raw_segment<R: BufRead>(
    reader: &mut R,
    framing: &SegmentFraming,
    cap_error: impl FnOnce() -> FormatError,
    truncation_error: impl FnOnce(usize) -> FormatError,
) -> Result<Option<Vec<u8>>, FormatError> {
    let terminator = framing.terminator;
    let release = framing.release;

    let mut raw: Vec<u8> = Vec::new();
    // Holds the release byte when the previous input byte was an unescaped
    // release char, so the byte that follows it is taken as literal data.
    let mut pending_release: Option<u8> = None;

    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            // A release char dangling at EOF has nothing to escape and no
            // following byte to carry, so the segment ends exactly at the
            // bytes already accumulated — the dangling release is dropped,
            // never counted toward an empty-vs-pending decision.
            if raw.is_empty() {
                return Ok(None);
            }
            return match framing.trailing {
                TrailingSegment::AcceptUnterminated => Ok(Some(strip_edge_whitespace(&raw))),
                TrailingSegment::RejectUnterminated => Err(truncation_error(raw.len())),
            };
        }

        let mut consumed = 0;
        let mut finished = false;
        for &b in buf {
            consumed += 1;
            if let Some(rel) = pending_release.take() {
                // The previous byte was an unescaped release char; this byte
                // is literal regardless of its delimiter role. Both bytes are
                // kept verbatim so the format's splitter can decode the escape.
                raw.push(rel);
                raw.push(b);
                continue;
            }
            if release == Some(b) {
                pending_release = Some(b);
                continue;
            }
            if b == terminator {
                finished = true;
                break;
            }
            raw.push(b);
        }
        reader.consume(consumed);

        if raw.len() > framing.max_segment_bytes {
            return Err(cap_error());
        }

        // A terminator is reached only when no release escape is pending (the
        // escape branch above consumes the byte after a release before any
        // terminator test), so the finished segment never carries a dangling
        // release here.
        if finished {
            return Ok(Some(strip_edge_whitespace(&raw)));
        }
    }
}
