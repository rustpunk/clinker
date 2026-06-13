//! Character-set decoding for EDIFACT element text, selected in-band from
//! the `UNB` syntax identifier.
//!
//! Unlike X12 — which has no in-band repertoire marker and declares its
//! encoding out-of-band on the source — an EDIFACT interchange names its
//! repertoire on the wire: `UNB` data element S001 component 1 carries the
//! syntax identifier (`UNOA`, `UNOB`, `UNOC`, …). That identifier is always
//! ASCII, so the reader frames the `UNB` raw and reads the level directly
//! from the bytes ([`Charset::from_raw_unb_segment`]) before decoding
//! anything; it then decodes the whole `UNB` — including its own non-ASCII
//! sender/recipient identification — through the negotiated repertoire. The
//! writer derives the same repertoire from the `UNB` it emits and arms it
//! before writing the header. No element is ever processed under a wrong
//! repertoire, so the `UNB` round-trips byte-for-byte just like the body.
//!
//! Only repertoires whose byte↔codepoint mapping is total and
//! round-trippable in pure Rust are supported, so a read→write→read cycle is
//! byte-faithful for the negotiated charset. An unsupported syntax level
//! fails explicitly, naming the offending level, rather than guessing or
//! silently substituting replacement characters.

use crate::edifact::tokenizer::Delimiters;
use crate::error::FormatError;

/// The character repertoire used to decode and encode EDIFACT element text,
/// resolved from the `UNB` syntax identifier.
///
/// Decoding happens per segment (no whole-interchange buffering), so the
/// charset is consulted once per segment read rather than over the stream.
/// The syntax identifier that selects the repertoire is itself ASCII and is
/// read from the raw `UNB` bytes before any segment is decoded, so the
/// `UNB`'s own non-ASCII elements (sender / recipient identification under
/// UNOC or UNOY) decode and encode under the negotiated repertoire, never a
/// placeholder — a read → write → read cycle is byte-faithful end to end.
///
/// The [`Default`] is [`Charset::Utf8`], used only as the tokenizer's
/// pre-negotiation placeholder; it is overwritten before any byte is
/// interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Charset {
    /// Syntax level UNOA/UNOB: restricted/basic ASCII. A byte `>= 0x80` is
    /// rejected loudly rather than reinterpreted, so a high byte declared
    /// under an ASCII repertoire fails instead of silently corrupting.
    Ascii,
    /// Syntax level UNOC: ISO-8859-1 (Latin-1). Each byte `0xNN` maps to
    /// codepoint `U+00NN` across the whole `0x00..=0xFF` range, so decoding
    /// never fails and a round-trip is byte-exact. Encoding rejects a
    /// character above `U+00FF`, which Latin-1 cannot represent, rather than
    /// corrupting the output.
    Latin1,
    /// Syntax level UNOY: UTF-8. Strict — invalid UTF-8 is an error, not a
    /// lossy substitution, so a mis-declared interchange fails loudly. The
    /// default placeholder, never used to interpret bytes before the real
    /// repertoire is negotiated.
    #[default]
    Utf8,
}

impl Charset {
    /// Resolve a `UNB` syntax-identifier level (the first component of data
    /// element S001, e.g. `"UNOA"`, `"UNOC"`, `"UNOY"`) to a [`Charset`].
    ///
    /// The level is matched case-insensitively. UNOA and UNOB both map to
    /// ASCII (the acceptance only exercises a UNOC round-trip and an
    /// unsupported-level rejection, so byte-faithful ASCII for the two
    /// ASCII-subset levels is sufficient and keeps the read→write→read cycle
    /// exact for them).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Edifact`] naming the unsupported level and the
    /// supported set when the level matches no mapped repertoire (UNOD..UNOX
    /// have no pure-Rust total single-byte mapping available here). The
    /// interchange then fails loudly at decode time rather than guessing a
    /// fallback or silently substituting replacement characters.
    pub(crate) fn from_syntax_id(level: &str) -> Result<Self, FormatError> {
        let normalized = level.trim().to_ascii_uppercase();
        match normalized.as_str() {
            "UNOA" | "UNOB" => Ok(Charset::Ascii),
            "UNOC" => Ok(Charset::Latin1),
            "UNOY" => Ok(Charset::Utf8),
            _ => Err(FormatError::Edifact(format!(
                "unsupported EDIFACT syntax level {level:?} in the UNB syntax identifier. \
                 Supported levels are \"UNOA\"/\"UNOB\" (ASCII), \"UNOC\" (ISO-8859-1, \
                 Latin-1), and \"UNOY\" (UTF-8)"
            ))),
        }
    }

    /// Resolve the repertoire from a whole `UNB` syntax-identifier element
    /// (data element S001, e.g. `"UNOA:1"`), splitting off the level
    /// (component 1) on the active component separator before resolving.
    ///
    /// The writer arms the charset from this already-decoded element (its
    /// header comes from config or a `$doc` echo), so the "component 1 is the
    /// syntax level" structure lives here rather than at the call site. The
    /// reader instead uses [`Self::from_raw_unb_segment`], because it must
    /// know the repertoire before it can decode the `UNB`.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Edifact`] when the level component matches no
    /// mapped repertoire (see [`Self::from_syntax_id`]).
    pub(crate) fn from_unb_syntax_identifier(
        syntax_id: &str,
        component_sep: u8,
    ) -> Result<Self, FormatError> {
        let level = syntax_id.split(component_sep as char).next().unwrap_or("");
        Self::from_syntax_id(level)
    }

    /// Resolve the repertoire from the raw, still-undecoded bytes of a `UNB`
    /// segment (terminator already stripped), reading the syntax-identifier
    /// level (S001 component 1) directly from the bytes.
    ///
    /// The reader cannot decode the `UNB` until it knows the repertoire, and
    /// the repertoire is named inside the `UNB` — so the level is read from
    /// the raw bytes first. The level is always ASCII (`UNOA`..`UNOY`), and
    /// ASCII bytes are identical under every supported repertoire, so reading
    /// it byte-for-byte is unambiguous even when a later `UNB` element (a
    /// sender/recipient identification) carries a non-ASCII byte. Once
    /// resolved, the caller decodes the whole `UNB` through the returned
    /// repertoire.
    ///
    /// The level spans from just after the tag's element separator to the
    /// next component or element separator, honoring the release character so
    /// an escaped delimiter inside the (pathological) identifier is not
    /// mistaken for a boundary.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Edifact`] when the segment carries no
    /// syntax-identifier element, when that identifier is not ASCII (a
    /// malformed `UNB`, since the level is spec-ASCII), or when the level
    /// matches no mapped repertoire (see [`Self::from_syntax_id`]).
    pub(crate) fn from_raw_unb_segment(
        raw: &[u8],
        delims: &Delimiters,
    ) -> Result<Self, FormatError> {
        let level_bytes = raw_unb_syntax_level(raw, delims).ok_or_else(|| {
            FormatError::Edifact(
                "UNB carries no syntax-identifier element (S001); the interchange \
                 declares no character repertoire and is malformed"
                    .into(),
            )
        })?;
        let level = std::str::from_utf8(&level_bytes).map_err(|_| {
            FormatError::Edifact(
                "the UNB syntax-identifier level (S001 component 1) is not ASCII; the \
                 interchange is malformed (the syntax level is always one of \
                 UNOA/UNOB/UNOC/UNOY)"
                    .into(),
            )
        })?;
        Self::from_syntax_id(level)
    }

    /// Decode raw segment bytes to a `String` through this repertoire,
    /// consuming the buffer so the UTF-8 path validates in place without a
    /// copy (this runs once per segment on the hot path).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Edifact`] when the bytes are not valid in the
    /// repertoire: a byte `>= 0x80` under [`Charset::Ascii`], or invalid
    /// UTF-8 under [`Charset::Utf8`]. Latin-1 maps every byte and never
    /// fails. The message names the active level so a mis-declared syntax
    /// identifier is diagnosable.
    pub(crate) fn decode(&self, bytes: Vec<u8>) -> Result<String, FormatError> {
        match self {
            Charset::Latin1 => Ok(latin1_decode(bytes)),
            Charset::Ascii => {
                if let Some(&high) = bytes.iter().find(|&&b| b >= 0x80) {
                    return Err(FormatError::Edifact(format!(
                        "segment carries byte {high:#04X}, which is outside the ASCII \
                         repertoire declared by the UNB syntax identifier (UNOA/UNOB). \
                         Declare UNOC (Latin-1) or UNOY (UTF-8) if the interchange uses a \
                         wider character set"
                    )));
                }
                // Every byte is < 0x80, so the bytes are valid ASCII; the
                // Latin-1 byte→codepoint map is identical to ASCII here.
                Ok(latin1_decode(bytes))
            }
            Charset::Utf8 => String::from_utf8(bytes).map_err(|e| {
                FormatError::Edifact(format!(
                    "segment is not valid UTF-8: {e}. The UNB syntax identifier declares \
                     UNOY (UTF-8); declare UNOC (Latin-1) if the interchange uses a \
                     single-byte repertoire instead"
                ))
            }),
        }
    }

    /// Encode element text to raw bytes through this repertoire.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Edifact`] when the text carries a character the
    /// repertoire cannot represent: any non-ASCII character under
    /// [`Charset::Ascii`], or a character above `U+00FF` under
    /// [`Charset::Latin1`]. The writer then fails explicitly rather than
    /// emitting a structurally truncated value. UTF-8 encoding cannot fail.
    pub(crate) fn encode(&self, text: &str) -> Result<Vec<u8>, FormatError> {
        match self {
            Charset::Utf8 => Ok(text.as_bytes().to_vec()),
            Charset::Ascii => {
                if let Some(c) = text.chars().find(|c| !c.is_ascii()) {
                    return Err(FormatError::Edifact(format!(
                        "element text {text:?} contains character {c:?} (U+{:04X}), which is \
                         outside the ASCII repertoire declared by the UNB syntax identifier \
                         (UNOA/UNOB) and cannot be encoded",
                        u32::from(c)
                    )));
                }
                Ok(text.as_bytes().to_vec())
            }
            Charset::Latin1 => latin1_encode(text),
        }
    }
}

/// Extract the raw bytes of the `UNB` syntax-identifier level (S001
/// component 1) from a raw `UNB` segment, or `None` when the segment has no
/// data element after the tag.
///
/// Scans past the tag's element separator, then collects bytes up to the
/// first component or element separator. The release character escapes the
/// following byte so an escaped delimiter is treated as literal data,
/// mirroring [`super::split_segment`]; the escaped byte is included verbatim
/// (release char dropped) so the returned slice is the decoded level.
fn raw_unb_syntax_level(raw: &[u8], delims: &Delimiters) -> Option<Vec<u8>> {
    let mut iter = raw.iter().copied();
    // Skip the tag up to and including the first (unescaped) element
    // separator. A release before the separator escapes it.
    let mut pending_release = false;
    let mut found_tag_end = false;
    for b in iter.by_ref() {
        if pending_release {
            pending_release = false;
            continue;
        }
        if b == delims.release {
            pending_release = true;
            continue;
        }
        if b == delims.element {
            found_tag_end = true;
            break;
        }
    }
    if !found_tag_end {
        return None;
    }
    // Collect the first data element's first component (the syntax level).
    let mut level: Vec<u8> = Vec::new();
    let mut pending_release = false;
    for b in iter {
        if pending_release {
            level.push(b);
            pending_release = false;
            continue;
        }
        if b == delims.release {
            pending_release = true;
            continue;
        }
        if b == delims.component || b == delims.element {
            break;
        }
        level.push(b);
    }
    if pending_release {
        // A dangling release at element end escapes nothing — keep it.
        level.push(delims.release);
    }
    Some(level)
}

/// Decode bytes as ISO-8859-1 (Latin-1): byte `0xNN` is codepoint `U+00NN`
/// across the whole range, so every byte maps and decoding never fails. The
/// ASCII repertoire reuses this after rejecting high bytes, since the two
/// maps agree below `0x80`.
fn latin1_decode(bytes: Vec<u8>) -> String {
    bytes.into_iter().map(|b| b as char).collect()
}

/// Encode text as ISO-8859-1 (Latin-1), one byte per character.
///
/// # Errors
///
/// Returns [`FormatError::Edifact`] when a character is above `U+00FF`, which
/// Latin-1 cannot represent, so the value is rejected rather than truncated.
fn latin1_encode(text: &str) -> Result<Vec<u8>, FormatError> {
    text.chars()
        .map(|c| {
            u32::from(c).try_into().map_err(|_| {
                FormatError::Edifact(format!(
                    "element text {text:?} contains character {c:?} (U+{:04X}), which is \
                     outside ISO-8859-1 (Latin-1) and cannot be encoded; the repertoire \
                     declared by the UNB syntax identifier (UNOC) cannot represent it",
                    u32::from(c)
                ))
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_syntax_id_resolves_ascii_levels() {
        assert_eq!(Charset::from_syntax_id("UNOA").unwrap(), Charset::Ascii);
        assert_eq!(Charset::from_syntax_id("UNOB").unwrap(), Charset::Ascii);
        assert_eq!(Charset::from_syntax_id("unoa").unwrap(), Charset::Ascii);
    }

    #[test]
    fn from_syntax_id_resolves_latin1_and_utf8() {
        assert_eq!(Charset::from_syntax_id("UNOC").unwrap(), Charset::Latin1);
        assert_eq!(Charset::from_syntax_id("UNOY").unwrap(), Charset::Utf8);
        assert_eq!(Charset::from_syntax_id("unoc").unwrap(), Charset::Latin1);
    }

    #[test]
    fn from_syntax_id_rejects_unsupported_level_naming_value() {
        let err = Charset::from_syntax_id("UNOD").unwrap_err();
        assert!(
            matches!(&err, FormatError::Edifact(m) if m.contains("UNOD") && m.contains("UNOC")),
            "expected precise unsupported-level error, got {err:?}"
        );
    }

    #[test]
    fn latin1_decodes_high_bytes_to_codepoints() {
        // 0xE9 is é, 0xF1 is ñ in Latin-1.
        let decoded = Charset::Latin1
            .decode(vec![b'C', b'a', b'f', 0xE9])
            .unwrap();
        assert_eq!(decoded, "Café");
        let decoded = Charset::Latin1.decode(vec![b'A', 0xF1, b'o']).unwrap();
        assert_eq!(decoded, "Año");
    }

    #[test]
    fn latin1_round_trip_is_byte_exact() {
        let bytes: Vec<u8> = (0u8..=255).collect();
        let decoded = Charset::Latin1.decode(bytes.clone()).unwrap();
        let reencoded = Charset::Latin1.encode(&decoded).unwrap();
        assert_eq!(reencoded, bytes, "Latin-1 round-trip must be byte-exact");
    }

    #[test]
    fn from_raw_unb_segment_sniffs_level_before_decoding() {
        // The syntax level is read from raw UNB bytes (ASCII) even when a
        // later element carries a high byte that the level's repertoire would
        // later decode — proving the sniff does not need the body decoded.
        let d = Delimiters::level_a();
        let mut raw = b"UNB+UNOC:3+Caf".to_vec();
        raw.push(0xE9); // a non-ASCII byte further along the UNB
        assert_eq!(
            Charset::from_raw_unb_segment(&raw, &d).unwrap(),
            Charset::Latin1
        );

        // UNOA -> ASCII, UNOY -> UTF-8.
        assert_eq!(
            Charset::from_raw_unb_segment(b"UNB+UNOA:1+S+R", &d).unwrap(),
            Charset::Ascii
        );
        assert_eq!(
            Charset::from_raw_unb_segment(b"UNB+UNOY:4+S+R", &d).unwrap(),
            Charset::Utf8
        );
    }

    #[test]
    fn from_raw_unb_segment_handles_level_without_a_component() {
        // A syntax identifier that is just the level, with no `:version`
        // component, still resolves (the level runs to the element sep).
        let d = Delimiters::level_a();
        assert_eq!(
            Charset::from_raw_unb_segment(b"UNB+UNOC+S+R", &d).unwrap(),
            Charset::Latin1
        );
    }

    #[test]
    fn from_raw_unb_segment_rejects_missing_syntax_id() {
        // A UNB with no data element after the tag declares no repertoire.
        let d = Delimiters::level_a();
        let err = Charset::from_raw_unb_segment(b"UNB", &d).unwrap_err();
        assert!(
            matches!(&err, FormatError::Edifact(m) if m.contains("syntax-identifier")),
            "expected missing-syntax-id error, got {err:?}"
        );
    }

    #[test]
    fn from_raw_unb_segment_rejects_unsupported_level() {
        let d = Delimiters::level_a();
        let err = Charset::from_raw_unb_segment(b"UNB+UNOD:4+S+R", &d).unwrap_err();
        assert!(
            matches!(&err, FormatError::Edifact(m) if m.contains("UNOD")),
            "expected unsupported-level error naming UNOD, got {err:?}"
        );
    }

    #[test]
    fn ascii_rejects_high_byte_loudly() {
        let err = Charset::Ascii
            .decode(vec![b'C', b'a', b'f', 0xE9])
            .unwrap_err();
        assert!(
            matches!(&err, FormatError::Edifact(m) if m.contains("ASCII")),
            "expected ASCII high-byte rejection, got {err:?}"
        );
    }

    #[test]
    fn ascii_decodes_plain_ascii() {
        let decoded = Charset::Ascii.decode(b"PLAIN".to_vec()).unwrap();
        assert_eq!(decoded, "PLAIN");
    }

    #[test]
    fn utf8_rejects_invalid_bytes() {
        // 0xE9 alone is not valid UTF-8.
        let err = Charset::Utf8
            .decode(vec![b'C', b'a', b'f', 0xE9])
            .unwrap_err();
        assert!(
            matches!(&err, FormatError::Edifact(m) if m.contains("not valid UTF-8")),
            "expected UTF-8 decode error, got {err:?}"
        );
    }

    #[test]
    fn latin1_encode_rejects_out_of_range_char() {
        // U+20AC (euro sign) is above U+00FF and cannot be Latin-1 encoded.
        let err = Charset::Latin1.encode("price €5").unwrap_err();
        assert!(
            matches!(&err, FormatError::Edifact(m) if m.contains("Latin-1") && m.contains("U+20AC")),
            "expected out-of-range encode error, got {err:?}"
        );
    }

    #[test]
    fn ascii_encode_rejects_high_char() {
        let err = Charset::Ascii.encode("Café").unwrap_err();
        assert!(
            matches!(&err, FormatError::Edifact(m) if m.contains("ASCII") && m.contains("U+00E9")),
            "expected ASCII out-of-range encode error, got {err:?}"
        );
    }

    #[test]
    fn utf8_encode_is_passthrough() {
        assert_eq!(Charset::Utf8.encode("Café €").unwrap(), "Café €".as_bytes());
    }
}
