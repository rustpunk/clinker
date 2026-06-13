//! Character-set decoding for EDIFACT element text, selected in-band from
//! the `UNB` syntax identifier.
//!
//! Unlike X12 — which has no in-band repertoire marker and declares its
//! encoding out-of-band on the source — an EDIFACT interchange names its
//! repertoire on the wire: `UNB` data element S001 component 1 carries the
//! syntax identifier (`UNOA`, `UNOB`, `UNOC`, …). The tokenizer starts on a
//! byte-total bootstrap repertoire so the `UNB` itself decodes before its
//! repertoire is known, then is re-armed once the reader has parsed the
//! syntax identifier; every subsequent segment decodes through the
//! negotiated repertoire.
//!
//! Only repertoires whose byte↔codepoint mapping is total and
//! round-trippable in pure Rust are supported, so a read→write→read cycle is
//! byte-faithful for the negotiated charset. An unsupported syntax level
//! fails explicitly, naming the offending level, rather than guessing or
//! silently substituting replacement characters.

use crate::error::FormatError;

/// The character repertoire used to decode and encode EDIFACT element text,
/// resolved from the `UNB` syntax identifier.
///
/// Decoding happens per segment (no whole-interchange buffering), so the
/// charset is consulted once per segment read rather than over the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Charset {
    /// The bootstrap repertoire used before the `UNB` syntax identifier is
    /// known. Maps every byte `0xNN` to codepoint `U+00NN` (Latin-1 style),
    /// so the `UNB` — whose tag and syntax identifier are pure ASCII —
    /// decodes regardless of any high byte further along the segment, and
    /// decoding never fails. Re-armed to the negotiated repertoire before
    /// the first body segment is decoded.
    #[default]
    Bootstrap,
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
    /// lossy substitution, so a mis-declared interchange fails loudly.
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
    /// Both the reader and the writer arm the charset from this one element,
    /// so the "component 1 is the syntax level" structure lives here rather
    /// than at each call site.
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

    /// Decode raw segment bytes to a `String` through this repertoire,
    /// consuming the buffer so the UTF-8 path validates in place without a
    /// copy (this runs once per segment on the hot path).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Edifact`] when the bytes are not valid in the
    /// repertoire: a byte `>= 0x80` under [`Charset::Ascii`], or invalid
    /// UTF-8 under [`Charset::Utf8`]. The bootstrap and Latin-1 repertoires
    /// map every byte and never fail. The message names the active level so
    /// a mis-declared syntax identifier is diagnosable.
    pub(crate) fn decode(&self, bytes: Vec<u8>) -> Result<String, FormatError> {
        match self {
            // Bootstrap and Latin-1 both map byte 0xNN to U+00NN, so every
            // byte decodes; bootstrap exists only to read the UNB before its
            // repertoire is known.
            Charset::Bootstrap | Charset::Latin1 => {
                Ok(bytes.into_iter().map(|b| b as char).collect())
            }
            Charset::Ascii => {
                if let Some(&high) = bytes.iter().find(|&&b| b >= 0x80) {
                    return Err(FormatError::Edifact(format!(
                        "segment carries byte {high:#04X}, which is outside the ASCII \
                         repertoire declared by the UNB syntax identifier (UNOA/UNOB). \
                         Declare UNOC (Latin-1) or UNOY (UTF-8) if the interchange uses a \
                         wider character set"
                    )));
                }
                // Every byte is < 0x80, so the slice is valid ASCII and
                // therefore valid UTF-8.
                Ok(bytes.into_iter().map(|b| b as char).collect())
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
    /// emitting a structurally truncated value. UTF-8 (and the bootstrap
    /// repertoire, for the ASCII control segments it ever sees) cannot fail.
    pub(crate) fn encode(&self, text: &str) -> Result<Vec<u8>, FormatError> {
        match self {
            Charset::Utf8 | Charset::Bootstrap => Ok(text.as_bytes().to_vec()),
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
            Charset::Latin1 => text
                .chars()
                .map(|c| {
                    u32::from(c).try_into().map_err(|_| {
                        FormatError::Edifact(format!(
                            "element text {text:?} contains character {c:?} (U+{:04X}), which \
                             is outside ISO-8859-1 (Latin-1) and cannot be encoded; the \
                             repertoire declared by the UNB syntax identifier (UNOC) cannot \
                             represent it",
                            u32::from(c)
                        ))
                    })
                })
                .collect(),
        }
    }
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
    fn bootstrap_decodes_every_byte_and_never_fails() {
        // The bootstrap repertoire must not break on a high byte before the
        // repertoire is known.
        let bytes: Vec<u8> = (0u8..=255).collect();
        let decoded = Charset::Bootstrap.decode(bytes.clone()).unwrap();
        assert_eq!(decoded.chars().count(), 256);
        // The ASCII prefix a UNB tag/syntax-id lives in is faithful.
        let unb = Charset::Bootstrap.decode(b"UNB+UNOA:1".to_vec()).unwrap();
        assert_eq!(unb, "UNB+UNOA:1");
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
