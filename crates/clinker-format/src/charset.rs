//! Pure-Rust single-byte character-set decoding for flat text formats.
//!
//! Some text interchange and tabular formats carry a declared character
//! repertoire rather than UTF-8 (X12 free-text name/address fields, CSV
//! exports from legacy systems). These formats have no reliable in-band
//! marker naming the body encoding, so the repertoire is declared on the
//! source and defaults to UTF-8.
//!
//! Only single-byte repertoires whose byte↔codepoint mapping is total and
//! round-trippable in pure Rust are supported, so a read→write→read cycle is
//! byte-faithful for the configured charset. An unsupported or unconfigured
//! non-UTF-8 repertoire fails explicitly rather than guessing or silently
//! substituting replacement characters.
//!
//! No decoding dependency is pulled in: UTF-8 validates through the standard
//! library and Latin-1 is a direct byte→codepoint map.

use crate::error::FormatError;

/// The character repertoire used to decode and encode field/element text.
///
/// Decoding happens per field (no whole-input buffering), so the charset is
/// consulted once per value rather than over the stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Charset {
    /// UTF-8 (the default). Strict: invalid UTF-8 is an error, not a
    /// lossy substitution, so a mis-declared encoding fails loudly.
    #[default]
    Utf8,
    /// ISO-8859-1 (Latin-1). Each byte `0xNN` maps to codepoint `U+00NN`
    /// across the whole `0x00..=0xFF` range, so decoding never fails and a
    /// round-trip is byte-exact. Encoding rejects a character above
    /// `U+00FF`, which Latin-1 cannot represent, rather than corrupting the
    /// output.
    Latin1,
}

impl Charset {
    /// Resolve a source-declared encoding name to a [`Charset`],
    /// case-insensitively, accepting the common aliases for each repertoire.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Charset`] naming the unsupported value and the
    /// supported set when the name matches no known repertoire, so a
    /// mis-configured source fails with actionable guidance rather than
    /// guessing.
    pub fn from_name(name: &str) -> Result<Self, FormatError> {
        // Match on a normalized form so `UTF-8`, `utf8`, `ISO-8859-1`,
        // `iso8859-1`, `Latin-1`, and `latin1` all resolve.
        let normalized: String = name
            .chars()
            .filter(|c| !matches!(c, '-' | '_' | ' '))
            .map(|c| c.to_ascii_lowercase())
            .collect();
        match normalized.as_str() {
            "utf8" => Ok(Charset::Utf8),
            "iso88591" | "latin1" | "l1" => Ok(Charset::Latin1),
            _ => Err(FormatError::Charset(format!(
                "unsupported character set {name:?}. Supported encodings are \
                 \"utf-8\" (the default) and \"iso-8859-1\" (Latin-1)"
            ))),
        }
    }

    /// Decode raw field bytes to a `String` through this repertoire,
    /// consuming the buffer so the UTF-8 path validates in place without a
    /// copy (this runs once per value on the hot path).
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Charset`] when the bytes are not valid in the
    /// repertoire — only possible for [`Charset::Utf8`], since Latin-1 maps
    /// every byte. The message names the configured charset so a
    /// mis-declared encoding is diagnosable.
    pub fn decode(&self, bytes: Vec<u8>) -> Result<String, FormatError> {
        match self {
            Charset::Utf8 => String::from_utf8(bytes).map_err(|e| {
                FormatError::Charset(format!(
                    "input is not valid UTF-8: {e}. Declare the source's character \
                     set (e.g. `encoding: iso-8859-1`) if the input uses a \
                     non-UTF-8 repertoire"
                ))
            }),
            // Latin-1: byte 0xNN is codepoint U+00NN, so every byte decodes.
            Charset::Latin1 => Ok(bytes.into_iter().map(|b| b as char).collect()),
        }
    }

    /// Encode field text to raw bytes through this repertoire.
    ///
    /// # Errors
    ///
    /// Returns [`FormatError::Charset`] for [`Charset::Latin1`] when the text
    /// carries a character above `U+00FF` that Latin-1 cannot represent, so
    /// the writer fails explicitly rather than emitting a structurally
    /// truncated value. UTF-8 encoding never fails.
    pub fn encode(&self, text: &str) -> Result<Vec<u8>, FormatError> {
        match self {
            Charset::Utf8 => Ok(text.as_bytes().to_vec()),
            Charset::Latin1 => text
                .chars()
                .map(|c| {
                    u32::from(c).try_into().map_err(|_| {
                        FormatError::Charset(format!(
                            "text {text:?} contains character {c:?} (U+{:04X}), which \
                             is outside ISO-8859-1 (Latin-1) and cannot be encoded; the \
                             configured charset cannot represent it",
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
    fn from_name_resolves_utf8_aliases() {
        assert_eq!(Charset::from_name("utf-8").unwrap(), Charset::Utf8);
        assert_eq!(Charset::from_name("UTF8").unwrap(), Charset::Utf8);
        assert_eq!(Charset::from_name("utf8").unwrap(), Charset::Utf8);
    }

    #[test]
    fn from_name_resolves_latin1_aliases() {
        assert_eq!(Charset::from_name("iso-8859-1").unwrap(), Charset::Latin1);
        assert_eq!(Charset::from_name("ISO8859-1").unwrap(), Charset::Latin1);
        assert_eq!(Charset::from_name("latin-1").unwrap(), Charset::Latin1);
        assert_eq!(Charset::from_name("Latin1").unwrap(), Charset::Latin1);
    }

    #[test]
    fn from_name_rejects_unsupported_with_precise_error() {
        let err = Charset::from_name("shift_jis").unwrap_err();
        assert!(
            matches!(&err, FormatError::Charset(m) if m.contains("shift_jis") && m.contains("iso-8859-1")),
            "expected precise unsupported-charset error, got {err:?}"
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
    fn utf8_rejects_invalid_bytes() {
        // 0xE9 alone is not valid UTF-8.
        let err = Charset::Utf8
            .decode(vec![b'C', b'a', b'f', 0xE9])
            .unwrap_err();
        assert!(
            matches!(&err, FormatError::Charset(m) if m.contains("not valid UTF-8")),
            "expected UTF-8 decode error, got {err:?}"
        );
    }

    #[test]
    fn latin1_encode_rejects_out_of_range_char() {
        // U+20AC (euro sign) is above U+00FF and cannot be Latin-1 encoded.
        let err = Charset::Latin1.encode("price €5").unwrap_err();
        assert!(
            matches!(&err, FormatError::Charset(m) if m.contains("Latin-1") && m.contains("U+20AC")),
            "expected out-of-range encode error, got {err:?}"
        );
    }

    #[test]
    fn utf8_encode_is_passthrough() {
        assert_eq!(Charset::Utf8.encode("Café €").unwrap(), "Café €".as_bytes());
    }
}
