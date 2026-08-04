//! Single-line, byte-bounded previews for untrusted diagnostic values.

/// Maximum rendered preview size after sanitization.
pub(crate) const PREVIEW_MAX_BYTES: usize = 256;
/// Marker appended exactly once when the sanitized token stream is truncated.
pub(crate) const PREVIEW_TRUNCATION_MARKER: &str = "…";

/// Sanitized preview plus the unmodified input length used for attribution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiagnosticPreview {
    pub(crate) rendered: String,
    pub(crate) original_byte_length: usize,
    pub(crate) redacted: bool,
}

/// Escape untrusted bytes into indivisible tokens, then retain the longest
/// whole-token prefix that fits the rendered byte cap.
///
/// `sensitive` is an explicit classification supplied by the caller. The
/// function deliberately does not infer sensitivity from field names or data.
pub(crate) fn build_diagnostic_preview(raw: &[u8], sensitive: bool) -> DiagnosticPreview {
    let original_byte_length = raw.len();
    if sensitive {
        return DiagnosticPreview {
            rendered: "[REDACTED]".into(),
            original_byte_length,
            redacted: true,
        };
    }

    let (tokens, truncated) = sanitize_prefix_tokens(raw);
    let rendered_len: usize = tokens.iter().map(|token| token.len()).sum();
    let mut rendered = String::with_capacity(rendered_len.min(PREVIEW_MAX_BYTES));
    if !truncated {
        for token in tokens {
            rendered.push_str(&token);
        }
    } else {
        let prefix_cap = PREVIEW_MAX_BYTES - PREVIEW_TRUNCATION_MARKER.len();
        for token in tokens {
            if rendered.len() + token.len() > prefix_cap {
                break;
            }
            rendered.push_str(&token);
        }
        rendered.push_str(PREVIEW_TRUNCATION_MARKER);
    }
    debug_assert!(rendered.len() <= PREVIEW_MAX_BYTES);
    DiagnosticPreview {
        rendered,
        original_byte_length,
        redacted: false,
    }
}

/// Retain only the bounded token prefix needed to decide whether the preview
/// truncates. A rejected source value may itself be large, so building tokens
/// for the entire value would defeat the diagnostic payload bound.
fn sanitize_prefix_tokens(raw: &[u8]) -> (Vec<String>, bool) {
    let mut tokens = Vec::new();
    let mut rendered_len = 0;
    let mut offset = 0;
    while offset < raw.len() {
        match std::str::from_utf8(&raw[offset..]) {
            Ok(valid) => {
                for ch in valid.chars() {
                    if push_token(&mut tokens, &mut rendered_len, sanitize_char(ch)) {
                        return (tokens, true);
                    }
                }
                return (tokens, false);
            }
            Err(error) => {
                let valid_end = offset + error.valid_up_to();
                if valid_end > offset {
                    let valid = std::str::from_utf8(&raw[offset..valid_end])
                        .expect("valid_up_to identifies a UTF-8 prefix");
                    for ch in valid.chars() {
                        if push_token(&mut tokens, &mut rendered_len, sanitize_char(ch)) {
                            return (tokens, true);
                        }
                    }
                }
                let invalid_len = error.error_len().unwrap_or(raw.len() - valid_end);
                for byte in &raw[valid_end..valid_end + invalid_len] {
                    if push_token(&mut tokens, &mut rendered_len, format!("\\x{byte:02X}")) {
                        return (tokens, true);
                    }
                }
                offset = valid_end + invalid_len;
            }
        }
    }
    (tokens, false)
}

fn push_token(tokens: &mut Vec<String>, rendered_len: &mut usize, token: String) -> bool {
    *rendered_len += token.len();
    tokens.push(token);
    *rendered_len > PREVIEW_MAX_BYTES
}

fn sanitize_char(ch: char) -> String {
    match ch {
        '\r' => "\\r".into(),
        '\n' => "\\n".into(),
        '\t' => "\\t".into(),
        '\\' => "\\\\".into(),
        // Diagnostic delimiters are escaped so an input value cannot forge a
        // field boundary in the surrounding structured message.
        '[' | ']' | '{' | '}' | '<' | '>' | '|' | '"' | '\'' | '=' => {
            format!("\\u{{{:04X}}}", ch as u32)
        }
        _ if is_control_or_bidi(ch) => format!("\\u{{{:04X}}}", ch as u32),
        _ => ch.to_string(),
    }
}

fn is_control_or_bidi(ch: char) -> bool {
    matches!(
        ch as u32,
        0x0000..=0x001F
            | 0x007F..=0x009F
            | 0x061C
            | 0x200E..=0x200F
            | 0x202A..=0x202E
            | 0x2066..=0x2069
    )
}

#[cfg(test)]
mod boundary_tests {
    use super::*;

    #[test]
    fn ascii_boundaries_reserve_the_complete_marker() {
        for length in [255usize, 256] {
            let preview = build_diagnostic_preview(&vec![b'x'; length], false);
            assert_eq!(preview.rendered, "x".repeat(length));
            assert_eq!(preview.rendered.len(), length);
        }

        let preview = build_diagnostic_preview(&vec![b'x'; 257], false);
        assert_eq!(preview.rendered, format!("{}…", "x".repeat(253)));
        assert_eq!(preview.rendered.len(), PREVIEW_MAX_BYTES);
        assert_eq!(preview.original_byte_length, 257);
    }

    #[test]
    fn invalid_utf8_controls_bidi_delimiters_and_backslash_are_explicit_tokens() {
        let preview =
            build_diagnostic_preview(b"\xFF\r\n\t\\\"'=[]{}<>|\xC2\x85\xE2\x80\xAE", false);
        assert_eq!(
            preview.rendered,
            concat!(
                "\\xFF",
                "\\r",
                "\\n",
                "\\t",
                "\\\\",
                "\\u{0022}",
                "\\u{0027}",
                "\\u{003D}",
                "\\u{005B}",
                "\\u{005D}",
                "\\u{007B}",
                "\\u{007D}",
                "\\u{003C}",
                "\\u{003E}",
                "\\u{007C}",
                "\\u{0085}",
                "\\u{202E}",
            )
        );
    }

    #[test]
    fn truncation_never_splits_unicode_scalars_or_escape_tokens() {
        let mut unicode = vec![b'x'; 252];
        unicode.extend_from_slice("ééé".as_bytes());
        let preview = build_diagnostic_preview(&unicode, false);
        assert_eq!(preview.rendered, format!("{}…", "x".repeat(252)));
        assert_eq!(preview.rendered.len(), 255);

        let mut escaped = vec![b'x'; 252];
        escaped.push(b'[');
        let preview = build_diagnostic_preview(&escaped, false);
        assert_eq!(preview.rendered, format!("{}…", "x".repeat(252)));
        assert_eq!(
            preview.rendered.matches(PREVIEW_TRUNCATION_MARKER).count(),
            1
        );
    }

    #[test]
    fn explicit_sensitive_classification_redacts_without_guessing() {
        let preview = build_diagnostic_preview(b"secret", true);
        assert_eq!(preview.rendered, "[REDACTED]");
        assert_eq!(preview.original_byte_length, 6);
        assert!(preview.redacted);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preview_boundaries_keep_255_and_256_bytes_and_truncate_257() {
        for size in [255, 256] {
            let raw = vec![b'a'; size];
            let preview = build_diagnostic_preview(&raw, false);
            assert_eq!(preview.rendered.as_bytes(), raw);
            assert_eq!(preview.original_byte_length, size);
        }

        let preview = build_diagnostic_preview(&vec![b'a'; 257], false);
        assert_eq!(preview.rendered.len(), PREVIEW_MAX_BYTES);
        assert_eq!(&preview.rendered.as_bytes()[..253], vec![b'a'; 253]);
        assert!(preview.rendered.ends_with(PREVIEW_TRUNCATION_MARKER));
        assert_eq!(
            preview.rendered.matches(PREVIEW_TRUNCATION_MARKER).count(),
            1
        );
    }

    #[test]
    fn preview_never_splits_unicode_or_escape_tokens_at_the_prefix_boundary() {
        let unicode = format!("{}ézz", "a".repeat(253));
        let preview = build_diagnostic_preview(unicode.as_bytes(), false);
        assert_eq!(preview.rendered, format!("{}…", "a".repeat(253)));

        let escaped = format!("{}\nzz", "a".repeat(253));
        let preview = build_diagnostic_preview(escaped.as_bytes(), false);
        assert_eq!(preview.rendered, format!("{}…", "a".repeat(253)));
    }

    #[test]
    fn preview_uses_explicit_tokens_for_invalid_utf8_controls_and_redaction() {
        let raw = [b'a', 0xff, b'\r', b'\n', b'\t', b'\\', 0x9f];
        let preview = build_diagnostic_preview(&raw, false);
        assert_eq!(preview.rendered, "a\\xFF\\r\\n\\t\\\\\\x9F");
        assert_eq!(preview.original_byte_length, raw.len());
        assert!(!preview.redacted);

        let redacted = build_diagnostic_preview(b"complete secret", true);
        assert_eq!(redacted.rendered, "[REDACTED]");
        assert_eq!(redacted.original_byte_length, 15);
        assert!(redacted.redacted);
    }
}
