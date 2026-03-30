/// Minimal YAML syntax tokeniser for static Phase 1 display.
///
/// No external dependencies — no regex, no tree-sitter (C binding). A simple
/// line-by-line pass that covers the common patterns in the demo YAML.
///
/// Replaced in Phase 2 with incremental parsing driven by serde-saphyr.

/// A single coloured span within a YAML line.
#[derive(Clone, Debug)]
pub struct Token {
    pub kind: TokenKind,
    pub text: String,
}

/// Semantic token kind — maps directly to CSS `data-token` attribute values
/// which drive the syntax colour rules in `kiln.css`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum TokenKind {
    /// `#` comment — text-floor colour.
    Comment,
    /// Mapping key before the colon — verdigris.
    Key,
    /// Scalar value / sequence item content — phosphor.
    Value,
    /// Punctuation: `:`, `- `, `"`, `'` — iron.
    Punct,
    /// Whitespace-only indent — rendered as-is, no colour class needed.
    Indent,
}

impl TokenKind {
    pub fn as_data_attr(self) -> &'static str {
        match self {
            TokenKind::Comment => "comment",
            TokenKind::Key => "key",
            TokenKind::Value => "value",
            TokenKind::Punct => "punct",
            TokenKind::Indent => "indent",
        }
    }
}

impl Token {
    fn new(kind: TokenKind, text: impl Into<String>) -> Self {
        Self { kind, text: text.into() }
    }
}

/// Tokenise a YAML document into a `Vec` of lines, each line a `Vec<Token>`.
pub fn tokenize(yaml: &str) -> Vec<Vec<Token>> {
    yaml.lines().map(tokenize_line).collect()
}

fn tokenize_line(line: &str) -> Vec<Token> {
    if line.trim().is_empty() {
        return vec![Token::new(TokenKind::Indent, "")];
    }

    let indent_len = line.len() - line.trim_start().len();
    let indent = &line[..indent_len];
    let rest = &line[indent_len..];

    // Comment line
    if rest.starts_with('#') {
        return vec![Token::new(TokenKind::Comment, line)];
    }

    let mut tokens: Vec<Token> = Vec::new();
    if indent_len > 0 {
        tokens.push(Token::new(TokenKind::Indent, indent));
    }

    // Strip list-item prefix "- "
    let (after_prefix, had_prefix) = if let Some(stripped) = rest.strip_prefix("- ") {
        (stripped, true)
    } else if rest == "-" {
        ("", true)
    } else {
        (rest, false)
    };

    if had_prefix {
        tokens.push(Token::new(TokenKind::Punct, "- "));
    }

    // Key: value  or  key:\n
    if let Some(colon_pos) = find_key_colon(after_prefix) {
        let key = &after_prefix[..colon_pos];
        let after_colon = &after_prefix[colon_pos + 1..];

        tokens.push(Token::new(TokenKind::Key, key));
        tokens.push(Token::new(TokenKind::Punct, ":"));

        if !after_colon.is_empty() {
            // "  value" — emit a space in the punct, then the value
            let value_trimmed = after_colon.trim_start();
            let leading = &after_colon[..after_colon.len() - value_trimmed.len()];
            if !leading.is_empty() {
                tokens.push(Token::new(TokenKind::Punct, leading));
            }
            tokens.push(Token::new(TokenKind::Value, value_trimmed));
        }
    } else {
        // Pure scalar value (sequence item body, bare word, etc.)
        tokens.push(Token::new(TokenKind::Value, after_prefix));
    }

    tokens
}

/// Finds the position of the `:` that ends a YAML mapping key, ignoring colons
/// that appear inside quoted strings. Returns `None` if the line is not a key.
fn find_key_colon(s: &str) -> Option<usize> {
    let mut in_single = false;
    let mut in_double = false;

    for (i, ch) in s.char_indices() {
        match ch {
            '\'' if !in_double => in_single = !in_single,
            '"' if !in_single => in_double = !in_double,
            ':' if !in_single && !in_double => {
                // Colon must be followed by whitespace, end-of-string, or EOF
                // to be a key colon (not an arbitrary colon inside a value).
                let next = s[i + 1..].chars().next();
                if matches!(next, None | Some(' ') | Some('\t')) {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}
