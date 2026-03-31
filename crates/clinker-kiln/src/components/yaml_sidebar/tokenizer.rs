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
    /// CXL statement keyword (`emit`, `let`, `set`, `if`, etc.) — verdigris.
    CxlKeyword,
    /// CXL operator (`==`, `!=`, `and`, `or`, `not`, `+`, etc.) — iron.
    CxlOp,
    /// CXL string literal (`"active"`, `'foo'`) — ember.
    CxlString,
    /// CXL identifier / field reference — phosphor.
    CxlIdent,
    /// CXL literal value (`true`, `false`, `null`, numbers) — phosphor-dim.
    CxlLiteral,
    /// CXL punctuation (parens, brackets, dots, commas) — iron.
    CxlPunct,
}

impl TokenKind {
    pub fn as_data_attr(self) -> &'static str {
        match self {
            TokenKind::Comment => "comment",
            TokenKind::Key => "key",
            TokenKind::Value => "value",
            TokenKind::Punct => "punct",
            TokenKind::Indent => "indent",
            TokenKind::CxlKeyword => "cxl-keyword",
            TokenKind::CxlOp => "cxl-op",
            TokenKind::CxlString => "cxl-string",
            TokenKind::CxlIdent => "cxl-ident",
            TokenKind::CxlLiteral => "cxl-literal",
            TokenKind::CxlPunct => "cxl-punct",
        }
    }
}

impl Token {
    fn new(kind: TokenKind, text: impl Into<String>) -> Self {
        Self { kind, text: text.into() }
    }
}

/// CXL keywords that start statements or control flow.
const CXL_KEYWORDS: &[&str] = &[
    "emit", "let", "trace", "use", "if", "then", "else", "match", "fn", "as",
];

/// CXL keyword-style operators.
const CXL_KEYWORD_OPS: &[&str] = &["and", "or", "not"];

/// CXL literal keywords.
const CXL_LITERALS: &[&str] = &["true", "false", "null", "now", "it", "window", "pipeline"];

/// Tokenise a YAML document into a `Vec` of lines, each line a `Vec<Token>`.
///
/// Tracks `cxl: |` block scalar context so that continuation lines inside
/// a CXL block get CXL syntax highlighting instead of plain YAML values.
pub fn tokenize(yaml: &str) -> Vec<Vec<Token>> {
    let mut result = Vec::new();
    let mut cxl_block: Option<usize> = None; // indent level of the cxl block body

    for line in yaml.lines() {
        let trimmed = line.trim();

        // Check if we're exiting a CXL block
        if let Some(block_indent) = cxl_block {
            if trimmed.is_empty() {
                // Blank lines inside block scalars are kept
                result.push(vec![Token::new(TokenKind::Indent, "")]);
                continue;
            }
            let line_indent = line.len() - line.trim_start().len();
            if line_indent < block_indent {
                // Dedented — we've left the CXL block
                cxl_block = None;
            }
        }

        // If we're inside a CXL block, tokenize as CXL
        if cxl_block.is_some() {
            let indent = &line[..line.len() - line.trim_start().len()];
            let body = line.trim_start();
            let mut tokens = Vec::new();
            if !indent.is_empty() {
                tokens.push(Token::new(TokenKind::Indent, indent));
            }
            tokens.extend(tokenize_cxl(body));
            result.push(tokens);
            continue;
        }

        // Normal YAML tokenization
        let tokens = tokenize_line(line);

        // Detect `cxl: |` pattern to enter CXL block mode
        if is_cxl_block_start(&tokens) {
            let line_indent = line.len() - line.trim_start().len();
            // The block body will be indented further than the key
            cxl_block = Some(line_indent + 2); // typical YAML indent is +2
        }

        // Detect inline `cxl: <expr>` (single-line CXL value)
        let final_tokens = maybe_inline_cxl(tokens);
        result.push(final_tokens);
    }

    result
}

/// Check if a tokenized line represents `cxl: |` (block scalar start).
fn is_cxl_block_start(tokens: &[Token]) -> bool {
    let mut saw_cxl_key = false;
    for token in tokens {
        if token.kind == TokenKind::Key && token.text == "cxl" {
            saw_cxl_key = true;
        }
        if saw_cxl_key && token.kind == TokenKind::Value && token.text.trim() == "|" {
            return true;
        }
    }
    false
}

/// If the line has a `cxl` key with an inline value (not `|`), replace the
/// value token with CXL-highlighted tokens.
fn maybe_inline_cxl(tokens: Vec<Token>) -> Vec<Token> {
    let mut saw_cxl_key = false;
    let mut cxl_value_idx = None;

    for (i, token) in tokens.iter().enumerate() {
        if token.kind == TokenKind::Key && token.text == "cxl" {
            saw_cxl_key = true;
        }
        if saw_cxl_key && token.kind == TokenKind::Value {
            let trimmed = token.text.trim();
            if trimmed != "|" && trimmed != ">" && !trimmed.is_empty() {
                cxl_value_idx = Some(i);
            }
            break;
        }
    }

    if let Some(idx) = cxl_value_idx {
        let mut result = Vec::with_capacity(tokens.len() + 4);
        for (i, token) in tokens.into_iter().enumerate() {
            if i == idx {
                result.extend(tokenize_cxl(&token.text));
            } else {
                result.push(token);
            }
        }
        result
    } else {
        tokens
    }
}

/// Tokenize a CXL expression into coloured spans.
///
/// This is a lightweight lexer for syntax colouring — not a full parser.
/// It handles keywords, operators, string literals, numbers, identifiers,
/// and punctuation.
pub fn tokenize_cxl(cxl: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = cxl.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        let ch = chars[i];

        // Whitespace
        if ch.is_whitespace() {
            let start = i;
            while i < len && chars[i].is_whitespace() {
                i += 1;
            }
            tokens.push(Token::new(TokenKind::Indent, &cxl[start..i]));
            continue;
        }

        // Line comment
        if ch == '#' {
            tokens.push(Token::new(TokenKind::Comment, &cxl[i..]));
            break;
        }

        // String literals
        if ch == '"' || ch == '\'' {
            let quote = ch;
            let start = i;
            i += 1;
            while i < len && chars[i] != quote {
                if chars[i] == '\\' {
                    i += 1; // skip escaped char
                }
                i += 1;
            }
            if i < len {
                i += 1; // closing quote
            }
            tokens.push(Token::new(TokenKind::CxlString, &cxl[start..i]));
            continue;
        }

        // Date literal #YYYY-MM-DD#
        if ch == '#' {
            let start = i;
            i += 1;
            while i < len && chars[i] != '#' {
                i += 1;
            }
            if i < len {
                i += 1;
            }
            tokens.push(Token::new(TokenKind::CxlLiteral, &cxl[start..i]));
            continue;
        }

        // Multi-char operators
        if i + 1 < len {
            let two = &cxl[i..i + 2];
            match two {
                "==" | "!=" | ">=" | "<=" | "=>" | "??" | "::" => {
                    tokens.push(Token::new(TokenKind::CxlOp, two));
                    i += 2;
                    continue;
                }
                _ => {}
            }
        }

        // Single-char operators
        if matches!(ch, '+' | '-' | '*' | '/' | '%' | '>' | '<' | '=' | '|') {
            tokens.push(Token::new(TokenKind::CxlOp, &cxl[i..i + 1]));
            i += 1;
            continue;
        }

        // Punctuation
        if matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | '.' | ',' | ':' | '_')
            && !(ch == '_' && i + 1 < len && (chars[i + 1].is_alphanumeric() || chars[i + 1] == '_'))
        {
            tokens.push(Token::new(TokenKind::CxlPunct, &cxl[i..i + 1]));
            i += 1;
            continue;
        }

        // Numbers
        if ch.is_ascii_digit() {
            let start = i;
            while i < len && (chars[i].is_ascii_digit() || chars[i] == '.') {
                i += 1;
            }
            tokens.push(Token::new(TokenKind::CxlLiteral, &cxl[start..i]));
            continue;
        }

        // Identifiers and keywords
        if ch.is_alphabetic() || ch == '_' {
            let start = i;
            while i < len && (chars[i].is_alphanumeric() || chars[i] == '_') {
                i += 1;
            }
            let word = &cxl[start..i];
            let kind = if CXL_KEYWORDS.contains(&word) {
                TokenKind::CxlKeyword
            } else if CXL_KEYWORD_OPS.contains(&word) {
                TokenKind::CxlOp
            } else if CXL_LITERALS.contains(&word) {
                TokenKind::CxlLiteral
            } else {
                TokenKind::CxlIdent
            };
            tokens.push(Token::new(kind, word));
            continue;
        }

        // Fallback: single character
        tokens.push(Token::new(TokenKind::CxlIdent, &cxl[i..i + 1]));
        i += 1;
    }

    tokens
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
