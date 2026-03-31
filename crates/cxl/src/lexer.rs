/// Maximum CXL source length (64KB). Prevents pathological inputs.
pub const MAX_SOURCE_LEN: usize = 64 * 1024;

/// Byte-offset span for source mapping and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

impl Span {
    pub fn new(start: usize, end: usize) -> Self {
        Self {
            start: start as u32,
            end: end as u32,
        }
    }
}

/// CXL token produced by the lexer.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // --- Keywords (20) ---
    Let,
    Emit,
    If,
    Then,
    Else,
    And,
    Or,
    Not,
    Match,
    Use,
    As,
    Fn,
    Trace,
    Null,
    True,
    False,
    Now,
    It,
    Window,
    Pipeline,

    // --- Operators (14) ---
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    EqEq,
    NotEq,
    Gt,
    Lt,
    GtEq,
    LtEq,
    QuestionQuestion,
    FatArrow,
    Eq,

    // --- Delimiters ---
    LParen,
    RParen,
    LBracket,
    RBracket,
    LBrace,
    RBrace,
    Comma,
    Dot,
    Colon,
    Pipe,
    Underscore,
    ColonColon,

    // --- Literals ---
    StringLit(Box<str>),
    IntLit(i64),
    FloatLit(f64),
    DateLit(chrono::NaiveDate),

    // --- Identifiers ---
    Ident(Box<str>),

    // --- Structure ---
    Newline,
    Comment,
    Eof,
    Error,
}

/// Single-pass lexer for CXL source text.
pub struct Lexer<'src> {
    source: &'src [u8],
    pos: usize,
}

impl<'src> Lexer<'src> {
    pub fn new(source: &'src str) -> Result<Self, LexError> {
        if source.len() > MAX_SOURCE_LEN {
            return Err(LexError::SourceTooLarge(source.len()));
        }
        Ok(Self {
            source: source.as_bytes(),
            pos: 0,
        })
    }

    /// Tokenize entire source into a Vec. Convenience method.
    pub fn tokenize(source: &'src str) -> Vec<(Token, Span)> {
        let mut lexer = match Self::new(source) {
            Ok(l) => l,
            Err(_) => return vec![(Token::Error, Span::new(0, source.len()))],
        };
        let mut tokens = Vec::new();
        loop {
            let (tok, span) = lexer.next_token();
            let is_eof = tok == Token::Eof;
            tokens.push((tok, span));
            if is_eof {
                break;
            }
        }
        tokens
    }

    /// Advance and return the next token with its span.
    pub fn next_token(&mut self) -> (Token, Span) {
        self.skip_whitespace();

        if self.pos >= self.source.len() {
            return (Token::Eof, Span::new(self.pos, self.pos));
        }

        let start = self.pos;
        let ch = self.source[self.pos];

        match ch {
            b'\n' => {
                self.pos += 1;
                (Token::Newline, Span::new(start, self.pos))
            }
            b'\r' => {
                self.pos += 1;
                if self.pos < self.source.len() && self.source[self.pos] == b'\n' {
                    self.pos += 1;
                }
                (Token::Newline, Span::new(start, self.pos))
            }

            // Operators and delimiters
            b'+' => self.single(Token::Plus, start),
            b'*' => self.single(Token::Star, start),
            b'/' => self.single(Token::Slash, start),
            b'%' => self.single(Token::Percent, start),
            b'(' => self.single(Token::LParen, start),
            b')' => self.single(Token::RParen, start),
            b'[' => self.single(Token::LBracket, start),
            b']' => self.single(Token::RBracket, start),
            b'{' => self.single(Token::LBrace, start),
            b'}' => self.single(Token::RBrace, start),
            b',' => self.single(Token::Comma, start),
            b'.' => self.single(Token::Dot, start),
            b'|' => self.single(Token::Pipe, start),

            b'-' => self.single(Token::Minus, start),

            b':' => {
                self.pos += 1;
                if self.pos < self.source.len() && self.source[self.pos] == b':' {
                    self.pos += 1;
                    (Token::ColonColon, Span::new(start, self.pos))
                } else {
                    (Token::Colon, Span::new(start, self.pos))
                }
            }

            b'=' => {
                self.pos += 1;
                if self.pos < self.source.len() && self.source[self.pos] == b'=' {
                    self.pos += 1;
                    (Token::EqEq, Span::new(start, self.pos))
                } else if self.pos < self.source.len() && self.source[self.pos] == b'>' {
                    self.pos += 1;
                    (Token::FatArrow, Span::new(start, self.pos))
                } else {
                    (Token::Eq, Span::new(start, self.pos))
                }
            }

            b'!' => {
                self.pos += 1;
                if self.pos < self.source.len() && self.source[self.pos] == b'=' {
                    self.pos += 1;
                    (Token::NotEq, Span::new(start, self.pos))
                } else {
                    (Token::Error, Span::new(start, self.pos))
                }
            }

            b'>' => {
                self.pos += 1;
                if self.pos < self.source.len() && self.source[self.pos] == b'=' {
                    self.pos += 1;
                    (Token::GtEq, Span::new(start, self.pos))
                } else {
                    (Token::Gt, Span::new(start, self.pos))
                }
            }

            b'<' => {
                self.pos += 1;
                if self.pos < self.source.len() && self.source[self.pos] == b'=' {
                    self.pos += 1;
                    (Token::LtEq, Span::new(start, self.pos))
                } else {
                    (Token::Lt, Span::new(start, self.pos))
                }
            }

            b'?' => {
                self.pos += 1;
                if self.pos < self.source.len() && self.source[self.pos] == b'?' {
                    self.pos += 1;
                    (Token::QuestionQuestion, Span::new(start, self.pos))
                } else {
                    (Token::Error, Span::new(start, self.pos))
                }
            }

            // Hash: date literal or comment
            b'#' => self.lex_hash(start),

            // String literals
            b'"' => self.lex_string(b'"', start),
            b'\'' => self.lex_string(b'\'', start),

            // Numbers
            b'0'..=b'9' => self.lex_number(start),

            // Identifiers and keywords
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => self.lex_ident(start),

            // Unknown character → Error, advance one byte
            _ => {
                self.pos += 1;
                (Token::Error, Span::new(start, self.pos))
            }
        }
    }

    fn single(&mut self, tok: Token, start: usize) -> (Token, Span) {
        self.pos += 1;
        (tok, Span::new(start, self.pos))
    }

    fn skip_whitespace(&mut self) {
        while self.pos < self.source.len() {
            match self.source[self.pos] {
                b' ' | b'\t' => self.pos += 1,
                _ => break,
            }
        }
    }

    fn lex_hash(&mut self, start: usize) -> (Token, Span) {
        self.pos += 1; // skip #

        // Check if next char is a digit → date literal attempt
        if self.pos < self.source.len() && self.source[self.pos].is_ascii_digit() {
            return self.lex_date_literal(start);
        }

        // Otherwise it's a line comment
        while self.pos < self.source.len()
            && self.source[self.pos] != b'\n'
            && self.source[self.pos] != b'\r'
        {
            self.pos += 1;
        }
        (Token::Comment, Span::new(start, self.pos))
    }

    fn lex_date_literal(&mut self, start: usize) -> (Token, Span) {
        // We already consumed '#' and saw a digit. Collect chars until closing '#' or EOL/EOF.
        let content_start = self.pos;
        while self.pos < self.source.len()
            && self.source[self.pos] != b'#'
            && self.source[self.pos] != b'\n'
            && self.source[self.pos] != b'\r'
        {
            self.pos += 1;
        }

        if self.pos >= self.source.len() || self.source[self.pos] != b'#' {
            // No closing #
            return (Token::Error, Span::new(start, self.pos));
        }

        let content = &self.source[content_start..self.pos];
        self.pos += 1; // skip closing #

        // Validate as YYYY-MM-DD
        let content_str = match std::str::from_utf8(content) {
            Ok(s) => s,
            Err(_) => return (Token::Error, Span::new(start, self.pos)),
        };

        match chrono::NaiveDate::parse_from_str(content_str, "%Y-%m-%d") {
            Ok(date) => (Token::DateLit(date), Span::new(start, self.pos)),
            Err(_) => (Token::Error, Span::new(start, self.pos)),
        }
    }

    fn lex_string(&mut self, quote: u8, start: usize) -> (Token, Span) {
        self.pos += 1; // skip opening quote
        let mut content = String::new();

        loop {
            if self.pos >= self.source.len() {
                // Unterminated string
                return (Token::Error, Span::new(start, self.pos));
            }

            let ch = self.source[self.pos];

            if ch == quote {
                self.pos += 1; // skip closing quote
                return (
                    Token::StringLit(content.into_boxed_str()),
                    Span::new(start, self.pos),
                );
            }

            if ch == b'\\' {
                self.pos += 1;
                if self.pos >= self.source.len() {
                    return (Token::Error, Span::new(start, self.pos));
                }
                let escaped = self.source[self.pos];
                match escaped {
                    b'\'' => content.push('\''),
                    b'"' => content.push('"'),
                    b'\\' => content.push('\\'),
                    b'n' => content.push('\n'),
                    b't' => content.push('\t'),
                    b'r' => content.push('\r'),
                    _ => {
                        content.push('\\');
                        content.push(escaped as char);
                    }
                }
                self.pos += 1;
            } else if ch == b'\n' || ch == b'\r' {
                // Unterminated — newline before closing quote
                return (Token::Error, Span::new(start, self.pos));
            } else {
                content.push(ch as char);
                self.pos += 1;
            }
        }
    }

    fn lex_number(&mut self, start: usize) -> (Token, Span) {
        // Consume digits
        while self.pos < self.source.len() && self.source[self.pos].is_ascii_digit() {
            self.pos += 1;
        }

        // Check for decimal point
        if self.pos < self.source.len()
            && self.source[self.pos] == b'.'
            && self.pos + 1 < self.source.len()
            && self.source[self.pos + 1].is_ascii_digit()
        {
            self.pos += 1; // skip dot
            while self.pos < self.source.len() && self.source[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            let text = std::str::from_utf8(&self.source[start..self.pos]).unwrap();
            let val: f64 = text.parse().unwrap();
            return (Token::FloatLit(val), Span::new(start, self.pos));
        }

        let text = std::str::from_utf8(&self.source[start..self.pos]).unwrap();
        let val: i64 = text.parse().unwrap();
        (Token::IntLit(val), Span::new(start, self.pos))
    }

    fn lex_ident(&mut self, start: usize) -> (Token, Span) {
        while self.pos < self.source.len()
            && (self.source[self.pos].is_ascii_alphanumeric() || self.source[self.pos] == b'_')
        {
            self.pos += 1;
        }

        let text = std::str::from_utf8(&self.source[start..self.pos]).unwrap();

        // Single underscore is Underscore token
        if text == "_" {
            return (Token::Underscore, Span::new(start, self.pos));
        }

        let tok = match text {
            "let" => Token::Let,
            "emit" => Token::Emit,
            "if" => Token::If,
            "then" => Token::Then,
            "else" => Token::Else,
            "and" => Token::And,
            "or" => Token::Or,
            "not" => Token::Not,
            "match" => Token::Match,
            "use" => Token::Use,
            "as" => Token::As,
            "fn" => Token::Fn,
            "trace" => Token::Trace,
            "null" => Token::Null,
            "true" => Token::True,
            "false" => Token::False,
            "now" => Token::Now,
            "it" => Token::It,
            "window" => Token::Window,
            "pipeline" => Token::Pipeline,
            _ => Token::Ident(text.into()),
        };

        (tok, Span::new(start, self.pos))
    }
}

/// Error returned by `Lexer::new()` for invalid input.
#[derive(Debug)]
pub enum LexError {
    SourceTooLarge(usize),
}

impl std::fmt::Display for LexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LexError::SourceTooLarge(size) => {
                write!(
                    f,
                    "CXL source too large: {} bytes (max {})",
                    size, MAX_SOURCE_LEN
                )
            }
        }
    }
}

impl std::error::Error for LexError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lex_keywords_all_20() {
        let keywords = [
            ("let", Token::Let),
            ("emit", Token::Emit),
            ("if", Token::If),
            ("then", Token::Then),
            ("else", Token::Else),
            ("and", Token::And),
            ("or", Token::Or),
            ("not", Token::Not),
            ("match", Token::Match),
            ("use", Token::Use),
            ("as", Token::As),
            ("fn", Token::Fn),
            ("trace", Token::Trace),
            ("null", Token::Null),
            ("true", Token::True),
            ("false", Token::False),
            ("now", Token::Now),
            ("it", Token::It),
            ("window", Token::Window),
            ("pipeline", Token::Pipeline),
        ];
        assert_eq!(keywords.len(), 20);
        for (src, expected) in keywords {
            let tokens = Lexer::tokenize(src);
            assert_eq!(tokens[0].0, expected, "keyword '{}' failed", src);
        }
    }

    #[test]
    fn test_lex_trace_levels_are_idents() {
        for level in ["info", "warn", "error", "debug"] {
            let tokens = Lexer::tokenize(level);
            assert!(
                matches!(tokens[0].0, Token::Ident(_)),
                "'{}' should be Ident, got {:?}",
                level,
                tokens[0].0
            );
        }
    }

    #[test]
    fn test_lex_operators_single_char() {
        let ops = [
            ("+", Token::Plus),
            ("-", Token::Minus),
            ("*", Token::Star),
            ("/", Token::Slash),
            ("%", Token::Percent),
            (">", Token::Gt),
            ("<", Token::Lt),
        ];
        for (src, expected) in ops {
            let tokens = Lexer::tokenize(src);
            assert_eq!(tokens[0].0, expected, "operator '{}' failed", src);
        }
    }

    #[test]
    fn test_lex_operators_two_char() {
        let ops = [
            ("==", Token::EqEq),
            ("!=", Token::NotEq),
            (">=", Token::GtEq),
            ("<=", Token::LtEq),
            ("??", Token::QuestionQuestion),
            ("=>", Token::FatArrow),
            ("::", Token::ColonColon),
        ];
        for (src, expected) in ops {
            let tokens = Lexer::tokenize(src);
            assert_eq!(tokens[0].0, expected, "operator '{}' failed", src);
        }
    }

    #[test]
    fn test_lex_string_double_quoted() {
        let tokens = Lexer::tokenize(r#""hello \"world\"""#);
        assert_eq!(tokens[0].0, Token::StringLit("hello \"world\"".into()));
    }

    #[test]
    fn test_lex_string_single_quoted() {
        let tokens = Lexer::tokenize(r"'it\'s'");
        assert_eq!(tokens[0].0, Token::StringLit("it's".into()));
    }

    #[test]
    fn test_lex_int_literal() {
        let tokens = Lexer::tokenize("42");
        assert_eq!(tokens[0].0, Token::IntLit(42));
    }

    #[test]
    fn test_lex_float_literal() {
        let tokens = Lexer::tokenize("3.14");
        assert_eq!(tokens[0].0, Token::FloatLit(3.14));
    }

    #[test]
    fn test_lex_date_literal() {
        let tokens = Lexer::tokenize("#2024-01-15#");
        assert_eq!(
            tokens[0].0,
            Token::DateLit(chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap())
        );
    }

    #[test]
    fn test_lex_date_literal_invalid() {
        let tokens = Lexer::tokenize("#9999-99-99#");
        assert_eq!(tokens[0].0, Token::Error);
    }

    #[test]
    fn test_lex_hash_comment() {
        let tokens = Lexer::tokenize("# this is a comment");
        assert_eq!(tokens[0].0, Token::Comment);
    }

    #[test]
    fn test_lex_hash_disambiguation() {
        let date_tokens = Lexer::tokenize("#2024-01-15#");
        assert!(matches!(date_tokens[0].0, Token::DateLit(_)));

        let comment_tokens = Lexer::tokenize("# note");
        assert_eq!(comment_tokens[0].0, Token::Comment);
    }

    #[test]
    fn test_lex_span_tracking() {
        let tokens = Lexer::tokenize("let x = 42");
        assert_eq!(tokens[0].1, Span::new(0, 3)); // "let"
        assert_eq!(tokens[1].1, Span::new(4, 5)); // "x"
        assert_eq!(tokens[2].1, Span::new(6, 7)); // "="
        assert_eq!(tokens[3].1, Span::new(8, 10)); // "42"
    }

    #[test]
    fn test_lex_error_recovery() {
        let tokens = Lexer::tokenize("@42");
        assert_eq!(tokens[0].0, Token::Error);
        assert_eq!(tokens[1].0, Token::IntLit(42));
    }

    #[test]
    fn test_lex_identifier() {
        let tokens = Lexer::tokenize("my_field");
        assert_eq!(tokens[0].0, Token::Ident("my_field".into()));
    }

    #[test]
    fn test_lex_underscore_token() {
        let tokens = Lexer::tokenize("_");
        assert_eq!(tokens[0].0, Token::Underscore);
    }

    #[test]
    fn test_lex_newlines_preserved() {
        let tokens = Lexer::tokenize("a\nb");
        assert!(tokens.iter().any(|(t, _)| *t == Token::Newline));
    }

    #[test]
    fn test_lex_complete_statement() {
        let tokens = Lexer::tokenize("let total = price * qty");
        let kinds: Vec<_> = tokens.iter().map(|(t, _)| t.clone()).collect();
        assert_eq!(
            kinds,
            vec![
                Token::Let,
                Token::Ident("total".into()),
                Token::Eq,
                Token::Ident("price".into()),
                Token::Star,
                Token::Ident("qty".into()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_lex_module_path() {
        let tokens = Lexer::tokenize("use shared.dates as d");
        let kinds: Vec<_> = tokens.iter().map(|(t, _)| t.clone()).collect();
        assert_eq!(
            kinds,
            vec![
                Token::Use,
                Token::Ident("shared".into()),
                Token::Dot,
                Token::Ident("dates".into()),
                Token::As,
                Token::Ident("d".into()),
                Token::Eof,
            ]
        );
    }
}
