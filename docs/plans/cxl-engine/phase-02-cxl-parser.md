# Phase 2: CXL Lexer + Parser

**Status:** 🔲 Not Started
**Depends on:** Phase 1 exit criteria
**Entry criteria:** `cargo build --workspace` succeeds, `clinker-record` types tested
**Prerequisites:** Add `static_assertions = "1"` to `[workspace.dependencies]` in root `Cargo.toml` and `static_assertions = { workspace = true }` to `crates/cxl/Cargo.toml` `[dependencies]`
**Exit criteria:** All CXL spec examples parse to correct ASTs, 80+ parser tests pass

---

## Tasks

### Task 2.1: Lexer/Tokenizer
**Status:** 🔲 Not Started
**Blocked by:** Entry criteria

**Description:**
Build the lexer for CXL in `crates/cxl/src/lexer.rs`. The lexer converts CXL source text
into a flat token stream. Every token carries a `Span` (byte offset start..end) for miette
diagnostics. Error recovery emits `Token::Error` with span, advances one byte, and continues
lexing so the parser receives a complete token stream.

**Implementation notes:**
- `Token` enum variants:
  - **Keywords (20):** `let`, `emit`, `if`, `then`, `else`, `and`, `or`, `not`, `match`, `use`, `as`, `fn`, `trace`, `null`, `true`, `false`, `now`, `it`, `window`, `pipeline`
  - **Operators (14):** `+`, `-`, `*`, `/`, `%`, `==`, `!=`, `>`, `<`, `>=`, `<=`, `??`, `=>`, `=`
  - **Delimiters (16):** `(`, `)`, `[`, `]`, `{`, `}`, `,`, `.`, `:`, `|`, `_`, `::`, `"`, `'`, `#`, newline-related
  - **Literals:** `StringLit(Box<str>)` (single- and double-quoted with `\'` and `\"` escapes, stored unescaped), `IntLit(i64)`, `FloatLit(f64)`, `DateLit(NaiveDate)` in `#YYYY-MM-DD#` format (validated by chrono at lex time — invalid dates emit `Error` token)
  - **Identifiers:** `Ident(Box<str>)`
  - **Structure:** `Newline`, `Comment`, `Eof`, `Error`
- `Span` struct: `start: u32, end: u32` (byte offsets). `u32` supports files up to 4GB.
- The `#` character is context-sensitive: if followed by a digit, lex a date literal `#YYYY-MM-DD#`; otherwise lex a line comment to EOL.
- Date literal validation: the lexer parses the `YYYY-MM-DD` content with `chrono::NaiveDate::parse_from_str`. Invalid dates (e.g. `#9999-99-99#`) produce `Token::Error` with span — this is a compile-time error, not a runtime/DLQ issue.
- Trace levels (`info`, `warn`, `error`, `debug`) are NOT keywords — they lex as `Ident` and are interpreted contextually by the parser only when following `trace`.
- `_` lexes as a distinct `Underscore` token (not `Ident("_")`).
- `::` lexes as a single `ColonColon` token for module paths (`use shared::dates`).
- **Source size guard:** `Lexer::new()` checks source length against `MAX_SOURCE_LEN` (default 64KB). Returns error if exceeded. CXL programs are 10-50 lines; this prevents pathological inputs.
- Hand-rolled lexer (no codegen deps). Single-pass, O(n) in source length.
- Keywords are identified by looking up the identifier text in a `match` statement after lexing the identifier.

#### Sub-tasks
- [ ] **2.1.1** Define `Token` enum (20 keywords, 14 operators, 16 delimiters, 4 literal types, `Ident`, `Newline`, `Comment`, `Eof`, `Error`) and `Span { start: u32, end: u32 }` struct
- [ ] **2.1.2** Implement `Lexer` struct with single-pass `next_token()`. Hand-rolled, O(n). Keyword lookup via `match` on identifier text. Date literal validation with chrono. `#` disambiguation (digit→date, else→comment). `::` as single token. `_` as `Underscore`.
- [ ] **2.1.3** Error recovery: invalid characters produce `Error` token with span, lexer advances one byte and continues. Unterminated strings produce `Error` with span of the opening quote to current position.

#### Code scaffolding
```rust
// Module: crates/cxl/src/lexer.rs

/// Byte-offset span for source mapping and diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: u32,
    pub end: u32,
}

/// CXL token produced by the lexer. Every variant except Eof/Error
/// represents a valid syntactic element of the CXL language.
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // --- Keywords (20) ---
    Let, Emit, If, Then, Else, And, Or, Not, Match, Use, As,
    Fn, Trace, Null, True, False, Now, It, Window, Pipeline,

    // --- Operators (14) ---
    Plus, Minus, Star, Slash, Percent,
    EqEq, NotEq, Gt, Lt, GtEq, LtEq,
    QuestionQuestion, // ??
    FatArrow,         // =>
    Eq,               // =

    // --- Delimiters ---
    LParen, RParen, LBracket, RBracket, LBrace, RBrace,
    Comma, Dot, Colon, Pipe, Underscore,
    ColonColon, // ::

    // --- Literals ---
    StringLit(Box<str>),        // unescaped content
    IntLit(i64),
    FloatLit(f64),
    DateLit(chrono::NaiveDate), // validated at lex time

    // --- Identifiers ---
    Ident(Box<str>),

    // --- Structure ---
    Newline,
    Comment,
    Eof,
    Error,
}

/// Single-pass lexer for CXL source text.
/// Produces a stream of (Token, Span) pairs in O(n) time.
pub struct Lexer<'src> {
    source: &'src str,
    pos: usize,
}

impl<'src> Lexer<'src> {
    pub fn new(source: &'src str) -> Self { todo!() }

    /// Advance and return the next token with its span.
    pub fn next_token(&mut self) -> (Token, Span) { todo!() }

    /// Convenience: tokenize the entire source into a Vec.
    pub fn tokenize(source: &'src str) -> Vec<(Token, Span)> { todo!() }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Token` | `crates/cxl/src/lexer.rs` | Public enum, all variants public |
| `Span` | `crates/cxl/src/lexer.rs` | Re-exported from `crates/cxl/src/lib.rs` — used by AST and parser |
| `Lexer` | `crates/cxl/src/lexer.rs` | Public struct with `new()` and `next_token()` |

#### Intra-phase dependencies
- Requires: Phase 1 exit (workspace builds, `clinker-record` types available)
- Unblocks: Task 2.2

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    /// All 20 keywords lex to distinct token variants (not Ident).
    #[test]
    fn test_lex_keywords_all_20() {
        let keywords = [
            ("let", Token::Let), ("emit", Token::Emit), ("if", Token::If),
            ("then", Token::Then), ("else", Token::Else), ("and", Token::And),
            ("or", Token::Or), ("not", Token::Not), ("match", Token::Match),
            ("use", Token::Use), ("as", Token::As), ("fn", Token::Fn),
            ("trace", Token::Trace), ("null", Token::Null), ("true", Token::True),
            ("false", Token::False), ("now", Token::Now), ("it", Token::It),
            ("window", Token::Window), ("pipeline", Token::Pipeline),
        ];
        for (src, expected) in keywords {
            let tokens = Lexer::tokenize(src);
            assert_eq!(tokens[0].0, expected, "keyword '{}' failed", src);
        }
    }

    /// Trace levels lex as Ident, not keywords.
    #[test]
    fn test_lex_trace_levels_are_idents() {
        for level in ["info", "warn", "error", "debug"] {
            let tokens = Lexer::tokenize(level);
            assert!(matches!(tokens[0].0, Token::Ident(_)), "'{}' should be Ident", level);
        }
    }

    /// Single-char operators each lex correctly.
    #[test]
    fn test_lex_operators_single_char() {
        let ops = [
            ("+", Token::Plus), ("-", Token::Minus), ("*", Token::Star),
            ("/", Token::Slash), ("%", Token::Percent), (">", Token::Gt),
            ("<", Token::Lt),
        ];
        for (src, expected) in ops {
            let tokens = Lexer::tokenize(src);
            assert_eq!(tokens[0].0, expected);
        }
    }

    /// Two-char operators lex as single tokens, not two separate tokens.
    #[test]
    fn test_lex_operators_two_char() {
        let ops = [
            ("==", Token::EqEq), ("!=", Token::NotEq),
            (">=", Token::GtEq), ("<=", Token::LtEq),
            ("??", Token::QuestionQuestion), ("=>", Token::FatArrow),
            ("::", Token::ColonColon),
        ];
        for (src, expected) in ops {
            let tokens = Lexer::tokenize(src);
            assert_eq!(tokens[0].0, expected, "operator '{}' failed", src);
        }
    }

    /// Double-quoted string with escape sequences.
    #[test]
    fn test_lex_string_double_quoted() {
        let tokens = Lexer::tokenize(r#""hello \"world\"""#);
        assert_eq!(tokens[0].0, Token::StringLit("hello \"world\"".into()));
    }

    /// Single-quoted string with escape sequences.
    #[test]
    fn test_lex_string_single_quoted() {
        let tokens = Lexer::tokenize(r"'it\'s'");
        assert_eq!(tokens[0].0, Token::StringLit("it's".into()));
    }

    /// Integer literal parses to IntLit.
    #[test]
    fn test_lex_int_literal() {
        let tokens = Lexer::tokenize("42");
        assert_eq!(tokens[0].0, Token::IntLit(42));
    }

    /// Float literal parses to FloatLit.
    #[test]
    fn test_lex_float_literal() {
        let tokens = Lexer::tokenize("3.14");
        assert_eq!(tokens[0].0, Token::FloatLit(3.14));
    }

    /// Date literal is validated by chrono and stored as NaiveDate.
    #[test]
    fn test_lex_date_literal() {
        let tokens = Lexer::tokenize("#2024-01-15#");
        assert_eq!(tokens[0].0, Token::DateLit(chrono::NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()));
    }

    /// Invalid date literal produces Error token (compile-time error).
    #[test]
    fn test_lex_date_literal_invalid() {
        let tokens = Lexer::tokenize("#9999-99-99#");
        assert_eq!(tokens[0].0, Token::Error);
    }

    /// # followed by non-digit is a line comment.
    #[test]
    fn test_lex_hash_comment() {
        let tokens = Lexer::tokenize("# this is a comment");
        assert_eq!(tokens[0].0, Token::Comment);
    }

    /// Disambiguation: #YYYY-MM-DD# is DateLit; # text is Comment.
    #[test]
    fn test_lex_hash_disambiguation() {
        let date_tokens = Lexer::tokenize("#2024-01-15#");
        assert!(matches!(date_tokens[0].0, Token::DateLit(_)));

        let comment_tokens = Lexer::tokenize("# note");
        assert_eq!(comment_tokens[0].0, Token::Comment);
    }

    /// Every token in `let x = 42` has correct start/end byte offsets.
    #[test]
    fn test_lex_span_tracking() {
        let tokens = Lexer::tokenize("let x = 42");
        assert_eq!(tokens[0].1, Span { start: 0, end: 3 });  // "let"
        assert_eq!(tokens[1].1, Span { start: 4, end: 5 });  // "x"
        assert_eq!(tokens[2].1, Span { start: 6, end: 7 });  // "="
        assert_eq!(tokens[3].1, Span { start: 8, end: 10 }); // "42"
    }

    /// Invalid char produces Error token; next valid token still lexed.
    #[test]
    fn test_lex_error_recovery() {
        let tokens = Lexer::tokenize("@42");
        assert_eq!(tokens[0].0, Token::Error);
        assert_eq!(tokens[1].0, Token::IntLit(42));
    }

    /// Bare identifier lexes to Ident.
    #[test]
    fn test_lex_identifier() {
        let tokens = Lexer::tokenize("my_field");
        assert_eq!(tokens[0].0, Token::Ident("my_field".into()));
    }

    /// Underscore lexes as distinct Underscore token, not Ident("_").
    #[test]
    fn test_lex_underscore_token() {
        let tokens = Lexer::tokenize("_");
        assert_eq!(tokens[0].0, Token::Underscore);
    }

    /// Newlines appear as Newline tokens in the stream.
    #[test]
    fn test_lex_newlines_preserved() {
        let tokens = Lexer::tokenize("a\nb");
        assert!(tokens.iter().any(|(t, _)| *t == Token::Newline));
    }

    /// Complete statement produces correct token sequence.
    #[test]
    fn test_lex_complete_statement() {
        let tokens = Lexer::tokenize("let total = price * qty");
        let kinds: Vec<_> = tokens.iter().map(|(t, _)| t.clone()).collect();
        assert_eq!(kinds, vec![
            Token::Let,
            Token::Ident("total".into()),
            Token::Eq,
            Token::Ident("price".into()),
            Token::Star,
            Token::Ident("qty".into()),
            Token::Eof,
        ]);
    }

    /// Module path with :: lexes correctly.
    #[test]
    fn test_lex_module_path() {
        let tokens = Lexer::tokenize("use shared::dates as d");
        let kinds: Vec<_> = tokens.iter().map(|(t, _)| t.clone()).collect();
        assert_eq!(kinds, vec![
            Token::Use,
            Token::Ident("shared".into()),
            Token::ColonColon,
            Token::Ident("dates".into()),
            Token::As,
            Token::Ident("d".into()),
            Token::Eof,
        ]);
    }
}
```

#### Risk / gotcha
> **`#` followed by digits but not a valid date format:** e.g. `#2024` at EOF, or `#2024-01-`.
> The lexer must not hang or panic. If the `#YYYY-MM-DD#` pattern doesn't complete
> (missing closing `#`, wrong digit count), emit `Token::Error` with span covering
> what was consumed, and resume lexing at the next byte after the error.

**Acceptance criteria:**
- [ ] All 20 keywords lex to distinct token variants
- [ ] All 14 operators lex correctly, including two-character operators (`==`, `!=`, `>=`, `<=`, `??`, `=>`, `::`)
- [ ] `_` lexes as `Underscore` token, not `Ident("_")`
- [ ] Trace levels (`info`, `warn`, `error`, `debug`) lex as `Ident`, not keywords
- [ ] String literals handle both quote styles and escape sequences
- [ ] Integer and float literals parse correctly (including negative via unary minus in parser)
- [ ] Date literal `#2024-01-15#` lexes to `DateLit` with validated `NaiveDate`
- [ ] Invalid date literal `#9999-99-99#` produces `Error` token (compile-time error)
- [ ] `#` followed by non-digit lexes as a line comment
- [ ] Invalid characters produce `Error` token with correct span and lexing continues
- [ ] Every token carries a `Span` with correct byte offsets
- [ ] Module path `use shared::dates as d` lexes to correct token sequence

**Required unit tests (must pass before Task 2.2 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_lex_keywords_all_20` | Each of 20 keyword strings produces its keyword token variant | ⛔ Hard gate |
| `test_lex_trace_levels_are_idents` | `info`, `warn`, `error`, `debug` lex as Ident (contextual, not keywords) | ⛔ Hard gate |
| `test_lex_operators_single_char` | `+`, `-`, `*`, `/`, `%`, `>`, `<` each lex correctly | ⛔ Hard gate |
| `test_lex_operators_two_char` | `==`, `!=`, `>=`, `<=`, `??`, `=>`, `::` lex as single tokens | ⛔ Hard gate |
| `test_lex_string_double_quoted` | `"hello \"world\""` → StringLit with unescaped content | ⛔ Hard gate |
| `test_lex_string_single_quoted` | `'it\'s'` → StringLit with unescaped content | ⛔ Hard gate |
| `test_lex_int_literal` | `42` → IntLit(42) | ⛔ Hard gate |
| `test_lex_float_literal` | `3.14` → FloatLit(3.14) | ⛔ Hard gate |
| `test_lex_date_literal` | `#2024-01-15#` → DateLit(2024-01-15), validated | ⛔ Hard gate |
| `test_lex_date_literal_invalid` | `#9999-99-99#` → Error token | ⛔ Hard gate |
| `test_lex_hash_comment` | `# this is a comment` → Comment token | ⛔ Hard gate |
| `test_lex_hash_disambiguation` | `#2024-01-15#` is DateLit; `# note` is Comment | ⛔ Hard gate |
| `test_lex_span_tracking` | Every token in `let x = 42` has correct start/end byte offsets | ⛔ Hard gate |
| `test_lex_error_recovery` | Invalid char `@` produces Error token; next valid token still lexed | ⛔ Hard gate |
| `test_lex_identifier` | `my_field` → Ident("my_field") | ⛔ Hard gate |
| `test_lex_underscore_token` | `_` → Underscore (not Ident) | ⛔ Hard gate |
| `test_lex_newlines_preserved` | Newlines appear as `Newline` tokens in the stream | ⛔ Hard gate |
| `test_lex_complete_statement` | `let total = price * qty` → correct 7-token sequence | ⛔ Hard gate |
| `test_lex_module_path` | `use shared::dates as d` → correct Use/Ident/ColonColon/Ident/As/Ident sequence | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 2.2 status remains `Blocked` until all tests above pass.

---

### Task 2.2: AST Node Types
**Status:** ⛔ Blocked (waiting on Task 2.1)
**Blocked by:** Task 2.1 — lexer must produce correct tokens

**Description:**
Define all AST node types in `crates/cxl/src/ast.rs`. These are the untyped syntax tree
nodes produced by the parser. Every node carries a `Span` for error reporting and
source mapping. All AST types must be `Send + Sync` for `Arc<TypedAst>` sharing
across rayon worker threads.

**Implementation notes:**
- Top-level: `Program { statements: Vec<Statement>, span: Span }`
- `Statement` enum:
  - `Let { name: Box<str>, expr: Expr, span: Span }`
  - `Emit { name: Box<str>, expr: Expr, span: Span }` — name is the output field name (`emit out_field = expr`)
  - `Trace { level: Option<TraceLevel>, guard: Option<Box<Expr>>, message: Expr, span: Span }` — optional level and conditional guard
  - `UseStmt { path: Vec<Box<str>>, alias: Option<Box<str>>, span: Span }` — `use shared::dates as d` → path=["shared","dates"], alias=Some("d")
  - `ExprStmt { expr: Expr, span: Span }`
- `TraceLevel` enum: `Trace` (default), `Debug`, `Info`, `Warn`, `Error`
- `Expr` enum (all carry `span: Span`):
  - `Binary { op: BinOp, lhs: Box<Expr>, rhs: Box<Expr> }`
  - `Unary { op: UnaryOp, operand: Box<Expr> }`
  - `Literal { value: LiteralValue }` (Int, Float, String, Date, Bool, Null)
  - `FieldRef { name: Box<str> }` — bare identifier referencing a record field
  - `QualifiedFieldRef { source: Box<str>, field: Box<str> }` — `source.field` cross-source lookup
  - `MethodCall { receiver: Box<Expr>, method: Box<str>, args: Vec<Expr> }`
  - `Match { subject: Option<Box<Expr>>, arms: Vec<MatchArm> }` — both condition-match and value-match forms
  - `IfThenElse { condition: Box<Expr>, then_branch: Box<Expr>, else_branch: Option<Box<Expr>> }`
  - `Coalesce { lhs: Box<Expr>, rhs: Box<Expr> }` — the `??` operator
  - `WindowCall { function: Box<str>, args: Vec<Expr> }` — `window.sum(field)`, etc.
  - `PipelineAccess { field: Box<str> }` — `pipeline.total_count`, etc.
  - `Wildcard` — the `_` catch-all in match arms
- `MatchArm { pattern: Expr, body: Expr, span: Span }`
- `FnDecl { name: Box<str>, params: Vec<Box<str>>, body: Box<Expr>, span: Span }` — single expression body, enforces purity structurally (`fn name(params) = expr`)
- `ModuleDecl { name: Box<str>, functions: Vec<FnDecl>, span: Span }` — for module files
- `BinOp` enum: `Add`, `Sub`, `Mul`, `Div`, `Mod`, `Eq`, `Neq`, `Gt`, `Lt`, `Gte`, `Lte`, `And`, `Or`
- `UnaryOp` enum: `Neg`, `Not`
- Use `Box<Expr>` for recursive variants to keep `Expr` size manageable.
- Compile-time `static_assertions::assert_impl_all!(Program: Send, Sync)` for thread safety.

#### Sub-tasks
- [ ] **2.2.1** Define all AST node types: `Program`, `Statement` (with corrected `Emit`, `Trace`, `UseStmt`), `Expr` (all variants), `MatchArm`, `FnDecl` (single-expr body), `ModuleDecl`, `TraceLevel`, `BinOp`, `UnaryOp`, `LiteralValue`. Derive `Clone + Debug` on all types.
- [ ] **2.2.2** Add compile-time assertions: `static_assertions::assert_impl_all!(Program: Send, Sync)` for all AST types. Size assertion `std::mem::size_of::<Expr>() <= 64`.

#### Code scaffolding
```rust
// Module: crates/cxl/src/ast.rs
use crate::lexer::Span;

/// A complete CXL program — a sequence of statements.
#[derive(Debug, Clone)]
pub struct Program {
    pub statements: Vec<Statement>,
    pub span: Span,
}

/// Trace severity level. Default is Trace when omitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraceLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// Top-level CXL statement.
#[derive(Debug, Clone)]
pub enum Statement {
    Let { name: Box<str>, expr: Expr, span: Span },
    Emit { name: Box<str>, expr: Expr, span: Span },
    Trace {
        level: Option<TraceLevel>,
        guard: Option<Box<Expr>>,
        message: Expr,
        span: Span,
    },
    UseStmt {
        path: Vec<Box<str>>,          // ["shared", "dates"]
        alias: Option<Box<str>>,       // Some("d") for `as d`
        span: Span,
    },
    ExprStmt { expr: Expr, span: Span },
}

/// CXL expression — the core of the language. All variants carry a Span.
#[derive(Debug, Clone)]
pub enum Expr {
    Binary { op: BinOp, lhs: Box<Expr>, rhs: Box<Expr>, span: Span },
    Unary { op: UnaryOp, operand: Box<Expr>, span: Span },
    Literal { value: LiteralValue, span: Span },
    FieldRef { name: Box<str>, span: Span },
    QualifiedFieldRef { source: Box<str>, field: Box<str>, span: Span },
    MethodCall { receiver: Box<Expr>, method: Box<str>, args: Vec<Expr>, span: Span },
    Match { subject: Option<Box<Expr>>, arms: Vec<MatchArm>, span: Span },
    IfThenElse { condition: Box<Expr>, then_branch: Box<Expr>, else_branch: Option<Box<Expr>>, span: Span },
    Coalesce { lhs: Box<Expr>, rhs: Box<Expr>, span: Span },
    WindowCall { function: Box<str>, args: Vec<Expr>, span: Span },
    PipelineAccess { field: Box<str>, span: Span },
    Wildcard { span: Span }, // _ catch-all in match arms
}

#[derive(Debug, Clone)]
pub struct MatchArm {
    pub pattern: Expr,
    pub body: Expr,
    pub span: Span,
}

/// Module-level function declaration. Pure: single expression body.
#[derive(Debug, Clone)]
pub struct FnDecl {
    pub name: Box<str>,
    pub params: Vec<Box<str>>,
    pub body: Box<Expr>,  // fn name(params) = expr
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct ModuleDecl {
    pub name: Box<str>,
    pub functions: Vec<FnDecl>,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add, Sub, Mul, Div, Mod,
    Eq, Neq, Gt, Lt, Gte, Lte,
    And, Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp { Neg, Not }

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Int(i64),
    Float(f64),
    String(Box<str>),
    Date(chrono::NaiveDate),
    Bool(bool),
    Null,
}

// Compile-time thread safety assertions
static_assertions::assert_impl_all!(Program: Send, Sync);
static_assertions::assert_impl_all!(Statement: Send, Sync);
static_assertions::assert_impl_all!(Expr: Send, Sync);
static_assertions::assert_impl_all!(FnDecl: Send, Sync);
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Program`, `Statement`, `Expr` | `crates/cxl/src/ast.rs` | All AST node types |
| `TraceLevel`, `BinOp`, `UnaryOp` | `crates/cxl/src/ast.rs` | Enums used by AST |
| `LiteralValue` | `crates/cxl/src/ast.rs` | Literal variants for Expr::Literal |
| `FnDecl`, `ModuleDecl`, `MatchArm` | `crates/cxl/src/ast.rs` | Supporting AST structures |

#### Intra-phase dependencies
- Requires: Task 2.1 (`Span` type, `Token` enum for reference)
- Unblocks: Task 2.3

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Span;

    /// Program with 3 mixed statements constructs and is accessible.
    #[test]
    fn test_ast_program_construction() {
        let span = Span { start: 0, end: 10 };
        let program = Program {
            statements: vec![
                Statement::Let { name: "x".into(), expr: Expr::Literal { value: LiteralValue::Int(1), span }, span },
                Statement::Emit { name: "out".into(), expr: Expr::FieldRef { name: "x".into(), span }, span },
                Statement::ExprStmt { expr: Expr::Literal { value: LiteralValue::Null, span }, span },
            ],
            span,
        };
        assert_eq!(program.statements.len(), 3);
    }

    /// Emit node has a name field (output field binding).
    #[test]
    fn test_ast_emit_has_name() {
        let span = Span { start: 0, end: 20 };
        let stmt = Statement::Emit {
            name: "member_name".into(),
            expr: Expr::FieldRef { name: "label".into(), span },
            span,
        };
        if let Statement::Emit { name, .. } = stmt {
            assert_eq!(&*name, "member_name");
        }
    }

    /// Trace node has level and guard fields.
    #[test]
    fn test_ast_trace_with_level_and_guard() {
        let span = Span { start: 0, end: 30 };
        let stmt = Statement::Trace {
            level: Some(TraceLevel::Warn),
            guard: Some(Box::new(Expr::Literal { value: LiteralValue::Bool(true), span })),
            message: Expr::Literal { value: LiteralValue::String("alert".into()), span },
            span,
        };
        if let Statement::Trace { level, guard, .. } = stmt {
            assert_eq!(level, Some(TraceLevel::Warn));
            assert!(guard.is_some());
        }
    }

    /// UseStmt with structured path and alias.
    #[test]
    fn test_ast_use_stmt_structured() {
        let span = Span { start: 0, end: 25 };
        let stmt = Statement::UseStmt {
            path: vec!["shared".into(), "dates".into()],
            alias: Some("d".into()),
            span,
        };
        if let Statement::UseStmt { path, alias, .. } = stmt {
            assert_eq!(path.len(), 2);
            assert_eq!(&*path[0], "shared");
            assert_eq!(alias.as_deref(), Some("d"));
        }
    }

    /// FnDecl has single expression body (not Vec<Statement>).
    #[test]
    fn test_ast_fn_decl_single_expr() {
        let span = Span { start: 0, end: 20 };
        let decl = FnDecl {
            name: "double".into(),
            params: vec!["x".into()],
            body: Box::new(Expr::Binary {
                op: BinOp::Mul,
                lhs: Box::new(Expr::FieldRef { name: "x".into(), span }),
                rhs: Box::new(Expr::Literal { value: LiteralValue::Int(2), span }),
                span,
            }),
            span,
        };
        assert_eq!(&*decl.name, "double");
        assert_eq!(decl.params.len(), 1);
    }

    /// Nested Binary expr constructs without stack overflow.
    #[test]
    fn test_ast_expr_binary_nested() {
        let span = Span { start: 0, end: 1 };
        let leaf = || Box::new(Expr::Literal { value: LiteralValue::Int(1), span });
        let _deep = Expr::Binary {
            op: BinOp::Add,
            lhs: Box::new(Expr::Binary { op: BinOp::Mul, lhs: leaf(), rhs: leaf(), span }),
            rhs: leaf(),
            span,
        };
    }

    /// Expr size stays within budget for cache efficiency.
    #[test]
    fn test_ast_expr_size() {
        assert!(std::mem::size_of::<Expr>() <= 64,
            "Expr is {} bytes, budget is 64", std::mem::size_of::<Expr>());
    }

    /// All core AST types implement Send + Sync (compile-time, but also runtime check).
    #[test]
    fn test_ast_all_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Program>();
        assert_send_sync::<Statement>();
        assert_send_sync::<Expr>();
        assert_send_sync::<MatchArm>();
        assert_send_sync::<FnDecl>();
    }

    /// Match with subject: None (condition-match) constructs correctly.
    #[test]
    fn test_ast_match_condition_form() {
        let span = Span { start: 0, end: 1 };
        let m = Expr::Match {
            subject: None,
            arms: vec![MatchArm {
                pattern: Expr::Literal { value: LiteralValue::Bool(true), span },
                body: Expr::Literal { value: LiteralValue::String("yes".into()), span },
                span,
            }],
            span,
        };
        if let Expr::Match { subject, .. } = m { assert!(subject.is_none()); }
    }

    /// Match with subject: Some (value-match) constructs correctly.
    #[test]
    fn test_ast_match_value_form() {
        let span = Span { start: 0, end: 1 };
        let m = Expr::Match {
            subject: Some(Box::new(Expr::FieldRef { name: "status".into(), span })),
            arms: vec![],
            span,
        };
        if let Expr::Match { subject, .. } = m { assert!(subject.is_some()); }
    }

    /// Debug format is human-readable.
    #[test]
    fn test_ast_debug_output() {
        let span = Span { start: 0, end: 2 };
        let expr = Expr::Literal { value: LiteralValue::Int(42), span };
        let dbg = format!("{:?}", expr);
        assert!(dbg.contains("42"));
    }
}
```

#### Risk / gotcha
> **`Expr` size budget (≤ 64 bytes):** The `Match` variant has `Option<Box<Expr>>` + `Vec<MatchArm>` + `Span`.
> A `Vec` is 24 bytes, `Option<Box<Expr>>` is 8, `Span` is 8 = 40 bytes for Match.
> Largest variant is likely `IfThenElse` at ~40 bytes (3 Box + Span) or `Binary` at ~32 bytes.
> Should be within budget, but verify with the `test_ast_expr_size` test early. If over budget,
> box the `arms` vec: `Box<Vec<MatchArm>>` saves 16 bytes (pointer vs inline Vec).

**Acceptance criteria:**
- [ ] All grammar productions from spec have corresponding AST node types
- [ ] Every AST node variant carries a `Span`
- [ ] `Emit` has `name: Box<str>` for output field binding
- [ ] `Trace` has `level: Option<TraceLevel>` and `guard: Option<Box<Expr>>`
- [ ] `FnDecl` has `body: Box<Expr>` (single expression, not `Vec<Statement>`)
- [ ] `UseStmt` has `path: Vec<Box<str>>` and `alias: Option<Box<str>>`
- [ ] `Expr` size is ≤ 64 bytes (use `Box` for recursive variants)
- [ ] All AST types are `Send + Sync` (compile-time assertion)
- [ ] `MatchArm` supports both condition-match and value-match forms
- [ ] `Clone` and `Debug` derived on all AST types

**Required unit tests (must pass before Task 2.3 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_ast_program_construction` | `Program` with 3 mixed statements constructs and is accessible | ⛔ Hard gate |
| `test_ast_emit_has_name` | `Emit` node has `name` field for output field binding | ⛔ Hard gate |
| `test_ast_trace_with_level_and_guard` | `Trace` node has level and guard fields | ⛔ Hard gate |
| `test_ast_use_stmt_structured` | `UseStmt` has `Vec<Box<str>>` path and `Option<Box<str>>` alias | ⛔ Hard gate |
| `test_ast_fn_decl_single_expr` | `FnDecl` has `Box<Expr>` body (not Vec<Statement>) | ⛔ Hard gate |
| `test_ast_expr_binary_nested` | Nested `Binary { Binary, Literal }` constructs without stack overflow | ⛔ Hard gate |
| `test_ast_expr_size` | `std::mem::size_of::<Expr>() <= 64` | ⛔ Hard gate |
| `test_ast_all_send_sync` | `Program`, `Statement`, `Expr`, `MatchArm`, `FnDecl` are all Send + Sync | ⛔ Hard gate |
| `test_ast_match_condition_form` | `Match` with `subject: None` (condition-match) constructs correctly | ⛔ Hard gate |
| `test_ast_match_value_form` | `Match` with `subject: Some(expr)` (value-match) constructs correctly | ⛔ Hard gate |
| `test_ast_debug_output` | `Debug` format of a simple Expr is human-readable | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 2.3 status remains `Blocked` until all tests above pass.

---

### Task 2.3: Pratt Parser
**Status:** ⛔ Blocked (waiting on Task 2.2)
**Blocked by:** Task 2.2 — AST types must be defined

**Description:**
Implement a hand-rolled Pratt parser in `crates/cxl/src/parser.rs` that consumes the token
stream from Task 2.1 and produces the untyped AST from Task 2.2. Covers all ~30 grammar
productions from spec §5.2. Includes panic-mode error recovery and miette-powered diagnostics.

**Implementation notes:**
- **Binding power table** (spec §5.3 precedence, corrected — 10 levels, lowest BP to highest):

  | BP | Operators | Associativity | Notes |
  |---|---|---|---|
  | 1-2 | `??` (coalesce) | Right | Loosest — `a ?? if x then y else z` works naturally |
  | 3-4 | `if/then/else` | Right (keyword nud) | Condition parsed at `or` BP floor, branches at `coalesce` BP |
  | 5-6 | `or` | Left | |
  | 7-8 | `and` | Left | |
  | 9-10 | `not` | Prefix | Tight — `not a and b` = `(not a) and b` |
  | 11-12 | `==`, `!=`, `>`, `<`, `>=`, `<=` | Non-associative | `a == b == c` → parse error with specific diagnostic |
  | 13-14 | `+`, `-` | Left | |
  | 15-16 | `*`, `/`, `%` | Left | |
  | 17-18 | Unary `-` | Prefix | |
  | 19-20 | `.method()`, `.field` | Left (postfix) | Tightest binding |

- **Pratt core:** `parse_expr(min_bp: u8) -> Result<Expr>` dispatches on token type.
  - Null-denotation (prefix/atom): literals, identifiers, `(`, `if`, `match`, `not`, unary `-`, `window.*`, `pipeline.*`
  - Left-denotation (infix/postfix): binary ops, `??`, `.method()`, `.field`
- **`if/then/else` as nud handler:** When the Pratt loop sees `if`, parse condition with `parse_expr(or_bp)` (stops before `or`), consume `then`, parse then-branch with `parse_expr(coalesce_bp)`, optionally consume `else` and parse else-branch with `parse_expr(coalesce_bp)`.
- **Non-associative comparisons:** Both structural (BP gap prevents chaining) AND explicit check. When a led handler parses a comparison and the next token is also a comparison, emit diagnostic: "Comparisons are not chainable — use `(a == b) and (b == c)` instead."
- **Recursive descent** for top-level structures:
  - `parse_program()` → loop of `parse_statement()` separated by newlines
  - `parse_statement()` → dispatches on `let` / `emit` / `trace` / `use` / `fn` / expression
  - `parse_emit()` → consume `emit`, expect `Ident` (name), expect `=`, parse expr
  - `parse_trace()` → consume `trace`, check for contextual level ident (`info`/`warn`/`error`/`debug`), check for `if` guard, parse message expr
  - `parse_use()` → consume `use`, parse `IDENT (:: IDENT)*` path, optional `as IDENT` alias
  - `parse_match()` → both condition-match (`match {`) and value-match (`match expr {`) — peek after `match`: if `{`, condition form; else parse subject expr then expect `{`
  - `parse_fn_decl()` → `fn name(params) = expr` — single expression body
- **Error recovery:** On syntax error, record a `ParseError` (with span, message, expected tokens),
  skip tokens until the next `Newline` or `Eof`, then continue parsing. The parser returns
  `ParseResult { ast: Program, errors: Vec<ParseError> }` — partial ASTs with collected errors.
- **Diagnostics (4-part miette):** Each `ParseError` maps to a miette diagnostic with:
  `what` (error title), `where` (source span highlighted), `why` (explanation of the grammar
  violation), `how_to_fix` (suggested correction).
- Use `peek()` / `advance()` / `expect()` helpers on the token stream. No backtracking needed.

#### Sub-tasks
- [ ] **2.3.1** Implement binding power table with corrected spec precedence (10 levels). Define `fn prefix_bp(token) -> Option<((), u8)>` and `fn infix_bp(token) -> Option<(u8, u8)>` and `fn postfix_bp(token) -> Option<(u8, ())>`. `if/then/else` as nud at BP 3-4.
- [ ] **2.3.2** Implement Pratt core: `parse_expr(min_bp)` with nud/led dispatch. Nud: literals, idents, `(`, `if`, `match`, `not`, unary `-`, `window.*`, `pipeline.*`. Led: binary ops, `??`, `.method()`, `.field`. Non-associative comparison check.
- [ ] **2.3.3** Implement recursive descent for top-level: `parse_program()`, `parse_statement()`, `parse_emit()`, `parse_trace()` (contextual level + guard), `parse_use()` (path segments + alias), `parse_match()` (both forms), `parse_fn_decl()` (single-expr body).
- [ ] **2.3.4** Error recovery: `ParseError` with span/message/expected, skip to `Newline`/`Eof`, continue. Miette diagnostics with what/where/why/how_to_fix. Return `ParseResult { ast, errors }`.

#### Code scaffolding
```rust
// Module: crates/cxl/src/parser.rs
use crate::ast::*;
use crate::lexer::{Token, Span, Lexer};

/// Result of parsing: a (possibly partial) AST plus collected errors.
pub struct ParseResult {
    pub ast: Program,
    pub errors: Vec<ParseError>,
}

/// A parse error with source location and diagnostic information.
pub struct ParseError {
    pub span: Span,
    pub message: String,
    pub expected: Vec<&'static str>,
    // miette fields
    pub why: String,
    pub how_to_fix: String,
}

/// Maximum expression nesting depth. Prevents stack overflow on malicious input.
const MAX_DEPTH: u32 = 256;

/// Hand-rolled Pratt parser for CXL.
pub struct Parser<'src> {
    tokens: Vec<(Token, Span)>,
    pos: usize,
    source: &'src str,
    errors: Vec<ParseError>,
    depth: u32, // recursion depth counter for parse_expr
}

impl<'src> Parser<'src> {
    /// Parse CXL source text into an AST.
    pub fn parse(source: &'src str) -> ParseResult { todo!() }

    /// Pratt expression parser with minimum binding power.
    fn parse_expr(&mut self, min_bp: u8) -> Result<Expr, ParseError> { todo!() }

    fn parse_statement(&mut self) -> Result<Statement, ParseError> { todo!() }
    fn parse_emit(&mut self) -> Result<Statement, ParseError> { todo!() }
    fn parse_trace(&mut self) -> Result<Statement, ParseError> { todo!() }
    fn parse_use(&mut self) -> Result<Statement, ParseError> { todo!() }
    fn parse_match(&mut self) -> Result<Expr, ParseError> { todo!() }
    fn parse_fn_decl(&mut self) -> Result<FnDecl, ParseError> { todo!() }

    // Token stream helpers
    fn peek(&self) -> &Token { todo!() }
    fn advance(&mut self) -> (Token, Span) { todo!() }
    fn expect(&mut self, expected: Token) -> Result<Span, ParseError> { todo!() }
}
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `Parser` | `crates/cxl/src/parser.rs` | Public struct, `parse()` is the entry point |
| `ParseResult` | `crates/cxl/src/parser.rs` | Returned by `Parser::parse()` |
| `ParseError` | `crates/cxl/src/parser.rs` | Implements `miette::Diagnostic` |

#### Intra-phase dependencies
- Requires: Task 2.1 (token stream), Task 2.2 (AST types)
- Unblocks: Task 2.4

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::*;

    /// let x = 42 → Let { name: "x", expr: Literal(42) }
    #[test]
    fn test_parse_let_simple() {
        let result = Parser::parse("let x = 42");
        assert!(result.errors.is_empty());
        // assert Let statement with name "x" and IntLit(42)
    }

    /// emit out = name → Emit { name: "out", expr: FieldRef("name") }
    #[test]
    fn test_parse_emit_with_name() {
        let result = Parser::parse("emit out = name");
        assert!(result.errors.is_empty());
        // assert Emit with name "out" and FieldRef("name")
    }

    /// trace warn if amount > 10000 "Large transaction"
    #[test]
    fn test_parse_trace_with_level_and_guard() {
        let result = Parser::parse(r#"trace warn if amount > 10000 "Large transaction""#);
        assert!(result.errors.is_empty());
        // assert Trace with level=Warn, guard=Some(Binary(Gt)), message=StringLit
    }

    /// trace "simple message" — no level, no guard
    #[test]
    fn test_parse_trace_simple() {
        let result = Parser::parse(r#"trace "simple""#);
        assert!(result.errors.is_empty());
        // assert Trace with level=None, guard=None
    }

    /// use shared::dates as d
    #[test]
    fn test_parse_use_with_alias() {
        let result = Parser::parse("use shared::dates as d");
        assert!(result.errors.is_empty());
        // assert UseStmt with path=["shared","dates"], alias=Some("d")
    }

    /// use validators (no alias)
    #[test]
    fn test_parse_use_simple() {
        let result = Parser::parse("use validators");
        assert!(result.errors.is_empty());
        // assert UseStmt with path=["validators"], alias=None
    }

    /// 1 + 2 * 3 → Add(1, Mul(2, 3))
    #[test]
    fn test_parse_binary_precedence() {
        let result = Parser::parse("let r = 1 + 2 * 3");
        assert!(result.errors.is_empty());
        // assert Add(Lit(1), Mul(Lit(2), Lit(3)))
    }

    /// 1 - 2 - 3 → Sub(Sub(1, 2), 3) (left-associative)
    #[test]
    fn test_parse_binary_left_assoc() {
        let result = Parser::parse("let r = 1 - 2 - 3");
        assert!(result.errors.is_empty());
        // assert Sub(Sub(1, 2), 3)
    }

    /// a ?? b ?? c → Coalesce(a, Coalesce(b, c)) (right-associative)
    #[test]
    fn test_parse_coalesce_right_assoc() {
        let result = Parser::parse("let r = a ?? b ?? c");
        assert!(result.errors.is_empty());
        // assert Coalesce(a, Coalesce(b, c))
    }

    /// not active → Unary(Not, FieldRef("active"))
    #[test]
    fn test_parse_unary_not() {
        let result = Parser::parse("let r = not active");
        assert!(result.errors.is_empty());
    }

    /// -price → Unary(Neg, FieldRef("price"))
    #[test]
    fn test_parse_unary_neg() {
        let result = Parser::parse("let r = -price");
        assert!(result.errors.is_empty());
    }

    /// a == b == c → parse error (non-associative)
    #[test]
    fn test_parse_comparison_non_assoc() {
        let result = Parser::parse("let r = a == b == c");
        assert!(!result.errors.is_empty());
        assert!(result.errors[0].message.contains("chainable")
            || result.errors[0].how_to_fix.contains("and"));
    }

    /// (1 + 2) * 3 → Mul(Add(1, 2), 3)
    #[test]
    fn test_parse_parentheses() {
        let result = Parser::parse("let r = (1 + 2) * 3");
        assert!(result.errors.is_empty());
    }

    /// name.upper() → MethodCall { receiver: FieldRef, method: "upper" }
    #[test]
    fn test_parse_method_call() {
        let result = Parser::parse("let r = name.upper()");
        assert!(result.errors.is_empty());
    }

    /// name.trim().upper() → nested MethodCall
    #[test]
    fn test_parse_method_chain() {
        let result = Parser::parse("let r = name.trim().upper()");
        assert!(result.errors.is_empty());
    }

    /// record.name → QualifiedFieldRef { source: "record", field: "name" }
    #[test]
    fn test_parse_field_access() {
        let result = Parser::parse("let r = record.name");
        assert!(result.errors.is_empty());
    }

    /// name.substring(0, 5) → MethodCall with 2 args
    #[test]
    fn test_parse_method_with_args() {
        let result = Parser::parse("let r = name.substring(0, 5)");
        assert!(result.errors.is_empty());
    }

    /// if x > 0 then "pos" else "neg"
    #[test]
    fn test_parse_if_then_else() {
        let result = Parser::parse(r#"let r = if x > 0 then "pos" else "neg""#);
        assert!(result.errors.is_empty());
    }

    /// if x > 0 then "pos" (no else)
    #[test]
    fn test_parse_if_then_no_else() {
        let result = Parser::parse(r#"let r = if x > 0 then "pos""#);
        assert!(result.errors.is_empty());
    }

    /// Condition-match with 3 arms
    #[test]
    fn test_parse_match_condition() {
        let result = Parser::parse(r#"let r = match { score >= 90 => "A", score >= 80 => "B", _ => "F", }"#);
        assert!(result.errors.is_empty());
    }

    /// Value-match: match status { ... }
    #[test]
    fn test_parse_match_value() {
        let result = Parser::parse(r#"let r = match status { "gold" => 1, _ => 0, }"#);
        assert!(result.errors.is_empty());
    }

    /// window.sum(amount) → WindowCall
    #[test]
    fn test_parse_window_call() {
        let result = Parser::parse("let r = window.sum(amount)");
        assert!(result.errors.is_empty());
    }

    /// pipeline.total_count → PipelineAccess
    #[test]
    fn test_parse_pipeline_access() {
        let result = Parser::parse("let r = pipeline.total_count");
        assert!(result.errors.is_empty());
    }

    /// nickname ?? "N/A"
    #[test]
    fn test_parse_coalesce_with_literal() {
        let result = Parser::parse(r#"let r = nickname ?? "N/A""#);
        assert!(result.errors.is_empty());
    }

    /// Complex mixed expression
    #[test]
    fn test_parse_complex_expression() {
        let result = Parser::parse(r#"let r = if age >= 18 and not blocked then name.upper() else "MINOR""#);
        assert!(result.errors.is_empty());
    }

    /// Error on line 2 of 3 → lines 1 and 3 still parse, 1 error collected
    #[test]
    fn test_parse_error_recovery_continues() {
        let result = Parser::parse("let x = 1\nlet = = =\nlet y = 2");
        assert_eq!(result.errors.len(), 1);
        // ast should have statements from lines 1 and 3
    }

    /// ParseError has miette diagnostic fields
    #[test]
    fn test_parse_error_miette_diagnostic() {
        let result = Parser::parse("let = ");
        assert!(!result.errors.is_empty());
        let err = &result.errors[0];
        assert!(!err.message.is_empty());
        assert!(!err.why.is_empty());
        assert!(!err.how_to_fix.is_empty());
    }

    /// 5-statement program parses completely
    #[test]
    fn test_parse_multiline_program() {
        let src = "let x = 1\nlet y = 2\nlet z = x + y\nemit out = z\ntrace \"done\"";
        let result = Parser::parse(src);
        assert!(result.errors.is_empty());
        assert_eq!(result.ast.statements.len(), 5);
    }

    /// fn greet(name) = name.upper()
    #[test]
    fn test_parse_fn_decl() {
        let result = Parser::parse("fn greet(name) = name.upper()");
        assert!(result.errors.is_empty());
        // assert FnDecl with single-expression body
    }

    /// #2024-01-15# in expression → Literal(Date)
    #[test]
    fn test_parse_date_literal() {
        let result = Parser::parse("let d = #2024-01-15#");
        assert!(result.errors.is_empty());
    }

    /// Both quote styles in expressions
    #[test]
    fn test_parse_string_literals() {
        let result = Parser::parse(r#"let a = "hello"
let b = 'world'"#);
        assert!(result.errors.is_empty());
    }

    /// Every CXL example from spec §5.2 parses without error
    #[test]
    fn test_parse_all_spec_examples() {
        let spec_example = r#"let eff_date = Effective_Date.to_date("%m/%d/%Y")
let end_date = End_Date.try_date("%m/%d/%Y") ?? #2099-12-31#
let prev_end = window.lag(1).End_Date ?? #2099-12-31#
let is_first = window.lag(1) == null
let gap_days = if not is_first then eff_date.diff_days(prev_end) else 0
let status = if end_date < now then "terminated" else "active"
let plan_count = window.count()
let member_label = Last_Name.upper() + ", " + First_Name
emit member_name = member_label
emit enrollment_status = status
emit gap_from_prior = gap_days
emit total_plan_enrollments = plan_count"#;
        let result = Parser::parse(spec_example);
        assert!(result.errors.is_empty(), "Spec example failed: {:?}", result.errors);
    }

    /// ?? is the loosest operator (spec §5.3)
    #[test]
    fn test_parse_coalesce_is_loosest() {
        // a ?? b + c should parse as a ?? (b + c), not (a ?? b) + c
        let result = Parser::parse("let r = a ?? b + c");
        assert!(result.errors.is_empty());
        // assert Coalesce(a, Add(b, c))
    }

    /// not binds tighter than and: not a and b = (not a) and b
    #[test]
    fn test_parse_not_binds_tight() {
        let result = Parser::parse("let r = not a and b");
        assert!(result.errors.is_empty());
        // assert And(Unary(Not, a), b)
    }
}
```

#### Risk / gotcha
> **`match` parsing ambiguity:** `match { ... }` (condition form, no subject) vs `match expr { ... }`
> (value form). The parser peeks after consuming `match`: if the next token is `{`, it's condition
> form; otherwise parse an expression then expect `{`. This is unambiguous because `{` is not a
> valid expression start in CXL (no block expressions).

> **`if/then/else` dangling else:** `if a then if b then x else y` — does `else` bind to inner
> or outer `if`? Since `if/then/else` is right-associative and branches are parsed at `coalesce`
> BP floor, the `else` binds to the innermost `if` (standard dangling-else resolution).

**Acceptance criteria:**
- [ ] All 10 precedence levels parse correctly with associativity rules
- [ ] `??` is the loosest operator — `a ?? b + c` = `a ?? (b + c)`
- [ ] `not` is tight — `not a and b` = `(not a) and b`
- [ ] `let`, `emit` (with name), `trace` (with level/guard), `use` (with path/alias) parse correctly
- [ ] `fn` declarations parse with single-expression body (`fn name(params) = expr`)
- [ ] `match` expression parses both condition-match and value-match forms
- [ ] `if/then/else` parses with optional else branch
- [ ] `.method(args)` and `.field` postfix parsing works at highest binding power
- [ ] `??` coalesce operator is right-associative
- [ ] Parenthesized expressions override precedence
- [ ] Non-associative comparisons rejected with specific diagnostic
- [ ] Error recovery: syntax error in line 2 does not prevent parsing line 3
- [ ] Miette diagnostics include what/where/why/how-to-fix
- [ ] All CXL spec examples from §5.2 parse to correct ASTs

**Required unit tests (must pass before Task 2.4 unlocks):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_parse_let_simple` | `let x = 42` → `Let { name: "x", expr: Literal(42) }` | ⛔ Hard gate |
| `test_parse_emit_with_name` | `emit out = name` → `Emit { name: "out", expr: FieldRef("name") }` | ⛔ Hard gate |
| `test_parse_trace_with_level_and_guard` | `trace warn if amount > 10000 "msg"` → Trace with level+guard | ⛔ Hard gate |
| `test_parse_trace_simple` | `trace "msg"` → Trace with level=None, guard=None | ⛔ Hard gate |
| `test_parse_use_with_alias` | `use shared::dates as d` → UseStmt with path+alias | ⛔ Hard gate |
| `test_parse_use_simple` | `use validators` → UseStmt with path=["validators"], alias=None | ⛔ Hard gate |
| `test_parse_binary_precedence` | `1 + 2 * 3` → `Add(1, Mul(2, 3))` not `Mul(Add(1, 2), 3)` | ⛔ Hard gate |
| `test_parse_binary_left_assoc` | `1 - 2 - 3` → `Sub(Sub(1, 2), 3)` not `Sub(1, Sub(2, 3))` | ⛔ Hard gate |
| `test_parse_coalesce_right_assoc` | `a ?? b ?? c` → `Coalesce(a, Coalesce(b, c))` | ⛔ Hard gate |
| `test_parse_coalesce_is_loosest` | `a ?? b + c` → `Coalesce(a, Add(b, c))` | ⛔ Hard gate |
| `test_parse_not_binds_tight` | `not a and b` → `And(Not(a), b)` | ⛔ Hard gate |
| `test_parse_unary_not` | `not active` → `Unary(Not, FieldRef("active"))` | ⛔ Hard gate |
| `test_parse_unary_neg` | `-price` → `Unary(Neg, FieldRef("price"))` | ⛔ Hard gate |
| `test_parse_comparison_non_assoc` | `a == b == c` produces parse error with actionable message | ⛔ Hard gate |
| `test_parse_parentheses` | `(1 + 2) * 3` → `Mul(Add(1, 2), 3)` | ⛔ Hard gate |
| `test_parse_method_call` | `name.upper()` → `MethodCall { receiver: FieldRef, method: "upper" }` | ⛔ Hard gate |
| `test_parse_method_chain` | `name.trim().upper()` → nested MethodCall | ⛔ Hard gate |
| `test_parse_field_access` | `record.name` → `QualifiedFieldRef { source: "record", field: "name" }` | ⛔ Hard gate |
| `test_parse_method_with_args` | `name.substring(0, 5)` → MethodCall with 2 args | ⛔ Hard gate |
| `test_parse_if_then_else` | `if x > 0 then "pos" else "neg"` → IfThenElse with both branches | ⛔ Hard gate |
| `test_parse_if_then_no_else` | `if x > 0 then "pos"` → IfThenElse with else_branch: None | ⛔ Hard gate |
| `test_parse_match_condition` | Condition-match with 3 arms parses correctly | ⛔ Hard gate |
| `test_parse_match_value` | Value-match `match status` with arms parses correctly | ⛔ Hard gate |
| `test_parse_window_call` | `window.sum(amount)` → WindowCall | ⛔ Hard gate |
| `test_parse_pipeline_access` | `pipeline.total_count` → PipelineAccess | ⛔ Hard gate |
| `test_parse_coalesce_with_literal` | `nickname ?? "N/A"` → Coalesce | ⛔ Hard gate |
| `test_parse_complex_expression` | `if age >= 18 and not blocked then name.upper() else "MINOR"` | ⛔ Hard gate |
| `test_parse_error_recovery_continues` | Syntax error on line 2 of 3 → line 1 and 3 still parse, 1 error collected | ⛔ Hard gate |
| `test_parse_error_miette_diagnostic` | ParseError produces diagnostic with what/where/why/how_to_fix fields | ⛔ Hard gate |
| `test_parse_multiline_program` | 5-statement program → Program with 5 statements, all correct | ⛔ Hard gate |
| `test_parse_fn_decl` | `fn greet(name) = name.upper()` → FnDecl with single-expr body | ⛔ Hard gate |
| `test_parse_date_literal` | `#2024-01-15#` in expression context → Literal(Date) | ⛔ Hard gate |
| `test_parse_string_literals` | Both single- and double-quoted strings in expressions | ⛔ Hard gate |
| `test_parse_all_spec_examples` | Every CXL example from spec §5.7 parses without error | ⛔ Hard gate |

> ⛔ **Hard gate:** Task 2.4 status remains `Blocked` until all tests above pass.

---

### Task 2.4: Built-in Function Registry
**Status:** ⛔ Blocked (waiting on Task 2.3)
**Blocked by:** Task 2.3 — parser must produce correct ASTs

**Description:**
Build the built-in function/method registry in `crates/cxl/src/builtins.rs`. This is a
lookup table mapping method names to their type signatures `(receiver_type, arg_types,
return_type)` for all 81 built-in methods. Uses two separate hashmaps: one for scalar
methods (70), one for window functions (11). Used by the type checker (Phase 3), not by
evaluation.

**Implementation notes:**
- Registry struct: `BuiltinRegistry` with separate lookups for scalar methods vs window functions.
  - `lookup_method(&self, name: &str) -> Option<&BuiltinDef>` — scalar methods (70)
  - `lookup_window(&self, name: &str) -> Option<&BuiltinDef>` — window functions (11)
- `BuiltinDef { name: &'static str, receiver: TypeTag, args: Vec<TypeTag>, return_type: TypeTag, category: Category }`.
- `TypeTag` enum: `String`, `Int`, `Float`, `Bool`, `Date`, `DateTime`, `Null`, `Any`, `Array`, `Numeric` (Int|Float union).
- `Category` enum for documentation/grouping:
  - **String (24):** `upper`, `lower`, `trim`, `trim_start`, `trim_end`, `starts_with`, `ends_with`, `contains`, `replace`, `substring`, `left`, `right`, `pad_left`, `pad_right`, `repeat`, `reverse`, `length`, `split`, `join`, `matches`, `format`, `concat`, `find`, `capture`
  - **Path (5):** `file_name`, `file_stem`, `extension`, `parent`, `parent_name`
  - **Array (2):** `join`, `length`
  - **Numeric (8):** `abs`, `ceil`, `floor`, `round`, `round_to`, `clamp`, `min`, `max`
  - **Date (13):** `year`, `month`, `day`, `hour`, `minute`, `second`, `add_days`, `add_months`, `add_years`, `diff_days`, `diff_months`, `diff_years`, `format_date`
  - **Conversion strict (6):** `to_int`, `to_float`, `to_string`, `to_bool`, `to_date`, `to_datetime`
  - **Conversion lenient (5):** `try_int`, `try_float`, `try_bool`, `try_date`, `try_datetime`
  - **Introspection (4):** `type_of`, `is_null`, `is_empty`, `catch`
  - **Debug (1):** `debug`
  - **Window aggregate (5):** `sum`, `avg`, `min`, `max`, `count`
  - **Window positional (4):** `first`, `last`, `lag`, `lead`
  - **Window iterable (4):** `any`, `all`, `collect`, `distinct`
- `min`/`max` appear in both Numeric (scalar) and Window aggregate categories — no key collision because they live in separate hashmaps (`lookup_method` vs `lookup_window`).
- Populate registry at construction time (not lazily). Use `ahash::HashMap` for O(1) lookup.
- Registry is immutable after construction (`&self` methods only). Must be `Send + Sync`.
- `BuiltinDef` includes `min_args: usize` and `max_args: Option<usize>` (None = variadic) to represent optional parameters (e.g. `substring(start, len?)`, `concat(b, ...)`).
- **Scalar methods: 70** (24 string + 5 path + 2 array + 8 numeric + 13 date + 6 strict conv + 5 lenient conv + 4 introspection + 1 debug + 2 overlap from min/max counted once)
- **Window functions: 11** (5 aggregate + 4 positional + 4 iterable - 2 min/max counted separately)
- **Total unique method names: 81**

#### Sub-tasks
- [ ] **2.4.1** Define `BuiltinRegistry`, `BuiltinDef` (with `min_args`/`max_args` for arity), `TypeTag`, `Category` types. Two `ahash::HashMap`s: `methods` (70 scalar) and `window_fns` (11 window). `Send + Sync` assertion.
- [ ] **2.4.2** Populate all 81 methods with correct `(receiver, args, min_args, max_args, return_type)` signatures. Validate counts per category match the spec. Add `lookup_method()` and `lookup_window()` accessors.

#### Code scaffolding
```rust
// Module: crates/cxl/src/builtins.rs

/// Type tags for method signature validation (used by Phase 3 type checker).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeTag {
    String, Int, Float, Bool, Date, DateTime,
    Null, Any, Array, Numeric, // Numeric = Int | Float union
}

/// Method category for documentation and grouping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    String, Path, Array, Numeric, Date,
    ConversionStrict, ConversionLenient,
    Introspection, Debug,
    WindowAggregate, WindowPositional, WindowIterable,
}

/// Definition of a single built-in method or window function.
#[derive(Debug, Clone)]
pub struct BuiltinDef {
    pub name: &'static str,
    pub receiver: TypeTag,
    pub args: Vec<TypeTag>,
    pub min_args: usize,           // minimum required args
    pub max_args: Option<usize>,   // None = variadic (e.g. concat)
    pub return_type: TypeTag,
    pub category: Category,
}

/// Registry of all built-in methods and window functions.
/// Two separate hashmaps: scalar methods and window functions.
/// Immutable after construction. Send + Sync for cross-thread sharing.
pub struct BuiltinRegistry {
    methods: ahash::HashMap<&'static str, BuiltinDef>,    // 70 scalar methods
    window_fns: ahash::HashMap<&'static str, BuiltinDef>, // 11 window functions
}

impl BuiltinRegistry {
    /// Construct the registry with all 81 built-in methods populated.
    pub fn new() -> Self { todo!() }

    /// Look up a scalar method by name (e.g. "upper", "substring", "to_int").
    pub fn lookup_method(&self, name: &str) -> Option<&BuiltinDef> {
        self.methods.get(name)
    }

    /// Look up a window function by name (e.g. "sum", "lag", "any").
    pub fn lookup_window(&self, name: &str) -> Option<&BuiltinDef> {
        self.window_fns.get(name)
    }

    /// Total number of registered methods (scalar + window).
    pub fn total_count(&self) -> usize {
        self.methods.len() + self.window_fns.len()
    }
}

// Compile-time thread safety assertion
static_assertions::assert_impl_all!(BuiltinRegistry: Send, Sync);
```

#### Module / file map
| Item | Path | Notes |
|------|------|-------|
| `BuiltinRegistry` | `crates/cxl/src/builtins.rs` | Public struct, constructed once and shared |
| `BuiltinDef` | `crates/cxl/src/builtins.rs` | Method signature definition |
| `TypeTag` | `crates/cxl/src/builtins.rs` | Re-exported; used by Phase 3 type checker |
| `Category` | `crates/cxl/src/builtins.rs` | Grouping enum for documentation |

#### Intra-phase dependencies
- Requires: Task 2.3 (parser produces correct ASTs with MethodCall/WindowCall distinction)
- Unblocks: Phase 3 (type checker uses registry for signature validation)

#### Expanded test stubs
```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> BuiltinRegistry { BuiltinRegistry::new() }

    /// All 24 string methods registered with correct category.
    #[test]
    fn test_registry_all_string_methods() {
        let r = registry();
        let string_methods = [
            "upper", "lower", "trim", "trim_start", "trim_end",
            "starts_with", "ends_with", "contains", "replace", "substring",
            "left", "right", "pad_left", "pad_right", "repeat", "reverse",
            "length", "split", "join", "matches", "format", "concat",
            "find", "capture",
        ];
        for name in string_methods {
            let def = r.lookup_method(name).unwrap_or_else(|| panic!("missing string method: {}", name));
            assert_eq!(def.category, Category::String, "wrong category for {}", name);
        }
        assert_eq!(string_methods.len(), 24);
    }

    /// All 5 path methods registered with spec naming.
    #[test]
    fn test_registry_all_path_methods() {
        let r = registry();
        for name in ["file_name", "file_stem", "extension", "parent", "parent_name"] {
            let def = r.lookup_method(name).unwrap_or_else(|| panic!("missing path method: {}", name));
            assert_eq!(def.category, Category::Path);
        }
    }

    /// All 2 array methods registered.
    #[test]
    fn test_registry_all_array_methods() {
        let r = registry();
        for name in ["join", "length"] {
            // Array methods may share names with string methods — check receiver type
            let def = r.lookup_method(name).expect("missing array method");
            // Note: join/length are overloaded (String + Array). Registry stores one entry;
            // type checker disambiguates by receiver type at call site.
            assert!(def.receiver == TypeTag::String || def.receiver == TypeTag::Array);
        }
    }

    /// All 8 numeric methods registered.
    #[test]
    fn test_registry_all_numeric_methods() {
        let r = registry();
        for name in ["abs", "ceil", "floor", "round", "round_to", "clamp", "min", "max"] {
            let def = r.lookup_method(name).unwrap_or_else(|| panic!("missing numeric method: {}", name));
            assert_eq!(def.category, Category::Numeric);
        }
    }

    /// All 13 date methods registered.
    #[test]
    fn test_registry_all_date_methods() {
        let r = registry();
        let date_methods = [
            "year", "month", "day", "hour", "minute", "second",
            "add_days", "add_months", "add_years",
            "diff_days", "diff_months", "diff_years", "format_date",
        ];
        for name in date_methods {
            let def = r.lookup_method(name).unwrap_or_else(|| panic!("missing date method: {}", name));
            assert_eq!(def.category, Category::Date);
        }
        assert_eq!(date_methods.len(), 13);
    }

    /// All 6 strict conversion methods registered.
    #[test]
    fn test_registry_all_conversion_strict() {
        let r = registry();
        for name in ["to_int", "to_float", "to_string", "to_bool", "to_date", "to_datetime"] {
            let def = r.lookup_method(name).unwrap_or_else(|| panic!("missing strict conv: {}", name));
            assert_eq!(def.category, Category::ConversionStrict);
        }
    }

    /// All 5 lenient conversion methods registered.
    #[test]
    fn test_registry_all_conversion_lenient() {
        let r = registry();
        for name in ["try_int", "try_float", "try_bool", "try_date", "try_datetime"] {
            let def = r.lookup_method(name).unwrap_or_else(|| panic!("missing lenient conv: {}", name));
            assert_eq!(def.category, Category::ConversionLenient);
        }
    }

    /// All 4 introspection methods registered.
    #[test]
    fn test_registry_all_introspection() {
        let r = registry();
        for name in ["type_of", "is_null", "is_empty", "catch"] {
            let def = r.lookup_method(name).unwrap_or_else(|| panic!("missing introspection: {}", name));
            assert_eq!(def.category, Category::Introspection);
        }
    }

    /// Debug method registered.
    #[test]
    fn test_registry_debug_method() {
        let r = registry();
        let def = r.lookup_method("debug").expect("missing debug method");
        assert_eq!(def.category, Category::Debug);
    }

    /// All 5 window aggregate methods in window registry.
    #[test]
    fn test_registry_all_window_aggregate() {
        let r = registry();
        for name in ["sum", "avg", "min", "max", "count"] {
            let def = r.lookup_window(name).unwrap_or_else(|| panic!("missing window agg: {}", name));
            assert_eq!(def.category, Category::WindowAggregate);
        }
    }

    /// All 4 window positional methods in window registry.
    #[test]
    fn test_registry_all_window_positional() {
        let r = registry();
        for name in ["first", "last", "lag", "lead"] {
            let def = r.lookup_window(name).unwrap_or_else(|| panic!("missing window pos: {}", name));
            assert_eq!(def.category, Category::WindowPositional);
        }
    }

    /// All 4 window iterable methods in window registry.
    #[test]
    fn test_registry_all_window_iterable() {
        let r = registry();
        for name in ["any", "all", "collect", "distinct"] {
            let def = r.lookup_window(name).unwrap_or_else(|| panic!("missing window iter: {}", name));
            assert_eq!(def.category, Category::WindowIterable);
        }
    }

    /// Total registered methods equals 81.
    #[test]
    fn test_registry_total_count() {
        let r = registry();
        assert_eq!(r.total_count(), 81, "expected 81, got {}", r.total_count());
    }

    /// Unknown method returns None.
    #[test]
    fn test_registry_lookup_returns_none() {
        let r = registry();
        assert!(r.lookup_method("nonexistent").is_none());
        assert!(r.lookup_window("nonexistent").is_none());
    }

    /// upper → receiver: String, args: [], return: String
    #[test]
    fn test_registry_signature_upper() {
        let r = registry();
        let def = r.lookup_method("upper").unwrap();
        assert_eq!(def.receiver, TypeTag::String);
        assert!(def.args.is_empty());
        assert_eq!(def.return_type, TypeTag::String);
    }

    /// substring → receiver: String, args: [Int, Int], return: String
    #[test]
    fn test_registry_signature_substring() {
        let r = registry();
        let def = r.lookup_method("substring").unwrap();
        assert_eq!(def.receiver, TypeTag::String);
        assert_eq!(def.args, vec![TypeTag::Int, TypeTag::Int]);
        assert_eq!(def.return_type, TypeTag::String);
    }

    /// round → receiver: Float, args: [Int], return: Float
    #[test]
    fn test_registry_signature_round() {
        let r = registry();
        let def = r.lookup_method("round").unwrap();
        assert_eq!(def.receiver, TypeTag::Float);
        assert_eq!(def.return_type, TypeTag::Float);
    }

    /// min/max exist in BOTH scalar and window registries (no collision).
    #[test]
    fn test_registry_min_max_dual() {
        let r = registry();
        let scalar_min = r.lookup_method("min").unwrap();
        assert_eq!(scalar_min.category, Category::Numeric);

        let window_min = r.lookup_window("min").unwrap();
        assert_eq!(window_min.category, Category::WindowAggregate);
    }

    /// BuiltinRegistry is Send + Sync.
    #[test]
    fn test_registry_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BuiltinRegistry>();
    }
}
```

#### Risk / gotcha
> **Overloaded method names (`join`, `length`, `min`, `max`):** `join` and `length` appear on
> both String and Array receivers. The scalar registry stores a single entry — the type checker
> (Phase 3) must disambiguate by the receiver's type at the call site. If this becomes unwieldy,
> the registry can switch to `(name, receiver_type)` composite keys. For `min`/`max`, the split
> registries handle it cleanly — scalar in `methods`, aggregate in `window_fns`.

**Acceptance criteria:**
- [ ] All 81 methods registered across 12 categories
- [ ] Scalar methods (70) accessible via `lookup_method()`
- [ ] Window functions (11) accessible via `lookup_window()`
- [ ] `lookup_method("upper")` returns correct signature: `(String, [], String)`
- [ ] `lookup_method("substring")` returns correct signature: `(String, [Int, Int], String)`
- [ ] `lookup_method("nonexistent")` returns `None`
- [ ] `lookup_window("nonexistent")` returns `None`
- [ ] `min`/`max` accessible in both registries without collision
- [ ] Every category has the correct method count
- [ ] Registry is `Send + Sync`
- [ ] Window `any`/`all` methods registered in WindowIterable category
- [ ] Debug method `debug` is a passthrough (returns receiver type)

**Required unit tests (must pass before Phase 2 exit):**

| Test name | What it verifies | Gate |
|-----------|-----------------|------|
| `test_registry_all_string_methods` | All 24 string methods registered with correct category | ⛔ Hard gate |
| `test_registry_all_path_methods` | All 5 path methods registered with spec naming | ⛔ Hard gate |
| `test_registry_all_array_methods` | All 2 array methods registered | ⛔ Hard gate |
| `test_registry_all_numeric_methods` | All 8 numeric methods registered (incl clamp, round_to) | ⛔ Hard gate |
| `test_registry_all_date_methods` | All 13 date methods registered | ⛔ Hard gate |
| `test_registry_all_conversion_strict` | All 6 strict conversion methods registered | ⛔ Hard gate |
| `test_registry_all_conversion_lenient` | All 5 lenient conversion methods registered | ⛔ Hard gate |
| `test_registry_all_introspection` | All 4 introspection methods registered | ⛔ Hard gate |
| `test_registry_debug_method` | `debug` method registered as passthrough | ⛔ Hard gate |
| `test_registry_all_window_aggregate` | All 5 window aggregate methods in window registry | ⛔ Hard gate |
| `test_registry_all_window_positional` | All 4 window positional methods in window registry | ⛔ Hard gate |
| `test_registry_all_window_iterable` | All 4 window iterable methods (any, all, collect, distinct) | ⛔ Hard gate |
| `test_registry_total_count` | Total registered methods equals 81 | ⛔ Hard gate |
| `test_registry_lookup_returns_none` | Unknown method name returns None in both registries | ⛔ Hard gate |
| `test_registry_signature_upper` | `upper` → receiver: String, args: [], return: String | ⛔ Hard gate |
| `test_registry_signature_substring` | `substring` → receiver: String, args: [Int, Int], return: String | ⛔ Hard gate |
| `test_registry_signature_round` | `round` → receiver: Float, args: [Int], return: Float | ⛔ Hard gate |
| `test_registry_min_max_dual` | `min`/`max` in both scalar (Numeric) and window (Aggregate) registries | ⛔ Hard gate |
| `test_registry_send_sync` | `BuiltinRegistry` is Send + Sync | ⛔ Hard gate |

> ⛔ **Hard gate:** Phase 2 exit criteria remain unmet until all tests above pass.

---

## Intra-Phase Dependency Graph

```
Task 2.1 (Lexer) ──→ Task 2.2 (AST) ──→ Task 2.3 (Parser) ──→ Task 2.4 (Registry)
```

Critical path: 2.1 → 2.2 → 2.3 → 2.4 (fully sequential)
No parallelizable tasks within this phase.

---

## Decisions Log (drill-phase — 2026-03-29)

| # | Decision | Rationale | Affects |
|---|---------|-----------|---------|
| 1 | `Emit` gets `name: Box<str>` field | Spec grammar: `emit IDENT "=" expr`. Name is the output field binding. | Task 2.2, 2.3 |
| 2 | `Trace` gets `level: Option<TraceLevel>`, `guard: Option<Box<Expr>>`, `message: Expr` | Spec grammar has optional level and conditional guard. Evaluator needs level for routing. | Task 2.2, 2.3 |
| 3 | `FnDecl` gets `body: Box<Expr>` (single expression) | Spec says `fn name(params) = expr`. Enforces purity structurally — no emit/trace inside functions. | Task 2.2, 2.3 |
| 4 | `UseStmt` gets `path: Vec<Box<str>>`, `alias: Option<Box<str>>` | Spec: `use IDENT::IDENT ("as" IDENT)?`. Parser decomposes path into segments. | Task 2.1, 2.2, 2.3 |
| 5 | Trace levels (`info`/`warn`/`error`/`debug`) are contextual, not keywords | Common field names like `error` and `info` shouldn't be reserved. Parser interprets after `trace`. | Task 2.1, 2.3 |
| 6 | `_` lexes as distinct `Underscore` token | Unambiguous match-arm wildcard parsing, no string comparison needed. | Task 2.1, 2.3 |
| 7 | Keywords: 20 (original 19 + `as`) | `as` needed for use-stmt aliases. `_` is a delimiter token, not a keyword. | Task 2.1 |
| 8 | Keep `Box<str>` for identifiers (no interning) | CXL programs are tiny (10-50 lines), lexer runs once. Interning adds complexity for negligible gain. | Task 2.1 |
| 9 | Date literals validated by chrono in lexer | Invalid dates in CXL source are compile-time errors. `DateLit` token always holds valid `NaiveDate`. | Task 2.1 |
| 10 | Binding power table corrected to match spec PEG | Plan had `??` at level 5 (wrong), spec puts it loosest. `not` is tight (just below comparisons). | Task 2.3 |
| 11 | `if/then/else` is Pratt nud handler at BP 3-4 | Condition at `or` BP floor, branches at `coalesce` BP floor. Clean Pratt integration. | Task 2.3 |
| 12 | Non-associative comparisons: BP + explicit check | BP system prevents structurally, explicit check adds actionable error message. | Task 2.3 |
| 13 | Builtin registry aligned to spec with plan-preferred names | Plan names kept for name diffs (e.g. `substring`, `length`, `type_of`, `debug`). Spec methods added where missing. | Task 2.4 |
| 14 | String methods: 24 (spec 16 + plan extras + missing spec methods) | Added `concat`, `find`, `capture` from spec. Kept plan's `trim_start`, `trim_end`, `left`, `right`, `repeat`, `reverse`, `format`. | Task 2.4 |
| 15 | Path methods: spec naming (`file_name`, `file_stem`, `parent`, `parent_name`) | Spec naming convention preferred over plan's `basename`/`dirname`/`stem`. | Task 2.4 |
| 16 | Window iterable: all 4 (`any`, `all`, `collect`, `distinct`) | Spec's `any`/`all` (boolean predicates) + plan's `collect`/`distinct` (value accumulators). | Task 2.4 |
| 17 | Date methods: 13 (plan 10 + spec's `add_years`, `diff_months`, `diff_years`) | Merged both. Kept plan's `format_date` name and `hour`/`minute`/`second`. | Task 2.4 |
| 18 | Introspection: 4 methods (`type_of`, `is_null`, `is_empty`, `catch`) | Plan's 3 + spec's `catch`. Both emptiness and null-coalesce sugar. | Task 2.4 |
| 19 | Debug method named `debug` (plan name, not spec's `log`) | Consistent with preference to keep plan names for name diffs. Same passthrough semantics. | Task 2.4 |
| 20 | Separate registries for scalar methods vs window functions | Two hashmaps: `lookup_method()` and `lookup_window()`. Resolves `min`/`max` ambiguity cleanly. | Task 2.4 |
| 21 | Add `Expr::Wildcard { span }` for `_` in match arms | `Underscore` token needs AST representation. Wildcard is explicit and smallest variant. | Task 2.2, 2.3 |
| 22 | Parser recursion depth limit `MAX_DEPTH = 256` | Prevents stack overflow on malicious deeply-nested input. Standard parser security. | Task 2.3 |
| 23 | Add `MAX_SOURCE_LEN = 64KB` guard to lexer | CXL programs are 10-50 lines. Prevents pathological input from consuming memory. | Task 2.1 |
| 24 | Add `clamp(min, max)` and `round_to(n)` to Numeric methods | Phase 3.4 expects them. Total methods: 81 (70 scalar + 11 window). | Task 2.4 |
| 25 | Add `min_args`/`max_args` to `BuiltinDef` | Needed for optional params (`substring(start, len?)`), variadic (`concat(b, ...)`). | Task 2.4 |
| 26 | Add `static_assertions` crate to workspace deps | Required for module-scope Send+Sync assertions. Must be added before Phase 2 starts. | Prerequisite |

## Assumptions Log (drill-phase — 2026-03-29)

| # | Assumption | Basis | Risk if wrong |
|---|-----------|-------|---------------|
| 1 | `Expr` fits in ≤ 64 bytes with all variants | Size estimation: largest variant ~40 bytes with Box fields | If over budget, box `Vec<MatchArm>` in Match variant to recover ~16 bytes |
| 2 | Overloaded method names (`join`, `length`) can share single registry entry | Type checker disambiguates by receiver type at Phase 3 | If unwieldy, switch to `(name, receiver_type)` composite keys |
| 3 | `static_assertions` crate available in workspace deps | Used for Send+Sync compile-time checks | Add to workspace Cargo.toml if missing |
