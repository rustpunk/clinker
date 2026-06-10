use crate::ast::*;
use crate::lexer::{Lexer, Span, Token};

/// Maximum expression nesting depth. Prevents stack overflow on malicious input.
const MAX_DEPTH: u32 = 256;

/// Returns true for identifiers that are aggregate function names. Used by
/// NUD lookahead in `parse_nud()` to branch to `parse_agg_call()` instead of
/// treating the identifier as a field reference.
///
/// Aggregate function set: sum, count, avg, min, max, collect, weighted_avg.
/// `first`/`last` are window-only (spec grammar) and are NOT aggregate names.
pub(crate) fn is_aggregate_name(name: &str) -> bool {
    matches!(
        name,
        "sum" | "count" | "avg" | "min" | "max" | "collect" | "weighted_avg"
    )
}

/// Result of parsing: a (possibly partial) AST plus collected errors.
pub struct ParseResult {
    pub ast: Program,
    pub errors: Vec<ParseError>,
    /// Total number of AST nodes created. Useful for pre-sizing side-tables.
    pub node_count: u32,
}

/// A parse error with source location and diagnostic information.
#[derive(Debug)]
pub struct ParseError {
    pub span: Span,
    pub message: String,
    pub expected: Vec<&'static str>,
    pub why: String,
    pub how_to_fix: String,
}

/// Result of parsing a CXL module file (.cxl).
pub struct ModuleParseResult {
    pub module: Module,
    pub errors: Vec<ParseError>,
    /// Total number of AST nodes created. Useful for pre-sizing side-tables.
    pub node_count: u32,
}

/// Hand-rolled Pratt parser for CXL.
pub struct Parser {
    tokens: Vec<(Token, Span)>,
    pos: usize,
    errors: Vec<ParseError>,
    depth: u32,
    next_id: u32,
    /// Current `emit each` block nesting depth. Fan-out may nest (an
    /// `emit each` inside another `emit each` body, fanning out within
    /// fan-out for one trigger row); the cumulative `max_expansion`
    /// ceiling — not a parser-level ban — gates total output cardinality.
    /// The depth is still capped at [`MAX_EMIT_EACH_DEPTH`] so adversarial
    /// input cannot drive `parse_emit_each`'s statement recursion into a
    /// stack overflow, the same stack-safety guarantee `depth` gives the
    /// expression parser.
    emit_each_depth: u32,
}

/// Maximum `emit each` block nesting depth. Bounds `parse_emit_each`'s
/// statement-level recursion so deeply nested fan-out in malicious input
/// cannot overflow the parser stack. Real document fan-out (article →
/// section → tag) is a handful of levels; 32 is far beyond any legitimate
/// pipeline yet small enough to keep recursion shallow.
const MAX_EMIT_EACH_DEPTH: u32 = 32;

// Binding power pairs (left_bp, right_bp).
// Lower number = looser binding. Right-associative: right_bp < left_bp.
// Left-associative: right_bp = left_bp + 1.

fn infix_bp(tok: &Token) -> Option<(u8, u8)> {
    match tok {
        Token::QuestionQuestion => Some((2, 1)), // right-assoc, loosest
        Token::Or => Some((5, 6)),
        Token::And => Some((7, 8)),
        // Comparisons: non-associative (left_bp == right_bp means neither side can chain)
        Token::EqEq | Token::NotEq | Token::Gt | Token::Lt | Token::GtEq | Token::LtEq => {
            Some((11, 12))
        }
        Token::Plus | Token::Minus => Some((13, 14)),
        Token::Star | Token::Slash | Token::Percent => Some((15, 16)),
        Token::Dot => Some((19, 20)), // postfix field/method, tightest
        // Bracket-index access mirrors postfix-dot precedence so chains
        // like `record.field[0]` and `arr[0].name` parse without parens.
        Token::LBracket => Some((19, 20)),
        _ => None,
    }
}

fn prefix_bp(tok: &Token) -> Option<u8> {
    match tok {
        Token::Not => Some(10),   // between and(8) and comparisons(11)
        Token::Minus => Some(18), // between mul(16) and postfix(19)
        _ => None,
    }
}

fn is_comparison(tok: &Token) -> bool {
    matches!(
        tok,
        Token::EqEq | Token::NotEq | Token::Gt | Token::Lt | Token::GtEq | Token::LtEq
    )
}

fn token_to_binop(tok: &Token) -> Option<BinOp> {
    match tok {
        Token::Plus => Some(BinOp::Add),
        Token::Minus => Some(BinOp::Sub),
        Token::Star => Some(BinOp::Mul),
        Token::Slash => Some(BinOp::Div),
        Token::Percent => Some(BinOp::Mod),
        Token::EqEq => Some(BinOp::Eq),
        Token::NotEq => Some(BinOp::Neq),
        Token::Gt => Some(BinOp::Gt),
        Token::Lt => Some(BinOp::Lt),
        Token::GtEq => Some(BinOp::Gte),
        Token::LtEq => Some(BinOp::Lte),
        Token::And => Some(BinOp::And),
        Token::Or => Some(BinOp::Or),
        _ => None,
    }
}

impl Parser {
    pub fn parse(source: &str) -> ParseResult {
        let tokens = Lexer::tokenize(source);
        let mut parser = Parser {
            tokens,
            pos: 0,
            errors: Vec::new(),
            depth: 0,
            next_id: 0,
            emit_each_depth: 0,
        };
        let start_span = parser.current_span();
        let mut statements = Vec::new();
        let mut seen_non_use = false;

        parser.skip_newlines();
        while !parser.at_eof() {
            // Check: use must appear before let/emit/trace
            if *parser.peek() == Token::Use && seen_non_use {
                parser.errors.push(parser.error(
                    "use must appear before let/emit/trace",
                    "All use statements must come at the top of the transform, before any other statements",
                    "Move this use statement to the top of the file",
                ));
                parser.recover_to_newline();
                parser.skip_newlines();
                continue;
            }
            match parser.parse_statement() {
                Ok(stmt) => {
                    if !matches!(stmt, Statement::UseStmt { .. }) {
                        seen_non_use = true;
                    }
                    statements.push(stmt);
                }
                Err(e) => {
                    seen_non_use = true;
                    parser.errors.push(e);
                    parser.recover_to_newline();
                }
            }
            parser.skip_newlines();
        }

        let end_span = parser.current_span();
        let node_count = parser.next_id;
        ParseResult {
            ast: Program {
                statements,
                span: Span::new(start_span.start as usize, end_span.end as usize),
            },
            errors: parser.errors,
            node_count,
        }
    }

    /// Parse a .cxl module file. Only `fn` and `let` are accepted.
    /// `emit`, `trace`, and `use` produce immediate parse errors.
    pub fn parse_module(source: &str) -> ModuleParseResult {
        let tokens = Lexer::tokenize(source);
        let mut parser = Parser {
            tokens,
            pos: 0,
            errors: Vec::new(),
            depth: 0,
            next_id: 0,
            emit_each_depth: 0,
        };
        let start_span = parser.current_span();
        let mut functions = Vec::new();
        let mut constants = Vec::new();

        parser.skip_newlines();
        while !parser.at_eof() {
            match parser.peek() {
                Token::Fn => {
                    match parser.parse_fn_decl() {
                        Ok(decl) => {
                            // Validate: no emit or trace in fn body
                            if let Some(err) = Self::check_module_fn_body(&decl) {
                                parser.errors.push(err);
                            } else {
                                functions.push(decl);
                            }
                        }
                        Err(e) => {
                            parser.errors.push(e);
                            parser.recover_to_newline();
                        }
                    }
                }
                Token::Let => match parser.parse_let() {
                    Ok(Statement::Let {
                        node_id,
                        name,
                        expr,
                        span,
                    }) => {
                        constants.push(ModuleConst {
                            node_id,
                            name,
                            expr,
                            span,
                        });
                    }
                    Ok(_) => unreachable!(),
                    Err(e) => {
                        parser.errors.push(e);
                        parser.recover_to_newline();
                    }
                },
                Token::Emit => {
                    parser.errors.push(parser.error(
                        "emit is not allowed in module files",
                        "Module files can only contain fn declarations and let constants",
                        "Remove the emit statement — modules are pure",
                    ));
                    parser.recover_to_newline();
                }
                Token::Trace => {
                    parser.errors.push(parser.error(
                        "trace is not allowed in module files",
                        "Module files can only contain fn declarations and let constants",
                        "Remove the trace statement — modules are pure",
                    ));
                    parser.recover_to_newline();
                }
                Token::Use => {
                    parser.errors.push(parser.error(
                        "modules cannot import other modules",
                        "Cross-module imports are not supported in v1",
                        "Move shared logic to a separate module and import it from the transform",
                    ));
                    parser.recover_to_newline();
                }
                Token::Filter => {
                    parser.errors.push(parser.error(
                        "filter is not allowed in module files",
                        "Module files can only contain fn declarations and let constants",
                        "Remove the filter statement — modules are pure",
                    ));
                    parser.recover_to_newline();
                }
                Token::Distinct => {
                    parser.errors.push(parser.error(
                        "distinct is not allowed in module files",
                        "Module files can only contain fn declarations and let constants",
                        "Remove the distinct statement — modules are pure",
                    ));
                    parser.recover_to_newline();
                }
                _ => {
                    parser.errors.push(parser.error(
                        &format!("unexpected token {:?} in module file", parser.peek()),
                        "Module files can only contain fn declarations and let constants",
                        "Use 'fn name(params) = expr' or 'let NAME = value'",
                    ));
                    parser.recover_to_newline();
                }
            }
            parser.skip_newlines();
        }

        let end_span = parser.current_span();
        let node_count = parser.next_id;
        ModuleParseResult {
            module: Module {
                functions,
                constants,
                span: Span::new(start_span.start as usize, end_span.end as usize),
            },
            errors: parser.errors,
            node_count,
        }
    }

    /// Check that a module fn body does not contain emit or trace keywords.
    /// We walk the expression tree looking for patterns that indicate emit/trace usage.
    /// Since emit and trace are statements (not expressions), they can't appear in fn bodies
    /// through normal parsing. But we detect if someone tries to use them as identifiers.
    fn check_module_fn_body(_decl: &FnDecl) -> Option<ParseError> {
        // emit and trace are keywords — they can't appear in expression position.
        // The parser already rejects them as unexpected tokens in parse_expr().
        // No additional validation needed here since fn body is a single expression.
        None
    }

    /// Allocate the next monotonic NodeId.
    fn alloc_id(&mut self) -> NodeId {
        let id = NodeId(self.next_id);
        self.next_id += 1;
        id
    }

    // ── Statement parsing ──────────────────────────────────────────

    fn parse_statement(&mut self) -> Result<Statement, ParseError> {
        match self.peek() {
            Token::Let => self.parse_let(),
            Token::Emit => self.parse_emit(),
            Token::Trace => self.parse_trace(),
            Token::Use => self.parse_use(),
            Token::Fn => self.parse_fn_stmt(),
            Token::Filter => self.parse_filter(),
            Token::Distinct => self.parse_distinct(),
            _ => {
                let nid = self.alloc_id();
                let expr = self.parse_expr(0)?;
                let span = expr.span();
                Ok(Statement::ExprStmt {
                    node_id: nid,
                    expr,
                    span,
                })
            }
        }
    }

    fn parse_let(&mut self) -> Result<Statement, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'let'
        let name = self.expect_ident("variable name")?;
        self.expect_token(&Token::Eq, "'='")?;
        let expr = self.parse_expr(0)?;
        let end = expr.span();
        Ok(Statement::Let {
            node_id: nid,
            name: name.into(),
            expr,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    fn parse_emit(&mut self) -> Result<Statement, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'emit'

        // Detect `emit each <binding> in <source> { <body> }` — fan-out
        // form. Matches identifier "each" via lookahead because `each`
        // is not a reserved keyword (it is only meaningful after `emit`).
        if let Token::Ident(name) = self.peek()
            && name.as_ref() == "each"
        {
            return self.parse_emit_each(nid, start);
        }

        // Detect `emit $<ns>.field = expr`. Allowed namespaces:
        // - `meta`: per-record metadata sidecar (deleted in Stage 6).
        // - `pipeline` / `source` / `record`: producer-declared scoped
        //   state. The variable name must be declared in the
        //   transform's `config.declares:` block; bind_schema enforces.
        let (name, target) = if *self.peek() == Token::Dollar {
            self.advance(); // consume '$'
            let ns = self.expect_ident("system namespace after '$'")?;
            if ns == "ck" {
                return Err(self.error(
                    "the `$ck` namespace is reserved for engine-stamped \
                     correlation snapshots and cannot be assigned by `emit`",
                    "$ck.* is read-only — the value is captured at Source ingest",
                    "To override the user-visible value, write `emit <field_name> = ...` instead",
                ));
            }
            if ns == "vars" {
                return Err(self.error(
                    "the `$vars` namespace is read-only static configuration \
                     and cannot be assigned by `emit`",
                    "$vars.* values are declared at the pipeline's top-level vars: block, \
                     channel-overridable, and frozen at pipeline start",
                    "Move computed values to `$pipeline.<key>` (declared via the producer's \
                     `config.declares:` block) — those are writable.",
                ));
            }
            let target = match ns.as_str() {
                "pipeline" => EmitTarget::Pipeline,
                "source" => EmitTarget::Source,
                "record" => EmitTarget::Record,
                _ => {
                    return Err(self.error(
                        &format!(
                            "emit ${}... is not valid; emit accepts $pipeline, $source, \
                             or $record namespaces",
                            ns
                        ),
                        "Only producer-declared scope namespaces are writable",
                        "Use one of: emit name = expr, emit $pipeline.x = expr, \
                         emit $source.x = expr, emit $record.x = expr",
                    ));
                }
            };
            self.expect_token(&Token::Dot, "'.'")?;
            let field = self.expect_ident("emit target name")?;
            (field, target)
        } else {
            let field = self.expect_ident("output field name")?;
            (field, EmitTarget::Field)
        };

        self.expect_token(&Token::Eq, "'='")?;
        let expr = self.parse_expr(0)?;
        let end = expr.span();
        Ok(Statement::Emit {
            node_id: nid,
            name: name.into(),
            expr,
            target,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    /// Parse `emit each <binding> in <source> { <body> }`, or its
    /// outer-join variant `emit each <binding> in <source> outer
    /// { <body> }`. Called from [`Self::parse_emit`] after the `each`
    /// keyword has been peeked (but not consumed). `nid` and `start` are
    /// taken from the parent's allocator so the resulting statement
    /// carries the same NodeId/Span origin as a plain `emit`. A trailing
    /// `outer` modifier yields [`Statement::ExplodeOuter`]; its absence
    /// yields [`Statement::EmitEach`]. Fan-out may nest — an `emit each`
    /// body may itself contain `emit each` blocks — bounded by
    /// [`MAX_EMIT_EACH_DEPTH`] so adversarial input cannot overflow the
    /// statement-recursion stack.
    fn parse_emit_each(&mut self, nid: NodeId, start: Span) -> Result<Statement, ParseError> {
        if self.emit_each_depth >= MAX_EMIT_EACH_DEPTH {
            return Err(self.error(
                "emit each nesting too deep (max 32 levels)",
                "Deeply nested fan-out is bounded to keep parser recursion stack-safe; legitimate document fan-out is only a few levels deep",
                "Flatten part of the nesting (e.g. precompute a flattened array with .flat_map) and fan out over fewer levels",
            ));
        }

        self.advance(); // consume 'each' ident
        // Accept either a plain identifier or the `it` keyword token as
        // the binding name — the closure surface uses `it` for the
        // closure parameter and the emit_each binding convention is the
        // same.
        let binding = match self.peek().clone() {
            Token::Ident(name) => {
                self.advance();
                name.to_string()
            }
            Token::It => {
                self.advance();
                "it".to_string()
            }
            other => {
                return Err(self.error(
                    &format!(
                        "expected emit_each binding name (identifier), got {:?}",
                        other
                    ),
                    "emit_each binds an iterator variable; `it` is the conventional name",
                    "Use an identifier (or `it`) as the binding name",
                ));
            }
        };
        // The `in` separator is an identifier (CXL has no `in` keyword);
        // accept the literal text "in" to keep the grammar surface from
        // adding another reserved word.
        match self.peek().clone() {
            Token::Ident(name) if name.as_ref() == "in" => {
                self.advance();
            }
            other => {
                return Err(self.error(
                    &format!("expected 'in' between binding and source, got {:?}", other),
                    "emit_each grammar is `emit each <binding> in <source> { <body> }`",
                    "Insert 'in' between the binding identifier and the source expression",
                ));
            }
        }
        let source = self.parse_expr(0)?;

        // Detect the trailing `outer` modifier — the outer-join variant
        // that preserves the trigger row when the source is null/empty.
        // `outer` is a bare identifier (not a reserved keyword), so it is
        // only meaningful in this position; `parse_expr` above stops at it
        // because an identifier carries no infix binding power, leaving it
        // as the current token between the source and the `{`.
        let is_outer = matches!(self.peek(), Token::Ident(name) if name.as_ref() == "outer");
        if is_outer {
            self.advance(); // consume 'outer' ident
        }

        self.expect_token(&Token::LBrace, "'{'")?;

        // Enter one nesting level. A nested `emit each` inside the body
        // re-enters here and increments again; on the error path the
        // depth is restored so a recoverable parse does not leak a
        // permanently-elevated depth into sibling statements.
        self.emit_each_depth += 1;
        let mut body = Vec::new();
        self.skip_newlines();
        while *self.peek() != Token::RBrace && !self.at_eof() {
            match self.parse_statement() {
                Ok(stmt) => body.push(stmt),
                Err(e) => {
                    self.emit_each_depth -= 1;
                    return Err(e);
                }
            }
            self.skip_newlines();
        }
        self.emit_each_depth -= 1;
        let end_span = self.current_span();
        self.expect_token(&Token::RBrace, "'}'")?;

        let span = Span::new(start.start as usize, end_span.end as usize);
        if is_outer {
            Ok(Statement::ExplodeOuter {
                node_id: nid,
                binding: binding.into(),
                source,
                body,
                span,
            })
        } else {
            Ok(Statement::EmitEach {
                node_id: nid,
                binding: binding.into(),
                source,
                body,
                span,
            })
        }
    }

    fn parse_filter(&mut self) -> Result<Statement, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'filter'

        // Must have a predicate expression
        if *self.peek() == Token::Newline || *self.peek() == Token::Eof {
            return Err(self.error(
                "filter requires a predicate expression",
                "Expected a boolean expression after 'filter'",
                "Example: filter status == \"active\"",
            ));
        }

        let predicate = self.parse_expr(0)?;
        let end = predicate.span();
        Ok(Statement::Filter {
            node_id: nid,
            predicate,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    fn parse_distinct(&mut self) -> Result<Statement, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'distinct'

        // Check for optional 'by <field>'
        if *self.peek() == Token::By {
            self.advance(); // consume 'by'

            // Must have a field name after 'by'
            if *self.peek() == Token::Newline || *self.peek() == Token::Eof {
                return Err(self.error(
                    "distinct by requires a field name",
                    "Expected an identifier after 'by'",
                    "Example: distinct by id",
                ));
            }

            let field = self.expect_ident("field name")?;
            let end = self.prev_span();
            Ok(Statement::Distinct {
                node_id: nid,
                field: Some(field.into()),
                span: Span::new(start.start as usize, end.end as usize),
            })
        } else if let Token::Ident(_) = self.peek() {
            // "distinct id" without 'by' → error
            Err(self.error(
                "distinct requires 'by' before the field name",
                "Expected 'by' keyword between 'distinct' and the field name",
                "Example: distinct by id",
            ))
        } else {
            // Bare distinct — all fields
            Ok(Statement::Distinct {
                node_id: nid,
                field: None,
                span: Span::new(start.start as usize, start.end as usize),
            })
        }
    }

    fn parse_trace(&mut self) -> Result<Statement, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'trace'

        // Check for contextual level: info/warn/error/debug
        let level = if let Token::Ident(ref name) = self.peek().clone() {
            match name.as_ref() {
                "info" => {
                    self.advance();
                    Some(TraceLevel::Info)
                }
                "warn" => {
                    self.advance();
                    Some(TraceLevel::Warn)
                }
                "error" => {
                    self.advance();
                    Some(TraceLevel::Error)
                }
                "debug" => {
                    self.advance();
                    Some(TraceLevel::Debug)
                }
                _ => None,
            }
        } else {
            None
        };

        // Check for guard: if expr
        let guard = if *self.peek() == Token::If {
            self.advance(); // consume 'if'
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        let message = self.parse_expr(0)?;
        let end = message.span();
        Ok(Statement::Trace {
            node_id: nid,
            level,
            guard,
            message,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    fn parse_use(&mut self) -> Result<Statement, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'use'

        let mut path = vec![self.expect_ident("module path")?.into_boxed_str()];

        // Reject :: separator with helpful migration message
        if *self.peek() == Token::ColonColon {
            return Err(self.error(
                "use paths use '.' separator, not '::'",
                "CXL module paths use dot notation like 'use shared.dates'",
                "Replace '::' with '.' — e.g. 'use shared.dates'",
            ));
        }

        while *self.peek() == Token::Dot {
            self.advance(); // consume '.'
            // Reject wildcard imports: use mod.*
            if *self.peek() == Token::Star {
                return Err(self.error(
                    "wildcard imports not supported in v1",
                    "CXL does not support 'use module.*' — import the module and use qualified access",
                    "Remove the '.*' and use qualified access like 'module.function()'",
                ));
            }
            path.push(self.expect_ident("module path segment")?.into_boxed_str());
        }

        let alias = if *self.peek() == Token::As {
            self.advance(); // consume 'as'
            Some(self.expect_ident("alias name")?.into_boxed_str())
        } else {
            None
        };

        let end = self.prev_span();
        Ok(Statement::UseStmt {
            node_id: nid,
            path,
            alias,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    fn parse_fn_stmt(&mut self) -> Result<Statement, ParseError> {
        let nid = self.alloc_id();
        let decl = self.parse_fn_decl()?;
        let span = decl.span;
        // Wrap FnDecl as an ExprStmt for now — Phase 3 resolver handles fn registration
        Ok(Statement::ExprStmt {
            node_id: nid,
            expr: Expr::Literal {
                node_id: self.alloc_id(),
                value: LiteralValue::Null,
                span,
            },
            span,
        })
    }

    pub fn parse_fn_decl(&mut self) -> Result<FnDecl, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'fn'
        let name = self.expect_ident("function name")?;
        self.expect_token(&Token::LParen, "'('")?;

        let mut params = Vec::new();
        if *self.peek() != Token::RParen {
            params.push(self.expect_ident("parameter name")?.into_boxed_str());
            while *self.peek() == Token::Comma {
                self.advance();
                params.push(self.expect_ident("parameter name")?.into_boxed_str());
            }
        }
        self.expect_token(&Token::RParen, "')'")?;
        self.expect_token(&Token::Eq, "'='")?;
        let body = Box::new(self.parse_expr(0)?);
        let end = body.span();

        Ok(FnDecl {
            node_id: nid,
            name: name.into(),
            params,
            body,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    // ── Pratt expression parser ────────────────────────────────────

    fn parse_expr(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            self.depth -= 1;
            return Err(self.error(
                "expression nesting too deep (max 256 levels)",
                "The parser has a maximum nesting depth to prevent stack overflow",
                "Simplify the expression or break it into let-bindings",
            ));
        }

        let mut lhs = self.parse_nud()?;

        loop {
            // Check for newline/eof — stop the expression
            if self.at_eof() || *self.peek() == Token::Newline {
                break;
            }

            // Stop tokens that end an expression context
            if matches!(
                self.peek(),
                Token::RParen
                    | Token::RBrace
                    | Token::RBracket
                    | Token::Comma
                    | Token::FatArrow
                    | Token::Then
                    | Token::Else
            ) {
                break;
            }

            let tok = self.peek().clone();

            if let Some((l_bp, r_bp)) = infix_bp(&tok) {
                if l_bp < min_bp {
                    break;
                }

                // Non-associative comparison check
                if is_comparison(&tok)
                    && let Expr::Binary { op, .. } = &lhs
                    && matches!(
                        op,
                        BinOp::Eq | BinOp::Neq | BinOp::Gt | BinOp::Lt | BinOp::Gte | BinOp::Lte
                    )
                {
                    self.depth -= 1;
                    return Err(self.error(
                        "comparisons are not chainable",
                        "CXL comparisons are non-associative — a == b == c is ambiguous",
                        "use (a == b) and (b == c) instead",
                    ));
                }

                // LBracket: postfix bracket-index access for arrays and
                // maps. Mirrors Dot's precedence so `arr[0].name` and
                // `record.field[0]` parse without parens. The receiver
                // is the existing `lhs`; the index expression is parsed
                // at BP 0 (full expression), then `]` closes it.
                if tok == Token::LBracket {
                    self.advance(); // consume '['
                    let lhs_span = lhs.span();
                    let index = self.parse_expr(0)?;
                    self.expect_token(&Token::RBracket, "']'")?;
                    let end = self.prev_span();
                    let nid = self.alloc_id();
                    lhs = Expr::IndexAccess {
                        node_id: nid,
                        receiver: Box::new(lhs),
                        index: Box::new(index),
                        span: Span::new(lhs_span.start as usize, end.end as usize),
                    };
                    continue;
                }

                // Dot: postfix field access or method call
                if tok == Token::Dot {
                    self.advance(); // consume '.'
                    // Accept identifiers AND keyword tokens that double
                    // as method names (`filter`, `distinct`, `match`,
                    // `not`, `it`, etc.) — the lexer eagerly produces
                    // keyword tokens for those reserved words, but in
                    // method-call position any of them should bind as
                    // the method name string.
                    let method_name = self.expect_ident_or_keyword("field or method name")?;
                    let lhs_span = lhs.span();

                    if *self.peek() == Token::LParen {
                        // Method call
                        let nid = self.alloc_id();
                        self.advance(); // consume '('
                        let args = self.parse_arg_list()?;
                        let end = self.prev_span();
                        lhs = Expr::MethodCall {
                            node_id: nid,
                            receiver: Box::new(lhs),
                            method: method_name.into(),
                            args,
                            span: Span::new(lhs_span.start as usize, end.end as usize),
                        };
                    } else {
                        // Field access → QualifiedFieldRef (supports chained dots)
                        let end = self.prev_span();
                        let new_span = Span::new(lhs_span.start as usize, end.end as usize);
                        match lhs {
                            Expr::FieldRef { name, .. } => {
                                let nid = self.alloc_id();
                                lhs = Expr::QualifiedFieldRef {
                                    node_id: nid,
                                    parts: vec![name, method_name.into()].into_boxed_slice(),
                                    span: new_span,
                                };
                            }
                            Expr::QualifiedFieldRef { parts, .. } => {
                                let nid = self.alloc_id();
                                let mut new_parts = parts.into_vec();
                                new_parts.push(method_name.into());
                                lhs = Expr::QualifiedFieldRef {
                                    node_id: nid,
                                    parts: new_parts.into_boxed_slice(),
                                    span: new_span,
                                };
                            }
                            _ => {
                                // For chained access like window.lag(1).field, keep as method
                                let nid = self.alloc_id();
                                lhs = Expr::MethodCall {
                                    node_id: nid,
                                    receiver: Box::new(lhs),
                                    method: method_name.into(),
                                    args: vec![],
                                    span: new_span,
                                };
                                continue;
                            }
                        }
                    }
                    continue;
                }

                // Coalesce
                if tok == Token::QuestionQuestion {
                    let nid = self.alloc_id();
                    self.advance();
                    let rhs = self.parse_expr(r_bp)?;
                    let start = lhs.span();
                    let end = rhs.span();
                    lhs = Expr::Coalesce {
                        node_id: nid,
                        lhs: Box::new(lhs),
                        rhs: Box::new(rhs),
                        span: Span::new(start.start as usize, end.end as usize),
                    };
                    continue;
                }

                // Standard binary operator
                if let Some(op) = token_to_binop(&tok) {
                    let nid = self.alloc_id();
                    self.advance();
                    let rhs = self.parse_expr(r_bp)?;
                    let start = lhs.span();
                    let end = rhs.span();
                    lhs = Expr::Binary {
                        node_id: nid,
                        op,
                        lhs: Box::new(lhs),
                        rhs: Box::new(rhs),
                        span: Span::new(start.start as usize, end.end as usize),
                    };
                    continue;
                }
            }

            break;
        }

        self.depth -= 1;
        Ok(lhs)
    }

    /// Null-denotation: prefix operators and atoms.
    fn parse_nud(&mut self) -> Result<Expr, ParseError> {
        let tok = self.peek().clone();
        let start = self.current_span();

        match tok {
            // Prefix not
            Token::Not => {
                let nid = self.alloc_id();
                self.advance();
                let bp = prefix_bp(&Token::Not).unwrap();
                let operand = self.parse_expr(bp)?;
                let end = operand.span();
                Ok(Expr::Unary {
                    node_id: nid,
                    op: UnaryOp::Not,
                    operand: Box::new(operand),
                    span: Span::new(start.start as usize, end.end as usize),
                })
            }

            // Prefix unary minus
            Token::Minus => {
                let nid = self.alloc_id();
                self.advance();
                let bp = prefix_bp(&Token::Minus).unwrap();
                let operand = self.parse_expr(bp)?;
                let end = operand.span();
                Ok(Expr::Unary {
                    node_id: nid,
                    op: UnaryOp::Neg,
                    operand: Box::new(operand),
                    span: Span::new(start.start as usize, end.end as usize),
                })
            }

            // Parenthesized expression
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr(0)?;
                self.expect_token(&Token::RParen, "')'")?;
                Ok(expr)
            }

            // if/then/else
            Token::If => self.parse_if_expr(),

            // match
            Token::Match => self.parse_match_expr(),

            // Literals
            Token::IntLit(v) => {
                let nid = self.alloc_id();
                self.advance();
                Ok(Expr::Literal {
                    node_id: nid,
                    value: LiteralValue::Int(v),
                    span: start,
                })
            }
            Token::FloatLit(v) => {
                let nid = self.alloc_id();
                self.advance();
                Ok(Expr::Literal {
                    node_id: nid,
                    value: LiteralValue::Float(v),
                    span: start,
                })
            }
            Token::StringLit(ref s) => {
                let nid = self.alloc_id();
                let s = s.clone();
                self.advance();
                Ok(Expr::Literal {
                    node_id: nid,
                    value: LiteralValue::String(s),
                    span: start,
                })
            }
            Token::DateLit(d) => {
                let nid = self.alloc_id();
                self.advance();
                Ok(Expr::Literal {
                    node_id: nid,
                    value: LiteralValue::Date(d),
                    span: start,
                })
            }
            Token::True => {
                let nid = self.alloc_id();
                self.advance();
                Ok(Expr::Literal {
                    node_id: nid,
                    value: LiteralValue::Bool(true),
                    span: start,
                })
            }
            Token::False => {
                let nid = self.alloc_id();
                self.advance();
                Ok(Expr::Literal {
                    node_id: nid,
                    value: LiteralValue::Bool(false),
                    span: start,
                })
            }
            Token::Null => {
                let nid = self.alloc_id();
                self.advance();
                Ok(Expr::Literal {
                    node_id: nid,
                    value: LiteralValue::Null,
                    span: start,
                })
            }
            Token::Now => {
                let nid = self.alloc_id();
                self.advance();
                Ok(Expr::Now {
                    node_id: nid,
                    span: start,
                })
            }

            // `it` keyword — resolver validates context (predicate_expr only)
            Token::It => {
                let nid = self.alloc_id();
                self.advance();
                Ok(Expr::FieldRef {
                    node_id: nid,
                    name: "it".into(),
                    span: start,
                })
            }

            // Underscore wildcard
            Token::Underscore => {
                let nid = self.alloc_id();
                self.advance();
                Ok(Expr::Wildcard {
                    node_id: nid,
                    span: start,
                })
            }

            // $pipeline.field, $vars.key, $source.field, $record.field, $window.fn(), $doc.section.field
            Token::Dollar => {
                self.advance(); // consume '$'
                let ns = self.expect_ident(
                    "system namespace (pipeline, vars, source, record, window, doc)",
                )?;
                self.expect_token(&Token::Dot, "'.'")?;

                match ns.as_str() {
                    "pipeline" => {
                        let nid = self.alloc_id();
                        let field = self.expect_ident("pipeline property name")?;
                        let end = self.prev_span();
                        Ok(Expr::PipelineAccess {
                            node_id: nid,
                            field: field.into(),
                            span: Span::new(start.start as usize, end.end as usize),
                        })
                    }
                    "vars" => {
                        let nid = self.alloc_id();
                        let key = self.expect_ident("vars key name")?;
                        let end = self.prev_span();
                        Ok(Expr::VarsAccess {
                            node_id: nid,
                            key: key.into(),
                            span: Span::new(start.start as usize, end.end as usize),
                        })
                    }
                    "source" => {
                        let first = self.expect_ident("source property name")?;
                        // peek for a qualified `.field` suffix so
                        // `$source.<input_name>.<field>` parses as a
                        // QualifiedSourceAccess. The plain `$source.<field>`
                        // form remains a SourceAccess.
                        if *self.peek() == Token::Dot {
                            self.advance();
                            let nid = self.alloc_id();
                            let field = self.expect_ident("source property name")?;
                            let end = self.prev_span();
                            Ok(Expr::QualifiedSourceAccess {
                                node_id: nid,
                                input_name: first.into(),
                                field: field.into(),
                                span: Span::new(start.start as usize, end.end as usize),
                            })
                        } else {
                            let nid = self.alloc_id();
                            let end = self.prev_span();
                            Ok(Expr::SourceAccess {
                                node_id: nid,
                                field: first.into(),
                                span: Span::new(start.start as usize, end.end as usize),
                            })
                        }
                    }
                    "record" => {
                        let nid = self.alloc_id();
                        let field = self.expect_ident("record property name")?;
                        let end = self.prev_span();
                        Ok(Expr::RecordAccess {
                            node_id: nid,
                            field: field.into(),
                            span: Span::new(start.start as usize, end.end as usize),
                        })
                    }
                    "window" => {
                        let fn_name = self.expect_ident("window function name")?;
                        if *self.peek() == Token::LParen {
                            let nid = self.alloc_id();
                            self.advance();
                            let args = self.parse_arg_list()?;
                            let end = self.prev_span();
                            Ok(Expr::WindowCall {
                                node_id: nid,
                                function: fn_name.into(),
                                args,
                                span: Span::new(start.start as usize, end.end as usize),
                            })
                        } else {
                            let nid = self.alloc_id();
                            let end = self.prev_span();
                            Ok(Expr::WindowCall {
                                node_id: nid,
                                function: fn_name.into(),
                                args: vec![],
                                span: Span::new(start.start as usize, end.end as usize),
                            })
                        }
                    }
                    "doc" => {
                        let section = self.expect_ident("envelope section name")?;
                        self.expect_token(&Token::Dot, "'.'")?;
                        let field = self.expect_ident("envelope section field name")?;
                        let end = self.prev_span();
                        let nid = self.alloc_id();
                        Ok(Expr::DocAccess {
                            node_id: nid,
                            section: section.into(),
                            field: field.into(),
                            span: Span::new(start.start as usize, end.end as usize),
                        })
                    }
                    other => Err(self.error(
                        &format!("unknown system namespace '${other}'"),
                        "Valid system namespaces are: pipeline, source, record, window, doc",
                        "Use $pipeline.field, $source.field, $record.field, $window.fn(), or \
                         $doc.<section>.<field>",
                    )),
                }
            }

            // Identifiers (field references) — with aggregate function lookahead.
            // If the identifier matches a known aggregate name AND the next token
            // is `(`, parse as an `AggCall`. Otherwise parse as a `FieldRef` so that
            // columns named e.g. `sum` or `count` still work outside a call site.
            Token::Ident(ref name) => {
                let name_cloned = name.clone();
                if is_aggregate_name(&name_cloned) && matches!(self.peek_ahead(1), Token::LParen) {
                    self.parse_agg_call(name_cloned, start)
                } else {
                    let nid = self.alloc_id();
                    self.advance();
                    Ok(Expr::FieldRef {
                        node_id: nid,
                        name: name_cloned,
                        span: start,
                    })
                }
            }

            _ => Err(self.error(
                &format!("unexpected token {:?}", tok),
                "Expected an expression (literal, identifier, '(', 'if', 'match', etc.)",
                "Check for missing operands or mismatched delimiters",
            )),
        }
    }

    fn parse_if_expr(&mut self) -> Result<Expr, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'if'

        // Parse condition at 'or' BP floor (BP 5) so `if a or b then ...` captures fully
        let condition = self.parse_expr(5)?;
        self.expect_token(&Token::Then, "'then'")?;

        // Parse then-branch at coalesce BP floor (BP 1) so it extends far right
        let then_branch = self.parse_expr(1)?;

        self.skip_newlines();
        let else_branch = if *self.peek() == Token::Else {
            self.advance();
            Some(Box::new(self.parse_expr(1)?))
        } else {
            None
        };

        let end = if let Some(ref eb) = else_branch {
            eb.span()
        } else {
            then_branch.span()
        };

        Ok(Expr::IfThenElse {
            node_id: nid,
            condition: Box::new(condition),
            then_branch: Box::new(then_branch),
            else_branch,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    fn parse_match_expr(&mut self) -> Result<Expr, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'match'

        // Peek: if '{' then condition form, else value form
        let subject = if *self.peek() != Token::LBrace {
            Some(Box::new(self.parse_expr(0)?))
        } else {
            None
        };

        self.expect_token(&Token::LBrace, "'{'")?;
        let mut arms = Vec::new();

        while *self.peek() != Token::RBrace && !self.at_eof() {
            self.skip_newlines();
            if *self.peek() == Token::RBrace {
                break;
            }
            let arm_nid = self.alloc_id();
            let arm_start = self.current_span();
            let pattern = self.parse_expr(0)?;
            self.expect_token(&Token::FatArrow, "'=>'")?;
            let body = self.parse_expr(0)?;
            let arm_end = body.span();

            // Optional trailing comma
            if *self.peek() == Token::Comma {
                self.advance();
            }
            self.skip_newlines();

            arms.push(MatchArm {
                node_id: arm_nid,
                pattern,
                body,
                span: Span::new(arm_start.start as usize, arm_end.end as usize),
            });
        }

        self.expect_token(&Token::RBrace, "'}'")?;
        let end = self.prev_span();

        Ok(Expr::Match {
            node_id: nid,
            subject,
            arms,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    fn parse_arg_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = Vec::new();
        if *self.peek() != Token::RParen {
            args.push(self.parse_arg_or_closure()?);
            while *self.peek() == Token::Comma {
                self.advance();
                args.push(self.parse_arg_or_closure()?);
            }
        }
        self.expect_token(&Token::RParen, "')'")?;
        Ok(args)
    }

    /// Parse a single argument inside a method call's argument list.
    /// Detects the `it => body` closure shape via two-token lookahead;
    /// otherwise falls back to a regular expression. The closure body
    /// is a single expression at full BP — block-bodied closures are
    /// not part of the surface map.
    fn parse_arg_or_closure(&mut self) -> Result<Expr, ParseError> {
        if matches!(self.peek(), Token::It) && matches!(self.peek_ahead(1), Token::FatArrow) {
            let nid = self.alloc_id();
            let start = self.current_span();
            self.advance(); // consume `it`
            self.advance(); // consume `=>`
            let body = self.parse_expr(0)?;
            let end = body.span();
            Ok(Expr::Closure {
                node_id: nid,
                param: "it".into(),
                body: Box::new(body),
                span: Span::new(start.start as usize, end.end as usize),
            })
        } else {
            self.parse_expr(0)
        }
    }

    /// Parse a free-standing aggregate function call: `name(args...)`.
    /// The identifier token is still at `self.pos`.
    fn parse_agg_call(&mut self, name: Box<str>, start: Span) -> Result<Expr, ParseError> {
        let nid = self.alloc_id();
        self.advance(); // consume the identifier
        self.expect_token(&Token::LParen, "'('")?;
        let args = self.parse_agg_arg_list()?;
        let end = self.prev_span();

        // `count()` with no args is sugar for `count(*)` — emit a Wildcard arg.
        // `collect()` with no args is reserved for window aggregates and is
        // not yet implemented.
        let args = if args.is_empty() && &*name == "count" {
            let wnid = self.alloc_id();
            vec![Expr::Wildcard {
                node_id: wnid,
                span: start,
            }]
        } else {
            args
        };

        Ok(Expr::AggCall {
            node_id: nid,
            name,
            args,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    /// Parse an aggregate argument list. Unlike `parse_arg_list`, this intercepts
    /// `Token::Star` before entering `parse_expr(0)` so that `count(*)` parses
    /// as `[Wildcard]` rather than trying to consume `*` as multiplication.
    fn parse_agg_arg_list(&mut self) -> Result<Vec<Expr>, ParseError> {
        let mut args = Vec::new();
        if *self.peek() != Token::RParen {
            args.push(self.parse_agg_arg()?);
            while *self.peek() == Token::Comma {
                self.advance();
                args.push(self.parse_agg_arg()?);
            }
        }
        self.expect_token(&Token::RParen, "')'")?;
        Ok(args)
    }

    fn parse_agg_arg(&mut self) -> Result<Expr, ParseError> {
        if *self.peek() == Token::Star {
            let nid = self.alloc_id();
            let span = self.current_span();
            self.advance();
            Ok(Expr::Wildcard { node_id: nid, span })
        } else {
            self.parse_expr(0)
        }
    }

    // ── Token stream helpers ───────────────────────────────────────

    fn peek(&self) -> &Token {
        self.tokens
            .get(self.pos)
            .map(|(t, _)| t)
            .unwrap_or(&Token::Eof)
    }

    fn peek_ahead(&self, offset: usize) -> &Token {
        self.tokens
            .get(self.pos + offset)
            .map(|(t, _)| t)
            .unwrap_or(&Token::Eof)
    }

    fn current_span(&self) -> Span {
        self.tokens
            .get(self.pos)
            .map(|(_, s)| *s)
            .unwrap_or(Span::new(0, 0))
    }

    fn prev_span(&self) -> Span {
        if self.pos > 0 {
            self.tokens[self.pos - 1].1
        } else {
            Span::new(0, 0)
        }
    }

    fn advance(&mut self) -> (Token, Span) {
        let result = self
            .tokens
            .get(self.pos)
            .cloned()
            .unwrap_or((Token::Eof, Span::new(0, 0)));
        self.pos += 1;
        result
    }

    fn at_eof(&self) -> bool {
        matches!(self.peek(), Token::Eof)
    }

    fn skip_newlines(&mut self) {
        while matches!(self.peek(), Token::Newline | Token::Comment) {
            self.advance();
        }
    }

    fn expect_token(&mut self, expected: &Token, label: &str) -> Result<Span, ParseError> {
        if std::mem::discriminant(self.peek()) == std::mem::discriminant(expected)
            || *self.peek() == *expected
        {
            let (_, span) = self.advance();
            Ok(span)
        } else {
            Err(self.error(
                &format!("expected {}, got {:?}", label, self.peek()),
                &format!("The parser expected {} at this position", label),
                &format!("Add {} here", label),
            ))
        }
    }

    fn expect_ident(&mut self, context: &str) -> Result<String, ParseError> {
        if let Token::Ident(ref name) = self.peek().clone() {
            let name = name.to_string();
            self.advance();
            Ok(name)
        } else {
            Err(self.error(
                &format!("expected {} (identifier), got {:?}", context, self.peek()),
                &format!("A {} must be an identifier", context),
                &format!("Provide a valid identifier for the {}", context),
            ))
        }
    }

    /// Like [`Self::expect_ident`] but additionally accepts keyword
    /// tokens whose lexeme overlaps with a method name (`filter`,
    /// `distinct`, `match`, `it`, etc.). Used in method-call position
    /// so user-visible methods named after reserved words still parse.
    fn expect_ident_or_keyword(&mut self, context: &str) -> Result<String, ParseError> {
        let tok = self.peek().clone();
        match tok {
            Token::Ident(name) => {
                self.advance();
                Ok(name.to_string())
            }
            // Map reserved-word tokens back to their lexeme. The lexer's
            // keyword table is the source of truth; this list mirrors
            // it for tokens that double as method names in source.
            Token::Let => {
                self.advance();
                Ok("let".to_string())
            }
            Token::Emit => {
                self.advance();
                Ok("emit".to_string())
            }
            Token::If => {
                self.advance();
                Ok("if".to_string())
            }
            Token::Then => {
                self.advance();
                Ok("then".to_string())
            }
            Token::Else => {
                self.advance();
                Ok("else".to_string())
            }
            Token::And => {
                self.advance();
                Ok("and".to_string())
            }
            Token::Or => {
                self.advance();
                Ok("or".to_string())
            }
            Token::Not => {
                self.advance();
                Ok("not".to_string())
            }
            Token::Match => {
                self.advance();
                Ok("match".to_string())
            }
            Token::Use => {
                self.advance();
                Ok("use".to_string())
            }
            Token::As => {
                self.advance();
                Ok("as".to_string())
            }
            Token::Fn => {
                self.advance();
                Ok("fn".to_string())
            }
            Token::Trace => {
                self.advance();
                Ok("trace".to_string())
            }
            Token::Null => {
                self.advance();
                Ok("null".to_string())
            }
            Token::True => {
                self.advance();
                Ok("true".to_string())
            }
            Token::False => {
                self.advance();
                Ok("false".to_string())
            }
            Token::Now => {
                self.advance();
                Ok("now".to_string())
            }
            Token::It => {
                self.advance();
                Ok("it".to_string())
            }
            Token::Filter => {
                self.advance();
                Ok("filter".to_string())
            }
            Token::Distinct => {
                self.advance();
                Ok("distinct".to_string())
            }
            Token::By => {
                self.advance();
                Ok("by".to_string())
            }
            _ => self.expect_ident(context),
        }
    }

    fn recover_to_newline(&mut self) {
        while !self.at_eof() && *self.peek() != Token::Newline {
            self.advance();
        }
    }

    fn error(&self, message: &str, why: &str, how_to_fix: &str) -> ParseError {
        ParseError {
            span: self.current_span(),
            message: message.to_string(),
            expected: vec![],
            why: why.to_string(),
            how_to_fix: how_to_fix.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse_ok(src: &str) -> ParseResult {
        let result = Parser::parse(src);
        assert!(
            result.errors.is_empty(),
            "Expected no errors for '{}', got: {:?}",
            src,
            result.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        result
    }

    fn first_stmt(result: &ParseResult) -> &Statement {
        &result.ast.statements[0]
    }

    fn let_expr(result: &ParseResult) -> &Expr {
        match first_stmt(result) {
            Statement::Let { expr, .. } => expr,
            other => panic!("expected Let, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn test_parse_let_simple() {
        let r = parse_ok("let x = 42");
        match first_stmt(&r) {
            Statement::Let { name, expr, .. } => {
                assert_eq!(&**name, "x");
                assert!(matches!(
                    expr,
                    Expr::Literal {
                        value: LiteralValue::Int(42),
                        ..
                    }
                ));
            }
            _ => panic!("expected Let"),
        }
    }

    #[test]
    fn test_parse_emit_simple() {
        let r = parse_ok("emit out = name");
        match first_stmt(&r) {
            Statement::Emit { name, expr, .. } => {
                assert_eq!(&**name, "out");
                assert!(matches!(expr, Expr::FieldRef { .. }));
            }
            _ => panic!("expected Emit"),
        }
    }

    #[test]
    fn test_parse_emit_ck_namespace_rejected_with_specialized_message() {
        let result = Parser::parse("emit $ck.employee_id = 1");
        assert!(
            !result.errors.is_empty(),
            "expected parse error for `emit $ck.* = ...`",
        );
        let msg = &result.errors[0].message;
        assert!(
            msg.contains("$ck"),
            "diagnostic should name the $ck namespace; got: {msg}",
        );
        assert!(
            msg.contains("reserved") || msg.contains("read-only") || msg.contains("snapshot"),
            "diagnostic should explain why $ck.* is unwritable; got: {msg}",
        );
    }

    #[test]
    fn test_parse_emit_pipeline_namespace_succeeds() {
        // The variable-system redesign accepts producer-declared scope
        // writes via `emit $pipeline.x = ...` (and $source / $record).
        // Bind-schema rejects undeclared names; the parser accepts the
        // syntax.
        let result = Parser::parse("emit $pipeline.x = 1");
        assert!(
            result.errors.is_empty(),
            "emit $pipeline.x parses; bind-schema is the layer that \
             rejects undeclared scope vars. Got: {:?}",
            result.errors
        );
    }

    #[test]
    fn test_parse_emit_unknown_namespace_rejected() {
        let result = Parser::parse("emit $undeclared_namespace.x = 1");
        assert!(!result.errors.is_empty());
        let msg = &result.errors[0].message;
        assert!(
            msg.contains("$undeclared_namespace"),
            "unknown $namespace.* should mention the offending namespace; got: {msg}",
        );
    }

    #[test]
    fn test_parse_emit_vars_namespace_rejected_as_readonly() {
        let result = Parser::parse("emit $vars.x = 1");
        assert!(!result.errors.is_empty());
        let msg = &result.errors[0].message;
        assert!(
            msg.contains("$vars") && msg.contains("read-only"),
            "$vars.* writes should be rejected as read-only; got: {msg}",
        );
    }

    /// `$widened` is the engine-stamped sidecar absorber for the
    /// `on_unmapped: auto_widen` policy. CXL has no syntax for
    /// reading or writing it: the parser rejects `$widened.<key>`
    /// in expression position via the catch-all "unknown system
    /// namespace" path. The typechecker is blind to the sidecar's
    /// keys; users who need an absorbed input field at output
    /// time set `include_unmapped: true` on the Output node and the
    /// projection layer expands the map to top-level columns.
    #[test]
    fn test_parse_rejects_widened_in_read_position() {
        let result = Parser::parse("emit foo = $widened.bar");
        assert!(
            !result.errors.is_empty(),
            "$widened.* in expression position must be rejected"
        );
        let msg = &result.errors[0].message;
        assert!(
            msg.contains("unknown system namespace"),
            "diagnostic must explain it's an unknown namespace; got: {msg}"
        );
        assert!(
            msg.contains("$widened"),
            "diagnostic must name the offending namespace; got: {msg}"
        );
    }

    /// `emit $widened.<key> = ...` is rejected by the emit-target
    /// parser path. Different error surface than the read-side
    /// (the emit parser maintains its own list of writable
    /// namespaces — pipeline / source / record), but the rejection
    /// must be unambiguous.
    #[test]
    fn test_parse_rejects_widened_in_emit_target() {
        let result = Parser::parse("emit $widened.foo = 1");
        assert!(
            !result.errors.is_empty(),
            "emit $widened.<key> = ... must be rejected"
        );
        let msg = &result.errors[0].message;
        assert!(
            msg.contains("$widened") || msg.contains("widened"),
            "diagnostic must name the offending namespace; got: {msg}"
        );
    }

    #[test]
    fn test_parse_emit_source_namespace_succeeds() {
        let r = parse_ok("emit $source.batch = id");
        match first_stmt(&r) {
            Statement::Emit { name, target, .. } => {
                assert_eq!(&**name, "batch");
                assert_eq!(*target, EmitTarget::Source);
            }
            _ => panic!("expected Emit"),
        }
    }

    #[test]
    fn test_parse_emit_record_namespace_succeeds() {
        let r = parse_ok("emit $record.score = amount * 2");
        match first_stmt(&r) {
            Statement::Emit { name, target, .. } => {
                assert_eq!(&**name, "score");
                assert_eq!(*target, EmitTarget::Record);
            }
            _ => panic!("expected Emit"),
        }
    }

    #[test]
    fn test_parse_emit_pipeline_target_tag() {
        let r = parse_ok("emit $pipeline.last = amount");
        match first_stmt(&r) {
            Statement::Emit { name, target, .. } => {
                assert_eq!(&**name, "last");
                assert_eq!(*target, EmitTarget::Pipeline);
            }
            _ => panic!("expected Emit"),
        }
    }

    #[test]
    fn test_parse_emit_field_default_target_tag() {
        let r = parse_ok("emit total = a + b");
        match first_stmt(&r) {
            Statement::Emit { name, target, .. } => {
                assert_eq!(&**name, "total");
                assert_eq!(*target, EmitTarget::Field);
            }
            _ => panic!("expected Emit"),
        }
    }

    #[test]
    fn test_parse_arithmetic() {
        let r = parse_ok("let x = 1 + 2 * 3");
        let expr = let_expr(&r);
        // Should be Add(1, Mul(2, 3)) due to precedence
        match expr {
            Expr::Binary {
                op: BinOp::Add,
                rhs,
                ..
            } => {
                assert!(matches!(**rhs, Expr::Binary { op: BinOp::Mul, .. }));
            }
            _ => panic!("expected Binary Add at top"),
        }
    }

    #[test]
    fn test_parse_coalesce() {
        let r = parse_ok("let x = a ?? b");
        let expr = let_expr(&r);
        assert!(matches!(expr, Expr::Coalesce { .. }));
    }

    #[test]
    fn test_parse_method_call() {
        let r = parse_ok("let x = name.trim()");
        let expr = let_expr(&r);
        match expr {
            Expr::MethodCall { method, args, .. } => {
                assert_eq!(&**method, "trim");
                assert!(args.is_empty());
            }
            _ => panic!("expected MethodCall"),
        }
    }

    #[test]
    fn test_parse_method_call_with_args() {
        let r = parse_ok("let x = name.substring(0, 5)");
        let expr = let_expr(&r);
        match expr {
            Expr::MethodCall { method, args, .. } => {
                assert_eq!(&**method, "substring");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("expected MethodCall"),
        }
    }

    #[test]
    fn test_parse_chained_method() {
        let r = parse_ok("let x = name.trim().upper()");
        let expr = let_expr(&r);
        match expr {
            Expr::MethodCall {
                method, receiver, ..
            } => {
                assert_eq!(&**method, "upper");
                assert!(matches!(**receiver, Expr::MethodCall { .. }));
            }
            _ => panic!("expected chained MethodCall"),
        }
    }

    #[test]
    fn test_parse_qualified_field() {
        let r = parse_ok("let x = source.field");
        let expr = let_expr(&r);
        match expr {
            Expr::QualifiedFieldRef { parts, .. } => {
                assert_eq!(parts.len(), 2);
                assert_eq!(&*parts[0], "source");
                assert_eq!(&*parts[1], "field");
            }
            _ => panic!("expected QualifiedFieldRef"),
        }
    }

    #[test]
    fn test_parse_if_then_else() {
        let r = parse_ok("let x = if age > 18 then \"adult\" else \"minor\"");
        let expr = let_expr(&r);
        match expr {
            Expr::IfThenElse { else_branch, .. } => {
                assert!(else_branch.is_some());
            }
            _ => panic!("expected IfThenElse"),
        }
    }

    #[test]
    fn test_parse_if_then_no_else() {
        let r = parse_ok("let x = if cond then 1");
        let expr = let_expr(&r);
        match expr {
            Expr::IfThenElse { else_branch, .. } => {
                assert!(else_branch.is_none());
            }
            _ => panic!("expected IfThenElse"),
        }
    }

    #[test]
    fn test_parse_match_condition_form() {
        let r = parse_ok("let x = match { true => 1 }");
        let expr = let_expr(&r);
        match expr {
            Expr::Match { subject, arms, .. } => {
                assert!(subject.is_none());
                assert_eq!(arms.len(), 1);
            }
            _ => panic!("expected Match"),
        }
    }

    #[test]
    fn test_parse_match_value_form() {
        let r = parse_ok("let x = match status { \"A\" => 1, _ => 0 }");
        let expr = let_expr(&r);
        match expr {
            Expr::Match { subject, arms, .. } => {
                assert!(subject.is_some());
                assert_eq!(arms.len(), 2);
            }
            _ => panic!("expected Match"),
        }
    }

    #[test]
    fn test_parse_comparison_not_chainable() {
        let result = Parser::parse("let x = a == b == c");
        assert!(!result.errors.is_empty());
        assert!(result.errors[0].message.contains("not chainable"));
    }

    #[test]
    fn test_parse_unary_neg() {
        let r = parse_ok("let x = -42");
        let expr = let_expr(&r);
        assert!(matches!(
            expr,
            Expr::Unary {
                op: UnaryOp::Neg,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_unary_not() {
        let r = parse_ok("let x = not true");
        let expr = let_expr(&r);
        assert!(matches!(
            expr,
            Expr::Unary {
                op: UnaryOp::Not,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_not_binds_loose() {
        // not a == b parses as not(a == b) because not has BP 10
        // and == has BP 11 — not captures the entire comparison
        let r = parse_ok("let x = not a == b");
        let expr = let_expr(&r);
        match expr {
            Expr::Unary {
                op: UnaryOp::Not,
                operand,
                ..
            } => {
                assert!(matches!(**operand, Expr::Binary { op: BinOp::Eq, .. }));
            }
            other => panic!(
                "expected Unary Not wrapping Binary Eq, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }

    #[test]
    fn test_parse_null_literal() {
        let r = parse_ok("let x = null");
        let expr = let_expr(&r);
        assert!(matches!(
            expr,
            Expr::Literal {
                value: LiteralValue::Null,
                ..
            }
        ));
    }

    #[test]
    fn test_parse_bool_literals() {
        let r = parse_ok("let x = true");
        assert!(matches!(
            let_expr(&r),
            Expr::Literal {
                value: LiteralValue::Bool(true),
                ..
            }
        ));
        let r2 = parse_ok("let x = false");
        assert!(matches!(
            let_expr(&r2),
            Expr::Literal {
                value: LiteralValue::Bool(false),
                ..
            }
        ));
    }

    #[test]
    fn test_parse_string_literals() {
        let r = parse_ok("let x = \"hello world\"");
        match let_expr(&r) {
            Expr::Literal {
                value: LiteralValue::String(s),
                ..
            } => {
                assert_eq!(&**s, "hello world");
            }
            _ => panic!("expected String literal"),
        }
    }

    #[test]
    fn test_parse_window_call() {
        let r = parse_ok("let x = $window.sum(amount)");
        let expr = let_expr(&r);
        match expr {
            Expr::WindowCall { function, args, .. } => {
                assert_eq!(&**function, "sum");
                assert_eq!(args.len(), 1);
            }
            _ => panic!("expected WindowCall"),
        }
    }

    #[test]
    fn test_parse_pipeline_access() {
        let r = parse_ok("let x = $pipeline.start_time");
        let expr = let_expr(&r);
        match expr {
            Expr::PipelineAccess { field, .. } => {
                assert_eq!(&**field, "start_time");
            }
            _ => panic!("expected PipelineAccess"),
        }
    }

    #[test]
    fn test_parse_source_access() {
        let r = parse_ok("let x = $source.file");
        let expr = let_expr(&r);
        match expr {
            Expr::SourceAccess { field, .. } => {
                assert_eq!(&**field, "file");
            }
            _ => panic!("expected SourceAccess"),
        }

        let r = parse_ok("let x = $source.row");
        let expr = let_expr(&r);
        match expr {
            Expr::SourceAccess { field, .. } => {
                assert_eq!(&**field, "row");
            }
            _ => panic!("expected SourceAccess"),
        }
    }

    #[test]
    fn test_parse_doc_access_arbitrary_section_names() {
        // Section names are user-defined identifiers; no engine-reserved
        // labels. Pipelines pick whatever fits their format.
        for (input, expected_section, expected_field) in &[
            ("let x = $doc.Head.batch_id", "Head", "batch_id"),
            ("let x = $doc.Foot.record_count", "Foot", "record_count"),
            (
                "let x = $doc.batch_metadata.run_date",
                "batch_metadata",
                "run_date",
            ),
            ("let x = $doc.eob_summary.hash", "eob_summary", "hash"),
        ] {
            let r = parse_ok(input);
            let expr = let_expr(&r);
            match expr {
                Expr::DocAccess { section, field, .. } => {
                    assert_eq!(&**section, *expected_section, "input {input}");
                    assert_eq!(&**field, *expected_field, "input {input}");
                }
                other => panic!("expected DocAccess for {input}, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_parse_doc_access_requires_section_and_field() {
        // `$doc.foo` alone is a parse error — DocAccess is always
        // two-level (`$doc.<section>.<field>`).
        let result = Parser::parse("let x = $doc.foo");
        assert!(
            !result.errors.is_empty(),
            "expected parse error for `$doc.<section>` without `.<field>`"
        );
    }

    #[test]
    fn test_parse_qualified_source_access() {
        let r = parse_ok("let x = $source.salesforce.batch_id");
        let expr = let_expr(&r);
        match expr {
            Expr::QualifiedSourceAccess {
                input_name, field, ..
            } => {
                assert_eq!(&**input_name, "salesforce");
                assert_eq!(&**field, "batch_id");
            }
            other => panic!("expected QualifiedSourceAccess, got {other:?}"),
        }
    }

    #[test]
    fn test_parse_trace_simple() {
        let r = parse_ok("trace \"hello\"");
        match first_stmt(&r) {
            Statement::Trace { level, guard, .. } => {
                assert!(level.is_none());
                assert!(guard.is_none());
            }
            _ => panic!("expected Trace"),
        }
    }

    #[test]
    fn test_parse_trace_with_level_and_guard() {
        let r = parse_ok("trace warn if active \"alert\"");
        match first_stmt(&r) {
            Statement::Trace { level, guard, .. } => {
                assert_eq!(*level, Some(TraceLevel::Warn));
                assert!(guard.is_some());
            }
            _ => panic!("expected Trace"),
        }
    }

    #[test]
    fn test_parse_use_simple() {
        let r = parse_ok("use shared.dates");
        match first_stmt(&r) {
            Statement::UseStmt { path, alias, .. } => {
                assert_eq!(path.len(), 2);
                assert_eq!(&*path[0], "shared");
                assert_eq!(&*path[1], "dates");
                assert!(alias.is_none());
            }
            _ => panic!("expected UseStmt"),
        }
    }

    #[test]
    fn test_parse_use_with_alias() {
        let r = parse_ok("use shared.dates as d");
        match first_stmt(&r) {
            Statement::UseStmt { alias, .. } => {
                assert_eq!(alias.as_deref(), Some("d"));
            }
            _ => panic!("expected UseStmt"),
        }
    }

    #[test]
    fn test_parse_fn_decl() {
        let r = parse_ok("fn double(x) = x * 2");
        // fn is wrapped as ExprStmt (placeholder)
        assert!(!r.ast.statements.is_empty());
    }

    #[test]
    fn test_parse_multiline_program() {
        let src = "let x = 1\nemit out = x + 2";
        let r = parse_ok(src);
        assert_eq!(r.ast.statements.len(), 2);
    }

    #[test]
    fn test_parse_parenthesized() {
        let r = parse_ok("let x = (1 + 2) * 3");
        let expr = let_expr(&r);
        match expr {
            Expr::Binary {
                op: BinOp::Mul,
                lhs,
                ..
            } => {
                assert!(matches!(**lhs, Expr::Binary { op: BinOp::Add, .. }));
            }
            _ => panic!("expected Mul(Add(...), 3)"),
        }
    }

    #[test]
    fn test_parse_logical_operators() {
        let r = parse_ok("let x = a and b or c");
        let expr = let_expr(&r);
        // or binds looser than and: (a and b) or c
        assert!(matches!(expr, Expr::Binary { op: BinOp::Or, .. }));
    }

    #[test]
    fn test_parse_now_keyword() {
        let r = parse_ok("let x = now");
        let expr = let_expr(&r);
        assert!(matches!(expr, Expr::Now { .. }));
    }

    #[test]
    fn test_parse_now_with_method() {
        let r = parse_ok("let x = now.year()");
        let expr = let_expr(&r);
        match expr {
            Expr::MethodCall { method, .. } => {
                assert_eq!(&**method, "year");
            }
            _ => panic!("expected MethodCall on now"),
        }
    }

    #[test]
    fn test_parse_wildcard() {
        let r = parse_ok("let x = match status { _ => 0 }");
        let expr = let_expr(&r);
        if let Expr::Match { arms, .. } = expr {
            assert!(matches!(arms[0].pattern, Expr::Wildcard { .. }));
        } else {
            panic!("expected Match");
        }
    }

    #[test]
    fn test_parse_error_recovery() {
        let result = Parser::parse("let x = \nlet y = 2");
        // First statement errors, second should still parse
        assert!(!result.errors.is_empty());
        // We should have at least the second statement
        assert!(!result.ast.statements.is_empty());
    }

    #[test]
    fn test_parse_node_ids_monotonic() {
        let r = parse_ok("let x = 1 + 2");
        assert!(r.node_count > 0);
        // The let statement, literal 1, literal 2, and binary add all get IDs
        // Plus the let statement itself
        assert!(
            r.node_count >= 4,
            "Expected at least 4 nodes, got {}",
            r.node_count
        );
    }

    #[test]
    fn test_parse_node_ids_unique() {
        let r = parse_ok("let x = 1\nlet y = 2");
        // All NodeIds should be unique (monotonically increasing)
        // We can verify by checking node_count covers all allocated IDs
        assert!(r.node_count >= 4); // 2 let stmts + 2 literals minimum
    }

    // ── Module parsing tests ──────────────────────────────────────

    fn parse_module_ok(src: &str) -> ModuleParseResult {
        let result = Parser::parse_module(src);
        assert!(
            result.errors.is_empty(),
            "Expected no errors for '{}', got: {:?}",
            src,
            result.errors.iter().map(|e| &e.message).collect::<Vec<_>>()
        );
        result
    }

    #[test]
    fn test_module_parse_fn_declaration() {
        let r = parse_module_ok("fn is_valid(x) = x > 0");
        assert_eq!(r.module.functions.len(), 1);
        let f = &r.module.functions[0];
        assert_eq!(&*f.name, "is_valid");
        assert_eq!(f.params.len(), 1);
        assert_eq!(&*f.params[0], "x");
        assert!(matches!(*f.body, Expr::Binary { op: BinOp::Gt, .. }));
    }

    #[test]
    fn test_module_parse_let_constant() {
        let r = parse_module_ok("let MAX = 100");
        assert_eq!(r.module.constants.len(), 1);
        assert_eq!(&*r.module.constants[0].name, "MAX");
        assert!(matches!(
            r.module.constants[0].expr,
            Expr::Literal {
                value: LiteralValue::Int(100),
                ..
            }
        ));
    }

    #[test]
    fn test_module_reject_emit_in_fn() {
        // emit is a keyword, so "fn bad() = emit" will fail at expression parse level
        let result = Parser::parse_module("fn bad() = emit");
        assert!(!result.errors.is_empty());
    }

    #[test]
    fn test_module_reject_trace_in_fn() {
        // trace is a keyword/statement, can't appear in expression position
        let result = Parser::parse_module("fn bad() = trace");
        assert!(!result.errors.is_empty());
    }

    #[test]
    fn test_module_reject_cross_import() {
        let result = Parser::parse_module("use other");
        assert!(!result.errors.is_empty());
        assert!(
            result.errors[0]
                .message
                .contains("modules cannot import other modules")
        );
    }

    #[test]
    fn test_module_empty_file() {
        let r = parse_module_ok("");
        assert!(r.module.functions.is_empty());
        assert!(r.module.constants.is_empty());
    }

    #[test]
    fn test_module_constants_only() {
        let r = parse_module_ok("let A = 1\nlet B = 2");
        assert!(r.module.functions.is_empty());
        assert_eq!(r.module.constants.len(), 2);
        assert_eq!(&*r.module.constants[0].name, "A");
        assert_eq!(&*r.module.constants[1].name, "B");
    }

    #[test]
    fn test_module_functions_only() {
        let r = parse_module_ok("fn add(a, b) = a + b");
        assert!(r.module.constants.is_empty());
        assert_eq!(r.module.functions.len(), 1);
    }

    #[test]
    fn test_module_fn_no_params() {
        let r = parse_module_ok("fn pi() = 3.14159");
        assert_eq!(r.module.functions[0].params.len(), 0);
        assert!(matches!(
            *r.module.functions[0].body,
            Expr::Literal {
                value: LiteralValue::Float(_),
                ..
            }
        ));
    }

    #[test]
    fn test_module_fn_method_chain_body() {
        let r = parse_module_ok("fn clean(val) = val.trim().upper()");
        assert_eq!(r.module.functions.len(), 1);
        assert!(matches!(
            *r.module.functions[0].body,
            Expr::MethodCall { .. }
        ));
    }

    #[test]
    fn test_module_fn_conditional_body() {
        let r = parse_module_ok(
            "fn clamp(val, lo, hi) = if val < lo then lo else if val > hi then hi else val",
        );
        assert_eq!(r.module.functions.len(), 1);
        assert!(matches!(
            *r.module.functions[0].body,
            Expr::IfThenElse { .. }
        ));
    }

    #[test]
    fn test_module_fn_match_body() {
        let r = parse_module_ok(
            "fn tier(score) = match { score >= 90 => \"A\", score >= 80 => \"B\", _ => \"C\" }",
        );
        assert_eq!(r.module.functions.len(), 1);
        assert!(matches!(*r.module.functions[0].body, Expr::Match { .. }));
    }

    #[test]
    fn test_module_with_comments() {
        let r = parse_module_ok(
            "# Utility functions\nfn add(a, b) = a + b\n# Constants\nlet MAX = 100",
        );
        assert_eq!(r.module.functions.len(), 1);
        assert_eq!(r.module.constants.len(), 1);
    }

    #[test]
    fn test_module_reject_wildcard_import() {
        let result = Parser::parse("use validators.*");
        assert!(!result.errors.is_empty());
        assert!(
            result.errors[0]
                .message
                .contains("wildcard imports not supported")
        );
    }

    #[test]
    fn test_module_use_colons_rejected() {
        let result = Parser::parse("use validators::helpers");
        assert!(!result.errors.is_empty());
        assert!(
            result.errors[0]
                .message
                .contains("use paths use '.' separator, not '::'")
        );
    }

    #[test]
    fn test_module_use_path_resolution() {
        // Parse a use statement with dot separator
        let r = parse_ok("use validators");
        match first_stmt(&r) {
            Statement::UseStmt { path, .. } => {
                assert_eq!(path.len(), 1);
                assert_eq!(&*path[0], "validators");
            }
            _ => panic!("expected UseStmt"),
        }
    }

    #[test]
    fn test_module_use_nested_path() {
        let r = parse_ok("use reporting.fiscal");
        match first_stmt(&r) {
            Statement::UseStmt { path, .. } => {
                assert_eq!(path.len(), 2);
                assert_eq!(&*path[0], "reporting");
                assert_eq!(&*path[1], "fiscal");
            }
            _ => panic!("expected UseStmt"),
        }
    }

    #[test]
    fn test_module_deep_nested_path() {
        let r = parse_ok("use shared.utils.string_helpers");
        match first_stmt(&r) {
            Statement::UseStmt { path, .. } => {
                assert_eq!(path.len(), 3);
                assert_eq!(&*path[0], "shared");
                assert_eq!(&*path[1], "utils");
                assert_eq!(&*path[2], "string_helpers");
            }
            _ => panic!("expected UseStmt"),
        }
    }

    #[test]
    fn test_module_use_alias() {
        let r = parse_ok("use shared.date_helpers as dates");
        match first_stmt(&r) {
            Statement::UseStmt { path, alias, .. } => {
                assert_eq!(path.len(), 2);
                assert_eq!(&*path[0], "shared");
                assert_eq!(&*path[1], "date_helpers");
                assert_eq!(alias.as_deref(), Some("dates"));
            }
            _ => panic!("expected UseStmt"),
        }
    }

    #[test]
    fn test_module_const_duplicate_name() {
        // Parsing succeeds — duplicate detection happens in module_eval toposort
        let r = parse_module_ok("let X = 1\nlet X = 2");
        assert_eq!(r.module.constants.len(), 2);
    }

    #[test]
    fn test_module_fn_duplicate_name() {
        // Parsing succeeds — duplicate detection happens in module registry
        let r = parse_module_ok("fn f(x) = x\nfn f(y) = y + 1");
        assert_eq!(r.module.functions.len(), 2);
    }

    #[test]
    fn test_module_use_after_let_error() {
        let result = Parser::parse("let x = 1\nuse validators");
        assert!(!result.errors.is_empty());
        assert!(
            result.errors[0]
                .message
                .contains("use must appear before let/emit/trace")
        );
    }

    // ── aggregate function parsing ─────────────────────────────────────

    fn emit_expr(r: &ParseResult) -> &Expr {
        match &r.ast.statements[0] {
            Statement::Emit { expr, .. } => expr,
            _ => panic!("expected emit"),
        }
    }

    #[test]
    fn test_parse_sum_function() {
        let r = parse_ok("emit t = sum(salary)");
        match emit_expr(&r) {
            Expr::AggCall { name, args, .. } => {
                assert_eq!(&**name, "sum");
                assert_eq!(args.len(), 1);
                assert!(matches!(&args[0], Expr::FieldRef { name, .. } if &**name == "salary"));
            }
            other => panic!("expected AggCall, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn test_parse_count_star() {
        let r = parse_ok("emit c = count(*)");
        match emit_expr(&r) {
            Expr::AggCall { name, args, .. } => {
                assert_eq!(&**name, "count");
                assert_eq!(args.len(), 1);
                assert!(matches!(&args[0], Expr::Wildcard { .. }));
            }
            _ => panic!("expected AggCall"),
        }
    }

    #[test]
    fn test_parse_count_field() {
        let r = parse_ok("emit c = count(employee_id)");
        match emit_expr(&r) {
            Expr::AggCall { name, args, .. } => {
                assert_eq!(&**name, "count");
                assert!(
                    matches!(&args[0], Expr::FieldRef { name, .. } if &**name == "employee_id")
                );
            }
            _ => panic!("expected AggCall"),
        }
    }

    #[test]
    fn test_parse_count_bare() {
        // count() is sugar for count(*)
        let r = parse_ok("emit c = count()");
        match emit_expr(&r) {
            Expr::AggCall { name, args, .. } => {
                assert_eq!(&**name, "count");
                assert_eq!(args.len(), 1);
                assert!(matches!(&args[0], Expr::Wildcard { .. }));
            }
            _ => panic!("expected AggCall"),
        }
    }

    #[test]
    fn test_parse_all_agg_functions() {
        // All 7 agg names + count(*)
        let cases = [
            ("sum(x)", "sum"),
            ("count(x)", "count"),
            ("count(*)", "count"),
            ("avg(x)", "avg"),
            ("min(x)", "min"),
            ("max(x)", "max"),
            ("collect(x)", "collect"),
            ("weighted_avg(x, y)", "weighted_avg"),
        ];
        for (src, expected) in cases {
            let r = parse_ok(&format!("emit t = {src}"));
            match emit_expr(&r) {
                Expr::AggCall { name, .. } => assert_eq!(&**name, expected, "for {src}"),
                _ => panic!("expected AggCall for {src}"),
            }
        }
    }

    #[test]
    fn test_parse_weighted_avg_two_args() {
        let r = parse_ok("emit t = weighted_avg(score, weight)");
        match emit_expr(&r) {
            Expr::AggCall { name, args, .. } => {
                assert_eq!(&**name, "weighted_avg");
                assert_eq!(args.len(), 2);
            }
            _ => panic!("expected AggCall"),
        }
    }

    #[test]
    fn test_parse_count_star_plus_one_does_not_misparse() {
        // count(*) + 1 — the star must not be treated as multiplication.
        let r = parse_ok("emit c = count(*) + 1");
        match emit_expr(&r) {
            Expr::Binary { lhs, .. } => match &**lhs {
                Expr::AggCall { name, args, .. } => {
                    assert_eq!(&**name, "count");
                    assert!(matches!(&args[0], Expr::Wildcard { .. }));
                }
                _ => panic!("lhs should be count(*) AggCall"),
            },
            _ => panic!("expected Binary at top"),
        }
    }

    #[test]
    fn test_field_named_sum_still_parses_as_fieldref() {
        // A bare identifier `sum` (no parens) must still be a FieldRef so that
        // columns named `sum`, `count`, etc. can be referenced.
        let r = parse_ok("emit t = sum + 1");
        match emit_expr(&r) {
            Expr::Binary { lhs, .. } => {
                assert!(matches!(&**lhs, Expr::FieldRef { name, .. } if &**name == "sum"));
            }
            _ => panic!("expected Binary"),
        }
    }

    #[test]
    fn test_parse_emit_each_without_outer_is_emit_each() {
        let r = parse_ok("emit each it in items {\n  emit val = it\n}");
        match first_stmt(&r) {
            Statement::EmitEach { binding, .. } => assert_eq!(&**binding, "it"),
            other => panic!("expected EmitEach, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn test_parse_emit_each_outer_modifier_is_explode_outer() {
        // The trailing non-reserved `outer` identifier selects the
        // outer-join variant. The source expression parses greedily and
        // stops at `outer` because a bare identifier carries no infix
        // binding power.
        let r = parse_ok("emit each it in items outer {\n  emit val = it\n}");
        match first_stmt(&r) {
            Statement::ExplodeOuter {
                binding, source, ..
            } => {
                assert_eq!(&**binding, "it");
                // `outer` must not have been absorbed into the source.
                assert!(
                    matches!(source, Expr::FieldRef { name, .. } if &**name == "items"),
                    "source should be the `items` FieldRef, got {:?}",
                    std::mem::discriminant(source)
                );
            }
            other => panic!(
                "expected ExplodeOuter, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }

    #[test]
    fn test_parse_emit_each_outer_with_named_binding() {
        let r = parse_ok("emit each tag in tags outer {\n  emit t = tag\n}");
        match first_stmt(&r) {
            Statement::ExplodeOuter { binding, .. } => assert_eq!(&**binding, "tag"),
            other => panic!(
                "expected ExplodeOuter, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }

    #[test]
    fn test_parse_nested_emit_each_is_accepted() {
        // Nesting is no longer a parse error: an `emit each` body may
        // contain another `emit each`, parsed as a body statement.
        let r =
            parse_ok("emit each g in groups {\n  emit each it in g {\n    emit val = it\n  }\n}");
        match first_stmt(&r) {
            Statement::EmitEach { body, .. } => {
                assert!(
                    matches!(body.first(), Some(Statement::EmitEach { .. })),
                    "expected a nested EmitEach in the body, got {:?}",
                    body.first().map(std::mem::discriminant)
                );
            }
            other => panic!(
                "expected outer EmitEach, got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }

    #[test]
    fn test_parse_emit_each_source_named_outer_is_not_explode_outer() {
        // A source field literally named `outer` is consumed by the
        // source `parse_expr`; the modifier check then sees `{`, not a
        // trailing `outer`, so this is a plain EmitEach over
        // `FieldRef("outer")`, never ExplodeOuter. The modifier only
        // fires when an `outer` identifier sits *between* the source
        // expression and the `{`.
        let r = parse_ok("emit each it in outer {\n  emit val = it\n}");
        match first_stmt(&r) {
            Statement::EmitEach {
                binding, source, ..
            } => {
                assert_eq!(&**binding, "it");
                assert!(
                    matches!(source, Expr::FieldRef { name, .. } if &**name == "outer"),
                    "source should be the `outer` FieldRef, got {:?}",
                    std::mem::discriminant(source)
                );
            }
            other => panic!(
                "expected EmitEach over FieldRef(\"outer\"), got {:?}",
                std::mem::discriminant(other)
            ),
        }
    }
}
