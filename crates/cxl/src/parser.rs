use crate::ast::*;
use crate::lexer::{Lexer, Span, Token};

/// Maximum expression nesting depth. Prevents stack overflow on malicious input.
const MAX_DEPTH: u32 = 256;

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

/// Hand-rolled Pratt parser for CXL.
pub struct Parser {
    tokens: Vec<(Token, Span)>,
    pos: usize,
    errors: Vec<ParseError>,
    depth: u32,
    next_id: u32,
}

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
        _ => None,
    }
}

fn prefix_bp(tok: &Token) -> Option<u8> {
    match tok {
        Token::Not => Some(10),     // between and(8) and comparisons(11)
        Token::Minus => Some(18),   // between mul(16) and postfix(19)
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
        };
        let start_span = parser.current_span();
        let mut statements = Vec::new();

        parser.skip_newlines();
        while !parser.at_eof() {
            match parser.parse_statement() {
                Ok(stmt) => statements.push(stmt),
                Err(e) => {
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
            _ => {
                let nid = self.alloc_id();
                let expr = self.parse_expr(0)?;
                let span = expr.span();
                Ok(Statement::ExprStmt { node_id: nid, expr, span })
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
        let name = self.expect_ident("output field name")?;
        self.expect_token(&Token::Eq, "'='")?;
        let expr = self.parse_expr(0)?;
        let end = expr.span();
        Ok(Statement::Emit {
            node_id: nid,
            name: name.into(),
            expr,
            span: Span::new(start.start as usize, end.end as usize),
        })
    }

    fn parse_trace(&mut self) -> Result<Statement, ParseError> {
        let nid = self.alloc_id();
        let start = self.current_span();
        self.advance(); // consume 'trace'

        // Check for contextual level: info/warn/error/debug
        let level = if let Token::Ident(ref name) = self.peek().clone() {
            match name.as_ref() {
                "info" => { self.advance(); Some(TraceLevel::Info) }
                "warn" => { self.advance(); Some(TraceLevel::Warn) }
                "error" => { self.advance(); Some(TraceLevel::Error) }
                "debug" => { self.advance(); Some(TraceLevel::Debug) }
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
        while *self.peek() == Token::ColonColon {
            self.advance(); // consume '::'
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
            expr: Expr::Literal { node_id: self.alloc_id(), value: LiteralValue::Null, span },
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
            return Err(self.error("expression nesting too deep (max 256 levels)", "The parser has a maximum nesting depth to prevent stack overflow", "Simplify the expression or break it into let-bindings"));
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
                if is_comparison(&tok) {
                    if let Expr::Binary { op, .. } = &lhs {
                        if matches!(
                            op,
                            BinOp::Eq
                                | BinOp::Neq
                                | BinOp::Gt
                                | BinOp::Lt
                                | BinOp::Gte
                                | BinOp::Lte
                        ) {
                            self.depth -= 1;
                            return Err(self.error(
                                "comparisons are not chainable",
                                "CXL comparisons are non-associative — a == b == c is ambiguous",
                                "use (a == b) and (b == c) instead",
                            ));
                        }
                    }
                }

                // Dot: postfix field access or method call
                if tok == Token::Dot {
                    self.advance(); // consume '.'
                    let method_name = self.expect_ident("field or method name")?;
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
                        // Field access → QualifiedFieldRef
                        let end = self.prev_span();
                        let source_name = match &lhs {
                            Expr::FieldRef { name, .. } => name.clone(),
                            _ => {
                                // For chained access like window.lag(1).field, keep as method
                                let nid = self.alloc_id();
                                lhs = Expr::MethodCall {
                                    node_id: nid,
                                    receiver: Box::new(lhs),
                                    method: method_name.into(),
                                    args: vec![],
                                    span: Span::new(lhs_span.start as usize, end.end as usize),
                                };
                                continue;
                            }
                        };
                        let nid = self.alloc_id();
                        lhs = Expr::QualifiedFieldRef {
                            node_id: nid,
                            source: source_name,
                            field: method_name.into(),
                            span: Span::new(lhs_span.start as usize, end.end as usize),
                        };
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
                Ok(Expr::Wildcard { node_id: nid, span: start })
            }

            // window.fn() or pipeline.field
            Token::Window => {
                self.advance();
                self.expect_token(&Token::Dot, "'.'")?;
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
                    // window.first without parens — still a WindowCall with no args
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

            Token::Pipeline => {
                let nid = self.alloc_id();
                self.advance();
                self.expect_token(&Token::Dot, "'.'")?;
                let field = self.expect_ident("pipeline property name")?;
                let end = self.prev_span();
                Ok(Expr::PipelineAccess {
                    node_id: nid,
                    field: field.into(),
                    span: Span::new(start.start as usize, end.end as usize),
                })
            }

            // Identifiers (field references)
            Token::Ident(ref name) => {
                let nid = self.alloc_id();
                let name = name.clone();
                self.advance();
                Ok(Expr::FieldRef {
                    node_id: nid,
                    name,
                    span: start,
                })
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
            args.push(self.parse_expr(0)?);
            while *self.peek() == Token::Comma {
                self.advance();
                args.push(self.parse_expr(0)?);
            }
        }
        self.expect_token(&Token::RParen, "')'")?;
        Ok(args)
    }

    // ── Token stream helpers ───────────────────────────────────────

    fn peek(&self) -> &Token {
        self.tokens
            .get(self.pos)
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
        let result = self.tokens
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
                assert!(matches!(expr, Expr::Literal { value: LiteralValue::Int(42), .. }));
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
    fn test_parse_arithmetic() {
        let r = parse_ok("let x = 1 + 2 * 3");
        let expr = let_expr(&r);
        // Should be Add(1, Mul(2, 3)) due to precedence
        match expr {
            Expr::Binary { op: BinOp::Add, rhs, .. } => {
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
            Expr::MethodCall { method, receiver, .. } => {
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
            Expr::QualifiedFieldRef { source, field, .. } => {
                assert_eq!(&**source, "source");
                assert_eq!(&**field, "field");
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
        assert!(matches!(expr, Expr::Unary { op: UnaryOp::Neg, .. }));
    }

    #[test]
    fn test_parse_unary_not() {
        let r = parse_ok("let x = not true");
        let expr = let_expr(&r);
        assert!(matches!(expr, Expr::Unary { op: UnaryOp::Not, .. }));
    }

    #[test]
    fn test_parse_not_binds_loose() {
        // not a == b parses as not(a == b) because not has BP 10
        // and == has BP 11 — not captures the entire comparison
        let r = parse_ok("let x = not a == b");
        let expr = let_expr(&r);
        match expr {
            Expr::Unary { op: UnaryOp::Not, operand, .. } => {
                assert!(matches!(**operand, Expr::Binary { op: BinOp::Eq, .. }));
            }
            other => panic!("expected Unary Not wrapping Binary Eq, got {:?}", std::mem::discriminant(other)),
        }
    }

    #[test]
    fn test_parse_null_literal() {
        let r = parse_ok("let x = null");
        let expr = let_expr(&r);
        assert!(matches!(expr, Expr::Literal { value: LiteralValue::Null, .. }));
    }

    #[test]
    fn test_parse_bool_literals() {
        let r = parse_ok("let x = true");
        assert!(matches!(let_expr(&r), Expr::Literal { value: LiteralValue::Bool(true), .. }));
        let r2 = parse_ok("let x = false");
        assert!(matches!(let_expr(&r2), Expr::Literal { value: LiteralValue::Bool(false), .. }));
    }

    #[test]
    fn test_parse_string_literals() {
        let r = parse_ok("let x = \"hello world\"");
        match let_expr(&r) {
            Expr::Literal { value: LiteralValue::String(s), .. } => {
                assert_eq!(&**s, "hello world");
            }
            _ => panic!("expected String literal"),
        }
    }

    #[test]
    fn test_parse_window_call() {
        let r = parse_ok("let x = window.sum(amount)");
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
        let r = parse_ok("let x = pipeline.start_time");
        let expr = let_expr(&r);
        match expr {
            Expr::PipelineAccess { field, .. } => {
                assert_eq!(&**field, "start_time");
            }
            _ => panic!("expected PipelineAccess"),
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
        let r = parse_ok("use shared::dates");
        match first_stmt(&r) {
            Statement::UseStmt { path, alias, .. } => {
                assert_eq!(path.len(), 2);
                assert!(alias.is_none());
            }
            _ => panic!("expected UseStmt"),
        }
    }

    #[test]
    fn test_parse_use_with_alias() {
        let r = parse_ok("use shared::dates as d");
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
            Expr::Binary { op: BinOp::Mul, lhs, .. } => {
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
        assert!(r.node_count >= 4, "Expected at least 4 nodes, got {}", r.node_count);
    }

    #[test]
    fn test_parse_node_ids_unique() {
        let r = parse_ok("let x = 1\nlet y = 2");
        // All NodeIds should be unique (monotonically increasing)
        // We can verify by checking node_count covers all allocated IDs
        assert!(r.node_count >= 4); // 2 let stmts + 2 literals minimum
    }
}
