//! Pratt parsing with an explicit, depth-bounded continuation stack.
//!
//! A child expression suspends its parent's binding-power loop and constructor.
//! Keeping those frames here avoids retaining large native stack frames for every
//! nested expression, including in unoptimized builds on small platform stacks.

use super::*;

#[derive(Clone, Copy)]
struct NodeStart {
    node_id: NodeId,
    span: Span,
}

struct Frame {
    depth: u32,
    action: Action,
}

enum Action {
    Expr(u8),
    Tail(u8),
    Paren,
    Unary(NodeStart, UnaryOp),
    Binary(NodeId, Expr, Option<BinOp>),
    Index(Expr),
    CallNext(Call),
    CallArg(Call),
    Closure(NodeStart),
    ArrayItem(Array),
    ComprehensionSource(NodeStart, Expr, Box<str>),
    ComprehensionPredicate(NodeStart, Expr, Box<str>, Expr),
    MapNext(Map),
    MapKey(Map, Span),
    MapValue(Map, Span, MapKey),
    IfCondition(NodeStart),
    IfThen(NodeStart, Expr),
    IfElse(NodeStart, Expr, Expr),
    MatchSubject(NodeStart),
    MatchNext(Match),
    MatchPattern(Match, NodeStart),
    MatchBody(Match, NodeStart, Expr),
}

struct Call {
    start: NodeStart,
    kind: CallKind,
    args: Vec<Expr>,
}

enum CallKind {
    Method(Expr, Box<str>),
    Window(Box<str>),
    Aggregate(Box<str>),
}

struct Array {
    start: NodeStart,
    elements: Vec<Expr>,
}

struct Map {
    start: NodeStart,
    entries: Vec<MapEntry>,
    static_keys: Vec<String>,
}

struct Match {
    start: NodeStart,
    subject: Option<Box<Expr>>,
    arms: Vec<MatchArm>,
}

impl Parser {
    pub(super) fn parse_expr(&mut self, min_bp: u8) -> Result<Expr, ParseError> {
        // At most a tail and one constructor continuation per expression level,
        // plus one closure continuation per call. MAX_DEPTH bounds live frames;
        // argument/element counts belong to the AST, not the native call stack.
        let mut frames = vec![Frame {
            depth: 1,
            action: Action::Expr(min_bp),
        }];
        let mut value = None;
        while let Some(Frame { depth, action }) = frames.pop() {
            self.resume_expr(action, depth, &mut frames, &mut value)?;
        }
        Ok(value.expect("completed expression has a value"))
    }

    fn resume_expr(
        &mut self,
        action: Action,
        depth: u32,
        frames: &mut Vec<Frame>,
        value: &mut Option<Expr>,
    ) -> Result<(), ParseError> {
        match action {
            Action::Expr(min_bp) => {
                if depth > MAX_DEPTH {
                    return Err(self.error(
                        "expression nesting too deep (max 256 levels)",
                        "The parser has a maximum nesting depth to prevent stack overflow",
                        "Simplify the expression or break it into let-bindings",
                    ));
                }
                push(frames, depth, Action::Tail(min_bp));
                self.start_expr(depth, frames, value)?;
            }
            Action::Tail(min_bp) => self.continue_expr(min_bp, depth, frames, value)?,
            Action::Paren => {
                self.expect_token(&Token::RParen, "')'")?;
            }
            Action::Unary(start, op) => {
                let operand = take_value(value);
                let span = through(start.span, operand.span());
                *value = Some(Expr::Unary {
                    node_id: start.node_id,
                    op,
                    operand: Box::new(operand),
                    span,
                });
            }
            Action::Binary(node_id, lhs, op) => {
                let rhs = take_value(value);
                let span = through(lhs.span(), rhs.span());
                *value = Some(match op {
                    Some(op) => Expr::Binary {
                        node_id,
                        op,
                        lhs: Box::new(lhs),
                        rhs: Box::new(rhs),
                        span,
                    },
                    None => Expr::Coalesce {
                        node_id,
                        lhs: Box::new(lhs),
                        rhs: Box::new(rhs),
                        span,
                    },
                });
            }
            Action::Index(receiver) => {
                let index = take_value(value);
                self.expect_token(&Token::RBracket, "']'")?;
                *value = Some(Expr::IndexAccess {
                    node_id: self.alloc_id(),
                    span: through(receiver.span(), self.prev_span()),
                    receiver: Box::new(receiver),
                    index: Box::new(index),
                });
            }
            Action::CallNext(call) => self.start_call_arg(call, depth, frames, value)?,
            Action::CallArg(mut call) => {
                call.args.push(take_value(value));
                if *self.peek() == Token::Comma {
                    self.advance();
                    self.start_call_arg(call, depth, frames, value)?;
                } else {
                    *value = Some(self.finish_call(call)?);
                }
            }
            Action::Closure(start) => {
                let body = take_value(value);
                *value = Some(Expr::Closure {
                    node_id: start.node_id,
                    span: through(start.span, body.span()),
                    param: "it".into(),
                    body: Box::new(body),
                });
            }
            Action::ArrayItem(array) => self.finish_array_item(array, depth, frames, value)?,
            Action::ComprehensionSource(start, item, binding) => {
                let source = take_value(value);
                self.skip_newlines();
                if *self.peek() == Token::If {
                    self.advance();
                    self.skip_newlines();
                    child(
                        frames,
                        depth,
                        Action::ComprehensionPredicate(start, item, binding, source),
                        0,
                    );
                } else {
                    *value = Some(self.finish_comprehension(start, item, binding, source, None)?);
                }
            }
            Action::ComprehensionPredicate(start, item, binding, source) => {
                let predicate = take_value(value);
                self.skip_newlines();
                *value = Some(self.finish_comprehension(
                    start,
                    item,
                    binding,
                    source,
                    Some(Box::new(predicate)),
                )?);
            }
            Action::MapNext(map) => self.start_map_entry(map, depth, frames, value)?,
            Action::MapKey(map, entry_start) => {
                let key = MapKey::Computed(Box::new(take_value(value)));
                self.skip_newlines();
                self.expect_token(&Token::RBracket, "']'")?;
                self.start_map_value(map, entry_start, key, depth, frames)?;
            }
            Action::MapValue(mut map, entry_start, key) => {
                let entry_value = take_value(value);
                map.entries.push(MapEntry {
                    span: through(entry_start, entry_value.span()),
                    key,
                    value: entry_value,
                });
                self.skip_newlines();
                if *self.peek() == Token::Comma {
                    self.advance();
                    self.skip_newlines();
                    push(frames, depth, Action::MapNext(map));
                } else {
                    *value = Some(self.finish_map(map)?);
                }
            }
            Action::IfCondition(start) => {
                let condition = take_value(value);
                self.expect_token(&Token::Then, "'then'")?;
                child(frames, depth, Action::IfThen(start, condition), 1);
            }
            Action::IfThen(start, condition) => {
                let then_branch = take_value(value);
                self.skip_newlines();
                if *self.peek() == Token::Else {
                    self.advance();
                    child(
                        frames,
                        depth,
                        Action::IfElse(start, condition, then_branch),
                        1,
                    );
                } else {
                    *value = Some(finish_if(start, condition, then_branch, None));
                }
            }
            Action::IfElse(start, condition, then_branch) => {
                let else_branch = take_value(value);
                *value = Some(finish_if(
                    start,
                    condition,
                    then_branch,
                    Some(Box::new(else_branch)),
                ));
            }
            Action::MatchSubject(start) => {
                let subject = Some(Box::new(take_value(value)));
                self.expect_token(&Token::LBrace, "'{'")?;
                push(
                    frames,
                    depth,
                    Action::MatchNext(Match {
                        start,
                        subject,
                        arms: Vec::new(),
                    }),
                );
            }
            Action::MatchNext(matched) => self.start_match_arm(matched, depth, frames, value)?,
            Action::MatchPattern(matched, start) => {
                let pattern = take_value(value);
                self.expect_token(&Token::FatArrow, "'=>'")?;
                child(frames, depth, Action::MatchBody(matched, start, pattern), 0);
            }
            Action::MatchBody(mut matched, start, pattern) => {
                let body = take_value(value);
                matched.arms.push(MatchArm {
                    node_id: start.node_id,
                    span: through(start.span, body.span()),
                    pattern,
                    body,
                });
                if *self.peek() == Token::Comma {
                    self.advance();
                }
                self.skip_newlines();
                push(frames, depth, Action::MatchNext(matched));
            }
        }
        Ok(())
    }

    fn start_expr(
        &mut self,
        depth: u32,
        frames: &mut Vec<Frame>,
        value: &mut Option<Expr>,
    ) -> Result<(), ParseError> {
        match self.peek() {
            Token::Not | Token::Minus => {
                let bp = prefix_bp(self.peek()).expect("prefix operator has binding power");
                let op = if *self.peek() == Token::Not {
                    UnaryOp::Not
                } else {
                    UnaryOp::Neg
                };
                let start = self.node_start();
                self.advance();
                child(frames, depth, Action::Unary(start, op), bp);
            }
            Token::LParen => {
                self.advance();
                child(frames, depth, Action::Paren, 0);
            }
            Token::LBracket => {
                let start = self.node_start();
                self.advance();
                self.skip_newlines();
                let array = Array {
                    start,
                    elements: Vec::new(),
                };
                if *self.peek() == Token::RBracket {
                    *value = Some(self.finish_array(array)?);
                } else {
                    child(frames, depth, Action::ArrayItem(array), 0);
                }
            }
            Token::LBrace => {
                let start = self.node_start();
                self.advance();
                self.skip_newlines();
                push(
                    frames,
                    depth,
                    Action::MapNext(Map {
                        start,
                        entries: Vec::new(),
                        static_keys: Vec::new(),
                    }),
                );
            }
            Token::If => {
                let start = self.node_start();
                self.advance();
                child(frames, depth, Action::IfCondition(start), 5);
            }
            Token::Match => {
                let start = self.node_start();
                self.advance();
                if *self.peek() == Token::LBrace {
                    self.advance();
                    push(
                        frames,
                        depth,
                        Action::MatchNext(Match {
                            start,
                            subject: None,
                            arms: Vec::new(),
                        }),
                    );
                } else {
                    child(frames, depth, Action::MatchSubject(start), 0);
                }
            }
            Token::Ident(name)
                if is_aggregate_name(name) && *self.peek_ahead(1) == Token::LParen =>
            {
                let name = name.clone();
                let start = self.node_start();
                self.advance();
                self.advance();
                self.start_call(
                    Call {
                        start,
                        kind: CallKind::Aggregate(name),
                        args: Vec::new(),
                    },
                    depth,
                    frames,
                    value,
                )?;
            }
            _ => match self.parse_atom()? {
                Expr::WindowCall {
                    node_id,
                    function,
                    span,
                    ..
                } if *self.peek() == Token::LParen => {
                    self.advance();
                    self.start_call(
                        Call {
                            start: NodeStart { node_id, span },
                            kind: CallKind::Window(function),
                            args: Vec::new(),
                        },
                        depth,
                        frames,
                        value,
                    )?;
                }
                atom => *value = Some(atom),
            },
        }
        Ok(())
    }

    fn continue_expr(
        &mut self,
        min_bp: u8,
        depth: u32,
        frames: &mut Vec<Frame>,
        value: &mut Option<Expr>,
    ) -> Result<(), ParseError> {
        let Some((l_bp, r_bp)) = infix_bp(self.peek()) else {
            return Ok(());
        };
        if l_bp < min_bp {
            return Ok(());
        }
        let lhs = take_value(value);
        if is_comparison(self.peek())
            && matches!(
                lhs,
                Expr::Binary {
                    op: BinOp::Eq | BinOp::Neq | BinOp::Gt | BinOp::Lt | BinOp::Gte | BinOp::Lte,
                    ..
                }
            )
        {
            return Err(self.error(
                "comparisons are not chainable",
                "CXL comparisons are non-associative — a == b == c is ambiguous",
                "use (a == b) and (b == c) instead",
            ));
        }
        push(frames, depth, Action::Tail(min_bp));
        match self.peek() {
            Token::LBracket => {
                self.advance();
                child(frames, depth, Action::Index(lhs), 0);
            }
            Token::Dot => {
                self.advance();
                let name = self.expect_ident_or_keyword("field or method name")?;
                if *self.peek() == Token::LParen {
                    let start = NodeStart {
                        node_id: self.alloc_id(),
                        span: lhs.span(),
                    };
                    self.advance();
                    self.start_call(
                        Call {
                            start,
                            kind: CallKind::Method(lhs, name.into()),
                            args: Vec::new(),
                        },
                        depth,
                        frames,
                        value,
                    )?;
                } else {
                    let node_id = self.alloc_id();
                    let span = through(lhs.span(), self.prev_span());
                    *value = Some(match lhs {
                        Expr::FieldRef { name: first, .. } => Expr::QualifiedFieldRef {
                            node_id,
                            parts: vec![first, name.into()].into_boxed_slice(),
                            span,
                        },
                        Expr::QualifiedFieldRef { parts, .. } => {
                            let mut parts = parts.into_vec();
                            parts.push(name.into());
                            Expr::QualifiedFieldRef {
                                node_id,
                                parts: parts.into_boxed_slice(),
                                span,
                            }
                        }
                        receiver => Expr::MethodCall {
                            node_id,
                            receiver: Box::new(receiver),
                            method: name.into(),
                            args: Vec::new(),
                            span,
                        },
                    });
                }
            }
            tok => {
                let op = token_to_binop(tok);
                let node_id = self.alloc_id();
                self.advance();
                child(frames, depth, Action::Binary(node_id, lhs, op), r_bp);
            }
        }
        Ok(())
    }

    fn start_call(
        &mut self,
        call: Call,
        depth: u32,
        frames: &mut Vec<Frame>,
        value: &mut Option<Expr>,
    ) -> Result<(), ParseError> {
        if *self.peek() == Token::RParen {
            *value = Some(self.finish_call(call)?);
        } else {
            push(frames, depth, Action::CallNext(call));
        }
        Ok(())
    }

    fn start_call_arg(
        &mut self,
        call: Call,
        depth: u32,
        frames: &mut Vec<Frame>,
        value: &mut Option<Expr>,
    ) -> Result<(), ParseError> {
        let aggregate = matches!(call.kind, CallKind::Aggregate(_));
        push(frames, depth, Action::CallArg(call));
        if aggregate && *self.peek() == Token::Star {
            let start = self.node_start();
            self.advance();
            *value = Some(Expr::Wildcard {
                node_id: start.node_id,
                span: start.span,
            });
        } else {
            if !aggregate && *self.peek() == Token::It && *self.peek_ahead(1) == Token::FatArrow {
                let start = self.node_start();
                self.advance();
                self.advance();
                push(frames, depth, Action::Closure(start));
            }
            push(frames, depth + 1, Action::Expr(0));
        }
        Ok(())
    }

    fn finish_call(&mut self, mut call: Call) -> Result<Expr, ParseError> {
        self.expect_token(&Token::RParen, "')'")?;
        let node_id = call.start.node_id;
        let span = through(call.start.span, self.prev_span());
        Ok(match call.kind {
            CallKind::Method(receiver, method) => Expr::MethodCall {
                node_id,
                receiver: Box::new(receiver),
                method,
                args: call.args,
                span,
            },
            CallKind::Window(function) => Expr::WindowCall {
                node_id,
                function,
                args: call.args,
                span,
            },
            CallKind::Aggregate(name) => {
                if call.args.is_empty() && &*name == "count" {
                    call.args.push(Expr::Wildcard {
                        node_id: self.alloc_id(),
                        span: call.start.span,
                    });
                }
                Expr::AggCall {
                    node_id,
                    name,
                    args: call.args,
                    span,
                }
            }
        })
    }

    fn finish_array_item(
        &mut self,
        mut array: Array,
        depth: u32,
        frames: &mut Vec<Frame>,
        value: &mut Option<Expr>,
    ) -> Result<(), ParseError> {
        let item = take_value(value);
        self.skip_newlines();
        if array.elements.is_empty() && *self.peek() == Token::For {
            self.advance();
            self.skip_newlines();
            let binding = self.expect_ident("array-comprehension binding")?;
            self.expect_token(&Token::In, "'in'")?;
            self.skip_newlines();
            child(
                frames,
                depth,
                Action::ComprehensionSource(array.start, item, binding.into()),
                0,
            );
            return Ok(());
        }
        array.elements.push(item);
        if *self.peek() == Token::Comma {
            self.advance();
            self.skip_newlines();
            if *self.peek() != Token::RBracket {
                child(frames, depth, Action::ArrayItem(array), 0);
                return Ok(());
            }
        }
        *value = Some(self.finish_array(array)?);
        Ok(())
    }

    fn finish_array(&mut self, array: Array) -> Result<Expr, ParseError> {
        self.expect_token(&Token::RBracket, "']'")?;
        Ok(Expr::ArrayLiteral {
            node_id: array.start.node_id,
            elements: array.elements,
            span: through(array.start.span, self.prev_span()),
        })
    }

    fn finish_comprehension(
        &mut self,
        start: NodeStart,
        item: Expr,
        binding: Box<str>,
        source: Expr,
        predicate: Option<Box<Expr>>,
    ) -> Result<Expr, ParseError> {
        self.expect_token(&Token::RBracket, "']'")?;
        Ok(Expr::ArrayComprehension {
            node_id: start.node_id,
            item: Box::new(item),
            binding,
            source: Box::new(source),
            predicate,
            span: through(start.span, self.prev_span()),
        })
    }

    fn start_map_entry(
        &mut self,
        mut map: Map,
        depth: u32,
        frames: &mut Vec<Frame>,
        value: &mut Option<Expr>,
    ) -> Result<(), ParseError> {
        if *self.peek() == Token::RBrace {
            *value = Some(self.finish_map(map)?);
            return Ok(());
        }
        let entry_start = self.current_span();
        match self.peek().clone() {
            Token::Ident(key) | Token::StringLit(key) => {
                self.advance();
                let decoded = clinker_record::nested_key::NestedKey::decode(&key).map_err(|error| self.error(
                    &error.to_string(),
                    "nested map keys use one canonical backslash escape grammar",
                    "use `\\@name`, `\\#text`, or `\\\\name` only when escaping a reserved-looking literal key",
                ))?;
                if map
                    .static_keys
                    .iter()
                    .any(|existing| existing == decoded.text.as_ref())
                {
                    return Err(self.error(
                        &format!("duplicate map key {:?}", decoded.text),
                        "static map keys must be unique after canonical escape decoding",
                        "remove or rename the duplicate key",
                    ));
                }
                map.static_keys.push(decoded.text.into_owned());
                self.start_map_value(map, entry_start, MapKey::Static(key), depth, frames)?;
            }
            Token::LBracket => {
                self.advance();
                self.skip_newlines();
                child(frames, depth, Action::MapKey(map, entry_start), 0);
            }
            _ => {
                return Err(self.error(
                    "map keys must be identifiers, strings, or computed string expressions",
                    "CXL maps use `{ name: value }`, `{ \"name\": value }`, or `{ [expr]: value }`",
                    "quote the key or wrap a string-valued expression in brackets",
                ));
            }
        }
        Ok(())
    }

    fn start_map_value(
        &mut self,
        map: Map,
        entry_start: Span,
        key: MapKey,
        depth: u32,
        frames: &mut Vec<Frame>,
    ) -> Result<(), ParseError> {
        self.skip_newlines();
        self.expect_token(&Token::Colon, "':'")?;
        self.skip_newlines();
        child(frames, depth, Action::MapValue(map, entry_start, key), 0);
        Ok(())
    }

    fn finish_map(&mut self, map: Map) -> Result<Expr, ParseError> {
        self.expect_token(&Token::RBrace, "'}'")?;
        Ok(Expr::MapLiteral {
            node_id: map.start.node_id,
            entries: map.entries,
            span: through(map.start.span, self.prev_span()),
        })
    }

    fn start_match_arm(
        &mut self,
        matched: Match,
        depth: u32,
        frames: &mut Vec<Frame>,
        value: &mut Option<Expr>,
    ) -> Result<(), ParseError> {
        // Preserve recovery at an unterminated arm: EOF after a newline is
        // where an arm expression was expected, while immediate EOF is where
        // the closing brace was expected.
        let at_eof = self.at_eof();
        self.skip_newlines();
        if *self.peek() == Token::RBrace || at_eof {
            self.expect_token(&Token::RBrace, "'}'")?;
            *value = Some(Expr::Match {
                node_id: matched.start.node_id,
                subject: matched.subject,
                arms: matched.arms,
                span: through(matched.start.span, self.prev_span()),
            });
        } else {
            let start = self.node_start();
            child(frames, depth, Action::MatchPattern(matched, start), 0);
        }
        Ok(())
    }

    fn node_start(&mut self) -> NodeStart {
        NodeStart {
            node_id: self.alloc_id(),
            span: self.current_span(),
        }
    }
}

fn push(frames: &mut Vec<Frame>, depth: u32, action: Action) {
    frames.push(Frame { depth, action });
}

fn child(frames: &mut Vec<Frame>, depth: u32, continuation: Action, min_bp: u8) {
    push(frames, depth, continuation);
    push(frames, depth + 1, Action::Expr(min_bp));
}

fn take_value(value: &mut Option<Expr>) -> Expr {
    value
        .take()
        .expect("expression continuation follows its child")
}

fn through(start: Span, end: Span) -> Span {
    Span::new(start.start as usize, end.end as usize)
}

fn finish_if(
    start: NodeStart,
    condition: Expr,
    then_branch: Expr,
    else_branch: Option<Box<Expr>>,
) -> Expr {
    let end = else_branch.as_deref().unwrap_or(&then_branch).span();
    Expr::IfThenElse {
        node_id: start.node_id,
        condition: Box::new(condition),
        then_branch: Box::new(then_branch),
        else_branch,
        span: through(start.span, end),
    }
}
