use crate::lexer::Span;

/// Unique identifier for an AST node. Assigned monotonically by the parser.
/// Used as index into side-tables (bindings, types, regexes).
/// Fits in alignment padding — size_of::<Expr>() stays at 64 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NodeId(pub u32);

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
    Let {
        node_id: NodeId,
        name: Box<str>,
        expr: Expr,
        span: Span,
    },
    Emit {
        node_id: NodeId,
        name: Box<str>,
        expr: Expr,
        /// When true, writes to per-record metadata (`$meta.*`) instead of output.
        is_meta: bool,
        span: Span,
    },
    Trace {
        node_id: NodeId,
        level: Option<TraceLevel>,
        guard: Option<Box<Expr>>,
        message: Expr,
        span: Span,
    },
    UseStmt {
        node_id: NodeId,
        path: Vec<Box<str>>,
        alias: Option<Box<str>>,
        span: Span,
    },
    ExprStmt {
        node_id: NodeId,
        expr: Expr,
        span: Span,
    },
    Filter {
        node_id: NodeId,
        predicate: Expr,
        span: Span,
    },
    Distinct {
        node_id: NodeId,
        /// None = bare `distinct` (all fields), Some = `distinct by <field>`
        field: Option<Box<str>>,
        span: Span,
    },
}

/// CXL expression — the core of the language. All variants carry a NodeId and Span.
#[derive(Debug, Clone)]
pub enum Expr {
    Binary {
        node_id: NodeId,
        op: BinOp,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
        span: Span,
    },
    Unary {
        node_id: NodeId,
        op: UnaryOp,
        operand: Box<Expr>,
        span: Span,
    },
    Literal {
        node_id: NodeId,
        value: LiteralValue,
        span: Span,
    },
    FieldRef {
        node_id: NodeId,
        name: Box<str>,
        span: Span,
    },
    /// Qualified field reference — N-part dotted path.
    /// 2 parts: source.field (existing). 3 parts: source.record_type.field (multi-record).
    QualifiedFieldRef {
        node_id: NodeId,
        parts: Box<[Box<str>]>,
        span: Span,
    },
    MethodCall {
        node_id: NodeId,
        receiver: Box<Expr>,
        method: Box<str>,
        args: Vec<Expr>,
        span: Span,
    },
    Match {
        node_id: NodeId,
        subject: Option<Box<Expr>>,
        arms: Vec<MatchArm>,
        span: Span,
    },
    IfThenElse {
        node_id: NodeId,
        condition: Box<Expr>,
        then_branch: Box<Expr>,
        else_branch: Option<Box<Expr>>,
        span: Span,
    },
    Coalesce {
        node_id: NodeId,
        lhs: Box<Expr>,
        rhs: Box<Expr>,
        span: Span,
    },
    WindowCall {
        node_id: NodeId,
        function: Box<str>,
        args: Vec<Expr>,
        span: Span,
    },
    PipelineAccess {
        node_id: NodeId,
        field: Box<str>,
        span: Span,
    },
    MetaAccess {
        node_id: NodeId,
        field: Box<str>,
        span: Span,
    },
    /// The `now` keyword — wall-clock DateTime at the point of evaluation.
    Now {
        node_id: NodeId,
        span: Span,
    },
    Wildcard {
        node_id: NodeId,
        span: Span,
    },
    /// Free-standing aggregate function call (sum, count, avg, min, max,
    /// collect, weighted_avg). Parsed via NUD lookahead: Ident + LParen where
    /// the identifier matches a known aggregate name. Distinct from
    /// MethodCall (which is receiver-based via `.`).
    AggCall {
        node_id: NodeId,
        name: Box<str>,
        args: Vec<Expr>,
        span: Span,
    },
    /// Extractor-produced leaf: references the Nth accumulator slot in a
    /// CompiledAggregate. Never emitted by the parser; introduced by
    /// `extract_aggregates` when rewriting an emit RHS that contains AggCalls.
    /// At finalize time, the aggregate evaluator resolves this to the
    /// accumulator's `finalize()` result.
    AggSlot {
        node_id: NodeId,
        slot: u32,
        span: Span,
    },
    /// Extractor-produced leaf: references the Nth group-by key column.
    /// Introduced when a bare `FieldRef` in an emit RHS matches a group-by
    /// field name — reverses the `value_to_group_key → Value` conversion at
    /// finalize time.
    GroupKey {
        node_id: NodeId,
        slot: u32,
        span: Span,
    },
}

impl Expr {
    /// Get the span of this expression.
    pub fn span(&self) -> Span {
        match self {
            Expr::Binary { span, .. }
            | Expr::Unary { span, .. }
            | Expr::Literal { span, .. }
            | Expr::FieldRef { span, .. }
            | Expr::QualifiedFieldRef { span, .. }
            | Expr::MethodCall { span, .. }
            | Expr::Match { span, .. }
            | Expr::IfThenElse { span, .. }
            | Expr::Coalesce { span, .. }
            | Expr::WindowCall { span, .. }
            | Expr::PipelineAccess { span, .. }
            | Expr::MetaAccess { span, .. }
            | Expr::Now { span, .. }
            | Expr::Wildcard { span, .. }
            | Expr::AggCall { span, .. }
            | Expr::AggSlot { span, .. }
            | Expr::GroupKey { span, .. } => *span,
        }
    }

    /// Get the NodeId of this expression.
    pub fn node_id(&self) -> NodeId {
        match self {
            Expr::Binary { node_id, .. }
            | Expr::Unary { node_id, .. }
            | Expr::Literal { node_id, .. }
            | Expr::FieldRef { node_id, .. }
            | Expr::QualifiedFieldRef { node_id, .. }
            | Expr::MethodCall { node_id, .. }
            | Expr::Match { node_id, .. }
            | Expr::IfThenElse { node_id, .. }
            | Expr::Coalesce { node_id, .. }
            | Expr::WindowCall { node_id, .. }
            | Expr::PipelineAccess { node_id, .. }
            | Expr::MetaAccess { node_id, .. }
            | Expr::Now { node_id, .. }
            | Expr::Wildcard { node_id, .. }
            | Expr::AggCall { node_id, .. }
            | Expr::AggSlot { node_id, .. }
            | Expr::GroupKey { node_id, .. } => *node_id,
        }
    }

    /// Accumulate the set of input column references this expression
    /// (and its sub-expressions) reads. Used by the planner's
    /// deferred-region column-pruning pass to compute the minimum
    /// buffer schema a producing Aggregate needs to carry forward to
    /// commit time.
    ///
    /// `$pipeline.*`, `$meta.*`, and `$ck.*` are skipped — they are
    /// system namespaces, not record-schema columns. The deferred
    /// buffer carries `$ck.*` shadow columns implicitly via row
    /// identity, so they don't need tracking here. This mirrors the
    /// analyzer's `walk_expr` namespace-exclusion convention.
    pub fn support_into(&self, fields: &mut std::collections::HashSet<String>) {
        match self {
            Expr::FieldRef { name, .. } => {
                if !is_system_namespace(name) {
                    fields.insert(name.to_string());
                }
            }
            Expr::QualifiedFieldRef { parts, .. } => {
                if let Some(first) = parts.first() {
                    if !is_system_namespace(first) {
                        fields.insert(
                            parts
                                .iter()
                                .map(|p| p.as_ref())
                                .collect::<Vec<_>>()
                                .join("."),
                        );
                    }
                }
            }
            Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
                lhs.support_into(fields);
                rhs.support_into(fields);
            }
            Expr::Unary { operand, .. } => operand.support_into(fields),
            Expr::IfThenElse {
                condition,
                then_branch,
                else_branch,
                ..
            } => {
                condition.support_into(fields);
                then_branch.support_into(fields);
                if let Some(e) = else_branch {
                    e.support_into(fields);
                }
            }
            Expr::Match { subject, arms, .. } => {
                if let Some(s) = subject {
                    s.support_into(fields);
                }
                for arm in arms {
                    arm.pattern.support_into(fields);
                    arm.body.support_into(fields);
                }
            }
            Expr::MethodCall { receiver, args, .. } => {
                receiver.support_into(fields);
                for a in args {
                    a.support_into(fields);
                }
            }
            Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
                for a in args {
                    a.support_into(fields);
                }
            }
            Expr::PipelineAccess { .. }
            | Expr::MetaAccess { .. }
            | Expr::Now { .. }
            | Expr::Wildcard { .. }
            | Expr::Literal { .. }
            | Expr::AggSlot { .. }
            | Expr::GroupKey { .. } => {}
        }
    }
}

fn is_system_namespace(name: &str) -> bool {
    name.starts_with("$pipeline") || name.starts_with("$meta") || name.starts_with("$ck")
}

#[derive(Debug, Clone)]
pub struct MatchArm {
    pub node_id: NodeId,
    pub pattern: Expr,
    pub body: Expr,
    pub span: Span,
}

/// Module-level function declaration. Pure: single expression body.
#[derive(Debug, Clone)]
pub struct FnDecl {
    pub node_id: NodeId,
    pub name: Box<str>,
    pub params: Vec<Box<str>>,
    pub body: Box<Expr>,
    pub span: Span,
}

/// A parsed CXL module file (only fn declarations and let constants).
#[derive(Debug, Clone)]
pub struct Module {
    pub functions: Vec<FnDecl>,
    pub constants: Vec<ModuleConst>,
    pub span: Span,
}

/// A module-level constant binding.
#[derive(Debug, Clone)]
pub struct ModuleConst {
    pub node_id: NodeId,
    pub name: Box<str>,
    pub expr: Expr,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    Neq,
    Gt,
    Lt,
    Gte,
    Lte,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}

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
static_assertions::assert_impl_all!(Module: Send, Sync);
static_assertions::assert_impl_all!(ModuleConst: Send, Sync);
static_assertions::assert_impl_all!(MatchArm: Send, Sync);
static_assertions::assert_impl_all!(NodeId: Send, Sync);

#[cfg(test)]
mod tests {
    use super::*;

    /// Dummy NodeId for test construction (tests don't need unique IDs).
    const NID: NodeId = NodeId(0);

    #[test]
    fn test_ast_program_construction() {
        let span = Span::new(0, 10);
        let program = Program {
            statements: vec![
                Statement::Let {
                    node_id: NID,
                    name: "x".into(),
                    expr: Expr::Literal {
                        node_id: NID,
                        value: LiteralValue::Int(1),
                        span,
                    },
                    span,
                },
                Statement::Emit {
                    node_id: NID,
                    name: "out".into(),
                    expr: Expr::FieldRef {
                        node_id: NID,
                        name: "x".into(),
                        span,
                    },
                    is_meta: false,
                    span,
                },
                Statement::ExprStmt {
                    node_id: NID,
                    expr: Expr::Literal {
                        node_id: NID,
                        value: LiteralValue::Null,
                        span,
                    },
                    span,
                },
            ],
            span,
        };
        assert_eq!(program.statements.len(), 3);
    }

    #[test]
    fn test_ast_emit_has_name() {
        let span = Span::new(0, 20);
        let stmt = Statement::Emit {
            node_id: NID,
            name: "member_name".into(),
            expr: Expr::FieldRef {
                node_id: NID,
                name: "label".into(),
                span,
            },
            is_meta: false,
            span,
        };
        if let Statement::Emit { name, .. } = stmt {
            assert_eq!(&*name, "member_name");
        } else {
            panic!("expected Emit");
        }
    }

    #[test]
    fn test_ast_trace_with_level_and_guard() {
        let span = Span::new(0, 30);
        let stmt = Statement::Trace {
            node_id: NID,
            level: Some(TraceLevel::Warn),
            guard: Some(Box::new(Expr::Literal {
                node_id: NID,
                value: LiteralValue::Bool(true),
                span,
            })),
            message: Expr::Literal {
                node_id: NID,
                value: LiteralValue::String("alert".into()),
                span,
            },
            span,
        };
        if let Statement::Trace { level, guard, .. } = stmt {
            assert_eq!(level, Some(TraceLevel::Warn));
            assert!(guard.is_some());
        } else {
            panic!("expected Trace");
        }
    }

    #[test]
    fn test_ast_use_stmt_structured() {
        let span = Span::new(0, 25);
        let stmt = Statement::UseStmt {
            node_id: NID,
            path: vec!["shared".into(), "dates".into()],
            alias: Some("d".into()),
            span,
        };
        if let Statement::UseStmt { path, alias, .. } = stmt {
            assert_eq!(path.len(), 2);
            assert_eq!(&*path[0], "shared");
            assert_eq!(alias.as_deref(), Some("d"));
        } else {
            panic!("expected UseStmt");
        }
    }

    #[test]
    fn test_ast_fn_decl_single_expr() {
        let span = Span::new(0, 20);
        let decl = FnDecl {
            node_id: NID,
            name: "double".into(),
            params: vec!["x".into()],
            body: Box::new(Expr::Binary {
                node_id: NID,
                op: BinOp::Mul,
                lhs: Box::new(Expr::FieldRef {
                    node_id: NID,
                    name: "x".into(),
                    span,
                }),
                rhs: Box::new(Expr::Literal {
                    node_id: NID,
                    value: LiteralValue::Int(2),
                    span,
                }),
                span,
            }),
            span,
        };
        assert_eq!(&*decl.name, "double");
        assert_eq!(decl.params.len(), 1);
    }

    #[test]
    fn test_ast_expr_binary_nested() {
        let span = Span::new(0, 1);
        let leaf = || {
            Box::new(Expr::Literal {
                node_id: NID,
                value: LiteralValue::Int(1),
                span,
            })
        };
        let _deep = Expr::Binary {
            node_id: NID,
            op: BinOp::Add,
            lhs: Box::new(Expr::Binary {
                node_id: NID,
                op: BinOp::Mul,
                lhs: leaf(),
                rhs: leaf(),
                span,
            }),
            rhs: leaf(),
            span,
        };
    }

    #[test]
    fn test_ast_expr_size() {
        let size = std::mem::size_of::<Expr>();
        assert!(size <= 72, "Expr is {} bytes, budget is 72", size);
    }

    #[test]
    fn test_ast_all_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Program>();
        assert_send_sync::<Statement>();
        assert_send_sync::<Expr>();
        assert_send_sync::<MatchArm>();
        assert_send_sync::<FnDecl>();
        assert_send_sync::<NodeId>();
    }

    #[test]
    fn test_ast_match_condition_form() {
        let span = Span::new(0, 1);
        let m = Expr::Match {
            node_id: NID,
            subject: None,
            arms: vec![MatchArm {
                node_id: NID,
                pattern: Expr::Literal {
                    node_id: NID,
                    value: LiteralValue::Bool(true),
                    span,
                },
                body: Expr::Literal {
                    node_id: NID,
                    value: LiteralValue::String("yes".into()),
                    span,
                },
                span,
            }],
            span,
        };
        if let Expr::Match { subject, .. } = m {
            assert!(subject.is_none());
        }
    }

    #[test]
    fn test_ast_match_value_form() {
        let span = Span::new(0, 1);
        let m = Expr::Match {
            node_id: NID,
            subject: Some(Box::new(Expr::FieldRef {
                node_id: NID,
                name: "status".into(),
                span,
            })),
            arms: vec![],
            span,
        };
        if let Expr::Match { subject, .. } = m {
            assert!(subject.is_some());
        }
    }

    #[test]
    fn test_ast_debug_output() {
        let span = Span::new(0, 2);
        let expr = Expr::Literal {
            node_id: NID,
            value: LiteralValue::Int(42),
            span,
        };
        let dbg = format!("{:?}", expr);
        assert!(dbg.contains("42"));
    }

    #[test]
    fn test_ast_now_variant() {
        let span = Span::new(0, 3);
        let expr = Expr::Now { node_id: NID, span };
        assert_eq!(expr.span(), span);
        assert_eq!(expr.node_id(), NID);
    }

    #[test]
    fn test_ast_node_id_on_expr() {
        let span = Span::new(0, 1);
        let id = NodeId(42);
        let expr = Expr::Literal {
            node_id: id,
            value: LiteralValue::Int(1),
            span,
        };
        assert_eq!(expr.node_id(), id);
    }
}

#[cfg(test)]
mod support_into_tests {
    use super::*;
    use crate::parser::Parser;
    use std::collections::HashSet;

    /// Parse a single `emit out = <source>` and run `support_into` over the RHS.
    /// Covers every `Expr` variant the parser can construct from concrete syntax.
    fn fields_of(source: &str) -> HashSet<String> {
        let parsed = Parser::parse(&format!("emit out = {source}"));
        assert!(parsed.errors.is_empty(), "{:?}", parsed.errors);
        let stmt = parsed.ast.statements.first().expect("one stmt");
        let Statement::Emit { expr, .. } = stmt else {
            panic!("not emit")
        };
        let mut fields = HashSet::new();
        expr.support_into(&mut fields);
        fields
    }

    #[test]
    fn bare_field_ref() {
        assert_eq!(fields_of("amount"), HashSet::from(["amount".into()]));
    }

    #[test]
    fn binary_two_fields() {
        assert_eq!(fields_of("a + b"), HashSet::from(["a".into(), "b".into()]));
    }

    #[test]
    fn nested_if_coalesce() {
        let f = fields_of("if x > 0 then y ?? z else w");
        assert_eq!(
            f,
            HashSet::from(["x".into(), "y".into(), "z".into(), "w".into()])
        );
    }

    #[test]
    fn window_call_with_field_args() {
        let f = fields_of("$window.sum(amount)");
        assert!(f.contains("amount"));
    }

    #[test]
    fn method_chain_on_window_call() {
        let f = fields_of("$window.first(score).name");
        assert!(f.contains("score"));
    }

    #[test]
    fn qualified_field_ref() {
        let f = fields_of("orders.total");
        assert!(f.contains("orders.total"));
    }

    #[test]
    fn pipeline_namespace_excluded() {
        let f = fields_of("$pipeline.run_id");
        assert!(f.is_empty());
    }

    #[test]
    fn meta_namespace_excluded() {
        let f = fields_of("$meta.source");
        assert!(f.is_empty());
    }

    /// `$ck.*` references appear only as engine-stamped FieldRef names — the
    /// parser rejects `$ck.field` syntax (only `$pipeline`, `$window`, `$meta`
    /// are recognized system namespaces). Construct the FieldRef directly to
    /// exercise the namespace-exclusion path that fires on engine-injected
    /// shadow-column references.
    #[test]
    fn ck_namespace_excluded() {
        let span = Span::new(0, 0);
        let expr = Expr::FieldRef {
            node_id: NodeId(0),
            name: "$ck.employee_id".into(),
            span,
        };
        let mut f = HashSet::new();
        expr.support_into(&mut f);
        assert!(f.is_empty());
    }
}
