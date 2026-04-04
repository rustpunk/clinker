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
            | Expr::Wildcard { span, .. } => *span,
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
            | Expr::Wildcard { node_id, .. } => *node_id,
        }
    }
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
