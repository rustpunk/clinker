//! Shared typed-AST traversal for analyzer passes.
//!
//! A single set of `walk_*` functions enumerate every recursive edge in
//! the [`Statement`] / [`Expr`] / [`MatchArm`] grammar exactly once. Each
//! analyzer pass implements [`Visitor`], overriding only the nodes it
//! inspects and delegating every other node to the shared walk via the
//! default method bodies.
//!
//! The point of centralizing the recursion shape: a new expression or
//! statement variant — or a change to which children an existing variant
//! holds — is handled in one place here, so a pass cannot silently drop a
//! sub-expression it never knew to descend into. Without this, every pass
//! carried its own exhaustive match and a forgotten arm in one of them
//! would quietly miss whatever lives beneath the new variant.
//!
//! The traversal mirrors syn's `Visit`: a [`Visitor`] method receives a
//! node and is responsible for descending into that node's children by
//! calling the matching free `walk_*` function (the default method body
//! does exactly this). A pass that wants to *replace* a node's default
//! descent — to treat a sub-tree as an indivisible unit, or to thread
//! extra state through the children — overrides the method and chooses
//! whether to call `walk_*` at all.

use crate::ast::{Expr, MatchArm, Statement};

/// A typed-AST traversal hook.
///
/// Default method bodies perform a full pre-order descent over the AST.
/// Override `visit_expr` / `visit_statement` / `visit_match_arm` to act on
/// specific nodes; call the matching `walk_*` free function from an
/// override to continue the default descent into that node's children, or
/// omit the call to prune the sub-tree.
pub trait Visitor {
    /// Visit one statement. The default descends into every expression and
    /// nested-statement body the statement holds.
    fn visit_statement(&mut self, stmt: &Statement) {
        walk_statement(self, stmt);
    }

    /// Visit one expression. The default descends into every
    /// sub-expression the variant holds.
    fn visit_expr(&mut self, expr: &Expr) {
        walk_expr(self, expr);
    }

    /// Visit one match arm. The default descends into the arm's pattern
    /// and body expressions.
    fn visit_match_arm(&mut self, arm: &MatchArm) {
        walk_match_arm(self, arm);
    }
}

/// Descend into a statement's children, dispatching each through the
/// visitor. Covers every [`Statement`] variant; variants with no
/// sub-expressions (`UseStmt`, `Distinct`) are terminal.
pub fn walk_statement<V: Visitor + ?Sized>(visitor: &mut V, stmt: &Statement) {
    match stmt {
        Statement::Let { expr, .. }
        | Statement::Emit { expr, .. }
        | Statement::ExprStmt { expr, .. } => visitor.visit_expr(expr),
        Statement::Filter { predicate, .. } => visitor.visit_expr(predicate),
        Statement::Trace { guard, message, .. } => {
            if let Some(g) = guard {
                visitor.visit_expr(g);
            }
            visitor.visit_expr(message);
        }
        Statement::EmitEach { source, body, .. } | Statement::ExplodeOuter { source, body, .. } => {
            visitor.visit_expr(source);
            for inner in body {
                visitor.visit_statement(inner);
            }
        }
        Statement::UseStmt { .. } | Statement::Distinct { .. } => {}
    }
}

/// Descend into an expression's children, dispatching each through the
/// visitor. Covers every [`Expr`] variant; leaf variants (literals,
/// namespace accesses, `now`, wildcards, aggregate/group-key slots) hold
/// no sub-expressions and are terminal.
pub fn walk_expr<V: Visitor + ?Sized>(visitor: &mut V, expr: &Expr) {
    match expr {
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            visitor.visit_expr(lhs);
            visitor.visit_expr(rhs);
        }
        Expr::Unary { operand, .. } => visitor.visit_expr(operand),
        Expr::MethodCall { receiver, args, .. } => {
            visitor.visit_expr(receiver);
            for arg in args {
                visitor.visit_expr(arg);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                visitor.visit_expr(s);
            }
            for arm in arms {
                visitor.visit_match_arm(arm);
            }
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            visitor.visit_expr(condition);
            visitor.visit_expr(then_branch);
            if let Some(e) = else_branch {
                visitor.visit_expr(e);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for arg in args {
                visitor.visit_expr(arg);
            }
        }
        Expr::IndexAccess {
            receiver, index, ..
        } => {
            visitor.visit_expr(receiver);
            visitor.visit_expr(index);
        }
        Expr::Closure { body, .. } => visitor.visit_expr(body),
        Expr::Literal { .. }
        | Expr::FieldRef { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::PipelineAccess { .. }
        | Expr::VarsAccess { .. }
        | Expr::ConfigAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::QualifiedSourceAccess { .. }
        | Expr::RecordAccess { .. }
        | Expr::DocAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }
}

/// Descend into a match arm's pattern and body expressions.
pub fn walk_match_arm<V: Visitor + ?Sized>(visitor: &mut V, arm: &MatchArm) {
    visitor.visit_expr(&arm.pattern);
    visitor.visit_expr(&arm.body);
}
