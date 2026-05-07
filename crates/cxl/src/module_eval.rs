use std::collections::HashMap;

use crate::ast::{Expr, FnDecl, ModuleConst};
use crate::lexer::Span;

/// Error from module constant evaluation.
#[derive(Debug)]
pub struct ModuleConstError {
    pub span: Span,
    pub message: String,
}

/// Topologically sort module constants by their dependencies (Kahn's algorithm).
/// Returns constants in evaluation order, or an error if there is a cycle,
/// a duplicate name, or a field reference in a constant expression.
pub fn toposort_constants(constants: &[ModuleConst]) -> Result<Vec<usize>, ModuleConstError> {
    let n = constants.len();
    if n == 0 {
        return Ok(vec![]);
    }

    // Build name → index map, checking for duplicates
    let mut name_to_idx: HashMap<&str, usize> = HashMap::with_capacity(n);
    for (i, c) in constants.iter().enumerate() {
        if let Some(prev) = name_to_idx.insert(&c.name, i) {
            let _ = prev;
            return Err(ModuleConstError {
                span: c.span,
                message: format!("duplicate constant name '{}'", c.name),
            });
        }
    }

    // Build adjacency: edges[i] = set of indices that i depends on
    let mut in_degree = vec![0usize; n];
    let mut dependents: Vec<Vec<usize>> = vec![vec![]; n]; // dependents[dep] = list of nodes that depend on dep

    for (i, c) in constants.iter().enumerate() {
        let deps = collect_ident_refs(&c.expr);
        for dep_name in &deps {
            if let Some(&dep_idx) = name_to_idx.get(dep_name.as_str()) {
                // i depends on dep_idx
                dependents[dep_idx].push(i);
                in_degree[i] += 1;
            } else if !is_known_builtin(dep_name) {
                // Not a constant and not a builtin — it's a field reference
                return Err(ModuleConstError {
                    span: c.span,
                    message: format!(
                        "module constants cannot reference fields (found '{}' in constant '{}')",
                        dep_name, c.name
                    ),
                });
            }
        }
    }

    // Kahn's algorithm
    let mut queue: Vec<usize> = Vec::new();
    for (i, &deg) in in_degree.iter().enumerate().take(n) {
        if deg == 0 {
            queue.push(i);
        }
    }

    let mut order = Vec::with_capacity(n);
    while let Some(node) = queue.pop() {
        order.push(node);
        for &dep in &dependents[node] {
            in_degree[dep] -= 1;
            if in_degree[dep] == 0 {
                queue.push(dep);
            }
        }
    }

    if order.len() != n {
        // Find a node still in the cycle for the error message
        let cycle_node = in_degree.iter().position(|&d| d > 0).unwrap();
        return Err(ModuleConstError {
            span: constants[cycle_node].span,
            message: format!(
                "cyclic dependency detected involving constant '{}'",
                constants[cycle_node].name
            ),
        });
    }

    Ok(order)
}

/// Collect all identifier references from an expression (names that could be
/// other constants or field references).
fn collect_ident_refs(expr: &Expr) -> Vec<String> {
    let mut refs = Vec::new();
    walk_expr(expr, &mut refs);
    refs
}

fn walk_expr(expr: &Expr, refs: &mut Vec<String>) {
    match expr {
        Expr::FieldRef { name, .. } => {
            refs.push(name.to_string());
        }
        Expr::Binary { lhs, rhs, .. } => {
            walk_expr(lhs, refs);
            walk_expr(rhs, refs);
        }
        Expr::Unary { operand, .. } => {
            walk_expr(operand, refs);
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            walk_expr(lhs, refs);
            walk_expr(rhs, refs);
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            walk_expr(condition, refs);
            walk_expr(then_branch, refs);
            if let Some(eb) = else_branch {
                walk_expr(eb, refs);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                walk_expr(s, refs);
            }
            for arm in arms {
                walk_expr(&arm.pattern, refs);
                walk_expr(&arm.body, refs);
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            walk_expr(receiver, refs);
            for arg in args {
                walk_expr(arg, refs);
            }
        }
        Expr::Literal { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
        Expr::QualifiedFieldRef { .. } => {
            // Qualified refs like module.CONST are handled separately
        }
        Expr::WindowCall { args, .. } => {
            for arg in args {
                walk_expr(arg, refs);
            }
        }
        Expr::PipelineAccess { .. } | Expr::SourceAccess { .. } | Expr::MetaAccess { .. } => {
            // pipeline.*/source.*/meta.* not allowed in module constants — but
            // we don't reject here; the evaluator will catch it at runtime.
        }
        Expr::AggCall { args, .. } => {
            for arg in args {
                walk_expr(arg, refs);
            }
        }
    }
}

/// Check if a name is a known builtin (literal keywords that parse as FieldRef).
/// These are NOT constant references and should be ignored during dependency analysis.
fn is_known_builtin(_name: &str) -> bool {
    // Keywords like true/false/null/now/it parse as their own tokens,
    // not FieldRef. So any FieldRef that isn't a constant name is a
    // field reference — which is invalid in module constants.
    false
}

/// Phase C validation: check that no module function calls itself recursively.
/// Since modules have single-expression bodies and no cross-module imports,
/// only direct self-recursion is possible.
pub fn check_recursive_calls(functions: &[FnDecl]) -> Result<(), ModuleConstError> {
    for f in functions {
        if contains_self_call(&f.name, &f.body) {
            return Err(ModuleConstError {
                span: f.span,
                message: format!(
                    "recursive calls are not supported: function '{}' calls itself",
                    f.name
                ),
            });
        }
    }
    Ok(())
}

/// Walk an expression tree looking for a MethodCall where the receiver is a
/// FieldRef matching `fn_name`, or a bare function-call pattern.
/// Since module functions are called as `module.fn(args)`, direct self-recursion
/// appears as FieldRef(fn_name) used as a function name in the body.
fn contains_self_call(fn_name: &str, expr: &Expr) -> bool {
    match expr {
        // A bare identifier matching the function name followed by parens
        // would appear as MethodCall with the fn name. But in CXL, module
        // functions are called qualified (module.fn). Inside the fn body,
        // a self-call would be just `fn_name(args)` which parses as
        // FieldRef + method call, or as an unresolved FieldRef.
        // We check for FieldRef matching fn_name used in a call-like position.
        Expr::MethodCall { receiver, args, .. } => {
            // Check: receiver is FieldRef matching fn_name (e.g., f.something)
            if let Expr::FieldRef { name, .. } = &**receiver
                && &**name == fn_name
            {
                return true;
            }
            // Check: method name matching fn_name on any receiver won't happen
            // for self-recursion since it would need module prefix.
            // But check args recursively
            contains_self_call(fn_name, receiver)
                || args.iter().any(|a| contains_self_call(fn_name, a))
        }
        // Direct self-call: the parser would see `f(args)` as a method call
        // on the previous expression, but if `f` is at the start of an expression
        // context and followed by `(`, it's parsed as FieldRef then method call.
        // Actually, `f(x)` in CXL parses as: FieldRef("f") then dot handling
        // doesn't apply (no dot), so it would be FieldRef("f") followed by
        // a parse error for the `(`. The parser doesn't have free function calls.
        //
        // BUT: `f(x - 1)` would parse differently depending on context.
        // In a module fn body (single expression), `f(x)` is NOT valid CXL
        // syntax since CXL has no free function calls — only method calls.
        // So we just need to catch the case where someone writes the fn name
        // as an identifier in a context that could be a recursive reference.
        Expr::Binary { lhs, rhs, .. } => {
            contains_self_call(fn_name, lhs) || contains_self_call(fn_name, rhs)
        }
        Expr::Unary { operand, .. } => contains_self_call(fn_name, operand),
        Expr::Coalesce { lhs, rhs, .. } => {
            contains_self_call(fn_name, lhs) || contains_self_call(fn_name, rhs)
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            contains_self_call(fn_name, condition)
                || contains_self_call(fn_name, then_branch)
                || else_branch
                    .as_ref()
                    .is_some_and(|eb| contains_self_call(fn_name, eb))
        }
        Expr::Match { subject, arms, .. } => {
            subject
                .as_ref()
                .is_some_and(|s| contains_self_call(fn_name, s))
                || arms.iter().any(|arm| {
                    contains_self_call(fn_name, &arm.pattern)
                        || contains_self_call(fn_name, &arm.body)
                })
        }
        Expr::WindowCall { args, .. } => args.iter().any(|a| contains_self_call(fn_name, a)),
        Expr::AggCall { args, .. } => args.iter().any(|a| contains_self_call(fn_name, a)),
        Expr::FieldRef { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::Literal { .. }
        | Expr::PipelineAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::MetaAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{BinOp, LiteralValue, NodeId};
    use crate::lexer::Span;

    fn make_const(name: &str, expr: Expr) -> ModuleConst {
        ModuleConst {
            node_id: NodeId(0),
            name: name.into(),
            expr,
            span: Span::new(0, 1),
        }
    }

    fn lit_int(v: i64) -> Expr {
        Expr::Literal {
            node_id: NodeId(0),
            value: LiteralValue::Int(v),
            span: Span::new(0, 1),
        }
    }

    fn field_ref(name: &str) -> Expr {
        Expr::FieldRef {
            node_id: NodeId(0),
            name: name.into(),
            span: Span::new(0, 1),
        }
    }

    fn add(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            node_id: NodeId(0),
            op: BinOp::Add,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            span: Span::new(0, 1),
        }
    }

    #[test]
    fn test_module_const_forward_reference() {
        // let B = A + 1; let A = 5 → order: A first, then B
        let constants = vec![
            make_const("B", add(field_ref("A"), lit_int(1))),
            make_const("A", lit_int(5)),
        ];
        let order = toposort_constants(&constants).unwrap();
        // A (index 1) must come before B (index 0)
        let a_pos = order.iter().position(|&i| i == 1).unwrap();
        let b_pos = order.iter().position(|&i| i == 0).unwrap();
        assert!(a_pos < b_pos, "A must be evaluated before B");
    }

    #[test]
    fn test_module_const_cycle_detection() {
        // let A = B + 1; let B = A + 1 → cycle
        let constants = vec![
            make_const("A", add(field_ref("B"), lit_int(1))),
            make_const("B", add(field_ref("A"), lit_int(1))),
        ];
        let err = toposort_constants(&constants).unwrap_err();
        assert!(err.message.contains("cyclic dependency"));
    }

    #[test]
    fn test_module_const_duplicate_name() {
        let constants = vec![make_const("X", lit_int(1)), make_const("X", lit_int(2))];
        let err = toposort_constants(&constants).unwrap_err();
        assert!(err.message.contains("duplicate constant name 'X'"));
    }

    #[test]
    fn test_module_const_reject_field_reference() {
        // let BAD = Amount → "module constants cannot reference fields"
        let constants = vec![make_const("BAD", field_ref("Amount"))];
        let err = toposort_constants(&constants).unwrap_err();
        assert!(
            err.message
                .contains("module constants cannot reference fields")
        );
        assert!(err.message.contains("Amount"));
    }

    #[test]
    fn test_module_const_no_deps() {
        // Independent constants — any order is valid
        let constants = vec![
            make_const("A", lit_int(1)),
            make_const("B", lit_int(2)),
            make_const("C", lit_int(3)),
        ];
        let order = toposort_constants(&constants).unwrap();
        assert_eq!(order.len(), 3);
    }

    #[test]
    fn test_module_const_empty() {
        let order = toposort_constants(&[]).unwrap();
        assert!(order.is_empty());
    }

    #[test]
    fn test_module_const_chain() {
        // A = 1, B = A + 1, C = B + 1 → must be A, B, C
        let constants = vec![
            make_const("C", add(field_ref("B"), lit_int(1))),
            make_const("B", add(field_ref("A"), lit_int(1))),
            make_const("A", lit_int(1)),
        ];
        let order = toposort_constants(&constants).unwrap();
        let a_pos = order.iter().position(|&i| i == 2).unwrap();
        let b_pos = order.iter().position(|&i| i == 1).unwrap();
        let c_pos = order.iter().position(|&i| i == 0).unwrap();
        assert!(a_pos < b_pos, "A before B");
        assert!(b_pos < c_pos, "B before C");
    }

    // ── Recursive call detection tests ────────────────────────────

    fn make_fn(name: &str, params: &[&str], body: Expr) -> FnDecl {
        FnDecl {
            node_id: NodeId(0),
            name: name.into(),
            params: params.iter().map(|p| (*p).into()).collect(),
            body: Box::new(body),
            span: Span::new(0, 1),
        }
    }

    fn method_call(receiver: Expr, method: &str, args: Vec<Expr>) -> Expr {
        Expr::MethodCall {
            node_id: NodeId(0),
            receiver: Box::new(receiver),
            method: method.into(),
            args,
            span: Span::new(0, 1),
        }
    }

    fn if_then_else(cond: Expr, then_br: Expr, else_br: Expr) -> Expr {
        Expr::IfThenElse {
            node_id: NodeId(0),
            condition: Box::new(cond),
            then_branch: Box::new(then_br),
            else_branch: Some(Box::new(else_br)),
            span: Span::new(0, 1),
        }
    }

    fn gt(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            node_id: NodeId(0),
            op: BinOp::Gt,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            span: Span::new(0, 1),
        }
    }

    fn sub(lhs: Expr, rhs: Expr) -> Expr {
        Expr::Binary {
            node_id: NodeId(0),
            op: crate::ast::BinOp::Sub,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
            span: Span::new(0, 1),
        }
    }

    #[test]
    fn test_module_fn_recursive_call_rejected() {
        // fn f(x) = if x > 0 then f.something(x - 1) else x
        // This simulates self-reference: receiver FieldRef("f") with method call
        let body = if_then_else(
            gt(field_ref("x"), lit_int(0)),
            method_call(
                field_ref("f"),
                "call",
                vec![sub(field_ref("x"), lit_int(1))],
            ),
            field_ref("x"),
        );
        let functions = vec![make_fn("f", &["x"], body)];
        let err = check_recursive_calls(&functions).unwrap_err();
        assert!(err.message.contains("recursive calls are not supported"));
        assert!(err.message.contains("'f'"));
    }

    #[test]
    fn test_module_fn_no_recursion_ok() {
        // fn double(x) = x * 2 — no recursion
        let body = Expr::Binary {
            node_id: NodeId(0),
            op: BinOp::Mul,
            lhs: Box::new(field_ref("x")),
            rhs: Box::new(lit_int(2)),
            span: Span::new(0, 1),
        };
        let functions = vec![make_fn("double", &["x"], body)];
        assert!(check_recursive_calls(&functions).is_ok());
    }

    #[test]
    fn test_module_fn_different_name_method_ok() {
        // fn clean(val) = val.trim() — method name != fn name, no recursion
        let body = method_call(field_ref("val"), "trim", vec![]);
        let functions = vec![make_fn("clean", &["val"], body)];
        assert!(check_recursive_calls(&functions).is_ok());
    }
}
