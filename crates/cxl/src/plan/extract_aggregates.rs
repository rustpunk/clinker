//! Extraction pass: lift `Expr::AggCall` subtrees out of an aggregate
//! transform's typed program into a flat, deduplicated list of
//! `AggregateBinding`s, leaving behind `Expr::AggSlot` leaves in the
//! emit residuals. Group-by `Expr::FieldRef`s are likewise replaced
//! with `Expr::GroupKey` leaves.
//!
//! Mirrors DataFusion's `find_aggregate_exprs`, DuckDB's
//! `Binder::BindAggregate`, and Spark Catalyst's
//! `ResolveAggregateFunctions`. Runs once at plan-compile time; the
//! executor never re-walks the original composed expressions during
//! `add_record` — only the pre-classified `BindingArg`s in the hot loop
//! and the residuals at finalize time.

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use clinker_record::accumulator::AggregateType;

use super::{AggregateBinding, BindingArg, CompiledAggregate, CompiledEmit};
use crate::ast::{BinOp, Expr, LiteralValue, NodeId, Statement};
use crate::lexer::Span;
use crate::typecheck::{TypeDiagnostic, TypedProgram};

/// Structural dedup key for `AggCall` subtrees. Two calls share a slot
/// iff they have the same function name and structurally equal args
/// (ignoring `NodeId` / `Span`).
type StructuralKey = String;

/// Walk an aggregate transform's typed program and produce a
/// `CompiledAggregate`. See module docs for the algorithm.
pub fn extract_aggregates(
    typed: &TypedProgram,
    group_by_fields: &[String],
    input_schema: &[String],
) -> Result<CompiledAggregate, Vec<TypeDiagnostic>> {
    let mut diagnostics: Vec<TypeDiagnostic> = Vec::new();
    let mut bindings: Vec<AggregateBinding> = Vec::new();
    let mut dedup: HashMap<StructuralKey, u32> = HashMap::new();
    let group_by_set: HashSet<&str> = group_by_fields.iter().map(String::as_str).collect();
    let mut pre_agg_filter: Option<Expr> = None;
    let mut emits: Vec<CompiledEmit> = Vec::new();
    let mut let_bindings: HashMap<Box<str>, Expr> = HashMap::new();

    for stmt in &typed.program.statements {
        match stmt {
            Statement::Let {
                name, expr, span, ..
            } => {
                let mut substituted = expr.clone();
                substitute_let_bindings(&mut substituted, &let_bindings);
                if contains_agg_call(&substituted) {
                    diagnostics.push(diag_let_of_aggregate(name, *span));
                    continue;
                }
                let_bindings.insert(name.clone(), substituted);
            }
            Statement::Filter { predicate, .. } => {
                let mut pred = predicate.clone();
                substitute_let_bindings(&mut pred, &let_bindings);
                if contains_agg_call(&pred) {
                    diagnostics.push(diag_agg_in_filter(pred.span()));
                    continue;
                }
                pre_agg_filter = Some(match pre_agg_filter.take() {
                    None => pred,
                    Some(existing) => and_combine(existing, pred),
                });
            }
            Statement::Distinct { span, .. } => {
                diagnostics.push(diag_distinct_in_aggregate(*span));
            }
            Statement::Emit {
                name,
                expr,
                is_meta,
                ..
            } => {
                let mut residual = expr.clone();
                substitute_let_bindings(&mut residual, &let_bindings);
                if let Err(e) =
                    extract_aggs_from_expr(&mut residual, &mut bindings, &mut dedup, input_schema)
                {
                    diagnostics.push(e);
                    continue;
                }
                rewrite_group_key_refs(&mut residual, &group_by_set, group_by_fields);
                emits.push(CompiledEmit {
                    output_name: name.clone(),
                    residual,
                    is_meta: *is_meta,
                });
            }
            Statement::Trace { .. } | Statement::UseStmt { .. } | Statement::ExprStmt { .. } => {
                // Pass-through: trace is a no-op at the aggregation
                // boundary; UseStmt was already resolved at use-phase;
                // ExprStmt in aggregate mode is meaningless and
                // already rejected upstream.
            }
        }
    }

    let mut group_by_indices = Vec::with_capacity(group_by_fields.len());
    for gb in group_by_fields {
        match input_schema.iter().position(|f| f == gb) {
            Some(idx) => group_by_indices.push(idx as u32),
            None => diagnostics.push(diag_missing_group_by_field(gb)),
        }
    }

    if !diagnostics.is_empty() {
        return Err(diagnostics);
    }

    Ok(CompiledAggregate {
        bindings,
        group_by_indices,
        group_by_fields: group_by_fields.to_vec(),
        pre_agg_filter,
        emits,
        // Both retraction-strategy flags default to `false`; the planner
        // flips at most one of them on via `set_retraction_flags` when
        // the owning aggregate's `group_by` omits a correlation-key
        // field.
        requires_lineage: false,
        requires_buffer_mode: false,
    })
}

// ---------------------------------------------------------------------------
// AggCall extraction
// ---------------------------------------------------------------------------

/// Depth-first rewrite: replace every `AggCall` subtree with an
/// `AggSlot` leaf, recording a binding the first time each structural
/// key is seen.
fn extract_aggs_from_expr(
    expr: &mut Expr,
    bindings: &mut Vec<AggregateBinding>,
    dedup: &mut HashMap<StructuralKey, u32>,
    input_schema: &[String],
) -> Result<(), TypeDiagnostic> {
    // Recurse into children first. Note: we deliberately do NOT recurse
    // into the args of an `AggCall` we are about to replace — its args
    // were already typechecked in row-level scope at 16.2 and may not
    // contain nested AggCalls (rejected upstream).
    match expr {
        Expr::Binary { lhs, rhs, .. } => {
            extract_aggs_from_expr(lhs, bindings, dedup, input_schema)?;
            extract_aggs_from_expr(rhs, bindings, dedup, input_schema)?;
        }
        Expr::Unary { operand, .. } => {
            extract_aggs_from_expr(operand, bindings, dedup, input_schema)?;
        }
        Expr::MethodCall { receiver, args, .. } => {
            extract_aggs_from_expr(receiver, bindings, dedup, input_schema)?;
            for a in args {
                extract_aggs_from_expr(a, bindings, dedup, input_schema)?;
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                extract_aggs_from_expr(s, bindings, dedup, input_schema)?;
            }
            for arm in arms {
                extract_aggs_from_expr(&mut arm.pattern, bindings, dedup, input_schema)?;
                extract_aggs_from_expr(&mut arm.body, bindings, dedup, input_schema)?;
            }
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            extract_aggs_from_expr(condition, bindings, dedup, input_schema)?;
            extract_aggs_from_expr(then_branch, bindings, dedup, input_schema)?;
            if let Some(e) = else_branch {
                extract_aggs_from_expr(e, bindings, dedup, input_schema)?;
            }
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            extract_aggs_from_expr(lhs, bindings, dedup, input_schema)?;
            extract_aggs_from_expr(rhs, bindings, dedup, input_schema)?;
        }
        Expr::WindowCall { args, .. } => {
            for a in args {
                extract_aggs_from_expr(a, bindings, dedup, input_schema)?;
            }
        }
        Expr::AggCall { .. }
        | Expr::Literal { .. }
        | Expr::FieldRef { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::PipelineAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::MetaAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }

    if let Expr::AggCall {
        name,
        args,
        node_id,
        span,
    } = expr
    {
        let key = structural_key(name, args);
        let slot = if let Some(&existing) = dedup.get(&key) {
            existing
        } else {
            let acc_type = classify_acc_type(name, args);
            let arg = classify_binding_arg(name, args, input_schema);
            let slot = bindings.len() as u32;
            bindings.push(AggregateBinding {
                output_name: format!("__agg_{slot}_{name}").into(),
                arg,
                acc_type,
            });
            dedup.insert(key, slot);
            slot
        };
        *expr = Expr::AggSlot {
            node_id: *node_id,
            slot,
            span: *span,
        };
    }
    Ok(())
}

fn classify_acc_type(name: &str, args: &[Expr]) -> AggregateType {
    match name {
        "sum" => AggregateType::Sum,
        "count" => AggregateType::Count {
            count_all: args.is_empty() || matches!(args[0], Expr::Wildcard { .. }),
        },
        "avg" => AggregateType::Avg,
        "min" => AggregateType::Min,
        "max" => AggregateType::Max,
        "collect" => AggregateType::Collect,
        "weighted_avg" => AggregateType::WeightedAvg,
        "any" => AggregateType::Any,
        // typecheck (16.2) rejects unknown agg names before we get here.
        other => unreachable!("unknown aggregate function `{other}` reached extractor"),
    }
}

fn classify_binding_arg(name: &str, args: &[Expr], input_schema: &[String]) -> BindingArg {
    if name == "count" && (args.is_empty() || matches!(args[0], Expr::Wildcard { .. })) {
        return BindingArg::Wildcard;
    }
    if name == "weighted_avg" && args.len() == 2 {
        return BindingArg::Pair(
            Box::new(classify_single_arg(&args[0], input_schema)),
            Box::new(classify_single_arg(&args[1], input_schema)),
        );
    }
    classify_single_arg(&args[0], input_schema)
}

fn classify_single_arg(arg: &Expr, input_schema: &[String]) -> BindingArg {
    match arg {
        Expr::Wildcard { .. } => BindingArg::Wildcard,
        Expr::FieldRef { name, .. } => match input_schema.iter().position(|f| f == name.as_ref()) {
            Some(idx) => BindingArg::Field(idx as u32),
            None => BindingArg::Expr(arg.clone()),
        },
        _ => BindingArg::Expr(arg.clone()),
    }
}

// ---------------------------------------------------------------------------
// Group-by FieldRef → GroupKey rewrite
// ---------------------------------------------------------------------------

fn rewrite_group_key_refs(
    expr: &mut Expr,
    group_by_set: &HashSet<&str>,
    group_by_fields: &[String],
) {
    // Recurse first; AggSlot leaves will be skipped (no children, not a
    // FieldRef). FieldRefs *inside* a binding's argument were captured
    // verbatim during extraction and are NOT rewritten here — only
    // FieldRefs left in the residual are eligible.
    match expr {
        Expr::Binary { lhs, rhs, .. } => {
            rewrite_group_key_refs(lhs, group_by_set, group_by_fields);
            rewrite_group_key_refs(rhs, group_by_set, group_by_fields);
        }
        Expr::Unary { operand, .. } => {
            rewrite_group_key_refs(operand, group_by_set, group_by_fields);
        }
        Expr::MethodCall { receiver, args, .. } => {
            rewrite_group_key_refs(receiver, group_by_set, group_by_fields);
            for a in args {
                rewrite_group_key_refs(a, group_by_set, group_by_fields);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                rewrite_group_key_refs(s, group_by_set, group_by_fields);
            }
            for arm in arms {
                rewrite_group_key_refs(&mut arm.pattern, group_by_set, group_by_fields);
                rewrite_group_key_refs(&mut arm.body, group_by_set, group_by_fields);
            }
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            rewrite_group_key_refs(condition, group_by_set, group_by_fields);
            rewrite_group_key_refs(then_branch, group_by_set, group_by_fields);
            if let Some(e) = else_branch {
                rewrite_group_key_refs(e, group_by_set, group_by_fields);
            }
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            rewrite_group_key_refs(lhs, group_by_set, group_by_fields);
            rewrite_group_key_refs(rhs, group_by_set, group_by_fields);
        }
        Expr::WindowCall { args, .. } => {
            for a in args {
                rewrite_group_key_refs(a, group_by_set, group_by_fields);
            }
        }
        Expr::FieldRef { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::Literal { .. }
        | Expr::PipelineAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::MetaAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggCall { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }

    if let Expr::FieldRef {
        name,
        node_id,
        span,
    } = expr
        && group_by_set.contains(name.as_ref())
    {
        let slot = group_by_fields
            .iter()
            .position(|f| f == name.as_ref())
            .expect("group_by_set membership implies position") as u32;
        *expr = Expr::GroupKey {
            node_id: *node_id,
            slot,
            span: *span,
        };
    }
}

// ---------------------------------------------------------------------------
// Let-binding inlining
// ---------------------------------------------------------------------------

fn substitute_let_bindings(expr: &mut Expr, let_bindings: &HashMap<Box<str>, Expr>) {
    match expr {
        Expr::Binary { lhs, rhs, .. } => {
            substitute_let_bindings(lhs, let_bindings);
            substitute_let_bindings(rhs, let_bindings);
        }
        Expr::Unary { operand, .. } => substitute_let_bindings(operand, let_bindings),
        Expr::MethodCall { receiver, args, .. } => {
            substitute_let_bindings(receiver, let_bindings);
            for a in args {
                substitute_let_bindings(a, let_bindings);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                substitute_let_bindings(s, let_bindings);
            }
            for arm in arms {
                substitute_let_bindings(&mut arm.pattern, let_bindings);
                substitute_let_bindings(&mut arm.body, let_bindings);
            }
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            substitute_let_bindings(condition, let_bindings);
            substitute_let_bindings(then_branch, let_bindings);
            if let Some(e) = else_branch {
                substitute_let_bindings(e, let_bindings);
            }
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            substitute_let_bindings(lhs, let_bindings);
            substitute_let_bindings(rhs, let_bindings);
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                substitute_let_bindings(a, let_bindings);
            }
        }
        Expr::FieldRef { name, .. } => {
            if let Some(replacement) = let_bindings.get(name.as_ref()) {
                *expr = replacement.clone();
            }
        }
        Expr::Literal { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::PipelineAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::MetaAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }
}

// ---------------------------------------------------------------------------
// AggCall presence check
// ---------------------------------------------------------------------------

fn contains_agg_call(expr: &Expr) -> bool {
    match expr {
        Expr::AggCall { .. } => true,
        Expr::Binary { lhs, rhs, .. } | Expr::Coalesce { lhs, rhs, .. } => {
            contains_agg_call(lhs) || contains_agg_call(rhs)
        }
        Expr::Unary { operand, .. } => contains_agg_call(operand),
        Expr::MethodCall { receiver, args, .. } => {
            contains_agg_call(receiver) || args.iter().any(contains_agg_call)
        }
        Expr::Match { subject, arms, .. } => {
            subject.as_deref().map(contains_agg_call).unwrap_or(false)
                || arms
                    .iter()
                    .any(|a| contains_agg_call(&a.pattern) || contains_agg_call(&a.body))
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            contains_agg_call(condition)
                || contains_agg_call(then_branch)
                || else_branch
                    .as_deref()
                    .map(contains_agg_call)
                    .unwrap_or(false)
        }
        Expr::WindowCall { args, .. } => args.iter().any(contains_agg_call),
        Expr::Literal { .. }
        | Expr::FieldRef { .. }
        | Expr::QualifiedFieldRef { .. }
        | Expr::PipelineAccess { .. }
        | Expr::SourceAccess { .. }
        | Expr::MetaAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => false,
    }
}

// ---------------------------------------------------------------------------
// Structural key for AggCall dedup
// ---------------------------------------------------------------------------

fn structural_key(name: &str, args: &[Expr]) -> String {
    let mut buf = String::new();
    buf.push_str(name);
    buf.push('(');
    for (i, a) in args.iter().enumerate() {
        if i > 0 {
            buf.push(',');
        }
        write_struct_form(&mut buf, a);
    }
    buf.push(')');
    buf
}

fn write_struct_form(buf: &mut String, expr: &Expr) {
    match expr {
        Expr::Literal { value, .. } => match value {
            LiteralValue::Int(n) => {
                let _ = write!(buf, "I{n}");
            }
            LiteralValue::Float(f) => {
                let _ = write!(buf, "F{}", f.to_bits());
            }
            LiteralValue::String(s) => {
                let _ = write!(buf, "S{:?}", s);
            }
            LiteralValue::Date(d) => {
                let _ = write!(buf, "D{d}");
            }
            LiteralValue::Bool(b) => {
                let _ = write!(buf, "B{b}");
            }
            LiteralValue::Null => buf.push('N'),
        },
        Expr::FieldRef { name, .. } => {
            let _ = write!(buf, "f:{name}");
        }
        Expr::QualifiedFieldRef { parts, .. } => {
            buf.push_str("qf:");
            for (i, p) in parts.iter().enumerate() {
                if i > 0 {
                    buf.push('.');
                }
                buf.push_str(p);
            }
        }
        Expr::Wildcard { .. } => buf.push('*'),
        Expr::Now { .. } => buf.push_str("now"),
        Expr::PipelineAccess { field, .. } => {
            let _ = write!(buf, "p:{field}");
        }
        Expr::SourceAccess { field, .. } => {
            let _ = write!(buf, "s:{field}");
        }
        Expr::MetaAccess { field, .. } => {
            let _ = write!(buf, "m:{field}");
        }
        Expr::Binary { op, lhs, rhs, .. } => {
            let _ = write!(buf, "({:?} ", op);
            write_struct_form(buf, lhs);
            buf.push(' ');
            write_struct_form(buf, rhs);
            buf.push(')');
        }
        Expr::Unary { op, operand, .. } => {
            let _ = write!(buf, "({:?} ", op);
            write_struct_form(buf, operand);
            buf.push(')');
        }
        Expr::Coalesce { lhs, rhs, .. } => {
            buf.push_str("(?? ");
            write_struct_form(buf, lhs);
            buf.push(' ');
            write_struct_form(buf, rhs);
            buf.push(')');
        }
        Expr::MethodCall {
            receiver,
            method,
            args,
            ..
        } => {
            buf.push('[');
            write_struct_form(buf, receiver);
            let _ = write!(buf, ".{method}(");
            for (i, a) in args.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_struct_form(buf, a);
            }
            buf.push_str(")]");
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            buf.push_str("(if ");
            write_struct_form(buf, condition);
            buf.push(' ');
            write_struct_form(buf, then_branch);
            if let Some(e) = else_branch {
                buf.push(' ');
                write_struct_form(buf, e);
            }
            buf.push(')');
        }
        Expr::Match { subject, arms, .. } => {
            buf.push_str("(match ");
            if let Some(s) = subject {
                write_struct_form(buf, s);
            } else {
                buf.push('_');
            }
            for arm in arms {
                buf.push_str(" |");
                write_struct_form(buf, &arm.pattern);
                buf.push_str("=>");
                write_struct_form(buf, &arm.body);
            }
            buf.push(')');
        }
        Expr::WindowCall { function, args, .. } => {
            let _ = write!(buf, "w:{function}(");
            for (i, a) in args.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_struct_form(buf, a);
            }
            buf.push(')');
        }
        Expr::AggCall { name, args, .. } => {
            // Should not appear under another AggCall (typecheck rejects),
            // but support it for completeness/idempotence.
            let _ = write!(buf, "a:{name}(");
            for (i, a) in args.iter().enumerate() {
                if i > 0 {
                    buf.push(',');
                }
                write_struct_form(buf, a);
            }
            buf.push(')');
        }
        Expr::AggSlot { slot, .. } => {
            let _ = write!(buf, "s:{slot}");
        }
        Expr::GroupKey { slot, .. } => {
            let _ = write!(buf, "k:{slot}");
        }
    }
}

// ---------------------------------------------------------------------------
// AND combinator for chained `filter` statements
// ---------------------------------------------------------------------------

fn and_combine(lhs: Expr, rhs: Expr) -> Expr {
    let span = Span {
        start: lhs.span().start.min(rhs.span().start),
        end: lhs.span().end.max(rhs.span().end),
    };
    Expr::Binary {
        node_id: NodeId(0),
        op: BinOp::And,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
        span,
    }
}

// ---------------------------------------------------------------------------
// Diagnostics
// ---------------------------------------------------------------------------

fn diag_let_of_aggregate(name: &str, span: Span) -> TypeDiagnostic {
    TypeDiagnostic {
        span,
        message: format!(
            "let-binding `{name}` references an aggregate; \
             let in aggregate transforms is restricted to row-pure expressions"
        ),
        help: Some(
            "inline the aggregate directly into the emit expression, \
             or move the let outside the aggregate transform"
                .to_string(),
        ),
        related_span: None,
        is_warning: false,
    }
}

fn diag_agg_in_filter(span: Span) -> TypeDiagnostic {
    TypeDiagnostic {
        span,
        message: "aggregate function call in `filter` is not allowed; \
                  filters in an aggregate transform run pre-aggregation"
            .to_string(),
        help: Some(
            "use a post-aggregation filter on the downstream transform, \
             or restructure the aggregate to materialize the value"
                .to_string(),
        ),
        related_span: None,
        is_warning: false,
    }
}

fn diag_distinct_in_aggregate(span: Span) -> TypeDiagnostic {
    TypeDiagnostic {
        span,
        message: "`distinct` is not permitted inside an aggregate transform".to_string(),
        help: Some("place a separate distinct transform upstream of the aggregate".to_string()),
        related_span: None,
        is_warning: false,
    }
}

fn diag_missing_group_by_field(name: &str) -> TypeDiagnostic {
    TypeDiagnostic {
        span: Span { start: 0, end: 0 },
        message: format!("group_by field `{name}` is not present in the input schema"),
        help: Some(
            "check the upstream transform's output schema and the spelling \
             of the group_by entry in the aggregate config"
                .to_string(),
        ),
        related_span: None,
        is_warning: false,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Span;
    use crate::parser::Parser;
    use crate::resolve::pass::resolve_program;
    use crate::typecheck::pass::{AggregateMode, type_check_with_mode};
    use crate::typecheck::row::Row;
    use crate::typecheck::types::Type;
    use indexmap::IndexMap;
    use std::collections::HashSet;

    fn typed_for_agg(src: &str, schema_fields: &[(&str, Type)], group_by: &[&str]) -> TypedProgram {
        let parsed = Parser::parse(src);
        assert!(
            parsed.errors.is_empty(),
            "parse errors: {:?}",
            parsed.errors
        );
        let field_names: Vec<&str> = schema_fields.iter().map(|(n, _)| *n).collect();
        let resolved =
            resolve_program(parsed.ast, &field_names, parsed.node_count).expect("resolve");
        let cols: IndexMap<crate::typecheck::QualifiedField, Type> = schema_fields
            .iter()
            .map(|(n, t)| (crate::typecheck::QualifiedField::bare(*n), t.clone()))
            .collect();
        let schema = Row::closed(cols, Span::new(0, 0));
        let mode = AggregateMode::GroupBy {
            group_by_fields: group_by
                .iter()
                .map(|s| s.to_string())
                .collect::<HashSet<_>>(),
        };
        type_check_with_mode(resolved, &schema, mode).expect("typecheck")
    }

    fn schema_names(fields: &[(&str, Type)]) -> Vec<String> {
        fields.iter().map(|(n, _)| (*n).to_string()).collect()
    }

    #[test]
    fn test_extract_flat_emit() {
        let fields = &[("salary", Type::Int)];
        let typed = typed_for_agg("emit total = sum(salary)", fields, &[]);
        let compiled = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap();
        assert_eq!(compiled.bindings.len(), 1);
        assert!(matches!(compiled.bindings[0].arg, BindingArg::Field(0)));
        assert!(matches!(
            compiled.emits[0].residual,
            Expr::AggSlot { slot: 0, .. }
        ));
    }

    #[test]
    fn test_extract_composed_emit() {
        let fields = &[("a", Type::Int)];
        let typed = typed_for_agg("emit pct = sum(a) / count(*) * 100", fields, &[]);
        let compiled = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap();
        assert_eq!(compiled.bindings.len(), 2);
        assert!(matches!(compiled.bindings[0].arg, BindingArg::Field(0)));
        assert!(matches!(compiled.bindings[1].arg, BindingArg::Wildcard));
        // Residual should still be a Binary tree referencing AggSlot leaves.
        let residual = &compiled.emits[0].residual;
        assert!(matches!(residual, Expr::Binary { .. }));
    }

    #[test]
    fn test_extract_dedup_identical_aggcalls() {
        let fields = &[("x", Type::Int)];
        let typed = typed_for_agg("emit a = sum(x)\nemit b = sum(x)", fields, &[]);
        let compiled = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap();
        assert_eq!(compiled.bindings.len(), 1);
        assert_eq!(compiled.emits.len(), 2);
        assert!(matches!(
            compiled.emits[0].residual,
            Expr::AggSlot { slot: 0, .. }
        ));
        assert!(matches!(
            compiled.emits[1].residual,
            Expr::AggSlot { slot: 0, .. }
        ));
    }

    #[test]
    fn test_extract_group_key_leaf_substitution() {
        let fields = &[("dept", Type::String), ("salary", Type::Int)];
        let typed = typed_for_agg(
            "emit dept = dept\nemit total = sum(salary)",
            fields,
            &["dept"],
        );
        let compiled =
            extract_aggregates(&typed, &["dept".to_string()], &schema_names(fields)).unwrap();
        assert_eq!(compiled.group_by_indices, vec![0]);
        assert!(matches!(
            compiled.emits[0].residual,
            Expr::GroupKey { slot: 0, .. }
        ));
    }

    #[test]
    fn test_extract_non_trivial_agg_arg() {
        let fields = &[("amount", Type::Float)];
        let typed = typed_for_agg("emit t = sum(amount * 1.1)", fields, &[]);
        let compiled = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap();
        assert_eq!(compiled.bindings.len(), 1);
        assert!(matches!(compiled.bindings[0].arg, BindingArg::Expr(_)));
    }

    #[test]
    fn test_extract_count_wildcard() {
        let fields: &[(&str, Type)] = &[];
        let typed = typed_for_agg("emit n = count(*)", fields, &[]);
        let compiled = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap();
        assert_eq!(compiled.bindings.len(), 1);
        assert!(matches!(compiled.bindings[0].arg, BindingArg::Wildcard));
        assert!(matches!(
            compiled.bindings[0].acc_type,
            AggregateType::Count { count_all: true }
        ));
    }

    #[test]
    fn test_extract_weighted_avg_pair() {
        let fields = &[("v", Type::Float), ("w", Type::Float)];
        let typed = typed_for_agg("emit wa = weighted_avg(v, w)", fields, &[]);
        let compiled = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap();
        assert_eq!(compiled.bindings.len(), 1);
        match &compiled.bindings[0].arg {
            BindingArg::Pair(a, b) => {
                assert!(matches!(**a, BindingArg::Field(0)));
                assert!(matches!(**b, BindingArg::Field(1)));
            }
            other => panic!("expected Pair, got {other:?}"),
        }
    }

    #[test]
    fn test_extract_rejects_distinct_in_group_by() {
        let fields = &[("x", Type::Int)];
        let typed = typed_for_agg("distinct\nemit n = count(*)", fields, &[]);
        let err = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap_err();
        assert!(err.iter().any(|d| d.message.contains("distinct")));
    }

    #[test]
    fn test_extract_rejects_let_of_aggregate() {
        let fields = &[("a", Type::Int)];
        let typed = typed_for_agg("let x = sum(a)\nemit y = x + 1", fields, &[]);
        let err = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap_err();
        assert!(err.iter().any(|d| d.message.contains("aggregate")));
    }

    #[test]
    fn test_extract_let_row_pure_inlined() {
        let fields = &[("salary", Type::Int)];
        let typed = typed_for_agg("let bonus = salary * 2\nemit t = sum(bonus)", fields, &[]);
        let compiled = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap();
        assert_eq!(compiled.bindings.len(), 1);
        // Inlined: sum(salary * 2) is a composed arg, not a bare field.
        assert!(matches!(compiled.bindings[0].arg, BindingArg::Expr(_)));
    }

    #[test]
    fn test_pre_agg_filter_applied() {
        let fields = &[("active", Type::Bool)];
        let typed = typed_for_agg("filter active == true\nemit c = count(*)", fields, &[]);
        let compiled = extract_aggregates(&typed, &[], &schema_names(fields)).unwrap();
        assert!(compiled.pre_agg_filter.is_some());
    }

    #[test]
    fn test_extract_missing_group_by_field() {
        let fields = &[("x", Type::Int)];
        let typed = typed_for_agg("emit n = count(*)", fields, &[]);
        let err =
            extract_aggregates(&typed, &["nope".to_string()], &schema_names(fields)).unwrap_err();
        assert!(err.iter().any(|d| d.message.contains("nope")));
    }

    // ----- retraction-strategy truth table -----
    //
    // The two flags are exact complements under relaxed-CK. Lineage is the
    // Reversible-path optimization for retraction: it lets an O(1) sub()
    // on Sum/Count/Collect/Any target only the rows a downstream rollback
    // names. Buffer-mode replays contributions from a separate per-group
    // buffer for BufferRequired bindings (Min/Max/Avg/WeightedAvg) where
    // an inverse op would either drift (Avg, WeightedAvg) or be impossible
    // without the surviving multiset (Min, Max). A single BufferRequired
    // binding flips the whole aggregate to buffer-mode — splitting
    // strategies per slot would defeat the point.

    #[test]
    fn test_retraction_flags_relaxed_with_all_reversible() {
        let fields = &[("dept", Type::String), ("salary", Type::Int)];
        let typed = typed_for_agg(
            "emit total = sum(salary)\nemit n = count(*)",
            fields,
            &["dept"],
        );
        let mut compiled =
            extract_aggregates(&typed, &["dept".to_string()], &schema_names(fields)).unwrap();
        // Default after extraction is always `false` for both.
        assert!(!compiled.requires_lineage);
        assert!(!compiled.requires_buffer_mode);
        compiled.set_retraction_flags(true);
        assert!(
            compiled.requires_lineage,
            "all-Reversible bindings under retraction mode must enable lineage"
        );
        assert!(
            !compiled.requires_buffer_mode,
            "all-Reversible bindings must not select buffer-mode"
        );
    }

    #[test]
    fn test_retraction_flags_relaxed_with_any_buffer_required() {
        let fields = &[("dept", Type::String), ("salary", Type::Int)];
        // `min(salary)` is BufferRequired — its presence forces the
        // whole aggregate off the lineage path and onto buffer-mode
        // even though `sum` is Reversible.
        let typed = typed_for_agg(
            "emit total = sum(salary)\nemit lo = min(salary)",
            fields,
            &["dept"],
        );
        let mut compiled =
            extract_aggregates(&typed, &["dept".to_string()], &schema_names(fields)).unwrap();
        compiled.set_retraction_flags(true);
        assert!(
            !compiled.requires_lineage,
            "a single BufferRequired binding short-circuits requires_lineage to false"
        );
        assert!(
            compiled.requires_buffer_mode,
            "a single BufferRequired binding must select buffer-mode"
        );
    }

    #[test]
    fn test_retraction_flags_relaxed_with_only_buffer_required() {
        // Pure BufferRequired bindings — exercise the path with no
        // Reversible accumulator at all.
        let fields = &[("dept", Type::String), ("salary", Type::Int)];
        let typed = typed_for_agg(
            "emit lo = min(salary)\nemit hi = max(salary)\nemit mean = avg(salary)",
            fields,
            &["dept"],
        );
        let mut compiled =
            extract_aggregates(&typed, &["dept".to_string()], &schema_names(fields)).unwrap();
        compiled.set_retraction_flags(true);
        assert!(!compiled.requires_lineage);
        assert!(compiled.requires_buffer_mode);
    }

    #[test]
    fn test_retraction_flags_strict_always_false() {
        let fields = &[("dept", Type::String), ("salary", Type::Int)];
        let typed = typed_for_agg("emit total = sum(salary)", fields, &["dept"]);
        let mut compiled =
            extract_aggregates(&typed, &["dept".to_string()], &schema_names(fields)).unwrap();
        // Strict mode (`is_relaxed = false`) suppresses both flags
        // regardless of binding shape.
        compiled.set_retraction_flags(false);
        assert!(!compiled.requires_lineage);
        assert!(!compiled.requires_buffer_mode);

        // And the same call with a BufferRequired binding mix still
        // resolves to false — strict short-circuits before the binding
        // walk matters.
        let typed2 = typed_for_agg(
            "emit total = sum(salary)\nemit lo = min(salary)",
            fields,
            &["dept"],
        );
        let mut compiled2 =
            extract_aggregates(&typed2, &["dept".to_string()], &schema_names(fields)).unwrap();
        compiled2.set_retraction_flags(false);
        assert!(!compiled2.requires_lineage);
        assert!(!compiled2.requires_buffer_mode);
    }
}
