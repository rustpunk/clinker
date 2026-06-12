//! Compile-time `$doc` path analysis.
//!
//! Walks the typed AST of every CXL-bearing node and collects the
//! `$doc.<section>.<field>` envelope paths the pipeline's programs
//! actually reference, including trailing index accesses
//! (`$doc.section.items[0]`), attributing each path to the node(s) that
//! reference it. Document readers consult this set to learn, before
//! reading any input, which declared envelope paths a run will consume —
//! a declared section the programs never read need not be extracted.
//!
//! Attribution stops at the referencing node because a `$doc` access
//! carries no source qualifier; the planner traces each node back through
//! the DAG to its source(s) and stamps only those. A multi-source run
//! must not tell every source to extract the pipeline-wide union, or a
//! source would attempt to extract sections its own document never
//! declares.
//!
//! A `$doc` access is statically resolvable iff its section, field, and
//! every trailing index are literals. The CXL grammar guarantees the
//! section and field are always literal identifiers (the parser rejects a
//! one-level `$doc.foo`), so the only dynamic axis is an index expression
//! computed from runtime data (`$doc.section.items[some_field]`). Such an
//! access is reported as an unresolvable path with a fail-fast diagnostic
//! carrying the offending index span; the declared-path set cannot name a
//! row-dependent element.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::analyzer::visitor::{Visitor, walk_expr};
use crate::ast::{Expr, LiteralValue, UnaryOp};
use crate::lexer::Span;
use crate::typecheck::pass::{TypeDiagnostic, TypedProgram};

/// One trailing index segment on a `$doc` path.
///
/// `$doc.section.items[0]["k"]` carries `[Int(0), Key("k")]`. Only
/// literal indices are representable — a computed index makes the whole
/// access unresolvable and is reported as a diagnostic instead.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DocIndex {
    /// Integer array index, e.g. `items[0]`.
    Int(i64),
    /// String map key, e.g. `meta["run_date"]`.
    Key(Box<str>),
}

/// A statically-resolved `$doc` envelope path referenced by some program.
///
/// `section` and `field` are the two literal levels every `$doc` access
/// carries; `indices` holds any trailing literal index segments in order.
/// Ordered by `(section, field, indices)` so the collected set has a
/// deterministic iteration order independent of program walk order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct DocPath {
    /// Envelope section name (`$doc.<section>.…`).
    pub section: Box<str>,
    /// Field within the section (`$doc.section.<field>`).
    pub field: Box<str>,
    /// Trailing literal index segments, outermost first.
    pub indices: Vec<DocIndex>,
}

/// The result of [`collect_doc_paths`]: every statically-resolved `$doc`
/// path the programs reference, attributed to the node(s) that reference
/// it, plus a fail-fast diagnostic for each `$doc` access that could not
/// be statically resolved.
///
/// A `$doc` access carries no source qualifier, so attribution stops at
/// the referencing node: the planner traces each node back through the
/// DAG to the source(s) feeding it and stamps only those sources with the
/// path. Stamping every source with the pipeline-wide union would tell a
/// multi-source run to extract envelope sections a given source's own
/// document never declares.
#[derive(Debug, Clone, Default)]
pub struct DocPathSet {
    /// Each statically-resolved path mapped to the set of node names whose
    /// program references it. The outer `BTreeMap` keys (paths) and inner
    /// `BTreeSet` values (node names) are both ordered, so iteration is
    /// deterministic independent of program walk order.
    pub by_node: BTreeMap<DocPath, BTreeSet<String>>,
    /// One entry per `$doc` access whose index is not a literal. The
    /// `String` is the name of the program's node so the caller can anchor
    /// a plan-level diagnostic at that node; the [`TypeDiagnostic`] carries
    /// the precise offending-index span and help.
    pub unresolvable: Vec<(String, TypeDiagnostic)>,
}

impl DocPathSet {
    /// The deduplicated, sorted set of every resolved path across all
    /// nodes, discarding per-node attribution. Useful for callers that
    /// only need the pipeline-wide path list (diagnostics, display).
    pub fn all_paths(&self) -> Vec<DocPath> {
        self.by_node.keys().cloned().collect()
    }
}

/// Collect the `$doc` path set referenced across a set of typed programs,
/// attributing each resolved path to the node(s) that reference it.
///
/// Each tuple is `(node_name, typed_program)`. A path referenced by
/// multiple nodes is attributed to each of them, so the planner can union
/// the source sets of every referencing node when stamping a source.
/// Unresolvable-index diagnostics are accumulated in walk order.
pub fn collect_doc_paths(programs: &[(&str, &TypedProgram)]) -> DocPathSet {
    let mut collector = DocPathCollector::default();
    for (name, typed) in programs {
        collector.node_name = name;
        for stmt in &typed.program.statements {
            collector.visit_statement(stmt);
        }
    }
    DocPathSet {
        by_node: collector.by_node,
        unresolvable: collector.unresolvable,
    }
}

/// Collects the `$doc` envelope paths a single program references, driving
/// the shared typed-AST [`Visitor`] walk.
///
/// Only `DocAccess` and `IndexAccess` carry `$doc` meaning, so those are
/// the sole overrides — a `DocAccess` records a plain two-level path, and
/// an `IndexAccess` whose receiver chain bottoms out at a `DocAccess`
/// records a path with trailing index segments (or a diagnostic for a
/// non-literal index). Every other node, including index chains that do
/// not root at a `$doc` access, falls through to the default descent, so a
/// `$doc` access nested in any control-flow branch or index expression is
/// still collected.
#[derive(Default)]
struct DocPathCollector<'a> {
    /// Name of the program currently being walked, so each collected path
    /// — and each unresolvable-index diagnostic — is attributed to its
    /// node.
    node_name: &'a str,
    by_node: BTreeMap<DocPath, BTreeSet<String>>,
    unresolvable: Vec<(String, TypeDiagnostic)>,
}

impl DocPathCollector<'_> {
    /// Record a resolved path against the node currently being walked.
    fn record(&mut self, path: DocPath) {
        self.by_node
            .entry(path)
            .or_default()
            .insert(self.node_name.to_string());
    }
}

impl Visitor for DocPathCollector<'_> {
    fn visit_expr(&mut self, expr: &Expr) {
        // An IndexAccess whose receiver chain bottoms out at a DocAccess is
        // a `$doc` path with trailing indices — classify it as a unit
        // rather than descending into the DocAccess as a bare access. Other
        // IndexAccess shapes (`record_field[0]`) fall through to the
        // default descent below.
        if let Expr::IndexAccess { .. } = expr
            && let Some(classified) = classify_doc_index_chain(expr)
        {
            match classified {
                Ok(path) => self.record(path),
                Err(diag) => self.unresolvable.push((self.node_name.to_string(), diag)),
            }
            return;
        }

        if let Expr::DocAccess { section, field, .. } = expr {
            self.record(DocPath {
                section: section.clone(),
                field: field.clone(),
                indices: Vec::new(),
            });
            return;
        }

        walk_expr(self, expr);
    }
}

/// Classify an `IndexAccess` whose receiver chain bottoms out at a
/// `DocAccess`.
///
/// Returns:
/// - `None` — not a `$doc` index chain (the receiver is something else);
///   the caller falls back to generic recursion.
/// - `Some(Ok(path))` — every index in the chain is literal.
/// - `Some(Err(diag))` — some index is a computed expression, so the path
///   is unresolvable; the diagnostic points at that index's span.
fn classify_doc_index_chain(expr: &Expr) -> Option<Result<DocPath, TypeDiagnostic>> {
    // First descend the receiver chain to its root WITHOUT judging the
    // indices. Only once the root is confirmed to be a `$doc` access do
    // the index expressions matter — `region[$doc.s.f]` has a `$doc`
    // node in INDEX position, not as the indexed receiver, so it is not a
    // `$doc` path chain and must fall through to generic recursion (which
    // collects the `$doc` access inside the index expression).
    let mut indexed_exprs: Vec<&Expr> = Vec::new();
    let mut cursor = expr;
    let (section, field) = loop {
        match cursor {
            Expr::IndexAccess {
                receiver, index, ..
            } => {
                indexed_exprs.push(index);
                cursor = receiver;
            }
            Expr::DocAccess { section, field, .. } => break (section, field),
            // Receiver chain does not bottom out at a `$doc` access — not
            // a `$doc` path. Let generic recursion handle it.
            _ => return None,
        }
    };

    // Root is a `$doc` access. Validate the indices outermost-first (the
    // chain was collected innermost-first, so reverse). The first
    // non-literal index makes the whole path unresolvable.
    let mut indices = Vec::with_capacity(indexed_exprs.len());
    for index in indexed_exprs.into_iter().rev() {
        match literal_index(index) {
            Some(seg) => indices.push(seg),
            None => return Some(Err(diag_dynamic_doc_index(index.span()))),
        }
    }
    Some(Ok(DocPath {
        section: section.clone(),
        field: field.clone(),
        indices,
    }))
}

/// Extract a literal index segment, or `None` if the index is computed.
///
/// Accepts an integer literal, a unary-negated integer literal
/// (`items[-1]` — a static from-end index), or a string-key literal.
/// Every other shape (a field reference, an arithmetic expression, a
/// float/bool/date/null literal) is non-literal and makes the path
/// unresolvable.
fn literal_index(index: &Expr) -> Option<DocIndex> {
    match index {
        Expr::Literal {
            value: LiteralValue::Int(n),
            ..
        } => Some(DocIndex::Int(*n)),
        Expr::Literal {
            value: LiteralValue::String(s),
            ..
        } => Some(DocIndex::Key(s.clone())),
        // `items[-1]` parses as `Unary{Neg, Literal(Int)}` — still a
        // static index, so resolve it rather than rejecting it as
        // "computed".
        Expr::Unary {
            op: UnaryOp::Neg,
            operand,
            ..
        } => match operand.as_ref() {
            Expr::Literal {
                value: LiteralValue::Int(n),
                ..
            } => Some(DocIndex::Int(-*n)),
            _ => None,
        },
        _ => None,
    }
}

fn diag_dynamic_doc_index(span: Span) -> TypeDiagnostic {
    TypeDiagnostic {
        span,
        message: "a `$doc` access must be indexed by a literal, but this index is a \
                  non-literal expression, so the declared document path cannot be \
                  resolved at compile time"
            .to_string(),
        help: Some(
            "index the envelope path with an integer literal (including a \
             negative such as `[-1]`) or a string-key literal \
             (`$doc.section.items[0]` / `$doc.section.meta[\"run_date\"]`)"
                .to_string(),
        ),
        related_span: None,
        is_warning: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::Span;
    use crate::parser::Parser;
    use crate::resolve::pass::resolve_program;
    use crate::typecheck::pass::type_check;
    use crate::typecheck::row::Row;

    /// Compile CXL source to a `TypedProgram` for the path analyzer.
    /// The schema is empty and field references resolve against a fixed
    /// set — sufficient because the pass only inspects `$doc` accesses.
    fn compile(source: &str) -> TypedProgram {
        let parsed = Parser::parse(source);
        assert!(
            parsed.errors.is_empty(),
            "parse errors: {:?}",
            parsed.errors
        );
        let fields: Vec<&str> = vec!["idx", "amount", "region"];
        let resolved = resolve_program(parsed.ast, &fields, parsed.node_count).unwrap();
        let schema = Row::closed(indexmap::IndexMap::new(), Span::new(0, 0));
        type_check(resolved, &schema).unwrap()
    }

    fn collect(source: &str) -> DocPathSet {
        let typed = compile(source);
        collect_doc_paths(&[("t", &typed)])
    }

    /// Sorted node names a path was attributed to.
    fn attributed_to(set: &DocPathSet, p: &DocPath) -> Vec<String> {
        set.by_node
            .get(p)
            .map(|nodes| nodes.iter().cloned().collect())
            .unwrap_or_default()
    }

    fn path(section: &str, field: &str, indices: Vec<DocIndex>) -> DocPath {
        DocPath {
            section: section.into(),
            field: field.into(),
            indices,
        }
    }

    #[test]
    fn test_collects_plain_two_level_path() {
        let set = collect("emit batch = $doc.Head.batch_id");
        assert!(set.unresolvable.is_empty());
        let p = path("Head", "batch_id", vec![]);
        assert_eq!(set.all_paths(), vec![p.clone()]);
        // The path is attributed to the single program's node.
        assert_eq!(attributed_to(&set, &p), vec!["t".to_string()]);
    }

    #[test]
    fn test_collects_multiple_distinct_paths_sorted() {
        let set = collect(
            "emit a = $doc.Foot.record_count\n\
             emit b = $doc.Head.batch_id",
        );
        assert!(set.unresolvable.is_empty());
        // BTreeMap ordering is by (section, field): Foot precedes Head.
        assert_eq!(
            set.all_paths(),
            vec![
                path("Foot", "record_count", vec![]),
                path("Head", "batch_id", vec![]),
            ]
        );
    }

    #[test]
    fn test_dedups_identical_paths_across_statements() {
        let set = collect(
            "emit a = $doc.Head.batch_id\n\
             emit b = $doc.Head.batch_id + 1",
        );
        assert_eq!(set.all_paths(), vec![path("Head", "batch_id", vec![])]);
    }

    #[test]
    fn test_collects_literal_int_index() {
        let set = collect("emit first = $doc.summary.items[0]");
        assert!(set.unresolvable.is_empty());
        assert_eq!(
            set.all_paths(),
            vec![path("summary", "items", vec![DocIndex::Int(0)])]
        );
    }

    #[test]
    fn test_collects_literal_string_index() {
        let set = collect("emit run = $doc.meta.props[\"run_date\"]");
        assert!(set.unresolvable.is_empty());
        assert_eq!(
            set.all_paths(),
            vec![path(
                "meta",
                "props",
                vec![DocIndex::Key("run_date".into())]
            )]
        );
    }

    #[test]
    fn test_collects_nested_index_chain() {
        let set = collect("emit deep = $doc.summary.rows[2][\"k\"]");
        assert!(set.unresolvable.is_empty());
        assert_eq!(
            set.all_paths(),
            vec![path(
                "summary",
                "rows",
                vec![DocIndex::Int(2), DocIndex::Key("k".into())]
            )]
        );
    }

    #[test]
    fn test_collects_path_used_only_in_if_branch() {
        // The `$doc` access lives only inside the then-branch of an `if`.
        // Conditional-only usage must still be collected.
        let set = collect("emit v = if region == \"x\" then $doc.Head.batch_id else 0");
        assert!(set.unresolvable.is_empty());
        assert_eq!(set.all_paths(), vec![path("Head", "batch_id", vec![])]);
    }

    #[test]
    fn test_collects_path_used_only_in_match_arm() {
        let set = collect(
            "emit v = match region {\n\
             \"x\" => $doc.Foot.record_count,\n\
             _ => 0,\n\
             }",
        );
        assert!(set.unresolvable.is_empty());
        assert_eq!(set.all_paths(), vec![path("Foot", "record_count", vec![])]);
    }

    #[test]
    fn test_collects_path_inside_index_expression() {
        // `$doc` appears as the index of an unrelated array access.
        let set = collect("emit v = region[$doc.Head.offset]");
        assert!(set.unresolvable.is_empty());
        assert_eq!(set.all_paths(), vec![path("Head", "offset", vec![])]);
    }

    #[test]
    fn test_dynamic_index_is_unresolvable_with_span() {
        // `idx` is a record field, not a literal — the `$doc` element it
        // selects cannot be named at compile time.
        let src = "emit v = $doc.summary.items[idx]";
        let set = collect(src);
        assert!(
            set.by_node.is_empty(),
            "no path is recorded for an unresolvable access"
        );
        assert_eq!(set.unresolvable.len(), 1);
        let (node, diag) = &set.unresolvable[0];
        // The diagnostic is tagged with the program's node name so a
        // plan-level caller can anchor a node-level span.
        assert_eq!(node, "t");
        // The span must point at the offending index expression `idx`,
        // not the whole access.
        let snippet = &src[diag.span.start as usize..diag.span.end as usize];
        assert_eq!(snippet, "idx");
        assert!(!diag.is_warning);
    }

    #[test]
    fn test_negative_literal_index_is_resolved() {
        // `items[-1]` parses as a unary-negated integer literal — a
        // static from-end index, so the path resolves rather than being
        // flagged unresolvable.
        let set = collect("emit last = $doc.summary.items[-1]");
        assert!(
            set.unresolvable.is_empty(),
            "a negated integer literal is a static index"
        );
        assert_eq!(
            set.all_paths(),
            vec![path("summary", "items", vec![DocIndex::Int(-1)])]
        );
    }

    #[test]
    fn test_unresolvable_message_does_not_claim_runtime_for_static_forms() {
        // A field-reference index is genuinely non-literal; the message
        // must describe it as a non-literal expression, never as a
        // specific runtime claim that would be false for a static `-1`.
        let set = collect("emit v = $doc.summary.items[region]");
        assert_eq!(set.unresolvable.len(), 1);
        let (_node, diag) = &set.unresolvable[0];
        assert!(
            diag.message.contains("non-literal expression"),
            "message should describe the index as a non-literal expression: {}",
            diag.message
        );
        assert!(
            diag.help.as_deref().unwrap_or("").contains("[-1]"),
            "help should mention that a negative literal is accepted"
        );
    }

    #[test]
    fn test_resolvable_and_unresolvable_coexist() {
        let set = collect(
            "emit a = $doc.Head.batch_id\n\
             emit b = $doc.summary.items[region]",
        );
        assert_eq!(set.all_paths(), vec![path("Head", "batch_id", vec![])]);
        assert_eq!(set.unresolvable.len(), 1);
    }

    #[test]
    fn test_no_doc_access_yields_empty_set() {
        let set = collect("emit v = amount + 1");
        assert!(set.by_node.is_empty());
        assert!(set.unresolvable.is_empty());
    }

    #[test]
    fn test_attributes_each_path_to_its_referencing_node() {
        // Two programs read disjoint `$doc` paths. Each path must be
        // attributed to ONLY the node that references it — the foundation
        // for stamping each source with just its own paths rather than the
        // pipeline-wide union.
        let a = compile("emit x = $doc.Head.batch_id");
        let b = compile("emit y = $doc.Foot.record_count");
        let set = collect_doc_paths(&[("node_a", &a), ("node_b", &b)]);

        let head = path("Head", "batch_id", vec![]);
        let foot = path("Foot", "record_count", vec![]);
        assert_eq!(set.all_paths(), vec![foot.clone(), head.clone()]);
        assert_eq!(attributed_to(&set, &head), vec!["node_a".to_string()]);
        assert_eq!(attributed_to(&set, &foot), vec!["node_b".to_string()]);
    }

    #[test]
    fn test_shared_path_is_attributed_to_every_referencing_node() {
        // The same path read by two nodes is attributed to both, so the
        // planner unions their source sets when stamping.
        let a = compile("emit x = $doc.Head.batch_id");
        let b = compile("emit y = $doc.Head.batch_id + 1");
        let set = collect_doc_paths(&[("node_a", &a), ("node_b", &b)]);

        let head = path("Head", "batch_id", vec![]);
        assert_eq!(set.all_paths(), vec![head.clone()]);
        assert_eq!(
            attributed_to(&set, &head),
            vec!["node_a".to_string(), "node_b".to_string()]
        );
    }
}
