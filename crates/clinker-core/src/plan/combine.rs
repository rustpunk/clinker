//! Phase Combine C.0.2 — plan-side types for the Combine node.
//!
//! This module defines the execution-plan-layer vocabulary for combine nodes:
//! the planner-selected execution [`CombineStrategy`], the compile-time
//! decomposition of the `where:` predicate into equality / range / residual
//! conjuncts ([`DecomposedPredicate`], [`EqualityConjunct`], [`RangeConjunct`],
//! [`RangeOp`]), and per-input metadata ([`CombineInput`]).
//!
//! Per the V-1-1 side-table architecture
//! (`RESEARCH-plan-node-incremental-construction.md`), the late-populated
//! compile artifacts that these types describe do NOT live inline on
//! `PlanNode::Combine`. Instead, they live in `CompileArtifacts` side-tables:
//!   - `CompileArtifacts.typed["{name}"]`     — typed cxl-body program
//!     (same key convention as Transform; drill D10)
//!   - `CompileArtifacts.combine_predicates`  — `DecomposedPredicate`
//!     (the residual re-typechecked program lives inside this, not in
//!     `typed`; drill D9 — where-clause TypedProgram is a local
//!     intermediate that doesn't survive bind_schema)
//!   - `CompileArtifacts.combine_inputs`      — per-input metadata
//!     (name-keyed; see `CombineInput` for the no-`NodeIndex` rationale)
//!
//! C.0 only adds the types; C.1 fills predicate decomposition, C.2 adds
//! execution and strategy selection, C.4 adds N-ary decomposition.

use std::collections::HashSet;
use std::sync::Arc;

use serde::Serialize;

use crate::plan::row_type::Row;
use cxl::ast::{BinOp, Expr, NodeId, Program, Statement};
use cxl::typecheck::pass::TypedProgram;
use cxl::typecheck::{TypeDiagnostic, type_check};

/// Execution strategy for a combine node, selected by the planner based on
/// predicate decomposition and input characteristics.
///
/// Serialized into `--explain` JSON (write-only — no `Deserialize`). The six
/// variants span every strategy the Phase Combine planner can pick:
/// hash build/probe (default for equi joins), in-memory hash (small side
/// known to fit in RAM), hash-partitioned IE-join (mixed equi + range),
/// sort-merge (pure range with sorted inputs), grace hash (large inputs
/// requiring disk partitioning), and block-nested-loop as the permanent
/// final-fallback (RESOLUTION B-2).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CombineStrategy {
    HashBuildProbe,
    InMemoryHash,
    HashPartitionIEJoin {
        partition_bits: u8,
    },
    SortMerge,
    GraceHash {
        partition_bits: u8,
    },
    /// Block nested loop — universal final-fallback strategy.
    /// Used for: (1) pure-range temporary fallback in C.3 until SortMerge,
    /// (2) irreducible grace hash partition fallback in C.4 (permanent).
    /// Processes in 10K-record chunks with `should_abort()` checks (D50).
    BlockNestedLoop,
}

/// Compile-time decomposition of a combine's `where:` clause into
/// equality conjuncts, range conjuncts, and a residual program.
///
/// Populated by C.1 and stored in `CompileArtifacts.combine_predicates`
/// keyed by combine node name. The planner reads this in C.2 to pick a
/// `CombineStrategy` and build hash keys / range indices.
#[derive(Debug, Clone)]
pub struct DecomposedPredicate {
    pub equalities: Vec<EqualityConjunct>,
    pub ranges: Vec<RangeConjunct>,
    pub residual: Option<Arc<TypedProgram>>,
}

/// Equality conjunct between two inputs.
///
/// Stores full [`Expr`] pairs (drill D14), not field names — enables
/// expression-based hash keys (e.g. `lower(orders.region) ==
/// lower(products.region)`) from day one. C.2 `KeyExtractor` evaluates
/// these `Expr` nodes at build/probe time.
#[derive(Debug, Clone)]
pub struct EqualityConjunct {
    pub left_expr: Expr,
    pub left_input: Arc<str>,
    pub right_expr: Expr,
    pub right_input: Arc<str>,
}

/// Range conjunct between two inputs with an operator.
///
/// Expr-based operands (drill D14) — same rationale as [`EqualityConjunct`].
/// Used by C.3 sort-merge / IE-join strategies.
#[derive(Debug, Clone)]
pub struct RangeConjunct {
    pub left_expr: Expr,
    pub left_input: Arc<str>,
    pub op: RangeOp,
    pub right_expr: Expr,
    pub right_input: Arc<str>,
}

/// Range operator for [`RangeConjunct`].
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RangeOp {
    Lt,
    Le,
    Gt,
    Ge,
}

/// Per-input metadata for a combine node in the execution plan.
///
/// Stored in `CompileArtifacts.combine_inputs["{combine_name}"][{qualifier}]`
/// with the outer `IndexMap` preserving declaration order of the inputs.
/// Populated by C.1 during schema propagation in `bind_schema`.
///
/// The upstream node is identified by its **author-visible name**
/// (`upstream_name: Arc<str>`), not by a graph-layer `NodeIndex`. This
/// is deliberate and load-bearing: `bind_schema` runs before
/// `ExecutionPlanDag` is constructed, so no `NodeIndex` exists yet.
/// Downstream phases that need a graph handle call
/// `node_by_name[&combine_input.upstream_name]` at the point of use.
///
/// Research backing: `RESEARCH-combine-compile-architecture.md` (Approach
/// A). Name-indexed compile artifacts match dbt's `depends_on.nodes`,
/// Beam's `TupleTag`, Dagster's `AssetKey`, Calcite's digest, and every
/// surveyed ETL tool. `NodeIndex`-keyed compile artifacts are documented
/// to break under `petgraph::Graph` node removal (swap-remove invalidation
/// — see petgraph issue #456) and caused the SPARK-17154 class of bugs
/// where side-annotations went stale after optimizer rewrites. Clinker's
/// unique-node-name invariant (E001) is the precondition that makes
/// name-keying safe.
#[derive(Debug, Clone)]
pub struct CombineInput {
    /// Name of the upstream node this input resolves to. Looked up in
    /// the DAG's `node_by_name` table when a `NodeIndex` is needed.
    pub upstream_name: Arc<str>,
    /// Schema of the upstream row, cloned at bind_schema time. Used by
    /// C.2+ execution strategies without having to re-traverse
    /// `schema_by_name`.
    pub row: Row,
    /// Optional cardinality estimate. Populated by later phases (e.g.
    /// statistics) when available; `None` at bind_schema time.
    pub estimated_cardinality: Option<u64>,
}

// ─── Predicate decomposition (Phase Combine C.1.2) ──────────────────
//
// The DataFusion "split-then-classify" pattern (drill D6 — universal
// consensus across DataFusion, DuckDB, Spark, PostgreSQL, Calcite,
// CockroachDB):
//
//   1. `split_conjunction` — break the root predicate on top-level `and`
//      into leaf conjuncts. `or` nodes are NEVER split (drill D7).
//   2. `classify_conjunct` — for each conjunct, decide whether it is a
//      cross-input equality (`a.x == b.y`), cross-input range
//      (`a.x < b.y`), or residual (anything else).
//   3. Residual conjuncts are folded with `and` and re-typechecked as a
//      standalone `TypedProgram` against the merged row.

/// Maximum depth for iterative conjunction splitting (RESOLUTION S-1).
/// Pathologically deep `and` chains get capped at this depth; additional
/// conjuncts beyond the cap fall into residual, preserving correctness
/// at the cost of optimization. Matches the CXL compile-time nesting
/// bound.
const MAX_SPLIT_DEPTH: usize = 256;

/// Classified conjunct returned by [`classify_conjunct`].
#[derive(Debug)]
enum ConjunctClass {
    Equality(EqualityConjunct),
    Range(RangeConjunct),
    Residual,
}

/// Split a boolean expression on top-level `and` into leaf conjuncts.
///
/// `or` nodes are kept whole — the whole OR expression becomes one
/// (residual-classified) conjunct. Iterative implementation (stack-based)
/// to guard against stack overflow for deep `and`-chains.
///
/// Preserves source order: `a and b and c` → `[a, b, c]`.
pub(crate) fn split_conjunction(expr: &Expr) -> Vec<&Expr> {
    let mut out = Vec::new();
    let mut stack: Vec<&Expr> = Vec::with_capacity(16);
    stack.push(expr);
    let mut depth = 0usize;
    while let Some(node) = stack.pop() {
        depth += 1;
        if depth > MAX_SPLIT_DEPTH {
            // Beyond the cap: emit whatever remains on the stack
            // (plus the current node) as a single residual conjunct
            // each, without further splitting.
            out.push(node);
            while let Some(remaining) = stack.pop() {
                out.push(remaining);
            }
            break;
        }
        match node {
            Expr::Binary {
                op: BinOp::And,
                lhs,
                rhs,
                ..
            } => {
                // Push RHS first so LHS pops first — preserves the
                // left-to-right source order in the output.
                stack.push(rhs.as_ref());
                stack.push(lhs.as_ref());
            }
            other => out.push(other),
        }
    }
    out
}

/// Collect all input qualifiers referenced by an expression subtree.
///
/// Walks the AST and returns the set of `Arc<str>` qualifiers gathered
/// from every `Expr::QualifiedFieldRef` whose `parts.len() >= 2`. Used by
/// [`classify_conjunct`] to decide whether a conjunct is single-input,
/// cross-input, or mixed.
///
/// 3+ part qualified refs (e.g. `a.b.c`) contribute `parts[0]` only;
/// their semantic "unknown field" status surfaces separately as E304 /
/// E308 during combine bind_schema validation.
pub(crate) fn collect_qualifiers(expr: &Expr) -> HashSet<Arc<str>> {
    let mut out = HashSet::new();
    collect_qualifiers_inner(expr, &mut out);
    out
}

fn collect_qualifiers_inner(expr: &Expr, out: &mut HashSet<Arc<str>>) {
    match expr {
        Expr::QualifiedFieldRef { parts, .. } if parts.len() >= 2 => {
            out.insert(Arc::from(parts[0].as_ref()));
        }
        Expr::QualifiedFieldRef { .. } => {}
        Expr::Binary { lhs, rhs, .. } => {
            collect_qualifiers_inner(lhs, out);
            collect_qualifiers_inner(rhs, out);
        }
        Expr::Unary { operand, .. } => collect_qualifiers_inner(operand, out),
        Expr::Coalesce { lhs, rhs, .. } => {
            collect_qualifiers_inner(lhs, out);
            collect_qualifiers_inner(rhs, out);
        }
        Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_qualifiers_inner(condition, out);
            collect_qualifiers_inner(then_branch, out);
            if let Some(eb) = else_branch {
                collect_qualifiers_inner(eb, out);
            }
        }
        Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                collect_qualifiers_inner(s, out);
            }
            for arm in arms {
                collect_qualifiers_inner(&arm.pattern, out);
                collect_qualifiers_inner(&arm.body, out);
            }
        }
        Expr::MethodCall { receiver, args, .. } => {
            collect_qualifiers_inner(receiver, out);
            for a in args {
                collect_qualifiers_inner(a, out);
            }
        }
        Expr::WindowCall { args, .. } | Expr::AggCall { args, .. } => {
            for a in args {
                collect_qualifiers_inner(a, out);
            }
        }
        Expr::FieldRef { .. }
        | Expr::Literal { .. }
        | Expr::PipelineAccess { .. }
        | Expr::MetaAccess { .. }
        | Expr::Now { .. }
        | Expr::Wildcard { .. }
        | Expr::AggSlot { .. }
        | Expr::GroupKey { .. } => {}
    }
}

/// Classify a single conjunct as cross-input equality, cross-input
/// range, or residual.
///
/// A conjunct qualifies as `Equality` iff it is `Expr::Binary { op: Eq,
/// lhs, rhs }` where `lhs` references exactly one input qualifier and
/// `rhs` references exactly one different input qualifier. Ranges are
/// the same shape with `Lt`/`Le`/`Gt`/`Ge`. Everything else — `or`,
/// same-input comparisons, mixed-qualifier operands, unary expressions,
/// literals — is residual.
///
/// Expression-based operands are fully supported: `lower(a.name) ==
/// lower(b.name)` classifies as equality because each side's qualifier
/// set has exactly one element (drill D5, matches DataFusion
/// `EquijoinPredicate`).
fn classify_conjunct(conjunct: &Expr) -> ConjunctClass {
    let Expr::Binary { op, lhs, rhs, .. } = conjunct else {
        return ConjunctClass::Residual;
    };

    let range_op = match op {
        BinOp::Eq => None,
        BinOp::Lt => Some(RangeOp::Lt),
        BinOp::Lte => Some(RangeOp::Le),
        BinOp::Gt => Some(RangeOp::Gt),
        BinOp::Gte => Some(RangeOp::Ge),
        _ => return ConjunctClass::Residual,
    };

    let lhs_quals = collect_qualifiers(lhs);
    let rhs_quals = collect_qualifiers(rhs);
    if lhs_quals.len() != 1 || rhs_quals.len() != 1 {
        return ConjunctClass::Residual;
    }
    let left_input = lhs_quals.into_iter().next().unwrap();
    let right_input = rhs_quals.into_iter().next().unwrap();
    if left_input == right_input {
        return ConjunctClass::Residual;
    }

    let left_expr = (**lhs).clone();
    let right_expr = (**rhs).clone();
    match range_op {
        None => ConjunctClass::Equality(EqualityConjunct {
            left_expr,
            left_input,
            right_expr,
            right_input,
        }),
        Some(op) => ConjunctClass::Range(RangeConjunct {
            left_expr,
            left_input,
            op,
            right_expr,
            right_input,
        }),
    }
}

/// Build a standalone `TypedProgram` wrapping the residual conjuncts as
/// a single `Statement::Filter` with an `and`-folded predicate. Returns
/// an error if the residual fails to resolve or typecheck.
///
/// Allocates fresh `NodeId`s above `original_node_count` for the
/// synthesized `and` and `Filter` nodes, then runs the full
/// resolve→typecheck pipeline so the typechecker remains the authority
/// on types (drill D8).
fn build_residual_program(
    residual_exprs: Vec<Expr>,
    merged_row: &Row,
    original_node_count: u32,
    filter_span: cxl::lexer::Span,
) -> Result<Arc<TypedProgram>, Vec<TypeDiagnostic>> {
    debug_assert!(
        !residual_exprs.is_empty(),
        "build_residual_program called with empty conjuncts — caller must guard"
    );

    let mut next_id: u32 = original_node_count;
    let mut iter = residual_exprs.into_iter();
    let mut folded = iter.next().expect("non-empty residual");
    for rhs in iter {
        let node_id = NodeId(next_id);
        next_id = next_id
            .checked_add(1)
            .expect("NodeId overflow while folding residual");
        let span = folded.span();
        folded = Expr::Binary {
            node_id,
            op: BinOp::And,
            lhs: Box::new(folded),
            rhs: Box::new(rhs),
            span,
        };
    }

    let filter_node_id = NodeId(next_id);
    next_id = next_id
        .checked_add(1)
        .expect("NodeId overflow on synthesized Filter");

    let filter_stmt = Statement::Filter {
        node_id: filter_node_id,
        predicate: folded,
        span: filter_span,
    };

    let program = Program {
        statements: vec![filter_stmt],
        span: filter_span,
    };

    let field_refs: Vec<&str> = merged_row
        .field_names()
        .map(|qf| qf.name.as_ref())
        .collect();
    let resolved =
        cxl::resolve::resolve_program(program, &field_refs, next_id).map_err(|diags| {
            diags
                .into_iter()
                .map(|d| TypeDiagnostic {
                    span: d.span,
                    message: d.message,
                    help: d.help,
                    related_span: None,
                    is_warning: false,
                })
                .collect::<Vec<_>>()
        })?;
    let typed = type_check(resolved, merged_row)?;
    Ok(Arc::new(typed))
}

/// Decompose a typed boolean predicate into cross-input equality
/// conjuncts, cross-input range conjuncts, and a residual `TypedProgram`.
///
/// Implements the split-then-classify algorithm described at the top of
/// this decomposition block. Returns `DecomposedPredicate`
/// unconditionally; callers interpret an empty-equalities +
/// empty-ranges result as E305 (no cross-input comparisons).
///
/// `typed_where` must be a TypedProgram whose first statement is
/// `Statement::Filter` — i.e. produced by parsing `filter {where_src}`
/// through the CXL pipeline. If the first statement is not a Filter,
/// the function returns a trivially-residual decomposition; the caller
/// is responsible for emitting a higher-level diagnostic in that case.
pub(crate) fn decompose_predicate(
    typed_where: &TypedProgram,
    merged_row: &Row,
    filter_span: cxl::lexer::Span,
) -> Result<DecomposedPredicate, Vec<TypeDiagnostic>> {
    let predicate = match typed_where.program.statements.first() {
        Some(Statement::Filter { predicate, .. }) => predicate,
        _ => {
            return Ok(DecomposedPredicate {
                equalities: Vec::new(),
                ranges: Vec::new(),
                residual: None,
            });
        }
    };

    let conjuncts = split_conjunction(predicate);
    let mut equalities = Vec::new();
    let mut ranges = Vec::new();
    let mut residual_exprs = Vec::new();
    for c in conjuncts {
        match classify_conjunct(c) {
            ConjunctClass::Equality(eq) => equalities.push(eq),
            ConjunctClass::Range(r) => ranges.push(r),
            ConjunctClass::Residual => residual_exprs.push(c.clone()),
        }
    }

    let residual = if residual_exprs.is_empty() {
        None
    } else {
        Some(build_residual_program(
            residual_exprs,
            merged_row,
            typed_where.node_count,
            filter_span,
        )?)
    };

    Ok(DecomposedPredicate {
        equalities,
        ranges,
        residual,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_combine_strategy_serde_round_trip() {
        // Serialize CombineStrategy variants to JSON, verify --explain shape.
        let json = serde_json::to_string(&CombineStrategy::HashBuildProbe).unwrap();
        assert_eq!(json, r#""hash_build_probe""#);

        let json =
            serde_json::to_string(&CombineStrategy::GraceHash { partition_bits: 8 }).unwrap();
        assert!(json.contains("grace_hash"));
        assert!(json.contains("partition_bits"));

        // Exercise all 6 variants so the test fails if any variant is
        // accidentally made non-serializable.
        let _ = serde_json::to_string(&CombineStrategy::InMemoryHash).unwrap();
        let _ = serde_json::to_string(&CombineStrategy::HashPartitionIEJoin { partition_bits: 10 })
            .unwrap();
        let _ = serde_json::to_string(&CombineStrategy::SortMerge).unwrap();
        let _ = serde_json::to_string(&CombineStrategy::BlockNestedLoop).unwrap();
    }

    #[test]
    fn test_existing_lookup_tests_pass() {
        // Meta-test: constructing PlanNode::Combine with the C.0.2 field set
        // and calling its `name()` / `type_tag()` methods must succeed. This
        // is a compile-time regression gate for the variant addition; the
        // hard lookup-regression coverage is `cargo test --workspace`, which
        // re-runs every existing lookup fixture test on every invocation.
        use crate::config::pipeline_node::{MatchMode, OnMiss};
        use crate::plan::execution::PlanNode;
        use crate::span::Span;

        let node = PlanNode::Combine {
            name: "test_combine".into(),
            span: Span::SYNTHETIC,
            strategy: CombineStrategy::HashBuildProbe,
            driving_input: String::new(),
            match_mode: MatchMode::First,
            on_miss: OnMiss::NullFields,
            decomposed_from: None,
        };
        assert_eq!(node.name(), "test_combine");
        assert_eq!(node.type_tag(), "combine");
    }
}
