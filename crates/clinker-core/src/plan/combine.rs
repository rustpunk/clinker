//! Plan-side types for the Combine node.
//!
//! This module defines the execution-plan-layer vocabulary for combine nodes:
//! the planner-selected execution [`CombineStrategy`], the compile-time
//! decomposition of the `where:` predicate into equality / range / residual
//! conjuncts ([`DecomposedPredicate`], [`EqualityConjunct`], [`RangeConjunct`],
//! [`RangeOp`]), and per-input metadata ([`CombineInput`]).
//!
//! The late-populated compile artifacts that these types describe do NOT
//! live inline on `PlanNode::Combine`. Instead, they live in
//! `CompileArtifacts` side-tables:
//!   - `CompileArtifacts.typed["{name}"]`     — typed cxl-body program
//!     (same key convention as Transform)
//!   - `CompileArtifacts.combine_predicates`  — `DecomposedPredicate`
//!     (the residual re-typechecked program lives inside this, not in
//!     `typed` — the where-clause TypedProgram is a local intermediate
//!     that doesn't survive bind_schema)
//!   - `CompileArtifacts.combine_inputs`      — per-input metadata
//!     (name-keyed; see `CombineInput` for the no-`NodeIndex` rationale)

use std::collections::HashSet;
use std::sync::Arc;

use indexmap::IndexMap;
use serde::Serialize;

use crate::error::{Diagnostic, LabeledSpan};
use crate::plan::row_type::Row;
use crate::span::Span;
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

/// Shape-only projection of [`DecomposedPredicate`] for `--explain` output.
///
/// Carries counts plus a `has_residual` flag — the full conjunct/program
/// data is not serializable (holds `Arc<TypedProgram>` and raw `Expr`
/// trees). Lives inline on `PlanNode::Combine` so the JSON/text renderers
/// reach it without consulting `CompileArtifacts`, which is not threaded
/// through the serializer. Populated from
/// `CompileArtifacts.combine_predicates[name]` at combine lowering time;
/// zero-valued on combines whose predicate decomposition failed (E3xx
/// diagnostics; combine absent from `combine_predicates`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct CombinePredicateSummary {
    pub equalities: usize,
    pub ranges: usize,
    pub has_residual: bool,
}

impl CombinePredicateSummary {
    /// Project a [`DecomposedPredicate`] to its `--explain`-visible shape.
    pub fn from_decomposed(d: &DecomposedPredicate) -> Self {
        Self {
            equalities: d.equalities.len(),
            ranges: d.ranges.len(),
            has_residual: d.residual.is_some(),
        }
    }
}

/// Equality conjunct between two inputs.
///
/// Stores full [`Expr`] pairs (drill D14), not field names — enables
/// expression-based hash keys (e.g. `lower(orders.region) ==
/// lower(products.region)`) from day one. C.2 `KeyExtractor` evaluates
/// these `Expr` nodes at build/probe time via `cxl::eval::eval_expr`,
/// which routes regex-cache lookups through the supplied
/// [`TypedProgram`].
///
/// Both sides share a single [`Arc<TypedProgram>`] — the original
/// where-clause program. The where-typecheck establishes regex caches
/// over the entire predicate's `NodeId` range, and equality sub-`Expr`s
/// preserve their original `NodeId`s (no re-resolve), so a single typed
/// program covers both sides without re-compilation. Each side carries
/// its own `Arc` clone (cheap atomic) to keep the consumer-facing shape
/// symmetric and to leave room for per-side specialization in later
/// phases (e.g. C.3 IEJoin's per-input compiled regex caches).
#[derive(Debug, Clone)]
pub struct EqualityConjunct {
    pub left_expr: Expr,
    pub left_input: Arc<str>,
    pub left_program: Arc<TypedProgram>,
    pub right_expr: Expr,
    pub right_input: Arc<str>,
    pub right_program: Arc<TypedProgram>,
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
/// Name-indexed compile artifacts match dbt's `depends_on.nodes`, Beam's
/// `TupleTag`, Dagster's `AssetKey`, Calcite's digest, and every surveyed
/// ETL tool. `NodeIndex`-keyed compile artifacts break under
/// `petgraph::Graph` node removal (swap-remove invalidation — see petgraph
/// issue #456) and caused the SPARK-17154 class of bugs
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
fn classify_conjunct(conjunct: &Expr, typed: &Arc<TypedProgram>) -> ConjunctClass {
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
            left_program: Arc::clone(typed),
            right_expr,
            right_input,
            right_program: Arc::clone(typed),
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
    typed_where: &Arc<TypedProgram>,
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
        match classify_conjunct(c, typed_where) {
            ConjunctClass::Equality(eq) => equalities.push(eq),
            ConjunctClass::Range(r) => {
                // Ranges are preserved as typed range conjuncts for
                // strategy selection (a future IEJoin / sort-merge
                // strategy reads them directly). But they are also
                // folded into the residual program so that the current
                // HashBuildProbe executor — which only consults the
                // residual — applies them as post-match filters. Once
                // IEJoin lands, the strategy-selection post-pass can
                // split ranges back out for that strategy.
                ranges.push(r);
                residual_exprs.push(c.clone());
            }
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

// ─── Plan-time strategy + driving-input selection (Phase Combine C.2.4) ─

/// Cardinality threshold below which an input is considered "small" for
/// the purposes of strategy selection. When every combine input has a
/// cardinality estimate `<= SMALL_INPUT_THRESHOLD` and the predicate is
/// pure-equality, the planner emits **W302** advising that
/// `InMemoryHash` may be a better fit than `HashBuildProbe`.
///
/// 10,000 mirrors the C.2 module constants in
/// `pipeline/combine.rs::COLLECT_PER_GROUP_CAP` and
/// `MEMORY_CHECK_INTERVAL`. Aligned with DataFusion's small-side
/// broadcast threshold (~10K rows in many practical configurations);
/// configurable per-combine is a future knob (out of scope for C.2).
pub(crate) const SMALL_INPUT_THRESHOLD: u64 = 10_000;

/// Plan-time post-pass that finalizes every `PlanNode::Combine` in the
/// DAG. Runs after `bind_schema` so that `CompileArtifacts` is fully
/// populated. Mirrors `select_aggregation_strategies` in shape.
///
/// Per-combine work:
///   1. **E312** — N>2 inputs are unsupported in C.2 (decomposition is
///      Phase C.4). Emits the diagnostic and leaves the node's
///      strategy/driving_input as construction defaults; downstream
///      executor dispatch sees the unstamped placeholder and bails.
///   2. **E313** — pure-range / pure-residual predicates have no
///      equi-conjuncts and cannot run on `HashBuildProbe`. Phase C.3
///      will downgrade this to a strategy switch (`SortMerge` /
///      `IEJoin`); for C.2 it's a hard error.
///   3. **W302** — pure-equi predicates with all inputs marked small
///      (cardinality ≤ [`SMALL_INPUT_THRESHOLD`]) get an advisory
///      hinting that `InMemoryHash` may be cheaper than
///      `HashBuildProbe`. Non-fatal.
///   4. Stamps `CombineStrategy::HashBuildProbe` and the driving
///      qualifier (read from `artifacts.combine_driving`) onto the node.
///      Combines whose driver selection failed at bind time (E306) are
///      absent from `combine_driving` and skipped here — the prior
///      diagnostic is the root cause.
///
/// Always runs to completion — the post-pass itself is infallible.
/// Diagnostics accumulate in `diags`; callers (e.g.
/// `compile_with_diagnostics`) inspect them to decide overall compile
/// success.
pub fn select_combine_strategies(
    plan: &mut crate::plan::execution::ExecutionPlanDag,
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
    diags: &mut Vec<Diagnostic>,
) {
    use crate::plan::execution::PlanNode;
    use petgraph::graph::NodeIndex;

    let combine_nodes: Vec<(NodeIndex, String, Span)> = plan
        .graph
        .node_indices()
        .filter_map(|idx| match &plan.graph[idx] {
            PlanNode::Combine { name, span, .. } => Some((idx, name.clone(), *span)),
            _ => None,
        })
        .collect();

    for (idx, name, span) in combine_nodes {
        let inputs = match artifacts.combine_inputs.get(&name) {
            Some(i) => i,
            None => continue,
        };
        let decomposed = match artifacts.combine_predicates.get(&name) {
            Some(d) => d,
            None => continue,
        };

        if inputs.len() > 2 {
            diags.push(combine_e312_nary_unsupported(&name, inputs.len(), span));
            continue;
        }
        if decomposed.equalities.is_empty() {
            diags.push(combine_e313_no_equi(&name, span));
            continue;
        }
        if decomposed.ranges.is_empty()
            && inputs
                .values()
                .all(|ci| matches!(ci.estimated_cardinality, Some(c) if c <= SMALL_INPUT_THRESHOLD))
        {
            diags.push(combine_w302_small_both(&name, span));
        }

        let driving = match artifacts.combine_driving.get(&name) {
            Some(d) => d.clone(),
            None => continue,
        };

        // All non-driver input qualifiers, in declaration (IndexMap)
        // order. The driver is singular by construction
        // (`select_driving_input`); everything else is a build side.
        let build: Vec<String> = inputs
            .keys()
            .filter(|q| q.as_str() != driving.as_str())
            .cloned()
            .collect();

        if let PlanNode::Combine {
            strategy,
            driving_input,
            build_inputs,
            ..
        } = &mut plan.graph[idx]
        {
            *strategy = CombineStrategy::HashBuildProbe;
            *driving_input = driving;
            *build_inputs = build;
        }
    }
}

/// E312 — combine has more than 2 inputs. C.2 supports binary combines
/// only; N>2 is decomposed in C.4.
fn combine_e312_nary_unsupported(combine_name: &str, input_count: usize, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E312",
        format!(
            "combine {combine_name:?} declares {input_count} inputs; C.2 supports binary \
             combines only (N=2). N-ary combine decomposition lands in Phase C.4."
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "split the combine into a chain of 2-input combines, or wait for the C.4 N-ary \
         decomposition pass",
    )
}

/// E313 — combine `where:` predicate has no extractable equality
/// conjuncts. `HashBuildProbe` requires at least one cross-input
/// equality; pure-range predicates need `SortMerge` / `IEJoin`
/// (Phase C.3).
fn combine_e313_no_equi(combine_name: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E313",
        format!(
            "combine {combine_name:?} where-clause has no equality conjuncts; \
             `HashBuildProbe` requires at least one cross-input `==` comparison"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "add a cross-input equality (e.g. `a.id == b.id`); pure-range predicates land in \
         Phase C.3 (`SortMerge` / `IEJoin`)",
    )
}

/// W302 — pure-equi combine where every input has a cardinality
/// estimate at or below [`SMALL_INPUT_THRESHOLD`]. `InMemoryHash` may
/// be cheaper; this is advisory only.
fn combine_w302_small_both(combine_name: &str, span: Span) -> Diagnostic {
    Diagnostic::warning(
        "W302",
        format!(
            "combine {combine_name:?} has pure-equality predicates and all inputs are small \
             (cardinality ≤ {SMALL_INPUT_THRESHOLD}); consider `InMemoryHash` for lower \
             setup cost"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "InMemoryHash skips the build/probe pipeline split when both sides fit in RAM; \
         opt in via the planner once C.3 wires the strategy override",
    )
}

/// Pick the driving (probe) input for a combine node.
///
/// Resolution order — universal across surveyed engines (DataFusion's
/// `JoinSelection`, Spark's `JoinSelectionHelper`, Calcite's
/// `JoinPushTransitivePredicatesRule`):
///   1. **Explicit `drive:` hint** — author override. Validated against
///      `inputs`; an unknown hint emits **E306** and returns `Err(())`
///      so downstream sub-passes can skip the broken combine cleanly.
///   2. **Cardinality** — pick the input with the highest
///      `estimated_cardinality` when every input has a `Some(_)` value.
///      Build the smaller side; probe the larger one. Matches
///      DataFusion's "build the small side" rule and Spark's
///      `chooseBuildSide` policy.
///   3. **Declaration order fallback** — first qualifier in the
///      `IndexMap`. For `inputs.len() >= 3` this also emits **W306**
///      (planner cannot determine optimal driver). C.2 gates N>2 at
///      `select_combine_strategies` (E312), so W306 is unreachable in
///      C.2; it's wired now for the C.4 N-ary path.
pub(crate) fn select_driving_input(
    inputs: &IndexMap<String, CombineInput>,
    explicit_drive: Option<&str>,
    combine_name: &str,
    span: Span,
    diags: &mut Vec<Diagnostic>,
) -> Option<String> {
    if let Some(d) = explicit_drive {
        if inputs.contains_key(d) {
            return Some(d.to_string());
        }
        diags.push(combine_e306_invalid_drive(combine_name, d, inputs, span));
        return None;
    }

    if inputs.values().all(|ci| ci.estimated_cardinality.is_some()) {
        // Iterate via `IndexMap::iter` so ties resolve to the earliest
        // declared qualifier (deterministic; `max_by_key` keeps the
        // first max under stable ordering).
        let (qualifier, _) = inputs
            .iter()
            .max_by_key(|(_, ci)| ci.estimated_cardinality.unwrap())
            .expect("inputs non-empty (E300 guard upstream)");
        return Some(qualifier.clone());
    }

    if inputs.len() >= 3 {
        diags.push(combine_w306_ambiguous_driver(combine_name, span));
    }

    Some(
        inputs
            .keys()
            .next()
            .expect("inputs non-empty (E300 guard upstream)")
            .clone(),
    )
}

/// E306 — explicit `drive:` hint references an input that is not
/// declared on the combine node.
fn combine_e306_invalid_drive(
    combine_name: &str,
    requested: &str,
    inputs: &IndexMap<String, CombineInput>,
    span: Span,
) -> Diagnostic {
    let valid: Vec<&str> = inputs.keys().map(String::as_str).collect();
    Diagnostic::error(
        "E306",
        format!(
            "combine {combine_name:?} drive hint {requested:?} is not a declared input; \
             valid inputs: {valid:?}"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "set `drive:` to one of the qualifiers declared under `input:`, or omit it to use \
         the planner default (highest-cardinality input, or the first declared)",
    )
}

/// W306 — planner cannot pick a single optimal driving input. Emitted
/// for combines with 3+ inputs that lack cardinality estimates. Wired
/// now for C.4 N-ary; unreachable in C.2 (E312 fires first).
fn combine_w306_ambiguous_driver(combine_name: &str, span: Span) -> Diagnostic {
    Diagnostic::warning(
        "W306",
        format!(
            "combine {combine_name:?} planner cannot determine an optimal driving input \
             (3+ inputs without cardinality estimates); falling back to declaration order"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "add an explicit `drive:` hint to silence this warning, or attach cardinality \
         estimates upstream",
    )
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

    fn synthetic_input(qualifier: &str, cardinality: Option<u64>) -> (String, CombineInput) {
        use indexmap::IndexMap;
        let row = Row::closed(IndexMap::new(), cxl::lexer::Span::new(0, 0));
        (
            qualifier.to_string(),
            CombineInput {
                upstream_name: Arc::from(qualifier),
                row,
                estimated_cardinality: cardinality,
            },
        )
    }

    #[test]
    fn test_select_driving_input_explicit_drive() {
        let mut inputs = IndexMap::new();
        let (k, v) = synthetic_input("orders", None);
        inputs.insert(k, v);
        let (k, v) = synthetic_input("products", None);
        inputs.insert(k, v);

        let mut diags = Vec::new();
        let chosen =
            select_driving_input(&inputs, Some("products"), "c1", Span::SYNTHETIC, &mut diags)
                .expect("explicit drive must succeed");
        assert_eq!(chosen, "products");
        assert!(diags.is_empty(), "no diagnostics for valid drive");
    }

    #[test]
    fn test_select_driving_input_invalid_drive_e306() {
        let mut inputs = IndexMap::new();
        let (k, v) = synthetic_input("orders", None);
        inputs.insert(k, v);
        let (k, v) = synthetic_input("products", None);
        inputs.insert(k, v);

        let mut diags = Vec::new();
        let result =
            select_driving_input(&inputs, Some("ghost"), "c1", Span::SYNTHETIC, &mut diags);
        assert!(result.is_none(), "invalid drive must return None");
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code, "E306");
    }

    #[test]
    fn test_select_driving_input_by_cardinality() {
        let mut inputs = IndexMap::new();
        let (k, v) = synthetic_input("orders", Some(100));
        inputs.insert(k, v);
        let (k, v) = synthetic_input("products", Some(10_000));
        inputs.insert(k, v);

        let mut diags = Vec::new();
        let chosen = select_driving_input(&inputs, None, "c1", Span::SYNTHETIC, &mut diags)
            .expect("cardinality path must succeed");
        assert_eq!(chosen, "products", "highest cardinality drives");
        assert!(diags.is_empty());
    }

    #[test]
    fn test_select_driving_input_default_first_in_indexmap() {
        let mut inputs = IndexMap::new();
        let (k, v) = synthetic_input("orders", None);
        inputs.insert(k, v);
        let (k, v) = synthetic_input("products", None);
        inputs.insert(k, v);

        let mut diags = Vec::new();
        let chosen = select_driving_input(&inputs, None, "c1", Span::SYNTHETIC, &mut diags)
            .expect("default path must succeed");
        assert_eq!(chosen, "orders", "first in declaration order wins");
        assert!(diags.is_empty(), "no W306 below 3 inputs");
    }

    #[test]
    fn test_select_driving_input_w306_three_inputs_no_cardinality() {
        let mut inputs = IndexMap::new();
        for q in ["a", "b", "c"] {
            let (k, v) = synthetic_input(q, None);
            inputs.insert(k, v);
        }

        let mut diags = Vec::new();
        let chosen = select_driving_input(&inputs, None, "c1", Span::SYNTHETIC, &mut diags)
            .expect("3-input fallback must succeed");
        assert_eq!(chosen, "a");
        assert_eq!(diags.len(), 1, "W306 fired");
        assert_eq!(diags[0].code, "W306");
    }

    fn synthetic_combine_plan(
        inputs: Vec<(&str, Option<u64>)>,
        equalities: usize,
        ranges: usize,
        with_driver: bool,
    ) -> (
        crate::plan::execution::ExecutionPlanDag,
        crate::plan::bind_schema::CompileArtifacts,
    ) {
        use crate::config::pipeline_node::{MatchMode, OnMiss};
        use crate::plan::bind_schema::CompileArtifacts;
        use crate::plan::execution::{ExecutionPlanDag, PlanNode};
        use crate::span::Span;
        use cxl::ast::{Expr, LiteralValue, NodeId};
        use cxl::lexer::Span as CxlSpan;
        use indexmap::IndexMap;

        let combine_name = "test_combine";
        let mut artifacts = CompileArtifacts::default();

        let mut inputs_map: IndexMap<String, CombineInput> = IndexMap::new();
        for (qual, card) in &inputs {
            inputs_map.insert(
                (*qual).to_string(),
                CombineInput {
                    upstream_name: Arc::from(*qual),
                    row: Row::closed(IndexMap::new(), CxlSpan::new(0, 0)),
                    estimated_cardinality: *card,
                },
            );
        }
        artifacts
            .combine_inputs
            .insert(combine_name.to_string(), inputs_map);

        if with_driver {
            artifacts
                .combine_driving
                .insert(combine_name.to_string(), inputs[0].0.to_string());
        }

        let dummy_lit = || Expr::Literal {
            node_id: NodeId(0),
            value: LiteralValue::Int(0),
            span: CxlSpan::new(0, 0),
        };
        let dummy_program = Arc::new(cxl::typecheck::pass::TypedProgram {
            program: cxl::ast::Program {
                statements: Vec::new(),
                span: CxlSpan::new(0, 0),
            },
            types: Vec::new(),
            bindings: Vec::new(),
            field_types: IndexMap::new(),
            regexes: Vec::new(),
            node_count: 0,
            output_row: cxl::typecheck::row::Row::closed(IndexMap::new(), CxlSpan::new(0, 0)),
        });
        let mk_eq = || EqualityConjunct {
            left_expr: dummy_lit(),
            left_input: Arc::from(inputs[0].0),
            left_program: Arc::clone(&dummy_program),
            right_expr: dummy_lit(),
            right_input: Arc::from(inputs.get(1).map(|(q, _)| *q).unwrap_or(inputs[0].0)),
            right_program: Arc::clone(&dummy_program),
        };
        let mk_range = || RangeConjunct {
            left_expr: dummy_lit(),
            left_input: Arc::from(inputs[0].0),
            op: RangeOp::Lt,
            right_expr: dummy_lit(),
            right_input: Arc::from(inputs.get(1).map(|(q, _)| *q).unwrap_or(inputs[0].0)),
        };
        let decomposed = DecomposedPredicate {
            equalities: (0..equalities).map(|_| mk_eq()).collect(),
            ranges: (0..ranges).map(|_| mk_range()).collect(),
            residual: None,
        };
        artifacts
            .combine_predicates
            .insert(combine_name.to_string(), decomposed);

        let predicate_summary = CombinePredicateSummary::from_decomposed(
            artifacts
                .combine_predicates
                .get(combine_name)
                .expect("synthetic_combine_plan just inserted a DecomposedPredicate"),
        );
        let mut graph = petgraph::graph::DiGraph::new();
        graph.add_node(PlanNode::Combine {
            name: combine_name.to_string(),
            span: Span::SYNTHETIC,
            strategy: CombineStrategy::HashBuildProbe,
            driving_input: String::new(),
            build_inputs: Vec::new(),
            predicate_summary,
            match_mode: MatchMode::First,
            on_miss: OnMiss::NullFields,
            decomposed_from: None,
            output_schema: clinker_record::SchemaBuilder::new().build(),
            resolved_column_map: Arc::new(std::collections::HashMap::new()),
        });
        let plan = ExecutionPlanDag {
            graph,
            topo_order: Vec::new(),
            source_dag: Vec::new(),
            indices_to_build: Vec::new(),
            output_projections: Vec::new(),
            parallelism: crate::plan::execution::ParallelismProfile {
                per_transform: Vec::new(),
                worker_threads: 1,
            },
            correlation_sort_note: None,
            node_properties: std::collections::HashMap::new(),
        };
        (plan, artifacts)
    }

    fn first_combine(
        plan: &crate::plan::execution::ExecutionPlanDag,
    ) -> &crate::plan::execution::PlanNode {
        plan.graph
            .node_weights()
            .find(|n| matches!(n, crate::plan::execution::PlanNode::Combine { .. }))
            .expect("plan has a combine node")
    }

    #[test]
    fn test_combine_strategy_selects_hash_build_probe() {
        let (mut plan, artifacts) =
            synthetic_combine_plan(vec![("orders", None), ("products", None)], 1, 0, true);
        let mut diags = Vec::new();
        select_combine_strategies(&mut plan, &artifacts, &mut diags);
        assert!(diags.is_empty(), "no diagnostics expected; got {diags:?}");
        if let crate::plan::execution::PlanNode::Combine {
            strategy,
            driving_input,
            ..
        } = first_combine(&plan)
        {
            assert!(matches!(strategy, CombineStrategy::HashBuildProbe));
            assert_eq!(driving_input, "orders");
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_combine_strategy_e312_nary_unsupported() {
        let (mut plan, artifacts) =
            synthetic_combine_plan(vec![("a", None), ("b", None), ("c", None)], 1, 0, true);
        let mut diags = Vec::new();
        select_combine_strategies(&mut plan, &artifacts, &mut diags);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code, "E312");
        if let crate::plan::execution::PlanNode::Combine { driving_input, .. } =
            first_combine(&plan)
        {
            assert!(driving_input.is_empty(), "no driver stamped on E312");
        }
    }

    #[test]
    fn test_combine_strategy_e313_no_equi() {
        let (mut plan, artifacts) =
            synthetic_combine_plan(vec![("orders", None), ("products", None)], 0, 1, true);
        let mut diags = Vec::new();
        select_combine_strategies(&mut plan, &artifacts, &mut diags);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code, "E313");
    }

    #[test]
    fn test_combine_strategy_w302_small_both_advisory() {
        let (mut plan, artifacts) = synthetic_combine_plan(
            vec![("orders", Some(100)), ("products", Some(500))],
            1,
            0,
            true,
        );
        let mut diags = Vec::new();
        select_combine_strategies(&mut plan, &artifacts, &mut diags);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code, "W302");
        // Strategy still stamped — W302 is advisory.
        if let crate::plan::execution::PlanNode::Combine { strategy, .. } = first_combine(&plan) {
            assert!(matches!(strategy, CombineStrategy::HashBuildProbe));
        }
    }

    #[test]
    fn test_combine_strategy_skips_when_no_driver() {
        let (mut plan, artifacts) =
            synthetic_combine_plan(vec![("orders", None), ("products", None)], 1, 0, false);
        let mut diags = Vec::new();
        select_combine_strategies(&mut plan, &artifacts, &mut diags);
        // E306 is the root cause and was already in diags before this pass;
        // this pass should NOT add a new diagnostic for the missing driver.
        assert!(diags.is_empty());
        if let crate::plan::execution::PlanNode::Combine { driving_input, .. } =
            first_combine(&plan)
        {
            assert!(driving_input.is_empty(), "no stamp without driver");
        }
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
            build_inputs: Vec::new(),
            predicate_summary: CombinePredicateSummary::default(),
            match_mode: MatchMode::First,
            on_miss: OnMiss::NullFields,
            decomposed_from: None,
            output_schema: clinker_record::SchemaBuilder::new().build(),
            resolved_column_map: Arc::new(std::collections::HashMap::new()),
        };
        assert_eq!(node.name(), "test_combine");
        assert_eq!(node.type_tag(), "combine");
    }
}
