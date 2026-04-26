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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use indexmap::IndexMap;
use serde::Serialize;

use crate::error::{Diagnostic, LabeledSpan};
use crate::plan::execution::DependencyType;
use crate::plan::row_type::{QualifiedField, Row};
use crate::span::Span;
use cxl::ast::{BinOp, Expr, NodeId, Program, Statement};
use cxl::typecheck::Type;
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
    #[serde(rename = "hash_partition_iejoin")]
    HashPartitionIEJoin {
        partition_bits: u8,
    },
    /// Pure-range IEJoin without hash partitioning. Selected when the
    /// `where:` clause has only range conjuncts — there is no equality
    /// key to bucket on, so the IEJoin runs as a single virtual
    /// partition. Single-inequality predicates route through the same
    /// strategy and dispatch to the piecewise merge join kernel
    /// inside the executor.
    #[serde(rename = "iejoin")]
    IEJoin,
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

/// Short label for an `Expr` operand inside an equality / range conjunct,
/// rendered in `--explain` predicate-detail lines. Bare
/// `QualifiedFieldRef`s render as `"input.field"` (the common case);
/// any compound expression (function call, arithmetic, coalesce) prints
/// as `"<expr>"` because a faithful re-printer would require a CXL AST
/// pretty-printer that does not yet exist. The qualifier is preserved
/// as a leading prefix when the expression starts with one so users
/// can still pin which side an opaque expression belongs to.
fn format_conjunct_operand(expr: &Expr) -> String {
    match expr {
        Expr::QualifiedFieldRef { parts, .. } => parts
            .iter()
            .map(|p| p.as_ref())
            .collect::<Vec<_>>()
            .join("."),
        _ => "<expr>".to_string(),
    }
}

/// Symbol form of a [`RangeOp`] for `--explain` rendering. Matches CXL
/// surface syntax (`<`, `<=`, `>`, `>=`).
fn range_op_symbol(op: RangeOp) -> &'static str {
    match op {
        RangeOp::Lt => "<",
        RangeOp::Le => "<=",
        RangeOp::Gt => ">",
        RangeOp::Ge => ">=",
    }
}

impl DecomposedPredicate {
    /// Render the `Equalities`/`Ranges`/`Residual` detail block for
    /// `--explain` text output. Each non-empty bucket emits a header line
    /// followed by indented `- left == right` (or `< / <= / > / >=`)
    /// entries. The residual program is rendered as `<compiled
    /// expression>` because a faithful pretty-printer for `TypedProgram`
    /// is not part of this surface — the count + presence flag is the
    /// load-bearing signal for users tuning a predicate.
    ///
    /// Returns the empty string when every bucket is empty (the
    /// summary-only line above is enough). Each emitted line ends with
    /// a `\n` so the caller composes blocks via `push_str`.
    pub fn format_text(&self) -> String {
        let mut out = String::new();
        if !self.equalities.is_empty() {
            out.push_str("  Equalities:\n");
            for eq in &self.equalities {
                out.push_str(&format!(
                    "    - {} == {}\n",
                    format_conjunct_operand(&eq.left_expr),
                    format_conjunct_operand(&eq.right_expr),
                ));
            }
        }
        if !self.ranges.is_empty() {
            out.push_str("  Ranges:\n");
            for r in &self.ranges {
                out.push_str(&format!(
                    "    - {} {} {}\n",
                    format_conjunct_operand(&r.left_expr),
                    range_op_symbol(r.op),
                    format_conjunct_operand(&r.right_expr),
                ));
            }
        }
        if self.residual.is_some() {
            out.push_str("  Residual:\n");
            out.push_str("    - <compiled expression>\n");
        }
        out
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
///   1. **E313** — pure-residual predicates have no equi-conjuncts and
///      no range conjuncts, so no decomposable strategy can run them.
///      Hard error; downstream operator dispatch never sees the node.
///   2. **W302** — pure-equi predicates with all inputs marked small
///      (cardinality ≤ [`SMALL_INPUT_THRESHOLD`]) get an advisory
///      hinting that `InMemoryHash` may be cheaper than
///      `HashBuildProbe`. Non-fatal.
///   3. Stamps `CombineStrategy::HashBuildProbe` and the driving
///      qualifier (read from `artifacts.combine_driving`) onto the node.
///      Combines whose driver selection failed at bind time (E306) are
///      absent from `combine_driving` and skipped here — the prior
///      diagnostic is the root cause.
///
/// N-ary combines (input count > 2) are rewritten upstream by
/// [`decompose_nary_combines`] into a chain of binary combines before
/// this post-pass runs, so `inputs.len() <= 2` is an invariant here.
///
/// Always runs to completion — the post-pass itself is infallible.
/// Diagnostics accumulate in `diags`; callers (e.g.
/// `compile_with_diagnostics`) inspect them to decide overall compile
/// success.
/// Bundled artifact subsets the per-graph combine post-pass reads.
///
/// Threaded into [`select_combine_strategies_in_graph`] so the same
/// work runs uniformly over the top-level
/// [`crate::plan::execution::ExecutionPlanDag`] and over each
/// composition body's mini-DAG. Carrying the subsets as a struct
/// (rather than half a dozen positional arguments) lets the wrapper
/// `select_combine_strategies_in_bodies` borrow-split
/// [`crate::plan::bind_schema::CompileArtifacts`] across
/// `composition_bodies` (mutable) and the four lookup tables
/// (immutable) without falling back to a clippy allow.
pub(crate) struct CombineSelectionTables<'a> {
    /// Per-combine input qualifier metadata, keyed by combine node name.
    pub combine_inputs: &'a std::collections::HashMap<String, IndexMap<String, CombineInput>>,
    /// Per-combine decomposed `where:` predicate, keyed by combine node name.
    pub combine_predicates: &'a std::collections::HashMap<String, DecomposedPredicate>,
    /// Per-combine driving qualifier, keyed by combine node name.
    pub combine_driving: &'a std::collections::HashMap<String, String>,
    /// Per-combine user-supplied strategy hint, keyed by combine node name.
    pub combine_strategy_hints:
        &'a std::collections::HashMap<String, crate::config::CombineStrategyHint>,
}

pub fn select_combine_strategies(
    plan: &mut crate::plan::execution::ExecutionPlanDag,
    artifacts: &crate::plan::bind_schema::CompileArtifacts,
    diags: &mut Vec<Diagnostic>,
    memory_limit: Option<&str>,
) {
    // The top-level pass owns the `node_properties` table needed by
    // `pure_range_inputs_presorted`. Snapshot it as a clone so the
    // shared inner pass can take an immutable view alongside the
    // graph's mutable borrow without aliasing through `plan`.
    let node_properties_snapshot = plan.node_properties.clone();
    let tables = CombineSelectionTables {
        combine_inputs: &artifacts.combine_inputs,
        combine_predicates: &artifacts.combine_predicates,
        combine_driving: &artifacts.combine_driving,
        combine_strategy_hints: &artifacts.combine_strategy_hints,
    };
    select_combine_strategies_in_graph(
        &mut plan.graph,
        Some(&node_properties_snapshot),
        &tables,
        diags,
        memory_limit,
    );
}

/// Run [`select_combine_strategies`]'s per-graph work on every
/// composition body's mini-DAG. Body graphs hold their own
/// `PlanNode::Combine` nodes that the top-level pass cannot see, so
/// without this companion sweep body-context combines reach the
/// executor with placeholder strategy + empty driving_input, then
/// short-circuit at dispatch with an `Internal` "driving_input not
/// stamped" error. Body graphs lack populated `node_properties` —
/// the pure-range SortMerge eligibility probe is skipped.
pub fn select_combine_strategies_in_bodies(
    artifacts: &mut crate::plan::bind_schema::CompileArtifacts,
    diags: &mut Vec<Diagnostic>,
    memory_limit: Option<&str>,
) {
    // Split the artifacts borrow: the bodies map mutates via
    // `values_mut()`, while the lookup tables (combine_inputs,
    // combine_predicates, combine_driving, combine_strategy_hints)
    // remain immutable references for the inner per-graph call.
    let crate::plan::bind_schema::CompileArtifacts {
        composition_bodies,
        combine_inputs,
        combine_predicates,
        combine_driving,
        combine_strategy_hints,
        ..
    } = artifacts;

    let tables = CombineSelectionTables {
        combine_inputs,
        combine_predicates,
        combine_driving,
        combine_strategy_hints,
    };
    for body in composition_bodies.values_mut() {
        select_combine_strategies_in_graph(&mut body.graph, None, &tables, diags, memory_limit);
    }
}

/// Inner per-graph implementation of [`select_combine_strategies`].
///
/// Splits the artifacts borrow so the same work can run twice — once
/// over the top-level `ExecutionPlanDag.graph` and once per body's
/// mini-DAG inside `CompileArtifacts.composition_bodies`.
///
/// `node_properties` carries sort-order provenance for pure-range
/// `SortMerge` eligibility. Top-level callers pass `Some(&props)`;
/// body-context callers pass `None` because body lowering never runs
/// the parallelism / property-derivation passes that populate it.
/// Without that view, pure-range combines uniformly route to
/// `IEJoin`, never `SortMerge` — body bodies don't author pure-range
/// fixtures today, so the difference is academic.
pub(crate) fn select_combine_strategies_in_graph(
    graph: &mut petgraph::graph::DiGraph<
        crate::plan::execution::PlanNode,
        crate::plan::execution::PlanEdge,
    >,
    node_properties: Option<
        &std::collections::HashMap<
            petgraph::graph::NodeIndex,
            crate::plan::properties::NodeProperties,
        >,
    >,
    tables: &CombineSelectionTables<'_>,
    diags: &mut Vec<Diagnostic>,
    memory_limit: Option<&str>,
) {
    use crate::plan::execution::PlanNode;
    use petgraph::graph::NodeIndex;

    let combine_nodes: Vec<(NodeIndex, String, Span)> = graph
        .node_indices()
        .filter_map(|idx| match &graph[idx] {
            PlanNode::Combine { name, span, .. } => Some((idx, name.clone(), *span)),
            _ => None,
        })
        .collect();

    for (idx, name, span) in combine_nodes {
        let inputs = match tables.combine_inputs.get(&name) {
            Some(i) => i,
            None => continue,
        };
        let decomposed = match tables.combine_predicates.get(&name) {
            Some(d) => d,
            None => continue,
        };

        // Pre-classify the predicate shape. Three branches:
        //   - mixed equi + range  → HashPartitionIEJoin
        //   - pure range, no equi → IEJoin (single partition, plus W305)
        //   - pure equi, no range → HashBuildProbe (default path)
        // A predicate with neither equi nor range conjuncts emits
        // E313: there is no decomposable strategy that can run it.
        let has_equi = !decomposed.equalities.is_empty();
        let has_range = !decomposed.ranges.is_empty();
        if !has_equi && !has_range {
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

        let driving = match tables.combine_driving.get(&name) {
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

        let chosen_strategy: CombineStrategy = if has_equi && has_range {
            CombineStrategy::HashPartitionIEJoin {
                partition_bits: compute_partition_bits(inputs),
            }
        } else if !has_equi && has_range {
            // Pure-range predicate. SortMerge is preferred when both
            // inputs already arrive sorted on the range key prefix —
            // the kernel skips Phase A external sort and walks the
            // pre-sorted inputs in place, which is asymptotically
            // cheaper than IEJoin's L1/L2/permutation construction
            // for large pre-sorted workloads. SortMerge today only
            // covers the single-inequality case; mixed-axis (`a.x <=
            // b.y AND a.z >= b.w`) keeps routing to IEJoin.
            let sort_merge_eligible = decomposed.ranges.len() == 1
                && node_properties.is_some_and(|props| {
                    pure_range_inputs_presorted(graph, props, &name, &decomposed.ranges[0])
                });
            // W305 still fires either way: pure-range without a hash
            // partition key surfaces the perf cliff so authors notice
            // even when SortMerge takes the in-place fast path.
            diags.push(combine_w305_pure_range(&name, span));
            if sort_merge_eligible {
                CombineStrategy::SortMerge
            } else {
                CombineStrategy::IEJoin
            }
        } else if grace_hash_should_fire(inputs, memory_limit)
            || matches!(
                tables.combine_strategy_hints.get(&name),
                Some(crate::config::CombineStrategyHint::GraceHash)
            )
        {
            // Pure-equi predicate with either:
            //   - an estimated build cardinality large enough to risk
            //     overrunning the soft memory limit (planner-driven), or
            //   - a user-supplied `strategy: grace_hash` override.
            // Both routes pick the disk-spilling grace hash strategy.
            // Cardinality may be unknown under the user override path,
            // in which case `compute_grace_partition_bits` falls back to
            // its `MIN_BITS` floor — the executor still partitions the
            // build correctly, it just splits into fewer buckets.
            CombineStrategy::GraceHash {
                partition_bits: compute_grace_partition_bits(inputs),
            }
        } else {
            CombineStrategy::HashBuildProbe
        };

        if let PlanNode::Combine {
            strategy,
            driving_input,
            build_inputs,
            ..
        } = &mut graph[idx]
        {
            *strategy = chosen_strategy;
            *driving_input = driving;
            *build_inputs = build;
        }
    }
}

/// True when the planner should emit `GraceHash` instead of
/// `HashBuildProbe`. Fires when every input has an estimated
/// cardinality AND the smaller side multiplied by a conservative
/// per-record byte estimate would breach the configured 80% soft
/// limit. The pipeline's `memory_limit:` YAML knob is what binds
/// the threshold; absent that, we fall back to the same default
/// `MemoryBudget::from_config(None)` uses.
///
/// When estimates are missing on either side, returns false — there
/// is no signal to override the default in-memory hash strategy, and
/// overspilling small joins is worse than the rare case of a missed
/// grace dispatch.
fn grace_hash_should_fire(
    inputs: &IndexMap<String, CombineInput>,
    memory_limit: Option<&str>,
) -> bool {
    use crate::pipeline::grace_hash::GRACE_RECORD_BYTES_ESTIMATE;
    use crate::pipeline::memory::parse_memory_limit_bytes;
    let estimates: Vec<u64> = inputs
        .values()
        .filter_map(|ci| ci.estimated_cardinality)
        .collect();
    if estimates.len() < inputs.len() {
        return false;
    }
    let build_estimate = estimates.iter().copied().min().unwrap_or(0);
    if build_estimate == 0 {
        return false;
    }
    let limit = parse_memory_limit_bytes(memory_limit);
    let soft_limit = limit / 100 * 80;
    let estimated_bytes = build_estimate.saturating_mul(GRACE_RECORD_BYTES_ESTIMATE);
    estimated_bytes >= soft_limit
}

/// Compute `partition_bits` for `GraceHash`. Targets ~256 records per
/// partition under an unspilled build to keep per-partition hash table
/// overhead amortized. Capped at 12 bits (4096 partitions) per the
/// per-partition file overhead breakeven.
fn compute_grace_partition_bits(inputs: &IndexMap<String, CombineInput>) -> u8 {
    const TARGET_PER_PARTITION: u64 = 256;
    const MIN_BITS: u8 = 4; // 16 partitions floor
    const MAX_BITS: u8 = 12; // 4096 partitions cap
    let estimates: Vec<u64> = inputs
        .values()
        .filter_map(|ci| ci.estimated_cardinality)
        .collect();
    if estimates.len() < inputs.len() {
        return MIN_BITS;
    }
    let build_estimate = estimates.iter().copied().min().unwrap_or(0);
    if build_estimate == 0 {
        return MIN_BITS;
    }
    let max_partitions = build_estimate / TARGET_PER_PARTITION;
    if max_partitions <= 1 {
        return MIN_BITS;
    }
    let bits = (max_partitions as f64).log2().ceil() as u8;
    bits.clamp(MIN_BITS, MAX_BITS)
}

/// Compute `partition_bits` for `HashPartitionIEJoin`. Default is 8 (256
/// partitions). When estimated build cardinality is known and would
/// produce partitions averaging fewer than 64 records — the breakeven
/// where IEJoin's sort overhead dominates over linear scan — the bit
/// count is reduced. The minimum returned value is 1 (two partitions);
/// pure-range combines bypass partitioning entirely and use
/// [`CombineStrategy::IEJoin`] instead.
fn compute_partition_bits(inputs: &IndexMap<String, CombineInput>) -> u8 {
    const DEFAULT_BITS: u8 = 8;
    const MIN_BITS: u8 = 1;
    const TARGET_PER_PARTITION: u64 = 64;
    // Use the smaller side as the "build" estimate — same heuristic
    // the grace-hash partitioner uses. When estimates are missing on
    // either input, fall back to the default.
    let estimates: Vec<u64> = inputs
        .values()
        .filter_map(|ci| ci.estimated_cardinality)
        .collect();
    if estimates.len() < inputs.len() {
        return DEFAULT_BITS;
    }
    let build_estimate = estimates.iter().copied().min().unwrap_or(0);
    if build_estimate == 0 {
        return DEFAULT_BITS;
    }
    // Find the largest `bits` such that build_estimate / 2^bits >= 64.
    // i.e. 2^bits <= build_estimate / 64.
    let max_partitions = build_estimate / TARGET_PER_PARTITION;
    if max_partitions <= 1 {
        return MIN_BITS;
    }
    let bits = (max_partitions as f64).log2().floor() as u8;
    bits.clamp(MIN_BITS, DEFAULT_BITS)
}

/// True when the combine's two inputs already arrive sorted on the
/// range conjunct's per-side key — the precondition for the SortMerge
/// kernel to skip Phase A external sort and walk the inputs in place.
///
/// Only simple `qualifier.field` operands qualify: an expression-shaped
/// range axis (e.g. `lower(a.x) < b.y`) cannot be answered by upstream
/// `sort_order`, since the sort declaration lives at the field level
/// of the source, not the predicate level. Returns false for any
/// expression-shaped operand on either side.
fn pure_range_inputs_presorted(
    graph: &petgraph::graph::DiGraph<
        crate::plan::execution::PlanNode,
        crate::plan::execution::PlanEdge,
    >,
    node_properties: &std::collections::HashMap<
        petgraph::graph::NodeIndex,
        crate::plan::properties::NodeProperties,
    >,
    combine_name: &str,
    range: &RangeConjunct,
) -> bool {
    use petgraph::Direction;

    let Some(left_field) = simple_field_name(&range.left_expr) else {
        return false;
    };
    let Some(right_field) = simple_field_name(&range.right_expr) else {
        return false;
    };

    // Locate the combine node and its incoming neighbors.
    let Some(combine_idx) = graph
        .node_indices()
        .find(|&i| graph[i].name() == combine_name)
    else {
        return false;
    };
    let predecessors: Vec<petgraph::graph::NodeIndex> = graph
        .neighbors_directed(combine_idx, Direction::Incoming)
        .collect();

    // Match each input qualifier to its predecessor by name. The
    // qualifier is the per-input key the user wrote in the YAML
    // `input:` block; the upstream's stored name is what
    // `combine_inputs[qualifier].upstream_name` records. Walk the
    // predecessors and check whichever one carries the field we need.
    let covers = |upstream_idx: petgraph::graph::NodeIndex, field: &str| -> bool {
        let Some(props) = node_properties.get(&upstream_idx) else {
            return false;
        };
        let Some(sort_order) = props.ordering.sort_order.as_ref() else {
            return false;
        };
        sort_order.first().is_some_and(|sf| sf.field == field)
    };

    // For each side, find the predecessor whose name matches the
    // qualifier's `upstream_name`. We don't have the full
    // `combine_inputs` here — read upstream names off the
    // `PlanNode::name()` of each predecessor and match against the
    // range conjunct's qualifier strings. Combine inputs use
    // `qualifier == upstream_name` for plain (non-chain) cases; chain
    // steps fall through and return false (the qualifier is synthetic
    // and the upstream is a synthetic chain step with no source
    // sort_order). That's the correct conservative choice — chain
    // intermediates do not preserve a sort_order anyway.
    let left_input_str = range.left_input.as_ref();
    let right_input_str = range.right_input.as_ref();
    let mut left_covered = false;
    let mut right_covered = false;
    for &pred in &predecessors {
        let name = graph[pred].name();
        if name == left_input_str && covers(pred, &left_field) {
            left_covered = true;
        }
        if name == right_input_str && covers(pred, &right_field) {
            right_covered = true;
        }
    }
    left_covered && right_covered
}

/// Extract a bare field name from a simple `qualifier.field` reference,
/// or `None` for any other expression shape (literal, function call,
/// arithmetic, etc.).
fn simple_field_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::QualifiedFieldRef { parts, .. } if parts.len() == 2 => Some(parts[1].to_string()),
        _ => None,
    }
}

/// E313 — combine `where:` predicate has no extractable equality OR
/// range conjuncts. With neither, no decomposable strategy can run
/// the join short of a non-decomposable fallback (grace hash with a
/// pure-residual filter), which is not yet wired.
fn combine_e313_no_equi(combine_name: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E313",
        format!(
            "combine {combine_name:?} where-clause has no decomposable cross-input \
             comparisons; needs at least one equality or range conjunct"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "add a cross-input comparison (e.g. `a.id == b.id` or `a.t < b.t`); \
         predicates with neither shape have no supported execution strategy yet",
    )
}

/// W305 — pure-range combine predicate. Selects [`CombineStrategy::IEJoin`]
/// (no hash partitioning); IEJoin is correct for any range shape but
/// without an equality key the partitioning that
/// `HashPartitionIEJoin` provides cannot apply, so very large inputs
/// stay slower than equi-joins.
fn combine_w305_pure_range(combine_name: &str, span: Span) -> Diagnostic {
    Diagnostic::warning(
        "W305",
        format!(
            "combine {combine_name:?} has no equality conjuncts; selecting `IEJoin` \
             — performance may be slower than an equi-join. Add a cross-input \
             equality (e.g. `a.id == b.id`) to enable hash partitioning."
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "IEJoin is `O(N log N + M)` in the matched-pair count; without an equality \
         key the planner cannot partition inputs, so the comparison runs across \
         every record pair",
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
///      (planner cannot determine optimal driver) so authors notice
///      missing cardinality estimates on the original N-ary combine
///      before [`decompose_nary_combines`] runs.
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
/// for combines with 3+ inputs that lack cardinality estimates. Fires
/// on the original N-ary combine before decomposition runs, surfacing
/// missing cardinality data while the user-authored shape is still
/// visible.
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

// ─── N-ary plan-time decomposition ───────────────────────────────────
//
// Authors may declare a combine with N > 2 inputs:
//
//   - type: combine
//     name: enriched
//     input:
//       orders: orders
//       products: products
//       categories: categories
//     config:
//       where: orders.product_id == products.product_id
//              and products.product_id == categories.product_id
//
// The executor's combine arm operates on binary nodes. Rather than ship
// a separate N-ary kernel, the planner rewrites every N > 2 combine
// into a chain of (N - 1) binary combines before strategy selection
// runs. Each step joins the running intermediate against one new
// build-side input; the predicate is sliced into the conjuncts whose
// qualifiers are all available at that step.
//
// Ordering is predicate-aware greedy (GOO): from the driver, accept
// the connected partner with the smallest cardinality estimate, breaking
// ties by declaration order. Cardinality-only ordering can pick pairs
// that share no conjunct, forcing a Cartesian product between
// disconnected components — checking connectivity first and prioritizing
// connected pairs eliminates that failure mode.
//
// The chain is left-deep: the final step adopts the user-authored
// `match_mode` and `on_miss`; intermediate steps emit every match and
// drop misses so the cardinality contract of the original N-ary join
// survives the rewrite.

/// Maximum number of inputs the decomposition pass accepts on a single
/// combine node. Beyond this, a chain of (N-1) binary combines becomes
/// hard to reason about for plan stability and memory accounting; the
/// planner emits an E300 with the cap surfaced in the diagnostic and
/// skips the rewrite.
pub(crate) const MAX_COMBINE_INPUTS: usize = 8;

/// Encode a `(qualifier, field)` pair into a flat schema column name for
/// chain-intermediate combine outputs.
///
/// A non-final step in an N-ary decomposition chain emits records whose
/// schema is the union of the joined inputs' fields. Original qualifiers
/// (`orders`, `products`, …) are not preserved on a flat
/// `clinker_record::Schema` — every column is a single bare string —
/// so the decomposition encodes each `(qualifier, field)` pair as
/// `__{qualifier}__{field}` and routes the next step's predicate /
/// body through the executor's qualifier-aware resolver to find them
/// again. The double-underscore prefix distinguishes encoded columns
/// from author-declared field names which never legally start with
/// `__` (CXL identifiers begin with `[A-Za-z_]` but pipeline-author
/// fields by convention do not lead with double-underscore).
pub(crate) fn encode_chain_column(qualifier: &str, name: &str) -> String {
    format!("__{qualifier}__{name}")
}

/// One link in an N-ary combine's decomposition chain.
///
/// `name` is the synthetic node name `__combine_{original}_step_{i}`;
/// `driving_input` carries the previous step's synthetic name (for
/// `i >= 1`) or the original driver qualifier (for `i == 0`); `build_input`
/// is the qualifier joined at this step. `intermediate_row` is the
/// merged row column-pruned to the qualifiers joined so far —
/// downstream compile/runtime work that needs the row at this step
/// reads it without walking the original merged row again.
/// `predicate_slice` carries the conjuncts whose left+right qualifiers
/// are both joined at this step.
pub(crate) struct BinaryDecomposeStep {
    pub(crate) name: String,
    pub(crate) build_input: String,
    pub(crate) driving_input: String,
    pub(crate) intermediate_row: Row,
    pub(crate) predicate_slice: DecomposedPredicate,
}

/// Returns true iff the conjunct's left/right qualifiers are both
/// elements of `joined`.
fn equality_in_joined(eq: &EqualityConjunct, joined: &HashSet<String>) -> bool {
    joined.contains(eq.left_input.as_ref()) && joined.contains(eq.right_input.as_ref())
}

fn range_in_joined(r: &RangeConjunct, joined: &HashSet<String>) -> bool {
    joined.contains(r.left_input.as_ref()) && joined.contains(r.right_input.as_ref())
}

/// Count predicate conjuncts whose qualifiers are both inside the
/// `(joined ∪ {candidate})` set. Used by the GOO partner selector.
fn connected_conjuncts(
    predicate: &DecomposedPredicate,
    joined: &HashSet<String>,
    candidate: &str,
) -> usize {
    let mut frontier = joined.clone();
    frontier.insert(candidate.to_string());
    let eq = predicate
        .equalities
        .iter()
        .filter(|c| equality_in_joined(c, &frontier))
        .count();
    let rg = predicate
        .ranges
        .iter()
        .filter(|c| range_in_joined(c, &frontier))
        .count();
    eq + rg
}

/// Predicate-aware greedy decomposition of an N-ary combine into N-1
/// binary steps.
///
/// At each step, every still-unjoined input is scored by the count of
/// predicate conjuncts that connect it to the already-joined frontier.
/// Inputs with zero connecting conjuncts are skipped — selecting one
/// would force a Cartesian product. From the connected candidates the
/// planner picks the one with the smallest `estimated_cardinality`,
/// breaking ties by `IndexMap` declaration order so the result is
/// deterministic across runs.
///
/// Returns E305 (disconnected join graph) when no unjoined input is
/// connected to the joined frontier — equivalent to a missing
/// transitive edge between two components of the predicate graph.
pub(crate) fn decompose_nary_combine(
    original_name: &str,
    inputs: &IndexMap<String, CombineInput>,
    predicate: &DecomposedPredicate,
    full_merged_row: &Row,
    driving: &str,
    span: Span,
) -> Result<Vec<BinaryDecomposeStep>, Box<Diagnostic>> {
    let total = inputs.len();
    debug_assert!(
        total > 2,
        "decompose_nary_combine called with {total} inputs; binary combines do not need decomposition"
    );

    let mut joined: HashSet<String> = HashSet::new();
    joined.insert(driving.to_string());
    let mut current_intermediate = driving.to_string();
    // Conjuncts already assigned to a step. Indexed by position in the
    // parent decomposition's `equalities` / `ranges` Vec so the same
    // conjunct never lands on two steps.
    let mut assigned_eq: HashSet<usize> = HashSet::new();
    let mut assigned_range: HashSet<usize> = HashSet::new();
    let mut steps = Vec::with_capacity(total - 1);

    for step_idx in 0..(total - 1) {
        let mut best: Option<(&str, usize, Option<u64>, usize)> = None;
        for (decl_pos, (qual, ci)) in inputs.iter().enumerate() {
            if joined.contains(qual) {
                continue;
            }
            let connected = connected_conjuncts(predicate, &joined, qual);
            if connected == 0 {
                continue;
            }
            let card = ci.estimated_cardinality;
            let candidate = (qual.as_str(), decl_pos, card, connected);
            best = Some(match best {
                None => candidate,
                Some(prev) => {
                    let (_, prev_decl, prev_card, _) = prev;
                    let prefer_candidate = match (card, prev_card) {
                        (Some(a), Some(b)) if a != b => a < b,
                        _ => decl_pos < prev_decl,
                    };
                    if prefer_candidate { candidate } else { prev }
                }
            });
        }

        let picked = match best {
            Some((q, _, _, _)) => q.to_string(),
            None => return Err(Box::new(combine_e305_disconnected(original_name, span))),
        };

        // Slice the predicate to conjuncts now fully available. A
        // conjunct is "now fully available" iff both its qualifiers are
        // in `joined ∪ {picked}` AND it has not yet been assigned to a
        // prior step.
        let mut frontier = joined.clone();
        frontier.insert(picked.clone());
        let mut equalities = Vec::new();
        for (i, eq) in predicate.equalities.iter().enumerate() {
            if assigned_eq.contains(&i) {
                continue;
            }
            if equality_in_joined(eq, &frontier) {
                equalities.push(eq.clone());
                assigned_eq.insert(i);
            }
        }
        let mut ranges = Vec::new();
        for (i, r) in predicate.ranges.iter().enumerate() {
            if assigned_range.contains(&i) {
                continue;
            }
            if range_in_joined(r, &frontier) {
                ranges.push(r.clone());
                assigned_range.insert(i);
            }
        }
        // When the slice has range conjuncts but no equality program
        // to source NodeIds from, the IEJoin executor falls back to
        // `slice.residual` for the typed-program reference its range
        // KeyExtractor needs. The original predicate's residual covers
        // every range NodeId because `decompose_predicate` folds every
        // range conjunct into the residual program at the original
        // merged-row scope. Threading that same `Arc` through here
        // satisfies the IEJoin invariant without re-typechecking;
        // when the slice has equalities, the equality conjunct's
        // `left_program` provides the same coverage and the residual
        // is unused.
        let slice_residual = if !ranges.is_empty() && equalities.is_empty() {
            predicate.residual.as_ref().map(Arc::clone)
        } else {
            None
        };
        let predicate_slice = DecomposedPredicate {
            equalities,
            ranges,
            residual: slice_residual,
        };

        // Column-prune the merged row to fields whose qualifier is in
        // the joined ∪ {picked} set. Preserve the merged row's
        // `RowTail` so an open parent (composition boundary) stays
        // open, even though combine-merged rows are closed in practice.
        let mut declared: IndexMap<QualifiedField, Type> = IndexMap::new();
        for (qf, ty) in full_merged_row.fields() {
            if let Some(qualifier) = qf.qualifier.as_deref()
                && frontier.contains(qualifier)
            {
                declared.insert(qf.clone(), ty.clone());
            }
        }
        let intermediate_row = Row::from_parts(
            declared,
            full_merged_row.declared_span,
            full_merged_row.tail.clone(),
        );

        let step_name = format!("__combine_{original_name}_step_{step_idx}");
        steps.push(BinaryDecomposeStep {
            name: step_name.clone(),
            build_input: picked.clone(),
            driving_input: current_intermediate,
            intermediate_row,
            predicate_slice,
        });
        joined.insert(picked);
        current_intermediate = step_name;
    }

    Ok(steps)
}

/// Validate that the join graph induced by `predicate` connects every
/// declared input. The graph has one node per qualifier and an edge
/// `(left, right)` per cross-input conjunct (equality or range).
///
/// Single-input predicates contribute no edges: a same-input
/// `(a.x == a.y)` conjunct never appears here because such conjuncts
/// classify as residual at decomposition time and the residual carries
/// no `left_input`/`right_input` pair.
///
/// Returns E305 when the graph is disconnected — joining the inputs
/// would produce a Cartesian product across components.
pub(crate) fn validate_join_graph_connectivity(
    inputs: &IndexMap<String, CombineInput>,
    predicate: &DecomposedPredicate,
    combine_name: &str,
    span: Span,
) -> Result<(), Box<Diagnostic>> {
    if inputs.len() <= 1 {
        return Ok(());
    }
    // Union-find over qualifier indices. `parent[i]` points at i's
    // representative; flat after `find` path-compression.
    let qualifiers: Vec<&str> = inputs.keys().map(String::as_str).collect();
    let mut parent: Vec<usize> = (0..qualifiers.len()).collect();
    fn find(parent: &mut [usize], i: usize) -> usize {
        let mut root = i;
        while parent[root] != root {
            root = parent[root];
        }
        let mut cur = i;
        while parent[cur] != root {
            let next = parent[cur];
            parent[cur] = root;
            cur = next;
        }
        root
    }
    fn union(parent: &mut [usize], a: usize, b: usize) {
        let ra = find(parent, a);
        let rb = find(parent, b);
        if ra != rb {
            parent[ra] = rb;
        }
    }
    let index_of = |q: &str| qualifiers.iter().position(|x| *x == q);
    for eq in &predicate.equalities {
        if eq.left_input.as_ref() == eq.right_input.as_ref() {
            continue;
        }
        if let (Some(li), Some(ri)) = (
            index_of(eq.left_input.as_ref()),
            index_of(eq.right_input.as_ref()),
        ) {
            union(&mut parent, li, ri);
        }
    }
    for r in &predicate.ranges {
        if r.left_input.as_ref() == r.right_input.as_ref() {
            continue;
        }
        if let (Some(li), Some(ri)) = (
            index_of(r.left_input.as_ref()),
            index_of(r.right_input.as_ref()),
        ) {
            union(&mut parent, li, ri);
        }
    }
    let root0 = find(&mut parent, 0);
    let all_connected = (1..qualifiers.len()).all(|i| find(&mut parent, i) == root0);
    if all_connected {
        Ok(())
    } else {
        Err(Box::new(combine_e305_disconnected(combine_name, span)))
    }
}

/// E305 — combine where-clause forms a disconnected join graph. Two or
/// more groups of inputs share no cross-input comparison; joining them
/// would produce a Cartesian product across components.
fn combine_e305_disconnected(combine_name: &str, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E305",
        format!(
            "combine {combine_name:?} where-clause forms a disconnected join graph; \
             inputs in different components would produce a Cartesian product"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "add a cross-input comparison that links all inputs (e.g. via a transitive \
         equality chain)",
    )
}

/// E300 — combine input count exceeds the supported maximum. Sibling
/// constructor to `combine_e300` (in `bind_schema.rs`) which fires for
/// the lower bound (`< 2`); this fires for the upper bound after the
/// decomposition pass has the fully-bound input map. Same code so
/// authors see one diagnostic family for "input-count out of bounds".
fn combine_e300_too_many_inputs(combine_name: &str, count: usize, span: Span) -> Diagnostic {
    Diagnostic::error(
        "E300",
        format!(
            "combine {combine_name:?} declares {count} inputs, maximum supported is \
             {MAX_COMBINE_INPUTS}"
        ),
        LabeledSpan::primary(span, String::new()),
    )
    .with_help(
        "split the combine into two or more chained combine nodes; each chained \
         combine joins one additional input against the running intermediate",
    )
}

/// Plan-time post-pass that decomposes every N-ary `PlanNode::Combine`
/// (input count > 2) into a left-deep chain of (N-1) binary
/// `PlanNode::Combine` nodes. Runs before
/// [`select_combine_strategies`] so strategy selection sees only
/// binary nodes.
///
/// The original combine's `NodeIndex` is reused as the final step in
/// the chain — its outgoing edges, span, output_schema, and
/// authored `match_mode`/`on_miss` survive in place. Earlier steps
/// are added as new nodes with synthetic names; they default to
/// `MatchMode::All` + `OnMiss::Drop` so intermediate cardinality
/// matches the user-authored N-ary shape.
///
/// Diagnostics:
///   - E300 — `inputs.len() > MAX_COMBINE_INPUTS`. Decomposition is
///     skipped; the original N-ary node remains in the graph and the
///     downstream strategy pass also skips it (no driver is stamped
///     because [`combine_driving`](CompileArtifacts::combine_driving)
///     was populated normally but `select_combine_strategies` will
///     leave the strategy/driver fields unset until the user reduces
///     input count).
///   - E305 — disconnected join graph. Decomposition is skipped; the
///     diagnostic surfaces the missing transitive comparison.
pub(crate) fn decompose_nary_combines(
    plan: &mut crate::plan::execution::ExecutionPlanDag,
    artifacts: &mut crate::plan::bind_schema::CompileArtifacts,
    diags: &mut Vec<Diagnostic>,
) {
    use crate::config::pipeline_node::{MatchMode, OnMiss};
    use crate::plan::execution::{PlanEdge, PlanNode};
    use petgraph::Direction;
    use petgraph::graph::NodeIndex;
    use petgraph::visit::EdgeRef;

    // Snapshot every N-ary combine before any structural change, so
    // adding new nodes during the pass can't cause us to revisit
    // synthetic step nodes (which all have inputs.len() == 2 anyway,
    // but the snapshot is the simpler invariant to reason about).
    let nary: Vec<(NodeIndex, String, Span)> = plan
        .graph
        .node_indices()
        .filter_map(|idx| match &plan.graph[idx] {
            PlanNode::Combine { name, span, .. } => {
                let count = artifacts
                    .combine_inputs
                    .get(name)
                    .map(IndexMap::len)
                    .unwrap_or(0);
                if count > 2 {
                    Some((idx, name.clone(), *span))
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect();

    for (original_idx, original_name, span) in nary {
        let inputs = match artifacts.combine_inputs.get(&original_name) {
            Some(i) => i.clone(),
            None => continue,
        };
        if inputs.len() > MAX_COMBINE_INPUTS {
            diags.push(combine_e300_too_many_inputs(
                &original_name,
                inputs.len(),
                span,
            ));
            continue;
        }
        let predicate = match artifacts.combine_predicates.get(&original_name) {
            Some(p) => p.clone(),
            None => continue,
        };
        if let Err(d) = validate_join_graph_connectivity(&inputs, &predicate, &original_name, span)
        {
            diags.push(*d);
            continue;
        }
        let driving = match artifacts.combine_driving.get(&original_name) {
            Some(d) => d.clone(),
            None => continue,
        };

        // Reconstruct the merged row from the per-input rows. This is
        // the same shape `bind_combine` builds; we don't keep the
        // merged row in artifacts, so we rematerialize it here.
        let mut merged_declared: IndexMap<QualifiedField, Type> = IndexMap::new();
        for (qualifier, ci) in &inputs {
            for (qf, ty) in ci.row.fields() {
                let qualified = QualifiedField::qualified(qualifier.as_str(), qf.name.clone());
                merged_declared.insert(qualified, ty.clone());
            }
        }
        let merged_row = Row::closed(
            merged_declared,
            cxl::lexer::Span::new(span.start as usize, span.start as usize),
        );

        let steps = match decompose_nary_combine(
            &original_name,
            &inputs,
            &predicate,
            &merged_row,
            &driving,
            span,
        ) {
            Ok(s) => s,
            Err(d) => {
                diags.push(*d);
                continue;
            }
        };

        // Capture original combine details before any mutation. The
        // snapshot above filtered to `PlanNode::Combine`; an earlier
        // pass running between the snapshot and here that mutated this
        // index away from `Combine` would invalidate the snapshot's
        // assumption — skip and continue rather than panic.
        let PlanNode::Combine {
            match_mode: original_match_mode,
            on_miss: original_on_miss,
            output_schema: original_output_schema,
            ..
        } = plan.graph[original_idx].clone()
        else {
            continue;
        };

        // Migrate the original combine's body typed program (if any)
        // and resolved column map from the original-name slot to the
        // final step's name. The body's `QualifiedFieldRef`s reference
        // original qualifiers (`orders`, `products`, …); the final
        // step's resolved column map is rebuilt below to map those
        // through the chain.
        let original_body_typed = artifacts.typed.remove(&original_name);
        // Drop the original combine's resolved column map — we rebuild
        // per-step maps below, and the final step's map differs from
        // the original because chain qualifiers route through Probe at
        // encoded indices rather than directly to the original sources.
        artifacts.combine_resolved_columns.remove(&original_name);

        // Build a name → NodeIndex lookup over the current graph for
        // wiring source-edges. Snapshot here so it captures the
        // pre-mutation state; new step nodes get added to the local
        // map as we create them.
        let mut node_by_name: HashMap<String, NodeIndex> = HashMap::new();
        for idx in plan.graph.node_indices() {
            node_by_name.insert(plan.graph[idx].name().to_string(), idx);
        }

        // Original combine's incoming edges go away — each step adds
        // its own. Capture targets first, then delete.
        let incoming_edges: Vec<petgraph::graph::EdgeIndex> = plan
            .graph
            .edges_directed(original_idx, Direction::Incoming)
            .map(|e| e.id())
            .collect();
        for eid in incoming_edges {
            plan.graph.remove_edge(eid);
        }

        // Compute each non-final step's encoded output `Arc<Schema>`.
        // Column order matches `intermediate_row.fields()` iteration
        // order; the i-th encoded column corresponds to the i-th
        // qualified field in the running merged-row prefix. This
        // alignment is the load-bearing invariant for resolved column
        // map indices below — the encoded schema column at index `j`
        // holds the value for the i-th joined `(qualifier, field)`,
        // and `j == position_in_intermediate_row.fields()`.
        let last_step_idx = steps.len() - 1;
        let mut step_output_schemas: Vec<Arc<clinker_record::Schema>> =
            Vec::with_capacity(steps.len());
        for (i, step) in steps.iter().enumerate() {
            let schema = if i == last_step_idx {
                Arc::clone(&original_output_schema)
            } else {
                let mut builder = clinker_record::SchemaBuilder::new();
                for (qf, _ty) in step.intermediate_row.fields() {
                    let qualifier = qf
                        .qualifier
                        .as_deref()
                        .expect("intermediate_row carries qualified fields by construction");
                    builder = builder.with_field(encode_chain_column(qualifier, qf.name.as_ref()));
                }
                builder.build()
            };
            step_output_schemas.push(schema);
        }

        // Build per-step resolved column maps. Keyed by ORIGINAL
        // qualifier + field name; values are `(JoinSide, idx)`. The
        // index for `Probe` is the column position in the driver
        // record's schema (encoded for steps ≥ 1, native for step 0);
        // the index for `Build` is the column position in the build
        // input's source schema. The `CombineResolver` consumes this
        // map at runtime via
        // `CombineResolverMapping::from_pre_resolved`.
        let mut step_resolved_maps: Vec<
            HashMap<QualifiedField, (crate::executor::combine::JoinSide, u32)>,
        > = Vec::with_capacity(steps.len());
        for (i, step) in steps.iter().enumerate() {
            let mut step_map: HashMap<QualifiedField, (crate::executor::combine::JoinSide, u32)> =
                HashMap::new();
            // Probe-side qualifiers: every original qualifier that's
            // already in the joined chain at the start of this step.
            // For step 0 the chain has only the original driving
            // qualifier; the driver record is that source's row, so
            // probe idx = position in the driver source's row fields
            // (which are bare QualifiedFields). For step ≥ 1 the
            // chain holds every qualifier joined in steps `[0..i)`;
            // probe idx = position in step[i-1].intermediate_row's
            // fields iteration (= position in the encoded schema).
            if i == 0 {
                let driver_input = inputs
                    .get(&driving)
                    .expect("driver qualifier present in combine_inputs");
                for (idx, (qf, _ty)) in driver_input.row.fields().enumerate() {
                    let resolve_qual = qf.qualifier.as_deref().unwrap_or(driving.as_str());
                    let key = QualifiedField::qualified(resolve_qual, qf.name.clone());
                    step_map.insert(key, (crate::executor::combine::JoinSide::Probe, idx as u32));
                }
            } else {
                let prev_intermediate = &steps[i - 1].intermediate_row;
                for (idx, (qf, _ty)) in prev_intermediate.fields().enumerate() {
                    let resolve_qual = qf
                        .qualifier
                        .as_deref()
                        .expect("intermediate_row fields are qualified by construction");
                    let key = QualifiedField::qualified(resolve_qual, qf.name.clone());
                    step_map.insert(key, (crate::executor::combine::JoinSide::Probe, idx as u32));
                }
            }
            // Build-side qualifier: the single input newly joined at
            // this step. Build idx = position in that source's row
            // fields (bare QualifiedFields). We key the resolved map
            // using `step.build_input` as the original qualifier so
            // the conjunct's `QualifiedFieldRef("c", "id")` matches.
            let build_input_meta = inputs
                .get(&step.build_input)
                .expect("step.build_input qualifier was selected from inputs.keys()");
            for (idx, (qf, _ty)) in build_input_meta.row.fields().enumerate() {
                let key = QualifiedField::qualified(step.build_input.as_str(), qf.name.clone());
                step_map.insert(key, (crate::executor::combine::JoinSide::Build, idx as u32));
            }
            step_resolved_maps.push(step_map);
        }

        // Add (N - 2) earlier steps as new nodes. Step (N - 2) reuses
        // `original_idx`. Each step gets its own CompileArtifacts entries.
        let mut step_indices: Vec<NodeIndex> = Vec::with_capacity(steps.len());
        for (i, step) in steps.iter().enumerate() {
            let predicate_summary = CombinePredicateSummary::from_decomposed(&step.predicate_slice);
            let step_output_schema = Arc::clone(&step_output_schemas[i]);
            let step_resolved_map: crate::plan::execution::ResolvedColumnMap =
                Arc::new(step_resolved_maps[i].clone());
            let (match_mode, on_miss, output_schema, idx) = if i == last_step_idx {
                (
                    original_match_mode,
                    original_on_miss,
                    step_output_schema.clone(),
                    original_idx,
                )
            } else {
                (
                    MatchMode::All,
                    OnMiss::Skip,
                    step_output_schema.clone(),
                    plan.graph.add_node(PlanNode::Combine {
                        name: step.name.clone(),
                        span,
                        strategy: CombineStrategy::HashBuildProbe,
                        driving_input: String::new(),
                        build_inputs: Vec::new(),
                        predicate_summary,
                        match_mode: MatchMode::All,
                        on_miss: OnMiss::Skip,
                        decomposed_from: Some(original_name.clone()),
                        output_schema: Arc::clone(&step_output_schema),
                        resolved_column_map: Arc::clone(&step_resolved_map),
                    }),
                )
            };
            if i == last_step_idx {
                // Mutate the original combine in place. Name shifts to
                // the synthetic step name; metadata becomes the final
                // step's; outgoing edges already point at downstream
                // consumers.
                if let PlanNode::Combine {
                    name,
                    strategy,
                    driving_input,
                    build_inputs,
                    predicate_summary: ps,
                    match_mode: mm,
                    on_miss: om,
                    decomposed_from,
                    output_schema: os,
                    resolved_column_map,
                    ..
                } = &mut plan.graph[original_idx]
                {
                    *name = step.name.clone();
                    *strategy = CombineStrategy::HashBuildProbe;
                    *driving_input = String::new();
                    *build_inputs = Vec::new();
                    *ps = predicate_summary;
                    *mm = match_mode;
                    *om = on_miss;
                    *decomposed_from = Some(original_name.clone());
                    *os = output_schema;
                    *resolved_column_map = Arc::clone(&step_resolved_map);
                }
            }
            step_indices.push(idx);
            node_by_name.insert(step.name.clone(), idx);
        }

        // Wire edges. Step 0 ← driver source + first build source.
        // Step i ≥ 1 ← step (i-1) + step's build source.
        for (i, step) in steps.iter().enumerate() {
            let driver_idx = if i == 0 {
                let original_driver_input = inputs
                    .get(&driving)
                    .expect("driver qualifier present in combine_inputs");
                node_by_name
                    .get(original_driver_input.upstream_name.as_ref())
                    .copied()
            } else {
                Some(step_indices[i - 1])
            };
            let build_input_meta = inputs
                .get(&step.build_input)
                .expect("step.build_input was selected from inputs.keys()");
            let build_idx = node_by_name
                .get(build_input_meta.upstream_name.as_ref())
                .copied();
            let target = step_indices[i];
            if let Some(d) = driver_idx {
                plan.graph.add_edge(
                    d,
                    target,
                    PlanEdge {
                        dependency_type: DependencyType::Data,
                    },
                );
            }
            if let Some(b) = build_idx {
                plan.graph.add_edge(
                    b,
                    target,
                    PlanEdge {
                        dependency_type: DependencyType::Data,
                    },
                );
            }
        }

        // Populate CompileArtifacts for each step.
        for (i, step) in steps.iter().enumerate() {
            // Per-step inputs map: driver row + build row. The driver
            // entry for step ≥ 1 carries the previous step's
            // intermediate_row (with qualified fields keyed by
            // original qualifiers); the executor's resolver uses each
            // QualifiedField's INNER qualifier to match against the
            // resolved column map, so the outer IndexMap key (the
            // synthetic step name) does not need to align with body
            // references.
            let driver_row = if i == 0 {
                inputs
                    .get(&driving)
                    .map(|ci| ci.row.clone())
                    .expect("driver qualifier present in combine_inputs")
            } else {
                steps[i - 1].intermediate_row.clone()
            };
            let driver_upstream: Arc<str> = if i == 0 {
                Arc::clone(&inputs.get(&driving).unwrap().upstream_name)
            } else {
                Arc::from(steps[i - 1].name.as_str())
            };
            let build_meta = inputs
                .get(&step.build_input)
                .expect("step.build_input qualifier was selected from inputs.keys()");

            let mut step_inputs: IndexMap<String, CombineInput> = IndexMap::new();
            step_inputs.insert(
                step.driving_input.clone(),
                CombineInput {
                    upstream_name: driver_upstream,
                    row: driver_row,
                    estimated_cardinality: None,
                },
            );
            step_inputs.insert(
                step.build_input.clone(),
                CombineInput {
                    upstream_name: Arc::clone(&build_meta.upstream_name),
                    row: build_meta.row.clone(),
                    estimated_cardinality: build_meta.estimated_cardinality,
                },
            );

            artifacts
                .combine_inputs
                .insert(step.name.clone(), step_inputs);
            artifacts
                .combine_predicates
                .insert(step.name.clone(), step.predicate_slice.clone());
            artifacts
                .combine_driving
                .insert(step.name.clone(), step.driving_input.clone());
            artifacts
                .combine_resolved_columns
                .insert(step.name.clone(), Arc::new(step_resolved_maps[i].clone()));

            // Propagate the user's strategy hint to every chain step.
            // The hint applies to the join semantics (pure-equi grace
            // hash), and a left-deep chain of pure-equi joins routes
            // every step through the same strategy when the user asked
            // for it.
            if let Some(hint) = artifacts
                .combine_strategy_hints
                .get(&original_name)
                .copied()
            {
                artifacts
                    .combine_strategy_hints
                    .insert(step.name.clone(), hint);
            }
        }

        // The final step inherits the original combine's body typed
        // program (if any). The body's QualifiedFieldRefs against the
        // original qualifiers route through the final step's resolved
        // column map at runtime — chain-buried qualifiers map to
        // (Probe, encoded_idx); the newly joined qualifier maps to
        // (Build, native_idx).
        if let Some(body) = original_body_typed {
            let final_step_name = &steps[last_step_idx].name;
            artifacts.typed.insert(final_step_name.clone(), body);
        }

        // Drop the original combine's artifacts — its name is gone from
        // the graph (the index now hosts the final step under a
        // synthetic name).
        artifacts.combine_inputs.remove(&original_name);
        artifacts.combine_predicates.remove(&original_name);
        artifacts.combine_driving.remove(&original_name);
        artifacts.combine_strategy_hints.remove(&original_name);
    }
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

        // Exercise all 7 variants so the test fails if any variant is
        // accidentally made non-serializable.
        let _ = serde_json::to_string(&CombineStrategy::InMemoryHash).unwrap();
        let _ = serde_json::to_string(&CombineStrategy::HashPartitionIEJoin { partition_bits: 10 })
            .unwrap();
        let _ = serde_json::to_string(&CombineStrategy::IEJoin).unwrap();
        let _ = serde_json::to_string(&CombineStrategy::SortMerge).unwrap();
        let _ = serde_json::to_string(&CombineStrategy::BlockNestedLoop).unwrap();
    }

    #[test]
    fn test_combine_strategy_iejoin_serialize() {
        // Pure-range IEJoin serializes as the bare snake_case tag.
        let json = serde_json::to_string(&CombineStrategy::IEJoin).unwrap();
        assert_eq!(json, r#""iejoin""#);

        // HashPartitionIEJoin serializes as a tagged object with
        // partition_bits (an externally-tagged adjacent-content shape
        // matching every other strategy that carries fields).
        let json =
            serde_json::to_string(&CombineStrategy::HashPartitionIEJoin { partition_bits: 8 })
                .unwrap();
        assert!(json.contains("hash_partition_iejoin"));
        assert!(json.contains("\"partition_bits\":8"));
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
        select_combine_strategies(&mut plan, &artifacts, &mut diags, None);
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
    fn test_combine_strategy_pure_range_w305_iejoin() {
        // Pure range → W305 + IEJoin strategy.
        let (mut plan, artifacts) =
            synthetic_combine_plan(vec![("orders", None), ("products", None)], 0, 1, true);
        let mut diags = Vec::new();
        select_combine_strategies(&mut plan, &artifacts, &mut diags, None);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code, "W305");
        if let crate::plan::execution::PlanNode::Combine { strategy, .. } = first_combine(&plan) {
            assert!(matches!(strategy, CombineStrategy::IEJoin));
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_combine_strategy_non_decomposable_e313() {
        // Predicate with neither equality nor range conjuncts (e.g.
        // pure residual `or` clause) — still emits E313.
        let (mut plan, artifacts) =
            synthetic_combine_plan(vec![("orders", None), ("products", None)], 0, 0, true);
        let mut diags = Vec::new();
        select_combine_strategies(&mut plan, &artifacts, &mut diags, None);
        assert_eq!(diags.len(), 1);
        assert_eq!(diags[0].code, "E313");
    }

    #[test]
    fn test_combine_strategy_selects_iejoin_for_mixed_equi_range() {
        // Mixed equi+range → HashPartitionIEJoin with default
        // partition_bits=8 when cardinalities are unknown.
        let (mut plan, artifacts) =
            synthetic_combine_plan(vec![("orders", None), ("products", None)], 1, 1, true);
        let mut diags = Vec::new();
        select_combine_strategies(&mut plan, &artifacts, &mut diags, None);
        assert!(diags.is_empty(), "no diagnostics expected; got {diags:?}");
        if let crate::plan::execution::PlanNode::Combine { strategy, .. } = first_combine(&plan) {
            match strategy {
                CombineStrategy::HashPartitionIEJoin { partition_bits } => {
                    assert_eq!(*partition_bits, 8, "default partition_bits is 8");
                }
                other => panic!("expected HashPartitionIEJoin, got {other:?}"),
            }
        } else {
            unreachable!()
        }
    }

    #[test]
    fn test_compute_partition_bits_reduces_for_small_cardinality() {
        // Both inputs estimated; smaller side dominates the heuristic.
        // 100 records, target=64-per-partition → max 1 partition →
        // MIN_BITS=1.
        let (mut plan, artifacts) = synthetic_combine_plan(
            vec![("orders", Some(100)), ("products", Some(500))],
            1,
            1,
            true,
        );
        let mut diags = Vec::new();
        select_combine_strategies(&mut plan, &artifacts, &mut diags, None);
        if let crate::plan::execution::PlanNode::Combine { strategy, .. } = first_combine(&plan) {
            match strategy {
                CombineStrategy::HashPartitionIEJoin { partition_bits } => {
                    assert!(
                        *partition_bits < 8,
                        "small input must reduce partition_bits"
                    );
                }
                other => panic!("expected HashPartitionIEJoin, got {other:?}"),
            }
        }
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
        select_combine_strategies(&mut plan, &artifacts, &mut diags, None);
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
        select_combine_strategies(&mut plan, &artifacts, &mut diags, None);
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

    // ─── N-ary decomposition tests ─────────────────────────────────────

    /// Build an N-ary plan with explicit per-pair equality conjuncts.
    /// `inputs` is `(qualifier, cardinality)`; `edges` is a list of
    /// `(left_qualifier, right_qualifier)` predicate edges. The resulting
    /// `ExecutionPlanDag` contains a single user-authored Combine node
    /// named `test_combine` with no source edges (decomposition does not
    /// require sources to exist) and N source nodes named after the
    /// input qualifiers so edge-rewiring lookups work.
    fn nary_plan(
        combine_name: &str,
        inputs: Vec<(&str, Option<u64>)>,
        edges: Vec<(&str, &str)>,
        driver: &str,
    ) -> (
        crate::plan::execution::ExecutionPlanDag,
        crate::plan::bind_schema::CompileArtifacts,
        Row,
    ) {
        use crate::config::pipeline_node::{MatchMode, OnMiss};
        use crate::plan::bind_schema::CompileArtifacts;
        use crate::plan::execution::{ExecutionPlanDag, PlanNode};
        use crate::span::Span;
        use cxl::ast::{Expr, LiteralValue, NodeId};
        use cxl::lexer::Span as CxlSpan;

        let mut artifacts = CompileArtifacts::default();
        let mut graph = petgraph::graph::DiGraph::new();

        // Add a Source node per input qualifier so name lookups resolve
        // when the decomposition pass wires edges. The Source's
        // `output_schema` is empty; tests don't run records through it.
        for (qual, _card) in &inputs {
            graph.add_node(PlanNode::Source {
                name: (*qual).to_string(),
                span: Span::SYNTHETIC,
                resolved: None,
                output_schema: clinker_record::SchemaBuilder::new().build(),
            });
        }

        // Per-input row: each input declares one bare field "id" so the
        // merged row has unambiguous columns per qualifier.
        let mut inputs_map: IndexMap<String, CombineInput> = IndexMap::new();
        for (qual, card) in &inputs {
            let mut row_fields: IndexMap<QualifiedField, Type> = IndexMap::new();
            row_fields.insert(QualifiedField::bare("id"), Type::String);
            inputs_map.insert(
                (*qual).to_string(),
                CombineInput {
                    upstream_name: Arc::from(*qual),
                    row: Row::closed(row_fields, CxlSpan::new(0, 0)),
                    estimated_cardinality: *card,
                },
            );
        }
        artifacts
            .combine_inputs
            .insert(combine_name.to_string(), inputs_map.clone());
        artifacts
            .combine_driving
            .insert(combine_name.to_string(), driver.to_string());

        // Construct EqualityConjuncts whose left/right inputs match the
        // edges argument. The expressions themselves are dummy literals;
        // decomposition only inspects qualifier identity.
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
        let equalities: Vec<EqualityConjunct> = edges
            .iter()
            .map(|(l, r)| EqualityConjunct {
                left_expr: dummy_lit(),
                left_input: Arc::from(*l),
                left_program: Arc::clone(&dummy_program),
                right_expr: dummy_lit(),
                right_input: Arc::from(*r),
                right_program: Arc::clone(&dummy_program),
            })
            .collect();
        let predicate = DecomposedPredicate {
            equalities,
            ranges: Vec::new(),
            residual: None,
        };
        artifacts
            .combine_predicates
            .insert(combine_name.to_string(), predicate);

        // Build the merged row mirror of bind_combine's logic so the
        // intermediate-row pruning has something to filter against.
        let mut merged: IndexMap<QualifiedField, Type> = IndexMap::new();
        for (qual, _) in &inputs {
            merged.insert(
                QualifiedField::qualified(*qual, Arc::from("id")),
                Type::String,
            );
        }
        let merged_row = Row::closed(merged, CxlSpan::new(0, 0));

        graph.add_node(PlanNode::Combine {
            name: combine_name.to_string(),
            span: Span::SYNTHETIC,
            strategy: CombineStrategy::HashBuildProbe,
            driving_input: String::new(),
            build_inputs: Vec::new(),
            predicate_summary: CombinePredicateSummary::from_decomposed(
                artifacts.combine_predicates.get(combine_name).unwrap(),
            ),
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
            node_properties: std::collections::HashMap::new(),
        };
        (plan, artifacts, merged_row)
    }

    fn collect_combines(
        plan: &crate::plan::execution::ExecutionPlanDag,
    ) -> Vec<&crate::plan::execution::PlanNode> {
        plan.graph
            .node_weights()
            .filter(|n| matches!(n, crate::plan::execution::PlanNode::Combine { .. }))
            .collect()
    }

    #[test]
    fn test_nary_decompose_three_inputs() {
        let (mut plan, mut artifacts, _row) = nary_plan(
            "c",
            vec![("a", None), ("b", None), ("d", None)],
            vec![("a", "b"), ("b", "d")],
            "a",
        );
        let mut diags = Vec::new();
        decompose_nary_combines(&mut plan, &mut artifacts, &mut diags);
        let errors: Vec<&Diagnostic> = diags
            .iter()
            .filter(|d| matches!(d.severity, crate::error::Severity::Error))
            .collect();
        assert!(errors.is_empty(), "no errors expected; got {errors:?}");

        let combines = collect_combines(&plan);
        assert_eq!(
            combines.len(),
            2,
            "3-input decomposition produces N-1 = 2 binary combines"
        );
        for n in combines {
            if let crate::plan::execution::PlanNode::Combine {
                name,
                decomposed_from,
                ..
            } = n
            {
                assert!(
                    name != "c",
                    "original combine name 'c' must be gone post-decomposition"
                );
                assert_eq!(
                    decomposed_from.as_deref(),
                    Some("c"),
                    "synthetic step {name:?} must record its origin combine"
                );
            }
        }
    }

    #[test]
    fn test_nary_decompose_four_inputs() {
        let (mut plan, mut artifacts, _row) = nary_plan(
            "c",
            vec![("a", None), ("b", None), ("d", None), ("e", None)],
            vec![("a", "b"), ("b", "d"), ("d", "e")],
            "a",
        );
        let mut diags = Vec::new();
        decompose_nary_combines(&mut plan, &mut artifacts, &mut diags);
        assert!(
            !diags
                .iter()
                .any(|d| matches!(d.severity, crate::error::Severity::Error)),
            "no errors on a connected 4-input chain"
        );
        let combines = collect_combines(&plan);
        assert_eq!(
            combines.len(),
            3,
            "4-input decomposition produces N-1 = 3 binary combines"
        );
    }

    #[test]
    fn test_nary_greedy_ordering() {
        // Inputs a, b, d, e with predicate edges a—b, b—d, d—e plus a
        // shortcut a—d, where d has the smallest cardinality. From
        // driver `a`, the greedy selector should prefer `d` (connected
        // via the shortcut and lowest cardinality) over `b` (also
        // connected but larger).
        let (mut plan, mut artifacts, _row) = nary_plan(
            "c",
            vec![
                ("a", Some(1000)),
                ("b", Some(500)),
                ("d", Some(50)),
                ("e", Some(800)),
            ],
            vec![("a", "b"), ("b", "d"), ("d", "e"), ("a", "d")],
            "a",
        );
        let mut diags = Vec::new();
        decompose_nary_combines(&mut plan, &mut artifacts, &mut diags);
        // Step 0 joined `a` with the smallest connected partner — `d`.
        // The synthetic step name carries its build qualifier in
        // `combine_inputs[step.name]`, so we look that up here.
        let step0 = artifacts
            .combine_inputs
            .get("__combine_c_step_0")
            .expect("step 0 inserted into combine_inputs");
        assert!(
            step0.contains_key("d"),
            "greedy ordering picks d (smallest cardinality, connected via a—d shortcut); \
             step 0 inputs: {:?}",
            step0.keys().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_nary_e300_exceeds_cap() {
        // 9-input combine — exceeds MAX_COMBINE_INPUTS. The pass emits
        // E300 and skips decomposition; the original node remains.
        let inputs: Vec<(&str, Option<u64>)> = ["a", "b", "c", "d", "e", "f", "g", "h", "i"]
            .iter()
            .map(|s| (*s, None))
            .collect();
        let edges: Vec<(&str, &str)> = (1..inputs.len())
            .map(|i| (inputs[0].0, inputs[i].0))
            .collect();
        let (mut plan, mut artifacts, _row) = nary_plan("c", inputs.clone(), edges, "a");
        let mut diags = Vec::new();
        decompose_nary_combines(&mut plan, &mut artifacts, &mut diags);
        let e300: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == "E300").collect();
        assert_eq!(e300.len(), 1, "expected one E300; got {diags:?}");
        assert!(
            e300[0].message.contains("maximum supported is 8"),
            "E300 message must surface the cap; got {:?}",
            e300[0].message
        );
        let combines = collect_combines(&plan);
        assert_eq!(
            combines.len(),
            1,
            "decomposition is skipped when the cap is exceeded"
        );
    }

    #[test]
    fn test_nary_e305_disconnected_graph() {
        // 4 inputs split into two components: a—b and d—e. No bridge
        // edge → E305. Decomposition is skipped.
        let (mut plan, mut artifacts, _row) = nary_plan(
            "c",
            vec![("a", None), ("b", None), ("d", None), ("e", None)],
            vec![("a", "b"), ("d", "e")],
            "a",
        );
        let mut diags = Vec::new();
        decompose_nary_combines(&mut plan, &mut artifacts, &mut diags);
        let e305: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == "E305").collect();
        assert_eq!(e305.len(), 1, "expected one E305; got {diags:?}");
        let combines = collect_combines(&plan);
        assert_eq!(
            combines.len(),
            1,
            "disconnected graph: no synthetic steps are inserted"
        );
        if let crate::plan::execution::PlanNode::Combine {
            name,
            decomposed_from,
            ..
        } = combines[0]
        {
            assert_eq!(name, "c", "original N-ary node must remain");
            assert!(
                decomposed_from.is_none(),
                "the surviving node is the original, not a synthetic step"
            );
        }
    }

    #[test]
    fn test_nary_intermediate_row_correct() {
        // Direct unit test of `decompose_nary_combine`: step 0's
        // intermediate row contains driver + first build qualifiers
        // only; step 1's adds the second build's qualifiers.
        let (_, _, merged_row) = nary_plan(
            "c",
            vec![("a", Some(100)), ("b", Some(200)), ("d", Some(300))],
            vec![("a", "b"), ("b", "d")],
            "a",
        );
        let mut inputs_map: IndexMap<String, CombineInput> = IndexMap::new();
        for (q, card) in [("a", Some(100u64)), ("b", Some(200)), ("d", Some(300))] {
            let mut row_fields: IndexMap<QualifiedField, Type> = IndexMap::new();
            row_fields.insert(QualifiedField::bare("id"), Type::String);
            inputs_map.insert(
                q.to_string(),
                CombineInput {
                    upstream_name: Arc::from(q),
                    row: Row::closed(row_fields, cxl::lexer::Span::new(0, 0)),
                    estimated_cardinality: card,
                },
            );
        }
        let dummy_lit = || Expr::Literal {
            node_id: NodeId(0),
            value: cxl::ast::LiteralValue::Int(0),
            span: cxl::lexer::Span::new(0, 0),
        };
        let dummy_program = Arc::new(cxl::typecheck::pass::TypedProgram {
            program: cxl::ast::Program {
                statements: Vec::new(),
                span: cxl::lexer::Span::new(0, 0),
            },
            types: Vec::new(),
            bindings: Vec::new(),
            field_types: IndexMap::new(),
            regexes: Vec::new(),
            node_count: 0,
            output_row: cxl::typecheck::row::Row::closed(
                IndexMap::new(),
                cxl::lexer::Span::new(0, 0),
            ),
        });
        let predicate = DecomposedPredicate {
            equalities: vec![
                EqualityConjunct {
                    left_expr: dummy_lit(),
                    left_input: Arc::from("a"),
                    left_program: Arc::clone(&dummy_program),
                    right_expr: dummy_lit(),
                    right_input: Arc::from("b"),
                    right_program: Arc::clone(&dummy_program),
                },
                EqualityConjunct {
                    left_expr: dummy_lit(),
                    left_input: Arc::from("b"),
                    left_program: Arc::clone(&dummy_program),
                    right_expr: dummy_lit(),
                    right_input: Arc::from("d"),
                    right_program: Arc::clone(&dummy_program),
                },
            ],
            ranges: Vec::new(),
            residual: None,
        };
        let steps = decompose_nary_combine(
            "c",
            &inputs_map,
            &predicate,
            &merged_row,
            "a",
            Span::SYNTHETIC,
        )
        .expect("decomposition succeeds on a connected chain");

        let step0_quals: HashSet<&str> = steps[0]
            .intermediate_row
            .fields()
            .filter_map(|(qf, _)| qf.qualifier.as_deref())
            .collect();
        assert!(
            step0_quals.contains("a") && step0_quals.contains("b") && !step0_quals.contains("d"),
            "step 0 intermediate row must contain driver + first build only; got {step0_quals:?}"
        );

        let step1_quals: HashSet<&str> = steps[1]
            .intermediate_row
            .fields()
            .filter_map(|(qf, _)| qf.qualifier.as_deref())
            .collect();
        assert!(
            step1_quals.contains("a") && step1_quals.contains("b") && step1_quals.contains("d"),
            "step 1 intermediate row must contain all joined qualifiers; got {step1_quals:?}"
        );
    }

    #[test]
    fn test_nary_predicate_assignment() {
        // Predicates a—b and b—d. After decomposition, step 0 carries
        // the a—b conjunct; step 1 carries the b—d conjunct. No
        // conjunct lands on two steps.
        let (_, _, merged_row) = nary_plan(
            "c",
            vec![("a", None), ("b", None), ("d", None)],
            vec![("a", "b"), ("b", "d")],
            "a",
        );
        let mut inputs_map: IndexMap<String, CombineInput> = IndexMap::new();
        for q in ["a", "b", "d"] {
            let mut row_fields: IndexMap<QualifiedField, Type> = IndexMap::new();
            row_fields.insert(QualifiedField::bare("id"), Type::String);
            inputs_map.insert(
                q.to_string(),
                CombineInput {
                    upstream_name: Arc::from(q),
                    row: Row::closed(row_fields, cxl::lexer::Span::new(0, 0)),
                    estimated_cardinality: None,
                },
            );
        }
        let dummy_lit = || Expr::Literal {
            node_id: NodeId(0),
            value: cxl::ast::LiteralValue::Int(0),
            span: cxl::lexer::Span::new(0, 0),
        };
        let dummy_program = Arc::new(cxl::typecheck::pass::TypedProgram {
            program: cxl::ast::Program {
                statements: Vec::new(),
                span: cxl::lexer::Span::new(0, 0),
            },
            types: Vec::new(),
            bindings: Vec::new(),
            field_types: IndexMap::new(),
            regexes: Vec::new(),
            node_count: 0,
            output_row: cxl::typecheck::row::Row::closed(
                IndexMap::new(),
                cxl::lexer::Span::new(0, 0),
            ),
        });
        let predicate = DecomposedPredicate {
            equalities: vec![
                EqualityConjunct {
                    left_expr: dummy_lit(),
                    left_input: Arc::from("a"),
                    left_program: Arc::clone(&dummy_program),
                    right_expr: dummy_lit(),
                    right_input: Arc::from("b"),
                    right_program: Arc::clone(&dummy_program),
                },
                EqualityConjunct {
                    left_expr: dummy_lit(),
                    left_input: Arc::from("b"),
                    left_program: Arc::clone(&dummy_program),
                    right_expr: dummy_lit(),
                    right_input: Arc::from("d"),
                    right_program: Arc::clone(&dummy_program),
                },
            ],
            ranges: Vec::new(),
            residual: None,
        };
        let steps = decompose_nary_combine(
            "c",
            &inputs_map,
            &predicate,
            &merged_row,
            "a",
            Span::SYNTHETIC,
        )
        .expect("decomposition succeeds on a connected chain");

        assert_eq!(steps[0].predicate_slice.equalities.len(), 1);
        let step0_eq = &steps[0].predicate_slice.equalities[0];
        assert_eq!(step0_eq.left_input.as_ref(), "a");
        assert_eq!(step0_eq.right_input.as_ref(), "b");

        assert_eq!(steps[1].predicate_slice.equalities.len(), 1);
        let step1_eq = &steps[1].predicate_slice.equalities[0];
        assert_eq!(step1_eq.left_input.as_ref(), "b");
        assert_eq!(step1_eq.right_input.as_ref(), "d");
    }
}
