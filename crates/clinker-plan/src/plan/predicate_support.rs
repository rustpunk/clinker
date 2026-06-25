//! Structured per-predicate input-column support.
//!
//! Exposes, for a node's control-flow / grouping predicates, the set of
//! input columns each predicate *reads* — recovered from the node's
//! retained typechecked programs ([`PlanNode::Route`]'s `branch_programs`,
//! [`PlanNode::Cull`]'s decision aggregate) and decomposed `where:`
//! predicate ([`PlanNode::Combine`]'s `decomposed_predicate`) without
//! re-parsing CXL source. Built on [`cxl::ast::Expr::support_into`] /
//! [`cxl::ast::program_support_into`], so the column set matches the
//! engine's authoritative typechecked form — qualified `input.field`
//! references included for Combine.
//!
//! Lineage consumers read this instead of re-parsing each predicate's raw
//! `CxlSource` string. Per-input *value* provenance for Combine/Merge
//! value columns is a separate concern and not covered here.

use std::collections::{BTreeSet, HashSet};

use clinker_record::Schema;

use crate::plan::execution::PlanNode;

/// Input-column support for one node's control-flow / grouping predicates.
///
/// Returned by [`predicate_support`] for the predicate-bearing node kinds;
/// every other node kind yields `None`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PredicateSupport {
    /// One support set per Route branch condition, in `branches`
    /// declaration order (aligned with `PlanNode::Route.branches`).
    /// Columns are bare names — Route predicates run against the single
    /// upstream row.
    RouteBranches(Vec<BTreeSet<String>>),
    /// Columns read by the Combine `where:` join predicate. Qualified
    /// `input.field` where the predicate names a side. Unions the
    /// decomposed equality / range conjuncts and any residual program, so
    /// it is the full read-set of the original `where:` regardless of how
    /// the predicate decomposed.
    CombineWhere(BTreeSet<String>),
    /// Columns read by the Cull OR-combined `drop_group_when` decision
    /// predicate. Bare names — Cull predicates run against the group's
    /// upstream rows. Excludes the synthetic decision-emit target and the
    /// `partition_by` grouping key (which the predicate reads only by
    /// virtue of the group, not by naming it).
    CullDrop(BTreeSet<String>),
}

/// Return the structured input-column support of `node`'s control-flow /
/// grouping predicate(s), or `None` for nodes that carry no such predicate
/// (Transform, Aggregation, Merge, Source, Output, …).
///
/// The support is read from the node's retained typed programs /
/// decomposed predicate / decision aggregate — no CXL re-parse. See
/// [`PredicateSupport`] for the per-kind column-qualification rules. A
/// Combine whose predicate decomposition failed (no `decomposed_predicate`)
/// yields `None`.
pub fn predicate_support(node: &PlanNode) -> Option<PredicateSupport> {
    match node {
        PlanNode::Route {
            branch_programs, ..
        } => {
            let branches = branch_programs
                .iter()
                .map(|program| collect(|set| cxl::ast::program_support_into(&program.program, set)))
                .collect();
            Some(PredicateSupport::RouteBranches(branches))
        }
        PlanNode::Combine {
            decomposed_predicate,
            ..
        } => {
            let decomposed = decomposed_predicate.as_ref()?;
            let support = collect(|set| {
                for eq in &decomposed.equalities {
                    eq.left_expr.support_into(set);
                    eq.right_expr.support_into(set);
                }
                for range in &decomposed.ranges {
                    range.left_expr.support_into(set);
                    range.right_expr.support_into(set);
                }
                if let Some(residual) = &decomposed.residual {
                    cxl::ast::program_support_into(&residual.program, set);
                }
            });
            Some(PredicateSupport::CombineWhere(support))
        }
        PlanNode::Cull {
            compiled,
            typed,
            output_schema,
            ..
        } => {
            // The decision predicate is stored as an extracted aggregate:
            // its aggregated column reads live in the binding args, and the
            // post-extraction `typed` residual references slots, not raw
            // columns. So the predicate's read-set is the union of:
            //   - any direct (non-aggregated) reads surviving in the residual,
            //   - the pre-aggregation row filter, if any, and
            //   - every binding arg.
            // Cull does not widen, so `output_schema` is the decision
            // aggregate's input schema — the index space for simple-field
            // binding args (`sum(amount)` → `BindingArg::Field(idx)`, whose
            // column name is not carried on the binding).
            let support = collect(|set| {
                cxl::ast::program_support_into(&typed.program, set);
                if let Some(filter) = &compiled.pre_agg_filter {
                    filter.support_into(set);
                }
                for binding in &compiled.bindings {
                    accumulate_binding_arg(&binding.arg, output_schema, set);
                }
            });
            Some(PredicateSupport::CullDrop(support))
        }
        _ => None,
    }
}

/// Accumulate the input columns an aggregate binding argument reads.
/// Simple-field args carry only an input-schema index, resolved here
/// through `schema`; composed args carry a full `Expr` walked via
/// [`cxl::ast::Expr::support_into`].
fn accumulate_binding_arg(arg: &cxl::plan::BindingArg, schema: &Schema, set: &mut HashSet<String>) {
    use cxl::plan::BindingArg;
    match arg {
        BindingArg::Field(idx) => {
            if let Some(name) = schema.column_name(*idx as usize) {
                set.insert(name.to_string());
            }
        }
        // `count(*)` reads no specific column.
        BindingArg::Wildcard => {}
        BindingArg::Expr(e) => e.support_into(set),
        BindingArg::Pair(a, b) => {
            accumulate_binding_arg(a, schema, set);
            accumulate_binding_arg(b, schema, set);
        }
    }
}

/// Run `f` to fill a fresh column set, returning it sorted as a
/// `BTreeSet` for deterministic consumer / test output.
fn collect(f: impl FnOnce(&mut HashSet<String>)) -> BTreeSet<String> {
    let mut set = HashSet::new();
    f(&mut set);
    set.into_iter().collect()
}
