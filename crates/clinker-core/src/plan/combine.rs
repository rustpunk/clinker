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
//!   - `CompileArtifacts.typed["{name}::where"]` — typed where-clause program
//!   - `CompileArtifacts.typed["{name}::body"]`  — typed output body program
//!   - `CompileArtifacts.combine_predicates`     — `DecomposedPredicate`
//!   - `CompileArtifacts.combine_inputs`         — per-input metadata
//!
//! C.0 only adds the types; C.1 fills predicate decomposition, C.2 adds
//! execution and strategy selection, C.4 adds N-ary decomposition.

use std::sync::Arc;

use indexmap::IndexMap;
use petgraph::graph::NodeIndex;
use serde::Serialize;

use crate::plan::row_type::Row;
use cxl::ast::Expr;
use cxl::typecheck::pass::TypedProgram;

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
/// Populated by C.1 during schema propagation.
#[derive(Debug, Clone)]
pub struct CombineInput {
    pub node_index: NodeIndex,
    pub row: Row,
    pub estimated_cardinality: Option<u64>,
}

// Keep `IndexMap` live as a re-export hint for downstream consumers that
// reach into `CompileArtifacts.combine_inputs`. Doc-only — no API surface.
#[allow(dead_code)]
type _CombineInputMap = IndexMap<String, CombineInput>;

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
