//! Plan-compile artifacts for CXL aggregate transforms.
//!
//! This module defines the IR produced by `extract_aggregates` and consumed
//! by the `PlanNode::Aggregation` executor dispatch arm.
//!
//! Design:
//! - Aggregate arguments are pre-classified as `BindingArg` so the hot
//!   loop can dispatch `Field(idx)` in O(1) without re-walking the AST
//!   for the 90% case (`sum(salary)`, `count(*)`). Composed arguments
//!   like `sum(amount * 1.1)` fall through to the `Expr` path and are
//!   evaluated via `ProgramEvaluator`.
//! - Emits carry a post-extraction residual expression where every
//!   `AggCall` has been replaced by `Expr::AggSlot` and every group-by
//!   `FieldRef` by `Expr::GroupKey`. At finalize time the residual is
//!   evaluated in a scope where those leaves resolve against the
//!   group's accumulator row and key tuple.
//! - **NOT serde-derivable.** Prior-art systems (DataFusion, Databend,
//!   Polars, Spark, DuckDB, Trino, ClickHouse, Beam, Flink, NiFi, Kettle,
//!   Informatica) spill **only runtime state** (group keys + accumulator
//!   bytes). The plan lives on the operator that authored the spill and
//!   interprets the bytes on read-back. `CompiledAggregate` is held in
//!   memory behind `Arc` on `PlanNode::Aggregation` with `#[serde(skip)]`;
//!   the spill payload is `[GroupByKey || AccumulatorRow ||
//!   MetadataCommonTracker]`, all of which are serde-derived in
//!   `clinker-record`. Keeping `Expr` serde-free matches DataFusion's
//!   architectural decision (see apache/arrow-datafusion#1832).

use clinker_record::accumulator::AggregateType;

use crate::ast::Expr;

pub mod extract_aggregates;
pub use extract_aggregates::extract_aggregates;

/// How a single aggregate argument is evaluated per input record.
///
/// Per D1: the fast path is a bare field index matching the 90% case
/// (`sum(salary)`, `count(*)`). The general path evaluates an arbitrary
/// CXL expression for composed arguments like `sum(amount * 1.1)`.
#[derive(Debug, Clone)]
pub enum BindingArg {
    /// Fast path: pre-resolved input field index (`sum(salary)`).
    Field(u32),
    /// `count(*)` — no per-row value; the accumulator's `count_all`
    /// path counts calls regardless of field content.
    Wildcard,
    /// General path: compiled expression eval'd per record.
    Expr(Expr),
    /// Two-arg aggregate: `weighted_avg(value, weight)`.
    Pair(Box<BindingArg>, Box<BindingArg>),
}

/// Binding from a CXL aggregate call to a runtime accumulator slot.
///
/// `output_name` is a debug label (e.g. the original call's pretty
/// form) — NOT the output column name, which lives on `CompiledEmit`.
#[derive(Debug, Clone)]
pub struct AggregateBinding {
    pub output_name: Box<str>,
    pub arg: BindingArg,
    pub acc_type: AggregateType,
}

/// One emit statement, post-extraction.
///
/// `residual` is the original emit RHS with every `AggCall` replaced by
/// `Expr::AggSlot { slot }` and every group-by `FieldRef` replaced by
/// `Expr::GroupKey { slot }`. At finalize time the executor evaluates
/// `residual` in a scope where `AggSlot` resolves to
/// `bindings[slot].finalize()` and `GroupKey` resolves to
/// `key[slot].to_value()`.
#[derive(Debug, Clone)]
pub struct CompiledEmit {
    pub output_name: Box<str>,
    pub residual: Expr,
    pub is_meta: bool,
}

/// Full extraction artifact for one aggregate transform.
///
/// Produced by `cxl::plan::extract_aggregates` during plan compile;
/// stored on `PlanNode::Aggregation` as `compiled: Arc<CompiledAggregate>`.
#[derive(Debug, Clone)]
pub struct CompiledAggregate {
    /// Deduplicated accumulator specs. `bindings[i]` corresponds to
    /// `Expr::AggSlot { slot: i as u32 }`.
    pub bindings: Vec<AggregateBinding>,
    /// Input-schema indices for extracting the group key from each
    /// incoming record. `group_by_indices[i]` corresponds to
    /// `Expr::GroupKey { slot: i as u32 }`.
    pub group_by_indices: Vec<u32>,
    /// Group-by field names (needed for spill schema and diagnostics).
    pub group_by_fields: Vec<String>,
    /// Optional pre-aggregation row filter, AND-combined from all
    /// `Statement::Filter` statements in the aggregate CXL (D9).
    pub pre_agg_filter: Option<Expr>,
    /// One per `Statement::Emit` in the CXL (post-extraction residuals).
    pub emits: Vec<CompiledEmit>,
}
