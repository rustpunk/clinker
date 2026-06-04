//! The aggregation error taxonomy.
//!
//! Holds the lowest layer of the aggregation engine: the finalize-time
//! [`AggregateEvalError`], the hot-loop [`HashAggError`], and the
//! `HashAggError → PipelineError` mapping consumed by the executor
//! dispatch arm. Every other aggregation submodule depends on these
//! types, so they live below the hash, spill, and streaming machinery.

use clinker_record::accumulator::AccumulatorError;
use cxl::eval::EvalError;

use crate::error::PipelineError;

/// Errors that can arise while evaluating a residual in aggregate
/// scope.
#[derive(Debug, Clone, thiserror::Error)]
pub enum AggregateEvalError {
    /// `Expr::AggSlot { slot }` referenced a slot index outside the
    /// finalized slot vector. Indicates an extractor / executor
    /// mismatch — should be unreachable in a correctly compiled plan.
    #[error("AggSlot {{ slot: {slot} }} out of range (slot_count: {slot_count})")]
    SlotOutOfRange { slot: u32, slot_count: usize },
    /// `Expr::GroupKey { slot }` referenced a group-key column outside
    /// the key tuple.
    #[error("GroupKey {{ slot: {slot} }} out of range (key_count: {key_count})")]
    GroupKeyOutOfRange { slot: u32, key_count: usize },
    /// A residual contained a construct that the finalize-time
    /// evaluator does not yet support. Unsupported expressions are
    /// reported via this variant rather than panicking.
    #[error("unsupported aggregate residual construct: {what}")]
    UnsupportedResidual { what: &'static str },
}

/// Errors raised by the hash aggregator hot loop. The 16.3.13 dispatch
/// arm wraps these into `PipelineError` for DLQ routing; until then the
/// engine surfaces them via this dedicated enum so the runtime stays
/// decoupled from the executor's error taxonomy.
#[derive(Debug, thiserror::Error)]
pub enum HashAggError {
    /// A `BindingArg::Expr` failed to evaluate against the input record.
    #[error("binding expression eval failed: {0:?}")]
    EvalFailed(#[from] EvalError),
    /// `value_to_group_key` rejected an input value (NaN, unsupported
    /// type, etc.). Carries the field name and row number for routing
    /// in the executor dispatch path.
    #[error("group-key extraction failed for `{field}` at row {row}: {message}")]
    GroupKey {
        field: String,
        row: u64,
        message: String,
    },
    /// Spill path raised an I/O error. Stub until 16.3.10 lands the
    /// real spill writer integration.
    #[error("spill failed: {0}")]
    Spill(String),
    /// An accumulator's `finalize()` failed (e.g. `SumOverflow`). Routed
    /// to `PipelineError::Accumulator` by the 16.3.13 dispatch arm.
    #[error("aggregate {transform}.{binding}: accumulator finalize failed: {source:?}")]
    Accumulator {
        transform: String,
        binding: String,
        source: AccumulatorError,
    },
    /// A post-extraction emit residual failed to evaluate in
    /// `AggregateEvalScope`. Indicates an extractor/executor mismatch or
    /// an unsupported residual construct.
    #[error("aggregate residual eval failed: {0}")]
    Residual(AggregateEvalError),
    /// Streaming aggregation observed an input record whose group-by key
    /// sorts strictly before the currently-open group, violating the
    /// pre-sorted-input invariant of `StreamingAggregator<AddRaw>`. This
    /// is always a hard abort regardless of error strategy — it means
    /// either the upstream ordering contract was wrong or the plan
    /// property pass qualified streaming aggregation on a node whose
    /// output was not actually sorted (DataFusion #12086-class bug).
    /// `GroupBoundary` hands the executor both pre/next encoded keys for
    /// diagnostics. The user-input vs spill-merge distinction is
    /// captured in two separate variants because the two cases route
    /// differently through `From<HashAggError> for PipelineError`.
    #[error(
        "streaming aggregate sort-order violation: prev={prev_key_debug} next={next_key_debug}"
    )]
    SortOrderViolation {
        prev_key_debug: String,
        next_key_debug: String,
    },
    /// Spill-merge produced an out-of-order key — Clinker bug, not a
    /// user data error. Always hard-aborts.
    #[error(
        "spill-merge sort-order violation (Clinker bug): prev={prev_key_debug} next={next_key_debug}"
    )]
    MergeSortOrderViolation {
        prev_key_debug: String,
        next_key_debug: String,
    },
}

/// Map a `HashAggError` to a `PipelineError` for the executor dispatch
/// arm. Accumulator-finalize errors get a dedicated typed variant; the
/// remaining cases are wrapped in `PipelineError::Eval` (data errors) or
/// `PipelineError::Internal` (engine bugs / unsupported residuals).
impl From<HashAggError> for PipelineError {
    fn from(e: HashAggError) -> Self {
        match e {
            HashAggError::EvalFailed(eval) => PipelineError::Eval(eval),
            HashAggError::GroupKey {
                field,
                row,
                message,
            } => PipelineError::Internal {
                op: "aggregation",
                node: String::new(),
                detail: format!("group-key field `{field}` at row {row}: {message}"),
            },
            HashAggError::Spill(msg) => PipelineError::Internal {
                op: "aggregation",
                node: String::new(),
                detail: format!("spill failed: {msg}"),
            },
            HashAggError::Accumulator {
                transform,
                binding,
                source,
            } => PipelineError::Accumulator {
                transform,
                binding,
                source,
            },
            HashAggError::Residual(r) => PipelineError::Internal {
                op: "aggregation",
                node: String::new(),
                detail: format!("aggregate residual eval failed: {r}"),
            },
            HashAggError::SortOrderViolation {
                prev_key_debug,
                next_key_debug,
            } => PipelineError::SortOrderViolation {
                message: format!(
                    "streaming aggregation requires sorted input; prev={prev_key_debug} next={next_key_debug}"
                ),
            },
            HashAggError::MergeSortOrderViolation {
                prev_key_debug,
                next_key_debug,
            } => PipelineError::MergeSortOrderViolation {
                message: format!(
                    "internal Clinker bug — LoserTree produced out-of-order keys: prev={prev_key_debug} next={next_key_debug}"
                ),
            },
        }
    }
}
