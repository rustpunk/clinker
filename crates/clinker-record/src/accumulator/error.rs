//! Errors produced by accumulator finalization.
//!
//! Leaf error type in the foundation crate, following the same convention as
//! `clinker_format::FormatError` and `cxl::eval::EvalError`. Wrapped into
//! `clinker_core::PipelineError` via a `From` impl at the integration point.

use std::fmt;

/// Errors produced by `AccumulatorEnum::finalize`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccumulatorError {
    /// Integer sum exceeded i64 range after i128 internal accumulation.
    ///
    /// Raised by Sum, Avg, and WeightedAvg when their integer-path result
    /// cannot be represented as i64. The `field` name is populated by the
    /// aggregation engine when known.
    ///
    /// Finalize uses `i64::try_from` on the internal i128 sum — never
    /// `as i64` — so overflow surfaces as this error rather than silently
    /// wrapping. Mirrors DuckDB's `HUGEINT` overflow-on-finalize pattern.
    SumOverflow { field: Option<String> },
}

impl fmt::Display for AccumulatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SumOverflow { field: Some(name) } => write!(
                f,
                "integer sum overflow on field '{name}' (i64 range exceeded)"
            ),
            Self::SumOverflow { field: None } => {
                write!(f, "integer sum overflow (i64 range exceeded)")
            }
        }
    }
}

impl std::error::Error for AccumulatorError {}
