//! Errors produced by accumulator finalization.
//!
//! Leaf error type in the foundation crate, following the same convention as
//! `clinker_format::FormatError` and `cxl::eval::EvalError`. Wrapped into
//! `clinker_plan::PipelineError` via a `From` impl at the integration point.
//!
//! A numeric aggregate's result is the exact value of its definition over the
//! group's values, rounded once, or one of these errors: a failure is never
//! reported as a null or as a total that silently left out some values. Each
//! message names the rule the group broke and gives one fix, in a form the
//! author can paste: a CXL expression, a Source schema type or a Transform
//! config, with the example column named so the author knows what to put in
//! its place. No fix converts a float to a decimal, which would keep the
//! float's binary digits. The aggregation engine prefixes the Aggregate and
//! the `emit` that failed.

use std::fmt;

/// The largest magnitude a decimal can hold, as the messages quote it.
const DECIMAL_RANGE: &str = "±79228162514264337593543950335";

/// Errors produced by `AccumulatorEnum::finalize`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccumulatorError {
    /// Integer sum exceeded i64 range after i128 internal accumulation.
    ///
    /// Raised by Sum when its integer-only result cannot be represented as
    /// i64. The `field` name is populated by the aggregation engine when
    /// known.
    ///
    /// Finalize uses `i64::try_from` on the internal i128 sum — never
    /// `as i64` — so overflow surfaces as this error rather than silently
    /// wrapping. Mirrors DuckDB's `HUGEINT` overflow-on-finalize pattern.
    /// The message's fix sums the column as a decimal, which holds an
    /// integer total exactly.
    SumOverflow { field: Option<String> },
    /// A decimal total — of `sum`, of the sum `avg` divides, or of either sum
    /// `weighted_avg` divides — is outside the decimal range. Decided on the
    /// group's exact total, so it does not depend on the order values arrived
    /// in or on how the group was split.
    DecimalOutOfRange,
    /// An `avg` or `weighted_avg` decimal quotient is outside the decimal
    /// range although both totals are inside it (weights that nearly cancel).
    /// The message prints the two emits that show the totals.
    QuotientOutOfRange,
    /// A `weighted_avg` row the group holds has a `value * weight` product
    /// outside the decimal range. Counted per row, so retracting the row
    /// clears it.
    ProductOverflow,
    /// A `weighted_avg` group has rows whose weights total exactly zero, so
    /// the weighted average divides by zero (as the scalar `x / 0` does).
    ZeroTotalWeight,
    /// A `sum`, `avg` or `weighted_avg` group holds both a decimal and a
    /// float. A decimal is never added to a float without an explicit
    /// conversion, as in scalar CXL.
    MixedDecimalFloat,
}

impl fmt::Display for AccumulatorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SumOverflow { field: Some(name) } => write!(
                f,
                "integer sum overflow on field '{name}' (i64 range exceeded); sum the column \
                 as a decimal instead, `sum({name}.to_decimal())`"
            ),
            Self::SumOverflow { field: None } => write!(
                f,
                "integer sum overflow (i64 range exceeded); sum the column as a decimal \
                 instead, for example `sum(amount.to_decimal())` with your column in place \
                 of `amount`"
            ),
            Self::DecimalOutOfRange => write!(
                f,
                "decimal total out of range: the group's exact decimal total is outside \
                 {DECIMAL_RANGE}; aggregate the column as floats, for example \
                 `sum(amount.to_float())` with your column in place of `amount`, if a \
                 binary float's range and precision will do"
            ),
            Self::QuotientOutOfRange => write!(
                f,
                "decimal average out of range: the quotient of the group's exact totals is \
                 outside {DECIMAL_RANGE} because its weights nearly cancel; emit \
                 `sum(value * weight)` and `sum(weight)` separately to inspect them"
            ),
            Self::ProductOverflow => write!(
                f,
                "decimal product out of range: a row's value times its weight is outside \
                 the decimal range; compute the average in floats, for example \
                 `weighted_avg(price.to_float(), qty.to_float())` with your columns in place \
                 of `price` and `qty`"
            ),
            Self::ZeroTotalWeight => write!(
                f,
                "zero total weight: the group's weights add up to exactly zero, so the \
                 weighted average divides by zero; rows whose weight is zero add nothing to \
                 the average, so add a Transform before the Aggregate with \
                 `config: {{ cxl: \"filter qty != 0\" }}`, with your weight column in place \
                 of `qty`"
            ),
            Self::MixedDecimalFloat => write!(
                f,
                "decimal and float in one group: a decimal is never added to a float \
                 without an explicit conversion; declare the column that holds the floats \
                 `type: decimal` in its Source schema, so every value in the group is a \
                 decimal"
            ),
        }
    }
}

impl std::error::Error for AccumulatorError {}
