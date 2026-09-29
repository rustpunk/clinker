//! Enum-dispatched accumulators for GROUP BY aggregation.
//!
//! 7 built-in variants: Sum, Count, Avg, Min, Max, Collect, WeightedAvg.
//! Enum dispatch avoids per-group `Box<dyn>` heap allocations (2.3-5x faster
//! than trait objects in research benchmarks). Compile-time type matching in
//! `merge()` — wrong-type merge is a logic bug and debug-asserts.
//!
//! Streaming: all variants are O(1) memory except `Collect` (O(n) per group).
//! Blocking: hash aggregation buffers one accumulator set per group.
//!
//! `Sum`, `Avg` and `WeightedAvg` hold their integer, float and decimal
//! addends exactly, the floats in an [`ExactSum`] and the decimals in an
//! [`ExactDecimalSum`] (each one fixed-size allocation, made at the first
//! addend of its type), and round once at finalize. Their results
//! therefore depend only on the multiset of inputs, not on arrival order or
//! on how partial states were merged, and they retract exactly.
//!
//! One numeric rule governs their finalize: the result is the exact value of
//! the aggregate's definition over the group's values, rounded once, or a
//! typed [`AccumulatorError`]. The domain the result is computed in comes from
//! one count-derived classifier, `NumericDomain`, which every one of the three
//! finalizers matches exhaustively: a group holding a decimal and a float can
//! only be an error, and null is only the answer for a group with no non-null
//! input.
//!
//! Serde derive on `AccumulatorEnum` and all state structs enables spill
//! serialization without manual state/restore code.

use std::cmp::Ordering;

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::order;
use crate::value::Value;

pub mod error;
pub use error::AccumulatorError;

pub mod exact_sum;
pub use exact_sum::ExactSum;

pub mod exact_decimal_sum;
pub use exact_decimal_sum::ExactDecimalSum;

mod limbs;
use limbs::WideInt;

/// One row of accumulators — one entry per `AggregateBinding` in the
/// owning `CompiledAggregate`. Cloned from a prototype on group
/// insertion; `Vec` preserves binding insertion order so finalize
/// output columns match the authored aggregate order.
pub type AccumulatorRow = Vec<AccumulatorEnum>;

#[cfg(test)]
mod tests;

/// The numeric domain a `sum`, `avg` or `weighted_avg` result is computed in,
/// derived only from how many integer, float and decimal inputs a group holds.
///
/// A decimal is never added to a float without an explicit conversion, so a
/// group with both is `Mixed`, whose only outcome is
/// [`AccumulatorError::MixedDecimalFloat`]. Otherwise any decimal makes the
/// group `Decimal` (integers join the exact decimal total), else any float
/// makes it `Float` (integers join the exact float sum), else any integer
/// makes it `Integer`, and a group with no non-null input is `Empty`, the only
/// domain whose result is null. Counts are a function of the multiset, so the
/// domain does not depend on arrival order, on how a group was split into
/// partial states, or on which inputs were added and later retracted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NumericDomain {
    Empty,
    Integer,
    Float,
    Decimal,
    Mixed,
}

impl NumericDomain {
    /// The domain of a group holding `integers` integer, `floats` float and
    /// `decimals` decimal inputs.
    pub(crate) fn of(integers: u64, floats: u64, decimals: u64) -> Self {
        match (integers > 0, floats > 0, decimals > 0) {
            (_, true, true) => Self::Mixed,
            (_, false, true) => Self::Decimal,
            (_, true, false) => Self::Float,
            (true, false, false) => Self::Integer,
            (false, false, false) => Self::Empty,
        }
    }
}

// ============================================================================
// State structs
// ============================================================================

/// Sum accumulator state: the integer, float and decimal addends each held
/// exactly, in their own part.
///
/// Integers add into an `i128`, floats into an [`ExactSum`], decimals into an
/// [`ExactDecimalSum`]; each part counts its addends. The result follows from
/// the counts through [`NumericDomain`], not from the order values arrived in:
/// a decimal group gives the exact decimal and integer total rounded once at
/// the largest input scale, or [`AccumulatorError::DecimalOutOfRange`]; a
/// float group the exact sum of the floats and the integers rounded once; an
/// integer group an `Integer`, or [`AccumulatorError::SumOverflow`]; a group
/// holding a decimal and a float [`AccumulatorError::MixedDecimalFloat`]; an
/// empty group null. Adding, merging and subtracting never round, so the
/// result depends only on the multiset of addends: not on arrival order and
/// not on how spill runs split a group into partial states.
///
/// Finalize converts the integer total with `i64::try_from`, never `as i64`,
/// which would silently wrap.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SumState {
    /// The integer addends' exact total. An `i128` cannot overflow at ETL
    /// scale (it would take more than 2^64 addends at `i64::MAX`).
    pub int_sum: i128,
    /// Integer addends held.
    pub int_count: u64,
    /// The float addends, exactly; allocates at the first float addend.
    pub floats: ExactSum,
    /// The decimal addends, exactly, with a count per scale; allocates at the
    /// first decimal addend.
    pub decimals: ExactDecimalSum,
}

impl Default for SumState {
    fn default() -> Self {
        Self {
            int_sum: 0,
            int_count: 0,
            floats: ExactSum::new(),
            decimals: ExactDecimalSum::new(),
        }
    }
}

impl SumState {
    /// Add one value. Returns the heap bytes this allocated (the float or
    /// decimal part's state at its first addend), else 0. Null and
    /// non-numeric values are skipped (typecheck rejects a non-numeric `sum`;
    /// this is defence-in-depth).
    fn add(&mut self, value: &Value) -> usize {
        match value {
            Value::Integer(n) => {
                self.int_sum += i128::from(*n);
                self.int_count += 1;
                0
            }
            Value::Float(f) => self.floats.add_f64(*f),
            Value::Decimal(d) => self.decimals.add_decimal(*d),
            _ => 0,
        }
    }

    /// Subtract one value this state holds: the exact inverse of
    /// [`add`](Self::add), so the state afterwards equals one that never saw
    /// the value. Returns the heap delta (negative when the last float or
    /// decimal addend frees its part's state).
    fn sub(&mut self, value: &Value) -> isize {
        match value {
            Value::Integer(n) => {
                self.int_sum -= i128::from(*n);
                self.int_count = self.int_count.saturating_sub(1);
                0
            }
            Value::Float(f) => self.floats.sub_f64(*f),
            Value::Decimal(d) => self.decimals.sub_decimal(*d),
            _ => 0,
        }
    }

    /// Add every addend of `other`, part by part. A merge that gives the float
    /// or decimal part its first addends allocates;
    /// [`AccumulatorEnum::heap_size`] reports it.
    fn merge(&mut self, other: &SumState) {
        self.int_sum += other.int_sum;
        self.int_count += other.int_count;
        self.floats.merge(&other.floats);
        self.decimals.merge(&other.decimals);
    }

    /// Non-null numeric addends held, of every type.
    fn addend_count(&self) -> u64 {
        self.int_count + self.floats.count() + self.decimals.count()
    }

    fn domain(&self) -> NumericDomain {
        NumericDomain::of(self.int_count, self.floats.count(), self.decimals.count())
    }

    /// The decimal total plus the integer total, rounded once; `None` when it
    /// is outside the decimal range.
    fn decimal_total(&self) -> Option<Decimal> {
        self.decimals.round_with(self.int_sum)
    }

    /// The float result: the floats and the integer total rounded once.
    fn float_total(&self) -> f64 {
        round_float_sum(
            &self.floats,
            &limbs::from_i128(self.int_sum),
            self.int_count,
        )
    }

    fn heap_size(&self) -> usize {
        self.floats.heap_size() + self.decimals.heap_size()
    }

    fn finalize(&self) -> Result<Value, AccumulatorError> {
        match self.domain() {
            NumericDomain::Empty => Ok(Value::Null),
            NumericDomain::Integer => i64::try_from(self.int_sum)
                .map(Value::Integer)
                .map_err(|_| AccumulatorError::SumOverflow { field: None }),
            NumericDomain::Float => Ok(Value::Float(self.float_total())),
            NumericDomain::Decimal => self
                .decimal_total()
                .map(Value::Decimal)
                .ok_or(AccumulatorError::DecimalOutOfRange),
            NumericDomain::Mixed => Err(AccumulatorError::MixedDecimalFloat),
        }
    }
}

/// `floats` plus the integer total (two's-complement limbs), rounded once. An
/// integer addend is `+0`, so a zero sum is `-0.0` only when there was no
/// integer addend and every float addend was `-0.0`.
fn round_float_sum<const M: usize>(floats: &ExactSum, int_part: &[u64; M], int_count: u64) -> f64 {
    let sum = floats.round_with_limbs(int_part);
    if int_count > 0 && sum == 0.0 {
        0.0
    } else {
        sum
    }
}

// ----------------------------------------------------------------------------

/// Count accumulator state. Two modes: count-all (includes NULLs) vs
/// count-field (skips NULLs, SQL `COUNT(field)` semantics).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountState {
    pub count: u64,
    /// If true, NULLs are counted (SQL `COUNT(*)`). If false, NULLs are
    /// skipped (SQL `COUNT(field)`).
    pub count_all: bool,
}

impl CountState {
    pub fn new_count_all() -> Self {
        Self {
            count: 0,
            count_all: true,
        }
    }

    pub fn new_count_field() -> Self {
        Self {
            count: 0,
            count_all: false,
        }
    }

    fn add(&mut self, value: &Value) {
        if self.count_all || !value.is_null() {
            self.count += 1;
        }
    }

    fn merge(&mut self, other: &CountState) {
        self.count += other.count;
    }

    fn finalize(&self) -> Value {
        Value::Integer(self.count as i64)
    }
}

/// Decrement a `CountState` by one observation.
///
/// `count_all` mode mirrors SQL `COUNT(*)` and decrements unconditionally;
/// `count_field` mode skips nulls so a retracted null contributes nothing
/// (symmetric with `add`). Saturates at zero — over-retraction is a
/// programmer bug but does not panic; finalize returns `Integer(0)` on an
/// empty group.
fn count_state_sub(s: &mut CountState, value: &Value) {
    if s.count_all || !value.is_null() {
        s.count = s.count.saturating_sub(1);
    }
}

// ----------------------------------------------------------------------------

/// Average accumulator state: the addends held exactly, as a Sum holds them.
///
/// The count of non-null numeric addends is derived from the parts, so it
/// cannot disagree with them. Finalize is `sum(x) / count(x)` with the exact
/// sum, in the group's [`NumericDomain`]: a float average is the exact sum of
/// the floats and the integers rounded once, divided by the count; an
/// integer-only average is the exact integer total converted to a float and
/// divided by the count; a decimal average is the decimal total divided by the
/// count with the scalar decimal `/`. Adding, merging and subtracting never
/// round, so the result depends only on the multiset of addends.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct AvgState {
    /// The addends, in their integer, float and decimal parts.
    pub sum: SumState,
}

impl AvgState {
    fn add(&mut self, value: &Value) -> usize {
        self.sum.add(value)
    }

    fn sub(&mut self, value: &Value) -> isize {
        self.sum.sub(value)
    }

    fn merge(&mut self, other: &AvgState) {
        self.sum.merge(&other.sum);
    }

    /// Avg returns Float for int/float inputs; for `decimal` inputs it returns
    /// the decimal total divided by the count at full division precision
    /// (rounding to a declared `scale` happens when the value lands in a
    /// scaled decimal column, matching intermediate `decimal / decimal`
    /// division semantics). A decimal total or quotient outside the decimal
    /// range, and a group mixing a decimal with a float, are errors.
    fn finalize(&self) -> Result<Value, AccumulatorError> {
        let sum = &self.sum;
        let count = sum.addend_count();
        match sum.domain() {
            NumericDomain::Empty => Ok(Value::Null),
            NumericDomain::Integer => Ok(Value::Float(sum.int_sum as f64 / count as f64)),
            NumericDomain::Float => Ok(Value::Float(sum.float_total() / count as f64)),
            NumericDomain::Decimal => sum
                .decimal_total()
                .ok_or(AccumulatorError::DecimalOutOfRange)?
                .checked_div(Decimal::from(count))
                .map(Value::Decimal)
                .ok_or(AccumulatorError::QuotientOutOfRange),
            NumericDomain::Mixed => Err(AccumulatorError::MixedDecimalFloat),
        }
    }
}

// ----------------------------------------------------------------------------

/// The order Aggregate `min` and `max` pick by: the one value order
/// ([`order::compare`]), with a fixed representative among the values it ties.
///
/// The value order ties values that print differently — `1`, `1.0` and the
/// decimal `1.00`; `-0.0` and `0.0`; NaNs of either sign and any payload — so
/// keeping whichever tied value arrived first would make the answer depend on
/// arrival order. Among tied values this orders an integer before a decimal
/// before a float, a decimal with fewer fractional digits (a smaller stored
/// scale) first, and floats by [`f64::total_cmp`] (so `-0.0` before `0.0` and
/// a negative-sign NaN before a positive one). `min` therefore returns `1` for
/// `1` and `1.0`, and `max` returns `1.0`.
///
/// It never reverses two values the value order separates, so it is not a
/// second order: numbers still compare by exact value across integer, float
/// and decimal, and NaN is above every other number. It is total, and `Equal`
/// only for two identical values, so a fold that keeps a value only when it
/// is strictly before (or after) the current one depends only on the multiset
/// of values it is given. Callers skip nulls; a null passed here sorts below
/// every other value, as in the value order. Pure; allocates only where the
/// value order does (to order a map's entries).
pub fn extremum_order(a: &Value, b: &Value) -> Ordering {
    order::compare(a, b).then_with(|| representative_order(a, b))
}

/// Order two values the value order ties, so that only identical values are
/// `Equal`. Tied arrays have the same length and tied elements; tied maps have
/// the same keys and tied values, possibly in a different insertion order.
fn representative_order(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Float(x), Value::Float(y)) => x.total_cmp(y),
        (Value::Decimal(x), Value::Decimal(y)) => x
            .scale()
            .cmp(&y.scale())
            .then_with(|| x.is_sign_positive().cmp(&y.is_sign_positive())),
        // Tied datetimes differ only at a leap second, which the value order
        // places on the following second; chrono keeps the leap instant first.
        (Value::DateTime(x), Value::DateTime(y)) => x.cmp(y),
        (Value::Array(x), Value::Array(y)) => x
            .iter()
            .zip(y.iter())
            .map(|(p, q)| extremum_order(p, q))
            .find(|ordering| ordering.is_ne())
            .unwrap_or(Ordering::Equal),
        (Value::Map(x), Value::Map(y)) => x
            .iter()
            .zip(y.iter())
            .map(|((kx, vx), (ky, vy))| {
                kx.as_str()
                    .as_bytes()
                    .cmp(ky.as_str().as_bytes())
                    .then_with(|| extremum_order(vx, vy))
            })
            .find(|ordering| ordering.is_ne())
            .unwrap_or(Ordering::Equal),
        _ => numeric_kind_rank(a).cmp(&numeric_kind_rank(b)),
    }
}

/// Rank of a number's type among tied values: integer, then decimal, then
/// float. Two tied non-numeric values of the same type are identical, and
/// share a rank.
fn numeric_kind_rank(v: &Value) -> u8 {
    match v {
        Value::Integer(_) => 0,
        Value::Decimal(_) => 1,
        Value::Float(_) => 2,
        _ => 3,
    }
}

/// Min/Max state — stores the extremum `Value` seen so far. The comparison
/// direction (Min vs Max) is determined by the `AccumulatorEnum` variant, not
/// by a flag on the state.
///
/// Values are picked by [`extremum_order`], which is total, so no non-null
/// value is ever skipped as incomparable and the result depends only on the
/// multiset of values added or merged: not on arrival order, not on how
/// partial states were split and merged, and so not on the memory limit or
/// the aggregate strategy. Nulls are skipped; an all-null group is null.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MinMaxState {
    pub current: Option<Value>,
}

impl MinMaxState {
    /// Keep `value` when [`extremum_order`] puts it strictly before (`keep_if`
    /// `Less`, for min) or strictly after (`Greater`, for max) the current
    /// extremum. `Equal` means identical, so keeping the current value on a
    /// tie cannot depend on arrival order.
    fn add_with(&mut self, value: &Value, keep_if: Ordering) {
        if value.is_null() {
            return;
        }
        match &self.current {
            Some(cur) if extremum_order(value, cur) != keep_if => {}
            _ => self.current = Some(value.clone()),
        }
    }

    fn merge_with(&mut self, other: &MinMaxState, keep_if: Ordering) {
        if let Some(v) = &other.current {
            self.add_with(v, keep_if);
        }
    }

    fn finalize(&self) -> Value {
        self.current.clone().unwrap_or(Value::Null)
    }
}

// ----------------------------------------------------------------------------

/// Collect accumulator state — accumulates all values including NULLs
/// (SQL `ARRAY_AGG` semantics, documented exception).
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct CollectState {
    pub values: Vec<Value>,
}

impl CollectState {
    fn add(&mut self, value: &Value) -> usize {
        // Heap delta: one Value slot (possibly from Vec growth, but we
        // conservatively charge size_of::<Value>() for simplicity) plus the
        // value's own heap.
        let delta = std::mem::size_of::<Value>() + value.heap_size();
        self.values.push(value.clone());
        delta
    }

    fn merge(&mut self, other: &CollectState) {
        self.values.extend(other.values.iter().cloned());
    }

    fn finalize(&self) -> Value {
        Value::Array(crate::owned_storage::OwnedValues::from_vec(
            self.values.clone(),
        ))
    }

    fn heap_size(&self) -> usize {
        self.values.capacity() * std::mem::size_of::<Value>()
            + self.values.iter().map(Value::heap_size).sum::<usize>()
    }
}

/// Remove the first occurrence of `value` from a `CollectState`'s array.
///
/// Returns the negative heap delta (one `Value` slot plus the removed
/// value's own heap footprint, symmetric with `CollectState::add`).
/// "First occurrence" rather than "every occurrence" preserves multiset
/// semantics: feed `[a, a, b]` then retract `a` produces `[a, b]`,
/// byte-identical to feed-from-scratch of `[a, b]`. Missing values are
/// no-ops returning zero — over-retraction is a programmer bug surfaced
/// by mismatched per-group `input_rows` lineage rather than a panic here.
fn collect_state_sub(s: &mut CollectState, value: &Value) -> isize {
    if let Some(idx) = s.values.iter().position(|v| values_equal(v, value)) {
        let removed = s.values.remove(idx);
        let delta = std::mem::size_of::<Value>() + removed.heap_size();
        -(delta as isize)
    } else {
        0
    }
}

// ----------------------------------------------------------------------------

/// Weighted average state. Two-argument: value + weight. Each row's product
/// `v * w` and its weight land in the part of the row's domain, held exactly:
///
/// - a row of two integers adds its exact product and weight to integer
///   totals wide enough for any number of `i64 × i64` products;
/// - a row with a float operand (and no decimal) adds its product, the scalar
///   `v * w` (one IEEE multiplication, which does not depend on any other
///   row), to an exact float sum of products, and its weight to an exact
///   float sum of weights, or to the integer weight total when the weight is
///   an integer;
/// - a row with a decimal operand (and no float) adds its product, the scalar
///   decimal `v * w`, and its weight to exact decimal sums; a row whose
///   product is outside the decimal range is counted instead, so retracting
///   it clears the count;
/// - a row holding a decimal and a float operand is counted as mixed and
///   joins no total.
///
/// The result follows from the row counts through [`NumericDomain`], not from
/// the order rows arrived in: a decimal group gives `round(Σ v*w) / round(Σ w)`
/// with the scalar decimal `/`, each total the exact decimal and integer
/// total rounded once at its largest input scale; a float group gives
/// `round(Σ v*w) / round(Σ w)`, each total rounded once, with IEEE division;
/// an integer group the exact integer totals, each converted once to a
/// float, divided. So `weighted_avg(v, w)` is `sum(v * w) / sum(w)` for
/// decimals and floats. Weights that total exactly zero are
/// [`AccumulatorError::ZeroTotalWeight`] in every domain, as the scalar
/// `x / 0` is an error; a group holding a decimal and a float, in one row or
/// across rows, is [`AccumulatorError::MixedDecimalFloat`]; only an empty
/// group is null. Adding, merging and subtracting never round, so the result
/// depends only on the multiset of rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeightedAvgState {
    /// Σ v·w over the rows of two integers, exactly.
    int_products: WideInt,
    /// Σ w over every integer weight of a row without a decimal operand,
    /// exactly.
    int_weights: WideInt,
    /// Rows of two integers.
    pub int_rows: u64,
    /// Each float row's product, summed exactly. Its addend count is the
    /// number of float rows.
    pub float_products: ExactSum,
    /// The float rows' float weights, summed exactly.
    pub float_weights: ExactSum,
    /// Each decimal row's in-range product, summed exactly.
    pub decimal_products: ExactDecimalSum,
    /// Each decimal row's weight, summed exactly. Its addend count is the
    /// number of decimal rows.
    pub decimal_weights: ExactDecimalSum,
    /// Decimal rows whose product `v * w` is outside the decimal range; the
    /// group fails with [`AccumulatorError::ProductOverflow`] while any is
    /// held.
    pub product_overflows: u64,
    /// Rows holding a decimal and a float operand, which join no total; the
    /// group fails with [`AccumulatorError::MixedDecimalFloat`] while any is
    /// held.
    pub mixed_rows: u64,
}

impl Default for WeightedAvgState {
    fn default() -> Self {
        Self {
            int_products: WideInt::default(),
            int_weights: WideInt::default(),
            int_rows: 0,
            float_products: ExactSum::new(),
            float_weights: ExactSum::new(),
            decimal_products: ExactDecimalSum::new(),
            decimal_weights: ExactDecimalSum::new(),
            product_overflows: 0,
            mixed_rows: 0,
        }
    }
}

/// A numeric operand of `weighted_avg`, kept in exact form. Null and
/// non-numeric values are filtered by [`Operand::numeric`] (which returns
/// `None`) and skip the row.
#[derive(Debug, Clone, Copy)]
enum Operand {
    Int(i64),
    Float(f64),
    Decimal(Decimal),
}

impl Operand {
    fn numeric(v: &Value) -> Option<Self> {
        match v {
            Value::Integer(n) => Some(Operand::Int(*n)),
            Value::Float(f) => Some(Operand::Float(*f)),
            Value::Decimal(d) => Some(Operand::Decimal(*d)),
            // Null and non-numeric values skip the row.
            _ => None,
        }
    }
}

/// A float row's weight: an integer weight joins the exact integer weight
/// total, a float weight the exact float one.
#[derive(Debug, Clone, Copy)]
enum FloatRowWeight {
    Int(i64),
    Float(f64),
}

/// Which domain a weighted row belongs to, with its contribution.
enum WeightedRow {
    Integer {
        product: i128,
        weight: i128,
    },
    Float {
        product: f64,
        weight: FloatRowWeight,
    },
    /// `product` is `None` when `v * w` is outside the decimal range.
    Decimal {
        product: Option<Decimal>,
        weight: Decimal,
    },
    /// A decimal operand with a float operand.
    Mixed,
}

impl WeightedRow {
    /// Classify a row, or `None` when either operand is null or non-numeric
    /// (the row is skipped, as SQL skips nulls). Products are the scalar
    /// `v * w` of CXL: an integer widens exactly to a decimal or converts to a
    /// float as the scalar operator does.
    fn classify(value: &Value, weight: &Value) -> Option<Self> {
        use Operand::{Decimal as Dec, Float, Int};
        let row = match (Operand::numeric(value)?, Operand::numeric(weight)?) {
            (Dec(_), Float(_)) | (Float(_), Dec(_)) => WeightedRow::Mixed,
            (Int(v), Int(w)) => WeightedRow::Integer {
                product: i128::from(v) * i128::from(w),
                weight: i128::from(w),
            },
            (Dec(v), Dec(w)) => WeightedRow::Decimal {
                product: v.checked_mul(w),
                weight: w,
            },
            (Dec(v), Int(w)) => WeightedRow::Decimal {
                product: v.checked_mul(Decimal::from(w)),
                weight: Decimal::from(w),
            },
            (Int(v), Dec(w)) => WeightedRow::Decimal {
                product: Decimal::from(v).checked_mul(w),
                weight: w,
            },
            (Float(v), Float(w)) => WeightedRow::Float {
                product: v * w,
                weight: FloatRowWeight::Float(w),
            },
            (Float(v), Int(w)) => WeightedRow::Float {
                product: v * w as f64,
                weight: FloatRowWeight::Int(w),
            },
            (Int(v), Float(w)) => WeightedRow::Float {
                product: v as f64 * w,
                weight: FloatRowWeight::Float(w),
            },
        };
        Some(row)
    }
}

impl WeightedAvgState {
    /// Add one row. Returns the heap bytes this allocated (an exact sum's
    /// state at its first addend), else 0.
    fn add_weighted(&mut self, value: &Value, weight: &Value) -> usize {
        let Some(row) = WeightedRow::classify(value, weight) else {
            return 0;
        };
        match row {
            WeightedRow::Integer { product, weight } => {
                self.int_products.add_i128(product);
                self.int_weights.add_i128(weight);
                self.int_rows += 1;
                0
            }
            WeightedRow::Float { product, weight } => {
                let mut delta = self.float_products.add_f64(product);
                match weight {
                    FloatRowWeight::Int(w) => self.int_weights.add_i128(i128::from(w)),
                    FloatRowWeight::Float(w) => delta += self.float_weights.add_f64(w),
                }
                delta
            }
            WeightedRow::Decimal { product, weight } => {
                let mut delta = self.decimal_weights.add_decimal(weight);
                match product {
                    Some(p) => delta += self.decimal_products.add_decimal(p),
                    None => self.product_overflows += 1,
                }
                delta
            }
            WeightedRow::Mixed => {
                self.mixed_rows += 1;
                0
            }
        }
    }

    /// Subtract one row this state holds: the exact inverse of
    /// [`add_weighted`](Self::add_weighted) with the same operands, so the
    /// state afterwards equals one that never saw the row. Returns the heap
    /// delta (negative when an exact sum loses its last addend).
    fn sub_weighted(&mut self, value: &Value, weight: &Value) -> isize {
        let Some(row) = WeightedRow::classify(value, weight) else {
            return 0;
        };
        match row {
            WeightedRow::Integer { product, weight } => {
                self.int_products.sub_i128(product);
                self.int_weights.sub_i128(weight);
                self.int_rows = self.int_rows.saturating_sub(1);
                0
            }
            WeightedRow::Float { product, weight } => {
                let mut delta = self.float_products.sub_f64(product);
                match weight {
                    FloatRowWeight::Int(w) => self.int_weights.sub_i128(i128::from(w)),
                    FloatRowWeight::Float(w) => delta += self.float_weights.sub_f64(w),
                }
                delta
            }
            WeightedRow::Decimal { product, weight } => {
                let mut delta = self.decimal_weights.sub_decimal(weight);
                match product {
                    Some(p) => delta += self.decimal_products.sub_decimal(p),
                    None => {
                        debug_assert!(self.product_overflows > 0, "retracted a row never added");
                        self.product_overflows = self.product_overflows.saturating_sub(1);
                    }
                }
                delta
            }
            WeightedRow::Mixed => {
                debug_assert!(self.mixed_rows > 0, "retracted a row never added");
                self.mixed_rows = self.mixed_rows.saturating_sub(1);
                0
            }
        }
    }

    /// Add every row of `other`, part by part. A merge that gives an exact
    /// sum its first addends allocates; [`AccumulatorEnum::heap_size`]
    /// reports it.
    fn merge(&mut self, other: &WeightedAvgState) {
        self.int_products.add(&other.int_products);
        self.int_weights.add(&other.int_weights);
        self.int_rows += other.int_rows;
        self.float_products.merge(&other.float_products);
        self.float_weights.merge(&other.float_weights);
        self.decimal_products.merge(&other.decimal_products);
        self.decimal_weights.merge(&other.decimal_weights);
        self.product_overflows += other.product_overflows;
        self.mixed_rows += other.mixed_rows;
    }

    fn heap_size(&self) -> usize {
        self.float_products.heap_size()
            + self.float_weights.heap_size()
            + self.decimal_products.heap_size()
            + self.decimal_weights.heap_size()
    }

    /// A row holding a decimal and a float counts as both, so it alone makes
    /// the group mixed.
    fn domain(&self) -> NumericDomain {
        NumericDomain::of(
            self.int_rows,
            self.float_products.count() + self.mixed_rows,
            self.decimal_weights.count() + self.mixed_rows,
        )
    }

    fn finalize(&self) -> Result<Value, AccumulatorError> {
        match self.domain() {
            NumericDomain::Empty => Ok(Value::Null),
            NumericDomain::Integer => {
                if self.int_weights.is_zero() {
                    return Err(AccumulatorError::ZeroTotalWeight);
                }
                let integer = |total: &WideInt| ExactSum::new().round_with_limbs(total.limbs());
                Ok(Value::Float(
                    integer(&self.int_products) / integer(&self.int_weights),
                ))
            }
            NumericDomain::Float => {
                let products = round_float_sum(
                    &self.float_products,
                    self.int_products.limbs(),
                    self.int_rows,
                );
                let weights = self
                    .float_weights
                    .round_with_limbs(self.int_weights.limbs());
                // A nonzero exact total never rounds to zero, so this is the
                // exact total being zero.
                if weights == 0.0 {
                    return Err(AccumulatorError::ZeroTotalWeight);
                }
                Ok(Value::Float(products / weights))
            }
            NumericDomain::Decimal => {
                if self.product_overflows > 0 {
                    return Err(AccumulatorError::ProductOverflow);
                }
                let products = self
                    .decimal_products
                    .round_with_limbs(self.int_products.limbs())
                    .ok_or(AccumulatorError::DecimalOutOfRange)?;
                let weights = self
                    .decimal_weights
                    .round_with_limbs(self.int_weights.limbs())
                    .ok_or(AccumulatorError::DecimalOutOfRange)?;
                if weights.is_zero() {
                    return Err(AccumulatorError::ZeroTotalWeight);
                }
                products
                    .checked_div(weights)
                    .map(Value::Decimal)
                    .ok_or(AccumulatorError::QuotientOutOfRange)
            }
            NumericDomain::Mixed => Err(AccumulatorError::MixedDecimalFloat),
        }
    }
}

// ============================================================================
// AccumulatorEnum
// ============================================================================

/// SQL `ANY_VALUE` / `arbitrary()` accumulator state.
///
/// Two parallel pieces of state:
///
/// * `value` — the first non-null value observed. Locks at the first add and
///   moves only when retraction empties its refcount entry; preserves the
///   first-wins guarantee that drove this variant into the catalogue
///   (explicit escape hatch for metadata propagation when the user knows
///   the value is constant within a group and wants to skip the
///   `MetadataCommonTracker` conflict-detection overhead).
/// * `refcounts` — multiset cardinality stored as `(Value, count)` pairs.
///   `Vec` rather than `HashMap`/`BTreeMap` because [`Value`] does not impl
///   `Hash`/`Eq`/`Ord` (`Value::Float` carries `f64`, NaN-comparisons are
///   undefined). Linear search is O(distinct values per group), which is
///   O(1) for the canonical "constant within a group" case and bounded by
///   group cardinality otherwise.
///
/// The refcount vec exists for retraction support: an `add` increments the
/// observed value's count; a `sub` decrements it. When the locked `value`'s
/// count drops to zero, finalize falls back to any other still-positive
/// entry in iteration order. When every entry has been retracted, finalize
/// returns `Null`. Strict (non-relaxed) aggregation paths never call `sub`,
/// so the refcount stays a write-only scoreboard there.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AnyState {
    /// First non-null value observed, locked once set. `None` until the
    /// first non-null `add` and after every observation has been retracted
    /// via `sub`.
    pub value: Option<Value>,
    /// `(Value, count)` pairs keyed by observed value. `Vec` rather than
    /// `HashMap` because `Value` is not `Hash`/`Eq`. An entry with count
    /// zero is removed eagerly so `is_empty()` mirrors "no surviving
    /// contributions". Empty by default to preserve the no-allocation
    /// fast path on the strict aggregation path.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refcounts: Vec<(Value, u32)>,
}

impl AnyState {
    fn refcount_index(&self, value: &Value) -> Option<usize> {
        self.refcounts
            .iter()
            .position(|(v, _)| values_equal(v, value))
    }

    fn add(&mut self, value: &Value) {
        if value.is_null() {
            return;
        }
        if self.value.is_none() {
            self.value = Some(value.clone());
        }
        match self.refcount_index(value) {
            Some(i) => self.refcounts[i].1 += 1,
            None => self.refcounts.push((value.clone(), 1)),
        }
    }

    fn merge(&mut self, other: &AnyState) {
        // First-wins on the locked `value`.
        if self.value.is_none() {
            self.value.clone_from(&other.value);
        }
        for (k, v) in other.refcounts.iter() {
            match self.refcount_index(k) {
                Some(i) => self.refcounts[i].1 += *v,
                None => self.refcounts.push((k.clone(), *v)),
            }
        }
    }

    fn finalize(&self) -> Value {
        if self.refcounts.is_empty() {
            return Value::Null;
        }
        // Prefer the locked `value` when its refcount entry is still alive.
        if let Some(v) = &self.value
            && self
                .refcount_index(v)
                .map(|i| self.refcounts[i].1 > 0)
                .unwrap_or(false)
        {
            return v.clone();
        }
        // Otherwise return the first surviving entry in iteration order.
        // Only reached when the originally locked value was retracted to
        // zero, in which case the caller's add order continues to drive
        // determinism (the vec is push-ordered).
        self.refcounts
            .iter()
            .find(|(_, c)| *c > 0)
            .map(|(k, _)| k.clone())
            .unwrap_or(Value::Null)
    }

    /// Decrement the refcount of `value`; when it drops to zero the entry is
    /// removed. Null values are ignored (mirrors `add`). When the vec empties,
    /// the locked `value` is cleared so subsequent `add` calls re-lock to the
    /// next observed value, restoring the "feed-from-scratch over surviving
    /// rows" equivalence the retract path is built around.
    fn sub(&mut self, value: &Value) {
        if value.is_null() {
            return;
        }
        if let Some(i) = self.refcount_index(value) {
            if self.refcounts[i].1 > 1 {
                self.refcounts[i].1 -= 1;
            } else {
                self.refcounts.swap_remove(i);
            }
        }
        if self.refcounts.is_empty() {
            self.value = None;
        }
    }

    /// Reported heap footprint: per-entry (Value heap + tag bytes + u32) plus
    /// vec capacity overhead.
    fn heap_size(&self) -> usize {
        let entry_overhead = std::mem::size_of::<(Value, u32)>();
        let vec_overhead = self.refcounts.capacity() * entry_overhead;
        let entry_heap: usize = self.refcounts.iter().map(|(k, _)| k.heap_size()).sum();
        let value_heap = self.value.as_ref().map(Value::heap_size).unwrap_or(0);
        vec_overhead + entry_heap + value_heap
    }
}

/// Equality predicate for refcount lookup. `Value` does not derive `Eq`
/// because `Value::Float` carries `f64` (NaN ≠ NaN). The retract path treats
/// floats by bit pattern so `add(NaN); sub(NaN)` round-trips; downstream
/// finalize then re-emits whichever surviving entry the locked `value`
/// originally pointed at.
fn values_equal(a: &Value, b: &Value) -> bool {
    use Value::*;
    match (a, b) {
        (Null, Null) => true,
        (Bool(x), Bool(y)) => x == y,
        (Integer(x), Integer(y)) => x == y,
        (Float(x), Float(y)) => x.to_bits() == y.to_bits(),
        (Decimal(x), Decimal(y)) => x == y,
        (String(x), String(y)) => x == y,
        (Date(x), Date(y)) => x == y,
        (DateTime(x), DateTime(y)) => x == y,
        (Array(x), Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(a, b)| values_equal(a, b))
        }
        (Map(x), Map(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|((kx, vx), (ky, vy))| kx == ky && values_equal(vx, vy))
        }
        _ => false,
    }
}

/// Tag enum mirroring `AccumulatorEnum` variants. Used by `AggregateBinding`
/// and the `AccumulatorEnum::for_type()` factory to construct empty
/// accumulators from the plan-time binding description (D5).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggregateType {
    Sum,
    Count { count_all: bool },
    Avg,
    Min,
    Max,
    Collect,
    WeightedAvg,
    Any,
}

/// Whether an accumulator can subtract a value to undo a prior contribution.
///
/// `Reversible` variants admit an `O(1)` retract step that walks back a
/// single contribution and recovers a state equal to never having observed
/// it. `BufferRequired` variants must replay surviving inputs from scratch,
/// because the operation is positional (`Min`, `Max`): the extremum a
/// retracted value shadowed is not in the state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reversibility {
    Reversible,
    BufferRequired,
}

/// Per-group heap allocation cost: one `AccumulatorEnum` in the group's row.
/// No `Box<dyn>` indirection. Serde derive enables spill-to-disk via JSON/NDJSON.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AccumulatorEnum {
    Sum(SumState),
    Count(CountState),
    Avg(AvgState),
    Min(MinMaxState),
    Max(MinMaxState),
    Collect(CollectState),
    WeightedAvg(WeightedAvgState),
    Any(AnyState),
}

impl AccumulatorEnum {
    /// Incorporate one input value. Returns a heap bytes delta for memory
    /// tracking: `Sum` and `Avg` return an exact sum's allocation at the first
    /// float or decimal addend; `Collect` returns the size of one `Value` slot
    /// plus the value's own heap footprint; every other call returns 0.
    ///
    /// For `WeightedAvg`, this is a no-op — use `add_weighted` instead.
    pub fn add(&mut self, value: &Value) -> usize {
        match self {
            Self::Sum(s) => s.add(value),
            Self::Count(s) => {
                s.add(value);
                0
            }
            Self::Avg(s) => s.add(value),
            Self::Min(s) => {
                s.add_with(value, Ordering::Less);
                0
            }
            Self::Max(s) => {
                s.add_with(value, Ordering::Greater);
                0
            }
            Self::Collect(s) => s.add(value),
            Self::WeightedAvg(_) => {
                debug_assert!(false, "WeightedAvg requires add_weighted, not add");
                0
            }
            Self::Any(s) => {
                s.add(value);
                0
            }
        }
    }

    /// Factory: build a default-initialized accumulator from an `AggregateType` tag.
    /// Used by `AccumulatorFactory` to materialize per-group prototype rows
    /// from a `CompiledAggregate`'s bindings (D5).
    pub fn for_type(t: &AggregateType) -> Self {
        match t {
            AggregateType::Sum => Self::Sum(SumState::default()),
            AggregateType::Count { count_all: true } => Self::Count(CountState::new_count_all()),
            AggregateType::Count { count_all: false } => Self::Count(CountState::new_count_field()),
            AggregateType::Avg => Self::Avg(AvgState::default()),
            AggregateType::Min => Self::Min(MinMaxState::default()),
            AggregateType::Max => Self::Max(MinMaxState::default()),
            AggregateType::Collect => Self::Collect(CollectState::default()),
            AggregateType::WeightedAvg => Self::WeightedAvg(WeightedAvgState::default()),
            AggregateType::Any => Self::Any(AnyState::default()),
        }
    }

    /// Whether this accumulator admits an O(1) retract step.
    ///
    /// `Sum`, `Count`, `Collect`, `Any`, `Avg` and `WeightedAvg` are
    /// reversible: an inverse operation recovers a state equal to never
    /// having observed the retracted value. `Sum`, `Avg` and `WeightedAvg`
    /// hold every part exactly (integers, exact float and decimal sums), so
    /// subtraction is exact. `Min` and `Max` are positional and need the full
    /// surviving multiset to recompute.
    pub const fn reversibility(&self) -> Reversibility {
        match self {
            Self::Sum(_)
            | Self::Count(_)
            | Self::Collect(_)
            | Self::Any(_)
            | Self::Avg(_)
            | Self::WeightedAvg(_) => Reversibility::Reversible,
            Self::Min(_) | Self::Max(_) => Reversibility::BufferRequired,
        }
    }

    /// Two-argument add for `WeightedAvg`. Returns the heap bytes delta, as
    /// [`add`](Self::add) does: the exact sums' allocations at their first
    /// addends (both decimal sums at a first decimal row), else 0. No-op returning 0 on other variants
    /// (debug-asserts to catch programmer errors).
    pub fn add_weighted(&mut self, value: &Value, weight: &Value) -> usize {
        match self {
            Self::WeightedAvg(s) => s.add_weighted(value, weight),
            _ => {
                debug_assert!(false, "add_weighted only valid for WeightedAvg");
                0
            }
        }
    }

    /// Two-argument retract for `WeightedAvg`: subtract one row previously
    /// added with [`add_weighted`](Self::add_weighted) with the same operands,
    /// exactly. Returns the heap bytes delta (negative when an exact sum loses
    /// its last addend and frees its allocation). No-op returning 0 on
    /// other variants (debug-asserts to catch programmer errors).
    pub fn sub_weighted(&mut self, value: &Value, weight: &Value) -> isize {
        match self {
            Self::WeightedAvg(s) => s.sub_weighted(value, weight),
            _ => {
                debug_assert!(false, "sub_weighted only valid for WeightedAvg");
                0
            }
        }
    }

    /// Retract one previously-added value's contribution.
    ///
    /// Defined on the single-argument `Reversibility::Reversible` variants
    /// (`Sum`, `Count`, `Collect`, `Any`, `Avg`); `WeightedAvg` retracts a
    /// row through [`sub_weighted`](Self::sub_weighted). Returns the
    /// heap-bytes delta for memory tracking — negative for shrink (`Collect`
    /// removing one slot, `Any` decrementing a refcount entry to zero, `Sum`
    /// or `Avg` removing its last float or decimal addend, which frees that
    /// exact sum's allocation) and zero otherwise. `Min` and `Max`
    /// (`BufferRequired`) and `WeightedAvg` debug-assert: `Min` and `Max`
    /// retract by replaying surviving rows from a per-group buffer.
    pub fn sub(&mut self, value: &Value) -> isize {
        match self {
            Self::Sum(s) => s.sub(value),
            Self::Count(s) => {
                count_state_sub(s, value);
                0
            }
            Self::Collect(s) => collect_state_sub(s, value),
            Self::Any(s) => {
                let before = s.heap_size();
                s.sub(value);
                let after = s.heap_size();
                after as isize - before as isize
            }
            Self::Avg(s) => s.sub(value),
            Self::WeightedAvg(_) => {
                debug_assert!(false, "WeightedAvg requires sub_weighted, not sub");
                0
            }
            Self::Min(_) | Self::Max(_) => {
                debug_assert!(false, "sub only valid on Reversible accumulators");
                0
            }
        }
    }

    /// Merge another accumulator of the same variant into this one.
    /// Variant mismatch is a programmer bug and debug-asserts.
    pub fn merge(&mut self, other: &AccumulatorEnum) {
        match (self, other) {
            (Self::Sum(a), Self::Sum(b)) => a.merge(b),
            (Self::Count(a), Self::Count(b)) => a.merge(b),
            (Self::Avg(a), Self::Avg(b)) => a.merge(b),
            (Self::Min(a), Self::Min(b)) => a.merge_with(b, Ordering::Less),
            (Self::Max(a), Self::Max(b)) => a.merge_with(b, Ordering::Greater),
            (Self::Collect(a), Self::Collect(b)) => a.merge(b),
            (Self::WeightedAvg(a), Self::WeightedAvg(b)) => a.merge(b),
            (Self::Any(a), Self::Any(b)) => a.merge(b),
            _ => debug_assert!(false, "AccumulatorEnum::merge variant mismatch"),
        }
    }

    /// Produce the final aggregate result.
    ///
    /// `Sum`, `Avg` and `WeightedAvg` return the exact value of their
    /// definition over the group, rounded once, or an [`AccumulatorError`]
    /// naming the rule the group broke: an integer `sum` beyond `i64`, a
    /// decimal total or quotient outside the decimal range, a `weighted_avg`
    /// row product outside it, a zero total weight, or a group mixing a
    /// decimal with a float. Null is only the result of a group with no
    /// non-null input. Pure; allocates only what the result value holds.
    pub fn finalize(&self) -> Result<Value, AccumulatorError> {
        match self {
            Self::Sum(s) => s.finalize(),
            Self::Count(s) => Ok(s.finalize()),
            Self::Avg(s) => s.finalize(),
            Self::Min(s) => Ok(s.finalize()),
            Self::Max(s) => Ok(s.finalize()),
            Self::Collect(s) => Ok(s.finalize()),
            Self::WeightedAvg(s) => s.finalize(),
            Self::Any(s) => Ok(s.finalize()),
        }
    }

    /// Estimated heap size for memory tracking. Fixed-size variants return
    /// `size_of::<Self>()` (inline enum footprint, no heap); `Sum`, `Avg` and
    /// `WeightedAvg` add their exact float and decimal sums' allocations, if
    /// they hold any. Collect reports `Vec` capacity × `size_of::<Value>()` plus each
    /// value's own heap.
    ///
    /// Report `Vec` capacity, not `len()` — len-based reporting has caused up
    /// to 19× undercounts in DataFusion (arrow-rs issue #13831).
    pub fn heap_size(&self) -> usize {
        match self {
            Self::Sum(s) => std::mem::size_of::<Self>() + s.heap_size(),
            Self::Avg(s) => std::mem::size_of::<Self>() + s.sum.heap_size(),
            Self::WeightedAvg(s) => std::mem::size_of::<Self>() + s.heap_size(),
            Self::Collect(s) => s.heap_size(),
            Self::Any(s) => std::mem::size_of::<Self>() + s.heap_size(),
            _ => std::mem::size_of::<Self>(),
        }
    }
}
