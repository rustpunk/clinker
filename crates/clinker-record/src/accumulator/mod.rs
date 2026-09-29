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
//! addends exactly, the floats in an [`ExactSum`] (one fixed-size allocation,
//! made at the first float addend), and round once at finalize. Their results
//! therefore depend only on the multiset of inputs, not on arrival order or
//! on how partial states were merged, and they retract exactly.
//!
//! Serde derive on `AccumulatorEnum` and all state structs enables spill
//! serialization without manual state/restore code.

use std::cmp::Ordering;

use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize};

use crate::order;
use crate::value::Value;

/// Convert an `i128` integer accumulator to an exact `Decimal`, or `None` when
/// it exceeds `Decimal`'s ~7.9e28 range. Used to fold an integer running sum
/// into the exact decimal path; the `None` case is surfaced as an overflow
/// rather than panicking (`Decimal::from_i128_with_scale` would panic).
fn i128_to_decimal(n: i128) -> Option<Decimal> {
    Decimal::from_i128(n)
}

pub mod error;
pub use error::AccumulatorError;

pub mod exact_sum;
pub use exact_sum::ExactSum;

/// One row of accumulators — one entry per `AggregateBinding` in the
/// owning `CompiledAggregate`. Cloned from a prototype on group
/// insertion; `Vec` preserves binding insertion order so finalize
/// output columns match the authored aggregate order.
pub type AccumulatorRow = Vec<AccumulatorEnum>;

#[cfg(test)]
mod tests;

// ============================================================================
// State structs
// ============================================================================

/// Sum accumulator state: the integer, float and decimal addends each held
/// exactly, in their own part.
///
/// Integers add into an `i128`, floats into an [`ExactSum`], decimals into an
/// exact `Decimal`; each part counts its addends. The result type follows from
/// the counts, not from the order values arrived in: any decimal addend gives
/// a `Decimal` (the decimal total plus the integer total); else any float
/// gives a `Float`, the exact sum of the floats and the integers rounded once;
/// else any integer gives an `Integer`; else null. Adding, merging and
/// subtracting never round, so a float sum depends only on the multiset of
/// addends: not on arrival order and not on how spill runs split a group into
/// partial states. A float addend mixed with a decimal (reachable only on an
/// untyped column) is not part of the decimal total.
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
    /// The decimal addends' exact total.
    #[serde(with = "crate::decimal_serde")]
    pub decimal_sum: Decimal,
    /// Decimal addends held.
    pub decimal_count: u64,
    /// Set once a decimal total leaves `Decimal`'s ~7.9e28 range; finalize
    /// then surfaces `SumOverflow` rather than a silently-wrong total. Sticky:
    /// an overflowed sum is an error outcome, and subtracting a value does not
    /// undo it.
    pub decimal_overflow: bool,
}

impl Default for SumState {
    fn default() -> Self {
        Self {
            int_sum: 0,
            int_count: 0,
            floats: ExactSum::new(),
            decimal_sum: Decimal::ZERO,
            decimal_count: 0,
            decimal_overflow: false,
        }
    }
}

impl SumState {
    /// Add one value. Returns the heap bytes this allocated (the float part's
    /// state at the first float addend), else 0. Null and non-numeric values
    /// are skipped (typecheck rejects a non-numeric `sum`; this is
    /// defence-in-depth).
    fn add(&mut self, value: &Value) -> usize {
        match value {
            Value::Integer(n) => {
                self.int_sum += i128::from(*n);
                self.int_count += 1;
                0
            }
            Value::Float(f) => self.floats.add_f64(*f),
            Value::Decimal(d) => {
                self.decimal_count += 1;
                self.decimal_add(*d);
                0
            }
            _ => 0,
        }
    }

    /// Subtract one value this state holds: the exact inverse of
    /// [`add`](Self::add), so the state afterwards equals one that never saw
    /// the value. Returns the heap delta (negative when the last float addend
    /// frees the float part's state).
    fn sub(&mut self, value: &Value) -> isize {
        match value {
            Value::Integer(n) => {
                self.int_sum -= i128::from(*n);
                self.int_count = self.int_count.saturating_sub(1);
                0
            }
            Value::Float(f) => self.floats.sub_f64(*f),
            Value::Decimal(d) => {
                self.decimal_count = self.decimal_count.saturating_sub(1);
                self.decimal_add(-*d);
                if self.decimal_count == 0 {
                    // The exact total is zero again; drop the scale it kept.
                    self.decimal_sum = Decimal::ZERO;
                }
                0
            }
            _ => 0,
        }
    }

    fn decimal_add(&mut self, d: Decimal) {
        match self.decimal_sum.checked_add(d) {
            Some(sum) => self.decimal_sum = sum,
            None => self.decimal_overflow = true,
        }
    }

    /// Add every addend of `other`, part by part. A merge that gives the float
    /// part its first addends allocates; [`AccumulatorEnum::heap_size`]
    /// reports it.
    fn merge(&mut self, other: &SumState) {
        self.int_sum += other.int_sum;
        self.int_count += other.int_count;
        self.floats.merge(&other.floats);
        if other.decimal_count > 0 {
            self.decimal_count += other.decimal_count;
            self.decimal_add(other.decimal_sum);
        }
        self.decimal_overflow |= other.decimal_overflow;
    }

    /// Non-null numeric addends held, of every type.
    fn addend_count(&self) -> u64 {
        self.int_count + self.floats.count() + self.decimal_count
    }

    /// The decimal total plus the integer total, exactly; `None` when either
    /// leaves `Decimal`'s range.
    fn exact_decimal_total(&self) -> Option<Decimal> {
        if self.decimal_overflow {
            return None;
        }
        i128_to_decimal(self.int_sum).and_then(|ints| self.decimal_sum.checked_add(ints))
    }

    /// The float result: the floats and the integer total rounded once.
    fn float_total(&self) -> f64 {
        round_float_sum(&self.floats, self.int_sum, self.int_count)
    }

    fn finalize(&self) -> Result<Value, AccumulatorError> {
        if self.decimal_count > 0 {
            return self
                .exact_decimal_total()
                .map(Value::Decimal)
                .ok_or(AccumulatorError::SumOverflow { field: None });
        }
        if !self.floats.is_empty() {
            return Ok(Value::Float(self.float_total()));
        }
        if self.int_count > 0 {
            let n = i64::try_from(self.int_sum)
                .map_err(|_| AccumulatorError::SumOverflow { field: None })?;
            return Ok(Value::Integer(n));
        }
        Ok(Value::Null)
    }
}

/// `floats` plus the integer total, rounded once. An integer addend is `+0`,
/// so a zero sum is `-0.0` only when there was no integer addend and every
/// float addend was `-0.0`.
fn round_float_sum(floats: &ExactSum, int_sum: i128, int_count: u64) -> f64 {
    let sum = floats.round_with(int_sum);
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
/// cannot disagree with them. Finalize divides once: a float average is the
/// exact sum of the floats and the integers rounded once, divided by the
/// count; an integer-only average is the exact integer total converted to a
/// float and divided by the count; a decimal average is the exact decimal
/// quotient. Adding, merging and subtracting never round, so the result
/// depends only on the multiset of addends.
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
    /// an exact `Value::Decimal` quotient at full division precision (rounding
    /// to a declared `scale` happens when the value lands in a scaled decimal
    /// column, matching intermediate `decimal / decimal` division semantics).
    /// A decimal total out of `Decimal`'s range, or a binary float mixed with
    /// a decimal (reachable only on an untyped column; the float cannot join
    /// an exact decimal total), yields `Null`: avg has no error channel, and
    /// either alternative would be a silently wrong average.
    fn finalize(&self) -> Value {
        let sum = &self.sum;
        let count = sum.addend_count();
        if count == 0 {
            return Value::Null;
        }
        if sum.decimal_count > 0 {
            if !sum.floats.is_empty() {
                return Value::Null;
            }
            return sum
                .exact_decimal_total()
                .and_then(|total| total.checked_div(Decimal::from(count)))
                .map_or(Value::Null, Value::Decimal);
        }
        let total = if sum.floats.is_empty() {
            sum.int_sum as f64
        } else {
            sum.float_total()
        };
        Value::Float(total / count as f64)
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
/// `v·w` and its weight land in the part of the row's domain, held exactly:
///
/// - a row of two integers adds its exact product and weight to `i128`
///   totals;
/// - a row with a float operand (and no decimal) adds its product, rounded
///   once for that row (`v as f64 * w as f64`, which does not depend on any
///   other row), to an exact float sum of products, and its weight to an
///   exact float sum of weights, or to the integer weight total when the
///   weight is an integer;
/// - a row with a decimal operand adds its exact product and weight to
///   `Decimal` totals.
///
/// The result's type follows from the row counts, not from the order rows
/// arrived in: any decimal row gives the exact decimal quotient (the decimal
/// and integer totals combined); else any float row gives
/// `round(products) / round(weights)`, each total rounded once; else the
/// integer totals' quotient as a float. Zero total weight → Null (V-7-2a).
/// Adding, merging and subtracting never round, so the result depends only on
/// the multiset of rows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeightedAvgState {
    /// Σ vᵢ·wᵢ over the rows of two integers.
    pub int_products: i128,
    /// Σ wᵢ over every integer weight of a row without a decimal operand.
    pub int_weights: i128,
    /// Rows of two integers.
    pub int_rows: u64,
    /// Each float row's product, rounded once per row, summed exactly. Its
    /// addend count is the number of float rows.
    pub float_products: ExactSum,
    /// The float rows' float weights, summed exactly.
    pub float_weights: ExactSum,
    /// Σ vᵢ·wᵢ over the decimal rows, exactly: a `decimal·int` product widens
    /// exactly via `Decimal::from` and `decimal·decimal` is exact.
    #[serde(with = "crate::decimal_serde")]
    pub decimal_products: Decimal,
    /// Σ wᵢ over the decimal rows, exactly.
    #[serde(with = "crate::decimal_serde")]
    pub decimal_weights: Decimal,
    /// Rows with a decimal operand, including a row that mixes a decimal with
    /// a float (reachable only on an untyped column), which also counts as a
    /// float row so that the result is Null while it is held.
    pub decimal_rows: u64,
    /// Set once an exact decimal total leaves `Decimal`'s ~7.9e28 range;
    /// finalize then returns `Null` rather than a silently-wrong weighted
    /// average (weighted_avg has no error channel). Sticky: an overflowed sum
    /// is an error outcome, and subtracting a row does not undo it.
    pub decimal_overflow: bool,
}

impl Default for WeightedAvgState {
    fn default() -> Self {
        Self {
            int_products: 0,
            int_weights: 0,
            int_rows: 0,
            float_products: ExactSum::new(),
            float_weights: ExactSum::new(),
            decimal_products: Decimal::ZERO,
            decimal_weights: Decimal::ZERO,
            decimal_rows: 0,
            decimal_overflow: false,
        }
    }
}

/// A numeric operand of `weighted_avg`, kept in exact form so the decimal
/// path never reconstructs a lossy float. Null and non-numeric values are
/// filtered by [`Operand::numeric`] (which returns `None`) and skip the row.
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

    fn is_decimal(&self) -> bool {
        matches!(self, Operand::Decimal(_))
    }

    /// The f64 projection a float row's product is computed from. A decimal
    /// reaches it only in a row that mixes a decimal with a float, whose
    /// result is Null.
    fn as_f64(&self) -> f64 {
        match self {
            Operand::Int(n) => *n as f64,
            Operand::Float(f) => *f,
            Operand::Decimal(d) => d.to_f64().unwrap_or(f64::NAN),
        }
    }

    /// The operand as an exact `Decimal`, or `None` for a float (which cannot
    /// widen to decimal without loss).
    fn as_decimal(&self) -> Option<Decimal> {
        match self {
            Operand::Int(n) => Some(Decimal::from(*n)),
            Operand::Decimal(d) => Some(*d),
            Operand::Float(_) => None,
        }
    }
}

/// Which domain a weighted row belongs to, with its exact contribution.
enum WeightedRow {
    Integer {
        product: i128,
        weight: i128,
    },
    Float {
        product: f64,
        weight: Operand,
    },
    Decimal {
        product: Option<Decimal>,
        weight: Decimal,
    },
    /// A decimal operand with a float operand: counted as a decimal row and a
    /// float row, so the result is Null while the row is held.
    DecimalWithFloat {
        product: f64,
        weight: Operand,
    },
}

impl WeightedRow {
    /// Classify a row, or `None` when either operand is null or non-numeric
    /// (the row is skipped, as SQL skips nulls).
    fn classify(value: &Value, weight: &Value) -> Option<Self> {
        let (v, w) = (Operand::numeric(value)?, Operand::numeric(weight)?);
        let float_product = || v.as_f64() * w.as_f64();
        Some(if v.is_decimal() || w.is_decimal() {
            match (v.as_decimal(), w.as_decimal()) {
                (Some(vd), Some(wd)) => WeightedRow::Decimal {
                    product: vd.checked_mul(wd),
                    weight: wd,
                },
                _ => WeightedRow::DecimalWithFloat {
                    product: float_product(),
                    weight: w,
                },
            }
        } else if let (Operand::Int(vi), Operand::Int(wi)) = (v, w) {
            WeightedRow::Integer {
                product: i128::from(vi) * i128::from(wi),
                weight: i128::from(wi),
            }
        } else {
            WeightedRow::Float {
                product: float_product(),
                weight: w,
            }
        })
    }
}

impl WeightedAvgState {
    /// Add one row. Returns the heap bytes this allocated (an exact float sum's
    /// state at its first addend), else 0.
    fn add_weighted(&mut self, value: &Value, weight: &Value) -> usize {
        let Some(row) = WeightedRow::classify(value, weight) else {
            return 0;
        };
        match row {
            WeightedRow::Integer { product, weight } => {
                self.int_products += product;
                self.int_weights += weight;
                self.int_rows += 1;
                0
            }
            WeightedRow::Float { product, weight } => self.add_float_row(product, weight),
            WeightedRow::Decimal { product, weight } => {
                self.decimal_rows += 1;
                self.decimal_add(product, weight);
                0
            }
            WeightedRow::DecimalWithFloat { product, weight } => {
                self.decimal_rows += 1;
                self.add_float_row(product, weight)
            }
        }
    }

    /// Subtract one row this state holds: the exact inverse of
    /// [`add_weighted`](Self::add_weighted) with the same operands, so the
    /// state afterwards equals one that never saw the row. Returns the heap
    /// delta (negative when an exact float sum loses its last addend).
    fn sub_weighted(&mut self, value: &Value, weight: &Value) -> isize {
        let Some(row) = WeightedRow::classify(value, weight) else {
            return 0;
        };
        match row {
            WeightedRow::Integer { product, weight } => {
                self.int_products -= product;
                self.int_weights -= weight;
                self.int_rows = self.int_rows.saturating_sub(1);
                0
            }
            WeightedRow::Float { product, weight } => self.sub_float_row(product, weight),
            WeightedRow::Decimal { product, weight } => {
                self.decimal_rows = self.decimal_rows.saturating_sub(1);
                self.decimal_add(product.map(|p| -p), -weight);
                self.reset_empty_decimal_totals();
                0
            }
            WeightedRow::DecimalWithFloat { product, weight } => {
                self.decimal_rows = self.decimal_rows.saturating_sub(1);
                self.reset_empty_decimal_totals();
                self.sub_float_row(product, weight)
            }
        }
    }

    fn add_float_row(&mut self, product: f64, weight: Operand) -> usize {
        let mut delta = self.float_products.add_f64(product);
        match weight {
            Operand::Int(w) => self.int_weights += i128::from(w),
            Operand::Float(w) => delta += self.float_weights.add_f64(w),
            // A decimal weight only occurs in a row mixing it with a float,
            // whose result is Null; it joins no total.
            Operand::Decimal(_) => {}
        }
        delta
    }

    fn sub_float_row(&mut self, product: f64, weight: Operand) -> isize {
        let mut delta = self.float_products.sub_f64(product);
        match weight {
            Operand::Int(w) => self.int_weights -= i128::from(w),
            Operand::Float(w) => delta += self.float_weights.sub_f64(w),
            Operand::Decimal(_) => {}
        }
        delta
    }

    /// Fold one decimal row's exact product (`None` when it overflowed) and
    /// weight into the decimal totals; an out-of-range total sets the overflow
    /// flag rather than panicking.
    fn decimal_add(&mut self, product: Option<Decimal>, weight: Decimal) {
        match product.and_then(|p| self.decimal_products.checked_add(p)) {
            Some(sum) => self.decimal_products = sum,
            None => self.decimal_overflow = true,
        }
        match self.decimal_weights.checked_add(weight) {
            Some(sum) => self.decimal_weights = sum,
            None => self.decimal_overflow = true,
        }
    }

    /// With no decimal row left the exact totals are zero again; drop the
    /// scale they kept, so the state equals one that never saw a decimal row.
    fn reset_empty_decimal_totals(&mut self) {
        if self.decimal_rows == 0 {
            self.decimal_products = Decimal::ZERO;
            self.decimal_weights = Decimal::ZERO;
        }
    }

    /// Add every row of `other`, part by part. A merge that gives an exact
    /// float sum its first addends allocates; [`AccumulatorEnum::heap_size`]
    /// reports it.
    fn merge(&mut self, other: &WeightedAvgState) {
        self.int_products += other.int_products;
        self.int_weights += other.int_weights;
        self.int_rows += other.int_rows;
        self.float_products.merge(&other.float_products);
        self.float_weights.merge(&other.float_weights);
        if other.decimal_rows > 0 {
            self.decimal_rows += other.decimal_rows;
            self.decimal_add(Some(other.decimal_products), other.decimal_weights);
        }
        self.decimal_overflow |= other.decimal_overflow;
    }

    fn heap_size(&self) -> usize {
        self.float_products.heap_size() + self.float_weights.heap_size()
    }

    fn finalize(&self) -> Value {
        let float_rows = self.float_products.count();
        if self.decimal_rows > 0 {
            // A float row cannot join an exact decimal total, and an
            // overflowed total has no honest value; weighted_avg has no error
            // channel, so both give Null rather than a biased average.
            if float_rows > 0 || self.decimal_overflow {
                return Value::Null;
            }
            let products = i128_to_decimal(self.int_products)
                .and_then(|ints| self.decimal_products.checked_add(ints));
            let weights = i128_to_decimal(self.int_weights)
                .and_then(|ints| self.decimal_weights.checked_add(ints));
            return match (products, weights) {
                // V-7-2a: zero total weight → Null (prevents a divide-by-zero).
                (Some(p), Some(w)) if !w.is_zero() => {
                    p.checked_div(w).map_or(Value::Null, Value::Decimal)
                }
                _ => Value::Null,
            };
        }
        let (products, weights) = if float_rows > 0 {
            (
                round_float_sum(&self.float_products, self.int_products, self.int_rows),
                self.float_weights.round_with(self.int_weights),
            )
        } else if self.int_rows > 0 {
            (self.int_products as f64, self.int_weights as f64)
        } else {
            return Value::Null;
        };
        // V-7-2a: zero total weight → Null (prevents NaN/Infinity). An exact
        // nonzero total never rounds to zero.
        if weights == 0.0 {
            return Value::Null;
        }
        Value::Float(products / weights)
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
    /// tracking: `Sum` and `Avg` return their exact float sum's allocation at
    /// the first float addend; `Collect` returns the size of one `Value` slot
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
    /// hold every part exactly (integers, an exact float sum, decimals), so
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
    /// [`add`](Self::add) does: the exact float sums' allocations at their
    /// first addends, else 0. No-op returning 0 on other variants
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
    /// exactly. Returns the heap bytes delta (negative when an exact float sum
    /// loses its last addend and frees its allocation). No-op returning 0 on
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
    /// or `Avg` removing its last float addend, which frees the exact float
    /// sum's allocation) and zero otherwise. `Min` and `Max`
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
    /// Returns `AccumulatorError::SumOverflow` if a `Sum`, `Avg`, or
    /// `WeightedAvg` integer result exceeds `i64` range.
    pub fn finalize(&self) -> Result<Value, AccumulatorError> {
        match self {
            Self::Sum(s) => s.finalize(),
            Self::Count(s) => Ok(s.finalize()),
            Self::Avg(s) => Ok(s.finalize()),
            Self::Min(s) => Ok(s.finalize()),
            Self::Max(s) => Ok(s.finalize()),
            Self::Collect(s) => Ok(s.finalize()),
            Self::WeightedAvg(s) => Ok(s.finalize()),
            Self::Any(s) => Ok(s.finalize()),
        }
    }

    /// Estimated heap size for memory tracking. Fixed-size variants return
    /// `size_of::<Self>()` (inline enum footprint, no heap); `Sum`, `Avg` and
    /// `WeightedAvg` add their exact float sums' allocations, if they hold
    /// any. Collect reports `Vec` capacity × `size_of::<Value>()` plus each
    /// value's own heap.
    ///
    /// Report `Vec` capacity, not `len()` — len-based reporting has caused up
    /// to 19× undercounts in DataFusion (arrow-rs issue #13831).
    pub fn heap_size(&self) -> usize {
        match self {
            Self::Sum(s) => std::mem::size_of::<Self>() + s.floats.heap_size(),
            Self::Avg(s) => std::mem::size_of::<Self>() + s.sum.floats.heap_size(),
            Self::WeightedAvg(s) => std::mem::size_of::<Self>() + s.heap_size(),
            Self::Collect(s) => s.heap_size(),
            Self::Any(s) => std::mem::size_of::<Self>() + s.heap_size(),
            _ => std::mem::size_of::<Self>(),
        }
    }
}
