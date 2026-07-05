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
//! Serde derive on `AccumulatorEnum` and all state structs enables spill
//! serialization without manual state/restore code.

use std::cmp::Ordering;

use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize};

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

/// Sum accumulator state: i128 integer path + Kahan compensated f64 path.
///
/// Uses the DuckDB HUGEINT pattern: i128 internal accumulation cannot overflow
/// at ETL scale (would need >2×10^19 rows at i64::MAX each). Finalize via
/// `i64::try_from` — NEVER `as i64`, which silently wraps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SumState {
    /// i128 internal accumulation for integer inputs. Infallible at ETL scale.
    pub int_sum: i128,
    /// Kahan compensated sum for float inputs.
    pub float_sum: f64,
    /// Kahan compensation term (running residual).
    pub compensation: f64,
    /// Exact running sum for `decimal` inputs. A `decimal` aggregate is
    /// type-homogeneous (typecheck keeps a column decimal), so this path and
    /// the int/float paths are not mixed in practice; `is_decimal` selects it.
    #[serde(with = "crate::decimal_serde")]
    pub decimal_sum: Decimal,
    /// Set once a `decimal` sum exceeds `Decimal`'s ~7.9e28 range; finalize
    /// then surfaces `SumOverflow` rather than a silently-wrong total.
    pub decimal_overflow: bool,
    /// False until first non-null value observed.
    pub has_value: bool,
    /// True while all observed values have been integers. Flips false on
    /// first Float, and the finalize path returns Float thereafter.
    pub is_integer: bool,
    /// True once a `decimal` value has been observed; finalize then returns
    /// an exact `Value::Decimal`.
    pub is_decimal: bool,
}

impl Default for SumState {
    fn default() -> Self {
        Self {
            int_sum: 0,
            float_sum: 0.0,
            compensation: 0.0,
            decimal_sum: Decimal::ZERO,
            decimal_overflow: false,
            has_value: false,
            is_integer: true,
            is_decimal: false,
        }
    }
}

impl SumState {
    /// Kahan compensated summation step.
    fn kahan_add(&mut self, val: f64) {
        let y = val - self.compensation;
        let t = self.float_sum + y;
        self.compensation = (t - self.float_sum) - y;
        self.float_sum = t;
    }

    fn add(&mut self, value: &Value) {
        match value {
            Value::Null => {}
            Value::Integer(n) => {
                self.has_value = true;
                self.int_sum += *n as i128;
                if !self.is_integer {
                    // Already in float mode: also accumulate in float path.
                    self.kahan_add(*n as f64);
                }
                if self.is_decimal {
                    // Already in exact-decimal mode: fold the integer in
                    // exactly too, so an integer observed after a decimal
                    // (`sum(if flag then amount else 1)`) is not lost at
                    // finalize, which returns `decimal_sum`.
                    self.decimal_add(Decimal::from(*n));
                }
            }
            Value::Float(f) => {
                self.has_value = true;
                if self.is_integer {
                    // First float: seed float path from current integer sum.
                    self.is_integer = false;
                    self.float_sum = self.int_sum as f64;
                    self.compensation = 0.0;
                }
                self.kahan_add(*f);
            }
            Value::Decimal(d) => {
                self.has_value = true;
                self.enter_decimal_mode();
                self.decimal_add(*d);
            }
            _ => {
                // Non-numeric: skip (SQL SUM on non-numeric is undefined;
                // typecheck rejects at plan time, so this is defence-in-depth).
            }
        }
    }

    /// Switch to the exact decimal path, folding the integers already summed
    /// into the decimal accumulator so subsequent integers (added incrementally
    /// by the `add`/`sub` integer arms) and decimals compose exactly. A decimal
    /// aggregate is type-homogeneous per typecheck, so `int_sum` is normally 0
    /// here; folding it in keeps the result exact if the paths ever mix (via a
    /// conditional whose branches unify `Decimal` and `Int`). An `int_sum` too
    /// large to represent as a `Decimal` sets the overflow flag rather than
    /// panicking.
    fn enter_decimal_mode(&mut self) {
        if !self.is_decimal {
            self.is_decimal = true;
            match i128_to_decimal(self.int_sum) {
                Some(d) => self.decimal_sum = d,
                None => {
                    self.decimal_sum = Decimal::ZERO;
                    self.decimal_overflow = true;
                }
            }
        }
    }

    fn decimal_add(&mut self, d: Decimal) {
        match self.decimal_sum.checked_add(d) {
            Some(sum) => self.decimal_sum = sum,
            None => self.decimal_overflow = true,
        }
    }

    /// This state's exact decimal total: `decimal_sum` (which already folds its
    /// integers) when in decimal mode, else its integer sum promoted to a
    /// decimal. `None` when a pure-integer sum is too large to represent.
    fn decimal_contribution(&self) -> Option<Decimal> {
        if self.is_decimal {
            Some(self.decimal_sum)
        } else {
            i128_to_decimal(self.int_sum)
        }
    }

    fn merge(&mut self, other: &SumState) {
        if !other.has_value {
            return;
        }
        self.has_value = true;
        // Combine the exact decimal totals BEFORE mutating `int_sum`, so each
        // side's contribution is read from its own (un-merged) integer sum —
        // avoiding both the double-count (folding `other`'s integers twice) and
        // the drop (losing an integer-only `other` when `self` is decimal).
        if self.is_decimal || other.is_decimal {
            // Read each side's contribution BEFORE flipping `self.is_decimal`,
            // so an integer-only `self` promotes its `int_sum` rather than
            // reading an unseeded `decimal_sum`.
            let self_dec = self.decimal_contribution();
            let other_dec = other.decimal_contribution();
            self.is_decimal = true;
            match (self_dec, other_dec) {
                (Some(a), Some(b)) => match a.checked_add(b) {
                    Some(sum) => self.decimal_sum = sum,
                    None => self.decimal_overflow = true,
                },
                // An integer-only side whose sum exceeds Decimal's range.
                _ => self.decimal_overflow = true,
            }
            self.decimal_overflow |= other.decimal_overflow;
        }
        self.int_sum += other.int_sum;
        if !other.is_integer {
            if self.is_integer {
                // Seed float path from our current int sum.
                self.is_integer = false;
                self.float_sum = (self.int_sum - other.int_sum) as f64;
                self.compensation = 0.0;
            }
            // Add other's compensated total into our Kahan sum.
            self.kahan_add(other.float_sum);
            self.kahan_add(-other.compensation);
        }
    }

    fn finalize(&self) -> Result<Value, AccumulatorError> {
        if !self.has_value {
            return Ok(Value::Null);
        }
        if self.is_decimal {
            if self.decimal_overflow {
                return Err(AccumulatorError::SumOverflow { field: None });
            }
            return Ok(Value::Decimal(self.decimal_sum));
        }
        if self.is_integer {
            let n = i64::try_from(self.int_sum)
                .map_err(|_| AccumulatorError::SumOverflow { field: None })?;
            Ok(Value::Integer(n))
        } else {
            Ok(Value::Float(self.float_sum))
        }
    }
}

/// Subtract one previously-added value from a `SumState`.
///
/// Symmetric inverse of `SumState::add`: integer path subtracts from the
/// i128 accumulator; float path runs Kahan compensated subtraction (negate
/// the value, feed through `kahan_add` so the residual updates the same
/// way an add would). `has_value` is left at `true` once set — the empty-
/// group sentinel for retraction is "every row retracted" which the caller
/// detects via the surrounding `retract_row` count, not by clearing the
/// flag; finalize on a fully-retracted Sum returns `Integer(0)` /
/// `Float(0.0)`, byte-identical to a feed-from-scratch on the surviving
/// (empty) row set after the per-group state was built.
fn sum_state_sub(s: &mut SumState, value: &Value) {
    match value {
        Value::Null => {}
        Value::Integer(n) => {
            s.int_sum -= *n as i128;
            if !s.is_integer {
                s.kahan_add(-(*n as f64));
            }
            if s.is_decimal {
                // Symmetric with the decimal fold in `add`'s integer arm.
                s.decimal_add(-Decimal::from(*n));
            }
        }
        Value::Float(f) => {
            if s.is_integer {
                // First sub on a still-integer state with a float value
                // promotes the same way `add` does — seed the Kahan path
                // from the running int sum so the subtraction's drift is
                // bounded by what an equivalent add-then-add(-x) would
                // produce.
                s.is_integer = false;
                s.float_sum = s.int_sum as f64;
                s.compensation = 0.0;
            }
            s.kahan_add(-(*f));
        }
        Value::Decimal(d) => {
            s.enter_decimal_mode();
            s.decimal_add(-*d);
        }
        _ => {}
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

/// Average accumulator state: i128 sum + Kahan f64 path + u64 count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AvgState {
    pub int_sum: i128,
    pub float_sum: f64,
    pub compensation: f64,
    /// Exact running sum for `decimal` inputs (see `SumState::decimal_sum`).
    #[serde(with = "crate::decimal_serde")]
    pub decimal_sum: Decimal,
    /// Set if the exact decimal running sum overflowed; finalize then returns
    /// `Null` rather than a silently-wrong (biased) average, since avg has no
    /// error channel.
    pub decimal_overflow: bool,
    pub count: u64,
    pub is_integer: bool,
    /// True once a `decimal` value has been observed; finalize then returns an
    /// exact `Value::Decimal` quotient.
    pub is_decimal: bool,
    pub has_value: bool,
}

impl Default for AvgState {
    fn default() -> Self {
        Self {
            int_sum: 0,
            float_sum: 0.0,
            compensation: 0.0,
            decimal_sum: Decimal::ZERO,
            decimal_overflow: false,
            count: 0,
            is_integer: true,
            is_decimal: false,
            has_value: false,
        }
    }
}

impl AvgState {
    fn kahan_add(&mut self, val: f64) {
        let y = val - self.compensation;
        let t = self.float_sum + y;
        self.compensation = (t - self.float_sum) - y;
        self.float_sum = t;
    }

    fn add(&mut self, value: &Value) {
        match value {
            Value::Null => {}
            Value::Integer(n) => {
                self.has_value = true;
                self.count += 1;
                self.int_sum += *n as i128;
                if !self.is_integer {
                    self.kahan_add(*n as f64);
                }
                if self.is_decimal {
                    // Fold integers observed after decimal mode into the exact
                    // sum, so they are not lost from the decimal quotient.
                    self.decimal_add(Decimal::from(*n));
                }
            }
            Value::Float(f) => {
                self.has_value = true;
                self.count += 1;
                if self.is_integer {
                    self.is_integer = false;
                    self.float_sum = self.int_sum as f64;
                    self.compensation = 0.0;
                }
                self.kahan_add(*f);
                if self.is_decimal {
                    // A binary float observed after decimal mode lands only in
                    // the Kahan float sum, which the decimal quotient never
                    // reads; poison rather than silently drop it from the avg.
                    self.decimal_overflow = true;
                }
            }
            Value::Decimal(d) => {
                self.has_value = true;
                self.count += 1;
                self.enter_decimal_mode();
                self.decimal_add(*d);
            }
            _ => {}
        }
    }

    /// Enter the exact decimal path, seeding from the integers already summed
    /// (fallible — an out-of-range integer sum sets the overflow flag rather
    /// than panicking). A binary float accumulated before the first decimal row
    /// (an `Any`-typed column that mixed a float and a decimal — typed columns
    /// cannot) cannot widen to an exact decimal; the seed pulls in only the
    /// integer sum, so poison the result rather than silently drop the float.
    fn enter_decimal_mode(&mut self) {
        if !self.is_decimal {
            self.is_decimal = true;
            if !self.is_integer {
                self.decimal_overflow = true;
            }
            match i128_to_decimal(self.int_sum) {
                Some(d) => self.decimal_sum = d,
                None => {
                    self.decimal_sum = Decimal::ZERO;
                    self.decimal_overflow = true;
                }
            }
        }
    }

    fn decimal_add(&mut self, d: Decimal) {
        match self.decimal_sum.checked_add(d) {
            Some(sum) => self.decimal_sum = sum,
            None => self.decimal_overflow = true,
        }
    }

    fn decimal_contribution(&self) -> Option<Decimal> {
        if self.is_decimal {
            Some(self.decimal_sum)
        } else {
            i128_to_decimal(self.int_sum)
        }
    }

    fn merge(&mut self, other: &AvgState) {
        if !other.has_value {
            return;
        }
        self.has_value = true;
        // Combine exact decimal totals from each side's own integer sum BEFORE
        // mutating `int_sum` (see `SumState::merge`), so an integer-only side is
        // neither double-counted nor dropped.
        if self.is_decimal || other.is_decimal {
            // Read contributions before flipping `self.is_decimal` (see
            // `SumState::merge`).
            let self_dec = self.decimal_contribution();
            let other_dec = other.decimal_contribution();
            // A float-mode side (not decimal, not integer-only) contributes only
            // its integer sum above; its binary-float total cannot widen to an
            // exact decimal. Combining one into the decimal domain would silently
            // drop it, so poison the result to Null instead.
            if (!self.is_decimal && !self.is_integer) || (!other.is_decimal && !other.is_integer) {
                self.decimal_overflow = true;
            }
            self.is_decimal = true;
            match (self_dec, other_dec) {
                (Some(a), Some(b)) => match a.checked_add(b) {
                    Some(sum) => self.decimal_sum = sum,
                    None => self.decimal_overflow = true,
                },
                _ => self.decimal_overflow = true,
            }
            self.decimal_overflow |= other.decimal_overflow;
        }
        self.count += other.count;
        self.int_sum += other.int_sum;
        if !other.is_integer {
            if self.is_integer {
                self.is_integer = false;
                self.float_sum = (self.int_sum - other.int_sum) as f64;
                self.compensation = 0.0;
            }
            self.kahan_add(other.float_sum);
            self.kahan_add(-other.compensation);
        }
    }

    /// Avg returns Float for int/float inputs; for `decimal` inputs it returns
    /// an exact `Value::Decimal` quotient at full division precision (rounding
    /// to a declared `scale` happens when the value lands in a scaled decimal
    /// column, matching intermediate `decimal / decimal` division semantics).
    /// An overflowed decimal sum yields `Null` (avg has no error channel).
    fn finalize(&self) -> Value {
        if !self.has_value || self.count == 0 {
            return Value::Null;
        }
        if self.is_decimal {
            if self.decimal_overflow {
                return Value::Null;
            }
            return match self.decimal_sum.checked_div(Decimal::from(self.count)) {
                Some(avg) => Value::Decimal(avg),
                None => Value::Null,
            };
        }
        let sum = if self.is_integer {
            self.int_sum as f64
        } else {
            self.float_sum
        };
        Value::Float(sum / self.count as f64)
    }
}

// ----------------------------------------------------------------------------

/// Min/Max state — stores the extremum `Value` seen so far. The comparison
/// direction (Min vs Max) is determined by the `AccumulatorEnum` variant, not
/// by a flag on the state.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct MinMaxState {
    pub current: Option<Value>,
}

impl MinMaxState {
    fn add_with(&mut self, value: &Value, keep_if: Ordering) {
        if value.is_null() {
            return;
        }
        match &self.current {
            None => self.current = Some(value.clone()),
            Some(cur) => {
                // Cross-type comparisons return None — skip the incomparable
                // value here; the executor DLQs records with type conflicts.
                if let Some(cmp) = value.partial_cmp(cur)
                    && cmp == keep_if
                {
                    self.current = Some(value.clone());
                }
            }
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
        Value::Array(self.values.clone())
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

/// Weighted average state. Two-argument: value + weight. i128 weighted sum
/// plus i128 weight sum, with Kahan compensated float paths for both and an
/// exact `Decimal` path for `decimal` inputs (mirroring `AvgState`).
///
/// Finalize: weighted_sum / weight_sum. Zero total weight → Null (V-7-2a).
/// A `decimal` value or weight yields the exact full-precision quotient; the
/// int/float paths keep the prior binary-float behavior.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeightedAvgState {
    pub int_weighted_sum: i128,
    pub float_weighted_sum: f64,
    pub compensation: f64,
    pub weight_sum_int: i128,
    pub weight_sum_float: f64,
    pub weight_compensation: f64,
    /// Exact running weighted sum (`Σ vᵢ·wᵢ`) once a `decimal` operand is
    /// seen. A `decimal·int` product widens exactly via `Decimal::from` and
    /// `decimal·decimal` is exact; `is_decimal` selects this path at finalize.
    #[serde(with = "crate::decimal_serde")]
    pub decimal_weighted_sum: Decimal,
    /// Exact running weight total (`Σ wᵢ`) in the decimal domain, paired with
    /// `decimal_weighted_sum` for a full-precision quotient at finalize.
    #[serde(with = "crate::decimal_serde")]
    pub decimal_weight_sum: Decimal,
    /// Set once an exact decimal sum leaves `Decimal`'s ~7.9e28 range (or a
    /// decimal is mixed with a binary float, which cannot fold exactly);
    /// finalize then returns `Null` rather than a silently-wrong weighted
    /// average (weighted_avg has no error channel).
    pub decimal_overflow: bool,
    pub is_integer: bool,
    /// True once a `decimal` operand has been observed; finalize then returns
    /// an exact `Value::Decimal` quotient.
    pub is_decimal: bool,
    pub has_value: bool,
}

impl Default for WeightedAvgState {
    fn default() -> Self {
        Self {
            int_weighted_sum: 0,
            float_weighted_sum: 0.0,
            compensation: 0.0,
            weight_sum_int: 0,
            weight_sum_float: 0.0,
            weight_compensation: 0.0,
            decimal_weighted_sum: Decimal::ZERO,
            decimal_weight_sum: Decimal::ZERO,
            decimal_overflow: false,
            is_integer: true,
            is_decimal: false,
            has_value: false,
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

    /// The exact integer value, or `None` for a float/decimal (which the i128
    /// fast path cannot take).
    fn as_int(&self) -> Option<i64> {
        match self {
            Operand::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// The f64 projection used by the compensated float path. Reached only for
    /// integers and floats — decimals take the exact path — but total for
    /// completeness.
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

impl WeightedAvgState {
    fn kahan_add_val(&mut self, v: f64) {
        let y = v - self.compensation;
        let t = self.float_weighted_sum + y;
        self.compensation = (t - self.float_weighted_sum) - y;
        self.float_weighted_sum = t;
    }

    fn kahan_add_weight(&mut self, w: f64) {
        let y = w - self.weight_compensation;
        let t = self.weight_sum_float + y;
        self.weight_compensation = (t - self.weight_sum_float) - y;
        self.weight_sum_float = t;
    }

    fn add_weighted(&mut self, value: &Value, weight: &Value) {
        // A null or non-numeric operand in either position skips the row and
        // does not mark `has_value` (matches the prior behavior and SQL null
        // handling).
        let (Some(v), Some(w)) = (Operand::numeric(value), Operand::numeric(weight)) else {
            return;
        };
        self.has_value = true;

        // Exact decimal path: taken as soon as either operand is a decimal, and
        // for every row thereafter (so integers observed after the first
        // decimal fold into the exact sums instead of being dropped from the
        // quotient). A `decimal·int` product widens exactly via `Decimal::from`
        // and `decimal·decimal` is exact. A decimal mixed with a binary float
        // is a typecheck error, so that combination reaches this path only on
        // an untyped column, where the result is poisoned to Null rather than
        // emitting a silently-wrong average — `decimal_add_weighted` catches a
        // float in the same row, and `enter_decimal_mode` catches a float
        // accumulated before this first decimal row.
        if self.is_decimal || v.is_decimal() || w.is_decimal() {
            self.enter_decimal_mode();
            self.decimal_add_weighted(&v, &w);
            return;
        }

        // No decimal in play: exact i128 product when both operands are
        // integers, else the Kahan-compensated float path.
        match (v.as_int(), w.as_int()) {
            (Some(vi), Some(wi)) => {
                self.int_weighted_sum += vi as i128 * wi as i128;
                self.weight_sum_int += wi as i128;
                if !self.is_integer {
                    // Already float mode: mirror into the float paths too.
                    self.kahan_add_val(v.as_f64() * w.as_f64());
                    self.kahan_add_weight(w.as_f64());
                }
            }
            _ => {
                if self.is_integer {
                    // First float operand: seed the float paths from the
                    // integer state.
                    self.is_integer = false;
                    self.float_weighted_sum = self.int_weighted_sum as f64;
                    self.weight_sum_float = self.weight_sum_int as f64;
                    self.compensation = 0.0;
                    self.weight_compensation = 0.0;
                }
                self.kahan_add_val(v.as_f64() * w.as_f64());
                self.kahan_add_weight(w.as_f64());
            }
        }
    }

    /// Switch to the exact decimal path, seeding both running sums from the
    /// integers already accumulated (`Σ vᵢ·wᵢ` and `Σ wᵢ`). Fallible — an
    /// integer sum beyond `Decimal`'s ~7.9e28 range sets the overflow flag
    /// rather than panicking (`Decimal::from_i128_with_scale` would panic).
    /// Float contributions accumulated before the first decimal row (an
    /// `Any`-typed column that mixed a binary float and a decimal — typed
    /// columns cannot) cannot widen to an exact decimal; the seed only pulls in
    /// the integer sums, so poison the result rather than silently drop them.
    fn enter_decimal_mode(&mut self) {
        if !self.is_decimal {
            self.is_decimal = true;
            if !self.is_integer {
                self.decimal_overflow = true;
            }
            match i128_to_decimal(self.int_weighted_sum) {
                Some(d) => self.decimal_weighted_sum = d,
                None => {
                    self.decimal_weighted_sum = Decimal::ZERO;
                    self.decimal_overflow = true;
                }
            }
            match i128_to_decimal(self.weight_sum_int) {
                Some(d) => self.decimal_weight_sum = d,
                None => {
                    self.decimal_weight_sum = Decimal::ZERO;
                    self.decimal_overflow = true;
                }
            }
        }
    }

    /// Fold one `(value, weight)` row into the exact decimal accumulators.
    /// Both operands are integers or decimals here; a binary float mixed with
    /// a decimal cannot widen exactly and sets the overflow flag so finalize
    /// returns Null instead of a silently-wrong total. `checked_*` guards keep
    /// an out-of-range intermediate from panicking.
    fn decimal_add_weighted(&mut self, value: &Operand, weight: &Operand) {
        let (Some(v), Some(w)) = (value.as_decimal(), weight.as_decimal()) else {
            self.decimal_overflow = true;
            return;
        };
        match v.checked_mul(w) {
            Some(prod) => match self.decimal_weighted_sum.checked_add(prod) {
                Some(sum) => self.decimal_weighted_sum = sum,
                None => self.decimal_overflow = true,
            },
            None => self.decimal_overflow = true,
        }
        match self.decimal_weight_sum.checked_add(w) {
            Some(sum) => self.decimal_weight_sum = sum,
            None => self.decimal_overflow = true,
        }
    }

    /// This state's exact weighted-sum total (`Σ vᵢ·wᵢ`): the decimal
    /// accumulator when in decimal mode (which already folds its integers),
    /// else the integer weighted sum promoted to a decimal. `None` when a pure
    /// integer sum is too large to represent as a `Decimal`.
    fn decimal_weighted_contribution(&self) -> Option<Decimal> {
        if self.is_decimal {
            Some(self.decimal_weighted_sum)
        } else {
            i128_to_decimal(self.int_weighted_sum)
        }
    }

    /// This state's exact weight total (`Σ wᵢ`), analogous to
    /// [`decimal_weighted_contribution`](Self::decimal_weighted_contribution).
    fn decimal_weight_contribution(&self) -> Option<Decimal> {
        if self.is_decimal {
            Some(self.decimal_weight_sum)
        } else {
            i128_to_decimal(self.weight_sum_int)
        }
    }

    fn merge(&mut self, other: &WeightedAvgState) {
        if !other.has_value {
            return;
        }
        self.has_value = true;
        // Combine each side's exact decimal totals from its own (un-merged)
        // integer sums BEFORE mutating them (see `SumState::merge`), so an
        // integer-only side is neither double-counted nor dropped. Read both
        // contributions before flipping `self.is_decimal` so an integer-only
        // `self` promotes its int sums rather than reading unseeded decimals.
        if self.is_decimal || other.is_decimal {
            let self_weighted = self.decimal_weighted_contribution();
            let other_weighted = other.decimal_weighted_contribution();
            let self_weight = self.decimal_weight_contribution();
            let other_weight = other.decimal_weight_contribution();
            // A float-mode side (not decimal, not integer-only) contributes
            // only its integer sums above; its binary-float totals cannot widen
            // to an exact decimal. Combining one into the decimal domain would
            // silently drop them, so poison the result to Null instead.
            if (!self.is_decimal && !self.is_integer) || (!other.is_decimal && !other.is_integer) {
                self.decimal_overflow = true;
            }
            self.is_decimal = true;
            match (self_weighted, other_weighted) {
                (Some(a), Some(b)) => match a.checked_add(b) {
                    Some(sum) => self.decimal_weighted_sum = sum,
                    None => self.decimal_overflow = true,
                },
                _ => self.decimal_overflow = true,
            }
            match (self_weight, other_weight) {
                (Some(a), Some(b)) => match a.checked_add(b) {
                    Some(sum) => self.decimal_weight_sum = sum,
                    None => self.decimal_overflow = true,
                },
                _ => self.decimal_overflow = true,
            }
            self.decimal_overflow |= other.decimal_overflow;
        }
        self.int_weighted_sum += other.int_weighted_sum;
        self.weight_sum_int += other.weight_sum_int;
        if !other.is_integer {
            if self.is_integer {
                self.is_integer = false;
                self.float_weighted_sum = (self.int_weighted_sum - other.int_weighted_sum) as f64;
                self.weight_sum_float = (self.weight_sum_int - other.weight_sum_int) as f64;
                self.compensation = 0.0;
                self.weight_compensation = 0.0;
            }
            self.kahan_add_val(other.float_weighted_sum);
            self.kahan_add_val(-other.compensation);
            self.kahan_add_weight(other.weight_sum_float);
            self.kahan_add_weight(-other.weight_compensation);
        }
    }

    fn finalize(&self) -> Value {
        if !self.has_value {
            return Value::Null;
        }
        if self.is_decimal {
            // An overflowed (or decimal/float-mixed) exact sum has no honest
            // value — weighted_avg has no error channel — so return Null
            // rather than a biased average.
            if self.decimal_overflow {
                return Value::Null;
            }
            // V-7-2a: zero total weight → Null (prevents a divide-by-zero).
            if self.decimal_weight_sum.is_zero() {
                return Value::Null;
            }
            return match self
                .decimal_weighted_sum
                .checked_div(self.decimal_weight_sum)
            {
                Some(avg) => Value::Decimal(avg),
                None => Value::Null,
            };
        }
        let (weighted, weight) = if self.is_integer {
            (self.int_weighted_sum as f64, self.weight_sum_int as f64)
        } else {
            (self.float_weighted_sum, self.weight_sum_float)
        };
        // V-7-2a: zero total weight → Null (prevents NaN/Infinity).
        if weight == 0.0 {
            return Value::Null;
        }
        Value::Float(weighted / weight)
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
/// single contribution and recovers a state byte-equivalent to never having
/// observed it. `BufferRequired` variants must replay surviving inputs from
/// scratch — either because the operation is positional (`Min`, `Max`) or
/// because incremental retract under floating-point arithmetic accumulates
/// drift that diverges from a re-fold (`Avg`, `WeightedAvg`).
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
    /// tracking. Fixed-size variants return 0; `Collect` returns the size of
    /// one `Value` slot plus the value's own heap footprint.
    ///
    /// For `WeightedAvg`, this is a no-op — use `add_weighted` instead.
    pub fn add(&mut self, value: &Value) -> usize {
        match self {
            Self::Sum(s) => {
                s.add(value);
                0
            }
            Self::Count(s) => {
                s.add(value);
                0
            }
            Self::Avg(s) => {
                s.add(value);
                0
            }
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
    /// `Sum`, `Count`, `Collect`, `Any` are reversible: an inverse operation
    /// recovers a state equivalent to never having observed the retracted
    /// value. `Min` and `Max` are positional and need the full surviving
    /// multiset to recompute. `Avg` and `WeightedAvg` are mathematically
    /// reversible but classified `BufferRequired` because incremental
    /// retract on the Kahan-compensated f64 paths accumulates drift that
    /// diverges from a re-fold over surviving rows; the buffered path
    /// trades memory for byte-exact equivalence to a baseline rerun.
    pub const fn reversibility(&self) -> Reversibility {
        match self {
            Self::Sum(_) | Self::Count(_) | Self::Collect(_) | Self::Any(_) => {
                Reversibility::Reversible
            }
            Self::Min(_) | Self::Max(_) | Self::Avg(_) | Self::WeightedAvg(_) => {
                Reversibility::BufferRequired
            }
        }
    }

    /// Two-argument add for `WeightedAvg`. No-op on other variants
    /// (debug-asserts to catch programmer errors).
    pub fn add_weighted(&mut self, value: &Value, weight: &Value) {
        match self {
            Self::WeightedAvg(s) => s.add_weighted(value, weight),
            _ => debug_assert!(false, "add_weighted only valid for WeightedAvg"),
        }
    }

    /// Retract one previously-added value's contribution.
    ///
    /// Defined only on `Reversibility::Reversible` variants (`Sum`, `Count`,
    /// `Collect`, `Any`). Returns the heap-bytes delta for memory tracking
    /// — negative for shrink (`Collect` removing one slot, `Any` decrementing
    /// a refcount entry to zero) and zero for fixed-size variants.
    /// `BufferRequired` variants (`Min`, `Max`, `Avg`, `WeightedAvg`)
    /// debug-assert: their retraction path replays surviving rows from a
    /// per-group buffer rather than walking back state in place.
    pub fn sub(&mut self, value: &Value) -> isize {
        match self {
            Self::Sum(s) => {
                sum_state_sub(s, value);
                0
            }
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
            Self::Avg(_) | Self::Min(_) | Self::Max(_) | Self::WeightedAvg(_) => {
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
    /// `size_of::<Self>()` (inline enum footprint, no heap). Collect reports
    /// `Vec` capacity × `size_of::<Value>()` plus each value's own heap.
    ///
    /// Report `Vec` capacity, not `len()` — len-based reporting has caused up
    /// to 19× undercounts in DataFusion (arrow-rs issue #13831).
    pub fn heap_size(&self) -> usize {
        match self {
            Self::Collect(s) => s.heap_size(),
            Self::Any(s) => std::mem::size_of::<Self>() + s.heap_size(),
            _ => std::mem::size_of::<Self>(),
        }
    }
}
