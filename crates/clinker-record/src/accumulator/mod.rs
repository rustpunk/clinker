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
//! serialization without manual state/restore code (Decision #10).
//!
//! Research: RESEARCH-accumulator-merge-pattern.md, RESEARCH-sum-overflow.md,
//! RESEARCH-accumulator-memory-tracking.md.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::value::Value;

pub mod error;
pub use error::AccumulatorError;

#[cfg(test)]
mod tests;

// ============================================================================
// State structs
// ============================================================================

/// Sum accumulator state: i128 integer path + Kahan compensated f64 path.
///
/// Research: RESEARCH-sum-overflow.md — DuckDB HUGEINT pattern. i128 cannot
/// overflow at ETL scale (>2×10^19 rows at i64::MAX needed). Finalize via
/// `i64::try_from` — NEVER `as i64`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SumState {
    /// i128 internal accumulation for integer inputs. Infallible at ETL scale.
    pub int_sum: i128,
    /// Kahan compensated sum for float inputs.
    pub float_sum: f64,
    /// Kahan compensation term (running residual).
    pub compensation: f64,
    /// False until first non-null value observed.
    pub has_value: bool,
    /// True while all observed values have been integers. Flips false on
    /// first Float, and the finalize path returns Float thereafter.
    pub is_integer: bool,
}

impl Default for SumState {
    fn default() -> Self {
        Self {
            int_sum: 0,
            float_sum: 0.0,
            compensation: 0.0,
            has_value: false,
            is_integer: true,
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
            _ => {
                // Non-numeric: skip (SQL SUM on non-numeric is undefined;
                // typecheck rejects at plan time per Task 16.2).
            }
        }
    }

    fn merge(&mut self, other: &SumState) {
        if !other.has_value {
            return;
        }
        self.has_value = true;
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
        if self.is_integer {
            let n = i64::try_from(self.int_sum)
                .map_err(|_| AccumulatorError::SumOverflow { field: None })?;
            Ok(Value::Integer(n))
        } else {
            Ok(Value::Float(self.float_sum))
        }
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

// ----------------------------------------------------------------------------

/// Average accumulator state: i128 sum + Kahan f64 path + u64 count.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AvgState {
    pub int_sum: i128,
    pub float_sum: f64,
    pub compensation: f64,
    pub count: u64,
    pub is_integer: bool,
    pub has_value: bool,
}

impl Default for AvgState {
    fn default() -> Self {
        Self {
            int_sum: 0,
            float_sum: 0.0,
            compensation: 0.0,
            count: 0,
            is_integer: true,
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
            }
            _ => {}
        }
    }

    fn merge(&mut self, other: &AvgState) {
        if !other.has_value {
            return;
        }
        self.has_value = true;
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

    /// Avg always returns Float per typecheck rule (Task 16.2).
    fn finalize(&self) -> Value {
        if !self.has_value || self.count == 0 {
            return Value::Null;
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
                // Cross-type comparisons return None; Task 16.3 DLQs those.
                // For Task 16.1 we just skip the incomparable value.
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

// ----------------------------------------------------------------------------

/// Weighted average state. Two-argument: value + weight. i128 weighted sum
/// plus i128 weight sum, with Kahan compensated float paths for both.
///
/// Finalize: weighted_sum / weight_sum. Zero total weight → Null (V-7-2a).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WeightedAvgState {
    pub int_weighted_sum: i128,
    pub float_weighted_sum: f64,
    pub compensation: f64,
    pub weight_sum_int: i128,
    pub weight_sum_float: f64,
    pub weight_compensation: f64,
    pub is_integer: bool,
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
            is_integer: true,
            has_value: false,
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

    /// Convert a numeric Value to (i64_opt, f64_opt). Returns None if value
    /// is Null or non-numeric.
    fn numeric(v: &Value) -> Option<(Option<i64>, f64)> {
        match v {
            Value::Null => None,
            Value::Integer(n) => Some((Some(*n), *n as f64)),
            Value::Float(f) => Some((None, *f)),
            _ => None,
        }
    }

    fn add_weighted(&mut self, value: &Value, weight: &Value) {
        let Some((v_int, v_f)) = Self::numeric(value) else {
            return;
        };
        let Some((w_int, w_f)) = Self::numeric(weight) else {
            return;
        };
        self.has_value = true;

        // Integer path: both must be integer.
        match (v_int, w_int) {
            (Some(vi), Some(wi)) => {
                self.int_weighted_sum += vi as i128 * wi as i128;
                self.weight_sum_int += wi as i128;
                if !self.is_integer {
                    // Already float mode: mirror into float paths too.
                    self.kahan_add_val(v_f * w_f);
                    self.kahan_add_weight(w_f);
                }
            }
            _ => {
                if self.is_integer {
                    // First float arg: seed float paths from integer state.
                    self.is_integer = false;
                    self.float_weighted_sum = self.int_weighted_sum as f64;
                    self.weight_sum_float = self.weight_sum_int as f64;
                    self.compensation = 0.0;
                    self.weight_compensation = 0.0;
                }
                self.kahan_add_val(v_f * w_f);
                self.kahan_add_weight(w_f);
            }
        }
    }

    fn merge(&mut self, other: &WeightedAvgState) {
        if !other.has_value {
            return;
        }
        self.has_value = true;
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

/// Enum-dispatched accumulator with 7 built-in variants.
///
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
        }
    }

    /// Estimated heap size for memory tracking. Fixed-size variants return
    /// `size_of::<Self>()` (inline enum footprint, no heap). Collect reports
    /// `Vec` capacity × `size_of::<Value>()` plus each value's own heap.
    ///
    /// Research: RESEARCH-accumulator-memory-tracking.md — DataFusion issue
    /// #13831 (19x undercount from len-based reporting). Report capacity.
    pub fn heap_size(&self) -> usize {
        match self {
            Self::Collect(s) => s.heap_size(),
            _ => std::mem::size_of::<Self>(),
        }
    }
}
