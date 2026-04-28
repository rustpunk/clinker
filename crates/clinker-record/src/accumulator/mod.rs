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

use serde::{Deserialize, Serialize};

use crate::value::Value;

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
                // typecheck rejects at plan time, so this is defence-in-depth).
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

    /// Avg always returns Float per typecheck rule.
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
