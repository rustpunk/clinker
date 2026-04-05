//! Unit tests for AccumulatorEnum and all 7 built-in aggregates.
//!
//! Covers: Sum (i128 + Kahan), Count (all/field), Avg, Min, Max,
//! Collect (NULL-inclusive), WeightedAvg. Tests NULL handling, merge
//! correctness, serde round-trip, heap_size reporting, and overflow.

// Imports will be added when AccumulatorEnum is implemented in Task 16.1.
// use super::*;

// ---------- Sum ----------

#[test]
fn test_sum_integers() {
    // Sum of [1, 2, 3] → Integer(6)
    todo!("Task 16.1")
}

#[test]
fn test_sum_floats() {
    // Sum of [1.5, 2.5] → Float(4.0)
    todo!("Task 16.1")
}

#[test]
fn test_sum_mixed_int_float() {
    // Int + Float → Float result
    todo!("Task 16.1")
}

#[test]
fn test_sum_null_skipped() {
    // Sum of [1, Null, 3] → Integer(4)
    todo!("Task 16.1")
}

#[test]
fn test_sum_all_null() {
    // Sum of [Null, Null] → Null
    todo!("Task 16.1")
}

#[test]
fn test_sum_kahan_precision() {
    // 1M additions of 0.1 → closer to 100000.0 than naive
    todo!("Task 16.1")
}

#[test]
fn test_sum_merge() {
    // Two partial sums merged → correct total
    todo!("Task 16.1")
}

#[test]
fn test_sum_i128_no_overflow() {
    // Sum of [i64::MAX, i64::MAX] → SumOverflow error (exceeds i64)
    todo!("Task 16.1")
}

#[test]
fn test_sum_i128_large_cancel() {
    // Sum of [i64::MAX, -i64::MAX, 1] → Integer(1) (cancellation fits i64)
    todo!("Task 16.1")
}

// ---------- Count ----------

#[test]
fn test_count_all() {
    // COUNT(*) of [1, Null, 3] → Integer(3)
    todo!("Task 16.1")
}

#[test]
fn test_count_field() {
    // COUNT(field) of [1, Null, 3] → Integer(2)
    todo!("Task 16.1")
}

// ---------- Avg ----------

#[test]
fn test_avg_basic() {
    // Avg of [2, 4, 6] → Float(4.0)
    todo!("Task 16.1")
}

#[test]
fn test_avg_null_skipped() {
    // Avg of [2, Null, 6] → Float(4.0)
    todo!("Task 16.1")
}

#[test]
fn test_avg_all_null() {
    // Avg of [Null] → Null
    todo!("Task 16.1")
}

#[test]
fn test_avg_kahan() {
    // Kahan precision for avg over 1M values
    todo!("Task 16.1")
}

// ---------- Min ----------

#[test]
fn test_min_basic() {
    // Min of [3, 1, 2] → Integer(1)
    todo!("Task 16.1")
}

#[test]
fn test_min_null_skipped() {
    // Min of [3, Null, 1] → Integer(1)
    todo!("Task 16.1")
}

#[test]
fn test_min_all_null() {
    // Min of [Null] → Null
    todo!("Task 16.1")
}

// ---------- Max ----------

#[test]
fn test_max_basic() {
    // Max of [1, 3, 2] → Integer(3)
    todo!("Task 16.1")
}

#[test]
fn test_max_strings() {
    // Max of ["b", "a", "c"] → String("c")
    todo!("Task 16.1")
}

// ---------- Collect ----------

#[test]
fn test_collect_produces_array() {
    // Collect of [Integer(1), Integer(2)] ��� Array([Integer(1), Integer(2)])
    todo!("Task 16.1")
}

#[test]
fn test_collect_includes_nulls() {
    // Collect of [Integer(1), Null, Integer(3)] → Array([Integer(1), Null, Integer(3)])
    todo!("Task 16.1")
}

#[test]
fn test_collect_empty() {
    // Collect of [] → Array([])
    todo!("Task 16.1")
}

#[test]
fn test_collect_merge() {
    // Two partial collects merged → correct combined array
    todo!("Task 16.1")
}

#[test]
fn test_collect_heap_size_capacity() {
    // heap_size() reflects Vec capacity, not just len
    todo!("Task 16.1")
}

// ---------- WeightedAvg ----------

#[test]
fn test_weighted_avg_basic() {
    // weighted_avg([(2,3), (4,1)]) → Float(2.5)
    todo!("Task 16.1")
}

#[test]
fn test_weighted_avg_null_skipped() {
    // weighted_avg([(2,3), (Null,1), (4,1)]) �� Float(2.5)
    todo!("Task 16.1")
}

#[test]
fn test_weighted_avg_merge() {
    // Two partial weighted avgs merged → correct result
    todo!("Task 16.1")
}

#[test]
fn test_weighted_avg_zero_weight() {
    // Zero total weight → Null (V-7-2a)
    todo!("Task 16.1")
}

// ---------- Serde round-trip ----------

#[test]
fn test_accumulator_serde_roundtrip() {
    // Serde derive round-trip preserves all 7 variants' internal state
    todo!("Task 16.1")
}

// ---------- heap_size ----------

#[test]
fn test_accumulator_heap_size_fixed() {
    // Fixed-size variants return size_of::<Self>()
    todo!("Task 16.1")
}
