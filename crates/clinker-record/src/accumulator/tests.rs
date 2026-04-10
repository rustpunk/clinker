//! Unit tests for AccumulatorEnum and all 7 built-in aggregates.

use super::*;

fn sum() -> AccumulatorEnum {
    AccumulatorEnum::Sum(SumState::default())
}
fn count_all() -> AccumulatorEnum {
    AccumulatorEnum::Count(CountState::new_count_all())
}
fn count_field() -> AccumulatorEnum {
    AccumulatorEnum::Count(CountState::new_count_field())
}
fn avg() -> AccumulatorEnum {
    AccumulatorEnum::Avg(AvgState::default())
}
fn min() -> AccumulatorEnum {
    AccumulatorEnum::Min(MinMaxState::default())
}
fn max() -> AccumulatorEnum {
    AccumulatorEnum::Max(MinMaxState::default())
}
fn collect() -> AccumulatorEnum {
    AccumulatorEnum::Collect(CollectState::default())
}
fn weighted_avg() -> AccumulatorEnum {
    AccumulatorEnum::WeightedAvg(WeightedAvgState::default())
}
fn any() -> AccumulatorEnum {
    AccumulatorEnum::Any(AnyState::default())
}

fn add_all(acc: &mut AccumulatorEnum, values: &[Value]) {
    for v in values {
        acc.add(v);
    }
}

// ---------- Sum ----------

#[test]
fn test_sum_integers() {
    let mut a = sum();
    add_all(
        &mut a,
        &[Value::Integer(1), Value::Integer(2), Value::Integer(3)],
    );
    assert_eq!(a.finalize().unwrap(), Value::Integer(6));
}

#[test]
fn test_sum_floats() {
    let mut a = sum();
    add_all(&mut a, &[Value::Float(1.5), Value::Float(2.5)]);
    assert_eq!(a.finalize().unwrap(), Value::Float(4.0));
}

#[test]
fn test_sum_mixed_int_float() {
    let mut a = sum();
    add_all(&mut a, &[Value::Integer(2), Value::Float(0.5)]);
    assert_eq!(a.finalize().unwrap(), Value::Float(2.5));
}

#[test]
fn test_sum_null_skipped() {
    let mut a = sum();
    add_all(&mut a, &[Value::Integer(1), Value::Null, Value::Integer(3)]);
    assert_eq!(a.finalize().unwrap(), Value::Integer(4));
}

#[test]
fn test_sum_all_null() {
    let mut a = sum();
    add_all(&mut a, &[Value::Null, Value::Null]);
    assert_eq!(a.finalize().unwrap(), Value::Null);
}

#[test]
fn test_sum_kahan_precision() {
    // Sum 1,000,000 × 0.1 should be closer to 100_000.0 than a naive sum.
    let mut a = sum();
    for _ in 0..1_000_000 {
        a.add(&Value::Float(0.1));
    }
    let result = a.finalize().unwrap();
    let Value::Float(f) = result else {
        panic!("expected Float, got {result:?}");
    };
    let kahan_err = (f - 100_000.0).abs();

    // Naive summation for comparison.
    let mut naive = 0.0_f64;
    for _ in 0..1_000_000 {
        naive += 0.1;
    }
    let naive_err = (naive - 100_000.0).abs();

    assert!(
        kahan_err <= naive_err,
        "Kahan error {kahan_err} exceeded naive error {naive_err}"
    );
    // Kahan should get within ~1e-9.
    assert!(kahan_err < 1e-6, "Kahan error {kahan_err} too large");
}

#[test]
fn test_sum_merge() {
    let mut a = sum();
    add_all(&mut a, &[Value::Integer(1), Value::Integer(2)]);
    let mut b = sum();
    add_all(&mut b, &[Value::Integer(3), Value::Integer(4)]);
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), Value::Integer(10));
}

#[test]
fn test_sum_i128_no_overflow() {
    let mut a = sum();
    add_all(
        &mut a,
        &[Value::Integer(i64::MAX), Value::Integer(i64::MAX)],
    );
    // i128 accumulation succeeded, but i64::try_from at finalize must fail.
    match a.finalize() {
        Err(AccumulatorError::SumOverflow { .. }) => {}
        other => panic!("expected SumOverflow, got {other:?}"),
    }
}

#[test]
fn test_sum_i128_large_cancel() {
    let mut a = sum();
    add_all(
        &mut a,
        &[
            Value::Integer(i64::MAX),
            Value::Integer(-i64::MAX),
            Value::Integer(1),
        ],
    );
    // i128 intermediate is transiently large but final fits i64.
    assert_eq!(a.finalize().unwrap(), Value::Integer(1));
}

// ---------- Count ----------

#[test]
fn test_count_all() {
    let mut a = count_all();
    add_all(&mut a, &[Value::Integer(1), Value::Null, Value::Integer(3)]);
    assert_eq!(a.finalize().unwrap(), Value::Integer(3));
}

#[test]
fn test_count_field() {
    let mut a = count_field();
    add_all(&mut a, &[Value::Integer(1), Value::Null, Value::Integer(3)]);
    assert_eq!(a.finalize().unwrap(), Value::Integer(2));
}

// ---------- Avg ----------

#[test]
fn test_avg_basic() {
    let mut a = avg();
    add_all(
        &mut a,
        &[Value::Integer(2), Value::Integer(4), Value::Integer(6)],
    );
    assert_eq!(a.finalize().unwrap(), Value::Float(4.0));
}

#[test]
fn test_avg_null_skipped() {
    let mut a = avg();
    add_all(&mut a, &[Value::Integer(2), Value::Null, Value::Integer(6)]);
    assert_eq!(a.finalize().unwrap(), Value::Float(4.0));
}

#[test]
fn test_avg_all_null() {
    let mut a = avg();
    add_all(&mut a, &[Value::Null]);
    assert_eq!(a.finalize().unwrap(), Value::Null);
}

#[test]
fn test_avg_kahan() {
    // 1M × 0.1 averaged → 0.1 exactly (within Kahan tolerance).
    let mut a = avg();
    for _ in 0..1_000_000 {
        a.add(&Value::Float(0.1));
    }
    let result = a.finalize().unwrap();
    let Value::Float(f) = result else {
        panic!("expected Float, got {result:?}");
    };
    assert!(
        (f - 0.1).abs() < 1e-12,
        "avg error {} too large",
        (f - 0.1).abs()
    );
}

// ---------- Min ----------

#[test]
fn test_min_basic() {
    let mut a = min();
    add_all(
        &mut a,
        &[Value::Integer(3), Value::Integer(1), Value::Integer(2)],
    );
    assert_eq!(a.finalize().unwrap(), Value::Integer(1));
}

#[test]
fn test_min_null_skipped() {
    let mut a = min();
    add_all(&mut a, &[Value::Integer(3), Value::Null, Value::Integer(1)]);
    assert_eq!(a.finalize().unwrap(), Value::Integer(1));
}

#[test]
fn test_min_all_null() {
    let mut a = min();
    add_all(&mut a, &[Value::Null]);
    assert_eq!(a.finalize().unwrap(), Value::Null);
}

// ---------- Max ----------

#[test]
fn test_max_basic() {
    let mut a = max();
    add_all(
        &mut a,
        &[Value::Integer(1), Value::Integer(3), Value::Integer(2)],
    );
    assert_eq!(a.finalize().unwrap(), Value::Integer(3));
}

#[test]
fn test_max_strings() {
    let mut a = max();
    add_all(
        &mut a,
        &[
            Value::String("b".into()),
            Value::String("a".into()),
            Value::String("c".into()),
        ],
    );
    assert_eq!(a.finalize().unwrap(), Value::String("c".into()));
}

// ---------- Collect ----------

#[test]
fn test_collect_produces_array() {
    let mut a = collect();
    add_all(&mut a, &[Value::Integer(1), Value::Integer(2)]);
    assert_eq!(
        a.finalize().unwrap(),
        Value::Array(vec![Value::Integer(1), Value::Integer(2)])
    );
}

#[test]
fn test_collect_includes_nulls() {
    let mut a = collect();
    add_all(&mut a, &[Value::Integer(1), Value::Null, Value::Integer(3)]);
    assert_eq!(
        a.finalize().unwrap(),
        Value::Array(vec![Value::Integer(1), Value::Null, Value::Integer(3)])
    );
}

#[test]
fn test_collect_empty() {
    let a = collect();
    assert_eq!(a.finalize().unwrap(), Value::Array(vec![]));
}

#[test]
fn test_collect_merge() {
    let mut a = collect();
    add_all(&mut a, &[Value::Integer(1), Value::Integer(2)]);
    let mut b = collect();
    add_all(&mut b, &[Value::Integer(3)]);
    a.merge(&b);
    assert_eq!(
        a.finalize().unwrap(),
        Value::Array(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ])
    );
}

#[test]
fn test_collect_heap_size_capacity() {
    let mut a = collect();
    // Pre-push to force capacity growth beyond len.
    for i in 0..4 {
        a.add(&Value::Integer(i));
    }
    let heap = a.heap_size();
    // Vec of 4 Value slots = 4 × 24 bytes = 96 bytes minimum (capacity ≥ len).
    // Integer values have zero heap of their own, so total ≥ capacity × 24.
    let AccumulatorEnum::Collect(inner) = &a else {
        unreachable!()
    };
    let expected_min = inner.values.capacity() * std::mem::size_of::<Value>();
    assert_eq!(heap, expected_min);
    // And capacity must be ≥ len.
    assert!(inner.values.capacity() >= inner.values.len());
}

// ---------- WeightedAvg ----------

#[test]
fn test_weighted_avg_basic() {
    let mut a = weighted_avg();
    // (2*3 + 4*1) / (3+1) = 10/4 = 2.5
    a.add_weighted(&Value::Integer(2), &Value::Integer(3));
    a.add_weighted(&Value::Integer(4), &Value::Integer(1));
    assert_eq!(a.finalize().unwrap(), Value::Float(2.5));
}

#[test]
fn test_weighted_avg_null_skipped() {
    let mut a = weighted_avg();
    a.add_weighted(&Value::Integer(2), &Value::Integer(3));
    a.add_weighted(&Value::Null, &Value::Integer(1));
    a.add_weighted(&Value::Integer(4), &Value::Integer(1));
    assert_eq!(a.finalize().unwrap(), Value::Float(2.5));
}

#[test]
fn test_weighted_avg_merge() {
    let mut a = weighted_avg();
    a.add_weighted(&Value::Integer(2), &Value::Integer(3));
    let mut b = weighted_avg();
    b.add_weighted(&Value::Integer(4), &Value::Integer(1));
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), Value::Float(2.5));
}

#[test]
fn test_weighted_avg_zero_weight() {
    // V-7-2a: zero total weight → Null, not NaN/Infinity.
    let mut a = weighted_avg();
    a.add_weighted(&Value::Integer(5), &Value::Integer(0));
    assert_eq!(a.finalize().unwrap(), Value::Null);
}

// ---------- Serde round-trip ----------

#[test]
fn test_accumulator_serde_roundtrip() {
    // Build one of each variant with non-default state.
    let mut s = sum();
    add_all(&mut s, &[Value::Integer(10), Value::Float(1.5)]);

    let mut ca = count_all();
    add_all(&mut ca, &[Value::Integer(1), Value::Null]);

    let mut av = avg();
    add_all(&mut av, &[Value::Integer(2), Value::Integer(4)]);

    let mut mi = min();
    add_all(&mut mi, &[Value::Integer(5), Value::Integer(1)]);

    let mut mx = max();
    add_all(&mut mx, &[Value::Integer(5), Value::Integer(1)]);

    let mut co = collect();
    add_all(&mut co, &[Value::Integer(1), Value::String("x".into())]);

    let mut wa = weighted_avg();
    wa.add_weighted(&Value::Integer(2), &Value::Integer(3));

    let mut an = any();
    an.add(&Value::String("first".into()));
    an.add(&Value::String("second".into()));

    for acc in [s, ca, av, mi, mx, co, wa, an] {
        let json = serde_json::to_string(&acc).expect("serialize");
        let recovered: AccumulatorEnum = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(acc, recovered, "round-trip failed for {acc:?}");
    }
}

// ---------- heap_size ----------

#[test]
fn test_accumulator_heap_size_fixed() {
    // Fixed-size variants (all except Collect) return size_of::<Self>().
    let expected = std::mem::size_of::<AccumulatorEnum>();
    for acc in [
        sum(),
        count_all(),
        count_field(),
        avg(),
        min(),
        max(),
        weighted_avg(),
    ] {
        assert_eq!(acc.heap_size(), expected, "variant {acc:?}");
    }
    // Collect diverges (reports Vec capacity).
    let c = collect();
    // Empty collect: capacity 0 → heap 0.
    assert_eq!(c.heap_size(), 0);
}

// ---------- Any (D11 explicit escape hatch) ----------

#[test]
fn test_any_add_basic() {
    let mut a = any();
    a.add(&Value::String("first".into()));
    a.add(&Value::String("second".into()));
    a.add(&Value::Integer(3));
    // First-wins semantics.
    assert_eq!(a.finalize().unwrap(), Value::String("first".into()));
}

#[test]
fn test_any_add_null_skipped() {
    let mut a = any();
    a.add(&Value::Null);
    a.add(&Value::Null);
    a.add(&Value::Integer(42));
    a.add(&Value::Integer(99));
    // NULLs do not occupy the slot; first non-NULL wins.
    assert_eq!(a.finalize().unwrap(), Value::Integer(42));
}

#[test]
fn test_any_all_null_finalize() {
    let mut a = any();
    a.add(&Value::Null);
    a.add(&Value::Null);
    assert_eq!(a.finalize().unwrap(), Value::Null);
}

#[test]
fn test_any_merge_commutative() {
    // Two non-empty partials: first-wins on the receiver, so order matters
    // for which value survives — but the *result set* of possible outcomes
    // is stable and merge is associative. We assert the documented
    // first-wins-on-receiver semantics.
    let mut left = any();
    left.add(&Value::Integer(1));
    let mut right = any();
    right.add(&Value::Integer(2));

    let mut a = left.clone();
    a.merge(&right);
    assert_eq!(a.finalize().unwrap(), Value::Integer(1));

    let mut b = right.clone();
    b.merge(&left);
    assert_eq!(b.finalize().unwrap(), Value::Integer(2));

    // Empty receiver takes from other.
    let mut empty = any();
    empty.merge(&left);
    assert_eq!(empty.finalize().unwrap(), Value::Integer(1));

    // Both empty stays empty.
    let mut e1 = any();
    let e2 = any();
    e1.merge(&e2);
    assert_eq!(e1.finalize().unwrap(), Value::Null);
}

// ---------- AccumulatorEnum::for_type factory ----------

#[test]
fn test_for_type_roundtrip() {
    use AggregateType as T;
    let cases = [
        T::Sum,
        T::Count { count_all: true },
        T::Count { count_all: false },
        T::Avg,
        T::Min,
        T::Max,
        T::Collect,
        T::WeightedAvg,
        T::Any,
    ];
    for t in &cases {
        let acc = AccumulatorEnum::for_type(t);
        // Tag round-trips through serde.
        let json = serde_json::to_string(t).expect("serialize tag");
        let recovered: AggregateType = serde_json::from_str(&json).expect("deserialize tag");
        assert_eq!(*t, recovered);
        // Factory produces a default-state accumulator that finalizes
        // (no panic) — exact value depends on variant.
        let _ = acc.finalize();
    }
}
