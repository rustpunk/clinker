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

fn dec(mantissa: i64, scale: u32) -> Value {
    Value::Decimal(rust_decimal::Decimal::new(mantissa, scale))
}

// ---------- Decimal aggregates (exactness) ----------

#[test]
fn test_sum_decimal_is_exact() {
    // 0.10 + 0.20 + 0.30 == 0.60 exactly (a binary-float sum drifts).
    let mut acc = sum();
    add_all(&mut acc, &[dec(10, 2), dec(20, 2), dec(30, 2)]);
    assert_eq!(acc.finalize().unwrap(), dec(60, 2));
}

#[test]
fn test_sum_decimal_and_integer_widen_to_decimal() {
    // A decimal observed after an integer folds the integer into the exact
    // decimal accumulator (defence-in-depth; typecheck keeps columns
    // homogeneous). Result stays exact decimal.
    let mut acc = sum();
    add_all(&mut acc, &[Value::Integer(1), dec(50, 2)]);
    assert_eq!(acc.finalize().unwrap(), dec(150, 2));
}

#[test]
fn test_avg_decimal_is_exact() {
    // avg(0.10, 0.20, 0.30) = 0.20 exactly.
    let mut acc = avg();
    add_all(&mut acc, &[dec(10, 2), dec(20, 2), dec(30, 2)]);
    assert_eq!(acc.finalize().unwrap(), dec(20, 2));
}

#[test]
fn test_min_max_decimal() {
    let mut lo = min();
    let mut hi = max();
    let vals = [dec(250, 2), dec(125, 2), dec(999, 2)];
    add_all(&mut lo, &vals);
    add_all(&mut hi, &vals);
    assert_eq!(lo.finalize().unwrap(), dec(125, 2));
    assert_eq!(hi.finalize().unwrap(), dec(999, 2));
}

#[test]
fn test_sum_decimal_merge_is_exact() {
    let mut a = sum();
    add_all(&mut a, &[dec(10, 2), dec(20, 2)]);
    let mut b = sum();
    add_all(&mut b, &[dec(30, 2), dec(5, 2)]);
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), dec(65, 2));
}

#[test]
fn test_sum_decimal_then_integer_not_lost() {
    // An integer observed AFTER decimal mode (e.g. `sum(if flag then amount
    // else 1)`) must fold into the exact decimal sum, not vanish.
    let mut acc = sum();
    add_all(
        &mut acc,
        &[dec(250, 2), Value::Integer(1), Value::Integer(1)],
    );
    assert_eq!(acc.finalize().unwrap(), dec(450, 2)); // 2.50 + 1 + 1 = 4.50
}

#[test]
fn test_sum_decimal_merge_with_integer_only_state_either_order() {
    // Merging an integer-only partial into a decimal partial (and vice versa)
    // must give the same exact total regardless of merge direction.
    let build_dec = || {
        let mut a = sum();
        add_all(&mut a, &[dec(250, 2)]); // 2.50
        a
    };
    let build_int = || {
        let mut b = sum();
        add_all(&mut b, &[Value::Integer(2)]);
        b
    };
    let mut ab = build_dec();
    ab.merge(&build_int());
    assert_eq!(ab.finalize().unwrap(), dec(450, 2)); // 2.50 + 2 = 4.50
    let mut ba = build_int();
    ba.merge(&build_dec());
    assert_eq!(ba.finalize().unwrap(), dec(450, 2)); // order-independent
}

#[test]
fn test_avg_decimal_then_integer_not_lost() {
    // avg(2.00, 4) with 4 an integer observed after decimal mode = 3.00.
    let mut acc = avg();
    add_all(&mut acc, &[dec(200, 2), Value::Integer(4)]);
    assert_eq!(acc.finalize().unwrap(), dec(300, 2));
}

#[test]
fn test_avg_decimal_merge_with_integer_only_state() {
    // avg over [2.00] merged with avg over [4] = (2.00 + 4) / 2 = 3.00.
    let mut a = avg();
    add_all(&mut a, &[dec(200, 2)]);
    let mut b = avg();
    add_all(&mut b, &[Value::Integer(4)]);
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), dec(300, 2));
}

#[test]
fn test_avg_float_then_decimal_is_null() {
    // A binary float accumulated BEFORE the first decimal row cannot widen into
    // the exact decimal sum (only the integer sum is seeded on the switch).
    // Poison to Null rather than silently dropping the float and reporting the
    // decimal-only quotient. Reachable only on an untyped column; typecheck
    // rejects a float/decimal mix for typed columns.
    let mut a = avg();
    add_all(&mut a, &[Value::Float(1.5), dec(250, 2)]);
    // The true avg is (1.5 + 2.5) / 2 = 2.0; dropping the float would report
    // 2.5 / 2 = 1.25. Neither is honest, so finalize returns Null.
    assert_eq!(a.finalize().unwrap(), Value::Null);
}

#[test]
fn test_avg_decimal_then_float_is_null() {
    // A binary float observed AFTER decimal mode lands only in the Kahan float
    // sum, which the decimal quotient never reads. Poison to Null rather than
    // silently dropping it. Reachable only on an untyped column.
    let mut a = avg();
    add_all(&mut a, &[dec(200, 2), Value::Float(1.5)]);
    // The true avg is (2.0 + 1.5) / 2 = 1.75; dropping the float would report
    // 2.0 / 2 = 1.0. Neither is honest, so finalize returns Null.
    assert_eq!(a.finalize().unwrap(), Value::Null);
}

#[test]
fn test_avg_merge_float_mode_with_decimal_mode_is_null() {
    // Merging a float-mode partial (its exact contribution reads only its
    // integer sum) into a decimal-mode partial — or vice versa — would drop the
    // float side's total. Both merge directions poison to Null.
    let build_float = || {
        let mut s = avg();
        add_all(&mut s, &[Value::Float(1.5)]);
        s
    };
    let build_decimal = || {
        let mut s = avg();
        add_all(&mut s, &[dec(250, 2)]);
        s
    };
    let mut df = build_decimal();
    df.merge(&build_float());
    assert_eq!(df.finalize().unwrap(), Value::Null);
    let mut fd = build_float();
    fd.merge(&build_decimal());
    assert_eq!(fd.finalize().unwrap(), Value::Null);
}

#[test]
fn test_sum_decimal_spill_roundtrip() {
    // The accumulator state serializes exactly (decimal via the 16-byte form),
    // so a spilled-and-reloaded Sum finalizes identically.
    let mut a = sum();
    add_all(&mut a, &[dec(1050, 2), dec(295, 2)]);
    let bytes = postcard::to_stdvec(&a).unwrap();
    let restored: AccumulatorEnum = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(restored.finalize().unwrap(), dec(1345, 2));
    assert_eq!(restored, a);
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
fn sum_merge_keeps_an_integer_only_partial() {
    // A spilled aggregate merges one partial per spill run. A partial that saw
    // only the float 0.5, merged with one that saw only the integers 1, 2 and
    // 3, holds all four addends: 6.5, which is also what one fold of them
    // gives, whichever partial the merge starts from.
    let float_partial = || {
        let mut s = sum();
        add_all(&mut s, &[Value::Float(0.5)]);
        s
    };
    let integer_partial = || {
        let mut s = sum();
        add_all(
            &mut s,
            &[Value::Integer(1), Value::Integer(2), Value::Integer(3)],
        );
        s
    };
    let mut folded = sum();
    add_all(
        &mut folded,
        &[
            Value::Float(0.5),
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ],
    );
    assert_eq!(folded.finalize().unwrap(), Value::Float(6.5));

    let mut float_first = float_partial();
    float_first.merge(&integer_partial());
    assert_eq!(
        float_first.finalize().unwrap(),
        Value::Float(6.5),
        "the float partial merged with the integer-only partial"
    );
    let mut integer_first = integer_partial();
    integer_first.merge(&float_partial());
    assert_eq!(
        integer_first.finalize().unwrap(),
        Value::Float(6.5),
        "the integer-only partial merged with the float partial"
    );
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
fn test_sum_of_many_floats_is_the_exact_sum_rounded_once() {
    // The double nearest 0.1 is 0.1000000000000000055511151231257827..., so
    // a million of them total exactly 100000.0000000000055511151231257827...
    // The doubles next to 100000 are 2^-36 (about 1.46e-11) apart, and the
    // excess is less than half of that, so the sum rounds to 100000.0. A
    // left-to-right fold drifts to 100000.00000133288.
    let mut a = sum();
    for _ in 0..1_000_000 {
        a.add(&Value::Float(0.1));
    }
    assert_eq!(float_bits(&a.finalize().unwrap()), 100_000.0_f64.to_bits());
}

// ---------- Exact float sums ----------

/// A small deterministic generator for shuffles (SplitMix64).
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn shuffle<T>(&mut self, items: &mut [T]) {
        for i in (1..items.len()).rev() {
            let j = (self.next() % (i as u64 + 1)) as usize;
            items.swap(i, j);
        }
    }
}

/// Every order of `values` when there are at most 7 of them, else 200
/// seeded shuffles.
fn arrival_orders(values: &[Value]) -> Vec<Vec<Value>> {
    if values.len() <= 7 {
        return permutations(values);
    }
    let mut rng = SplitMix(0x5EED);
    (0..200)
        .map(|_| {
            let mut order = values.to_vec();
            rng.shuffle(&mut order);
            order
        })
        .collect()
}

/// A finalized float's bits, or a panic naming the value that is not a
/// float.
fn float_bits(value: &Value) -> u64 {
    match value {
        Value::Float(f) => f.to_bits(),
        other => panic!("expected a float, got {other:?}"),
    }
}

fn as_float(value: &Value) -> f64 {
    f64::from_bits(float_bits(value))
}

/// `make`'s accumulator over `values` gives `expected` in every arrival order
/// and for every split into two partial states merged either way.
fn assert_order_and_split_independent(make: NewState, values: &[Value], expected: f64) {
    for order in arrival_orders(values) {
        let context = format!("{order:?}");
        assert_eq!(
            float_bits(&fold(make, &order).finalize().unwrap()),
            expected.to_bits(),
            "folded {context}"
        );
        for at in 0..=order.len() {
            let (left, right) = order.split_at(at);
            let mut ab = fold(make, left);
            ab.merge(&fold(make, right));
            let mut ba = fold(make, right);
            ba.merge(&fold(make, left));
            for merged in [ab, ba] {
                assert_eq!(
                    float_bits(&merged.finalize().unwrap()),
                    expected.to_bits(),
                    "split {left:?} | {right:?}"
                );
            }
        }
    }
}

fn floats(values: &[f64]) -> Vec<Value> {
    values.iter().map(|f| Value::Float(*f)).collect()
}

#[test]
fn float_sum_does_not_depend_on_arrival_or_split() {
    // 1e16 + 1 is not a double (they are 2 apart there), so a fold that adds
    // 1.0 to 1e16 first loses it; the exact sum is 1.
    assert_order_and_split_independent(sum, &floats(&[1e16, 1.0, -1e16]), 1.0);
    // Ten copies of the double nearest 0.1 total exactly
    // 1.000000000000000055511151231257827..., which exceeds 1 by less than
    // half the 2^-52 spacing above 1, so the sum is 1.0.
    assert_order_and_split_independent(sum, &floats(&[0.1; 10]), 1.0);
    // f64::MAX + f64::MAX is beyond the range, but the exact total is
    // f64::MAX in every order.
    assert_order_and_split_independent(sum, &floats(&[f64::MAX, f64::MAX, -f64::MAX]), f64::MAX);
    // Subnormals of 1, 2, 3, -4 and 5 units of 2^-1074 total 7 units.
    let units = |n: u64| f64::from_bits(n);
    assert_order_and_split_independent(
        sum,
        &floats(&[units(1), units(2), units(3), -units(4), units(5)]),
        units(7),
    );
    // The double nearest 1e-16 is just below it, so ten of them total just
    // under 1e-15, which is 4.5036 spacings of 2^-52 above 1. The nearest
    // double to 1 + that is 1 + 5 * 2^-52. A left-to-right fold from 1.0
    // loses every 1e-16 (each is under half a spacing) and gives 1.0.
    let mut one_and_tenths = vec![1.0];
    one_and_tenths.extend([1e-16; 10]);
    assert_order_and_split_independent(sum, &floats(&one_and_tenths), 1.0 + 5.0 * f64::EPSILON);
}

#[test]
fn exact_sum_special_values_follow_ieee() {
    let total = |values: &[Value]| fold(sum, values).finalize().unwrap();
    let nan = Value::Float(f64::NAN);
    let inf = Value::Float(f64::INFINITY);
    let neg_inf = Value::Float(f64::NEG_INFINITY);
    assert!(as_float(&total(&[Value::Float(1.0), nan.clone()])).is_nan());
    assert!(as_float(&total(&[inf.clone(), neg_inf.clone()])).is_nan());
    assert!(as_float(&total(&[inf.clone(), nan, Value::Integer(3)])).is_nan());
    assert_eq!(
        float_bits(&total(&[inf.clone(), Value::Float(-1e308)])),
        f64::INFINITY.to_bits()
    );
    assert_eq!(float_bits(&total(&[neg_inf])), f64::NEG_INFINITY.to_bits());
    assert_eq!(
        float_bits(&total(&floats(&[f64::MAX, f64::MAX]))),
        f64::INFINITY.to_bits()
    );
    assert_eq!(
        float_bits(&total(&floats(&[-0.0, -0.0]))),
        (-0.0_f64).to_bits()
    );
    assert_eq!(float_bits(&total(&floats(&[-0.0, 0.0]))), 0.0_f64.to_bits());
    assert_eq!(float_bits(&total(&floats(&[1.0, -1.0]))), 0.0_f64.to_bits());
    assert_eq!(
        float_bits(&total(&[Value::Integer(0), Value::Float(-0.0)])),
        0.0_f64.to_bits()
    );
}

#[test]
fn integer_promotion_does_not_depend_on_where_the_first_float_arrives() {
    // The exact total is 2^61 + 4.5. The doubles next to 2^61 are 512 apart,
    // so it rounds to 2^61. No integer is rounded through a float before the
    // float arrives, so every order agrees.
    let two_60 = 1_i64 << 60;
    let values = [
        Value::Integer(two_60 + 1),
        Value::Integer(two_60 + 3),
        Value::Float(0.5),
    ];
    for order in permutations(&values) {
        assert_eq!(
            float_bits(&fold(sum, &order).finalize().unwrap()),
            2.0_f64.powi(61).to_bits(),
            "{order:?}"
        );
    }
}

#[test]
fn accumulator_enum_inline_size_does_not_grow() {
    // 112 bytes before float sums were exact: the largest variant,
    // `WeightedAvgState`, held two `i128`s, four `f64`s, two 16-byte
    // `Decimal`s and four `bool`s (100 bytes, rounded up to the `i128`'s
    // 16-byte alignment), and the enum's tag fits in a `bool`'s spare values.
    // An exact float sum is one pointer inline, so no variant grew.
    assert!(
        std::mem::size_of::<AccumulatorEnum>() <= 112,
        "AccumulatorEnum is {} bytes",
        std::mem::size_of::<AccumulatorEnum>()
    );
}

#[test]
fn exact_sum_state_roundtrips_through_postcard() {
    let mut a = sum();
    add_all(
        &mut a,
        &[
            Value::Integer(7),
            Value::Float(0.1),
            Value::Float(-1e300),
            Value::Float(1e300),
            Value::Float(-0.0),
            Value::Float(f64::from_bits(1)),
            Value::Integer(-2),
        ],
    );
    let mut special = sum();
    add_all(
        &mut special,
        &[
            Value::Float(f64::INFINITY),
            Value::Float(2.5),
            Value::Integer(1),
        ],
    );
    for state in [a, special] {
        let bits = float_bits(&state.finalize().unwrap());
        let restored: AccumulatorEnum =
            postcard::from_bytes(&postcard::to_stdvec(&state).unwrap()).unwrap();
        assert_eq!(restored, state, "postcard");
        assert_eq!(float_bits(&restored.finalize().unwrap()), bits, "postcard");
        let restored: AccumulatorEnum =
            serde_json::from_str(&serde_json::to_string(&state).unwrap()).unwrap();
        assert_eq!(restored, state, "serde_json");
        assert_eq!(
            float_bits(&restored.finalize().unwrap()),
            bits,
            "serde_json"
        );
    }
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

// ---------- Min/Max on the value order ----------

/// Every ordering of `items`.
fn permutations(items: &[Value]) -> Vec<Vec<Value>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut all = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let first = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, first.clone());
            all.push(tail);
        }
    }
    all
}

/// Whether `a` and `b` are the same value down to a float's sign and payload
/// bits and a decimal's scale and sign, which `Value`'s `==` does not see.
fn identical(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Float(x), Value::Float(y)) => x.to_bits() == y.to_bits(),
        (Value::Decimal(x), Value::Decimal(y)) => x.serialize() == y.serialize(),
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(p, q)| identical(p, q))
        }
        (Value::Map(x), Value::Map(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|((kx, vx), (ky, vy))| kx.as_str() == ky.as_str() && identical(vx, vy))
        }
        _ => a == b,
    }
}

fn assert_identical(actual: &Value, expected: &Value, context: &str) {
    assert!(
        identical(actual, expected),
        "{context}: got {actual:?}, expected {expected:?}"
    );
}

/// A constructor of an empty `min` or `max` accumulator.
type NewState = fn() -> AccumulatorEnum;

fn fold(make: NewState, values: &[Value]) -> AccumulatorEnum {
    let mut acc = make();
    add_all(&mut acc, values);
    acc
}

/// `min` and `max` over two values, in both arrival orders.
fn assert_min_max(values: [Value; 2], lo: &Value, hi: &Value) {
    let [a, b] = values;
    for order in [[a.clone(), b.clone()], [b, a]] {
        let context = format!("{order:?}");
        assert_identical(&fold(min, &order).finalize().unwrap(), lo, &context);
        assert_identical(&fold(max, &order).finalize().unwrap(), hi, &context);
    }
}

#[test]
fn min_max_do_not_depend_on_arrival_order() {
    let values = [
        Value::Integer(0),
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Integer(1),
        Value::Float(1.0),
        dec(100, 2),
        Value::Float(f64::NAN),
        Value::Null,
    ];
    let lo = Value::Integer(0);
    let hi = Value::Float(f64::NAN);
    let folds: [(NewState, &Value); 2] = [(min, &lo), (max, &hi)];
    for order in permutations(&values) {
        for (make, expected) in folds {
            let context = format!("{order:?}");
            assert_identical(&fold(make, &order).finalize().unwrap(), expected, &context);
            // Split into two partial states at every point, merged both ways.
            for at in 0..=order.len() {
                let (left, right) = order.split_at(at);
                let mut ab = fold(make, left);
                ab.merge(&fold(make, right));
                let mut ba = fold(make, right);
                ba.merge(&fold(make, left));
                let context = format!("{left:?} | {right:?}");
                assert_identical(&ab.finalize().unwrap(), expected, &context);
                assert_identical(&ba.finalize().unwrap(), expected, &context);
            }
        }
    }
}

#[test]
fn min_max_follow_the_one_order() {
    const TWO_POW_53: i64 = 1 << 53;
    // Exact across integer and float: 2^53 + 1 is above the float 2^53.
    assert_min_max(
        [
            Value::Integer(TWO_POW_53 + 1),
            Value::Float(TWO_POW_53 as f64),
        ],
        &Value::Float(TWO_POW_53 as f64),
        &Value::Integer(TWO_POW_53 + 1),
    );
    // A float after an integer is compared, not skipped.
    assert_min_max(
        [Value::Integer(5), Value::Float(3.0)],
        &Value::Float(3.0),
        &Value::Integer(5),
    );
    // Tied decimals: fewer fractional digits first.
    assert_min_max([dec(10, 1), dec(100, 2)], &dec(10, 1), &dec(100, 2));
    // Tied integer and float: the integer first.
    assert_min_max(
        [Value::Integer(1), Value::Float(1.0)],
        &Value::Integer(1),
        &Value::Float(1.0),
    );
    // Tied integer and decimal: the integer first.
    assert_min_max(
        [Value::Integer(1), dec(1, 0)],
        &Value::Integer(1),
        &dec(1, 0),
    );
    // Signed zeros: the smaller sign first.
    assert_min_max(
        [Value::Float(-0.0), Value::Float(0.0)],
        &Value::Float(-0.0),
        &Value::Float(0.0),
    );
    // NaN is above infinity; NaNs of both signs tie, the negative one first.
    assert_min_max(
        [Value::Float(f64::NAN), Value::Float(f64::INFINITY)],
        &Value::Float(f64::INFINITY),
        &Value::Float(f64::NAN),
    );
    assert_min_max(
        [Value::Float(-f64::NAN), Value::Float(f64::NAN)],
        &Value::Float(-f64::NAN),
        &Value::Float(f64::NAN),
    );
    // Nulls are skipped; an all-null group is null.
    assert_min_max(
        [Value::Null, Value::Float(2.5)],
        &Value::Float(2.5),
        &Value::Float(2.5),
    );
    assert_min_max([Value::Null, Value::Null], &Value::Null, &Value::Null);
}

fn datetime(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32, nano: u32) -> Value {
    Value::DateTime(
        chrono::NaiveDate::from_ymd_opt(y, mo, d)
            .and_then(|date| date.and_hms_nano_opt(h, mi, s, nano))
            .expect("test datetime"),
    )
}

#[test]
fn extremum_order_is_total_and_keeps_the_comparators_order() {
    let array = |items| Value::Array(crate::owned_storage::OwnedValues::from_vec(items));
    let values = vec![
        Value::Integer(0),
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Integer(1),
        Value::Float(1.0),
        dec(1, 0),
        dec(10, 1),
        dec(100, 2),
        dec(-100, 2),
        Value::Integer(1 << 53),
        Value::Integer((1 << 53) + 1),
        Value::Float((1u64 << 53) as f64),
        Value::Float(f64::INFINITY),
        Value::Float(f64::NEG_INFINITY),
        Value::Float(f64::NAN),
        Value::Float(-f64::NAN),
        Value::Float(f64::from_bits(0x7FF0_0000_0000_0001)),
        Value::Bool(false),
        Value::Bool(true),
        Value::String("".into()),
        Value::String("a".into()),
        Value::String("b".into()),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 2, 29).expect("test date")),
        Value::Date(chrono::NaiveDate::from_ymd_opt(2024, 3, 1).expect("test date")),
        // A leap second ties the next second's instant with the same fraction.
        datetime(2016, 12, 31, 23, 59, 59, 1_500_000_000),
        datetime(2017, 1, 1, 0, 0, 0, 500_000_000),
        array(vec![Value::Integer(1)]),
        array(vec![Value::Float(1.0)]),
        Value::map(vec![("a", Value::Integer(1)), ("b", Value::Integer(2))]),
        Value::map(vec![("b", Value::Integer(2)), ("a", Value::Integer(1))]),
    ];
    for a in &values {
        for b in &values {
            let ab = extremum_order(a, b);
            assert_eq!(
                ab,
                extremum_order(b, a).reverse(),
                "antisymmetric: {a:?} {b:?}"
            );
            assert_eq!(
                ab == Ordering::Equal,
                identical(a, b),
                "Equal only for identical values: {a:?} {b:?}"
            );
            let by_order = crate::order::compare(a, b);
            if by_order != Ordering::Equal {
                assert_eq!(ab, by_order, "keeps the value order: {a:?} {b:?}");
            }
            for c in &values {
                if ab != Ordering::Greater && extremum_order(b, c) != Ordering::Greater {
                    assert_ne!(
                        extremum_order(a, c),
                        Ordering::Greater,
                        "transitive: {a:?} {b:?} {c:?}"
                    );
                }
            }
        }
    }
}

// ---------- Collect ----------

#[test]
fn test_collect_produces_array() {
    let mut a = collect();
    add_all(&mut a, &[Value::Integer(1), Value::Integer(2)]);
    assert_eq!(
        a.finalize().unwrap(),
        Value::Array(crate::owned_storage::OwnedValues::from_vec(vec![
            Value::Integer(1),
            Value::Integer(2),
        ]))
    );
}

#[test]
fn test_collect_includes_nulls() {
    let mut a = collect();
    add_all(&mut a, &[Value::Integer(1), Value::Null, Value::Integer(3)]);
    assert_eq!(
        a.finalize().unwrap(),
        Value::Array(crate::owned_storage::OwnedValues::from_vec(vec![
            Value::Integer(1),
            Value::Null,
            Value::Integer(3),
        ]))
    );
}

#[test]
fn test_collect_empty() {
    let a = collect();
    assert_eq!(
        a.finalize().unwrap(),
        Value::Array(crate::owned_storage::OwnedValues::from_vec(vec![]))
    );
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
        Value::Array(crate::owned_storage::OwnedValues::from_vec(vec![
            Value::Integer(1),
            Value::Integer(2),
            Value::Integer(3),
        ]))
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

// ---------- WeightedAvg over decimals (exactness) ----------

#[test]
fn test_weighted_avg_decimal_is_exact() {
    // (0.10 + 0.20 + 0.30) / 3 = 0.60 / 3 = 0.20 exactly. The binary-float
    // path drifts: 0.1 + 0.2 + 0.3 is 0.6000000000000001, / 3 is not 0.2.
    let mut a = weighted_avg();
    a.add_weighted(&dec(10, 2), &Value::Integer(1));
    a.add_weighted(&dec(20, 2), &Value::Integer(1));
    a.add_weighted(&dec(30, 2), &Value::Integer(1));
    assert_eq!(a.finalize().unwrap(), dec(20, 2));
}

#[test]
fn test_weighted_avg_decimal_value_and_weight_exact() {
    // Both operands decimal: (2.00*3.00 + 4.00*1.00) / (3.00 + 1.00)
    //                        = 10.00 / 4.00 = 2.50, exactly.
    let mut a = weighted_avg();
    a.add_weighted(&dec(200, 2), &dec(300, 2));
    a.add_weighted(&dec(400, 2), &dec(100, 2));
    assert_eq!(a.finalize().unwrap(), dec(250, 2));
}

#[test]
fn test_weighted_avg_decimal_value_int_weight_exact() {
    // The canonical `weighted_avg(price, qty)` shape: decimal value, integer
    // weight. (0.10*3 + 0.20*1) / (3 + 1) = 0.50 / 4 = 0.125, exactly.
    let mut a = weighted_avg();
    a.add_weighted(&dec(10, 2), &Value::Integer(3));
    a.add_weighted(&dec(20, 2), &Value::Integer(1));
    assert_eq!(a.finalize().unwrap(), dec(125, 3));
}

#[test]
fn test_weighted_avg_int_value_decimal_weight_exact() {
    // Integer value, decimal weight: (3*0.5 + 1*0.5) / (0.5 + 0.5)
    //                                = 2.0 / 1.0 = 2.0, exactly.
    let mut a = weighted_avg();
    a.add_weighted(&Value::Integer(3), &dec(5, 1));
    a.add_weighted(&Value::Integer(1), &dec(5, 1));
    assert_eq!(a.finalize().unwrap(), dec(20, 1));
}

#[test]
fn test_weighted_avg_decimal_then_integer_not_lost() {
    // An integer row observed AFTER decimal mode (e.g.
    // `weighted_avg(if flag then amount else 2, w)`) must fold into the exact
    // sums, not vanish. (0.50*1 + 2*1) / (1 + 1) = 2.50 / 2 = 1.25.
    let mut a = weighted_avg();
    a.add_weighted(&dec(50, 2), &Value::Integer(1));
    a.add_weighted(&Value::Integer(2), &Value::Integer(1));
    assert_eq!(a.finalize().unwrap(), dec(125, 2));
}

#[test]
fn test_weighted_avg_decimal_merge_with_integer_only_state_either_order() {
    // Merging an integer-only partial into a decimal partial (and vice versa)
    // must give the same exact result regardless of merge direction:
    // (0.50 + 2) / (1 + 1) = 2.50 / 2 = 1.25.
    let build_dec = || {
        let mut a = weighted_avg();
        a.add_weighted(&dec(50, 2), &Value::Integer(1)); // 0.50 weighted, weight 1
        a
    };
    let build_int = || {
        let mut b = weighted_avg();
        b.add_weighted(&Value::Integer(2), &Value::Integer(1)); // 2 weighted, weight 1
        b
    };
    let mut ab = build_dec();
    ab.merge(&build_int());
    assert_eq!(ab.finalize().unwrap(), dec(125, 2));
    let mut ba = build_int();
    ba.merge(&build_dec());
    assert_eq!(ba.finalize().unwrap(), dec(125, 2)); // order-independent
}

#[test]
fn test_weighted_avg_decimal_merge_both_decimal_exact() {
    // Two decimal partials combine exactly:
    // (10.50*2 + 1.50*2) / (2 + 2) = 24.00 / 4 = 6.00.
    let mut a = weighted_avg();
    a.add_weighted(&dec(1050, 2), &Value::Integer(2));
    let mut b = weighted_avg();
    b.add_weighted(&dec(150, 2), &Value::Integer(2));
    a.merge(&b);
    assert_eq!(a.finalize().unwrap(), dec(600, 2));
}

#[test]
fn test_weighted_avg_decimal_mixed_with_float_is_null() {
    // A float mixed with a decimal cannot fold exactly (reachable only on an
    // untyped column; typecheck rejects the mix for typed columns). The result
    // is Null rather than a silently-wrong average.
    let mut a = weighted_avg();
    a.add_weighted(&dec(200, 2), &Value::Float(2.0));
    assert_eq!(a.finalize().unwrap(), Value::Null);
}

#[test]
fn test_weighted_avg_float_then_decimal_is_null() {
    // A binary float accumulated BEFORE the first decimal row cannot widen into
    // the exact decimal sums (only the integer accumulators are seeded on the
    // switch). Poison to Null rather than silently dropping the float total and
    // reporting the decimal-only quotient. Reachable only on an untyped column.
    let mut a = weighted_avg();
    a.add_weighted(&Value::Float(1.5), &Value::Integer(1)); // float path first
    a.add_weighted(&dec(250, 2), &Value::Integer(1)); // then a decimal row
    // The true weighted avg is (1.5 + 2.5) / 2 = 2.0; dropping the float total
    // would report 2.5. Neither is honest, so finalize returns Null.
    assert_eq!(a.finalize().unwrap(), Value::Null);
}

#[test]
fn test_weighted_avg_merge_float_mode_with_decimal_mode_is_null() {
    // Merging a float-mode partial (its exact contribution reads only its
    // integer sums) into a decimal-mode partial — or vice versa — would drop
    // the float side's total. Both merge directions poison to Null.
    let build_float = || {
        let mut s = weighted_avg();
        s.add_weighted(&Value::Float(1.5), &Value::Integer(1));
        s
    };
    let build_decimal = || {
        let mut s = weighted_avg();
        s.add_weighted(&dec(250, 2), &Value::Integer(1));
        s
    };
    let mut df = build_decimal();
    df.merge(&build_float());
    assert_eq!(df.finalize().unwrap(), Value::Null);
    let mut fd = build_float();
    fd.merge(&build_decimal());
    assert_eq!(fd.finalize().unwrap(), Value::Null);
}

#[test]
fn test_weighted_avg_decimal_spill_roundtrip() {
    // The accumulator state serializes exactly (decimal via the 16-byte form),
    // so a spilled-and-reloaded WeightedAvg finalizes identically.
    // (10.50*2 + 1.50*2) / (2 + 2) = 24.00 / 4 = 6.00.
    let mut a = weighted_avg();
    a.add_weighted(&dec(1050, 2), &Value::Integer(2));
    a.add_weighted(&dec(150, 2), &Value::Integer(2));
    let bytes = postcard::to_stdvec(&a).unwrap();
    let restored: AccumulatorEnum = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(restored.finalize().unwrap(), dec(600, 2));
    assert_eq!(restored, a);
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

// ---------- Reversibility classification ----------
//
// Sum / Count / Collect / Any expose an O(1) inverse operation that can
// recover state byte-equivalent to never having observed a retracted
// contribution. Min, Max, Avg, WeightedAvg are classified BufferRequired:
// Min/Max because they are positional (the prior extremum is unrecoverable
// once shadowed); Avg/WeightedAvg because incremental retract on the
// Kahan-compensated f64 paths drifts away from a re-fold over surviving
// rows.

#[test]
fn test_reversibility_sum() {
    assert_eq!(sum().reversibility(), Reversibility::Reversible);
}

#[test]
fn test_reversibility_count() {
    // Both Count flavors classify the same — Reversible — because the
    // increment is by 1 and the inverse is decrement-by-1.
    assert_eq!(count_all().reversibility(), Reversibility::Reversible);
    assert_eq!(count_field().reversibility(), Reversibility::Reversible);
}

#[test]
fn test_reversibility_collect() {
    assert_eq!(collect().reversibility(), Reversibility::Reversible);
}

#[test]
fn test_reversibility_any() {
    assert_eq!(any().reversibility(), Reversibility::Reversible);
}

#[test]
fn test_reversibility_min() {
    assert_eq!(min().reversibility(), Reversibility::BufferRequired);
}

#[test]
fn test_reversibility_max() {
    assert_eq!(max().reversibility(), Reversibility::BufferRequired);
}

#[test]
fn test_reversibility_avg() {
    assert_eq!(avg().reversibility(), Reversibility::BufferRequired);
}

#[test]
fn test_reversibility_weighted_avg() {
    assert_eq!(
        weighted_avg().reversibility(),
        Reversibility::BufferRequired
    );
}

// ============================================================================
// Retract round-trip — feed N values, retract M < N, finalize equals
// feed-(N-M)-from-scratch. One test per `Reversibility::Reversible` variant.
// ============================================================================

/// Feed values into a fresh accumulator and finalize. Helper for the round-
/// trip equivalence checks below.
fn feed_and_finalize(builder: fn() -> AccumulatorEnum, values: &[Value]) -> Value {
    let mut acc = builder();
    for v in values {
        acc.add(v);
    }
    acc.finalize().unwrap()
}

#[test]
fn test_sum_retract_roundtrip() {
    // Feed 5 values, retract last 2: must equal feed-of-first-3.
    let mut acc = sum();
    let all = [
        Value::Integer(10),
        Value::Integer(20),
        Value::Integer(30),
        Value::Integer(40),
        Value::Integer(50),
    ];
    for v in &all {
        acc.add(v);
    }
    acc.sub(&all[3]);
    acc.sub(&all[4]);
    let after_retract = acc.finalize().unwrap();
    let baseline = feed_and_finalize(sum, &all[..3]);
    assert_eq!(after_retract, baseline);
}

#[test]
fn test_sum_retract_to_empty_yields_null() {
    // Retracting every contribution leaves the state of a Sum that saw
    // nothing, which finalizes to null, as a fresh fold of the (empty)
    // surviving rows does.
    let mut acc = sum();
    acc.add(&Value::Integer(7));
    acc.sub(&Value::Integer(7));
    assert_eq!(acc, sum());
    assert_eq!(acc.finalize().unwrap(), Value::Null);
}

#[test]
fn test_count_all_retract_roundtrip() {
    let mut acc = count_all();
    let all = [
        Value::Integer(1),
        Value::Null,
        Value::Integer(2),
        Value::Null,
    ];
    for v in &all {
        acc.add(v);
    }
    // Retract one Null and one Integer. count_all counts both; after
    // retraction the count should match feed-of-first-2 (one Integer, one Null).
    acc.sub(&Value::Null);
    acc.sub(&Value::Integer(2));
    let baseline = feed_and_finalize(count_all, &all[..2]);
    assert_eq!(acc.finalize().unwrap(), baseline);
}

#[test]
fn test_count_field_retract_skips_nulls() {
    let mut acc = count_field();
    acc.add(&Value::Integer(1));
    acc.add(&Value::Null);
    acc.add(&Value::Integer(2));
    // count_field saw 2 non-null adds. Retracting a Null is a no-op.
    acc.sub(&Value::Null);
    assert_eq!(acc.finalize().unwrap(), Value::Integer(2));
    acc.sub(&Value::Integer(2));
    assert_eq!(acc.finalize().unwrap(), Value::Integer(1));
}

#[test]
fn test_collect_retract_roundtrip() {
    let mut acc = collect();
    let all = [
        Value::Integer(1),
        Value::Integer(2),
        Value::Integer(3),
        Value::Integer(2),
    ];
    for v in &all {
        acc.add(v);
    }
    // Retract one of the duplicate `Integer(2)`s: the surviving array
    // should still contain one `Integer(2)` (multiset semantics).
    acc.sub(&Value::Integer(2));
    let result = acc.finalize().unwrap();
    let Value::Array(arr) = result else {
        panic!("Collect.finalize must return Array");
    };
    assert_eq!(arr.len(), 3);
    let baseline = feed_and_finalize(
        collect,
        &[Value::Integer(1), Value::Integer(2), Value::Integer(3)],
    );
    let Value::Array(baseline_arr) = baseline else {
        panic!("baseline must be Array");
    };
    assert_eq!(arr.len(), baseline_arr.len());
}

#[test]
fn test_any_retract_preserves_first_wins_until_emptied() {
    let mut acc = any();
    acc.add(&Value::String("alice".into()));
    acc.add(&Value::String("bob".into()));
    acc.add(&Value::String("alice".into()));
    // Locked value is "alice" with refcount 2; retracting one leaves it
    // locked because refcount=1 still positive.
    acc.sub(&Value::String("alice".into()));
    assert_eq!(acc.finalize().unwrap(), Value::String("alice".into()));
    // Retract the second "alice": refcount empties, finalize falls back
    // to the surviving "bob".
    acc.sub(&Value::String("alice".into()));
    assert_eq!(acc.finalize().unwrap(), Value::String("bob".into()));
    // Retract the last "bob": every contribution gone, finalize returns Null.
    acc.sub(&Value::String("bob".into()));
    assert_eq!(acc.finalize().unwrap(), Value::Null);
}

#[test]
fn test_any_serde_roundtrip_preserves_refcounts() {
    // The refcount Vec rides through the Serialize/Deserialize derive
    // alongside the locked value, so a spilled-and-merged AnyState
    // continues to admit retraction with byte-equivalent finalize output.
    let mut acc = any();
    acc.add(&Value::Integer(7));
    acc.add(&Value::Integer(7));
    acc.add(&Value::Integer(8));
    let bytes = postcard::to_stdvec(&acc).unwrap();
    let mut roundtripped: AccumulatorEnum = postcard::from_bytes(&bytes).unwrap();
    roundtripped.sub(&Value::Integer(7));
    roundtripped.sub(&Value::Integer(7));
    assert_eq!(roundtripped.finalize().unwrap(), Value::Integer(8));
}
