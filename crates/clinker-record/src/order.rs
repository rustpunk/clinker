//! The one value order.
//!
//! Every comparison that sorts, groups, merges sorted runs or checks a declared
//! order belongs on this module, so an in-memory path and a spilled path cannot
//! disagree about which of two values comes first or whether two values fall in
//! the same group. [`compare`] is a total order over every [`Value`]; [`encode`]
//! writes a memcomparable key whose unsigned byte order is exactly [`compare`];
//! [`ties`] and [`hash_tie_class`] give group keys the comparator's equality and
//! a hash that agrees with it.
//!
//! The order:
//!
//! - Values of different domains order by a fixed rank:
//!   null < bool < number < string < date < datetime < array < map.
//! - Integers, floats and decimals are one numeric domain compared by exact
//!   value, never through an `f64` widening: `9007199254740993` sorts after the
//!   float `9007199254740992.0`; `1`, `1.0` and the decimal `1` tie; the decimal
//!   `0.1` sorts before the float `0.1`, whose exact binary value is larger.
//! - `-0.0` ties `+0.0`. Every NaN, whatever its sign or payload, ties every
//!   other NaN and sorts above `+inf`, so a NaN is a placeable value instead of
//!   a barrier that breaks transitivity.
//! - Strings by UTF-8 bytes (no collation); `false` before `true`; dates by
//!   day; datetimes by [`datetime_to_orderable_i128`].
//! - Arrays element by element, a shorter prefix first; maps by their entries
//!   sorted by key (key bytes, then value), a shorter prefix first.
//!
//! Null placement is not part of the value order. A sort field's authored
//! `null_order` places a top-level null, so callers handle `Value::Null` before
//! reaching this module; the null rank only decides where a null inside an
//! array sorts.
//!
//! This module defines ordering, not predicates. A comparison operator still
//! decides what `NaN < 1` or `null < 1` means; for comparable operands its
//! answer should be this order.
//!
//! Every function here is pure: no allocation except map ordering (which sorts
//! a borrowed entry list) and the caller's output buffer, no I/O, no retained
//! state.

use std::cmp::Ordering;
use std::hash::Hasher;

use chrono::{NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;

use crate::value::Value;

/// Resolution of the exact decimal grid a number's key falls back to when the
/// number is not exactly an `f64`: `rust_decimal`'s maximum scale, so every
/// decimal and every `i64` lands on the grid exactly.
pub const DECIMAL_SORT_KEY_SCALE: u32 = 28;

/// Compare two values under the one value order. Total over every [`Value`].
pub fn compare(_a: &Value, _b: &Value) -> Ordering {
    Ordering::Equal
}

/// Whether two values tie under [`compare`]: the equality group keys use.
pub fn ties(a: &Value, b: &Value) -> bool {
    compare(a, b) == Ordering::Equal
}

/// Append the memcomparable key of `v` to `out`.
pub fn encode(_v: &Value, _out: &mut Vec<u8>) {}

/// Append the key of an integer, as [`encode`] writes it for `Value::Integer`.
pub fn encode_i64(_v: i64, _out: &mut Vec<u8>) {}

/// Append the key of a float, as [`encode`] writes it for `Value::Float`.
pub fn encode_f64(_v: f64, _out: &mut Vec<u8>) {}

/// Append the key of a decimal, as [`encode`] writes it for `Value::Decimal`.
pub fn encode_decimal(_v: Decimal, _out: &mut Vec<u8>) {}

/// Append the key of a string, as [`encode`] writes it for `Value::String`.
pub fn encode_str(_v: &str, _out: &mut Vec<u8>) {}

/// Append the key of a bool, as [`encode`] writes it for `Value::Bool`.
pub fn encode_bool(_v: bool, _out: &mut Vec<u8>) {}

/// Append the key of a date, as [`encode`] writes it for `Value::Date`.
pub fn encode_date(_v: NaiveDate, _out: &mut Vec<u8>) {}

/// Append the key of a datetime, as [`encode`] writes it for `Value::DateTime`.
pub fn encode_datetime(_v: NaiveDateTime, _out: &mut Vec<u8>) {}

/// The class a number falls in under [`ties`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NumericTieClass {
    Integer(i64),
    Float(u64),
    Decimal([u8; 16]),
}

impl NumericTieClass {
    pub fn from_i64(_v: i64) -> Self {
        Self::Integer(0)
    }

    pub fn from_f64(_v: f64) -> Self {
        Self::Integer(0)
    }

    pub fn from_decimal(_v: Decimal) -> Self {
        Self::Integer(0)
    }
}

/// Feed `v`'s tie class into `state`.
pub fn hash_tie_class<H: Hasher>(_v: &Value, _state: &mut H) {}

/// Order-preserving `f64` to `u64`.
pub fn f64_orderable_bits(_f: f64) -> u64 {
    0
}

/// The canonical nanosecond key of a datetime.
pub fn datetime_to_orderable_i128(_dt: NaiveDateTime) -> i128 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_storage::OwnedValues;
    use std::hash::DefaultHasher;

    fn int(v: i64) -> Value {
        Value::Integer(v)
    }

    fn float(v: f64) -> Value {
        Value::Float(v)
    }

    fn dec(text: &str) -> Value {
        Value::Decimal(text.parse().expect("test decimal literal"))
    }

    fn string(text: &str) -> Value {
        Value::from(text)
    }

    fn array(items: Vec<Value>) -> Value {
        Value::Array(OwnedValues::from_vec(items))
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).expect("test date")
    }

    fn datetime(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32, nano: u32) -> NaiveDateTime {
        date(y, m, d)
            .and_hms_nano_opt(h, min, s, nano)
            .expect("test datetime")
    }

    fn key(v: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        encode(v, &mut out);
        out
    }

    fn tie_hash(v: &Value) -> u64 {
        let mut state = DefaultHasher::new();
        hash_tie_class(v, &mut state);
        state.finish()
    }

    /// NaNs of both signs, quiet and signalling, with several payloads.
    fn nans() -> [f64; 5] {
        [
            f64::NAN,
            -f64::NAN,
            f64::from_bits(0x7FF0_0000_0000_0001),
            f64::from_bits(0xFFF4_0000_0000_0ABC),
            f64::from_bits(0x7FFF_FFFF_FFFF_FFFF),
        ]
    }

    /// The last datetime of 2016 was a leap second; chrono stores it as
    /// second 59 with a sub-second field above one second.
    fn leap_second() -> NaiveDateTime {
        datetime(2016, 12, 31, 23, 59, 59, 1_500_000_000)
    }

    fn after_leap_second() -> NaiveDateTime {
        datetime(2017, 1, 1, 0, 0, 0, 500_000_000)
    }

    /// Every fixed case below, for the all-pairs agreement tests.
    fn fixed_cases() -> Vec<Value> {
        let mut cases = vec![
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            int(i64::MIN),
            int(i64::MIN + 1),
            int(-9_007_199_254_740_993),
            int(-1),
            int(0),
            int(1),
            int(2),
            int(9_007_199_254_740_991),
            int(9_007_199_254_740_992),
            int(9_007_199_254_740_993),
            int(i64::MAX - 1),
            int(i64::MAX),
            float(f64::NEG_INFINITY),
            float(f64::MIN),
            float(-9_223_372_036_854_775_808.0),
            float(-1.5),
            float(-1.0),
            float(-0.0),
            float(0.0),
            float(-f64::from_bits(1)),
            float(f64::from_bits(1)),
            float(f64::MIN_POSITIVE),
            float(1e-30),
            float(0.1),
            float(0.5),
            float(1.0),
            float(1.0f64.next_up()),
            float(2.5),
            float(9_007_199_254_740_992.0),
            float(9_223_372_036_854_775_808.0),
            float(1e30),
            float(f64::MAX),
            float(f64::INFINITY),
            dec("0"),
            dec("0.00"),
            dec("-0"),
            dec("1"),
            dec("1.00"),
            dec("-1.5"),
            dec("0.1"),
            dec("0.5"),
            dec("2.5"),
            dec("2.50"),
            dec("1.0000000000000001"),
            dec("0.0000000000000000000000000001"),
            dec("-0.0000000000000000000000000001"),
            dec("9007199254740993"),
            dec("9223372036854775808"),
            dec("79228162514264337593543950335"),
            dec("-79228162514264337593543950335"),
            dec("7.9228162514264337593543950335"),
            string(""),
            string("a"),
            string("a\0"),
            string("a\0b"),
            string("ab"),
            string("é"),
            string("\u{10FFFF}"),
            Value::Date(date(-4000, 1, 1)),
            Value::Date(date(1969, 12, 31)),
            Value::Date(date(1970, 1, 1)),
            Value::Date(date(9999, 12, 31)),
            Value::DateTime(datetime(1969, 12, 31, 23, 59, 59, 999_999_999)),
            Value::DateTime(datetime(1970, 1, 1, 0, 0, 0, 0)),
            Value::DateTime(leap_second()),
            Value::DateTime(after_leap_second()),
            Value::DateTime(datetime(2017, 1, 1, 0, 0, 0, 500_000_001)),
            array(vec![]),
            array(vec![Value::Null]),
            array(vec![Value::Null, int(1)]),
            array(vec![int(1)]),
            array(vec![float(1.0)]),
            array(vec![int(1), int(2)]),
            array(vec![array(vec![int(1)])]),
            array(vec![string("a")]),
            Value::empty_map(),
            Value::map([("a", int(1))]),
            Value::map([("a", int(1)), ("b", int(2))]),
            Value::map([("b", int(2)), ("a", int(1))]),
            Value::map([("a", float(1.0)), ("b", dec("2"))]),
            Value::map([("a", int(2))]),
            Value::map([("b", int(1))]),
        ];
        cases.extend(nans().into_iter().map(float));
        cases
    }

    #[test]
    fn nan_is_one_value_above_infinity() {
        let above = [
            float(f64::INFINITY),
            float(f64::MAX),
            float(0.0),
            float(f64::NEG_INFINITY),
            int(i64::MAX),
            int(i64::MIN),
            dec("79228162514264337593543950335"),
            dec("-0.5"),
        ];
        for a in nans() {
            for b in nans() {
                assert_eq!(compare(&float(a), &float(b)), Ordering::Equal);
                assert_eq!(key(&float(a)), key(&float(b)));
            }
            for other in &above {
                assert_eq!(compare(&float(a), other), Ordering::Greater, "{a:?} vs {other:?}");
                assert_eq!(compare(other, &float(a)), Ordering::Less, "{other:?} vs {a:?}");
                assert!(key(&float(a)) > key(other), "{a:?} vs {other:?}");
            }
        }
    }

    #[test]
    fn negative_zero_ties_zero() {
        let zeros = [float(-0.0), float(0.0), int(0), dec("0.00"), dec("-0")];
        for a in &zeros {
            for b in &zeros {
                assert!(ties(a, b), "{a:?} vs {b:?}");
                assert_eq!(key(a), key(b), "{a:?} vs {b:?}");
            }
            assert_eq!(compare(a, &float(-f64::from_bits(1))), Ordering::Greater);
            assert_eq!(compare(a, &float(f64::from_bits(1))), Ordering::Less);
        }
    }

    #[test]
    fn integers_above_two_pow_53_stay_exact() {
        let two_pow_53 = 9_007_199_254_740_992i64;
        assert_eq!(
            compare(&int(two_pow_53 + 1), &float(9_007_199_254_740_992.0)),
            Ordering::Greater
        );
        assert_eq!(compare(&int(two_pow_53 + 1), &int(two_pow_53)), Ordering::Greater);
        assert!(key(&int(two_pow_53 + 1)) > key(&float(9_007_199_254_740_992.0)));
        assert!(ties(&int(two_pow_53), &float(9_007_199_254_740_992.0)));
        assert_eq!(
            compare(&int(i64::MAX), &float(9_223_372_036_854_775_808.0)),
            Ordering::Less
        );
        assert!(key(&int(i64::MAX)) < key(&float(9_223_372_036_854_775_808.0)));
        assert!(ties(&int(i64::MIN), &float(-9_223_372_036_854_775_808.0)));
        assert_eq!(key(&int(i64::MIN)), key(&float(-9_223_372_036_854_775_808.0)));
        assert_eq!(compare(&int(i64::MAX), &int(i64::MAX - 1)), Ordering::Greater);
        assert!(key(&int(i64::MAX)) > key(&int(i64::MAX - 1)));
    }

    #[test]
    fn numbers_compare_by_exact_value_across_types() {
        let ones = [int(1), float(1.0), dec("1"), dec("1.00")];
        for a in &ones {
            for b in &ones {
                assert!(ties(a, b), "{a:?} vs {b:?}");
                assert_eq!(key(a), key(b), "{a:?} vs {b:?}");
            }
        }
        // The float nearest 0.1 is 0.1000000000000000055511151231257827...
        assert_eq!(compare(&dec("0.1"), &float(0.1)), Ordering::Less);
        assert!(key(&dec("0.1")) < key(&float(0.1)));
        assert!(ties(&dec("2.5"), &float(2.5)));
        assert_eq!(key(&dec("2.5")), key(&float(2.5)));

        // 1 + 1e-16 lies strictly between 1.0 and the next float up.
        let between = dec("1.0000000000000001");
        assert_eq!(compare(&float(1.0), &between), Ordering::Less);
        assert_eq!(compare(&between, &float(1.0f64.next_up())), Ordering::Less);
        assert!(key(&float(1.0)) < key(&between));
        assert!(key(&between) < key(&float(1.0f64.next_up())));

        // Floats beyond the decimal range decide by sign.
        assert_eq!(
            compare(&dec("79228162514264337593543950335"), &float(1e30)),
            Ordering::Less
        );
        assert_eq!(
            compare(&dec("-79228162514264337593543950335"), &float(-1e30)),
            Ordering::Greater
        );
        assert_eq!(
            compare(&dec("0.0000000000000000000000000001"), &float(1e-30)),
            Ordering::Greater
        );
    }

    #[test]
    fn type_rank_is_fixed() {
        let ranked = [
            Value::Null,
            Value::Bool(true),
            float(f64::NEG_INFINITY),
            int(i64::MAX),
            float(f64::NAN),
            string(""),
            Value::Date(date(-4000, 1, 1)),
            Value::DateTime(datetime(1970, 1, 1, 0, 0, 0, 0)),
            array(vec![]),
            Value::empty_map(),
        ];
        for (i, a) in ranked.iter().enumerate() {
            for b in &ranked[i + 1..] {
                if matches!((a, b), (Value::Float(_), Value::Integer(_)))
                    || matches!((a, b), (Value::Integer(_), Value::Float(_)))
                    || matches!((a, b), (Value::Float(_), Value::Float(_)))
                {
                    continue;
                }
                assert_eq!(compare(a, b), Ordering::Less, "{a:?} vs {b:?}");
                assert!(key(a) < key(b), "{a:?} vs {b:?}");
            }
        }
        assert_eq!(compare(&Value::Bool(false), &Value::Bool(true)), Ordering::Less);
        let null_first = array(vec![Value::Null]);
        for v in [Value::Bool(false), int(i64::MIN), float(f64::NEG_INFINITY), string("")] {
            let other = array(vec![v]);
            assert_eq!(compare(&null_first, &other), Ordering::Less, "{other:?}");
            assert!(key(&null_first) < key(&other), "{other:?}");
        }
    }

    #[test]
    fn arrays_and_maps_compare_by_content() {
        assert_eq!(
            compare(&array(vec![int(1)]), &array(vec![int(1), int(0)])),
            Ordering::Less
        );
        assert_eq!(
            compare(&array(vec![int(1), int(2)]), &array(vec![int(2)])),
            Ordering::Less
        );
        assert!(ties(&array(vec![int(1)]), &array(vec![float(1.0)])));
        assert_eq!(key(&array(vec![int(1)])), key(&array(vec![float(1.0)])));
        assert_eq!(compare(&array(vec![]), &array(vec![Value::Null])), Ordering::Less);

        let ab = Value::map([("a", int(1)), ("b", int(2))]);
        let ba = Value::map([("b", int(2)), ("a", int(1))]);
        assert!(ties(&ab, &ba));
        assert_eq!(key(&ab), key(&ba));
        assert_eq!(compare(&Value::map([("a", int(1))]), &ab), Ordering::Less);
        assert_eq!(compare(&ab, &Value::map([("a", int(2))])), Ordering::Less);
        assert_eq!(compare(&ab, &Value::map([("b", int(1))])), Ordering::Less);
        assert!(ties(&ab, &Value::map([("a", dec("1.0")), ("b", float(2.0))])));
    }

    #[test]
    fn datetime_orders_by_its_canonical_key() {
        let leap = Value::DateTime(leap_second());
        let next = Value::DateTime(after_leap_second());
        assert!(ties(&leap, &next));
        assert_eq!(key(&leap), key(&next));
        assert_eq!(tie_hash(&leap), tie_hash(&next));
        // chrono's own order keeps them apart; the one order does not.
        assert_eq!(leap_second().cmp(&after_leap_second()), Ordering::Less);

        let before = Value::DateTime(datetime(1969, 12, 31, 23, 59, 59, 999_999_999));
        let epoch = Value::DateTime(datetime(1970, 1, 1, 0, 0, 0, 0));
        assert_eq!(compare(&before, &epoch), Ordering::Less);
        assert!(key(&before) < key(&epoch));
        assert_eq!(datetime_to_orderable_i128(datetime(1970, 1, 1, 0, 0, 0, 1)), 1);
        assert_eq!(
            datetime_to_orderable_i128(datetime(1969, 12, 31, 23, 59, 59, 999_999_999)),
            -1
        );
    }

    #[test]
    fn encoder_agrees_on_the_fixed_cases() {
        let cases = fixed_cases();
        for a in &cases {
            for b in &cases {
                let (ka, kb) = (key(a), key(b));
                assert_eq!(ka.cmp(&kb), compare(a, b), "{a:?} vs {b:?}");
                assert_eq!(ka == kb, ties(a, b), "{a:?} vs {b:?}");
                assert_eq!(compare(a, b), compare(b, a).reverse(), "{a:?} vs {b:?}");
                if ka != kb {
                    assert!(!ka.starts_with(&kb) && !kb.starts_with(&ka), "{a:?} vs {b:?}");
                }
            }
        }
    }

    #[test]
    fn tie_class_hash_is_equal_on_ties() {
        let cases = fixed_cases();
        let mut tied_pairs = 0;
        for a in &cases {
            for b in &cases {
                if ties(a, b) {
                    tied_pairs += usize::from(!std::ptr::eq(a, b));
                    assert_eq!(tie_hash(a), tie_hash(b), "{a:?} vs {b:?}");
                }
            }
        }
        assert!(tied_pairs > 0, "the fixed cases must contain cross-representation ties");
        assert_ne!(tie_hash(&int(1)), tie_hash(&int(2)));
        assert_ne!(tie_hash(&int(1)), tie_hash(&string("1")));
        assert_ne!(
            tie_hash(&int(9_007_199_254_740_993)),
            tie_hash(&int(9_007_199_254_740_992))
        );

        assert_eq!(NumericTieClass::from_f64(-0.0), NumericTieClass::from_i64(0));
        assert_eq!(
            NumericTieClass::from_decimal("42.00".parse().expect("decimal")),
            NumericTieClass::from_i64(42)
        );
        assert_eq!(
            NumericTieClass::from_decimal("2.50".parse().expect("decimal")),
            NumericTieClass::from_f64(2.5)
        );
        assert_eq!(NumericTieClass::from_f64(f64::NAN), NumericTieClass::from_f64(-f64::NAN));
        assert_ne!(NumericTieClass::from_f64(0.1), NumericTieClass::from_decimal("0.1".parse().expect("decimal")));
    }
}
