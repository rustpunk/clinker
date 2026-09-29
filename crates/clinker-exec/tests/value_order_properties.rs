//! Property suite for the one value order, `clinker_record::order`.
//!
//! Every sort, group, sorted-run merge and declared-order check compares values
//! through that module. Its comparator and its memcomparable encoder are two
//! pieces of code, and a node's in-memory path uses one while its spilled path
//! uses the other, so the properties here prove the two cannot disagree on any
//! value the generator reaches: the comparator is a total order, the encoder's
//! byte order is the comparator, byte equality is exactly a tie, and tied
//! values hash equally.
//!
//! The suite lives in this crate because proptest is one of its
//! dev-dependencies and not one of the record crate's.
//!
//! The generator covers `i64` extremes and the neighbours of 2^53, subnormal,
//! signed-zero, infinite and NaN floats (random sign and payload), decimals of
//! every scale 0..=28 including maximum-precision mantissas, strings with NUL
//! and non-ASCII characters, dates, datetimes including leap seconds, and
//! arrays and maps up to depth 2 with null elements. Half of the pairs and
//! triples are numbers only, and half are drawn as a value and its relatives
//! (the same number in another domain, an adjacent float, a rescaled decimal, a
//! leap second and the instant it ties), so ties and near-ties across domains
//! are frequent rather than accidental.
//!
//! The comparator answers two integers, two floats or two strings without its
//! domain dispatch, so one property draws those pairs densely and checks the
//! answer against each type's own order and against the encoder, which shares
//! no code with that shortcut.
//!
//! Group keys (`GroupByKey`) must tie exactly as the order does, because a
//! hash table groups by key equality while a spilled or streamed grouping
//! detects groups by byte ties, so one property proves key equality, key
//! hashing and the keys' tie bytes agree with the order.
//!
//! The Sort node's authored key adds null placement, direction and several
//! fields on top of the value order, so a last property proves its byte key
//! and its comparator agree on whole records as well.
//!
//! Case counts: 1,024 per pair property (the group-key and authored-key
//! properties included) and 512 for the triple property.

use std::cmp::Ordering;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use clinker_exec::pipeline::sort_key::{compare_authored_keys, stable_sort_key_for_record};
use clinker_plan::config::{NullOrder, SortField, SortOrder};
use clinker_record::order::{NumericTieClass, compare, encode, hash_tie_class, ties};
use clinker_record::owned_storage::{OwnedValues, SharedStorage};
use clinker_record::{GroupByKey, Record, Schema, Value, value_to_group_key};
use proptest::prelude::*;
use rust_decimal::Decimal;

const TWO_POW_53: i64 = 1 << 53;
const MAX_MANTISSA: i128 = (1 << 96) - 1;

fn key(v: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode(v, &mut out);
    out
}

fn edge_i64() -> impl Strategy<Value = i64> {
    prop_oneof![
        3 => prop::sample::select(vec![
            i64::MIN,
            i64::MIN + 1,
            -TWO_POW_53 - 1,
            -TWO_POW_53,
            -TWO_POW_53 + 1,
            -1,
            0,
            1,
            TWO_POW_53 - 1,
            TWO_POW_53,
            TWO_POW_53 + 1,
            i64::MAX - 1,
            i64::MAX,
        ]),
        2 => -8i64..=8,
        1 => (TWO_POW_53 - 4)..=(TWO_POW_53 + 4),
        2 => any::<i64>(),
    ]
}

/// A NaN of random sign, quiet or signalling, with a random payload.
fn nan() -> impl Strategy<Value = f64> {
    (any::<bool>(), 1u64..(1 << 52)).prop_map(|(negative, payload)| {
        f64::from_bits((u64::from(negative) << 63) | 0x7FF0_0000_0000_0000 | payload)
    })
}

fn edge_f64() -> impl Strategy<Value = f64> {
    prop_oneof![
        3 => prop::sample::select(vec![
            0.0,
            -0.0,
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::MAX,
            f64::MIN,
            f64::MIN_POSITIVE,
            -f64::MIN_POSITIVE,
            f64::from_bits(1),
            -f64::from_bits(1),
            f64::from_bits(0x000F_FFFF_FFFF_FFFF),
            9_007_199_254_740_991.0,
            9_007_199_254_740_992.0,
            9_007_199_254_740_994.0,
            -9_007_199_254_740_992.0,
            9_223_372_036_854_775_808.0,
            -9_223_372_036_854_775_808.0,
            0.1,
            0.5,
            1.0,
            2.5,
            -1.5,
            1e-28,
            1e30,
            7.922_816_251_426_434e28,
        ]),
        2 => nan(),
        // Small dyadic rationals: the same values the decimal and integer
        // strategies produce, so cross-domain ties are common.
        2 => (-8i64..=8, 0u32..4).prop_map(|(k, n)| k as f64 / f64::from(1u32 << n)),
        2 => edge_i64().prop_map(|i| i as f64),
        2 => any::<f64>(),
        1 => any::<u64>().prop_map(f64::from_bits),
    ]
}

fn edge_decimal() -> impl Strategy<Value = Decimal> {
    prop_oneof![
        2 => prop::sample::select(vec![
            Decimal::MAX,
            Decimal::MIN,
            Decimal::ZERO,
            Decimal::ONE,
            Decimal::NEGATIVE_ONE,
            Decimal::new(1, 28),
            Decimal::new(-1, 28),
            Decimal::from_i128_with_scale(MAX_MANTISSA, 28),
            Decimal::new(1, 1),
            Decimal::new(25, 1),
            Decimal::new(10_000_000_000_000_001, 16),
        ]),
        // k / 2^n written exactly in decimal (k·5^n at scale n), with extra
        // trailing zeros so equal values arrive at different scales.
        3 => (-8i64..=8, 0u32..4, 0u32..=24).prop_map(|(k, n, extra)| {
            let mantissa = i128::from(k) * 5i128.pow(n) * 10i128.pow(extra);
            Decimal::from_i128_with_scale(mantissa, n + extra)
        }),
        2 => edge_i64().prop_map(Decimal::from),
        2 => (-MAX_MANTISSA..=MAX_MANTISSA, 0u32..=28)
            .prop_map(|(m, s)| Decimal::from_i128_with_scale(m, s)),
        1 => (any::<i64>(), 0u32..=28).prop_map(|(m, s)| Decimal::new(m, s)),
    ]
}

fn number() -> BoxedStrategy<Value> {
    prop_oneof![
        edge_i64().prop_map(Value::Integer),
        edge_f64().prop_map(Value::Float),
        edge_decimal().prop_map(Value::Decimal),
    ]
    .boxed()
}

fn text() -> impl Strategy<Value = String> {
    prop::collection::vec(
        prop_oneof![
            Just('\0'),
            Just('a'),
            Just('b'),
            Just('é'),
            Just('\u{10FFFF}'),
            any::<char>(),
        ],
        0..4,
    )
    .prop_map(|chars| chars.into_iter().collect())
}

fn date() -> impl Strategy<Value = NaiveDate> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
        .expect("epoch")
        .num_days_from_ce();
    prop_oneof![
        NaiveDate::MIN.num_days_from_ce()..=NaiveDate::MAX.num_days_from_ce(),
        (epoch - 3)..=(epoch + 3),
    ]
    .prop_map(|days| NaiveDate::from_num_days_from_ce_opt(days).expect("day in chrono's range"))
}

fn datetime() -> impl Strategy<Value = NaiveDateTime> {
    prop_oneof![
        3 => (date(), 0u32..86_400, 0u32..1_000_000_000).prop_map(|(day, secs, nano)| {
            let time = NaiveTime::from_num_seconds_from_midnight_opt(secs, nano).expect("time");
            day.and_time(time)
        }),
        // A leap second: second 59 with a sub-second field above one second.
        1 => (date(), 0u32..1_000_000_000).prop_map(|(day, nano)| {
            let time = NaiveTime::from_num_seconds_from_midnight_opt(86_399, 1_000_000_000 + nano)
                .expect("leap second");
            day.and_time(time)
        }),
    ]
}

fn scalar() -> BoxedStrategy<Value> {
    prop_oneof![
        6 => number(),
        1 => any::<bool>().prop_map(Value::Bool),
        2 => text().prop_map(Value::from),
        1 => date().prop_map(Value::Date),
        2 => datetime().prop_map(Value::DateTime),
    ]
    .boxed()
}

fn element() -> impl Strategy<Value = Value> {
    prop_oneof![5 => scalar(), 1 => Just(Value::Null)]
}

fn map_key() -> impl Strategy<Value = &'static str> {
    prop::sample::select(vec!["", "a", "b", "a\0"])
}

fn array_of(inner: impl Strategy<Value = Value>) -> impl Strategy<Value = Value> {
    prop::collection::vec(inner, 0..4).prop_map(|items| Value::Array(OwnedValues::from_vec(items)))
}

fn map_of(inner: impl Strategy<Value = Value>) -> impl Strategy<Value = Value> {
    prop::collection::vec((map_key(), inner), 0..4).prop_map(Value::map)
}

/// A value of any domain; arrays and maps nest to depth 2.
fn value() -> BoxedStrategy<Value> {
    let nested = || {
        prop_oneof![
            4 => element(),
            1 => array_of(element()),
            1 => map_of(element()),
        ]
    };
    prop_oneof![
        8 => scalar(),
        1 => Just(Value::Null),
        1 => array_of(nested()),
        1 => map_of(nested()),
    ]
    .boxed()
}

/// The decimal equal to `f`, when `f` is a dyadic rational a decimal can hold:
/// `f · 2^n` is an integer `k` for some `n ≤ 28`, and `k · 5^n` fits the
/// 96-bit mantissa at scale `n`.
fn exact_decimal(f: f64) -> Option<Decimal> {
    if !f.is_finite() || f.abs() >= 7.9e28 {
        return None;
    }
    (0u32..=28).find_map(|n| {
        // Scaling by a power of two is exact.
        let scaled = f * f64::from(1u32 << n);
        if scaled.fract() != 0.0 || scaled.abs() >= 1.7e38 {
            return None;
        }
        (scaled as i128)
            .checked_mul(5i128.pow(n))
            .and_then(|mantissa| Decimal::try_from_i128_with_scale(mantissa, n).ok())
    })
}

/// Values that tie `v` or sit next to it in the order, in other domains and
/// representations where one exists.
fn relatives(v: &Value) -> Vec<Value> {
    let mut out = vec![v.clone()];
    match v {
        Value::Integer(i) => {
            let rounded = *i as f64;
            out.extend([
                Value::Float(rounded),
                Value::Float(rounded.next_up()),
                Value::Float(rounded.next_down()),
                Value::Decimal(Decimal::from(*i)),
                Value::Decimal(Decimal::from_i128_with_scale(i128::from(*i) * 1000, 3)),
            ]);
            out.extend(i.checked_add(1).map(Value::Integer));
            out.extend(i.checked_sub(1).map(Value::Integer));
        }
        Value::Float(f) => {
            out.extend([
                Value::Float(f.next_up()),
                Value::Float(f.next_down()),
                Value::Float(-*f),
            ]);
            if f.is_nan() {
                out.extend([
                    Value::Float(f64::NAN),
                    Value::Float(-f64::NAN),
                    Value::Float(f64::INFINITY),
                ]);
            }
            if f.fract() == 0.0
                && *f >= -9_223_372_036_854_775_808.0
                && *f < 9_223_372_036_854_775_808.0
            {
                let i = *f as i64;
                out.extend([Value::Integer(i), Value::Decimal(Decimal::from(i))]);
            }
            out.extend(exact_decimal(*f).map(Value::Decimal));
        }
        Value::Decimal(d) => {
            // From the normalized form, so a short dyadic such as 2.50 lands
            // on its float exactly.
            let normal = d.normalize();
            let approx = normal.mantissa() as f64 / 10f64.powi(normal.scale() as i32);
            out.extend([
                Value::Float(approx),
                Value::Float(approx.next_up()),
                Value::Float(approx.next_down()),
                Value::Decimal(d.normalize()),
            ]);
            if d.scale() < 28 {
                out.extend(
                    d.mantissa()
                        .checked_mul(10)
                        .and_then(|m| Decimal::try_from_i128_with_scale(m, d.scale() + 1).ok())
                        .map(Value::Decimal),
                );
            }
            out.extend(d.checked_add(Decimal::new(1, 28)).map(Value::Decimal));
            out.extend(d.checked_sub(Decimal::new(1, 28)).map(Value::Decimal));
            if d.fract().is_zero()
                && let Ok(i) = i64::try_from(*d)
            {
                out.push(Value::Integer(i));
            }
        }
        Value::DateTime(dt) => {
            let nanos = dt.nanosecond();
            out.extend(
                [
                    dt.checked_add_signed(chrono::TimeDelta::nanoseconds(1)),
                    dt.checked_sub_signed(chrono::TimeDelta::nanoseconds(1)),
                ]
                .into_iter()
                .flatten()
                .map(Value::DateTime),
            );
            if nanos >= 1_000_000_000 {
                // The instant a leap second ties: the next second, same fraction.
                out.extend(
                    dt.date()
                        .succ_opt()
                        .and_then(|next| next.and_hms_nano_opt(0, 0, 0, nanos - 1_000_000_000))
                        .map(Value::DateTime),
                );
            }
        }
        Value::String(s) => {
            let s = s.as_str();
            out.extend([
                Value::from(format!("{s}\0")),
                Value::from(format!("{s}a")),
                Value::from(s.chars().take(1).collect::<String>()),
            ]);
        }
        Value::Map(m) => {
            let reversed: Vec<(String, Value)> = m
                .iter()
                .rev()
                .map(|(k, v)| (k.to_string(), v.clone()))
                .collect();
            out.push(Value::map(reversed));
        }
        Value::Array(items) => {
            let mut longer = items.to_vec();
            longer.push(Value::Null);
            out.push(Value::Array(OwnedValues::from_vec(longer)));
            out.push(Value::Array(OwnedValues::from_vec(
                items.iter().take(1).cloned().collect(),
            )));
        }
        Value::Null | Value::Bool(_) | Value::Date(_) => {}
    }
    out
}

fn related_pair(base: impl Strategy<Value = Value>) -> impl Strategy<Value = (Value, Value)> {
    base.prop_flat_map(|a| {
        let related = relatives(&a);
        (Just(a), prop::sample::select(related))
    })
}

fn related_triple(
    base: impl Strategy<Value = Value>,
) -> impl Strategy<Value = (Value, Value, Value)> {
    base.prop_flat_map(|a| {
        let related = relatives(&a);
        (
            Just(a),
            prop::sample::select(related.clone()),
            prop::sample::select(related),
        )
    })
}

/// Pairs of any domain and pairs of numbers, each drawn half independently and
/// half as a value and one of its relatives. Numbers get half the weight
/// because the numeric domain is where three representations share one order.
fn pair() -> BoxedStrategy<(Value, Value)> {
    prop_oneof![
        (value(), value()),
        related_pair(value()),
        (number(), number()),
        related_pair(number()),
    ]
    .boxed()
}

fn triple() -> BoxedStrategy<(Value, Value, Value)> {
    prop_oneof![
        (value(), value(), value()),
        related_triple(value()),
        (number(), number(), number()),
        related_triple(number()),
    ]
    .boxed()
}

/// Two integers, two floats or two strings: the pairs the comparator answers
/// without its domain dispatch. Each type is drawn half independently and half
/// as a value and a neighbour (the next integer, the same float or its
/// negation or its next float, the same string or it extended), so ties, NaN
/// against NaN, the two zeros and shared prefixes are frequent.
fn same_type_pair() -> BoxedStrategy<(Value, Value)> {
    prop_oneof![
        (edge_i64(), edge_i64()).prop_map(|(x, y)| (Value::Integer(x), Value::Integer(y))),
        (edge_i64(), -1i64..=1)
            .prop_map(|(x, step)| (Value::Integer(x), Value::Integer(x.saturating_add(step)))),
        (edge_f64(), edge_f64()).prop_map(|(x, y)| (Value::Float(x), Value::Float(y))),
        (edge_f64(), 0u8..3).prop_map(|(x, neighbour)| {
            let y = match neighbour {
                0 => x,
                1 => -x,
                _ => x.next_up(),
            };
            (Value::Float(x), Value::Float(y))
        }),
        (text(), text()).prop_map(|(x, y)| (Value::from(x.as_str()), Value::from(y.as_str()))),
        (text(), text()).prop_map(|(x, suffix)| {
            (
                Value::from(x.as_str()),
                Value::from(format!("{x}{suffix}").as_str()),
            )
        }),
    ]
    .boxed()
}

fn non_nan_number() -> impl Strategy<Value = Value> {
    number().prop_filter(
        "a NaN is not a non-NaN number",
        |v| !matches!(v, Value::Float(f) if f.is_nan()),
    )
}

fn tie_hash(v: &Value) -> u64 {
    let mut state = DefaultHasher::new();
    hash_tie_class(v, &mut state);
    state.finish()
}

fn tie_class(v: &Value) -> Option<NumericTieClass> {
    match v {
        Value::Integer(i) => Some(NumericTieClass::from_i64(*i)),
        Value::Float(f) => Some(NumericTieClass::from_f64(*f)),
        Value::Decimal(d) => Some(NumericTieClass::from_decimal(*d)),
        _ => None,
    }
}

/// `x ≤ y ≤ z` implies `x ≤ z`, strictly when either step is strict.
fn check_transitive(x: &Value, y: &Value, z: &Value) -> Result<(), TestCaseError> {
    let (xy, yz, xz) = (compare(x, y), compare(y, z), compare(x, z));
    if xy != Ordering::Greater && yz != Ordering::Greater {
        let expected = if xy == Ordering::Equal && yz == Ordering::Equal {
            Ordering::Equal
        } else {
            Ordering::Less
        };
        prop_assert_eq!(xz, expected, "{:?} / {:?} / {:?}", x, y, z);
    }
    Ok(())
}

// Case counts: 1,024 per pair property, 512 per triple property. The whole
// file runs in well under a second in a debug build.
proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn encoder_agrees_with_comparator((a, b) in pair()) {
        prop_assert_eq!(key(&a).cmp(&key(&b)), compare(&a, &b), "{:?} vs {:?}", a, b);
    }

    #[test]
    fn equal_bytes_iff_tie((a, b) in pair()) {
        prop_assert_eq!(key(&a) == key(&b), ties(&a, &b), "{:?} vs {:?}", a, b);
    }

    #[test]
    fn encoding_is_prefix_free((a, b) in pair()) {
        let (ka, kb) = (key(&a), key(&b));
        if ka != kb {
            prop_assert!(!ka.starts_with(&kb), "{:?} vs {:?}", a, b);
            prop_assert!(!kb.starts_with(&ka), "{:?} vs {:?}", a, b);
        }
    }

    #[test]
    fn ties_hash_equally((a, b) in pair()) {
        if ties(&a, &b) {
            prop_assert_eq!(tie_hash(&a), tie_hash(&b), "{:?} vs {:?}", a, b);
        }
        // Group keys lean on the numeric class being exactly the tie, in
        // both directions, not only on equal hashes.
        if let (Some(ca), Some(cb)) = (tie_class(&a), tie_class(&b)) {
            prop_assert_eq!(ca == cb, ties(&a, &b), "{:?} vs {:?}", a, b);
        }
    }

    #[test]
    fn same_type_pairs_order_by_their_type_and_their_keys((a, b) in same_type_pair()) {
        let expected = match (&a, &b) {
            (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
            (Value::Float(x), Value::Float(y)) => match (x.is_nan(), y.is_nan()) {
                (true, true) => Ordering::Equal,
                (true, false) => Ordering::Greater,
                (false, true) => Ordering::Less,
                // `partial_cmp` already ties -0.0 with 0.0.
                (false, false) => x.partial_cmp(y).expect("neither side is NaN"),
            },
            (Value::String(x), Value::String(y)) => {
                x.as_str().as_bytes().cmp(y.as_str().as_bytes())
            }
            _ => unreachable!("the strategy draws same-type pairs only"),
        };
        prop_assert_eq!(compare(&a, &b), expected, "{:?} vs {:?}", a, b);
        prop_assert_eq!(key(&a).cmp(&key(&b)), expected, "{:?} vs {:?}", a, b);
    }

    #[test]
    fn every_nan_ties_and_sorts_above_infinity(
        first in nan(),
        second in nan(),
        other in non_nan_number(),
    ) {
        let (first, second) = (Value::Float(first), Value::Float(second));
        prop_assert!(ties(&first, &second), "{:?} vs {:?}", first, second);
        prop_assert_eq!(key(&first), key(&second));
        prop_assert_eq!(compare(&first, &other), Ordering::Greater, "{:?}", other);
        prop_assert_eq!(compare(&other, &first), Ordering::Less, "{:?}", other);
        prop_assert!(key(&first) > key(&other), "{:?}", other);
        prop_assert_eq!(
            compare(&first, &Value::Float(f64::INFINITY)),
            Ordering::Greater
        );
    }
}

/// Pairs of values a group key accepts (every scalar, NaN included; arrays
/// and maps are not group keys), drawn as [`pair`] draws them, with either or
/// both sides sometimes replaced by a null.
fn group_key_pair() -> BoxedStrategy<(Value, Value)> {
    let groupable = |v: &Value| !matches!(v, Value::Array(_) | Value::Map(_));
    (
        pair().prop_filter("arrays and maps are not group keys", move |(a, b)| {
            groupable(a) && groupable(b)
        }),
        0u8..6,
    )
        .prop_map(|((a, b), nulls)| match nulls {
            0 => (Value::Null, b),
            1 => (a, Value::Null),
            2 => (Value::Null, Value::Null),
            _ => (a, b),
        })
        .boxed()
}

/// The group key a grouping node builds for `v`: a null keys as `Null`.
fn group_key(v: &Value) -> GroupByKey {
    value_to_group_key(v, "k", 0)
        .expect("a scalar is a group key")
        .unwrap_or(GroupByKey::Null)
}

fn group_key_hash(k: &GroupByKey) -> u64 {
    let mut state = DefaultHasher::new();
    k.hash(&mut state);
    state.finish()
}

fn tie_bytes(k: &GroupByKey) -> Vec<u8> {
    let mut out = Vec::new();
    k.encode_tie_bytes(&mut out);
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// Group keys are equal exactly when their values tie in the value order
    /// (two nulls included), equal keys hash equally, and their tie bytes
    /// are equal exactly when the keys are: a hash table, a spilled merge and
    /// a sorted grouping form the same groups.
    #[test]
    fn group_key_equality_is_the_order_tie((a, b) in group_key_pair()) {
        let (ka, kb) = (group_key(&a), group_key(&b));
        prop_assert_eq!(ka == kb, ties(&a, &b), "{:?} vs {:?}", a, b);
        if ka == kb {
            prop_assert_eq!(group_key_hash(&ka), group_key_hash(&kb), "{:?} vs {:?}", a, b);
        }
        prop_assert_eq!(tie_bytes(&ka) == tie_bytes(&kb), ka == kb, "{:?} vs {:?}", a, b);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(512))]

    #[test]
    fn value_order_is_a_total_order((a, b, c) in triple()) {
        for v in [&a, &b, &c] {
            prop_assert_eq!(compare(v, v), Ordering::Equal, "{:?}", v);
        }
        for (x, y) in [(&a, &b), (&b, &c), (&a, &c)] {
            prop_assert_eq!(compare(x, y), compare(y, x).reverse(), "{:?} vs {:?}", x, y);
        }
        for (x, y, z) in [
            (&a, &b, &c),
            (&a, &c, &b),
            (&b, &a, &c),
            (&b, &c, &a),
            (&c, &a, &b),
            (&c, &b, &a),
        ] {
            check_transitive(x, y, z)?;
        }
    }
}

/// One sort field's content in one record: a value (possibly null) or no
/// column of that name at all.
#[derive(Debug, Clone)]
enum Slot {
    Present(Value),
    Absent,
}

fn slot(v: Value) -> impl Strategy<Value = Slot> {
    prop_oneof![
        6 => Just(Slot::Present(v)),
        1 => Just(Slot::Present(Value::Null)),
        1 => Just(Slot::Absent),
    ]
}

/// The two records' slots for one sort field, drawn from the pair generator
/// so a field ties as often as it differs and later fields get to decide.
fn slot_pair() -> BoxedStrategy<(Slot, Slot)> {
    pair().prop_flat_map(|(a, b)| (slot(a), slot(b))).boxed()
}

/// A record holding the present slots under the names `f0`, `f1`, …; an
/// absent slot has no column, so `Record::get` finds nothing for it.
fn slot_record<'a>(slots: impl Iterator<Item = &'a Slot>) -> Record {
    let mut names = Vec::new();
    let mut values = Vec::new();
    for (index, slot) in slots.enumerate() {
        if let Slot::Present(v) = slot {
            names.push(format!("f{index}").into());
            values.push(v.clone());
        }
    }
    Record::new(
        SharedStorage::from_arc(Arc::new(Schema::new(names))),
        values,
    )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    /// The Sort node's byte key orders two records exactly as its comparator
    /// does, over one to three fields with any direction and nulls first or
    /// last: the spilled merge and the streaming aggregate compare the bytes,
    /// the resident sort the comparator.
    #[test]
    fn authored_key_encoder_agrees_with_authored_comparator(
        fields in prop::collection::vec((slot_pair(), any::<bool>(), any::<bool>()), 1..=3),
    ) {
        let sort_by: Vec<SortField> = fields
            .iter()
            .enumerate()
            .map(|(index, (_, descending, nulls_first))| SortField {
                field: format!("f{index}"),
                order: if *descending { SortOrder::Desc } else { SortOrder::Asc },
                null_order: Some(if *nulls_first { NullOrder::First } else { NullOrder::Last }),
            })
            .collect();
        let a = slot_record(fields.iter().map(|((a, _), _, _)| a));
        let b = slot_record(fields.iter().map(|((_, b), _, _)| b));
        prop_assert_eq!(
            stable_sort_key_for_record(&a, &sort_by).cmp(&stable_sort_key_for_record(&b, &sort_by)),
            compare_authored_keys(&a, &b, &sort_by),
            "{:?} vs {:?} under {:?}",
            fields.iter().map(|((a, _), _, _)| a).collect::<Vec<_>>(),
            fields.iter().map(|((_, b), _, _)| b).collect::<Vec<_>>(),
            sort_by
        );
    }
}

#[test]
fn relatives_reach_cross_domain_ties() {
    // Guards the generator itself: without relatives drawn this way, the
    // properties would almost never see a cross-domain tie.
    let related = relatives(&Value::Integer(TWO_POW_53));
    let ties = related
        .iter()
        .filter(|r| compare(&Value::Integer(TWO_POW_53), r) == Ordering::Equal)
        .count();
    assert!(
        ties >= 3,
        "an integer must have tied relatives in other domains"
    );
    assert!(
        ties < related.len(),
        "an integer must have untied neighbours"
    );
}
