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
//! arrays and maps up to depth 2 with null elements. Half of the pairs are
//! drawn as a value and one of its relatives (the same number in another
//! domain, an adjacent float, a rescaled decimal, a leap second and the instant
//! it ties), so ties and near-ties across domains are frequent rather than
//! accidental.

use std::cmp::Ordering;

use chrono::{Datelike, NaiveDate, NaiveDateTime, NaiveTime, Timelike};
use clinker_record::Value;
use clinker_record::order::{compare, encode};
use clinker_record::owned_storage::OwnedValues;
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

fn number() -> impl Strategy<Value = Value> {
    prop_oneof![
        edge_i64().prop_map(Value::Integer),
        edge_f64().prop_map(Value::Float),
        edge_decimal().prop_map(Value::Decimal),
    ]
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

fn scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        6 => number(),
        1 => any::<bool>().prop_map(Value::Bool),
        2 => text().prop_map(Value::from),
        1 => date().prop_map(Value::Date),
        2 => datetime().prop_map(Value::DateTime),
    ]
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
fn value() -> impl Strategy<Value = Value> {
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
        }
        Value::Decimal(d) => {
            let approx = d.mantissa() as f64 / 10f64.powi(d.scale() as i32);
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

/// Half independent pairs, half a value and one of its relatives.
fn pair() -> impl Strategy<Value = (Value, Value)> {
    prop_oneof![
        (value(), value()),
        value().prop_flat_map(|a| {
            let related = relatives(&a);
            (Just(a), prop::sample::select(related))
        }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1024))]

    #[test]
    fn encoder_agrees_with_comparator((a, b) in pair()) {
        prop_assert_eq!(key(&a).cmp(&key(&b)), compare(&a, &b), "{:?} vs {:?}", a, b);
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
