//! GroupByKey and value_to_group_key: the key every grouping, partitioning
//! and dedup path groups rows by.
//!
//! Lives in the foundation crate so `cxl::eval` can use it for distinct
//! without depending on `clinker-exec`, where the pipeline index that
//! groups on these keys lives.
//!
//! Two keys are equal exactly when their values tie in the one value order
//! ([`crate::order`]). A grouping that detects groups by comparing keys in a
//! hash table and one that detects them by byte ties of a sorted, spilled or
//! streamed key therefore form the same groups.

use std::hash::{Hash, Hasher};

use chrono::{NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::order::{self, NumericTieClass};
use crate::value::Value;

/// The one bit pattern every NaN key holds, whatever the NaN's sign or payload.
const CANONICAL_NAN_BITS: u64 = f64::NAN.to_bits();

/// Group-by key for grouping, partitioning and distinct dedup.
///
/// Equality and hashing are the one value order's tie, not the variant's
/// payload: an `Int`, a `Float` and a `Decimal` holding the same number are
/// equal and hash equally (`5`, `5.0` and the decimal `5` are one group),
/// distinct integers are never equal however large, every NaN is one key,
/// and a `DateTime` compares by its nanosecond instant. `Null` equals only
/// `Null`; keys of different non-numeric variants are unequal. Hashing never
/// allocates.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GroupByKey {
    Str(Box<str>),
    /// An integer, exactly. `value_to_group_key` keeps a `Value::Integer`
    /// here without widening; callers that already hold a typed integer
    /// partition or row index construct it directly.
    Int(i64),
    /// `f64::to_bits()` of a float, with `-0.0` canonicalized to `0.0` and
    /// every NaN to one bit pattern, so a NaN key round-trips as NaN.
    Float(u64),
    /// Exact `decimal` key: the 16-byte form of the value normalized
    /// (trailing zeros stripped), so `2.50` and `2.5` produce identical bytes.
    Decimal([u8; 16]),
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    /// SQL-standard NULL = NULL for DISTINCT/GROUP BY (ISO 9075).
    /// 12/12 systems agree: PostgreSQL, MySQL, Spark, DuckDB, Polars, etc.
    Null,
}

// Hash rank of each value domain. Numbers share one rank because an `Int`,
// a `Float` and a `Decimal` can be equal.
const RANK_NULL: u8 = 0;
const RANK_BOOL: u8 = 1;
const RANK_NUMBER: u8 = 2;
const RANK_STRING: u8 = 3;
const RANK_DATE: u8 = 4;
const RANK_DATETIME: u8 = 5;

impl GroupByKey {
    /// The key's tie class when it is a number: equal classes are exactly
    /// the numbers the value order ties, whatever their variants.
    fn numeric_tie_class(&self) -> Option<NumericTieClass> {
        match self {
            GroupByKey::Int(i) => Some(NumericTieClass::from_i64(*i)),
            GroupByKey::Float(bits) => Some(NumericTieClass::from_f64(f64::from_bits(*bits))),
            GroupByKey::Decimal(bytes) => {
                Some(NumericTieClass::from_decimal(Decimal::deserialize(*bytes)))
            }
            _ => None,
        }
    }

    /// Append bytes that are equal for two keys exactly when the keys are
    /// equal: `0x00` for `Null`, else `0x01` followed by the value order's
    /// byte key of the key's value ([`order::encode`], written through the
    /// matching `order::encode_*`, so no `Value` is built).
    ///
    /// The bytes are prefix-free, so a tuple of keys can be written one after
    /// another and two tuples' bytes are equal exactly when every key is. They
    /// also sort `Null` first and every other key in the value order.
    /// Allocates only by growing `out`.
    pub fn encode_tie_bytes(&self, out: &mut Vec<u8>) {
        out.push(if matches!(self, GroupByKey::Null) {
            0x00
        } else {
            0x01
        });
        match self {
            GroupByKey::Null => {}
            GroupByKey::Str(s) => order::encode_str(s, out),
            GroupByKey::Int(i) => order::encode_i64(*i, out),
            GroupByKey::Float(bits) => order::encode_f64(f64::from_bits(*bits), out),
            GroupByKey::Decimal(bytes) => order::encode_decimal(Decimal::deserialize(*bytes), out),
            GroupByKey::Bool(b) => order::encode_bool(*b, out),
            GroupByKey::Date(d) => order::encode_date(*d, out),
            GroupByKey::DateTime(dt) => order::encode_datetime(*dt, out),
        }
    }

    /// Convert a group-by key back into a `Value`, to stamp a group's key
    /// columns on its output row and for finalize-time evaluation in
    /// aggregation scope.
    ///
    /// Lossless against `value_to_group_key` up to its canonicalization: an
    /// integer comes back as the same integer, `-0.0` as `0.0`, any NaN as
    /// the one canonical NaN and a decimal normalized. A group keeps the key
    /// of its first-arriving row, so its output reports that row's value
    /// (an integer column is written as integers even when a later row of
    /// the same group held the equal float).
    pub fn to_value(&self) -> Value {
        match self {
            GroupByKey::Null => Value::Null,
            GroupByKey::Int(n) => Value::Integer(*n),
            // `GroupByKey::Str` keeps its own `Box<str>` (it is an independent
            // HashMap/HashSet key); convert through `&str` to the inline/Arc-
            // backed field-value string when materializing the group key.
            GroupByKey::Str(s) => Value::String(s.as_ref().into()),
            GroupByKey::Bool(b) => Value::Bool(*b),
            GroupByKey::Date(d) => Value::Date(*d),
            GroupByKey::DateTime(dt) => Value::DateTime(*dt),
            GroupByKey::Float(bits) => Value::Float(f64::from_bits(*bits)),
            // Reconstruct the exact (normalized) decimal from its 16-byte form.
            GroupByKey::Decimal(bytes) => Value::Decimal(Decimal::deserialize(*bytes)),
        }
    }
}

impl PartialEq for GroupByKey {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (GroupByKey::Null, GroupByKey::Null) => true,
            (GroupByKey::Str(a), GroupByKey::Str(b)) => a == b,
            // Same-variant numbers compare with the expression the value
            // order uses for them; mixed variants go through the tie class.
            (GroupByKey::Int(a), GroupByKey::Int(b)) => a == b,
            (GroupByKey::Float(a), GroupByKey::Float(b)) => {
                order::f64_orderable_bits(f64::from_bits(*a))
                    == order::f64_orderable_bits(f64::from_bits(*b))
            }
            (GroupByKey::Bool(a), GroupByKey::Bool(b)) => a == b,
            (GroupByKey::Date(a), GroupByKey::Date(b)) => a == b,
            (GroupByKey::DateTime(a), GroupByKey::DateTime(b)) => {
                order::datetime_to_orderable_i128(*a) == order::datetime_to_orderable_i128(*b)
            }
            _ => match (self.numeric_tie_class(), other.numeric_tie_class()) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            },
        }
    }
}

impl Eq for GroupByKey {}

impl Hash for GroupByKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            GroupByKey::Null => state.write_u8(RANK_NULL),
            GroupByKey::Bool(b) => {
                state.write_u8(RANK_BOOL);
                b.hash(state);
            }
            GroupByKey::Int(i) => {
                state.write_u8(RANK_NUMBER);
                NumericTieClass::from_i64(*i).hash(state);
            }
            GroupByKey::Float(bits) => {
                state.write_u8(RANK_NUMBER);
                NumericTieClass::from_f64(f64::from_bits(*bits)).hash(state);
            }
            GroupByKey::Decimal(bytes) => {
                state.write_u8(RANK_NUMBER);
                NumericTieClass::from_decimal(Decimal::deserialize(*bytes)).hash(state);
            }
            GroupByKey::Str(s) => {
                state.write_u8(RANK_STRING);
                s.hash(state);
            }
            GroupByKey::Date(d) => {
                state.write_u8(RANK_DATE);
                d.hash(state);
            }
            GroupByKey::DateTime(dt) => {
                state.write_u8(RANK_DATETIME);
                order::datetime_to_orderable_i128(*dt).hash(state);
            }
        }
    }
}

/// Errors from group key conversion.
#[derive(Debug)]
pub enum GroupKeyError {
    /// A value that cannot be a group key: an array or a map.
    UnsupportedType {
        field: String,
        type_name: &'static str,
        row: u64,
    },
}

impl std::fmt::Display for GroupKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupKeyError::UnsupportedType {
                field,
                type_name,
                row,
            } => {
                write!(
                    f,
                    "unsupported type '{}' in group_by field '{}' at row {}",
                    type_name, field, row
                )
            }
        }
    }
}

impl std::error::Error for GroupKeyError {}

/// Convert a Value to a GroupByKey.
///
/// Every number keeps its own domain: an integer becomes `Int` exactly (never
/// widened to a float), a float `Float` with `-0.0` canonicalized to `0.0`
/// and every NaN, of either sign and any payload, to one canonical NaN, and a
/// decimal the normalized `Decimal`. Key equality then ties equal numbers
/// across domains. `Null` returns `None` (the caller decides whether to skip
/// the row or key it as `GroupByKey::Null`). An array or a map returns
/// `GroupKeyError::UnsupportedType`.
pub fn value_to_group_key(
    val: &Value,
    field: &str,
    row: u64,
) -> Result<Option<GroupByKey>, GroupKeyError> {
    match val {
        Value::Null => Ok(None),

        Value::Float(f) if f.is_nan() => Ok(Some(GroupByKey::Float(CANONICAL_NAN_BITS))),

        Value::Float(f) => {
            let canonical = if *f == 0.0 { 0.0f64 } else { *f };
            Ok(Some(GroupByKey::Float(canonical.to_bits())))
        }

        Value::Integer(i) => Ok(Some(GroupByKey::Int(*i))),

        // Normalize so equal values with differing scale (2.50 vs 2.5)
        // produce byte-identical keys.
        Value::Decimal(d) => Ok(Some(GroupByKey::Decimal(d.normalize().serialize()))),

        // Materialize an owned `Box<str>` key from the field-value string;
        // `GroupByKey` keys must own independently of the source `Value`.
        Value::String(s) => Ok(Some(GroupByKey::Str(Box::from(s.as_str())))),
        Value::Bool(b) => Ok(Some(GroupByKey::Bool(*b))),
        Value::Date(d) => Ok(Some(GroupByKey::Date(*d))),
        Value::DateTime(dt) => Ok(Some(GroupByKey::DateTime(*dt))),
        Value::Array(_) => Err(GroupKeyError::UnsupportedType {
            field: field.to_string(),
            type_name: "array",
            row,
        }),
        Value::Map(_) => Err(GroupKeyError::UnsupportedType {
            field: field.to_string(),
            type_name: "map",
            row,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_by_key_decimal_scale_insensitive() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        // 2.50 and 2.5 are the same value: they must produce equal, equally
        // hashing keys (normalized), and round-trip back to an equal Value.
        let k1 = value_to_group_key(&Value::Decimal(Decimal::new(250, 2)), "amt", 0)
            .unwrap()
            .unwrap();
        let k2 = value_to_group_key(&Value::Decimal(Decimal::new(25, 1)), "amt", 0)
            .unwrap()
            .unwrap();
        assert_eq!(k1, k2, "equal decimals of differing scale share one key");

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        k1.hash(&mut h1);
        k2.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());

        assert_eq!(k1.to_value(), Value::Decimal(Decimal::new(25, 1)));

        // A decimal `42` and an integer `42` tie in the value order, so they
        // are one key and hash equally.
        let dec = value_to_group_key(&Value::Decimal(Decimal::new(42, 0)), "amt", 0)
            .unwrap()
            .unwrap();
        let int = value_to_group_key(&Value::Integer(42), "amt", 0)
            .unwrap()
            .unwrap();
        assert_eq!(dec, int);
        let mut hd = DefaultHasher::new();
        let mut hi = DefaultHasher::new();
        dec.hash(&mut hd);
        int.hash(&mut hi);
        assert_eq!(hd.finish(), hi.finish());
    }

    #[test]
    fn test_group_by_key_eq_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = GroupByKey::Str("hello".into());
        let b = GroupByKey::Str("hello".into());
        let c = GroupByKey::Str("world".into());

        assert_eq!(a, b);
        assert_ne!(a, c);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        a.hash(&mut h1);
        b.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_group_by_key_int_float_unify() {
        let int_key = value_to_group_key(&Value::Integer(42), "x", 0)
            .unwrap()
            .unwrap();
        let float_key = value_to_group_key(&Value::Float(42.0), "x", 0)
            .unwrap()
            .unwrap();
        assert_eq!(int_key, float_key);
    }

    #[test]
    fn test_group_by_key_neg_zero_canonical() {
        let pos = value_to_group_key(&Value::Float(0.0), "x", 0)
            .unwrap()
            .unwrap();
        let neg = value_to_group_key(&Value::Float(-0.0), "x", 0)
            .unwrap()
            .unwrap();
        assert_eq!(pos, neg);
    }

    #[test]
    fn test_group_by_key_null_returns_none() {
        let result = value_to_group_key(&Value::Null, "x", 0).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_group_by_key_null_variant() {
        let null_key = GroupByKey::Null;
        let null_key2 = GroupByKey::Null;
        assert_eq!(null_key, null_key2);

        // Null is different from any concrete key
        assert_ne!(GroupByKey::Null, GroupByKey::Str("".into()));
        assert_ne!(GroupByKey::Null, GroupByKey::Int(0));
    }

    #[test]
    fn nan_of_any_sign_is_one_key() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let payload_nan = f64::from_bits(f64::NAN.to_bits() | 0x5);
        let keys: Vec<GroupByKey> = [f64::NAN, -f64::NAN, payload_nan, -payload_nan]
            .into_iter()
            .map(|f| {
                value_to_group_key(&Value::Float(f), "amount", 5)
                    .unwrap()
                    .unwrap()
            })
            .collect();
        let hash = |k: &GroupByKey| {
            let mut h = DefaultHasher::new();
            k.hash(&mut h);
            h.finish()
        };
        let GroupByKey::Float(first_bits) = keys[0] else {
            panic!("a NaN keys as a float, got {:?}", keys[0]);
        };
        for k in &keys {
            assert_eq!(k, &keys[0], "every NaN is one key");
            assert_eq!(hash(k), hash(&keys[0]));
            assert!(
                matches!(k, GroupByKey::Float(bits) if *bits == first_bits),
                "every NaN holds one canonical bit pattern, got {k:?}"
            );
            assert!(matches!(k.to_value(), Value::Float(f) if f.is_nan()));
        }
        assert_ne!(keys[0], GroupByKey::Null, "the NaN key is not the null key");
        let inf = value_to_group_key(&Value::Float(f64::INFINITY), "amount", 5)
            .unwrap()
            .unwrap();
        assert_ne!(keys[0], inf);
    }

    #[test]
    fn large_integers_are_distinct_keys() {
        use std::collections::HashSet;

        let two_pow_53 = 9_007_199_254_740_992_i64;
        let keys: HashSet<GroupByKey> = [two_pow_53, two_pow_53 + 1, i64::MAX, i64::MAX - 1]
            .into_iter()
            .map(|i| {
                value_to_group_key(&Value::Integer(i), "x", 0)
                    .unwrap()
                    .unwrap()
            })
            .collect();
        assert_eq!(
            keys.len(),
            4,
            "integers that share an f64 stay apart: {keys:?}"
        );

        // The float 2^53 ties the integer 2^53 and not its neighbour.
        let float = value_to_group_key(&Value::Float(two_pow_53 as f64), "x", 0)
            .unwrap()
            .unwrap();
        assert_eq!(float, GroupByKey::Int(two_pow_53));
        assert_ne!(float, GroupByKey::Int(two_pow_53 + 1));
        assert_eq!(
            GroupByKey::Int(two_pow_53 + 1).to_value(),
            Value::Integer(two_pow_53 + 1)
        );
    }

    #[test]
    fn encode_tie_bytes_equal_iff_keys_equal() {
        use chrono::NaiveDate;

        fn key(v: Value) -> GroupByKey {
            value_to_group_key(&v, "x", 0)
                .unwrap()
                .unwrap_or(GroupByKey::Null)
        }
        fn bytes(k: &GroupByKey) -> Vec<u8> {
            let mut out = Vec::new();
            k.encode_tie_bytes(&mut out);
            out
        }
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let cases = [
            key(Value::Null),
            key(Value::Integer(0)),
            key(Value::Float(0.0)),
            key(Value::Float(-0.0)),
            key(Value::Decimal(Decimal::new(0, 3))),
            key(Value::Integer(5)),
            key(Value::Float(5.0)),
            key(Value::Decimal(Decimal::new(500, 2))),
            key(Value::Decimal(Decimal::new(55, 1))),
            key(Value::Float(5.5)),
            key(Value::Decimal(Decimal::new(1, 1))),
            key(Value::Float(0.1)),
            key(Value::Integer(9_007_199_254_740_992)),
            key(Value::Integer(9_007_199_254_740_993)),
            key(Value::Float(9_007_199_254_740_992.0)),
            key(Value::Float(f64::NAN)),
            key(Value::Float(-f64::NAN)),
            key(Value::Float(f64::INFINITY)),
            key(Value::String("".into())),
            key(Value::String("5".into())),
            key(Value::Bool(false)),
            key(Value::Bool(true)),
            key(Value::Date(d)),
            key(Value::DateTime(d.and_hms_opt(0, 0, 0).unwrap())),
        ];
        for a in &cases {
            for b in &cases {
                assert_eq!(
                    bytes(a) == bytes(b),
                    a == b,
                    "tie bytes and key equality disagree for {a:?} and {b:?}"
                );
            }
        }
        // The ties and separations the cases exist to cover.
        assert_eq!(cases[1], cases[2]);
        assert_eq!(cases[2], cases[3]);
        assert_eq!(cases[1], cases[4]);
        assert_eq!(cases[5], cases[6]);
        assert_eq!(cases[5], cases[7]);
        assert_eq!(cases[8], cases[9]);
        assert_ne!(cases[10], cases[11]);
        assert_ne!(cases[12], cases[13]);
        assert_eq!(cases[12], cases[14]);
        assert_eq!(cases[15], cases[16]);
        assert_ne!(cases[0], cases[1]);
        assert_ne!(cases[5], cases[19]);
    }

    #[test]
    fn test_group_by_key_array_unsupported() {
        let arr = Value::Array(crate::owned_storage::OwnedValues::from_vec(vec![
            Value::Integer(1),
        ]));
        let result = value_to_group_key(&arr, "tags", 3);
        assert!(result.is_err());
        let GroupKeyError::UnsupportedType {
            field,
            type_name,
            row,
        } = result.unwrap_err();
        assert_eq!(field, "tags");
        assert_eq!(type_name, "array");
        assert_eq!(row, 3);
    }

    #[test]
    fn test_group_by_key_all_types() {
        use chrono::NaiveDate;

        assert!(matches!(
            value_to_group_key(&Value::String("x".into()), "f", 0).unwrap(),
            Some(GroupByKey::Str(_))
        ));
        assert!(matches!(
            value_to_group_key(&Value::Bool(true), "f", 0).unwrap(),
            Some(GroupByKey::Bool(true))
        ));
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert!(matches!(
            value_to_group_key(&Value::Date(d), "f", 0).unwrap(),
            Some(GroupByKey::Date(_))
        ));
        let dt = d.and_hms_opt(10, 0, 0).unwrap();
        assert!(matches!(
            value_to_group_key(&Value::DateTime(dt), "f", 0).unwrap(),
            Some(GroupByKey::DateTime(_))
        ));
    }

    #[test]
    fn test_group_by_key_to_value_all_variants() {
        use chrono::NaiveDate;

        assert_eq!(GroupByKey::Null.to_value(), Value::Null);
        assert_eq!(GroupByKey::Int(42).to_value(), Value::Integer(42));
        assert_eq!(
            GroupByKey::Str("hi".into()).to_value(),
            Value::String("hi".into())
        );
        assert_eq!(GroupByKey::Bool(true).to_value(), Value::Bool(true));

        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(GroupByKey::Date(d).to_value(), Value::Date(d));

        let dt = d.and_hms_opt(10, 0, 0).unwrap();
        assert_eq!(GroupByKey::DateTime(dt).to_value(), Value::DateTime(dt));

        // Float round-trips via f64::from_bits.
        let f = 2.5_f64;
        assert_eq!(GroupByKey::Float(f.to_bits()).to_value(), Value::Float(f));
    }

    #[test]
    fn test_group_by_key_integer_keys_exactly() {
        // An integer keeps its own domain; key equality, not the variant,
        // makes `42` and `42.0` one group.
        let key = value_to_group_key(&Value::Integer(42), "x", 0)
            .unwrap()
            .unwrap();
        assert!(matches!(key, GroupByKey::Int(42)));
    }
}
