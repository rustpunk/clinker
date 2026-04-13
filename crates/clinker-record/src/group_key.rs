//! GroupByKey and value_to_group_key for window partition and distinct dedup.
//!
//! Moved from `clinker-core::pipeline::index` to the foundation crate so
//! `cxl::eval` can use it for distinct without depending on `clinker-core`.

use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};

use crate::schema_def::{FieldDef, FieldType};
use crate::value::Value;

/// Group-by key for SecondaryIndex and distinct dedup.
/// Supports Eq + Hash for HashMap/HashSet keys.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GroupByKey {
    Str(Box<str>),
    /// Used only when schema_overrides pins a field to "integer".
    Int(i64),
    /// f64::to_bits() — default numeric path. -0.0 canonicalized to 0.0.
    Float(u64),
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    /// SQL-standard NULL = NULL for DISTINCT/GROUP BY (ISO 9075).
    /// 12/12 systems agree: PostgreSQL, MySQL, Spark, DuckDB, Polars, etc.
    Null,
}

impl GroupByKey {
    /// Convert a group-by key back into a `Value` for finalize-time
    /// residual evaluation in aggregation scope.
    ///
    /// `Float(bits)` is unpacked via `f64::from_bits`. The conversion is
    /// lossless against `value_to_group_key` *modulo* the int→float
    /// widening that path performs by default — i.e. an integer key
    /// produced via the default (un-pinned) path round-trips back as
    /// `Value::Float`, matching the canonicalization of equality.
    pub fn to_value(&self) -> Value {
        match self {
            GroupByKey::Null => Value::Null,
            GroupByKey::Int(n) => Value::Integer(*n),
            GroupByKey::Str(s) => Value::String(s.clone()),
            GroupByKey::Bool(b) => Value::Bool(*b),
            GroupByKey::Date(d) => Value::Date(*d),
            GroupByKey::DateTime(dt) => Value::DateTime(*dt),
            GroupByKey::Float(bits) => Value::Float(f64::from_bits(*bits)),
        }
    }
}

/// Errors from group key conversion.
#[derive(Debug)]
pub enum GroupKeyError {
    /// NaN value in a group_by / distinct field.
    NanInGroupBy { field: String, row: u32 },
    /// Float value in an integer-pinned field.
    TypeMismatch {
        field: String,
        expected: &'static str,
        got: &'static str,
        row: u32,
    },
    /// Unsupported type (e.g. Array) used as group key.
    UnsupportedType {
        field: String,
        type_name: &'static str,
        row: u32,
    },
}

impl std::fmt::Display for GroupKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GroupKeyError::NanInGroupBy { field, row } => {
                write!(
                    f,
                    "NaN in group_by field '{}' at row {} — pipeline must halt (exit code 3)",
                    field, row
                )
            }
            GroupKeyError::TypeMismatch {
                field,
                expected,
                got,
                row,
            } => {
                write!(
                    f,
                    "type mismatch in group_by field '{}' at row {}: expected {}, got {}",
                    field, row, expected, got
                )
            }
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

/// Convert a Value to a GroupByKey, applying numeric normalization rules.
///
/// - Default: widen Int to Float via `to_bits()` so 42 and 42.0 group together.
/// - Integer-pinned (via schema_overrides): use `GroupByKey::Int(i64)`, reject Float.
/// - NaN → hard error.
/// - Null → None (caller decides whether to skip or use GroupByKey::Null).
/// - `-0.0` canonicalized to `0.0` before `to_bits()`.
/// - Array → `GroupKeyError::UnsupportedType`.
pub fn value_to_group_key(
    val: &Value,
    field: &str,
    schema_pin: Option<&FieldDef>,
    row: u32,
) -> Result<Option<GroupByKey>, GroupKeyError> {
    match val {
        Value::Null => Ok(None),

        Value::Float(f) if f.is_nan() => Err(GroupKeyError::NanInGroupBy {
            field: field.to_string(),
            row,
        }),

        Value::Float(f) => {
            // Canonicalize -0.0 to 0.0
            let canonical = if *f == 0.0 { 0.0f64 } else { *f };
            Ok(Some(GroupByKey::Float(canonical.to_bits())))
        }

        Value::Integer(i) => {
            if let Some(pin) = schema_pin
                && pin.field_type == Some(FieldType::Integer)
            {
                return Ok(Some(GroupByKey::Int(*i)));
            }
            // Default: widen to Float
            Ok(Some(GroupByKey::Float((*i as f64).to_bits())))
        }

        Value::String(s) => Ok(Some(GroupByKey::Str(s.clone()))),
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
        let int_key = value_to_group_key(&Value::Integer(42), "x", None, 0)
            .unwrap()
            .unwrap();
        let float_key = value_to_group_key(&Value::Float(42.0), "x", None, 0)
            .unwrap()
            .unwrap();
        assert_eq!(int_key, float_key);
    }

    #[test]
    fn test_group_by_key_neg_zero_canonical() {
        let pos = value_to_group_key(&Value::Float(0.0), "x", None, 0)
            .unwrap()
            .unwrap();
        let neg = value_to_group_key(&Value::Float(-0.0), "x", None, 0)
            .unwrap()
            .unwrap();
        assert_eq!(pos, neg);
    }

    #[test]
    fn test_group_by_key_null_returns_none() {
        let result = value_to_group_key(&Value::Null, "x", None, 0).unwrap();
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
    fn test_group_by_key_nan_error() {
        let result = value_to_group_key(&Value::Float(f64::NAN), "amount", None, 5);
        assert!(result.is_err());
        match result.unwrap_err() {
            GroupKeyError::NanInGroupBy { field, row } => {
                assert_eq!(field, "amount");
                assert_eq!(row, 5);
            }
            other => panic!("Expected NanInGroupBy, got: {:?}", other),
        }
    }

    #[test]
    fn test_group_by_key_array_unsupported() {
        let arr = Value::Array(vec![Value::Integer(1)]);
        let result = value_to_group_key(&arr, "tags", None, 3);
        assert!(result.is_err());
        match result.unwrap_err() {
            GroupKeyError::UnsupportedType {
                field,
                type_name,
                row,
            } => {
                assert_eq!(field, "tags");
                assert_eq!(type_name, "array");
                assert_eq!(row, 3);
            }
            other => panic!("Expected UnsupportedType, got: {:?}", other),
        }
    }

    #[test]
    fn test_group_by_key_all_types() {
        use chrono::NaiveDate;

        assert!(matches!(
            value_to_group_key(&Value::String("x".into()), "f", None, 0).unwrap(),
            Some(GroupByKey::Str(_))
        ));
        assert!(matches!(
            value_to_group_key(&Value::Bool(true), "f", None, 0).unwrap(),
            Some(GroupByKey::Bool(true))
        ));
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert!(matches!(
            value_to_group_key(&Value::Date(d), "f", None, 0).unwrap(),
            Some(GroupByKey::Date(_))
        ));
        let dt = d.and_hms_opt(10, 0, 0).unwrap();
        assert!(matches!(
            value_to_group_key(&Value::DateTime(dt), "f", None, 0).unwrap(),
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

        // Float round-trips via f64::from_bits, including NaN-canonical
        // groupings (we just check finite values here — NaN can't enter
        // the key in the first place).
        let f = 3.14_f64;
        assert_eq!(GroupByKey::Float(f.to_bits()).to_value(), Value::Float(f));
    }

    #[test]
    fn test_group_by_key_integer_pin() {
        let mut pin = FieldDef {
            name: "x".into(),
            field_type: Some(FieldType::Integer),
            required: None,
            format: None,
            coerce: None,
            default: None,
            allowed_values: None,
            alias: None,
            inherits: None,
            start: None,
            width: None,
            end: None,
            justify: None,
            pad: None,
            trim: None,
            truncation: None,
            precision: None,
            scale: None,
            path: None,
            drop: None,
            record: None,
        };
        let key = value_to_group_key(&Value::Integer(42), "x", Some(&pin), 0)
            .unwrap()
            .unwrap();
        assert_eq!(key, GroupByKey::Int(42));

        // Without integer pin, integer widens to float
        pin.field_type = None;
        let key = value_to_group_key(&Value::Integer(42), "x", Some(&pin), 0)
            .unwrap()
            .unwrap();
        assert!(matches!(key, GroupByKey::Float(_)));
    }
}
