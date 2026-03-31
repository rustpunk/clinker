use crate::value::Value;
use chrono::{NaiveDate, NaiveDateTime};
use std::fmt;

#[derive(Debug, Clone)]
pub enum CoercionError {
    TypeMismatch {
        from: &'static str,
        to: &'static str,
        value: String,
    },
    ParseFailure {
        input: String,
        target: &'static str,
    },
}

impl fmt::Display for CoercionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CoercionError::TypeMismatch { from, to, value } => {
                write!(f, "cannot coerce {from} to {to}: {value}")
            }
            CoercionError::ParseFailure { input, target } => {
                write!(f, "failed to parse '{input}' as {target}")
            }
        }
    }
}

impl std::error::Error for CoercionError {}

/// Default date format chain — US-oriented.
pub const DEFAULT_DATE_FORMATS: &[&str] = &[
    "%Y-%m-%d", // ISO 8601: 2024-01-15
    "%m/%d/%Y", // US slash: 01/15/2024
    "%Y/%m/%d", // Alt ISO: 2024/01/15
    "%m-%d-%Y", // US dash: 01-15-2024
];

/// Default datetime format chain — US-oriented.
pub const DEFAULT_DATETIME_FORMATS: &[&str] = &[
    "%Y-%m-%dT%H:%M:%S", // ISO 8601: 2024-01-15T10:30:00
    "%Y-%m-%d %H:%M:%S", // Space sep: 2024-01-15 10:30:00
    "%m/%d/%Y %H:%M:%S", // US: 01/15/2024 10:30:00
    "%m/%d/%Y %H:%M",    // US no sec: 01/15/2024 10:30
];

/// Strict coercion: returns Err on failure. Null propagates as Ok(Null).
pub fn coerce_to_int(value: &Value) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Integer(_) => Ok(value.clone()),
        Value::Float(f) => Ok(Value::Integer(*f as i64)),
        Value::Bool(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
        Value::String(s) => {
            s.parse::<i64>()
                .map(Value::Integer)
                .map_err(|_| CoercionError::ParseFailure {
                    input: s.to_string(),
                    target: "Integer",
                })
        }
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "Integer",
            value: other.to_string(),
        }),
    }
}

/// Lenient coercion: returns None on failure. Null propagates as Some(Null).
pub fn coerce_to_int_lenient(value: &Value) -> Option<Value> {
    coerce_to_int(value).ok()
}

pub fn coerce_to_float(value: &Value) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Float(_) => Ok(value.clone()),
        Value::Integer(n) => Ok(Value::Float(*n as f64)),
        Value::Bool(b) => Ok(Value::Float(if *b { 1.0 } else { 0.0 })),
        Value::String(s) => {
            s.parse::<f64>()
                .map(Value::Float)
                .map_err(|_| CoercionError::ParseFailure {
                    input: s.to_string(),
                    target: "Float",
                })
        }
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "Float",
            value: other.to_string(),
        }),
    }
}

pub fn coerce_to_float_lenient(value: &Value) -> Option<Value> {
    coerce_to_float(value).ok()
}

pub fn coerce_to_string(value: &Value) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        other => Ok(Value::String(other.to_string().into_boxed_str())),
    }
}

pub fn coerce_to_string_lenient(value: &Value) -> Option<Value> {
    coerce_to_string(value).ok()
}

pub fn coerce_to_bool(value: &Value) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Bool(_) => Ok(value.clone()),
        Value::Integer(n) => Ok(Value::Bool(*n != 0)),
        Value::String(s) => match s.to_ascii_lowercase().as_str() {
            "true" | "yes" | "1" | "on" => Ok(Value::Bool(true)),
            "false" | "no" | "0" | "off" => Ok(Value::Bool(false)),
            _ => Err(CoercionError::ParseFailure {
                input: s.to_string(),
                target: "Bool",
            }),
        },
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "Bool",
            value: other.to_string(),
        }),
    }
}

pub fn coerce_to_bool_lenient(value: &Value) -> Option<Value> {
    coerce_to_bool(value).ok()
}

/// Strict date coercion with configurable format chain.
/// Pass &[] to use DEFAULT_DATE_FORMATS.
pub fn coerce_to_date(value: &Value, formats: &[&str]) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Date(_) => Ok(value.clone()),
        Value::DateTime(dt) => Ok(Value::Date(dt.date())),
        Value::String(s) => {
            let chain = if formats.is_empty() {
                DEFAULT_DATE_FORMATS
            } else {
                formats
            };
            for fmt in chain {
                if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
                    return Ok(Value::Date(d));
                }
            }
            Err(CoercionError::ParseFailure {
                input: s.to_string(),
                target: "Date",
            })
        }
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "Date",
            value: other.to_string(),
        }),
    }
}

pub fn coerce_to_date_lenient(value: &Value, formats: &[&str]) -> Option<Value> {
    coerce_to_date(value, formats).ok()
}

/// Strict datetime coercion with configurable format chain.
/// Date values promoted to DateTime at midnight.
pub fn coerce_to_datetime(value: &Value, formats: &[&str]) -> Result<Value, CoercionError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::DateTime(_) => Ok(value.clone()),
        Value::Date(d) => Ok(Value::DateTime(d.and_hms_opt(0, 0, 0).unwrap())),
        Value::String(s) => {
            let chain = if formats.is_empty() {
                DEFAULT_DATETIME_FORMATS
            } else {
                formats
            };
            for fmt in chain {
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                    return Ok(Value::DateTime(dt));
                }
            }
            Err(CoercionError::ParseFailure {
                input: s.to_string(),
                target: "DateTime",
            })
        }
        other => Err(CoercionError::TypeMismatch {
            from: other.type_name(),
            to: "DateTime",
            value: other.to_string(),
        }),
    }
}

pub fn coerce_to_datetime_lenient(value: &Value, formats: &[&str]) -> Option<Value> {
    coerce_to_datetime(value, formats).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_coerce_string_to_int_valid() {
        let v = Value::String("42".into());
        let result = coerce_to_int(&v).unwrap();
        assert_eq!(result, Value::Integer(42));
    }

    #[test]
    fn test_coerce_string_to_int_invalid() {
        let v = Value::String("abc".into());
        assert!(coerce_to_int(&v).is_err());
        assert!(coerce_to_int_lenient(&v).is_none());
    }

    #[test]
    fn test_coerce_null_propagation() {
        assert_eq!(coerce_to_int(&Value::Null).unwrap(), Value::Null);
        assert_eq!(coerce_to_float(&Value::Null).unwrap(), Value::Null);
        assert_eq!(coerce_to_string(&Value::Null).unwrap(), Value::Null);
        assert_eq!(coerce_to_bool(&Value::Null).unwrap(), Value::Null);
        assert_eq!(coerce_to_date(&Value::Null, &[]).unwrap(), Value::Null);
        assert_eq!(coerce_to_datetime(&Value::Null, &[]).unwrap(), Value::Null);
    }

    #[test]
    fn test_coerce_string_to_bool() {
        for s in ["true", "yes", "1", "on"] {
            let v = Value::String(s.into());
            assert_eq!(coerce_to_bool(&v).unwrap(), Value::Bool(true));
        }
        for s in ["false", "no", "0", "off"] {
            let v = Value::String(s.into());
            assert_eq!(coerce_to_bool(&v).unwrap(), Value::Bool(false));
        }
        let v = Value::String("maybe".into());
        assert!(coerce_to_bool(&v).is_err());
    }

    #[test]
    fn test_coerce_string_to_date() {
        let v = Value::String("2024-01-15".into());
        let expected = Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        assert_eq!(coerce_to_date(&v, &[]).unwrap(), expected);

        // US format
        let v = Value::String("01/15/2024".into());
        assert_eq!(coerce_to_date(&v, &[]).unwrap(), expected);

        // Explicit format
        let v = Value::String("15-01-2024".into());
        assert_eq!(coerce_to_date(&v, &["%d-%m-%Y"]).unwrap(), expected);
    }

    #[test]
    fn test_coerce_string_to_datetime() {
        let v = Value::String("2024-01-15T10:30:00".into());
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let expected = Value::DateTime(d.and_hms_opt(10, 30, 0).unwrap());
        assert_eq!(coerce_to_datetime(&v, &[]).unwrap(), expected);

        // US datetime format
        let v = Value::String("01/15/2024 10:30:00".into());
        assert_eq!(coerce_to_datetime(&v, &[]).unwrap(), expected);
    }

    #[test]
    fn test_coerce_float_to_int_truncation() {
        assert_eq!(
            coerce_to_int(&Value::Float(1.9)).unwrap(),
            Value::Integer(1)
        );
        assert_eq!(
            coerce_to_int(&Value::Float(-1.9)).unwrap(),
            Value::Integer(-1)
        );
        assert_eq!(
            coerce_to_int(&Value::Float(0.5)).unwrap(),
            Value::Integer(0)
        );
    }

    #[test]
    fn test_coerce_empty_string() {
        assert!(coerce_to_int(&Value::String("".into())).is_err());
        assert_eq!(coerce_to_int_lenient(&Value::String("".into())), None);
        assert!(coerce_to_bool(&Value::String("".into())).is_err());
        assert!(coerce_to_date(&Value::String("".into()), &[]).is_err());
    }

    #[test]
    fn test_coerce_string_to_int_overflow() {
        let v = Value::String("99999999999999999999".into());
        assert!(coerce_to_int(&v).is_err());
        assert!(coerce_to_int_lenient(&v).is_none());
    }

    #[test]
    fn test_coerce_string_to_int_float_string() {
        assert!(coerce_to_int(&Value::String("1.5".into())).is_err());
    }
}
