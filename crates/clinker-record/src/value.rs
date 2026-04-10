use chrono::{NaiveDate, NaiveDateTime};
use indexmap::IndexMap;
use serde::Serialize;
use serde::ser::{SerializeMap, SerializeSeq};
use std::cmp::Ordering;
use std::fmt;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(Box<str>),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Array(Vec<Value>),
    /// Nested key-value map (ordered, insertion-preserving).
    /// Box<IndexMap> is 8 bytes — enum stays at 24 bytes (Vec<Value> is already 24).
    Map(Box<IndexMap<Box<str>, Value>>),
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Value::Null => serializer.serialize_unit(),
            Value::Bool(b) => serializer.serialize_bool(*b),
            Value::Integer(n) => serializer.serialize_i64(*n),
            Value::Float(f) => serializer.serialize_f64(*f),
            Value::String(s) => serializer.serialize_str(s),
            Value::Date(d) => serializer.serialize_str(&d.format("%Y-%m-%d").to_string()),
            Value::DateTime(dt) => {
                serializer.serialize_str(&dt.format("%Y-%m-%dT%H:%M:%S").to_string())
            }
            Value::Array(arr) => {
                let mut seq = serializer.serialize_seq(Some(arr.len()))?;
                for v in arr {
                    seq.serialize_element(v)?;
                }
                seq.end()
            }
            Value::Map(m) => {
                let mut map = serializer.serialize_map(Some(m.len()))?;
                for (k, v) in m.iter() {
                    map.serialize_entry(k.as_ref(), v)?;
                }
                map.end()
            }
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Date(a), Value::Date(b)) => a == b,
            (Value::DateTime(a), Value::DateTime(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Map(a), Value::Map(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => Some(a.total_cmp(b)),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Date(a), Value::Date(b)) => a.partial_cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "null"),
            Value::Bool(b) => write!(f, "{b}"),
            Value::Integer(n) => write!(f, "{n}"),
            Value::Float(n) => write!(f, "{n}"),
            Value::String(s) => write!(f, "{s}"),
            Value::Date(d) => write!(f, "{}", d.format("%Y-%m-%d")),
            Value::DateTime(dt) => write!(f, "{}", dt.format("%Y-%m-%dT%H:%M:%S")),
            Value::Array(arr) => {
                write!(f, "[")?;
                for (i, v) in arr.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{v}")?;
                }
                write!(f, "]")
            }
            Value::Map(m) => {
                write!(f, "{{")?;
                for (i, (k, v)) in m.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{k}: {v}")?;
                }
                write!(f, "}}")
            }
        }
    }
}

impl Value {
    /// Returns the CXL type name as a static string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Integer(_) => "int",
            Value::Float(_) => "float",
            Value::String(_) => "string",
            Value::Date(_) => "date",
            Value::DateTime(_) => "datetime",
            Value::Array(_) => "array",
            Value::Map(_) => "map",
        }
    }

    /// Returns true if the value is Null.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Estimated heap bytes owned by this value (excludes the enum itself).
    ///
    /// Used by SortBuffer for self-tracking allocation counting.
    /// Scalar variants (Null, Bool, Integer, Float, Date, DateTime) store
    /// data inline in the enum — zero heap allocation.
    pub fn heap_size(&self) -> usize {
        match self {
            Value::String(s) => s.len(),
            Value::Array(arr) => {
                arr.capacity() * std::mem::size_of::<Value>()
                    + arr.iter().map(Value::heap_size).sum::<usize>()
            }
            Value::Map(m) => {
                // IndexMap overhead: each entry is (Box<str>, Value) = key heap + value heap
                m.iter()
                    .map(|(k, v)| k.len() + v.heap_size())
                    .sum::<usize>()
            }
            _ => 0,
        }
    }

    /// Create a Map from an iterator of key-value pairs.
    pub fn map(pairs: impl IntoIterator<Item = (impl Into<Box<str>>, Value)>) -> Self {
        let map: IndexMap<Box<str>, Value> =
            pairs.into_iter().map(|(k, v)| (k.into(), v)).collect();
        Value::Map(Box::new(map))
    }

    /// Create an empty Map.
    pub fn empty_map() -> Self {
        Value::Map(Box::default())
    }

    /// Borrow the inner IndexMap if this is a Map variant.
    pub fn as_map(&self) -> Option<&IndexMap<Box<str>, Value>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Mutably borrow the inner IndexMap if this is a Map variant.
    pub fn as_map_mut(&mut self) -> Option<&mut IndexMap<Box<str>, Value>> {
        match self {
            Value::Map(m) => Some(m),
            _ => None,
        }
    }

    /// Get a field from a Map by name. Returns None if not a Map or field missing.
    pub fn get_field(&self, name: &str) -> Option<&Value> {
        self.as_map().and_then(|m| m.get(name))
    }

    /// Set a field on a Map. No-op if not a Map variant.
    pub fn set_field(&mut self, name: impl Into<Box<str>>, value: Value) {
        if let Value::Map(m) = self {
            m.insert(name.into(), value);
        }
    }
}

use serde::Deserialize;
use serde::de::{self, MapAccess, SeqAccess, Visitor};

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct ValueVisitor;

        impl<'de> Visitor<'de> for ValueVisitor {
            type Value = Value;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a CXL value")
            }

            fn visit_bool<E: de::Error>(self, v: bool) -> Result<Value, E> {
                Ok(Value::Bool(v))
            }

            fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
                Ok(Value::Integer(v))
            }

            fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
                if v <= i64::MAX as u64 {
                    Ok(Value::Integer(v as i64))
                } else {
                    Ok(Value::Float(v as f64))
                }
            }

            fn visit_f64<E: de::Error>(self, v: f64) -> Result<Value, E> {
                Ok(Value::Float(v))
            }

            fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
                // Try DateTime first (more specific), then Date, then String
                if let Ok(dt) = NaiveDateTime::parse_from_str(v, "%Y-%m-%dT%H:%M:%S") {
                    return Ok(Value::DateTime(dt));
                }
                if let Ok(dt) = NaiveDateTime::parse_from_str(v, "%Y-%m-%d %H:%M:%S") {
                    return Ok(Value::DateTime(dt));
                }
                if let Ok(d) = NaiveDate::parse_from_str(v, "%Y-%m-%d") {
                    return Ok(Value::Date(d));
                }
                Ok(Value::String(v.into()))
            }

            fn visit_none<E: de::Error>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }

            fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
                Ok(Value::Null)
            }

            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Value, A::Error> {
                let mut arr = Vec::new();
                while let Some(v) = seq.next_element()? {
                    arr.push(v);
                }
                Ok(Value::Array(arr))
            }

            fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Value, A::Error> {
                let mut m = IndexMap::new();
                while let Some((k, v)) = map.next_entry::<String, Value>()? {
                    m.insert(k.into_boxed_str(), v);
                }
                Ok(Value::Map(Box::new(m)))
            }
        }

        deserializer.deserialize_any(ValueVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_value_display_all_variants() {
        assert_eq!(Value::Null.to_string(), "null");
        assert_eq!(Value::Bool(true).to_string(), "true");
        assert_eq!(Value::Integer(42).to_string(), "42");
        assert_eq!(Value::Float(3.14).to_string(), "3.14");
        assert_eq!(Value::String("hello".into()).to_string(), "hello");
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(Value::Date(d).to_string(), "2024-01-15");
        let dt = d.and_hms_opt(10, 30, 0).unwrap();
        assert_eq!(Value::DateTime(dt).to_string(), "2024-01-15T10:30:00");
        let arr = Value::Array(vec![Value::Integer(1), Value::Integer(2)]);
        assert_eq!(arr.to_string(), "[1, 2]");
    }

    #[test]
    fn test_value_serde_roundtrip() {
        let cases = vec![
            Value::Null,
            Value::Bool(false),
            Value::Integer(42),
            Value::Float(3.14),
            Value::String("test".into()),
            Value::Array(vec![Value::Integer(1), Value::Bool(true)]),
        ];
        for original in cases {
            let json = serde_json::to_string(&original).unwrap();
            let recovered: Value = serde_json::from_str(&json).unwrap();
            assert_eq!(original, recovered);
        }
    }

    #[test]
    fn test_value_partial_ord_same_variant() {
        assert!(Value::Integer(1) < Value::Integer(2));
        assert!(Value::Float(1.0) < Value::Float(2.0));
        assert!(Value::String("a".into()) < Value::String("b".into()));
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();
        assert!(Value::Date(d1) < Value::Date(d2));
    }

    #[test]
    fn test_value_partial_ord_cross_variant() {
        assert_eq!(
            Value::Integer(1).partial_cmp(&Value::String("1".into())),
            None
        );
        assert_eq!(Value::Bool(true).partial_cmp(&Value::Integer(1)), None);
        assert_eq!(Value::Null.partial_cmp(&Value::Integer(0)), None);
    }

    #[test]
    fn test_value_float_nan_ordering() {
        let nan = Value::Float(f64::NAN);
        let inf = Value::Float(f64::INFINITY);
        let normal = Value::Float(1.0);
        assert!(nan > normal);
        assert!(nan > inf);
    }

    #[test]
    fn test_value_float_partial_eq_nan() {
        let nan1 = Value::Float(f64::NAN);
        let nan2 = Value::Float(f64::NAN);
        assert_eq!(nan1, nan2);

        let neg_zero = Value::Float(-0.0_f64);
        let pos_zero = Value::Float(0.0_f64);
        assert_ne!(neg_zero, pos_zero);
    }

    #[test]
    fn test_value_serde_roundtrip_with_dates() {
        let date = Value::Date(NaiveDate::from_ymd_opt(2024, 1, 15).unwrap());
        let json = serde_json::to_string(&date).unwrap();
        assert_eq!(json, r#""2024-01-15""#);
        let recovered: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, date);

        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let dt = Value::DateTime(d.and_hms_opt(10, 30, 0).unwrap());
        let json = serde_json::to_string(&dt).unwrap();
        let recovered: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(recovered, dt);
    }

    #[test]
    fn test_value_serde_plain_string_not_date() {
        let json = r#""hello""#;
        let v: Value = serde_json::from_str(json).unwrap();
        assert!(matches!(v, Value::String(_)));
    }

    #[test]
    fn test_value_serde_integer_not_float() {
        let v: Value = serde_json::from_str("42").unwrap();
        assert!(matches!(v, Value::Integer(42)));
        let v: Value = serde_json::from_str("42.0").unwrap();
        assert!(matches!(v, Value::Float(_)));
    }

    #[test]
    fn test_value_heap_size_scalar() {
        assert_eq!(Value::Null.heap_size(), 0);
        assert_eq!(Value::Bool(true).heap_size(), 0);
        assert_eq!(Value::Integer(42).heap_size(), 0);
        assert_eq!(Value::Float(3.14).heap_size(), 0);
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        assert_eq!(Value::Date(d).heap_size(), 0);
        assert_eq!(
            Value::DateTime(d.and_hms_opt(10, 30, 0).unwrap()).heap_size(),
            0
        );
    }

    #[test]
    fn test_value_heap_size_string() {
        assert_eq!(Value::String("hello".into()).heap_size(), 5);
        assert_eq!(Value::String("".into()).heap_size(), 0);
        assert_eq!(
            Value::String("a longer string value".into()).heap_size(),
            21
        );
    }

    #[test]
    fn test_value_heap_size_array() {
        let arr = Value::Array(vec![Value::Integer(1), Value::String("ab".into())]);
        let expected = 2 * std::mem::size_of::<Value>() // Vec backing (capacity=2)
            + 0   // Integer heap = 0
            + 2; // String "ab" heap = 2
        assert_eq!(arr.heap_size(), expected);
    }

    #[test]
    fn test_value_type_name() {
        assert_eq!(Value::Null.type_name(), "null");
        assert_eq!(Value::Bool(true).type_name(), "bool");
        assert_eq!(Value::Integer(1).type_name(), "int");
        assert_eq!(Value::Float(1.0).type_name(), "float");
        assert_eq!(Value::String("x".into()).type_name(), "string");
        assert_eq!(Value::empty_map().type_name(), "map");
    }

    // --- Value::Map tests (Phase 13, Task 13.1) ---

    #[test]
    fn test_map_construction() {
        let m = Value::map([("a", Value::Integer(1)), ("b", Value::Integer(2))]);
        let inner = m.as_map().unwrap();
        assert_eq!(inner.len(), 2);
        assert_eq!(inner.get("a"), Some(&Value::Integer(1)));
        assert_eq!(inner.get("b"), Some(&Value::Integer(2)));
    }

    #[test]
    fn test_empty_map() {
        let m = Value::empty_map();
        let inner = m.as_map().unwrap();
        assert_eq!(inner.len(), 0);
    }

    #[test]
    fn test_map_field_access() {
        let m = Value::map([("a", Value::Integer(1)), ("b", Value::Integer(2))]);
        assert_eq!(m.get_field("a"), Some(&Value::Integer(1)));
        assert_eq!(m.get_field("missing"), None);
    }

    #[test]
    fn test_map_set_field() {
        let mut m = Value::map([("a", Value::Integer(1))]);
        m.set_field("c", Value::Integer(3));
        assert_eq!(m.get_field("c"), Some(&Value::Integer(3)));
        // Overwrite existing
        m.set_field("a", Value::Integer(99));
        assert_eq!(m.get_field("a"), Some(&Value::Integer(99)));
    }

    #[test]
    fn test_map_as_map() {
        let m = Value::empty_map();
        assert!(m.as_map().is_some());
        assert!(Value::Integer(1).as_map().is_none());
        assert!(Value::Null.as_map().is_none());
    }

    #[test]
    fn test_map_as_map_mut() {
        let mut m = Value::empty_map();
        assert!(m.as_map_mut().is_some());
        let mut i = Value::Integer(1);
        assert!(i.as_map_mut().is_none());
    }

    #[test]
    fn test_map_display() {
        let m = Value::map([
            ("key1", Value::Integer(1)),
            ("key2", Value::String("val".into())),
        ]);
        assert_eq!(m.to_string(), "{key1: 1, key2: val}");
        assert_eq!(Value::empty_map().to_string(), "{}");
    }

    #[test]
    fn test_map_serialize_roundtrip() {
        let m = Value::map([
            ("a", Value::Integer(1)),
            ("b", Value::String("hello".into())),
            ("c", Value::Bool(true)),
        ]);
        let json = serde_json::to_string(&m).unwrap();
        let recovered: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(m, recovered);
    }

    #[test]
    fn test_map_partial_eq() {
        let m1 = Value::map([("a", Value::Integer(1)), ("b", Value::Integer(2))]);
        let m2 = Value::map([("a", Value::Integer(1)), ("b", Value::Integer(2))]);
        assert_eq!(m1, m2);

        let m3 = Value::map([("a", Value::Integer(1)), ("b", Value::Integer(3))]);
        assert_ne!(m1, m3);

        // Map != non-Map
        assert_ne!(m1, Value::Null);
    }

    #[test]
    fn test_map_clone() {
        let m1 = Value::map([("a", Value::Integer(1))]);
        let mut m2 = m1.clone();
        assert_eq!(m1, m2);
        // Modifying clone doesn't affect original
        m2.set_field("a", Value::Integer(99));
        assert_eq!(m1.get_field("a"), Some(&Value::Integer(1)));
        assert_eq!(m2.get_field("a"), Some(&Value::Integer(99)));
    }

    #[test]
    fn test_nested_map_in_array() {
        let nested = Value::Array(vec![
            Value::map([("x", Value::Integer(10))]),
            Value::map([("y", Value::Integer(20))]),
        ]);
        if let Value::Array(arr) = &nested {
            assert_eq!(arr[0].get_field("x"), Some(&Value::Integer(10)));
            assert_eq!(arr[1].get_field("y"), Some(&Value::Integer(20)));
        } else {
            panic!("expected Array");
        }
    }

    #[test]
    fn test_value_enum_size() {
        assert_eq!(std::mem::size_of::<Value>(), 24);
    }

    #[test]
    fn test_map_duplicate_key_last_wins() {
        let m = Value::map([("a", Value::Integer(1)), ("a", Value::Integer(2))]);
        let inner = m.as_map().unwrap();
        assert_eq!(inner.len(), 1);
        assert_eq!(inner.get("a"), Some(&Value::Integer(2)));
    }
}
