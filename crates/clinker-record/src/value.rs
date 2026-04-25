use chrono::{Datelike, NaiveDate, NaiveDateTime};
use indexmap::IndexMap;
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

// ── Serialize (tagged, postcard-compatible) ────────────────────────────────
//
// Wire form: variant index (varint u32 in postcard; values 0-8 fit in 1 byte)
// followed by the variant payload.
//
// Discriminant assignment:
//   0=Null  1=Bool  2=Integer  3=Float  4=String
//   5=Date  6=DateTime  7=Array  8=Map
//
// Date payload: i32 days from proleptic Gregorian ordinal
//   (chrono::Datelike::num_days_from_ce, where 1 = 0001-01-01).
// DateTime payload: i64 nanoseconds since Unix epoch (1970-01-01T00:00:00 UTC).
//   Sub-second resolution is preserved; subsecond = nanos % 1_000_000_000.
//
// Array payload: Vec<Value> (length-prefixed by serde/postcard).
// Map payload:   Vec<(String, Value)> pairs in insertion order.
//
// Both Date and DateTime round-trip exactly through postcard. JSON output
// (via serde_json::to_string) will produce externally-tagged form, e.g.
// {"Integer":42}. Production JSON output uses the `clinker_to_json` helper
// in clinker-format and clinker-core, which bypasses serde Value dispatch.

use serde::de::{self, Deserializer, EnumAccess, VariantAccess, Visitor};
use serde::ser::Serializer;
use serde::{Deserialize, Serialize};

impl Serialize for Value {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        // serialize_newtype_variant: binary formats (postcard) emit VARINT(index) + payload.
        // Text formats (JSON) emit {"VariantName": payload}.
        match self {
            Value::Null => serializer.serialize_unit_variant("Value", 0, "Null"),
            Value::Bool(b) => serializer.serialize_newtype_variant("Value", 1, "Bool", b),
            Value::Integer(n) => serializer.serialize_newtype_variant("Value", 2, "Integer", n),
            Value::Float(f) => serializer.serialize_newtype_variant("Value", 3, "Float", f),
            Value::String(s) => {
                serializer.serialize_newtype_variant("Value", 4, "String", s.as_ref())
            }
            Value::Date(d) => {
                // days from proleptic Gregorian ordinal (1 = 0001-01-01)
                serializer.serialize_newtype_variant("Value", 5, "Date", &d.num_days_from_ce())
            }
            Value::DateTime(dt) => {
                // nanoseconds since Unix epoch
                let nanos = dt.and_utc().timestamp_nanos_opt().unwrap_or(0);
                serializer.serialize_newtype_variant("Value", 6, "DateTime", &nanos)
            }
            Value::Array(arr) => {
                serializer.serialize_newtype_variant("Value", 7, "Array", arr.as_slice())
            }
            Value::Map(m) => {
                // Serialize as vec of (key, value) pairs to preserve insertion order.
                let pairs: Vec<(&str, &Value)> = m.iter().map(|(k, v)| (k.as_ref(), v)).collect();
                serializer.serialize_newtype_variant("Value", 8, "Map", &pairs)
            }
        }
    }
}

// ── Deserialize (tagged, mirrors Serialize) ────────────────────────────────

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        const VARIANTS: &[&str] = &[
            "Null", "Bool", "Integer", "Float", "String", "Date", "DateTime", "Array", "Map",
        ];
        deserializer.deserialize_enum("Value", VARIANTS, ValueVisitor)
    }
}

struct ValueVisitor;

impl<'de> Visitor<'de> for ValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a tagged CXL Value enum")
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<Value, A::Error> {
        let (idx, va): (VariantIdx, _) = data.variant()?;
        match idx.0 {
            0 => {
                va.unit_variant()?;
                Ok(Value::Null)
            }
            1 => Ok(Value::Bool(va.newtype_variant()?)),
            2 => Ok(Value::Integer(va.newtype_variant()?)),
            3 => Ok(Value::Float(va.newtype_variant()?)),
            4 => {
                let s: std::string::String = va.newtype_variant()?;
                Ok(Value::String(s.into_boxed_str()))
            }
            5 => {
                let days: i32 = va.newtype_variant()?;
                NaiveDate::from_num_days_from_ce_opt(days)
                    .map(Value::Date)
                    .ok_or_else(|| de::Error::custom(format!("invalid date ordinal: {days}")))
            }
            6 => {
                let nanos: i64 = va.newtype_variant()?;
                let secs = nanos.div_euclid(1_000_000_000);
                let subsec = nanos.rem_euclid(1_000_000_000) as u32;
                let dt = chrono::DateTime::from_timestamp(secs, subsec)
                    .ok_or_else(|| de::Error::custom(format!("invalid timestamp nanos: {nanos}")))?
                    .naive_utc();
                Ok(Value::DateTime(dt))
            }
            7 => {
                let arr: Vec<Value> = va.newtype_variant()?;
                Ok(Value::Array(arr))
            }
            8 => {
                let pairs: Vec<(std::string::String, Value)> = va.newtype_variant()?;
                let mut m = IndexMap::with_capacity(pairs.len());
                for (k, v) in pairs {
                    m.insert(k.into_boxed_str(), v);
                }
                Ok(Value::Map(Box::new(m)))
            }
            other => Err(de::Error::unknown_variant(
                &other.to_string(),
                &["0", "1", "2", "3", "4", "5", "6", "7", "8"],
            )),
        }
    }
}

// ── VariantIdx: maps variant name or u32 index ────────────────────────────

/// Maps a variant identifier (string name or numeric index) to a `u32`.
///
/// Binary formats (postcard) emit numeric indices; text formats (JSON)
/// emit string names. This wrapper accepts both.
struct VariantIdx(u32);

impl<'de> Deserialize<'de> for VariantIdx {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct VIdxVisitor;
        impl<'de> Visitor<'de> for VIdxVisitor {
            type Value = VariantIdx;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                write!(f, "a Value variant name or index")
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<VariantIdx, E> {
                Ok(VariantIdx(v as u32))
            }
            fn visit_u32<E: de::Error>(self, v: u32) -> Result<VariantIdx, E> {
                Ok(VariantIdx(v))
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<VariantIdx, E> {
                match v {
                    "Null" => Ok(VariantIdx(0)),
                    "Bool" => Ok(VariantIdx(1)),
                    "Integer" => Ok(VariantIdx(2)),
                    "Float" => Ok(VariantIdx(3)),
                    "String" => Ok(VariantIdx(4)),
                    "Date" => Ok(VariantIdx(5)),
                    "DateTime" => Ok(VariantIdx(6)),
                    "Array" => Ok(VariantIdx(7)),
                    "Map" => Ok(VariantIdx(8)),
                    other => Err(de::Error::unknown_variant(
                        other,
                        &[
                            "Null", "Bool", "Integer", "Float", "String", "Date", "DateTime",
                            "Array", "Map",
                        ],
                    )),
                }
            }
        }
        d.deserialize_identifier(VIdxVisitor)
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

/// Shared `Value::Null` sentinel used by `FieldResolver` implementations
/// that need to hand out a `&Value` for a logically-absent field.
///
/// Lets `CombineResolver::resolve_qualified` surface
/// `Some(&NULL)` for build-side lookups under `on_miss: null_fields`
/// without materializing a fresh owned `Value::Null` per call —
/// the trait surface returns borrowed values, and a constructed
/// `Value` has no backing storage to borrow from.
pub static NULL: Value = Value::Null;

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn roundtrip(v: &Value) -> Value {
        let bytes = postcard::to_stdvec(v).expect("postcard serialize failed");
        postcard::from_bytes(&bytes).expect("postcard deserialize failed")
    }

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
    fn test_value_postcard_roundtrip_all_variants() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 15).unwrap();
        let dt = d.and_hms_opt(10, 30, 0).unwrap();
        let cases = vec![
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            Value::Integer(0),
            Value::Integer(i64::MIN),
            Value::Integer(i64::MAX),
            Value::Integer(42),
            Value::Float(0.0),
            Value::Float(3.14),
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(f64::NAN),
            Value::String("".into()),
            Value::String("hello world".into()),
            Value::Date(d),
            Value::Date(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()),
            Value::Date(NaiveDate::from_ymd_opt(2000, 2, 29).unwrap()),
            Value::DateTime(dt),
            Value::Array(vec![]),
            Value::Array(vec![Value::Integer(1), Value::Bool(true)]),
            Value::Array(vec![Value::Null, Value::String("x".into())]),
        ];
        for original in &cases {
            let recovered = roundtrip(original);
            assert_eq!(*original, recovered, "roundtrip failed for: {original:?}");
        }
    }

    #[test]
    fn test_value_postcard_roundtrip_map() {
        let m = Value::map([
            ("a", Value::Integer(1)),
            ("b", Value::String("hello".into())),
            ("c", Value::Bool(true)),
        ]);
        let recovered = roundtrip(&m);
        assert_eq!(m, recovered);
    }

    #[test]
    fn test_value_postcard_roundtrip_nested() {
        let nested = Value::Array(vec![
            Value::map([("x", Value::Integer(10)), ("y", Value::Float(1.5))]),
            Value::Array(vec![Value::Null, Value::Bool(false)]),
        ]);
        let recovered = roundtrip(&nested);
        assert_eq!(nested, recovered);
    }

    #[test]
    fn test_value_postcard_roundtrip_date_boundary() {
        // Dates near the Unix epoch and CE epoch
        let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
        let pre_epoch = NaiveDate::from_ymd_opt(1969, 12, 31).unwrap();
        let far_past = NaiveDate::from_ymd_opt(1, 1, 1).unwrap();
        let far_future = NaiveDate::from_ymd_opt(9999, 12, 31).unwrap();
        for d in [epoch, pre_epoch, far_past, far_future] {
            let v = Value::Date(d);
            assert_eq!(v, roundtrip(&v), "date boundary roundtrip failed: {d}");
        }
    }

    #[test]
    fn test_value_postcard_roundtrip_datetime_boundary() {
        let epoch_dt = NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        let future_dt = NaiveDate::from_ymd_opt(2100, 12, 31)
            .unwrap()
            .and_hms_opt(23, 59, 59)
            .unwrap();
        for dt in [epoch_dt, future_dt] {
            let v = Value::DateTime(dt);
            assert_eq!(v, roundtrip(&v), "datetime boundary roundtrip failed: {dt}");
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

    // --- Value::Map tests ---

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
        let recovered = roundtrip(&m);
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
