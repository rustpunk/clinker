//! Compact sorted canonical JSON v1.
//!
//! This intentionally implements the release byte contract, not RFC 8785.

use std::collections::BTreeMap;
use std::fmt;

use serde::de::{Deserialize, Deserializer, Error as DeError, MapAccess, SeqAccess, Visitor};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};

use crate::error::GateError;
use crate::limits::{MAX_INPUT_BYTES, MAX_JSON_DEPTH, MAX_JSON_NODES};

/// Integer-only JSON number representation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Integer {
    /// Signed JSON integer.
    Signed(i64),
    /// Unsigned JSON integer above the signed range.
    Unsigned(u64),
}

/// Validated canonical JSON value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CanonicalValue {
    /// JSON null.
    Null,
    /// JSON boolean.
    Bool(bool),
    /// JSON integer; floats are unrepresentable.
    Integer(Integer),
    /// JSON string.
    String(String),
    /// Ordered JSON array.
    Array(Vec<Self>),
    /// Recursively key-sorted JSON object.
    Object(BTreeMap<String, Self>),
}

impl<'de> Deserialize<'de> for CanonicalValue {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(CanonicalVisitor)
    }
}

struct CanonicalVisitor;

impl<'de> Visitor<'de> for CanonicalVisitor {
    type Value = CanonicalValue;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("integer-only JSON without duplicate object keys")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(CanonicalValue::Null)
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(CanonicalValue::Null)
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E> {
        Ok(CanonicalValue::Bool(value))
    }

    fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
        Ok(CanonicalValue::Integer(Integer::Signed(value)))
    }

    fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
        Ok(CanonicalValue::Integer(Integer::Unsigned(value)))
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Err(E::custom("floating-point numbers are forbidden"))
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: DeError,
    {
        Ok(CanonicalValue::String(value.to_owned()))
    }

    fn visit_string<E>(self, value: String) -> Result<Self::Value, E> {
        Ok(CanonicalValue::String(value))
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut values = Vec::with_capacity(sequence.size_hint().unwrap_or(0));
        while let Some(value) = sequence.next_element()? {
            values.push(value);
        }
        Ok(CanonicalValue::Array(values))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut values = BTreeMap::new();
        while let Some((key, value)) = map.next_entry::<String, CanonicalValue>()? {
            if values.insert(key.clone(), value).is_some() {
                return Err(A::Error::custom(format!("duplicate JSON key: {key}")));
            }
        }
        Ok(CanonicalValue::Object(values))
    }
}

impl Serialize for CanonicalValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::Null => serializer.serialize_unit(),
            Self::Bool(value) => serializer.serialize_bool(*value),
            Self::Integer(Integer::Signed(value)) => serializer.serialize_i64(*value),
            Self::Integer(Integer::Unsigned(value)) => serializer.serialize_u64(*value),
            Self::String(value) => serializer.serialize_str(value),
            Self::Array(values) => {
                let mut sequence = serializer.serialize_seq(Some(values.len()))?;
                for value in values {
                    sequence.serialize_element(value)?;
                }
                sequence.end()
            }
            Self::Object(values) => {
                let mut map = serializer.serialize_map(Some(values.len()))?;
                for (key, value) in values {
                    map.serialize_entry(key, value)?;
                }
                map.end()
            }
        }
    }
}

impl CanonicalValue {
    /// Return this value as an object.
    #[must_use]
    pub const fn as_object(&self) -> Option<&BTreeMap<String, Self>> {
        match self {
            Self::Object(value) => Some(value),
            _ => None,
        }
    }

    /// Return this value as an array.
    #[must_use]
    pub fn as_array(&self) -> Option<&[Self]> {
        match self {
            Self::Array(value) => Some(value),
            _ => None,
        }
    }

    /// Return this value as a string.
    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    /// Return this value as a boolean.
    #[must_use]
    pub const fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    /// Return this value as an unsigned integer when non-negative.
    #[must_use]
    pub const fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Integer(Integer::Unsigned(value)) => Some(*value),
            Self::Integer(Integer::Signed(value)) if *value >= 0 => Some(*value as u64),
            _ => None,
        }
    }
}

/// Parse and validate canonical-v1-compatible JSON.
pub fn parse_json(input: &[u8]) -> Result<CanonicalValue, GateError> {
    parse_json_with_limit(input, MAX_INPUT_BYTES)
}

/// Parse canonical-v1-compatible JSON through an explicit byte ceiling.
pub fn parse_json_with_limit(input: &[u8], maximum: usize) -> Result<CanonicalValue, GateError> {
    if input.len() > maximum {
        return Err(GateError::policy(
            "json.too_large",
            format!("JSON exceeds the {maximum}-byte limit"),
        ));
    }
    let mut deserializer = serde_json::Deserializer::from_slice(input);
    let value = CanonicalValue::deserialize(&mut deserializer).map_err(|error| {
        GateError::policy(
            "json.invalid",
            format!(
                "invalid canonical JSON at line {}, column {}",
                error.line(),
                error.column()
            ),
        )
    })?;
    deserializer.end().map_err(|error| {
        GateError::policy(
            "json.trailing_data",
            format!(
                "trailing JSON data at line {}, column {}",
                error.line(),
                error.column()
            ),
        )
    })?;
    validate_shape(&value)?;
    Ok(value)
}

/// Serialize the exact sorted, compact canonical v1 bytes.
pub fn to_bytes(value: &CanonicalValue) -> Result<Vec<u8>, GateError> {
    validate_shape(value)?;
    serde_json::to_vec(value)
        .map_err(|_| GateError::internal("json.serialize", "canonical JSON serialization failed"))
}

fn validate_shape(value: &CanonicalValue) -> Result<(), GateError> {
    let mut stack = Vec::with_capacity(MAX_JSON_DEPTH);
    stack.push((value, 1_usize));
    let mut nodes = 0_usize;
    while let Some((current, depth)) = stack.pop() {
        nodes = nodes.saturating_add(1);
        if nodes > MAX_JSON_NODES {
            return Err(GateError::policy(
                "json.too_many_nodes",
                format!("JSON exceeds the {MAX_JSON_NODES}-node limit"),
            ));
        }
        if depth > MAX_JSON_DEPTH {
            return Err(GateError::policy(
                "json.too_deep",
                format!("JSON exceeds the {MAX_JSON_DEPTH}-level depth limit"),
            ));
        }
        match current {
            CanonicalValue::Array(values) => {
                stack.extend(values.iter().map(|value| (value, depth + 1)));
            }
            CanonicalValue::Object(values) => {
                stack.extend(values.values().map(|value| (value, depth + 1)));
            }
            CanonicalValue::Null
            | CanonicalValue::Bool(_)
            | CanonicalValue::Integer(_)
            | CanonicalValue::String(_) => {}
        }
    }
    Ok(())
}
