//! Canonical escaping and depth limits for nested neutral values.

use std::borrow::Cow;

use crate::Value;

/// Maximum number of nested array/map containers accepted by CXL and native
/// recursive writers.
pub const MAX_NESTED_VALUE_DEPTH: usize = 64;

/// A decoded neutral map key. `escaped` distinguishes a literal key from an
/// unescaped spelling that a format may assign a structural role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NestedKey<'a> {
    pub text: Cow<'a, str>,
    pub escaped: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NestedKeyError {
    NonCanonicalEscape { key: String },
    DuplicateLogicalKey { key: String },
    DepthExceeded { depth: usize, limit: usize },
}

impl std::fmt::Display for NestedKeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NonCanonicalEscape { key } => write!(
                f,
                "non-canonical nested key escape in {key:?}; only `\\@...`, `\\#text`, and `\\\\...` are valid escaped forms"
            ),
            Self::DuplicateLogicalKey { key } => {
                write!(f, "duplicate logical nested key {key:?}")
            }
            Self::DepthExceeded { depth, limit } => {
                write!(
                    f,
                    "nested value depth {depth} exceeds the maximum of {limit}"
                )
            }
        }
    }
}

impl std::error::Error for NestedKeyError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NestedDepthError {
    pub depth: usize,
    pub limit: usize,
}

impl std::fmt::Display for NestedDepthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "nested value depth {} exceeds the maximum of {}",
            self.depth, self.limit
        )
    }
}

impl std::error::Error for NestedDepthError {}

impl<'a> NestedKey<'a> {
    /// Decode one canonical key spelling without interpreting format roles.
    pub fn decode(key: &'a str) -> Result<Self, NestedKeyError> {
        let Some(rest) = key.strip_prefix('\\') else {
            return Ok(Self {
                text: Cow::Borrowed(key),
                escaped: false,
            });
        };
        if rest.starts_with('@') || rest == "#text" || rest.starts_with('\\') {
            Ok(Self {
                text: Cow::Borrowed(rest),
                escaped: true,
            })
        } else {
            Err(NestedKeyError::NonCanonicalEscape {
                key: key.to_string(),
            })
        }
    }

    /// Return the unique canonical spelling for a literal logical key.
    pub fn encode_literal(key: &'a str) -> Cow<'a, str> {
        if key.starts_with('@') || key == "#text" || key.starts_with('\\') {
            Cow::Owned(format!("\\{key}"))
        } else {
            Cow::Borrowed(key)
        }
    }
}

/// Validate the shared nested depth cap iteratively. Scalars have depth zero;
/// each array or map container adds one level.
pub fn validate_nested_depth(value: &Value) -> Result<(), NestedDepthError> {
    fn visit(value: &Value, depth: usize) -> Result<(), NestedDepthError> {
        match value {
            Value::Array(values) => {
                let next = depth + 1;
                if next > MAX_NESTED_VALUE_DEPTH {
                    return Err(NestedDepthError {
                        depth: next,
                        limit: MAX_NESTED_VALUE_DEPTH,
                    });
                }
                for value in values {
                    visit(value, next)?;
                }
            }
            Value::Map(values) => {
                let next = depth + 1;
                if next > MAX_NESTED_VALUE_DEPTH {
                    return Err(NestedDepthError {
                        depth: next,
                        limit: MAX_NESTED_VALUE_DEPTH,
                    });
                }
                for value in values.values() {
                    visit(value, next)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    visit(value, 0)
}

/// Validate canonical key spellings throughout a neutral value tree.
pub fn validate_nested_keys(value: &Value) -> Result<(), NestedKeyError> {
    fn visit(value: &Value, depth: usize) -> Result<(), NestedKeyError> {
        match value {
            Value::Array(values) => {
                let next = depth + 1;
                if next > MAX_NESTED_VALUE_DEPTH {
                    return Err(NestedKeyError::DepthExceeded {
                        depth: next,
                        limit: MAX_NESTED_VALUE_DEPTH,
                    });
                }
                for value in values {
                    visit(value, next)?;
                }
            }
            Value::Map(values) => {
                let next = depth + 1;
                if next > MAX_NESTED_VALUE_DEPTH {
                    return Err(NestedKeyError::DepthExceeded {
                        depth: next,
                        limit: MAX_NESTED_VALUE_DEPTH,
                    });
                }
                for (position, (key, value)) in values.iter().enumerate() {
                    let decoded = NestedKey::decode(key)?;
                    for prior in values.keys().take(position) {
                        if NestedKey::decode(prior)?.text == decoded.text {
                            return Err(NestedKeyError::DuplicateLogicalKey {
                                key: decoded.text.into_owned(),
                            });
                        }
                    }
                    visit(value, next)?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    visit(value, 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_key_escapes_round_trip() {
        for logical in ["name", "@id", "#text", "\\leading"] {
            let encoded = NestedKey::encode_literal(logical);
            let decoded = NestedKey::decode(&encoded).unwrap();
            assert_eq!(decoded.text, logical);
            assert_eq!(decoded.escaped, encoded != logical);
        }
    }

    #[test]
    fn noncanonical_escape_is_rejected() {
        assert!(NestedKey::decode("\\ordinary").is_err());
        assert!(NestedKey::decode("\\#other").is_err());
    }

    #[test]
    fn duplicate_logical_keys_are_rejected_after_escape_decoding() {
        let mut values = indexmap::IndexMap::new();
        values.insert("@id".into(), Value::Integer(1));
        values.insert("\\@id".into(), Value::Integer(2));
        let value = Value::Map(Box::new(values));
        assert_eq!(
            validate_nested_keys(&value),
            Err(NestedKeyError::DuplicateLogicalKey { key: "@id".into() })
        );
    }

    #[test]
    fn depth_cap_accepts_cap_and_rejects_cap_plus_one() {
        fn nested(depth: usize) -> Value {
            let mut value = Value::Null;
            for _ in 0..depth {
                value = Value::Array(vec![value]);
            }
            value
        }
        assert!(validate_nested_depth(&nested(MAX_NESTED_VALUE_DEPTH)).is_ok());
        assert!(validate_nested_depth(&nested(MAX_NESTED_VALUE_DEPTH + 1)).is_err());
    }
}
