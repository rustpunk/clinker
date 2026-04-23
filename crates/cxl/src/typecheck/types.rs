use crate::builtins::TypeTag;
use serde::{Deserialize, Serialize};

/// CXL type system. 7 concrete types + Numeric (union) + Any (unknown) + Nullable wrapper.
///
/// Serde representation: lowercase snake-case tag for the atomic variants
/// (`string`, `int`, `float`, `bool`, `date`, `date_time`, `null`, `array`,
/// `numeric`, `any`) plus a `nullable: <inner>` mapping form for `Nullable`.
/// Used by `SourceBody.schema` for compile-time CXL typecheck to declare
/// source column types inline in YAML.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Type {
    Null,
    Bool,
    Int,
    Float,
    String,
    Date,
    DateTime,
    Array,
    /// Union of Int | Float. Resolved during unification.
    Numeric,
    /// Unknown type — field used only in type-agnostic contexts.
    Any,
    /// Nullable wrapper. Idempotent: Nullable(Nullable(T)) is flattened to Nullable(T).
    Nullable(Box<Type>),
}

impl Type {
    /// Smart constructor: flattens nested Nullable. `Nullable(Null)` stays `Null`.
    pub fn nullable(inner: Type) -> Type {
        match inner {
            Type::Nullable(_) => inner,
            Type::Null => Type::Null,
            other => Type::Nullable(Box::new(other)),
        }
    }

    /// True if this type is or wraps Nullable.
    pub fn is_nullable(&self) -> bool {
        matches!(self, Type::Nullable(_) | Type::Null)
    }

    /// Strip Nullable wrapper if present.
    pub fn unwrap_nullable(&self) -> &Type {
        match self {
            Type::Nullable(inner) => inner,
            other => other,
        }
    }

    /// Convert from TypeTag (builtin method signatures) to Type.
    pub fn from_type_tag(tag: TypeTag) -> Type {
        match tag {
            TypeTag::String => Type::String,
            TypeTag::Int => Type::Int,
            TypeTag::Float => Type::Float,
            TypeTag::Bool => Type::Bool,
            TypeTag::Date => Type::Date,
            TypeTag::DateTime => Type::DateTime,
            TypeTag::Null => Type::Null,
            TypeTag::Any => Type::Any,
            TypeTag::Array => Type::Array,
            TypeTag::Numeric => Type::Numeric,
        }
    }

    /// Check if two types are compatible (can unify without error).
    /// Returns the unified type, or None if incompatible.
    pub fn unify(&self, other: &Type) -> Option<Type> {
        match (self, other) {
            // Same type
            (a, b) if a == b => Some(a.clone()),

            // Any unifies with anything
            (Type::Any, b) => Some(b.clone()),
            (a, Type::Any) => Some(a.clone()),

            // Numeric unifies with Int or Float
            (Type::Numeric, Type::Int) | (Type::Int, Type::Numeric) => Some(Type::Int),
            (Type::Numeric, Type::Float) | (Type::Float, Type::Numeric) => Some(Type::Float),

            // Int promotes to Float
            (Type::Int, Type::Float) | (Type::Float, Type::Int) => Some(Type::Float),

            // Nullable unification
            (Type::Nullable(a), Type::Nullable(b)) => a.unify(b).map(Type::nullable),
            (Type::Nullable(a), b) | (b, Type::Nullable(a)) => a.unify(b).map(Type::nullable),

            // Null unifies with anything as Nullable
            (Type::Null, b) => Some(Type::nullable(b.clone())),
            (a, Type::Null) => Some(Type::nullable(a.clone())),

            _ => None,
        }
    }

    /// Human-readable name for diagnostics.
    pub fn display_name(&self) -> &'static str {
        match self {
            Type::Null => "Null",
            Type::Bool => "Bool",
            Type::Int => "Int",
            Type::Float => "Float",
            Type::String => "String",
            Type::Date => "Date",
            Type::DateTime => "DateTime",
            Type::Array => "Array",
            Type::Numeric => "Numeric",
            Type::Any => "Any",
            Type::Nullable(_) => "Nullable",
        }
    }
}

impl std::fmt::Display for Type {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Type::Nullable(inner) => write!(f, "Nullable({})", inner),
            other => write!(f, "{}", other.display_name()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nullable_flattening() {
        let t = Type::nullable(Type::Int);
        assert_eq!(t, Type::Nullable(Box::new(Type::Int)));

        // Double-wrap flattens
        let t2 = Type::nullable(t);
        assert_eq!(t2, Type::Nullable(Box::new(Type::Int)));
    }

    #[test]
    fn test_nullable_null_stays_null() {
        assert_eq!(Type::nullable(Type::Null), Type::Null);
    }

    #[test]
    fn test_unify_same() {
        assert_eq!(Type::Int.unify(&Type::Int), Some(Type::Int));
    }

    #[test]
    fn test_unify_numeric_int() {
        assert_eq!(Type::Numeric.unify(&Type::Int), Some(Type::Int));
    }

    #[test]
    fn test_unify_numeric_float() {
        assert_eq!(Type::Numeric.unify(&Type::Float), Some(Type::Float));
    }

    #[test]
    fn test_unify_int_float_promotes() {
        assert_eq!(Type::Int.unify(&Type::Float), Some(Type::Float));
    }

    #[test]
    fn test_unify_incompatible() {
        assert_eq!(Type::String.unify(&Type::Int), None);
    }

    #[test]
    fn test_unify_any() {
        assert_eq!(Type::Any.unify(&Type::String), Some(Type::String));
    }

    #[test]
    fn test_unify_null_wraps() {
        assert_eq!(
            Type::Null.unify(&Type::Int),
            Some(Type::Nullable(Box::new(Type::Int)))
        );
    }

    #[test]
    fn test_from_type_tag() {
        assert_eq!(Type::from_type_tag(TypeTag::String), Type::String);
        assert_eq!(Type::from_type_tag(TypeTag::Numeric), Type::Numeric);
    }
}
