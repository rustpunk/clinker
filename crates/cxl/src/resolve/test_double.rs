use std::collections::HashMap;

use clinker_record::Value;

use super::traits::FieldResolver;

/// Test double for unit testing the resolver and evaluator.
/// Wraps HashMaps for both unqualified and qualified field lookup.
pub struct HashMapResolver {
    fields: HashMap<String, Value>,
    qualified: HashMap<(String, String), Value>,
}

impl HashMapResolver {
    /// Create a resolver from a flat map of field names to values.
    pub fn new(fields: HashMap<String, Value>) -> Self {
        Self {
            fields,
            qualified: HashMap::new(),
        }
    }

    /// Add a qualified field entry (`source.field` → value). Builder pattern.
    pub fn with_qualified(mut self, source: &str, field: &str, value: Value) -> Self {
        self.qualified
            .insert((source.into(), field.into()), value);
        self
    }
}

impl FieldResolver for HashMapResolver {
    fn resolve(&self, name: &str) -> Option<Value> {
        self.fields.get(name).cloned()
    }

    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value> {
        self.qualified
            .get(&(source.to_owned(), field.to_owned()))
            .cloned()
    }

    fn available_fields(&self) -> Vec<&str> {
        self.fields.keys().map(|s| s.as_str()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// FieldResolver must be object-safe for dynamic dispatch in the evaluator.
    #[test]
    fn test_field_resolver_object_safety() {
        let resolver = HashMapResolver::new(HashMap::from([(
            "name".into(),
            Value::String("Ada".into()),
        )]));
        let dyn_ref: &dyn FieldResolver = &resolver;
        assert_eq!(
            dyn_ref.resolve("name"),
            Some(Value::String("Ada".into()))
        );
    }

    /// WindowContext must be object-safe for dynamic dispatch in the evaluator.
    #[test]
    fn test_window_context_object_safety() {
        use super::super::traits::WindowContext;
        // Compile-time check: dyn WindowContext is constructible as a trait object
        fn _accepts_dyn(_: &dyn WindowContext) {}
    }

    /// Unqualified lookup returns stored value.
    #[test]
    fn test_hashmap_resolver_unqualified() {
        let resolver =
            HashMapResolver::new(HashMap::from([("age".into(), Value::Integer(30))]));
        assert_eq!(resolver.resolve("age"), Some(Value::Integer(30)));
    }

    /// Qualified lookup returns stored value.
    #[test]
    fn test_hashmap_resolver_qualified() {
        let resolver =
            HashMapResolver::new(HashMap::new()).with_qualified("src", "field", Value::Bool(true));
        assert_eq!(
            resolver.resolve_qualified("src", "field"),
            Some(Value::Bool(true))
        );
    }

    /// Missing field returns None, not an error.
    #[test]
    fn test_hashmap_resolver_missing_field() {
        let resolver = HashMapResolver::new(HashMap::new());
        assert_eq!(resolver.resolve("nonexistent"), None);
        assert_eq!(resolver.resolve_qualified("src", "nope"), None);
    }

    /// available_fields returns all registered field names.
    #[test]
    fn test_hashmap_resolver_available_fields() {
        let resolver = HashMapResolver::new(HashMap::from([
            ("a".into(), Value::Null),
            ("b".into(), Value::Null),
        ]));
        let mut fields = resolver.available_fields();
        fields.sort();
        assert_eq!(fields, vec!["a", "b"]);
    }
}
