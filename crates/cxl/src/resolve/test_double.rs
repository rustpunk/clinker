// HashMapResolver promoted to clinker-record::resolver::HashMapResolver.
// Re-export for backward compatibility with existing cxl tests.
pub use clinker_record::resolver::HashMapResolver;

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use clinker_record::Value;
    use clinker_record::resolver::FieldResolver;

    use super::*;

    /// FieldResolver must be object-safe for dynamic dispatch in the evaluator.
    #[test]
    fn test_field_resolver_object_safety() {
        let resolver = HashMapResolver::new(HashMap::from([(
            "name".into(),
            Value::String("Ada".into()),
        )]));
        let dyn_ref: &dyn FieldResolver = &resolver;
        assert_eq!(dyn_ref.resolve("name"), Some(Value::String("Ada".into())));
    }

    /// WindowContext must be object-safe for dynamic dispatch in the evaluator.
    #[test]
    fn test_window_context_object_safety() {
        use clinker_record::{RecordStorage, WindowContext};
        struct DummyStorage;
        impl RecordStorage for DummyStorage {
            fn resolve_field(&self, _: u32, _: &str) -> Option<Value> {
                None
            }
            fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<Value> {
                None
            }
            fn available_fields(&self, _: u32) -> Vec<&str> {
                vec![]
            }
            fn record_count(&self) -> u32 {
                0
            }
        }
        fn _accepts_dyn<'a>(_: &dyn WindowContext<'a, DummyStorage>) {}
    }

    /// Unqualified lookup returns stored value.
    #[test]
    fn test_hashmap_resolver_unqualified() {
        let resolver = HashMapResolver::new(HashMap::from([("age".into(), Value::Integer(30))]));
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
