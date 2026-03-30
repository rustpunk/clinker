use crate::resolver::FieldResolver;
use crate::schema::Schema;
use crate::value::Value;
use indexmap::IndexMap;
use std::sync::Arc;

/// Schema-indexed record with optional overflow for CXL-emitted unknown fields.
/// Overflow uses IndexMap to preserve emit statement order in output.
#[derive(Debug, Clone)]
pub struct Record {
    schema: Arc<Schema>,
    values: Vec<Value>,
    overflow: Option<Box<IndexMap<Box<str>, Value>>>,
}

impl Record {
    pub fn new(schema: Arc<Schema>, mut values: Vec<Value>) -> Self {
        debug_assert_eq!(
            schema.column_count(),
            values.len(),
            "Record::new: schema has {} columns but got {} values",
            schema.column_count(),
            values.len(),
        );
        // Release-mode safety: pad with Null or truncate
        let expected = schema.column_count();
        match values.len().cmp(&expected) {
            std::cmp::Ordering::Less => values.resize(expected, Value::Null),
            std::cmp::Ordering::Greater => values.truncate(expected),
            std::cmp::Ordering::Equal => {}
        }
        Self {
            schema,
            values,
            overflow: None,
        }
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        if let Some(idx) = self.schema.index(name) {
            return self.values.get(idx);
        }
        self.overflow.as_ref().and_then(|m| m.get(name))
    }

    pub fn set(&mut self, name: &str, value: Value) -> bool {
        if let Some(idx) = self.schema.index(name) {
            self.values[idx] = value;
            return true;
        }
        false
    }

    pub fn set_overflow(&mut self, name: Box<str>, value: Value) {
        // If name exists in schema, redirect to schema slot
        if let Some(idx) = self.schema.index(&name) {
            debug_assert!(
                false,
                "set_overflow called with schema field '{name}'; redirecting to schema slot"
            );
            self.values[idx] = value;
            return;
        }
        let map = self.overflow.get_or_insert_with(|| Box::new(IndexMap::new()));
        map.insert(name, value);
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Raw access to the schema-indexed values slice (positional).
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Iterator over overflow fields only. Returns None if no overflow exists.
    pub fn overflow_fields(&self) -> Option<impl Iterator<Item = (&str, &Value)>> {
        self.overflow
            .as_ref()
            .map(|m| m.iter().map(|(k, v)| (k.as_ref(), v)))
    }

    /// Iterator over ALL fields: schema fields first (in schema order), then overflow.
    pub fn iter_all_fields(&self) -> impl Iterator<Item = (&str, &Value)> {
        let schema_fields = self
            .schema
            .columns()
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_ref(), &self.values[i]));
        let overflow_fields = self
            .overflow
            .as_ref()
            .into_iter()
            .flat_map(|m| m.iter().map(|(k, v)| (k.as_ref(), v)));
        schema_fields.chain(overflow_fields)
    }

    /// Number of schema fields (not counting overflow).
    pub fn field_count(&self) -> usize {
        self.schema.column_count()
    }

    /// Total number of fields including overflow.
    pub fn total_field_count(&self) -> usize {
        self.schema.column_count() + self.overflow.as_ref().map_or(0, |m| m.len())
    }

    /// Estimated heap bytes owned by this record.
    ///
    /// Includes Vec<Value> backing store + per-value heap allocations
    /// (strings, arrays) + overflow IndexMap if present.
    /// Used by SortBuffer for self-tracking allocation counting.
    pub fn estimated_heap_size(&self) -> usize {
        let values_backing = self.values.capacity() * std::mem::size_of::<Value>();
        let values_heap: usize = self.values.iter().map(Value::heap_size).sum();
        let overflow_size = self.overflow.as_ref().map_or(0, |m| {
            // IndexMap overhead: capacity * (key + value + hash)
            let entry_size = std::mem::size_of::<Box<str>>()
                + std::mem::size_of::<Value>()
                + std::mem::size_of::<u64>();
            let map_backing = m.capacity() * entry_size;
            let keys_heap: usize = m.keys().map(|k| k.len()).sum();
            let values_heap: usize = m.values().map(Value::heap_size).sum();
            map_backing + keys_heap + values_heap
        });
        values_backing + values_heap + overflow_size
    }
}

impl FieldResolver for Record {
    fn resolve(&self, name: &str) -> Option<Value> {
        self.get(name).cloned()
    }

    fn resolve_qualified(&self, _source: &str, field: &str) -> Option<Value> {
        self.get(field).cloned()
    }

    fn available_fields(&self) -> Vec<&str> {
        self.schema().columns().iter().map(|c| &**c).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Arc<Schema> {
        let cols: Vec<Box<str>> = vec![
            "id".into(),
            "name".into(),
            "age".into(),
            "email".into(),
            "active".into(),
        ];
        Arc::new(Schema::new(cols))
    }

    #[test]
    fn test_record_get_set_by_name() {
        let schema = test_schema();
        let values = vec![
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Integer(30),
            Value::String("alice@example.com".into()),
            Value::Bool(true),
        ];
        let mut record = Record::new(schema, values);

        assert_eq!(record.get("name"), Some(&Value::String("Alice".into())));

        record.set("name", Value::String("Bob".into()));
        assert_eq!(record.get("name"), Some(&Value::String("Bob".into())));
    }

    #[test]
    fn test_record_overflow_lazy_init() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);

        assert!(record.overflow.is_none());

        record.set_overflow("extra_field".into(), Value::Integer(99));
        assert!(record.overflow.is_some());
        assert_eq!(record.get("extra_field"), Some(&Value::Integer(99)));
    }

    #[test]
    #[cfg_attr(debug_assertions, should_panic)]
    fn test_record_overflow_no_shadow() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);

        // Setting a schema field name via set_overflow should redirect (panics in debug)
        record.set_overflow("name".into(), Value::String("redirected".into()));
        assert_eq!(
            record.get("name"),
            Some(&Value::String("redirected".into()))
        );
        assert!(
            record.overflow.is_none()
                || !record.overflow.as_ref().unwrap().contains_key("name")
        );
    }

    #[test]
    fn test_record_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Record>();
    }

    #[test]
    #[cfg_attr(debug_assertions, ignore = "debug_assert_eq panics on mismatch")]
    fn test_record_new_length_mismatch_pad() {
        let schema = test_schema();
        let values = vec![Value::Integer(1), Value::String("Alice".into())];
        let record = Record::new(schema, values);
        assert_eq!(record.get("id"), Some(&Value::Integer(1)));
        assert_eq!(record.get("name"), Some(&Value::String("Alice".into())));
        assert_eq!(record.get("age"), Some(&Value::Null));
    }

    #[test]
    fn test_record_iter_all_fields_ordering() {
        let schema = test_schema();
        let values = vec![
            Value::Integer(1),
            Value::String("Alice".into()),
            Value::Integer(30),
            Value::String("alice@test.com".into()),
            Value::Bool(true),
        ];
        let mut record = Record::new(schema, values);
        record.set_overflow("extra".into(), Value::String("bonus".into()));

        let fields: Vec<(&str, &Value)> = record.iter_all_fields().collect();
        assert_eq!(fields[0].0, "id");
        assert_eq!(fields[1].0, "name");
        assert_eq!(fields[4].0, "active");
        assert_eq!(fields[5].0, "extra");
        assert_eq!(fields.len(), 6);
    }

    #[test]
    fn test_record_values_raw_access() {
        let schema = test_schema();
        let values = vec![Value::Integer(1); 5];
        let record = Record::new(schema, values);
        assert_eq!(record.values().len(), 5);
        assert_eq!(record.values()[0], Value::Integer(1));
    }

    #[test]
    fn test_record_total_field_count() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);
        assert_eq!(record.total_field_count(), 5);
        record.set_overflow("extra1".into(), Value::Integer(1));
        record.set_overflow("extra2".into(), Value::Integer(2));
        assert_eq!(record.total_field_count(), 7);
        assert_eq!(record.field_count(), 5);
    }

    #[test]
    fn test_record_implements_field_resolver() {
        use crate::resolver::FieldResolver;

        let schema = Arc::new(Schema::new(vec!["name".into(), "age".into()]));
        let record = Record::new(
            schema,
            vec![Value::String("Ada".into()), Value::Integer(30)],
        );
        assert_eq!(record.resolve("name"), Some(Value::String("Ada".into())));
        assert_eq!(record.resolve("age"), Some(Value::Integer(30)));
        assert_eq!(record.resolve("unknown"), None);

        // Qualified lookup delegates to unqualified
        assert_eq!(
            record.resolve_qualified("any_source", "name"),
            Some(Value::String("Ada".into()))
        );

        let mut fields = record.available_fields();
        fields.sort();
        assert_eq!(fields, vec!["age", "name"]);
    }

    #[test]
    fn test_record_estimated_heap_size() {
        let schema = Arc::new(Schema::new(vec!["name".into(), "value".into()]));
        let record = Record::new(
            schema,
            vec![Value::String("hello".into()), Value::Integer(42)],
        );
        let size = record.estimated_heap_size();
        // Vec backing: capacity(2) * sizeof(Value)
        let expected_backing = 2 * std::mem::size_of::<Value>();
        // String "hello" = 5 bytes heap, Integer = 0
        let expected_heap = 5;
        assert_eq!(size, expected_backing + expected_heap);
    }

    #[test]
    fn test_record_estimated_heap_size_with_overflow() {
        let schema = Arc::new(Schema::new(vec!["id".into()]));
        let mut record = Record::new(schema, vec![Value::Integer(1)]);
        let base_size = record.estimated_heap_size();
        record.set_overflow("extra".into(), Value::String("test".into()));
        let with_overflow = record.estimated_heap_size();
        // Overflow adds: map backing + key "extra" (5 bytes) + value "test" (4 bytes)
        assert!(with_overflow > base_size);
    }

    #[test]
    fn test_record_set_unknown_field_returns_false() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);
        assert!(!record.set("nonexistent", Value::Integer(1)));
    }

    #[test]
    fn test_record_overflow_preserves_emit_order() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);

        record.set_overflow("Zulu".into(), Value::Integer(3));
        record.set_overflow("Alpha".into(), Value::Integer(1));
        record.set_overflow("Mike".into(), Value::Integer(2));

        let overflow: Vec<_> = record.overflow_fields().unwrap().collect();
        assert_eq!(overflow[0].0, "Zulu");
        assert_eq!(overflow[1].0, "Alpha");
        assert_eq!(overflow[2].0, "Mike");
    }
}
