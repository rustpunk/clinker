use crate::resolver::FieldResolver;
use crate::schema::Schema;
use crate::value::Value;
use indexmap::IndexMap;
use std::sync::Arc;

/// Maximum number of metadata keys per record.
const MAX_METADATA_KEYS: usize = 64;

/// Schema-indexed record (Option-W runtime layout).
///
/// Every `Record` carries the **producing node's bound output schema**
/// on `schema`, with `values` positionally aligned to it. There is no
/// overflow side-channel: fields not declared in the node's bound
/// output schema simply do not exist on records produced by that node.
/// Emits land in positional slots baked in at plan time by
/// `bind_schema` + the Option-W projection pass in the executor.
///
/// Per the 2026-04-13 research (`docs/internal/research/RESEARCH-runtime-record-layout.md`):
/// this is the universal pattern in Kettle/Apache Hop, PostgreSQL,
/// Spark Tungsten, DataFusion, DuckDB, Velox, CockroachDB, Materialize,
/// Polars, and SQLite VDBE — every row-major / typed tabular engine
/// surveyed. The overflow side-channel previously on this struct was
/// Option-Y (no prior art across 20+ engines; documented failure
/// pattern in every system that grew a two-record-type split) and has
/// been removed as part of landing Option-W.
#[derive(Debug, Clone)]
pub struct Record {
    schema: Arc<Schema>,
    values: Vec<Value>,
    /// Per-record metadata. Stripped from output unless `include_metadata` is set.
    /// Lazy-initialized on first `set_meta()` call. Deep-cloned on `Record::clone`.
    metadata: Option<Box<IndexMap<Box<str>, Value>>>,
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
            metadata: None,
        }
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.schema.index(name).and_then(|idx| self.values.get(idx))
    }

    /// Set a schema field positionally by name. Returns `true` if the
    /// name is in the record's schema, `false` otherwise (the value is
    /// **not** stored). Callers that need to emit a field not in the
    /// record's current schema must construct a new `Record` with the
    /// producing node's bound output schema (Option-W contract).
    pub fn set(&mut self, name: &str, value: Value) -> bool {
        if let Some(idx) = self.schema.index(name) {
            self.values[idx] = value;
            return true;
        }
        false
    }

    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Raw access to the schema-indexed values slice (positional).
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    // ── Metadata ────────────────────────────────────────────────────

    /// Read a per-record metadata value by key.
    pub fn get_meta(&self, key: &str) -> Option<&Value> {
        self.metadata.as_ref().and_then(|m| m.get(key))
    }

    /// Write a per-record metadata value. Lazy-inits the map on first call.
    /// Returns `Err` if the per-record metadata cap (64 keys) would be exceeded.
    pub fn set_meta(&mut self, key: &str, value: Value) -> Result<(), &'static str> {
        let map = self
            .metadata
            .get_or_insert_with(|| Box::new(IndexMap::new()));
        if !map.contains_key(key) && map.len() >= MAX_METADATA_KEYS {
            return Err("per-record metadata cap exceeded (64 keys)");
        }
        map.insert(key.into(), value);
        Ok(())
    }

    /// Whether this record has any metadata entries.
    pub fn has_meta(&self) -> bool {
        self.metadata.as_ref().is_some_and(|m| !m.is_empty())
    }

    /// Iterator over all metadata key-value pairs.
    pub fn iter_meta(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.metadata
            .iter()
            .flat_map(|m| m.iter().map(|(k, v)| (k.as_ref(), v)))
    }

    // ── Fields ─────────────────────────────────────────────────────

    /// Iterator over schema fields in declaration order.
    pub fn iter_all_fields(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.schema
            .columns()
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_ref(), &self.values[i]))
    }

    /// Number of schema fields.
    pub fn field_count(&self) -> usize {
        self.schema.column_count()
    }

    /// Total number of fields (same as field_count under Option-W; the
    /// old overflow side-channel has been removed).
    pub fn total_field_count(&self) -> usize {
        self.schema.column_count()
    }

    /// Estimated heap bytes owned by this record.
    ///
    /// Includes `Vec<Value>` backing store + per-value heap
    /// allocations (strings, arrays) + metadata `IndexMap` if present.
    /// Used by SortBuffer for self-tracking allocation counting.
    pub fn estimated_heap_size(&self) -> usize {
        let values_backing = self.values.capacity() * std::mem::size_of::<Value>();
        let values_heap: usize = self.values.iter().map(Value::heap_size).sum();
        let indexmap_heap = |m: &IndexMap<Box<str>, Value>| {
            let entry_size = std::mem::size_of::<Box<str>>()
                + std::mem::size_of::<Value>()
                + std::mem::size_of::<u64>();
            let map_backing = m.capacity() * entry_size;
            let keys_heap: usize = m.keys().map(|k| k.len()).sum();
            let values_heap: usize = m.values().map(Value::heap_size).sum();
            map_backing + keys_heap + values_heap
        };
        let metadata_size = self.metadata.as_ref().map_or(0, |m| indexmap_heap(m));
        values_backing + values_heap + metadata_size
    }
}

impl FieldResolver for Record {
    fn resolve(&self, name: &str) -> Option<Value> {
        // Handle $meta.* namespace: resolve from per-record metadata map.
        if let Some(meta_key) = name.strip_prefix("$meta.") {
            return self.get_meta(meta_key).cloned();
        }
        self.get(name).cloned()
    }

    fn resolve_qualified(&self, _source: &str, field: &str) -> Option<Value> {
        self.get(field).cloned()
    }

    fn available_fields(&self) -> Vec<&str> {
        self.schema().columns().iter().map(|c| &**c).collect()
    }

    fn iter_fields(&self) -> Vec<(String, Value)> {
        self.iter_all_fields()
            .map(|(name, val)| (name.to_string(), val.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    mod metadata;

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
    fn test_record_set_unknown_field_returns_false() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);
        // Under Option-W, setting a field not declared in the record's
        // bound schema is a no-op — callers must construct a new
        // Record with the correct schema.
        assert!(!record.set("nonexistent", Value::Integer(1)));
        assert_eq!(record.get("nonexistent"), None);
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
        let record = Record::new(schema, values);

        let fields: Vec<(&str, &Value)> = record.iter_all_fields().collect();
        assert_eq!(fields[0].0, "id");
        assert_eq!(fields[1].0, "name");
        assert_eq!(fields[4].0, "active");
        assert_eq!(fields.len(), 5);
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
        let record = Record::new(schema, values);
        assert_eq!(record.total_field_count(), 5);
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
}
