use crate::resolver::FieldResolver;
use crate::schema::Schema;
use crate::value::Value;
use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// Maximum number of metadata keys per record.
const MAX_METADATA_KEYS: usize = 64;

/// Schema-indexed row record.
///
/// Values live in a positional `Vec<Value>` whose length equals
/// `schema.column_count()`. Every write lands at a known schema slot;
/// there is no side map for unknown fields. Plan-time schema widening
/// guarantees every emit site addresses a column declared on the
/// operator's `Arc<Schema>`.
///
/// Per-record framing data (unknown keys discovered by readers,
/// reader/writer context) lives in the separate `metadata` map under
/// the `$meta.*` namespace, capped at 64 keys per record.
#[derive(Debug, Clone)]
pub struct Record {
    schema: Arc<Schema>,
    values: Vec<Value>,
    /// Per-record metadata. Stripped from output unless `include_metadata` is set.
    /// Lazy-initialized on first `set_meta()` call. Deep-cloned on `Record::clone`.
    metadata: Option<Box<IndexMap<Box<str>, Value>>>,
}

/// Wire payload for a [`Record`] in binary spill streams.
///
/// Schema is not embedded — the spill stream writes the schema once in
/// a header line, so each record body carries only positional values and
/// optional per-record metadata. Callers reconstruct a full `Record` via
/// [`RecordPayload::into_record`].
///
/// Wire format (postcard): `(Vec<Value>, Option<Vec<(Box<str>, Value)>>)`.
#[derive(Debug, Serialize, Deserialize)]
pub struct RecordPayload {
    /// Positional field values in schema column order.
    pub values: Vec<Value>,
    /// Per-record metadata entries, or `None` if the record has no metadata.
    pub metadata: Option<Vec<(Box<str>, Value)>>,
}

impl RecordPayload {
    /// Reconstruct a [`Record`] from this payload using a caller-supplied schema.
    pub fn into_record(self, schema: Arc<Schema>) -> Record {
        let mut record = Record::new(schema, self.values);
        if let Some(meta_pairs) = self.metadata {
            for (k, v) in meta_pairs {
                // Metadata cap errors are ignored on deserialization — the cap
                // is a write-time guard, not a wire-format invariant.
                let _ = record.set_meta(&k, v);
            }
        }
        record
    }
}

impl Serialize for Record {
    /// Serializes this record as a [`RecordPayload`] (values + metadata).
    ///
    /// The schema is not included in the wire output. Callers that need
    /// schema information must write it separately (e.g., as a header line)
    /// and supply it on deserialization via [`RecordPayload::into_record`].
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let meta = self.metadata_pairs();
        RecordPayload {
            values: self.values.clone(),
            metadata: if meta.is_empty() { None } else { Some(meta) },
        }
        .serialize(serializer)
    }
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

    /// Returns the schema-indexed value for `name`, or `None` when the
    /// name is not declared on this record's schema.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.schema.index(name).and_then(|i| self.values.get(i))
    }

    /// Writes `value` to the schema-indexed slot for `name`. Plan-time
    /// widening guarantees `name` is in schema at every emit site;
    /// `debug_assert` surfaces widening bugs in debug builds. In release,
    /// writes to unknown fields are silently dropped — this is a
    /// defense-in-depth no-op behind the debug guard, not a recovery
    /// mechanism.
    pub fn set(&mut self, name: &str, value: Value) {
        match self.schema.index(name) {
            Some(idx) => self.values[idx] = value,
            None => {
                debug_assert!(
                    false,
                    "Record::set called with unknown field {name:?}; plan-time widening should have \
                     added it to the operator's output schema",
                );
            }
        }
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

    /// Collect all metadata entries as owned `(Box<str>, Value)` pairs.
    ///
    /// Returns an empty `Vec` when the record has no metadata. Used by
    /// [`crate::Record`]'s `Serialize` impl to produce a [`RecordPayload`]
    /// for binary spill encoding.
    pub fn metadata_pairs(&self) -> Vec<(Box<str>, Value)> {
        match &self.metadata {
            None => Vec::new(),
            Some(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        }
    }

    // ── Fields ─────────────────────────────────────────────────────

    /// Iterator over every schema field in schema order. Metadata is
    /// exposed separately via [`Record::iter_meta`]; writers that want
    /// to include metadata in their output layer the two iterators.
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

    /// Total number of fields — identical to [`Record::field_count`]
    /// now that overflow storage no longer exists. Retained as an alias
    /// for call-site readability at heap-accounting sites.
    pub fn total_field_count(&self) -> usize {
        self.schema.column_count()
    }

    /// Estimated heap bytes owned by this record.
    ///
    /// Includes `Vec<Value>` backing store, per-value heap allocations
    /// (strings, arrays), and the metadata IndexMap (if present). Used
    /// by `SortBuffer` for self-tracking allocation counting.
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
    fn resolve(&self, name: &str) -> Option<&Value> {
        // Handle $meta.* namespace: resolve from per-record metadata map.
        if let Some(meta_key) = name.strip_prefix("$meta.") {
            return self.get_meta(meta_key);
        }
        self.get(name)
    }

    fn resolve_qualified(&self, _source: &str, field: &str) -> Option<&Value> {
        self.get(field)
    }

    fn available_fields(&self) -> Vec<&str> {
        self.schema().columns().iter().map(|c| &**c).collect()
    }

    fn iter_fields(&self) -> Vec<(&str, &Value)> {
        self.iter_all_fields().collect()
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

    /// Structural assertion: `Record` holds exactly `schema`, `values`,
    /// and `metadata`. Adding or renaming a field fails compilation.
    #[test]
    fn test_record_struct_has_exactly_schema_values_metadata_fields() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let rec = Record::new(schema, values);
        let Record {
            schema: _,
            values: _,
            metadata: _,
        } = rec;
    }

    #[test]
    fn test_record_set_writes_schema_slot() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);
        record.set("name", Value::String("Alice".into()));
        assert_eq!(record.get("name"), Some(&Value::String("Alice".into())),);
        assert_eq!(record.values()[1], Value::String("Alice".into()));
    }

    /// Writing an unknown field in debug panics via `debug_assert`.
    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "Record::set called with unknown field")]
    fn test_record_set_unknown_field_debug_panics() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);
        record.set("nonexistent", Value::Integer(1));
    }

    #[test]
    fn test_record_iter_all_fields_is_schema_only() {
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
        assert_eq!(fields.len(), 5);
        assert_eq!(fields[0].0, "id");
        assert_eq!(fields[1].0, "name");
        assert_eq!(fields[4].0, "active");
    }

    /// Metadata round-trip survives the overflow rip — the metadata map
    /// is preserved as the sole off-schema per-record storage.
    #[test]
    fn test_record_metadata_preserved() {
        let schema = test_schema();
        let values = vec![Value::Null; 5];
        let mut record = Record::new(schema, values);

        assert!(!record.has_meta());
        record.set_meta("k", Value::Integer(1)).unwrap();
        assert!(record.has_meta());
        assert_eq!(record.get_meta("k"), Some(&Value::Integer(1)));
        let collected: Vec<(&str, &Value)> = record.iter_meta().collect();
        assert_eq!(collected.len(), 1);
        assert_eq!(collected[0].0, "k");
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
        assert_eq!(record.resolve("name"), Some(&Value::String("Ada".into())));
        assert_eq!(record.resolve("age"), Some(&Value::Integer(30)));
        assert_eq!(record.resolve("unknown"), None);

        // Qualified lookup delegates to unqualified
        assert_eq!(
            record.resolve_qualified("any_source", "name"),
            Some(&Value::String("Ada".into()))
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
