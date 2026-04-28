use std::collections::HashMap;

use crate::Value;
use crate::record_view::RecordView;
use crate::storage::RecordStorage;

/// Resolve a field name to a value from the current record.
///
/// Object-safe: usable as `dyn FieldResolver`. All methods take `&self`
/// — resolvers are shared references during evaluation. Returns
/// borrowed `&Value` (lifetime tied to `&self`) so the combine /
/// transform body-evaluator hot path can short-circuit through
/// coalesce, filter, and let-binding without cloning the underlying
/// `Value::String(Box<str>)`. The clone happens at most once per
/// field reference, at the eval-site that needs to keep the value
/// past the next `resolve` call.
///
/// Resolvers that want to hand out a `&Value` for a logically-absent
/// field (e.g. `CombineResolver` under `on_miss: null_fields`) can
/// return `Some(&clinker_record::NULL)` — the module-level sentinel
/// that exists solely to give these impls a stable address to borrow
/// from.
pub trait FieldResolver {
    /// Unqualified field lookup: `field_name` → `&Value`.
    fn resolve(&self, name: &str) -> Option<&Value>;

    /// Qualified field lookup: `source.field` → `&Value`.
    fn resolve_qualified(&self, source: &str, field: &str) -> Option<&Value>;

    /// All available field names. Used for fuzzy-match diagnostics.
    /// Lifetime tied to `&self` — zero-copy borrows from internal storage.
    fn available_fields(&self) -> Vec<&str>;

    /// All fields as borrowed `(&str, &Value)` pairs. Used for bare
    /// `distinct` (all-fields hash) and the evaluator's record-level
    /// iteration.
    fn iter_fields(&self) -> Vec<(&str, &Value)>;
}

/// Access window partition data (Arena + Secondary Index).
///
/// Lifetime `'a` ties the context to the Arena's lifetime.
/// Type parameter `S` is the record storage backend (Arena in production).
///
/// Positional functions return `Option<RecordView<'a, S>>` — zero heap allocation,
/// stack-allocated Copy views into the Arena. Aggregation functions take `&str`
/// field names and return computed `Value`s.
///
/// `any`/`all` removed from this trait — the evaluator controls iteration via
/// `partition_len()` and `partition_record()` for short-circuit evaluation.
pub trait WindowContext<'a, S: RecordStorage> {
    /// First record in the partition. None if empty.
    fn first(&self) -> Option<RecordView<'a, S>>;

    /// Last record in the partition. None if empty.
    fn last(&self) -> Option<RecordView<'a, S>>;

    /// Record `offset` positions before current. None if out of bounds.
    fn lag(&self, offset: usize) -> Option<RecordView<'a, S>>;

    /// Record `offset` positions after current. None if out of bounds.
    fn lead(&self, offset: usize) -> Option<RecordView<'a, S>>;

    /// Number of records in the partition.
    fn count(&self) -> i64;

    /// Sum of a named field across all records in the partition.
    /// Non-numeric fields → Value::Null.
    fn sum(&self, field: &str) -> Value;

    /// Running total of a named field over the prefix of the partition
    /// up to and including the current record. Returns the same shape
    /// as `sum` for the prefix slice; non-numeric fields → Value::Null.
    fn cumulative_sum(&self, field: &str) -> Value;

    /// Average of a named field across all records in the partition.
    /// Non-numeric fields → Value::Null.
    fn avg(&self, field: &str) -> Value;

    /// Minimum of a named field across all records in the partition.
    fn min(&self, field: &str) -> Value;

    /// Maximum of a named field across all records in the partition.
    fn max(&self, field: &str) -> Value;

    /// Number of records in the partition (for evaluator-driven any/all iteration).
    fn partition_len(&self) -> usize;

    /// Access record at position `index` in the partition (for evaluator-driven any/all).
    fn partition_record(&self, index: usize) -> RecordView<'a, S>;

    /// Collect field values from all partition records into an Array.
    fn collect(&self, field: &str) -> Value;

    /// Collect unique field values from all partition records into an Array.
    fn distinct(&self, field: &str) -> Value;
}

// Compile-time object safety assertion for FieldResolver
const _: () = {
    fn _assert_field_resolver_object_safe(_: &dyn FieldResolver) {}
};

// Note: WindowContext<'a, S> is object-safe when S is concrete (e.g. dyn WindowContext<'a, Arena>).
// The assertion is tested in clinker-core where Arena is available.

/// HashMap-backed FieldResolver for route condition evaluation and testing.
///
/// Wraps HashMaps for both unqualified and qualified field lookup.
/// Promoted from test double to production code — used by route condition
/// evaluation to resolve emitted fields.
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
        self.qualified.insert((source.into(), field.into()), value);
        self
    }
}

impl FieldResolver for HashMapResolver {
    fn resolve(&self, name: &str) -> Option<&Value> {
        self.fields.get(name)
    }

    fn resolve_qualified(&self, source: &str, field: &str) -> Option<&Value> {
        self.qualified.get(&(source.to_owned(), field.to_owned()))
    }

    fn available_fields(&self) -> Vec<&str> {
        self.fields.keys().map(|s| s.as_str()).collect()
    }

    fn iter_fields(&self) -> Vec<(&str, &Value)> {
        self.fields.iter().map(|(k, v)| (k.as_str(), v)).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Dummy RecordStorage for testing WindowContext object safety without circular deps.
    struct DummyStorage;

    impl RecordStorage for DummyStorage {
        fn resolve_field(&self, _index: u32, _name: &str) -> Option<&Value> {
            None
        }
        fn resolve_qualified(&self, _index: u32, _source: &str, _field: &str) -> Option<&Value> {
            None
        }
        fn available_fields(&self, _index: u32) -> Vec<&str> {
            vec![]
        }
        fn record_count(&self) -> u32 {
            0
        }
    }

    /// Compile-time assertion: WindowContext<'a, S> is object-safe when S is concrete.
    #[test]
    fn test_window_context_object_safe() {
        fn _assert_object_safe<'a>(_: &dyn WindowContext<'a, DummyStorage>) {}
    }

    /// RecordView is Copy and 16 bytes.
    #[test]
    fn test_record_view_size() {
        assert_eq!(
            std::mem::size_of::<RecordView<'_, DummyStorage>>(),
            16,
            "RecordView should be 16 bytes (pointer + u32 + padding)"
        );

        // Verify Copy by assignment
        let storage = DummyStorage;
        let view = RecordView::new(&storage, 0);
        let _copy = view; // Copy
        let _ = view; // Still usable
    }
}
