use clinker_record::Value;

/// Resolve a field name to a value from the current record.
///
/// Object-safe: usable as `dyn FieldResolver`. All methods take `&self` —
/// resolvers are shared references during evaluation. Returns owned `Value`
/// (not borrowed) to avoid lifetime entanglement through the evaluator call
/// chain — see spec SS11.4 for the rationale.
pub trait FieldResolver {
    /// Unqualified field lookup: `field_name` → Value.
    fn resolve(&self, name: &str) -> Option<Value>;

    /// Qualified field lookup: `source.field` → Value.
    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value>;

    /// All available field names. Used for fuzzy-match diagnostics in Phase B.
    /// Lifetime tied to `&self` — zero-copy borrows from internal storage.
    fn available_fields(&self) -> Vec<&str>;
}

/// Access window partition data (Arena + Secondary Index).
///
/// Object-safe: usable as `dyn WindowContext`. Positional functions return
/// `Option<Box<dyn FieldResolver>>` — one heap allocation per call site
/// (not per record or per field). Aggregation functions take `&str` field
/// names — the evaluator resolves expressions before calling the trait.
/// Predicate functions take closures where the `&dyn FieldResolver`
/// argument IS the `it` binding.
pub trait WindowContext {
    /// Return a resolver for the first record in the partition.
    /// None if the partition is empty.
    fn first(&self) -> Option<Box<dyn FieldResolver>>;

    /// Return a resolver for the last record in the partition.
    /// None if the partition is empty.
    fn last(&self) -> Option<Box<dyn FieldResolver>>;

    /// Return a resolver for the record `offset` positions before the current.
    /// None if out of bounds.
    fn lag(&self, offset: usize) -> Option<Box<dyn FieldResolver>>;

    /// Return a resolver for the record `offset` positions after the current.
    /// None if out of bounds.
    fn lead(&self, offset: usize) -> Option<Box<dyn FieldResolver>>;

    /// Number of records in the partition.
    fn count(&self) -> i64;

    /// Sum of a named field across all records in the partition.
    fn sum(&self, field: &str) -> Value;

    /// Average of a named field across all records in the partition.
    fn avg(&self, field: &str) -> Value;

    /// Minimum of a named field across all records in the partition.
    fn min(&self, field: &str) -> Value;

    /// Maximum of a named field across all records in the partition.
    fn max(&self, field: &str) -> Value;

    /// True if any record in the partition satisfies the predicate.
    /// The `&dyn FieldResolver` passed to the closure is the `it` binding.
    fn any(&self, predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool;

    /// True if all records in the partition satisfy the predicate.
    /// The `&dyn FieldResolver` passed to the closure is the `it` binding.
    fn all(&self, predicate: &dyn Fn(&dyn FieldResolver) -> bool) -> bool;
}

// Compile-time object safety assertions
const _: () = {
    fn _assert_field_resolver_object_safe(_: &dyn FieldResolver) {}
    fn _assert_window_context_object_safe(_: &dyn WindowContext) {}
};
