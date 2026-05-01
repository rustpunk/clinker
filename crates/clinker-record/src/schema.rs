use ahash::RandomState;
use std::collections::HashMap;
use std::sync::Arc;

/// Per-column annotation distinguishing an engine-stamped column from a
/// user-declared one.
///
/// The annotation is the structural marker writers and the projection
/// fast path consult to decide whether a column is included in default
/// output. Two engine-stamping shapes exist:
///
/// - [`FieldMetadata::SourceCorrelation`] — the source-binding pass
///   widens a Source's schema with a `$ck.<field>` shadow column when
///   that source declares a `correlation_key`. The shadow column is
///   stamped at ingest with the user-declared field's value, then
///   write-protected at the CXL parser (`emit $ck.* = ...` is
///   rejected). Frozen-identity semantics flow through the column for
///   the rest of the DAG so downstream Transforms cannot disturb
///   correlation-group identity.
/// - [`FieldMetadata::AggregateGroupIndex`] — a relaxed aggregate
///   (one whose `group_by` does not cover the upstream correlation
///   key) emits one `$ck.aggregate.<aggregate_name>` column carrying
///   the aggregator's group index. Detect-phase walks this lineage to
///   resolve a downstream failure back to every contributing source
///   row, matching the upstream-failure DLQ-fan-out semantic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FieldMetadata {
    /// Source-CK shadow column. `source_field` is the user-declared
    /// field whose value the engine snapshots into this column at
    /// ingest. The column is named `$ck.<source_field>`.
    SourceCorrelation { source_field: Box<str> },
    /// Synthetic correlation column emitted by a relaxed aggregate.
    /// `aggregate_name` is the aggregate node's name; the column is
    /// named `$ck.aggregate.<aggregate_name>` and carries the group
    /// index assigned by the aggregator at finalize.
    AggregateGroupIndex { aggregate_name: Box<str> },
}

impl FieldMetadata {
    /// Marks this column as a source-CK snapshot of `source_field`.
    pub fn source_correlation(source_field: impl Into<Box<str>>) -> Self {
        Self::SourceCorrelation {
            source_field: source_field.into(),
        }
    }

    /// Marks this column as the synthetic group-index emitted by a
    /// relaxed aggregate node named `aggregate_name`.
    pub fn aggregate_group_index(aggregate_name: impl Into<Box<str>>) -> Self {
        Self::AggregateGroupIndex {
            aggregate_name: aggregate_name.into(),
        }
    }

    /// True for every variant. The presence of [`FieldMetadata`] on a
    /// column is itself the engine-stamp marker; the variant only
    /// distinguishes which engine subsystem stamped it. Centralizes the
    /// writer-strip / projection-skip predicate so future engine-
    /// stamped namespaces extend this enum and surface here without
    /// every consumer having to re-enumerate variants.
    pub fn is_engine_stamped(&self) -> bool {
        true
    }
}

#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<Box<str>>,
    /// Per-column metadata, parallel to `columns` (same length, same
    /// order). `None` for user-declared columns; `Some(...)` for
    /// engine-stamped columns (e.g. `$ck.<field>` snapshot columns).
    field_metadata: Vec<Option<FieldMetadata>>,
    index: HashMap<Box<str>, usize, RandomState>,
}

impl Schema {
    /// Construct a schema from column names alone, with no per-column
    /// metadata. Equivalent to `Schema::with_metadata(columns, vec![None; n])`.
    pub fn new(columns: Vec<Box<str>>) -> Self {
        let n = columns.len();
        Self::with_metadata(columns, vec![None; n])
    }

    /// Construct a schema from columns plus a parallel metadata vector.
    /// `field_metadata.len()` must equal `columns.len()`.
    pub fn with_metadata(
        columns: Vec<Box<str>>,
        field_metadata: Vec<Option<FieldMetadata>>,
    ) -> Self {
        debug_assert_eq!(
            columns.len(),
            field_metadata.len(),
            "Schema::with_metadata: columns ({}) vs field_metadata ({}) length mismatch",
            columns.len(),
            field_metadata.len(),
        );
        let hasher = RandomState::with_seeds(1, 2, 3, 4);
        let mut index = HashMap::with_capacity_and_hasher(columns.len(), hasher);
        for (i, name) in columns.iter().enumerate() {
            index.insert(name.clone(), i);
        }
        Self {
            columns,
            field_metadata,
            index,
        }
    }

    /// All column names in insertion order (determines output field ordering).
    pub fn columns(&self) -> &[Box<str>] {
        &self.columns
    }

    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// O(1) name -> positional index lookup. Returns None if field not in schema.
    pub fn index(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    /// Positional index -> name. Returns None if index out of bounds.
    pub fn column_name(&self, idx: usize) -> Option<&str> {
        self.columns.get(idx).map(|s| &**s)
    }

    /// Returns true if the schema contains a field with this name.
    pub fn contains(&self, name: &str) -> bool {
        self.index.contains_key(name)
    }

    /// Per-column metadata at positional index, or `None` when the
    /// column has no engine-stamp annotation or `idx` is out of range.
    pub fn field_metadata(&self, idx: usize) -> Option<&FieldMetadata> {
        self.field_metadata.get(idx).and_then(|m| m.as_ref())
    }

    /// Per-column metadata for the named column. Returns `None` for
    /// unknown names or columns with no engine-stamp annotation.
    pub fn field_metadata_by_name(&self, name: &str) -> Option<&FieldMetadata> {
        self.index(name).and_then(|i| self.field_metadata(i))
    }
}

/// Fluent builder for `Arc<Schema>` construction.
///
/// Consolidates the many ad-hoc `Arc::new(Schema::new(Vec<Box<str>>))` call
/// sites across readers, planners, and projection into a single API — chained
/// `.with_field(...)`, `.extend(...)`, or `.collect::<SchemaBuilder>()` all
/// terminate in `.build()`, which is the sole place a fresh `Arc<Schema>` is
/// materialized outside the test suite. Patterned on `arrow_schema::SchemaBuilder`.
#[derive(Debug, Clone, Default)]
pub struct SchemaBuilder {
    columns: Vec<Box<str>>,
    field_metadata: Vec<Option<FieldMetadata>>,
}

impl SchemaBuilder {
    /// Empty builder; equivalent to `SchemaBuilder::default()`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Preallocate storage for `n` columns so push-style construction
    /// avoids intermediate `Vec` growth.
    pub fn with_capacity(n: usize) -> Self {
        Self {
            columns: Vec::with_capacity(n),
            field_metadata: Vec::with_capacity(n),
        }
    }

    /// Append a single column name with no metadata, and return `self`
    /// for chaining.
    pub fn with_field(mut self, name: impl Into<Box<str>>) -> Self {
        self.columns.push(name.into());
        self.field_metadata.push(None);
        self
    }

    /// Append a column with attached engine-stamp metadata.
    pub fn with_field_meta(mut self, name: impl Into<Box<str>>, meta: FieldMetadata) -> Self {
        self.columns.push(name.into());
        self.field_metadata.push(Some(meta));
        self
    }

    /// Append every column yielded by `iter`, converting each item via `Into<Box<str>>`.
    /// All appended columns receive `None` metadata.
    pub fn extend<I, S>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Box<str>>,
    {
        for s in iter {
            self.columns.push(s.into());
            self.field_metadata.push(None);
        }
        self
    }

    /// Finalize the builder into an `Arc<Schema>`. Consumes `self`.
    pub fn build(self) -> Arc<Schema> {
        Arc::new(Schema::with_metadata(self.columns, self.field_metadata))
    }
}

impl<S: Into<Box<str>>> FromIterator<S> for SchemaBuilder {
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        let cols: Vec<Box<str>> = iter.into_iter().map(Into::into).collect();
        let n = cols.len();
        Self {
            columns: cols,
            field_metadata: vec![None; n],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_schema() -> Schema {
        let cols: Vec<Box<str>> = vec![
            "id".into(),
            "name".into(),
            "age".into(),
            "email".into(),
            "active".into(),
        ];
        Schema::new(cols)
    }

    #[test]
    fn test_schema_index_lookup() {
        let schema = test_schema();
        assert_eq!(schema.index("id"), Some(0));
        assert_eq!(schema.index("name"), Some(1));
        assert_eq!(schema.index("age"), Some(2));
        assert_eq!(schema.index("email"), Some(3));
        assert_eq!(schema.index("active"), Some(4));
        assert_eq!(schema.column_count(), 5);
        assert_eq!(schema.column_name(0), Some("id"));
        assert_eq!(schema.columns().len(), 5);
    }

    #[test]
    fn test_schema_unknown_field_returns_none() {
        let schema = test_schema();
        assert_eq!(schema.index("nonexistent"), None);
        assert_eq!(schema.column_name(99), None);
    }

    #[test]
    fn test_schema_contains() {
        let schema = test_schema();
        assert!(schema.contains("id"));
        assert!(schema.contains("name"));
        assert!(!schema.contains("nonexistent"));
    }

    #[test]
    fn test_schema_duplicate_column_names() {
        let cols: Vec<Box<str>> = vec!["a".into(), "b".into(), "a".into()];
        let schema = Schema::new(cols);
        assert_eq!(schema.column_count(), 3);
        assert_eq!(schema.index("a"), Some(2)); // last occurrence wins
    }

    #[test]
    fn test_schema_empty() {
        let schema = Schema::new(vec![]);
        assert_eq!(schema.column_count(), 0);
        assert_eq!(schema.index("anything"), None);
        assert_eq!(schema.column_name(0), None);
        assert!(schema.columns().is_empty());
    }

    #[test]
    fn test_schema_no_metadata_by_default() {
        let schema = test_schema();
        assert!(schema.field_metadata(0).is_none());
        assert!(schema.field_metadata_by_name("id").is_none());
    }

    #[test]
    fn test_schema_with_metadata_attaches_source_correlation() {
        let cols: Vec<Box<str>> = vec!["employee_id".into(), "$ck.employee_id".into()];
        let meta = vec![None, Some(FieldMetadata::source_correlation("employee_id"))];
        let schema = Schema::with_metadata(cols, meta);
        assert!(schema.field_metadata(0).is_none());
        let stamp = schema.field_metadata(1).expect("metadata attached");
        match stamp {
            FieldMetadata::SourceCorrelation { source_field } => {
                assert_eq!(source_field.as_ref(), "employee_id");
            }
            other => panic!("expected SourceCorrelation, got {other:?}"),
        }
        assert!(stamp.is_engine_stamped());
        match schema.field_metadata_by_name("$ck.employee_id") {
            Some(FieldMetadata::SourceCorrelation { source_field }) => {
                assert_eq!(source_field.as_ref(), "employee_id");
            }
            other => panic!("expected SourceCorrelation, got {other:?}"),
        }
    }

    #[test]
    fn test_schema_with_metadata_attaches_aggregate_group_index() {
        let cols: Vec<Box<str>> = vec!["region".into(), "$ck.aggregate.dept_totals".into()];
        let meta = vec![
            None,
            Some(FieldMetadata::aggregate_group_index("dept_totals")),
        ];
        let schema = Schema::with_metadata(cols, meta);
        let stamp = schema.field_metadata(1).expect("metadata attached");
        match stamp {
            FieldMetadata::AggregateGroupIndex { aggregate_name } => {
                assert_eq!(aggregate_name.as_ref(), "dept_totals");
            }
            other => panic!("expected AggregateGroupIndex, got {other:?}"),
        }
        assert!(stamp.is_engine_stamped());
    }

    /// Pins the in-memory size of [`FieldMetadata`] so a future variant
    /// addition that bloats the discriminant or payload surfaces here
    /// before it ships through every `Schema` and `Vec<Option<_>>`
    /// allocation in the workspace.
    #[test]
    fn test_field_metadata_size_regression_guard() {
        use std::mem::size_of;
        // Both variants carry one `Box<str>` (16 bytes on 64-bit) plus
        // the discriminant. 24 bytes (16 payload + 8 discriminant with
        // niche packing) is the natural shape; assert <= 24 to allow
        // the niche optimization to keep working without locking us in.
        assert!(
            size_of::<FieldMetadata>() <= 24,
            "FieldMetadata grew beyond 24 bytes ({} B)",
            size_of::<FieldMetadata>(),
        );
    }

    #[test]
    fn test_schema_builder_empty_build() {
        let schema = SchemaBuilder::new().build();
        assert_eq!(schema.column_count(), 0);
    }

    #[test]
    fn test_schema_builder_field_order_preserved() {
        let schema = SchemaBuilder::new().with_field("a").with_field("b").build();
        assert_eq!(&*schema.columns()[0], "a");
        assert_eq!(&*schema.columns()[1], "b");
    }

    #[test]
    fn test_schema_builder_extend_matches_manual() {
        let via_extend = SchemaBuilder::new().extend(["a", "b", "c"]).build();
        let via_chain = SchemaBuilder::new()
            .with_field("a")
            .with_field("b")
            .with_field("c")
            .build();
        assert_eq!(via_extend.columns(), via_chain.columns());
    }

    #[test]
    fn test_schema_builder_from_iterator_collect() {
        let schema = ["a", "b", "c"]
            .into_iter()
            .collect::<SchemaBuilder>()
            .build();
        assert_eq!(schema.column_count(), 3);
        assert_eq!(&*schema.columns()[0], "a");
        assert_eq!(&*schema.columns()[1], "b");
        assert_eq!(&*schema.columns()[2], "c");
    }

    #[test]
    fn test_schema_builder_index_lookup_post_build() {
        let schema = SchemaBuilder::new().with_field("x").with_field("y").build();
        assert_eq!(schema.index("y"), Some(1));
    }

    #[test]
    fn test_schema_builder_duplicate_column_last_wins() {
        let schema = SchemaBuilder::new()
            .with_field("a")
            .with_field("b")
            .with_field("a")
            .build();
        assert_eq!(schema.column_count(), 3);
        assert_eq!(schema.index("a"), Some(2));
    }

    #[test]
    fn test_schema_builder_with_field_meta_attaches_source_correlation() {
        let schema = SchemaBuilder::new()
            .with_field("employee_id")
            .with_field_meta(
                "$ck.employee_id",
                FieldMetadata::source_correlation("employee_id"),
            )
            .build();
        assert_eq!(schema.column_count(), 2);
        assert!(schema.field_metadata(0).is_none());
        match schema.field_metadata(1) {
            Some(FieldMetadata::SourceCorrelation { source_field }) => {
                assert_eq!(source_field.as_ref(), "employee_id");
            }
            other => panic!("expected SourceCorrelation, got {other:?}"),
        }
    }
}
