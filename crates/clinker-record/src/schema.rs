use ahash::RandomState;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct Schema {
    columns: Vec<Box<str>>,
    index: HashMap<Box<str>, usize, RandomState>,
}

impl Schema {
    pub fn new(columns: Vec<Box<str>>) -> Self {
        let hasher = RandomState::with_seeds(1, 2, 3, 4);
        let mut index = HashMap::with_capacity_and_hasher(columns.len(), hasher);
        for (i, name) in columns.iter().enumerate() {
            index.insert(name.clone(), i);
        }
        Self { columns, index }
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
        }
    }

    /// Append a single column name and return `self` for chaining.
    pub fn with_field(mut self, name: impl Into<Box<str>>) -> Self {
        self.columns.push(name.into());
        self
    }

    /// Append every column yielded by `iter`, converting each item via `Into<Box<str>>`.
    pub fn extend<I, S>(mut self, iter: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<Box<str>>,
    {
        for s in iter {
            self.columns.push(s.into());
        }
        self
    }

    /// Finalize the builder into an `Arc<Schema>`. Consumes `self`.
    pub fn build(self) -> Arc<Schema> {
        Arc::new(Schema::new(self.columns))
    }
}

impl<S: Into<Box<str>>> FromIterator<S> for SchemaBuilder {
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        Self {
            columns: iter.into_iter().map(Into::into).collect(),
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
}
