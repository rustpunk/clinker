use ahash::RandomState;
use std::collections::HashMap;

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
}
