//! SecondaryIndex for window partition lookup.
//!
//! Maps composite group keys to Arena record positions. NaN values
//! in group_by keys are rejected as hard errors.
//!
//! `GroupByKey`, `GroupKeyError`, and `value_to_group_key()` live in
//! `clinker-record` (foundation crate) so `cxl::eval` can use them
//! for distinct without depending on `clinker-core`.

use std::collections::HashMap;

use clinker_record::RecordStorage;
use clinker_record::schema_def::FieldDef;

// Re-export from clinker-record for backwards compatibility
pub use clinker_record::{GroupByKey, GroupKeyError, value_to_group_key};

/// Secondary index: maps composite group keys to Arena record positions.
#[derive(Debug)]
pub struct SecondaryIndex {
    pub groups: HashMap<Vec<GroupByKey>, Vec<u64>>,
}

impl SecondaryIndex {
    /// Build index from Arena in a single pass.
    ///
    /// Iterates all arena records, extracts group_by fields, builds composite
    /// keys, and appends each record's position to its group's Vec.
    ///
    /// - NaN in any group_by field → `GroupKeyError::NanInGroupBy` (hard error).
    /// - Null in any group_by field → record excluded from all groups (debug log).
    pub fn build<S: RecordStorage>(
        storage: &S,
        group_by: &[String],
        schema_pins: &HashMap<String, FieldDef>,
    ) -> Result<Self, GroupKeyError> {
        let mut groups: HashMap<Vec<GroupByKey>, Vec<u64>> = HashMap::new();
        let record_count = storage.record_count();

        for pos in 0..record_count {
            let mut key = Vec::with_capacity(group_by.len());
            let mut skip = false;

            for field_name in group_by {
                let value = storage
                    .resolve_field(pos, field_name)
                    .unwrap_or(&clinker_record::NULL);

                let pin = schema_pins.get(field_name.as_str());
                match value_to_group_key(value, field_name, pin, pos)? {
                    Some(gk) => key.push(gk),
                    None => {
                        // Null value — exclude this record from all groups
                        skip = true;
                        break;
                    }
                }
            }

            if !skip {
                groups.entry(key).or_default().push(pos);
            }
        }

        Ok(SecondaryIndex { groups })
    }

    /// Look up a partition by its composite group key. Returns the position list.
    pub fn get(&self, key: &[GroupByKey]) -> Option<&[u64]> {
        self.groups.get(key).map(|v| v.as_slice())
    }

    /// Number of distinct groups in this index.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

/// Re-exported as `IndexError` for backwards compatibility within clinker-core.
pub type IndexError = GroupKeyError;

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{MinimalRecord, Schema, Value};
    use std::sync::Arc;

    /// Simple in-memory storage for testing SecondaryIndex without depending on Arena.
    struct TestStorage {
        schema: Arc<Schema>,
        records: Vec<MinimalRecord>,
    }

    impl TestStorage {
        fn new(columns: &[&str], rows: Vec<Vec<Value>>) -> Self {
            let schema = Arc::new(Schema::new(columns.iter().map(|c| (*c).into()).collect()));
            let records = rows
                .into_iter()
                .map(|fields| MinimalRecord::new(fields))
                .collect();
            TestStorage { schema, records }
        }
    }

    impl RecordStorage for TestStorage {
        fn resolve_field(&self, index: u64, name: &str) -> Option<&Value> {
            let col = self.schema.index(name)?;
            self.records.get(index as usize)?.get(col)
        }
        fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
            None
        }
        fn available_fields(&self, _: u64) -> Vec<&str> {
            self.schema.columns().iter().map(|s| &**s).collect()
        }
        fn record_count(&self) -> u64 {
            self.records.len() as u64
        }
    }

    // GroupByKey unit tests (eq/hash, int-float unify, neg zero, integer pin)
    // now live in clinker_record::group_key::tests.

    #[test]
    fn test_secondary_index_single_group_by() {
        let storage = TestStorage::new(
            &["dept", "amount"],
            vec![
                vec![Value::String("A".into()), Value::Integer(100)],
                vec![Value::String("B".into()), Value::Integer(200)],
                vec![Value::String("A".into()), Value::Integer(150)],
                vec![Value::String("C".into()), Value::Integer(300)],
                vec![Value::String("B".into()), Value::Integer(250)],
                vec![Value::String("A".into()), Value::Integer(175)],
                vec![Value::String("C".into()), Value::Integer(350)],
                vec![Value::String("B".into()), Value::Integer(225)],
                vec![Value::String("A".into()), Value::Integer(125)],
                vec![Value::String("C".into()), Value::Integer(275)],
            ],
        );

        let index = SecondaryIndex::build(&storage, &["dept".into()], &HashMap::new()).unwrap();

        assert_eq!(index.group_count(), 3);

        let key_a = vec![GroupByKey::Str("A".into())];
        let key_b = vec![GroupByKey::Str("B".into())];
        let key_c = vec![GroupByKey::Str("C".into())];

        assert_eq!(index.get(&key_a).unwrap().len(), 4); // rows 0,2,5,8
        assert_eq!(index.get(&key_b).unwrap().len(), 3); // rows 1,4,7
        assert_eq!(index.get(&key_c).unwrap().len(), 3); // rows 3,6,9

        // Total positions should equal total records
        let total: usize = index.groups.values().map(|v| v.len()).sum();
        assert_eq!(total, 10);
    }

    #[test]
    fn test_secondary_index_composite_key() {
        let storage = TestStorage::new(
            &["dept", "region"],
            vec![
                vec![Value::String("A".into()), Value::String("East".into())],
                vec![Value::String("A".into()), Value::String("West".into())],
                vec![Value::String("B".into()), Value::String("East".into())],
                vec![Value::String("A".into()), Value::String("East".into())],
            ],
        );

        let index =
            SecondaryIndex::build(&storage, &["dept".into(), "region".into()], &HashMap::new())
                .unwrap();

        assert_eq!(index.group_count(), 3); // (A,East), (A,West), (B,East)

        let key_ae = vec![GroupByKey::Str("A".into()), GroupByKey::Str("East".into())];
        assert_eq!(index.get(&key_ae).unwrap(), &[0, 3]);
    }

    #[test]
    fn test_secondary_index_nan_rejection() {
        let storage = TestStorage::new(
            &["amount"],
            vec![vec![Value::Float(1.0)], vec![Value::Float(f64::NAN)]],
        );

        let result = SecondaryIndex::build(&storage, &["amount".into()], &HashMap::new());
        assert!(result.is_err());
        match result.unwrap_err() {
            GroupKeyError::NanInGroupBy { field, row } => {
                assert_eq!(field, "amount");
                assert_eq!(row, 1);
            }
            other => panic!("Expected NanInGroupBy, got: {:?}", other),
        }
    }

    #[test]
    fn test_secondary_index_null_exclusion() {
        let storage = TestStorage::new(
            &["dept", "amount"],
            vec![
                vec![Value::String("A".into()), Value::Integer(100)],
                vec![Value::Null, Value::Integer(200)],
                vec![Value::String("A".into()), Value::Integer(150)],
                vec![Value::String("B".into()), Value::Integer(300)],
                vec![Value::Null, Value::Integer(250)],
            ],
        );

        let index = SecondaryIndex::build(&storage, &["dept".into()], &HashMap::new()).unwrap();

        // Total positions should be 3 (rows 0, 2, 3) — two null rows excluded
        let total: usize = index.groups.values().map(|v| v.len()).sum();
        assert_eq!(total, 3);
    }

    #[test]
    fn test_secondary_index_empty_arena() {
        let storage = TestStorage::new(&["dept"], vec![]);
        let index = SecondaryIndex::build(&storage, &["dept".into()], &HashMap::new()).unwrap();
        assert!(index.groups.is_empty());
    }

    #[test]
    fn test_secondary_index_all_nulls() {
        let storage = TestStorage::new(
            &["dept"],
            vec![vec![Value::Null], vec![Value::Null], vec![Value::Null]],
        );

        let index = SecondaryIndex::build(&storage, &["dept".into()], &HashMap::new()).unwrap();
        assert!(index.groups.is_empty());
    }
}
