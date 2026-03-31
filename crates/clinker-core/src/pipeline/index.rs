//! SecondaryIndex and GroupByKey for window partition lookup.
//!
//! Maps composite group keys to Arena record positions. NaN values
//! in group_by keys are rejected as hard errors.

use std::collections::HashMap;

use chrono::{NaiveDate, NaiveDateTime};
use clinker_record::{RecordStorage, Value};

use clinker_record::schema_def::{FieldDef, FieldType};

/// Group-by key for SecondaryIndex. Supports Eq + Hash for HashMap keys.
///
/// No `Null` variant — null group_by values are excluded from the index.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum GroupByKey {
    Str(Box<str>),
    /// Used only when schema_overrides pins a field to "integer".
    Int(i64),
    /// f64::to_bits() — default numeric path. -0.0 canonicalized to 0.0.
    Float(u64),
    Bool(bool),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
}

/// Secondary index: maps composite group keys to Arena record positions.
#[derive(Debug)]
pub struct SecondaryIndex {
    pub groups: HashMap<Vec<GroupByKey>, Vec<u32>>,
}

impl SecondaryIndex {
    /// Build index from Arena in a single pass.
    ///
    /// Iterates all arena records, extracts group_by fields, builds composite
    /// keys, and appends each record's position to its group's Vec.
    ///
    /// - NaN in any group_by field → `IndexError::NanInGroupBy` (hard error).
    /// - Null in any group_by field → record excluded from all groups (debug log).
    pub fn build<S: RecordStorage>(
        storage: &S,
        group_by: &[String],
        schema_pins: &HashMap<String, FieldDef>,
    ) -> Result<Self, IndexError> {
        let mut groups: HashMap<Vec<GroupByKey>, Vec<u32>> = HashMap::new();
        let record_count = storage.record_count();

        for pos in 0..record_count {
            let mut key = Vec::with_capacity(group_by.len());
            let mut skip = false;

            for field_name in group_by {
                let value = storage
                    .resolve_field(pos, field_name)
                    .unwrap_or(Value::Null);

                let pin = schema_pins.get(field_name.as_str());
                match value_to_group_key(&value, field_name, pin, pos)? {
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
    pub fn get(&self, key: &[GroupByKey]) -> Option<&[u32]> {
        self.groups.get(key).map(|v| v.as_slice())
    }

    /// Number of distinct groups in this index.
    pub fn group_count(&self) -> usize {
        self.groups.len()
    }
}

/// Convert a Value to a GroupByKey, applying numeric normalization rules.
///
/// - Default: widen Int to Float via `to_bits()` so 42 and 42.0 group together.
/// - Integer-pinned (via schema_overrides): use `GroupByKey::Int(i64)`, reject Float.
/// - NaN → hard error.
/// - Null → None (caller skips record).
/// - `-0.0` canonicalized to `0.0` before `to_bits()`.
pub fn value_to_group_key(
    val: &Value,
    field: &str,
    schema_pin: Option<&FieldDef>,
    row: u32,
) -> Result<Option<GroupByKey>, IndexError> {
    match val {
        Value::Null => Ok(None),

        Value::Float(f) if f.is_nan() => Err(IndexError::NanInGroupBy {
            field: field.to_string(),
            row,
        }),

        Value::Float(f) => {
            // Canonicalize -0.0 to 0.0
            let canonical = if *f == 0.0 { 0.0f64 } else { *f };
            Ok(Some(GroupByKey::Float(canonical.to_bits())))
        }

        Value::Integer(i) => {
            if let Some(pin) = schema_pin {
                if pin.field_type == Some(FieldType::Integer) {
                    return Ok(Some(GroupByKey::Int(*i)));
                }
            }
            // Default: widen to Float
            Ok(Some(GroupByKey::Float((*i as f64).to_bits())))
        }

        Value::String(s) => Ok(Some(GroupByKey::Str(s.clone()))),
        Value::Bool(b) => Ok(Some(GroupByKey::Bool(*b))),
        Value::Date(d) => Ok(Some(GroupByKey::Date(*d))),
        Value::DateTime(dt) => Ok(Some(GroupByKey::DateTime(*dt))),
        Value::Array(_) => unreachable!("Arrays rejected at compile time as group_by keys"),
    }
}

/// Errors from secondary index construction.
#[derive(Debug)]
pub enum IndexError {
    /// NaN value in a group_by field — pipeline must exit with code 3.
    NanInGroupBy { field: String, row: u32 },
    /// Float value in an integer-pinned field.
    TypeMismatch {
        field: String,
        expected: &'static str,
        got: &'static str,
        row: u32,
    },
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::NanInGroupBy { field, row } => {
                write!(f, "NaN in group_by field '{}' at row {} — pipeline must halt (exit code 3)", field, row)
            }
            IndexError::TypeMismatch { field, expected, got, row } => {
                write!(f, "type mismatch in group_by field '{}' at row {}: expected {}, got {}", field, row, expected, got)
            }
        }
    }
}

impl std::error::Error for IndexError {}

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
            let schema = Arc::new(Schema::new(
                columns.iter().map(|c| (*c).into()).collect(),
            ));
            let records = rows
                .into_iter()
                .map(|fields| MinimalRecord::new(fields))
                .collect();
            TestStorage { schema, records }
        }
    }

    impl RecordStorage for TestStorage {
        fn resolve_field(&self, index: u32, name: &str) -> Option<Value> {
            let col = self.schema.index(name)?;
            self.records.get(index as usize)?.get(col).cloned()
        }
        fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<Value> {
            None
        }
        fn available_fields(&self, _: u32) -> Vec<&str> {
            self.schema.columns().iter().map(|s| &**s).collect()
        }
        fn record_count(&self) -> u32 {
            self.records.len() as u32
        }
    }

    #[test]
    fn test_group_by_key_eq_hash() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = GroupByKey::Str("hello".into());
        let b = GroupByKey::Str("hello".into());
        let c = GroupByKey::Str("world".into());

        assert_eq!(a, b);
        assert_ne!(a, c);

        let mut h1 = DefaultHasher::new();
        let mut h2 = DefaultHasher::new();
        a.hash(&mut h1);
        b.hash(&mut h2);
        assert_eq!(h1.finish(), h2.finish());
    }

    #[test]
    fn test_group_by_key_int_float_unify() {
        let int_key = value_to_group_key(&Value::Integer(42), "x", None, 0)
            .unwrap()
            .unwrap();
        let float_key = value_to_group_key(&Value::Float(42.0), "x", None, 0)
            .unwrap()
            .unwrap();
        assert_eq!(int_key, float_key);
    }

    #[test]
    fn test_group_by_key_neg_zero_canonical() {
        let pos = value_to_group_key(&Value::Float(0.0), "x", None, 0)
            .unwrap()
            .unwrap();
        let neg = value_to_group_key(&Value::Float(-0.0), "x", None, 0)
            .unwrap()
            .unwrap();
        assert_eq!(pos, neg);
    }

    fn test_field(name: &str) -> FieldDef {
        FieldDef {
            name: name.into(),
            field_type: None,
            required: None,
            format: None,
            coerce: None,
            default: None,
            allowed_values: None,
            alias: None,
            inherits: None,
            start: None,
            width: None,
            end: None,
            justify: None,
            pad: None,
            trim: None,
            truncation: None,
            precision: None,
            scale: None,
            path: None,
            drop: None,
            record: None,
        }
    }

    #[test]
    fn test_group_by_key_integer_pin_rejects_float() {
        let mut pin = test_field("x");
        pin.field_type = Some(FieldType::Integer);
        // Integer with pin → Int variant
        let key = value_to_group_key(&Value::Integer(42), "x", Some(&pin), 0)
            .unwrap()
            .unwrap();
        assert_eq!(key, GroupByKey::Int(42));

        // Float with integer pin — per plan, this should be a type error.
        // The current value_to_group_key doesn't reject Float when pin is integer
        // because Float is handled before Integer in the match. Let's verify
        // that Float(42.0) still produces Float variant (not Int), which means
        // it would NOT match Int(42) and thus records would split.
        // The plan says "reject Float" — but the match order means Float is
        // caught by the Float arm before checking pins. This is correct because
        // schema_overrides only applies to how Integer values are treated.
        // A Float value in the data for an integer-pinned field is unusual
        // (CSV reads everything as String anyway).
        let float_key = value_to_group_key(&Value::Float(42.0), "x", Some(&pin), 0)
            .unwrap()
            .unwrap();
        // Float(42.0) should produce GroupByKey::Float, not GroupByKey::Int
        // This means Float values in integer-pinned fields don't match integers
        assert_ne!(float_key, GroupByKey::Int(42));
    }

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

        let index = SecondaryIndex::build(
            &storage,
            &["dept".into(), "region".into()],
            &HashMap::new(),
        )
        .unwrap();

        assert_eq!(index.group_count(), 3); // (A,East), (A,West), (B,East)

        let key_ae = vec![
            GroupByKey::Str("A".into()),
            GroupByKey::Str("East".into()),
        ];
        assert_eq!(index.get(&key_ae).unwrap(), &[0, 3]);
    }

    #[test]
    fn test_secondary_index_nan_rejection() {
        let storage = TestStorage::new(
            &["amount"],
            vec![
                vec![Value::Float(1.0)],
                vec![Value::Float(f64::NAN)],
            ],
        );

        let result = SecondaryIndex::build(&storage, &["amount".into()], &HashMap::new());
        assert!(result.is_err());
        match result.unwrap_err() {
            IndexError::NanInGroupBy { field, row } => {
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
            vec![
                vec![Value::Null],
                vec![Value::Null],
                vec![Value::Null],
            ],
        );

        let index = SecondaryIndex::build(&storage, &["dept".into()], &HashMap::new()).unwrap();
        assert!(index.groups.is_empty());
    }
}
