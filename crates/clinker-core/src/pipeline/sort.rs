//! Phase 1.5: Pointer sorting within partitions.
//!
//! Sorts each partition's `Vec<u32>` by looking up `sort_by` fields in the Arena.
//! Stable sort preserves insertion order for equal keys.

use std::cmp::Ordering;

use clinker_record::{RecordStorage, Value};

use crate::config::{NullOrder, SortField, SortOrder};

/// Sort a partition's position vector in-place by sort_by fields.
///
/// Stable sort preserves insertion order for equal keys.
/// Null handling applied per-field via `NullOrder`.
pub fn sort_partition<S: RecordStorage>(
    storage: &S,
    positions: &mut Vec<u32>,
    sort_by: &[SortField],
) {
    // First pass: drop nulls for any field with NullOrder::Drop
    for sf in sort_by {
        if sf.null_order == Some(NullOrder::Drop) {
            positions.retain(|&pos| {
                let val = storage.resolve_field(pos, &sf.field);
                !val.as_ref().is_none_or(|v| v.is_null())
            });
        }
    }

    // Sort
    positions.sort_by(|&a, &b| compare_records(storage, a, b, sort_by));
}

/// Check if a partition is already sorted (linear scan).
///
/// Returns true if all consecutive pairs are in the correct order.
pub fn is_sorted<S: RecordStorage>(
    storage: &S,
    positions: &[u32],
    sort_by: &[SortField],
) -> bool {
    positions
        .windows(2)
        .all(|pair| compare_records(storage, pair[0], pair[1], sort_by) != Ordering::Greater)
}

/// Compare two records by sort_by fields.
fn compare_records<S: RecordStorage>(
    storage: &S,
    a: u32,
    b: u32,
    sort_by: &[SortField],
) -> Ordering {
    for sf in sort_by {
        let va = storage.resolve_field(a, &sf.field);
        let vb = storage.resolve_field(b, &sf.field);
        let null_order = sf.null_order.unwrap_or(NullOrder::Last);

        let ord = compare_values_with_nulls(va.as_ref(), vb.as_ref(), sf.order, null_order);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Compare two optional values with null handling and sort direction.
fn compare_values_with_nulls(
    a: Option<&Value>,
    b: Option<&Value>,
    order: SortOrder,
    null_order: NullOrder,
) -> Ordering {
    let a_null = a.is_none() || a.is_some_and(|v| v.is_null());
    let b_null = b.is_none() || b.is_some_and(|v| v.is_null());

    match (a_null, b_null) {
        (true, true) => Ordering::Equal,
        (true, false) => match null_order {
            NullOrder::First => Ordering::Less,
            NullOrder::Last | NullOrder::Drop => Ordering::Greater,
        },
        (false, true) => match null_order {
            NullOrder::First => Ordering::Greater,
            NullOrder::Last | NullOrder::Drop => Ordering::Less,
        },
        (false, false) => {
            let base = compare_values(a.unwrap(), b.unwrap());
            match order {
                SortOrder::Asc => base,
                SortOrder::Desc => base.reverse(),
            }
        }
    }
}

/// Compare two non-null values using the same ordering as the evaluator.
fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Integer(x), Value::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Float(x), Value::Integer(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Date(x), Value::Date(y)) => x.cmp(y),
        (Value::DateTime(x), Value::DateTime(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => Ordering::Equal, // incomparable types treated as equal
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::{MinimalRecord, Schema, Value};
    use std::sync::Arc;

    struct TestStorage {
        schema: Arc<Schema>,
        records: Vec<MinimalRecord>,
    }

    impl TestStorage {
        fn new(columns: &[&str], rows: Vec<Vec<Value>>) -> Self {
            let schema = Arc::new(Schema::new(columns.iter().map(|c| (*c).into()).collect()));
            let records = rows.into_iter().map(MinimalRecord::new).collect();
            TestStorage { schema, records }
        }
    }

    impl RecordStorage for TestStorage {
        fn resolve_field(&self, index: u32, name: &str) -> Option<Value> {
            let col = self.schema.index(name)?;
            self.records.get(index as usize)?.get(col).cloned()
        }
        fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<Value> { None }
        fn available_fields(&self, _: u32) -> Vec<&str> {
            self.schema.columns().iter().map(|s| &**s).collect()
        }
        fn record_count(&self) -> u32 { self.records.len() as u32 }
    }

    fn sf(field: &str, order: SortOrder, null_order: Option<NullOrder>) -> SortField {
        SortField { field: field.into(), order, null_order }
    }

    #[test]
    fn test_sort_partition_ascending() {
        let storage = TestStorage::new(&["amount"], vec![
            vec![Value::Integer(30)],
            vec![Value::Integer(10)],
            vec![Value::Integer(50)],
            vec![Value::Integer(20)],
            vec![Value::Integer(40)],
        ]);
        let mut positions: Vec<u32> = vec![0, 1, 2, 3, 4];
        sort_partition(&storage, &mut positions, &[sf("amount", SortOrder::Asc, None)]);
        assert_eq!(positions, vec![1, 3, 0, 4, 2]); // 10, 20, 30, 40, 50
    }

    #[test]
    fn test_sort_partition_descending() {
        let storage = TestStorage::new(&["amount"], vec![
            vec![Value::Integer(30)],
            vec![Value::Integer(10)],
            vec![Value::Integer(50)],
            vec![Value::Integer(20)],
            vec![Value::Integer(40)],
        ]);
        let mut positions: Vec<u32> = vec![0, 1, 2, 3, 4];
        sort_partition(&storage, &mut positions, &[sf("amount", SortOrder::Desc, None)]);
        assert_eq!(positions, vec![2, 4, 0, 3, 1]); // 50, 40, 30, 20, 10
    }

    #[test]
    fn test_sort_null_first() {
        let storage = TestStorage::new(&["amount"], vec![
            vec![Value::Integer(30)],
            vec![Value::Null],
            vec![Value::Integer(10)],
        ]);
        let mut positions: Vec<u32> = vec![0, 1, 2];
        sort_partition(&storage, &mut positions, &[sf("amount", SortOrder::Asc, Some(NullOrder::First))]);
        assert_eq!(positions[0], 1); // null first
        assert_eq!(positions[1], 2); // 10
        assert_eq!(positions[2], 0); // 30
    }

    #[test]
    fn test_sort_null_last() {
        let storage = TestStorage::new(&["amount"], vec![
            vec![Value::Integer(30)],
            vec![Value::Null],
            vec![Value::Integer(10)],
        ]);
        let mut positions: Vec<u32> = vec![0, 1, 2];
        sort_partition(&storage, &mut positions, &[sf("amount", SortOrder::Asc, Some(NullOrder::Last))]);
        assert_eq!(positions[0], 2); // 10
        assert_eq!(positions[1], 0); // 30
        assert_eq!(positions[2], 1); // null last
    }

    #[test]
    fn test_sort_null_drop() {
        let storage = TestStorage::new(&["amount"], vec![
            vec![Value::Integer(30)],
            vec![Value::Null],
            vec![Value::Integer(10)],
        ]);
        let mut positions: Vec<u32> = vec![0, 1, 2];
        sort_partition(&storage, &mut positions, &[sf("amount", SortOrder::Asc, Some(NullOrder::Drop))]);
        assert_eq!(positions.len(), 2);
        assert!(!positions.contains(&1)); // null dropped
    }

    #[test]
    fn test_sort_presorted_skip() {
        let storage = TestStorage::new(&["amount"], vec![
            vec![Value::Integer(10)],
            vec![Value::Integer(20)],
            vec![Value::Integer(30)],
        ]);
        let positions: Vec<u32> = vec![0, 1, 2];
        assert!(is_sorted(&storage, &positions, &[sf("amount", SortOrder::Asc, None)]));
    }

    #[test]
    fn test_sort_partition_composite() {
        let storage = TestStorage::new(&["dept", "amount"], vec![
            vec![Value::String("B".into()), Value::Integer(200)],
            vec![Value::String("A".into()), Value::Integer(300)],
            vec![Value::String("A".into()), Value::Integer(100)],
            vec![Value::String("B".into()), Value::Integer(100)],
        ]);
        let mut positions: Vec<u32> = vec![0, 1, 2, 3];
        sort_partition(&storage, &mut positions, &[
            sf("dept", SortOrder::Asc, None),
            sf("amount", SortOrder::Desc, None),
        ]);
        // A first (sorted by dept asc), then within A: 300, 100 (amount desc)
        assert_eq!(positions, vec![1, 2, 0, 3]); // A/300, A/100, B/200, B/100
    }
}
