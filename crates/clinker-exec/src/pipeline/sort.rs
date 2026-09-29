//! Phase 1.5: Pointer sorting within partitions.
//!
//! Sorts each partition's `Vec<u32>` by looking up `sort_by` fields in the Arena.
//! Stable sort preserves insertion order for equal keys.

use std::cmp::Ordering;

use clinker_record::{Record, RecordStorage, Value};

use clinker_plan::config::{NullOrder, OrderField, SortField, SortOrder};

use crate::pipeline::sort_key::{
    compare_authored_keys, compare_authored_values, compare_authored_values_with_nulls,
};

/// Sort a window partition's position vector in place by its `sort_by`
/// fields.
///
/// Every position stays: the fields are placement-only, so nulls go first
/// or last and no row leaves the partition. The sort is stable, so equal
/// keys keep arrival order.
pub fn sort_partition<S: RecordStorage>(
    storage: &S,
    positions: &mut [u64],
    sort_by: &[OrderField],
) {
    positions.sort_by(|&a, &b| compare_records(storage, a, b, sort_by));
}

/// Check if a partition is already sorted (linear scan).
///
/// Returns true if all consecutive pairs are in the correct order.
pub fn is_sorted<S: RecordStorage>(storage: &S, positions: &[u64], sort_by: &[OrderField]) -> bool {
    positions
        .windows(2)
        .all(|pair| compare_records(storage, pair[0], pair[1], sort_by) != Ordering::Greater)
}

/// Compare two records by sort_by fields.
fn compare_records<S: RecordStorage>(
    storage: &S,
    a: u64,
    b: u64,
    sort_by: &[OrderField],
) -> Ordering {
    for sf in sort_by {
        let va = storage.resolve_field(a, &sf.field);
        let vb = storage.resolve_field(b, &sf.field);

        let ord = compare_values_with_nulls(va, vb, sf.order, sf.null_order.into());
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Compare two records directly by sort_by fields.
///
/// Unlike `compare_records` which uses RecordStorage + position indices,
/// this operates on Record references — used by SortBuffer for in-memory sorting.
pub fn compare_records_by_fields(a: &Record, b: &Record, sort_by: &[SortField]) -> Ordering {
    compare_authored_keys(a, b, sort_by)
}

/// Compare two optional values with null handling and sort direction.
pub fn compare_values_with_nulls(
    a: Option<&Value>,
    b: Option<&Value>,
    order: SortOrder,
    null_order: NullOrder,
) -> Ordering {
    compare_authored_values_with_nulls(a, b, order, null_order)
}

/// Compare two non-null values in ascending order under the one value order,
/// [`clinker_record::order::compare`], which every sort shares.
///
/// This is not yet the order of CXL's comparison operators: `<` and `>` in an
/// expression still decide NaN and mixed-type operands their own way.
pub fn compare_values(a: &Value, b: &Value) -> Ordering {
    compare_authored_values(a, b)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_plan::config::NullPlacement;
    use clinker_record::owned_storage::SharedStorage;
    use clinker_record::{MinimalRecord, Schema, Value};
    use std::sync::Arc;

    struct TestStorage {
        schema: SharedStorage<Schema>,
        records: Vec<MinimalRecord>,
    }

    impl TestStorage {
        fn new(columns: &[&str], rows: Vec<Vec<Value>>) -> Self {
            let schema = SharedStorage::from_arc(Arc::new(Schema::new(
                columns.iter().map(|c| (*c).into()).collect(),
            )));
            let records = rows.into_iter().map(MinimalRecord::new).collect();
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

    fn sf(field: &str, order: SortOrder, null_order: NullPlacement) -> OrderField {
        OrderField {
            field: field.into(),
            order,
            null_order,
        }
    }

    #[test]
    fn test_sort_partition_ascending() {
        let storage = TestStorage::new(
            &["amount"],
            vec![
                vec![Value::Integer(30)],
                vec![Value::Integer(10)],
                vec![Value::Integer(50)],
                vec![Value::Integer(20)],
                vec![Value::Integer(40)],
            ],
        );
        let mut positions: Vec<u64> = vec![0, 1, 2, 3, 4];
        sort_partition(
            &storage,
            &mut positions,
            &[sf("amount", SortOrder::Asc, NullPlacement::Last)],
        );
        assert_eq!(positions, vec![1, 3, 0, 4, 2]); // 10, 20, 30, 40, 50
    }

    #[test]
    fn test_sort_partition_descending() {
        let storage = TestStorage::new(
            &["amount"],
            vec![
                vec![Value::Integer(30)],
                vec![Value::Integer(10)],
                vec![Value::Integer(50)],
                vec![Value::Integer(20)],
                vec![Value::Integer(40)],
            ],
        );
        let mut positions: Vec<u64> = vec![0, 1, 2, 3, 4];
        sort_partition(
            &storage,
            &mut positions,
            &[sf("amount", SortOrder::Desc, NullPlacement::Last)],
        );
        assert_eq!(positions, vec![2, 4, 0, 3, 1]); // 50, 40, 30, 20, 10
    }

    #[test]
    fn test_sort_null_first() {
        let storage = TestStorage::new(
            &["amount"],
            vec![
                vec![Value::Integer(30)],
                vec![Value::Null],
                vec![Value::Integer(10)],
            ],
        );
        let mut positions: Vec<u64> = vec![0, 1, 2];
        sort_partition(
            &storage,
            &mut positions,
            &[sf("amount", SortOrder::Asc, NullPlacement::First)],
        );
        assert_eq!(positions[0], 1); // null first
        assert_eq!(positions[1], 2); // 10
        assert_eq!(positions[2], 0); // 30
    }

    #[test]
    fn test_sort_null_last() {
        let storage = TestStorage::new(
            &["amount"],
            vec![
                vec![Value::Integer(30)],
                vec![Value::Null],
                vec![Value::Integer(10)],
            ],
        );
        let mut positions: Vec<u64> = vec![0, 1, 2];
        sort_partition(
            &storage,
            &mut positions,
            &[sf("amount", SortOrder::Asc, NullPlacement::Last)],
        );
        assert_eq!(positions[0], 2); // 10
        assert_eq!(positions[1], 0); // 30
        assert_eq!(positions[2], 1); // null last
    }

    /// A window partition keeps every row: `null_order` only decides
    /// whether null keys sort before or after the values, in either
    /// direction, and nulls keep their arrival order among themselves.
    #[test]
    fn window_partition_places_nulls_by_null_order() {
        // Positions:      0        1     2       3     4
        let values = vec![
            Value::Integer(30),
            Value::Null,
            Value::Integer(10),
            Value::Null,
            Value::Integer(20),
        ];
        let storage = TestStorage::new(&["amount"], values.into_iter().map(|v| vec![v]).collect());
        let cases = [
            (SortOrder::Asc, NullPlacement::First, vec![1, 3, 2, 4, 0]),
            (SortOrder::Asc, NullPlacement::Last, vec![2, 4, 0, 1, 3]),
            (SortOrder::Desc, NullPlacement::First, vec![1, 3, 0, 4, 2]),
            (SortOrder::Desc, NullPlacement::Last, vec![0, 4, 2, 1, 3]),
        ];
        for (order, placement, expected) in cases {
            let sort_by = [sf("amount", order, placement)];
            let mut positions: Vec<u64> = (0..5).collect();
            sort_partition(&storage, &mut positions, &sort_by);
            assert_eq!(positions, expected, "{order:?} {placement:?}");
            assert!(is_sorted(&storage, &positions, &sort_by));
        }
    }

    #[test]
    fn test_sort_presorted_skip() {
        let storage = TestStorage::new(
            &["amount"],
            vec![
                vec![Value::Integer(10)],
                vec![Value::Integer(20)],
                vec![Value::Integer(30)],
            ],
        );
        let positions: Vec<u64> = vec![0, 1, 2];
        assert!(is_sorted(
            &storage,
            &positions,
            &[sf("amount", SortOrder::Asc, NullPlacement::Last)]
        ));
    }

    #[test]
    fn test_sort_partition_composite() {
        let storage = TestStorage::new(
            &["dept", "amount"],
            vec![
                vec![Value::String("B".into()), Value::Integer(200)],
                vec![Value::String("A".into()), Value::Integer(300)],
                vec![Value::String("A".into()), Value::Integer(100)],
                vec![Value::String("B".into()), Value::Integer(100)],
            ],
        );
        let mut positions: Vec<u64> = vec![0, 1, 2, 3];
        sort_partition(
            &storage,
            &mut positions,
            &[
                sf("dept", SortOrder::Asc, NullPlacement::Last),
                sf("amount", SortOrder::Desc, NullPlacement::Last),
            ],
        );
        // A first (sorted by dept asc), then within A: 300, 100 (amount desc)
        assert_eq!(positions, vec![1, 2, 0, 3]); // A/300, A/100, B/200, B/100
    }

    /// A window partition holding NaN, signed zeros and integers mixed with
    /// floats sorts in the one value order. A comparator that called a NaN
    /// or a cross-type pair equal is not an order at all: the stable sort
    /// then leaves values stranded on either side of a NaN, and on a long
    /// enough slice `sort_by` is allowed to panic.
    #[test]
    fn window_partition_sort_is_total() {
        // Positions:            0          1                 2             3
        let values = vec![
            Value::Float(1.5),
            Value::Float(f64::NAN),
            Value::Float(-0.0),
            Value::Integer(2),
            // 4                  5                   6              7
            Value::Float(0.0),
            Value::Float(-f64::NAN),
            Value::Integer(-3),
            Value::Float(2.0),
            // 8                                   9
            Value::Float(f64::INFINITY),
            Value::Integer(9_007_199_254_740_993),
            // 10
            Value::Float(9_007_199_254_740_992.0),
        ];
        let storage = TestStorage::new(&["v"], values.into_iter().map(|v| vec![v]).collect());
        let all: Vec<u64> = (0..11).collect();

        let mut ascending = all.clone();
        sort_partition(
            &storage,
            &mut ascending,
            &[sf("v", SortOrder::Asc, NullPlacement::Last)],
        );
        // -3, -0.0 ~ 0.0 (arrival), 1.5, 2 ~ 2.0 (arrival), 2^53, 2^53 + 1,
        // +inf, NaN ~ -NaN (arrival).
        assert_eq!(ascending, vec![6, 2, 4, 0, 3, 7, 10, 9, 8, 1, 5]);

        let mut descending = all.clone();
        sort_partition(
            &storage,
            &mut descending,
            &[sf("v", SortOrder::Desc, NullPlacement::Last)],
        );
        assert_eq!(descending, vec![1, 5, 8, 9, 10, 3, 7, 0, 2, 4, 6]);

        // A partition long enough for the sort's run detection, cycling
        // through the same kinds of value. The oracle ranks every NaN above
        // every number and otherwise compares the numbers as exact f64s (all
        // of them are), with -0.0 read as 0.0.
        let cycle = |i: usize| match i % 7 {
            0 => Value::Float(f64::NAN),
            1 => Value::Integer((i % 13) as i64 - 6),
            2 => Value::Float(-0.0),
            3 => Value::Float((i % 11) as f64 - 5.5),
            4 => Value::Float(-f64::NAN),
            5 => Value::Float(0.0),
            _ => Value::Integer((i % 5) as i64),
        };
        let long: Vec<Value> = (0..400).map(cycle).collect();
        let rank = |v: &Value| -> (u8, f64) {
            match v {
                Value::Float(f) if f.is_nan() => (1, 0.0),
                Value::Float(f) => (0, *f + 0.0),
                Value::Integer(i) => (0, *i as f64),
                other => panic!("unexpected {other:?}"),
            }
        };
        let storage = TestStorage::new(&["v"], long.iter().cloned().map(|v| vec![v]).collect());
        let mut positions: Vec<u64> = (0..400).collect();
        sort_partition(
            &storage,
            &mut positions,
            &[sf("v", SortOrder::Asc, NullPlacement::Last)],
        );
        for pair in positions.windows(2) {
            let (a, b) = (rank(&long[pair[0] as usize]), rank(&long[pair[1] as usize]));
            let ordering = a.0.cmp(&b.0).then(a.1.total_cmp(&b.1));
            assert_ne!(ordering, Ordering::Greater, "out of order at {pair:?}");
            if ordering == Ordering::Equal {
                assert!(
                    pair[0] < pair[1],
                    "equal values lost arrival order at {pair:?}"
                );
            }
        }
    }
}
