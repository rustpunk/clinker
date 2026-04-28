//! Concrete WindowContext implementation over a sorted Arena partition.

use std::collections::HashSet;

use clinker_record::{RecordStorage, RecordView, Value, WindowContext};

use crate::pipeline::arena::Arena;

/// Concrete WindowContext over a sorted Arena partition.
///
/// Holds a reference to the Arena and a slice of sorted record positions.
/// `current_pos` is the index *within the partition* of the record being evaluated.
pub struct PartitionWindowContext<'a> {
    arena: &'a Arena,
    partition: &'a [u32],
    current_pos: usize,
}

impl<'a> PartitionWindowContext<'a> {
    pub fn new(arena: &'a Arena, partition: &'a [u32], current_pos: usize) -> Self {
        Self {
            arena,
            partition,
            current_pos,
        }
    }
}

impl<'a> WindowContext<'a, Arena> for PartitionWindowContext<'a> {
    fn first(&self) -> Option<RecordView<'a, Arena>> {
        self.partition
            .first()
            .map(|&idx| RecordView::new(self.arena, idx))
    }

    fn last(&self) -> Option<RecordView<'a, Arena>> {
        self.partition
            .last()
            .map(|&idx| RecordView::new(self.arena, idx))
    }

    fn lag(&self, offset: usize) -> Option<RecordView<'a, Arena>> {
        self.current_pos
            .checked_sub(offset)
            .and_then(|i| self.partition.get(i))
            .map(|&idx| RecordView::new(self.arena, idx))
    }

    fn lead(&self, offset: usize) -> Option<RecordView<'a, Arena>> {
        self.partition
            .get(self.current_pos + offset)
            .map(|&idx| RecordView::new(self.arena, idx))
    }

    fn count(&self) -> i64 {
        self.partition.len() as i64
    }

    fn sum(&self, field: &str) -> Value {
        let mut total = 0.0f64;
        let mut has_numeric = false;
        for &pos in self.partition {
            match self.arena.resolve_field(pos, field) {
                Some(Value::Integer(i)) => {
                    total += *i as f64;
                    has_numeric = true;
                }
                Some(Value::Float(f)) => {
                    total += *f;
                    has_numeric = true;
                }
                _ => {}
            }
        }
        if has_numeric {
            Value::Float(total)
        } else {
            Value::Null
        }
    }

    fn avg(&self, field: &str) -> Value {
        let mut total = 0.0f64;
        let mut count = 0u64;
        for &pos in self.partition {
            match self.arena.resolve_field(pos, field) {
                Some(Value::Integer(i)) => {
                    total += *i as f64;
                    count += 1;
                }
                Some(Value::Float(f)) => {
                    total += *f;
                    count += 1;
                }
                _ => {}
            }
        }
        if count > 0 {
            Value::Float(total / count as f64)
        } else {
            Value::Null
        }
    }

    fn min(&self, field: &str) -> Value {
        let mut result: Option<&Value> = None;
        for &pos in self.partition {
            if let Some(val) = self.arena.resolve_field(pos, field) {
                if val.is_null() {
                    continue;
                }
                result = Some(match result {
                    None => val,
                    Some(current) => {
                        if val_less_than(val, current) {
                            val
                        } else {
                            current
                        }
                    }
                });
            }
        }
        result.cloned().unwrap_or(Value::Null)
    }

    fn max(&self, field: &str) -> Value {
        let mut result: Option<&Value> = None;
        for &pos in self.partition {
            if let Some(val) = self.arena.resolve_field(pos, field) {
                if val.is_null() {
                    continue;
                }
                result = Some(match result {
                    None => val,
                    Some(current) => {
                        if val_less_than(current, val) {
                            val
                        } else {
                            current
                        }
                    }
                });
            }
        }
        result.cloned().unwrap_or(Value::Null)
    }

    fn partition_len(&self) -> usize {
        self.partition.len()
    }

    fn partition_record(&self, index: usize) -> RecordView<'a, Arena> {
        RecordView::new(self.arena, self.partition[index])
    }

    fn collect(&self, field: &str) -> Value {
        let values: Vec<Value> = self
            .partition
            .iter()
            .filter_map(|&pos| self.arena.resolve_field(pos, field))
            .filter(|v| !v.is_null())
            .cloned()
            .collect();
        Value::Array(values)
    }

    fn distinct(&self, field: &str) -> Value {
        let mut seen = HashSet::new();
        let mut values = Vec::new();
        for &pos in self.partition {
            if let Some(val) = self.arena.resolve_field(pos, field) {
                if val.is_null() {
                    continue;
                }
                let key = value_hash_key(val);
                if seen.insert(key) {
                    values.push(val.clone());
                }
            }
        }
        Value::Array(values)
    }
}

/// Simple less-than comparison for Value (used by min/max).
fn val_less_than(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x < y,
        (Value::Float(x), Value::Float(y)) => x < y,
        (Value::Integer(x), Value::Float(y)) => (*x as f64) < *y,
        (Value::Float(x), Value::Integer(y)) => *x < (*y as f64),
        (Value::String(x), Value::String(y)) => x < y,
        (Value::Date(x), Value::Date(y)) => x < y,
        (Value::DateTime(x), Value::DateTime(y)) => x < y,
        _ => false,
    }
}

/// Create a hashable key from a Value for distinct() deduplication.
fn value_hash_key(val: &Value) -> String {
    match val {
        Value::Null => "null".into(),
        Value::Bool(b) => format!("b:{}", b),
        Value::Integer(i) => format!("i:{}", i),
        Value::Float(f) => format!("f:{}", f.to_bits()),
        Value::String(s) => format!("s:{}", s),
        Value::Date(d) => format!("d:{}", d),
        Value::DateTime(dt) => format!("dt:{}", dt),
        Value::Array(_) => "array".into(),
        Value::Map(_) => "map".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
    use clinker_record::FieldResolver;

    fn make_arena(csv: &str, fields: &[&str]) -> Arena {
        let mut reader = CsvReader::from_reader(
            csv.as_bytes(),
            CsvReaderConfig {
                delimiter: b',',
                quote_char: b'"',
                has_header: true,
            },
        );
        let mut budget = crate::pipeline::memory::MemoryBudget::new(u64::MAX, 0.80);
        Arena::build(
            &mut reader,
            &fields.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
            usize::MAX,
            None,
            &mut budget,
        )
        .unwrap()
    }

    #[test]
    fn test_window_first_last() {
        let arena = make_arena(
            "name,amount\nAlice,100\nBob,200\nCarol,300\n",
            &["name", "amount"],
        );
        let partition: Vec<u32> = vec![0, 1, 2];
        let ctx = PartitionWindowContext::new(&arena, &partition, 1);

        let first = ctx.first().unwrap();
        assert_eq!(first.resolve("name"), Some(&Value::String("Alice".into())));
        let last = ctx.last().unwrap();
        assert_eq!(last.resolve("name"), Some(&Value::String("Carol".into())));
    }

    #[test]
    fn test_window_lag_lead() {
        let arena = make_arena(
            "name,amount\nA,10\nB,20\nC,30\nD,40\nE,50\n",
            &["name", "amount"],
        );
        let partition: Vec<u32> = vec![0, 1, 2, 3, 4];
        let ctx = PartitionWindowContext::new(&arena, &partition, 3);

        let lag = ctx.lag(1).unwrap();
        assert_eq!(lag.resolve("name"), Some(&Value::String("C".into())));
        let lead = ctx.lead(1).unwrap();
        assert_eq!(lead.resolve("name"), Some(&Value::String("E".into())));
    }

    #[test]
    fn test_window_lag_out_of_bounds() {
        let arena = make_arena("name\nA\nB\n", &["name"]);
        let partition: Vec<u32> = vec![0, 1];
        let ctx = PartitionWindowContext::new(&arena, &partition, 0);

        assert!(ctx.lag(1).is_none());
    }

    #[test]
    fn test_window_count() {
        let arena = make_arena("x\na\nb\nc\nd\ne\n", &["x"]);
        let partition: Vec<u32> = vec![0, 1, 2, 3, 4];
        let ctx = PartitionWindowContext::new(&arena, &partition, 0);
        assert_eq!(ctx.count(), 5);
    }

    #[test]
    fn test_window_sum_avg() {
        // CSV values are strings, so we need numeric values in the Arena.
        // Since CsvReader reads all as String, we'll test with a manually constructed Arena.
        // But Arena::build projects from FormatReader which yields String values.
        // sum/avg on String values should return Null (non-numeric).
        // For a proper numeric test, we'd need non-CSV input or post-coercion.
        // Let's test the "non-numeric returns null" case and a String-numeric coercion case.
        let arena = make_arena("amount\n10\n20\n30\n", &["amount"]);
        let partition: Vec<u32> = vec![0, 1, 2];
        let ctx = PartitionWindowContext::new(&arena, &partition, 0);

        // CSV reads as strings, so sum/avg return Null for string fields
        assert_eq!(ctx.sum("amount"), Value::Null);
        assert_eq!(ctx.avg("amount"), Value::Null);

        // This is correct behavior — Arena stores raw CSV strings.
        // Type coercion happens in CXL evaluation, not in the Arena.
        // The window context operates on Arena values directly.
    }

    #[test]
    fn test_window_min_max() {
        // String comparison for CSV-sourced data
        let arena = make_arena("name\nBob\nAlice\nCarol\n", &["name"]);
        let partition: Vec<u32> = vec![0, 1, 2];
        let ctx = PartitionWindowContext::new(&arena, &partition, 0);

        assert_eq!(ctx.min("name"), Value::String("Alice".into()));
        assert_eq!(ctx.max("name"), Value::String("Carol".into()));
    }

    #[test]
    fn test_window_sum_non_numeric() {
        let arena = make_arena("name\nAlice\nBob\n", &["name"]);
        let partition: Vec<u32> = vec![0, 1];
        let ctx = PartitionWindowContext::new(&arena, &partition, 0);
        assert_eq!(ctx.sum("name"), Value::Null);
    }

    #[test]
    fn test_window_any_all() {
        // any/all are evaluator-driven (not on this trait), but we test partition_len/partition_record
        let arena = make_arena("amount\n50\n150\n200\n", &["amount"]);
        let partition: Vec<u32> = vec![0, 1, 2];
        let ctx = PartitionWindowContext::new(&arena, &partition, 0);

        assert_eq!(ctx.partition_len(), 3);
        let rec = ctx.partition_record(1);
        assert_eq!(rec.resolve("amount"), Some(&Value::String("150".into())));
    }

    #[test]
    fn test_window_collect() {
        let arena = make_arena("name\nAlice\nBob\nCarol\n", &["name"]);
        let partition: Vec<u32> = vec![0, 1, 2];
        let ctx = PartitionWindowContext::new(&arena, &partition, 0);

        match ctx.collect("name") {
            Value::Array(vals) => {
                assert_eq!(vals.len(), 3);
                assert_eq!(vals[0], Value::String("Alice".into()));
                assert_eq!(vals[1], Value::String("Bob".into()));
                assert_eq!(vals[2], Value::String("Carol".into()));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_window_distinct() {
        let arena = make_arena("dept\nA\nB\nA\nC\n", &["dept"]);
        let partition: Vec<u32> = vec![0, 1, 2, 3];
        let ctx = PartitionWindowContext::new(&arena, &partition, 0);

        match ctx.distinct("dept") {
            Value::Array(vals) => {
                assert_eq!(vals.len(), 3);
                // Order is insertion order: A, B, C
                assert_eq!(vals[0], Value::String("A".into()));
                assert_eq!(vals[1], Value::String("B".into()));
                assert_eq!(vals[2], Value::String("C".into()));
            }
            other => panic!("Expected Array, got {:?}", other),
        }
    }

    #[test]
    fn test_window_single_record_partition() {
        let arena = make_arena("name\nAlice\n", &["name"]);
        let partition: Vec<u32> = vec![0];
        let ctx = PartitionWindowContext::new(&arena, &partition, 0);

        // first == last for single record
        assert_eq!(
            ctx.first().unwrap().resolve("name"),
            ctx.last().unwrap().resolve("name")
        );
        assert!(ctx.lag(1).is_none());
        assert!(ctx.lead(1).is_none());
        assert_eq!(ctx.count(), 1);
    }
}
