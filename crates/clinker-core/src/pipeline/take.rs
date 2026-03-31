use std::sync::Arc;

use clinker_format::error::FormatError;
use clinker_format::traits::FormatReader;
use clinker_record::{Record, Schema};

/// Wraps any FormatReader to yield at most `limit` records.
/// Used by `--dry-run -n <N>` to limit pipeline processing.
pub struct TakeReader<R> {
    inner: R,
    limit: u64,
    count: u64,
}

impl<R> TakeReader<R> {
    pub fn new(inner: R, limit: u64) -> Self {
        Self { inner, limit, count: 0 }
    }

    /// Number of records yielded so far.
    pub fn count(&self) -> u64 {
        self.count
    }
}

impl<R: FormatReader> FormatReader for TakeReader<R> {
    fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
        self.inner.schema()
    }

    fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
        if self.count >= self.limit {
            return Ok(None);
        }
        let record = self.inner.next_record()?;
        if record.is_some() {
            self.count += 1;
        }
        Ok(record)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;

    /// Mock reader that yields `total` records then None.
    struct MockReader {
        schema: Arc<Schema>,
        total: u64,
        yielded: u64,
    }

    impl MockReader {
        fn new(total: u64) -> Self {
            let schema = Arc::new(Schema::new(vec!["id".into()]));
            Self { schema, total, yielded: 0 }
        }
    }

    impl FormatReader for MockReader {
        fn schema(&mut self) -> Result<Arc<Schema>, FormatError> {
            Ok(Arc::clone(&self.schema))
        }

        fn next_record(&mut self) -> Result<Option<Record>, FormatError> {
            if self.yielded >= self.total {
                return Ok(None);
            }
            self.yielded += 1;
            let schema = Arc::clone(&self.schema);
            let record = Record::new(schema, vec![Value::Integer(self.yielded as i64)]);
            Ok(Some(record))
        }
    }

    #[test]
    fn test_take_reader_limit() {
        let mut reader = TakeReader::new(MockReader::new(100), 5);
        let mut count = 0;
        while reader.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
        assert_eq!(reader.count(), 5);
    }

    #[test]
    fn test_take_reader_limit_one() {
        let mut reader = TakeReader::new(MockReader::new(100), 1);
        assert!(reader.next_record().unwrap().is_some());
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(reader.count(), 1);
    }

    #[test]
    fn test_dry_run_n_zero() {
        let mut reader = TakeReader::new(MockReader::new(100), 0);
        assert!(reader.next_record().unwrap().is_none());
        assert_eq!(reader.count(), 0);
    }

    #[test]
    fn test_dry_run_n_exceeds_input() {
        // Limit is 1000 but only 50 records available → yields 50
        let mut reader = TakeReader::new(MockReader::new(50), 1000);
        let mut count = 0;
        while reader.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 50);
    }

    #[test]
    fn test_dry_run_n_limits_records() {
        // -n 10 on 1000 input → exactly 10 output
        let mut reader = TakeReader::new(MockReader::new(1000), 10);
        let mut count = 0;
        while reader.next_record().unwrap().is_some() {
            count += 1;
        }
        assert_eq!(count, 10);
    }

    #[test]
    fn test_take_reader_schema_delegation() {
        let mut reader = TakeReader::new(MockReader::new(5), 3);
        let schema = reader.schema().unwrap();
        assert_eq!(schema.columns().len(), 1);
        assert_eq!(&*schema.columns()[0], "id");
    }

    #[test]
    fn test_take_reader_error_passthrough() {
        // MockReader errors are not possible in this simple mock,
        // but we verify the interface compiles and works
        let mut reader = TakeReader::new(MockReader::new(0), 5);
        assert!(reader.next_record().unwrap().is_none());
    }

    #[test]
    fn test_dry_run_n_streaming_mode() {
        // Verify TakeReader works in streaming (sequential) context
        let mut reader = TakeReader::new(MockReader::new(100), 3);
        let r1 = reader.next_record().unwrap().unwrap();
        let r2 = reader.next_record().unwrap().unwrap();
        let r3 = reader.next_record().unwrap().unwrap();
        assert!(reader.next_record().unwrap().is_none());
        // Records should have sequential IDs
        assert_eq!(r1.get("id"), Some(&Value::Integer(1)));
        assert_eq!(r2.get("id"), Some(&Value::Integer(2)));
        assert_eq!(r3.get("id"), Some(&Value::Integer(3)));
    }
}
