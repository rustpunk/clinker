use chrono::NaiveDateTime;
use std::sync::Arc;

/// Tracks where a record came from in the source data.
/// Arc<str> fields are shared across all records from the same file/run.
#[derive(Debug, Clone)]
pub struct RecordProvenance {
    pub source_file: Arc<str>,
    pub source_row: u64,
    pub source_count: u64,
    pub source_batch: Arc<str>,
    pub ingestion_timestamp: NaiveDateTime,
}

impl RecordProvenance {
    /// Create provenance for a specific row in a source file.
    pub fn new(
        source_file: Arc<str>,
        source_row: u64,
        source_count: u64,
        source_batch: Arc<str>,
        ingestion_timestamp: NaiveDateTime,
    ) -> Self {
        Self {
            source_file,
            source_row,
            source_count,
            source_batch,
            ingestion_timestamp,
        }
    }

    /// Create a provenance factory for a specific source file + batch.
    /// Returns a closure that produces provenances with the given row number.
    pub fn factory(
        source_file: Arc<str>,
        source_batch: Arc<str>,
        ingestion_timestamp: NaiveDateTime,
    ) -> impl FnMut(u64) -> Self {
        move |source_row| Self {
            source_file: Arc::clone(&source_file),
            source_row,
            source_count: 0, // set after Phase 1 completes (Arena size)
            source_batch: Arc::clone(&source_batch),
            ingestion_timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_provenance_arc_sharing() {
        let file: Arc<str> = Arc::from("data/input.csv");
        let batch: Arc<str> = Arc::from("batch-001");
        let ts = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let p1 = RecordProvenance {
            source_file: Arc::clone(&file),
            source_row: 0,
            source_count: 1,
            source_batch: Arc::clone(&batch),
            ingestion_timestamp: ts,
        };
        let p2 = RecordProvenance {
            source_file: Arc::clone(&file),
            source_row: 1,
            source_count: 1,
            source_batch: Arc::clone(&batch),
            ingestion_timestamp: ts,
        };

        assert!(Arc::ptr_eq(&p1.source_file, &p2.source_file));
        assert!(Arc::ptr_eq(&p1.source_batch, &p2.source_batch));
    }

    #[test]
    fn test_provenance_row_independence() {
        let file: Arc<str> = Arc::from("data/input.csv");
        let batch: Arc<str> = Arc::from("batch-001");
        let ts = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let p1 = RecordProvenance::new(Arc::clone(&file), 1, 100, Arc::clone(&batch), ts);
        let p2 = RecordProvenance::new(Arc::clone(&file), 2, 100, Arc::clone(&batch), ts);

        assert_eq!(p1.source_row, 1);
        assert_eq!(p2.source_row, 2);
        assert!(Arc::ptr_eq(&p1.source_file, &p2.source_file));
    }

    #[test]
    fn test_provenance_factory() {
        let file: Arc<str> = Arc::from("data/input.csv");
        let batch: Arc<str> = Arc::from("batch-001");
        let ts = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();

        let mut make_prov = RecordProvenance::factory(file.clone(), batch.clone(), ts);
        let p1 = make_prov(1);
        let p2 = make_prov(2);

        assert_eq!(p1.source_row, 1);
        assert_eq!(p2.source_row, 2);
        assert!(Arc::ptr_eq(&p1.source_file, &p2.source_file));
        assert_eq!(p1.source_count, 0);
    }
}
