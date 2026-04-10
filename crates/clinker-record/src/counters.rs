/// Pipeline-wide counters for total/ok/dlq record counts.
/// Lives in clinker-record so cxl evaluator can reference it
/// without depending on clinker-core.
///
/// Updated at chunk boundaries during Phase 2 -- all records within a chunk
/// see the same counter values (snapshotted at chunk start).
#[derive(Debug, Clone, Default)]
pub struct PipelineCounters {
    pub total_count: u64,
    pub ok_count: u64,
    pub dlq_count: u64,
    pub filtered_count: u64,
    pub distinct_count: u64,
}

impl PipelineCounters {
    /// Increment ok_count by n.
    pub fn increment_ok(&mut self, n: u64) {
        self.ok_count += n;
    }

    /// Increment dlq_count by n.
    pub fn increment_dlq(&mut self, n: u64) {
        self.dlq_count += n;
    }

    /// Increment filtered_count by n.
    pub fn increment_filtered(&mut self, n: u64) {
        self.filtered_count += n;
    }

    /// Increment distinct_count by n.
    pub fn increment_distinct(&mut self, n: u64) {
        self.distinct_count += n;
    }

    /// Snapshot the current counters for sharing with a chunk's parallel evaluation.
    pub fn snapshot(&self) -> Self {
        self.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::minimal::MinimalRecord;
    use crate::provenance::RecordProvenance;

    #[test]
    fn test_pipeline_counters_default() {
        let c = PipelineCounters::default();
        assert_eq!(c.total_count, 0);
        assert_eq!(c.ok_count, 0);
        assert_eq!(c.dlq_count, 0);
    }

    #[test]
    fn test_all_structs_send_sync() {
        fn assert_send_sync_clone<T: Send + Sync + Clone>() {}
        assert_send_sync_clone::<MinimalRecord>();
        assert_send_sync_clone::<RecordProvenance>();
        assert_send_sync_clone::<PipelineCounters>();
    }

    #[test]
    fn test_pipeline_counters_increment() {
        let mut c = PipelineCounters::default();
        c.total_count = 1000;
        c.increment_ok(100);
        c.increment_dlq(5);
        assert_eq!(c.ok_count, 100);
        assert_eq!(c.dlq_count, 5);

        let snap = c.snapshot();
        c.increment_ok(200);
        assert_eq!(snap.ok_count, 100);
        assert_eq!(c.ok_count, 300);
    }
}
