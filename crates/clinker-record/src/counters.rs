/// Pipeline-wide counters for record-level outcomes.
///
/// Lives in clinker-record so the cxl evaluator can reference it
/// without depending on clinker-core. Updated at chunk boundaries
/// during Phase 2 — all records within a chunk see the same counter
/// values (snapshotted at chunk start).
///
/// Counter semantics (Phase 16d, locked decision LD-16d-1):
///
/// * `ok_count` — distinct source records that successfully reached
///   at least one Output. Inclusive Route fan-out where one input
///   matches N branches counts ONE toward `ok_count` (the input
///   succeeded once, not N times). The end-to-end "successfully
///   processed" metric a user typically reaches for.
///
/// * `records_written` — total writes across all sinks. The same
///   input reaching N Outputs counts N times. Aligns with per-Output
///   write throughput; for a single-Output exclusive pipeline this
///   equals `ok_count`. For inclusive Route fan-out / multi-Output
///   sinks it exceeds `ok_count`.
///
/// Source-row identity for `ok_count` deduplication uses each
/// record's source row number (`row_num`) as it flows through the
/// DAG. For multi-source pipelines, row-num collisions across
/// sources may undercount distinct inputs; LD-16d-1 documents this
/// as a known limitation pending a globally-unique source-row stamp.
#[derive(Debug, Clone, Default)]
pub struct PipelineCounters {
    pub total_count: u64,
    pub ok_count: u64,
    pub records_written: u64,
    pub dlq_count: u64,
    pub filtered_count: u64,
    pub distinct_count: u64,
}

impl PipelineCounters {
    /// Increment `ok_count` by n. Use once per source record that
    /// successfully reached at least one Output.
    pub fn increment_ok(&mut self, n: u64) {
        self.ok_count += n;
    }

    /// Increment `records_written` by n. Use per Output write — one
    /// per record reaching each sink.
    pub fn increment_records_written(&mut self, n: u64) {
        self.records_written += n;
    }

    /// Increment `dlq_count` by n.
    pub fn increment_dlq(&mut self, n: u64) {
        self.dlq_count += n;
    }

    /// Increment `filtered_count` by n.
    pub fn increment_filtered(&mut self, n: u64) {
        self.filtered_count += n;
    }

    /// Increment `distinct_count` by n.
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
