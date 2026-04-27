/// Pipeline-wide counters for record-level outcomes.
///
/// Lives in clinker-record so the cxl evaluator can reference it
/// without depending on clinker-core. Updated at chunk boundaries —
/// all records within a chunk see the same counter values
/// (snapshotted at chunk start).
///
/// Counter semantics — distinct-input vs write-throughput split under
/// fan-out:
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
/// sources may undercount distinct inputs — a known limitation
/// pending a globally-unique source-row stamp.
#[derive(Debug, Clone, Default)]
pub struct PipelineCounters {
    pub total_count: u64,
    pub ok_count: u64,
    pub records_written: u64,
    pub dlq_count: u64,
    pub filtered_count: u64,
    pub distinct_count: u64,
    /// Counters for the retraction protocol that fires when an
    /// aggregate's `group_by` omits a correlation-key field. All fields
    /// stay zero on pipelines whose every aggregate has
    /// `group_by ⊇ correlation_key` (and on pipelines without a
    /// correlation key) because the orchestrator short-circuits to the
    /// fast path before any retraction phase runs; non-zero values
    /// appear only on pipelines whose configuration activates the
    /// retraction protocol and that triggered at least one DLQ event.
    pub retraction: RetractionCounters,
}

/// Runtime counters surfaced by the relaxed correlation-key
/// commit-orchestrator.
///
/// The orchestrator's five phases each contribute one counter so an
/// operator reading `clinker metrics collect` output can answer the
/// follow-up questions a `--explain` plan-time estimate cannot:
///
/// * which retraction *strategy* paths actually fired in production
///   (groups recomputed via reverse-op or buffer-rerun, partitions
///   recomputed wholesale),
/// * how much downstream work the replay phase did
///   (`subdag_replay_rows`),
/// * how many output rows the post-replay flush actually retracted
///   relative to the strict path's blanket-collateral DLQ
///   (`output_rows_retracted_total`),
/// * how many groups or partitions fell into the documented degrade
///   fallback because the retract-affordable preconditions broke at
///   runtime (`degrade_fallback_count`).
///
/// Counters never decrement; sums are monotonic over the pipeline run.
#[derive(Debug, Clone, Default)]
pub struct RetractionCounters {
    /// Aggregate groups whose accumulator state was rerun
    /// (reverse-op or buffer-rerun) by `recompute_aggregates`. One
    /// increment per emitted retract-old / add-new delta pair.
    pub groups_recomputed: u64,
    /// Window partitions whose stored row buffer was wholesale-rerun
    /// by `recompute_window_partitions`. Counts distinct
    /// (window_node, partition_key) pairs touched, not deltas.
    pub partitions_recomputed: u64,
    /// Total record-deltas the replay phase pushed downstream of the
    /// recompute steps. Each iteration adds the number of deltas
    /// inspected on that pass; covers both substituted-into-buffer and
    /// carried-forward shapes.
    pub subdag_replay_rows: u64,
    /// Output rows the post-replay flush retracted relative to the
    /// pre-replay output set. Equals the count of replayed deltas
    /// whose `retract_old_row` was found in a buffered output cell —
    /// the scope where the flush phase actually substituted a new row
    /// for the old one.
    pub output_rows_retracted_total: u64,
    /// Aggregate-or-partition retract paths that broke the
    /// retract-afford precondition (e.g. spilled aggregator state for a
    /// triggered group) and degraded to documented strict-collateral
    /// DLQ behavior. Each increment indicates one (aggregate_node,
    /// group_key) or (window_node, partition_key) that took the
    /// fallback.
    pub degrade_fallback_count: u64,
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
    fn test_retraction_counters_default_zero() {
        let c = PipelineCounters::default();
        assert_eq!(c.retraction.groups_recomputed, 0);
        assert_eq!(c.retraction.partitions_recomputed, 0);
        assert_eq!(c.retraction.subdag_replay_rows, 0);
        assert_eq!(c.retraction.output_rows_retracted_total, 0);
        assert_eq!(c.retraction.degrade_fallback_count, 0);
    }

    #[test]
    fn test_retraction_counters_clone_preserves_fields() {
        let mut c = PipelineCounters::default();
        c.retraction.groups_recomputed = 7;
        c.retraction.partitions_recomputed = 2;
        c.retraction.subdag_replay_rows = 13;
        c.retraction.output_rows_retracted_total = 6;
        c.retraction.degrade_fallback_count = 1;
        let snap = c.snapshot();
        assert_eq!(snap.retraction.groups_recomputed, 7);
        assert_eq!(snap.retraction.partitions_recomputed, 2);
        assert_eq!(snap.retraction.subdag_replay_rows, 13);
        assert_eq!(snap.retraction.output_rows_retracted_total, 6);
        assert_eq!(snap.retraction.degrade_fallback_count, 1);
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
