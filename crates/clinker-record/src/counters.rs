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
/// The orchestrator surfaces these so an operator reading
/// `clinker metrics collect` output can answer the follow-up questions
/// a `--explain` plan-time estimate cannot:
///
/// * how many aggregate groups the recompute phase rewrote
///   (`groups_recomputed`),
/// * how many windowed-Transform members the deferred-region
///   dispatcher re-evaluated on the commit pass
///   (`partitions_dispatched`),
/// * how many cascading-retraction iterations the orchestrator ran
///   before convergence (`iterations`) — surfaces operator-visible
///   retraction depth so a deep cascade reads out as a single number,
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
    /// Windowed-Transform members dispatched on the deferred-region
    /// commit pass. Each increment marks one windowed-Transform
    /// re-evaluation against post-recompute upstream emits — the
    /// per-partition emit during deferred dispatch, not an ahead-of-time
    /// recompute walk.
    pub partitions_dispatched: u64,
    /// Cascading-retraction loop iterations executed before
    /// convergence. Always `0` on strict / fast-path pipelines (the
    /// orchestrator never enters the loop). Always `>= 1` on relaxed
    /// pipelines that reached the orchestrator. Each increment is one
    /// recompute-then-dispatch cycle; values `> 1` indicate downstream
    /// failures during commit-pass dispatch widened the retract scope
    /// and the orchestrator re-ran with the expanded set.
    pub iterations: u64,
    /// Aggregate-or-partition retract paths that broke the
    /// retract-afford precondition (e.g. spilled aggregator state for a
    /// triggered group) and degraded to documented strict-collateral
    /// DLQ behavior. Each increment indicates one (aggregate_node,
    /// group_key) or (window_node, partition_key) that took the
    /// fallback.
    pub degrade_fallback_count: u64,
    /// Synthetic `$ck.aggregate.<name>` column writes performed by the
    /// relaxed aggregator's finalize step. One increment per output
    /// row whose synthetic-CK slot was populated with the group index.
    /// Stays zero on strict aggregates (the slot is absent from the
    /// output schema, so the lookup short-circuits without a write).
    pub synthetic_ck_columns_emitted_total: u64,
    /// Detect-phase observations of an `AggregateGroupIndex` column on
    /// a triggered correlation buffer cell. Each increment corresponds
    /// to one resolution attempt — the orchestrator tried to decode the
    /// synthetic group index back into contributing source row ids.
    /// Increments even when the retained aggregator state is missing
    /// (degrade-fallback path).
    pub synthetic_ck_fanout_lookups_total: u64,
    /// Source-row ids unioned into the affected set as a result of
    /// successful synthetic-CK fan-out. Equal to the sum of
    /// `input_rows.len()` across every successful resolution; pairs
    /// with `synthetic_ck_fanout_lookups_total` to characterize
    /// per-failure expansion factor.
    pub synthetic_ck_fanout_rows_expanded_total: u64,
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
        assert_eq!(c.retraction.partitions_dispatched, 0);
        assert_eq!(c.retraction.iterations, 0);
        assert_eq!(c.retraction.degrade_fallback_count, 0);
        assert_eq!(c.retraction.synthetic_ck_columns_emitted_total, 0);
        assert_eq!(c.retraction.synthetic_ck_fanout_lookups_total, 0);
        assert_eq!(c.retraction.synthetic_ck_fanout_rows_expanded_total, 0);
    }

    #[test]
    fn test_retraction_counters_clone_preserves_fields() {
        let mut c = PipelineCounters::default();
        c.retraction.groups_recomputed = 7;
        c.retraction.partitions_dispatched = 2;
        c.retraction.iterations = 5;
        c.retraction.degrade_fallback_count = 1;
        c.retraction.synthetic_ck_columns_emitted_total = 4;
        c.retraction.synthetic_ck_fanout_lookups_total = 3;
        c.retraction.synthetic_ck_fanout_rows_expanded_total = 19;
        let snap = c.snapshot();
        assert_eq!(snap.retraction.groups_recomputed, 7);
        assert_eq!(snap.retraction.partitions_dispatched, 2);
        assert_eq!(snap.retraction.iterations, 5);
        assert_eq!(snap.retraction.degrade_fallback_count, 1);
        assert_eq!(snap.retraction.synthetic_ck_columns_emitted_total, 4);
        assert_eq!(snap.retraction.synthetic_ck_fanout_lookups_total, 3);
        assert_eq!(snap.retraction.synthetic_ck_fanout_rows_expanded_total, 19);
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
