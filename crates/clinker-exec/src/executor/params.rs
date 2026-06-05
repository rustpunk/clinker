//! Runtime parameters for a pipeline execution and the post-run
//! execution report.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use clinker_record::{PipelineCounters, Value};
use indexmap::IndexMap;

use super::{DlqEntry, stage_metrics};

/// Runtime parameters for a pipeline execution (not derived from config YAML).
#[derive(Default)]
pub struct PipelineRunParams {
    /// UUID v7 execution ID, unique per run.
    pub execution_id: String,
    /// Batch ID from --batch-id CLI flag or auto UUID v7.
    pub batch_id: String,
    /// Channel-supplied overrides/adds for `$pipeline.*`. Layered atop
    /// `collect_pipeline_var_defaults` at executor init; channel wins.
    pub pipeline_vars: IndexMap<String, Value>,
    /// Channel-supplied overrides/adds for `$vars.*`. Layered atop
    /// `convert_vars(config.pipeline.vars)`; channel wins.
    pub static_vars: IndexMap<String, Value>,
    /// Channel-supplied overrides/adds for `$source.<src>.<var>`. Outer
    /// key is source-node name; inner key is var name. Layered atop
    /// `collect_source_var_defaults` per file Arc at materialization.
    pub source_vars: IndexMap<String, IndexMap<String, Value>>,
    /// Channel-supplied overrides/adds for `$record.*`. Pre-seeded into
    /// every Record's `record_vars` map at materialization, layered
    /// atop `collect_record_var_defaults`.
    pub record_vars: IndexMap<String, Value>,
    /// Per-run shutdown handle. The executor checks this at chunk boundaries
    /// and inside `Arena::build`. `None` disables shutdown signaling for this
    /// run; production callers typically construct one via
    /// `crate::pipeline::shutdown::ShutdownToken::new()` so SIGINT/SIGTERM
    /// can trip it.
    pub shutdown_token: Option<crate::pipeline::shutdown::ShutdownToken>,
    /// Root directory for the per-run `clinker-spill-*` directory, resolved
    /// from the workspace `clinker.toml` `[storage.spill] dir` setting. `None`
    /// (no setting, or no `clinker.toml`) → the OS temp dir, the historical
    /// default. The caller validates the directory exists and is writable
    /// before the run starts, so the executor treats `Some(dir)` as a vetted
    /// path: a failure to create the spill root under it is an internal error,
    /// not a config error.
    pub spill_root_dir: Option<std::path::PathBuf>,
    /// Cumulative disk-spill quota for the run, in bytes, resolved from the
    /// workspace `clinker.toml` `[storage.spill] disk_cap_bytes` setting.
    /// `None` (no setting, or no `clinker.toml`) → unlimited spill, the
    /// historical default. `Some(cap)` is folded into the run's memory
    /// arbitrator as `max_spill_bytes`; once the cumulative on-disk size of
    /// the run's spill files crosses it, the spilling operator aborts with
    /// `PipelineError::SpillCapExceeded` (E320) — a disk-cap surface kept
    /// distinct from both the RSS budget (E310) and a full volume (E321).
    pub spill_disk_cap_bytes: Option<u64>,
    /// Spill-file compression policy, resolved from the workspace
    /// `clinker.toml` `[storage.spill] compress` setting. Defaults to
    /// [`clinker_plan::config::CompressMode::Auto`] (the `Default`), which
    /// compresses only when a spilled batch is projected large enough to
    /// amortize LZ4's per-frame fixed cost. Threaded into the dispatch
    /// context and resolved per blocking operator at each spill site.
    pub spill_compress: clinker_plan::config::CompressMode,
}

/// Summary returned after a pipeline execution completes (success or partial).
///
/// Replaces the previous `(PipelineCounters, Vec<DlqEntry>)` tuple. Callers
/// that previously destructured the tuple should access `report.counters` and
/// `report.dlq_entries` instead.
#[derive(Debug)]
pub struct ExecutionReport {
    /// Record counts: total, ok, dlq.
    pub counters: PipelineCounters,
    /// Records that were routed to the dead-letter queue.
    pub dlq_entries: Vec<DlqEntry>,
    /// Human-readable execution summary (e.g., "Streaming", "TwoPass").
    pub execution_summary: String,
    /// Whether any transform required arena allocation (window functions).
    pub required_arena: bool,
    /// Peak process RSS observed across chunk boundaries. `None` only on
    /// platforms where RSS measurement is unavailable (e.g., FreeBSD).
    pub peak_rss_bytes: Option<u64>,
    /// Total user CPU time across all stages with capture (nanoseconds).
    /// `None` if no stage captured CPU times. Process-wide; sums across rayon workers.
    pub total_cpu_user_ns: Option<u64>,
    /// Total system CPU time across all stages with capture (nanoseconds).
    pub total_cpu_sys_ns: Option<u64>,
    /// Total disk bytes read across all stages with capture.
    /// Excludes page-cache hits — cold-cache mode required for meaningful numbers.
    pub total_io_read_bytes: Option<u64>,
    /// Total disk bytes written across all stages with capture.
    pub total_io_write_bytes: Option<u64>,
    /// Wall-clock time when `run_with_readers_writers` was entered.
    pub started_at: DateTime<Utc>,
    /// Wall-clock time immediately after the last write and flush completed.
    pub finished_at: DateTime<Utc>,
    /// Per-stage instrumentation metrics, ordered by execution sequence.
    pub stages: Vec<stage_metrics::StageMetrics>,
    /// Per-(source, file) event-time watermarks observed at ingest,
    /// keyed by `(source_name, source_file_path)`. The max event-time
    /// seen for that pair in i64 nanoseconds, or `None` when the
    /// partition had no observations. Finest granularity; drives the
    /// `fan_out_per_source_file` 1:1 source-file → sink case where
    /// each file's watermark is independently meaningful.
    pub per_source_file_watermarks: BTreeMap<(String, String), Option<i64>>,
    /// Per-source rollup = `min` across the source's per-file
    /// watermarks. One entry per declared source (sources whose
    /// `SourceConfig.watermark.column` is set). A glob source with one
    /// lagging file holds its source-level watermark back to that
    /// file's max — matching the Flink/Arroyo per-partition + min
    /// reducer pattern.
    pub per_source_watermarks: BTreeMap<String, Option<i64>>,
    /// Cross-source rollup = `min` across rolled-up source values.
    /// The reducer a time-windowed aggregate's close decision reads
    /// (future consumer). `None` when every declared source rolls up
    /// to `None`.
    pub effective_watermark: Option<i64>,
    /// Per-source rollback cursor at run completion. Keyed by
    /// Source-node name; the value is the highest source row number
    /// that cleanly exited a forward operator. Sources that never
    /// emitted a clean record (every record DLQ'd, or the source had
    /// zero records) are absent from the map. Combine-rooted rewinds
    /// reflect into this map via the `combine_input_snapshots`
    /// restore path. Surfaces as both the per-source replay anchor
    /// for the live mpsc-channel executor and the diagnostic counter
    /// that per-source-rollback tests assert against.
    pub per_source_rollback_cursors: BTreeMap<String, u64>,
    /// Finalized per-source ingest record counts, keyed by
    /// Source-node name. Equals each source's `total_count`
    /// contribution to the aggregate `counters.total_count`. Sources
    /// whose ingest thread never finalized (e.g. fatal abort before
    /// the crossbeam `Receiver` disconnected) are absent rather than
    /// reported as zero — distinguishes "stream closed with zero records"
    /// from
    /// "never finished". The synthetic pipeline-wide rollup slot
    /// stamped internally under `<merged>` is filtered out before
    /// surfacing here.
    pub per_source_record_counts: BTreeMap<String, u64>,
    /// Per-source DLQ entry counts, keyed by Source-node name. A
    /// source with no DLQ entries is absent from the map, matching
    /// the "absent = none landed" precedent on
    /// `per_source_rollback_cursors`. Sums across this map equal
    /// `counters.dlq_count` minus any entries the executor failed to
    /// attribute to a declared source.
    pub per_source_dlq_counts: BTreeMap<String, u64>,
    /// Saturating sum of bytes committed to spill files across every
    /// spill site (`node_buffers` admission, grace-hash partition
    /// flush, sort-merge external sort). Sourced from
    /// `MemoryArbitrator`'s running total at dispatch close; an aborted
    /// run still surfaces the last committed value.
    pub cumulative_spill_bytes: u64,
    /// High-water mark of the arbitrator's summed pull-mode charged bytes
    /// observed across streaming per-batch charges. For a streaming stage
    /// this stays bounded to one in-flight batch (plus the bounded
    /// channel's capacity) rather than the whole stage output — the
    /// observable that proves the per-batch admit/discharge model. `0`
    /// when no streaming charge fired (a fully materialized pipeline).
    pub peak_consumer_usage_bytes: u64,
    /// `true` when the run unwound early because a shutdown signal
    /// (SIGINT/SIGTERM, or a programmatic request) tripped the run's
    /// [`crate::pipeline::shutdown::ShutdownToken`]. The CLI maps this to
    /// the interrupted exit code (130). A clean run leaves it `false`.
    pub interrupted: bool,
}

/// Sum per-stage CPU and I/O deltas into run-level totals. Stages with `None`
/// (e.g. cumulative timers) are skipped. Returns `None` per metric if no stage
/// reported a value, otherwise `Some(sum)`.
pub(super) fn sum_cpu_io_totals(
    stages: &[stage_metrics::StageMetrics],
) -> (Option<u64>, Option<u64>, Option<u64>, Option<u64>) {
    let mut cpu_user: Option<u64> = None;
    let mut cpu_sys: Option<u64> = None;
    let mut io_read: Option<u64> = None;
    let mut io_write: Option<u64> = None;
    fn add(acc: &mut Option<u64>, v: Option<u64>) {
        if let Some(x) = v {
            *acc = Some(acc.unwrap_or(0).saturating_add(x));
        }
    }
    for s in stages {
        add(&mut cpu_user, s.cpu_user_delta_ns);
        add(&mut cpu_sys, s.cpu_sys_delta_ns);
        add(&mut io_read, s.io_read_delta);
        add(&mut io_write, s.io_write_delta);
    }
    (cpu_user, cpu_sys, io_read, io_write)
}
