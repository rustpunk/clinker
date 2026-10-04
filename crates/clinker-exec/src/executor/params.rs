//! Runtime parameters for a pipeline execution and the post-run
//! execution report.

use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};

use chrono::{DateTime, Utc};
use clinker_record::{PipelineCounters, Value};
use indexmap::IndexMap;

use super::stage_metrics;
use crate::dlq::DlqReport;

/// Whether an admitted run executes normally or reads a bounded preview.
///
/// The per-source bound is nonzero by construction and is checked immediately
/// before each `RecordSource::next_record` call. This keeps a preview from
/// reading one row ahead of the limit, including for composition-body Sources.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreviewPolicy {
    /// Execute the complete finite pipeline and publish configured outputs.
    Disabled,
    /// Compile and preflight only. This mode never enters the executor.
    ConfigOnly,
    /// Execute at most this many records from each declared Source.
    RecordsPerSource(NonZeroU64),
}

impl PreviewPolicy {
    /// Return the read bound for a bounded preview.
    pub fn records_per_source(self) -> Option<NonZeroU64> {
        match self {
            Self::RecordsPerSource(limit) => Some(limit),
            Self::Disabled | Self::ConfigOnly => None,
        }
    }

    /// Whether configured output publication is permitted.
    pub fn publishes_configured_outputs(self) -> bool {
        matches!(self, Self::Disabled)
    }
}

/// Fully resolved execution policy shared by scheduler, readers, and report.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RunPolicy {
    thread_capacity: NonZeroUsize,
    preview: PreviewPolicy,
}

impl RunPolicy {
    /// Construct a policy from already-validated, nonzero CLI/config values.
    pub const fn new(thread_capacity: NonZeroUsize, preview: PreviewPolicy) -> Self {
        Self {
            thread_capacity,
            preview,
        }
    }

    /// Independent cap for CPU workers and concurrent Source read calls.
    ///
    /// This is not a combined operating-system thread limit: Source workers
    /// remain distinct from the Rayon kernel pool.
    pub const fn thread_capacity(self) -> NonZeroUsize {
        self.thread_capacity
    }

    /// Preview/publication behavior for this run.
    pub const fn preview(self) -> PreviewPolicy {
        self.preview
    }
}

/// Runtime parameters for a pipeline execution (not derived from config YAML).
#[derive(Default)]
pub struct PipelineRunParams {
    /// UUID v7 execution ID, unique per run.
    pub execution_id: String,
    /// Batch ID from --batch-id CLI flag or auto UUID v7.
    pub batch_id: String,
    /// Optional fixed-arena telemetry producer for this run. `None` keeps the
    /// executor's signal path allocation-free and performs no worker or
    /// signaling work.
    pub telemetry_producer: Option<crate::telemetry::TelemetryProducer>,
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
    /// Per-run live progress counters, advanced by the executor and sampled by
    /// an observer on another thread. `None` keeps the executor from
    /// publishing anything; the counters that drive its own decisions are
    /// owned state and are unaffected either way.
    pub progress: Option<crate::progress::RunProgress>,
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
    /// Test levers on the memory figures the run starts from: a ledger
    /// capacity below `memory.limit`, the baseline resident memory the
    /// startup check compares against, and a one-shot forced shortfall.
    ///
    /// Production code can build only [`MemoryTestOverrides::process`],
    /// which changes nothing: the run measures its baseline and enforces
    /// `memory.limit`. The CLI names it explicitly so a binary built with
    /// the test features on still reads real process memory.
    #[doc(hidden)]
    pub memory_test: MemoryTestOverrides,
}

/// Baseline resident memory the in-process test harness injects in place of
/// a measurement, in bytes.
///
/// An in-process test shares its process with every sibling test, so a
/// measured baseline depends on what else is running. A fixed figure makes
/// the startup check (E312) decide the same way on every run.
#[cfg(any(test, feature = "test-utils"))]
pub const IN_PROCESS_BASELINE_BYTES: u64 = 16 << 20;

/// Test-only memory figures for one run. See [`PipelineRunParams::memory_test`].
///
/// The fields are private: without the `test-utils` feature the only value
/// that can be built is [`Self::process`]. With the feature (or in this
/// crate's own tests) `Default` is the in-process harness default, which
/// injects [`IN_PROCESS_BASELINE_BYTES`] as the baseline; a test opts back
/// into a measured baseline with `with_process_memory()`.
#[doc(hidden)]
pub struct MemoryTestOverrides {
    ledger_capacity: Option<u64>,
    baseline_rss: Option<u64>,
    #[cfg(any(test, feature = "test-utils"))]
    forced_shortfall: Option<ForcedShortfall>,
}

impl MemoryTestOverrides {
    /// Real process readings and no override: the run measures its baseline
    /// resident memory and enforces `memory.limit` as configured.
    pub fn process() -> Self {
        Self {
            ledger_capacity: None,
            baseline_rss: None,
            #[cfg(any(test, feature = "test-utils"))]
            forced_shortfall: None,
        }
    }

    /// The ledger capacity the run is held to, if any: the in-process
    /// override, else (debug builds only) `CLINKER_TEST_LEDGER_CAPACITY`.
    /// The run enforces the smaller of this and `memory.limit`.
    pub(crate) fn ledger_capacity(&self) -> Option<u64> {
        self.ledger_capacity.or_else(env_ledger_capacity)
    }

    /// The baseline resident memory to judge `memory.limit` against in place
    /// of a measurement, if one is injected.
    pub(crate) fn baseline_rss(&self) -> Option<u64> {
        self.baseline_rss
    }

    /// The forced shortfall to arm on the run's arbitrator, if any.
    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn forced_shortfall(&self) -> Option<&ForcedShortfall> {
        self.forced_shortfall.as_ref()
    }
}

impl Default for MemoryTestOverrides {
    /// The in-process harness default when the crate is built for tests;
    /// [`Self::process`] otherwise.
    fn default() -> Self {
        #[cfg(any(test, feature = "test-utils"))]
        {
            Self {
                baseline_rss: Some(IN_PROCESS_BASELINE_BYTES),
                ..Self::process()
            }
        }
        #[cfg(not(any(test, feature = "test-utils")))]
        {
            Self::process()
        }
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl MemoryTestOverrides {
    /// Hold the run's ledger to `bytes` while `memory.limit` stays as
    /// configured. The run enforces the smaller of the two: every runtime
    /// figure derived from the limit (spill and resume thresholds, operator
    /// budgets, the bytes `reserve` grants against) follows it, while the
    /// startup check still judges `memory.limit`. It never raises the limit.
    pub fn with_ledger_capacity(mut self, bytes: u64) -> Self {
        self.ledger_capacity = Some(bytes);
        self
    }

    /// Judge `memory.limit` at startup against a baseline of `bytes` instead
    /// of a measurement.
    pub fn with_baseline_rss(mut self, bytes: u64) -> Self {
        self.baseline_rss = Some(bytes);
        self
    }

    /// Read real process memory: measure the baseline rather than inject one.
    pub fn with_process_memory(mut self) -> Self {
        self.baseline_rss = None;
        self
    }

    /// Arm `shortfall` on the run's arbitrator before the run starts.
    ///
    /// For the two kinds of test [`ForcedShortfall`] permits: a whole-unit
    /// spill-then-reload test, and a spill-path-equivalence test run twice
    /// at one ample limit. Each use records its reason in its test's doc
    /// comment. In this build a run-level arm reaches only a Source's record
    /// allocations and fails the run, because nothing answers the refusal
    /// until requesters off the walk can wait
    /// ([#1247](https://github.com/rustpunk/clinker/issues/1247)).
    pub fn with_forced_shortfall(mut self, shortfall: ForcedShortfall) -> Self {
        self.forced_shortfall = Some(shortfall);
        self
    }

    /// The baseline this value injects in place of a measurement, if any.
    pub fn injected_baseline_rss(&self) -> Option<u64> {
        self.baseline_rss
    }
}

/// Read `CLINKER_TEST_LEDGER_CAPACITY` (a plain byte count) for a debug
/// build's run. Subprocess tests set it on the child so a real binary can be
/// held to a small ledger while its `memory.limit` stays ample. An
/// unparseable value is ignored with a warning.
#[cfg(debug_assertions)]
fn env_ledger_capacity() -> Option<u64> {
    let raw = std::env::var("CLINKER_TEST_LEDGER_CAPACITY").ok()?;
    match raw.trim().parse::<u64>() {
        Ok(bytes) => Some(bytes),
        Err(error) => {
            tracing::warn!(
                value = %raw,
                %error,
                "ignoring CLINKER_TEST_LEDGER_CAPACITY: expected a plain byte count"
            );
            None
        }
    }
}

/// Release builds never read the test capacity variable.
#[cfg(not(debug_assertions))]
fn env_ledger_capacity() -> Option<u64> {
    None
}

/// A targeted forced shortfall: charges by a requester whose label `matcher`
/// accepts fall short as if nothing were available, from the `nth` matching
/// charge on, [`Self::times`] times, [`Self::every`] matching charges apart.
///
/// Every checked charge path counts: `MemoryArbitrator::reserve`,
/// `Grant::try_grow` and `ConsumerHandle::try_grow` / `try_resize` all reach
/// the one locked admission check that consults it. `nth` counts from 1 and
/// counts only matching charges, so `nth = 1` is the next one. A charge for
/// zero bytes or for more than the whole limit, a charge on a closed ledger,
/// a governed charge and one by an unlabelled consumer never count. A due
/// firing waits for a matching charge whose requester holds resident bytes
/// (its handle plus the grants made in its name). The refusal is the one a
/// real shortage gives, with nothing charged, and reports itself as forced
/// (`Shortfall::forced`); every charge that does not fire takes the real
/// path.
///
/// In this build the supported use is on an arbitrator a test builds
/// itself, armed with `MemoryArbitrator::force_shortfall_once` or
/// `MemoryArbitrator::arm_forced_shortfall`: there a matching labelled
/// charge through any of the checked paths above counts and fires as
/// described. In a run, node state charges its handles through the
/// unchecked forms, so an arm set with
/// [`MemoryTestOverrides::with_forced_shortfall`] reaches only a Source's
/// record allocations; nothing answers that refusal, and the run fails with
/// a budget error rather than spilling. The run-level uses below need a run
/// whose requester can answer a forced refusal, which arrives with the
/// waiting work for requesters off the walk
/// ([#1247](https://github.com/rustpunk/clinker/issues/1247)).
///
/// Two kinds of test may use it, and each records its reason in its doc
/// comment:
/// - a test that spills a whole unit and then reloads it, where no ledger
///   capacity both forces the spill and admits the reload;
/// - a spill-path-equivalence test (spilled output equals resident output).
///   It runs twice at the same ample limit: unarmed, asserting no spill
///   bytes were written; armed, asserting [`Self::fired`] counted its
///   firings and the named node's `per_stage_spill_bytes_written` is above
///   0.
///
/// Proving that the arbitrator spills under real pressure is not one of
/// them: that stays with the two-direction pairs on a derived ledger
/// capacity.
#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone)]
pub struct ForcedShortfall {
    matcher:
        std::sync::Arc<dyn Fn(&clinker_plan::runtime_error::ConsumerLabel) -> bool + Send + Sync>,
    nth: u32,
    times: u32,
    every: u32,
    fired: std::sync::Arc<std::sync::atomic::AtomicU32>,
}

#[cfg(any(test, feature = "test-utils"))]
impl ForcedShortfall {
    /// Fall short once, on the `nth` (from 1) charge whose requester
    /// `matcher` accepts, or on the first matching charge after it at which
    /// the requester holds resident bytes.
    ///
    /// # Panics
    ///
    /// When `nth` is 0: there is no zeroth request.
    pub fn at(
        matcher: impl Fn(&clinker_plan::runtime_error::ConsumerLabel) -> bool + Send + Sync + 'static,
        nth: u32,
    ) -> Self {
        assert!(
            nth >= 1,
            "a forced shortfall counts matching charges from 1; nth = 0 names no request"
        );
        Self {
            matcher: std::sync::Arc::new(matcher),
            nth,
            times: 1,
            every: 1,
            fired: std::sync::Arc::default(),
        }
    }

    /// Fall short `n` times in all (once unless set); the arm is gone after
    /// the last firing.
    ///
    /// # Panics
    ///
    /// When `n` is 0: an arm that never fires forces nothing.
    pub fn times(mut self, n: u32) -> Self {
        assert!(
            n >= 1,
            "a forced shortfall fires at least once; times(0) never fires"
        );
        self.times = n;
        self
    }

    /// Space the firings `charges` matching charges apart (1 unless set):
    /// after a firing on matching charge k, the next is due on matching
    /// charge k + `charges`, or on the first one after it at which the
    /// requester holds resident bytes.
    ///
    /// # Panics
    ///
    /// When `charges` is 0: two firings cannot fall on one charge.
    pub fn every(mut self, charges: u32) -> Self {
        assert!(
            charges >= 1,
            "forced shortfalls fall at least one matching charge apart; every(0) names none"
        );
        self.every = charges;
        self
    }

    /// The number of firings so far. Every clone of this value shares the
    /// counter, so a test keeps it before handing the value to a run.
    pub fn fired(&self) -> std::sync::Arc<std::sync::atomic::AtomicU32> {
        std::sync::Arc::clone(&self.fired)
    }

    pub(crate) fn accepts(&self, label: &clinker_plan::runtime_error::ConsumerLabel) -> bool {
        (self.matcher)(label)
    }

    pub(crate) fn nth(&self) -> u32 {
        self.nth
    }

    /// How many times the arm fires in all.
    pub(crate) fn firings(&self) -> u32 {
        self.times
    }

    /// Matching charges from one firing to the next.
    pub(crate) fn spacing(&self) -> u32 {
        self.every
    }

    pub(crate) fn record_firing(&self) {
        self.fired
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}

#[cfg(any(test, feature = "test-utils"))]
impl std::fmt::Debug for ForcedShortfall {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForcedShortfall")
            .field("nth", &self.nth)
            .field("times", &self.times)
            .field("every", &self.every)
            .finish_non_exhaustive()
    }
}

/// Summary returned after a pipeline execution completes (success or partial).
///
/// Holds counts and metrics only; it retains no record. Dead letters are
/// counted in [`Self::counters`] (`dlq_count`), [`Self::per_source_dlq_counts`]
/// and [`Self::dead_letters`]; the dead-lettered rows themselves were written
/// through the caller's [`crate::dlq::DlqSink`] while the run executed.
#[derive(Debug)]
pub struct ExecutionReport {
    /// Record counts: total, ok, dlq.
    pub counters: PipelineCounters,
    /// Dead-letter counters: rows per stage and category, and rows written
    /// per dead-letter file. Holds counts only, never rows; its size is
    /// bounded by the plan. The rows themselves went to the caller's
    /// [`crate::dlq::DlqSink`] while the run executed.
    pub dead_letters: DlqReport,
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
    /// `per_source_rollback_cursors`.
    ///
    /// Contract: the sum of the values is `<=` `counters.dlq_count`,
    /// never `==` in general. The difference is the DLQ
    /// entries the executor could not attribute to a single declared
    /// source and stamped under the synthetic `<merged>` rollup —
    /// Combine emits and post-aggregate synthetic rows carry no
    /// originating source stamp, so a failure downstream of them is
    /// counted in `dlq_count` but filtered out of this map. Equality
    /// holds only for pipelines whose every DLQ entry traces to a
    /// declared source (e.g. a Transform funnel over plain sources).
    pub per_source_dlq_counts: BTreeMap<String, u64>,
    /// Bytes committed to spill files across every spill site
    /// (`node_buffers` admission, grace-hash partition flush, sort-merge
    /// external sort), net of any released as a run was unlinked — so a
    /// cascaded k-way merge's transient intermediate runs do not inflate it.
    /// Sourced from `MemoryArbitrator` after dispatch and all Source workers
    /// finish, including interrupted ordered-source cleanup. Released spill
    /// charges are excluded; this is not a count of every byte ever written.
    pub cumulative_spill_bytes: u64,
    /// Per-stage on-disk spill totals, keyed by the spilling node's name.
    /// The sum of the values equals [`Self::cumulative_spill_bytes`]; this
    /// breakdown is the per-stage actual an operator compares against the
    /// pre-run `--explain` per-stage estimate (the calibration loop #176
    /// exists for). Empty when no stage spilled. Distinct from
    /// `cumulative_spill_bytes`, which is the single pipeline-wide total.
    /// Both are sampled after all Source workers have joined.
    pub per_stage_spill_bytes: BTreeMap<String, u64>,
    /// Bytes each stage wrote to spill files over the whole run, keyed by the
    /// spilling node's name. Never lowered when a run is unlinked, so a sort
    /// whose runs were merged and deleted before the run ended still shows the
    /// bytes it wrote here while its [`Self::per_stage_spill_bytes`] entry is
    /// back at zero. This is the figure that answers "did this stage spill";
    /// the on-disk figures answer "how much disk is still held". Stages that
    /// never spilled have no entry. Sampled with the on-disk figures, after
    /// all Source workers have joined.
    pub per_stage_spill_bytes_written: BTreeMap<String, u64>,
    /// The run's charged peak: the most bytes the memory ledger held charged
    /// at one instant, every registered consumer's handle charge and every
    /// governed allocation together. Raised by every charge, not sampled, so
    /// a streaming stage that admits and discharges one batch at a time
    /// keeps it near one in-flight batch (plus the bounded channel's
    /// capacity) rather than the whole stage output. `0` when nothing was
    /// ever charged. Sampled after all Source workers have joined.
    pub peak_consumer_usage_bytes: u64,
    /// For each node whose retained state is registered with the arbitrator
    /// under the node's name, the highest number of bytes any one of that
    /// node's memory consumers held charged. Each consumer's mark is raised
    /// on every charge, not sampled, and belongs to that consumer alone, so
    /// another node's state never raises this node's figure. A node with
    /// several consumers reports the largest single consumer's mark, not
    /// their sum. Run-scoped state that no node owns (writer output staging,
    /// the credential registry) has no entry. A consumer's mark covers its
    /// handle's charge plus the governed allocations made in its name.
    /// Attribution travels with each lease, so a Source's figure includes its
    /// admitted records wherever they are held downstream.
    /// Unlike [`Self::peak_consumer_usage_bytes`], the run-wide peak of
    /// everything charged at once, this is the figure that says how much one
    /// node's state held. Sampled after all Source workers have joined.
    pub per_node_peak_charged_bytes: BTreeMap<String, u64>,
    /// The memory limit the run's arbitrator enforced, in bytes: the figure
    /// every `reserve` was granted against. It is `memory.limit` unless a
    /// test held the run to a smaller ledger capacity, in which case it is
    /// that capacity. Read from the arbitrator after all Source workers have
    /// joined, not counted during the run.
    pub memory_limit_bytes: u64,
    /// `true` when the run unwound early because a shutdown signal
    /// (SIGINT/SIGTERM, or a programmatic request) tripped the run's
    /// [`crate::pipeline::shutdown::ShutdownToken`]. The CLI maps this to
    /// the interrupted exit code (130). A clean run leaves it `false`.
    pub interrupted: bool,
    /// Advisory end-of-run findings, already rendered, in Output declaration
    /// order within each kind. The per-Sink `mapping:` report comes first —
    /// **W365** for an entry whose column no record carried, **W366** for an
    /// upstream column a mapped output name displaced — then **W367**, one per
    /// Sink whose `truncation: warn` columns cut values to fit.
    ///
    /// Never fatal. Each describes a file that was written and is readable; by
    /// the time a stream ends its sibling Outputs have flushed, so aborting
    /// would leave a half-written run behind for a fault visible in the output
    /// itself. Empty on a clean run.
    pub advisories: Vec<String>,
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
