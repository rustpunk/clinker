pub mod stage_metrics;

pub mod combine;
pub(crate) mod commit;
pub(crate) mod dispatch;
pub mod node_buffer;
pub(crate) mod node_buffer_spill;
mod schema_check;
pub mod source_stream;
pub(crate) mod time_window;
pub(crate) mod watermark;
pub(crate) mod window_runtime;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufWriter, Read, Write};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use clinker_record::{PipelineCounters, Record, RecordStorage, Schema, SchemaBuilder, Value};
use indexmap::IndexMap;

use crate::config::{OutputConfig, PipelineConfig};
use crate::error::PipelineError;
use crate::pipeline::arena::Arena;
use crate::pipeline::index::{GroupByKey, value_to_group_key};
use crate::pipeline::memory::rss_bytes;
use crate::pipeline::window_context::PartitionWindowContext;
use crate::plan::execution::ExecutionPlanDag;
use clinker_format::counting::{CountedFormatWriter, CountingWriter, SharedByteCounter};
use clinker_format::csv::reader::{CsvReader, CsvReaderConfig};
use clinker_format::csv::writer::{CsvWriter, CsvWriterConfig, HeaderCapturingCsvWriter};
use clinker_format::fixed_width::reader::{FixedWidthReader, FixedWidthReaderConfig};
use clinker_format::fixed_width::writer::{FixedWidthWriter, FixedWidthWriterConfig};
use clinker_format::json::reader::{
    ArrayPathMode, ArrayPathSpec, JsonMode, JsonReader, JsonReaderConfig,
};
use clinker_format::json::writer::{JsonOutputMode, JsonWriter, JsonWriterConfig};
use clinker_format::splitting::{OversizeGroupPolicy, SplitPolicy, SplittingWriter, WriterFactory};
use clinker_format::traits::{FormatReader, FormatWriter};
use clinker_format::xml::reader::{
    NamespaceMode, XmlArrayMode, XmlArrayPath, XmlReader, XmlReaderConfig,
};
use clinker_format::xml::writer::{XmlWriter, XmlWriterConfig};
use cxl::ast::Statement;
use cxl::eval::{
    EvalContext, EvalResult, ProgramEvaluator, SkipReason, StableEvalContext, WallClock,
};
use cxl::typecheck::{Type, TypedProgram};
use petgraph::Direction;

/// Executor-internal transform spec.
///
/// Produced by walking `PipelineConfig::nodes` directly and matching on
/// `PipelineNode::{Transform, Aggregate, Route}` variants. Used by
/// executor read paths that still consume a flat Vec-of-transforms view.
/// The fields are the superset that executor sites read from; see
/// `build_transform_specs`.
#[derive(Debug, Clone)]
pub struct TransformSpec {
    pub name: String,
    pub cxl: Option<String>,
    pub aggregate: Option<crate::config::AggregateConfig>,
    pub route: Option<crate::config::RouteConfig>,
    pub input: Option<crate::config::TransformInput>,
    /// Per-record fan-out ceiling for `emit each` blocks inside this
    /// transform. Threaded to the evaluator at construction time and
    /// surfaces as a typed DLQ entry on overflow.
    pub max_expansion: u64,
}

impl TransformSpec {
    pub fn cxl_source(&self) -> &str {
        if let Some(agg) = &self.aggregate {
            agg.cxl.as_str()
        } else if let Some(s) = &self.cxl {
            s.as_str()
        } else {
            ""
        }
    }
}

/// Walk `PipelineConfig::nodes` and materialize a flat `Vec<TransformSpec>`
/// from the Transform/Aggregate/Route variants in declaration order. Merge
/// nodes referenced by a transform's input are expanded back into
/// `TransformInput::Multiple(list)` to match the legacy executor wire shape.
pub fn build_transform_specs(config: &PipelineConfig) -> Vec<TransformSpec> {
    use crate::config::node_header::NodeInput;
    use crate::config::{AggregateConfig, PipelineNode, RouteBranch, RouteConfig, TransformInput};

    let merge_by_name: std::collections::HashMap<&str, Vec<String>> = config
        .nodes
        .iter()
        .filter_map(|n| match &n.value {
            PipelineNode::Merge { header, .. } => {
                let upstreams: Vec<String> = header
                    .inputs
                    .iter()
                    .map(|spanned_ni| match &spanned_ni.value {
                        NodeInput::Single(s) => s.clone(),
                        NodeInput::Port { node, port } => format!("{node}.{port}"),
                    })
                    .collect();
                Some((header.name.as_str(), upstreams))
            }
            _ => None,
        })
        .collect();

    let project_input = |ni: &NodeInput| -> Option<TransformInput> {
        match ni {
            NodeInput::Single(s) => {
                if let Some(upstreams) = merge_by_name.get(s.as_str()) {
                    Some(TransformInput::Multiple(upstreams.clone()))
                } else {
                    Some(TransformInput::Single(s.clone()))
                }
            }
            NodeInput::Port { node, port } => {
                Some(TransformInput::Single(format!("{node}.{port}")))
            }
        }
    };

    let mut out = Vec::new();
    for spanned in &config.nodes {
        match &spanned.value {
            PipelineNode::Transform {
                header,
                config: body,
            } => {
                out.push(TransformSpec {
                    name: header.name.clone(),
                    cxl: Some(body.cxl.as_ref().to_string()),
                    aggregate: None,
                    route: None,
                    input: project_input(&header.input.value),
                    max_expansion: body.max_expansion,
                });
            }
            PipelineNode::Aggregate {
                header,
                config: body,
            } => {
                out.push(TransformSpec {
                    name: header.name.clone(),
                    cxl: None,
                    aggregate: Some(AggregateConfig {
                        group_by: body.group_by.clone(),
                        cxl: body.cxl.as_ref().to_string(),
                        strategy: body.strategy,
                        time_window: body.time_window.clone(),
                        allowed_lateness: body.allowed_lateness,
                    }),
                    route: None,
                    input: project_input(&header.input.value),
                    max_expansion: cxl::eval::DEFAULT_MAX_EXPANSION,
                });
            }
            PipelineNode::Route {
                header,
                config: body,
            } => {
                let branches: Vec<RouteBranch> = body
                    .conditions
                    .iter()
                    .map(|(name, cxl)| RouteBranch {
                        name: name.clone(),
                        condition: cxl.as_ref().to_string(),
                    })
                    .collect();
                out.push(TransformSpec {
                    name: header.name.clone(),
                    cxl: Some(String::new()),
                    aggregate: None,
                    route: Some(RouteConfig {
                        mode: body.mode,
                        branches,
                        default: body.default.clone(),
                    }),
                    input: project_input(&header.input.value),
                    max_expansion: cxl::eval::DEFAULT_MAX_EXPANSION,
                });
            }
            _ => {}
        }
    }
    out
}

/// Map from source-node name to the ordered list of files feeding that
/// source. Each entry pairs the originating filesystem path (used to
/// stamp `$source.file` per record) with an open `Read` handle.
///
/// For literal-`path:` sources the vec holds a single element. For
/// `glob`/`regex`/`paths` sources the discovery layer produces one
/// entry per matched file; the executor concatenates them into a single
/// stream via [`crate::source::multi_file::MultiFileFormatReader`] and
/// stamps each record with the file it came from.
pub type SourceReaders = HashMap<String, Vec<crate::source::multi_file::FileSlot>>;

/// Dispatch result threaded out of `execute_dag` and
/// `execute_dag_branching` and folded into the [`ExecutionReport`].
/// Per-run summary fed up from `execute_dag_branching` into
/// [`ExecutionReport`] construction. Each field is the final, post-walk
/// state of an executor counter or bookkeeping table; the consumer
/// (`run_with_readers_writers`) folds them into the user-facing report
/// and may layer derived stages on top.
pub(crate) struct DispatchOutcome {
    /// Aggregate pipeline counters: total / ok / dlq / records-written.
    pub(crate) counters: PipelineCounters,
    /// Every DLQ entry produced across every dispatcher arm, in
    /// observation order. Empty when the run had no failures.
    pub(crate) dlq_entries: Vec<DlqEntry>,
    /// Peak process RSS observed across chunk boundaries. `None` on
    /// platforms where RSS measurement is unavailable.
    pub(crate) peak_rss_bytes: Option<u64>,
    /// Per-source / per-file event-time watermarks accumulated by the
    /// ingest tasks. Carried through so the report can roll them up to
    /// source-level and effective-watermark granularities.
    pub(crate) watermarks: crate::executor::watermark::PerSourceWatermarks,
    /// Per-source forward-progress rollback cursors at run completion,
    /// keyed by Source-node name. Sources that never emitted a clean
    /// record are absent.
    pub(crate) per_source_rollback_cursors: BTreeMap<String, u64>,
    /// Finalized per-source ingest record counts, keyed by Source-node
    /// name. Built from the executor's per-source count map at run
    /// close; sources whose finalize slot is still unstamped (`None`)
    /// are omitted rather than fabricated as zero. The synthetic
    /// pipeline-wide rollup slot stamped under
    /// `dispatch::MERGED_SOURCE_NAME` is excluded — this map exposes
    /// declared sources only.
    pub(crate) per_source_record_counts: BTreeMap<String, u64>,
    /// Per-source DLQ entry counts, keyed by Source-node name. Sources
    /// with zero DLQ entries are absent (matching the
    /// `per_source_rollback_cursors` precedent of "absent = none
    /// landed"). The synthetic `MERGED_SOURCE_NAME` slot is filtered
    /// out on the way through.
    pub(crate) per_source_dlq_counts: BTreeMap<String, u64>,
    /// Saturating sum of bytes the run committed to spill files across
    /// every spill site (node_buffer admission, grace-hash partition
    /// flush, sort-merge external sort). Read from `MemoryArbitrator`'s
    /// running total at dispatch close so an aborted run still
    /// reports the last committed value.
    pub(crate) cumulative_spill_bytes: u64,
}

/// Output writer registry. Holds two parallel maps:
///
/// - `single`: one writer per output name (the legacy shape; matches
///   one-Output-to-one-file pipelines).
/// - `fan_out`: per-source-file writers for outputs flagged
///   `fan_out_per_source_file` in the plan. Outer key is the output
///   name; inner key is the source-file `Arc<str>` (matching the
///   per-record path read from each record's `$source.file` engine-
///   stamped column).
///
/// Auto-converts from `HashMap<String, Box<dyn Write + Send>>` so
/// existing callers that don't need fan-out keep the simpler shape.
#[derive(Default)]
pub struct WriterRegistry {
    pub single: HashMap<String, Box<dyn Write + Send>>,
    pub fan_out: HashMap<String, HashMap<std::sync::Arc<str>, Box<dyn Write + Send>>>,
}

impl From<HashMap<String, Box<dyn Write + Send>>> for WriterRegistry {
    fn from(single: HashMap<String, Box<dyn Write + Send>>) -> Self {
        Self {
            single,
            fan_out: HashMap::new(),
        }
    }
}

/// Helper for callers (mostly tests and benchmarks) that have a single
/// in-memory reader per source: wraps it in the one-element
/// `Vec<FileSlot>` shape that the executor's reader registry expects.
pub fn single_file_reader(
    path: impl Into<std::path::PathBuf>,
    reader: Box<dyn Read + Send>,
) -> Vec<crate::source::multi_file::FileSlot> {
    vec![crate::source::multi_file::FileSlot {
        path: path.into(),
        reader,
    }]
}

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
    /// whose ingest task never finalized (e.g. fatal abort before
    /// `mpsc::Receiver` close) are absent rather than reported as
    /// zero — distinguishes "stream closed with zero records" from
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
}

/// Sum per-stage CPU and I/O deltas into run-level totals. Stages with `None`
/// (e.g. cumulative timers) are skipped. Returns `None` per metric if no stage
/// reported a value, otherwise `Some(sum)`.
fn sum_cpu_io_totals(
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

/// Dummy storage type for streaming (no-window) evaluation.
/// Used to satisfy the `S: RecordStorage` type parameter when `window` is `None`.
pub(crate) struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u64, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u64, _: &str, _: &str) -> Option<&Value> {
        None
    }
    fn available_fields(&self, _: u64) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u64 {
        0
    }
}

/// Compiled transform: CXL source compiled once, evaluated per record.
#[derive(Debug)]
pub struct CompiledTransform {
    pub(crate) name: String,
    pub(crate) typed: Arc<TypedProgram>,
    pub(crate) max_expansion: u64,
}

impl CompiledTransform {
    pub(crate) fn has_distinct(&self) -> bool {
        self.typed
            .program
            .statements
            .iter()
            .any(|s| matches!(s, Statement::Distinct { .. }))
    }
}

/// Compiled route branch: a named CXL boolean condition evaluator.
pub(crate) struct CompiledRouteBranch {
    name: String,
    evaluator: ProgramEvaluator,
}

/// Compiled route configuration for multi-output dispatch.
pub(crate) struct CompiledRoute {
    branches: Vec<CompiledRouteBranch>,
    default: String,
    mode: crate::config::RouteMode,
}

impl CompiledRoute {
    /// Evaluate route conditions against the authoritative Record.
    ///
    /// Returns the list of output names the record should be dispatched to.
    /// In Exclusive mode: first matching branch (or default).
    /// In Inclusive mode: all matching branches (or default if none match).
    ///
    /// Branch predicates resolve field references through the Record's
    /// own [`FieldResolver`] impl — schema + overflow for bare names.
    /// No parallel bookkeeping map is required (Invariant 3).
    pub(crate) fn evaluate(
        &mut self,
        record: &Record,
        ctx: &EvalContext,
    ) -> Result<Vec<String>, cxl::eval::EvalError> {
        match self.mode {
            crate::config::RouteMode::Exclusive => {
                for branch in &mut self.branches {
                    match branch
                        .evaluator
                        .eval_record::<NullStorage>(ctx, record, None)?
                    {
                        EvalResult::Emit { .. } | EvalResult::EmitMany { .. } => {
                            return Ok(vec![branch.name.clone()]);
                        }
                        EvalResult::Skip(_) => continue,
                    }
                }
                Ok(vec![self.default.clone()])
            }
            crate::config::RouteMode::Inclusive => {
                let mut matched = Vec::new();
                for branch in &mut self.branches {
                    match branch
                        .evaluator
                        .eval_record::<NullStorage>(ctx, record, None)?
                    {
                        EvalResult::Emit { .. } | EvalResult::EmitMany { .. } => {
                            matched.push(branch.name.clone());
                        }
                        EvalResult::Skip(_) => {}
                    }
                }
                if matched.is_empty() {
                    matched.push(self.default.clone());
                }
                Ok(matched)
            }
        }
    }
}

/// Record that failed evaluation, queued for DLQ output.
#[derive(Debug, Clone)]
pub struct DlqEntry {
    pub source_row: u64,
    pub category: crate::dlq::DlqErrorCategory,
    pub error_message: String,
    pub original_record: Record,
    /// Pipeline stage where error occurred.
    /// Convention: "source", "transform:{name}", "route_eval", "output:{name}"
    pub stage: Option<String>,
    /// Route branch name if error occurred during or after routing.
    /// None for pre-routing errors.
    pub route: Option<String>,
    /// `true` if this record's own evaluation caused the DLQ entry.
    /// Serialized as `_cxl_dlq_trigger` column in DLQ CSV.
    pub trigger: bool,
    /// Originating Source-node name. Read from the failing record's
    /// `FieldMetadata::SourceName` engine-stamp at the push site so a
    /// post-Merge / post-Combine DLQ entry still identifies which
    /// upstream Source produced the record. Serialized as
    /// `_cxl_dlq_source_name` in DLQ CSV.
    pub source_name: Arc<str>,
    /// Output field the evaluator was computing when the error fired,
    /// captured at the emit-statement boundary. `None` for collateral
    /// entries that were not directly eval-triggered (correlation
    /// fan-out, group-size overflow, etc.). Serialized as
    /// `_cxl_dlq_triggering_field`.
    pub triggering_field: Option<Arc<str>>,
    /// Value carried by the failing `EvalErrorKind` payload, when the
    /// variant exposes one (conversion source string, out-of-bounds
    /// index, mismatched arity). `None` otherwise. Serialized as
    /// `_cxl_dlq_triggering_value`.
    pub triggering_value: Option<clinker_record::Value>,
}

impl DlqEntry {
    /// Stage: source read error.
    pub fn stage_source() -> String {
        "source".into()
    }

    /// Stage: transform evaluation error.
    pub fn stage_transform(name: &str) -> String {
        format!("transform:{name}")
    }

    /// Stage: route condition evaluation error.
    pub fn stage_route_eval() -> String {
        "route_eval".into()
    }

    /// Stage: output write error.
    pub fn stage_output(name: &str) -> String {
        format!("output:{name}")
    }
}

/// Unified pipeline executor. Plan-driven branching:
/// - Streaming (single-pass) when no window functions
/// - TwoPass (arena + indices) when windows are present
pub struct PipelineExecutor;

impl PipelineExecutor {
    /// `&CompiledPlan`-consuming public entry point.
    ///
    /// Accepts the typed `CompiledPlan` handle returned by
    /// [`crate::config::PipelineConfig::compile`] and forwards to
    /// [`Self::run_with_readers_writers`] using the plan's embedded
    /// [`PipelineConfig`]. Every declared Source is ingested through
    /// the same code path; there is no "primary" driving source. DAG
    /// dispatch order is determined by topological walk of the plan,
    /// not by source declaration order.
    ///
    /// Compile-fail guarantee — `&PipelineConfig` is NOT accepted:
    ///
    /// ```compile_fail
    /// use clinker_core::executor::PipelineExecutor;
    /// use clinker_core::config::PipelineConfig;
    /// use std::collections::HashMap;
    /// fn _demo(cfg: &PipelineConfig, params: &clinker_core::executor::PipelineRunParams) {
    ///     let _ = PipelineExecutor::run_plan_with_readers_writers(
    ///         cfg, // ← should be &CompiledPlan, not &PipelineConfig
    ///         HashMap::new(),
    ///         HashMap::new(),
    ///         params,
    ///     );
    /// }
    /// ```
    ///
    /// # Panics
    ///
    /// The executor wraps its heavyweight CPU-bound operator arms
    /// (sort, aggregate finalize, grace-hash, IEJoin, sort-merge) in
    /// [`tokio::task::block_in_place`], which panics when invoked
    /// outside a multi-thread tokio runtime. Callers must drive this
    /// entry from a runtime built via
    /// `tokio::runtime::Builder::new_multi_thread().enable_all().build()`
    /// or annotate tests with `#[tokio::test(flavor = "multi_thread")]`.
    pub async fn run_plan_with_readers_writers<W: Into<WriterRegistry>>(
        plan: &crate::plan::CompiledPlan,
        readers: SourceReaders,
        writers: W,
        params: &PipelineRunParams,
    ) -> Result<ExecutionReport, PipelineError> {
        Self::run_with_readers_writers(plan.config(), readers, writers.into(), params).await
    }

    /// `&CompiledPlan`-consuming `--explain` text entry.
    pub fn explain_plan(plan: &crate::plan::CompiledPlan) -> Result<String, PipelineError> {
        Self::explain(plan.config())
    }

    /// `&CompiledPlan`-consuming `--explain` DAG entry.
    pub fn explain_plan_dag(
        plan: &crate::plan::CompiledPlan,
    ) -> Result<(ExecutionPlanDag, ()), PipelineError> {
        Self::explain_dag(plan.config())
    }

    /// Run with explicit reader/writer registries.
    ///
    /// `readers` and `writers` are keyed by the input/output `name` fields from
    /// the pipeline config. For single-input/output pipelines, pass single-entry
    /// HashMaps. Every declared Source must have a reader entry; missing
    /// readers and empty file lists are hard errors.
    ///
    /// Returns an [`ExecutionReport`] containing record counts, DLQ entries,
    /// execution mode, peak RSS, and wall-clock start/finish timestamps.
    ///
    /// # Panics
    ///
    /// Same constraint as [`Self::run_plan_with_readers_writers`]: the
    /// executor's CPU-bound operator arms call
    /// [`tokio::task::block_in_place`], which panics outside a
    /// multi-thread tokio runtime. Build the runtime via
    /// `tokio::runtime::Builder::new_multi_thread().enable_all().build()`
    /// or use `#[tokio::test(flavor = "multi_thread")]`.
    pub(crate) async fn run_with_readers_writers(
        config: &PipelineConfig,
        readers: SourceReaders,
        writers: WriterRegistry,
        params: &PipelineRunParams,
    ) -> Result<ExecutionReport, PipelineError> {
        Self::run_with_readers_writers_in_context(
            config,
            readers,
            writers,
            params,
            crate::config::CompileContext::default(),
        )
        .await
    }

    /// Variant of [`Self::run_with_readers_writers`] that accepts an
    /// explicit [`crate::config::CompileContext`]. Tests use this to
    /// supply a temp-dir workspace root without mutating CWD —
    /// `CompileContext::default()` reads CWD at call time, which is
    /// not thread-safe across parallel test runs that need different
    /// workspace roots.
    pub(crate) async fn run_with_readers_writers_in_context(
        config: &PipelineConfig,
        mut readers: SourceReaders,
        writers: WriterRegistry,
        params: &PipelineRunParams,
        compile_ctx: crate::config::CompileContext,
    ) -> Result<ExecutionReport, PipelineError> {
        let started_at = Utc::now();

        let source_configs: Vec<_> = config.source_configs().cloned().collect();
        let output_configs: Vec<_> = config.output_configs().cloned().collect();
        if source_configs.is_empty() {
            return Err(PipelineError::Config(
                crate::config::ConfigError::Validation(
                    "pipeline declares no source nodes; nothing to execute".to_string(),
                ),
            ));
        }
        let mut collector = stage_metrics::StageCollector::default();

        // Single canonical compile path.
        //
        // `PipelineConfig::compile(&ctx)` drives the unified nodes
        // pipeline through stages 1-4 (topology/path validation),
        // stage 4.4 (workspace composition scan), stage 4.5 (CXL
        // typecheck via `bind_schema`), and stage 5 (per-variant
        // lowering + full enrichment: source DAG / indices / output
        // projections / parallelism / correlation-sort / enforcer-
        // sort / node properties / aggregation-strategy / combine-
        // strategy post-passes). The runtime `ExecutionPlanDag` comes
        // straight off `validated_plan.dag()` — no re-compile.
        //
        // Aggregate lowering reads `group_by_indices` from
        // `typed.field_types`, which `bind_schema` already keyed and
        // ordered by the upstream node's bound `Row`. The author-
        // declared superset never reaches the index resolver.
        let compile_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Compile);
        let validated_plan =
            config
                .compile(&compile_ctx)
                .map_err(|diags| PipelineError::Compilation {
                    transform_name: String::new(),
                    messages: diags.iter().map(|d| d.message.clone()).collect(),
                })?;
        let resolved_transforms_owned = crate::executor::build_transform_specs(config);
        let resolved_transforms: Vec<&TransformSpec> = resolved_transforms_owned.iter().collect();
        let scoped_vars: cxl::resolve::ScopedVarsRegistry =
            crate::config::build_scoped_vars_registry(config.pipeline.vars.as_ref(), &config.nodes);
        let mut compiled_transforms: Vec<CompiledTransform> = resolved_transforms
            .iter()
            .map(|t| {
                let typed = validated_plan
                    .artifacts()
                    .typed
                    .get(&t.name)
                    .cloned()
                    .ok_or_else(|| PipelineError::Compilation {
                        transform_name: t.name.clone(),
                        messages: vec![
                            "internal: bind_schema produced no typed program for this node"
                                .to_string(),
                        ],
                    })?;
                Ok::<CompiledTransform, PipelineError>(CompiledTransform {
                    name: t.name.clone(),
                    typed,
                    max_expansion: t.max_expansion,
                })
            })
            .collect::<Result<_, _>>()?;

        // Extend with body transforms. Composition bodies' Transform
        // / Aggregate nodes have their typed programs sitting in
        // `artifacts.typed` under the body's node name. The body
        // dispatcher arm looks up `transform_by_name` in the same
        // table the top-level walker uses, so every executable body
        // node has to be present here too. Skip names already
        // present so the top-level entry wins on collision.
        let mut existing_names: std::collections::HashSet<String> =
            compiled_transforms.iter().map(|c| c.name.clone()).collect();
        for body in validated_plan.artifacts().composition_bodies.values() {
            for idx in body.graph.node_indices() {
                let body_node = &body.graph[idx];
                let n = body_node.name();
                if matches!(
                    body_node,
                    crate::plan::execution::PlanNode::Transform { .. }
                        | crate::plan::execution::PlanNode::Aggregation { .. }
                ) && !existing_names.contains(n)
                    && let Some(typed) = validated_plan.artifacts().typed.get(n)
                {
                    compiled_transforms.push(CompiledTransform {
                        name: n.to_string(),
                        typed: typed.clone(),
                        max_expansion: cxl::eval::DEFAULT_MAX_EXPANSION,
                    });
                    existing_names.insert(n.to_string());
                }
            }
        }

        // Compile route conditions if any transform has a route config.
        // Collect all emitted field names for route condition resolution.
        let compiled_route = {
            let route_config = resolved_transforms
                .iter()
                .rev()
                .find_map(|t| t.route.as_ref());
            match route_config {
                Some(rc) => {
                    // Union of user-declared fields across every Source's
                    // bound output_row. Engine-stamped tail columns
                    // ($ck.*, $widened, $source.file) are filtered: Route
                    // conditions reference user-declared field names, not
                    // engine sidecars. Previously this seeded only the
                    // primary source's reader columns, which silently
                    // dropped non-primary fields from Route resolution in
                    // multi-source pipelines.
                    let mut emitted_fields: Vec<String> = Vec::new();
                    for src_cfg in &source_configs {
                        if let Some(typed) = validated_plan.artifacts().typed.get(&src_cfg.name) {
                            for (qf, _) in typed.output_row.fields() {
                                let s = qf.name.to_string();
                                if s.starts_with('$') {
                                    continue;
                                }
                                if !emitted_fields.contains(&s) {
                                    emitted_fields.push(s);
                                }
                            }
                        }
                    }
                    for ct in &compiled_transforms {
                        cxl::ast::for_each_field_emit(
                            &ct.typed.program.statements,
                            &mut |name, _| {
                                if !emitted_fields.iter().any(|s| s == name) {
                                    emitted_fields.push(name.to_string());
                                }
                            },
                        );
                    }
                    // Combine nodes also emit fields via their `cxl:` body;
                    // those typed programs live in `artifacts.typed` keyed
                    // by combine node name (not in `compiled_transforms`,
                    // which is transform-only). A route downstream of a
                    // combine sees the combine's emits, so every emit in
                    // every typed program that's NOT a transform must be
                    // surfaced to the route resolver.
                    let transform_names: std::collections::HashSet<&str> = compiled_transforms
                        .iter()
                        .map(|ct| ct.name.as_str())
                        .collect();
                    for (node_name, typed) in &validated_plan.artifacts().typed {
                        if transform_names.contains(node_name.as_str()) {
                            continue;
                        }
                        cxl::ast::for_each_field_emit(&typed.program.statements, &mut |name, _| {
                            if !emitted_fields.iter().any(|s| s == name) {
                                emitted_fields.push(name.to_string());
                            }
                        });
                    }
                    Some(Self::compile_route(rc, &emitted_fields, &scoped_vars)?)
                }
                None => None,
            }
        };

        // Compile body Routes alongside the top-level singleton.
        // The dispatcher's Route arm consults
        // `compiled_routes_by_name` first; any name found here wins
        // over the singleton, which keeps the long-standing
        // single-top-level-Route path intact while letting body
        // Routes carry their own conditions.
        let mut compiled_routes_by_name: std::collections::HashMap<String, CompiledRoute> =
            std::collections::HashMap::new();
        for body in validated_plan.artifacts().composition_bodies.values() {
            for (route_name, route_body) in &body.route_bodies {
                // Build the body Route's RouteConfig from its parsed
                // RouteBody. The condition resolver needs the field
                // set — for body context the body's input port
                // schema(s) plus everything emitted upstream within
                // the body.
                let conditions: Vec<crate::config::RouteBranch> = route_body
                    .conditions
                    .iter()
                    .map(|(name, cxl)| crate::config::RouteBranch {
                        name: name.clone(),
                        condition: cxl.source.as_str().to_string(),
                    })
                    .collect();
                let route_config = crate::config::RouteConfig {
                    mode: route_body.mode,
                    branches: conditions,
                    default: route_body.default.clone(),
                };
                // Use the union of port column names + any emit on
                // body upstream nodes. A union of declared body
                // schemas is overinclusive but safe — branches'
                // typecheck already passed at bind time, so resolver
                // false-positives won't surface here.
                let mut emitted_fields: Vec<String> = Vec::new();
                for row in body.input_port_rows.values() {
                    for qf in row.field_names() {
                        let s = qf.name.to_string();
                        if !emitted_fields.contains(&s) {
                            emitted_fields.push(s);
                        }
                    }
                }
                for (n, typed) in &validated_plan.artifacts().typed {
                    if !body.name_to_idx.contains_key(n.as_str()) {
                        continue;
                    }
                    cxl::ast::for_each_field_emit(&typed.program.statements, &mut |name, _| {
                        if !emitted_fields.iter().any(|s| s == name) {
                            emitted_fields.push(name.to_string());
                        }
                    });
                }
                let cr = Self::compile_route(&route_config, &emitted_fields, &scoped_vars)?;
                compiled_routes_by_name.insert(route_name.clone(), cr);
            }
        }

        let plan = validated_plan.dag();
        collector.record(compile_timer.finish(0, 0));

        let execution_summary = plan.execution_summary();
        let required_arena = plan.required_arena();

        // Validate that all configured outputs have registered writers.
        // Either the single-writer slot or the fan-out slot must contain
        // the output's name; the dispatcher consults whichever applies.
        for output in &output_configs {
            let registered = writers.single.contains_key(&output.name)
                || writers.fan_out.contains_key(&output.name);
            if !registered {
                return Err(PipelineError::Config(
                    crate::config::ConfigError::Validation(format!(
                        "no writer registered for output '{}'",
                        output.name
                    )),
                ));
            }
        }

        // Pipeline-scoped TempDir. Allocated here (not inside
        // `execute_dag_branching`) so every source-ingest spill file
        // lands under the same panic-recovery sweep as later operator
        // spills. Primary cleanup is per-file `tempfile::TempPath`
        // Drop; secondary sweep is this TempDir's recursive remove on
        // panic unwind.
        let spill_root = Arc::new(
            tempfile::Builder::new()
                .prefix("clinker-spill-")
                .tempdir()
                .map_err(|e| PipelineError::Internal {
                    op: "executor",
                    node: String::new(),
                    detail: format!("failed to allocate pipeline spill root: {e}"),
                })?,
        );
        let spill_root_path: Arc<std::path::Path> = Arc::from(spill_root.path());

        // Pipeline-scoped MemoryArbitrator. One declared `memory.limit`
        // envelopes every node-rooted arena finalize — including the
        // arenas built at Source dispatch-arm exits. Constructed
        // before the source loop so each Source can register a
        // `SourceConsumer` with the arbitrator at its construction
        // site instead of after-the-fact discovery. The active
        // policy is the one `pipeline.memory.backpressure` selects;
        // default `pause` installs `BackPressurePreferred -> Priority`.
        let memory_budget = std::sync::Arc::new(build_arbitrator_from_config(config));

        // ── Unified source ingest pass ───────────────────────────────
        // Every declared Source spawns one `tokio::task` that drives
        // its format reader on a blocking worker and pushes records
        // through a `TokioSourceStream`. The paired `mpsc::Receiver`
        // lands in `source_records[name]`; the dispatch loop's Source
        // arm drains it via `recv().await`. No primary asymmetry;
        // missing reader or empty file list is a hard error.
        // Per-source totals + per-(source, file) watermark observations
        // are returned through each task's `JoinHandle` and folded into
        // the run's accounting once the receivers have drained.
        let reader_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::ReaderInit);
        let counters = PipelineCounters::default();
        let mut source_records: HashMap<String, tokio::sync::mpsc::Receiver<(Record, u64)>> =
            HashMap::new();
        let mut watermarks = crate::executor::watermark::PerSourceWatermarks::new();
        let mut ingest_handles: Vec<
            tokio::task::JoinHandle<Result<IngestTaskOutcome, PipelineError>>,
        > = Vec::with_capacity(source_configs.len());
        for src_cfg in &source_configs {
            // Pre-declare so the report's `iter_declared_sources` view
            // emits a per-source rollup entry even when ingest produces
            // zero observable records (e.g. empty input file).
            if src_cfg.watermark.is_some() {
                watermarks.declare(&src_cfg.name);
            }
            let files = readers.remove(&src_cfg.name).ok_or_else(|| {
                PipelineError::Config(crate::config::ConfigError::Validation(format!(
                    "no reader registered for source '{}'",
                    src_cfg.name
                )))
            })?;
            let (stream, rx) = crate::executor::source_stream::TokioSourceStream::new(
                crate::executor::source_stream::TokioSourceStream::DEFAULT_CAPACITY,
            );
            // Register a `SourceConsumer` with the pipeline-scoped
            // arbitrator at the source's construction site. The
            // handle's `bytes` counter mirrors the ingest channel's
            // queued bytes once that wiring lands; until then the
            // consumer reports zero and serves as a back-pressureable
            // pause target for `BackPressurePreferred::wrapping(Priority)`.
            // The arbitrator owns the boxed wrapper for the run; the
            // arbitrator's Drop releases it on pipeline teardown.
            let source_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
            let _source_consumer_id = memory_budget.register_consumer(Box::new(
                crate::executor::source_stream::SourceConsumer::new(source_consumer_handle.clone()),
            ));
            source_records.insert(src_cfg.name.clone(), rx);
            let src_cfg_owned = src_cfg.clone();
            let config_clone = config.clone();
            ingest_handles.push(tokio::spawn(async move {
                ingest_source_task(src_cfg_owned, files, config_clone, stream).await
            }));
        }

        let DispatchOutcome {
            counters,
            dlq_entries,
            peak_rss_bytes,
            mut watermarks,
            per_source_rollback_cursors,
            per_source_record_counts,
            per_source_dlq_counts,
            cumulative_spill_bytes,
        } = Self::execute_dag(
            config,
            source_records,
            &source_configs,
            writers,
            &compiled_transforms,
            compiled_route,
            compiled_routes_by_name,
            plan,
            validated_plan.artifacts(),
            params,
            &mut collector,
            spill_root,
            spill_root_path,
            counters,
            watermarks,
            memory_budget,
        )
        .await?;

        // Collect ingest-task outcomes: per-source row counts and the
        // per-(source, file) watermark observations each task captured
        // locally. The dispatch path consumed each task's receiver
        // already, so a clean ingest task's join is the synchronization
        // point that confirms readers + spill writers closed without
        // error. A task error here (reader I/O, spill writer failure,
        // closed-receiver — which can only fire if dispatch aborted
        // before draining, in which case the dispatch error fires
        // first) propagates after dispatch's own result.
        let mut total_ingested: u64 = 0;
        let mut counters = counters;
        for handle in ingest_handles {
            let outcome = handle.await.map_err(|e| PipelineError::Internal {
                op: "source-ingest-task",
                node: String::new(),
                detail: format!("tokio task join failed: {e}"),
            })??;
            counters.total_count += outcome.total_count;
            total_ingested += outcome.total_count;
            for (file_arc, ts) in outcome.watermark_observations {
                watermarks.observe(&outcome.source_name, &file_arc, ts);
            }
        }
        collector.record(reader_timer.finish(total_ingested, total_ingested));

        let stages = collector.into_stages();
        let (total_cpu_user_ns, total_cpu_sys_ns, total_io_read_bytes, total_io_write_bytes) =
            sum_cpu_io_totals(&stages);

        // Roll the per-(source, file) watermark map into the report's
        // three granularities. BTreeMap keys are owned `String`s so the
        // report is `'static`-clonable for snapshot testing.
        let per_source_file_watermarks: BTreeMap<(String, String), Option<i64>> = watermarks
            .iter_partitions()
            .map(|(src, file, w)| {
                let ts = match w.status {
                    crate::executor::watermark::WatermarkStatus::Active(ts) => Some(ts),
                    crate::executor::watermark::WatermarkStatus::NoObservation
                    | crate::executor::watermark::WatermarkStatus::Idle => None,
                };
                ((src.to_string(), file.as_ref().to_string()), ts)
            })
            .collect();
        let per_source_watermarks: BTreeMap<String, Option<i64>> = watermarks
            .iter_declared_sources()
            .map(|name| (name.to_string(), watermarks.source_min(name)))
            .collect();
        let declared_source_names: Vec<&str> = watermarks.iter_declared_sources().collect();
        let effective_watermark = watermarks.min_across_sources(&declared_source_names);

        Ok(ExecutionReport {
            counters,
            dlq_entries,
            execution_summary,
            required_arena,
            peak_rss_bytes,
            total_cpu_user_ns,
            total_cpu_sys_ns,
            total_io_read_bytes,
            total_io_write_bytes,
            started_at,
            finished_at: Utc::now(),
            stages,
            per_source_file_watermarks,
            per_source_watermarks,
            effective_watermark,
            per_source_rollback_cursors,
            per_source_record_counts,
            per_source_dlq_counts,
            cumulative_spill_bytes,
        })
    }

    /// Single DAG-driven execution entry point — replaces execute_streaming,
    /// execute_two_pass, and execute_correlated_streaming.
    ///
    /// Walks the DAG in topological order and dispatches per-node based on
    /// `NodeExecutionReqs`. Handles all three execution modes internally:
    /// 1. RequiresArena → build Arena + indices first, then walk DAG with window context
    /// 2. RequiresSortedInput → read all, sort, then walk DAG with group-boundary logic
    /// 3. Streaming → read all, walk DAG with per-record evaluation
    ///
    /// `source_records` holds one live `mpsc::Receiver` per declared
    /// Source. Each `(Record, row_num)` carries the source-file
    /// `Arc<str>` engine-stamped on the `$source.file` column. The
    /// caller's ingest pass spawns a `tokio::task` per Source to push
    /// through a paired `TokioSourceStream`; this function consumes
    /// the receivers via `recv().await` and never touches a
    /// `FormatReader` directly.
    ///
    /// Returns `(counters, dlq_entries, peak_rss_bytes)`.
    #[allow(clippy::too_many_arguments)]
    async fn execute_dag(
        config: &PipelineConfig,
        source_records: HashMap<String, tokio::sync::mpsc::Receiver<(Record, u64)>>,
        source_configs: &[crate::config::SourceConfig],
        writers: WriterRegistry,
        transforms: &[CompiledTransform],
        compiled_route: Option<CompiledRoute>,
        compiled_routes_by_name: HashMap<String, CompiledRoute>,
        plan: &ExecutionPlanDag,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        params: &PipelineRunParams,
        collector: &mut stage_metrics::StageCollector,
        spill_root: Arc<tempfile::TempDir>,
        spill_root_path: Arc<std::path::Path>,
        mut counters: PipelineCounters,
        watermarks: crate::executor::watermark::PerSourceWatermarks,
        memory_budget: std::sync::Arc<crate::pipeline::memory::MemoryArbitrator>,
    ) -> Result<DispatchOutcome, PipelineError> {
        let mut dlq_entries: Vec<DlqEntry> = Vec::new();

        // No prologue drain or arena build. The dispatch Source arm
        // is the first consumer of every `mpsc::Receiver`:
        // - canonicalize per record onto the source's plan-time schema,
        // - seed `$record.<key>` defaults per record,
        // - seed `$source.<key>` defaults per `(source, file)` Arc on
        //   first observation,
        // - on `recv().await` returning `None`, stamp the finalized
        //   per-source count and call `finalize_node_rooted_windows`
        //   to populate every spec rooted at this source's NodeIndex.
        //
        // The D12 global-fold-over-empty-input case fires from the
        // Aggregate arm's own empty-input emit — the topo walk
        // dispatches the Aggregate node even when its upstream Source
        // produced zero records, so the special case still emits the
        // single global-fold row.
        let window_runtime =
            crate::executor::window_runtime::WindowRuntimeRegistry::new(&plan.indices_to_build);

        Self::execute_dag_branching(
            config,
            source_records,
            source_configs,
            writers,
            transforms,
            compiled_route,
            compiled_routes_by_name,
            plan,
            artifacts,
            params,
            &mut counters,
            &mut dlq_entries,
            collector,
            window_runtime,
            memory_budget,
            spill_root,
            spill_root_path,
            watermarks,
        )
        .await
    }

    /// Execute a branching DAG by walking nodes in topological order.
    ///
    /// Records flow through inter-node buffers. Route nodes partition records
    /// into branch-specific buffers. Merge nodes concatenate predecessor
    /// buffers in declaration order. Transform nodes evaluate a single CXL
    /// program per record.
    ///
    /// Branch dispatch is sequential: each branch runs its transform chain
    /// in topo order. Industry consensus (DataFusion, Polars, DuckDB, Flink)
    /// is partition-level parallelism (par_iter_mut on chunks), not
    /// branch-level fork-join — scheduling overhead exceeds benefit at
    /// typical ETL branch sizes (2-4 branches, millisecond chains).
    #[allow(clippy::too_many_arguments)]
    async fn execute_dag_branching(
        config: &PipelineConfig,
        source_records: HashMap<String, tokio::sync::mpsc::Receiver<(Record, u64)>>,
        source_configs: &[crate::config::SourceConfig],
        mut writers: WriterRegistry,
        transforms: &[CompiledTransform],
        compiled_route: Option<CompiledRoute>,
        compiled_routes_by_name: HashMap<String, CompiledRoute>,
        plan: &ExecutionPlanDag,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        params: &PipelineRunParams,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        collector: &mut stage_metrics::StageCollector,
        window_runtime: crate::executor::window_runtime::WindowRuntimeRegistry,
        memory_budget: std::sync::Arc<crate::pipeline::memory::MemoryArbitrator>,
        spill_root: Arc<tempfile::TempDir>,
        spill_root_path: Arc<std::path::Path>,
        watermarks: crate::executor::watermark::PerSourceWatermarks,
    ) -> Result<DispatchOutcome, PipelineError> {
        let output_configs: Vec<_> = config.output_configs().cloned().collect();
        let pipeline_start_time = chrono::Local::now().naive_local();

        // Pipeline-stable evaluation context. Built once here, reused
        // (via borrow) at every per-record dispatch site below.
        let stable = build_stable_eval_context(
            config,
            pipeline_start_time,
            &params.execution_id,
            &params.batch_id,
            &params.pipeline_vars,
            &params.static_vars,
        );

        // Per-record `$record.<key>` defaults. The Source dispatch
        // arm applies these per record at canonicalize time via
        // `Record::seed_record_vars`; no upfront walk of materialized
        // records. Channel-supplied record_vars layer atop Transform-
        // `declares: scope: record` defaults; existing per-record
        // entries (if any) are preserved by `seed_record_vars`.
        let record_var_seed: IndexMap<String, Value> = {
            let mut seed = crate::config::collect_record_var_defaults(&config.nodes);
            for (k, v) in &params.record_vars {
                seed.insert(k.clone(), v.clone());
            }
            seed
        };

        // Source-scope variable defaults from
        // `declares: scope: source`. The Source dispatch arm seeds
        // `stable.source_vars` per `(source, file)` Arc on first
        // observation; channel overrides from `params.source_vars`
        // layer atop the declared defaults.
        let declared_source_defaults = crate::config::collect_source_var_defaults(&config.nodes);

        // Per-source idle-timeout durations derived from each
        // `SourceConfig.watermark.idle_timeout`. Borrowed by the
        // Source dispatch arm to wrap its `rx.recv().await` in
        // `tokio::time::timeout`. Sources without a configured
        // timeout fall through to unbounded recv.
        let idle_timeouts: HashMap<String, std::time::Duration> = source_configs
            .iter()
            .filter_map(|s| {
                s.watermark
                    .as_ref()
                    .and_then(|w| w.idle_timeout.map(|d| (s.name.clone(), d)))
            })
            .collect();

        // `$source.batch` is per-pipeline-run scalar today (sub-issue
        // #54 introduces per-source attribution as a separate stamp).
        let source_batch_arc: Arc<str> = Arc::from(uuid::Uuid::now_v7().to_string());
        let source_ingestion_timestamp = pipeline_start_time;

        let strategy = config.error_handling.strategy;

        // Name → index map for looking up `CompiledTransform` by node
        // name. Borrowed by the dispatcher's Transform / Aggregation
        // arms.
        let transform_by_name: HashMap<&str, usize> = transforms
            .iter()
            .enumerate()
            .map(|(i, t)| (t.name.as_str(), i))
            .collect();

        // Correlation grouping context. `Some(...)` iff at least one
        // source declares a `correlation_key:`; the planner's
        // `inject_correlation_commit` pass guarantees a terminal
        // `PlanNode::CorrelationCommit` is also present so the
        // dispatcher walks the buffers at end-of-DAG. Group identity
        // travels through the schema as `$ck.<field>` shadow columns
        // stamped at Source ingest; the dispatcher reads them via the
        // engine-stamp annotation on each shadow column's
        // `FieldMetadata`, so the executor context only needs to know
        // whether buffering is active and the per-group cap.
        let (correlation_buffers, correlation_max_group_buffer) =
            if config.any_source_has_correlation_key() {
                let cap = config.error_handling.max_group_buffer.unwrap_or(100_000);
                (Some(HashMap::new()), cap)
            } else {
                (None, 0)
            };

        // Pre-seed each declared source's slot at `None` so the Source
        // dispatch arm can flip to `Some(n)` at receiver close.
        // [`MERGED_SOURCE_NAME`] gets its own slot; populated when
        // every per-source slot is `Some` (see
        // `ExecutorContext::finalize_source_count`).
        let mut source_count_per_source: HashMap<Arc<str>, Option<u64>> = source_configs
            .iter()
            .map(|s| (Arc::from(s.name.as_str()), None))
            .collect();
        source_count_per_source.insert(Arc::clone(&dispatch::MERGED_SOURCE_NAME), None);

        // Per-source running record count, advanced by the Source
        // dispatch arm as it canonicalizes each record. Used as the
        // denominator by the DLQ rate-threshold check
        // (`check_dlq_rate`); the existing `min_records` floor
        // prevents false positives on the first few records.
        let total_per_source: HashMap<Arc<str>, u64> = source_configs
            .iter()
            .map(|s| (Arc::from(s.name.as_str()), 0u64))
            .collect();

        // Identify Merge.interleave nodes whose predecessors are all
        // Sources. Those Source receivers move out of
        // `ctx.source_records` and into the fused Merge arm's
        // `tokio::select!` so a slow upstream Source no longer blocks
        // peer Sources' channels from filling — back-pressure flows
        // end-to-end. Per-Source arms detect membership in
        // `fused_sources` and return cleanly.
        let init_phase_set = compute_init_phase_node_set(plan);
        let mut fused_sources: HashSet<String> =
            compute_merge_interleave_fused_sources(plan, config);
        // Extend `fused_sources` with Source names whose receivers are
        // claimed by a downstream `PlanNode::Transform` running in
        // streaming mode (issue #74). `fused_transforms` carries the
        // matching Transform `NodeIndex`es so the Transform arm can
        // dispatch into the streaming branch instead of consuming a
        // pre-drained Vec from `node_buffers`.
        let (extra_fused_sources, fused_transforms) =
            compute_transform_fused_sources(plan, &fused_sources, &init_phase_set);
        fused_sources.extend(extra_fused_sources);

        // Streaming-Output setup (issue #72). For every fused
        // `Merge.interleave → single Output` chain that satisfies the
        // eligibility predicate, take the writer out of `writers.single`
        // and spawn a `tokio::spawn`-ed task that drains a bounded
        // `mpsc::channel`. The Merge arm streams records into the
        // channel as it produces them; the Output arm's topo turn
        // becomes a no-op because its writer has already moved into the
        // streaming task. `JoinHandle`s are stored on the context so
        // the dispatcher can await them at end-of-DAG and fold the
        // per-task counter / timer / error accounting back into the
        // context.
        let streaming_specs = compute_streaming_output_specs(
            plan,
            config,
            &fused_sources,
            &init_phase_set,
            &output_configs,
            &writers,
        );
        let mut streaming_output_senders: HashMap<
            petgraph::graph::NodeIndex,
            tokio::sync::mpsc::Sender<(Record, u64)>,
        > = HashMap::new();
        let mut streaming_output_nodes: HashSet<petgraph::graph::NodeIndex> = HashSet::new();
        let mut streaming_output_tasks: Vec<tokio::task::JoinHandle<StreamingOutputTaskOutput>> =
            Vec::new();
        for spec in streaming_specs {
            let raw_writer = writers
                .single
                .remove(&spec.output_name)
                .expect("compute_streaming_output_specs verified writers.single contains output");
            // 256 is the bounded channel capacity. The writer task
            // typically clears each record in microseconds; capacity
            // above ~256 buys no measured throughput but burns memory
            // on the slow-writer / fast-merge end of the back-pressure
            // curve. Mirrors the Source ingest channel sizing (issue
            // #67) — the same bound paces both ends of the pipeline.
            let (tx, rx) = tokio::sync::mpsc::channel::<(Record, u64)>(256);
            let merge_idx = spec.merge_idx;
            let output_idx = spec.output_idx;
            let handle = tokio::spawn(streaming_output_task(rx, raw_writer, spec));
            streaming_output_senders.insert(merge_idx, tx);
            streaming_output_nodes.insert(output_idx);
            streaming_output_tasks.push(handle);
        }

        let mut ctx = dispatch::ExecutorContext {
            config,
            artifacts,
            output_configs: &output_configs,
            primary_output: &output_configs[0],
            compiled_transforms: transforms,
            transform_by_name,
            stable: &stable,
            source_batch_arc: &source_batch_arc,
            source_count_per_source,
            source_ingestion_timestamp,
            strategy,

            node_buffers: HashMap::new(),
            source_records,
            fused_sources,
            fused_transforms,
            record_var_seed: &record_var_seed,
            declared_source_defaults: &declared_source_defaults,
            channel_source_vars: &params.source_vars,
            idle_timeouts: &idle_timeouts,
            source_vars_seeded_files: HashMap::new(),
            writers: writers.single,
            fan_out_writers: writers.fan_out,
            compiled_route,
            counters: std::mem::take(counters),
            dlq_entries: std::mem::take(dlq_entries),
            dlq_per_source: HashMap::new(),
            total_per_source,
            rollback_cursors: HashMap::new(),
            combine_input_snapshots: HashMap::new(),
            output_errors: Vec::new(),
            ok_source_rows: HashSet::new(),
            records_emitted: 0,
            transform_timer: stage_metrics::CumulativeTimer::new(),
            route_timer: stage_metrics::CumulativeTimer::new(),
            projection_timer: stage_metrics::CumulativeTimer::new(),
            write_timer: stage_metrics::CumulativeTimer::new(),
            collector,
            recursion_depth: 0,
            compiled_routes_by_name,
            current_body_node_input_refs: None,
            spill_root,
            spill_root_path,
            window_runtime,
            watermarks,
            memory_budget,
            correlation_buffers,
            correlation_max_group_buffer,
            relaxed_aggregator_states: HashMap::new(),
            relaxed_aggregator_degrade: Vec::new(),
            commit_step_path: dispatch::CommitStepPath::NotSelected,
            region_input_buffers: HashMap::new(),
            in_deferred_dispatch: false,
            streaming_output_senders,
            streaming_output_nodes,
            streaming_output_tasks,
        };

        // Walk DAG in topological order. `topo_order` is cloned so
        // the iterator does not hold a whole-struct immutable borrow
        // of `plan` for the loop body — `&mut ctx` already borrows
        // `ctx.current_dag` (which aliases `plan`) at the field
        // granularity through every dispatcher call.
        //
        // two-pass walk for init-phase orchestration.
        // Pass 1 dispatches every node in the init-phase ancestor
        // closure (Source/Aggregate/etc. feeding `phase: init`
        // Transforms, plus the init-phase Transforms themselves).
        // Pass 2 dispatches every other node — the runtime DAG.
        // Init-phase Transforms are E164-validated as terminal, so
        // Pass 2 never references their output edge. The
        // dispatcher's `remaining_consumers` heuristic counts
        // neighbors based on graph structure (pass-independent), so
        // a Source feeding both an init branch and a runtime branch
        // correctly clones its buffer for Pass 1's first consumer
        // and removes it for Pass 2's last consumer.
        // Topo walk. Wrapped so a `?` error inside doesn't short-circuit
        // past the streaming-output task join below — the spawned tasks
        // own writers we still need to flush (or drop) before the
        // function returns, and dropping `ctx.streaming_output_senders`
        // on the error path is what signals the tasks to close their
        // channel and run their flush.
        let walk_result: Result<(), PipelineError> = async {
            if !init_phase_set.is_empty() {
                for node_idx in plan.topo_order.clone() {
                    if init_phase_set.contains(&node_idx) {
                        dispatch::dispatch_plan_node(&mut ctx, plan, node_idx).await?;
                    }
                }
                for node_idx in plan.topo_order.clone() {
                    if !init_phase_set.contains(&node_idx) {
                        dispatch::dispatch_plan_node(&mut ctx, plan, node_idx).await?;
                    }
                }
            } else {
                for node_idx in plan.topo_order.clone() {
                    dispatch::dispatch_plan_node(&mut ctx, plan, node_idx).await?;
                }
            }
            Ok(())
        }
        .await;

        // Streaming-output drain. Drop every remaining sender so the
        // tasks' `rx.recv().await` returns `None` and they fall through
        // to their flush path. The fused Merge arm normally removes its
        // sender at clean exit; remaining entries here are the error-
        // path leftovers (Merge arm never ran) or pipelines where no
        // streaming chain was eligible (the map is empty).
        ctx.streaming_output_senders.clear();
        for handle in std::mem::take(&mut ctx.streaming_output_tasks) {
            match handle.await {
                Ok(out) => out.fold_into(&mut ctx),
                Err(join_err) => {
                    ctx.output_errors.push(PipelineError::Internal {
                        op: "streaming_output",
                        node: String::from("<unknown>"),
                        detail: format!("streaming output task panicked: {join_err}"),
                    });
                }
            }
        }

        walk_result?;

        // Take the pipeline-scoped TempDir out of the context. Held
        // until the very end of the walk so any operator-side spill
        // files survive the topo loop; dropped explicitly below
        // after the metrics flush so the directory's recursive
        // remove runs after every reader has gone away.
        let pipeline_spill_root = ctx.spill_root().clone();

        // Drain mutable per-walk state out of the context for the
        // post-walk reporting + caller-facing return tuple. Pipeline-
        // wide ingest total is reconstructed from the per-source
        // running counters the Source dispatch arm advanced — the
        // pre-drain prologue no longer exists, so we derive it at
        // exit instead of pre-computing it.
        let transform_timer = ctx.transform_timer;
        let route_timer = ctx.route_timer;
        let projection_timer = ctx.projection_timer;
        let write_timer = ctx.write_timer;
        let records_emitted = ctx.records_emitted;
        let output_errors = ctx.output_errors;
        let collector = ctx.collector;
        let total_records: u64 = ctx.total_per_source.values().sum();
        *counters = ctx.counters;
        *dlq_entries = ctx.dlq_entries;

        collector.record(transform_timer.finish(
            stage_metrics::StageName::TransformEval,
            total_records,
            records_emitted,
        ));
        collector.record(projection_timer.finish(
            stage_metrics::StageName::Projection,
            records_emitted,
            records_emitted,
        ));
        collector.record(route_timer.finish(
            stage_metrics::StageName::RouteEval,
            total_records,
            records_emitted,
        ));
        collector.record(write_timer.finish(
            stage_metrics::StageName::Write,
            records_emitted,
            records_emitted,
        ));

        // Aggregate Output errors collected during the topo walk.
        // Single error → bare error; ≥2 errors →
        // `PipelineError::Multiple` (the DataFusion collection-pattern
        // shape). Zero errors → fall through to Ok.
        match output_errors.len() {
            0 => {}
            1 => return Err(output_errors.into_iter().next().unwrap()),
            _ => return Err(PipelineError::Multiple(output_errors)),
        }

        // Release the pipeline-scoped TempDir Arc clone last; the
        // directory drops once the original `ctx.spill_root` and
        // this clone are both gone. Spill operators have already
        // released their per-file `TempPath` handles by this point;
        // any path the dispatcher couldn't drain still lives inside
        // this directory, and its Drop now sweeps the filesystem.
        drop(pipeline_spill_root);

        let rollback_cursors: BTreeMap<String, u64> = ctx
            .rollback_cursors
            .iter()
            .map(|(k, &v)| (k.as_ref().to_string(), v))
            .collect();
        let merged_key: &Arc<str> = &crate::executor::dispatch::MERGED_SOURCE_NAME;
        let per_source_record_counts: BTreeMap<String, u64> = ctx
            .source_count_per_source
            .iter()
            .filter(|(k, _)| !Arc::ptr_eq(k, merged_key))
            .filter_map(|(k, v)| v.map(|n| (k.as_ref().to_string(), n)))
            .collect();
        let per_source_dlq_counts: BTreeMap<String, u64> = ctx
            .dlq_per_source
            .iter()
            .filter(|(k, v)| !Arc::ptr_eq(k, merged_key) && **v > 0)
            .map(|(k, v)| (k.as_ref().to_string(), *v))
            .collect();

        let cumulative_spill_bytes = ctx.memory_budget.cumulative_spill_bytes();
        Ok(DispatchOutcome {
            counters: std::mem::take(counters),
            dlq_entries: std::mem::take(dlq_entries),
            peak_rss_bytes: rss_bytes(),
            watermarks: ctx.watermarks,
            per_source_rollback_cursors: rollback_cursors,
            per_source_record_counts,
            per_source_dlq_counts,
            cumulative_spill_bytes,
        })
    }

    /// Compile route conditions from the last transform's RouteConfig.
    ///
    /// Each branch condition is compiled as `filter <condition>` — a one-statement
    /// CXL program. The filter returns Emit if true (route matches), Skip(Filtered)
    /// if false (no match). This reuses the existing filter evaluation pattern.
    fn compile_route(
        route_config: &crate::config::RouteConfig,
        emitted_fields: &[String],
        scoped_vars: &cxl::resolve::ScopedVarsRegistry,
    ) -> Result<CompiledRoute, PipelineError> {
        let type_cols: IndexMap<cxl::typecheck::QualifiedField, Type> = emitted_fields
            .iter()
            .map(|f| (cxl::typecheck::QualifiedField::bare(f.as_str()), Type::Any))
            .collect();
        let type_schema = cxl::typecheck::Row::closed(type_cols, cxl::lexer::Span::new(0, 0));
        let field_refs: Vec<&str> = emitted_fields.iter().map(|s| s.as_str()).collect();

        let mut branches = Vec::with_capacity(route_config.branches.len());
        for branch in &route_config.branches {
            // Compile condition as "filter <condition>"
            let cxl_source = format!("filter {}", branch.condition);

            let parse_result = cxl::parser::Parser::parse(&cxl_source);
            if !parse_result.errors.is_empty() {
                let messages: Vec<String> = parse_result
                    .errors
                    .iter()
                    .map(|e| e.message.clone())
                    .collect();
                return Err(PipelineError::Compilation {
                    transform_name: format!("route:{}", branch.name),
                    messages,
                });
            }

            let resolved = cxl::resolve::resolve_program_with_modules_and_vars(
                parse_result.ast,
                &field_refs,
                parse_result.node_count,
                &std::collections::HashMap::new(),
                scoped_vars,
            )
            .map_err(|diags| PipelineError::Compilation {
                transform_name: format!("route:{}", branch.name),
                messages: diags.into_iter().map(|d| d.message).collect(),
            })?;

            let typed = cxl::typecheck::pass::type_check_with_mode_and_vars(
                resolved,
                &type_schema,
                cxl::typecheck::pass::AggregateMode::Row,
                scoped_vars,
            )
            .map_err(|diags| {
                let errors: Vec<String> = diags
                    .iter()
                    .filter(|d| !d.is_warning)
                    .map(|d| d.message.clone())
                    .collect();
                PipelineError::Compilation {
                    transform_name: format!("route:{}", branch.name),
                    messages: if errors.is_empty() {
                        diags.into_iter().map(|d| d.message).collect()
                    } else {
                        errors
                    },
                }
            })?;

            branches.push(CompiledRouteBranch {
                name: branch.name.clone(),
                evaluator: ProgramEvaluator::new(Arc::new(typed), false),
            });
        }

        Ok(CompiledRoute {
            branches,
            default: route_config.default.clone(),
            mode: route_config.mode,
        })
    }

    /// Compile execution plan and return `--explain` output without reading data.
    ///
    /// Input files are NOT opened. Field names are extracted from CXL AST
    /// so the resolver can compile without a data-derived schema.
    pub(crate) fn explain(config: &PipelineConfig) -> Result<String, PipelineError> {
        let (plan, _) = Self::explain_dag(config)?;
        Ok(plan.explain_full(config))
    }

    /// Compile execution plan and return the DAG for format-specific rendering.
    ///
    /// `PipelineConfig::compile(&ctx)` is the single canonical compile
    /// entry point. It produces a fully-enriched `ExecutionPlanDag` via
    /// `bind_schema` (stage 4.5) and stage 5 lowering + enrichment.
    /// Aggregate lowering reads `group_by_indices` from
    /// `typed.field_types`, which is keyed by the upstream node's bound
    /// `Row` — the same path the runtime executor uses, so `--explain`
    /// matches runtime layout without needing live reader columns.
    pub(crate) fn explain_dag(
        config: &PipelineConfig,
    ) -> Result<(ExecutionPlanDag, ()), PipelineError> {
        let validated_plan = config
            .compile(&crate::config::CompileContext::default())
            .map_err(|diags| PipelineError::Compilation {
                transform_name: String::new(),
                messages: diags.iter().map(|d| d.message.clone()).collect(),
            })?;
        Ok((validated_plan.dag().clone(), ()))
    }
}

/// Build a format-specific reader from input config and raw reader.
///
/// Dispatches on `InputFormat` to construct the correct reader type.
/// Returns `Box<dyn FormatReader>` — all downstream code uses trait methods
/// (`schema()`, `next_record()`).
fn build_format_reader(
    input: &crate::config::SourceConfig,
    reader: Box<dyn Read + Send>,
) -> Result<Box<dyn FormatReader>, PipelineError> {
    match &input.format {
        crate::config::InputFormat::Csv(opts) => {
            let config = build_csv_reader_config(opts.as_ref());
            Ok(Box::new(CsvReader::from_reader(reader, config)))
        }
        crate::config::InputFormat::Json(opts) => {
            let config = build_json_reader_config(opts.as_ref(), input.array_paths.as_deref());
            Ok(Box::new(JsonReader::from_reader(reader, config)?))
        }
        crate::config::InputFormat::Xml(opts) => {
            let config = build_xml_reader_config(opts.as_ref(), input.array_paths.as_deref());
            let buf_reader = std::io::BufReader::new(reader);
            Ok(Box::new(XmlReader::new(buf_reader, config)))
        }
        crate::config::InputFormat::FixedWidth(opts) => {
            let fields = extract_field_defs(input)?;
            let config = build_fw_reader_config(opts.as_ref());
            Ok(Box::new(FixedWidthReader::new(reader, fields, config)?))
        }
    }
}

/// Build a [`MultiFileFormatReader`] wrapping every file the discovery
/// layer returned for this source. Single-file sources go through the
/// same path with a one-element slot list — the wrapper short-circuits
/// transparently.
fn build_multi_file_reader(
    input: &crate::config::SourceConfig,
    files: Vec<crate::source::multi_file::FileSlot>,
) -> Result<Box<dyn FormatReader>, PipelineError> {
    use crate::source::multi_file::{FactoryFn, MultiFileFormatReader};

    // The factory closure captures the source config by clone so each
    // file gets a fresh format reader configured identically. Format
    // construction errors map to the wrapper's `Schema` variant via
    // `clinker_format::FormatError` so they bubble through the trait
    // boundary intact.
    let owned_config = input.clone();
    let factory: Box<FactoryFn> = Box::new(
        move |reader: Box<dyn Read + Send>,
              _idx: usize|
              -> Result<Box<dyn FormatReader>, clinker_format::FormatError> {
            build_format_reader(&owned_config, reader).map_err(|e| {
                clinker_format::FormatError::SchemaInference(format!(
                    "format reader construction failed: {e}"
                ))
            })
        },
    );
    Ok(Box::new(MultiFileFormatReader::new(files, factory)))
}

/// Wrap a format reader with schema-based type coercion if the source
/// declares typed columns in its `schema:` block.
fn wrap_with_schema_coercion(
    reader: Box<dyn FormatReader>,
    config: &PipelineConfig,
    source_name: &str,
) -> Result<Box<dyn FormatReader>, PipelineError> {
    use crate::config::PipelineNode;
    use crate::config::pipeline_node::OnUnmapped;

    // Find the source node's schema declaration + on_unmapped policy
    // + format. Format is needed for the auto_widen-on-fixed-width
    // structural-inertness diagnostic below.
    let body_data = config.nodes.iter().find_map(|s| {
        if let PipelineNode::Source {
            header,
            config: body,
        } = &s.value
            && header.name == source_name
        {
            return Some((
                &body.schema.columns,
                body.on_unmapped.clone(),
                body.source.format.clone(),
            ));
        }
        None
    });

    match body_data {
        Some((columns, policy, format)) if !columns.is_empty() => {
            // Fixed-width format: the reader's schema is constructed
            // positionally from the user-declared `FieldDef` list,
            // so the reader cannot ever produce an "undeclared
            // field" — every byte that isn't covered by a FieldDef
            // is structurally invisible. `auto_widen`'s sidecar
            // therefore stays Null forever for fixed-width sources.
            // Surface the inertness once per source at compile time
            // so a user who set `auto_widen` (the engine-wide
            // default) on a fixed-width source sees the no-op
            // explicitly and can either pick `drop`/`reject` (the
            // honest scalar policies for fixed-width) or accept the
            // empty sidecar.
            if matches!(policy, OnUnmapped::AutoWiden)
                && matches!(format, crate::config::InputFormat::FixedWidth(_))
            {
                tracing::info!(
                    source = source_name,
                    "on_unmapped: auto_widen is structurally inert for fixed_width sources — \
                     the schema is positional, so the reader cannot detect undeclared \
                     fields. The `$widened` sidecar slot will always carry Value::Null. \
                     Set `on_unmapped: drop` or `reject` to make the policy explicit."
                );
            }
            let coercing = crate::pipeline::schema_coerce::CoercingReader::new(
                reader,
                columns,
                policy,
                source_name,
            )
            .map_err(|e| PipelineError::Compilation {
                transform_name: source_name.to_string(),
                messages: vec![format!("schema coercion init error: {e}")],
            })?;
            Ok(Box::new(coercing))
        }
        _ => Ok(reader),
    }
}

/// Convert a record value at a declared watermark column into the
/// canonical i64-nanoseconds event-time stamp folded into
/// [`crate::executor::watermark::PerSourceWatermarks`].
///
/// Accepts [`clinker_record::Value::DateTime`] (preserved at nano
/// resolution via `and_utc().timestamp_nanos_opt()`) and
/// [`clinker_record::Value::Date`] (anchored at 00:00:00 UTC). Null,
/// non-temporal, or out-of-`i64`-nanos-range values return `None`
/// and are silently skipped — late-record dropping is the time-window
/// operator's job.
fn value_to_event_time_nanos(value: &clinker_record::Value) -> Option<i64> {
    use clinker_record::Value;
    match value {
        Value::DateTime(nd) => nd.and_utc().timestamp_nanos_opt(),
        Value::Date(d) => d
            .and_hms_opt(0, 0, 0)
            .and_then(|nd| nd.and_utc().timestamp_nanos_opt()),
        _ => None,
    }
}

/// Ingest a Source's records into a bounded
/// [`TokioSourceStream`](crate::executor::source_stream::TokioSourceStream)
/// so the dispatch loop drains the paired `mpsc::Receiver` via
/// `recv().await`. Used uniformly by every declared Source — no
/// primary asymmetry.
///
/// Each ingested record is widened with two tail engine-stamped
/// columns:
///
/// - `$source.file` (`FieldMetadata::SourceFile`) — per-record
///   originating file `Arc<str>` from
///   `MultiFileFormatReader::current_source_file()`.
/// - `$source.name` (`FieldMetadata::SourceName`) — per-record
///   originating Source-node name as a shared `Arc<str>`. One Arc per
///   Source, cloned by every record from that Source — the runtime
///   cost is one Arc bump per ingested row.
///
/// Stamping at ingest lets `$source.file` / `$source.path` /
/// `$source.name` resolution survive Merge / Combine downstream
/// without external row-keyed arrays. `counters.total_count`
/// increments per record so every source contributes to the
/// pipeline-wide total.
/// One ingest task's outcome, collected by the executor entry once
/// dispatch has drained the paired receiver. The watermark
/// observations are folded back into the executor's owned
/// `PerSourceWatermarks` (per-(source, file) max-event-time map);
/// `total_count` increments `counters.total_count` and seeds the
/// `$source.count` pipeline-wide total.
struct IngestTaskOutcome {
    source_name: String,
    total_count: u64,
    watermark_observations: Vec<(Arc<str>, i64)>,
}

/// Drive one Source's format reader on a tokio-blocking worker and
/// push every record through `stream`. Watermark observations are
/// captured locally so the executor's `PerSourceWatermarks` is owned
/// by a single task (no shared-mutex contention). The blocking
/// closure inside `tokio::task::spawn_blocking` runs the synchronous
/// reader; records are forwarded via `Sender::blocking_send` so the
/// channel's back-pressure semantics still apply.
async fn ingest_source_task(
    src_cfg: crate::config::SourceConfig,
    files: Vec<crate::source::multi_file::FileSlot>,
    config: PipelineConfig,
    stream: crate::executor::source_stream::TokioSourceStream,
) -> Result<IngestTaskOutcome, PipelineError> {
    if files.is_empty() {
        return Err(PipelineError::Config(
            crate::config::ConfigError::Validation(format!(
                "source '{}' has empty file list",
                src_cfg.name
            )),
        ));
    }
    // Keep an owned copy of the source name so the post-await error
    // path retains attribution even though the blocking closure moves
    // `src_cfg` in.
    let source_name = src_cfg.name.clone();

    // `spawn_blocking` because every layer below — the format reader,
    // schema-coercion wrapper, file I/O — is synchronous. The
    // `TokioSourceStream` sender's `blocking_send` lives inside this
    // closure to preserve channel back-pressure even though the
    // producer is on a blocking worker.
    let join = tokio::task::spawn_blocking(move || -> Result<IngestTaskOutcome, PipelineError> {
        let raw_reader = build_multi_file_reader(&src_cfg, files)?;
        let mut src_reader = wrap_with_schema_coercion(raw_reader, &config, &src_cfg.name)?;
        let reader_schema = src_reader.schema()?;

        // Widen the reader schema with `$source.file`, `$source.name`,
        // and `$source.event_time` engine-stamped tail columns. The
        // stamps travel with every record so downstream operators
        // resolve them per-record via column reads (see
        // `dispatch::source_file_arc_of`, `dispatch::source_name_arc_of`,
        // and the time-windowed aggregate arm) instead of indexing
        // external row-keyed Vecs. Tail order is load-bearing — see
        // `bind_schema::columns_from_decl`: `$ck.<field>` shadows first,
        // then `$widened`, then `$source.file`, then `$source.name`,
        // then `$source.event_time` last.
        let mut widened_builder =
            clinker_record::SchemaBuilder::with_capacity(reader_schema.column_count() + 3);
        for (idx, col) in reader_schema.columns().iter().enumerate() {
            widened_builder = match reader_schema.field_metadata(idx) {
                Some(meta) => widened_builder.with_field_meta(col.as_ref(), meta.clone()),
                None => widened_builder.with_field(col.as_ref()),
            };
        }
        let widened_schema = widened_builder
            .with_field_meta(
                crate::config::pipeline_node::SOURCE_FILE_COLUMN,
                clinker_record::FieldMetadata::source_file(),
            )
            .with_field_meta(
                crate::config::pipeline_node::SOURCE_NAME_COLUMN,
                clinker_record::FieldMetadata::source_name(),
            )
            .with_field_meta(
                crate::config::pipeline_node::SOURCE_EVENT_TIME_COLUMN,
                clinker_record::FieldMetadata::source_event_time(),
            )
            .build();

        let static_source_file: Arc<str> = if src_cfg.path_str().is_empty() {
            Arc::from("<source>")
        } else {
            Arc::from(src_cfg.path_str())
        };
        let source_name_arc: Arc<str> = Arc::from(src_cfg.name.as_str());

        let (watermark_column_idx, delay_nanos): (Option<usize>, i64) =
            match src_cfg.watermark.as_ref() {
                None => (None, 0),
                Some(wm) => {
                    let idx = widened_schema.index(wm.column.as_str()).ok_or_else(|| {
                        PipelineError::Config(crate::config::ConfigError::Validation(format!(
                            "source '{}' declares watermark.column = '{}' but no such column \
                         exists on the runtime schema (declared columns: {:?})",
                            src_cfg.name,
                            wm.column,
                            widened_schema.columns(),
                        )))
                    })?;
                    // Saturating i64 nanos — a 292-year `Duration` is the
                    // theoretical ceiling. Anything beyond saturates to
                    // `i64::MAX` and stays a useful (if extreme) shift.
                    let delay_nanos = wm
                        .delay
                        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
                        .unwrap_or(0);
                    (Some(idx), delay_nanos)
                }
            };

        let mut stream = stream;
        let mut rn: u64 = 0;
        let mut total_count: u64 = 0;
        let mut watermark_observations: Vec<(Arc<str>, i64)> = Vec::new();
        loop {
            match src_reader.next_record() {
                Ok(Some(record)) => {
                    rn += 1;
                    total_count += 1;
                    let file_arc = src_reader
                        .current_source_file()
                        .cloned()
                        .unwrap_or_else(|| Arc::clone(&static_source_file));
                    let mut values: Vec<clinker_record::Value> = record.values().to_vec();
                    let event_time_value: clinker_record::Value = if let Some(idx) =
                        watermark_column_idx
                        && let Some(value) = values.get(idx)
                        && let Some(raw_nanos) = value_to_event_time_nanos(value)
                    {
                        let effective = raw_nanos.saturating_sub(delay_nanos);
                        watermark_observations.push((Arc::clone(&file_arc), effective));
                        clinker_record::Value::Integer(effective)
                    } else {
                        clinker_record::Value::Null
                    };
                    values.push(clinker_record::Value::String(
                        file_arc.as_ref().to_string().into_boxed_str(),
                    ));
                    values.push(clinker_record::Value::String(
                        source_name_arc.as_ref().to_string().into_boxed_str(),
                    ));
                    values.push(event_time_value);
                    let widened_record =
                        clinker_record::Record::new(Arc::clone(&widened_schema), values);
                    stream.blocking_push(widened_record, rn).map_err(|e| {
                        PipelineError::Internal {
                            op: "source-stream-push",
                            node: src_cfg.name.clone(),
                            detail: e.to_string(),
                        }
                    })?;
                }
                Ok(None) => break,
                Err(other) => return Err(other.into()),
            }
        }
        // Drop the sender so the dispatch-side `recv().await` returns
        // `None` once the channel drains.
        drop(stream);
        Ok(IngestTaskOutcome {
            source_name: src_cfg.name.clone(),
            total_count,
            watermark_observations,
        })
    });

    join.await.map_err(|e| PipelineError::Internal {
        op: "source-ingest-blocking",
        node: source_name,
        detail: format!("blocking worker join failed: {e}"),
    })?
}

/// Extract `Vec<FieldDef>` from `SourceConfig.schema` for fixed-width format.
///
/// Resolves `SchemaSource::Inline` or `SchemaSource::FilePath` to `Vec<FieldDef>`.
/// Returns a config validation error if schema is `None` (fixed-width requires
/// explicit schema with field definitions).
fn extract_field_defs(
    input: &crate::config::SourceConfig,
) -> Result<Vec<clinker_record::schema_def::FieldDef>, PipelineError> {
    let schema_source = input.schema.as_ref().ok_or_else(|| {
        PipelineError::Config(crate::config::ConfigError::Validation(
            "fixed-width format requires explicit schema with field definitions".into(),
        ))
    })?;
    let def = match schema_source {
        crate::config::SchemaSource::Inline(def) => def.clone(),
        crate::config::SchemaSource::FilePath(path) => {
            crate::schema::load_schema(std::path::Path::new(path)).map_err(|e| {
                PipelineError::Config(crate::config::ConfigError::Validation(format!(
                    "failed to load schema from '{path}': {e}",
                )))
            })?
        }
    };
    def.fields.ok_or_else(|| {
        PipelineError::Config(crate::config::ConfigError::Validation(
            "fixed-width schema must have 'fields' defined".into(),
        ))
    })
}

/// Build CSV reader config from optional CSV input options.
fn build_csv_reader_config(opts: Option<&crate::config::CsvInputOptions>) -> CsvReaderConfig {
    let mut config = CsvReaderConfig::default();
    if let Some(opts) = opts {
        if let Some(ref d) = opts.delimiter
            && let Some(b) = d.as_bytes().first()
        {
            config.delimiter = *b;
        }
        if let Some(ref q) = opts.quote_char
            && let Some(b) = q.as_bytes().first()
        {
            config.quote_char = *b;
        }
        if let Some(h) = opts.has_header {
            config.has_header = h;
        }
    }
    config
}

/// Build JSON reader config from optional JSON input options and array paths.
fn build_json_reader_config(
    opts: Option<&crate::config::JsonInputOptions>,
    array_paths: Option<&[crate::config::ArrayPathConfig]>,
) -> JsonReaderConfig {
    let mut config = JsonReaderConfig::default();
    if let Some(opts) = opts {
        config.format = opts.format.as_ref().map(|f| match f {
            crate::config::JsonFormat::Array => JsonMode::Array,
            crate::config::JsonFormat::Ndjson => JsonMode::Ndjson,
            crate::config::JsonFormat::Object => JsonMode::Object,
        });
        config.record_path = opts.record_path.clone();
    }
    if let Some(paths) = array_paths {
        config.array_paths = paths
            .iter()
            .map(|p| ArrayPathSpec {
                path: p.path.clone(),
                mode: match p.mode {
                    crate::config::ArrayMode::Explode => ArrayPathMode::Explode,
                    crate::config::ArrayMode::Join => ArrayPathMode::Join,
                },
                separator: p.separator.clone().unwrap_or_else(|| ",".to_string()),
            })
            .collect();
    }
    config
}

/// Build XML reader config from optional XML input options and array paths.
fn build_xml_reader_config(
    opts: Option<&crate::config::XmlInputOptions>,
    array_paths: Option<&[crate::config::ArrayPathConfig]>,
) -> XmlReaderConfig {
    let mut config = XmlReaderConfig::default();
    if let Some(opts) = opts {
        config.record_path = opts.record_path.clone();
        if let Some(ref prefix) = opts.attribute_prefix {
            config.attribute_prefix = prefix.clone();
        }
        config.namespace_handling = match opts.namespace_handling {
            Some(crate::config::NamespaceHandling::Qualify) => NamespaceMode::Qualify,
            _ => NamespaceMode::Strip,
        };
    }
    if let Some(paths) = array_paths {
        config.array_paths = paths
            .iter()
            .map(|p| XmlArrayPath {
                path: p.path.clone(),
                mode: match p.mode {
                    crate::config::ArrayMode::Explode => XmlArrayMode::Explode,
                    crate::config::ArrayMode::Join => XmlArrayMode::Join,
                },
                separator: p.separator.clone().unwrap_or_else(|| ",".to_string()),
            })
            .collect();
    }
    config
}

/// Build fixed-width reader config from optional fixed-width input options.
fn build_fw_reader_config(
    opts: Option<&crate::config::FixedWidthInputOptions>,
) -> FixedWidthReaderConfig {
    let mut config = FixedWidthReaderConfig::default();
    if let Some(opts) = opts
        && let Some(ref sep) = opts.line_separator
    {
        config.line_separator = sep.clone();
    }
    config
}

/// Build a CsvWriterConfig from CSV output options and the top-level include_header flag.
fn build_csv_writer_config(
    opts: Option<&crate::config::CsvOutputOptions>,
    include_header: Option<bool>,
) -> CsvWriterConfig {
    let mut config = CsvWriterConfig::default();
    if let Some(h) = include_header {
        config.include_header = h;
    }
    if let Some(opts) = opts
        && let Some(ref d) = opts.delimiter
        && let Some(b) = d.as_bytes().first()
    {
        config.delimiter = *b;
    }
    config
}

/// Build a JsonWriterConfig from JSON output options.
fn build_json_writer_config(opts: Option<&crate::config::JsonOutputOptions>) -> JsonWriterConfig {
    let mut config = JsonWriterConfig::default();
    if let Some(opts) = opts {
        if let Some(ref fmt) = opts.format {
            config.format = match fmt {
                crate::config::JsonOutputFormat::Array => JsonOutputMode::Array,
                crate::config::JsonOutputFormat::Ndjson => JsonOutputMode::Ndjson,
            };
        }
        if let Some(pretty) = opts.pretty {
            config.pretty = pretty;
        }
    }
    config
}

/// Build an XmlWriterConfig from XML output options.
fn build_xml_writer_config(opts: Option<&crate::config::XmlOutputOptions>) -> XmlWriterConfig {
    let mut config = XmlWriterConfig::default();
    if let Some(opts) = opts {
        if let Some(ref root) = opts.root_element {
            config.root_element = root.clone();
        }
        if let Some(ref rec) = opts.record_element {
            config.record_element = rec.clone();
        }
    }
    config
}

fn build_fw_writer_config(
    opts: Option<&crate::config::FixedWidthOutputOptions>,
) -> FixedWidthWriterConfig {
    let mut config = FixedWidthWriterConfig::default();
    if let Some(opts) = opts
        && let Some(ref sep) = opts.line_separator
    {
        config.line_separator = sep.clone();
    }
    config
}

/// Extract `Vec<FieldDef>` from an output config's schema for fixed-width format.
///
/// Fixed-width output requires explicit schema with field definitions specifying
/// field names, widths, and optionally start positions, justification, and padding.
fn extract_output_field_defs(
    output: &OutputConfig,
) -> Result<Vec<clinker_record::schema_def::FieldDef>, PipelineError> {
    let schema_source = output.schema.as_ref().ok_or_else(|| {
        PipelineError::Config(crate::config::ConfigError::Validation(
            "fixed-width output format requires explicit schema with field definitions".into(),
        ))
    })?;
    let def = match schema_source {
        crate::config::SchemaSource::Inline(def) => def.clone(),
        crate::config::SchemaSource::FilePath(path) => {
            crate::schema::load_schema(std::path::Path::new(path)).map_err(|e| {
                PipelineError::Config(crate::config::ConfigError::Validation(format!(
                    "failed to load output schema from '{path}': {e}",
                )))
            })?
        }
    };
    def.fields.ok_or_else(|| {
        PipelineError::Config(crate::config::ConfigError::Validation(
            "fixed-width output schema must have 'fields' defined".into(),
        ))
    })
}

/// Build a writer factory closure for the given output format.
///
/// The returned `WriterFactory` creates format writers wrapping a `CountingWriter`.
/// For CSV with `repeat_header`, the first call creates a `HeaderCapturingCsvWriter`
/// and subsequent calls replay the captured header via `write_preset_header`.
/// For fixed-width, the factory captures pre-resolved `FieldDef`s from the output schema.
fn build_writer_factory(
    format: &crate::config::OutputFormat,
    include_header: Option<bool>,
    repeat_header: bool,
    field_defs: Option<Vec<clinker_record::schema_def::FieldDef>>,
    include_engine_stamped: bool,
) -> WriterFactory {
    match format {
        crate::config::OutputFormat::Csv(opts) => {
            let mut csv_config = build_csv_writer_config(opts.as_ref(), include_header);
            csv_config.include_engine_stamped = include_engine_stamped;
            if repeat_header {
                let shared_header: Arc<Mutex<Option<Vec<Box<str>>>>> = Arc::new(Mutex::new(None));
                let call_count = Arc::new(AtomicU32::new(0));
                Box::new(move |counting_writer, schema| {
                    let seq = call_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    let inner_csv =
                        CsvWriter::new(counting_writer, schema.clone(), csv_config.clone());
                    if seq == 0 {
                        // First file: capture header from first record
                        Ok(Box::new(HeaderCapturingCsvWriter::new(
                            inner_csv,
                            schema,
                            Arc::clone(&shared_header),
                        )))
                    } else {
                        // Subsequent files: replay captured header
                        let mut csv = inner_csv;
                        if let Some(header) = shared_header.lock().unwrap().as_ref() {
                            csv.write_preset_header(header)?;
                        }
                        Ok(Box::new(csv))
                    }
                })
            } else {
                Box::new(move |counting_writer, schema| {
                    Ok(Box::new(CsvWriter::new(
                        counting_writer,
                        schema,
                        csv_config.clone(),
                    )))
                })
            }
        }
        crate::config::OutputFormat::Json(opts) => {
            let mut json_config = build_json_writer_config(opts.as_ref());
            json_config.include_engine_stamped = include_engine_stamped;
            Box::new(move |counting_writer, schema| {
                Ok(Box::new(JsonWriter::new(
                    counting_writer,
                    schema,
                    json_config.clone(),
                )))
            })
        }
        crate::config::OutputFormat::Xml(opts) => {
            let mut xml_config = build_xml_writer_config(opts.as_ref());
            xml_config.include_engine_stamped = include_engine_stamped;
            Box::new(move |counting_writer, schema| {
                Ok(Box::new(XmlWriter::new(
                    counting_writer,
                    schema,
                    xml_config.clone(),
                )))
            })
        }
        crate::config::OutputFormat::FixedWidth(opts) => {
            let fw_config = build_fw_writer_config(opts.as_ref());
            let fields = field_defs.expect(
                "fixed-width writer factory requires field_defs — \
                 build_format_writer must validate schema before calling",
            );
            Box::new(move |counting_writer, _schema| {
                Ok(Box::new(FixedWidthWriter::new(
                    counting_writer,
                    fields.clone(),
                    fw_config.clone(),
                )?))
            })
        }
    }
}

/// Build a format writer for an output config, handling both split and non-split paths.
///
/// For split outputs: creates a `SplittingWriter` with a file factory and writer factory.
/// For non-split outputs: creates a single writer wrapped in `CountedFormatWriter`.
pub(crate) fn build_format_writer(
    output: &OutputConfig,
    raw_writer: Box<dyn Write + Send>,
    schema: Arc<Schema>,
) -> Result<Box<dyn FormatWriter>, PipelineError> {
    // Extract field definitions for fixed-width output (requires explicit schema).
    let field_defs = if matches!(output.format, crate::config::OutputFormat::FixedWidth(_)) {
        Some(extract_output_field_defs(output)?)
    } else {
        None
    };

    let repeat_header = output.split.as_ref().is_some_and(|s| s.repeat_header);
    let writer_factory = build_writer_factory(
        &output.format,
        output.include_header,
        repeat_header,
        field_defs,
        output.include_correlation_keys,
    );

    if let Some(ref split) = output.split {
        let policy = build_split_policy(split);
        let output_path = output.path.clone();
        let naming = split.naming.clone();
        let if_exists = output.if_exists;
        let unique_suffix_width = output.unique_suffix_width;

        let file_factory: clinker_format::splitting::FileFactory =
            Box::new(move |seq: u32| -> std::io::Result<Box<dyn Write + Send>> {
                let bare = std::path::PathBuf::from(apply_split_naming(&output_path, &naming, seq));
                let path_for_n =
                    |n: Option<u64>| -> Result<std::path::PathBuf, crate::config::ConfigError> {
                        Ok(match n {
                            None => bare.clone(),
                            Some(k) => {
                                let suffix = if unique_suffix_width == 0 {
                                    format!("-{k}")
                                } else {
                                    format!("-{:0>width$}", k, width = unique_suffix_width as usize)
                                };
                                crate::output::open::append_suffix_before_ext(&bare, &suffix)
                            }
                        })
                    };
                let (_path, file) = crate::output::open::open_output(if_exists, false, path_for_n)
                    .map_err(|e| std::io::Error::other(format!("{e:?}")))?;
                Ok(Box::new(BufWriter::with_capacity(65536, file)))
            });

        // SplittingWriter creates its own files; don't use raw_writer.
        drop(raw_writer);

        Ok(Box::new(SplittingWriter::new(
            file_factory,
            writer_factory,
            schema,
            policy,
        )))
    } else {
        let buf_writer = BufWriter::with_capacity(65536, raw_writer);
        let counter = SharedByteCounter::new();
        let counting_writer = CountingWriter::new(
            Box::new(buf_writer) as Box<dyn Write + Send>,
            counter.clone(),
        );
        let inner = writer_factory(counting_writer, schema).map_err(PipelineError::Format)?;
        Ok(Box::new(CountedFormatWriter::new(inner, counter)))
    }
}

/// Convert serde `SplitConfig` to runtime `SplitPolicy`.
fn build_split_policy(split: &crate::config::SplitConfig) -> SplitPolicy {
    SplitPolicy {
        max_records: split.max_records,
        max_bytes: split.max_bytes,
        group_key: split.group_key.clone(),
        repeat_header: split.repeat_header,
        oversize_group: match split.oversize_group {
            crate::config::SplitOversizeGroupPolicy::Warn => OversizeGroupPolicy::Warn,
            crate::config::SplitOversizeGroupPolicy::Error => OversizeGroupPolicy::Error,
            crate::config::SplitOversizeGroupPolicy::Allow => OversizeGroupPolicy::Allow,
        },
    }
}

/// Apply `{stem}_{seq:04}.{ext}` naming pattern to an output path.
fn apply_split_naming(base_path: &str, naming: &str, seq: u32) -> String {
    let path = std::path::Path::new(base_path);
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("dat");
    let parent = path.parent().unwrap_or(std::path::Path::new(""));

    let filename = naming
        .replace("{stem}", stem)
        .replace("{ext}", ext)
        .replace("{seq:04}", &format!("{seq:04}"));

    parent.join(filename).to_string_lossy().into_owned()
}

/// Reports whether `idx` has exactly one outgoing edge in `plan`. The
/// streaming-fusion classifiers reject fan-out at the upstream side —
/// a Source feeding two consumers cannot move its live receiver into
/// one of them, and a Merge whose merged stream is read by siblings
/// must stay on the buffered path so those siblings see the records
/// via `node_buffers`.
fn has_single_outgoing(
    plan: &crate::plan::execution::ExecutionPlanDag,
    idx: petgraph::graph::NodeIndex,
) -> bool {
    plan.graph
        .neighbors_directed(idx, petgraph::Direction::Outgoing)
        .count()
        == 1
}

/// Build the pipeline-stable evaluation context.
///
/// Called ONCE per pipeline run at the top of `execute_dag_branching`. The
/// returned `StableEvalContext` is reused (via borrow) at every per-record
/// dispatch site, killing the prior `String::clone` + `IndexMap::clone`
/// Compute the init-phase ancestor closure for a compiled plan.
///
/// Walks every `PlanNode::Transform` whose payload phase is
/// [`crate::config::Phase::Init`] and unions their transitive
/// upstream ancestors via reverse-BFS along the graph's incoming
/// edges. The init-phase transforms themselves are included.
///
/// Returns an empty set when no init-phase transforms exist — the
/// caller then falls through to a single-pass topo walk.
///
/// E164 validation guarantees init-phase Transforms are terminal
/// (no runtime descendants), so the closure forms a well-bounded
/// init sub-DAG that Pass 1 can run to completion before Pass 2
/// starts.
/// Identify Source-node names whose receivers are claimed by a
/// downstream `Merge.mode: interleave` whose every direct predecessor
/// is a `PlanNode::Source`. The Merge arm consumes those receivers
/// via `tokio::select!` so a slow Source no longer blocks peer
/// Sources' channels from filling. Per-Source dispatch arms whose
/// name is in this set return cleanly without touching
/// `ctx.source_records`.
///
/// Only triggers when ALL predecessors are Sources. Mixed
/// predecessors (Source + Transform + ...) keep today's per-arm
/// drain path — the Merge arm reads from `node_buffers` for those.
/// Concat-mode Merges keep the per-Source drain too: concat drains
/// predecessors in declaration order, which the per-arm path
/// already delivers.
pub(crate) fn compute_merge_interleave_fused_sources(
    plan: &crate::plan::execution::ExecutionPlanDag,
    config: &PipelineConfig,
) -> HashSet<String> {
    use crate::plan::execution::PlanNode;
    let mut fused: HashSet<String> = HashSet::new();
    for node_idx in plan.graph.node_indices() {
        let PlanNode::Merge {
            name: merge_name, ..
        } = &plan.graph[node_idx]
        else {
            continue;
        };
        let predecessors: Vec<_> = plan
            .graph
            .neighbors_directed(node_idx, petgraph::Direction::Incoming)
            .collect();
        if predecessors.is_empty() {
            continue;
        }
        let all_sources = predecessors
            .iter()
            .all(|p| matches!(&plan.graph[*p], PlanNode::Source { .. }));
        if !all_sources {
            continue;
        }
        // Look up the Merge's mode + seed in the config side-table
        // by name. Missing entry (defaulted MergeBody) is Concat.
        // Seeded interleaves (`interleave_seed: Some(_)`) take the
        // non-fused path so determinism survives — the fastrand
        // schedule over a pre-buffered Vec yields the same sequence
        // across runs, whereas a live-channel select! depends on
        // tokio scheduling. Unseeded interleaves get fusion +
        // back-pressure.
        let (mode, seed) = config
            .nodes
            .iter()
            .find_map(|spanned| match &spanned.value {
                crate::config::PipelineNode::Merge { header, config }
                    if header.name == *merge_name =>
                {
                    Some((config.mode, config.interleave_seed))
                }
                _ => None,
            })
            .unwrap_or((crate::config::MergeMode::Concat, None));
        if !matches!(mode, crate::config::MergeMode::Interleave) || seed.is_some() {
            continue;
        }
        for pred in predecessors {
            if let PlanNode::Source { name, .. } = &plan.graph[pred] {
                fused.insert(name.clone());
            }
        }
    }
    fused
}

/// Identify `PlanNode::Transform` nodes whose sole upstream is a
/// `PlanNode::Source` with a parked receiver in `ctx.source_records`,
/// and whose evaluation can stream directly off that receiver instead
/// of a pre-drained Vec. Returns the set of fused Transform
/// `NodeIndex`es plus the set of Source names whose receivers move
/// into the fused Transform's hands (those names extend
/// [`compute_merge_interleave_fused_sources`]'s output so the Source
/// dispatch arm short-circuits via the same membership check).
///
/// Eligibility (all must hold):
///
/// - Exactly one incoming edge.
/// - That predecessor is a `PlanNode::Source`.
/// - The upstream Source has exactly one outgoing edge (no Merge or
///   sibling fan-out — fanning would require cloning the live channel).
/// - `window_index` is `None`. Windowed Transforms drive
///   `evaluate_single_transform_windowed`, which indexes records into
///   the upstream operator's arena via the row's enumerate position;
///   that arena is built upstream and must already be materialized.
/// - The Transform is not in the init-phase ancestor closure.
/// - The upstream Source's name is not already claimed by
///   `merge_fused`. Merge.interleave fusion wins because the Merge
///   needs every predecessor's receiver concurrently.
///
/// See https://github.com/rustpunk/clinker/issues/74 for the rationale
/// (extending #67's pattern from Merge to Transform unblocks
/// `[slow Source → Transform → out]` topologies from sitting idle on
/// the upstream Source's drain).
pub(crate) fn compute_transform_fused_sources(
    plan: &crate::plan::execution::ExecutionPlanDag,
    merge_fused: &HashSet<String>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
) -> (HashSet<String>, HashSet<petgraph::graph::NodeIndex>) {
    use crate::plan::execution::PlanNode;
    let mut extra_fused_sources: HashSet<String> = HashSet::new();
    let mut fused_transforms: HashSet<petgraph::graph::NodeIndex> = HashSet::new();
    for node_idx in plan.graph.node_indices() {
        let PlanNode::Transform { window_index, .. } = &plan.graph[node_idx] else {
            continue;
        };
        if window_index.is_some() {
            continue;
        }
        if init_phase_set.contains(&node_idx) {
            continue;
        }
        let mut incoming = plan
            .graph
            .neighbors_directed(node_idx, petgraph::Direction::Incoming);
        let Some(pred_idx) = incoming.next() else {
            continue;
        };
        if incoming.next().is_some() {
            continue;
        }
        let PlanNode::Source {
            name: source_name, ..
        } = &plan.graph[pred_idx]
        else {
            continue;
        };
        if merge_fused.contains(source_name) {
            continue;
        }
        if !has_single_outgoing(plan, pred_idx) {
            continue;
        }
        extra_fused_sources.insert(source_name.clone());
        fused_transforms.insert(node_idx);
    }
    (extra_fused_sources, fused_transforms)
}

/// Identify fused `Merge.interleave` → single `Output` chains eligible for
/// per-record streaming writes (issue #72). The buffered Output arm waits
/// until the Merge arm has produced every record before invoking the
/// writer; under a slow upstream Source this defeats the live back-
/// pressure #67 delivered, because every record sits in
/// `ctx.node_buffers[merge_idx]` until the Merge finishes.
///
/// The streaming path moves the Output's writer into a `tokio::spawn`-ed
/// task that consumes from a bounded `mpsc::channel` populated by the
/// fused Merge arm. The Output arm becomes a no-op at its topo turn;
/// per-record `Writer::write_record` fires concurrently with Merge
/// production, and back-pressure flows writer → Merge → Source.
///
/// Eligibility (every predicate must hold; otherwise the buffered path
/// runs):
///
/// - The Output node has exactly one incoming edge, and that predecessor
///   is a `PlanNode::Merge`.
/// - The Merge is `mode: interleave` with every direct predecessor a
///   `PlanNode::Source` (the [`compute_merge_interleave_fused_sources`]
///   predicate — same membership the fused arm relies on).
/// - The Merge has no other downstream consumer besides this one Output
///   (no fan-out from the Merge).
/// - The Output is not in the init-phase ancestor closure.
/// - The Output's writer entry lives in `writers.single` (not
///   `fan_out_writers` — splitting / per-source-file writers handle
///   their own buffering).
/// - The OutputConfig has no `split:` block — `SplittingWriter` owns its
///   file rotation lifecycle and is incompatible with the streaming
///   writer task.
/// - Correlation buffering is inactive pipeline-wide
///   (`!any_source_has_correlation_key`). The correlation path defers
///   writes to [`PlanNode::CorrelationCommit`] which is incompatible
///   with per-record write at Merge-arm time.
///
/// Returns the list of streaming specs (one per qualifying Output). Empty
/// for pipelines that don't match the topology, leaving every Output on
/// the existing buffered path. See
/// https://github.com/rustpunk/clinker/issues/72 for the rationale.
fn compute_streaming_output_specs(
    plan: &crate::plan::execution::ExecutionPlanDag,
    config: &PipelineConfig,
    fused_merge_sources: &HashSet<String>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
    output_configs: &[OutputConfig],
    writers: &WriterRegistry,
) -> Vec<StreamingOutputSpec> {
    use crate::plan::execution::PlanNode;

    // Pipeline-wide correlation buffering disables streaming for every
    // Output — the CorrelationCommit terminal owns the actual writes.
    if config.any_source_has_correlation_key() {
        return Vec::new();
    }

    let mut specs: Vec<StreamingOutputSpec> = Vec::new();
    for output_idx in plan.graph.node_indices() {
        let PlanNode::Output {
            name: output_name, ..
        } = &plan.graph[output_idx]
        else {
            continue;
        };
        if init_phase_set.contains(&output_idx) {
            continue;
        }

        // Exactly one incoming edge, and that predecessor is a Merge.
        let mut incoming = plan
            .graph
            .neighbors_directed(output_idx, petgraph::Direction::Incoming);
        let Some(merge_idx) = incoming.next() else {
            continue;
        };
        if incoming.next().is_some() {
            continue;
        }
        let PlanNode::Merge {
            name: merge_name, ..
        } = &plan.graph[merge_idx]
        else {
            continue;
        };

        // The Merge must be fused — every predecessor is a Source whose
        // name appears in the fused-source set the Merge arm uses to
        // run a live `tokio::select!`.
        let merge_predecessors: Vec<_> = plan
            .graph
            .neighbors_directed(merge_idx, petgraph::Direction::Incoming)
            .collect();
        if merge_predecessors.is_empty() {
            continue;
        }
        let all_fused_sources = merge_predecessors.iter().all(|p| match &plan.graph[*p] {
            PlanNode::Source { name, .. } => fused_merge_sources.contains(name),
            _ => false,
        });
        if !all_fused_sources {
            continue;
        }

        // The Merge has exactly one outgoing edge (this Output). Anything
        // else and the buffered path is needed so siblings still see the
        // merged records via `node_buffers`.
        if !has_single_outgoing(plan, merge_idx) {
            continue;
        }

        // Output writer registry checks: must be a single writer, not a
        // fan-out, and the OutputConfig must not declare a split block.
        if !writers.single.contains_key(output_name) {
            continue;
        }
        if writers.fan_out.contains_key(output_name) {
            continue;
        }
        let Some(out_cfg) = output_configs.iter().find(|o| &o.name == output_name) else {
            continue;
        };
        if out_cfg.split.is_some() {
            continue;
        }

        // Pre-compute the schema-check + projection metadata so the
        // spawned task carries owned `Arc<Schema>` / `Vec<String>` / the
        // upstream node name for E314 diagnostics, with no borrow back
        // into the plan.
        let expected_input_schema = plan.graph[output_idx]
            .expected_input_schema_in(plan)
            .cloned();
        let cxl_emit_names: Vec<String> = plan.graph[output_idx].cxl_emit_names_in(plan);

        specs.push(StreamingOutputSpec {
            merge_idx,
            output_idx,
            output_name: output_name.clone(),
            merge_name: merge_name.clone(),
            out_cfg: out_cfg.clone(),
            expected_input_schema,
            cxl_emit_names,
        });
    }
    specs
}

/// Plan-time metadata for a streaming `Output` task spawned at executor
/// entry. Carries every field the task needs in owned form so it can run
/// independently of the borrowed `&PipelineConfig` / `&ExecutionPlanDag`
/// references that anchor the dispatcher's `ExecutorContext`.
pub(crate) struct StreamingOutputSpec {
    /// `NodeIndex` of the upstream `Merge` node. The Merge arm looks up
    /// its sender in [`dispatch::ExecutorContext::streaming_output_senders`]
    /// keyed by this index.
    pub(crate) merge_idx: petgraph::graph::NodeIndex,
    /// `NodeIndex` of the downstream `Output` node. The Output arm
    /// short-circuits when its index appears in
    /// [`dispatch::ExecutorContext::streaming_output_nodes`].
    pub(crate) output_idx: petgraph::graph::NodeIndex,
    pub(crate) output_name: String,
    pub(crate) merge_name: String,
    pub(crate) out_cfg: OutputConfig,
    /// Compile-time input schema for the Output; used by the streaming
    /// task to run the same `check_input_schema` invariant the buffered
    /// path enforces (E314 SchemaMismatch diagnostics).
    pub(crate) expected_input_schema: Option<Arc<Schema>>,
    /// Upstream `cxl_emit_names_in` result — passed to
    /// `project_output_from_record` so the streaming projection drops
    /// passthroughs the user didn't explicitly emit (matching the
    /// buffered path's `include_unmapped: false` semantic).
    pub(crate) cxl_emit_names: Vec<String>,
}

/// Per-task return shape merged into the dispatcher's
/// [`dispatch::ExecutorContext`] after `JoinHandle::await`. Mirrors the
/// counter / timer / error accounting the buffered Output arm performs
/// inline; the streaming task accumulates these locally and the
/// dispatcher folds them back into `ctx.counters`, `ctx.records_emitted`,
/// `ctx.write_timer`, `ctx.projection_timer`, `ctx.ok_source_rows`, and
/// `ctx.output_errors`.
pub(crate) struct StreamingOutputTaskOutput {
    pub(crate) records_written: u64,
    pub(crate) records_emitted: u64,
    pub(crate) seen_row_nums: HashSet<u64>,
    pub(crate) write_timer: stage_metrics::CumulativeTimer,
    pub(crate) projection_timer: stage_metrics::CumulativeTimer,
    pub(crate) errors: Vec<PipelineError>,
    pub(crate) stage_metrics: Vec<stage_metrics::StageMetrics>,
}

impl StreamingOutputTaskOutput {
    /// Fold the task-local counters / timers / errors / stage metrics
    /// back into the dispatcher's [`dispatch::ExecutorContext`] after
    /// `JoinHandle::await`. Mirrors the buffered Output arm's inline
    /// counter accounting: `ok_count` counts distinct source rows
    /// (deduplicated against `ctx.ok_source_rows`), `records_written`
    /// counts every write, and the per-task timers accumulate via
    /// [`stage_metrics::CumulativeTimer::add`].
    fn fold_into(self, ctx: &mut dispatch::ExecutorContext<'_>) {
        let mut newly_ok: u64 = 0;
        for rn in self.seen_row_nums {
            if ctx.ok_source_rows.insert(rn) {
                newly_ok += 1;
            }
        }
        ctx.counters.ok_count += newly_ok;
        ctx.counters.records_written += self.records_written;
        ctx.records_emitted += self.records_emitted;
        ctx.write_timer.add(self.write_timer);
        ctx.projection_timer.add(self.projection_timer);
        ctx.output_errors.extend(self.errors);
        for sm in self.stage_metrics {
            ctx.collector.record(sm);
        }
    }
}

/// Streaming-output writer task body. Drains the `mpsc::Receiver`
/// populated by the fused `Merge.interleave` arm, projects each record
/// through `project_output_from_record`, lazily constructs the writer on
/// the first record, calls `Writer::write_record` per record, and
/// flushes at channel close. Errors are accumulated rather than aborting
/// the loop so the dispatcher can surface them alongside any sibling
/// `output_errors`. See [`StreamingOutputSpec`] for the eligibility
/// predicate and https://github.com/rustpunk/clinker/issues/72 for the
/// rationale.
async fn streaming_output_task(
    mut rx: tokio::sync::mpsc::Receiver<(Record, u64)>,
    raw_writer: Box<dyn Write + Send>,
    spec: StreamingOutputSpec,
) -> StreamingOutputTaskOutput {
    use crate::projection::project_output_from_record;

    let mut out = StreamingOutputTaskOutput {
        records_written: 0,
        records_emitted: 0,
        seen_row_nums: HashSet::new(),
        write_timer: stage_metrics::CumulativeTimer::new(),
        projection_timer: stage_metrics::CumulativeTimer::new(),
        errors: Vec::new(),
        stage_metrics: Vec::new(),
    };
    let cxl_emit_names_opt: Option<&[String]> = if spec.cxl_emit_names.is_empty() {
        None
    } else {
        Some(&spec.cxl_emit_names)
    };

    let mut scan_timer_slot: Option<stage_metrics::StageTimer> = Some(
        stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan),
    );
    let mut writer: Option<Box<dyn FormatWriter>> = None;
    let mut raw_writer_slot: Option<Box<dyn Write + Send>> = Some(raw_writer);

    while let Some((record, rn)) = rx.recv().await {
        if let Some(expected) = spec.expected_input_schema.as_ref()
            && let Err(err) = crate::executor::schema_check::check_input_schema(
                expected,
                record.schema(),
                &spec.output_name,
                "output",
                &spec.merge_name,
            )
        {
            out.errors.push(err);
            continue;
        }

        let projected = {
            let _guard = out.projection_timer.guard();
            project_output_from_record(&record, &spec.out_cfg, cxl_emit_names_opt)
        };

        // Lazy writer construction: defer until we have the first
        // record's projected schema so the writer's column list matches
        // what `project_output_from_record` actually emits (same source
        // of truth as the buffered Output arm at dispatch.rs's
        // `output_schema = Arc::clone(projected.schema())`).
        if writer.is_none() {
            let raw = raw_writer_slot
                .take()
                .expect("raw_writer_slot is Some until first record arrives");
            let schema = Arc::clone(projected.schema());
            match build_format_writer(&spec.out_cfg, raw, schema) {
                Ok(w) => {
                    writer = Some(w);
                    if let Some(timer) = scan_timer_slot.take() {
                        out.stage_metrics.push(timer.finish(1, 1));
                    }
                }
                Err(e) => {
                    out.errors.push(e);
                    if let Some(timer) = scan_timer_slot.take() {
                        out.stage_metrics.push(timer.finish(0, 0));
                    }
                    // Drain the rest of the channel so the Merge arm's
                    // bounded send doesn't deadlock — the streaming
                    // task must keep consuming even when the writer is
                    // dead, otherwise back-pressure stalls the whole
                    // pipeline.
                    while rx.recv().await.is_some() {}
                    return out;
                }
            }
        }

        let write_result = {
            let _guard = out.write_timer.guard();
            writer
                .as_mut()
                .expect("writer is Some after lazy construction")
                .write_record(&projected)
        };
        match write_result {
            Ok(()) => {
                out.records_written += 1;
                out.records_emitted += 1;
                out.seen_row_nums.insert(rn);
            }
            Err(e) => {
                out.errors.push(PipelineError::from(e));
                while rx.recv().await.is_some() {}
                return out;
            }
        }
    }

    // Channel closed — every Merge predecessor's receiver returned None,
    // so no more records will arrive. Flush whatever the writer has
    // buffered. `writer.is_none()` is the empty-stream case (zero
    // records arrived); matches the buffered path's
    // `if unbuffered.is_empty() { return Ok(()); }` short-circuit.
    if let Some(timer) = scan_timer_slot.take() {
        out.stage_metrics.push(timer.finish(0, 0));
    }
    if let Some(mut w) = writer {
        let flush_result = {
            let _guard = out.write_timer.guard();
            w.flush()
        };
        if let Err(e) = flush_result {
            out.errors.push(PipelineError::from(e));
        }
    }
    out
}

pub(crate) fn compute_init_phase_node_set(
    plan: &crate::plan::execution::ExecutionPlanDag,
) -> std::collections::HashSet<petgraph::graph::NodeIndex> {
    use crate::config::Phase as ConfPhase;
    use crate::plan::execution::PlanNode;
    let mut set: std::collections::HashSet<petgraph::graph::NodeIndex> =
        std::collections::HashSet::new();
    let mut stack: Vec<petgraph::graph::NodeIndex> = plan
        .graph
        .node_indices()
        .filter(|&idx| match &plan.graph[idx] {
            PlanNode::Transform {
                resolved: Some(p), ..
            } => p.phase == ConfPhase::Init,
            _ => false,
        })
        .collect();
    while let Some(idx) = stack.pop() {
        if !set.insert(idx) {
            continue;
        }
        for parent in plan
            .graph
            .neighbors_directed(idx, petgraph::Direction::Incoming)
        {
            stack.push(parent);
        }
    }
    set
}

/// per-record allocation profile. `pipeline_start_time` must be frozen at
/// pipeline start so `$pipeline.start_time` is deterministic within a run.
/// The `now` keyword uses `ctx.stable.clock.now()` (wall-clock) and is
/// intentionally non-deterministic.
fn build_stable_eval_context(
    config: &PipelineConfig,
    pipeline_start_time: chrono::NaiveDateTime,
    execution_id: &str,
    batch_id: &str,
    pipeline_vars: &IndexMap<String, Value>,
    static_vars_overrides: &IndexMap<String, Value>,
) -> StableEvalContext {
    // Seed pipeline-scope vars with declared defaults from every
    // Transform's `declares:` entries; runtime injections (channel
    // overrides, test seeds) overlay on top.
    let mut seeded = crate::config::collect_pipeline_var_defaults(&config.nodes);
    for (k, v) in pipeline_vars {
        seeded.insert(k.clone(), v.clone());
    }
    StableEvalContext {
        clock: Box::new(WallClock),
        pipeline_start_time,
        pipeline_name: Arc::from(config.pipeline.name.as_str()),
        pipeline_execution_id: Arc::from(execution_id),
        pipeline_batch_id: Arc::from(batch_id),
        pipeline_counters: PipelineCounters::default(),
        pipeline_vars: Arc::new(std::sync::RwLock::new(seeded)),
        source_vars: Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())),
        source_input_arcs: Arc::new(compute_source_input_arcs(config)),
        static_vars: Arc::new(build_static_vars(config, static_vars_overrides)),
    }
}

/// Build the `$vars.*` runtime value map from the pipeline's `vars:`
/// block, with `overrides` (channel-supplied) layered on top. Channel
/// adds extend the map; channel overrides replace.
fn build_static_vars(
    config: &PipelineConfig,
    overrides: &IndexMap<String, Value>,
) -> IndexMap<String, Value> {
    let mut out = config
        .pipeline
        .vars
        .as_ref()
        .map(crate::config::convert_vars)
        .unwrap_or_default();
    for (k, v) in overrides {
        out.insert(k.clone(), v.clone());
    }
    out
}

/// Walk the YAML config to map every `Merge` / `Combine` named input to
/// the source-file `Arc<str>`s of the upstream `Source` node(s) it
/// transitively reads from. Used by `Expr::QualifiedSourceAccess` eval
/// to look up `source_vars` entries written by upstream source-scope
/// Transform writers (qualified post-merge `$source.<input>.<key>` reads).
///
/// For Merge, each entry in `inputs:` becomes its own input name (the
/// referenced node name itself, since Merge does not rename). For
/// Combine, the IndexMap key is the input name. Walk-back follows the
/// consumer-side `input:` field through Transform/Aggregate/Route nodes
/// until a Source is reached; nested Merges fan out, collecting every
/// reachable Source's path Arc.
fn compute_source_input_arcs(
    config: &PipelineConfig,
) -> std::collections::HashMap<String, Vec<Arc<str>>> {
    use crate::config::pipeline_node::PipelineNode;
    let mut by_name: HashMap<&str, &PipelineNode> = HashMap::new();
    for node in &config.nodes {
        by_name.insert(node.value.name(), &node.value);
    }
    let mut out: std::collections::HashMap<String, Vec<Arc<str>>> =
        std::collections::HashMap::new();
    for node in &config.nodes {
        match &node.value {
            PipelineNode::Combine { header, .. } => {
                for (input_name, upstream) in header.input.iter() {
                    let arcs = arcs_reachable_from(&by_name, upstream.value.name());
                    if !arcs.is_empty() {
                        out.entry(input_name.clone()).or_default().extend(arcs);
                    }
                }
            }
            PipelineNode::Merge { header, .. } => {
                for upstream in &header.inputs {
                    let upstream_name = upstream.value.name();
                    let arcs = arcs_reachable_from(&by_name, upstream_name);
                    if !arcs.is_empty() {
                        out.entry(upstream_name.to_string())
                            .or_default()
                            .extend(arcs);
                    }
                }
            }
            _ => {}
        }
    }
    for arcs in out.values_mut() {
        arcs.sort();
        arcs.dedup();
    }
    out
}

fn arcs_reachable_from(
    by_name: &HashMap<&str, &crate::config::pipeline_node::PipelineNode>,
    start: &str,
) -> Vec<Arc<str>> {
    use crate::config::pipeline_node::PipelineNode;
    let mut out = Vec::new();
    let mut stack = vec![start.to_string()];
    let mut seen: HashSet<String> = HashSet::new();
    while let Some(name) = stack.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        let Some(node) = by_name.get(name.as_str()) else {
            continue;
        };
        match node {
            PipelineNode::Source { config: body, .. } => {
                let path = body.source.path_str();
                if !path.is_empty() {
                    out.push(Arc::from(path));
                }
            }
            PipelineNode::Merge { header, .. } => {
                for upstream in &header.inputs {
                    stack.push(upstream.value.name().to_string());
                }
            }
            PipelineNode::Combine { header, .. } => {
                for upstream in header.input.values() {
                    stack.push(upstream.value.name().to_string());
                }
            }
            other => {
                if let Some(input) = other.input_node_name() {
                    stack.push(input.to_string());
                }
            }
        }
    }
    out
}

/// Evaluate a single transform against a record, returning the
/// modified record with emitted fields and metadata merged in.
///
/// Used by the DAG-walking executor for per-node evaluation. The
/// record schema is widened in-place (rebuilt onto a schema that
/// includes every emitted field) so `Record::set` always hits a
/// known slot. Downstream positional-index consumers (aggregation
/// `group_by_indices`) rely on the upstream layout, so the widened
/// schema preserves every upstream column at its original index.
///
/// Single emitted (or skipped) output of a per-record transform
/// evaluation. The `Result<(), SkipReason>` distinguishes a real emit
/// (`Ok(())`) from a `filter`/`distinct` skip; the leading `Record` is
/// the output record (or the original record on Skip). One entry per
/// emitted record under `emit each` fan-out.
pub(crate) type TransformOutput = (Record, Result<(), SkipReason>);

/// Error shape returned by [`evaluate_single_transform`] and
/// [`evaluate_single_transform_windowed`] — pairs the transform name
/// with the underlying [`cxl::eval::EvalError`] for DLQ classification.
pub(crate) type TransformEvalError = (String, cxl::eval::EvalError);

/// Returns a `Vec` of `(record, Ok | Skip)` entries — typically one
/// entry per call, but `emit each` fan-out produces N entries from a
/// single input record. On error, returns `(transform_name, EvalError)`.
#[allow(clippy::result_large_err)]
pub(crate) fn evaluate_single_transform(
    record: &Record,
    transform_name: &str,
    evaluator: &mut ProgramEvaluator,
    ctx: &EvalContext,
    _output_schema: &Arc<Schema>,
) -> Result<Vec<TransformOutput>, TransformEvalError> {
    let input = record;
    let result = evaluator
        .eval_record::<NullStorage>(ctx, input, None)
        .map_err(|e| (transform_name.to_string(), e))?;
    Ok(materialize_eval_result(input, result))
}

/// Convert an [`EvalResult`] into the per-record dispatch shape used
/// by the executor: one `(Record, Ok)` per emitted body record, or a
/// single `(Record, Skip)` entry on Filtered/Duplicate. The fan-out
/// case applies emitted fields and record-var writes on a fresh
/// `record_with_emitted_fields` projection of `input` once per
/// `EmitOne`; downstream cursor advancement happens once per emitted
/// record at the call site.
fn materialize_eval_result(input: &Record, result: EvalResult) -> Vec<TransformOutput> {
    match result {
        EvalResult::Emit {
            fields: emitted,
            record_vars,
            ..
        } => {
            let mut out = record_with_emitted_fields(input, &emitted);
            for (name, value) in &emitted {
                out.set(name, value.clone());
            }
            for (key, value) in *record_vars {
                let _ = out.set_record_var(&key, value);
            }
            vec![(out, Ok(()))]
        }
        EvalResult::EmitMany { records } => {
            let mut acc = Vec::with_capacity(records.len());
            for rec in records {
                let cxl::eval::EmitOne {
                    fields: emitted,
                    record_vars,
                } = rec;
                let mut out = record_with_emitted_fields(input, &emitted);
                for (name, value) in &emitted {
                    out.set(name, value.clone());
                }
                for (key, value) in *record_vars {
                    let _ = out.set_record_var(&key, value);
                }
                acc.push((out, Ok(())));
            }
            acc
        }
        EvalResult::Skip(reason) => vec![(input.clone(), Err(reason))],
    }
}

/// Same as [`evaluate_single_transform`] but threads a
/// [`PartitionWindowContext`] derived from the resolved
/// [`crate::executor::window_runtime::WindowRuntime`] so `$window.*`
/// expressions resolve against the transform's analytic window.
/// `window_index` indexes `plan.indices_to_build`.
///
/// The caller resolves `runtime` via
/// `ctx.window_runtime.resolve(&spec.root, window_index)` and is
/// responsible for surfacing `PipelineError::Internal` if `resolve`
/// returns `None` — at this point in the dispatch arm the upstream
/// operator has already finalized its emit, so a missing runtime is
/// always an invariant violation.
///
/// `record_pos` is the record's position in the arena, derived by the
/// caller from the spec's root: `(rn - 1)` for `Source(_)` roots
/// (Phase-0 materializes source records in row-number order), or the
/// per-record enumerate index for `Node{..}` and `ParentNode{..}` roots
/// (the upstream operator's emit order is the arena's order).
///
/// If the record's group-by values do not resolve to a partition (null
/// in any group key, or no matching partition), the evaluator runs
/// without a window context — matching the legacy inline arena path's
/// behavior.
#[allow(clippy::too_many_arguments, clippy::result_large_err)]
pub(crate) fn evaluate_single_transform_windowed(
    record: &Record,
    transform_name: &str,
    evaluator: &mut ProgramEvaluator,
    ctx: &EvalContext,
    plan: &ExecutionPlanDag,
    window_index: usize,
    runtime: &crate::executor::window_runtime::WindowRuntime,
    record_pos: u64,
) -> Result<Vec<TransformOutput>, TransformEvalError> {
    let spec = &plan.indices_to_build[window_index];
    let arena = runtime.arena.as_ref();
    let index = runtime.index.as_ref();

    let key: Option<Vec<GroupByKey>> = spec
        .group_by
        .iter()
        .map(|field| {
            let val = record.get(field).cloned().unwrap_or(Value::Null);
            value_to_group_key(&val, field, None, record_pos)
                .ok()
                .flatten()
        })
        .collect();

    let sort_fields: Vec<&str> = spec.sort_by.iter().map(|s| s.field.as_str()).collect();
    let result = if let Some(key) = key {
        if let Some(partition) = index.get(&key) {
            let pos_in_partition = partition.iter().position(|&p| p == record_pos).unwrap_or(0);
            let wctx =
                PartitionWindowContext::new(arena, partition, pos_in_partition, &sort_fields);
            evaluator
                .eval_record(ctx, record, Some(&wctx))
                .map_err(|e| (transform_name.to_string(), e))?
        } else {
            evaluator
                .eval_record::<Arena>(ctx, record, None)
                .map_err(|e| (transform_name.to_string(), e))?
        }
    } else {
        evaluator
            .eval_record::<Arena>(ctx, record, None)
            .map_err(|e| (transform_name.to_string(), e))?
    };

    Ok(materialize_eval_result(record, result))
}

/// Re-project `input` onto `target`: allocate a fresh `Record` whose
/// schema is `target`, copying over any upstream field that `target`
/// still declares. Used at operator boundaries to canonicalize the
/// `Arc<Schema>` on the Record so downstream `Arc::ptr_eq` checks hit
/// the fast path.
pub(crate) fn widen_record_to_schema(input: &Record, target: &Arc<Schema>) -> Record {
    if Arc::ptr_eq(input.schema(), target) {
        return input.clone();
    }
    let mut values: Vec<clinker_record::Value> = Vec::with_capacity(target.column_count());
    for (i, col) in target.columns().iter().enumerate() {
        let v = match input.get(col.as_ref()) {
            Some(v) => v.clone(),
            None => recover_engine_stamped_value(input, target, i, col.as_ref()),
        };
        values.push(v);
    }
    Record::new(Arc::clone(target), values)
}

/// Recover an engine-stamped column's value when the input does not
/// expose it under the target column name. The N-ary combine
/// decomposition encodes intermediate-step columns as
/// `__<qualifier>__<name>`, so a `$ck.<field>` snapshot column
/// arrives at the final step's body construction under
/// `__<driver>__$ck.<field>` rather than `$ck.<field>` directly.
/// Walk the input schema for any column ending in `__<col>` and
/// recover the value; otherwise default to `Value::Null` (the column
/// remains an unstamped slot, matching the behavior for build sources
/// that never declared the correlation-key field).
fn recover_engine_stamped_value(
    input: &Record,
    target: &Arc<Schema>,
    target_idx: usize,
    target_col: &str,
) -> clinker_record::Value {
    let is_engine_stamped = target
        .field_metadata(target_idx)
        .is_some_and(|m| m.is_engine_stamped());
    if !is_engine_stamped {
        return clinker_record::Value::Null;
    }
    let suffix = format!("__{target_col}");
    if let Some(j) = input
        .schema()
        .columns()
        .iter()
        .position(|n| n.as_ref().ends_with(&suffix))
    {
        return input.values()[j].clone();
    }
    clinker_record::Value::Null
}

/// Copy build-side `$ck.<field>` columns from `build` into `out` per
/// the combine's `propagate_ck` spec. Driver wins on collision: if
/// `out` already carries a non-null value for the column (because
/// `widen_record_to_schema(driver, …)` filled it), the build value is
/// not written. This is the single runtime CK-copy path used by every
/// combine strategy (HashBuildProbe, IEJoin, GraceHash, SortMerge);
/// keeping the policy here means a strategy never has to encode the
/// `$ck` semantics itself.
///
/// `Driver` copies only the engine-managed synthetic CK
/// (`$ck.aggregate.<name>`, stamped
/// [`clinker_record::FieldMetadata::AggregateGroupIndex`]) — the user-
/// declared source CK is suppressed. `All` copies every `$ck.*` column
/// the build record carries. `Named(set)` copies only the listed
/// source-CK fields plus every synthetic-CK column unconditionally.
///
/// Synthetic CK is engine-managed lineage from a relaxed aggregate to
/// its source rows; the user did not declare it, so `propagate_ck`
/// (a knob over user-declared source CK) has no semantic meaning over
/// it. Without this exemption, the detect-phase fan-out from a
/// downstream failure to the aggregator's per-group source-row table
/// would lose its bridge whenever a Combine sat between the relaxed
/// aggregate and the failing node.
pub(crate) fn copy_build_ck_columns(
    out: &mut Record,
    build: &Record,
    spec: &crate::config::pipeline_node::PropagateCkSpec,
) {
    use crate::config::pipeline_node::PropagateCkSpec;
    let build_schema = Arc::clone(build.schema());
    for (idx, col) in build_schema.columns().iter().enumerate() {
        let Some(field_name) = col.strip_prefix("$ck.") else {
            continue;
        };
        let is_synthetic = matches!(
            build_schema.field_metadata(idx),
            Some(clinker_record::FieldMetadata::AggregateGroupIndex { .. }),
        );
        let allowed = is_synthetic
            || match spec {
                PropagateCkSpec::Driver => false,
                PropagateCkSpec::All => true,
                PropagateCkSpec::Named(names) => names.contains(field_name),
            };
        if !allowed {
            continue;
        }
        if out.schema().index(col.as_ref()).is_none() {
            // Output schema didn't widen for this column — the
            // plan-time `combine_output_row` filtered it out
            // already. Skip rather than silently lose data.
            continue;
        }
        // Driver wins on collision: a non-null value at this slot
        // came from the driver via `widen_record_to_schema`. Only
        // fill when the slot is still null. Synthetic-CK names are
        // unique per aggregate (`$ck.aggregate.<name>`), so a driver
        // and build pairing can collide only when both descend from
        // the same relaxed aggregate, in which case the slot is
        // already populated with the correct lineage.
        match out.get(col.as_ref()) {
            Some(clinker_record::Value::Null) | None => {
                let v = &build.values()[idx];
                out.set(col.as_ref(), v.clone());
            }
            Some(_) => {}
        }
    }
}

/// Widen `input`'s schema in place to include every key in `emitted`
/// that is not already declared. Allocates a fresh `Arc<Schema>` only
/// when new names appear; otherwise clones `input`. Used by the legacy
/// linear pipeline path where the emit set is determined at eval time
/// rather than via a plan-time `output_schema`, and by the
/// commit-phase window recompute which materializes new output rows
/// against the same emit shape the dispatcher's window arm produced.
pub(crate) fn record_with_emitted_fields(
    input: &Record,
    emitted: &IndexMap<String, clinker_record::Value>,
) -> Record {
    let mut missing: Vec<&str> = Vec::new();
    for key in emitted.keys() {
        if input.schema().index(key).is_none() {
            missing.push(key.as_str());
        }
    }
    if missing.is_empty() {
        return input.clone();
    }
    let n = input.schema().column_count();
    let mut builder = SchemaBuilder::with_capacity(n + missing.len());
    // Preserve every existing column AND its `FieldMetadata`. Without
    // explicit forwarding the engine-stamp annotation on `$ck.<field>`
    // shadow columns is dropped and downstream readers (the buffer-key
    // extractor at the Output arm, the projection fast path) treat the
    // column as a user-declared one.
    for i in 0..n {
        let name = input
            .schema()
            .column_name(i)
            .expect("column_name within column_count");
        match input.schema().field_metadata(i) {
            Some(meta) => builder = builder.with_field_meta(name, meta.clone()),
            None => builder = builder.with_field(name),
        }
    }
    for name in missing {
        builder = builder.with_field(name);
    }
    let widened = builder.build();
    widen_record_to_schema(input, &widened)
}

/// Parse memory limit from config (default 512MB).
/// Plan-invariant predecessor lookup for nodes that require exactly one
/// upstream input (currently `PlanNode::Aggregation`). Returns
/// `PipelineError::Internal` on misshapen plans — never panics. Mirrors
/// DataFusion's `internal_err!` macro.
pub(crate) fn single_predecessor(
    plan: &ExecutionPlanDag,
    node_idx: petgraph::graph::NodeIndex,
    op: &'static str,
    node_name: &str,
) -> Result<petgraph::graph::NodeIndex, PipelineError> {
    let preds: Vec<_> = plan
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .collect();
    match preds.as_slice() {
        [p] => Ok(*p),
        _ => Err(PipelineError::Internal {
            op,
            node: node_name.to_string(),
            detail: format!("expected exactly 1 predecessor, got {}", preds.len()),
        }),
    }
}

pub(crate) fn parse_memory_limit(config: &PipelineConfig) -> usize {
    config
        .pipeline
        .memory
        .limit
        .as_ref()
        .and_then(|s| {
            let s = s.trim();
            if let Some(num) = s.strip_suffix('G').or_else(|| s.strip_suffix('g')) {
                num.parse::<usize>().ok().map(|n| n * 1024 * 1024 * 1024)
            } else if let Some(num) = s.strip_suffix('M').or_else(|| s.strip_suffix('m')) {
                num.parse::<usize>().ok().map(|n| n * 1024 * 1024)
            } else {
                s.parse::<usize>().ok()
            }
        })
        .unwrap_or(512 * 1024 * 1024) // 512MB default
}

/// Build a `MemoryArbitrator` from the pipeline-level `memory:` block.
/// Resolves `memory.limit` through `parse_memory_limit_bytes` (defaults
/// to 512 MiB when omitted) and chooses the active policy via
/// `BackpressureKnob::build_policy`. Used by both the pipeline-scoped
/// arbitrator and every per-arm budget the dispatch path constructs.
pub(crate) fn build_arbitrator_from_config(
    config: &PipelineConfig,
) -> crate::pipeline::memory::MemoryArbitrator {
    let mem = &config.pipeline.memory;
    let limit = crate::pipeline::memory::parse_memory_limit_bytes(mem.limit.as_deref());
    crate::pipeline::memory::MemoryArbitrator::with_policy(
        limit,
        0.80,
        mem.backpressure.build_policy(),
    )
}

#[cfg(test)]
mod tests {
    //! Executor-level tests — submodules below each target a specific
    //! executor concern (aggregation dispatch, multi-output routing,
    //! branching/DAG, format dispatch, correlated DLQ).
    //!
    //! Each submodule starts with `use super::*;` and expects the
    //! executor's public-in-crate symbols (`PipelineExecutor`,
    //! `PipelineRunParams`, `DlqEntry`, `CompiledRoute`, `PipelineError`,
    //! etc.) plus a shared `run_test(yaml, csv)` helper (defined here).

    use super::*;

    /// Run a single-source, single-output pipeline with the given YAML
    /// config and CSV input. Returns `(counters, dlq_entries, output_csv)`.
    ///
    /// This mirrors `integration_tests::run_pipeline` but lives inside
    /// the `executor` module so submodules can reference it via
    /// `crate::executor::tests::run_test`.
    pub(super) async fn run_test(
        yaml: &str,
        csv_input: &str,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
        let config = crate::config::parse_config(yaml).unwrap();
        let output_buf = clinker_bench_support::io::SharedBuffer::new();

        let primary = config.source_configs().next().unwrap().name.clone();
        let readers: crate::executor::SourceReaders = HashMap::from([(
            primary.clone(),
            crate::executor::single_file_reader(
                "test.csv",
                Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec())),
            ),
        )]);
        let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
            config.output_configs().next().unwrap().name.clone(),
            Box::new(output_buf.clone()) as Box<dyn Write + Send>,
        )]);

        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars: IndexMap::new(),
            shutdown_token: None,
            ..Default::default()
        };

        let report =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params)
                .await?;

        let output = output_buf.as_string();
        Ok((report.counters, report.dlq_entries, output))
    }

    mod aggregation;
    mod branching;
    mod ck_aligned_partition_runtime;
    mod correlated_dlq;
    mod correlated_dlq_retract;
    mod correlated_post_aggregate_retract;
    mod correlated_window_after_aggregate_retract;
    mod correlated_window_retract;
    mod cross_source_window_topology;
    mod deferred_dispatch;
    mod format_dispatch;
    mod multi_output;
    mod post_aggregate_any_all;
    mod post_aggregate_lag_lead;
    mod post_aggregate_recompute_determinism;
    mod post_aggregate_window;
    mod post_aggregate_window_ranking;
    mod post_aggregate_window_spilled;
    mod post_combine_array_field;
    mod post_combine_synthetic_ck;
    mod post_combine_window_strategies;
    mod strict_pipeline_zero_overhead;
    mod window_recompute_correctness;
}
