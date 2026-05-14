pub mod stage_metrics;

pub mod combine;
pub(crate) mod commit;
pub(crate) mod dispatch;
mod schema_check;
pub(crate) mod source_stream;
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
use crate::pipeline::index::{GroupByKey, SecondaryIndex, value_to_group_key};
use crate::pipeline::memory::rss_bytes;
use crate::pipeline::sort;
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
    pub local_window: Option<serde_json::Value>,
    pub route: Option<crate::config::RouteConfig>,
    pub input: Option<crate::config::TransformInput>,
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
                    local_window: body.analytic_window.clone(),
                    route: None,
                    input: project_input(&header.input.value),
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
                    }),
                    local_window: None,
                    route: None,
                    input: project_input(&header.input.value),
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
                    local_window: None,
                    route: Some(RouteConfig {
                        mode: body.mode,
                        branches,
                        default: body.default.clone(),
                    }),
                    input: project_input(&header.input.value),
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
/// Holds the final pipeline counters, the DLQ vector built across
/// every dispatcher arm, the peak RSS observation, the per-source
/// watermark bookkeeping, and the per-source rollback-cursor map.
pub(crate) type DispatchOutcome = (
    PipelineCounters,
    Vec<DlqEntry>,
    Option<u64>,
    crate::executor::watermark::PerSourceWatermarks,
    BTreeMap<String, u64>,
);

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
    /// restore path. Today's pre-drained executor surfaces these for
    /// diagnostics and per-source-rollback test assertions; once the
    /// async runtime lands, the cursor becomes the replay anchor.
    pub per_source_rollback_cursors: BTreeMap<String, u64>,
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
                        EvalResult::Emit { .. } => return Ok(vec![branch.name.clone()]),
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
                        EvalResult::Emit { .. } => matched.push(branch.name.clone()),
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
                        for stmt in &ct.typed.program.statements {
                            if let Statement::Emit { name, .. } = stmt
                                && !emitted_fields.contains(&name.to_string())
                            {
                                emitted_fields.push(name.to_string());
                            }
                        }
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
                        for stmt in &typed.program.statements {
                            if let Statement::Emit { name, .. } = stmt
                                && !emitted_fields.contains(&name.to_string())
                            {
                                emitted_fields.push(name.to_string());
                            }
                        }
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
                    for stmt in &typed.program.statements {
                        if let Statement::Emit { name: en, .. } = stmt {
                            let s = en.to_string();
                            if !emitted_fields.contains(&s) {
                                emitted_fields.push(s);
                            }
                        }
                    }
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
            source_records.insert(src_cfg.name.clone(), rx);
            let src_cfg_owned = src_cfg.clone();
            let config_clone = config.clone();
            ingest_handles.push(tokio::spawn(async move {
                ingest_source_task(src_cfg_owned, files, config_clone, stream).await
            }));
        }

        let (counters, dlq_entries, peak_rss_bytes, mut watermarks, per_source_rollback_cursors) =
            Self::execute_dag(
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
    /// `source_records` holds every declared Source's pre-ingested records,
    /// each tuple carrying the source-file `Arc<str>` engine-stamped on
    /// the `$source.file` column. The caller's unified ingest pass
    /// populates this map; this function does not read from any
    /// `FormatReader` directly.
    ///
    /// Returns `(counters, dlq_entries, peak_rss_bytes)`.
    #[allow(clippy::too_many_arguments)]
    async fn execute_dag(
        config: &PipelineConfig,
        mut source_records: HashMap<String, tokio::sync::mpsc::Receiver<(Record, u64)>>,
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
        mut watermarks: crate::executor::watermark::PerSourceWatermarks,
    ) -> Result<DispatchOutcome, PipelineError> {
        let mut dlq_entries: Vec<DlqEntry> = Vec::new();

        // Per-source idle-timeout map, derived from each
        // `SourceConfig.watermark.idle_timeout`. Sources without a
        // configured timeout poll receivers without a deadline (today's
        // behavior). The window-close consumer at
        // https://github.com/rustpunk/clinker/issues/61 reads
        // `PerSourceWatermarks::is_idle` populated by the timeout path.
        let idle_timeouts: HashMap<&str, std::time::Duration> = source_configs
            .iter()
            .filter_map(|s| {
                s.watermark
                    .as_ref()
                    .and_then(|w| w.idle_timeout.map(|d| (s.name.as_str(), d)))
            })
            .collect();

        // Pipeline-scoped MemoryBudget. One declared `memory_limit`
        // envelopes the source-rooted Phase-0 arena AND every node-
        // rooted arena finalize. Promoted to ctx-owned so a relaxed
        // pipeline cannot multiply its declared limit across N upstream
        // operators by accident.
        let mut memory_budget = crate::pipeline::memory::MemoryBudget::from_config(
            config.pipeline.memory_limit.as_deref(),
        );

        // Drain every per-source receiver into a local Vec so the
        // arena-build, record-var seed, and `$source.count` pre-passes
        // below can operate on the full per-source record set. After
        // the pre-passes complete we re-channel the records into a
        // fresh `(Sender, Receiver)` pair per source so the dispatch
        // loop's Source arm sees the same `mpsc::Receiver` interface
        // it would see under live streaming. The migration to true
        // live consumption (Source arm reads as ingest tasks push)
        // moves these pre-passes into per-record evaluation in a
        // follow-up commit — for today the architecture lands without
        // disrupting source-rooted window builds or `$source.count`.
        let mut drained_records: HashMap<String, Vec<(Record, u64)>> = HashMap::new();
        for (name, rx) in source_records.iter_mut() {
            let mut buf: Vec<(Record, u64)> = Vec::new();
            // Track the file Arc of the most-recent record so an
            // idle-timeout can flip THAT file's partition to `Idle`.
            // Before any record has arrived the consumer doesn't know
            // which file is producing — use the synthetic
            // [`crate::executor::dispatch::MERGED_SOURCE_FILE`] Arc as
            // the partition key, matching the same convention the
            // engine-stamp path uses for record-less source contexts.
            let mut last_file: Arc<str> =
                Arc::clone(&crate::executor::dispatch::MERGED_SOURCE_FILE);
            let timeout = idle_timeouts.get(name.as_str()).copied();
            loop {
                match timeout {
                    Some(t) => match tokio::time::timeout(t, rx.recv()).await {
                        Ok(Some(item)) => {
                            last_file = crate::executor::dispatch::source_file_arc_of(&item.0);
                            buf.push(item);
                        }
                        Ok(None) => break,
                        Err(_elapsed) => {
                            // Quiet for longer than `idle_timeout` —
                            // flip the partition tracked by `last_file`
                            // to `Idle`. The next record un-idles via
                            // `observe`. Idempotent on repeat timeouts.
                            watermarks.mark_idle(name, &last_file);
                            continue;
                        }
                    },
                    None => match rx.recv().await {
                        Some(item) => {
                            last_file = crate::executor::dispatch::source_file_arc_of(&item.0);
                            buf.push(item);
                        }
                        None => break,
                    },
                }
            }
            drained_records.insert(name.clone(), buf);
        }

        // Emit an observability trace for any source whose consumer
        // loop tripped at least one idle timeout AND ended with every
        // partition still idle. The load-bearing consumer of
        // `is_idle` is the time-window operator at
        // https://github.com/rustpunk/clinker/issues/61; this trace
        // keeps the rollup visible at runtime so operators can see
        // when an idle_timeout is firing.
        for name in idle_timeouts.keys() {
            if watermarks.is_idle(name) {
                tracing::debug!(
                    target: "clinker::watermark",
                    source = name,
                    "source consumer ended with all partitions in WatermarkStatus::Idle"
                );
            }
        }

        // Build per-window arena+index runtimes for every source-rooted
        // IndexSpec — each spec's `root: Source(name)` resolves against
        // the drained record set. Source-rooted arenas are built up-
        // front here because their input is the Source's full ingested
        // record set, available once each source's receiver has
        // returned `None`. Node-rooted (`Node`/`ParentNode`) slots still
        // populate at their upstream operator's dispatch-arm exit
        // through `finalize_node_rooted_windows`.
        let mut window_runtime =
            crate::executor::window_runtime::WindowRuntimeRegistry::new(&plan.indices_to_build);
        let schema_overrides_by_name: HashMap<
            &str,
            HashMap<String, clinker_record::schema_def::FieldDef>,
        > = source_configs
            .iter()
            .map(|s| {
                let overrides = s
                    .schema_overrides
                    .as_ref()
                    .map(|overrides| {
                        overrides
                            .iter()
                            .map(|o| (o.name.clone(), o.clone()))
                            .collect::<HashMap<_, _>>()
                    })
                    .unwrap_or_default();
                (s.name.as_str(), overrides)
            })
            .collect();
        let empty_schema_pins: HashMap<String, clinker_record::schema_def::FieldDef> =
            HashMap::new();
        for (idx, spec) in plan.indices_to_build.iter().enumerate() {
            let crate::plan::index::PlanIndexRoot::Source(source_name) = &spec.root else {
                continue;
            };
            let Some(records) = drained_records.get(source_name.as_str()) else {
                continue;
            };
            if records.is_empty() {
                continue;
            }
            let source_schema = Arc::clone(records[0].0.schema());
            let schema_pins = schema_overrides_by_name
                .get(source_name.as_str())
                .unwrap_or(&empty_schema_pins);

            let arena_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::ArenaBuild);
            let arena = Arena::from_records(
                records,
                &spec.arena_fields,
                &source_schema,
                &mut memory_budget,
            )
            .map_err(|e| PipelineError::Compilation {
                transform_name: String::new(),
                messages: vec![format!("E310 source-arena build: {e}")],
            })?;
            let arena_len = arena.record_count();
            collector.record(arena_timer.finish(arena_len, arena_len));

            let index_name = format!("{}:{}", source_name, spec.group_by.join(","));
            let index_timer =
                stage_metrics::StageTimer::new(stage_metrics::StageName::IndexBuild {
                    name: index_name,
                });
            let mut secondary_index = SecondaryIndex::build(&arena, &spec.group_by, schema_pins)
                .map_err(|e| PipelineError::Compilation {
                    transform_name: String::new(),
                    messages: vec![e.to_string()],
                })?;
            collector.record(index_timer.finish(arena_len, arena_len));

            if !spec.already_sorted {
                for partition in secondary_index.groups.values_mut() {
                    if !sort::is_sorted(&arena, partition, &spec.sort_by) {
                        sort::sort_partition(&arena, partition, &spec.sort_by);
                    }
                }
            }

            window_runtime.top[idx] = Some(crate::executor::window_runtime::WindowRuntime {
                arena: Arc::new(arena),
                index: Arc::new(secondary_index),
            });
        }

        // D12: a global-fold aggregate (group_by: []) must still emit
        // one row over empty input, so we cannot early-return when the
        // plan contains an Aggregation node — the DAG walk needs to
        // fire the aggregator's empty-input special case. Generalized
        // from primary-only to "every source ingested zero records".
        if drained_records.values().all(|v| v.is_empty()) && !plan.has_branching() {
            return Ok((
                counters,
                dlq_entries,
                rss_bytes(),
                watermarks,
                BTreeMap::new(),
            ));
        }

        Self::execute_dag_branching(
            config,
            drained_records,
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
        mut source_records: HashMap<String, Vec<(Record, u64)>>,
        source_configs: &[crate::config::SourceConfig],
        writers: WriterRegistry,
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
        memory_budget: crate::pipeline::memory::MemoryBudget,
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

        // Seed source-scope vars on every distinct per-record file Arc
        // across every declared source. Channel overrides (per source-
        // node name) layer atop Transform-`declares: scope: source`
        // defaults; the per-file inner map uses `or_insert` for
        // declared defaults (so any source-scope writer that planted
        // earlier wins) and `insert` for channel values (channel
        // always wins). The per-record file path is read off the
        // `$source.file` engine-stamped column on each record.
        let declared_source_defaults = crate::config::collect_source_var_defaults(&config.nodes);
        if (!declared_source_defaults.is_empty() || !params.source_vars.is_empty())
            && let Ok(mut map) = stable.source_vars.write()
        {
            for src_cfg in source_configs {
                let Some(records) = source_records.get(&src_cfg.name) else {
                    continue;
                };
                let channel = params.source_vars.get(&src_cfg.name);
                let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
                for (record, _) in records {
                    let Some(file_path) = dispatch::source_file_path_of(record) else {
                        continue;
                    };
                    if !seen.insert(file_path) {
                        continue;
                    }
                    let entry = map.entry(Arc::from(file_path)).or_default();
                    for (k, v) in &declared_source_defaults {
                        entry.entry(k.clone()).or_insert_with(|| v.clone());
                    }
                    if let Some(ch) = channel {
                        for (k, v) in ch {
                            entry.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
        }

        // Pre-seed `$record.<key>` defaults into every materialized
        // record so init-phase reads observe the declared/channel
        // default before any per-record `state` writer fires. Channel
        // record-vars layer atop Transform-`declares: scope: record`
        // defaults; existing per-record entries (if any) are preserved.
        let record_var_seed: IndexMap<String, Value> = {
            let mut seed = crate::config::collect_record_var_defaults(&config.nodes);
            for (k, v) in &params.record_vars {
                seed.insert(k.clone(), v.clone());
            }
            seed
        };
        if !record_var_seed.is_empty() {
            for recs in source_records.values_mut() {
                for (record, _) in recs {
                    record.seed_record_vars(&record_var_seed);
                }
            }
        }
        // `$source.batch` is per-pipeline-run scalar today (sub-issue
        // #54 introduces per-source attribution as a separate stamp).
        let source_batch_arc: Arc<str> = Arc::from(uuid::Uuid::now_v7().to_string());
        let source_ingestion_timestamp = pipeline_start_time;

        let strategy = config.error_handling.strategy;
        // `$source.count` is the pipeline-wide total across every
        // declared source. Per-source attribution is sub-issue #54.
        let total_records: u64 = source_records.values().map(|v| v.len() as u64).sum();
        let source_count: u64 = total_records;

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

        // Construct the dispatcher context. Mutable per-walk state
        // (node_buffers, counters, DLQ, timers, output writers,
        // output errors, the visited-source set backing the
        // dual-counter semantic) lives on the context; the immutable
        // plan-time refs are borrowed for the lifetime of the walk.
        //
        // `counters` and `dlq_entries` are taken out of their `&mut`
        // borrows here, mutated through the dispatcher, and written
        // back at the end of the walk.
        let total_per_source: HashMap<Arc<str>, u64> = source_records
            .iter()
            .map(|(name, records)| (Arc::from(name.as_str()), records.len() as u64))
            .collect();

        // Re-channel each source's drained record set into a fresh
        // bounded `(Sender, Receiver)` pair so the dispatch loop's
        // Source arm consumes records through the same
        // `mpsc::Receiver` interface it would see under live
        // streaming. Capacity is sized to the source's record count
        // (with a `.max(1)` to satisfy `channel`'s non-zero
        // requirement), and the sender is fed synchronously then
        // dropped so `recv().await` returns `None` once the records
        // drain.
        let mut source_records_rx: HashMap<String, tokio::sync::mpsc::Receiver<(Record, u64)>> =
            HashMap::with_capacity(source_records.len());
        for (name, records) in source_records.drain() {
            let (tx, rx) = tokio::sync::mpsc::channel(records.len().max(1));
            for item in records {
                if tx.try_send(item).is_err() {
                    return Err(PipelineError::Internal {
                        op: "executor",
                        node: name.clone(),
                        detail: "source re-channel try_send failed unexpectedly".to_string(),
                    });
                }
            }
            drop(tx);
            source_records_rx.insert(name, rx);
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
            source_count,
            source_ingestion_timestamp,
            strategy,

            node_buffers: HashMap::new(),
            source_records: source_records_rx,
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
        let init_phase_set = compute_init_phase_node_set(plan);
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

        // Take the pipeline-scoped TempDir out of the context. Held
        // until the very end of the walk so any operator-side spill
        // files survive the topo loop; dropped explicitly below
        // after the metrics flush so the directory's recursive
        // remove runs after every reader has gone away.
        let pipeline_spill_root = ctx.spill_root().clone();

        // Drain mutable per-walk state out of the context for the
        // post-walk reporting + caller-facing return tuple.
        let transform_timer = ctx.transform_timer;
        let route_timer = ctx.route_timer;
        let projection_timer = ctx.projection_timer;
        let write_timer = ctx.write_timer;
        let records_emitted = ctx.records_emitted;
        let output_errors = ctx.output_errors;
        let collector = ctx.collector;
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

        Ok((
            std::mem::take(counters),
            std::mem::take(dlq_entries),
            rss_bytes(),
            ctx.watermarks,
            rollback_cursors,
        ))
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

/// Ingest a Source's records into a bounded `SourceStream` and drain
/// to the read-side handle. Used uniformly by every declared Source —
/// no primary asymmetry.
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

        // Widen the reader schema with `$source.file` and `$source.name`
        // engine-stamped tail columns. The stamps travel with every record
        // so downstream operators resolve `$source.file` / `$source.name`
        // per-record via column reads (see `dispatch::source_file_arc_of`
        // and `dispatch::source_name_arc_of`) instead of indexing external
        // row-keyed Vecs. Tail order is load-bearing — see
        // `bind_schema::columns_from_decl`: `$ck.<field>` shadows first,
        // then `$widened`, then `$source.file`, then `$source.name` last.
        let mut widened_builder =
            clinker_record::SchemaBuilder::with_capacity(reader_schema.column_count() + 2);
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
            .build();

        let static_source_file: Arc<str> = if src_cfg.path_str().is_empty() {
            Arc::from("<source>")
        } else {
            Arc::from(src_cfg.path_str())
        };
        let source_name_arc: Arc<str> = Arc::from(src_cfg.name.as_str());

        let watermark_column_idx: Option<usize> = match src_cfg.watermark.as_ref() {
            None => None,
            Some(wm) => match widened_schema.index(wm.column.as_str()) {
                Some(i) => Some(i),
                None => {
                    return Err(PipelineError::Config(
                        crate::config::ConfigError::Validation(format!(
                            "source '{}' declares watermark.column = '{}' but no such column \
                             exists on the runtime schema (declared columns: {:?})",
                            src_cfg.name,
                            wm.column,
                            widened_schema.columns(),
                        )),
                    ));
                }
            },
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
                    if let Some(idx) = watermark_column_idx
                        && let Some(value) = values.get(idx)
                        && let Some(ts_nanos) = value_to_event_time_nanos(value)
                    {
                        watermark_observations.push((Arc::clone(&file_arc), ts_nanos));
                    }
                    values.push(clinker_record::Value::String(
                        file_arc.as_ref().to_string().into_boxed_str(),
                    ));
                    values.push(clinker_record::Value::String(
                        source_name_arc.as_ref().to_string().into_boxed_str(),
                    ));
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
fn compute_init_phase_node_set(
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
/// Returns `Ok((modified_record, Ok(())))` on emit,
/// `Ok((record, Err(SkipReason)))` on skip. On error, returns
/// `(transform_name, EvalError)`.
#[allow(clippy::result_large_err)]
pub(crate) fn evaluate_single_transform(
    record: &Record,
    transform_name: &str,
    evaluator: &mut ProgramEvaluator,
    ctx: &EvalContext,
    _output_schema: &Arc<Schema>,
) -> Result<(Record, Result<(), SkipReason>), (String, cxl::eval::EvalError)> {
    let input = record;
    match evaluator
        .eval_record::<NullStorage>(ctx, input, None)
        .map_err(|e| (transform_name.to_string(), e))?
    {
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
            Ok((out, Ok(())))
        }
        EvalResult::Skip(reason) => Ok((input.clone(), Err(reason))),
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
) -> Result<(Record, Result<(), SkipReason>), (String, cxl::eval::EvalError)> {
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

    let result = if let Some(key) = key {
        if let Some(partition) = index.get(&key) {
            let pos_in_partition = partition.iter().position(|&p| p == record_pos).unwrap_or(0);
            let wctx = PartitionWindowContext::new(arena, partition, pos_in_partition);
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

    match result {
        EvalResult::Emit {
            fields: emitted,
            record_vars,
            ..
        } => {
            let mut out = record_with_emitted_fields(record, &emitted);
            for (name, value) in &emitted {
                out.set(name, value.clone());
            }
            for (key, value) in *record_vars {
                let _ = out.set_record_var(&key, value);
            }
            Ok((out, Ok(())))
        }
        EvalResult::Skip(reason) => Ok((record.clone(), Err(reason))),
    }
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
        .memory_limit
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
    mod post_aggregate_window_spilled;
    mod post_combine_array_field;
    mod post_combine_synthetic_ck;
    mod post_combine_window_strategies;
    mod strict_pipeline_zero_overhead;
    mod window_recompute_correctness;
}
