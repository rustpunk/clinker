pub mod stage_metrics;

use std::collections::HashMap;
use std::io::{BufWriter, Read, Write};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use clinker_record::{PipelineCounters, Record, RecordStorage, Schema, Value};
use indexmap::IndexMap;
use rayon::prelude::*;

use crate::config::{ErrorStrategy, OutputConfig, PipelineConfig};
use crate::error::PipelineError;
use crate::pipeline::arena::Arena;
use crate::pipeline::index::{GroupByKey, SecondaryIndex, value_to_group_key};
use crate::pipeline::memory::{MemoryBudget, rss_bytes};
use crate::pipeline::sort;
use crate::pipeline::window_context::PartitionWindowContext;
use crate::plan::execution::{ExecutionPlanDag, ParallelismClass, PlanNode};
use crate::projection::{project_output, project_output_with_meta};
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

/// Phase 16b Task 16b.7 — executor-internal transform spec.
///
/// Produced by walking `PipelineConfig::nodes` directly and matching on
/// `PipelineNode::{Transform, Aggregate, Route}` variants. Replaces the
/// deleted `TransformSpec` for executor read paths that still
/// consume a flat Vec-of-transforms view. The fields are the superset
/// that executor sites read from; see `build_transform_specs`.
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

/// Runtime parameters for a pipeline execution (not derived from config YAML).
pub struct PipelineRunParams {
    /// UUID v7 execution ID, unique per run.
    pub execution_id: String,
    /// Batch ID from --batch-id CLI flag or auto UUID v7.
    pub batch_id: String,
    /// Converted pipeline.vars (already validated and converted from serde_json).
    pub pipeline_vars: IndexMap<String, Value>,
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
    /// Human-readable execution summary (e.g., "Streaming", "TwoPass", "SortedStreaming").
    pub execution_summary: String,
    /// Whether any transform required arena allocation (window functions).
    pub required_arena: bool,
    /// Whether any transform required sorted input (correlation key).
    pub required_sorted_input: bool,
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
struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<Value> {
        None
    }
    fn available_fields(&self, _: u32) -> Vec<&str> {
        vec![]
    }
    fn record_count(&self) -> u32 {
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
    fn has_distinct(&self) -> bool {
        self.typed
            .program
            .statements
            .iter()
            .any(|s| matches!(s, Statement::Distinct { .. }))
    }
}

/// Build ProgramEvaluators for a set of compiled transforms.
fn build_evaluators(transforms: &[CompiledTransform]) -> Vec<ProgramEvaluator> {
    transforms
        .iter()
        .map(|t| ProgramEvaluator::new(Arc::clone(&t.typed), t.has_distinct()))
        .collect()
}

/// Compiled route branch: a named CXL boolean condition evaluator.
struct CompiledRouteBranch {
    name: String,
    evaluator: ProgramEvaluator,
}

/// Compiled route configuration for multi-output dispatch.
struct CompiledRoute {
    branches: Vec<CompiledRouteBranch>,
    default: String,
    mode: crate::config::RouteMode,
}

impl CompiledRoute {
    /// Evaluate route conditions against emitted fields.
    ///
    /// Returns the list of output names the record should be dispatched to.
    /// In Exclusive mode: first matching branch (or default).
    /// In Inclusive mode: all matching branches (or default if none match).
    fn evaluate(
        &mut self,
        emitted: &IndexMap<String, Value>,
        metadata: &IndexMap<String, Value>,
        ctx: &EvalContext,
    ) -> Result<Vec<String>, cxl::eval::EvalError> {
        // Convert emitted IndexMap to HashMap for HashMapResolver.
        // Include $meta.* entries so route conditions can reference metadata.
        let mut hash_fields: HashMap<String, Value> = emitted
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (key, value) in metadata {
            hash_fields.insert(format!("$meta.{key}"), value.clone());
        }
        let resolver = clinker_record::HashMapResolver::new(hash_fields);

        match self.mode {
            crate::config::RouteMode::Exclusive => {
                for branch in &mut self.branches {
                    match branch
                        .evaluator
                        .eval_record::<NullStorage>(ctx, &resolver, None)?
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
                        .eval_record::<NullStorage>(ctx, &resolver, None)?
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

/// Per-output writer channel for multi-output dispatch.
///
/// Each output gets a dedicated thread with a bounded SPSC channel.
/// Records are sent as `Vec<Record>` batches (one batch per chunk or per
/// accumulated routing result) to amortize ~50ns/send channel overhead.
struct OutputChannel {
    sender: crossbeam_channel::Sender<Vec<Record>>,
    cancel_sender: crossbeam_channel::Sender<()>,
    handle: std::thread::JoinHandle<Result<(), PipelineError>>,
}

/// Spawn a dedicated writer thread per output.
///
/// Each thread owns a `CsvWriter` and reads from a bounded channel (capacity 4).
/// The channel carries `Vec<Record>` batches. The thread uses `crossbeam::select!`
/// to wait on data or cancel signals.
fn spawn_writer_threads(
    writers: HashMap<String, Box<dyn Write + Send>>,
    output_configs: &[OutputConfig],
    output_schema: Arc<Schema>,
) -> HashMap<String, OutputChannel> {
    writers
        .into_iter()
        .map(|(name, raw_writer)| {
            let (data_tx, data_rx) = crossbeam_channel::bounded::<Vec<Record>>(4);
            let (cancel_tx, cancel_rx) = crossbeam_channel::bounded::<()>(0);
            let config = output_configs
                .iter()
                .find(|o| o.name == name)
                .unwrap()
                .clone();
            let schema = Arc::clone(&output_schema);

            let handle = std::thread::Builder::new()
                .name(format!("cxl-writer-{name}"))
                .spawn(move || {
                    let mut writer = build_format_writer(&config, raw_writer, schema)?;

                    loop {
                        crossbeam_channel::select! {
                            recv(data_rx) -> msg => match msg {
                                Ok(records) => {
                                    for record in &records {
                                        writer.write_record(record)?;
                                    }
                                }
                                Err(_) => break, // all senders dropped = normal EOF
                            },
                            recv(cancel_rx) -> _ => {
                                // Cancel signal — drain remaining buffered items
                                while let Ok(records) = data_rx.try_recv() {
                                    for record in &records {
                                        writer.write_record(record)?;
                                    }
                                }
                                break;
                            },
                        }
                    }
                    writer.flush()?;
                    Ok(())
                })
                .expect("failed to spawn writer thread");

            (
                name,
                OutputChannel {
                    sender: data_tx,
                    cancel_sender: cancel_tx,
                    handle,
                },
            )
        })
        .collect()
}

/// Join all writer threads, collecting ALL results. Never early-return.
/// DataFusion Collection pattern (PR #14439).
fn join_writer_threads(channels: HashMap<String, OutputChannel>) -> Result<(), PipelineError> {
    let mut errors = Vec::new();
    for (_name, channel) in channels {
        drop(channel.sender); // signal EOF
        drop(channel.cancel_sender);
        match channel.handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => errors.push(e),
            Err(panic_payload) => std::panic::resume_unwind(panic_payload),
        }
    }
    match errors.len() {
        0 => Ok(()),
        1 => Err(errors.into_iter().next().unwrap()),
        _ => Err(PipelineError::Multiple(errors)),
    }
}

/// Flush accumulated per-output batches through channels.
fn flush_output_batches(
    per_output_batches: &mut HashMap<String, Vec<Record>>,
    output_channels: &HashMap<String, OutputChannel>,
) -> Result<(), PipelineError> {
    for (name, batch) in per_output_batches.drain() {
        if batch.is_empty() {
            continue;
        }
        if let Some(channel) = output_channels.get(&name)
            && channel.sender.send(batch).is_err()
        {
            // Writer thread died (panicked or errored) — channel disconnected.
            // Don't deadlock; stop sending to this output. Errors will be
            // collected in join_writer_threads.
        }
    }
    Ok(())
}
/// Record that failed evaluation, queued for DLQ output.
#[derive(Debug)]
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
    /// `true` if this record's own evaluation caused the DLQ entry (root cause).
    /// `false` if DLQ'd due to correlated group failure (collateral).
    /// Serialized as `_cxl_dlq_trigger` column in DLQ CSV.
    pub trigger: bool,
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

/// Extract correlation key values from a record as a vector of GroupByKey.
fn extract_correlation_key(
    record: &Record,
    correlation_key: &crate::config::CorrelationKey,
) -> Vec<GroupByKey> {
    let fields = correlation_key.fields();
    fields
        .iter()
        .map(|field| {
            match record.get(field) {
                Some(value) if !value.is_null() => {
                    // Treat empty strings as null (CSV has no native null concept)
                    if let Value::String(s) = value
                        && s.is_empty()
                    {
                        return GroupByKey::Null;
                    }
                    // Use value_to_group_key for consistent hashing/comparison
                    match value_to_group_key(value, field, None, 0) {
                        Ok(Some(gk)) => gk,
                        Ok(None) => GroupByKey::Null,
                        Err(_) => GroupByKey::Null,
                    }
                }
                _ => GroupByKey::Null,
            }
        })
        .collect()
}

/// Unified pipeline executor. Plan-driven branching:
/// - Streaming (single-pass) when no window functions
/// - TwoPass (arena + indices) when windows are present
pub struct PipelineExecutor;

impl PipelineExecutor {
    /// Phase 16b Wave 4ab — `&CompiledPlan`-consuming public entry point.
    ///
    /// Accepts the typed `CompiledPlan` handle returned by
    /// [`crate::config::PipelineConfig::compile`] and forwards to
    /// [`Self::run_with_readers_writers`] using the plan's embedded
    /// [`PipelineConfig`]. This is the forward-compatible entry point;
    /// the legacy `&PipelineConfig`-consuming variant is retained
    /// pending the full executor-internal cutover in a follow-up.
    ///
    /// The primary (driving) source is the first source node in
    /// declaration order (`config.source_configs().next()`). Callers
    /// that need to drive the pipeline from a non-first declared
    /// source must use
    /// [`Self::run_plan_with_readers_writers_with_primary`] instead —
    /// declaration-order-as-primary is a convenience of this wrapper,
    /// not a contract of the executor itself.
    ///
    /// D3b compile-fail guarantee — `&PipelineConfig` is NOT accepted:
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
    pub fn run_plan_with_readers_writers(
        plan: &crate::plan::CompiledPlan,
        readers: HashMap<String, Box<dyn Read + Send>>,
        writers: HashMap<String, Box<dyn Write + Send>>,
        params: &PipelineRunParams,
    ) -> Result<ExecutionReport, PipelineError> {
        let config = plan.config();
        let primary_name = config
            .source_configs()
            .next()
            .map(|s| s.name.clone())
            .ok_or_else(|| {
                PipelineError::Config(crate::config::ConfigError::Validation(
                    "pipeline declares no source nodes; cannot infer primary driving input"
                        .to_string(),
                ))
            })?;
        Self::run_with_readers_writers(config, &primary_name, readers, writers, params)
    }

    /// Same as [`Self::run_plan_with_readers_writers`] but with an
    /// explicit `primary` driving-source name. Use this when the
    /// driving input is not the first-declared source — e.g., lookup
    /// baselines that put the reference table earlier in YAML for
    /// readability but drive the pipeline from the probe source.
    ///
    /// `primary` must match the `name` of one of the source nodes
    /// declared in the pipeline config, and a reader for that name
    /// must be present in `readers`. Violations surface as
    /// `PipelineError::Config(ConfigError::Validation(..))`.
    pub fn run_plan_with_readers_writers_with_primary(
        plan: &crate::plan::CompiledPlan,
        primary: &str,
        readers: HashMap<String, Box<dyn Read + Send>>,
        writers: HashMap<String, Box<dyn Write + Send>>,
        params: &PipelineRunParams,
    ) -> Result<ExecutionReport, PipelineError> {
        Self::run_with_readers_writers(plan.config(), primary, readers, writers, params)
    }

    /// Phase 16b Wave 4ab — `&CompiledPlan`-consuming `--explain` text entry.
    pub fn explain_plan(plan: &crate::plan::CompiledPlan) -> Result<String, PipelineError> {
        Self::explain(plan.config())
    }

    /// Phase 16b Wave 4ab — `&CompiledPlan`-consuming `--explain` DAG entry.
    pub fn explain_plan_dag(
        plan: &crate::plan::CompiledPlan,
    ) -> Result<(ExecutionPlanDag, ()), PipelineError> {
        Self::explain_dag(plan.config())
    }

    /// Run with explicit reader/writer registries.
    ///
    /// `readers` and `writers` are keyed by the input/output `name` fields from
    /// the pipeline config. For single-input/output pipelines, pass single-entry
    /// HashMaps.
    ///
    /// `primary` is the name of the source node that drives the
    /// pipeline: its reader is consumed as the streaming input, and
    /// every other entry in `readers` flows into `build_lookup_tables`
    /// for lookup-stage build. Source declaration order in YAML is
    /// irrelevant — `primary` is chosen explicitly. If `primary` does
    /// not match a declared source name, or no reader is registered
    /// under that name, the function returns
    /// `PipelineError::Config(ConfigError::Validation(..))`.
    ///
    /// Returns an [`ExecutionReport`] containing record counts, DLQ entries,
    /// execution mode, peak RSS, and wall-clock start/finish timestamps.
    pub(crate) fn run_with_readers_writers(
        config: &PipelineConfig,
        primary: &str,
        mut readers: HashMap<String, Box<dyn Read + Send>>,
        writers: HashMap<String, Box<dyn Write + Send>>,
        params: &PipelineRunParams,
    ) -> Result<ExecutionReport, PipelineError> {
        let started_at = Utc::now();

        // Resolve the primary source config by name. The driving
        // input is chosen explicitly via `primary` — declaration
        // order in `config.source_configs()` plays no role.
        let source_configs: Vec<_> = config.source_configs().cloned().collect();
        let output_configs: Vec<_> = config.output_configs().cloned().collect();
        let primary_idx = source_configs
            .iter()
            .position(|s| s.name == primary)
            .ok_or_else(|| {
                PipelineError::Config(crate::config::ConfigError::Validation(format!(
                    "primary source '{primary}' is not declared in the pipeline config",
                )))
            })?;
        let input = &source_configs[primary_idx];
        let reader = readers.remove(&input.name).ok_or_else(|| {
            PipelineError::Config(crate::config::ConfigError::Validation(format!(
                "no reader registered for input '{}'",
                input.name
            )))
        })?;
        let mut collector = stage_metrics::StageCollector::default();
        let reader_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::ReaderInit);
        let raw_reader = build_format_reader(input, reader)?;
        // Wrap with schema-based type coercion if the source declares typed columns.
        let mut format_reader = wrap_with_schema_coercion(raw_reader, config, &input.name)?;
        let schema = format_reader.schema()?;
        collector.record(reader_timer.finish(0, 0));

        // Phase 16b Task 16b.9 — compile-time CXL typecheck.
        // `config.compile()` drives the unified nodes: pipeline
        // through stages 1-4 (topology/path validation), then the
        // stage-4.5 `bind_schema` pass that typechecks every CXL
        // body against the author-declared source schema and
        // propagates emitted columns downstream. Type errors surface
        // as E200 diagnostics here, BEFORE any file handle opens.
        let compile_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Compile);
        // Phase 16b Task 16b.9: ALL CXL typechecking happens at
        // `config.compile()` time via the `bind_schema` stage-4.5 pass.
        // The runtime never re-typechecks; it pulls each transform's
        // pre-typechecked `Arc<TypedProgram>` straight from the
        // `CompiledPlan`'s artifacts map, keyed by node name. There is
        // no second typecheck pass — `compile_transforms` is gone.
        let _ = &schema;
        let validated_plan = config
            .compile(&crate::config::CompileContext::default())
            .map_err(|diags| PipelineError::Compilation {
                transform_name: String::new(),
                messages: diags.iter().map(|d| d.message.clone()).collect(),
            })?;
        let resolved_transforms_owned = crate::executor::build_transform_specs(config);
        let resolved_transforms: Vec<&TransformSpec> = resolved_transforms_owned.iter().collect();
        let compiled_transforms: Vec<CompiledTransform> = resolved_transforms
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

        // Compile route conditions if any transform has a route config.
        // Collect all emitted field names for route condition resolution.
        let compiled_route = {
            let route_config = resolved_transforms
                .iter()
                .rev()
                .find_map(|t| t.route.as_ref());
            match route_config {
                Some(rc) => {
                    let mut emitted_fields: Vec<String> =
                        schema.columns().iter().map(|c| c.to_string()).collect();
                    for ct in &compiled_transforms {
                        for stmt in &ct.typed.program.statements {
                            if let Statement::Emit { name, .. } = stmt
                                && !emitted_fields.contains(&name.to_string())
                            {
                                emitted_fields.push(name.to_string());
                            }
                        }
                    }
                    Some(Self::compile_route(rc, &emitted_fields)?)
                }
                None => None,
            }
        };

        // Build ExecutionPlanDag to determine mode
        let compiled_refs: Vec<(&str, &TypedProgram)> = compiled_transforms
            .iter()
            .map(|ct| (ct.name.as_str(), ct.typed.as_ref()))
            .collect();

        let runtime_input_schema: Vec<String> =
            schema.columns().iter().map(|c| c.to_string()).collect();
        let plan = ExecutionPlanDag::compile_with_runtime_schema(
            config,
            &compiled_refs,
            Some(&runtime_input_schema),
        )
        .map_err(|e| PipelineError::Compilation {
            transform_name: String::new(),
            messages: vec![e.to_string()],
        })?;
        collector.record(compile_timer.finish(0, 0));

        let execution_summary = plan.execution_summary();
        let required_arena = plan.required_arena();
        let required_sorted_input = plan.required_sorted_input();

        // Validate that all configured outputs have registered writers.
        for output in &output_configs {
            if !writers.contains_key(&output.name) {
                return Err(PipelineError::Config(
                    crate::config::ConfigError::Validation(format!(
                        "no writer registered for output '{}'",
                        output.name
                    )),
                ));
            }
        }

        // ── Build lookup tables for transforms with `lookup:` config ──
        let lookup_tables = Self::build_lookup_tables(config, &mut readers, &source_configs)?;

        let (counters, dlq_entries, peak_rss_bytes) = Self::execute_dag(
            config,
            input,
            format_reader,
            writers,
            &compiled_transforms,
            compiled_route,
            &plan,
            params,
            &mut collector,
            lookup_tables,
        )?;

        let stages = collector.into_stages();
        let (total_cpu_user_ns, total_cpu_sys_ns, total_io_read_bytes, total_io_write_bytes) =
            sum_cpu_io_totals(&stages);

        Ok(ExecutionReport {
            counters,
            dlq_entries,
            execution_summary,
            required_arena,
            required_sorted_input,
            peak_rss_bytes,
            total_cpu_user_ns,
            total_cpu_sys_ns,
            total_io_read_bytes,
            total_io_write_bytes,
            started_at,
            finished_at: Utc::now(),
            stages,
        })
    }

    /// Flush a buffered correlation group: evaluate all records, DLQ entire group if any fails.
    #[allow(clippy::too_many_arguments)]
    fn flush_correlated_group(
        buffer: &mut Vec<(Record, u64)>,
        _config: &PipelineConfig,
        output_configs: &[OutputConfig],
        primary_output: &OutputConfig,
        input: &crate::config::SourceConfig,
        _pipeline_start_time: chrono::NaiveDateTime,
        stable: &StableEvalContext,
        transform_names: &[&str],
        evaluators: &mut [ProgramEvaluator],
        mut compiled_route: Option<&mut CompiledRoute>,
        _params: &PipelineRunParams,
        strategy: ErrorStrategy,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        per_output_batches: &mut HashMap<String, Vec<Record>>,
        _max_group_buffer: u64,
    ) {
        if buffer.is_empty() {
            return;
        }
        let source_file_arc: Arc<str> = Arc::from(input.path.as_str());

        // Evaluate all records in the group, collect results
        #[allow(clippy::type_complexity)]
        let mut results: Vec<(
            Record,
            u64,
            Result<EvalResult, (String, cxl::eval::EvalError)>,
        )> = Vec::with_capacity(buffer.len());
        let mut any_failed = false;
        let mut first_failure_message: Option<String> = None;

        for (record, rn) in buffer.drain(..) {
            let ctx = EvalContext {
                stable,
                source_file: &source_file_arc,
                source_row: rn,
            };
            let result = evaluate_record(&record, transform_names, evaluators, &ctx);
            if result.is_err() && !any_failed {
                any_failed = true;
                if let Err((_, ref eval_err)) = result {
                    first_failure_message = Some(eval_err.to_string());
                }
            }
            results.push((record, rn, result));
        }

        if any_failed {
            // DLQ entire group
            for (record, rn, result) in results {
                let (is_trigger, error_message) = match result {
                    Err((_, eval_err)) => (true, eval_err.to_string()),
                    Ok(_) => (
                        false,
                        format!(
                            "correlated with failure in group: {}",
                            first_failure_message.as_deref().unwrap_or("unknown")
                        ),
                    ),
                };
                counters.dlq_count += 1;
                dlq_entries.push(DlqEntry {
                    source_row: rn,
                    category: crate::dlq::DlqErrorCategory::ValidationFailure,
                    error_message,
                    original_record: record,
                    stage: Some("transform:correlated_dlq".to_string()),
                    route: None,
                    trigger: is_trigger,
                });
            }
        } else {
            // All passed — dispatch to outputs
            for (record, rn, result) in results {
                match result {
                    Ok(EvalResult::Emit {
                        fields: emitted,
                        metadata,
                    }) => {
                        if let Some(ref mut route) = compiled_route {
                            let ctx = EvalContext {
                                stable,
                                source_file: &source_file_arc,
                                source_row: rn,
                            };
                            match route.evaluate(&emitted, &metadata, &ctx) {
                                Ok(targets) => {
                                    for target in &targets {
                                        let out_cfg = output_configs
                                            .iter()
                                            .find(|o| o.name == *target)
                                            .unwrap_or(primary_output);
                                        let projected = project_output_with_meta(
                                            &record, &emitted, &metadata, out_cfg,
                                        );
                                        per_output_batches
                                            .entry(target.clone())
                                            .or_default()
                                            .push(projected);
                                    }
                                    counters.ok_count += 1;
                                }
                                Err(route_err) => {
                                    if strategy == ErrorStrategy::FailFast {
                                        // Can't propagate error from here easily;
                                        // this path shouldn't hit FailFast with correlation
                                        counters.dlq_count += 1;
                                        dlq_entries.push(DlqEntry {
                                            source_row: rn,
                                            category:
                                                crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                            error_message: route_err.to_string(),
                                            original_record: record,
                                            stage: Some(DlqEntry::stage_route_eval()),
                                            route: None,
                                            trigger: true,
                                        });
                                    } else {
                                        counters.dlq_count += 1;
                                        dlq_entries.push(DlqEntry {
                                            source_row: rn,
                                            category:
                                                crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                            error_message: route_err.to_string(),
                                            original_record: record,
                                            stage: Some(DlqEntry::stage_route_eval()),
                                            route: None,
                                            trigger: true,
                                        });
                                    }
                                }
                            }
                        } else {
                            // Single-output in multi-output path (shouldn't happen, but handle)
                            let projected = project_output(&record, &emitted, primary_output);
                            per_output_batches
                                .entry(primary_output.name.clone())
                                .or_default()
                                .push(projected);
                            counters.ok_count += 1;
                        }
                    }
                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                        counters.filtered_count += 1;
                    }
                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                        counters.distinct_count += 1;
                    }
                    Err(_) => unreachable!("failures handled in any_failed branch"),
                }
            }
        }
    }

    /// Flush a buffered correlation group for single-output path.
    #[allow(clippy::too_many_arguments)]
    fn flush_correlated_group_single_output(
        buffer: &mut Vec<(Record, u64)>,
        _config: &PipelineConfig,
        primary_output: &OutputConfig,
        input: &crate::config::SourceConfig,
        _pipeline_start_time: chrono::NaiveDateTime,
        stable: &StableEvalContext,
        transform_names: &[&str],
        evaluators: &mut [ProgramEvaluator],
        _params: &PipelineRunParams,
        _strategy: ErrorStrategy,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        writer: &mut dyn FormatWriter,
        _max_group_buffer: u64,
    ) -> Result<(), PipelineError> {
        if buffer.is_empty() {
            return Ok(());
        }
        let source_file_arc: Arc<str> = Arc::from(input.path.as_str());

        #[allow(clippy::type_complexity)]
        let mut results: Vec<(
            Record,
            u64,
            Result<EvalResult, (String, cxl::eval::EvalError)>,
        )> = Vec::with_capacity(buffer.len());
        let mut any_failed = false;
        let mut first_failure_message: Option<String> = None;

        for (record, rn) in buffer.drain(..) {
            let ctx = EvalContext {
                stable,
                source_file: &source_file_arc,
                source_row: rn,
            };
            let result = evaluate_record(&record, transform_names, evaluators, &ctx);
            if result.is_err() && !any_failed {
                any_failed = true;
                if let Err((_, ref eval_err)) = result {
                    first_failure_message = Some(eval_err.to_string());
                }
            }
            results.push((record, rn, result));
        }

        if any_failed {
            for (record, rn, result) in results {
                let (is_trigger, error_message) = match result {
                    Err((_, eval_err)) => (true, eval_err.to_string()),
                    Ok(_) => (
                        false,
                        format!(
                            "correlated with failure in group: {}",
                            first_failure_message.as_deref().unwrap_or("unknown")
                        ),
                    ),
                };
                counters.dlq_count += 1;
                dlq_entries.push(DlqEntry {
                    source_row: rn,
                    category: crate::dlq::DlqErrorCategory::ValidationFailure,
                    error_message,
                    original_record: record,
                    stage: Some("transform:correlated_dlq".to_string()),
                    route: None,
                    trigger: is_trigger,
                });
            }
        } else {
            for (record, _rn, result) in results {
                match result {
                    Ok(EvalResult::Emit {
                        fields: emitted, ..
                    }) => {
                        let projected = project_output(&record, &emitted, primary_output);
                        writer.write_record(&projected)?;
                        counters.ok_count += 1;
                    }
                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                        counters.filtered_count += 1;
                    }
                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                        counters.distinct_count += 1;
                    }
                    Err(_) => unreachable!("failures handled in any_failed branch"),
                }
            }
        }

        Ok(())
    }

    /// Dispatch an emitted record to outputs via route evaluation (multi-output helper).
    #[allow(clippy::too_many_arguments)]
    fn dispatch_to_outputs(
        record: &Record,
        emitted: &IndexMap<String, Value>,
        metadata: &IndexMap<String, Value>,
        _config: &PipelineConfig,
        output_configs: &[OutputConfig],
        primary_output: &OutputConfig,
        compiled_route: Option<&mut CompiledRoute>,
        ctx: &EvalContext,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        per_output_batches: &mut HashMap<String, Vec<Record>>,
        _strategy: ErrorStrategy,
        row_num: u64,
    ) {
        if let Some(route) = compiled_route {
            match route.evaluate(emitted, metadata, ctx) {
                Ok(targets) => {
                    for target in &targets {
                        let out_cfg = output_configs
                            .iter()
                            .find(|o| o.name == *target)
                            .unwrap_or(primary_output);
                        let projected =
                            project_output_with_meta(record, emitted, metadata, out_cfg);
                        per_output_batches
                            .entry(target.clone())
                            .or_default()
                            .push(projected);
                    }
                    counters.ok_count += 1;
                }
                Err(route_err) => {
                    counters.dlq_count += 1;
                    dlq_entries.push(DlqEntry {
                        source_row: row_num,
                        category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                        error_message: route_err.to_string(),
                        original_record: record.clone(),
                        stage: Some(DlqEntry::stage_route_eval()),
                        route: None,
                        trigger: true,
                    });
                }
            }
        } else {
            let projected = project_output(record, emitted, primary_output);
            per_output_batches
                .entry(primary_output.name.clone())
                .or_default()
                .push(projected);
            counters.ok_count += 1;
        }
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
    /// Returns `(counters, dlq_entries, peak_rss_bytes)`.
    #[allow(clippy::too_many_arguments)]
    fn execute_dag(
        config: &PipelineConfig,
        input: &crate::config::SourceConfig,
        mut format_reader: Box<dyn FormatReader>,
        writers: HashMap<String, Box<dyn Write + Send>>,
        transforms: &[CompiledTransform],
        compiled_route: Option<CompiledRoute>,
        plan: &ExecutionPlanDag,
        params: &PipelineRunParams,
        collector: &mut stage_metrics::StageCollector,
        lookup_tables: HashMap<String, RuntimeLookup>,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, Option<u64>), PipelineError> {
        let output_configs: Vec<_> = config.output_configs().cloned().collect();
        let primary_output = &output_configs[0];
        let pipeline_start_time = chrono::Local::now().naive_local();

        // Pipeline-stable evaluation context (D59 / Task 16.3.13a). Built once
        // here, reused (via borrow) at every per-record dispatch site below.
        let stable = build_stable_eval_context(
            config,
            pipeline_start_time,
            &params.execution_id,
            &params.batch_id,
            &params.pipeline_vars,
        );
        let source_file_arc: Arc<str> = Arc::from(input.path.as_str());

        let mut counters = PipelineCounters::default();
        let mut dlq_entries: Vec<DlqEntry> = Vec::new();
        let strategy = config.error_handling.strategy;
        let is_multi_output = compiled_route.is_some() && writers.len() > 1;
        let mut compiled_route = compiled_route;

        let requires_arena = plan.required_arena();
        let requires_sorted_input = plan.required_sorted_input();

        // ── Phase 0+1: Read source and optionally build Arena ──

        if requires_arena {
            // TwoPass path: build Arena + indices, chunk-based evaluation
            let arena_fields =
                crate::plan::index::collect_arena_fields(&plan.indices_to_build, &input.name);
            let memory_limit = parse_memory_limit(config);
            let arena_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::ArenaBuild);
            let arena = Arena::build(
                &mut *format_reader,
                &arena_fields,
                memory_limit,
                params.shutdown_token.as_ref(),
            )
            .map_err(|e| PipelineError::Compilation {
                transform_name: String::new(),
                messages: vec![e.to_string()],
            })?;
            let arena_len = arena.record_count() as u64;
            collector.record(arena_timer.finish(arena_len, arena_len));

            let schema_pins: HashMap<String, clinker_record::schema_def::FieldDef> = input
                .schema_overrides
                .as_ref()
                .map(|overrides| {
                    overrides
                        .iter()
                        .map(|o| (o.name.clone(), o.clone()))
                        .collect()
                })
                .unwrap_or_default();

            let mut indices: Vec<SecondaryIndex> = Vec::new();
            for spec in &plan.indices_to_build {
                let index_name = format!("{}:{}", spec.source, spec.group_by.join(","));
                let index_timer =
                    stage_metrics::StageTimer::new(stage_metrics::StageName::IndexBuild {
                        name: index_name,
                    });
                let idx =
                    SecondaryIndex::build(&arena, &spec.group_by, &schema_pins).map_err(|e| {
                        PipelineError::Compilation {
                            transform_name: String::new(),
                            messages: vec![e.to_string()],
                        }
                    })?;
                collector.record(index_timer.finish(arena_len, arena_len));
                indices.push(idx);
            }

            // Phase 1.5: Sort partitions
            for (i, spec) in plan.indices_to_build.iter().enumerate() {
                if !spec.already_sorted {
                    for partition in indices[i].groups.values_mut() {
                        if !sort::is_sorted(&arena, partition, &spec.sort_by) {
                            sort::sort_partition(&arena, partition, &spec.sort_by);
                        }
                    }
                }
            }

            // Phase 2: Chunk-based evaluation with optional rayon parallelism
            let pool = build_thread_pool(config)?;
            let use_parallel = can_parallelize(plan);

            let output_schema_ref = arena.schema();
            let record_count = arena.record_count();
            if record_count == 0 {
                return Ok((PipelineCounters::default(), Vec::new(), rss_bytes()));
            }

            let mut rss_budget = MemoryBudget::from_config(config.pipeline.memory_limit.as_deref());
            let chunk_size = config
                .pipeline
                .concurrency
                .as_ref()
                .and_then(|c| c.chunk_size)
                .unwrap_or(1024) as u32;

            let build_record_from_arena = |pos: u32| -> Record {
                let schema = Arc::clone(output_schema_ref);
                let values: Vec<Value> = schema
                    .columns()
                    .iter()
                    .map(|col| arena.resolve_field(pos, col).unwrap_or(Value::Null))
                    .collect();
                Record::new(schema, values)
            };

            // Schema derivation scan
            let scan_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
            let mut records_scanned: u64 = 0;
            let mut first_emit_pos: Option<u32> = None;
            #[allow(clippy::type_complexity)]
            let mut first_emitted: Option<(
                Record,
                IndexMap<String, Value>,
                IndexMap<String, Value>,
            )> = None;
            let mut evaluators = build_evaluators(transforms);
            let transform_names: Vec<&str> = transforms.iter().map(|t| t.name.as_str()).collect();

            for pos in 0..record_count {
                records_scanned += 1;
                let record = build_record_from_arena(pos);
                let ctx = EvalContext {
                    stable: &stable,
                    source_file: &source_file_arc,
                    source_row: pos as u64 + 1,
                };
                let result = evaluate_record_with_window(
                    &record,
                    &transform_names,
                    &mut evaluators,
                    &ctx,
                    plan,
                    &arena,
                    &indices,
                    pos,
                );
                counters.total_count += 1;
                match result {
                    Ok(EvalResult::Emit {
                        fields: emitted,
                        metadata,
                    }) => {
                        first_emitted = Some((record, emitted, metadata));
                        first_emit_pos = Some(pos);
                        counters.ok_count += 1;
                        break;
                    }
                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                        counters.filtered_count += 1;
                    }
                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                        counters.distinct_count += 1;
                    }
                    Err((transform_name, eval_err)) => {
                        if strategy == ErrorStrategy::FailFast {
                            return Err(eval_err.into());
                        }
                        handle_error_no_writer(
                            &record,
                            pos as u64 + 1,
                            &eval_err,
                            Some(DlqEntry::stage_transform(&transform_name)),
                            &mut counters,
                            &mut dlq_entries,
                        );
                    }
                }
            }

            collector.record(
                scan_timer.finish(records_scanned, if first_emitted.is_some() { 1 } else { 0 }),
            );

            let final_output_schema = if let Some((ref rec, ref emitted, ref metadata)) =
                first_emitted
            {
                let projected = project_output_with_meta(rec, emitted, metadata, primary_output);
                Arc::clone(projected.schema())
            } else {
                Arc::clone(output_schema_ref)
            };

            // Evaluate chunk helper (same as execute_two_pass)
            // Returns accumulated transform eval duration for this chunk.
            #[allow(clippy::type_complexity)]
            let evaluate_chunk = |chunk: &mut Vec<(
                u32,
                Record,
                Option<Result<EvalResult, (String, cxl::eval::EvalError)>>,
            )>,
                                  evaluators: &mut Vec<ProgramEvaluator>|
             -> std::time::Duration {
                if use_parallel {
                    pool.install(|| {
                        chunk
                            .par_iter_mut()
                            .fold(
                                stage_metrics::ChunkTimers::default,
                                |mut timers, (pos, record, result)| {
                                    let start = std::time::Instant::now();
                                    let ctx = EvalContext {
                                        stable: &stable,
                                        source_file: &source_file_arc,
                                        source_row: *pos as u64 + 1,
                                    };
                                    let mut local_evals = build_evaluators(transforms);
                                    *result = Some(evaluate_record_with_window(
                                        record,
                                        &transform_names,
                                        &mut local_evals,
                                        &ctx,
                                        plan,
                                        &arena,
                                        &indices,
                                        *pos,
                                    ));
                                    timers.transform_eval += start.elapsed();
                                    timers
                                },
                            )
                            .reduce(stage_metrics::ChunkTimers::default, |a, b| a.merge(b))
                            .transform_eval
                    })
                } else {
                    let mut elapsed = std::time::Duration::ZERO;
                    for (pos, record, result) in chunk.iter_mut() {
                        let start = std::time::Instant::now();
                        let ctx = EvalContext {
                            stable: &stable,
                            source_file: &source_file_arc,
                            source_row: *pos as u64 + 1,
                        };
                        *result = Some(evaluate_record_with_window(
                            record,
                            &transform_names,
                            evaluators,
                            &ctx,
                            plan,
                            &arena,
                            &indices,
                            *pos,
                        ));
                        elapsed += start.elapsed();
                    }
                    elapsed
                }
            };

            let mut chunk_start = first_emit_pos.map_or(record_count, |p| p + 1);
            let first_emitted_was_some = first_emitted.is_some();

            if is_multi_output {
                let output_channels = spawn_writer_threads(
                    writers,
                    &output_configs,
                    Arc::clone(&final_output_schema),
                );

                // Dispatch first emitted record
                if let Some((record, emitted, metadata)) = first_emitted {
                    let route = compiled_route.as_mut().unwrap();
                    let ctx = EvalContext {
                        stable: &stable,
                        source_file: &source_file_arc,
                        source_row: first_emit_pos.unwrap() as u64 + 1,
                    };
                    match route.evaluate(&emitted, &metadata, &ctx) {
                        Ok(targets) => {
                            let mut per_output_batches: HashMap<String, Vec<Record>> =
                                HashMap::new();
                            for target in &targets {
                                let out_cfg = output_configs
                                    .iter()
                                    .find(|o| o.name == *target)
                                    .unwrap_or(primary_output);
                                let projected =
                                    project_output_with_meta(&record, &emitted, &metadata, out_cfg);
                                per_output_batches
                                    .entry(target.clone())
                                    .or_default()
                                    .push(projected);
                            }
                            flush_output_batches(&mut per_output_batches, &output_channels)?;
                        }
                        Err(route_err) => {
                            if strategy == ErrorStrategy::FailFast {
                                drop(output_channels);
                                return Err(route_err.into());
                            }
                            counters.dlq_count += 1;
                            counters.ok_count = counters.ok_count.saturating_sub(1);
                            dlq_entries.push(DlqEntry {
                                source_row: first_emit_pos.unwrap() as u64 + 1,
                                category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                error_message: route_err.to_string(),
                                original_record: record,
                                stage: Some(DlqEntry::stage_route_eval()),
                                route: None,
                                trigger: true,
                            });
                        }
                    }
                }

                // Process remaining records in chunks
                let route = compiled_route.as_mut().unwrap();
                let mut transform_dur = std::time::Duration::ZERO;
                let mut projection_timer = stage_metrics::CumulativeTimer::new();
                let mut route_timer = stage_metrics::CumulativeTimer::new();
                let mut write_timer = stage_metrics::CumulativeTimer::new();
                let mut records_emitted: u64 = if first_emitted_was_some { 1 } else { 0 };

                while chunk_start < record_count {
                    let chunk_end = (chunk_start + chunk_size).min(record_count);

                    #[allow(clippy::type_complexity)]
                    let mut chunk: Vec<(
                        u32,
                        Record,
                        Option<Result<EvalResult, (String, cxl::eval::EvalError)>>,
                    )> = (chunk_start..chunk_end)
                        .map(|pos| (pos, build_record_from_arena(pos), None))
                        .collect();

                    transform_dur += evaluate_chunk(&mut chunk, &mut evaluators);

                    let mut per_output_batches: HashMap<String, Vec<Record>> = HashMap::new();
                    for (pos, record, result) in &chunk {
                        let row_num = *pos as u64 + 1;
                        counters.total_count += 1;
                        match result.as_ref().unwrap() {
                            Ok(EvalResult::Emit {
                                fields: emitted,
                                metadata,
                            }) => {
                                let ctx = EvalContext {
                                    stable: &stable,
                                    source_file: &source_file_arc,
                                    source_row: row_num,
                                };
                                let route_result = {
                                    let _guard = route_timer.guard();
                                    route.evaluate(emitted, metadata, &ctx)
                                };
                                match route_result {
                                    Ok(targets) => {
                                        for target in &targets {
                                            let out_cfg = output_configs
                                                .iter()
                                                .find(|o| o.name == *target)
                                                .unwrap_or(primary_output);
                                            let projected = {
                                                let _guard = projection_timer.guard();
                                                project_output_with_meta(
                                                    record, emitted, metadata, out_cfg,
                                                )
                                            };
                                            per_output_batches
                                                .entry(target.clone())
                                                .or_default()
                                                .push(projected);
                                        }
                                        counters.ok_count += 1;
                                        records_emitted += 1;
                                    }
                                    Err(route_err) => {
                                        if strategy == ErrorStrategy::FailFast {
                                            drop(output_channels);
                                            return Err(route_err.into());
                                        }
                                        counters.dlq_count += 1;
                                        dlq_entries.push(DlqEntry {
                                            source_row: row_num,
                                            category:
                                                crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                            error_message: route_err.to_string(),
                                            original_record: record.clone(),
                                            stage: Some(DlqEntry::stage_route_eval()),
                                            route: None,
                                            trigger: true,
                                        });
                                    }
                                }
                            }
                            Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                counters.filtered_count += 1;
                            }
                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                counters.distinct_count += 1;
                            }
                            Err((transform_name, eval_err)) => {
                                if strategy == ErrorStrategy::FailFast {
                                    drop(output_channels);
                                    return Err(PipelineError::Eval(cxl::eval::EvalError {
                                        span: eval_err.span,
                                        kind: eval_err.kind.clone(),
                                    }));
                                }
                                counters.dlq_count += 1;
                                dlq_entries.push(DlqEntry {
                                    source_row: row_num,
                                    category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                    error_message: eval_err.to_string(),
                                    original_record: record.clone(),
                                    stage: Some(DlqEntry::stage_transform(transform_name)),
                                    route: None,
                                    trigger: true,
                                });
                            }
                        }
                    }
                    {
                        let _guard = write_timer.guard();
                        flush_output_batches(&mut per_output_batches, &output_channels)?;
                    }

                    rss_budget.observe();
                    chunk_start = chunk_end;
                }

                join_writer_threads(output_channels)?;

                collector.record(stage_metrics::StageMetrics {
                    name: stage_metrics::StageName::TransformEval,
                    elapsed: transform_dur,
                    records_in: record_count as u64,
                    records_out: records_emitted,
                    bytes_written: None,
                    rss_after: None,
                    cpu_user_delta_ns: None,
                    cpu_sys_delta_ns: None,
                    io_read_delta: None,
                    io_write_delta: None,
                    heap_delta_bytes: None,
                    heap_alloc_count: None,
                });
                collector.record(projection_timer.finish(
                    stage_metrics::StageName::Projection,
                    records_emitted,
                    records_emitted,
                ));
                collector.record(route_timer.finish(
                    stage_metrics::StageName::RouteEval,
                    records_emitted,
                    records_emitted,
                ));
                collector.record(write_timer.finish(
                    stage_metrics::StageName::Write,
                    records_emitted,
                    records_emitted,
                ));
            } else {
                // Single-output path
                let output = primary_output;
                let raw_writer = writers
                    .into_iter()
                    .next()
                    .map(|(_, w)| w)
                    .expect("validated above");
                let mut csv_writer =
                    build_format_writer(output, raw_writer, Arc::clone(&final_output_schema))?;

                // Write first emitted record
                if let Some((record, emitted, metadata)) = first_emitted {
                    let projected = project_output_with_meta(&record, &emitted, &metadata, output);
                    csv_writer.write_record(&projected)?;
                }

                let mut transform_dur = std::time::Duration::ZERO;
                let mut projection_timer = stage_metrics::CumulativeTimer::new();
                let mut write_timer = stage_metrics::CumulativeTimer::new();
                let mut records_emitted: u64 = if first_emitted_was_some { 1 } else { 0 };

                while chunk_start < record_count {
                    let chunk_end = (chunk_start + chunk_size).min(record_count);

                    #[allow(clippy::type_complexity)]
                    let mut chunk: Vec<(
                        u32,
                        Record,
                        Option<Result<EvalResult, (String, cxl::eval::EvalError)>>,
                    )> = (chunk_start..chunk_end)
                        .map(|pos| (pos, build_record_from_arena(pos), None))
                        .collect();

                    transform_dur += evaluate_chunk(&mut chunk, &mut evaluators);

                    for (pos, record, result) in &chunk {
                        let row_num = *pos as u64 + 1;
                        counters.total_count += 1;
                        match result.as_ref().unwrap() {
                            Ok(EvalResult::Emit {
                                fields: emitted, ..
                            }) => {
                                let projected = {
                                    let _guard = projection_timer.guard();
                                    project_output(record, emitted, output)
                                };
                                {
                                    let _guard = write_timer.guard();
                                    csv_writer.write_record(&projected)?;
                                }
                                counters.ok_count += 1;
                                records_emitted += 1;
                            }
                            Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                counters.filtered_count += 1;
                            }
                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                counters.distinct_count += 1;
                            }
                            Err((transform_name, eval_err)) => {
                                handle_error(
                                    strategy,
                                    record,
                                    row_num,
                                    eval_err,
                                    Some(DlqEntry::stage_transform(transform_name)),
                                    None,
                                    &mut counters,
                                    &mut dlq_entries,
                                    output,
                                    &final_output_schema,
                                    &mut *csv_writer,
                                )?;
                            }
                        }
                    }

                    rss_budget.observe();
                    chunk_start = chunk_end;
                }

                csv_writer.flush()?;

                collector.record(stage_metrics::StageMetrics {
                    name: stage_metrics::StageName::TransformEval,
                    elapsed: transform_dur,
                    records_in: record_count as u64,
                    records_out: records_emitted,
                    bytes_written: None,
                    rss_after: None,
                    cpu_user_delta_ns: None,
                    cpu_sys_delta_ns: None,
                    io_read_delta: None,
                    io_write_delta: None,
                    heap_delta_bytes: None,
                    heap_alloc_count: None,
                });
                collector.record(projection_timer.finish(
                    stage_metrics::StageName::Projection,
                    records_emitted,
                    records_emitted,
                ));
                let mut write_metrics = write_timer.finish(
                    stage_metrics::StageName::Write,
                    records_emitted,
                    records_emitted,
                );
                write_metrics.bytes_written = csv_writer.bytes_written();
                collector.record(write_metrics);
            }

            return Ok((counters, dlq_entries, rss_budget.peak_rss));
        }

        // ── Non-arena path: read all records, optionally sort ──

        let schema = format_reader.schema()?;
        let mut all_records: Vec<(Record, u64)> = Vec::new();
        let mut row_num: u64 = 0;
        while let Some(record) = format_reader.next_record()? {
            row_num += 1;
            counters.total_count += 1;
            all_records.push((record, row_num));
        }

        // D12: a global-fold aggregate (group_by: []) must still emit one
        // row over empty input, so we cannot early-return when the plan
        // contains an Aggregation node — the DAG walk needs to fire the
        // aggregator's empty-input special case.
        if all_records.is_empty() && !plan.has_branching() {
            return Ok((counters, dlq_entries, rss_bytes()));
        }

        // ── Branching DAG walk path ──
        // When the DAG has Route/Merge nodes, use per-node evaluation
        // instead of the sequential transform chain.
        if plan.has_branching() {
            return Self::execute_dag_branching(
                config,
                input,
                all_records,
                writers,
                transforms,
                compiled_route,
                plan,
                params,
                &mut counters,
                &mut dlq_entries,
                collector,
                &lookup_tables,
            );
        }

        // Legacy non-arena sort block deleted by Task 16.0.5.8.
        // Sorts now flow through the planner-synthesized `PlanNode::Sort`
        // enforcer node, dispatched in the DAG topo walk via
        // `SortBuffer<P>` (Pattern B; see
        // docs/research/RESEARCH-sort-node-sidecar-payload.md).

        // Phase 3: Schema derivation (scan for first emitting record)
        let mut evaluators = build_evaluators(transforms);
        let transform_names: Vec<&str> = transforms.iter().map(|t| t.name.as_str()).collect();

        let mut first_emitted_schema: Option<Arc<Schema>> = None;
        if requires_sorted_input {
            // For sorted path, scan once for schema then recreate evaluators
            let scan_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
            let mut records_scanned: u64 = 0;
            for (record, rn) in &all_records {
                records_scanned += 1;
                let ctx = EvalContext {
                    stable: &stable,
                    source_file: &source_file_arc,
                    source_row: *rn,
                };
                if let Ok(EvalResult::Emit {
                    fields: emitted, ..
                }) = evaluate_record(record, &transform_names, &mut evaluators, &ctx)
                {
                    let projected = project_output(record, &emitted, primary_output);
                    first_emitted_schema = Some(Arc::clone(projected.schema()));
                    break;
                }
            }
            collector.record(scan_timer.finish(
                records_scanned,
                if first_emitted_schema.is_some() { 1 } else { 0 },
            ));
            evaluators = build_evaluators(transforms);
        }

        if requires_sorted_input {
            // ── Correlated streaming path ──
            let correlation_key = config
                .error_handling
                .correlation_key
                .as_ref()
                .expect("SortedStreaming requires correlation_key");
            let max_group_buffer = config.error_handling.max_group_buffer.unwrap_or(100_000);

            let output_schema = first_emitted_schema.unwrap_or(schema);

            if is_multi_output {
                let output_channels =
                    spawn_writer_threads(writers, &output_configs, Arc::clone(&output_schema));

                let mut group_buffer: Vec<(Record, u64)> = Vec::new();
                let mut current_key: Option<Vec<GroupByKey>> = None;
                let mut per_output_batches: HashMap<String, Vec<Record>> = HashMap::new();
                let mut transform_timer = stage_metrics::CumulativeTimer::new();
                let mut projection_timer = stage_metrics::CumulativeTimer::new();
                let mut route_timer = stage_metrics::CumulativeTimer::new();
                let mut write_timer = stage_metrics::CumulativeTimer::new();
                let mut group_flush_timer = stage_metrics::CumulativeTimer::new();
                let total_records = all_records.len() as u64;
                let mut records_emitted: u64 = 0;
                let mut group_flush_records_in: u64 = 0;
                let mut group_flush_records_out: u64 = 0;

                for (record, rn) in all_records {
                    let key = extract_correlation_key(&record, correlation_key);
                    let is_null_key = key.iter().all(|k| matches!(k, GroupByKey::Null));

                    if is_null_key {
                        let ctx = EvalContext {
                            stable: &stable,
                            source_file: &source_file_arc,
                            source_row: rn,
                        };
                        let results = {
                            let _guard = transform_timer.guard();
                            evaluate_record_with_lookups(
                                &record,
                                &transform_names,
                                &mut evaluators,
                                &ctx,
                                &lookup_tables,
                            )
                        };
                        match results {
                            Ok(eval_results) => {
                                for eval_result in eval_results {
                                    match eval_result {
                                        EvalResult::Emit {
                                            fields: emitted,
                                            metadata,
                                        } => {
                                            {
                                                let _guard = route_timer.guard();
                                                let _guard2 = projection_timer.guard();
                                                Self::dispatch_to_outputs(
                                                    &record,
                                                    &emitted,
                                                    &metadata,
                                                    config,
                                                    &output_configs,
                                                    primary_output,
                                                    compiled_route.as_mut(),
                                                    &ctx,
                                                    &mut counters,
                                                    &mut dlq_entries,
                                                    &mut per_output_batches,
                                                    strategy,
                                                    rn,
                                                );
                                            }
                                            records_emitted += 1;
                                        }
                                        EvalResult::Skip(SkipReason::Filtered) => {
                                            counters.filtered_count += 1;
                                        }
                                        EvalResult::Skip(SkipReason::Duplicate) => {
                                            counters.distinct_count += 1;
                                        }
                                    }
                                }
                            }
                            Err((transform_name, eval_err)) => {
                                if strategy == ErrorStrategy::FailFast {
                                    drop(output_channels);
                                    return Err(PipelineError::Eval(cxl::eval::EvalError {
                                        span: eval_err.span,
                                        kind: eval_err.kind.clone(),
                                    }));
                                }
                                counters.dlq_count += 1;
                                dlq_entries.push(DlqEntry {
                                    source_row: rn,
                                    category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                    error_message: eval_err.to_string(),
                                    original_record: record,
                                    stage: Some(DlqEntry::stage_transform(&transform_name)),
                                    route: None,
                                    trigger: true,
                                });
                            }
                        }
                        continue;
                    }

                    if current_key.as_ref() != Some(&key) {
                        {
                            let _guard = group_flush_timer.guard();
                            group_flush_records_in += group_buffer.len() as u64;
                            let before = counters.ok_count;
                            Self::flush_correlated_group(
                                &mut group_buffer,
                                config,
                                &output_configs,
                                primary_output,
                                input,
                                pipeline_start_time,
                                &stable,
                                &transform_names,
                                &mut evaluators,
                                compiled_route.as_mut(),
                                params,
                                strategy,
                                &mut counters,
                                &mut dlq_entries,
                                &mut per_output_batches,
                                max_group_buffer,
                            );
                            group_flush_records_out += counters.ok_count - before;
                        }
                        current_key = Some(key);
                    }

                    if group_buffer.len() as u64 >= max_group_buffer {
                        group_buffer.push((record, rn));
                        for (rec, rec_rn) in group_buffer.drain(..) {
                            counters.dlq_count += 1;
                            dlq_entries.push(DlqEntry {
                                source_row: rec_rn,
                                category: crate::dlq::DlqErrorCategory::ValidationFailure,
                                error_message:
                                    "group_size_exceeded: correlation group exceeded max_group_buffer"
                                        .to_string(),
                                original_record: rec,
                                stage: Some("transform:correlated_dlq".to_string()),
                                route: None,
                                trigger: false,
                            });
                        }
                        current_key = None;
                        continue;
                    }

                    group_buffer.push((record, rn));
                }

                // Flush final group
                {
                    let _guard = group_flush_timer.guard();
                    group_flush_records_in += group_buffer.len() as u64;
                    let before = counters.ok_count;
                    Self::flush_correlated_group(
                        &mut group_buffer,
                        config,
                        &output_configs,
                        primary_output,
                        input,
                        pipeline_start_time,
                        &stable,
                        &transform_names,
                        &mut evaluators,
                        compiled_route.as_mut(),
                        params,
                        strategy,
                        &mut counters,
                        &mut dlq_entries,
                        &mut per_output_batches,
                        max_group_buffer,
                    );
                    group_flush_records_out += counters.ok_count - before;
                }

                {
                    let _guard = write_timer.guard();
                    flush_output_batches(&mut per_output_batches, &output_channels)?;
                }
                drop(output_channels);

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
                    records_emitted,
                    records_emitted,
                ));
                collector.record(write_timer.finish(
                    stage_metrics::StageName::Write,
                    records_emitted,
                    records_emitted,
                ));
                collector.record(group_flush_timer.finish(
                    stage_metrics::StageName::GroupFlush,
                    group_flush_records_in,
                    group_flush_records_out,
                ));
            } else {
                // Single-output correlated path
                let raw_writer = writers
                    .into_iter()
                    .next()
                    .map(|(_, w)| w)
                    .expect("at least one writer");

                let mut csv_writer =
                    build_format_writer(primary_output, raw_writer, Arc::clone(&output_schema))?;

                let mut group_buffer: Vec<(Record, u64)> = Vec::new();
                let mut current_key: Option<Vec<GroupByKey>> = None;
                let mut transform_timer = stage_metrics::CumulativeTimer::new();
                let mut projection_timer = stage_metrics::CumulativeTimer::new();
                let mut write_timer = stage_metrics::CumulativeTimer::new();
                let mut group_flush_timer = stage_metrics::CumulativeTimer::new();
                let total_records = all_records.len() as u64;
                let mut records_emitted: u64 = 0;
                let mut group_flush_records_in: u64 = 0;
                let mut group_flush_records_out: u64 = 0;

                for (record, rn) in all_records {
                    let key = extract_correlation_key(&record, correlation_key);
                    let is_null_key = key.iter().all(|k| matches!(k, GroupByKey::Null));

                    if is_null_key {
                        let ctx = EvalContext {
                            stable: &stable,
                            source_file: &source_file_arc,
                            source_row: rn,
                        };
                        let results = {
                            let _guard = transform_timer.guard();
                            evaluate_record_with_lookups(
                                &record,
                                &transform_names,
                                &mut evaluators,
                                &ctx,
                                &lookup_tables,
                            )
                        };
                        match results {
                            Ok(eval_results) => {
                                for eval_result in eval_results {
                                    match eval_result {
                                        EvalResult::Emit {
                                            fields: emitted, ..
                                        } => {
                                            let projected = {
                                                let _guard = projection_timer.guard();
                                                project_output(&record, &emitted, primary_output)
                                            };
                                            {
                                                let _guard = write_timer.guard();
                                                csv_writer.write_record(&projected)?;
                                            }
                                            counters.ok_count += 1;
                                            records_emitted += 1;
                                        }
                                        EvalResult::Skip(SkipReason::Filtered) => {
                                            counters.filtered_count += 1;
                                        }
                                        EvalResult::Skip(SkipReason::Duplicate) => {
                                            counters.distinct_count += 1;
                                        }
                                    }
                                }
                            }
                            Err((transform_name, eval_err)) => {
                                if strategy == ErrorStrategy::FailFast {
                                    return Err(PipelineError::Eval(cxl::eval::EvalError {
                                        span: eval_err.span,
                                        kind: eval_err.kind.clone(),
                                    }));
                                }
                                counters.dlq_count += 1;
                                dlq_entries.push(DlqEntry {
                                    source_row: rn,
                                    category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                    error_message: eval_err.to_string(),
                                    original_record: record,
                                    stage: Some(DlqEntry::stage_transform(&transform_name)),
                                    route: None,
                                    trigger: true,
                                });
                            }
                        }
                        continue;
                    }

                    if current_key.as_ref() != Some(&key) {
                        {
                            let _guard = group_flush_timer.guard();
                            group_flush_records_in += group_buffer.len() as u64;
                            let before = counters.ok_count;
                            Self::flush_correlated_group_single_output(
                                &mut group_buffer,
                                config,
                                primary_output,
                                input,
                                pipeline_start_time,
                                &stable,
                                &transform_names,
                                &mut evaluators,
                                params,
                                strategy,
                                &mut counters,
                                &mut dlq_entries,
                                &mut *csv_writer,
                                max_group_buffer,
                            )?;
                            group_flush_records_out += counters.ok_count - before;
                        }
                        current_key = Some(key);
                    }

                    if group_buffer.len() as u64 >= max_group_buffer {
                        group_buffer.push((record, rn));
                        for (rec, rec_rn) in group_buffer.drain(..) {
                            counters.dlq_count += 1;
                            dlq_entries.push(DlqEntry {
                                source_row: rec_rn,
                                category: crate::dlq::DlqErrorCategory::ValidationFailure,
                                error_message:
                                    "group_size_exceeded: correlation group exceeded max_group_buffer"
                                        .to_string(),
                                original_record: rec,
                                stage: Some("transform:correlated_dlq".to_string()),
                                route: None,
                                trigger: false,
                            });
                        }
                        current_key = None;
                        continue;
                    }

                    group_buffer.push((record, rn));
                }

                {
                    let _guard = group_flush_timer.guard();
                    group_flush_records_in += group_buffer.len() as u64;
                    let before = counters.ok_count;
                    Self::flush_correlated_group_single_output(
                        &mut group_buffer,
                        config,
                        primary_output,
                        input,
                        pipeline_start_time,
                        &stable,
                        &transform_names,
                        &mut evaluators,
                        params,
                        strategy,
                        &mut counters,
                        &mut dlq_entries,
                        &mut *csv_writer,
                        max_group_buffer,
                    )?;
                    group_flush_records_out += counters.ok_count - before;
                }

                csv_writer.flush()?;

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
                let mut write_metrics = write_timer.finish(
                    stage_metrics::StageName::Write,
                    records_emitted,
                    records_emitted,
                );
                write_metrics.bytes_written = csv_writer.bytes_written();
                collector.record(write_metrics);
                collector.record(group_flush_timer.finish(
                    stage_metrics::StageName::GroupFlush,
                    group_flush_records_in,
                    group_flush_records_out,
                ));
            }

            return Ok((counters, dlq_entries, rss_bytes()));
        }

        // ── Streaming path: read all records already done, now evaluate ──

        // Schema derivation: scan forward until a record emits
        let scan_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
        let mut records_scanned: u64 = 0;
        // First emitted result (for schema detection) plus any additional
        // fan-out results from the same record that need dispatching later.
        #[allow(clippy::type_complexity)]
        let mut first_emitted: Option<(
            Record,
            IndexMap<String, Value>,
            IndexMap<String, Value>,
        )> = None;
        // Extra fan-out results from the first emitting record (beyond the
        // first Emit used for schema detection).
        #[allow(clippy::type_complexity)]
        let mut first_record_extra_emits: Vec<(
            IndexMap<String, Value>,
            IndexMap<String, Value>,
        )> = Vec::new();
        #[allow(clippy::type_complexity)]
        let mut pending_skips_and_errors: Vec<(
            u64,
            Record,
            Result<Vec<EvalResult>, (String, cxl::eval::EvalError)>,
        )> = Vec::new();
        let mut scan_idx = 0;

        for (record, rn) in &all_records {
            scan_idx += 1;
            records_scanned += 1;
            let ctx = EvalContext {
                stable: &stable,
                source_file: &source_file_arc,
                source_row: *rn,
            };
            let result = evaluate_record_with_lookups(
                record,
                &transform_names,
                &mut evaluators,
                &ctx,
                &lookup_tables,
            );
            match &result {
                Ok(eval_results) => {
                    let mut found_first = false;
                    for eval_result in eval_results {
                        if let EvalResult::Emit {
                            fields: emitted,
                            metadata,
                        } = eval_result
                        {
                            if !found_first {
                                counters.ok_count += 1;
                                first_emitted =
                                    Some((record.clone(), emitted.clone(), metadata.clone()));
                                found_first = true;
                            } else {
                                counters.ok_count += 1;
                                first_record_extra_emits.push((emitted.clone(), metadata.clone()));
                            }
                        }
                        // Skip counting is deferred to pending_skips_and_errors processing
                    }
                    if found_first {
                        break;
                    }
                    // All results are Skips — defer (counters updated in pending processing)
                    pending_skips_and_errors.push((*rn, record.clone(), result));
                }
                Err(_) => {
                    pending_skips_and_errors.push((*rn, record.clone(), result));
                }
            }
        }

        collector.record(
            scan_timer.finish(records_scanned, if first_emitted.is_some() { 1 } else { 0 }),
        );

        let output_schema = if let Some((ref rec, ref emitted, ref metadata)) = first_emitted {
            let projected = project_output_with_meta(rec, emitted, metadata, primary_output);
            Arc::clone(projected.schema())
        } else {
            schema
        };

        if is_multi_output {
            // Multi-output streaming path
            let output_channels =
                spawn_writer_threads(writers, &output_configs, Arc::clone(&output_schema));

            // Process pending skips/errors
            for (_rn, _record, result) in &pending_skips_and_errors {
                match result {
                    Ok(eval_results) => {
                        for eval_result in eval_results {
                            match eval_result {
                                EvalResult::Skip(SkipReason::Filtered) => {
                                    counters.filtered_count += 1;
                                }
                                EvalResult::Skip(SkipReason::Duplicate) => {
                                    counters.distinct_count += 1;
                                }
                                EvalResult::Emit { .. } => {} // emits handled in scan
                            }
                        }
                    }
                    Err((transform_name, eval_err)) => {
                        if strategy == ErrorStrategy::FailFast {
                            drop(output_channels);
                            return Err(PipelineError::Eval(cxl::eval::EvalError {
                                span: eval_err.span,
                                kind: eval_err.kind.clone(),
                            }));
                        }
                        counters.dlq_count += 1;
                        dlq_entries.push(DlqEntry {
                            source_row: _rn.to_owned(),
                            category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                            error_message: eval_err.to_string(),
                            original_record: _record.clone(),
                            stage: Some(DlqEntry::stage_transform(transform_name)),
                            route: None,
                            trigger: true,
                        });
                    }
                }
            }

            // Dispatch first emitted record + extra fan-out results
            let first_emitted_was_some = first_emitted.is_some();
            if let Some((ref record, ref emitted, ref metadata)) = first_emitted {
                let route = compiled_route.as_mut().unwrap();
                let ctx = EvalContext {
                    stable: &stable,
                    source_file: &source_file_arc,
                    source_row: 1,
                };
                // Dispatch first result + all extra fan-out results
                #[allow(clippy::type_complexity)]
                let all_emits: Vec<(
                    &IndexMap<String, Value>,
                    &IndexMap<String, Value>,
                )> = std::iter::once((emitted, metadata))
                    .chain(first_record_extra_emits.iter().map(|(e, m)| (e, m)))
                    .collect();
                let mut per_output_batches: HashMap<String, Vec<Record>> = HashMap::new();
                for (emit, meta) in all_emits {
                    match route.evaluate(emit, meta, &ctx) {
                        Ok(targets) => {
                            for target in &targets {
                                let out_cfg = output_configs
                                    .iter()
                                    .find(|o| o.name == *target)
                                    .unwrap_or(primary_output);
                                let projected =
                                    project_output_with_meta(record, emit, meta, out_cfg);
                                per_output_batches
                                    .entry(target.clone())
                                    .or_default()
                                    .push(projected);
                            }
                        }
                        Err(route_err) => {
                            if strategy == ErrorStrategy::FailFast {
                                drop(output_channels);
                                return Err(route_err.into());
                            }
                            counters.dlq_count += 1;
                            counters.ok_count = counters.ok_count.saturating_sub(1);
                            dlq_entries.push(DlqEntry {
                                source_row: 1,
                                category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                error_message: route_err.to_string(),
                                original_record: record.clone(),
                                stage: Some(DlqEntry::stage_route_eval()),
                                route: None,
                                trigger: true,
                            });
                        }
                    }
                }
                flush_output_batches(&mut per_output_batches, &output_channels)?;
            }

            // Process remaining records
            let route = compiled_route.as_mut().unwrap();
            let mut per_output_batches: HashMap<String, Vec<Record>> = HashMap::new();
            let mut transform_timer = stage_metrics::CumulativeTimer::new();
            let mut projection_timer = stage_metrics::CumulativeTimer::new();
            let mut route_timer = stage_metrics::CumulativeTimer::new();
            let mut write_timer = stage_metrics::CumulativeTimer::new();
            let mut records_emitted: u64 = if first_emitted_was_some {
                1 + first_record_extra_emits.len() as u64
            } else {
                0
            };
            let remaining_count = all_records.len().saturating_sub(scan_idx) as u64;

            for (record, rn) in all_records.into_iter().skip(scan_idx) {
                let ctx = EvalContext {
                    stable: &stable,
                    source_file: &source_file_arc,
                    source_row: rn,
                };
                let results = {
                    let _guard = transform_timer.guard();
                    evaluate_record_with_lookups(
                        &record,
                        &transform_names,
                        &mut evaluators,
                        &ctx,
                        &lookup_tables,
                    )
                };
                match results {
                    Ok(eval_results) => {
                        for eval_result in eval_results {
                            match eval_result {
                                EvalResult::Emit {
                                    fields: emitted,
                                    metadata,
                                } => {
                                    let route_result = {
                                        let _guard = route_timer.guard();
                                        route.evaluate(&emitted, &metadata, &ctx)
                                    };
                                    match route_result {
                                        Ok(targets) => {
                                            for target in &targets {
                                                let out_cfg = output_configs
                                                    .iter()
                                                    .find(|o| o.name == *target)
                                                    .unwrap_or(primary_output);
                                                let projected = {
                                                    let _guard = projection_timer.guard();
                                                    project_output_with_meta(
                                                        &record, &emitted, &metadata, out_cfg,
                                                    )
                                                };
                                                per_output_batches
                                                    .entry(target.clone())
                                                    .or_default()
                                                    .push(projected);
                                            }
                                            counters.ok_count += 1;
                                            records_emitted += 1;
                                        }
                                        Err(route_err) => {
                                            if strategy == ErrorStrategy::FailFast {
                                                drop(output_channels);
                                                return Err(route_err.into());
                                            }
                                            counters.dlq_count += 1;
                                            dlq_entries.push(DlqEntry {
                                                source_row: rn,
                                                category:
                                                    crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                                error_message: route_err.to_string(),
                                                original_record: record.clone(),
                                                stage: Some(DlqEntry::stage_route_eval()),
                                                route: None,
                                                trigger: true,
                                            });
                                        }
                                    }
                                }
                                EvalResult::Skip(SkipReason::Filtered) => {
                                    counters.filtered_count += 1;
                                }
                                EvalResult::Skip(SkipReason::Duplicate) => {
                                    counters.distinct_count += 1;
                                }
                            }
                        }
                    }
                    Err((transform_name, eval_err)) => {
                        if strategy == ErrorStrategy::FailFast {
                            drop(output_channels);
                            return Err(eval_err.into());
                        }
                        counters.dlq_count += 1;
                        dlq_entries.push(DlqEntry {
                            source_row: rn,
                            category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                            error_message: eval_err.to_string(),
                            original_record: record,
                            stage: Some(DlqEntry::stage_transform(&transform_name)),
                            route: None,
                            trigger: true,
                        });
                    }
                }

                {
                    let _guard = write_timer.guard();
                    flush_output_batches(&mut per_output_batches, &output_channels)?;
                }
            }

            // Final flush
            {
                let _guard = write_timer.guard();
                flush_output_batches(&mut per_output_batches, &output_channels)?;
            }
            join_writer_threads(output_channels)?;

            let total_records = records_scanned + remaining_count;
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
                records_emitted,
                records_emitted,
            ));
            collector.record(write_timer.finish(
                stage_metrics::StageName::Write,
                records_emitted,
                records_emitted,
            ));
        } else {
            // Single-output streaming path
            let output = primary_output;
            let raw_writer = writers
                .into_iter()
                .next()
                .map(|(_, w)| w)
                .expect("validated above");

            let mut csv_writer =
                build_format_writer(output, raw_writer, Arc::clone(&output_schema))?;

            // Process pending skips and errors from scan-forward phase
            for (rn, record, result) in pending_skips_and_errors {
                match result {
                    Ok(eval_results) => {
                        for eval_result in eval_results {
                            match eval_result {
                                EvalResult::Skip(SkipReason::Filtered) => {
                                    counters.filtered_count += 1;
                                }
                                EvalResult::Skip(SkipReason::Duplicate) => {
                                    counters.distinct_count += 1;
                                }
                                EvalResult::Emit { .. } => {} // emits handled in scan
                            }
                        }
                    }
                    Err((transform_name, eval_err)) => {
                        handle_error(
                            strategy,
                            &record,
                            rn,
                            &eval_err,
                            Some(DlqEntry::stage_transform(&transform_name)),
                            None,
                            &mut counters,
                            &mut dlq_entries,
                            output,
                            &output_schema,
                            &mut *csv_writer,
                        )?;
                    }
                }
            }

            // Write first emitted record (ok_count already incremented during scan)
            let first_emitted_was_some = first_emitted.is_some();
            if let Some((ref record, ref emitted, ref metadata)) = first_emitted {
                let projected = project_output_with_meta(record, emitted, metadata, output);
                csv_writer.write_record(&projected)?;
                // Write extra fan-out results from the same first record
                for (extra_emitted, extra_metadata) in &first_record_extra_emits {
                    let projected =
                        project_output_with_meta(record, extra_emitted, extra_metadata, output);
                    csv_writer.write_record(&projected)?;
                }
            }

            // Process remaining records
            let mut transform_timer = stage_metrics::CumulativeTimer::new();
            let mut projection_timer = stage_metrics::CumulativeTimer::new();
            let mut write_timer = stage_metrics::CumulativeTimer::new();
            let mut records_emitted: u64 = if first_emitted_was_some {
                1 + first_record_extra_emits.len() as u64
            } else {
                0
            };
            let remaining_count = all_records.len().saturating_sub(scan_idx) as u64;

            for (record, rn) in all_records.into_iter().skip(scan_idx) {
                let ctx = EvalContext {
                    stable: &stable,
                    source_file: &source_file_arc,
                    source_row: rn,
                };
                let results = {
                    let _guard = transform_timer.guard();
                    evaluate_record_with_lookups(
                        &record,
                        &transform_names,
                        &mut evaluators,
                        &ctx,
                        &lookup_tables,
                    )
                };
                match results {
                    Ok(eval_results) => {
                        for eval_result in eval_results {
                            match eval_result {
                                EvalResult::Emit {
                                    fields: emitted,
                                    metadata,
                                } => {
                                    let projected = {
                                        let _guard = projection_timer.guard();
                                        project_output_with_meta(
                                            &record, &emitted, &metadata, output,
                                        )
                                    };
                                    {
                                        let _guard = write_timer.guard();
                                        csv_writer.write_record(&projected)?;
                                    }
                                    counters.ok_count += 1;
                                    records_emitted += 1;
                                }
                                EvalResult::Skip(SkipReason::Filtered) => {
                                    counters.filtered_count += 1;
                                }
                                EvalResult::Skip(SkipReason::Duplicate) => {
                                    counters.distinct_count += 1;
                                }
                            }
                        }
                    }
                    Err((transform_name, eval_err)) => {
                        handle_error(
                            strategy,
                            &record,
                            rn,
                            &eval_err,
                            Some(DlqEntry::stage_transform(&transform_name)),
                            None,
                            &mut counters,
                            &mut dlq_entries,
                            output,
                            &output_schema,
                            &mut *csv_writer,
                        )?;
                    }
                }
            }

            csv_writer.flush()?;

            let total_records = records_scanned + remaining_count;
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
            let mut write_metrics = write_timer.finish(
                stage_metrics::StageName::Write,
                records_emitted,
                records_emitted,
            );
            write_metrics.bytes_written = csv_writer.bytes_written();
            collector.record(write_metrics);
        }

        Ok((counters, dlq_entries, rss_bytes()))
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
    fn execute_dag_branching(
        config: &PipelineConfig,
        input: &crate::config::SourceConfig,
        all_records: Vec<(Record, u64)>,
        mut writers: HashMap<String, Box<dyn Write + Send>>,
        transforms: &[CompiledTransform],
        compiled_route: Option<CompiledRoute>,
        plan: &ExecutionPlanDag,
        params: &PipelineRunParams,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        collector: &mut stage_metrics::StageCollector,
        lookup_tables: &HashMap<String, RuntimeLookup>,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, Option<u64>), PipelineError> {
        use petgraph::graph::NodeIndex;

        let output_configs: Vec<_> = config.output_configs().cloned().collect();
        let primary_output = &output_configs[0];
        let pipeline_start_time = chrono::Local::now().naive_local();

        // Pipeline-stable evaluation context (D59 / Task 16.3.13a). Built once
        // here, reused (via borrow) at every per-record dispatch site below.
        let stable = build_stable_eval_context(
            config,
            pipeline_start_time,
            &params.execution_id,
            &params.batch_id,
            &params.pipeline_vars,
        );
        let source_file_arc: Arc<str> = Arc::from(input.path.as_str());

        let strategy = config.error_handling.strategy;
        let mut compiled_route = compiled_route;
        let mut transform_timer = stage_metrics::CumulativeTimer::new();
        let mut route_timer = stage_metrics::CumulativeTimer::new();
        let mut projection_timer = stage_metrics::CumulativeTimer::new();
        let mut write_timer = stage_metrics::CumulativeTimer::new();
        let total_records = all_records.len() as u64;
        let mut records_emitted: u64 = 0;

        // Build transform name -> index map for looking up CompiledTransform by name
        let transform_by_name: HashMap<&str, usize> = transforms
            .iter()
            .enumerate()
            .map(|(i, t)| (t.name.as_str(), i))
            .collect();

        // Inter-node buffers: each node produces records into its buffer.
        // Records are (Record, row_number, accumulated_emitted, accumulated_metadata).
        #[allow(clippy::type_complexity)]
        let mut node_buffers: HashMap<
            NodeIndex,
            Vec<(
                Record,
                u64,
                IndexMap<String, Value>,
                IndexMap<String, Value>,
            )>,
        > = HashMap::new();

        // Walk DAG in topological order
        for &node_idx in &plan.topo_order {
            let node = plan.graph[node_idx].clone();
            match node {
                PlanNode::Source { .. } => {
                    // Source node: populate buffer with all input records
                    let records: Vec<_> = all_records
                        .iter()
                        .map(|(r, rn)| (r.clone(), *rn, IndexMap::new(), IndexMap::new()))
                        .collect();
                    node_buffers.insert(node_idx, records);
                }

                PlanNode::Transform { ref name, .. } => {
                    // Get input records: first check own buffer (set by Route
                    // node for branch dispatch), then fall back to predecessor.
                    let input_records = if let Some(own_buf) = node_buffers.remove(&node_idx) {
                        own_buf
                    } else {
                        let predecessors: Vec<NodeIndex> = plan
                            .graph
                            .neighbors_directed(node_idx, Direction::Incoming)
                            .collect();
                        if predecessors.len() == 1 {
                            node_buffers.remove(&predecessors[0]).unwrap_or_default()
                        } else {
                            predecessors
                                .iter()
                                .find_map(|p| node_buffers.remove(p))
                                .unwrap_or_default()
                        }
                    };

                    // Find the CompiledTransform for this node
                    let transform_idx = match transform_by_name.get(name.as_str()) {
                        Some(&idx) => idx,
                        None => {
                            // No transform found — pass through
                            node_buffers.insert(node_idx, input_records);
                            continue;
                        }
                    };

                    let mut evaluator = ProgramEvaluator::new(
                        Arc::clone(&transforms[transform_idx].typed),
                        transforms[transform_idx].has_distinct(),
                    );

                    let mut output_records = Vec::with_capacity(input_records.len());

                    for (record, rn, mut all_emitted, mut all_metadata) in input_records {
                        let ctx = EvalContext {
                            stable: &stable,
                            source_file: &source_file_arc,
                            source_row: rn,
                        };

                        // Check if this transform has a lookup table
                        if let Some(rt_lookup) = lookup_tables.get(name.as_str()) {
                            let _guard = transform_timer.guard();
                            let matches = crate::pipeline::lookup::find_matches(
                                &rt_lookup.table,
                                &record,
                                &mut rt_lookup
                                    .where_evaluator
                                    .lock()
                                    .expect("lookup evaluator lock"),
                                &ctx,
                                rt_lookup.match_mode,
                                rt_lookup.equality_index.as_ref(),
                            )
                            .map_err(|e| {
                                PipelineError::Compilation {
                                    transform_name: name.clone(),
                                    messages: vec![e.to_string()],
                                }
                            })?;

                            if matches.is_empty() {
                                match rt_lookup.on_miss {
                                    crate::config::pipeline_node::OnMiss::Skip => {
                                        counters.filtered_count += 1;
                                        continue;
                                    }
                                    crate::config::pipeline_node::OnMiss::Error => {
                                        return Err(PipelineError::Compilation {
                                            transform_name: name.clone(),
                                            messages: vec![format!(
                                                "no matching row in lookup source '{}'",
                                                rt_lookup.table.source_name()
                                            )],
                                        });
                                    }
                                    crate::config::pipeline_node::OnMiss::NullFields => {
                                        let resolver =
                                            crate::pipeline::lookup::LookupResolver::no_match(
                                                &record,
                                                rt_lookup.table.source_name(),
                                            );
                                        match evaluator
                                            .eval_record::<NullStorage>(&ctx, &resolver, None)
                                            .map_err(|e| (name.clone(), e))
                                        {
                                            Ok(EvalResult::Emit {
                                                fields: emitted,
                                                metadata,
                                            }) => {
                                                let mut rec = record.clone();
                                                for (n, v) in &emitted {
                                                    if !rec.set(n, v.clone()) {
                                                        rec.set_overflow(
                                                            n.clone().into_boxed_str(),
                                                            v.clone(),
                                                        );
                                                    }
                                                }
                                                for (k, v) in &metadata {
                                                    let _ = rec.set_meta(k, v.clone());
                                                }
                                                all_emitted.extend(emitted);
                                                all_metadata.extend(metadata);
                                                output_records.push((
                                                    rec,
                                                    rn,
                                                    all_emitted,
                                                    all_metadata,
                                                ));
                                            }
                                            Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                                counters.filtered_count += 1;
                                            }
                                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                                counters.distinct_count += 1;
                                            }
                                            Err((tn, eval_err)) => {
                                                if strategy == ErrorStrategy::FailFast {
                                                    return Err(eval_err.into());
                                                }
                                                counters.dlq_count += 1;
                                                dlq_entries.push(DlqEntry {
                                                    source_row: rn,
                                                    category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                                    error_message: eval_err.to_string(),
                                                    original_record: record,
                                                    stage: Some(DlqEntry::stage_transform(&tn)),
                                                    route: None,
                                                    trigger: true,
                                                });
                                            }
                                        }
                                    }
                                }
                            } else {
                                // Emit one output record per match
                                for &idx in &matches {
                                    let matched_row = &rt_lookup.table.records()[idx];
                                    let resolver = crate::pipeline::lookup::LookupResolver::matched(
                                        &record,
                                        rt_lookup.table.source_name(),
                                        matched_row,
                                    );
                                    match evaluator
                                        .eval_record::<NullStorage>(&ctx, &resolver, None)
                                        .map_err(|e| (name.clone(), e))
                                    {
                                        Ok(EvalResult::Emit {
                                            fields: emitted,
                                            metadata,
                                        }) => {
                                            let mut rec = record.clone();
                                            for (n, v) in &emitted {
                                                if !rec.set(n, v.clone()) {
                                                    rec.set_overflow(
                                                        n.clone().into_boxed_str(),
                                                        v.clone(),
                                                    );
                                                }
                                            }
                                            for (k, v) in &metadata {
                                                let _ = rec.set_meta(k, v.clone());
                                            }
                                            let mut em = all_emitted.clone();
                                            let mut mt = all_metadata.clone();
                                            em.extend(emitted);
                                            mt.extend(metadata);
                                            output_records.push((rec, rn, em, mt));
                                        }
                                        Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                            counters.filtered_count += 1;
                                        }
                                        Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                            counters.distinct_count += 1;
                                        }
                                        Err((tn, eval_err)) => {
                                            if strategy == ErrorStrategy::FailFast {
                                                return Err(eval_err.into());
                                            }
                                            counters.dlq_count += 1;
                                            dlq_entries.push(DlqEntry {
                                                source_row: rn,
                                                category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                                error_message: eval_err.to_string(),
                                                original_record: record.clone(),
                                                stage: Some(DlqEntry::stage_transform(&tn)),
                                                route: None,
                                                trigger: true,
                                            });
                                        }
                                    }
                                }
                            }
                        } else {
                            // No lookup — existing single-transform evaluation
                            let eval_result = {
                                let _guard = transform_timer.guard();
                                evaluate_single_transform(&record, name, &mut evaluator, &ctx)
                            };
                            match eval_result {
                                Ok((modified_record, Ok((emitted, metadata)))) => {
                                    all_emitted.extend(emitted);
                                    all_metadata.extend(metadata);
                                    output_records.push((
                                        modified_record,
                                        rn,
                                        all_emitted,
                                        all_metadata,
                                    ));
                                }
                                Ok((_record, Err(SkipReason::Filtered))) => {
                                    counters.filtered_count += 1;
                                }
                                Ok((_record, Err(SkipReason::Duplicate))) => {
                                    counters.distinct_count += 1;
                                }
                                Err((transform_name, eval_err)) => {
                                    if strategy == ErrorStrategy::FailFast {
                                        return Err(eval_err.into());
                                    }
                                    counters.dlq_count += 1;
                                    dlq_entries.push(DlqEntry {
                                        source_row: rn,
                                        category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                        error_message: eval_err.to_string(),
                                        original_record: record,
                                        stage: Some(DlqEntry::stage_transform(&transform_name)),
                                        route: None,
                                        trigger: true,
                                    });
                                }
                            }
                        }
                    }

                    node_buffers.insert(node_idx, output_records);
                }

                PlanNode::Route {
                    ref name,
                    mode,
                    branches: _,
                    default: _,
                    ..
                } => {
                    // Get input records from predecessor
                    let predecessors: Vec<NodeIndex> = plan
                        .graph
                        .neighbors_directed(node_idx, Direction::Incoming)
                        .collect();
                    let input_records = predecessors
                        .iter()
                        .find_map(|p| node_buffers.remove(p))
                        .unwrap_or_default();

                    // Get successor nodes (branch transform nodes)
                    let successors: Vec<NodeIndex> = plan
                        .graph
                        .neighbors_directed(node_idx, Direction::Outgoing)
                        .collect();

                    // Build branch_name -> successor NodeIndex mapping.
                    // A successor transform's `input:` field is "route_parent.branch_name".
                    // Extract the route parent name from this Route node's name
                    // (which is "route_{parent_transform}").
                    let route_parent = name.strip_prefix("route_").unwrap_or(name);
                    let mut branch_to_succ: HashMap<String, NodeIndex> = HashMap::new();
                    for &succ in &successors {
                        let succ_name = plan.graph[succ].name();
                        // Check config transforms for matching input reference
                        for tc in crate::executor::build_transform_specs(config) {
                            if tc.name == succ_name
                                && let Some(crate::config::TransformInput::Single(ref input_ref)) =
                                    tc.input
                            {
                                // input_ref is "parent.branch" or just "parent"
                                if let Some(branch) =
                                    input_ref.strip_prefix(&format!("{}.", route_parent))
                                {
                                    branch_to_succ.insert(branch.to_string(), succ);
                                }
                            }
                        }
                    }

                    // Initialize per-successor buffers
                    let mut branch_buffers: HashMap<NodeIndex, Vec<_>> = HashMap::new();
                    for &succ in &successors {
                        branch_buffers.insert(succ, Vec::new());
                    }

                    // Use compiled_route to evaluate conditions
                    if let Some(ref mut route) = compiled_route {
                        for (record, rn, all_emitted, all_metadata) in input_records {
                            let ctx = EvalContext {
                                stable: &stable,
                                source_file: &source_file_arc,
                                source_row: rn,
                            };

                            let route_result = {
                                let _guard = route_timer.guard();
                                route.evaluate(&all_emitted, &all_metadata, &ctx)
                            };
                            match route_result {
                                Ok(targets) => {
                                    for target in &targets {
                                        if let Some(&succ) = branch_to_succ.get(target.as_str()) {
                                            branch_buffers.entry(succ).or_default().push((
                                                record.clone(),
                                                rn,
                                                all_emitted.clone(),
                                                all_metadata.clone(),
                                            ));
                                        }
                                        // Exclusive mode: stop after first match
                                        if mode == crate::config::RouteMode::Exclusive {
                                            break;
                                        }
                                    }
                                }
                                Err(route_err) => {
                                    if strategy == ErrorStrategy::FailFast {
                                        return Err(route_err.into());
                                    }
                                    counters.dlq_count += 1;
                                    dlq_entries.push(DlqEntry {
                                        source_row: rn,
                                        category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                        error_message: route_err.to_string(),
                                        original_record: record,
                                        stage: Some(DlqEntry::stage_route_eval()),
                                        route: None,
                                        trigger: true,
                                    });
                                }
                            }
                        }
                    }

                    // Put branch buffers into node_buffers keyed by successor
                    for (succ_idx, buf) in branch_buffers {
                        node_buffers.insert(succ_idx, buf);
                    }
                }

                PlanNode::Merge { ref name, .. } => {
                    // Concatenate predecessor buffers in declaration order.
                    // Declaration order = order in the `input:` array of the
                    // merge's downstream transform.
                    let predecessors: Vec<NodeIndex> = plan
                        .graph
                        .neighbors_directed(node_idx, Direction::Incoming)
                        .collect();

                    // The Merge node is named "merge_{transform}". Find the
                    // transform's input: array to determine declaration order.
                    let merge_transform_name = name.strip_prefix("merge_").unwrap_or(name);
                    let declaration_order: Vec<String> =
                        crate::executor::build_transform_specs(config)
                            .into_iter()
                            .find(|tc| tc.name == merge_transform_name)
                            .and_then(|tc| {
                                if let Some(crate::config::TransformInput::Multiple(ref inputs)) =
                                    tc.input
                                {
                                    Some(inputs.clone())
                                } else {
                                    None
                                }
                            })
                            .unwrap_or_default();

                    // Sort predecessors by declaration order
                    let mut sorted_preds = predecessors.clone();
                    sorted_preds.sort_by_key(|p| {
                        let pred_name = plan.graph[*p].name();
                        declaration_order
                            .iter()
                            .position(|d| d == pred_name)
                            .unwrap_or(usize::MAX)
                    });

                    let total: usize = sorted_preds
                        .iter()
                        .map(|p| node_buffers.get(p).map_or(0, |b| b.len()))
                        .sum();
                    let mut merged = Vec::with_capacity(total);
                    for pred in &sorted_preds {
                        if let Some(buf) = node_buffers.remove(pred) {
                            merged.extend(buf);
                        }
                    }
                    node_buffers.insert(node_idx, merged);
                }

                PlanNode::Sort {
                    ref name,
                    ref sort_fields,
                    ..
                } => {
                    // Enforcer-sort dispatch (Task 16.0.5.8). Reuses the
                    // generalized `SortBuffer<P>` carrying per-record sidecar
                    // (row_num + emitted/accumulated metadata maps) through
                    // the sort permutation. See
                    // docs/research/RESEARCH-sort-node-sidecar-payload.md.
                    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};

                    type SortPayload = (u64, IndexMap<String, Value>, IndexMap<String, Value>);
                    type SortRow = (
                        Record,
                        u64,
                        IndexMap<String, Value>,
                        IndexMap<String, Value>,
                    );

                    let predecessors: Vec<NodeIndex> = plan
                        .graph
                        .neighbors_directed(node_idx, Direction::Incoming)
                        .collect();
                    let input_records: Vec<_> = predecessors
                        .iter()
                        .find_map(|p| node_buffers.remove(p))
                        .unwrap_or_default();

                    if input_records.is_empty() {
                        node_buffers.insert(node_idx, Vec::new());
                        continue;
                    }

                    let schema = input_records[0].0.schema().clone();
                    let memory_limit = parse_memory_limit(config);
                    let mut buf: SortBuffer<SortPayload> =
                        SortBuffer::new(sort_fields.clone(), memory_limit, None, schema);

                    let sort_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
                    let sort_count = input_records.len() as u64;
                    for (record, row_num, emitted, accumulated) in input_records {
                        buf.push(record, (row_num, emitted, accumulated));
                        if buf.should_spill() {
                            buf.sort_and_spill().map_err(|e| {
                                PipelineError::Io(std::io::Error::other(format!(
                                    "sort enforcer '{name}' spill failed: {e}"
                                )))
                            })?;
                        }
                    }

                    let sorted = buf.finish().map_err(|e| {
                        PipelineError::Io(std::io::Error::other(format!(
                            "sort enforcer '{name}' finish failed: {e}"
                        )))
                    })?;

                    let mut out: Vec<SortRow> = Vec::with_capacity(sort_count as usize);
                    match sorted {
                        SortedOutput::InMemory(pairs) => {
                            for (record, (row_num, emitted, accumulated)) in pairs {
                                out.push((record, row_num, emitted, accumulated));
                            }
                        }
                        SortedOutput::Spilled(files) => {
                            // Spill files are individually sorted but not
                            // globally merged. For correctness when multiple
                            // spill files exist, a k-way merge is required.
                            // Single-file fast path is exact.
                            if files.len() > 1 {
                                return Err(PipelineError::Io(std::io::Error::other(format!(
                                    "sort enforcer '{name}' produced {} spill files; \
                                     k-way merge for enforcer sort is not yet implemented \
                                     (memory_limit too small for input)",
                                    files.len()
                                ))));
                            }
                            for file in files {
                                let reader = file.reader().map_err(|e| {
                                    PipelineError::Io(std::io::Error::other(format!(
                                        "sort enforcer '{name}' spill read failed: {e}"
                                    )))
                                })?;
                                for entry in reader {
                                    let (record, (row_num, emitted, accumulated)) =
                                        entry.map_err(|e| {
                                            PipelineError::Io(std::io::Error::other(format!(
                                                "sort enforcer '{name}' spill decode failed: {e}"
                                            )))
                                        })?;
                                    out.push((record, row_num, emitted, accumulated));
                                }
                            }
                        }
                    }
                    collector.record(sort_timer.finish(sort_count, sort_count));
                    node_buffers.insert(node_idx, out);
                }

                PlanNode::Aggregation {
                    ref name,
                    ref compiled,
                    strategy: agg_strategy,
                    ref output_schema,
                    ..
                } => {
                    // Task 16.3.13 — hash-aggregation dispatch arm.
                    //
                    // DataFusion PR #9241 / #12086 lesson: any
                    // `PipelineError::Internal` raised here (e.g. via
                    // `single_predecessor`, or via the wrapper enum's
                    // Streaming-not-yet-implemented arm) MUST hard-abort
                    // regardless of `error_strategy`. We achieve this
                    // structurally by propagating those errors via `?`
                    // before reaching the per-record FailFast/Continue
                    // match — internal invariants always abort.
                    use crate::aggregation::{
                        AggregateStream, HashAggError, SortRow as AggSortRow,
                    };

                    let pred = single_predecessor(plan, node_idx, "aggregation", name)?;
                    let input = node_buffers.remove(&pred).unwrap_or_default();

                    // Build the per-aggregation runtime artifacts. The
                    // executor owns the evaluator + spill metadata; the
                    // wrapper enum owns the engine.
                    let transform_idx = transform_by_name.get(name.as_str()).copied();
                    let evaluator = match transform_idx {
                        Some(idx) => ProgramEvaluator::new(
                            Arc::clone(&transforms[idx].typed),
                            transforms[idx].has_distinct(),
                        ),
                        None => {
                            return Err(PipelineError::Internal {
                                op: "aggregation",
                                node: name.clone(),
                                detail: "no compiled transform found for aggregate node"
                                    .to_string(),
                            });
                        }
                    };

                    // Spill schema follows the format used by
                    // `HashAggregator::spill`: group-by columns ++
                    // `__acc_state` ++ `__meta_tracker`.
                    let mut spill_columns: Vec<Box<str>> = compiled
                        .group_by_fields
                        .iter()
                        .map(|s| Box::<str>::from(s.as_str()))
                        .collect();
                    spill_columns.push(Box::<str>::from("__acc_state"));
                    spill_columns.push(Box::<str>::from("__meta_tracker"));
                    let spill_schema = Arc::new(Schema::new(spill_columns));

                    let memory_limit = parse_memory_limit(config);

                    let mut stream = AggregateStream::for_node(
                        Arc::clone(compiled),
                        evaluator,
                        agg_strategy,
                        Arc::clone(output_schema),
                        spill_schema,
                        memory_limit,
                        None,
                        name.clone(),
                    )?;

                    let agg_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
                    let input_count = input.len() as u64;

                    let mut emitted_rows: Vec<AggSortRow> = Vec::with_capacity(64);
                    for (record, row_num, emitted, accumulated) in &input {
                        let ctx = EvalContext {
                            stable: &stable,
                            source_file: &source_file_arc,
                            source_row: *row_num,
                        };
                        if let Err(e) = stream.add_record(
                            record,
                            *row_num,
                            emitted,
                            accumulated,
                            &ctx,
                            &mut emitted_rows,
                        ) {
                            match config.error_handling.strategy {
                                ErrorStrategy::FailFast => return Err(e.into()),
                                ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                                    counters.dlq_count += 1;
                                    dlq_entries.push(DlqEntry {
                                        source_row: *row_num,
                                        category: crate::dlq::DlqErrorCategory::AggregateFinalize,
                                        error_message: format!("aggregate {name}: {e}"),
                                        original_record: record.clone(),
                                        stage: Some(crate::dlq::stage_aggregate(name)),
                                        route: None,
                                        trigger: true,
                                    });
                                }
                            }
                        }
                    }

                    // Finalize. Accumulator finalize errors get the
                    // typed `PipelineError::Accumulator` mapping; under
                    // `Continue` we route to the DLQ and emit zero rows
                    // for the failed group. All other engine errors
                    // propagate (Internal/Spill/Residual always abort).
                    let ctx = EvalContext {
                        stable: &stable,
                        source_file: &source_file_arc,
                        source_row: 0,
                    };
                    let out_rows: Vec<AggSortRow> = match stream.finalize(&ctx, &mut emitted_rows) {
                        Ok(()) => emitted_rows,
                        Err(HashAggError::Accumulator {
                            transform,
                            binding,
                            source,
                        }) => match config.error_handling.strategy {
                            ErrorStrategy::FailFast => {
                                return Err(PipelineError::Accumulator {
                                    transform,
                                    binding,
                                    source,
                                });
                            }
                            ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                                counters.dlq_count += 1;
                                let synthetic = if let Some((rec, _, _, _)) = input.first() {
                                    rec.clone()
                                } else {
                                    Record::new(Arc::clone(output_schema), Vec::new())
                                };
                                dlq_entries.push(DlqEntry {
                                    source_row: 0,
                                    category: crate::dlq::DlqErrorCategory::AggregateFinalize,
                                    error_message: format!(
                                        "aggregate {transform}.{binding}: {source:?}"
                                    ),
                                    original_record: synthetic,
                                    stage: Some(crate::dlq::stage_aggregate(name)),
                                    route: None,
                                    trigger: true,
                                });
                                Vec::new()
                            }
                        },
                        Err(other) => return Err(other.into()),
                    };

                    collector.record(agg_timer.finish(input_count, out_rows.len() as u64));
                    node_buffers.insert(node_idx, out_rows);
                }

                PlanNode::Output { ref name, .. } => {
                    // Get input records: check own buffer first (Route
                    // nodes store records at the successor's index), then
                    // fall back to predecessor buffers.
                    let input_records = if let Some(own_buf) = node_buffers.remove(&node_idx) {
                        own_buf
                    } else {
                        let predecessors: Vec<NodeIndex> = plan
                            .graph
                            .neighbors_directed(node_idx, Direction::Incoming)
                            .collect();
                        predecessors
                            .iter()
                            .find_map(|&p| {
                                // When multiple outputs share a predecessor,
                                // clone the buffer for all but the last
                                // consumer to avoid starving siblings.
                                let remaining_consumers = plan
                                    .graph
                                    .neighbors_directed(p, Direction::Outgoing)
                                    .filter(|&succ| succ > node_idx)
                                    .count();
                                if remaining_consumers == 0 {
                                    node_buffers.remove(&p)
                                } else {
                                    node_buffers.get(&p).cloned()
                                }
                            })
                            .unwrap_or_default()
                    };

                    // Count ok records
                    let output_record_count = input_records.len() as u64;
                    counters.ok_count += output_record_count;
                    records_emitted += output_record_count;

                    // Derive output schema from first emitted record
                    let scan_timer =
                        stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
                    let out_cfg = output_configs
                        .iter()
                        .find(|o| o.name == *name)
                        .unwrap_or(primary_output);

                    let output_schema =
                        if let Some((rec, _, emitted, metadata)) = input_records.first() {
                            let projected = {
                                let _guard = projection_timer.guard();
                                project_output_with_meta(rec, emitted, metadata, out_cfg)
                            };
                            Arc::clone(projected.schema())
                        } else {
                            // No records to write — use empty schema
                            collector.record(scan_timer.finish(0, 0));
                            continue;
                        };

                    // Find and take the writer for this output
                    if let Some(raw_writer) = writers.remove(name) {
                        let mut csv_writer =
                            build_format_writer(out_cfg, raw_writer, Arc::clone(&output_schema))?;
                        collector.record(scan_timer.finish(1, 1));

                        for (record, _rn, emitted, metadata) in &input_records {
                            let projected = {
                                let _guard = projection_timer.guard();
                                project_output_with_meta(record, emitted, metadata, out_cfg)
                            };
                            {
                                let _guard = write_timer.guard();
                                csv_writer.write_record(&projected)?;
                            }
                        }
                        {
                            let _guard = write_timer.guard();
                            csv_writer.flush()?;
                        }
                    }
                }

                PlanNode::Composition { ref name, body, .. } => {
                    // Composition runtime expansion deferred to Phase 16c.3.
                    // For now, pass records through unchanged — the body is
                    // lowered for --explain and property derivation only.
                    debug_assert_ne!(
                        body,
                        crate::plan::composition_body::CompositionBodyId::SENTINEL,
                        "composition {name:?}: body_id is sentinel — bind_composition did not run"
                    );
                    let predecessors: Vec<NodeIndex> = plan
                        .graph
                        .neighbors_directed(node_idx, Direction::Incoming)
                        .collect();
                    let input_records = predecessors
                        .iter()
                        .find_map(|p| node_buffers.remove(p))
                        .unwrap_or_default();
                    node_buffers.insert(node_idx, input_records);
                }

                PlanNode::Combine { ref name, .. } => {
                    // Phase Combine C.0: no executor code yet. C.2 wires up
                    // the multi-input build/probe dispatch arm. Reaching this
                    // arm in C.0 is a planner bug (a Combine node escaped
                    // into the DAG without C.0.4 DAG-edge wiring or with
                    // premature execution). Fail loud.
                    return Err(PipelineError::Internal {
                        op: "combine",
                        node: name.clone(),
                        detail: "combine executor dispatch not implemented until Phase Combine C.2"
                            .to_string(),
                    });
                }
            }
        }

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

        Ok((
            std::mem::take(counters),
            std::mem::take(dlq_entries),
            rss_bytes(),
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
    ) -> Result<CompiledRoute, PipelineError> {
        let type_cols: IndexMap<String, Type> = emitted_fields
            .iter()
            .map(|f| (f.clone(), Type::Any))
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

            let resolved = cxl::resolve::resolve_program(
                parse_result.ast,
                &field_refs,
                parse_result.node_count,
            )
            .map_err(|diags| PipelineError::Compilation {
                transform_name: format!("route:{}", branch.name),
                messages: diags.into_iter().map(|d| d.message).collect(),
            })?;

            let typed = cxl::typecheck::type_check(resolved, &type_schema).map_err(|diags| {
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

    /// Build lookup tables for all transforms that have a `lookup:` config.
    ///
    /// Extracts readers from the `readers` registry for each unique lookup source,
    /// loads them into in-memory `LookupTable`s, and compiles the `where:` predicate
    /// as a CXL filter evaluator. Returns a map from transform name → RuntimeLookup.
    fn build_lookup_tables(
        config: &PipelineConfig,
        readers: &mut HashMap<String, Box<dyn Read + Send>>,
        source_configs: &[crate::config::SourceConfig],
    ) -> Result<HashMap<String, RuntimeLookup>, PipelineError> {
        use crate::config::PipelineNode;
        use crate::pipeline::lookup::LookupTable;

        let mut lookup_tables: HashMap<String, RuntimeLookup> = HashMap::new();

        // Walk nodes to find transforms with lookup config
        for spanned in &config.nodes {
            if let PipelineNode::Transform {
                header,
                config: body,
            } = &spanned.value
                && let Some(lookup_config) = &body.lookup
            {
                let transform_name = &header.name;
                let source_name = &lookup_config.source;

                // Find the source config for this lookup source
                let source_cfg = source_configs
                        .iter()
                        .find(|s| s.name == *source_name)
                        .ok_or_else(|| PipelineError::Config(
                            crate::config::ConfigError::Validation(format!(
                                "transform '{}' has lookup source '{}' which is not declared as a source node",
                                transform_name, source_name,
                            )),
                        ))?;

                // Extract the reader for this source
                let reader = readers.remove(source_name).ok_or_else(|| {
                    PipelineError::Config(crate::config::ConfigError::Validation(format!(
                        "no reader registered for lookup source '{}'",
                        source_name,
                    )))
                })?;

                // Build format reader with schema coercion and load into LookupTable
                let raw_reader = build_format_reader(source_cfg, reader)?;
                let mut format_reader = wrap_with_schema_coercion(raw_reader, config, source_name)?;
                let memory_limit = parse_memory_limit(config);
                let table =
                    LookupTable::build(source_name.clone(), &mut *format_reader, memory_limit)
                        .map_err(|e| PipelineError::Compilation {
                            transform_name: transform_name.clone(),
                            messages: vec![e.to_string()],
                        })?;

                // Compile the where predicate as a CXL filter program.
                // Collect field names from the primary source schema so the
                // resolver can recognize bare field references. Qualified
                // references (source.field) are resolved at runtime by the
                // LookupResolver and don't need to be registered here.
                // Normalize newlines to spaces so YAML block scalars
                // (where: |) with line breaks between `and` clauses parse
                // correctly — the CXL parser treats newlines as statement
                // boundaries.
                let where_expr = lookup_config.where_expr.replace('\n', " ");
                let where_expr = where_expr.trim();
                let filter_source = format!("filter {}", where_expr);
                let parse_result = cxl::parser::Parser::parse(&filter_source);
                if !parse_result.errors.is_empty() {
                    return Err(PipelineError::Compilation {
                        transform_name: transform_name.clone(),
                        messages: parse_result
                            .errors
                            .iter()
                            .map(|e| format!("lookup where parse error: {}", e.message))
                            .collect(),
                    });
                }
                // Collect field names from ALL source node schemas so
                // the CXL resolver accepts bare field references from
                // both the input source and the lookup source.
                let mut field_names: Vec<String> = Vec::new();
                for s in &config.nodes {
                    if let PipelineNode::Source { config: body, .. } = &s.value {
                        for col in &body.schema.columns {
                            if !field_names.contains(&col.name) {
                                field_names.push(col.name.clone());
                            }
                        }
                    }
                }
                let field_refs: Vec<&str> = field_names.iter().map(|s| s.as_str()).collect();
                let resolved = cxl::resolve::resolve_program(
                    parse_result.ast,
                    &field_refs,
                    parse_result.node_count,
                )
                .map_err(|diags| PipelineError::Compilation {
                    transform_name: transform_name.clone(),
                    messages: diags
                        .into_iter()
                        .map(|d| format!("lookup where resolve error: {}", d.message))
                        .collect(),
                })?;
                let schema_row = cxl::typecheck::Row::closed(
                    indexmap::IndexMap::new(),
                    cxl::lexer::Span::new(0, 0),
                );
                let typed = cxl::typecheck::type_check(resolved, &schema_row).map_err(|diags| {
                    PipelineError::Compilation {
                        transform_name: transform_name.clone(),
                        messages: diags
                            .iter()
                            .map(|d| format!("lookup where type error: {}", d.message))
                            .collect(),
                    }
                })?;
                let typed = Arc::new(typed);
                let equality_index =
                    crate::pipeline::lookup::build_equality_index(&typed, source_name, &table);
                let evaluator = ProgramEvaluator::new(typed, false);

                lookup_tables.insert(
                    transform_name.clone(),
                    RuntimeLookup {
                        table,
                        where_evaluator: std::sync::Mutex::new(evaluator),
                        on_miss: lookup_config.on_miss,
                        match_mode: lookup_config.match_mode,
                        equality_index,
                    },
                );
            }
        }

        Ok(lookup_tables)
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
    /// Phase 16b Task 16b.9: drives `config.compile()` so the
    /// stage-4.5 `bind_schema` pass produces the typed programs;
    /// `--explain` no longer parses CXL twice.
    pub(crate) fn explain_dag(
        config: &PipelineConfig,
    ) -> Result<(ExecutionPlanDag, ()), PipelineError> {
        // Phase 16b Task 16b.9 — compile-time typecheck via bind_schema.
        // The validated plan gives us pre-typechecked `Arc<TypedProgram>`s
        // per node; feed them straight into `ExecutionPlanDag::compile`
        // (the full planner path that derives output projections,
        // parallelism profiles, and physical properties) WITHOUT
        // re-typechecking.
        let validated_plan = config
            .compile(&crate::config::CompileContext::default())
            .map_err(|diags| PipelineError::Compilation {
                transform_name: String::new(),
                messages: diags.iter().map(|d| d.message.clone()).collect(),
            })?;
        let resolved_transforms_owned = crate::executor::build_transform_specs(config);
        let compiled_refs: Vec<(&str, &TypedProgram)> = resolved_transforms_owned
            .iter()
            .map(|t| {
                let typed = validated_plan
                    .artifacts()
                    .typed
                    .get(&t.name)
                    .expect("bind_schema must produce a typed program for every transform");
                (t.name.as_str(), typed.as_ref())
            })
            .collect();
        let plan = ExecutionPlanDag::compile(config, &compiled_refs).map_err(|e| {
            PipelineError::Compilation {
                transform_name: String::new(),
                messages: vec![e.to_string()],
            }
        })?;
        Ok((plan, ()))
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

/// Wrap a format reader with schema-based type coercion if the source
/// declares typed columns in its `schema:` block.
fn wrap_with_schema_coercion(
    reader: Box<dyn FormatReader>,
    config: &PipelineConfig,
    source_name: &str,
) -> Result<Box<dyn FormatReader>, PipelineError> {
    use crate::config::PipelineNode;

    // Find the source node's schema declaration
    let schema_decl = config.nodes.iter().find_map(|s| {
        if let PipelineNode::Source {
            header,
            config: body,
        } = &s.value
            && header.name == source_name
        {
            return Some(&body.schema.columns);
        }
        None
    });

    match schema_decl {
        Some(columns) if !columns.is_empty() => {
            let coercing = crate::pipeline::schema_coerce::CoercingReader::new(reader, columns)
                .map_err(|e| PipelineError::Compilation {
                    transform_name: source_name.to_string(),
                    messages: vec![format!("schema coercion init error: {e}")],
                })?;
            Ok(Box::new(coercing))
        }
        _ => Ok(reader),
    }
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
) -> WriterFactory {
    match format {
        crate::config::OutputFormat::Csv(opts) => {
            let csv_config = build_csv_writer_config(opts.as_ref(), include_header);
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
            let json_config = build_json_writer_config(opts.as_ref());
            Box::new(move |counting_writer, schema| {
                Ok(Box::new(JsonWriter::new(
                    counting_writer,
                    schema,
                    json_config.clone(),
                )))
            })
        }
        crate::config::OutputFormat::Xml(opts) => {
            let xml_config = build_xml_writer_config(opts.as_ref());
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
fn build_format_writer(
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
    );

    if let Some(ref split) = output.split {
        let policy = build_split_policy(split);
        let output_path = output.path.clone();
        let naming = split.naming.clone();

        let file_factory: clinker_format::splitting::FileFactory =
            Box::new(move |seq: u32| -> std::io::Result<Box<dyn Write + Send>> {
                let path = apply_split_naming(&output_path, &naming, seq);
                let file = std::fs::File::create(path)?;
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

/// Build a rayon thread pool from config. Explicit pool (not global) for testability.
///
/// Default: `min(num_cpus - 2, 4)`. Overridable via `concurrency.threads` config.
fn build_thread_pool(config: &PipelineConfig) -> Result<rayon::ThreadPool, PipelineError> {
    let worker_threads = config
        .pipeline
        .concurrency
        .as_ref()
        .and_then(|c| c.threads)
        .unwrap_or_else(|| std::cmp::min(num_cpus::get().saturating_sub(2).max(1), 4));

    rayon::ThreadPoolBuilder::new()
        .num_threads(worker_threads)
        .thread_name(|i| format!("cxl-worker-{i}"))
        .build()
        .map_err(|e| PipelineError::ThreadPool(e.to_string()))
}

/// Determine if all transforms can be parallelized (Stateless or IndexReading).
fn can_parallelize(plan: &ExecutionPlanDag) -> bool {
    plan.parallelism.per_transform.iter().all(|c| {
        matches!(
            c,
            ParallelismClass::Stateless | ParallelismClass::IndexReading
        )
    })
}

/// Build the pipeline-stable evaluation context (D59 / Task 16.3.13a).
///
/// Called ONCE per pipeline run at the top of `execute_dag_branching`. The
/// returned `StableEvalContext` is reused (via borrow) at every per-record
/// dispatch site, killing the prior `String::clone` + `IndexMap::clone`
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
) -> StableEvalContext {
    StableEvalContext {
        clock: Box::new(WallClock),
        pipeline_start_time,
        pipeline_name: Arc::from(config.pipeline.name.as_str()),
        pipeline_execution_id: Arc::from(execution_id),
        pipeline_batch_id: Arc::from(batch_id),
        pipeline_counters: PipelineCounters::default(),
        pipeline_vars: Arc::new(pipeline_vars.clone()),
    }
}

/// Evaluate all transforms against a single record, accumulating emitted fields.
///
/// Emitted fields from earlier transforms are merged into the record's overflow
/// so that later transforms can reference them as input fields.
///
/// On error, returns `(transform_name, EvalError)` so callers can populate
/// the DLQ stage field with `"transform:{name}"`.
fn evaluate_record(
    record: &Record,
    transform_names: &[&str],
    evaluators: &mut [ProgramEvaluator],
    ctx: &EvalContext,
) -> Result<EvalResult, (String, cxl::eval::EvalError)> {
    // No lookup tables → always returns exactly one result
    let mut results =
        evaluate_record_with_lookups(record, transform_names, evaluators, ctx, &HashMap::new())?;
    Ok(results
        .pop()
        .unwrap_or(EvalResult::Skip(SkipReason::Filtered)))
}

/// Evaluate a transform chain with lookup enrichment, potentially producing
/// multiple output records when `match: all` fan-out is active.
fn evaluate_record_with_lookups(
    record: &Record,
    transform_names: &[&str],
    evaluators: &mut [ProgramEvaluator],
    ctx: &EvalContext,
    lookup_tables: &HashMap<String, RuntimeLookup>,
) -> Result<Vec<EvalResult>, (String, cxl::eval::EvalError)> {
    // Each "branch" is an in-progress record being transformed through the chain.
    // For match:first / no-lookup this stays at 1; match:all expands it.
    type Branch = (Record, IndexMap<String, Value>, IndexMap<String, Value>);
    let mut branches: Vec<Branch> = vec![(record.clone(), IndexMap::new(), IndexMap::new())];
    let mut last_skip_reason = SkipReason::Filtered;

    for (i, eval) in evaluators.iter_mut().enumerate() {
        let name = transform_names.get(i).copied().unwrap_or("unknown");
        let mut new_branches: Vec<Branch> = Vec::with_capacity(branches.len());

        for (record, all_emitted, all_metadata) in branches {
            if let Some(rt_lookup) = lookup_tables.get(name) {
                let matches = crate::pipeline::lookup::find_matches(
                    &rt_lookup.table,
                    &record,
                    &mut rt_lookup
                        .where_evaluator
                        .lock()
                        .expect("lookup evaluator lock"),
                    ctx,
                    rt_lookup.match_mode,
                    rt_lookup.equality_index.as_ref(),
                )
                .map_err(|e| {
                    (
                        name.to_string(),
                        cxl::eval::EvalError {
                            kind: cxl::eval::EvalErrorKind::ConversionFailed {
                                value: e.to_string(),
                                target: "lookup match",
                            },
                            span: cxl::lexer::Span::new(0, 0),
                        },
                    )
                })?;

                if matches.is_empty() {
                    match rt_lookup.on_miss {
                        crate::config::pipeline_node::OnMiss::Skip => {
                            // Drop this branch (filtered)
                            continue;
                        }
                        crate::config::pipeline_node::OnMiss::Error => {
                            return Err((
                                name.to_string(),
                                cxl::eval::EvalError {
                                    kind: cxl::eval::EvalErrorKind::ConversionFailed {
                                        value: format!(
                                            "no matching row in lookup source '{}'",
                                            rt_lookup.table.source_name()
                                        ),
                                        target: "lookup match",
                                    },
                                    span: cxl::lexer::Span::new(0, 0),
                                },
                            ));
                        }
                        crate::config::pipeline_node::OnMiss::NullFields => {
                            let resolver = crate::pipeline::lookup::LookupResolver::no_match(
                                &record,
                                rt_lookup.table.source_name(),
                            );
                            let eval_result = eval
                                .eval_record::<NullStorage>(ctx, &resolver, None)
                                .map_err(|e| (name.to_string(), e))?;
                            match apply_eval_to_branch(
                                record,
                                all_emitted,
                                all_metadata,
                                eval_result,
                            ) {
                                Ok(branch) => new_branches.push(branch),
                                Err(reason) => last_skip_reason = reason,
                            }
                        }
                    }
                } else {
                    // Emit one branch per match (1 for First, N for All)
                    for &idx in &matches {
                        let matched_row = &rt_lookup.table.records()[idx];
                        let resolver = crate::pipeline::lookup::LookupResolver::matched(
                            &record,
                            rt_lookup.table.source_name(),
                            matched_row,
                        );
                        let eval_result = eval
                            .eval_record::<NullStorage>(ctx, &resolver, None)
                            .map_err(|e| (name.to_string(), e))?;
                        match apply_eval_to_branch(
                            record.clone(),
                            all_emitted.clone(),
                            all_metadata.clone(),
                            eval_result,
                        ) {
                            Ok(branch) => new_branches.push(branch),
                            Err(reason) => last_skip_reason = reason,
                        }
                    }
                }
            } else {
                let eval_result = eval
                    .eval_record::<NullStorage>(ctx, &record, None)
                    .map_err(|e| (name.to_string(), e))?;
                match apply_eval_to_branch(record, all_emitted, all_metadata, eval_result) {
                    Ok(branch) => new_branches.push(branch),
                    Err(reason) => last_skip_reason = reason,
                }
            }
        }

        branches = new_branches;
        if branches.is_empty() {
            return Ok(vec![EvalResult::Skip(last_skip_reason)]);
        }
    }

    Ok(branches
        .into_iter()
        .map(|(_, emitted, metadata)| EvalResult::Emit {
            fields: emitted,
            metadata,
        })
        .collect())
}

/// Apply an eval result to a branch, returning the updated branch or the skip reason.
#[allow(clippy::type_complexity)]
fn apply_eval_to_branch(
    mut record: Record,
    mut all_emitted: IndexMap<String, Value>,
    mut all_metadata: IndexMap<String, Value>,
    eval_result: EvalResult,
) -> Result<(Record, IndexMap<String, Value>, IndexMap<String, Value>), SkipReason> {
    match eval_result {
        EvalResult::Emit {
            fields: emitted,
            metadata,
        } => {
            for (name, value) in &emitted {
                if !record.set(name, value.clone()) {
                    record.set_overflow(name.clone().into_boxed_str(), value.clone());
                }
            }
            for (key, value) in &metadata {
                let _ = record.set_meta(key, value.clone());
            }
            all_emitted.extend(emitted);
            all_metadata.extend(metadata);
            Ok((record, all_emitted, all_metadata))
        }
        EvalResult::Skip(reason) => Err(reason),
    }
}

/// Evaluate all transforms with window context, accumulating emitted fields.
///
/// Emitted fields from earlier transforms are merged into the record's overflow
/// so that later transforms can reference them as input fields.
///
/// On error, returns `(transform_name, EvalError)` so callers can populate
/// the DLQ stage field with `"transform:{name}"`.
#[allow(clippy::too_many_arguments)]
fn evaluate_record_with_window(
    record: &Record,
    transform_names: &[&str],
    evaluators: &mut [ProgramEvaluator],
    ctx: &EvalContext,
    plan: &ExecutionPlanDag,
    arena: &Arena,
    indices: &[SecondaryIndex],
    record_pos: u32,
) -> Result<EvalResult, (String, cxl::eval::EvalError)> {
    let mut record = record.clone();
    let mut all_emitted = IndexMap::new();
    let mut all_metadata = IndexMap::new();

    let window_info = plan.transform_window_info();

    for (i, eval) in evaluators.iter_mut().enumerate() {
        let (wi, _pl) = &window_info[i];
        let name = transform_names.get(i).copied().unwrap_or("unknown");

        let result = if let Some(idx_num) = *wi {
            // This transform uses windows — look up partition
            let spec = &plan.indices_to_build[idx_num];
            let index = &indices[idx_num];

            // Compute group key from current record
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

            if let Some(key) = key {
                if let Some(partition) = index.get(&key) {
                    // Find current position within partition
                    let pos_in_partition =
                        partition.iter().position(|&p| p == record_pos).unwrap_or(0);
                    let wctx = PartitionWindowContext::new(arena, partition, pos_in_partition);
                    eval.eval_record(ctx, &record, Some(&wctx))
                        .map_err(|e| (name.to_string(), e))?
                } else {
                    eval.eval_record::<Arena>(ctx, &record, None)
                        .map_err(|e| (name.to_string(), e))?
                }
            } else {
                eval.eval_record::<Arena>(ctx, &record, None)
                    .map_err(|e| (name.to_string(), e))?
            }
        } else {
            eval.eval_record::<NullStorage>(ctx, &record, None)
                .map_err(|e| (name.to_string(), e))?
        };

        match result {
            EvalResult::Emit {
                fields: emitted,
                metadata,
            } => {
                for (name, value) in &emitted {
                    // Use set() for schema fields (e.g. emit amount = amount.to_int()),
                    // set_overflow() for new fields introduced by the transform.
                    if !record.set(name, value.clone()) {
                        record.set_overflow(name.clone().into_boxed_str(), value.clone());
                    }
                }
                for (key, value) in &metadata {
                    let _ = record.set_meta(key, value.clone());
                }
                all_emitted.extend(emitted);
                all_metadata.extend(metadata);
            }
            skip @ EvalResult::Skip(_) => return Ok(skip),
        }
    }

    Ok(EvalResult::Emit {
        fields: all_emitted,
        metadata: all_metadata,
    })
}

/// Evaluate a single transform against a record, returning the modified record
/// with emitted fields merged in.
///
/// Used by the DAG-walking executor for per-node evaluation.
/// Returns `Ok((modified_record, emitted, metadata))` on emit,
/// `Ok` with `EvalResult::Skip` variant on skip.
/// On error, returns `(transform_name, EvalError)`.
#[allow(clippy::type_complexity)]
fn evaluate_single_transform(
    record: &Record,
    transform_name: &str,
    evaluator: &mut ProgramEvaluator,
    ctx: &EvalContext,
) -> Result<
    (
        Record,
        Result<(IndexMap<String, Value>, IndexMap<String, Value>), SkipReason>,
    ),
    (String, cxl::eval::EvalError),
> {
    let mut record = record.clone();
    match evaluator
        .eval_record::<NullStorage>(ctx, &record, None)
        .map_err(|e| (transform_name.to_string(), e))?
    {
        EvalResult::Emit {
            fields: emitted,
            metadata,
        } => {
            for (name, value) in &emitted {
                if !record.set(name, value.clone()) {
                    record.set_overflow(name.clone().into_boxed_str(), value.clone());
                }
            }
            for (key, value) in &metadata {
                let _ = record.set_meta(key, value.clone());
            }
            Ok((record, Ok((emitted, metadata))))
        }
        EvalResult::Skip(reason) => Ok((record, Err(reason))),
    }
}

/// Parse memory limit from config (default 512MB).
/// Plan-invariant predecessor lookup for nodes that require exactly one
/// upstream input (currently `PlanNode::Aggregation`). Returns
/// `PipelineError::Internal` on misshapen plans — never panics. Mirrors
/// DataFusion's `internal_err!` macro per drill pass 7 D58.
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

fn parse_memory_limit(config: &PipelineConfig) -> usize {
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

/// Handle an evaluation error according to the configured strategy.
#[allow(clippy::too_many_arguments)]
fn handle_error(
    strategy: ErrorStrategy,
    record: &Record,
    row_num: u64,
    eval_err: &cxl::eval::EvalError,
    stage: Option<String>,
    route: Option<String>,
    counters: &mut PipelineCounters,
    dlq_entries: &mut Vec<DlqEntry>,
    output: &OutputConfig,
    _output_schema: &Arc<Schema>,
    writer: &mut dyn FormatWriter,
) -> Result<(), PipelineError> {
    match strategy {
        ErrorStrategy::FailFast => {
            return Err(PipelineError::Eval(cxl::eval::EvalError {
                span: eval_err.span,
                kind: eval_err.kind.clone(),
            }));
        }
        ErrorStrategy::Continue => {
            counters.dlq_count += 1;
            dlq_entries.push(DlqEntry {
                source_row: row_num,
                category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                error_message: eval_err.to_string(),
                original_record: record.clone(),
                stage,
                route,
                trigger: true,
            });
            // Skip this record — don't write to output
        }
        ErrorStrategy::BestEffort => {
            counters.dlq_count += 1;
            dlq_entries.push(DlqEntry {
                source_row: row_num,
                category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                error_message: eval_err.to_string(),
                original_record: record.clone(),
                stage,
                route,
                trigger: true,
            });
            // Write original record unchanged
            let emitted = IndexMap::new();
            let projected = project_output(record, &emitted, output);
            writer.write_record(&projected)?;
            counters.ok_count += 1;
        }
    }
    Ok(())
}

/// Handle an evaluation error before the writer is initialized (scan-forward phase).
/// FailFast should be handled by the caller before reaching this function.
/// For Continue/BestEffort, we count the error and queue it for DLQ — no output write
/// is possible because the output schema is not yet known.
fn handle_error_no_writer(
    record: &Record,
    row_num: u64,
    eval_err: &cxl::eval::EvalError,
    stage: Option<String>,
    counters: &mut PipelineCounters,
    dlq_entries: &mut Vec<DlqEntry>,
) {
    counters.dlq_count += 1;
    dlq_entries.push(DlqEntry {
        source_row: row_num,
        category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
        error_message: eval_err.to_string(),
        original_record: record.clone(),
        stage,
        route: None,
        trigger: true,
    });
}

/// Runtime lookup context for a transform with `lookup:` config.
/// Holds the in-memory reference table, compiled where-predicate
/// evaluator, and match/miss policies.
pub(crate) struct RuntimeLookup {
    pub table: crate::pipeline::lookup::LookupTable,
    pub where_evaluator: std::sync::Mutex<ProgramEvaluator>,
    pub on_miss: crate::config::pipeline_node::OnMiss,
    pub match_mode: crate::config::pipeline_node::MatchMode,
    pub equality_index: Option<crate::pipeline::lookup::EqualityIndex>,
}
