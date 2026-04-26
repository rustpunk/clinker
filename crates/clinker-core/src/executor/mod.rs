pub mod stage_metrics;

pub mod combine;
mod dispatch;
mod schema_check;

use std::collections::{HashMap, HashSet};
use std::io::{BufWriter, Read, Write};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use clinker_record::{PipelineCounters, Record, RecordStorage, Schema, SchemaBuilder, Value};
use indexmap::IndexMap;
use rayon::prelude::*;

use crate::config::{ErrorStrategy, OutputConfig, PipelineConfig};
use crate::error::PipelineError;
use crate::pipeline::arena::Arena;
use crate::pipeline::index::{GroupByKey, SecondaryIndex, value_to_group_key};
use crate::pipeline::memory::{MemoryBudget, rss_bytes};
use crate::pipeline::sort;
use crate::pipeline::window_context::PartitionWindowContext;
use crate::plan::execution::{ExecutionPlanDag, ParallelismClass};
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
pub(crate) struct NullStorage;
impl RecordStorage for NullStorage {
    fn resolve_field(&self, _: u32, _: &str) -> Option<&Value> {
        None
    }
    fn resolve_qualified(&self, _: u32, _: &str, _: &str) -> Option<&Value> {
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
    /// own [`FieldResolver`] impl — schema + overflow for bare names,
    /// `$meta.*` prefix stripping for per-record metadata. No parallel
    /// bookkeeping map is required (Invariant 3).
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
    /// `&CompiledPlan`-consuming public entry point.
    ///
    /// Accepts the typed `CompiledPlan` handle returned by
    /// [`crate::config::PipelineConfig::compile`] and forwards to
    /// [`Self::run_with_readers_writers`] using the plan's embedded
    /// [`PipelineConfig`].
    ///
    /// The primary (driving) source is the first source node in
    /// declaration order (`config.source_configs().next()`). Callers
    /// that need to drive the pipeline from a non-first declared
    /// source must use
    /// [`Self::run_plan_with_readers_writers_with_primary`] instead —
    /// declaration-order-as-primary is a convenience of this wrapper,
    /// not a contract of the executor itself.
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
    /// HashMaps.
    ///
    /// `primary` is the name of the source node that drives the
    /// pipeline: its reader is consumed as the streaming input, and
    /// every other entry in `readers` feeds combine-node build sides.
    /// Source declaration order in YAML is irrelevant — `primary` is
    /// chosen explicitly. If `primary` does not match a declared source
    /// name, or no reader is registered under that name, the function
    /// returns `PipelineError::Config(ConfigError::Validation(..))`.
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
        // `runtime_input_schema` threads the live column layout into
        // `CompileContext` so the Aggregate lowering arm resolves
        // `group_by_indices` against the real file columns rather
        // than the author-declared source schema (which may be a
        // superset).
        let compile_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Compile);
        let runtime_input_schema: Vec<String> =
            schema.columns().iter().map(|c| c.to_string()).collect();
        let compile_ctx = crate::config::CompileContext {
            runtime_input_schema: Some(runtime_input_schema),
            ..crate::config::CompileContext::default()
        };
        let validated_plan =
            config
                .compile(&compile_ctx)
                .map_err(|diags| PipelineError::Compilation {
                    transform_name: String::new(),
                    messages: diags.iter().map(|d| d.message.clone()).collect(),
                })?;
        let resolved_transforms_owned = crate::executor::build_transform_specs(config);
        let resolved_transforms: Vec<&TransformSpec> = resolved_transforms_owned.iter().collect();
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
                    Some(Self::compile_route(rc, &emitted_fields)?)
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
                let cr = Self::compile_route(&route_config, &emitted_fields)?;
                compiled_routes_by_name.insert(route_name.clone(), cr);
            }
        }

        let plan = validated_plan.dag();
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

        let (counters, dlq_entries, peak_rss_bytes) = Self::execute_dag(
            config,
            input,
            format_reader,
            &mut readers,
            &source_configs,
            writers,
            &compiled_transforms,
            compiled_route,
            compiled_routes_by_name,
            plan,
            validated_plan.artifacts(),
            params,
            &mut collector,
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
            // Preserve the mutated record from a successful evaluation
            // so the downstream projection sees every emit and
            // `$meta.*` write the Record already carries.
            let (out_record, flat_result) = match result {
                Ok((rec, res)) => (rec, Ok(res)),
                Err(e) => (record, Err(e)),
            };
            results.push((out_record, rn, flat_result));
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
                            match route.evaluate(&record, &ctx) {
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
                                    counters.records_written += targets.len() as u64;
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
                            counters.records_written += 1;
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
            // Preserve the mutated record from a successful evaluation
            // so the downstream projection sees every emit and
            // `$meta.*` write the Record already carries.
            let (out_record, flat_result) = match result {
                Ok((rec, res)) => (rec, Ok(res)),
                Err(e) => (record, Err(e)),
            };
            results.push((out_record, rn, flat_result));
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
                        counters.records_written += 1;
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
            match route.evaluate(record, ctx) {
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
                    // Dual counter: one source record reaching N
                    // inclusive-route targets counts ONCE toward
                    // `ok_count` (input succeeded) and N times toward
                    // `records_written` (one per Output write).
                    counters.ok_count += 1;
                    counters.records_written += targets.len() as u64;
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
            // Single-output direct path: one input → one write.
            counters.ok_count += 1;
            counters.records_written += 1;
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
        readers: &mut HashMap<String, Box<dyn Read + Send>>,
        source_configs: &[crate::config::SourceConfig],
        writers: HashMap<String, Box<dyn Write + Send>>,
        transforms: &[CompiledTransform],
        compiled_route: Option<CompiledRoute>,
        compiled_routes_by_name: HashMap<String, CompiledRoute>,
        plan: &ExecutionPlanDag,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        params: &PipelineRunParams,
        collector: &mut stage_metrics::StageCollector,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, Option<u64>), PipelineError> {
        let output_configs: Vec<_> = config.output_configs().cloned().collect();
        let primary_output = &output_configs[0];
        let pipeline_start_time = chrono::Local::now().naive_local();

        // Pipeline-stable evaluation context. Built once here, reused
        // (via borrow) at every per-record dispatch site below.
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
                    .map(|col| {
                        arena
                            .resolve_field(pos, col)
                            .cloned()
                            .unwrap_or(Value::Null)
                    })
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
                    Ok((
                        mutated,
                        EvalResult::Emit {
                            fields: emitted,
                            metadata,
                        },
                    )) => {
                        first_emitted = Some((mutated, emitted, metadata));
                        first_emit_pos = Some(pos);
                        counters.ok_count += 1;
                        counters.records_written += 1;
                        break;
                    }
                    Ok((_, EvalResult::Skip(SkipReason::Filtered))) => {
                        counters.filtered_count += 1;
                    }
                    Ok((_, EvalResult::Skip(SkipReason::Duplicate))) => {
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
                                    *result = Some(
                                        evaluate_record_with_window(
                                            record,
                                            &transform_names,
                                            &mut local_evals,
                                            &ctx,
                                            plan,
                                            &arena,
                                            &indices,
                                            *pos,
                                        )
                                        .map(
                                            |(mutated, res)| {
                                                *record = mutated;
                                                res
                                            },
                                        ),
                                    );
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
                        *result = Some(
                            evaluate_record_with_window(
                                record,
                                &transform_names,
                                evaluators,
                                &ctx,
                                plan,
                                &arena,
                                &indices,
                                *pos,
                            )
                            .map(|(mutated, res)| {
                                *record = mutated;
                                res
                            }),
                        );
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
                    match route.evaluate(&record, &ctx) {
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
                                    route.evaluate(record, &ctx)
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
                                        counters.records_written += targets.len() as u64;
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
                                counters.records_written += 1;
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
        loop {
            match format_reader.next_record() {
                Ok(Some(record)) => {
                    row_num += 1;
                    counters.total_count += 1;
                    all_records.push((record, row_num));
                }
                Ok(None) => break,
                Err(clinker_format::error::FormatError::MetadataCapExceeded {
                    record,
                    key,
                    count,
                }) => {
                    // Reader captured 64 off-schema keys into `$meta.*`
                    // and surfaced the 65th — quarantine the partial
                    // record and continue reading subsequent records.
                    // The pipeline summary aggregates this per-category
                    // via the standard source-stage DLQ counters.
                    row_num += 1;
                    counters.total_count += 1;
                    counters.dlq_count += 1;
                    dlq_entries.push(DlqEntry {
                        source_row: row_num,
                        category: crate::dlq::DlqErrorCategory::MetadataCapExceeded,
                        error_message: format!(
                            "per-record metadata cap exceeded at key {key:?} (count={count})"
                        ),
                        original_record: record,
                        stage: Some(DlqEntry::stage_source()),
                        route: None,
                        trigger: true,
                    });
                }
                Err(other) => return Err(other.into()),
            }
        }

        // D12: a global-fold aggregate (group_by: []) must still emit one
        // row over empty input, so we cannot early-return when the plan
        // contains an Aggregation node — the DAG walk needs to fire the
        // aggregator's empty-input special case.
        if all_records.is_empty() && !plan.has_branching() {
            return Ok((counters, dlq_entries, rss_bytes()));
        }

        // ── Pre-load combine build-side sources ──
        // Every combine input whose upstream source is NOT the primary
        // driving reader needs its records materialized before the DAG
        // walk. `execute_dag_branching`'s `PlanNode::Source` arm consults
        // this map first; primary-sourced entries fall back to
        // `all_records`.
        //
        // Scope: every source name referenced by `artifacts.combine_inputs`
        // as a build-side upstream whose reader is still in the `readers`
        // registry (primary was removed at function entry). Drained here
        // so the readers are consumed exactly once.
        //
        // Body-context translation: `combine_input.upstream_name` is the
        // raw value from the combine's `input:` map. For top-level
        // combines that's a parent-scope source name. For combines
        // inside a composition body that's a port name — translated
        // here through the owning body's `input_port_sources` table to
        // recover the parent-scope source backing the port. Nested
        // compositions chain port references — an inner body's port
        // resolves to an outer body's port, which resolves to the
        // top-level pipeline source — so the translation walks the
        // body-parent chain until it lands on a source name. Without
        // this step, body-context build-side readers would never load
        // and the parent's secondary Source dispatch would canonicalize
        // primary records onto the wrong schema.
        //
        // Two reverse lookups: combine-node-name → owning body, and
        // body → its enclosing body (parent_of). Top-level combines
        // are absent from `combine_owning_body`; bodies whose
        // composition node lives in the top-level pipeline are absent
        // from `parent_of`.
        let mut combine_owning_body: HashMap<
            &str,
            crate::plan::composition_body::CompositionBodyId,
        > = HashMap::new();
        for (body_id, body) in &artifacts.composition_bodies {
            for combine_name in body.name_to_idx.keys() {
                if artifacts.combine_inputs.contains_key(combine_name) {
                    combine_owning_body.insert(combine_name.as_str(), *body_id);
                }
            }
        }
        let mut parent_of: HashMap<
            crate::plan::composition_body::CompositionBodyId,
            crate::plan::composition_body::CompositionBodyId,
        > = HashMap::new();
        for (parent_id, parent_body) in &artifacts.composition_bodies {
            for child_id in &parent_body.nested_body_ids {
                parent_of.insert(*child_id, *parent_id);
            }
        }

        // Walk the port-chain from a body-context combine up to a
        // parent-pipeline source name. At each level, translate the
        // current port name through `body.input_port_sources`. If the
        // resolved value is itself a port name in the enclosing body,
        // step up; otherwise it is the source name we want.
        let resolve_upstream = |start_body_id: crate::plan::composition_body::CompositionBodyId,
                                start_name: &str|
         -> String {
            let mut current_body_id = start_body_id;
            let mut current_name = start_name.to_string();
            loop {
                let Some(body) = artifacts.composition_bodies.get(&current_body_id) else {
                    return current_name;
                };
                let Some(parent_node) = body.input_port_sources.get(&current_name) else {
                    // Not a port in this body — it's a body-internal
                    // node name, which is the terminal case for
                    // body-context combines whose input map references
                    // a transform/aggregate inside the same body.
                    return current_name;
                };
                let parent_node = parent_node.clone();
                match parent_of.get(&current_body_id) {
                    Some(&outer_id) => {
                        current_body_id = outer_id;
                        current_name = parent_node;
                    }
                    None => {
                        // No enclosing body — the resolved name is a
                        // top-level pipeline node (typically a Source).
                        return parent_node;
                    }
                }
            }
        };

        let mut combine_source_records: HashMap<String, Vec<(Record, u64)>> = HashMap::new();
        for (combine_name, combine_inputs) in &artifacts.combine_inputs {
            let owning_body_id = combine_owning_body.get(combine_name.as_str()).copied();
            for combine_input in combine_inputs.values() {
                let raw_upstream = combine_input.upstream_name.as_ref();
                let resolved = match owning_body_id {
                    Some(body_id) => resolve_upstream(body_id, raw_upstream),
                    None => raw_upstream.to_string(),
                };
                let upstream: &str = resolved.as_str();
                if upstream == input.name {
                    // Primary source — records already live in
                    // `all_records`.
                    continue;
                }
                if combine_source_records.contains_key(upstream) {
                    continue;
                }
                let Some(reader) = readers.remove(upstream) else {
                    // Another combine already drained the reader (safe —
                    // the map is shared across all combine inputs).
                    continue;
                };
                let src_cfg = source_configs
                    .iter()
                    .find(|s| s.name == upstream)
                    .ok_or_else(|| {
                        PipelineError::Config(crate::config::ConfigError::Validation(format!(
                            "combine build-side source '{upstream}' not declared in the pipeline",
                        )))
                    })?;
                let raw_reader = build_format_reader(src_cfg, reader)?;
                let mut src_reader = wrap_with_schema_coercion(raw_reader, config, upstream)?;
                let mut recs: Vec<(Record, u64)> = Vec::new();
                let mut rn: u64 = 0;
                loop {
                    match src_reader.next_record() {
                        Ok(Some(record)) => {
                            rn += 1;
                            recs.push((record, rn));
                        }
                        Ok(None) => break,
                        Err(clinker_format::error::FormatError::MetadataCapExceeded {
                            record,
                            key,
                            count,
                        }) => {
                            rn += 1;
                            counters.dlq_count += 1;
                            dlq_entries.push(DlqEntry {
                                source_row: rn,
                                category: crate::dlq::DlqErrorCategory::MetadataCapExceeded,
                                error_message: format!(
                                    "per-record metadata cap exceeded at key {key:?} \
                                     (count={count}) reading combine build-side source \
                                     {upstream:?}"
                                ),
                                original_record: record,
                                stage: Some(DlqEntry::stage_source()),
                                route: None,
                                trigger: true,
                            });
                        }
                        Err(other) => return Err(other.into()),
                    }
                }
                combine_source_records.insert(upstream.to_string(), recs);
            }
        }

        // ── Branching DAG walk path ──
        // When the DAG has Route/Merge nodes, use per-node evaluation
        // instead of the sequential transform chain.
        if plan.has_branching() {
            return Self::execute_dag_branching(
                config,
                input,
                all_records,
                combine_source_records,
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
            );
        }

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
                if let Ok((
                    mutated,
                    EvalResult::Emit {
                        fields: emitted, ..
                    },
                )) = evaluate_record(record, &transform_names, &mut evaluators, &ctx)
                {
                    let projected = project_output(&mutated, &emitted, primary_output);
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
                        let result = {
                            let _guard = transform_timer.guard();
                            evaluate_record(&record, &transform_names, &mut evaluators, &ctx)
                        };
                        match result {
                            Ok((
                                mutated,
                                EvalResult::Emit {
                                    fields: emitted,
                                    metadata,
                                },
                            )) => {
                                {
                                    let _guard = route_timer.guard();
                                    let _guard2 = projection_timer.guard();
                                    Self::dispatch_to_outputs(
                                        &mutated,
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
                            Ok((_, EvalResult::Skip(SkipReason::Filtered))) => {
                                counters.filtered_count += 1;
                            }
                            Ok((_, EvalResult::Skip(SkipReason::Duplicate))) => {
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
                        let result = {
                            let _guard = transform_timer.guard();
                            evaluate_record(&record, &transform_names, &mut evaluators, &ctx)
                        };
                        match result {
                            Ok((
                                mutated,
                                EvalResult::Emit {
                                    fields: emitted, ..
                                },
                            )) => {
                                let projected = {
                                    let _guard = projection_timer.guard();
                                    project_output(&mutated, &emitted, primary_output)
                                };
                                {
                                    let _guard = write_timer.guard();
                                    csv_writer.write_record(&projected)?;
                                }
                                counters.ok_count += 1;
                                counters.records_written += 1;
                                records_emitted += 1;
                            }
                            Ok((_, EvalResult::Skip(SkipReason::Filtered))) => {
                                counters.filtered_count += 1;
                            }
                            Ok((_, EvalResult::Skip(SkipReason::Duplicate))) => {
                                counters.distinct_count += 1;
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
        #[allow(clippy::type_complexity)]
        let mut pending_skips_and_errors: Vec<(
            u64,
            Record,
            Result<EvalResult, (String, cxl::eval::EvalError)>,
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
            let result = evaluate_record(record, &transform_names, &mut evaluators, &ctx);
            match result {
                Ok((
                    mutated,
                    EvalResult::Emit {
                        fields: emitted,
                        metadata,
                    },
                )) => {
                    counters.ok_count += 1;
                    counters.records_written += 1;
                    first_emitted = Some((mutated, emitted, metadata));
                    break;
                }
                Ok((_, skip @ EvalResult::Skip(_))) => {
                    // All results are Skips — defer (counters updated in pending processing)
                    pending_skips_and_errors.push((*rn, record.clone(), Ok(skip)));
                }
                Err(e) => {
                    pending_skips_and_errors.push((*rn, record.clone(), Err(e)));
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
                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                        counters.filtered_count += 1;
                    }
                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                        counters.distinct_count += 1;
                    }
                    Ok(EvalResult::Emit { .. }) => {} // emits handled in scan
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

            // Dispatch first emitted record
            let first_emitted_was_some = first_emitted.is_some();
            if let Some((ref record, ref emitted, ref metadata)) = first_emitted {
                let route = compiled_route.as_mut().unwrap();
                let ctx = EvalContext {
                    stable: &stable,
                    source_file: &source_file_arc,
                    source_row: 1,
                };
                let mut per_output_batches: HashMap<String, Vec<Record>> = HashMap::new();
                match route.evaluate(record, &ctx) {
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
                flush_output_batches(&mut per_output_batches, &output_channels)?;
            }

            // Process remaining records
            let route = compiled_route.as_mut().unwrap();
            let mut per_output_batches: HashMap<String, Vec<Record>> = HashMap::new();
            let mut transform_timer = stage_metrics::CumulativeTimer::new();
            let mut projection_timer = stage_metrics::CumulativeTimer::new();
            let mut route_timer = stage_metrics::CumulativeTimer::new();
            let mut write_timer = stage_metrics::CumulativeTimer::new();
            let mut records_emitted: u64 = if first_emitted_was_some { 1 } else { 0 };
            let remaining_count = all_records.len().saturating_sub(scan_idx) as u64;

            for (record, rn) in all_records.into_iter().skip(scan_idx) {
                let ctx = EvalContext {
                    stable: &stable,
                    source_file: &source_file_arc,
                    source_row: rn,
                };
                let result = {
                    let _guard = transform_timer.guard();
                    evaluate_record(&record, &transform_names, &mut evaluators, &ctx)
                };
                match result {
                    Ok((
                        mutated,
                        EvalResult::Emit {
                            fields: emitted,
                            metadata,
                        },
                    )) => {
                        let route_result = {
                            let _guard = route_timer.guard();
                            route.evaluate(&mutated, &ctx)
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
                                            &mutated, &emitted, &metadata, out_cfg,
                                        )
                                    };
                                    per_output_batches
                                        .entry(target.clone())
                                        .or_default()
                                        .push(projected);
                                }
                                counters.ok_count += 1;
                                counters.records_written += targets.len() as u64;
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
                                    category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                    error_message: route_err.to_string(),
                                    original_record: mutated.clone(),
                                    stage: Some(DlqEntry::stage_route_eval()),
                                    route: None,
                                    trigger: true,
                                });
                            }
                        }
                    }
                    Ok((_, EvalResult::Skip(SkipReason::Filtered))) => {
                        counters.filtered_count += 1;
                    }
                    Ok((_, EvalResult::Skip(SkipReason::Duplicate))) => {
                        counters.distinct_count += 1;
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
                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                        counters.filtered_count += 1;
                    }
                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                        counters.distinct_count += 1;
                    }
                    Ok(EvalResult::Emit { .. }) => {} // emits handled in scan
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
            }

            // Process remaining records
            let mut transform_timer = stage_metrics::CumulativeTimer::new();
            let mut projection_timer = stage_metrics::CumulativeTimer::new();
            let mut write_timer = stage_metrics::CumulativeTimer::new();
            let mut records_emitted: u64 = if first_emitted_was_some { 1 } else { 0 };
            let remaining_count = all_records.len().saturating_sub(scan_idx) as u64;

            for (record, rn) in all_records.into_iter().skip(scan_idx) {
                let ctx = EvalContext {
                    stable: &stable,
                    source_file: &source_file_arc,
                    source_row: rn,
                };
                let result = {
                    let _guard = transform_timer.guard();
                    evaluate_record(&record, &transform_names, &mut evaluators, &ctx)
                };
                match result {
                    Ok((
                        mutated,
                        EvalResult::Emit {
                            fields: emitted,
                            metadata,
                        },
                    )) => {
                        let projected = {
                            let _guard = projection_timer.guard();
                            project_output_with_meta(&mutated, &emitted, &metadata, output)
                        };
                        {
                            let _guard = write_timer.guard();
                            csv_writer.write_record(&projected)?;
                        }
                        counters.ok_count += 1;
                        counters.records_written += 1;
                        records_emitted += 1;
                    }
                    Ok((_, EvalResult::Skip(SkipReason::Filtered))) => {
                        counters.filtered_count += 1;
                    }
                    Ok((_, EvalResult::Skip(SkipReason::Duplicate))) => {
                        counters.distinct_count += 1;
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
        combine_source_records: HashMap<String, Vec<(Record, u64)>>,
        writers: HashMap<String, Box<dyn Write + Send>>,
        transforms: &[CompiledTransform],
        compiled_route: Option<CompiledRoute>,
        compiled_routes_by_name: HashMap<String, CompiledRoute>,
        plan: &ExecutionPlanDag,
        artifacts: &crate::plan::bind_schema::CompileArtifacts,
        params: &PipelineRunParams,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        collector: &mut stage_metrics::StageCollector,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, Option<u64>), PipelineError> {
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
        );
        let source_file_arc: Arc<str> = Arc::from(input.path.as_str());

        let strategy = config.error_handling.strategy;
        let total_records = all_records.len() as u64;

        // Name → index map for looking up `CompiledTransform` by node
        // name. Borrowed by the dispatcher's Transform / Aggregation
        // arms.
        let transform_by_name: HashMap<&str, usize> = transforms
            .iter()
            .enumerate()
            .map(|(i, t)| (t.name.as_str(), i))
            .collect();

        // Pipeline-scoped spill directory. One TempDir per pipeline
        // run; every spilling operator borrows its path. Drop runs at
        // the end of the walk (normal exit) or during stack unwinding
        // (panic), so any individual spill file an operator left
        // behind mid-flight gets swept by the directory's recursive
        // remove. This is the secondary cleanup sweep that closes the
        // panic-leak hole; primary cleanup remains per-file via
        // `tempfile::TempPath` Drop.
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

        // Construct the dispatcher context. Mutable per-walk state
        // (node_buffers, counters, DLQ, timers, output writers,
        // output errors, the visited-source set backing the
        // dual-counter semantic) lives on the context; the immutable
        // plan-time refs are borrowed for the lifetime of the walk.
        //
        // `counters` and `dlq_entries` are taken out of their `&mut`
        // borrows here, mutated through the dispatcher, and written
        // back at the end of the walk.
        let mut ctx = dispatch::ExecutorContext {
            config,
            artifacts,
            output_configs: &output_configs,
            primary_output: &output_configs[0],
            compiled_transforms: transforms,
            transform_by_name,
            stable: &stable,
            source_file_arc: &source_file_arc,
            strategy,

            node_buffers: HashMap::new(),
            combine_source_records,
            all_records,
            writers,
            compiled_route,
            counters: std::mem::take(counters),
            dlq_entries: std::mem::take(dlq_entries),
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
        };

        // Walk DAG in topological order. `topo_order` is cloned so
        // the iterator does not hold a whole-struct immutable borrow
        // of `plan` for the loop body — `&mut ctx` already borrows
        // `ctx.current_dag` (which aliases `plan`) at the field
        // granularity through every dispatcher call.
        for node_idx in plan.topo_order.clone() {
            dispatch::dispatch_plan_node(&mut ctx, plan, node_idx)?;
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
    /// `bind_schema` (stage 4.5) and stage 5 lowering + enrichment. The
    /// runtime schema is not available here (this path serves `--explain`
    /// which runs before readers open), so
    /// `CompileContext::runtime_input_schema` stays
    /// `None` — Aggregate lowering falls back to the author-declared
    /// schema, which is fine for explain output.
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

/// Build the pipeline-stable evaluation context.
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
/// The record is rebuilt onto a widened schema that includes every
/// emitted field before the writes happen, so every `Record::set` lands
/// at a known slot. The returned record carries this widened schema.
///
/// On error, returns `(transform_name, EvalError)` so callers can populate
/// the DLQ stage field with `"transform:{name}"`.
fn evaluate_record(
    record: &Record,
    transform_names: &[&str],
    evaluators: &mut [ProgramEvaluator],
    ctx: &EvalContext,
) -> Result<(Record, EvalResult), (String, cxl::eval::EvalError)> {
    let mut record = record.clone();
    let mut all_emitted: IndexMap<String, clinker_record::Value> = IndexMap::new();
    let mut all_metadata: IndexMap<String, clinker_record::Value> = IndexMap::new();

    for (i, eval) in evaluators.iter_mut().enumerate() {
        let name = transform_names.get(i).copied().unwrap_or("unknown");
        let eval_result = eval
            .eval_record::<NullStorage>(ctx, &record, None)
            .map_err(|e| (name.to_string(), e))?;
        match eval_result {
            EvalResult::Emit {
                fields: emitted,
                metadata,
            } => {
                record = record_with_emitted_fields(&record, &emitted);
                for (field_name, value) in &emitted {
                    record.set(field_name, value.clone());
                }
                for (key, value) in &metadata {
                    let _ = record.set_meta(key, value.clone());
                }
                all_emitted.extend(emitted);
                all_metadata.extend(metadata);
            }
            skip @ EvalResult::Skip(_) => return Ok((record, skip)),
        }
    }

    Ok((
        record,
        EvalResult::Emit {
            fields: all_emitted,
            metadata: all_metadata,
        },
    ))
}

/// Evaluate all transforms with window context, accumulating emitted fields.
///
/// Emitted fields from earlier transforms land on the widened schema
/// so later transforms can reference them as input fields.
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
) -> Result<(Record, EvalResult), (String, cxl::eval::EvalError)> {
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
                record = record_with_emitted_fields(&record, &emitted);
                for (name, value) in &emitted {
                    record.set(name, value.clone());
                }
                for (key, value) in &metadata {
                    let _ = record.set_meta(key, value.clone());
                }
                all_emitted.extend(emitted);
                all_metadata.extend(metadata);
            }
            skip @ EvalResult::Skip(_) => return Ok((record, skip)),
        }
    }

    Ok((
        record,
        EvalResult::Emit {
            fields: all_emitted,
            metadata: all_metadata,
        },
    ))
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
            metadata,
        } => {
            let mut out = record_with_emitted_fields(input, &emitted);
            for (name, value) in &emitted {
                out.set(name, value.clone());
            }
            for (key, value) in &metadata {
                let _ = out.set_meta(key, value.clone());
            }
            Ok((out, Ok(())))
        }
        EvalResult::Skip(reason) => Ok((input.clone(), Err(reason))),
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
    for col in target.columns() {
        values.push(
            input
                .get(col)
                .cloned()
                .unwrap_or(clinker_record::Value::Null),
        );
    }
    let mut out = Record::new(Arc::clone(target), values);
    for (k, v) in input.iter_meta() {
        let _ = out.set_meta(k, v.clone());
    }
    out
}

/// Widen `input`'s schema in place to include every key in `emitted`
/// that is not already declared. Allocates a fresh `Arc<Schema>` only
/// when new names appear; otherwise clones `input`. Used by the legacy
/// linear pipeline path where the emit set is determined at eval time
/// rather than via a plan-time `output_schema`.
fn record_with_emitted_fields(
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
    let mut builder = SchemaBuilder::with_capacity(input.schema().column_count() + missing.len());
    for col in input.schema().columns() {
        builder = builder.with_field(col.as_ref());
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
            counters.records_written += 1;
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
    pub(super) fn run_test(
        yaml: &str,
        csv_input: &str,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
        let config = crate::config::parse_config(yaml).unwrap();
        let output_buf = clinker_bench_support::io::SharedBuffer::new();

        let primary = config.source_configs().next().unwrap().name.clone();
        let readers: HashMap<String, Box<dyn Read + Send>> = HashMap::from([(
            primary.clone(),
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec())) as Box<dyn Read + Send>,
        )]);
        let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
            config.output_configs().next().unwrap().name.clone(),
            Box::new(output_buf.clone()) as Box<dyn Write + Send>,
        )]);

        let pipeline_vars = config
            .pipeline
            .vars
            .as_ref()
            .map(crate::config::convert_pipeline_vars)
            .unwrap_or_default();
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars,
            shutdown_token: None,
        };

        let report = PipelineExecutor::run_with_readers_writers(
            &config, &primary, readers, writers, &params,
        )?;

        let output = output_buf.as_string();
        Ok((report.counters, report.dlq_entries, output))
    }

    mod aggregation;
    mod branching;
    mod correlated_dlq;
    mod format_dispatch;
    mod multi_output;
}
