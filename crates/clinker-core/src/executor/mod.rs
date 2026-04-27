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
    /// `true` if this record's own evaluation caused the DLQ entry.
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
        // Aggregate lowering reads `group_by_indices` from
        // `typed.field_types`, which `bind_schema` already keyed and
        // ordered by the upstream node's bound `Row`. The author-
        // declared superset never reaches the index resolver.
        let compile_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Compile);
        let compile_ctx = crate::config::CompileContext::default();
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
        let mut counters = PipelineCounters::default();
        let mut dlq_entries: Vec<DlqEntry> = Vec::new();

        let requires_arena = plan.required_arena();

        // ── Phase 0: source materialization ──
        // Drain the primary reader into `all_records` carrying the
        // full source schema. Records flow through the dispatcher
        // unchanged; the windowed Transform arm reads through arena
        // (a projected columnar view) for `$window.*` lookups but
        // the records themselves keep every source field for
        // downstream transforms / outputs that reference them.
        // `MetadataCapExceeded` is recoverable per-record (DLQ);
        // every other reader error short-circuits.
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

        // Build arena + per-source secondary indices when the plan
        // declares analytic windows. The arena projects each record
        // to the windowed transforms' `arena_fields` set; the index
        // partitions group-key → arena positions for the windowed
        // Transform arm's partition lookup. Records remain
        // full-schema in `all_records`; arena/indices are a side
        // structure consulted only by `$window.*` evaluation.
        let (arena_for_ctx, indices_for_ctx) = if requires_arena && !all_records.is_empty() {
            let arena_fields =
                crate::plan::index::collect_arena_fields(&plan.indices_to_build, &input.name);
            let memory_limit = parse_memory_limit(config);
            let arena_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::ArenaBuild);

            let source_schema = Arc::clone(all_records[0].0.schema());
            let arena_schema: Arc<Schema> = arena_fields
                .iter()
                .map(|f| f.clone().into_boxed_str())
                .collect::<SchemaBuilder>()
                .build();
            let field_indices: Vec<Option<usize>> = arena_fields
                .iter()
                .map(|f| source_schema.index(f))
                .collect();
            let mut bytes_used: usize = 0;
            let mut minimal_records: Vec<clinker_record::MinimalRecord> =
                Vec::with_capacity(all_records.len());
            for (record, _) in &all_records {
                let projected: Vec<Value> = field_indices
                    .iter()
                    .map(|idx| {
                        idx.and_then(|i| record.values().get(i).cloned())
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                let minimal = clinker_record::MinimalRecord::new(projected);
                bytes_used += crate::pipeline::arena::estimated_size(&minimal);
                if bytes_used > memory_limit {
                    return Err(PipelineError::Compilation {
                        transform_name: String::new(),
                        messages: vec![format!(
                            "arena memory budget exceeded: {bytes_used} > {memory_limit}"
                        )],
                    });
                }
                minimal_records.push(minimal);
            }
            let arena = Arena::from_parts(arena_schema, minimal_records);
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

            // Sort partitions whose source is not already in the
            // required order; the windowed Transform arm assumes
            // partition slices are sorted by `IndexSpec.sort_by`.
            for (i, spec) in plan.indices_to_build.iter().enumerate() {
                if !spec.already_sorted {
                    for partition in indices[i].groups.values_mut() {
                        if !sort::is_sorted(&arena, partition, &spec.sort_by) {
                            sort::sort_partition(&arena, partition, &spec.sort_by);
                        }
                    }
                }
            }

            (Some(Arc::new(arena)), Some(Arc::new(indices)))
        } else {
            (None, None)
        };

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
        // inside a composition body that's a port name — resolved here
        // by walking the live edge graph one composition layer at a
        // time. Each step finds the parent scope's incoming edge to
        // the current body's composition node tagged with the current
        // port name, follows to the producer, and recurses if the
        // producer is the parent body's own synthetic-port-source
        // (i.e., the composition's input was itself a port in the
        // enclosing scope). The walk terminates on a non-port producer
        // and returns its name (typically a top-level Source). This
        // keeps reader setup aligned with the dispatcher's port
        // resolution: both consult the live edge graph, so any
        // planner-pass rewrite that updates edges automatically flows
        // through both paths without a parallel name snapshot.
        //
        // Two reverse lookups: combine-node-name → owning body, and
        // body → (enclosing scope, composition NodeIndex in that scope).
        // Top-level combines are absent from `combine_owning_body`;
        // bodies whose composition node lives in the top-level pipeline
        // have `parent_body_id = None` in `body_to_parent`.
        use petgraph::Direction;
        use petgraph::graph::NodeIndex;
        use petgraph::visit::EdgeRef;
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

        #[derive(Clone, Copy)]
        struct BodyParentScope {
            parent_body_id: Option<crate::plan::composition_body::CompositionBodyId>,
            composition_node_idx: NodeIndex,
        }
        let mut body_to_parent: HashMap<
            crate::plan::composition_body::CompositionBodyId,
            BodyParentScope,
        > = HashMap::new();
        for idx in plan.graph.node_indices() {
            if let crate::plan::execution::PlanNode::Composition { body, .. } = &plan.graph[idx] {
                body_to_parent.insert(
                    *body,
                    BodyParentScope {
                        parent_body_id: None,
                        composition_node_idx: idx,
                    },
                );
            }
        }
        for (parent_id, parent_body) in &artifacts.composition_bodies {
            for idx in parent_body.graph.node_indices() {
                if let crate::plan::execution::PlanNode::Composition { body, .. } =
                    &parent_body.graph[idx]
                {
                    body_to_parent.insert(
                        *body,
                        BodyParentScope {
                            parent_body_id: Some(*parent_id),
                            composition_node_idx: idx,
                        },
                    );
                }
            }
        }

        // Edge-walk a body-context port reference to its terminal name
        // in the topmost enclosing scope. At each step the current name
        // must be a port in the current body (otherwise it's already
        // body-internal, terminal). The corresponding parent scope's
        // incoming edge to the body's composition node carries the port
        // tag; the producer of that edge is either:
        //   - the parent body's synthetic-port-source for one of its
        //     own input ports → recurse one layer up, AND
        //   - any other node → terminal (return its name).
        // Distinguishes the synthetic-port-source case by exact
        // NodeIndex match against the parent body's
        // `port_name_to_node_idx`, not just by name, because port names
        // can legally collide with body-internal node names.
        let resolve_upstream = |start_body_id: crate::plan::composition_body::CompositionBodyId,
                                start_name: &str|
         -> String {
            let mut current_body_id = start_body_id;
            let mut current_name = start_name.to_string();
            loop {
                let Some(body) = artifacts.composition_bodies.get(&current_body_id) else {
                    return current_name;
                };
                if !body.port_name_to_node_idx.contains_key(&current_name) {
                    // Not a port in this body — body-internal node
                    // name (Transform/Aggregate/Source declared in the
                    // body); terminal.
                    return current_name;
                }
                let Some(scope) = body_to_parent.get(&current_body_id).copied() else {
                    return current_name;
                };
                let parent_graph = match scope.parent_body_id {
                    None => &plan.graph,
                    Some(pid) => match artifacts.composition_bodies.get(&pid) {
                        Some(b) => &b.graph,
                        None => return current_name,
                    },
                };
                let mut producer_idx_opt: Option<NodeIndex> = None;
                for edge in
                    parent_graph.edges_directed(scope.composition_node_idx, Direction::Incoming)
                {
                    if edge.weight().port.as_deref() == Some(current_name.as_str()) {
                        producer_idx_opt = Some(edge.source());
                        break;
                    }
                }
                let Some(producer_idx) = producer_idx_opt else {
                    // No port-tagged incoming edge — planner-pass
                    // invariant violation; the dispatcher would also
                    // surface this as PipelineError::Internal at the
                    // composition arm. Return current_name; the caller
                    // (reader setup) will silently skip if no reader
                    // matches, mirroring the existing fall-through.
                    return current_name;
                };
                let producer_name = parent_graph[producer_idx].name().to_string();
                match scope.parent_body_id {
                    None => {
                        // Parent is top-level; producer is a top-level
                        // node (typically a Source).
                        return producer_name;
                    }
                    Some(pid) => {
                        let parent_body = match artifacts.composition_bodies.get(&pid) {
                            Some(b) => b,
                            None => return producer_name,
                        };
                        // Synthetic-port-source identification: the
                        // producer's NodeIndex must match the parent
                        // body's port_name_to_node_idx entry for that
                        // name. Name-only check is unsafe — port and
                        // body-internal namespaces can legally collide.
                        if parent_body
                            .port_name_to_node_idx
                            .get(&producer_name)
                            .copied()
                            == Some(producer_idx)
                        {
                            current_body_id = pid;
                            current_name = producer_name;
                        } else {
                            return producer_name;
                        }
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

        Self::execute_dag_branching(
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
            arena_for_ctx,
            indices_for_ctx,
        )
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
        arena: Option<Arc<crate::pipeline::arena::Arena>>,
        indices: Option<Arc<Vec<crate::pipeline::index::SecondaryIndex>>>,
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

        // Correlation grouping context. `Some(...)` iff
        // `error_handling.correlation_key` is set; the planner's
        // `inject_correlation_commit` pass guarantees a terminal
        // `PlanNode::CorrelationCommit` is also present so the
        // dispatcher walks the buffers at end-of-DAG. Group identity
        // travels through the schema as `$ck.<field>` shadow columns
        // stamped at Source ingest; the dispatcher reads them via the
        // `FieldMetadata.snapshot_of` annotation, so the executor
        // context only needs to know whether buffering is active and
        // the per-group cap.
        let (correlation_buffers, correlation_max_group_buffer) =
            match config.error_handling.correlation_key.as_ref() {
                Some(_) => {
                    let cap = config.error_handling.max_group_buffer.unwrap_or(100_000);
                    (Some(HashMap::new()), cap)
                }
                None => (None, 0),
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
            arena,
            indices,
            correlation_buffers,
            correlation_max_group_buffer,
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

/// Same as [`evaluate_single_transform`] but threads a
/// [`PartitionWindowContext`] derived from the per-source `arena` and
/// `indices` so `$window.*` expressions resolve against the
/// transform's analytic window. `window_index` indexes
/// `plan.indices_to_build`; the matching [`SecondaryIndex`] is looked
/// up via `indices[window_index]`.
///
/// `record_pos` is the record's arena position (zero-based). The
/// caller derives it from `EvalContext::source_row - 1`, which holds
/// because Phase-0 setup materializes records in arena order with
/// `source_row = pos + 1` and intervening dispatcher arms preserve
/// the row number through transform/route/merge buffers.
///
/// If the record's group-by values do not resolve to a partition
/// (null in any group key, or no matching partition), the evaluator
/// runs without a window context — matching the legacy inline arena
/// path's behavior.
#[allow(clippy::too_many_arguments)]
pub(crate) fn evaluate_single_transform_windowed(
    record: &Record,
    transform_name: &str,
    evaluator: &mut ProgramEvaluator,
    ctx: &EvalContext,
    plan: &ExecutionPlanDag,
    window_index: usize,
    arena: &Arena,
    indices: &[SecondaryIndex],
    record_pos: u32,
) -> Result<(Record, Result<(), SkipReason>), (String, cxl::eval::EvalError)> {
    let spec = &plan.indices_to_build[window_index];
    let index = &indices[window_index];

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
            metadata,
        } => {
            let mut out = record_with_emitted_fields(record, &emitted);
            for (name, value) in &emitted {
                out.set(name, value.clone());
            }
            for (key, value) in &metadata {
                let _ = out.set_meta(key, value.clone());
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
    let mut out = Record::new(Arc::clone(target), values);
    for (k, v) in input.iter_meta() {
        let _ = out.set_meta(k, v.clone());
    }
    out
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
