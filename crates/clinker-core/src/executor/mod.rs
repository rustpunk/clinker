pub mod stage_metrics;

use std::collections::HashMap;
use std::io::{BufWriter, Read, Write};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use clinker_record::{PipelineCounters, Record, RecordStorage, Schema, Value};
use indexmap::IndexMap;

use crate::config::{ErrorStrategy, OutputConfig, PipelineConfig};
use crate::error::PipelineError;
use crate::pipeline::arena::Arena;
use crate::pipeline::index::{GroupByKey, SecondaryIndex, value_to_group_key};
use crate::pipeline::memory::rss_bytes;
use crate::pipeline::sort;
use crate::pipeline::window_context::PartitionWindowContext;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};
use crate::projection::project_output_with_meta;
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
                    .map(|ni| match ni {
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
                    input: project_input(&header.input),
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
                    input: project_input(&header.input),
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
                    input: project_input(&header.input),
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

/// One captured record at an Output node under deferred-write
/// semantics. Used by `OutputSink::Captured` during correlation-key
/// per-group walker invocation.
pub(crate) struct CapturedRow {
    pub record: Record,
    pub row_num: u64,
    pub metadata: IndexMap<String, Value>,
}

/// Output dispatch target for the DAG walker's Output arm.
///
/// `Streamed` is the default: records are projected through the
/// predecessor's `OutputLayout` and written directly to the configured
/// `Write + Send` writer. Ownership of the writer map is transferred
/// into the walker.
///
/// `Captured` is used by the correlation-key pre-DAG driver
/// (Option W Shortcut #2) to defer writes until the group's
/// success/failure decision is known. The walker pushes raw (pre-
/// projection) records into a per-output-name `Vec`; the caller
/// inspects the captured records and decides whether to flush them
/// through the real writers or demote them to collateral DLQ.
///
/// One parameter encodes the choice structurally. "If capture is set,
/// writers should be empty" is NOT expressible as two separate
/// parameters without a conventional invariant — hence the enum.
pub(crate) enum OutputSink<'a> {
    Streamed(HashMap<String, Box<dyn Write + Send>>),
    Captured(&'a mut HashMap<String, Vec<CapturedRow>>),
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

/// Composite resolver for route predicate evaluation: the record
/// resolves bare `FieldRef`s positionally (via `Record: FieldResolver`)
/// while accumulated per-record metadata resolves `$meta.*` lookups.
///
/// Why not set_meta on the record and use the record as a sole resolver?
/// Accumulated metadata across a multi-transform chain can exceed
/// `Record`'s 64-key cap; keeping metadata side-by-side avoids that bound.
struct RouteResolver<'a> {
    record: &'a Record,
    metadata: &'a IndexMap<String, Value>,
}

impl clinker_record::FieldResolver for RouteResolver<'_> {
    fn resolve(&self, name: &str) -> Option<Value> {
        if let Some(meta_key) = name.strip_prefix("$meta.") {
            return self.metadata.get(meta_key).cloned();
        }
        self.record.resolve(name)
    }
    fn resolve_qualified(&self, source: &str, field: &str) -> Option<Value> {
        self.record.resolve_qualified(source, field)
    }
    fn available_fields(&self) -> Vec<&str> {
        self.record.available_fields()
    }
    fn iter_fields(&self) -> Vec<(String, Value)> {
        self.record.iter_fields()
    }
}

impl CompiledRoute {
    /// Evaluate route conditions against a positional record plus
    /// accumulated metadata.
    ///
    /// Under Option W, the record carries the upstream transform's
    /// bound output schema — all passthroughs and emits are resolvable
    /// positionally via `Record: FieldResolver`. Route predicates may
    /// reference any column of that schema, not just CXL-emitted ones:
    /// the "emitted vs passthrough" distinction that the per-record
    /// emit map used to preserve is now a compile-time property of the
    /// upstream node's `OutputLayout`, not a runtime field-set filter.
    /// Strictly stronger semantics — matches the single-oracle mandate
    /// of `RESEARCH-runtime-record-layout.md`.
    ///
    /// Returns the list of output names the record should be dispatched to.
    /// In Exclusive mode: first matching branch (or default).
    /// In Inclusive mode: all matching branches (or default if none match).
    fn evaluate(
        &mut self,
        record: &Record,
        metadata: &IndexMap<String, Value>,
        ctx: &EvalContext,
    ) -> Result<Vec<String>, cxl::eval::EvalError> {
        let resolver = RouteResolver { record, metadata };

        match self.mode {
            crate::config::RouteMode::Exclusive => {
                for branch in &mut self.branches {
                    match branch
                        .evaluator
                        .eval_record::<NullStorage>(ctx, record, &resolver, None)?
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
                        .eval_record::<NullStorage>(ctx, record, &resolver, None)?
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

/// Unified pipeline executor.
///
/// Option-W single dispatch path: reads source records, optionally
/// builds an Arena + SecondaryIndex pair when `plan.required_arena()`
/// is true, and walks the DAG in topological order. One code path for
/// transforms, windowed transforms, aggregates, routes, merges, sorts,
/// and outputs.
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
        Self::run_with_readers_writers(plan.config(), readers, writers, params)
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
    /// Returns an [`ExecutionReport`] containing record counts, DLQ entries,
    /// execution mode, peak RSS, and wall-clock start/finish timestamps.
    pub(crate) fn run_with_readers_writers(
        config: &PipelineConfig,
        mut readers: HashMap<String, Box<dyn Read + Send>>,
        writers: HashMap<String, Box<dyn Write + Send>>,
        params: &PipelineRunParams,
    ) -> Result<ExecutionReport, PipelineError> {
        let started_at = Utc::now();

        // Extract the primary reader from the registry
        let source_configs: Vec<_> = config.source_configs().cloned().collect();
        let output_configs: Vec<_> = config.output_configs().cloned().collect();
        let input = &source_configs[0];
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
        let plan = ExecutionPlanDag::compile_with_bound_schemas(
            config,
            &compiled_refs,
            Some(&validated_plan.artifacts().bound_schemas),
            Some(&runtime_input_schema),
            // Bind-time output_layouts map — the planner augments this
            // with inheritance for synthesised nodes and stores the
            // complete map on the DAG.
            Some(&validated_plan.artifacts().output_layouts),
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

        // Option-W runtime layout: materialize each node's bound
        // output `Arc<Schema>` from `bind_schema`'s `BoundSchemas` so
        // every record produced at a node's output is positionally
        // aligned to the schema that downstream positional consumers
        // (aggregator `group_by_indices`, sort keys) resolve against.
        let bound_output_schemas: HashMap<String, Arc<Schema>> = validated_plan
            .artifacts()
            .bound_schemas
            .iter_outputs()
            .filter_map(|(name, _row)| {
                validated_plan
                    .artifacts()
                    .bound_schemas
                    .schema_of(name)
                    .map(|s| (name.to_string(), s))
            })
            .collect();

        // Option W Amendment A7: per-plan-node `OutputLayout` map is
        // authoritative on the DAG itself — populated at plan-compile
        // time with synthesized-node inheritance already applied. No
        // runtime derivation: read and consult.
        let bound_output_layouts = &plan.output_layouts;

        let (counters, dlq_entries, peak_rss_bytes) = Self::execute_dag(
            config,
            format_reader,
            writers,
            &compiled_transforms,
            compiled_route,
            &plan,
            params,
            &mut collector,
            lookup_tables,
            &bound_output_schemas,
            bound_output_layouts,
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

    /// Option-W unified execute entry point — single DAG-driven dispatch.
    ///
    /// Reads every record from the source, optionally builds an `Arena`
    /// and `SecondaryIndex`es when `plan.required_arena()` is true, then
    /// dispatches to the single DAG walker (`execute_dag_branching`) for
    /// every pipeline shape — transforms, routes, merges, aggregates,
    /// sorts, and windowed transforms. There is no longer a separate
    /// chunk-based closed loop, no `has_branching` dispatch gate, and
    /// no `requires_sorted_input` early-bailout: the unified walker
    /// handles all of it.
    ///
    /// Rayon chunk-parallelism (previously gated by
    /// `can_parallelize(plan)` in the arena path) has been removed.
    /// Parallel re-entry via `ParallelismClass` annotations on DAG
    /// nodes is explicit future work per
    /// `RESEARCH-runtime-record-layout.md` Failure mode #7's "pool
    /// later if measured" guardrail.
    ///
    /// Returns `(counters, dlq_entries, peak_rss_bytes)`.
    #[allow(clippy::too_many_arguments)]
    fn execute_dag(
        config: &PipelineConfig,
        mut format_reader: Box<dyn FormatReader>,
        writers: HashMap<String, Box<dyn Write + Send>>,
        transforms: &[CompiledTransform],
        mut compiled_route: Option<CompiledRoute>,
        plan: &ExecutionPlanDag,
        params: &PipelineRunParams,
        collector: &mut stage_metrics::StageCollector,
        lookup_tables: HashMap<String, RuntimeLookup>,
        bound_output_schemas: &HashMap<String, Arc<Schema>>,
        bound_output_layouts: &HashMap<String, Arc<cxl::typecheck::OutputLayout>>,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, Option<u64>), PipelineError> {
        let mut counters = PipelineCounters::default();
        let mut dlq_entries: Vec<DlqEntry> = Vec::new();

        // Read every source record. Arena construction, when required,
        // consumes the reader directly (it streams through the same
        // reader with a field-projection filter); otherwise we collect
        // into `all_records` here for the DAG walker.
        let source_configs: Vec<_> = config.source_configs().cloned().collect();
        let input = &source_configs[0];

        #[allow(clippy::type_complexity)]
        let (arena_opt, indices_opt, all_records): (
            Option<Arc<Arena>>,
            Option<Vec<SecondaryIndex>>,
            Vec<(Record, u64)>,
        ) = if plan.required_arena() {
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

            // Sort partitions per spec, same as the former arena path.
            for (i, spec) in plan.indices_to_build.iter().enumerate() {
                if !spec.already_sorted {
                    for partition in indices[i].groups.values_mut() {
                        if !sort::is_sorted(&arena, partition, &spec.sort_by) {
                            sort::sort_partition(&arena, partition, &spec.sort_by);
                        }
                    }
                }
            }

            // Materialize arena records into Records aligned to the
            // source's bound output schema (what the DAG walker feeds
            // into the Source node arm).
            let arena_schema = Arc::clone(arena.schema());
            let record_count = arena.record_count();
            let mut recs: Vec<(Record, u64)> = Vec::with_capacity(record_count as usize);
            for pos in 0..record_count {
                let values: Vec<Value> = arena_schema
                    .columns()
                    .iter()
                    .map(|col| arena.resolve_field(pos, col).unwrap_or(Value::Null))
                    .collect();
                recs.push((
                    Record::new(Arc::clone(&arena_schema), values),
                    pos as u64 + 1,
                ));
            }
            (Some(Arc::new(arena)), Some(indices), recs)
        } else {
            let mut all_records: Vec<(Record, u64)> = Vec::new();
            let mut row_num: u64 = 0;
            while let Some(record) = format_reader.next_record()? {
                row_num += 1;
                counters.total_count += 1;
                all_records.push((record, row_num));
            }
            (None, None, all_records)
        };

        if plan.required_arena() {
            counters.total_count = all_records.len() as u64;
        }

        // Option W Shortcut #2: correlation-key atomic failure-domain
        // batching. When `error_handling.correlation_key` is set, group
        // source records by the key and invoke the walker PER GROUP
        // with a Captured output sink. If any record in a group
        // produces a DLQ entry, demote ALL captured records in that
        // group to collateral DLQ (`trigger: false`); otherwise flush
        // the captured records through the real writer.
        //
        // Combined with window/arena operations this combination is
        // rejected at plan time (see `plan/execution.rs` E150
        // diagnostic) — per-group Arena construction is documented
        // follow-up work.
        if let Some(corr_key) = config.error_handling.correlation_key.clone() {
            return Self::execute_dag_with_correlation(
                config,
                all_records,
                writers,
                transforms,
                compiled_route,
                plan,
                params,
                counters,
                dlq_entries,
                collector,
                &lookup_tables,
                bound_output_schemas,
                bound_output_layouts,
                arena_opt.as_deref(),
                indices_opt.as_deref(),
                &corr_key,
            );
        }

        Self::execute_dag_branching(
            config,
            all_records,
            OutputSink::Streamed(writers),
            transforms,
            compiled_route.as_mut(),
            plan,
            params,
            &mut counters,
            &mut dlq_entries,
            collector,
            &lookup_tables,
            bound_output_schemas,
            bound_output_layouts,
            arena_opt.as_deref(),
            indices_opt.as_deref(),
        )
    }

    /// Per-group correlation-key execution driver. See
    /// [`execute_dag`]'s comment at the branch for semantics. Produces
    /// the same `(PipelineCounters, Vec<DlqEntry>, Option<u64>)` tuple
    /// as the non-correlated path, with group-demotion accounting
    /// folded into the counters and DLQ entries.
    #[allow(clippy::too_many_arguments)]
    fn execute_dag_with_correlation(
        config: &PipelineConfig,
        all_records: Vec<(Record, u64)>,
        mut writers: HashMap<String, Box<dyn Write + Send>>,
        transforms: &[CompiledTransform],
        mut compiled_route: Option<CompiledRoute>,
        plan: &ExecutionPlanDag,
        params: &PipelineRunParams,
        mut counters: PipelineCounters,
        mut dlq_entries: Vec<DlqEntry>,
        collector: &mut stage_metrics::StageCollector,
        lookup_tables: &HashMap<String, RuntimeLookup>,
        bound_output_schemas: &HashMap<String, Arc<Schema>>,
        bound_output_layouts: &HashMap<String, Arc<cxl::typecheck::OutputLayout>>,
        arena: Option<&Arena>,
        indices: Option<&[SecondaryIndex]>,
        correlation_key: &crate::config::CorrelationKey,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, Option<u64>), PipelineError> {
        let max_buffer = config.error_handling.max_group_buffer.unwrap_or(100_000);

        // Phase 1: group records by correlation key with sticky
        // overflow flag. `IndexMap` preserves first-seen insertion
        // order so group flushing is deterministic.
        struct GroupState {
            records: Vec<(Record, u64)>,
            /// Sticky: once true, the group is pre-failed (overflow).
            /// All further records of this group append to `records`
            /// for DLQ accounting but never reach the walker.
            failed: bool,
        }
        let mut groups: IndexMap<Vec<GroupByKey>, GroupState> = IndexMap::new();
        for (record, rn) in all_records {
            let key = extract_correlation_key(&record, correlation_key);
            let state = groups.entry(key).or_insert_with(|| GroupState {
                records: Vec::new(),
                failed: false,
            });
            if !state.failed && state.records.len() as u64 >= max_buffer {
                state.failed = true;
            }
            state.records.push((record, rn));
        }

        // Accumulator for captured records from successful groups.
        // Flushed once after all groups are processed to ensure a
        // single FormatWriter per output across all successful groups
        // (writers are owned; `writers.remove` only succeeds once).
        let mut successful_captures: HashMap<String, Vec<CapturedRow>> = HashMap::new();

        // Phase 2: per-group walker invocation with deferred output.
        for (key, state) in groups {
            if state.failed {
                // Overflow: synthesise a `GroupSizeExceeded` root-cause
                // DLQ entry (trigger: true) tied to the last appended
                // record, plus collateral entries for every record in
                // the group. The walker is NOT invoked for overflowing
                // groups.
                let key_fmt = format_group_key(&key);
                let count = state.records.len() as u64;
                let overflow_msg = format!(
                    "correlation-key group {key_fmt:?} exceeded \
                     max_group_buffer={max_buffer} after {count} records"
                );
                // Root cause: the offending (last-appended) record.
                if let Some((trigger_rec, trigger_rn)) = state.records.last().cloned() {
                    dlq_entries.push(DlqEntry {
                        source_row: trigger_rn,
                        category: crate::dlq::DlqErrorCategory::GroupSizeExceeded,
                        error_message: overflow_msg.clone(),
                        original_record: trigger_rec,
                        stage: Some("correlation_group_overflow".to_string()),
                        route: None,
                        trigger: true,
                    });
                }
                // Collaterals: every OTHER record in the group.
                let last_idx = state.records.len().saturating_sub(1);
                for (idx, (rec, rn)) in state.records.into_iter().enumerate() {
                    if idx == last_idx {
                        continue;
                    }
                    dlq_entries.push(DlqEntry {
                        source_row: rn,
                        category: crate::dlq::DlqErrorCategory::Correlated,
                        error_message: format!("correlated with failure in group: {overflow_msg}"),
                        original_record: rec,
                        stage: Some("correlation_group_overflow".to_string()),
                        route: None,
                        trigger: false,
                    });
                }
                counters.dlq_count += count;
                continue;
            }

            // Non-overflowing group: invoke walker with Captured sink.
            //
            // Note: `execute_dag_branching` uses `std::mem::take` on the
            // counters/dlq &mut params at the end, packaging them into
            // its return tuple. So we must read from the return tuple,
            // NOT from the `group_counters`/`group_dlq` bindings we
            // pass in (which will be reset to default post-call).
            let mut group_capture: HashMap<String, Vec<CapturedRow>> = HashMap::new();
            let mut counters_slot = PipelineCounters::default();
            let mut dlq_slot: Vec<DlqEntry> = Vec::new();
            let (group_counters, mut group_dlq, _group_rss) = Self::execute_dag_branching(
                config,
                state.records,
                OutputSink::Captured(&mut group_capture),
                transforms,
                compiled_route.as_mut(),
                plan,
                params,
                &mut counters_slot,
                &mut dlq_slot,
                collector,
                lookup_tables,
                bound_output_schemas,
                bound_output_layouts,
                arena,
                indices,
            )?;

            if group_dlq.is_empty() {
                // Group succeeded: accumulate captured records for
                // post-loop flush. We defer the actual writer build +
                // write until ALL groups are processed so multiple
                // successful groups can share a single FormatWriter
                // per output (writers are owned once; `writers.remove`
                // can only succeed once per output name).
                for (out_name, rows) in group_capture {
                    successful_captures
                        .entry(out_name)
                        .or_default()
                        .extend(rows);
                }
                counters.ok_count += group_counters.ok_count;
                counters.filtered_count += group_counters.filtered_count;
                counters.distinct_count += group_counters.distinct_count;
            } else {
                // Group failed: demote every captured OK record to
                // collateral DLQ (trigger: false) with the first
                // root-cause's message. Root-cause DLQ entries already
                // carry trigger: true from the walker.
                let root_msg = group_dlq
                    .first()
                    .map(|e| e.error_message.clone())
                    .unwrap_or_else(|| "group failed".to_string());
                let mut collateral_count: u64 = 0;
                for (out_name, rows) in group_capture {
                    for row in rows {
                        dlq_entries.push(DlqEntry {
                            source_row: row.row_num,
                            category: crate::dlq::DlqErrorCategory::Correlated,
                            error_message: format!("correlated with failure in group: {root_msg}"),
                            original_record: row.record,
                            stage: Some(format!("output:{out_name}")),
                            route: None,
                            trigger: false,
                        });
                        collateral_count += 1;
                    }
                }
                // Accept the walker's DLQ entries verbatim (they have
                // trigger: true / real categories already).
                dlq_entries.append(&mut group_dlq);
                counters.dlq_count += group_counters.dlq_count + collateral_count;
                counters.filtered_count += group_counters.filtered_count;
                counters.distinct_count += group_counters.distinct_count;
            }
        }

        // Phase 3: flush accumulated successful-group captures to real
        // writers. One FormatWriter per output spans all successful
        // groups; records preserve source-row order within each group
        // and are emitted in group-insertion (IndexMap) order across
        // groups.
        for (out_name, rows) in successful_captures {
            if rows.is_empty() {
                continue;
            }
            let out_cfg = config
                .output_configs()
                .find(|o| o.name == out_name)
                .cloned()
                .ok_or_else(|| PipelineError::Internal {
                    op: "correlation_group_flush",
                    node: out_name.clone(),
                    detail: "Output node not in config".to_string(),
                })?;
            // The Output node's projection layout = its single
            // predecessor's entry in `plan.output_layouts`. Planner
            // populated the map for every top-level + synthesized
            // passthrough node at compile time; a direct lookup is
            // authoritative. No on-demand graph walk needed.
            let layout = plan
                .graph
                .node_indices()
                .find(|&i| plan.graph[i].name() == out_name)
                .and_then(|idx| {
                    plan.graph
                        .neighbors_directed(idx, petgraph::Direction::Incoming)
                        .find_map(|p| bound_output_layouts.get(plan.graph[p].name()).cloned())
                });
            let Some(layout) = layout else {
                // Edge case (direct Source → Output with no wired
                // predecessor): write nothing, mirroring Streamed-mode.
                continue;
            };
            let Some(raw_writer) = writers.remove(&out_name) else {
                continue;
            };
            let first = &rows[0];
            let first_projected =
                project_output_with_meta(&first.record, &layout, &first.metadata, &out_cfg);
            let mut w =
                build_format_writer(&out_cfg, raw_writer, Arc::clone(first_projected.schema()))?;
            w.write_record(&first_projected)?;
            for row in &rows[1..] {
                let projected =
                    project_output_with_meta(&row.record, &layout, &row.metadata, &out_cfg);
                w.write_record(&projected)?;
            }
            w.flush()?;
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
    #[allow(clippy::too_many_arguments)]
    fn execute_dag_branching(
        config: &PipelineConfig,
        all_records: Vec<(Record, u64)>,
        mut sink: OutputSink<'_>,
        transforms: &[CompiledTransform],
        // Option W Shortcut #2: passed by `&mut` rather than owned so
        // the per-group correlation-key driver can invoke the walker N
        // times against the same CompiledRoute (route evaluators carry
        // per-record mutable state). Serial per-group execution means
        // exclusive borrow across calls is sound.
        mut compiled_route: Option<&mut CompiledRoute>,
        plan: &ExecutionPlanDag,
        params: &PipelineRunParams,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        collector: &mut stage_metrics::StageCollector,
        lookup_tables: &HashMap<String, RuntimeLookup>,
        bound_output_schemas: &HashMap<String, Arc<Schema>>,
        bound_output_layouts: &HashMap<String, Arc<cxl::typecheck::OutputLayout>>,
        arena: Option<&Arena>,
        indices: Option<&[SecondaryIndex]>,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, Option<u64>), PipelineError> {
        let _ = arena;
        let _ = indices;
        use petgraph::graph::NodeIndex;

        let source_configs: Vec<_> = config.source_configs().cloned().collect();
        let output_configs: Vec<_> = config.output_configs().cloned().collect();
        let input = &source_configs[0];
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
        // Option-W: per-node buffer tuple is (record, row_num, metadata).
        // Emits live positionally on the record via its bound output
        // schema; the per-record emitted side-map has been eliminated
        // (single oracle, Failure mode #5).
        type NodeRow = (Record, u64, IndexMap<String, Value>);
        let mut node_buffers: HashMap<NodeIndex, Vec<NodeRow>> = HashMap::new();

        // Walk DAG in topological order
        for &node_idx in &plan.topo_order {
            let node = plan.graph[node_idx].clone();
            match node {
                PlanNode::Source { ref name, .. } => {
                    // Option-W: widen source records onto the source's
                    // bound output schema (the author-declared schema),
                    // so every record produced by this node carries
                    // the same `Arc<Schema>` that downstream positional
                    // consumers (aggregator `group_by_indices`, sort
                    // keys) resolve against.
                    //
                    // The CSV/JSON reader produces records whose
                    // schema reflects what's actually in the file
                    // (a subset of the declared schema); `bind_schema`
                    // walks the author-declared schema (a superset).
                    // Left unreconciled, indices resolve at the wrong
                    // slot — widen here once at source ingress.
                    let source_schema = bound_output_schemas.get(name.as_str()).cloned();
                    let records: Vec<_> = all_records
                        .iter()
                        .map(|(r, rn)| {
                            let widened = if let Some(schema) = source_schema.as_ref() {
                                widen_to_schema(r, schema)
                            } else {
                                r.clone()
                            };
                            (widened, *rn, IndexMap::<String, Value>::new())
                        })
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

                    // Option-W: every record emitted by this Transform
                    // carries the node's bound output schema (single
                    // oracle — the same Arc as transforms[i].typed
                    // .output_layout.schema).
                    let node_output_schema = bound_output_schemas
                        .get(name.as_str())
                        .cloned()
                        .expect("Option W: every CXL-bearing node has a bound output schema");

                    let mut output_records = Vec::with_capacity(input_records.len());

                    for (record, rn, mut all_metadata) in input_records {
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
                                            .eval_record::<NullStorage>(
                                                &ctx, &record, &resolver, None,
                                            )
                                            .map_err(|e| (name.clone(), e))
                                        {
                                            Ok(EvalResult::Emit { values, metadata }) => {
                                                let mut rec = Record::new(
                                                    Arc::clone(&node_output_schema),
                                                    values,
                                                );
                                                if record.has_meta() {
                                                    for (k, v) in record.iter_meta() {
                                                        let _ = rec.set_meta(k, v.clone());
                                                    }
                                                }
                                                for (k, v) in &metadata {
                                                    let _ = rec.set_meta(k, v.clone());
                                                }
                                                all_metadata.extend(metadata);
                                                output_records.push((rec, rn, all_metadata));
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
                                        .eval_record::<NullStorage>(&ctx, &record, &resolver, None)
                                        .map_err(|e| (name.clone(), e))
                                    {
                                        Ok(EvalResult::Emit { values, metadata }) => {
                                            let mut rec = Record::new(
                                                Arc::clone(&node_output_schema),
                                                values,
                                            );
                                            if record.has_meta() {
                                                for (k, v) in record.iter_meta() {
                                                    let _ = rec.set_meta(k, v.clone());
                                                }
                                            }
                                            for (k, v) in &metadata {
                                                let _ = rec.set_meta(k, v.clone());
                                            }
                                            let mut mt = all_metadata.clone();
                                            mt.extend(metadata);
                                            output_records.push((rec, rn, mt));
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
                            // No lookup — single-transform evaluation.
                            // Windowed transforms dispatch with a
                            // `PartitionWindowContext` derived from the
                            // arena + secondary index built in
                            // `execute_dag` before the walker runs; the
                            // record's position in the arena is passed as
                            // `rn - 1` (source row numbers are 1-based).
                            let window_idx =
                                plan.graph.node_weight(node_idx).and_then(|n| match n {
                                    PlanNode::Transform { window_index, .. } => *window_index,
                                    _ => None,
                                });
                            type TransformEvalResult = Result<
                                (Record, Result<IndexMap<String, Value>, SkipReason>),
                                (String, cxl::eval::EvalError),
                            >;
                            let eval_result: TransformEvalResult = {
                                let _guard = transform_timer.guard();
                                if let (Some(widx), Some(arena_ref), Some(indices_ref)) =
                                    (window_idx, arena, indices)
                                {
                                    let record_pos = rn.saturating_sub(1) as u32;
                                    let spec = &plan.indices_to_build[widx];
                                    let index = &indices_ref[widx];
                                    let key: Option<Vec<GroupByKey>> = spec
                                        .group_by
                                        .iter()
                                        .map(|field| {
                                            let val =
                                                record.get(field).cloned().unwrap_or(Value::Null);
                                            value_to_group_key(&val, field, None, record_pos)
                                                .ok()
                                                .flatten()
                                        })
                                        .collect();
                                    let result: Result<EvalResult, cxl::eval::EvalError> =
                                        if let Some(key) = key {
                                            if let Some(partition) = index.get(&key) {
                                                let pos_in_partition = partition
                                                    .iter()
                                                    .position(|&p| p == record_pos)
                                                    .unwrap_or(0);
                                                let wctx = PartitionWindowContext::new(
                                                    arena_ref,
                                                    partition,
                                                    pos_in_partition,
                                                );
                                                evaluator.eval_record(
                                                    &ctx,
                                                    &record,
                                                    &record,
                                                    Some(&wctx),
                                                )
                                            } else {
                                                evaluator.eval_record::<Arena>(
                                                    &ctx, &record, &record, None,
                                                )
                                            }
                                        } else {
                                            evaluator
                                                .eval_record::<Arena>(&ctx, &record, &record, None)
                                        };
                                    match result {
                                        Ok(EvalResult::Emit { values, metadata }) => {
                                            let mut out_record = Record::new(
                                                Arc::clone(&node_output_schema),
                                                values,
                                            );
                                            if record.has_meta() {
                                                for (k, v) in record.iter_meta() {
                                                    let _ = out_record.set_meta(k, v.clone());
                                                }
                                            }
                                            for (k, v) in &metadata {
                                                let _ = out_record.set_meta(k, v.clone());
                                            }
                                            Ok((out_record, Ok(metadata)))
                                        }
                                        Ok(EvalResult::Skip(reason)) => {
                                            Ok((record.clone(), Err(reason)))
                                        }
                                        Err(e) => Err((name.clone(), e)),
                                    }
                                } else {
                                    evaluate_single_transform(
                                        &record,
                                        name,
                                        &mut evaluator,
                                        &ctx,
                                        &node_output_schema,
                                    )
                                }
                            };
                            match eval_result {
                                Ok((modified_record, Ok(metadata))) => {
                                    all_metadata.extend(metadata);
                                    output_records.push((modified_record, rn, all_metadata));
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
                    //
                    // Two successor shapes are supported:
                    //
                    // 1. **Branch transforms** whose `input:` is
                    //    "route_parent.branch_name" (classic fan-out:
                    //    Route → Transform → Output). The branch name is
                    //    the suffix after the route parent.
                    //
                    // 2. **Output nodes** directly downstream of the Route
                    //    whose name matches a branch name in the route
                    //    config (multi-output split: Route → Output[name]).
                    //    This is the shape used by
                    //    `route: { conditions: { high: ... }, default: low }`
                    //    with two `output` nodes named `high` and `low`.
                    let route_parent = name.strip_prefix("route_").unwrap_or(name);
                    let mut branch_to_succ: HashMap<String, NodeIndex> = HashMap::new();
                    for &succ in &successors {
                        let succ_name = plan.graph[succ].name().to_string();
                        let mut matched = false;
                        for tc in crate::executor::build_transform_specs(config) {
                            if tc.name == succ_name
                                && let Some(crate::config::TransformInput::Single(ref input_ref)) =
                                    tc.input
                                && let Some(branch) =
                                    input_ref.strip_prefix(&format!("{}.", route_parent))
                            {
                                branch_to_succ.insert(branch.to_string(), succ);
                                matched = true;
                            }
                        }
                        // Shape 2: Output node directly downstream of Route
                        // — the output's name IS the branch label.
                        if !matched && matches!(plan.graph[succ], PlanNode::Output { .. }) {
                            branch_to_succ.insert(succ_name, succ);
                        }
                    }

                    // Initialize per-successor buffers
                    let mut branch_buffers: HashMap<NodeIndex, Vec<_>> = HashMap::new();
                    for &succ in &successors {
                        branch_buffers.insert(succ, Vec::new());
                    }

                    // Use compiled_route to evaluate conditions
                    if let Some(ref mut route) = compiled_route {
                        for (record, rn, all_metadata) in input_records {
                            let ctx = EvalContext {
                                stable: &stable,
                                source_file: &source_file_arc,
                                source_row: rn,
                            };

                            let route_result = {
                                let _guard = route_timer.guard();
                                route.evaluate(&record, &all_metadata, &ctx)
                            };
                            match route_result {
                                Ok(targets) => {
                                    for target in &targets {
                                        if let Some(&succ) = branch_to_succ.get(target.as_str()) {
                                            branch_buffers.entry(succ).or_default().push((
                                                record.clone(),
                                                rn,
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
                    let _ = name;
                    let _ = sort_fields;
                    // Enforcer-sort dispatch (Task 16.0.5.8). Reuses the
                    // generalized `SortBuffer<P>` carrying per-record sidecar
                    // (row_num + emitted/accumulated metadata maps) through
                    // the sort permutation. See
                    // docs/research/RESEARCH-sort-node-sidecar-payload.md.
                    use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};

                    type SortPayload = (u64, IndexMap<String, Value>);
                    type SortRow = (Record, u64, IndexMap<String, Value>);

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
                    for (record, row_num, accumulated) in input_records {
                        buf.push(record, (row_num, accumulated));
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
                            for (record, (row_num, accumulated)) in pairs {
                                out.push((record, row_num, accumulated));
                            }
                        }
                        SortedOutput::Spilled(files) => {
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
                                    let (record, (row_num, accumulated)) = entry.map_err(|e| {
                                        PipelineError::Io(std::io::Error::other(format!(
                                            "sort enforcer '{name}' spill decode failed: {e}"
                                        )))
                                    })?;
                                    out.push((record, row_num, accumulated));
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
                    for (record, row_num, accumulated) in &input {
                        let ctx = EvalContext {
                            stable: &stable,
                            source_file: &source_file_arc,
                            source_row: *row_num,
                        };
                        if let Err(e) = stream.add_record(
                            record,
                            *row_num,
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
                                let synthetic = if let Some((rec, _, _)) = input.first() {
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
                    //
                    // Locate the predecessor node so we can fetch its
                    // canonical `OutputLayout` from `bound_output_layouts`
                    // (Option W: the predecessor's layout is the single
                    // oracle for the CXL-emitted vs passthrough distinction
                    // that drives `include_unmapped: false` semantics).
                    let predecessors: Vec<NodeIndex> = plan
                        .graph
                        .neighbors_directed(node_idx, Direction::Incoming)
                        .collect();
                    let input_records = if let Some(own_buf) = node_buffers.remove(&node_idx) {
                        own_buf
                    } else {
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

                    // Option W Amendment A7: use the predecessor's bound
                    // `OutputLayout` as the single oracle for projection.
                    // For Transform/Aggregate/Route predecessors the layout
                    // carries real `emit_slots` (CXL-named columns); for
                    // Source/Merge predecessors it's an identity-passthrough
                    // layout. `include_unmapped: false` correctly filters
                    // to emits-only because the layout knows the truth.
                    //
                    // Edge case: a direct Source → Output pipeline (no
                    // transform between them) has NO edge wired in the
                    // plan DAG — the planner's output-wiring logic at
                    // `plan/execution.rs:1273-1297` only connects Output
                    // nodes when `prev_transform_key` is Some. In that
                    // case `input_records` is empty (handled below) and
                    // no projection happens. We mirror this by only
                    // looking up a layout when we actually have records.
                    let layout = predecessors
                        .first()
                        .map(|&p| plan.graph[p].name())
                        .and_then(|pred_name| bound_output_layouts.get(pred_name));

                    match sink {
                        OutputSink::Captured(ref mut cap) => {
                            // Correlation-key pre-DAG driver: buffer raw
                            // records pending group success/failure. No
                            // projection, no write — caller decides.
                            let buf = cap.entry(name.clone()).or_default();
                            for (record, rn, metadata) in &input_records {
                                buf.push(CapturedRow {
                                    record: record.clone(),
                                    row_num: *rn,
                                    metadata: metadata.clone(),
                                });
                            }
                            collector.record(
                                scan_timer
                                    .finish(input_records.len() as u64, input_records.len() as u64),
                            );
                        }
                        OutputSink::Streamed(ref mut writers) => {
                            let output_schema = match (input_records.first(), layout) {
                                (Some((rec, _, metadata)), Some(layout)) => {
                                    let projected = {
                                        let _guard = projection_timer.guard();
                                        project_output_with_meta(rec, layout, metadata, out_cfg)
                                    };
                                    Arc::clone(projected.schema())
                                }
                                _ => {
                                    // No records or no predecessor — nothing
                                    // to write. Record the scan metric and
                                    // continue.
                                    collector.record(scan_timer.finish(0, 0));
                                    continue;
                                }
                            };
                            let layout = layout.expect(
                                "Option W invariant: layout lookup succeeded \
                                 above implies predecessor in bound_output_layouts",
                            );

                            if let Some(raw_writer) = writers.remove(name) {
                                let mut csv_writer = build_format_writer(
                                    out_cfg,
                                    raw_writer,
                                    Arc::clone(&output_schema),
                                )?;
                                collector.record(scan_timer.finish(1, 1));

                                for (record, _rn, metadata) in &input_records {
                                    let projected = {
                                        let _guard = projection_timer.guard();
                                        project_output_with_meta(record, layout, metadata, out_cfg)
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

/// Extract the correlation-key value(s) from a record.
///
/// For a compound key (`CorrelationKey::Compound(["a", "b"])`) returns
/// one `GroupByKey` per field. Missing or null values — as well as
/// empty strings (CSV's null stand-in) — produce `GroupByKey::Null`.
///
/// Re-instated from commit `74a06c7` as part of Option W Shortcut #2
/// (correlation-key DLQ failure-domain batching).
fn extract_correlation_key(
    record: &Record,
    correlation_key: &crate::config::CorrelationKey,
) -> Vec<GroupByKey> {
    let fields = correlation_key.fields();
    fields
        .iter()
        .map(|field| match record.get(field) {
            Some(value) if !value.is_null() => {
                // Treat empty strings as null (CSV has no native null concept).
                if let Value::String(s) = value
                    && s.is_empty()
                {
                    return GroupByKey::Null;
                }
                match value_to_group_key(value, field, None, 0) {
                    Ok(Some(gk)) => gk,
                    Ok(None) => GroupByKey::Null,
                    Err(_) => GroupByKey::Null,
                }
            }
            _ => GroupByKey::Null,
        })
        .collect()
}

/// Format a compound group key for diagnostics (e.g., collateral DLQ
/// messages, `CorrelationGroupOverflow` carriage).
fn format_group_key(key: &[GroupByKey]) -> String {
    key.iter()
        .map(|k| format!("{k:?}"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Option-W source widening: materialize a record onto the declared
/// source schema. The reader-produced record's schema reflects what was
/// in the file (possibly a subset of declared columns); this helper
/// pads to the full declared schema positionally. Replaces the former
/// `reproject_record(input, schema, &IndexMap::new())` source-arm call.
fn widen_to_schema(input: &Record, target: &Arc<Schema>) -> Record {
    if Arc::ptr_eq(input.schema(), target) {
        return input.clone();
    }
    let values: Vec<Value> = target
        .columns()
        .iter()
        .map(|c| input.get(c.as_ref()).cloned().unwrap_or(Value::Null))
        .collect();
    let mut rec = Record::new(Arc::clone(target), values);
    if input.has_meta() {
        for (k, v) in input.iter_meta() {
            let _ = rec.set_meta(k, v.clone());
        }
    }
    rec
}

/// Apply an eval result to a branch (Option-W positional). The
/// evaluator's output `values: Vec<Value>` is positionally aligned to
/// the producing node's `OutputLayout::schema`, which is passed in as
/// `output_schema`.
/// Evaluate a single transform against a record (Option-W positional).
///
/// The evaluator writes its output positionally into `values: Vec<Value>`
/// aligned to the transform's bound output schema (`output_schema`).
/// Callers materialize the output record via `Record::new(output_schema,
/// values)` — no reprojection, no name lookups.
///
/// Returns `Ok((output_record, Ok(metadata)))` on emit,
/// `Ok((input_clone, Err(SkipReason)))` on skip.
/// On error, returns `(transform_name, EvalError)`.
#[allow(clippy::type_complexity)]
fn evaluate_single_transform(
    record: &Record,
    transform_name: &str,
    evaluator: &mut ProgramEvaluator,
    ctx: &EvalContext,
    output_schema: &Arc<Schema>,
) -> Result<(Record, Result<IndexMap<String, Value>, SkipReason>), (String, cxl::eval::EvalError)> {
    match evaluator
        .eval_record::<NullStorage>(ctx, record, record, None)
        .map_err(|e| (transform_name.to_string(), e))?
    {
        EvalResult::Emit { values, metadata } => {
            let mut out_record = Record::new(Arc::clone(output_schema), values);
            if record.has_meta() {
                for (k, v) in record.iter_meta() {
                    let _ = out_record.set_meta(k, v.clone());
                }
            }
            for (key, value) in &metadata {
                let _ = out_record.set_meta(key, value.clone());
            }
            Ok((out_record, Ok(metadata)))
        }
        EvalResult::Skip(reason) => Ok((record.clone(), Err(reason))),
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
