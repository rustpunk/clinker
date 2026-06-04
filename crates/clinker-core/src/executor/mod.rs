pub mod stage_metrics;

pub(crate) mod aggregate_dispatch;
pub(crate) mod batch_handoff;
pub mod combine;
pub(crate) mod combine_dispatch;
pub(crate) mod commit;
pub(crate) mod composition_dispatch;
pub(crate) mod correlation_dispatch;
pub(crate) mod dispatch;
mod dlq;
mod ingest;
pub(crate) mod merge_dispatch;
pub mod node_buffer;
pub(crate) mod node_buffer_spill;
pub(crate) mod output_dispatch;
mod params;
mod registry;
mod route;
pub(crate) mod route_dispatch;
mod schema_check;
pub(crate) mod sort_dispatch;
pub(crate) mod source_dispatch;
pub mod source_stream;
pub(crate) mod stream_event;
pub(crate) mod time_window;
pub(crate) mod transform;
pub(crate) mod transform_dispatch;
pub(crate) mod watermark;
pub(crate) mod window_runtime;

pub use dlq::DlqEntry;
use ingest::{IngestTaskOutcome, ingest_source};
use params::sum_cpu_io_totals;
pub use params::{ExecutionReport, PipelineRunParams};
pub use registry::WriterRegistry;
pub(crate) use registry::build_format_writer;
pub(crate) use route::{CompiledRoute, CompiledRouteBranch};
pub(crate) use transform::CompiledTransform;
pub use transform::{TransformSpec, build_transform_specs};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write};
use std::sync::Arc;

use chrono::Utc;
use clinker_record::{PipelineCounters, Record, RecordStorage, Schema, SchemaBuilder, Value};
use indexmap::IndexMap;

use crate::config::{OutputConfig, PipelineConfig};
use crate::error::PipelineError;
use crate::pipeline::arena::Arena;
use crate::pipeline::index::{GroupByKey, value_to_group_key};
use crate::pipeline::memory::rss_bytes;
use crate::pipeline::window_context::PartitionWindowContext;
use crate::plan::execution::ExecutionPlanDag;
use clinker_format::traits::FormatWriter;
use cxl::eval::{
    EvalContext, EvalResult, ProgramEvaluator, SkipReason, StableEvalContext, WallClock,
};
use cxl::typecheck::Type;
use petgraph::Direction;

/// Map from source-node name to the input feeding that source.
///
/// The value is a [`crate::source::SourceInput`], generalized off the
/// file-slot model so a non-file transport registers without being
/// forced through the file abstractions. The `Files` arm carries the
/// ordered file slots (one per matched file; the executor concatenates
/// them via [`crate::source::multi_file::MultiFileFormatReader`] and
/// stamps each record with its originating file); the `Records` arm
/// carries a ready-to-drive [`crate::source::RecordSource`]. Both arms
/// feed the identical `SourceIngestChannel` — the dispatcher never
/// branches on transport.
pub type SourceReaders = HashMap<String, crate::source::SourceInput>;

/// Re-export so callers building a [`SourceReaders`] reach the transport
/// variants and the row-yielding contract through the same module that
/// owns the registry alias and [`single_file_reader`].
pub use crate::source::{RecordSource, SourceInput};

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
    /// High-water mark of `MemoryArbitrator::sum_consumer_usage()` sampled
    /// at every streaming per-batch charge. A streaming stage's peak stays
    /// bounded to one in-flight batch (plus the channel's bound), proving
    /// the per-batch admit/discharge model never charges the whole stage.
    pub(crate) peak_consumer_usage_bytes: u64,
    /// `true` when a chunk-boundary shutdown poll tripped and the topo
    /// walk unwound early. Carried up so the report surfaces the
    /// interrupted state to the CLI.
    pub(crate) interrupted: bool,
}

/// Helper for callers (mostly tests and benchmarks) that have a single
/// in-memory reader per source: wraps it in the one-element file-slot
/// [`crate::source::SourceInput::Files`] shape the reader registry
/// expects, hiding the transport variant so the common single-file case
/// registers in one call.
pub fn single_file_reader(
    path: impl Into<std::path::PathBuf>,
    reader: Box<dyn Read + Send>,
) -> crate::source::SourceInput {
    crate::source::SourceInput::Files(vec![crate::source::multi_file::FileSlot {
        path: path.into(),
        reader,
    }])
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
    /// The executor is fully synchronous: it drives source ingest on
    /// `std::thread` workers, runs CPU-bound operator kernels (sort,
    /// grace-hash, IEJoin, sort-merge) on a shared Rayon pool, and
    /// blocks the calling thread on bounded crossbeam channels for
    /// back-pressure. No async runtime is required.
    pub fn run_plan_with_readers_writers<W: Into<WriterRegistry>>(
        plan: &crate::plan::CompiledPlan,
        readers: SourceReaders,
        writers: W,
        params: &PipelineRunParams,
    ) -> Result<ExecutionReport, PipelineError> {
        Self::run_with_readers_writers(plan.config(), readers, writers.into(), params)
    }

    /// Run a compiled plan resolving source paths against an explicit
    /// compile anchor.
    ///
    /// The executor re-derives per-node volume estimates from on-disk file
    /// sizes during its run-time compile, and those estimates drive the
    /// memory-aware dispatch order. Resolving against `compile_ctx` — the
    /// same anchor `--explain` used — keeps the run's dispatch order
    /// identical to the surfaced predictions and independent of the process
    /// CWD; [`run_plan_with_readers_writers`](Self::run_plan_with_readers_writers)
    /// anchors at the CWD via `CompileContext::default()`, which is correct
    /// only for callers whose sources are absent on disk (estimates resolve
    /// to `0`, so dispatch falls back to topological order). A file-backed
    /// run must come through here so the scheduler sees the same byte sizes
    /// it would read.
    ///
    /// # Errors
    ///
    /// Surfaces every failure of the underlying run: compilation diagnostics,
    /// reader/writer setup errors, and runtime operator failures.
    pub fn run_plan_with_readers_writers_in_context<W: Into<WriterRegistry>>(
        plan: &crate::plan::CompiledPlan,
        readers: SourceReaders,
        writers: W,
        params: &PipelineRunParams,
        compile_ctx: crate::config::CompileContext,
    ) -> Result<ExecutionReport, PipelineError> {
        Self::run_with_readers_writers_in_context(
            plan.config(),
            readers,
            writers.into(),
            params,
            compile_ctx,
        )
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
    pub(crate) fn run_with_readers_writers(
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
    }

    /// Variant of [`Self::run_with_readers_writers`] that accepts an
    /// explicit [`crate::config::CompileContext`]. Tests use this to
    /// supply a temp-dir workspace root without mutating CWD —
    /// `CompileContext::default()` reads CWD at call time, which is
    /// not thread-safe across parallel test runs that need different
    /// workspace roots.
    ///
    /// Builds the pipeline-scoped [`MemoryArbitrator`] from `config` and
    /// delegates to [`Self::run_with_readers_writers_with_arbitrator`],
    /// the single owner of the run body. The arbitrator is the only
    /// process-wide RSS authority for the run; constructing it here keeps
    /// production call sites free of any test-only seam.
    ///
    /// [`MemoryArbitrator`]: crate::pipeline::memory::MemoryArbitrator
    pub(crate) fn run_with_readers_writers_in_context(
        config: &PipelineConfig,
        readers: SourceReaders,
        writers: WriterRegistry,
        params: &PipelineRunParams,
        compile_ctx: crate::config::CompileContext,
    ) -> Result<ExecutionReport, PipelineError> {
        let memory_budget = std::sync::Arc::new(build_arbitrator_from_config(config));
        Self::run_with_readers_writers_with_arbitrator(
            config,
            readers,
            writers,
            params,
            compile_ctx,
            memory_budget,
        )
    }

    /// Owns the entire run body: compile, source ingest, DAG dispatch,
    /// and report assembly. Takes the pipeline-scoped
    /// [`MemoryArbitrator`] as a parameter rather than constructing it,
    /// so a caller can seed `peak_rss` (via the test hook) before the run
    /// drives the RSS-based abort and disk-spill-quota gates. Production
    /// reaches this only through
    /// [`Self::run_with_readers_writers_in_context`], which builds the
    /// arbitrator from `config` and delegates.
    ///
    /// [`MemoryArbitrator`]: crate::pipeline::memory::MemoryArbitrator
    pub(crate) fn run_with_readers_writers_with_arbitrator(
        config: &PipelineConfig,
        mut readers: SourceReaders,
        writers: WriterRegistry,
        params: &PipelineRunParams,
        compile_ctx: crate::config::CompileContext,
        memory_budget: std::sync::Arc<crate::pipeline::memory::MemoryArbitrator>,
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
        // arenas built at Source dispatch-arm exits. Arrives as a
        // parameter (built by the caller before the source loop) so each
        // Source can register a `SourceConsumer` with it at its
        // construction site instead of after-the-fact discovery. The
        // active policy is the one `pipeline.memory.backpressure`
        // selects; default `pause` installs `BackPressurePreferred ->
        // Priority`.

        // ── Unified source ingest pass ───────────────────────────────
        // Every declared Source spawns one `std::thread` that drives its
        // format reader and pushes records through a
        // `SourceIngestChannel`. The paired crossbeam `Receiver` lands in
        // `source_records[name]`; the dispatch loop's Source arm drains
        // it via `recv`. No primary asymmetry; missing reader or empty
        // file list is a hard error. Per-source totals + per-(source,
        // file) watermark observations are returned through each thread's
        // `JoinHandle` and folded into the run's accounting once the
        // receivers have drained.
        let reader_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::ReaderInit);
        let counters = PipelineCounters::default();
        let mut source_records: HashMap<
            String,
            crossbeam_channel::Receiver<crate::executor::stream_event::StreamEvent>,
        > = HashMap::new();
        let mut watermarks = crate::executor::watermark::PerSourceWatermarks::new();
        let mut ingest_handles: Vec<
            std::thread::JoinHandle<Result<IngestTaskOutcome, PipelineError>>,
        > = Vec::with_capacity(source_configs.len());
        for src_cfg in &source_configs {
            // Pre-declare so the report's `iter_declared_sources` view
            // emits a per-source rollup entry even when ingest produces
            // zero observable records (e.g. empty input file).
            if src_cfg.watermark.is_some() {
                watermarks.declare(&src_cfg.name);
            }
            let source_input = readers.remove(&src_cfg.name).ok_or_else(|| {
                PipelineError::Config(crate::config::ConfigError::Validation(format!(
                    "no reader registered for source '{}'",
                    src_cfg.name
                )))
            })?;
            // Single ConsumerHandle shared between the SourceConsumer
            // wrapper (BackPressurePreferred / Priority pause target)
            // and the SourceIngestChannel that mirrors the channel queue
            // depth × per-record bytes into the handle's counter on
            // every `push`. The arbitrator owns the boxed wrapper for the
            // run; arbitrator Drop releases it on pipeline teardown.
            let source_consumer_handle = crate::pipeline::memory::ConsumerHandle::new();
            let (stream, rx) = crate::executor::source_stream::SourceIngestChannel::new(
                crate::executor::source_stream::SourceIngestChannel::DEFAULT_CAPACITY,
                source_consumer_handle.clone(),
            );
            let _source_consumer_id = memory_budget.register_consumer(Arc::new(
                crate::executor::source_stream::SourceConsumer::new(source_consumer_handle),
            ));
            source_records.insert(src_cfg.name.clone(), rx);
            let src_cfg_owned = src_cfg.clone();
            let config_clone = config.clone();
            // Per-thread clone of the run's cancellation handle. A network
            // ingest reader polls it at page/row-batch boundaries to stop
            // within the documented shutdown bound; the file arm ignores
            // it (dropped-receiver stop suffices).
            let ingest_shutdown = params.shutdown_token.clone();
            // One OS thread per Source. Spawned before the DAG dispatch
            // drains so the producers fill the bounded channels while the
            // consumer dispatch loop runs concurrently. Joined after
            // dispatch returns (receivers already drained).
            let handle = std::thread::Builder::new()
                .name(format!("clinker-ingest-{}", src_cfg.name))
                .spawn(move || {
                    ingest_source(
                        src_cfg_owned,
                        source_input,
                        config_clone,
                        stream,
                        ingest_shutdown,
                    )
                })
                .map_err(|e| PipelineError::Internal {
                    op: "source-ingest-spawn",
                    node: src_cfg.name.clone(),
                    detail: format!("failed to spawn source ingest thread: {e}"),
                })?;
            ingest_handles.push(handle);
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
            peak_consumer_usage_bytes,
            interrupted,
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
        )?;

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
            // `join()` Err is the panic payload (`Box<dyn Any>`); the
            // inner `??` then unwraps the ingest fn's own
            // `Result<IngestTaskOutcome, PipelineError>`.
            let outcome = handle.join().map_err(|_| PipelineError::Internal {
                op: "source-ingest-thread",
                node: String::new(),
                detail: String::from("source ingest thread panicked"),
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
            peak_consumer_usage_bytes,
            interrupted,
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
    /// `source_records` holds one live crossbeam `Receiver` per declared
    /// Source. Each `(Record, row_num)` carries the source-file
    /// `Arc<str>` engine-stamped on the `$source.file` column. The
    /// caller's ingest pass spawns one `std::thread` per Source to push
    /// through a paired `SourceIngestChannel`; this function consumes
    /// the receivers via `recv` and never touches a `FormatReader`
    /// directly.
    ///
    /// Returns `(counters, dlq_entries, peak_rss_bytes)`.
    #[allow(clippy::too_many_arguments)]
    fn execute_dag(
        config: &PipelineConfig,
        source_records: HashMap<
            String,
            crossbeam_channel::Receiver<crate::executor::stream_event::StreamEvent>,
        >,
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
        // is the first consumer of every crossbeam `Receiver`:
        // - canonicalize per record onto the source's plan-time schema,
        // - seed `$record.<key>` defaults per record,
        // - seed `$source.<key>` defaults per `(source, file)` Arc on
        //   first observation,
        // - on `recv` returning `Err` (channel disconnected), stamp the
        //   finalized per-source count and call
        //   `finalize_node_rooted_windows` to populate every spec rooted
        //   at this source's NodeIndex.
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
        source_records: HashMap<
            String,
            crossbeam_channel::Receiver<crate::executor::stream_event::StreamEvent>,
        >,
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
        // Source dispatch arm, which uses `rx.recv_timeout` when a
        // source has a configured timeout. Sources without one fall
        // through to blocking `rx.recv`.
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
        // `crossbeam_channel::Select` so a slow upstream Source no longer
        // blocks peer Sources' channels from filling — back-pressure
        // flows end-to-end. Per-Source arms detect membership in
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

        // Shared Rayon pool for the CPU-bound owned-input kernels (sort,
        // grace-hash partition build, IEJoin, sort-merge). Sized off the
        // pipeline's `concurrency.threads` knob; `0`/absent defers to
        // Rayon's default (one worker per logical CPU). Built once per
        // run and shared via `Arc` so every kernel `install` reuses the
        // same worker set rather than spinning up a pool per operator.
        let kernel_pool = {
            let mut builder = rayon::ThreadPoolBuilder::new();
            if let Some(n) = config.pipeline.concurrency.as_ref().and_then(|c| c.threads)
                && n > 0
            {
                builder = builder.num_threads(n);
            }
            std::sync::Arc::new(
                builder
                    .build()
                    .map_err(|e| PipelineError::ThreadPool(e.to_string()))?,
            )
        };

        // Streaming-Output setup (issue #72). For every fused
        // `Merge.interleave → single Output` chain that satisfies the
        // eligibility predicate, take the writer out of `writers.single`
        // and spawn one `std::thread` that drains a bounded crossbeam
        // channel. The Merge arm streams records into the channel as it
        // produces them; the Output arm's topo turn becomes a no-op
        // because its writer has already moved into the streaming thread.
        // `JoinHandle`s are stored on the context so the dispatcher can
        // join them at end-of-DAG and fold the per-thread counter /
        // timer / error accounting back into the context.
        let streaming_specs = compute_streaming_output_specs(
            plan,
            config,
            &fused_transforms,
            &init_phase_set,
            &output_configs,
            &writers,
        );
        let mut streaming_output_senders: HashMap<
            petgraph::graph::NodeIndex,
            crossbeam_channel::Sender<crate::executor::stream_event::StreamEvent>,
        > = HashMap::new();
        let mut streaming_output_nodes: HashSet<petgraph::graph::NodeIndex> = HashSet::new();
        let mut streaming_output_tasks: Vec<std::thread::JoinHandle<StreamingOutputTaskOutput>> =
            Vec::new();
        let mut streaming_charge_consumers: HashMap<
            petgraph::graph::NodeIndex,
            (
                crate::pipeline::memory::ConsumerId,
                Arc<crate::pipeline::memory::ConsumerHandle>,
            ),
        > = HashMap::new();
        for spec in streaming_specs {
            let raw_writer = writers
                .single
                .remove(&spec.output_name)
                .expect("compute_streaming_output_specs verified writers.single contains output");
            // 256 is the bounded channel capacity. The writer thread
            // typically clears each record in microseconds; capacity
            // above ~256 buys no measured throughput but burns memory
            // on the slow-writer / fast-merge end of the back-pressure
            // curve. Mirrors the Source ingest channel sizing (issue
            // #67) — the same bound paces both ends of the pipeline.
            let (tx, rx) =
                crossbeam_channel::bounded::<crate::executor::stream_event::StreamEvent>(256);
            let producer_idx = spec.producer_idx;
            let output_idx = spec.output_idx;
            let output_name = spec.output_name.clone();
            // Register exactly one charge consumer per streaming slot. The
            // producer arm `add_bytes` each flushed batch into this
            // handle; the writer thread holds a clone and `sub_bytes` each
            // consumed record, so the live count is "batches in flight,"
            // never the whole stage. `can_back_pressure` is conservatively
            // false: the bounded channel already paces the producer, and
            // the static pause-reachability analysis that would let the
            // arbitrator prefer pausing this slot over spilling has not
            // landed — matching `admit_node_buffer`'s posture.
            let charge_handle = crate::pipeline::memory::ConsumerHandle::new();
            let charge_consumer_id = memory_budget.register_consumer(Arc::new(
                crate::executor::node_buffer::NodeBufferConsumer::new(charge_handle.clone(), false),
            ));
            let writer_charge_handle = charge_handle.clone();
            let handle = std::thread::Builder::new()
                .name(format!("clinker-output-{output_name}"))
                .spawn(move || streaming_output(rx, raw_writer, spec, writer_charge_handle))
                .map_err(|e| PipelineError::Internal {
                    op: "streaming-output-spawn",
                    node: output_name,
                    detail: format!("failed to spawn streaming output thread: {e}"),
                })?;
            streaming_output_senders.insert(producer_idx, tx);
            streaming_output_nodes.insert(output_idx);
            streaming_output_tasks.push(handle);
            streaming_charge_consumers.insert(producer_idx, (charge_consumer_id, charge_handle));
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
            node_buffer_consumer_ids: HashMap::new(),
            window_arena_consumer_ids: HashMap::new(),
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
            streaming_charge_consumers,
            kernel_pool,
            shutdown_token: params.shutdown_token.clone(),
            interrupted: false,
            batch_size: config
                .pipeline
                .batch_size
                .unwrap_or(crate::executor::batch_handoff::DEFAULT_BATCH_SIZE),
        };

        // Resolve dispatch order through the memory arbitrator rather
        // than walking `topo_order` blindly. `scheduled_pass_order` runs
        // a frontier scheduler over each pass's candidate set: at every
        // step it asks `MemoryArbitrator::next_runnable` which runnable
        // node to dispatch by predicted memory impact (headroom fit, then
        // largest immediate freed-on-complete, then largest downstream
        // subtree reclaim, then stable topo index). With no
        // volume estimates the selection collapses to lowest topo index,
        // reproducing the prior front-to-back walk byte-for-byte; record
        // output is independent of dispatch order, so only peak resident
        // memory — never results — moves when estimates distinguish nodes.
        //
        // Two-pass orchestration for the init phase.
        // Pass 1 dispatches every node in the init-phase ancestor closure
        // (Source/Aggregate/etc. feeding `phase: init` Transforms, plus
        // the init-phase Transforms themselves). Pass 2 dispatches every
        // other node — the runtime DAG. Init-phase Transforms are
        // E164-validated as terminal, so Pass 2 never references their
        // output edge. The dispatcher's `remaining_consumers` heuristic
        // counts neighbors based on graph structure (pass-independent), so
        // a Source feeding both an init branch and a runtime branch
        // correctly clones its buffer for Pass 1's first consumer and
        // removes it for Pass 2's last consumer.
        //
        // The dispatch sequences are resolved up front (before the loop)
        // so each `scheduled_pass_order` call borrows `plan` only for its
        // own duration — `&mut ctx` already borrows `ctx.current_dag`
        // (which aliases `plan`) at the field granularity through every
        // dispatcher call, so the resolved `Vec<NodeIndex>` is what the
        // loop iterates instead of re-borrowing `plan` per step.
        //
        // Wrapped in an immediately-invoked closure so a `?` error inside
        // doesn't short-circuit past the streaming-output thread join
        // below — the spawned threads own writers we still need to flush
        // (or drop) before the function returns, and dropping
        // `ctx.streaming_output_senders` on the error path is what signals
        // the threads to disconnect their channel and run their flush. A
        // tripped shutdown token surfaces as `PipelineError::Interrupted`
        // from a per-node poll, which lands here too so the same
        // drain-then-join cleanup runs.
        let dispatch_sequence: Vec<petgraph::graph::NodeIndex> = if !init_phase_set.is_empty() {
            let runtime_set: HashSet<petgraph::graph::NodeIndex> = plan
                .topo_order
                .iter()
                .copied()
                .filter(|idx| !init_phase_set.contains(idx))
                .collect();
            let mut seq = scheduled_pass_order(plan, &ctx.memory_budget, &init_phase_set);
            seq.extend(scheduled_pass_order(plan, &ctx.memory_budget, &runtime_set));
            seq
        } else {
            let all: HashSet<petgraph::graph::NodeIndex> =
                plan.topo_order.iter().copied().collect();
            scheduled_pass_order(plan, &ctx.memory_budget, &all)
        };

        let walk_result: Result<(), PipelineError> = (|| {
            for node_idx in dispatch_sequence {
                ctx.check_shutdown()?;
                dispatch::dispatch_plan_node(&mut ctx, plan, node_idx)?;
            }
            Ok(())
        })();

        // Streaming-output drain. Drop every remaining sender BEFORE
        // joining so the writer threads' `rx.recv` returns `Err`
        // (channel disconnected) and they fall through to their flush
        // path — joining before dropping would deadlock, the thread
        // blocked on a `recv` that never disconnects. The fused Merge arm
        // normally removes its sender at clean exit; remaining entries
        // here are the error-/interrupt-path leftovers (Merge arm never
        // ran) or pipelines where no streaming chain was eligible (the
        // map is empty).
        ctx.streaming_output_senders.clear();
        for handle in std::mem::take(&mut ctx.streaming_output_tasks) {
            match handle.join() {
                Ok(out) => out.fold_into(&mut ctx),
                Err(_panic) => {
                    ctx.output_errors.push(PipelineError::Internal {
                        op: "streaming_output",
                        node: String::from("<unknown>"),
                        detail: String::from("streaming output thread panicked"),
                    });
                }
            }
        }

        // Every streaming writer has joined, so its discharge is
        // complete and no more batches will charge these slots.
        // Unregister each per-slot charge consumer so the arbitrator's
        // registry does not outlive the run with a stale wrapper reading
        // zero forever. The peak charged usage was already sampled at the
        // producer-side charges, so the report read survives this.
        for (_, (id, _)) in std::mem::take(&mut ctx.streaming_charge_consumers) {
            ctx.memory_budget.unregister_consumer(id);
        }

        // A tripped shutdown token unwinds the walk via
        // `PipelineError::Interrupted`; that is a graceful early stop, not
        // a failure, so swallow it here (the interruption is recorded in
        // `ctx.interrupted` and surfaced through the report) and let the
        // run finish draining. Every other walk error still propagates.
        match walk_result {
            Ok(()) => {}
            Err(PipelineError::Interrupted) => {}
            Err(other) => return Err(other),
        }

        // Top-scope teardown: the run has drained, so unregister every
        // window-runtime arena consumer that survived to the top scope.
        // Body-scope arenas already unregistered at body exit; this
        // sweeps the top-level (source-rooted and parent-scope node-
        // rooted) arenas so the arbitrator's registry does not outlive
        // the run with stale wrappers reading zero forever.
        for (_, (id, _)) in std::mem::take(&mut ctx.window_arena_consumer_ids) {
            ctx.memory_budget.unregister_consumer(id);
        }

        // A relaxed-CK aggregate that finalized successfully keeps its
        // aggregator parked across the commit phase's retract +
        // re-finalize iterations, so its consumer stays registered for
        // that whole window. Any state still present once the walk has
        // drained never degraded; drop it and unregister its consumer
        // here so the arbitrator's registry does not outlive the run
        // with the retained aggregator's bytes still summed in.
        for (_, retained) in std::mem::take(&mut ctx.relaxed_aggregator_states) {
            ctx.memory_budget.unregister_consumer(retained.consumer_id);
        }

        // Take the pipeline-scoped TempDir out of the context. Held
        // until the very end of the walk so any operator-side spill
        // files survive the topo loop; dropped explicitly below
        // after the metrics flush so the directory's recursive
        // remove runs after every reader has gone away.
        let pipeline_spill_root = ctx.spill_root().clone();

        // Drain mutable per-walk state out of the context for the
        // post-walk reporting + caller-facing return tuple. Sources are
        // drained lazily by the dispatch loop rather than in a pre-pass,
        // so the pipeline-wide ingest total is reconstructed here at exit
        // from the per-source running counters the Source dispatch arm
        // advanced.
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
        let peak_consumer_usage_bytes = ctx.memory_budget.peak_consumer_usage();
        let interrupted = ctx.interrupted;
        Ok(DispatchOutcome {
            counters: std::mem::take(counters),
            dlq_entries: std::mem::take(dlq_entries),
            peak_rss_bytes: rss_bytes(),
            watermarks: ctx.watermarks,
            per_source_rollback_cursors: rollback_cursors,
            per_source_record_counts,
            per_source_dlq_counts,
            cumulative_spill_bytes,
            peak_consumer_usage_bytes,
            interrupted,
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

/// Resolve the dispatch order for a topological pass via the memory
/// arbitrator's [`next_runnable`](crate::pipeline::memory::MemoryArbitrator::next_runnable)
/// instead of walking `topo_order` blindly.
///
/// `candidates` is the subset of nodes this pass dispatches — the
/// init-phase ancestor closure on Pass 1, its complement on Pass 2, or
/// the whole DAG when there is no init phase. The returned vector lists
/// those same nodes in the order the executor should dispatch them.
///
/// At each step the runnable frontier is every not-yet-emitted candidate
/// whose graph predecessors *within `candidates`* are all emitted. The
/// arbitrator picks one frontier node by predicted memory impact (headroom
/// fit, then largest immediate freed-on-complete, then largest downstream
/// subtree reclaim, then stable topo index); finishing the chosen node may
/// open new frontier members for the next step.
///
/// Restricting the predecessor check to `candidates` is what lets Pass 1
/// run before Pass 2: an init-phase node is gated only by its init-phase
/// ancestors, never by runtime-DAG nodes that have not been dispatched
/// yet. The init-phase closure is exactly the set of init Transforms plus
/// their transitive upstream ancestors, so every init node's
/// non-init predecessors are themselves impossible — the restriction is
/// total within the closure, not a relaxation of any real dependency.
///
/// Determinism: with no volume estimates every candidate's
/// `predicted_peak_bytes` is `0`, so the arbitrator's headroom, freed-bytes,
/// and subtree-reclaim tiers are all-equal and selection collapses to the
/// lowest stable topo index. Greedily taking the lowest-topo-index frontier node
/// reproduces a plain front-to-back walk of `topo_order` byte-for-byte —
/// scheduling reorders the dispatch sequence only when real estimates
/// distinguish the candidates, and never changes record output, which is
/// independent of dispatch order.
fn scheduled_pass_order(
    plan: &crate::plan::execution::ExecutionPlanDag,
    arbitrator: &crate::pipeline::memory::MemoryArbitrator,
    candidates: &HashSet<petgraph::graph::NodeIndex>,
) -> Vec<petgraph::graph::NodeIndex> {
    use crate::pipeline::memory::SchedulingHint;

    let mut emitted: HashSet<petgraph::graph::NodeIndex> = HashSet::with_capacity(candidates.len());
    let mut order: Vec<petgraph::graph::NodeIndex> = Vec::with_capacity(candidates.len());
    let hint: &dyn SchedulingHint = plan;

    // A node is runnable once every in-`candidates` predecessor is
    // already emitted. Recomputed each step from `emitted` so that
    // finishing a node exposes its now-unblocked successors.
    while order.len() < candidates.len() {
        let runnable: Vec<petgraph::graph::NodeIndex> = candidates
            .iter()
            .copied()
            .filter(|idx| !emitted.contains(idx))
            .filter(|&idx| {
                plan.graph
                    .neighbors_directed(idx, petgraph::Direction::Incoming)
                    .all(|pred| !candidates.contains(&pred) || emitted.contains(&pred))
            })
            .collect();

        // The candidate set is the node set of an acyclic graph, so as
        // long as work remains at least one node has all in-set
        // predecessors satisfied. An empty frontier with work left would
        // mean a cycle, which the planner forbids.
        debug_assert!(
            !runnable.is_empty(),
            "scheduled_pass_order: empty runnable frontier with {} candidates left to emit",
            candidates.len() - order.len()
        );

        let chosen = arbitrator.next_runnable(&runnable, hint);
        emitted.insert(chosen);
        order.push(chosen);
    }

    order
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
/// via `crossbeam_channel::Select` so a slow Source no longer blocks
/// peer Sources' channels from filling. Per-Source dispatch arms whose
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
        // across runs, whereas a live-channel `Select` depends on
        // arrival order. Unseeded interleaves get fusion +
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

/// Per-node inter-stage materialization class.
///
/// A `Streaming` stage's output bypasses a `ctx.node_buffers` slot: a
/// sink (`Output`) that never admits a buffer, a fused `Source` whose
/// receiver the downstream consumer takes directly, or a fused
/// `Transform` that hands each bounded batch to a streaming Output thread
/// over a back-pressured channel without admitting a slot. A
/// `Materialized` stage's output crosses a `node_buffers` slot that
/// registers a `NodeBufferConsumer` and is spill-eligible.
///
/// The non-trivial verdict — whether a fused Transform streams — is
/// decided by [`streaming_output_producer`], the one predicate both
/// [`classify_stream_nodes`] (the `--explain` annotation) and
/// [`compute_streaming_output_specs`] (the runtime sender install)
/// consult. A Transform is `Streaming` here iff a streaming sender is
/// installed for it at runtime, so the explain annotation can never drift
/// from what the dispatcher does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StreamClass {
    /// No `node_buffers` slot, no arbitrator charge, no spill.
    Streaming,
    /// Output crosses a `node_buffers` slot; spill-eligible.
    Materialized,
}

/// Classify every node in `plan` as [`StreamClass::Streaming`] or
/// [`StreamClass::Materialized`], derived from the same fusion analysis
/// the runtime dispatch consumes.
///
/// Streaming nodes:
///
/// - **Outputs**, unconditionally — sinks that write to their writer and
///   never admit a `node_buffers` slot.
/// - **Sources** whose name lands in either fused-source set
///   (Merge.interleave fusion or single-Transform fusion). The Source
///   dispatch arm returns without admitting a buffer; the downstream
///   fused consumer takes the receiver directly.
/// - A fused **Transform** whose single downstream is a streaming-
///   eligible Output, as decided by [`streaming_output_producer`]. Such a
///   Transform hands each bounded batch straight to the streaming Output
///   thread over a back-pressured channel and never admits a
///   `node_buffers` slot, so its peak inter-stage footprint is one batch
///   rather than the whole stage. A fused Transform that feeds anything
///   other than a streaming Output (a fan-out, a window root, a blocking
///   operator) stays `Materialized`: it accumulates and admits its full
///   output.
///
/// Everything else materializes.
///
/// Both `--explain` (plan-only, no live consumers) and the runtime
/// dispatch call this — the dispatcher reads the same `Streaming` verdict
/// for a fused Transform to decide whether to install a streaming sender,
/// so the explain annotation can never disagree with the dispatcher. The
/// returned map covers every `node_indices()` slot so callers can index
/// it by `NodeIndex` directly.
pub(crate) fn classify_stream_nodes(
    plan: &crate::plan::execution::ExecutionPlanDag,
    config: &PipelineConfig,
) -> HashMap<petgraph::graph::NodeIndex, StreamClass> {
    use crate::plan::execution::PlanNode;
    let init_phase = compute_init_phase_node_set(plan);
    let mut fused_sources = compute_merge_interleave_fused_sources(plan, config);
    let (extra_fused_sources, fused_transforms) =
        compute_transform_fused_sources(plan, &fused_sources, &init_phase);
    fused_sources.extend(extra_fused_sources);

    // Pipeline-wide correlation buffering routes every write through the
    // CorrelationCommit terminal, so no Output streams. Mirrors the same
    // short-circuit in `compute_streaming_output_specs` so the explain
    // annotation matches the runtime spec computation.
    let correlation_active = config.any_source_has_correlation_key();

    // A fused Transform streams iff some streaming-eligible Output names
    // it as its producer. Collect those producer indices once.
    let streaming_producers: HashSet<petgraph::graph::NodeIndex> = if correlation_active {
        HashSet::new()
    } else {
        plan.graph
            .node_indices()
            .filter_map(|output_idx| {
                streaming_output_producer(plan, output_idx, &fused_transforms, &init_phase)
            })
            .collect()
    };

    plan.graph
        .node_indices()
        .map(|idx| {
            let class = match &plan.graph[idx] {
                PlanNode::Output { .. } => StreamClass::Streaming,
                PlanNode::Source { name, .. } if fused_sources.contains(name.as_str()) => {
                    StreamClass::Streaming
                }
                // Any producer the eligibility predicate accepts as
                // feeding a streaming Output — fused Transform, non-fused
                // Merge, single-branch Route, streaming Aggregate, inline
                // Combine probe — streams its output to the writer thread
                // and admits no `node_buffers` slot, so it renders
                // `streaming`. The predicate already excluded blocking
                // strategies (hash Aggregate, range/sort-merge/grace
                // Combine) and window roots, so this branch only fires for
                // genuinely streaming producers.
                PlanNode::Transform { .. }
                | PlanNode::Merge { .. }
                | PlanNode::Route { .. }
                | PlanNode::Aggregation { .. }
                | PlanNode::Combine { .. }
                    if streaming_producers.contains(&idx) =>
                {
                    StreamClass::Streaming
                }
                _ => StreamClass::Materialized,
            };
            (idx, class)
        })
        .collect()
}

/// Resolve the streaming producer for an `Output` node, or `None` when
/// the Output is not streaming-eligible.
///
/// A streaming Output's writer moves into a `std::thread` that consumes a
/// bounded crossbeam channel; the producer arm sends records into that
/// channel as it produces them, so back-pressure flows writer → producer
/// → Source and no inter-stage `node_buffers` slot is charged. Two
/// producer topologies qualify, and this function is the single
/// plan-derived eligibility predicate both the runtime spawn path
/// ([`compute_streaming_output_specs`]) and the `--explain` buffer-class
/// annotation ([`classify_stream_nodes`]) consult, so the explain
/// annotation can never disagree with what the dispatcher does.
///
/// Common eligibility (independent of producer kind):
///
/// - The Output has exactly one incoming edge.
/// - The Output is not in the init-phase ancestor closure.
/// - The Output is not a per-source-file fan-out writer and its config
///   declares no `split:` block — both own their own writer lifecycle
///   and are incompatible with the single streaming writer task.
///
/// Producer = fused `Merge.interleave` (issue #72):
///
/// - The predecessor is a `PlanNode::Merge` in `interleave` mode whose
///   every direct predecessor is a fused Source, with exactly one
///   outgoing edge (this Output).
///
/// Producer = fused `Source → Transform` (the bounded per-batch
/// inter-stage handoff):
///
/// - The predecessor is a `PlanNode::Transform` that
///   `compute_transform_fused_sources` classified as fused (its sole
///   upstream is a single-outgoing Source it streams off directly), with
///   exactly one outgoing edge (this Output).
/// - The Transform roots no node-anchored window arena. A window rooted
///   at this Transform needs the Transform's full output materialized to
///   build the arena, so streaming would starve it; such a Transform
///   stays on the buffered path.
///
/// Producer = non-fused `Merge` (concat, or interleave with non-Source
/// inputs):
///
/// - The Merge feeds only this Output and roots no window arena. The
///   arm drains its predecessors' `node_buffers` slots in declaration
///   order (concat) or round-robin (interleave) into the merged result,
///   then streams that result to the writer thread rather than admitting
///   it to its own charged `node_buffers` slot. (The fused
///   Merge.interleave path streams record-by-record off the live Source
///   channels instead and is the only Merge whose footprint is one
///   batch.)
///
/// Producer = single-branch `Route`:
///
/// - The Route has exactly one outgoing edge (its sole branch) and that
///   edge feeds this Output, and the Route roots no window arena. A
///   multi-branch Route forks records across several successor slots and
///   stays on the materialized fan-out path so each branch keeps its own
///   slot — only a Route with a single downstream consumer is a linear
///   producer the streaming writer can drain.
///
/// Producer = streaming-strategy `Aggregate`:
///
/// - The aggregate resolved to [`AggregateStrategy::Streaming`] (the
///   planner certified pre-sorted input), feeds only this Output, and
///   roots no window arena. Hash aggregation stays blocking: it must
///   accumulate every group before emitting, so it cannot stream.
///
/// Producer = `Combine` probe-side (`HashBuildProbe`):
///
/// - The combine resolved to [`CombineStrategy::HashBuildProbe`], feeds
///   only this Output, and roots no window arena. The build side stays
///   fully materialized inside the arm; only the probe-side emit streams.
///   Range / sort-merge / grace-hash strategies are blocking and stay
///   materialized.
///
/// Common to every non-`Output` producer kind below: the producer must
/// have exactly one outgoing edge (this Output) and root no node-anchored
/// window arena, because a window rooted at the producer needs the
/// producer's full output materialized to build the arena and streaming
/// would starve it.
///
/// Correlation buffering disables streaming pipeline-wide — the
/// `CorrelationCommit` terminal owns the writes — so the caller short-
/// circuits before calling this.
fn streaming_output_producer(
    plan: &crate::plan::execution::ExecutionPlanDag,
    output_idx: petgraph::graph::NodeIndex,
    fused_transforms: &HashSet<petgraph::graph::NodeIndex>,
    init_phase_set: &HashSet<petgraph::graph::NodeIndex>,
) -> Option<petgraph::graph::NodeIndex> {
    use crate::aggregation::AggregateStrategy;
    use crate::plan::combine::CombineStrategy;
    use crate::plan::execution::PlanNode;

    let PlanNode::Output { resolved, .. } = &plan.graph[output_idx] else {
        return None;
    };
    let payload = resolved.as_deref()?;
    if init_phase_set.contains(&output_idx) {
        return None;
    }
    // Per-source-file fan-out and split writers own their own file
    // rotation lifecycle and cannot share the single streaming writer
    // task. Both are plan-derived so the explain and runtime surfaces
    // agree without consulting the runtime writer registry.
    if payload.fan_out_per_source_file || payload.output.split.is_some() {
        return None;
    }

    // Exactly one incoming edge.
    let mut incoming = plan
        .graph
        .neighbors_directed(output_idx, petgraph::Direction::Incoming);
    let producer_idx = incoming.next()?;
    if incoming.next().is_some() {
        return None;
    }

    // A window rooted at the producer needs the producer's full output
    // materialized to build the arena, so a streaming producer must root
    // no node-anchored window. Shared by every linear-producer arm below.
    let roots_window = |idx: petgraph::graph::NodeIndex| -> bool {
        plan.indices_to_build.iter().any(|spec| {
            matches!(
                &spec.root,
                crate::plan::index::PlanIndexRoot::Node { upstream, .. }
                    if *upstream == idx
            )
        })
    };

    match &plan.graph[producer_idx] {
        PlanNode::Merge { .. } => {
            // Fused Merge.interleave streams off the live Source channels
            // directly inside `merge_fused_interleave`; the non-fused
            // Merge (concat, or interleave with non-Source inputs) drains
            // its predecessors' `node_buffers` slots and forwards each
            // record to the writer thread. Both route their emit through
            // the streaming sender — the runtime arm picks the path — so
            // every Merge with a single downstream Output and no window
            // root is eligible regardless of fusion.
            let has_predecessor = plan
                .graph
                .neighbors_directed(producer_idx, petgraph::Direction::Incoming)
                .next()
                .is_some();
            if !has_predecessor
                || !has_single_outgoing(plan, producer_idx)
                || roots_window(producer_idx)
            {
                return None;
            }
            Some(producer_idx)
        }
        PlanNode::Transform { .. } => {
            // The Transform must be a fused Source→Transform that feeds
            // only this Output and roots no node-anchored window arena.
            if !fused_transforms.contains(&producer_idx)
                || !has_single_outgoing(plan, producer_idx)
                || roots_window(producer_idx)
            {
                return None;
            }
            Some(producer_idx)
        }
        PlanNode::Route { .. } => {
            // Single-branch Route: exactly one outgoing edge (this
            // Output). A multi-branch Route forks across successor slots
            // and stays materialized.
            if !has_single_outgoing(plan, producer_idx) || roots_window(producer_idx) {
                return None;
            }
            Some(producer_idx)
        }
        PlanNode::Aggregation { strategy, .. } => {
            // Streaming aggregation emits one group per sort-key
            // boundary, so it can stream to the writer; hash aggregation
            // must accumulate every group before emitting and stays
            // blocking.
            if !matches!(strategy, AggregateStrategy::Streaming) {
                return None;
            }
            if !has_single_outgoing(plan, producer_idx) || roots_window(producer_idx) {
                return None;
            }
            Some(producer_idx)
        }
        PlanNode::Combine { strategy, .. } => {
            // Only the inline hash build-probe streams its probe-side
            // emit; the build side stays fully materialized inside the
            // arm. Range / sort-merge / grace-hash joins are blocking.
            if !matches!(strategy, CombineStrategy::HashBuildProbe) {
                return None;
            }
            if !has_single_outgoing(plan, producer_idx) || roots_window(producer_idx) {
                return None;
            }
            Some(producer_idx)
        }
        _ => None,
    }
}

/// Identify fused producer → single `Output` chains eligible for
/// streaming writes and build a [`StreamingOutputSpec`] for each.
///
/// The buffered Output arm waits until its producer has emitted every
/// record before invoking the writer; under a slow upstream Source this
/// defeats the live back-pressure the Source ingest channel delivers,
/// because every record sits in the producer's `node_buffers` slot until
/// the producer finishes. The streaming path moves the Output's writer
/// into a `std::thread` that consumes a bounded crossbeam channel the
/// producer arm fills as it produces; per-record `Writer::write_record`
/// fires concurrently with production and back-pressure flows writer →
/// producer → Source. The producer's output never crosses a
/// `node_buffers` slot, so peak inter-stage memory for that stage is one
/// bounded batch rather than its whole output.
///
/// Eligibility is decided by [`streaming_output_producer`] (the shared
/// plan-derived predicate); this function adds the runtime writer-
/// registry checks (the writer must be a registered single writer, not a
/// fan-out) and packages the owned per-task metadata.
///
/// Returns the list of streaming specs (one per qualifying Output). Empty
/// for pipelines that don't match the topology, leaving every Output on
/// the existing buffered path. See
/// https://github.com/rustpunk/clinker/issues/72 for the rationale.
fn compute_streaming_output_specs(
    plan: &crate::plan::execution::ExecutionPlanDag,
    config: &PipelineConfig,
    fused_transforms: &HashSet<petgraph::graph::NodeIndex>,
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
        let Some(producer_idx) =
            streaming_output_producer(plan, output_idx, fused_transforms, init_phase_set)
        else {
            continue;
        };

        // Runtime writer-registry checks: the writer must be registered
        // as a single writer, never a fan-out. (The plan-derived
        // fan-out / split exclusions live in `streaming_output_producer`
        // so the explain surface agrees; this is the runtime-only
        // registry confirmation.)
        if !writers.single.contains_key(output_name) || writers.fan_out.contains_key(output_name) {
            continue;
        }
        let Some(out_cfg) = output_configs.iter().find(|o| &o.name == output_name) else {
            continue;
        };

        // Pre-compute the schema-check + projection metadata so the
        // spawned task carries owned `Arc<Schema>` / `Vec<String>` / the
        // upstream node name for E314 diagnostics, with no borrow back
        // into the plan.
        let expected_input_schema = plan.graph[output_idx]
            .expected_input_schema_in(plan)
            .cloned();
        let cxl_emit_names: Vec<String> = plan.graph[output_idx].cxl_emit_names_in(plan);

        specs.push(StreamingOutputSpec {
            producer_idx,
            output_idx,
            output_name: output_name.clone(),
            producer_name: plan.graph[producer_idx].name().to_string(),
            out_cfg: out_cfg.clone(),
            expected_input_schema,
            cxl_emit_names,
        });
    }
    specs
}

/// Plan-time metadata for a streaming `Output` thread spawned at executor
/// entry. Carries every field the thread needs in owned form so it can run
/// independently of the borrowed `&PipelineConfig` / `&ExecutionPlanDag`
/// references that anchor the dispatcher's `ExecutorContext`.
pub(crate) struct StreamingOutputSpec {
    /// `NodeIndex` of the upstream producer node whose arm writes records
    /// into the streaming channel — either a fused `Merge.interleave` or
    /// a fused `Source → Transform`. The producer arm looks up its sender
    /// in [`dispatch::ExecutorContext::streaming_output_senders`] keyed by
    /// this index.
    pub(crate) producer_idx: petgraph::graph::NodeIndex,
    /// `NodeIndex` of the downstream `Output` node. The Output arm
    /// short-circuits when its index appears in
    /// [`dispatch::ExecutorContext::streaming_output_nodes`].
    pub(crate) output_idx: petgraph::graph::NodeIndex,
    pub(crate) output_name: String,
    /// Name of the upstream producer (`Merge` or fused `Transform`), used
    /// as the upstream-node label in the streaming task's E314
    /// schema-mismatch diagnostics.
    pub(crate) producer_name: String,
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

/// Per-thread return shape merged into the dispatcher's
/// [`dispatch::ExecutorContext`] after `JoinHandle::join`. Mirrors the
/// counter / timer / error accounting the buffered Output arm performs
/// inline; the streaming thread accumulates these locally and the
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
    /// Fold the thread-local counters / timers / errors / stage metrics
    /// back into the dispatcher's [`dispatch::ExecutorContext`] after
    /// `JoinHandle::join`. Mirrors the buffered Output arm's inline
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

/// Streaming-output writer thread body. Drains the crossbeam `Receiver`
/// populated by the fused `Merge.interleave` arm, projects each record
/// through `project_output_from_record`, lazily constructs the writer on
/// the first record, calls `Writer::write_record` per record, and
/// flushes at channel close. Errors are accumulated rather than aborting
/// the loop so the dispatcher can surface them alongside any sibling
/// `output_errors`. See [`StreamingOutputSpec`] for the eligibility
/// predicate and https://github.com/rustpunk/clinker/issues/72 for the
/// rationale.
fn streaming_output(
    rx: crossbeam_channel::Receiver<crate::executor::stream_event::StreamEvent>,
    raw_writer: Box<dyn Write + Send>,
    spec: StreamingOutputSpec,
    charge_handle: Arc<crate::pipeline::memory::ConsumerHandle>,
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

    while let Ok(event) = rx.recv() {
        // Streaming output is terminal — it writes records straight to
        // disk. Punctuations are consumed here; per-document writer
        // finalization (envelope header/footer streaming reconstruction
        // on `DocumentClose`) is a follow-up filed under #91 backlog.
        let (record, rn) = match event {
            crate::executor::stream_event::StreamEvent::Record(r, rn) => (r, rn),
            crate::executor::stream_event::StreamEvent::Punctuation(_) => continue,
        };
        // Discharge this record's per-row cost from the shared charge
        // handle — the consume half of the per-batch admit/discharge
        // model. The producer charged the whole batch's
        // `EventBatch::estimated_bytes` on flush; subtracting each
        // record's `record_byte_cost` as it drains keeps the slot's live
        // count tracking exactly what is still buffered between producer
        // and writer. The formula matches the producer's charge, so a
        // fully-drained stream nets the counter back to zero.
        charge_handle.sub_bytes(crate::executor::node_buffer::record_byte_cost(
            record.schema().column_count(),
        ));
        if let Some(expected) = spec.expected_input_schema.as_ref()
            && let Err(err) = crate::executor::schema_check::check_input_schema(
                expected,
                record.schema(),
                &spec.output_name,
                "output",
                &spec.producer_name,
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
                    // bounded `send` doesn't deadlock — the streaming
                    // thread must keep consuming until every sender drops
                    // even when the writer is dead, otherwise the Merge
                    // producer blocks forever on a full bounded channel.
                    while rx.recv().is_ok() {}
                    // Nothing is buffered downstream once the channel
                    // drains, so zero the slot's charge unconditionally —
                    // the error path never discharged the records it
                    // dropped on the floor above.
                    charge_handle.set_bytes(0);
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
                while rx.recv().is_ok() {}
                charge_handle.set_bytes(0);
                return out;
            }
        }
    }

    // Channel closed — every sender dropped (`recv` returned `Err`),
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
    // The channel is fully drained, so nothing is in flight; the
    // per-record discharge above should have zeroed the counter already.
    // Pin it to zero defensively so a heuristic mismatch between the
    // batch charge and the per-record discharge can never leave a stale
    // positive charge for the post-join arbitrator read.
    charge_handle.set_bytes(0);
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
    let mut out = Record::new(Arc::clone(target), values);
    // Widening reshapes the schema for the same document's row; carry
    // the envelope context forward so a downstream node reading
    // `$doc.<section>.<field>` resolves against the originating
    // document rather than the empty synthetic context.
    out.set_doc_ctx(Arc::clone(input.doc_ctx()));
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
    pub(super) fn run_test(
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
            PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params)?;

        let output = output_buf.as_string();
        Ok((report.counters, report.dlq_entries, output))
    }

    mod aggregation;
    mod branching;
    mod ck_aligned_partition_runtime;
    mod composition_port_admission_overshoot;
    mod correlated_dlq;
    mod correlated_dlq_retract;
    mod correlated_post_aggregate_retract;
    mod correlated_window_after_aggregate_retract;
    mod correlated_window_retract;
    mod cross_source_window_topology;
    mod deferred_dispatch;
    mod diamond_node_buffer_overshoot;
    mod format_dispatch;
    mod multi_output;
    mod nested_composition_overshoot;
    mod post_aggregate_any_all;
    mod post_aggregate_lag_lead;
    mod post_aggregate_recompute_determinism;
    mod post_aggregate_window;
    mod post_aggregate_window_ranking;
    mod post_aggregate_window_spilled;
    mod post_combine_array_field;
    mod post_combine_synthetic_ck;
    mod post_combine_window_strategies;
    mod record_source_transport;
    mod scheduling;
    mod strict_pipeline_zero_overhead;
    mod window_recompute_correctness;
}
