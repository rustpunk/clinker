pub mod stage_metrics;

pub(crate) mod aggregate_dispatch;
pub(crate) mod batch_handoff;
pub mod combine;
pub(crate) mod combine_dispatch;
pub(crate) mod commit;
pub(crate) mod composition_dispatch;
mod context;
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
mod streaming;
pub(crate) mod time_window;
pub(crate) mod transform;
pub(crate) mod transform_dispatch;
mod util;
pub(crate) mod watermark;
pub(crate) mod window_runtime;

use context::build_stable_eval_context;
pub use dlq::DlqEntry;
use ingest::{IngestTaskOutcome, ingest_source};
use params::sum_cpu_io_totals;
pub use params::{ExecutionReport, PipelineRunParams};
pub use registry::WriterRegistry;
pub(crate) use registry::build_format_writer;
pub(crate) use route::{CompiledRoute, CompiledRouteBranch};
pub(crate) use streaming::StreamingOutputTaskOutput;
use streaming::{compute_streaming_output_specs, streaming_output};
pub(crate) use transform::CompiledTransform;
pub use transform::{TransformSpec, build_transform_specs};
pub(crate) use transform::{
    WindowedEvalCtx, evaluate_single_transform, evaluate_single_transform_windowed,
};
use util::scheduled_pass_order;
pub(crate) use util::{
    build_arbitrator_from_config, copy_build_ck_columns, parse_memory_limit,
    record_with_emitted_fields, widen_record_to_schema,
};

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Read;
use std::sync::Arc;

use chrono::Utc;
use clinker_record::{PipelineCounters, RecordStorage, Value};
use indexmap::IndexMap;

use crate::pipeline::memory::rss_bytes;
use clinker_plan::config::PipelineConfig;
use clinker_plan::error::PipelineError;
use clinker_plan::plan::execution::ExecutionPlanDag;
use cxl::eval::ProgramEvaluator;
use cxl::typecheck::Type;

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

/// Borrowed, read-only inputs threaded through `execute_dag` and
/// `execute_dag_branching`: the compiled program, the bound plan, and
/// the per-run parameters. Grouped so both entry points share one
/// `&` argument instead of six positional borrows, and so the owned
/// run-scoped resources in [`DagExecResources`] stay visually distinct
/// from the inputs that outlive the call.
struct DagExecInputs<'a> {
    /// Pipeline configuration: node list, error-handling policy,
    /// concurrency knobs, output configs.
    config: &'a PipelineConfig,
    /// Declared Source configs in declaration order. Borrowed for
    /// per-source seeding (watermark idle-timeouts, count slots).
    source_configs: &'a [clinker_plan::config::SourceConfig],
    /// Compiled per-node CXL transform programs, looked up by name at
    /// each Transform / Aggregation dispatch arm.
    transforms: &'a [CompiledTransform],
    /// Topologically-sorted execution DAG walked by the dispatcher.
    plan: &'a ExecutionPlanDag,
    /// Compile artifacts (bound schemas, composition bodies) consulted
    /// by the dispatcher while walking the plan.
    artifacts: &'a clinker_plan::plan::bind_schema::CompileArtifacts,
    /// Per-run parameters: execution / batch ids, channel variable
    /// overrides, shutdown token.
    params: &'a PipelineRunParams,
}

/// Owned, run-scoped resources moved through `execute_dag` into
/// `execute_dag_branching`, where they are consumed when the dispatch
/// [`dispatch::ExecutorContext`] is built. Distinct from
/// [`DagExecInputs`] because every field here transfers ownership into
/// the walk rather than being borrowed for its duration.
struct DagExecResources {
    /// One live crossbeam `Receiver` per declared Source, drained by
    /// the Source dispatch arm.
    source_records:
        HashMap<String, crossbeam_channel::Receiver<crate::executor::stream_event::StreamEvent>>,
    /// Single + fan-out output writers, split into the dispatcher's
    /// `writers` / `fan_out_writers` maps at context construction.
    writers: WriterRegistry,
    /// Compiled top-level route, if the pipeline declares one Route node.
    compiled_route: Option<CompiledRoute>,
    /// Compiled routes keyed by node name, for nested / composition-body
    /// Route nodes resolved during the walk.
    compiled_routes_by_name: HashMap<String, CompiledRoute>,
    /// Pipeline-scoped spill `TempDir`, held until the walk drains so
    /// operator-side spill files survive the topo loop.
    spill_root: Arc<tempfile::TempDir>,
    /// Cached path of `spill_root`, handed to each spill site without
    /// re-borrowing the `TempDir`.
    spill_root_path: Arc<std::path::Path>,
    /// Per-source / per-file event-time watermarks, carried through and
    /// returned in the [`DispatchOutcome`] for report roll-up.
    watermarks: crate::executor::watermark::PerSourceWatermarks,
    /// Pipeline-scoped memory arbitrator that envelopes every spill /
    /// back-pressure decision across the run.
    memory_budget: std::sync::Arc<crate::pipeline::memory::MemoryArbitrator>,
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
    /// [`clinker_plan::config::PipelineConfig::compile`] and forwards to
    /// [`Self::run_with_readers_writers`] using the plan's embedded
    /// [`PipelineConfig`]. Every declared Source is ingested through
    /// the same code path; there is no "primary" driving source. DAG
    /// dispatch order is determined by topological walk of the plan,
    /// not by source declaration order.
    ///
    /// Compile-fail guarantee — `&PipelineConfig` is NOT accepted:
    ///
    /// ```compile_fail
    /// use clinker_exec::executor::PipelineExecutor;
    /// use clinker_plan::config::PipelineConfig;
    /// use std::collections::HashMap;
    /// fn _demo(cfg: &PipelineConfig, params: &clinker_exec::executor::PipelineRunParams) {
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
        plan: &clinker_plan::plan::CompiledPlan,
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
        plan: &clinker_plan::plan::CompiledPlan,
        readers: SourceReaders,
        writers: W,
        params: &PipelineRunParams,
        compile_ctx: clinker_plan::config::CompileContext,
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
    pub fn explain_plan(plan: &clinker_plan::plan::CompiledPlan) -> Result<String, PipelineError> {
        Self::explain(plan.config())
    }

    /// `&CompiledPlan`-consuming `--explain` DAG entry.
    pub fn explain_plan_dag(
        plan: &clinker_plan::plan::CompiledPlan,
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
            clinker_plan::config::CompileContext::default(),
        )
    }

    /// Variant of [`Self::run_with_readers_writers`] that accepts an
    /// explicit [`clinker_plan::config::CompileContext`]. Tests use this to
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
        compile_ctx: clinker_plan::config::CompileContext,
    ) -> Result<ExecutionReport, PipelineError> {
        let arbitrator = build_arbitrator_from_config(config);
        // Fold the workspace `storage.spill.disk_cap_bytes` quota into the
        // arbitrator's disk-spill ceiling. Absent config leaves the cap at
        // its `u64::MAX` (unlimited) default, so behavior is unchanged when
        // no `clinker.toml` opts in. Set here — not inside
        // `build_arbitrator_from_config` — because the cap lives in the
        // workspace `clinker.toml` (a runtime parameter), not the per-pipeline
        // `memory:` block the arbitrator builder reads.
        if let Some(cap) = params.spill_disk_cap_bytes {
            arbitrator.set_max_spill_bytes(cap);
        }
        let memory_budget = std::sync::Arc::new(arbitrator);
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
        compile_ctx: clinker_plan::config::CompileContext,
        memory_budget: std::sync::Arc<crate::pipeline::memory::MemoryArbitrator>,
    ) -> Result<ExecutionReport, PipelineError> {
        let started_at = Utc::now();

        let source_configs: Vec<_> = config.source_configs().cloned().collect();
        let output_configs: Vec<_> = config.output_configs().cloned().collect();
        if source_configs.is_empty() {
            return Err(PipelineError::Config(
                clinker_plan::config::ConfigError::Validation(
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
            clinker_plan::config::build_scoped_vars_registry(
                config.pipeline.vars.as_ref(),
                &config.nodes,
            );
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
                    clinker_plan::plan::execution::PlanNode::Transform { .. }
                        | clinker_plan::plan::execution::PlanNode::Aggregation { .. }
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
                let conditions: Vec<clinker_plan::config::RouteBranch> = route_body
                    .conditions
                    .iter()
                    .map(|(name, cxl)| clinker_plan::config::RouteBranch {
                        name: name.clone(),
                        condition: cxl.source.as_str().to_string(),
                    })
                    .collect();
                let route_config = clinker_plan::config::RouteConfig {
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
                    clinker_plan::config::ConfigError::Validation(format!(
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
        //
        // The root lands under `params.spill_root_dir` when the workspace
        // `clinker.toml` set `[storage.spill] dir`, and under the OS temp dir
        // otherwise. Operators on a `/tmp`-on-tmpfs host can thus redirect
        // spill to a real disk so spilling does not defeat the memory budget
        // by paging back into RAM. The directory is pre-validated by the
        // caller, so a creation failure here is an internal fault, not a
        // misconfiguration the user can fix.
        let mut builder = tempfile::Builder::new();
        builder.prefix("clinker-spill-");
        let spill_root = Arc::new(
            match &params.spill_root_dir {
                Some(dir) => builder.tempdir_in(dir),
                None => builder.tempdir(),
            }
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
                PipelineError::Config(clinker_plan::config::ConfigError::Validation(format!(
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
            &DagExecInputs {
                config,
                source_configs: &source_configs,
                transforms: &compiled_transforms,
                plan,
                artifacts: validated_plan.artifacts(),
                params,
            },
            DagExecResources {
                source_records,
                writers,
                compiled_route,
                compiled_routes_by_name,
                spill_root,
                spill_root_path,
                watermarks,
                memory_budget,
            },
            &mut collector,
            counters,
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
    fn execute_dag(
        inputs: &DagExecInputs<'_>,
        resources: DagExecResources,
        collector: &mut stage_metrics::StageCollector,
        mut counters: PipelineCounters,
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
        let window_runtime = crate::executor::window_runtime::WindowRuntimeRegistry::new(
            &inputs.plan.indices_to_build,
        );

        Self::execute_dag_branching(
            inputs,
            resources,
            &mut counters,
            &mut dlq_entries,
            collector,
            window_runtime,
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
    fn execute_dag_branching(
        inputs: &DagExecInputs<'_>,
        resources: DagExecResources,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        collector: &mut stage_metrics::StageCollector,
        window_runtime: crate::executor::window_runtime::WindowRuntimeRegistry,
    ) -> Result<DispatchOutcome, PipelineError> {
        let &DagExecInputs {
            config,
            source_configs,
            transforms,
            plan,
            artifacts,
            params,
        } = inputs;
        let DagExecResources {
            source_records,
            mut writers,
            compiled_route,
            compiled_routes_by_name,
            spill_root,
            spill_root_path,
            watermarks,
            memory_budget,
        } = resources;

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
            let mut seed = clinker_plan::config::collect_record_var_defaults(&config.nodes);
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
        let declared_source_defaults =
            clinker_plan::config::collect_source_var_defaults(&config.nodes);

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
        let init_phase_set = clinker_plan::plan::execution::compute_init_phase_node_set(plan);
        let mut fused_sources: HashSet<String> =
            clinker_plan::plan::execution::compute_merge_interleave_fused_sources(plan, config);
        // Extend `fused_sources` with Source names whose receivers are
        // claimed by a downstream `PlanNode::Transform` running in
        // streaming mode (issue #74). `fused_transforms` carries the
        // matching Transform `NodeIndex`es so the Transform arm can
        // dispatch into the streaming branch instead of consuming a
        // pre-drained Vec from `node_buffers`.
        let (extra_fused_sources, fused_transforms) =
            clinker_plan::plan::execution::compute_transform_fused_sources(
                plan,
                &fused_sources,
                &init_phase_set,
            );
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
        route_config: &clinker_plan::config::RouteConfig,
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
            .compile(&clinker_plan::config::CompileContext::default())
            .map_err(|diags| PipelineError::Compilation {
                transform_name: String::new(),
                messages: diags.iter().map(|d| d.message.clone()).collect(),
            })?;
        Ok((validated_plan.dag().clone(), ()))
    }
}

#[cfg(test)]
mod tests {
    //! Executor white-box tests — submodules below each read a crate-private
    //! executor seam the public API does not surface (the pre-seeded
    //! `MemoryArbitrator` run entry, `scheduled_pass_order`,
    //! `single_predecessor`, `compile_route` / `CompiledRoute`,
    //! `commit::with_test_loop_cap`, `ExecutionPlanDag::deferred_region_at`),
    //! so they cannot live in the `tests/` integration directory.
    //!
    //! The pure-integration executor tests — those that drive a pipeline
    //! end-to-end through the public `&CompiledPlan` entry point — live in
    //! `crates/clinker-exec/tests/` and share the `run_config` helper in
    //! `tests/common/mod.rs`.
    //!
    //! Each submodule starts with `use super::*;` for the executor's
    //! in-crate symbols.

    use super::*;

    mod aggregation;
    mod composition_port_admission_overshoot;
    mod deferred_dispatch;
    mod diamond_node_buffer_overshoot;
    mod multi_output;
    mod nested_composition_overshoot;
    mod scheduling;
}
