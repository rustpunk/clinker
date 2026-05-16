//! Top-level DAG dispatcher.
//!
//! [`dispatch_plan_node`] reads `current_dag.graph[node_idx]` and routes
//! the node to its executor arm — Source materialization, Transform projection,
//! Route fan-out, Merge concatenation, Sort enforcement, Aggregation,
//! Combine, Composition pass-through, and Output writing. Mutable per-walk
//! state (node buffers, counters, DLQ, timers, output writers, output errors,
//! the visited-source set for the dual-counter semantic) lives on
//! [`ExecutorContext`]; immutable plan-time state (config, artifacts, run
//! params, the current DAG) is borrowed.
//!
//! The free-function shape is the entry point composition body recursion
//! will reuse: a body walk constructs an `ExecutorContext` whose
//! `current_dag` borrows the body's mini-DAG and feeds nodes through the
//! same dispatcher, mirroring DataFusion's `RecursiveQueryExec` pattern
//! where `recursive_term.execute(partition, Arc::clone(&task_context))`
//! re-enters the same execution loop with a different plan.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::{Arc, LazyLock};

use clinker_record::{FieldMetadata, GroupByKey, PipelineCounters, Record, SchemaBuilder, Value};
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, SkipReason, StableEvalContext};
use cxl::typecheck::TypedProgram;
use indexmap::IndexMap;
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use crate::config::{ErrorStrategy, OutputConfig, PipelineConfig};
use crate::error::PipelineError;

/// Stand-in `$source.file` value for dispatch sites that have no
/// originating source record (Combine/finalize/post-aggregate emits with
/// `source_row: 0`, body composition emits that lost lineage at fan-in).
/// Used when [`source_file_of`] returns `None`.
pub(crate) static MERGED_SOURCE_FILE: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from("<merged>"));

/// Stand-in `$source.name` value for the same synthetic-emit sites that
/// [`MERGED_SOURCE_FILE`] covers — Combine / post-aggregate finalize and
/// composition-body emits that lost lineage at fan-in.
pub(crate) static MERGED_SOURCE_NAME: LazyLock<Arc<str>> = LazyLock::new(|| Arc::from("<merged>"));

/// Read the per-record source-file path from the record's
/// [`FieldMetadata::SourceFile`] engine-stamped column. Returns `None`
/// when the record carries no such column (typical for Combine /
/// post-aggregate synthetic emits with `source_row: 0`) or when the
/// column's value is non-String. Callers that need an owned `Arc<str>`
/// for `RecordContext.source_file` use [`source_file_arc_of`].
pub(crate) fn source_file_path_of(record: &Record) -> Option<&str> {
    let schema = record.schema();
    for idx in 0..schema.column_count() {
        if matches!(schema.field_metadata(idx), Some(FieldMetadata::SourceFile))
            && let Some(Value::String(s)) = record.values().get(idx)
        {
            return Some(s.as_ref());
        }
    }
    None
}

/// Read the per-record source-file `Arc<str>` from the record's
/// [`FieldMetadata::SourceFile`] engine-stamped column, materializing
/// a fresh `Arc<str>` per call. Falls back to a clone of
/// [`MERGED_SOURCE_FILE`] when the stamp is absent. The Arc
/// construction is O(N) on the path length per call; per-record
/// dispatch sites accept this as a localized cost in exchange for
/// post-merge correctness — records flowing past a Merge still resolve
/// to their actual origin file rather than a primary-only fallback.
pub(crate) fn source_file_arc_of(record: &Record) -> Arc<str> {
    match source_file_path_of(record) {
        Some(s) => Arc::from(s),
        None => Arc::clone(&MERGED_SOURCE_FILE),
    }
}

/// Read the per-record Source-node name from the record's
/// [`FieldMetadata::SourceName`] engine-stamped column. Returns `None`
/// when the column is absent (synthetic emits) or non-String.
pub(crate) fn source_name_of(record: &Record) -> Option<&str> {
    let schema = record.schema();
    for idx in 0..schema.column_count() {
        if matches!(schema.field_metadata(idx), Some(FieldMetadata::SourceName))
            && let Some(Value::String(s)) = record.values().get(idx)
        {
            return Some(s.as_ref());
        }
    }
    None
}

/// Read the per-record Source-node `Arc<str>` from the record's
/// [`FieldMetadata::SourceName`] engine-stamped column, materializing
/// a fresh `Arc<str>` per call. Falls back to a clone of
/// [`MERGED_SOURCE_NAME`] when the stamp is absent. Same trade-off as
/// [`source_file_arc_of`]: localized O(N) per-record Arc construction
/// in exchange for post-merge correctness — peer Sources with shared
/// column shape still resolve to their originating Source by name.
pub(crate) fn source_name_arc_of(record: &Record) -> Arc<str> {
    match source_name_of(record) {
        Some(s) => Arc::from(s),
        None => Arc::clone(&MERGED_SOURCE_NAME),
    }
}

/// Push a [`DlqEntry`] to the run-scoped DLQ vector, increment both the
/// pipeline-wide and per-source DLQ counters, then check the configured
/// rate ceilings. Returns [`PipelineError::DlqRateExceeded`] (E315 or
/// E316) when the per-source ratio crosses
/// `error_handling.dlq.per_source.<name>.max_rate` or the pipeline-wide
/// ratio crosses `error_handling.dlq.max_rate`. Per-source > pipeline-wide
/// precedence so the offending source surfaces in the rendered diagnostic.
///
/// The rate denominator is the full per-source ingest count, seeded
/// once at executor entry from
/// [`ExecutorContext::total_per_source`] — not a moving "records
/// processed so far" counter. The total is read from each ingest
/// task's `JoinHandle` once its `mpsc::Receiver` drains, then frozen
/// for the duration of the run, so the ratio remains stable across
/// the dispatch loop's `recv().await` interleaving.
pub(crate) fn push_dlq(
    ctx: &mut ExecutorContext<'_>,
    entry: DlqEntry,
) -> Result<(), PipelineError> {
    let source_name = Arc::clone(&entry.source_name);
    ctx.counters.dlq_count += 1;
    *ctx.dlq_per_source
        .entry(Arc::clone(&source_name))
        .or_insert(0) += 1;
    ctx.dlq_entries.push(entry);
    check_dlq_rate(ctx, &source_name)
}

/// Advance `rollback_cursors[source_name]` to `row_num` when the
/// argument exceeds the stored value. Called at every clean exit from
/// a forward operator: Transform / Route success branches, and the
/// per-record `add_record` Ok path on Aggregate ingest (the absorbing
/// step that hands a source row off to the accumulator's lineage —
/// the post-finalize emit operates on aggregator-synthetic identity
/// and has no per-record source row to advance against). Monotonic
/// per source — the cursor never moves backward through this entry
/// point. A backward write happens only when a Combine output-row
/// failure consumes a `combine_input_snapshots` entry and restores
/// each contributing source's cursor; that path applies under the
/// recoverable-DLQ arm and is bypassed by setup-time
/// `PipelineError::Internal { op: "combine" }` invariant violations,
/// which still fail-fast.
///
/// `row_num` is the engine-stamped source row number stamped on each
/// record as it leaves the Source ingest task's `TokioSourceStream`
/// and threaded through every `(record, row_num)` tuple in the
/// dispatch path. It is the same value the relaxed-CK retract
/// orchestrator passes to
/// [`crate::aggregation::HashAggregator::retract_row`], so a cursor
/// stored here is directly comparable against `retract_row`
/// arguments at rewind time.
pub(crate) fn advance_cursor(ctx: &mut ExecutorContext<'_>, source_name: &Arc<str>, row_num: u64) {
    let slot = ctx
        .rollback_cursors
        .entry(Arc::clone(source_name))
        .or_insert(0);
    if row_num > *slot {
        *slot = row_num;
    }
}

/// Resolve the configured DLQ rate ceiling for `source` and return an
/// E316 (per-source) or E315 (pipeline-wide) error when the cumulative
/// fraction exceeds it. Per-source thresholds win against pipeline-wide
/// when set. Both branches honor `min_records` to avoid 1/1 = 100%
/// false positives on the very first failure.
pub(crate) fn check_dlq_rate(
    ctx: &ExecutorContext<'_>,
    source: &Arc<str>,
) -> Result<(), PipelineError> {
    let Some(dlq) = ctx.config.error_handling.dlq.as_ref() else {
        return Ok(());
    };
    let pipeline_min = dlq
        .min_records
        .unwrap_or(crate::config::DEFAULT_DLQ_MIN_RECORDS);

    if let Some(per) = dlq.per_source.get(source.as_ref())
        && let Some(max) = per.max_rate
    {
        let total = ctx.total_per_source.get(source).copied().unwrap_or(0);
        let observed = ctx.dlq_per_source.get(source).copied().unwrap_or(0);
        let min = per.min_records.unwrap_or(pipeline_min);
        if total >= min && total > 0 {
            let rate = observed as f64 / total as f64;
            if rate >= max {
                return Err(PipelineError::DlqRateExceeded {
                    source: Some(Arc::clone(source)),
                    observed_rate: rate,
                    max_rate: max,
                    observed_count: observed,
                    total_count: total,
                });
            }
        }
    }

    if let Some(max) = dlq.max_rate {
        let total: u64 = ctx.total_per_source.values().sum();
        let observed = ctx.counters.dlq_count;
        if total >= pipeline_min && total > 0 {
            let rate = observed as f64 / total as f64;
            if rate >= max {
                return Err(PipelineError::DlqRateExceeded {
                    source: None,
                    observed_rate: rate,
                    max_rate: max,
                    observed_count: observed,
                    total_count: total,
                });
            }
        }
    }

    Ok(())
}

fn value_to_correlation_key(v: &Value, idx: usize) -> GroupByKey {
    use clinker_record::value_to_group_key;
    if let Value::String(s) = v
        && s.is_empty()
    {
        return GroupByKey::Null;
    }
    if v.is_null() {
        return GroupByKey::Null;
    }
    value_to_group_key(v, "__correlation_key", None, idx as u64)
        .ok()
        .flatten()
        .unwrap_or(GroupByKey::Null)
}

/// True if every component of the key is null.
fn key_is_null(key: &[GroupByKey]) -> bool {
    key.iter().all(|k| matches!(k, GroupByKey::Null))
}

/// Build the buffer key for a record by reading every correlation-
/// lattice engine-stamped column. Two kinds of engine-stamped column
/// participate in the correlation lattice:
///
/// - `$ck.<field>` source-CK shadow columns. The shadow column carries
///   the user-declared field's value at Source ingest, so a downstream
///   Transform that rewrites the user-visible field cannot change a
///   row's group identity.
/// - `$ck.aggregate.<aggregate_name>` synthetic columns from relaxed
///   aggregates, carrying the aggregator's group index.
///
/// The `$widened` `auto_widen` sidecar absorber is engine-stamped but
/// **not** a lattice column — its `Value::Map` payload is a
/// per-record passthrough of unmapped fields, not a correlation
/// identity. Including it would split records of the same correlation
/// group into separate buffer cells (each record's distinct map
/// payload becomes a distinct key), defeating the source-CK invariant
/// that a downstream rewrite of the user-declared field cannot move
/// rows between buffer cells. The match arm below filters
/// `WidenedSidecar` out of the lattice walk.
///
/// When every component of the key is null — either because the
/// record carries no correlation-lattice columns (e.g. an Aggregate
/// output that did not propagate `$ck.*` because the user did not
/// list it in `group_by`) or because every snapshot value is itself
/// Null — a row-number disambiguator lands the record in its own
/// buffer cell, preserving per-record null-rejection semantics
/// without forcing the Output arm onto a separate non-buffered writer
/// path.
fn buffer_key_for_record(record: &Record, row_num: u64) -> Vec<GroupByKey> {
    use clinker_record::FieldMetadata;
    let schema = record.schema();
    let mut key: Vec<GroupByKey> = Vec::new();
    for i in 0..schema.column_count() {
        match schema.field_metadata(i) {
            Some(FieldMetadata::SourceCorrelation { .. })
            | Some(FieldMetadata::AggregateGroupIndex { .. }) => {
                let idx = key.len();
                key.push(value_to_correlation_key(&record.values()[i], idx));
            }
            Some(FieldMetadata::WidenedSidecar)
            | Some(FieldMetadata::SourceFile)
            | Some(FieldMetadata::SourceName)
            | Some(FieldMetadata::SourceEventTime)
            | None => {}
        }
    }
    if key.is_empty() || key_is_null(&key) {
        key.push(GroupByKey::Int(row_num as i64));
    }
    key
}

/// Redirect a per-record error into the correlation buffer when
/// correlation buffering is active.
///
/// Returns `true` iff the buffer is active and the error has been
/// parked under the record's group cell — the caller must NOT also
/// push to `ctx.dlq_entries` / `counters.dlq_count`. Returns `false`
/// when the buffer is unconfigured, signaling the caller to take the
/// per-record DLQ path. Buffer admission bumps `total_records`,
/// tripping the overflow flag once `max_group_buffer` is exceeded.
/// Null-keyed records get a row-number-disambiguated cell so each is
/// its own group of one.
fn record_error_to_buffer_if_grouped(
    ctx: &mut ExecutorContext<'_>,
    record: &Record,
    row_num: u64,
    category: crate::dlq::DlqErrorCategory,
    error_message: String,
    stage: Option<String>,
    route: Option<String>,
) -> bool {
    if ctx.correlation_buffers.is_none() {
        return false;
    }
    let key = buffer_key_for_record(record, row_num);
    let max_buf = ctx.correlation_max_group_buffer;
    let buffers = ctx
        .correlation_buffers
        .as_mut()
        .expect("checked buffers Some above");
    let entry = buffers.entry(key).or_default();
    entry.total_records += 1;
    if max_buf > 0 && entry.total_records > max_buf {
        entry.overflowed = true;
    }
    entry.error_rows.insert(row_num);
    entry.error_messages.push(CorrelationErrorRecord {
        row_num,
        original_record: record.clone(),
        category,
        error_message,
        stage,
        route,
    });
    true
}
use crate::aggregation::AggregateStrategy;
use crate::executor::schema_check::check_input_schema;
use crate::executor::{
    CompiledRoute, CompiledTransform, DlqEntry, NullStorage, build_format_writer,
    evaluate_single_transform, evaluate_single_transform_windowed, parse_memory_limit,
    stage_metrics, widen_record_to_schema,
};
use crate::pipeline::memory::MemoryBudget;
use crate::plan::bind_schema::CompileArtifacts;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};
use crate::projection::project_output_from_record;
use clinker_record::Schema;

/// Mutable per-walk and borrowed plan-time state passed to
/// [`dispatch_plan_node`].
///
/// Borrowed (immutable for the entire walk):
/// * `config`      — the YAML pipeline config (e.g. error strategy).
/// * `artifacts`   — compile-time CXL typed programs and combine metadata.
/// * `current_dag` — the DAG being walked. Composition body recursion will
///   later swap this in place to re-enter the dispatcher on a body's
///   mini-DAG without duplicating arm logic.
/// * `output_configs` / `primary_output` — output sinks and the
///   declaration-order primary used as the fallback projection target.
/// * `transform_by_name` — name → index into `compiled_transforms`.
/// * `compiled_transforms` — precompiled CXL programs for Transform and
///   Aggregation arms.
/// * `stable` / `source_batch_arc` / `strategy` — pipeline-stable scalars
///   reused at every per-record dispatch site.
///
/// Owned (mutated across the walk):
/// * `node_buffers` — `(Record, row_num)` queues threaded between arms.
/// * `source_records` — per-source live `mpsc::Receiver`s keyed by
///   Source node name. The Source dispatch arm drains its receiver
///   via `recv().await`; the paired sender lives in a `tokio::spawn`-ed
///   ingest task that drives the format reader and pushes through a
///   `TokioSourceStream`. The `$source.file` per-record stamp travels
///   on each record's engine-stamped column.
/// * `writers` — output writer registry consumed lazily as Output arms fire.
/// * `compiled_route` — cached evaluator for Route arms.
/// * `counters` / `dlq_entries` — pipeline-wide accounting.
/// * `output_errors` — collected sink failures so siblings still attempt
///   their writes (DataFusion collection-pattern PR #14439).
/// * `ok_source_rows` — distinct source rows that have reached at least
///   one Output, backing the dual-counter `counters.ok_count` semantic.
/// * `records_emitted` — drives stage-metric reporting at end of walk.
/// * Cumulative timers (`transform_timer`, `route_timer`,
///   `projection_timer`, `write_timer`) — accumulated under match-arm
///   guards.
/// * `collector` — stage-metrics collector receiving per-arm timing.
pub(crate) struct ExecutorContext<'a> {
    // Borrowed plan-time state.
    pub(crate) config: &'a PipelineConfig,
    pub(crate) artifacts: &'a CompileArtifacts,
    pub(crate) output_configs: &'a [OutputConfig],
    pub(crate) primary_output: &'a OutputConfig,
    pub(crate) compiled_transforms: &'a [CompiledTransform],
    pub(crate) transform_by_name: HashMap<&'a str, usize>,
    pub(crate) stable: &'a StableEvalContext,
    /// Backing storage for `$source.batch` — per-pipeline-run UUID v7
    /// (per-source attribution is sub-issue #54). Distinct from
    /// `pipeline.batch_id`.
    pub(crate) source_batch_arc: &'a Arc<str>,
    /// Per-source finalized record count, keyed by Source node name.
    /// `Some(n)` once the Source's `mpsc::Receiver` has returned `None`
    /// — i.e. the upstream ingest task closed its sender and we know
    /// the total. `None` while the source is still streaming.
    ///
    /// The `$source.count` evaluator reads this through
    /// [`Self::source_count_for`], which resolves the right per-source
    /// slot from a record's engine-stamped `$source.name`. Mid-stream
    /// reads resolve to `Value::Null` (defer-emit semantics — see
    /// `EvalContext.source_count` docs); finalized reads (terminal
    /// aggregate emits, commit-time deferred dispatch, post-recompute
    /// paths) resolve to the per-source total.
    pub(crate) source_count_per_source: HashMap<Arc<str>, Option<u64>>,
    /// Backing storage for `$source.ingestion_timestamp` — wall-clock
    /// time when the pipeline run began.
    pub(crate) source_ingestion_timestamp: chrono::NaiveDateTime,
    pub(crate) strategy: ErrorStrategy,

    // Owned mutable per-walk state.
    pub(crate) node_buffers: HashMap<NodeIndex, Vec<(Record, u64)>>,
    /// Per-source live ingest channels keyed by Source node name. Each
    /// declared Source has one `tokio::spawn`-ed task pushing records
    /// through a `TokioSourceStream`; this map holds the paired
    /// `Receiver` the dispatch loop's Source arm drains via
    /// `recv().await`. Replaces the pre-drained `Vec<(Record, u64)>`
    /// carrier: producers run concurrently with consumption, bounded by
    /// channel capacity so back-pressure flows end-to-end. A missing
    /// entry at the Source arm surfaces as a defense-in-depth Internal
    /// error.
    pub(crate) source_records: HashMap<String, tokio::sync::mpsc::Receiver<(Record, u64)>>,
    /// Source-node names whose receivers have been moved out of
    /// `source_records` by a downstream Merge.interleave fusion. The
    /// Source dispatch arm checks this set at entry and returns
    /// cleanly without consuming when its name is present — the fused
    /// Merge arm is now the sole consumer of those records. Empty for
    /// pipelines whose Merge predecessors are not all Sources, or
    /// whose Merge mode is concat (concat keeps today's
    /// declaration-order drain through the Source arms).
    pub(crate) fused_sources: HashSet<String>,

    /// Transforms whose sole upstream is a `PlanNode::Source` and that
    /// have taken ownership of the Source's
    /// [`tokio::sync::mpsc::Receiver`] out of [`Self::source_records`].
    /// The Transform arm drives `recv().await` per record and runs CXL
    /// evaluation inline; the upstream Source's dispatch arm short-
    /// circuits via [`Self::fused_sources`] (the same set membership
    /// the Merge.interleave fusion relies on). Populated at executor
    /// entry by the same pre-pass that fills `fused_sources`. See
    /// https://github.com/rustpunk/clinker/issues/74 for the
    /// eligibility predicate and the windowed-Transform fallback.
    pub(crate) fused_transforms: HashSet<NodeIndex>,
    /// Pipeline-wide `$record.<key>` default seed: declared
    /// `declares: scope: record` defaults plus channel
    /// `record_vars:` overrides. Borrowed by the Source dispatch
    /// arm and applied per record via
    /// [`clinker_record::Record::seed_record_vars`] at canonicalize
    /// time. Empty when no record-scope variables are declared.
    pub(crate) record_var_seed: &'a indexmap::IndexMap<String, Value>,
    /// Source-scope variable defaults from
    /// `declares: scope: source` Transforms. Seeded into
    /// [`StableEvalContext::source_vars`] at first observation of
    /// each `(source, file)` Arc by the Source dispatch arm.
    pub(crate) declared_source_defaults: &'a indexmap::IndexMap<String, Value>,
    /// Per-channel source-scope variable overrides. Layered atop
    /// [`Self::declared_source_defaults`] when the Source arm seeds
    /// the per-`(source, file)` slot. Outer key is the source-node
    /// name; inner map is the channel's `$source.<key>` overrides
    /// for that source.
    pub(crate) channel_source_vars:
        &'a indexmap::IndexMap<String, indexmap::IndexMap<String, Value>>,
    /// Per-source idle-timeout durations derived from
    /// `SourceConfig.watermark.idle_timeout`. The Source dispatch
    /// arm wraps its `rx.recv().await` in `tokio::time::timeout`
    /// when its source name is present; absent entries fall back to
    /// unbounded `rx.recv().await`.
    pub(crate) idle_timeouts: &'a HashMap<String, std::time::Duration>,
    /// Per-source set of `(file_arc)` slots whose source-scope vars
    /// have already been seeded into `stable.source_vars`. The
    /// Source dispatch arm consults this map per record; the first
    /// observation of a new file Arc grabs the stable context's
    /// write lock and seeds; subsequent records skip the lock. Outer
    /// key is the source-node name.
    pub(crate) source_vars_seeded_files: HashMap<String, HashSet<Arc<str>>>,
    pub(crate) writers: HashMap<String, Box<dyn Write + Send>>,
    /// Per-source-file writers for outputs marked `fan_out_per_source_file`
    /// in the plan. Outer key is the output name; inner key is the
    /// per-file `Arc<str>` (matching the path returned by
    /// [`source_file_path_of`] for records flowing through). The CLI
    /// populates this for outputs whose path templates reference
    /// `{source_file}` / `{source_path}` AND whose input is
    /// `FilePartitioned`. Empty map means no fan-out outputs in this run.
    pub(crate) fan_out_writers: HashMap<String, HashMap<Arc<str>, Box<dyn Write + Send>>>,
    pub(crate) compiled_route: Option<CompiledRoute>,
    /// Per-route compiled evaluators keyed by route node name.
    /// Populated for body Routes (and any Route whose conditions
    /// must survive an explicit name lookup). The Route dispatcher
    /// arm checks this map first; if absent, it falls back to the
    /// `compiled_route` singleton — the long-standing single-route
    /// path the top-level executor uses.
    pub(crate) compiled_routes_by_name: HashMap<String, CompiledRoute>,
    /// Body-scope `input:` reference table installed by the body
    /// executor before each body walk and cleared afterward. The
    /// Route dispatcher arm consults this map (when present) to
    /// resolve branch successors instead of walking `ctx.config.nodes`,
    /// which is top-level only and would not see body siblings.
    /// `None` at the top level — the existing config-walk path
    /// covers every top-level node.
    pub(crate) current_body_node_input_refs: Option<HashMap<String, Vec<String>>>,
    pub(crate) counters: PipelineCounters,
    pub(crate) dlq_entries: Vec<DlqEntry>,
    /// Per-source DLQ counters keyed by Source-node name. Incremented
    /// alongside `counters.dlq_count` at the [`push_dlq`] funnel so
    /// per-source `max_rate` thresholds can fire with attribution.
    pub(crate) dlq_per_source: HashMap<Arc<str>, u64>,
    /// Per-source total record counters keyed by Source-node name.
    /// Seeded once at executor entry from
    /// [`ExecutorContext::source_records`] so the rate-check
    /// denominator is the per-source ingest count, not a moving target.
    pub(crate) total_per_source: HashMap<Arc<str>, u64>,
    /// Per-source forward-progress rollback cursor keyed by Source-node
    /// name. Advances at each clean exit from a forward operator via
    /// [`advance_cursor`]; consulted at every collateral-DLQ and
    /// aggregate-retract decision point to bound rewind to records from
    /// the failing source. Seeded empty — sources land in the map on
    /// their first cleanly-emitted record. Sibling of `dlq_per_source`
    /// and `total_per_source`; same source-keyed
    /// `HashMap<Arc<str>, u64>` shape.
    pub(crate) rollback_cursors: HashMap<Arc<str>, u64>,
    /// Per-Combine input-cursor snapshots captured at Combine fold
    /// entry. The outer key is the Combine node's `NodeIndex`; the
    /// inner map captures the `(source_name -> cursor)` pair for every
    /// source whose record participates in the fold. The snapshot is
    /// captured at fold start, cleared at every Combine exit (inline,
    /// IEJoin, Grace, SortMerge), and restored from at the
    /// recoverable-DLQ rewind path — driver and build sides are
    /// treated symmetrically. Setup-time
    /// `PipelineError::Internal { op: "combine" }` invariant violations
    /// still fail-fast and bypass the rewind path.
    pub(crate) combine_input_snapshots: HashMap<NodeIndex, HashMap<Arc<str>, u64>>,
    pub(crate) output_errors: Vec<PipelineError>,
    pub(crate) ok_source_rows: HashSet<u64>,
    pub(crate) records_emitted: u64,
    pub(crate) transform_timer: stage_metrics::CumulativeTimer,
    pub(crate) route_timer: stage_metrics::CumulativeTimer,
    pub(crate) projection_timer: stage_metrics::CumulativeTimer,
    pub(crate) write_timer: stage_metrics::CumulativeTimer,
    pub(crate) collector: &'a mut stage_metrics::StageCollector,

    /// Composition-body recursion depth. Incremented inside
    /// `execute_composition_body` before recursing on the body's
    /// mini-DAG and decremented at every exit path. Initialized to
    /// 0 at top-level `execute_dag_branching`; never read by
    /// non-Composition arms. The Composition arm in
    /// `dispatch_plan_node` checks this against
    /// `MAX_COMPOSITION_DEPTH` before recursing and emits E112 on
    /// overflow.
    pub(crate) recursion_depth: u32,

    /// Pipeline-scoped temporary directory. Allocated once at
    /// `execute_dag_branching` start; every spilling operator (grace
    /// hash, sort-merge join, sort buffer, hash aggregation) writes
    /// spill files inside it. Shared across composition body
    /// recursion — body executors do not allocate a fresh subdir.
    /// Primary cleanup is per-file `tempfile::TempPath` Drop;
    /// secondary sweep is the pipeline-scoped TempDir Drop on panic
    /// unwinding, which closes the operator-level panic-leak hole
    /// observed in Spark (SPARK-3563, SPARK-24340) and DuckDB
    /// (issues 6420, 5878). The `Arc` keeps the directory alive
    /// while any outstanding spill file path borrows it; this
    /// matters in error paths where the executor returns before
    /// every reader has been drained.
    pub(crate) spill_root: Arc<tempfile::TempDir>,
    /// Cached path into [`Self::spill_root`]. Sharing an
    /// `Arc<Path>` keeps every operator-side `to_path_buf()` call
    /// off the `TempDir::path()` chain in hot paths and bypasses a
    /// lifetime acrobatic that would otherwise force every operator
    /// to thread the same lifetime as the borrowed plan-time refs.
    pub(crate) spill_root_path: Arc<std::path::Path>,

    /// Per-window arena+index registry. Slot `i` corresponds to
    /// `plan.indices_to_build[i]`; source-rooted slots populate at
    /// Phase-0 setup, node-rooted slots populate at their upstream
    /// operator's dispatch-arm exit through
    /// `finalize_node_rooted_windows`. Body executors push a fresh
    /// per-body vec onto `bodies` at recursion entry and pop it at
    /// exit; `ParentNode`-rooted slots in a body are inherited via
    /// `Arc::clone` from the parent's `top` vec at body entry.
    /// Replaces the singleton `(arena, indices)` pair the
    /// source-only geometry shipped: the singleton silently broke
    /// when a window sat downstream of an aggregate because the
    /// aggregate emitted columns the source arena could not project.
    pub(crate) window_runtime: crate::executor::window_runtime::WindowRuntimeRegistry,

    /// Per-(source, file) event-time watermark bookkeeping. Populated
    /// at ingest from each `SourceConfig.watermark.column` declaration;
    /// keyed by `(source_name, $source.file Arc)` so glob/regex/paths
    /// sources keep per-file isolation matching the
    /// `fan_out_per_source_file` 1:1 source-file → sink invariant.
    /// Today's only consumer is the [`ExecutionReport`]; future time-
    /// windowed aggregates will read
    /// [`PerSourceWatermarks::min_across_sources`] at the
    /// window-close decision.
    ///
    /// [`ExecutionReport`]: crate::executor::ExecutionReport
    /// [`PerSourceWatermarks::min_across_sources`]:
    ///   crate::executor::watermark::PerSourceWatermarks::min_across_sources
    pub(crate) watermarks: crate::executor::watermark::PerSourceWatermarks,

    /// Pipeline-scoped memory budget shared across the source-rooted
    /// Phase-0 arena and every node-rooted arena materialized at an
    /// upstream operator's dispatch-arm exit. One declared
    /// `memory_limit` envelopes the whole pipeline; per-arena charges
    /// accumulate against this budget so a relaxed pipeline cannot
    /// multiply its declared limit across N upstream operators by
    /// accident.
    pub(crate) memory_budget: crate::pipeline::memory::MemoryBudget,

    /// Per-correlation-group buffer holding deferred output writes
    /// and per-record error events. `Some` iff the plan carries a
    /// [`PlanNode::CorrelationCommit`] terminal — that node walks the
    /// map at end-of-DAG to flush clean groups to writers and DLQ
    /// dirty groups uniformly across all output sinks. `None`
    /// otherwise; Output writes go straight to the FormatWriter.
    pub(crate) correlation_buffers: Option<HashMap<Vec<GroupByKey>, CorrelationGroupBuffer>>,

    /// Cap on per-group record count; mirrors
    /// `error_handling.max_group_buffer`. Zero when correlation
    /// buffering is disabled. Read by the Output arm before admitting
    /// a record into the buffer to detect overflow at admission time.
    pub(crate) correlation_max_group_buffer: u64,

    /// Per-aggregate retained state for nodes whose `group_by` omits a
    /// correlation-key field. Populated by the Aggregate dispatch arm
    /// BEFORE finalize on those aggregates; drained by the
    /// orchestrator's recompute-aggregates phase. Empty for pipelines
    /// whose every aggregate has `group_by ⊇ correlation_key`, so the
    /// strict commit path observes zero overhead.
    pub(crate) relaxed_aggregator_states: HashMap<NodeIndex, RetainedAggregatorState>,

    /// Per-aggregate-node degraded set: aggregates whose retract path
    /// failed (e.g. spilled state) and that the flush phase must roll
    /// back wholesale via strict-collateral DLQ. Empty for strict
    /// pipelines.
    pub(crate) relaxed_aggregator_degrade: Vec<NodeIndex>,

    /// Tracks which commit-step path the orchestrator selected. Read by
    /// the zero-overhead-on-strict-pipeline test so the assertion
    /// proves the short-circuit is taken on every strict workload.
    pub(crate) commit_step_path: CommitStepPath,

    /// Per-edge buffer parking records that cross from a non-deferred
    /// upstream into a deferred-region member (typically Combine's
    /// build-side input). Populated by the upstream operator's arm at
    /// emit time; drained by the commit-time deferred dispatcher in a
    /// later phase. Keyed by `(active body, EdgeIndex)` because
    /// top-level and body graphs maintain disjoint EdgeIndex namespaces
    /// — the body id disambiguates collisions.
    ///
    /// Charges against `ctx.memory_budget` via `charge_arena_bytes` per
    /// admission, mirroring the per-row accounting the windowed
    /// Transform arm uses for buffer-recompute mode.
    pub(crate) region_input_buffers: RegionInputBuffers,

    /// Inverts the meaning of the per-operator deferred-region guard at
    /// the top of `dispatch_plan_node`. `false` (forward pass) makes
    /// every deferred-region member short-circuit so the producer's
    /// emit can be parked for commit-time replay. `true` (commit-time
    /// deferred dispatch) lifts the short-circuit so the same operator
    /// arms run on the post-recompute aggregate emits the deferred
    /// dispatcher seeds into the producer's `node_buffers` slot. The
    /// flag is owned by the deferred dispatcher (set on entry, cleared
    /// on exit) and never touched anywhere else; the single dispatcher
    /// design — one set of operator arms, two pass modes — is the whole
    /// architectural value of the deferred-region landing.
    pub(crate) in_deferred_dispatch: bool,

    /// Streaming-Output channel senders keyed by the upstream fused
    /// `Merge` node's `NodeIndex`. Present when the executor entry has
    /// matched a `Merge.interleave → single Output` chain against
    /// the streaming eligibility predicate (issue #72) and spawned the
    /// writer task. The fused Merge arm checks the map for its index;
    /// if present, it streams each canonicalized record through the
    /// bounded `tokio::sync::mpsc::channel` instead of accumulating
    /// into a `Vec`, and drops its sender at clean exit so the writer
    /// task's `recv()` returns `None`. Empty for pipelines that don't
    /// match the topology — every other Output stays on the buffered
    /// path. See
    /// https://github.com/rustpunk/clinker/issues/72.
    pub(crate) streaming_output_senders:
        HashMap<NodeIndex, tokio::sync::mpsc::Sender<(Record, u64)>>,
    /// `Output` `NodeIndex`es whose writes are streamed by a task
    /// spawned at executor entry. The `Output` dispatch arm short-
    /// circuits at the top when its index is in this set — the writer
    /// has already been moved into the streaming task and there is no
    /// buffered batch to drain. Empty when no streaming chain
    /// qualified.
    pub(crate) streaming_output_nodes: HashSet<NodeIndex>,
    /// `JoinHandle`s for spawned streaming-output writer tasks. Owned
    /// by the dispatcher so the end-of-DAG join surface (in
    /// `execute_dag_branching`) folds per-task counter / timer /
    /// error accounting back into the dispatcher's
    /// `counters` / `records_emitted` / `write_timer` /
    /// `projection_timer` / `ok_source_rows` / `output_errors`.
    /// Drained via `std::mem::take` at the join surface.
    pub(crate) streaming_output_tasks:
        Vec<tokio::task::JoinHandle<crate::executor::StreamingOutputTaskOutput>>,
}

/// Map keying (active composition body, outgoing edge id) to the rows
/// that crossed from a non-deferred upstream into a deferred-region
/// consumer along that edge. The body id is `None` for top-level edges;
/// each composition body has its own EdgeIndex namespace.
pub(crate) type RegionInputBuffers = HashMap<
    (
        Option<crate::plan::CompositionBodyId>,
        petgraph::graph::EdgeIndex,
    ),
    Vec<(Record, u64)>,
>;

/// Which commit-step body the orchestrator selected for the current
/// pipeline. `FastPath` short-circuits to the strict body and is the
/// only path strict pipelines ever touch; `ThreePhase` runs the
/// detect → recompute → deferred-dispatch loop followed by a single
/// flush. Surfaced on `ExecutorContext` so the
/// zero-overhead-invariant test can assert the FastPath branch fires
/// on every strict workload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommitStepPath {
    /// Default at executor start. Indicates the commit step has not
    /// yet selected its path (a no-correlation-buffer pipeline never
    /// fires the orchestrator and stays in this state).
    NotSelected,
    /// Strict pipeline (no relaxed-CK aggregate); orchestrator routed
    /// straight to the existing two-phase commit body.
    FastPath,
    /// Relaxed pipeline (at least one aggregate whose `group_by`
    /// omits a correlation-key field); orchestrator ran the
    /// cascading-retraction loop.
    ThreePhase,
}

/// Per-aggregate state retained from the Aggregate dispatch arm so the
/// orchestrator's recompute-aggregates phase can call `retract_row` +
/// `finalize_in_place`. Populated only on relaxed-CK aggregates;
/// strict pipelines never instantiate this struct.
pub(crate) struct RetainedAggregatorState {
    pub(crate) aggregator: Box<crate::aggregation::HashAggregator>,
}

impl ExecutorContext<'_> {
    /// Borrow the pipeline-scoped TempDir handle. Held alongside
    /// the cached `spill_root_path` so every operator that prefers
    /// the owned form (e.g. `SortBuffer::new` taking
    /// `Option<PathBuf>`) can clone the path without re-traversing
    /// `TempDir::path()`. The TempDir itself is needed to keep the
    /// directory alive across the whole walk including any
    /// concurrent reads on spill paths the executor has not yet
    /// drained.
    pub(crate) fn spill_root(&self) -> &Arc<tempfile::TempDir> {
        &self.spill_root
    }

    /// Resolve `$source.count` by explicit source name. Used by
    /// finalize sites that don't have a per-record source attribution
    /// — they pass [`MERGED_SOURCE_NAME`], whose slot is stamped with
    /// the pipeline-wide total once every per-source slot is `Some`.
    pub(crate) fn source_count_by_name(&self, name: &Arc<str>) -> Option<u64> {
        self.source_count_per_source.get(name).copied().flatten()
    }

    /// Stamp the per-source finalized count for `source_name` and, if
    /// every per-source slot is now populated, derive and stamp the
    /// pipeline-wide total under [`MERGED_SOURCE_NAME`]. Called by the
    /// Source dispatch arm (and the Merge.interleave fusion arm) when
    /// a source's `mpsc::Receiver` returns `None`.
    pub(crate) fn finalize_source_count(&mut self, source_name: &Arc<str>, count: u64) {
        self.source_count_per_source
            .insert(Arc::clone(source_name), Some(count));
        // Pre-seeded slots track every declared source. When none
        // remain `None` (excluding the MERGED slot itself, which we
        // are about to write), compute the cross-source total.
        let merged_key: &Arc<str> = &MERGED_SOURCE_NAME;
        let all_closed = self
            .source_count_per_source
            .iter()
            .filter(|(k, _)| !Arc::ptr_eq(k, merged_key))
            .all(|(_, v)| v.is_some());
        if all_closed {
            let total: u64 = self
                .source_count_per_source
                .iter()
                .filter(|(k, _)| !Arc::ptr_eq(k, merged_key))
                .map(|(_, v)| v.unwrap_or(0))
                .sum();
            self.source_count_per_source
                .insert(Arc::clone(merged_key), Some(total));
        }
    }
}

/// Project every record in `rows` onto the column set in
/// `buffer_schema`, returning narrow `Record` instances on a fresh,
/// per-call `Arc<Schema>` shared by every emitted Record.
///
/// Mirrors the column-pruning pattern at
/// `pipeline::arena::project_records_into_minimal`. Invoked once at the
/// deferred-region producer (a relaxed-CK Aggregate) so the emit buffer
/// retains exactly the columns the deferred operators reach via
/// `Expr::support_into`. Source row numbers carry through unchanged.
///
/// `pub(crate)` so the commit-time `recompute_aggregates` phase can
/// Emit a buffered record stream to a fan-out output: one writer per
/// source-file `Arc<str>`, route each record to the writer keyed by
/// its `$source.file` Arc. Writers without any matched records still
/// flush an empty file (preserving header and any per-file framing).
///
/// All errors land in `output_errors` rather than short-circuiting so
/// sibling writers in the same Output still get their chance to flush
/// or report.
#[allow(clippy::too_many_arguments)]
fn emit_fan_out(
    name: &str,
    out_cfg: &crate::config::OutputConfig,
    cxl_emit_names_opt: Option<&[String]>,
    output_schema: &Arc<clinker_record::Schema>,
    unbuffered: &[(Record, u64)],
    per_file: HashMap<Arc<str>, Box<dyn Write + Send>>,
    output_errors: &mut Vec<PipelineError>,
    write_timer: &mut crate::executor::stage_metrics::CumulativeTimer,
    projection_timer: &mut crate::executor::stage_metrics::CumulativeTimer,
    collector: &mut crate::executor::stage_metrics::StageCollector,
    scan_timer: crate::executor::stage_metrics::StageTimer,
) {
    use std::collections::HashMap as Hm;

    // Build one format writer per pre-opened raw writer. Failed
    // construction for one file does NOT abort the whole output —
    // siblings still get their chance.
    let mut format_writers: Hm<Arc<str>, Box<dyn clinker_format::FormatWriter>> = Hm::new();
    for (file_arc, raw) in per_file {
        match build_format_writer(out_cfg, raw, Arc::clone(output_schema)) {
            Ok(fw) => {
                format_writers.insert(file_arc, fw);
            }
            Err(e) => output_errors.push(e),
        }
    }
    collector.record(scan_timer.finish(1, 1));

    for (record, rn) in unbuffered {
        let Some(file_path) = source_file_path_of(record) else {
            output_errors.push(PipelineError::Internal {
                op: "fan_out",
                node: name.to_string(),
                detail: format!(
                    "row {rn} has no `$source.file` stamp; fan-out output requires per-record source-file lineage",
                ),
            });
            continue;
        };
        // Look up the writer by path; the registry keys by Arc<str>
        // so we need to find by string equality. Build a probing Arc
        // once per record (cheap relative to the write itself).
        let file_arc: Arc<str> = Arc::from(file_path);
        let Some(fw) = format_writers.get_mut(&file_arc) else {
            // Record's file isn't in the fan-out registry — typically
            // means the CLI's writer setup didn't pre-open one for
            // this file. Surface but keep going.
            output_errors.push(PipelineError::Internal {
                op: "fan_out",
                node: name.to_string(),
                detail: format!(
                    "no fan-out writer registered for source file {:?}",
                    file_arc
                ),
            });
            continue;
        };
        let projected = {
            let _guard = projection_timer.guard();
            project_output_from_record(record, out_cfg, cxl_emit_names_opt)
        };
        let write_result = {
            let _guard = write_timer.guard();
            fw.write_record(&projected)
        };
        if let Err(e) = write_result {
            output_errors.push(PipelineError::from(e));
        }
    }

    // Flush every writer regardless of per-record errors so partial
    // outputs land on disk for inspection.
    for (_arc, mut fw) in format_writers {
        let flush_result = {
            let _guard = write_timer.guard();
            fw.flush()
        };
        if let Err(e) = flush_result {
            output_errors.push(PipelineError::from(e));
        }
    }
}

/// project the post-retract finalize output onto the same buffer
/// schema before re-seeding the producer's `node_buffers` slot for
/// the deferred dispatcher to consume.
pub(crate) fn project_rows_to_buffer_schema(
    rows: Vec<(Record, u64)>,
    buffer_schema: &[String],
) -> Vec<(Record, u64)> {
    // Reuse the wide schema's field metadata for every narrow column;
    // dropping it would silently strip `FieldMetadata::SourceCorrelation`
    // / `AggregateGroupIndex` markers the downstream Output's
    // `buffer_key_for_record` keys on, collapsing every narrow record
    // into a per-row `null` group.
    let narrow_schema: Arc<clinker_record::Schema> = match rows.first() {
        Some((rec, _)) => {
            let wide_schema = rec.schema();
            let mut builder = SchemaBuilder::with_capacity(buffer_schema.len());
            for col in buffer_schema {
                let metadata = wide_schema
                    .index(col)
                    .and_then(|i| wide_schema.field_metadata(i).cloned());
                builder = match metadata {
                    Some(meta) => builder.with_field_meta(col.as_str(), meta),
                    None => builder.with_field(col.as_str()),
                };
            }
            builder.build()
        }
        None => buffer_schema
            .iter()
            .map(|s| Box::<str>::from(s.as_str()))
            .collect::<SchemaBuilder>()
            .build(),
    };
    rows.into_iter()
        .map(|(record, rn)| {
            let wide_schema = record.schema();
            let mut values = Vec::with_capacity(buffer_schema.len());
            for col in buffer_schema {
                let v = wide_schema
                    .index(col)
                    .and_then(|i| record.values().get(i).cloned())
                    .unwrap_or(Value::Null);
                values.push(v);
            }
            let narrow = Record::new(Arc::clone(&narrow_schema), values);
            (narrow, rn)
        })
        .collect()
}

/// Tee `emit_rows` into `region_input_buffers` for every outgoing edge
/// from `producer_idx` whose target is a deferred-region member or
/// output AND whose source (`producer_idx`) is NOT in the same region.
/// Internal-region edges are skipped — they live in `node_buffers`
/// already. Edges leaving the region's producer toward a member are
/// also skipped because the producer's own `node_buffers[producer_idx]`
/// is the canonical entry point the commit-time deferred dispatcher
/// reads from.
///
/// Charges per-row size against `ctx.memory_budget.charge_arena_bytes`;
/// returns the same `E310`-shape `PipelineError::Compilation` the
/// windowed Transform's buffer-recompute path raises on overflow so
/// downstream callers see a uniform admission failure mode.
fn tee_emit_to_region_input_buffers(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    producer_idx: NodeIndex,
    emit_rows: &[(Record, u64)],
) -> Result<(), PipelineError> {
    use petgraph::visit::EdgeRef;
    let producer_region_producer = current_dag
        .deferred_region_at(producer_idx)
        .map(|r| r.producer);
    let active_body = ctx.window_runtime.active_stack.last().copied();
    let mut crossing_edges: Vec<petgraph::graph::EdgeIndex> = Vec::new();
    for edge_ref in current_dag
        .graph
        .edges_directed(producer_idx, petgraph::Direction::Outgoing)
    {
        let target = edge_ref.target();
        let target_region_producer = current_dag.deferred_region_at(target).map(|r| r.producer);
        let crosses = match (producer_region_producer, target_region_producer) {
            // Non-deferred source feeding a deferred consumer: park
            // narrow rows so the commit-time dispatcher can re-feed
            // the deferred member without losing the upstream emit.
            (None, Some(_)) => true,
            // Distinct deferred regions abutting at this edge.
            (Some(p), Some(t)) if p != t => true,
            // Same region (internal edge) or both non-deferred: no
            // cross-region tee needed; node_buffers carries the
            // payload already.
            _ => false,
        };
        if crosses {
            crossing_edges.push(edge_ref.id());
        }
    }
    if crossing_edges.is_empty() {
        return Ok(());
    }
    for edge_id in crossing_edges {
        charge_harvest_admission(ctx, emit_rows)?;
        for (record, rn) in emit_rows {
            ctx.region_input_buffers
                .entry((active_body, edge_id))
                .or_default()
                .push((record.clone(), *rn));
        }
    }
    Ok(())
}

/// Per-row arena charge against `ctx.memory_budget` for records about
/// to be admitted into a deferred-region buffer (cross-region tee on
/// the forward pass, body→parent harvest on the commit pass). Returns
/// the same `E310`-shape `PipelineError::Compilation` as the windowed
/// Transform's buffer-recompute path so every admission site fails
/// uniformly when the per-pipeline memory limit is exhausted.
///
/// Per-row size mirrors the per-Value-slot accounting the cross-region
/// tee uses so a runaway body cannot evade the per-pipeline budget by
/// routing its output through the harvest path instead of the
/// forward-pass tee.
pub(crate) fn charge_harvest_admission(
    ctx: &mut ExecutorContext<'_>,
    rows: &[(Record, u64)],
) -> Result<(), PipelineError> {
    let row_bytes_each: u64 = rows
        .first()
        .map(|(rec, _)| {
            (std::mem::size_of::<Value>() * rec.schema().column_count()
                + std::mem::size_of::<(Record, u64)>()) as u64
        })
        .unwrap_or(0);
    if row_bytes_each == 0 {
        return Ok(());
    }
    for _ in rows {
        if ctx.memory_budget.charge_arena_bytes(row_bytes_each) {
            return Err(PipelineError::Compilation {
                transform_name: String::new(),
                messages: vec![format!(
                    "E310 deferred-region buffer admission exceeded \
                     memory limit: hard limit {}",
                    ctx.memory_budget.hard_limit()
                )],
            });
        }
    }
    Ok(())
}

/// Build the engine-stamped tail mapping for a Source's plan-time
/// target schema: `(target_index, source_field_name)` per
/// `$ck.<field>` shadow column. Resolved once per Source so the
/// per-record canonicalize call does a single name-to-index lookup
/// against the reader schema instead of re-scanning the target's
/// `field_metadata` every record.
///
/// Aggregate-emitted synthetic CK columns are stamped at aggregate
/// finalize, not at source ingest. The `$widened` sidecar is filled
/// by `CoercingReader` from input-record keys, not by name-based
/// mapping from the reader. `$source.file` and `$source.name` are
/// stamped at ingest into the record's value vector directly, so
/// they flow through canonicalize via the per-name copy loop in
/// `canonicalize_to_source_schema` rather than this engine-stamp
/// tail.
pub(crate) fn build_engine_stamped_tail(target: &Arc<Schema>) -> Vec<(usize, Box<str>)> {
    (0..target.column_count())
        .filter_map(|i| match target.field_metadata(i) {
            Some(clinker_record::FieldMetadata::SourceCorrelation { source_field }) => {
                Some((i, source_field.clone()))
            }
            Some(clinker_record::FieldMetadata::AggregateGroupIndex { .. })
            | Some(clinker_record::FieldMetadata::WidenedSidecar)
            | Some(clinker_record::FieldMetadata::SourceFile)
            | Some(clinker_record::FieldMetadata::SourceName)
            | Some(clinker_record::FieldMetadata::SourceEventTime)
            | None => None,
        })
        .collect()
}

/// Canonicalize a record produced by an ingest reader (or a body-
/// port seeded record) onto a Source's plan-time `Arc<Schema>` so
/// every downstream operator hits the `Arc::ptr_eq` fast path on the
/// first record. Used by both the legacy Source dispatch arm and the
/// `Merge.interleave` fusion that consumes Source receivers directly.
///
/// Build target-positional values by name lookup from the reader's
/// record. Admits both narrower readers (extra target slots are
/// engine-stamped — `$ck.<field>` filled by the snapshot stamp loop;
/// `$widened` left `Null` when absent on the reader) and wider
/// readers (e.g. composition-body port-sources whose port schema is
/// a subset of the parent producer's auto_widen-extended schema —
/// sidecar columns the body never declared simply do not get copied
/// through).
///
/// The integrity contract: every user-declared column on the target
/// appears under the same name on the reader, OR the target column
/// is engine-stamped (filled here) or `$widened` (copied if reader
/// has it, `Null` otherwise).
pub(crate) fn canonicalize_to_source_schema(
    r: &Record,
    target: &Arc<Schema>,
    engine_stamped: &[(usize, Box<str>)],
) -> Record {
    if Arc::ptr_eq(r.schema(), target) {
        return r.clone();
    }
    let reader = r.schema();
    let reader_vals = r.values();
    let mut values: Vec<Value> = Vec::with_capacity(target.column_count());
    for (target_idx, target_name) in target.columns().iter().enumerate() {
        if let Some(src_idx) = reader.index(target_name) {
            values.push(reader_vals[src_idx].clone());
        } else {
            debug_assert!(
                target.field_metadata(target_idx).is_some(),
                "target column {target_name:?} absent on reader and \
                 not engine-stamped — reader: {:?}, target: {:?}",
                reader.columns(),
                target.columns(),
            );
            values.push(Value::Null);
        }
    }
    // Stamp the engine-stamped CK tail at ingest: each
    // `$ck.<field>` slot captures the user-declared field's value
    // here, before any downstream Transform can rewrite it. Frozen-
    // identity semantics flow through the schema column for the
    // rest of the DAG.
    for (target_idx, source_field) in engine_stamped {
        if let Some(src_idx) = reader.index(source_field) {
            values[*target_idx] = reader_vals[src_idx].clone();
        }
    }
    Record::new(Arc::clone(target), values)
}

/// Seed declared and channel-supplied `$source.<key>` defaults for
/// the `(source, file_arc)` slot of this record. First observation
/// of a new file Arc grabs the stable context's write lock and
/// seeds; subsequent records skip the lock via the per-source
/// `source_vars_seeded_files` set.
///
/// Errors propagate as `PipelineError::Internal` if the stable
/// context's `RwLock` is poisoned — non-recoverable.
pub(crate) fn seed_source_vars_for_record(
    ctx: &mut ExecutorContext<'_>,
    source_name: &str,
    record: &Record,
) -> Result<(), PipelineError> {
    if ctx.declared_source_defaults.is_empty() && !ctx.channel_source_vars.contains_key(source_name)
    {
        return Ok(());
    }
    let Some(file_path) = source_file_path_of(record) else {
        return Ok(());
    };
    let seen = ctx
        .source_vars_seeded_files
        .entry(source_name.to_string())
        .or_default();
    let file_arc: Arc<str> = Arc::from(file_path);
    if !seen.insert(Arc::clone(&file_arc)) {
        return Ok(());
    }
    let mut map = ctx
        .stable
        .source_vars
        .write()
        .map_err(|_| PipelineError::Internal {
            op: "executor",
            node: source_name.to_string(),
            detail: "stable source_vars RwLock poisoned".to_string(),
        })?;
    let entry = map.entry(file_arc).or_default();
    for (k, v) in ctx.declared_source_defaults {
        entry.entry(k.clone()).or_insert_with(|| v.clone());
    }
    if let Some(ch) = ctx.channel_source_vars.get(source_name) {
        for (k, v) in ch {
            entry.insert(k.clone(), v.clone());
        }
    }
    Ok(())
}

/// Build node-rooted window runtimes for every IndexSpec rooted at
/// `upstream_idx` in the current DAG, after that operator has finalized
/// its emit buffer. Called from the upstream operator's dispatch arm
/// (Aggregation, Combine, Transform-with-schema-change, Merge,
/// Composition) — anything that may produce rows feeding a node-rooted
/// window. `Sort` and `Route` are pass-through and do not finalize
/// windows here; rooting walks past them at lowering time.
///
/// `rows` is the buffer the upstream operator just produced. The arena
/// projects each row onto the spec's `arena_fields` against the spec's
/// `anchor_schema` (which equals the upstream operator's
/// `output_schema` at lowering time). The arena and SecondaryIndex are
/// inserted into `ctx.window_runtime` at the spec's slot — into the
/// active body's vec when `active_stack` is non-empty, otherwise into
/// `top`.
pub(crate) fn finalize_node_rooted_windows(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    upstream_idx: NodeIndex,
    rows: &[(Record, u64)],
) -> Result<(), PipelineError> {
    use crate::executor::window_runtime::WindowRuntime;
    use crate::pipeline::arena::Arena;
    use crate::pipeline::index::SecondaryIndex;
    use crate::plan::index::PlanIndexRoot;
    use clinker_record::RecordStorage;

    for (idx, spec) in current_dag.indices_to_build.iter().enumerate() {
        let plan_anchor = match &spec.root {
            PlanIndexRoot::Node {
                upstream,
                anchor_schema,
            } if *upstream == upstream_idx => anchor_schema,
            // ParentNode never matches a current-DAG upstream — body
            // recursion installs ParentNode slots from the parent's
            // runtime at body entry, not here.
            _ => continue,
        };
        // The plan-time `anchor_schema` is the upstream operator's
        // full `output_schema`. The forward pass invokes this helper
        // with rows carrying that exact `Arc<Schema>`, so projection
        // by `anchor_schema.index(field)` lands on the right value
        // slots. The commit-pass deferred-region path invokes the
        // helper with NARROW rows (column-pruned to
        // `region.buffer_schema`), which carry their own `Arc<Schema>`
        // — projecting against the wide plan-anchor would index off
        // the end / into the wrong slot. Whenever the actual rows
        // carry a different column count than the plan-anchor, drop
        // back to the rows' own schema so projection indexes match
        // the value vector. Empty `rows` keeps the plan-anchor (no
        // narrowing observed) so the resulting empty arena still
        // pins the canonical column set.
        let anchor_schema = match rows.first() {
            Some((rec, _)) if rec.schema().column_count() != plan_anchor.column_count() => {
                rec.schema()
            }
            _ => plan_anchor,
        };
        let arena_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::ArenaBuild);
        let arena = Arena::from_records(
            rows,
            &spec.arena_fields,
            anchor_schema,
            &mut ctx.memory_budget,
        )
        .map_err(|e| PipelineError::Compilation {
            transform_name: String::new(),
            messages: vec![format!("E310 node-rooted arena build: {e}")],
        })?;
        let arena_len = arena.record_count();
        ctx.collector
            .record(arena_timer.finish(arena_len, arena_len));

        // Node-rooted arenas project from already-coerced upstream
        // output Records — the upstream operator's plan-time
        // `output_schema` is the canonical type table, so no
        // schema_pins overrides apply here. Source-rooted arenas
        // need pins because raw reader Values land as strings; node-
        // rooted Values arrive pre-typed.
        let schema_pins: HashMap<String, clinker_record::schema_def::FieldDef> = HashMap::new();
        let index_name = format!(
            "node({}):{}",
            current_dag.graph[upstream_idx].name(),
            spec.group_by.join(","),
        );
        let index_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::IndexBuild {
            name: index_name,
        });
        let mut secondary_index = SecondaryIndex::build(&arena, &spec.group_by, &schema_pins)
            .map_err(|e| PipelineError::Compilation {
                transform_name: String::new(),
                messages: vec![e.to_string()],
            })?;
        ctx.collector
            .record(index_timer.finish(arena_len, arena_len));

        // Per-partition sort. Source-declared `already_sorted` does not
        // apply to a node-rooted arena: upstream emit order is the
        // partition's underlying order, which the windowed evaluator
        // sees normalized via this sort. lag/lead determinism over
        // hash-aggregate non-deterministic emit comes from this step.
        if !spec.already_sorted {
            for partition in secondary_index.groups.values_mut() {
                if !crate::pipeline::sort::is_sorted(&arena, partition, &spec.sort_by) {
                    crate::pipeline::sort::sort_partition(&arena, partition, &spec.sort_by);
                }
            }
        }

        let runtime = WindowRuntime {
            arena: Arc::new(arena),
            index: Arc::new(secondary_index),
        };
        if !ctx.window_runtime.install(idx, runtime) {
            return Err(PipelineError::Internal {
                op: "executor",
                node: current_dag.graph[upstream_idx].name().to_string(),
                detail: format!(
                    "node-rooted window slot {idx} out of bounds; \
                     plan.indices_to_build size and runtime registry size diverged"
                ),
            });
        }
    }
    Ok(())
}

/// Drive the fused `Merge.mode: interleave` arm when every direct
/// predecessor is a `PlanNode::Source` whose receiver is parked in
/// `ctx.source_records`. Takes ownership of each predecessor's
/// `mpsc::Receiver`, runs a fair `tokio::select!` over them via a
/// `FuturesUnordered`, applies the same per-record pipeline the
/// non-fused Source arm runs (canonicalize onto the source's plan-
/// time schema, seed `$record.<key>` defaults, seed
/// `$source.<key>` defaults per `(source, file_arc)`, advance the
/// per-source running counter), and re-canonicalizes each emitted
/// record onto the Merge's output schema.
///
/// Live back-pressure semantic: a slow upstream Source's
/// `tokio::time::sleep`-gated reader does not delay peer Source
/// records from being consumed off their bounded `mpsc::channel`.
/// The select! schedules whichever receiver has a ready record;
/// the others continue producing into their channels concurrently.
///
/// Seeded interleaves (`interleave_seed: Some(n)`) replace the
/// arrival-driven schedule with a deterministic fastrand-driven
/// poll order so snapshot tests remain reproducible. Records are
/// pulled in seed-determined order from whichever receivers have
/// records available; unseeded variants follow arrival order via
/// pure `tokio::select!`.
///
/// `streaming_sender` is `Some` iff the executor matched a fused
/// `Merge.interleave → single Output` chain against the streaming
/// eligibility predicate (issue #72). In streaming mode this function
/// pushes each canonicalized record through the bounded
/// `mpsc::channel` instead of accumulating into a Vec, applies live
/// back-pressure end-to-end (writer slow → Merge yields → Sources
/// yield), and returns an empty Vec at close. In non-streaming mode
/// (`None`) it returns the accumulated `Vec<(Record, u64)>` that the
/// Merge arm then teed into `node_buffers` for downstream consumers.
pub(crate) async fn merge_fused_interleave(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    merge_name: &str,
    sorted_preds: &[NodeIndex],
    merge_output_schema: Option<&Arc<Schema>>,
    streaming_sender: Option<tokio::sync::mpsc::Sender<(Record, u64)>>,
) -> Result<Vec<(Record, u64)>, PipelineError> {
    use std::future::poll_fn;
    use std::task::Poll;

    // Per-predecessor state: the source's plan-time schema, its
    // engine-stamped tail mapping, its `Arc<str>` name (used for
    // the per-source running counter and the
    // `source_count_per_source` finalize stamp). Held in
    // declaration order matching `sorted_preds`.
    struct PredState {
        source_name_arc: Arc<str>,
        source_name_string: String,
        source_schema: Option<Arc<Schema>>,
        engine_stamped: Vec<(usize, Box<str>)>,
    }

    let mut states: Vec<PredState> = Vec::with_capacity(sorted_preds.len());
    let mut receivers: Vec<Option<tokio::sync::mpsc::Receiver<(Record, u64)>>> =
        Vec::with_capacity(sorted_preds.len());
    for pred in sorted_preds {
        let PlanNode::Source { name, .. } = &current_dag.graph[*pred] else {
            return Err(PipelineError::Internal {
                op: "executor",
                node: merge_name.to_string(),
                detail: format!(
                    "fused Merge.interleave reached non-Source predecessor at \
                     NodeIndex {pred:?} — fusion classifier diverged from runtime",
                ),
            });
        };
        let rx =
            ctx.source_records
                .remove(name.as_str())
                .ok_or_else(|| PipelineError::Internal {
                    op: "executor",
                    node: merge_name.to_string(),
                    detail: format!(
                        "fused Merge.interleave: predecessor Source {name:?} has no \
                     receiver in ctx.source_records — Source arm already consumed it?",
                    ),
                })?;
        let source_schema = current_dag.graph[*pred].stored_output_schema().cloned();
        let engine_stamped: Vec<(usize, Box<str>)> = source_schema
            .as_ref()
            .map(build_engine_stamped_tail)
            .unwrap_or_default();
        states.push(PredState {
            source_name_arc: Arc::from(name.as_str()),
            source_name_string: name.clone(),
            source_schema,
            engine_stamped,
        });
        receivers.push(Some(rx));
    }

    let merge_schema_arc = merge_output_schema.cloned();
    let has_record_seed = !ctx.record_var_seed.is_empty();
    let mut merged: Vec<(Record, u64)> = Vec::new();
    let mut per_source_counts: Vec<u64> = vec![0; receivers.len()];
    // Round-robin start cursor rotated each iteration so no
    // predecessor monopolizes the schedule when several are ready.
    // Seeded interleaves (`interleave_seed: Some(_)`) take the
    // non-fused path so their fastrand-driven determinism survives;
    // this fused path runs unseeded only.
    let mut cursor: usize = 0;

    // Each iteration: poll every active receiver via `poll_recv`,
    // taking the first ready one. Tokio guarantees `poll_recv`
    // wakes the task when a sender pushes; combining with
    // `poll_fn` gives a fair scheduler without dependencies
    // beyond tokio.
    loop {
        let active_count = receivers.iter().filter(|r| r.is_some()).count();
        if active_count == 0 {
            break;
        }
        let n = receivers.len();
        let start = {
            let s = cursor % n;
            cursor = cursor.wrapping_add(1);
            s
        };
        let polled = poll_fn(|poll_cx| {
            for offset in 0..n {
                let i = (start + offset) % n;
                let Some(rx) = receivers[i].as_mut() else {
                    continue;
                };
                match rx.poll_recv(poll_cx) {
                    Poll::Ready(item) => return Poll::Ready((i, item)),
                    Poll::Pending => continue,
                }
            }
            Poll::Pending
        })
        .await;
        let (i, item) = polled;
        match item {
            Some((record, rn)) => {
                let state = &states[i];
                let mut rec = match state.source_schema.as_ref() {
                    Some(target) => {
                        canonicalize_to_source_schema(&record, target, &state.engine_stamped)
                    }
                    None => record,
                };
                if has_record_seed {
                    rec.seed_record_vars(ctx.record_var_seed);
                }
                seed_source_vars_for_record(ctx, &state.source_name_string, &rec)?;
                if let Some(slot) = ctx.total_per_source.get_mut(&state.source_name_arc) {
                    *slot += 1;
                }
                per_source_counts[i] += 1;
                // Re-canonicalize onto the Merge's output schema so
                // downstream operators hit `Arc::ptr_eq` regardless
                // of which Source produced the record.
                if let Some(merge_schema) = merge_schema_arc.as_ref() {
                    check_input_schema(
                        merge_schema,
                        rec.schema(),
                        merge_name,
                        "merge",
                        &state.source_name_string,
                    )?;
                    let values = rec.values().to_vec();
                    rec = Record::new(Arc::clone(merge_schema), values);
                }
                match streaming_sender.as_ref() {
                    Some(tx) => {
                        // Bounded `send().await` is the back-pressure
                        // pivot — if the writer task is slow, the Merge
                        // arm yields here, which in turn slows the
                        // `poll_recv` loop above and lets the Source
                        // channels fill up. Send errors mean the
                        // receiver was dropped (writer task panicked or
                        // an error path raced); surface as Internal so
                        // the caller knows the streaming chain is
                        // broken rather than silently dropping records.
                        if tx.send((rec, rn)).await.is_err() {
                            return Err(PipelineError::Internal {
                                op: "executor",
                                node: merge_name.to_string(),
                                detail: String::from(
                                    "streaming Output writer task dropped its \
                                     receiver before the Merge arm finished",
                                ),
                            });
                        }
                    }
                    None => merged.push((rec, rn)),
                }
            }
            None => {
                // Source closed. Stamp finalized per-source count
                // and drop the receiver slot so subsequent
                // iterations skip it.
                let count = per_source_counts[i];
                let name_arc = Arc::clone(&states[i].source_name_arc);
                receivers[i] = None;
                ctx.finalize_source_count(&name_arc, count);
            }
        }
    }

    Ok(merged)
}

/// Drive a `PlanNode::Transform` whose sole upstream is a
/// `PlanNode::Source` directly off the Source's `mpsc::Receiver`,
/// running per-record CXL evaluation inline instead of consuming a
/// pre-drained `Vec` from `node_buffers`. The Source dispatch arm
/// short-circuits via [`ExecutorContext::fused_sources`]; this helper
/// takes ownership of the receiver, applies the same per-record
/// pipeline the non-fused Source arm runs (canonicalize onto the
/// Source's plan-time schema, seed `$record.<key>` defaults, seed
/// `$source.<key>` defaults per `(source, file)`, advance the per-
/// source running counter), runs the Transform's `evaluate_single_transform`
/// per record, and emits records into `ctx.node_buffers[transform_idx]`
/// at the close. See https://github.com/rustpunk/clinker/issues/74.
///
/// Eligibility (windowed Transforms, multi-input Transforms, body-
/// context Transforms, init-phase Transforms, fanned-out Sources) is
/// enforced upstream by `compute_transform_fused_sources` in
/// `executor/mod.rs`; this helper trusts the predicate and panics on
/// shape violations.
pub(crate) async fn transform_fused_consume(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
) -> Result<(), PipelineError> {
    let pred_idx = current_dag
        .graph
        .neighbors_directed(node_idx, Direction::Incoming)
        .next()
        .ok_or_else(|| PipelineError::Internal {
            op: "executor",
            node: name.to_string(),
            detail: format!(
                "fused Transform {name:?} has no incoming edge — \
                 fusion classifier accepted a node the predicate rejects"
            ),
        })?;
    let PlanNode::Source {
        name: source_name, ..
    } = &current_dag.graph[pred_idx]
    else {
        return Err(PipelineError::Internal {
            op: "executor",
            node: name.to_string(),
            detail: format!(
                "fused Transform {name:?} predecessor at NodeIndex {pred_idx:?} \
                 is not a PlanNode::Source — fusion classifier diverged from runtime"
            ),
        });
    };
    let source_name_owned = source_name.clone();

    let source_schema = current_dag.graph[pred_idx].stored_output_schema().cloned();
    let engine_stamped: Vec<(usize, Box<str>)> = source_schema
        .as_ref()
        .map(build_engine_stamped_tail)
        .unwrap_or_default();
    let canonicalize = |r: &Record| -> Record {
        match source_schema.as_ref() {
            Some(target) => canonicalize_to_source_schema(r, target, &engine_stamped),
            None => r.clone(),
        }
    };

    let transform_idx_opt = ctx.transform_by_name.get(name).copied();
    let expected_input = current_dag.graph[node_idx]
        .expected_input_schema_in(current_dag)
        .cloned();
    let output_schema = current_dag.graph[node_idx].stored_output_schema().cloned();
    let upstream_name = source_name_owned.clone();

    let mut evaluator_opt: Option<ProgramEvaluator> = transform_idx_opt.map(|idx| {
        ProgramEvaluator::new(
            Arc::clone(&ctx.compiled_transforms[idx].typed),
            ctx.compiled_transforms[idx].has_distinct(),
        )
    });

    let mut rx = ctx
        .source_records
        .remove(source_name_owned.as_str())
        .ok_or_else(|| PipelineError::Internal {
            op: "executor",
            node: name.to_string(),
            detail: format!(
                "fused Transform {name:?}: upstream Source {source_name_owned:?} \
                 has no receiver in ctx.source_records — Source arm already consumed it?"
            ),
        })?;
    let timeout = ctx.idle_timeouts.get(source_name_owned.as_str()).copied();
    let has_record_seed = !ctx.record_var_seed.is_empty();
    let source_name_arc: Arc<str> = Arc::from(source_name_owned.as_str());

    let mut output_records: Vec<(Record, u64)> = Vec::new();
    let mut last_file: Arc<str> = Arc::clone(&MERGED_SOURCE_FILE);
    let mut count: u64 = 0;
    let mut yields_since_last: u32 = 0;
    loop {
        let item: Option<(Record, u64)> = match timeout {
            Some(t) => match tokio::time::timeout(t, rx.recv()).await {
                Ok(item) => item,
                Err(_elapsed) => {
                    ctx.watermarks
                        .mark_idle(source_name_owned.as_str(), &last_file);
                    continue;
                }
            },
            None => rx.recv().await,
        };
        let Some((record, rn)) = item else {
            break;
        };
        last_file = source_file_arc_of(&record);
        let mut rec = canonicalize(&record);
        if has_record_seed {
            rec.seed_record_vars(ctx.record_var_seed);
        }
        seed_source_vars_for_record(ctx, source_name_owned.as_str(), &rec)?;
        if let Some(slot) = ctx.total_per_source.get_mut(&source_name_arc) {
            *slot += 1;
        }
        count += 1;
        yields_since_last += 1;
        if yields_since_last >= 1024 {
            yields_since_last = 0;
            tokio::task::yield_now().await;
        }

        if let Some(exp) = expected_input.as_ref() {
            check_input_schema(exp, rec.schema(), name, "transform", &upstream_name)?;
        }

        if let Some(evaluator) = evaluator_opt.as_mut() {
            let source_file_arc = source_file_arc_of(&rec);
            let rec_source_name_arc = source_name_arc_of(&rec);
            let eval_ctx = EvalContext {
                stable: ctx.stable,
                source_file: &source_file_arc,
                source_row: rn,
                source_path: &source_file_arc,
                source_count: ctx.source_count_by_name(&rec_source_name_arc),
                source_batch: ctx.source_batch_arc,
                ingestion_timestamp: ctx.source_ingestion_timestamp,
                source_name: &rec_source_name_arc,
            };
            let target_schema = output_schema
                .as_ref()
                .cloned()
                .unwrap_or_else(|| Arc::clone(rec.schema()));
            let eval_result = {
                let _guard = ctx.transform_timer.guard();
                evaluate_single_transform(&rec, name, evaluator, &eval_ctx, &target_schema)
            };
            match eval_result {
                Ok((modified_record, Ok(()))) => {
                    let advance_source = source_name_arc_of(&modified_record);
                    output_records.push((modified_record, rn));
                    advance_cursor(ctx, &advance_source, rn);
                }
                Ok((_record, Err(SkipReason::Filtered))) => {
                    ctx.counters.filtered_count += 1;
                    advance_cursor(ctx, &source_name_arc_of(&rec), rn);
                }
                Ok((_record, Err(SkipReason::Duplicate))) => {
                    ctx.counters.distinct_count += 1;
                    advance_cursor(ctx, &source_name_arc_of(&rec), rn);
                }
                Err((transform_name, eval_err)) => {
                    if ctx.strategy == ErrorStrategy::FailFast {
                        return Err(eval_err.into());
                    }
                    let stage = Some(DlqEntry::stage_transform(&transform_name));
                    let routed = record_error_to_buffer_if_grouped(
                        ctx,
                        &rec,
                        rn,
                        crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                        eval_err.to_string(),
                        stage.clone(),
                        None,
                    );
                    if !routed {
                        let dlq_source_name = source_name_arc_of(&rec);
                        let triggering_field = eval_err.triggering_field.clone();
                        let triggering_value = eval_err.triggering_value();
                        push_dlq(
                            ctx,
                            DlqEntry {
                                source_row: rn,
                                category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                error_message: eval_err.to_string(),
                                original_record: rec,
                                stage,
                                route: None,
                                trigger: true,
                                source_name: dlq_source_name,
                                triggering_field,
                                triggering_value,
                            },
                        )?;
                    }
                }
            }
        } else {
            // Transform has no compiled CXL — passthrough behavior
            // mirrors the buffered arm's `transform_by_name.get` miss
            // at the top of the existing path.
            let advance_source = source_name_arc_of(&rec);
            output_records.push((rec, rn));
            advance_cursor(ctx, &advance_source, rn);
        }
    }
    ctx.finalize_source_count(&source_name_arc, count);
    if timeout.is_some() && ctx.watermarks.is_idle(source_name_owned.as_str()) {
        tracing::debug!(
            target: "clinker::watermark",
            source = %source_name_owned,
            "fused transform consumer ended with all partitions in WatermarkStatus::Idle"
        );
    }

    finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
    ctx.node_buffers.insert(node_idx, output_records);
    Ok(())
}

/// Execute one DAG node by routing it to its arm.
///
/// Reads the node by `node_idx` from `current_dag.graph` and dispatches
/// on `PlanNode` variant. Each arm reads from and writes to `ctx.node_buffers`
/// and updates the cumulative counters / timers. Errors short-circuit only
/// for invariant violations and `ErrorStrategy::FailFast` runtime failures;
/// per-record DLQ-able errors land in `ctx.dlq_entries` under
/// `Continue`/`BestEffort`. Output sink errors are collected into
/// `ctx.output_errors` instead of short-circuiting so sibling outputs still
/// get their chance to fail (and be reported) — the caller aggregates after
/// the walk.
pub(crate) async fn dispatch_plan_node(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
) -> Result<(), PipelineError> {
    // Single deferred-region guard: skip every non-producer member on
    // the forward pass so the producer's narrow emit can be parked for
    // commit-time replay. The flag flips when the commit-time deferred
    // dispatcher walks the same arms on post-recompute aggregate emits.
    // One guard, two pass modes — the architectural payoff of routing
    // both passes through the same operator arms.
    if !ctx.in_deferred_dispatch && current_dag.is_deferred_consumer(node_idx) {
        return Ok(());
    }
    let node = current_dag.graph[node_idx].clone();
    match node {
        PlanNode::Source { ref name, .. } => {
            // Three input paths feed a Source's emit:
            //
            // 1. Source name in `ctx.fused_sources` — a downstream
            //    `Merge.interleave` arm has taken ownership of this
            //    Source's `mpsc::Receiver` and is consuming records
            //    directly via `tokio::select!`. This arm returns
            //    cleanly without emitting; the fused Merge populates
            //    the merge node's buffer.
            // 2. Records already seeded into `ctx.node_buffers[node_idx]`
            //    by the body executor at composition entry —
            //    composition input ports surface as synthetic Source
            //    nodes owning the records the parent scope harvested.
            // 3. The live `mpsc::Receiver` in `source_records[name]`,
            //    consumed via `recv().await` per record until the
            //    paired ingest task drops its sender. Per record:
            //    canonicalize onto the source's plan-time schema,
            //    seed `$record.<key>` defaults, seed
            //    `$source.<key>` defaults per `(source, file_arc)`,
            //    advance the per-source running counter. On
            //    `recv().await` returning `None`, stamp the
            //    finalized per-source count and call
            //    `finalize_node_rooted_windows` so every spec
            //    rooted at this Source's `NodeIndex` lands its
            //    arena.
            //
            // Records are canonicalized onto the Source's plan-time
            // `Arc<Schema>` so every downstream operator hits the
            // `Arc::ptr_eq` fast path on the first record. Structural
            // equality holds by construction.
            if ctx.fused_sources.contains(name.as_str()) {
                return Ok(());
            }
            let source_schema = current_dag.graph[node_idx].stored_output_schema().cloned();
            let engine_stamped: Vec<(usize, Box<str>)> = source_schema
                .as_ref()
                .map(build_engine_stamped_tail)
                .unwrap_or_default();
            let canonicalize = |r: &Record| -> Record {
                match source_schema.as_ref() {
                    Some(target) => canonicalize_to_source_schema(r, target, &engine_stamped),
                    None => r.clone(),
                }
            };

            let records: Vec<(Record, u64)> =
                if let Some(seeded) = ctx.node_buffers.remove(&node_idx) {
                    // Body-context port source — records were seeded by
                    // `execute_composition_body` from parent-scope
                    // output. The seeded records still carry the parent
                    // producer's `Arc<Schema>`, so canonicalize them
                    // onto this port source's schema before downstream
                    // consumers run. Apply per-record seeding for body-
                    // declared record_vars (parent's writes survive via
                    // `seed_record_vars`'s preserve-existing semantics).
                    let mut out: Vec<(Record, u64)> = Vec::with_capacity(seeded.len());
                    let has_record_seed = !ctx.record_var_seed.is_empty();
                    for (r, rn) in seeded {
                        let mut rec = canonicalize(&r);
                        if has_record_seed {
                            rec.seed_record_vars(ctx.record_var_seed);
                        }
                        seed_source_vars_for_record(ctx, name.as_str(), &rec)?;
                        out.push((rec, rn));
                    }
                    out
                } else if let Some(mut rx) = ctx.source_records.remove(name.as_str()) {
                    // Live channel: consume per record so back-pressure
                    // engages — a slow upstream Source no longer blocks
                    // peers' channels from filling, and watermark
                    // observations on a different source's records
                    // flow through the dispatcher in the meantime
                    // wherever the executor task scheduler interleaves.
                    let timeout = ctx.idle_timeouts.get(name.as_str()).copied();
                    let has_record_seed = !ctx.record_var_seed.is_empty();
                    let source_name_arc: Arc<str> = Arc::from(name.as_str());
                    let mut drained: Vec<(Record, u64)> = Vec::new();
                    // Tracked so an idle-timeout flips THAT file's
                    // partition to `Idle`. Before any record arrives the
                    // consumer uses the synthetic [`MERGED_SOURCE_FILE`]
                    // Arc, matching the engine-stamp path for record-
                    // less source contexts.
                    let mut last_file: Arc<str> = Arc::clone(&MERGED_SOURCE_FILE);
                    let mut count: u64 = 0;
                    loop {
                        let item: Option<(Record, u64)> = match timeout {
                            Some(t) => match tokio::time::timeout(t, rx.recv()).await {
                                Ok(item) => item,
                                Err(_elapsed) => {
                                    // Quiet for longer than
                                    // `idle_timeout` — flip the
                                    // partition tracked by `last_file`
                                    // to `Idle`. The next record un-
                                    // idles via `observe`. Idempotent on
                                    // repeat timeouts.
                                    ctx.watermarks.mark_idle(name.as_str(), &last_file);
                                    continue;
                                }
                            },
                            None => rx.recv().await,
                        };
                        let Some((record, rn)) = item else {
                            break;
                        };
                        last_file = source_file_arc_of(&record);
                        let mut rec = canonicalize(&record);
                        if has_record_seed {
                            rec.seed_record_vars(ctx.record_var_seed);
                        }
                        seed_source_vars_for_record(ctx, name.as_str(), &rec)?;
                        if let Some(slot) = ctx.total_per_source.get_mut(&source_name_arc) {
                            *slot += 1;
                        }
                        count += 1;
                        drained.push((rec, rn));
                    }
                    ctx.finalize_source_count(&source_name_arc, count);
                    if timeout.is_some() && ctx.watermarks.is_idle(name.as_str()) {
                        tracing::debug!(
                            target: "clinker::watermark",
                            source = %name,
                            "source consumer ended with all partitions in WatermarkStatus::Idle"
                        );
                    }
                    drained
                } else {
                    // Defense-in-depth: a Source reaching this arm with
                    // neither a body-port seed, nor an entry in
                    // `source_records`, nor fused-bit set means the
                    // executor's unified ingest pass missed it. Every
                    // declared Source ingests through the same
                    // `source_records` map (no primary fallthrough), so
                    // any miss surfaces here as a loud internal error
                    // rather than the silent-corruption surface at the
                    // root of #47.
                    return Err(PipelineError::Internal {
                        op: "executor",
                        node: name.clone(),
                        detail: format!(
                            "Source '{name}' has no ingested records; \
                         the executor's source-ingest pass missed this Source — \
                         likely a planner regression introducing a Source topology \
                         the ingest pass doesn't enumerate.",
                        ),
                    });
                };
            // Build node-rooted arenas anchored at this Source's
            // `NodeIndex`. Replaces the prologue's Phase-0 build:
            // every spec previously rooted at `PlanIndexRoot::Source`
            // now anchors here.
            finalize_node_rooted_windows(ctx, current_dag, node_idx, &records)?;
            tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &records)?;
            ctx.node_buffers.insert(node_idx, records);
        }

        PlanNode::Transform {
            ref name,
            window_index,
            ..
        } => {
            // Streaming-fused path: when the pre-pass has flagged this
            // Transform as eligible (sole upstream is a Source whose
            // receiver lives in `ctx.source_records`, non-windowed,
            // non-init-phase, no upstream fan-out, and the Source is
            // not already claimed by Merge.interleave fusion), drive
            // per-record evaluation directly off the receiver instead
            // of consuming a Vec from `node_buffers`. Composition body
            // walks reuse the dispatcher with a body-local
            // `ExecutionPlanDag` whose `NodeIndex` numbering is
            // independent of the top-level plan; `fused_transforms`
            // was populated against the top-level plan, so the check
            // is gated by `current_body_node_input_refs.is_none()` to
            // avoid spurious matches inside a body. See
            // https://github.com/rustpunk/clinker/issues/74.
            if ctx.current_body_node_input_refs.is_none()
                && ctx.fused_transforms.contains(&node_idx)
            {
                return transform_fused_consume(ctx, current_dag, node_idx, name).await;
            }
            // Get input records: first check own buffer (set by Route
            // node for branch dispatch), then fall back to predecessor.
            let input_records = if let Some(own_buf) = ctx.node_buffers.remove(&node_idx) {
                own_buf
            } else {
                let predecessors: Vec<NodeIndex> = current_dag
                    .graph
                    .neighbors_directed(node_idx, Direction::Incoming)
                    .collect();
                if predecessors.len() == 1 {
                    ctx.node_buffers
                        .remove(&predecessors[0])
                        .unwrap_or_default()
                } else {
                    predecessors
                        .iter()
                        .find_map(|p| ctx.node_buffers.remove(p))
                        .unwrap_or_default()
                }
            };

            // Find the CompiledTransform for this node
            let transform_idx = match ctx.transform_by_name.get(name.as_str()) {
                Some(&idx) => idx,
                None => {
                    // No transform found — pass through
                    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &input_records)?;
                    ctx.node_buffers.insert(node_idx, input_records);
                    return Ok(());
                }
            };

            let mut evaluator = ProgramEvaluator::new(
                Arc::clone(&ctx.compiled_transforms[transform_idx].typed),
                ctx.compiled_transforms[transform_idx].has_distinct(),
            );

            let expected_input = current_dag.graph[node_idx]
                .expected_input_schema_in(current_dag)
                .cloned();
            let output_schema = current_dag.graph[node_idx].stored_output_schema().cloned();
            let upstream_name = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .next()
                .map(|i| current_dag.graph[i].name().to_string())
                .unwrap_or_default();

            // Plan invariant: any Transform with `window_index: Some`
            // requires its WindowRuntime slot populated by either the
            // source-rooted Phase-0 build (Source root) or the upstream
            // operator's `finalize_node_rooted_windows` call (Node /
            // ParentNode roots). Fail loudly if the slot is missing —
            // the alternative (silently fall back to no-window eval)
            // corrupts `$window.*` results.
            if let Some(idx_num) = window_index {
                let spec = current_dag.indices_to_build.get(idx_num).ok_or_else(|| {
                    PipelineError::Internal {
                        op: "executor",
                        node: name.clone(),
                        detail: format!(
                            "transform {name:?} declares window_index {idx_num} \
                             but plan.indices_to_build is too short"
                        ),
                    }
                })?;
                if ctx.window_runtime.resolve(&spec.root, idx_num).is_none() {
                    return Err(PipelineError::Internal {
                        op: "executor",
                        node: name.clone(),
                        detail: format!(
                            "transform {name:?} declares window_index {idx_num} \
                             but the runtime registry has no populated slot for that root; \
                             upstream operator did not finalize its node-rooted windows"
                        ),
                    });
                }
            }

            let mut output_records = Vec::with_capacity(input_records.len());

            for (i, (record, rn)) in input_records.into_iter().enumerate() {
                // Cooperative yield every 1024 records so long
                // Transform chains do not starve sibling tokio tasks
                // (Vector's in-line VRL pattern). Per-record CXL eval
                // sits well below the 10–100 µs "what is blocking"
                // threshold, so on-runtime execution with periodic
                // yields beats a `block_in_place` wrap here.
                if i > 0 && i.is_multiple_of(1024) {
                    tokio::task::yield_now().await;
                }
                if let Some(exp) = expected_input.as_ref() {
                    check_input_schema(exp, record.schema(), name, "transform", &upstream_name)?;
                }
                let source_file_arc = source_file_arc_of(&record);
                let source_name_arc = source_name_arc_of(&record);
                let eval_ctx = EvalContext {
                    stable: ctx.stable,
                    source_file: &source_file_arc,
                    source_row: rn,
                    source_path: &source_file_arc,
                    source_count: ctx.source_count_by_name(&source_name_arc),
                    source_batch: ctx.source_batch_arc,
                    ingestion_timestamp: ctx.source_ingestion_timestamp,
                    source_name: &source_name_arc,
                };

                let target_schema = output_schema
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| Arc::clone(record.schema()));
                let eval_result = {
                    let _guard = ctx.transform_timer.guard();
                    if let Some(idx_num) = window_index {
                        // Resolve the WindowRuntime via the spec's root.
                        // Source-rooted windows root at Phase-0's source
                        // arena; node-rooted windows root at the
                        // upstream operator's finalize. The plan-time
                        // invariant check above guarantees `resolve`
                        // returns `Some`; reaching `None` here is an
                        // upstream-arm bug (e.g. forgetting to call
                        // `finalize_node_rooted_windows` after emit).
                        let spec = &current_dag.indices_to_build[idx_num];
                        let runtime =
                            ctx.window_runtime
                                .resolve(&spec.root, idx_num)
                                .ok_or_else(|| PipelineError::Internal {
                                    op: "executor",
                                    node: name.clone(),
                                    detail: format!(
                                        "transform {name:?} window_index {idx_num} \
                                         resolves to no runtime at per-record dispatch; \
                                         upstream finalize was skipped"
                                    ),
                                })?;
                        // record_pos: enumerate index `i`. Every arena
                        // is node-rooted; it was built from the
                        // upstream's emit buffer in iteration order,
                        // and the per-record dispatch loop iterates
                        // the same buffer, so `i` equals the row's
                        // arena position by construction.
                        let record_pos = i as u64;
                        evaluate_single_transform_windowed(
                            &record,
                            name,
                            &mut evaluator,
                            &eval_ctx,
                            current_dag,
                            idx_num,
                            runtime,
                            record_pos,
                        )
                    } else {
                        evaluate_single_transform(
                            &record,
                            name,
                            &mut evaluator,
                            &eval_ctx,
                            &target_schema,
                        )
                    }
                };
                match eval_result {
                    Ok((modified_record, Ok(()))) => {
                        let advance_source = source_name_arc_of(&modified_record);
                        output_records.push((modified_record, rn));
                        advance_cursor(ctx, &advance_source, rn);
                    }
                    Ok((_record, Err(SkipReason::Filtered))) => {
                        ctx.counters.filtered_count += 1;
                        advance_cursor(ctx, &source_name_arc_of(&record), rn);
                    }
                    Ok((_record, Err(SkipReason::Duplicate))) => {
                        ctx.counters.distinct_count += 1;
                        advance_cursor(ctx, &source_name_arc_of(&record), rn);
                    }
                    Err((transform_name, eval_err)) => {
                        if ctx.strategy == ErrorStrategy::FailFast {
                            return Err(eval_err.into());
                        }
                        let stage = Some(DlqEntry::stage_transform(&transform_name));
                        let routed = record_error_to_buffer_if_grouped(
                            ctx,
                            &record,
                            rn,
                            crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                            eval_err.to_string(),
                            stage.clone(),
                            None,
                        );
                        if !routed {
                            let source_name = source_name_arc_of(&record);
                            let triggering_field = eval_err.triggering_field.clone();
                            let triggering_value = eval_err.triggering_value();
                            push_dlq(
                                ctx,
                                DlqEntry {
                                    source_row: rn,
                                    category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                    error_message: eval_err.to_string(),
                                    original_record: record,
                                    stage,
                                    route: None,
                                    trigger: true,
                                    source_name,
                                    triggering_field,
                                    triggering_value,
                                },
                            )?;
                        }
                    }
                }
            }

            // Materialize node-rooted window runtimes for any IndexSpec
            // rooted at THIS Transform. The schema-changing case is the
            // only one that drives node-rooted lookups here:
            // pure-passthrough Transforms feed downstream windows
            // through their predecessor's runtime (Sort/Route walk-
            // through at lowering time covers passthroughs that did not
            // change schema). When the lowering pass roots a window at
            // this Transform's `NodeIndex`, the call below installs the
            // matching runtime; otherwise the helper is a no-op.
            finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
            tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
            ctx.node_buffers.insert(node_idx, output_records);
        }

        PlanNode::Route {
            ref name,
            mode,
            branches: _,
            default: _,
            ..
        } => {
            // Body-context Routes that consume an input port have no
            // predecessor in the body's mini-DAG — the records are
            // seeded into this node's own buffer at composition entry.
            // Check own buffer first, fall back to predecessor.
            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            let input_records = if let Some(own_buf) = ctx.node_buffers.remove(&node_idx) {
                own_buf
            } else {
                predecessors
                    .iter()
                    .find_map(|p| ctx.node_buffers.remove(p))
                    .unwrap_or_default()
            };

            if let Some(expected) = current_dag.graph[node_idx]
                .expected_input_schema_in(current_dag)
                .cloned()
            {
                let upstream_name = predecessors
                    .first()
                    .map(|&i| current_dag.graph[i].name().to_string())
                    .unwrap_or_default();
                for (record, _) in &input_records {
                    check_input_schema(&expected, record.schema(), name, "route", &upstream_name)?;
                }
            }

            // Get successor nodes (branch transform nodes)
            let successors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Outgoing)
                .collect();

            // Build branch_name -> successor NodeIndex mapping.
            //
            // Under the unified taxonomy, a
            // `PlanNode::Route` is a first-class node named as the
            // user wrote it in YAML (e.g. `classify_route`). Successors
            // may be Transforms, Aggregates, Outputs, or Compositions —
            // any node whose `input:` is declared as
            // `<route_name>.<branch>` (NodeInput::Port form). We
            // can't use `build_transform_specs` alone because it
            // excludes Outputs and Compositions.
            //
            // Walk `config.nodes` once, read each node's
            // `header.input`, match against `<route_parent>.<branch>`,
            // and map the branch to the successor's NodeIndex.
            let route_parent = name.strip_prefix("route_").unwrap_or(name);
            let mut succ_by_name: HashMap<String, NodeIndex> = HashMap::new();
            for &succ in &successors {
                succ_by_name.insert(current_dag.graph[succ].name().to_string(), succ);
            }
            let mut branch_to_succ: HashMap<String, NodeIndex> = HashMap::new();
            // Iterate either the body's input-ref table (when set
            // by the body executor for the current walk) or the
            // top-level config nodes. The shape is the same:
            // `(consumer_name, input_ref)` pairs.
            let mut name_input_pairs: Vec<(String, String)> = Vec::new();
            if let Some(body_refs) = ctx.current_body_node_input_refs.as_ref() {
                for (consumer, refs) in body_refs {
                    for r in refs {
                        name_input_pairs.push((consumer.clone(), r.clone()));
                    }
                }
            } else {
                use crate::config::PipelineNode;
                use crate::config::node_header::NodeInput;
                for spanned in &ctx.config.nodes {
                    let (node_name, input_ref) = match &spanned.value {
                        PipelineNode::Transform { header, .. }
                        | PipelineNode::Aggregate { header, .. }
                        | PipelineNode::Route { header, .. }
                        | PipelineNode::Output { header, .. } => {
                            let ir = match &header.input.value {
                                NodeInput::Single(s) => s.clone(),
                                NodeInput::Port { node, port } => format!("{node}.{port}"),
                            };
                            (header.name.clone(), ir)
                        }
                        _ => continue,
                    };
                    name_input_pairs.push((node_name, input_ref));
                }
            }
            for (node_name, input_ref) in name_input_pairs {
                // Port form: "<route>.<branch>" — branch name comes
                // from the port suffix. Standard for named branches
                // on routes with multiple downstream consumers per
                // branch.
                if let Some(branch) = input_ref.strip_prefix(&format!("{}.", route_parent))
                    && let Some(&succ) = succ_by_name.get(&node_name)
                {
                    branch_to_succ.insert(branch.to_string(), succ);
                    continue;
                }
                // Bare form: `input: <route>` on an Output whose
                // *name* matches a Route branch (or the Route
                // `default:`). Conventional shorthand when each
                // branch has exactly one downstream consumer and
                // the consumer's name is the branch name.
                if input_ref == route_parent
                    && let Some(&succ) = succ_by_name.get(&node_name)
                {
                    branch_to_succ.insert(node_name.clone(), succ);
                }
            }

            // Initialize per-successor buffers
            let mut branch_buffers: HashMap<NodeIndex, Vec<_>> = HashMap::new();
            for &succ in &successors {
                branch_buffers.insert(succ, Vec::new());
            }

            // Per-route compiled evaluator wins over the singleton
            // — `compiled_routes_by_name` is populated for body
            // Routes (and any Route whose conditions need an explicit
            // lookup), while the singleton remains the long-standing
            // path for the top-level Route. A body's Route arm
            // therefore finds its own conditions here instead of
            // inheriting the top-level Route's conditions, which
            // would have been the only ones available before.
            // `take` + restore keeps the route's `&mut`-mutated state
            // (regex caches, evaluator counters) consistent across
            // records hitting the same Route node within one walk.
            let from_map = ctx.compiled_routes_by_name.remove(name.as_str());
            let mut from_singleton_flag = false;
            let mut route_handle = match from_map {
                Some(r) => Some(r),
                None => {
                    let taken = ctx.compiled_route.take();
                    from_singleton_flag = taken.is_some();
                    taken
                }
            };

            if let Some(ref mut route) = route_handle {
                for (i, (record, rn)) in input_records.into_iter().enumerate() {
                    // Cooperative yield every 1024 records so long
                    // Route chains do not starve sibling tokio tasks.
                    // Same Vector-style on-runtime + periodic yield
                    // pattern the Transform arm uses.
                    if i > 0 && i.is_multiple_of(1024) {
                        tokio::task::yield_now().await;
                    }
                    let source_file_arc = source_file_arc_of(&record);
                    let source_name_arc = source_name_arc_of(&record);
                    let eval_ctx = EvalContext {
                        stable: ctx.stable,
                        source_file: &source_file_arc,
                        source_row: rn,
                        source_path: &source_file_arc,
                        source_count: ctx.source_count_by_name(&source_name_arc),
                        source_batch: ctx.source_batch_arc,
                        ingestion_timestamp: ctx.source_ingestion_timestamp,
                        source_name: &source_name_arc,
                    };

                    let route_result = {
                        let _guard = ctx.route_timer.guard();
                        route.evaluate(&record, &eval_ctx)
                    };
                    match route_result {
                        Ok(targets) => {
                            for target in &targets {
                                if let Some(&succ) = branch_to_succ.get(target.as_str()) {
                                    branch_buffers
                                        .entry(succ)
                                        .or_default()
                                        .push((record.clone(), rn));
                                }
                                // Exclusive mode: stop after first match
                                if mode == crate::config::RouteMode::Exclusive {
                                    break;
                                }
                            }
                            advance_cursor(ctx, &source_name_arc, rn);
                        }
                        Err(route_err) => {
                            if ctx.strategy == ErrorStrategy::FailFast {
                                return Err(route_err.into());
                            }
                            let stage = Some(DlqEntry::stage_route_eval());
                            let routed = record_error_to_buffer_if_grouped(
                                ctx,
                                &record,
                                rn,
                                crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                route_err.to_string(),
                                stage.clone(),
                                None,
                            );
                            if !routed {
                                let source_name = source_name_arc_of(&record);
                                let triggering_field = route_err.triggering_field.clone();
                                let triggering_value = route_err.triggering_value();
                                push_dlq(
                                    ctx,
                                    DlqEntry {
                                        source_row: rn,
                                        category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
                                        error_message: route_err.to_string(),
                                        original_record: record,
                                        stage,
                                        route: None,
                                        trigger: true,
                                        source_name,
                                        triggering_field,
                                        triggering_value,
                                    },
                                )?;
                            }
                        }
                    }
                }
            }
            // Restore the route to whichever storage it came from.
            if let Some(r) = route_handle {
                if from_singleton_flag {
                    ctx.compiled_route = Some(r);
                } else {
                    ctx.compiled_routes_by_name.insert(name.to_string(), r);
                }
            }

            // Put branch buffers into node_buffers keyed by successor.
            // For successors that fall inside a deferred region while
            // this Route does not, also park the per-branch records on
            // the matching outgoing edge so the commit-time deferred
            // dispatcher receives the same records the forward branch
            // assignment selected. Internal-region edges and edges
            // between two non-deferred operators skip the tee — the
            // node_buffers entry already covers them.
            let route_region_producer =
                current_dag.deferred_region_at(node_idx).map(|r| r.producer);
            let active_body = ctx.window_runtime.active_stack.last().copied();
            for (succ_idx, buf) in branch_buffers {
                let succ_region_producer =
                    current_dag.deferred_region_at(succ_idx).map(|r| r.producer);
                let crosses = match (route_region_producer, succ_region_producer) {
                    (None, Some(_)) => true,
                    (Some(p), Some(t)) if p != t => true,
                    _ => false,
                };
                if crosses && let Some(edge) = current_dag.graph.find_edge(node_idx, succ_idx) {
                    let row_bytes_each: u64 = buf
                        .first()
                        .map(|(rec, _)| {
                            (std::mem::size_of::<Value>() * rec.schema().column_count()
                                + std::mem::size_of::<(Record, u64)>())
                                as u64
                        })
                        .unwrap_or(0);
                    for (record, rn) in &buf {
                        if row_bytes_each > 0
                            && ctx.memory_budget.charge_arena_bytes(row_bytes_each)
                        {
                            return Err(PipelineError::Compilation {
                                transform_name: String::new(),
                                messages: vec![format!(
                                    "E310 deferred-region buffer admission exceeded \
                                     memory limit: hard limit {}",
                                    ctx.memory_budget.hard_limit()
                                )],
                            });
                        }
                        ctx.region_input_buffers
                            .entry((active_body, edge))
                            .or_default()
                            .push((record.clone(), *rn));
                    }
                }
                ctx.node_buffers.insert(succ_idx, buf);
            }
        }

        PlanNode::Merge { ref name, .. } => {
            // Two architectural modes for a Merge arm:
            //
            // 1. **Fused** — every direct predecessor is a Source and
            //    `mode: interleave`. The pre-pass at executor entry
            //    marked each predecessor's source name in
            //    `ctx.fused_sources`, the Source dispatch arms
            //    returned cleanly without consuming, and the receivers
            //    are still parked in `ctx.source_records`. This arm
            //    takes ownership of those receivers and runs a fair
            //    `tokio::select!` over them so a slow Source no longer
            //    blocks peers — back-pressure flows end-to-end.
            //
            // 2. **Non-fused** — concat mode, or a mix of Source and
            //    non-Source predecessors. Predecessor arms have
            //    already populated `ctx.node_buffers`; this arm
            //    consumes those buffers in declaration order (Concat)
            //    or round-robins across them (Interleave).
            //
            // Per-source FIFO survives both modes; per-record schema
            // canonicalization runs at consume time so the canonical
            // `Arc<Schema>` reaches downstream operators uniformly.
            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();

            let (declaration_order, mode, interleave_seed): (
                Vec<String>,
                crate::config::MergeMode,
                Option<u64>,
            ) = {
                use crate::config::PipelineNode;
                use crate::config::node_header::NodeInput;
                ctx.config
                    .nodes
                    .iter()
                    .find_map(|spanned| match &spanned.value {
                        PipelineNode::Merge { header, config, .. } if header.name == *name => {
                            let order = header
                                .inputs
                                .iter()
                                .map(|ni| match &ni.value {
                                    NodeInput::Single(s) => s.clone(),
                                    NodeInput::Port { node, port } => {
                                        format!("{node}.{port}")
                                    }
                                })
                                .collect();
                            Some((order, config.mode, config.interleave_seed))
                        }
                        _ => None,
                    })
                    .unwrap_or_else(|| (Vec::new(), crate::config::MergeMode::Concat, None))
            };

            // Sort predecessors by declaration order
            let mut sorted_preds = predecessors.clone();
            sorted_preds.sort_by_key(|p| {
                let pred_name = current_dag.graph[*p].name();
                declaration_order
                    .iter()
                    .position(|d| d == pred_name)
                    .unwrap_or(usize::MAX)
            });

            let merge_output_schema = current_dag.graph[node_idx].stored_output_schema().cloned();
            // Detect fused mode: every predecessor is a Source whose
            // name is in `ctx.fused_sources`. The pre-pass at executor
            // entry already validated mode == Interleave for these,
            // but we double-check here defensively.
            let fused_mode = matches!(mode, crate::config::MergeMode::Interleave)
                && sorted_preds.iter().all(|p| match &current_dag.graph[*p] {
                    PlanNode::Source { name: src_name, .. } => {
                        ctx.fused_sources.contains(src_name.as_str())
                    }
                    _ => false,
                });

            let merged: Vec<(Record, u64)> = if fused_mode {
                // Streaming-Output handoff (issue #72): if a single
                // Output downstream of this Merge passed the
                // eligibility predicate at executor entry, its
                // `mpsc::Sender` was installed under our `node_idx`.
                // Take it here so the Merge arm streams every record
                // through the channel instead of accumulating, and so
                // dropping the sender at clean exit closes the
                // streaming task's `recv()` loop.
                let streaming_sender = ctx.streaming_output_senders.remove(&node_idx);
                merge_fused_interleave(
                    ctx,
                    current_dag,
                    name,
                    &sorted_preds,
                    merge_output_schema.as_ref(),
                    streaming_sender,
                )
                .await?
            } else {
                let total: usize = sorted_preds
                    .iter()
                    .map(|p| ctx.node_buffers.get(p).map_or(0, |b| b.len()))
                    .sum();
                let mut merged = Vec::with_capacity(total);
                let emit = |merged: &mut Vec<(Record, u64)>,
                            upstream_name: &str,
                            mut record: Record,
                            rn: u64|
                 -> Result<(), PipelineError> {
                    if let Some(canonical) = merge_output_schema.as_ref() {
                        check_input_schema(
                            canonical,
                            record.schema(),
                            name,
                            "merge",
                            upstream_name,
                        )?;
                        // Rebuild record with the Merge's
                        // `Arc<Schema>` so downstream operators hit
                        // the ptr_eq fast path regardless of which
                        // input the record originated from.
                        let values = record.values().to_vec();
                        record = Record::new(Arc::clone(canonical), values);
                    }
                    merged.push((record, rn));
                    Ok(())
                };

                match mode {
                    crate::config::MergeMode::Concat => {
                        for pred in &sorted_preds {
                            let upstream_name = current_dag.graph[*pred].name().to_string();
                            if let Some(buf) = ctx.node_buffers.remove(pred) {
                                for (record, rn) in buf {
                                    emit(&mut merged, &upstream_name, record, rn)?;
                                }
                            }
                        }
                    }
                    crate::config::MergeMode::Interleave => {
                        // Round-robin across pre-buffered predecessor
                        // outputs. Reached only when predecessors are
                        // not all Sources (the fused path took
                        // those); the inputs are produced by upstream
                        // operator arms that wrote to `node_buffers`.
                        use std::collections::VecDeque;
                        let upstream_names: Vec<String> = sorted_preds
                            .iter()
                            .map(|p| current_dag.graph[*p].name().to_string())
                            .collect();
                        let mut deques: Vec<VecDeque<(Record, u64)>> = sorted_preds
                            .iter()
                            .map(|pred| {
                                ctx.node_buffers
                                    .remove(pred)
                                    .map(VecDeque::from)
                                    .unwrap_or_default()
                            })
                            .collect();
                        let mut rng = interleave_seed.map(fastrand::Rng::with_seed);
                        let mut cursor = 0usize;
                        loop {
                            let ready: Vec<usize> = deques
                                .iter()
                                .enumerate()
                                .filter_map(|(i, d)| (!d.is_empty()).then_some(i))
                                .collect();
                            if ready.is_empty() {
                                break;
                            }
                            let pick_idx = match rng.as_mut() {
                                Some(r) => ready[r.usize(0..ready.len())],
                                None => {
                                    let mut chosen = ready[0];
                                    for _ in 0..deques.len() {
                                        let candidate = cursor % deques.len();
                                        cursor = cursor.wrapping_add(1);
                                        if !deques[candidate].is_empty() {
                                            chosen = candidate;
                                            break;
                                        }
                                    }
                                    chosen
                                }
                            };
                            if let Some((record, rn)) = deques[pick_idx].pop_front() {
                                emit(&mut merged, &upstream_names[pick_idx], record, rn)?;
                            }
                        }
                    }
                }
                merged
            };

            // Merge is rejected as a window root at compile time
            // (E150d) because the multi-producer concatenation has no
            // single producer identity for record_pos to anchor
            // against. The call below is a defense-in-depth no-op:
            // the helper iterates plan.indices_to_build and matches
            // nothing because no spec roots at a Merge NodeIndex.
            finalize_node_rooted_windows(ctx, current_dag, node_idx, &merged)?;
            tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &merged)?;
            ctx.node_buffers.insert(node_idx, merged);
        }

        PlanNode::Sort {
            ref name,
            ref sort_fields,
            ..
        } => {
            // Enforcer-sort dispatch. Carries `row_num` through
            // the sort permutation as the `SortBuffer<u64>`
            // payload — the Record itself carries every field
            // value, emitted content, and metadata, so no
            // parallel bookkeeping map rides alongside.
            use crate::pipeline::sort_buffer::{SortBuffer, SortedOutput};

            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            let input_records: Vec<_> = predecessors
                .iter()
                .find_map(|p| ctx.node_buffers.remove(p))
                .unwrap_or_default();

            if input_records.is_empty() {
                tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &[])?;
                ctx.node_buffers.insert(node_idx, Vec::new());
                return Ok(());
            }

            let schema = input_records[0].0.schema().clone();
            let memory_limit = parse_memory_limit(ctx.config);
            let mut buf: SortBuffer<u64> = SortBuffer::new(
                sort_fields.clone(),
                memory_limit,
                Some(ctx.spill_root_path.to_path_buf()),
                schema,
            );

            let sort_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
            let sort_count = input_records.len() as u64;
            // CPU-bound: sort buffer push + per-batch comparison + spill
            // I/O can saturate a worker thread. `block_in_place` runs
            // synchronously on the current multi-thread runtime worker
            // and moves the worker's other tasks to a sibling, keeping
            // the runtime responsive (DataFusion/Vector pattern: CPU-
            // bound operators stay on-runtime via `block_in_place` so
            // borrows of `&mut ExecutorContext` survive the boundary).
            let sorted = tokio::task::block_in_place(|| {
                for (record, row_num) in input_records {
                    buf.push(record, row_num);
                    if buf.should_spill() {
                        buf.sort_and_spill().map_err(|e| {
                            PipelineError::Io(std::io::Error::other(format!(
                                "sort enforcer '{name}' spill failed: {e}"
                            )))
                        })?;
                    }
                }
                buf.finish().map_err(|e| {
                    PipelineError::Io(std::io::Error::other(format!(
                        "sort enforcer '{name}' finish failed: {e}"
                    )))
                })
            })?;

            let mut out: Vec<(Record, u64)> = Vec::with_capacity(sort_count as usize);
            match sorted {
                SortedOutput::InMemory(pairs) => {
                    for (record, row_num) in pairs {
                        out.push((record, row_num));
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
                            let (record, row_num) = entry.map_err(|e| {
                                PipelineError::Io(std::io::Error::other(format!(
                                    "sort enforcer '{name}' spill decode failed: {e}"
                                )))
                            })?;
                            out.push((record, row_num));
                        }
                    }
                }
            }
            ctx.collector
                .record(sort_timer.finish(sort_count, sort_count));
            tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &out)?;
            ctx.node_buffers.insert(node_idx, out);
        }

        PlanNode::Aggregation {
            ref name,
            ref compiled,
            strategy: agg_strategy,
            ref output_schema,
            ref config,
            ..
        } => {
            // Whether this aggregate participates in retraction-mode
            // commit is fully determined by the planner-set
            // retraction-strategy flags on `compiled`: `requires_lineage`
            // for all-Reversible bindings, `requires_buffer_mode` for
            // any BufferRequired binding. Strict aggregates have both
            // flags `false` and bypass the retain-finalize-into-state
            // path entirely. Time-windowed aggregates always take the
            // strict path — relaxed-CK retraction over multi-window
            // emissions is not supported (see `apply_retraction_flags`).
            let is_time_windowed = config.time_window.is_some();
            let is_relaxed =
                !is_time_windowed && (compiled.requires_lineage || compiled.requires_buffer_mode);
            // Hash-aggregation dispatch arm.
            //
            // DataFusion PR #9241 / #12086 lesson: any
            // `PipelineError::Internal` raised here (e.g. via
            // `single_predecessor`, or via the wrapper enum's
            // Streaming-not-yet-implemented arm) MUST hard-abort
            // regardless of `error_strategy`. We achieve this
            // structurally by propagating those errors via `?`
            // before reaching the per-record FailFast/Continue
            // match — internal invariants always abort.
            use crate::aggregation::{HashAggError, SortRow as AggSortRow};

            let pred =
                crate::executor::single_predecessor(current_dag, node_idx, "aggregation", name)?;
            let input = ctx.node_buffers.remove(&pred).unwrap_or_default();

            if let Some(expected) = current_dag.graph[node_idx]
                .expected_input_schema_in(current_dag)
                .cloned()
            {
                let upstream_name = current_dag.graph[pred].name().to_string();
                for (record, _) in &input {
                    check_input_schema(
                        &expected,
                        record.schema(),
                        name,
                        "aggregation",
                        &upstream_name,
                    )?;
                }
            }

            // Build the per-aggregation runtime artifacts. The
            // executor owns the evaluator + spill metadata; the
            // wrapper enum owns the engine.
            let transform_idx = ctx.transform_by_name.get(name.as_str()).copied();
            let evaluator = match transform_idx {
                Some(idx) => ProgramEvaluator::new(
                    Arc::clone(&ctx.compiled_transforms[idx].typed),
                    ctx.compiled_transforms[idx].has_distinct(),
                ),
                None => {
                    return Err(PipelineError::Internal {
                        op: "aggregation",
                        node: name.clone(),
                        detail: "no compiled transform found for aggregate node".to_string(),
                    });
                }
            };

            // Spill schema follows the format used by
            // `HashAggregator::spill`: group-by columns ++
            // `__acc_state` ++ `__meta_tracker`.
            let spill_schema = compiled
                .group_by_fields
                .iter()
                .map(|s| Box::<str>::from(s.as_str()))
                .chain([
                    Box::<str>::from("__acc_state"),
                    Box::<str>::from("__meta_tracker"),
                ])
                .collect::<SchemaBuilder>()
                .build();

            let memory_limit = parse_memory_limit(ctx.config);

            let agg_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
            let input_count = input.len() as u64;

            let out_rows: Vec<AggSortRow> = if is_time_windowed {
                // Time-windowed path: per-(window) AggregateStream
                // instances dispatched in `run_time_windowed_aggregate`.
                // The single positional `stream` built above is unused
                // here — drop it so its spill files (if any) are cleaned
                // up promptly. Building a fresh evaluator + spill setup
                // per window keeps the time-windowed code self-
                // contained at the cost of one extra evaluator clone per
                // window (the heavy `TypedProgram` lives behind an Arc).
                drop(evaluator);
                run_time_windowed_aggregate(
                    ctx,
                    current_dag,
                    node_idx,
                    name,
                    compiled,
                    agg_strategy,
                    Arc::clone(output_schema),
                    spill_schema,
                    memory_limit,
                    &input,
                    config
                        .time_window
                        .as_ref()
                        .expect("guarded by is_time_windowed"),
                    config.allowed_lateness,
                    transform_idx,
                )?
            } else {
                let mut stream = crate::aggregation::AggregateStream::for_node(
                    Arc::clone(compiled),
                    evaluator,
                    agg_strategy,
                    Arc::clone(output_schema),
                    spill_schema,
                    memory_limit,
                    Some(ctx.spill_root_path.to_path_buf()),
                    name.clone(),
                )?;

                // CPU-bound: per-record accumulator updates + spill I/O.
                // `block_in_place` keeps the `&mut ExecutorContext` borrows
                // alive across the synchronous body while moving sibling
                // tokio tasks off this worker (DataFusion/Vector pattern).
                let mut emitted_rows: Vec<AggSortRow> = Vec::with_capacity(64);
                tokio::task::block_in_place(|| -> Result<(), PipelineError> {
                    for (record, row_num) in &input {
                        let source_file_arc = source_file_arc_of(record);
                        let source_name_arc = source_name_arc_of(record);
                        let eval_ctx = EvalContext {
                            stable: ctx.stable,
                            source_file: &source_file_arc,
                            source_row: *row_num,
                            source_path: &source_file_arc,
                            source_count: ctx.source_count_by_name(&source_name_arc),
                            source_batch: ctx.source_batch_arc,
                            ingestion_timestamp: ctx.source_ingestion_timestamp,
                            source_name: &source_name_arc,
                        };
                        let add_result =
                            stream.add_record(record, *row_num, &eval_ctx, &mut emitted_rows);
                        if add_result.is_ok() {
                            advance_cursor(ctx, &source_name_arc, *row_num);
                        }
                        if let Err(e) = add_result {
                            match ctx.config.error_handling.strategy {
                                ErrorStrategy::FailFast => return Err(e.into()),
                                ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                                    let stage = Some(crate::dlq::stage_aggregate(name));
                                    let routed = record_error_to_buffer_if_grouped(
                                        ctx,
                                        record,
                                        *row_num,
                                        crate::dlq::DlqErrorCategory::AggregateFinalize,
                                        format!("aggregate {name}: {e}"),
                                        stage.clone(),
                                        None,
                                    );
                                    if !routed {
                                        let source_name = source_name_arc_of(record);
                                        push_dlq(
                                            ctx,
                                            DlqEntry {
                                                source_row: *row_num,
                                                category:
                                                    crate::dlq::DlqErrorCategory::AggregateFinalize,
                                                error_message: format!("aggregate {name}: {e}"),
                                                original_record: record.clone(),
                                                stage,
                                                route: None,
                                                trigger: true,
                                                source_name,
                                                triggering_field: None,
                                                triggering_value: None,
                                            },
                                        )?;
                                    }
                                }
                            }
                        }
                    }
                    Ok(())
                })?;

                // Finalize. Accumulator finalize errors get the
                // typed `PipelineError::Accumulator` mapping; under
                // `Continue` we route to the DLQ and emit zero rows
                // for the failed group. All other engine errors
                // propagate (Internal/Spill/Residual always abort).
                //
                // Relaxed-CK aggregates split the finalize path: instead
                // of consuming the wrapper, the Hash-arm boxed aggregator
                // is extracted and kept on `ExecutorContext.relaxed_aggregator_states`
                // so the correlation-commit orchestrator can call
                // `retract_row` + `finalize_in_place` against the same
                // instance that produced these rows. Strict aggregates
                // continue to consume-and-discard so non-relaxed
                // pipelines pay zero overhead.
                let finalize_ctx = EvalContext {
                    stable: ctx.stable,
                    source_file: &MERGED_SOURCE_FILE,
                    source_row: 0,
                    source_path: &MERGED_SOURCE_FILE,
                    source_count: ctx.source_count_by_name(&MERGED_SOURCE_NAME),
                    source_batch: ctx.source_batch_arc,
                    ingestion_timestamp: ctx.source_ingestion_timestamp,
                    source_name: &MERGED_SOURCE_NAME,
                };
                if is_relaxed {
                    let mut hash_box = match stream.into_retained_hash() {
                        Some(b) => b,
                        None => {
                            // Streaming + retraction-mode is rejected at
                            // compile time (E15Y); reaching this branch is
                            // a planner-pass bug.
                            return Err(PipelineError::Internal {
                                op: "aggregation",
                                node: name.clone(),
                                detail: "retraction-mode aggregate produced a non-Hash \
                                 stream — E15Y should have rejected this at compile time"
                                    .to_string(),
                            });
                        }
                    };
                    let emits_synthetic = hash_box.emits_synthetic_ck();
                    let pre_finalize_len = emitted_rows.len();
                    match hash_box.finalize_in_place(&finalize_ctx, &mut emitted_rows) {
                        Ok(()) => {
                            if emits_synthetic {
                                ctx.counters.retraction.synthetic_ck_columns_emitted_total +=
                                    (emitted_rows.len() - pre_finalize_len) as u64;
                            }
                            ctx.relaxed_aggregator_states.insert(
                                node_idx,
                                RetainedAggregatorState {
                                    aggregator: hash_box,
                                },
                            );
                            emitted_rows
                        }
                        Err(HashAggError::Accumulator {
                            transform,
                            binding,
                            source,
                        }) => match ctx.config.error_handling.strategy {
                            ErrorStrategy::FailFast => {
                                return Err(PipelineError::Accumulator {
                                    transform,
                                    binding,
                                    source,
                                });
                            }
                            ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                                // Empty-input finalize failures have no real
                                // record to attribute to a Source, so stamp
                                // the aggregate node's own name as the
                                // source label. The DLQ reader sees a
                                // specific aggregate identifier instead of
                                // the generic `<merged>` fallthrough
                                // `source_name_arc_of` would yield for a
                                // schema-only synthetic record.
                                let (synthetic, source_name) = if let Some((rec, _)) = input.first()
                                {
                                    let sn = source_name_arc_of(rec);
                                    (rec.clone(), sn)
                                } else {
                                    (
                                        Record::new(Arc::clone(output_schema), Vec::new()),
                                        Arc::from(name.as_str()),
                                    )
                                };
                                push_dlq(
                                    ctx,
                                    DlqEntry {
                                        source_row: 0,
                                        category: crate::dlq::DlqErrorCategory::AggregateFinalize,
                                        error_message: format!(
                                            "aggregate {transform}.{binding}: {source:?}"
                                        ),
                                        original_record: synthetic,
                                        stage: Some(crate::dlq::stage_aggregate(name)),
                                        route: None,
                                        trigger: true,
                                        source_name,
                                        triggering_field: None,
                                        triggering_value: None,
                                    },
                                )?;
                                Vec::new()
                            }
                        },
                        Err(other) => return Err(other.into()),
                    }
                } else {
                    match stream.finalize(&finalize_ctx, &mut emitted_rows) {
                        Ok(()) => emitted_rows,
                        Err(HashAggError::Accumulator {
                            transform,
                            binding,
                            source,
                        }) => match ctx.config.error_handling.strategy {
                            ErrorStrategy::FailFast => {
                                return Err(PipelineError::Accumulator {
                                    transform,
                                    binding,
                                    source,
                                });
                            }
                            ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                                // Empty-input finalize failures have no real
                                // record to attribute to a Source, so stamp
                                // the aggregate node's own name as the
                                // source label. The DLQ reader sees a
                                // specific aggregate identifier instead of
                                // the generic `<merged>` fallthrough
                                // `source_name_arc_of` would yield for a
                                // schema-only synthetic record.
                                let (synthetic, source_name) = if let Some((rec, _)) = input.first()
                                {
                                    let sn = source_name_arc_of(rec);
                                    (rec.clone(), sn)
                                } else {
                                    (
                                        Record::new(Arc::clone(output_schema), Vec::new()),
                                        Arc::from(name.as_str()),
                                    )
                                };
                                push_dlq(
                                    ctx,
                                    DlqEntry {
                                        source_row: 0,
                                        category: crate::dlq::DlqErrorCategory::AggregateFinalize,
                                        error_message: format!(
                                            "aggregate {transform}.{binding}: {source:?}"
                                        ),
                                        original_record: synthetic,
                                        stage: Some(crate::dlq::stage_aggregate(name)),
                                        route: None,
                                        trigger: true,
                                        source_name,
                                        triggering_field: None,
                                        triggering_value: None,
                                    },
                                )?;
                                Vec::new()
                            }
                        },
                        Err(other) => return Err(other.into()),
                    }
                }
            };

            ctx.collector
                .record(agg_timer.finish(input_count, out_rows.len() as u64));
            if let Some(region) = current_dag.deferred_region_at_producer(node_idx) {
                // Deferred-region producer. Project emits to the region's
                // buffer schema (the planner already pruned this to the
                // minimum columns the deferred operators reach via
                // `Expr::support_into` plus every windowed-Transform
                // member's `arena_fields`). Park narrow rows in
                // `node_buffers[node_idx]` and build any node-rooted
                // window runtimes against the same narrow projection so
                // a windowed-Transform member finds its slot populated
                // when the commit-time deferred dispatcher walks it —
                // the forward pass produces these emits exactly once and
                // every downstream member runs on the commit pass, so
                // pre-building here matches the strict path's window
                // lifecycle (forward-pass build, downstream consumption)
                // without an extra commit-time materialization for the
                // no-retraction case. Retraction iterations overwrite
                // the slot in `recompute_aggregates::emit_post_recompute`.
                let buffer_schema = region.buffer_schema.clone();
                let projected = project_rows_to_buffer_schema(out_rows, &buffer_schema);
                finalize_node_rooted_windows(ctx, current_dag, node_idx, &projected)?;
                tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &projected)?;
                ctx.node_buffers.insert(node_idx, projected);
            } else {
                // Materialize node-rooted window runtimes for any IndexSpec
                // rooted at this aggregate. The aggregate emits columns the
                // source arena cannot project (e.g. `total = sum(amount)`,
                // `$ck.aggregate.<name>`); a downstream window's IndexSpec
                // pins its `arena_fields` against the aggregate's
                // `output_schema`, so the arena materializes from
                // `out_rows` here, not from the source stream.
                finalize_node_rooted_windows(ctx, current_dag, node_idx, &out_rows)?;
                tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &out_rows)?;
                ctx.node_buffers.insert(node_idx, out_rows);
            }
        }

        PlanNode::Output { ref name, .. } => {
            // Streaming-Output short-circuit (issue #72). The executor
            // entry already moved this Output's writer into a
            // `tokio::spawn`-ed task that drained records from a
            // bounded `mpsc::channel` populated by the fused Merge
            // arm. Per-record `write_record` already fired concurrently
            // with Merge production; the dispatcher's end-of-DAG join
            // surface awaits the task and folds its counters / timers /
            // errors into the context. The Output's topo turn here is
            // a no-op.
            if ctx.streaming_output_nodes.contains(&node_idx) {
                return Ok(());
            }
            // Get input records: check own buffer first (Route
            // nodes store records at the successor's index), then
            // fall back to predecessor buffers.
            let input_records = if let Some(own_buf) = ctx.node_buffers.remove(&node_idx) {
                own_buf
            } else {
                let predecessors: Vec<NodeIndex> = current_dag
                    .graph
                    .neighbors_directed(node_idx, Direction::Incoming)
                    .collect();
                predecessors
                    .iter()
                    .find_map(|&p| {
                        // When multiple outputs share a predecessor,
                        // clone the buffer for all but the last
                        // consumer to avoid starving siblings.
                        let remaining_consumers = current_dag
                            .graph
                            .neighbors_directed(p, Direction::Outgoing)
                            .filter(|&succ| succ > node_idx)
                            .count();
                        if remaining_consumers == 0 {
                            ctx.node_buffers.remove(&p)
                        } else {
                            ctx.node_buffers.get(&p).cloned()
                        }
                    })
                    .unwrap_or_default()
            };

            if let Some(expected) = current_dag.graph[node_idx]
                .expected_input_schema_in(current_dag)
                .cloned()
            {
                let upstream_name = current_dag
                    .graph
                    .neighbors_directed(node_idx, Direction::Incoming)
                    .next()
                    .map(|i| current_dag.graph[i].name().to_string())
                    .unwrap_or_default();
                for (record, _) in &input_records {
                    check_input_schema(&expected, record.schema(), name, "output", &upstream_name)?;
                }
            }

            // When correlation buffering is active, every record
            // routed to this Output goes through the per-group buffer
            // — `CorrelationCommit` decides at end-of-DAG whether to
            // flush the group to the writer or DLQ it. Null-keyed
            // records get a row-disambiguated buffer cell each so
            // they retain per-record-rejection semantics without
            // splitting the writer path.
            let buffered: Vec<(Record, u64, Vec<GroupByKey>)>;
            let unbuffered: Vec<(Record, u64)>;
            if ctx.correlation_buffers.is_some() {
                buffered = input_records
                    .into_iter()
                    .map(|(rec, rn)| {
                        let key = buffer_key_for_record(&rec, rn);
                        (rec, rn, key)
                    })
                    .collect();
                unbuffered = Vec::new();
            } else {
                buffered = Vec::new();
                unbuffered = input_records;
            }

            // Counter semantics:
            //
            // * `records_written` increments per WRITE — under
            //   inclusive Route fan-out, one input matching N
            //   branches counts N (one per Output that received
            //   it). Aligns with per-Output throughput and the
            //   `records_emitted` local that drives stage-metric
            //   reporting.
            //
            // * `ok_count` increments by the number of DISTINCT
            //   source rows reaching this Output that haven't
            //   already been counted at another Output during
            //   the same DAG walk. Source identity is
            //   `row_num` (per-source counter), tracked across
            //   all Output arms via the `ok_source_rows` set
            //   declared at function scope.
            //
            // Buffered records DEFER counter increments to the
            // `CorrelationCommit` arm — clean groups bump
            // counters at flush time; dirty groups never count
            // toward `ok_count`.
            let unbuffered_record_count = unbuffered.len() as u64;
            let mut newly_ok: u64 = 0;
            for (_, row_num) in &unbuffered {
                if ctx.ok_source_rows.insert(*row_num) {
                    newly_ok += 1;
                }
            }
            ctx.counters.ok_count += newly_ok;
            ctx.counters.records_written += unbuffered_record_count;
            ctx.records_emitted += unbuffered_record_count;

            // Derive output schema from first emitted record.
            // The Record is authoritative post-rip; materialize
            // the output-projection's `emitted` / `metadata`
            // maps from it on demand at this boundary. That
            // pays the bucket-insert cost once per record
            // reaching the writer, not every intermediate node
            // transition (Invariant 3).
            let scan_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
            let out_cfg = ctx
                .output_configs
                .iter()
                .find(|o| o.name == *name)
                .unwrap_or(ctx.primary_output);

            // Compute the upstream node's CXL emit names once.
            // `include_widened: false` consults this to drop upstream
            // passthroughs the user did not explicitly emit.
            let cxl_emit_names = current_dag.graph[node_idx].cxl_emit_names_in(current_dag);
            let cxl_emit_names_opt: Option<&[String]> = if cxl_emit_names.is_empty() {
                None
            } else {
                Some(&cxl_emit_names)
            };

            // Buffer non-null-key records. Project once, push slot.
            // Overflow check fires the moment a group's record count
            // exceeds the configured cap; subsequent records of the
            // same group are still admitted so they can become
            // collateral entries when `CorrelationCommit` drains the
            // group, but admission flips the overflow flag so the
            // commit arm emits a `GroupSizeExceeded` trigger.
            if !buffered.is_empty() {
                let max_buf = ctx.correlation_max_group_buffer;
                let buffers = ctx
                    .correlation_buffers
                    .as_mut()
                    .expect("correlation_buffers is Some — we just checked above");
                for (record, rn, group_key) in buffered.iter() {
                    let projected = project_output_from_record(record, out_cfg, cxl_emit_names_opt);
                    let entry = buffers.entry(group_key.clone()).or_default();
                    entry.total_records += 1;
                    if max_buf > 0 && entry.total_records > max_buf {
                        entry.overflowed = true;
                    }
                    entry.records.push(CorrelationRecordSlot {
                        row_num: *rn,
                        original_record: record.clone(),
                        projected,
                        output_name: name.clone(),
                    });
                }
            }

            if unbuffered.is_empty() {
                ctx.collector.record(scan_timer.finish(0, 0));
                return Ok(());
            }

            let output_schema = {
                let projected = {
                    let _guard = ctx.projection_timer.guard();
                    project_output_from_record(&unbuffered[0].0, out_cfg, cxl_emit_names_opt)
                };
                Arc::clone(projected.schema())
            };

            // Find and take the writer for this output. Errors from
            // build_format_writer / write_record / flush are captured
            // into `output_errors` instead of short-circuiting via `?`
            // so siblings still get their chance to fail.
            //
            // Fan-out path: when the plan flagged this Output for
            // per-source-file routing, each record's source_file Arc
            // selects the right writer; the registry holds N writers
            // (one per discovered file).
            if let Some(per_file) = ctx.fan_out_writers.remove(name) {
                emit_fan_out(
                    name,
                    out_cfg,
                    cxl_emit_names_opt,
                    &output_schema,
                    &unbuffered,
                    per_file,
                    &mut ctx.output_errors,
                    &mut ctx.write_timer,
                    &mut ctx.projection_timer,
                    ctx.collector,
                    scan_timer,
                );
            } else if let Some(raw_writer) = ctx.writers.remove(name) {
                match build_format_writer(out_cfg, raw_writer, Arc::clone(&output_schema)) {
                    Ok(mut csv_writer) => {
                        ctx.collector.record(scan_timer.finish(1, 1));
                        let mut write_failed = false;
                        for (record, _rn) in &unbuffered {
                            let projected = {
                                let _guard = ctx.projection_timer.guard();
                                project_output_from_record(record, out_cfg, cxl_emit_names_opt)
                            };
                            let write_result = {
                                let _guard = ctx.write_timer.guard();
                                csv_writer.write_record(&projected)
                            };
                            if let Err(e) = write_result {
                                ctx.output_errors.push(PipelineError::from(e));
                                write_failed = true;
                                break;
                            }
                        }
                        if !write_failed {
                            let flush_result = {
                                let _guard = ctx.write_timer.guard();
                                csv_writer.flush()
                            };
                            if let Err(e) = flush_result {
                                ctx.output_errors.push(PipelineError::from(e));
                            }
                        }
                    }
                    Err(e) => ctx.output_errors.push(e),
                }
            }
        }

        PlanNode::Composition { ref name, body, .. } => {
            // Recursive body execution: collect parent-scope records
            // per declared input port, swap `current_dag` to the body's
            // mini-DAG, walk the body's topo, then collect the body's
            // first declared output port and write it to this node's
            // buffer in the parent scope. The dispatcher arm logic
            // never diverged across body and top-level walks — both
            // run through `dispatch_plan_node` after a current_dag
            // swap, mirroring DataFusion's `RecursiveQueryExec` pattern
            // where the recursive term re-enters the same execution
            // loop with a different plan.
            debug_assert_ne!(
                body,
                crate::plan::composition_body::CompositionBodyId::SENTINEL,
                "composition {name:?}: body_id is sentinel — bind_composition did not run"
            );

            // Verify the body exists; the value itself is no longer
            // needed at this scope — schema checks read the parent
            // graph directly and `collect_port_records` walks live
            // edges, so the body's lookup is deferred to
            // `execute_composition_body`.
            ctx.artifacts
                .body_of(body)
                .ok_or_else(|| PipelineError::compose_body_missing(name.clone()))?;

            // Schema-check parent records before stepping into the
            // body. Failures here surface with the parent-scope
            // upstream name, matching the diagnostic shape every
            // other arm emits at its own entry.
            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            if let Some(expected) = current_dag.graph[node_idx]
                .expected_input_schema_in(current_dag)
                .cloned()
            {
                let upstream_name = predecessors
                    .first()
                    .map(|&i| current_dag.graph[i].name().to_string())
                    .unwrap_or_default();
                // Peek-only schema check; the records are still owned
                // by their producer's buffer until `collect_port_records`
                // claims them below.
                if let Some(&first_pred) = predecessors.first()
                    && let Some(records) = ctx.node_buffers.get(&first_pred)
                {
                    for (record, _) in records {
                        check_input_schema(
                            &expected,
                            record.schema(),
                            name,
                            "composition",
                            &upstream_name,
                        )?;
                    }
                }
            }

            // Depth guard before recursion — same constant the
            // compile-time IsolatedFromAbove check uses, distinct
            // emission code for log greppability.
            if ctx.recursion_depth >= crate::plan::bind_schema::MAX_COMPOSITION_DEPTH {
                return Err(PipelineError::compose_depth_exceeded(
                    name.clone(),
                    ctx.recursion_depth,
                ));
            }

            let composition_name = name.clone();
            let port_records = collect_port_records(ctx, current_dag, node_idx, &composition_name)?;
            let output_records = Box::pin(execute_composition_body(
                ctx,
                body,
                port_records,
                &composition_name,
            ))
            .await?;
            // Materialize node-rooted window runtimes for any IndexSpec
            // rooted at this composition's call-site NodeIndex. The
            // body executor returned with `active_stack` already
            // popped, so the install lands on `top` (parent scope).
            finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
            tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
            ctx.node_buffers.insert(node_idx, output_records);
        }

        PlanNode::Combine {
            ref name,
            ref strategy,
            ref driving_input,
            ref match_mode,
            ref on_miss,
            ref resolved_column_map,
            ref propagate_ck,
            ..
        } => {
            use crate::config::pipeline_node::{MatchMode, OnMiss};
            use crate::executor::combine::{CombineResolver, CombineResolverMapping};
            use crate::pipeline::combine::{CombineHashTable, KeyExtractor};
            use crate::pipeline::grace_hash::{GraceHashExec, execute_combine_grace_hash};
            use crate::pipeline::iejoin::{IEJoinExec, execute_combine_iejoin};
            use crate::pipeline::sort_merge_join::{SortMergeExec, execute_combine_sort_merge};
            use crate::plan::combine::CombineStrategy;

            // Strategy dispatch up front. HashBuildProbe stays
            // inline below (the long-standing path);
            // HashPartitionIEJoin and pure-range IEJoin route
            // to the IEJoin executor; GraceHash routes to the
            // grace-hash executor; SortMerge routes to the
            // sort-merge executor; in-memory hash and the BNL
            // fallback are not yet wired.
            enum Dispatch {
                Inline,
                IEJoin(Option<u8>),
                Grace(u8),
                SortMerge,
            }
            let dispatch = match strategy {
                CombineStrategy::HashBuildProbe => Dispatch::Inline,
                CombineStrategy::HashPartitionIEJoin { partition_bits } => {
                    Dispatch::IEJoin(Some(*partition_bits))
                }
                CombineStrategy::IEJoin => Dispatch::IEJoin(None),
                CombineStrategy::GraceHash { partition_bits } => Dispatch::Grace(*partition_bits),
                CombineStrategy::SortMerge => Dispatch::SortMerge,
                CombineStrategy::InMemoryHash | CombineStrategy::BlockNestedLoop => {
                    return Err(PipelineError::Internal {
                        op: "combine",
                        node: name.clone(),
                        detail: format!(
                            "combine executor does not yet implement strategy {:?}",
                            strategy
                        ),
                    });
                }
            };

            // Cap on matches collected per driver under
            // `match: collect` before truncation. 10K mirrors
            // the module constants in pipeline/combine.rs and
            // aligns with DataFusion's collect-list bound.
            const COLLECT_PER_GROUP_CAP: usize = 10_000;

            // Combine's widened output schema — every emitted
            // record lands on this `Arc<Schema>` so downstream
            // operators hit the ptr_eq fast path and
            // `Record::set` always addresses a known slot.
            let combine_output_schema = current_dag.graph[node_idx].stored_output_schema().cloned();

            let combine_inputs =
                ctx.artifacts
                    .combine_inputs
                    .get(name)
                    .ok_or_else(|| PipelineError::Internal {
                        op: "combine",
                        node: name.clone(),
                        detail: "no combine_inputs entry for combine node".to_string(),
                    })?;
            let decomposed = ctx.artifacts.combine_predicates.get(name).ok_or_else(|| {
                PipelineError::Internal {
                    op: "combine",
                    node: name.clone(),
                    detail: "no combine_predicates entry for combine node".to_string(),
                }
            })?;

            // E312 confines the executor to binary combines.
            // An escaped N>2 combine reaches the executor only
            // when the planner's post-pass skipped stamping —
            // that's a planner bug.
            if combine_inputs.len() != 2 {
                return Err(PipelineError::Internal {
                    op: "combine",
                    node: name.clone(),
                    detail: format!(
                        "combine executor requires binary inputs (N=2); got {}",
                        combine_inputs.len()
                    ),
                });
            }
            if driving_input.is_empty() {
                return Err(PipelineError::Internal {
                    op: "combine",
                    node: name.clone(),
                    detail: "combine has no driving_input stamped (planner post-pass did not run)"
                        .to_string(),
                });
            }

            // Identify the single build-side qualifier —
            // everything that is not the driver.
            let build_qualifier: String = combine_inputs
                .keys()
                .find(|q| q.as_str() != driving_input.as_str())
                .cloned()
                .ok_or_else(|| PipelineError::Internal {
                    op: "combine",
                    node: name.clone(),
                    detail: "no build-side input found among combine inputs".to_string(),
                })?;
            let driver_upstream: &str = combine_inputs[driving_input.as_str()]
                .upstream_name
                .as_ref();
            let build_upstream: &str = combine_inputs[build_qualifier.as_str()]
                .upstream_name
                .as_ref();

            // Resolve predecessor buffers by upstream node name.
            // DAG edges run upstream_source -> combine (or via
            // an intermediate Transform chain). We search the
            // incoming neighbors and match by the node's name.
            //
            // `inject_correlation_sort` splices a synthetic
            // `__correlation_sort_<source>` Sort node between the
            // primary source and its downstream consumers. The
            // suffix carries the original source name; treating
            // any such Sort as an alias for its prefix-stripped
            // upstream lets the combine arm transparently resolve
            // through the splice. See `plan::execution::CORRELATION_SORT_PREFIX`.
            let predecessors: Vec<NodeIndex> = current_dag
                .graph
                .neighbors_directed(node_idx, Direction::Incoming)
                .collect();
            let predecessor_matches = |p: NodeIndex, target: &str| -> bool {
                let pname = current_dag.graph[p].name();
                if pname == target {
                    return true;
                }
                if let Some(stripped) =
                    pname.strip_prefix(crate::plan::execution::CORRELATION_SORT_PREFIX)
                {
                    return stripped == target;
                }
                false
            };
            let driver_pred = predecessors
                .iter()
                .copied()
                .find(|p| predecessor_matches(*p, driver_upstream))
                .ok_or_else(|| PipelineError::Internal {
                    op: "combine",
                    node: name.clone(),
                    detail: format!(
                        "combine driver upstream {driver_upstream:?} is not an \
                         incoming neighbor in the DAG"
                    ),
                })?;
            let build_pred = predecessors
                .iter()
                .copied()
                .find(|p| predecessor_matches(*p, build_upstream))
                .ok_or_else(|| PipelineError::Internal {
                    op: "combine",
                    node: name.clone(),
                    detail: format!(
                        "combine build upstream {build_upstream:?} is not an \
                         incoming neighbor in the DAG"
                    ),
                })?;

            let driver_buf = ctx.node_buffers.remove(&driver_pred).unwrap_or_default();
            let build_buf = ctx.node_buffers.remove(&build_pred).unwrap_or_default();

            // Operator-entry schema check per D4: every record
            // arriving on the probe and build channels is
            // validated against its upstream's compile-time
            // output schema. Arc::ptr_eq is the fast path; a
            // mismatch raises E314.
            let driver_expected = current_dag.graph[driver_pred]
                .output_schema_in(current_dag)
                .clone();
            let build_expected = current_dag.graph[build_pred]
                .output_schema_in(current_dag)
                .clone();
            for (record, _) in &driver_buf {
                check_input_schema(
                    &driver_expected,
                    record.schema(),
                    name,
                    "combine",
                    driver_upstream,
                )?;
            }
            for (record, _) in &build_buf {
                check_input_schema(
                    &build_expected,
                    record.schema(),
                    name,
                    "combine",
                    build_upstream,
                )?;
            }

            // Snapshot per-source cursors for every Source contributing
            // a record to this Combine's fold. Captured at fold start so
            // a Combine-output-row failure rewinds every contributing
            // source independently — driver and build sides treat
            // symmetrically. Cleared at every Combine exit (inline,
            // IEJoin, Grace, SortMerge); the recoverable-DLQ rewind
            // path restores from this entry before clearing. Setup-time
            // `PipelineError::Internal { op: "combine" }` invariant
            // violations still fail-fast and bypass the rewind.
            let mut combine_snapshot: HashMap<Arc<str>, u64> = HashMap::new();
            for (rec, _) in driver_buf.iter().chain(build_buf.iter()) {
                let sn = source_name_arc_of(rec);
                let cursor = ctx.rollback_cursors.get(&sn).copied().unwrap_or(0);
                combine_snapshot.entry(sn).or_insert(cursor);
            }
            ctx.combine_input_snapshots
                .insert(node_idx, combine_snapshot);

            // Build the KeyExtractor pair: one side aligned to
            // the build qualifier, the other to the probe. The
            // i-th equality conjunct contributes one key column
            // to each extractor — the side that matches the
            // build qualifier feeds the build extractor; the
            // OTHER side feeds the probe extractor. For an
            // N-ary chain step, the probe-side qualifier may be
            // a chain-buried original qualifier (e.g. `b` in a
            // step that joins `__combine_X_step_0` against
            // `c`); the executor accepts any non-build
            // qualifier as probe-side and routes its lookup
            // through the resolver mapping below.
            let mut build_progs: Vec<(Arc<TypedProgram>, cxl::ast::Expr)> = Vec::new();
            let mut probe_progs: Vec<(Arc<TypedProgram>, cxl::ast::Expr)> = Vec::new();
            for eq in &decomposed.equalities {
                let (build_expr, build_prog, probe_expr, probe_prog) =
                    if eq.left_input.as_ref() == build_qualifier.as_str() {
                        (
                            eq.left_expr.clone(),
                            Arc::clone(&eq.left_program),
                            eq.right_expr.clone(),
                            Arc::clone(&eq.right_program),
                        )
                    } else if eq.right_input.as_ref() == build_qualifier.as_str() {
                        (
                            eq.right_expr.clone(),
                            Arc::clone(&eq.right_program),
                            eq.left_expr.clone(),
                            Arc::clone(&eq.left_program),
                        )
                    } else {
                        // Neither side matches the build
                        // qualifier — the plan-time
                        // decomposition placed a foreign
                        // conjunct into `equalities`. Planner
                        // bug.
                        return Err(PipelineError::Internal {
                            op: "combine",
                            node: name.clone(),
                            detail: format!(
                                "equality conjunct has qualifiers ({}, {}); \
                                 neither matches build qualifier {build_qualifier:?}",
                                eq.left_input, eq.right_input,
                            ),
                        });
                    };
                build_progs.push((build_prog, build_expr));
                probe_progs.push((probe_prog, probe_expr));
            }
            let build_extractor = KeyExtractor::new(build_progs);
            let probe_extractor = KeyExtractor::new(probe_progs);

            // Resolver mapping is built once and reused for
            // every probe iteration below. The `(side, u32)`
            // index pairs come from the CXL typechecker's
            // pre-resolved column map stashed on the PlanNode;
            // the `bare_to_side` fallback is derived here from
            // `combine_inputs` so unambiguous bare names keep
            // resolving after the resolver is constructed.
            let resolver_mapping =
                CombineResolverMapping::from_pre_resolved(resolved_column_map, combine_inputs);

            // IEJoin / HashPartitionIEJoin / GraceHash
            // dispatch: prep is identical to HashBuildProbe
            // up to this point (input fetch + schema check +
            // resolver mapping), but the matching kernel is
            // strategy-specific.
            match dispatch {
                Dispatch::IEJoin(partition_bits) => {
                    let mut budget =
                        MemoryBudget::from_config(ctx.config.pipeline.memory_limit.as_deref());
                    // Advance per-source `rollback_cursors` for every
                    // build-side record before its `row_num` is dropped
                    // in the `(r, _)` map. Source→Combine direct paths
                    // (no intermediate Transform/Aggregate to advance the
                    // cursor on the way through) would otherwise leave
                    // the build source's cursor anchored at zero. Same
                    // pass advances driver-side cursors for symmetry —
                    // a Source→Combine direct driver has the same gap.
                    for (rec, rn) in &driver_buf {
                        advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                    }
                    for (rec, rn) in &build_buf {
                        advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                    }
                    let build_records: Vec<Record> =
                        build_buf.into_iter().map(|(r, _)| r).collect();
                    let build_records_in = build_records.len() as u64;
                    let build_timer =
                        stage_metrics::StageTimer::new(stage_metrics::StageName::CombineBuild {
                            name: name.clone(),
                        });
                    let build_records_out = build_records.len() as u64;
                    ctx.collector
                        .record(build_timer.finish(build_records_in, build_records_out));
                    let probe_records_in = driver_buf.len() as u64;
                    let probe_timer =
                        stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
                            name: name.clone(),
                        });
                    let body_typed = ctx.artifacts.typed.get(name);
                    let combine_output_schema_arc = combine_output_schema.clone();
                    let iejoin_ctx = EvalContext {
                        stable: ctx.stable,
                        source_file: &MERGED_SOURCE_FILE,
                        source_row: 0,
                        source_path: &MERGED_SOURCE_FILE,
                        source_count: ctx.source_count_by_name(&MERGED_SOURCE_NAME),
                        source_batch: ctx.source_batch_arc,
                        ingestion_timestamp: ctx.source_ingestion_timestamp,
                        source_name: &MERGED_SOURCE_NAME,
                    };
                    // CPU-bound IEJoin kernel — partition + range-walk
                    // + materialize; sized to saturate a worker thread.
                    // `block_in_place` keeps the runtime responsive
                    // for sibling tasks (e.g. concurrent source
                    // ingest still pumping records into other
                    // receivers) while this Combine arm holds the
                    // worker.
                    let output_records = tokio::task::block_in_place(|| {
                        execute_combine_iejoin(IEJoinExec {
                            name,
                            build_qualifier: &build_qualifier,
                            driver_records: driver_buf,
                            build_records,
                            decomposed,
                            body_program: body_typed,
                            resolver_mapping: &resolver_mapping,
                            output_schema: combine_output_schema_arc.as_ref(),
                            match_mode: *match_mode,
                            on_miss: *on_miss,
                            partition_bits,
                            propagate_ck,
                            ctx: &iejoin_ctx,
                            budget: &mut budget,
                        })
                    })?;
                    let probe_records_out = output_records.len() as u64;
                    ctx.collector
                        .record(probe_timer.finish(probe_records_in, probe_records_out));
                    finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
                    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
                    ctx.node_buffers.insert(node_idx, output_records);
                    // Combine arm clean-exit: drop the per-fold cursor
                    // snapshot. Every emitted record has cleared into
                    // `node_buffers` without rewind, so the snapshot is
                    // no longer needed. Mirrors the inline arm's
                    // post-emit clear.
                    ctx.combine_input_snapshots.remove(&node_idx);
                    return Ok(());
                }
                Dispatch::Grace(partition_bits) => {
                    let mut budget =
                        MemoryBudget::from_config(ctx.config.pipeline.memory_limit.as_deref());
                    // Same per-source cursor advance the IEJoin arm
                    // performs; Source→Combine direct paths on either
                    // side need the explicit walk because the build /
                    // driver bufs flow directly out of `node_buffers`
                    // with no operator in between to advance.
                    for (rec, rn) in &driver_buf {
                        advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                    }
                    for (rec, rn) in &build_buf {
                        advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                    }
                    let build_records: Vec<Record> =
                        build_buf.into_iter().map(|(r, _)| r).collect();
                    let build_records_in = build_records.len() as u64;
                    let build_timer =
                        stage_metrics::StageTimer::new(stage_metrics::StageName::CombineBuild {
                            name: name.clone(),
                        });
                    let build_records_out = build_records.len() as u64;
                    ctx.collector
                        .record(build_timer.finish(build_records_in, build_records_out));
                    let probe_records_in = driver_buf.len() as u64;
                    let probe_timer =
                        stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
                            name: name.clone(),
                        });
                    let body_typed = ctx.artifacts.typed.get(name);
                    let combine_output_schema_arc = combine_output_schema.clone();
                    let grace_ctx = EvalContext {
                        stable: ctx.stable,
                        source_file: &MERGED_SOURCE_FILE,
                        source_row: 0,
                        source_path: &MERGED_SOURCE_FILE,
                        source_count: ctx.source_count_by_name(&MERGED_SOURCE_NAME),
                        source_batch: ctx.source_batch_arc,
                        ingestion_timestamp: ctx.source_ingestion_timestamp,
                        source_name: &MERGED_SOURCE_NAME,
                    };
                    // CPU-bound grace-hash join kernel: partition build
                    // + probe + spill I/O. `block_in_place` keeps the
                    // borrow shape unchanged while letting the runtime
                    // park other tasks during the heavy phase.
                    let output_records = tokio::task::block_in_place(|| {
                        execute_combine_grace_hash(GraceHashExec {
                            name,
                            build_qualifier: &build_qualifier,
                            driver_records: driver_buf,
                            build_records,
                            decomposed,
                            body_program: body_typed,
                            resolver_mapping: &resolver_mapping,
                            output_schema: combine_output_schema_arc.as_ref(),
                            match_mode: *match_mode,
                            on_miss: *on_miss,
                            partition_bits,
                            propagate_ck,
                            ctx: &grace_ctx,
                            budget: &mut budget,
                            spill_dir: ctx.spill_root_path.as_ref(),
                        })
                    })?;
                    let probe_records_out = output_records.len() as u64;
                    ctx.collector
                        .record(probe_timer.finish(probe_records_in, probe_records_out));
                    finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
                    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
                    ctx.node_buffers.insert(node_idx, output_records);
                    // Combine arm clean-exit: drop the per-fold cursor
                    // snapshot. Mirrors the inline arm's post-emit
                    // clear so non-inline strategies do not leak
                    // stale snapshot entries across runs.
                    ctx.combine_input_snapshots.remove(&node_idx);
                    return Ok(());
                }
                Dispatch::SortMerge => {
                    // SortMerge is selected by the planner only
                    // for pure-range predicates whose inputs
                    // already arrive sorted on the range key
                    // prefix. The kernel's `presorted: true`
                    // path skips Phase A external sort and walks
                    // the inputs in place via the two-cursor
                    // merge.
                    let mut budget =
                        MemoryBudget::from_config(ctx.config.pipeline.memory_limit.as_deref());
                    // Same per-source cursor advance the IEJoin /
                    // Grace arms perform; Source→Combine direct paths
                    // on either side need the explicit walk because
                    // the build / driver bufs flow directly out of
                    // `node_buffers` with no operator in between to
                    // advance.
                    for (rec, rn) in &driver_buf {
                        advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                    }
                    for (rec, rn) in &build_buf {
                        advance_cursor(ctx, &source_name_arc_of(rec), *rn);
                    }
                    let build_records: Vec<Record> =
                        build_buf.into_iter().map(|(r, _)| r).collect();
                    let build_records_in = build_records.len() as u64;
                    let build_timer =
                        stage_metrics::StageTimer::new(stage_metrics::StageName::CombineBuild {
                            name: name.clone(),
                        });
                    let build_records_out = build_records.len() as u64;
                    ctx.collector
                        .record(build_timer.finish(build_records_in, build_records_out));
                    let probe_records_in = driver_buf.len() as u64;
                    let probe_timer =
                        stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
                            name: name.clone(),
                        });
                    let body_typed = ctx.artifacts.typed.get(name);
                    let combine_output_schema_arc = combine_output_schema.clone();
                    let sm_ctx = EvalContext {
                        stable: ctx.stable,
                        source_file: &MERGED_SOURCE_FILE,
                        source_row: 0,
                        source_path: &MERGED_SOURCE_FILE,
                        source_count: ctx.source_count_by_name(&MERGED_SOURCE_NAME),
                        source_batch: ctx.source_batch_arc,
                        ingestion_timestamp: ctx.source_ingestion_timestamp,
                        source_name: &MERGED_SOURCE_NAME,
                    };
                    // CPU-bound sort-merge join kernel: two-cursor merge
                    // over pre-sorted inputs. `block_in_place` keeps
                    // sibling tasks unblocked during the merge body.
                    let output_records = tokio::task::block_in_place(|| {
                        execute_combine_sort_merge(SortMergeExec {
                            name,
                            build_qualifier: &build_qualifier,
                            driver_records: driver_buf,
                            build_records,
                            decomposed,
                            body_program: body_typed,
                            resolver_mapping: &resolver_mapping,
                            output_schema: combine_output_schema_arc.as_ref(),
                            match_mode: *match_mode,
                            on_miss: *on_miss,
                            presorted: true,
                            propagate_ck,
                            ctx: &sm_ctx,
                            budget: &mut budget,
                            spill_dir: ctx.spill_root_path.as_ref(),
                        })
                    })?;
                    let probe_records_out = output_records.len() as u64;
                    ctx.collector
                        .record(probe_timer.finish(probe_records_in, probe_records_out));
                    finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
                    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
                    ctx.node_buffers.insert(node_idx, output_records);
                    // Combine arm clean-exit: drop the per-fold cursor
                    // snapshot. Mirrors the inline arm's post-emit
                    // clear.
                    ctx.combine_input_snapshots.remove(&node_idx);
                    return Ok(());
                }
                Dispatch::Inline => {}
            }

            // Hash-build phase — drain the build buffer into a
            // fresh MemoryBudget-governed CombineHashTable. The
            // stage timer covers the full build walk; on a
            // budget abort the timer is dropped without
            // recording (matches the `StageTimer` "no report on
            // error" contract documented at its definition).
            let mut budget = MemoryBudget::from_config(ctx.config.pipeline.memory_limit.as_deref());
            // Per-source cursor advance for both build and driver.
            // Source→Combine direct paths bypass the Transform /
            // Aggregate advance points so the cursor never moved off
            // zero for those records; this is the operator-entry
            // advance that catches the gap. The inline arm's driver
            // loop below does not advance per-row inline, so this is
            // the single advance point for driver and build alike.
            for (rec, rn) in &driver_buf {
                advance_cursor(ctx, &source_name_arc_of(rec), *rn);
            }
            for (rec, rn) in &build_buf {
                advance_cursor(ctx, &source_name_arc_of(rec), *rn);
            }
            let build_records: Vec<Record> = build_buf.into_iter().map(|(r, _)| r).collect();
            let build_records_in = build_records.len() as u64;
            let estimated_rows = Some(build_records.len());
            let hash_table_ctx = EvalContext {
                stable: ctx.stable,
                source_file: &MERGED_SOURCE_FILE,
                source_row: 0,
                source_path: &MERGED_SOURCE_FILE,
                source_count: ctx.source_count_by_name(&MERGED_SOURCE_NAME),
                source_batch: ctx.source_batch_arc,
                ingestion_timestamp: ctx.source_ingestion_timestamp,
                source_name: &MERGED_SOURCE_NAME,
            };
            let build_timer =
                stage_metrics::StageTimer::new(stage_metrics::StageName::CombineBuild {
                    name: name.clone(),
                });
            let hash_table = CombineHashTable::build(
                build_records,
                &build_extractor,
                &hash_table_ctx,
                &mut budget,
                estimated_rows,
            )
            .map_err(|e| PipelineError::Compilation {
                transform_name: name.clone(),
                messages: vec![format!("E310 combine build: {e}")],
            })?;
            let build_records_out = hash_table.len() as u64;
            ctx.collector
                .record(build_timer.finish(build_records_in, build_records_out));

            // Body evaluator (only used when the body is not
            // empty — `match: collect` leaves it empty).
            let body_typed = ctx.artifacts.typed.get(name).cloned();
            let mut body_evaluator = body_typed
                .as_ref()
                .map(|bt| ProgramEvaluator::new(Arc::clone(bt), false));

            // Per-driver probe loop. Stage timer covers the
            // full per-driver iteration; on early-return via the
            // 10K-cadence E310 abort or a residual/body eval
            // error, the timer is dropped without recording.
            let probe_records_in = driver_buf.len() as u64;
            let probe_timer =
                stage_metrics::StageTimer::new(stage_metrics::StageName::CombineProbe {
                    name: name.clone(),
                });
            let mut output_records = Vec::with_capacity(driver_buf.len());
            let mut emitted_since_check: usize = 0;
            // Reused across every probe iteration to avoid an
            // allocation per driver row. `KeyExtractor::extract`
            // pushes into the end; we clear before each call.
            let mut probe_keys_buf: Vec<Value> = Vec::with_capacity(probe_extractor.len());

            for (probe_record, rn) in driver_buf {
                let source_file_arc = source_file_arc_of(&probe_record);
                let source_name_arc = source_name_arc_of(&probe_record);
                let eval_ctx = EvalContext {
                    stable: ctx.stable,
                    source_file: &source_file_arc,
                    source_row: rn,
                    source_path: &source_file_arc,
                    source_count: ctx.source_count_by_name(&source_name_arc),
                    source_batch: ctx.source_batch_arc,
                    ingestion_timestamp: ctx.source_ingestion_timestamp,
                    source_name: &source_name_arc,
                };

                // Probe-side key extraction routes through the
                // shared `CombineResolver` so chain-buried
                // qualifiers (e.g. `b.id` against an N-ary
                // decomposition step's encoded intermediate
                // record) resolve via the resolved column map
                // rather than `Record::resolve_qualified`'s
                // bare-name fallback. For non-chain combines
                // the lookup goes through the same path with
                // the same answer (probe-side qualifier maps
                // to its native source-row position).
                let probe_key_resolver =
                    CombineResolver::new(&resolver_mapping, &probe_record, None);
                probe_keys_buf.clear();
                probe_extractor
                    .extract_into(&eval_ctx, &probe_key_resolver, &mut probe_keys_buf)
                    .map_err(|e| PipelineError::Compilation {
                        transform_name: name.clone(),
                        messages: vec![format!("combine probe key eval error: {e}")],
                    })?;

                match match_mode {
                    MatchMode::Collect => {
                        // Synthesize the output directly. No
                        // body evaluator runs. On miss, emit
                        // an empty array (E311-guarded: the
                        // body is enforced empty at compile
                        // time, so on_miss policy does not
                        // bypass emission under Collect).
                        let mut arr: Vec<Value> = Vec::new();
                        let mut first_collected_build: Option<Record> = None;
                        let mut truncated = false;
                        let probe_iter = hash_table.probe(&probe_keys_buf);
                        for candidate in probe_iter {
                            // Residual filter, if any.
                            if let Some(residual) = decomposed.residual.as_ref() {
                                let resolver = CombineResolver::new(
                                    &resolver_mapping,
                                    &probe_record,
                                    Some(candidate.record),
                                );
                                let mut residual_eval =
                                    ProgramEvaluator::new(Arc::clone(residual), false);
                                match residual_eval
                                    .eval_record::<NullStorage>(&eval_ctx, &resolver, None)
                                {
                                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                        continue;
                                    }
                                    Ok(EvalResult::Emit { .. }) => {}
                                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                        continue;
                                    }
                                    Err(e) => {
                                        return Err(PipelineError::from(e));
                                    }
                                }
                            }
                            if arr.len() >= COLLECT_PER_GROUP_CAP {
                                truncated = true;
                                break;
                            }
                            if first_collected_build.is_none() {
                                first_collected_build = Some(candidate.record.clone());
                            }
                            // Build a Value::Map for every matched
                            // build record, preserving its own
                            // schema order. `iter_user_fields`
                            // filters every engine-stamped column
                            // — both `$ck.*` (correlation lineage;
                            // not meaningful nested inside a
                            // collect-array entry) and `$widened`
                            // (auto_widen sidecar; build-side
                            // sidecars drop at the join boundary
                            // by design, mirroring
                            // `propagate_ck: Driver`). Without
                            // this filter, a build record's
                            // `$widened` `Value::Map` payload
                            // nests inside the collect-mode
                            // `Value::Map` and reaches the writer
                            // as a nested Map, triggering
                            // `FormatError::UnserializableMapValue`.
                            let mut m: IndexMap<Box<str>, Value> = IndexMap::new();
                            for (fname, val) in candidate.record.iter_user_fields() {
                                m.insert(fname.into(), val.clone());
                            }
                            arr.push(Value::Map(Box::new(m)));
                        }
                        if truncated {
                            eprintln!(
                                "W: combine {:?} match: collect truncated at \
                                 {COLLECT_PER_GROUP_CAP} matches for driver row {}",
                                name, rn
                            );
                        }

                        // Output record inherits the probe's
                        // data re-projected onto the combine's
                        // widened output_schema; the
                        // `<build_qualifier>` column is
                        // guaranteed to exist on it.
                        let mut rec = match combine_output_schema.as_ref() {
                            Some(s) => widen_record_to_schema(&probe_record, s),
                            None => probe_record.clone(),
                        };
                        // Build-side `$ck.<field>` propagation under
                        // collect mode is single-valued: the first
                        // matched build's CK fills the slot. Every
                        // matched build's full payload is still
                        // preserved inside the array via the
                        // per-row `Value::Map` encoding above, so
                        // nothing is lost — the slot simply mirrors
                        // the first match's identity. Skipped under
                        // `propagate_ck: driver`.
                        if let Some(first_build) = first_collected_build.as_ref() {
                            crate::executor::copy_build_ck_columns(
                                &mut rec,
                                first_build,
                                propagate_ck,
                            );
                        }
                        rec.set(&build_qualifier, Value::Array(arr));
                        output_records.push((rec, rn));
                        emitted_since_check += 1;
                    }

                    MatchMode::First | MatchMode::All => {
                        // Residual-filter + emit pass. We
                        // clone each surviving build record
                        // before dropping the iterator so the
                        // evaluator borrow doesn't alias the
                        // hash-table borrow.
                        let matched_records: Vec<Record> = {
                            let probe_iter = hash_table.probe(&probe_keys_buf);
                            let mut matched: Vec<Record> = Vec::new();
                            for candidate in probe_iter {
                                if let Some(residual) = decomposed.residual.as_ref() {
                                    let resolver = CombineResolver::new(
                                        &resolver_mapping,
                                        &probe_record,
                                        Some(candidate.record),
                                    );
                                    let mut residual_eval =
                                        ProgramEvaluator::new(Arc::clone(residual), false);
                                    match residual_eval
                                        .eval_record::<NullStorage>(&eval_ctx, &resolver, None)
                                    {
                                        Ok(EvalResult::Skip(_)) => continue,
                                        Ok(EvalResult::Emit { .. }) => {}
                                        Err(e) => {
                                            return Err(PipelineError::from(e));
                                        }
                                    }
                                }
                                matched.push(candidate.record.clone());
                                if matches!(match_mode, MatchMode::First) {
                                    break;
                                }
                            }
                            matched
                        };

                        if matched_records.is_empty() {
                            // On-miss dispatch.
                            match on_miss {
                                OnMiss::Skip => {
                                    continue;
                                }
                                OnMiss::Error => {
                                    return Err(PipelineError::Compilation {
                                        transform_name: name.clone(),
                                        messages: vec![format!(
                                            "E310 combine on_miss: error — no \
                                             matching build row for driver row {rn}"
                                        )],
                                    });
                                }
                                OnMiss::NullFields => {
                                    // Evaluate body against a
                                    // resolver whose build
                                    // slot is None — build-
                                    // qualified fields return
                                    // Value::Null.
                                    let resolver = CombineResolver::new(
                                        &resolver_mapping,
                                        &probe_record,
                                        None,
                                    );
                                    let evaluator = body_evaluator.as_mut().ok_or_else(|| {
                                        PipelineError::Internal {
                                            op: "combine",
                                            node: name.clone(),
                                            detail: "combine body typed program \
                                                         missing for on_miss: null_fields"
                                                .to_string(),
                                        }
                                    })?;
                                    match evaluator
                                        .eval_record::<NullStorage>(&eval_ctx, &resolver, None)
                                    {
                                        Ok(EvalResult::Emit {
                                            fields: emitted,
                                            record_vars,
                                            ..
                                        }) => {
                                            let mut rec = match combine_output_schema.as_ref() {
                                                Some(s) => widen_record_to_schema(&probe_record, s),
                                                None => probe_record.clone(),
                                            };
                                            for (n, v) in emitted {
                                                rec.set(&n, v);
                                            }
                                            for (k, v) in *record_vars {
                                                let _ = rec.set_record_var(&k, v);
                                            }
                                            output_records.push((rec, rn));
                                            emitted_since_check += 1;
                                        }
                                        Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                            ctx.counters.filtered_count += 1;
                                        }
                                        Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                            ctx.counters.distinct_count += 1;
                                        }
                                        Err(e) => {
                                            return Err(PipelineError::from(e));
                                        }
                                    }
                                }
                            }
                        } else if let Some(evaluator) = body_evaluator.as_mut() {
                            for matched in &matched_records {
                                let resolver = CombineResolver::new(
                                    &resolver_mapping,
                                    &probe_record,
                                    Some(matched),
                                );
                                match evaluator
                                    .eval_record::<NullStorage>(&eval_ctx, &resolver, None)
                                {
                                    Ok(EvalResult::Emit {
                                        fields: emitted,
                                        record_vars,
                                        ..
                                    }) => {
                                        let mut rec = match combine_output_schema.as_ref() {
                                            Some(s) => widen_record_to_schema(&probe_record, s),
                                            None => probe_record.clone(),
                                        };
                                        for (n, v) in emitted {
                                            rec.set(&n, v);
                                        }
                                        for (k, v) in *record_vars {
                                            let _ = rec.set_record_var(&k, v);
                                        }
                                        // Build-side `$ck.<field>` propagation
                                        // for this matched row. Driver-only
                                        // pipelines short-circuit inside the
                                        // helper; the call is uniform across
                                        // every emit site so the policy is one
                                        // code path for the whole engine.
                                        crate::executor::copy_build_ck_columns(
                                            &mut rec,
                                            matched,
                                            propagate_ck,
                                        );
                                        output_records.push((rec, rn));
                                        emitted_since_check += 1;
                                    }
                                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                        ctx.counters.filtered_count += 1;
                                    }
                                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                                        ctx.counters.distinct_count += 1;
                                    }
                                    Err(e) => {
                                        return Err(PipelineError::from(e));
                                    }
                                }
                            }
                        } else {
                            // Body-less synthetic step from
                            // N-ary combine decomposition: the
                            // step's encoded output schema
                            // concatenates driver columns then
                            // build columns, both in the order
                            // their `intermediate_row.fields()`
                            // walk produces. Emit one record
                            // per match by concatenating value
                            // slices and constructing on the
                            // encoded `Arc<Schema>`.
                            //
                            // No `copy_build_ck_columns` call here:
                            // build-side `$ck.<field>` values are
                            // already in the concatenated tail under
                            // their encoded names (`__<qualifier>__$ck.<field>`).
                            // The chain's final step then resolves
                            // them via `widen_record_to_schema`'s
                            // engine-stamped fallback when projecting
                            // onto the original output schema.
                            let target_schema =
                                combine_output_schema.as_ref().ok_or_else(|| {
                                    PipelineError::Internal {
                                        op: "combine",
                                        node: name.clone(),
                                        detail: "synthetic combine step has no output \
                                             schema; decomposition pass did not run"
                                            .to_string(),
                                    }
                                })?;
                            for matched in &matched_records {
                                let mut values: Vec<Value> =
                                    Vec::with_capacity(target_schema.column_count());
                                values.extend(probe_record.values().iter().cloned());
                                values.extend(matched.values().iter().cloned());
                                if values.len() != target_schema.column_count() {
                                    return Err(PipelineError::Internal {
                                        op: "combine",
                                        node: name.clone(),
                                        detail: format!(
                                            "synthetic combine step produced {} \
                                             concatenated values; encoded schema \
                                             has {} columns",
                                            values.len(),
                                            target_schema.column_count()
                                        ),
                                    });
                                }
                                let rec = Record::new(Arc::clone(target_schema), values);
                                output_records.push((rec, rn));
                                emitted_since_check += 1;
                            }
                        }
                    }
                }

                // Budget check every 10K emitted records to
                // bound memory under fan-out. The build phase
                // polls `should_abort` every 10K inserts inside
                // `CombineHashTable::build`; this loop covers
                // the symmetric probe-side risk where a small
                // build × large driver fan-out can blow RSS
                // even though the table itself is bounded.
                if emitted_since_check >= 10_000 && budget.should_abort() {
                    return Err(PipelineError::Compilation {
                        transform_name: name.clone(),
                        messages: vec![format!(
                            "E310 combine probe memory limit exceeded: \
                             hard limit {}",
                            budget.hard_limit()
                        )],
                    });
                }
                if emitted_since_check >= 10_000 {
                    emitted_since_check = 0;
                }
            }

            let probe_records_out = output_records.len() as u64;
            ctx.collector
                .record(probe_timer.finish(probe_records_in, probe_records_out));
            finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
            tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
            ctx.node_buffers.insert(node_idx, output_records);
            // Drop the per-fold cursor snapshot: every emitted record
            // has cleared into `node_buffers` without rewind, so the
            // snapshot is no longer needed. The rewind path on a
            // Combine-output-row failure reads and restores this entry
            // before clearing.
            ctx.combine_input_snapshots.remove(&node_idx);
        }

        PlanNode::CorrelationCommit { .. } => {
            Box::pin(crate::executor::commit::orchestrate(ctx, current_dag)).await?;
        }
    }

    Ok(())
}

/// Correlation-group buffer entry.
///
/// One per `(group_key, output_name)` cell in
/// [`ExecutorContext::correlation_buffers`]. `records` holds projected
/// output rows captured by the Output arm before any writer commit;
/// `error_set` carries source-row IDs of records that failed somewhere
/// in the pipeline, with the first-error message held alongside for the
/// trigger entry. `total_records` counts every distinct source row that
/// passed through the group (including failures and successes), so the
/// `CorrelationCommit` arm can detect overflow against the configured
/// `max_group_buffer`. `overflowed` short-circuits further admissions
/// once the cap has fired.
///
/// `Clone` is derived so the relaxed-CK orchestrator can snapshot the
/// forward-pass baseline state and restore it at the top of each
/// cascading-retraction iteration. Records are `Arc`-shared, so cloning
/// is cheap; per-iteration restore costs one `Arc::clone` per buffered
/// slot plus one `HashMap` rebuild.
#[derive(Debug, Default, Clone)]
pub(crate) struct CorrelationGroupBuffer {
    pub(crate) records: Vec<CorrelationRecordSlot>,
    pub(crate) error_rows: HashSet<u64>,
    pub(crate) error_messages: Vec<CorrelationErrorRecord>,
    pub(crate) total_records: u64,
    pub(crate) overflowed: bool,
}

/// One buffered output record awaiting commit.
///
/// Captured at the Output arm. `output_name` identifies which writer
/// the record will land in once the group is flushed clean. The
/// `original_record` is held alongside the projected row because a
/// dirty group's DLQ entry uses the original (pre-projection) record
/// to preserve source-side context.
#[derive(Debug, Clone)]
pub(crate) struct CorrelationRecordSlot {
    pub(crate) row_num: u64,
    pub(crate) original_record: Record,
    pub(crate) projected: Record,
    pub(crate) output_name: String,
}

/// One failure event captured against a correlation group.
///
/// Pushed by the Transform / Route / Output arms when correlation
/// buffering is active. `original_record` is the record at the moment
/// of failure (e.g., the Transform input that failed evaluation).
/// Multiple events per row are possible if a row fans out across
/// branches and more than one branch fails — the `CorrelationCommit`
/// arm dedupes by `row_num` for trigger emission.
#[derive(Debug, Clone)]
pub(crate) struct CorrelationErrorRecord {
    pub(crate) row_num: u64,
    pub(crate) original_record: Record,
    pub(crate) category: crate::dlq::DlqErrorCategory,
    pub(crate) error_message: String,
    pub(crate) stage: Option<String>,
    pub(crate) route: Option<String>,
}

/// Walk every correlation buffer and emit the per-group commit:
/// flush-to-writer for clean groups, DLQ-with-trigger for dirty groups,
/// overflow-aware DLQ for groups that tripped `max_group_buffer`.
///
/// Two-phase: phase 1 walks every group once, accumulating clean
/// records into a per-output queue and emitting DLQ entries for
/// dirty/overflow groups. Phase 2 opens each writer ONCE, writes the
/// accumulated queue, and flushes — one writer take per Output, not
/// one per group. The phasing is necessary because every group flush
/// would otherwise contend for `ctx.writers.remove(name)`, leaving
/// subsequent groups silently dropped.
///
/// Single source of truth for both strict and relaxed pipelines. The
/// relaxed-CK orchestrator runs its cascading-retraction loop, then
/// delegates here once the post-recompute aggregate output rows have
/// been substituted into the buffer. The per-group emission shape is
/// identical; only the path that populates the buffer differs.
pub(crate) fn commit_correlation_buffers(
    ctx: &mut ExecutorContext<'_>,
) -> Result<(), PipelineError> {
    use std::collections::BTreeMap;

    let Some(mut buffers) = ctx.correlation_buffers.take() else {
        return Ok(());
    };

    // Stable iteration order — sort by formatted group key so DLQ
    // emission and writer flush are deterministic across runs.
    let mut group_keys: Vec<Vec<GroupByKey>> = buffers.keys().cloned().collect();
    group_keys.sort_by_key(|k| format_group_key(k));

    // Phase 1: per-group disposition. Clean groups' records flow
    // into `clean_per_output` keyed by output_name; the BTreeMap key
    // ordering is the writer-flush order. Dirty groups land in the
    // DLQ inline.
    let mut clean_per_output: BTreeMap<String, Vec<CorrelationRecordSlot>> = BTreeMap::new();
    for group_key in group_keys {
        let Some(group) = buffers.remove(&group_key) else {
            continue;
        };
        commit_one_group(ctx, &group_key, group, &mut clean_per_output)?;
    }

    // Phase 2: drain the clean per-output queues to writers.
    flush_clean_records_to_writers(ctx, clean_per_output)?;

    Ok(())
}

fn format_group_key(key: &[GroupByKey]) -> String {
    let parts: Vec<String> = key
        .iter()
        .map(|k| match k {
            GroupByKey::Null => "null".to_string(),
            GroupByKey::Bool(b) => b.to_string(),
            GroupByKey::Int(i) => i.to_string(),
            GroupByKey::Float(bits) => f64::from_bits(*bits).to_string(),
            GroupByKey::Str(s) => format!("{s:?}"),
            GroupByKey::Date(d) => d.to_string(),
            GroupByKey::DateTime(ts) => ts.to_string(),
        })
        .collect();
    format!("[{}]", parts.join(", "))
}

fn commit_one_group(
    ctx: &mut ExecutorContext<'_>,
    group_key: &[GroupByKey],
    group: CorrelationGroupBuffer,
    clean_per_output: &mut std::collections::BTreeMap<String, Vec<CorrelationRecordSlot>>,
) -> Result<(), PipelineError> {
    let CorrelationGroupBuffer {
        records,
        error_rows,
        error_messages,
        total_records,
        overflowed,
    } = group;

    if overflowed {
        // Overflow disposition: one root-cause DLQ entry with
        // category=GroupSizeExceeded and trigger=true; every other
        // buffered record of the group becomes a collateral with
        // category=Correlated and trigger=false. Per-record original
        // records come from `records` (the projected buffer holds the
        // un-projected original alongside).
        let mut emitted_trigger = false;
        let group_repr = format_group_key(group_key);
        let overflow_msg = PipelineError::CorrelationGroupOverflow {
            group_key: group_repr.clone(),
            count: total_records,
        }
        .to_string();
        // Dedup distinct rows by (source, row_num) so Route fan-out
        // (one row to N outputs) emits one DLQ entry, not N. Pairing on
        // `source_name` matters under multi-source ingest where row_num
        // is per-source.
        let mut seen_rows: HashSet<(Arc<str>, u64)> = HashSet::new();
        for slot in &records {
            let source_name = source_name_arc_of(&slot.original_record);
            if !seen_rows.insert((Arc::clone(&source_name), slot.row_num)) {
                continue;
            }
            let (category, trigger, error_message) = if !emitted_trigger {
                emitted_trigger = true;
                (
                    crate::dlq::DlqErrorCategory::GroupSizeExceeded,
                    true,
                    overflow_msg.clone(),
                )
            } else {
                (
                    crate::dlq::DlqErrorCategory::Correlated,
                    false,
                    format!("correlated with failure in group: {overflow_msg}"),
                )
            };
            push_dlq(
                ctx,
                DlqEntry {
                    source_row: slot.row_num,
                    category,
                    error_message,
                    original_record: slot.original_record.clone(),
                    stage: Some("correlation_commit".to_string()),
                    route: None,
                    trigger,
                    source_name,
                    triggering_field: None,
                    triggering_value: None,
                },
            )?;
        }
        // Per-source rollback rewind: each contributing source rewinds
        // its `rollback_cursors` entry to the lowest `row_num` of any
        // group member from that source. The cursor narrows the replay
        // anchor so a downstream resume reprocesses every record that
        // contributed to the overflowing group, including those whose
        // forward operators had already advanced the cursor past them.
        // No causal-source attribution is required for an overflow —
        // every contributing source shared blame proportionally — so
        // every source contributing a slot rewinds independently.
        let mut per_source_min: HashMap<Arc<str>, u64> = HashMap::new();
        for slot in &records {
            let sn = source_name_arc_of(&slot.original_record);
            per_source_min
                .entry(sn)
                .and_modify(|m| *m = (*m).min(slot.row_num))
                .or_insert(slot.row_num);
        }
        for (sn, min_rn) in per_source_min {
            ctx.rollback_cursors
                .entry(sn)
                .and_modify(|c| *c = (*c).min(min_rn))
                .or_insert(min_rn);
        }
        return Ok(());
    }

    let group_dirty = !error_rows.is_empty();
    if !group_dirty {
        // Clean group → drop the records into the per-output queue
        // for batched flush after every group has been visited.
        for slot in records {
            clean_per_output
                .entry(slot.output_name.clone())
                .or_default()
                .push(slot);
        }
        return Ok(());
    }

    // Dirty group → drop projected records, emit DLQ entries for every
    // distinct row_num touched by the group. Triggers come from
    // `error_messages`; collaterals come from `records` (rows that
    // succeeded their leg but get rolled back because the group failed).
    let first_err_message = error_messages
        .first()
        .map(|e| e.error_message.clone())
        .unwrap_or_else(|| "unknown".to_string());

    // Per-source narrowing: a collateral slot is spared whenever its
    // originating Source did not contribute any trigger error to this
    // group. With multi-source ingest, a single trigger from `src_b`
    // would otherwise DLQ every co-grouped `src_a` slot purely by
    // sharing the correlation key — `src_a` had no causal role in the
    // failure. The set is built from `error_messages` before the
    // trigger loop so each trigger's `$source.name` stamp drives its
    // own narrowing of the collateral walk below. Empty in a
    // single-source pipeline by construction: every co-grouped slot
    // shares the failing source, so the wider behavior is
    // bit-identical to today's pipeline-wide collateral DLQ.
    let failing_sources: HashSet<Arc<str>> = error_messages
        .iter()
        .map(|err| source_name_arc_of(&err.original_record))
        .collect();

    // Dedup distinct rows by (source, row_num) so Route fan-out (one
    // row to N outputs) emits one DLQ entry, not N. Pairing on
    // `source_name` matters under multi-source ingest where row_num is
    // per-source — pre-sprint-7 `HashSet<u64>` collided across sources
    // and silently dropped co-rowed slots from the collateral walk.
    let mut seen_rows: HashSet<(Arc<str>, u64)> = HashSet::new();
    // Trigger entries: one per distinct erroring row. Multiple errors
    // per row (different branches failing) collapse to a single trigger
    // entry carrying the first error.
    for err in &error_messages {
        let source_name = source_name_arc_of(&err.original_record);
        if !seen_rows.insert((Arc::clone(&source_name), err.row_num)) {
            continue;
        }
        push_dlq(
            ctx,
            DlqEntry {
                source_row: err.row_num,
                category: err.category,
                error_message: err.error_message.clone(),
                original_record: err.original_record.clone(),
                stage: err.stage.clone(),
                route: err.route.clone(),
                trigger: true,
                source_name,
                triggering_field: None,
                triggering_value: None,
            },
        )?;
    }
    // Collateral entries: every other distinct row that flowed through
    // the group's Output buffers but didn't itself error. Two sparing
    // axes apply, in order:
    //   1. Per-source narrowing — a slot from a non-failing source is
    //      always spared. Falls back to today's `Any` semantics within
    //      single-source pipelines.
    //   2. CorrelationFanoutPolicy interpretation — Any/All/Primary
    //      operate WITHIN the failing-source's records.
    // The resolved policy is read per-Output (per-Combine / per-Output
    // overrides win against the pipeline default). Today the strict
    // path always resolves to `Any` so axis 2 sparing is a no-op; the
    // wire is in place for the relaxed-CK orchestrator to substitute a
    // different policy.
    for slot in &records {
        let slot_source = source_name_arc_of(&slot.original_record);
        if seen_rows.contains(&(Arc::clone(&slot_source), slot.row_num)) {
            continue;
        }
        // Per-source spare: slot's Source did not contribute a trigger
        // to this group, so it never had a causal role in the failure.
        // Slots stamped as `MERGED_SOURCE_NAME` carry no single-source
        // attribution — Combine outputs (multi-input fold) and
        // synthetic aggregate emits both reach this branch. Treating
        // them as ambiguous, they stay on the collateral path so a
        // downstream failure on a Combine-derived row still flushes
        // the upstream-trigger correlation under today's
        // pipeline-wide-equivalent semantics. Per-source narrowing
        // only spares slots whose stamp identifies a distinct
        // non-failing Source.
        let slot_attributable = slot_source.as_ref() != MERGED_SOURCE_NAME.as_ref();
        if slot_attributable && !failing_sources.contains(&slot_source) {
            clean_per_output
                .entry(slot.output_name.clone())
                .or_default()
                .push(slot.clone());
            continue;
        }
        let policy = crate::executor::commit::output_fanout_policy(ctx, &slot.output_name);
        // Strict path: every slot's CK identity already equals the
        // group's CK identity by construction (the buffer key is
        // shared). `is_full_tuple_match` is `true`; `is_primary_match`
        // is also `true`. Under `Any` the slot DLQs; under `All` the
        // slot DLQs (full match); under `Primary` the slot DLQs
        // (primary match). The relaxed orchestrator's flush path
        // populates these flags differently per slot when multi-CK
        // fan-out makes the match partial.
        let spare = crate::executor::commit::should_spare_collateral(policy, true, true);
        if spare {
            // Spared collateral lands in the per-output clean queue
            // alongside the originally-clean records.
            clean_per_output
                .entry(slot.output_name.clone())
                .or_default()
                .push(slot.clone());
            continue;
        }
        if !seen_rows.insert((Arc::clone(&slot_source), slot.row_num)) {
            continue;
        }
        push_dlq(
            ctx,
            DlqEntry {
                source_row: slot.row_num,
                category: crate::dlq::DlqErrorCategory::Correlated,
                error_message: format!("correlated with failure in group: {first_err_message}"),
                original_record: slot.original_record.clone(),
                stage: Some("correlation_commit".to_string()),
                route: None,
                trigger: false,
                source_name: slot_source,
                triggering_field: None,
                triggering_value: None,
            },
        )?;
    }

    Ok(())
}

fn flush_clean_records_to_writers(
    ctx: &mut ExecutorContext<'_>,
    per_output: std::collections::BTreeMap<String, Vec<CorrelationRecordSlot>>,
) -> Result<(), PipelineError> {
    for (output_name, slots) in per_output {
        if slots.is_empty() {
            continue;
        }
        let out_cfg = ctx
            .output_configs
            .iter()
            .find(|o| o.name == output_name)
            .unwrap_or(ctx.primary_output);
        let output_schema = Arc::clone(slots[0].projected.schema());

        let Some(raw_writer) = ctx.writers.remove(&output_name) else {
            continue;
        };
        match build_format_writer(out_cfg, raw_writer, Arc::clone(&output_schema)) {
            Ok(mut writer) => {
                let mut write_failed = false;
                for slot in &slots {
                    let write_result = {
                        let _guard = ctx.write_timer.guard();
                        writer.write_record(&slot.projected)
                    };
                    if let Err(e) = write_result {
                        ctx.output_errors.push(PipelineError::from(e));
                        write_failed = true;
                        break;
                    }
                }
                if !write_failed {
                    let flush_result = {
                        let _guard = ctx.write_timer.guard();
                        writer.flush()
                    };
                    if let Err(e) = flush_result {
                        ctx.output_errors.push(PipelineError::from(e));
                    }
                }
            }
            Err(e) => ctx.output_errors.push(e),
        }

        // Counter accounting: per-row newly-ok bookkeeping was
        // deferred at Output arm time when buffering. Apply it here
        // so records never count toward `ok_count` if their group
        // rolled back.
        let mut newly_ok: u64 = 0;
        for slot in &slots {
            if ctx.ok_source_rows.insert(slot.row_num) {
                newly_ok += 1;
            }
        }
        ctx.counters.ok_count += newly_ok;
        ctx.counters.records_written += slots.len() as u64;
        ctx.records_emitted += slots.len() as u64;
    }
    Ok(())
}

/// Collect parent-scope records keyed by composition input port name.
///
/// Resolves ports via the live edge graph: walks `parent_dag`'s incoming
/// edges into `composition_node_idx`, reads each edge's `port` tag, and
/// clones records from the producer's `node_buffers` slot. The frozen
/// port-name snapshot kept by an earlier design drifted whenever a
/// planner pass spliced a node between the producer and the composition
/// (the synthetic `inject_correlation_sort` Sort being the canonical
/// trigger), silently emptying the producer's buffer one hop downstream.
/// Edge-walking the live graph is the same source-of-truth pattern every
/// other arm of the dispatcher uses (`Transform`, `Aggregate`, `Output`,
/// `Sort` all read predecessors via `neighbors_directed`).
///
/// Cloning rather than removing keeps the parent producer's buffer
/// intact for any sibling consumer the parent walk has not yet reached;
/// fan-out from a single producer to multiple ports is a normal case.
fn collect_port_records(
    ctx: &ExecutorContext<'_>,
    parent_dag: &ExecutionPlanDag,
    composition_node_idx: NodeIndex,
    composition_name: &str,
) -> Result<IndexMap<String, Vec<(clinker_record::Record, u64)>>, PipelineError> {
    let mut result: IndexMap<String, Vec<(clinker_record::Record, u64)>> = IndexMap::new();
    use petgraph::visit::EdgeRef;
    for edge in parent_dag
        .graph
        .edges_directed(composition_node_idx, Direction::Incoming)
    {
        let Some(port_name) = edge.weight().port.as_ref() else {
            // Composition incoming edges are always port-tagged at
            // bind time; an untagged edge is a planner-pass bug
            // (likely a rewrite that forgot to preserve the tag) and
            // surfaces here as an internal error rather than a silent
            // record drop.
            return Err(PipelineError::Internal {
                op: "composition",
                node: composition_name.to_string(),
                detail: format!(
                    "untagged incoming edge from node {:?}; every composition input edge must \
                     carry a port name (planner-pass invariant — see PlanEdge.port)",
                    parent_dag.graph[edge.source()].name(),
                ),
            });
        };
        let records = ctx
            .node_buffers
            .get(&edge.source())
            .cloned()
            .unwrap_or_default();
        // Two parallel edges to the same port (e.g. `inputs: { p: a,
        // p: a }` — currently rejected at parse, but the runtime is
        // defensive) would overwrite; the wiring pass guarantees
        // unique port names per consumer.
        result.insert(port_name.clone(), records);
    }
    Ok(result)
}

/// Execute one composition body's mini-DAG.
///
/// Builds a transient body-scope `ExecutionPlanDag` and walks it
/// through `dispatch_plan_node` — the same dispatcher entry the
/// top-level walker uses. The body's `node_buffers` namespace
/// is swapped in via `mem::replace` so body NodeIndices index a
/// fresh space; the parent buffers are restored after the walk.
/// The depth-counter guard increments via RAII so `?`-bubbled
/// errors can't leak the counter.
async fn execute_composition_body(
    ctx: &mut ExecutorContext<'_>,
    body_id: crate::plan::composition_body::CompositionBodyId,
    port_records: IndexMap<String, Vec<(clinker_record::Record, u64)>>,
    composition_name: &str,
) -> Result<Vec<(clinker_record::Record, u64)>, PipelineError> {
    use crate::plan::index::PlanIndexRoot;

    // Resolve body and pre-compute everything that needs the
    // bound_body borrow before the swap so the body_dag clone is
    // independent of the artifacts borrow.
    let bound_body = ctx
        .artifacts
        .body_of(body_id)
        .ok_or_else(|| PipelineError::compose_body_missing(composition_name.to_string()))?;

    let body_dag = crate::plan::execution::ExecutionPlanDag::from_body(bound_body);

    // Window runtime entry: install a fresh per-body vec sized to
    // `body_indices_to_build.len()`. ParentNode-rooted slots inherit
    // the parent's runtime via `Arc::clone` from `top` so the body's
    // windowed Transform arm can resolve through to it without re-
    // materializing the parent's arena. Node-rooted body slots stay
    // `None` here — they populate when the body's upstream operator
    // arm calls `finalize_node_rooted_windows` during the body walk.
    let body_index_count = bound_body.body_indices_to_build.len();
    let mut body_window_vec: Vec<Option<crate::executor::window_runtime::WindowRuntime>> =
        (0..body_index_count).map(|_| None).collect();
    for (idx, spec) in bound_body.body_indices_to_build.iter().enumerate() {
        if matches!(spec.root, PlanIndexRoot::ParentNode { .. })
            && let Some(parent_runtime) = ctx.window_runtime.resolve(&spec.root, idx)
        {
            // `resolve` for ParentNode in this position falls through
            // to top[idx] (no body is on the active_stack yet), which
            // is correct: the body inherits the parent operator's
            // runtime by cloning its `Arc<Arena>` and `Arc<SecondaryIndex>`.
            body_window_vec[idx] = Some(parent_runtime.clone());
        }
    }

    // Seed body-scope buffers from parent records keyed by port.
    let mut body_buffers: HashMap<NodeIndex, Vec<(clinker_record::Record, u64)>> = HashMap::new();
    for (port_name, records) in port_records {
        let body_idx = bound_body
            .port_name_to_node_idx
            .get(port_name.as_str())
            .ok_or_else(|| PipelineError::compose_unknown_port(composition_name, &port_name))?;
        body_buffers.insert(*body_idx, records);
    }

    // Pick the body's terminal output node. The bind-time alias
    // resolution wrote the port → NodeIndex map onto BoundBody;
    // the first declared output port wins. Zero-output-port bodies
    // are legal (sink-only / side-effect bodies) and produce no
    // record stream back to the parent.
    let output_idx = bound_body.output_port_to_node_idx.values().next().copied();

    // Swap node_buffers to a body-local namespace so body NodeIndices
    // don't collide with the parent's. `source_records` is also
    // swapped to an empty map so body-scope Source nodes resolve
    // through `node_buffers` (port seeding from parent scope), not
    // through parent-scope source ingestion — bodies declare ports,
    // not top-level sources. Any non-port-seeded body Source surfaces
    // as the defense-in-depth `Internal` error from the Source arm.
    let saved_buffers = std::mem::replace(&mut ctx.node_buffers, body_buffers);
    let saved_combine = std::mem::take(&mut ctx.source_records);
    // Install the body's `input:` reference table so the Route arm
    // can resolve `<route>.<branch>` references against body
    // siblings. Restored on exit.
    let saved_body_refs = ctx
        .current_body_node_input_refs
        .replace(bound_body.node_input_refs.clone());

    // Push the body's window-runtime overlay onto the registry. The
    // ParentNode-rooted slots were populated above via `Arc::clone`
    // from the parent's `top` runtime; Node-rooted body slots stay
    // `None` and populate when the body's upstream operator arm
    // calls `finalize_node_rooted_windows` during the body walk.
    // `active_stack.push` makes `resolve` route through this overlay
    // for any window dispatched inside the body.
    ctx.window_runtime.bodies.insert(body_id, body_window_vec);
    ctx.window_runtime.active_stack.push(body_id);

    // Increment depth before recursing. Every exit path below
    // decrements before returning so the counter stays in sync —
    // including the `walk_result?` early-return at the end. The
    // dispatcher loop already collects errors into `walk_result`
    // rather than `?`-bubbling, so the only `?`-bubble that escapes
    // this function is on `walk_result?` itself, which fires AFTER
    // the decrement.
    ctx.recursion_depth += 1;

    // Walk the body's topo through the same dispatcher the top-level
    // walker uses. Errors from within the body are wrapped with the
    // composition's name for diagnosability — the user sees
    // "in composition '<name>': <inner>" instead of an opaque
    // inner-only message.
    let topo: Vec<NodeIndex> = body_dag.topo_order.clone();
    let mut walk_result: Result<(), PipelineError> = Ok(());
    for node_idx in topo {
        if let Err(inner) = Box::pin(dispatch_plan_node(ctx, &body_dag, node_idx)).await {
            walk_result = Err(PipelineError::compose_body_error(
                composition_name.to_string(),
                Box::new(inner),
            ));
            break;
        }
    }

    // Harvest output before restoring parent buffers. When the output
    // port aliases a deferred-region producer (a relaxed-CK Aggregate
    // sitting at the body's terminal port), the producer's forward
    // emit is NOT a final stream — the commit-pass body→parent
    // harvest path re-emits the post-recompute narrow rows into the
    // parent's `node_buffers[composition_idx]`. Returning the forward
    // emit here would double-feed the parent's continuation: once
    // through the parent Composition arm's `node_buffers.insert`, then
    // again when `recurse_into_body` extends the slot with the
    // commit-pass harvest. Drop the forward emit so the commit pass
    // is the single source of records for that slot.
    let output_records = match (&walk_result, output_idx) {
        (Ok(()), Some(idx)) => {
            let drained = ctx.node_buffers.remove(&idx).unwrap_or_default();
            if body_dag.deferred_region_at_producer(idx).is_some() {
                Vec::new()
            } else {
                drained
            }
        }
        _ => Vec::new(),
    };

    // Decrement depth and restore parent scope. `saturating_sub`
    // is defensive over the invariant; the inc/dec pairs are kept in
    // sync by hand on every exit path through this function.
    ctx.recursion_depth = ctx.recursion_depth.saturating_sub(1);
    ctx.node_buffers = saved_buffers;
    ctx.source_records = saved_combine;
    ctx.current_body_node_input_refs = saved_body_refs;
    // Pop the window-runtime overlay so subsequent windows in the
    // parent scope route through `top` again. Removing the body's
    // entry releases the `Arc` clones (parent runtimes stay alive in
    // `top`; body-local node-rooted runtimes drop here).
    ctx.window_runtime.active_stack.pop();
    ctx.window_runtime.bodies.remove(&body_id);

    walk_result?;
    Ok(output_records)
}

/// Dispatch arm for time-windowed aggregates. Branches the existing
/// strict `PlanNode::Aggregation` flow when `AggregateConfig.time_window`
/// is `Some(_)`. Each window (tumbling / hopping window or per-key
/// session) gets its own [`crate::aggregation::AggregateStream`]
/// instance so the existing per-group hash-aggregator machinery is
/// reused without duplication — windowing partitions records, not the
/// accumulator implementation.
///
/// Close-readiness is decided once at dispatch entry: every window
/// whose `end + allowed_lateness <= min_across_sources` over the
/// upstream Source set is treated as closed, and any record whose
/// assigned window falls in that closed set routes to the DLQ as
/// `DlqErrorCategory::LateRecord`. Open windows accumulate every
/// in-order record; at end-of-input (drain-to-Vec batch model) every
/// open window finalizes and contributes one emit row per
/// (group-by-key) group it observed.
#[allow(clippy::too_many_arguments)]
fn run_time_windowed_aggregate(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    node_idx: NodeIndex,
    name: &str,
    compiled: &Arc<cxl::plan::CompiledAggregate>,
    strategy: AggregateStrategy,
    output_schema: Arc<Schema>,
    spill_schema: Arc<Schema>,
    memory_limit: usize,
    input: &[(Record, u64)],
    spec: &crate::config::pipeline_node::TimeWindowSpec,
    allowed_lateness: Option<std::time::Duration>,
    transform_idx: Option<usize>,
) -> Result<Vec<crate::aggregation::SortRow>, PipelineError> {
    use crate::aggregation::{AggregateStream, HashAggError, SortRow as AggSortRow};
    use crate::config::pipeline_node::TimeWindowSpec;
    use crate::executor::time_window::{
        WindowBounds, duration_to_nanos, hopping_windows, partition_into_sessions,
        record_event_time_nanos, session_is_closed, tumbling_window, upstream_source_names,
        window_is_closed,
    };
    use clinker_record::group_key::value_to_group_key;
    use std::collections::HashMap;

    let upstream_sources = upstream_source_names(current_dag, node_idx);
    let allowed_lateness_nanos = allowed_lateness.map(duration_to_nanos).unwrap_or(0);
    // Per-record streaming watermark: as we walk `input` in arrival
    // order, each record's `$source.event_time` advances its source's
    // running max; the running `min_across_sources` at the moment we
    // examine record N is the min over per-source maxes from records
    // {0..N-1}. A record at event-time `t` whose window
    // `[w_start, w_end)` already satisfies
    // `w_end + allowed_lateness <= running_min` was assigned to a
    // closed window and routes to the DLQ as `LateRecord`. This
    // mirrors Flink's per-record watermark advance under the
    // BoundedOutOfOrdernessWatermarks pattern and gives the dispatch
    // arm correct late-record semantics in batch mode without waiting
    // for the post-execute_dag `ctx.watermarks` fold (which lands
    // after dispatch returns).
    let mut running_per_source_max: std::collections::HashMap<String, i64> =
        std::collections::HashMap::with_capacity(upstream_sources.len());
    let running_min_across =
        |running: &std::collections::HashMap<String, i64>, sources: &[String]| -> Option<i64> {
            sources
                .iter()
                .filter_map(|s| running.get(s.as_str()).copied())
                .min()
        };

    // Factory: each per-window aggregator is a fresh `AggregateStream`
    // sharing the compiled CXL, the output schema, and the spill
    // schema. Building a new `ProgramEvaluator` per window is cheap —
    // the heavy CXL pipeline lives behind `Arc<TypedProgram>` inside
    // `compiled_transforms`.
    let make_stream = |ctx: &ExecutorContext<'_>| -> Result<AggregateStream, PipelineError> {
        let evaluator = match transform_idx {
            Some(idx) => ProgramEvaluator::new(
                Arc::clone(&ctx.compiled_transforms[idx].typed),
                ctx.compiled_transforms[idx].has_distinct(),
            ),
            None => {
                return Err(PipelineError::Internal {
                    op: "time-windowed-aggregation",
                    node: name.to_string(),
                    detail: "no compiled transform found for time-windowed aggregate node"
                        .to_string(),
                });
            }
        };
        AggregateStream::for_node(
            Arc::clone(compiled),
            evaluator,
            strategy,
            Arc::clone(&output_schema),
            Arc::clone(&spill_schema),
            memory_limit,
            Some(ctx.spill_root_path.to_path_buf()),
            name.to_string(),
        )
    };

    let mut out_rows: Vec<AggSortRow> = Vec::new();
    // Per-(group_by) key extraction for session bucketing: mirrors
    // `HashAggregator::add_record`'s prefix walk so session assignment
    // here groups records identically to how the underlying aggregator
    // would have grouped them post-walk.
    let compute_group_key = |record: &Record,
                             row_num: u64|
     -> Result<Vec<GroupByKey>, PipelineError> {
        let mut key: Vec<GroupByKey> = Vec::with_capacity(compiled.group_by_indices.len());
        for (i, idx) in compiled.group_by_indices.iter().enumerate() {
            let field_name = compiled
                .group_by_fields
                .get(i)
                .map(String::as_str)
                .unwrap_or("");
            let val = record
                .values()
                .get(*idx as usize)
                .cloned()
                .unwrap_or(Value::Null);
            match value_to_group_key(&val, field_name, None, row_num) {
                Ok(Some(gk)) => key.push(gk),
                Ok(None) => key.push(GroupByKey::Null),
                Err(e) => {
                    return Err(PipelineError::Internal {
                        op: "time-windowed-aggregation",
                        node: name.to_string(),
                        detail: format!(
                            "group-by key extraction failed for field {field_name:?} at row {row_num}: {e}"
                        ),
                    });
                }
            }
        }
        Ok(key)
    };

    match spec {
        TimeWindowSpec::Tumbling { size } => {
            let size_nanos = duration_to_nanos(*size);
            if size_nanos <= 0 {
                return Err(PipelineError::Internal {
                    op: "time-windowed-aggregation",
                    node: name.to_string(),
                    detail: "tumbling window size must be > 0".to_string(),
                });
            }
            let mut per_window: HashMap<i64, AggregateStream> = HashMap::new();
            tokio::task::block_in_place(|| -> Result<(), PipelineError> {
                for (record, row_num) in input {
                    let Some(t) = record_event_time_nanos(record) else {
                        continue;
                    };
                    let w = tumbling_window(t, size_nanos);
                    // Per-record running watermark BEFORE folding this
                    // record's event-time. Records with `event_time`
                    // older than the watermark by more than
                    // `allowed_lateness` past their window end route
                    // to the DLQ as `LateRecord`.
                    let running_min =
                        running_min_across(&running_per_source_max, &upstream_sources);
                    if window_is_closed(w.end, allowed_lateness_nanos, running_min) {
                        push_late_record(ctx, name, record, *row_num, w)?;
                        continue;
                    }
                    // Fold this record into the per-source running max
                    // AFTER the late check so a late record does not
                    // advance its source's watermark.
                    let src = source_name_arc_of(record).to_string();
                    running_per_source_max
                        .entry(src)
                        .and_modify(|v| {
                            if t > *v {
                                *v = t;
                            }
                        })
                        .or_insert(t);
                    add_to_window(
                        ctx,
                        name,
                        record,
                        *row_num,
                        w.start,
                        &mut per_window,
                        &make_stream,
                        &mut out_rows,
                    )?;
                }
                Ok(())
            })?;
            finalize_windows(ctx, name, per_window, &mut out_rows, &output_schema, input)?;
        }
        TimeWindowSpec::Hopping { size, slide } => {
            let size_nanos = duration_to_nanos(*size);
            let slide_nanos = duration_to_nanos(*slide);
            if size_nanos <= 0 || slide_nanos <= 0 {
                return Err(PipelineError::Internal {
                    op: "time-windowed-aggregation",
                    node: name.to_string(),
                    detail: "hopping window size and slide must both be > 0".to_string(),
                });
            }
            let mut per_window: HashMap<i64, AggregateStream> = HashMap::new();
            tokio::task::block_in_place(|| -> Result<(), PipelineError> {
                for (record, row_num) in input {
                    let Some(t) = record_event_time_nanos(record) else {
                        continue;
                    };
                    let windows = hopping_windows(t, size_nanos, slide_nanos);
                    let running_min =
                        running_min_across(&running_per_source_max, &upstream_sources);
                    // A record is late iff EVERY window it would
                    // belong to is closed at the current watermark.
                    // For overlapping HOP windows, partial closure is
                    // possible (some closed, some still open) — route
                    // the record to its still-open windows and emit a
                    // single DLQ entry only when no window remains
                    // open. Mirrors Flink's late-event-on-sliding
                    // semantics.
                    let mut routed_to_any = false;
                    let mut first_closed: Option<crate::executor::time_window::WindowBounds> = None;
                    for w in windows {
                        if window_is_closed(w.end, allowed_lateness_nanos, running_min) {
                            if first_closed.is_none() {
                                first_closed = Some(w);
                            }
                            continue;
                        }
                        add_to_window(
                            ctx,
                            name,
                            record,
                            *row_num,
                            w.start,
                            &mut per_window,
                            &make_stream,
                            &mut out_rows,
                        )?;
                        routed_to_any = true;
                    }
                    if !routed_to_any {
                        if let Some(w) = first_closed {
                            push_late_record(ctx, name, record, *row_num, w)?;
                            continue;
                        }
                        // No windows at all (slide > size gap) —
                        // record falls in no bucket; silently skip,
                        // matching the empty-window branch in
                        // `hopping_windows`.
                        continue;
                    }
                    let src = source_name_arc_of(record).to_string();
                    running_per_source_max
                        .entry(src)
                        .and_modify(|v| {
                            if t > *v {
                                *v = t;
                            }
                        })
                        .or_insert(t);
                }
                Ok(())
            })?;
            finalize_windows(ctx, name, per_window, &mut out_rows, &output_schema, input)?;
        }
        TimeWindowSpec::Session { gap } => {
            let gap_nanos = duration_to_nanos(*gap);
            if gap_nanos <= 0 {
                return Err(PipelineError::Internal {
                    op: "time-windowed-aggregation",
                    node: name.to_string(),
                    detail: "session window gap must be > 0".to_string(),
                });
            }
            // First pass: bucket input indices by group-by key and
            // record each one's event-time. Records without
            // `$source.event_time` are skipped — session assignment
            // requires a per-record event-time anchor.
            let mut by_key: HashMap<Vec<GroupByKey>, Vec<(usize, i64)>> = HashMap::new();
            for (i, (record, row_num)) in input.iter().enumerate() {
                let Some(t) = record_event_time_nanos(record) else {
                    continue;
                };
                let key = compute_group_key(record, *row_num)?;
                by_key.entry(key).or_default().push((i, t));
            }
            // Second pass: per group, sort indices by event-time,
            // partition into sessions; remember the (group_key,
            // session_idx, session_bounds) assignment per input index.
            type SessionStreamKey = (Vec<GroupByKey>, usize);
            #[derive(Clone)]
            struct PerRecordSession {
                key: Vec<GroupByKey>,
                session_idx: usize,
                session: crate::executor::time_window::SessionInstance,
            }
            let mut record_session_info: Vec<Option<PerRecordSession>> = vec![None; input.len()];
            for (key, mut indexed_times) in by_key {
                indexed_times.sort_by_key(|(_, t)| *t);
                let sorted_times: Vec<i64> = indexed_times.iter().map(|(_, t)| *t).collect();
                let (assignments, sessions) = partition_into_sessions(&sorted_times, gap_nanos);
                for ((record_idx, _t), session_idx) in indexed_times.iter().zip(assignments.iter())
                {
                    record_session_info[*record_idx] = Some(PerRecordSession {
                        key: key.clone(),
                        session_idx: *session_idx,
                        session: sessions[*session_idx].clone(),
                    });
                }
            }
            // Third pass: walk input in arrival order so the
            // streaming running watermark advances monotonically with
            // the per-record check. Bucketing the AggregateStream by
            // (group_key, session_idx) separates session emits
            // without changing the underlying HashAggregator's
            // group-by contract.
            let mut session_streams: HashMap<SessionStreamKey, AggregateStream> = HashMap::new();
            tokio::task::block_in_place(|| -> Result<(), PipelineError> {
                for (i, (rec, rn)) in input.iter().enumerate() {
                    let Some(info) = record_session_info[i].clone() else {
                        continue;
                    };
                    let running_min =
                        running_min_across(&running_per_source_max, &upstream_sources);
                    if session_is_closed(
                        &info.session,
                        gap_nanos,
                        allowed_lateness_nanos,
                        running_min,
                    ) {
                        let bounds = WindowBounds {
                            start: info.session.start,
                            end: info.session.last_event_time.saturating_add(gap_nanos),
                        };
                        push_late_record(ctx, name, rec, *rn, bounds)?;
                        continue;
                    }
                    if let Some(t) = record_event_time_nanos(rec) {
                        let src = source_name_arc_of(rec).to_string();
                        running_per_source_max
                            .entry(src)
                            .and_modify(|v| {
                                if t > *v {
                                    *v = t;
                                }
                            })
                            .or_insert(t);
                    }
                    let stream_key = (info.key, info.session_idx);
                    let entry = session_streams.entry(stream_key);
                    let stream = match entry {
                        std::collections::hash_map::Entry::Occupied(o) => o.into_mut(),
                        std::collections::hash_map::Entry::Vacant(v) => {
                            let fresh = make_stream(ctx)?;
                            v.insert(fresh)
                        }
                    };
                    let source_file_arc = source_file_arc_of(rec);
                    let source_name_arc = source_name_arc_of(rec);
                    let eval_ctx = EvalContext {
                        stable: ctx.stable,
                        source_file: &source_file_arc,
                        source_row: *rn,
                        source_path: &source_file_arc,
                        source_count: ctx.source_count_by_name(&source_name_arc),
                        source_batch: ctx.source_batch_arc,
                        ingestion_timestamp: ctx.source_ingestion_timestamp,
                        source_name: &source_name_arc,
                    };
                    let add_result = stream.add_record(rec, *rn, &eval_ctx, &mut out_rows);
                    if add_result.is_ok() {
                        advance_cursor(ctx, &source_name_arc, *rn);
                    }
                    if let Err(e) = add_result {
                        handle_aggregate_add_error(ctx, name, rec, *rn, e)?;
                    }
                }
                Ok(())
            })?;
            // Finalize every (group, session) stream. Walk in
            // deterministic order (sorted by (group_key, session_idx))
            // so emit order is stable across runs.
            let mut entries: Vec<(SessionStreamKey, AggregateStream)> =
                session_streams.into_iter().collect();
            // `GroupByKey` does not implement `Ord`; fall back to a
            // Debug-formatted key for deterministic finalize order
            // across runs. The session_idx breaks ties for the same
            // group key.
            entries.sort_by(|a, b| {
                let ka: Vec<String> = a.0.0.iter().map(|k| format!("{k:?}")).collect();
                let kb: Vec<String> = b.0.0.iter().map(|k| format!("{k:?}")).collect();
                ka.cmp(&kb).then(a.0.1.cmp(&b.0.1))
            });
            let finalize_ctx = EvalContext {
                stable: ctx.stable,
                source_file: &MERGED_SOURCE_FILE,
                source_row: 0,
                source_path: &MERGED_SOURCE_FILE,
                source_count: ctx.source_count_by_name(&MERGED_SOURCE_NAME),
                source_batch: ctx.source_batch_arc,
                ingestion_timestamp: ctx.source_ingestion_timestamp,
                source_name: &MERGED_SOURCE_NAME,
            };
            for (_, stream) in entries {
                match stream.finalize(&finalize_ctx, &mut out_rows) {
                    Ok(()) => {}
                    Err(HashAggError::Accumulator {
                        transform,
                        binding,
                        source,
                    }) => match ctx.config.error_handling.strategy {
                        ErrorStrategy::FailFast => {
                            return Err(PipelineError::Accumulator {
                                transform,
                                binding,
                                source,
                            });
                        }
                        ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                            emit_aggregate_finalize_dlq(
                                ctx,
                                name,
                                input,
                                &output_schema,
                                &transform,
                                &binding,
                                &source,
                            )?;
                        }
                    },
                    Err(other) => return Err(other.into()),
                }
            }
        }
    }

    Ok(out_rows)
}

/// Route a per-window add for tumbling/hopping. Wraps
/// `AggregateStream::add_record` with the existing per-record DLQ /
/// failfast policy.
#[allow(clippy::too_many_arguments)]
fn add_to_window<F>(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    record: &Record,
    row_num: u64,
    window_start: i64,
    per_window: &mut std::collections::HashMap<i64, crate::aggregation::AggregateStream>,
    make_stream: &F,
    out_rows: &mut Vec<crate::aggregation::SortRow>,
) -> Result<(), PipelineError>
where
    F: Fn(&ExecutorContext<'_>) -> Result<crate::aggregation::AggregateStream, PipelineError>,
{
    let source_file_arc = source_file_arc_of(record);
    let source_name_arc = source_name_arc_of(record);
    let eval_ctx = EvalContext {
        stable: ctx.stable,
        source_file: &source_file_arc,
        source_row: row_num,
        source_path: &source_file_arc,
        source_count: ctx.source_count_by_name(&source_name_arc),
        source_batch: ctx.source_batch_arc,
        ingestion_timestamp: ctx.source_ingestion_timestamp,
        source_name: &source_name_arc,
    };
    let stream = match per_window.entry(window_start) {
        std::collections::hash_map::Entry::Occupied(o) => o.into_mut(),
        std::collections::hash_map::Entry::Vacant(v) => {
            let fresh = make_stream(ctx)?;
            v.insert(fresh)
        }
    };
    let add_result = stream.add_record(record, row_num, &eval_ctx, out_rows);
    if add_result.is_ok() {
        advance_cursor(ctx, &source_name_arc, row_num);
        return Ok(());
    }
    let e = add_result.err().unwrap();
    handle_aggregate_add_error(ctx, name, record, row_num, e)
}

/// Shared per-record `add_record` error handler for both
/// tumbling/hopping and session arms. Mirrors the positional
/// aggregate arm's error-strategy switch.
fn handle_aggregate_add_error(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    record: &Record,
    row_num: u64,
    e: crate::aggregation::HashAggError,
) -> Result<(), PipelineError> {
    match ctx.config.error_handling.strategy {
        ErrorStrategy::FailFast => Err(e.into()),
        ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
            let stage = Some(crate::dlq::stage_aggregate(name));
            let routed = record_error_to_buffer_if_grouped(
                ctx,
                record,
                row_num,
                crate::dlq::DlqErrorCategory::AggregateFinalize,
                format!("aggregate {name}: {e}"),
                stage.clone(),
                None,
            );
            if !routed {
                let source_name = source_name_arc_of(record);
                push_dlq(
                    ctx,
                    DlqEntry {
                        source_row: row_num,
                        category: crate::dlq::DlqErrorCategory::AggregateFinalize,
                        error_message: format!("aggregate {name}: {e}"),
                        original_record: record.clone(),
                        stage,
                        route: None,
                        trigger: true,
                        source_name,
                        triggering_field: None,
                        triggering_value: None,
                    },
                )?;
            }
            Ok(())
        }
    }
}

/// Finalize every per-window aggregator after the per-record walk.
/// Walks windows in ascending `window_start` order for deterministic
/// emit ordering. Accumulator failures route to the DLQ as
/// `AggregateFinalize`, mirroring the positional aggregate arm.
fn finalize_windows(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    per_window: std::collections::HashMap<i64, crate::aggregation::AggregateStream>,
    out_rows: &mut Vec<crate::aggregation::SortRow>,
    output_schema: &Arc<Schema>,
    input: &[(Record, u64)],
) -> Result<(), PipelineError> {
    use crate::aggregation::HashAggError;
    let mut entries: Vec<(i64, crate::aggregation::AggregateStream)> =
        per_window.into_iter().collect();
    entries.sort_by_key(|(start, _)| *start);
    let finalize_ctx = EvalContext {
        stable: ctx.stable,
        source_file: &MERGED_SOURCE_FILE,
        source_row: 0,
        source_path: &MERGED_SOURCE_FILE,
        source_count: ctx.source_count_by_name(&MERGED_SOURCE_NAME),
        source_batch: ctx.source_batch_arc,
        ingestion_timestamp: ctx.source_ingestion_timestamp,
        source_name: &MERGED_SOURCE_NAME,
    };
    for (_, stream) in entries {
        match stream.finalize(&finalize_ctx, out_rows) {
            Ok(()) => {}
            Err(HashAggError::Accumulator {
                transform,
                binding,
                source,
            }) => match ctx.config.error_handling.strategy {
                ErrorStrategy::FailFast => {
                    return Err(PipelineError::Accumulator {
                        transform,
                        binding,
                        source,
                    });
                }
                ErrorStrategy::Continue | ErrorStrategy::BestEffort => {
                    emit_aggregate_finalize_dlq(
                        ctx,
                        name,
                        input,
                        output_schema,
                        &transform,
                        &binding,
                        &source,
                    )?;
                }
            },
            Err(other) => return Err(other.into()),
        }
    }
    Ok(())
}

/// Route a late-arriving record to the DLQ as
/// `DlqErrorCategory::LateRecord`. The error detail carries the
/// window bounds in nanoseconds so a downstream reader can correlate
/// each late drop to the specific time bucket that had closed.
fn push_late_record(
    ctx: &mut ExecutorContext<'_>,
    transform: &str,
    record: &Record,
    row_num: u64,
    bounds: crate::executor::time_window::WindowBounds,
) -> Result<(), PipelineError> {
    let source_name = source_name_arc_of(record);
    push_dlq(
        ctx,
        DlqEntry {
            source_row: row_num,
            category: crate::dlq::DlqErrorCategory::LateRecord,
            error_message: format!(
                "time-window {transform}: record at event-time inside window \
                 [{}, {}) (nanos) which had already closed",
                bounds.start, bounds.end
            ),
            original_record: record.clone(),
            stage: Some(crate::dlq::stage_time_window(transform)),
            route: None,
            trigger: true,
            source_name,
            triggering_field: None,
            triggering_value: None,
        },
    )
}

/// Emit an `AggregateFinalize` DLQ entry for an accumulator failure at
/// finalize-time. Shared by tumbling/hopping `finalize_windows` and
/// the session arm; mirrors the positional aggregate arm's
/// synthetic-record fallback when input is empty.
fn emit_aggregate_finalize_dlq(
    ctx: &mut ExecutorContext<'_>,
    name: &str,
    input: &[(Record, u64)],
    output_schema: &Arc<Schema>,
    transform: &str,
    binding: &str,
    source: &clinker_record::accumulator::AccumulatorError,
) -> Result<(), PipelineError> {
    let (synthetic, source_name) = if let Some((rec, _)) = input.first() {
        let sn = source_name_arc_of(rec);
        (rec.clone(), sn)
    } else {
        (
            Record::new(Arc::clone(output_schema), Vec::new()),
            Arc::from(name),
        )
    };
    push_dlq(
        ctx,
        DlqEntry {
            source_row: 0,
            category: crate::dlq::DlqErrorCategory::AggregateFinalize,
            error_message: format!("aggregate {transform}.{binding}: {source:?}"),
            original_record: synthetic,
            stage: Some(crate::dlq::stage_aggregate(name)),
            route: None,
            trigger: true,
            source_name,
            triggering_field: None,
            triggering_value: None,
        },
    )
}
