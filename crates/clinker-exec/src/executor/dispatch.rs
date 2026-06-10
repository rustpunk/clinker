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
use cxl::eval::{EvalContext, ProgramEvaluator, SkipReason, StableEvalContext};
use petgraph::Direction;
use petgraph::graph::NodeIndex;

use clinker_plan::config::{ErrorStrategy, OutputConfig, PipelineConfig};
use clinker_plan::error::PipelineError;

/// Re-export of the correlation-buffer commit entry point, relocated to
/// [`crate::executor::correlation_dispatch`]. The commit subtree reaches it
/// through this module path, so the re-export preserves that surface.
pub(crate) use crate::executor::correlation_dispatch::commit_correlation_buffers;

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
fn source_name_of(record: &Record) -> Option<&str> {
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
/// thread's `JoinHandle` once its crossbeam `Receiver` drains, then
/// frozen for the duration of the run, so the ratio remains stable
/// across the dispatch loop's `recv` interleaving.
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
/// record as it leaves the Source ingest thread's `SourceIngestChannel`
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
fn check_dlq_rate(ctx: &ExecutorContext<'_>, source: &Arc<str>) -> Result<(), PipelineError> {
    let Some(dlq) = ctx.config.error_handling.dlq.as_ref() else {
        return Ok(());
    };
    let pipeline_min = dlq
        .min_records
        .unwrap_or(clinker_plan::config::DEFAULT_DLQ_MIN_RECORDS);

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
pub(crate) fn buffer_key_for_record(record: &Record, row_num: u64) -> Vec<GroupByKey> {
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
pub(crate) fn record_error_to_buffer_if_grouped(
    ctx: &mut ExecutorContext<'_>,
    record: &Record,
    row_num: u64,
    category: clinker_core_types::dlq::DlqErrorCategory,
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

/// Dispatch a Transform CXL evaluation failure through the shared
/// error path used by every Transform call site.
///
/// Both the fused and buffered Transform arms route eval errors through
/// the same three-way decision: under [`ErrorStrategy::FailFast`] the
/// error propagates immediately; with correlation buffering active the
/// row is parked under its group cell so the group's success/failure
/// stays atomic; otherwise the row is pushed to the run-scoped DLQ with
/// the offending field/value attached. Folding the duplicated arm body
/// here keeps the three-way precedence honored in one place — any
/// future change to how Transform eval errors are routed lands here
/// rather than having to be mirrored across arms.
pub(crate) fn dispatch_transform_eval_error(
    ctx: &mut ExecutorContext<'_>,
    record: Record,
    row_num: u64,
    transform_name: String,
    eval_err: cxl::eval::EvalError,
) -> Result<(), PipelineError> {
    if ctx.strategy == ErrorStrategy::FailFast {
        return Err(eval_err.into());
    }
    // emit_each fan-out overflow is its own DLQ category so users can
    // distinguish "I asked for too much fan-out" from "a coercion
    // failed". Every other eval-path failure stays under
    // TypeCoercionFailure, matching the pre-existing classification.
    let category = match &eval_err.kind {
        cxl::eval::EvalErrorKind::ExpansionLimitExceeded { .. } => {
            clinker_core_types::dlq::DlqErrorCategory::ExpansionLimitExceeded
        }
        _ => clinker_core_types::dlq::DlqErrorCategory::TypeCoercionFailure,
    };
    let stage = Some(DlqEntry::stage_transform(&transform_name));
    let routed = record_error_to_buffer_if_grouped(
        ctx,
        &record,
        row_num,
        category,
        eval_err.to_string(),
        stage.clone(),
        None,
    );
    if routed {
        return Ok(());
    }
    let source_name = source_name_arc_of(&record);
    let triggering_field = eval_err.triggering_field.clone();
    let triggering_value = eval_err.triggering_value();
    push_dlq(
        ctx,
        DlqEntry {
            source_row: row_num,
            category,
            error_message: eval_err.to_string(),
            original_record: record,
            stage,
            route: None,
            trigger: true,
            source_name,
            triggering_field,
            triggering_value,
        },
    )
}

/// Record a sink write/flush failure in `output_errors` instead of
/// short-circuiting, so sibling writers in the same Output still get
/// their chance to flush or report (the DataFusion collection-pattern,
/// PR #14439). The single conversion point for every writer error path —
/// both the single-writer arms (which push into `ctx.output_errors`) and
/// `emit_fan_out` (which threads its own `output_errors` vec) — so a
/// future change to how write errors are attributed lands here once.
pub(crate) fn push_write_error(
    output_errors: &mut Vec<PipelineError>,
    e: impl Into<PipelineError>,
) {
    output_errors.push(e.into());
}

use crate::executor::node_buffer::NodeBuffer;
use crate::executor::schema_check::check_input_schema;
use crate::executor::{
    CompiledRoute, CompiledTransform, DlqEntry, evaluate_single_transform, stage_metrics,
};
use clinker_plan::BudgetCategory;
use clinker_plan::plan::bind_schema::CompileArtifacts;
use clinker_plan::plan::execution::{ExecutionPlanDag, PlanNode};
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
/// * `source_records` — per-source live crossbeam `Receiver`s keyed by
///   Source node name. The Source dispatch arm drains its receiver
///   via `recv`; the paired sender lives on a `std::thread` ingest
///   worker that drives the format reader and pushes through a
///   `SourceIngestChannel`. The `$source.file` per-record stamp travels
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
    /// `Some(n)` once the Source's crossbeam `Receiver` has disconnected
    /// — i.e. the upstream ingest thread closed its sender and we know
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
    pub(crate) node_buffers: HashMap<NodeIndex, NodeBuffer>,
    /// Per-slot consumer registration for `node_buffers`. `admit_node_buffer`
    /// registers a `NodeBufferConsumer` with the pipeline-scoped arbitrator
    /// and stores both the returned `ConsumerId` (used to `unregister_consumer`
    /// at the slot's drain site) and a clone of the `Arc<ConsumerHandle>`
    /// (used by partial-discharge sites to drive `handle.sub_bytes`). Keyed
    /// by the slot's `NodeIndex`; body-scope swaps replace this map alongside
    /// `node_buffers` so a body walk does not pollute the parent scope's
    /// registry.
    pub(crate) node_buffer_consumer_ids: HashMap<
        NodeIndex,
        (
            crate::pipeline::memory::ConsumerId,
            std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
        ),
    >,
    /// Per-slot consumer registration for window-runtime arenas.
    /// `finalize_node_rooted_windows` registers an `ArenaConsumer` with
    /// the pipeline-scoped arbitrator after each arena builds and stores
    /// the returned `ConsumerId` plus a clone of the seeded
    /// `Arc<ConsumerHandle>`. Keyed by the window-runtime slot index —
    /// the same `idx` `WindowRuntimeRegistry::install` uses — so a re-
    /// build at the same slot replaces exactly one entry. Body-scope
    /// swaps replace this map alongside `window_runtime` body overlays
    /// so a body walk's slot-0 arena does not clobber the parent's
    /// slot-0 registration. Without this, window arenas contribute zero
    /// to `sum_consumer_usage`, so `Priority` / `LargestFirst` cannot
    /// see them and the RSS-vs-charged disagreement warning false-trips
    /// when a large arena is the only sizable in-memory state.
    pub(crate) window_arena_consumer_ids: HashMap<
        usize,
        (
            crate::pipeline::memory::ConsumerId,
            std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
        ),
    >,
    /// Per-source live ingest channels keyed by Source node name. Each
    /// declared Source has one `std::thread` pushing records through a
    /// `SourceIngestChannel`; this map holds the paired `Receiver` the
    /// dispatch loop's Source arm drains via `recv`. Replaces the
    /// pre-drained `Vec<(Record, u64)>` carrier: producers run
    /// concurrently with consumption, bounded by channel capacity so
    /// back-pressure flows end-to-end. A missing entry at the Source arm
    /// surfaces as a defense-in-depth Internal error.
    pub(crate) source_records:
        HashMap<String, crossbeam_channel::Receiver<crate::executor::stream_event::StreamEvent>>,
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
    /// have taken ownership of the Source's crossbeam `Receiver` out of
    /// [`Self::source_records`].
    /// The Transform arm drives `recv` per record and runs CXL
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
    /// arm uses `rx.recv_timeout` when its source name is present;
    /// absent entries fall back to blocking `rx.recv`.
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

    /// Pipeline-scoped spill directory bundled with the OS advisory lock held on
    /// its `.lock` file. Allocated once at `execute_dag_branching` start; every
    /// spilling operator (grace hash, sort-merge join, sort buffer, hash
    /// aggregation) writes spill files inside it. Shared across composition body
    /// recursion — body executors do not allocate a fresh subdir.
    ///
    /// Primary cleanup is per-file `tempfile::TempPath` Drop; secondary sweep is
    /// this guard's Drop on panic unwinding or error-return, which closes the
    /// operator-level panic-leak hole observed in Spark (SPARK-3563, SPARK-24340)
    /// and DuckDB (issues 6420, 5878). The held lock marks the directory as owned
    /// by a live process so the next run's startup crash-purge reaps only spill
    /// dirs whose lock it can acquire.
    ///
    /// The `Arc` keeps the directory alive while any outstanding spill file path
    /// borrows it; this matters in error paths where the executor returns before
    /// every reader has been drained. [`SpillDir`]'s `Drop` releases the lock
    /// *before* the directory is removed — on Windows an open handle blocks the
    /// directory deletion, so the lock must close first. The clean-exit path drops
    /// this guard explicitly for deterministic teardown timing; the early-error
    /// returns and panic unwinds drop it implicitly with the rest of `ctx`. Both
    /// run the same lock-before-removal `Drop`, so the ordering holds on every
    /// path by construction rather than by a manual `drop` no error-return could
    /// be trusted to reach.
    ///
    /// [`SpillDir`]: crate::executor::spill_purge::SpillDir
    pub(crate) spill_root: Arc<crate::executor::spill_purge::SpillDir>,
    /// Cached path into [`Self::spill_root`]. Sharing an
    /// `Arc<Path>` keeps every operator-side `to_path_buf()` call
    /// off the `SpillDir::path()` chain in hot paths and bypasses a
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
    /// `mem_limit` envelopes the whole pipeline; per-arena charges
    /// accumulate against this budget so a relaxed pipeline cannot
    /// multiply its declared limit across N upstream operators by
    /// accident.
    /// Held as `Arc<MemoryArbitrator>` so the same instance is the
    /// single seat for every Combine dispatch arm, every per-operator
    /// `MemoryConsumer`, and any Rayon worker the kernel hands off to.
    /// One declared `mem_limit` cannot be multiplied across arms when
    /// every arm clones a handle to the same arbitrator instead of
    /// constructing its own.
    pub(crate) memory_budget: std::sync::Arc<crate::pipeline::memory::MemoryArbitrator>,

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
    /// Per-pipeline footprint flows through pull-mode attribution: the
    /// arbitrator's `should_abort` poll at downstream batch boundaries
    /// guards the hard limit on the cumulative cross-region buffer
    /// payload, same envelope every other operator uses.
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
    /// writer thread. The fused Merge arm checks the map for its index;
    /// if present, it streams each canonicalized record through the
    /// bounded crossbeam channel instead of accumulating into a `Vec`,
    /// and drops its sender at clean exit so the writer thread's `recv`
    /// returns `Err` (channel disconnected). Empty for pipelines that
    /// don't match the topology — every other Output stays on the
    /// buffered path. See
    /// https://github.com/rustpunk/clinker/issues/72.
    pub(crate) streaming_output_senders:
        HashMap<NodeIndex, crossbeam_channel::Sender<crate::executor::stream_event::StreamEvent>>,
    /// Per-streaming-producer-slot arbitrator registration. One
    /// `NodeBufferConsumer` is registered per logical streaming slot at
    /// executor entry (keyed by the producer's `NodeIndex`); its shared
    /// [`crate::pipeline::memory::ConsumerHandle`] is the per-batch charge
    /// counter the producer arm `add_bytes` into on flush and the writer
    /// thread `sub_bytes` out of on drain. Exactly one wrapper per slot so
    /// `sum_consumer_usage` never double-counts a streaming stage. The
    /// `ConsumerId` is unregistered at the end-of-DAG writer join, after
    /// the writer's discharge has completed. Empty for pipelines with no
    /// streaming chain.
    pub(crate) streaming_charge_consumers: HashMap<
        NodeIndex,
        (
            crate::pipeline::memory::ConsumerId,
            std::sync::Arc<crate::pipeline::memory::ConsumerHandle>,
        ),
    >,
    /// `Output` `NodeIndex`es whose writes are streamed by a task
    /// spawned at executor entry. The `Output` dispatch arm short-
    /// circuits at the top when its index is in this set — the writer
    /// has already been moved into the streaming task and there is no
    /// buffered batch to drain. Empty when no streaming chain
    /// qualified.
    pub(crate) streaming_output_nodes: HashSet<NodeIndex>,

    /// `producer → Aggregate` edges whose ingest streams. Keyed by the
    /// producer's `NodeIndex`, valued by the consuming `Aggregate`'s
    /// `NodeIndex`. Computed once at executor entry from the plan's fusion
    /// analysis ([`certify_streaming_edge`] with an `Aggregation`
    /// consumer).
    ///
    /// The producer's own topo turn is short-circuited (the dispatcher
    /// skips a node whose index is a key here): the producer instead runs
    /// inside the consuming Aggregate's dispatch turn, on the main thread,
    /// streaming each batch into a bounded channel while a scoped thread
    /// drives `add_record` off the receiver. Running the producer inside
    /// the consumer's turn is what keeps the bounded `send` from
    /// dead-locking — the drain side is live before the producer's first
    /// flush, and back-pressure flows Aggregate-ingest → producer →
    /// Source. Empty for pipelines with no streaming-ingest Aggregate.
    pub(crate) streaming_aggregate_ingest_edges: HashMap<NodeIndex, NodeIndex>,

    /// `driver-producer → Combine` edges whose probe-side ingest streams.
    /// Keyed by the driver (probe-side) producer's `NodeIndex`, valued by
    /// the consuming `Combine`'s `NodeIndex`. Computed once at executor
    /// entry from the plan's fusion analysis
    /// ([`certify_streaming_edge`] with a `Combine` consumer, resolving
    /// the driver via the plan-stamped `driving_upstream` index).
    ///
    /// The driver producer's own topo turn is short-circuited (the
    /// dispatcher skips a node whose index is a key here): it instead runs
    /// inside the consuming Combine's dispatch turn, on the main thread,
    /// streaming each batch into a bounded channel while the probe consumer
    /// drains it on its own thread. The build side is fully materialized
    /// into the hash table on the main thread *before* the driver producer
    /// is redispatched, so the probe never reads an incomplete table and
    /// the bounded `send` cannot deadlock — the probe consumer is live
    /// before the driver's first flush, and back-pressure flows probe →
    /// driver → Source. Empty for pipelines with no streaming-probe
    /// Combine.
    pub(crate) streaming_combine_probe_edges: HashMap<NodeIndex, NodeIndex>,
    /// `JoinHandle`s for spawned streaming-output writer threads. Owned
    /// by the dispatcher so the end-of-DAG join surface (in
    /// `execute_dag_branching`) folds per-thread counter / timer /
    /// error accounting back into the dispatcher's
    /// `counters` / `records_emitted` / `write_timer` /
    /// `projection_timer` / `ok_source_rows` / `output_errors`.
    /// Drained via `std::mem::take` at the join surface.
    pub(crate) streaming_output_tasks:
        Vec<std::thread::JoinHandle<crate::executor::StreamingOutputTaskOutput>>,

    /// Shared Rayon pool for CPU-bound owned-input kernels (sort,
    /// grace-hash partition build, IEJoin range-walk, sort-merge). Sized
    /// off the run's thread budget; each kernel extracts its owned input
    /// out of `ctx` and runs under `pool.install(...)`, so the closures
    /// capture only owned data plus `&memory_budget` and stay order-
    /// preserving (the kernels emit in a deterministic order independent
    /// of pool scheduling).
    pub(crate) kernel_pool: Arc<rayon::ThreadPool>,

    /// Per-run shutdown handle, cloned from `PipelineRunParams`. The
    /// operator loops poll it at chunk boundaries via
    /// [`Self::check_shutdown`]; `None` disables shutdown signaling.
    pub(crate) shutdown_token: Option<crate::pipeline::shutdown::ShutdownToken>,

    /// Set when a chunk-boundary shutdown poll tripped and the run
    /// unwound early. Surfaced through the report so the CLI can return
    /// the interrupted exit code.
    pub(crate) interrupted: bool,

    /// Resolved per-batch event count for the streaming inter-stage
    /// handoff, from `pipeline.batch_size` (or
    /// [`crate::executor::batch_handoff::DEFAULT_BATCH_SIZE`] when
    /// omitted). A per-Transform `batch_size` override takes precedence
    /// for that one stage via [`Self::batch_size_for`]. Read by the fused
    /// streaming arms to size their [`crate::executor::batch_handoff::EventBatcher`].
    pub(crate) batch_size: usize,

    /// Spill-file compression policy from the workspace `[storage.spill]
    /// compress` knob. Resolved per blocking operator against the slot's
    /// schema width and [`Self::batch_size`] at each spill site, so `auto`
    /// can skip LZ4 on narrow/short batches where the per-frame fixed cost
    /// outweighs the savings. See [`clinker_plan::config::CompressMode`].
    pub(crate) spill_compress: clinker_plan::config::CompressMode,
}

/// Map keying (active composition body, outgoing edge id) to the rows
/// that crossed from a non-deferred upstream into a deferred-region
/// consumer along that edge. The body id is `None` for top-level edges;
/// each composition body has its own EdgeIndex namespace.
type RegionInputBuffers = HashMap<
    (
        Option<clinker_plan::plan::CompositionBodyId>,
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
    /// Arbitrator registration for this aggregator's memory consumer.
    /// The aggregator survives past forward-pass finalize so the commit
    /// phase can retract and re-finalize against it, so its bytes stay
    /// live in `sum_consumer_usage` for that whole window. Held here so
    /// the consumer is unregistered exactly when the aggregator is
    /// dropped — at degrade, where the state is discarded mid-run —
    /// keeping the registry aligned with what is actually live.
    pub(crate) consumer_id: crate::pipeline::memory::ConsumerId,
}

impl<'a> ExecutorContext<'a> {
    /// Poll the run's shutdown flag at an operator chunk boundary. When
    /// the token has tripped (SIGINT/SIGTERM, or a programmatic
    /// request), record the interruption and return
    /// [`PipelineError::Interrupted`] so the dispatch unwinds back to
    /// the join surface, which drops senders and joins worker threads
    /// before the run exits — a graceful run → drain → exit, never an
    /// abort mid-write. Cheap (one relaxed atomic load) when no token is
    /// installed or the flag is clear.
    pub(crate) fn check_shutdown(&mut self) -> Result<(), PipelineError> {
        if self
            .shutdown_token
            .as_ref()
            .is_some_and(|t| t.is_requested())
        {
            self.interrupted = true;
            return Err(PipelineError::Interrupted);
        }
        Ok(())
    }

    /// Resolve `$source.count` by explicit source name. Used by
    /// finalize sites that don't have a per-record source attribution
    /// — they pass [`MERGED_SOURCE_NAME`], whose slot is stamped with
    /// the pipeline-wide total once every per-source slot is `Some`.
    pub(crate) fn source_count_by_name(&self, name: &Arc<str>) -> Option<u64> {
        self.source_count_per_source.get(name).copied().flatten()
    }

    /// True when the Transform at `node_idx` should run via the
    /// fused streaming arm. Composition bodies share `NodeIndex`
    /// numeric space with the top-level plan, so the membership check
    /// is gated by `current_body_node_input_refs.is_none()` —
    /// `fused_transforms` was populated against the top-level plan
    /// only.
    pub(crate) fn should_fuse_transform(&self, node_idx: NodeIndex) -> bool {
        self.current_body_node_input_refs.is_none() && self.fused_transforms.contains(&node_idx)
    }

    /// Take the streaming-Output sender installed under `node_idx`, but
    /// only for a top-level producer. Returns `None` inside a composition
    /// body even when the index happens to match.
    ///
    /// A composition body has no streaming arm by construction: its
    /// terminal is an output *port* harvested back into the parent's
    /// `node_buffers[composition_idx]` (and, for a body whose terminal
    /// aliases a relaxed-CK aggregate, re-emitted by the commit pass), not
    /// a sink `Output` with its own writer thread. The streaming substrate
    /// only ever targets a terminal `Output` writer thread, so there is no
    /// body sender to install — `streaming_output_senders` is computed
    /// over the top-level plan's `Output` nodes alone. Bodies nonetheless
    /// share the `NodeIndex` numeric space with the top-level plan and run
    /// on the same `ExecutorContext`, so a body operator whose index
    /// collides with a top-level Output's producer could otherwise steal
    /// that top-level sender; gating on
    /// `current_body_node_input_refs.is_none()` forecloses that collision.
    pub(crate) fn take_streaming_sender(
        &mut self,
        node_idx: NodeIndex,
    ) -> Option<crossbeam_channel::Sender<crate::executor::stream_event::StreamEvent>> {
        if self.current_body_node_input_refs.is_some() {
            return None;
        }
        self.streaming_output_senders.remove(&node_idx)
    }

    /// Install a bounded streaming-ingest channel for the producer at
    /// `producer_idx`: register a per-edge `NodeBufferConsumer` charge
    /// consumer, key the sender by the producer index so its dispatch arm
    /// streams into the channel via the shared `take_streaming_sender`
    /// path, and return the receiver, the charge handle, and the charge
    /// consumer's id for later unregistration.
    ///
    /// Shared by the streaming-ingest consumers (Aggregate ingest, Combine
    /// probe): both move a producer's emit onto a back-pressured channel a
    /// consumer thread drains. The 256-event bound mirrors the Source
    /// ingest channel so back-pressure paces both ends, and the per-batch
    /// `add_bytes` the producer charges on flush is netted to zero by the
    /// consumer's per-record `sub_bytes` discharge.
    pub(crate) fn install_streaming_ingest_channel(
        &mut self,
        producer_idx: NodeIndex,
    ) -> (
        crossbeam_channel::Receiver<crate::executor::stream_event::StreamEvent>,
        Arc<crate::pipeline::memory::ConsumerHandle>,
        crate::pipeline::memory::ConsumerId,
    ) {
        let (tx, rx) =
            crossbeam_channel::bounded::<crate::executor::stream_event::StreamEvent>(256);
        let charge_handle = crate::pipeline::memory::ConsumerHandle::new();
        let charge_consumer_id = self.memory_budget.register_consumer(Arc::new(
            crate::executor::node_buffer::NodeBufferConsumer::new(charge_handle.clone(), false),
        ));
        self.streaming_output_senders.insert(producer_idx, tx);
        self.streaming_charge_consumers
            .insert(producer_idx, (charge_consumer_id, charge_handle.clone()));
        (rx, charge_handle, charge_consumer_id)
    }

    /// Build the [`crate::executor::batch_handoff::StreamingChargeHandle`]
    /// for the streaming producer slot at `node_idx`, if one was
    /// registered at executor entry.
    ///
    /// The slot's `NodeBufferConsumer` wrapper and its shared
    /// `ConsumerHandle` are installed once per streaming spec; this clones
    /// the handle into a charge handle the producer arm drives per flushed
    /// batch. `spill_allowed` is computed by the caller via
    /// [`node_buffer_spill_allowed`] so a future multi-consumer streaming
    /// topology cannot spill a batch into a slot whose consumer would hit
    /// `NodeBuffer::clone_memory_only`.
    pub(crate) fn streaming_charge_handle(
        &self,
        node_idx: NodeIndex,
        node_name: &str,
        spill_allowed: bool,
    ) -> Option<crate::executor::batch_handoff::StreamingChargeHandle> {
        let (_id, handle) = self.streaming_charge_consumers.get(&node_idx)?;
        Some(crate::executor::batch_handoff::StreamingChargeHandle::new(
            std::sync::Arc::clone(handle),
            std::sync::Arc::clone(&self.memory_budget),
            std::sync::Arc::clone(&self.spill_root_path),
            node_name.to_string(),
            spill_allowed,
            self.spill_compress,
            self.batch_size,
        ))
    }

    /// Resolve the streaming-handoff batch size for the Transform named
    /// `transform_name`: its per-Transform `batch_size` override when
    /// present, otherwise the pipeline-resolved [`Self::batch_size`].
    /// Both have been validated `>= 1` at config time, so the returned
    /// value is always a usable flush threshold.
    pub(crate) fn batch_size_for(&self, transform_name: &str) -> usize {
        self.config
            .nodes
            .iter()
            .find_map(|spanned| match &spanned.value {
                clinker_plan::config::PipelineNode::Transform { header, config }
                    if header.name == transform_name =>
                {
                    config.batch_size
                }
                _ => None,
            })
            .unwrap_or(self.batch_size)
    }

    /// Stamp the per-source finalized count for `source_name` and, if
    /// every per-source slot is now populated, derive and stamp the
    /// pipeline-wide total under [`MERGED_SOURCE_NAME`]. Called by the
    /// Source dispatch arm (and the Merge.interleave fusion arm) when
    /// a source's crossbeam `Receiver` disconnects.
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

    /// Build the per-record [`EvalContext`] view from a record's
    /// engine-stamped `source_file` / `source_name` and its envelope
    /// `doc_ctx`, at `row_num`. The single construction point for every
    /// record-driven dispatch arm (Transform, Route, hash probe,
    /// per-record aggregate add). The returned view borrows only the
    /// caller's argument references plus the context's `'a`-lifetime
    /// stable / batch handles — it does NOT hold a `&self` borrow, so the
    /// caller may mutate other `ExecutorContext` fields (timers, counters,
    /// cursors) while the view is live.
    pub(crate) fn eval_ctx_for_record<'r>(
        &self,
        source_file: &'r Arc<str>,
        source_name: &'r Arc<str>,
        row_num: u64,
        doc_ctx: &'r Arc<clinker_record::DocumentContext>,
    ) -> EvalContext<'r>
    where
        'a: 'r,
    {
        EvalContext {
            stable: self.stable,
            source_file,
            source_row: row_num,
            source_path: source_file,
            source_count: self.source_count_by_name(source_name),
            source_batch: self.source_batch_arc,
            ingestion_timestamp: self.source_ingestion_timestamp,
            source_name,
            doc_ctx,
        }
    }

    /// Build the synthetic-provenance [`EvalContext`] for a finalize /
    /// post-blocking emit that has no per-record source attribution. Uses
    /// the [`MERGED_SOURCE_FILE`] / [`MERGED_SOURCE_NAME`] sentinels,
    /// row 0, the finalized `$source.count` total, and the synthetic
    /// (empty-section) envelope so `$doc.*` resolves to `Null`. The single
    /// construction point for every blocking-operator finalize arm
    /// (aggregate / windowed-aggregate finalize, Combine kernel contexts).
    /// The returned view borrows only `'a`-lifetime and `'static` data, so
    /// it does not hold a `&self` borrow.
    pub(crate) fn merged_eval_ctx<'r>(&self) -> EvalContext<'r>
    where
        'a: 'r,
    {
        EvalContext {
            stable: self.stable,
            source_file: &MERGED_SOURCE_FILE,
            source_row: 0,
            source_path: &MERGED_SOURCE_FILE,
            source_count: self.source_count_by_name(&MERGED_SOURCE_NAME),
            source_batch: self.source_batch_arc,
            ingestion_timestamp: self.source_ingestion_timestamp,
            source_name: &MERGED_SOURCE_NAME,
            doc_ctx: clinker_record::synthetic_document_context_ref(),
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
/// In-flight bytes flow through pull-mode attribution; the
/// arbitrator's `should_abort` poll at downstream batch boundaries
/// guards the pipeline-wide hard limit.
pub(crate) fn tee_emit_to_region_input_buffers(
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
        for (record, rn) in emit_rows {
            ctx.region_input_buffers
                .entry((active_body, edge_id))
                .or_default()
                .push((record.clone(), *rn));
        }
    }
    Ok(())
}

/// Per-row heuristic byte cost. Returns `0` for an empty slice. The
/// formula matches `NodeBuffer::estimated_memory_bytes` so the
/// admission surfaces agree on the same per-row size.
fn estimate_node_buffer_bytes(rows: &[(Record, u64)]) -> u64 {
    let Some((first, _)) = rows.first() else {
        return 0;
    };
    crate::executor::node_buffer::record_byte_cost(first.schema().column_count())
        .saturating_mul(rows.len() as u64)
}

/// Predicate: does the topology at `slot_key` permit a soft-threshold
/// spill at admission time?
///
/// Returns `false` when the slot will be cloned by a downstream
/// consumer rather than drained — multi-consumer fan-out
/// (`Output` arms with more than one Output sharing a producer) and
/// composition input-port edges both reach
/// `NodeBuffer::clone_memory_only`, which panics on `Spilled`/`Mixed`
/// because spill chunks cannot be cheap-cloned for parallel readers.
/// Lifting that constraint requires sharing `Arc<SpillFile<u64>>` across
/// readers and is tracked as a sibling sub-issue of #108 (back-pressure
/// for fan-out spilling). Until that lands, producer-side spill is
/// gated to slots with a single drain consumer.
///
/// Route's per-successor admission already pre-forks into per-branch
/// slots keyed by the successor's `NodeIndex`; each such slot has
/// exactly one outgoing consumer (the successor itself drives one
/// chain), so this predicate returns `true` and spill applies — the
/// canonical Route-fan-out scenario the issue body calls out.
pub(crate) fn node_buffer_spill_allowed(
    current_dag: &ExecutionPlanDag,
    slot_key: NodeIndex,
) -> bool {
    let mut outgoing = 0usize;
    for edge in current_dag
        .graph
        .edges_directed(slot_key, Direction::Outgoing)
    {
        outgoing += 1;
        if outgoing > 1 {
            return false;
        }
        if edge.weight().port.is_some() {
            return false;
        }
    }
    true
}

/// Admit `rows` into a `ctx.node_buffers` slot, choosing between the
/// in-memory and on-disk variants based on the live RSS reading.
///
/// 1. Empty input returns `NodeBuffer::Memory(Vec::new())`.
/// 2. The slot's byte estimate seeds a fresh `NodeBufferConsumer`
///    handle and the arbitrator registers the wrapper. Pull-mode
///    attribution flows through the handle; the arbitrator's
///    `should_abort` poll guards the pipeline-wide hard limit.
/// 3. When `spill_allowed` is `true` and `MemoryArbitrator::should_spill()`
///    reports the RSS soft threshold tripped, the rows flush to a
///    `SpillFile<u64>` via [`node_buffer_spill::spill_node_buffer`].
///    The in-memory charge is discharged immediately and the file size
///    is added to `cumulative_spill_bytes`; an over-quota disk total
///    surfaces `PipelineError::SpillCapExceeded` (E320) — a disk-cap
///    surface deliberately distinct from the memory-budget E310 so a
///    spilled-out volume never reads as an out-of-memory failure.
/// 4. Otherwise rows stay in memory as `NodeBuffer::Memory(rows)`.
///
/// `spill_allowed` should be computed via [`node_buffer_spill_allowed`]
/// for the slot's `NodeIndex` so multi-consumer fan-out and
/// composition input-port slots stay memory-bound (their consumers
/// reach `NodeBuffer::clone_memory_only`, which panics on spill
/// variants).
/// Remove a `node_buffers` slot and unregister its paired
/// `NodeBufferConsumer` from the pipeline-scoped arbitrator. Mirrors
/// `HashMap::remove` shape — returns the buffer if the slot was
/// occupied, `None` otherwise. Every drain site that previously
/// called `ctx.node_buffers.remove(&idx)` routes through this helper
/// so the arbitrator's registry stays aligned with the live slot map.
pub(crate) fn drain_node_buffer_slot(
    ctx: &mut ExecutorContext<'_>,
    node_idx: NodeIndex,
) -> Option<NodeBuffer> {
    if let Some((id, _)) = ctx.node_buffer_consumer_ids.remove(&node_idx) {
        ctx.memory_budget.unregister_consumer(id);
    }
    ctx.node_buffers.remove(&node_idx)
}

pub(crate) fn admit_node_buffer(
    ctx: &mut ExecutorContext<'_>,
    node_name: &str,
    node_idx: NodeIndex,
    rows: Vec<(Record, u64)>,
    puncts: Vec<crate::executor::stream_event::Punctuation>,
    spill_allowed: bool,
) -> Result<NodeBuffer, PipelineError> {
    if rows.is_empty() && puncts.is_empty() {
        return Ok(NodeBuffer::Memory(Vec::new()));
    }
    let bytes = estimate_node_buffer_bytes(&rows);
    // Register a NodeBufferConsumer with the pipeline-scoped
    // arbitrator for every slot admission. The handle's `bytes`
    // counter seeds to the admitted byte estimate so the consumer's
    // current_usage immediately reflects the slot's footprint at
    // policy-poll time. `can_back_pressure` is conservatively false
    // until the static DAG analysis that classifies upstream pause
    // reachability lands; the Priority policy then ranks node_buffer
    // consumers first among spill candidates (priority 0).
    //
    // A prior registration at the same slot (re-admit after partial
    // discharge — e.g. the post-recompute aggregate emit path)
    // unregisters first so the arbitrator's registry holds exactly
    // one wrapper per live slot.
    if let Some((prev_id, _)) = ctx.node_buffer_consumer_ids.remove(&node_idx) {
        ctx.memory_budget.unregister_consumer(prev_id);
    }
    let handle = crate::pipeline::memory::ConsumerHandle::new();
    handle.set_bytes(bytes);
    let consumer_id = ctx.memory_budget.register_consumer(Arc::new(
        crate::executor::node_buffer::NodeBufferConsumer::new(handle.clone(), false),
    ));
    ctx.node_buffer_consumer_ids
        .insert(node_idx, (consumer_id, handle.clone()));
    // Raise the run's high-water mark now that this slot's charge has
    // joined the registry. Sampling at admission (not only on streaming
    // batch charges) makes `peak_consumer_usage_bytes` a faithful peak
    // over every coexisting `node_buffers` slot — which is the quantity
    // dispatch order moves: finishing a chain's blocking consumer before
    // charging the next chain's source keeps fewer slots live at once.
    ctx.memory_budget.sample_peak_consumer_usage();
    if !spill_allowed || !ctx.memory_budget.should_spill() {
        return Ok(NodeBuffer::memory_from_records_and_puncts(rows, puncts));
    }
    // Resolve the spill compression mode against this slot's schema width and
    // the run's batch size, so the file's on-disk format matches what
    // `--explain` projects. `auto` skips LZ4 on narrow/short batches where the
    // per-frame fixed cost outweighs the savings.
    let column_count = rows
        .first()
        .map(|(r, _)| r.schema().column_count())
        .unwrap_or(0);
    let compress = ctx
        .spill_compress
        .resolve_for_schema(column_count, ctx.batch_size as u64);
    match crate::executor::node_buffer_spill::spill_node_buffer(
        rows,
        Some(ctx.spill_root_path.as_ref()),
        compress,
    )? {
        Some((file, count)) => {
            // Rows are now on disk; the in-memory portion of this slot
            // is zero. The handle reflects the operator's live state
            // for the arbitrator's pull-mode `current_usage` —
            // Velox's "reclaimable ≠ held" point.
            handle.set_bytes(0);
            let file_bytes = std::fs::metadata(file.path()).map(|m| m.len()).unwrap_or(0);
            if ctx.memory_budget.record_spill_bytes(node_name, file_bytes) {
                return Err(PipelineError::spill_cap_exceeded(
                    node_name,
                    ctx.memory_budget.max_spill_bytes(),
                    file_bytes,
                    ctx.memory_budget.cumulative_spill_bytes(),
                ));
            }
            Ok(NodeBuffer::Spilled {
                chunks: vec![(file, count)],
                pending_puncts: puncts,
            })
        }
        None => Ok(NodeBuffer::memory_from_records_and_puncts(
            Vec::new(),
            puncts,
        )),
    }
}

/// Hand an operator's already-produced output rows to a downstream
/// streaming `Output` thread over the bounded crossbeam channel `sender`,
/// in `batch_size`-event batches, instead of admitting the whole stage to
/// a charged `node_buffers` slot.
///
/// The callers (single-branch Route, non-fused Merge, streaming-strategy
/// Aggregate, inline Combine probe) have each already materialized their
/// full output in the `rows` Vec by the time they reach this handoff, so
/// this does NOT bound the producer's working set to one batch — `rows`
/// still holds the whole result. What it saves over the materialized path
/// is the *second* full copy: the result is streamed straight to the
/// writer thread rather than admitted to a `node_buffers` slot that would
/// charge the memory budget, become spill-eligible, and be re-drained by
/// the Output arm. The writer thread also overlaps with the next topo
/// node's work. (The fused Source→Transform→Output and fused
/// Merge.interleave paths are the true one-batch-bounded streamers; they
/// pull record-by-record off a live channel and never build a full result
/// Vec — they do not call this helper.)
///
/// Each record is pushed through an [`EventBatcher`] sized at
/// `batch_size`; a full batch is routed through `charge` — the slot's
/// [`crate::executor::batch_handoff::StreamingChargeHandle`] — which
/// `add_bytes` the batch's footprint to the slot's arbitrator wrapper,
/// optionally spills the batch to disk on a soft-threshold trip (when the
/// slot has a single drain consumer), and then sends the events to the
/// writer thread. The writer discharges each record on drain, so the live
/// charge is "batches in flight," never the whole stage. The bounded
/// `send` inside the charge handle is the back-pressure pivot — a slow
/// writer stalls this producer and lets upstream channels fill.
/// Punctuations follow the records (a terminal `Output` consumes
/// punctuations, so their relative position past the records is immaterial
/// to the sink, and the within-records order — the FIFO the writer
/// observes — is preserved by the batcher's append-only cut). Callers
/// reach this only when [`super::certify_streaming_edge`] certified the
/// producer roots no window and tees to no deferred region, so no
/// materialized-tail bookkeeping is skipped that a downstream stage
/// depends on.
pub(crate) fn stream_linear_producer_emit(
    sender: &crossbeam_channel::Sender<crate::executor::stream_event::StreamEvent>,
    batch_size: usize,
    node_name: &str,
    rows: Vec<(Record, u64)>,
    puncts: Vec<crate::executor::stream_event::Punctuation>,
    charge: &crate::executor::batch_handoff::StreamingChargeHandle,
) -> Result<(), PipelineError> {
    let mut batcher = crate::executor::batch_handoff::EventBatcher::new(
        batch_size,
        |batch: crate::executor::batch_handoff::EventBatch| -> Result<(), PipelineError> {
            charge.charge_and_route(
                batch,
                |event: crate::executor::stream_event::StreamEvent| {
                    sender.send(event).map_err(|_| PipelineError::Internal {
                        op: "executor",
                        node: node_name.to_string(),
                        detail: String::from(
                            "streaming Output writer task dropped its receiver before \
                             the streaming producer arm finished",
                        ),
                    })
                },
            )
        },
    );
    for (record, rn) in rows {
        batcher.push_record(record, rn)?;
    }
    for punct in puncts {
        batcher.push_punctuation(punct)?;
    }
    batcher.finish()
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
    let mut out = Record::new(Arc::clone(target), values);
    // Canonicalization reshapes the schema but the record is the same
    // document's row — carry its envelope context forward so
    // `$doc.<section>.<field>` resolves on the canonicalized record.
    out.set_doc_ctx(Arc::clone(r.doc_ctx()));
    out
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
    use clinker_plan::plan::index::PlanIndexRoot;
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
        let arena =
            Arena::from_records(rows, &spec.arena_fields, anchor_schema, &ctx.memory_budget)
                .map_err(|e| PipelineError::MemoryBudgetExceeded {
                    node: current_dag.graph[upstream_idx].name().to_string(),
                    used: ctx.memory_budget.peak_rss().unwrap_or(0),
                    limit: ctx.memory_budget.hard_limit(),
                    source: BudgetCategory::Arena,
                    detail: Some(format!("node-rooted arena build: {e}")),
                })?;
        let arena_len = arena.record_count();
        ctx.collector
            .record(arena_timer.finish(arena_len, arena_len));

        // Register the arena with the pipeline-scoped arbitrator so its
        // projected footprint flows through pull-mode attribution
        // (`sum_consumer_usage`) like every other memory-touching
        // operator. The handle is seeded once from the arena's measured
        // bytes — the arena is immutable after this point, so a single
        // absolute write suffices. A re-build at the same slot (commit-
        // pass deferred-region replay overwrites the slot each
        // iteration) unregisters the stale wrapper first so the registry
        // holds exactly one entry per live arena slot.
        if let Some((stale_id, _)) = ctx.window_arena_consumer_ids.remove(&idx) {
            ctx.memory_budget.unregister_consumer(stale_id);
        }
        let arena_handle = crate::pipeline::memory::ConsumerHandle::new();
        arena_handle.set_bytes(arena.estimated_bytes() as u64);
        let arena_consumer_id = ctx.memory_budget.register_consumer(Arc::new(
            crate::pipeline::arena::ArenaConsumer::new(arena_handle.clone()),
        ));
        ctx.window_arena_consumer_ids
            .insert(idx, (arena_consumer_id, arena_handle));

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
/// crossbeam `Receiver`, runs a fair `crossbeam_channel::Select` over
/// them, applies the same per-record pipeline the non-fused Source arm
/// runs (canonicalize onto the source's plan-time schema, seed
/// `$record.<key>` defaults, seed `$source.<key>` defaults per
/// `(source, file_arc)`, advance the per-source running counter), and
/// re-canonicalizes each emitted record onto the Merge's output schema.
///
/// Live back-pressure semantic: a slow upstream Source's ingest thread
/// does not delay peer Source records from being consumed off their
/// bounded channel. `Select::select` blocks the merge thread (no busy
/// spin) until some receiver has a message or is disconnected, then
/// returns one ready operation; among several ready peers crossbeam
/// picks at random, and registering from a per-iteration rotating
/// cursor offset keeps the choice round-robin-fair so no predecessor
/// monopolizes the schedule.
///
/// This fused path runs UNSEEDED only — seeded interleaves
/// (`interleave_seed: Some(n)`) take the non-fused Merge arm, whose
/// `fastrand`-driven deterministic order is unaffected by this rewrite.
/// Unseeded interleave is arrival-order-driven by contract, which
/// crossbeam's randomized ready-pick matches; no snapshot/baseline test
/// asserts a specific order on this path.
///
/// `streaming_sender` is `Some` iff the executor matched a fused
/// `Merge.interleave → single Output` chain against the streaming
/// eligibility predicate (issue #72). In streaming mode this function
/// pushes each canonicalized record through the bounded channel instead
/// of accumulating into a Vec, applies live back-pressure end-to-end
/// (writer slow → Merge `send` blocks → Sources' channels fill →
/// ingest threads block), and returns an empty Vec at close. In
/// non-streaming mode (`None`) it returns the accumulated
/// `Vec<(Record, u64)>` that the Merge arm then teed into
/// `node_buffers` for downstream consumers.
/// Streaming handoff bundle for the fused `Merge.interleave` arm: the
/// bounded-channel sender to the writer thread, the slot's per-batch
/// charge handle, and the flush batch size. Present only when a single
/// streaming-eligible Output downstream of the Merge installed its sender;
/// absent on the materialized path (the arm accumulates a full `merged`
/// Vec instead). Bundled into one struct so the arm's signature stays
/// under the argument-count lint.
pub(crate) struct MergeStreamHandoff<'a> {
    pub(crate) sender: crossbeam_channel::Sender<crate::executor::stream_event::StreamEvent>,
    pub(crate) charge: &'a crate::executor::batch_handoff::StreamingChargeHandle,
    pub(crate) batch_size: usize,
}

pub(crate) fn merge_fused_interleave(
    ctx: &mut ExecutorContext<'_>,
    current_dag: &ExecutionPlanDag,
    merge_name: &str,
    sorted_preds: &[NodeIndex],
    merge_output_schema: Option<&Arc<Schema>>,
    streaming: Option<MergeStreamHandoff<'_>>,
) -> Result<Vec<(Record, u64)>, PipelineError> {
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
    let mut receivers: Vec<
        Option<crossbeam_channel::Receiver<crate::executor::stream_event::StreamEvent>>,
    > = Vec::with_capacity(sorted_preds.len());
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
    // Streaming-mode batch accumulator: records collect into one
    // `EventBatch` and flush through the slot's charge handle once the
    // batch fills, so the fused Merge participates in per-batch arbitrator
    // accounting (and one-batch soft-threshold spill) exactly like the
    // other streaming arms. `None` in materialized mode (`merged`
    // accumulates the whole result instead).
    let batch_size = streaming.as_ref().map_or(1, |s| s.batch_size.max(1));
    let mut stream_batch: Option<crate::executor::batch_handoff::EventBatch> = streaming
        .as_ref()
        .map(|_| crate::executor::batch_handoff::EventBatch::with_capacity(batch_size));
    let mut per_source_counts: Vec<u64> = vec![0; receivers.len()];
    // Round-robin start cursor rotated each iteration so no
    // predecessor monopolizes the schedule when several are ready.
    // Seeded interleaves (`interleave_seed: Some(_)`) take the
    // non-fused path so their fastrand-driven determinism survives;
    // this fused path runs unseeded only.
    let mut cursor: usize = 0;

    // Each iteration: build a fresh `Select`, register every still-
    // active receiver starting from the rotating cursor offset (so
    // registration order rotates and crossbeam's random ready-pick
    // stays round-robin-fair across iterations), block on `select()`
    // (parks the thread — no busy spin — until some receiver has a
    // message or is disconnected), then map the chosen operation back
    // to its predecessor index. A disconnected channel surfaces as a
    // ready op whose `recv` returns `Err`, so closed-source detection
    // happens inside the same select loop.
    //
    // Flush one accumulated streaming batch through the slot's charge
    // handle: `add_bytes` its footprint, optionally one-batch spill on a
    // soft-threshold trip, then send each event over the bounded channel.
    // Borrows only the streaming sender, the charge handle, and the merge
    // name (never `ctx`), so it composes with the loop's `&mut ctx` work.
    let flush_stream_batch =
        |batch: crate::executor::batch_handoff::EventBatch| -> Result<(), PipelineError> {
            let Some(handoff) = streaming.as_ref() else {
                return Ok(());
            };
            handoff.charge.charge_and_route(
                batch,
                |event: crate::executor::stream_event::StreamEvent| {
                    handoff
                        .sender
                        .send(event)
                        .map_err(|_| PipelineError::Internal {
                            op: "executor",
                            node: merge_name.to_string(),
                            detail: String::from(
                                "streaming Output writer task dropped its receiver \
                             before the Merge arm finished",
                            ),
                        })
                },
            )
        };
    let n = receivers.len();
    loop {
        // Honor cooperative shutdown at the chunk boundary before blocking on
        // the next `select()`. Without this poll a long fused
        // Merge.interleave -> streaming-Output stream ignores a tripped token
        // and runs to natural EOF, blowing the shutdown-latency bound that the
        // non-fused operator loops already respect.
        ctx.check_shutdown()?;
        let start = {
            let s = if n == 0 { 0 } else { cursor % n };
            cursor = cursor.wrapping_add(1);
            s
        };
        let mut sel = crossbeam_channel::Select::new();
        // Map each registered select operation index back to its
        // predecessor index.
        let mut op_to_pred: Vec<usize> = Vec::with_capacity(n);
        for offset in 0..n {
            let i = (start + offset) % n;
            if let Some(rx) = receivers[i].as_ref() {
                sel.recv(rx);
                op_to_pred.push(i);
            }
        }
        if op_to_pred.is_empty() {
            // Every source closed.
            break;
        }
        let oper = sel.select();
        let op_index = oper.index();
        let i = op_to_pred[op_index];
        // Complete the chosen operation on its receiver. `Ok` is a
        // record/punctuation; `Err(RecvError)` means that source's
        // channel disconnected (all senders dropped) — `ok()` maps it to
        // the `None` close arm below.
        let item: Option<crate::executor::stream_event::StreamEvent> = oper
            .recv(
                receivers[i]
                    .as_ref()
                    .expect("select op_index maps to an active receiver"),
            )
            .ok();
        // Fused merge path: punctuations from the per-Source live
        // channels are currently consumed here. Forwarding them
        // through the streaming-output channel with multi-input dedup
        // (Merge-style barrier counter) requires per-source state on
        // this select loop and lands as a follow-up — file under #91
        // backlog. The non-fused Merge arm already deduplicates
        // punctuations via the `all_puncts` collect-then-dedup pass.
        let item: Option<(Record, u64)> = match item {
            Some(crate::executor::stream_event::StreamEvent::Record(r, rn)) => Some((r, rn)),
            Some(crate::executor::stream_event::StreamEvent::Punctuation(_)) => continue,
            None => None,
        };
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
                match stream_batch.as_mut() {
                    // Streaming: accumulate into the current batch and flush
                    // it through the charge handle once it fills. The
                    // charge handle's bounded `send` is the back-pressure
                    // pivot — a slow writer stalls the Merge thread here,
                    // stopping its `select()` calls and letting the Source
                    // channels fill.
                    Some(batch) => {
                        batch.push_record(rec, rn);
                        if batch.len() >= batch_size {
                            let full = std::mem::replace(
                                batch,
                                crate::executor::batch_handoff::EventBatch::with_capacity(
                                    batch_size,
                                ),
                            );
                            flush_stream_batch(full)?;
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

    // Flush the trailing partial batch (streaming mode only). The
    // streaming sender then drops with this function's frame, disconnecting
    // the writer thread's `recv` loop and triggering its flush.
    if let Some(batch) = stream_batch.take()
        && !batch.is_empty()
    {
        flush_stream_batch(batch)?;
    }

    Ok(merged)
}

/// Drive a `PlanNode::Transform` whose sole upstream is a
/// `PlanNode::Source` directly off the Source's crossbeam `Receiver`,
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
pub(crate) fn transform_fused_consume(
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
    // Per-Transform-resolved streaming batch size. The fused loop drives
    // emitted records through an `EventBatcher` of this size, bounding the
    // in-flight working set to one batch before each flush. Resolved
    // before the receiver is consumed so the immutable `ctx` borrow ends
    // before the loop's `&mut ctx` work begins.
    let batch_size = ctx.batch_size_for(name);

    // Streaming inter-stage handoff: when a single streaming-eligible
    // Output downstream of this fused Transform installed its sender under
    // this node's index at executor entry (per `classify_stream_nodes` /
    // `compute_streaming_output_specs`), take it here. Each flushed batch
    // is then sent straight to the writer thread over the bounded channel
    // — the blocking `send` is the back-pressure pivot, so peak
    // inter-stage memory is one batch plus the channel's bound, never the
    // whole stage. The Transform admits no `node_buffers` slot in this
    // mode. When absent (the Transform feeds a fan-out, a window root, or
    // a blocking operator), each batch drains into the stage's full output
    // accumulator and `admit_node_buffer` charges it as one slot, the
    // existing materialized path.
    let streaming_sender = ctx.streaming_output_senders.remove(&node_idx);
    let streaming = streaming_sender.is_some();
    // The per-batch charge handle for the streaming slot. Built before the
    // batcher closure that moves it; `None` in materialized mode where the
    // batch drains into the stage accumulator and `admit_node_buffer`
    // charges the full slot instead.
    let charge = streaming_sender.as_ref().and_then(|_| {
        let spill_allowed = node_buffer_spill_allowed(current_dag, node_idx);
        ctx.streaming_charge_handle(node_idx, name, spill_allowed)
    });

    let mut evaluator_opt: Option<ProgramEvaluator> = transform_idx_opt.map(|idx| {
        ProgramEvaluator::with_max_expansion(
            Arc::clone(&ctx.compiled_transforms[idx].typed),
            ctx.compiled_transforms[idx].has_distinct(),
            ctx.compiled_transforms[idx].max_expansion,
        )
    });

    let rx = ctx
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

    // In materialized mode `output_records` accumulates emitted records
    // (records only), the input to `finalize_node_rooted_windows` and
    // `tee_emit_to_region_input_buffers` below — both reshape records and
    // never read punctuations — and `forwarded_puncts` carries the
    // document-boundary punctuations to the slot tail (matching the
    // non-fused Transform arm). In streaming mode both stay empty: the
    // batcher sink sends every event to the Output thread instead, and a
    // streaming-eligible Transform roots no window and tees to no region
    // (enforced by `certify_streaming_edge`), so the helper calls see
    // an empty slice and the slot is never admitted.
    //
    // The fused loop pushes each emitted record AND each document-boundary
    // punctuation through `event_batcher` in arrival order. The batcher
    // bounds the live working set to `batch_size` events before each
    // flush — the per-batch boundary the streaming inter-stage handoff is
    // built on. Forwarding punctuations (rather than dropping them, as
    // this fused path historically did) lets a downstream document-scoped
    // operator observe the same boundaries the non-fused Transform arm
    // preserves; punctuations are O(1) per document, so they stay tiny
    // next to the record stream — no per-record clone.
    let mut output_records: Vec<(Record, u64)> = Vec::new();
    let mut forwarded_puncts: Vec<crate::executor::stream_event::Punctuation> = Vec::new();
    let mut last_file: Arc<str> = Arc::clone(&MERGED_SOURCE_FILE);
    let mut count: u64 = 0;
    let mut records_since_check: u32 = 0;
    // The batcher's sink either streams each flushed batch to the Output
    // thread (back-pressured `send`, bounding inter-stage memory to one
    // batch) or, in materialized mode, splits the batch into the
    // records-only accumulator (the input to the window/region helpers)
    // and the punctuation list. A send error or accumulation error
    // bubbles out through `loop_result`. The closure owns
    // `streaming_sender` by value so the bounded channel's sender drops
    // when the batcher drops — clean exit then disconnects the writer
    // thread's `recv` loop and triggers its flush.
    let mut event_batcher = crate::executor::batch_handoff::EventBatcher::new(
        batch_size,
        |batch: crate::executor::batch_handoff::EventBatch| -> Result<(), PipelineError> {
            if let Some(tx) = streaming_sender.as_ref() {
                // Route the flushed batch through the slot's charge handle:
                // it `add_bytes` the batch footprint to the arbitrator
                // wrapper, optionally spills the batch on a soft-threshold
                // trip, then sends each event over the bounded channel. The
                // blocking `send` is the back-pressure pivot — a slow writer
                // stalls this producer's fused recv loop and lets the ingest
                // channel fill. A send error means the writer dropped its
                // receiver, so the streaming chain is broken; surface it
                // rather than silently dropping the record.
                let charge = charge
                    .as_ref()
                    .expect("streaming sender implies a registered charge handle");
                return charge.charge_and_route(
                    batch,
                    |event: crate::executor::stream_event::StreamEvent| {
                        tx.send(event).map_err(|_| PipelineError::Internal {
                            op: "executor",
                            node: String::from("fused-transform-stream"),
                            detail: String::from(
                                "streaming Output writer task dropped its receiver \
                                 before the fused Transform arm finished",
                            ),
                        })
                    },
                );
            }
            for event in batch.into_events() {
                match event {
                    crate::executor::stream_event::StreamEvent::Record(r, rn) => {
                        output_records.push((r, rn));
                    }
                    crate::executor::stream_event::StreamEvent::Punctuation(p) => {
                        forwarded_puncts.push(p);
                    }
                }
            }
            Ok(())
        },
    );
    let loop_result: Result<(), PipelineError> = (|| {
        loop {
            let item: Option<crate::executor::stream_event::StreamEvent> = match timeout {
                Some(t) => match rx.recv_timeout(t) {
                    Ok(item) => Some(item),
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        ctx.watermarks
                            .mark_idle(source_name_owned.as_str(), &last_file);
                        continue;
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => None,
                },
                None => rx.recv().ok(),
            };
            // Transform-fused-with-Source: this path consumes the live
            // Source channel directly and drives per-record CXL eval
            // inline. A document-boundary punctuation forwards verbatim onto
            // this stage's output event stream at the position it arrived,
            // so a downstream document-scoped operator observes the same
            // boundaries the non-fused Transform arm preserves. Because it is
            // appended in arrival order, a `DocumentClose` stays after the
            // last record of its own document even when a multi-file Source
            // interleaves several documents through this one fused Transform.
            let (record, rn) = match item {
                Some(crate::executor::stream_event::StreamEvent::Record(r, rn)) => (r, rn),
                Some(crate::executor::stream_event::StreamEvent::Punctuation(p)) => {
                    event_batcher.push_punctuation(p)?;
                    continue;
                }
                None => break,
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
            records_since_check += 1;
            if records_since_check >= 1024 {
                records_since_check = 0;
                ctx.check_shutdown()?;
            }

            if let Some(exp) = expected_input.as_ref() {
                check_input_schema(exp, rec.schema(), name, "transform", &source_name_owned)?;
            }

            if let Some(evaluator) = evaluator_opt.as_mut() {
                let source_file_arc = Arc::clone(&last_file);
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
                    doc_ctx: rec.doc_ctx(),
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
                    Ok(records) => {
                        if records.is_empty() {
                            // No-record outcome (e.g. emit_each on Null source);
                            // still advance the cursor to keep watermarks live.
                            advance_cursor(ctx, &source_name_arc_of(&rec), rn);
                        } else {
                            let mut emitted_any = false;
                            for (modified_record, status) in records {
                                match status {
                                    Ok(()) => {
                                        let advance_source = source_name_arc_of(&modified_record);
                                        event_batcher.push_record(modified_record, rn)?;
                                        advance_cursor(ctx, &advance_source, rn);
                                        emitted_any = true;
                                    }
                                    Err(SkipReason::Filtered) => {
                                        ctx.counters.filtered_count += 1;
                                    }
                                    Err(SkipReason::Duplicate) => {
                                        ctx.counters.distinct_count += 1;
                                    }
                                }
                            }
                            if !emitted_any {
                                advance_cursor(ctx, &source_name_arc_of(&rec), rn);
                            }
                        }
                    }
                    Err((transform_name, eval_err)) => {
                        dispatch_transform_eval_error(ctx, rec, rn, transform_name, eval_err)?;
                    }
                }
            } else {
                // Transform has no compiled CXL — passthrough behavior
                // mirrors the buffered arm's `transform_by_name.get` miss
                // at the top of the existing path.
                let advance_source = source_name_arc_of(&rec);
                event_batcher.push_record(rec, rn)?;
                advance_cursor(ctx, &advance_source, rn);
            }
        }
        Ok(())
    })();
    // Surface a loop error first (the originating failure), then flush the
    // trailing partial batch. `finish` consumes the batcher, dropping the
    // streaming sender (clean-exit disconnect of the writer thread) or
    // releasing the borrow of `output_records` for the materialized-path
    // helper calls below; on the loop-error path the batcher drops here
    // instead, which also drops the sender / releases the borrow.
    loop_result?;
    event_batcher.finish()?;
    ctx.finalize_source_count(&source_name_arc, count);
    if timeout.is_some() && ctx.watermarks.is_idle(source_name_owned.as_str()) {
        tracing::debug!(
            target: "clinker::watermark",
            source = %source_name_owned,
            "fused transform consumer ended with all partitions in WatermarkStatus::Idle"
        );
    }

    // Streaming mode already handed every record to the Output thread over
    // the bounded channel; it admits no `node_buffers` slot, so peak
    // inter-stage memory for this stage is one batch rather than the whole
    // output. The window-root / region-tee helpers are skipped because a
    // streaming-eligible Transform roots no node-anchored window and tees
    // to no deferred region (enforced by `certify_streaming_edge`), and
    // `output_records` is empty in this mode anyway.
    if streaming {
        return Ok(());
    }

    finalize_node_rooted_windows(ctx, current_dag, node_idx, &output_records)?;
    tee_emit_to_region_input_buffers(ctx, current_dag, node_idx, &output_records)?;
    let nb = admit_node_buffer(
        ctx,
        name,
        node_idx,
        output_records,
        forwarded_puncts,
        node_buffer_spill_allowed(current_dag, node_idx),
    )?;
    ctx.node_buffers.insert(node_idx, nb);
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
pub(crate) fn dispatch_plan_node(
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
    // Streaming-ingest producer short-circuit. A producer feeding a
    // streaming-ingest Aggregate runs inside that Aggregate's dispatch turn
    // (on the main thread, streaming into the bounded channel a scoped
    // ingest thread drains), so its own topo turn is a no-op — mirroring the
    // streaming-Output producer-stays / consumer-no-ops split, inverted. The
    // composition-body guard matches `take_streaming_sender`: a body
    // operator's index can collide with a top-level producer's, and only the
    // top-level plan installs ingest edges.
    if ctx.current_body_node_input_refs.is_none()
        && ctx.streaming_aggregate_ingest_edges.contains_key(&node_idx)
    {
        return Ok(());
    }
    // Streaming-probe producer short-circuit. A driver (probe-side)
    // producer feeding a streaming-probe Combine runs inside that Combine's
    // dispatch turn — the Combine materializes its build-side hash table on
    // the main thread, spawns the probe consumer on its own thread, then
    // redispatches this driver producer to stream into the bounded channel.
    // So the driver's own topo turn is a no-op here. Same composition-body
    // guard as the Aggregate-ingest short-circuit: only the top-level plan
    // installs probe edges.
    if ctx.current_body_node_input_refs.is_none()
        && ctx.streaming_combine_probe_edges.contains_key(&node_idx)
    {
        return Ok(());
    }
    let node = current_dag.graph[node_idx].clone();
    match node {
        PlanNode::Source { .. } => {
            crate::executor::source_dispatch::dispatch_source(ctx, current_dag, node_idx, &node)?;
        }

        PlanNode::Transform { .. } => {
            crate::executor::transform_dispatch::dispatch_transform(
                ctx,
                current_dag,
                node_idx,
                &node,
            )?;
        }

        PlanNode::Route { .. } => {
            crate::executor::route_dispatch::dispatch_route(ctx, current_dag, node_idx, &node)?;
        }

        PlanNode::Merge { .. } => {
            crate::executor::merge_dispatch::dispatch_merge(ctx, current_dag, node_idx, &node)?;
        }

        PlanNode::Sort { .. } => {
            crate::executor::sort_dispatch::dispatch_sort(ctx, current_dag, node_idx, &node)?;
        }

        PlanNode::Aggregation { .. } => {
            crate::executor::aggregate_dispatch::dispatch_aggregation(
                ctx,
                current_dag,
                node_idx,
                &node,
            )?;
        }

        PlanNode::Output { .. } => {
            crate::executor::output_dispatch::dispatch_output(ctx, current_dag, node_idx, &node)?;
        }

        PlanNode::Composition { .. } => {
            crate::executor::composition_dispatch::dispatch_composition(
                ctx,
                current_dag,
                node_idx,
                &node,
            )?;
        }

        PlanNode::Combine { .. } => {
            crate::executor::combine_dispatch::dispatch_combine(ctx, current_dag, node_idx, &node)?;
        }

        PlanNode::CorrelationCommit { .. } => {
            crate::executor::correlation_dispatch::dispatch_correlation_commit(ctx, current_dag)?;
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
    pub(crate) category: clinker_core_types::dlq::DlqErrorCategory,
    pub(crate) error_message: String,
    pub(crate) stage: Option<String>,
    pub(crate) route: Option<String>,
}
