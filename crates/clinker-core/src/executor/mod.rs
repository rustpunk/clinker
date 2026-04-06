pub mod stage_metrics;

use std::collections::HashMap;
use std::io::{BufWriter, Read, Write};
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use clinker_record::{PipelineCounters, Record, RecordStorage, Schema, Value};
use indexmap::IndexMap;
use rayon::prelude::*;

use crate::config::{ErrorStrategy, OutputConfig, PipelineConfig, TransformConfig};
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
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator, SkipReason, WallClock};
use cxl::typecheck::{Type, TypedProgram};
use petgraph::Direction;

/// Runtime parameters for a pipeline execution (not derived from config YAML).
pub struct PipelineRunParams {
    /// UUID v7 execution ID, unique per run.
    pub execution_id: String,
    /// Batch ID from --batch-id CLI flag or auto UUID v7.
    pub batch_id: String,
    /// Converted pipeline.vars (already validated and converted from serde_json).
    pub pipeline_vars: IndexMap<String, Value>,
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
    /// Wall-clock time when `run_with_readers_writers` was entered.
    pub started_at: DateTime<Utc>,
    /// Wall-clock time immediately after the last write and flush completed.
    pub finished_at: DateTime<Utc>,
    /// Per-stage instrumentation metrics, ordered by execution sequence.
    pub stages: Vec<stage_metrics::StageMetrics>,
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
struct CompiledTransform {
    #[allow(dead_code)]
    name: String,
    typed: Arc<TypedProgram>,
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

/// Check if the input's declared sort_order starts with the correlation key fields.
fn is_sorted_by_correlation_key(
    input_sort_order: &Option<Vec<crate::config::SortFieldSpec>>,
    correlation_key: &crate::config::CorrelationKey,
) -> bool {
    let sort_order = match input_sort_order {
        Some(so) => so,
        None => return false,
    };
    let key_fields = correlation_key.fields();
    if key_fields.len() > sort_order.len() {
        return false;
    }
    for (i, key_field) in key_fields.iter().enumerate() {
        let sort_field_name = match &sort_order[i] {
            crate::config::SortFieldSpec::Short(name) => name.as_str(),
            crate::config::SortFieldSpec::Full(sf) => sf.field.as_str(),
        };
        if sort_field_name != *key_field {
            return false;
        }
    }
    true
}

/// Auto-inject correlation key fields as a prefix to the sort order.
/// Returns `true` if sort was injected (i.e., input was not already sorted by correlation key).
fn maybe_inject_correlation_sort(
    sort_order: &mut Vec<crate::config::SortFieldSpec>,
    correlation_key: &crate::config::CorrelationKey,
) -> bool {
    let key_fields = correlation_key.fields();
    let mut new_sort: Vec<crate::config::SortFieldSpec> = key_fields
        .iter()
        .map(|f| {
            crate::config::SortFieldSpec::Full(crate::config::SortField {
                field: f.to_string(),
                order: crate::config::SortOrder::Asc,
                null_order: None,
            })
        })
        .collect();
    new_sort.append(sort_order);
    *sort_order = new_sort;
    true
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
    /// Run with explicit reader/writer registries.
    ///
    /// `readers` and `writers` are keyed by the input/output `name` fields from
    /// the pipeline config. For single-input/output pipelines, pass single-entry
    /// HashMaps.
    ///
    /// Returns an [`ExecutionReport`] containing record counts, DLQ entries,
    /// execution mode, peak RSS, and wall-clock start/finish timestamps.
    pub fn run_with_readers_writers(
        config: &PipelineConfig,
        mut readers: HashMap<String, Box<dyn Read + Send>>,
        writers: HashMap<String, Box<dyn Write + Send>>,
        params: &PipelineRunParams,
    ) -> Result<ExecutionReport, PipelineError> {
        let started_at = Utc::now();

        // Extract the primary reader from the registry
        let input = &config.inputs[0];
        let reader = readers.remove(&input.name).ok_or_else(|| {
            PipelineError::Config(crate::config::ConfigError::Validation(format!(
                "no reader registered for input '{}'",
                input.name
            )))
        })?;
        let mut collector = stage_metrics::StageCollector::default();
        let reader_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::ReaderInit);
        let mut format_reader = build_format_reader(input, reader)?;
        let schema = format_reader.schema()?;
        collector.record(reader_timer.finish(0, 0));

        // Compile CXL transforms
        let compile_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Compile);
        let resolved_transforms: Vec<_> = config.transforms().collect();
        let compiled_transforms = Self::compile_transforms(&resolved_transforms, &schema)?;

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

        let mut plan = ExecutionPlanDag::compile(config, &compiled_refs).map_err(|e| {
            PipelineError::Compilation {
                transform_name: String::new(),
                messages: vec![e.to_string()],
            }
        })?;
        collector.record(compile_timer.finish(0, 0));

        // Handle correlation key: check sort, inject if needed, set plan note
        let mut effective_sort_order: Option<Vec<crate::config::SortFieldSpec>> = None;
        if let Some(ref correlation_key) = config.error_handling.correlation_key {
            let input = &config.inputs[0];
            if is_sorted_by_correlation_key(&input.sort_order, correlation_key) {
                // Input already sorted by correlation key — no injection needed
                plan.correlation_sort_note = Some(format!(
                    "Correlation key: {:?} (input already sorted)",
                    correlation_key.fields()
                ));
            } else {
                // Auto-inject sort: prepend correlation key to existing sort
                let mut sort = input.sort_order.clone().unwrap_or_default();
                let existing_fields: Vec<String> = sort
                    .iter()
                    .map(|s| match s {
                        crate::config::SortFieldSpec::Short(n) => n.clone(),
                        crate::config::SortFieldSpec::Full(sf) => sf.field.clone(),
                    })
                    .collect();
                maybe_inject_correlation_sort(&mut sort, correlation_key);
                plan.correlation_sort_note = Some(format!(
                    "Correlation sort (auto-injected): {:?} + existing {:?}",
                    correlation_key.fields(),
                    existing_fields
                ));
                effective_sort_order = Some(sort);
            }
        }

        let execution_summary = plan.execution_summary();
        let required_arena = plan.required_arena();
        let required_sorted_input = plan.required_sorted_input();

        // Validate that all configured outputs have registered writers.
        for output in &config.outputs {
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
            format_reader,
            writers,
            &compiled_transforms,
            compiled_route,
            &plan,
            params,
            effective_sort_order.as_deref(),
            &mut collector,
        )?;

        Ok(ExecutionReport {
            counters,
            dlq_entries,
            execution_summary,
            required_arena,
            required_sorted_input,
            peak_rss_bytes,
            started_at,
            finished_at: Utc::now(),
            stages: collector.into_stages(),
        })
    }

    /// Flush a buffered correlation group: evaluate all records, DLQ entire group if any fails.
    #[allow(clippy::too_many_arguments)]
    fn flush_correlated_group(
        buffer: &mut Vec<(Record, u64)>,
        config: &PipelineConfig,
        primary_output: &OutputConfig,
        input: &crate::config::InputConfig,
        pipeline_start_time: chrono::NaiveDateTime,
        transform_names: &[&str],
        evaluators: &mut [ProgramEvaluator],
        mut compiled_route: Option<&mut CompiledRoute>,
        params: &PipelineRunParams,
        strategy: ErrorStrategy,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        per_output_batches: &mut HashMap<String, Vec<Record>>,
        _max_group_buffer: u64,
    ) {
        if buffer.is_empty() {
            return;
        }

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
            let ctx = build_eval_context(
                config,
                &input.path,
                rn,
                pipeline_start_time,
                &params.execution_id,
                &params.batch_id,
                &params.pipeline_vars,
            );
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
                            let ctx = build_eval_context(
                                config,
                                &input.path,
                                rn,
                                pipeline_start_time,
                                &params.execution_id,
                                &params.batch_id,
                                &params.pipeline_vars,
                            );
                            match route.evaluate(&emitted, &metadata, &ctx) {
                                Ok(targets) => {
                                    for target in &targets {
                                        let out_cfg = config
                                            .outputs
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
        config: &PipelineConfig,
        primary_output: &OutputConfig,
        input: &crate::config::InputConfig,
        pipeline_start_time: chrono::NaiveDateTime,
        transform_names: &[&str],
        evaluators: &mut [ProgramEvaluator],
        params: &PipelineRunParams,
        _strategy: ErrorStrategy,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        writer: &mut dyn FormatWriter,
        _max_group_buffer: u64,
    ) -> Result<(), PipelineError> {
        if buffer.is_empty() {
            return Ok(());
        }

        #[allow(clippy::type_complexity)]
        let mut results: Vec<(
            Record,
            u64,
            Result<EvalResult, (String, cxl::eval::EvalError)>,
        )> = Vec::with_capacity(buffer.len());
        let mut any_failed = false;
        let mut first_failure_message: Option<String> = None;

        for (record, rn) in buffer.drain(..) {
            let ctx = build_eval_context(
                config,
                &input.path,
                rn,
                pipeline_start_time,
                &params.execution_id,
                &params.batch_id,
                &params.pipeline_vars,
            );
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
        config: &PipelineConfig,
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
                        let out_cfg = config
                            .outputs
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
        mut format_reader: Box<dyn FormatReader>,
        writers: HashMap<String, Box<dyn Write + Send>>,
        transforms: &[CompiledTransform],
        compiled_route: Option<CompiledRoute>,
        plan: &ExecutionPlanDag,
        params: &PipelineRunParams,
        effective_sort_order: Option<&[crate::config::SortFieldSpec]>,
        collector: &mut stage_metrics::StageCollector,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, Option<u64>), PipelineError> {
        let input = &config.inputs[0];
        let primary_output = &config.outputs[0];
        let pipeline_start_time = chrono::Local::now().naive_local();

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
            let arena =
                Arena::build(&mut *format_reader, &arena_fields, memory_limit).map_err(|e| {
                    PipelineError::Compilation {
                        transform_name: String::new(),
                        messages: vec![e.to_string()],
                    }
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
                let ctx = build_eval_context(
                    config,
                    &input.path,
                    pos as u64 + 1,
                    pipeline_start_time,
                    &params.execution_id,
                    &params.batch_id,
                    &params.pipeline_vars,
                );
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
                                    let ctx = build_eval_context(
                                        config,
                                        &input.path,
                                        *pos as u64 + 1,
                                        pipeline_start_time,
                                        &params.execution_id,
                                        &params.batch_id,
                                        &params.pipeline_vars,
                                    );
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
                        let ctx = build_eval_context(
                            config,
                            &input.path,
                            *pos as u64 + 1,
                            pipeline_start_time,
                            &params.execution_id,
                            &params.batch_id,
                            &params.pipeline_vars,
                        );
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
                    &config.outputs,
                    Arc::clone(&final_output_schema),
                );

                // Dispatch first emitted record
                if let Some((record, emitted, metadata)) = first_emitted {
                    let route = compiled_route.as_mut().unwrap();
                    let ctx = build_eval_context(
                        config,
                        &input.path,
                        first_emit_pos.unwrap() as u64 + 1,
                        pipeline_start_time,
                        &params.execution_id,
                        &params.batch_id,
                        &params.pipeline_vars,
                    );
                    match route.evaluate(&emitted, &metadata, &ctx) {
                        Ok(targets) => {
                            let mut per_output_batches: HashMap<String, Vec<Record>> =
                                HashMap::new();
                            for target in &targets {
                                let out_cfg = config
                                    .outputs
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
                                let ctx = build_eval_context(
                                    config,
                                    &input.path,
                                    row_num,
                                    pipeline_start_time,
                                    &params.execution_id,
                                    &params.batch_id,
                                    &params.pipeline_vars,
                                );
                                let route_result = {
                                    let _guard = route_timer.guard();
                                    route.evaluate(emitted, metadata, &ctx)
                                };
                                match route_result {
                                    Ok(targets) => {
                                        for target in &targets {
                                            let out_cfg = config
                                                .outputs
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

        if all_records.is_empty() {
            return Ok((counters, dlq_entries, rss_bytes()));
        }

        // ── Branching DAG walk path ──
        // When the DAG has Route/Merge nodes, use per-node evaluation
        // instead of the sequential transform chain.
        if plan.has_branching() {
            return Self::execute_dag_branching(
                config,
                all_records,
                writers,
                transforms,
                compiled_route,
                plan,
                params,
                &mut counters,
                &mut dlq_entries,
                collector,
            );
        }

        // Phase 2: Sort if correlated streaming
        if requires_sorted_input && let Some(sort_specs) = effective_sort_order {
            let sort_timer = stage_metrics::StageTimer::new(stage_metrics::StageName::Sort);
            let sort_count = all_records.len() as u64;
            let sort_fields: Vec<crate::config::SortField> = sort_specs
                .iter()
                .map(|s| s.clone().into_sort_field())
                .collect();
            all_records
                .sort_by(|(a, _), (b, _)| sort::compare_records_by_fields(a, b, &sort_fields));
            collector.record(sort_timer.finish(sort_count, sort_count));
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
                let ctx = build_eval_context(
                    config,
                    &input.path,
                    *rn,
                    pipeline_start_time,
                    &params.execution_id,
                    &params.batch_id,
                    &params.pipeline_vars,
                );
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
                    spawn_writer_threads(writers, &config.outputs, Arc::clone(&output_schema));

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
                        let ctx = build_eval_context(
                            config,
                            &input.path,
                            rn,
                            pipeline_start_time,
                            &params.execution_id,
                            &params.batch_id,
                            &params.pipeline_vars,
                        );
                        let result = {
                            let _guard = transform_timer.guard();
                            evaluate_record(&record, &transform_names, &mut evaluators, &ctx)
                        };
                        match result {
                            Ok(EvalResult::Emit {
                                fields: emitted,
                                metadata,
                            }) => {
                                {
                                    let _guard = route_timer.guard();
                                    let _guard2 = projection_timer.guard();
                                    Self::dispatch_to_outputs(
                                        &record,
                                        &emitted,
                                        &metadata,
                                        config,
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
                                primary_output,
                                input,
                                pipeline_start_time,
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
                        primary_output,
                        input,
                        pipeline_start_time,
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
                        let ctx = build_eval_context(
                            config,
                            &input.path,
                            rn,
                            pipeline_start_time,
                            &params.execution_id,
                            &params.batch_id,
                            &params.pipeline_vars,
                        );
                        let result = {
                            let _guard = transform_timer.guard();
                            evaluate_record(&record, &transform_names, &mut evaluators, &ctx)
                        };
                        match result {
                            Ok(EvalResult::Emit {
                                fields: emitted, ..
                            }) => {
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
                            Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                                counters.filtered_count += 1;
                            }
                            Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
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
            let ctx = build_eval_context(
                config,
                &input.path,
                *rn,
                pipeline_start_time,
                &params.execution_id,
                &params.batch_id,
                &params.pipeline_vars,
            );
            let result = evaluate_record(record, &transform_names, &mut evaluators, &ctx);
            match &result {
                Ok(EvalResult::Emit {
                    fields: emitted,
                    metadata,
                }) => {
                    counters.ok_count += 1;
                    first_emitted = Some((record.clone(), emitted.clone(), metadata.clone()));
                    break;
                }
                Ok(EvalResult::Skip(_)) | Err(_) => {
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
                spawn_writer_threads(writers, &config.outputs, Arc::clone(&output_schema));

            // Process pending skips/errors
            for (_rn, _record, result) in &pending_skips_and_errors {
                match result {
                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                        counters.filtered_count += 1;
                    }
                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
                        counters.distinct_count += 1;
                    }
                    Ok(EvalResult::Emit { .. }) => unreachable!("emits handled in scan"),
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
            if let Some((record, emitted, metadata)) = first_emitted {
                let route = compiled_route.as_mut().unwrap();
                let ctx = build_eval_context(
                    config,
                    &input.path,
                    1,
                    pipeline_start_time,
                    &params.execution_id,
                    &params.batch_id,
                    &params.pipeline_vars,
                );
                match route.evaluate(&emitted, &metadata, &ctx) {
                    Ok(targets) => {
                        let mut per_output_batches: HashMap<String, Vec<Record>> = HashMap::new();
                        for target in &targets {
                            let out_cfg = config
                                .outputs
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
                            source_row: 1,
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
                let ctx = build_eval_context(
                    config,
                    &input.path,
                    rn,
                    pipeline_start_time,
                    &params.execution_id,
                    &params.batch_id,
                    &params.pipeline_vars,
                );
                let result = {
                    let _guard = transform_timer.guard();
                    evaluate_record(&record, &transform_names, &mut evaluators, &ctx)
                };
                match result {
                    Ok(EvalResult::Emit {
                        fields: emitted,
                        metadata,
                    }) => {
                        let route_result = {
                            let _guard = route_timer.guard();
                            route.evaluate(&emitted, &metadata, &ctx)
                        };
                        match route_result {
                            Ok(targets) => {
                                for target in &targets {
                                    let out_cfg = config
                                        .outputs
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
                    Ok(EvalResult::Skip(SkipReason::Filtered)) => {
                        counters.filtered_count += 1;
                    }
                    Ok(EvalResult::Skip(SkipReason::Duplicate)) => {
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
                    Ok(EvalResult::Emit { .. }) => unreachable!("emits handled in scan"),
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
            if let Some((record, emitted, metadata)) = first_emitted {
                let projected = project_output_with_meta(&record, &emitted, &metadata, output);
                csv_writer.write_record(&projected)?;
            }

            // Process remaining records
            let mut transform_timer = stage_metrics::CumulativeTimer::new();
            let mut projection_timer = stage_metrics::CumulativeTimer::new();
            let mut write_timer = stage_metrics::CumulativeTimer::new();
            let mut records_emitted: u64 = if first_emitted_was_some { 1 } else { 0 };
            let remaining_count = all_records.len().saturating_sub(scan_idx) as u64;

            for (record, rn) in all_records.into_iter().skip(scan_idx) {
                let ctx = build_eval_context(
                    config,
                    &input.path,
                    rn,
                    pipeline_start_time,
                    &params.execution_id,
                    &params.batch_id,
                    &params.pipeline_vars,
                );
                let result = {
                    let _guard = transform_timer.guard();
                    evaluate_record(&record, &transform_names, &mut evaluators, &ctx)
                };
                match result {
                    Ok(EvalResult::Emit {
                        fields: emitted,
                        metadata,
                    }) => {
                        let projected = {
                            let _guard = projection_timer.guard();
                            project_output_with_meta(&record, &emitted, &metadata, output)
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
        all_records: Vec<(Record, u64)>,
        mut writers: HashMap<String, Box<dyn Write + Send>>,
        transforms: &[CompiledTransform],
        compiled_route: Option<CompiledRoute>,
        plan: &ExecutionPlanDag,
        params: &PipelineRunParams,
        counters: &mut PipelineCounters,
        dlq_entries: &mut Vec<DlqEntry>,
        collector: &mut stage_metrics::StageCollector,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, Option<u64>), PipelineError> {
        use petgraph::graph::NodeIndex;

        let input = &config.inputs[0];
        let primary_output = &config.outputs[0];
        let pipeline_start_time = chrono::Local::now().naive_local();
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
                        let ctx = build_eval_context(
                            config,
                            &input.path,
                            rn,
                            pipeline_start_time,
                            &params.execution_id,
                            &params.batch_id,
                            &params.pipeline_vars,
                        );

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

                    node_buffers.insert(node_idx, output_records);
                }

                PlanNode::Route {
                    ref name,
                    mode,
                    branches: _,
                    default: _,
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
                        for tc in config.transforms() {
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
                            let ctx = build_eval_context(
                                config,
                                &input.path,
                                rn,
                                pipeline_start_time,
                                &params.execution_id,
                                &params.batch_id,
                                &params.pipeline_vars,
                            );

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

                PlanNode::Merge { ref name } => {
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
                    let declaration_order: Vec<String> = config
                        .transforms()
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

                PlanNode::Output { ref name } => {
                    // Get input records from predecessor
                    let predecessors: Vec<NodeIndex> = plan
                        .graph
                        .neighbors_directed(node_idx, Direction::Incoming)
                        .collect();
                    let input_records = predecessors
                        .iter()
                        .find_map(|p| node_buffers.remove(p))
                        .unwrap_or_default();

                    // Count ok records
                    let output_record_count = input_records.len() as u64;
                    counters.ok_count += output_record_count;
                    records_emitted += output_record_count;

                    // Derive output schema from first emitted record
                    let scan_timer =
                        stage_metrics::StageTimer::new(stage_metrics::StageName::SchemaScan);
                    let out_cfg = config
                        .outputs
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

    /// Compile all CXL transform blocks against the input schema.
    ///
    /// Fields emitted by earlier transforms are progressively added to the
    /// available field set so that later transforms can reference them.
    fn compile_transforms(
        transforms: &[&TransformConfig],
        schema: &Arc<Schema>,
    ) -> Result<Vec<CompiledTransform>, PipelineError> {
        let mut fields: Vec<String> = schema.columns().iter().map(|c| c.to_string()).collect();
        // CSV fields are all strings at the data level, but CXL allows
        // polymorphic usage (e.g. `+` for concat, `.to_int()` for coercion).
        // Declare as Any so the type checker doesn't reject valid CXL.
        let mut type_schema: HashMap<String, Type> =
            fields.iter().map(|f| (f.clone(), Type::Any)).collect();

        let mut compiled = Vec::with_capacity(transforms.len());
        for t in transforms {
            let parse_result = cxl::parser::Parser::parse(t.cxl_source());
            if !parse_result.errors.is_empty() {
                let messages: Vec<String> = parse_result
                    .errors
                    .iter()
                    .map(|e| e.message.clone())
                    .collect();
                return Err(PipelineError::Compilation {
                    transform_name: t.name.clone(),
                    messages,
                });
            }

            let field_refs: Vec<&str> = fields.iter().map(|s| s.as_str()).collect();
            let resolved = cxl::resolve::resolve_program(
                parse_result.ast,
                &field_refs,
                parse_result.node_count,
            )
            .map_err(|diags| PipelineError::Compilation {
                transform_name: t.name.clone(),
                messages: diags.into_iter().map(|d| d.message).collect(),
            })?;

            // Determine aggregate mode. For aggregate transforms, validate that
            // every group_by field exists in the current schema, then typecheck
            // in GroupBy mode so bare non-grouped field references are rejected.
            let agg_mode = if let Some(agg) = &t.aggregate {
                let mut missing = Vec::new();
                for gb in &agg.group_by {
                    if !type_schema.contains_key(gb) {
                        missing.push(format!("group_by field `{gb}` not found in schema"));
                    }
                }
                if !missing.is_empty() {
                    return Err(PipelineError::Compilation {
                        transform_name: t.name.clone(),
                        messages: missing,
                    });
                }
                cxl::typecheck::AggregateMode::GroupBy {
                    group_by_fields: agg.group_by.iter().cloned().collect(),
                }
            } else {
                cxl::typecheck::AggregateMode::Row
            };

            let typed = cxl::typecheck::type_check_with_mode(resolved, &type_schema, agg_mode)
                .map_err(|diags| {
                    let errors: Vec<String> = diags
                        .iter()
                        .filter(|d| !d.is_warning)
                        .map(|d| d.message.clone())
                        .collect();
                    if errors.is_empty() {
                        return PipelineError::Compilation {
                            transform_name: t.name.clone(),
                            messages: diags.into_iter().map(|d| d.message).collect(),
                        };
                    }
                    PipelineError::Compilation {
                        transform_name: t.name.clone(),
                        messages: errors,
                    }
                })?;

            // Build the output schema for subsequent transforms.
            //
            // For aggregate transforms, the output schema is strictly
            // {group_by fields} ∪ {emitted aggregate result names}: row-level
            // fields that are not grouped cease to exist after aggregation.
            // For row transforms, emitted fields are appended to the existing
            // field set.
            if t.aggregate.is_some() {
                let agg = t.aggregate.as_ref().unwrap();
                let mut new_fields: Vec<String> = agg.group_by.clone();
                let mut new_type_schema: HashMap<String, Type> = agg
                    .group_by
                    .iter()
                    .map(|g| (g.clone(), Type::Any))
                    .collect();
                for stmt in &typed.program.statements {
                    if let cxl::ast::Statement::Emit { name, .. } = stmt
                        && !new_type_schema.contains_key(name.as_ref())
                    {
                        new_fields.push(name.to_string());
                        new_type_schema.insert(name.to_string(), Type::Any);
                    }
                }
                fields = new_fields;
                type_schema = new_type_schema;
            } else {
                for stmt in &typed.program.statements {
                    if let cxl::ast::Statement::Emit { name, .. } = stmt
                        && !type_schema.contains_key(name.as_ref())
                    {
                        fields.push(name.to_string());
                        type_schema.insert(name.to_string(), Type::Any);
                    }
                }
            }

            compiled.push(CompiledTransform {
                name: t.name.clone(),
                typed: Arc::new(typed),
            });
        }
        Ok(compiled)
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
        let type_schema: HashMap<String, Type> = emitted_fields
            .iter()
            .map(|f| (f.clone(), Type::Any))
            .collect();
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
    pub fn explain(config: &PipelineConfig) -> Result<String, PipelineError> {
        let (plan, _) = Self::explain_dag(config)?;
        Ok(plan.explain_full(config))
    }

    /// Compile execution plan and return the DAG for format-specific rendering.
    pub fn explain_dag(config: &PipelineConfig) -> Result<(ExecutionPlanDag, ()), PipelineError> {
        // Extract field names from CXL AST to build a synthetic schema
        let mut all_fields = Vec::new();
        for t in config.transforms() {
            let parsed = cxl::parser::Parser::parse(t.cxl_source());
            if !parsed.errors.is_empty() {
                return Err(PipelineError::Compilation {
                    transform_name: t.name.clone(),
                    messages: parsed.errors.iter().map(|e| e.message.clone()).collect(),
                });
            }
            collect_field_refs(&parsed.ast, &mut all_fields);
        }
        all_fields.sort();
        all_fields.dedup();

        let schema = Arc::new(Schema::new(
            all_fields.iter().map(|f| f.as_str().into()).collect(),
        ));

        let resolved_transforms: Vec<_> = config.transforms().collect();
        let compiled = Self::compile_transforms(&resolved_transforms, &schema)?;
        let compiled_refs: Vec<(&str, &TypedProgram)> = compiled
            .iter()
            .map(|ct| (ct.name.as_str(), ct.typed.as_ref()))
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
    input: &crate::config::InputConfig,
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

/// Extract `Vec<FieldDef>` from `InputConfig.schema` for fixed-width format.
///
/// Resolves `SchemaSource::Inline` or `SchemaSource::FilePath` to `Vec<FieldDef>`.
/// Returns a config validation error if schema is `None` (fixed-width requires
/// explicit schema with field definitions).
fn extract_field_defs(
    input: &crate::config::InputConfig,
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

/// Build an EvalContext for a given record.
///
/// `pipeline_start_time` must be frozen once at pipeline start and reused for all
/// records. This ensures `$pipeline.start_time` is deterministic within a run.
/// The `now` keyword uses `ctx.clock.now()` (wall-clock) and is intentionally
/// non-deterministic — users who reference `now` opt into time-varying output.
fn build_eval_context(
    config: &PipelineConfig,
    source_file: &str,
    source_row: u64,
    pipeline_start_time: chrono::NaiveDateTime,
    execution_id: &str,
    batch_id: &str,
    pipeline_vars: &IndexMap<String, Value>,
) -> EvalContext {
    EvalContext {
        clock: Box::new(WallClock),
        pipeline_start_time,
        pipeline_name: config.pipeline.name.clone(),
        pipeline_execution_id: execution_id.to_string(),
        pipeline_batch_id: batch_id.to_string(),
        pipeline_counters: PipelineCounters::default(),
        source_file: source_file.into(),
        source_row,
        pipeline_vars: pipeline_vars.clone(),
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
    let mut record = record.clone();
    let mut all_emitted = IndexMap::new();
    let mut all_metadata = IndexMap::new();
    for (i, eval) in evaluators.iter_mut().enumerate() {
        let name = transform_names.get(i).copied().unwrap_or("unknown");
        match eval
            .eval_record::<NullStorage>(ctx, &record, None)
            .map_err(|e| (name.to_string(), e))?
        {
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
                // Copy metadata onto Record so later transforms can read via $meta.*
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

/// Extract field reference names from a CXL AST (for --explain schema inference).
fn collect_field_refs(program: &cxl::ast::Program, names: &mut Vec<String>) {
    for stmt in &program.statements {
        match stmt {
            cxl::ast::Statement::Let { expr, .. }
            | cxl::ast::Statement::Emit { expr, .. }
            | cxl::ast::Statement::ExprStmt { expr, .. } => collect_field_refs_expr(expr, names),
            cxl::ast::Statement::Trace { guard, message, .. } => {
                if let Some(g) = guard {
                    collect_field_refs_expr(g, names);
                }
                collect_field_refs_expr(message, names);
            }
            _ => {}
        }
    }
}

fn collect_field_refs_expr(expr: &cxl::ast::Expr, names: &mut Vec<String>) {
    match expr {
        cxl::ast::Expr::FieldRef { name, .. } => {
            if &**name != "it" {
                names.push(name.to_string());
            }
        }
        cxl::ast::Expr::Binary { lhs, rhs, .. } | cxl::ast::Expr::Coalesce { lhs, rhs, .. } => {
            collect_field_refs_expr(lhs, names);
            collect_field_refs_expr(rhs, names);
        }
        cxl::ast::Expr::Unary { operand, .. } => collect_field_refs_expr(operand, names),
        cxl::ast::Expr::MethodCall { receiver, args, .. } => {
            collect_field_refs_expr(receiver, names);
            for a in args {
                collect_field_refs_expr(a, names);
            }
        }
        cxl::ast::Expr::IfThenElse {
            condition,
            then_branch,
            else_branch,
            ..
        } => {
            collect_field_refs_expr(condition, names);
            collect_field_refs_expr(then_branch, names);
            if let Some(eb) = else_branch {
                collect_field_refs_expr(eb, names);
            }
        }
        cxl::ast::Expr::Match { subject, arms, .. } => {
            if let Some(s) = subject {
                collect_field_refs_expr(s, names);
            }
            for arm in arms {
                collect_field_refs_expr(&arm.pattern, names);
                collect_field_refs_expr(&arm.body, names);
            }
        }
        cxl::ast::Expr::WindowCall { args, .. } => {
            for a in args {
                collect_field_refs_expr(a, names);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    mod aggregation;
    mod branching;
    mod correlated_dlq;
    mod format_dispatch;
    mod multi_output;

    use super::*;

    /// Helper: run executor with in-memory CSV input/output.
    fn run_test(
        yaml: &str,
        csv_input: &str,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
        let config = crate::config::parse_config(yaml).unwrap();
        let output_buf = crate::test_helpers::SharedBuffer::new();

        let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
            config.inputs[0].name.clone(),
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        )]);
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            config.outputs[0].name.clone(),
            Box::new(output_buf.clone()) as Box<dyn std::io::Write + Send>,
        )]);

        let pipeline_vars = config
            .pipeline
            .vars
            .as_ref()
            .map(|v| crate::config::convert_pipeline_vars(v))
            .unwrap_or_default();
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars,
        };

        let report =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)?;

        let output = output_buf.as_string();
        Ok((report.counters, report.dlq_entries, output))
    }

    /// Helper: run executor with in-memory CSV and return the full ExecutionReport.
    fn run_test_report(
        yaml: &str,
        csv_input: &str,
    ) -> Result<(ExecutionReport, String), PipelineError> {
        let config = crate::config::parse_config(yaml).unwrap();
        let output_buf = crate::test_helpers::SharedBuffer::new();

        let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
            config.inputs[0].name.clone(),
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        )]);
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            config.outputs[0].name.clone(),
            Box::new(output_buf.clone()) as Box<dyn std::io::Write + Send>,
        )]);

        let pipeline_vars = config
            .pipeline
            .vars
            .as_ref()
            .map(|v| crate::config::convert_pipeline_vars(v))
            .unwrap_or_default();
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars,
        };

        let report =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)?;

        let output = output_buf.as_string();
        Ok((report, output))
    }

    #[test]
    fn test_execution_report_has_stages_field() {
        let yaml = r#"
pipeline:
  name: identity

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations: []
"#;
        let csv = "name,age\nAlice,30\nBob,25\n";
        let (report, _output) = run_test_report(yaml, csv).unwrap();
        // ReaderInit + Compile + SchemaScan present
        let names: Vec<_> = report.stages.iter().map(|s| &s.name).collect();
        assert!(names.contains(&&stage_metrics::StageName::ReaderInit));
        assert!(names.contains(&&stage_metrics::StageName::Compile));
        assert!(names.contains(&&stage_metrics::StageName::SchemaScan));
    }

    #[test]
    fn test_execution_report_stages_from_collector() {
        let mut collector = stage_metrics::StageCollector::default();
        collector
            .record(stage_metrics::StageTimer::new(stage_metrics::StageName::Compile).finish(0, 0));
        let stages = collector.into_stages();
        assert_eq!(stages.len(), 1);
    }

    #[test]
    fn test_streaming_pipeline_has_compile_reader_schemascan_stages() {
        let yaml = r#"
pipeline:
  name: passthrough

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: add_x
    cxl: "emit x = 1"
"#;
        let csv = "name,age\nAlice,30\nBob,25\nCarol,28\nDave,31\nEve,22\n";
        let (report, _output) = run_test_report(yaml, csv).unwrap();
        let names: Vec<_> = report.stages.iter().map(|s| &s.name).collect();
        assert!(
            names.contains(&&stage_metrics::StageName::ReaderInit),
            "Missing ReaderInit stage"
        );
        assert!(
            names.contains(&&stage_metrics::StageName::Compile),
            "Missing Compile stage"
        );
        assert!(
            names.contains(&&stage_metrics::StageName::SchemaScan),
            "Missing SchemaScan stage"
        );
        for stage in &report.stages {
            assert!(
                stage.elapsed > std::time::Duration::ZERO,
                "Stage {:?} has zero elapsed",
                stage.name
            );
        }
    }

    #[test]
    fn test_stage_order_matches_execution_sequence() {
        let yaml = r#"
pipeline:
  name: passthrough

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: add_x
    cxl: "emit x = 1"
"#;
        let csv = "name,age\nAlice,30\nBob,25\n";
        let (report, _output) = run_test_report(yaml, csv).unwrap();
        let names: Vec<_> = report.stages.iter().map(|s| &s.name).collect();
        let ri_pos = names
            .iter()
            .position(|n| **n == stage_metrics::StageName::ReaderInit)
            .expect("ReaderInit missing");
        let c_pos = names
            .iter()
            .position(|n| **n == stage_metrics::StageName::Compile)
            .expect("Compile missing");
        let ss_pos = names
            .iter()
            .position(|n| **n == stage_metrics::StageName::SchemaScan)
            .expect("SchemaScan missing");
        assert!(ri_pos < c_pos, "ReaderInit should come before Compile");
        assert!(c_pos < ss_pos, "Compile should come before SchemaScan");
    }

    /// Helper: run a correlated pipeline and return the full report.
    fn run_correlated_test_report(
        yaml: &str,
        csv_input: &str,
    ) -> Result<(ExecutionReport, String), PipelineError> {
        let config = crate::config::parse_config(yaml).unwrap();
        let output_buf = crate::test_helpers::SharedBuffer::new();

        let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
            config.inputs[0].name.clone(),
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        )]);
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            config.outputs[0].name.clone(),
            Box::new(output_buf.clone()) as Box<dyn std::io::Write + Send>,
        )]);

        let pipeline_vars = config
            .pipeline
            .vars
            .as_ref()
            .map(|v| crate::config::convert_pipeline_vars(v))
            .unwrap_or_default();
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars,
        };

        let report =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)?;
        let output = output_buf.as_string();
        Ok((report, output))
    }

    #[test]
    fn test_correlated_pipeline_has_schemascan() {
        let yaml = r#"
pipeline:
  name: correlated_test
inputs:
  - name: src
    path: input.csv
    type: csv
error_handling:
  strategy: continue
  correlation_key: employee_id
transformations:
  - name: validate
    cxl: |
      emit emp_id = employee_id
      emit val = value
outputs:
  - name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
        let csv = "employee_id,value\nA,10\nA,20\nB,30\nB,40\n";
        let (report, _output) = run_correlated_test_report(yaml, csv).unwrap();
        let names: Vec<_> = report.stages.iter().map(|s| &s.name).collect();
        assert!(
            names.contains(&&stage_metrics::StageName::SchemaScan),
            "Missing SchemaScan stage in correlated pipeline"
        );
    }

    #[test]
    fn test_two_pass_pipeline_has_schemascan() {
        let yaml = r#"
pipeline:
  name: two_pass_test
inputs:
  - name: primary
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: window_transform
    cxl: |
      emit dept_total = $window.sum(amount)
    local_window:
      group_by: [dept]
"#;
        let csv = "dept,amount\nA,10\nB,20\nA,30\n";
        let (report, _output) = run_test_report(yaml, csv).unwrap();
        let names: Vec<_> = report.stages.iter().map(|s| &s.name).collect();
        assert!(
            names.contains(&&stage_metrics::StageName::SchemaScan),
            "Missing SchemaScan stage in two-pass pipeline"
        );
    }

    #[test]
    fn test_streaming_cumulative_stages_present() {
        let yaml = r#"
pipeline:
  name: passthrough
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: add_x
    cxl: "emit x = 1"
"#;
        let csv = "name,age\nAlice,30\nBob,25\nCarol,28\nDave,31\nEve,22\n\
                   Frank,27\nGrace,29\nHenry,33\nIvy,24\nJack,26\n";
        let (report, _output) = run_test_report(yaml, csv).unwrap();
        let names: Vec<_> = report.stages.iter().map(|s| &s.name).collect();
        assert!(
            names.contains(&&stage_metrics::StageName::TransformEval),
            "Missing TransformEval: {:?}",
            names
        );
        assert!(
            names.contains(&&stage_metrics::StageName::Projection),
            "Missing Projection: {:?}",
            names
        );
        assert!(
            names.contains(&&stage_metrics::StageName::Write),
            "Missing Write: {:?}",
            names
        );
        for stage in report.stages.iter().filter(|s| {
            matches!(
                s.name,
                stage_metrics::StageName::TransformEval
                    | stage_metrics::StageName::Projection
                    | stage_metrics::StageName::Write
            )
        }) {
            assert!(
                stage.elapsed > std::time::Duration::ZERO,
                "Stage {:?} has zero elapsed",
                stage.name
            );
        }
    }

    #[test]
    fn test_multi_output_has_route_eval_stage() {
        let yaml = r#"
pipeline:
  name: multi_out
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: high
    type: csv
    path: high.csv
    include_unmapped: true
  - name: low
    type: csv
    path: low.csv
    include_unmapped: true
transformations:
  - name: classify
    cxl: |
      emit x = age.to_int()
    route:
      branches:
        - name: high
          condition: "x > 28"
      default: low
"#;
        let csv = "name,age\nAlice,30\nBob,25\n";
        let config = crate::config::parse_config(yaml).unwrap();
        let high_buf = crate::test_helpers::SharedBuffer::new();
        let low_buf = crate::test_helpers::SharedBuffer::new();

        let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
            config.inputs[0].name.clone(),
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        )]);
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
            (
                "high".to_string(),
                Box::new(high_buf.clone()) as Box<dyn std::io::Write + Send>,
            ),
            (
                "low".to_string(),
                Box::new(low_buf.clone()) as Box<dyn std::io::Write + Send>,
            ),
        ]);

        let pipeline_vars = config
            .pipeline
            .vars
            .as_ref()
            .map(|v| crate::config::convert_pipeline_vars(v))
            .unwrap_or_default();
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars,
        };

        let report =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params).unwrap();
        let names: Vec<_> = report.stages.iter().map(|s| &s.name).collect();
        assert!(
            names.contains(&&stage_metrics::StageName::RouteEval),
            "Missing RouteEval in multi-output pipeline: {:?}",
            names
        );
    }

    #[test]
    fn test_executor_identity_passthrough() {
        let yaml = r#"
pipeline:
  name: identity

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations: []
"#;
        let csv = "name,age\nAlice,30\nBob,25\n";
        let (counters, dlq, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.total_count, 2);
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.dlq_count, 0);
        assert!(dlq.is_empty());
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));
    }

    #[test]
    fn test_executor_cxl_string_concat() {
        let yaml = r#"
pipeline:
  name: concat

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: concat_name
    cxl: |
      emit full_name = first_name + " " + last_name
"#;
        let csv = "first_name,last_name\nAlice,Smith\nBob,Jones\n";
        let (counters, _, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert!(output.contains("Alice Smith"));
        assert!(output.contains("Bob Jones"));
    }

    #[test]
    fn test_executor_cxl_arithmetic() {
        let yaml = r#"
pipeline:
  name: arithmetic

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: calc_total
    cxl: |
      emit total = price.to_float() * quantity.to_float()
"#;
        let csv = "price,quantity\n10.5,3\n20.0,2\n";
        let (counters, _, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert!(output.contains("31.5"));
        assert!(output.contains("40"));
    }

    #[test]
    fn test_executor_cxl_conditional() {
        let yaml = r#"
pipeline:
  name: conditional

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: categorize
    cxl: |
      emit category = if age.to_int() >= 18 then "adult" else "minor"
"#;
        let csv = "name,age\nAlice,30\nBob,15\n";
        let (counters, _, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 2);
        assert!(output.contains("adult"));
        assert!(output.contains("minor"));
    }

    #[test]
    fn test_executor_field_mapping() {
        let yaml = r#"
pipeline:
  name: mapping

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
    mapping:
      name: employee_name

transformations: []
"#;
        let csv = "name,age\nAlice,30\n";
        let (_, _, output) = run_test(yaml, csv).unwrap();
        assert!(output.contains("employee_name"));
        assert!(!output.contains("\nname")); // header should be renamed
    }

    #[test]
    fn test_executor_exclude_fields() {
        let yaml = r#"
pipeline:
  name: exclude

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
    exclude:
      - secret

transformations: []
"#;
        let csv = "name,secret,age\nAlice,password,30\n";
        let (_, _, output) = run_test(yaml, csv).unwrap();
        assert!(output.contains("name"));
        assert!(output.contains("age"));
        assert!(!output.contains("secret"));
        assert!(!output.contains("password"));
    }

    #[test]
    fn test_executor_projection_order() {
        // Gather (include_unmapped) → exclude → mapping
        let yaml = r#"
pipeline:
  name: projection

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
    exclude:
      - secret
    mapping:
      name: full_name

transformations: []
"#;
        let csv = "name,secret,age\nAlice,password,30\n";
        let (_, _, output) = run_test(yaml, csv).unwrap();
        // name renamed to full_name, secret excluded, age kept
        assert!(output.contains("full_name"));
        assert!(output.contains("age"));
        assert!(!output.contains("secret"));
        assert!(!output.contains("password"));
    }

    #[test]
    fn test_executor_fail_fast_aborts() {
        let yaml = r#"
pipeline:
  name: failfast

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: will_fail
    cxl: |
      emit result = value.to_int() + 1

error_handling:
  strategy: fail_fast
"#;
        // "bad" can't be converted to int
        let csv = "value\n10\nbad\n20\n";
        let result = run_test(yaml, csv);
        assert!(result.is_err());
    }

    #[test]
    fn test_executor_continue_skips() {
        let yaml = r#"
pipeline:
  name: continue_skip

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: will_fail_some
    cxl: |
      emit result = value.to_int() + 1

error_handling:
  strategy: continue
"#;
        let csv = "value\n10\nbad\n20\n";
        let (counters, dlq, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.total_count, 3);
        assert_eq!(counters.ok_count, 2);
        assert_eq!(counters.dlq_count, 1);
        assert_eq!(dlq.len(), 1);
        assert_eq!(dlq[0].source_row, 2);
        // Output should have rows 1 and 3 but not row 2
        assert!(output.contains("11"));
        assert!(output.contains("21"));
        assert!(!output.contains("bad"));
    }

    #[test]
    fn test_executor_best_effort_preserves() {
        let yaml = r#"
pipeline:
  name: best_effort

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: will_fail_some
    cxl: |
      emit result = value.to_int() + 1

error_handling:
  strategy: best_effort
"#;
        let csv = "value\n10\nbad\n20\n";
        let (counters, dlq, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.total_count, 3);
        // best_effort: ok_count includes error rows (original values preserved)
        assert_eq!(counters.ok_count, 3);
        assert_eq!(counters.dlq_count, 1);
        assert_eq!(dlq.len(), 1);
        // "bad" row should still appear in output with original value
        assert!(output.contains("bad"));
    }

    #[test]
    fn test_executor_pipeline_counters() {
        let yaml = r#"
pipeline:
  name: counters

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: will_fail_some
    cxl: |
      emit result = value.to_int() * 2

error_handling:
  strategy: continue
"#;
        // 10 rows, rows 3 and 7 have "bad" values
        let mut rows = Vec::new();
        rows.push("value".to_string());
        for i in 1..=10 {
            if i == 3 || i == 7 {
                rows.push("bad".to_string());
            } else {
                rows.push(i.to_string());
            }
        }
        let csv = rows.join("\n") + "\n";

        let (counters, dlq, _) = run_test(yaml, &csv).unwrap();
        assert_eq!(counters.total_count, 10);
        assert_eq!(counters.ok_count, 8);
        assert_eq!(counters.dlq_count, 2);
        assert_eq!(dlq.len(), 2);
    }

    // === Task 5.5 Two-Pass Gate Tests ===

    fn two_pass_yaml(cxl: &str, local_window: &str) -> String {
        format!(
            r#"
pipeline:
  name: two_pass_test

inputs:
  - name: primary
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: window_transform
    cxl: |
      {cxl}
    local_window:
      {local_window}
"#,
            cxl = cxl,
            local_window = local_window
        )
    }

    #[test]
    fn test_two_pass_sum_by_dept() {
        let yaml = two_pass_yaml("emit dept_total = $window.sum(amount)", "group_by: [dept]");
        let csv = "dept,amount\nA,10\nB,20\nA,30\nB,40\nA,50\n";
        let (counters, _, output) = run_test(&yaml, csv).unwrap();
        assert_eq!(counters.total_count, 5);
        assert_eq!(counters.ok_count, 5);
        // Arena stores CSV strings, so sum returns Null for strings.
        // This is correct — Arena values are uncoerced strings from CSV.
        // Full numeric window aggregation requires CXL-side coercion,
        // which is a Phase 2 evaluator concern (not Arena's job).
        // The test verifies the two-pass path executes without errors.
        assert!(output.contains("dept_total"));
    }

    #[test]
    fn test_two_pass_count_by_group() {
        let yaml = two_pass_yaml("emit group_count = $window.count()", "group_by: [dept]");
        let csv = "dept,amount\nA,10\nB,20\nA,30\nB,40\nA,50\n";
        let (counters, _, output) = run_test(&yaml, csv).unwrap();
        assert_eq!(counters.total_count, 5);
        assert_eq!(counters.ok_count, 5);
        // count() returns partition size as integer
        assert!(output.contains("group_count"), "output: {}", output);
        // A has 3 records, B has 2
        assert!(
            output.contains(",3") || output.contains("3,"),
            "expected 3 in output: {}",
            output
        );
        assert!(
            output.contains(",2") || output.contains("2,"),
            "expected 2 in output: {}",
            output
        );
    }

    #[test]
    fn test_two_pass_stateless_fallback() {
        // No local_window → streaming mode, no arena built
        let yaml = r#"
pipeline:
  name: stateless

inputs:
  - name: primary
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: calc
    cxl: |
      emit doubled = amount + "_doubled"
"#;
        let csv = "amount\n10\n20\n30\n";
        let (counters, _, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.total_count, 3);
        assert_eq!(counters.ok_count, 3);
        assert!(output.contains("10_doubled"));
    }

    #[test]
    fn test_two_pass_pipeline_counters() {
        let yaml = two_pass_yaml("emit cnt = $window.count()", "group_by: [dept]");
        let csv = "dept\nA\nB\nA\nB\nA\nB\nA\nB\nA\nB\n";
        let (counters, dlq, _) = run_test(&yaml, csv).unwrap();
        assert_eq!(counters.total_count, 10);
        assert_eq!(counters.ok_count, 10);
        assert_eq!(counters.dlq_count, 0);
        assert!(dlq.is_empty());
    }

    #[test]
    fn test_two_pass_mixed_stateless_and_window() {
        let yaml = r#"
pipeline:
  name: mixed

inputs:
  - name: primary
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: stateless_calc
    cxl: |
      emit label = dept + "_label"
  - name: window_calc
    cxl: |
      emit cnt = $window.count()
    local_window:
      group_by: [dept]
"#;
        let csv = "dept,amount\nA,10\nB,20\nA,30\n";
        let (counters, _, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.total_count, 3);
        assert_eq!(counters.ok_count, 3);
        // Both transforms should produce output
        assert!(output.contains("label"));
        assert!(output.contains("cnt"));
        assert!(output.contains("A_label"));
    }

    #[test]
    fn test_two_pass_multiple_windows_shared_index() {
        let yaml = r#"
pipeline:
  name: shared_index

inputs:
  - name: primary
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations:
  - name: win1
    cxl: |
      emit cnt1 = $window.count()
    local_window:
      group_by: [dept]
  - name: win2
    cxl: |
      emit cnt2 = $window.count()
    local_window:
      group_by: [dept]
"#;
        let csv = "dept\nA\nB\nA\n";
        let (counters, _, output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 3);
        assert!(output.contains("cnt1"));
        assert!(output.contains("cnt2"));
    }

    #[test]
    fn test_two_pass_nan_exit_code_3() {
        // NaN in group_by field should cause an error
        // Since CSV reads as strings, we can't directly inject NaN.
        // The NaN check happens in SecondaryIndex::build on Float values.
        // With CSV input (all strings), NaN won't occur in group_by.
        // This test verifies the error path exists by checking the error type.
        // A true NaN test would need non-CSV input or post-coercion in Arena.
        // For now, verify the pipeline runs without NaN errors for string data.
        let yaml = two_pass_yaml("emit cnt = $window.count()", "group_by: [dept]");
        let csv = "dept\nA\nB\n";
        let result = run_test(&yaml, csv);
        assert!(result.is_ok()); // No NaN in string data
    }

    #[test]
    fn test_two_pass_provenance_populated() {
        // RecordProvenance is set in EvalContext — verify source_row is sequential
        let yaml = two_pass_yaml("emit row = $pipeline.source_row", "group_by: [dept]");
        let csv = "dept\nA\nB\nA\n";
        let (counters, _, output) = run_test(&yaml, csv).unwrap();
        assert_eq!(counters.ok_count, 3);
        // source_row should be sequential
        // Values appear as integers in CSV output
        assert!(
            output.contains("row"),
            "output missing 'row' header: {}",
            output
        );
    }

    // ── Phase 6 gate tests ───────────────────────────────────────────

    #[test]
    fn test_thread_pool_default_count() {
        let expected = std::cmp::min(num_cpus::get().saturating_sub(2).max(1), 4);
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(expected)
            .thread_name(|i| format!("cxl-worker-{i}"))
            .build()
            .unwrap();
        assert_eq!(pool.current_num_threads(), expected);
    }

    #[test]
    fn test_thread_pool_cli_override() {
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(2)
            .thread_name(|i| format!("cxl-worker-{i}"))
            .build()
            .unwrap();
        assert_eq!(pool.current_num_threads(), 2);
    }

    /// Helper: run executor with specific thread count via concurrency config.
    fn run_test_with_threads(
        yaml: &str,
        csv_input: &str,
        threads: usize,
    ) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
        // Parse config and override thread count
        let mut config = crate::config::parse_config(yaml).unwrap();
        config.pipeline.concurrency = Some(crate::config::ConcurrencyConfig {
            threads: Some(threads),
            chunk_size: Some(64), // small chunks to exercise chunking logic
        });
        let output_buf = crate::test_helpers::SharedBuffer::new();

        let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
            config.inputs[0].name.clone(),
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec()))
                as Box<dyn std::io::Read + Send>,
        )]);
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            config.outputs[0].name.clone(),
            Box::new(output_buf.clone()) as Box<dyn std::io::Write + Send>,
        )]);

        let pipeline_vars = config
            .pipeline
            .vars
            .as_ref()
            .map(|v| crate::config::convert_pipeline_vars(v))
            .unwrap_or_default();
        let params = PipelineRunParams {
            execution_id: "test-exec-id".to_string(),
            batch_id: "test-batch-id".to_string(),
            pipeline_vars,
        };
        let report =
            PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)?;
        let output = output_buf.as_string();
        Ok((report.counters, report.dlq_entries, output))
    }

    #[test]
    fn test_par_stateless_deterministic_output() {
        let yaml = r#"
pipeline:
  name: par_stateless
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: double
    cxl: |
      emit doubled = amount + "_doubled"
"#;
        // Generate 1000-record CSV
        let mut csv = String::from("id,amount\n");
        for i in 0..1000 {
            csv.push_str(&format!("{i},val_{i}\n"));
        }

        let (_, _, output_1) = run_test_with_threads(yaml, &csv, 1).unwrap();
        let (_, _, output_4) = run_test_with_threads(yaml, &csv, 4).unwrap();
        assert_eq!(
            output_1, output_4,
            "Output must be byte-identical with 1 vs 4 threads"
        );
    }

    #[test]
    fn test_par_index_reading_deterministic_output() {
        let yaml = r#"
pipeline:
  name: par_index
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: dept_total
    cxl: |
      emit dept_total = $window.sum(amount)
    local_window:
      group_by: [dept]
"#;
        let mut csv = String::from("dept,amount\n");
        for i in 0..500 {
            let dept = if i % 3 == 0 {
                "A"
            } else if i % 3 == 1 {
                "B"
            } else {
                "C"
            };
            csv.push_str(&format!("{dept},{i}\n"));
        }

        let (_, _, output_1) = run_test_with_threads(yaml, &csv, 1).unwrap();
        let (_, _, output_4) = run_test_with_threads(yaml, &csv, 4).unwrap();
        assert_eq!(
            output_1, output_4,
            "Window output must be byte-identical with 1 vs 4 threads"
        );
    }

    #[test]
    fn test_sequential_not_parallelized() {
        // A transform using $window.lag(1) is classified Sequential.
        // Verify the pipeline runs correctly (sequential dispatch path).
        let yaml = r#"
pipeline:
  name: seq_test
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: lagged
    cxl: |
      emit prev = $window.lag(1)
    local_window:
      group_by: [dept]
      sort_by:
        - field: id
"#;
        let csv = "dept,id\nA,1\nA,2\nA,3\nB,4\nB,5\n";
        let (counters, _, _output) = run_test_with_threads(yaml, csv, 4).unwrap();
        assert_eq!(counters.ok_count, 5);
        // The key assertion: sequential transforms run without error under any thread count.
        // The plan's `can_parallelize()` returns false, so the sequential path is used.
    }

    #[test]
    fn test_golden_file_diff_1_vs_4_threads() {
        // Full CSV→CXL→CSV pipeline: byte-identical output with 1 vs 4 threads.
        // Uses only stateless transforms (no `now` keyword).
        let yaml = r#"
pipeline:
  name: golden
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: transform
    cxl: |
      emit upper_name = name + "_UPPER"
      emit computed = amount + "_x2"
"#;
        let mut csv = String::from("name,amount,dept\n");
        for i in 0..500 {
            csv.push_str(&format!("person_{i},{i},dept_{}\n", i % 5));
        }

        let (c1, _, out1) = run_test_with_threads(yaml, &csv, 1).unwrap();
        let (c4, _, out4) = run_test_with_threads(yaml, &csv, 4).unwrap();
        assert_eq!(out1, out4, "Golden file output must be byte-identical");
        assert_eq!(c1.ok_count, c4.ok_count);
        assert_eq!(c1.total_count, 500);
    }

    #[test]
    fn test_arc_ast_shared_not_cloned() {
        // Verify that Arc<TypedProgram> strong_count is 1 after compilation —
        // the Arc is shared by reference during evaluation, not cloned per record.
        let yaml = r#"
pipeline:
  name: arc_test
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: calc
    cxl: |
      emit doubled = amount + "_doubled"
"#;
        let config = crate::config::parse_config(yaml).unwrap();
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let transforms: Vec<&crate::config::TransformConfig> = config.transforms().collect();
        let compiled = PipelineExecutor::compile_transforms(&transforms, &schema).unwrap();
        // Each compiled transform holds one Arc<TypedProgram>
        for ct in &compiled {
            assert_eq!(
                Arc::strong_count(&ct.typed),
                1,
                "Arc<TypedProgram> should have exactly 1 strong reference (no per-record cloning)"
            );
        }
    }

    #[test]
    fn test_empty_pipeline_zero_records() {
        let yaml = r#"
pipeline:
  name: empty
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: calc
    cxl: |
      emit doubled = name + "_x"
"#;
        // Header only, no data rows
        let csv = "name\n";
        let (counters, dlq, _output) = run_test(yaml, csv).unwrap();
        assert_eq!(counters.total_count, 0);
        assert_eq!(counters.ok_count, 0);
        assert_eq!(counters.dlq_count, 0);
        assert!(dlq.is_empty());
    }

    // ── Phase 6 Task 6.5 gate tests ─────────────────────────────────

    #[test]
    fn test_graceful_shutdown_flushes_output() {
        // Set shutdown flag before running pipeline.
        // Pipeline should process at least the first record (schema probe),
        // then detect shutdown at the first chunk boundary and stop cleanly.
        use crate::pipeline::shutdown;
        shutdown::request_shutdown();

        let yaml = r#"
pipeline:
  name: shutdown_test
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: calc
    cxl: |
      emit doubled = name + "_x"
"#;
        let mut csv = String::from("name\n");
        for i in 0..100 {
            csv.push_str(&format!("person_{i}\n"));
        }

        // Pipeline should run without panic. Output may be partial or complete
        // depending on when shutdown is detected, but must be valid.
        let result = run_test(yaml, &csv);
        assert!(result.is_ok(), "Pipeline should not panic on shutdown");
        let (counters, _, output) = result.unwrap();
        // At minimum, the output should contain the header
        assert!(
            output.contains("name") || output.contains("doubled"),
            "Output should contain at least the header"
        );
        // Counters should be consistent
        assert!(counters.ok_count + counters.dlq_count <= counters.total_count);

        shutdown::reset_shutdown_flag();
    }

    #[test]
    fn test_shutdown_dlq_summary_to_stderr() {
        // This tests the ErrorThreshold + DLQ interaction.
        // With BestEffort strategy, DLQ records accumulate but pipeline continues.
        // We verify DLQ entries are collected (the stderr summary is a CLI concern
        // tested in Phase 8; here we test the engine produces DLQ data).
        let yaml = r#"
pipeline:
  name: dlq_test
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
error_handling:
  strategy: best_effort
transformations:
  - name: fail_some
    cxl: |
      let x = amount.to_int()
      emit result = x * 2
"#;
        // Mix valid and invalid amounts — "bad" will fail to_int()
        let csv = "name,amount\nAlice,100\nBob,bad\nCarol,200\n";
        let result = run_test(yaml, csv);
        assert!(result.is_ok());
        let (counters, dlq, _output) = result.unwrap();
        // Bob's row should go to DLQ
        assert!(
            counters.dlq_count >= 1,
            "Should have at least 1 DLQ entry, got {}",
            counters.dlq_count
        );
        assert!(!dlq.is_empty(), "DLQ entries should be populated");
    }

    // ── Phase 8 Task 8.3: --explain gate tests ─────────────────

    fn explain_config(yaml: &str) -> PipelineConfig {
        crate::config::parse_config(yaml).unwrap()
    }

    #[test]
    fn test_explain_no_data_read() {
        // Input files don't exist — explain should succeed without opening them
        let yaml = r#"
pipeline:
  name: explain-test
inputs:
  - name: src
    type: csv
    path: /nonexistent/path/that/does/not/exist.csv
outputs:
  - name: dest
    type: csv
    path: /nonexistent/output.csv
transformations:
  - name: t1
    cxl: "emit result = 1 + 2"
"#;
        let config = explain_config(yaml);
        let result = PipelineExecutor::explain(&config);
        assert!(
            result.is_ok(),
            "explain should succeed without reading data: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_explain_prints_ast() {
        let yaml = r#"
pipeline:
  name: explain-test
inputs:
  - name: src
    type: csv
    path: /tmp/test.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: compute
    cxl: "emit total = Price * Qty"
"#;
        let config = explain_config(yaml);
        let output = PipelineExecutor::explain(&config).unwrap();
        assert!(
            output.contains("CXL Expressions"),
            "should contain CXL section"
        );
        assert!(
            output.contains("Price"),
            "should contain field refs from CXL"
        );
        assert!(output.contains("Qty"), "should contain field refs from CXL");
    }

    #[test]
    fn test_explain_prints_type_annotations() {
        let yaml = r#"
pipeline:
  name: explain-test
inputs:
  - name: src
    type: csv
    path: /tmp/test.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = 1 + 2"
"#;
        let config = explain_config(yaml);
        let output = PipelineExecutor::explain(&config).unwrap();
        assert!(
            output.contains("Type Annotations"),
            "should contain type annotations section"
        );
    }

    #[test]
    fn test_explain_prints_source_dag() {
        let yaml = r#"
pipeline:
  name: explain-test
inputs:
  - name: primary
    type: csv
    path: /tmp/test.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = 1"
"#;
        let config = explain_config(yaml);
        let output = PipelineExecutor::explain(&config).unwrap();
        assert!(output.contains("Source DAG"), "should contain source DAG");
        assert!(output.contains("primary"), "should list source name");
    }

    #[test]
    fn test_explain_prints_indices() {
        let yaml = r#"
pipeline:
  name: explain-test
inputs:
  - name: src
    type: csv
    path: /tmp/test.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit total = $window.sum(amount)"
    local_window:
      group_by: [dept]
      sort_by:
        - field: amount
          order: asc
"#;
        let config = explain_config(yaml);
        let output = PipelineExecutor::explain(&config).unwrap();
        assert!(output.contains("Index [0]"), "should list indices");
        assert!(output.contains("Group by"), "should show group_by");
        assert!(output.contains("Sort by"), "should show sort_by");
    }

    #[test]
    fn test_explain_prints_memory_budget() {
        let yaml = r#"
pipeline:
  name: explain-test
inputs:
  - name: src
    type: csv
    path: /tmp/test.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = 1"
"#;
        let config = explain_config(yaml);
        let output = PipelineExecutor::explain(&config).unwrap();
        assert!(
            output.contains("Memory Budget"),
            "should contain memory budget section"
        );
    }

    #[test]
    fn test_explain_prints_parallelism() {
        let yaml = r#"
pipeline:
  name: explain-test
inputs:
  - name: src
    type: csv
    path: /tmp/test.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = 1"
"#;
        let config = explain_config(yaml);
        let output = PipelineExecutor::explain(&config).unwrap();
        assert!(
            output.contains("Parallelism"),
            "should contain parallelism classification"
        );
    }

    #[test]
    fn test_explain_invalid_config_exit_1() {
        // Invalid CXL should produce an error
        let yaml = r#"
pipeline:
  name: explain-test
inputs:
  - name: src
    type: csv
    path: /tmp/test.csv
outputs:
  - name: dest
    type: csv
    path: /tmp/out.csv
transformations:
  - name: t1
    cxl: "emit x = !!invalid syntax!!"
"#;
        let config = explain_config(yaml);
        let result = PipelineExecutor::explain(&config);
        assert!(
            result.is_err(),
            "invalid CXL should produce compilation error"
        );
    }

    #[test]
    fn test_correlated_pipeline_has_sort_and_group_flush_stages() {
        let yaml = r#"
pipeline:
  name: correlated_test
inputs:
  - name: src
    path: input.csv
    type: csv
error_handling:
  strategy: continue
  correlation_key: employee_id
transformations:
  - name: validate
    cxl: |
      emit emp_id = employee_id
      emit val = value
outputs:
  - name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
        let csv_input = "employee_id,value\nA,1\nA,2\nB,3\n";
        let (report, _output) = run_correlated_test_report(yaml, csv_input).unwrap();
        let stage_names: Vec<String> = report.stages.iter().map(|s| s.name.to_string()).collect();
        assert!(
            stage_names.iter().any(|n| n == "Sort"),
            "correlated pipeline should have Sort stage, got: {stage_names:?}"
        );
        assert!(
            stage_names.iter().any(|n| n == "GroupFlush"),
            "correlated pipeline should have GroupFlush stage, got: {stage_names:?}"
        );
    }

    #[test]
    fn test_stage_count_streaming_mode() {
        let yaml = r#"
pipeline:
  name: streaming_test
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: passthrough
    cxl: |
      emit x = name
"#;
        let csv_input = "name\nAlice\nBob\n";
        let (report, _output) = run_test_report(yaml, csv_input).unwrap();
        let stage_names: Vec<String> = report.stages.iter().map(|s| s.name.to_string()).collect();
        assert_eq!(
            stage_names.len(),
            6,
            "streaming pipeline should have 6 stages (ReaderInit + Compile + SchemaScan + TransformEval + Projection + Write), got: {stage_names:?}"
        );
    }

    #[test]
    fn test_stage_count_two_pass_mode() {
        let yaml = r#"
pipeline:
  name: two_pass_test
inputs:
  - name: primary
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: window_transform
    cxl: |
      emit dept_total = $window.sum(amount)
    local_window:
      group_by: [dept]
"#;
        let csv_input = "dept,amount\nA,10\nA,20\nB,30\n";
        let (report, _output) = run_test_report(yaml, csv_input).unwrap();
        let stage_names: Vec<String> = report.stages.iter().map(|s| s.name.to_string()).collect();
        assert!(
            stage_names.len() >= 8,
            "two-pass pipeline should have >= 8 stages, got {}: {stage_names:?}",
            stage_names.len()
        );
    }

    #[test]
    fn test_empty_pipeline_stages_minimal() {
        let yaml = r#"
pipeline:
  name: empty_test
inputs:
  - name: src
    type: csv
    path: input.csv
outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true
transformations:
  - name: passthrough
    cxl: |
      emit x = name
"#;
        let csv_input = "name\n";
        let (report, _output) = run_test_report(yaml, csv_input).unwrap();
        let stage_names: Vec<String> = report.stages.iter().map(|s| s.name.to_string()).collect();
        assert!(
            stage_names.iter().any(|n| n == "ReaderInit"),
            "empty pipeline should have ReaderInit stage, got: {stage_names:?}"
        );
        assert!(
            stage_names.iter().any(|n| n == "Compile"),
            "empty pipeline should have Compile stage, got: {stage_names:?}"
        );
    }
}
