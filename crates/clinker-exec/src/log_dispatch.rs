//! Typed transform observability dispatch.
//!
//! Authored directives select stable events and explicit record fields. This
//! module converts those directives into the closed telemetry vocabulary; it
//! never writes directly, interpolates messages, or carries raw errors and
//! records across the observability boundary.

use std::sync::Arc;

use clinker_plan::config::{LogDirective, LogLevel, LogTiming};
use clinker_record::Record;
use cxl::eval::{EvalContext, EvalResult, ProgramEvaluator};
use cxl::typecheck::TypedProgram;

use crate::executor::NullStorage;
use crate::telemetry::{
    LogEvent, MetricKey, RunCorrelation, Severity, SignalField, SpanFact, SpanName, SpanStatus,
    TelemetryProducer, bounded_identity, unix_nanos_now,
};

/// Logical run correlation supplied by the executor's stable context.
pub(crate) struct TransformSignalContext<'a> {
    pub(crate) execution_id: &'a str,
    pub(crate) batch_id: &'a str,
    pub(crate) pipeline_name: &'a str,
    /// The transform's exported identity: its authored name at the top level,
    /// and `<call site>.<name>` inside a composition body. Callers build this
    /// through `ExecutorContext::qualified_node_name` rather than reading the
    /// node's name directly, which inside a body is scope-local.
    pub(crate) logical_node: &'a str,
}

/// Runtime state for one Transform's typed signal lifecycle.
///
/// `enabled == None` is the disabled fast path: it allocates no counters or
/// correlation storage and every method returns before inspecting directives
/// or records.
pub(crate) struct LogDispatcher<'a> {
    enabled: Option<EnabledDispatcher<'a>>,
}

/// The signal state of one transform, held across the passes the cascading
/// retract loop makes over it.
///
/// A converge re-dispatches every deferred-region member once per iteration,
/// and each iteration's records supersede the last — the loop restores the
/// forward-pass baseline before it goes round again. One converge is therefore
/// one execution of that transform, which is also the shape the reference
/// documents for the exported signals: a Transform is one span, covering the
/// interval it ran for. A dispatcher built fresh per pass instead opened a span
/// and a `started`/`completed` pair per iteration and restarted every `every:`
/// cadence at one, so an operator summing `clinker.transform.records` over a
/// four-iteration converge read four times the rows the run actually carried.
///
/// The counts here are replaced per pass rather than added to, so what is
/// finally reported is the converged pass — the one whose records the run
/// published. Nothing is emitted until [`Self::close`], because until the loop
/// stops widening there is no way to know which pass that is.
pub(crate) struct TransformSignalCarry {
    /// Cloned from the plan so the close can emit `after` directives without
    /// borrowing a payload the executor has moved on from. One clone per
    /// transform per converge, of a list an author wrote by hand.
    directives: Vec<LogDirective>,
    cadence: Vec<u64>,
    execution_id: Box<str>,
    batch_id: Box<str>,
    pipeline_name: Box<str>,
    logical_node: Box<str>,
    started_at_unix_nanos: u64,
    records: u64,
    errors: u64,
    saw_error: bool,
}

impl TransformSignalCarry {
    /// Report the converged execution: one start, the last pass's counts, one
    /// completion, and one span covering the whole converge.
    pub(crate) fn close(self, producer: &TelemetryProducer) {
        let correlation = RunCorrelation {
            execution_id: &*self.execution_id,
            batch_id: &*self.batch_id,
            pipeline_name: &*self.pipeline_name,
        };
        producer.record_metric(MetricKey::TransformStarted, 1);
        if self.records > 0 {
            producer.record_metric(MetricKey::TransformRecords, self.records);
        }
        if self.errors > 0 {
            producer.record_metric(MetricKey::TransformErrors, self.errors);
        }
        for directive in &self.directives {
            if directive.when == LogTiming::AfterTransform {
                emit_directive(producer, directive, correlation, None);
            }
        }
        producer.record_metric(MetricKey::TransformCompleted, 1);
        let ended_at_unix_nanos = unix_nanos_now().max(self.started_at_unix_nanos);
        let _ = producer.emit_span(SpanFact {
            name: SpanName::Transform,
            status: if self.saw_error {
                SpanStatus::Error
            } else {
                SpanStatus::Ok
            },
            logical_node: &self.logical_node,
            started_at_unix_nanos: self.started_at_unix_nanos,
            ended_at_unix_nanos,
        });
    }
}

/// Counts a converge pass accumulates instead of reporting.
struct DeferredPass {
    records: u64,
    errors: u64,
    /// Whether this is the converge's first pass over the transform. The
    /// `before` directive and the span's start belong to that one.
    first_pass: bool,
}

struct EnabledDispatcher<'a> {
    producer: TelemetryProducer,
    directives: &'a [LogDirective],
    cadence: Vec<u64>,
    /// `Some` while this dispatch is one pass of a cascading-retract converge,
    /// which may run again. Its counts are carried rather than reported, and
    /// the span stays open until the converge stops.
    deferred: Option<DeferredPass>,
    /// Compiled `condition` gate per directive, parallel to `directives` and
    /// `cadence`. `None` where the directive declared no condition.
    gates: Vec<Option<ProgramEvaluator>>,
    /// Whether any `per_record` directive carries a gate, decided once here
    /// because it cannot change afterwards and the answer is read per record.
    wants_per_record_context: bool,
    execution_id: Box<str>,
    batch_id: Box<str>,
    pipeline_name: Box<str>,
    logical_node: Box<str>,
    started_at_unix_nanos: u64,
    saw_error: bool,
    closed: bool,
}

impl<'a> LogDispatcher<'a> {
    /// `conditions` carries one compiled gate slot per entry in `directives`,
    /// in the same order — the pairing plan lowering guarantees.
    pub(crate) fn new(
        producer: Option<TelemetryProducer>,
        directives: &'a [LogDirective],
        conditions: &[Option<Arc<TypedProgram>>],
        context: TransformSignalContext<'_>,
    ) -> Self {
        Self::build(producer, directives, conditions, context, None, false)
    }

    /// A dispatcher for one pass of a cascading-retract converge, resuming
    /// `carry` when the converge has already passed over this transform.
    ///
    /// Everything that describes the execution as a whole — the span, the
    /// `started`/`completed` pair, the record and error counts, and each
    /// directive's `every:` cadence — belongs to the converge rather than to
    /// the pass, so it is carried out again by [`Self::into_carry`] and
    /// reported once the loop stops.
    pub(crate) fn deferred(
        producer: Option<TelemetryProducer>,
        directives: &'a [LogDirective],
        conditions: &[Option<Arc<TypedProgram>>],
        context: TransformSignalContext<'_>,
        carry: Option<TransformSignalCarry>,
    ) -> Self {
        Self::build(producer, directives, conditions, context, carry, true)
    }

    fn build(
        producer: Option<TelemetryProducer>,
        directives: &'a [LogDirective],
        conditions: &[Option<Arc<TypedProgram>>],
        context: TransformSignalContext<'_>,
        carry: Option<TransformSignalCarry>,
        defer: bool,
    ) -> Self {
        // A carry from an earlier pass was built against the same plan node, so
        // its cadence is already the right length; a pass that is the first one
        // starts every directive at zero.
        let resumed = carry.filter(|carry| carry.cadence.len() == directives.len());
        let enabled = producer.map(|producer| EnabledDispatcher {
            producer,
            directives,
            // Counts start at zero on every pass: a pass replaces the one
            // before it rather than adding to it, which is what keeps the
            // reported figure the converged execution's and not a multiple of
            // it. `saw_error` is the exception and latches, because a record
            // that failed on any pass is a fact about the execution.
            deferred: defer.then(|| DeferredPass {
                records: 0,
                errors: 0,
                first_pass: resumed.is_none(),
            }),
            cadence: resumed
                .as_ref()
                .map_or_else(|| vec![0; directives.len()], |carry| carry.cadence.clone()),
            // Indexed by directive position, so it is built to that length
            // rather than from `conditions` directly. Lowering already refuses
            // to emit a node whose gate set is short, so a missing slot cannot
            // occur; building this way keeps the per-record path free of a
            // bounds panic regardless.
            gates: (0..directives.len())
                .map(|index| {
                    conditions
                        .get(index)
                        .and_then(Option::as_ref)
                        .map(|program| ProgramEvaluator::new(Arc::clone(program), false))
                })
                .collect(),
            wants_per_record_context: directives.iter().zip(conditions.iter()).any(
                |(directive, condition)| {
                    directive.when == LogTiming::PerRecord && condition.is_some()
                },
            ),
            execution_id: bounded_correlation(context.execution_id),
            batch_id: bounded_correlation(context.batch_id),
            pipeline_name: bounded_correlation(context.pipeline_name),
            logical_node: bounded_correlation(context.logical_node),
            started_at_unix_nanos: resumed
                .as_ref()
                .map_or_else(unix_nanos_now, |carry| carry.started_at_unix_nanos),
            saw_error: resumed.as_ref().is_some_and(|carry| carry.saw_error),
            closed: false,
        });
        Self { enabled }
    }

    /// Hand this pass's state back to the converge, leaving the span open.
    ///
    /// Returns `None` when telemetry is disabled or this dispatch was not a
    /// converge pass, in which case the dispatcher has already reported
    /// everything it had.
    pub(crate) fn into_carry(mut self) -> Option<TransformSignalCarry> {
        let enabled = self.enabled.as_mut()?;
        let pass = enabled.deferred.take()?;
        // The span belongs to the converge, so this dispatcher must not close
        // one on the way out. `Drop` closes an unclosed span with an error
        // status, which is right for a dispatch that unwound and wrong for one
        // that is being handed on.
        enabled.closed = true;
        Some(TransformSignalCarry {
            directives: enabled.directives.to_vec(),
            cadence: enabled.cadence.clone(),
            execution_id: enabled.execution_id.clone(),
            batch_id: enabled.batch_id.clone(),
            pipeline_name: enabled.pipeline_name.clone(),
            logical_node: enabled.logical_node.clone(),
            started_at_unix_nanos: enabled.started_at_unix_nanos,
            records: pass.records,
            errors: pass.errors,
            saw_error: enabled.saw_error,
        })
    }

    pub(crate) fn fire_before_transform(&mut self) {
        let Some(enabled) = self.enabled.as_mut() else {
            return;
        };
        match enabled.deferred.as_ref() {
            // A converge that has already passed over this transform has begun
            // once and started once; a second `before` event and a second
            // `started` would describe a beginning that never happened.
            Some(pass) if !pass.first_pass => return,
            Some(_) => {
                enabled.emit_timing(LogTiming::BeforeTransform, None);
                return;
            }
            None => {}
        }
        enabled
            .producer
            .record_metric(MetricKey::TransformStarted, 1);
        // The span itself is admitted once, at close. This metric is what
        // reaches a collector while the transform is still running.
        enabled.started_at_unix_nanos = unix_nanos_now();
        enabled.emit_timing(LogTiming::BeforeTransform, None);
    }

    /// Whether a per-record evaluation context will actually be read.
    ///
    /// Callers assemble that context per row, which costs a handful of
    /// reference-count bumps and a source lookup. Only an authored `condition`
    /// on a `per_record` directive reads it: a transform whose directives are
    /// all `before`/`after`, or whose per-record directives carry no gate,
    /// emits from the record alone. Answering "is observability configured at
    /// all" would rebuild it for every row of those transforms too.
    pub(crate) fn wants_per_record_context(&self) -> bool {
        self.enabled
            .as_ref()
            .is_some_and(|enabled| enabled.wants_per_record_context)
    }

    /// `eval_ctx` is the record's own evaluation context, used only to run
    /// authored `condition` gates.
    pub(crate) fn fire_per_record(&mut self, record: &Record, eval_ctx: &EvalContext<'_>) {
        let Some(enabled) = self.enabled.as_mut() else {
            return;
        };
        match enabled.deferred.as_mut() {
            Some(pass) => pass.records = pass.records.saturating_add(1),
            None => enabled
                .producer
                .record_metric(MetricKey::TransformRecords, 1),
        }
        for (index, directive) in enabled.directives.iter().enumerate() {
            if directive.when != LogTiming::PerRecord {
                continue;
            }
            enabled.cadence[index] = enabled.cadence[index].saturating_add(1);
            let every = directive.every.unwrap_or(1);
            if !(enabled.cadence[index] - 1).is_multiple_of(every) {
                continue;
            }
            // Cadence first, then the gate, so the two compose the way the
            // reference documents them: `every: 100` with a condition logs
            // every hundredth record that also matches, not every hundredth
            // match.
            if let Some(gate) = enabled.gates[index].as_mut()
                && !gate_admits(gate, eval_ctx, record)
            {
                continue;
            }
            enabled.emit(directive, Some(record));
        }
    }

    pub(crate) fn fire_on_error(&mut self, record: &Record) {
        let Some(enabled) = self.enabled.as_mut() else {
            return;
        };
        enabled.saw_error = true;
        match enabled.deferred.as_mut() {
            Some(pass) => pass.errors = pass.errors.saturating_add(1),
            None => enabled
                .producer
                .record_metric(MetricKey::TransformErrors, 1),
        }
        // Fired on the pass that saw it, on every pass that sees it. An error
        // event is an observation of a record rather than a summary of the
        // execution, and a converge that keeps failing the same record is
        // reporting a condition that keeps holding.
        enabled.emit_timing(LogTiming::OnError, Some(record));
    }

    /// Close the successful dispatch lifecycle. A recoverable record error is
    /// reflected in the terminal span status while the transform still emits
    /// its authored after event and completion metric.
    pub(crate) fn finish(&mut self) {
        let Some(enabled) = self.enabled.as_mut() else {
            return;
        };
        // A converge pass finishing is not the transform finishing: the loop
        // may restore the baseline and run it again. What this pass has is
        // carried out by `into_carry` and reported once, by
        // `TransformSignalCarry::close`.
        if enabled.deferred.is_some() {
            return;
        }
        enabled.emit_timing(LogTiming::AfterTransform, None);
        enabled
            .producer
            .record_metric(MetricKey::TransformCompleted, 1);
        enabled.close(if enabled.saw_error {
            SpanStatus::Error
        } else {
            SpanStatus::Ok
        });
    }
}

impl EnabledDispatcher<'_> {
    fn emit_timing(&self, timing: LogTiming, record: Option<&Record>) {
        for directive in self.directives {
            if directive.when == timing {
                self.emit(directive, record);
            }
        }
    }

    fn emit(&self, directive: &LogDirective, record: Option<&Record>) {
        emit_directive(
            &self.producer,
            directive,
            RunCorrelation {
                execution_id: &self.execution_id,
                batch_id: &self.batch_id,
                pipeline_name: &self.pipeline_name,
            },
            record,
        );
    }

    fn close(&mut self, status: SpanStatus) {
        if self.closed {
            return;
        }
        // A wall clock that stepped backwards mid-transform would otherwise
        // produce a span that ends before it starts.
        let ended_at_unix_nanos = unix_nanos_now().max(self.started_at_unix_nanos);
        let _ = self.producer.emit_span(SpanFact {
            name: SpanName::Transform,
            status,
            logical_node: &self.logical_node,
            started_at_unix_nanos: self.started_at_unix_nanos,
            ended_at_unix_nanos,
        });
        self.closed = true;
    }
}

impl Drop for EnabledDispatcher<'_> {
    fn drop(&mut self) {
        self.close(SpanStatus::Error);
    }
}

#[cfg(test)]
thread_local! {
    /// Count of record field reads performed on this thread.
    ///
    /// Field gathering is the work the admission peek exists to avoid, and
    /// whether it was avoided is invisible in the telemetry a run reports: a
    /// sampled signal is counted the same either way. Thread-local, so one test
    /// observes only its own dispatcher.
    static FIELD_LOOKUPS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Publish one authored directive as a log signal.
///
/// Shared by the live dispatcher and by the converge's deferred close, so an
/// `after` event reads the same whichever of the two emitted it.
fn emit_directive(
    producer: &TelemetryProducer,
    directive: &LogDirective,
    correlation: RunCorrelation<&str>,
    record: Option<&Record>,
) {
    // Ask before reading the record. Sampling is decided from the event's
    // identity and its lane alone, so at `every: 1` under a policy sampling
    // one record in a hundred, this is what keeps the other ninety-nine
    // from being read field by field to build a signal with nowhere to go.
    // The producer has already counted a refusal under its own reason and
    // lane, so there is nothing to report here.
    let severity = severity(directive.level);
    let Ok(ticket) = producer.peek_log(&directive.name, severity) else {
        return;
    };
    let fields = gather_fields(
        producer,
        directive.fields.as_deref().unwrap_or_default(),
        record,
    );
    let _ = producer.commit_log(
        ticket,
        LogEvent {
            event: &directive.name,
            severity,
            message: &directive.message,
            correlation,
            fields: &fields,
        },
    );
}

/// Read the values a directive asked for out of the record it fired on.
///
/// Timing directives fire without a record and request nothing from one, so
/// they gather no fields.
///
/// A field the record does not carry is counted rather than passed over.
/// `fields` is the only channel by which record data reaches an event, so a
/// selector that matches nothing publishes an event with no attributes at all —
/// which reads exactly like a run where the condition never held. The count is
/// a backstop for a mistake that is decidable when the pipeline compiles; see
/// [`TelemetryProducer::record_missing_field`].
fn gather_fields<'a>(
    producer: &TelemetryProducer,
    requested: &'a [String],
    record: Option<&'a Record>,
) -> Vec<SignalField<'a>> {
    let Some(record) = record else {
        return Vec::new();
    };
    let mut fields = Vec::with_capacity(requested.len());
    for field in requested {
        #[cfg(test)]
        FIELD_LOOKUPS.with(|count| count.set(count.get().saturating_add(1)));
        match record.get(field) {
            Some(value) => fields.push(SignalField::from_record(field, value)),
            None => producer.record_missing_field(),
        }
    }
    fields
}

/// Run one directive's authored gate against the transform's input record.
///
/// A gate that fails to evaluate drops its own event and nothing else. This is
/// a deliberate departure from the row-predicate path in `reshape_dispatch`,
/// which propagates the error and fails the run: telemetry is best effort and
/// must never change transform results or published output, so an unevaluable
/// gate cannot be allowed to abort a pipeline. Dropping rather than emitting is
/// the safe direction — the author asked for a subset, and a gate that cannot
/// prove membership has not proven it.
fn gate_admits(gate: &mut ProgramEvaluator, eval_ctx: &EvalContext<'_>, record: &Record) -> bool {
    matches!(
        gate.eval_record::<NullStorage>(eval_ctx, record, None),
        Ok(EvalResult::Emit { .. } | EvalResult::EmitMany { .. })
    )
}

const fn severity(level: LogLevel) -> Severity {
    match level {
        LogLevel::Trace => Severity::Trace,
        LogLevel::Debug => Severity::Debug,
        LogLevel::Info => Severity::Info,
        LogLevel::Warn => Severity::Warn,
        LogLevel::Error => Severity::Error,
    }
}

/// Retain one identity string for the dispatcher's lifetime under the same
/// ceiling the exporter applies, so a name too long to export whole is already
/// marked here rather than exported as a plausible shorter identity.
fn bounded_correlation(value: &str) -> Box<str> {
    bounded_identity(value).into_owned().into_boxed_str()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use clinker_plan::config::{ClinkerToml, CompileContext, parse_config};
    use clinker_plan::plan::execution::PlanNode;
    use clinker_record::{Record, Schema, Value};
    use cxl::eval::{EvalContext, StableEvalContext};

    use super::{FIELD_LOOKUPS, LogDispatcher, TransformSignalContext};
    use crate::telemetry::TelemetryArena;

    /// Compile a one-transform pipeline and hand back its directives paired
    /// with the gate programs lowering produced for them.
    fn compiled_directives(
        condition: Option<&str>,
    ) -> (
        Vec<clinker_plan::config::LogDirective>,
        Vec<Option<Arc<cxl::typecheck::TypedProgram>>>,
    ) {
        let gate = condition
            .map(|expr| format!("\n          condition: \"{expr}\""))
            .unwrap_or_default();
        let yaml = format!(
            r#"
pipeline:
  name: gate_runtime
nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - {{ name: amount, type: int }}
  - type: transform
    name: observe
    input: input
    config:
      cxl: |
        emit amount = amount
      log:
        - name: transform.seen
          level: info
          when: per_record
          every: 1
          message: seen
          fields: [amount]{gate}
  - type: output
    name: output
    input: observe
    config:
      name: output
      type: csv
      path: output.csv
"#
        );
        let plan = parse_config(&yaml)
            .expect("fixture parses")
            .compile(&CompileContext::default())
            .expect("fixture compiles");
        let payload = plan
            .dag()
            .graph
            .node_weights()
            .find_map(|node| match node {
                PlanNode::Transform {
                    resolved: Some(payload),
                    ..
                } => Some(payload.clone()),
                _ => None,
            })
            .expect("transform payload");
        (payload.log.clone(), payload.log_conditions.clone())
    }

    /// Compile a one-transform pipeline whose transform is named `node_name`,
    /// returning the name the plan kept. Establishes what configuration
    /// actually admits before the dispatcher is asked to export a span for it.
    fn compiled_transform_node_name(node_name: &str) -> String {
        let yaml = format!(
            r#"
pipeline:
  name: node_naming
nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - {{ name: amount, type: int }}
  - type: transform
    name: "{node_name}"
    input: input
    config:
      cxl: |
        emit amount = amount
  - type: output
    name: output
    input: "{node_name}"
    config:
      name: output
      type: csv
      path: output.csv
"#
        );
        let plan = parse_config(&yaml)
            .expect("fixture parses")
            .compile(&CompileContext::default())
            .expect("fixture compiles");
        plan.dag()
            .graph
            .node_weights()
            .find_map(|node| match node {
                PlanNode::Transform { name, .. } => Some(name.clone()),
                _ => None,
            })
            .expect("transform node")
    }

    /// A node name is validated only for duplication, so `normalize orders`
    /// compiles and runs. Its span has to reach the collector like any other:
    /// dropping it left the operator with that transform's metrics and authored
    /// log events present, the span absent, and nothing to explain the gap.
    #[test]
    fn a_node_name_the_planner_accepts_still_closes_a_span() {
        let node_name = compiled_transform_node_name("normalize orders");
        assert_eq!(
            node_name, "normalize orders",
            "configuration applies no grammar to a node name"
        );

        let (directives, conditions) = compiled_directives(None);
        let policy = telemetry_policy();
        let (producer, receiver) = TelemetryArena::reserve(&policy).expect("arena reserves");
        {
            let mut dispatcher = LogDispatcher::new(
                Some(producer),
                &directives,
                &conditions,
                TransformSignalContext {
                    execution_id: "exec",
                    batch_id: "batch",
                    pipeline_name: "node_naming",
                    logical_node: &node_name,
                },
            );
            dispatcher.fire_before_transform();
            dispatcher.finish();
        }

        let mut spans = Vec::new();
        while let Some(batch) = receiver.try_recv_batch() {
            spans.extend(batch.traces().iter().map(|span| span.logical_node.clone()));
        }
        assert_eq!(
            spans,
            vec!["normalize orders".to_string()],
            "the closed span must carry the authored node name"
        );
    }

    fn telemetry_policy() -> clinker_plan::config::ResolvedObservabilityPolicy {
        sampling_policy(1)
    }

    fn sampling_policy(sample_every: u32) -> clinker_plan::config::ResolvedObservabilityPolicy {
        ClinkerToml::parse(&format!(
            r#"
[observability]
arena_bytes = "768KB"
ordinary_lane_bytes = "512KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 8
max_attribute_bytes = "256B"
drop_policy = "drop_newest"
sample_every = {sample_every}
rate_limit_per_second = 100000
rate_limit_burst = 100000
flush_timeout_ms = 1000

[observability.otlp]
endpoint = "https://collector.invalid"
connect_timeout_ms = 100
request_timeout_ms = 200
retry_max_attempts = 1
retry_total_timeout_ms = 500
max_response_bytes = "1KB"

[observability.otlp.auth]
mode = "none"

[observability.lineage]
queue_bytes = "1KB"
max_event_bytes = "512B"
drop_policy = "drop_newest"
flush_timeout_ms = 500
identity_mode = "local_diagnostic_paths"

[[observability.field_policy]]
event = "transform.seen"
field = "amount"
action = "allow"
"#
        ))
        .expect("policy parses")
        .resolve_observability(None)
        .expect("policy resolves")
    }

    /// Drive `amounts` through a dispatcher and return the `amount` attribute
    /// of every log event that actually reached the telemetry arena.
    fn emitted_amounts(condition: Option<&str>, amounts: &[i64]) -> Vec<String> {
        let (directives, conditions) = compiled_directives(condition);
        let policy = telemetry_policy();
        let (producer, receiver) = TelemetryArena::reserve(&policy).expect("arena reserves");
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let stable = StableEvalContext::test_default();
        let eval_ctx = EvalContext::test_default_borrowed(&stable);

        {
            let mut dispatcher = LogDispatcher::new(
                Some(producer),
                &directives,
                &conditions,
                TransformSignalContext {
                    execution_id: "exec",
                    batch_id: "batch",
                    pipeline_name: "gate_runtime",
                    logical_node: "observe",
                },
            );
            for amount in amounts {
                let record = Record::new(Arc::clone(&schema), vec![Value::Integer(*amount)]);
                dispatcher.fire_per_record(&record, &eval_ctx);
            }
        }

        let mut seen = Vec::new();
        while let Some(batch) = receiver.try_recv_batch() {
            for log in batch.logs() {
                if let Some(amount) = log.fields.get("amount") {
                    seen.push(amount.clone());
                }
            }
        }
        seen
    }

    /// A record sampling is going to discard must not be read.
    ///
    /// The directive asks for `amount`, so a dispatcher that builds the signal
    /// and lets admission judge it afterwards reads all twelve records to
    /// export three. Nothing in a run's own accounting distinguishes the two —
    /// the nine are counted as sampled either way — so the read count is the
    /// only evidence, and it is counted rather than timed.
    #[test]
    fn a_record_sampling_will_discard_is_never_read() {
        let (directives, conditions) = compiled_directives(None);
        let (producer, receiver) =
            TelemetryArena::reserve(&sampling_policy(4)).expect("arena reserves");
        let observer = producer.clone();
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let stable = StableEvalContext::test_default();
        let eval_ctx = EvalContext::test_default_borrowed(&stable);
        FIELD_LOOKUPS.with(|count| count.set(0));

        let snapshot = {
            let mut dispatcher = LogDispatcher::new(
                Some(producer),
                &directives,
                &conditions,
                TransformSignalContext {
                    execution_id: "exec",
                    batch_id: "batch",
                    pipeline_name: "gate_runtime",
                    logical_node: "observe",
                },
            );
            for amount in 0..12_i64 {
                let record = Record::new(Arc::clone(&schema), vec![Value::Integer(amount)]);
                dispatcher.fire_per_record(&record, &eval_ctx);
            }
            // Taken before the dispatcher closes its span, which is an
            // admission of its own in the same lane.
            observer.snapshot()
        };

        assert_eq!(
            FIELD_LOOKUPS.with(std::cell::Cell::get),
            3,
            "one in four records is admitted, so one in four is read"
        );
        assert_eq!(snapshot.accepted, 3);
        assert_eq!(snapshot.ordinary_sampled_drops, 9);
        assert_eq!(snapshot.high_sampled_drops, 0);

        let mut seen = Vec::new();
        while let Some(batch) = receiver.try_recv_batch() {
            for log in batch.logs() {
                if let Some(amount) = log.fields.get("amount") {
                    seen.push(amount.clone());
                }
            }
        }
        assert_eq!(
            seen,
            vec!["0".to_string(), "4".to_string(), "8".to_string()],
            "the records that were read are the ones sampling chose"
        );
    }

    /// Run one converge pass over `amounts`, resuming `carry`, and hand the
    /// converge's state back.
    fn converge_pass(
        producer: &crate::telemetry::TelemetryProducer,
        directives: &[clinker_plan::config::LogDirective],
        conditions: &[Option<Arc<cxl::typecheck::TypedProgram>>],
        carry: Option<super::TransformSignalCarry>,
        amounts: &[i64],
    ) -> super::TransformSignalCarry {
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let stable = StableEvalContext::test_default();
        let eval_ctx = EvalContext::test_default_borrowed(&stable);
        let mut dispatcher = LogDispatcher::deferred(
            Some(producer.clone()),
            directives,
            conditions,
            TransformSignalContext {
                execution_id: "exec",
                batch_id: "batch",
                pipeline_name: "gate_runtime",
                logical_node: "observe",
            },
            carry,
        );
        dispatcher.fire_before_transform();
        for amount in amounts {
            let record = Record::new(Arc::clone(&schema), vec![Value::Integer(*amount)]);
            dispatcher.fire_per_record(&record, &eval_ctx);
        }
        dispatcher.finish();
        dispatcher
            .into_carry()
            .expect("a converge pass hands its state back")
    }

    /// A converge is one execution of the transform, and has to report as one.
    ///
    /// The cascading-retract loop re-dispatches every deferred-region member
    /// once per iteration and keeps only the converged result. A dispatcher
    /// built fresh per pass opened a span and a `started`/`completed` pair per
    /// iteration and counted every pass's records, so an operator summing
    /// `clinker.transform.records` over a three-iteration converge read three
    /// times the rows the run carried — and the counters are monotonic sums, so
    /// there is nothing downstream that could have told the difference.
    #[test]
    fn a_converge_reports_one_execution_with_counts_an_operator_can_sum() {
        let (directives, conditions) = compiled_directives(None);
        let (producer, receiver) =
            TelemetryArena::reserve(&telemetry_policy()).expect("arena reserves");

        // Three passes over a shrinking record set: the retract loop takes rows
        // out as it converges, and the last pass is the one whose records the
        // run published.
        let mut carry = converge_pass(&producer, &directives, &conditions, None, &[1, 2, 3, 4]);
        carry = converge_pass(&producer, &directives, &conditions, Some(carry), &[1, 2, 3]);
        carry = converge_pass(&producer, &directives, &conditions, Some(carry), &[1, 2]);
        carry.close(&producer);

        let mut started = 0;
        let mut completed = 0;
        let mut records = 0;
        let mut spans = 0;
        while let Some(batch) = receiver.try_recv_batch() {
            started += batch.metric(crate::telemetry::MetricKey::TransformStarted);
            completed += batch.metric(crate::telemetry::MetricKey::TransformCompleted);
            records += batch.metric(crate::telemetry::MetricKey::TransformRecords);
            spans += batch.traces().len();
        }

        assert_eq!(started, 1, "a converge begins once");
        assert_eq!(completed, 1, "and finishes once");
        assert_eq!(spans, 1, "one Transform is one span, as the reference says");
        assert_eq!(
            records, 2,
            "the reported rows are the converged pass's, not every pass's added together"
        );
    }

    /// The `every:` cadence belongs to the converge, not to the pass.
    ///
    /// Rebuilding the dispatcher per iteration restarted it at one, so a
    /// directive throttled to one record in three fired on the first record of
    /// every pass — the cadence an author configured silently became denser the
    /// more the loop had to converge.
    #[test]
    fn a_cadence_does_not_restart_when_the_converge_goes_round_again() {
        let (mut directives, conditions) = compiled_directives(None);
        directives[0].every = Some(3);
        let (producer, receiver) =
            TelemetryArena::reserve(&telemetry_policy()).expect("arena reserves");

        let carry = converge_pass(&producer, &directives, &conditions, None, &[10, 11, 12, 13]);
        let carry = converge_pass(
            &producer,
            &directives,
            &conditions,
            Some(carry),
            &[20, 21, 22],
        );
        carry.close(&producer);

        let mut seen = Vec::new();
        while let Some(batch) = receiver.try_recv_batch() {
            for log in batch.logs() {
                if let Some(amount) = log.fields.get("amount") {
                    seen.push(amount.clone());
                }
            }
        }
        assert_eq!(
            seen,
            vec!["10".to_string(), "13".to_string(), "22".to_string()],
            "the second pass continues the count the first left off at; \
             restarting it would have fired on 20"
        );
    }

    /// A field name the upstream row does not carry publishes an event with
    /// nothing in it. `fields` is the only channel by which record data reaches
    /// an event, so a misspelling is the difference between an observation and
    /// an empty one — and it looked exactly like a run where the condition
    /// never held.
    #[test]
    fn a_requested_field_the_record_does_not_carry_is_counted() {
        let (directives, conditions) = compiled_directives(None);
        let (producer, receiver) =
            TelemetryArena::reserve(&telemetry_policy()).expect("arena reserves");
        let observer = producer.clone();
        // The directive asks for `amount`; this row spells it `amount_total`.
        let schema = Arc::new(Schema::new(vec!["amount_total".into()]));
        let stable = StableEvalContext::test_default();
        let eval_ctx = EvalContext::test_default_borrowed(&stable);

        {
            let mut dispatcher = LogDispatcher::new(
                Some(producer),
                &directives,
                &conditions,
                TransformSignalContext {
                    execution_id: "exec",
                    batch_id: "batch",
                    pipeline_name: "gate_runtime",
                    logical_node: "observe",
                },
            );
            let record = Record::new(Arc::clone(&schema), vec![Value::Integer(7)]);
            dispatcher.fire_per_record(&record, &eval_ctx);
        }

        assert_eq!(
            observer.snapshot().missing_field_drops,
            1,
            "a selector that matches nothing must not pass unremarked"
        );
        let batch = receiver
            .try_recv_batch()
            .expect("the event itself is still admitted");
        assert!(
            batch.logs()[0].fields.is_empty(),
            "which is the whole problem: {:?}",
            batch.logs()[0]
        );
    }

    /// The other half of the distinction: a field the record does carry is not
    /// counted as missing. Without this the test above would pass on a
    /// dispatcher that counted every requested field.
    #[test]
    fn a_requested_field_the_record_carries_is_not_counted_as_missing() {
        let (directives, conditions) = compiled_directives(None);
        let (producer, _receiver) =
            TelemetryArena::reserve(&telemetry_policy()).expect("arena reserves");
        let observer = producer.clone();
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let stable = StableEvalContext::test_default();
        let eval_ctx = EvalContext::test_default_borrowed(&stable);

        {
            let mut dispatcher = LogDispatcher::new(
                Some(producer),
                &directives,
                &conditions,
                TransformSignalContext {
                    execution_id: "exec",
                    batch_id: "batch",
                    pipeline_name: "gate_runtime",
                    logical_node: "observe",
                },
            );
            let record = Record::new(Arc::clone(&schema), vec![Value::Integer(7)]);
            dispatcher.fire_per_record(&record, &eval_ctx);
        }

        assert_eq!(observer.snapshot().missing_field_drops, 0);
    }

    /// The capability itself: an authored gate must actually suppress the
    /// records it excludes, not merely compile.
    #[test]
    fn condition_suppresses_non_matching_records() {
        let gated = emitted_amounts(Some("amount > 1000"), &[500, 5000, 900, 2000]);
        assert_eq!(
            gated,
            vec!["5000".to_string(), "2000".to_string()],
            "only records satisfying the gate may emit"
        );
    }

    /// The same directive without a gate must still emit for every record —
    /// otherwise the test above would pass on a dispatcher that emits nothing.
    #[test]
    fn absent_condition_leaves_every_record_emitting() {
        let ungated = emitted_amounts(None, &[500, 5000, 900, 2000]);
        assert_eq!(
            ungated,
            vec![
                "500".to_string(),
                "5000".to_string(),
                "900".to_string(),
                "2000".to_string()
            ],
            "a directive with no condition must be ungated"
        );
    }

    /// `every` is applied before the gate, which is what the reference
    /// documents: `every: 2` with a condition logs every second record that
    /// also matches, not every second match.
    #[test]
    fn cadence_is_applied_before_the_gate() {
        let (mut directives, conditions) = compiled_directives(Some("amount > 1000"));
        directives[0].every = Some(2);
        let policy = telemetry_policy();
        let (producer, receiver) = TelemetryArena::reserve(&policy).expect("arena reserves");
        let schema = Arc::new(Schema::new(vec!["amount".into()]));
        let stable = StableEvalContext::test_default();
        let eval_ctx = EvalContext::test_default_borrowed(&stable);

        {
            let mut dispatcher = LogDispatcher::new(
                Some(producer),
                &directives,
                &conditions,
                TransformSignalContext {
                    execution_id: "exec",
                    batch_id: "batch",
                    pipeline_name: "gate_runtime",
                    logical_node: "observe",
                },
            );
            // Cadence admits positions 0 and 2 (values 5000 and 900); the gate
            // then rejects 900. Gate-first would have admitted 5000 and 2000.
            for amount in [5000_i64, 4000, 900, 2000] {
                let record = Record::new(Arc::clone(&schema), vec![Value::Integer(amount)]);
                dispatcher.fire_per_record(&record, &eval_ctx);
            }
        }

        let mut seen = Vec::new();
        while let Some(batch) = receiver.try_recv_batch() {
            for log in batch.logs() {
                if let Some(amount) = log.fields.get("amount") {
                    seen.push(amount.clone());
                }
            }
        }
        assert_eq!(
            seen,
            vec!["5000".to_string()],
            "cadence must select the record before the gate judges it"
        );
    }
}
