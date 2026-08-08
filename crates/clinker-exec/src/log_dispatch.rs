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

struct EnabledDispatcher<'a> {
    producer: TelemetryProducer,
    directives: &'a [LogDirective],
    cadence: Vec<u64>,
    /// Compiled `condition` gate per directive, parallel to `directives` and
    /// `cadence`. `None` where the directive declared no condition.
    gates: Vec<Option<ProgramEvaluator>>,
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
        let enabled = producer.map(|producer| EnabledDispatcher {
            producer,
            directives,
            cadence: vec![0; directives.len()],
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
            execution_id: bounded_correlation(context.execution_id),
            batch_id: bounded_correlation(context.batch_id),
            pipeline_name: bounded_correlation(context.pipeline_name),
            logical_node: bounded_correlation(context.logical_node),
            started_at_unix_nanos: unix_nanos_now(),
            saw_error: false,
            closed: false,
        });
        Self { enabled }
    }

    pub(crate) fn fire_before_transform(&mut self) {
        let Some(enabled) = self.enabled.as_mut() else {
            return;
        };
        enabled
            .producer
            .record_metric(MetricKey::TransformStarted, 1);
        // The span itself is admitted once, at close. This metric is what
        // reaches a collector while the transform is still running.
        enabled.started_at_unix_nanos = unix_nanos_now();
        enabled.emit_timing(LogTiming::BeforeTransform, None);
    }

    /// Whether any signal is being produced for this transform.
    ///
    /// Callers assemble the per-record evaluation context, which costs a
    /// handful of reference-count bumps and a source lookup per row. A
    /// deployment that configures no observability reads none of it, so this
    /// lets the hot loop skip building what nothing will look at.
    pub(crate) const fn is_enabled(&self) -> bool {
        self.enabled.is_some()
    }

    /// `eval_ctx` is the record's own evaluation context, used only to run
    /// authored `condition` gates.
    pub(crate) fn fire_per_record(&mut self, record: &Record, eval_ctx: &EvalContext<'_>) {
        let Some(enabled) = self.enabled.as_mut() else {
            return;
        };
        enabled
            .producer
            .record_metric(MetricKey::TransformRecords, 1);
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
        enabled
            .producer
            .record_metric(MetricKey::TransformErrors, 1);
        enabled.emit_timing(LogTiming::OnError, Some(record));
    }

    /// Close the successful dispatch lifecycle. A recoverable record error is
    /// reflected in the terminal span status while the transform still emits
    /// its authored after event and completion metric.
    pub(crate) fn finish(&mut self) {
        let Some(enabled) = self.enabled.as_mut() else {
            return;
        };
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
        let requested = directive.fields.as_deref().unwrap_or_default();
        let mut fields = Vec::with_capacity(requested.len());
        if let Some(record) = record {
            for field in requested {
                if let Some(value) = record.get(field) {
                    fields.push(SignalField::from_record(field, value));
                }
            }
        }
        let _ = self.producer.emit_log(LogEvent {
            event: &directive.name,
            severity: severity(directive.level),
            message: &directive.message,
            correlation: RunCorrelation {
                execution_id: &self.execution_id,
                batch_id: &self.batch_id,
                pipeline_name: &self.pipeline_name,
            },
            fields: &fields,
        });
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

    use super::{LogDispatcher, TransformSignalContext};
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
        ClinkerToml::parse(
            r#"
[observability]
arena_bytes = "768KB"
ordinary_lane_bytes = "512KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 8
max_attribute_bytes = "256B"
drop_policy = "drop-newest"
sample_every = 1
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
drop_policy = "drop-newest"
flush_timeout_ms = 500
identity_mode = "local_diagnostic_paths"

[[observability.field_policy]]
event = "transform.seen"
field = "amount"
action = "allow"
"#,
        )
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
