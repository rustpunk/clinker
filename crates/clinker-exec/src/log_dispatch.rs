//! Typed transform observability dispatch.
//!
//! Authored directives select stable events and explicit record fields. This
//! module converts those directives into the closed telemetry vocabulary; it
//! never writes directly, interpolates messages, or carries raw errors and
//! records across the observability boundary.

use clinker_plan::config::{LogDirective, LogLevel, LogTiming};
use clinker_record::Record;

use crate::telemetry::{
    LogEvent, MetricKey, Severity, SignalField, SpanFact, SpanName, SpanPhase, SpanStatus,
    TelemetryProducer,
};

const MAX_CORRELATION_BYTES: usize = 128;

/// Logical run correlation supplied by the executor's stable context.
pub(crate) struct TransformSignalContext<'a> {
    pub(crate) execution_id: &'a str,
    pub(crate) batch_id: &'a str,
    pub(crate) pipeline_name: &'a str,
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
    execution_id: Box<str>,
    batch_id: Box<str>,
    pipeline_name: Box<str>,
    logical_node: Box<str>,
    saw_error: bool,
    closed: bool,
}

impl<'a> LogDispatcher<'a> {
    pub(crate) fn new(
        producer: Option<TelemetryProducer>,
        directives: &'a [LogDirective],
        context: TransformSignalContext<'_>,
    ) -> Self {
        let enabled = producer.map(|producer| EnabledDispatcher {
            producer,
            directives,
            cadence: vec![0; directives.len()],
            execution_id: bounded_correlation(context.execution_id),
            batch_id: bounded_correlation(context.batch_id),
            pipeline_name: bounded_correlation(context.pipeline_name),
            logical_node: bounded_correlation(context.logical_node),
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
        let _ = enabled.producer.emit_span(SpanFact {
            name: SpanName::Transform,
            phase: SpanPhase::Start,
            status: SpanStatus::Unset,
            logical_node: &enabled.logical_node,
        });
        enabled.emit_timing(LogTiming::BeforeTransform, None);
    }

    pub(crate) fn fire_per_record(&mut self, record: &Record) {
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
        let mut fields = Vec::with_capacity(requested.len().saturating_add(3));
        fields.push(SignalField::new("execution_id", &self.execution_id));
        fields.push(SignalField::new("batch_id", &self.batch_id));
        fields.push(SignalField::new("pipeline_name", &self.pipeline_name));
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
            fields: &fields,
        });
    }

    fn close(&mut self, status: SpanStatus) {
        if self.closed {
            return;
        }
        let _ = self.producer.emit_span(SpanFact {
            name: SpanName::Transform,
            phase: SpanPhase::End,
            status,
            logical_node: &self.logical_node,
        });
        self.closed = true;
    }
}

impl Drop for EnabledDispatcher<'_> {
    fn drop(&mut self) {
        self.close(SpanStatus::Error);
    }
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

fn bounded_correlation(value: &str) -> Box<str> {
    if value.len() <= MAX_CORRELATION_BYTES {
        return value.into();
    }
    let mut end = MAX_CORRELATION_BYTES;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].into()
}
