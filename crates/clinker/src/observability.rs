//! CLI-owned operational telemetry delivery bulkhead.
//!
//! The planner owns secret-free policy, the executor owns bounded production,
//! and `clinker-net` alone admits the Collector origin and derives signal
//! routes. This module composes those capabilities once and owns one finite
//! blocking worker. Delivery outcomes are optional observations only; they
//! never determine execution, publication, or process status.

use std::fmt;
#[cfg(debug_assertions)]
use std::fs::File;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::Duration;

use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_exec::telemetry::{
    LogRecord, MetricKey, MetricPoint, Severity, SpanName, SpanPhase, SpanStatus, TelemetryArena,
    TelemetryArenaError, TelemetryBatch, TelemetryProducer, TelemetryReceiver, TraceSpan,
};
use clinker_net::{
    AdmittedOtlpEndpoint, OtlpAuthentication, OtlpDeliveryBudget, OtlpDeliveryBudgetError,
    OtlpDeliveryFailure, OtlpDeliveryOutcome, OtlpEndpointAdmissionError, OtlpSignal,
    admit_otlp_endpoint, send_otlp_json,
};
use clinker_plan::config::{ObservabilityAuth, ResolvedObservabilityPolicy};
use serde::Serialize;

use crate::lifecycle::{RunLifecycleSnapshot, RunTerminalOutcome};

const IDLE_POLL: Duration = Duration::from_millis(2);

/// Fixed aggregate counters suitable for the machine terminal.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct ObservabilitySummary {
    pub(crate) logs: SignalSummary,
    pub(crate) metrics: SignalSummary,
    pub(crate) traces: SignalSummary,
}

/// Aggregate-only visibility for one closed signal kind.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize)]
pub(crate) struct SignalSummary {
    pub(crate) accepted: u64,
    pub(crate) rejected: u64,
    pub(crate) attempts: u64,
    pub(crate) failures: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArenaBounds {
    arena_bytes: u64,
    ordinary_lane_bytes: u64,
    high_severity_lane_bytes: u64,
    max_batch_bytes: u64,
}

/// Immutable proof that endpoint, transport, and arena bounds were composed.
pub(crate) struct OtlpRuntimeBundle {
    endpoint: AdmittedOtlpEndpoint,
    delivery_budget: OtlpDeliveryBudget,
    flush_timeout: Duration,
    arena: ArenaBounds,
}

/// Sanitized failure before any source, output, arena, worker, or network work.
#[derive(Debug)]
pub(crate) enum ObservabilityRuntimeError {
    Endpoint(OtlpEndpointAdmissionError),
    CredentialUnresolved,
    Budget(OtlpDeliveryBudgetError),
    Arena(TelemetryArenaError),
    Worker,
}

impl fmt::Display for ObservabilityRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Endpoint(error) => error.fmt(formatter),
            Self::CredentialUnresolved => formatter.write_str(
                "observability.otlp.auth.reference is unresolved for this run. Correction: provision the logical reference through the Phase 4 credential provider before starting the run",
            ),
            Self::Budget(error) => error.fmt(formatter),
            Self::Arena(_) => formatter.write_str(
                "observability.arena_bytes cannot be reserved before execution. Correction: reduce the configured fixed telemetry arena",
            ),
            Self::Worker => formatter.write_str(
                "the bounded observability exporter could not start before execution. Correction: reduce host resource pressure or disable observability",
            ),
        }
    }
}

impl std::error::Error for ObservabilityRuntimeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Endpoint(error) => Some(error),
            Self::Budget(error) => Some(error),
            Self::Arena(error) => Some(error),
            Self::CredentialUnresolved | Self::Worker => None,
        }
    }
}

impl OtlpRuntimeBundle {
    /// Admit raw endpoint text exactly once, then compose exact numeric bounds.
    pub(crate) fn admit(
        policy: &ResolvedObservabilityPolicy,
    ) -> Result<Option<Self>, ObservabilityRuntimeError> {
        if !policy.is_enabled() {
            return Ok(None);
        }
        let otlp = policy.otlp().ok_or(ObservabilityRuntimeError::Worker)?;

        // This is deliberately the first capability transition. No CLI code
        // interprets, normalizes, or reconstructs the authored endpoint.
        let endpoint = admit_otlp_endpoint(otlp.raw_endpoint())
            .map_err(ObservabilityRuntimeError::Endpoint)?;
        let max_request_bytes = usize::try_from(policy.max_batch_bytes())
            .map_err(|_| ObservabilityRuntimeError::Worker)?;
        let delivery_budget = OtlpDeliveryBudget::new(
            max_request_bytes,
            otlp.max_response_bytes().get(),
            otlp.retry_max_attempts().get(),
            otlp.connect_timeout(),
            otlp.request_timeout(),
            otlp.retry_initial_backoff(),
            otlp.retry_total_timeout(),
        )
        .map_err(ObservabilityRuntimeError::Budget)?;
        let bundle = Self {
            endpoint,
            delivery_budget,
            flush_timeout: policy.flush_timeout(),
            arena: ArenaBounds {
                arena_bytes: policy.arena_bytes(),
                ordinary_lane_bytes: policy.ordinary_lane_bytes(),
                high_severity_lane_bytes: policy.high_severity_lane_bytes(),
                max_batch_bytes: policy.max_batch_bytes(),
            },
        };

        // AUTH-01 owns referenced credential capabilities. The check follows
        // endpoint admission and complete numeric composition, but still occurs
        // before arena reservation, worker construction, or any request effect.
        match otlp.auth() {
            ObservabilityAuth::None => Ok(Some(bundle)),
            ObservabilityAuth::Reference { .. } => {
                Err(ObservabilityRuntimeError::CredentialUnresolved)
            }
        }
    }

    pub(crate) fn reserve_arena(
        &self,
        policy: &ResolvedObservabilityPolicy,
    ) -> Result<(TelemetryProducer, TelemetryReceiver), ObservabilityRuntimeError> {
        let supplied = ArenaBounds {
            arena_bytes: policy.arena_bytes(),
            ordinary_lane_bytes: policy.ordinary_lane_bytes(),
            high_severity_lane_bytes: policy.high_severity_lane_bytes(),
            max_batch_bytes: policy.max_batch_bytes(),
        };
        if supplied != self.arena {
            return Err(ObservabilityRuntimeError::Worker);
        }
        TelemetryArena::new(policy).map_err(ObservabilityRuntimeError::Arena)
    }
}

#[derive(Default)]
struct SignalDeliveryReport {
    summary: SignalSummary,
    last_typed: Option<Result<OtlpDeliveryOutcome, OtlpDeliveryFailure>>,
    #[cfg(debug_assertions)]
    injected_outcome: Option<&'static str>,
}

#[derive(Default)]
struct OtlpDeliveryReport {
    logs: SignalDeliveryReport,
    metrics: SignalDeliveryReport,
    traces: SignalDeliveryReport,
}

impl OtlpDeliveryReport {
    fn signal_mut(&mut self, signal: OtlpSignal) -> &mut SignalDeliveryReport {
        match signal {
            OtlpSignal::Logs => &mut self.logs,
            OtlpSignal::Metrics => &mut self.metrics,
            OtlpSignal::Traces => &mut self.traces,
        }
    }

    fn record(&mut self, signal: OtlpSignal, result: DeliveryResult) {
        let report = self.signal_mut(signal);
        report.summary.accepted = report.summary.accepted.saturating_add(result.accepted());
        report.summary.rejected = report.summary.rejected.saturating_add(result.rejected());
        report.summary.attempts = report
            .summary
            .attempts
            .saturating_add(u64::from(result.attempts()));
        if result.failed() {
            report.summary.failures = report.summary.failures.saturating_add(1);
        }
        #[cfg(debug_assertions)]
        if let Some(outcome) = result.injected_outcome() {
            report.injected_outcome = Some(outcome);
        }
        if let DeliveryResult::Typed(typed) = result {
            report.last_typed = Some(typed);
        }
    }

    fn summary(&self) -> ObservabilitySummary {
        ObservabilitySummary {
            logs: self.logs.summary,
            metrics: self.metrics.summary,
            traces: self.traces.summary,
        }
    }

    fn report_failures(&self) {
        for (name, signal) in [
            ("logs", &self.logs),
            ("metrics", &self.metrics),
            ("traces", &self.traces),
        ] {
            if let Some(Err(error)) = signal.last_typed.as_ref() {
                eprintln!(
                    "clinker: optional OTLP {name} delivery outcome: kind={:?} attempts={}",
                    error.kind(),
                    error.attempts()
                );
            }
            #[cfg(debug_assertions)]
            if let Some(outcome) = signal.injected_outcome {
                eprintln!(
                    "clinker: optional OTLP {name} delivery outcome: kind={outcome} attempts={}",
                    signal.summary.attempts
                );
            }
        }
    }
}

enum DeliveryResult {
    Typed(Result<OtlpDeliveryOutcome, OtlpDeliveryFailure>),
    #[cfg(debug_assertions)]
    Injected {
        accepted: u64,
        rejected: u64,
        attempts: u32,
        failed: bool,
        outcome: Option<&'static str>,
    },
    EncodingFailure,
}

impl DeliveryResult {
    fn accepted(&self) -> u64 {
        match self {
            Self::Typed(Ok(outcome)) => outcome.accepted(),
            #[cfg(debug_assertions)]
            Self::Injected { accepted, .. } => *accepted,
            Self::Typed(Err(_)) | Self::EncodingFailure => 0,
        }
    }

    fn rejected(&self) -> u64 {
        match self {
            Self::Typed(Ok(outcome)) => outcome.rejected(),
            Self::Typed(Err(_)) | Self::EncodingFailure => 1,
            #[cfg(debug_assertions)]
            Self::Injected { rejected, .. } => *rejected,
        }
    }

    fn attempts(&self) -> u32 {
        match self {
            Self::Typed(Ok(outcome)) => outcome.attempts(),
            Self::Typed(Err(error)) => error.attempts(),
            #[cfg(debug_assertions)]
            Self::Injected { attempts, .. } => *attempts,
            Self::EncodingFailure => 0,
        }
    }

    fn failed(&self) -> bool {
        match self {
            Self::Typed(Err(_)) | Self::EncodingFailure => true,
            Self::Typed(Ok(_)) => false,
            #[cfg(debug_assertions)]
            Self::Injected { failed, .. } => *failed,
        }
    }

    #[cfg(debug_assertions)]
    fn injected_outcome(&self) -> Option<&'static str> {
        match self {
            Self::Injected { outcome, .. } => *outcome,
            Self::Typed(_) | Self::EncodingFailure => None,
        }
    }
}

enum WorkerCommand {
    Finish(RunLifecycleSnapshot),
}

/// One finite run's sole telemetry receiver and blocking exporter worker.
pub(crate) struct OtlpWorker {
    command: mpsc::SyncSender<WorkerCommand>,
    done: mpsc::Receiver<OtlpDeliveryReport>,
    handle: Option<thread::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    flush_timeout: Duration,
}

impl OtlpWorker {
    pub(crate) fn start(
        bundle: OtlpRuntimeBundle,
        receiver: TelemetryReceiver,
        shutdown: ShutdownToken,
    ) -> Result<Self, ObservabilityRuntimeError> {
        let backend = DeliveryBackend::new()?;
        let max_payload_bytes = bundle.arena.max_batch_bytes;
        let mut payload = BoundedPayload::new(max_payload_bytes)?;
        let (command, commands) = mpsc::sync_channel(1);
        let (done_sender, done) = mpsc::sync_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let flush_timeout = bundle.flush_timeout;
        let handle = thread::Builder::new()
            .name("clinker-otlp-export".to_owned())
            .spawn(move || {
                let mut state = WorkerState {
                    bundle,
                    receiver,
                    backend,
                    payload: &mut payload,
                    shutdown,
                    stop: worker_stop,
                    report: OtlpDeliveryReport::default(),
                };
                loop {
                    state.drain_available();
                    match commands.recv_timeout(IDLE_POLL) {
                        Ok(WorkerCommand::Finish(snapshot)) => {
                            state.drain_available();
                            state.deliver_lifecycle(&snapshot);
                            let _ = done_sender.try_send(state.report);
                            return;
                        }
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                        Err(mpsc::RecvTimeoutError::Disconnected) => return,
                    }
                    if state.stop.load(Ordering::Acquire) {
                        return;
                    }
                }
            })
            .map_err(|_| ObservabilityRuntimeError::Worker)?;
        Ok(Self {
            command,
            done,
            handle: Some(handle),
            stop,
            flush_timeout,
        })
    }

    pub(crate) fn finish(mut self, snapshot: RunLifecycleSnapshot) -> ObservabilitySummary {
        if self.command.send(WorkerCommand::Finish(snapshot)).is_err() {
            return ObservabilitySummary::default();
        }
        match self.done.recv_timeout(self.flush_timeout) {
            Ok(report) => {
                if let Some(handle) = self.handle.take() {
                    let _ = handle.join();
                }
                report.report_failures();
                report.summary()
            }
            Err(_) => {
                self.stop.store(true, Ordering::Release);
                // A still-active transport call remains bounded by the smaller
                // admitted retry-total deadline. Dropping the handle prevents
                // the optional path from extending the authoritative run.
                let _ = self.handle.take();
                ObservabilitySummary {
                    traces: SignalSummary {
                        rejected: 1,
                        failures: 1,
                        ..SignalSummary::default()
                    },
                    ..ObservabilitySummary::default()
                }
            }
        }
    }
}

impl Drop for OtlpWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}

struct WorkerState<'a> {
    bundle: OtlpRuntimeBundle,
    receiver: TelemetryReceiver,
    backend: DeliveryBackend,
    payload: &'a mut BoundedPayload,
    shutdown: ShutdownToken,
    stop: Arc<AtomicBool>,
    report: OtlpDeliveryReport,
}

impl WorkerState<'_> {
    fn drain_available(&mut self) {
        while let Some(batch) = self.receiver.try_recv_batch() {
            self.deliver_batch(&batch);
        }
    }

    fn deliver_batch(&mut self, batch: &TelemetryBatch) {
        if !batch.logs().is_empty() {
            let envelope = logs_envelope(batch.logs());
            self.deliver(OtlpSignal::Logs, &envelope, batch.logs().len() as u64);
        }
        if !batch.metrics().is_empty() {
            let envelope = metrics_envelope(batch.metrics());
            self.deliver(OtlpSignal::Metrics, &envelope, batch.metrics().len() as u64);
        }
        if !batch.traces().is_empty() {
            let envelope = traces_envelope(batch.traces());
            self.deliver(OtlpSignal::Traces, &envelope, batch.traces().len() as u64);
        }
    }

    fn deliver_lifecycle(&mut self, snapshot: &RunLifecycleSnapshot) {
        let envelope = lifecycle_envelope(snapshot);
        self.deliver(OtlpSignal::Traces, &envelope, 1);
    }

    fn deliver<T: Serialize>(&mut self, signal: OtlpSignal, value: &T, item_count: u64) {
        let result = match self.payload.encode(value) {
            Ok(payload) => self
                .backend
                .deliver(&self.bundle, signal, payload, item_count, &|| {
                    self.shutdown.is_requested() || self.stop.load(Ordering::Acquire)
                }),
            Err(_) => DeliveryResult::EncodingFailure,
        };
        self.report.record(signal, result);
    }
}

enum DeliveryBackend {
    Network,
    #[cfg(debug_assertions)]
    Injected(InjectedDelivery),
}

impl DeliveryBackend {
    fn new() -> Result<Self, ObservabilityRuntimeError> {
        #[cfg(debug_assertions)]
        if std::env::var_os("CLINKER_TEST_OTLP_OUTCOME").as_deref()
            == Some(std::ffi::OsStr::new("success"))
        {
            return InjectedDelivery::from_environment().map(Self::Injected);
        }
        Ok(Self::Network)
    }

    fn deliver(
        &mut self,
        bundle: &OtlpRuntimeBundle,
        signal: OtlpSignal,
        payload: &[u8],
        item_count: u64,
        shutdown: &dyn Fn() -> bool,
    ) -> DeliveryResult {
        #[cfg(not(debug_assertions))]
        let _ = item_count;
        match self {
            Self::Network => DeliveryResult::Typed(send_otlp_json(
                &bundle.endpoint,
                signal,
                payload,
                &bundle.delivery_budget,
                shutdown,
                OtlpAuthentication::None,
            )),
            #[cfg(debug_assertions)]
            Self::Injected(injected) => injected.deliver(signal, payload, item_count),
        }
    }
}

#[cfg(debug_assertions)]
struct InjectedDelivery {
    capture: Option<File>,
    reported: [bool; 3],
}

#[cfg(debug_assertions)]
impl InjectedDelivery {
    fn from_environment() -> Result<Self, ObservabilityRuntimeError> {
        let capture = std::env::var_os("CLINKER_TEST_OTLP_CAPTURE")
            .map(File::create)
            .transpose()
            .map_err(|_| ObservabilityRuntimeError::Worker)?;
        Ok(Self {
            capture,
            reported: [false; 3],
        })
    }

    fn deliver(&mut self, signal: OtlpSignal, payload: &[u8], item_count: u64) -> DeliveryResult {
        if let Some(capture) = self.capture.as_mut() {
            #[derive(Serialize)]
            struct Captured<'a> {
                signal: &'static str,
                authentication: &'static str,
                payload: &'a serde_json::Value,
            }
            let parsed = match serde_json::from_slice(payload) {
                Ok(parsed) => parsed,
                Err(_) => return DeliveryResult::EncodingFailure,
            };
            let event = Captured {
                signal: signal_name(signal),
                authentication: "none",
                payload: &parsed,
            };
            if serde_json::to_writer(&mut *capture, &event).is_err()
                || capture.write_all(b"\n").is_err()
                || capture.flush().is_err()
            {
                return DeliveryResult::EncodingFailure;
            }
        }
        let mode = injected_signal_outcome(signal);
        let signal_index = match signal {
            OtlpSignal::Logs => 0,
            OtlpSignal::Metrics => 1,
            OtlpSignal::Traces => 2,
        };
        if self.reported[signal_index] {
            return DeliveryResult::Injected {
                accepted: 0,
                rejected: 0,
                attempts: 0,
                failed: false,
                outcome: None,
            };
        }
        self.reported[signal_index] = true;
        let (accepted, rejected, attempts, failed, outcome) = match mode.as_deref() {
            None | Some("success") => (1, 0, 1, false, None),
            Some("partial") => (0, 1, 1, false, Some("partial")),
            Some("transient-exhausted") => {
                (0, item_count.max(1), 3, true, Some("transient-exhausted"))
            }
            Some("shutdown") => (0, item_count.max(1), 0, true, Some("shutdown")),
            Some("flush-expiry") => (0, item_count.max(1), 0, true, Some("flush-expiry")),
            Some("permanent-rejection") => {
                (0, item_count.max(1), 1, true, Some("permanent-rejection"))
            }
            Some("auth") => (0, item_count.max(1), 1, true, Some("auth")),
            Some("tls") => (0, item_count.max(1), 1, true, Some("tls")),
            Some("connect") => (0, item_count.max(1), 1, true, Some("connect")),
            Some("read-timeout") => (0, item_count.max(1), 1, true, Some("read-timeout")),
            Some("oversized-response") => {
                (0, item_count.max(1), 1, true, Some("oversized-response"))
            }
            Some("malformed-response") => {
                (0, item_count.max(1), 1, true, Some("malformed-response"))
            }
            Some(_) => (0, item_count.max(1), 0, true, Some("invalid-test-outcome")),
        };
        DeliveryResult::Injected {
            accepted,
            rejected,
            attempts,
            failed,
            outcome,
        }
    }
}

#[cfg(debug_assertions)]
fn injected_signal_outcome(signal: OtlpSignal) -> Option<String> {
    let variable = match signal {
        OtlpSignal::Logs => "CLINKER_TEST_OTLP_LOGS_OUTCOME",
        OtlpSignal::Metrics => "CLINKER_TEST_OTLP_METRICS_OUTCOME",
        OtlpSignal::Traces => "CLINKER_TEST_OTLP_TRACES_OUTCOME",
    };
    std::env::var(variable).ok()
}

#[cfg(debug_assertions)]
fn signal_name(signal: OtlpSignal) -> &'static str {
    match signal {
        OtlpSignal::Logs => "logs",
        OtlpSignal::Metrics => "metrics",
        OtlpSignal::Traces => "traces",
    }
}

struct BoundedPayload {
    bytes: Vec<u8>,
    max_bytes: usize,
}

impl BoundedPayload {
    fn new(max_bytes: u64) -> Result<Self, ObservabilityRuntimeError> {
        let max_bytes =
            usize::try_from(max_bytes).map_err(|_| ObservabilityRuntimeError::Worker)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(max_bytes)
            .map_err(|_| ObservabilityRuntimeError::Worker)?;
        Ok(Self { bytes, max_bytes })
    }

    fn encode<T: Serialize>(&mut self, value: &T) -> io::Result<&[u8]> {
        self.bytes.clear();
        let mut writer = BoundedWriter {
            bytes: &mut self.bytes,
            max_bytes: self.max_bytes,
        };
        serde_json::to_writer(&mut writer, value).map_err(io::Error::other)?;
        Ok(&self.bytes)
    }
}

struct BoundedWriter<'a> {
    bytes: &'a mut Vec<u8>,
    max_bytes: usize,
}

impl Write for BoundedWriter<'_> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        let end = self
            .bytes
            .len()
            .checked_add(input.len())
            .filter(|end| *end <= self.max_bytes)
            .ok_or_else(|| io::Error::new(io::ErrorKind::WriteZero, "OTLP batch is full"))?;
        self.bytes.extend_from_slice(input);
        debug_assert_eq!(self.bytes.len(), end);
        Ok(input.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Serialize)]
struct LogsEnvelope<'a> {
    #[serde(rename = "resourceLogs")]
    resource_logs: [ResourceLogs<'a>; 1],
}

#[derive(Serialize)]
struct ResourceLogs<'a> {
    #[serde(rename = "scopeLogs")]
    scope_logs: [ScopeLogs<'a>; 1],
}

#[derive(Serialize)]
struct ScopeLogs<'a> {
    #[serde(rename = "logRecords")]
    log_records: Vec<OtlpLogRecord<'a>>,
}

#[derive(Serialize)]
struct OtlpLogRecord<'a> {
    #[serde(rename = "severityText")]
    severity_text: &'static str,
    body: StringValue<'a>,
    attributes: Vec<KeyValue<'a>>,
}

fn logs_envelope(logs: &[LogRecord]) -> LogsEnvelope<'_> {
    let log_records = logs
        .iter()
        .map(|record| {
            let mut attributes = Vec::with_capacity(record.fields.len().saturating_add(1));
            attributes.push(KeyValue::string("clinker.event", &record.event));
            attributes.extend(
                record
                    .fields
                    .iter()
                    .map(|(key, value)| KeyValue::string(key, value)),
            );
            OtlpLogRecord {
                severity_text: severity_name(record.severity),
                body: StringValue::new(&record.message),
                attributes,
            }
        })
        .collect();
    LogsEnvelope {
        resource_logs: [ResourceLogs {
            scope_logs: [ScopeLogs { log_records }],
        }],
    }
}

#[derive(Serialize)]
struct MetricsEnvelope {
    #[serde(rename = "resourceMetrics")]
    resource_metrics: [ResourceMetrics; 1],
}

#[derive(Serialize)]
struct ResourceMetrics {
    #[serde(rename = "scopeMetrics")]
    scope_metrics: [ScopeMetrics; 1],
}

#[derive(Serialize)]
struct ScopeMetrics {
    metrics: Vec<OtlpMetric>,
}

#[derive(Serialize)]
struct OtlpMetric {
    name: &'static str,
    gauge: Gauge,
}

#[derive(Serialize)]
struct Gauge {
    #[serde(rename = "dataPoints")]
    data_points: [NumberDataPoint; 1],
}

#[derive(Serialize)]
struct NumberDataPoint {
    #[serde(rename = "asInt")]
    as_int: String,
}

fn metrics_envelope(metrics: &[MetricPoint]) -> MetricsEnvelope {
    MetricsEnvelope {
        resource_metrics: [ResourceMetrics {
            scope_metrics: [ScopeMetrics {
                metrics: metrics
                    .iter()
                    .map(|point| OtlpMetric {
                        name: metric_name(point.key),
                        gauge: Gauge {
                            data_points: [NumberDataPoint {
                                as_int: point.value.to_string(),
                            }],
                        },
                    })
                    .collect(),
            }],
        }],
    }
}

#[derive(Serialize)]
struct TracesEnvelope<'a> {
    #[serde(rename = "resourceSpans")]
    resource_spans: [ResourceSpans<'a>; 1],
}

#[derive(Serialize)]
struct ResourceSpans<'a> {
    #[serde(rename = "scopeSpans")]
    scope_spans: [ScopeSpans<'a>; 1],
}

#[derive(Serialize)]
struct ScopeSpans<'a> {
    spans: Vec<OtlpSpan<'a>>,
}

#[derive(Serialize)]
struct OtlpSpan<'a> {
    name: &'static str,
    attributes: Vec<KeyValue<'a>>,
    status: OtlpStatus,
    #[serde(rename = "startTimeUnixNano", skip_serializing_if = "Option::is_none")]
    start_time_unix_nano: Option<String>,
    #[serde(rename = "endTimeUnixNano", skip_serializing_if = "Option::is_none")]
    end_time_unix_nano: Option<String>,
}

#[derive(Serialize)]
struct OtlpStatus {
    code: u8,
}

fn traces_envelope(traces: &[TraceSpan]) -> TracesEnvelope<'_> {
    TracesEnvelope {
        resource_spans: [ResourceSpans {
            scope_spans: [ScopeSpans {
                spans: traces
                    .iter()
                    .map(|span| OtlpSpan {
                        name: span_name(span.name),
                        attributes: vec![
                            KeyValue::string("clinker.logical_node", &span.logical_node),
                            KeyValue::static_string("clinker.span.phase", span_phase(span.phase)),
                        ],
                        status: OtlpStatus {
                            code: span_status(span.status),
                        },
                        start_time_unix_nano: None,
                        end_time_unix_nano: None,
                    })
                    .collect(),
            }],
        }],
    }
}

fn lifecycle_envelope(snapshot: &RunLifecycleSnapshot) -> TracesEnvelope<'_> {
    let start = snapshot.start();
    let terminal = snapshot.terminal();
    let fingerprint = start.fingerprint();
    let digest = clinker_exec::output::sidecar::hash_to_hex(&fingerprint.digest());
    let (outcome, status, failure_code) = match terminal.map(|facts| facts.outcome()) {
        Some(RunTerminalOutcome::Complete) => ("complete", 1, None),
        Some(RunTerminalOutcome::Abort) => ("abort", 2, None),
        Some(RunTerminalOutcome::Fail(failure)) => ("fail", 2, Some(failure.code())),
        None => ("unavailable", 0, None),
    };
    let mut attributes = vec![
        KeyValue::string("clinker.batch_id", start.batch_id()),
        KeyValue::string("clinker.execution_id", start.execution_id()),
        KeyValue::static_string("clinker.plan.algorithm", fingerprint.algorithm()),
        KeyValue::integer("clinker.plan.version", u64::from(fingerprint.version())),
        KeyValue::owned_string("clinker.plan.digest", digest),
        KeyValue::static_string("clinker.run.outcome", outcome),
    ];
    if let Some(failure_code) = failure_code {
        attributes.push(KeyValue::static_string(
            "clinker.run.failure_code",
            failure_code,
        ));
    }
    if let Some(terminal) = terminal {
        let counts = terminal.counts();
        attributes.extend([
            KeyValue::integer("clinker.records.read", counts.records_read),
            KeyValue::integer("clinker.records.written", counts.records_written),
            KeyValue::integer("clinker.records.dlq", counts.records_dlq),
        ]);
    }
    TracesEnvelope {
        resource_spans: [ResourceSpans {
            scope_spans: [ScopeSpans {
                spans: vec![OtlpSpan {
                    name: "clinker.run",
                    attributes,
                    status: OtlpStatus { code: status },
                    start_time_unix_nano: Some(unix_nanos(start.started_at())),
                    end_time_unix_nano: terminal.map(|facts| unix_nanos(facts.finished_at())),
                }],
            }],
        }],
    }
}

#[derive(Serialize)]
struct KeyValue<'a> {
    key: &'a str,
    value: AnyValue<'a>,
}

impl<'a> KeyValue<'a> {
    fn string(key: &'a str, value: &'a str) -> Self {
        Self {
            key,
            value: AnyValue::String(value.into()),
        }
    }

    fn static_string(key: &'a str, value: &'static str) -> Self {
        Self {
            key,
            value: AnyValue::String(std::borrow::Cow::Borrowed(value).into()),
        }
    }

    fn owned_string(key: &'a str, value: String) -> Self {
        Self {
            key,
            value: AnyValue::String(StringValue {
                string_value: std::borrow::Cow::Owned(value),
            }),
        }
    }

    fn integer(key: &'a str, value: u64) -> Self {
        Self {
            key,
            value: AnyValue::integer(value),
        }
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum AnyValue<'a> {
    String(StringValue<'a>),
    Integer(IntegerValue),
}

#[derive(Serialize)]
struct StringValue<'a> {
    #[serde(rename = "stringValue")]
    string_value: std::borrow::Cow<'a, str>,
}

impl<'a> StringValue<'a> {
    fn new(value: &'a str) -> Self {
        Self {
            string_value: value.into(),
        }
    }
}

#[derive(Serialize)]
struct IntegerValue {
    #[serde(rename = "intValue")]
    int_value: String,
}

impl<'a> From<std::borrow::Cow<'a, str>> for StringValue<'a> {
    fn from(string_value: std::borrow::Cow<'a, str>) -> Self {
        Self { string_value }
    }
}

impl<'a> From<&'a str> for StringValue<'a> {
    fn from(value: &'a str) -> Self {
        Self::new(value)
    }
}

impl<'a> AnyValue<'a> {
    fn integer(value: u64) -> Self {
        Self::Integer(IntegerValue {
            int_value: value.to_string(),
        })
    }
}

impl<'a> From<StringValue<'a>> for AnyValue<'a> {
    fn from(value: StringValue<'a>) -> Self {
        Self::String(value)
    }
}

fn severity_name(severity: Severity) -> &'static str {
    match severity {
        Severity::Trace => "TRACE",
        Severity::Debug => "DEBUG",
        Severity::Info => "INFO",
        Severity::Warn => "WARN",
        Severity::Error => "ERROR",
    }
}

fn metric_name(key: MetricKey) -> &'static str {
    match key {
        MetricKey::TransformStarted => "clinker.transform.started",
        MetricKey::TransformCompleted => "clinker.transform.completed",
        MetricKey::TransformRecords => "clinker.transform.records",
        MetricKey::TransformErrors => "clinker.transform.errors",
    }
}

fn span_name(name: SpanName) -> &'static str {
    match name {
        SpanName::Transform => "clinker.transform",
    }
}

fn span_phase(phase: SpanPhase) -> &'static str {
    match phase {
        SpanPhase::Start => "start",
        SpanPhase::End => "end",
    }
}

fn span_status(status: SpanStatus) -> u8 {
    match status {
        SpanStatus::Unset => 0,
        SpanStatus::Ok => 1,
        SpanStatus::Error => 2,
    }
}

fn unix_nanos(timestamp: chrono::DateTime<chrono::Utc>) -> String {
    timestamp
        .timestamp_nanos_opt()
        .unwrap_or(i64::MAX)
        .max(0)
        .to_string()
}
