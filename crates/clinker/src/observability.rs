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
    LogRecord, MetricKey, MetricPoint, Severity, SpanName, SpanStatus, TelemetryArena,
    TelemetryArenaError, TelemetryBatch, TelemetryProducer, TelemetryReceiver, TraceSpan,
    unix_nanos_now,
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
    max_attributes_per_event: u32,
}

/// Re-encoding a stored record as OTLP rewraps each retained attribute as
/// `{"key":…,"value":{"stringValue":…}}`. That costs a constant per attribute,
/// not a multiple of the record, so the request bound is the slot bound plus an
/// exact allowance rather than the slot bound itself.
const OTLP_ATTRIBUTE_WRAPPING_BYTES: u64 = 64;

/// Run correlation and the event name become attributes on top of the policy's
/// per-event allowance.
const OTLP_ENGINE_ATTRIBUTES: u64 = 8;

/// Envelope nesting, severity text, body wrapper, and the fixed-shape lifecycle
/// span, whose attributes are bounded independently of the arena.
const OTLP_ENVELOPE_OVERHEAD_BYTES: u64 = 4 * 1024;

impl ArenaBounds {
    fn from_policy(policy: &ResolvedObservabilityPolicy) -> Self {
        Self {
            arena_bytes: policy.arena_bytes(),
            ordinary_lane_bytes: policy.ordinary_lane_bytes(),
            high_severity_lane_bytes: policy.high_severity_lane_bytes(),
            max_batch_bytes: policy.max_batch_bytes(),
            max_attributes_per_event: policy.max_attributes_per_event(),
        }
    }

    /// Bound one HTTP request. `max_batch_bytes` bounds one *stored record*;
    /// a delivery re-encodes a whole drained batch, so binding the two to the
    /// same number made a realistic per-record log config overflow the buffer
    /// and discard the entire batch before any request. This bound covers one
    /// maximal record's OTLP expansion; a batch needing more is split across
    /// requests.
    const fn request_capacity_bytes(self) -> u64 {
        let attributes =
            (self.max_attributes_per_event as u64).saturating_add(OTLP_ENGINE_ATTRIBUTES);
        self.max_batch_bytes
            .saturating_add(attributes.saturating_mul(OTLP_ATTRIBUTE_WRAPPING_BYTES))
            .saturating_add(OTLP_ENVELOPE_OVERHEAD_BYTES)
    }
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
        // The transport rejects anything over this cap outright, so it has to
        // be the per-request bound and not the per-stored-record bound.
        let arena = ArenaBounds::from_policy(policy);
        let max_request_bytes = usize::try_from(arena.request_capacity_bytes())
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
            arena,
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
        let supplied = ArenaBounds::from_policy(policy);
        if supplied != self.arena {
            return Err(ObservabilityRuntimeError::Worker);
        }
        TelemetryArena::reserve(policy).map_err(ObservabilityRuntimeError::Arena)
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
        #[cfg(debug_assertions)]
        if std::env::var_os("CLINKER_TEST_OTLP_WORKER_START_FAILURE").as_deref()
            == Some(std::ffi::OsStr::new("1"))
        {
            return Err(ObservabilityRuntimeError::Worker);
        }
        let backend = DeliveryBackend::new()?;
        let mut payload = BoundedPayload::new(bundle.arena.request_capacity_bytes())?;
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
                    trace_id: new_trace_id(),
                    next_span_id: RUN_SPAN_ID.saturating_add(1),
                    metrics_window_start_unix_nanos: unix_nanos_now(),
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
    /// One run is one trace; every exported span carries this id.
    trace_id: String,
    next_span_id: u64,
    /// Start of the delta window the next drained counters describe.
    metrics_window_start_unix_nanos: u64,
}

impl WorkerState<'_> {
    fn drain_available(&mut self) {
        while let Some(batch) = self.receiver.try_recv_batch() {
            self.deliver_batch(&batch);
        }
    }

    fn deliver_batch(&mut self, batch: &TelemetryBatch) {
        self.deliver_chunks(OtlpSignal::Logs, batch.logs(), |payload, _offset, chunk| {
            payload.encode(&logs_envelope(chunk))
        });
        if !batch.metrics().is_empty() {
            let window = self.take_metrics_window();
            self.deliver_chunks(
                OtlpSignal::Metrics,
                batch.metrics(),
                |payload, _offset, chunk| payload.encode(&metrics_envelope(chunk, window)),
            );
        }
        if !batch.traces().is_empty() {
            let trace_id = self.trace_id.clone();
            let first_span_id = self.reserve_span_ids(batch.traces().len());
            self.deliver_chunks(
                OtlpSignal::Traces,
                batch.traces(),
                |payload, offset, chunk| {
                    let first =
                        first_span_id.saturating_add(u64::try_from(offset).unwrap_or(u64::MAX));
                    payload.encode(&traces_envelope(chunk, &trace_id, first))
                },
            );
        }
    }

    fn deliver_lifecycle(&mut self, snapshot: &RunLifecycleSnapshot) {
        let envelope = lifecycle_envelope(snapshot, &self.trace_id);
        let encoded = self.payload.encode(&envelope);
        self.deliver_encoded(OtlpSignal::Traces, encoded, 1);
    }

    /// Split one drained batch across as many requests as its bounded buffer
    /// needs. Halving on overflow keeps the memory fixed while guaranteeing
    /// forward progress; only an item that cannot be represented alone is
    /// reported as a failure, and it is reported as exactly one item rather
    /// than taking its whole batch down with it.
    fn deliver_chunks<T>(
        &mut self,
        signal: OtlpSignal,
        items: &[T],
        encode: impl Fn(&mut BoundedPayload, usize, &[T]) -> io::Result<()>,
    ) {
        let mut start = 0;
        while start < items.len() {
            let (len, encoded) = encode_largest_prefix(self.payload, start, items, &encode);
            self.deliver_encoded(signal, encoded, len as u64);
            start += len;
        }
    }

    fn deliver_encoded(&mut self, signal: OtlpSignal, encoded: io::Result<()>, item_count: u64) {
        let result = match encoded {
            Ok(()) => self.backend.deliver(
                &self.bundle,
                signal,
                self.payload.bytes(),
                item_count,
                &|| self.shutdown.is_requested() || self.stop.load(Ordering::Acquire),
            ),
            Err(_) => DeliveryResult::EncodingFailure,
        };
        self.report.record(signal, result);
    }

    /// Close the current delta window and open the next one.
    fn take_metrics_window(&mut self) -> MetricsWindow {
        let start = self.metrics_window_start_unix_nanos;
        let end = unix_nanos_now().max(start);
        self.metrics_window_start_unix_nanos = end;
        MetricsWindow { start, end }
    }

    fn reserve_span_ids(&mut self, count: usize) -> u64 {
        let first = self.next_span_id;
        self.next_span_id = self
            .next_span_id
            .checked_add(u64::try_from(count).unwrap_or(u64::MAX))
            .unwrap_or(RUN_SPAN_ID.saturating_add(1));
        first
    }
}

/// Encode the longest prefix of `items[offset..]` that fits the fixed payload,
/// halving on overflow. Returns the item count encoded and the encoding result;
/// a returned count of one with an error means that single item cannot be
/// represented at all, which is the only case a caller reports as a loss.
fn encode_largest_prefix<T>(
    payload: &mut BoundedPayload,
    offset: usize,
    items: &[T],
    encode: &impl Fn(&mut BoundedPayload, usize, &[T]) -> io::Result<()>,
) -> (usize, io::Result<()>) {
    let mut len = items.len() - offset;
    loop {
        let encoded = encode(payload, offset, &items[offset..offset + len]);
        if encoded.is_ok() || len == 1 {
            return (len, encoded);
        }
        len = len.div_ceil(2);
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

    fn encode<T: Serialize>(&mut self, value: &T) -> io::Result<()> {
        self.bytes.clear();
        let mut writer = BoundedWriter {
            bytes: &mut self.bytes,
            max_bytes: self.max_bytes,
        };
        serde_json::to_writer(&mut writer, value).map_err(io::Error::other)
    }

    fn bytes(&self) -> &[u8] {
        &self.bytes
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
            let mut attributes = Vec::with_capacity(record.fields.len().saturating_add(4));
            attributes.push(KeyValue::string("clinker.event", &record.event));
            // Engine-supplied run identity, not record data. Without it an
            // exported record cannot be joined to the machine stream or the
            // lineage events for the same run.
            attributes.push(KeyValue::string(
                "clinker.execution_id",
                &record.correlation.execution_id,
            ));
            attributes.push(KeyValue::string(
                "clinker.batch_id",
                &record.correlation.batch_id,
            ));
            attributes.push(KeyValue::string(
                "clinker.pipeline_name",
                &record.correlation.pipeline_name,
            ));
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
    unit: &'static str,
    sum: Sum,
}

#[derive(Serialize)]
struct Sum {
    #[serde(rename = "dataPoints")]
    data_points: [NumberDataPoint; 1],
    #[serde(rename = "aggregationTemporality")]
    aggregation_temporality: u8,
    #[serde(rename = "isMonotonic")]
    is_monotonic: bool,
}

#[derive(Serialize)]
struct NumberDataPoint {
    #[serde(rename = "startTimeUnixNano")]
    start_time_unix_nano: String,
    #[serde(rename = "timeUnixNano")]
    time_unix_nano: String,
    #[serde(rename = "asInt")]
    as_int: String,
}

/// `AGGREGATION_TEMPORALITY_DELTA`.
const DELTA_TEMPORALITY: u8 = 1;

/// The half-open interval one drain's counters describe.
#[derive(Clone, Copy)]
struct MetricsWindow {
    start: u64,
    end: u64,
}

/// The producer's counters are drained with `swap(0)`, so each point is the
/// count accumulated since the previous drain. Exporting that as a gauge made a
/// backend read the last delta as the absolute total; a monotonic sum with
/// explicit delta temporality and both required timestamps is what lets a
/// backend re-derive the run total and any rate over it.
fn metrics_envelope(metrics: &[MetricPoint], window: MetricsWindow) -> MetricsEnvelope {
    MetricsEnvelope {
        resource_metrics: [ResourceMetrics {
            scope_metrics: [ScopeMetrics {
                metrics: metrics
                    .iter()
                    .map(|point| OtlpMetric {
                        name: metric_name(point.key),
                        unit: "1",
                        sum: Sum {
                            data_points: [NumberDataPoint {
                                start_time_unix_nano: window.start.to_string(),
                                time_unix_nano: window.end.to_string(),
                                as_int: point.value.to_string(),
                            }],
                            aggregation_temporality: DELTA_TEMPORALITY,
                            is_monotonic: true,
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
    #[serde(rename = "traceId")]
    trace_id: &'a str,
    #[serde(rename = "spanId")]
    span_id: String,
    #[serde(rename = "parentSpanId", skip_serializing_if = "Option::is_none")]
    parent_span_id: Option<String>,
    name: &'static str,
    attributes: Vec<KeyValue<'a>>,
    status: OtlpStatus,
    #[serde(rename = "startTimeUnixNano")]
    start_time_unix_nano: String,
    #[serde(rename = "endTimeUnixNano")]
    end_time_unix_nano: String,
}

#[derive(Serialize)]
struct OtlpStatus {
    code: u8,
}

/// The lifecycle span is the trace root and every transform span is one of its
/// children. A span id only has to be unique within its trace and must not be
/// zero, so a per-run counter is sufficient: the trace id carries the global
/// uniqueness.
const RUN_SPAN_ID: u64 = 1;

/// Derive one run's trace id. OTLP requires 16 bytes; a v7 UUID always sets its
/// version and variant bits, so the result is never the invalid all-zero id.
fn new_trace_id() -> String {
    format!(
        "{:032x}",
        u128::from_be_bytes(uuid::Uuid::now_v7().into_bytes())
    )
}

/// OTLP/JSON encodes `trace_id` and `span_id` as hex, not as the base64 the
/// standard Protobuf JSON mapping would use for a bytes field.
fn span_id_hex(span_id: u64) -> String {
    format!("{span_id:016x}")
}

fn traces_envelope<'a>(
    traces: &'a [TraceSpan],
    trace_id: &'a str,
    first_span_id: u64,
) -> TracesEnvelope<'a> {
    TracesEnvelope {
        resource_spans: [ResourceSpans {
            scope_spans: [ScopeSpans {
                spans: traces
                    .iter()
                    .enumerate()
                    .map(|(index, span)| OtlpSpan {
                        trace_id,
                        span_id: span_id_hex(
                            first_span_id.saturating_add(u64::try_from(index).unwrap_or(u64::MAX)),
                        ),
                        parent_span_id: Some(span_id_hex(RUN_SPAN_ID)),
                        name: span_name(span.name),
                        attributes: vec![KeyValue::string(
                            "clinker.logical_node",
                            &span.logical_node,
                        )],
                        status: OtlpStatus {
                            code: span_status(span.status),
                        },
                        start_time_unix_nano: span.started_at_unix_nanos.to_string(),
                        end_time_unix_nano: span
                            .ended_at_unix_nanos
                            .max(span.started_at_unix_nanos)
                            .to_string(),
                    })
                    .collect(),
            }],
        }],
    }
}

fn lifecycle_envelope<'a>(
    snapshot: &'a RunLifecycleSnapshot,
    trace_id: &'a str,
) -> TracesEnvelope<'a> {
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
    // A run whose terminal facts are unavailable still has to produce a closed
    // span: an unset end time is not a span a collector can accept.
    let started_at = unix_nanos(start.started_at());
    let ended_at = terminal
        .map_or_else(unix_nanos_now, |facts| unix_nanos(facts.finished_at()))
        .max(started_at);
    TracesEnvelope {
        resource_spans: [ResourceSpans {
            scope_spans: [ScopeSpans {
                spans: vec![OtlpSpan {
                    trace_id,
                    span_id: span_id_hex(RUN_SPAN_ID),
                    parent_span_id: None,
                    name: "clinker.run",
                    attributes,
                    status: OtlpStatus { code: status },
                    start_time_unix_nano: started_at.to_string(),
                    end_time_unix_nano: ended_at.to_string(),
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

fn span_status(status: SpanStatus) -> u8 {
    match status {
        SpanStatus::Unset => 0,
        SpanStatus::Ok => 1,
        SpanStatus::Error => 2,
    }
}

fn unix_nanos(timestamp: chrono::DateTime<chrono::Utc>) -> u64 {
    u64::try_from(timestamp.timestamp_nanos_opt().unwrap_or(i64::MAX).max(0)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use clinker_exec::telemetry::RunCorrelation;
    use serde_json::Value;

    use super::*;

    const SHIPPED_BOUNDS: ArenaBounds = ArenaBounds {
        arena_bytes: 4 * 1024 * 1024,
        ordinary_lane_bytes: 3 * 1024 * 1024,
        high_severity_lane_bytes: 1024 * 1024,
        max_batch_bytes: 256 * 1024,
        max_attributes_per_event: 32,
    };

    fn correlation() -> RunCorrelation<String> {
        RunCorrelation {
            execution_id: "0198c0de-0000-7000-8000-00000000cafe".to_owned(),
            batch_id: "batch-987654321".to_owned(),
            pipeline_name: "payments".to_owned(),
        }
    }

    /// A log record that fills its arena slot: the largest one the producer
    /// admits, and therefore the largest one a delivery has to be able to
    /// carry. Every attribute sits at the shipped per-attribute cap.
    fn slot_filling_log_record(bounds: ArenaBounds) -> LogRecord {
        let attribute_bytes = 4 * 1024;
        let fields = (0..bounds.max_attributes_per_event)
            .map(|index| (format!("field_{index:03}"), "v".repeat(attribute_bytes)))
            .collect::<BTreeMap<_, _>>();
        let mut record = LogRecord {
            event: "transform.customer_seen".to_owned(),
            severity: Severity::Info,
            message: String::new(),
            correlation: correlation(),
            fields,
        };
        let slot = usize::try_from(bounds.max_batch_bytes).expect("slot bound fits this platform");
        let occupied = stored_bytes(&record);
        record.message = "m".repeat(slot.saturating_sub(occupied));
        assert!(
            stored_bytes(&record) <= slot,
            "the fixture must be admissible into one arena slot"
        );
        record
    }

    /// The producer stores a record as JSON in one fixed slot; this is that
    /// stored size, before any OTLP rewrapping.
    fn stored_bytes(record: &LogRecord) -> usize {
        serde_json::to_vec(record).expect("record serializes").len()
    }

    fn span(logical_node: &str, started: u64, ended: u64) -> TraceSpan {
        TraceSpan {
            name: SpanName::Transform,
            status: SpanStatus::Ok,
            logical_node: logical_node.to_owned(),
            started_at_unix_nanos: started,
            ended_at_unix_nanos: ended,
        }
    }

    fn to_value<T: Serialize>(value: &T) -> Value {
        serde_json::to_value(value).expect("envelope serializes")
    }

    fn spans_of(envelope: &Value) -> &Vec<Value> {
        envelope["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .expect("spans")
    }

    fn is_hex(value: &str, width: usize) -> bool {
        value.len() == width && value.bytes().all(|byte| byte.is_ascii_hexdigit())
    }

    #[test]
    fn every_exported_span_carries_a_valid_trace_and_span_identifier() {
        let trace_id = new_trace_id();
        assert!(is_hex(&trace_id, 32), "16-byte hex trace id: {trace_id}");
        assert_ne!(trace_id, "0".repeat(32), "the all-zero trace id is invalid");

        let traces = [span("alpha", 10, 20), span("beta", 30, 40)];
        let envelope = to_value(&traces_envelope(&traces, &trace_id, 2));
        let spans = spans_of(&envelope);
        assert_eq!(spans.len(), 2);

        let mut span_ids = Vec::new();
        for exported in spans {
            let id = exported["spanId"].as_str().expect("spanId");
            assert!(is_hex(id, 16), "8-byte hex span id: {id}");
            assert_ne!(id, &"0".repeat(16), "the all-zero span id is invalid");
            assert_eq!(exported["traceId"], trace_id.as_str());
            assert_eq!(
                exported["parentSpanId"].as_str().expect("parentSpanId"),
                span_id_hex(RUN_SPAN_ID),
                "transform spans hang off the lifecycle root"
            );
            span_ids.push(id.to_owned());
        }
        span_ids.dedup();
        assert_eq!(span_ids.len(), 2, "span ids are unique within one trace");

        assert_eq!(spans[0]["startTimeUnixNano"], "10");
        assert_eq!(spans[0]["endTimeUnixNano"], "20");
        assert_eq!(spans[1]["startTimeUnixNano"], "30");
        assert_eq!(spans[1]["endTimeUnixNano"], "40");
    }

    #[test]
    fn an_exported_span_is_never_open_at_either_end() {
        // A backwards wall-clock step must not produce a span that ends before
        // it starts, and neither boundary is ever omitted.
        let traces = [span("alpha", 500, 100)];
        let envelope = to_value(&traces_envelope(&traces, &new_trace_id(), 2));
        let exported = &spans_of(&envelope)[0];
        assert_eq!(exported["startTimeUnixNano"], "500");
        assert_eq!(exported["endTimeUnixNano"], "500");
        for required in ["traceId", "spanId", "startTimeUnixNano", "endTimeUnixNano"] {
            assert!(
                exported.get(required).is_some_and(|value| !value.is_null()),
                "{required} is required on every span: {exported}"
            );
        }
    }

    #[test]
    fn drained_counters_export_as_delta_sums_with_required_timestamps() {
        let metrics = [MetricPoint {
            key: MetricKey::TransformRecords,
            value: 20_000,
        }];
        let window = MetricsWindow {
            start: 1_000,
            end: 2_000,
        };
        let envelope = to_value(&metrics_envelope(&metrics, window));
        let metric = &envelope["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0];
        assert_eq!(metric["name"], "clinker.transform.records");
        assert!(
            metric.get("gauge").is_none(),
            "a per-flush delta read as a gauge is read as the absolute total"
        );
        let sum = &metric["sum"];
        assert_eq!(
            sum["aggregationTemporality"], 1,
            "AGGREGATION_TEMPORALITY_DELTA"
        );
        assert_eq!(sum["isMonotonic"], true);
        let point = &sum["dataPoints"][0];
        assert_eq!(point["asInt"], "20000");
        assert_eq!(point["startTimeUnixNano"], "1000");
        assert_eq!(point["timeUnixNano"], "2000");
    }

    #[test]
    fn one_slot_filling_log_record_fits_one_request_but_not_one_arena_slot() {
        let record = slot_filling_log_record(SHIPPED_BOUNDS);
        let batch = std::slice::from_ref(&record);

        let mut request = BoundedPayload::new(SHIPPED_BOUNDS.request_capacity_bytes())
            .expect("request buffer reserves");
        request
            .encode(&logs_envelope(batch))
            .expect("the per-delivery bound admits one slot-filling record");
        assert!(
            request.bytes().len() > stored_bytes(&record),
            "rewrapping every attribute as an OTLP key/value pair expands the record"
        );

        // The per-record slot bound was reused as the request bound. Re-encoding
        // one stored record as OTLP already overflows it, and the whole drained
        // batch was then discarded before any HTTP call.
        let mut coupled = BoundedPayload::new(SHIPPED_BOUNDS.max_batch_bytes)
            .expect("slot-sized buffer reserves");
        assert!(
            coupled.encode(&logs_envelope(batch)).is_err(),
            "the per-record slot bound is not a per-request bound"
        );
    }

    #[test]
    fn a_batch_too_large_for_one_request_is_split_rather_than_dropped() {
        let records = (0..4)
            .map(|_| slot_filling_log_record(SHIPPED_BOUNDS))
            .collect::<Vec<_>>();
        let mut whole_batch =
            BoundedPayload::new(SHIPPED_BOUNDS.request_capacity_bytes()).expect("buffer reserves");
        assert!(
            whole_batch.encode(&logs_envelope(&records)).is_err(),
            "the fixture must be a batch that cannot travel as one request"
        );

        let mut payload = BoundedPayload::new(SHIPPED_BOUNDS.request_capacity_bytes())
            .expect("request buffer reserves");
        let encode = |payload: &mut BoundedPayload, _offset: usize, chunk: &[LogRecord]| {
            payload.encode(&logs_envelope(chunk))
        };

        let mut delivered = 0;
        let mut requests = 0;
        while delivered < records.len() {
            let (len, encoded) = encode_largest_prefix(&mut payload, delivered, &records, &encode);
            encoded.expect("every chunk encodes once split");
            assert!(payload.bytes().len() as u64 <= SHIPPED_BOUNDS.request_capacity_bytes());
            delivered += len;
            requests += 1;
        }
        assert_eq!(delivered, records.len(), "no record is discarded");
        assert!(
            requests > 1,
            "a batch that cannot fit one request is split across requests"
        );
    }

    #[test]
    fn exported_log_records_carry_run_correlation() {
        let record = LogRecord {
            event: "transform.customer_seen".to_owned(),
            severity: Severity::Info,
            message: "customer observed".to_owned(),
            correlation: correlation(),
            fields: BTreeMap::new(),
        };
        let envelope = to_value(&logs_envelope(std::slice::from_ref(&record)));
        let attributes = envelope["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"]
            .as_array()
            .expect("attributes")
            .iter()
            .map(|entry| {
                (
                    entry["key"].as_str().expect("key").to_owned(),
                    entry["value"]["stringValue"]
                        .as_str()
                        .expect("stringValue")
                        .to_owned(),
                )
            })
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            attributes.get("clinker.execution_id").map(String::as_str),
            Some("0198c0de-0000-7000-8000-00000000cafe")
        );
        assert_eq!(
            attributes.get("clinker.batch_id").map(String::as_str),
            Some("batch-987654321")
        );
        assert_eq!(
            attributes.get("clinker.pipeline_name").map(String::as_str),
            Some("payments")
        );
    }
}
