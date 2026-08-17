//! The OTLP exporter: one finite blocking worker that drains the telemetry
//! arena to a collector.
//!
//! The planner owns secret-free policy, the executor owns bounded production,
//! and `clinker-net` alone admits the collector origin and derives signal
//! routes. This module composes those three and owns the worker. Delivery
//! outcomes are optional observations only; they never determine execution,
//! publication, or process status.
//!
//! Split from the reporting vocabulary in the parent module because only this
//! half needs a collector. The counters an operator reads in the machine
//! terminal are shaped the same whether or not one was ever configured.

use std::fmt;
#[cfg(debug_assertions)]
use std::fs::File;
use std::io::{self, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_exec::telemetry::{
    DrainOutcome, LogRecord, MetricKey, MetricPoint, RunCorrelation, Severity, SpanName,
    SpanStatus, TelemetryArena, TelemetryArenaError, TelemetryBatch, TelemetryProducer,
    TelemetryReceiver, TraceSpan, unix_nanos_now,
};
use clinker_net::{
    AdmittedOtlpEndpoint, OtlpAuthentication, OtlpDeliveryBudget, OtlpDeliveryBudgetError,
    OtlpDeliveryFailure, OtlpDeliveryFailureKind, OtlpDeliveryOutcome, OtlpEndpointAdmissionError,
    OtlpSignal, admit_otlp_endpoint, send_otlp_json,
};
use clinker_plan::config::{ObservabilityAuth, ResolvedObservabilityPolicy};
use serde::Serialize;

#[cfg(test)]
use clinker_exec::telemetry::ArenaSnapshot;

#[cfg(test)]
use super::AdmissionSummary;
use super::{ObservabilitySummary, SignalSummary};

const IDLE_POLL: Duration = Duration::from_millis(2);

/// How many times the final flush re-asks for an arena a producer is holding
/// before it reports the flush as incomplete. Each refusal costs one
/// `yield_now`, and the producers are quiescent by the time the final flush
/// runs, so this is headroom over a straggler rather than a wait.
const FINAL_DRAIN_REFUSALS: u32 = 64;
use crate::lifecycle::{RunLifecycleSnapshot, RunTerminalOutcome};

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

/// Run correlation, the event name, and the observation timestamp ride on top
/// of the policy's per-event allowance.
const OTLP_ENGINE_ATTRIBUTES: u64 = 8;

/// Envelope nesting, the resource block naming the producing run, severity
/// text, body wrapper, and the fixed-shape lifecycle span, whose attributes are
/// bounded independently of the arena.
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
                "observability.otlp.auth.reference is unresolved for this run. Correction: provision the referenced credential before starting the run, or set observability.otlp.auth.mode = \"none\" to export without authentication",
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
        // Observability can be enabled for lineage alone. No collector export
        // was requested, so there is no OTLP runtime to admit — a configuration
        // choice, not a capability that failed to start.
        let Some(otlp) = policy.otlp() else {
            return Ok(None);
        };

        // This is deliberately the first capability transition. No CLI code
        // interprets, normalizes, or reconstructs the authored endpoint.
        let endpoint = admit_otlp_endpoint(otlp.raw_endpoint())
            .map_err(ObservabilityRuntimeError::Endpoint)?;
        // The transport rejects anything over this cap outright, so it has to
        // be the per-request bound and not the per-stored-record bound.
        let arena = ArenaBounds::from_policy(policy);
        let max_request_bytes = usize::try_from(arena.request_capacity_bytes())
            .map_err(|_| ObservabilityRuntimeError::Worker)?;
        let delivery_budget = OtlpDeliveryBudget::new(clinker_net::OtlpDeliveryBounds {
            max_request_bytes,
            max_response_bytes: otlp.max_response_bytes().get(),
            max_attempts: otlp.retry_max_attempts().get(),
            connect_timeout: otlp.connect_timeout(),
            request_timeout: otlp.request_timeout(),
            retry_backoff: otlp.retry_initial_backoff(),
            total_timeout: otlp.retry_total_timeout(),
        })
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
    /// The most recent failure, which no later success clears.
    ///
    /// Retaining the last *delivery* instead meant a run whose final chunk
    /// happened to succeed reported nothing, however many chunks before it had
    /// been rejected. Without `--machine ndjson-v1` the summary is discarded
    /// entirely, so this line is the only place an operator ever learns that
    /// telemetry was lost.
    last_failure: Option<DeliveryFailureCause>,
    #[cfg(debug_assertions)]
    injected_outcome: Option<&'static str>,
}

/// Why one chunk of a signal was not delivered.
enum DeliveryFailureCause {
    /// The transport returned a sanitized typed failure.
    Transport(OtlpDeliveryFailure),
    /// The chunk could not be represented as an OTLP request at all, so there
    /// is no transport outcome to name.
    Unencodable,
}

struct OtlpDeliveryReport {
    logs: SignalDeliveryReport,
    metrics: SignalDeliveryReport,
    traces: SignalDeliveryReport,
    /// Whether the final flush emptied the arena. False when a producer still
    /// held it after the flush exhausted its attempts, in which case signals
    /// may remain that no collector will ever be sent.
    arena_drained: bool,
}

impl Default for OtlpDeliveryReport {
    fn default() -> Self {
        Self {
            logs: SignalDeliveryReport::default(),
            metrics: SignalDeliveryReport::default(),
            traces: SignalDeliveryReport::default(),
            arena_drained: true,
        }
    }
}

impl OtlpDeliveryReport {
    fn signal_mut(&mut self, signal: OtlpSignal) -> &mut SignalDeliveryReport {
        match signal {
            OtlpSignal::Logs => &mut self.logs,
            OtlpSignal::Metrics => &mut self.metrics,
            OtlpSignal::Traces => &mut self.traces,
        }
    }

    fn record(&mut self, signal: OtlpSignal, result: DeliveryResult, item_count: u64) {
        let report = self.signal_mut(signal);
        report.summary.accepted = report
            .summary
            .accepted
            .saturating_add(result.accepted(item_count));
        report.summary.rejected = report
            .summary
            .rejected
            .saturating_add(result.rejected(item_count));
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
        match result {
            DeliveryResult::Typed(Err(error)) => {
                report.last_failure = Some(DeliveryFailureCause::Transport(error));
            }
            DeliveryResult::EncodingFailure => {
                report.last_failure = Some(DeliveryFailureCause::Unencodable);
            }
            _ => {}
        }
    }

    fn summary(&self) -> ObservabilitySummary {
        ObservabilitySummary {
            logs: self.logs.summary,
            metrics: self.metrics.summary,
            traces: self.traces.summary,
            // A flush that never got the arena delivered whatever it held
            // before that, and there is no way to tell from here how much it
            // left behind — so it reports as an incomplete flush rather than as
            // a clean one.
            flush_complete: self.arena_drained,
            // Attached by the terminal writer, which reads the arena after this
            // worker has drained it. The exporter never sees the producer.
            admission: None,
        }
    }

    fn report_failures(&self) {
        for (name, signal) in [
            ("logs", &self.logs),
            ("metrics", &self.metrics),
            ("traces", &self.traces),
        ] {
            if let Some(line) = failure_line(name, signal) {
                eprintln!("{line}");
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

/// What an operator should look at, for the kinds that do not name it.
///
/// `Tls` is reported when a peer on an `https://` origin never completed a
/// handshake. Which error kind that produces is a property of the host, and on
/// some hosts it is the same kind a connection dropped in front of a healthy
/// collector produces. Both survive the retry budget identically, so the line
/// names both rather than sending someone to inspect a certificate that may
/// never have been involved.
fn failure_hint(kind: OtlpDeliveryFailureKind) -> &'static str {
    match kind {
        OtlpDeliveryFailureKind::Tls => {
            " (no TLS handshake completed: either the collector is not an HTTPS endpoint, or connections to it are being dropped)"
        }
        _ => "",
    }
}

/// The one stderr line an operator gets for a signal that lost chunks, or
/// `None` when it lost none.
///
/// The count matters as much as the reason: nine rejected chunks out of ten is
/// not a healthy export, and naming only the most recent failure would read
/// like an isolated one. Delivery outcomes are observations; this never gates
/// the run.
fn failure_line(name: &str, signal: &SignalDeliveryReport) -> Option<String> {
    let failures = signal.summary.failures;
    match signal.last_failure.as_ref()? {
        DeliveryFailureCause::Transport(error) => Some(format!(
            "clinker: optional OTLP {name} delivery outcome: kind={:?} attempts={} failures={failures}{}",
            error.kind(),
            error.attempts(),
            failure_hint(error.kind())
        )),
        DeliveryFailureCause::Unencodable => Some(format!(
            "clinker: optional OTLP {name} delivery outcome: kind=Unencodable failures={failures}"
        )),
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
    /// A test double took the payload; no transport produced an outcome.
    #[cfg(test)]
    Recorded {
        item_count: u64,
    },
}

impl DeliveryResult {
    /// Items the collector holds, whether or not it said so readably.
    ///
    /// A failure that reached the collector answered 200 before its reply
    /// became unreadable, so the batch was ingested. Counting it as lost would
    /// report a healthy export as a lossy one to anyone comparing this against
    /// the run's record counts.
    fn accepted(&self, item_count: u64) -> u64 {
        match self {
            Self::Typed(Ok(outcome)) => outcome.accepted(),
            Self::Typed(Err(error)) if error.reached_collector() => item_count,
            #[cfg(debug_assertions)]
            Self::Injected { accepted, .. } => *accepted,
            #[cfg(test)]
            Self::Recorded { item_count } => *item_count,
            Self::Typed(Err(_)) | Self::EncodingFailure => 0,
        }
    }

    /// A chunk travels whole, so a failed or unencodable one loses every item
    /// in it. Counting one loss per failed request instead left a supervisor
    /// reconciling accepted plus rejected against the run's record counts with
    /// thousands of records unaccounted for, and reading that shortfall as an
    /// export that had nothing to report.
    fn rejected(&self, item_count: u64) -> u64 {
        match self {
            Self::Typed(Ok(outcome)) => outcome.rejected(),
            Self::Typed(Err(error)) if error.reached_collector() => 0,
            Self::Typed(Err(_)) | Self::EncodingFailure => item_count,
            #[cfg(debug_assertions)]
            Self::Injected { rejected, .. } => *rejected,
            #[cfg(test)]
            Self::Recorded { .. } => 0,
        }
    }

    fn attempts(&self) -> u32 {
        match self {
            Self::Typed(Ok(outcome)) => outcome.attempts(),
            Self::Typed(Err(error)) => error.attempts(),
            #[cfg(debug_assertions)]
            Self::Injected { attempts, .. } => *attempts,
            #[cfg(test)]
            Self::Recorded { .. } => 1,
            Self::EncodingFailure => 0,
        }
    }

    fn failed(&self) -> bool {
        match self {
            Self::Typed(Err(_)) | Self::EncodingFailure => true,
            Self::Typed(Ok(_)) => false,
            #[cfg(debug_assertions)]
            Self::Injected { failed, .. } => *failed,
            #[cfg(test)]
            Self::Recorded { .. } => false,
        }
    }

    #[cfg(debug_assertions)]
    fn injected_outcome(&self) -> Option<&'static str> {
        match self {
            Self::Injected { outcome, .. } => *outcome,
            #[cfg(test)]
            Self::Recorded { .. } => None,
            Self::Typed(_) | Self::EncodingFailure => None,
        }
    }
}

enum WorkerCommand {
    /// Boxed so the two commands are the same size on the channel: a snapshot
    /// is two orders of magnitude larger than a drain request, and every send
    /// would otherwise pay for the larger one.
    Finish(Box<RunLifecycleSnapshot>),
    /// Deliver what is buffered and stop, with no lifecycle terminal.
    ///
    /// The run never reached its terminal, so there is no snapshot to describe
    /// one — but the records already produced still describe why.
    Drain,
}

/// Hold the final flush past its deadline, standing in for a collector slower
/// than `flush_timeout_ms`.
///
/// The real condition needs a collector that accepts a connection and then
/// does not answer, which no offline test has. Holding the worker here reaches
/// the same branch — the flush deadline expires, `finish` detaches the worker,
/// and the arena is read while that worker is still draining it.
#[cfg(debug_assertions)]
fn injected_flush_hold() {
    let Some(millis) = std::env::var_os("CLINKER_TEST_OTLP_FLUSH_HOLD_MS") else {
        return;
    };
    let Ok(millis) = millis.to_string_lossy().parse::<u64>() else {
        return;
    };
    thread::sleep(Duration::from_millis(millis));
}

/// One finite run's sole telemetry receiver and blocking exporter worker.
pub(crate) struct OtlpWorker {
    command: mpsc::SyncSender<WorkerCommand>,
    done: mpsc::Receiver<OtlpDeliveryReport>,
    handle: Option<thread::JoinHandle<()>>,
    stop: Arc<AtomicBool>,
    flush_timeout: Duration,
    /// The worker's counters as of its last delivery, readable without the
    /// completion channel.
    progress: Arc<Mutex<ObservabilitySummary>>,
}

impl OtlpWorker {
    pub(crate) fn start(
        bundle: OtlpRuntimeBundle,
        receiver: TelemetryReceiver,
        shutdown: ShutdownToken,
        correlation: RunCorrelation<String>,
    ) -> Result<Self, ObservabilityRuntimeError> {
        #[cfg(debug_assertions)]
        if std::env::var_os("CLINKER_TEST_OTLP_WORKER_START_FAILURE").as_deref()
            == Some(std::ffi::OsStr::new("1"))
        {
            return Err(ObservabilityRuntimeError::Worker);
        }
        let backend = DeliveryBackend::new()?;
        Self::start_with_backend(bundle, receiver, shutdown, correlation, backend)
    }

    fn start_with_backend(
        bundle: OtlpRuntimeBundle,
        receiver: TelemetryReceiver,
        shutdown: ShutdownToken,
        correlation: RunCorrelation<String>,
        backend: DeliveryBackend,
    ) -> Result<Self, ObservabilityRuntimeError> {
        let mut payload = BoundedPayload::new(bundle.arena.request_capacity_bytes())?;
        let (command, commands) = mpsc::sync_channel(1);
        let (done_sender, done) = mpsc::sync_channel(1);
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let progress = Arc::new(Mutex::new(ObservabilitySummary::default()));
        let worker_progress = Arc::clone(&progress);
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
                    final_flush: false,
                    report: OtlpDeliveryReport::default(),
                    progress: worker_progress,
                    correlation,
                    trace_id: new_trace_id(),
                    next_span_id: RUN_SPAN_ID.saturating_add(1),
                    metrics_window_start_unix_nanos: unix_nanos_now(),
                };
                loop {
                    state.drain_available();
                    match commands.recv_timeout(IDLE_POLL) {
                        Ok(WorkerCommand::Finish(snapshot)) => {
                            #[cfg(debug_assertions)]
                            injected_flush_hold();
                            state.enter_final_flush();
                            state.drain_final();
                            state.deliver_lifecycle(snapshot.as_ref());
                            let _ = done_sender.try_send(state.report);
                            return;
                        }
                        Ok(WorkerCommand::Drain) => {
                            state.enter_final_flush();
                            state.drain_final();
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
            progress,
        })
    }

    /// A handle to the counters this worker keeps as it delivers.
    ///
    /// Handed to the machine emitter so a terminal written on a path that
    /// never reaches the explicit flush still reports what was delivered,
    /// rather than omitting the field entirely on exactly the early failures.
    pub(crate) fn progress_handle(&self) -> Arc<Mutex<ObservabilitySummary>> {
        Arc::clone(&self.progress)
    }

    pub(crate) fn finish(mut self, snapshot: RunLifecycleSnapshot) -> ObservabilitySummary {
        if self
            .command
            .send(WorkerCommand::Finish(Box::new(snapshot)))
            .is_err()
        {
            // The worker is gone, so it will report nothing further — but what
            // it delivered before it went is in the mirror, and the default
            // summary would claim a run whose collector holds thousands of
            // records delivered none. Same rule as the timeout branch below:
            // report what was observed, and say the flush did not complete.
            let progress = *self
                .progress
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            return ObservabilitySummary {
                flush_complete: false,
                ..progress
            };
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
                // Report what the worker actually recorded. Inventing a summary
                // here told a supervisor that a run which delivered thousands of
                // records delivered none, which is the one direction the number
                // must not be wrong in: it turns a slow flush into an apparent
                // collector outage.
                let progress = *self
                    .progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                ObservabilitySummary {
                    flush_complete: false,
                    ..progress
                }
            }
        }
    }
}

impl Drop for OtlpWorker {
    fn drop(&mut self) {
        // The worker is constructed before every startup step that can fail,
        // and the flush is only reached after execution — so a run that fails
        // during discovery, staging, or sink creation dropped everything it
        // had already buffered, which is precisely the telemetry describing
        // that failure. Deliver it instead.
        //
        // `finish` takes the handle before returning on its own deadline, so a
        // flush that already timed out is not joined again here: the optional
        // path still cannot extend the authoritative run.
        let Some(handle) = self.handle.take() else {
            self.stop.store(true, Ordering::Release);
            return;
        };
        // `stop` is the signal every in-flight delivery checks before opening
        // a connection, so raising it alongside the drain request made each of
        // those deliveries short-circuit and the drain export nothing at all.
        // The request is itself the instruction to stop; `stop` is held back
        // until the worker has either finished or run past the deadline.
        if self.command.try_send(WorkerCommand::Drain).is_err() {
            self.stop.store(true, Ordering::Release);
            let _ = handle.join();
            return;
        }
        match self.done.recv_timeout(self.flush_timeout) {
            Ok(report) => {
                let _ = handle.join();
                report.report_failures();
            }
            Err(_) => {
                self.stop.store(true, Ordering::Release);
                // Same bound as the flush that timed out: a still-active
                // transport call remains capped by the admitted retry-total
                // deadline, and abandoning the handle keeps the optional path
                // from extending the authoritative run.
                drop(handle);
            }
        }
    }
}

struct WorkerState<'a> {
    bundle: OtlpRuntimeBundle,
    receiver: TelemetryReceiver,
    backend: DeliveryBackend,
    payload: &'a mut BoundedPayload,
    shutdown: ShutdownToken,
    stop: Arc<AtomicBool>,
    /// Whether the worker is delivering its final flush rather than exporting
    /// alongside a still-running execution.
    final_flush: bool,
    report: OtlpDeliveryReport,
    /// The counters so far, readable by the parent.
    ///
    /// The full report only crosses the completion channel on a clean
    /// shutdown, so without this a parent whose flush deadline expired has no
    /// account of what was delivered and can only invent one.
    progress: Arc<Mutex<ObservabilitySummary>>,
    /// The run identity every exported envelope names as its producer.
    ///
    /// Supplied when the worker starts rather than read off the records it
    /// drains. Metrics and spans are produced for every transform, while log
    /// records exist only where the author wrote a `log:` directive — taking
    /// the identity from the records would leave a pipeline that declares no
    /// log directives exporting all of its telemetry under the service name
    /// alone, joinable to nothing.
    correlation: RunCorrelation<String>,
    /// One run is one trace; every exported span carries this id.
    trace_id: String,
    next_span_id: u64,
    /// Start of the delta window the next drained counters describe.
    metrics_window_start_unix_nanos: u64,
}

impl WorkerState<'_> {
    /// Stop reading the run's shutdown token as an instruction to abandon.
    ///
    /// The token is raised for the whole run the moment a SIGINT or SIGTERM
    /// arrives, so leaving it in the abandon predicate here made every
    /// delivery of the final flush return before opening a connection: a
    /// cancelled run exported nothing at all, terminal span included, which is
    /// exactly the run whose telemetry is wanted. The flush command is itself
    /// the instruction to wind up; from here only `stop` — raised by the
    /// parent when its flush deadline expires — abandons a delivery, which is
    /// what keeps the flush bounded.
    fn enter_final_flush(&mut self) {
        self.final_flush = true;
    }

    /// Deliver whatever the arena will give up right now.
    ///
    /// Refusals are ordinary here: a producer holding the arena on an idle poll
    /// means the next poll, two milliseconds later, takes what this one left.
    fn drain_available(&mut self) {
        while let DrainOutcome::Batch(batch) = self.receiver.drain() {
            self.deliver_batch(&batch);
        }
    }

    /// Deliver everything the arena holds, and record whether it was emptied.
    ///
    /// A refused drain took nothing and says nothing about what is still in
    /// there, so the final flush retries rather than concluding from it. By
    /// this point execution has ended and the producers are quiescent, so a
    /// refusal is a straggler admission finishing — the length of one
    /// serialization. What the attempts bound is a producer that never lets go,
    /// which is reported as a flush that did not complete instead of as one
    /// that did.
    fn drain_final(&mut self) {
        let mut refusals = 0_u32;
        loop {
            match self.receiver.drain() {
                DrainOutcome::Batch(batch) => self.deliver_batch(&batch),
                DrainOutcome::Empty => return,
                DrainOutcome::Contended => {
                    refusals += 1;
                    if refusals > FINAL_DRAIN_REFUSALS {
                        self.report.arena_drained = false;
                        return;
                    }
                    thread::yield_now();
                }
            }
        }
    }

    fn deliver_batch(&mut self, batch: &TelemetryBatch) {
        let correlation = self.correlation.clone();
        // One drain is one observation instant: these records left the arena
        // together, whenever each of them was authored.
        let observed_at = unix_nanos_now().to_string();
        self.deliver_chunks(OtlpSignal::Logs, batch.logs(), |payload, _offset, chunk| {
            payload.encode(&logs_envelope(chunk, &observed_at, &correlation))
        });
        if !batch.metrics().is_empty() {
            let window = self.take_metrics_window();
            self.deliver_chunks(
                OtlpSignal::Metrics,
                batch.metrics(),
                |payload, _offset, chunk| {
                    payload.encode(&metrics_envelope(chunk, window, &correlation))
                },
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
                    payload.encode(&traces_envelope(chunk, &trace_id, first, &correlation))
                },
            );
        }
    }

    fn deliver_lifecycle(&mut self, snapshot: &RunLifecycleSnapshot) {
        let envelope = lifecycle_envelope(snapshot, &self.trace_id, &self.correlation);
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
                &|| {
                    (!self.final_flush && self.shutdown.is_requested())
                        || self.stop.load(Ordering::Acquire)
                },
            ),
            Err(_) => DeliveryResult::EncodingFailure,
        };
        self.report.record(signal, result, item_count);
        self.publish_progress();
    }

    /// Mirror the running counters where the parent can read them.
    fn publish_progress(&self) {
        let mut progress = self
            .progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *progress = ObservabilitySummary {
            flush_complete: false,
            ..self.report.summary()
        };
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
    #[cfg(test)]
    Recording(RecordingDelivery),
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
        #[cfg(not(any(debug_assertions, test)))]
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
            #[cfg(test)]
            Self::Recording(recording) => recording.deliver(signal, item_count, shutdown),
        }
    }
}

/// Records what the worker handed the transport, and whether the worker would
/// have abandoned it.
///
/// `send_otlp_json` returns before opening a connection when the abandon
/// predicate holds, so a recorded `abandoned` is a delivery that reached no
/// collector.
#[cfg(test)]
struct RecordingDelivery {
    deliveries: Arc<Mutex<Vec<RecordedDelivery>>>,
    started: mpsc::Sender<()>,
    /// How long the first delivery occupies the worker. It gives a test a
    /// window in which the worker is demonstrably busy, so telemetry produced
    /// during it is still buffered when the worker is asked to stop.
    hold: Duration,
    held: bool,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RecordedDelivery {
    signal: OtlpSignal,
    item_count: u64,
    abandoned: bool,
}

#[cfg(test)]
impl RecordingDelivery {
    fn deliver(
        &mut self,
        signal: OtlpSignal,
        item_count: u64,
        shutdown: &dyn Fn() -> bool,
    ) -> DeliveryResult {
        let recorded = RecordedDelivery {
            signal,
            item_count,
            abandoned: shutdown(),
        };
        self.deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(recorded);
        let _ = self.started.send(());
        if !self.held {
            self.held = true;
            thread::sleep(self.hold);
        }
        DeliveryResult::Recorded { item_count }
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

/// The producer identity every exported envelope carries.
///
/// OTLP identifies whatever produced a signal by its resource, and a collector
/// given none files everything it receives under one anonymous producer — so
/// two pipelines reporting to the same collector merged into a single series
/// with no way to tell which run a point came from.
#[derive(Serialize)]
struct Resource<'a> {
    attributes: Vec<KeyValue<'a>>,
}

/// The exporting service as a collector sees it. One run is one process of one
/// engine, so the run correlation rather than the service name is what
/// separates two pipelines reporting to the same collector.
const SERVICE_NAME: &str = "clinker";

fn resource(correlation: &RunCorrelation<String>) -> Resource<'_> {
    let mut attributes = vec![KeyValue::static_string("service.name", SERVICE_NAME)];
    attributes.extend(correlation_attributes(correlation));
    Resource { attributes }
}

/// Engine-supplied run identity, in the spelling the log records already use.
fn correlation_attributes(correlation: &RunCorrelation<String>) -> [KeyValue<'_>; 3] {
    [
        KeyValue::string("clinker.execution_id", &correlation.execution_id),
        KeyValue::string("clinker.batch_id", &correlation.batch_id),
        KeyValue::string("clinker.pipeline_name", &correlation.pipeline_name),
    ]
}

#[derive(Serialize)]
struct LogsEnvelope<'a> {
    #[serde(rename = "resourceLogs")]
    resource_logs: [ResourceLogs<'a>; 1],
}

#[derive(Serialize)]
struct ResourceLogs<'a> {
    resource: Resource<'a>,
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
    /// When the exporter drained this record from the arena.
    ///
    /// A record carrying no time at all is one a collector stamps with its own
    /// receive time, which places an authored event after every span it
    /// actually sits between — the spans alongside it do carry their
    /// boundaries. `timeUnixNano`, the time the event itself occurred, stays
    /// absent because the producer does not retain it; a collector reads this
    /// field in its place.
    #[serde(rename = "observedTimeUnixNano")]
    observed_time_unix_nano: &'a str,
    #[serde(rename = "severityText")]
    severity_text: &'static str,
    body: StringValue<'a>,
    attributes: Vec<KeyValue<'a>>,
}

fn logs_envelope<'a>(
    logs: &'a [LogRecord],
    observed_at_unix_nanos: &'a str,
    correlation: &'a RunCorrelation<String>,
) -> LogsEnvelope<'a> {
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
                observed_time_unix_nano: observed_at_unix_nanos,
                severity_text: severity_name(record.severity),
                body: StringValue::new(&record.message),
                attributes,
            }
        })
        .collect();
    LogsEnvelope {
        resource_logs: [ResourceLogs {
            resource: resource(correlation),
            scope_logs: [ScopeLogs { log_records }],
        }],
    }
}

#[derive(Serialize)]
struct MetricsEnvelope<'a> {
    #[serde(rename = "resourceMetrics")]
    resource_metrics: [ResourceMetrics<'a>; 1],
}

#[derive(Serialize)]
struct ResourceMetrics<'a> {
    resource: Resource<'a>,
    #[serde(rename = "scopeMetrics")]
    scope_metrics: [ScopeMetrics<'a>; 1],
}

#[derive(Serialize)]
struct ScopeMetrics<'a> {
    metrics: Vec<OtlpMetric<'a>>,
}

#[derive(Serialize)]
struct OtlpMetric<'a> {
    name: &'static str,
    unit: &'static str,
    sum: Sum<'a>,
}

#[derive(Serialize)]
struct Sum<'a> {
    #[serde(rename = "dataPoints")]
    data_points: [NumberDataPoint<'a>; 1],
    #[serde(rename = "aggregationTemporality")]
    aggregation_temporality: u8,
    #[serde(rename = "isMonotonic")]
    is_monotonic: bool,
}

#[derive(Serialize)]
struct NumberDataPoint<'a> {
    /// The run this point counts, in the same spelling a log record uses.
    ///
    /// A backend that keys a series on point attributes alone — resource
    /// attributes are routinely flattened away in transit — otherwise sums two
    /// pipelines' counters into one anonymous series.
    attributes: Vec<KeyValue<'a>>,
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
fn metrics_envelope<'a>(
    metrics: &[MetricPoint],
    window: MetricsWindow,
    correlation: &'a RunCorrelation<String>,
) -> MetricsEnvelope<'a> {
    MetricsEnvelope {
        resource_metrics: [ResourceMetrics {
            resource: resource(correlation),
            scope_metrics: [ScopeMetrics {
                metrics: metrics
                    .iter()
                    .map(|point| OtlpMetric {
                        name: metric_name(point.key),
                        unit: "1",
                        sum: Sum {
                            data_points: [NumberDataPoint {
                                attributes: Vec::from(correlation_attributes(correlation)),
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
    resource: Resource<'a>,
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
    correlation: &'a RunCorrelation<String>,
) -> TracesEnvelope<'a> {
    TracesEnvelope {
        resource_spans: [ResourceSpans {
            resource: resource(correlation),
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
    correlation: &'a RunCorrelation<String>,
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
    // Only counts that were actually observed. A run that failed before any
    // were taken has no throughput to report, and exporting zeros would tell a
    // volume alert that the batch processed nothing rather than that nothing
    // was measured — the same misreport the lineage terminal omits its
    // run-statistics facet to avoid.
    if let Some(counts) = terminal.and_then(|facts| facts.measured_counts()) {
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
            resource: resource(correlation),
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
        MetricKey::CredentialResolveStarted => "clinker.credential.resolve.started",
        MetricKey::CredentialResolveCompleted => "clinker.credential.resolve.completed",
        MetricKey::CredentialResolveFailed => "clinker.credential.resolve.failed",
        MetricKey::CredentialResolveInterrupted => "clinker.credential.resolve.interrupted",
        MetricKey::ResourceOpenStarted => "clinker.resource.open.started",
        MetricKey::ResourceOpenCompleted => "clinker.resource.open.completed",
        MetricKey::ResourceOpenFailed => "clinker.resource.open.failed",
        MetricKey::ResourceOpenInterrupted => "clinker.resource.open.interrupted",
        MetricKey::CredentialRenewStarted => "clinker.credential.renew.started",
        MetricKey::CredentialRenewCompleted => "clinker.credential.renew.completed",
        MetricKey::CredentialRenewFailed => "clinker.credential.renew.failed",
        MetricKey::CredentialRenewInterrupted => "clinker.credential.renew.interrupted",
        MetricKey::CredentialRevokeStarted => "clinker.credential.revoke.started",
        MetricKey::CredentialRevokeCompleted => "clinker.credential.revoke.completed",
        MetricKey::CredentialRevokeFailed => "clinker.credential.revoke.failed",
        MetricKey::CredentialRevokeInterrupted => "clinker.credential.revoke.interrupted",
        MetricKey::SourceStarted => "clinker.source.started",
        MetricKey::SourceCompleted => "clinker.source.completed",
        MetricKey::SourceFailed => "clinker.source.failed",
        MetricKey::SourceInterrupted => "clinker.source.interrupted",
        MetricKey::GuessStarted => "clinker.guess.started",
        MetricKey::GuessCompleted => "clinker.guess.completed",
        MetricKey::GuessUnresolved => "clinker.guess.unresolved",
        MetricKey::GuessFailed => "clinker.guess.failed",
        MetricKey::GuessInterrupted => "clinker.guess.interrupted",
        MetricKey::SinkStarted => "clinker.sink.started",
        MetricKey::SinkCompleted => "clinker.sink.completed",
        MetricKey::SinkRecords => "clinker.sink.records",
        MetricKey::SinkErrors => "clinker.sink.errors",
    }
}

fn span_name(name: SpanName) -> &'static str {
    match name {
        SpanName::Transform => "clinker.transform",
        SpanName::CredentialResolve => "clinker.credential.resolve",
        SpanName::ResourceOpen => "clinker.resource.open",
        SpanName::CredentialRenew => "clinker.credential.renew",
        SpanName::CredentialRevoke => "clinker.credential.revoke",
        SpanName::Source => "clinker.source",
        SpanName::Guess => "clinker.guess",
        SpanName::Sink => "clinker.sink",
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

    use clinker_exec::telemetry::LogEvent;
    use clinker_plan::config::ClinkerToml;
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

    /// An arena that lost nothing, kept every field it was asked for, and
    /// never faulted under its own guard.
    fn snapshot_of_a_clean_run() -> ArenaSnapshot {
        ArenaSnapshot {
            missing_field_drops: 0,
            arena_recoveries: 0,
            ..snapshot_reporting_a_miss_and_a_recovery()
        }
    }

    /// An arena reporting one missed field and one recovered panic, and
    /// nothing else. Every other counter is zero so a mapping that routed
    /// either value into the wrong group is visible as a non-zero it cannot
    /// explain.
    fn snapshot_reporting_a_miss_and_a_recovery() -> ArenaSnapshot {
        ArenaSnapshot {
            owned_bytes: 64_000,
            ordinary_capacity_bytes: 32_000,
            high_capacity_bytes: 32_000,
            retained_bytes: 0,
            ordinary_retained_bytes: 0,
            high_retained_bytes: 0,
            peak_retained_bytes: 0,
            accepted: 7,
            denied_fields: 0,
            truncated_fields: 0,
            attribute_limit_drops: 0,
            sampled_drops: 0,
            ordinary_sampled_drops: 0,
            high_sampled_drops: 0,
            rate_limited_drops: 0,
            contention_drops: 0,
            full_drops: 0,
            ordinary_full_drops: 0,
            high_full_drops: 0,
            oversize_drops: 0,
            invalid_drops: 0,
            undecodable_drops: 0,
            missing_field_drops: 3,
            arena_recoveries: 1,
        }
    }

    /// A missed field is not a lost signal, and a recovered panic is not a
    /// count of anything lost.
    ///
    /// Both used to be produced by the arena and read by nobody. Adding either
    /// into `dropped` would have made a run that discarded no telemetry report
    /// that it had, which is the mistake `dropped_total` exists to prevent an
    /// operator from making by hand.
    #[test]
    fn a_missed_field_and_an_arena_recovery_are_reported_outside_the_drop_total() {
        let summary =
            AdmissionSummary::from_arena(snapshot_reporting_a_miss_and_a_recovery(), true);
        assert_eq!(summary.fields.missing, 3);
        assert_eq!(summary.arena_recoveries, 1);
        assert_eq!(
            summary.dropped_total(),
            0,
            "this run lost no signal at admission: {summary:#?}"
        );

        let encoded = serde_json::to_value(summary).expect("the summary serializes");
        assert_eq!(encoded["fields"]["missing"], 3);
        assert_eq!(encoded["arena_recoveries"], 1);
        assert!(
            encoded["dropped"]
                .as_object()
                .expect("drop reasons")
                .values()
                .all(|count| count == 0),
            "neither counter may appear under a drop reason: {encoded:#?}"
        );
    }

    /// The standard-error line is suppressed exactly when the accounting has
    /// nothing to say, and each thing it can say breaks the silence alone.
    ///
    /// Checked against a snapshot rather than against a run, because a run is
    /// not a witness to the silent case: a producer that meets the drain
    /// thread at the arena lock is refused and credited to `contended`. That
    /// is a real drop, correctly reported, and the drain holds that lock for a
    /// shorter time than it used to rather than for no time at all — so a run
    /// asserted to print nothing is asserting that a collision did not happen,
    /// which is a property of the schedule and not of the rule.
    #[test]
    fn an_accounting_with_nothing_to_report_is_suppressed_and_any_loss_breaks_it() {
        assert_eq!(
            AdmissionSummary::from_arena(snapshot_of_a_clean_run(), true).standard_error_line(),
            None,
            "a final count of a run that lost nothing has nothing to report"
        );

        /// A counter that must keep the line, paired with the text its
        /// presence must produce.
        type Breaker = (&'static str, fn(&mut ArenaSnapshot));

        let breakers: [Breaker; 9] = [
            ("sampled=1", |snapshot| snapshot.sampled_drops = 1),
            ("rate_limited=1", |snapshot| snapshot.rate_limited_drops = 1),
            ("queue_full=1", |snapshot| snapshot.full_drops = 1),
            ("contended=1", |snapshot| snapshot.contention_drops = 1),
            ("oversize=1", |snapshot| snapshot.oversize_drops = 1),
            ("invalid_identity=1", |snapshot| snapshot.invalid_drops = 1),
            ("undecodable=1", |snapshot| snapshot.undecodable_drops = 1),
            ("missing_fields=1", |snapshot| {
                snapshot.missing_field_drops = 1;
            }),
            ("arena_recoveries=1", |snapshot| {
                snapshot.arena_recoveries = 1;
            }),
        ];
        for (reported, break_the_silence) in breakers {
            let mut snapshot = snapshot_of_a_clean_run();
            break_the_silence(&mut snapshot);
            let line = AdmissionSummary::from_arena(snapshot, true)
                .standard_error_line()
                .unwrap_or_else(|| panic!("a run reporting {reported} is not a silent one"));
            assert!(
                line.contains(reported),
                "the line must name what broke the silence: {line}"
            );
        }

        let unfinished = AdmissionSummary::from_arena(snapshot_of_a_clean_run(), false)
            .standard_error_line()
            .expect("counters that are not final are reported however they read");
        assert!(
            unfinished.contains("dropped=0") && unfinished.contains("counts_complete=false"),
            "all-zero counters read from an arena still being drained are not \
             evidence of a clean run, and the line says which of the two it \
             is: {unfinished}"
        );
    }

    /// Field policy doing what an operator configured it to do is not a loss,
    /// and reporting it would put a line on every run of a pipeline that denies
    /// or truncates a field by design.
    #[test]
    fn configured_field_policy_leaves_the_admission_line_suppressed() {
        let mut snapshot = snapshot_of_a_clean_run();
        snapshot.denied_fields = 4;
        snapshot.truncated_fields = 2;
        snapshot.attribute_limit_drops = 1;
        assert_eq!(
            AdmissionSummary::from_arena(snapshot, true).standard_error_line(),
            None,
            "denied, truncated and attribute-limited fields are policy, not loss"
        );
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

    #[test]
    fn lifecycle_metric_and_span_wire_names_are_stable() {
        let metric_names = [
            (MetricKey::TransformStarted, "clinker.transform.started"),
            (MetricKey::TransformCompleted, "clinker.transform.completed"),
            (MetricKey::TransformRecords, "clinker.transform.records"),
            (MetricKey::TransformErrors, "clinker.transform.errors"),
            (
                MetricKey::CredentialResolveStarted,
                "clinker.credential.resolve.started",
            ),
            (
                MetricKey::CredentialResolveCompleted,
                "clinker.credential.resolve.completed",
            ),
            (
                MetricKey::CredentialResolveFailed,
                "clinker.credential.resolve.failed",
            ),
            (
                MetricKey::CredentialResolveInterrupted,
                "clinker.credential.resolve.interrupted",
            ),
            (
                MetricKey::ResourceOpenStarted,
                "clinker.resource.open.started",
            ),
            (
                MetricKey::ResourceOpenCompleted,
                "clinker.resource.open.completed",
            ),
            (
                MetricKey::ResourceOpenFailed,
                "clinker.resource.open.failed",
            ),
            (
                MetricKey::ResourceOpenInterrupted,
                "clinker.resource.open.interrupted",
            ),
            (
                MetricKey::CredentialRenewStarted,
                "clinker.credential.renew.started",
            ),
            (
                MetricKey::CredentialRenewCompleted,
                "clinker.credential.renew.completed",
            ),
            (
                MetricKey::CredentialRenewFailed,
                "clinker.credential.renew.failed",
            ),
            (
                MetricKey::CredentialRenewInterrupted,
                "clinker.credential.renew.interrupted",
            ),
            (
                MetricKey::CredentialRevokeStarted,
                "clinker.credential.revoke.started",
            ),
            (
                MetricKey::CredentialRevokeCompleted,
                "clinker.credential.revoke.completed",
            ),
            (
                MetricKey::CredentialRevokeFailed,
                "clinker.credential.revoke.failed",
            ),
            (
                MetricKey::CredentialRevokeInterrupted,
                "clinker.credential.revoke.interrupted",
            ),
            (MetricKey::SourceStarted, "clinker.source.started"),
            (MetricKey::SourceCompleted, "clinker.source.completed"),
            (MetricKey::SourceFailed, "clinker.source.failed"),
            (MetricKey::SourceInterrupted, "clinker.source.interrupted"),
            (MetricKey::GuessStarted, "clinker.guess.started"),
            (MetricKey::GuessCompleted, "clinker.guess.completed"),
            (MetricKey::GuessUnresolved, "clinker.guess.unresolved"),
            (MetricKey::GuessFailed, "clinker.guess.failed"),
            (MetricKey::GuessInterrupted, "clinker.guess.interrupted"),
        ];
        assert_eq!(metric_names.len(), MetricKey::COUNT);
        for (key, expected) in metric_names {
            assert_eq!(metric_name(key), expected, "stable name for {key:?}");
        }

        let span_names = [
            (SpanName::Transform, "clinker.transform"),
            (SpanName::CredentialResolve, "clinker.credential.resolve"),
            (SpanName::ResourceOpen, "clinker.resource.open"),
            (SpanName::CredentialRenew, "clinker.credential.renew"),
            (SpanName::CredentialRevoke, "clinker.credential.revoke"),
            (SpanName::Source, "clinker.source"),
            (SpanName::Guess, "clinker.guess"),
        ];
        for (name, expected) in span_names {
            assert_eq!(span_name(name), expected, "stable name for {name:?}");
        }
    }

    fn spans_of(envelope: &Value) -> &Vec<Value> {
        envelope["resourceSpans"][0]["scopeSpans"][0]["spans"]
            .as_array()
            .expect("spans")
    }

    #[test]
    fn closed_metric_and_span_inventories_have_one_stable_export_name_each() {
        assert_eq!(
            MetricKey::ALL.map(metric_name),
            [
                "clinker.transform.started",
                "clinker.transform.completed",
                "clinker.transform.records",
                "clinker.transform.errors",
                "clinker.credential.resolve.started",
                "clinker.credential.resolve.completed",
                "clinker.credential.resolve.failed",
                "clinker.credential.resolve.interrupted",
                "clinker.resource.open.started",
                "clinker.resource.open.completed",
                "clinker.resource.open.failed",
                "clinker.resource.open.interrupted",
                "clinker.credential.renew.started",
                "clinker.credential.renew.completed",
                "clinker.credential.renew.failed",
                "clinker.credential.renew.interrupted",
                "clinker.credential.revoke.started",
                "clinker.credential.revoke.completed",
                "clinker.credential.revoke.failed",
                "clinker.credential.revoke.interrupted",
                "clinker.source.started",
                "clinker.source.completed",
                "clinker.source.failed",
                "clinker.source.interrupted",
                "clinker.guess.started",
                "clinker.guess.completed",
                "clinker.guess.unresolved",
                "clinker.guess.failed",
                "clinker.guess.interrupted",
                "clinker.sink.started",
                "clinker.sink.completed",
                "clinker.sink.records",
                "clinker.sink.errors",
            ]
        );
        assert_eq!(
            SpanName::ALL.map(span_name),
            [
                "clinker.transform",
                "clinker.credential.resolve",
                "clinker.resource.open",
                "clinker.credential.renew",
                "clinker.credential.revoke",
                "clinker.source",
                "clinker.guess",
                "clinker.sink",
            ]
        );
    }

    /// Flatten one OTLP `KeyValue` list into the string attributes it carries.
    fn attributes_of(attributes: &Value) -> BTreeMap<String, String> {
        attributes
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
            .collect()
    }

    fn log_record(message: &str) -> LogRecord {
        LogRecord {
            event: "transform.customer_seen".to_owned(),
            severity: Severity::Info,
            message: message.to_owned(),
            correlation: correlation(),
            fields: BTreeMap::new(),
        }
    }

    /// A policy that both reserves an arena and admits a collector endpoint.
    /// The endpoint never resolves; every test using it substitutes a backend.
    fn exporting_policy() -> ResolvedObservabilityPolicy {
        let text = r#"
[observability]
arena_bytes = "1024KB"
ordinary_lane_bytes = "768KB"
high_severity_lane_bytes = "256KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 4
max_attribute_bytes = "64B"
drop_policy = "drop_newest"
sample_every = 1
rate_limit_per_second = 1000
rate_limit_burst = 1000
flush_timeout_ms = 5000

[observability.otlp]
endpoint = "https://collector.invalid"
connect_timeout_ms = 100
request_timeout_ms = 200
retry_max_attempts = 1
retry_total_timeout_ms = 500
max_response_bytes = "1KB"

[observability.otlp.auth]
mode = "none"
"#;
        ClinkerToml::parse(text)
            .expect("the observability policy parses")
            .resolve_observability(None)
            .expect("the observability policy resolves")
    }

    /// Emit one log, retrying while the arena is merely busy.
    ///
    /// Admission takes the arena with `try_lock` and reports `Contended` rather
    /// than waiting, so a producer racing a drain is a designed outcome, not a
    /// fault. Tests that run a live export worker race it by construction: the
    /// worker loops drain-then-deliver, and an emit landing inside a drain's
    /// lock window is dropped. Asserting on a single attempt makes the test
    /// depend on the scheduler rather than on the behaviour it names.
    ///
    /// Retrying is bounded and only forgives `Contended`. A genuine admission
    /// regression — sampling, capacity, a poisoned arena — is not retried away:
    /// those outcomes fail on the first attempt, and exhausting the budget
    /// fails too, so this cannot turn a broken arena green.
    fn emit_one_log(producer: &TelemetryProducer) {
        const ATTEMPTS: u32 = 200;
        let correlation = correlation();
        let mut last = None;
        for attempt in 0..ATTEMPTS {
            let outcome = producer.emit_log(LogEvent {
                event: "transform.customer_seen",
                severity: Severity::Info,
                message: "customer observed",
                correlation: RunCorrelation {
                    execution_id: &correlation.execution_id,
                    batch_id: &correlation.batch_id,
                    pipeline_name: &correlation.pipeline_name,
                },
                fields: &[],
            });
            if outcome.is_accepted() {
                return;
            }
            assert!(
                matches!(
                    outcome,
                    clinker_exec::telemetry::AdmissionOutcome::Dropped(
                        clinker_exec::telemetry::DropReason::Contended
                    )
                ),
                "the arena admits the fixture: {outcome:?}"
            );
            last = Some(outcome);
            if attempt + 1 < ATTEMPTS {
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        panic!(
            "the arena stayed contended across {ATTEMPTS} attempts, last outcome {:?}",
            last.expect("a rejected attempt records its outcome")
        );
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
        let envelope = to_value(&traces_envelope(&traces, &trace_id, 2, &correlation()));
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
        let envelope = to_value(&traces_envelope(
            &traces,
            &new_trace_id(),
            2,
            &correlation(),
        ));
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
        let envelope = to_value(&metrics_envelope(&metrics, window, &correlation()));
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
        // The largest request this record can produce: the resource block and
        // the observation timestamp travel with it.
        let correlation = correlation();
        let observed_at = "1723065600000000000";

        let mut request = BoundedPayload::new(SHIPPED_BOUNDS.request_capacity_bytes())
            .expect("request buffer reserves");
        request
            .encode(&logs_envelope(batch, observed_at, &correlation))
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
            coupled
                .encode(&logs_envelope(batch, observed_at, &correlation))
                .is_err(),
            "the per-record slot bound is not a per-request bound"
        );
    }

    #[test]
    fn a_batch_too_large_for_one_request_is_split_rather_than_dropped() {
        let records = (0..4)
            .map(|_| slot_filling_log_record(SHIPPED_BOUNDS))
            .collect::<Vec<_>>();
        let correlation = correlation();
        let observed_at = "1723065600000000000";
        let mut whole_batch =
            BoundedPayload::new(SHIPPED_BOUNDS.request_capacity_bytes()).expect("buffer reserves");
        assert!(
            whole_batch
                .encode(&logs_envelope(&records, observed_at, &correlation))
                .is_err(),
            "the fixture must be a batch that cannot travel as one request"
        );

        let mut payload = BoundedPayload::new(SHIPPED_BOUNDS.request_capacity_bytes())
            .expect("request buffer reserves");
        let encode = |payload: &mut BoundedPayload, _offset: usize, chunk: &[LogRecord]| {
            payload.encode(&logs_envelope(chunk, observed_at, &correlation))
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
        let record = log_record("customer observed");
        let envelope = to_value(&logs_envelope(
            std::slice::from_ref(&record),
            "1",
            &correlation(),
        ));
        let attributes = attributes_of(
            &envelope["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0]["attributes"],
        );
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

    #[test]
    fn exported_log_records_carry_the_time_they_left_the_arena() {
        let record = log_record("customer observed");
        let envelope = to_value(&logs_envelope(
            std::slice::from_ref(&record),
            "1723065600000000000",
            &correlation(),
        ));
        let exported = &envelope["resourceLogs"][0]["scopeLogs"][0]["logRecords"][0];
        assert_eq!(
            exported["observedTimeUnixNano"], "1723065600000000000",
            "a record carrying no time at all is stamped with the collector's \
             receive time and cannot be ordered against the run's spans"
        );
    }

    #[test]
    fn every_exported_envelope_names_its_producing_run() {
        let correlation = correlation();
        let record = log_record("customer observed");
        let logs = to_value(&logs_envelope(
            std::slice::from_ref(&record),
            "1",
            &correlation,
        ));
        let points = [MetricPoint {
            key: MetricKey::TransformRecords,
            value: 7,
        }];
        let metrics = to_value(&metrics_envelope(
            &points,
            MetricsWindow { start: 1, end: 2 },
            &correlation,
        ));
        let traces = [span("alpha", 10, 20)];
        let traces = to_value(&traces_envelope(&traces, &new_trace_id(), 2, &correlation));

        for (signal, envelope) in [
            ("logs", &logs["resourceLogs"][0]["resource"]),
            ("metrics", &metrics["resourceMetrics"][0]["resource"]),
            ("traces", &traces["resourceSpans"][0]["resource"]),
        ] {
            let attributes = attributes_of(&envelope["attributes"]);
            assert_eq!(
                attributes.get("service.name").map(String::as_str),
                Some("clinker"),
                "{signal} reaches the collector with no producer identity"
            );
            assert_eq!(
                attributes.get("clinker.pipeline_name").map(String::as_str),
                Some("payments"),
                "{signal} cannot be told apart from another pipeline's"
            );
        }
    }

    #[test]
    fn exported_metric_points_name_the_run_that_produced_them() {
        let points = [MetricPoint {
            key: MetricKey::TransformRecords,
            value: 20_000,
        }];
        let correlation = correlation();
        let envelope = to_value(&metrics_envelope(
            &points,
            MetricsWindow {
                start: 1_000,
                end: 2_000,
            },
            &correlation,
        ));
        let point = &envelope["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0]["sum"]["dataPoints"]
            [0];
        let attributes = attributes_of(&point["attributes"]);
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
            Some("payments"),
            "two pipelines reporting to one collector otherwise share a series"
        );
    }

    #[test]
    fn a_failed_chunk_counts_every_record_it_carried_as_rejected() {
        let mut report = OtlpDeliveryReport::default();
        for _ in 0..3 {
            report.record(OtlpSignal::Logs, DeliveryResult::EncodingFailure, 1_000);
        }

        assert_eq!(
            report.logs.summary.rejected, 3_000,
            "a supervisor reconciles accepted plus rejected against the run's \
             record counts, so a lost chunk owes every record in it"
        );
        assert_eq!(report.logs.summary.accepted, 0);
        assert_eq!(report.logs.summary.failures, 3, "three requests failed");
    }

    #[test]
    fn a_signal_reports_its_losses_even_when_its_last_chunk_succeeded() {
        let mut report = OtlpDeliveryReport::default();
        for _ in 0..9 {
            report.record(OtlpSignal::Logs, DeliveryResult::EncodingFailure, 100);
        }
        report.record(
            OtlpSignal::Logs,
            DeliveryResult::Recorded { item_count: 100 },
            100,
        );

        let line = failure_line("logs", &report.logs)
            .expect("a chunk that never reached the collector is still reported");
        assert!(
            line.contains("failures=9"),
            "the count separates one unlucky request from a failed export: {line}"
        );
        assert!(
            failure_line("metrics", &report.metrics).is_none(),
            "a signal that lost nothing reports nothing"
        );
    }

    /// An aborted run's terminal snapshot, built the way the CLI builds one.
    fn aborted_snapshot() -> RunLifecycleSnapshot {
        let started_at = chrono::DateTime::parse_from_rfc3339("2026-08-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&chrono::Utc);
        let fingerprint = clinker_plan::config::parse_config(
            "pipeline: { name: cancelled }\nnodes:\n  - type: source\n    name: src\n    config: { name: src, type: csv, path: in.csv, schema: [{ name: id, type: int }] }\n",
        )
        .expect("config")
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("plan")
        .semantic_fingerprint()
        .expect("fingerprint");
        let correlation = correlation();
        let identity = crate::lifecycle::RunCorrelationIdentity::new(
            correlation.batch_id.clone(),
            correlation.execution_id.clone(),
        )
        .expect("identity");
        let facts = crate::lifecycle::RunLifecycleFacts::new(identity, fingerprint, started_at);
        facts
            .record_terminal(started_at, RunTerminalOutcome::Abort, None)
            .expect("terminal");
        facts.snapshot()
    }

    #[test]
    fn a_cancelled_run_still_exports_its_final_flush_and_terminal_span() {
        let policy = exporting_policy();
        let bundle = OtlpRuntimeBundle::admit(&policy)
            .expect("the collector endpoint is admissible")
            .expect("the policy configures a collector");
        let (producer, receiver) = bundle.reserve_arena(&policy).expect("the arena reserves");
        let deliveries = Arc::new(Mutex::new(Vec::new()));
        let (started, _delivery_started) = mpsc::channel();
        // Cancelled before the exporter starts, as a run interrupted during
        // startup or early execution is.
        let shutdown = ShutdownToken::detached();
        shutdown.request();
        let worker = OtlpWorker::start_with_backend(
            bundle,
            receiver,
            shutdown,
            correlation(),
            DeliveryBackend::Recording(RecordingDelivery {
                deliveries: Arc::clone(&deliveries),
                started,
                hold: Duration::ZERO,
                held: false,
            }),
        )
        .expect("the exporter starts");

        emit_one_log(&producer);
        let summary = worker.finish(aborted_snapshot());

        let deliveries = deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // The lifecycle span exists only in the final flush, so it is the one
        // delivery whose phase is not a race with the idle drain: a record
        // emitted during execution may legitimately be abandoned by the
        // cancellation, and this test does not assert which phase drained it.
        let lifecycle = deliveries
            .iter()
            .filter(|delivery| delivery.signal == OtlpSignal::Traces)
            .collect::<Vec<_>>();
        assert_eq!(
            lifecycle.len(),
            1,
            "the lifecycle span carrying the abort outcome is exported: \
             {deliveries:?}"
        );
        assert!(
            !lifecycle[0].abandoned,
            "the run's shutdown token is not an instruction to abandon the \
             final flush; reading it as one returns before opening a \
             connection, so a cancelled run exports no terminal at all: \
             {deliveries:?}"
        );
        assert_eq!(
            deliveries
                .iter()
                .filter(|delivery| delivery.signal == OtlpSignal::Logs)
                .map(|delivery| delivery.item_count)
                .sum::<u64>(),
            1,
            "the records describing the cancelled run are exported: \
             {deliveries:?}"
        );
        assert!(
            summary.flush_complete,
            "the flush ran to completion rather than expiring: {summary:?}"
        );
        assert_eq!(
            (
                summary.logs.accepted + summary.traces.accepted,
                summary.logs.rejected + summary.traces.rejected,
                summary.logs.failures + summary.traces.failures,
            ),
            (2, 0, 0),
            "a delivery that was never attempted must not be reported as one \
             the collector rejected: {summary:?}"
        );
    }

    #[test]
    fn a_dropped_worker_delivers_the_telemetry_it_still_holds() {
        let policy = exporting_policy();
        let bundle = OtlpRuntimeBundle::admit(&policy)
            .expect("the collector endpoint is admissible")
            .expect("the policy configures a collector");
        let (producer, receiver) = bundle.reserve_arena(&policy).expect("the arena reserves");
        let deliveries = Arc::new(Mutex::new(Vec::new()));
        let (started, delivery_started) = mpsc::channel();
        let worker = OtlpWorker::start_with_backend(
            bundle,
            receiver,
            ShutdownToken::new(),
            correlation(),
            DeliveryBackend::Recording(RecordingDelivery {
                deliveries: Arc::clone(&deliveries),
                started,
                hold: Duration::from_millis(250),
                held: false,
            }),
        )
        .expect("the exporter starts");

        emit_one_log(&producer);
        delivery_started
            .recv_timeout(Duration::from_secs(10))
            .expect("the exporter delivers the first drained batch");

        // Produced while the exporter is occupied, so it is still buffered when
        // the run gives up and drops the exporter — which is the telemetry
        // describing why the run gave up.
        emit_one_log(&producer);
        drop(worker);

        let deliveries = deliveries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let delivered = deliveries
            .iter()
            .filter(|delivery| delivery.signal == OtlpSignal::Logs)
            .map(|delivery| delivery.item_count)
            .sum::<u64>();
        assert_eq!(
            delivered, 2,
            "the drop-time drain exports what the run had buffered: {deliveries:?}"
        );
        assert!(
            deliveries.iter().all(|delivery| !delivery.abandoned),
            "raising the abandon flag alongside the drain request makes every \
             delivery return before it opens a connection: {deliveries:?}"
        );
    }
}
