//! Sole ownership of the optional machine-run stream.

use std::io::{self, BufWriter, Write};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::Duration;

use clinker_core_types::FailureClassification;
use clinker_exec::output::attempt::{ArtifactKind, ArtifactState, AttemptPublicationOutcome};
use clinker_exec::pipeline::shutdown::ShutdownToken;
use clinker_exec::progress::{BoundedProgress, ProgressSnapshot};

use crate::observability::ObservabilitySummary;
use crate::{MachineFormat, RunArgs};

const MAX_EVENT_BYTES: usize = 16 * 1024;
const MAX_BATCH_ID_BYTES: usize = 256;
const PUBLICATION_ARTIFACTS_PER_EVENT: usize = 64;
/// Wake cadence of the progress worker. `BoundedProgress` throttles what
/// actually reaches the stream; this only bounds how late a due snapshot is.
const PROGRESS_TICK: Duration = Duration::from_millis(20);

/// Resolve the worker's wake cadence.
///
/// Overridable in debug builds because a run that finishes inside one tick
/// emits no periodic observation at all, which leaves the fault-injection
/// tests racing a real timer against how fast the host reads the fixture
/// rather than asserting the behaviour they name.
fn progress_tick() -> Duration {
    #[cfg(debug_assertions)]
    if let Some(value) = std::env::var_os("CLINKER_TEST_MACHINE_PROGRESS_TICK_MS")
        && let Ok(millis) = value.to_string_lossy().parse::<u64>()
    {
        return Duration::from_millis(millis.max(1));
    }
    PROGRESS_TICK
}

/// One ordered serializer and terminal arbiter for a controlled invocation.
#[derive(Clone)]
pub(crate) struct MachineEmitter {
    state: Arc<Mutex<MachineState>>,
    shutdown: ShutdownToken,
}

struct MachineState {
    writer: BufWriter<Box<dyn Write + Send>>,
    batch_id: String,
    execution_id: String,
    sequence: u64,
    plan_identity: serde_json::Value,
    terminal_reserved: bool,
    /// Classification of a `failed` terminal whose encoding did not fit the
    /// event bound. The slot is still free after such a failure, so this is
    /// what the next terminal attempt must report instead of inventing one.
    attempted_failure: Option<FailureClassification>,
    observability: Option<ObservabilitySummary>,
    progress: BoundedProgress,
}

pub(crate) struct MachineProgressWorker {
    stop: mpsc::SyncSender<()>,
    /// `None` once the thread has been joined, by `finish` or by `Drop`.
    handle: Option<thread::JoinHandle<io::Result<()>>>,
}

impl MachineProgressWorker {
    pub(crate) fn finish(mut self) -> io::Result<()> {
        self.stop_and_join().unwrap_or(Ok(()))
    }

    fn stop_and_join(&mut self) -> Option<io::Result<()>> {
        let handle = self.handle.take()?;
        let _ = self.stop.try_send(());
        Some(
            handle
                .join()
                .map_err(|_| io::Error::other("machine progress worker panicked"))
                .and_then(|result| result),
        )
    }
}

impl Drop for MachineProgressWorker {
    fn drop(&mut self) {
        // The worker is started before discovery so a failure to start one is
        // refused up front, which puts many early returns between its start and
        // `finish`. Dropping it only disconnects the channel, so without this
        // the thread outlives the value and can still be holding the emitter
        // when the terminal is written. Joining here bounds it to this scope on
        // every path, not just the one that reaches `finish`.
        //
        // The join waits for a worker that is mid-write, which on a stream
        // nobody is reading is a wait on the reader. That is the same wait the
        // terminal write itself makes, so a stalled supervisor stalls the run
        // either way. A flag suppressing the worker's next emission was tried
        // to narrow that window and withdrawn: it also cancelled snapshots the
        // worker had already become due for, which on a host with coarse timer
        // granularity was every snapshot the run produced. Ordering is the
        // terminal reservation's job, not this one's.
        let _ = self.stop_and_join();
    }
}

impl MachineEmitter {
    pub(crate) fn admit(args: &RunArgs) -> Result<Option<Self>, String> {
        let Some(MachineFormat::NdjsonV1) = args.machine else {
            return Ok(None);
        };
        let batch_id = args
            .batch_id
            .as_deref()
            .filter(|id| !id.trim().is_empty())
            .ok_or_else(|| {
                "--machine ndjson-v1 requires a non-empty caller-supplied --batch-id".to_owned()
            })?;
        if batch_id.len() > MAX_BATCH_ID_BYTES || batch_id.chars().any(char::is_control) {
            return Err(format!(
                "--batch-id must be at most {MAX_BATCH_ID_BYTES} UTF-8 bytes and contain no control characters"
            ));
        }
        if let Some(conflict) = stdout_conflict(args) {
            return Err(format!(
                "machine stdout conflict: --machine ndjson-v1 cannot be combined with {conflict}; remove {conflict} and retry"
            ));
        }
        Ok(Some(Self::with_writer(
            batch_id.to_owned(),
            Box::new(std::io::stdout()),
        )))
    }

    fn with_writer(batch_id: String, writer: Box<dyn Write + Send>) -> Self {
        Self {
            state: Arc::new(Mutex::new(MachineState {
                writer: BufWriter::new(writer),
                batch_id,
                execution_id: uuid::Uuid::now_v7().to_string(),
                sequence: 0,
                plan_identity: serde_json::json!({"status": "pending"}),
                terminal_reserved: false,
                attempted_failure: None,
                observability: None,
                progress: BoundedProgress::default(),
            })),
            shutdown: ShutdownToken::new(),
        }
    }

    pub(crate) fn execution_id(&self) -> String {
        self.lock_state().execution_id.clone()
    }

    pub(crate) fn batch_id(&self) -> String {
        self.lock_state().batch_id.clone()
    }

    pub(crate) fn shutdown_token(&self) -> ShutdownToken {
        self.shutdown.clone()
    }

    pub(crate) fn set_observability_summary(&self, summary: ObservabilitySummary) {
        self.lock_state().observability = Some(summary);
    }

    pub(crate) fn emit_started(&self) -> io::Result<()> {
        self.emit_event("started", serde_json::Map::new())
    }

    pub(crate) fn emit_plan_resolved(
        &self,
        fingerprint: clinker_plan::plan::SemanticFingerprint,
    ) -> io::Result<()> {
        self.with_state(|state| {
            state.plan_identity = serde_json::json!({
                "status": "resolved",
                "algorithm": fingerprint.algorithm(),
                "version": fingerprint.version(),
                "digest": fingerprint.digest_hex(),
            });
            state.write_event("plan_resolved", serde_json::Map::new())
        })
    }

    pub(crate) fn emit_progress_transition(&self, phase: &str) -> io::Result<()> {
        self.with_state(|state| {
            let snapshot = state.progress.transition(phase);
            state.write_progress(snapshot)
        })
    }

    /// Write one discardable periodic observation.
    ///
    /// Deliberately not routed through [`Self::with_state`]: losing an
    /// advisory snapshot is not evidence that the run must stop. Cancelling
    /// here would destroy a healthy run's computed output over a record the
    /// protocol allows to be missing. A control channel that is genuinely
    /// broken still fails closed at the next *required* record — the
    /// finalizing transition or the terminal.
    fn emit_periodic(&self) -> io::Result<()> {
        let mut state = self.lock_state();
        let Some(snapshot) = state.progress.periodic("executing") else {
            return Ok(());
        };
        state.write_progress(snapshot)
    }

    /// Start the periodic liveness worker.
    ///
    /// The cadence is the worker's own clock, not the engine's cancellation
    /// polling: a run blocked inside one long operation — a large spill
    /// merge, a slow REST page — polls no token and must still look alive to
    /// a supervisor reading the stream.
    pub(crate) fn start_execution_progress(&self) -> io::Result<MachineProgressWorker> {
        #[cfg(debug_assertions)]
        if std::env::var_os("CLINKER_TEST_MACHINE_PROGRESS_WORKER_START_FAILURE").as_deref()
            == Some(std::ffi::OsStr::new("1"))
        {
            return Err(io::Error::other(
                "injected machine progress worker startup failure",
            ));
        }
        let emitter = self.clone();
        let tick = progress_tick();
        let (stop, receiver) = mpsc::sync_channel(1);
        let handle = thread::Builder::new()
            .name("clinker-machine-progress".to_owned())
            .spawn(move || {
                loop {
                    match receiver.recv_timeout(tick) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    emitter.emit_periodic()?;
                }
            })?;
        Ok(MachineProgressWorker {
            stop,
            handle: Some(handle),
        })
    }

    pub(crate) fn emit_completed(&self, exit_code: u8) -> io::Result<()> {
        self.emit_completed_with_publication(exit_code, None)
    }

    pub(crate) fn emit_completed_with_publication(
        &self,
        exit_code: u8,
        publication: Option<&AttemptPublicationOutcome>,
    ) -> io::Result<()> {
        self.with_state(|state| {
            let result = match exit_code {
                0 => "success",
                2 => "completed_with_dlq",
                130 => {
                    return state.write_terminal_event("cancelled", serde_json::Map::new(), None);
                }
                // No other exit code has a `completed` meaning. Reaching here
                // means an earlier `failed` terminal could not be encoded and
                // left the one terminal slot free, so this call is the last
                // chance to state the truth. Relabelling a failed run as
                // success is the one terminal a supervisor cannot recover
                // from: it reconciles a non-zero exit against a success
                // result and reports the batch as done.
                _ => return state.write_unrepresentable_exit_terminal(exit_code, publication),
            };
            let fields = serde_json::Map::from_iter([
                ("result".to_owned(), serde_json::json!(result)),
                ("exit_code".to_owned(), serde_json::json!(exit_code)),
            ]);
            state.write_terminal_event("completed", fields, publication)
        })
    }

    pub(crate) fn emit_failed(
        &self,
        exit_code: u8,
        failure: &FailureClassification,
    ) -> io::Result<()> {
        self.emit_failed_with_publication(exit_code, failure, None)
    }

    pub(crate) fn emit_failed_with_publication(
        &self,
        exit_code: u8,
        failure: &FailureClassification,
        publication: Option<&AttemptPublicationOutcome>,
    ) -> io::Result<()> {
        self.with_state(|state| {
            if state.plan_identity["status"] == "pending" {
                state.plan_identity =
                    serde_json::json!({"status": "unavailable", "reason": "admission_failed"});
            }
            state.attempted_failure = Some(failure.clone());
            state.write_terminal_event("failed", failure_fields(failure, exit_code), publication)
        })
    }

    fn emit_event(
        &self,
        event: &'static str,
        fields: serde_json::Map<String, serde_json::Value>,
    ) -> io::Result<()> {
        self.with_state(|state| state.write_event(event, fields))
    }

    fn with_state(
        &self,
        operation: impl FnOnce(&mut MachineState) -> io::Result<()>,
    ) -> io::Result<()> {
        let result = operation(&mut self.lock_state());
        if result.is_err() {
            self.shutdown.request();
        }
        result
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, MachineState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

impl MachineState {
    fn reserve_terminal(&mut self) -> bool {
        if self.terminal_reserved {
            false
        } else {
            self.terminal_reserved = true;
            true
        }
    }

    fn write_event(
        &mut self,
        event: &'static str,
        fields: serde_json::Map<String, serde_json::Value>,
    ) -> io::Result<()> {
        let encoded = self.encode_event(event, fields, self.sequence)?;
        self.write_encoded_event(&encoded)
    }

    /// Terminal for an exit code the `completed` family cannot express.
    ///
    /// The classification is the one an earlier `failed` emission already
    /// attempted, so the recovered terminal carries the run's real failure
    /// rather than a substitute. Without that record the code alone is all
    /// that is known, and an exit the protocol has no family for is a
    /// violation of this emitter's own contract.
    fn write_unrepresentable_exit_terminal(
        &mut self,
        exit_code: u8,
        publication: Option<&AttemptPublicationOutcome>,
    ) -> io::Result<()> {
        let failure = self.attempted_failure.clone().unwrap_or_else(|| {
            FailureClassification::unknown_internal("run exit code has no machine terminal family")
        });
        self.write_terminal_event("failed", failure_fields(&failure, exit_code), publication)
    }

    fn write_terminal_event(
        &mut self,
        event: &'static str,
        mut fields: serde_json::Map<String, serde_json::Value>,
        publication: Option<&AttemptPublicationOutcome>,
    ) -> io::Result<()> {
        if self.terminal_reserved {
            return Ok(());
        }
        let mut events = Vec::new();
        if let Some(publication) = publication {
            let (artifact_events, summary) = publication_payloads(publication);
            events.extend(
                artifact_events
                    .into_iter()
                    .map(|fields| ("publication_artifacts", fields)),
            );
            fields.insert("publication".to_owned(), summary);
        }
        if let Some(observability) = self.observability {
            fields.insert("observability".to_owned(), serde_json::json!(observability));
        }
        events.push((event, fields));

        // Encode and bound every record before reserving the one terminal slot.
        // An encoding or size failure therefore cannot poison terminal state
        // and suppress a later diagnostic terminal.
        let encoded = events
            .into_iter()
            .enumerate()
            .map(|(offset, (event, fields))| {
                self.encode_event(event, fields, self.sequence.saturating_add(offset as u64))
            })
            .collect::<io::Result<Vec<_>>>()?;
        if !self.reserve_terminal() {
            return Ok(());
        }
        for record in encoded {
            self.write_encoded_event(&record)?;
        }
        Ok(())
    }

    fn encode_event(
        &self,
        event: &'static str,
        fields: serde_json::Map<String, serde_json::Value>,
        sequence: u64,
    ) -> io::Result<Vec<u8>> {
        #[cfg(debug_assertions)]
        if injected_write_failure(event, &fields) {
            return Err(io::Error::other(format!(
                "injected machine write failure at {event}"
            )));
        }

        let mut object = serde_json::Map::new();
        object.insert("protocol".to_owned(), serde_json::json!("clinker.run"));
        object.insert("schema".to_owned(), serde_json::json!(1));
        object.insert("event".to_owned(), serde_json::json!(event));
        object.insert("seq".to_owned(), serde_json::json!(sequence));
        object.insert("batch_id".to_owned(), serde_json::json!(self.batch_id));
        object.insert(
            "execution_id".to_owned(),
            serde_json::json!(self.execution_id),
        );
        object.insert("plan_identity".to_owned(), self.plan_identity.clone());
        object.extend(fields);
        let mut encoded = serde_json::to_vec(&object).map_err(io::Error::other)?;
        encoded.push(b'\n');
        if encoded.len() > MAX_EVENT_BYTES {
            return Err(io::Error::other(format!(
                "machine event exceeds {MAX_EVENT_BYTES}-byte limit"
            )));
        }
        Ok(encoded)
    }

    fn write_encoded_event(&mut self, encoded: &[u8]) -> io::Result<()> {
        self.writer.write_all(encoded)?;
        self.writer.flush()?;
        self.sequence = self.sequence.saturating_add(1);
        Ok(())
    }

    fn write_progress(&mut self, snapshot: ProgressSnapshot) -> io::Result<()> {
        // The terminal record is the last record of a run. A progress writer
        // runs on its own thread and can still be mid-tick when the run ends,
        // so the reservation is what orders them rather than the shutdown
        // handshake — a supervisor that stops reading at the terminal would
        // otherwise be handed one more line it can never attribute.
        if self.terminal_reserved {
            return Ok(());
        }
        let progress = serde_json::json!({
            "phase": snapshot.phase(),
            "kind": snapshot.kind().as_str(),
            "elapsed_ms": snapshot.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        });
        let fields = serde_json::Map::from_iter([
            ("progress".to_owned(), progress),
            (
                "truncation".to_owned(),
                serde_json::json!({
                    "detail": snapshot.detail_truncated(),
                    "events": snapshot.event_limit_reached(),
                }),
            ),
        ]);
        self.write_event("progress", fields)
    }
}

fn failure_fields(
    failure: &FailureClassification,
    exit_code: u8,
) -> serde_json::Map<String, serde_json::Value> {
    serde_json::Map::from_iter([
        (
            "failure".to_owned(),
            serde_json::json!({
                "code": failure.code(),
                "category": failure.category().as_str(),
                "retry": failure.retry_advice().as_str(),
                "message": failure.message(),
            }),
        ),
        ("exit_code".to_owned(), serde_json::json!(exit_code)),
    ])
}

fn publication_payloads(
    outcome: &AttemptPublicationOutcome,
) -> (
    Vec<serde_json::Map<String, serde_json::Value>>,
    serde_json::Value,
) {
    let (cleanup_debt_count, complete) = match outcome {
        AttemptPublicationOutcome::Complete {
            cleanup_debt_count, ..
        } => (*cleanup_debt_count, true),
        AttemptPublicationOutcome::Incomplete {
            cleanup_debt_count, ..
        } => (*cleanup_debt_count, false),
    };
    let artifacts = outcome
        .artifacts()
        .iter()
        .map(|artifact| {
            serde_json::json!({
                "artifact_id": artifact.artifact_id(),
                "kind": artifact_kind(artifact.kind()),
                "state": artifact_state(artifact.state()),
            })
        })
        .collect::<Vec<_>>();
    publication_payloads_from_artifacts(complete, cleanup_debt_count, artifacts)
}

fn publication_payloads_from_artifacts(
    complete: bool,
    cleanup_debt_count: usize,
    artifacts: Vec<serde_json::Value>,
) -> (
    Vec<serde_json::Map<String, serde_json::Value>>,
    serde_json::Value,
) {
    let chunk_count = artifacts.len().div_ceil(PUBLICATION_ARTIFACTS_PER_EVENT);
    let artifact_events = artifacts
        .chunks(PUBLICATION_ARTIFACTS_PER_EVENT)
        .enumerate()
        .map(|(chunk_index, artifacts)| {
            serde_json::Map::from_iter([(
                "publication".to_owned(),
                serde_json::json!({
                    "chunk_index": chunk_index,
                    "chunk_count": chunk_count,
                    "artifacts": artifacts,
                }),
            )])
        })
        .collect();
    let mut state_counts = serde_json::Map::from_iter([
        ("staging".to_owned(), serde_json::json!(0)),
        ("ready".to_owned(), serde_json::json!(0)),
        ("promoting".to_owned(), serde_json::json!(0)),
        ("published".to_owned(), serde_json::json!(0)),
        ("visible_unsynchronized".to_owned(), serde_json::json!(0)),
        ("unpublished".to_owned(), serde_json::json!(0)),
    ]);
    for artifact in &artifacts {
        let Some(key) = artifact["state"].as_str() else {
            continue;
        };
        let count = state_counts[key].as_u64().unwrap_or(0).saturating_add(1);
        state_counts.insert(key.to_owned(), serde_json::json!(count));
    }
    let summary = serde_json::json!({
        "complete": complete,
        "cleanup_debt_count": cleanup_debt_count,
        "artifact_count": artifacts.len(),
        "state_counts": state_counts,
    });
    (artifact_events, summary)
}

fn artifact_kind(kind: ArtifactKind) -> &'static str {
    match kind {
        ArtifactKind::Primary => "primary",
        ArtifactKind::FanOut => "fan_out",
        ArtifactKind::Split => "split",
        ArtifactKind::Dlq => "dlq",
        ArtifactKind::Sidecar => "sidecar",
    }
}

fn artifact_state(state: ArtifactState) -> &'static str {
    match state {
        ArtifactState::Staging => "staging",
        ArtifactState::Ready => "ready",
        ArtifactState::Promoting => "promoting",
        ArtifactState::Published => "published",
        ArtifactState::VisibleUnsynchronized => "visible_unsynchronized",
        ArtifactState::Unpublished => "unpublished",
    }
}

#[cfg(debug_assertions)]
fn injected_write_failure(
    event: &str,
    fields: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let Some(point) = std::env::var_os("CLINKER_TEST_MACHINE_WRITE_FAILURE") else {
        return false;
    };
    match point.to_string_lossy().as_ref() {
        "periodic" => event == "progress" && fields["progress"]["kind"] == "periodic",
        "finalizing" => {
            event == "progress"
                && fields["progress"]["kind"] == "transition"
                && fields["progress"]["phase"] == "finalizing"
        }
        "failed_terminal" => event == "failed",
        "terminal" => matches!(event, "completed" | "failed" | "cancelled"),
        _ => false,
    }
}

fn stdout_conflict(args: &RunArgs) -> Option<&'static str> {
    if args.explain.is_some() {
        Some("--explain")
    } else if args.dry_run {
        Some("--dry-run")
    } else if args.dry_run_n.is_some() {
        Some("--dry-run-n")
    } else if args.dry_run_output.is_some() {
        Some("--dry-run-output")
    } else if args
        .lineage
        .as_deref()
        .is_some_and(|path| path.as_os_str() == "-")
    {
        Some("--lineage -")
    } else if args
        .lineage_events
        .as_deref()
        .is_some_and(|path| path.as_os_str() == "-")
    {
        Some("--lineage-events -")
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writer that keeps every flushed byte readable by the test that owns it.
    #[derive(Clone)]
    struct SharedSink(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn events(sink: &SharedSink) -> Vec<serde_json::Value> {
        let bytes = sink
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        std::str::from_utf8(&bytes)
            .expect("machine stream is UTF-8")
            .lines()
            .filter(|line| !line.is_empty())
            .map(|line| serde_json::from_str(line).expect("every record is JSON"))
            .collect()
    }

    fn capturing_emitter() -> (MachineEmitter, SharedSink) {
        let sink = SharedSink(Arc::new(Mutex::new(Vec::new())));
        let emitter = MachineEmitter::with_writer("batch".to_owned(), Box::new(sink.clone()));
        (emitter, sink)
    }

    fn state() -> MachineState {
        capturing_state().0
    }

    fn capturing_state() -> (MachineState, SharedSink) {
        let sink = SharedSink(Arc::new(Mutex::new(Vec::new())));
        let state = MachineState {
            writer: BufWriter::new(Box::new(sink.clone())),
            batch_id: "batch".to_owned(),
            execution_id: "018f47a2-9a41-7a27-b4d6-4f7137e3c159".to_owned(),
            sequence: 0,
            plan_identity: serde_json::json!({"status": "resolved"}),
            terminal_reserved: false,
            attempted_failure: None,
            observability: None,
            progress: BoundedProgress::default(),
        };
        (state, sink)
    }

    #[test]
    fn maximum_artifact_inventory_is_chunked_below_the_event_limit() {
        let artifacts = (0..clinker_exec::output::attempt::MANIFEST_MAX_ARTIFACTS)
            .map(|index| {
                serde_json::json!({
                    "artifact_id": format!("artifact-{index:08x}"),
                    "kind": "fan_out",
                    "state": "published",
                })
            })
            .collect::<Vec<_>>();
        let (artifact_events, summary) = publication_payloads_from_artifacts(true, 0, artifacts);
        assert_eq!(artifact_events.len(), 64);
        assert_eq!(summary["artifact_count"], 4096);
        assert_eq!(summary["state_counts"]["published"], 4096);

        let state = state();
        for (sequence, fields) in artifact_events.into_iter().enumerate() {
            let encoded = state
                .encode_event("publication_artifacts", fields, sequence as u64)
                .expect("bounded publication chunk");
            assert!(encoded.len() <= MAX_EVENT_BYTES);
        }
        let terminal = state
            .encode_event(
                "completed",
                serde_json::Map::from_iter([
                    ("result".to_owned(), serde_json::json!("success")),
                    ("exit_code".to_owned(), serde_json::json!(0)),
                    ("publication".to_owned(), summary),
                ]),
                64,
            )
            .expect("bounded terminal");
        assert!(terminal.len() <= MAX_EVENT_BYTES);
    }

    #[test]
    fn terminal_is_reserved_only_after_successful_encoding() {
        let mut state = state();
        let oversized = serde_json::Map::from_iter([(
            "detail".to_owned(),
            serde_json::json!("x".repeat(MAX_EVENT_BYTES)),
        )]);
        assert!(
            state
                .write_terminal_event("failed", oversized, None)
                .is_err()
        );
        assert!(!state.terminal_reserved);
        assert_eq!(state.sequence, 0);

        state
            .write_terminal_event("failed", serde_json::Map::new(), None)
            .expect("later bounded terminal remains available");
        assert!(state.terminal_reserved);
        assert_eq!(state.sequence, 1);
    }

    #[test]
    fn an_exit_code_outside_the_completed_family_is_never_labelled_success() {
        let (emitter, sink) = capturing_emitter();
        emitter.emit_completed(4).expect("terminal");

        let stream = events(&sink);
        let terminal = stream.last().expect("terminal record");
        assert_eq!(terminal["event"], "failed");
        assert_eq!(terminal["exit_code"], 4);
        assert_eq!(terminal["failure"]["code"], "runtime.invariant.unknown");
        assert!(
            stream.iter().all(|event| event["result"] != "success"),
            "a non-zero exit must never reconcile against a success result: {stream:#?}"
        );
    }

    #[test]
    fn a_failed_terminal_that_did_not_fit_is_recovered_with_its_own_classification() {
        let (mut state, sink) = capturing_state();
        let failure = FailureClassification::for_code("attempt.publication.promotion_failed")
            .expect("registered code");
        let mut oversized = failure_fields(&failure, 4);
        oversized.insert(
            "detail".to_owned(),
            serde_json::json!("x".repeat(MAX_EVENT_BYTES)),
        );
        state.attempted_failure = Some(failure);
        assert!(
            state
                .write_terminal_event("failed", oversized, None)
                .is_err()
        );
        assert!(!state.terminal_reserved);

        // This is the call `main()`'s `Ok(4)` arm makes against the still-free
        // slot; it must restate the publication failure, not relabel it.
        state
            .write_unrepresentable_exit_terminal(4, None)
            .expect("recovered terminal");

        let terminal = events(&sink).pop().expect("terminal record");
        assert_eq!(terminal["event"], "failed");
        assert_eq!(terminal["exit_code"], 4);
        assert_eq!(
            terminal["failure"]["code"],
            "attempt.publication.promotion_failed"
        );
    }

    #[test]
    fn periodic_liveness_does_not_depend_on_cancellation_polling() {
        let (emitter, sink) = capturing_emitter();
        let worker = emitter
            .start_execution_progress()
            .expect("progress worker starts");
        // Nothing polls the run's shutdown token here, exactly like a run
        // wedged inside one long spill merge or one slow REST page.
        //
        // Waited for rather than slept past. The claim is that the worker's
        // own clock produces liveness, not that it produces it within any
        // particular span: sleeping a fixed multiple of the tick asserts that
        // a thread starts and completes one wait inside it, which a loaded
        // host with coarse timer granularity does not owe us.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let periodics = loop {
            let seen = events(&sink)
                .into_iter()
                .filter(|event| {
                    event["event"] == "progress" && event["progress"]["kind"] == "periodic"
                })
                .collect::<Vec<_>>();
            if !seen.is_empty() || std::time::Instant::now() >= deadline {
                break seen;
            }
            thread::sleep(PROGRESS_TICK);
        };
        worker.finish().expect("progress worker drains");
        assert!(
            !periodics.is_empty(),
            "liveness must come from the worker's own clock"
        );
        assert_eq!(periodics[0]["progress"]["phase"], "executing");
    }
}
