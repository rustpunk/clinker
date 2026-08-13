//! Sole ownership of the optional machine-run stream.

use std::io::{self, Write};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant};

use clinker_core_types::FailureClassification;
use clinker_exec::output::attempt::{ArtifactKind, ArtifactState, AttemptPublicationOutcome};
use clinker_exec::pipeline::shutdown::ShutdownToken;
#[cfg(debug_assertions)]
use clinker_exec::progress::ProgressKind;
use clinker_exec::progress::{BoundedProgress, ProgressSnapshot};
use clinker_exec::telemetry::TelemetryProducer;

use crate::observability::{AdmissionSummary, ObservabilitySummary};
use crate::{MachineFormat, RunArgs};

const MAX_EVENT_BYTES: usize = 16 * 1024;
const MAX_BATCH_ID_BYTES: usize = 256;
const PUBLICATION_ARTIFACTS_PER_EVENT: usize = 64;
/// Wake cadence of the progress worker. `BoundedProgress` throttles what
/// actually reaches the stream; this only bounds how late a due snapshot is.
const PROGRESS_TICK: Duration = Duration::from_millis(20);

/// How long the liveness worker keeps trying a sink that will not take a
/// record before it reports the sink as failed.
///
/// The error kind cannot answer this. Listing the kinds worth retrying stopped
/// a healthy run on the first unlisted one; listing the kinds worth giving up
/// on retried a full filesystem for the rest of the run and reported success,
/// because a filesystem that is full and a reader that is merely behind
/// present identically at the first attempt and differ only in whether they
/// clear. Time is what distinguishes them, so time is what bounds the trying.
const PROGRESS_SINK_PATIENCE: Duration = Duration::from_secs(5);

/// Resolve the failing-sink window.
///
/// Overridable in debug builds for the same reason the tick is: a test that
/// waits out the real window spends five seconds proving one branch.
fn progress_sink_patience() -> Duration {
    #[cfg(debug_assertions)]
    if let Some(value) = std::env::var_os("CLINKER_TEST_MACHINE_SINK_PATIENCE_MS")
        && let Ok(millis) = value.to_string_lossy().parse::<u64>()
    {
        return Duration::from_millis(millis);
    }
    PROGRESS_SINK_PATIENCE
}

/// Whether a failed write means the reader is already gone.
///
/// Only the unambiguous cases, which are worth ending on immediately rather
/// than spending the patience window discovering. Everything else is left to
/// that window.
fn reader_is_gone(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::BrokenPipe
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::NotConnected
    )
}

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
    /// The sink itself, with no buffering layer of our own between it and the
    /// record.
    ///
    /// A `BufWriter` here retained the unwritten remainder of a short write in
    /// a buffer this emitter could not see, so a record the sink had refused
    /// completed itself on the next flush — after a retry had already written
    /// the whole record again. Two terminals under one contract that carries
    /// exactly one. What is on the stream has to be derived from what the sink
    /// took, and that is only knowable by handing it the bytes directly.
    writer: Box<dyn Write + Send>,
    /// The one record the sink has taken part of, if it took part of one.
    ///
    /// Its remainder is owned here rather than by the sink, so a short write
    /// is recoverable: the next attempt finishes this record before writing
    /// anything after it, and no new bytes are ever appended to a line the
    /// sink has not been given in full. A record the sink took *nothing* of is
    /// not held here — it never reached the stream, so there is nothing to
    /// finish. Bounded by [`MAX_EVENT_BYTES`], the largest record this
    /// protocol admits.
    pending: Option<PendingRecord>,
    batch_id: String,
    execution_id: String,
    /// The number the *next* accepted record carries.
    ///
    /// Consumed by acceptance, not by the attempt: a record the sink refused
    /// never reached a reader, so leaving its number unused is what keeps the
    /// stream densely numbered from zero. Both consumers require that — the
    /// published contract states `seq` increases by exactly one, and the
    /// reference adapter reconciles a gap as an incomplete attempt — and a
    /// discardable liveness record refused by a momentarily full pipe would
    /// otherwise shift every later number in a run that went on to succeed.
    sequence: u64,
    plan_identity: serde_json::Value,
    /// Whether a terminal record has been handed to the sink in full.
    ///
    /// Only that. It is deliberately not "a terminal emission is under way":
    /// the state lock is held across an entire emission, so an in-flight
    /// terminal has no duration another thread could observe and needs no bit
    /// of its own. Spending the slot on the attempt instead of on the record
    /// let one refused write suppress every later terminal while telling the
    /// caller a terminal had been reported.
    terminal_reserved: bool,
    /// Whether the one-shot event-limit notice has already been re-offered
    /// after a record that did not go out. It is offered again at most once.
    notice_reoffered: bool,
    /// Classification of a `failed` terminal whose encoding did not fit the
    /// event bound. The slot is still free after such a failure, so this is
    /// what the next terminal attempt must report instead of inventing one.
    attempted_failure: Option<FailureClassification>,
    observability: Option<ObservabilitySummary>,
    /// The export worker's live counters, read when a terminal is written on a
    /// path that never reached the explicit flush. Without it those terminals
    /// carried no `observability` field at all, so one condition was reported
    /// two ways depending on how early the run failed.
    observability_progress: Option<Arc<Mutex<ObservabilitySummary>>>,
    /// Arena-admission accounting, kept apart from the export summary because
    /// it comes from the producer rather than from the exporter. It is merged
    /// into whichever export summary the terminal ends up carrying, so a
    /// terminal written off the progress fallback still reports the run's
    /// telemetry loss rather than only what the exporter managed to ship.
    observability_admission: Option<AdmissionSummary>,
    /// The arena itself, read when a terminal is written on a path that never
    /// reached the flush that would have pushed a final accounting.
    ///
    /// The field's absence in the terminal means *no arena was reserved*, so a
    /// run that reserved one and then left by an early return — a storage
    /// validation failure, an unopenable lineage destination, a source that
    /// does not resolve — must not report itself the same way. Reading the
    /// arena here is the accounting a supervisor can still be given; it is
    /// marked incomplete, exactly as a mid-drain flush sample is, because the
    /// exporter has not been joined and the counters can still move.
    observability_arena: Option<TelemetryProducer>,
    /// The publication the first terminal attempt carried.
    ///
    /// A refused inventory write leaves the terminal slot free, and the retry
    /// that follows is made from `main` with no publication in hand. Without
    /// this the recovered terminal dropped a summary that had been available,
    /// making the artifact evidence a supervisor reads depend on whether the
    /// first write happened to succeed.
    publication: Option<TerminalPublication>,
    /// How many of the retained publication's inventory records the sink has
    /// taken in full.
    ///
    /// A retry resumes from here instead of restarting the inventory. The
    /// chunk index a record carries is its position in the inventory, so
    /// re-sending a chunk the sink already took puts two records with the same
    /// index on one stream, and a consumer told to reassemble chunks zero
    /// through `chunk_count - 1` in sequence rejects that — including the
    /// reference adapter, which then reports a recovered terminal whose
    /// content is correct as an incomplete attempt.
    publication_chunks_sent: usize,
    progress: BoundedProgress,
}

/// One publication's inventory records and terminal summary, retained so a
/// terminal retried after a refused write still carries them.
#[derive(Clone)]
struct TerminalPublication {
    artifact_events: Vec<serde_json::Map<String, serde_json::Value>>,
    summary: serde_json::Value,
}

/// What accepting a record settles, beyond its sequence number.
#[derive(Clone, Copy, Eq, PartialEq)]
enum RecordRole {
    Ordinary,
    /// One `publication_artifacts` chunk of the retained publication.
    Inventory,
    /// The run's terminal. Accepting it spends the one terminal slot.
    Terminal,
}

/// A record handed to the sink that the sink has not taken in full.
struct PendingRecord {
    encoded: Vec<u8>,
    /// How many leading bytes the sink has taken. The record is delivered when
    /// this reaches `encoded.len()` *and* the sink has flushed: a writer with
    /// its own buffer can report bytes as taken and still be holding them.
    taken: usize,
    role: RecordRole,
}

impl PendingRecord {
    /// Push the remainder to the sink, returning once the whole record is out.
    ///
    /// On error the record keeps exactly what it has left to send, so the next
    /// attempt continues the same line rather than starting a second copy of
    /// it.
    fn deliver(&mut self, writer: &mut (dyn Write + Send)) -> io::Result<()> {
        while self.taken < self.encoded.len() {
            match writer.write(&self.encoded[self.taken..]) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "machine sink accepted no bytes of a record",
                    ));
                }
                Ok(taken) => self.taken = self.taken.saturating_add(taken),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
        writer.flush()
    }
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
        // either way. Ordering the last record against the terminal is the
        // terminal reservation's job, not this one's.
        //
        // What the join returns is reported for the same reason the explicit
        // `finish` reports it. The worker returns `Err` only for a record it
        // can never encode, or for a reader that is gone or has refused for
        // the whole patience window — and `emit_periodic` deliberately does
        // not trip the shutdown token, so nothing else says so. Discarding it
        // here meant a run leaving by an early return between the worker's
        // start and `finish` lost the one condition this thread exists to
        // report, with no line on stderr at all.
        if let Some(Err(error)) = self.stop_and_join() {
            tracing::warn!(error = %error, "machine progress worker failed before the run reached its explicit finish");
        }
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
                writer,
                pending: None,
                batch_id,
                execution_id: uuid::Uuid::now_v7().to_string(),
                sequence: 0,
                plan_identity: serde_json::json!({"status": "pending"}),
                terminal_reserved: false,
                notice_reoffered: false,
                attempted_failure: None,
                observability: None,
                observability_progress: None,
                observability_admission: None,
                observability_arena: None,
                publication: None,
                publication_chunks_sent: 0,
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

    /// Attach the export worker's live counters as a fallback for terminals
    /// written before the flush that would have pushed a final summary.
    pub(crate) fn attach_observability_progress(&self, progress: Arc<Mutex<ObservabilitySummary>>) {
        self.lock_state().observability_progress = Some(progress);
    }

    /// Record what the telemetry arena admitted and refused.
    ///
    /// Separate from the export summary because the two have different
    /// authors and different availability: the exporter reports what reached
    /// a collector, and only the producer can say what never got that far.
    pub(crate) fn set_observability_admission(&self, admission: AdmissionSummary) {
        self.lock_state().observability_admission = Some(admission);
    }

    /// Attach the arena itself as the accounting a terminal falls back to.
    ///
    /// Handed over as soon as the producer exists, so no path between here and
    /// the flush can reach a terminal that omits the field. Omitting it means
    /// "no arena was reserved", and the run reserved one — a terminal written
    /// on an early return said the opposite of what had happened, and there
    /// was no call site to add that could not later be forgotten again.
    pub(crate) fn attach_observability_arena(&self, producer: TelemetryProducer) {
        self.lock_state().observability_arena = Some(producer);
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
    fn emit_periodic(&self) -> ProgressWrite {
        let mut state = self.lock_state();
        let Some(snapshot) = state.progress.periodic("executing") else {
            return ProgressWrite::Skipped;
        };
        let notice = snapshot.event_limit_reached();
        let written = state.write_progress_staged(snapshot);
        if notice && !matches!(written, ProgressWrite::Written) && !state.notice_reoffered {
            // The notice is a one-shot spent on being handed out, so a record
            // that did not go out gives it back -- otherwise the stream falls
            // silent with the one record explaining why being the one lost.
            //
            // Once only: a failed write may still have buffered its bytes and
            // sent them later, so re-offering can duplicate the notice. One
            // extra is the price of not going silent; an unbounded number is
            // not, and a sink that keeps refusing is reported by the window.
            state.notice_reoffered = true;
            state.progress.restore_event_limit_notice();
        }
        written
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
                let patience = progress_sink_patience();
                let mut failing: Option<(Instant, io::Error)> = None;
                loop {
                    match receiver.recv_timeout(tick) {
                        Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                        Err(mpsc::RecvTimeoutError::Timeout) => {}
                    }
                    // A liveness record that could not be written this tick is
                    // a reason to try again next tick, not a reason to stop
                    // writing them. Returning on the first error left the run
                    // going with no further records at all -- and since this
                    // path deliberately does not trip the shutdown token,
                    // nothing said so; a supervisor watching for liveness then
                    // killed a healthy run. A closed reader is different: no
                    // later tick can reach it either.
                    match emitter.emit_periodic() {
                        // Only a record that got through says the sink is
                        // alive. A tick with nothing due says nothing.
                        ProgressWrite::Skipped => {}
                        ProgressWrite::Written => failing = None,
                        // Nothing about a later tick makes an unbuildable
                        // record buildable.
                        ProgressWrite::Unencodable(error) => return Err(error),
                        ProgressWrite::Unsent(error) if reader_is_gone(&error) => {
                            return Err(error);
                        }
                        ProgressWrite::Unsent(error) => {
                            failing.get_or_insert((Instant::now(), error));
                        }
                    }
                    // Measured from the first refusal and checked on every
                    // tick, so the window is a duration rather than a count of
                    // the records that happen to fall due inside it.
                    if failing
                        .as_ref()
                        .is_some_and(|(since, _)| since.elapsed() >= patience)
                    {
                        let (_, error) = failing.expect("just checked");
                        // A sink that has refused for the whole window is not
                        // behind, it is broken, and saying so is the point of
                        // this thread.
                        return Err(error);
                    }
                }
            })?;
        Ok(MachineProgressWorker {
            stop,
            handle: Some(handle),
        })
    }

    pub(crate) fn emit_completed(&self, exit_code: u8) -> io::Result<TerminalOutcome> {
        self.emit_completed_with_publication(exit_code, None)
    }

    /// Terminal for an invocation that succeeded without running an attempt.
    ///
    /// The plan-only `--lineage` export is the case: it preflights the
    /// identity policy, writes its document, and returns before any data is
    /// read, sharing this stream's `execution_id` and `batch_id` so the
    /// exported document is correlatable with the invocation that produced it.
    ///
    /// It carries an explicit empty inventory rather than no `publication`
    /// field. A `completed` / `success` / exit `0` terminal is read by the
    /// published contract as *publication is complete*, and a consumer that
    /// reconciles artifact evidence — the reference adapter among them — reads
    /// an absent inventory on that row as a stream it cannot accept. Zero
    /// artifacts, every state count zero, and no cleanup debt is the same
    /// statement in the vocabulary the reconciliation table already defines:
    /// nothing was published, and that is the whole of it.
    pub(crate) fn emit_completed_without_attempt(&self) -> io::Result<TerminalOutcome> {
        self.with_state(|state| {
            let fields = serde_json::Map::from_iter([
                ("result".to_owned(), serde_json::json!("success")),
                ("exit_code".to_owned(), serde_json::json!(0)),
            ]);
            let (artifact_events, summary) =
                publication_payloads_from_artifacts(true, 0, Vec::new());
            state.write_terminal_for(
                "completed",
                fields,
                Some(TerminalPublication {
                    artifact_events,
                    summary,
                }),
            )
        })
    }

    pub(crate) fn emit_completed_with_publication(
        &self,
        exit_code: u8,
        publication: Option<&AttemptPublicationOutcome>,
    ) -> io::Result<TerminalOutcome> {
        self.with_state(|state| {
            let result = match exit_code {
                0 => "success",
                2 => "completed_with_dlq",
                // Forwarded like every other arm. A cancelled run that did
                // publish artifacts before the cancellation reports the same
                // inventory a `completed` or `failed` run reports; dropping
                // the argument here made one field's presence depend on which
                // terminal family the exit code happened to fall in.
                130 => {
                    return state.write_terminal_event(
                        "cancelled",
                        serde_json::Map::new(),
                        publication,
                    );
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
    ) -> io::Result<TerminalOutcome> {
        self.emit_failed_with_publication(exit_code, failure, None)
    }

    pub(crate) fn emit_failed_with_publication(
        &self,
        exit_code: u8,
        failure: &FailureClassification,
        publication: Option<&AttemptPublicationOutcome>,
    ) -> io::Result<TerminalOutcome> {
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

    fn with_state<T>(
        &self,
        operation: impl FnOnce(&mut MachineState) -> io::Result<T>,
    ) -> io::Result<T> {
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
    fn write_event(
        &mut self,
        event: &'static str,
        fields: serde_json::Map<String, serde_json::Value>,
    ) -> io::Result<()> {
        self.resume_pending()?;
        let encoded = self.encode_event(event, fields, self.sequence)?;
        self.write_encoded_event(&encoded, RecordRole::Ordinary)
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
    ) -> io::Result<TerminalOutcome> {
        let failure = self.attempted_failure.clone().unwrap_or_else(|| {
            FailureClassification::unknown_internal("run exit code has no machine terminal family")
        });
        self.write_terminal_event("failed", failure_fields(&failure, exit_code), publication)
    }

    fn write_terminal_event(
        &mut self,
        event: &'static str,
        fields: serde_json::Map<String, serde_json::Value>,
        publication: Option<&AttemptPublicationOutcome>,
    ) -> io::Result<TerminalOutcome> {
        let publication = publication.map(|publication| {
            let (artifact_events, summary) = publication_payloads(publication);
            TerminalPublication {
                artifact_events,
                summary,
            }
        });
        self.write_terminal_for(event, fields, publication)
    }

    /// Write a terminal, remembering the publication it carried.
    ///
    /// A refused inventory write leaves the terminal slot free by design, and
    /// the retry that follows is made from `main`, which has no publication in
    /// hand — so without the memo the recovered terminal reported no artifact
    /// evidence at all, and what a supervisor learned about the attempt
    /// depended on whether the first write happened to succeed.
    fn write_terminal_for(
        &mut self,
        event: &'static str,
        fields: serde_json::Map<String, serde_json::Value>,
        publication: Option<TerminalPublication>,
    ) -> io::Result<TerminalOutcome> {
        if publication.is_some() {
            self.publication = publication;
            // A new inventory is numbered from its own start, so nothing sent
            // for a previous one counts against it.
            self.publication_chunks_sent = 0;
        }
        let (artifact_events, summary) = match self.publication.clone() {
            Some(publication) => (publication.artifact_events, Some(publication.summary)),
            None => (Vec::new(), None),
        };
        self.write_terminal_records(event, fields, artifact_events, summary)
    }

    /// Write the terminal record, preceded by the publication inventory the
    /// terminal's summary counts.
    ///
    /// The one terminal slot is spent by a terminal that reached the sink, not
    /// by the intent to send one. Everything that can fail earlier — encoding
    /// any record, and writing the inventory records that precede the terminal
    /// — leaves the slot free, so a later attempt can still put a terminal on
    /// the stream. The inventory matters here: a run can carry up to
    /// `MANIFEST_MAX_ARTIFACTS` artifacts, and one refused write anywhere in
    /// that window used to end the run with no terminal at all while telling
    /// the caller the terminal had been written.
    ///
    /// A retry after such a failure sends the whole inventory the terminal
    /// counts, minus the chunks the sink already took. The intent is
    /// unchanged — the terminal always arrives with its complete artifact
    /// evidence ahead of it — but the evidence is assembled from what the sink
    /// accepted rather than from what was attempted, so each chunk index
    /// appears exactly once and a reader reassembling zero through
    /// `chunk_count - 1` in sequence gets the set the summary counts.
    fn write_terminal_records(
        &mut self,
        event: &'static str,
        mut fields: serde_json::Map<String, serde_json::Value>,
        artifact_events: Vec<serde_json::Map<String, serde_json::Value>>,
        publication_summary: Option<serde_json::Value>,
    ) -> io::Result<TerminalOutcome> {
        // A terminal the sink took only part of is still that terminal.
        // Finishing it here is what makes the retry `main` performs report
        // `AlreadyWritten` instead of putting a second one on the stream.
        self.resume_pending()?;
        if self.terminal_reserved {
            return Ok(TerminalOutcome::AlreadyWritten);
        }
        if let Some(summary) = publication_summary {
            fields.insert("publication".to_owned(), summary);
        }
        // The pushed summary when the flush completed, and otherwise whatever
        // the worker had delivered by now. A terminal that omits the field
        // entirely tells a supervisor nothing about whether this run's
        // telemetry can be trusted.
        let observability = self.observability.or_else(|| {
            self.observability_progress.as_ref().map(|progress| {
                let delivered = *progress
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                ObservabilitySummary {
                    flush_complete: false,
                    ..delivered
                }
            })
        });
        if let Some(mut observability) = observability {
            // Producer-side, so it is attached to whichever export summary the
            // terminal ends up carrying rather than to one of them.
            //
            // The pushed accounting when the flush completed, and otherwise
            // the arena read as it stands. The read is marked incomplete: the
            // export worker has not been joined, so `undecodable` is still
            // being credited and these counters can still move. That is the
            // same distinction a mid-drain flush sample carries, and it is the
            // one thing an absent field could not say.
            observability.admission = self.observability_admission.or_else(|| {
                self.observability_arena
                    .as_ref()
                    .map(|producer| AdmissionSummary::from_arena(producer.snapshot(), false))
            });
            fields.insert("observability".to_owned(), serde_json::json!(observability));
        }

        // Encode and bound every record before any of them is written. An
        // encoding or size failure therefore cannot put a partial inventory on
        // the stream, and cannot suppress a later diagnostic terminal.
        let inventory = artifact_events
            .into_iter()
            .skip(self.publication_chunks_sent)
            .enumerate()
            .map(|(offset, fields)| {
                self.encode_event(
                    "publication_artifacts",
                    fields,
                    self.sequence.saturating_add(offset as u64),
                )
            })
            .collect::<io::Result<Vec<_>>>()?;
        let terminal = self.encode_event(
            event,
            fields,
            self.sequence.saturating_add(inventory.len() as u64),
        )?;

        for record in inventory {
            self.write_encoded_event(&record, RecordRole::Inventory)?;
        }
        #[cfg(debug_assertions)]
        if let Some(error) = injected_terminal_sink_failure() {
            return Err(error);
        }
        self.write_encoded_event(&terminal, RecordRole::Terminal)?;
        Ok(TerminalOutcome::Written)
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

    /// A fault in the sink stage, for the periodic record only.
    ///
    /// Gated on the record's own kind, as the encode-stage point is: a
    /// transition record is written through the same path, and refusing one
    /// fails the run rather than exercising the branch under test.
    #[cfg(debug_assertions)]
    fn injected_sink_failure(&self, kind: ProgressKind) -> Option<io::Error> {
        let point = std::env::var_os("CLINKER_TEST_MACHINE_WRITE_FAILURE")?;
        (point.to_string_lossy() == "periodic_sink" && kind == ProgressKind::Periodic).then(|| {
            io::Error::new(
                io::ErrorKind::StorageFull,
                "injected machine sink failure at periodic",
            )
        })
    }

    /// Hand one whole record to the sink.
    ///
    /// A record is delivered or it is not: nothing is written after a record
    /// the sink has taken only part of, so the stream never carries a line
    /// that a later write completes into a duplicate. The state a delivered
    /// record settles — its sequence number, its inventory position, the
    /// terminal slot — moves only when the sink has taken the whole of it.
    ///
    /// Callers resume any pending record *before* encoding, because completing
    /// one advances the sequence number the next record must carry.
    fn write_encoded_event(&mut self, encoded: &[u8], role: RecordRole) -> io::Result<()> {
        debug_assert!(
            self.pending.is_none(),
            "a record is encoded only once the sink has taken the previous one"
        );
        self.pending = Some(PendingRecord {
            encoded: encoded.to_vec(),
            taken: 0,
            role,
        });
        self.resume_pending()
    }

    /// Finish the record the sink took only part of, if there is one.
    ///
    /// Called before every write, so a record left half-delivered by a sink
    /// that was momentarily full is completed by the next attempt rather than
    /// re-sent whole. Until it completes, nothing else can be written: bytes
    /// appended to an unterminated line would corrupt both records.
    fn resume_pending(&mut self) -> io::Result<()> {
        let Some(mut pending) = self.pending.take() else {
            return Ok(());
        };
        match pending.deliver(self.writer.as_mut()) {
            Ok(()) => {
                self.accept(pending.role);
                Ok(())
            }
            Err(error) => {
                // A sink that took nothing did not put this record on the
                // stream at all, and a record that is not on the stream is
                // simply one that did not happen: it holds no sequence number
                // and nothing has to be finished before the next write. Only
                // bytes the sink actually took oblige the next attempt to
                // complete the line they started.
                if pending.taken > 0 {
                    self.pending = Some(pending);
                }
                Err(error)
            }
        }
    }

    /// Record what the sink has taken.
    fn accept(&mut self, role: RecordRole) {
        self.sequence = self.sequence.saturating_add(1);
        match role {
            RecordRole::Ordinary => {}
            RecordRole::Inventory => {
                self.publication_chunks_sent = self.publication_chunks_sent.saturating_add(1);
            }
            RecordRole::Terminal => self.terminal_reserved = true,
        }
    }

    /// Write a progress record, saying which stage failed if one did.
    ///
    /// The two stages fail for unrelated reasons and only one of them can
    /// succeed on a later attempt. Building the record depends on nothing but
    /// the record, so a failure there will repeat identically every tick;
    /// handing it to the sink depends on the reader, which may simply be
    /// behind. Judging them by error kind instead put a permanent encoding
    /// fault into a loop that retried it for the rest of the run.
    fn write_progress_staged(&mut self, snapshot: ProgressSnapshot) -> ProgressWrite {
        // Before the reservation is read, not after: a terminal the sink took
        // only part of is finished here, and finishing it is what makes the
        // reservation below true. Writing a progress record first would put a
        // line after the terminal on a stream whose last record is the
        // terminal by contract.
        if let Err(error) = self.resume_pending() {
            return ProgressWrite::Unsent(error);
        }
        // The terminal record is the last record of a run. A progress writer
        // runs on its own thread and can still be mid-tick when the run ends,
        // so the reservation is what orders them rather than the shutdown
        // handshake — a supervisor that stops reading at the terminal would
        // otherwise be handed one more line it can never attribute.
        if self.terminal_reserved {
            return ProgressWrite::Skipped;
        }
        let kind = snapshot.kind();
        let progress = serde_json::json!({
            "phase": snapshot.phase(),
            "kind": kind.as_str(),
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
        let encoded = match self.encode_event("progress", fields, self.sequence) {
            Ok(encoded) => encoded,
            Err(error) => return ProgressWrite::Unencodable(error),
        };
        #[cfg(debug_assertions)]
        if let Some(error) = self.injected_sink_failure(kind) {
            // The injected refusal stands in for a sink that took nothing, so
            // it leaves the sequence number free exactly as a real one does.
            return ProgressWrite::Unsent(error);
        }
        match self.write_encoded_event(&encoded, RecordRole::Ordinary) {
            Ok(()) => ProgressWrite::Written,
            Err(error) => ProgressWrite::Unsent(error),
        }
    }

    fn write_progress(&mut self, snapshot: ProgressSnapshot) -> io::Result<()> {
        match self.write_progress_staged(snapshot) {
            ProgressWrite::Skipped | ProgressWrite::Written => Ok(()),
            ProgressWrite::Unencodable(error) | ProgressWrite::Unsent(error) => Err(error),
        }
    }
}

/// What a terminal emission did to the one terminal slot.
///
/// `Ok(())` alone could not say this: it was returned both by the call that
/// put a terminal on the stream and by a call that wrote nothing, and — before
/// the slot was tied to a written record — by a call that wrote nothing
/// because an earlier attempt had failed. A caller deciding what to tell its
/// own supervisor needs the difference.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TerminalOutcome {
    /// This call wrote the run's terminal record.
    Written,
    /// An earlier call already wrote it; this call wrote nothing, and the
    /// stream is terminated either way.
    AlreadyWritten,
}

/// How far a progress record got.
enum ProgressWrite {
    /// Nothing was attempted this tick -- no record was due, or none will be
    /// due again.
    ///
    /// Evidence neither for the sink nor against it, and in particular not a
    /// success: only a record that reached the sink shows the sink working,
    /// and only that clears a latched refusal.
    Skipped,
    Written,
    /// The record could not be built. No later attempt can build it either.
    Unencodable(io::Error),
    /// The record was built but the sink refused it. The next tick may fare
    /// better, unless the reader is gone.
    Unsent(io::Error),
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
    let mut state_counts = serde_json::Map::from_iter(
        ARTIFACT_STATES
            .into_iter()
            .map(|state| (artifact_state(state), serde_json::json!(0))),
    );
    for artifact in &artifacts {
        let Some(key) = artifact["state"].as_str() else {
            continue;
        };
        let count = state_counts
            .get(key)
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0)
            .saturating_add(1);
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

/// Artifact states the publication summary carries a count for, in the order
/// the schema-1 summary lists them.
const ARTIFACT_STATES: [ArtifactState; 6] = [
    ArtifactState::Staging,
    ArtifactState::Ready,
    ArtifactState::Promoting,
    ArtifactState::Published,
    ArtifactState::VisibleUnsynchronized,
    ArtifactState::Unpublished,
];

/// Wire spelling of an artifact state for the schema-1 machine protocol.
///
/// The token is taken from the enum's serde representation, which is the same
/// representation the on-disk attempt manifest carries, so the protocol and the
/// manifest cannot spell one state two ways. Serializing a fieldless variant
/// into a JSON string does not fail; the fallback derives the identical
/// `snake_case` token from the variant name so the protocol still emits a
/// stable token rather than failing the run.
fn artifact_state(state: ArtifactState) -> String {
    match serde_json::to_value(state) {
        Ok(serde_json::Value::String(name)) => name,
        _ => snake_case(&format!("{state:?}")),
    }
}

fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (index, character) in name.char_indices() {
        if character.is_ascii_uppercase() {
            if index > 0 {
                out.push('_');
            }
            out.push(character.to_ascii_lowercase());
        } else {
            out.push(character);
        }
    }
    out
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
        // Only the encode stage. `periodic_sink` is the sink-stage point and
        // is matched there; claiming it here shadowed it, because encoding
        // runs first -- so the branch the sink point exists to reach was
        // still unreachable and its test passed through the fatal path.
        "periodic" => event == "progress" && fields["progress"]["kind"] == "periodic",
        // The required record every path emits once the plan is known, on the
        // run path and on the plan-only export alike. One point, so a test can
        // hold the two paths to the same answer for the same condition.
        "plan_resolved" => event == "plan_resolved",
        "finalizing" => {
            event == "progress"
                && fields["progress"]["kind"] == "transition"
                && fields["progress"]["phase"] == "finalizing"
        }
        "failed_terminal" => event == "failed",
        // The first terminal of a published run, and only that one. The retry
        // `main` makes still reaches the stream, which is where a supervisor
        // reads what a run whose report could not be delivered actually did.
        "completed_terminal" => event == "completed",
        "terminal" => matches!(event, "completed" | "failed" | "cancelled"),
        _ => false,
    }
}

/// A sink that takes the inventory and refuses the terminal, once.
///
/// The encode-stage points cannot reach this: encoding runs before any record
/// is written, so a terminal that fails to encode leaves the inventory unsent
/// too, and the retry that follows has nothing to resume. What a supervisor
/// pipe actually does — take some records and then refuse — is only reachable
/// at the sink stage, and it is the case in which a retry could re-send a
/// chunk the reader already has.
///
/// One-shot, so the retry the refusal exists to provoke can get through.
#[cfg(debug_assertions)]
fn injected_terminal_sink_failure() -> Option<io::Error> {
    static FIRED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    let point = std::env::var_os("CLINKER_TEST_MACHINE_WRITE_FAILURE")?;
    (point.to_string_lossy() == "terminal_sink"
        && !FIRED.swap(true, std::sync::atomic::Ordering::Relaxed))
    .then(|| {
        io::Error::new(
            io::ErrorKind::WouldBlock,
            "injected machine sink refusal at terminal",
        )
    })
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

    /// Sink that takes a fixed number of records and then refuses, standing in
    /// for a supervisor pipe that stops accepting writes mid-stream.
    ///
    /// The budget is in records rather than bytes because the emitter flushes
    /// each record as it writes it, so one refusal is one lost record.
    #[derive(Clone)]
    struct RefusingSink {
        accepted: SharedSink,
        budget: Arc<Mutex<usize>>,
    }

    impl RefusingSink {
        fn new(records: usize) -> Self {
            Self {
                accepted: SharedSink(Arc::new(Mutex::new(Vec::new()))),
                budget: Arc::new(Mutex::new(records)),
            }
        }

        fn allow(&self, records: usize) {
            *self
                .budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = records;
        }
    }

    impl Write for RefusingSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            {
                let mut budget = self
                    .budget
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if *budget == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::WouldBlock,
                        "injected sink refusal",
                    ));
                }
                *budget -= 1;
            }
            self.accepted.write(bytes)
        }

        fn flush(&mut self) -> io::Result<()> {
            self.accepted.flush()
        }
    }

    /// Sink that takes a bounded number of *bytes* and then refuses, standing
    /// in for a supervisor pipe with room for part of a record.
    ///
    /// This is what `RefusingSink` cannot model: a pipe does not refuse a
    /// record, it refuses a byte, and the bytes it took before refusing are
    /// already on the wire.
    #[derive(Clone)]
    struct ShortWritingSink {
        accepted: SharedSink,
        budget: Arc<Mutex<usize>>,
    }

    impl ShortWritingSink {
        fn new(bytes: usize) -> Self {
            Self {
                accepted: SharedSink(Arc::new(Mutex::new(Vec::new()))),
                budget: Arc::new(Mutex::new(bytes)),
            }
        }

        fn allow(&self, bytes: usize) {
            *self
                .budget
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = bytes;
        }

        fn accepted_bytes(&self) -> Vec<u8> {
            self.accepted
                .0
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    impl Write for ShortWritingSink {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            let taken = {
                let mut budget = self
                    .budget
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let taken = (*budget).min(bytes.len());
                *budget -= taken;
                taken
            };
            if taken == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "injected sink refusal",
                ));
            }
            self.accepted.write(&bytes[..taken])
        }

        fn flush(&mut self) -> io::Result<()> {
            self.accepted.flush()
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

    /// Pins the schema-1 tokens a supervisor parses, so a variant rename that
    /// changes the wire vocabulary has to be a deliberate protocol change.
    #[test]
    fn artifact_state_tokens_are_the_schema_1_spellings() {
        assert_eq!(
            ARTIFACT_STATES.map(artifact_state).as_slice(),
            [
                "staging",
                "ready",
                "promoting",
                "published",
                "visible_unsynchronized",
                "unpublished",
            ]
        );
    }

    #[test]
    fn artifact_state_fallback_matches_the_serde_token() {
        for state in ARTIFACT_STATES {
            assert_eq!(snake_case(&format!("{state:?}")), artifact_state(state));
        }
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
        let state = state_writing_to(Box::new(sink.clone()));
        (state, sink)
    }

    fn state_writing_to(writer: Box<dyn Write + Send>) -> MachineState {
        MachineState {
            writer,
            pending: None,
            batch_id: "batch".to_owned(),
            execution_id: "018f47a2-9a41-7a27-b4d6-4f7137e3c159".to_owned(),
            sequence: 0,
            plan_identity: serde_json::json!({"status": "resolved"}),
            terminal_reserved: false,
            notice_reoffered: false,
            attempted_failure: None,
            observability: None,
            observability_progress: None,
            observability_admission: None,
            observability_arena: None,
            publication: None,
            publication_chunks_sent: 0,
            progress: BoundedProgress::default(),
        }
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

    /// The terminal slot must survive a sink refusal in the inventory window.
    ///
    /// A publication carries up to `MANIFEST_MAX_ARTIFACTS` artifacts, so a
    /// terminal can be preceded by dozens of records. Spending the slot before
    /// those writes meant one refused inventory record ended the run with no
    /// terminal on the stream at all, while every later attempt — `main()`'s
    /// retry and the unrepresentable-exit recovery — was told a terminal had
    /// been reported.
    #[test]
    fn an_inventory_write_failure_leaves_a_later_terminal_writable() {
        let sink = RefusingSink::new(1);
        let mut state = state_writing_to(Box::new(sink.clone()));
        let artifacts = (0..130)
            .map(|index| {
                serde_json::json!({
                    "artifact_id": format!("artifact-{index:08x}"),
                    "kind": "fan_out",
                    "state": "published",
                })
            })
            .collect::<Vec<_>>();
        let (artifact_events, summary) = publication_payloads_from_artifacts(true, 0, artifacts);
        assert_eq!(
            artifact_events.len(),
            3,
            "the terminal must be preceded by more than one inventory record"
        );
        let failure = FailureClassification::for_code("attempt.publication.promotion_failed")
            .expect("registered code");

        assert!(
            state
                .write_terminal_records(
                    "failed",
                    failure_fields(&failure, 4),
                    artifact_events,
                    Some(summary),
                )
                .is_err()
        );
        let refused = events(&sink.accepted);
        assert!(
            refused
                .iter()
                .all(|event| event["event"] == "publication_artifacts"),
            "the sink refused before the terminal: {refused:#?}"
        );
        assert!(
            !state.terminal_reserved,
            "an inventory record the sink refused must not spend the terminal slot"
        );

        // The attempt `main()` makes against the slot after the first one
        // failed. Before the slot was tied to a written record this wrote
        // nothing and returned success.
        sink.allow(16);
        assert_eq!(
            state
                .write_terminal_records("failed", failure_fields(&failure, 4), Vec::new(), None)
                .expect("the terminal slot is still free"),
            TerminalOutcome::Written
        );
        let stream = events(&sink.accepted);
        let terminal = stream.last().expect("terminal record");
        assert_eq!(terminal["event"], "failed");
        assert_eq!(terminal["exit_code"], 4);
        assert_eq!(
            terminal["failure"]["code"],
            "attempt.publication.promotion_failed"
        );

        // A terminal that did reach the stream is still written exactly once.
        assert_eq!(
            state
                .write_terminal_records("completed", serde_json::Map::new(), Vec::new(), None)
                .expect("a spent slot is not an error"),
            TerminalOutcome::AlreadyWritten
        );
        let settled = events(&sink.accepted);
        assert_eq!(settled.len(), stream.len());
        assert_eq!(
            settled
                .iter()
                .filter(|event| matches!(
                    event["event"].as_str(),
                    Some("completed" | "failed" | "cancelled")
                ))
                .count(),
            1,
            "exactly one terminal record: {settled:#?}"
        );
    }

    /// A terminal recovered after a refused inventory write keeps its
    /// publication.
    ///
    /// The retry is made from `main` with no publication argument, so the
    /// recovered terminal used to drop a summary that had been available and
    /// report no artifact evidence at all. A supervisor is told to infer
    /// nothing about the visible set when `publication` is absent, which made
    /// the attempt look unreconcilable for a reason that was purely an
    /// accident of which write the sink refused.
    #[test]
    fn a_terminal_recovered_after_an_inventory_failure_keeps_its_publication() {
        let sink = RefusingSink::new(1);
        let mut state = state_writing_to(Box::new(sink.clone()));
        let artifacts = (0..130)
            .map(|index| {
                serde_json::json!({
                    "artifact_id": format!("artifact-{index:08x}"),
                    "kind": "fan_out",
                    "state": "published",
                })
            })
            .collect::<Vec<_>>();
        let (artifact_events, summary) = publication_payloads_from_artifacts(true, 0, artifacts);
        let expected = summary.clone();

        assert!(
            state
                .write_terminal_for(
                    "completed",
                    serde_json::Map::from_iter([
                        ("result".to_owned(), serde_json::json!("success")),
                        ("exit_code".to_owned(), serde_json::json!(0)),
                    ]),
                    Some(TerminalPublication {
                        artifact_events,
                        summary,
                    }),
                )
                .is_err(),
            "the sink refuses inside the inventory window"
        );
        assert!(!state.terminal_reserved);

        // The retry `main` makes, which carries no publication of its own.
        sink.allow(16);
        assert_eq!(
            state
                .write_terminal_for(
                    "completed",
                    serde_json::Map::from_iter([
                        ("result".to_owned(), serde_json::json!("success")),
                        ("exit_code".to_owned(), serde_json::json!(0)),
                    ]),
                    None,
                )
                .expect("the terminal slot is still free"),
            TerminalOutcome::Written
        );

        let stream = events(&sink.accepted);
        let terminal = stream.last().expect("terminal record");
        assert_eq!(terminal["event"], "completed");
        assert_eq!(
            terminal["publication"], expected,
            "the recovered terminal reports the publication the first attempt carried: {stream:#?}"
        );
        assert_eq!(
            stream[stream.len() - 4..]
                .iter()
                .map(|event| event["event"].as_str().expect("event name"))
                .collect::<Vec<_>>(),
            [
                "publication_artifacts",
                "publication_artifacts",
                "publication_artifacts",
                "completed",
            ],
            "the retry re-sends the whole inventory ahead of the terminal it counts: {stream:#?}"
        );
    }

    /// A short write followed by a refusal must not produce two terminals.
    ///
    /// This is the case a supervisor pipe presents: it takes part of a record
    /// and then blocks. The terminal slot is spent by a terminal the sink
    /// took, so the slot is still free here and `main` retries — and the
    /// stream must end with one terminal record, not with the tail of the
    /// first one followed by a whole second copy.
    #[test]
    fn a_terminal_the_sink_took_only_part_of_is_never_written_twice() {
        let sink = ShortWritingSink::new(48);
        let mut state = state_writing_to(Box::new(sink.clone()));
        let failure = FailureClassification::for_code("attempt.publication.promotion_failed")
            .expect("registered code");

        assert!(
            state
                .write_terminal_records("failed", failure_fields(&failure, 4), Vec::new(), None)
                .is_err(),
            "the sink blocks partway through the terminal record"
        );
        let partial = sink.accepted_bytes();
        assert!(
            !partial.is_empty() && !partial.ends_with(b"\n"),
            "the sink took part of the record: {}",
            String::from_utf8_lossy(&partial)
        );
        assert!(
            !state.terminal_reserved,
            "a terminal the sink has not taken in full is not the run's terminal"
        );

        // The retry `main` makes against the still-free slot.
        sink.allow(usize::MAX);
        assert_eq!(
            state
                .write_terminal_records("failed", failure_fields(&failure, 4), Vec::new(), None)
                .expect("the refused terminal is completed rather than re-sent"),
            TerminalOutcome::AlreadyWritten
        );

        // `events` parses every line, so a record completed by a later write
        // and then written again would fail here as malformed JSON before the
        // count below could pass.
        let stream = events(&sink.accepted);
        assert_eq!(
            stream.len(),
            1,
            "one record was attempted, so one record is on the stream: {stream:#?}"
        );
        assert_eq!(stream[0]["event"], "failed");
        assert_eq!(stream[0]["seq"], 0);
        assert_eq!(
            sink.accepted_bytes()
                .iter()
                .filter(|byte| **byte == b'\n')
                .count(),
            1,
            "and it is terminated exactly once"
        );
    }

    /// A record the sink refused leaves the numbering dense.
    ///
    /// Both consumers of this stream require `seq` to increase by exactly one:
    /// the published contract says so, and the reference adapter reconciles a
    /// gap as an incomplete attempt. One discardable liveness record refused
    /// by a momentarily full pipe — which the worker deliberately swallows so
    /// a healthy run still exits 0 — would otherwise shift every later number
    /// and condemn the whole run.
    #[test]
    fn a_record_the_sink_refused_consumes_no_sequence_number() {
        let sink = RefusingSink::new(1);
        let mut state = state_writing_to(Box::new(sink.clone()));
        state
            .write_event("started", serde_json::Map::new())
            .expect("the sink takes the first record");
        assert_eq!(state.sequence, 1);

        assert!(
            state
                .write_event("plan_resolved", serde_json::Map::new())
                .is_err(),
            "the sink refuses the second"
        );
        assert_eq!(
            state.sequence, 1,
            "a number is consumed by a record that reached the sink, not by an attempt"
        );

        sink.allow(4);
        state
            .write_event("plan_resolved", serde_json::Map::new())
            .expect("a later record still writes");

        let stream = events(&sink.accepted);
        assert_eq!(
            stream
                .iter()
                .map(|event| event["seq"].as_u64().expect("sequence"))
                .collect::<Vec<_>>(),
            [0, 1],
            "the delivered stream is densely numbered from zero: {stream:#?}"
        );
    }

    /// A retried terminal does not re-send inventory the sink already took.
    ///
    /// Each chunk carries its index in the inventory, and a consumer is told
    /// to reassemble indices zero through `chunk_count - 1` in sequence. A
    /// retry that restarts the inventory puts two records with index zero on
    /// the stream, so a terminal whose content is correct is read as
    /// unreconcilable.
    #[test]
    fn a_retried_terminal_does_not_repeat_a_chunk_the_sink_took() {
        let sink = RefusingSink::new(1);
        let mut state = state_writing_to(Box::new(sink.clone()));
        let artifacts = (0..130)
            .map(|index| {
                serde_json::json!({
                    "artifact_id": format!("artifact-{index:08x}"),
                    "kind": "fan_out",
                    "state": "published",
                })
            })
            .collect::<Vec<_>>();
        let (artifact_events, summary) = publication_payloads_from_artifacts(true, 0, artifacts);
        assert_eq!(artifact_events.len(), 3);

        assert!(
            state
                .write_terminal_for(
                    "completed",
                    serde_json::Map::from_iter([
                        ("result".to_owned(), serde_json::json!("success")),
                        ("exit_code".to_owned(), serde_json::json!(0)),
                    ]),
                    Some(TerminalPublication {
                        artifact_events,
                        summary,
                    }),
                )
                .is_err(),
            "the sink takes the first chunk and refuses the second"
        );

        sink.allow(16);
        assert_eq!(
            state
                .write_terminal_for(
                    "completed",
                    serde_json::Map::from_iter([
                        ("result".to_owned(), serde_json::json!("success")),
                        ("exit_code".to_owned(), serde_json::json!(0)),
                    ]),
                    None,
                )
                .expect("the terminal slot is still free"),
            TerminalOutcome::Written
        );

        let stream = events(&sink.accepted);
        let chunks = stream
            .iter()
            .filter(|event| event["event"] == "publication_artifacts")
            .collect::<Vec<_>>();
        assert_eq!(
            chunks
                .iter()
                .map(|event| event["publication"]["chunk_index"].as_u64().expect("index"))
                .collect::<Vec<_>>(),
            [0, 1, 2],
            "every chunk index appears exactly once, in order: {stream:#?}"
        );
        assert!(
            chunks
                .iter()
                .all(|event| event["publication"]["chunk_count"] == 3),
            "and each states the same inventory size: {stream:#?}"
        );
        assert_eq!(
            stream
                .iter()
                .map(|event| event["seq"].as_u64().expect("sequence"))
                .collect::<Vec<_>>(),
            [0, 1, 2, 3]
        );
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
