//! OpenLineage emission for a run: where the events go, and what is reported
//! when they cannot get there.
//!
//! Behind the `lineage` feature. `clinker-lineage` builds the events; this
//! module owns the CLI side of them — resolving `--lineage` / `--lineage-events`
//! against deployment policy, opening and truncating a destination, holding the
//! live sink open across the run, and turning a refused write into a diagnostic
//! that says whether re-running unchanged is worth anything.
//!
//! The glob import is what keeps this a move rather than a rewrite: these items
//! were factored out of the crate root and still share its error constructors,
//! lifecycle vocabulary and machine emitter.

use crate::*;

pub(crate) fn lineage_worker_start_error(_error: std::io::Error) -> PipelineError {
    observability_delivery_error(
        "the bounded lineage exporter could not start before execution. Correction: reduce host resource pressure or disable external lineage delivery",
    )
}

/// How many dropped events a diagnostic names before it summarizes the rest.
pub(crate) const LINEAGE_EXPORT_REPORTED_DROPS: usize = 4;

/// Remove a lineage destination this run truncated and never wrote to.
///
/// Only a regular file. A destination that always reports zero length —
/// `/dev/null`, a FIFO, any character device — is a successful export, not an
/// empty one, and unlinking it would take the device node with it on a
/// container running as root.
///
/// The caller must not use this on a verdict that leaves the export worker
/// running: a detached worker re-creates the destination on its next write,
/// which would put a truncated event stream back after the removal.
///
/// Reports whether the operator is still left holding an export, so a
/// diagnostic that describes the destination's state describes one that exists.
pub(crate) fn remove_empty_lineage_export(
    path: &std::path::Path,
) -> (LineageExportRemains, Option<PipelineError>) {
    // Standard output is not a path. Resolving it as one makes an ordinary
    // file named `-` in the working directory this run's destination, and a
    // stdout export would unlink a file the operator never named.
    if path.as_os_str() == std::ffi::OsStr::new("-") {
        return (LineageExportRemains::Kept, None);
    }
    // `symlink_metadata`, so a symlink is not a regular file here. Following
    // it would report the target's size and then unlink the link instead of
    // the target, destroying the operator's configured path while leaving the
    // empty artifact exactly where it was. A symlinked destination is left
    // alone: an ambiguous empty file is a smaller harm than a broken link.
    let Some(metadata) = std::fs::symlink_metadata(path).ok() else {
        return (LineageExportRemains::Kept, None);
    };
    if !metadata.is_file() || metadata.len() != 0 {
        return (LineageExportRemains::Kept, None);
    }
    // A removal that fails leaves the empty file exactly where it was, so this
    // still reports an export the operator holds — an empty one, which ends on
    // a record boundary as truthfully as any other.
    match std::fs::remove_file(path) {
        Ok(()) => (LineageExportRemains::Removed, None),
        Err(error) => (
            LineageExportRemains::Kept,
            Some(observability_delivery_error(format!(
                "--lineage output {} is empty and could not be removed: {error}. Correction: delete it before publishing this run's artifacts, so an empty file is not read as its lineage export",
                path.display()
            ))),
        ),
    }
}

/// Whether a failed `--lineage` export left the operator holding a destination.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum LineageExportRemains {
    /// Bytes are on the destination, and a publish step would pick them up.
    Kept,
    /// The destination was empty and is gone; there is no file to describe.
    Removed,
}

pub(crate) fn lineage_destination(path: &std::path::Path) -> String {
    if path.as_os_str() == std::ffi::OsStr::new("-") {
        "standard output".to_owned()
    } else {
        path.display().to_string()
    }
}

pub(crate) fn lineage_admission_reason(
    admission: clinker_lineage::LineageAdmission,
) -> &'static str {
    match admission {
        clinker_lineage::LineageAdmission::Accepted => "was admitted",
        clinker_lineage::LineageAdmission::DroppedEventTooLarge => {
            "serialized larger than `observability.lineage.max_event_bytes`"
        }
        clinker_lineage::LineageAdmission::DroppedEncodingFailed => "could not be serialized",
        clinker_lineage::LineageAdmission::DroppedQueueFull => {
            "did not fit the remaining `observability.lineage.queue_bytes` capacity"
        }
        clinker_lineage::LineageAdmission::DroppedProducerBusy => {
            "found the export queue momentarily locked"
        }
        clinker_lineage::LineageAdmission::DroppedShutdown => {
            "arrived after the export worker had already stopped"
        }
    }
}

/// Diagnose a plan-only `--lineage` export that did not deliver every event.
///
/// Returns `None` only when every event was admitted and the worker drained
/// and flushed the destination. This export is the whole invocation, so a
/// partial or unwritten one is a run failure — the opposite of the
/// `--lineage-events` bulkhead alongside a live run, whose delivery outcome
/// deliberately cannot redefine run truth.
pub(crate) fn lineage_export_failure(
    path: &std::path::Path,
    expected: usize,
    rejected: &[(usize, clinker_lineage::LineageAdmission)],
    outcome: clinker_lineage::LineageDeliveryOutcome,
) -> Option<PipelineError> {
    let destination = lineage_destination(path);
    // The destination was emptied once the exporter started, so an export that
    // then wrote nothing leaves a zero-byte file for a publish step to upload
    // as though it were the export. Skipped on `DeadlineExceeded`, the one
    // verdict that returns while the worker is still running: removing the file
    // there would race a writer that re-creates it.
    //
    // A failure to remove is reported only when nothing else is — every verdict
    // below already tells the operator the export did not complete, and that is
    // the more useful of the two diagnostics.
    let (remains, removal) = match outcome.terminal() {
        clinker_lineage::LineageDeliveryTerminal::DeadlineExceeded => {
            (LineageExportRemains::Kept, None)
        }
        _ => remove_empty_lineage_export(path),
    };
    match outcome.terminal() {
        // Whether the same invocation could ever succeed is what separates
        // these, and the diagnostic has to say the same thing its retry advice
        // does. A destination the process may not write will refuse every
        // identical retry, so it is a configuration failure and the correction
        // names a different destination. A reader that went away or a write
        // that timed out may not recur, so it stays a delivery failure the
        // supervisor is free to retry.
        clinker_lineage::LineageDeliveryTerminal::WriteFailed(kind)
        | clinker_lineage::LineageDeliveryTerminal::FlushFailed(kind) => {
            // A sink that refused a write is not the same fact as the state it
            // was left in, and this path can leave either of the two files the
            // deadline path can: a refusal that transferred nothing stops on a
            // record boundary, one that transferred part of a record does not.
            // Nothing about the destination distinguishes them from the outside,
            // so the run has to say which — the same `records_complete` fact,
            // exact here because the export sink is unbuffered and every record
            // is newline-framed, so the bytes it accepted are the bytes on the
            // destination.
            //
            // What it cannot borrow from the deadline path is the correction.
            // There the export was going fine and ran out of time, so re-running
            // is the whole answer; here the destination itself refused, and where
            // the retry should point is already decided below by whether the
            // refusal is permanent. This adds only the disposal of what is
            // already on the destination, which that choice does not cover.
            let (state, disposal) = match (remains, outcome.records_complete()) {
                (LineageExportRemains::Removed, _) => ("", ""),
                (LineageExportRemains::Kept, true) => (
                    "; the export it left ends on a record boundary and is readable as NDJSON",
                    "",
                ),
                (LineageExportRemains::Kept, false) => (
                    "; the export it left ends inside a record and is not readable as NDJSON",
                    "discard that partial export rather than publishing it as this run's lineage, then ",
                ),
            };
            let observed = format!(
                "cannot write --lineage output {destination}: the export sink reported {} ({}) after {} of {expected} events entered the export queue{state}. Correction: {disposal}",
                outcome.terminal().as_str(),
                lineage_error_kind(kind),
                outcome.accepted(),
            );
            return Some(if is_permanent_sink_refusal(kind) {
                observability_configuration_error(format!(
                    "{observed}point --lineage at a writable destination, for example `--lineage ./lineage.ndjson`"
                ))
            } else {
                observability_delivery_error(format!(
                    "{observed}re-run the export; if it recurs, point --lineage at a different destination"
                ))
            });
        }
        // A collector that was merely slow this time is the same phenomenon
        // `FlushFailed(TimedOut)` reports one layer down, and one slow
        // collector must not produce two opposite instructions depending on
        // which layer noticed it. Retryable, with a correction that offers the
        // deadline as the fix if it keeps happening.
        clinker_lineage::LineageDeliveryTerminal::DeadlineExceeded => {
            // This is the one verdict that keeps what it wrote, so the operator
            // is left holding a file — and the two files this path can leave
            // want opposite handling. One is short: valid NDJSON missing its
            // tail, which a catalogue can still read and a diff can still
            // compare. The other stops inside a record, and every conformant
            // reader fails on it. Nothing about the destination distinguishes
            // them from the outside, which is why the run has to say which.
            let (state, correction) = if outcome.records_complete() {
                (
                    "the partial export it left ends on a record boundary, short by the events that never got out",
                    "re-run the export",
                )
            } else {
                (
                    "the partial export it left ends inside a record and is not readable as NDJSON",
                    "discard that partial export rather than publishing it as this run's lineage, then re-run",
                )
            };
            return Some(observability_delivery_error(format!(
                "--lineage output {destination} did not finish writing within the configured lineage flush deadline; {state}. Correction: {correction}; if it recurs, raise the deadline in clinker.toml:\n\n  [observability.lineage]\n  flush_timeout_ms = 30000"
            )));
        }
        clinker_lineage::LineageDeliveryTerminal::Shutdown => {}
    }
    if rejected.is_empty() {
        return removal;
    }

    let mut listed = rejected
        .iter()
        .take(LINEAGE_EXPORT_REPORTED_DROPS)
        .map(|(index, admission)| {
            format!(
                "event {} {}",
                index + 1,
                lineage_admission_reason(*admission)
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    if let Some(remaining) = rejected.len().checked_sub(LINEAGE_EXPORT_REPORTED_DROPS)
        && remaining > 0
    {
        listed.push_str(&format!("; and {remaining} more"));
    }

    // Only the byte caps have a configuration correction. Do not offer one for
    // a serializer failure or a stopped worker: raising a cap would not change
    // the outcome and the author would stop looking for the real cause.
    let capped = rejected.iter().any(|(_, admission)| {
        matches!(
            admission,
            clinker_lineage::LineageAdmission::DroppedEventTooLarge
                | clinker_lineage::LineageAdmission::DroppedQueueFull
        )
    });
    let detail = format!(
        "--lineage output {destination} is incomplete: {} of {expected} events were dropped before delivery ({listed}). Correction: ",
        rejected.len(),
    );
    // The classification has to agree with the correction. A breached cap is
    // answered by editing the caps, so it is a configuration failure and must
    // not be retried unchanged; a serializer failure or a momentarily locked
    // queue is answered by running it again, and filing that as invalid
    // configuration would tell a supervisor to refuse the retry the diagnostic
    // just asked for.
    if capped {
        Some(observability_configuration_error(format!(
            "{detail}raise the lineage caps in clinker.toml:\n\n  [observability.lineage]\n  queue_bytes = \"1MB\"\n  max_event_bytes = \"1MB\""
        )))
    } else {
        Some(observability_delivery_error(format!(
            "{detail}re-run the export; if it recurs, remove --lineage and report the diagnostic above"
        )))
    }
}

pub(crate) enum CliLineageIdentity {
    External(clinker_lineage::LineageIdentityContext),
    LocalDiagnosticPaths,
}

pub(crate) struct CliLineageConfiguration {
    pub(crate) identity: CliLineageIdentity,
    pub(crate) delivery: Option<clinker_lineage::LineageDeliveryConfig>,
}

pub(crate) fn resolve_cli_lineage_configuration(
    observability: &clinker_plan::config::ResolvedObservabilityPolicy,
) -> Result<CliLineageConfiguration, PipelineError> {
    let lineage = observability.lineage().ok_or_else(|| {
        observability_configuration_error(
            "lineage export requires an explicit [observability.lineage] identity policy",
        )
    })?;
    match lineage.identity_mode() {
        clinker_plan::config::LineageIdentityMode::External => {
            let identity = clinker_lineage::LineageIdentityContext::from_resolved(lineage)
                .map_err(observability_configuration_error)?;
            let delivery = clinker_lineage::LineageDeliveryConfig::from_resolved(lineage)
                .map_err(observability_configuration_error)?;
            Ok(CliLineageConfiguration {
                identity: CliLineageIdentity::External(identity),
                delivery: Some(delivery),
            })
        }
        clinker_plan::config::LineageIdentityMode::LocalDiagnosticPaths => {
            eprintln!(
                "clinker: lineage identity mode: local_diagnostic_paths (local diagnostic compatibility only)"
            );
            Ok(CliLineageConfiguration {
                identity: CliLineageIdentity::LocalDiagnosticPaths,
                delivery: None,
            })
        }
    }
}

pub(crate) fn build_cli_lineage(
    compiled: &clinker_plan::plan::CompiledPlan,
    identity: &CliLineageIdentity,
    base_dir: &std::path::Path,
) -> Result<clinker_lineage::PlanColumnLineage, PipelineError> {
    match identity {
        CliLineageIdentity::External(context) => {
            clinker_lineage::column_lineage_external(compiled, context)
                .map_err(observability_configuration_error)
        }
        CliLineageIdentity::LocalDiagnosticPaths => Ok(
            clinker_lineage::column_lineage_local_diagnostic_paths(compiled, base_dir),
        ),
    }
}

/// A destination opened on first write rather than at admission.
///
/// Used for anything that is not a regular file. Opening a FIFO for writing
/// blocks until a reader attaches, so doing it during admission — on the run's
/// own thread, before discovery — hangs a run whose collector connects a moment
/// later. A regular file is still opened eagerly, because proving it writable
/// before the run stages anything is the point of doing it there.
pub(crate) struct LazyLineageFile {
    path: PathBuf,
    file: Option<std::fs::File>,
}

impl LazyLineageFile {
    /// Name a destination whose open is deferred to the first write.
    pub(crate) const fn deferred(path: PathBuf) -> Self {
        Self { path, file: None }
    }
}

impl std::io::Write for LazyLineageFile {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let file = match self.file {
            Some(ref mut file) => file,
            None => self.file.insert(std::fs::File::create(&self.path)?),
        };
        std::io::Write::write(file, bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.file.as_mut() {
            Some(file) => std::io::Write::flush(file),
            None => Ok(()),
        }
    }
}

pub(crate) enum LiveLineageSink {
    External(clinker_lineage::LineageDelivery),
    LocalDiagnostic(Box<dyn std::io::Write>),
}

pub(crate) struct LiveLineageOutput {
    sink: LiveLineageSink,
    lineage: clinker_lineage::PlanColumnLineage,
    job: clinker_lineage::Job,
    /// Whether the consumer was told this run began.
    ///
    /// The bounded sink can refuse an event, so a START is not guaranteed to
    /// arrive. A terminal for a run the consumer never saw start describes
    /// nothing it can attach to, so the pairing is tracked rather than assumed.
    started: bool,
    /// The admitted START, kept so a run that never reaches its terminal can
    /// still be closed against the identity the consumer already has.
    start_facts: Option<clinker_lineage::RunLifecycleStartFacts>,
    /// Whether a terminal has been accepted by the sink for this run.
    terminal_emitted: bool,
    /// This run's real terminal, when the sink refused it. Re-offered before
    /// any synthetic close, so a completed run is never reported as failed.
    refused_terminal: Option<clinker_lineage::RunEvent>,
}

impl LiveLineageOutput {
    /// Open a live output over an already-constructed sink.
    ///
    /// The four state fields start at "nothing has been emitted", which is the
    /// only admissible starting point: they exist to pair a terminal with a
    /// START the sink actually took, and a caller that could set them could
    /// claim a pairing that never happened.
    pub(crate) const fn new(
        sink: LiveLineageSink,
        lineage: clinker_lineage::PlanColumnLineage,
        job: clinker_lineage::Job,
    ) -> Self {
        Self {
            sink,
            lineage,
            job,
            started: false,
            start_facts: None,
            terminal_emitted: false,
            refused_terminal: None,
        }
    }

    pub(crate) fn emit_start(&mut self, start: &RunLifecycleStartFacts) {
        let facts = lineage_start_facts(start);
        let event = clinker_lineage::start_event(&self.lineage, self.job.clone(), &facts);
        // A failed write is reported, never propagated. Lineage is an optional
        // observation of the run, and the external sink already treats a
        // refusal this way — propagating the local sink's error instead made a
        // healthy pipeline exit 4 because something downstream closed the pipe
        // it was writing events to, and made success depend on which identity
        // mode was configured rather than on the data.
        self.started = match self.emit(event) {
            Ok(admitted) => admitted,
            Err(error) => {
                tracing::warn!(error = %error, "this run's lineage START could not be written");
                false
            }
        };
        self.start_facts = Some(facts);
    }

    pub(crate) fn emit_terminal(
        &mut self,
        snapshot: &RunLifecycleSnapshot,
    ) -> Result<(), PipelineError> {
        // A refused START makes this run's record partial; withholding the
        // terminal too would make it empty. The terminal is the event that
        // carries the column lineage and the run statistics, and events are
        // keyed by run identity rather than by arrival order, so a consumer can
        // attach one whose START it never received. Report the gap and send it.
        if !self.started {
            tracing::warn!(
                "this run's lineage START was never admitted; its terminal describes a run the consumer has no start record for"
            );
        }
        let facts = lineage_lifecycle_facts(snapshot)?;
        let event = clinker_lineage::terminal_event(&self.lineage, self.job.clone(), &facts);
        // Marked emitted only if the sink took it. Setting the flag first
        // meant a terminal the bounded queue refused still disabled the
        // fallback that exists to close the run, so the consumer kept a START
        // with no terminal — the state that fallback was written to prevent.
        //
        // The refused event is kept rather than discarded: it carries this
        // run's real outcome, and closing a completed run with the synthetic
        // failure would trade an open run for a wrong one.
        match self.emit(event.clone()) {
            // Refused, not failed: the sink is alive and said no. Keep the
            // event so a drained queue can still take this run's real outcome.
            Ok(false) => {
                self.refused_terminal = Some(event);
                Ok(())
            }
            Ok(true) => {
                self.terminal_emitted = true;
                Ok(())
            }
            // The write itself failed. This run produced a terminal and the
            // sink could not take it, so the fallback must not append a
            // synthetic failure describing a different outcome — a completed
            // run would be published as failed. Record the attempt and report
            // the error.
            Err(error) => {
                self.terminal_emitted = true;
                Err(PipelineError::Io(error))
            }
        }
    }

    /// Offer one event to the sink. `Ok(false)` means it was refused.
    fn emit(&mut self, event: clinker_lineage::RunEvent) -> std::io::Result<bool> {
        match &mut self.sink {
            LiveLineageSink::External(delivery) => {
                // Whether the consumer ever sees this event is the admission's
                // answer, and it was being discarded — so a refused event was
                // indistinguishable from a delivered one, and the pairing below
                // could not be maintained. Delivery stays optional: a refusal
                // is reported, never propagated, because an observation of the
                // run must not decide the run.
                let admission = delivery.try_emit(&event);
                if admission != clinker_lineage::LineageAdmission::Accepted {
                    tracing::warn!(
                        admission = ?admission,
                        "lineage event was refused by the bounded sink"
                    );
                    return Ok(false);
                }
                #[cfg(debug_assertions)]
                if std::env::var_os("CLINKER_TEST_LINEAGE_REPEAT").as_deref()
                    == Some(std::ffi::OsStr::new("64"))
                {
                    for _ in 1..64 {
                        let _ = delivery.try_emit(&event);
                    }
                }
                Ok(true)
            }
            LiveLineageSink::LocalDiagnostic(writer) => {
                clinker_lineage::write_ndjson(std::slice::from_ref(&event), writer).map(|()| true)
            }
        }
    }

    fn finish(&mut self) -> Option<clinker_lineage::LineageDeliveryOutcome> {
        // Before the sink goes away, so a run that reached here without a
        // terminal is still closed against the consumer that saw its START.
        self.close_open_run();
        let sink = std::mem::replace(
            &mut self.sink,
            LiveLineageSink::LocalDiagnostic(Box::new(std::io::sink())),
        );
        match sink {
            LiveLineageSink::External(delivery) => Some(delivery.finish()),
            LiveLineageSink::LocalDiagnostic(_) => None,
        }
    }

    /// Emit a terminal for a run the consumer was told had begun and that
    /// never reported how it ended.
    ///
    /// A run that ended without saying how did not succeed, so this reports a
    /// failure under the invariant code and carries no statistics, because
    /// none were observed. Doing nothing instead leaves a lone START, and a
    /// catalogue shows the run as still executing forever.
    fn close_open_run(&mut self) {
        if self.terminal_emitted {
            return;
        }
        // The run's own terminal, if the sink refused it earlier. Offered
        // again before anything else, and regardless of whether the START was
        // admitted: `emit_terminal` sends it either way because it carries the
        // column lineage and the run statistics, and a consumer attaches it by
        // run identity. Gating this on the START meant a run whose START and
        // terminal were both refused by a momentarily full queue delivered
        // nothing at all, even once the queue had drained.
        if let Some(event) = self.refused_terminal.take() {
            if self.emit(event).unwrap_or(false) {
                self.terminal_emitted = true;
                return;
            }
            tracing::warn!("lineage run left open: its own terminal was refused twice");
            return;
        }
        // Nothing of this run's own to send. A synthetic close is only
        // meaningful to a consumer that saw the run begin.
        if !self.started {
            return;
        }
        let Some(start) = self.start_facts.clone() else {
            return;
        };
        let facts = clinker_lineage::RunLifecycleFacts {
            start,
            terminal: clinker_lineage::RunLifecycleTerminalFacts {
                event_time: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                outcome: clinker_lineage::Terminal::Fail {
                    failure: clinker_core_types::FailureClassification::unknown_internal(
                        "the run ended without recording a terminal",
                    ),
                },
                stats: None,
            },
        };
        let event = clinker_lineage::terminal_event(&self.lineage, self.job.clone(), &facts);
        // Same rule as the real terminal: the flag records what the sink took,
        // not what was offered. Setting it first and then checking only for an
        // error let a refusal — the sink alive and saying no — be dropped while
        // the run was recorded as closed.
        match self.emit(event) {
            Ok(true) => self.terminal_emitted = true,
            Ok(false) => tracing::warn!(
                "lineage run left open: its closing event was refused by the export queue"
            ),
            Err(error) => tracing::warn!(
                error = %error,
                "lineage run left open: its closing event could not be written"
            ),
        }
    }
}

/// Say on standard error what the lineage export lost, and what state it left
/// its destination in.
///
/// `records_complete` is the destination's own completeness, and it is the one
/// fact a consumer cannot recover for itself: a file that stops inside a record
/// simply ends, with nothing in it to say that more was coming. The counters
/// beside it cannot answer that either — a run that gave up on a slow
/// destination reports the same accepted total whether the last record made it
/// out whole or not.
///
/// An incomplete destination breaks the clean-run silence on its own, exactly
/// as incomplete counts do on the telemetry line. It is not a count of anything
/// lost, so a run that dropped nothing would otherwise report nothing while
/// leaving an unreadable file behind.
pub(crate) fn report_lineage_delivery(outcome: clinker_lineage::LineageDeliveryOutcome) {
    if outcome.terminal() != clinker_lineage::LineageDeliveryTerminal::Shutdown
        || outcome.dropped() > 0
        || !outcome.records_complete()
    {
        let error_kind = match outcome.terminal() {
            clinker_lineage::LineageDeliveryTerminal::WriteFailed(kind)
            | clinker_lineage::LineageDeliveryTerminal::FlushFailed(kind) => {
                lineage_error_kind(kind)
            }
            clinker_lineage::LineageDeliveryTerminal::Shutdown
            | clinker_lineage::LineageDeliveryTerminal::DeadlineExceeded => "none",
        };
        eprintln!(
            "clinker: lineage delivery outcome: status={} error_kind={} accepted={} dropped={} full={} records_complete={}",
            outcome.terminal().as_str(),
            error_kind,
            outcome.accepted(),
            outcome.dropped(),
            outcome.full(),
            outcome.records_complete()
        );
    }
}
/// Whether a sink that refused a write will refuse the identical retry.
///
/// The distinction decides the retry advice a supervisor acts on. Permission,
/// a missing directory, a read-only filesystem, and a path that is the wrong
/// kind of thing are properties of the destination rather than of the moment,
/// so re-running unchanged is wasted work.
///
/// A full disk is deliberately not here. It reads like a permanent property
/// and is usually a temporary one: the run that filled the volume, or a
/// neighbour on it, releases the space and the next attempt succeeds. Telling
/// an operator to choose a different destination for it would be advice about
/// the wrong thing.
pub(crate) fn is_permanent_sink_refusal(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::PermissionDenied
            | std::io::ErrorKind::NotFound
            | std::io::ErrorKind::ReadOnlyFilesystem
            | std::io::ErrorKind::IsADirectory
            | std::io::ErrorKind::InvalidInput
    )
}

/// Name the observed failure for the operator.
///
/// Every kind [`is_permanent_sink_refusal`] treats as permanent appears here:
/// those are the ones whose diagnostic tells the author to choose a different
/// destination, and a correction is only actionable next to the reason for it.
pub(crate) fn lineage_error_kind(kind: std::io::ErrorKind) -> &'static str {
    match kind {
        std::io::ErrorKind::PermissionDenied => "permission-denied",
        std::io::ErrorKind::NotFound => "not-found",
        std::io::ErrorKind::ReadOnlyFilesystem => "read-only-filesystem",
        std::io::ErrorKind::StorageFull => "storage-full",
        std::io::ErrorKind::IsADirectory => "is-a-directory",
        std::io::ErrorKind::InvalidInput => "invalid-input",
        std::io::ErrorKind::BrokenPipe => "broken-pipe",
        std::io::ErrorKind::WriteZero => "write-zero",
        std::io::ErrorKind::TimedOut => "timed-out",
        _ => "other",
    }
}

impl Drop for LiveLineageOutput {
    /// Close a run the consumer was told had begun.
    ///
    /// Every ordinary path emits its own terminal and finishes the exporter.
    /// What this covers is the one that reaches neither: an early return
    /// between START and the point where the run reports how it ended, which
    /// otherwise leaves a lone START and a catalogue showing the run as
    /// executing forever.
    ///
    /// It runs the whole finish rather than only emitting, because the
    /// external exporter delivers on its own thread: enqueuing a terminal and
    /// returning would drop the queue and detach that thread, and the process
    /// would exit before the event this exists to guarantee had been written.
    ///
    /// Unwinding is not covered. The release profile aborts on panic, so there
    /// is no unwind for a destructor to run during.
    fn drop(&mut self) {
        if let Some(outcome) = self.finish() {
            report_lineage_delivery(outcome);
        }
    }
}

pub(crate) fn finish_live_lineage(output: &mut Option<LiveLineageOutput>) {
    if let Some(outcome) = output.as_mut().and_then(LiveLineageOutput::finish) {
        report_lineage_delivery(outcome);
    }
    output.take();
}

pub(crate) struct ExternalLineageFileSink {
    path: PathBuf,
    file: Option<std::fs::File>,
}

impl ExternalLineageFileSink {
    fn file(&mut self) -> std::io::Result<&mut std::fs::File> {
        if self.file.is_none() {
            self.file = Some(std::fs::File::create(&self.path)?);
        }
        self.file
            .as_mut()
            .ok_or_else(|| std::io::Error::other("lineage file sink initialization failed"))
    }
}

impl std::io::Write for ExternalLineageFileSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        std::io::Write::write(self.file()?, bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self.file.as_mut() {
            Some(file) => std::io::Write::flush(file),
            None => Ok(()),
        }
    }
}

/// Empty the `--lineage` destination now that the export worker is running.
///
/// Deliberately after the worker starts, not before: a worker that fails to
/// start has produced no effect, and the run is refused with the filesystem
/// exactly as it was. Once the worker is up the run is committed to writing
/// here, and emptying the file at that point is what stops a run that then
/// fails before its first event from leaving the previous run's events — whose
/// COMPLETE terminal a catalogue would attribute to this run.
pub(crate) fn truncate_lineage_destination(
    flag: &'static str,
    path: &std::path::Path,
) -> Result<(), PipelineError> {
    if path.as_os_str() == std::ffi::OsStr::new("-") {
        return Ok(());
    }
    // Skipped only for a destination whose open can block. Opening a FIFO for
    // writing waits for a reader, and this runs on the run's own thread during
    // admission, so a destination wired to a collector that connects a moment
    // later would hang the run before it read a record. A pipe or device has no
    // previous contents to strip anyway.
    //
    // Resolved through symlinks deliberately. A link to a regular file is a
    // regular destination — a "current run" indirection is exactly that — and
    // treating the link itself as non-regular would leave the previous run's
    // COMPLETE terminal in the target for a catalogue to attribute to this run.
    if std::fs::metadata(path).is_ok_and(|metadata| !metadata.is_file() && !metadata.is_dir()) {
        return Ok(());
    }
    std::fs::File::create(path)
        .map(|_| ())
        .map_err(|e| lineage_open_error(flag, path, &e))
}

/// Classify a destination this run could not open.
///
/// Every site that opens a lineage destination reports through here, so one
/// operator-visible condition cannot produce two exit classes depending on
/// which identity mode or which flag reached it first.
pub(crate) fn lineage_open_error(
    flag: &'static str,
    path: &std::path::Path,
    error: &std::io::Error,
) -> PipelineError {
    let detail = format!("cannot open {flag} output {}: {error}", path.display());
    {
        // The same split the write path uses: a destination the process may
        // never write refuses every identical retry and is a configuration
        // failure, while a full volume or a momentarily unavailable mount is
        // one a supervisor should try again. Classifying every open failure as
        // configuration would tell it to give up on both.
        if is_permanent_sink_refusal(error.kind()) {
            // Through the helper, not a bare validation error: the leading code
            // is what `classify_pipeline_error` matches to report this as an
            // observability problem rather than an invalid pipeline, which is
            // what decides whose queue the alert lands in.
            observability_configuration_error(format!(
                "{detail}. Correction: point {flag} at a writable destination"
            ))
        } else {
            observability_delivery_error(format!(
                "{detail}. Correction: re-run the export; if it recurs, point {flag} at a different destination"
            ))
        }
    }
}

#[cfg(debug_assertions)]
pub(crate) struct QualificationLineageSink {
    inner: Box<dyn std::io::Write + Send>,
    mode: QualificationLineageSinkMode,
    blocked: bool,
    writes: usize,
}

#[cfg(debug_assertions)]
#[derive(Clone, Copy)]
pub(crate) enum QualificationLineageSinkMode {
    PermissionDenied,
    WriteFailed,
    /// Takes one whole record, then refuses. Leaves a destination that stops on
    /// a record boundary — the readable half of what a write failure can leave.
    WriteFailedAfterRecord,
    /// Takes part of a record, then refuses. Leaves a destination that stops
    /// inside a record — the half no conformant NDJSON reader can finish.
    WriteFailedMidRecord,
    FlushFailed,
    HangAfterFirstWrite,
}

#[cfg(debug_assertions)]
impl std::io::Write for QualificationLineageSink {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.writes += 1;
        let mut bytes = bytes;
        match self.mode {
            QualificationLineageSinkMode::PermissionDenied => {
                return Err(std::io::Error::from(std::io::ErrorKind::PermissionDenied));
            }
            QualificationLineageSinkMode::WriteFailed => {
                return Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
            }
            QualificationLineageSinkMode::WriteFailedAfterRecord => {
                if self.writes > 1 {
                    return Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
                }
            }
            QualificationLineageSinkMode::WriteFailedMidRecord => {
                if self.writes > 1 {
                    return Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe));
                }
                // A short accepted count is what a destination reports when it
                // takes part of a record, and the caller is obliged to offer the
                // rest in a further call — which this then refuses.
                bytes = &bytes[..bytes.len().div_ceil(2)];
            }
            QualificationLineageSinkMode::FlushFailed
            | QualificationLineageSinkMode::HangAfterFirstWrite => {}
        }
        self.inner.write_all(bytes)?;
        self.inner.flush()?;
        if matches!(self.mode, QualificationLineageSinkMode::HangAfterFirstWrite) && !self.blocked {
            self.blocked = true;
            eprintln!("clinker: lineage sink received bytes; blocking for qualification");
            let released = std::sync::Mutex::new(false);
            let release = std::sync::Condvar::new();
            let mut released = released
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            while !*released {
                released = release
                    .wait(released)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()?;
        if matches!(self.mode, QualificationLineageSinkMode::FlushFailed) {
            Err(std::io::Error::from(std::io::ErrorKind::WriteZero))
        } else {
            Ok(())
        }
    }
}

pub(crate) fn external_lineage_sink(path: &std::path::Path) -> Box<dyn std::io::Write + Send> {
    let sink: Box<dyn std::io::Write + Send> = if path.as_os_str() == std::ffi::OsStr::new("-") {
        Box::new(std::io::stdout())
    } else {
        // Created and emptied here, while the run is being admitted, rather
        // than on the export worker at its first event. A run refused before it
        // emits anything would otherwise leave the previous run's file intact,
        // COMPLETE terminal and all, for a catalogue to read as this run having
        // succeeded.
        Box::new(ExternalLineageFileSink {
            path: path.to_owned(),
            file: None,
        })
    };
    #[cfg(debug_assertions)]
    if let Some(mode) = std::env::var_os("CLINKER_TEST_LINEAGE_SINK").and_then(|mode| {
        match mode.to_string_lossy().as_ref() {
            "permission-denied" => Some(QualificationLineageSinkMode::PermissionDenied),
            "write-failed" => Some(QualificationLineageSinkMode::WriteFailed),
            "write-failed-after-record" => {
                Some(QualificationLineageSinkMode::WriteFailedAfterRecord)
            }
            "write-failed-mid-record" => Some(QualificationLineageSinkMode::WriteFailedMidRecord),
            "flush-failed" => Some(QualificationLineageSinkMode::FlushFailed),
            "hang-after-first-write" => Some(QualificationLineageSinkMode::HangAfterFirstWrite),
            _ => None,
        }
    }) {
        return Box::new(QualificationLineageSink {
            inner: sink,
            mode,
            blocked: false,
            writes: 0,
        });
    }
    sink
}

pub(crate) fn lineage_start_facts(
    start: &RunLifecycleStartFacts,
) -> clinker_lineage::RunLifecycleStartFacts {
    let fingerprint = start.fingerprint();
    clinker_lineage::RunLifecycleStartFacts {
        batch_id: start.batch_id().to_owned(),
        execution_id: start.execution_id().to_owned(),
        plan_fingerprint_algorithm: fingerprint.algorithm().to_owned(),
        plan_fingerprint_version: fingerprint.version(),
        plan_fingerprint_digest: clinker_exec::output::sidecar::hash_to_hex(&fingerprint.digest()),
        event_time: start
            .started_at()
            .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
    }
}

pub(crate) fn lineage_lifecycle_facts(
    snapshot: &RunLifecycleSnapshot,
) -> Result<clinker_lineage::RunLifecycleFacts, PipelineError> {
    let terminal = snapshot.terminal().ok_or_else(|| PipelineError::Internal {
        op: "lineage lifecycle snapshot",
        node: "pipeline".to_owned(),
        detail: "terminal facts are unavailable".to_owned(),
    })?;
    let outcome = match terminal.outcome() {
        RunTerminalOutcome::Complete => clinker_lineage::Terminal::Complete,
        RunTerminalOutcome::Abort => clinker_lineage::Terminal::Abort,
        RunTerminalOutcome::Fail(failure) => clinker_lineage::Terminal::Fail {
            failure: failure.clone(),
        },
    };
    let counts = terminal.measured_counts();
    Ok(clinker_lineage::RunLifecycleFacts {
        start: lineage_start_facts(snapshot.start()),
        terminal: clinker_lineage::RunLifecycleTerminalFacts {
            event_time: terminal
                .finished_at()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            outcome,
            stats: counts.map(|counts| clinker_lineage::RunStats {
                records_read: counts.records_read,
                records_written: counts.records_written,
                records_dlq: counts.records_dlq,
                duration_ms: terminal.duration_ms(),
            }),
        },
    })
}
