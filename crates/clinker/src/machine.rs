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
    observability: Option<ObservabilitySummary>,
    progress: BoundedProgress,
}

pub(crate) struct MachineProgressWorker {
    stop: mpsc::SyncSender<()>,
    handle: thread::JoinHandle<io::Result<()>>,
}

impl MachineProgressWorker {
    pub(crate) fn finish(self) -> io::Result<()> {
        let _ = self.stop.try_send(());
        self.handle
            .join()
            .map_err(|_| io::Error::other("machine progress worker panicked"))?
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
            state.write_progress(snapshot, None)
        })
    }

    fn emit_periodic(&self, checkpoints: u64) -> io::Result<()> {
        self.with_state(|state| {
            let Some(snapshot) = state.progress.periodic("executing") else {
                return Ok(());
            };
            state.write_progress(snapshot, Some(checkpoints))
        })
    }

    pub(crate) fn start_execution_progress(&self, token: ShutdownToken) -> MachineProgressWorker {
        let emitter = self.clone();
        let (stop, receiver) = mpsc::sync_channel(1);
        let handle = thread::spawn(move || {
            let mut observed = token.progress_checkpoints();
            loop {
                match receiver.recv_timeout(Duration::from_millis(20)) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
                let checkpoints = token.progress_checkpoints();
                if checkpoints > observed {
                    observed = checkpoints;
                    emitter.emit_periodic(checkpoints)?;
                }
            }
        });
        MachineProgressWorker { stop, handle }
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
            if exit_code == 130 {
                return state.write_terminal_event("cancelled", serde_json::Map::new(), None);
            }
            let result = if exit_code == 2 {
                "completed_with_dlq"
            } else {
                "success"
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
            let fields = serde_json::Map::from_iter([
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
            ]);
            state.write_terminal_event("failed", fields, publication)
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

    fn write_progress(
        &mut self,
        snapshot: ProgressSnapshot,
        checkpoints: Option<u64>,
    ) -> io::Result<()> {
        let mut progress = serde_json::json!({
            "phase": snapshot.phase(),
            "kind": snapshot.kind().as_str(),
            "elapsed_ms": snapshot.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        });
        if let Some(checkpoints) = checkpoints {
            progress["checkpoints"] = serde_json::json!(checkpoints);
        }
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
        "finalizing" => {
            event == "progress"
                && fields["progress"]["kind"] == "transition"
                && fields["progress"]["phase"] == "finalizing"
        }
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

    fn state() -> MachineState {
        MachineState {
            writer: BufWriter::new(Box::new(Vec::<u8>::new())),
            batch_id: "batch".to_owned(),
            execution_id: "018f47a2-9a41-7a27-b4d6-4f7137e3c159".to_owned(),
            sequence: 0,
            plan_identity: serde_json::json!({"status": "resolved"}),
            terminal_reserved: false,
            observability: None,
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
}
