//! Sole ownership of the optional machine-run stream.

use std::io::{self, BufWriter, Write};
use std::sync::{Arc, Mutex};

use clinker_core_types::FailureClassification;
use clinker_exec::pipeline::shutdown::ShutdownToken;

use crate::{MachineFormat, RunArgs};

const MAX_EVENT_BYTES: usize = 16 * 1024;
const MAX_BATCH_ID_BYTES: usize = 256;

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

    pub(crate) fn emit_completed(&self, exit_code: u8) -> io::Result<()> {
        self.with_state(|state| {
            if !state.reserve_terminal() {
                return Ok(());
            }
            if exit_code == 130 {
                return state.write_event("cancelled", serde_json::Map::new());
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
            state.write_event("completed", fields)
        })
    }

    pub(crate) fn emit_failed(
        &self,
        exit_code: u8,
        failure: &FailureClassification,
    ) -> io::Result<()> {
        self.with_state(|state| {
            if !state.reserve_terminal() {
                return Ok(());
            }
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
            state.write_event("failed", fields)
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
        let mut object = serde_json::Map::new();
        object.insert("protocol".to_owned(), serde_json::json!("clinker.run"));
        object.insert("schema".to_owned(), serde_json::json!(1));
        object.insert("event".to_owned(), serde_json::json!(event));
        object.insert("seq".to_owned(), serde_json::json!(self.sequence));
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
        self.writer.write_all(&encoded)?;
        self.writer.flush()?;
        self.sequence = self.sequence.saturating_add(1);
        Ok(())
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
