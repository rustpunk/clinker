//! One immutable source of admitted-run lifecycle facts.

use std::fmt;
use std::sync::OnceLock;

use chrono::{DateTime, Utc};
use clinker_core_types::FailureClassification;
use clinker_plan::plan::SemanticFingerprint;

const MAX_CORRELATION_ID_BYTES: usize = 256;

/// CLI-owned lifecycle facts for one admitted finite run.
///
/// Start facts are immutable. The terminal slot is single-assignment so an
/// optional signal can observe run truth but cannot redefine it.
pub(crate) struct RunLifecycleFacts {
    start: RunLifecycleStartFacts,
    terminal: OnceLock<RunLifecycleTerminalFacts>,
}

/// Immutable proof that both caller-visible run correlation IDs were admitted.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RunCorrelationIdentity {
    batch_id: String,
    execution_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RunLifecycleStartFacts {
    batch_id: String,
    execution_id: String,
    fingerprint: PlanFingerprintFacts,
    started_at: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PlanFingerprintFacts {
    algorithm: &'static str,
    version: u32,
    digest: [u8; 32],
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct RunCountFacts {
    pub(crate) records_read: u64,
    pub(crate) records_written: u64,
    pub(crate) records_dlq: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum RunTerminalOutcome {
    Complete,
    Abort,
    Fail(FailureClassification),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RunLifecycleTerminalFacts {
    finished_at: DateTime<Utc>,
    outcome: RunTerminalOutcome,
    counts: RunCountFacts,
    duration_ms: i64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct RunLifecycleSnapshot {
    start: RunLifecycleStartFacts,
    terminal: Option<RunLifecycleTerminalFacts>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleFactsError {
    InvalidBatchId,
    InvalidExecutionId,
    TerminalAlreadyRecorded,
}

impl RunLifecycleFacts {
    pub(crate) fn new(
        identity: RunCorrelationIdentity,
        fingerprint: SemanticFingerprint,
        started_at: DateTime<Utc>,
    ) -> Self {
        Self {
            start: RunLifecycleStartFacts {
                batch_id: identity.batch_id,
                execution_id: identity.execution_id,
                fingerprint: PlanFingerprintFacts {
                    algorithm: fingerprint.algorithm(),
                    version: fingerprint.version(),
                    digest: fingerprint.digest(),
                },
                started_at,
            },
            terminal: OnceLock::new(),
        }
    }

    pub(crate) fn start_snapshot(&self) -> RunLifecycleStartFacts {
        self.start.clone()
    }

    /// Return a bounded owned snapshot for optional signal adapters.
    pub(crate) fn snapshot(&self) -> RunLifecycleSnapshot {
        RunLifecycleSnapshot {
            start: self.start.clone(),
            terminal: self.terminal.get().cloned(),
        }
    }

    pub(crate) fn record_terminal(
        &self,
        finished_at: DateTime<Utc>,
        outcome: RunTerminalOutcome,
        counts: RunCountFacts,
    ) -> Result<(), LifecycleFactsError> {
        let duration_ms = (finished_at - self.start.started_at)
            .num_milliseconds()
            .max(0);
        self.terminal
            .set(RunLifecycleTerminalFacts {
                finished_at,
                outcome,
                counts,
                duration_ms,
            })
            .map_err(|_| LifecycleFactsError::TerminalAlreadyRecorded)
    }
}

impl RunCorrelationIdentity {
    pub(crate) fn new(batch_id: String, execution_id: String) -> Result<Self, LifecycleFactsError> {
        validate_correlation_id(&batch_id).map_err(|()| LifecycleFactsError::InvalidBatchId)?;
        validate_correlation_id(&execution_id)
            .map_err(|()| LifecycleFactsError::InvalidExecutionId)?;
        Ok(Self {
            batch_id,
            execution_id,
        })
    }
}

impl RunLifecycleStartFacts {
    pub(crate) fn batch_id(&self) -> &str {
        &self.batch_id
    }

    pub(crate) fn execution_id(&self) -> &str {
        &self.execution_id
    }

    pub(crate) const fn fingerprint(&self) -> PlanFingerprintFacts {
        self.fingerprint
    }

    pub(crate) const fn started_at(&self) -> DateTime<Utc> {
        self.started_at
    }
}

impl PlanFingerprintFacts {
    pub(crate) const fn algorithm(self) -> &'static str {
        self.algorithm
    }

    pub(crate) const fn version(self) -> u32 {
        self.version
    }

    pub(crate) const fn digest(self) -> [u8; 32] {
        self.digest
    }
}

impl RunLifecycleTerminalFacts {
    pub(crate) const fn finished_at(&self) -> DateTime<Utc> {
        self.finished_at
    }

    pub(crate) fn outcome(&self) -> &RunTerminalOutcome {
        &self.outcome
    }

    pub(crate) const fn counts(&self) -> RunCountFacts {
        self.counts
    }

    pub(crate) const fn duration_ms(&self) -> i64 {
        self.duration_ms
    }
}

impl RunLifecycleSnapshot {
    pub(crate) fn start(&self) -> &RunLifecycleStartFacts {
        &self.start
    }

    pub(crate) fn terminal(&self) -> Option<&RunLifecycleTerminalFacts> {
        self.terminal.as_ref()
    }
}

impl fmt::Display for LifecycleFactsError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidBatchId => write!(
                formatter,
                "batch ID must be non-empty, at most {MAX_CORRELATION_ID_BYTES} UTF-8 bytes, and contain no control characters"
            ),
            Self::InvalidExecutionId => write!(
                formatter,
                "execution ID must be non-empty, at most {MAX_CORRELATION_ID_BYTES} UTF-8 bytes, and contain no control characters"
            ),
            Self::TerminalAlreadyRecorded => {
                write!(
                    formatter,
                    "run lifecycle terminal facts were already recorded"
                )
            }
        }
    }
}

impl std::error::Error for LifecycleFactsError {}

fn validate_correlation_id(value: &str) -> Result<(), ()> {
    if value.is_empty()
        || value.len() > MAX_CORRELATION_ID_BYTES
        || value.chars().any(char::is_control)
    {
        Err(())
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_facts_are_single_assignment_and_snapshots_are_owned() {
        let start = chrono::DateTime::parse_from_rfc3339("2026-08-06T12:00:00Z")
            .expect("timestamp")
            .with_timezone(&Utc);
        let fingerprint = clinker_plan::config::parse_config(
            "pipeline: { name: lifecycle }\nnodes:\n  - type: source\n    name: src\n    config: { name: src, type: csv, path: in.csv, schema: [{ name: id, type: int }] }\n",
        )
        .expect("config")
        .compile(&clinker_plan::config::CompileContext::default())
        .expect("plan")
        .semantic_fingerprint()
        .expect("fingerprint");
        let identity = RunCorrelationIdentity::new(
            "batch".to_owned(),
            "0190b7e0-0000-7000-8000-000000000000".to_owned(),
        )
        .expect("identity");
        let facts = RunLifecycleFacts::new(identity, fingerprint, start);
        let start_snapshot = facts.start_snapshot();
        facts
            .record_terminal(
                start,
                RunTerminalOutcome::Complete,
                RunCountFacts::default(),
            )
            .expect("first terminal");
        assert_eq!(start_snapshot.batch_id(), "batch");
        assert!(facts.snapshot().terminal().is_some());
        assert_eq!(
            facts.record_terminal(start, RunTerminalOutcome::Abort, RunCountFacts::default(),),
            Err(LifecycleFactsError::TerminalAlreadyRecorded)
        );
    }

    #[test]
    fn correlation_identity_rejects_each_invalid_input_class() {
        for batch_id in [String::new(), "x".repeat(257), "batch\ncontrol".to_owned()] {
            assert_eq!(
                RunCorrelationIdentity::new(
                    batch_id,
                    "0190b7e0-0000-7000-8000-000000000000".to_owned(),
                ),
                Err(LifecycleFactsError::InvalidBatchId)
            );
        }
    }
}
