//! Destination-owned attempt manifests and finite artifact publication.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Instant;

use clinker_plan::config::{DestinationProfile, PublicationMode, ResolvedPublicationPolicy};
use clinker_plan::error::PipelineError;
use clinker_plan::plan::CompiledPlan;
use clinker_plan::security::{ValidatedPath, validate_path};
use fs4::FileExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::containment::{
    AnchoredDirectory, ContainedEntryKind, ContainmentError, OutputContainment,
    PromotionDisposition,
};
use super::staging::{OutputStagingRegistry, PublicationOutcome};
use crate::pipeline::shutdown::ShutdownToken;

const MANIFEST_SCHEMA: &str = "clinker.attempt-manifest/v1";
const CONTINUATION_SCHEMA: &str = "clinker.attempt-continuation/v1";
const OWNER_METADATA_CURSOR: &str = "owner-metadata-last";
const PRODUCER_MAX_CHARS: usize = 96;
const PRODUCER_MAX_ENCODED_BYTES: usize = 192;
const LOGICAL_MAX_CHARS: usize = 384;
const LOGICAL_MAX_ENCODED_BYTES: usize = 512;
pub const ARTIFACT_MAX_ENCODED_BYTES: usize = 992;
pub const MANIFEST_MAX_ARTIFACTS: usize = 4096;
pub const MANIFEST_MAX_BYTES: usize = 4 * 1024 * 1024;
pub const PUBLICATION_COPY_BUFFER_BYTES: usize = 1024 * 1024;

/// Exact cross-platform outcome vocabulary preserved by publication and cleanup.
pub const ATTEMPT_EDGE_OUTCOME_TAXONOMY: [&str; 6] = [
    "cancellation_no_final",
    "cleanup_liveness",
    "confinement",
    "cross_filesystem_no_copy",
    "rename_visibility",
    "sync_durability",
];

/// Shared publication and cleanup prohibitions kept as an executable contract.
pub const ATTEMPT_PUBLICATION_PROHIBITIONS: [&str; 5] = [
    "no_visible_final_copy_fallback",
    "no_automatic_publication_mode_fallback",
    "no_cross_artifact_set_atomicity_claim",
    "no_cross_run_staging_sharing",
    "no_destructive_cleanup_by_raw_path",
];

/// Managed artifact role within one run-owned publication attempt.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ArtifactKind {
    /// Ordinary output file.
    Primary,
    /// Per-source fan-out output.
    FanOut,
    /// Split output segment.
    Split,
    /// Dead-letter output.
    Dlq,
    /// Metadata sidecar associated with a data output.
    Sidecar,
}

/// One artifact registered before a run-owned attempt creates any files.
#[derive(Clone, Debug)]
pub struct ArtifactRegistration {
    kind: ArtifactKind,
    producer_label: String,
    logical_leaf: String,
    destination: ValidatedPath,
    disposition: PromotionDisposition,
}

impl ArtifactRegistration {
    /// Build and validate a bounded logical artifact registration.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] when the producer label or logical leaf is
    /// oversized, empty, or not a single relative path component.
    pub fn new(
        kind: ArtifactKind,
        producer_label: impl Into<String>,
        logical_leaf: impl Into<String>,
        destination: ValidatedPath,
        disposition: PromotionDisposition,
    ) -> Result<Self, AttemptError> {
        let producer_label = producer_label.into();
        let logical_leaf = logical_leaf.into();
        validate_text(
            "producer_label",
            &producer_label,
            PRODUCER_MAX_CHARS,
            PRODUCER_MAX_ENCODED_BYTES,
        )?;
        validate_text(
            "logical_leaf",
            &logical_leaf,
            LOGICAL_MAX_CHARS,
            LOGICAL_MAX_ENCODED_BYTES,
        )?;
        if Path::new(&logical_leaf).is_absolute()
            || Path::new(&logical_leaf).components().count() != 1
        {
            return Err(AttemptError::InvalidManifest(
                "logical_leaf must be one relative path component",
            ));
        }
        Ok(Self {
            kind,
            producer_label,
            logical_leaf,
            destination,
            disposition,
        })
    }
}

/// Writable artifact returned by [`AttemptPublication::create_run`].
#[derive(Debug)]
pub struct AttemptArtifactWriter {
    execution_id: String,
    artifact_id: String,
    kind: ArtifactKind,
    file: File,
}

/// Logical per-artifact result returned by run-owned publication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactPublicationResult {
    artifact_id: String,
    kind: ArtifactKind,
    logical_leaf: String,
    state: ArtifactState,
}

impl ArtifactPublicationResult {
    /// Stable artifact identity within the execution.
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    /// Managed artifact role.
    pub fn kind(&self) -> ArtifactKind {
        self.kind
    }

    /// Logical leaf without a physical directory prefix.
    pub fn logical_leaf(&self) -> &str {
        &self.logical_leaf
    }

    /// Exact terminal artifact state.
    pub fn state(&self) -> ArtifactState {
        self.state
    }
}

/// Path-free publication result for a run-owned artifact set.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AttemptPublicationOutcome {
    /// Every artifact is visible and synchronized.
    Complete {
        /// Existing execution identity.
        execution_id: String,
        /// Exact logical result for every registered artifact.
        artifacts: Vec<ArtifactPublicationResult>,
        /// Number of post-publication cleanup-debt entries.
        cleanup_debt_count: usize,
    },
    /// At least one artifact is unpublished or durability is uncertain.
    Incomplete {
        /// Existing execution identity.
        execution_id: String,
        /// Exact logical result for every registered artifact.
        artifacts: Vec<ArtifactPublicationResult>,
        /// Number of post-publication cleanup-debt entries.
        cleanup_debt_count: usize,
    },
}

impl AttemptPublicationOutcome {
    /// Whether every artifact reached synchronized-visible state.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete { .. })
    }

    /// Exact logical artifact results without physical paths.
    pub fn artifacts(&self) -> &[ArtifactPublicationResult] {
        match self {
            Self::Complete { artifacts, .. } | Self::Incomplete { artifacts, .. } => artifacts,
        }
    }
}

/// Explicit capability token for callers that sanitize physical paths before
/// rendering them outside trusted diagnostics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SanitizedPathOptIn;

/// Physical path view available only through explicit sanitized-output opt-in.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactPhysicalPaths {
    /// Stable artifact identity.
    pub artifact_id: String,
    /// Physical final path.
    pub final_path: PathBuf,
    /// Physical destination quarantine path.
    pub quarantine_path: PathBuf,
}

impl AttemptArtifactWriter {
    /// Existing execution identity shared by every writer in this run.
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Stable bounded artifact identity.
    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    /// Managed role of this artifact.
    pub fn kind(&self) -> ArtifactKind {
        self.kind
    }

    /// Borrow the restrictive file handle used by the format writer.
    pub fn file_mut(&mut self) -> &mut File {
        &mut self.file
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptState {
    Staging,
    Ready,
    Publishing,
    Complete,
    Incomplete,
    Abandoned,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactState {
    Staging,
    Ready,
    Published,
    VisibleUnsynchronized,
    Unpublished,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactManifest {
    artifact_id: String,
    producer_label: String,
    logical_leaf: String,
    size_bytes: u64,
    blake3_hex: String,
    state: ArtifactState,
}

impl ArtifactManifest {
    pub fn new(
        artifact_id: &str,
        producer_label: &str,
        logical_leaf: &str,
        size_bytes: u64,
        blake3_hex: &str,
        state: ArtifactState,
    ) -> Result<Self, AttemptError> {
        let artifact = Self {
            artifact_id: artifact_id.to_owned(),
            producer_label: producer_label.to_owned(),
            logical_leaf: logical_leaf.to_owned(),
            size_bytes,
            blake3_hex: blake3_hex.to_owned(),
            state,
        };
        artifact.validate()?;
        Ok(artifact)
    }

    pub fn artifact_id(&self) -> &str {
        &self.artifact_id
    }

    pub fn state(&self) -> ArtifactState {
        self.state
    }

    pub fn encoded_len(&self) -> Result<usize, AttemptError> {
        Ok(serde_json::to_vec(self)
            .map_err(AttemptError::Serialize)?
            .len())
    }

    fn validate(&self) -> Result<(), AttemptError> {
        validate_artifact_id(&self.artifact_id)?;
        validate_text(
            "producer_label",
            &self.producer_label,
            PRODUCER_MAX_CHARS,
            PRODUCER_MAX_ENCODED_BYTES,
        )?;
        validate_text(
            "logical_leaf",
            &self.logical_leaf,
            LOGICAL_MAX_CHARS,
            LOGICAL_MAX_ENCODED_BYTES,
        )?;
        if Path::new(&self.logical_leaf).is_absolute()
            || Path::new(&self.logical_leaf).components().count() != 1
        {
            return Err(AttemptError::InvalidManifest(
                "logical_leaf must be one relative path component",
            ));
        }
        if self.blake3_hex.len() != 64
            || !self
                .blake3_hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(AttemptError::InvalidManifest(
                "blake3_hex must be 64 lowercase hexadecimal characters",
            ));
        }
        if self.encoded_len()? > ARTIFACT_MAX_ENCODED_BYTES {
            return Err(AttemptError::InvalidManifest(
                "artifact entry exceeds its encoded byte limit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptManifest {
    schema: String,
    execution_id: String,
    created_unix_ms: u64,
    eligible_after_unix_ms: u64,
    state: AttemptState,
    artifact_count: usize,
    total_bytes: u64,
    artifacts: Vec<ArtifactManifest>,
}

impl AttemptManifest {
    pub fn new(
        execution_id: &str,
        created_unix_ms: u64,
        eligible_after_unix_ms: u64,
        state: AttemptState,
        artifacts: Vec<ArtifactManifest>,
    ) -> Result<Self, AttemptError> {
        let total_bytes = artifacts.iter().try_fold(0_u64, |total, artifact| {
            total
                .checked_add(artifact.size_bytes)
                .ok_or(AttemptError::InvalidManifest(
                    "artifact byte total overflows u64",
                ))
        })?;
        let manifest = Self {
            schema: MANIFEST_SCHEMA.to_owned(),
            execution_id: execution_id.to_owned(),
            created_unix_ms,
            eligible_after_unix_ms,
            state,
            artifact_count: artifacts.len(),
            total_bytes,
            artifacts,
        };
        manifest.validate(None)?;
        Ok(manifest)
    }

    pub fn read(path: &Path, observed_unix_ms: u64) -> Result<Self, AttemptError> {
        let mut file = File::open(path).map_err(|source| AttemptError::Io {
            operation: "open attempt manifest",
            path: path.to_path_buf(),
            source,
        })?;
        let mut bytes = Vec::new();
        std::io::Read::by_ref(&mut file)
            .take((MANIFEST_MAX_BYTES + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|source| AttemptError::Io {
                operation: "read attempt manifest",
                path: path.to_path_buf(),
                source,
            })?;
        Self::from_bytes(&bytes, observed_unix_ms)
    }

    pub fn from_bytes(bytes: &[u8], observed_unix_ms: u64) -> Result<Self, AttemptError> {
        if bytes.len() > MANIFEST_MAX_BYTES {
            return Err(AttemptError::InvalidManifest(
                "attempt manifest exceeds its byte limit",
            ));
        }
        let manifest: Self = serde_json::from_slice(bytes).map_err(AttemptError::Deserialize)?;
        manifest.validate(Some(observed_unix_ms))?;
        if manifest.to_bytes()?.as_slice() != bytes {
            return Err(AttemptError::InvalidManifest(
                "attempt manifest is not in canonical compact form",
            ));
        }
        Ok(manifest)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, AttemptError> {
        self.validate(None)?;
        let bytes = serde_json::to_vec(self).map_err(AttemptError::Serialize)?;
        if bytes.len() > MANIFEST_MAX_BYTES {
            return Err(AttemptError::InvalidManifest(
                "attempt manifest exceeds its byte limit",
            ));
        }
        Ok(bytes)
    }

    pub fn state(&self) -> AttemptState {
        self.state
    }

    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    pub fn artifact_count(&self) -> usize {
        self.artifact_count
    }

    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub fn artifacts(&self) -> &[ArtifactManifest] {
        &self.artifacts
    }

    fn validate(&self, observed_unix_ms: Option<u64>) -> Result<(), AttemptError> {
        if self.schema != MANIFEST_SCHEMA {
            return Err(AttemptError::InvalidManifest(
                "unsupported attempt manifest schema",
            ));
        }
        validate_execution_id(&self.execution_id)?;
        if self.eligible_after_unix_ms < self.created_unix_ms {
            return Err(AttemptError::InvalidManifest(
                "eligible clock precedes attempt creation",
            ));
        }
        if observed_unix_ms.is_some_and(|observed| self.created_unix_ms > observed) {
            return Err(AttemptError::InvalidManifest(
                "attempt creation clock is later than observation clock",
            ));
        }
        if self.artifacts.len() > MANIFEST_MAX_ARTIFACTS
            || self.artifact_count != self.artifacts.len()
        {
            return Err(AttemptError::InvalidManifest("artifact count is invalid"));
        }
        let mut total_bytes = 0_u64;
        let mut previous = None;
        for artifact in &self.artifacts {
            artifact.validate()?;
            if previous.is_some_and(|id: &str| id >= artifact.artifact_id.as_str()) {
                return Err(AttemptError::InvalidManifest(
                    "artifacts must be strictly ordered by artifact_id",
                ));
            }
            previous = Some(artifact.artifact_id.as_str());
            total_bytes = total_bytes.checked_add(artifact.size_bytes).ok_or(
                AttemptError::InvalidManifest("artifact byte total overflows u64"),
            )?;
        }
        if total_bytes != self.total_bytes {
            return Err(AttemptError::InvalidManifest(
                "artifact byte total does not match entries",
            ));
        }
        if self.state == AttemptState::Complete
            && self
                .artifacts
                .iter()
                .any(|artifact| artifact.state != ArtifactState::Published)
        {
            return Err(AttemptError::InvalidManifest(
                "complete attempt contains a non-published artifact",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptFault {
    Write,
    Copy,
    FileSync,
    DestinationFileSync,
    Digest,
    ManifestReplace,
    BeforeRename,
    DirectorySync,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptTestStage {
    CompleteBeforeCleanup,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptTestEvent {
    pub execution_id: String,
    pub stage: AttemptTestStage,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupDisposition {
    Removed,
    AlreadyAbsent,
    Kept,
}

/// Stable reason an attempt remains retained or a bounded query stops.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CleanupDebtKind {
    /// The entry budget was consumed exactly.
    EntryBudget,
    /// The considered-byte budget would be exceeded by the next regular file.
    ByteBudget,
    /// The monotonic elapsed-time budget was consumed exactly.
    TimeBudget,
    /// The supplied monotonic clock moved backwards.
    MonotonicClock,
    /// A live owner still holds the attempt lock.
    LiveAttempt,
    /// Ownership metadata was missing or inconsistent.
    InvalidOwnership,
    /// The durable manifest was malformed, unsupported, or unreadable.
    InvalidManifest,
    /// The attempt contained a child not named by its ownership manifest.
    UnknownChild,
    /// A link, reparse point, or other unsupported filesystem object was observed.
    UnsafeEntry,
    /// Durable wall-clock evidence was ambiguous.
    ClockAmbiguous,
    /// A contained filesystem operation failed conservatively.
    Operational,
    /// Cleanup was interrupted after making bounded progress.
    Interrupted,
}

/// Path-free, bounded cleanup debt suitable for operator diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CleanupDebt {
    kind: CleanupDebtKind,
    detail: &'static str,
}

impl CleanupDebt {
    fn new(kind: CleanupDebtKind, detail: &'static str) -> Self {
        Self { kind, detail }
    }

    /// Stable debt category.
    pub fn kind(&self) -> CleanupDebtKind {
        self.kind
    }

    /// Bounded path-free explanation.
    pub fn detail(&self) -> &'static str {
        self.detail
    }
}

/// Versioned opaque cursor for a single bounded selector.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AttemptContinuation {
    schema: String,
    plan_hash: [u8; 32],
    root_identifier: String,
    selector: String,
    cursor: Option<String>,
    binding: [u8; 32],
}

impl AttemptContinuation {
    /// Encode a canonical compact continuation.
    pub fn to_bytes(&self) -> Result<Vec<u8>, AttemptError> {
        validate_continuation(self)?;
        serde_json::to_vec(self).map_err(AttemptError::Serialize)
    }

    /// Decode and validate a versioned canonical continuation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, AttemptError> {
        if bytes.len() > 2_048 {
            return Err(AttemptError::InvalidContinuation(
                "continuation exceeds its encoded byte limit",
            ));
        }
        let continuation: Self =
            serde_json::from_slice(bytes).map_err(AttemptError::Deserialize)?;
        validate_continuation(&continuation)?;
        if continuation.to_bytes()?.as_slice() != bytes {
            return Err(AttemptError::InvalidContinuation(
                "continuation is not in canonical compact form",
            ));
        }
        Ok(continuation)
    }
}

/// Exact query bounds consumed by a list, inspection, preview, or purge.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttemptQueryBounds {
    considered_entries: u64,
    considered_bytes: u64,
    elapsed_ms: u64,
}

impl AttemptQueryBounds {
    /// Directory entries visited through retained handles.
    pub fn considered_entries(&self) -> u64 {
        self.considered_entries
    }

    /// Regular-file sizes admitted into the considered-byte budget.
    pub fn considered_bytes(&self) -> u64 {
        self.considered_bytes
    }

    /// Last observed monotonic elapsed time.
    pub fn elapsed_ms(&self) -> u64 {
        self.elapsed_ms
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct AttemptInspection {
    execution_id: String,
    disposition: CleanupDisposition,
    state: Option<AttemptState>,
    created_unix_ms: Option<u64>,
    eligible_after_unix_ms: Option<u64>,
    artifact_ids: Vec<String>,
    cleanup_debt: Vec<CleanupDebt>,
    bounds: AttemptQueryBounds,
    physical_path: Option<PathBuf>,
    eligible: bool,
}

impl std::fmt::Debug for AttemptInspection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AttemptInspection")
            .field("execution_id", &self.execution_id)
            .field("disposition", &self.disposition)
            .field("state", &self.state)
            .field("created_unix_ms", &self.created_unix_ms)
            .field("eligible_after_unix_ms", &self.eligible_after_unix_ms)
            .field("artifact_ids", &self.artifact_ids)
            .field("cleanup_debt", &self.cleanup_debt)
            .field("bounds", &self.bounds)
            .field("eligible", &self.eligible)
            .finish()
    }
}

impl AttemptInspection {
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    pub fn disposition(&self) -> CleanupDisposition {
        self.disposition
    }

    /// Durable attempt lifecycle state, when ownership parsed successfully.
    pub fn state(&self) -> Option<AttemptState> {
        self.state
    }

    /// Persisted creation timestamp, when ownership parsed successfully.
    pub fn created_unix_ms(&self) -> Option<u64> {
        self.created_unix_ms
    }

    /// Persisted creation-grace timestamp, when ownership parsed successfully.
    pub fn eligible_after_unix_ms(&self) -> Option<u64> {
        self.eligible_after_unix_ms
    }

    /// Stable logical artifact identities named by the durable manifest.
    pub fn artifact_ids(&self) -> &[String] {
        &self.artifact_ids
    }

    /// Conservative reasons this attempt remains retained.
    pub fn cleanup_debt(&self) -> &[CleanupDebt] {
        &self.cleanup_debt
    }

    /// Exact bounds consumed while producing this inspection.
    pub fn bounds(&self) -> AttemptQueryBounds {
        self.bounds
    }

    /// Whether durable state and the configured policy make this attempt eligible.
    pub fn is_eligible(&self) -> bool {
        self.eligible
    }

    /// Physical attempt path behind explicit sanitized-output opt-in.
    pub fn physical_path_for_sanitized_output(&self, _opt_in: SanitizedPathOptIn) -> Option<&Path> {
        self.physical_path.as_deref()
    }
}

/// Path-free list entry for one positively identified attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptListEntry {
    inspection: AttemptInspection,
}

impl AttemptListEntry {
    /// Logical execution identity from a supported manifest.
    pub fn execution_id(&self) -> &str {
        self.inspection.execution_id()
    }

    /// Full path-free inspection truth.
    pub fn inspection(&self) -> &AttemptInspection {
        &self.inspection
    }
}

/// One bounded page of owned attempts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptList {
    entries: Vec<AttemptListEntry>,
    continuation: Option<AttemptContinuation>,
    cleanup_debt: Vec<CleanupDebt>,
    bounds: AttemptQueryBounds,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum PurgeSelector {
    Execution(String),
    Expired,
}

/// Typed purge selector bound to a compiled plan and owned root.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgeRequest {
    plan_hash: [u8; 32],
    root_identifier: String,
    selector: PurgeSelector,
}

/// Exact terminal state of one purge invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PurgeDisposition {
    /// The selected attempt and its owner metadata were removed.
    Removed,
    /// The selected attempt was already absent.
    AlreadyAbsent,
    /// Safety or eligibility evidence required retaining the attempt.
    Kept,
    /// Bounded work stopped after zero or more safe mutations.
    Partial,
}

/// Non-mutating bounded purge selection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgePreview {
    selected_execution_ids: Vec<String>,
    inspections: Vec<AttemptInspection>,
    continuation: Option<AttemptContinuation>,
    cleanup_debt: Vec<CleanupDebt>,
    bounds: AttemptQueryBounds,
}

impl PurgePreview {
    /// Eligible logical executions selected without filesystem mutation.
    pub fn selected_execution_ids(&self) -> &[String] {
        &self.selected_execution_ids
    }

    /// Path-free inspection evidence behind the selection.
    pub fn inspections(&self) -> &[AttemptInspection] {
        &self.inspections
    }

    /// Cursor for the next bounded preview page.
    pub fn continuation(&self) -> Option<&AttemptContinuation> {
        self.continuation.as_ref()
    }

    /// Budget and ambiguity debt encountered during preview.
    pub fn cleanup_debt(&self) -> &[CleanupDebt] {
        &self.cleanup_debt
    }

    /// Exact bounds consumed by preview.
    pub fn bounds(&self) -> AttemptQueryBounds {
        self.bounds
    }
}

/// Exact bounded result of purge execution.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PurgeReport {
    disposition: PurgeDisposition,
    selected_execution_ids: Vec<String>,
    removed_execution_ids: Vec<String>,
    kept_execution_ids: Vec<String>,
    removed_artifact_count: usize,
    continuation: Option<AttemptContinuation>,
    cleanup_debt: Vec<CleanupDebt>,
    bounds: AttemptQueryBounds,
}

impl PurgeReport {
    /// Terminal state of this bounded invocation.
    pub fn disposition(&self) -> PurgeDisposition {
        self.disposition
    }

    /// Logical executions selected by the request.
    pub fn selected_execution_ids(&self) -> &[String] {
        &self.selected_execution_ids
    }

    /// Logical executions fully removed by this invocation.
    pub fn removed_execution_ids(&self) -> &[String] {
        &self.removed_execution_ids
    }

    /// Logical executions retained after this invocation.
    pub fn kept_execution_ids(&self) -> &[String] {
        &self.kept_execution_ids
    }

    /// Owned artifact files removed by this invocation.
    pub fn removed_artifact_count(&self) -> usize {
        self.removed_artifact_count
    }

    /// Cursor required to resume partial bounded cleanup.
    pub fn continuation(&self) -> Option<&AttemptContinuation> {
        self.continuation.as_ref()
    }

    /// Exact retryable cleanup debt.
    pub fn cleanup_debt(&self) -> &[CleanupDebt] {
        &self.cleanup_debt
    }

    /// Exact bounds consumed by execution.
    pub fn bounds(&self) -> AttemptQueryBounds {
        self.bounds
    }
}

impl AttemptList {
    /// Positively identified attempts in deterministic logical order.
    pub fn entries(&self) -> &[AttemptListEntry] {
        &self.entries
    }

    /// Cursor for the next bounded page, if traversal stopped early.
    pub fn continuation(&self) -> Option<&AttemptContinuation> {
        self.continuation.as_ref()
    }

    /// Query-level budget or ambiguity debt.
    pub fn cleanup_debt(&self) -> &[CleanupDebt] {
        &self.cleanup_debt
    }

    /// Directory entries visited through retained handles.
    pub fn considered_entries(&self) -> u64 {
        self.bounds.considered_entries()
    }

    /// Regular-file metadata bytes admitted by the query.
    pub fn considered_bytes(&self) -> u64 {
        self.bounds.considered_bytes()
    }

    /// Last monotonic elapsed time observed by the query.
    pub fn elapsed_ms(&self) -> u64 {
        self.bounds.elapsed_ms()
    }
}

#[derive(Clone, Debug)]
struct OwnedAttemptRoot {
    identifier: String,
    destination: ValidatedPath,
}

/// Compiled-plan-bound entry point for retained-attempt operations.
#[derive(Debug)]
pub struct AttemptQuery {
    plan_hash: [u8; 32],
    policy: ResolvedPublicationPolicy,
    roots: Vec<OwnedAttemptRoot>,
    consumed_continuations: Mutex<std::collections::BTreeSet<String>>,
}

impl AttemptQuery {
    /// Bind retained-attempt operations to one freshly compiled plan, one
    /// resolved publication policy, and a finite set of validated roots.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] when no root is supplied or two roots collapse
    /// to the same path-free ownership identifier.
    pub fn new(
        plan: &CompiledPlan,
        policy: &ResolvedPublicationPolicy,
        destination_roots: Vec<ValidatedPath>,
    ) -> Result<Self, AttemptError> {
        if destination_roots.is_empty() {
            return Err(AttemptError::InvalidQuery(
                "at least one owned root is required",
            ));
        }
        let mut roots = destination_roots
            .into_iter()
            .map(|destination| OwnedAttemptRoot {
                identifier: owned_root_identifier(destination.as_path()),
                destination,
            })
            .collect::<Vec<_>>();
        roots.sort_by(|left, right| left.identifier.cmp(&right.identifier));
        if roots
            .windows(2)
            .any(|pair| pair[0].identifier == pair[1].identifier)
        {
            return Err(AttemptError::InvalidQuery(
                "owned destination roots must be distinct",
            ));
        }
        Ok(Self {
            plan_hash: *plan.pipeline_hash(),
            policy: policy.clone(),
            roots,
            consumed_continuations: Mutex::new(std::collections::BTreeSet::new()),
        })
    }

    /// Path-free identifiers for the finite roots admitted by this query.
    pub fn owned_root_ids(&self) -> Vec<&str> {
        self.roots
            .iter()
            .map(|root| root.identifier.as_str())
            .collect()
    }

    /// Enumerate a bounded page using a process monotonic clock.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] for an invalid root or continuation binding.
    pub fn list(
        &self,
        root_identifier: &str,
        observed_unix_ms: u64,
        continuation: Option<&AttemptContinuation>,
    ) -> Result<AttemptList, AttemptError> {
        let started = Instant::now();
        self.list_with_elapsed(root_identifier, observed_unix_ms, continuation, || {
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
        })
    }

    /// Deterministic monotonic-clock form used by embedders and boundary tests.
    /// Each returned value is elapsed milliseconds since this query began.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] for an invalid root or continuation binding.
    #[doc(hidden)]
    pub fn list_with_elapsed<F>(
        &self,
        root_identifier: &str,
        observed_unix_ms: u64,
        continuation: Option<&AttemptContinuation>,
        mut elapsed_ms: F,
    ) -> Result<AttemptList, AttemptError>
    where
        F: FnMut() -> u64,
    {
        let root = self.root(root_identifier)?;
        let cursor = self.validate_selector_continuation(root_identifier, "list", continuation)?;
        let mut budget = QueryBudget::new(&self.policy);
        budget.observe_time(elapsed_ms())?;
        let mut list = AttemptList {
            entries: Vec::new(),
            continuation: None,
            cleanup_debt: Vec::new(),
            bounds: AttemptQueryBounds::default(),
        };

        let destination = match AnchoredDirectory::open(&root.destination) {
            Ok(destination) => destination,
            Err(_) => {
                list.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::Operational,
                    "owned root could not be opened through retained handles",
                ));
                list.bounds = budget.bounds();
                return Ok(list);
            }
        };
        let namespace = match destination.open_child(".clinker-attempts") {
            Ok(namespace) => namespace,
            Err(error) if containment_kind(&error) == Some(std::io::ErrorKind::NotFound) => {
                list.bounds = budget.bounds();
                return Ok(list);
            }
            Err(_) => {
                list.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::UnsafeEntry,
                    "attempt namespace failed handle-relative confinement checks",
                ));
                list.bounds = budget.bounds();
                return Ok(list);
            }
        };
        let remaining = budget.remaining_entries_as_usize();
        let mut namespace_entries = match namespace.bounded_entries(remaining) {
            Ok(entries) => entries,
            Err(_) => {
                list.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::Operational,
                    "attempt namespace enumeration failed",
                ));
                list.bounds = budget.bounds();
                return Ok(list);
            }
        };
        namespace_entries
            .entries
            .sort_by(|left, right| left.name.cmp(&right.name));
        let mut last_cursor = cursor.clone();
        for entry in namespace_entries.entries {
            let Ok(name) = entry.name.into_string() else {
                list.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::InvalidOwnership,
                    "attempt namespace contains a non-UTF-8 child",
                ));
                continue;
            };
            if cursor.as_ref().is_some_and(|cursor| name <= *cursor) {
                continue;
            }
            if let Err(debt) = budget.visit(entry.kind, entry.size_bytes, elapsed_ms()) {
                list.cleanup_debt.push(debt);
                list.continuation = Some(self.continuation(root_identifier, "list", last_cursor));
                list.bounds = budget.bounds();
                return Ok(list);
            }
            last_cursor = Some(name.clone());
            if entry.kind != ContainedEntryKind::Directory || validate_execution_id(&name).is_err()
            {
                list.cleanup_debt.push(CleanupDebt::new(
                    if matches!(entry.kind, ContainedEntryKind::LinkOrReparse) {
                        CleanupDebtKind::UnsafeEntry
                    } else {
                        CleanupDebtKind::InvalidOwnership
                    },
                    "attempt namespace child is not a supported execution directory",
                ));
                continue;
            }
            let inspection = inspect_owned_attempt(
                root,
                &name,
                observed_unix_ms,
                &self.policy,
                &mut budget,
                &mut elapsed_ms,
            );
            if inspection
                .cleanup_debt
                .iter()
                .any(|debt| is_budget_debt(debt.kind))
            {
                list.cleanup_debt.extend(
                    inspection
                        .cleanup_debt
                        .iter()
                        .filter(|debt| is_budget_debt(debt.kind))
                        .cloned(),
                );
                list.continuation =
                    Some(self.continuation(root_identifier, "list", Some(name.clone())));
            }
            if inspection.state.is_some() {
                list.entries.push(AttemptListEntry { inspection });
            } else {
                for debt in inspection.cleanup_debt {
                    if !list.cleanup_debt.contains(&debt) {
                        list.cleanup_debt.push(debt);
                    }
                }
            }
            if list.continuation.is_some() {
                break;
            }
        }
        if list.continuation.is_none() && !namespace_entries.complete {
            list.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::EntryBudget,
                "entry budget stopped attempt namespace enumeration",
            ));
            list.continuation = Some(self.continuation(root_identifier, "list", last_cursor));
        }
        list.bounds = budget.bounds();
        Ok(list)
    }

    /// Inspect one logical execution without mutating its files.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] when the root or execution selector is invalid.
    pub fn inspect(
        &self,
        root_identifier: &str,
        execution_id: &str,
        observed_unix_ms: u64,
    ) -> Result<AttemptInspection, AttemptError> {
        validate_execution_id(execution_id)?;
        let root = self.root(root_identifier)?;
        let started = Instant::now();
        let mut elapsed = || u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
        let mut budget = QueryBudget::new(&self.policy);
        budget.observe_time(elapsed())?;
        budget
            .visit(ContainedEntryKind::Directory, None, elapsed())
            .map_err(|_| {
                AttemptError::InvalidQuery(
                    "entry or time budget cannot admit the execution selector",
                )
            })?;
        Ok(inspect_owned_attempt(
            root,
            execution_id,
            observed_unix_ms,
            &self.policy,
            &mut budget,
            &mut elapsed,
        ))
    }

    /// Create a compiled-plan-bound selector for one logical execution.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] when the root or execution identity is invalid.
    pub fn purge_execution(
        &self,
        root_identifier: &str,
        execution_id: &str,
    ) -> Result<PurgeRequest, AttemptError> {
        self.root(root_identifier)?;
        validate_execution_id(execution_id)?;
        Ok(PurgeRequest {
            plan_hash: self.plan_hash,
            root_identifier: root_identifier.to_owned(),
            selector: PurgeSelector::Execution(execution_id.to_owned()),
        })
    }

    /// Create a compiled-plan-bound selector for all policy-expired attempts.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] when the root is not owned by this query.
    pub fn purge_expired(&self, root_identifier: &str) -> Result<PurgeRequest, AttemptError> {
        self.root(root_identifier)?;
        Ok(PurgeRequest {
            plan_hash: self.plan_hash,
            root_identifier: root_identifier.to_owned(),
            selector: PurgeSelector::Expired,
        })
    }

    /// Select purge candidates with the same bounds as execution and no writes.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] for a request or continuation not bound to this query.
    pub fn preview(
        &self,
        request: &PurgeRequest,
        observed_unix_ms: u64,
        continuation: Option<&AttemptContinuation>,
    ) -> Result<PurgePreview, AttemptError> {
        self.validate_request(request)?;
        let selector = purge_selector_name(&request.selector);
        let cursor =
            self.validate_selector_continuation(&request.root_identifier, &selector, continuation)?;
        match &request.selector {
            PurgeSelector::Execution(execution_id) => {
                if cursor.is_some() {
                    return Err(AttemptError::InvalidContinuation(
                        "completed execution preview has no continuation cursor",
                    ));
                }
                let inspection =
                    self.inspect(&request.root_identifier, execution_id, observed_unix_ms)?;
                let selected_execution_ids = if inspection.is_eligible() {
                    vec![execution_id.clone()]
                } else {
                    Vec::new()
                };
                Ok(PurgePreview {
                    cleanup_debt: inspection.cleanup_debt.clone(),
                    bounds: inspection.bounds,
                    inspections: vec![inspection],
                    selected_execution_ids,
                    continuation: None,
                })
            }
            PurgeSelector::Expired => {
                let list_continuation = continuation.map(|continuation| AttemptContinuation {
                    schema: CONTINUATION_SCHEMA.to_owned(),
                    plan_hash: continuation.plan_hash,
                    root_identifier: continuation.root_identifier.clone(),
                    selector: "list".to_owned(),
                    cursor: continuation.cursor.clone(),
                    binding: continuation_binding(
                        &continuation.plan_hash,
                        &continuation.root_identifier,
                        "list",
                        continuation.cursor.as_deref(),
                    ),
                });
                let list = self.list(
                    &request.root_identifier,
                    observed_unix_ms,
                    list_continuation.as_ref(),
                )?;
                let inspections = list
                    .entries
                    .into_iter()
                    .map(|entry| entry.inspection)
                    .collect::<Vec<_>>();
                let selected_execution_ids = inspections
                    .iter()
                    .filter(|inspection| inspection.is_eligible())
                    .map(|inspection| inspection.execution_id.clone())
                    .collect();
                let next = list.continuation.map(|continuation| {
                    self.continuation(&request.root_identifier, &selector, continuation.cursor)
                });
                Ok(PurgePreview {
                    selected_execution_ids,
                    inspections,
                    continuation: next,
                    cleanup_debt: list.cleanup_debt,
                    bounds: list.bounds,
                })
            }
        }
    }

    /// Execute bounded metadata-last cleanup with a process monotonic clock.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] for a request or continuation not bound to this query.
    pub fn execute(
        &self,
        request: &PurgeRequest,
        observed_unix_ms: u64,
        continuation: Option<&AttemptContinuation>,
        shutdown: &ShutdownToken,
    ) -> Result<PurgeReport, AttemptError> {
        let started = Instant::now();
        self.execute_with_elapsed(request, observed_unix_ms, continuation, shutdown, || {
            u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
        })
    }

    /// Deterministic monotonic-clock form of [`Self::execute`].
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] for a request or continuation not bound to this query.
    #[doc(hidden)]
    pub fn execute_with_elapsed<F>(
        &self,
        request: &PurgeRequest,
        observed_unix_ms: u64,
        continuation: Option<&AttemptContinuation>,
        shutdown: &ShutdownToken,
        mut elapsed_ms: F,
    ) -> Result<PurgeReport, AttemptError>
    where
        F: FnMut() -> u64,
    {
        self.validate_request(request)?;
        match &request.selector {
            PurgeSelector::Execution(execution_id) => {
                let selector_name = purge_selector_name(&request.selector);
                let cursor = self.validate_selector_continuation(
                    &request.root_identifier,
                    &selector_name,
                    continuation,
                )?;
                self.execute_one(
                    request,
                    execution_id,
                    observed_unix_ms,
                    cursor,
                    shutdown,
                    &mut elapsed_ms,
                    AttemptQueryBounds::default(),
                )
            }
            PurgeSelector::Expired => {
                let preview = self.preview(request, observed_unix_ms, continuation)?;
                if preview.selected_execution_ids.is_empty() {
                    return Ok(PurgeReport {
                        disposition: if preview.continuation.is_some() {
                            PurgeDisposition::Partial
                        } else {
                            PurgeDisposition::Kept
                        },
                        selected_execution_ids: Vec::new(),
                        removed_execution_ids: Vec::new(),
                        kept_execution_ids: Vec::new(),
                        removed_artifact_count: 0,
                        continuation: preview.continuation,
                        cleanup_debt: preview.cleanup_debt,
                        bounds: preview.bounds,
                    });
                }
                // Expired sweeps remain finite. Each selected execution is
                // independently revalidated and locked immediately before mutation.
                let mut report = PurgeReport {
                    disposition: if preview.continuation.is_some() {
                        PurgeDisposition::Partial
                    } else {
                        PurgeDisposition::Removed
                    },
                    selected_execution_ids: preview.selected_execution_ids.clone(),
                    removed_execution_ids: Vec::new(),
                    kept_execution_ids: preview.selected_execution_ids.clone(),
                    removed_artifact_count: 0,
                    continuation: preview.continuation,
                    cleanup_debt: preview.cleanup_debt,
                    bounds: preview.bounds,
                };
                for execution_id in preview.selected_execution_ids {
                    let exact = self.purge_execution(&request.root_identifier, &execution_id)?;
                    let child = self.execute_one(
                        &exact,
                        &execution_id,
                        observed_unix_ms,
                        None,
                        shutdown,
                        &mut elapsed_ms,
                        report.bounds,
                    )?;
                    report.removed_artifact_count = report
                        .removed_artifact_count
                        .checked_add(child.removed_artifact_count)
                        .ok_or(AttemptError::InvalidQuery(
                            "purge artifact accounting overflowed",
                        ))?;
                    report
                        .removed_execution_ids
                        .extend(child.removed_execution_ids);
                    let removed = report
                        .removed_execution_ids
                        .iter()
                        .cloned()
                        .collect::<std::collections::BTreeSet<_>>();
                    report
                        .kept_execution_ids
                        .retain(|kept| !removed.contains(kept));
                    report.cleanup_debt.extend(child.cleanup_debt);
                    report.bounds = child.bounds;
                    if child.disposition != PurgeDisposition::Removed {
                        report.disposition = child.disposition;
                        report.continuation = child.continuation;
                        break;
                    }
                }
                Ok(report)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_one<F>(
        &self,
        request: &PurgeRequest,
        execution_id: &str,
        observed_unix_ms: u64,
        cursor: Option<String>,
        shutdown: &ShutdownToken,
        elapsed_ms: &mut F,
        initial_bounds: AttemptQueryBounds,
    ) -> Result<PurgeReport, AttemptError>
    where
        F: FnMut() -> u64,
    {
        let root = self.root(&request.root_identifier)?;
        let selector_name = purge_selector_name(&request.selector);
        let mut report = PurgeReport {
            disposition: PurgeDisposition::Kept,
            selected_execution_ids: vec![execution_id.to_owned()],
            removed_execution_ids: Vec::new(),
            kept_execution_ids: vec![execution_id.to_owned()],
            removed_artifact_count: 0,
            continuation: None,
            cleanup_debt: Vec::new(),
            bounds: AttemptQueryBounds::default(),
        };
        let mut budget = QueryBudget::resume(&self.policy, initial_bounds);
        budget.observe_time(elapsed_ms())?;
        if let Err(debt) = budget.visit(ContainedEntryKind::Directory, None, elapsed_ms()) {
            report.disposition = PurgeDisposition::Partial;
            report.cleanup_debt.push(debt);
            report.continuation =
                Some(self.continuation(&request.root_identifier, &selector_name, cursor));
            report.bounds = budget.bounds();
            return Ok(report);
        }
        let Some(attempt_root) = (match AttemptRoot::open(&root.destination, execution_id) {
            Ok(attempt_root) => attempt_root,
            Err(_) => {
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::UnsafeEntry,
                    "execution directory failed handle-relative confinement checks",
                ));
                report.bounds = budget.bounds();
                return Ok(report);
            }
        }) else {
            report.disposition = PurgeDisposition::AlreadyAbsent;
            report.kept_execution_ids.clear();
            report.bounds = budget.bounds();
            return Ok(report);
        };

        if cursor.as_deref() == Some(OWNER_METADATA_CURSOR) {
            let entries = match attempt_root
                .directory
                .bounded_entries(budget.remaining_entries_as_usize())
            {
                Ok(entries) => entries,
                Err(_) => {
                    report.disposition = PurgeDisposition::Partial;
                    report.cleanup_debt.push(CleanupDebt::new(
                        CleanupDebtKind::Operational,
                        "owner-metadata retry enumeration failed",
                    ));
                    report.continuation = Some(self.continuation(
                        &request.root_identifier,
                        &selector_name,
                        Some(OWNER_METADATA_CURSOR.to_owned()),
                    ));
                    report.bounds = budget.bounds();
                    return Ok(report);
                }
            };
            if !entries.complete {
                report.disposition = PurgeDisposition::Partial;
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::EntryBudget,
                    "entry budget stopped owner-metadata retry",
                ));
                report.continuation = Some(self.continuation(
                    &request.root_identifier,
                    &selector_name,
                    Some(OWNER_METADATA_CURSOR.to_owned()),
                ));
                report.bounds = budget.bounds();
                return Ok(report);
            }
            let mut names = std::collections::BTreeSet::new();
            for entry in entries.entries {
                if let Err(debt) = budget.visit(entry.kind, entry.size_bytes, elapsed_ms()) {
                    report.disposition = PurgeDisposition::Partial;
                    report.cleanup_debt.push(debt);
                    report.continuation = Some(self.continuation(
                        &request.root_identifier,
                        &selector_name,
                        Some(OWNER_METADATA_CURSOR.to_owned()),
                    ));
                    report.bounds = budget.bounds();
                    return Ok(report);
                }
                let Ok(name) = entry.name.into_string() else {
                    report.disposition = PurgeDisposition::Partial;
                    report.cleanup_debt.push(CleanupDebt::new(
                        CleanupDebtKind::InvalidOwnership,
                        "owner-metadata retry found a non-UTF-8 child",
                    ));
                    report.continuation = Some(self.continuation(
                        &request.root_identifier,
                        &selector_name,
                        Some(OWNER_METADATA_CURSOR.to_owned()),
                    ));
                    report.bounds = budget.bounds();
                    return Ok(report);
                };
                if entry.kind != ContainedEntryKind::File {
                    report.disposition = PurgeDisposition::Partial;
                    report.cleanup_debt.push(CleanupDebt::new(
                        CleanupDebtKind::UnsafeEntry,
                        "owner-metadata retry found an unsafe filesystem entry",
                    ));
                    report.continuation = Some(self.continuation(
                        &request.root_identifier,
                        &selector_name,
                        Some(OWNER_METADATA_CURSOR.to_owned()),
                    ));
                    report.bounds = budget.bounds();
                    return Ok(report);
                }
                names.insert(name);
            }
            if names.is_empty() {
                if attempt_root.remove_empty().is_err() {
                    report.disposition = PurgeDisposition::Partial;
                    report.cleanup_debt.push(CleanupDebt::new(
                        CleanupDebtKind::Operational,
                        "empty attempt root could not be removed last",
                    ));
                    report.continuation = Some(self.continuation(
                        &request.root_identifier,
                        &selector_name,
                        Some(OWNER_METADATA_CURSOR.to_owned()),
                    ));
                    report.bounds = budget.bounds();
                    return Ok(report);
                }
            } else if names == ["live.lock".to_owned()].into_iter().collect() {
                let lock = match attempt_root.directory.open_file("live.lock") {
                    Ok(lock) => lock,
                    Err(_) => {
                        report.disposition = PurgeDisposition::Partial;
                        report.cleanup_debt.push(CleanupDebt::new(
                            CleanupDebtKind::Operational,
                            "owner-metadata retry could not open the liveness guard",
                        ));
                        report.continuation = Some(self.continuation(
                            &request.root_identifier,
                            &selector_name,
                            Some(OWNER_METADATA_CURSOR.to_owned()),
                        ));
                        report.bounds = budget.bounds();
                        return Ok(report);
                    }
                };
                if FileExt::try_lock(&lock).is_err() {
                    report.disposition = PurgeDisposition::Partial;
                    report.cleanup_debt.push(CleanupDebt::new(
                        CleanupDebtKind::LiveAttempt,
                        "owner-metadata retry found a live writer",
                    ));
                    report.continuation = Some(self.continuation(
                        &request.root_identifier,
                        &selector_name,
                        Some(OWNER_METADATA_CURSOR.to_owned()),
                    ));
                    report.bounds = budget.bounds();
                    return Ok(report);
                }
                let _ = FileExt::unlock(&lock);
                drop(lock);
                if attempt_root.directory.remove_file("live.lock").is_err()
                    || attempt_root.remove_empty().is_err()
                {
                    report.disposition = PurgeDisposition::Partial;
                    report.cleanup_debt.push(CleanupDebt::new(
                        CleanupDebtKind::Operational,
                        "owner metadata or empty attempt root could not be removed last",
                    ));
                    report.continuation = Some(self.continuation(
                        &request.root_identifier,
                        &selector_name,
                        Some(OWNER_METADATA_CURSOR.to_owned()),
                    ));
                    report.bounds = budget.bounds();
                    return Ok(report);
                }
            } else {
                report.disposition = PurgeDisposition::Partial;
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::UnknownChild,
                    "owner-metadata retry found unrelated attempt contents",
                ));
                report.continuation = Some(self.continuation(
                    &request.root_identifier,
                    &selector_name,
                    Some(OWNER_METADATA_CURSOR.to_owned()),
                ));
                report.bounds = budget.bounds();
                return Ok(report);
            }
            report.disposition = PurgeDisposition::Removed;
            report.removed_execution_ids.push(execution_id.to_owned());
            report.kept_execution_ids.clear();
            report.bounds = budget.bounds();
            return Ok(report);
        }

        let lock = match attempt_root.directory.open_file("live.lock") {
            Ok(lock) => lock,
            Err(_) => {
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::InvalidOwnership,
                    "attempt liveness metadata could not be opened",
                ));
                report.bounds = budget.bounds();
                return Ok(report);
            }
        };
        if FileExt::try_lock(&lock).is_err() {
            report.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::LiveAttempt,
                "attempt liveness lock is held by a writer",
            ));
            report.bounds = budget.bounds();
            return Ok(report);
        }

        let mut entries = match attempt_root
            .directory
            .bounded_entries(budget.remaining_entries_as_usize())
        {
            Ok(entries) => entries,
            Err(_) => {
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::Operational,
                    "attempt directory enumeration failed",
                ));
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
        };
        entries
            .entries
            .sort_by(|left, right| left.name.cmp(&right.name));
        let mut observed = BTreeMap::new();
        for entry in entries.entries {
            if let Err(debt) = budget.visit(entry.kind, entry.size_bytes, elapsed_ms()) {
                report.disposition = PurgeDisposition::Partial;
                report.cleanup_debt.push(debt);
                report.continuation =
                    Some(self.continuation(&request.root_identifier, &selector_name, cursor));
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
            let Ok(name) = entry.name.into_string() else {
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::InvalidOwnership,
                    "attempt contains a non-UTF-8 child",
                ));
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            };
            observed.insert(name, entry.kind);
        }
        if !entries.complete {
            report.disposition = PurgeDisposition::Partial;
            report.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::EntryBudget,
                "entry budget stopped purge ownership validation",
            ));
            report.continuation =
                Some(self.continuation(&request.root_identifier, &selector_name, cursor));
            let _ = FileExt::unlock(&lock);
            report.bounds = budget.bounds();
            return Ok(report);
        }
        if observed.get("live.lock") != Some(&ContainedEntryKind::File)
            || observed.get("manifest.json") != Some(&ContainedEntryKind::File)
        {
            report.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::InvalidOwnership,
                "attempt ownership metadata is missing or invalid",
            ));
            let _ = FileExt::unlock(&lock);
            report.bounds = budget.bounds();
            return Ok(report);
        }
        let manifest = match read_manifest_from_anchor(&attempt_root.directory, observed_unix_ms) {
            Ok(manifest) => manifest,
            Err(error) => {
                report.cleanup_debt.push(manifest_cleanup_debt(&error));
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
        };
        let expected = manifest
            .artifacts
            .iter()
            .map(|artifact| artifact.artifact_id.as_str())
            .chain(["live.lock", "manifest.json"])
            .collect::<std::collections::BTreeSet<_>>();
        let refusal = if manifest.execution_id != execution_id {
            Some(CleanupDebt::new(
                CleanupDebtKind::InvalidOwnership,
                "manifest execution identity does not match the typed selector",
            ))
        } else if observed
            .keys()
            .any(|name| !expected.contains(name.as_str()))
        {
            Some(CleanupDebt::new(
                CleanupDebtKind::UnknownChild,
                "attempt contains a child not named by its ownership manifest",
            ))
        } else if observed.iter().any(|(name, kind)| {
            name != "live.lock" && name != "manifest.json" && *kind != ContainedEntryKind::File
        }) {
            Some(CleanupDebt::new(
                CleanupDebtKind::UnsafeEntry,
                "manifest-owned artifact is a link, reparse point, or unsupported entry",
            ))
        } else {
            None
        };
        if let Some(debt) = refusal {
            report.cleanup_debt.push(debt);
            let _ = FileExt::unlock(&lock);
            report.bounds = budget.bounds();
            return Ok(report);
        }
        match cleanup_eligible(&manifest, observed_unix_ms, &self.policy) {
            Ok(true) => {}
            Ok(false) => {
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
            Err(debt) => {
                report.cleanup_debt.push(debt);
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
        }

        let mut last_removed = cursor;
        for artifact in &manifest.artifacts {
            if last_removed
                .as_ref()
                .is_some_and(|cursor| artifact.artifact_id <= *cursor)
            {
                continue;
            }
            if shutdown.is_requested() {
                report.disposition = PurgeDisposition::Partial;
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::Interrupted,
                    "cleanup was interrupted with ownership metadata retained",
                ));
                report.continuation =
                    Some(self.continuation(&request.root_identifier, &selector_name, last_removed));
                let _ = attempt_root.directory.sync();
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
            if let Err(debt) = budget.check_time(elapsed_ms()) {
                report.disposition = PurgeDisposition::Partial;
                report.cleanup_debt.push(debt);
                report.continuation =
                    Some(self.continuation(&request.root_identifier, &selector_name, last_removed));
                let _ = attempt_root.directory.sync();
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
            if observed.contains_key(&artifact.artifact_id) {
                if attempt_root
                    .directory
                    .remove_file(&artifact.artifact_id)
                    .is_err()
                {
                    report.disposition = PurgeDisposition::Partial;
                    report.cleanup_debt.push(CleanupDebt::new(
                        CleanupDebtKind::Operational,
                        "owned artifact removal failed with owner metadata retained",
                    ));
                    report.continuation = Some(self.continuation(
                        &request.root_identifier,
                        &selector_name,
                        last_removed,
                    ));
                    let _ = attempt_root.directory.sync();
                    let _ = FileExt::unlock(&lock);
                    report.bounds = budget.bounds();
                    return Ok(report);
                }
                report.removed_artifact_count += 1;
            }
            last_removed = Some(artifact.artifact_id.clone());
        }
        if shutdown.is_requested() {
            report.disposition = PurgeDisposition::Partial;
            report.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::Interrupted,
                "cleanup was interrupted with ownership metadata retained",
            ));
            report.continuation =
                Some(self.continuation(&request.root_identifier, &selector_name, last_removed));
            let _ = attempt_root.directory.sync();
            let _ = FileExt::unlock(&lock);
            report.bounds = budget.bounds();
            return Ok(report);
        }
        let final_entries = match attempt_root
            .directory
            .bounded_entries(budget.remaining_entries_as_usize())
        {
            Ok(entries) => entries,
            Err(_) => {
                report.disposition = PurgeDisposition::Partial;
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::Operational,
                    "final owner-metadata revalidation failed",
                ));
                report.continuation =
                    Some(self.continuation(&request.root_identifier, &selector_name, last_removed));
                let _ = attempt_root.directory.sync();
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
        };
        let final_complete = final_entries.complete;
        let mut final_names = std::collections::BTreeSet::new();
        for entry in final_entries.entries {
            if let Err(debt) = budget.visit(entry.kind, entry.size_bytes, elapsed_ms()) {
                report.disposition = PurgeDisposition::Partial;
                report.cleanup_debt.push(debt);
                report.continuation =
                    Some(self.continuation(&request.root_identifier, &selector_name, last_removed));
                let _ = attempt_root.directory.sync();
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
            let Ok(name) = entry.name.into_string() else {
                report.disposition = PurgeDisposition::Partial;
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::InvalidOwnership,
                    "final owner metadata contains a non-UTF-8 child",
                ));
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            };
            if entry.kind != ContainedEntryKind::File {
                report.disposition = PurgeDisposition::Partial;
                report.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::UnsafeEntry,
                    "final owner metadata contains an unsafe filesystem entry",
                ));
                let _ = FileExt::unlock(&lock);
                report.bounds = budget.bounds();
                return Ok(report);
            }
            final_names.insert(name);
        }
        if !final_complete
            || final_names
                != ["live.lock".to_owned(), "manifest.json".to_owned()]
                    .into_iter()
                    .collect()
        {
            report.disposition = PurgeDisposition::Partial;
            report.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::UnknownChild,
                "attempt contents changed before owner metadata removal",
            ));
            let _ = FileExt::unlock(&lock);
            report.bounds = budget.bounds();
            return Ok(report);
        }
        if attempt_root.directory.sync().is_err()
            || attempt_root.directory.remove_file("manifest.json").is_err()
        {
            report.disposition = PurgeDisposition::Partial;
            report.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::Operational,
                "artifact cleanup completed but manifest removal failed",
            ));
            report.continuation =
                Some(self.continuation(&request.root_identifier, &selector_name, last_removed));
            let _ = FileExt::unlock(&lock);
            report.bounds = budget.bounds();
            return Ok(report);
        }
        if attempt_root.directory.sync().is_err() {
            report.disposition = PurgeDisposition::Partial;
            report.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::Operational,
                "manifest removal completed but its directory sync was uncertain",
            ));
            report.continuation = Some(self.continuation(
                &request.root_identifier,
                &selector_name,
                Some(OWNER_METADATA_CURSOR.to_owned()),
            ));
            let _ = FileExt::unlock(&lock);
            report.bounds = budget.bounds();
            return Ok(report);
        }
        let _ = FileExt::unlock(&lock);
        drop(lock);
        if attempt_root.directory.remove_file("live.lock").is_err()
            || attempt_root.remove_empty().is_err()
        {
            report.disposition = PurgeDisposition::Partial;
            report.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::Operational,
                "owner metadata or empty attempt root could not be removed last",
            ));
            report.continuation = Some(self.continuation(
                &request.root_identifier,
                &selector_name,
                Some(OWNER_METADATA_CURSOR.to_owned()),
            ));
            report.bounds = budget.bounds();
            return Ok(report);
        }
        report.disposition = PurgeDisposition::Removed;
        report.removed_execution_ids.push(execution_id.to_owned());
        report.kept_execution_ids.clear();
        report.bounds = budget.bounds();
        Ok(report)
    }

    fn validate_request(&self, request: &PurgeRequest) -> Result<(), AttemptError> {
        self.root(&request.root_identifier)?;
        if request.plan_hash != self.plan_hash {
            return Err(AttemptError::InvalidQuery(
                "purge request is not bound to this compiled plan",
            ));
        }
        if let PurgeSelector::Execution(execution_id) = &request.selector {
            validate_execution_id(execution_id)?;
        }
        Ok(())
    }

    fn root(&self, root_identifier: &str) -> Result<&OwnedAttemptRoot, AttemptError> {
        self.roots
            .iter()
            .find(|root| root.identifier == root_identifier)
            .ok_or(AttemptError::InvalidQuery(
                "root identifier is not owned by the compiled query",
            ))
    }

    fn continuation(
        &self,
        root_identifier: &str,
        selector: &str,
        cursor: Option<String>,
    ) -> AttemptContinuation {
        let binding = continuation_binding(
            &self.plan_hash,
            root_identifier,
            selector,
            cursor.as_deref(),
        );
        AttemptContinuation {
            schema: CONTINUATION_SCHEMA.to_owned(),
            plan_hash: self.plan_hash,
            root_identifier: root_identifier.to_owned(),
            selector: selector.to_owned(),
            cursor,
            binding,
        }
    }

    fn validate_selector_continuation(
        &self,
        root_identifier: &str,
        selector: &str,
        continuation: Option<&AttemptContinuation>,
    ) -> Result<Option<String>, AttemptError> {
        let Some(continuation) = continuation else {
            return Ok(None);
        };
        validate_continuation(continuation)?;
        if continuation.plan_hash != self.plan_hash
            || continuation.root_identifier != root_identifier
            || continuation.selector != selector
        {
            return Err(AttemptError::InvalidContinuation(
                "continuation is not bound to this compiled plan, root, and selector",
            ));
        }
        let token = blake3::hash(&continuation.to_bytes()?).to_hex().to_string();
        let mut consumed = self
            .consumed_continuations
            .lock()
            .map_err(|_| AttemptError::InvalidContinuation("continuation replay guard failed"))?;
        if !consumed.insert(token) {
            return Err(AttemptError::InvalidContinuation(
                "continuation selector was already consumed",
            ));
        }
        Ok(continuation.cursor.clone())
    }
}

#[derive(Debug)]
struct QueryBudget {
    entry_limit: u64,
    byte_limit: u64,
    time_limit_ms: u64,
    bounds: AttemptQueryBounds,
    last_elapsed_ms: Option<u64>,
}

impl QueryBudget {
    fn new(policy: &ResolvedPublicationPolicy) -> Self {
        Self {
            entry_limit: policy.sweep_entry_limit(),
            byte_limit: policy.sweep_byte_limit(),
            time_limit_ms: policy.sweep_time_limit_ms(),
            bounds: AttemptQueryBounds::default(),
            last_elapsed_ms: None,
        }
    }

    fn resume(policy: &ResolvedPublicationPolicy, bounds: AttemptQueryBounds) -> Self {
        Self {
            entry_limit: policy.sweep_entry_limit(),
            byte_limit: policy.sweep_byte_limit(),
            time_limit_ms: policy.sweep_time_limit_ms(),
            bounds,
            last_elapsed_ms: None,
        }
    }

    fn observe_time(&mut self, elapsed_ms: u64) -> Result<(), AttemptError> {
        if self
            .last_elapsed_ms
            .is_some_and(|previous| elapsed_ms < previous)
        {
            return Err(AttemptError::InvalidQuery(
                "monotonic elapsed time moved backwards",
            ));
        }
        self.last_elapsed_ms = Some(elapsed_ms);
        self.bounds.elapsed_ms = elapsed_ms;
        Ok(())
    }

    fn visit(
        &mut self,
        kind: ContainedEntryKind,
        size_bytes: Option<u64>,
        elapsed_ms: u64,
    ) -> Result<(), CleanupDebt> {
        if self
            .last_elapsed_ms
            .is_some_and(|previous| elapsed_ms < previous)
        {
            return Err(CleanupDebt::new(
                CleanupDebtKind::MonotonicClock,
                "monotonic elapsed time moved backwards",
            ));
        }
        self.last_elapsed_ms = Some(elapsed_ms);
        self.bounds.elapsed_ms = elapsed_ms;
        if elapsed_ms >= self.time_limit_ms {
            return Err(CleanupDebt::new(
                CleanupDebtKind::TimeBudget,
                "monotonic time budget stopped the query",
            ));
        }
        if self.bounds.considered_entries == self.entry_limit {
            return Err(CleanupDebt::new(
                CleanupDebtKind::EntryBudget,
                "entry budget stopped the query",
            ));
        }
        self.bounds.considered_entries =
            self.bounds
                .considered_entries
                .checked_add(1)
                .ok_or_else(|| {
                    CleanupDebt::new(
                        CleanupDebtKind::EntryBudget,
                        "entry accounting overflow stopped the query",
                    )
                })?;
        if kind == ContainedEntryKind::File {
            let bytes = size_bytes.ok_or_else(|| {
                CleanupDebt::new(
                    CleanupDebtKind::Operational,
                    "regular-file metadata omitted its byte length",
                )
            })?;
            let next = self
                .bounds
                .considered_bytes
                .checked_add(bytes)
                .ok_or_else(|| {
                    CleanupDebt::new(
                        CleanupDebtKind::ByteBudget,
                        "considered-byte accounting overflow stopped the query",
                    )
                })?;
            if next > self.byte_limit {
                return Err(CleanupDebt::new(
                    CleanupDebtKind::ByteBudget,
                    "considered-byte budget stopped the query",
                ));
            }
            self.bounds.considered_bytes = next;
        }
        Ok(())
    }

    fn check_time(&mut self, elapsed_ms: u64) -> Result<(), CleanupDebt> {
        if self
            .last_elapsed_ms
            .is_some_and(|previous| elapsed_ms < previous)
        {
            return Err(CleanupDebt::new(
                CleanupDebtKind::MonotonicClock,
                "monotonic elapsed time moved backwards",
            ));
        }
        self.last_elapsed_ms = Some(elapsed_ms);
        self.bounds.elapsed_ms = elapsed_ms;
        if elapsed_ms >= self.time_limit_ms {
            Err(CleanupDebt::new(
                CleanupDebtKind::TimeBudget,
                "monotonic time budget stopped cleanup",
            ))
        } else {
            Ok(())
        }
    }

    fn remaining_entries_as_usize(&self) -> usize {
        usize::try_from(
            self.entry_limit
                .saturating_sub(self.bounds.considered_entries),
        )
        .unwrap_or(usize::MAX)
    }

    fn bounds(&self) -> AttemptQueryBounds {
        self.bounds
    }
}

fn inspect_owned_attempt<F>(
    root: &OwnedAttemptRoot,
    execution_id: &str,
    observed_unix_ms: u64,
    policy: &ResolvedPublicationPolicy,
    budget: &mut QueryBudget,
    elapsed_ms: &mut F,
) -> AttemptInspection
where
    F: FnMut() -> u64,
{
    let mut inspection = AttemptInspection {
        execution_id: execution_id.to_owned(),
        disposition: CleanupDisposition::Kept,
        state: None,
        created_unix_ms: None,
        eligible_after_unix_ms: None,
        artifact_ids: Vec::new(),
        cleanup_debt: Vec::new(),
        bounds: budget.bounds(),
        physical_path: None,
        eligible: false,
    };
    let attempt_root = match AttemptRoot::open(&root.destination, execution_id) {
        Ok(Some(attempt_root)) => attempt_root,
        Ok(None) => {
            inspection.disposition = CleanupDisposition::AlreadyAbsent;
            inspection.bounds = budget.bounds();
            return inspection;
        }
        Err(_) => {
            inspection.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::UnsafeEntry,
                "execution directory failed handle-relative confinement checks",
            ));
            inspection.bounds = budget.bounds();
            return inspection;
        }
    };
    inspection.physical_path = Some(attempt_root.path.clone());
    let mut entries = match attempt_root
        .directory
        .bounded_entries(budget.remaining_entries_as_usize())
    {
        Ok(entries) => entries,
        Err(_) => {
            inspection.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::Operational,
                "attempt directory enumeration failed",
            ));
            inspection.bounds = budget.bounds();
            return inspection;
        }
    };
    entries
        .entries
        .sort_by(|left, right| left.name.cmp(&right.name));
    let mut observed = BTreeMap::new();
    for entry in entries.entries {
        if let Err(debt) = budget.visit(entry.kind, entry.size_bytes, elapsed_ms()) {
            inspection.cleanup_debt.push(debt);
            inspection.bounds = budget.bounds();
            return inspection;
        }
        let Ok(name) = entry.name.into_string() else {
            inspection.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::InvalidOwnership,
                "attempt contains a non-UTF-8 child",
            ));
            continue;
        };
        if observed.insert(name, entry.kind).is_some() {
            inspection.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::InvalidOwnership,
                "attempt contains duplicate child identities",
            ));
        }
    }
    if !entries.complete {
        inspection.cleanup_debt.push(CleanupDebt::new(
            CleanupDebtKind::EntryBudget,
            "entry budget stopped attempt inspection",
        ));
        inspection.bounds = budget.bounds();
        return inspection;
    }
    if observed.get("live.lock") != Some(&ContainedEntryKind::File)
        || observed.get("manifest.json") != Some(&ContainedEntryKind::File)
    {
        inspection.cleanup_debt.push(CleanupDebt::new(
            CleanupDebtKind::InvalidOwnership,
            "attempt ownership metadata is missing or not a regular file",
        ));
        inspection.bounds = budget.bounds();
        return inspection;
    }
    if observed.values().any(|kind| {
        matches!(
            kind,
            ContainedEntryKind::LinkOrReparse
                | ContainedEntryKind::Directory
                | ContainedEntryKind::Other
        )
    }) {
        inspection.cleanup_debt.push(CleanupDebt::new(
            CleanupDebtKind::UnsafeEntry,
            "attempt contains a link, reparse point, directory, or unsupported entry",
        ));
    }

    let manifest = match read_manifest_from_anchor(&attempt_root.directory, observed_unix_ms) {
        Ok(manifest) => manifest,
        Err(error) => {
            inspection.cleanup_debt.push(manifest_cleanup_debt(&error));
            inspection.bounds = budget.bounds();
            return inspection;
        }
    };
    if manifest.execution_id != execution_id {
        inspection.cleanup_debt.push(CleanupDebt::new(
            CleanupDebtKind::InvalidOwnership,
            "manifest execution identity does not match the typed selector",
        ));
    }
    inspection.state = Some(manifest.state);
    inspection.created_unix_ms = Some(manifest.created_unix_ms);
    inspection.eligible_after_unix_ms = Some(manifest.eligible_after_unix_ms);
    inspection.artifact_ids = manifest
        .artifacts
        .iter()
        .map(|artifact| artifact.artifact_id.clone())
        .collect();

    let expected = manifest
        .artifacts
        .iter()
        .map(|artifact| artifact.artifact_id.as_str())
        .chain(["live.lock", "manifest.json"])
        .collect::<std::collections::BTreeSet<_>>();
    if observed
        .keys()
        .any(|name| !expected.contains(name.as_str()))
    {
        inspection.cleanup_debt.push(CleanupDebt::new(
            CleanupDebtKind::UnknownChild,
            "attempt contains a child not named by its ownership manifest",
        ));
    }
    for artifact in &manifest.artifacts {
        match observed.get(&artifact.artifact_id) {
            Some(ContainedEntryKind::File) => {
                if attempt_root
                    .directory
                    .open_file(&artifact.artifact_id)
                    .is_err()
                {
                    inspection.cleanup_debt.push(CleanupDebt::new(
                        CleanupDebtKind::Operational,
                        "owned artifact could not be opened through its retained handle",
                    ));
                }
            }
            None => inspection.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::Operational,
                "manifest-owned artifact is already absent",
            )),
            Some(_) => inspection.cleanup_debt.push(CleanupDebt::new(
                CleanupDebtKind::UnsafeEntry,
                "manifest-owned artifact is not a regular file",
            )),
        }
    }

    match attempt_root.directory.open_file("live.lock") {
        Ok(lock) => {
            if FileExt::try_lock(&lock).is_err() {
                inspection.cleanup_debt.push(CleanupDebt::new(
                    CleanupDebtKind::LiveAttempt,
                    "attempt liveness lock is held by a writer",
                ));
            } else {
                let _ = FileExt::unlock(&lock);
            }
        }
        Err(_) => inspection.cleanup_debt.push(CleanupDebt::new(
            CleanupDebtKind::InvalidOwnership,
            "attempt liveness metadata could not be opened",
        )),
    }

    inspection.eligible = match cleanup_eligible(&manifest, observed_unix_ms, policy) {
        Ok(eligible) => eligible,
        Err(debt) => {
            inspection.cleanup_debt.push(debt);
            false
        }
    } && !inspection.cleanup_debt.iter().any(|debt| {
        matches!(
            debt.kind,
            CleanupDebtKind::LiveAttempt
                | CleanupDebtKind::InvalidOwnership
                | CleanupDebtKind::InvalidManifest
                | CleanupDebtKind::UnknownChild
                | CleanupDebtKind::UnsafeEntry
                | CleanupDebtKind::ClockAmbiguous
        )
    });
    inspection.bounds = budget.bounds();
    inspection
}

fn cleanup_eligible(
    manifest: &AttemptManifest,
    observed_unix_ms: u64,
    policy: &ResolvedPublicationPolicy,
) -> Result<bool, CleanupDebt> {
    if manifest.created_unix_ms > observed_unix_ms
        || manifest.eligible_after_unix_ms < manifest.created_unix_ms
    {
        return Err(CleanupDebt::new(
            CleanupDebtKind::ClockAmbiguous,
            "durable attempt timestamps are inconsistent with the observation clock",
        ));
    }
    let eligible_after = match manifest.state {
        AttemptState::Complete => manifest.created_unix_ms,
        AttemptState::Incomplete | AttemptState::Abandoned => {
            let retention_ms = policy
                .failed_retention_seconds()
                .checked_mul(1_000)
                .ok_or_else(|| {
                    CleanupDebt::new(
                        CleanupDebtKind::ClockAmbiguous,
                        "failed-attempt retention overflows the durable clock",
                    )
                })?;
            manifest
                .created_unix_ms
                .checked_add(retention_ms)
                .ok_or_else(|| {
                    CleanupDebt::new(
                        CleanupDebtKind::ClockAmbiguous,
                        "failed-attempt eligibility overflows the durable clock",
                    )
                })?
        }
        AttemptState::Staging | AttemptState::Ready | AttemptState::Publishing => {
            manifest.eligible_after_unix_ms
        }
    };
    Ok(eligible_after <= observed_unix_ms)
}

fn manifest_cleanup_debt(error: &AttemptError) -> CleanupDebt {
    match error {
        AttemptError::InvalidManifest(
            "eligible clock precedes attempt creation"
            | "attempt creation clock is later than observation clock",
        ) => CleanupDebt::new(
            CleanupDebtKind::ClockAmbiguous,
            "durable attempt timestamps are inconsistent with the observation clock",
        ),
        _ => CleanupDebt::new(
            CleanupDebtKind::InvalidManifest,
            "attempt manifest is malformed, unsupported, or unreadable",
        ),
    }
}

fn is_budget_debt(kind: CleanupDebtKind) -> bool {
    matches!(
        kind,
        CleanupDebtKind::EntryBudget
            | CleanupDebtKind::ByteBudget
            | CleanupDebtKind::TimeBudget
            | CleanupDebtKind::MonotonicClock
    )
}

fn purge_selector_name(selector: &PurgeSelector) -> String {
    match selector {
        PurgeSelector::Execution(execution_id) => format!("purge-execution:{execution_id}"),
        PurgeSelector::Expired => "purge-expired".to_owned(),
    }
}

fn validate_continuation(continuation: &AttemptContinuation) -> Result<(), AttemptError> {
    if continuation.schema != CONTINUATION_SCHEMA {
        return Err(AttemptError::InvalidContinuation(
            "unsupported attempt continuation schema",
        ));
    }
    if continuation.root_identifier.len() != 64
        || !continuation
            .root_identifier
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(AttemptError::InvalidContinuation(
            "continuation root identifier is invalid",
        ));
    }
    if continuation.selector != "list"
        && !continuation.selector.starts_with("purge-execution:")
        && continuation.selector != "purge-expired"
    {
        return Err(AttemptError::InvalidContinuation(
            "continuation selector is unsupported",
        ));
    }
    if continuation.cursor.as_ref().is_some_and(|cursor| {
        cursor.len() > 128 || cursor.is_empty() || cursor.contains(['/', '\\'])
    }) {
        return Err(AttemptError::InvalidContinuation(
            "continuation cursor is invalid",
        ));
    }
    if continuation.binding
        != continuation_binding(
            &continuation.plan_hash,
            &continuation.root_identifier,
            &continuation.selector,
            continuation.cursor.as_deref(),
        )
    {
        return Err(AttemptError::InvalidContinuation(
            "continuation binding does not match its selector and cursor",
        ));
    }
    Ok(())
}

fn continuation_binding(
    plan_hash: &[u8; 32],
    root_identifier: &str,
    selector: &str,
    cursor: Option<&str>,
) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CONTINUATION_SCHEMA.as_bytes());
    hasher.update(&[0]);
    hasher.update(plan_hash);
    hasher.update(&[0]);
    hasher.update(root_identifier.as_bytes());
    hasher.update(&[0]);
    hasher.update(selector.as_bytes());
    hasher.update(&[0]);
    if let Some(cursor) = cursor {
        hasher.update(cursor.as_bytes());
    }
    *hasher.finalize().as_bytes()
}

fn owned_root_identifier(path: &Path) -> String {
    blake3::hash(destination_root_key(path).as_bytes())
        .to_hex()
        .to_string()
}

fn legacy_inspection(execution_id: &str, disposition: CleanupDisposition) -> AttemptInspection {
    AttemptInspection {
        execution_id: execution_id.to_owned(),
        disposition,
        state: None,
        created_unix_ms: None,
        eligible_after_unix_ms: None,
        artifact_ids: Vec::new(),
        cleanup_debt: Vec::new(),
        bounds: AttemptQueryBounds::default(),
        physical_path: None,
        eligible: disposition == CleanupDisposition::Removed,
    }
}

/// Retained ownership boundary for one execution directory.
#[derive(Debug)]
pub struct AttemptRoot {
    namespace: AnchoredDirectory,
    directory: AnchoredDirectory,
    execution_id: String,
    path: PathBuf,
}

impl AttemptRoot {
    fn create(destination_root: &ValidatedPath, execution_id: &str) -> Result<Self, AttemptError> {
        let destination = AnchoredDirectory::open(destination_root)?;
        let namespace = match destination.create_child(".clinker-attempts") {
            Ok(namespace) => namespace,
            Err(error) if containment_kind(&error) == Some(std::io::ErrorKind::AlreadyExists) => {
                destination.open_child(".clinker-attempts")?
            }
            Err(error) => return Err(error.into()),
        };
        let directory = namespace.create_child(execution_id)?;
        let path = directory.path().to_path_buf();
        Ok(Self {
            namespace,
            directory,
            execution_id: execution_id.to_owned(),
            path,
        })
    }

    fn open(
        destination_root: &ValidatedPath,
        execution_id: &str,
    ) -> Result<Option<Self>, AttemptError> {
        let destination = AnchoredDirectory::open(destination_root)?;
        let namespace = match destination.open_child(".clinker-attempts") {
            Ok(namespace) => namespace,
            Err(error) if containment_kind(&error) == Some(std::io::ErrorKind::NotFound) => {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let directory = match namespace.open_child(execution_id) {
            Ok(directory) => directory,
            Err(error) if containment_kind(&error) == Some(std::io::ErrorKind::NotFound) => {
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let path = directory.path().to_path_buf();
        Ok(Some(Self {
            namespace,
            directory,
            execution_id: execution_id.to_owned(),
            path,
        }))
    }

    fn remove_empty(self) -> Result<(), AttemptError> {
        let Self {
            namespace,
            directory,
            execution_id,
            ..
        } = self;
        drop(directory);
        namespace.remove_child(&execution_id)?;
        namespace.sync()?;
        Ok(())
    }
}

#[derive(Debug)]
struct ArtifactRuntime {
    kind: ArtifactKind,
    artifact_id: String,
    logical_leaf: String,
    final_path: PathBuf,
    local_source: ValidatedPath,
    publication_source: ValidatedPath,
    publication_root_key: String,
    copied_from_local: bool,
}

#[derive(Debug)]
struct AdditionalAttemptRoot {
    root: AttemptRoot,
    lock_file: Option<File>,
}

/// One destination-owned execution attempt.
pub struct AttemptPublication {
    execution_id: String,
    attempt_root: Option<AttemptRoot>,
    owner_root_key: String,
    owner_profile: DestinationProfile,
    additional_roots: BTreeMap<String, AdditionalAttemptRoot>,
    destination_root_keys: Vec<String>,
    manifest_path: PathBuf,
    lock_file: Option<File>,
    manifest: AttemptManifest,
    artifacts: Vec<ArtifactRuntime>,
    policy: Option<ResolvedPublicationPolicy>,
    terminal: bool,
    fault: Option<AttemptFault>,
    test_hook: Option<Box<dyn FnOnce(AttemptTestEvent) + Send>>,
}

impl std::fmt::Debug for AttemptPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AttemptPublication")
            .field("execution_id", &self.execution_id)
            .field(
                "attempt_root",
                &self.attempt_root.as_ref().map(|root| root.path.as_path()),
            )
            .field("manifest", &self.manifest)
            .field("destination_root_count", &self.destination_root_keys.len())
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

impl AttemptPublication {
    pub fn create(
        destination_root: ValidatedPath,
        execution_id: &str,
        created_unix_ms: u64,
        eligible_after_unix_ms: u64,
    ) -> Result<Self, AttemptError> {
        Self::create_with_owner_profile(
            destination_root,
            DestinationProfile::Local,
            execution_id,
            created_unix_ms,
            eligible_after_unix_ms,
        )
    }

    fn create_with_owner_profile(
        destination_root: ValidatedPath,
        owner_profile: DestinationProfile,
        execution_id: &str,
        created_unix_ms: u64,
        eligible_after_unix_ms: u64,
    ) -> Result<Self, AttemptError> {
        validate_execution_id(execution_id)?;
        let owner_root_key = destination_root_key(destination_root.as_path());
        let attempt_root = AttemptRoot::create(&destination_root, execution_id)?;
        let lock_path = attempt_root.path.join("live.lock");
        let lock_file = attempt_root.directory.create_file("live.lock")?;
        FileExt::try_lock(&lock_file).map_err(|source| AttemptError::Io {
            operation: "lock live attempt",
            path: lock_path.clone(),
            source: source.into(),
        })?;
        let manifest = AttemptManifest::new(
            execution_id,
            created_unix_ms,
            eligible_after_unix_ms,
            AttemptState::Staging,
            Vec::new(),
        )?;
        let mut publication = Self {
            execution_id: execution_id.to_owned(),
            manifest_path: attempt_root.path.join("manifest.json"),
            lock_file: Some(lock_file),
            attempt_root: Some(attempt_root),
            owner_root_key: owner_root_key.clone(),
            owner_profile,
            additional_roots: BTreeMap::new(),
            destination_root_keys: vec![owner_root_key],
            manifest,
            artifacts: Vec::new(),
            policy: None,
            terminal: false,
            fault: None,
            test_hook: None,
        };
        publication.persist_manifest(false)?;
        Ok(publication)
    }

    /// Create one run-owned attempt for a complete, pre-registered artifact
    /// set.
    ///
    /// Duplicate destinations and over-limit registrations are rejected before
    /// `.clinker-attempts` is created. Direct mode owns quarantine in each
    /// destination parent. Local-then-publish owns its writer files in the
    /// configured local spool and compiles a bounded destination-root map for
    /// the verified copy step.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] for invalid registration cardinality,
    /// collisions, path validation, contained root creation, manifest writes,
    /// or artifact admission failures.
    pub fn create_run(
        policy: ResolvedPublicationPolicy,
        registry: &OutputStagingRegistry,
        execution_id: &str,
        created_unix_ms: u64,
        eligible_after_unix_ms: u64,
        registrations: Vec<ArtifactRegistration>,
    ) -> Result<(Self, Vec<AttemptArtifactWriter>), AttemptError> {
        validate_execution_id(execution_id)?;
        if registrations.is_empty() || registrations.len() > MANIFEST_MAX_ARTIFACTS {
            return Err(AttemptError::InvalidManifest(
                "run registration count is outside the bounded artifact range",
            ));
        }

        let mut claims = BTreeMap::new();
        let mut destination_roots = BTreeMap::new();
        for registration in &registrations {
            let destination_key = clinker_plan::config::collision_key(
                &registration.destination.as_path().to_string_lossy(),
            );
            if let Some(first) =
                claims.insert(destination_key, registration.producer_label.as_str())
            {
                return Err(AttemptError::RegistrationCollision {
                    first: first.to_owned(),
                    second: registration.producer_label.clone(),
                });
            }
            let parent = destination_parent(&registration.destination)?;
            destination_roots
                .entry(destination_root_key(parent.as_path()))
                .or_insert(parent);
        }

        let owner_root = match policy.mode() {
            PublicationMode::Direct => destination_roots
                .first_key_value()
                .map(|(_, root)| root.clone())
                .ok_or(AttemptError::InvalidManifest(
                    "run registration has no destination root",
                ))?,
            PublicationMode::LocalThenPublish => {
                let spool = policy
                    .local_spool_dir()
                    .ok_or(AttemptError::InvalidTransition(
                        "resolved policy is missing local spool",
                    ))?;
                validate_path(Path::new("."), spool, false).map_err(|_| {
                    AttemptError::InvalidManifest("local spool path failed validation")
                })?
            }
        };

        // Exact remote-profile verification happens through the retained
        // destination-parent containment boundary before any attempt directory
        // is created. The policy layer has already rejected an unqualified
        // network destination; this probe distinguishes NFS from SMB.
        for registration in &registrations {
            OutputContainment::for_profile(
                registration.destination.clone(),
                containment_profile(policy.destination_profile()),
            )?;
        }

        let owner_profile = match policy.mode() {
            PublicationMode::Direct => policy.destination_profile(),
            PublicationMode::LocalThenPublish => DestinationProfile::Local,
        };
        let mut attempt = Self::create_with_owner_profile(
            owner_root,
            owner_profile,
            execution_id,
            created_unix_ms,
            eligible_after_unix_ms,
        )?;
        attempt.policy = Some(policy);
        attempt.destination_root_keys.clear();
        for (key, root) in destination_roots {
            attempt.ensure_destination_root(key.clone(), root)?;
            attempt.destination_root_keys.push(key);
        }
        attempt.destination_root_keys.sort();

        let mut writers = Vec::with_capacity(registrations.len());
        for registration in registrations {
            writers.push(attempt.stage_registered(registry, registration)?);
        }
        Ok((attempt, writers))
    }

    /// Existing execution identity shared by every managed artifact.
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    /// Number of distinct compiled destination-parent roots in this run.
    pub fn destination_root_count(&self) -> usize {
        self.destination_root_keys.len()
    }

    /// Artifact roles in deterministic registration order.
    pub fn registered_kinds(&self) -> Vec<ArtifactKind> {
        self.artifacts
            .iter()
            .map(|artifact| artifact.kind)
            .collect()
    }

    /// Return physical paths only for a caller that explicitly opts into a
    /// sanitized rendering path. Default run outcomes never contain them.
    pub fn physical_paths_for_sanitized_output(
        &self,
        _opt_in: SanitizedPathOptIn,
    ) -> Vec<ArtifactPhysicalPaths> {
        self.artifacts
            .iter()
            .map(|artifact| ArtifactPhysicalPaths {
                artifact_id: artifact.artifact_id.clone(),
                final_path: artifact.final_path.clone(),
                quarantine_path: artifact.publication_source.as_path().to_path_buf(),
            })
            .collect()
    }

    fn ensure_destination_root(
        &mut self,
        key: String,
        destination_root: ValidatedPath,
    ) -> Result<(), AttemptError> {
        if key == self.owner_root_key || self.additional_roots.contains_key(&key) {
            return Ok(());
        }
        if self.additional_roots.len() >= MANIFEST_MAX_ARTIFACTS {
            return Err(AttemptError::InvalidManifest(
                "destination root map exceeds its bound",
            ));
        }
        let root = AttemptRoot::create(&destination_root, &self.execution_id)?;
        let lock_path = root.path.join("live.lock");
        let lock_file = root.directory.create_file("live.lock")?;
        FileExt::try_lock(&lock_file).map_err(|source| AttemptError::Io {
            operation: "lock destination attempt root",
            path: lock_path,
            source: source.into(),
        })?;
        self.additional_roots.insert(
            key,
            AdditionalAttemptRoot {
                root,
                lock_file: Some(lock_file),
            },
        );
        Ok(())
    }

    fn stage_registered(
        &mut self,
        registry: &OutputStagingRegistry,
        registration: ArtifactRegistration,
    ) -> Result<AttemptArtifactWriter, AttemptError> {
        if self.terminal || self.manifest.state != AttemptState::Staging {
            return Err(AttemptError::InvalidTransition(
                "attempt no longer accepts artifacts",
            ));
        }
        let policy = self.policy.as_ref().ok_or(AttemptError::InvalidTransition(
            "run attempt has no resolved publication policy",
        ))?;
        let mode = policy.mode();
        let profile = policy.destination_profile();
        let artifact_id = format!("artifact-{:08x}", self.artifacts.len() + 1);
        let destination_root = destination_parent(&registration.destination)?;
        let publication_root_key = destination_root_key(destination_root.as_path());
        self.ensure_destination_root(publication_root_key.clone(), destination_root)?;

        let publication_leaf = if mode == PublicationMode::LocalThenPublish
            && publication_root_key == self.owner_root_key
        {
            format!("{artifact_id}.destination")
        } else {
            artifact_id.clone()
        };
        let publication_source =
            self.validated_artifact_in_root(&publication_root_key, &publication_leaf)?;
        let (local_source, file, copied_from_local) = match mode {
            PublicationMode::Direct => {
                let file =
                    self.create_artifact_in_root(&publication_root_key, &publication_leaf)?;
                (publication_source.clone(), file, false)
            }
            PublicationMode::LocalThenPublish => {
                let source = self.validated_artifact_in_root(&self.owner_root_key, &artifact_id)?;
                let file = self.create_artifact_in_root(&self.owner_root_key, &artifact_id)?;
                (source, file, true)
            }
        };

        let destination_boundary = OutputContainment::for_profile(
            registration.destination.clone(),
            containment_profile(profile),
        )?;
        registry.register_attempt_output(
            registration.producer_label.clone(),
            registration.destination.as_path().to_path_buf(),
            destination_boundary,
            publication_source.clone(),
            registration.disposition,
        )?;
        let entry = ArtifactManifest::new(
            &artifact_id,
            &registration.producer_label,
            &registration.logical_leaf,
            0,
            &"0".repeat(64),
            ArtifactState::Staging,
        )?;
        let mut next = self.manifest.clone();
        next.artifacts.push(entry);
        next.artifact_count = next.artifacts.len();
        self.persist_replacement(next, false)?;
        self.artifacts.push(ArtifactRuntime {
            kind: registration.kind,
            artifact_id: artifact_id.clone(),
            logical_leaf: registration.logical_leaf,
            final_path: registration.destination.as_path().to_path_buf(),
            local_source,
            publication_source,
            publication_root_key,
            copied_from_local,
        });
        Ok(AttemptArtifactWriter {
            execution_id: self.execution_id.clone(),
            artifact_id,
            kind: registration.kind,
            file,
        })
    }

    fn create_artifact_in_root(&self, root_key: &str, leaf: &str) -> Result<File, AttemptError> {
        if root_key == self.owner_root_key {
            return self
                .attempt_root
                .as_ref()
                .ok_or(AttemptError::InvalidTransition("attempt root was removed"))?
                .directory
                .create_file(leaf)
                .map_err(AttemptError::from);
        }
        self.additional_roots
            .get(root_key)
            .ok_or(AttemptError::InvalidTransition(
                "destination attempt root is missing",
            ))?
            .root
            .directory
            .create_file(leaf)
            .map_err(AttemptError::from)
    }

    fn validated_artifact_in_root(
        &self,
        root_key: &str,
        leaf: &str,
    ) -> Result<ValidatedPath, AttemptError> {
        let root_path = if root_key == self.owner_root_key {
            &self
                .attempt_root
                .as_ref()
                .ok_or(AttemptError::InvalidTransition("attempt root was removed"))?
                .path
        } else {
            &self
                .additional_roots
                .get(root_key)
                .ok_or(AttemptError::InvalidTransition(
                    "destination attempt root is missing",
                ))?
                .root
                .path
        };
        validate_path(Path::new(leaf), root_path, false)
            .map_err(|_| AttemptError::InvalidManifest("artifact path failed validation"))
    }

    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn attempt_root(&self) -> &Path {
        &self
            .attempt_root
            .as_ref()
            .expect("attempt root exists until successful cleanup")
            .path
    }

    pub fn stage_direct(
        &mut self,
        registry: &OutputStagingRegistry,
        destination: ValidatedPath,
        producer_label: &str,
        logical_leaf: &str,
        disposition: PromotionDisposition,
    ) -> Result<(String, File), AttemptError> {
        if self.terminal || self.manifest.state != AttemptState::Staging {
            return Err(AttemptError::InvalidTransition(
                "attempt no longer accepts artifacts",
            ));
        }
        let artifact_id = format!("artifact-{:08x}", self.artifacts.len() + 1);
        let attempt_root = self
            .attempt_root
            .as_ref()
            .ok_or(AttemptError::InvalidTransition("attempt root was removed"))?;
        let source = validate_path(Path::new(&artifact_id), &attempt_root.path, false)
            .map_err(|_| AttemptError::InvalidManifest("artifact path failed validation"))?;
        let file = attempt_root.directory.create_file(&artifact_id)?;
        let destination_boundary =
            OutputContainment::for_profile(destination.clone(), "local-filesystem")?;
        registry.register_attempt_output(
            producer_label.to_owned(),
            destination.as_path().to_path_buf(),
            destination_boundary,
            source.clone(),
            disposition,
        )?;
        let entry = ArtifactManifest::new(
            &artifact_id,
            producer_label,
            logical_leaf,
            0,
            &"0".repeat(64),
            ArtifactState::Staging,
        )?;
        let mut next = self.manifest.clone();
        next.artifacts.push(entry);
        next.artifact_count = next.artifacts.len();
        self.persist_replacement(next, false)?;
        self.artifacts.push(ArtifactRuntime {
            kind: ArtifactKind::Primary,
            artifact_id: artifact_id.clone(),
            logical_leaf: logical_leaf.to_owned(),
            final_path: destination.as_path().to_path_buf(),
            local_source: source.clone(),
            publication_source: source,
            publication_root_key: self.owner_root_key.clone(),
            copied_from_local: false,
        });
        Ok((artifact_id, file))
    }

    pub fn mark_ready(&mut self, artifact_id: &str) -> Result<(), AttemptError> {
        if self.fault == Some(AttemptFault::Write) {
            return Err(AttemptError::Injected("artifact write"));
        }
        let runtime = self
            .artifacts
            .iter()
            .find(|artifact| artifact.artifact_id == artifact_id)
            .ok_or(AttemptError::InvalidTransition(
                "artifact is not registered",
            ))?;
        let local_source = runtime.local_source.clone();
        let publication_source = runtime.publication_source.clone();
        let publication_root_key = runtime.publication_root_key.clone();
        let copied_from_local = runtime.copied_from_local;

        let local_path = local_source.as_path();
        let mut local_file = File::open(local_path).map_err(|source| AttemptError::Io {
            operation: "open staged artifact",
            path: local_path.to_path_buf(),
            source,
        })?;
        if self.fault == Some(AttemptFault::FileSync) {
            return Err(AttemptError::Injected("artifact file sync"));
        }
        local_file.sync_all().map_err(|source| AttemptError::Io {
            operation: "sync staged artifact",
            path: local_path.to_path_buf(),
            source,
        })?;
        let local_size = local_file
            .metadata()
            .map_err(|source| AttemptError::Io {
                operation: "inspect staged artifact",
                path: local_path.to_path_buf(),
                source,
            })?
            .len();
        let (size_bytes, digest) = if copied_from_local {
            if self.fault == Some(AttemptFault::Copy) {
                return Err(AttemptError::Injected("artifact copy"));
            }
            local_file.rewind().map_err(|source| AttemptError::Io {
                operation: "rewind local artifact",
                path: local_path.to_path_buf(),
                source,
            })?;
            let publication_leaf = publication_source
                .as_path()
                .file_name()
                .and_then(|leaf| leaf.to_str())
                .ok_or(AttemptError::InvalidManifest(
                    "publication artifact leaf is not UTF-8",
                ))?;
            let mut destination_file =
                self.create_artifact_in_root(&publication_root_key, publication_leaf)?;
            let mut source_hasher = blake3::Hasher::new();
            let mut copied = 0_u64;
            let mut buffer = vec![0_u8; PUBLICATION_COPY_BUFFER_BYTES];
            loop {
                let read = local_file
                    .read(&mut buffer)
                    .map_err(|source| AttemptError::Io {
                        operation: "read local publication artifact",
                        path: local_path.to_path_buf(),
                        source,
                    })?;
                if read == 0 {
                    break;
                }
                copied = copied
                    .checked_add(read as u64)
                    .ok_or(AttemptError::InvalidManifest(
                        "copied artifact byte count overflows u64",
                    ))?;
                source_hasher.update(&buffer[..read]);
                destination_file
                    .write_all(&buffer[..read])
                    .map_err(|source| AttemptError::Io {
                        operation: "copy artifact into destination quarantine",
                        path: publication_source.as_path().to_path_buf(),
                        source,
                    })?;
            }
            if self.fault == Some(AttemptFault::DestinationFileSync) {
                return Err(AttemptError::Injected(
                    "destination quarantine artifact sync",
                ));
            }
            destination_file
                .sync_all()
                .map_err(|source| AttemptError::Io {
                    operation: "sync destination quarantine artifact",
                    path: publication_source.as_path().to_path_buf(),
                    source,
                })?;
            drop(destination_file);
            if self.fault == Some(AttemptFault::Digest) {
                return Err(AttemptError::Injected("destination artifact digest"));
            }
            let (destination_size, destination_digest) = digest_file(publication_source.as_path())?;
            let source_digest = source_hasher.finalize().to_hex().to_string();
            if copied != local_size
                || destination_size != local_size
                || destination_digest != source_digest
            {
                return Err(AttemptError::IntegrityMismatch {
                    expected_bytes: local_size,
                    observed_bytes: destination_size,
                });
            }
            (destination_size, destination_digest)
        } else {
            if self.fault == Some(AttemptFault::Digest) {
                return Err(AttemptError::Injected("artifact digest"));
            }
            digest_file(local_path)?
        };
        let mut next = self.manifest.clone();
        let entry = next
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.artifact_id == artifact_id)
            .ok_or(AttemptError::InvalidTransition(
                "artifact receipt is missing",
            ))?;
        entry.size_bytes = size_bytes;
        entry.blake3_hex = digest;
        entry.state = ArtifactState::Ready;
        next.total_bytes = next.artifacts.iter().try_fold(0_u64, |total, artifact| {
            total
                .checked_add(artifact.size_bytes)
                .ok_or(AttemptError::InvalidManifest(
                    "artifact byte total overflows u64",
                ))
        })?;
        if let Some(policy) = &self.policy {
            let admitted_estimate = policy.explain().estimated_attempt_bytes;
            if next.total_bytes > policy.max_attempt_bytes() || next.total_bytes > admitted_estimate
            {
                return Err(AttemptError::AttemptByteLimitExceeded {
                    actual_bytes: next.total_bytes,
                    admitted_bytes: admitted_estimate.min(policy.max_attempt_bytes()),
                });
            }
        }
        if next
            .artifacts
            .iter()
            .all(|artifact| artifact.state == ArtifactState::Ready)
        {
            next.state = AttemptState::Ready;
        }
        self.persist_replacement(next, self.fault == Some(AttemptFault::ManifestReplace))?;

        if copied_from_local {
            let owner = self
                .attempt_root
                .as_ref()
                .ok_or(AttemptError::InvalidTransition("attempt root was removed"))?;
            owner.directory.remove_file(artifact_id)?;
            owner.directory.sync()?;
        }
        Ok(())
    }

    pub fn publish(
        &mut self,
        registry: &OutputStagingRegistry,
        shutdown: &ShutdownToken,
    ) -> Result<Option<PublicationOutcome>, AttemptError> {
        if self.terminal || self.manifest.state != AttemptState::Ready {
            return Err(AttemptError::InvalidTransition(
                "attempt is not eligible for publication",
            ));
        }
        if !shutdown.try_begin_publication() {
            let mut abandoned = self.manifest.clone();
            abandoned.state = AttemptState::Abandoned;
            self.persist_replacement(abandoned, false)?;
            self.terminal = true;
            return Ok(None);
        }
        let mut publishing = self.manifest.clone();
        publishing.state = AttemptState::Publishing;
        self.persist_replacement(publishing, false)?;
        let outcome = match self.fault {
            Some(AttemptFault::BeforeRename) => registry.commit_all_inner(Some(0), None, None)?,
            Some(AttemptFault::DirectorySync) => registry.commit_all_inner(None, Some(0), None)?,
            _ => registry.commit_all()?,
        };
        let mut finished = self.manifest.clone();
        match &outcome {
            PublicationOutcome::Complete { .. } => {
                finished.state = AttemptState::Complete;
                for artifact in &mut finished.artifacts {
                    artifact.state = ArtifactState::Published;
                }
            }
            PublicationOutcome::Incomplete {
                published,
                visible_unsynchronized,
                unpublished,
                ..
            } => {
                finished.state = AttemptState::Incomplete;
                for (index, artifact) in finished.artifacts.iter_mut().enumerate() {
                    if index < published.len() {
                        artifact.state = ArtifactState::Published;
                    } else if index < published.len() + visible_unsynchronized.len() {
                        artifact.state = ArtifactState::VisibleUnsynchronized;
                    } else if !unpublished.is_empty() {
                        artifact.state = ArtifactState::Unpublished;
                    }
                }
            }
        }
        self.persist_replacement(finished, false)?;
        self.terminal = true;
        if outcome.is_complete() {
            if let Some(hook) = self.test_hook.take() {
                hook(AttemptTestEvent {
                    execution_id: self.execution_id.clone(),
                    stage: AttemptTestStage::CompleteBeforeCleanup,
                });
            }
            self.remove_completed_attempt()?;
        }
        Ok(Some(outcome))
    }

    /// Publish a pre-registered run and return path-free logical truth.
    ///
    /// Cancellation before the publication gate returns `None` after the
    /// durable abandoned transition. Physical path disclosure remains outside
    /// this default result type.
    ///
    /// # Errors
    ///
    /// Returns [`AttemptError`] for invalid transitions, manifest persistence,
    /// containment, or publication failures.
    pub fn publish_run(
        &mut self,
        registry: &OutputStagingRegistry,
        shutdown: &ShutdownToken,
    ) -> Result<Option<AttemptPublicationOutcome>, AttemptError> {
        let Some(outcome) = self.publish(registry, shutdown)? else {
            return Ok(None);
        };
        let cleanup_debt_count = outcome.cleanup_debt().len();
        let artifacts = self
            .artifacts
            .iter()
            .map(|runtime| {
                let state = self
                    .manifest
                    .artifacts
                    .iter()
                    .find(|entry| entry.artifact_id == runtime.artifact_id)
                    .map_or(ArtifactState::Unpublished, |entry| entry.state);
                ArtifactPublicationResult {
                    artifact_id: runtime.artifact_id.clone(),
                    kind: runtime.kind,
                    logical_leaf: runtime.logical_leaf.clone(),
                    state,
                }
            })
            .collect();
        let logical = if outcome.is_complete() {
            AttemptPublicationOutcome::Complete {
                execution_id: self.execution_id.clone(),
                artifacts,
                cleanup_debt_count,
            }
        } else {
            AttemptPublicationOutcome::Incomplete {
                execution_id: self.execution_id.clone(),
                artifacts,
                cleanup_debt_count,
            }
        };
        Ok(Some(logical))
    }

    /// Inspect and, only with exact ownership proof, remove one orphaned attempt.
    pub fn cleanup(
        destination_root: ValidatedPath,
        execution_id: &str,
        observed_unix_ms: u64,
    ) -> Result<AttemptInspection, AttemptError> {
        validate_execution_id(execution_id)?;
        let kept = || legacy_inspection(execution_id, CleanupDisposition::Kept);
        let Some(attempt_root) = (match AttemptRoot::open(&destination_root, execution_id) {
            Ok(root) => root,
            Err(_) => return Ok(kept()),
        }) else {
            return Ok(legacy_inspection(
                execution_id,
                CleanupDisposition::AlreadyAbsent,
            ));
        };

        let lock = match attempt_root.directory.open_file("live.lock") {
            Ok(lock) => lock,
            Err(_) => return Ok(kept()),
        };
        if FileExt::try_lock(&lock).is_err() {
            return Ok(kept());
        }

        let entries = match attempt_root.directory.entries(MANIFEST_MAX_ARTIFACTS + 3) {
            Ok(entries) => entries,
            Err(_) => return Ok(kept()),
        };
        let mut observed = BTreeMap::new();
        for entry in entries {
            let Ok(name) = entry.name.into_string() else {
                return Ok(kept());
            };
            if observed.insert(name, entry.kind).is_some() {
                return Ok(kept());
            }
        }
        if observed.get("live.lock") != Some(&ContainedEntryKind::File)
            || observed.get("manifest.json") != Some(&ContainedEntryKind::File)
        {
            return Ok(kept());
        }

        let manifest = match read_manifest_from_anchor(&attempt_root.directory, observed_unix_ms) {
            Ok(manifest) => manifest,
            Err(_) => return Ok(kept()),
        };
        if manifest.execution_id != execution_id
            || manifest.eligible_after_unix_ms > observed_unix_ms
        {
            return Ok(kept());
        }
        let expected_count = manifest.artifacts.len() + 2;
        if observed.len() != expected_count {
            return Ok(kept());
        }
        for artifact in &manifest.artifacts {
            if observed.get(&artifact.artifact_id) != Some(&ContainedEntryKind::File)
                || attempt_root
                    .directory
                    .open_file(&artifact.artifact_id)
                    .is_err()
            {
                return Ok(kept());
            }
        }

        for artifact in &manifest.artifacts {
            if attempt_root
                .directory
                .remove_file(&artifact.artifact_id)
                .is_err()
            {
                return Ok(kept());
            }
        }
        if attempt_root.directory.sync().is_err()
            || attempt_root.directory.remove_file("manifest.json").is_err()
            || attempt_root.directory.sync().is_err()
        {
            return Ok(kept());
        }
        let _ = FileExt::unlock(&lock);
        drop(lock);
        if attempt_root.directory.remove_file("live.lock").is_err() {
            return Ok(kept());
        }
        if attempt_root.remove_empty().is_err() {
            return Ok(kept());
        }
        Ok(legacy_inspection(execution_id, CleanupDisposition::Removed))
    }

    pub fn set_fault_for_testing(&mut self, fault: AttemptFault) {
        self.fault = Some(fault);
    }

    pub fn install_test_hook<F>(&mut self, hook: F)
    where
        F: FnOnce(AttemptTestEvent) + Send + 'static,
    {
        self.test_hook = Some(Box::new(hook));
    }

    fn persist_replacement(
        &mut self,
        manifest: AttemptManifest,
        inject_replace_failure: bool,
    ) -> Result<(), AttemptError> {
        manifest.validate(None)?;
        let old = std::mem::replace(&mut self.manifest, manifest);
        if let Err(error) = self.persist_manifest(inject_replace_failure) {
            self.manifest = old;
            return Err(error);
        }
        Ok(())
    }

    fn persist_manifest(&mut self, inject_replace_failure: bool) -> Result<(), AttemptError> {
        let attempt_root = self
            .attempt_root
            .as_ref()
            .ok_or(AttemptError::InvalidTransition("attempt root was removed"))?;
        let next_path = attempt_root.path.join("manifest.next");
        match attempt_root.directory.remove_file("manifest.next") {
            Ok(()) => {}
            Err(error) if containment_kind(&error) == Some(std::io::ErrorKind::NotFound) => {}
            Err(error) => return Err(error.into()),
        }
        let mut next = attempt_root.directory.create_file("manifest.next")?;
        next.write_all(&self.manifest.to_bytes()?)
            .and_then(|()| next.sync_all())
            .map_err(|source| AttemptError::Io {
                operation: "write attempt manifest replacement",
                path: next_path.clone(),
                source,
            })?;
        drop(next);
        if inject_replace_failure {
            return Err(AttemptError::Injected("manifest replacement"));
        }
        let source = validate_path(Path::new("manifest.next"), &attempt_root.path, false)
            .map_err(|_| AttemptError::InvalidManifest("manifest source failed validation"))?;
        let destination = validate_path(Path::new("manifest.json"), &attempt_root.path, false)
            .map_err(|_| AttemptError::InvalidManifest("manifest destination failed validation"))?;
        OutputContainment::for_profile(destination, containment_profile(self.owner_profile))?
            .promote_from(source, PromotionDisposition::Replace)?;
        Ok(())
    }

    fn remove_completed_attempt(&mut self) -> Result<(), AttemptError> {
        let additional_roots = std::mem::take(&mut self.additional_roots);
        for (_, mut additional) in additional_roots {
            if let Some(lock) = additional.lock_file.take() {
                let _ = FileExt::unlock(&lock);
                drop(lock);
            }
            additional.root.directory.remove_file("live.lock")?;
            additional.root.remove_empty()?;
        }
        let attempt_root = self
            .attempt_root
            .as_ref()
            .ok_or(AttemptError::InvalidTransition("attempt root was removed"))?;
        attempt_root.directory.remove_file("manifest.json")?;
        attempt_root.directory.sync()?;
        if let Some(lock) = self.lock_file.take() {
            let _ = FileExt::unlock(&lock);
            drop(lock);
        }
        attempt_root.directory.remove_file("live.lock")?;
        let attempt_root = self
            .attempt_root
            .take()
            .ok_or(AttemptError::InvalidTransition("attempt root was removed"))?;
        attempt_root.remove_empty()
    }
}

#[derive(Debug, Error)]
pub enum AttemptError {
    #[error("invalid attempt manifest: {0}")]
    InvalidManifest(&'static str),
    #[error("invalid attempt transition: {0}")]
    InvalidTransition(&'static str),
    #[error("invalid attempt query: {0}")]
    InvalidQuery(&'static str),
    #[error("invalid attempt continuation: {0}")]
    InvalidContinuation(&'static str),
    #[error("injected attempt publication failure at {0}")]
    Injected(&'static str),
    #[error("artifact destination collision between producers {first:?} and {second:?}")]
    RegistrationCollision { first: String, second: String },
    #[error(
        "destination artifact integrity mismatch: expected {expected_bytes} bytes, observed {observed_bytes} bytes"
    )]
    IntegrityMismatch {
        expected_bytes: u64,
        observed_bytes: u64,
    },
    #[error(
        "attempt artifacts total {actual_bytes} bytes exceeds the admitted bound {admitted_bytes} bytes"
    )]
    AttemptByteLimitExceeded {
        actual_bytes: u64,
        admitted_bytes: u64,
    },
    #[error("attempt manifest serialization failed: {0}")]
    Serialize(serde_json::Error),
    #[error("attempt manifest parsing failed: {0}")]
    Deserialize(serde_json::Error),
    #[error("{operation} failed for {path}: {source}", path = path.display())]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(transparent)]
    Containment(#[from] ContainmentError),
    #[error(transparent)]
    Pipeline(#[from] PipelineError),
}

fn destination_parent(destination: &ValidatedPath) -> Result<ValidatedPath, AttemptError> {
    let parent = destination
        .as_path()
        .parent()
        .ok_or(AttemptError::InvalidManifest(
            "artifact destination has no parent",
        ))?;
    validate_path(Path::new("."), parent, false)
        .map_err(|_| AttemptError::InvalidManifest("destination parent failed validation"))
}

fn destination_root_key(path: &Path) -> String {
    clinker_plan::config::collision_key(&path.to_string_lossy())
}

fn containment_profile(profile: DestinationProfile) -> &'static str {
    match profile {
        DestinationProfile::Local => "local-filesystem",
        DestinationProfile::NfsV4_1 => "linux-nfsv4.1-loopback-ci",
        DestinationProfile::Smb3_1_1 => "linux-smb3.1.1-loopback-ci",
    }
}

fn digest_file(path: &Path) -> Result<(u64, String), AttemptError> {
    let mut file = File::open(path).map_err(|source| AttemptError::Io {
        operation: "open artifact for digest",
        path: path.to_path_buf(),
        source,
    })?;
    let mut size = 0_u64;
    let mut hasher = blake3::Hasher::new();
    let mut buffer = vec![0_u8; PUBLICATION_COPY_BUFFER_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(|source| AttemptError::Io {
            operation: "digest staged artifact",
            path: path.to_path_buf(),
            source,
        })?;
        if read == 0 {
            break;
        }
        size = size
            .checked_add(read as u64)
            .ok_or(AttemptError::InvalidManifest(
                "artifact digest byte count overflows u64",
            ))?;
        hasher.update(&buffer[..read]);
    }
    Ok((size, hasher.finalize().to_hex().to_string()))
}

fn validate_execution_id(execution_id: &str) -> Result<(), AttemptError> {
    let parsed = uuid::Uuid::parse_str(execution_id)
        .map_err(|_| AttemptError::InvalidManifest("execution_id must be a UUID"))?;
    if parsed.hyphenated().to_string() != execution_id {
        return Err(AttemptError::InvalidManifest(
            "execution_id must be a lowercase hyphenated UUID",
        ));
    }
    Ok(())
}

fn read_manifest_from_anchor(
    directory: &AnchoredDirectory,
    observed_unix_ms: u64,
) -> Result<AttemptManifest, AttemptError> {
    let mut file = directory.open_file("manifest.json")?;
    let mut bytes = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take((MANIFEST_MAX_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|source| AttemptError::Io {
            operation: "read anchored attempt manifest",
            path: directory.path().join("manifest.json"),
            source,
        })?;
    AttemptManifest::from_bytes(&bytes, observed_unix_ms)
}

fn containment_kind(error: &ContainmentError) -> Option<std::io::ErrorKind> {
    match error {
        ContainmentError::Io { source, .. }
        | ContainmentError::VisibleButUnsynced { source, .. } => Some(source.kind()),
        ContainmentError::PublishedCleanup { .. }
        | ContainmentError::SecurityPolicy { .. }
        | ContainmentError::PolicyRequired { .. } => None,
    }
}

fn validate_artifact_id(artifact_id: &str) -> Result<(), AttemptError> {
    if artifact_id.len() != 17
        || !artifact_id.starts_with("artifact-")
        || !artifact_id[9..]
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(AttemptError::InvalidManifest(
            "artifact_id must be artifact- followed by eight lowercase hexadecimal characters",
        ));
    }
    Ok(())
}

fn validate_text(
    field: &'static str,
    value: &str,
    max_chars: usize,
    max_encoded_bytes: usize,
) -> Result<(), AttemptError> {
    if value.is_empty()
        || value.chars().count() > max_chars
        || serde_json::to_string(value)
            .map_err(AttemptError::Serialize)?
            .len()
            .saturating_sub(2)
            > max_encoded_bytes
    {
        return Err(match field {
            "producer_label" => AttemptError::InvalidManifest(
                "producer_label exceeds its character or encoded byte limit",
            ),
            _ => AttemptError::InvalidManifest(
                "logical_leaf exceeds its character or encoded byte limit",
            ),
        });
    }
    Ok(())
}
