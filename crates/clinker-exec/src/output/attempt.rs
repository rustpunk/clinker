//! Destination-owned attempt manifests and finite artifact publication.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::{Path, PathBuf};

use clinker_plan::config::{DestinationProfile, PublicationMode, ResolvedPublicationPolicy};
use clinker_plan::error::PipelineError;
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
const PRODUCER_MAX_CHARS: usize = 96;
const PRODUCER_MAX_ENCODED_BYTES: usize = 192;
const LOGICAL_MAX_CHARS: usize = 384;
const LOGICAL_MAX_ENCODED_BYTES: usize = 512;
pub const ARTIFACT_MAX_ENCODED_BYTES: usize = 992;
pub const MANIFEST_MAX_ARTIFACTS: usize = 4096;
pub const MANIFEST_MAX_BYTES: usize = 4 * 1024 * 1024;
pub const PUBLICATION_COPY_BUFFER_BYTES: usize = 1024 * 1024;

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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttemptInspection {
    execution_id: String,
    disposition: CleanupDisposition,
}

impl AttemptInspection {
    pub fn execution_id(&self) -> &str {
        &self.execution_id
    }

    pub fn disposition(&self) -> CleanupDisposition {
        self.disposition
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
        let kept = || AttemptInspection {
            execution_id: execution_id.to_owned(),
            disposition: CleanupDisposition::Kept,
        };
        let Some(attempt_root) = (match AttemptRoot::open(&destination_root, execution_id) {
            Ok(root) => root,
            Err(_) => return Ok(kept()),
        }) else {
            return Ok(AttemptInspection {
                execution_id: execution_id.to_owned(),
                disposition: CleanupDisposition::AlreadyAbsent,
            });
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
        Ok(AttemptInspection {
            execution_id: execution_id.to_owned(),
            disposition: CleanupDisposition::Removed,
        })
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
