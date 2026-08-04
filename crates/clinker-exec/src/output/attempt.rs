//! Destination-owned attempt manifests and finite artifact publication.

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use clinker_plan::error::PipelineError;
use clinker_plan::security::{ValidatedPath, validate_path};
use fs4::FileExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::containment::{ContainmentError, OutputContainment, PromotionDisposition};
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
    FileSync,
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

#[derive(Debug)]
struct ArtifactRuntime {
    artifact_id: String,
    source: ValidatedPath,
}

/// One destination-owned execution attempt.
pub struct AttemptPublication {
    execution_id: String,
    attempt_root: PathBuf,
    manifest_path: PathBuf,
    lock_path: PathBuf,
    lock_file: Option<File>,
    manifest: AttemptManifest,
    artifacts: Vec<ArtifactRuntime>,
    terminal: bool,
    fault: Option<AttemptFault>,
    test_hook: Option<Box<dyn FnOnce(AttemptTestEvent) + Send>>,
}

impl std::fmt::Debug for AttemptPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AttemptPublication")
            .field("execution_id", &self.execution_id)
            .field("attempt_root", &self.attempt_root)
            .field("manifest", &self.manifest)
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
        validate_execution_id(execution_id)?;
        let attempt_root = destination_root
            .as_path()
            .join(".clinker-attempts")
            .join(execution_id);
        create_restrictive_directory(&attempt_root)?;
        let lock_path = attempt_root.join("live.lock");
        let lock_file = restrictive_file(&lock_path, true)?;
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
            manifest_path: attempt_root.join("manifest.json"),
            lock_path,
            lock_file: Some(lock_file),
            attempt_root,
            manifest,
            artifacts: Vec::new(),
            terminal: false,
            fault: None,
            test_hook: None,
        };
        publication.persist_manifest(false)?;
        Ok(publication)
    }

    pub fn manifest_path(&self) -> &Path {
        &self.manifest_path
    }

    pub fn attempt_root(&self) -> &Path {
        &self.attempt_root
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
        let source_path = self.attempt_root.join(&artifact_id);
        let source = validate_path(Path::new(&artifact_id), &self.attempt_root, false)
            .map_err(|_| AttemptError::InvalidManifest("artifact path failed validation"))?;
        let file = restrictive_file(&source_path, true)?;
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
            artifact_id: artifact_id.clone(),
            source,
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
        let path = runtime.source.as_path();
        let mut file = File::open(path).map_err(|source| AttemptError::Io {
            operation: "open staged artifact",
            path: path.to_path_buf(),
            source,
        })?;
        if self.fault == Some(AttemptFault::FileSync) {
            return Err(AttemptError::Injected("artifact file sync"));
        }
        file.sync_all().map_err(|source| AttemptError::Io {
            operation: "sync staged artifact",
            path: path.to_path_buf(),
            source,
        })?;
        let size_bytes = file
            .metadata()
            .map_err(|source| AttemptError::Io {
                operation: "inspect staged artifact",
                path: path.to_path_buf(),
                source,
            })?
            .len();
        let mut hasher = blake3::Hasher::new();
        let mut buffer = [0_u8; 64 * 1024];
        loop {
            let read = file.read(&mut buffer).map_err(|source| AttemptError::Io {
                operation: "digest staged artifact",
                path: path.to_path_buf(),
                source,
            })?;
            if read == 0 {
                break;
            }
            hasher.update(&buffer[..read]);
        }
        let mut next = self.manifest.clone();
        let entry = next
            .artifacts
            .iter_mut()
            .find(|artifact| artifact.artifact_id == artifact_id)
            .ok_or(AttemptError::InvalidTransition(
                "artifact receipt is missing",
            ))?;
        entry.size_bytes = size_bytes;
        entry.blake3_hex = hasher.finalize().to_hex().to_string();
        entry.state = ArtifactState::Ready;
        next.total_bytes = next
            .artifacts
            .iter()
            .map(|artifact| artifact.size_bytes)
            .sum();
        if next
            .artifacts
            .iter()
            .all(|artifact| artifact.state == ArtifactState::Ready)
        {
            next.state = AttemptState::Ready;
        }
        self.persist_replacement(next, self.fault == Some(AttemptFault::ManifestReplace))
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
        let next_path = self.attempt_root.join("manifest.next");
        match std::fs::remove_file(&next_path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(AttemptError::Io {
                    operation: "remove stale manifest replacement",
                    path: next_path,
                    source,
                });
            }
        }
        let mut next = restrictive_file(&next_path, true)?;
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
        let source = validate_path(Path::new("manifest.next"), &self.attempt_root, false)
            .map_err(|_| AttemptError::InvalidManifest("manifest source failed validation"))?;
        let destination = validate_path(Path::new("manifest.json"), &self.attempt_root, false)
            .map_err(|_| AttemptError::InvalidManifest("manifest destination failed validation"))?;
        OutputContainment::for_profile(destination, "local-filesystem")?
            .promote_from(source, PromotionDisposition::Replace)?;
        Ok(())
    }

    fn remove_completed_attempt(&mut self) -> Result<(), AttemptError> {
        std::fs::remove_file(&self.manifest_path).map_err(|source| AttemptError::Io {
            operation: "remove complete manifest",
            path: self.manifest_path.clone(),
            source,
        })?;
        if let Some(lock) = self.lock_file.take() {
            let _ = FileExt::unlock(&lock);
            drop(lock);
        }
        std::fs::remove_file(&self.lock_path).map_err(|source| AttemptError::Io {
            operation: "remove attempt lock",
            path: self.lock_path.clone(),
            source,
        })?;
        std::fs::remove_dir(&self.attempt_root).map_err(|source| AttemptError::Io {
            operation: "remove completed attempt directory",
            path: self.attempt_root.clone(),
            source,
        })
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

fn create_restrictive_directory(path: &Path) -> Result<(), AttemptError> {
    std::fs::create_dir_all(path).map_err(|source| AttemptError::Io {
        operation: "create attempt directory",
        path: path.to_path_buf(),
        source,
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(
            |source| AttemptError::Io {
                operation: "restrict attempt directory",
                path: path.to_path_buf(),
                source,
            },
        )?;
    }
    Ok(())
}

fn restrictive_file(path: &Path, create_new: bool) -> Result<File, AttemptError> {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create_new(create_new);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options.open(path).map_err(|source| AttemptError::Io {
        operation: "create restrictive attempt file",
        path: path.to_path_buf(),
        source,
    })
}
