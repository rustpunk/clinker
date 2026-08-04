//! Strict release-evidence validation and durable atomic file transitions.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::os::fd::AsRawFd;
use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use chrono::{DateTime, FixedOffset, Timelike};
use nix::fcntl::FlockArg;
use regex::Regex;
use tempfile::{Builder, NamedTempFile};

use crate::canonical::{self, CanonicalValue};
use crate::error::GateError;
use crate::limits::{MAX_INPUT_BYTES, MAX_SCHEMA_BYTES, read_bounded};

const RELEASE_SCHEMA_ID: &str = "clinker.release-evidence/v1";
const CANDIDATE_SCHEMA_ID: &str = "clinker.candidate-evidence/v1";
const PUBLICATION_SCHEMA_ID: &str = "clinker.publication-evidence/v1";
const TARGETS: [&str; 4] = [
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
];

const CANDIDATE_FIELDS: [&str; 26] = [
    "schema",
    "kind",
    "candidate_authorization_sha256",
    "candidate_tag",
    "candidate_version",
    "source_sha",
    "build_workflow_sha",
    "publish_workflow_ref",
    "publish_workflow_ref_resolved_sha",
    "publish_workflow_sha",
    "candidate_release_id",
    "checksum_sha256",
    "archive_digests",
    "ci_run_ref",
    "changelog_ref",
    "inventory_ref",
    "authorized_release_maintainer_ref",
    "build_workflow_path",
    "build_run_id",
    "build_head_sha",
    "publish_workflow_path",
    "archives",
    "attestations",
    "tag_mutation_performed",
    "tag_readback_ref",
    "release_trigger_event_ref",
];

/// Strict release-evidence variant selected by the CLI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceKind {
    /// Immutable build candidate evidence.
    Candidate,
    /// Monotonic protected-publication evidence.
    Publication,
}

/// Result of a durable evidence write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvidenceWrite {
    /// A previously absent destination was installed.
    Created,
    /// The existing canonical bytes and semantic identity were identical.
    ExactReplay,
    /// A compare-and-swap transition replaced the prior revision.
    Replaced,
}

/// Expected identity for one compare-and-swap transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvidenceExpectation {
    revision: u64,
    state: String,
    immutable_authority_sha256: String,
}

impl EvidenceExpectation {
    /// Construct an expected prior evidence identity.
    #[must_use]
    pub fn new(
        revision: u64,
        state: impl Into<String>,
        immutable_authority_sha256: impl Into<String>,
    ) -> Self {
        Self {
            revision,
            state: state.into(),
            immutable_authority_sha256: immutable_authority_sha256.into(),
        }
    }
}

/// Deterministic fault point used to prove cleanup before final installation.
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    AfterTemporaryCreate,
    BeforeFileSync,
    BeforeInstall,
    BeforeDirectorySync,
}

/// Held nonblocking advisory lock for one evidence path.
pub struct EvidenceLock {
    file: File,
    #[allow(dead_code)]
    path: PathBuf,
}

impl EvidenceLock {
    /// Acquire the exclusive sibling lock without waiting.
    pub fn acquire(destination: &Path) -> Result<Self, GateError> {
        let path = sibling_lock_path(destination);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(nix::libc::O_CLOEXEC | nix::libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|error| GateError::io("open evidence lock", &error))?;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .map_err(|error| GateError::io("restrict evidence lock", &error))?;
        #[allow(deprecated)]
        nix::fcntl::flock(file.as_raw_fd(), FlockArg::LockExclusiveNonblock).map_err(|_| {
            GateError::policy(
                "evidence.lock_contended",
                "exclusive evidence lock is already held",
            )
        })?;
        Ok(Self { file, path })
    }
}

impl Drop for EvidenceLock {
    fn drop(&mut self) {
        #[allow(deprecated)]
        let _ = nix::fcntl::flock(self.file.as_raw_fd(), FlockArg::Unlock);
    }
}

/// Validate one evidence manifest without modifying it.
pub fn validate_file(
    kind: EvidenceKind,
    schema_path: &Path,
    manifest_path: &Path,
) -> Result<(), GateError> {
    let schema_bytes = read_bounded(
        schema_path,
        "read release evidence schema",
        MAX_SCHEMA_BYTES,
    )?;
    let schema = canonical::parse_json_with_limit(&schema_bytes, MAX_SCHEMA_BYTES)?;
    let schema = object(&schema, "release evidence schema")?;
    expect_exact_string(schema, "$id", RELEASE_SCHEMA_ID, "release evidence schema")?;

    let manifest_bytes = read_bounded(manifest_path, "read release evidence", MAX_INPUT_BYTES)?;
    let manifest = canonical::parse_json(&manifest_bytes)?;
    match kind {
        EvidenceKind::Candidate => validate_candidate(&manifest),
        EvidenceKind::Publication => validate_publication(&manifest),
    }
}

/// Install canonical evidence without replacing any existing destination.
pub fn create_only(destination: &Path, value: &CanonicalValue) -> Result<EvidenceWrite, GateError> {
    create_only_inner(destination, value, None)
}

/// Install canonical evidence while injecting one deterministic pre-install failure.
#[doc(hidden)]
pub fn create_only_with_fault(
    destination: &Path,
    value: &CanonicalValue,
    fault: FaultPoint,
) -> Result<EvidenceWrite, GateError> {
    create_only_inner(destination, value, Some(fault))
}

/// Replace one evidence revision after exclusive reread and identity comparison.
pub fn compare_and_swap(
    destination: &Path,
    expected: &EvidenceExpectation,
    next: &CanonicalValue,
) -> Result<EvidenceWrite, GateError> {
    compare_and_swap_inner(destination, expected, next, None)
}

/// Compare-and-swap with one deterministic pre-install failure.
#[doc(hidden)]
pub fn compare_and_swap_with_fault(
    destination: &Path,
    expected: &EvidenceExpectation,
    next: &CanonicalValue,
    fault: FaultPoint,
) -> Result<EvidenceWrite, GateError> {
    compare_and_swap_inner(destination, expected, next, Some(fault))
}

fn create_only_inner(
    destination: &Path,
    value: &CanonicalValue,
    fault: Option<FaultPoint>,
) -> Result<EvidenceWrite, GateError> {
    let bytes = canonical::to_bytes(value)?;
    let parent = parent_directory(destination);
    fs::create_dir_all(parent)
        .map_err(|error| GateError::io("create evidence directory", &error))?;
    if destination.exists() {
        return exact_replay_or_conflict(destination, &bytes);
    }
    let temporary = prepared_temporary(destination, &bytes, fault)?;
    checkpoint(fault, FaultPoint::BeforeInstall)?;
    checkpoint(fault, FaultPoint::BeforeDirectorySync)?;
    sync_directory(parent)?;
    match temporary.persist_noclobber(destination) {
        Ok(file) => {
            file.sync_all()
                .map_err(|error| GateError::io("sync installed evidence", &error))?;
            sync_directory(parent)?;
            Ok(EvidenceWrite::Created)
        }
        Err(error) if error.error.kind() == std::io::ErrorKind::AlreadyExists => {
            exact_replay_or_conflict(destination, &bytes)
        }
        Err(error) => Err(GateError::io("install create-only evidence", &error.error)),
    }
}

fn compare_and_swap_inner(
    destination: &Path,
    expected: &EvidenceExpectation,
    next: &CanonicalValue,
    fault: Option<FaultPoint>,
) -> Result<EvidenceWrite, GateError> {
    validate_sha256(
        &expected.immutable_authority_sha256,
        "expected immutable_authority_sha256",
    )?;
    if expected.state.trim().is_empty() {
        return Err(policy("expected state must be a non-empty string"));
    }
    let _lock = EvidenceLock::acquire(destination)?;
    let original = read_bounded(destination, "read current evidence", MAX_INPUT_BYTES)?;
    let current = canonical::parse_json(&original)?;
    let canonical_current = canonical::to_bytes(&current)?;
    if original != canonical_current {
        return Err(policy("current evidence bytes are not canonical v1"));
    }
    let identity = transition_identity(&current, "current evidence")?;
    if identity.revision != expected.revision
        || identity.state != expected.state
        || identity.authority != expected.immutable_authority_sha256
    {
        return Err(policy(
            "current evidence revision, state, or immutable authority is stale",
        ));
    }

    let next_bytes = canonical::to_bytes(next)?;
    if next_bytes == original {
        return Ok(EvidenceWrite::ExactReplay);
    }
    let next_identity = transition_identity(next, "next evidence")?;
    if next_identity.revision != identity.revision.saturating_add(1) {
        return Err(policy(
            "next evidence revision must increase by exactly one",
        ));
    }
    if next_identity.authority != identity.authority {
        return Err(policy("immutable evidence authority cannot change"));
    }

    let parent = parent_directory(destination);
    let temporary = prepared_temporary(destination, &next_bytes, fault)?;
    checkpoint(fault, FaultPoint::BeforeInstall)?;
    let reread = read_bounded(destination, "reread current evidence", MAX_INPUT_BYTES)?;
    if reread != original {
        return Err(policy("evidence changed during the locked transition"));
    }
    checkpoint(fault, FaultPoint::BeforeDirectorySync)?;
    sync_directory(parent)?;
    let installed = temporary
        .persist(destination)
        .map_err(|error| GateError::io("replace evidence atomically", &error.error))?;
    installed
        .sync_all()
        .map_err(|error| GateError::io("sync replaced evidence", &error))?;
    sync_directory(parent)?;
    Ok(EvidenceWrite::Replaced)
}

fn prepared_temporary(
    destination: &Path,
    bytes: &[u8],
    fault: Option<FaultPoint>,
) -> Result<NamedTempFile, GateError> {
    let parent = parent_directory(destination);
    let filename = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("evidence");
    let prefix = format!(".{filename}.");
    let mut temporary = Builder::new()
        .prefix(&prefix)
        .permissions(fs::Permissions::from_mode(0o600))
        .tempfile_in(parent)
        .map_err(|error| GateError::io("create sibling evidence temporary", &error))?;
    checkpoint(fault, FaultPoint::AfterTemporaryCreate)?;
    temporary
        .write_all(bytes)
        .map_err(|error| GateError::io("write evidence temporary", &error))?;
    temporary
        .flush()
        .map_err(|error| GateError::io("flush evidence temporary", &error))?;
    checkpoint(fault, FaultPoint::BeforeFileSync)?;
    temporary
        .as_file()
        .sync_all()
        .map_err(|error| GateError::io("sync evidence temporary", &error))?;
    Ok(temporary)
}

fn checkpoint(active: Option<FaultPoint>, point: FaultPoint) -> Result<(), GateError> {
    if active == Some(point) {
        return Err(GateError::internal(
            "evidence.injected_failure",
            "injected durability failure",
        ));
    }
    Ok(())
}

fn exact_replay_or_conflict(
    destination: &Path,
    canonical_bytes: &[u8],
) -> Result<EvidenceWrite, GateError> {
    let existing = read_bounded(destination, "read existing evidence", MAX_INPUT_BYTES)?;
    if existing == canonical_bytes {
        let parsed = canonical::parse_json(&existing)?;
        if canonical::to_bytes(&parsed)? == existing {
            return Ok(EvidenceWrite::ExactReplay);
        }
    }
    Err(policy(
        "existing evidence differs; create-only replacement is forbidden",
    ))
}

fn sync_directory(directory: &Path) -> Result<(), GateError> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|error| GateError::io("sync evidence directory", &error))
}

fn parent_directory(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

fn sibling_lock_path(destination: &Path) -> PathBuf {
    let mut name = destination.as_os_str().to_os_string();
    name.push(OsString::from(".lock"));
    PathBuf::from(name)
}

struct TransitionIdentity<'a> {
    revision: u64,
    state: &'a str,
    authority: &'a str,
}

fn transition_identity<'a>(
    value: &'a CanonicalValue,
    label: &str,
) -> Result<TransitionIdentity<'a>, GateError> {
    let object = object(value, label)?;
    let revision = required(object, "revision", label)?
        .as_u64()
        .ok_or_else(|| policy(format!("{label}.revision must be a non-negative integer")))?;
    let state = string(required(object, "state", label)?, &format!("{label}.state"))?;
    let authority = string(
        required(object, "immutable_authority_sha256", label)?,
        &format!("{label}.immutable_authority_sha256"),
    )?;
    validate_sha256(authority, &format!("{label}.immutable_authority_sha256"))?;
    Ok(TransitionIdentity {
        revision,
        state,
        authority,
    })
}

fn validate_candidate(value: &CanonicalValue) -> Result<(), GateError> {
    let candidate = object(value, "candidate evidence")?;
    let expected = CANDIDATE_FIELDS.into_iter().collect::<BTreeSet<_>>();
    exact_keys(candidate, &expected, "candidate evidence")?;
    expect_exact_string(
        candidate,
        "schema",
        CANDIDATE_SCHEMA_ID,
        "candidate evidence",
    )?;
    expect_exact_string(candidate, "kind", "candidate", "candidate evidence")?;
    validate_candidate_identity(candidate)?;
    validate_sha256_field(
        candidate,
        "candidate_authorization_sha256",
        "candidate evidence",
    )?;
    expect_exact_string(
        candidate,
        "build_workflow_path",
        ".github/workflows/release.yml",
        "candidate evidence",
    )?;
    expect_exact_string(
        candidate,
        "publish_workflow_path",
        ".github/workflows/publish-release.yml",
        "candidate evidence",
    )?;
    string(
        required(candidate, "build_run_id", "candidate evidence")?,
        "candidate evidence.build_run_id",
    )?;
    compare_fields(
        candidate,
        "build_head_sha",
        candidate,
        "source_sha",
        "candidate build head",
    )?;
    if required(candidate, "tag_mutation_performed", "candidate evidence")?.as_bool() != Some(false)
    {
        return Err(policy(
            "candidate evidence must record tag_mutation_performed=false",
        ));
    }
    let tag = string(
        required(candidate, "candidate_tag", "candidate evidence")?,
        "candidate evidence.candidate_tag",
    )?;
    let readback = string(
        required(candidate, "tag_readback_ref", "candidate evidence")?,
        "candidate evidence.tag_readback_ref",
    )?;
    if !readback.trim_end_matches('/').ends_with(tag) {
        return Err(policy(
            "candidate tag_readback_ref must identify candidate_tag",
        ));
    }
    string(
        required(candidate, "release_trigger_event_ref", "candidate evidence")?,
        "candidate evidence.release_trigger_event_ref",
    )?;
    validate_archives(candidate)?;
    validate_attestations(candidate)?;
    Ok(())
}

fn validate_candidate_identity(
    candidate: &BTreeMap<String, CanonicalValue>,
) -> Result<(), GateError> {
    let tag = string(
        required(candidate, "candidate_tag", "candidate evidence")?,
        "candidate evidence.candidate_tag",
    )?;
    let version = string(
        required(candidate, "candidate_version", "candidate evidence")?,
        "candidate evidence.candidate_version",
    )?;
    let semver = Regex::new(r"^v[0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$")
        .map_err(|_| GateError::internal("evidence.regex", "candidate-tag regex is invalid"))?;
    if !semver.is_match(tag) || tag.strip_prefix('v') != Some(version) {
        return Err(policy("candidate_tag must equal v plus candidate_version"));
    }
    for field in [
        "source_sha",
        "build_workflow_sha",
        "publish_workflow_ref_resolved_sha",
        "publish_workflow_sha",
    ] {
        validate_sha40_field(candidate, field, "candidate evidence")?;
    }
    if candidate.get("source_sha") != candidate.get("publish_workflow_ref_resolved_sha")
        || candidate.get("source_sha") != candidate.get("publish_workflow_sha")
    {
        return Err(policy(
            "publish workflow authority must resolve to candidate source_sha",
        ));
    }
    expect_exact_string(candidate, "publish_workflow_ref", tag, "candidate evidence")?;
    validate_sha256_field(candidate, "checksum_sha256", "candidate evidence")?;
    for field in [
        "candidate_release_id",
        "ci_run_ref",
        "changelog_ref",
        "authorized_release_maintainer_ref",
    ] {
        string(
            required(candidate, field, "candidate evidence")?,
            &format!("candidate evidence.{field}"),
        )?;
    }
    expect_exact_string(
        candidate,
        "inventory_ref",
        "release/inventory.toml",
        "candidate evidence",
    )?;
    let digests = object(
        required(candidate, "archive_digests", "candidate evidence")?,
        "candidate evidence.archive_digests",
    )?;
    exact_keys(
        digests,
        &TARGETS.into_iter().collect(),
        "candidate evidence.archive_digests",
    )?;
    for target in TARGETS {
        validate_sha256_field(digests, target, "candidate evidence.archive_digests")?;
    }
    Ok(())
}

fn validate_archives(candidate: &BTreeMap<String, CanonicalValue>) -> Result<(), GateError> {
    let archives = required(candidate, "archives", "candidate evidence")?
        .as_array()
        .ok_or_else(|| policy("candidate evidence.archives must be an array"))?;
    if archives.len() != TARGETS.len() {
        return Err(policy(
            "candidate evidence.archives must contain four entries",
        ));
    }
    let version = string(
        required(candidate, "candidate_version", "candidate evidence")?,
        "candidate evidence.candidate_version",
    )?;
    let digests = object(
        required(candidate, "archive_digests", "candidate evidence")?,
        "candidate evidence.archive_digests",
    )?;
    let mut seen = BTreeSet::new();
    let mut order = Vec::with_capacity(archives.len());
    for (index, value) in archives.iter().enumerate() {
        let archive = object(value, &format!("candidate evidence.archives[{index}]"))?;
        exact_keys(
            archive,
            &["target", "archive_name", "sha256"].into_iter().collect(),
            &format!("candidate evidence.archives[{index}]"),
        )?;
        let target = string(
            required(archive, "target", "archive")?,
            &format!("candidate evidence.archives[{index}].target"),
        )?;
        if !TARGETS.contains(&target) || !seen.insert(target) {
            return Err(policy(
                "candidate archives contain an invalid or duplicate target",
            ));
        }
        order.push(target);
        let extension = if target == "x86_64-pc-windows-msvc" {
            "zip"
        } else {
            "tar.gz"
        };
        let expected_name = format!("clinker-v{version}-{target}.{extension}");
        expect_exact_string(archive, "archive_name", &expected_name, "candidate archive")?;
        if archive.get("sha256") != digests.get(target) {
            return Err(policy("candidate archive digest does not match authority"));
        }
        validate_sha256_field(archive, "sha256", "candidate archive")?;
    }
    if order != TARGETS {
        return Err(policy("candidate archives must use stable target order"));
    }
    Ok(())
}

fn validate_attestations(candidate: &BTreeMap<String, CanonicalValue>) -> Result<(), GateError> {
    let attestations = required(candidate, "attestations", "candidate evidence")?
        .as_array()
        .ok_or_else(|| policy("candidate evidence.attestations must be an array"))?;
    let archives = required(candidate, "archives", "candidate evidence")?
        .as_array()
        .ok_or_else(|| policy("candidate evidence.archives must be an array"))?;
    if attestations.len() != TARGETS.len() {
        return Err(policy(
            "candidate evidence.attestations must contain four entries",
        ));
    }
    let archive_by_name = archives
        .iter()
        .filter_map(CanonicalValue::as_object)
        .filter_map(|archive| {
            Some((
                archive.get("archive_name")?.as_str()?,
                archive.get("sha256")?,
            ))
        })
        .collect::<BTreeMap<_, _>>();
    let tag = string(
        required(candidate, "candidate_tag", "candidate evidence")?,
        "candidate evidence.candidate_tag",
    )?;
    let source = required(candidate, "source_sha", "candidate evidence")?;
    let mut names = Vec::with_capacity(attestations.len());
    for (index, value) in attestations.iter().enumerate() {
        let label = format!("candidate evidence.attestations[{index}]");
        let attestation = object(value, &label)?;
        exact_keys(
            attestation,
            &[
                "archive_name",
                "subject_sha256",
                "repository",
                "workflow",
                "ref",
                "source_sha",
                "runner_environment",
            ]
            .into_iter()
            .collect(),
            &label,
        )?;
        let name = string(required(attestation, "archive_name", &label)?, &label)?;
        names.push(name);
        if archive_by_name.get(name) != Some(&required(attestation, "subject_sha256", &label)?) {
            return Err(policy("attestation subject does not match archive digest"));
        }
        expect_exact_string(attestation, "repository", "rustpunk/clinker", &label)?;
        expect_exact_string(
            attestation,
            "workflow",
            ".github/workflows/release.yml",
            &label,
        )?;
        expect_exact_string(attestation, "ref", &format!("refs/tags/{tag}"), &label)?;
        if attestation.get("source_sha") != Some(source) {
            return Err(policy("attestation source_sha does not match candidate"));
        }
        expect_exact_string(attestation, "runner_environment", "github-hosted", &label)?;
        validate_sha256_field(attestation, "subject_sha256", &label)?;
    }
    let mut sorted = names.clone();
    sorted.sort_unstable();
    sorted.dedup();
    if names != sorted || names.len() != archive_by_name.len() {
        return Err(policy(
            "attestations must uniquely use stable archive-name order",
        ));
    }
    Ok(())
}

fn validate_publication(value: &CanonicalValue) -> Result<(), GateError> {
    let publication = object(value, "publication evidence")?;
    let state = string(
        required(publication, "state", "publication evidence")?,
        "publication evidence.state",
    )?;
    let mut expected = [
        "schema",
        "kind",
        "state",
        "candidate",
        "candidate_authorization_sha256",
        "candidate_authorization_blob_sha",
        "approval_record_blob_sha",
        "approval_record_sha256",
        "dispatch",
        "inspection",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if matches!(state, "protected-approved" | "published-verified") {
        expected.insert("approval");
    }
    if state == "published-verified" {
        expected.insert("protected_job");
        expected.insert("public_verification");
    }
    exact_keys(publication, &expected, "publication evidence")?;
    expect_exact_string(
        publication,
        "schema",
        PUBLICATION_SCHEMA_ID,
        "publication evidence",
    )?;
    expect_exact_string(publication, "kind", "publication", "publication evidence")?;
    if !matches!(
        state,
        "awaiting-protected-environment-approval"
            | "inspection-in-progress"
            | "protected-approved"
            | "published-verified"
    ) {
        return Err(policy("publication evidence state is invalid"));
    }
    let candidate_value = required(publication, "candidate", "publication evidence")?;
    validate_candidate(candidate_value)?;
    let candidate = object(candidate_value, "publication evidence.candidate")?;
    validate_sha256_field(
        publication,
        "candidate_authorization_sha256",
        "publication evidence",
    )?;
    if publication.get("candidate_authorization_sha256")
        != candidate.get("candidate_authorization_sha256")
    {
        return Err(policy(
            "publication and candidate authorization digests must match",
        ));
    }
    validate_sha40_field(
        publication,
        "candidate_authorization_blob_sha",
        "publication evidence",
    )?;
    validate_sha40_field(
        publication,
        "approval_record_blob_sha",
        "publication evidence",
    )?;
    validate_sha256_field(
        publication,
        "approval_record_sha256",
        "publication evidence",
    )?;
    let dispatch = validate_dispatch(required(publication, "dispatch", "publication evidence")?)?;
    let inspection = validate_inspection(
        required(publication, "inspection", "publication evidence")?,
        state,
    )?;
    if let Some(started) = inspection.get("started_at") {
        let started = timestamp_value(started, "inspection.started_at")?;
        let dispatched = timestamp_value(
            required(dispatch, "dispatched_at", "dispatch")?,
            "dispatch.dispatched_at",
        )?;
        if started < dispatched {
            return Err(policy("inspection cannot begin before dispatch"));
        }
    }
    if matches!(state, "protected-approved" | "published-verified") {
        validate_approval(
            required(publication, "approval", "publication evidence")?,
            publication,
            dispatch,
            inspection,
        )?;
    }
    if state == "published-verified" {
        validate_public_fragment(publication, dispatch)?;
    }
    Ok(())
}

fn validate_dispatch(
    value: &CanonicalValue,
) -> Result<&BTreeMap<String, CanonicalValue>, GateError> {
    let dispatch = object(value, "dispatch")?;
    exact_keys(
        dispatch,
        &[
            "dispatch_id",
            "run_id",
            "run_url",
            "job_id",
            "job_name",
            "environment",
            "trigger_actor_ref",
            "dispatched_at",
        ]
        .into_iter()
        .collect(),
        "dispatch",
    )?;
    for field in [
        "dispatch_id",
        "run_id",
        "run_url",
        "job_id",
        "job_name",
        "trigger_actor_ref",
    ] {
        string(
            required(dispatch, field, "dispatch")?,
            &format!("dispatch.{field}"),
        )?;
    }
    expect_exact_string(dispatch, "environment", "release", "dispatch")?;
    timestamp_value(
        required(dispatch, "dispatched_at", "dispatch")?,
        "dispatch.dispatched_at",
    )?;
    Ok(dispatch)
}

fn validate_inspection<'a>(
    value: &'a CanonicalValue,
    state: &str,
) -> Result<&'a BTreeMap<String, CanonicalValue>, GateError> {
    let inspection = object(value, "inspection")?;
    let status = string(
        required(inspection, "status", "inspection")?,
        "inspection.status",
    )?;
    let expected = match status {
        "not-started" => ["status"].into_iter().collect(),
        "in-progress" => [
            "status",
            "inspector_actor_ref",
            "started_at",
            "evidence_ref",
        ]
        .into_iter()
        .collect(),
        "completed" => [
            "status",
            "inspector_actor_ref",
            "started_at",
            "completed_at",
            "evidence_ref",
        ]
        .into_iter()
        .collect(),
        _ => return Err(policy("inspection status is invalid")),
    };
    exact_keys(inspection, &expected, "inspection")?;
    if status != "not-started" {
        string(
            required(inspection, "inspector_actor_ref", "inspection")?,
            "inspection.inspector_actor_ref",
        )?;
        string(
            required(inspection, "evidence_ref", "inspection")?,
            "inspection.evidence_ref",
        )?;
        let started = timestamp_value(
            required(inspection, "started_at", "inspection")?,
            "inspection.started_at",
        )?;
        if status == "completed" {
            let completed = timestamp_value(
                required(inspection, "completed_at", "inspection")?,
                "inspection.completed_at",
            )?;
            if completed < started {
                return Err(policy("inspection completion cannot precede start"));
            }
        }
    }
    let consistent = match state {
        "inspection-in-progress" => status == "in-progress",
        "protected-approved" | "published-verified" => status == "completed",
        "awaiting-protected-environment-approval" => {
            matches!(status, "not-started" | "completed")
        }
        _ => false,
    };
    if !consistent {
        return Err(policy("publication state and inspection status disagree"));
    }
    Ok(inspection)
}

fn validate_approval(
    value: &CanonicalValue,
    publication: &BTreeMap<String, CanonicalValue>,
    dispatch: &BTreeMap<String, CanonicalValue>,
    inspection: &BTreeMap<String, CanonicalValue>,
) -> Result<(), GateError> {
    let approval = object(value, "approval")?;
    let mode = string(
        required(approval, "approval_mode", "approval")?,
        "approval.approval_mode",
    )?;
    let mut expected = [
        "approval_mode",
        "approver_actor_ref",
        "approved_at",
        "approval_receipt_ref",
        "approval_kind",
        "automated_approval",
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if mode == "single-maintainer-inspect-then-approve" {
        expected.insert("configured_maintainer_actor_ref");
        expected.insert("two_person_unavailable_reason");
    } else if mode != "two-person-non-self" {
        return Err(policy("approval mode is invalid"));
    }
    exact_keys(approval, &expected, "approval")?;
    let approver = string(
        required(approval, "approver_actor_ref", "approval")?,
        "approval.approver_actor_ref",
    )?;
    let trigger = string(
        required(dispatch, "trigger_actor_ref", "dispatch")?,
        "dispatch.trigger_actor_ref",
    )?;
    if mode == "two-person-non-self" && approver == trigger {
        return Err(policy("trigger actor cannot approve in two-person mode"));
    }
    if mode == "single-maintainer-inspect-then-approve" {
        let configured = string(
            required(approval, "configured_maintainer_actor_ref", "approval")?,
            "approval.configured_maintainer_actor_ref",
        )?;
        string(
            required(approval, "two_person_unavailable_reason", "approval")?,
            "approval.two_person_unavailable_reason",
        )?;
        if Some(configured)
            != inspection
                .get("inspector_actor_ref")
                .and_then(CanonicalValue::as_str)
            || approver != configured
        {
            return Err(policy(
                "single-maintainer inspector and approver must match configured maintainer",
            ));
        }
    }
    let approved = timestamp_value(
        required(approval, "approved_at", "approval")?,
        "approval.approved_at",
    )?;
    let completed = timestamp_value(
        required(inspection, "completed_at", "inspection")?,
        "inspection.completed_at",
    )?;
    if approved <= completed {
        return Err(policy("approval must follow completed inspection"));
    }
    if approval.get("approval_receipt_ref") != dispatch.get("run_url") {
        return Err(policy("approval receipt must identify dispatched run"));
    }
    expect_exact_string(approval, "approval_kind", "manual", "approval")?;
    if required(approval, "automated_approval", "approval")?.as_bool() != Some(false) {
        return Err(policy("approval must be manual"));
    }
    let _ = publication;
    Ok(())
}

fn validate_public_fragment(
    publication: &BTreeMap<String, CanonicalValue>,
    dispatch: &BTreeMap<String, CanonicalValue>,
) -> Result<(), GateError> {
    let job = object(
        required(publication, "protected_job", "publication evidence")?,
        "protected_job",
    )?;
    exact_keys(
        job,
        &["run_id", "job_id", "conclusion", "completed_at"]
            .into_iter()
            .collect(),
        "protected_job",
    )?;
    expect_exact_string(job, "conclusion", "success", "protected_job")?;
    if job.get("run_id") != dispatch.get("run_id") || job.get("job_id") != dispatch.get("job_id") {
        return Err(policy("protected job must match dispatched run and job"));
    }
    let completed = timestamp_value(
        required(job, "completed_at", "protected_job")?,
        "protected_job.completed_at",
    )?;
    let approval = object(
        required(publication, "approval", "publication evidence")?,
        "approval",
    )?;
    let approved = timestamp_value(
        required(approval, "approved_at", "approval")?,
        "approval.approved_at",
    )?;
    if completed < approved {
        return Err(policy("protected job completion cannot precede approval"));
    }
    let public = object(
        required(publication, "public_verification", "publication evidence")?,
        "public_verification",
    )?;
    exact_keys(
        public,
        &[
            "release_url",
            "verified_at",
            "immutable",
            "assets_match",
            "attestations_match",
        ]
        .into_iter()
        .collect(),
        "public_verification",
    )?;
    let verified = timestamp_value(
        required(public, "verified_at", "public_verification")?,
        "public_verification.verified_at",
    )?;
    if verified < completed {
        return Err(policy("public verification cannot precede protected job"));
    }
    let release_url = string(
        required(public, "release_url", "public_verification")?,
        "public_verification.release_url",
    )?;
    let candidate = object(
        required(publication, "candidate", "publication evidence")?,
        "candidate evidence",
    )?;
    let tag = string(
        required(candidate, "candidate_tag", "candidate evidence")?,
        "candidate evidence.candidate_tag",
    )?;
    if !release_url.trim_end_matches('/').ends_with(tag) {
        return Err(policy("public release URL must identify candidate tag"));
    }
    for field in ["immutable", "assets_match", "attestations_match"] {
        if required(public, field, "public_verification")?.as_bool() != Some(true) {
            return Err(policy(format!("public_verification.{field} must be true")));
        }
    }
    Ok(())
}

fn timestamp_value(
    value: &CanonicalValue,
    label: &str,
) -> Result<DateTime<FixedOffset>, GateError> {
    let raw = string(value, label)?;
    if !raw.ends_with('Z') {
        return Err(policy(format!("{label} must end in Z")));
    }
    let parsed = DateTime::parse_from_rfc3339(raw)
        .map_err(|_| policy(format!("{label} must be an RFC 3339 timestamp")))?;
    if parsed.nanosecond() != 0 {
        return Err(policy(format!("{label} must use whole-second precision")));
    }
    Ok(parsed)
}

fn validate_sha40_field(
    value: &BTreeMap<String, CanonicalValue>,
    field: &str,
    label: &str,
) -> Result<(), GateError> {
    let text = string(required(value, field, label)?, &format!("{label}.{field}"))?;
    validate_hex(text, 40, &format!("{label}.{field}"))
}

fn validate_sha256_field(
    value: &BTreeMap<String, CanonicalValue>,
    field: &str,
    label: &str,
) -> Result<(), GateError> {
    let text = string(required(value, field, label)?, &format!("{label}.{field}"))?;
    validate_sha256(text, &format!("{label}.{field}"))
}

fn validate_sha256(value: &str, label: &str) -> Result<(), GateError> {
    validate_hex(value, 64, label)
}

fn validate_hex(value: &str, length: usize, label: &str) -> Result<(), GateError> {
    if value.len() != length
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(policy(format!(
            "{label} must be {length} lowercase hexadecimal characters"
        )));
    }
    Ok(())
}

fn compare_fields(
    left: &BTreeMap<String, CanonicalValue>,
    left_field: &str,
    right: &BTreeMap<String, CanonicalValue>,
    right_field: &str,
    label: &str,
) -> Result<(), GateError> {
    if left.get(left_field) != right.get(right_field) {
        return Err(policy(format!("{label} fields do not match")));
    }
    Ok(())
}

fn expect_exact_string(
    value: &BTreeMap<String, CanonicalValue>,
    field: &str,
    expected: &str,
    label: &str,
) -> Result<(), GateError> {
    let actual = string(required(value, field, label)?, &format!("{label}.{field}"))?;
    if actual != expected {
        return Err(policy(format!("{label}.{field} must equal {expected}")));
    }
    Ok(())
}

fn exact_keys(
    value: &BTreeMap<String, CanonicalValue>,
    expected: &BTreeSet<&str>,
    label: &str,
) -> Result<(), GateError> {
    let actual = value.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if actual != *expected {
        let missing = expected.difference(&actual).copied().collect::<Vec<_>>();
        let unknown = actual.difference(expected).copied().collect::<Vec<_>>();
        return Err(policy(format!(
            "{label} fields mismatch (missing={missing:?}, unknown={unknown:?})"
        )));
    }
    Ok(())
}

fn object<'a>(
    value: &'a CanonicalValue,
    label: &str,
) -> Result<&'a BTreeMap<String, CanonicalValue>, GateError> {
    value
        .as_object()
        .ok_or_else(|| policy(format!("{label} must be an object")))
}

fn required<'a>(
    value: &'a BTreeMap<String, CanonicalValue>,
    field: &str,
    label: &str,
) -> Result<&'a CanonicalValue, GateError> {
    value
        .get(field)
        .ok_or_else(|| policy(format!("{label}.{field} is required")))
}

fn string<'a>(value: &'a CanonicalValue, label: &str) -> Result<&'a str, GateError> {
    let value = value
        .as_str()
        .ok_or_else(|| policy(format!("{label} must be a string")))?;
    if value.trim().is_empty() {
        return Err(policy(format!("{label} must not be empty")));
    }
    Ok(value)
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("evidence.invalid", detail)
}
