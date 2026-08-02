//! Authenticated, revisioned protected-publication state machine.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use chrono::{SecondsFormat, Utc};
use serde_json::{Map, Value, json};

use crate::canonical::{self, CanonicalValue};
use crate::decision::{self, DecisionRequest};
use crate::digest;
use crate::error::GateError;
use crate::evidence::{self, EvidenceExpectation, EvidenceWrite};
use crate::limits::{MAX_INPUT_BYTES, MAX_SCHEMA_BYTES, read_bounded};

use super::github::{GitHubTransport, Method, Request};

const REPOSITORY: &str = "rustpunk/clinker";
const EVIDENCE_SCHEMA_ID: &str = "clinker.release-evidence/v1";
const CANDIDATE_SCHEMA_ID: &str = "clinker.candidate-evidence/v1";
const PUBLICATION_SCHEMA_ID: &str = "clinker.publication-evidence/v1";
const PUBLISH_WORKFLOW: &str = "publish-release.yml";
const JOB_NAME: &str = "publish-approved-release";
const ENVIRONMENT: &str = "release";

/// Shared authority inputs for candidate-tag creation and readback.
#[derive(Debug, Clone)]
pub struct CandidateAuthorityRequest {
    pub repository: String,
    pub authorization_record: PathBuf,
    pub authorization_schema: PathBuf,
    pub decision_record: PathBuf,
    pub decision_schema: PathBuf,
    pub deadline_seconds: u64,
}

/// Inputs for the sole protected workflow dispatch.
#[derive(Debug, Clone)]
pub struct DispatchRequest {
    pub repository: String,
    pub workflow: String,
    pub decision_dir: PathBuf,
    pub authorization_record: PathBuf,
    pub authorization_schema: PathBuf,
    pub decision_record: PathBuf,
    pub decision_schema: PathBuf,
    pub approval_record: PathBuf,
    pub candidate_evidence: PathBuf,
    pub evidence_schema: PathBuf,
    pub publication_evidence: PathBuf,
    pub discovery_deadline_seconds: u64,
}

/// Read-only approval target request.
#[derive(Debug, Clone)]
pub struct ApprovalTargetRequest {
    pub repository: String,
    pub publication_evidence: PathBuf,
    pub evidence_schema: PathBuf,
}

/// Caller expectation for an inspection transition.
#[derive(Debug, Clone)]
pub struct InspectionRequest {
    pub repository: String,
    pub publication_evidence: PathBuf,
    pub evidence_schema: PathBuf,
    pub expected_state: String,
    pub expected_revision: u64,
}

/// Authority and caller expectation used by approval and public readback.
#[derive(Debug, Clone)]
pub struct VerificationRequest {
    pub repository: String,
    pub decision_dir: PathBuf,
    pub authorization_record: PathBuf,
    pub authorization_schema: PathBuf,
    pub decision_record: PathBuf,
    pub decision_schema: PathBuf,
    pub approval_record: PathBuf,
    pub candidate_evidence: PathBuf,
    pub evidence_schema: PathBuf,
    pub publication_evidence: PathBuf,
    pub expected_state: String,
    pub expected_revision: u64,
    pub deadline_seconds: u64,
}

/// Authenticated workflow context that the hidden protected worker must reread remotely.
#[derive(Debug, Clone)]
pub struct WorkflowContext {
    pub repository: String,
    pub run_id: String,
    pub run_attempt: u64,
    pub workflow_ref: String,
    pub git_ref: String,
    pub source_sha: String,
    pub actor: String,
}

impl WorkflowContext {
    /// Read the standard GitHub Actions context without treating it as authority.
    pub fn from_environment() -> Result<Self, GateError> {
        Ok(Self {
            repository: required_environment("GITHUB_REPOSITORY")?,
            run_id: required_environment("GITHUB_RUN_ID")?,
            run_attempt: required_environment("GITHUB_RUN_ATTEMPT")?
                .parse()
                .map_err(|_| policy("GITHUB_RUN_ATTEMPT must be an integer"))?,
            workflow_ref: required_environment("GITHUB_WORKFLOW_REF")?,
            git_ref: required_environment("GITHUB_REF")?,
            source_sha: required_environment("GITHUB_SHA")?,
            actor: required_environment("GITHUB_ACTOR")?,
        })
    }
}

/// Immutable inputs admitted by the help-hidden protected publication worker.
#[derive(Debug, Clone)]
pub struct ProtectedPublishRequest {
    pub repository: String,
    pub candidate_tag: String,
    pub candidate_authorization_blob_sha: String,
    pub candidate_authorization_sha256: String,
    pub candidate_decision_blob_sha: String,
    pub candidate_evidence_blob_sha: String,
    pub source_sha: String,
    pub build_workflow_sha: String,
    pub publish_workflow_ref: String,
    pub publish_workflow_sha: String,
    pub candidate_release_id: String,
    pub approval_payload_blob_sha: String,
    pub approval_record_sha256: String,
    pub approval_mode: String,
    pub authorization_schema: PathBuf,
    pub decision_schema: PathBuf,
    pub evidence_schema: PathBuf,
    pub decision_dir: PathBuf,
    pub context: WorkflowContext,
    pub deadline_seconds: u64,
}

struct Authority {
    authorization: Value,
    authorization_bytes: Vec<u8>,
    authorization_digest: String,
    decision: Value,
    candidate: Option<Value>,
    candidate_bytes: Option<Vec<u8>>,
    approval_bytes: Option<Vec<u8>>,
}

/// Perform the sole protected draft-to-public mutation after environment approval.
///
/// This worker deliberately accepts no evidence path and cannot dispatch or approve.
pub fn protected_publish(
    request: &ProtectedPublishRequest,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    require_repository(&request.repository)?;
    validate_protected_context(request)?;
    let deadline = deadline(request.deadline_seconds)?;
    for (value, length, label) in [
        (
            &request.candidate_authorization_blob_sha,
            40,
            "authorization blob SHA",
        ),
        (
            &request.candidate_decision_blob_sha,
            40,
            "candidate decision blob SHA",
        ),
        (
            &request.candidate_evidence_blob_sha,
            40,
            "candidate evidence blob SHA",
        ),
        (&request.approval_payload_blob_sha, 40, "approval blob SHA"),
        (&request.source_sha, 40, "source SHA"),
        (&request.build_workflow_sha, 40, "build workflow SHA"),
        (&request.publish_workflow_sha, 40, "publish workflow SHA"),
        (
            &request.candidate_authorization_sha256,
            64,
            "authorization digest",
        ),
        (&request.approval_record_sha256, 64, "approval digest"),
    ] {
        validate_hex(value, length, label)?;
    }

    let authorization_bytes = fetch_blob(
        &request.repository,
        &request.candidate_authorization_blob_sha,
        deadline,
        transport,
    )?;
    let decision_bytes = fetch_blob(
        &request.repository,
        &request.candidate_decision_blob_sha,
        deadline,
        transport,
    )?;
    let candidate_bytes = fetch_blob(
        &request.repository,
        &request.candidate_evidence_blob_sha,
        deadline,
        transport,
    )?;
    let approval_bytes = fetch_blob(
        &request.repository,
        &request.approval_payload_blob_sha,
        deadline,
        transport,
    )?;
    if digest::sha256_hex(&approval_bytes) != request.approval_record_sha256 {
        return Err(policy(
            "approval blob digest differs from the immutable input",
        ));
    }
    let candidate = parse_canonical_bytes(&candidate_bytes, "candidate evidence blob")?;
    validate_schema(&request.evidence_schema)?;
    validate_candidate(&candidate)?;
    let approval = parse_canonical_bytes(&approval_bytes, "approval record blob")?;
    let candidate_decision = parse_canonical_bytes(&decision_bytes, "candidate decision blob")?;
    let authorization = parse_json(&authorization_bytes, "candidate authorization blob")?;

    let temporary = tempfile::tempdir()
        .map_err(|error| GateError::io("create protected validation directory", &error))?;
    let authorization_path = temporary.path().join("authorization.json");
    let decision_path = temporary.path().join("candidate.json");
    let candidate_path = temporary.path().join("candidate-evidence.json");
    let approval_path = temporary.path().join("approval.json");
    for (path, bytes) in [
        (&authorization_path, authorization_bytes.as_slice()),
        (&decision_path, decision_bytes.as_slice()),
        (&candidate_path, candidate_bytes.as_slice()),
        (&approval_path, approval_bytes.as_slice()),
    ] {
        fs::write(path, bytes)
            .map_err(|error| GateError::io("stage protected validation input", &error))?;
    }
    decision::validate(&DecisionRequest {
        schema: Some(request.decision_schema.clone()),
        records: vec![decision_path, approval_path],
        authorization_schema: Some(request.authorization_schema.clone()),
        authorization_record: Some(authorization_path),
        candidate_evidence: Some(candidate_path),
        require_ids: Vec::new(),
        require_authorization_id: None,
        require_authorized: true,
        require_complete: false,
        require_accepted: true,
    })?;
    validate_protected_authority(
        request,
        &authorization,
        &candidate_decision,
        &candidate,
        &approval,
    )?;

    let run = transport.send(&Request::new(
        Method::Get,
        format!(
            "repos/{}/actions/runs/{}",
            request.repository, request.context.run_id
        ),
        deadline,
    ))?;
    let run = object(&run.body, "protected workflow run")?;
    validate_run_identity(run, &request.source_sha, request.context.run_attempt)?;
    if value_id_or_string(run, "id", "protected workflow run")? != request.context.run_id
        || string_field(run, "event", "protected workflow run")? != "workflow_dispatch"
        || nested_string_value(run, "actor", "login")? != request.context.actor
    {
        return Err(policy(
            "token-backed workflow run readback differs from expected context",
        ));
    }
    let jobs = transport.send(&Request::new(
        Method::Get,
        format!(
            "repos/{}/actions/runs/{}/jobs",
            request.repository, request.context.run_id
        ),
        deadline,
    ))?;
    let job = unique_array_entry(&jobs.body, "jobs", "protected job readback")?;
    validate_job_identity(job, &request.context.run_id, None)?;
    let tag = transport.send(&Request::new(
        Method::Get,
        format!(
            "repos/{}/git/ref/tags/{}",
            request.repository, request.candidate_tag
        ),
        deadline,
    ))?;
    validate_tag(&tag.body, &request.candidate_tag, &request.source_sha)?;

    let candidate_object = object(&candidate, "candidate evidence")?;
    let release_endpoint = format!(
        "repos/{}/releases/{}",
        request.repository, request.candidate_release_id
    );
    let initial = transport.send(&Request::new(Method::Get, &release_endpoint, deadline))?;
    let initial = object(&initial.body, "private release readback")?;
    validate_release_identity(initial, request)?;
    if !bool_field(initial, "draft", "release readback")? {
        require_immutable_public(
            initial,
            candidate_object,
            &request.repository,
            deadline,
            transport,
        )?;
        return Ok("Protected release already public and byte-identical\n".to_owned());
    }
    verify_release_assets(
        initial,
        candidate_object,
        &request.repository,
        deadline,
        transport,
    )?;
    let release_body = string_field(initial, "body", "release readback")?;
    let prerelease = bool_field(initial, "prerelease", "release readback")?;
    let mutation = transport.send(
        &Request::new(Method::Patch, &release_endpoint, deadline)
            .field("body", release_body)
            .field("draft", "false")
            .field("prerelease", prerelease.to_string()),
    )?;
    if !mutation.body.is_null() {
        let mutation = object(&mutation.body, "publication mutation")?;
        validate_release_identity(mutation, request)?;
        if bool_field(mutation, "draft", "publication mutation")? {
            return Err(policy(
                "publication mutation left the release in draft state",
            ));
        }
    }
    let final_readback = transport.send(&Request::new(Method::Get, release_endpoint, deadline))?;
    let final_readback = object(&final_readback.body, "public release readback")?;
    validate_release_identity(final_readback, request)?;
    require_immutable_public(
        final_readback,
        candidate_object,
        &request.repository,
        deadline,
        transport,
    )?;
    Ok("Protected release published and immutable bytes verified\n".to_owned())
}

fn validate_protected_context(request: &ProtectedPublishRequest) -> Result<(), GateError> {
    if request.context.repository != request.repository
        || request.context.run_attempt != 1
        || request.context.git_ref != format!("refs/tags/{}", request.candidate_tag)
        || request.context.source_sha != request.source_sha
        || !request
            .context
            .workflow_ref
            .contains("/.github/workflows/publish-release.yml@refs/tags/")
        || !request
            .context
            .workflow_ref
            .ends_with(&request.candidate_tag)
    {
        return Err(policy(
            "protected worker context must identify the exact first publish-release tag run",
        ));
    }
    if request.publish_workflow_ref != request.candidate_tag
        || request.publish_workflow_sha != request.source_sha
    {
        return Err(policy(
            "publish workflow ref and full SHA must identify the candidate source",
        ));
    }
    Ok(())
}

fn validate_protected_authority(
    request: &ProtectedPublishRequest,
    authorization: &Value,
    decision: &Value,
    candidate: &Value,
    approval: &Value,
) -> Result<(), GateError> {
    let authorization = object(authorization, "candidate authorization")?;
    if string_field(
        authorization,
        "candidate_authorization_sha256",
        "candidate authorization",
    )? != request.candidate_authorization_sha256
    {
        return Err(policy(
            "candidate authorization digest differs from workflow input",
        ));
    }
    let nested = object(
        field(authorization, "authorization", "candidate authorization")?,
        "candidate authorization payload",
    )?;
    let candidate = object(candidate, "candidate evidence")?;
    let decision = object(decision, "candidate decision")?;
    let approval = object(approval, "publication approval")?;
    for (field_name, expected) in [
        ("candidate_tag", request.candidate_tag.as_str()),
        ("source_sha", request.source_sha.as_str()),
        ("build_workflow_sha", request.build_workflow_sha.as_str()),
        (
            "publish_workflow_ref",
            request.publish_workflow_ref.as_str(),
        ),
        (
            "publish_workflow_sha",
            request.publish_workflow_sha.as_str(),
        ),
        (
            "candidate_release_id",
            request.candidate_release_id.as_str(),
        ),
    ] {
        for (object, label) in [
            (nested, "candidate authorization payload"),
            (decision, "candidate decision"),
            (candidate, "candidate evidence"),
            (approval, "publication approval"),
        ] {
            if string_field(object, field_name, label)? != expected {
                return Err(policy(format!(
                    "{label}.{field_name} differs from protected workflow input"
                )));
            }
        }
    }
    for (object, label) in [
        (decision, "candidate decision"),
        (candidate, "candidate evidence"),
        (approval, "publication approval"),
    ] {
        if string_field(object, "candidate_authorization_sha256", label)?
            != request.candidate_authorization_sha256
        {
            return Err(policy(format!(
                "{label} authorization digest differs from protected workflow input"
            )));
        }
    }
    if approval_mode(&request.decision_dir)? != request.approval_mode {
        return Err(policy(
            "approval mode differs from accepted environment policy",
        ));
    }
    Ok(())
}

fn fetch_blob(
    repository: &str,
    sha: &str,
    deadline: Duration,
    transport: &mut dyn GitHubTransport,
) -> Result<Vec<u8>, GateError> {
    let response = transport.send(
        &Request::new(
            Method::Get,
            format!("repos/{repository}/git/blobs/{sha}"),
            deadline,
        )
        .header("Accept", "application/vnd.github.raw")
        .raw(),
    )?;
    let bytes = response
        .raw
        .ok_or_else(|| policy("Git blob transport did not return raw bytes"))?;
    if digest::git_blob_sha1_hex(&bytes) != sha {
        return Err(policy("Git blob bytes differ from the immutable SHA"));
    }
    Ok(bytes)
}

fn validate_release_identity(
    release: &Map<String, Value>,
    request: &ProtectedPublishRequest,
) -> Result<(), GateError> {
    if value_id_or_string(release, "id", "release readback")? != request.candidate_release_id
        || string_field(release, "tag_name", "release readback")? != request.candidate_tag
        || string_field(release, "target_commitish", "release readback")? != request.source_sha
    {
        return Err(policy(
            "release id, tag, or source differs from candidate authority",
        ));
    }
    bool_field(release, "draft", "release readback")?;
    let version = request.candidate_tag.trim_start_matches('v');
    let version_without_build = version
        .split_once('+')
        .map_or(version, |(version, _)| version);
    if bool_field(release, "prerelease", "release readback")? != version_without_build.contains('-')
    {
        return Err(policy("release prerelease state differs from its tag"));
    }
    string_field(release, "body", "release readback")?;
    Ok(())
}

fn verify_release_assets(
    release: &Map<String, Value>,
    candidate: &Map<String, Value>,
    repository: &str,
    deadline: Duration,
    transport: &mut dyn GitHubTransport,
) -> Result<(), GateError> {
    let assets = field(release, "assets", "release readback")?
        .as_array()
        .ok_or_else(|| policy("release assets must be an array"))?;
    let archives = field(candidate, "archives", "candidate evidence")?
        .as_array()
        .ok_or_else(|| policy("candidate archives must be an array"))?;
    if assets.len() != 4 || archives.len() != 4 {
        return Err(policy(
            "release must contain exactly four authorized assets",
        ));
    }
    let expected = archives
        .iter()
        .map(|archive| {
            let archive = object(archive, "candidate archive")?;
            Ok((
                string_field(archive, "archive_name", "candidate archive")?.to_owned(),
                string_field(archive, "sha256", "candidate archive")?.to_owned(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, GateError>>()?;
    let mut observed = BTreeMap::new();
    for asset in assets {
        let asset = object(asset, "release asset")?;
        let name = string_field(asset, "name", "release asset")?;
        let id = value_id_or_string(asset, "id", "release asset")?;
        if observed.contains_key(name) {
            return Err(policy("release asset names must be unique"));
        }
        let response = transport.send(
            &Request::new(
                Method::Get,
                format!("repos/{repository}/releases/assets/{id}"),
                deadline,
            )
            .header("Accept", "application/octet-stream")
            .raw(),
        )?;
        let bytes = response
            .raw
            .ok_or_else(|| policy("release asset transport did not return raw bytes"))?;
        observed.insert(name.to_owned(), digest::sha256_hex(&bytes));
    }
    if observed != expected {
        return Err(policy(
            "fresh release asset bytes differ from candidate digests",
        ));
    }
    Ok(())
}

fn require_immutable_public(
    release: &Map<String, Value>,
    candidate: &Map<String, Value>,
    repository: &str,
    deadline: Duration,
    transport: &mut dyn GitHubTransport,
) -> Result<(), GateError> {
    if bool_field(release, "draft", "public release")?
        || !bool_field(release, "immutable", "public release")?
    {
        return Err(policy(
            "public release readback must be non-draft and immutable",
        ));
    }
    verify_release_assets(release, candidate, repository, deadline, transport)
}

fn parse_canonical_bytes(bytes: &[u8], label: &str) -> Result<Value, GateError> {
    let canonical = canonical::parse_json(bytes)?;
    if canonical::to_bytes(&canonical)? != bytes {
        return Err(policy(format!("{label} must use canonical JSON v1")));
    }
    serde_json::from_slice(bytes).map_err(|_| policy(format!("{label} is malformed")))
}

fn required_environment(name: &'static str) -> Result<String, GateError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| GateError::usage(format!("{name} must be set by GitHub Actions")))
}

/// Create the one immutable candidate tag from accepted authority.
pub fn create_candidate_tag(
    request: &CandidateAuthorityRequest,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    let authority = load_authority(request, None, None, None)?;
    let authorization = object(&authority.authorization, "authorization")?;
    let nested = object(
        field(authorization, "authorization", "authorization")?,
        "authorization",
    )?;
    let tag = string_field(nested, "candidate_tag", "authorization")?;
    let source = string_field(nested, "source_sha", "authorization")?;
    let deadline = deadline(request.deadline_seconds)?;
    let endpoint = format!("repos/{}/git/refs", request.repository);
    let response = transport.send(
        &Request::new(Method::Post, endpoint, deadline)
            .field("ref", format!("refs/tags/{tag}"))
            .field("sha", source),
    )?;
    validate_tag(&response.body, tag, source)?;
    Ok(format!("Candidate tag {tag} created at {source}\n"))
}

/// Resolve and peel the protected candidate tag without mutation.
pub fn resolve_protected_ref(
    request: &CandidateAuthorityRequest,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    let authority = load_authority(request, None, None, None)?;
    let authorization = object(&authority.authorization, "authorization")?;
    let nested = object(
        field(authorization, "authorization", "authorization")?,
        "authorization",
    )?;
    let tag = string_field(nested, "candidate_tag", "authorization")?;
    let source = string_field(nested, "source_sha", "authorization")?;
    let response = transport.send(&Request::new(
        Method::Get,
        format!("repos/{}/git/ref/tags/{tag}", request.repository),
        deadline(request.deadline_seconds)?,
    ))?;
    validate_tag(&response.body, tag, source)?;
    Ok(format!(
        "Protected ref refs/tags/{tag} resolves to {source}\n"
    ))
}

/// Dispatch exactly once and create revision-zero publication evidence.
pub fn dispatch(
    request: &DispatchRequest,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    require_repository(&request.repository)?;
    if request.workflow != PUBLISH_WORKFLOW {
        return Err(policy("workflow must be publish-release.yml"));
    }
    validate_schema(&request.evidence_schema)?;
    if request.publication_evidence.exists() {
        let existing =
            read_canonical_json(&request.publication_evidence, "read publication evidence")?;
        validate_publication(&existing)?;
        let object = object(&existing, "publication evidence")?;
        let candidate =
            read_canonical_json(&request.candidate_evidence, "read candidate evidence")?;
        if object.get("candidate") != Some(&candidate) {
            return Err(policy(
                "existing dispatch evidence binds a different candidate",
            ));
        }
        return Ok(format!(
            "Protected publication already recorded: {}\n",
            nested_string(object, "dispatch", "run_url")?
        ));
    }

    let common = CandidateAuthorityRequest {
        repository: request.repository.clone(),
        authorization_record: request.authorization_record.clone(),
        authorization_schema: request.authorization_schema.clone(),
        decision_record: request.decision_record.clone(),
        decision_schema: request.decision_schema.clone(),
        deadline_seconds: request.discovery_deadline_seconds,
    };
    let authority = load_authority(
        &common,
        Some(&request.candidate_evidence),
        Some(&request.approval_record),
        Some(&request.decision_dir),
    )?;
    let candidate = authority
        .candidate
        .as_ref()
        .ok_or_else(|| internal("candidate authority was not loaded"))?;
    let candidate_bytes = authority
        .candidate_bytes
        .as_ref()
        .ok_or_else(|| internal("candidate bytes were not loaded"))?;
    let approval_bytes = authority
        .approval_bytes
        .as_ref()
        .ok_or_else(|| internal("approval bytes were not loaded"))?;
    let candidate_object = object(candidate, "candidate evidence")?;
    let tag = string_field(candidate_object, "candidate_tag", "candidate evidence")?;
    let source = string_field(candidate_object, "source_sha", "candidate evidence")?;
    let publish_sha = string_field(
        candidate_object,
        "publish_workflow_sha",
        "candidate evidence",
    )?;
    let release_id = string_field(
        candidate_object,
        "candidate_release_id",
        "candidate evidence",
    )?;
    let deadline = deadline(request.discovery_deadline_seconds)?;

    let authorization_blob = upload_blob(
        &request.repository,
        &authority.authorization_bytes,
        deadline,
        transport,
    )?;
    let approval_blob = upload_blob(&request.repository, approval_bytes, deadline, transport)?;
    let decision_bytes = canonical_bytes(&authority.decision)?
        .ok_or_else(|| internal("candidate decision bytes were not available"))?;
    let decision_blob = upload_blob(&request.repository, &decision_bytes, deadline, transport)?;
    let candidate_blob = upload_blob(&request.repository, candidate_bytes, deadline, transport)?;
    let candidate_digest = digest::sha256_hex(candidate_bytes);
    let approval_digest = digest::sha256_hex(approval_bytes);
    let dispatch_id = digest::sha256_hex(
        format!("{authorization_blob}:{approval_blob}:{candidate_digest}").as_bytes(),
    );

    let dispatch_endpoint = format!(
        "repos/{}/actions/workflows/{}/dispatches",
        request.repository, request.workflow
    );
    let dispatch_request = Request::new(Method::Post, dispatch_endpoint, deadline)
        .field("ref", tag)
        .field("inputs[candidate_tag]", tag)
        .field(
            "inputs[candidate_authorization_blob_sha]",
            &authorization_blob,
        )
        .field(
            "inputs[candidate_authorization_sha256]",
            &authority.authorization_digest,
        )
        .field("inputs[candidate_decision_blob_sha]", &decision_blob)
        .field("inputs[candidate_evidence_blob_sha]", &candidate_blob)
        .field("inputs[source_sha]", source)
        .field("inputs[publish_workflow_sha]", publish_sha)
        .field("inputs[candidate_release_id]", release_id)
        .field("inputs[approval_payload_blob_sha]", &approval_blob)
        .field("inputs[approval_record_sha256]", &approval_digest)
        .field("inputs[dispatch_id]", &dispatch_id);
    let response = transport.send(&dispatch_request)?;
    if !response.body.is_null() && response.body != json!({"accepted": true}) {
        return Err(policy("workflow dispatch returned an ambiguous result"));
    }

    let runs = transport.send(
        &Request::new(
            Method::Get,
            format!(
                "repos/{}/actions/workflows/{}/runs",
                request.repository, request.workflow
            ),
            deadline,
        )
        .field("event", "workflow_dispatch")
        .field("head_sha", source),
    )?;
    let run = unique_array_entry(&runs.body, "workflow_runs", "workflow run discovery")?;
    validate_run_identity(run, source, 1)?;
    let run_id = value_id(run, "id", "workflow run")?;
    let jobs = transport.send(&Request::new(
        Method::Get,
        format!("repos/{}/actions/runs/{run_id}/jobs", request.repository),
        deadline,
    ))?;
    let job = unique_array_entry(&jobs.body, "jobs", "protected job discovery")?;
    validate_job_identity(job, &run_id, None)?;

    let manifest = json!({
        "schema": PUBLICATION_SCHEMA_ID,
        "kind": "publication",
        "state": "awaiting-approval",
        "revision": 0,
        "release_status": "incomplete",
        "completion_eligible": false,
        "immutable_authority_sha256": authority.authorization_digest,
        "repository": request.repository,
        "candidate": candidate,
        "candidate_sha256": candidate_digest,
        "candidate_authorization_sha256": authority.authorization_digest,
        "candidate_authorization_blob_sha": authorization_blob,
        "candidate_decision_blob_sha": decision_blob,
        "candidate_evidence_blob_sha": candidate_blob,
        "approval_record_blob_sha": approval_blob,
        "approval_record_sha256": approval_digest,
        "dispatch": {
            "dispatch_id": dispatch_id,
            "workflow": PUBLISH_WORKFLOW,
            "workflow_sha": publish_sha,
            "source_sha": source,
            "run_id": run_id,
            "run_attempt": 1,
            "run_url": string_field(run, "html_url", "workflow run")?,
            "job_id": value_id(job, "id", "protected job")?,
            "job_name": JOB_NAME,
            "environment": ENVIRONMENT,
            "trigger_actor_ref": nested_string_value(run, "actor", "login")?,
            "dispatched_at": string_field(run, "created_at", "workflow run")?,
        },
        "inspection": {"status": "not-started"},
        "asset_identities": {
            "checksum_sha256": field(candidate_object, "checksum_sha256", "candidate evidence")?,
            "archive_digests": field(candidate_object, "archive_digests", "candidate evidence")?,
        },
    });
    validate_publication(&manifest)?;
    let canonical = to_canonical(&manifest)?;
    match evidence::create_only(&request.publication_evidence, &canonical)? {
        EvidenceWrite::Created => Ok(format!(
            "Protected publication dispatched: {}\n",
            nested_string(
                object(&manifest, "publication evidence")?,
                "dispatch",
                "run_url"
            )?
        )),
        EvidenceWrite::ExactReplay => Ok("Protected publication already recorded\n".to_owned()),
        EvidenceWrite::Replaced => Err(internal("create-only dispatch replaced evidence")),
    }
}

/// Return the exact run/job/environment target a human must inspect.
pub fn approval_target(request: &ApprovalTargetRequest) -> Result<String, GateError> {
    require_repository(&request.repository)?;
    validate_schema(&request.evidence_schema)?;
    let value = read_canonical_json(&request.publication_evidence, "read publication evidence")?;
    validate_publication(&value)?;
    let publication = object(&value, "publication evidence")?;
    if string_field(publication, "repository", "publication evidence")? != request.repository {
        return Err(policy(
            "publication evidence repository differs from --repo",
        ));
    }
    let dispatch = object_fn(
        field(publication, "dispatch", "publication evidence")?,
        "dispatch",
    )?;
    Ok(format!(
        "Run URL: {}\nRun ID: {}\nJob ID: {}\nEnvironment: {}\nCandidate: {}\n",
        string_field(dispatch, "run_url", "dispatch")?,
        string_field(dispatch, "run_id", "dispatch")?,
        string_field(dispatch, "job_id", "dispatch")?,
        string_field(dispatch, "environment", "dispatch")?,
        nested_string(publication, "candidate", "candidate_tag")?,
    ))
}

/// Record authenticated inspection start through revisioned CAS.
pub fn begin_inspection(
    request: &InspectionRequest,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    require_expected(request, "awaiting-approval", 0)?;
    transition_inspection(request, "inspection-started", 1, transport)
}

/// Record completed inspection after rereading the exact run.
pub fn complete_inspection(
    request: &InspectionRequest,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    require_expected(request, "inspection-started", 1)?;
    transition_inspection(request, "inspection-completed", 2, transport)
}

fn transition_inspection(
    request: &InspectionRequest,
    next_state: &str,
    next_revision: u64,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    require_repository(&request.repository)?;
    validate_schema(&request.evidence_schema)?;
    let mut current =
        read_canonical_json(&request.publication_evidence, "read publication evidence")?;
    validate_publication(&current)?;
    if is_state_revision(&current, next_state, next_revision)? {
        return Ok(format!("Publication evidence already at {next_state}\n"));
    }
    require_state_revision(&current, &request.expected_state, request.expected_revision)?;
    let current_object = object(&current, "publication evidence")?;
    let dispatch = object(
        field(current_object, "dispatch", "publication evidence")?,
        "dispatch",
    )?;
    let actor = transport.send(&Request::new(Method::Get, "user", Duration::from_secs(120)))?;
    let login = string_field(
        object(&actor.body, "authenticated user")?,
        "login",
        "authenticated user",
    )?;
    let now = now();
    if next_state == "inspection-started" {
        current["inspection"] = json!({
            "status": "in-progress",
            "inspector_actor_ref": login,
            "started_at": now,
            "evidence_ref": string_field(dispatch, "run_url", "dispatch")?,
        });
    } else {
        let run_id = string_field(dispatch, "run_id", "dispatch")?;
        let observed = transport.send(&Request::new(
            Method::Get,
            format!("repos/{}/actions/runs/{run_id}", request.repository),
            Duration::from_secs(120),
        ))?;
        validate_run_identity(
            object(&observed.body, "workflow run")?,
            string_field(dispatch, "source_sha", "dispatch")?,
            1,
        )?;
        let current_object = object(&current, "publication evidence")?;
        let inspection = object(
            field(current_object, "inspection", "publication evidence")?,
            "inspection",
        )?;
        if string_field(inspection, "inspector_actor_ref", "inspection")? != login {
            return Err(policy(
                "inspection must be completed by its authenticated inspector",
            ));
        }
        current["inspection"] = json!({
            "status": "completed",
            "inspector_actor_ref": login,
            "started_at": field(inspection, "started_at", "inspection")?,
            "completed_at": now,
            "evidence_ref": string_field(dispatch, "run_url", "dispatch")?,
        });
    }
    current["state"] = Value::String(next_state.to_owned());
    current["revision"] = Value::from(next_revision);
    validate_publication(&current)?;
    cas(
        &request.publication_evidence,
        request.expected_revision,
        &request.expected_state,
        &current,
    )?;
    Ok(format!("Publication evidence advanced to {next_state}\n"))
}

/// Verify the recorded manual environment approval and advance to revision three.
pub fn verify_approval(
    request: &VerificationRequest,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    require_verification_expected(request, "inspection-completed", 2)?;
    let (mut current, authority) = load_verification(request)?;
    if is_state_revision(&current, "approved", 3)? {
        return Ok("Publication approval already verified\n".to_owned());
    }
    require_state_revision(&current, &request.expected_state, request.expected_revision)?;
    verify_manifest_authority(&current, &authority, &request.repository)?;
    let publication = object(&current, "publication evidence")?;
    let dispatch = object(
        field(publication, "dispatch", "publication evidence")?,
        "dispatch",
    )?;
    let run_id = string_field(dispatch, "run_id", "dispatch")?;
    let response = transport.send(&Request::new(
        Method::Get,
        format!(
            "repos/{}/actions/runs/{run_id}/deployments/approval",
            request.repository
        ),
        deadline(request.deadline_seconds)?,
    ))?;
    let approval = object(&response.body, "protected approval")?;
    for (field_name, expected) in [
        ("run_id", run_id),
        ("job_id", string_field(dispatch, "job_id", "dispatch")?),
        ("environment", ENVIRONMENT),
        (
            "candidate_tag",
            nested_string(publication, "candidate", "candidate_tag")?,
        ),
    ] {
        if value_id_or_string(approval, field_name, "protected approval")? != expected {
            return Err(policy(format!(
                "protected approval {field_name} does not match dispatch"
            )));
        }
    }
    if string_field(approval, "state", "protected approval")? != "approved"
        || string_field(approval, "approval_kind", "protected approval")? != "manual"
        || bool_field(approval, "automated_approval", "protected approval")?
    {
        return Err(policy(
            "protected approval must be an observed manual approval",
        ));
    }
    let approver = nested_string_value(approval, "actor", "login")?;
    let mode = approval_mode(&request.decision_dir)?;
    let current_object = object(&current, "publication evidence")?;
    let inspection = object(
        field(current_object, "inspection", "publication evidence")?,
        "inspection",
    )?;
    let trigger = string_field(dispatch, "trigger_actor_ref", "dispatch")?;
    if mode == "two-person-non-self" && approver == trigger {
        return Err(policy("trigger actor cannot approve in two-person mode"));
    }
    let mut evidence_approval = json!({
        "approval_mode": mode,
        "approver_actor_ref": approver,
        "approved_at": string_field(approval, "approved_at", "protected approval")?,
        "approval_receipt_ref": string_field(dispatch, "run_url", "dispatch")?,
        "approval_kind": "manual",
        "automated_approval": false,
    });
    if mode == "single-maintainer-inspect-then-approve" {
        let authorization_object = object(&authority.authorization, "authorization")?;
        let configured = nested_string(
            object(
                field(authorization_object, "authorization", "authorization")?,
                "authorization",
            )?,
            "",
            "authorized_release_maintainer_ref",
        )?;
        if approver != configured
            || string_field(inspection, "inspector_actor_ref", "inspection")? != configured
        {
            return Err(policy(
                "single-maintainer inspector and approver must equal configured maintainer",
            ));
        }
        evidence_approval["configured_maintainer_actor_ref"] = Value::String(configured.to_owned());
        evidence_approval["two_person_unavailable_reason"] =
            Value::String("recorded single-maintainer policy".to_owned());
    }
    current["approval"] = evidence_approval;
    current["state"] = Value::String("approved".to_owned());
    current["revision"] = Value::from(3_u64);
    validate_publication(&current)?;
    cas(
        &request.publication_evidence,
        2,
        "inspection-completed",
        &current,
    )?;
    Ok("Publication approval verified\n".to_owned())
}

/// Wait read-only for the recorded run and prove immutable public bytes.
pub fn wait_and_verify(
    request: &VerificationRequest,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    require_verification_expected(request, "approved", 3)?;
    let (mut current, authority) = load_verification(request)?;
    if is_state_revision(&current, "public-verified", 4)? {
        verify_manifest_authority(&current, &authority, &request.repository)?;
        return Ok("Public release evidence already verified\n".to_owned());
    }
    require_state_revision(&current, &request.expected_state, request.expected_revision)?;
    verify_manifest_authority(&current, &authority, &request.repository)?;
    let publication = object(&current, "publication evidence")?;
    let dispatch = object(
        field(publication, "dispatch", "publication evidence")?,
        "dispatch",
    )?;
    let run_id = string_field(dispatch, "run_id", "dispatch")?;
    let run = transport.send(&Request::new(
        Method::Get,
        format!("repos/{}/actions/runs/{run_id}", request.repository),
        deadline(request.deadline_seconds)?,
    ))?;
    let run = object(&run.body, "workflow run")?;
    validate_run_identity(run, string_field(dispatch, "source_sha", "dispatch")?, 1)?;
    if string_field(run, "conclusion", "workflow run")? != "success" {
        return Err(policy(
            "recorded workflow run did not complete successfully",
        ));
    }
    let job_id = string_field(dispatch, "job_id", "dispatch")?;
    let job = transport.send(&Request::new(
        Method::Get,
        format!("repos/{}/actions/jobs/{job_id}", request.repository),
        deadline(request.deadline_seconds)?,
    ))?;
    let job = object(&job.body, "protected job")?;
    validate_job_identity(job, run_id, Some("success"))?;

    let candidate = object(
        field(publication, "candidate", "publication evidence")?,
        "candidate evidence",
    )?;
    let tag = string_field(candidate, "candidate_tag", "candidate evidence")?;
    let release = transport.send(&Request::new(
        Method::Get,
        format!("repos/{}/releases/tags/{tag}", request.repository),
        deadline(request.deadline_seconds)?,
    ))?;
    let release = object(&release.body, "public release")?;
    validate_public_release(release, candidate)?;
    let release_url = string_field(release, "html_url", "public release")?;
    let completed_at = string_field(job, "completed_at", "protected job")?;
    current["protected_job"] = json!({
        "run_id": run_id,
        "job_id": job_id,
        "status": "success",
        "wait_mode": "read-only",
        "completed_at": completed_at,
    });
    current["public_verification"] = json!({
        "status": "verified",
        "release_url": release_url,
        "verified_at": now(),
        "immutable_release": true,
        "asset_count": 4,
        "assets_match": true,
        "attestations_match": true,
    });
    current["state"] = Value::String("public-verified".to_owned());
    current["revision"] = Value::from(4_u64);
    validate_publication(&current)?;
    cas(&request.publication_evidence, 3, "approved", &current)?;
    Ok("Immutable public release readback verified\n".to_owned())
}

/// Validate candidate or publication evidence against the current Rust contract.
pub fn validate_evidence_file(
    kind: crate::evidence::EvidenceKind,
    schema_path: &Path,
    manifest_path: &Path,
) -> Result<(), GateError> {
    validate_schema(schema_path)?;
    let value = read_canonical_json(manifest_path, "read release evidence")?;
    match kind {
        crate::evidence::EvidenceKind::Candidate => validate_candidate(&value),
        crate::evidence::EvidenceKind::Publication => validate_publication(&value),
    }
}

fn load_authority(
    request: &CandidateAuthorityRequest,
    candidate_path: Option<&Path>,
    approval_path: Option<&Path>,
    decision_dir: Option<&Path>,
) -> Result<Authority, GateError> {
    require_repository(&request.repository)?;
    deadline(request.deadline_seconds)?;
    let authorization_bytes = read_bounded(
        &request.authorization_record,
        "read candidate authorization",
        MAX_INPUT_BYTES,
    )?;
    let authorization = parse_json(&authorization_bytes, "candidate authorization")?;
    let decision_value = read_canonical_json(&request.decision_record, "read candidate decision")?;
    let candidate = candidate_path
        .map(|path| read_canonical_json(path, "read candidate evidence"))
        .transpose()?;
    if let Some(value) = &candidate {
        validate_candidate(value)?;
    }
    let candidate_bytes = candidate
        .as_ref()
        .map(|value| canonical_bytes(value).map(|bytes| bytes.unwrap_or_default()))
        .transpose()?;
    let approval = approval_path
        .map(|path| read_canonical_json(path, "read publication approval"))
        .transpose()?;
    let approval_bytes = approval
        .as_ref()
        .map(|value| canonical_bytes(value).map(|bytes| bytes.unwrap_or_default()))
        .transpose()?;

    let mut records = vec![request.decision_record.clone()];
    if let Some(path) = approval_path {
        records.push(path.to_path_buf());
    }
    decision::validate(&DecisionRequest {
        schema: Some(request.decision_schema.clone()),
        records,
        authorization_schema: Some(request.authorization_schema.clone()),
        authorization_record: Some(request.authorization_record.clone()),
        candidate_evidence: candidate_path.map(Path::to_path_buf),
        require_ids: Vec::new(),
        require_authorization_id: None,
        require_authorized: true,
        require_complete: false,
        require_accepted: true,
    })?;
    if let Some(directory) = decision_dir {
        validate_decision_directory(directory, request, candidate_path)?;
    }
    let authorization_object = object(&authorization, "candidate authorization")?;
    let authorization_digest = string_field(
        authorization_object,
        "candidate_authorization_sha256",
        "candidate authorization",
    )?;
    let authorization_digest = authorization_digest.to_owned();
    Ok(Authority {
        authorization,
        authorization_bytes,
        authorization_digest,
        decision: decision_value,
        candidate,
        candidate_bytes,
        approval_bytes,
    })
}

fn validate_decision_directory(
    directory: &Path,
    authority: &CandidateAuthorityRequest,
    candidate: Option<&Path>,
) -> Result<(), GateError> {
    let names = [
        "semantic-identity.json",
        "native-filesystem.json",
        "remote-share.json",
        "release-rules.json",
        "release-environment.json",
        "publication-policy.json",
        "release-candidate.json",
        "publication-approval.json",
    ];
    let records = names
        .iter()
        .map(|name| directory.join(name))
        .collect::<Vec<_>>();
    if records.iter().all(|path| path.is_file()) {
        decision::validate(&DecisionRequest {
            schema: Some(authority.decision_schema.clone()),
            records,
            authorization_schema: Some(authority.authorization_schema.clone()),
            authorization_record: Some(authority.authorization_record.clone()),
            candidate_evidence: candidate.map(Path::to_path_buf),
            require_ids: Vec::new(),
            require_authorization_id: None,
            require_authorized: true,
            require_complete: true,
            require_accepted: true,
        })?;
    }
    Ok(())
}

fn load_verification(request: &VerificationRequest) -> Result<(Value, Authority), GateError> {
    require_repository(&request.repository)?;
    validate_schema(&request.evidence_schema)?;
    let common = CandidateAuthorityRequest {
        repository: request.repository.clone(),
        authorization_record: request.authorization_record.clone(),
        authorization_schema: request.authorization_schema.clone(),
        decision_record: request.decision_record.clone(),
        decision_schema: request.decision_schema.clone(),
        deadline_seconds: request.deadline_seconds,
    };
    let authority = load_authority(
        &common,
        Some(&request.candidate_evidence),
        Some(&request.approval_record),
        Some(&request.decision_dir),
    )?;
    let current = read_canonical_json(&request.publication_evidence, "read publication evidence")?;
    validate_publication(&current)?;
    Ok((current, authority))
}

fn verify_manifest_authority(
    publication: &Value,
    authority: &Authority,
    repository: &str,
) -> Result<(), GateError> {
    let object = object(publication, "publication evidence")?;
    if string_field(object, "repository", "publication evidence")? != repository
        || string_field(object, "immutable_authority_sha256", "publication evidence")?
            != authority.authorization_digest
        || object.get("candidate") != authority.candidate.as_ref()
        || string_field(
            object,
            "candidate_authorization_blob_sha",
            "publication evidence",
        )? != digest::git_blob_sha1_hex(&authority.authorization_bytes)
        || string_field(
            object,
            "candidate_decision_blob_sha",
            "publication evidence",
        )? != digest::git_blob_sha1_hex(
            &canonical_bytes(&authority.decision)?
                .ok_or_else(|| internal("candidate decision bytes missing"))?,
        )
        || string_field(
            object,
            "candidate_evidence_blob_sha",
            "publication evidence",
        )? != digest::git_blob_sha1_hex(
            authority
                .candidate_bytes
                .as_ref()
                .ok_or_else(|| internal("candidate bytes missing"))?,
        )
        || string_field(object, "approval_record_blob_sha", "publication evidence")?
            != digest::git_blob_sha1_hex(
                authority
                    .approval_bytes
                    .as_ref()
                    .ok_or_else(|| internal("approval bytes missing"))?,
            )
        || string_field(object, "approval_record_sha256", "publication evidence")?
            != digest::sha256_hex(
                authority
                    .approval_bytes
                    .as_ref()
                    .ok_or_else(|| internal("approval bytes missing"))?,
            )
    {
        return Err(policy("publication immutable authority changed"));
    }
    Ok(())
}

fn upload_blob(
    repository: &str,
    bytes: &[u8],
    deadline: Duration,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    let content =
        std::str::from_utf8(bytes).map_err(|_| policy("authority blob must be UTF-8 JSON"))?;
    let expected = digest::git_blob_sha1_hex(bytes);
    let response = transport.send(
        &Request::new(
            Method::Post,
            format!("repos/{repository}/git/blobs"),
            deadline,
        )
        .field("encoding", "utf-8")
        .field("content", content),
    )?;
    if string_field(
        object(&response.body, "Git blob response")?,
        "sha",
        "Git blob response",
    )? != expected
    {
        return Err(policy(
            "Git blob readback SHA differs from exact authority bytes",
        ));
    }
    Ok(expected)
}

fn validate_tag(value: &Value, tag: &str, source: &str) -> Result<(), GateError> {
    let object = object(value, "tag readback")?;
    if string_field(object, "ref", "tag readback")? != format!("refs/tags/{tag}")
        || nested_string_value(object, "object", "sha")? != source
        || nested_string_value(object, "object", "type")? != "commit"
    {
        return Err(policy(
            "tag readback does not match authorized ref and full commit",
        ));
    }
    Ok(())
}

fn validate_run_identity(
    run: &Map<String, Value>,
    source: &str,
    attempt: u64,
) -> Result<(), GateError> {
    if string_field(run, "head_sha", "workflow run")? != source
        || u64_field(run, "run_attempt", "workflow run")? != attempt
    {
        return Err(policy(
            "workflow run full SHA or attempt differs from authority",
        ));
    }
    Ok(())
}

fn validate_job_identity(
    job: &Map<String, Value>,
    run_id: &str,
    conclusion: Option<&str>,
) -> Result<(), GateError> {
    if value_id_or_string(job, "run_id", "protected job")? != run_id
        || string_field(job, "name", "protected job")? != JOB_NAME
        || string_field(job, "environment", "protected job")? != ENVIRONMENT
    {
        return Err(policy(
            "protected job identity differs from the dispatched authority",
        ));
    }
    if let Some(expected) = conclusion
        && string_field(job, "conclusion", "protected job")? != expected
    {
        return Err(policy("protected job did not complete successfully"));
    }
    Ok(())
}

fn validate_public_release(
    release: &Map<String, Value>,
    candidate: &Map<String, Value>,
) -> Result<(), GateError> {
    if value_id_or_string(release, "id", "public release")?
        != string_field(candidate, "candidate_release_id", "candidate evidence")?
        || string_field(release, "tag_name", "public release")?
            != string_field(candidate, "candidate_tag", "candidate evidence")?
        || string_field(release, "target_commitish", "public release")?
            != string_field(candidate, "source_sha", "candidate evidence")?
        || bool_field(release, "is_draft", "public release")?
        || !bool_field(release, "immutable", "public release")?
        || !bool_field(release, "attestations_match", "public release")?
    {
        return Err(policy(
            "public release identity or immutability differs from candidate",
        ));
    }
    let assets = field(release, "assets", "public release")?
        .as_array()
        .ok_or_else(|| policy("public release assets must be an array"))?;
    let archives = field(candidate, "archives", "candidate evidence")?
        .as_array()
        .ok_or_else(|| policy("candidate archives must be an array"))?;
    if assets.len() != 4 || archives.len() != 4 {
        return Err(policy(
            "public release must contain the exact four authorized assets",
        ));
    }
    let expected = archives
        .iter()
        .map(|archive| {
            let archive = object(archive, "candidate archive")?;
            Ok((
                string_field(archive, "archive_name", "candidate archive")?.to_owned(),
                string_field(archive, "sha256", "candidate archive")?.to_owned(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, GateError>>()?;
    let observed = assets
        .iter()
        .map(|asset| {
            let asset = object(asset, "public asset")?;
            if !bool_field(asset, "uploaded", "public asset")? {
                return Err(policy("public asset upload is incomplete"));
            }
            Ok((
                string_field(asset, "name", "public asset")?.to_owned(),
                string_field(asset, "sha256", "public asset")?.to_owned(),
            ))
        })
        .collect::<Result<BTreeMap<_, _>, GateError>>()?;
    if observed != expected {
        return Err(policy(
            "public asset digest readback differs from candidate",
        ));
    }
    Ok(())
}

fn validate_candidate(value: &Value) -> Result<(), GateError> {
    let object = object(value, "candidate evidence")?;
    exact_keys(
        object,
        &[
            "schema",
            "kind",
            "state",
            "revision",
            "release_status",
            "completion_eligible",
            "immutable_authority_sha256",
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
        ],
        "candidate evidence",
    )?;
    expect_string(object, "schema", CANDIDATE_SCHEMA_ID, "candidate evidence")?;
    expect_string(object, "kind", "candidate", "candidate evidence")?;
    expect_string(object, "state", "candidate-verified", "candidate evidence")?;
    expect_u64(object, "revision", 0, "candidate evidence")?;
    require_incomplete(object, "candidate evidence")?;
    let authority = string_field(
        object,
        "candidate_authorization_sha256",
        "candidate evidence",
    )?;
    if string_field(object, "immutable_authority_sha256", "candidate evidence")? != authority {
        return Err(policy(
            "candidate immutable authority must equal authorization digest",
        ));
    }
    validate_hex(authority, 64, "candidate authorization digest")?;
    validate_hex(
        string_field(object, "source_sha", "candidate evidence")?,
        40,
        "candidate source SHA",
    )?;
    if string_field(object, "source_sha", "candidate evidence")?
        != string_field(object, "build_head_sha", "candidate evidence")?
        || string_field(object, "source_sha", "candidate evidence")?
            != string_field(object, "publish_workflow_sha", "candidate evidence")?
        || string_field(object, "source_sha", "candidate evidence")?
            != string_field(
                object,
                "publish_workflow_ref_resolved_sha",
                "candidate evidence",
            )?
        || bool_field(object, "tag_mutation_performed", "candidate evidence")?
    {
        return Err(policy(
            "candidate workflow, source, or tag-mutation identity is invalid",
        ));
    }
    let archives = field(object, "archives", "candidate evidence")?
        .as_array()
        .filter(|values| values.len() == 4)
        .ok_or_else(|| policy("candidate must contain four archives"))?;
    let attestations = field(object, "attestations", "candidate evidence")?
        .as_array()
        .filter(|values| values.len() == 4)
        .ok_or_else(|| policy("candidate must contain four attestations"))?;
    let _ = (archives, attestations);
    Ok(())
}

fn validate_publication(value: &Value) -> Result<(), GateError> {
    let publication = object(value, "publication evidence")?;
    let state = string_field(publication, "state", "publication evidence")?;
    let revision = u64_field(publication, "revision", "publication evidence")?;
    let expected_revision = match state {
        "awaiting-approval" => 0,
        "inspection-started" => 1,
        "inspection-completed" => 2,
        "approved" => 3,
        "public-verified" => 4,
        _ => return Err(policy("publication state is invalid")),
    };
    if revision != expected_revision {
        return Err(policy("publication state/revision pair is invalid"));
    }
    let mut keys = vec![
        "schema",
        "kind",
        "state",
        "revision",
        "release_status",
        "completion_eligible",
        "immutable_authority_sha256",
        "repository",
        "candidate",
        "candidate_sha256",
        "candidate_authorization_sha256",
        "candidate_authorization_blob_sha",
        "candidate_decision_blob_sha",
        "candidate_evidence_blob_sha",
        "approval_record_blob_sha",
        "approval_record_sha256",
        "dispatch",
        "inspection",
        "asset_identities",
    ];
    if revision >= 3 {
        keys.push("approval");
    }
    if revision == 4 {
        keys.extend(["protected_job", "public_verification"]);
    }
    exact_keys(publication, &keys, "publication evidence")?;
    expect_string(
        publication,
        "schema",
        PUBLICATION_SCHEMA_ID,
        "publication evidence",
    )?;
    expect_string(publication, "kind", "publication", "publication evidence")?;
    expect_string(
        publication,
        "repository",
        REPOSITORY,
        "publication evidence",
    )?;
    require_incomplete(publication, "publication evidence")?;
    validate_candidate(field(publication, "candidate", "publication evidence")?)?;
    let candidate = object_fn(
        field(publication, "candidate", "publication evidence")?,
        "candidate evidence",
    )?;
    for field_name in [
        "immutable_authority_sha256",
        "candidate_authorization_sha256",
        "candidate_sha256",
        "approval_record_sha256",
    ] {
        validate_hex(
            string_field(publication, field_name, "publication evidence")?,
            64,
            field_name,
        )?;
    }
    for field_name in [
        "candidate_authorization_blob_sha",
        "candidate_decision_blob_sha",
        "candidate_evidence_blob_sha",
        "approval_record_blob_sha",
    ] {
        validate_hex(
            string_field(publication, field_name, "publication evidence")?,
            40,
            field_name,
        )?;
    }
    if publication.get("immutable_authority_sha256")
        != publication.get("candidate_authorization_sha256")
        || publication.get("candidate_authorization_sha256")
            != candidate.get("candidate_authorization_sha256")
        || publication.get("candidate_sha256")
            != Some(&Value::String(digest::sha256_hex(
                &canonical_bytes(field(publication, "candidate", "publication evidence")?)?
                    .unwrap_or_default(),
            )))
    {
        return Err(policy("publication immutable candidate authority differs"));
    }
    let dispatch = object_fn(
        field(publication, "dispatch", "publication evidence")?,
        "dispatch",
    )?;
    exact_keys(
        dispatch,
        &[
            "dispatch_id",
            "workflow",
            "workflow_sha",
            "source_sha",
            "run_id",
            "run_attempt",
            "run_url",
            "job_id",
            "job_name",
            "environment",
            "trigger_actor_ref",
            "dispatched_at",
        ],
        "dispatch",
    )?;
    expect_string(dispatch, "workflow", PUBLISH_WORKFLOW, "dispatch")?;
    expect_string(dispatch, "job_name", JOB_NAME, "dispatch")?;
    expect_string(dispatch, "environment", ENVIRONMENT, "dispatch")?;
    expect_u64(dispatch, "run_attempt", 1, "dispatch")?;
    if dispatch.get("source_sha") != candidate.get("source_sha")
        || dispatch.get("workflow_sha") != candidate.get("publish_workflow_sha")
    {
        return Err(policy("dispatch authority differs from candidate"));
    }
    let inspection = object_fn(
        field(publication, "inspection", "publication evidence")?,
        "inspection",
    )?;
    let expected_status = match revision {
        0 => "not-started",
        1 => "in-progress",
        _ => "completed",
    };
    expect_string(inspection, "status", expected_status, "inspection")?;
    if revision >= 3 {
        let approval = object_fn(
            field(publication, "approval", "publication evidence")?,
            "approval",
        )?;
        expect_string(approval, "approval_kind", "manual", "approval")?;
        if bool_field(approval, "automated_approval", "approval")? {
            return Err(policy("automated approval is forbidden"));
        }
    }
    if revision == 4 {
        let job = object_fn(
            field(publication, "protected_job", "publication evidence")?,
            "protected job",
        )?;
        expect_string(job, "status", "success", "protected job")?;
        expect_string(job, "wait_mode", "read-only", "protected job")?;
        let public = object_fn(
            field(publication, "public_verification", "publication evidence")?,
            "public verification",
        )?;
        expect_string(public, "status", "verified", "public verification")?;
        if !bool_field(public, "immutable_release", "public verification")?
            || u64_field(public, "asset_count", "public verification")? != 4
        {
            return Err(policy(
                "public verification must prove four immutable assets",
            ));
        }
    }
    Ok(())
}

fn cas(path: &Path, revision: u64, state: &str, next: &Value) -> Result<(), GateError> {
    let authority = string_field(
        object(next, "publication evidence")?,
        "immutable_authority_sha256",
        "publication evidence",
    )?;
    let result = evidence::compare_and_swap(
        path,
        &EvidenceExpectation::new(revision, state, authority),
        &to_canonical(next)?,
    )?;
    if result == EvidenceWrite::Created {
        return Err(internal("publication CAS unexpectedly created evidence"));
    }
    Ok(())
}

fn validate_schema(path: &Path) -> Result<(), GateError> {
    let bytes = read_bounded(path, "read release evidence schema", MAX_SCHEMA_BYTES)?;
    let schema = parse_json(&bytes, "release evidence schema")?;
    expect_string(
        object(&schema, "release evidence schema")?,
        "$id",
        EVIDENCE_SCHEMA_ID,
        "release evidence schema",
    )
}

fn require_expected(
    request: &InspectionRequest,
    state: &str,
    revision: u64,
) -> Result<(), GateError> {
    if request.expected_state != state || request.expected_revision != revision {
        return Err(GateError::usage(format!(
            "expected inspection CAS must be --expected-state {state} --expected-revision {revision}"
        )));
    }
    Ok(())
}

fn require_verification_expected(
    request: &VerificationRequest,
    state: &str,
    revision: u64,
) -> Result<(), GateError> {
    if request.expected_state != state || request.expected_revision != revision {
        return Err(GateError::usage(format!(
            "expected verification CAS must be --expected-state {state} --expected-revision {revision}"
        )));
    }
    Ok(())
}

fn require_state_revision(value: &Value, state: &str, revision: u64) -> Result<(), GateError> {
    if !is_state_revision(value, state, revision)? {
        return Err(policy("publication evidence state or revision is stale"));
    }
    Ok(())
}

fn is_state_revision(value: &Value, state: &str, revision: u64) -> Result<bool, GateError> {
    let object = object(value, "publication evidence")?;
    Ok(
        string_field(object, "state", "publication evidence")? == state
            && u64_field(object, "revision", "publication evidence")? == revision,
    )
}

fn approval_mode(decision_dir: &Path) -> Result<String, GateError> {
    let value = read_canonical_json(
        &decision_dir.join("release-environment.json"),
        "read release environment decision",
    )?;
    let decision = object(&value, "release environment decision")?;
    let environment_policy = object_fn(
        field(
            decision,
            "environment_policy",
            "release environment decision",
        )?,
        "environment policy",
    )?;
    let mode = string_field(environment_policy, "approval_mode", "environment policy")?;
    if !matches!(
        mode,
        "two-person-non-self" | "single-maintainer-inspect-then-approve"
    ) {
        return Err(policy("release environment approval mode is invalid"));
    }
    Ok(mode.to_owned())
}

fn require_repository(repository: &str) -> Result<(), GateError> {
    if repository != REPOSITORY {
        return Err(policy("repository must be rustpunk/clinker"));
    }
    Ok(())
}

fn deadline(seconds: u64) -> Result<Duration, GateError> {
    if seconds == 0 || seconds > 3600 {
        return Err(GateError::usage(
            "deadline must be between 1 and 3600 seconds",
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn parse_json(bytes: &[u8], label: &str) -> Result<Value, GateError> {
    canonical::parse_json(bytes)?;
    serde_json::from_slice(bytes).map_err(|_| policy(format!("{label} is malformed")))
}

fn read_canonical_json(path: &Path, operation: &'static str) -> Result<Value, GateError> {
    let bytes = read_bounded(path, operation, MAX_INPUT_BYTES)?;
    let canonical = canonical::parse_json(&bytes)?;
    if canonical::to_bytes(&canonical)? != bytes {
        return Err(policy("evidence bytes must use canonical JSON v1"));
    }
    serde_json::from_slice(&bytes).map_err(|_| policy("evidence JSON is malformed"))
}

fn to_canonical(value: &Value) -> Result<CanonicalValue, GateError> {
    canonical::parse_json(
        &serde_json::to_vec(value)
            .map_err(|_| internal("publication evidence serialization failed"))?,
    )
}

fn canonical_bytes(value: &Value) -> Result<Option<Vec<u8>>, GateError> {
    Ok(Some(canonical::to_bytes(&to_canonical(value)?)?))
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, GateError> {
    value
        .as_object()
        .ok_or_else(|| policy(format!("{label} must be an object")))
}

fn field<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    label: &str,
) -> Result<&'a Value, GateError> {
    object
        .get(field)
        .ok_or_else(|| policy(format!("{label}.{field} is required")))
}

fn string_field<'a>(
    object: &'a Map<String, Value>,
    name: &str,
    label: &str,
) -> Result<&'a str, GateError> {
    field(object, name, label)?
        .as_str()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| policy(format!("{label}.{name} must be a non-empty string")))
}

fn bool_field(object: &Map<String, Value>, name: &str, label: &str) -> Result<bool, GateError> {
    field(object, name, label)?
        .as_bool()
        .ok_or_else(|| policy(format!("{label}.{name} must be a boolean")))
}

fn u64_field(object: &Map<String, Value>, name: &str, label: &str) -> Result<u64, GateError> {
    field(object, name, label)?
        .as_u64()
        .ok_or_else(|| policy(format!("{label}.{name} must be a non-negative integer")))
}

fn expect_string(
    object: &Map<String, Value>,
    field: &str,
    expected: &str,
    label: &str,
) -> Result<(), GateError> {
    if string_field(object, field, label)? != expected {
        return Err(policy(format!("{label}.{field} must equal {expected}")));
    }
    Ok(())
}

fn expect_u64(
    object: &Map<String, Value>,
    field: &str,
    expected: u64,
    label: &str,
) -> Result<(), GateError> {
    if u64_field(object, field, label)? != expected {
        return Err(policy(format!("{label}.{field} must equal {expected}")));
    }
    Ok(())
}

fn require_incomplete(object: &Map<String, Value>, label: &str) -> Result<(), GateError> {
    expect_string(object, "release_status", "incomplete", label)?;
    if bool_field(object, "completion_eligible", label)? {
        return Err(policy(format!("{label} cannot be completion eligible")));
    }
    Ok(())
}

fn exact_keys(
    object: &Map<String, Value>,
    expected: &[&str],
    label: &str,
) -> Result<(), GateError> {
    let actual = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(policy(format!("{label} has missing or unknown fields")));
    }
    Ok(())
}

fn nested_string<'a>(
    object: &'a Map<String, Value>,
    outer: &str,
    inner: &str,
) -> Result<&'a str, GateError> {
    if outer.is_empty() {
        return string_field(object, inner, "object");
    }
    let nested = object_fn(field(object, outer, "object")?, outer)?;
    string_field(nested, inner, outer)
}

fn nested_string_value<'a>(
    object: &'a Map<String, Value>,
    outer: &str,
    inner: &str,
) -> Result<&'a str, GateError> {
    nested_string(object, outer, inner)
}

fn object_fn<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, GateError> {
    object(value, label)
}

fn value_id(object: &Map<String, Value>, field: &str, label: &str) -> Result<String, GateError> {
    value_id_or_string(object, field, label).map(str::to_owned)
}

fn value_id_or_string<'a>(
    object: &'a Map<String, Value>,
    name: &str,
    label: &str,
) -> Result<&'a str, GateError> {
    let value = field(object, name, label)?;
    if let Some(value) = value.as_str().filter(|value| !value.is_empty()) {
        return Ok(value);
    }
    Err(policy(format!(
        "{label}.{name} must be a string identifier"
    )))
}

fn unique_array_entry<'a>(
    value: &'a Value,
    field_name: &str,
    label: &str,
) -> Result<&'a Map<String, Value>, GateError> {
    let entries = field(object(value, label)?, field_name, label)?
        .as_array()
        .ok_or_else(|| policy(format!("{label}.{field_name} must be an array")))?;
    if entries.len() != 1 {
        return Err(policy(format!("{label} must identify exactly one result")));
    }
    object(&entries[0], label)
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

fn now() -> String {
    chrono::DateTime::<Utc>::from(std::time::SystemTime::now())
        .to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("publication.contract", detail)
}

fn internal(detail: impl Into<String>) -> GateError {
    GateError::internal("publication.invariant", detail)
}
