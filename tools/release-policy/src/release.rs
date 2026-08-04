//! Artifact-derived candidate verification boundary.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{Map, Value, json};
use tempfile::{Builder, NamedTempFile};

use crate::bundle::{self, AssemblyRequest};
use crate::canonical;
use crate::child::{self, ChildSpec, Termination};
use crate::cli::github::{
    DownloadedAsset, GitHubTransport, MAX_RELEASE_ASSET_BYTES, MAX_RELEASE_ASSET_SET_BYTES, Method,
    Request as GitHubRequest,
};
use crate::decision::{self, DecisionRequest};
use crate::digest::{sha256_hex, sha256_reader_bounded};
use crate::error::GateError;
use crate::evidence::{self, EvidenceKind};
use crate::inventory;
use crate::limits::{MAX_CHILD_OUTPUT_BYTES, MAX_INPUT_BYTES, read_bounded};

const BUILD_WORKFLOW: &str = ".github/workflows/release.yml";
const PUBLISH_WORKFLOW: &str = ".github/workflows/publish-release.yml";
const METADATA_MARKER: &str = "\n\n<!-- clinker-release-metadata\n";
const METADATA_END: &str = "\n-->\n";
const AUTHORIZATION_IDENTITY_FIELDS: [&str; 9] = [
    "candidate_tag",
    "candidate_version",
    "source_sha",
    "publish_workflow_ref",
    "publish_workflow_ref_resolved_sha",
    "publish_workflow_sha",
    "changelog_ref",
    "inventory_ref",
    "authorized_release_maintainer_ref",
];

/// Exact artifact-derived candidate producer/readback request.
#[derive(Debug, Clone)]
pub struct CandidateRequest {
    /// Exact repository identity.
    pub repository: String,
    /// Canonical decision directory, required only for accepted-candidate readback.
    pub decision_dir: Option<PathBuf>,
    /// Separate candidate-authorization record.
    pub authorization_record: PathBuf,
    /// Candidate-authorization schema.
    pub authorization_schema: PathBuf,
    /// Accepted post-build candidate decision, required only for readback.
    pub decision_record: Option<PathBuf>,
    /// Decision schema, required only for readback.
    pub decision_schema: Option<PathBuf>,
    /// Existing candidate evidence for fresh byte readback.
    pub candidate_evidence: Option<PathBuf>,
    /// Candidate evidence schema.
    pub evidence_schema: PathBuf,
    /// Producer destination when creating candidate evidence.
    pub evidence_manifest: Option<PathBuf>,
}

/// Token-backed workflow context for the help-hidden private-draft worker.
#[derive(Debug, Clone)]
pub struct StageWorkflowContext {
    pub repository: String,
    pub run_id: String,
    pub run_attempt: u64,
    pub workflow_ref: String,
    pub workflow_sha: String,
    pub git_ref: String,
    pub source_sha: String,
}

impl StageWorkflowContext {
    /// Read standard GitHub Actions context for subsequent remote comparison.
    pub fn from_environment() -> Result<Self, GateError> {
        Ok(Self {
            repository: required_environment("GITHUB_REPOSITORY")?,
            run_id: required_environment("GITHUB_RUN_ID")?,
            run_attempt: required_environment("GITHUB_RUN_ATTEMPT")?
                .parse()
                .map_err(|_| GateError::usage("GITHUB_RUN_ATTEMPT must be an integer"))?,
            workflow_ref: required_environment("GITHUB_WORKFLOW_REF")?,
            workflow_sha: required_environment("GITHUB_WORKFLOW_SHA")?,
            git_ref: required_environment("GITHUB_REF")?,
            source_sha: required_environment("GITHUB_SHA")?,
        })
    }
}

/// Exact inputs for the help-hidden private candidate draft worker.
#[derive(Debug, Clone)]
pub struct StageDraftRequest {
    pub repository: String,
    pub candidate_tag: String,
    pub source_sha: String,
    pub asset_dir: PathBuf,
    pub context: StageWorkflowContext,
    pub deadline_seconds: u64,
}

/// Create or byte-identically reconcile one private candidate draft.
///
/// No evidence path or public-release option is accepted by this boundary.
pub fn stage_candidate_draft(
    repo_root: &Path,
    request: &StageDraftRequest,
    transport: &mut dyn GitHubTransport,
) -> Result<String, GateError> {
    if request.repository != "rustpunk/clinker" {
        return Err(policy(
            "candidate draft repository must be rustpunk/clinker",
        ));
    }
    validate_stage_context(request)?;
    let deadline = stage_deadline(request.deadline_seconds)?;
    let (_, release_inventory) = inventory::load(repo_root, None)?;
    if request.candidate_tag != format!("v{}", release_inventory.version) {
        return Err(policy(
            "candidate draft tag must equal v plus the governed inventory version",
        ));
    }
    validate_sha40(&request.source_sha, "--source-sha")?;
    bundle::verify_assembly(
        &release_inventory,
        &AssemblyRequest {
            asset_dir: request.asset_dir.clone(),
            draft_dir: None,
            repository: request.repository.clone(),
            workflow: BUILD_WORKFLOW.to_owned(),
            release_ref: format!("refs/tags/{}", request.candidate_tag),
            source_sha: request.source_sha.clone(),
        },
    )?;
    let expected = local_release_assets(&release_inventory, &request.asset_dir)?;

    let run = transport.send(&GitHubRequest::new(
        Method::Get,
        format!(
            "repos/{}/actions/runs/{}",
            request.repository, request.context.run_id
        ),
        deadline,
    ))?;
    validate_stage_run(&run.body, request)?;
    let tag = transport.send(&GitHubRequest::new(
        Method::Get,
        format!(
            "repos/{}/git/ref/tags/{}",
            request.repository, request.candidate_tag
        ),
        deadline,
    ))?;
    validate_stage_tag(&tag.body, request)?;

    let mut release = find_candidate_release(request, deadline, transport)?;
    let created = release.is_none();
    if release.is_none() {
        let created_release = transport.send(
            &GitHubRequest::new(
                Method::Post,
                format!("repos/{}/releases", request.repository),
                deadline,
            )
            .field("tag_name", &request.candidate_tag)
            .field("target_commitish", &request.source_sha)
            .field("name", format!("Clinker {}", request.candidate_tag))
            .field("draft", "true")
            .field(
                "prerelease",
                tag_is_prerelease(&request.candidate_tag).to_string(),
            )
            .field("generate_release_notes", "true"),
        )?;
        let created_release = release_object(&created_release.body, "created candidate release")?;
        validate_stage_release_identity(created_release, request)?;
        if !bool_value(created_release, "draft", "created candidate release")? {
            return Err(policy("created candidate release is not private"));
        }
        release = Some(Value::Object(created_release.clone()));
    }
    let release = release.ok_or_else(|| {
        GateError::internal(
            "release.stage",
            "candidate release disappeared after create",
        )
    })?;
    let release = release_object(&release, "candidate release")?;
    validate_stage_release_identity(release, request)?;
    if !bool_value(release, "draft", "candidate release")? {
        return Err(policy(
            "candidate release is already public; repair is forbidden",
        ));
    }
    let release_id = value_id(release, "id", "candidate release")?;
    let observed_body = string_field(release, "body", "candidate release")?;
    let release_notes = if created {
        observed_body.trim_end()
    } else {
        split_release_body(observed_body)?.0
    };
    let metadata = stage_metadata(request, &release_id, release_notes)?;
    let metadata = std::str::from_utf8(&metadata)
        .map_err(|_| GateError::internal("release.stage", "metadata was not UTF-8"))?;
    let expected_body = release_body(release_notes, metadata);
    if created {
        let updated = transport.send(
            &GitHubRequest::new(
                Method::Patch,
                format!("repos/{}/releases/{release_id}", request.repository),
                deadline,
            )
            .field("body", &expected_body)
            .field("draft", "true")
            .field(
                "prerelease",
                tag_is_prerelease(&request.candidate_tag).to_string(),
            ),
        )?;
        if !updated.body.is_null() {
            let updated = release_object(&updated.body, "candidate metadata update")?;
            validate_stage_release_identity(updated, request)?;
            if !bool_value(updated, "draft", "candidate metadata update")?
                || string_field(updated, "body", "candidate metadata update")? != expected_body
            {
                return Err(policy(
                    "candidate metadata update did not preserve exact private state",
                ));
            }
        }
    } else if observed_body != expected_body {
        return Err(policy(
            "existing candidate draft metadata differs; repair is forbidden",
        ));
    }

    let existing = remote_asset_map(release, "candidate release")?;
    reject_remote_asset_drift(&existing, &expected)?;
    let mut downloaded_bytes = 0_u64;
    for (name, local) in &expected {
        if let Some(remote) = existing.get(name) {
            if remote.length != local.length {
                return Err(policy(format!(
                    "existing candidate asset size differs from local authority: {name}"
                )));
            }
            let asset =
                download_release_asset(&request.repository, &remote.id, deadline, transport)?;
            add_downloaded_bytes(&mut downloaded_bytes, asset.length())?;
            if !asset.matches_identity(local.length, &local.sha256) {
                return Err(policy(format!(
                    "existing candidate asset differs from local authority: {name}"
                )));
            }
        }
    }
    for (name, local) in &expected {
        if existing.contains_key(name) {
            continue;
        }
        let uploaded = transport.send(
            &GitHubRequest::new(
                Method::Post,
                format!(
                    "https://uploads.github.com/repos/{}/releases/{release_id}/assets?name={name}",
                    request.repository
                ),
                deadline,
            )
            .header("Content-Type", "application/octet-stream")
            .input_file(local.path.clone()),
        )?;
        if !uploaded.body.is_null() {
            let uploaded = release_object(&uploaded.body, "uploaded candidate asset")?;
            if string_field(uploaded, "name", "uploaded candidate asset")? != name {
                return Err(policy(
                    "uploaded candidate asset name differs from inventory",
                ));
            }
        }
    }

    let reread = find_candidate_release(request, deadline, transport)?
        .ok_or_else(|| policy("candidate release disappeared during staging"))?;
    let reread = release_object(&reread, "staged candidate release")?;
    validate_stage_release_identity(reread, request)?;
    if !bool_value(reread, "draft", "staged candidate release")?
        || value_id(reread, "id", "staged candidate release")? != release_id
        || string_field(reread, "body", "staged candidate release")? != expected_body
    {
        return Err(policy("candidate release identity changed during staging"));
    }
    let final_assets = remote_asset_map(reread, "staged candidate release")?;
    if final_assets.keys().collect::<BTreeSet<_>>() != expected.keys().collect::<BTreeSet<_>>() {
        return Err(policy(
            "staged candidate asset inventory is incomplete or contains extras",
        ));
    }
    let mut downloaded_bytes = 0_u64;
    for (name, remote) in final_assets {
        let asset = download_release_asset(&request.repository, &remote.id, deadline, transport)?;
        add_downloaded_bytes(&mut downloaded_bytes, asset.length())?;
        if expected.get(&name).is_none_or(|expected| {
            remote.length != expected.length
                || !asset.matches_identity(expected.length, &expected.sha256)
        }) {
            return Err(policy(format!(
                "fresh staged candidate asset differs from local authority: {name}"
            )));
        }
    }
    Ok(format!(
        "Private candidate draft staged and verified: {release_id}\n"
    ))
}

fn validate_stage_context(request: &StageDraftRequest) -> Result<(), GateError> {
    let context = &request.context;
    if context.repository != request.repository
        || context.run_attempt != 1
        || context.git_ref != format!("refs/tags/{}", request.candidate_tag)
        || context.source_sha != request.source_sha
        || context.workflow_sha != request.source_sha
        || context.workflow_ref
            != format!(
                "{}/.github/workflows/release.yml@refs/tags/{}",
                request.repository, request.candidate_tag
            )
    {
        return Err(policy(
            "draft worker context must identify the exact first release tag run",
        ));
    }
    Ok(())
}

fn validate_stage_run(value: &Value, request: &StageDraftRequest) -> Result<(), GateError> {
    let run = release_object(value, "release workflow run")?;
    if value_id(run, "id", "release workflow run")? != request.context.run_id
        || run.get("run_attempt").and_then(Value::as_u64) != Some(1)
        || string_field(run, "head_sha", "release workflow run")? != request.source_sha
        || string_field(run, "event", "release workflow run")? != "push"
        || string_field(run, "path", "release workflow run")? != BUILD_WORKFLOW
    {
        return Err(policy(
            "token-backed release workflow readback differs from expected context",
        ));
    }
    Ok(())
}

fn validate_stage_tag(value: &Value, request: &StageDraftRequest) -> Result<(), GateError> {
    let tag = release_object(value, "release tag readback")?;
    let object = tag
        .get("object")
        .and_then(Value::as_object)
        .ok_or_else(|| policy("release tag readback object is absent"))?;
    if string_field(tag, "ref", "release tag readback")?
        != format!("refs/tags/{}", request.candidate_tag)
        || string_field(object, "type", "release tag object")? != "commit"
        || string_field(object, "sha", "release tag object")? != request.source_sha
    {
        return Err(policy(
            "release tag does not resolve directly to the exact source commit",
        ));
    }
    Ok(())
}

fn stage_deadline(seconds: u64) -> Result<Duration, GateError> {
    if seconds == 0 || seconds > 3600 {
        return Err(GateError::usage(
            "draft deadline must be between 1 and 3600 seconds",
        ));
    }
    Ok(Duration::from_secs(seconds))
}

fn local_release_assets(
    inventory: &inventory::ReleaseInventory,
    directory: &Path,
) -> Result<BTreeMap<String, LocalReleaseAsset>, GateError> {
    require_complete_asset_set(inventory, directory)?;
    let mut assets = BTreeMap::new();
    let mut names = BTreeSet::from(["SHA256SUMS".to_owned()]);
    for target in &inventory.targets {
        names.insert(target.archive_name.clone());
        names.insert(format!("{}.sha256", target.archive_name));
        names.insert(format!("{}.intoto.jsonl", target.archive_name));
    }
    let mut aggregate = 0_u64;
    for name in names {
        if !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        {
            return Err(policy(
                "local release asset names must be safe GitHub upload identifiers",
            ));
        }
        let path = directory.join(&name);
        let metadata = fs::symlink_metadata(&path)
            .map_err(|error| GateError::io("inspect local release asset", &error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(policy(
                "local release assets must be regular non-symlink files",
            ));
        }
        if metadata.len() > MAX_RELEASE_ASSET_BYTES {
            return Err(policy(format!(
                "local release asset exceeds the {MAX_RELEASE_ASSET_BYTES}-byte limit: {name}"
            )));
        }
        aggregate = aggregate
            .checked_add(metadata.len())
            .ok_or_else(|| policy("local release asset sizes overflowed"))?;
        if aggregate > MAX_RELEASE_ASSET_SET_BYTES {
            return Err(policy(format!(
                "local release asset set exceeds the {MAX_RELEASE_ASSET_SET_BYTES}-byte limit"
            )));
        }
        let (length, sha256) = sha256_reader_bounded(
            File::open(&path).map_err(|error| GateError::io("open local release asset", &error))?,
            MAX_RELEASE_ASSET_BYTES,
        )
        .map_err(|error| GateError::io("hash local release asset", &error))?;
        if length != metadata.len() {
            return Err(policy(format!(
                "local release asset changed while it was admitted: {name}"
            )));
        }
        assets.insert(
            name,
            LocalReleaseAsset {
                path,
                length,
                sha256,
            },
        );
    }
    Ok(assets)
}

#[derive(Debug)]
struct LocalReleaseAsset {
    path: PathBuf,
    length: u64,
    sha256: String,
}

fn find_candidate_release(
    request: &StageDraftRequest,
    deadline: Duration,
    transport: &mut dyn GitHubTransport,
) -> Result<Option<Value>, GateError> {
    let response = transport.send(
        &GitHubRequest::new(
            Method::Get,
            format!("repos/{}/releases", request.repository),
            deadline,
        )
        .field("per_page", "100"),
    )?;
    let releases = response
        .body
        .as_array()
        .ok_or_else(|| policy("release list response must be an array"))?;
    let matches = releases
        .iter()
        .filter(|release| {
            release.get("tag_name").and_then(Value::as_str) == Some(request.candidate_tag.as_str())
        })
        .collect::<Vec<_>>();
    if matches.len() > 1 {
        return Err(policy(
            "candidate tag resolved to ambiguous duplicate releases",
        ));
    }
    Ok(matches.first().map(|release| (*release).clone()))
}

fn validate_stage_release_identity(
    release: &Map<String, Value>,
    request: &StageDraftRequest,
) -> Result<(), GateError> {
    value_id(release, "id", "candidate release")?;
    if string_field(release, "tag_name", "candidate release")? != request.candidate_tag
        || string_field(release, "target_commitish", "candidate release")? != request.source_sha
    {
        return Err(policy(
            "candidate release tag or source differs from governed input",
        ));
    }
    bool_value(release, "draft", "candidate release")?;
    if bool_value(release, "prerelease", "candidate release")?
        != tag_is_prerelease(&request.candidate_tag)
    {
        return Err(policy(
            "candidate release prerelease state differs from its tag",
        ));
    }
    string_field(release, "body", "candidate release")?;
    release
        .get("assets")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("candidate release assets must be an array"))?;
    Ok(())
}

fn stage_metadata(
    request: &StageDraftRequest,
    release_id: &str,
    release_notes: &str,
) -> Result<Vec<u8>, GateError> {
    let value = json!({
        "build_workflow_path": BUILD_WORKFLOW,
        "build_workflow_sha": request.context.workflow_sha,
        "build_run_id": request.context.run_id,
        "build_event": "push",
        "build_ref": request.context.git_ref,
        "build_head_sha": request.source_sha,
        "source_sha": request.source_sha,
        "publish_workflow_ref": request.candidate_tag,
        "publish_workflow_sha": request.source_sha,
        "candidate_release_id": release_id,
        "release_notes_sha256": sha256_hex(release_notes.as_bytes()),
    });
    let parsed = canonical::parse_json(&serde_json::to_vec(&value).map_err(|_| {
        GateError::internal("release.stage", "candidate metadata serialization failed")
    })?)?;
    canonical::to_bytes(&parsed)
}

fn tag_is_prerelease(tag: &str) -> bool {
    tag.trim_start_matches('v')
        .split_once('+')
        .map_or(tag.trim_start_matches('v'), |(version, _)| version)
        .contains('-')
}

fn release_body(release_notes: &str, metadata: &str) -> String {
    format!(
        "{}{METADATA_MARKER}{}{METADATA_END}",
        release_notes.trim_end(),
        metadata.trim_end()
    )
}

fn split_release_body(body: &str) -> Result<(&str, &str), GateError> {
    let (notes, metadata) = body
        .rsplit_once(METADATA_MARKER)
        .ok_or_else(|| policy("private release metadata trailer is absent"))?;
    let metadata = metadata
        .strip_suffix(METADATA_END)
        .ok_or_else(|| policy("private release metadata trailer is malformed"))?;
    if sha256_hex(notes.trim_end().as_bytes())
        != metadata_value(metadata)?
            .get("release_notes_sha256")
            .and_then(Value::as_str)
            .ok_or_else(|| policy("release notes digest is absent"))?
    {
        return Err(policy("release notes differ from their metadata digest"));
    }
    Ok((notes.trim_end(), metadata))
}

fn metadata_value(metadata: &str) -> Result<Map<String, Value>, GateError> {
    canonical::parse_json(metadata.as_bytes())?;
    serde_json::from_str::<Value>(metadata)
        .map_err(|_| policy("private release metadata is malformed"))?
        .as_object()
        .cloned()
        .ok_or_else(|| policy("private release metadata must be an object"))
}

fn remote_asset_map(
    release: &Map<String, Value>,
    label: &str,
) -> Result<BTreeMap<String, RemoteReleaseAsset>, GateError> {
    let assets = release
        .get("assets")
        .and_then(Value::as_array)
        .ok_or_else(|| policy(format!("{label}.assets must be an array")))?;
    let mut observed = BTreeMap::new();
    let mut aggregate = 0_u64;
    for asset in assets {
        let asset = release_object(asset, "candidate release asset")?;
        let name = string_field(asset, "name", "candidate release asset")?.to_owned();
        let id = value_id(asset, "id", "candidate release asset")?;
        let length = asset
            .get("size")
            .and_then(Value::as_u64)
            .ok_or_else(|| policy("candidate release asset size must be an integer"))?;
        if length > MAX_RELEASE_ASSET_BYTES {
            return Err(policy("candidate release asset exceeds its byte limit"));
        }
        aggregate = aggregate
            .checked_add(length)
            .ok_or_else(|| policy("candidate release asset sizes overflowed"))?;
        if aggregate > MAX_RELEASE_ASSET_SET_BYTES {
            return Err(policy("candidate release asset set exceeds its byte limit"));
        }
        if observed
            .insert(name, RemoteReleaseAsset { id, length })
            .is_some()
        {
            return Err(policy("candidate release contains duplicate asset names"));
        }
    }
    Ok(observed)
}

#[derive(Debug)]
struct RemoteReleaseAsset {
    id: String,
    length: u64,
}

fn add_downloaded_bytes(total: &mut u64, length: u64) -> Result<(), GateError> {
    *total = total
        .checked_add(length)
        .ok_or_else(|| policy("downloaded release asset sizes overflowed"))?;
    if *total > MAX_RELEASE_ASSET_SET_BYTES {
        return Err(policy(
            "downloaded release asset set exceeds its byte limit",
        ));
    }
    Ok(())
}

fn reject_remote_asset_drift(
    remote: &BTreeMap<String, RemoteReleaseAsset>,
    expected: &BTreeMap<String, LocalReleaseAsset>,
) -> Result<(), GateError> {
    if remote.keys().any(|name| !expected.contains_key(name)) {
        return Err(policy(
            "candidate release contains an undeclared, renamed, or starter asset",
        ));
    }
    Ok(())
}

fn download_release_asset(
    repository: &str,
    asset_id: &str,
    deadline: Duration,
    transport: &mut dyn GitHubTransport,
) -> Result<DownloadedAsset, GateError> {
    transport.download(
        &GitHubRequest::new(
            Method::Get,
            format!("repos/{repository}/releases/assets/{asset_id}"),
            deadline,
        )
        .header("Accept", "application/octet-stream")
        .raw(),
        MAX_RELEASE_ASSET_BYTES,
    )
}

fn release_object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>, GateError> {
    value
        .as_object()
        .ok_or_else(|| policy(format!("{label} must be an object")))
}

fn value_id(object: &Map<String, Value>, field: &str, label: &str) -> Result<String, GateError> {
    if let Some(value) = object.get(field).and_then(Value::as_str)
        && !value.is_empty()
    {
        return Ok(value.to_owned());
    }
    if let Some(value) = object.get(field).and_then(Value::as_u64) {
        return Ok(value.to_string());
    }
    Err(policy(format!("{label}.{field} must be an identifier")))
}

fn bool_value(object: &Map<String, Value>, field: &str, label: &str) -> Result<bool, GateError> {
    object
        .get(field)
        .and_then(Value::as_bool)
        .ok_or_else(|| policy(format!("{label}.{field} must be a boolean")))
}

fn validate_sha40(value: &str, label: &str) -> Result<(), GateError> {
    if value.len() != 40
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(policy(format!(
            "{label} must be a lowercase full commit SHA"
        )));
    }
    Ok(())
}

fn required_environment(name: &'static str) -> Result<String, GateError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| GateError::usage(format!("{name} must be set by GitHub Actions")))
}

/// Validate immutable authority before any live download or evidence mutation.
pub fn verify_candidate(repo_root: &Path, request: &CandidateRequest) -> Result<String, GateError> {
    if request.repository != "rustpunk/clinker" {
        return Err(policy("candidate repository must be rustpunk/clinker"));
    }
    let root = fs::canonicalize(repo_root)
        .map_err(|error| GateError::io("resolve repository root", &error))?;
    contained_file(&root, &request.authorization_record)?;
    contained_file(&root, &request.authorization_schema)?;
    contained_file(&root, &request.evidence_schema)?;
    let existing_candidate = if let Some(candidate) = &request.candidate_evidence {
        let candidate = contained_file(&root, candidate)?;
        crate::cli::publication::validate_evidence_file(
            EvidenceKind::Candidate,
            &resolve(&root, &request.evidence_schema),
            &candidate,
        )?;
        Some(candidate)
    } else {
        None
    };
    match (
        &request.decision_dir,
        &request.decision_record,
        &request.decision_schema,
    ) {
        (Some(directory), Some(record), Some(schema)) => {
            let decision_dir = contained_directory(&root, directory)?;
            for path in [&request.authorization_record, record] {
                let path = contained_file(&root, path)?;
                if !path.starts_with(&decision_dir) {
                    return Err(policy(
                        "candidate authority records must remain beneath --decision-dir",
                    ));
                }
            }
            contained_file(&root, schema)?;
            decision::validate(&DecisionRequest {
                schema: Some(resolve(&root, schema)),
                records: vec![resolve(&root, record)],
                authorization_schema: Some(resolve(&root, &request.authorization_schema)),
                authorization_record: Some(resolve(&root, &request.authorization_record)),
                candidate_evidence: existing_candidate.clone(),
                require_ids: Vec::new(),
                require_authorization_id: None,
                require_authorized: true,
                require_complete: false,
                require_accepted: true,
            })?;
        }
        (None, None, None) => decision::validate(&DecisionRequest {
            schema: None,
            records: Vec::new(),
            authorization_schema: Some(resolve(&root, &request.authorization_schema)),
            authorization_record: Some(resolve(&root, &request.authorization_record)),
            candidate_evidence: None,
            require_ids: Vec::new(),
            require_authorization_id: None,
            require_authorized: true,
            require_complete: false,
            require_accepted: false,
        })?,
        _ => {
            return Err(policy(
                "candidate decision directory, record, and schema must be supplied together",
            ));
        }
    }
    let destination = if let Some(destination) = &request.evidence_manifest {
        let destination = resolve(&root, destination);
        let parent = destination
            .parent()
            .ok_or_else(|| policy("candidate evidence destination has no parent"))?;
        let parent = fs::canonicalize(parent)
            .map_err(|error| GateError::io("resolve candidate evidence parent", &error))?;
        if !parent.starts_with(&root) {
            return Err(policy(
                "candidate evidence destination escapes the repository",
            ));
        }
        Some(destination)
    } else {
        None
    };

    let authorization = read_json(
        &resolve(&root, &request.authorization_record),
        "candidate authorization",
    )?;
    let authority = authority(&authorization)?;
    let tag = authority_string(authority, "candidate_tag")?;
    let version = authority_string(authority, "candidate_version")?;
    let source_sha = authority_string(authority, "source_sha")?;
    let authorization_digest = string_field(
        authorization
            .as_object()
            .ok_or_else(|| policy("candidate authorization must be an object"))?,
        "candidate_authorization_sha256",
        "candidate authorization",
    )?;
    let (_, release_inventory) = inventory::load(&root, None)?;
    if release_inventory.version != version {
        return Err(policy(
            "candidate version does not match the committed inventory",
        ));
    }

    let initial_release = release_view(&request.repository, tag)?;
    validate_release_view(&initial_release, authority)?;
    let initial_tag = tag_view(&request.repository, tag)?;
    validate_tag_view(&initial_tag, authority)?;

    let first = Builder::new()
        .prefix("release-release-readback-")
        .tempdir()
        .map_err(|error| GateError::io("create fresh release readback directory", &error))?;
    let second = Builder::new()
        .prefix("release-release-reread-")
        .tempdir()
        .map_err(|error| GateError::io("create final release reread directory", &error))?;
    download_release(&request.repository, tag, first.path())?;
    download_release(&request.repository, tag, second.path())?;
    require_complete_asset_set(&release_inventory, first.path())?;
    require_complete_asset_set(&release_inventory, second.path())?;
    bundle::verify_assembly(
        &release_inventory,
        &AssemblyRequest {
            asset_dir: first.path().to_path_buf(),
            draft_dir: Some(second.path().to_path_buf()),
            repository: request.repository.clone(),
            workflow: BUILD_WORKFLOW.to_owned(),
            release_ref: format!("refs/tags/{tag}"),
            source_sha: source_sha.to_owned(),
        },
    )?;
    let (archive_digests, checksum_sha256) =
        observed_candidate_digests(&release_inventory, first.path())?;

    let final_release = release_view(&request.repository, tag)?;
    if initial_release != final_release {
        return Err(policy(
            "private release identity changed during fresh reread",
        ));
    }
    let observation = CandidateObservation {
        release: &initial_release,
        tag: &initial_tag,
        archive_digests: &archive_digests,
        checksum_sha256: &checksum_sha256,
    };
    let candidate = candidate_value(
        &request.repository,
        &release_inventory,
        first.path(),
        authority,
        authorization_digest,
        &observation,
    )?;
    let candidate_bytes = serde_json::to_vec(&candidate).map_err(|_| {
        GateError::internal(
            "release.candidate_json",
            "candidate evidence serialization failed",
        )
    })?;
    let canonical_candidate = canonical::parse_json(&candidate_bytes)?;

    let mut staged = NamedTempFile::new_in(first.path())
        .map_err(|error| GateError::io("stage candidate evidence validation", &error))?;
    use std::io::Write as _;
    staged
        .write_all(&canonical::to_bytes(&canonical_candidate)?)
        .map_err(|error| GateError::io("write staged candidate evidence", &error))?;
    staged
        .flush()
        .map_err(|error| GateError::io("flush staged candidate evidence", &error))?;
    crate::cli::publication::validate_evidence_file(
        EvidenceKind::Candidate,
        &resolve(&root, &request.evidence_schema),
        staged.path(),
    )?;

    if let Some(existing) = existing_candidate {
        let bytes = read_bounded(
            &existing,
            "read existing candidate evidence",
            MAX_INPUT_BYTES,
        )?;
        if canonical::parse_json(&bytes)? != canonical_candidate
            || canonical::to_bytes(&canonical_candidate)? != bytes
        {
            return Err(policy(
                "candidate evidence differs from fresh private release readback",
            ));
        }
        return Ok(
            "Fresh private release bytes and candidate evidence readback verified.\n".to_owned(),
        );
    }
    let destination = destination.ok_or_else(|| {
        GateError::internal(
            "release.candidate_destination",
            "candidate destination is missing after CLI preflight",
        )
    })?;
    evidence::create_only(&destination, &canonical_candidate)?;
    Ok("Fresh private release bytes verified and candidate evidence created.\n".to_owned())
}

fn release_view(repository: &str, tag: &str) -> Result<Value, GateError> {
    gh_json(&[
        "release",
        "view",
        tag,
        "--repo",
        repository,
        "--json",
        "id,isDraft,tagName,body",
    ])
}

fn tag_view(repository: &str, tag: &str) -> Result<Value, GateError> {
    gh_json(&["api", &format!("repos/{repository}/git/ref/tags/{tag}")])
}

fn download_release(repository: &str, tag: &str, destination: &Path) -> Result<(), GateError> {
    gh(&[
        "release",
        "download",
        tag,
        "--repo",
        repository,
        "--dir",
        destination
            .to_str()
            .ok_or_else(|| policy("fresh release directory is not UTF-8"))?,
    ])?;
    Ok(())
}

fn gh_json(arguments: &[&str]) -> Result<Value, GateError> {
    let output = gh(arguments)?;
    canonical::parse_json(&output)?;
    serde_json::from_slice(&output).map_err(|_| policy("GitHub response is not valid JSON"))
}

fn gh(arguments: &[&str]) -> Result<Vec<u8>, GateError> {
    let result = child::run(ChildSpec {
        program: PathBuf::from("gh"),
        arguments: arguments.iter().map(OsString::from).collect(),
        environment: child::github_environment(),
        timeout: Duration::from_secs(300),
        output_limit: MAX_CHILD_OUTPUT_BYTES,
    })?;
    if result.termination != Termination::Exited(Some(0))
        || result.stdout_truncated
        || result.stderr_truncated
    {
        return Err(policy(
            "authenticated GitHub release readback failed or exceeded its bound",
        ));
    }
    Ok(result.stdout)
}

fn validate_release_view(release: &Value, authority: &Map<String, Value>) -> Result<(), GateError> {
    let release = exact_object(
        release,
        &["id", "isDraft", "tagName", "body"],
        "release readback",
    )?;
    if release.get("isDraft").and_then(Value::as_bool) != Some(true) {
        return Err(policy("candidate release is not a private draft"));
    }
    if release.get("tagName") != authority.get("candidate_tag") {
        return Err(policy(
            "release tagName does not match candidate authorization",
        ));
    }
    string_field(release, "id", "release readback")?;
    let body = release
        .get("body")
        .and_then(Value::as_str)
        .ok_or_else(|| policy("private release metadata body is absent"))?;
    let (_, metadata_text) = split_release_body(body)?;
    let metadata = Value::Object(metadata_value(metadata_text)?);
    let metadata = exact_object(
        &metadata,
        &[
            "build_workflow_path",
            "build_workflow_sha",
            "build_run_id",
            "build_event",
            "build_ref",
            "build_head_sha",
            "source_sha",
            "publish_workflow_ref",
            "publish_workflow_sha",
            "candidate_release_id",
            "release_notes_sha256",
        ],
        "private release metadata",
    )?;
    for (metadata_field, authority_field) in [
        ("build_head_sha", "source_sha"),
        ("source_sha", "source_sha"),
        ("publish_workflow_ref", "publish_workflow_ref"),
        ("publish_workflow_sha", "publish_workflow_sha"),
    ] {
        if metadata.get(metadata_field) != authority.get(authority_field) {
            return Err(policy(format!(
                "release metadata {metadata_field} does not match candidate authority"
            )));
        }
    }
    if metadata.get("build_workflow_sha") != authority.get("source_sha") {
        return Err(policy(
            "release metadata build_workflow_sha does not match the authorized source",
        ));
    }
    if metadata.get("candidate_release_id") != release.get("id") {
        return Err(policy(
            "release metadata candidate_release_id does not match release readback",
        ));
    }
    if metadata.get("build_workflow_path") != Some(&Value::String(BUILD_WORKFLOW.to_owned()))
        || metadata.get("build_event") != Some(&Value::String("push".to_owned()))
        || metadata.get("build_ref")
            != Some(&Value::String(format!(
                "refs/tags/{}",
                authority_string(authority, "candidate_tag")?
            )))
    {
        return Err(policy(
            "release build workflow metadata does not match the protected tag build",
        ));
    }
    string_field(metadata, "build_run_id", "private release metadata")?;
    string_field(metadata, "release_notes_sha256", "private release metadata")?;
    Ok(())
}

fn validate_tag_view(tag: &Value, authority: &Map<String, Value>) -> Result<(), GateError> {
    let tag = tag
        .as_object()
        .ok_or_else(|| policy("tag readback must be an object"))?;
    let candidate_tag = authority_string(authority, "candidate_tag")?;
    if tag.get("ref") != Some(&Value::String(format!("refs/tags/{candidate_tag}"))) {
        return Err(policy(
            "tag readback ref does not match candidate authority",
        ));
    }
    let url = string_field(tag, "url", "tag readback")?;
    if !url.trim_end_matches('/').ends_with(candidate_tag) {
        return Err(policy(
            "tag readback URL does not identify the candidate tag",
        ));
    }
    let object = tag
        .get("object")
        .and_then(Value::as_object)
        .ok_or_else(|| policy("tag readback object is absent"))?;
    if object.get("type") != Some(&Value::String("commit".to_owned()))
        || object.get("sha") != authority.get("source_sha")
    {
        return Err(policy(
            "candidate tag does not directly resolve to the authorized source commit",
        ));
    }
    Ok(())
}

fn require_complete_asset_set(
    inventory: &inventory::ReleaseInventory,
    directory: &Path,
) -> Result<(), GateError> {
    let mut expected = BTreeSet::from(["SHA256SUMS".to_owned()]);
    for target in &inventory.targets {
        expected.insert(target.archive_name.clone());
        expected.insert(format!("{}.sha256", target.archive_name));
        expected.insert(format!("{}.intoto.jsonl", target.archive_name));
    }
    let mut observed = BTreeSet::new();
    for entry in fs::read_dir(directory)
        .map_err(|error| GateError::io("inspect fresh release download", &error))?
    {
        let entry = entry.map_err(|error| GateError::io("inspect fresh release asset", &error))?;
        let metadata = entry
            .metadata()
            .map_err(|error| GateError::io("inspect fresh release asset metadata", &error))?;
        if !metadata.is_file()
            || entry
                .file_type()
                .map(|kind| kind.is_symlink())
                .unwrap_or(true)
        {
            return Err(policy(
                "fresh release download contains a non-regular asset",
            ));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| policy("fresh release asset name is not UTF-8"))?;
        if !observed.insert(name) {
            return Err(policy(
                "fresh release download contains duplicate asset names",
            ));
        }
    }
    if observed != expected {
        return Err(policy(
            "fresh release asset inventory is incomplete or contains extras",
        ));
    }
    Ok(())
}

fn observed_candidate_digests(
    inventory: &inventory::ReleaseInventory,
    directory: &Path,
) -> Result<(Map<String, Value>, String), GateError> {
    let mut observed = Map::new();
    for target in &inventory.targets {
        let path = directory.join(&target.archive_name);
        observed.insert(
            target.target.clone(),
            Value::String(
                sha256_reader_bounded(
                    File::open(&path)
                        .map_err(|error| GateError::io("open candidate archive", &error))?,
                    MAX_RELEASE_ASSET_BYTES,
                )
                .map_err(|error| GateError::io("hash candidate archive", &error))?
                .1,
            ),
        );
    }
    let checksum = read_bounded(
        &directory.join("SHA256SUMS"),
        "read candidate checksums",
        MAX_INPUT_BYTES,
    )?;
    Ok((observed, sha256_hex(&checksum)))
}

struct CandidateObservation<'a> {
    release: &'a Value,
    tag: &'a Value,
    archive_digests: &'a Map<String, Value>,
    checksum_sha256: &'a str,
}

fn candidate_value(
    repository: &str,
    inventory: &inventory::ReleaseInventory,
    directory: &Path,
    authority: &Map<String, Value>,
    authorization_digest: &str,
    observation: &CandidateObservation<'_>,
) -> Result<Value, GateError> {
    let mut candidate = Map::new();
    candidate.insert(
        "schema".to_owned(),
        Value::String("clinker.candidate-evidence/v1".to_owned()),
    );
    candidate.insert("kind".to_owned(), Value::String("candidate".to_owned()));
    candidate.insert(
        "state".to_owned(),
        Value::String("candidate-verified".to_owned()),
    );
    candidate.insert("revision".to_owned(), Value::from(0_u64));
    candidate.insert(
        "release_status".to_owned(),
        Value::String("incomplete".to_owned()),
    );
    candidate.insert("completion_eligible".to_owned(), Value::Bool(false));
    candidate.insert(
        "immutable_authority_sha256".to_owned(),
        Value::String(authorization_digest.to_owned()),
    );
    candidate.insert(
        "candidate_authorization_sha256".to_owned(),
        Value::String(authorization_digest.to_owned()),
    );
    for field in AUTHORIZATION_IDENTITY_FIELDS {
        candidate.insert(
            field.to_owned(),
            authority
                .get(field)
                .cloned()
                .ok_or_else(|| policy(format!("candidate authority is missing {field}")))?,
        );
    }
    let release = observation
        .release
        .as_object()
        .ok_or_else(|| policy("release readback must be an object"))?;
    let (_, metadata_text) =
        split_release_body(string_field(release, "body", "release readback")?)?;
    let metadata = metadata_value(metadata_text)?;
    let build_run_id = metadata
        .get("build_run_id")
        .cloned()
        .ok_or_else(|| policy("release metadata is missing build_run_id"))?;
    let build_run_id_text = build_run_id
        .as_str()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| policy("release metadata build_run_id must be a non-empty string"))?;
    let ci_run_ref = format!("https://github.com/{repository}/actions/runs/{build_run_id_text}");
    candidate.insert(
        "build_workflow_sha".to_owned(),
        metadata
            .get("build_workflow_sha")
            .cloned()
            .ok_or_else(|| policy("release metadata is missing build_workflow_sha"))?,
    );
    candidate.insert(
        "candidate_release_id".to_owned(),
        release
            .get("id")
            .cloned()
            .ok_or_else(|| policy("release readback is missing id"))?,
    );
    candidate.insert(
        "checksum_sha256".to_owned(),
        Value::String(observation.checksum_sha256.to_owned()),
    );
    candidate.insert(
        "archive_digests".to_owned(),
        Value::Object(observation.archive_digests.clone()),
    );
    candidate.insert("ci_run_ref".to_owned(), Value::String(ci_run_ref.clone()));
    candidate.insert(
        "build_workflow_path".to_owned(),
        Value::String(BUILD_WORKFLOW.to_owned()),
    );
    candidate.insert("build_run_id".to_owned(), build_run_id);
    candidate.insert(
        "build_head_sha".to_owned(),
        authority
            .get("source_sha")
            .cloned()
            .ok_or_else(|| policy("candidate authority is missing source_sha"))?,
    );
    candidate.insert(
        "publish_workflow_path".to_owned(),
        Value::String(PUBLISH_WORKFLOW.to_owned()),
    );

    let digests = observation.archive_digests;
    let mut archives = Vec::new();
    let mut attestations = Vec::new();
    let mut targets = inventory.targets.iter().collect::<Vec<_>>();
    targets.sort_by(|left, right| left.target.cmp(&right.target));
    for target in targets {
        let digest = digests
            .get(&target.target)
            .cloned()
            .ok_or_else(|| policy("candidate target digest is absent"))?;
        archives.push(
            json!({"archive_name": target.archive_name, "sha256": digest, "target": target.target}),
        );
        attestations.push(json!({
            "archive_name": target.archive_name,
            "ref": format!("refs/tags/{}", authority_string(authority, "candidate_tag")?),
            "repository": repository,
            "runner_environment": "github-hosted",
            "source_sha": authority.get("source_sha").cloned().ok_or_else(|| policy("candidate authority is missing source_sha"))?,
            "subject_sha256": digests.get(&target.target).cloned().ok_or_else(|| policy("candidate target digest is absent"))?,
            "workflow": BUILD_WORKFLOW,
        }));
        let _ = directory;
    }
    attestations.sort_by(|left, right| {
        left.get("archive_name")
            .and_then(Value::as_str)
            .cmp(&right.get("archive_name").and_then(Value::as_str))
    });
    candidate.insert("archives".to_owned(), Value::Array(archives));
    candidate.insert("attestations".to_owned(), Value::Array(attestations));
    let assets = local_release_assets(inventory, directory)?
        .into_iter()
        .map(|(name, asset)| json!({"name": name, "length": asset.length, "sha256": asset.sha256}))
        .collect();
    candidate.insert("assets".to_owned(), Value::Array(assets));
    candidate.insert("tag_mutation_performed".to_owned(), Value::Bool(false));
    candidate.insert(
        "tag_readback_ref".to_owned(),
        Value::String(
            string_field(
                observation
                    .tag
                    .as_object()
                    .ok_or_else(|| policy("tag readback must be an object"))?,
                "url",
                "tag readback",
            )?
            .to_owned(),
        ),
    );
    candidate.insert(
        "release_trigger_event_ref".to_owned(),
        Value::String(ci_run_ref),
    );
    Ok(Value::Object(candidate))
}

fn read_json(path: &Path, label: &str) -> Result<Value, GateError> {
    let bytes = read_bounded(path, "read release authority JSON", MAX_INPUT_BYTES)?;
    canonical::parse_json(&bytes)?;
    serde_json::from_slice(&bytes).map_err(|_| policy(format!("{label} is malformed")))
}

fn authority(value: &Value) -> Result<&Map<String, Value>, GateError> {
    value
        .as_object()
        .and_then(|object| object.get("authorization"))
        .and_then(Value::as_object)
        .ok_or_else(|| policy("candidate authorization payload is absent"))
}

fn authority_string<'a>(
    authority: &'a Map<String, Value>,
    field: &str,
) -> Result<&'a str, GateError> {
    string_field(authority, field, "candidate authority")
}

fn string_field<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    label: &str,
) -> Result<&'a str, GateError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| policy(format!("{label}.{field} must be a non-empty string")))
}

fn exact_object<'a>(
    value: &'a Value,
    fields: &[&str],
    label: &str,
) -> Result<&'a Map<String, Value>, GateError> {
    let object = value
        .as_object()
        .ok_or_else(|| policy(format!("{label} must be an object")))?;
    let expected = fields.iter().copied().collect::<BTreeSet<_>>();
    let observed = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if observed != expected {
        return Err(policy(format!(
            "{label} fields do not match the v1 contract"
        )));
    }
    Ok(object)
}

fn contained_directory(root: &Path, path: &Path) -> Result<PathBuf, GateError> {
    let path = fs::canonicalize(resolve(root, path))
        .map_err(|error| GateError::io("resolve candidate decision directory", &error))?;
    if !path.starts_with(root) || !path.is_dir() {
        return Err(policy(
            "candidate decision directory is not a contained directory",
        ));
    }
    Ok(path)
}

fn contained_file(root: &Path, path: &Path) -> Result<PathBuf, GateError> {
    let requested = resolve(root, path);
    let path = fs::canonicalize(&requested)
        .map_err(|error| GateError::io("resolve candidate authority input", &error))?;
    let metadata = fs::symlink_metadata(&requested)
        .map_err(|error| GateError::io("inspect candidate authority input", &error))?;
    if !path.starts_with(root) || metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(policy(
            "candidate authority input must be a contained regular non-symlink file",
        ));
    }
    Ok(path)
}

fn resolve(root: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("release.candidate", detail)
}
