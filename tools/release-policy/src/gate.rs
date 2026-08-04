//! Staged release eligibility aggregation and sole completion reconciliation.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::canonical;
use crate::child::{self, ChildSpec, Termination};
use crate::decision::{self, DecisionRequest};
use crate::digest;
use crate::error::GateError;
use crate::evidence;
use crate::filesystem::{self, NFS_PROFILE, SMB_PROFILE};
use crate::limits::{MAX_CHILD_OUTPUT_BYTES, MAX_INPUT_BYTES, read_bounded};

const PRE_CANDIDATE_SCHEMA: &str = "clinker.pre-candidate-evidence/v1";
const FINAL_SCHEMA: &str = "clinker.final-evidence/v1";
const CANDIDATE_SCHEMA: &str = "clinker.candidate-evidence/v1";
const PUBLICATION_SCHEMA: &str = "clinker.publication-evidence/v1";
const NOFILE_FLOOR: u64 = 65_536;
const NFS_EVIDENCE_PATH: &str = "target/release-policy/filesystem-linux-nfsv4.1-loopback-ci.json";
const SMB_EVIDENCE_PATH: &str = "target/release-policy/filesystem-linux-smb3.1.1-loopback-ci.json";
const TARGETS: [&str; 4] = [
    "aarch64-apple-darwin",
    "x86_64-apple-darwin",
    "x86_64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
];

/// Exact pre-candidate input shape.
#[derive(Debug, Clone)]
pub struct PreCandidateRequest {
    pub command_deadline_seconds: u64,
    pub repository_controls_evidence: PathBuf,
    pub evidence_manifest: PathBuf,
}

/// Exact final reconciliation input shape.
#[derive(Debug, Clone)]
pub struct FinalRequest {
    pub authorization_record: PathBuf,
    pub authorization_schema: PathBuf,
    pub decision_record: PathBuf,
    pub decision_schema: PathBuf,
    pub pre_candidate_manifest: PathBuf,
    pub candidate_evidence: PathBuf,
    pub publication_evidence: PathBuf,
    pub evidence_manifest: PathBuf,
}

#[derive(Debug, Serialize)]
struct CheckObservation {
    name: &'static str,
    status: &'static str,
    exit_code: Option<i32>,
    timed_out: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    descriptor_limit: Option<DescriptorLimitObservation>,
}

/// Raise-only file-descriptor observation attached only to workspace tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DescriptorLimitObservation {
    pub floor: u64,
    pub pre_soft: u64,
    pub pre_hard: u64,
    pub post_soft: u64,
    pub post_hard: u64,
    pub disposition: &'static str,
}

#[derive(Debug, Serialize)]
struct FailureObservation {
    check: &'static str,
    reason: &'static str,
}

struct CommandCheck {
    name: &'static str,
    program: &'static str,
    arguments: &'static [&'static str],
}

const COMMAND_CHECKS: &[CommandCheck] = &[
    CommandCheck {
        name: "dependency-boundary",
        program: "cargo",
        arguments: &[
            "run",
            "--quiet",
            "--manifest-path",
            "tools/dependency-policy/Cargo.toml",
            "--target-dir",
            "target/clinker-release-policy-pre-candidate-dependency",
            "--locked",
            "--offline",
            "--",
            "--scope",
            "final",
            "--root",
            ".",
        ],
    },
    CommandCheck {
        name: "workflow-trust",
        program: "cargo",
        arguments: &[
            "run",
            "--quiet",
            "--manifest-path",
            "tools/release-policy/Cargo.toml",
            "--locked",
            "--offline",
            "--",
            "workflow",
            "verify",
        ],
    },
    CommandCheck {
        name: "repository-configuration",
        program: "cargo",
        arguments: &[
            "run",
            "--quiet",
            "--manifest-path",
            "tools/release-policy/Cargo.toml",
            "--locked",
            "--offline",
            "--",
            "repository",
            "verify",
            "--config-only",
        ],
    },
    CommandCheck {
        name: "release-inventory",
        program: "cargo",
        arguments: &[
            "run",
            "--quiet",
            "--manifest-path",
            "tools/release-policy/Cargo.toml",
            "--locked",
            "--offline",
            "--",
            "inventory",
            "check",
        ],
    },
    CommandCheck {
        name: "decision-record-contract",
        program: "cargo",
        arguments: &[
            "test",
            "--manifest-path",
            "tools/release-policy/Cargo.toml",
            "--locked",
            "--offline",
            "--test",
            "decision_contract",
        ],
    },
    CommandCheck {
        name: "release-bundle-contract",
        program: "cargo",
        arguments: &[
            "test",
            "--manifest-path",
            "tools/release-policy/Cargo.toml",
            "--locked",
            "--offline",
            "--test",
            "release_contract",
        ],
    },
    CommandCheck {
        name: "filesystem-matrix-topology",
        program: "cargo",
        arguments: &[
            "run",
            "--quiet",
            "--manifest-path",
            "tools/release-policy/Cargo.toml",
            "--locked",
            "--offline",
            "--",
            "filesystem",
            "self-test",
        ],
    },
    CommandCheck {
        name: "native-output-containment",
        program: "cargo",
        arguments: &[
            "test",
            "--locked",
            "-p",
            "clinker",
            "--test",
            "output_containment",
        ],
    },
    CommandCheck {
        name: "workspace-format",
        program: "cargo",
        arguments: &["fmt", "--all", "--", "--check"],
    },
    CommandCheck {
        name: "workspace-check-offline",
        program: "cargo",
        arguments: &["check", "--workspace", "--locked", "--offline"],
    },
    CommandCheck {
        name: "workspace-clippy-offline",
        program: "cargo",
        arguments: &[
            "clippy",
            "--workspace",
            "--locked",
            "--offline",
            "--",
            "-D",
            "warnings",
        ],
    },
    CommandCheck {
        name: "workspace-clippy-all-targets-offline",
        program: "cargo",
        arguments: &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--locked",
            "--offline",
            "--",
            "-D",
            "warnings",
        ],
    },
    CommandCheck {
        name: "workspace-test-offline",
        program: "cargo",
        arguments: &["test", "--workspace", "--locked", "--offline"],
    },
    CommandCheck {
        name: "workspace-clippy-ci",
        program: "cargo",
        arguments: &["clippy", "--workspace", "--", "-D", "warnings"],
    },
    CommandCheck {
        name: "workspace-clippy-all-targets-ci",
        program: "cargo",
        arguments: &[
            "clippy",
            "--workspace",
            "--all-targets",
            "--",
            "-D",
            "warnings",
        ],
    },
    CommandCheck {
        name: "workspace-test-ci",
        program: "cargo",
        arguments: &["test", "--workspace"],
    },
    CommandCheck {
        name: "cargo-deny",
        program: "cargo",
        arguments: &["deny", "--locked", "check"],
    },
    CommandCheck {
        name: "documentation-contract",
        program: "bash",
        arguments: &["scripts/check-ai-docs.sh"],
    },
    CommandCheck {
        name: "diff-hygiene",
        program: "git",
        arguments: &["diff", "--check"],
    },
];

/// Run all independent safe pre-candidate checks and always persist incomplete evidence.
pub fn run_pre_candidate(request: &PreCandidateRequest) -> Result<(), GateError> {
    if request.command_deadline_seconds != 3600 {
        return Err(GateError::usage(
            "--rust-command-deadline-seconds must equal 3600",
        ));
    }

    let mut checks = Vec::with_capacity(COMMAND_CHECKS.len() + 5);
    let mut failures = Vec::new();
    let repository_controls = check_repository_controls(
        &request.repository_controls_evidence,
        &mut checks,
        &mut failures,
    );
    let repository_revision =
        check_revision(request.command_deadline_seconds, &mut checks, &mut failures);
    let filesystem_evidence_sha256 = check_filesystem_evidence(
        &repository_controls.repository,
        &repository_revision,
        &mut checks,
        &mut failures,
    );
    for check in COMMAND_CHECKS {
        run_command_check(
            check,
            request.command_deadline_seconds,
            &mut checks,
            &mut failures,
        );
    }
    let manifest = json!({
        "schema": PRE_CANDIDATE_SCHEMA,
        "stage": "pre-candidate",
        "revision": 0,
        "release_status": "incomplete",
        "completion_eligible": false,
        "repository_revision": repository_revision,
        "repository_controls_sha256": repository_controls.sha256,
        "filesystem_evidence_sha256": filesystem_evidence_sha256,
        "command_deadline_seconds": request.command_deadline_seconds,
        "requirements": ["DIST-01", "DIST-02", "ORCH-01", "ORCH-02", "ORCH-03", "ORCH-04", "SECU-01", "SECU-02", "SECU-03", "SECU-04"],
        "checks": checks,
        "failures": failures,
    });
    create_manifest(&request.evidence_manifest, &manifest)?;
    if manifest["failures"]
        .as_array()
        .is_some_and(|items| !items.is_empty())
    {
        let detail = failures
            .iter()
            .find(|failure| failure.check.starts_with("filesystem-"))
            .map_or(
                "one or more pre-candidate checks failed; incomplete evidence was retained",
                |failure| failure.reason,
            );
        return Err(GateError::policy("gate.pre_candidate_failed", detail));
    }
    Ok(())
}

struct RepositoryControlsObservation {
    repository: String,
    sha256: String,
}

fn check_repository_controls(
    path: &Path,
    checks: &mut Vec<CheckObservation>,
    failures: &mut Vec<FailureObservation>,
) -> RepositoryControlsObservation {
    let result = read_json(path, "read repository controls evidence").and_then(|value| {
        let object = object(&value, "repository controls evidence")?;
        expect_string(
            object,
            "schema",
            "clinker.repository-controls-evidence/v1",
            "repository controls evidence",
        )?;
        expect_string(
            object,
            "release_status",
            "incomplete",
            "repository controls evidence",
        )?;
        expect_bool(
            object,
            "completion_eligible",
            false,
            "repository controls evidence",
        )?;
        object
            .get("readback")
            .and_then(Value::as_object)
            .ok_or_else(|| policy("repository controls evidence readback must be an object"))?;
        let repository = field_string(object, "repository", "repository controls evidence")?;
        Ok(RepositoryControlsObservation {
            repository: repository.to_owned(),
            sha256: digest::sha256_hex(&read_bounded(
                path,
                "hash repository controls evidence",
                MAX_INPUT_BYTES,
            )?),
        })
    });
    match result {
        Ok(value) => {
            checks.push(passed("repository-controls-evidence"));
            value
        }
        Err(_) => {
            checks.push(failed(
                "repository-controls-evidence",
                None,
                false,
                false,
                false,
            ));
            failures.push(FailureObservation {
                check: "repository-controls-evidence",
                reason: "invalid-or-missing",
            });
            RepositoryControlsObservation {
                repository: String::new(),
                sha256: String::new(),
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FilesystemCiIdentity {
    repository: String,
    repository_revision: String,
    workflow_path: String,
    workflow_ref: String,
    run_id: String,
    run_attempt: u64,
    job: String,
}

struct FilesystemEvidenceObservation {
    identity: FilesystemCiIdentity,
    sha256: String,
}

fn check_filesystem_evidence(
    repository: &str,
    revision: &str,
    checks: &mut Vec<CheckObservation>,
    failures: &mut Vec<FailureObservation>,
) -> BTreeMap<String, String> {
    let mut observations = Vec::with_capacity(2);
    for (name, profile, path, reason) in [
        (
            "filesystem-nfs-evidence",
            NFS_PROFILE,
            NFS_EVIDENCE_PATH,
            "missing-or-invalid: place the exact NFS qualification at target/release-policy/filesystem-linux-nfsv4.1-loopback-ci.json",
        ),
        (
            "filesystem-smb-evidence",
            SMB_PROFILE,
            SMB_EVIDENCE_PATH,
            "missing-or-invalid: place the exact SMB qualification at target/release-policy/filesystem-linux-smb3.1.1-loopback-ci.json",
        ),
    ] {
        match read_filesystem_evidence(Path::new(path), profile, repository, revision) {
            Ok(observation) => {
                checks.push(passed(name));
                observations.push((profile, observation));
            }
            Err(_) => {
                checks.push(failed(name, None, false, false, false));
                failures.push(FailureObservation {
                    check: name,
                    reason,
                });
            }
        }
    }

    if observations.len() == 2 && observations[0].1.identity == observations[1].1.identity {
        checks.push(passed("filesystem-evidence-coherence"));
    } else {
        checks.push(failed(
            "filesystem-evidence-coherence",
            None,
            false,
            false,
            false,
        ));
        failures.push(FailureObservation {
            check: "filesystem-evidence-coherence",
            reason: "both exact filesystem profiles must come from one governed CI run and current repository revision",
        });
    }

    observations
        .into_iter()
        .map(|(profile, observation)| (profile.to_owned(), observation.sha256))
        .collect()
}

fn read_filesystem_evidence(
    path: &Path,
    expected_profile: &str,
    expected_repository: &str,
    expected_revision: &str,
) -> Result<FilesystemEvidenceObservation, GateError> {
    let evidence = filesystem::read_passing_qualification(path)?;
    let object = object(&evidence, "filesystem qualification evidence")?;
    expect_string(
        object,
        "profile",
        expected_profile,
        "filesystem qualification evidence",
    )?;
    let identity = object
        .get("ci_identity")
        .and_then(Value::as_object)
        .ok_or_else(|| policy("filesystem qualification CI identity must be an object"))?;
    let identity = FilesystemCiIdentity {
        repository: field_string(identity, "repository", "filesystem CI identity")?.to_owned(),
        repository_revision: field_string(
            identity,
            "repository_revision",
            "filesystem CI identity",
        )?
        .to_owned(),
        workflow_path: field_string(identity, "workflow_path", "filesystem CI identity")?
            .to_owned(),
        workflow_ref: field_string(identity, "workflow_ref", "filesystem CI identity")?.to_owned(),
        run_id: field_string(identity, "run_id", "filesystem CI identity")?.to_owned(),
        run_attempt: identity
            .get("run_attempt")
            .and_then(Value::as_u64)
            .ok_or_else(|| policy("filesystem CI run attempt must be an integer"))?,
        job: field_string(identity, "job", "filesystem CI identity")?.to_owned(),
    };
    if identity.repository != expected_repository
        || identity.repository_revision != expected_revision
    {
        return Err(policy(
            "filesystem qualification is not bound to the repository controls and current revision",
        ));
    }
    let encoded = serde_json::to_vec(&evidence).map_err(|_| {
        GateError::internal(
            "gate.filesystem_json",
            "filesystem evidence serialization failed",
        )
    })?;
    let canonical = canonical::parse_json(&encoded)?;
    let sha256 = digest::sha256_hex(&canonical::to_bytes(&canonical)?);
    Ok(FilesystemEvidenceObservation { identity, sha256 })
}

fn check_revision(
    deadline: u64,
    checks: &mut Vec<CheckObservation>,
    failures: &mut Vec<FailureObservation>,
) -> String {
    let result = run_child("git", &["rev-parse", "HEAD"], deadline);
    match result {
        Ok(result) if command_passed(&result) => {
            let revision = String::from_utf8_lossy(&result.stdout).trim().to_owned();
            if is_hex(&revision, 40) {
                checks.push(passed("source-revision"));
                return revision;
            }
            checks.push(failed("source-revision", Some(0), false, false, false));
        }
        Ok(result) => checks.push(observation("source-revision", &result)),
        Err(_) => checks.push(failed("source-revision", None, false, false, false)),
    }
    failures.push(FailureObservation {
        check: "source-revision",
        reason: "unavailable-or-invalid",
    });
    String::new()
}

fn run_command_check(
    check: &CommandCheck,
    deadline: u64,
    checks: &mut Vec<CheckObservation>,
    failures: &mut Vec<FailureObservation>,
) {
    let descriptor_limit = match apply_nofile_floor(matches!(
        check.name,
        "workspace-test-offline" | "workspace-test-ci"
    )) {
        Ok(observation) => observation,
        Err(error) => {
            checks.push(failed(check.name, None, false, false, false));
            failures.push(FailureObservation {
                check: check.name,
                reason: descriptor_failure_reason(&error),
            });
            return;
        }
    };
    match run_child(check.program, check.arguments, deadline) {
        Ok(result) if command_passed(&result) => {
            checks.push(passed_with_descriptor(check.name, descriptor_limit))
        }
        Ok(result) => {
            checks.push(observation_with_descriptor(
                check.name,
                &result,
                descriptor_limit,
            ));
            failures.push(FailureObservation {
                check: check.name,
                reason: "command-failed",
            });
        }
        Err(_) => {
            let mut observation = failed(check.name, None, false, false, false);
            observation.descriptor_limit = descriptor_limit;
            checks.push(observation);
            failures.push(FailureObservation {
                check: check.name,
                reason: "command-unavailable",
            });
        }
    }
}

fn run_child(
    program: &str,
    arguments: &[&str],
    deadline: u64,
) -> Result<child::ChildResult, GateError> {
    run_owned_child(
        program,
        arguments.iter().map(OsString::from).collect(),
        deadline,
    )
}

fn run_owned_child(
    program: &str,
    arguments: Vec<OsString>,
    deadline: u64,
) -> Result<child::ChildResult, GateError> {
    let mut environment = BTreeMap::new();
    for name in [
        "PATH",
        "CI",
        "CARGO_BUILD_JOBS",
        "CARGO_INCREMENTAL",
        "CARGO_TARGET_DIR",
        "NO_COLOR",
        "TMPDIR",
    ] {
        if let Some(value) = std::env::var_os(name) {
            environment.insert(OsString::from(name), value);
        }
    }
    child::run(ChildSpec {
        program: PathBuf::from(program),
        arguments,
        environment,
        timeout: Duration::from_secs(deadline),
        output_limit: MAX_CHILD_OUTPUT_BYTES,
    })
}

fn command_passed(result: &child::ChildResult) -> bool {
    matches!(result.termination, Termination::Exited(Some(0)))
        && !result.stdout_truncated
        && !result.stderr_truncated
}

fn passed(name: &'static str) -> CheckObservation {
    passed_with_descriptor(name, None)
}

fn passed_with_descriptor(
    name: &'static str,
    descriptor_limit: Option<DescriptorLimitObservation>,
) -> CheckObservation {
    CheckObservation {
        name,
        status: "passed",
        exit_code: Some(0),
        timed_out: false,
        stdout_truncated: false,
        stderr_truncated: false,
        descriptor_limit,
    }
}

fn observation(name: &'static str, result: &child::ChildResult) -> CheckObservation {
    observation_with_descriptor(name, result, None)
}

fn observation_with_descriptor(
    name: &'static str,
    result: &child::ChildResult,
    descriptor_limit: Option<DescriptorLimitObservation>,
) -> CheckObservation {
    let (exit_code, timed_out) = match result.termination {
        Termination::Exited(code) => (code, false),
        Termination::TimedOut => (None, true),
    };
    let mut observation = failed(
        name,
        exit_code,
        timed_out,
        result.stdout_truncated,
        result.stderr_truncated,
    );
    observation.descriptor_limit = descriptor_limit;
    observation
}

fn failed(
    name: &'static str,
    exit_code: Option<i32>,
    timed_out: bool,
    stdout_truncated: bool,
    stderr_truncated: bool,
) -> CheckObservation {
    CheckObservation {
        name,
        status: "failed",
        exit_code,
        timed_out,
        stdout_truncated,
        stderr_truncated,
        descriptor_limit: None,
    }
}

fn descriptor_failure_reason(error: &GateError) -> &'static str {
    match error {
        GateError::Policy {
            code: "gate.descriptor_hard_limit_insufficient",
            ..
        } => "descriptor-hard-limit-insufficient",
        _ => "descriptor-limit-unavailable",
    }
}

// `rlim_t` varies across supported Unix targets; the conversions are intentionally
// checked even though the Linux host alias is exactly `u64`.
#[allow(clippy::useless_conversion)]
fn apply_nofile_floor(required: bool) -> Result<Option<DescriptorLimitObservation>, GateError> {
    #[cfg(unix)]
    {
        use nix::sys::resource::{Resource, getrlimit, setrlimit};

        apply_nofile_floor_with(
            required,
            || {
                let (soft, hard) = getrlimit(Resource::RLIMIT_NOFILE)
                    .map_err(|_| "getrlimit unavailable".to_owned())?;
                Ok((
                    u64::try_from(soft).map_err(|_| "soft limit unsupported".to_owned())?,
                    u64::try_from(hard).map_err(|_| "hard limit unsupported".to_owned())?,
                ))
            },
            |soft, hard| {
                let soft = soft
                    .try_into()
                    .map_err(|_| "soft limit unsupported".to_owned())?;
                let hard = hard
                    .try_into()
                    .map_err(|_| "hard limit unsupported".to_owned())?;
                setrlimit(Resource::RLIMIT_NOFILE, soft, hard)
                    .map_err(|_| "setrlimit unavailable".to_owned())
            },
        )
    }
    #[cfg(not(unix))]
    {
        if required {
            Err(GateError::policy(
                "gate.descriptor_limit_unavailable",
                "RLIMIT_NOFILE is unavailable on this platform",
            ))
        } else {
            Ok(None)
        }
    }
}

/// Model the raise-only descriptor prelude with injected limit operations.
#[doc(hidden)]
pub fn apply_nofile_floor_with(
    required: bool,
    mut read_limits: impl FnMut() -> Result<(u64, u64), String>,
    mut set_limits: impl FnMut(u64, u64) -> Result<(), String>,
) -> Result<Option<DescriptorLimitObservation>, GateError> {
    if !required {
        return Ok(None);
    }
    let (pre_soft, pre_hard) = read_limits().map_err(|_| {
        GateError::policy(
            "gate.descriptor_limit_unavailable",
            "could not observe the RLIMIT_NOFILE soft and hard limits",
        )
    })?;
    if pre_hard < NOFILE_FLOOR {
        return Err(GateError::policy(
            "gate.descriptor_hard_limit_insufficient",
            "the RLIMIT_NOFILE hard limit is below the required 65536 floor",
        ));
    }
    if pre_soft >= NOFILE_FLOOR {
        return Ok(Some(DescriptorLimitObservation {
            floor: NOFILE_FLOOR,
            pre_soft,
            pre_hard,
            post_soft: pre_soft,
            post_hard: pre_hard,
            disposition: "already-sufficient",
        }));
    }
    set_limits(NOFILE_FLOOR, pre_hard).map_err(|_| {
        GateError::policy(
            "gate.descriptor_limit_unavailable",
            "could not raise the RLIMIT_NOFILE soft limit to 65536",
        )
    })?;
    let (post_soft, post_hard) = read_limits().map_err(|_| {
        GateError::policy(
            "gate.descriptor_limit_unavailable",
            "could not verify the raised RLIMIT_NOFILE limits",
        )
    })?;
    if post_soft < NOFILE_FLOOR || post_soft < pre_soft || post_hard != pre_hard {
        return Err(GateError::policy(
            "gate.descriptor_limit_unavailable",
            "RLIMIT_NOFILE did not preserve the raise-only soft and hard limit contract",
        ));
    }
    Ok(Some(DescriptorLimitObservation {
        floor: NOFILE_FLOOR,
        pre_soft,
        pre_hard,
        post_soft,
        post_hard,
        disposition: "raised",
    }))
}

/// Reconcile all producer evidence and create the only completion-eligible shape.
pub fn run_final(request: &FinalRequest) -> Result<(), GateError> {
    decision::validate(&DecisionRequest {
        schema: Some(request.decision_schema.clone()),
        records: vec![request.decision_record.clone()],
        authorization_schema: Some(request.authorization_schema.clone()),
        authorization_record: Some(request.authorization_record.clone()),
        candidate_evidence: None,
        require_ids: vec!["release-candidate".to_owned()],
        require_authorization_id: Some("release-candidate-authorization".to_owned()),
        require_authorized: true,
        require_complete: false,
        require_accepted: true,
    })?;

    let authorization = read_json(&request.authorization_record, "read authorization record")?;
    let decision = read_json(&request.decision_record, "read decision record")?;
    let pre = read_json(
        &request.pre_candidate_manifest,
        "read pre-candidate evidence",
    )?;
    let candidate = read_json(&request.candidate_evidence, "read candidate evidence")?;
    let publication = read_json(&request.publication_evidence, "read publication evidence")?;

    validate_pre_candidate(&pre)?;
    validate_candidate(&candidate)?;
    validate_publication(&publication, &candidate)?;

    let authority = field_string(
        object(&authorization, "authorization record")?,
        "candidate_authorization_sha256",
        "authorization record",
    )?;
    if !is_hex(authority, 64) {
        return Err(policy(
            "authorization authority digest must be lowercase SHA-256",
        ));
    }
    for (value, scope) in [
        (&decision, "decision record"),
        (&candidate, "candidate evidence"),
        (&publication, "publication evidence"),
    ] {
        expect_string(
            object(value, scope)?,
            "candidate_authorization_sha256",
            authority,
            scope,
        )?;
    }
    let candidate_object = object(&candidate, "candidate evidence")?;
    let decision_object = object(&decision, "decision record")?;
    for field in [
        "candidate_tag",
        "source_sha",
        "candidate_release_id",
        "checksum_sha256",
    ] {
        equal_field(decision_object, candidate_object, field)?;
    }
    if decision_object.get("archive_digests") != candidate_object.get("archive_digests") {
        return Err(policy(
            "decision and candidate archive_digests must match exactly",
        ));
    }
    let pre_revision = field_string(
        object(&pre, "pre-candidate evidence")?,
        "repository_revision",
        "pre-candidate evidence",
    )?;
    expect_string(
        candidate_object,
        "source_sha",
        pre_revision,
        "candidate evidence",
    )?;

    let publication_object = object(&publication, "publication evidence")?;
    let final_value = json!({
        "schema": FINAL_SCHEMA,
        "stage": "final",
        "revision": 0,
        "release_status": "complete",
        "completion_eligible": true,
        "repository_revision": pre_revision,
        "immutable_authority_sha256": field_string(candidate_object, "immutable_authority_sha256", "candidate evidence")?,
        "candidate_authorization_sha256": authority,
        "candidate_authorization_blob_sha": field_string(publication_object, "candidate_authorization_blob_sha", "publication evidence")?,
        "approval_record_blob_sha": field_string(publication_object, "approval_record_blob_sha", "publication evidence")?,
        "approval_record_sha256": field_string(publication_object, "approval_record_sha256", "publication evidence")?,
        "candidate_tag": field_string(candidate_object, "candidate_tag", "candidate evidence")?,
        "candidate_release_id": field_string(candidate_object, "candidate_release_id", "candidate evidence")?,
        "checksum_sha256": field_string(candidate_object, "checksum_sha256", "candidate evidence")?,
        "archive_digests": candidate_object.get("archive_digests").cloned().ok_or_else(|| policy("candidate archive_digests is required"))?,
        "producer_order": ["pre-candidate", "candidate", "publication-public-verified", "final-reconciliation"],
        "input_sha256s": {
            "pre_candidate": file_sha256(&request.pre_candidate_manifest, "hash pre-candidate evidence")?,
            "candidate": file_sha256(&request.candidate_evidence, "hash candidate evidence")?,
            "publication": file_sha256(&request.publication_evidence, "hash publication evidence")?,
        },
        "public_verification": publication_object.get("public_verification").cloned().ok_or_else(|| policy("publication public_verification is required"))?,
    });
    create_manifest(&request.evidence_manifest, &final_value)
}

/// Accept only the exact final completion evidence family.
pub fn assert_complete(path: &Path) -> Result<(), GateError> {
    let value = read_json(path, "read completion evidence")?;
    let object = object(&value, "completion evidence")?;
    expect_string(object, "schema", FINAL_SCHEMA, "completion evidence")?;
    expect_string(object, "stage", "final", "completion evidence")?;
    expect_u64(object, "revision", 0, "completion evidence")?;
    expect_string(object, "release_status", "complete", "completion evidence")?;
    expect_bool(object, "completion_eligible", true, "completion evidence")?;
    let order = object
        .get("producer_order")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("completion evidence producer_order must be an array"))?;
    let expected = [
        "pre-candidate",
        "candidate",
        "publication-public-verified",
        "final-reconciliation",
    ];
    if order.len() != expected.len()
        || !order
            .iter()
            .zip(expected)
            .all(|(value, expected)| value == expected)
    {
        return Err(policy("completion evidence producer_order is invalid"));
    }
    let verification = object
        .get("public_verification")
        .and_then(Value::as_object)
        .ok_or_else(|| policy("completion evidence public_verification must be an object"))?;
    expect_string(
        verification,
        "status",
        "verified",
        "completion public verification",
    )?;
    expect_bool(
        verification,
        "immutable_release",
        true,
        "completion public verification",
    )?;
    Ok(())
}

fn validate_pre_candidate(value: &Value) -> Result<(), GateError> {
    let object = object(value, "pre-candidate evidence")?;
    exact_object_fields(
        object,
        &[
            "schema",
            "stage",
            "revision",
            "release_status",
            "completion_eligible",
            "repository_revision",
            "repository_controls_sha256",
            "filesystem_evidence_sha256",
            "command_deadline_seconds",
            "requirements",
            "checks",
            "failures",
        ],
        "pre-candidate evidence",
    )?;
    expect_string(
        object,
        "schema",
        PRE_CANDIDATE_SCHEMA,
        "pre-candidate evidence",
    )?;
    expect_string(object, "stage", "pre-candidate", "pre-candidate evidence")?;
    expect_u64(object, "revision", 0, "pre-candidate evidence")?;
    require_incomplete(object, "pre-candidate evidence")?;
    let revision = field_string(object, "repository_revision", "pre-candidate evidence")?;
    if !is_hex(revision, 40) {
        return Err(policy(
            "pre-candidate repository_revision must be a lowercase commit SHA",
        ));
    }
    if !is_hex(
        field_string(
            object,
            "repository_controls_sha256",
            "pre-candidate evidence",
        )?,
        64,
    ) {
        return Err(policy(
            "pre-candidate repository controls digest must be lowercase SHA-256",
        ));
    }
    expect_u64(
        object,
        "command_deadline_seconds",
        3600,
        "pre-candidate evidence",
    )?;
    let expected_requirements = [
        "DIST-01", "DIST-02", "ORCH-01", "ORCH-02", "ORCH-03", "ORCH-04", "SECU-01", "SECU-02",
        "SECU-03", "SECU-04",
    ];
    let requirements = object
        .get("requirements")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("pre-candidate requirements must be an array"))?;
    if requirements.len() != expected_requirements.len()
        || requirements
            .iter()
            .zip(expected_requirements)
            .any(|(observed, expected)| observed != expected)
    {
        return Err(policy(
            "pre-candidate requirements must contain the exact ten release requirements once",
        ));
    }
    let filesystem_hashes = object
        .get("filesystem_evidence_sha256")
        .and_then(Value::as_object)
        .ok_or_else(|| policy("pre-candidate filesystem evidence hashes must be an object"))?;
    exact_object_fields(
        filesystem_hashes,
        &[NFS_PROFILE, SMB_PROFILE],
        "pre-candidate filesystem evidence hashes",
    )?;
    for profile in [NFS_PROFILE, SMB_PROFILE] {
        if !is_hex(
            field_string(
                filesystem_hashes,
                profile,
                "pre-candidate filesystem evidence hashes",
            )?,
            64,
        ) {
            return Err(policy(format!(
                "pre-candidate filesystem evidence hash for {profile} must be lowercase SHA-256"
            )));
        }
    }
    let checks = object
        .get("checks")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("pre-candidate checks must be an array"))?;
    let mut expected_checks = vec![
        "repository-controls-evidence",
        "source-revision",
        "filesystem-nfs-evidence",
        "filesystem-smb-evidence",
        "filesystem-evidence-coherence",
    ];
    expected_checks.extend(COMMAND_CHECKS.iter().map(|check| check.name));
    if checks.len() != expected_checks.len() {
        return Err(policy(
            "pre-candidate checks do not match the complete one-shot inventory",
        ));
    }
    for (check, expected_name) in checks.iter().zip(expected_checks) {
        validate_passing_check(check, expected_name)?;
    }
    if !object
        .get("failures")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
    {
        return Err(policy("pre-candidate failures must be empty"));
    }
    Ok(())
}

fn validate_passing_check(value: &Value, expected_name: &str) -> Result<(), GateError> {
    let check = object(value, "pre-candidate check")?;
    let workspace_test = matches!(
        expected_name,
        "workspace-test-offline" | "workspace-test-ci"
    );
    let mut expected_fields = vec![
        "name",
        "status",
        "exit_code",
        "timed_out",
        "stdout_truncated",
        "stderr_truncated",
    ];
    if workspace_test {
        expected_fields.push("descriptor_limit");
    }
    exact_object_fields(check, &expected_fields, "pre-candidate check")?;
    expect_string(check, "name", expected_name, "pre-candidate check")?;
    expect_string(check, "status", "passed", "pre-candidate check")?;
    expect_u64(check, "exit_code", 0, "pre-candidate check")?;
    for field in ["timed_out", "stdout_truncated", "stderr_truncated"] {
        expect_bool(check, field, false, "pre-candidate check")?;
    }
    if workspace_test {
        validate_descriptor_observation(
            check
                .get("descriptor_limit")
                .and_then(Value::as_object)
                .ok_or_else(|| policy("workspace test descriptor observation must be an object"))?,
        )?;
    }
    Ok(())
}

fn validate_descriptor_observation(observation: &Map<String, Value>) -> Result<(), GateError> {
    exact_object_fields(
        observation,
        &[
            "floor",
            "pre_soft",
            "pre_hard",
            "post_soft",
            "post_hard",
            "disposition",
        ],
        "workspace test descriptor observation",
    )?;
    expect_u64(
        observation,
        "floor",
        NOFILE_FLOOR,
        "workspace test descriptor observation",
    )?;
    let pre_soft = value_u64(
        observation,
        "pre_soft",
        "workspace test descriptor observation",
    )?;
    let pre_hard = value_u64(
        observation,
        "pre_hard",
        "workspace test descriptor observation",
    )?;
    let post_soft = value_u64(
        observation,
        "post_soft",
        "workspace test descriptor observation",
    )?;
    let post_hard = value_u64(
        observation,
        "post_hard",
        "workspace test descriptor observation",
    )?;
    if pre_hard < NOFILE_FLOOR
        || post_hard != pre_hard
        || post_soft < NOFILE_FLOOR
        || post_soft < pre_soft
    {
        return Err(policy(
            "workspace test descriptor observation violates the raise-only floor contract",
        ));
    }
    match field_string(
        observation,
        "disposition",
        "workspace test descriptor observation",
    )? {
        "already-sufficient" if pre_soft >= NOFILE_FLOOR && post_soft == pre_soft => Ok(()),
        "raised" if pre_soft < NOFILE_FLOOR && post_soft == NOFILE_FLOOR => Ok(()),
        _ => Err(policy(
            "workspace test descriptor disposition does not match its observations",
        )),
    }
}

fn validate_candidate(value: &Value) -> Result<(), GateError> {
    let object = object(value, "candidate evidence")?;
    expect_string(object, "schema", CANDIDATE_SCHEMA, "candidate evidence")?;
    expect_string(object, "kind", "candidate", "candidate evidence")?;
    expect_string(object, "state", "candidate-verified", "candidate evidence")?;
    expect_u64(object, "revision", 0, "candidate evidence")?;
    require_incomplete(object, "candidate evidence")?;
    for field in [
        "immutable_authority_sha256",
        "candidate_authorization_sha256",
        "checksum_sha256",
    ] {
        if !is_hex(field_string(object, field, "candidate evidence")?, 64) {
            return Err(policy(format!(
                "candidate evidence {field} must be lowercase SHA-256"
            )));
        }
    }
    if field_string(object, "immutable_authority_sha256", "candidate evidence")?
        != field_string(
            object,
            "candidate_authorization_sha256",
            "candidate evidence",
        )?
    {
        return Err(policy(
            "candidate immutable authority must equal candidate authorization",
        ));
    }
    if !is_hex(
        field_string(object, "source_sha", "candidate evidence")?,
        40,
    ) {
        return Err(policy(
            "candidate source_sha must be a lowercase commit SHA",
        ));
    }
    let digests = object
        .get("archive_digests")
        .and_then(Value::as_object)
        .ok_or_else(|| policy("candidate archive_digests must be an object"))?;
    if digests.len() != TARGETS.len()
        || TARGETS.iter().any(|target| {
            !digests
                .get(*target)
                .and_then(Value::as_str)
                .is_some_and(|digest| is_hex(digest, 64))
        })
    {
        return Err(policy(
            "candidate archive_digests must cover exactly four targets",
        ));
    }
    for field in ["archives", "attestations"] {
        if object.get(field).and_then(Value::as_array).map(Vec::len) != Some(TARGETS.len()) {
            return Err(policy(format!(
                "candidate {field} must contain four entries"
            )));
        }
    }
    Ok(())
}

fn validate_publication(value: &Value, candidate: &Value) -> Result<(), GateError> {
    let object = object(value, "publication evidence")?;
    expect_string(object, "schema", PUBLICATION_SCHEMA, "publication evidence")?;
    expect_string(object, "kind", "publication", "publication evidence")?;
    expect_string(object, "state", "public-verified", "publication evidence")?;
    expect_u64(object, "revision", 4, "publication evidence")?;
    require_incomplete(object, "publication evidence")?;
    if object.get("candidate") != Some(candidate) {
        return Err(policy(
            "publication embedded candidate must match candidate evidence exactly",
        ));
    }
    for (field, length) in [
        ("immutable_authority_sha256", 64),
        ("candidate_authorization_sha256", 64),
        ("candidate_authorization_blob_sha", 40),
        ("approval_record_blob_sha", 40),
        ("approval_record_sha256", 64),
    ] {
        if !is_hex(field_string(object, field, "publication evidence")?, length) {
            return Err(policy(format!(
                "publication evidence {field} has invalid digest"
            )));
        }
    }
    let dispatch = object
        .get("dispatch")
        .and_then(Value::as_object)
        .ok_or_else(|| policy("publication dispatch must be an object"))?;
    expect_u64(dispatch, "run_attempt", 1, "publication dispatch")?;
    expect_string(dispatch, "environment", "release", "publication dispatch")?;
    expect_nested_string(object, "inspection", "status", "completed")?;
    expect_nested_string(object, "approval", "approval_kind", "manual")?;
    expect_nested_bool(object, "approval", "automated_approval", false)?;
    expect_nested_string(object, "protected_job", "status", "success")?;
    expect_nested_string(object, "protected_job", "wait_mode", "read-only")?;
    expect_nested_string(object, "public_verification", "status", "verified")?;
    expect_nested_bool(object, "public_verification", "immutable_release", true)?;
    let public = object["public_verification"]
        .as_object()
        .ok_or_else(|| policy("publication public_verification must be an object"))?;
    expect_u64(public, "asset_count", 4, "publication public verification")?;
    Ok(())
}

fn require_incomplete(object: &Map<String, Value>, scope: &str) -> Result<(), GateError> {
    expect_string(object, "release_status", "incomplete", scope)?;
    expect_bool(object, "completion_eligible", false, scope)
}

fn create_manifest(path: &Path, value: &Value) -> Result<(), GateError> {
    let bytes = serde_json::to_vec(value)
        .map_err(|_| GateError::internal("gate.serialize", "gate evidence serialization failed"))?;
    let canonical = canonical::parse_json(&bytes)?;
    evidence::create_only(path, &canonical)?;
    Ok(())
}

fn read_json(path: &Path, operation: &'static str) -> Result<Value, GateError> {
    let bytes = read_bounded(path, operation, MAX_INPUT_BYTES)?;
    let canonical = canonical::parse_json(&bytes)?;
    let canonical_bytes = canonical::to_bytes(&canonical)?;
    serde_json::from_slice(&canonical_bytes)
        .map_err(|_| GateError::internal("gate.json", "validated JSON conversion failed"))
}

fn file_sha256(path: &Path, operation: &'static str) -> Result<String, GateError> {
    let value = canonical::parse_json(&read_bounded(path, operation, MAX_INPUT_BYTES)?)?;
    Ok(digest::sha256_hex(&canonical::to_bytes(&value)?))
}

fn object<'a>(value: &'a Value, scope: &str) -> Result<&'a Map<String, Value>, GateError> {
    value
        .as_object()
        .ok_or_else(|| policy(format!("{scope} must be an object")))
}

fn field_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    scope: &str,
) -> Result<&'a str, GateError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| policy(format!("{scope} {field} must be a string")))
}

fn value_u64(object: &Map<String, Value>, field: &str, scope: &str) -> Result<u64, GateError> {
    object
        .get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| policy(format!("{scope} {field} must be an unsigned integer")))
}

fn exact_object_fields(
    object: &Map<String, Value>,
    expected: &[&str],
    scope: &str,
) -> Result<(), GateError> {
    let expected = expected.iter().copied().collect::<BTreeSet<_>>();
    let observed = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if observed != expected {
        return Err(policy(format!(
            "{scope} fields do not match the complete contract"
        )));
    }
    Ok(())
}

fn expect_string(
    object: &Map<String, Value>,
    field: &str,
    expected: &str,
    scope: &str,
) -> Result<(), GateError> {
    if field_string(object, field, scope)? != expected {
        return Err(policy(format!("{scope} {field} must be {expected}")));
    }
    Ok(())
}

fn expect_bool(
    object: &Map<String, Value>,
    field: &str,
    expected: bool,
    scope: &str,
) -> Result<(), GateError> {
    if object.get(field).and_then(Value::as_bool) != Some(expected) {
        return Err(policy(format!("{scope} {field} must be {expected}")));
    }
    Ok(())
}

fn expect_u64(
    object: &Map<String, Value>,
    field: &str,
    expected: u64,
    scope: &str,
) -> Result<(), GateError> {
    if object.get(field).and_then(Value::as_u64) != Some(expected) {
        return Err(policy(format!("{scope} {field} must be {expected}")));
    }
    Ok(())
}

fn expect_nested_string(
    object: &Map<String, Value>,
    parent: &str,
    field: &str,
    expected: &str,
) -> Result<(), GateError> {
    let nested = object
        .get(parent)
        .and_then(Value::as_object)
        .ok_or_else(|| policy(format!("publication {parent} must be an object")))?;
    expect_string(nested, field, expected, &format!("publication {parent}"))
}

fn expect_nested_bool(
    object: &Map<String, Value>,
    parent: &str,
    field: &str,
    expected: bool,
) -> Result<(), GateError> {
    let nested = object
        .get(parent)
        .and_then(Value::as_object)
        .ok_or_else(|| policy(format!("publication {parent} must be an object")))?;
    expect_bool(nested, field, expected, &format!("publication {parent}"))
}

fn equal_field(
    left: &Map<String, Value>,
    right: &Map<String, Value>,
    field: &str,
) -> Result<(), GateError> {
    if left.get(field).is_none() || left.get(field) != right.get(field) {
        return Err(policy(format!(
            "decision and candidate {field} must match exactly"
        )));
    }
    Ok(())
}

fn is_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("gate.contract", detail)
}
