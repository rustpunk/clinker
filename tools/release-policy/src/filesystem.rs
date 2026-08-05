//! Fail-closed NFSv4.1 and SMB3.1.1 qualification state machine.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Map, Value, json};
use tempfile::Builder;

use crate::canonical;
use crate::child::{self, ChildResult, ChildSpec, Termination};
use crate::error::GateError;
use crate::limits::{MAX_CHILD_OUTPUT_BYTES, MAX_INPUT_BYTES, read_bounded};

/// Exact NFS profile eligible for qualification.
pub const NFS_PROFILE: &str = "linux-nfsv4.1-loopback-ci";
/// Exact SMB profile eligible for qualification.
pub const SMB_PROFILE: &str = "linux-smb3.1.1-loopback-ci";

const EVIDENCE_SCHEMA: &str = "clinker.filesystem-matrix-evidence/3";
const NFS_EXPORT: &str = "/etc/exports.d/clinker-ci.exports";
const CI_REPOSITORY: &str = "rustpunk/clinker";
const CI_WORKFLOW_PATH: &str = ".github/workflows/ci.yml";
const CI_JOB: &str = "filesystem-matrix";

/// Request for disposable provisioning plus semantic execution.
#[derive(Debug, Clone)]
pub struct ProvisionRequest {
    /// Exact qualified profile.
    pub profile: String,
    /// Durable final evidence destination.
    pub evidence: PathBuf,
}

/// Request for semantic qualification against an already mounted share.
#[derive(Debug, Clone)]
pub struct RunProfileRequest {
    /// Exact qualified profile.
    pub profile: String,
    /// Existing mounted share root.
    pub mount_root: PathBuf,
    /// Semantic evidence destination.
    pub evidence: PathBuf,
    /// Exact package-version observation log.
    pub package_observations: PathBuf,
    /// Negotiated-protocol observation log.
    pub protocol_observations: PathBuf,
}

/// Request for unconditional cleanup/finalization.
#[derive(Debug, Clone)]
pub struct TeardownRequest {
    /// Exact qualified profile.
    pub profile: String,
    /// Evidence whose adjacent state locates the disposable environment.
    pub evidence: PathBuf,
}

/// Request for the direct-CI topology self-test.
#[derive(Debug, Clone)]
pub struct SelfTestRequest {
    /// CI workflow to inspect structurally.
    pub workflow: PathBuf,
}

#[derive(Debug, Clone)]
struct EnvironmentState {
    profile: String,
    scratch: PathBuf,
    mount_root: PathBuf,
    samba_pid: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct MountObservation {
    source: String,
    filesystem: String,
    options: Vec<String>,
}

#[derive(Debug, Clone)]
struct CleanupResult {
    success: bool,
    observations: Vec<String>,
}

#[derive(Debug)]
struct SemanticLogs {
    publication: String,
    publication_observations: Vec<String>,
}

/// Provision, qualify, and teardown one exact disposable profile.
pub fn provision_and_run(request: &ProvisionRequest) -> Result<String, GateError> {
    require_profile(&request.profile)?;
    admit_evidence_destination(&request.evidence)?;
    write_status(
        &request.evidence,
        minimal_status(&request.profile, "incomplete", "initialization", false),
    )?;

    let mut current_step = "runner-identity";
    let result = (|| {
        require_hosted_runner()?;
        let runner_temp = canonical_directory_from_env("RUNNER_TEMP")?;
        current_step = "workspace-create";
        let temporary = Builder::new()
            .prefix("clinker-filesystem-matrix.")
            .tempdir_in(&runner_temp)
            .map_err(|error| GateError::io("create filesystem matrix workspace", &error))?;
        let scratch = temporary.keep();
        let mount_root = scratch.join("mount");
        let server_root = scratch.join("server");
        let samba_dir = scratch.join("samba");
        fs::create_dir_all(&mount_root)
            .and_then(|()| fs::create_dir_all(&server_root))
            .and_then(|()| fs::create_dir_all(&samba_dir))
            .map_err(|error| GateError::io("create filesystem matrix directories", &error))?;
        let state = EnvironmentState {
            profile: request.profile.clone(),
            scratch,
            mount_root,
            samba_pid: (request.profile == SMB_PROFILE).then(|| samba_dir.join("smbd.pid")),
        };
        write_state(&state_path(&request.evidence), &state)?;

        current_step = "capacity-backing";
        provision_bounded_backing(&server_root)?;

        current_step = "package-install";
        let log_dir = log_directory(&request.evidence);
        fs::create_dir_all(&log_dir)
            .map_err(|error| GateError::io("create filesystem evidence log directory", &error))?;
        let package_path = log_dir.join("packages.txt");
        let protocol_path = log_dir.join("protocol.txt");
        provision_packages(&state.profile, &package_path)?;

        current_step = "server-and-mount";
        if state.profile == NFS_PROFILE {
            provision_nfs(&state, &server_root, &protocol_path)?;
        } else {
            provision_smb(&state, &server_root, &samba_dir, &protocol_path)?;
        }

        current_step = "semantic-matrix";
        run_profile(&RunProfileRequest {
            profile: state.profile.clone(),
            mount_root: state.mount_root.clone(),
            evidence: request.evidence.clone(),
            package_observations: package_path,
            protocol_observations: protocol_path,
        })?;

        current_step = "teardown";
        teardown(&TeardownRequest {
            profile: request.profile.clone(),
            evidence: request.evidence.clone(),
        })?;
        Ok(())
    })();

    match result {
        Ok(()) => Ok("Filesystem profile qualified and teardown verified.\n".to_owned()),
        Err(error) => {
            let cleaned = cleanup_after_failure(&request.profile, &request.evidence);
            let _ = write_status(
                &request.evidence,
                minimal_status(&request.profile, "failed", current_step, cleaned),
            );
            Err(error)
        }
    }
}

fn provision_bounded_backing(server_root: &Path) -> Result<(), GateError> {
    checked(
        "sudo",
        &[
            OsString::from("mount"),
            OsString::from("-t"),
            OsString::from("tmpfs"),
            OsString::from("-o"),
            OsString::from("size=64m,nr_inodes=4096,mode=0777"),
            OsString::from("tmpfs"),
            server_root.as_os_str().to_owned(),
        ],
        inherited_environment(&[]),
        Duration::from_secs(30),
        "mount capacity-bounded server backing",
    )?;
    validate_bounded_backing(server_root)
}

fn validate_bounded_backing(server_root: &Path) -> Result<(), GateError> {
    let observation = checked(
        "findmnt",
        &[
            OsString::from("-T"),
            server_root.as_os_str().to_owned(),
            OsString::from("-n"),
            OsString::from("-o"),
            OsString::from("FSTYPE,OPTIONS"),
        ],
        inherited_environment(&[]),
        Duration::from_secs(15),
        "observe capacity-bounded server backing",
    )?;
    let observation = String::from_utf8(observation)
        .map_err(|_| missing("bounded backing observation is not UTF-8"))?;
    let options = observation.split_whitespace().collect::<Vec<_>>();
    if options.first().copied() != Some("tmpfs")
        || !observation.contains("size=65536k") && !observation.contains("size=64m")
    {
        return Err(missing(
            "server backing is not the observed 64 MiB tmpfs mount",
        ));
    }
    Ok(())
}

/// Execute the complete semantic matrix against one observed mount.
pub fn run_profile(request: &RunProfileRequest) -> Result<String, GateError> {
    require_profile(&request.profile)?;
    admit_evidence_destination(&request.evidence)?;
    write_status(
        &request.evidence,
        minimal_status(&request.profile, "incomplete", "semantic-preflight", false),
    )?;
    match run_profile_inner(request) {
        Ok(value) => {
            write_status(&request.evidence, value)?;
            Ok("Filesystem semantic matrix passed; teardown is still required.\n".to_owned())
        }
        Err(error) => {
            let _ = write_status(
                &request.evidence,
                minimal_status(&request.profile, "failed", "semantic-matrix", false),
            );
            Err(error)
        }
    }
}

fn run_profile_inner(request: &RunProfileRequest) -> Result<Value, GateError> {
    require_hosted_runner()?;
    if !request.mount_root.is_dir() {
        return Err(missing("mounted test root is unavailable"));
    }
    let mounted = observe(
        "mountpoint",
        &[
            OsString::from("-q"),
            request.mount_root.as_os_str().to_owned(),
        ],
        BTreeMap::new(),
        Duration::from_secs(15),
    )?;
    if mounted.termination != Termination::Exited(Some(0)) {
        return Err(missing("test root is not an active mount point"));
    }
    let packages = observed_lines(&request.package_observations, "read package observations")?;
    validate_packages(&request.profile, &packages)?;
    let protocols = observed_lines(&request.protocol_observations, "read protocol observations")?;
    validate_protocol(&request.profile, &protocols)?;

    let findmnt = checked(
        "findmnt",
        &[
            OsString::from("-T"),
            request.mount_root.as_os_str().to_owned(),
            OsString::from("-n"),
            OsString::from("-o"),
            OsString::from("SOURCE,FSTYPE,OPTIONS"),
        ],
        BTreeMap::new(),
        Duration::from_secs(15),
        "findmnt observation",
    )?;
    let mount_line =
        String::from_utf8(findmnt).map_err(|_| missing("mount observations are not UTF-8"))?;
    let mount = parse_mount(&mount_line)?;
    validate_mount(&request.profile, &mount)?;

    let lock = byte_range_lock_observation(&request.mount_root)?;
    let state = read_state(&state_path(&request.evidence))?;
    validate_state_paths(&state)?;
    if state.profile != request.profile || state.mount_root != request.mount_root {
        return Err(policy(
            "semantic matrix does not match the provisioned environment state",
        ));
    }
    validate_bounded_backing(&state.scratch.join("server"))?;
    let semantic = semantic_test(&state, &request.evidence)?;
    let publication_results = publication_results_from_observations(
        &semantic.publication_observations,
        &semantic.publication,
    )?;
    let admission_lock_results =
        admission_lock_results_from_observations(&semantic.publication_observations)?;
    require_cleanup_liveness(&request.mount_root)?;
    let runner = runner_observation()?;
    Ok(json!({
        "admission_lock_results": admission_lock_results,
        "ci_identity": ci_identity()?,
        "cleanup_success": false,
        "cleanup_observations": [],
        "environment_teardown": "pending",
        "injected_failures": [
            "unlisted_profile=policy_required",
            "replaced_ancestor=security_policy",
            "cross_filesystem_promotion=security_policy",
            "cancel_before_promotion=no_final",
            "child_timeout=no_passing_evidence"
        ],
        "locations": {
            "local_workspace": "repository_workspace",
            "mounted_share": "profile_mount_root",
        },
        "lock_observations": lock,
        "mount": {
            "filesystem": mount.filesystem,
            "options": mount.options,
            "source": mount.source,
        },
        "packages": packages,
        "profile": request.profile,
        "protocol_observations": protocols,
        "runner": runner,
        "capacity_results": {
            "backing": "mounted_tmpfs_64_mib",
            "edquot_seam": "seam_covered",
            "enospc_final_absent": "pass",
            "enospc_manifest_state": "staging",
            "enospc_operator_cleanup": "pass",
            "enospc_raw_os_error": 28,
            "mounted_enospc": "pass",
            "quota": "seam_covered",
        },
        "edge_outcomes": {
            "cancellation_no_final": "pass",
            "cleanup_liveness": "pass",
            "confinement": "pass",
            "cross_filesystem_no_copy": "pass",
            "rename_visibility": "pass",
            "sync_durability": "pass",
        },
        "prohibitions": [
            "copy_fallback_to_visible_final=absent",
            "publication_mode_fallback=absent",
            "cross_artifact_atomicity_claim=absent",
            "cross_execution_staging_ownership=absent",
            "raw_deletion_path_authority=absent",
        ],
        "publication_results": publication_results,
        "schema": EVIDENCE_SCHEMA,
        "status": "semantic_pass",
        "support_eligible": false,
    }))
}

fn admission_lock_results_from_observations(observations: &[String]) -> Result<Value, GateError> {
    for scenario in ["admission-count", "admission-bytes"] {
        for outcome in [
            "bounded_completion",
            "exactly_one_admitted",
            "independent_processes",
            "mounted_root_readback",
            "opposite_root_order",
        ] {
            let expected = format!("{scenario}:{outcome}=pass");
            if !observations.iter().any(|observed| observed == &expected) {
                return Err(missing(format!(
                    "validated production admission observation {expected} is absent"
                )));
            }
        }
    }
    for expected in [
        "admission-count:estimated_attempt_bytes=100,retained_limit=1",
        "admission-bytes:estimated_attempt_bytes=100,retained_limit=150",
    ] {
        if !observations.iter().any(|observed| observed == expected) {
            return Err(missing(format!(
                "validated production admission bound {expected} is absent"
            )));
        }
    }
    Ok(json!({
        "api": "RunAttemptPublication::create",
        "count_limit": {
            "bounded_completion": "pass",
            "estimated_attempt_bytes": 100,
            "exactly_one_admitted": "pass",
            "independent_processes": "pass",
            "mounted_root_readback": "pass",
            "opposite_root_order": "pass",
            "retained_attempt_limit": 1,
        },
        "lock": "fs4::FileExt::lock",
        "retained_byte_limit": {
            "bounded_completion": "pass",
            "estimated_attempt_bytes": 100,
            "exactly_one_admitted": "pass",
            "independent_processes": "pass",
            "mounted_root_readback": "pass",
            "opposite_root_order": "pass",
            "retained_byte_limit": 150,
        },
    }))
}

fn publication_results_from_observations(
    observations: &[String],
    test_log: &str,
) -> Result<Value, GateError> {
    let required = [
        "success-direct:pre_cleanup_readback=pass",
        "success-direct:post_cleanup_readback=pass",
        "success-local_then_publish:pre_cleanup_readback=pass",
        "success-local_then_publish:post_cleanup_readback=pass",
        "ordinary-failure-direct:ordinary_failure_manifest_readback=pass",
        "ordinary-failure-direct:operator_purge=pass",
        "ordinary-failure-local_then_publish:ordinary_failure_manifest_readback=pass",
        "ordinary-failure-local_then_publish:operator_purge=pass",
        "capacity-enospc:mounted_raw_errno_28=pass",
        "capacity-enospc:operator_purge=pass",
        "control_endpoint_cleanup=pass",
    ];
    for expected in required {
        if !observations.iter().any(|observed| observed == expected) {
            return Err(missing(format!(
                "validated publication observation {expected} is absent"
            )));
        }
    }
    for scenario in [
        "interruption-direct-file_synchronization",
        "interruption-direct-rename",
        "interruption-direct-parent_directory_synchronization",
        "interruption-local_then_publish-copy",
        "interruption-local_then_publish-file_synchronization",
        "interruption-local_then_publish-rename",
        "interruption-local_then_publish-parent_directory_synchronization",
    ] {
        for outcome in [
            "service_interruption",
            "bounded_interruption",
            "service_recovery",
            "recovery_manifest_readback",
            "operator_purge",
        ] {
            let expected = format!("{scenario}:{outcome}=pass");
            if !observations.iter().any(|observed| observed == &expected) {
                return Err(missing(format!(
                    "validated publication observation {expected} is absent"
                )));
            }
        }
    }

    Ok(json!({
        "lifecycle_classes": [
            "success",
            "ordinary_failure",
            "interruption",
            "ambiguity_durability_uncertainty",
            "purge_cleanup",
            "support_eligibility",
        ],
        "modes": ["direct", "local_then_publish"],
        "operator_results": [
            "list=pass",
            "inspect=pass",
            "purge_preview=pass",
            "purge_execute=pass",
            "cleanup_debt=none",
        ],
        "persistence_results": [
            "ordinary_failure=retained_manifest",
            "interruption=retained_manifest",
            "ambiguity_durability_uncertainty=retained_manifest",
        ],
        "recovery_results": [
            "direct:file_synchronization=recovered_revalidated_completed_manifest_reopened",
            "direct:rename=recovered_revalidated_completed_manifest_reopened",
            "direct:parent_directory_synchronization=recovered_revalidated_completed_manifest_reopened",
            "local_then_publish:copy=recovered_revalidated_completed_manifest_reopened",
            "local_then_publish:file_synchronization=recovered_revalidated_completed_manifest_reopened",
            "local_then_publish:rename=recovered_revalidated_completed_manifest_reopened",
            "local_then_publish:parent_directory_synchronization=recovered_revalidated_completed_manifest_reopened",
        ],
        "stage_results": [
            "direct:file_synchronization=interrupted_retained",
            "direct:rename=interrupted_retained",
            "direct:parent_directory_synchronization=interrupted_retained",
            "local_then_publish:copy=interrupted_retained",
            "local_then_publish:file_synchronization=interrupted_retained",
            "local_then_publish:rename=interrupted_retained",
            "local_then_publish:parent_directory_synchronization=interrupted_retained",
        ],
        "success_results": [
            "direct=pre_cleanup_final_and_complete_manifest,post_cleanup_final_present_attempt_absent",
            "local_then_publish=pre_cleanup_final_and_complete_manifest,post_cleanup_final_present_attempt_absent",
        ],
        "test_filter": "remote_filesystem_publication_matrix",
        "test_log": test_log,
    }))
}

/// Perform unconditional cleanup and finalize eligibility only after success.
pub fn teardown(request: &TeardownRequest) -> Result<String, GateError> {
    require_profile(&request.profile)?;
    let state_file = state_path(&request.evidence);
    if !state_file.exists() {
        let evidence = read_status(&request.evidence)?;
        if validate_passing_qualification(&evidence).is_ok() {
            return Ok("Filesystem teardown verified.\n".to_owned());
        }
        if evidence.get("cleanup_success").and_then(Value::as_bool) == Some(true)
            && evidence.get("support_eligible").and_then(Value::as_bool) == Some(false)
        {
            return Ok("Filesystem failure cleanup already verified.\n".to_owned());
        }
        return Err(missing("teardown state is absent for non-passing evidence"));
    }
    let state = read_state(&state_file)?;
    if state.profile != request.profile {
        return Err(policy(
            "teardown profile does not match durable environment state",
        ));
    }
    validate_state_paths(&state)?;
    let cleanup = cleanup_environment(&state);
    write_cleanup_log(&request.evidence, &cleanup.observations)?;
    if !cleanup.success {
        if request.evidence.exists() {
            let mut value = read_status(&request.evidence)?;
            set_field(&mut value, "cleanup_success", Value::Bool(false))?;
            set_field(
                &mut value,
                "cleanup_observations",
                Value::Array(
                    cleanup
                        .observations
                        .iter()
                        .cloned()
                        .map(Value::String)
                        .collect(),
                ),
            )?;
            set_field(
                &mut value,
                "environment_teardown",
                Value::String("failed".to_owned()),
            )?;
            let _ = write_status(&request.evidence, value);
        }
        return Err(missing("filesystem environment teardown failed"));
    }
    fs::remove_file(&state_file)
        .map_err(|error| GateError::io("remove filesystem environment state", &error))?;
    let mut evidence = read_status(&request.evidence)?;
    set_field(
        &mut evidence,
        "cleanup_observations",
        Value::Array(
            cleanup
                .observations
                .iter()
                .cloned()
                .map(Value::String)
                .collect(),
        ),
    )?;
    if status_string(&evidence, "status")? != "semantic_pass" {
        set_field(&mut evidence, "cleanup_success", Value::Bool(true))?;
        set_field(
            &mut evidence,
            "environment_teardown",
            Value::String("pass".to_owned()),
        )?;
        write_status(&request.evidence, evidence)?;
        return Err(missing("semantic success is absent after teardown"));
    }
    finalize_qualification(&mut evidence, true)?;
    write_status(&request.evidence, evidence)?;
    Ok("Filesystem teardown verified.\n".to_owned())
}

/// Validate every semantic observation and perform the sole positive transition.
#[doc(hidden)]
pub fn finalize_qualification(
    evidence: &mut Value,
    cleanup_success: bool,
) -> Result<(), GateError> {
    if !cleanup_success {
        return Err(missing(
            "successful teardown is required before support eligibility",
        ));
    }
    let object = evidence
        .as_object()
        .ok_or_else(|| policy("filesystem semantic evidence must be an object"))?;
    exact_fields(
        object,
        &[
            "admission_lock_results",
            "ci_identity",
            "capacity_results",
            "cleanup_success",
            "cleanup_observations",
            "edge_outcomes",
            "environment_teardown",
            "injected_failures",
            "locations",
            "lock_observations",
            "mount",
            "packages",
            "profile",
            "prohibitions",
            "protocol_observations",
            "publication_results",
            "runner",
            "schema",
            "status",
            "support_eligible",
        ],
        "filesystem semantic evidence",
    )?;
    if object.get("schema") != Some(&Value::String(EVIDENCE_SCHEMA.to_owned()))
        || object.get("status") != Some(&Value::String("semantic_pass".to_owned()))
        || object.get("support_eligible").and_then(Value::as_bool) != Some(false)
        || object.get("cleanup_success").and_then(Value::as_bool) != Some(false)
        || object.get("environment_teardown") != Some(&Value::String("pending".to_owned()))
    {
        return Err(missing(
            "filesystem evidence is not an unfinalized semantic pass",
        ));
    }
    let profile = object_string(object, "profile", "filesystem semantic evidence")?;
    require_profile(profile)?;
    validate_ci_identity(
        object
            .get("ci_identity")
            .and_then(Value::as_object)
            .ok_or_else(|| missing("filesystem CI identity is absent"))?,
    )?;
    let cleanup = string_array(
        object.get("cleanup_observations"),
        "filesystem cleanup observations",
    )?;
    for required in [
        "post_teardown_mount=absent",
        "post_teardown_backing_mount=absent",
        "workspace_cleanup=pass",
        "cleanup_success=true",
    ] {
        if !cleanup.iter().any(|observed| observed == required) {
            return Err(missing(format!("cleanup observation {required} is absent")));
        }
    }

    let runner = object
        .get("runner")
        .and_then(Value::as_object)
        .ok_or_else(|| missing("runner observations are absent"))?;
    exact_fields(
        runner,
        &["image_os", "image_version", "kernel", "os"],
        "runner observations",
    )?;
    for field in ["image_os", "image_version", "kernel"] {
        object_string(runner, field, "runner observations")?;
    }
    if object_string(runner, "os", "runner observations")? != "Linux" {
        return Err(missing("runner OS observation is not Linux"));
    }

    let packages = string_array(object.get("packages"), "filesystem package observations")?;
    validate_packages(profile, &packages)?;
    let protocols = string_array(
        object.get("protocol_observations"),
        "filesystem protocol observations",
    )?;
    validate_protocol(profile, &protocols)?;

    let mount = object
        .get("mount")
        .and_then(Value::as_object)
        .ok_or_else(|| missing("mount observations are absent"))?;
    exact_fields(
        mount,
        &["filesystem", "options", "source"],
        "mount observations",
    )?;
    validate_mount(
        profile,
        &MountObservation {
            source: object_string(mount, "source", "mount observations")?.to_owned(),
            filesystem: object_string(mount, "filesystem", "mount observations")?.to_owned(),
            options: string_array(mount.get("options"), "mount options")?,
        },
    )?;

    let admission = object
        .get("admission_lock_results")
        .and_then(Value::as_object)
        .ok_or_else(|| missing("production admission-lock results are absent"))?;
    exact_fields(
        admission,
        &["api", "count_limit", "lock", "retained_byte_limit"],
        "production admission-lock results",
    )?;
    if admission.get("api") != Some(&Value::String("RunAttemptPublication::create".to_owned()))
        || admission.get("lock") != Some(&Value::String("fs4::FileExt::lock".to_owned()))
    {
        return Err(missing(
            "production admission API and filesystem lock proof are absent",
        ));
    }
    for (field, limit_field, expected_limit) in [
        ("count_limit", "retained_attempt_limit", 1),
        ("retained_byte_limit", "retained_byte_limit", 150),
    ] {
        let result = admission
            .get(field)
            .and_then(Value::as_object)
            .ok_or_else(|| missing(format!("production admission result {field} is absent")))?;
        exact_fields(
            result,
            &[
                "bounded_completion",
                "estimated_attempt_bytes",
                "exactly_one_admitted",
                "independent_processes",
                "mounted_root_readback",
                "opposite_root_order",
                limit_field,
            ],
            "production admission scenario",
        )?;
        for outcome in [
            "bounded_completion",
            "exactly_one_admitted",
            "independent_processes",
            "mounted_root_readback",
            "opposite_root_order",
        ] {
            if result.get(outcome) != Some(&Value::String("pass".to_owned())) {
                return Err(missing(format!(
                    "production admission result {field}.{outcome} did not pass"
                )));
            }
        }
        if result
            .get("estimated_attempt_bytes")
            .and_then(Value::as_u64)
            != Some(100)
            || result.get(limit_field).and_then(Value::as_u64) != Some(expected_limit)
        {
            return Err(missing(format!(
                "production admission result {field} has the wrong bound"
            )));
        }
    }

    if object.get("lock_observations")
        != Some(&json!([
            "holder=acquired",
            "competitor=blocked",
            "post_release=acquired"
        ]))
    {
        return Err(missing("complete byte-range lock proof is absent"));
    }
    if object.get("injected_failures")
        != Some(&json!([
            "unlisted_profile=policy_required",
            "replaced_ancestor=security_policy",
            "cross_filesystem_promotion=security_policy",
            "cancel_before_promotion=no_final",
            "child_timeout=no_passing_evidence"
        ]))
    {
        return Err(missing("complete injected-failure proof is absent"));
    }
    let locations = object
        .get("locations")
        .and_then(Value::as_object)
        .ok_or_else(|| missing("local and mounted location observations are absent"))?;
    exact_fields(
        locations,
        &["local_workspace", "mounted_share"],
        "filesystem locations",
    )?;
    for (field, expected) in [
        ("local_workspace", "repository_workspace"),
        ("mounted_share", "profile_mount_root"),
    ] {
        if object_string(locations, field, "filesystem locations")? != expected {
            return Err(policy(
                "filesystem evidence locations must use sanitized logical labels",
            ));
        }
    }

    let capacity = object
        .get("capacity_results")
        .and_then(Value::as_object)
        .ok_or_else(|| missing("capacity results are absent"))?;
    exact_fields(
        capacity,
        &[
            "backing",
            "edquot_seam",
            "enospc_final_absent",
            "enospc_manifest_state",
            "enospc_operator_cleanup",
            "enospc_raw_os_error",
            "mounted_enospc",
            "quota",
        ],
        "filesystem capacity results",
    )?;
    for (field, expected) in [
        ("backing", "mounted_tmpfs_64_mib"),
        ("edquot_seam", "seam_covered"),
        ("enospc_final_absent", "pass"),
        ("enospc_manifest_state", "staging"),
        ("enospc_operator_cleanup", "pass"),
        ("mounted_enospc", "pass"),
        ("quota", "seam_covered"),
    ] {
        if capacity.get(field) != Some(&Value::String(expected.to_owned())) {
            return Err(missing(format!("capacity result {field} is incomplete")));
        }
    }
    if capacity.get("enospc_raw_os_error").and_then(Value::as_i64) != Some(28) {
        return Err(missing(
            "actual mounted ENOSPC raw operating-system error is absent",
        ));
    }

    let edges = object
        .get("edge_outcomes")
        .and_then(Value::as_object)
        .ok_or_else(|| missing("edge outcomes are absent"))?;
    exact_fields(
        edges,
        &[
            "cancellation_no_final",
            "cleanup_liveness",
            "confinement",
            "cross_filesystem_no_copy",
            "rename_visibility",
            "sync_durability",
        ],
        "filesystem edge outcomes",
    )?;
    for field in [
        "cancellation_no_final",
        "cleanup_liveness",
        "confinement",
        "cross_filesystem_no_copy",
        "rename_visibility",
        "sync_durability",
    ] {
        if edges.get(field) != Some(&Value::String("pass".to_owned())) {
            return Err(missing(format!("edge outcome {field} did not pass")));
        }
    }

    require_exact_string_array(
        object.get("prohibitions"),
        &[
            "copy_fallback_to_visible_final=absent",
            "publication_mode_fallback=absent",
            "cross_artifact_atomicity_claim=absent",
            "cross_execution_staging_ownership=absent",
            "raw_deletion_path_authority=absent",
        ],
        "publication prohibitions",
    )?;

    let publication = object
        .get("publication_results")
        .and_then(Value::as_object)
        .ok_or_else(|| missing("publication results are absent"))?;
    exact_fields(
        publication,
        &[
            "lifecycle_classes",
            "modes",
            "operator_results",
            "persistence_results",
            "recovery_results",
            "stage_results",
            "success_results",
            "test_filter",
            "test_log",
        ],
        "filesystem publication results",
    )?;
    require_exact_string_array(
        publication.get("lifecycle_classes"),
        &[
            "success",
            "ordinary_failure",
            "interruption",
            "ambiguity_durability_uncertainty",
            "purge_cleanup",
            "support_eligibility",
        ],
        "publication lifecycle classes",
    )?;
    require_exact_string_array(
        publication.get("modes"),
        &["direct", "local_then_publish"],
        "publication modes",
    )?;
    require_exact_string_array(
        publication.get("operator_results"),
        &[
            "list=pass",
            "inspect=pass",
            "purge_preview=pass",
            "purge_execute=pass",
            "cleanup_debt=none",
        ],
        "operator results",
    )?;
    require_exact_string_array(
        publication.get("persistence_results"),
        &[
            "ordinary_failure=retained_manifest",
            "interruption=retained_manifest",
            "ambiguity_durability_uncertainty=retained_manifest",
        ],
        "publication persistence results",
    )?;
    require_exact_string_array(
        publication.get("recovery_results"),
        &[
            "direct:file_synchronization=recovered_revalidated_completed_manifest_reopened",
            "direct:rename=recovered_revalidated_completed_manifest_reopened",
            "direct:parent_directory_synchronization=recovered_revalidated_completed_manifest_reopened",
            "local_then_publish:copy=recovered_revalidated_completed_manifest_reopened",
            "local_then_publish:file_synchronization=recovered_revalidated_completed_manifest_reopened",
            "local_then_publish:rename=recovered_revalidated_completed_manifest_reopened",
            "local_then_publish:parent_directory_synchronization=recovered_revalidated_completed_manifest_reopened",
        ],
        "publication recovery results",
    )?;
    require_exact_string_array(
        publication.get("stage_results"),
        &[
            "direct:file_synchronization=interrupted_retained",
            "direct:rename=interrupted_retained",
            "direct:parent_directory_synchronization=interrupted_retained",
            "local_then_publish:copy=interrupted_retained",
            "local_then_publish:file_synchronization=interrupted_retained",
            "local_then_publish:rename=interrupted_retained",
            "local_then_publish:parent_directory_synchronization=interrupted_retained",
        ],
        "publication stage results",
    )?;
    require_exact_string_array(
        publication.get("success_results"),
        &[
            "direct=pre_cleanup_final_and_complete_manifest,post_cleanup_final_present_attempt_absent",
            "local_then_publish=pre_cleanup_final_and_complete_manifest,post_cleanup_final_present_attempt_absent",
        ],
        "publication success results",
    )?;
    if publication.get("test_filter")
        != Some(&Value::String(
            "remote_filesystem_publication_matrix".to_owned(),
        ))
    {
        return Err(missing(
            "the named mounted publication test was not selected",
        ));
    }
    object_string(publication, "test_log", "filesystem publication results")?;

    set_field(evidence, "status", Value::String("passed".to_owned()))?;
    set_field(evidence, "support_eligible", Value::Bool(true))?;
    set_field(evidence, "cleanup_success", Value::Bool(true))?;
    set_field(
        evidence,
        "environment_teardown",
        Value::String("pass".to_owned()),
    )
}

/// Validate an already-finalized qualification without trusting positive flags alone.
#[doc(hidden)]
pub fn validate_passing_qualification(evidence: &Value) -> Result<(), GateError> {
    if status_string(evidence, "status")? != "passed"
        || evidence.get("support_eligible").and_then(Value::as_bool) != Some(true)
        || evidence.get("cleanup_success").and_then(Value::as_bool) != Some(true)
        || evidence.get("environment_teardown") != Some(&Value::String("pass".to_owned()))
    {
        return Err(missing("filesystem qualification is not finalized"));
    }
    let mut replay = evidence.clone();
    set_field(
        &mut replay,
        "status",
        Value::String("semantic_pass".to_owned()),
    )?;
    set_field(&mut replay, "support_eligible", Value::Bool(false))?;
    set_field(&mut replay, "cleanup_success", Value::Bool(false))?;
    set_field(
        &mut replay,
        "environment_teardown",
        Value::String("pending".to_owned()),
    )?;
    finalize_qualification(&mut replay, true)?;
    if &replay != evidence {
        return Err(missing("filesystem qualification replay does not match"));
    }
    Ok(())
}

/// Read one canonical regular evidence file and replay its complete passing qualification.
#[doc(hidden)]
pub fn read_passing_qualification(path: &Path) -> Result<Value, GateError> {
    let evidence = read_status(path)?;
    validate_passing_qualification(&evidence)?;
    Ok(evidence)
}

/// Validate one raw `findmnt SOURCE,FSTYPE,OPTIONS` observation.
#[doc(hidden)]
pub fn validate_mount_observation(profile: &str, line: &str) -> Result<(), GateError> {
    validate_mount(profile, &parse_mount(line)?)
}

/// Validate the exact direct-CI job and internal fail-closed invariants.
pub fn self_test(request: &SelfTestRequest) -> Result<String, GateError> {
    let workflow = read_regular(&request.workflow, "read filesystem CI workflow")?;
    let workflow = std::str::from_utf8(&workflow)
        .map_err(|_| policy("filesystem CI workflow is not UTF-8"))?;
    let workflow: Value = serde_saphyr::from_str(workflow)
        .map_err(|_| policy("filesystem CI workflow YAML is malformed"))?;
    validate_workflow(&workflow)?;

    if require_profile("unlisted-profile").is_ok() {
        return Err(policy("self-test accepted an unlisted profile"));
    }
    validate_mount(
        NFS_PROFILE,
        &MountObservation {
            source: "127.0.0.1:/".to_owned(),
            filesystem: "nfs4".to_owned(),
            options: vec![
                "rw".to_owned(),
                "vers=4.1".to_owned(),
                "proto=tcp".to_owned(),
                "hard".to_owned(),
                "local_lock=none".to_owned(),
            ],
        },
    )?;
    validate_mount(
        SMB_PROFILE,
        &MountObservation {
            source: "//127.0.0.1/clinker".to_owned(),
            filesystem: "cifs".to_owned(),
            options: vec![
                "rw".to_owned(),
                "vers=3.1.1".to_owned(),
                "cache=strict".to_owned(),
                "strictsync".to_owned(),
                "noperm".to_owned(),
            ],
        },
    )?;
    for (profile, option) in [(NFS_PROFILE, "local_lock=all"), (SMB_PROFILE, "nobrl")] {
        let mut observation = if profile == NFS_PROFILE {
            MountObservation {
                source: "127.0.0.1:/".to_owned(),
                filesystem: "nfs4".to_owned(),
                options: vec![
                    "vers=4.1".to_owned(),
                    "proto=tcp".to_owned(),
                    "hard".to_owned(),
                ],
            }
        } else {
            MountObservation {
                source: "//127.0.0.1/clinker".to_owned(),
                filesystem: "cifs".to_owned(),
                options: vec![
                    "vers=3.1.1".to_owned(),
                    "cache=strict".to_owned(),
                    "noperm".to_owned(),
                ],
            }
        };
        observation.options.push(option.to_owned());
        if validate_mount(profile, &observation).is_ok() {
            return Err(policy(
                "self-test accepted a prohibited local-only mount option",
            ));
        }
    }
    Ok("filesystem matrix self-test: PASS\n".to_owned())
}

fn provision_packages(profile: &str, destination: &Path) -> Result<(), GateError> {
    checked(
        "sudo",
        &[OsString::from("apt-get"), OsString::from("update")],
        inherited_environment(&[]),
        Duration::from_secs(600),
        "package index update",
    )?;
    let packages = if profile == NFS_PROFILE {
        ["nfs-common", "nfs-kernel-server"]
    } else {
        ["cifs-utils", "samba"]
    };
    let mut install = vec![
        OsString::from("env"),
        OsString::from("DEBIAN_FRONTEND=noninteractive"),
        OsString::from("apt-get"),
        OsString::from("install"),
        OsString::from("-y"),
    ];
    install.extend(packages.iter().map(OsString::from));
    checked(
        "sudo",
        &install,
        inherited_environment(&[]),
        Duration::from_secs(600),
        "filesystem package installation",
    )?;
    let mut query = vec![
        OsString::from("-W"),
        OsString::from("-f=${Package}=${Version}\\n"),
    ];
    query.extend(packages.iter().map(OsString::from));
    let output = checked(
        "dpkg-query",
        &query,
        inherited_environment(&[]),
        Duration::from_secs(30),
        "filesystem package observation",
    )?;
    write_observation(destination, &output)
}

fn provision_nfs(
    state: &EnvironmentState,
    server_root: &Path,
    protocol_path: &Path,
) -> Result<(), GateError> {
    let export = state.scratch.join("clinker-ci.exports");
    fs::write(
        &export,
        format!(
            "{} 127.0.0.1(rw,sync,no_subtree_check,fsid=0,no_root_squash)\n",
            server_root.display()
        ),
    )
    .map_err(|error| GateError::io("write NFS export configuration", &error))?;
    for (program, arguments, label) in [
        (
            "sudo",
            vec![
                OsString::from("install"),
                OsString::from("-D"),
                OsString::from("-m"),
                OsString::from("0644"),
                export.as_os_str().to_owned(),
                OsString::from(NFS_EXPORT),
            ],
            "install NFS export configuration",
        ),
        (
            "sudo",
            vec![
                OsString::from("systemctl"),
                OsString::from("start"),
                OsString::from("rpcbind"),
            ],
            "start rpcbind",
        ),
        (
            "sudo",
            vec![
                OsString::from("systemctl"),
                OsString::from("restart"),
                OsString::from("nfs-kernel-server"),
            ],
            "start NFS server",
        ),
        (
            "sudo",
            vec![OsString::from("exportfs"), OsString::from("-rav")],
            "activate NFS export",
        ),
        (
            "sudo",
            vec![
                OsString::from("mount"),
                OsString::from("-t"),
                OsString::from("nfs4"),
                OsString::from("-o"),
                OsString::from("vers=4.1,proto=tcp,hard,timeo=600,retrans=2"),
                OsString::from("127.0.0.1:/"),
                state.mount_root.as_os_str().to_owned(),
            ],
            "mount NFSv4.1 share",
        ),
    ] {
        checked(
            program,
            &arguments,
            inherited_environment(&[]),
            Duration::from_secs(120),
            label,
        )?;
    }
    let mut protocol = checked(
        "nfsstat",
        &[OsString::from("--version")],
        inherited_environment(&[]),
        Duration::from_secs(30),
        "observe NFS client version",
    )?;
    protocol.extend(checked(
        "nfsstat",
        &[OsString::from("-m")],
        inherited_environment(&[]),
        Duration::from_secs(30),
        "observe negotiated NFS mount",
    )?);
    write_observation(protocol_path, &protocol)
}

fn provision_smb(
    state: &EnvironmentState,
    server_root: &Path,
    samba_dir: &Path,
    protocol_path: &Path,
) -> Result<(), GateError> {
    let configuration = state.scratch.join("smb.conf");
    let username = checked(
        "id",
        &[OsString::from("-un")],
        inherited_environment(&[]),
        Duration::from_secs(15),
        "observe runner user",
    )?;
    let username =
        String::from_utf8(username).map_err(|_| missing("runner user observation is not UTF-8"))?;
    fs::write(
        &configuration,
        format!(
            "[global]\nserver min protocol = SMB3_11\nserver max protocol = SMB3_11\ninterfaces = 127.0.0.1/8 lo\nbind interfaces only = yes\nmap to guest = Bad User\nsecurity = user\npid directory = {}\n[clinker]\npath = {}\nread only = no\nguest ok = yes\nforce user = {}\n",
            samba_dir.display(),
            server_root.display(),
            username.trim()
        ),
    )
    .map_err(|error| GateError::io("write Samba configuration", &error))?;
    checked(
        "sudo",
        &[
            OsString::from("smbd"),
            OsString::from("--daemon"),
            OsString::from("--no-process-group"),
            OsString::from(format!("--configfile={}", configuration.display())),
        ],
        inherited_environment(&[]),
        Duration::from_secs(30),
        "start Samba server",
    )?;
    let pid = state
        .samba_pid
        .as_ref()
        .ok_or_else(|| GateError::internal("filesystem.samba_pid", "Samba PID path is absent"))?;
    for _ in 0..50 {
        if pid.is_file() {
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    if !pid.is_file() {
        return Err(missing("Samba server did not become ready"));
    }
    wait_for_smb_listener()?;
    let mount_arguments = [
        OsString::from("mount"),
        OsString::from("-t"),
        OsString::from("cifs"),
        OsString::from("//127.0.0.1/clinker"),
        state.mount_root.as_os_str().to_owned(),
        OsString::from("-o"),
        OsString::from("guest,vers=3.1.1,cache=strict,strictsync,mfsymlinks,noperm"),
    ];
    let mut mount = observe(
        "sudo",
        &mount_arguments,
        inherited_environment(&[]),
        Duration::from_secs(15),
    )?;
    for _ in 1..20 {
        if mount.termination == Termination::Exited(Some(0))
            && !mount.stdout_truncated
            && !mount.stderr_truncated
        {
            break;
        }
        thread::sleep(Duration::from_millis(250));
        mount = observe(
            "sudo",
            &mount_arguments,
            inherited_environment(&[]),
            Duration::from_secs(15),
        )?;
    }
    write_child_observation(&protocol_path.with_file_name("mount.txt"), &mount)?;
    if mount.termination != Termination::Exited(Some(0))
        || mount.stdout_truncated
        || mount.stderr_truncated
    {
        return Err(missing(
            "mount SMB3.1.1 share failed; inspect the bounded mount observation artifact",
        ));
    }
    let mut protocol = checked(
        "smbd",
        &[OsString::from("--version")],
        inherited_environment(&[]),
        Duration::from_secs(30),
        "observe Samba version",
    )?;
    protocol.extend(checked(
        "sudo",
        &[
            OsString::from("cat"),
            OsString::from("/proc/fs/cifs/DebugData"),
        ],
        inherited_environment(&[]),
        Duration::from_secs(30),
        "observe negotiated SMB dialect",
    )?);
    let text = String::from_utf8_lossy(&protocol).to_ascii_lowercase();
    if !text.contains("3.1.1") && !text.contains("0x311") && !text.contains("dialect 311") {
        return Err(missing("negotiated SMB3.1.1 dialect was not observable"));
    }
    write_observation(protocol_path, &protocol)
}

fn wait_for_smb_listener() -> Result<(), GateError> {
    let address = SocketAddr::from(([127, 0, 0, 1], 445));
    for _ in 0..50 {
        if TcpStream::connect_timeout(&address, Duration::from_millis(100)).is_ok() {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(100));
    }
    Err(missing("Samba TCP listener did not become ready"))
}

fn write_child_observation(path: &Path, result: &ChildResult) -> Result<(), GateError> {
    let mut bytes = format!(
        "termination={:?}\nstdout_truncated={}\nstderr_truncated={}\nstdout:\n",
        result.termination, result.stdout_truncated, result.stderr_truncated
    )
    .into_bytes();
    bytes.extend(&result.stdout);
    bytes.extend(b"\nstderr:\n");
    bytes.extend(&result.stderr);
    if !bytes.ends_with(b"\n") {
        bytes.push(b'\n');
    }
    write_observation(path, &bytes)
}

fn semantic_test(state: &EnvironmentState, evidence: &Path) -> Result<SemanticLogs, GateError> {
    let environment = inherited_environment(&[
        ("CLINKER_FILESYSTEM_PROFILE", OsString::from(&state.profile)),
        (
            "CLINKER_FILESYSTEM_ROOT",
            state.mount_root.as_os_str().to_owned(),
        ),
        ("CARGO_INCREMENTAL", OsString::from("0")),
        ("CARGO_BUILD_JOBS", OsString::from("1")),
    ]);
    let edge_result = observe(
        "cargo",
        &[
            OsString::from("test"),
            OsString::from("--locked"),
            OsString::from("-p"),
            OsString::from("clinker"),
            OsString::from("--test"),
            OsString::from("output_containment"),
            OsString::from("remote_filesystem_matrix_semantics"),
            OsString::from("--"),
            OsString::from("--nocapture"),
        ],
        environment.clone(),
        Duration::from_secs(900),
    )?;
    let edge_log = log_directory(evidence).join("edge-test.txt");
    require_semantic_success(
        &edge_log,
        &edge_result,
        "remote_filesystem_matrix_semantics",
    )?;

    let publication_log = log_directory(evidence).join("publication-test.txt");
    let control_log = log_directory(evidence).join("publication-control.txt");
    let result = controlled_publication_test(state, environment, &control_log, &publication_log)?;
    let publication = require_semantic_success(
        &publication_log,
        &result,
        "remote_filesystem_publication_matrix",
    )?;
    let publication_observations =
        observed_lines(&control_log, "read publication controller observations")?;
    Ok(SemanticLogs {
        publication,
        publication_observations,
    })
}

fn require_semantic_success(
    log_path: &Path,
    result: &ChildResult,
    test_filter: &str,
) -> Result<String, GateError> {
    // A failed qualification is evidence too. Persist the bounded child
    // observation before interpreting its exit status so CI artifacts retain
    // the exact test failure, timeout, and truncation state.
    write_child_observation(log_path, result)?;
    if result.termination != Termination::Exited(Some(0))
        || result.stdout_truncated
        || result.stderr_truncated
    {
        return Err(missing(
            "remote semantic test failed, timed out, or exceeded its output bound",
        ));
    }
    let mut output = result.stdout.clone();
    output.extend_from_slice(&result.stderr);
    let text = String::from_utf8(output)
        .map_err(|_| missing("remote semantic test output is not UTF-8"))?;
    if !text.contains(test_filter) || !text.contains("test result: ok") {
        return Err(missing(
            "remote semantic test filter selected no passing test",
        ));
    }
    Ok(log_path
        .file_name()
        .and_then(OsStr::to_str)
        .unwrap_or("semantic-test.txt")
        .to_owned())
}

fn controlled_publication_test(
    state: &EnvironmentState,
    mut environment: BTreeMap<OsString, OsString>,
    control_log: &Path,
    publication_log: &Path,
) -> Result<ChildResult, GateError> {
    let endpoint = state.scratch.join("publication-control.sock");
    if endpoint.exists() {
        fs::remove_file(&endpoint)
            .map_err(|error| GateError::io("remove stale publication control endpoint", &error))?;
    }
    let listener = UnixListener::bind(&endpoint)
        .map_err(|error| GateError::io("bind publication control endpoint", &error))?;
    listener
        .set_nonblocking(true)
        .map_err(|error| GateError::io("bound publication control acceptance", &error))?;
    environment.insert(
        OsString::from("CLINKER_FILESYSTEM_CONTROL_ENDPOINT"),
        endpoint.as_os_str().to_owned(),
    );

    let worker = thread::spawn(move || {
        observe(
            "cargo",
            &[
                OsString::from("test"),
                OsString::from("--locked"),
                OsString::from("-p"),
                OsString::from("clinker"),
                OsString::from("--test"),
                OsString::from("attempt_publication"),
                OsString::from("remote_filesystem_publication_matrix"),
                OsString::from("--"),
                OsString::from("--nocapture"),
                OsString::from("--test-threads=1"),
            ],
            environment,
            Duration::from_secs(900),
        )
    });
    let mut observations = Vec::new();
    let control = run_publication_controller(state, &listener, &mut observations);
    drop(listener);
    let endpoint_removed = fs::remove_file(&endpoint).is_ok() && !endpoint.exists();
    observations.push(format!(
        "control_endpoint_cleanup={}",
        if endpoint_removed { "pass" } else { "failed" }
    ));
    let mut log = observations.join("\n").into_bytes();
    log.push(b'\n');
    write_observation(control_log, &log)?;
    let child_result = worker
        .join()
        .map_err(|_| missing("mounted publication child thread panicked"))??;
    write_child_observation(publication_log, &child_result)?;
    control?;
    if !endpoint_removed {
        return Err(missing("publication control endpoint cleanup failed"));
    }
    Ok(child_result)
}

fn run_publication_controller(
    state: &EnvironmentState,
    listener: &UnixListener,
    observations: &mut Vec<String>,
) -> Result<(), GateError> {
    for (scenario, mode) in [
        ("success-direct", "direct"),
        ("success-local_then_publish", "local_then_publish"),
        ("ordinary-failure-direct", "direct"),
        ("ordinary-failure-local_then_publish", "local_then_publish"),
    ] {
        let mut stream = accept_control(listener)?;
        validate_scenario_begin(&mut stream, scenario, mode)?;
        if scenario.starts_with("success-") {
            control_success(state, &mut stream, scenario, mode, observations)?;
        } else {
            control_retained(state, &mut stream, scenario, mode, false, observations)?;
        }
    }

    for (mode, stages) in [
        (
            "direct",
            &[
                "file_synchronization",
                "rename",
                "parent_directory_synchronization",
            ][..],
        ),
        (
            "local_then_publish",
            &[
                "copy",
                "file_synchronization",
                "rename",
                "parent_directory_synchronization",
            ][..],
        ),
    ] {
        for target in stages {
            let scenario = format!("interruption-{mode}-{target}");
            let mut stream = accept_control(listener)?;
            validate_scenario_begin(&mut stream, &scenario, mode)?;
            control_interruption(state, &mut stream, &scenario, mode, target, observations)?;
        }
    }

    let mut stream = accept_control(listener)?;
    validate_scenario_begin(&mut stream, "capacity-enospc", "direct")?;
    control_capacity(state, &mut stream, observations)?;

    for scenario in ["admission-count", "admission-bytes"] {
        let mut stream = accept_control(listener)?;
        validate_scenario_begin(&mut stream, scenario, "direct")?;
        control_admission(&mut stream, scenario, observations)?;
    }
    Ok(())
}

fn control_admission(
    stream: &mut UnixStream,
    scenario: &str,
    observations: &mut Vec<String>,
) -> Result<(), GateError> {
    let result = read_control(stream)?;
    let object = result
        .as_object()
        .ok_or_else(|| policy("admission result must be an object"))?;
    exact_fields(
        object,
        &[
            "action",
            "bounded_completion",
            "estimated_attempt_bytes",
            "exactly_one_admitted",
            "independent_processes",
            "mounted_root_readback",
            "opposite_root_order",
            "retained_limit",
            "scenario",
            "schema",
        ],
        "admission result",
    )?;
    let expected_limit = if scenario == "admission-count" {
        1
    } else {
        150
    };
    if object.get("schema")
        != Some(&Value::String(
            "clinker.filesystem-publication-control/1".to_owned(),
        ))
        || object.get("action") != Some(&Value::String("admission_complete".to_owned()))
        || object.get("scenario") != Some(&Value::String(scenario.to_owned()))
        || object
            .get("estimated_attempt_bytes")
            .and_then(Value::as_u64)
            != Some(100)
        || object.get("retained_limit").and_then(Value::as_u64) != Some(expected_limit)
    {
        return Err(policy(
            "admission result does not match the exact scenario contract",
        ));
    }
    for field in [
        "bounded_completion",
        "exactly_one_admitted",
        "independent_processes",
        "mounted_root_readback",
        "opposite_root_order",
    ] {
        if object.get(field).and_then(Value::as_bool) != Some(true) {
            return Err(missing(format!(
                "admission result {field} did not pass for {scenario}"
            )));
        }
        observations.push(format!("{scenario}:{field}=pass"));
    }
    observations.push(format!(
        "{scenario}:estimated_attempt_bytes=100,retained_limit={expected_limit}"
    ));
    write_scenario_action(stream, "finish", scenario)
}

fn accept_control(listener: &UnixListener) -> Result<UnixStream, GateError> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        match listener.accept() {
            Ok((stream, _)) => {
                stream
                    .set_read_timeout(Some(Duration::from_secs(30)))
                    .and_then(|()| stream.set_write_timeout(Some(Duration::from_secs(30))))
                    .map_err(|error| {
                        GateError::io("bound publication control connection", &error)
                    })?;
                return Ok(stream);
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                if Instant::now() >= deadline {
                    return Err(missing("publication child did not connect before deadline"));
                }
                thread::sleep(Duration::from_millis(25));
            }
            Err(error) => {
                return Err(GateError::io(
                    "accept publication control connection",
                    &error,
                ));
            }
        }
    }
}

fn read_control(stream: &mut UnixStream) -> Result<Value, GateError> {
    let mut encoded = Vec::new();
    loop {
        if encoded.len() == 4_096 {
            return Err(policy("publication control message exceeds its byte bound"));
        }
        let mut byte = [0_u8; 1];
        stream
            .read_exact(&mut byte)
            .map_err(|error| GateError::io("read publication control message", &error))?;
        if byte[0] == b'\n' {
            break;
        }
        encoded.push(byte[0]);
    }
    serde_json::from_slice(&encoded).map_err(|_| policy("publication control message is malformed"))
}

fn write_control(stream: &mut UnixStream, value: &Value) -> Result<(), GateError> {
    serde_json::to_writer(&mut *stream, value).map_err(|_| {
        GateError::internal(
            "filesystem.control_encoding",
            "publication control encoding failed",
        )
    })?;
    stream
        .write_all(b"\n")
        .and_then(|()| stream.flush())
        .map_err(|error| GateError::io("write publication control message", &error))
}

fn validate_scenario_begin(
    stream: &mut UnixStream,
    expected_scenario: &str,
    expected_mode: &str,
) -> Result<(), GateError> {
    let message = read_control(stream)?;
    let object = message
        .as_object()
        .ok_or_else(|| policy("publication scenario begin must be an object"))?;
    exact_fields(
        object,
        &["action", "publication_mode", "scenario", "schema"],
        "publication scenario begin",
    )?;
    if object.get("schema")
        != Some(&Value::String(
            "clinker.filesystem-publication-control/1".to_owned(),
        ))
        || object.get("action") != Some(&Value::String("scenario_begin".to_owned()))
        || object.get("scenario") != Some(&Value::String(expected_scenario.to_owned()))
        || object.get("publication_mode") != Some(&Value::String(expected_mode.to_owned()))
    {
        return Err(policy(
            "publication scenario begin does not match the expected identity",
        ));
    }
    Ok(())
}

fn read_stage(
    stream: &mut UnixStream,
    expected_mode: &str,
    expected_stage: &str,
    execution_id: Option<&str>,
    artifact_id: Option<&str>,
) -> Result<(Value, String, String), GateError> {
    let message = read_control(stream)?;
    let object = message
        .as_object()
        .ok_or_else(|| policy("publication stage message must be an object"))?;
    exact_fields(
        object,
        &[
            "action",
            "artifact_id",
            "execution_id",
            "publication_mode",
            "schema",
            "stage",
        ],
        "publication stage message",
    )?;
    let observed_execution = object_string(object, "execution_id", "publication stage")?;
    let observed_artifact = object_string(object, "artifact_id", "publication stage")?;
    if object.get("schema")
        != Some(&Value::String(
            "clinker.attempt-stage-control/v1".to_owned(),
        ))
        || object.get("action") != Some(&Value::String("stage_ready".to_owned()))
        || object.get("publication_mode") != Some(&Value::String(expected_mode.to_owned()))
        || object.get("stage") != Some(&Value::String(expected_stage.to_owned()))
        || execution_id.is_some_and(|expected| expected != observed_execution)
        || artifact_id.is_some_and(|expected| expected != observed_artifact)
    {
        return Err(policy(
            "publication stage message does not match the expected identity",
        ));
    }
    let observed_execution = observed_execution.to_owned();
    let observed_artifact = observed_artifact.to_owned();
    Ok((message, observed_execution, observed_artifact))
}

fn release_stage(stream: &mut UnixStream, mut message: Value) -> Result<(), GateError> {
    set_field(&mut message, "action", Value::String("release".to_owned()))?;
    write_control(stream, &message)
}

fn write_scenario_action(
    stream: &mut UnixStream,
    action: &str,
    scenario: &str,
) -> Result<(), GateError> {
    write_control(
        stream,
        &json!({
            "action": action,
            "scenario": scenario,
            "schema": "clinker.filesystem-publication-control/1",
        }),
    )
}

fn control_success(
    state: &EnvironmentState,
    stream: &mut UnixStream,
    scenario: &str,
    mode: &str,
    observations: &mut Vec<String>,
) -> Result<(), GateError> {
    let stages = if mode == "direct" {
        &[
            "file_synchronization",
            "rename",
            "parent_directory_synchronization",
            "complete_before_cleanup",
        ][..]
    } else {
        &[
            "copy",
            "file_synchronization",
            "rename",
            "parent_directory_synchronization",
            "complete_before_cleanup",
        ][..]
    };
    let mut execution_id = None;
    let mut artifact_id = None;
    for stage in stages {
        let (message, observed_execution, observed_artifact) = read_stage(
            stream,
            mode,
            stage,
            execution_id.as_deref(),
            artifact_id.as_deref(),
        )?;
        execution_id.get_or_insert(observed_execution);
        artifact_id.get_or_insert(observed_artifact);
        if *stage == "complete_before_cleanup" {
            verify_complete_readback(
                state,
                scenario,
                execution_id
                    .as_deref()
                    .expect("stage established execution"),
                artifact_id.as_deref().expect("stage established artifact"),
            )?;
            observations.push(format!("{scenario}:pre_cleanup_readback=pass"));
        }
        release_stage(stream, message)?;
    }
    let complete = read_control(stream)?;
    validate_exact_action(&complete, "success_complete", scenario, 3)?;
    let execution_id = execution_id.expect("success stages establish execution");
    let root = publication_sandbox(state)
        .join(".clinker-attempts")
        .join(&execution_id);
    if !publication_sandbox(state)
        .join(format!("{scenario}.bin"))
        .is_file()
        || root.exists()
        || root.join("manifest.json").exists()
    {
        return Err(missing(
            "successful publication post-cleanup readback is incomplete",
        ));
    }
    observations.push(format!("{scenario}:post_cleanup_readback=pass"));
    write_scenario_action(stream, "finish", scenario)
}

fn control_retained(
    state: &EnvironmentState,
    stream: &mut UnixStream,
    scenario: &str,
    _mode: &str,
    interrupted: bool,
    observations: &mut Vec<String>,
) -> Result<(), GateError> {
    let ready = read_control(stream)?;
    let (execution_id, artifact_id, manifest_state) = validate_recovery_ready(&ready, scenario)?;
    let expectation = retained_expectation(scenario)?;
    validate_child_manifest_state(&manifest_state, &expectation)?;
    verify_retained_readback(state, scenario, &execution_id, &artifact_id, &expectation)?;
    observations.push(format!(
        "{scenario}:{}=pass",
        if interrupted {
            "recovery_manifest_readback"
        } else {
            "ordinary_failure_manifest_readback"
        }
    ));
    write_scenario_action(stream, "purge", scenario)?;
    let purged = read_control(stream)?;
    validate_exact_action(&purged, "purge_complete", scenario, 3)?;
    if publication_sandbox(state)
        .join(".clinker-attempts")
        .join(&execution_id)
        .exists()
    {
        return Err(missing("operator purge left retained attempt metadata"));
    }
    observations.push(format!("{scenario}:operator_purge=pass"));
    write_scenario_action(stream, "finish", scenario)
}

fn control_interruption(
    state: &EnvironmentState,
    stream: &mut UnixStream,
    scenario: &str,
    mode: &str,
    target_stage: &str,
    observations: &mut Vec<String>,
) -> Result<(), GateError> {
    let stages = if mode == "direct" {
        &[
            "file_synchronization",
            "rename",
            "parent_directory_synchronization",
        ][..]
    } else {
        &[
            "copy",
            "file_synchronization",
            "rename",
            "parent_directory_synchronization",
        ][..]
    };
    let mut execution_id = None;
    let mut artifact_id = None;
    for stage in stages {
        let (message, observed_execution, observed_artifact) = read_stage(
            stream,
            mode,
            stage,
            execution_id.as_deref(),
            artifact_id.as_deref(),
        )?;
        execution_id.get_or_insert(observed_execution);
        artifact_id.get_or_insert(observed_artifact);
        if *stage == target_stage {
            interrupt_profile(state)?;
            observations.push(format!("{scenario}:service_interruption=pass"));
            release_stage(stream, message)?;
            let interrupted = read_control(stream)?;
            validate_exact_action(&interrupted, "interruption_observed", scenario, 3)?;
            observations.push(format!("{scenario}:bounded_interruption=pass"));
            recover_profile(state)?;
            observations.push(format!("{scenario}:service_recovery=pass"));
            write_scenario_action(stream, "recover", scenario)?;
            return control_retained(state, stream, scenario, mode, true, observations);
        }
        release_stage(stream, message)?;
    }
    Err(missing(
        "target publication interruption stage was not emitted",
    ))
}

fn control_capacity(
    state: &EnvironmentState,
    stream: &mut UnixStream,
    observations: &mut Vec<String>,
) -> Result<(), GateError> {
    let ready = read_control(stream)?;
    let object = ready
        .as_object()
        .ok_or_else(|| policy("capacity result must be an object"))?;
    exact_fields(
        object,
        &[
            "action",
            "artifact_id",
            "enospc_raw_os_error",
            "execution_id",
            "manifest_state",
            "scenario",
            "schema",
        ],
        "capacity result",
    )?;
    if object.get("schema")
        != Some(&Value::String(
            "clinker.filesystem-publication-control/1".to_owned(),
        ))
        || object.get("action") != Some(&Value::String("capacity_ready".to_owned()))
        || object.get("scenario") != Some(&Value::String("capacity-enospc".to_owned()))
        || object.get("enospc_raw_os_error").and_then(Value::as_i64) != Some(28)
    {
        return Err(missing("actual mounted ENOSPC result is incomplete"));
    }
    let execution_id = object_string(object, "execution_id", "capacity result")?;
    let artifact_id = object_string(object, "artifact_id", "capacity result")?;
    let manifest_state = object_string(object, "manifest_state", "capacity result")?;
    if manifest_state != "staging" {
        return Err(missing("ENOSPC retained manifest is not staging"));
    }
    let expectation = retained_expectation("capacity-enospc")?;
    validate_child_manifest_state(manifest_state, &expectation)?;
    verify_retained_readback(
        state,
        "capacity-enospc",
        execution_id,
        artifact_id,
        &expectation,
    )?;
    observations.push("capacity-enospc:mounted_raw_errno_28=pass".to_owned());
    write_scenario_action(stream, "purge", "capacity-enospc")?;
    let purged = read_control(stream)?;
    validate_exact_action(&purged, "purge_complete", "capacity-enospc", 3)?;
    if publication_sandbox(state)
        .join(".clinker-attempts")
        .join(execution_id)
        .exists()
    {
        return Err(missing("ENOSPC operator cleanup left attempt metadata"));
    }
    observations.push("capacity-enospc:operator_purge=pass".to_owned());
    write_scenario_action(stream, "finish", "capacity-enospc")
}

fn validate_exact_action(
    value: &Value,
    action: &str,
    scenario: &str,
    field_count: usize,
) -> Result<(), GateError> {
    let object = value
        .as_object()
        .ok_or_else(|| policy("publication result must be an object"))?;
    if object.len() != field_count
        || object.get("schema")
            != Some(&Value::String(
                "clinker.filesystem-publication-control/1".to_owned(),
            ))
        || object.get("action") != Some(&Value::String(action.to_owned()))
        || object.get("scenario") != Some(&Value::String(scenario.to_owned()))
    {
        return Err(policy(
            "publication result does not match the exact scenario action",
        ));
    }
    Ok(())
}

fn validate_recovery_ready(
    value: &Value,
    scenario: &str,
) -> Result<(String, String, String), GateError> {
    let object = value
        .as_object()
        .ok_or_else(|| policy("publication recovery result must be an object"))?;
    exact_fields(
        object,
        &[
            "action",
            "artifact_id",
            "execution_id",
            "manifest_state",
            "scenario",
            "schema",
        ],
        "publication recovery result",
    )?;
    if object.get("schema")
        != Some(&Value::String(
            "clinker.filesystem-publication-control/1".to_owned(),
        ))
        || object.get("action") != Some(&Value::String("recovery_ready".to_owned()))
        || object.get("scenario") != Some(&Value::String(scenario.to_owned()))
    {
        return Err(policy("publication recovery result identity is invalid"));
    }
    Ok((
        object_string(object, "execution_id", "publication recovery result")?.to_owned(),
        object_string(object, "artifact_id", "publication recovery result")?.to_owned(),
        object_string(object, "manifest_state", "publication recovery result")?.to_owned(),
    ))
}

fn publication_sandbox(state: &EnvironmentState) -> PathBuf {
    state.mount_root.join(".clinker-publication-matrix")
}

fn read_attempt_manifest(state: &EnvironmentState, execution_id: &str) -> Result<Value, GateError> {
    let path = publication_sandbox(state)
        .join(".clinker-attempts")
        .join(execution_id)
        .join("manifest.json");
    let bytes = read_regular(&path, "read mounted attempt manifest")?;
    serde_json::from_slice(&bytes).map_err(|_| missing("mounted attempt manifest is malformed"))
}

fn verify_complete_readback(
    state: &EnvironmentState,
    scenario: &str,
    execution_id: &str,
    artifact_id: &str,
) -> Result<(), GateError> {
    let final_path = publication_sandbox(state).join(format!("{scenario}.bin"));
    if fs::read(&final_path)
        .map_err(|error| GateError::io("read mounted final artifact", &error))?
        != b"mounted success"
    {
        return Err(missing("mounted success final readback did not match"));
    }
    let manifest = read_attempt_manifest(state, execution_id)?;
    if manifest.get("execution_id") != Some(&Value::String(execution_id.to_owned()))
        || manifest.get("state") != Some(&Value::String("complete".to_owned()))
        || manifest
            .get("artifacts")
            .and_then(Value::as_array)
            .is_none_or(|artifacts| {
                artifacts.len() != 1
                    || artifacts[0].get("artifact_id")
                        != Some(&Value::String(artifact_id.to_owned()))
                    || artifacts[0].get("state") != Some(&Value::String("published".to_owned()))
            })
    {
        return Err(missing(
            "mounted pre-cleanup Complete manifest readback is incomplete",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RetainedExpectation {
    attempt_state: &'static str,
    artifact_state: &'static str,
    size_bytes: u64,
    blake3_hex: &'static str,
    final_bytes: Option<&'static [u8]>,
}

fn retained_expectation(scenario: &str) -> Result<RetainedExpectation, GateError> {
    const EMPTY: &str = "0000000000000000000000000000000000000000000000000000000000000000";
    const REPLACEMENT: &str = "865329d9aa64a0a90e28403e1ad5bdfc26b0b5beb5a967537e78818c17e78c67";
    const INTERRUPTION: &str = "b7a4c2f87d102d06628d1ff7affff81fd46eb89f6dc029a1aae1033276227b30";

    if scenario.starts_with("ordinary-failure-") {
        return Ok(RetainedExpectation {
            attempt_state: "incomplete",
            artifact_state: "ready",
            size_bytes: 11,
            blake3_hex: REPLACEMENT,
            final_bytes: Some(b"existing final"),
        });
    }
    if scenario == "capacity-enospc"
        || scenario.ends_with("-copy")
        || scenario.ends_with("-file_synchronization")
    {
        return Ok(RetainedExpectation {
            attempt_state: "staging",
            artifact_state: "staging",
            size_bytes: 0,
            blake3_hex: EMPTY,
            final_bytes: None,
        });
    }
    if scenario.ends_with("-rename") {
        return Ok(RetainedExpectation {
            attempt_state: "publishing",
            artifact_state: "promoting",
            size_bytes: 20,
            blake3_hex: INTERRUPTION,
            final_bytes: None,
        });
    }
    if scenario.ends_with("-parent_directory_synchronization") {
        return Ok(RetainedExpectation {
            attempt_state: "publishing",
            artifact_state: "promoting",
            size_bytes: 20,
            blake3_hex: INTERRUPTION,
            final_bytes: Some(b"mounted interruption"),
        });
    }
    Err(policy(
        "publication scenario has no retained-state contract",
    ))
}

fn validate_child_manifest_state(
    child_state: &str,
    expectation: &RetainedExpectation,
) -> Result<(), GateError> {
    if child_state != expectation.attempt_state {
        return Err(missing(
            "child retained-state claim differs from the controller-derived scenario state",
        ));
    }
    Ok(())
}

fn verify_retained_readback(
    state: &EnvironmentState,
    scenario: &str,
    execution_id: &str,
    artifact_id: &str,
    expectation: &RetainedExpectation,
) -> Result<(), GateError> {
    let manifest = read_attempt_manifest(state, execution_id)?;
    let final_path = publication_sandbox(state).join(format!("{scenario}.bin"));
    let final_bytes = match expectation.final_bytes {
        Some(_) => Some(read_regular(
            &final_path,
            "read retained mounted final artifact",
        )?),
        None => match fs::symlink_metadata(&final_path) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => {
                return Err(GateError::io(
                    "inspect retained mounted final artifact",
                    &error,
                ));
            }
            Ok(_) => return Err(missing("retained scenario exposed an unexpected final")),
        },
    };
    validate_retained_observation(
        &manifest,
        execution_id,
        artifact_id,
        expectation,
        final_bytes.as_deref(),
    )
}

fn validate_retained_observation(
    manifest: &Value,
    execution_id: &str,
    artifact_id: &str,
    expectation: &RetainedExpectation,
    final_bytes: Option<&[u8]>,
) -> Result<(), GateError> {
    let artifact = manifest
        .get("artifacts")
        .and_then(Value::as_array)
        .filter(|artifacts| artifacts.len() == 1)
        .and_then(|artifacts| artifacts.first())
        .ok_or_else(|| missing("mounted retained manifest artifact inventory is incomplete"))?;
    if manifest.get("execution_id") != Some(&Value::String(execution_id.to_owned()))
        || manifest.get("state") != Some(&Value::String(expectation.attempt_state.to_owned()))
        || manifest.get("total_bytes").and_then(Value::as_u64) != Some(expectation.size_bytes)
        || artifact.get("artifact_id") != Some(&Value::String(artifact_id.to_owned()))
        || artifact.get("state") != Some(&Value::String(expectation.artifact_state.to_owned()))
        || artifact.get("size_bytes").and_then(Value::as_u64) != Some(expectation.size_bytes)
        || artifact.get("blake3_hex") != Some(&Value::String(expectation.blake3_hex.to_owned()))
        || final_bytes != expectation.final_bytes
    {
        return Err(missing(
            "mounted retained attempt, artifact, digest, or final readback differs from the scenario contract",
        ));
    }
    Ok(())
}

fn interrupt_profile(state: &EnvironmentState) -> Result<(), GateError> {
    if state.profile == NFS_PROFILE {
        let server_root = state.scratch.join("server");
        checked(
            "sudo",
            &[
                OsString::from("exportfs"),
                OsString::from("-u"),
                OsString::from(format!("127.0.0.1:{}", server_root.display())),
            ],
            inherited_environment(&[]),
            Duration::from_secs(30),
            "withdraw exact NFS publication export",
        )?;
    } else {
        let pid_path = state
            .samba_pid
            .as_ref()
            .ok_or_else(|| missing("exact Samba PID state is absent"))?;
        let pid = fs::read_to_string(pid_path)
            .map_err(|error| GateError::io("read exact Samba PID", &error))?;
        if !valid_pid(pid.trim()) {
            return Err(missing("exact Samba PID is invalid"));
        }
        checked(
            "sudo",
            &[OsString::from("kill"), OsString::from(pid.trim())],
            inherited_environment(&[]),
            Duration::from_secs(30),
            "stop exact Samba publication process",
        )?;
        let mut stopped = false;
        for _ in 0..50 {
            let state = observe(
                "sudo",
                &[
                    OsString::from("kill"),
                    OsString::from("-0"),
                    OsString::from(pid.trim()),
                ],
                inherited_environment(&[]),
                Duration::from_secs(5),
            )?;
            if state.termination == Termination::Exited(Some(1)) {
                stopped = true;
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        if !stopped {
            return Err(missing("exact Samba process did not stop before deadline"));
        }
        let _ = fs::remove_file(pid_path);
    }
    checked(
        "sudo",
        &[
            OsString::from("umount"),
            OsString::from("-f"),
            OsString::from("-l"),
            state.mount_root.as_os_str().to_owned(),
        ],
        inherited_environment(&[]),
        Duration::from_secs(30),
        "force-lazy-detach interrupted publication profile",
    )?;
    if mount_state(&state.mount_root)? {
        return Err(missing(
            "publication mount remained active after disruption",
        ));
    }
    Ok(())
}

fn recover_profile(state: &EnvironmentState) -> Result<(), GateError> {
    let protocol = state.scratch.join("recovery-protocol.txt");
    let server_root = state.scratch.join("server");
    if state.profile == NFS_PROFILE {
        provision_nfs(state, &server_root, &protocol)?;
    } else {
        provision_smb(state, &server_root, &state.scratch.join("samba"), &protocol)?;
    }
    let findmnt = checked(
        "findmnt",
        &[
            OsString::from("-T"),
            state.mount_root.as_os_str().to_owned(),
            OsString::from("-n"),
            OsString::from("-o"),
            OsString::from("SOURCE,FSTYPE,OPTIONS"),
        ],
        inherited_environment(&[]),
        Duration::from_secs(15),
        "revalidate recovered publication mount",
    )?;
    let line = String::from_utf8(findmnt)
        .map_err(|_| missing("recovered mount observation is not UTF-8"))?;
    validate_mount(&state.profile, &parse_mount(&line)?)
}

/// Execute the holder/competitor/post-release byte-range lock proof.
#[cfg(target_os = "linux")]
#[doc(hidden)]
pub fn byte_range_lock_observation(mount_root: &Path) -> Result<Vec<String>, GateError> {
    use nix::fcntl::{FcntlArg, fcntl};
    use nix::libc;

    let path = mount_root.join(".clinker-matrix-byte-range-lock");
    let holder = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .map_err(|error| GateError::io("open remote byte-range lock holder", &error))?;
    let competitor = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .map_err(|error| GateError::io("open remote byte-range lock competitor", &error))?;
    let locked = libc::flock {
        l_type: libc::F_WRLCK as _,
        l_whence: libc::SEEK_SET as _,
        l_start: 0,
        l_len: 1,
        l_pid: 0,
    };
    fcntl(&holder, FcntlArg::F_OFD_SETLK(&locked))
        .map_err(|_| missing("exclusive byte-range lock holder was not established"))?;
    if fcntl(&competitor, FcntlArg::F_OFD_SETLK(&locked)).is_ok() {
        return Err(missing(
            "a competing byte-range lock unexpectedly succeeded",
        ));
    }
    let unlocked = libc::flock {
        l_type: libc::F_UNLCK as _,
        ..locked
    };
    fcntl(&holder, FcntlArg::F_OFD_SETLK(&unlocked))
        .map_err(|_| missing("byte-range lock holder could not release its lock"))?;
    fcntl(&competitor, FcntlArg::F_OFD_SETLK(&locked))
        .map_err(|_| missing("byte-range lock was not acquirable after holder release"))?;
    fcntl(&competitor, FcntlArg::F_OFD_SETLK(&unlocked))
        .map_err(|_| missing("post-release byte-range lock could not be released"))?;
    drop(competitor);
    drop(holder);
    fs::remove_file(path)
        .map_err(|error| GateError::io("remove remote byte-range lock fixture", &error))?;
    Ok(vec![
        "holder=acquired".to_owned(),
        "competitor=blocked".to_owned(),
        "post_release=acquired".to_owned(),
    ])
}

/// Reject remote lock qualification on non-Linux hosts.
#[cfg(not(target_os = "linux"))]
#[doc(hidden)]
pub fn byte_range_lock_observation(_mount_root: &Path) -> Result<Vec<String>, GateError> {
    Err(policy("remote byte-range qualification requires Linux"))
}

fn cleanup_after_failure(profile: &str, evidence: &Path) -> bool {
    let path = state_path(evidence);
    let Ok(state) = read_state(&path) else {
        return false;
    };
    if state.profile != profile || validate_state_paths(&state).is_err() {
        return false;
    }
    let cleanup = cleanup_environment(&state);
    let logged = write_cleanup_log(evidence, &cleanup.observations).is_ok();
    if cleanup.success && logged {
        let _ = fs::remove_file(path);
    }
    cleanup.success && logged
}

fn cleanup_environment(state: &EnvironmentState) -> CleanupResult {
    let mut success = true;
    let mut observations = Vec::new();
    let mounted = match mount_state(&state.mount_root) {
        Ok(mounted) => {
            observations.push(format!(
                "pre_teardown_mount={}",
                if mounted { "present" } else { "absent" }
            ));
            mounted
        }
        Err(_) => {
            observations.push("pre_teardown_mount=unknown".to_owned());
            success = false;
            true
        }
    };
    if mounted {
        let unmounted = cleanup_command(
            "sudo",
            &[
                OsString::from("umount"),
                state.mount_root.as_os_str().to_owned(),
            ],
        );
        observations.push(format!(
            "unmount={}",
            if unmounted { "pass" } else { "failed" }
        ));
        success &= unmounted;
    } else {
        observations.push("unmount=skipped".to_owned());
    }
    if state.profile == NFS_PROFILE {
        let export_removed = cleanup_command(
            "sudo",
            &[
                OsString::from("rm"),
                OsString::from("-f"),
                OsString::from(NFS_EXPORT),
            ],
        );
        observations.push(format!(
            "nfs_export_remove={}",
            if export_removed { "pass" } else { "failed" }
        ));
        success &= export_removed;
        let exports_refreshed =
            cleanup_command("sudo", &[OsString::from("exportfs"), OsString::from("-ra")]);
        observations.push(format!(
            "nfs_export_refresh={}",
            if exports_refreshed { "pass" } else { "failed" }
        ));
        success &= exports_refreshed;
        let server_stopped = cleanup_command(
            "sudo",
            &[
                OsString::from("systemctl"),
                OsString::from("stop"),
                OsString::from("nfs-kernel-server"),
            ],
        );
        observations.push(format!(
            "nfs_server_stop={}",
            if server_stopped { "pass" } else { "failed" }
        ));
        success &= server_stopped;
    } else if let Some(pid_path) = &state.samba_pid
        && pid_path.exists()
    {
        let stopped = match fs::read_to_string(pid_path) {
            Ok(pid) if valid_pid(pid.trim()) => cleanup_command(
                "sudo",
                &[OsString::from("kill"), OsString::from(pid.trim())],
            ),
            _ => false,
        };
        observations.push(format!(
            "samba_stop={}",
            if stopped { "pass" } else { "failed" }
        ));
        success &= stopped;
    } else {
        observations.push("samba_stop=skipped".to_owned());
    }
    let backing_root = state.scratch.join("server");
    let backing_mounted = match mount_state(&backing_root) {
        Ok(mounted) => mounted,
        Err(_) => {
            observations.push("pre_teardown_backing_mount=unknown".to_owned());
            success = false;
            true
        }
    };
    if backing_mounted {
        let unmounted = cleanup_command(
            "sudo",
            &[
                OsString::from("umount"),
                backing_root.as_os_str().to_owned(),
            ],
        );
        observations.push(format!(
            "backing_unmount={}",
            if unmounted { "pass" } else { "failed" }
        ));
        success &= unmounted;
    } else {
        observations.push("backing_unmount=skipped".to_owned());
    }
    match mount_state(&state.mount_root) {
        Ok(false) => observations.push("post_teardown_mount=absent".to_owned()),
        Ok(true) => {
            observations.push("post_teardown_mount=present".to_owned());
            success = false;
        }
        Err(_) => {
            observations.push("post_teardown_mount=unknown".to_owned());
            success = false;
        }
    }
    match mount_state(&backing_root) {
        Ok(false) => observations.push("post_teardown_backing_mount=absent".to_owned()),
        Ok(true) => {
            observations.push("post_teardown_backing_mount=present".to_owned());
            success = false;
        }
        Err(_) => {
            observations.push("post_teardown_backing_mount=unknown".to_owned());
            success = false;
        }
    }
    if success {
        let removed = fs::remove_dir_all(&state.scratch).is_ok() && !state.scratch.exists();
        observations.push(format!(
            "workspace_cleanup={}",
            if removed { "pass" } else { "failed" }
        ));
        success &= removed;
    } else {
        observations.push("workspace_cleanup=deferred".to_owned());
    }
    observations.push(format!("cleanup_success={success}"));
    CleanupResult {
        success,
        observations,
    }
}

fn mount_state(path: &Path) -> Result<bool, GateError> {
    let result = observe(
        "mountpoint",
        &[OsString::from("-q"), path.as_os_str().to_owned()],
        BTreeMap::new(),
        Duration::from_secs(15),
    )?;
    if result.stdout_truncated || result.stderr_truncated {
        return Err(missing("mountpoint observation exceeded its output bound"));
    }
    classify_mountpoint_termination(result.termination)
}

fn classify_mountpoint_termination(termination: Termination) -> Result<bool, GateError> {
    match termination {
        Termination::Exited(Some(0)) => Ok(true),
        Termination::Exited(Some(32)) => Ok(false),
        _ => Err(missing("mountpoint observation failed or timed out")),
    }
}

fn write_cleanup_log(evidence: &Path, observations: &[String]) -> Result<(), GateError> {
    let directory = log_directory(evidence);
    fs::create_dir_all(&directory)
        .map_err(|error| GateError::io("create filesystem cleanup log directory", &error))?;
    let mut bytes = observations.join("\n").into_bytes();
    bytes.push(b'\n');
    write_observation(&directory.join("cleanup.txt"), &bytes)
}

fn cleanup_command(program: &str, arguments: &[OsString]) -> bool {
    observe(
        program,
        arguments,
        inherited_environment(&[]),
        Duration::from_secs(120),
    )
    .is_ok_and(|result| result.termination == Termination::Exited(Some(0)))
}

fn parse_mount(line: &str) -> Result<MountObservation, GateError> {
    let mut fields = line.split_whitespace();
    let source = fields
        .next()
        .ok_or_else(|| missing("mount source is absent"))?;
    let filesystem = fields
        .next()
        .ok_or_else(|| missing("mount filesystem type is absent"))?;
    let options = fields
        .next()
        .ok_or_else(|| missing("mount options are absent"))?;
    if fields.next().is_some() {
        return Err(missing("mount observations contain unexpected fields"));
    }
    Ok(MountObservation {
        source: source.to_owned(),
        filesystem: filesystem.to_owned(),
        options: options.split(',').map(str::to_owned).collect(),
    })
}

fn validate_mount(profile: &str, mount: &MountObservation) -> Result<(), GateError> {
    require_profile(profile)?;
    let options = mount
        .options
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    if mount.source.trim().is_empty() {
        return Err(missing("mount source is empty"));
    }
    if profile == NFS_PROFILE {
        if mount.source != "127.0.0.1:/"
            || !matches!(mount.filesystem.as_str(), "nfs" | "nfs4")
            || !options.contains("vers=4.1")
            || !options.contains("proto=tcp")
            || !options.contains("hard")
        {
            return Err(policy(
                "NFS mount must be nfs/nfs4 with vers=4.1, proto=tcp, and hard",
            ));
        }
        if ["local_lock=all", "local_lock=flock", "local_lock=posix"]
            .iter()
            .any(|option| options.contains(option))
        {
            return Err(policy("NFS mount uses a prohibited local-only lock mode"));
        }
    } else {
        if mount.source != "//127.0.0.1/clinker"
            || mount.filesystem != "cifs"
            || !options.contains("vers=3.1.1")
            || !options.contains("cache=strict")
            || !options.contains("noperm")
        {
            return Err(policy(
                "SMB mount must be cifs with vers=3.1.1, cache=strict, and noperm",
            ));
        }
        if options.contains("nobrl") || options.contains("nostrictsync") {
            return Err(policy(
                "SMB mount disables remote locking or strict synchronization",
            ));
        }
    }
    Ok(())
}

fn validate_packages(profile: &str, lines: &[String]) -> Result<(), GateError> {
    let expected = if profile == NFS_PROFILE {
        BTreeSet::from(["nfs-common", "nfs-kernel-server"])
    } else {
        BTreeSet::from(["cifs-utils", "samba"])
    };
    let mut observed = BTreeSet::new();
    for line in lines {
        let (name, version) = line
            .split_once('=')
            .ok_or_else(|| missing("package observation is malformed"))?;
        if version.trim().is_empty() || !observed.insert(name) {
            return Err(missing("package observation is empty or duplicated"));
        }
    }
    if observed != expected {
        return Err(missing(
            "exact client/server package observations are absent",
        ));
    }
    Ok(())
}

fn validate_protocol(profile: &str, lines: &[String]) -> Result<(), GateError> {
    let text = lines.join("\n").to_ascii_lowercase();
    let matched = if profile == NFS_PROFILE {
        text.contains("vers=4.1") || text.contains("nfsv4.1") || text.contains("nfs v4.1")
    } else {
        text.contains("3.1.1") || text.contains("0x311") || text.contains("dialect 311")
    };
    if !matched {
        return Err(missing("negotiated protocol observation is absent"));
    }
    Ok(())
}

fn require_cleanup_liveness(mount_root: &Path) -> Result<(), GateError> {
    for entry in fs::read_dir(mount_root)
        .map_err(|error| GateError::io("inspect mounted share cleanup", &error))?
    {
        let name = entry
            .map_err(|error| GateError::io("inspect mounted share entry", &error))?
            .file_name();
        if name.to_string_lossy().starts_with(".clinker-matrix-") {
            return Err(missing("remote semantic test left matrix-owned files"));
        }
    }
    Ok(())
}

fn runner_observation() -> Result<Value, GateError> {
    let kernel = checked(
        "uname",
        &[OsString::from("-r")],
        inherited_environment(&[]),
        Duration::from_secs(15),
        "observe runner kernel",
    )?;
    let kernel =
        String::from_utf8(kernel).map_err(|_| missing("runner kernel observation is not UTF-8"))?;
    Ok(json!({
        "image_os": required_env("ImageOS")?,
        "image_version": required_env("ImageVersion")?,
        "kernel": kernel.trim(),
        "os": required_env("RUNNER_OS")?,
    }))
}

fn ci_identity() -> Result<Value, GateError> {
    let identity = json!({
        "job": required_env("GITHUB_JOB")?,
        "repository": required_env("GITHUB_REPOSITORY")?,
        "repository_revision": required_env("GITHUB_SHA")?,
        "run_attempt": required_env("GITHUB_RUN_ATTEMPT")?
            .parse::<u64>()
            .map_err(|_| missing("filesystem CI run attempt is invalid"))?,
        "run_id": required_env("GITHUB_RUN_ID")?,
        "workflow_path": CI_WORKFLOW_PATH,
        "workflow_ref": required_env("GITHUB_WORKFLOW_REF")?,
    });
    validate_ci_identity(identity.as_object().ok_or_else(|| {
        GateError::internal("filesystem.ci_identity", "CI identity must be an object")
    })?)?;
    Ok(identity)
}

fn validate_ci_identity(identity: &Map<String, Value>) -> Result<(), GateError> {
    exact_fields(
        identity,
        &[
            "job",
            "repository",
            "repository_revision",
            "run_attempt",
            "run_id",
            "workflow_path",
            "workflow_ref",
        ],
        "filesystem CI identity",
    )?;
    if object_string(identity, "repository", "filesystem CI identity")? != CI_REPOSITORY {
        return Err(policy(
            "filesystem evidence repository is not rustpunk/clinker",
        ));
    }
    let revision = object_string(identity, "repository_revision", "filesystem CI identity")?;
    if revision.len() != 40
        || !revision
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(missing(
            "filesystem CI repository revision is not a lowercase commit SHA",
        ));
    }
    if identity.get("workflow_path") != Some(&Value::String(CI_WORKFLOW_PATH.to_owned())) {
        return Err(policy(
            "filesystem evidence workflow path is not .github/workflows/ci.yml",
        ));
    }
    let workflow_ref = object_string(identity, "workflow_ref", "filesystem CI identity")?;
    let expected_prefix = format!("{CI_REPOSITORY}/{CI_WORKFLOW_PATH}@");
    if workflow_ref
        .strip_prefix(&expected_prefix)
        .is_none_or(str::is_empty)
    {
        return Err(policy(
            "filesystem evidence workflow ref is not bound to the governed CI workflow",
        ));
    }
    let run_id = object_string(identity, "run_id", "filesystem CI identity")?;
    if run_id
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .is_none()
    {
        return Err(missing("filesystem CI run ID is invalid"));
    }
    if identity
        .get("run_attempt")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .is_none()
    {
        return Err(missing("filesystem CI run attempt is invalid"));
    }
    if object_string(identity, "job", "filesystem CI identity")? != CI_JOB {
        return Err(policy(
            "filesystem evidence job is not the governed filesystem-matrix job",
        ));
    }
    Ok(())
}

fn require_hosted_runner() -> Result<(), GateError> {
    if std::env::var("GITHUB_ACTIONS").as_deref() != Ok("true")
        || std::env::var("RUNNER_OS").as_deref() != Ok("Linux")
    {
        return Err(missing(
            "self-provisioning is restricted to disposable GitHub-hosted Linux runners",
        ));
    }
    for name in [
        "RUNNER_TEMP",
        "ImageOS",
        "ImageVersion",
        "GITHUB_JOB",
        "GITHUB_REPOSITORY",
        "GITHUB_RUN_ATTEMPT",
        "GITHUB_RUN_ID",
        "GITHUB_SHA",
        "GITHUB_WORKFLOW_REF",
    ] {
        required_env(name)?;
    }
    Ok(())
}

fn required_env(name: &'static str) -> Result<String, GateError> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| missing(format!("runner observation {name} is absent")))
}

fn canonical_directory_from_env(name: &'static str) -> Result<PathBuf, GateError> {
    let value = required_env(name)?;
    let path = fs::canonicalize(value)
        .map_err(|error| GateError::io("resolve runner temporary directory", &error))?;
    if !path.is_dir() {
        return Err(missing("runner temporary directory is unavailable"));
    }
    Ok(path)
}

fn require_profile(profile: &str) -> Result<(), GateError> {
    if !matches!(profile, NFS_PROFILE | SMB_PROFILE) {
        return Err(policy(format!(
            "profile '{profile}' is not in the accepted remote filesystem matrix"
        )));
    }
    Ok(())
}

fn validate_workflow(workflow: &Value) -> Result<(), GateError> {
    let job = workflow
        .get("jobs")
        .and_then(|jobs| jobs.get("filesystem-matrix"))
        .and_then(Value::as_object)
        .ok_or_else(|| policy("dedicated filesystem-matrix job is absent"))?;
    if job.get("runs-on") != Some(&Value::String("ubuntu-24.04".to_owned())) {
        return Err(policy("filesystem matrix is not pinned to ubuntu-24.04"));
    }
    if job.contains_key("container") {
        return Err(policy("filesystem matrix must run on a full VM"));
    }
    if job
        .get("env")
        .and_then(Value::as_object)
        .is_some_and(|env| env.contains_key("EVIDENCE_PATH"))
    {
        return Err(policy(
            "filesystem evidence path must be bound at runner step scope",
        ));
    }
    let profiles = job
        .get("strategy")
        .and_then(|value| value.get("matrix"))
        .and_then(|value| value.get("profile"))
        .and_then(Value::as_array)
        .ok_or_else(|| policy("filesystem profile matrix is absent"))?;
    let observed = profiles
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    if observed != [NFS_PROFILE, SMB_PROFILE] {
        return Err(policy(
            "filesystem profile matrix must contain each exact profile once",
        ));
    }
    let steps = job
        .get("steps")
        .and_then(Value::as_array)
        .ok_or_else(|| policy("filesystem matrix steps are absent"))?;
    let mut provision = 0_usize;
    let mut teardown = 0_usize;
    let mut teardown_index = None;
    let mut upload = 0_usize;
    let mut fetch = None;
    for (index, step) in steps.iter().enumerate() {
        let Some(step) = step.as_object() else {
            return Err(policy("filesystem matrix step must be an object"));
        };
        let upload_action = "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a";
        if step
            .get("uses")
            .and_then(Value::as_str)
            .is_some_and(|uses| {
                uses.starts_with("actions/upload-artifact@") && uses != upload_action
            })
        {
            return Err(policy(
                "filesystem evidence upload action is not the exact reviewed revision",
            ));
        }
        if step.get("uses") == Some(&Value::String(upload_action.to_owned())) {
            let with = step
                .get("with")
                .and_then(Value::as_object)
                .ok_or_else(|| policy("filesystem evidence upload inputs are absent"))?;
            exact_fields(
                with,
                &["if-no-files-found", "name", "path", "retention-days"],
                "filesystem evidence upload inputs",
            )?;
            if step.get("if") != Some(&Value::String("always()".to_owned()))
                || teardown_index.is_none_or(|teardown| teardown >= index)
                || with.get("name")
                    != Some(&Value::String(
                        "filesystem-${{ matrix.profile }}-evidence".to_owned(),
                    ))
                || with.get("path")
                    != Some(&Value::String(
                        "${{ runner.temp }}/filesystem-${{ matrix.profile }}*".to_owned(),
                    ))
                || with.get("if-no-files-found") != Some(&Value::String("error".to_owned()))
                || with.get("retention-days").and_then(Value::as_u64) != Some(14)
            {
                return Err(policy(
                    "filesystem evidence upload is not unconditional, bounded, and after teardown",
                ));
            }
            upload += 1;
        }
        let Some(run) = step.get("run").and_then(Value::as_str) else {
            continue;
        };
        if run.split_whitespace().eq([
            "cargo",
            "fetch",
            "--manifest-path",
            "tools/release-policy/Cargo.toml",
            "--locked",
        ]) {
            fetch = Some(index);
        }
        if run.contains("filesystem self-test") && fetch.is_none_or(|fetch| fetch >= index) {
            return Err(policy(
                "filesystem dependencies must be fetched before offline execution",
            ));
        }
        if run.contains("filesystem provision-and-run") {
            if !direct_locked_command(run, "provision-and-run")
                || !run.contains("--profile")
                || !run.contains("--evidence")
                || !has_step_evidence_path(step)
            {
                return Err(policy(
                    "filesystem provision step is not the exact direct locked Rust command",
                ));
            }
            if fetch.is_none_or(|fetch| fetch >= index) {
                return Err(policy(
                    "filesystem dependencies must be fetched before offline execution",
                ));
            }
            provision += 1;
        }
        if run.contains("filesystem teardown") {
            if step.get("if") != Some(&Value::String("always()".to_owned()))
                || !direct_locked_command(run, "teardown")
                || !run.contains("--profile")
                || !run.contains("--evidence")
                || !has_step_evidence_path(step)
            {
                return Err(policy(
                    "filesystem teardown is not unconditional and direct",
                ));
            }
            teardown += 1;
            teardown_index = Some(index);
        }
        if run.contains("test-filesystem-matrix.sh") {
            return Err(policy("filesystem CI must invoke the Rust gate directly"));
        }
    }
    if provision != 1 || teardown != 1 || upload != 1 {
        return Err(policy(
            "filesystem CI requires exactly one direct provision, teardown, and evidence upload step",
        ));
    }
    Ok(())
}

fn has_step_evidence_path(step: &serde_json::Map<String, Value>) -> bool {
    step.get("env")
        .and_then(Value::as_object)
        .and_then(|env| env.get("EVIDENCE_PATH"))
        .and_then(Value::as_str)
        == Some("${{ runner.temp }}/filesystem-${{ matrix.profile }}.json")
}

fn direct_locked_command(run: &str, command: &str) -> bool {
    let expected = [
        "cargo",
        "run",
        "--quiet",
        "--manifest-path",
        "tools/release-policy/Cargo.toml",
        "--locked",
        "--offline",
        "--",
        "filesystem",
        command,
        "--profile",
        "${{ matrix.profile }}",
        "--evidence",
        "${EVIDENCE_PATH}",
    ];
    shell_free_argv(run)
        .is_some_and(|argv| argv.iter().map(String::as_str).eq(expected.iter().copied()))
}

fn shell_free_argv(run: &str) -> Option<Vec<String>> {
    #[derive(Clone, Copy)]
    enum Quote {
        Single,
        Double,
    }

    let mut argv = Vec::new();
    let mut token = String::new();
    let mut quote = None;
    let mut characters = run.chars().peekable();
    while let Some(character) = characters.next() {
        if matches!(character, '\n' | '\r' | '\\' | '`')
            || (character == '$' && characters.peek() == Some(&'('))
        {
            return None;
        }
        match quote {
            Some(Quote::Single) if character == '\'' => quote = None,
            Some(Quote::Double) if character == '"' => quote = None,
            Some(_) => token.push(character),
            None if character == '\'' => quote = Some(Quote::Single),
            None if character == '"' => quote = Some(Quote::Double),
            None if character.is_whitespace() => {
                if !token.is_empty() {
                    argv.push(std::mem::take(&mut token));
                }
            }
            None if matches!(character, ';' | '&' | '|' | '<' | '>' | '(' | ')') => {
                return None;
            }
            None => token.push(character),
        }
    }
    if quote.is_some() {
        return None;
    }
    if !token.is_empty() {
        argv.push(token);
    }
    Some(argv)
}

fn minimal_status(profile: &str, status: &str, failed_step: &str, cleanup_success: bool) -> Value {
    json!({
        "cleanup_success": cleanup_success,
        "failed_step": failed_step,
        "profile": profile,
        "schema": EVIDENCE_SCHEMA,
        "status": status,
        "support_eligible": false,
    })
}

fn admit_evidence_destination(path: &Path) -> Result<(), GateError> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|error| GateError::io("create filesystem evidence directory", &error))?;
    let parent = fs::canonicalize(parent)
        .map_err(|error| GateError::io("resolve filesystem evidence directory", &error))?;
    if !parent.is_dir() {
        return Err(policy("filesystem evidence parent is not a directory"));
    }
    if path.exists() {
        let metadata = fs::symlink_metadata(path)
            .map_err(|error| GateError::io("inspect filesystem evidence", &error))?;
        if metadata.file_type().is_symlink() || !metadata.is_file() {
            return Err(policy(
                "filesystem evidence must be a regular non-symlink file",
            ));
        }
    }
    Ok(())
}

fn write_status(path: &Path, value: Value) -> Result<(), GateError> {
    admit_evidence_destination(path)?;
    let bytes = serde_json::to_vec(&value).map_err(|_| {
        GateError::internal("filesystem.evidence_json", "evidence serialization failed")
    })?;
    let value = canonical::parse_json(&bytes)?;
    let bytes = canonical::to_bytes(&value)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temporary = Builder::new()
        .prefix(".filesystem-evidence.")
        .permissions(fs::Permissions::from_mode(0o600))
        .tempfile_in(parent)
        .map_err(|error| GateError::io("create filesystem evidence temporary", &error))?;
    temporary
        .write_all(&bytes)
        .and_then(|()| temporary.flush())
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| GateError::io("write filesystem evidence temporary", &error))?;
    let file = temporary
        .persist(path)
        .map_err(|error| GateError::io("install filesystem evidence", &error.error))?;
    file.sync_all()
        .map_err(|error| GateError::io("sync filesystem evidence", &error))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| GateError::io("sync filesystem evidence directory", &error))?;
    Ok(())
}

fn read_status(path: &Path) -> Result<Value, GateError> {
    if !path.is_file() {
        return Err(missing("filesystem evidence is absent"));
    }
    let bytes = read_regular(path, "read filesystem evidence")?;
    let parsed = canonical::parse_json(&bytes)?;
    if canonical::to_bytes(&parsed)? != bytes {
        return Err(policy("filesystem evidence bytes are not canonical JSON"));
    }
    serde_json::from_slice(&bytes).map_err(|_| policy("filesystem evidence is malformed"))
}

fn set_field(value: &mut Value, field: &str, next: Value) -> Result<(), GateError> {
    value
        .as_object_mut()
        .ok_or_else(|| policy("filesystem evidence must be an object"))?
        .insert(field.to_owned(), next);
    Ok(())
}

fn status_string<'a>(value: &'a Value, field: &str) -> Result<&'a str, GateError> {
    value
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| policy(format!("filesystem evidence.{field} is absent")))
}

fn state_path(evidence: &Path) -> PathBuf {
    let mut path = evidence.as_os_str().to_os_string();
    path.push(".state.json");
    PathBuf::from(path)
}

fn log_directory(evidence: &Path) -> PathBuf {
    let mut path = evidence.as_os_str().to_os_string();
    path.push(".d");
    PathBuf::from(path)
}

fn write_state(path: &Path, state: &EnvironmentState) -> Result<(), GateError> {
    let value = json!({
        "mount_root": state.mount_root,
        "profile": state.profile,
        "samba_pid": state.samba_pid,
        "schema": "clinker.filesystem-matrix-state/v1",
        "scratch": state.scratch,
    });
    write_status(path, value)
}

fn read_state(path: &Path) -> Result<EnvironmentState, GateError> {
    let value = read_status(path)?;
    let object = value
        .as_object()
        .ok_or_else(|| policy("filesystem environment state must be an object"))?;
    let expected = ["schema", "profile", "scratch", "mount_root", "samba_pid"]
        .into_iter()
        .collect::<BTreeSet<_>>();
    if object.keys().map(String::as_str).collect::<BTreeSet<_>>() != expected
        || object.get("schema")
            != Some(&Value::String(
                "clinker.filesystem-matrix-state/v1".to_owned(),
            ))
    {
        return Err(policy("filesystem environment state fields do not match"));
    }
    Ok(EnvironmentState {
        profile: status_string(&value, "profile")?.to_owned(),
        scratch: PathBuf::from(status_string(&value, "scratch")?),
        mount_root: PathBuf::from(status_string(&value, "mount_root")?),
        samba_pid: match object.get("samba_pid") {
            Some(Value::String(path)) => Some(PathBuf::from(path)),
            Some(Value::Null) => None,
            _ => return Err(policy("filesystem Samba PID state is malformed")),
        },
    })
}

fn validate_state_paths(state: &EnvironmentState) -> Result<(), GateError> {
    require_profile(&state.profile)?;
    let runner_temp = canonical_directory_from_env("RUNNER_TEMP")?;
    let scratch = fs::canonicalize(&state.scratch)
        .map_err(|error| GateError::io("resolve filesystem state workspace", &error))?;
    if !scratch.starts_with(&runner_temp)
        || scratch
            .file_name()
            .and_then(OsStr::to_str)
            .is_none_or(|name| !name.starts_with("clinker-filesystem-matrix."))
        || state.mount_root != scratch.join("mount")
        || match state.profile.as_str() {
            NFS_PROFILE => state.samba_pid.is_some(),
            SMB_PROFILE => state.samba_pid.as_ref() != Some(&scratch.join("samba/smbd.pid")),
            _ => true,
        }
    {
        return Err(policy(
            "filesystem teardown state escapes the disposable runner workspace",
        ));
    }
    Ok(())
}

fn observed_lines(path: &Path, operation: &'static str) -> Result<Vec<String>, GateError> {
    let bytes = read_regular(path, operation)?;
    let text = std::str::from_utf8(&bytes)
        .map_err(|_| missing("filesystem observation log is not UTF-8"))?;
    let lines = text
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect::<Vec<_>>();
    if lines.is_empty() {
        return Err(missing("filesystem observation log is empty"));
    }
    Ok(lines)
}

fn write_observation(path: &Path, bytes: &[u8]) -> Result<(), GateError> {
    if bytes.is_empty() {
        return Err(missing("filesystem observation output is empty"));
    }
    fs::write(path, bytes)
        .map_err(|error| GateError::io("write filesystem observation", &error))?;
    File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|error| GateError::io("sync filesystem observation", &error))
}

fn read_regular(path: &Path, operation: &'static str) -> Result<Vec<u8>, GateError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| GateError::io(operation, &error))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(policy(
            "required filesystem input is not a regular non-symlink file",
        ));
    }
    read_bounded(path, operation, MAX_INPUT_BYTES)
}

fn inherited_environment(extra: &[(&str, OsString)]) -> BTreeMap<OsString, OsString> {
    let mut environment = BTreeMap::new();
    for name in [
        "PATH",
        "LANG",
        "LC_ALL",
        "TZ",
        "CI",
        "GITHUB_TOKEN",
        "GH_TOKEN",
        "RUNNER_TEMP",
    ] {
        if let Some(value) = std::env::var_os(name) {
            environment.insert(OsString::from(name), value);
        }
    }
    for (name, value) in extra {
        environment.insert(OsString::from(name), value.clone());
    }
    environment
}

fn checked(
    program: &str,
    arguments: &[OsString],
    environment: BTreeMap<OsString, OsString>,
    timeout: Duration,
    label: &'static str,
) -> Result<Vec<u8>, GateError> {
    let result = observe(program, arguments, environment, timeout)?;
    if result.termination != Termination::Exited(Some(0))
        || result.stdout_truncated
        || result.stderr_truncated
    {
        return Err(missing(format!(
            "{label} failed, timed out, or exceeded its output bound"
        )));
    }
    let mut output = result.stdout;
    output.extend(result.stderr);
    Ok(output)
}

fn observe(
    program: &str,
    arguments: &[OsString],
    environment: BTreeMap<OsString, OsString>,
    timeout: Duration,
) -> Result<ChildResult, GateError> {
    child::run(ChildSpec {
        program: PathBuf::from(program),
        arguments: arguments.to_vec(),
        environment,
        timeout,
        output_limit: MAX_CHILD_OUTPUT_BYTES,
    })
}

fn valid_pid(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()) && value != "0"
}

fn exact_fields(
    object: &Map<String, Value>,
    fields: &[&str],
    label: &str,
) -> Result<(), GateError> {
    let expected = fields.iter().copied().collect::<BTreeSet<_>>();
    let observed = object.keys().map(String::as_str).collect::<BTreeSet<_>>();
    if observed != expected {
        return Err(missing(format!(
            "{label} fields do not match the complete contract"
        )));
    }
    Ok(())
}

fn object_string<'a>(
    object: &'a Map<String, Value>,
    field: &str,
    label: &str,
) -> Result<&'a str, GateError> {
    object
        .get(field)
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| missing(format!("{label}.{field} is absent")))
}

fn string_array(value: Option<&Value>, label: &str) -> Result<Vec<String>, GateError> {
    let values = value
        .and_then(Value::as_array)
        .ok_or_else(|| missing(format!("{label} must be an array")))?;
    let mut output = Vec::with_capacity(values.len());
    for value in values {
        let value = value
            .as_str()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| missing(format!("{label} contains an empty observation")))?;
        output.push(value.to_owned());
    }
    if output.is_empty() {
        return Err(missing(format!("{label} is empty")));
    }
    Ok(output)
}

fn require_exact_string_array(
    value: Option<&Value>,
    expected: &[&str],
    label: &str,
) -> Result<(), GateError> {
    let observed = string_array(value, label)?;
    if observed
        .iter()
        .map(String::as_str)
        .eq(expected.iter().copied())
    {
        Ok(())
    } else {
        Err(missing(format!(
            "{label} do not match the complete ordered contract"
        )))
    }
}

fn policy(detail: impl Into<String>) -> GateError {
    GateError::policy("filesystem.policy_required", detail)
}

fn missing(detail: impl Into<String>) -> GateError {
    GateError::policy("filesystem.missing_evidence", detail)
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;
    use std::path::PathBuf;

    use serde_json::json;
    use tempfile::tempdir;

    use super::{
        ChildResult, Termination, classify_mountpoint_termination, direct_locked_command,
        require_semantic_success, retained_expectation, validate_child_manifest_state,
        validate_retained_observation,
    };

    const DIRECT_PROVISION: &str = "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem provision-and-run --profile \"${{ matrix.profile }}\" --evidence \"${EVIDENCE_PATH}\"";

    #[test]
    fn direct_filesystem_command_requires_one_exact_shell_free_argv() {
        assert!(direct_locked_command(DIRECT_PROVISION, "provision-and-run"));
        for invalid in [
            "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --offline --locked -- filesystem provision-and-run --profile \"${{ matrix.profile }}\" --evidence \"${EVIDENCE_PATH}\"",
            "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem provision-and-run --profile \"${{ matrix.profile }}\" --evidence \"${EVIDENCE_PATH}\" --verbose",
            "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem provision-and-run --profile \"${{ matrix.profile }}\" --evidence \"${EVIDENCE_PATH}\" && true",
            "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem provision-and-run --profile \"${{ matrix.profile }}\" --evidence \"${EVIDENCE_PATH}\"; true",
            "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem provision-and-run --profile \"${{ matrix.profile }}\" --evidence \"$(printf bad)\"",
        ] {
            assert!(!direct_locked_command(invalid, "provision-and-run"));
        }
    }

    #[test]
    fn retained_support_evidence_rejects_wrong_state_artifact_and_final_bytes() {
        let expectation =
            retained_expectation("interruption-direct-parent_directory_synchronization")
                .expect("known retained scenario");
        let manifest = json!({
            "execution_id": "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
            "state": expectation.attempt_state,
            "total_bytes": expectation.size_bytes,
            "artifacts": [{
                "artifact_id": "artifact-00000001",
                "state": expectation.artifact_state,
                "size_bytes": expectation.size_bytes,
                "blake3_hex": expectation.blake3_hex,
            }],
        });
        validate_child_manifest_state("publishing", &expectation).expect("derived child state");
        validate_retained_observation(
            &manifest,
            "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
            "artifact-00000001",
            &expectation,
            Some(b"mounted interruption"),
        )
        .expect("complete retained observation");

        validate_child_manifest_state("incomplete", &expectation)
            .expect_err("semantically wrong child state must fail");
        let mut wrong_artifact = manifest.clone();
        wrong_artifact["artifacts"][0]["state"] = json!("unpublished");
        validate_retained_observation(
            &wrong_artifact,
            "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
            "artifact-00000001",
            &expectation,
            Some(b"mounted interruption"),
        )
        .expect_err("wrong artifact state must fail");
        validate_retained_observation(
            &manifest,
            "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
            "artifact-00000001",
            &expectation,
            Some(b"corrupted final"),
        )
        .expect_err("wrong final bytes must fail");
    }

    #[test]
    fn util_linux_mountpoint_exit_contract_distinguishes_absent_mounts() {
        assert!(classify_mountpoint_termination(Termination::Exited(Some(0))).unwrap());
        assert!(!classify_mountpoint_termination(Termination::Exited(Some(32))).unwrap());
        assert!(classify_mountpoint_termination(Termination::Exited(Some(1))).is_err());
        assert!(classify_mountpoint_termination(Termination::TimedOut).is_err());
    }

    #[test]
    fn failed_semantic_test_persists_bounded_child_evidence() {
        let directory = tempdir().expect("temporary evidence directory");
        let log = directory.path().join("semantic-test.txt");
        let result = ChildResult {
            program: PathBuf::from("cargo"),
            arguments: vec![OsString::from("test")],
            termination: Termination::Exited(Some(101)),
            stdout: b"running remote_filesystem_matrix_semantics\n".to_vec(),
            stderr: b"test failed: concrete semantic error\n".to_vec(),
            stdout_truncated: false,
            stderr_truncated: false,
        };

        require_semantic_success(&log, &result, "remote_filesystem_matrix_semantics")
            .expect_err("failed child must fail qualification");

        let evidence = fs::read_to_string(log).expect("failed child evidence");
        assert!(evidence.contains("termination=Exited(Some(101))"));
        assert!(evidence.contains("running remote_filesystem_matrix_semantics"));
        assert!(evidence.contains("test failed: concrete semantic error"));
    }
}
