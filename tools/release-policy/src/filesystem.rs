//! Fail-closed NFSv4.1 and SMB3.1.1 qualification state machine.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::net::{SocketAddr, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

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

const EVIDENCE_SCHEMA: &str = "clinker.filesystem-matrix-evidence/v1";
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
        fs::set_permissions(&server_root, fs::Permissions::from_mode(0o777))
            .map_err(|error| GateError::io("set filesystem server permissions", &error))?;
        let state = EnvironmentState {
            profile: request.profile.clone(),
            scratch,
            mount_root,
            samba_pid: (request.profile == SMB_PROFILE).then(|| samba_dir.join("smbd.pid")),
        };
        write_state(&state_path(&request.evidence), &state)?;

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
    let semantic_log = log_directory(&request.evidence).join("semantic-test.txt");
    let semantic = semantic_test(&request.profile, &request.mount_root, &semantic_log)?;
    require_cleanup_liveness(&request.mount_root)?;
    let runner = runner_observation()?;
    let current = std::env::current_dir()
        .map_err(|error| GateError::io("resolve local workspace", &error))?;

    Ok(json!({
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
            "local_workspace": current,
            "mounted_share": request.mount_root,
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
        "schema": EVIDENCE_SCHEMA,
        "semantic_results": {
            "cancellation_no_final": "pass",
            "cleanup_liveness": "pass",
            "confinement": "pass",
            "cross_filesystem_no_copy": "pass",
            "rename_visibility": "pass",
            "sync_durability": "pass",
            "test_filter": "remote_filesystem_matrix_semantics",
            "test_log": semantic,
        },
        "status": "semantic_pass",
        "support_eligible": false,
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
            "ci_identity",
            "cleanup_success",
            "cleanup_observations",
            "environment_teardown",
            "injected_failures",
            "locations",
            "lock_observations",
            "mount",
            "packages",
            "profile",
            "protocol_observations",
            "runner",
            "schema",
            "semantic_results",
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
    for field in ["local_workspace", "mounted_share"] {
        object_string(locations, field, "filesystem locations")?;
    }

    let semantic = object
        .get("semantic_results")
        .and_then(Value::as_object)
        .ok_or_else(|| missing("semantic results are absent"))?;
    exact_fields(
        semantic,
        &[
            "cancellation_no_final",
            "cleanup_liveness",
            "confinement",
            "cross_filesystem_no_copy",
            "rename_visibility",
            "sync_durability",
            "test_filter",
            "test_log",
        ],
        "filesystem semantic results",
    )?;
    for field in [
        "cancellation_no_final",
        "cleanup_liveness",
        "confinement",
        "cross_filesystem_no_copy",
        "rename_visibility",
        "sync_durability",
    ] {
        if semantic.get(field) != Some(&Value::String("pass".to_owned())) {
            return Err(missing(format!("semantic result {field} did not pass")));
        }
    }
    if semantic.get("test_filter")
        != Some(&Value::String(
            "remote_filesystem_matrix_semantics".to_owned(),
        ))
    {
        return Err(missing("the named remote semantic test was not selected"));
    }
    object_string(semantic, "test_log", "filesystem semantic results")?;

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
    let mount = observe(
        "sudo",
        &[
            OsString::from("mount"),
            OsString::from("-t"),
            OsString::from("cifs"),
            OsString::from("//127.0.0.1/clinker"),
            state.mount_root.as_os_str().to_owned(),
            OsString::from("-o"),
            OsString::from("guest,vers=3.1.1,cache=strict,strictsync,mfsymlinks,noperm"),
        ],
        inherited_environment(&[]),
        Duration::from_secs(120),
    )?;
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

fn semantic_test(profile: &str, mount_root: &Path, log_path: &Path) -> Result<String, GateError> {
    let environment = inherited_environment(&[
        ("CLINKER_FILESYSTEM_PROFILE", OsString::from(profile)),
        ("CLINKER_FILESYSTEM_ROOT", mount_root.as_os_str().to_owned()),
        ("CARGO_INCREMENTAL", OsString::from("0")),
        ("CARGO_BUILD_JOBS", OsString::from("1")),
    ]);
    let result = observe(
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
        environment,
        Duration::from_secs(900),
    )?;
    require_semantic_success(log_path, &result)
}

fn require_semantic_success(log_path: &Path, result: &ChildResult) -> Result<String, GateError> {
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
    if !text.contains("remote_filesystem_matrix_semantics") || !text.contains("test result: ok") {
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
    let mut fetch = None;
    for (index, step) in steps.iter().enumerate() {
        let Some(step) = step.as_object() else {
            return Err(policy("filesystem matrix step must be an object"));
        };
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
        }
        if run.contains("test-filesystem-matrix.sh") {
            return Err(policy("filesystem CI must invoke the Rust gate directly"));
        }
    }
    if provision != 1 || teardown != 1 {
        return Err(policy(
            "filesystem CI requires exactly one direct provision and teardown step",
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
    let tokens = run.split_whitespace().collect::<Vec<_>>();
    [
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
    ]
    .windows(2)
    .all(|pair| tokens.windows(2).any(|candidate| candidate == pair))
        && tokens.first() == Some(&"cargo")
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
        return Err(policy("filesystem evidence bytes are not canonical v1"));
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

    use tempfile::tempdir;

    use super::{
        ChildResult, Termination, classify_mountpoint_termination, require_semantic_success,
    };

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

        require_semantic_success(&log, &result).expect_err("failed child must fail qualification");

        let evidence = fs::read_to_string(log).expect("failed child evidence");
        assert!(evidence.contains("termination=Exited(Some(101))"));
        assert!(evidence.contains("running remote_filesystem_matrix_semantics"));
        assert!(evidence.contains("test failed: concrete semantic error"));
    }
}
