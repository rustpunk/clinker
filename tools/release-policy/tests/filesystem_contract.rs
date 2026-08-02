use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::Duration;

use serde_json::{Value, json};
use tempfile::TempDir;

const NFS: &str = "linux-nfsv4.1-loopback-ci";
const SMB: &str = "linux-smb3.1.1-loopback-ci";

fn gate(root: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
        .current_dir(root)
        .args(arguments)
        .output()
        .expect("clinker-release-policy must execute")
}

fn fixture_workflow(root: &Path) -> PathBuf {
    let workflow = root.join("ci.yml");
    fs::write(
        &workflow,
        r#"name: CI
jobs:
  filesystem-matrix:
    runs-on: ubuntu-24.04
    strategy:
      matrix:
        profile:
          - linux-nfsv4.1-loopback-ci
          - linux-smb3.1.1-loopback-ci
    steps:
      - name: Fetch locked policy dependencies
        run: cargo fetch --manifest-path tools/release-policy/Cargo.toml --locked
      - name: Validate filesystem matrix contract
        run: cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem self-test
      - name: Provision and exercise exact remote profile
        env:
          EVIDENCE_PATH: ${{ runner.temp }}/filesystem-${{ matrix.profile }}.json
        run: cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem provision-and-run --profile "${{ matrix.profile }}" --evidence "${EVIDENCE_PATH}"
      - name: Teardown remote profile
        if: always()
        env:
          EVIDENCE_PATH: ${{ runner.temp }}/filesystem-${{ matrix.profile }}.json
        run: cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem teardown --profile "${{ matrix.profile }}" --evidence "${EVIDENCE_PATH}"
"#,
    )
    .expect("workflow fixture");
    workflow
}

fn evidence(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("evidence must exist"))
        .expect("evidence must be JSON")
}

#[test]
fn self_test_executes_the_exact_direct_ci_topology_contract() {
    let root = TempDir::new().expect("temporary root");
    let workflow = fixture_workflow(root.path());
    let output = gate(
        root.path(),
        &[
            "filesystem",
            "self-test",
            "--workflow",
            workflow.to_str().expect("UTF-8 workflow path"),
        ],
    );
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stdout, b"filesystem matrix self-test: PASS\n");
    assert!(output.stderr.is_empty());
}

#[test]
fn self_test_rejects_invalid_runner_topology_or_missing_direct_teardown() {
    let root = TempDir::new().expect("temporary root");
    let workflow = fixture_workflow(root.path());
    let base = fs::read_to_string(&workflow).expect("workflow fixture");
    for (name, changed) in [
        (
            "container",
            base.replace(
                "    runs-on: ubuntu-24.04\n",
                "    runs-on: ubuntu-24.04\n    container: ubuntu:24.04\n",
            ),
        ),
        (
            "duplicate",
            base.replace(
                "          - linux-smb3.1.1-loopback-ci\n",
                "          - linux-nfsv4.1-loopback-ci\n",
            ),
        ),
        (
            "conditional",
            base.replace("        if: always()\n", "        if: success()\n"),
        ),
        (
            "wrapper",
            base.replace(
                "cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- filesystem provision-and-run",
                "bash scripts/ci/test-filesystem-matrix.sh provision-and-run",
            ),
        ),
        (
            "job-scoped-evidence",
            base.replace(
                "    strategy:\n",
                "    env:\n      EVIDENCE_PATH: ${{ runner.temp }}/filesystem-${{ matrix.profile }}.json\n    strategy:\n",
            ),
        ),
        (
            "missing-fetch",
            base.replace(
                "      - name: Fetch locked policy dependencies\n        run: cargo fetch --manifest-path tools/release-policy/Cargo.toml --locked\n",
                "",
            ),
        ),
        (
            "missing-step-evidence",
            base.replacen(
                "        env:\n          EVIDENCE_PATH: ${{ runner.temp }}/filesystem-${{ matrix.profile }}.json\n",
                "",
                1,
            ),
        ),
        (
            "workspace-evidence",
            base.replacen("${{ runner.temp }}", "${{ github.workspace }}", 1),
        ),
    ] {
        let path = root.path().join(format!("{name}.yml"));
        fs::write(&path, changed).expect("negative workflow fixture");
        let output = gate(
            root.path(),
            &[
                "filesystem",
                "self-test",
                "--workflow",
                path.to_str().unwrap(),
            ],
        );
        assert_eq!(output.status.code(), Some(1), "fixture: {name}");
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[test]
fn exact_provision_argv_dispatches_both_profiles_and_fails_ineligible() {
    let root = TempDir::new().expect("temporary root");
    for profile in [NFS, SMB] {
        let path = root.path().join(format!("{profile}.json"));
        let output = gate(
            root.path(),
            &[
                "filesystem",
                "provision-and-run",
                "--profile",
                profile,
                "--evidence",
                path.to_str().expect("UTF-8 evidence path"),
            ],
        );
        assert_eq!(output.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&output.stderr).contains("missing_evidence"));
        let value = evidence(&path);
        assert_eq!(value["profile"], profile);
        assert_eq!(value["support_eligible"], false);
        assert_ne!(value["status"], "passed");
    }
}

#[test]
fn exact_run_profile_argv_captures_missing_mount_without_positive_evidence() {
    let root = TempDir::new().expect("temporary root");
    let evidence_path = root.path().join("semantic.json");
    let packages = root.path().join("packages.txt");
    let protocol = root.path().join("protocol.txt");
    fs::write(&packages, "nfs-common=1\nnfs-kernel-server=1\n").unwrap();
    fs::write(&protocol, "NFSv4.1\n").unwrap();
    let output = gate(
        root.path(),
        &[
            "filesystem",
            "run-profile",
            "--profile",
            NFS,
            "--mount-root",
            root.path().join("missing-mount").to_str().unwrap(),
            "--evidence",
            evidence_path.to_str().unwrap(),
            "--package-observations",
            packages.to_str().unwrap(),
            "--protocol-observations",
            protocol.to_str().unwrap(),
        ],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("missing_evidence"));
    assert_eq!(evidence(&evidence_path)["support_eligible"], false);
}

#[test]
fn exact_teardown_argv_cannot_upgrade_absent_or_incomplete_evidence() {
    let root = TempDir::new().expect("temporary root");
    let evidence_path = root.path().join("evidence.json");
    let absent = gate(
        root.path(),
        &[
            "filesystem",
            "teardown",
            "--profile",
            SMB,
            "--evidence",
            evidence_path.to_str().unwrap(),
        ],
    );
    assert_eq!(absent.status.code(), Some(1));
    assert!(!evidence_path.exists());

    fs::write(
        &evidence_path,
        format!(
            "{{\"schema\":\"clinker.filesystem-matrix-evidence/v1\",\"profile\":\"{SMB}\",\"status\":\"incomplete\",\"support_eligible\":false,\"cleanup_success\":false}}\n"
        ),
    )
    .unwrap();
    let incomplete = gate(
        root.path(),
        &[
            "filesystem",
            "teardown",
            "--profile",
            SMB,
            "--evidence",
            evidence_path.to_str().unwrap(),
        ],
    );
    assert_eq!(incomplete.status.code(), Some(1));
    assert_eq!(evidence(&evidence_path)["support_eligible"], false);
}

#[test]
fn unsupported_profile_is_policy_required_never_support_eligible() {
    let root = TempDir::new().expect("temporary root");
    let evidence_path = root.path().join("unsupported.json");
    let output = gate(
        root.path(),
        &[
            "filesystem",
            "provision-and-run",
            "--profile",
            "vendor-nas",
            "--evidence",
            evidence_path.to_str().unwrap(),
        ],
    );
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("policy_required"));
    if evidence_path.exists() {
        assert_eq!(evidence(&evidence_path)["support_eligible"], false);
    }
}

#[test]
fn missing_duplicate_unknown_or_incompatible_flags_fail_before_mutation() {
    let root = TempDir::new().expect("temporary root");
    let destination = root.path().join("must-not-exist.json");
    let path = destination.to_str().unwrap();
    for arguments in [
        vec!["filesystem", "provision-and-run", "--profile", NFS],
        vec![
            "filesystem",
            "provision-and-run",
            "--profile",
            NFS,
            "--profile",
            SMB,
            "--evidence",
            path,
        ],
        vec!["filesystem", "teardown", "--profile", NFS, "--unknown"],
        vec![
            "filesystem",
            "self-test",
            "--workflow",
            "ci.yml",
            "--evidence",
            path,
        ],
    ] {
        let output = gate(root.path(), &arguments);
        assert_eq!(output.status.code(), Some(2), "arguments: {arguments:?}");
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        assert!(!destination.exists());
    }
}

#[test]
fn mount_contract_requires_exact_loopback_protocol_and_lock_options() {
    for (profile, observation) in [
        (
            NFS,
            "127.0.0.1:/ nfs4 rw,vers=4.1,proto=tcp,hard,local_lock=none",
        ),
        (
            SMB,
            "//127.0.0.1/clinker cifs rw,vers=3.1.1,cache=strict,strictsync,noperm",
        ),
    ] {
        clinker_release_policy::filesystem::validate_mount_observation(profile, observation)
            .expect("qualified mount observation");
    }
    for (profile, observation) in [
        (NFS, "remote:/ nfs4 rw,vers=4.1,proto=tcp,hard"),
        (NFS, "127.0.0.1:/ nfs4 rw,vers=4.0,proto=tcp,hard"),
        (NFS, "127.0.0.1:/ nfs4 rw,vers=4.1,proto=udp,hard"),
        (
            NFS,
            "127.0.0.1:/ nfs4 rw,vers=4.1,proto=tcp,hard,local_lock=posix",
        ),
        (SMB, "//remote/clinker cifs rw,vers=3.1.1,cache=strict"),
        (SMB, "//127.0.0.1/clinker cifs rw,vers=3.0,cache=strict"),
        (SMB, "//127.0.0.1/clinker cifs rw,vers=3.1.1,cache=loose"),
        (
            SMB,
            "//127.0.0.1/clinker cifs rw,vers=3.1.1,cache=strict,strictsync",
        ),
        (
            SMB,
            "//127.0.0.1/clinker cifs rw,vers=3.1.1,cache=strict,noperm,nostrictsync",
        ),
    ] {
        assert!(
            clinker_release_policy::filesystem::validate_mount_observation(profile, observation)
                .is_err(),
            "accepted {profile}: {observation}"
        );
    }
}

fn complete_semantic_evidence() -> Value {
    json!({
        "ci_identity": {
            "job": "filesystem-matrix",
            "repository": "rustpunk/clinker",
            "repository_revision": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "run_attempt": 1,
            "run_id": "9001",
            "workflow_path": ".github/workflows/ci.yml",
            "workflow_ref": "rustpunk/clinker/.github/workflows/ci.yml@refs/heads/main"
        },
        "cleanup_observations": [
            "post_teardown_mount=absent",
            "workspace_cleanup=pass",
            "cleanup_success=true"
        ],
        "cleanup_success": false,
        "environment_teardown": "pending",
        "injected_failures": [
            "unlisted_profile=policy_required",
            "replaced_ancestor=security_policy",
            "cross_filesystem_promotion=security_policy",
            "cancel_before_promotion=no_final",
            "child_timeout=no_passing_evidence"
        ],
        "locations": {
            "local_workspace": "/workspace",
            "mounted_share": "/runner/mount"
        },
        "lock_observations": [
            "holder=acquired",
            "competitor=blocked",
            "post_release=acquired"
        ],
        "mount": {
            "source": "127.0.0.1:/",
            "filesystem": "nfs4",
            "options": ["rw", "vers=4.1", "proto=tcp", "hard", "local_lock=none"]
        },
        "packages": ["nfs-common=1", "nfs-kernel-server=1"],
        "profile": NFS,
        "protocol_observations": ["NFSv4.1", "vers=4.1"],
        "runner": {
            "os": "Linux",
            "image_os": "ubuntu24",
            "image_version": "20260801.1",
            "kernel": "6.11"
        },
        "schema": "clinker.filesystem-matrix-evidence/v1",
        "semantic_results": {
            "confinement": "pass",
            "rename_visibility": "pass",
            "cancellation_no_final": "pass",
            "sync_durability": "pass",
            "cross_filesystem_no_copy": "pass",
            "cleanup_liveness": "pass",
            "test_filter": "remote_filesystem_matrix_semantics",
            "test_log": "semantic-test.txt"
        },
        "status": "semantic_pass",
        "support_eligible": false
    })
}

#[test]
fn qualification_replay_rejects_missing_or_forged_ci_identity() {
    let mut complete = complete_semantic_evidence();
    clinker_release_policy::filesystem::finalize_qualification(&mut complete, true)
        .expect("complete evidence may finalize");

    for field in [
        "repository",
        "repository_revision",
        "workflow_path",
        "workflow_ref",
        "run_id",
        "run_attempt",
        "job",
    ] {
        let mut changed = complete.clone();
        changed["ci_identity"]
            .as_object_mut()
            .expect("CI identity object")
            .remove(field);
        assert!(
            clinker_release_policy::filesystem::validate_passing_qualification(&changed).is_err(),
            "missing field: {field}"
        );
    }

    for (field, value) in [
        ("repository", json!("foreign/project")),
        ("repository_revision", json!("not-a-sha")),
        ("workflow_path", json!(".github/workflows/other.yml")),
        (
            "workflow_ref",
            json!("rustpunk/clinker/.github/workflows/other.yml@refs/heads/main"),
        ),
        ("run_id", json!("not-numeric")),
        ("run_attempt", json!(0)),
        ("job", json!("other-job")),
    ] {
        let mut changed = complete.clone();
        changed["ci_identity"][field] = value;
        assert!(
            clinker_release_policy::filesystem::validate_passing_qualification(&changed).is_err(),
            "forged field: {field}"
        );
    }
}

#[test]
fn positive_transition_requires_complete_semantics_and_successful_teardown() {
    let mut pending = complete_semantic_evidence();
    assert!(
        clinker_release_policy::filesystem::finalize_qualification(&mut pending, false).is_err()
    );
    assert_eq!(pending["support_eligible"], false);
    assert_eq!(pending["status"], "semantic_pass");

    let mut missing_lock = complete_semantic_evidence();
    missing_lock["lock_observations"] = json!(["holder=acquired"]);
    assert!(
        clinker_release_policy::filesystem::finalize_qualification(&mut missing_lock, true)
            .is_err()
    );
    assert_eq!(missing_lock["support_eligible"], false);

    let mut complete = complete_semantic_evidence();
    clinker_release_policy::filesystem::finalize_qualification(&mut complete, true)
        .expect("complete teardown may finalize evidence");
    assert_eq!(complete["status"], "passed");
    assert_eq!(complete["support_eligible"], true);
    assert_eq!(complete["cleanup_success"], true);
    assert_eq!(complete["environment_teardown"], "pass");
    clinker_release_policy::filesystem::validate_passing_qualification(&complete)
        .expect("complete finalized evidence must replay");

    let forged = json!({
        "schema": "clinker.filesystem-matrix-evidence/v1",
        "profile": NFS,
        "status": "passed",
        "support_eligible": true,
        "cleanup_success": true,
        "environment_teardown": "pass"
    });
    assert!(clinker_release_policy::filesystem::validate_passing_qualification(&forged).is_err());
}

#[test]
fn redundant_teardown_accepts_durable_failure_cleanup() {
    let root = TempDir::new().expect("temporary root");
    let evidence_path = root.path().join("filesystem.json");
    fs::write(
        &evidence_path,
        format!(
            "{{\"cleanup_success\":true,\"failed_step\":\"server-and-mount\",\"profile\":\"{SMB}\",\"schema\":\"clinker.filesystem-matrix-evidence/v1\",\"status\":\"failed\",\"support_eligible\":false}}"
        ),
    )
    .expect("failed evidence");

    let output = gate(
        root.path(),
        &[
            "filesystem",
            "teardown",
            "--profile",
            SMB,
            "--evidence",
            evidence_path.to_str().expect("UTF-8 evidence path"),
        ],
    );

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        output.stdout,
        b"Filesystem failure cleanup already verified.\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn bounded_child_accepts_only_the_filesystem_semantic_environment_additions() {
    let mut environment = BTreeMap::new();
    for (name, value) in [
        ("CLINKER_FILESYSTEM_PROFILE", NFS),
        ("CLINKER_FILESYSTEM_ROOT", "/tmp/mount"),
        ("CARGO_INCREMENTAL", "0"),
        ("CARGO_BUILD_JOBS", "1"),
    ] {
        environment.insert(OsString::from(name), OsString::from(value));
    }
    let output = clinker_release_policy::child::run(clinker_release_policy::child::ChildSpec {
        program: PathBuf::from("/usr/bin/env"),
        arguments: Vec::new(),
        environment,
        timeout: Duration::from_secs(5),
        output_limit: 4096,
    })
    .expect("filesystem environment must be allowlisted");
    assert_eq!(
        output.termination,
        clinker_release_policy::child::Termination::Exited(Some(0))
    );
    let stdout = String::from_utf8(output.stdout).expect("environment output");
    for name in [
        "CLINKER_FILESYSTEM_PROFILE=",
        "CLINKER_FILESYSTEM_ROOT=",
        "CARGO_INCREMENTAL=0",
        "CARGO_BUILD_JOBS=1",
    ] {
        assert!(stdout.contains(name));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn byte_range_lock_proof_observes_competition_and_release() {
    let root = TempDir::new().expect("lock fixture directory");
    assert_eq!(
        clinker_release_policy::filesystem::byte_range_lock_observation(root.path())
            .expect("byte-range proof"),
        [
            "holder=acquired",
            "competitor=blocked",
            "post_release=acquired"
        ]
    );
    assert!(
        fs::read_dir(root.path())
            .expect("lock fixture listing")
            .next()
            .is_none()
    );
}
