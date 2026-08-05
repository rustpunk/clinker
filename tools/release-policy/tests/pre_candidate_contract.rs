use std::cell::Cell;
use std::collections::BTreeSet;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{Value, json};
use tempfile::TempDir;

const SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const AUTHORITY: &str = "61a9eead1082ea72871ac09dabca73e9cde6deaf00db920cdd4587944d4aa0a0";
const NFS: &str = "linux-nfsv4.1-loopback-ci";
const SMB: &str = "linux-smb3.1.1-loopback-ci";
const NFS_EVIDENCE: &str = "target/release-policy/filesystem-linux-nfsv4.1-loopback-ci.json";
const SMB_EVIDENCE: &str = "target/release-policy/filesystem-linux-smb3.1.1-loopback-ci.json";

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("detached manifest must be beneath repository root")
        .to_path_buf()
}

fn gate(root: &Path, path: Option<&str>, arguments: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"));
    command.current_dir(root).args(arguments);
    if let Some(path) = path {
        command.env("PATH", path);
    }
    command
        .output()
        .expect("clinker-release-policy must execute")
}

fn fixture(failing_fragment: Option<&str>) -> (TempDir, String) {
    let root = tempfile::tempdir().expect("temporary gate repository");
    fs::create_dir_all(root.path().join("bin")).expect("fake executable directory");
    fs::create_dir_all(root.path().join("target/release-policy"))
        .expect("evidence fixture directory");
    write_json(
        &root
            .path()
            .join("target/release-policy/repository-controls-evidence.json"),
        &json!({
            "schema": "clinker.repository-controls-evidence/v1",
            "repository": "rustpunk/clinker",
            "release_status": "incomplete",
            "completion_eligible": false,
            "readback": {"controls": "verified"}
        }),
    );
    write_filesystem_evidence(root.path(), NFS, NFS_EVIDENCE);
    write_filesystem_evidence(root.path(), SMB, SMB_EVIDENCE);
    for program in ["cargo", "bash"] {
        install_program(root.path(), program, failing_fragment);
    }
    install_git(root.path());
    let path = format!(
        "{}:{}",
        root.path().join("bin").display(),
        std::env::var("PATH").expect("test PATH")
    );
    (root, path)
}

fn pre_candidate(root: &Path, path: &str) -> Output {
    pre_candidate_with_deadline(root, path, "3600")
}

fn pre_candidate_with_deadline(root: &Path, path: &str, deadline: &str) -> Output {
    gate(
        root,
        Some(path),
        &[
            "gate",
            "run",
            "--stage",
            "pre-candidate",
            "--rust-command-deadline-seconds",
            deadline,
            "--repository-controls-evidence",
            "target/release-policy/repository-controls-evidence.json",
            "--evidence-manifest",
            "target/release-policy/pre-candidate-evidence.json",
        ],
    )
}

#[test]
fn pre_candidate_requires_the_published_3600_second_deadline() {
    let (root, path) = fixture(None);
    let output = pre_candidate_with_deadline(root.path(), &path, "3599");
    assert_eq!(output.status.code(), Some(2));
    assert!(
        !root
            .path()
            .join("target/release-policy/pre-candidate-evidence.json")
            .exists()
    );
}

#[test]
fn exact_pre_candidate_argv_aggregates_green_checks_without_completing() {
    let (root, path) = fixture(None);
    let output = pre_candidate(root.path(), &path);
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        output.stdout,
        b"Pre-candidate release policy passed with incomplete evidence\n"
    );
    assert!(output.stderr.is_empty());
    let manifest = read_json(
        &root
            .path()
            .join("target/release-policy/pre-candidate-evidence.json"),
    );
    assert_eq!(manifest["schema"], "clinker.pre-candidate-evidence/v1");
    assert_eq!(manifest["stage"], "pre-candidate");
    assert_eq!(manifest["release_status"], "incomplete");
    assert_eq!(manifest["completion_eligible"], false);
    assert_eq!(manifest["repository_revision"], SHA);
    assert!(manifest["failures"].as_array().is_some_and(Vec::is_empty));
    let requirements = manifest["requirements"]
        .as_array()
        .expect("requirement inventory");
    let expected = BTreeSet::from([
        "DIST-01", "DIST-02", "ORCH-01", "ORCH-02", "ORCH-03", "ORCH-04", "SECU-01", "SECU-02",
        "SECU-03", "SECU-04",
    ]);
    assert_eq!(requirements.len(), expected.len());
    assert_eq!(
        requirements
            .iter()
            .map(|value| value.as_str().expect("requirement identifier"))
            .collect::<BTreeSet<_>>(),
        expected
    );

    let checks = manifest["checks"].as_array().expect("check observations");
    for name in ["workspace-test-offline", "workspace-test-ci"] {
        let check = checks
            .iter()
            .find(|check| check["name"] == name)
            .expect("workspace test observation");
        assert_eq!(check["descriptor_limit"]["floor"], 65_536);
        assert_eq!(
            check["descriptor_limit"]["post_hard"],
            check["descriptor_limit"]["pre_hard"]
        );
        assert!(
            check["descriptor_limit"]["post_soft"]
                .as_u64()
                .is_some_and(|value| value >= 65_536)
        );
    }
    assert!(
        checks
            .iter()
            .filter(|check| !matches!(
                check["name"].as_str(),
                Some("workspace-test-offline" | "workspace-test-ci")
            ))
            .all(|check| check.get("descriptor_limit").is_none())
    );

    assert_eq!(invocations(root.path()), expected_invocations());
    assert_eq!(
        manifest["filesystem_evidence_sha256"]
            .as_object()
            .map(|hashes| hashes.len()),
        Some(2)
    );
    for profile in [NFS, SMB] {
        assert!(
            manifest["filesystem_evidence_sha256"][profile]
                .as_str()
                .is_some_and(|digest| digest.len() == 64)
        );
    }
}

#[test]
fn pre_candidate_retains_stable_failures_and_never_false_greens() {
    let (root, path) = fixture(Some("clippy --workspace --all-targets --locked --offline"));
    let output = pre_candidate(root.path(), &path);
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
    let manifest = read_json(
        &root
            .path()
            .join("target/release-policy/pre-candidate-evidence.json"),
    );
    assert_eq!(manifest["release_status"], "incomplete");
    assert_eq!(manifest["completion_eligible"], false);
    assert!(manifest["failures"].as_array().is_some_and(|failures| {
        failures
            .iter()
            .any(|failure| failure["check"] == "workspace-clippy-all-targets-offline")
    }));
    let checks = manifest["checks"].as_array().expect("check observations");
    let failed = checks
        .iter()
        .position(|check| check["status"] == "failed")
        .expect("one failed observation");
    assert!(
        checks
            .iter()
            .skip(failed + 1)
            .any(|check| check["status"] == "passed")
    );
}

#[test]
fn pre_candidate_rejects_completing_or_missing_repository_evidence() {
    let (root, path) = fixture(None);
    let repository = root
        .path()
        .join("target/release-policy/repository-controls-evidence.json");
    let mut evidence = read_json(&repository);
    evidence["release_status"] = Value::String("complete".to_owned());
    evidence["completion_eligible"] = Value::Bool(true);
    write_json(&repository, &evidence);
    let completing = pre_candidate(root.path(), &path);
    assert_eq!(completing.status.code(), Some(1));
    let manifest = read_json(
        &root
            .path()
            .join("target/release-policy/pre-candidate-evidence.json"),
    );
    assert_eq!(manifest["completion_eligible"], false);

    let (missing_root, missing_path) = fixture(None);
    fs::remove_file(
        missing_root
            .path()
            .join("target/release-policy/repository-controls-evidence.json"),
    )
    .expect("remove repository evidence fixture");
    let missing = pre_candidate(missing_root.path(), &missing_path);
    assert_eq!(missing.status.code(), Some(1));
    assert!(
        missing_root
            .path()
            .join("target/release-policy/pre-candidate-evidence.json")
            .is_file()
    );
}

#[test]
fn pre_candidate_rejects_missing_stale_foreign_mismatched_or_forged_remote_evidence() {
    for mutation in [
        "missing",
        "stale",
        "foreign",
        "mismatched-run",
        "incomplete",
        "forged",
        "duplicate-profile",
        "symlink",
    ] {
        let (root, path) = fixture(None);
        let nfs = root.path().join(NFS_EVIDENCE);
        let smb = root.path().join(SMB_EVIDENCE);
        match mutation {
            "missing" => fs::remove_file(&nfs).expect("remove NFS evidence"),
            "stale" => mutate_json(&nfs, |value| {
                value["ci_identity"]["repository_revision"] = Value::String("b".repeat(40));
            }),
            "foreign" => mutate_json(&nfs, |value| {
                value["ci_identity"]["repository"] = Value::String("foreign/project".to_owned());
            }),
            "mismatched-run" => mutate_json(&smb, |value| {
                value["ci_identity"]["run_id"] = Value::String("9002".to_owned());
            }),
            "incomplete" => mutate_json(&nfs, |value| {
                value["status"] = Value::String("semantic_pass".to_owned());
                value["support_eligible"] = Value::Bool(false);
            }),
            "forged" => write_canonical_json(
                &nfs,
                &json!({
                    "schema": "clinker.filesystem-matrix-evidence/v1",
                    "profile": NFS,
                    "status": "passed",
                    "support_eligible": true,
                    "cleanup_success": true,
                    "environment_teardown": "pass",
                }),
            ),
            "duplicate-profile" => mutate_json(&smb, |value| {
                value["profile"] = Value::String(NFS.to_owned());
            }),
            "symlink" => {
                fs::remove_file(&nfs).expect("remove evidence before symlink");
                symlink(&smb, &nfs).expect("symlink evidence fixture");
            }
            _ => unreachable!(),
        }
        let output = pre_candidate(root.path(), &path);
        assert_eq!(output.status.code(), Some(1), "mutation: {mutation}");
        let manifest = read_json(
            &root
                .path()
                .join("target/release-policy/pre-candidate-evidence.json"),
        );
        assert_eq!(manifest["release_status"], "incomplete");
        assert_eq!(manifest["completion_eligible"], false);
        assert!(manifest["failures"].as_array().is_some_and(|failures| {
            failures.iter().any(|failure| {
                failure["check"] == "filesystem-nfs-evidence"
                    || failure["check"] == "filesystem-smb-evidence"
                    || failure["check"] == "filesystem-evidence-coherence"
            })
        }));
    }
}

#[test]
fn descriptor_floor_model_is_raise_only_and_skips_non_test_commands() {
    let set_soft = Cell::new(0_u64);
    let set_hard = Cell::new(0_u64);
    let observed_soft = Cell::new(1024_u64);
    let raised = clinker_release_policy::cli::apply_nofile_floor_with(
        true,
        || Ok((observed_soft.get(), 70_000)),
        |soft, hard| {
            set_soft.set(soft);
            set_hard.set(hard);
            observed_soft.set(soft);
            Ok(())
        },
    )
    .expect("sufficient hard limit can be raised")
    .expect("test command has descriptor observation");
    assert_eq!(set_soft.get(), 65_536);
    assert_eq!(set_hard.get(), 70_000);
    assert_eq!(raised.pre_soft, 1024);
    assert_eq!(raised.post_soft, 65_536);
    assert_eq!(raised.pre_hard, raised.post_hard);

    let already_high = clinker_release_policy::cli::apply_nofile_floor_with(
        true,
        || Ok((80_000, 90_000)),
        |_, _| -> Result<(), String> { panic!("already-high limit must not be changed") },
    )
    .expect("already-high limit succeeds")
    .expect("test command has descriptor observation");
    assert_eq!(already_high.pre_soft, 80_000);
    assert_eq!(already_high.post_soft, 80_000);

    assert!(
        clinker_release_policy::cli::apply_nofile_floor_with(
            true,
            || Ok((1024, 4096)),
            |_, _| -> Result<(), String> { panic!("insufficient hard limit cannot be changed") },
        )
        .is_err()
    );

    assert!(
        clinker_release_policy::cli::apply_nofile_floor_with(
            false,
            || -> Result<(u64, u64), String> { panic!("non-test command must not inspect limits") },
            |_, _| -> Result<(), String> { panic!("non-test command must not change limits") },
        )
        .expect("non-test command skips the prelude")
        .is_none()
    );
}

#[test]
fn exact_final_argv_alone_creates_complete_assertable_evidence() {
    let root = final_fixture();
    let output = final_gate(root.path());
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        output.stdout,
        b"Final release evidence reconciliation passed\n"
    );
    assert!(output.stderr.is_empty());

    let final_path = root.path().join("final-evidence.json");
    let final_value = read_json(&final_path);
    assert_eq!(final_value["schema"], "clinker.final-evidence/v1");
    assert_eq!(final_value["stage"], "final");
    assert_eq!(final_value["release_status"], "complete");
    assert_eq!(final_value["completion_eligible"], true);
    let assertion = gate(
        root.path(),
        None,
        &[
            "evidence",
            "assert-complete",
            "--manifest",
            final_path.to_str().expect("final path UTF-8"),
        ],
    );
    assert_eq!(assertion.status.code(), Some(0));
    assert_eq!(assertion.stdout, b"Release completion evidence verified\n");
}

#[test]
fn final_rejects_stale_or_completing_producer_state_and_assertion_rejects_producers() {
    for mutation in [
        "publication-completing",
        "publication-stale",
        "authority-drift",
    ] {
        let root = final_fixture();
        let publication_path = root.path().join("publication.json");
        let mut publication = read_json(&publication_path);
        match mutation {
            "publication-completing" => {
                publication["release_status"] = Value::String("complete".to_owned());
                publication["completion_eligible"] = Value::Bool(true);
            }
            "publication-stale" => publication["revision"] = Value::from(3),
            "authority-drift" => {
                publication["candidate_authorization_sha256"] = Value::String("9".repeat(64));
            }
            _ => unreachable!(),
        }
        write_json(&publication_path, &publication);
        let output = final_gate(root.path());
        assert_eq!(output.status.code(), Some(1), "mutation: {mutation}");
        assert!(!root.path().join("final-evidence.json").exists());
    }

    let root = final_fixture();
    for name in ["pre.json", "candidate.json", "publication.json"] {
        let output = gate(
            root.path(),
            None,
            &[
                "evidence",
                "assert-complete",
                "--manifest",
                root.path()
                    .join(name)
                    .to_str()
                    .expect("producer path UTF-8"),
            ],
        );
        assert_eq!(output.status.code(), Some(1), "producer: {name}");
    }
}

#[test]
fn final_rejects_forged_or_incomplete_pre_candidate_inventory() {
    for mutation in [
        "token-check",
        "undefined-requirement",
        "missing-filesystem-hashes",
        "invalid-filesystem-hash",
        "missing-descriptor-observation",
    ] {
        let root = final_fixture();
        let path = root.path().join("pre.json");
        let mut pre = read_json(&path);
        match mutation {
            "token-check" => pre["checks"] = json!([{"name": "all", "status": "passed"}]),
            "undefined-requirement" => pre["requirements"]
                .as_array_mut()
                .expect("requirements array")
                .push(json!("DIST-03")),
            "missing-filesystem-hashes" => {
                pre.as_object_mut()
                    .expect("pre-candidate object")
                    .remove("filesystem_evidence_sha256");
            }
            "invalid-filesystem-hash" => {
                pre["filesystem_evidence_sha256"][NFS] = json!("not-a-digest");
            }
            "missing-descriptor-observation" => {
                let check = pre["checks"]
                    .as_array_mut()
                    .expect("checks array")
                    .iter_mut()
                    .find(|check| check["name"] == "workspace-test-offline")
                    .expect("workspace test check");
                check
                    .as_object_mut()
                    .expect("check object")
                    .remove("descriptor_limit");
            }
            _ => unreachable!(),
        }
        write_canonical_json(&path, &pre);
        let output = final_gate(root.path());
        assert_eq!(output.status.code(), Some(1), "mutation: {mutation}");
        assert!(!root.path().join("final-evidence.json").exists());
    }
}

fn final_gate(root: &Path) -> Output {
    gate(
        root,
        None,
        &[
            "gate",
            "run",
            "--stage",
            "final",
            "--authorization-record",
            "authorization.json",
            "--authorization-schema",
            "authorization-schema.json",
            "--decision-record",
            "decision.json",
            "--decision-schema",
            "decision-schema.json",
            "--pre-candidate-manifest",
            "pre.json",
            "--candidate-evidence",
            "candidate.json",
            "--publication-evidence",
            "publication.json",
            "--evidence-manifest",
            "final-evidence.json",
        ],
    )
}

fn final_fixture() -> TempDir {
    let (root, path) = fixture(None);
    let pre_candidate_output = pre_candidate(root.path(), &path);
    assert_eq!(pre_candidate_output.status.code(), Some(0));
    fs::rename(
        root.path()
            .join("target/release-policy/pre-candidate-evidence.json"),
        root.path().join("pre.json"),
    )
    .expect("retain semantically valid pre-candidate evidence");
    let source = repository_root();
    fs::copy(
        source.join("scripts/release/release-candidate-authorization.schema.json"),
        root.path().join("authorization-schema.json"),
    )
    .expect("copy authorization schema");
    fs::copy(
        source.join("scripts/release/release-decision.schema.json"),
        root.path().join("decision-schema.json"),
    )
    .expect("copy decision schema");
    let authorizations = read_json(
        &source.join("scripts/release/fixtures/release-decisions/candidate-authorizations.json"),
    );
    write_json(
        &root.path().join("authorization.json"),
        &authorizations["authorized"],
    );
    let accepted = read_json(
        &source.join("scripts/release/fixtures/release-decisions/accepted-record-set.json"),
    );
    let decision = accepted["records"]
        .as_array()
        .and_then(|records| {
            records
                .iter()
                .find(|record| record["decision_id"] == "release-candidate")
        })
        .expect("accepted candidate decision");
    write_json(&root.path().join("decision.json"), decision);

    let candidate = json!({
        "schema": "clinker.candidate-evidence/v1",
        "kind": "candidate",
        "state": "candidate-verified",
        "revision": 0,
        "release_status": "incomplete",
        "completion_eligible": false,
        "immutable_authority_sha256": AUTHORITY,
        "candidate_authorization_sha256": AUTHORITY,
        "candidate_tag": "v3.0.0",
        "source_sha": SHA,
        "candidate_release_id": "release-300",
        "checksum_sha256": "c".repeat(64),
        "archive_digests": {
            "aarch64-apple-darwin": "d".repeat(64),
            "x86_64-apple-darwin": "e".repeat(64),
            "x86_64-pc-windows-msvc": "f".repeat(64),
            "x86_64-unknown-linux-gnu": "0".repeat(64),
        },
        "archives": ["a", "b", "c", "d"],
        "attestations": ["a", "b", "c", "d"],
    });
    let publication = json!({
        "schema": "clinker.publication-evidence/v1",
        "kind": "publication",
        "state": "public-verified",
        "revision": 4,
        "release_status": "incomplete",
        "completion_eligible": false,
        "immutable_authority_sha256": AUTHORITY,
        "candidate_authorization_sha256": AUTHORITY,
        "candidate_authorization_blob_sha": "1".repeat(40),
        "approval_record_blob_sha": "2".repeat(40),
        "approval_record_sha256": "3".repeat(64),
        "candidate": candidate,
        "dispatch": {
            "dispatch_id": "dispatch-300",
            "run_id": "300",
            "run_attempt": 1,
            "job_id": "publish",
            "environment": "release",
        },
        "inspection": {"status": "completed"},
        "approval": {"approval_kind": "manual", "automated_approval": false},
        "protected_job": {"status": "success", "wait_mode": "read-only"},
        "public_verification": {"status": "verified", "immutable_release": true, "asset_count": 4},
    });
    write_json(&root.path().join("candidate.json"), &candidate);
    write_json(&root.path().join("publication.json"), &publication);
    root
}

fn install_program(root: &Path, name: &str, failing_fragment: Option<&str>) {
    let path = root.join("bin").join(name);
    let failure = failing_fragment.unwrap_or("never-match");
    fs::write(
        &path,
        format!(
            "#!/bin/bash\nset -euo pipefail\nprintf '%s' {name:?} >> target/release-policy/invocations.tsv\nprintf '\\t%s' \"$@\" >> target/release-policy/invocations.tsv\nprintf '\\n' >> target/release-policy/invocations.tsv\nif [[ \"$*\" == *{failure:?}* ]]; then exit 7; fi\nprintf 'ok\\n'\n"
        ),
    )
    .expect("fake executable");
    executable(&path);
}

fn install_git(root: &Path) {
    let path = root.join("bin/git");
    fs::write(
        &path,
        format!("#!/bin/bash\nset -euo pipefail\nprintf '%s' git >> target/release-policy/invocations.tsv\nprintf '\\t%s' \"$@\" >> target/release-policy/invocations.tsv\nprintf '\\n' >> target/release-policy/invocations.tsv\nprintf '%s\\n' {SHA}\n"),
    )
    .expect("fake git executable");
    executable(&path);
}

fn executable(path: &Path) {
    let mut permissions = fs::metadata(path)
        .expect("fake executable metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("fake executable mode");
}

fn write_json(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("JSON fixture directory");
    }
    fs::write(
        path,
        serde_json::to_vec_pretty(value).expect("serialize JSON fixture"),
    )
    .expect("write JSON fixture");
}

fn write_canonical_json(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("canonical fixture directory");
    }
    let encoded = serde_json::to_vec(value).expect("encode canonical fixture input");
    let canonical = clinker_release_policy::canonical::parse_json(&encoded)
        .expect("parse canonical fixture input");
    fs::write(
        path,
        clinker_release_policy::canonical::to_bytes(&canonical).expect("canonical fixture bytes"),
    )
    .expect("write canonical fixture");
}

fn mutate_json(path: &Path, mutation: impl FnOnce(&mut Value)) {
    let mut value = read_json(path);
    mutation(&mut value);
    write_canonical_json(path, &value);
}

fn write_filesystem_evidence(root: &Path, profile: &str, relative_path: &str) {
    let (packages, protocol, source, filesystem, options) = if profile == NFS {
        (
            json!(["nfs-common=1", "nfs-kernel-server=1"]),
            json!(["NFSv4.1", "vers=4.1"]),
            "127.0.0.1:/",
            "nfs4",
            json!(["rw", "vers=4.1", "proto=tcp", "hard", "local_lock=none"]),
        )
    } else {
        (
            json!(["cifs-utils=1", "samba=1"]),
            json!(["SMB3.1.1", "dialect 311"]),
            "//127.0.0.1/clinker",
            "cifs",
            json!(["rw", "vers=3.1.1", "cache=strict", "strictsync", "noperm"]),
        )
    };
    write_canonical_json(
        &root.join(relative_path),
        &json!({
            "admission_lock_results": {
                "api": "RunAttemptPublication::create",
                "count_limit": {
                    "bounded_completion": "pass",
                    "estimated_attempt_bytes": 100,
                    "exactly_one_admitted": "pass",
                    "independent_processes": "pass",
                    "mounted_root_readback": "pass",
                    "opposite_root_order": "pass",
                    "retained_attempt_limit": 1
                },
                "lock": "fs4::FileExt::lock",
                "retained_byte_limit": {
                    "bounded_completion": "pass",
                    "estimated_attempt_bytes": 100,
                    "exactly_one_admitted": "pass",
                    "independent_processes": "pass",
                    "mounted_root_readback": "pass",
                    "opposite_root_order": "pass",
                    "retained_byte_limit": 150
                }
            },
            "ci_identity": {
                "job": "filesystem-matrix",
                "repository": "rustpunk/clinker",
                "repository_revision": SHA,
                "run_attempt": 1,
                "run_id": "9001",
                "workflow_path": ".github/workflows/ci.yml",
                "workflow_ref": "rustpunk/clinker/.github/workflows/ci.yml@refs/heads/main",
            },
            "cleanup_observations": [
                "post_teardown_mount=absent",
                "post_teardown_backing_mount=absent",
                "workspace_cleanup=pass",
                "cleanup_success=true"
            ],
            "cleanup_success": true,
            "environment_teardown": "pass",
            "injected_failures": [
                "unlisted_profile=policy_required",
                "replaced_ancestor=security_policy",
                "cross_filesystem_promotion=security_policy",
                "cancel_before_promotion=no_final",
                "child_timeout=no_passing_evidence"
            ],
            "locations": {"local_workspace": "repository_workspace", "mounted_share": "profile_mount_root"},
            "lock_observations": ["holder=acquired", "competitor=blocked", "post_release=acquired"],
            "mount": {"source": source, "filesystem": filesystem, "options": options},
            "packages": packages,
            "profile": profile,
            "protocol_observations": protocol,
            "runner": {"os": "Linux", "image_os": "ubuntu24", "image_version": "20260801.1", "kernel": "6.11"},
            "capacity_results": {
                "backing": "mounted_tmpfs_64_mib",
                "edquot_seam": "seam_covered",
                "enospc_final_absent": "pass",
                "enospc_manifest_state": "staging",
                "enospc_operator_cleanup": "pass",
                "enospc_raw_os_error": 28,
                "mounted_enospc": "pass",
                "quota": "seam_covered"
            },
            "edge_outcomes": {
                "cancellation_no_final": "pass",
                "cleanup_liveness": "pass",
                "confinement": "pass",
                "cross_filesystem_no_copy": "pass",
                "rename_visibility": "pass",
                "sync_durability": "pass"
            },
            "prohibitions": [
                "copy_fallback_to_visible_final=absent",
                "publication_mode_fallback=absent",
                "cross_artifact_atomicity_claim=absent",
                "cross_execution_staging_ownership=absent",
                "raw_deletion_path_authority=absent"
            ],
            "publication_results": {
                "lifecycle_classes": [
                    "success", "ordinary_failure", "interruption",
                    "ambiguity_durability_uncertainty", "purge_cleanup", "support_eligibility"
                ],
                "modes": ["direct", "local_then_publish"],
                "operator_results": [
                    "list=pass", "inspect=pass", "purge_preview=pass",
                    "purge_execute=pass", "cleanup_debt=none"
                ],
                "persistence_results": [
                    "ordinary_failure=retained_manifest",
                    "interruption=retained_manifest",
                    "ambiguity_durability_uncertainty=retained_manifest"
                ],
                "recovery_results": [
                    "direct:file_synchronization=recovered_revalidated_completed_manifest_reopened",
                    "direct:rename=recovered_revalidated_completed_manifest_reopened",
                    "direct:parent_directory_synchronization=recovered_revalidated_completed_manifest_reopened",
                    "local_then_publish:copy=recovered_revalidated_completed_manifest_reopened",
                    "local_then_publish:file_synchronization=recovered_revalidated_completed_manifest_reopened",
                    "local_then_publish:rename=recovered_revalidated_completed_manifest_reopened",
                    "local_then_publish:parent_directory_synchronization=recovered_revalidated_completed_manifest_reopened"
                ],
                "stage_results": [
                    "direct:file_synchronization=interrupted_retained",
                    "direct:rename=interrupted_retained",
                    "direct:parent_directory_synchronization=interrupted_retained",
                    "local_then_publish:copy=interrupted_retained",
                    "local_then_publish:file_synchronization=interrupted_retained",
                    "local_then_publish:rename=interrupted_retained",
                    "local_then_publish:parent_directory_synchronization=interrupted_retained"
                ],
                "success_results": [
                    "direct=pre_cleanup_final_and_complete_manifest,post_cleanup_final_present_attempt_absent",
                    "local_then_publish=pre_cleanup_final_and_complete_manifest,post_cleanup_final_present_attempt_absent"
                ],
                "test_filter": "remote_filesystem_publication_matrix",
                "test_log": "publication-test.txt"
            },
            "schema": "clinker.filesystem-matrix-evidence/3",
            "status": "passed",
            "support_eligible": true
        }),
    );
}

fn invocations(root: &Path) -> Vec<String> {
    fs::read_to_string(root.join("target/release-policy/invocations.tsv"))
        .expect("invocation log")
        .lines()
        .map(str::to_owned)
        .collect()
}

fn expected_invocations() -> Vec<String> {
    let expected = vec![
        "git\trev-parse\tHEAD".to_owned(),
        "cargo\trun\t--quiet\t--manifest-path\ttools/dependency-policy/Cargo.toml\t--target-dir\ttarget/clinker-release-policy-pre-candidate-dependency\t--locked\t--offline\t--\t--scope\tfinal\t--root\t.".to_owned(),
        "cargo\trun\t--quiet\t--manifest-path\ttools/release-policy/Cargo.toml\t--locked\t--offline\t--\tworkflow\tverify".to_owned(),
        "cargo\trun\t--quiet\t--manifest-path\ttools/release-policy/Cargo.toml\t--locked\t--offline\t--\trepository\tverify\t--config-only".to_owned(),
        "cargo\trun\t--quiet\t--manifest-path\ttools/release-policy/Cargo.toml\t--locked\t--offline\t--\tinventory\tcheck".to_owned(),
        "cargo\ttest\t--manifest-path\ttools/release-policy/Cargo.toml\t--locked\t--offline\t--test\tdecision_contract".to_owned(),
        "cargo\ttest\t--manifest-path\ttools/release-policy/Cargo.toml\t--locked\t--offline\t--test\trelease_contract".to_owned(),
        "cargo\trun\t--quiet\t--manifest-path\ttools/release-policy/Cargo.toml\t--locked\t--offline\t--\tfilesystem\tself-test".to_owned(),
        "cargo\ttest\t--locked\t-p\tclinker\t--test\toutput_containment".to_owned(),
        "cargo\tfmt\t--all\t--\t--check".to_owned(),
        "cargo\tcheck\t--workspace\t--locked\t--offline".to_owned(),
        "cargo\tclippy\t--workspace\t--locked\t--offline\t--\t-D\twarnings".to_owned(),
        "cargo\tclippy\t--workspace\t--all-targets\t--locked\t--offline\t--\t-D\twarnings".to_owned(),
        "cargo\ttest\t--workspace\t--locked\t--offline".to_owned(),
        "cargo\tclippy\t--workspace\t--\t-D\twarnings".to_owned(),
        "cargo\tclippy\t--workspace\t--all-targets\t--\t-D\twarnings".to_owned(),
        "cargo\ttest\t--workspace".to_owned(),
        "cargo\tdeny\t--locked\tcheck".to_owned(),
        "bash\tscripts/check-ai-docs.sh".to_owned(),
        "git\tdiff\t--check".to_owned(),
    ];
    expected
}

fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("read JSON fixture")).expect("parse JSON fixture")
}
