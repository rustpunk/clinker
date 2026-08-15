use std::collections::BTreeMap;
use std::ffi::OsString;
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use tempfile::TempDir;

const DECISION_SCHEMA: &str = "scripts/release/release-decision.schema.json";
const AUTHORIZATION_SCHEMA: &str = "scripts/release/release-candidate-authorization.schema.json";
const ACCEPTED_FIXTURE: &str =
    "scripts/release/fixtures/release-decisions/accepted-record-set.json";
const AUTHORIZATION_FIXTURE: &str =
    "scripts/release/fixtures/release-decisions/candidate-authorizations.json";
const VECTOR_FIXTURE: &str = "scripts/release/fixtures/release-policy-vectors.json";

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root must resolve")
}

fn fixture(path: &str) -> Value {
    let bytes = fs::read(repository_root().join(path)).expect("fixture must be readable");
    serde_json::from_slice(&bytes).expect("fixture must be valid JSON")
}

fn write_json(directory: &TempDir, name: &str, value: &Value) -> PathBuf {
    let path = directory.path().join(name);
    fs::write(
        &path,
        serde_json::to_vec_pretty(value).expect("fixture JSON must serialize"),
    )
    .expect("temporary fixture must be writable");
    path
}

fn write_canonical_json(directory: &TempDir, name: &str, value: &Value) -> PathBuf {
    let path = directory.path().join(name);
    let parsed = clinker_release_policy::canonical::parse_json(&serde_json::to_vec(value).unwrap())
        .expect("fixture JSON must canonicalize");
    fs::write(
        &path,
        clinker_release_policy::canonical::to_bytes(&parsed).expect("canonical fixture bytes"),
    )
    .expect("temporary fixture must be writable");
    path
}

fn gate(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
        .current_dir(repository_root())
        .args(arguments)
        .output()
        .expect("clinker-release-policy must execute")
}

fn decision_args(record: &Path) -> Vec<&str> {
    vec![
        "decision",
        "validate",
        "--schema",
        DECISION_SCHEMA,
        "--record",
        record.to_str().expect("temporary path must be UTF-8"),
    ]
}

#[test]
fn frozen_canonical_vector_matches_bytes_and_both_digests() {
    let vectors = fixture(VECTOR_FIXTURE);
    let canonical = &vectors["canonical"];
    let input = serde_json::to_vec(&canonical["input"]).expect("input must serialize");
    let parsed =
        clinker_release_policy::canonical::parse_json(&input).expect("integer JSON must parse");
    let bytes =
        clinker_release_policy::canonical::to_bytes(&parsed).expect("canonicalization must pass");

    assert_eq!(bytes, canonical["bytes_utf8"].as_str().unwrap().as_bytes());
    assert_eq!(
        clinker_release_policy::digest::git_blob_sha1_hex(&bytes),
        canonical["git_blob_sha1"].as_str().unwrap()
    );
    assert_eq!(
        clinker_release_policy::digest::sha256_hex(&bytes),
        canonical["sha256"].as_str().unwrap()
    );
}

#[test]
fn canonical_parser_rejects_floats_non_finite_numbers_and_duplicate_keys() {
    let vectors = fixture(VECTOR_FIXTURE);
    let rejected = vectors["rejected_json"]
        .as_array()
        .expect("rejected vectors must be an array");

    for case in rejected {
        let input = case["json"].as_str().expect("rejected JSON must be text");
        assert!(
            clinker_release_policy::canonical::parse_json(input.as_bytes()).is_err(),
            "{} unexpectedly parsed",
            case["name"]
        );
    }
}

#[test]
fn decision_validate_accepts_single_repeated_and_required_record_shapes() {
    let directory = TempDir::new().expect("temporary directory must be created");
    let accepted = fixture(ACCEPTED_FIXTURE);
    let records = accepted["records"]
        .as_array()
        .expect("accepted records must be an array");
    let first = write_json(&directory, "first.json", &records[0]);
    let second = write_json(&directory, "second.json", &records[1]);

    let single = gate(&decision_args(&first));
    assert_eq!(single.status.code(), Some(0));
    assert_eq!(single.stdout, b"release decision validation passed\n");
    assert!(single.stderr.is_empty());

    let repeated = gate(&[
        "decision",
        "validate",
        "--schema",
        DECISION_SCHEMA,
        "--record",
        first.to_str().unwrap(),
        "--record",
        second.to_str().unwrap(),
        "--require-id",
        "semantic-identity",
        "--require-accepted",
    ]);
    assert_eq!(repeated.status.code(), Some(0));
    assert_eq!(repeated.stdout, b"release decision validation passed\n");
    assert!(repeated.stderr.is_empty());
}

#[test]
fn decision_validate_accepts_authorization_and_candidate_evidence_shape() {
    let directory = TempDir::new().expect("temporary directory must be created");
    let accepted = fixture(ACCEPTED_FIXTURE);
    let authorizations = fixture(AUTHORIZATION_FIXTURE);
    let authorization = write_json(
        &directory,
        "authorization.json",
        &authorizations["authorized"],
    );
    let mut evidence = accepted["candidate_evidence"].clone();
    evidence["assets"] = Value::Array(governed_assets(
        evidence["archives"].as_array().expect("candidate archives"),
    ));
    let candidate_evidence = write_json(&directory, "candidate-evidence.json", &evidence);
    let candidate = write_json(&directory, "candidate.json", &accepted["records"][6]);

    let output = gate(&[
        "decision",
        "validate",
        "--schema",
        DECISION_SCHEMA,
        "--record",
        candidate.to_str().unwrap(),
        "--authorization-schema",
        AUTHORIZATION_SCHEMA,
        "--authorization-record",
        authorization.to_str().unwrap(),
        "--candidate-evidence",
        candidate_evidence.to_str().unwrap(),
        "--require-id",
        "release-candidate",
        "--require-authorization-id",
        "release-candidate-authorization",
        "--require-authorized",
        "--require-accepted",
    ]);

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stdout, b"release decision validation passed\n");
    assert!(output.stderr.is_empty());
}

#[test]
fn decision_validate_rejects_policy_data_with_exit_one_and_bounded_stderr() {
    let directory = TempDir::new().expect("temporary directory must be created");
    let accepted = fixture(ACCEPTED_FIXTURE);
    let mut invalid_cases = vec![
        {
            let mut record = accepted["records"][0].clone();
            record["unknown"] = json!(true);
            ("unknown-field.json", record)
        },
        {
            let mut record = accepted["records"][0].clone();
            record["decision_id"] = json!("not-a-decision");
            ("invalid-id.json", record)
        },
    ];

    let authorizations = fixture(AUTHORIZATION_FIXTURE);
    let mut stale = authorizations["authorized"].clone();
    stale["candidate_authorization_sha256"] =
        json!("0000000000000000000000000000000000000000000000000000000000000000");
    let stale_path = write_json(&directory, "stale-digest.json", &stale);
    let stale_output = gate(&[
        "decision",
        "validate",
        "--authorization-schema",
        AUTHORIZATION_SCHEMA,
        "--authorization-record",
        stale_path.to_str().unwrap(),
    ]);
    assert_policy_rejection(&stale_output);

    let mut premature = authorizations["authorized"].clone();
    premature["authorization"]["candidate_release_id"] = json!("release-not-built-yet");
    let premature_path = write_json(&directory, "premature-release-id.json", &premature);
    let premature_output = gate(&[
        "decision",
        "validate",
        "--authorization-schema",
        AUTHORIZATION_SCHEMA,
        "--authorization-record",
        premature_path.to_str().unwrap(),
    ]);
    assert_policy_rejection(&premature_output);

    for (name, value) in invalid_cases.drain(..) {
        let path = write_json(&directory, name, &value);
        assert_policy_rejection(&gate(&decision_args(&path)));
    }
}

#[test]
fn decision_validate_rejects_duplicate_keys_floats_and_oversized_input() {
    let directory = TempDir::new().expect("temporary directory must be created");
    for (name, bytes) in [
        (
            "duplicate.json",
            br#"{"decision_id":"a","decision_id":"b"}"#.as_slice(),
        ),
        ("float.json", br#"{"decision_id":1.5}"#.as_slice()),
    ] {
        let path = directory.path().join(name);
        fs::write(&path, bytes).unwrap();
        assert_policy_rejection(&gate(&decision_args(&path)));
    }

    let oversized = directory.path().join("oversized.json");
    fs::write(&oversized, vec![b' '; 1_048_577]).unwrap();
    assert_policy_rejection(&gate(&decision_args(&oversized)));
}

#[test]
fn malformed_or_incompatible_invocations_exit_two_without_stdout() {
    for arguments in [
        vec!["decision", "validate"],
        vec!["decision", "validate", "--unknown"],
        vec![
            "decision",
            "validate",
            "--candidate-evidence",
            "missing.json",
        ],
        vec![
            "decision",
            "validate",
            "--schema",
            DECISION_SCHEMA,
            "--schema",
            DECISION_SCHEMA,
        ],
    ] {
        let output = gate(&arguments);
        assert_eq!(output.status.code(), Some(2), "arguments: {arguments:?}");
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        assert!(output.stderr.len() <= 1024);
    }
}

fn dependency_metadata() -> Value {
    let output = Command::new("cargo")
        .current_dir(repository_root())
        .args(["metadata", "--format-version", "1", "--locked", "--offline"])
        .output()
        .expect("cargo metadata must execute");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("cargo metadata JSON")
}

fn metadata_package_mut<'a>(
    metadata: &'a mut Value,
    name: &str,
    version: Option<&str>,
) -> &'a mut serde_json::Map<String, Value> {
    metadata["packages"]
        .as_array_mut()
        .expect("metadata packages")
        .iter_mut()
        .find_map(|package| {
            let package = package.as_object_mut()?;
            (package["name"] == name && version.is_none_or(|version| package["version"] == version))
                .then_some(package)
        })
        .expect("metadata package")
}

fn metadata_dependency_mut<'a>(
    package: &'a mut serde_json::Map<String, Value>,
    name: &str,
) -> &'a mut serde_json::Map<String, Value> {
    package["dependencies"]
        .as_array_mut()
        .expect("package dependencies")
        .iter_mut()
        .find_map(|dependency| {
            let dependency = dependency.as_object_mut()?;
            (dependency["name"] == name).then_some(dependency)
        })
        .expect("package dependency")
}

fn metadata_node_mut<'a>(
    metadata: &'a mut Value,
    id_fragment: &str,
) -> &'a mut serde_json::Map<String, Value> {
    metadata["resolve"]["nodes"]
        .as_array_mut()
        .expect("resolve nodes")
        .iter_mut()
        .find_map(|node| {
            let node = node.as_object_mut()?;
            node["id"]
                .as_str()
                .is_some_and(|id| id.contains(id_fragment))
                .then_some(node)
        })
        .expect("resolved node")
}

fn dependency_metadata_rejected(metadata: &Value) {
    assert!(
        clinker_release_policy::decision::verify_dependency_capability_metadata(
            &repository_root(),
            metadata,
        )
        .is_err(),
        "mutated dependency metadata unexpectedly passed"
    );
}

#[test]
fn dependency_capability_real_workspace_passes() {
    let output = gate(&[
        "decision",
        "verify-dependency-capabilities",
        "--workspace-root",
        ".",
    ]);
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        output.stdout,
        b"Approved dependency capabilities verified\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn dependency_capability_feature_and_consumer_drift_rejects() {
    let baseline = dependency_metadata();
    clinker_release_policy::decision::verify_dependency_capability_metadata(
        &repository_root(),
        &baseline,
    )
    .expect("real metadata contract");

    let mut premature_precision = baseline.clone();
    metadata_dependency_mut(
        metadata_package_mut(&mut premature_precision, "clinker-format", None),
        "serde_json",
    )["features"] = json!(["arbitrary_precision", "preserve_order"]);
    dependency_metadata_rejected(&premature_precision);

    let mut raw_value = baseline.clone();
    metadata_dependency_mut(
        metadata_package_mut(&mut raw_value, "clinker-format", None),
        "serde_json",
    )["features"] = json!(["preserve_order", "raw_value"]);
    dependency_metadata_rejected(&raw_value);

    let mut missing_consumer = baseline.clone();
    metadata_package_mut(&mut missing_consumer, "clinker-channel", None)["dependencies"]
        .as_array_mut()
        .unwrap()
        .retain(|dependency| dependency["name"] != "fs4");
    dependency_metadata_rejected(&missing_consumer);

    let mut unapproved_consumer = baseline.clone();
    let fs4 = metadata_dependency_mut(
        metadata_package_mut(&mut unapproved_consumer, "clinker-channel", None),
        "fs4",
    )
    .clone();
    metadata_package_mut(&mut unapproved_consumer, "clinker", None)["dependencies"]
        .as_array_mut()
        .unwrap()
        .push(Value::Object(fs4));
    dependency_metadata_rejected(&unapproved_consumer);

    let mut wrong_kind = baseline.clone();
    metadata_dependency_mut(
        metadata_package_mut(&mut wrong_kind, "clinker-exec", None),
        "fs4",
    )["kind"] = json!("dev");
    dependency_metadata_rejected(&wrong_kind);

    let mut version_drift = baseline.clone();
    metadata_package_mut(&mut version_drift, "serde_json", Some("1.0.149"))["version"] =
        json!("1.0.150");
    dependency_metadata_rejected(&version_drift);
}

#[test]
fn dependency_capability_native_and_transitive_drift_rejects() {
    let baseline = dependency_metadata();

    let mut custom_build = baseline.clone();
    metadata_package_mut(&mut custom_build, "fs4", Some("1.1.0"))["targets"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "kind": ["custom-build"],
            "crate_types": ["bin"],
            "name": "build-script-build"
        }));
    dependency_metadata_rejected(&custom_build);

    let mut links = baseline.clone();
    metadata_package_mut(&mut links, "fs4", Some("1.1.0"))["links"] = json!("fs4_native");
    dependency_metadata_rejected(&links);

    let mut build_dependency = baseline.clone();
    metadata_dependency_mut(
        metadata_package_mut(&mut build_dependency, "fs4", Some("1.1.0")),
        "rustix",
    )["kind"] = json!("build");
    dependency_metadata_rejected(&build_dependency);

    let mut native_edge = baseline.clone();
    metadata_node_mut(&mut native_edge, "#fs4@1.1.0")["deps"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "name": "cc",
            "pkg": "registry+example#cc@1.0.0",
            "dep_kinds": [{"kind": null, "target": null}]
        }));
    dependency_metadata_rejected(&native_edge);

    let mut substituted_direct_package = baseline.clone();
    metadata_node_mut(&mut substituted_direct_package, "#fs4@1.1.0")["deps"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|dependency| dependency["name"] == "rustix")
        .unwrap()["pkg"] = json!("registry+example#cc@1.0.0");
    dependency_metadata_rejected(&substituted_direct_package);

    let mut transitive_edge = baseline.clone();
    metadata_node_mut(&mut transitive_edge, "#rustix@1.1.4")["deps"]
        .as_array_mut()
        .unwrap()
        .push(json!({
            "name": "bindgen",
            "pkg": "registry+example#bindgen@1.0.0",
            "dep_kinds": [{"kind": null, "target": null}]
        }));
    dependency_metadata_rejected(&transitive_edge);

    let mut substituted_transitive_package = baseline.clone();
    metadata_node_mut(&mut substituted_transitive_package, "#rustix@1.1.4")["deps"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|dependency| dependency["name"] == "bitflags")
        .unwrap()["pkg"] = json!("registry+example#bindgen@1.0.0");
    dependency_metadata_rejected(&substituted_transitive_package);

    let mut target_drift = baseline.clone();
    metadata_dependency_mut(
        metadata_package_mut(&mut target_drift, "fs4", Some("1.1.0")),
        "rustix",
    )["target"] = json!("cfg(unix)");
    dependency_metadata_rejected(&target_drift);
}

#[test]
fn dependency_capability_malformed_duplicate_and_command_failure_rejects() {
    dependency_metadata_rejected(&Value::Null);

    let mut duplicate = dependency_metadata();
    let fs4 = duplicate["packages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|package| package["name"] == "fs4" && package["version"] == "1.1.0")
        .unwrap()
        .clone();
    duplicate["packages"].as_array_mut().unwrap().push(fs4);
    dependency_metadata_rejected(&duplicate);

    let output = gate(&[
        "decision",
        "verify-dependency-capabilities",
        "--workspace-root",
        "path-that-does-not-exist",
    ]);
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}

fn assert_policy_rejection(output: &Output) {
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
    assert!(output.stderr.len() <= 1024);
}

fn canonical(value: &Value) -> clinker_release_policy::canonical::CanonicalValue {
    let bytes = serde_json::to_vec(value).expect("test value must serialize");
    clinker_release_policy::canonical::parse_json(&bytes)
        .expect("test value must be canonical JSON")
}

fn governed_assets(archives: &[Value]) -> Vec<Value> {
    let mut assets = vec![json!({
        "name": "SHA256SUMS",
        "length": 1,
        "sha256": "1111111111111111111111111111111111111111111111111111111111111111",
    })];
    for archive in archives {
        let name = archive["archive_name"].as_str().expect("archive name");
        assets.push(json!({
            "name": name,
            "length": 1,
            "sha256": archive["sha256"],
        }));
        assets.push(json!({
            "name": format!("{name}.sha256"),
            "length": 1,
            "sha256": "2222222222222222222222222222222222222222222222222222222222222222",
        }));
        assets.push(json!({
            "name": format!("{name}.intoto.jsonl"),
            "length": 1,
            "sha256": "3333333333333333333333333333333333333333333333333333333333333333",
        }));
    }
    assets.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    assets
}

fn valid_candidate() -> Value {
    let accepted = fixture(ACCEPTED_FIXTURE);
    let mut candidate = accepted["candidate_evidence"].clone();
    candidate["assets"] = Value::Array(governed_assets(
        candidate["archives"]
            .as_array()
            .expect("candidate archives"),
    ));
    candidate
}

fn valid_publication() -> Value {
    let candidate = valid_candidate();
    let candidate_bytes = clinker_release_policy::canonical::to_bytes(
        &clinker_release_policy::canonical::parse_json(&serde_json::to_vec(&candidate).unwrap())
            .unwrap(),
    )
    .unwrap();
    json!({
        "schema": "clinker.publication-evidence/v1",
        "kind": "publication",
        "state": "awaiting-approval",
        "revision": 0,
        "release_status": "incomplete",
        "completion_eligible": false,
        "immutable_authority_sha256": candidate["candidate_authorization_sha256"],
        "repository": "rustpunk/clinker",
        "candidate_sha256": clinker_release_policy::digest::sha256_hex(&candidate_bytes),
        "candidate_authorization_sha256": candidate["candidate_authorization_sha256"],
        "candidate_authorization_blob_sha": "1111111111111111111111111111111111111111",
        "candidate_decision_blob_sha": "2222222222222222222222222222222222222222",
        "candidate_evidence_blob_sha": "3333333333333333333333333333333333333333",
        "approval_record_blob_sha": "4444444444444444444444444444444444444444",
        "approval_record_sha256": "5555555555555555555555555555555555555555555555555555555555555555",
        "candidate": candidate,
        "dispatch": {
            "dispatch_id": "dispatch-1001",
            "workflow": "publish-release.yml",
            "workflow_sha": candidate["publish_workflow_sha"],
            "source_sha": candidate["source_sha"],
            "run_id": "1001",
            "run_attempt": 1,
            "run_url": "https://github.com/rustpunk/clinker/actions/runs/1001",
            "job_id": "1002",
            "job_name": "publish-approved-release",
            "environment": "release",
            "trigger_actor_ref": "maintainer:release",
            "dispatched_at": "2026-07-31T12:00:00Z"
        },
        "inspection": {"status": "not-started"},
        "asset_identities": {
            "checksum_sha256": candidate["checksum_sha256"],
            "archive_digests": candidate["archive_digests"],
        }
    })
}

#[test]
fn evidence_validate_accepts_exact_candidate_and_publication_argv_without_mutation() {
    let directory = TempDir::new().expect("temporary directory must be created");
    for (kind, manifest) in [
        ("candidate", valid_candidate()),
        ("publication", valid_publication()),
    ] {
        let path = write_canonical_json(&directory, &format!("{kind}.json"), &manifest);
        let before = fs::read(&path).expect("manifest must be readable");
        let output = gate(&[
            "evidence",
            "validate",
            "--kind",
            kind,
            "--schema",
            "scripts/release/release-evidence.schema.json",
            "--manifest",
            path.to_str().unwrap(),
        ]);
        assert_eq!(
            output.status.code(),
            Some(0),
            "{kind} validation failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(output.stdout, b"release evidence validation passed\n");
        assert!(output.stderr.is_empty());
        assert_eq!(fs::read(&path).unwrap(), before);
    }

    let invalid_path = write_json(&directory, "invalid.json", &json!({"kind": "candidate"}));
    assert_policy_rejection(&gate(&[
        "evidence",
        "validate",
        "--kind",
        "candidate",
        "--schema",
        "scripts/release/release-evidence.schema.json",
        "--manifest",
        invalid_path.to_str().unwrap(),
    ]));
    let malformed = gate(&[
        "evidence",
        "validate",
        "--kind",
        "candidate",
        "--manifest",
        invalid_path.to_str().unwrap(),
        "--unknown",
    ]);
    assert_eq!(malformed.status.code(), Some(2));
    assert!(malformed.stdout.is_empty());
}

#[test]
fn create_only_is_mode_six_hundred_replay_safe_and_race_refusing() {
    use clinker_release_policy::evidence::{EvidenceWrite, create_only};

    let directory = TempDir::new().expect("temporary directory must be created");
    let path = directory.path().join("candidate.json");
    let first = canonical(&json!({"authority": "a", "revision": 1}));
    let second = canonical(&json!({"authority": "b", "revision": 1}));
    assert_eq!(create_only(&path, &first).unwrap(), EvidenceWrite::Created);
    assert_eq!(
        fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let bytes = fs::read(&path).unwrap();
    assert_eq!(
        create_only(&path, &first).unwrap(),
        EvidenceWrite::ExactReplay
    );
    assert!(create_only(&path, &second).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);

    let race_path = Arc::new(directory.path().join("race.json"));
    let barrier = Arc::new(Barrier::new(3));
    let mut writers = Vec::new();
    for value in [first, second] {
        let path = Arc::clone(&race_path);
        let barrier = Arc::clone(&barrier);
        writers.push(thread::spawn(move || {
            barrier.wait();
            create_only(&path, &value)
        }));
    }
    barrier.wait();
    let outcomes = writers
        .into_iter()
        .map(|writer| writer.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(outcomes.iter().filter(|result| result.is_err()).count(), 1);
}

#[test]
fn atomic_write_faults_clean_temporary_files_without_final_evidence() {
    use clinker_release_policy::evidence::{FaultPoint, create_only_with_fault};

    for fault in [
        FaultPoint::AfterTemporaryCreate,
        FaultPoint::BeforeFileSync,
        FaultPoint::BeforeInstall,
        FaultPoint::BeforeDirectorySync,
    ] {
        let directory = TempDir::new().expect("temporary directory must be created");
        let path = directory.path().join("evidence.json");
        let value = canonical(&json!({"authority": "a", "revision": 1}));
        assert!(create_only_with_fault(&path, &value, fault).is_err());
        assert!(!path.exists());
        assert_eq!(fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}

#[test]
fn compare_and_swap_enforces_lock_revision_state_authority_and_exact_replay() {
    use clinker_release_policy::evidence::{
        EvidenceExpectation, EvidenceLock, EvidenceWrite, compare_and_swap, create_only,
    };

    let directory = TempDir::new().expect("temporary directory must be created");
    let path = directory.path().join("publication.json");
    let authority = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let initial = canonical(&json!({
        "revision": 1,
        "state": "awaiting",
        "immutable_authority_sha256": authority,
        "payload": "initial"
    }));
    let next = canonical(&json!({
        "revision": 2,
        "state": "approved",
        "immutable_authority_sha256": authority,
        "payload": "next"
    }));
    create_only(&path, &initial).unwrap();
    let expected = EvidenceExpectation::new(1, "awaiting", authority);
    assert_eq!(
        compare_and_swap(&path, &expected, &next).unwrap(),
        EvidenceWrite::Replaced
    );
    let next_expected = EvidenceExpectation::new(2, "approved", authority);
    assert_eq!(
        compare_and_swap(&path, &next_expected, &next).unwrap(),
        EvidenceWrite::ExactReplay
    );

    for stale in [
        EvidenceExpectation::new(1, "approved", authority),
        EvidenceExpectation::new(2, "awaiting", authority),
        EvidenceExpectation::new(
            2,
            "approved",
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ),
    ] {
        assert!(compare_and_swap(&path, &stale, &next).is_err());
    }
    let bytes = fs::read(&path).unwrap();
    {
        let _held = EvidenceLock::acquire(&path).unwrap();
        assert!(compare_and_swap(&path, &next_expected, &next).is_err());
    }
    assert_eq!(fs::read(&path).unwrap(), bytes);

    for (index, fault) in [
        clinker_release_policy::evidence::FaultPoint::AfterTemporaryCreate,
        clinker_release_policy::evidence::FaultPoint::BeforeFileSync,
        clinker_release_policy::evidence::FaultPoint::BeforeInstall,
        clinker_release_policy::evidence::FaultPoint::BeforeDirectorySync,
    ]
    .into_iter()
    .enumerate()
    {
        let fault_path = directory.path().join(format!("fault-{index}.json"));
        create_only(&fault_path, &initial).unwrap();
        let original = fs::read(&fault_path).unwrap();
        assert!(
            clinker_release_policy::evidence::compare_and_swap_with_fault(
                &fault_path,
                &expected,
                &next,
                fault,
            )
            .is_err()
        );
        assert_eq!(fs::read(&fault_path).unwrap(), original);
        let temporary_prefix = format!(".fault-{index}.json.");
        assert!(fs::read_dir(directory.path()).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(&temporary_prefix)
        }));
    }
}

#[test]
fn bounded_child_uses_explicit_argv_allowlisted_environment_and_truncation_flags() {
    use clinker_release_policy::child::{ChildSpec, Termination, run};

    let result = run(ChildSpec {
        program: PathBuf::from("/usr/bin/printf"),
        arguments: vec![OsString::from("%s"), OsString::from("hello world")],
        environment: BTreeMap::new(),
        timeout: Duration::from_secs(2),
        output_limit: 64,
    })
    .unwrap();
    assert_eq!(result.termination, Termination::Exited(Some(0)));
    assert_eq!(result.stdout, b"hello world");
    assert!(result.stderr.is_empty());
    assert!(!result.stdout_truncated);

    // The deadline has to outlast spawning the child, scheduling it, and its
    // first write, or the capture is empty and the byte assertions below are
    // measuring host load rather than truncation. `yes` never exits, so a
    // deadline generous enough to remove that race still terminates by it.
    let flood = run(ChildSpec {
        program: PathBuf::from("/usr/bin/yes"),
        arguments: Vec::new(),
        environment: BTreeMap::new(),
        timeout: Duration::from_secs(2),
        output_limit: 128,
    })
    .unwrap();
    assert_eq!(flood.stdout.len(), 128);
    assert!(flood.stdout_truncated);
    assert_eq!(flood.termination, Termination::TimedOut);

    let mut forbidden = BTreeMap::new();
    forbidden.insert(OsString::from("SECRET_TOKEN"), OsString::from("secret"));
    assert!(
        run(ChildSpec {
            program: PathBuf::from("/usr/bin/env"),
            arguments: Vec::new(),
            environment: forbidden,
            timeout: Duration::from_secs(1),
            output_limit: 128,
        })
        .is_err()
    );
    assert!(
        run(ChildSpec {
            program: PathBuf::from("/definitely/missing/clinker-release-policy-child"),
            arguments: Vec::new(),
            environment: BTreeMap::new(),
            timeout: Duration::from_secs(1),
            output_limit: 128,
        })
        .is_err()
    );
}

#[test]
fn bounded_child_timeout_terminates_the_process_group_before_returning() {
    use clinker_release_policy::child::{ChildSpec, Termination, run};

    let started = Instant::now();
    let result = run(ChildSpec {
        program: PathBuf::from("/usr/bin/sleep"),
        arguments: vec![OsString::from("30")],
        environment: BTreeMap::new(),
        timeout: Duration::from_millis(100),
        output_limit: 128,
    })
    .unwrap();
    assert_eq!(result.termination, Termination::TimedOut);
    assert!(started.elapsed() < Duration::from_secs(3));
}
