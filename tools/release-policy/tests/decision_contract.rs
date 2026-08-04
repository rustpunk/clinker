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
    let authorizations = fixture(AUTHORIZATION_FIXTURE);
    let authorization = &authorizations["authorized"];
    let identity = &authorization["authorization"];
    let mut targets = identity["archive_digests"]
        .as_object()
        .expect("archive digests must be an object")
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    targets.sort();
    let archives = targets
        .iter()
        .map(|target| {
            let extension = if target.ends_with("windows-msvc") {
                "zip"
            } else {
                "tar.gz"
            };
            json!({
                "target": target,
                "archive_name": format!(
                    "clinker-v{}-{target}.{extension}",
                    identity["candidate_version"].as_str().unwrap()
                ),
                "sha256": identity["archive_digests"][target],
            })
        })
        .collect::<Vec<_>>();
    let attestations = archives
        .iter()
        .map(|archive| {
            json!({
                "archive_name": archive["archive_name"],
                "subject_sha256": archive["sha256"],
                "repository": "rustpunk/clinker",
                "workflow": ".github/workflows/release.yml",
                "ref": format!("refs/tags/{}", identity["candidate_tag"].as_str().unwrap()),
                "source_sha": identity["source_sha"],
                "runner_environment": "github-hosted",
            })
        })
        .collect::<Vec<_>>();
    let assets = governed_assets(&archives);

    json!({
        "schema": "clinker.candidate-evidence/v1",
        "kind": "candidate",
        "state": "candidate-verified",
        "revision": 0,
        "release_status": "incomplete",
        "completion_eligible": false,
        "immutable_authority_sha256": authorization["candidate_authorization_sha256"],
        "candidate_authorization_sha256": authorization["candidate_authorization_sha256"],
        "candidate_tag": identity["candidate_tag"],
        "candidate_version": identity["candidate_version"],
        "source_sha": identity["source_sha"],
        "build_workflow_sha": identity["build_workflow_sha"],
        "publish_workflow_ref": identity["publish_workflow_ref"],
        "publish_workflow_ref_resolved_sha": identity["publish_workflow_ref_resolved_sha"],
        "publish_workflow_sha": identity["publish_workflow_sha"],
        "candidate_release_id": identity["candidate_release_id"],
        "checksum_sha256": identity["checksum_sha256"],
        "archive_digests": identity["archive_digests"],
        "ci_run_ref": identity["ci_run_ref"],
        "changelog_ref": identity["changelog_ref"],
        "inventory_ref": identity["inventory_ref"],
        "authorized_release_maintainer_ref": identity["authorized_release_maintainer_ref"],
        "build_workflow_path": ".github/workflows/release.yml",
        "build_run_id": "1001",
        "build_head_sha": identity["source_sha"],
        "publish_workflow_path": ".github/workflows/publish-release.yml",
        "archives": archives,
        "attestations": attestations,
        "assets": assets,
        "tag_mutation_performed": false,
        "tag_readback_ref": format!(
            "https://github.com/rustpunk/clinker/git/ref/tags/{}",
            identity["candidate_tag"].as_str().unwrap()
        ),
        "release_trigger_event_ref": "https://github.com/rustpunk/clinker/actions/runs/1001",
    })
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

    let flood = run(ChildSpec {
        program: PathBuf::from("/usr/bin/yes"),
        arguments: Vec::new(),
        environment: BTreeMap::new(),
        timeout: Duration::from_millis(100),
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
