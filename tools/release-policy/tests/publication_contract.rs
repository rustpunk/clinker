use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use clinker_release_policy::canonical;
use clinker_release_policy::cli::github::{GitHubTransport, Request, Response};
use clinker_release_policy::cli::publication::{
    self, DispatchRequest, InspectionRequest, ProtectedPublishRequest, VerificationRequest,
    WorkflowContext,
};
use clinker_release_policy::decision::{self, DecisionRequest};
use clinker_release_policy::digest;
use clinker_release_policy::error::GateError;
use clinker_release_policy::evidence::EvidenceLock;
use clinker_release_policy::release::{self, StageDraftRequest, StageWorkflowContext};
use serde_json::{Map, Value, json};
use tempfile::TempDir;

const DRAFT_SOURCE_SHA: &str = "1111111111111111111111111111111111111111";
const DRAFT_RELEASE_NOTES: &str = "# Clinker v0.1.0\n\nGenerated release notes.";
const DRAFT_INVENTORY: &str = r#"schema = "clinker.release-inventory/v1"
version_source = "Cargo.toml:workspace.package.version"
license = "MIT"
license_file = "LICENSE"
archive_prefix = "clinker"
required_members = ["clinker", "cxl", "README.md", "LICENSE", "release-manifest.json"]

[[binaries]]
package = "clinker"
name = "clinker"
smoke_args = ["--version"]

[[binaries]]
package = "cxl-cli"
name = "cxl"
smoke_args = ["--version"]

[[targets]]
target = "x86_64-unknown-linux-gnu"
archive_format = "tar.gz"
binary_suffix = ""
archive_name = "clinker-v{version}-x86_64-unknown-linux-gnu.tar.gz"
root_name = "clinker-v{version}-x86_64-unknown-linux-gnu"

[[targets]]
target = "x86_64-apple-darwin"
archive_format = "tar.gz"
binary_suffix = ""
archive_name = "clinker-v{version}-x86_64-apple-darwin.tar.gz"
root_name = "clinker-v{version}-x86_64-apple-darwin"

[[targets]]
target = "aarch64-apple-darwin"
archive_format = "tar.gz"
binary_suffix = ""
archive_name = "clinker-v{version}-aarch64-apple-darwin.tar.gz"
root_name = "clinker-v{version}-aarch64-apple-darwin"

[[targets]]
target = "x86_64-pc-windows-msvc"
archive_format = "zip"
binary_suffix = ".exe"
archive_name = "clinker-v{version}-x86_64-pc-windows-msvc.zip"
root_name = "clinker-v{version}-x86_64-pc-windows-msvc"
"#;

struct Fixture {
    root: TempDir,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary publication repository");
        fs::create_dir_all(root.path().join("decisions")).expect("decision directory");
        fs::create_dir_all(root.path().join("target/release-policy")).expect("evidence directory");
        for path in [
            "decisions/authorization.json",
            "decisions/candidate.json",
            "decisions/approval.json",
            "authorization-schema.json",
            "decision-schema.json",
            "evidence-schema.json",
            "target/release-policy/candidate-evidence.json",
        ] {
            fs::write(root.path().join(path), "{}\n").expect("placeholder input");
        }
        Self { root }
    }

    fn run(&self, arguments: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
            .current_dir(self.root.path())
            .args(arguments)
            .output()
            .expect("run clinker-release-policy")
    }
}

struct DraftFixture {
    root: TempDir,
    assets: PathBuf,
}

impl DraftFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary draft repository");
        fs::create_dir_all(root.path().join("release")).expect("release directory");
        fs::create_dir_all(root.path().join("crates/cxl-cli")).expect("cxl manifest directory");
        fs::write(
            root.path().join("Cargo.toml"),
            "[workspace]\nresolver = \"2\"\n\n[workspace.package]\nversion = \"0.1.0\"\nlicense = \"MIT\"\n",
        )
        .expect("workspace manifest");
        fs::write(
            root.path().join("crates/cxl-cli/Cargo.toml"),
            "[package]\nname = \"cxl-cli\"\nversion = \"0.1.0\"\n\n[[bin]]\nname = \"cxl\"\npath = \"src/main.rs\"\n",
        )
        .expect("cxl manifest");
        fs::write(root.path().join("release/inventory.toml"), DRAFT_INVENTORY)
            .expect("release inventory");
        fs::write(root.path().join("README.md"), "# Clinker\n").expect("README");
        fs::write(
            root.path().join("LICENSE"),
            "MIT License\n\nPermission is hereby granted, free of charge, to any person obtaining a copy.\n\nTHE SOFTWARE IS PROVIDED \"AS IS\".\n",
        )
        .expect("license");
        let fixture_source = root.path().join("fixture.rs");
        let fixture_binary = root.path().join("fixture-bin");
        fs::write(
            &fixture_source,
            "fn main() { println!(\"fixture 0.1.0\"); }\n",
        )
        .expect("fixture source");
        let status = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .args(["-O", "-o"])
            .arg(&fixture_binary)
            .arg(&fixture_source)
            .status()
            .expect("compile fixture binary");
        assert!(status.success());

        let assets = root.path().join("artifacts");
        fs::create_dir(&assets).expect("asset directory");
        for target in [
            "x86_64-unknown-linux-gnu",
            "x86_64-apple-darwin",
            "aarch64-apple-darwin",
            "x86_64-pc-windows-msvc",
        ] {
            let suffix = if target == "x86_64-pc-windows-msvc" {
                ".exe"
            } else {
                ""
            };
            let binary_dir = root.path().join("target").join(target).join("release");
            fs::create_dir_all(&binary_dir).expect("binary directory");
            for name in ["clinker", "cxl"] {
                fs::copy(&fixture_binary, binary_dir.join(format!("{name}{suffix}")))
                    .expect("copy fixture binary");
            }
            let output = Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
                .current_dir(root.path())
                .args([
                    "release",
                    "build-bundle",
                    "--target",
                    target,
                    "--source-sha",
                    DRAFT_SOURCE_SHA,
                    "--output-dir",
                    assets.to_str().expect("UTF-8 asset path"),
                ])
                .output()
                .expect("build fixture bundle");
            assert!(
                output.status.success(),
                "fixture bundle failed: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        let output = Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
            .current_dir(root.path())
            .args([
                "release",
                "verify",
                "assemble",
                "--asset-dir",
                assets.to_str().expect("UTF-8 asset path"),
                "--repository",
                "rustpunk/clinker",
                "--workflow",
                ".github/workflows/release.yml",
                "--ref",
                "refs/tags/v0.1.0",
                "--source-sha",
                DRAFT_SOURCE_SHA,
            ])
            .output()
            .expect("assemble fixture assets");
        assert!(
            output.status.success(),
            "fixture assembly failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        Self { root, assets }
    }

    fn request(&self) -> StageDraftRequest {
        StageDraftRequest {
            repository: "rustpunk/clinker".to_owned(),
            candidate_tag: "v0.1.0".to_owned(),
            source_sha: DRAFT_SOURCE_SHA.to_owned(),
            asset_dir: self.assets.clone(),
            context: StageWorkflowContext {
                repository: "rustpunk/clinker".to_owned(),
                run_id: "700".to_owned(),
                run_attempt: 1,
                workflow_ref: "rustpunk/clinker/.github/workflows/release.yml@refs/tags/v0.1.0"
                    .to_owned(),
                workflow_sha: DRAFT_SOURCE_SHA.to_owned(),
                git_ref: "refs/tags/v0.1.0".to_owned(),
                source_sha: DRAFT_SOURCE_SHA.to_owned(),
            },
            deadline_seconds: 600,
        }
    }

    fn asset_bytes(&self) -> BTreeMap<String, Vec<u8>> {
        fs::read_dir(&self.assets)
            .expect("asset directory")
            .map(|entry| {
                let entry = entry.expect("asset entry");
                (
                    entry.file_name().into_string().expect("UTF-8 asset name"),
                    fs::read(entry.path()).expect("asset bytes"),
                )
            })
            .collect()
    }
}

fn assert_public_command_was_dispatched(output: &Output) {
    assert_ne!(
        output.status.code(),
        Some(2),
        "public argv was rejected by clap: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn protected_dispatch_interface() {
    let fixture = Fixture::new();
    let authority = [
        "--repo",
        "rustpunk/clinker",
        "--authorization-record",
        "decisions/authorization.json",
        "--authorization-schema",
        "authorization-schema.json",
        "--decision-record",
        "decisions/candidate.json",
        "--decision-schema",
        "decision-schema.json",
    ];

    for operation in ["create-candidate-tag", "resolve-protected-ref"] {
        let mut arguments = vec!["publication", operation];
        arguments.extend(authority);
        arguments.extend(["--deadline-seconds", "120"]);
        assert_public_command_was_dispatched(&fixture.run(&arguments));
    }

    let mut dispatch = vec!["publication", "dispatch"];
    dispatch.extend([
        "--repo",
        "rustpunk/clinker",
        "--workflow",
        "publish-release.yml",
        "--decision-dir",
        "decisions",
    ]);
    dispatch.extend_from_slice(&authority[2..]);
    dispatch.extend([
        "--approval-record",
        "decisions/approval.json",
        "--candidate-evidence",
        "target/release-policy/candidate-evidence.json",
        "--evidence-schema",
        "evidence-schema.json",
        "--publication-evidence",
        "target/release-policy/publication-evidence.json",
        "--discovery-deadline-seconds",
        "600",
    ]);
    assert_public_command_was_dispatched(&fixture.run(&dispatch));

    assert_public_command_was_dispatched(&fixture.run(&[
        "publication",
        "approval-target",
        "--repo",
        "rustpunk/clinker",
        "--publication-evidence",
        "target/release-policy/publication-evidence.json",
        "--evidence-schema",
        "evidence-schema.json",
    ]));

    for (operation, expected_state, expected_revision) in [
        ("begin-inspection", "awaiting-approval", "0"),
        ("complete-inspection", "inspection-started", "1"),
    ] {
        assert_public_command_was_dispatched(&fixture.run(&[
            "publication",
            operation,
            "--repo",
            "rustpunk/clinker",
            "--publication-evidence",
            "target/release-policy/publication-evidence.json",
            "--evidence-schema",
            "evidence-schema.json",
            "--expected-state",
            expected_state,
            "--expected-revision",
            expected_revision,
        ]));
    }

    for (operation, expected_state, expected_revision, deadline_flag, deadline) in [
        (
            "verify-approval",
            "inspection-completed",
            "2",
            "--deadline-seconds",
            "120",
        ),
        (
            "wait-and-verify",
            "approved",
            "3",
            "--run-deadline-seconds",
            "2700",
        ),
    ] {
        let mut arguments = vec!["publication", operation];
        arguments.extend(["--repo", "rustpunk/clinker", "--decision-dir", "decisions"]);
        arguments.extend_from_slice(&authority[2..]);
        arguments.extend([
            "--approval-record",
            "decisions/approval.json",
            "--candidate-evidence",
            "target/release-policy/candidate-evidence.json",
            "--evidence-schema",
            "evidence-schema.json",
            "--publication-evidence",
            "target/release-policy/publication-evidence.json",
            "--expected-state",
            expected_state,
            "--expected-revision",
            expected_revision,
            deadline_flag,
            deadline,
        ]);
        assert_public_command_was_dispatched(&fixture.run(&arguments));
    }
}

enum FakeResponse {
    Json(Value),
    Raw(Vec<u8>),
    Failure,
}

#[derive(Default)]
struct RecordingTransport {
    responses: VecDeque<FakeResponse>,
    requests: Vec<Request>,
}

impl RecordingTransport {
    fn new(responses: impl IntoIterator<Item = FakeResponse>) -> Self {
        Self {
            responses: responses.into_iter().collect(),
            requests: Vec::new(),
        }
    }
}

impl GitHubTransport for RecordingTransport {
    fn send(&mut self, request: &Request) -> Result<Response, GateError> {
        self.requests.push(request.clone());
        match self.responses.pop_front().expect("scripted response") {
            FakeResponse::Json(body) => Ok(Response { body, raw: None }),
            FakeResponse::Raw(raw) => Ok(Response {
                body: Value::Null,
                raw: Some(raw),
            }),
            FakeResponse::Failure => Err(GateError::policy(
                "test.transport",
                "scripted ambiguous transport failure",
            )),
        }
    }
}

fn draft_prefix() -> Vec<FakeResponse> {
    vec![
        FakeResponse::Json(json!({
            "id": "700",
            "run_attempt": 1,
            "head_sha": DRAFT_SOURCE_SHA,
            "event": "push",
            "path": ".github/workflows/release.yml"
        })),
        FakeResponse::Json(json!({
            "ref": "refs/tags/v0.1.0",
            "object": {"type": "commit", "sha": DRAFT_SOURCE_SHA}
        })),
    ]
}

fn draft_metadata(release_id: &str) -> String {
    let value = json!({
        "build_workflow_path": ".github/workflows/release.yml",
        "build_workflow_sha": DRAFT_SOURCE_SHA,
        "build_run_id": "700",
        "build_event": "push",
        "build_ref": "refs/tags/v0.1.0",
        "build_head_sha": DRAFT_SOURCE_SHA,
        "source_sha": DRAFT_SOURCE_SHA,
        "publish_workflow_ref": "v0.1.0",
        "publish_workflow_sha": DRAFT_SOURCE_SHA,
        "candidate_release_id": release_id,
        "release_notes_sha256": digest::sha256_hex(DRAFT_RELEASE_NOTES.as_bytes()),
    });
    let metadata = String::from_utf8(canonical::to_bytes(&canonical_value(&value)).unwrap())
        .expect("canonical metadata is UTF-8");
    format!(
        "{DRAFT_RELEASE_NOTES}\n\n<!-- clinker-release-metadata\n{}\n-->\n",
        metadata.trim_end()
    )
}

fn draft_release(
    body: &str,
    draft: bool,
    target_commitish: &str,
    names: impl IntoIterator<Item = String>,
) -> Value {
    let assets = names
        .into_iter()
        .enumerate()
        .map(|(index, name)| json!({"id": format!("draft-asset-{index}"), "name": name}))
        .collect::<Vec<_>>();
    json!({
        "id": "release-700",
        "tag_name": "v0.1.0",
        "target_commitish": target_commitish,
        "draft": draft,
        "prerelease": false,
        "body": body,
        "assets": assets,
    })
}

fn draft_asset_downloads(
    assets: &BTreeMap<String, Vec<u8>>,
    names: impl IntoIterator<Item = String>,
) -> Vec<FakeResponse> {
    names
        .into_iter()
        .map(|name| FakeResponse::Raw(assets.get(&name).expect("fixture asset").clone()))
        .collect()
}

#[test]
fn hidden_draft_worker_creates_retries_and_reconciles_exact_private_assets() {
    let fixture = DraftFixture::new();
    let request = fixture.request();
    let assets = fixture.asset_bytes();
    let names = assets.keys().cloned().collect::<Vec<_>>();
    let metadata = draft_metadata("release-700");
    let complete = draft_release(&metadata, true, DRAFT_SOURCE_SHA, names.clone());

    let mut create_responses = draft_prefix();
    create_responses.push(FakeResponse::Json(json!([])));
    create_responses.push(FakeResponse::Json(draft_release(
        DRAFT_RELEASE_NOTES,
        true,
        DRAFT_SOURCE_SHA,
        Vec::new(),
    )));
    create_responses.push(FakeResponse::Json(Value::Null));
    create_responses.extend((0..assets.len()).map(|_| FakeResponse::Json(Value::Null)));
    create_responses.push(FakeResponse::Json(json!([complete.clone()])));
    create_responses.extend(draft_asset_downloads(&assets, names.clone()));
    let mut create = RecordingTransport::new(create_responses);
    release::stage_candidate_draft(fixture.root.path(), &request, &mut create)
        .expect("create and verify private draft");
    assert!(create.responses.is_empty());
    assert_eq!(
        create
            .requests
            .iter()
            .filter(|request| request.method == clinker_release_policy::cli::github::Method::Patch)
            .count(),
        1
    );
    let create_release = create
        .requests
        .iter()
        .find(|request| {
            request.method == clinker_release_policy::cli::github::Method::Post
                && request.endpoint == "repos/rustpunk/clinker/releases"
        })
        .expect("release creation request");
    assert_eq!(
        create_release
            .fields
            .get("generate_release_notes")
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        create_release.fields.get("prerelease").map(String::as_str),
        Some("false")
    );
    let metadata_patch = create
        .requests
        .iter()
        .find(|request| request.method == clinker_release_policy::cli::github::Method::Patch)
        .expect("metadata patch request");
    assert!(
        metadata_patch
            .fields
            .get("body")
            .is_some_and(|body| body.starts_with(DRAFT_RELEASE_NOTES))
    );
    let uploads = create
        .requests
        .iter()
        .filter(|request| request.endpoint.starts_with("https://uploads.github.com/"))
        .collect::<Vec<_>>();
    assert_eq!(uploads.len(), assets.len());
    assert!(uploads.iter().all(|request| request.input_file.is_some()));
    assert!(!fixture.root.path().join("candidate-evidence.json").exists());

    let mut retry_responses = draft_prefix();
    retry_responses.push(FakeResponse::Json(json!([complete.clone()])));
    retry_responses.extend(draft_asset_downloads(&assets, names.clone()));
    retry_responses.push(FakeResponse::Json(json!([complete.clone()])));
    retry_responses.extend(draft_asset_downloads(&assets, names.clone()));
    let mut retry = RecordingTransport::new(retry_responses);
    release::stage_candidate_draft(fixture.root.path(), &request, &mut retry)
        .expect("byte-identical retry");
    assert!(retry.responses.is_empty());
    assert!(retry.requests.iter().all(|request| {
        request.method != clinker_release_policy::cli::github::Method::Patch
            && !request.endpoint.starts_with("https://uploads.github.com/")
    }));

    let existing_names = names[..4].to_vec();
    let partial = draft_release(&metadata, true, DRAFT_SOURCE_SHA, existing_names.clone());
    let mut reconcile_responses = draft_prefix();
    reconcile_responses.push(FakeResponse::Json(json!([partial])));
    reconcile_responses.extend(draft_asset_downloads(&assets, existing_names));
    reconcile_responses.extend((0..assets.len() - 4).map(|_| FakeResponse::Json(Value::Null)));
    reconcile_responses.push(FakeResponse::Json(json!([complete])));
    reconcile_responses.extend(draft_asset_downloads(&assets, names));
    let mut reconcile = RecordingTransport::new(reconcile_responses);
    release::stage_candidate_draft(fixture.root.path(), &request, &mut reconcile)
        .expect("byte-identical partial reconciliation");
    assert!(reconcile.responses.is_empty());
    assert_eq!(
        reconcile
            .requests
            .iter()
            .filter(|request| request.endpoint.starts_with("https://uploads.github.com/"))
            .count(),
        assets.len() - 4
    );
    assert_eq!(
        reconcile
            .requests
            .iter()
            .filter(|request| request.raw_response)
            .count(),
        assets.len() + 4,
        "existing assets and the complete final set must be freshly downloaded"
    );
    assert!(!fixture.root.path().join("candidate-evidence.json").exists());
}

#[test]
fn hidden_draft_worker_fails_closed_on_remote_drift_and_ambiguity() {
    let fixture = DraftFixture::new();
    let request = fixture.request();
    let assets = fixture.asset_bytes();
    let names = assets.keys().cloned().collect::<Vec<_>>();
    let metadata = draft_metadata("release-700");
    let exact = draft_release(&metadata, true, DRAFT_SOURCE_SHA, names.clone());

    let first_name = names[0].clone();
    let mut mismatch = draft_prefix();
    mismatch.push(FakeResponse::Json(json!([draft_release(
        &metadata,
        true,
        DRAFT_SOURCE_SHA,
        [first_name.clone()],
    )])));
    mismatch.push(FakeResponse::Raw(b"different remote bytes".to_vec()));
    let mut transport = RecordingTransport::new(mismatch);
    assert!(release::stage_candidate_draft(fixture.root.path(), &request, &mut transport).is_err());

    let mut duplicate = exact.clone();
    duplicate["assets"]
        .as_array_mut()
        .unwrap()
        .push(json!({"id": "duplicate", "name": first_name}));
    let mut cases = vec![
        ("duplicate asset", json!([duplicate])),
        (
            "extra asset",
            json!([draft_release(
                &metadata,
                true,
                DRAFT_SOURCE_SHA,
                ["starter.txt".to_owned()],
            )]),
        ),
        (
            "public release",
            json!([draft_release(
                &metadata,
                false,
                DRAFT_SOURCE_SHA,
                Vec::new(),
            )]),
        ),
        (
            "wrong source identity",
            json!([draft_release(
                &metadata,
                true,
                "2222222222222222222222222222222222222222",
                Vec::new(),
            )]),
        ),
        (
            "starter metadata",
            json!([draft_release(
                DRAFT_RELEASE_NOTES,
                true,
                DRAFT_SOURCE_SHA,
                Vec::new(),
            )]),
        ),
        (
            "ambiguous concurrent releases",
            json!([exact.clone(), exact.clone()]),
        ),
    ];
    for (label, releases) in cases.drain(..) {
        let mut responses = draft_prefix();
        responses.push(FakeResponse::Json(releases));
        let mut transport = RecordingTransport::new(responses);
        assert!(
            release::stage_candidate_draft(fixture.root.path(), &request, &mut transport).is_err(),
            "{label} must fail closed"
        );
        assert!(transport.requests.iter().all(|request| {
            request.method != clinker_release_policy::cli::github::Method::Patch
                && !request.endpoint.starts_with("https://uploads.github.com/")
        }));
    }

    let mut transport = RecordingTransport::new([FakeResponse::Failure]);
    assert!(
        release::stage_candidate_draft(fixture.root.path(), &request, &mut transport).is_err(),
        "ambiguous 502-style transport failure must fail closed"
    );

    let mut injection = request.clone();
    injection.candidate_tag = "v0.1.0; touch injected".to_owned();
    let mut transport = RecordingTransport::default();
    assert!(
        release::stage_candidate_draft(fixture.root.path(), &injection, &mut transport).is_err()
    );
    assert!(transport.requests.is_empty());
    assert!(!fixture.root.path().join("injected").exists());
    assert!(!fixture.root.path().join("candidate-evidence.json").exists());
}

#[test]
fn draft_worker_route_is_help_hidden_and_workflow_only() {
    let help = Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
        .args(["release", "--help"])
        .output()
        .expect("release help");
    assert!(help.status.success());
    assert!(!String::from_utf8_lossy(&help.stdout).contains("stage-candidate-draft"));

    let output = Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
        .args([
            "release",
            "stage-candidate-draft",
            "--repo",
            "rustpunk/clinker",
            "--candidate-tag",
            "v0.1.0",
            "--source-sha",
            DRAFT_SOURCE_SHA,
            "--asset-dir",
            "artifacts",
            "--deadline-seconds",
            "600",
            "--help",
        ])
        .output()
        .expect("hidden draft worker argv");
    assert!(
        output.status.success(),
        "workflow argv was rejected by clap: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("--candidate-tag"));
}

struct StateFixture {
    root: TempDir,
    authorization: PathBuf,
    decision: PathBuf,
    approval: PathBuf,
    candidate: PathBuf,
    publication: PathBuf,
    decision_dir: PathBuf,
    candidate_value: Value,
    asset_bytes: Vec<(String, Vec<u8>)>,
}

impl StateFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary state repository");
        let decision_dir = root.path().join("decisions");
        fs::create_dir(&decision_dir).expect("decision directory");
        let repository = repository_root();
        let authorization_fixture: Value =
            serde_json::from_slice(
                &fs::read(repository.join(
                    "scripts/release/fixtures/release-decisions/candidate-authorizations.json",
                ))
                .expect("authorization fixture"),
            )
            .expect("authorization JSON");
        let decisions_fixture: Value = serde_json::from_slice(
            &fs::read(
                repository
                    .join("scripts/release/fixtures/release-decisions/accepted-record-set.json"),
            )
            .expect("decision fixture"),
        )
        .expect("decision JSON");
        let mut authorization = authorization_fixture["authorized"].clone();
        let asset_bytes = [
            "aarch64-apple-darwin",
            "x86_64-apple-darwin",
            "x86_64-pc-windows-msvc",
            "x86_64-unknown-linux-gnu",
        ]
        .into_iter()
        .map(|target| {
            let bytes = format!("protected asset bytes for {target}\n").into_bytes();
            (target.to_owned(), bytes)
        })
        .collect::<Vec<_>>();
        let digests = asset_bytes
            .iter()
            .map(|(target, bytes)| (target.clone(), Value::String(digest::sha256_hex(bytes))))
            .collect::<Map<_, _>>();
        authorization["authorization"]["archive_digests"] = Value::Object(digests.clone());
        let nested = canonical_value(&authorization["authorization"]);
        let authorization_digest = digest::sha256_hex(&canonical::to_bytes(&nested).unwrap());
        authorization["candidate_authorization_sha256"] = json!(authorization_digest);

        let records = decisions_fixture["records"]
            .as_array()
            .expect("decision records");
        let mut decision = records
            .iter()
            .find(|record| record["decision_id"] == "release-candidate")
            .expect("candidate decision")
            .clone();
        let mut approval = records
            .iter()
            .find(|record| record["decision_id"] == "publication-approval")
            .expect("approval decision")
            .clone();
        for record in [&mut decision, &mut approval] {
            record["archive_digests"] = Value::Object(digests.clone());
            record["candidate_authorization_sha256"] = json!(authorization_digest);
        }
        let environment = records
            .iter()
            .find(|record| record["decision_id"] == "release-environment")
            .expect("environment decision")
            .clone();

        let identity = authorization["authorization"]
            .as_object()
            .expect("authorization identity");
        let archives = asset_bytes
            .iter()
            .map(|(target, _)| {
                let extension = if target == "x86_64-pc-windows-msvc" {
                    "zip"
                } else {
                    "tar.gz"
                };
                json!({
                    "target": target,
                    "archive_name": format!("clinker-v3.0.0-{target}.{extension}"),
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
                    "ref": "refs/tags/v3.0.0",
                    "source_sha": identity["source_sha"],
                    "runner_environment": "github-hosted",
                })
            })
            .collect::<Vec<_>>();
        let candidate_value = json!({
            "schema": "clinker.candidate-evidence/v1",
            "kind": "candidate",
            "state": "candidate-verified",
            "revision": 0,
            "release_status": "incomplete",
            "completion_eligible": false,
            "immutable_authority_sha256": authorization_digest,
            "candidate_authorization_sha256": authorization_digest,
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
            "build_run_id": "300",
            "build_head_sha": identity["source_sha"],
            "publish_workflow_path": ".github/workflows/publish-release.yml",
            "archives": archives,
            "attestations": attestations,
            "tag_mutation_performed": false,
            "tag_readback_ref": "https://api.github.com/repos/rustpunk/clinker/git/ref/tags/v3.0.0",
            "release_trigger_event_ref": identity["ci_run_ref"],
        });

        let authorization_path = decision_dir.join("release-candidate-authorization.json");
        let decision_path = decision_dir.join("release-candidate.json");
        let approval_path = decision_dir.join("publication-approval.json");
        let candidate_path = root.path().join("candidate-evidence.json");
        write_canonical(&authorization_path, &authorization);
        write_canonical(&decision_path, &decision);
        write_canonical(&approval_path, &approval);
        write_canonical(&decision_dir.join("release-environment.json"), &environment);
        write_canonical(&candidate_path, &candidate_value);
        let publication = root.path().join("publication-evidence.json");
        Self {
            root,
            authorization: authorization_path,
            decision: decision_path,
            approval: approval_path,
            candidate: candidate_path,
            publication,
            decision_dir,
            candidate_value,
            asset_bytes,
        }
    }

    fn schema(&self, name: &str) -> PathBuf {
        repository_root().join("scripts/release").join(name)
    }

    fn dispatch_request(&self) -> DispatchRequest {
        DispatchRequest {
            repository: "rustpunk/clinker".to_owned(),
            workflow: "publish-release.yml".to_owned(),
            decision_dir: self.decision_dir.clone(),
            authorization_record: self.authorization.clone(),
            authorization_schema: self.schema("release-candidate-authorization.schema.json"),
            decision_record: self.decision.clone(),
            decision_schema: self.schema("release-decision.schema.json"),
            approval_record: self.approval.clone(),
            candidate_evidence: self.candidate.clone(),
            evidence_schema: self.schema("release-evidence.schema.json"),
            publication_evidence: self.publication.clone(),
            discovery_deadline_seconds: 600,
        }
    }

    fn verification_request(&self, state: &str, revision: u64) -> VerificationRequest {
        VerificationRequest {
            repository: "rustpunk/clinker".to_owned(),
            decision_dir: self.decision_dir.clone(),
            authorization_record: self.authorization.clone(),
            authorization_schema: self.schema("release-candidate-authorization.schema.json"),
            decision_record: self.decision.clone(),
            decision_schema: self.schema("release-decision.schema.json"),
            approval_record: self.approval.clone(),
            candidate_evidence: self.candidate.clone(),
            evidence_schema: self.schema("release-evidence.schema.json"),
            publication_evidence: self.publication.clone(),
            expected_state: state.to_owned(),
            expected_revision: revision,
            deadline_seconds: 120,
        }
    }

    fn protected_request(&self) -> ProtectedPublishRequest {
        let authorization = fs::read(&self.authorization).unwrap();
        let decision = fs::read(&self.decision).unwrap();
        let candidate = fs::read(&self.candidate).unwrap();
        let approval = fs::read(&self.approval).unwrap();
        ProtectedPublishRequest {
            repository: "rustpunk/clinker".to_owned(),
            candidate_tag: "v3.0.0".to_owned(),
            candidate_authorization_blob_sha: digest::git_blob_sha1_hex(&authorization),
            candidate_authorization_sha256: self.candidate_value["candidate_authorization_sha256"]
                .as_str()
                .unwrap()
                .to_owned(),
            candidate_decision_blob_sha: digest::git_blob_sha1_hex(&decision),
            candidate_evidence_blob_sha: digest::git_blob_sha1_hex(&candidate),
            source_sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            build_workflow_sha: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned(),
            publish_workflow_ref: "v3.0.0".to_owned(),
            publish_workflow_sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
            candidate_release_id: "release-300".to_owned(),
            approval_payload_blob_sha: digest::git_blob_sha1_hex(&approval),
            approval_record_sha256: digest::sha256_hex(&approval),
            approval_mode: "two-person-non-self".to_owned(),
            authorization_schema: self.schema("release-candidate-authorization.schema.json"),
            decision_schema: self.schema("release-decision.schema.json"),
            evidence_schema: self.schema("release-evidence.schema.json"),
            decision_dir: self.decision_dir.clone(),
            context: WorkflowContext {
                repository: "rustpunk/clinker".to_owned(),
                run_id: "500".to_owned(),
                run_attempt: 1,
                workflow_ref:
                    "rustpunk/clinker/.github/workflows/publish-release.yml@refs/tags/v3.0.0"
                        .to_owned(),
                git_ref: "refs/tags/v3.0.0".to_owned(),
                source_sha: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                actor: "maintainer:trigger".to_owned(),
            },
            deadline_seconds: 600,
        }
    }
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root")
}

fn canonical_value(value: &Value) -> canonical::CanonicalValue {
    canonical::parse_json(&serde_json::to_vec(value).unwrap()).unwrap()
}

fn write_canonical(path: &Path, value: &Value) -> Vec<u8> {
    let bytes = canonical::to_bytes(&canonical_value(value)).unwrap();
    fs::write(path, &bytes).expect("canonical fixture");
    bytes
}

fn json_file(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("JSON file")).expect("valid JSON")
}

fn dispatch_responses(fixture: &StateFixture) -> Vec<FakeResponse> {
    let authorization = fs::read(&fixture.authorization).unwrap();
    let approval = fs::read(&fixture.approval).unwrap();
    let decision = fs::read(&fixture.decision).unwrap();
    let candidate = fs::read(&fixture.candidate).unwrap();
    vec![
        FakeResponse::Json(json!({"sha": digest::git_blob_sha1_hex(&authorization)})),
        FakeResponse::Json(json!({"sha": digest::git_blob_sha1_hex(&approval)})),
        FakeResponse::Json(json!({"sha": digest::git_blob_sha1_hex(&decision)})),
        FakeResponse::Json(json!({"sha": digest::git_blob_sha1_hex(&candidate)})),
        FakeResponse::Json(Value::Null),
        FakeResponse::Json(json!({"workflow_runs": [{
            "id": "500", "run_attempt": 1,
            "head_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "html_url": "https://github.com/rustpunk/clinker/actions/runs/500",
            "actor": {"login": "maintainer:trigger"},
            "created_at": "2026-08-01T10:00:00Z"
        }]})),
        FakeResponse::Json(json!({"jobs": [{
            "id": "501", "run_id": "500", "name": "publish-approved-release",
            "environment": "release"
        }]})),
    ]
}

#[test]
fn protected_dispatch_interface_revisioned_state_machine() {
    let fixture = StateFixture::new();
    let mut transport = RecordingTransport::new(dispatch_responses(&fixture));
    publication::dispatch(&fixture.dispatch_request(), &mut transport).expect("dispatch");
    assert_eq!(transport.requests.len(), 7);
    assert!(transport.responses.is_empty());
    assert_eq!(
        fs::metadata(&fixture.publication)
            .expect("publication metadata")
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let initial_bytes = fs::read(&fixture.publication).unwrap();
    let initial = json_file(&fixture.publication);
    assert_eq!(initial["state"], "awaiting-approval");
    assert_eq!(initial["revision"], 0);
    assert_eq!(initial["release_status"], "incomplete");
    assert_eq!(initial["completion_eligible"], false);

    let mut replay = RecordingTransport::default();
    publication::dispatch(&fixture.dispatch_request(), &mut replay).expect("dispatch replay");
    assert!(
        replay.requests.is_empty(),
        "replay must not redispatch or upload"
    );
    assert_eq!(fs::read(&fixture.publication).unwrap(), initial_bytes);

    let begin = InspectionRequest {
        repository: "rustpunk/clinker".to_owned(),
        publication_evidence: fixture.publication.clone(),
        evidence_schema: fixture.schema("release-evidence.schema.json"),
        expected_state: "awaiting-approval".to_owned(),
        expected_revision: 0,
    };
    let lock = EvidenceLock::acquire(&fixture.publication).expect("hold evidence lock");
    let mut contended = RecordingTransport::new([FakeResponse::Json(json!({
        "login": "maintainer:inspector"
    }))]);
    assert!(publication::begin_inspection(&begin, &mut contended).is_err());
    assert_eq!(fs::read(&fixture.publication).unwrap(), initial_bytes);
    drop(lock);

    let mut begin_transport = RecordingTransport::new([FakeResponse::Json(json!({
        "login": "maintainer:inspector"
    }))]);
    publication::begin_inspection(&begin, &mut begin_transport).expect("begin inspection");
    assert_eq!(json_file(&fixture.publication)["revision"], 1);

    let complete = InspectionRequest {
        expected_state: "inspection-started".to_owned(),
        expected_revision: 1,
        ..begin.clone()
    };
    let mut complete_transport = RecordingTransport::new([
        FakeResponse::Json(json!({"login": "maintainer:inspector"})),
        FakeResponse::Json(json!({
            "id": "500", "run_attempt": 1,
            "head_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        })),
    ]);
    publication::complete_inspection(&complete, &mut complete_transport)
        .expect("complete inspection");
    assert_eq!(json_file(&fixture.publication)["revision"], 2);
    let revision_two = fs::read(&fixture.publication).unwrap();

    let mut stale = RecordingTransport::default();
    assert!(publication::begin_inspection(&begin, &mut stale).is_err());
    assert!(stale.requests.is_empty());
    assert_eq!(fs::read(&fixture.publication).unwrap(), revision_two);

    let approval = fixture.verification_request("inspection-completed", 2);
    let mut approval_transport = RecordingTransport::new([FakeResponse::Json(json!({
        "run_id": "500", "job_id": "501", "environment": "release",
        "candidate_tag": "v3.0.0", "state": "approved",
        "approval_kind": "manual", "automated_approval": false,
        "actor": {"login": "maintainer:release"},
        "approved_at": "2026-08-01T10:10:00Z"
    }))]);
    publication::verify_approval(&approval, &mut approval_transport).expect("approval readback");
    assert_eq!(json_file(&fixture.publication)["revision"], 3);
    let approved_bytes = fs::read(&fixture.publication).unwrap();

    let partial_release = public_release(&fixture, false);
    let wait = fixture.verification_request("approved", 3);
    let mut partial = RecordingTransport::new([
        FakeResponse::Json(successful_run()),
        FakeResponse::Json(successful_job()),
        FakeResponse::Json(partial_release),
    ]);
    assert!(publication::wait_and_verify(&wait, &mut partial).is_err());
    assert_eq!(fs::read(&fixture.publication).unwrap(), approved_bytes);

    let mut public = RecordingTransport::new([
        FakeResponse::Json(successful_run()),
        FakeResponse::Json(successful_job()),
        FakeResponse::Json(public_release(&fixture, true)),
    ]);
    publication::wait_and_verify(&wait, &mut public).expect("public readback");
    let final_value = json_file(&fixture.publication);
    assert_eq!(final_value["state"], "public-verified");
    assert_eq!(final_value["revision"], 4);
    assert_eq!(final_value["release_status"], "incomplete");
    assert_eq!(final_value["completion_eligible"], false);

    let mut final_replay = RecordingTransport::default();
    publication::wait_and_verify(&wait, &mut final_replay).expect("final replay");
    assert!(final_replay.requests.is_empty());
}

fn successful_run() -> Value {
    json!({
        "id": "500", "run_attempt": 1,
        "head_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "conclusion": "success"
    })
}

fn successful_job() -> Value {
    json!({
        "id": "501", "run_id": "500", "name": "publish-approved-release",
        "environment": "release", "conclusion": "success",
        "completed_at": "2026-08-01T10:20:00Z"
    })
}

fn public_release(fixture: &StateFixture, complete: bool) -> Value {
    let archives = fixture.candidate_value["archives"].as_array().unwrap();
    let limit = if complete {
        archives.len()
    } else {
        archives.len() - 1
    };
    let assets = archives[..limit]
        .iter()
        .map(|archive| {
            json!({
                "name": archive["archive_name"],
                "sha256": archive["sha256"],
                "uploaded": true
            })
        })
        .collect::<Vec<_>>();
    json!({
        "id": "release-300", "tag_name": "v3.0.0",
        "target_commitish": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "is_draft": false, "immutable": true,
        "attestations_match": true,
        "html_url": "https://github.com/rustpunk/clinker/releases/tag/v3.0.0",
        "assets": assets
    })
}

#[test]
fn protected_worker_publishes_once_and_public_replay_is_read_only() {
    let fixture = StateFixture::new();
    let request = fixture.protected_request();
    let mut transport = RecordingTransport::new(protected_worker_success(&fixture));
    publication::protected_publish(&request, &mut transport).expect("protected publication");
    assert!(transport.responses.is_empty());
    assert_eq!(
        transport
            .requests
            .iter()
            .filter(|request| request.method == clinker_release_policy::cli::github::Method::Patch)
            .count(),
        1,
        "the worker may perform one draft-to-public mutation"
    );
    let publication = transport
        .requests
        .iter()
        .find(|request| request.method == clinker_release_policy::cli::github::Method::Patch)
        .expect("publication request");
    assert_eq!(
        publication.fields.get("body").map(String::as_str),
        Some("# Clinker v3.0.0\n\nRelease notes.")
    );
    assert_eq!(
        publication.fields.get("prerelease").map(String::as_str),
        Some("false")
    );
    assert!(
        transport
            .requests
            .iter()
            .all(|request| !request.endpoint.contains(';')),
        "typed endpoints must not contain injection-shaped input"
    );
    assert!(
        !fixture.publication.exists(),
        "the protected worker has no publication-evidence write authority"
    );

    let mut replay = RecordingTransport::new(protected_worker_replay(&fixture));
    publication::protected_publish(&request, &mut replay).expect("exact public replay");
    assert!(replay.responses.is_empty());
    assert!(
        replay
            .requests
            .iter()
            .all(|request| request.method != clinker_release_policy::cli::github::Method::Patch),
        "public replay must be verification-only"
    );
    assert!(!fixture.publication.exists());
}

#[test]
fn protected_worker_rejects_drift_partial_mutation_and_ambiguous_timeout() {
    let fixture = StateFixture::new();
    let request = fixture.protected_request();

    let mut blob_drift = protected_worker_prefix(&fixture);
    blob_drift[0] = FakeResponse::Raw(b"different authorization bytes".to_vec());
    let mut transport = RecordingTransport::new(blob_drift);
    assert!(publication::protected_publish(&request, &mut transport).is_err());
    assert_eq!(transport.requests.len(), 1);

    let mut asset_drift = protected_worker_prefix(&fixture);
    asset_drift.push(FakeResponse::Json(worker_release(&fixture, true, false)));
    let mut drifted_assets = worker_asset_responses(&fixture);
    drifted_assets[0] = FakeResponse::Raw(b"tampered asset".to_vec());
    asset_drift.extend(drifted_assets);
    let mut transport = RecordingTransport::new(asset_drift);
    assert!(publication::protected_publish(&request, &mut transport).is_err());
    assert!(
        transport
            .requests
            .iter()
            .all(|request| request.method != clinker_release_policy::cli::github::Method::Patch)
    );

    let mut partial = protected_worker_prefix(&fixture);
    partial.push(FakeResponse::Json(worker_release(&fixture, true, false)));
    partial.extend(worker_asset_responses(&fixture));
    partial.push(FakeResponse::Json(worker_release(&fixture, true, false)));
    let mut transport = RecordingTransport::new(partial);
    assert!(publication::protected_publish(&request, &mut transport).is_err());
    assert_eq!(
        transport
            .requests
            .iter()
            .filter(|request| request.method == clinker_release_policy::cli::github::Method::Patch)
            .count(),
        1
    );

    let mut ambiguous = protected_worker_prefix(&fixture);
    ambiguous.push(FakeResponse::Json(worker_release(&fixture, true, false)));
    ambiguous.extend(worker_asset_responses(&fixture));
    ambiguous.push(FakeResponse::Failure);
    let mut transport = RecordingTransport::new(ambiguous);
    assert!(publication::protected_publish(&request, &mut transport).is_err());
    assert_eq!(
        transport.requests.last().unwrap().method,
        clinker_release_policy::cli::github::Method::Patch
    );

    let mut nonimmutable = protected_worker_prefix(&fixture);
    nonimmutable.push(FakeResponse::Json(worker_release(&fixture, true, false)));
    nonimmutable.extend(worker_asset_responses(&fixture));
    nonimmutable.push(FakeResponse::Json(Value::Null));
    nonimmutable.push(FakeResponse::Json(worker_release(&fixture, false, false)));
    let mut transport = RecordingTransport::new(nonimmutable);
    assert!(publication::protected_publish(&request, &mut transport).is_err());
    assert!(!fixture.publication.exists());
}

#[test]
fn protected_worker_rejects_injection_shaped_context_before_transport() {
    let fixture = StateFixture::new();
    let mut request = fixture.protected_request();
    request.candidate_tag = "v3.0.0; touch injected".to_owned();
    let mut transport = RecordingTransport::default();
    assert!(publication::protected_publish(&request, &mut transport).is_err());
    assert!(transport.requests.is_empty());
    assert!(!fixture.root.path().join("injected").exists());
    assert!(!fixture.publication.exists());
}

fn protected_worker_prefix(fixture: &StateFixture) -> Vec<FakeResponse> {
    let authorization = fs::read(&fixture.authorization).unwrap();
    let decision = fs::read(&fixture.decision).unwrap();
    let candidate = fs::read(&fixture.candidate).unwrap();
    let approval = fs::read(&fixture.approval).unwrap();
    vec![
        FakeResponse::Raw(authorization),
        FakeResponse::Raw(decision),
        FakeResponse::Raw(candidate),
        FakeResponse::Raw(approval),
        FakeResponse::Json(json!({
            "id": "500", "run_attempt": 1,
            "head_sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "event": "workflow_dispatch", "actor": {"login": "maintainer:trigger"}
        })),
        FakeResponse::Json(json!({"jobs": [{
            "id": "501", "run_id": "500", "name": "publish-approved-release",
            "environment": "release"
        }]})),
        FakeResponse::Json(json!({
            "ref": "refs/tags/v3.0.0",
            "object": {"sha": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "type": "commit"}
        })),
    ]
}

fn protected_worker_success(fixture: &StateFixture) -> Vec<FakeResponse> {
    let mut responses = protected_worker_prefix(fixture);
    responses.push(FakeResponse::Json(worker_release(fixture, true, false)));
    responses.extend(worker_asset_responses(fixture));
    responses.push(FakeResponse::Json(Value::Null));
    responses.push(FakeResponse::Json(worker_release(fixture, false, true)));
    responses.extend(worker_asset_responses(fixture));
    responses
}

fn protected_worker_replay(fixture: &StateFixture) -> Vec<FakeResponse> {
    let mut responses = protected_worker_prefix(fixture);
    responses.push(FakeResponse::Json(worker_release(fixture, false, true)));
    responses.extend(worker_asset_responses(fixture));
    responses
}

fn worker_release(fixture: &StateFixture, draft: bool, immutable: bool) -> Value {
    let assets = fixture.candidate_value["archives"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .map(|(index, archive)| {
            json!({"id": format!("asset-{index}"), "name": archive["archive_name"]})
        })
        .collect::<Vec<_>>();
    json!({
        "id": "release-300", "tag_name": "v3.0.0",
        "target_commitish": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "draft": draft, "prerelease": false,
        "body": "# Clinker v3.0.0\n\nRelease notes.",
        "immutable": immutable, "assets": assets
    })
}

fn worker_asset_responses(fixture: &StateFixture) -> Vec<FakeResponse> {
    fixture
        .asset_bytes
        .iter()
        .map(|(_, bytes)| FakeResponse::Raw(bytes.clone()))
        .collect()
}

#[test]
fn normative_schemas_and_direct_workflows() {
    let repository = repository_root();
    for (name, id) in [
        ("release-decision.schema.json", "clinker.decision/v1"),
        (
            "release-candidate-authorization.schema.json",
            "clinker.release-candidate-authorization/v1",
        ),
        (
            "release-evidence.schema.json",
            "clinker.release-evidence/v1",
        ),
    ] {
        let schema: Value = serde_json::from_slice(
            &fs::read(repository.join("scripts/release").join(name)).expect("schema"),
        )
        .expect("Draft 2020-12 schema JSON");
        assert_eq!(
            schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
        assert_eq!(schema["$id"], id);
        let description = schema["description"].as_str().expect("schema description");
        assert!(description.contains("clinker-release-policy"));
        assert!(description.contains("Rust"));
        assert!(!description.contains(".py"));
    }

    let evidence: Value = serde_json::from_slice(
        &fs::read(repository.join("scripts/release/release-evidence.schema.json"))
            .expect("evidence schema"),
    )
    .unwrap();
    let candidate_required = evidence["$defs"]["candidate"]["required"]
        .as_array()
        .unwrap();
    for field in [
        "state",
        "revision",
        "release_status",
        "completion_eligible",
        "immutable_authority_sha256",
    ] {
        assert!(candidate_required.contains(&json!(field)));
    }
    let publication_states = evidence["$defs"]["publication"]["oneOf"]
        .as_array()
        .unwrap()
        .iter()
        .map(|branch| {
            (
                branch["allOf"][1]["properties"]["state"]["const"]
                    .as_str()
                    .unwrap()
                    .to_owned(),
                branch["allOf"][1]["properties"]["revision"]["const"]
                    .as_u64()
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(
        publication_states,
        [
            ("awaiting-approval".to_owned(), 0),
            ("inspection-started".to_owned(), 1),
            ("inspection-completed".to_owned(), 2),
            ("approved".to_owned(), 3),
            ("public-verified".to_owned(), 4),
        ]
    );

    let release = fs::read_to_string(repository.join(".github/workflows/release.yml")).unwrap();
    let publish =
        fs::read_to_string(repository.join(".github/workflows/publish-release.yml")).unwrap();
    for workflow in [&release, &publish] {
        assert!(!workflow.contains("python"));
        assert!(!workflow.contains(".py"));
        assert!(!workflow.contains("scripts/release/build-bundle.sh"));
        assert!(!workflow.contains("scripts/release/verify-release.sh"));
        assert!(workflow.contains("tools/release-policy/Cargo.toml"));
        assert!(workflow.contains("--locked"));
    }
    assert!(release.contains("inventory check"));
    assert!(release.contains("release build-bundle"));
    assert!(release.contains("release verify"));
    assert!(publish.contains("publication protected-publish"));
}

#[test]
fn exact_downstream_validator_argv_are_typed() {
    let fixture = Fixture::new();
    for arguments in [
        vec![
            "evidence",
            "validate",
            "--kind",
            "candidate",
            "--schema",
            "evidence-schema.json",
            "--manifest",
            "target/release-policy/candidate-evidence.json",
        ],
        vec![
            "evidence",
            "validate",
            "--kind",
            "publication",
            "--schema",
            "evidence-schema.json",
            "--manifest",
            "target/release-policy/candidate-evidence.json",
        ],
        vec![
            "decision",
            "validate",
            "--schema",
            "decision-schema.json",
            "--record",
            "decisions/candidate.json",
            "--authorization-schema",
            "authorization-schema.json",
            "--authorization-record",
            "decisions/authorization.json",
            "--candidate-evidence",
            "target/release-policy/candidate-evidence.json",
            "--require-accepted",
        ],
        vec![
            "release",
            "verify",
            "--repo",
            "rustpunk/clinker",
            "--decision-dir",
            "decisions",
            "--authorization-record",
            "decisions/authorization.json",
            "--authorization-schema",
            "authorization-schema.json",
            "--decision-record",
            "decisions/candidate.json",
            "--decision-schema",
            "decision-schema.json",
            "--require-private",
            "--fresh-download",
            "--evidence-kind",
            "candidate",
            "--evidence-schema",
            "evidence-schema.json",
            "--evidence-manifest",
            "target/release-policy/publication-evidence.json",
        ],
    ] {
        assert_public_command_was_dispatched(&fixture.run(&arguments));
    }
}

#[test]
fn candidate_validator_rejects_metadata_and_release_entry_drift() {
    let fixture = StateFixture::new();
    assert!(validate_decision_candidate(&fixture, &fixture.candidate_value).is_ok());
    for (field, value) in [
        ("state", json!("approved")),
        ("revision", json!(1)),
        ("release_status", json!("complete")),
        ("completion_eligible", json!(true)),
        ("immutable_authority_sha256", json!("0".repeat(64))),
        ("build_workflow_path", json!(".github/workflows/other.yml")),
        ("build_run_id", json!("")),
        ("build_head_sha", json!("0".repeat(40))),
        (
            "publish_workflow_path",
            json!(".github/workflows/other.yml"),
        ),
    ] {
        let mut candidate = fixture.candidate_value.clone();
        candidate[field] = value;
        assert!(
            validate_decision_candidate(&fixture, &candidate).is_err(),
            "{field} drift must fail"
        );
    }

    let mut unknown = fixture.candidate_value.clone();
    unknown["unexpected"] = json!(true);
    assert!(validate_decision_candidate(&fixture, &unknown).is_err());

    let mut short_archives = fixture.candidate_value.clone();
    short_archives["archives"].as_array_mut().unwrap().pop();
    assert!(validate_decision_candidate(&fixture, &short_archives).is_err());

    let mut duplicate_archive = fixture.candidate_value.clone();
    duplicate_archive["archives"][1] = duplicate_archive["archives"][0].clone();
    assert!(validate_decision_candidate(&fixture, &duplicate_archive).is_err());

    let mut duplicate_attestation = fixture.candidate_value.clone();
    duplicate_attestation["attestations"][1] = duplicate_attestation["attestations"][0].clone();
    assert!(validate_decision_candidate(&fixture, &duplicate_attestation).is_err());
}

fn validate_decision_candidate(fixture: &StateFixture, candidate: &Value) -> Result<(), GateError> {
    let path = fixture.root.path().join("candidate-under-test.json");
    write_canonical(&path, candidate);
    decision::validate(&DecisionRequest {
        schema: Some(fixture.schema("release-decision.schema.json")),
        records: vec![fixture.decision.clone()],
        authorization_schema: Some(fixture.schema("release-candidate-authorization.schema.json")),
        authorization_record: Some(fixture.authorization.clone()),
        candidate_evidence: Some(path),
        require_ids: Vec::new(),
        require_authorization_id: None,
        require_authorized: true,
        require_complete: false,
        require_accepted: true,
    })
}

#[test]
fn evidence_validator_rejects_unknown_state_revision_and_completion() {
    let fixture = StateFixture::new();
    publication::validate_evidence_file(
        clinker_release_policy::evidence::EvidenceKind::Candidate,
        &fixture.schema("release-evidence.schema.json"),
        &fixture.candidate,
    )
    .expect("valid candidate");
    let mut transport = RecordingTransport::new(dispatch_responses(&fixture));
    publication::dispatch(&fixture.dispatch_request(), &mut transport).expect("dispatch");
    let initial = json_file(&fixture.publication);
    for mutation in [
        ("revision", json!(1)),
        ("completion_eligible", json!(true)),
        ("release_status", json!("complete")),
        ("unexpected", json!(true)),
    ] {
        let mut value = initial.clone();
        value[mutation.0] = mutation.1;
        write_canonical(&fixture.publication, &value);
        assert!(
            publication::validate_evidence_file(
                clinker_release_policy::evidence::EvidenceKind::Publication,
                &fixture.schema("release-evidence.schema.json"),
                &fixture.publication,
            )
            .is_err()
        );
    }
}
