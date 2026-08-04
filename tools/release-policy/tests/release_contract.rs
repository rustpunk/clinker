use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::{Value, json};
use tempfile::TempDir;

const SOURCE_SHA: &str = "1111111111111111111111111111111111111111";
const INVENTORY: &str = r#"schema = "clinker.release-inventory/v1"
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

struct RepositoryFixture {
    root: TempDir,
    fixture_binary: PathBuf,
}

impl RepositoryFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().expect("temporary repository");
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
        fs::write(root.path().join("release/inventory.toml"), INVENTORY)
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
        .expect("Rust fixture source");
        let status = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
            .args(["-O", "-o"])
            .arg(&fixture_binary)
            .arg(&fixture_source)
            .status()
            .expect("compile Rust fixture binary");
        assert!(status.success(), "Rust fixture binary must compile");
        Self {
            root,
            fixture_binary,
        }
    }

    fn path(&self) -> &Path {
        self.root.path()
    }

    fn install_binaries(&self, target: &str) {
        let suffix = if target == "x86_64-pc-windows-msvc" {
            ".exe"
        } else {
            ""
        };
        let binary_dir = self.path().join("target").join(target).join("release");
        fs::create_dir_all(&binary_dir).expect("binary directory");
        for name in ["clinker", "cxl"] {
            let installed = binary_dir.join(format!("{name}{suffix}"));
            fs::copy(&self.fixture_binary, &installed).expect("copy fixture binary");
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;

                fs::set_permissions(&installed, fs::Permissions::from_mode(0o644))
                    .expect("simulate workflow artifact permission loss");
            }
        }
    }

    fn run(&self, arguments: &[&str]) -> Output {
        self.run_with_path(arguments, None)
    }

    fn run_with_path(&self, arguments: &[&str], path: Option<OsString>) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"));
        command.current_dir(self.path()).args(arguments);
        if let Some(path) = path {
            command.env("PATH", path);
        }
        command.output().expect("run clinker-release-policy")
    }
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repository root")
}

fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
}

fn assets(fixture: &RepositoryFixture) -> PathBuf {
    let assets = fixture.path().join("assets");
    fs::create_dir_all(&assets).expect("asset directory");
    assets
}

fn build_all_targets(fixture: &RepositoryFixture, output_dir: &Path) {
    for target in [
        "x86_64-unknown-linux-gnu",
        "x86_64-apple-darwin",
        "aarch64-apple-darwin",
        "x86_64-pc-windows-msvc",
    ] {
        fixture.install_binaries(target);
        let output = fixture.run(&[
            "release",
            "build-bundle",
            "--target",
            target,
            "--source-sha",
            SOURCE_SHA,
            "--output-dir",
            output_dir.to_str().expect("UTF-8 asset path"),
        ]);
        assert_success(&output);
    }
}

#[test]
fn inventory_check_executes_the_exact_public_argv() {
    let fixture = RepositoryFixture::new();
    let inventory = fixture.path().join("release/inventory.toml");
    let output = fixture.run(&[
        "inventory",
        "check",
        "--inventory",
        inventory.to_str().expect("UTF-8 inventory path"),
        "--repo-root",
        fixture.path().to_str().expect("UTF-8 repository path"),
        "--print-json",
    ]);
    assert_success(&output);
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 inventory output");
    assert!(stdout.contains("\"schema\":\"clinker.release-inventory/v1\""));
    assert!(stdout.contains("\"version\":\"0.1.0\""));
    assert_eq!(stdout.matches("\"archive_name\"").count(), 4);
}

#[test]
fn inventory_rejects_unknown_fields_and_path_escape() {
    let fixture = RepositoryFixture::new();
    let inventory = fixture.path().join("release/inventory.toml");
    fs::write(&inventory, format!("{INVENTORY}\nunknown = \"field\"\n"))
        .expect("mutated inventory");
    let unknown = fixture.run(&[
        "inventory",
        "check",
        "--inventory",
        "release/inventory.toml",
    ]);
    assert_eq!(unknown.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&unknown.stderr).contains("unknown"));

    let outside = tempfile::NamedTempFile::new().expect("outside inventory");
    let escaped = fixture.run(&[
        "inventory",
        "check",
        "--inventory",
        outside.path().to_str().expect("UTF-8 outside path"),
    ]);
    assert_eq!(escaped.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&escaped.stderr).contains("contain"));
}

#[test]
fn deterministic_four_target_bundle_verifies_end_to_end() {
    let fixture = RepositoryFixture::new();
    let first = assets(&fixture);
    build_all_targets(&fixture, &first);

    let verify = fixture.run(&[
        "release",
        "verify",
        "assemble",
        "--asset-dir",
        first.to_str().expect("UTF-8 asset path"),
        "--repository",
        "rustpunk/clinker",
        "--workflow",
        ".github/workflows/release.yml",
        "--ref",
        "refs/tags/v0.1.0",
        "--source-sha",
        SOURCE_SHA,
    ]);
    assert_success(&verify);
    assert!(first.join("SHA256SUMS").is_file());

    let second = fixture.path().join("reproducible");
    fs::create_dir(&second).expect("second output directory");
    let target = "x86_64-unknown-linux-gnu";
    let rebuilt = fixture.run(&[
        "release",
        "build-bundle",
        "--target",
        target,
        "--source-sha",
        SOURCE_SHA,
        "--output-dir",
        second.to_str().expect("UTF-8 output path"),
    ]);
    assert_success(&rebuilt);
    for suffix in ["", ".sha256", ".intoto.jsonl"] {
        let name = format!("clinker-v0.1.0-{target}.tar.gz{suffix}");
        assert_eq!(
            fs::read(first.join(&name)).expect("first release artifact"),
            fs::read(second.join(&name)).expect("second release artifact")
        );
    }
}

#[test]
fn verifier_rejects_digest_and_archive_layout_drift() {
    let fixture = RepositoryFixture::new();
    let asset_dir = assets(&fixture);
    build_all_targets(&fixture, &asset_dir);
    let archive = asset_dir.join("clinker-v0.1.0-x86_64-unknown-linux-gnu.tar.gz");
    fs::write(&archive, b"tampered").expect("tamper archive");
    let output = fixture.run(&[
        "release",
        "verify",
        "assemble",
        "--asset-dir",
        asset_dir.to_str().expect("UTF-8 asset path"),
        "--repository",
        "rustpunk/clinker",
        "--workflow",
        ".github/workflows/release.yml",
        "--ref",
        "refs/tags/v0.1.0",
        "--source-sha",
        SOURCE_SHA,
    ]);
    assert_eq!(output.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&output.stderr).contains("checksum"));
    assert!(!asset_dir.join("SHA256SUMS").exists());
}

#[test]
fn candidate_producer_and_readback_use_two_fresh_private_downloads() {
    let fixture = RepositoryFixture::new();
    let asset_dir = assets(&fixture);
    build_all_targets(&fixture, &asset_dir);
    assert_success(&fixture.run(&[
        "release",
        "verify",
        "assemble",
        "--asset-dir",
        asset_dir.to_str().unwrap(),
        "--repository",
        "rustpunk/clinker",
        "--workflow",
        ".github/workflows/release.yml",
        "--ref",
        "refs/tags/v0.1.0",
        "--source-sha",
        SOURCE_SHA,
    ]));

    let decision_dir = fixture.path().join("decisions");
    fs::create_dir(&decision_dir).expect("decision directory");
    let source = repository_root();
    for (from, to) in [
        (
            "scripts/release/release-candidate-authorization.schema.json",
            "authorization-schema.json",
        ),
        (
            "scripts/release/release-decision.schema.json",
            "decision-schema.json",
        ),
        (
            "scripts/release/release-evidence.schema.json",
            "evidence-schema.json",
        ),
    ] {
        fs::copy(source.join(from), fixture.path().join(to)).expect("copy schema");
    }

    let authorization_fixture: Value = serde_json::from_slice(
        &fs::read(
            source.join("scripts/release/fixtures/release-decisions/candidate-authorizations.json"),
        )
        .expect("authorization fixture"),
    )
    .expect("authorization JSON");
    let accepted_fixture: Value = serde_json::from_slice(
        &fs::read(
            source.join("scripts/release/fixtures/release-decisions/accepted-record-set.json"),
        )
        .expect("decision fixture"),
    )
    .expect("decision JSON");
    let mut authorization = authorization_fixture["authorized"].clone();
    let mut archive_digests = serde_json::Map::new();
    for target in [
        "aarch64-apple-darwin",
        "x86_64-apple-darwin",
        "x86_64-pc-windows-msvc",
        "x86_64-unknown-linux-gnu",
    ] {
        let extension = if target == "x86_64-pc-windows-msvc" {
            "zip"
        } else {
            "tar.gz"
        };
        let name = format!("clinker-v0.1.0-{target}.{extension}");
        archive_digests.insert(
            target.to_owned(),
            Value::String(clinker_release_policy::digest::sha256_hex(
                &fs::read(asset_dir.join(name)).expect("archive bytes"),
            )),
        );
    }
    let checksum_sha256 = clinker_release_policy::digest::sha256_hex(
        &fs::read(asset_dir.join("SHA256SUMS")).expect("checksum bytes"),
    );
    let identity = authorization["authorization"]
        .as_object_mut()
        .expect("authorization identity");
    for (field, value) in [
        ("candidate_tag", json!("v0.1.0")),
        ("candidate_version", json!("0.1.0")),
        ("source_sha", json!(SOURCE_SHA)),
        (
            "build_workflow_sha",
            json!("2222222222222222222222222222222222222222"),
        ),
        ("publish_workflow_ref", json!("v0.1.0")),
        ("publish_workflow_ref_resolved_sha", json!(SOURCE_SHA)),
        ("publish_workflow_sha", json!(SOURCE_SHA)),
        ("candidate_release_id", json!("release-100")),
        ("checksum_sha256", json!(checksum_sha256)),
        ("archive_digests", Value::Object(archive_digests)),
        (
            "ci_run_ref",
            json!("https://github.com/rustpunk/clinker/actions/runs/100"),
        ),
        ("changelog_ref", json!("CHANGELOG.md#010")),
        ("inventory_ref", json!("release/inventory.toml")),
        (
            "authorized_release_maintainer_ref",
            json!("maintainer:release"),
        ),
    ] {
        identity.insert(field.to_owned(), value);
    }
    let canonical_identity = clinker_release_policy::canonical::parse_json(
        &serde_json::to_vec(&authorization["authorization"]).unwrap(),
    )
    .unwrap();
    let authorization_digest = clinker_release_policy::digest::sha256_hex(
        &clinker_release_policy::canonical::to_bytes(&canonical_identity).unwrap(),
    );
    authorization["candidate_authorization_sha256"] = json!(authorization_digest);
    let authorization_path = decision_dir.join("authorization.json");
    fs::write(
        &authorization_path,
        serde_json::to_vec_pretty(&authorization).unwrap(),
    )
    .expect("authorization record");

    let mut decision = accepted_fixture["records"][6].clone();
    for field in [
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
        "approved_at",
    ] {
        decision[field] = authorization["authorization"][field].clone();
    }
    decision["candidate_authorization_ref"] = json!("decisions/authorization.json");
    decision["candidate_authorization_sha256"] = json!(authorization_digest);
    decision["candidate_tag_creation_ref"] =
        json!("https://api.github.com/repos/rustpunk/clinker/git/refs/tags/v0.1.0");
    decision["candidate_tag_readback_ref"] =
        json!("https://api.github.com/repos/rustpunk/clinker/git/ref/tags/v0.1.0");
    decision["authorization_recorded_at"] = authorization["recorded_at"].clone();
    let decision_path = decision_dir.join("decision.json");
    fs::write(
        &decision_path,
        serde_json::to_vec_pretty(&decision).unwrap(),
    )
    .expect("decision record");

    let release_notes = "# Clinker v0.1.0\n\nGenerated release notes.";
    let release_metadata = json!({
        "build_workflow_path": ".github/workflows/release.yml",
        "build_workflow_sha": "2222222222222222222222222222222222222222",
        "build_run_id": "100",
        "build_event": "push",
        "build_ref": "refs/tags/v0.1.0",
        "build_head_sha": SOURCE_SHA,
        "source_sha": SOURCE_SHA,
        "publish_workflow_ref": "v0.1.0",
        "publish_workflow_sha": SOURCE_SHA,
        "candidate_release_id": "release-100",
        "release_notes_sha256": clinker_release_policy::digest::sha256_hex(release_notes.as_bytes()),
    });
    let release_metadata = clinker_release_policy::canonical::to_bytes(
        &clinker_release_policy::canonical::parse_json(
            &serde_json::to_vec(&release_metadata).expect("release metadata JSON"),
        )
        .expect("canonical release metadata"),
    )
    .expect("release metadata bytes");
    let release_metadata = String::from_utf8(release_metadata).expect("UTF-8 release metadata");
    let release_body = format!(
        "{release_notes}\n\n<!-- clinker-release-metadata\n{}\n-->\n",
        release_metadata.trim_end()
    );
    fs::write(
        fixture.path().join("fake-release.json"),
        serde_json::to_vec(&json!({
            "body": release_body,
            "id": "release-100",
            "isDraft": true,
            "tagName": "v0.1.0",
        }))
        .unwrap(),
    )
    .expect("fake release");
    fs::write(
        fixture.path().join("fake-tag.json"),
        serde_json::to_vec(&json!({
            "ref": "refs/tags/v0.1.0",
            "url": "https://api.github.com/repos/rustpunk/clinker/git/ref/tags/v0.1.0",
            "object": {"sha": SOURCE_SHA, "type": "commit", "url": "https://api.github.com/commit"},
        }))
        .unwrap(),
    )
    .expect("fake tag");
    let fake_assets = fixture.path().join("fake-assets");
    fs::rename(&asset_dir, &fake_assets).expect("move fake assets");
    let fake_bin = fixture.path().join("fake-bin");
    fs::create_dir(&fake_bin).expect("fake binary directory");
    let fake_source = fixture.path().join("fake-gh.rs");
    fs::write(
        &fake_source,
        r#"use std::{env, fs, path::Path};
fn main() {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.get(0).map(String::as_str) == Some("release") && args.get(1).map(String::as_str) == Some("view") {
        print!("{}", fs::read_to_string("fake-release.json").unwrap());
    } else if args.get(0).map(String::as_str) == Some("api") {
        print!("{}", fs::read_to_string("fake-tag.json").unwrap());
    } else if args.get(0).map(String::as_str) == Some("release") && args.get(1).map(String::as_str) == Some("download") {
        let at = args.iter().position(|value| value == "--dir").unwrap();
        let destination = Path::new(&args[at + 1]);
        for entry in fs::read_dir("fake-assets").unwrap() {
            let entry = entry.unwrap();
            fs::copy(entry.path(), destination.join(entry.file_name())).unwrap();
        }
    } else {
        std::process::exit(2);
    }
}
"#,
    )
    .expect("fake gh source");
    let fake_gh = fake_bin.join("gh");
    let status = Command::new(std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into()))
        .args(["-O", "-o"])
        .arg(&fake_gh)
        .arg(&fake_source)
        .status()
        .expect("compile fake gh");
    assert!(status.success());
    let mut paths = vec![fake_bin];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let path = std::env::join_paths(paths).expect("fake PATH");
    let evidence = fixture.path().join("candidate-evidence.json");
    let producer = fixture.run_with_path(
        &[
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
            "decisions/decision.json",
            "--decision-schema",
            "decision-schema.json",
            "--require-private",
            "--fresh-download",
            "--evidence-kind",
            "candidate",
            "--evidence-schema",
            "evidence-schema.json",
            "--evidence-manifest",
            "candidate-evidence.json",
        ],
        Some(path.clone()),
    );
    assert_success(&producer);
    assert!(evidence.is_file());
    let readback = fixture.run_with_path(
        &[
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
            "decisions/decision.json",
            "--decision-schema",
            "decision-schema.json",
            "--candidate-evidence",
            "candidate-evidence.json",
            "--evidence-schema",
            "evidence-schema.json",
            "--require-private",
            "--fresh-download",
        ],
        Some(path),
    );
    assert_success(&readback);
}

#[test]
fn downstream_candidate_producer_argv_dispatches_before_domain_rejection() {
    let fixture = RepositoryFixture::new();
    for path in [
        "authorization.json",
        "authorization-schema.json",
        "decision.json",
        "decision-schema.json",
        "evidence-schema.json",
    ] {
        fs::write(fixture.path().join(path), "{}\n").expect("candidate fixture");
    }
    fs::create_dir(fixture.path().join("decisions")).expect("decision directory");
    let output = fixture.run(&[
        "release",
        "verify",
        "--repo",
        "rustpunk/clinker",
        "--decision-dir",
        "decisions",
        "--authorization-record",
        "authorization.json",
        "--authorization-schema",
        "authorization-schema.json",
        "--decision-record",
        "decision.json",
        "--decision-schema",
        "decision-schema.json",
        "--require-private",
        "--fresh-download",
        "--evidence-kind",
        "candidate",
        "--evidence-schema",
        "evidence-schema.json",
        "--evidence-manifest",
        "candidate-evidence.json",
    ]);
    assert_eq!(output.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("usage:"));
}

#[test]
fn downstream_candidate_readback_argv_dispatches_before_domain_rejection() {
    let fixture = RepositoryFixture::new();
    for path in [
        "authorization.json",
        "authorization-schema.json",
        "decision.json",
        "decision-schema.json",
        "candidate-evidence.json",
        "evidence-schema.json",
    ] {
        fs::write(fixture.path().join(path), "{}\n").expect("candidate fixture");
    }
    fs::create_dir(fixture.path().join("decisions")).expect("decision directory");
    let output = fixture.run(&[
        "release",
        "verify",
        "--repo",
        "rustpunk/clinker",
        "--decision-dir",
        "decisions",
        "--authorization-record",
        "authorization.json",
        "--authorization-schema",
        "authorization-schema.json",
        "--decision-record",
        "decision.json",
        "--decision-schema",
        "decision-schema.json",
        "--candidate-evidence",
        "candidate-evidence.json",
        "--evidence-schema",
        "evidence-schema.json",
        "--require-private",
        "--fresh-download",
    ]);
    assert_eq!(output.status.code(), Some(1));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("usage:"));
}

#[test]
fn malformed_or_authority_override_argv_fails_before_mutation() {
    let fixture = RepositoryFixture::new();
    let output = fixture.run(&[
        "release",
        "verify",
        "--repo",
        "rustpunk/clinker",
        "--source-sha",
        SOURCE_SHA,
    ]);
    assert_eq!(output.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument"));
    assert!(!fixture.path().join("candidate-evidence.json").exists());
    assert!(!fixture.path().join("assets").exists());
}
