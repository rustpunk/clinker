use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;

const CHECKOUT_SHA: &str = "3d3c42e5aac5ba805825da76410c181273ba90b1";

fn gate(root: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
        .current_dir(root)
        .args(["workflow", "verify"])
        .output()
        .expect("clinker-release-policy must execute")
}

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("detached manifest must be beneath repository root")
        .to_path_buf()
}

fn fixture() -> TempDir {
    let root = tempfile::tempdir().expect("temporary repository");
    fs::create_dir_all(root.path().join(".github/workflows")).expect("workflow fixture directory");
    root
}

fn minimal_workflow(extra: &str) -> String {
    format!(
        r#"name: fixture
on:
  workflow_dispatch:
permissions: {{}}
jobs:
  inspect:
    runs-on: ubuntu-24.04
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@{CHECKOUT_SHA} # v7.0.1
        with:
          persist-credentials: false
      - name: Inspect
        run: cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- workflow verify
{extra}"#
    )
}

fn write_workflow(root: &Path, name: &str, contents: &str) {
    fs::write(root.join(".github/workflows").join(name), contents)
        .expect("workflow fixture must be written");
}

#[test]
fn exact_workflow_verify_argv_accepts_the_governed_repository() {
    let output = gate(&repository_root());
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        output.stdout,
        b"Release workflow trust verification passed\n"
    );
    assert!(output.stderr.is_empty());
}

#[test]
fn typed_workflow_model_rejects_unknown_or_type_confused_policy() {
    let root = fixture();
    for (name, workflow) in [
        ("unknown", minimal_workflow("unexpected-authority: true\n")),
        (
            "permission-shorthand",
            minimal_workflow("").replacen("permissions: {}", "permissions: write-all", 1),
        ),
        (
            "job-permission-type",
            minimal_workflow("").replacen("      contents: read", "      contents: true", 1),
        ),
    ] {
        write_workflow(root.path(), &format!("{name}.yml"), &workflow);
        let output = gate(root.path());
        assert_eq!(output.status.code(), Some(1), "fixture: {name}");
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        fs::remove_file(
            root.path()
                .join(".github/workflows")
                .join(format!("{name}.yml")),
        )
        .expect("negative fixture cleanup");
    }
}

#[test]
fn workflow_trust_rejects_mutable_actions_and_persisted_checkout_credentials() {
    let root = fixture();
    for (name, workflow) in [
        (
            "mutable-action",
            minimal_workflow("").replace(CHECKOUT_SHA, "v7"),
        ),
        (
            "persisted-credentials",
            minimal_workflow("").replace("persist-credentials: false", "persist-credentials: true"),
        ),
        (
            "missing-version-annotation",
            minimal_workflow("").replace(" # v7.0.1", ""),
        ),
    ] {
        write_workflow(root.path(), &format!("{name}.yml"), &workflow);
        let output = gate(root.path());
        assert_eq!(output.status.code(), Some(1), "fixture: {name}");
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
        fs::remove_file(
            root.path()
                .join(".github/workflows")
                .join(format!("{name}.yml")),
        )
        .expect("negative fixture cleanup");
    }
}

#[test]
fn only_release_workflow_may_match_protected_version_tags() {
    let root = fixture();
    let workflow =
        minimal_workflow("").replace("  workflow_dispatch:\n", "  push:\n    tags: [\"v*\"]\n");
    write_workflow(root.path(), "competing-release.yml", &workflow);
    let output = gate(root.path());
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stdout.is_empty());
    assert!(!output.stderr.is_empty());
}
