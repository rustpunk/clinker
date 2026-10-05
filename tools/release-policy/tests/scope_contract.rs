use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use tempfile::TempDir;

const ZERO: &str = "0000000000000000000000000000000000000000";
const ABSENT: &str = "ffffffffffffffffffffffffffffffffffffffff";

fn scope(root: &Path, arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
        .current_dir(root)
        .args(["ci", "change-scope"])
        .args(arguments)
        .output()
        .expect("clinker-release-policy must execute")
}

fn docs_only(root: &Path, arguments: &[&str]) -> bool {
    let output = scope(root, arguments);
    assert_eq!(
        output.status.code(),
        Some(0),
        "arguments: {arguments:?}, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty(), "arguments: {arguments:?}");
    match output.stdout.as_slice() {
        b"docs_only=true\n" => true,
        b"docs_only=false\n" => false,
        other => panic!(
            "unexpected output for {arguments:?}: {:?}",
            String::from_utf8_lossy(other)
        ),
    }
}

fn git(root: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .current_dir(root)
        .args([
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(arguments)
        .output()
        .expect("git must execute");
    assert!(
        output.status.success(),
        "git {arguments:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("git output is UTF-8")
        .trim()
        .to_owned()
}

fn commit(root: &Path, files: &[&str], message: &str) -> String {
    for file in files {
        let path = root.join(file);
        fs::create_dir_all(path.parent().expect("fixture file has a parent"))
            .expect("fixture directory");
        fs::write(&path, message).expect("fixture file");
    }
    git(root, &["add", "--all"]);
    git(root, &["commit", "--quiet", "--allow-empty", "-m", message]);
    git(root, &["rev-parse", "HEAD"])
}

fn repository() -> TempDir {
    let root = TempDir::new().expect("temporary repository");
    git(root.path(), &["init", "--quiet"]);
    root
}

#[test]
fn a_change_under_docs_only_is_documentation_for_push_and_pull_request() {
    let root = repository();
    let base = commit(root.path(), &["crates/a/src/lib.rs"], "code");
    commit(
        root.path(),
        &["docs/user/src/page.md", "docs/engine/src/x-explainer.html"],
        "docs",
    );
    assert!(docs_only(
        root.path(),
        &["--event", "push", "--before", &base]
    ));
    assert!(docs_only(root.path(), &["--event", "pull_request"]));
    assert!(docs_only(
        root.path(),
        &["--event", "pull_request", "--before", ""]
    ));
}

#[test]
fn every_uncertain_case_runs_the_full_workflow() {
    let root = repository();
    let base = commit(root.path(), &["crates/a/src/lib.rs"], "code");
    commit(root.path(), &["docs/page.md"], "docs");
    for arguments in [
        &["--event", "workflow_dispatch"][..],
        &["--event", "workflow_dispatch", "--before", &base][..],
        &["--event", "push"][..],
        &["--event", "push", "--before", ""][..],
        &["--event", "push", "--before", ZERO][..],
        &["--event", "push", "--before", ABSENT][..],
        &["--event", "push", "--before", "HEAD^1"][..],
        &["--event", "push", "--before", "not-a-commit"][..],
    ] {
        assert!(
            !docs_only(root.path(), arguments),
            "arguments: {arguments:?}"
        );
    }
}

#[test]
fn code_examples_root_files_renames_and_empty_changes_are_not_documentation() {
    let root = repository();
    commit(root.path(), &["crates/a/src/lib.rs", "README.md"], "base");

    commit(
        root.path(),
        &["docs/page.md", "crates/a/src/lib.rs"],
        "mixed",
    );
    assert!(!docs_only(root.path(), &["--event", "pull_request"]));

    commit(root.path(), &["examples/pipelines/orders.yaml"], "example");
    assert!(!docs_only(root.path(), &["--event", "pull_request"]));

    commit(root.path(), &["README.md"], "root readme");
    assert!(!docs_only(root.path(), &["--event", "pull_request"]));

    git(root.path(), &["mv", "crates/a/src/lib.rs", "docs/lib.rs"]);
    git(
        root.path(),
        &["commit", "--quiet", "-m", "move code into docs"],
    );
    assert!(!docs_only(root.path(), &["--event", "pull_request"]));

    commit(root.path(), &[], "empty");
    assert!(!docs_only(root.path(), &["--event", "pull_request"]));
}

#[test]
fn a_root_commit_has_no_base_and_is_not_documentation() {
    let root = repository();
    commit(root.path(), &["docs/page.md"], "first");
    assert!(!docs_only(root.path(), &["--event", "pull_request"]));
}
