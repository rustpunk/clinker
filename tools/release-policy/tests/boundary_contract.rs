use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use tempfile::TempDir;

fn repository_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("detached manifest must be beneath repository root")
        .to_path_buf()
}

fn gate(root: &Path, scope: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker-release-policy"))
        .args([
            "boundary",
            "audit",
            "--scope",
            scope,
            "--root",
            root.to_str().expect("root path UTF-8"),
        ])
        .output()
        .expect("clinker-release-policy must execute")
}

#[test]
fn exact_dependency_audit_argv_runs_the_independent_checker() {
    let output = gate(&repository_root(), "dependency");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stdout, b"dependency boundary audit passed\n");
    assert!(output.stderr.is_empty());
}

#[test]
fn current_repository_has_no_legacy_executable_surface() {
    let output = gate(&repository_root(), "rust-only");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stdout, b"Rust-only boundary audit passed\n");
    assert!(output.stderr.is_empty());
}

#[test]
fn rust_only_audit_accepts_direct_rust_surfaces_and_thin_launchers() {
    let root = rust_only_fixture();
    let output = gate(root.path(), "rust-only");
    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stdout, b"Rust-only boundary audit passed\n");
    assert!(output.stderr.is_empty());
}

#[test]
fn rust_only_audit_rejects_interpreters_semantic_shells_and_python_sources() {
    for (name, path, contents) in [
        (
            "workflow-interpreter",
            ".github/workflows/release.yml",
            "name: release\non: workflow_dispatch\npermissions: {}\njobs:\n  publish:\n    runs-on: ubuntu-24.04\n    permissions:\n      contents: read\n    steps:\n      - run: python3 scripts/release/publish-approved-release.py\n",
        ),
        (
            "semantic-shell",
            "scripts/release/extra-policy.sh",
            "#!/usr/bin/env bash\nset -euo pipefail\nif [[ -f release/inventory.toml ]]; then echo pass; fi\n",
        ),
        (
            "python-source",
            "crates/example/build.py",
            "print('not part of the Rust-only toolchain')\n",
        ),
    ] {
        let root = rust_only_fixture();
        let target = root.path().join(path);
        fs::create_dir_all(target.parent().expect("negative fixture parent"))
            .expect("negative fixture directory");
        fs::write(&target, contents).expect("negative Rust-only fixture");
        if path.ends_with(".sh") {
            executable(&target);
        }
        let output = gate(root.path(), "rust-only");
        assert_eq!(output.status.code(), Some(1), "fixture: {name}");
        assert!(output.stdout.is_empty());
        assert!(!output.stderr.is_empty());
    }
}

#[test]
fn rust_only_audit_does_not_treat_inert_fixture_text_as_an_invocation() {
    let root = rust_only_fixture();
    let frozen = root.path().join("scripts/release/fixtures/frozen.json");
    fs::create_dir_all(frozen.parent().expect("frozen fixture parent"))
        .expect("frozen fixture directory");
    fs::write(
        frozen,
        r#"{"command":"python3 scripts/release/release-evidence.py"}"#,
    )
    .expect("frozen data fixture");
    let output = gate(root.path(), "rust-only");
    assert_eq!(output.status.code(), Some(0));
}

fn rust_only_fixture() -> TempDir {
    let root = tempfile::tempdir().expect("temporary Rust-only repository");
    fs::create_dir_all(root.path().join(".github/workflows")).expect("workflow fixture directory");
    fs::write(
        root.path().join(".github/workflows/ci.yml"),
        direct_workflow("ci", "workflow verify"),
    )
    .expect("CI workflow fixture");
    fs::write(
        root.path().join(".github/workflows/release.yml"),
        direct_workflow("release", "release verify"),
    )
    .expect("release workflow fixture");
    fs::write(
        root.path().join(".github/workflows/publish-release.yml"),
        direct_workflow("publish", "publication wait-and-verify"),
    )
    .expect("publication workflow fixture");
    for (path, domain) in [
        ("scripts/ci/test-filesystem-matrix.sh", "filesystem"),
        ("scripts/release/build-bundle.sh", "release build-bundle"),
        ("scripts/release/check-inventory.sh", "inventory check"),
        ("scripts/release/check-workflow-trust.sh", "workflow verify"),
        ("scripts/release/verify-release.sh", "release verify"),
    ] {
        let target = root.path().join(path);
        fs::create_dir_all(target.parent().expect("launcher fixture parent"))
            .expect("launcher fixture directory");
        fs::write(&target, launcher(domain)).expect("launcher fixture");
        executable(&target);
    }
    root
}

fn direct_workflow(name: &str, operation: &str) -> String {
    format!(
        "name: {name}\non: workflow_dispatch\npermissions: {{}}\njobs:\n  gate:\n    runs-on: ubuntu-24.04\n    permissions:\n      contents: read\n    steps:\n      - run: cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- {operation}\n"
    )
}

fn launcher(domain: &str) -> String {
    format!(
        "#!/usr/bin/env bash\nset -euo pipefail\nSCRIPT_DIR=$(CDPATH= cd -- \"$(dirname -- \"${{BASH_SOURCE[0]}}\")\" && pwd -P)\nREPO_ROOT=$(CDPATH= cd -- \"$SCRIPT_DIR/../..\" && pwd -P)\ncd -- \"$REPO_ROOT\"\nexec cargo run --quiet --manifest-path tools/release-policy/Cargo.toml --locked --offline -- {domain} \"$@\"\n"
    )
}

fn executable(path: &Path) {
    let mut permissions = fs::metadata(path).expect("fixture metadata").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("fixture executable mode");
}
