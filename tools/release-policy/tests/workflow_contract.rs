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

fn release_workflow() -> String {
    fs::read_to_string(repository_root().join(".github/workflows/release.yml"))
        .expect("release workflow must be readable")
}

fn named_step(source: &str, name: &str) -> String {
    let marker = format!("      - name: {name}\n");
    let start = source
        .find(&marker)
        .unwrap_or_else(|| panic!("release workflow step is absent: {name}"));
    let end = source[start + marker.len()..]
        .find("\n      - name: ")
        .map_or(source.len(), |relative| start + marker.len() + relative + 1);
    source[start..end].to_owned()
}

fn assert_release_workflow_rejected(root: &Path, source: &str, scenario: &str) {
    write_workflow(root, "release.yml", source);
    let output = gate(root);
    assert_eq!(output.status.code(), Some(1), "scenario: {scenario}");
    assert!(output.stdout.is_empty(), "scenario: {scenario}");
    assert!(!output.stderr.is_empty(), "scenario: {scenario}");
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
fn release_workflow_requires_every_build_stage_in_order() {
    let root = fixture();
    let source = release_workflow();
    let names = [
        "Fetch the locked workspace dependencies",
        "Fetch the locked release policy dependencies",
        "Build the governed target executables",
        "Validate the repository release inventory",
        "Build the locked target bundle with the Rust gate",
        "Attest the exact archive subject",
        "Upload the target bundle",
    ];
    let blocks = names
        .iter()
        .map(|name| named_step(&source, name))
        .collect::<Vec<_>>();

    for (name, block) in names.iter().zip(&blocks) {
        let without_step = source.replacen(block, "", 1);
        assert_ne!(without_step, source, "scenario fixture must change: {name}");
        assert_release_workflow_rejected(root.path(), &without_step, name);
    }

    for (index, pair) in blocks.windows(2).enumerate() {
        let adjacent = format!("{}{}", pair[0], pair[1]);
        let reversed = format!("{}{}", pair[1], pair[0]);
        let reordered = source.replacen(&adjacent, &reversed, 1);
        assert_ne!(
            reordered,
            source,
            "adjacent release steps must be discoverable: {} then {}",
            names[index],
            names[index + 1]
        );
        assert_release_workflow_rejected(
            root.path(),
            &reordered,
            &format!("{} after {}", names[index], names[index + 1]),
        );
    }
}

#[test]
fn release_workflow_rejects_unmodeled_mutation_and_environment_drift() {
    let root = fixture();
    let source = release_workflow();
    let attest = named_step(&source, "Attest the exact archive subject");
    let upload_evidence = named_step(&source, "Upload the immutable candidate evidence");
    let scenarios = [
        (
            "inserted binary replacement",
            source.replacen(
                &attest,
                &format!(
                    "      - name: Replace governed binary\n        shell: bash\n        run: cp unreviewed/clinker target/release/clinker\n{attest}"
                ),
                1,
            ),
        ),
        (
            "inserted direct release publication",
            source.replacen(
                &upload_evidence,
                &format!(
                    "      - name: Publish without the gate\n        shell: bash\n        run: gh release edit \"$RELEASE_CANDIDATE_TAG\" --draft=false\n{upload_evidence}"
                ),
                1,
            ),
        ),
        (
            "attacker-controlled build target",
            source.replacen(
                "BUILD_TARGET: ${{ matrix.target }}",
                "BUILD_TARGET: ${{ github.ref_name }}",
                1,
            ),
        ),
        (
            "swapped build source identity",
            source.replacen(
                "BUILD_SOURCE_SHA: ${{ github.sha }}",
                "BUILD_SOURCE_SHA: ${{ matrix.target }}",
                1,
            ),
        ),
        (
            "attacker-controlled assembly source identity",
            source.replacen(
                "RELEASE_SOURCE_SHA: ${{ github.sha }}",
                "RELEASE_SOURCE_SHA: ${{ github.ref_name }}",
                1,
            ),
        ),
    ];

    for (scenario, mutated) in scenarios {
        assert_ne!(mutated, source, "scenario fixture must change: {scenario}");
        assert_release_workflow_rejected(root.path(), &mutated, scenario);
    }
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
