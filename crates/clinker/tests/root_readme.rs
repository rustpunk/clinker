//! Guards the repository-root `README.md`: a GitHub visitor must land on an
//! accurate project overview, not an empty repo home. This test pins that the
//! root README exists and covers the topics issue #453 requires — description,
//! quick start, build commands, and a pointer into the docs tree — so the file
//! cannot silently regress into a stub.

use std::fs;
use std::path::PathBuf;

/// Resolve the workspace-root `README.md` from this crate's manifest dir.
/// `CARGO_MANIFEST_DIR` is `<repo>/crates/clinker`, so two parents up is the
/// workspace root regardless of the current working directory.
fn root_readme_path() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_root = manifest_dir
        .parent()
        .and_then(|p| p.parent())
        .expect("crates/clinker should have a workspace root two levels up");
    repo_root.join("README.md")
}

#[test]
fn root_readme_exists_and_is_substantial() {
    let path = root_readme_path();
    let body = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("root README.md must exist at {}: {e}", path.display()));

    assert!(
        body.len() > 1000,
        "root README.md is only {} bytes; it should be a real project overview, not a stub",
        body.len()
    );
}

#[test]
fn root_readme_covers_required_topics() {
    let path = root_readme_path();
    let body = fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("root README.md must exist at {}: {e}", path.display()));

    // Each anchor maps to a topic the root README must cover.
    let required: &[(&str, &str)] = &[
        ("# Clinker", "a top-level heading naming the project"),
        ("bounded-memory", "the bounded-memory architectural claim"),
        ("CXL", "the CXL expression language"),
        ("512", "the default memory budget"),
        ("clinker run", "a quick-start invocation"),
        (
            "cargo install --path crates/clinker",
            "a build-from-source command",
        ),
        (
            "docs/user/src/README.md",
            "a link into the user documentation",
        ),
    ];

    for &(needle, why) in required {
        assert!(
            body.contains(needle),
            "root README.md is missing {why} (expected to find {needle:?})"
        );
    }
}
