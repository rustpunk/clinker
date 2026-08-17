//! Canonical mdBook membership for the renamed Sink pages.

use std::fs;
use std::path::{Path, PathBuf};

struct CanonicalPage {
    summary: &'static str,
    source: &'static str,
    navigation_path: &'static str,
    retired_source: &'static str,
    retired_navigation_path: &'static str,
}

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root two levels above the crate manifest")
        .to_path_buf()
}

fn assert_canonical_page(page: &CanonicalPage) {
    let root = repo_root();
    let summary = fs::read_to_string(root.join(page.summary)).expect("read mdBook SUMMARY");
    let link = format!("]({})", page.navigation_path);

    assert_eq!(
        summary.matches(&link).count(),
        1,
        "{} must navigate {} exactly once",
        page.summary,
        page.navigation_path
    );
    assert!(
        !summary.contains(page.retired_navigation_path),
        "{} still navigates retired path {}",
        page.summary,
        page.retired_navigation_path
    );
    assert!(root.join(page.source).is_file(), "missing {}", page.source);
    assert!(
        !root.join(page.retired_source).exists(),
        "retired source still exists: {}",
        page.retired_source
    );
}

#[test]
fn user_sink_page_is_canonical() {
    assert_canonical_page(&CanonicalPage {
        summary: "docs/user/src/SUMMARY.md",
        source: "docs/user/src/nodes/sink.md",
        navigation_path: "nodes/sink.md",
        retired_source: "docs/user/src/nodes/output.md",
        retired_navigation_path: "nodes/output.md",
    });
}

#[test]
fn engine_sink_page_is_canonical() {
    assert_canonical_page(&CanonicalPage {
        summary: "docs/engine/src/SUMMARY.md",
        source: "docs/engine/src/sink-internals.md",
        navigation_path: "sink-internals.md",
        retired_source: "docs/engine/src/output-internals.md",
        retired_navigation_path: "output-internals.md",
    });
}
