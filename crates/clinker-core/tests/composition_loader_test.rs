//! Integration tests for the Phase 1 composition workspace scanner
//! (Task 16c.1.3).
//!
//! Uses on-disk fixture files to drive [`scan_workspace_signatures`] through
//! its complete code path: filesystem walk, YAML parse, signature
//! validation, workspace-relative keying. Separate from the unit test
//! module in `src/config/composition/tests.rs` because the 5-fixture
//! corpus lives in `tests/fixtures/compositions/`.

use clinker_core::config::{WORKSPACE_COMPOSITION_BUDGET, scan_workspace_signatures};
use std::path::PathBuf;

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Path to the committed 5-fixture corpus rewritten in Task 16c.1.3 to
/// match the drill-canonical composition signature shape.
fn fixture_workspace_root() -> PathBuf {
    manifest_dir().join("tests").join("fixtures")
}

// ---------------------------------------------------------------------
// Task 16c.1.3 hard-gate tests
// ---------------------------------------------------------------------

/// Gate 1: loading the committed `tests/fixtures/compositions/` dir
/// returns `Ok` with exactly 5 entries, one per on-disk `.comp.yaml`.
#[test]
fn test_phase1_scanner_loads_all_fixtures() {
    let root = fixture_workspace_root();
    let table =
        scan_workspace_signatures(&root).expect("5-fixture corpus must load without errors");

    assert_eq!(
        table.len(),
        5,
        "expected 5 composition signatures, got {}: {:?}",
        table.len(),
        table.keys().collect::<Vec<_>>()
    );

    // Sanity check: the known composition names are all present.
    let names: Vec<&str> = table.values().map(|sig| sig.name.as_str()).collect();
    for expected in [
        "customer_enrich",
        "address_normalize",
        "dlq_shape",
        "nested_caller",
        "passthrough_check",
    ] {
        assert!(
            names.contains(&expected),
            "missing composition `{expected}`; got {names:?}"
        );
    }
}

/// Gate 2: a directory containing one malformed `.comp.yaml` returns an
/// `Err` with at least one E101 diagnostic.
#[test]
fn test_phase1_scanner_emits_e101_on_malformed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let comp_path = tmp.path().join("broken.comp.yaml");
    // Missing `_compose.name` makes this malformed per the canonical shape.
    std::fs::write(
        &comp_path,
        "_compose:\n  inputs: {}\n  outputs: {}\nnodes: []\n",
    )
    .expect("write fixture");

    let err = scan_workspace_signatures(tmp.path())
        .expect_err("malformed .comp.yaml must produce an error");
    assert!(
        err.iter().any(|d| d.code == "E101"),
        "expected at least one E101 diagnostic, got: {:?}",
        err.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
}

/// Gate 3: exceeding the [`WORKSPACE_COMPOSITION_BUDGET`] returns an
/// `Err` — budget enforced DURING the walk per LD-16c-17, not after.
#[test]
fn test_phase1_scanner_enforces_50_file_budget() {
    let tmp = tempfile::tempdir().expect("tempdir");
    // Write (budget + 1) syntactically-valid .comp.yaml files. Minimal
    // shape: a named composition with empty ports/config and an empty
    // body.
    for i in 0..=WORKSPACE_COMPOSITION_BUDGET {
        let content = format!(
            "_compose:\n  name: budget_test_{i}\n  inputs: {{}}\n  outputs: {{}}\n  config_schema: {{}}\n  resources_schema: {{}}\nnodes: []\n"
        );
        std::fs::write(tmp.path().join(format!("budget_{i}.comp.yaml")), content)
            .expect("write fixture");
    }

    let err = scan_workspace_signatures(tmp.path())
        .expect_err("exceeding the 50-file budget must return an error");
    assert!(
        err.iter().any(
            |d| d.code == "E101" && d.message.contains("workspace composition budget exceeded")
        ),
        "expected budget-exceeded E101, got: {:?}",
        err.iter()
            .map(|d| (&d.code, &d.message))
            .collect::<Vec<_>>()
    );
}

/// Gate 4: symbol table keys are workspace-relative — never carry the
/// absolute workspace root prefix.
#[test]
fn test_phase1_scanner_keys_are_workspace_relative() {
    let root = fixture_workspace_root();
    let table = scan_workspace_signatures(&root).expect("fixture corpus must load");

    let absolute_root_str = root.to_string_lossy();
    for key in table.keys() {
        let key_str = key.to_string_lossy();
        assert!(
            !key_str.starts_with(absolute_root_str.as_ref()),
            "symbol table key `{key_str}` must NOT start with the workspace root prefix `{absolute_root_str}`"
        );
        // And the relative path must actually reach the fixture: joining
        // it back onto root must hit a real file.
        let full = root.join(key);
        assert!(
            full.exists(),
            "workspace-relative key `{key_str}` does not resolve back to a real file under {absolute_root_str}"
        );
    }
}
