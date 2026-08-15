//! CLI integration tests for `clinker explain` field provenance output.
//!
//! Tests the `--field` flag for provenance chain display and `--code` flag
//! for error code documentation dispatch.

use std::path::PathBuf;
use std::process::Command;

const REQUIRED_SECTIONS: [&str; 5] = [
    "## What it means",
    "## Example",
    "## How to fix",
    "## Technical context",
    "## See also",
];

/// Path to the `clinker` binary built by Cargo for this test run.
fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Fixture workspace root for composition/channel tests.
fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../clinker-exec/tests/fixtures")
        .canonicalize()
        .expect("fixture root must exist")
}

fn sibling_scope_workspace() -> tempfile::TempDir {
    let workspace = tempfile::tempdir().expect("create fixture workspace");
    let compositions = workspace.path().join("compositions");
    std::fs::create_dir(&compositions).expect("create compositions directory");
    let source = fixture_root().join("compositions");
    for file in ["nested_caller.comp.yaml", "address_normalize.comp.yaml"] {
        std::fs::copy(source.join(file), compositions.join(file)).expect("copy composition");
    }
    std::fs::write(
        workspace.path().join("pipeline.yaml"),
        r#"
pipeline:
  name: sibling_provenance
nodes:
  - type: source
    name: raw
    config:
      name: raw
      type: csv
      path: raw.csv
      schema:
        - { name: customer_id, type: string }
        - { name: name, type: string }
        - { name: street, type: string }
        - { name: city, type: string }
        - { name: zip, type: string }
  - type: composition
    name: left
    input: raw
    use: ./compositions/nested_caller.comp.yaml
    inputs: { raw: raw }
    config: { strict_mode: false }
  - type: composition
    name: right
    input: raw
    use: ./compositions/nested_caller.comp.yaml
    inputs: { raw: raw }
    config: { strict_mode: true }
  - type: output
    name: left_out
    input: left
    config: { name: left_out, type: csv, path: left.csv }
  - type: output
    name: right_out
    input: right
    config: { name: right_out, type: csv, path: right.csv }
"#,
    )
    .expect("write pipeline");
    workspace
}

#[test]
fn test_explain_field_provenance_shows_winning_layer() {
    // The nested_composition_pipeline.yaml has composition node
    // "nested_process" with config param "strict_mode" (default: false,
    // call-site: false). Provenance should show PipelineDefault as winner.
    let fixture_dir = fixture_root();
    let pipeline_path = fixture_dir.join("pipelines/nested_composition_pipeline.yaml");
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg(&pipeline_path)
        .arg("--field")
        .arg("nested_process.strict_mode")
        .arg("--base-dir")
        .arg(&fixture_dir)
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        output.status.success(),
        "clinker explain must succeed.\nstdout: {stdout}\nstderr: {stderr}"
    );

    // Must contain the [WON] marker
    assert!(
        stdout.contains("[WON]"),
        "output must contain [WON] marker.\nstdout: {stdout}"
    );

    // Must show the winning layer kind
    assert!(
        stdout.contains("PipelineDefault"),
        "output must show PipelineDefault layer.\nstdout: {stdout}"
    );

    // Must show the field path header
    assert!(
        stdout.contains("Field: nested_process.strict_mode"),
        "output must include the field path.\nstdout: {stdout}"
    );

    // Must show a resolved value
    assert!(
        stdout.contains("Resolved value:"),
        "output must show resolved value.\nstdout: {stdout}"
    );
}

#[test]
fn test_explain_field_unknown_path_returns_helpful_error() {
    let fixture_dir = fixture_root();
    let pipeline_path = fixture_dir.join("pipelines/nested_composition_pipeline.yaml");
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg(&pipeline_path)
        .arg("--field")
        .arg("nested_process.nonexistent_param")
        .arg("--base-dir")
        .arg(&fixture_dir)
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "clinker explain with unknown param must fail"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Must carry the stable no-match code and mention the unknown param.
    assert!(stderr.contains("[E128]"), "stderr: {stderr}");
    assert!(
        stderr.contains("nonexistent_param"),
        "error must mention the unknown param.\nstderr: {stderr}"
    );
    // Candidate disclosure is same-field only. A different parameter on the
    // same node must not be suggested.
    assert!(
        !stderr.contains("strict_mode"),
        "error must not leak a different field.\nstderr: {stderr}"
    );
}

#[test]
fn empty_query_reports_e116_with_paste_ready_help() {
    let fixture_dir = fixture_root();
    let pipeline_path = fixture_dir.join("pipelines/nested_composition_pipeline.yaml");
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg(&pipeline_path)
        .arg("--field")
        .arg("")
        .arg("--base-dir")
        .arg(&fixture_dir)
        .output()
        .expect("spawn clinker");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success());
    assert!(stderr.contains("[E127]"), "stderr: {stderr}");
    assert!(stderr.contains("--field 'node.param'"), "stderr: {stderr}");
}

#[test]
fn ambiguous_shorthand_reports_e118_and_only_exact_same_field_candidates() {
    let workspace = sibling_scope_workspace();
    let pipeline = workspace.path().join("pipeline.yaml");
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg(&pipeline)
        .arg("--field")
        .arg("inner_normalize.strict_mode")
        .arg("--base-dir")
        .arg(workspace.path())
        .output()
        .expect("spawn clinker");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "stderr: {stderr}");
    assert!(stderr.contains("[E129]"), "stderr: {stderr}");
    let left = "/v1/config/calls/left/nodes/inner_normalize/fields/strict_mode";
    let right = "/v1/config/calls/right/nodes/inner_normalize/fields/strict_mode";
    assert!(stderr.contains(left), "stderr: {stderr}");
    assert!(stderr.contains(right), "stderr: {stderr}");
    assert!(stderr.find(left) < stderr.find(right), "stderr: {stderr}");
    assert!(!stderr.contains("customer_id"), "stderr: {stderr}");

    let exact = Command::new(clinker_bin())
        .arg("explain")
        .arg(&pipeline)
        .arg("--field")
        .arg(left)
        .arg("--base-dir")
        .arg(workspace.path())
        .output()
        .expect("spawn clinker");
    assert!(
        exact.status.success(),
        "exact address must resolve.\nstderr: {}",
        String::from_utf8_lossy(&exact.stderr)
    );
}

#[test]
fn provenance_query_codes_are_registered_and_have_complete_pages() {
    for code in ["E127", "E128", "E129"] {
        let entries: Vec<_> = clinker_core_types::diagnostic::REGISTRY
            .iter()
            .filter(|entry| entry.code == code)
            .collect();
        assert_eq!(entries.len(), 1, "{code} must be registered exactly once");

        let output = Command::new(clinker_bin())
            .arg("explain")
            .arg("--code")
            .arg(code)
            .output()
            .expect("spawn clinker");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "{code}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(stdout.contains(code), "stdout: {stdout}");
        for section in REQUIRED_SECTIONS {
            assert!(stdout.contains(section), "{code} missing {section}");
        }
    }
}

#[test]
fn test_explain_error_code_e103_outputs_doc_content() {
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg("--code")
        .arg("E103")
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "clinker explain --code E103 must succeed.\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Must contain the E103 doc content
    assert!(
        stdout.contains("E103"),
        "output must contain E103.\nstdout: {stdout}"
    );

    // Must contain actual doc content (not empty)
    assert!(
        stdout.len() > 20,
        "output must contain meaningful doc content.\nstdout: {stdout}"
    );
}

#[test]
fn test_explain_error_code_e319_outputs_doc_content() {
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg("--code")
        .arg("E319")
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "clinker explain --code E319 must succeed.\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("E319"),
        "output must contain E319.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("on_miss: error"),
        "output must describe the on_miss policy.\nstdout: {stdout}"
    );
}

#[test]
fn test_explain_help_points_code_discovery_at_the_registry_list() {
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg("--help")
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "clinker explain --help must succeed.\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("Use --list to enumerate every registered diagnostic descriptor")
            && stdout.contains("exact codes accepted by --code"),
        "help output must direct code discovery to the registry-backed list.\nstdout: {stdout}"
    );
}

#[test]
fn test_explain_error_code_e15y_streaming_help() {
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg("--code")
        .arg("E15Y")
        .output()
        .expect("spawn clinker");
    assert!(
        output.status.success(),
        "clinker explain --code E15Y must succeed.\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("E15Y"),
        "E15Y doc must reference its own code.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("streaming") || stdout.contains("Streaming"),
        "E15Y doc must mention the streaming strategy interaction.\nstdout: {stdout}"
    );
}

#[test]
fn test_explain_staging_codes_are_lookup_able() {
    // Each staging-copy failure carries a stable code the operator can look up,
    // mirroring the spill subsystem's E320/E321.
    for code in ["E335", "E336", "E337"] {
        let output = Command::new(clinker_bin())
            .arg("explain")
            .arg("--code")
            .arg(code)
            .output()
            .expect("spawn clinker");
        assert!(
            output.status.success(),
            "clinker explain --code {code} must succeed.\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            stdout.contains(code),
            "{code} doc must reference its own code.\nstdout: {stdout}"
        );
    }
}

#[test]
fn test_explain_list_discovers_staging_codes() {
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg("--list")
        .output()
        .expect("spawn clinker");
    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    for code in ["E335", "E336", "E337"] {
        assert!(
            stdout.contains(&format!("Code: {code}\n")),
            "registry list must discover {code}.\nstdout: {stdout}"
        );
    }
}

#[test]
fn attempt_retention_codes_have_one_complete_registered_page() {
    for (code, failure_codes, retry) in [
        (
            "E371",
            &[
                "attempt.retention.ownership_refused",
                "attempt.retention.manifest_invalid",
                "attempt.retention.live",
                "attempt.retention.clock_ambiguous",
            ][..],
            "policy_required",
        ),
        (
            "E372",
            &[
                "attempt.retention.budget_exhausted",
                "attempt.retention.cleanup_failed",
            ][..],
            "retry_with_backoff",
        ),
    ] {
        let registry_rows: Vec<_> = clinker_core_types::diagnostic::REGISTRY
            .iter()
            .filter(|entry| entry.code == code)
            .collect();
        assert_eq!(registry_rows.len(), 1, "{code} must be registered once");

        let page_rows: Vec<_> = clinker_plan::plan::explain_provenance::EXPLAIN_PAGES
            .iter()
            .filter(|(page_code, _)| *page_code == code)
            .collect();
        assert_eq!(page_rows.len(), 1, "{code} must compile exactly one page");

        let output = Command::new(clinker_bin())
            .arg("explain")
            .arg("--code")
            .arg(code)
            .output()
            .expect("spawn clinker");
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success(),
            "clinker explain --code {code} must succeed.\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(stdout.contains(code), "stdout: {stdout}");
        for section in REQUIRED_SECTIONS {
            assert!(stdout.contains(section), "{code} missing {section}");
        }
        for failure_code in failure_codes {
            assert!(
                stdout.contains(failure_code),
                "{code} must name its registered failure family row {failure_code}"
            );
        }
        assert!(stdout.contains(retry), "{code} must pin retry advice");
        assert!(
            stdout.contains(
                "clinker attempts inspect pipelines/orders.yaml --execution-id \
                 018f47a2-9a41-7a27-b4d6-4f7137e3c159"
            ),
            "{code} must provide workspace-relative recovery guidance"
        );

        for sensitive in [
            "/home/",
            "/tmp/",
            r"C:\\Users\\",
            r"\\server\\share",
            "password=",
            "token=",
            "record={",
            ".clinker-attempts",
        ] {
            assert!(
                !stdout.contains(sensitive),
                "{code} explain output disclosed forbidden detail {sensitive:?}"
            );
        }
    }
}

#[test]
fn attempt_explain_pages_match_typed_diagnostic_identity() {
    use clinker_core_types::diagnostic::{
        AttemptDiagnosticData, AttemptOperation, FinalVisibility,
    };

    for (failure_code, expected_code) in [
        ("attempt.retention.ownership_refused", "E371"),
        ("attempt.retention.manifest_invalid", "E371"),
        ("attempt.retention.live", "E371"),
        ("attempt.retention.clock_ambiguous", "E371"),
        ("attempt.retention.budget_exhausted", "E372"),
        ("attempt.retention.cleanup_failed", "E372"),
    ] {
        let data = AttemptDiagnosticData::for_failure(
            failure_code,
            AttemptOperation::Inspect,
            "018f47a2-9a41-7a27-b4d6-4f7137e3c159",
            None,
            FinalVisibility::Unknown,
            true,
            "pipelines/orders.yaml",
        )
        .expect("registered retained-attempt diagnostic");
        assert_eq!(data.diagnostic_code(), expected_code);

        let page = clinker_plan::plan::explain_provenance::explain_code(expected_code)
            .expect("typed diagnostic code has a compiled page");
        assert!(page.contains(data.failure_code()));
        assert!(page.contains(data.retry_advice().as_str()));
        assert!(page.contains(data.recovery_command()));
    }
}
