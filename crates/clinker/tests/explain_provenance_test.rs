//! CLI integration tests for `clinker explain` field provenance output.
//!
//! Tests the `--field` flag for provenance chain display and `--code` flag
//! for error code documentation dispatch.

use std::path::PathBuf;
use std::process::Command;

/// Path to the `clinker` binary built by Cargo for this test run.
fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Fixture workspace root for composition/channel tests.
fn fixture_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../clinker-core/tests/fixtures")
        .canonicalize()
        .expect("fixture root must exist")
}

#[test]
fn test_explain_field_provenance_shows_winning_layer() {
    // The nested_composition_pipeline.yaml has composition node
    // "nested_process" with config param "strict_mode" (default: false,
    // call-site: false). Provenance should show CompositionDefault as winner.
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
        stdout.contains("CompositionDefault"),
        "output must show CompositionDefault layer.\nstdout: {stdout}"
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

    // Must mention the unknown param
    assert!(
        stderr.contains("nonexistent_param"),
        "error must mention the unknown param.\nstderr: {stderr}"
    );

    // Must suggest valid params
    assert!(
        stderr.contains("strict_mode"),
        "error must suggest valid params like strict_mode.\nstderr: {stderr}"
    );
}

#[test]
fn test_explain_error_code_e105_outputs_doc_content() {
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg("--code")
        .arg("E105")
        .output()
        .expect("spawn clinker");

    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(
        output.status.success(),
        "clinker explain --code E105 must succeed.\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Must contain the E105 doc content
    assert!(
        stdout.contains("E105"),
        "output must contain E105.\nstdout: {stdout}"
    );

    // Must contain actual doc content (not empty)
    assert!(
        stdout.len() > 20,
        "output must contain meaningful doc content.\nstdout: {stdout}"
    );
}

#[test]
fn test_explain_error_code_e15w_retraction_help() {
    let output = Command::new(clinker_bin())
        .arg("explain")
        .arg("--code")
        .arg("E15W")
        .output()
        .expect("spawn clinker");
    assert!(
        output.status.success(),
        "clinker explain --code E15W must succeed.\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("E15W"),
        "E15W doc must reference its own code.\nstdout: {stdout}"
    );
    assert!(
        stdout.contains("retraction"),
        "E15W doc must mention the retraction protocol.\nstdout: {stdout}"
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
