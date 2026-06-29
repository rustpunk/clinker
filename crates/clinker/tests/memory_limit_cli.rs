//! CLI integration coverage for `memory.limit` overflow handling.
//!
//! A `memory.limit` whose binary-suffix scaling exceeds the addressable byte
//! range (`u64`) must fail the run with a config diagnostic, not panic or
//! silently wrap to a tiny budget. The check is the executor's startup gate,
//! reached once the source file set has been discovered, so these tests
//! provide a real input file and shell out to the compiled binary to exercise
//! the path an operator would hit.

use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Minimal single-row pipeline whose `memory.limit` is templated in. Source
/// and output use workspace-relative paths (absolute paths are rejected by the
/// path-security check), so the run executes with its working directory set to
/// the temp dir.
const PIPELINE_TEMPLATE: &str = r#"pipeline:
  name: mem_limit_overflow
  memory:
    limit: "__LIMIT__"
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// Write `pipeline.yaml` (with the limit templated in) plus a real one-row
/// input CSV into a fresh temp dir. Returns the temp dir (kept alive for the
/// run's duration).
fn pipeline_with_limit(limit: &str) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().expect("create tempdir");
    std::fs::write(tmp.path().join("in.csv"), "amount\n1\n").expect("write input csv");
    let yaml = PIPELINE_TEMPLATE.replace("__LIMIT__", limit);
    std::fs::write(tmp.path().join("pipeline.yaml"), yaml).expect("write pipeline yaml");
    tmp
}

#[test]
fn overflowing_memory_limit_fails_run_at_startup() {
    // 2^34 G = 2^34 * 1024^3 = 2^64 bytes, one past u64::MAX — the numeric
    // part parses cleanly but the suffix multiply overflows.
    let tmp = pipeline_with_limit("17179869184G");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg("pipeline.yaml")
        .current_dir(tmp.path())
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "run with an overflowing memory.limit must fail; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr),
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("memory.limit"),
        "diagnostic must name the failing setting; got:\n{stderr}"
    );
}

#[test]
fn valid_memory_limit_passes_the_startup_gate() {
    // A well-formed `memory.limit` must clear the overflow gate and let the
    // run complete successfully.
    let tmp = pipeline_with_limit("256M");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg("pipeline.yaml")
        .current_dir(tmp.path())
        .output()
        .expect("spawn clinker");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "a valid memory.limit run must succeed; stderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("memory.limit"),
        "a valid memory.limit must not trip the overflow gate; got:\n{stderr}"
    );
}
