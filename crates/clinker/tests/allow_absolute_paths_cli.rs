//! CLI regression: `--allow-absolute-paths` must suppress the
//! absolute-path security gate on its own, with the
//! `CLINKER_ALLOW_ABSOLUTE_PATHS` environment variable unset.
//!
//! The flag is threaded onto the compile context in `main.rs` and read in
//! `PipelineConfig::compile_with_diagnostics` (which OR-combines it with the
//! env var). A prior defect let the env var work while the flag was ignored,
//! so these tests spawn the compiled binary and pin both paths to the same
//! behavior: the flag alone, the env var alone, and neither.
//!
//! Every child process explicitly controls `CLINKER_ALLOW_ABSOLUTE_PATHS` via
//! `env`/`env_remove` so the "without the env var" contract holds regardless of
//! the ambient environment the test harness inherited.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Path to the `clinker` binary built by Cargo for this test run.
fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

const ENV_VAR: &str = "CLINKER_ALLOW_ABSOLUTE_PATHS";

/// Build a pipeline whose source and output both use absolute paths — exactly
/// the shape the security gate rejects unless overridden. Returns the temp dir
/// (kept alive by the caller), the pipeline path, and the absolute output path.
fn absolute_path_fixture() -> (tempfile::TempDir, PathBuf, PathBuf) {
    let dir = tempfile::tempdir().expect("tempdir");
    let input = dir.path().join("input.csv");
    let output = dir.path().join("out.csv");
    std::fs::write(&input, "id,name\n1,Alice\n2,Bob\n").expect("write input");

    // Single-quote the paths so a Windows drive path (`C:\...`, which carries a
    // colon and backslashes) parses as one plain YAML scalar on every platform.
    let pipeline = format!(
        r#"pipeline:
  name: allow_absolute_paths_cli
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: '{input}'
    type: csv
    schema:
      - {{ name: id, type: int }}
      - {{ name: name, type: string }}
- type: output
  name: out
  input: src
  config:
    name: out
    path: '{output}'
    type: csv
    include_unmapped: true
"#,
        input = input.display(),
        output = output.display(),
    );
    let pipeline_path = dir.path().join("pipeline.yaml");
    std::fs::write(&pipeline_path, pipeline).expect("write pipeline");
    (dir, pipeline_path, output)
}

/// Absent both the flag and the env var, the gate must fire — proof the fixture
/// genuinely exercises absolute-path validation, so the passing cases below are
/// not vacuous.
#[test]
fn gate_fires_without_flag_or_env() {
    let (dir, pipeline_path, output) = absolute_path_fixture();

    let result = Command::new(clinker_bin())
        .current_dir(dir.path())
        .env_remove(ENV_VAR)
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker");

    assert!(
        !result.status.success(),
        "run without the flag or env var must fail on the absolute-path gate.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr),
    );
    let stderr = String::from_utf8_lossy(&result.stderr);
    assert!(
        stderr.contains("absolute path not allowed"),
        "failure must be the absolute-path gate specifically; got:\n{stderr}"
    );
    assert!(
        !output.exists(),
        "gated run must not produce the absolute output file"
    );
}

/// The core regression lock: `--allow-absolute-paths` alone, with the env var
/// explicitly removed from the child, must suppress the gate and run to
/// completion. Before the fix this failed because the flag never reached
/// validation.
#[test]
fn flag_without_env_suppresses_absolute_path_gate() {
    let (dir, pipeline_path, output) = absolute_path_fixture();

    let result = Command::new(clinker_bin())
        .current_dir(dir.path())
        .env_remove(ENV_VAR)
        .arg("run")
        .arg(&pipeline_path)
        .arg("--allow-absolute-paths")
        .output()
        .expect("spawn clinker");

    assert!(
        result.status.success(),
        "--allow-absolute-paths alone (env var unset) must suppress the gate.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr),
    );
    assert!(
        output.exists(),
        "override run must produce the absolute output file"
    );
    let body = std::fs::read_to_string(&output).expect("read output");
    assert!(
        body.contains("Alice") && body.contains("Bob"),
        "output must carry both source rows; got:\n{body}"
    );
}

/// Run the same absolute-path fixture once with only the flag and once with
/// only the env var, then read each output. Returns `(flag_output,
/// env_output)`.
fn run_flag_and_env_outputs() -> (String, String) {
    let (flag_dir, flag_pipeline, flag_output) = absolute_path_fixture();
    let flag_run = Command::new(clinker_bin())
        .current_dir(flag_dir.path())
        .env_remove(ENV_VAR)
        .arg("run")
        .arg(&flag_pipeline)
        .arg("--allow-absolute-paths")
        .output()
        .expect("spawn clinker (flag)");
    assert!(
        flag_run.status.success(),
        "flag-only run must succeed.\nstderr: {}",
        String::from_utf8_lossy(&flag_run.stderr),
    );

    let (env_dir, env_pipeline, env_output) = absolute_path_fixture();
    let env_run = Command::new(clinker_bin())
        .current_dir(env_dir.path())
        .env(ENV_VAR, "1")
        .arg("run")
        .arg(&env_pipeline)
        .output()
        .expect("spawn clinker (env)");
    assert!(
        env_run.status.success(),
        "env-var-only run must succeed.\nstderr: {}",
        String::from_utf8_lossy(&env_run.stderr),
    );

    (read_output(&flag_output), read_output(&env_output))
}

fn read_output(path: &Path) -> String {
    assert!(path.exists(), "expected output file at {}", path.display());
    std::fs::read_to_string(path).expect("read output")
}

/// The flag and the env var are documented as interchangeable overrides. Their
/// output for an identical pipeline must be byte-for-byte equal.
#[test]
fn flag_matches_env_var_behavior() {
    let (flag_body, env_body) = run_flag_and_env_outputs();
    assert!(
        flag_body.contains("Alice") && flag_body.contains("Bob"),
        "flag output must carry both rows; got:\n{flag_body}"
    );
    assert_eq!(
        flag_body, env_body,
        "--allow-absolute-paths and {ENV_VAR} must produce identical output"
    );
}
