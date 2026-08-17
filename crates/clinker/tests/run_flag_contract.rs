use std::fs;
use std::process::{Command, Output};

fn clinker() -> Command {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
}

fn run_in(root: &std::path::Path, args: &[&str]) -> Output {
    clinker()
        .current_dir(root)
        .args(args)
        .output()
        .expect("run clinker")
}

fn csv_pipeline(source: &str, output: &str) -> String {
    format!(
        r#"pipeline:
  name: run_flag_contract
nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: {source}
      schema:
        - {{ name: id, type: int }}
  - type: output
    name: final
    input: input
    config:
      name: final
      type: csv
      path: {output}
"#
    )
}

#[test]
fn tracer_config_only_opens_no_source_or_output() {
    let dir = tempfile::tempdir().expect("temp dir");
    fs::write(
        dir.path().join("pipeline.yaml"),
        csv_pipeline("missing.csv", "configured.csv"),
    )
    .expect("write pipeline");

    let output = run_in(dir.path(), &["run", "pipeline.yaml", "--dry-run"]);

    assert!(
        output.status.success(),
        "config-only dry run failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!dir.path().join("configured.csv").exists());
}

#[test]
fn tracer_preview_caps_each_source_and_never_publishes_configured_output() {
    let dir = tempfile::tempdir().expect("temp dir");
    fs::write(dir.path().join("a.csv"), "id\n1\n2\n3\n").expect("write source a");
    fs::write(dir.path().join("b.csv"), "id\n10\n20\n30\n").expect("write source b");
    fs::write(
        dir.path().join("pipeline.yaml"),
        r#"pipeline:
  name: run_flag_contract_preview
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
  - type: merge
    name: combined
    inputs: [a, b]
  - type: output
    name: final
    input: combined
    config:
      name: final
      type: csv
      path: configured.csv
"#,
    )
    .expect("write pipeline");

    let output = run_in(
        dir.path(),
        &[
            "run",
            "pipeline.yaml",
            "--dry-run",
            "-n",
            "2",
            "--dry-run-output",
            "preview.csv",
        ],
    );

    assert!(
        output.status.success(),
        "bounded preview failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("preview.csv")).expect("preview bytes"),
        "id\n1\n2\n10\n20\n"
    );
    assert!(!dir.path().join("configured.csv").exists());
}

#[test]
fn tracer_invalid_policy_values_and_adjacency_fail_before_config_access() {
    for args in [
        vec!["run", "missing.yaml", "--threads", "0"],
        vec!["run", "missing.yaml", "--dry-run", "-n", "0"],
        vec!["run", "missing.yaml", "--dry-run-output", "preview.csv"],
    ] {
        let output = clinker().args(&args).output().expect("run clinker");
        assert_eq!(output.status.code(), Some(2), "args: {args:?}");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            !stderr.contains("No such file") && !stderr.contains("not found"),
            "policy error must precede config access for {args:?}: {stderr}"
        );
    }
}

#[test]
fn tracer_log_level_is_closed_and_retired_error_threshold_has_yaml_correction() {
    let invalid_level = clinker()
        .args(["run", "missing.yaml", "--log-level", "verbose"])
        .output()
        .expect("run clinker");
    assert_eq!(invalid_level.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&invalid_level.stderr).contains("--log-level"));

    let retired = clinker()
        .args(["run", "missing.yaml", "--error-threshold", "10"])
        .output()
        .expect("run clinker");
    assert_eq!(retired.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&retired.stderr);
    assert!(stderr.contains("--error-threshold"));
    assert!(stderr.contains("error_handling.type_error_threshold"));
}

#[test]
fn tracer_thread_capacity_is_the_value_reported_after_real_execution() {
    let dir = tempfile::tempdir().expect("temp dir");
    fs::write(dir.path().join("input.csv"), "id\n1\n").expect("write source");
    fs::write(
        dir.path().join("pipeline.yaml"),
        csv_pipeline("input.csv", "configured.csv"),
    )
    .expect("write pipeline");
    fs::create_dir(dir.path().join("spool")).expect("create spool");

    let output = run_in(
        dir.path(),
        &[
            "run",
            "pipeline.yaml",
            "--threads",
            "2",
            "--metrics-spool-dir",
            "spool",
        ],
    );
    assert!(
        output.status.success(),
        "real run failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let spool = fs::read_dir(dir.path().join("spool"))
        .expect("read spool")
        .next()
        .expect("one spool entry")
        .expect("spool entry")
        .path();
    let metrics: clinker_exec::metrics::ExecutionMetrics =
        serde_json::from_str(&fs::read_to_string(spool).expect("read metrics"))
            .expect("parse metrics");
    assert_eq!(metrics.thread_count, 2);
}
