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
  - type: sink
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
  - type: sink
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

/// Source -> Transform -> Sink, where the Transform is the one a full run
/// fuses with its Source and streams into its Sink.
const TRANSFORM_CHAIN: &str = r#"pipeline:
  name: run_flag_contract_preview_transform
nodes:
  - type: source
    name: input
    config:
      name: input
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: transform
    name: doubled
    input: input
    config:
      cxl: |
        emit id = id
        emit doubled = amount * 2
  - type: sink
    name: final
    input: doubled
    config:
      name: final
      type: csv
      path: configured.csv
"#;

/// Write the Transform chain and `input` into a fresh directory.
fn transform_chain_dir(input: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    fs::write(dir.path().join("input.csv"), input).expect("write source");
    fs::write(dir.path().join("pipeline.yaml"), TRANSFORM_CHAIN).expect("write pipeline");
    dir
}

fn describe(output: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The header and first `rows` data lines of `text`.
fn leading_lines(text: &str, rows: usize) -> String {
    text.split_inclusive('\n').take(rows + 1).collect()
}

/// A bounded preview of a Transform reading one Source and feeding one Sink
/// writes the Transform's first rows, the same rows a full run writes first,
/// whether the preview goes to a file or to stdout.
#[test]
fn a_preview_of_a_source_transform_sink_pipeline_prints_its_first_rows() {
    let input = "id,amount\n1,10\n2,20\n3,30\n4,40\n5,50\n";
    let expected = "id,amount,doubled\n1,10,20\n2,20,40\n3,30,60\n";

    let dir = transform_chain_dir(input);
    let output = run_in(
        dir.path(),
        &[
            "run",
            "pipeline.yaml",
            "--dry-run",
            "-n",
            "3",
            "--dry-run-output",
            "preview.csv",
        ],
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "bounded preview failed:\n{}",
        describe(&output)
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("preview.csv")).expect("preview bytes"),
        expected
    );
    assert!(!dir.path().join("configured.csv").exists());

    let to_stdout = run_in(
        dir.path(),
        &["run", "pipeline.yaml", "--dry-run", "-n", "3"],
    );
    assert_eq!(
        to_stdout.status.code(),
        Some(0),
        "bounded preview to stdout failed:\n{}",
        describe(&to_stdout)
    );
    assert_eq!(String::from_utf8_lossy(&to_stdout.stdout), expected);
    assert!(!dir.path().join("configured.csv").exists());

    let full_dir = transform_chain_dir(input);
    let full = run_in(full_dir.path(), &["run", "pipeline.yaml"]);
    assert_eq!(
        full.status.code(),
        Some(0),
        "full run failed:\n{}",
        describe(&full)
    );
    let written = fs::read_to_string(full_dir.path().join("configured.csv")).expect("full output");
    assert_eq!(leading_lines(&written, 3), expected);
}

/// A preview long enough to cross several batches and many waits on the
/// writer's bounded channel writes the same bytes on every run.
#[test]
fn a_transform_chain_preview_writes_the_same_bytes_every_run() {
    const ROWS: usize = 5_000;
    const LIMIT: usize = 4_000;
    let mut input = String::from("id,amount\n");
    for id in 1..=ROWS {
        input.push_str(&format!("{id},{}\n", id % 97));
    }
    let mut expected = String::from("id,amount,doubled\n");
    for id in 1..=LIMIT {
        let amount = id % 97;
        expected.push_str(&format!("{id},{amount},{}\n", amount * 2));
    }

    let dir = transform_chain_dir(&input);
    let limit = LIMIT.to_string();
    for run in 0..20 {
        let output = run_in(
            dir.path(),
            &[
                "run",
                "pipeline.yaml",
                "--dry-run",
                "-n",
                &limit,
                "--dry-run-output",
                "preview.csv",
            ],
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "preview run {run} failed:\n{}",
            describe(&output)
        );
        let written = fs::read_to_string(dir.path().join("preview.csv")).expect("preview bytes");
        assert!(
            written == expected,
            "preview run {run} wrote different bytes"
        );
    }
    assert!(!dir.path().join("configured.csv").exists());
}

/// A preview's read limit is the end of the input it asked for, not a
/// cancellation: the Aggregate finishes on the rows the limit admitted and the
/// preview succeeds.
#[test]
fn a_preview_read_limit_ends_the_input_and_the_aggregate_finishes_on_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    fs::write(
        dir.path().join("orders.csv"),
        "grp,amount\na,1\na,2\nb,3\na,4\nc,6\nb,5\n",
    )
    .expect("write source");
    fs::write(
        dir.path().join("pipeline.yaml"),
        r#"pipeline:
  name: run_flag_contract_preview_aggregate
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: grp, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: totals
    input: orders
    config:
      group_by: [grp]
      cxl: |
        emit grp = grp
        emit n = count(*)
        emit total = sum(amount)
  - type: sink
    name: final
    input: totals
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

    assert_eq!(
        output.status.code(),
        Some(0),
        "bounded preview failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(dir.path().join("preview.csv")).expect("preview bytes"),
        "grp,n,total\na,2,3\n"
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
