use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

fn guess(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(root)
        .arg("guess")
        .arg("pipeline.yaml")
        .args(args)
        .output()
        .expect("spawn clinker guess")
}

fn guess_with_env(root: &Path, args: &[&str], name: &str, value: &str) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(root)
        .env(name, value)
        .arg("guess")
        .arg("pipeline.yaml")
        .args(args)
        .output()
        .expect("spawn clinker guess with environment")
}

fn spawn_write_at_barrier(root: &Path, barrier: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(root)
        .env("CLINKER_TEST_GUESS_WRITE_BARRIER", barrier)
        .args([
            "guess",
            "pipeline.yaml",
            "--write",
            "--field",
            "values.tags",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn barred multiplicity guess")
}

fn wait_for_barrier(barrier: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !barrier.join("ready").exists() {
        assert!(Instant::now() < deadline, "guess write barrier timed out");
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn write_pipeline(root: &Path, format: &str, options: &str, input: &str, extension: &str) {
    std::fs::write(
        root.join("pipeline.yaml"),
        format!(
            "pipeline:\n  name: multiplicity_guess\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: {format}\n      path: input.{extension}{options}\n      schema:\n        - name: tags\n          type: string\n"
        ),
    )
    .expect("write multiplicity pipeline");
    std::fs::write(root.join(format!("input.{extension}")), input)
        .expect("write multiplicity input");
}

fn parse_report(output: &Output) -> serde_json::Value {
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "guess stdout is not a JSON report: {error}\nstatus: {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        )
    })
}

fn assert_written_multiplicity(root: &Path, expected_split: Option<&str>) {
    let output = guess(root, &["--write", "--field", "values.tags"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    let report = parse_report(&output);
    assert_eq!(report["write"]["status"], "written");
    assert_eq!(report["multiplicity"][0]["outcome"], "conclusive");
    assert_eq!(report["multiplicity"][0]["multi_records"], 1);
    let config = std::fs::read_to_string(root.join("pipeline.yaml"))
        .expect("read written multiplicity pipeline");
    assert!(config.contains("multiple: true"), "{config}");
    match expected_split {
        Some(delimiter) => {
            assert!(config.contains("split_values:"), "{config}");
            assert!(
                config.contains(&format!("delimiter: {delimiter}")),
                "{config}"
            );
        }
        None => assert!(!config.contains("split_values:"), "{config}"),
    }
}

#[test]
fn tracer_xml_json_csv_multiplicity_evidence() {
    let xml = tempfile::tempdir().expect("temporary XML workspace");
    write_pipeline(
        xml.path(),
        "xml",
        "\n      options:\n        record_path: root/row",
        "<root><row><tags>a</tags></row><row><tags>a</tags><tags>b</tags></row></root>",
        "xml",
    );
    assert_written_multiplicity(xml.path(), None);

    let json = tempfile::tempdir().expect("temporary JSON workspace");
    write_pipeline(
        json.path(),
        "json",
        "\n      options:\n        format: array",
        r#"[{"tags":[]},{"tags":["a"]},{"tags":["a","b"]}]"#,
        "json",
    );
    assert_written_multiplicity(json.path(), None);

    let singleton_json = tempfile::tempdir().expect("temporary singleton JSON workspace");
    write_pipeline(
        singleton_json.path(),
        "json",
        "\n      options:\n        format: array",
        r#"[{"tags":[]},{"tags":["a"]},{"tags":["b"]}]"#,
        "json",
    );
    let before = std::fs::read(singleton_json.path().join("pipeline.yaml"))
        .expect("read singleton pipeline");
    let output = guess(
        singleton_json.path(),
        &["--write", "--field", "values.tags"],
    );
    assert_eq!(
        output.status.code(),
        Some(3),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report = parse_report(&output);
    assert_eq!(report["write"]["reason"], "unresolved_evidence");
    assert_eq!(report["multiplicity"][0]["outcome"], "unconfirmed");
    assert_eq!(
        std::fs::read(singleton_json.path().join("pipeline.yaml"))
            .expect("read unchanged singleton pipeline"),
        before
    );

    let csv = tempfile::tempdir().expect("temporary CSV workspace");
    write_pipeline(csv.path(), "csv", "", "tags\na|b\nc\n", "csv");
    assert_written_multiplicity(csv.path(), Some("\"|\""));

    let ambiguous_csv = tempfile::tempdir().expect("temporary ambiguous CSV workspace");
    write_pipeline(
        ambiguous_csv.path(),
        "csv",
        "",
        "tags\na|b;c\nd|e;f\n",
        "csv",
    );
    let before =
        std::fs::read(ambiguous_csv.path().join("pipeline.yaml")).expect("read ambiguous pipeline");
    let output = guess(ambiguous_csv.path(), &["--write", "--field", "values.tags"]);
    assert_eq!(output.status.code(), Some(3));
    let report = parse_report(&output);
    assert_eq!(report["write"]["reason"], "unresolved_evidence");
    assert_eq!(report["multiplicity"][0]["outcome"], "review_only");
    assert_eq!(
        report["multiplicity"][0]["reason"],
        "ambiguous_interpretation"
    );
    assert_eq!(
        std::fs::read(ambiguous_csv.path().join("pipeline.yaml"))
            .expect("read unchanged ambiguous pipeline"),
        before
    );
}

#[test]
fn escaped_csv_requires_one_activated_lossless_interpretation() {
    let workspace = tempfile::tempdir().expect("temporary escaped CSV workspace");
    write_pipeline(workspace.path(), "csv", "", "tags\na\\|b|c\nplain\n", "csv");
    let output = guess(workspace.path(), &["--write", "--field", "values.tags"]);
    assert_eq!(output.status.code(), Some(0));
    let report = parse_report(&output);
    assert_eq!(report["multiplicity"][0]["outcome"], "conclusive");
    let config = std::fs::read_to_string(workspace.path().join("pipeline.yaml")).unwrap();
    assert!(config.contains("delimiter: \"|\""), "{config}");
    assert!(config.contains("escape:"), "{config}");
}

#[test]
fn exhaustive_conflict_and_adjacent_paths_do_not_authorize_a_write() {
    let csv = tempfile::tempdir().expect("temporary late-conflict workspace");
    write_pipeline(csv.path(), "csv", "", "tags\na|b\nc;d\n", "csv");
    let before = std::fs::read(csv.path().join("pipeline.yaml")).unwrap();
    let output = guess(csv.path(), &["--write", "--field", "values.tags"]);
    assert_eq!(output.status.code(), Some(3));
    let report = parse_report(&output);
    assert_eq!(report["multiplicity"][0]["outcome"], "review_only");
    assert_eq!(
        std::fs::read(csv.path().join("pipeline.yaml")).unwrap(),
        before
    );

    let json = tempfile::tempdir().expect("temporary adjacent JSON workspace");
    std::fs::write(
        json.path().join("pipeline.yaml"),
        "pipeline:\n  name: adjacent\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: json\n      path: input.json\n      options: { format: array }\n      schema:\n        - { name: tags, type: string }\n        - { name: labels, type: string, multiple: true }\n",
    )
    .unwrap();
    std::fs::write(
        json.path().join("input.json"),
        r#"[{"tags":["one"],"labels":["x","y"]},{"tags":["two"],"labels":["z"]}]"#,
    )
    .unwrap();
    let before = std::fs::read(json.path().join("pipeline.yaml")).unwrap();
    let output = guess(json.path(), &["--write", "--field", "values.tags"]);
    assert_eq!(
        output.status.code(),
        Some(3),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report = parse_report(&output);
    assert_eq!(report["multiplicity"][0]["multi_records"], 0);
    assert_eq!(report["multiplicity"][0]["singleton_records"], 2);
    assert_eq!(
        std::fs::read(json.path().join("pipeline.yaml")).unwrap(),
        before
    );
}

#[test]
fn interruption_and_parallel_writers_leave_one_valid_exact_owner() {
    let interrupted = tempfile::tempdir().expect("temporary interrupted workspace");
    write_pipeline(interrupted.path(), "csv", "", "tags\na|b\nc|d\n", "csv");
    let before = std::fs::read(interrupted.path().join("pipeline.yaml")).unwrap();
    let output = guess_with_env(
        interrupted.path(),
        &["--write", "--field", "values.tags"],
        "CLINKER_TEST_GUESS_INTERRUPT_AFTER_RECORDS",
        "1",
    );
    assert_eq!(output.status.code(), Some(130));
    assert_eq!(
        std::fs::read(interrupted.path().join("pipeline.yaml")).unwrap(),
        before
    );

    let concurrent = tempfile::tempdir().expect("temporary concurrent workspace");
    write_pipeline(
        concurrent.path(),
        "json",
        "\n      options:\n        format: array",
        r#"[{"tags":["a","b"]}]"#,
        "json",
    );
    let barrier = tempfile::tempdir().expect("temporary write barrier");
    let first = spawn_write_at_barrier(concurrent.path(), barrier.path());
    wait_for_barrier(barrier.path());
    let second = guess(concurrent.path(), &["--write", "--field", "values.tags"]);
    assert_eq!(second.status.code(), Some(3));
    assert_eq!(
        parse_report(&second)["write"]["reason"],
        "config_lock_contended"
    );
    std::fs::write(barrier.path().join("continue"), b"continue").unwrap();
    let first = first.wait_with_output().expect("collect first writer");
    assert_eq!(first.status.code(), Some(0));
    let config = std::fs::read_to_string(concurrent.path().join("pipeline.yaml")).unwrap();
    assert_eq!(config.matches("multiple: true").count(), 1);
    clinker_plan::config::parse_config(&config).expect("written config remains valid");
}

#[test]
fn report_is_deterministic_value_redacted_and_preserves_authored_bytes() {
    let workspace = tempfile::tempdir().expect("temporary deterministic workspace");
    std::fs::write(
        workspace.path().join("pipeline.yaml"),
        "pipeline:\n  name: preserved\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      path: input.csv\n      # schema stays beside its note\n      schema:\n        - name: tags # field note\n          type: string\n",
    )
    .unwrap();
    std::fs::write(workspace.path().join("input.csv"), "tags\na|b\nplain\n").unwrap();
    let first = guess(workspace.path(), &["--check", "--field", "values.tags"]);
    let second = guess(workspace.path(), &["--check", "--field", "values.tags"]);
    assert_eq!(first.status.code(), Some(0));
    assert_eq!(first.stdout, second.stdout);
    assert!(!String::from_utf8_lossy(&first.stdout).contains("a|b"));
    let written = guess(workspace.path(), &["--write", "--field", "values.tags"]);
    assert_eq!(written.status.code(), Some(0));
    let config = std::fs::read_to_string(workspace.path().join("pipeline.yaml")).unwrap();
    assert!(
        config.contains("# schema stays beside its note"),
        "{config}"
    );
    assert!(config.contains("# field note"), "{config}");
    assert!(config.find("path: input.csv").unwrap() < config.find("schema:").unwrap());
}
