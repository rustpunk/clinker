//! End-to-end contract for the opt-in machine-run protocol.

use std::process::{Command, Output};

use serde_json::Value;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

fn fixture() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::current_dir().expect("current directory"))
        .expect("fixture directory")
}

fn write_pipeline(directory: &std::path::Path, output: &str) {
    std::fs::write(directory.join("input.csv"), "id,name\n1,Alice\n2,Bob\n")
        .expect("input fixture");
    std::fs::write(
        directory.join("pipeline.yaml"),
        format!(
            r#"pipeline:
  name: machine_protocol
nodes:
  - type: source
    name: src
    config:
      name: src
      path: input.csv
      type: csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: name, type: string }}
  - type: output
    name: out
    input: src
    config:
      name: out
      path: {output}
      type: csv
"#
        ),
    )
    .expect("pipeline fixture");
}

fn invoke(directory: &std::path::Path, extra: &[&str]) -> Output {
    let mut command = Command::new(clinker_bin());
    command
        .current_dir(directory)
        .args(["run", "pipeline.yaml"]);
    command.args(extra).output().expect("run clinker")
}

fn events(output: &Output) -> Vec<Value> {
    std::str::from_utf8(&output.stdout)
        .expect("machine stdout is UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("every stdout line is JSON"))
        .collect()
}

fn terminal(event: &Value) -> bool {
    matches!(
        event["event"].as_str(),
        Some("completed" | "failed" | "cancelled")
    )
}

fn compatible_v1(event: &Value) -> bool {
    event["protocol"] == "clinker.run"
        && event["schema"] == 1
        && event["event"].is_string()
        && event["seq"].is_u64()
        && event["batch_id"].is_string()
        && event["execution_id"].is_string()
        && event["plan_identity"].is_object()
}

fn assert_stream(stream: &[Value], batch_id: &str) {
    assert!(!stream.is_empty());
    let execution_id = stream[0]["execution_id"].as_str().expect("execution id");
    assert_eq!(
        uuid::Uuid::parse_str(execution_id)
            .expect("UUID")
            .get_version_num(),
        7
    );
    for (sequence, event) in stream.iter().enumerate() {
        assert!(compatible_v1(event), "incompatible event: {event}");
        assert_eq!(event["seq"], sequence as u64);
        assert_eq!(event["batch_id"], batch_id);
        assert_eq!(event["execution_id"], execution_id);
        assert!(serde_json::to_vec(event).expect("encode").len() <= 16 * 1024);
    }
    assert_eq!(stream.iter().filter(|event| terminal(event)).count(), 1);
}

#[test]
fn protocol_success_is_one_ordered_machine_only_stream() {
    let directory = fixture();
    write_pipeline(directory.path(), "machine.csv");
    let output = invoke(
        directory.path(),
        &["--machine", "ndjson-v1", "--batch-id", "batch-success"],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stream = events(&output);
    assert_stream(&stream, "batch-success");
    assert_eq!(stream[0]["event"], "started");
    assert_eq!(stream[0]["plan_identity"]["status"], "pending");
    assert_eq!(stream[1]["event"], "plan_resolved");
    assert_eq!(stream[1]["plan_identity"]["status"], "resolved");
    assert_eq!(stream[1]["plan_identity"]["algorithm"], "blake3");
    assert_eq!(stream[1]["plan_identity"]["version"], 1);
    assert_eq!(
        stream[1]["plan_identity"]["digest"]
            .as_str()
            .expect("digest")
            .len(),
        64
    );
    assert_eq!(stream.last().expect("terminal")["event"], "completed");
    assert!(directory.path().join("machine.csv").exists());
}

#[test]
fn protocol_admission_failure_has_one_typed_terminal() {
    let directory = fixture();
    std::fs::write(directory.path().join("pipeline.yaml"), "pipeline: [\n")
        .expect("invalid pipeline");
    let output = invoke(
        directory.path(),
        &["--machine", "ndjson-v1", "--batch-id", "batch-invalid"],
    );
    assert_eq!(output.status.code(), Some(1));
    let stream = events(&output);
    assert_stream(&stream, "batch-invalid");
    let failed = stream.last().expect("failed terminal");
    assert_eq!(failed["event"], "failed");
    assert_eq!(failed["plan_identity"]["status"], "unavailable");
    assert_eq!(failed["failure"]["code"], "admission.configuration.invalid");
    assert_eq!(failed["failure"]["retry"], "do_not_retry");
}

#[test]
fn protocol_rejects_missing_batch_and_stdout_conflicts_before_effects() {
    let directory = fixture();
    write_pipeline(directory.path(), "must-not-exist.csv");
    let missing = invoke(directory.path(), &["--machine", "ndjson-v1"]);
    assert_eq!(missing.status.code(), Some(1));
    assert!(
        String::from_utf8_lossy(&missing.stderr).contains("non-empty caller-supplied --batch-id")
    );

    let conflict = invoke(
        directory.path(),
        &[
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "batch-conflict",
            "--explain",
        ],
    );
    assert_eq!(conflict.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&conflict.stderr);
    assert!(stderr.contains("stdout conflict"), "stderr: {stderr}");
    assert!(stderr.contains("remove --explain"), "stderr: {stderr}");
    assert!(!directory.path().join("must-not-exist.csv").exists());
}

#[test]
fn protocol_consumer_accepts_additions_and_rejects_other_major_versions() {
    let mut event = serde_json::json!({
        "protocol": "clinker.run",
        "schema": 1,
        "event": "future_observation",
        "seq": 0,
        "batch_id": "batch",
        "execution_id": "019fb117-a005-7ee0-a938-1d9c237146a9",
        "plan_identity": {"status": "pending"},
    });
    event["additive"] = serde_json::json!({"safe": true});
    assert!(compatible_v1(&event));
    assert!(!terminal(&event), "unknown event kinds are never terminal");
    event["schema"] = serde_json::json!(2);
    assert!(!compatible_v1(&event));
}

#[test]
fn protocol_plain_run_does_not_emit_machine_records() {
    let directory = fixture();
    write_pipeline(directory.path(), "plain.csv");
    let output = invoke(directory.path(), &[]);
    assert!(output.status.success());
    assert!(!String::from_utf8_lossy(&output.stdout).contains("clinker.run"));
    assert!(directory.path().join("plain.csv").exists());
}
