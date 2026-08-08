//! End-to-end contract for the opt-in machine-run protocol.

use std::process::{Command, Output};

use serde_json::Value;

const OVERSIZED_REST_PAGE_BYTES: usize = 64 * 1024 * 1024 + 1;

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
      path: "{output}"
      type: csv
"#
        ),
    )
    .expect("pipeline fixture");
}

fn write_local_lineage_policy(directory: &std::path::Path) {
    std::fs::write(
        directory.join("clinker.toml"),
        r#"[observability]

[observability.otlp]
endpoint = "https://collector.example.com"

[observability.otlp.auth]
mode = "none"

[observability.lineage]
identity_mode = "local_diagnostic_paths"
"#,
    )
    .expect("local lineage policy");
}

fn write_rest_pipeline(directory: &std::path::Path, url: &str, pagination: &str) {
    std::fs::write(
        directory.join("pipeline.yaml"),
        format!(
            r#"pipeline:
  name: machine_rest_failure
nodes:
  - type: source
    name: api
    config:
      name: api
      type: json
      options: {{ format: array }}
      transport:
        kind: rest
        url: {url:?}
        max_pages: 2
        retries: 0
        timeout_secs: 1
{pagination}
      schema:
        - {{ name: id, type: int }}
  - type: output
    name: out
    input: api
    config: {{ name: out, path: out.csv, type: csv }}
"#
        ),
    )
    .expect("REST pipeline");
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

fn publication_artifacts(stream: &[Value]) -> Vec<Value> {
    stream
        .iter()
        .filter(|event| event["event"] == "publication_artifacts")
        .flat_map(|event| {
            event["publication"]["artifacts"]
                .as_array()
                .expect("artifact chunk")
                .iter()
                .cloned()
        })
        .collect()
}

#[test]
fn protocol_success_is_one_ordered_machine_only_stream() {
    let directory = fixture();
    write_pipeline(directory.path(), "{batch_id}-{execution_id}.csv");
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
    let plan_resolved = stream
        .iter()
        .find(|event| event["event"] == "plan_resolved")
        .expect("plan identity event");
    assert_eq!(plan_resolved["plan_identity"]["status"], "resolved");
    assert_eq!(plan_resolved["plan_identity"]["algorithm"], "blake3");
    assert_eq!(plan_resolved["plan_identity"]["version"], 1);
    assert_eq!(
        plan_resolved["plan_identity"]["digest"]
            .as_str()
            .expect("digest")
            .len(),
        64
    );
    assert_eq!(stream.last().expect("terminal")["event"], "completed");
    let execution_id = stream[0]["execution_id"].as_str().expect("execution id");
    assert!(
        directory
            .path()
            .join(format!("batch-success-{execution_id}.csv"))
            .exists(),
        "the emitted identities must be the output-template identities"
    );
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

#[test]
fn protocol_allows_file_lineage_but_rejects_lineage_stdout() {
    let directory = fixture();
    write_pipeline(directory.path(), "lineage-only.csv");
    write_local_lineage_policy(directory.path());
    let file_lineage = invoke(
        directory.path(),
        &[
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "lineage-file",
            "--lineage",
            "lineage.ndjson",
        ],
    );
    assert!(
        file_lineage.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&file_lineage.stderr)
    );
    let stream = events(&file_lineage);
    assert_stream(&stream, "lineage-file");
    assert!(stream.iter().any(|event| event["event"] == "plan_resolved"));
    assert!(directory.path().join("lineage.ndjson").exists());
    assert!(!directory.path().join("lineage-only.csv").exists());

    let stdout_lineage = invoke(
        directory.path(),
        &[
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "lineage-stdout",
            "--lineage",
            "-",
        ],
    );
    assert_eq!(stdout_lineage.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&stdout_lineage.stderr).contains("--lineage -"));
}

#[test]
fn protocol_retry_restarts_with_a_fresh_execution_identity() {
    let directory = fixture();
    write_pipeline(directory.path(), "{execution_id}.csv");
    let first = invoke(
        directory.path(),
        &["--machine", "ndjson-v1", "--batch-id", "same-batch"],
    );
    let second = invoke(
        directory.path(),
        &["--machine", "ndjson-v1", "--batch-id", "same-batch"],
    );
    assert!(first.status.success() && second.status.success());
    let first_events = events(&first);
    let second_events = events(&second);
    assert_stream(&first_events, "same-batch");
    assert_stream(&second_events, "same-batch");
    let first_id = first_events[0]["execution_id"].as_str().expect("first id");
    let second_id = second_events[0]["execution_id"]
        .as_str()
        .expect("second id");
    assert_ne!(first_id, second_id);
    assert_eq!(
        std::fs::read(directory.path().join(format!("{first_id}.csv"))).expect("first output"),
        std::fs::read(directory.path().join(format!("{second_id}.csv"))).expect("second output")
    );
}

#[test]
fn protocol_failed_terminals_cover_the_exact_retry_vocabulary() {
    let unavailable = fixture();
    let reserved = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve port");
    let unavailable_url = format!(
        "http://{}",
        reserved.local_addr().expect("reserved address")
    );
    drop(reserved);
    write_rest_pipeline(unavailable.path(), &unavailable_url, "");
    let transient = invoke(
        unavailable.path(),
        &["--machine", "ndjson-v1", "--batch-id", "transient"],
    );
    let transient_events = events(&transient);
    assert_stream(&transient_events, "transient");
    assert_eq!(
        transient_events.last().expect("transient terminal")["failure"]["retry"],
        "retry_with_backoff"
    );

    let policy = fixture();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind server");
    let url = format!("http://{}", listener.local_addr().expect("server address"));
    let server = std::thread::spawn(move || {
        use std::io::{Read as _, Write as _};

        let (mut stream, _) = listener.accept().expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).expect("read request");
        let body = "[]";
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nLink: </next; rel=next\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
        .expect("write response");
        stream.flush().expect("flush response");
    });
    write_rest_pipeline(
        policy.path(),
        &url,
        "        pagination:\n          strategy: link_header",
    );
    let policy_output = invoke(
        policy.path(),
        &["--machine", "ndjson-v1", "--batch-id", "policy"],
    );
    server.join().expect("server thread");
    let policy_events = events(&policy_output);
    assert_stream(&policy_events, "policy");
    let terminal = policy_events.last().expect("policy terminal");
    assert_eq!(terminal["failure"]["retry"], "policy_required");
    assert_eq!(
        terminal["failure"]["code"],
        "rest.protocol.malformed_continuation"
    );
}

#[test]
fn protocol_page_body_limit_requires_policy_before_retry() {
    let directory = fixture();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind server");
    let url = format!("http://{}", listener.local_addr().expect("server address"));
    let server = std::thread::spawn(move || {
        use std::io::{Read as _, Write as _};

        let (mut stream, _) = listener.accept().expect("accept request");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request).expect("read request");
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {OVERSIZED_REST_PAGE_BYTES}\r\nConnection: close\r\n\r\n"
        )
        .expect("write response headers");

        let chunk = [b' '; 64 * 1024];
        let mut remaining = OVERSIZED_REST_PAGE_BYTES;
        while remaining > 0 {
            let count = remaining.min(chunk.len());
            if stream.write_all(&chunk[..count]).is_err() {
                break;
            }
            remaining -= count;
        }
        let _ = stream.flush();
    });
    write_rest_pipeline(directory.path(), &url, "");

    let output = invoke(
        directory.path(),
        &["--machine", "ndjson-v1", "--batch-id", "page-body-limit"],
    );
    server.join().expect("server thread");
    assert_eq!(output.status.code(), Some(4));
    let stream = events(&output);
    assert_stream(&stream, "page-body-limit");
    let terminal = stream.last().expect("failed terminal");
    assert_eq!(terminal["event"], "failed");
    assert_eq!(
        terminal["failure"]["code"],
        "rest.protocol.page_body_limit_reached"
    );
    assert_eq!(terminal["failure"]["category"], "source_protocol");
    assert_eq!(terminal["failure"]["retry"], "policy_required");
}

#[test]
fn machine_protocol_zero_record_run_has_full_lifecycle_and_artifact_truth() {
    let directory = fixture();
    write_pipeline(directory.path(), "zero.csv");
    std::fs::write(directory.path().join("input.csv"), "id,name\n").expect("empty input");
    let output = invoke(
        directory.path(),
        &["--machine", "ndjson-v1", "--batch-id", "zero"],
    );
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stream = events(&output);
    assert_stream(&stream, "zero");
    for phase in ["planning", "executing", "finalizing", "publishing"] {
        assert!(
            stream.iter().any(|event| {
                event["event"] == "progress"
                    && event["progress"]["kind"] == "transition"
                    && event["progress"]["phase"] == phase
            }),
            "missing phase {phase}: {stream:?}"
        );
    }
    let completed = stream.last().expect("completed terminal");
    assert_eq!(completed["event"], "completed");
    let artifacts = publication_artifacts(&stream);
    assert_eq!(artifacts.len(), 1);
    assert_eq!(artifacts[0]["kind"], "primary");
    assert_eq!(artifacts[0]["state"], "published");
    assert_eq!(
        artifacts[0].as_object().expect("artifact object").len(),
        3,
        "only artifact_id, kind, and state are public"
    );
    assert_eq!(completed["publication"]["artifact_count"], 1);
    assert_eq!(completed["publication"]["state_counts"]["published"], 1);
    assert!(
        !String::from_utf8_lossy(&output.stdout)
            .contains(directory.path().to_string_lossy().as_ref())
    );
}

#[test]
fn machine_protocol_dlq_is_completed_with_dlq_and_path_free_artifacts() {
    let directory = fixture();
    std::fs::write(directory.path().join("input.csv"), "id,amount\n1,10\n2,0\n")
        .expect("DLQ input");
    std::fs::write(
        directory.path().join("pipeline.yaml"),
        r#"pipeline: { name: machine_dlq }
error_handling:
  strategy: continue
  dlq: { path: rejected.ndjson }
nodes:
  - type: source
    name: src
    config:
      name: src
      path: input.csv
      type: csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: transform
    name: map
    input: src
    config:
      cxl: "emit amount = if(amount == 0) then (1 / 0) else amount"
  - type: output
    name: out
    input: map
    config: { name: out, path: out.csv, type: csv }
"#,
    )
    .expect("DLQ pipeline");
    let output = invoke(
        directory.path(),
        &["--machine", "ndjson-v1", "--batch-id", "dlq"],
    );
    assert_eq!(output.status.code(), Some(2));
    let stream = events(&output);
    assert_stream(&stream, "dlq");
    let completed = stream.last().expect("completed terminal");
    assert_eq!(completed["event"], "completed");
    assert_eq!(completed["result"], "completed_with_dlq");
    let artifacts = publication_artifacts(&stream);
    assert!(artifacts.iter().any(|artifact| artifact["kind"] == "dlq"));
    assert!(
        artifacts
            .iter()
            .all(|artifact| artifact["state"] == "published")
    );
}

#[test]
fn machine_protocol_write_failure_before_publication_leaves_final_unchanged() {
    let directory = fixture();
    write_pipeline(directory.path(), "must-not-publish.csv");
    let output = Command::new(clinker_bin())
        .current_dir(directory.path())
        .env("CLINKER_TEST_MACHINE_WRITE_FAILURE", "finalizing")
        .args([
            "run",
            "pipeline.yaml",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "broken-control",
        ])
        .output()
        .expect("run broken control channel");
    assert_eq!(output.status.code(), Some(130));
    let stream = events(&output);
    assert_eq!(stream.last().expect("terminal")["event"], "cancelled");
    assert_eq!(stream.iter().filter(|event| terminal(event)).count(), 1);
    assert!(!directory.path().join("must-not-publish.csv").exists());
}

#[test]
fn machine_protocol_terminal_write_failure_does_not_undo_published_artifacts() {
    let directory = fixture();
    write_pipeline(directory.path(), "published.csv");
    let output = Command::new(clinker_bin())
        .current_dir(directory.path())
        .env("CLINKER_TEST_MACHINE_WRITE_FAILURE", "terminal")
        .args([
            "run",
            "pipeline.yaml",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "terminal-failure",
        ])
        .output()
        .expect("run terminal write failure");
    assert_eq!(output.status.code(), Some(4));
    let stream = events(&output);
    assert!(stream.iter().all(|event| !terminal(event)));
    assert!(directory.path().join("published.csv").exists());
}
