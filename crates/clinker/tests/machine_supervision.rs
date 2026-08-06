//! Direct child-process contract for the opt-in machine-run protocol.

mod support;

use std::io::Read as _;
use std::net::TcpListener;
use std::process::Command;
use std::time::Duration;

use serde_json::Value;
use support::process::{
    ControlledOutcome, ProcessConfig, ProtocolDrain, StdoutMode, run_child,
};

const PROCESS_DEADLINE: Duration = Duration::from_secs(10);

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

fn fixture() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::current_dir().expect("current directory"))
        .expect("fixture directory")
}

fn machine_command(directory: &std::path::Path, batch_id: &str) -> Command {
    let mut command = Command::new(clinker_bin());
    command.current_dir(directory).args([
        "run",
        "pipeline.yaml",
        "--machine",
        "ndjson-v1",
        "--batch-id",
        batch_id,
    ]);
    command
}

fn write_pipeline(directory: &std::path::Path, output: &str, rows: usize, log_rows: bool) {
    let mut input = String::from("id,name\n");
    for row in 0..rows {
        input.push_str(&format!("{row},record-{row}\n"));
    }
    std::fs::write(directory.join("input.csv"), input).expect("input fixture");
    let transform = if log_rows {
        r#"  - type: transform
    name: logged
    input: src
    config:
      cxl: "emit id = id; emit name = name"
      log:
        - level: info
          when: per_record
          message: "supervision bounded-pipe diagnostic payload"
"#
    } else {
        ""
    };
    let output_input = if log_rows { "logged" } else { "src" };
    std::fs::write(
        directory.join("pipeline.yaml"),
        format!(
            r#"pipeline:
  name: machine_supervision
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
{transform}  - type: output
    name: out
    input: {output_input}
    config:
      name: out
      path: "{output}"
      type: csv
"#
        ),
    )
    .expect("pipeline fixture");
}

fn write_dlq_pipeline(directory: &std::path::Path) {
    std::fs::write(directory.join("input.csv"), "id,amount\n1,10\n2,0\n")
        .expect("DLQ input");
    std::fs::write(
        directory.join("pipeline.yaml"),
        r#"pipeline: { name: supervised_dlq }
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
}

fn write_hanging_rest_pipeline(directory: &std::path::Path, address: std::net::SocketAddr) {
    std::fs::write(
        directory.join("pipeline.yaml"),
        format!(
            r#"pipeline: {{ name: supervised_deadline }}
nodes:
  - type: source
    name: api
    config:
      name: api
      type: json
      options: {{ format: array }}
      transport:
        kind: rest
        url: "http://{address}"
        max_pages: 1
        retries: 0
        timeout_secs: 60
      schema:
        - {{ name: id, type: int }}
  - type: output
    name: out
    input: api
    config: {{ name: out, path: out.csv, type: csv }}
"#
        ),
    )
    .expect("hanging REST pipeline");
}

fn execution_id(events: &[Value]) -> &str {
    events[0]["execution_id"].as_str().expect("execution id")
}

#[test]
fn concurrent_bounded_drains_prevent_high_output_deadlock() {
    let directory = fixture();
    write_pipeline(directory.path(), "out.csv", 4_096, true);
    let result = run_child(
        machine_command(directory.path(), "bounded-drains"),
        ProcessConfig::new(PROCESS_DEADLINE)
            .stdout_tail_bytes(4 * 1024)
            .stderr_tail_bytes(8 * 1024),
    )
    .expect("supervised run");

    assert_eq!(result.outcome(), ControlledOutcome::Success);
    assert!(result.reaped());
    assert!(result.stdout.total_bytes() > 0);
    assert!(result.stderr.total_bytes() > 64 * 1024);
    assert!(result.stdout.retained_tail().len() <= 4 * 1024);
    assert!(result.stderr.retained_tail().len() <= 8 * 1024);
    assert!(directory.path().join("out.csv").exists());
}

#[test]
fn terminal_and_process_status_must_reconcile_fail_closed() {
    let directory = fixture();
    write_pipeline(directory.path(), "out.csv", 1, false);
    let result = run_child(
        machine_command(directory.path(), "reconcile"),
        ProcessConfig::new(PROCESS_DEADLINE),
    )
    .expect("supervised run");
    assert_eq!(result.outcome(), ControlledOutcome::Success);

    let mut missing = result.stdout.clone();
    missing.events_mut().retain(|event| event["event"] != "completed");
    assert_eq!(
        result.outcome_for(&missing),
        ControlledOutcome::Incomplete,
        "EOF without a terminal is not success"
    );

    let mut malformed = result.stdout.clone();
    malformed.set_parse_error("malformed terminal JSON");
    assert_eq!(
        result.outcome_for(&malformed),
        ControlledOutcome::Incomplete
    );

    let mut unsupported = result.stdout.clone();
    unsupported.events_mut()[0]["schema"] = serde_json::json!(2);
    assert_eq!(
        result.outcome_for(&unsupported),
        ControlledOutcome::Incomplete
    );

    let mut duplicate = result.stdout.clone();
    duplicate
        .events_mut()
        .push(result.stdout.events().last().expect("terminal").clone());
    assert_eq!(
        result.outcome_for(&duplicate),
        ControlledOutcome::Incomplete
    );

    let mut mismatched = result.stdout.clone();
    mismatched.events_mut().last_mut().expect("terminal")["exit_code"] =
        serde_json::json!(2);
    assert_eq!(
        result.outcome_for(&mismatched),
        ControlledOutcome::Incomplete
    );
}

#[test]
fn controlled_terminal_families_match_exit_and_artifact_truth() {
    let zero = fixture();
    write_pipeline(zero.path(), "zero.csv", 0, false);
    let zero_result = run_child(
        machine_command(zero.path(), "zero"),
        ProcessConfig::new(PROCESS_DEADLINE),
    )
    .expect("zero-record run");
    assert_eq!(zero_result.outcome(), ControlledOutcome::Success);
    assert!(zero.path().join("zero.csv").exists());

    let dlq = fixture();
    write_dlq_pipeline(dlq.path());
    let dlq_result = run_child(
        machine_command(dlq.path(), "dlq"),
        ProcessConfig::new(PROCESS_DEADLINE),
    )
    .expect("DLQ run");
    assert_eq!(
        dlq_result.outcome(),
        ControlledOutcome::CompletedWithDlq
    );
    assert!(dlq.path().join("out.csv").exists());
    assert!(dlq.path().join("rejected.ndjson").exists());

    let failed = fixture();
    std::fs::write(failed.path().join("pipeline.yaml"), "pipeline: [\n")
        .expect("invalid pipeline");
    let failed_result = run_child(
        machine_command(failed.path(), "typed-failure"),
        ProcessConfig::new(PROCESS_DEADLINE),
    )
    .expect("typed failure run");
    assert_eq!(failed_result.outcome(), ControlledOutcome::Failed);
    let terminal = failed_result.stdout.events().last().expect("terminal");
    assert_eq!(
        terminal["failure"]["code"],
        "admission.configuration.invalid"
    );
}

#[test]
fn closed_protocol_stdout_cancels_before_publication() {
    let directory = fixture();
    write_pipeline(directory.path(), "must-not-publish.csv", 10_000, false);
    let result = run_child(
        machine_command(directory.path(), "closed-stdout"),
        ProcessConfig::new(PROCESS_DEADLINE).stdout_mode(StdoutMode::CloseAfterLines(1)),
    )
    .expect("closed-control run");

    assert_eq!(result.status_code(), Some(130));
    assert_eq!(result.outcome(), ControlledOutcome::Incomplete);
    assert!(result.reaped());
    assert!(!directory.path().join("must-not-publish.csv").exists());
}

#[test]
fn deadline_expiry_forces_termination_and_reaps_the_child() {
    let directory = fixture();
    std::fs::write(directory.path().join("out.csv"), "previous complete artifact\n")
        .expect("existing final");
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("listener address");
    write_hanging_rest_pipeline(directory.path(), address);
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut request = [0_u8; 4096];
        let _ = stream.read(&mut request);
    });

    let result = run_child(
        machine_command(directory.path(), "deadline"),
        ProcessConfig::new(Duration::from_millis(750)),
    )
    .expect("deadline run");
    server.join().expect("server thread");

    assert!(result.timed_out());
    assert!(result.reaped());
    assert_eq!(result.outcome(), ControlledOutcome::Incomplete);
    assert_eq!(
        std::fs::read_to_string(directory.path().join("out.csv")).expect("existing final"),
        "previous complete artifact\n",
        "an incomplete current attempt cannot relabel an older complete artifact"
    );
}

#[test]
fn retry_launches_a_fresh_process_from_fresh_input() {
    let directory = fixture();
    write_pipeline(
        directory.path(),
        "{batch_id}-{execution_id}.csv",
        2,
        false,
    );
    let first = run_child(
        machine_command(directory.path(), "same-batch"),
        ProcessConfig::new(PROCESS_DEADLINE),
    )
    .expect("first attempt");
    std::fs::write(directory.path().join("input.csv"), "id,name\n3,Carol\n")
        .expect("replacement input");
    let second = run_child(
        machine_command(directory.path(), "same-batch"),
        ProcessConfig::new(PROCESS_DEADLINE),
    )
    .expect("second attempt");

    assert_eq!(first.outcome(), ControlledOutcome::Success);
    assert_eq!(second.outcome(), ControlledOutcome::Success);
    let first_id = execution_id(first.stdout.events());
    let second_id = execution_id(second.stdout.events());
    assert_ne!(first_id, second_id);
    assert_eq!(
        first.stdout.events()[0]["batch_id"],
        second.stdout.events()[0]["batch_id"]
    );
    let first_output = directory
        .path()
        .join(format!("same-batch-{first_id}.csv"));
    let second_output = directory
        .path()
        .join(format!("same-batch-{second_id}.csv"));
    assert!(first_output.exists() && second_output.exists());
    assert_ne!(
        std::fs::read_to_string(first_output).expect("first output"),
        std::fs::read_to_string(second_output).expect("second output"),
        "the retry starts from the beginning of the replacement input"
    );
}

// Keep the imported type part of the test contract: malformed and unsupported
// streams are represented by the bounded protocol drain, not raw EOF success.
const _: Option<ProtocolDrain> = None;
