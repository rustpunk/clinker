//! Direct child-process contract for the opt-in machine-run protocol.

mod support;

use std::io::{Read as _, Write as _};
use std::net::TcpListener;
use std::process::Command;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use serde_json::Value;
use support::process::{ControlledOutcome, ProcessConfig, ProtocolDrain, StdoutMode, run_child};

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
    let (source_selector, ordering) = if log_rows {
        let inputs = directory.join("inputs");
        std::fs::create_dir(&inputs).expect("input directory");
        for file in 0..512 {
            std::fs::write(
                inputs.join(format!("part-{file:04}.csv")),
                "id,name\n2,B\n1,A\n",
            )
            .expect("unsorted input fixture");
        }
        (
            "      glob: inputs/*.csv\n".to_owned(),
            "      sort_order: [id]\n      on_unsorted: warn\n",
        )
    } else {
        let mut input = String::from("id,name\n");
        for row in 0..rows {
            input.push_str(&format!("{row},record-{row}\n"));
        }
        std::fs::write(directory.join("input.csv"), input).expect("input fixture");
        ("      path: input.csv\n".to_owned(), "")
    };
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
{source_selector}      type: csv
      schema:
        - {{ name: id, type: int }}
        - {{ name: name, type: string }}
{ordering}  - type: sink
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

/// A pipeline whose first source is large enough that discovery takes real
/// time, and whose second source resolves to nothing — so the run leaves by an
/// early return well after the liveness worker's first tick and long before
/// the executor call that would reach the worker's explicit `finish`.
fn write_unresolvable_second_source_pipeline(directory: &std::path::Path) {
    let inputs = directory.join("inputs");
    std::fs::create_dir(&inputs).expect("input directory");
    for file in 0..1024 {
        std::fs::write(inputs.join(format!("part-{file:04}.csv")), "id\n1\n")
            .expect("input fixture");
    }
    std::fs::write(
        directory.join("pipeline.yaml"),
        r#"pipeline:
  name: supervised_discovery_failure
nodes:
  - type: source
    name: src
    config:
      name: src
      glob: inputs/*.csv
      type: csv
      schema:
        - { name: id, type: int }
  - type: source
    name: absent
    config:
      name: absent
      glob: absent/*.csv
      type: csv
      schema:
        - { name: id, type: int }
  - type: sink
    name: out
    input: src
    config: { name: out, path: out.csv, type: csv }
  - type: sink
    name: absent_out
    input: absent
    config: { name: absent_out, path: absent-out.csv, type: csv }
"#,
    )
    .expect("discovery failure pipeline");
}

fn write_dlq_pipeline(directory: &std::path::Path) {
    std::fs::write(directory.join("input.csv"), "id,amount\n1,10\n2,0\n").expect("DLQ input");
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
  - type: sink
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
  - type: sink
    name: out
    input: api
    config: {{ name: out, path: out.csv, type: csv }}
"#
        ),
    )
    .expect("hanging REST pipeline");
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

fn execution_id(events: &[Value]) -> &str {
    events[0]["execution_id"].as_str().expect("execution id")
}

fn fixture_snapshot(root: &std::path::Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    fn visit(
        root: &std::path::Path,
        current: &std::path::Path,
        snapshot: &mut Vec<(std::path::PathBuf, Vec<u8>)>,
    ) {
        let mut entries = std::fs::read_dir(current)
            .expect("read fixture directory")
            .collect::<Result<Vec<_>, _>>()
            .expect("read fixture entries");
        entries.sort_by_key(std::fs::DirEntry::path);
        for entry in entries {
            let path = entry.path();
            if path.is_dir() {
                visit(root, &path, snapshot);
            } else {
                snapshot.push((
                    path.strip_prefix(root)
                        .expect("fixture-relative path")
                        .to_owned(),
                    std::fs::read(&path).expect("read fixture file"),
                ));
            }
        }
    }

    let mut snapshot = Vec::new();
    visit(root, root, &mut snapshot);
    snapshot
}

#[test]
fn signal_handler_installation_failure_is_preeffect() {
    let directory = fixture();
    write_pipeline(directory.path(), "out.csv", 1, false);
    std::fs::write(directory.path().join("out.csv"), "existing output\n").expect("existing output");
    std::fs::write(
        directory.path().join("lineage.ndjson"),
        "existing lineage\n",
    )
    .expect("existing lineage");
    let staging = directory.path().join("staging");
    std::fs::create_dir(&staging).expect("staging directory");
    std::fs::write(staging.join("sentinel"), "existing staging\n")
        .expect("existing staging sentinel");
    let before = fixture_snapshot(directory.path());

    let output = machine_command(directory.path(), "signal-handler-failure")
        .args(["--lineage-events", "lineage.ndjson"])
        .env("CLINKER_TEST_SIGNAL_HANDLER_FAILURE", "1")
        .output()
        .expect("run with injected signal-handler failure");

    assert_eq!(output.status.code(), Some(4));
    assert!(
        output.stdout.is_empty(),
        "the machine protocol must not open before signal admission succeeds"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("failed to install signal handler"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(fixture_snapshot(directory.path()), before);
    assert!(!directory.path().join(".clinker-attempts").exists());
}

#[test]
fn machine_progress_worker_start_failure_is_preeffect_and_classified() {
    let directory = fixture();
    write_pipeline(directory.path(), "out.csv", 1, false);
    write_local_lineage_policy(directory.path());
    std::fs::write(directory.path().join("out.csv"), "existing output\n").expect("existing output");
    std::fs::write(
        directory.path().join("lineage.ndjson"),
        "existing lineage\n",
    )
    .expect("existing lineage");
    let staging = directory.path().join("staging");
    std::fs::create_dir(&staging).expect("staging directory");
    std::fs::write(staging.join("sentinel"), "existing staging\n")
        .expect("existing staging sentinel");
    let before = fixture_snapshot(directory.path());

    let output = machine_command(directory.path(), "machine-worker-start-failure")
        .args(["--lineage-events", "lineage.ndjson"])
        .env("CLINKER_TEST_MACHINE_PROGRESS_WORKER_START_FAILURE", "1")
        .output()
        .expect("run with injected machine progress worker failure");

    assert_eq!(output.status.code(), Some(4));
    let events = output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Value>(line).expect("machine event JSON"))
        .collect::<Vec<_>>();
    let terminals = events
        .iter()
        .filter(|event| {
            matches!(
                event["event"].as_str(),
                Some("completed" | "failed" | "cancelled")
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(terminals.len(), 1, "events: {events:#?}");
    assert_eq!(terminals[0]["event"], "failed");
    assert_eq!(
        terminals[0]["failure"]["code"],
        "infrastructure.runtime.transient"
    );
    assert_eq!(terminals[0]["failure"]["category"], "infrastructure");
    assert_eq!(terminals[0]["failure"]["retry"], "retry_with_backoff");
    assert_eq!(fixture_snapshot(directory.path()), before);
    assert!(!directory.path().join(".clinker-attempts").exists());
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

    assert_eq!(
        result.outcome(),
        ControlledOutcome::Success,
        "events: {:?}\nstderr: {}",
        result.stdout.events(),
        String::from_utf8_lossy(result.stderr.retained_tail())
    );
    assert!(result.reaped());
    assert!(result.stdout.total_bytes() > 0);
    assert!(
        result.stderr.total_bytes() > 64 * 1024,
        "stderr bytes: {}",
        result.stderr.total_bytes()
    );
    assert!(result.stdout.retained_tail().len() <= 4 * 1024);
    assert!(result.stderr.retained_tail().len() <= 8 * 1024);
    assert!(directory.path().join("out.csv").exists());
}

/// The supervised plan-only export reconciles, like every other stream a
/// supervisor is told it may run.
///
/// `--lineage <FILE>` is a plan-only export that returns before any data is
/// read, and it is supervisable so its document carries the invocation's own
/// `execution_id` and `batch_id`. Its terminal is the success row, on which
/// the contract promises complete artifact evidence — so with no `publication`
/// field the adapter here, which is the contract expressed as code, called the
/// attempt incomplete while the CLI called it a success. The two now agree on
/// the same stream: an empty inventory, which is what an invocation that ran
/// no attempt published.
#[test]
fn a_supervised_plan_only_export_reconciles_as_success() {
    let directory = fixture();
    write_pipeline(directory.path(), "must-not-exist.csv", 1, false);
    write_local_lineage_policy(directory.path());
    let mut command = machine_command(directory.path(), "plan-only-export");
    command.args(["--lineage", "plan.ndjson"]);
    let result = run_child(command, ProcessConfig::new(PROCESS_DEADLINE)).expect("supervised run");

    assert_eq!(
        result.outcome(),
        ControlledOutcome::Success,
        "events: {:?}\nstderr: {}",
        result.stdout.events(),
        String::from_utf8_lossy(result.stderr.retained_tail())
    );
    assert_eq!(result.status_code(), Some(0));
    assert!(directory.path().join("plan.ndjson").exists());
    assert!(
        !directory.path().join("must-not-exist.csv").exists(),
        "a plan-only export reads no data and publishes nothing"
    );
    let terminal = result.stdout.events().last().expect("terminal");
    assert_eq!(terminal["publication"]["artifact_count"], 0);
    assert_eq!(terminal["publication"]["complete"], true);
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
    missing
        .events_mut()
        .retain(|event| event["event"] != "completed");
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
    mismatched.events_mut().last_mut().expect("terminal")["exit_code"] = serde_json::json!(2);
    assert_eq!(
        result.outcome_for(&mismatched),
        ControlledOutcome::Incomplete
    );

    let mut missing_inventory = result.stdout.clone();
    missing_inventory
        .events_mut()
        .retain(|event| event["event"] != "publication_artifacts");
    for (sequence, event) in missing_inventory.events_mut().iter_mut().enumerate() {
        event["seq"] = serde_json::json!(sequence);
    }
    assert_eq!(
        result.outcome_for(&missing_inventory),
        ControlledOutcome::Incomplete,
        "a completed terminal requires its complete artifact inventory"
    );

    let mut mismatched_inventory = result.stdout.clone();
    mismatched_inventory
        .events_mut()
        .last_mut()
        .expect("terminal")["publication"]["artifact_count"] = serde_json::json!(2);
    assert_eq!(
        result.outcome_for(&mismatched_inventory),
        ControlledOutcome::Incomplete,
        "terminal counts must reconcile with artifact chunks"
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
    assert_eq!(dlq_result.outcome(), ControlledOutcome::CompletedWithDlq);
    assert!(dlq.path().join("out.csv").exists());
    assert!(dlq.path().join("rejected.ndjson").exists());

    let failed = fixture();
    std::fs::write(failed.path().join("pipeline.yaml"), "pipeline: [\n").expect("invalid pipeline");
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
    let mut missing_retry = failed_result.stdout.clone();
    missing_retry.events_mut().last_mut().expect("terminal")["failure"]["retry"] =
        serde_json::Value::Null;
    assert_eq!(
        failed_result.outcome_for(&missing_retry),
        ControlledOutcome::Incomplete,
        "failed terminals require the complete typed failure vocabulary"
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

    // 130, and the same 130 wherever the write happens to fail. A supervisor
    // that closed the stream loses a different record depending on how far the
    // run got, so deriving the status from which write failed reported one
    // condition as two, and the assertion below is what makes 130 the right
    // one: nothing reached a final path.
    assert_eq!(result.status_code(), Some(130));
    assert_eq!(result.outcome(), ControlledOutcome::Incomplete);
    assert!(result.reaped());
    assert!(!directory.path().join("must-not-publish.csv").exists());
}

#[test]
fn deadline_expiry_forces_termination_and_reaps_the_child() {
    let directory = fixture();
    std::fs::write(
        directory.path().join("out.csv"),
        "previous complete artifact\n",
    )
    .expect("existing final");
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    listener
        .set_nonblocking(true)
        .expect("nonblocking listener");
    let address = listener.local_addr().expect("listener address");
    write_hanging_rest_pipeline(directory.path(), address);
    let server = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(2);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(
                        Instant::now() < deadline,
                        "child never connected to fixture"
                    );
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("accept request: {error}"),
            }
        };
        // A socket accepted from a non-blocking listener inherits O_NONBLOCK on
        // macOS/BSD and Windows, but not on Linux. Left implicit, the first read
        // returns WouldBlock, this loop exits immediately, and dropping the
        // stream closes the connection — so the child's REST read fails fast
        // instead of hanging and the deadline under test never expires.
        stream
            .set_nonblocking(false)
            .expect("blocking accepted stream");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut request = [0_u8; 4096];
        // The fixture must hold the connection open until the child is gone, so
        // an idle read is not a reason to stop. Only end-of-stream (the child
        // was terminated) or a real transport error ends the loop, bounded so a
        // wedged child cannot hang the suite.
        let hold_until = Instant::now() + Duration::from_secs(30);
        loop {
            match stream.read(&mut request) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
                    ) => {}
                Err(_) => break,
            }
            if Instant::now() >= hold_until {
                break;
            }
        }
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
    write_pipeline(directory.path(), "{batch_id}-{execution_id}.csv", 2, false);
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
    let first_output = directory.path().join(format!("same-batch-{first_id}.csv"));
    let second_output = directory.path().join(format!("same-batch-{second_id}.csv"));
    assert!(first_output.exists() && second_output.exists());
    assert_ne!(
        std::fs::read_to_string(first_output).expect("first output"),
        std::fs::read_to_string(second_output).expect("second output"),
        "the retry starts from the beginning of the replacement input"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn real_sigterm_cancels_during_grace() {
    let directory = fixture();
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("listener address");
    write_hanging_rest_pipeline(directory.path(), address);

    let (request_ready_tx, request_ready_rx) = mpsc::sync_channel(1);
    let (sigterm_sent_tx, sigterm_sent_rx) = mpsc::sync_channel(1);
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let bytes = stream.read(&mut buffer).expect("read request");
            assert!(bytes > 0, "request closed before its headers completed");
            request.extend_from_slice(&buffer[..bytes]);
        }
        request_ready_tx.send(()).expect("announce live request");
        sigterm_sent_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("supervisor sent SIGTERM");
        stream
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nConnection: close\r\n\r\n[{\"id\":1}]",
            )
            .expect("write response after SIGTERM");
    });

    let result = run_child(
        machine_command(directory.path(), "real-sigterm"),
        ProcessConfig::new(Duration::from_secs(5)).graceful_trigger(
            request_ready_rx,
            sigterm_sent_tx,
            Duration::from_secs(2),
        ),
    )
    .expect("gracefully supervised run");
    server.join().expect("server thread");

    assert!(result.graceful_requested());
    assert!(!result.forced());
    assert_eq!(result.observed_grace(), None);
    assert!(result.reaped());
    assert_eq!(result.status_code(), Some(130));
    assert_eq!(result.outcome(), ControlledOutcome::Cancelled);
    let terminals = result
        .stdout
        .events()
        .iter()
        .filter(|event| {
            matches!(
                event["event"].as_str(),
                Some("completed" | "failed" | "cancelled")
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(terminals.len(), 1);
    assert_eq!(terminals[0]["schema"], 1);
    assert_eq!(terminals[0]["event"], "cancelled");
}

#[cfg(target_os = "linux")]
#[test]
fn grace_expiry_forces_reaps_and_retries_fresh() {
    let directory = fixture();
    let previous_final = "previous complete artifact\n";
    std::fs::write(directory.path().join("out.csv"), previous_final).expect("existing final");
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("listener address");
    write_hanging_rest_pipeline(directory.path(), address);

    let (request_ready_tx, request_ready_rx) = mpsc::sync_channel(1);
    let (sigterm_sent_tx, sigterm_sent_rx) = mpsc::sync_channel(1);
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let bytes = stream.read(&mut buffer).expect("read request");
            assert!(bytes > 0, "request closed before its headers completed");
            request.extend_from_slice(&buffer[..bytes]);
        }
        request_ready_tx.send(()).expect("announce live request");
        sigterm_sent_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("supervisor sent SIGTERM");

        loop {
            match stream.read(&mut buffer) {
                Ok(0) => break,
                Ok(_) => {}
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::UnexpectedEof
                    ) =>
                {
                    break;
                }
                Err(error) => panic!("wait for forced socket closure: {error}"),
            }
        }
    });

    let grace = Duration::from_millis(500);
    let hard_deadline = Duration::from_secs(5);
    let result = run_child(
        machine_command(directory.path(), "force-retry"),
        ProcessConfig::new(hard_deadline).graceful_trigger(
            request_ready_rx,
            sigterm_sent_tx,
            grace,
        ),
    )
    .expect("forced supervised run");
    server.join().expect("server thread");

    assert!(result.graceful_requested());
    assert_eq!(result.force_count(), 1, "force must be issued exactly once");
    assert!(
        result
            .observed_grace()
            .is_some_and(|observed| observed >= grace),
        "the forced path must honor the configured grace: {:?}",
        result.observed_grace()
    );
    assert!(
        result
            .elapsed_to_force()
            .is_some_and(|elapsed| elapsed < hard_deadline),
        "grace expiry must force before the total hard deadline: {:?}",
        result.elapsed_to_force()
    );
    assert!(result.reaped());
    assert!(result.drains_joined_after_reap());
    assert_eq!(result.outcome(), ControlledOutcome::Incomplete);
    assert_eq!(
        std::fs::read_to_string(directory.path().join("out.csv")).expect("existing final"),
        previous_final,
        "an incomplete attempt cannot replace an older complete final"
    );

    let first_execution_id = execution_id(result.stdout.events()).to_owned();
    write_pipeline(directory.path(), "{batch_id}-{execution_id}.csv", 1, false);
    let retry = run_child(
        machine_command(directory.path(), "force-retry"),
        ProcessConfig::new(PROCESS_DEADLINE),
    )
    .expect("fresh retry");

    assert_eq!(retry.outcome(), ControlledOutcome::Success);
    let retry_execution_id = execution_id(retry.stdout.events());
    assert_ne!(first_execution_id, retry_execution_id);
    assert_eq!(
        result.stdout.events()[0]["batch_id"],
        retry.stdout.events()[0]["batch_id"]
    );
    assert!(
        directory
            .path()
            .join(format!("force-retry-{retry_execution_id}.csv"))
            .exists(),
        "retry must publish from a new independent execution"
    );
    assert_eq!(
        std::fs::read_to_string(directory.path().join("out.csv")).expect("existing final"),
        previous_final,
        "the retry cannot relabel or overwrite the older final"
    );
}

/// A refused liveness record must not renumber the rest of the run.
///
/// The worker deliberately swallows a refused periodic observation so a
/// healthy run still produces its output and exits 0. That is only true from
/// the supervisor's side if the record it lost took no sequence number with
/// it: this adapter reconciles any gap as an incomplete attempt, so one
/// momentarily full pipe would otherwise condemn a run that executed
/// correctly and reported every required record.
#[test]
fn a_refused_liveness_record_leaves_the_stream_densely_numbered() {
    let directory = fixture();
    write_pipeline(directory.path(), "out.csv", 4_096, true);

    let mut command = machine_command(directory.path(), "dense-after-refusal");
    command
        .env("CLINKER_TEST_MACHINE_WRITE_FAILURE", "periodic_sink")
        .env("CLINKER_TEST_MACHINE_PROGRESS_TICK_MS", "1")
        // Longer than this fixture takes, so the run ends before the failing
        // sink window does and the refusals stay advisory.
        .env("CLINKER_TEST_MACHINE_SINK_PATIENCE_MS", "600000");
    let result = run_child(command, ProcessConfig::new(PROCESS_DEADLINE)).expect("supervised run");

    assert_eq!(
        result.outcome(),
        ControlledOutcome::Success,
        "a lost advisory record is not a broken stream\nevents: {:?}\nstderr: {}",
        result.stdout.events(),
        String::from_utf8_lossy(result.stderr.retained_tail())
    );
    assert_eq!(result.status_code(), Some(0));
    assert!(
        !result
            .stdout
            .events()
            .iter()
            .any(|event| event["progress"]["kind"] == "periodic"),
        "while every periodic record was in fact refused: {:?}",
        result.stdout.events()
    );
    for (sequence, event) in result.stdout.events().iter().enumerate() {
        assert_eq!(
            event["seq"],
            serde_json::json!(sequence),
            "the delivered stream is numbered from zero without gaps: {:?}",
            result.stdout.events()
        );
    }
}

/// A terminal the sink refuses after taking the inventory must not make the
/// retry repeat a chunk the reader already has.
///
/// The chunk index is the record's position in the inventory, and an adapter
/// reassembling indices zero through `chunk_count - 1` in sequence rejects a
/// repeated one. Re-sending the whole inventory therefore turned a recovered
/// terminal whose artifact evidence was correct into an incomplete attempt —
/// over a fault that was purely on the reporting channel.
#[test]
fn a_terminal_retried_after_a_refusal_reconciles_with_its_inventory() {
    let directory = fixture();
    write_pipeline(directory.path(), "published.csv", 1, false);

    let mut command = machine_command(directory.path(), "retried-terminal");
    command.env("CLINKER_TEST_MACHINE_WRITE_FAILURE", "terminal_sink");
    let result = run_child(command, ProcessConfig::new(PROCESS_DEADLINE)).expect("supervised run");

    assert!(
        directory.path().join("published.csv").exists(),
        "the run published before its terminal was refused"
    );
    assert_eq!(
        result.outcome(),
        ControlledOutcome::Failed,
        "the recovered terminal reconciles against its own inventory\nevents: {:?}\nstderr: {}",
        result.stdout.events(),
        String::from_utf8_lossy(result.stderr.retained_tail())
    );
    assert_eq!(result.status_code(), Some(4));
    let chunks = result
        .stdout
        .events()
        .iter()
        .filter(|event| event["event"] == "publication_artifacts")
        .collect::<Vec<_>>();
    assert_eq!(
        chunks
            .iter()
            .map(|event| event["publication"]["chunk_index"].clone())
            .collect::<Vec<_>>(),
        [serde_json::json!(0)],
        "the chunk the sink took is not sent again: {:?}",
        result.stdout.events()
    );
    let terminal = result.stdout.events().last().expect("terminal");
    assert_eq!(terminal["event"], "failed");
    assert_eq!(terminal["publication"]["artifact_count"], 1);
}

/// A sink that refuses a liveness record is not the same as a record that
/// cannot be built. The first may be a reader that is briefly behind, so the
/// worker keeps trying and the run finishes normally; the second can never
/// succeed. Judging both by the error kind got each of them wrong in turn,
/// and the injected fault only ever fired in the encode stage, so the
/// keep-trying branch had no test that could reach it.
#[test]
fn a_refused_liveness_record_does_not_end_a_healthy_run() {
    let directory = fixture();
    write_pipeline(directory.path(), "out.csv", 4_096, true);

    let output = machine_command(directory.path(), "refused-periodic")
        .env("CLINKER_TEST_MACHINE_WRITE_FAILURE", "periodic_sink")
        .env("CLINKER_TEST_MACHINE_PROGRESS_TICK_MS", "1")
        // Long enough that no run of this fixture reaches it. Without a stated
        // window the assertion below depended on the default five seconds
        // being longer than a loaded host takes, which is a race rather than
        // a rule.
        .env("CLINKER_TEST_MACHINE_SINK_PATIENCE_MS", "600000")
        .output()
        .expect("run with injected periodic sink failure");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a refused observation is not a reason to fail the run\nstderr:\n{stderr}"
    );
    assert!(
        stdout
            .lines()
            .any(|line| line.contains("\"event\":\"completed\"")),
        "and the run still reaches its terminal\nstdout:\n{stdout}"
    );
    assert!(
        !stdout.contains("\"kind\":\"periodic\""),
        "while every periodic record was in fact refused\nstdout:\n{stdout}"
    );
    // The distinguishing assertion. A record that cannot be built ends the
    // worker and is reported here; a record the sink refused is retried and
    // is not. Without this the test passed through the fatal path while
    // claiming to cover the retry -- which is exactly what it did, because
    // the encode-stage injector matched this point too.
    assert!(
        !stderr.contains("machine progress channel failed"),
        "and the worker kept going rather than ending on it\nstderr:\n{stderr}"
    );
}

/// A sink that refuses every record for the whole window is not a reader that
/// is behind, and saying so is the point of the thread. Reporting nothing left
/// an orchestrator watching a stream that had stopped while the run exited
/// normally -- which is what happened when an idle tick, of which there are
/// many per due record, counted as evidence the sink was alive.
#[test]
fn a_sink_that_never_recovers_is_reported() {
    let directory = fixture();
    write_pipeline(directory.path(), "out.csv", 4_096, true);

    let output = machine_command(directory.path(), "dead-sink")
        .env("CLINKER_TEST_MACHINE_WRITE_FAILURE", "periodic_sink")
        .env("CLINKER_TEST_MACHINE_PROGRESS_TICK_MS", "1")
        .env("CLINKER_TEST_MACHINE_SINK_PATIENCE_MS", "50")
        .output()
        .expect("run with a permanently refusing periodic sink");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("machine progress channel failed"),
        "a sink refusing for the whole window is reported\nstderr:\n{stderr}"
    );
    assert_eq!(
        output.status.code(),
        Some(0),
        "and it is still only an observation: it does not fail the run\nstderr:\n{stderr}"
    );
}

/// A periodic snapshot is a discardable observation. Losing one must not
/// relabel a run that executed to completion as interrupted, abandon its
/// attempt, and report the conventional SIGINT status for a run no operator
/// cancelled.
#[test]
fn a_lost_periodic_observation_does_not_cancel_a_completed_run() {
    let directory = fixture();
    write_pipeline(directory.path(), "out.csv", 4_096, true);

    // Hold the run until the worker has attempted its first record, so the
    // injected failure has fired before anything else can end the run. Without
    // the barrier this test asserts nothing on a host that reads the fixture
    // faster than the first observation is due -- it passed only because this
    // fixture is larger than the one whose sibling test failed that way on
    // macOS and Windows. The faster tick then only shortens a wait that is
    // already guaranteed to end.
    let output = machine_command(directory.path(), "lost-periodic")
        .env("CLINKER_TEST_MACHINE_WRITE_FAILURE", "periodic")
        .env("CLINKER_TEST_MACHINE_PROGRESS_TICK_MS", "1")
        .env("CLINKER_TEST_MACHINE_PROGRESS_AWAIT_FIRST_OBSERVATION", "1")
        .output()
        .expect("run with injected periodic write failure");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("machine progress channel failed"),
        "the injected periodic failure must actually have fired; \
         status={:?}\nstderr:\n{stderr}\nstdout:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout)
    );
    assert_eq!(output.status.code(), Some(0));
    assert!(directory.path().join("out.csv").exists());

    let events = output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice::<Value>(line).expect("machine event JSON"))
        .collect::<Vec<_>>();
    let terminal = events.last().expect("terminal");
    assert_eq!(terminal["event"], "completed");
    assert_eq!(terminal["result"], "success");
    assert!(
        !events
            .iter()
            .any(|event| event["progress"]["kind"] == "periodic"),
        "the advisory records were the ones dropped, not the required ones"
    );
}

/// A shutdown signal that trips while a REST request is already in flight
/// unwinds the run as an error rather than through the drained-report flag.
/// Both paths are the same operator action, so both must record the same
/// lineage terminal; a `FAIL` here would page an on-call for a cancellation.
#[cfg(target_os = "linux")]
#[test]
fn sigterm_inside_a_rest_read_records_an_abort_lineage_terminal() {
    let directory = fixture();
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener");
    let address = listener.local_addr().expect("listener address");
    write_hanging_rest_pipeline(directory.path(), address);
    write_local_lineage_policy(directory.path());

    let (request_ready_tx, request_ready_rx) = mpsc::sync_channel(1);
    let (sigterm_sent_tx, sigterm_sent_rx) = mpsc::sync_channel(1);
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("accept request");
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("read timeout");
        let mut request = Vec::new();
        let mut buffer = [0_u8; 1024];
        while !request.windows(4).any(|window| window == b"\r\n\r\n") {
            let bytes = stream.read(&mut buffer).expect("read request");
            assert!(bytes > 0, "request closed before its headers completed");
            request.extend_from_slice(&buffer[..bytes]);
        }
        request_ready_tx.send(()).expect("announce live request");
        sigterm_sent_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("supervisor sent SIGTERM");
        // The signal has been sent, not yet handled. What this test is about
        // is the order of the two: the reader must already know the run is
        // cancelled when the connection goes away, or it reads the drop as an
        // ordinary transport failure and reports infrastructure instead of an
        // abort. Delivery and handling are separated by the child's scheduler,
        // so the drop waits well past any plausible gap rather than racing it.
        std::thread::sleep(Duration::from_millis(250));
        // Then drop without a response. The child is blocked inside the
        // request, so the teardown lands after cancellation and the reader
        // reports the interruption rather than a clean page boundary.
        drop(stream);
    });

    let mut command = machine_command(directory.path(), "rest-abort");
    command.args(["--lineage-events", "lineage.ndjson"]);
    let result = run_child(
        command,
        ProcessConfig::new(Duration::from_secs(5)).graceful_trigger(
            request_ready_rx,
            sigterm_sent_tx,
            Duration::from_secs(2),
        ),
    )
    .expect("gracefully supervised run");
    server.join().expect("server thread");

    assert_eq!(result.status_code(), Some(130));
    assert_eq!(result.outcome(), ControlledOutcome::Cancelled);

    let lineage = std::fs::read_to_string(directory.path().join("lineage.ndjson"))
        .expect("read lineage events");
    let events = lineage
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str::<Value>(line).expect("lineage event JSON"))
        .collect::<Vec<_>>();
    let terminal = events.last().expect("lineage terminal event");
    assert_eq!(
        terminal["eventType"], "ABORT",
        "a cancelled REST read is an abort, not an engine invariant failure: {events:#?}"
    );
}

/// The liveness worker's verdict survives a run that never reaches `finish`.
///
/// The worker returns `Err` only for a record it can never encode, or for a
/// reader that is gone or has refused for the whole patience window — and
/// `emit_periodic` deliberately does not trip the shutdown token, so nothing
/// else on the run says so. The explicit `finish` after the executor reports
/// that verdict, but every early return before the executor drops the worker
/// instead, and a discarded verdict is the condition the thread exists to
/// report going out with no line at all.
#[test]
fn a_liveness_verdict_is_reported_even_when_the_run_never_reaches_finish() {
    let directory = fixture();
    write_unresolvable_second_source_pipeline(directory.path());

    // Hold the run at the worker's start until the worker has made its first
    // — and under this injection, fatal — observation. What is under test is
    // what becomes of that verdict when the run leaves early, so the verdict
    // has to exist before the run leaves. Racing a 1 ms tick against a
    // 1024-file discovery walk established that on one platform and not on
    // others; the barrier establishes it on all of them, and the tick override
    // now only shortens a wait that is guaranteed to end.
    let output = machine_command(directory.path(), "dropped-liveness-verdict")
        .env("CLINKER_TEST_MACHINE_WRITE_FAILURE", "periodic")
        .env("CLINKER_TEST_MACHINE_PROGRESS_AWAIT_FIRST_OBSERVATION", "1")
        .env("CLINKER_TEST_MACHINE_PROGRESS_TICK_MS", "1")
        .output()
        .expect("run that fails before the executor");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_ne!(
        output.status.code(),
        Some(0),
        "the run stops at the unresolvable source\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("machine progress worker failed"),
        "a worker that failed on a run which never reached its explicit finish \
         still says so\nstatus={:?}\nstderr:\n{stderr}",
        output.status.code()
    );
}

// Keep the imported type part of the test contract: malformed and unsupported
// streams are represented by the bounded protocol drain, not raw EOF success.
const _: Option<ProtocolDrain> = None;
