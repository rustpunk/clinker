//! End-to-end contracts for the CLI-owned optional-observability bulkhead.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::{Command, Output};

use serde_json::Value;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

fn fixture() -> tempfile::TempDir {
    tempfile::tempdir_in(std::env::current_dir().expect("current directory"))
        .expect("fixture directory")
}

fn write_pipeline(root: &Path, output: &str) {
    std::fs::create_dir_all(root.join("private/source")).expect("source directory");
    std::fs::create_dir_all(root.join("private/output")).expect("output directory");
    std::fs::write(
        root.join("private/source/customers.csv"),
        "customer_id\ncustomer-7\n",
    )
    .expect("input fixture");
    std::fs::write(
        root.join("pipeline.yaml"),
        format!(
            r#"pipeline:
  name: telemetry_bulkhead
nodes:
  - type: source
    name: customers
    config:
      name: customers
      type: csv
      path: ./private/source/customers.csv
      options: {{ has_header: true }}
      schema: [{{ name: customer_id, type: string }}]
  - type: transform
    name: normalize
    input: customers
    config:
      cxl: emit customer_id = customer_id
      log:
        - name: transform.customer_seen
          level: info
          when: per_record
          message: customer processed
          fields: [customer_id]
          every: 1
  - type: output
    name: published_customers
    input: normalize
    config:
      name: published_customers
      type: csv
      path: {output}
"#
        ),
    )
    .expect("pipeline fixture");
}

fn write_observability_policy(root: &Path, endpoint: &str, auth: &str) {
    std::fs::write(
        root.join("clinker.toml"),
        format!(
            r#"[observability]
arena_bytes = "64KB"
ordinary_lane_bytes = "32KB"
high_severity_lane_bytes = "32KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 4
max_attribute_bytes = "256B"
sample_every = 1
rate_limit_per_second = 1000
rate_limit_burst = 1000
flush_timeout_ms = 500

[observability.otlp]
endpoint = {endpoint:?}
connect_timeout_ms = 20
request_timeout_ms = 50
retry_max_attempts = 1
retry_total_timeout_ms = 100
max_response_bytes = "4KB"

[observability.otlp.auth]
{auth}

[observability.lineage]
queue_bytes = "64KB"
max_event_bytes = "16KB"
flush_timeout_ms = 200
identity_mode = "local_diagnostic_paths"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "customer_id"
action = "allow"
"#
        ),
    )
    .expect("observability policy");
}

fn invoke(root: &Path, capture: &Path, lineage: bool) -> Output {
    let mut command = Command::new(clinker_bin());
    command
        .current_dir(root)
        .env("CLINKER_TEST_OTLP_OUTCOME", "success")
        .env("CLINKER_TEST_OTLP_CAPTURE", capture)
        .args([
            "run",
            "pipeline.yaml",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "telemetry-bulkhead",
        ]);
    if lineage {
        command.args(["--lineage-events", "lineage.ndjson"]);
    }
    command.output().expect("run clinker")
}

fn machine_events(output: &Output) -> Vec<Value> {
    output
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_slice(line).expect("machine event JSON"))
        .collect()
}

fn capture_events(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .expect("OTLP capture")
        .lines()
        .map(|line| serde_json::from_str(line).expect("OTLP capture JSON"))
        .collect()
}

fn attributes(span: &Value) -> std::collections::BTreeMap<&str, &Value> {
    span["attributes"]
        .as_array()
        .expect("span attributes")
        .iter()
        .map(|attribute| {
            (
                attribute["key"].as_str().expect("attribute key"),
                &attribute["value"],
            )
        })
        .collect()
}

#[test]
fn otlp_bulkhead_drains_three_signals_and_shares_lifecycle_facts() {
    let root = fixture();
    write_pipeline(root.path(), "./private/output/customers.csv");
    write_observability_policy(
        root.path(),
        "https://collector.example.com",
        "mode = \"none\"",
    );
    let capture = root.path().join("otlp.ndjson");

    let output = invoke(root.path(), &capture, true);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(root.path().join("private/output/customers.csv").exists());

    let machine = machine_events(&output);
    let started = machine.first().expect("machine start");
    let plan = machine
        .iter()
        .find(|event| event["event"] == "plan_resolved")
        .expect("resolved plan");
    let terminal = machine.last().expect("machine terminal");
    assert_eq!(terminal["event"], "completed");
    assert_eq!(terminal["result"], "success");
    let summary = terminal["observability"]
        .as_object()
        .expect("fixed observability summary");
    assert_eq!(
        summary.keys().map(String::as_str).collect::<BTreeSet<_>>(),
        BTreeSet::from(["logs", "metrics", "traces"])
    );
    for signal in ["logs", "metrics", "traces"] {
        let counters = summary[signal].as_object().expect("signal counters");
        assert_eq!(
            counters.keys().map(String::as_str).collect::<BTreeSet<_>>(),
            BTreeSet::from(["accepted", "attempts", "failures", "rejected"])
        );
        assert!(counters["accepted"].as_u64().is_some_and(|count| count > 0));
        assert_eq!(counters["failures"], 0);
    }

    let captured = capture_events(&capture);
    let signals = captured
        .iter()
        .map(|entry| entry["signal"].as_str().expect("signal"))
        .collect::<BTreeSet<_>>();
    assert_eq!(signals, BTreeSet::from(["logs", "metrics", "traces"]));
    assert!(
        captured
            .iter()
            .all(|entry| entry["authentication"] == "none")
    );

    let run_span = captured
        .iter()
        .filter(|entry| entry["signal"] == "traces")
        .flat_map(|entry| {
            entry["payload"]["resourceSpans"]
                .as_array()
                .expect("resource spans")
        })
        .flat_map(|resource| resource["scopeSpans"].as_array().expect("scope spans"))
        .flat_map(|scope| scope["spans"].as_array().expect("spans"))
        .find(|span| span["name"] == "clinker.run")
        .expect("lifecycle run span");
    let otlp = attributes(run_span);

    let lineage = std::fs::read_to_string(root.path().join("lineage.ndjson"))
        .expect("lineage output")
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).expect("lineage JSON"))
        .collect::<Vec<_>>();
    let lineage_terminal = lineage.last().expect("lineage terminal");
    assert_eq!(otlp["clinker.batch_id"]["stringValue"], started["batch_id"]);
    assert_eq!(
        otlp["clinker.execution_id"]["stringValue"],
        started["execution_id"]
    );
    assert_eq!(lineage_terminal["run"]["runId"], started["execution_id"]);
    assert_eq!(
        lineage_terminal["run"]["facets"]["clinker_batch"]["batchId"],
        started["batch_id"]
    );
    let lineage_plan = &lineage_terminal["job"]["facets"]["clinker_semanticPlan"];
    assert_eq!(
        otlp["clinker.plan.algorithm"]["stringValue"],
        plan["plan_identity"]["algorithm"]
    );
    assert_eq!(
        otlp["clinker.plan.version"]["intValue"],
        plan["plan_identity"]["version"]
            .as_u64()
            .expect("numeric semantic version")
            .to_string()
    );
    assert_eq!(
        otlp["clinker.plan.digest"]["stringValue"],
        plan["plan_identity"]["digest"]
    );
    assert_eq!(
        lineage_plan["algorithm"],
        plan["plan_identity"]["algorithm"]
    );
    assert_eq!(
        lineage_plan["semanticSchemaVersion"],
        plan["plan_identity"]["version"]
    );
    assert_eq!(lineage_plan["digest"], plan["plan_identity"]["digest"]);
    assert_eq!(otlp["clinker.run.outcome"]["stringValue"], "complete");
    assert_eq!(lineage_terminal["eventType"], "COMPLETE");
}

#[test]
fn otlp_bulkhead_admission_and_reference_fail_before_runtime_effects() {
    for (endpoint, auth, expected) in [
        (
            "http://collector.example.com",
            "mode = \"none\"",
            "https://collector.example.com",
        ),
        (
            "https://user:secret@collector.example.com",
            "mode = \"none\"",
            "https://collector.example.com",
        ),
        (
            "https://collector.example.com",
            "mode = \"reference\"\nreference = \"telemetry/production\"",
            "observability.otlp.auth.reference",
        ),
    ] {
        let root = fixture();
        write_pipeline(root.path(), "./private/output/must-not-exist.csv");
        write_observability_policy(root.path(), endpoint, auth);
        let capture = root.path().join("must-not-capture.ndjson");
        let output = invoke(root.path(), &capture, false);
        assert_eq!(output.status.code(), Some(1));
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains("observability.configuration.invalid"),
            "{stderr}"
        );
        assert!(stderr.contains(expected), "{stderr}");
        assert!(!stderr.contains("user:secret"), "{stderr}");
        assert!(!stderr.contains("telemetry/production"), "{stderr}");
        assert!(
            !root
                .path()
                .join("private/output/must-not-exist.csv")
                .exists()
        );
        assert!(!root.path().join(".clinker-attempts").exists());
        assert!(
            !root
                .path()
                .join("private/output/.clinker-attempts")
                .exists()
        );
        assert!(!capture.exists());
    }
}

#[test]
fn otlp_bulkhead_disabled_default_creates_no_exporter_effect() {
    let root = fixture();
    write_pipeline(root.path(), "./private/output/customers.csv");
    let capture = root.path().join("must-not-capture.ndjson");
    let output = invoke(root.path(), &capture, false);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(root.path().join("private/output/customers.csv").exists());
    assert!(!capture.exists());
    assert!(machine_events(&output).last().expect("machine terminal")["observability"].is_null());
}
