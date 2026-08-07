//! End-to-end contracts for the CLI-owned optional-observability bulkhead.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

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

/// Four customers with varying amounts, and a per-record event gated on a
/// field the directive never requests. The gate reads `amount`; only
/// `customer_id` is exported.
fn write_gated_pipeline(root: &Path, output: &str) {
    std::fs::create_dir_all(root.join("private/source")).expect("source directory");
    std::fs::create_dir_all(root.join("private/output")).expect("output directory");
    std::fs::write(
        root.join("private/source/customers.csv"),
        "customer_id,amount\ncustomer-1,500\ncustomer-2,5000\ncustomer-3,900\ncustomer-4,2000\n",
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
      schema:
        - {{ name: customer_id, type: string }}
        - {{ name: amount, type: int }}
  - type: transform
    name: normalize
    input: customers
    config:
      cxl: |
        emit customer_id = customer_id
        emit amount = amount
      log:
        - name: transform.customer_seen
          level: info
          when: per_record
          message: customer processed
          fields: [customer_id]
          every: 1
          condition: "amount > 1000"
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

/// Every exported log record's attributes, flattened to `key -> stringValue`.
fn captured_log_attributes(capture: &Path) -> Vec<BTreeMap<String, String>> {
    let mut records = Vec::new();
    for entry in capture_events(capture) {
        if entry["signal"] != "logs" {
            continue;
        }
        let Some(resource_logs) = entry["payload"]["resourceLogs"].as_array() else {
            continue;
        };
        for resource in resource_logs {
            let Some(scope_logs) = resource["scopeLogs"].as_array() else {
                continue;
            };
            for scope in scope_logs {
                let Some(log_records) = scope["logRecords"].as_array() else {
                    continue;
                };
                for record in log_records {
                    let attributes = record["attributes"]
                        .as_array()
                        .map(|attributes| {
                            attributes
                                .iter()
                                .filter_map(|attribute| {
                                    Some((
                                        attribute["key"].as_str()?.to_owned(),
                                        attribute["value"]["stringValue"].as_str()?.to_owned(),
                                    ))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    records.push(attributes);
                }
            }
        }
    }
    records
}

/// End-to-end proof that an authored `condition` survives the whole path —
/// YAML admission, planning, lowering, and executor dispatch — and reaches a
/// real OTLP payload having actually suppressed the records it excludes.
///
/// The dispatcher-level unit tests in `clinker-exec` cover the gate itself;
/// this covers everything between the author's file and the collector.
#[test]
fn authored_condition_gates_the_exported_payload() {
    let root = fixture();
    write_gated_pipeline(root.path(), "./private/output/customers.csv");
    write_observability_policy(
        root.path(),
        "https://collector.example.com",
        "mode = \"none\"",
    );
    let capture = root.path().join("otlp.ndjson");

    let output = invoke(root.path(), &capture, false);
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Every input record reached the transform, so a missing log event is the
    // gate's doing and not a short input. Without this the assertion below
    // would also pass on a pipeline that silently processed two rows.
    let published = std::fs::read_to_string(root.path().join("private/output/customers.csv"))
        .expect("published output");
    assert_eq!(
        published.lines().count(),
        5,
        "header plus four records must be published regardless of gating: {published}"
    );

    let events = captured_log_attributes(&capture)
        .into_iter()
        .filter(|attributes| {
            attributes.get("clinker.event").map(String::as_str) == Some("transform.customer_seen")
        })
        .collect::<Vec<_>>();
    let gated = events
        .iter()
        .filter_map(|attributes| attributes.get("customer_id").cloned())
        .collect::<Vec<_>>();

    assert_eq!(
        gated,
        vec!["customer-2".to_owned(), "customer-4".to_owned()],
        "only records satisfying `amount > 1000` may reach the collector"
    );

    // The gate reads `amount`, which the directive never requested. Reading a
    // field to decide whether to fire must not export it.
    assert!(
        events
            .iter()
            .all(|attributes| !attributes.contains_key("amount")),
        "a gated field must not become an exported attribute: {events:?}"
    );
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
        let events = machine_events(&output);
        let terminal = events.last().expect("machine terminal");
        assert_eq!(terminal["event"], "failed");
        assert_eq!(
            terminal["failure"]["code"],
            "observability.configuration.invalid"
        );
        assert_eq!(terminal["failure"]["category"], "observability");
        assert_eq!(terminal["failure"]["retry"], "do_not_retry");
        assert_eq!(terminal["exit_code"], 1);
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

const PRIVACY_SENTINEL: &str = "PRIVATE-OBSERVABILITY-SENTINEL-42";

fn write_fault_matrix_pipeline(root: &Path) {
    std::fs::create_dir_all(root.join("private/source")).expect("source directory");
    std::fs::create_dir_all(root.join("private/output")).expect("output directory");
    std::fs::write(
        root.join("private/source/customers.csv"),
        format!(
            "customer_id,amount,secret_note\ncustomer-7,10,{PRIVACY_SENTINEL}\ncustomer-8,0,{PRIVACY_SENTINEL}\n"
        ),
    )
    .expect("fault matrix input");
    std::fs::write(
        root.join("pipeline.yaml"),
        r#"pipeline: { name: observability_fault_matrix }
error_handling:
  strategy: continue
  dlq: { path: ./private/output/rejected.ndjson }
nodes:
  - type: source
    name: customers
    config:
      name: customers
      type: csv
      path: ./private/source/customers.csv
      schema:
        - { name: customer_id, type: string }
        - { name: amount, type: int }
        - { name: secret_note, type: string }
  - type: transform
    name: normalize
    input: customers
    config:
      cxl: |
        emit customer_id = customer_id
        emit secret_note = secret_note
        emit score = if(amount == 0) then (1 / 0) else amount
      log:
        - name: transform.customer_seen
          level: info
          when: per_record
          message: customer processed
          fields: [customer_id, secret_note]
          every: 1
  - type: output
    name: published_customers
    input: normalize
    config:
      name: published_customers
      type: csv
      path: ./private/output/customers.csv
      if_exists: overwrite
"#,
    )
    .expect("fault matrix pipeline");
}

fn write_fault_matrix_policy(root: &Path, lineage_max_event: &str) {
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
flush_timeout_ms = 100

[observability.otlp]
endpoint = "https://collector.example.com"
connect_timeout_ms = 20
request_timeout_ms = 50
retry_max_attempts = 1
retry_total_timeout_ms = 100
max_response_bytes = "4KB"

[observability.otlp.auth]
mode = "none"

[observability.lineage]
queue_bytes = "4KB"
max_event_bytes = "{lineage_max_event}"
flush_timeout_ms = 50
identity_mode = "external"

[[observability.lineage.dataset]]
node = "customers"
canonical_datasource = "s3://warehouse/customers"

[[observability.lineage.dataset]]
node = "published_customers"
catalog_namespace = "analytics"
catalog_name = "customers"

[[observability.field_policy]]
event = "transform.customer_seen"
field = "customer_id"
action = "allow"
"#
        ),
    )
    .expect("fault matrix policy");
}

fn seed_retained_attempt_evidence(root: &Path) {
    std::fs::create_dir_all(root.join("retained")).expect("retained output directory");
    let mut input = String::from("value\n");
    input.push_str(&"x".repeat(1_100_000));
    input.push('\n');
    std::fs::write(root.join("retained-source.csv"), input).expect("retention input");
    std::fs::write(
        root.join("retained-pipeline.yaml"),
        r#"pipeline: { name: retained_attempt_seed }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: retained-source.csv
      schema: [{ name: value, type: string }]
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: retained/result.csv
"#,
    )
    .expect("retention pipeline");
    std::fs::write(
        root.join("clinker.toml"),
        "[storage.publication]\nfailed_retention_seconds = 0\ncreation_grace_seconds = 1\nmax_attempt_bytes = \"1MB\"\nretained_byte_limit = \"2MB\"\nmin_free_bytes = \"1B\"\n",
    )
    .expect("retention policy");
    let seeded = Command::new(clinker_bin())
        .current_dir(root)
        .args(["run", "retained-pipeline.yaml"])
        .output()
        .expect("seed failed attempt");
    assert_eq!(
        seeded.status.code(),
        Some(4),
        "retention seed stderr: {}",
        String::from_utf8_lossy(&seeded.stderr)
    );
    let namespace = root.join("retained/.clinker-attempts");
    assert!(
        std::fs::read_dir(&namespace)
            .expect("retained namespace")
            .filter_map(Result::ok)
            .any(|entry| entry.path().is_dir() && entry.path().join("manifest.json").is_file()),
        "failed run did not retain its manifest"
    );
}

fn collect_files(
    root: &Path,
    current: &Path,
    include: impl Fn(&Path) -> bool + Copy,
) -> Vec<(String, Vec<u8>)> {
    fn visit(
        root: &Path,
        current: &Path,
        include: impl Fn(&Path) -> bool + Copy,
        files: &mut Vec<(String, Vec<u8>)>,
    ) {
        if !current.exists() {
            return;
        }
        let mut entries = std::fs::read_dir(current)
            .expect("read oracle directory")
            .map(|entry| entry.expect("oracle entry").path())
            .collect::<Vec<_>>();
        entries.sort();
        for path in entries {
            if path.is_dir() {
                visit(root, &path, include, files);
            } else if include(&path) {
                // Render with a single canonical separator rather than the
                // platform's. The oracle compares these paths across runs and
                // strips the run-local execution id by splitting on '/', so a
                // native separator would silently leave that identity in the
                // comparison and report every run as authoritative drift.
                files.push((
                    path.strip_prefix(root)
                        .expect("oracle relative path")
                        .components()
                        .map(|component| component.as_os_str().to_string_lossy())
                        .collect::<Vec<_>>()
                        .join("/"),
                    std::fs::read(&path).expect("read oracle file"),
                ));
            }
        }
    }

    let mut files = Vec::new();
    visit(root, current, include, &mut files);
    files
}

fn canonical_dlq_bytes(path: &Path) -> Vec<u8> {
    let bytes = std::fs::read(path).expect("DLQ bytes");
    let mut lines = bytes.split(|byte| *byte == b'\n');
    let header = lines.next().expect("DLQ header");
    assert!(header.starts_with(b"_cxl_dlq_id,_cxl_dlq_timestamp,"));
    let mut canonical = header.to_vec();
    canonical.push(b'\n');
    for line in lines.filter(|line| !line.is_empty()) {
        let first = line.iter().position(|byte| *byte == b',').expect("DLQ id");
        let second = line[first + 1..]
            .iter()
            .position(|byte| *byte == b',')
            .map(|offset| first + 1 + offset)
            .expect("DLQ timestamp");
        canonical.extend_from_slice(b"<run-local-id>,<run-local-time>");
        canonical.extend_from_slice(&line[second..]);
        canonical.push(b'\n');
    }
    canonical
}

#[derive(Debug, Eq, PartialEq)]
struct AuthorityOracle {
    final_bytes: Vec<u8>,
    canonical_dlq_bytes: Vec<u8>,
    status: Option<i32>,
    terminal: Value,
    publication_inventory: Vec<Value>,
    visible_finals: Vec<String>,
    retained_attempts: Vec<(String, usize)>,
}

#[derive(Debug)]
struct MatrixRun {
    output: Output,
    oracle: AuthorityOracle,
    observability: Value,
    collector_bytes: Vec<u8>,
    lineage_bytes: Vec<u8>,
    elapsed: Duration,
}

fn invoke_fault_matrix(
    otlp_signal: Option<&str>,
    otlp_outcome: &str,
    lineage_sink: Option<&str>,
    lineage_repeat: bool,
    lineage_max_event: &str,
) -> MatrixRun {
    let root = fixture();
    seed_retained_attempt_evidence(root.path());
    write_fault_matrix_pipeline(root.path());
    write_fault_matrix_policy(root.path(), lineage_max_event);
    let retained_root = root.path().join("retained/.clinker-attempts");
    let retained_before = collect_files(root.path(), &retained_root, |_| true);
    let capture = root.path().join("otlp.ndjson");
    let lineage = root.path().join("lineage.ndjson");
    let mut command = Command::new(clinker_bin());
    command
        .current_dir(root.path())
        .env("CLINKER_TEST_OTLP_OUTCOME", "success")
        .env("CLINKER_TEST_OTLP_CAPTURE", &capture)
        .args([
            "run",
            "pipeline.yaml",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "observability-fault-matrix",
            "--lineage-events",
            "lineage.ndjson",
        ]);
    if let Some(signal) = otlp_signal {
        let variable = match signal {
            "logs" => "CLINKER_TEST_OTLP_LOGS_OUTCOME",
            "metrics" => "CLINKER_TEST_OTLP_METRICS_OUTCOME",
            "traces" => "CLINKER_TEST_OTLP_TRACES_OUTCOME",
            _ => panic!("unknown OTLP signal"),
        };
        command.env(variable, otlp_outcome);
    }
    if let Some(mode) = lineage_sink {
        command.env("CLINKER_TEST_LINEAGE_SINK", mode);
    }
    if lineage_repeat {
        command.env("CLINKER_TEST_LINEAGE_REPEAT", "64");
    }

    let started = Instant::now();
    let output = command.output().expect("run fault matrix case");
    let elapsed = started.elapsed();
    let events = machine_events(&output);
    let mut terminal = events.last().expect("machine terminal").clone();
    let observability = terminal
        .as_object_mut()
        .expect("machine terminal object")
        .remove("observability")
        .unwrap_or_else(|| {
            panic!(
                "observability counters; stdout={} stderr={}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            )
        });
    terminal["execution_id"] = Value::String("<run-local-execution-id>".to_owned());
    // The stream counter advances with however many observability events precede
    // the terminal, and a delivery fault can emit one more than a clean run. That
    // is stream position, not ETL truth, so it is normalized out of the authority
    // oracle exactly like the run-local execution id. Sequence numbering itself is
    // covered by the machine-protocol contract test, which asserts seq == index.
    terminal["seq"] = Value::String("<stream-local-seq>".to_owned());
    let publication_inventory = events
        .iter()
        .filter(|event| event["event"] == "publication_artifacts")
        .map(|event| event["publication"].clone())
        .collect::<Vec<_>>();
    let visible_finals = collect_files(root.path(), &root.path().join("private/output"), |path| {
        !path
            .components()
            .any(|part| part.as_os_str().to_string_lossy().starts_with(".clinker"))
    })
    .into_iter()
    .map(|(path, _)| path)
    .collect();
    let retained_after = collect_files(root.path(), &retained_root, |_| true);
    assert_eq!(
        retained_after, retained_before,
        "D-15 retained attempt evidence changed"
    );
    let retained_attempts = retained_after
        .iter()
        .map(|(path, bytes)| {
            let mut components = path.split('/').collect::<Vec<_>>();
            if let Some(index) = components
                .iter()
                .position(|component| *component == ".clinker-attempts")
                && components
                    .get(index + 1)
                    .is_some_and(|component| !component.starts_with('.'))
            {
                components[index + 1] = "<run-local-execution-id>";
            }
            (components.join("/"), bytes.len())
        })
        .collect();
    let collector_bytes = std::fs::read(&capture).unwrap_or_default();
    let lineage_bytes = std::fs::read(&lineage).unwrap_or_default();
    let oracle = AuthorityOracle {
        final_bytes: std::fs::read(root.path().join("private/output/customers.csv"))
            .expect("authoritative final"),
        canonical_dlq_bytes: canonical_dlq_bytes(
            &root.path().join("private/output/rejected.ndjson"),
        ),
        status: output.status.code(),
        terminal,
        publication_inventory,
        visible_finals,
        retained_attempts,
    };
    MatrixRun {
        output,
        oracle,
        observability,
        collector_bytes,
        lineage_bytes,
        elapsed,
    }
}

fn assert_private_surfaces_are_clean(run: &MatrixRun) {
    for (name, bytes) in [
        ("collector", run.collector_bytes.as_slice()),
        ("lineage", run.lineage_bytes.as_slice()),
        ("diagnostic", run.output.stderr.as_slice()),
        ("machine", run.output.stdout.as_slice()),
    ] {
        assert!(
            !String::from_utf8_lossy(bytes).contains(PRIVACY_SENTINEL),
            "privacy sentinel escaped through {name}"
        );
    }
    assert!(
        !run.observability.to_string().contains(PRIVACY_SENTINEL),
        "privacy sentinel escaped through counters"
    );
}

#[test]
fn fault_matrix_endpoint_partitions_fail_before_every_effect() {
    for endpoint in [
        "not an endpoint",
        "/relative/collector",
        "http://collector.example.com",
        "https://user:secret@collector.example.com",
        "https://collector.example.com/tenant",
        "https://collector.example.com?tenant=red",
        "https://collector.example.com#tenant",
        "https://collector.example.com/v1/logs",
        "https://collector.example.com/v1/metrics",
        "https://collector.example.com/v1/traces",
    ] {
        let root = fixture();
        write_pipeline(root.path(), "./private/output/must-not-exist.csv");
        write_observability_policy(root.path(), endpoint, "mode = \"none\"");
        let capture = root.path().join("must-not-capture.ndjson");
        let output = invoke(root.path(), &capture, true);
        assert_eq!(output.status.code(), Some(1), "endpoint {endpoint}");
        let diagnostic = String::from_utf8_lossy(&output.stderr);
        assert!(
            diagnostic.contains("observability.configuration.invalid"),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("observability.otlp.endpoint"),
            "{diagnostic}"
        );
        assert!(
            diagnostic.contains("https://collector.example.com"),
            "{diagnostic}"
        );
        assert!(
            !diagnostic.contains(endpoint),
            "rejected endpoint leaked: {diagnostic}"
        );
        assert!(
            !root
                .path()
                .join("private/output/must-not-exist.csv")
                .exists()
        );
        assert!(!root.path().join("lineage.ndjson").exists());
        assert!(!root.path().join(".clinker-attempts").exists());
        assert!(!capture.exists());
    }
}

#[test]
fn worker_startup_failures_are_preeffect_and_machine_terminal() {
    fn snapshot(root: &Path) -> Vec<(String, Vec<u8>)> {
        collect_files(root, root, |_| true)
    }

    for variable in [
        "CLINKER_TEST_OTLP_WORKER_START_FAILURE",
        "CLINKER_TEST_LINEAGE_WORKER_START_FAILURE",
    ] {
        let root = fixture();
        write_fault_matrix_pipeline(root.path());
        write_fault_matrix_policy(root.path(), "4KB");
        std::fs::create_dir(root.path().join("staging")).expect("staging root");
        std::fs::write(root.path().join("staging/sentinel"), b"staging-before\n")
            .expect("staging sentinel");
        std::fs::write(
            root.path().join("private/output/customers.csv"),
            b"output-before\n",
        )
        .expect("output sentinel");
        std::fs::write(root.path().join("lineage.ndjson"), b"lineage-before\n")
            .expect("lineage sentinel");
        let before = snapshot(root.path());

        let output = Command::new(clinker_bin())
            .current_dir(root.path())
            .env("CLINKER_TEST_OTLP_OUTCOME", "success")
            .env(variable, "1")
            .args([
                "run",
                "pipeline.yaml",
                "--machine",
                "ndjson-v1",
                "--batch-id",
                "worker-startup-failure",
                "--lineage-events",
                "lineage.ndjson",
            ])
            .output()
            .expect("run worker startup failure");

        assert_eq!(output.status.code(), Some(4), "{variable}");
        let events = machine_events(&output);
        let terminals = events
            .iter()
            .filter(|event| {
                matches!(
                    event["event"].as_str(),
                    Some("completed" | "failed" | "cancelled")
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(terminals.len(), 1, "{variable}: {events:#?}");
        assert_eq!(terminals[0]["event"], "failed", "{variable}");
        assert_eq!(
            terminals[0]["failure"]["code"], "observability.delivery.failed",
            "{variable}"
        );
        assert_eq!(
            terminals[0]["failure"]["category"], "observability",
            "{variable}"
        );
        assert_eq!(
            terminals[0]["failure"]["retry"], "retry_with_backoff",
            "{variable}"
        );
        assert_eq!(terminals[0]["exit_code"], 4, "{variable}");
        assert_eq!(snapshot(root.path()), before, "{variable}");
        assert!(!root.path().join(".clinker-attempts").exists());
    }
}

#[test]
fn fault_matrix_otlp_outcomes_change_only_the_selected_signal() {
    let baseline = invoke_fault_matrix(None, "success", None, false, "4KB");
    assert_eq!(baseline.oracle.status, Some(2));
    assert_eq!(baseline.oracle.terminal["event"], "completed");
    assert_eq!(baseline.oracle.terminal["result"], "completed_with_dlq");
    assert_private_surfaces_are_clean(&baseline);

    for signal in ["logs", "metrics", "traces"] {
        for outcome in [
            "partial",
            "permanent-rejection",
            "transient-exhausted",
            "auth",
            "tls",
            "connect",
            "read-timeout",
            "oversized-response",
            "malformed-response",
            "shutdown",
            "flush-expiry",
        ] {
            let run = invoke_fault_matrix(Some(signal), outcome, None, false, "4KB");
            assert!(
                run.elapsed < Duration::from_secs(3),
                "{signal}/{outcome} blocked completion"
            );
            assert_eq!(
                run.oracle, baseline.oracle,
                "authoritative drift for {signal}/{outcome}"
            );
            assert_private_surfaces_are_clean(&run);
            for sibling in ["logs", "metrics", "traces"] {
                if sibling != signal {
                    assert_eq!(
                        run.observability[sibling], baseline.observability[sibling],
                        "{signal}/{outcome} changed sibling {sibling}"
                    );
                }
            }
            assert_ne!(
                run.observability[signal], baseline.observability[signal],
                "{signal}/{outcome} did not surface its typed outcome"
            );
            let diagnostic = String::from_utf8_lossy(&run.output.stderr);
            assert!(
                diagnostic.contains(&format!("optional OTLP {signal} delivery outcome")),
                "{diagnostic}"
            );
            assert!(diagnostic.contains(outcome), "{diagnostic}");
        }
    }
}

#[test]
fn fault_matrix_lineage_outcomes_leave_otlp_and_authoritative_truth_unchanged() {
    let baseline = invoke_fault_matrix(None, "success", None, false, "4KB");
    for (mode, repeat, max_event, expected) in [
        (
            "permission-denied",
            false,
            "4KB",
            "error_kind=permission-denied",
        ),
        ("write-failed", false, "4KB", "error_kind=broken-pipe"),
        ("flush-failed", false, "4KB", "error_kind=write-zero"),
        ("hang-after-first-write", false, "4KB", "deadline-exceeded"),
        ("hang-after-first-write", true, "4KB", "full="),
        ("success", false, "1KB", "dropped="),
    ] {
        let run = invoke_fault_matrix(Some("logs"), "success", Some(mode), repeat, max_event);
        assert!(
            run.elapsed < Duration::from_secs(3),
            "lineage {mode} blocked completion"
        );
        assert_eq!(
            run.oracle, baseline.oracle,
            "lineage {mode} changed authoritative truth"
        );
        assert_eq!(
            run.observability, baseline.observability,
            "lineage {mode} changed OTLP counters"
        );
        assert_private_surfaces_are_clean(&run);
        let diagnostic = String::from_utf8_lossy(&run.output.stderr);
        assert!(
            diagnostic.contains("lineage delivery outcome"),
            "{diagnostic}"
        );
        assert!(diagnostic.contains(expected), "{diagnostic}");
    }
}
