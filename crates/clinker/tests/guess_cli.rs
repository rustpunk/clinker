use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const FIXTURE_ROOT: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/guess");
const FIXTURE_FILES: &[&str] = &[
    "manifest.yaml",
    "pipeline.yaml",
    "channel.yaml",
    "group.yaml",
    "input.csv",
    "input.json",
    "input.xml",
    "expected-preview.json",
    "expected.patch",
];

fn fixture_path(name: &str) -> PathBuf {
    Path::new(FIXTURE_ROOT).join(name)
}

fn copy_fixture(root: &Path, name: &str, destination: &str) {
    let destination = root.join(destination);
    std::fs::create_dir_all(destination.parent().expect("fixture destination parent"))
        .expect("create fixture destination");
    std::fs::copy(fixture_path(name), destination).expect("copy guess fixture");
}

fn workspace() -> tempfile::TempDir {
    let workspace = tempfile::tempdir().expect("temporary guess workspace");
    let root = workspace.path();
    for name in ["pipeline.yaml", "input.csv", "input.json", "input.xml"] {
        copy_fixture(root, name, name);
    }
    copy_fixture(
        root,
        "channel.yaml",
        "channel/json_preview/pipeline.channel.yaml",
    );
    copy_fixture(root, "group.yaml", "group/xml_preview.group.yaml");
    std::fs::write(
        root.join("channel/json_preview/channel.cfg.yaml"),
        "channel:\n  name: json_preview\n  targets: [guess.pipeline]\n",
    )
    .expect("write channel manifest");
    std::fs::write(
        root.join("clinker.toml"),
        "[catalog.pipelines]\n\"guess.pipeline\" = \"pipeline.yaml\"\n\n\
         [catalog.channels]\njson_preview = \"channel/json_preview\"\n\n\
         [channel]\nroot = \"channel\"\n\n[group]\nroot = \"group\"\n",
    )
    .expect("write workspace catalog");
    workspace
}

fn guess(root: &Path, args: &[&str]) -> Output {
    guess_config(root, "pipeline.yaml", args)
}

fn guess_config(root: &Path, config: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(root)
        .arg("guess")
        .arg(config)
        .args(args)
        .output()
        .expect("spawn clinker guess")
}

fn guess_with_env(root: &Path, args: &[&str], envs: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_clinker"));
    command
        .current_dir(root)
        .arg("guess")
        .arg("pipeline.yaml")
        .args(args);
    for (name, value) in envs {
        command.env(name, value);
    }
    command.output().expect("spawn clinker guess")
}

fn enable_guess_telemetry(root: &Path) {
    let path = root.join("clinker.toml");
    let mut config = std::fs::read_to_string(&path).unwrap_or_default();
    config.push_str(
        r#"
[observability]
arena_bytes = "64KB"
ordinary_lane_bytes = "32KB"
high_severity_lane_bytes = "32KB"
max_batch_bytes = "8KB"
max_attributes_per_event = 4
max_attribute_bytes = "64B"
sample_every = 1
rate_limit_per_second = 1000
rate_limit_burst = 1000
flush_timeout_ms = 500

[observability.otlp]
endpoint = "https://collector.invalid"
connect_timeout_ms = 20
request_timeout_ms = 50
retry_max_attempts = 1
retry_total_timeout_ms = 100
max_response_bytes = "4KB"

[observability.otlp.auth]
mode = "none"
"#,
    );
    std::fs::write(path, config).expect("write Guess observability policy");
}

fn guess_with_telemetry(
    root: &Path,
    args: &[&str],
    capture: &Path,
    envs: &[(&str, &str)],
) -> Output {
    enable_guess_telemetry(root);
    let capture = capture.to_str().expect("UTF-8 capture path");
    let mut all_envs = vec![
        ("CLINKER_TEST_OTLP_OUTCOME", "success"),
        ("CLINKER_TEST_OTLP_CAPTURE", capture),
    ];
    all_envs.extend_from_slice(envs);
    guess_with_env(root, args, &all_envs)
}

fn telemetry_capture(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .expect("Guess telemetry capture")
        .lines()
        .map(|line| serde_json::from_str(line).expect("captured OTLP JSON"))
        .collect()
}

fn captured_metric_counts(capture: &[serde_json::Value]) -> BTreeMap<String, u64> {
    let mut counts = BTreeMap::new();
    for event in capture.iter().filter(|event| event["signal"] == "metrics") {
        for metric in event["payload"]["resourceMetrics"][0]["scopeMetrics"][0]["metrics"]
            .as_array()
            .expect("captured metrics")
        {
            let name = metric["name"].as_str().expect("metric name").to_owned();
            let value = metric["sum"]["dataPoints"][0]["asInt"]
                .as_str()
                .expect("metric integer")
                .parse::<u64>()
                .expect("metric count");
            *counts.entry(name).or_default() += value;
        }
    }
    counts
}

fn captured_spans(capture: &[serde_json::Value]) -> Vec<&serde_json::Value> {
    capture
        .iter()
        .filter(|event| event["signal"] == "traces")
        .flat_map(|event| {
            event["payload"]["resourceSpans"][0]["scopeSpans"][0]["spans"]
                .as_array()
                .expect("captured spans")
        })
        .collect()
}

fn assert_guess_telemetry(capture: &Path, terminal_metric: &str, expected_span_status: u64) {
    let capture = telemetry_capture(capture);
    let metrics = captured_metric_counts(&capture);
    assert_eq!(metrics.len(), 2, "one start and one terminal metric");
    assert_eq!(metrics.get("clinker.guess.started"), Some(&1));
    assert_eq!(metrics.get(terminal_metric), Some(&1));
    let spans = captured_spans(&capture);
    assert!(spans.len() <= 1, "at most one complete Guess span");
    for span in spans {
        assert_eq!(span["name"], "clinker.guess");
        assert_eq!(span["status"]["code"], expected_span_status);
        assert_eq!(
            span["attributes"],
            serde_json::json!([{
                "key": "clinker.logical_node",
                "value": { "stringValue": "guess" }
            }])
        );
    }
}

fn spawn_at_write_barrier(root: &Path, barrier: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(root)
        .env("CLINKER_TEST_GUESS_WRITE_BARRIER", barrier)
        .args(["guess", "pipeline.yaml", "--write"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn barred clinker guess")
}

fn wait_for_barrier(barrier: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !barrier.join("ready").exists() {
        assert!(Instant::now() < deadline, "guess write barrier timed out");
        std::thread::sleep(Duration::from_millis(2));
    }
}

fn parse_success(output: &Output) -> serde_json::Value {
    assert!(
        output.status.success(),
        "guess failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
    serde_json::from_slice(&output.stdout).expect("guess stdout is one JSON document")
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

fn write_flat_json_pipeline(root: &Path, declared_type: &str, default: Option<&str>, input: &str) {
    let default = default
        .map(|value| format!("\n          default: {value}"))
        .unwrap_or_default();
    std::fs::write(
        root.join("pipeline.yaml"),
        format!(
            "pipeline:\n  name: absence_policy\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: json\n      path: input.json\n      options:\n        format: array\n      schema:\n        - name: n\n          type: {declared_type}{default}\n"
        ),
    )
    .expect("write JSON policy pipeline");
    std::fs::write(root.join("input.json"), input).expect("write JSON policy input");
}

fn write_two_source_fairness_pipeline(root: &Path) {
    std::fs::write(
        root.join("pipeline.yaml"),
        "pipeline:\n  name: fair_preview\nnodes:\n  - type: source\n    name: alpha\n    config:\n      name: alpha\n      type: csv\n      glob: alpha-*.csv\n      schema:\n        - { name: n, type: numeric }\n  - type: source\n    name: beta\n    config:\n      name: beta\n      type: csv\n      glob: beta-*.csv\n      schema:\n        - { name: n, type: numeric }\n",
    )
    .expect("write fair preview pipeline");
    let body = format!("n\n{}", "1\n".repeat(400));
    for source in ["alpha", "beta"] {
        for file in 0..2 {
            std::fs::write(root.join(format!("{source}-{file}.csv")), &body)
                .expect("write fair preview input");
        }
    }
}

#[test]
fn preview_selector_corpus_manifest_lists_every_committed_artifact_once() {
    let manifest_text =
        std::fs::read_to_string(fixture_path("manifest.yaml")).expect("read manifest");
    let manifest: serde_json::Value =
        clinker_plan::yaml::from_str(&manifest_text).expect("parse manifest");
    assert_eq!(manifest["version"], 1);

    let serialized = serde_json::to_string(&manifest).expect("serialize manifest");
    for name in FIXTURE_FILES {
        assert!(fixture_path(name).is_file(), "missing fixture {name}");
        let expected_mentions = usize::from(*name != "manifest.yaml");
        assert_eq!(
            serialized.matches(name).count(),
            expected_mentions,
            "fixture {name} must be listed exactly once when it is an input or expected artifact",
        );
    }

    let case_names = manifest["cases"]
        .as_array()
        .expect("manifest cases")
        .iter()
        .map(|case| case["name"].as_str().expect("case name"))
        .collect::<Vec<_>>();
    assert_eq!(
        case_names,
        [
            "preview_selector_base_csv",
            "preview_selector_channel_json",
            "preview_selector_group_xml",
            "modes_preview_unresolved_exit_zero",
            "modes_check_unresolved_exit_three",
            "modes_write_unavailable_no_edit",
            "evidence_nullable_absence_and_default",
            "evidence_mixed_exact_votes",
        ]
    );
}

#[test]
fn preview_selector_base_is_deterministic_and_matches_byte_goldens() {
    let workspace = workspace();
    let first = guess(workspace.path(), &["--field", "csv_orders.amount"]);
    let second = guess(workspace.path(), &["--field", "csv_orders.amount"]);
    assert_eq!(first.stdout, second.stdout, "preview must be byte stable");
    assert_eq!(
        first.stdout,
        std::fs::read(fixture_path("expected-preview.json")).expect("read preview golden")
    );

    let report = parse_success(&first);
    assert_eq!(
        report["patch"],
        std::fs::read_to_string(fixture_path("expected.patch")).expect("read patch golden")
    );
}

#[test]
fn preview_selector_multi_record_field_reports_each_literal_numeric_owner() {
    let workspace = workspace();
    let report = parse_success(&guess(workspace.path(), &["--field", "csv_orders.amount"]));
    let owners = report["fields"][0]["owners"]
        .as_array()
        .expect("exact owner reports");
    assert_eq!(owners.len(), 2);
    assert_eq!(
        owners[0]["address"],
        "/v1/schema/sources/csv_orders/records/detail/columns/amount/attributes/type"
    );
    assert_eq!(owners[0]["observations"], 1);
    assert_eq!(owners[0]["proposed_type"], "int");
    assert_eq!(owners[0]["evidence"][0]["lexeme"], "10");
    assert_eq!(
        owners[1]["address"],
        "/v1/schema/sources/csv_orders/records/adjustment/columns/amount/attributes/type"
    );
    assert_eq!(owners[1]["observations"], 1);
    assert_eq!(owners[1]["proposed_type"], "float");
    assert_eq!(owners[1]["evidence"][0]["lexeme"], "20.5");
    let patch = report["patch"].as_str().expect("patch");
    assert!(patch.contains("records/detail/columns/amount/attributes/type"));
    assert!(patch.contains("records/adjustment/columns/amount/attributes/type"));
    assert!(
        !patch.contains("records/summary/columns/amount/attributes/type"),
        "the concrete summary declaration must not be proposed as an edit: {patch}"
    );
}

#[test]
fn preview_many_files_keeps_fixed_coverage_and_evidence_storage() {
    let workspace = workspace();
    let pipeline_path = workspace.path().join("pipeline.yaml");
    let pipeline = std::fs::read_to_string(&pipeline_path).expect("read pipeline fixture");
    std::fs::write(
        &pipeline_path,
        pipeline.replacen("path: input.csv", "glob: input-*.csv", 1),
    )
    .expect("select a many-file input set");
    let body = format!(
        "kind,order_id,amount\n{}",
        "D,c-detail,10\nA,c-adjustment,20.5\n".repeat(10)
    );
    for index in 0..12 {
        std::fs::write(
            workspace.path().join(format!("input-{index:02}.csv")),
            &body,
        )
        .expect("write sampled input");
    }

    let report = parse_success(&guess(workspace.path(), &["--field", "csv_orders.amount"]));
    let coverage = &report["coverage"][0];
    assert_eq!(coverage["discovered_files"], 12);
    assert_eq!(coverage["unreported_file_count"], 8);
    let manifest_source = &report["manifest"]["sources"][0];
    assert_eq!(manifest_source["discovered_files"], 12);
    assert_eq!(
        manifest_source["identity"]
            .as_str()
            .expect("fixed manifest identity")
            .len(),
        64
    );
    assert!(manifest_source.get("files").is_none());
    let files = coverage["files"].as_array().expect("bounded file reports");
    assert_eq!(files.len(), 4);
    assert_eq!(files[0]["path"], "input-00.csv");
    assert_eq!(files[3]["path"], "input-03.csv");

    for owner in report["fields"][0]["owners"]
        .as_array()
        .expect("owner reports")
    {
        assert_eq!(owner["observations"], 40);
        assert_eq!(
            owner["evidence"]
                .as_array()
                .expect("bounded evidence")
                .len(),
            8
        );
    }
}

#[test]
fn preview_selector_channel_uses_effective_json_schema_and_parser() {
    let workspace = workspace();
    let output = guess(
        workspace.path(),
        &[
            "--base-dir",
            ".",
            "--channel",
            "json_preview",
            "--field",
            "json_orders.ratio",
        ],
    );
    let report = parse_success(&output);
    assert_eq!(report["selection"]["kind"], "channel");
    assert_eq!(report["fields"][0]["owners"][0]["proposed_type"], "float");
    assert_eq!(
        report["fields"][0]["owners"][0]["evidence"][0]["boundary"],
        "json"
    );
    assert_eq!(report["coverage"][0]["files"][0]["path"], "input.json");
}

#[test]
fn preview_selector_group_uses_effective_xml_schema_and_parser() {
    let workspace = workspace();
    let output = guess(
        workspace.path(),
        &[
            "--base-dir",
            ".",
            "--group",
            "xml_preview",
            "--field",
            "xml_orders.total",
        ],
    );
    let report = parse_success(&output);
    assert_eq!(report["selection"]["kind"], "group");
    assert_eq!(report["fields"][0]["owners"][0]["proposed_type"], "int");
    assert_eq!(
        report["fields"][0]["owners"][0]["evidence"][0]["boundary"],
        "xml"
    );
    assert_eq!(report["coverage"][0]["files"][0]["path"], "input.xml");
}

#[test]
fn preview_selector_conflict_absence_and_ambiguity_exit_one() {
    let workspace = workspace();
    let conflict = guess(
        workspace.path(),
        &["--channel", "json_preview", "--group", "xml_preview"],
    );
    assert_eq!(conflict.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&conflict.stderr).contains("choose exactly one"));

    let absent = guess(workspace.path(), &["--channel", "missing"]);
    assert_eq!(absent.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&absent.stderr).contains("missing"));

    copy_fixture(workspace.path(), "group.yaml", "group/duplicate.group.yaml");
    let ambiguous = guess(workspace.path(), &["--group", "xml_preview"]);
    assert_eq!(ambiguous.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&ambiguous.stderr).contains("xml_preview"));
}

#[test]
fn preview_selector_fields_deduplicate_and_route_concrete_multiplicity_fields() {
    let workspace = workspace();
    let deduplicated = guess(
        workspace.path(),
        &[
            "--field",
            "csv_orders.amount",
            "--field",
            "csv_orders.amount",
        ],
    );
    let report = parse_success(&deduplicated);
    assert_eq!(report["fields"].as_array().expect("fields").len(), 1);

    for field in ["csv_orders.missing", "amount"] {
        let rejected = guess(workspace.path(), &["--field", field]);
        assert_eq!(rejected.status.code(), Some(1), "field {field}");
        let stderr = String::from_utf8_lossy(&rejected.stderr);
        assert!(stderr.contains(field), "stderr for {field}: {stderr}");
        assert!(stderr.contains("--field"), "stderr for {field}: {stderr}");
    }

    let multiplicity = tempfile::tempdir().expect("temporary concrete selector workspace");
    std::fs::write(
        multiplicity.path().join("pipeline.yaml"),
        "pipeline:\n  name: concrete_selector\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: json\n      path: input.json\n      options: { format: array }\n      schema:\n        - { name: tag, type: string }\n",
    )
    .unwrap();
    std::fs::write(
        multiplicity.path().join("input.json"),
        r#"[{"tag":"one"},{"tag":"two"}]"#,
    )
    .unwrap();
    let concrete = guess(multiplicity.path(), &["--field", "values.tag"]);
    assert_eq!(
        concrete.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&concrete.stdout),
        String::from_utf8_lossy(&concrete.stderr)
    );
    let concrete = parse_report(&concrete);
    assert_eq!(concrete["fields"], serde_json::json!([]));
    assert_eq!(concrete["multiplicity"][0]["field"], "values.tag");

    let pipeline_path = workspace.path().join("pipeline.yaml");
    let pipeline = std::fs::read_to_string(&pipeline_path).expect("read pipeline fixture");
    std::fs::write(
        &pipeline_path,
        pipeline.replacen("type: numeric", "type: { nullable: numeric }", 1),
    )
    .expect("make amount nullable");
    let nullable = parse_success(&guess(workspace.path(), &["--field", "csv_orders.amount"]));
    assert!(
        nullable["patch"]
            .as_str()
            .expect("patch")
            .contains("from: nullable(numeric)\n    to: nullable(int)",)
    );
}

#[test]
fn modes_preview_and_check_have_exhaustive_exit_truth() {
    let workspace = workspace();
    let input = workspace.path().join("input.csv");
    std::fs::write(
        &input,
        "kind,order_id,amount\nD,c-1,9223372036854775808\nA,c-2,20.5\nS,c-3,30\n",
    )
    .expect("write unresolved numeric input");

    let preview = guess(workspace.path(), &["--field", "csv_orders.amount"]);
    assert_eq!(preview.status.code(), Some(0));
    let preview_report = parse_report(&preview);
    assert_eq!(preview_report["mode"], "preview");
    assert_eq!(preview_report["exhaustive"], false);
    assert_eq!(
        preview_report["fields"][0]["owners"][0]["proposed_type"],
        serde_json::Value::Null
    );

    let check = guess(
        workspace.path(),
        &["--check", "--field", "csv_orders.amount"],
    );
    assert_eq!(check.status.code(), Some(3));
    let check_report = parse_report(&check);
    assert_eq!(check_report["mode"], "check");
    assert_eq!(check_report["exhaustive"], true);
    assert_eq!(
        check_report["fields"][0]["owners"][0]["unresolved_reasons"][0],
        "unsafe_integer_widening"
    );

    let conflicting = guess(workspace.path(), &["--check", "--write"]);
    assert_eq!(conflicting.status.code(), Some(1));
    let diagnostic = String::from_utf8_lossy(&conflicting.stderr);
    assert!(diagnostic.contains("--check and --write"));
    assert!(diagnostic.contains("remove one"));
}

#[test]
fn modes_check_exhausts_the_frozen_manifest_beyond_preview_budget() {
    let workspace = workspace();
    let pipeline_path = workspace.path().join("pipeline.yaml");
    let pipeline = std::fs::read_to_string(&pipeline_path).expect("read pipeline fixture");
    std::fs::write(
        &pipeline_path,
        pipeline.replacen("path: input.csv", "glob: input-*.csv", 1),
    )
    .expect("select a many-file input set");
    for index in 0..6 {
        std::fs::write(
            workspace.path().join(format!("input-{index:02}.csv")),
            "kind,order_id,amount\nD,c-detail,10\nA,c-adjustment,20.5\n",
        )
        .expect("write exhaustive input");
    }

    let preview = parse_success(&guess(workspace.path(), &["--field", "csv_orders.amount"]));
    assert_eq!(preview["fields"][0]["owners"][0]["observations"], 4);

    let check = guess(
        workspace.path(),
        &["--check", "--field", "csv_orders.amount"],
    );
    assert_eq!(check.status.code(), Some(0));
    let check = parse_report(&check);
    assert_eq!(check["fields"][0]["owners"][0]["observations"], 6);
    assert_eq!(check["coverage"][0]["discovered_files"], 6);
    assert_eq!(check["coverage"][0]["sampled_files"], 6);
}

#[test]
fn modes_preview_allocates_global_records_fairly_across_sources() {
    let workspace = workspace();
    write_two_source_fairness_pipeline(workspace.path());

    let report = parse_success(&guess(workspace.path(), &[]));
    assert_eq!(report["manifest"]["total_files"], 4);
    assert_eq!(report["limits"]["preview_max_file_opens_total"], 4);
    assert_eq!(report["limits"]["preview_max_records_total"], 1024);
    let coverage = report["coverage"].as_array().expect("source coverage");
    assert_eq!(coverage.len(), 2);
    assert_eq!(coverage[0]["records_sampled"], 512);
    assert_eq!(coverage[1]["records_sampled"], 512);
    assert_eq!(
        coverage
            .iter()
            .map(|source| source["records_sampled"].as_u64().expect("record count"))
            .sum::<u64>(),
        1024
    );
}

#[test]
fn modes_preview_enforces_one_global_input_byte_budget() {
    let workspace = workspace();
    std::fs::write(
        workspace.path().join("pipeline.yaml"),
        "pipeline:\n  name: byte_budget\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      glob: input-*.csv\n      schema:\n        - { name: n, type: numeric }\n",
    )
    .expect("write byte-budget pipeline");
    let body = format!("n\n{}", "1\n".repeat(5 * 1024 * 1024 / 2));
    for file in 0..2 {
        std::fs::write(workspace.path().join(format!("input-{file}.csv")), &body)
            .expect("write byte-budget input");
    }
    std::fs::write(workspace.path().join("input-2.csv"), "n\n1\n")
        .expect("write small file after an over-budget prefix member");

    let report = parse_success(&guess(workspace.path(), &[]));
    let coverage = &report["coverage"][0];
    assert_eq!(coverage["discovered_files"], 3);
    assert_eq!(coverage["sampled_files"], 1);
    assert_eq!(coverage["uncovered_files"], 2);
    assert_eq!(coverage["files"][0]["path"], "input-0.csv");
    assert!(
        coverage["sampled_input_bytes"]
            .as_u64()
            .expect("sampled input bytes")
            <= report["limits"]["preview_max_input_bytes_total"]
                .as_u64()
                .expect("global input byte budget")
    );
}

#[test]
fn write_single_owned_leaf_is_surgically_replaced() {
    let workspace = workspace();
    write_flat_json_pipeline(
        workspace.path(),
        "{ nullable: numeric }",
        Some("7"),
        "[{\"n\":1,\"untouched\":2}]",
    );
    let pipeline = workspace.path().join("pipeline.yaml");
    let mut configured = std::fs::read_to_string(&pipeline)
        .expect("read configured pipeline")
        .replace(
            "          default: 7",
            "          default: 7\n          required: true\n          precision: 9\n          scale: 2",
        );
    configured.push_str("        - name: untouched\n          type: numeric\n");
    std::fs::write(&pipeline, configured).expect("add sibling type attributes");
    let before = std::fs::read_to_string(&pipeline).expect("read pipeline before write");

    let output = guess(workspace.path(), &["--write", "--field", "values.n"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report = parse_report(&output);
    assert_eq!(report["mode"], "write");
    assert_eq!(report["version"], 3);
    assert_eq!(report["write"]["status"], "written");
    assert_eq!(
        report["write"]["owner"],
        "/v1/schema/sources/values/columns/n/attributes/type"
    );
    let after = std::fs::read_to_string(&pipeline).expect("read pipeline after write");
    assert_eq!(before.matches("numeric").count(), 2);
    assert_eq!(after.matches("numeric").count(), 1);
    assert!(after.contains("type: { nullable: int }"));
    assert!(
        after.contains("default: 7"),
        "default must survive:\n{after}"
    );
    for sibling in ["required: true", "precision: 9", "scale: 2"] {
        assert!(after.contains(sibling), "{sibling} must survive:\n{after}");
    }
    assert_eq!(
        before.replacen("numeric", "int", 1),
        after,
        "only the exact type leaf may change"
    );
}

#[test]
fn write_unresolved_or_multi_owner_selection_emits_patch_without_edit() {
    let workspace = workspace();
    let pipeline = workspace.path().join("pipeline.yaml");
    let before = std::fs::read(&pipeline).expect("read pipeline");
    let multi = guess(
        workspace.path(),
        &["--write", "--field", "csv_orders.amount"],
    );
    assert_eq!(multi.status.code(), Some(3));
    let report = parse_report(&multi);
    assert_eq!(report["write"]["status"], "not_written");
    assert_eq!(report["write"]["reason"], "write_requires_one_owner");
    assert!(report["patch"].as_str().expect("patch").contains("detail"));
    assert!(
        report["patch"]
            .as_str()
            .expect("patch")
            .contains("adjustment")
    );
    assert_eq!(
        std::fs::read(&pipeline).expect("pipeline unchanged"),
        before
    );

    write_flat_json_pipeline(
        workspace.path(),
        "numeric",
        None,
        "[{\"n\":9223372036854775808},{\"n\":1.5}]",
    );
    let before = std::fs::read(&pipeline).expect("read unresolved pipeline");
    let unresolved = guess(workspace.path(), &["--write"]);
    assert_eq!(unresolved.status.code(), Some(3));
    assert_eq!(
        parse_report(&unresolved)["write"]["reason"],
        "unresolved_evidence"
    );
    assert_eq!(
        std::fs::read(&pipeline).expect("pipeline unchanged"),
        before
    );
}

#[test]
fn write_mixed_multi_record_selector_changes_only_the_literal_numeric_owner() {
    let workspace = workspace();
    std::fs::write(
        workspace.path().join("pipeline.yaml"),
        "pipeline:\n  name: mixed_records\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      path: input.csv\n      schema:\n        discriminator: { field: kind }\n        records:\n          - id: detail\n            tag: D\n            columns:\n              - { name: kind, type: string }\n              - { name: n, type: numeric }\n          - id: trailer\n            tag: T\n            columns:\n              - { name: kind, type: string }\n              - { name: n, type: int }\n",
    )
    .expect("write mixed multi-record pipeline");
    std::fs::write(workspace.path().join("input.csv"), "kind,n\nD,1\nT,2\n")
        .expect("write mixed multi-record input");

    let output = guess(workspace.path(), &["--write", "--field", "values.n"]);
    assert_eq!(
        output.status.code(),
        Some(0),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let report = parse_report(&output);
    assert_eq!(report["fields"][0]["owners"].as_array().unwrap().len(), 1);
    assert_eq!(report["write"]["status"], "written");
    let after = std::fs::read_to_string(workspace.path().join("pipeline.yaml"))
        .expect("read edited pipeline");
    assert_eq!(after.matches("name: n, type: int").count(), 2);
}

#[test]
fn write_identical_alias_interpolation_external_and_synthetic_overlay_are_patch_only() {
    let base_workspace = workspace();
    std::fs::write(
        base_workspace.path().join("pipeline.yaml"),
        "pipeline:\n  name: alias_owner\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      path: input.csv\n      schema:\n        - { name: anchor, type: &number numeric }\n        - { name: n, type: *number }\n",
    )
    .expect("write alias pipeline");
    std::fs::write(base_workspace.path().join("input.csv"), "anchor,n\n1,2\n")
        .expect("write alias input");
    let alias_before = std::fs::read(base_workspace.path().join("pipeline.yaml")).unwrap();
    let alias = guess(base_workspace.path(), &["--write", "--field", "values.n"]);
    assert_eq!(alias.status.code(), Some(3));
    assert_eq!(
        parse_report(&alias)["write"]["reason"],
        "owner_indirect_provenance"
    );
    assert_eq!(
        std::fs::read(base_workspace.path().join("pipeline.yaml")).unwrap(),
        alias_before
    );

    write_flat_json_pipeline(base_workspace.path(), "${GUESS_TYPE}", None, "[{\"n\":1}]");
    let interpolated = guess_with_env(
        base_workspace.path(),
        &["--write"],
        &[("GUESS_TYPE", "numeric")],
    );
    assert_eq!(interpolated.status.code(), Some(3));
    assert_eq!(
        parse_report(&interpolated)["write"]["reason"],
        "owner_not_literal_numeric"
    );

    std::fs::write(
        base_workspace.path().join("schema.yaml"),
        "- { name: n, type: numeric }\n",
    )
    .expect("write external schema");
    std::fs::write(
        base_workspace.path().join("pipeline.yaml"),
        "pipeline:\n  name: external_owner\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      path: input.csv\n      schema: schema.yaml\n",
    )
    .expect("write external-schema pipeline");
    std::fs::write(base_workspace.path().join("input.csv"), "n\n1\n").unwrap();
    let external = guess(base_workspace.path(), &["--write"]);
    assert_eq!(external.status.code(), Some(3));
    assert_eq!(
        parse_report(&external)["write"]["reason"],
        "owner_not_inline"
    );

    let overlay_workspace = workspace();
    let overlay_pipeline = overlay_workspace.path().join("pipeline.yaml");
    let overlay_before = std::fs::read(&overlay_pipeline).unwrap();
    let overlay = guess(
        overlay_workspace.path(),
        &[
            "--write",
            "--channel",
            "json_preview",
            "--field",
            "json_orders.ratio",
        ],
    );
    assert_eq!(overlay.status.code(), Some(3));
    assert_eq!(
        parse_report(&overlay)["write"]["reason"],
        "effective_config_has_overlay"
    );
    assert_eq!(std::fs::read(overlay_pipeline).unwrap(), overlay_before);
}

#[cfg(unix)]
#[test]
fn write_symlink_config_is_patch_only() {
    use std::os::unix::fs::symlink;

    let workspace = workspace();
    write_flat_json_pipeline(workspace.path(), "numeric", None, "[{\"n\":1}]");
    symlink("pipeline.yaml", workspace.path().join("linked.yaml")).expect("create config symlink");
    let before = std::fs::read(workspace.path().join("pipeline.yaml")).unwrap();
    let output = guess_config(workspace.path(), "linked.yaml", &["--write"]);
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(parse_report(&output)["write"]["reason"], "config_symlink");
    assert_eq!(
        std::fs::read(workspace.path().join("pipeline.yaml")).unwrap(),
        before
    );
}

#[cfg(unix)]
#[test]
fn write_symlink_config_parent_is_patch_only() {
    use std::os::unix::fs::symlink;

    let workspace = tempfile::tempdir().expect("temporary guess workspace");
    let actual = workspace.path().join("actual");
    std::fs::create_dir(&actual).expect("create actual config directory");
    write_flat_json_pipeline(&actual, "numeric", None, "[{\"n\":1}]");
    symlink("actual", workspace.path().join("linked")).expect("create config parent symlink");
    let pipeline = actual.join("pipeline.yaml");
    let before = std::fs::read(&pipeline).unwrap();
    let output = guess_config(workspace.path(), "linked/pipeline.yaml", &["--write"]);
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(parse_report(&output)["write"]["reason"], "config_symlink");
    assert_eq!(std::fs::read(pipeline).unwrap(), before);
}

#[cfg(unix)]
#[test]
fn write_symlink_input_is_patch_only() {
    use std::os::unix::fs::symlink;

    let workspace = workspace();
    write_flat_json_pipeline(workspace.path(), "numeric", None, "[{\"n\":1}]");
    let pipeline = workspace.path().join("pipeline.yaml");
    let before = std::fs::read(&pipeline).unwrap();
    std::fs::rename(
        workspace.path().join("input.json"),
        workspace.path().join("actual-input.json"),
    )
    .expect("move input behind symlink");
    symlink("actual-input.json", workspace.path().join("input.json"))
        .expect("create input symlink");
    let output = guess(workspace.path(), &["--write"]);
    assert_eq!(output.status.code(), Some(3));
    assert_eq!(parse_report(&output)["write"]["reason"], "input_symlink");
    assert_eq!(std::fs::read(pipeline).unwrap(), before);
}

#[test]
fn write_two_cooperating_writers_serialize_on_the_stable_lock() {
    let workspace = workspace();
    write_flat_json_pipeline(workspace.path(), "numeric", None, "[{\"n\":1}]");
    let pipeline = workspace.path().join("pipeline.yaml");
    let barrier = tempfile::tempdir().expect("write barrier directory");
    let first = spawn_at_write_barrier(workspace.path(), barrier.path());
    wait_for_barrier(barrier.path());
    let second = guess(workspace.path(), &["--write"]);
    assert_eq!(second.status.code(), Some(3));
    assert_eq!(
        parse_report(&second)["write"]["reason"],
        "config_lock_contended"
    );
    std::fs::write(barrier.path().join("continue"), b"continue")
        .expect("release first cooperating writer");
    let first = first.wait_with_output().expect("collect first writer");
    assert_eq!(first.status.code(), Some(0));
    let after = std::fs::read_to_string(&pipeline).unwrap();
    assert!(after.contains("type: int"));
    assert!(
        workspace
            .path()
            .join("pipeline.yaml.clinker-guess.lock")
            .is_file()
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mode = std::fs::metadata(workspace.path().join("pipeline.yaml.clinker-guess.lock"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0, "lock sidecar must be owner-only");
    }
}

#[cfg(unix)]
#[test]
fn write_rejects_symlinked_or_broadly_accessible_lock_sidecar() {
    use std::os::unix::fs::{PermissionsExt, symlink};

    for unsafe_lock in ["symlink", "permissions"] {
        let workspace = workspace();
        write_flat_json_pipeline(workspace.path(), "numeric", None, "[{\"n\":1}]");
        let pipeline = workspace.path().join("pipeline.yaml");
        let before = std::fs::read(&pipeline).unwrap();
        let lock_path = workspace.path().join("pipeline.yaml.clinker-guess.lock");
        if unsafe_lock == "symlink" {
            std::fs::write(workspace.path().join("lock-target"), b"target")
                .expect("write lock symlink target");
            symlink("lock-target", &lock_path).expect("create lock symlink");
        } else {
            std::fs::write(&lock_path, b"lock").expect("write broad lock file");
            std::fs::set_permissions(&lock_path, std::fs::Permissions::from_mode(0o644))
                .expect("broaden lock permissions");
        }
        let output = guess(workspace.path(), &["--write"]);
        assert_eq!(output.status.code(), Some(3), "{unsafe_lock}");
        assert_eq!(
            parse_report(&output)["write"]["reason"],
            format!("config_lock_{unsafe_lock}")
        );
        assert_eq!(std::fs::read(&pipeline).unwrap(), before);
    }
}

#[test]
fn write_concurrent_config_and_input_drift_are_detected_before_publication() {
    for changed in ["config", "input"] {
        let workspace = workspace();
        write_flat_json_pipeline(workspace.path(), "numeric", None, "[{\"n\":1}]");
        let barrier = tempfile::tempdir().expect("write barrier directory");
        let child = spawn_at_write_barrier(workspace.path(), barrier.path());
        wait_for_barrier(barrier.path());
        if changed == "config" {
            let pipeline = workspace.path().join("pipeline.yaml");
            let mut raw = std::fs::read_to_string(&pipeline).unwrap();
            raw.push_str("# concurrent writer\n");
            std::fs::write(&pipeline, raw).expect("mutate config at barrier");
        } else {
            std::fs::write(workspace.path().join("input.json"), "[{\"n\":2}]")
                .expect("mutate input at barrier");
        }
        std::fs::write(barrier.path().join("continue"), b"continue")
            .expect("release write barrier");
        let output = child.wait_with_output().expect("collect barred guess");
        assert_eq!(output.status.code(), Some(3), "{changed} drift");
        let report = parse_report(&output);
        assert_eq!(report["write"]["status"], "not_written");
        let reason = report["write"]["reason"].as_str().expect("drift reason");
        assert!(
            reason.starts_with(changed),
            "unexpected {changed} drift reason {reason:?}"
        );
        let pipeline = std::fs::read_to_string(workspace.path().join("pipeline.yaml")).unwrap();
        assert!(
            pipeline.contains("type: numeric"),
            "{changed} drift edited config"
        );
    }
}

#[test]
fn write_failure_and_interruption_before_rename_preserve_original() {
    for (environment, expected_exit) in [
        ("CLINKER_TEST_GUESS_WRITE_FAIL_BEFORE_RENAME", 4),
        ("CLINKER_TEST_GUESS_WRITE_INTERRUPT_BEFORE_RENAME", 130),
    ] {
        let workspace = workspace();
        write_flat_json_pipeline(workspace.path(), "numeric", None, "[{\"n\":1}]");
        let pipeline = workspace.path().join("pipeline.yaml");
        let before = std::fs::read(&pipeline).unwrap();
        let output = guess_with_env(workspace.path(), &["--write"], &[(environment, "1")]);
        assert_eq!(output.status.code(), Some(expected_exit), "{environment}");
        assert!(
            output.stdout.is_empty(),
            "{environment} emitted partial report"
        );
        assert_eq!(std::fs::read(&pipeline).unwrap(), before, "{environment}");
        let siblings = std::fs::read_dir(workspace.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(
            siblings.iter().all(|name| !name.starts_with(".tmp")),
            "{environment} left a sibling temporary: {siblings:?}"
        );
    }
}

#[test]
fn modes_configuration_io_and_interruption_exits_are_distinct() {
    let workspace = workspace();
    let signal_install = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(workspace.path())
        .env("CLINKER_TEST_SIGNAL_HANDLER_FAILURE", "1")
        .args(["guess", "pipeline.yaml"])
        .output()
        .expect("spawn signal-install failure");
    assert_eq!(signal_install.status.code(), Some(4));
    assert!(signal_install.stdout.is_empty());

    let selection = guess(workspace.path(), &["--field", "missing.n"]);
    assert_eq!(selection.status.code(), Some(1));

    let disappeared = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(workspace.path())
        .env("CLINKER_TEST_GUESS_FAIL_OPEN_AFTER_DISCOVERY", "1")
        .args(["guess", "pipeline.yaml", "--field", "csv_orders.amount"])
        .output()
        .expect("spawn post-discovery open failure");
    assert_eq!(disappeared.status.code(), Some(4));

    let grew = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(workspace.path())
        .env("CLINKER_TEST_GUESS_GROW_AFTER_DISCOVERY", "1")
        .args(["guess", "pipeline.yaml", "--field", "csv_orders.amount"])
        .output()
        .expect("spawn post-discovery input growth");
    assert_eq!(grew.status.code(), Some(4));
    assert!(grew.stdout.is_empty());
    assert!(
        String::from_utf8_lossy(&grew.stderr).contains("source length changed after admission"),
        "growth must fail at the frozen reader boundary: {}",
        String::from_utf8_lossy(&grew.stderr)
    );

    std::fs::remove_file(workspace.path().join("input.csv")).expect("remove selected input");
    let io = guess(workspace.path(), &["--field", "csv_orders.amount"]);
    assert_eq!(io.status.code(), Some(4));

    write_flat_json_pipeline(workspace.path(), "numeric", None, "[{\"n\":1},{\"n\":2}]");
    let interrupted = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(workspace.path())
        .env("CLINKER_TEST_GUESS_INTERRUPT_AFTER_RECORDS", "1")
        .args(["guess", "pipeline.yaml", "--check"])
        .output()
        .expect("spawn interrupted guess");
    assert_eq!(interrupted.status.code(), Some(130));
    assert!(interrupted.stdout.is_empty());
}

#[test]
fn evidence_absence_default_and_all_no_value_follow_authored_policy() {
    let workspace = workspace();
    write_flat_json_pipeline(
        workspace.path(),
        "{ nullable: numeric }",
        Some("7"),
        "[{\"n\":null},{}]",
    );
    let with_default = parse_success(&guess(workspace.path(), &[]));
    let owner = &with_default["fields"][0]["owners"][0];
    assert_eq!(owner["proposed_type"], "int");
    assert_eq!(owner["absence"]["accepted"], 2);
    assert_eq!(owner["absence"]["forbidden"], 0);
    assert_eq!(owner["absence"]["default_votes"], 1);
    assert_eq!(owner["evidence"][0]["origin"], "default");
    assert_eq!(owner["evidence"][0]["boundary"], "schema_coerce");

    write_flat_json_pipeline(
        workspace.path(),
        "{ nullable: numeric }",
        None,
        "[{\"n\":null},{}]",
    );
    let no_values = parse_success(&guess(workspace.path(), &[]));
    let owner = &no_values["fields"][0]["owners"][0];
    assert_eq!(owner["proposed_type"], serde_json::Value::Null);
    assert_eq!(owner["unresolved_reasons"][0], "no_value_evidence");
    let check = guess(workspace.path(), &["--check"]);
    assert_eq!(check.status.code(), Some(3));
}

#[test]
fn evidence_default_applies_to_missing_but_not_explicit_null() {
    let workspace = workspace();
    write_flat_json_pipeline(workspace.path(), "numeric", Some("7"), "[{}]");
    let missing = parse_success(&guess(workspace.path(), &[]));
    let owner = &missing["fields"][0]["owners"][0];
    assert_eq!(owner["proposed_type"], "int");
    assert_eq!(owner["absence"]["accepted"], 1);
    assert_eq!(owner["absence"]["forbidden"], 0);
    assert_eq!(owner["evidence"][1]["origin"], "missing");

    write_flat_json_pipeline(workspace.path(), "numeric", Some("7"), "[{\"n\":null}]");
    let explicit_null = parse_success(&guess(workspace.path(), &[]));
    let owner = &explicit_null["fields"][0]["owners"][0];
    assert_eq!(owner["proposed_type"], serde_json::Value::Null);
    assert_eq!(owner["absence"]["accepted"], 0);
    assert_eq!(owner["absence"]["forbidden"], 1);
    assert_eq!(owner["unresolved_reasons"][0], "forbidden_absence");
}

#[test]
fn evidence_arbitrary_precision_default_uses_the_schema_parser() {
    let workspace = workspace();
    write_flat_json_pipeline(
        workspace.path(),
        "{ nullable: numeric }",
        Some("9223372036854775808"),
        "[{}]",
    );

    let report = parse_success(&guess(workspace.path(), &[]));
    let owner = &report["fields"][0]["owners"][0];
    assert_eq!(owner["proposed_type"], serde_json::Value::Null);
    assert_eq!(owner["evidence"][0]["origin"], "default");
    assert_eq!(owner["evidence"][0]["boundary"], "schema_coerce");
    assert_eq!(owner["evidence"][0]["lexeme"], "9223372036854775808");
    assert_eq!(owner["unresolved_reasons"][0], "unsafe_integer_widening");
}

#[test]
fn evidence_forbidden_absence_is_unresolved_not_an_io_failure() {
    let workspace = workspace();
    write_flat_json_pipeline(workspace.path(), "numeric", None, "[{\"n\":null}]");

    let preview = guess(workspace.path(), &[]);
    assert_eq!(preview.status.code(), Some(0));
    let preview = parse_report(&preview);
    let owner = &preview["fields"][0]["owners"][0];
    assert_eq!(owner["absence"]["forbidden"], 1);
    assert_eq!(owner["unresolved_reasons"][0], "forbidden_absence");

    let check = guess(workspace.path(), &["--check"]);
    assert_eq!(check.status.code(), Some(3));
}

#[test]
fn evidence_mixed_votes_use_exact_integer_widening_not_confidence() {
    let workspace = workspace();
    std::fs::write(
        workspace.path().join("pipeline.yaml"),
        "pipeline:\n  name: exact_votes\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      path: input.csv\n      schema:\n        - { name: n, type: numeric }\n",
    )
    .expect("write exact vote pipeline");
    std::fs::write(
        workspace.path().join("input.csv"),
        "n\n9007199254740993\n1.5\n",
    )
    .expect("write exact vote input");

    let report = parse_success(&guess(workspace.path(), &[]));
    let owner = &report["fields"][0]["owners"][0];
    assert_eq!(owner["votes"]["int"], 1);
    assert_eq!(owner["votes"]["float"], 1);
    assert_eq!(owner["proposed_type"], serde_json::Value::Null);
    assert_eq!(owner["unresolved_reasons"][0], "unsafe_integer_widening");
    assert!(report.get("confidence").is_none());
}

#[test]
fn evidence_reader_error_leaves_later_numeric_fields_fail_closed() {
    let workspace = workspace();
    std::fs::write(
        workspace.path().join("pipeline.yaml"),
        "pipeline:\n  name: fail_closed_row\nnodes:\n  - type: source\n    name: values\n    config:\n      name: values\n      type: csv\n      path: input.csv\n      schema:\n        - { name: a, type: numeric }\n        - { name: b, type: numeric }\n",
    )
    .expect("write fail-closed pipeline");
    std::fs::write(workspace.path().join("input.csv"), "a,b\n1,1\nbad,1.5\n")
        .expect("write fail-closed input");

    let output = guess(workspace.path(), &["--check"]);
    assert_eq!(output.status.code(), Some(3));
    let report = parse_report(&output);
    assert_eq!(report["fields"][0]["owners"][0]["votes"]["unresolved"], 1);
    let later = &report["fields"][1]["owners"][0];
    assert_eq!(later["votes"]["int"], 1);
    assert_eq!(later["proposed_type"], serde_json::Value::Null);
    assert_eq!(later["unresolved_reasons"][0], "missing_parser_observation");
    assert_eq!(report["patch"], "edits:\n");
}

#[test]
fn telemetry_lifecycle_covers_preview_check_write_and_terminal_exits() {
    let preview = workspace();
    let preview_capture = preview.path().join("preview-telemetry.ndjson");
    let preview_output = guess_with_telemetry(preview.path(), &[], &preview_capture, &[]);
    assert_eq!(preview_output.status.code(), Some(0));
    assert_guess_telemetry(&preview_capture, "clinker.guess.completed", 1);

    let preview_unresolved = workspace();
    std::fs::write(
        preview_unresolved.path().join("input.csv"),
        "kind,order_id,amount\nD,c-1,9223372036854775808\nA,c-2,20.5\n",
    )
    .expect("write unresolved preview input");
    let preview_unresolved_capture = preview_unresolved.path().join("unresolved-preview.ndjson");
    let preview_unresolved_output = guess_with_telemetry(
        preview_unresolved.path(),
        &["--field", "csv_orders.amount"],
        &preview_unresolved_capture,
        &[],
    );
    assert_eq!(preview_unresolved_output.status.code(), Some(0));
    assert_guess_telemetry(&preview_unresolved_capture, "clinker.guess.unresolved", 2);

    let check = workspace();
    std::fs::write(
        check.path().join("input.csv"),
        "kind,order_id,amount\nD,c-1,9223372036854775808\nA,c-2,20.5\n",
    )
    .expect("write unresolved check input");
    let check_capture = check.path().join("check-telemetry.ndjson");
    let check_output = guess_with_telemetry(
        check.path(),
        &["--check", "--field", "csv_orders.amount"],
        &check_capture,
        &[],
    );
    assert_eq!(check_output.status.code(), Some(3));
    assert_guess_telemetry(&check_capture, "clinker.guess.unresolved", 2);

    let write = workspace();
    write_flat_json_pipeline(write.path(), "numeric", None, "[{\"n\":1}]");
    let write_capture = write.path().join("write-telemetry.ndjson");
    let write_output = guess_with_telemetry(write.path(), &["--write"], &write_capture, &[]);
    assert_eq!(write_output.status.code(), Some(0));
    assert_guess_telemetry(&write_capture, "clinker.guess.completed", 1);

    let configuration = workspace();
    let configuration_capture = configuration.path().join("configuration-telemetry.ndjson");
    let configuration_output = guess_with_telemetry(
        configuration.path(),
        &["--field", "missing.n"],
        &configuration_capture,
        &[],
    );
    assert_eq!(configuration_output.status.code(), Some(1));
    assert_guess_telemetry(&configuration_capture, "clinker.guess.failed", 2);

    let infrastructure = workspace();
    let infrastructure_capture = infrastructure
        .path()
        .join("infrastructure-telemetry.ndjson");
    let infrastructure_output = guess_with_telemetry(
        infrastructure.path(),
        &["--field", "csv_orders.amount"],
        &infrastructure_capture,
        &[("CLINKER_TEST_GUESS_FAIL_OPEN_AFTER_DISCOVERY", "1")],
    );
    assert_eq!(infrastructure_output.status.code(), Some(4));
    assert_guess_telemetry(&infrastructure_capture, "clinker.guess.failed", 2);

    let interrupted = workspace();
    write_flat_json_pipeline(interrupted.path(), "numeric", None, "[{\"n\":1},{\"n\":2}]");
    let interrupted_capture = interrupted.path().join("interrupted-telemetry.ndjson");
    let interrupted_output = guess_with_telemetry(
        interrupted.path(),
        &["--check"],
        &interrupted_capture,
        &[("CLINKER_TEST_GUESS_INTERRUPT_AFTER_RECORDS", "1")],
    );
    assert_eq!(interrupted_output.status.code(), Some(130));
    assert_guess_telemetry(&interrupted_capture, "clinker.guess.interrupted", 2);
}

#[test]
fn telemetry_privacy_excludes_authored_names_paths_selectors_records_and_errors() {
    const SECRET: &str = "secret-shaped-guess-value-7f31";

    let workspace = workspace();
    std::fs::write(
        workspace.path().join("pipeline.yaml"),
        format!(
            "pipeline:\n  name: {SECRET}\nnodes:\n  - type: source\n    name: {SECRET}\n    config:\n      name: {SECRET}\n      type: csv\n      path: {SECRET}.csv\n      schema:\n        - {{ name: n, type: numeric }}\n        - {{ name: payload, type: string }}\n"
        ),
    )
    .expect("write privacy pipeline");
    std::fs::write(
        workspace.path().join(format!("{SECRET}.csv")),
        format!("n,payload\n1,{SECRET}\n"),
    )
    .expect("write privacy input");
    let capture = workspace.path().join("privacy-telemetry.ndjson");
    let output = guess_with_telemetry(
        workspace.path(),
        &["--field", &format!("{SECRET}.n")],
        &capture,
        &[],
    );
    assert_eq!(output.status.code(), Some(0));
    let captured = std::fs::read_to_string(&capture).expect("privacy telemetry capture");
    assert!(
        !captured.contains(SECRET),
        "authored data reached telemetry"
    );
    assert!(
        !captured.contains("resourceLogs"),
        "Guess emits no log records"
    );
    assert!(captured.contains("clinker.guess.started"));
    assert!(captured.contains("clinker.guess.completed"));
    assert!(captured.contains("\"stringValue\":\"clinker.guess\""));
}

#[test]
fn telemetry_admission_loss_does_not_change_write_truth_or_bytes() {
    let baseline = workspace();
    write_flat_json_pipeline(baseline.path(), "numeric", None, "[{\"n\":1}]");
    let baseline_capture = baseline.path().join("baseline-telemetry.ndjson");
    let baseline_output =
        guess_with_telemetry(baseline.path(), &["--write"], &baseline_capture, &[]);

    let rejected = workspace();
    write_flat_json_pipeline(rejected.path(), "numeric", None, "[{\"n\":1}]");
    let rejected_capture = rejected.path().join("rejected-telemetry.ndjson");
    enable_guess_telemetry(rejected.path());
    let policy_path = rejected.path().join("clinker.toml");
    let policy = std::fs::read_to_string(&policy_path)
        .expect("read telemetry policy")
        .replace("max_batch_bytes = \"8KB\"", "max_batch_bytes = \"1B\"");
    std::fs::write(&policy_path, policy).expect("force the fixed Guess span over its arena slot");
    let rejected_capture_text = rejected_capture.to_str().expect("UTF-8 capture path");
    let rejected_output = guess_with_env(
        rejected.path(),
        &["--write"],
        &[
            ("CLINKER_TEST_OTLP_OUTCOME", "success"),
            ("CLINKER_TEST_OTLP_CAPTURE", rejected_capture_text),
        ],
    );

    assert_eq!(baseline_output.status.code(), Some(0));
    assert_eq!(rejected_output.status.code(), Some(0));
    assert_eq!(baseline_output.stdout, rejected_output.stdout);
    assert_eq!(
        std::fs::read(baseline.path().join("pipeline.yaml")).expect("baseline config"),
        std::fs::read(rejected.path().join("pipeline.yaml")).expect("rejected config")
    );
    assert_guess_telemetry(&baseline_capture, "clinker.guess.completed", 1);
    let rejected_capture = telemetry_capture(&rejected_capture);
    let metrics = captured_metric_counts(&rejected_capture);
    assert_eq!(
        metrics.len(),
        2,
        "fixed metrics survive span admission loss"
    );
    assert_eq!(metrics.get("clinker.guess.started"), Some(&1));
    assert_eq!(metrics.get("clinker.guess.completed"), Some(&1));
    assert!(
        captured_spans(&rejected_capture).is_empty(),
        "the one-byte arena slot must drop the complete Guess span"
    );
}
