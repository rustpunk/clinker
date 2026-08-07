//! End-to-end coverage for `clinker run --lineage`: the built binary must emit a
//! well-formed OpenLineage START/COMPLETE NDJSON pair carrying DIRECT and INDIRECT
//! column lineage. The lineage *computation* is unit-tested in `clinker-lineage`;
//! these tests pin the CLI wiring and the on-the-wire document shape.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// `examples/` lives at the workspace root, two levels above this crate.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("workspace root two levels above the crate manifest")
        .to_path_buf()
}

/// Run `clinker run <pipeline> --lineage -` and return the two parsed NDJSON
/// events (START, COMPLETE).
fn run_lineage(pipeline: &Path) -> (serde_json::Value, serde_json::Value) {
    let output = Command::new(clinker_bin())
        .arg("run")
        .arg(pipeline)
        .arg("--lineage")
        .arg("-")
        .output()
        .expect("spawn clinker");
    assert!(
        output.status.success(),
        "clinker run --lineage failed:\nstderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    let lines: Vec<&str> = stdout.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        lines.len(),
        2,
        "expected exactly two NDJSON lines (START, COMPLETE), got:\n{stdout}"
    );
    let start: serde_json::Value = serde_json::from_str(lines[0]).expect("START line is JSON");
    let complete: serde_json::Value =
        serde_json::from_str(lines[1]).expect("COMPLETE line is JSON");
    (start, complete)
}

#[test]
fn lineage_emits_start_complete_pair_with_column_lineage() {
    let (_workspace, pipeline) = audit_join_workspace();
    let (start, complete) = run_lineage(&pipeline);

    // --- Event envelope: a START then a COMPLETE sharing one runId. ---
    assert_eq!(start["eventType"], "START");
    assert_eq!(complete["eventType"], "COMPLETE");
    assert_eq!(
        start["schemaURL"],
        "https://openlineage.io/spec/2-0-2/OpenLineage.json"
    );
    let run_id = start["run"]["runId"].as_str().expect("runId string");
    assert_eq!(
        complete["run"]["runId"].as_str(),
        Some(run_id),
        "START and COMPLETE must share one runId"
    );
    uuid::Uuid::parse_str(run_id).expect("runId is a UUID");
    for event in [&start, &complete] {
        let event_time = event["eventTime"].as_str().expect("eventTime string");
        assert!(
            event_time.ends_with('Z'),
            "eventTime should be RFC-3339 UTC: {event_time}"
        );
    }

    // --- Job identity + pipeline-hash job facet (not encoded in the name). ---
    assert_eq!(complete["job"]["namespace"], "clinker");
    assert_eq!(complete["job"]["name"], "audit_join");
    let source_hash = complete["job"]["facets"]["clinker_pipeline"]["sourceHash"]
        .as_str()
        .expect("clinker_pipeline job facet sourceHash");
    assert_eq!(source_hash.len(), 64, "sourceHash is full 64-char hex");
    assert!(source_hash.chars().all(|c| c.is_ascii_hexdigit()));

    // --- START announces the run with no datasets. ---
    assert!(start.get("inputs").is_none(), "START carries no inputs");
    assert!(start.get("outputs").is_none(), "START carries no outputs");

    // --- COMPLETE carries the input datasets (facet-less) ... ---
    let inputs = complete["inputs"].as_array().expect("inputs array");
    assert_eq!(inputs.len(), 2, "two source datasets joined");
    let input_names: Vec<&str> = inputs
        .iter()
        .map(|d| d["name"].as_str().expect("input dataset name"))
        .collect();
    assert!(
        input_names
            .iter()
            .any(|n| n.ends_with("data/audit_orders.csv"))
    );
    assert!(
        input_names
            .iter()
            .any(|n| n.ends_with("data/audit_events.csv"))
    );
    assert!(
        inputs.iter().all(|d| d.get("facets").is_none()),
        "input datasets carry no facets"
    );

    // --- ... and the output dataset with its columnLineage facet. ---
    let outputs = complete["outputs"].as_array().expect("outputs array");
    assert_eq!(outputs.len(), 1);
    let facet = &outputs[0]["facets"]["columnLineage"];
    assert_eq!(
        facet["_schemaURL"],
        "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json"
    );

    // DIRECT: each output column resolves to its own source column.
    let amount = &facet["fields"]["amount"]["inputFields"][0];
    assert!(
        amount["name"]
            .as_str()
            .unwrap()
            .ends_with("data/audit_orders.csv")
    );
    assert_eq!(amount["field"], "amount");
    assert_eq!(amount["transformations"][0]["type"], "DIRECT");
    assert_eq!(amount["transformations"][0]["subtype"], "IDENTITY");
    let actor = &facet["fields"]["actor"]["inputFields"][0];
    assert!(
        actor["name"]
            .as_str()
            .unwrap()
            .ends_with("data/audit_events.csv")
    );

    // INDIRECT: the join key influences the dataset as a whole.
    let influence = facet["dataset"].as_array().expect("INDIRECT dataset array");
    assert!(
        influence.iter().any(|f| {
            f["field"] == "order_id"
                && f["transformations"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|t| t["type"] == "INDIRECT" && t["subtype"] == "JOIN")
        }),
        "expected a JOIN influence on order_id, got: {influence:#?}"
    );
}

#[test]
fn lineage_writes_to_a_file_path() {
    let (workspace, pipeline) = audit_join_workspace();
    let out = workspace.path().join("lineage.ndjson");

    let status = Command::new(clinker_bin())
        .arg("run")
        .arg(&pipeline)
        .arg("--lineage")
        .arg(&out)
        .status()
        .expect("spawn clinker");
    assert!(status.success(), "clinker run --lineage <file> failed");

    let contents = std::fs::read_to_string(&out).expect("read lineage file");
    let lines: Vec<&str> = contents.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 2, "two NDJSON lines written to file");
    let start: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    let complete: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
    assert_eq!(start["eventType"], "START");
    assert_eq!(complete["eventType"], "COMPLETE");
}

#[test]
fn lineage_conflicts_with_explain() {
    // --lineage is a plan-only export; combining it with --explain would
    // silently drop one, so clap must reject the combination.
    let pipeline = repo_root().join("examples/pipelines/audit_join.yaml");
    let output = Command::new(clinker_bin())
        .args(["run"])
        .arg(&pipeline)
        .args(["--explain", "text", "--lineage", "-"])
        .output()
        .expect("spawn clinker");
    assert!(
        !output.status.success(),
        "--explain + --lineage must be rejected, not silently accepted"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot be used with"),
        "expected a clap conflict error, got:\n{stderr}"
    );
}

/// Write a one-source → one-output pipeline (explicit schema, so compile needs
/// no data file) into `dir` and return its path.
fn write_pipeline(dir: &Path, output_path: &str) -> PathBuf {
    let yaml = format!(
        "pipeline:\n  name: lineage_fixture\nnodes:\n  - type: source\n    name: src\n    \
         config:\n      name: src\n      type: csv\n      path: ./data/in.csv\n      \
         options: {{ has_header: true }}\n      schema:\n        - {{ name: id, type: string }}\n  \
         - type: output\n    name: out\n    input: src\n    config:\n      name: out\n      \
         type: csv\n      path: \"{output_path}\"\n"
    );
    let path = dir.join("pipeline.yaml");
    std::fs::write(&path, yaml).expect("write pipeline");
    path
}

fn write_lineage_policy(dir: &Path, identity_mode: &str, datasets: &str) {
    let policy = format!(
        r#"[observability]

[observability.otlp]
endpoint = "https://collector.example.com"

[observability.otlp.auth]
mode = "none"

[observability.lineage]
identity_mode = "{identity_mode}"

{datasets}"#
    );
    std::fs::write(dir.join("clinker.toml"), policy).expect("write lineage identity policy");
}

fn write_local_lineage_policy(dir: &Path) {
    write_lineage_policy(dir, "local_diagnostic_paths", "");
}

fn audit_join_workspace() -> (tempfile::TempDir, PathBuf) {
    let workspace = tempfile::tempdir().expect("audit lineage workspace");
    let pipeline = workspace.path().join("audit_join.yaml");
    std::fs::copy(
        repo_root().join("examples/pipelines/audit_join.yaml"),
        &pipeline,
    )
    .expect("copy audit lineage fixture");
    write_local_lineage_policy(workspace.path());
    (workspace, pipeline)
}

fn external_lineage_policy(dir: &Path, include_output: bool) {
    let output = if include_output {
        r#"
[[observability.lineage.dataset]]
node = "out"
catalog_namespace = "analytics"
catalog_name = "lineage_fixture"
"#
    } else {
        ""
    };
    write_lineage_policy(
        dir,
        "external",
        &format!(
            r#"[[observability.lineage.dataset]]
node = "src"
canonical_datasource = "s3://warehouse/lineage_fixture"
{output}"#
        ),
    );
}

fn lineage_complete(path: &Path) -> serde_json::Value {
    let contents = std::fs::read_to_string(path).expect("read lineage event file");
    let events: Vec<serde_json::Value> = contents
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("lineage event is JSON"))
        .collect();
    assert_eq!(events.len(), 2, "expected START and COMPLETE: {events:#?}");
    assert_eq!(events[1]["eventType"], "COMPLETE");
    events[1].clone()
}

fn machine_terminal(output: &std::process::Output) -> serde_json::Value {
    serde_json::from_slice(
        output
            .stdout
            .split(|byte| *byte == b'\n')
            .filter(|line| !line.is_empty())
            .next_back()
            .expect("machine terminal event"),
    )
    .expect("machine terminal is JSON")
}

fn assert_external_policy_rejected(datasets: &str, forbidden_diagnostic_text: Option<&str>) {
    let workspace = tempfile::tempdir().expect("rejected policy workspace");
    write_pipeline(workspace.path(), "./must-not-exist.csv");
    write_lineage_policy(workspace.path(), "external", datasets);
    let output = Command::new(clinker_bin())
        .current_dir(workspace.path())
        .args([
            "run",
            "pipeline.yaml",
            "--lineage",
            "must-not-exist.ndjson",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "rejected-identity-policy",
        ])
        .output()
        .expect("run rejected external policy");
    assert_eq!(output.status.code(), Some(1));
    let terminal = machine_terminal(&output);
    assert_eq!(terminal["event"], "failed");
    assert_eq!(
        terminal["failure"]["code"],
        "observability.configuration.invalid"
    );
    assert!(!workspace.path().join("must-not-exist.ndjson").exists());
    assert!(!workspace.path().join("must-not-exist.csv").exists());
    if let Some(forbidden) = forbidden_diagnostic_text {
        let rendered = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            !rendered.contains(forbidden),
            "rejected identity value leaked into the diagnostic: {rendered}"
        );
    }
}

#[test]
fn identity_preflight_and_local_compatibility() {
    let external = tempfile::tempdir().expect("external workspace");
    write_pipeline(external.path(), "./out.csv");
    external_lineage_policy(external.path(), true);
    let event_path = external.path().join("external.ndjson");
    let output = Command::new(clinker_bin())
        .current_dir(external.path())
        .args(["run", "pipeline.yaml", "--lineage", "external.ndjson"])
        .output()
        .expect("run external lineage export");
    assert!(
        output.status.success(),
        "external identity export failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let complete = lineage_complete(&event_path);
    assert_eq!(complete["inputs"][0]["namespace"], "s3://warehouse");
    assert_eq!(complete["inputs"][0]["name"], "lineage_fixture");
    assert_eq!(complete["outputs"][0]["namespace"], "analytics");
    assert_eq!(complete["outputs"][0]["name"], "lineage_fixture");
    assert!(
        !complete
            .to_string()
            .contains(external.path().to_string_lossy().as_ref()),
        "external lineage must not disclose the physical workspace"
    );

    let relocated = tempfile::tempdir().expect("relocated workspace");
    write_pipeline(relocated.path(), "./different/out.csv");
    external_lineage_policy(relocated.path(), true);
    let relocated_output = Command::new(clinker_bin())
        .current_dir(relocated.path())
        .args(["run", "pipeline.yaml", "--lineage", "relocated.ndjson"])
        .output()
        .expect("run relocated lineage export");
    assert!(
        relocated_output.status.success(),
        "relocated export failed:\n{}",
        String::from_utf8_lossy(&relocated_output.stderr)
    );
    let relocated_complete = lineage_complete(&relocated.path().join("relocated.ndjson"));
    assert_eq!(
        complete["inputs"], relocated_complete["inputs"],
        "source identity is independent of physical placement"
    );
    assert_eq!(
        complete["outputs"], relocated_complete["outputs"],
        "output identity is independent of physical placement"
    );

    let incomplete = tempfile::tempdir().expect("incomplete external workspace");
    write_pipeline(incomplete.path(), "./must-not-exist.csv");
    external_lineage_policy(incomplete.path(), false);
    let rejected = Command::new(clinker_bin())
        .current_dir(incomplete.path())
        .args([
            "run",
            "pipeline.yaml",
            "--lineage",
            "must-not-exist.ndjson",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "identity-preflight",
        ])
        .output()
        .expect("run incomplete identity preflight");
    assert_eq!(rejected.status.code(), Some(1));
    let terminal = machine_terminal(&rejected);
    assert_eq!(terminal["event"], "failed");
    assert_eq!(
        terminal["failure"]["code"],
        "observability.configuration.invalid"
    );
    assert!(
        !incomplete.path().join("must-not-exist.ndjson").exists(),
        "identity preflight must run before opening the lineage sink"
    );
    assert!(
        !incomplete.path().join("must-not-exist.csv").exists(),
        "identity preflight must run before output effects"
    );

    assert_external_policy_rejected(
        r#"[[observability.lineage.dataset]]
node = "src"
catalog_namespace = "analytics"
"#,
        None,
    );
    assert_external_policy_rejected(
        r#"[[observability.lineage.dataset]]
node = "src"
canonical_datasource = "s3://warehouse/source-a"

[[observability.lineage.dataset]]
node = "src"
canonical_datasource = "s3://warehouse/source-b"
"#,
        None,
    );
    assert_external_policy_rejected(
        r#"[[observability.lineage.dataset]]
node = "src"
canonical_datasource = "s3://warehouse/source"
catalog_namespace = "analytics"
catalog_name = "source"
"#,
        None,
    );
    assert_external_policy_rejected(
        r#"[[observability.lineage.dataset]]
node = "src"
canonical_datasource = "private-worker-path-without-a-scheme"

[[observability.lineage.dataset]]
node = "out"
catalog_namespace = "analytics"
catalog_name = "lineage_fixture"
"#,
        Some("private-worker-path-without-a-scheme"),
    );

    let live = tempfile::tempdir().expect("live preflight workspace");
    write_runnable_pipeline(live.path(), None);
    external_lineage_policy(live.path(), false);
    let live_rejected = Command::new(clinker_bin())
        .current_dir(live.path())
        .args([
            "run",
            "pipeline.yaml",
            "--lineage-events",
            "events.ndjson",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "live-identity-preflight",
        ])
        .output()
        .expect("run live identity preflight");
    assert_eq!(live_rejected.status.code(), Some(1));
    assert_eq!(
        machine_terminal(&live_rejected)["failure"]["code"],
        "observability.configuration.invalid"
    );
    assert!(!live.path().join("events.ndjson").exists());
    assert!(!live.path().join("out.csv").exists());
    assert!(!live.path().join(".clinker-attempts").exists());

    let local = tempfile::tempdir().expect("local compatibility workspace");
    write_pipeline(local.path(), "./local.csv");
    write_lineage_policy(local.path(), "local_diagnostic_paths", "");
    let local_output = Command::new(clinker_bin())
        .current_dir(local.path())
        .args(["run", "pipeline.yaml", "--lineage", "local.ndjson"])
        .output()
        .expect("run local compatibility export");
    assert!(local_output.status.success());
    assert!(
        String::from_utf8_lossy(&local_output.stderr)
            .contains("local_diagnostic_paths (local diagnostic compatibility only)"),
        "local compatibility output must be visibly labeled"
    );
    let local_complete = lineage_complete(&local.path().join("local.ndjson"));
    assert!(
        local_complete["outputs"][0]["name"]
            .as_str()
            .expect("local dataset name")
            .ends_with("local.csv")
    );

    let absent = tempfile::tempdir().expect("absent policy workspace");
    write_pipeline(absent.path(), "./absent.csv");
    let absent_output = Command::new(clinker_bin())
        .current_dir(absent.path())
        .args(["run", "pipeline.yaml", "--lineage", "absent.ndjson"])
        .output()
        .expect("run absent identity policy");
    assert_eq!(absent_output.status.code(), Some(1));
    assert!(
        !absent.path().join("absent.ndjson").exists(),
        "path identity must require the exact explicit compatibility mode"
    );
}

fn output_dataset_name(pipeline: &Path, base_dir: Option<&Path>) -> String {
    let mut cmd = Command::new(clinker_bin());
    cmd.arg("run").arg(pipeline).args(["--lineage", "-"]);
    if let Some(base) = base_dir {
        cmd.arg("--base-dir").arg(base);
    }
    let output = cmd.output().expect("spawn clinker");
    assert!(
        output.status.success(),
        "lineage run failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    let complete: serde_json::Value = serde_json::from_str(stdout.lines().nth(1).unwrap()).unwrap();
    complete["outputs"][0]["name"].as_str().unwrap().to_string()
}

#[test]
fn templated_output_dataset_name_is_the_declared_template() {
    // A per-run {execution_id} token must NOT be baked into the dataset name,
    // or two runs of the same pipeline name different (un-joinable) datasets.
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = write_pipeline(dir.path(), "./output/report-{execution_id}.csv");
    write_local_lineage_policy(dir.path());
    let name1 = output_dataset_name(&pipeline, None);
    let name2 = output_dataset_name(&pipeline, None);
    assert!(
        name1.ends_with("report-{execution_id}.csv"),
        "dataset name must keep the literal template, got: {name1}"
    );
    assert_eq!(name1, name2, "templated output name must be reproducible");
}

#[test]
fn base_dir_anchors_dataset_names_at_the_pipeline_directory() {
    // With --base-dir an ancestor of the pipeline file, the pipeline_dir
    // component must survive in the resolved dataset name.
    let ws = tempfile::tempdir().expect("tempdir");
    let subdir = ws.path().join("subdir");
    std::fs::create_dir_all(&subdir).expect("mkdir subdir");
    let pipeline = write_pipeline(&subdir, "./out.csv");
    write_local_lineage_policy(ws.path());
    let name = output_dataset_name(&pipeline, Some(ws.path()));
    assert!(
        name.contains("/subdir/"),
        "dataset name must include the pipeline subdir, got: {name}"
    );
}

// --- Live run-lifecycle emission (`--lineage-events`) ---------------------------
//
// Unlike `--lineage` (a static, plan-only export that exits without reading data),
// `--lineage-events` rides an actual run: it emits a START before the run and a
// terminal COMPLETE / FAIL / ABORT after, carrying real timing and row counts. The
// event *assembly* is unit-tested in `clinker-lineage`; these tests pin the CLI
// wiring against a real execution.

/// Write a `source -> transform -> output` pipeline plus a three-row input CSV
/// into `dir`, and return the pipeline path. When `memory_limit` is `Some`, it is
/// injected as `pipeline.memory.limit`; a value whose binary-suffix scaling
/// overflows `u64` makes the executor reject the run at its startup gate — a
/// deterministic fatal error raised after the run has begun.
fn write_runnable_pipeline(dir: &Path, memory_limit: Option<&str>) -> PathBuf {
    write_local_lineage_policy(dir);
    let data_dir = dir.join("data");
    std::fs::create_dir_all(&data_dir).expect("mkdir data");
    std::fs::write(
        data_dir.join("in.csv"),
        "id,amount\nid0,10\nid1,20\nid2,30\n",
    )
    .expect("write input csv");

    let memory_block = match memory_limit {
        Some(limit) => format!("  memory:\n    limit: \"{limit}\"\n"),
        None => String::new(),
    };
    let yaml = format!(
        r#"pipeline:
  name: live_lineage_fixture
{memory_block}nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: ./data/in.csv
      options: {{ has_header: true }}
      schema:
        - {{ name: id, type: string }}
        - {{ name: amount, type: int }}
  - type: transform
    name: xf
    input: src
    config:
      cxl: "emit doubled = amount * 2"
  - type: output
    name: out
    input: xf
    config:
      name: out
      type: csv
      path: ./out.csv
      include_unmapped: true
error_handling:
  strategy: fail_fast
"#
    );
    let path = dir.join("pipeline.yaml");
    std::fs::write(&path, yaml).expect("write pipeline");
    path
}

/// Run `clinker run pipeline.yaml --lineage-events events.ndjson` with the working
/// directory set to the pipeline's tempdir — so the input, the primary output, and
/// the events file all resolve inside it and no artifact leaks into the crate tree.
/// Returns the parsed NDJSON events plus whether the process exited successfully.
fn run_lineage_events(pipeline: &Path) -> (bool, Vec<serde_json::Value>) {
    let dir = pipeline.parent().expect("pipeline has a parent dir");
    let status = Command::new(clinker_bin())
        .arg("run")
        .arg("pipeline.yaml")
        .args(["--lineage-events", "events.ndjson"])
        .current_dir(dir)
        .status()
        .expect("spawn clinker");
    let contents =
        std::fs::read_to_string(dir.join("events.ndjson")).expect("read lineage-events file");
    let events: Vec<serde_json::Value> = contents
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).expect("event line is JSON"))
        .collect();
    (status.success(), events)
}

fn run_correlated_lifecycle(
    pipeline: &Path,
    batch_id: &str,
) -> (std::process::Output, Vec<serde_json::Value>) {
    let dir = pipeline.parent().expect("pipeline has a parent dir");
    let output = Command::new(clinker_bin())
        .arg("run")
        .arg("pipeline.yaml")
        .args([
            "--machine",
            "ndjson-v1",
            "--batch-id",
            batch_id,
            "--lineage-events",
            "events.ndjson",
        ])
        .current_dir(dir)
        .output()
        .expect("spawn clinker");
    let contents =
        std::fs::read_to_string(dir.join("events.ndjson")).expect("read lineage-events file");
    let lineage = contents
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str(line).expect("lineage event is JSON"))
        .collect();
    (output, lineage)
}

#[test]
fn invalid_standalone_batch_ids_leave_every_run_effect_unchanged() {
    fn snapshot(root: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        fn visit(root: &Path, current: &Path, files: &mut Vec<(PathBuf, Vec<u8>)>) {
            let mut entries = std::fs::read_dir(current)
                .expect("read fixture tree")
                .map(|entry| entry.expect("fixture entry").path())
                .collect::<Vec<_>>();
            entries.sort();
            for path in entries {
                if path.is_dir() {
                    visit(root, &path, files);
                } else {
                    files.push((
                        path.strip_prefix(root)
                            .expect("relative fixture path")
                            .to_owned(),
                        std::fs::read(&path).expect("read fixture file"),
                    ));
                }
            }
        }

        let mut files = Vec::new();
        visit(root, root, &mut files);
        files
    }

    for invalid_batch_id in [String::new(), "x".repeat(257), "batch\ncontrol".to_owned()] {
        let workspace = tempfile::tempdir().expect("invalid correlation workspace");
        write_runnable_pipeline(workspace.path(), None);
        std::fs::create_dir(workspace.path().join("staging")).expect("staging root");
        std::fs::write(
            workspace.path().join("staging/sentinel"),
            b"staging-before\n",
        )
        .expect("staging sentinel");
        std::fs::write(workspace.path().join("events.ndjson"), b"lineage-before\n")
            .expect("lineage sentinel");
        let before = snapshot(workspace.path());

        let output = Command::new(clinker_bin())
            .current_dir(workspace.path())
            .args([
                "run",
                "pipeline.yaml",
                "--batch-id",
                invalid_batch_id.as_str(),
                "--lineage-events",
                "events.ndjson",
            ])
            .output()
            .expect("run invalid standalone correlation");

        assert_eq!(output.status.code(), Some(1));
        assert!(
            String::from_utf8_lossy(&output.stderr).contains("batch ID must be non-empty"),
            "stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(snapshot(workspace.path()), before);
        assert!(!workspace.path().join("out.csv").exists());
        assert!(!workspace.path().join(".clinker-attempts").exists());
    }
}

fn machine_events(output: &std::process::Output) -> Vec<serde_json::Value> {
    std::str::from_utf8(&output.stdout)
        .expect("machine stdout is UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("machine event is JSON"))
        .collect()
}

fn assert_shared_correlation(
    machine: &[serde_json::Value],
    lineage: &[serde_json::Value],
    batch_id: &str,
    terminal_event_type: &str,
) {
    assert_eq!(
        lineage.len(),
        2,
        "one shared lifecycle emits exactly START and one terminal event"
    );
    assert_eq!(lineage[0]["eventType"], "START");
    assert_eq!(lineage[1]["eventType"], terminal_event_type);

    let resolved = machine
        .iter()
        .find(|event| event["event"] == "plan_resolved")
        .expect("machine plan_resolved event");
    let execution_id = resolved["execution_id"]
        .as_str()
        .expect("machine execution ID");
    let plan = &resolved["plan_identity"];

    for event in lineage {
        assert_eq!(event["run"]["runId"], execution_id);
        assert_eq!(event["run"]["facets"]["clinker_batch"]["batchId"], batch_id);
        let semantic = &event["job"]["facets"]["clinker_semanticPlan"];
        assert_eq!(semantic["algorithm"], plan["algorithm"]);
        assert_eq!(semantic["semanticSchemaVersion"], plan["version"]);
        assert_eq!(semantic["digest"], plan["digest"]);
    }
}

#[test]
fn shared_lifecycle_correlation() {
    let static_dir = tempfile::tempdir().expect("static lifecycle workspace");
    write_runnable_pipeline(static_dir.path(), None);
    let static_output = Command::new(clinker_bin())
        .current_dir(static_dir.path())
        .args([
            "run",
            "pipeline.yaml",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "correlation-static",
            "--lineage",
            "static.ndjson",
        ])
        .output()
        .expect("run static correlated lineage");
    assert!(
        static_output.status.success(),
        "static export stderr: {}",
        String::from_utf8_lossy(&static_output.stderr)
    );
    let static_lineage = std::fs::read_to_string(static_dir.path().join("static.ndjson"))
        .expect("read static lineage")
        .lines()
        .map(|line| serde_json::from_str(line).expect("static lineage event is JSON"))
        .collect::<Vec<_>>();
    assert_shared_correlation(
        &machine_events(&static_output),
        &static_lineage,
        "correlation-static",
        "COMPLETE",
    );

    let success_dir = tempfile::tempdir().expect("successful lifecycle workspace");
    let success_pipeline = write_runnable_pipeline(success_dir.path(), None);
    let (success_output, success_lineage) =
        run_correlated_lifecycle(&success_pipeline, "correlation-success");
    assert!(
        success_output.status.success(),
        "successful run stderr: {}",
        String::from_utf8_lossy(&success_output.stderr)
    );
    let success_machine = machine_events(&success_output);
    assert_shared_correlation(
        &success_machine,
        &success_lineage,
        "correlation-success",
        "COMPLETE",
    );
    assert_eq!(
        success_machine.last().expect("machine terminal")["event"],
        "completed"
    );
    let success_stats = &success_lineage[1]["run"]["facets"]["clinker_runStats"];
    assert_eq!(success_stats["recordsRead"], 3);
    assert_eq!(success_stats["recordsWritten"], 3);
    assert_eq!(success_stats["recordsDlq"], 0);

    let failure_dir = tempfile::tempdir().expect("failed lifecycle workspace");
    let failure_pipeline = write_runnable_pipeline(failure_dir.path(), Some("17179869184G"));
    let (failure_output, failure_lineage) =
        run_correlated_lifecycle(&failure_pipeline, "correlation-failure");
    assert_eq!(failure_output.status.code(), Some(1));
    let failure_machine = machine_events(&failure_output);
    assert_shared_correlation(
        &failure_machine,
        &failure_lineage,
        "correlation-failure",
        "FAIL",
    );
    let machine_failure = &failure_machine.last().expect("machine failure terminal")["failure"];
    let lineage_failure = &failure_lineage[1]["run"]["facets"]["clinker_failure"];
    assert_eq!(lineage_failure["code"], machine_failure["code"]);
    assert_eq!(lineage_failure["category"], machine_failure["category"]);
    assert_eq!(lineage_failure["retryAdvice"], machine_failure["retry"]);
    assert_eq!(lineage_failure["message"], machine_failure["message"]);
    assert_eq!(
        failure_lineage[1]["run"]["facets"]["errorMessage"]["message"],
        machine_failure["message"]
    );
}

#[test]
fn lineage_events_successful_run_emits_start_then_complete() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = write_runnable_pipeline(dir.path(), None);
    let (success, events) = run_lineage_events(&pipeline);
    assert!(success, "a valid run must exit cleanly");

    assert_eq!(
        events.len(),
        2,
        "a successful run emits exactly START then COMPLETE, got:\n{events:#?}"
    );
    let (start, complete) = (&events[0], &events[1]);
    assert_eq!(start["eventType"], "START");
    assert_eq!(complete["eventType"], "COMPLETE");

    // Same runId across the pair, and it is the run's execution_id (a UUID).
    let run_id = start["run"]["runId"].as_str().expect("runId string");
    assert_eq!(
        complete["run"]["runId"].as_str(),
        Some(run_id),
        "START and COMPLETE must share one runId"
    );
    uuid::Uuid::parse_str(run_id).expect("runId is a UUID (the execution_id)");

    // Live events carry distinct begin/end timestamps (not one shared clock like
    // the static export).
    let t_start = start["eventTime"].as_str().expect("START eventTime");
    let t_complete = complete["eventTime"].as_str().expect("COMPLETE eventTime");
    assert!(t_start.ends_with('Z') && t_complete.ends_with('Z'));

    // The column-lineage facet rides on the COMPLETE output.
    let facet = &complete["outputs"][0]["facets"]["columnLineage"];
    assert!(
        facet.is_object(),
        "COMPLETE output must carry the columnLineage facet, got:\n{complete:#?}"
    );
    // `doubled = amount * 2` is DIRECT lineage from the source `amount` column.
    let doubled = &facet["fields"]["doubled"]["inputFields"][0];
    assert_eq!(doubled["field"], "amount");
    assert_eq!(doubled["transformations"][0]["type"], "DIRECT");

    // Real, non-zero row counts ride the clinker run-stats facet.
    let stats = &complete["run"]["facets"]["clinker_runStats"];
    assert_eq!(stats["recordsRead"], 3, "three input rows were read");
    assert_eq!(stats["recordsWritten"], 3, "three rows were written");
    assert_eq!(stats["recordsDlq"], 0);
    assert!(
        stats["durationMs"].as_i64().expect("durationMs") >= 0,
        "durationMs is a real elapsed measurement"
    );
    // A clean run has no error facet.
    assert!(complete["run"]["facets"].get("errorMessage").is_none());
}

#[test]
fn lineage_events_failing_run_emits_start_then_fail() {
    // An overflowing `memory.limit` is rejected at the executor's startup gate,
    // which runs *inside* the executor call — after the emitter has written the
    // START to the sink. The executor returns an error, which closes the run out
    // as FAIL. `17179869184G` = 2^34 GiB = 2^64 bytes, one past `u64::MAX`.
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = write_runnable_pipeline(dir.path(), Some("17179869184G"));
    let (success, events) = run_lineage_events(&pipeline);
    assert!(!success, "an overflowing memory.limit must exit non-zero");

    assert_eq!(
        events.len(),
        2,
        "a failing run emits exactly START then FAIL, got:\n{events:#?}"
    );
    let (start, fail) = (&events[0], &events[1]);
    assert_eq!(start["eventType"], "START");
    assert_eq!(fail["eventType"], "FAIL");
    assert_eq!(
        start["run"]["runId"].as_str(),
        fail["run"]["runId"].as_str(),
        "START and FAIL must share one runId"
    );

    // FAIL carries the error message in the standard error facet.
    let message = fail["run"]["facets"]["errorMessage"]["message"]
        .as_str()
        .expect("FAIL carries an errorMessage facet");
    assert!(
        !message.is_empty(),
        "the FAIL error message must be populated"
    );
    assert_eq!(
        fail["run"]["facets"]["errorMessage"]["programmingLanguage"],
        "rust"
    );
    // A failed run did not fully produce its output, so no column-lineage facet.
    assert!(
        fail["outputs"][0]["facets"].is_null() || fail["outputs"][0].get("facets").is_none(),
        "FAIL output must not carry a columnLineage facet, got:\n{fail:#?}"
    );
}

#[test]
fn lineage_events_conflicts_with_lineage() {
    // --lineage exits before running; --lineage-events requires an actual run.
    // Combining them would silently drop one, so clap must reject the pair.
    let pipeline = repo_root().join("examples/pipelines/audit_join.yaml");
    let output = Command::new(clinker_bin())
        .args(["run"])
        .arg(&pipeline)
        .args(["--lineage", "-", "--lineage-events", "-"])
        .output()
        .expect("spawn clinker");
    assert!(
        !output.status.success(),
        "--lineage + --lineage-events must be rejected"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot be used with"),
        "expected a clap conflict error, got:\n{stderr}"
    );
}

#[test]
fn delivery_isolation() {
    fn run(workspace: &Path, inject_hang: bool) -> (std::process::Output, Duration) {
        let started = std::time::Instant::now();
        let mut command = Command::new(clinker_bin());
        command.current_dir(workspace).args([
            "run",
            "pipeline.yaml",
            "--machine",
            "ndjson-v1",
            "--batch-id",
            "delivery-isolation",
            "--lineage-events",
            "events.ndjson",
        ]);
        if inject_hang {
            command.env("CLINKER_TEST_LINEAGE_SINK", "hang-after-first-write");
        }
        let output = command.output().expect("run delivery isolation fixture");
        (output, started.elapsed())
    }

    fn prepare(workspace: &Path) {
        write_runnable_pipeline(workspace, None);
        external_lineage_policy(workspace, true);
        let policy = std::fs::read_to_string(workspace.join("clinker.toml"))
            .expect("read external lineage policy")
            .replace(
                "[observability.lineage]\n",
                "[observability.lineage]\nqueue_bytes = \"4KB\"\nmax_event_bytes = \"4KB\"\nflush_timeout_ms = 50\n",
            );
        std::fs::write(workspace.join("clinker.toml"), policy)
            .expect("write bounded lineage policy");
    }

    fn authoritative_files(workspace: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        ["pipeline.yaml", "clinker.toml", "data/in.csv", "out.csv"]
            .into_iter()
            .map(|path| {
                (
                    PathBuf::from(path),
                    std::fs::read(workspace.join(path)).expect("read authoritative fixture file"),
                )
            })
            .collect()
    }

    fn attempt_evidence(workspace: &Path) -> Vec<(PathBuf, Vec<u8>)> {
        fn visit(root: &Path, current: &Path, evidence: &mut Vec<(PathBuf, Vec<u8>)>) {
            if !current.exists() {
                return;
            }
            let mut entries = std::fs::read_dir(current)
                .expect("read attempt evidence")
                .map(|entry| entry.expect("attempt entry").path())
                .collect::<Vec<_>>();
            entries.sort();
            for path in entries {
                if path.is_dir() {
                    visit(root, &path, evidence);
                } else {
                    evidence.push((
                        path.strip_prefix(root)
                            .expect("attempt relative path")
                            .to_owned(),
                        std::fs::read(&path).expect("read attempt file"),
                    ));
                }
            }
        }

        let root = workspace.join(".clinker-attempts");
        let mut evidence = Vec::new();
        visit(&root, &root, &mut evidence);
        evidence
    }

    fn terminal_truth(output: &std::process::Output) -> serde_json::Value {
        let terminal = machine_events(output)
            .into_iter()
            .last()
            .expect("machine terminal event");
        serde_json::json!({
            "event": terminal["event"],
            "batch_id": terminal["batch_id"],
            "result": terminal["result"],
            "exit_code": terminal["exit_code"],
            "failure": terminal["failure"],
            "publication": terminal["publication"],
        })
    }

    fn publication_inventory(output: &std::process::Output) -> Vec<serde_json::Value> {
        machine_events(output)
            .into_iter()
            .filter(|event| event["event"] == "publication_artifacts")
            .map(|event| event["publication"].clone())
            .collect()
    }

    let oracle = tempfile::tempdir().expect("delivery oracle workspace");
    prepare(oracle.path());
    let (oracle_output, _) = run(oracle.path(), false);
    assert!(oracle_output.status.success());

    let hung = tempfile::tempdir().expect("hung delivery workspace");
    prepare(hung.path());
    let (hung_output, elapsed) = run(hung.path(), true);
    assert_eq!(hung_output.status.code(), oracle_output.status.code());
    assert!(
        elapsed < Duration::from_secs(3),
        "hung sink controlled CLI return"
    );
    assert_eq!(terminal_truth(&hung_output), terminal_truth(&oracle_output));
    assert_eq!(
        publication_inventory(&hung_output),
        publication_inventory(&oracle_output)
    );
    assert_eq!(
        authoritative_files(hung.path()),
        authoritative_files(oracle.path())
    );
    assert!(!hung.path().join("dlq.ndjson").exists());
    assert!(!oracle.path().join("dlq.ndjson").exists());
    assert_eq!(
        attempt_evidence(hung.path()),
        attempt_evidence(oracle.path())
    );
    let stderr = String::from_utf8_lossy(&hung_output.stderr);
    assert!(stderr.contains("lineage sink received bytes"), "{stderr}");
    assert!(stderr.contains("deadline-exceeded"), "{stderr}");
}
