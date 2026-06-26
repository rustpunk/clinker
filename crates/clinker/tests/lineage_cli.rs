//! End-to-end coverage for `clinker run --lineage`: the built binary must emit a
//! well-formed OpenLineage START/COMPLETE NDJSON pair carrying DIRECT and INDIRECT
//! column lineage. The lineage *computation* is unit-tested in `clinker-lineage`;
//! these tests pin the CLI wiring and the on-the-wire document shape.

use std::path::{Path, PathBuf};
use std::process::Command;

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
    let pipeline = repo_root().join("examples/pipelines/audit_join.yaml");
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
    let pipeline = repo_root().join("examples/pipelines/audit_join.yaml");
    let dir = tempfile::tempdir().expect("tempdir");
    let out = dir.path().join("lineage.ndjson");

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
    let name = output_dataset_name(&pipeline, Some(ws.path()));
    assert!(
        name.contains("/subdir/"),
        "dataset name must include the pipeline subdir, got: {name}"
    );
}
