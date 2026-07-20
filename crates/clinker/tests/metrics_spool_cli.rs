//! End-to-end coverage that `clinker run --metrics-spool-dir <dir>` writes a
//! v3 spool file whose per-source maps are actually populated.
//!
//! Every other per-source test drives `PipelineExecutor` in-process and never
//! exercises the CLI's spool-write path, so nothing proves the v3 JSON on disk
//! carries the `per_source_record_counts` / `per_source_dlq_counts` breakdowns.
//! This test shells out to the compiled binary, runs a multi-source pipeline
//! that also DLQs one row, and parses the produced JSON back through the typed
//! `ExecutionMetrics` shape.

use std::process::Command;

use clinker_exec::metrics::ExecutionMetrics;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

/// Multi-source merge with a Transform that dead-letters any row whose
/// `amount` is zero. `src_a` carries one such row (attributed to src_a at the
/// DLQ funnel); `src_b` is clean. Both sources therefore surface in the
/// record-count map, and `src_a` surfaces in the DLQ map.
const PIPELINE: &str = r#"pipeline:
  name: metrics_spool_cli
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: merge
    name: m
    inputs: [src_a, src_b]
  - type: transform
    name: tfm
    input: m
    config:
      cxl: |
        emit id = id
        emit v = if(amount == 0) then (1 / 0) else amount
  - type: output
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#;

#[test]
fn run_populates_per_source_maps_in_v3_spool_file() {
    let tmp = tempfile::tempdir().expect("create tempdir");
    let root = tmp.path();

    std::fs::write(root.join("pipeline.yaml"), PIPELINE).expect("write pipeline");
    // src_a: three rows, the middle one (amount=0) dead-letters.
    std::fs::write(root.join("a.csv"), "id,amount\n1,10\n2,0\n3,30\n").expect("write a.csv");
    // src_b: two clean rows.
    std::fs::write(root.join("b.csv"), "id,amount\n10,5\n20,7\n").expect("write b.csv");
    // write_spool requires the target directory to exist.
    std::fs::create_dir(root.join("spool")).expect("create spool dir");

    let output = Command::new(clinker_bin())
        .arg("run")
        .arg("pipeline.yaml")
        .arg("--metrics-spool-dir")
        .arg("spool")
        .current_dir(root)
        .output()
        .expect("spawn clinker");

    // A DLQ'd row makes this a partial-success run (exit code 2); the spool is
    // still written. Assert the run at least reached completion rather than
    // erroring out before the spool write.
    let code = output.status.code();
    assert!(
        matches!(code, Some(0) | Some(2)),
        "run should complete (clean or partial), got {code:?}; stderr:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );

    // Exactly one spool file, named <execution_id>.json.
    let spool_files: Vec<_> = std::fs::read_dir(root.join("spool"))
        .expect("read spool dir")
        .map(|e| e.expect("dir entry").path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("json"))
        .collect();
    assert_eq!(
        spool_files.len(),
        1,
        "exactly one v3 spool file is written; found {spool_files:?}"
    );

    let json = std::fs::read_to_string(&spool_files[0]).expect("read spool file");
    let metrics: ExecutionMetrics =
        serde_json::from_str(&json).expect("spool file deserializes as v3 ExecutionMetrics");

    assert_eq!(metrics.schema_version, 3, "spool file is stamped v3");

    // Both declared sources surface in the ingest-count map with their row
    // totals (the DLQ'd row still counts at ingest).
    assert_eq!(
        metrics.per_source_record_counts.get("src_a"),
        Some(&3),
        "src_a's three ingested rows surface in the spooled record-count map: {:?}",
        metrics.per_source_record_counts
    );
    assert_eq!(
        metrics.per_source_record_counts.get("src_b"),
        Some(&2),
        "src_b's two rows surface in the spooled record-count map"
    );

    // The DLQ map is populated with the attributed source.
    assert_eq!(
        metrics.per_source_dlq_counts.get("src_a"),
        Some(&1),
        "src_a's dead-lettered row surfaces in the spooled DLQ map: {:?}",
        metrics.per_source_dlq_counts
    );
    assert!(
        !metrics.per_source_dlq_counts.contains_key("src_b"),
        "src_b has no DLQ entries and is absent from the spooled DLQ map"
    );
    assert_eq!(
        metrics.records_dlq, 1,
        "the aggregate DLQ count matches the single dead-lettered row"
    );
}
