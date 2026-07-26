//! CLI regression for missing required materialized node-buffer inputs (#1029).

use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

#[test]
fn missing_planned_input_exits_nonzero_with_actionable_diagnostic() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("input.csv"), "id\n1\n2\n").expect("write source fixture");

    let pipeline_path = dir.path().join("pipeline.yaml");
    let pipeline = r#"pipeline:
  name: missing_node_buffer_cli
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: input.csv
      schema:
        - { name: id, type: string }
  - type: transform
    name: prepared
    input: rows
    config:
      cxl: |
        emit marker = "ready"
  - type: output
    name: alpha
    input: prepared
    config:
      name: alpha
      type: csv
      path: alpha.csv
  - type: output
    name: beta
    input: prepared
    config:
      name: beta
      type: csv
      path: beta.csv
"#;
    std::fs::write(&pipeline_path, pipeline).expect("write pipeline fixture");

    let output = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker");

    assert!(
        !output.status.success(),
        "a missing planned input must produce a nonzero exit; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("alpha"),
        "diagnostic must name consuming node alpha; got:\n{stderr}"
    );
    assert!(
        stderr.contains("prepared"),
        "diagnostic must name producer prepared; got:\n{stderr}"
    );
    let normalized_stderr = stderr.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(
        normalized_stderr.contains("run stopped instead of treating it as empty"),
        "diagnostic must explain its fail-closed disposition; got:\n{stderr}"
    );
}
