//! CLI regression for direct multi-Output materialized fan-out (#996).

use std::process::Command;

fn clinker_bin() -> &'static str {
    env!("CARGO_BIN_EXE_clinker")
}

#[test]
fn shared_output_input_writes_both_outputs() {
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
      include_unmapped: true
  - type: output
    name: beta
    input: prepared
    config:
      name: beta
      type: csv
      path: beta.csv
      include_unmapped: true
"#;
    std::fs::write(&pipeline_path, pipeline).expect("write pipeline fixture");

    let output = Command::new(clinker_bin())
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker");

    assert!(
        output.status.success(),
        "direct Output fan-out must succeed; stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let expected = "id,marker\n1,ready\n2,ready\n";
    let alpha = std::fs::read_to_string(dir.path().join("alpha.csv")).expect("read alpha output");
    let beta = std::fs::read_to_string(dir.path().join("beta.csv")).expect("read beta output");
    assert_eq!(alpha, expected, "alpha must receive the complete input");
    assert_eq!(beta, expected, "beta must receive the complete input");
}
