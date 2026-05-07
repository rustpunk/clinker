//! End-to-end multi-file source ingestion: glob matcher, multi-file reader
//! concatenates records across files, `$source.file` propagates per record,
//! and `$source.row` stays accurate across the file boundary.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use clinker_core::source::multi_file::FileSlot;

const YAML_SOURCE_FILE_PROJECTED: &str = r#"
pipeline:
  name: multi_file_demo
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: float }
  - type: transform
    name: stamp
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit amount = amount
        emit src_file = $source.file
        emit src_row = $source.row
  - type: output
    name: out
    input: stamp
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

#[test]
fn multi_file_glob_concatenates_and_stamps_source_provenance() {
    let config = parse_config(YAML_SOURCE_FILE_PROJECTED).unwrap();
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile multi-file pipeline");

    // Two in-memory files, fed in via the FileSlot API. The executor's
    // MultiFileFormatReader concatenates them and updates
    // current_source_file on file boundary; the ingestion loop stamps
    // each record with the right Arc.
    let file_a = FileSlot::new(
        PathBuf::from("orders_a.csv"),
        Box::new(Cursor::new(
            "order_id,amount\nORD-1,100.0\nORD-2,200.0\n"
                .as_bytes()
                .to_vec(),
        )),
    );
    let file_b = FileSlot::new(
        PathBuf::from("orders_b.csv"),
        Box::new(Cursor::new(
            "order_id,amount\nORD-3,300.0\n".as_bytes().to_vec(),
        )),
    );
    let readers: clinker_core::executor::SourceReaders =
        HashMap::from([("orders".to_string(), vec![file_a, file_b])]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run pipeline");
    assert_eq!(report.counters.total_count, 3);
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let lines: Vec<&str> = output.lines().collect();
    // Header + 3 data rows.
    assert_eq!(lines.len(), 4, "got: {output}");
    let header = lines[0];
    assert!(
        header.contains("src_file") && header.contains("src_row"),
        "header missing $source projection: {header}"
    );

    // Row 1 came from file A; row 3 from file B. The stamps should
    // reflect the per-row Arc swap when MultiFileFormatReader advanced.
    let row1 = lines[1];
    let row3 = lines[3];
    assert!(
        row1.contains("orders_a.csv"),
        "row 1 should carry orders_a.csv, got: {row1}"
    );
    assert!(
        row3.contains("orders_b.csv"),
        "row 3 should carry orders_b.csv, got: {row3}"
    );
}
