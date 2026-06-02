//! End-to-end document-envelope ingestion: an XML source declares
//! envelope sections at the file head and tail, the reader's pre-scan
//! extracts both before body streaming, and a downstream transform
//! reads `$doc.<section>.<field>` on every body record — including the
//! first row, proving the tail section was available up front.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use clinker_core::source::multi_file::FileSlot;

const YAML: &str = r#"
pipeline:
  name: envelope_demo
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
            fields:
              batch_id: string
          Summary:
            extract: { xml_path: "/doc/Summary" }
            fields:
              total: int
      schema:
        - { name: amount, type: int }
  - type: transform
    name: tag
    input: payments
    config:
      cxl: |
        emit amount = amount
        emit batch = $doc.BatchInfo.batch_id
  - type: transform
    name: tag2
    input: tag
    config:
      cxl: |
        emit amount = amount
        emit batch = batch
        emit declared_total = $doc.Summary.total
  - type: output
    name: out
    input: tag2
    config:
      name: out
      type: csv
      path: out.csv
"#;

const DOC_XML: &str = r#"<doc>
  <BatchInfo><batch_id>RUN-001</batch_id></BatchInfo>
  <records>
    <record><amount>10</amount></record>
    <record><amount>20</amount></record>
    <record><amount>30</amount></record>
  </records>
  <Summary><total>3</total></Summary>
</doc>"#;

#[test]
fn envelope_sections_available_on_every_body_record() {
    let config = parse_config(YAML).expect("parse envelope pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile envelope pipeline");

    let file = FileSlot::new(
        PathBuf::from("payments.xml"),
        Box::new(Cursor::new(DOC_XML.as_bytes().to_vec())),
    );
    let readers: clinker_core::executor::SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_core::executor::SourceInput::Files(vec![file]),
    )]);

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
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run envelope pipeline");
    assert_eq!(report.counters.total_count, 3, "three body records");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 4, "header + 3 data rows; got: {output}");

    // Head section (BatchInfo) and tail section (Summary) both resolve
    // on EVERY body record — including the first, which proves the
    // tail section was pre-scanned before body streaming began.
    for (i, row) in lines[1..].iter().enumerate() {
        assert!(
            row.contains("RUN-001"),
            "row {i} missing $doc.BatchInfo.batch_id: {row}"
        );
        // `declared_total` is the trailer's record count, read on each
        // body row — available mid-stream because the pre-scan pulled
        // the tail section up front.
        assert!(
            row.contains(",3") || row.ends_with("3"),
            "row {i} missing $doc.Summary.total: {row}"
        );
    }
}
