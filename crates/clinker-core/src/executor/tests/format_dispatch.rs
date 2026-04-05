//! Format dispatch integration tests for Phase 0.
//!
//! Tests in this module verify that the executor dispatches to the correct
//! format reader/writer based on `InputFormat`/`OutputFormat` config enums.
//! Uses small in-memory payloads (5-10 records) for correctness, not benchmarks.

use super::*;
use crate::test_helpers::SharedBuffer;
use std::io::Cursor;

/// Construct a minimal NDJSON input as in-memory bytes.
fn ndjson_input(records: &[serde_json::Value]) -> Cursor<Vec<u8>> {
    let mut buf = Vec::new();
    for r in records {
        serde_json::to_writer(&mut buf, r).unwrap();
        buf.push(b'\n');
    }
    Cursor::new(buf)
}

/// Construct a minimal XML input as in-memory bytes.
fn xml_input(root: &str, record_el: &str, records: &[Vec<(&str, &str)>]) -> Cursor<Vec<u8>> {
    let mut buf = format!("<{root}>");
    for fields in records {
        buf.push_str(&format!("<{record_el}>"));
        for (k, v) in fields {
            buf.push_str(&format!("<{k}>{v}</{k}>"));
        }
        buf.push_str(&format!("</{record_el}>"));
    }
    buf.push_str(&format!("</{root}>"));
    Cursor::new(buf.into_bytes())
}

/// Construct minimal fixed-width input as in-memory bytes.
#[allow(dead_code)]
fn fixed_width_input(lines: &[&str]) -> Cursor<Vec<u8>> {
    Cursor::new(lines.join("\n").into_bytes())
}

/// Build a PipelineRunParams with test defaults.
fn test_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "test-exec-001".into(),
        batch_id: "test-batch-001".into(),
        pipeline_vars: Default::default(),
    }
}

/// Run a pipeline with an arbitrary in-memory reader, YAML config, and CSV output.
/// Returns (counters, dlq_entries, output_string).
fn run_format_test(
    yaml: &str,
    input_name: &str,
    input_data: Cursor<Vec<u8>>,
) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
    let config = crate::config::parse_config(yaml).unwrap();
    let output_buf = SharedBuffer::new();

    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        input_name.to_string(),
        Box::new(input_data) as Box<dyn std::io::Read + Send>,
    )]);
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.outputs[0].name.clone(),
        Box::new(output_buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = test_params();
    let report = PipelineExecutor::run_with_readers_writers(&config, readers, writers, &params)?;

    let output = output_buf.as_string();
    Ok((report.counters, report.dlq_entries, output))
}

#[test]
fn test_scaffold_compiles() {
    // Verifies module is discovered by the test runner.
    let _ = test_params();
}

/// JSON NDJSON input produces records through the executor.
/// Constructs 5 NDJSON records in-memory, runs a passthrough pipeline,
/// verifies all 5 records appear in CSV output.
#[test]
fn test_format_dispatch_json_ndjson_input_produces_records() {
    let yaml = r#"
pipeline:
  name: json-input-test

inputs:
  - name: src
    type: json
    path: input.json
    options:
      format: ndjson

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations: []
"#;
    let input_data = ndjson_input(&[
        serde_json::json!({"name": "Alice", "age": "30"}),
        serde_json::json!({"name": "Bob", "age": "25"}),
        serde_json::json!({"name": "Charlie", "age": "35"}),
        serde_json::json!({"name": "Diana", "age": "28"}),
        serde_json::json!({"name": "Eve", "age": "22"}),
    ]);
    let (counters, dlq, output) = run_format_test(yaml, "src", input_data).unwrap();
    assert_eq!(counters.total_count, 5, "expected 5 records");
    assert_eq!(counters.ok_count, 5);
    assert_eq!(counters.dlq_count, 0);
    assert!(dlq.is_empty());
    assert!(output.contains("Alice"), "output missing Alice: {output}");
    assert!(output.contains("Eve"), "output missing Eve: {output}");
}

/// XML input produces records through the executor.
#[test]
fn test_format_dispatch_xml_input_produces_records() {
    let yaml = r#"
pipeline:
  name: xml-input-test

inputs:
  - name: src
    type: xml
    path: input.xml
    options:
      record_path: records/record

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations: []
"#;
    let input_data = xml_input(
        "records",
        "record",
        &[
            vec![("name", "Alice"), ("age", "30")],
            vec![("name", "Bob"), ("age", "25")],
        ],
    );
    let (counters, dlq, output) = run_format_test(yaml, "src", input_data).unwrap();
    assert_eq!(counters.total_count, 2, "expected 2 records");
    assert_eq!(counters.ok_count, 2);
    assert_eq!(counters.dlq_count, 0);
    assert!(dlq.is_empty());
    assert!(output.contains("Alice"), "output missing Alice: {output}");
    assert!(output.contains("Bob"), "output missing Bob: {output}");
}

/// CSV input backward compatibility — existing behavior unchanged after dispatch refactor.
#[test]
fn test_format_dispatch_csv_input_backward_compat() {
    let yaml = r#"
pipeline:
  name: csv-compat-test

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations: []
"#;
    let csv_input = "name,age\nAlice,30\nBob,25\nCharlie,35\n";
    let input_data = Cursor::new(csv_input.as_bytes().to_vec());
    let (counters, dlq, output) = run_format_test(yaml, "src", input_data).unwrap();
    assert_eq!(counters.total_count, 3, "expected 3 records");
    assert_eq!(counters.ok_count, 3);
    assert_eq!(counters.dlq_count, 0);
    assert!(dlq.is_empty());
    assert!(output.contains("Alice"), "output missing Alice: {output}");
    assert!(
        output.contains("Charlie"),
        "output missing Charlie: {output}"
    );
}
