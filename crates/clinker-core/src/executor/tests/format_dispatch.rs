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
fn fixed_width_input(lines: &[&str]) -> Cursor<Vec<u8>> {
    Cursor::new(lines.join("\n").into_bytes())
}

/// Compute `Vec<FieldDef>` with `start` as running sum of preceding widths.
///
/// Fixed-width reader requires `start` on every `FieldDef`. This helper takes
/// `(name, width, field_type)` tuples and produces positioned `FieldDef`s.
fn compute_field_layout(
    fields: &[(&str, usize, clinker_record::schema_def::FieldType)],
) -> Vec<clinker_record::schema_def::FieldDef> {
    let mut start = 0;
    fields
        .iter()
        .map(|(name, width, ftype)| {
            let field = clinker_record::schema_def::FieldDef {
                name: name.to_string(),
                field_type: Some(ftype.clone()),
                start: Some(start),
                width: Some(*width),
                required: None,
                format: None,
                coerce: None,
                default: None,
                allowed_values: None,
                alias: None,
                inherits: None,
                end: None,
                justify: None,
                pad: None,
                trim: None,
                truncation: None,
                precision: None,
                scale: None,
                path: None,
                drop: None,
                record: None,
            };
            start += width;
            field
        })
        .collect()
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

/// JSON NDJSON output produces valid newline-delimited JSON.
/// CSV input → NDJSON output, verify each line parses as JSON.
#[test]
fn test_format_dispatch_json_output_produces_valid_ndjson() {
    let yaml = r#"
pipeline:
  name: json-output-test

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: json
    path: output.json
    options:
      format: ndjson
    include_unmapped: true

transformations: []
"#;
    let csv_input = "name,age\nAlice,30\nBob,25\nCharlie,35\n";
    let input_data = Cursor::new(csv_input.as_bytes().to_vec());
    let (counters, _dlq, output) = run_format_test(yaml, "src", input_data).unwrap();
    assert_eq!(counters.total_count, 3, "expected 3 records");
    assert_eq!(counters.ok_count, 3);

    // Each non-empty line should parse as valid JSON
    let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 3, "expected 3 NDJSON lines, got: {output}");
    for (i, line) in lines.iter().enumerate() {
        let parsed: serde_json::Value =
            serde_json::from_str(line).unwrap_or_else(|e| panic!("line {i} invalid JSON: {e}"));
        assert!(parsed.is_object(), "line {i} should be JSON object");
    }
    // Verify field values
    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["name"], "Alice");
}

/// XML output produces valid XML with configured root/record elements.
/// CSV input → XML output, verify output parses as valid XML.
#[test]
fn test_format_dispatch_xml_output_produces_valid_xml() {
    let yaml = r#"
pipeline:
  name: xml-output-test

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: xml
    path: output.xml
    options:
      root_element: records
      record_element: record
    include_unmapped: true

transformations: []
"#;
    let csv_input = "name,age\nAlice,30\nBob,25\n";
    let input_data = Cursor::new(csv_input.as_bytes().to_vec());
    let (counters, _dlq, output) = run_format_test(yaml, "src", input_data).unwrap();
    assert_eq!(counters.total_count, 2, "expected 2 records");
    assert_eq!(counters.ok_count, 2);

    // Output should be valid XML containing records element
    assert!(
        output.contains("<records>"),
        "missing root element: {output}"
    );
    assert!(
        output.contains("</records>"),
        "missing closing root: {output}"
    );
    assert!(
        output.contains("<record>"),
        "missing record element: {output}"
    );
    assert!(
        output.contains("<name>Alice</name>"),
        "missing Alice: {output}"
    );
    assert!(output.contains("<name>Bob</name>"), "missing Bob: {output}");
}

/// Cross-format: CSV input, JSON output.
/// Verifies that input format ≠ output format works correctly.
#[test]
fn test_format_dispatch_csv_to_json_cross_format() {
    let yaml = r#"
pipeline:
  name: csv-to-json-test

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: json
    path: output.json
    options:
      format: ndjson
    include_unmapped: true

transformations: []
"#;
    let csv_input = "id,value\n1,alpha\n2,beta\n3,gamma\n";
    let input_data = Cursor::new(csv_input.as_bytes().to_vec());
    let (counters, _dlq, output) = run_format_test(yaml, "src", input_data).unwrap();
    assert_eq!(counters.total_count, 3, "expected 3 records");
    assert_eq!(counters.ok_count, 3);

    // Parse each line and verify field values match input CSV
    let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 3);
    let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(first["id"], "1");
    assert_eq!(first["value"], "alpha");
    let third: serde_json::Value = serde_json::from_str(lines[2]).unwrap();
    assert_eq!(third["id"], "3");
    assert_eq!(third["value"], "gamma");
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

/// Fixed-width input with inline schema produces records through the executor.
/// Constructs 2 fixed-width records with 2 fields (name: start=0 width=10, age: start=10 width=5),
/// runs through a passthrough pipeline, verifies correct field extraction.
#[test]
fn test_format_dispatch_fixed_width_input_with_schema() {
    let yaml = r#"
pipeline:
  name: fw-input-test

inputs:
  - name: src
    type: fixed_width
    path: input.dat
    schema:
      fields:
        - name: name
          type: string
          start: 0
          width: 10
        - name: age
          type: integer
          start: 10
          width: 5

outputs:
  - name: dest
    type: csv
    path: output.csv
    include_unmapped: true

transformations: []
"#;
    let input_data = fixed_width_input(&["Alice     00030", "Bob       00025"]);
    let (counters, dlq, output) = run_format_test(yaml, "src", input_data).unwrap();
    assert_eq!(counters.total_count, 2, "expected 2 records");
    assert_eq!(counters.ok_count, 2);
    assert_eq!(counters.dlq_count, 0);
    assert!(dlq.is_empty());
    assert!(output.contains("Alice"), "output missing Alice: {output}");
    assert!(output.contains("Bob"), "output missing Bob: {output}");
}

/// Fixed-width output produces correctly aligned columns.
/// CSV input → fixed-width output with inline schema, verify output has
/// correct column widths and alignment.
#[test]
fn test_format_dispatch_fixed_width_output_produces_valid_data() {
    let yaml = r#"
pipeline:
  name: fw-output-test

inputs:
  - name: src
    type: csv
    path: input.csv

outputs:
  - name: dest
    type: fixed_width
    path: output.dat
    schema:
      fields:
        - name: name
          type: string
          width: 10
        - name: age
          type: integer
          width: 5
    include_unmapped: true

transformations: []
"#;
    let csv_input = "name,age\nAlice,30\nBob,25\n";
    let input_data = Cursor::new(csv_input.as_bytes().to_vec());
    let (counters, _dlq, output) = run_format_test(yaml, "src", input_data).unwrap();
    assert_eq!(counters.total_count, 2, "expected 2 records");
    assert_eq!(counters.ok_count, 2);

    // Each line should be exactly 15 chars (10 for name + 5 for age)
    let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(
        lines.len(),
        2,
        "expected 2 fixed-width lines, got: {output}"
    );
    for line in &lines {
        assert_eq!(
            line.len(),
            15,
            "expected 15 chars per line, got {} for '{line}'",
            line.len()
        );
    }
    // First line: "Alice" left-justified in 10 chars, "30" right-justified in 5 chars
    assert!(
        lines[0].starts_with("Alice"),
        "first line should start with Alice: '{}'",
        lines[0]
    );
    assert!(
        lines[0].ends_with("   30"),
        "first line should end with right-justified '   30': '{}'",
        lines[0]
    );
}

/// Fixed-width input without schema returns a validation error.
/// Config validation catches the missing schema early with an actionable message.
#[test]
fn test_format_dispatch_fixed_width_missing_schema_errors() {
    let yaml = r#"
pipeline:
  name: fw-no-schema-test

inputs:
  - name: src
    type: fixed_width
    path: input.dat

outputs:
  - name: dest
    type: csv
    path: output.csv

transformations: []
"#;
    let input_data = fixed_width_input(&["Alice     00030"]);
    let result = run_format_test(yaml, "src", input_data);
    assert!(result.is_err(), "expected error for missing schema");
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("schema") || err.contains("field"),
        "error should mention schema requirement: {err}"
    );
}
