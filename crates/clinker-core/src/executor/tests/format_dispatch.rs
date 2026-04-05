//! Format dispatch integration tests for Phase 0.
//!
//! Tests in this module verify that the executor dispatches to the correct
//! format reader/writer based on `InputFormat`/`OutputFormat` config enums.
//! Uses small in-memory payloads (5-10 records) for correctness, not benchmarks.

use super::*;
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

/// Build a PipelineRunParams with test defaults.
fn test_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "test-exec-001".into(),
        batch_id: "test-batch-001".into(),
        pipeline_vars: Default::default(),
    }
}

#[test]
fn test_scaffold_compiles() {
    // Verifies module is discovered by the test runner.
    let _ = test_params();
}
