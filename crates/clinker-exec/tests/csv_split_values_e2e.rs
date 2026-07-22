//! End-to-end proof that a CSV source's `split_values` declaration is threaded
//! through ingest and materializes as a `Value::Array` in the output.
//!
//! The reader-level split is unit-tested in `clinker-format`; this guards the
//! executor wiring (`build_csv_reader_config` copying `split_values` onto the
//! single-schema CSV reader) that the unit tests cannot see.

mod common;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineRunParams, SourceReaders};

const PIPELINE: &str = r#"
pipeline:
  name: csv_split_values_e2e
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: ./in.csv
      split_values:
        - { field: tags, delimiter: ";" }
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: output
    name: out
    input: orders
    config:
      name: out
      type: json
      path: ./out.json
"#;

/// Parse the JSON output into one `serde_json::Value` per record, tolerating
/// either a top-level array or newline-delimited objects.
fn records(output: &str) -> Vec<serde_json::Value> {
    let trimmed = output.trim();
    if let Ok(serde_json::Value::Array(items)) = serde_json::from_str(trimmed) {
        return items;
    }
    trimmed
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("each output line is a JSON object"))
        .collect()
}

#[test]
fn csv_split_values_reads_delimited_cells_as_arrays() {
    let config = clinker_plan::config::parse_config(PIPELINE).expect("pipeline parses");
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    // Row 1: three values. Row 2: an empty cell (zero values). Row 3: one value.
    let csv_input = "order_id,tags\n1,a;b;c\n2,\n3,solo\n";
    let readers: SourceReaders = HashMap::from([(
        "orders".to_string(),
        clinker_exec::executor::single_file_reader(
            "in.csv",
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    common::run_config(&config, readers, writers, &params).expect("run succeeds");

    let output = String::from_utf8(buf.contents()).expect("utf-8 output");
    let recs = records(&output);
    assert_eq!(recs.len(), 3, "one record per input row: {output}");

    let tags = |i: usize| {
        recs[i]
            .get("tags")
            .cloned()
            .unwrap_or(serde_json::Value::Null)
    };
    assert_eq!(tags(0), serde_json::json!(["a", "b", "c"]), "{output}");
    assert_eq!(
        tags(1),
        serde_json::json!([]),
        "empty cell → empty array: {output}"
    );
    assert_eq!(tags(2), serde_json::json!(["solo"]), "{output}");
}
