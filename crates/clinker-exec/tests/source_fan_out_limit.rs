//! End-to-end coverage for the source-side `split_to_rows` cardinality ceiling.
//!
//! The format readers own the lazy N+1 detection; this target proves the
//! executor preserves the first N rows and routes the complete original input
//! through the ordinary source DLQ instead of truncating silently or aborting.

mod common;

use std::collections::HashMap;
use std::io::Cursor;

use clinker_bench_support::io::SharedBuffer;
use clinker_core_types::dlq::DlqErrorCategory;
use clinker_exec::executor::{PipelineRunParams, SourceReaders};
use clinker_record::Value;

fn run(
    pipeline: &str,
    input: &[u8],
    filename: &str,
) -> (clinker_exec::executor::ExecutionReport, String) {
    let config = clinker_plan::config::parse_config(pipeline).expect("pipeline parses");
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_exec::executor::single_file_reader(filename, Box::new(Cursor::new(input.to_vec()))),
    )]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let report = common::run_config(
        &config,
        readers,
        writers,
        &PipelineRunParams {
            execution_id: "source-fan-out-limit".into(),
            batch_id: "batch".into(),
            ..Default::default()
        },
    )
    .expect("continue strategy routes the breach and finishes");
    (report, output.as_string())
}

fn assert_common_limit_result(
    report: &clinker_exec::executor::ExecutionReport,
    output: &str,
    header: &str,
    triggering_field: &str,
    raw_shape: fn(&Value) -> bool,
) {
    assert_eq!(
        output.lines().collect::<Vec<_>>(),
        [header, "7,0,0", "7,0,1", "7,0,2", "7,1,0"],
        "the ceiling emits exactly four stable-order rows"
    );
    assert_eq!(report.counters.dlq_count, 1);
    assert_eq!(report.dlq_entries.len(), 1);
    let entry = &report.dlq_entries[0];
    assert_eq!(entry.category, DlqErrorCategory::ExpansionLimitExceeded);
    assert_eq!(entry.triggering_field.as_deref(), Some(triggering_field));
    assert_eq!(entry.triggering_value, Some(Value::String("5".into())));
    assert!(entry.error_message.contains("max_output_rows_per_input: 4"));
    assert!(entry.error_message.contains("attempted row 5"));
    let raw = entry
        .original_record
        .get("_cxl_dlq_source_record")
        .expect("source rejection retains the original decoded input");
    assert!(raw_shape(raw), "unexpected original input shape: {raw:?}");
}

#[test]
fn json_fan_out_limit_emits_n_then_dlqs_the_original_input() {
    const PIPELINE: &str = r#"
pipeline:
  name: json_source_fan_out_limit
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: json
      path: input.json
      split_to_rows: [left, right]
      max_output_rows_per_input: 4
      schema:
        - { name: id, type: int }
        - { name: l, type: int }
        - { name: r, type: int }
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: output.csv
"#;
    const INPUT: &[u8] =
        br#"[{"id":7,"left":[{"l":0},{"l":1}],"right":[{"r":0},{"r":1},{"r":2}]}]"#;

    let (report, output) = run(PIPELINE, INPUT, "input.json");
    assert_common_limit_result(
        &report,
        &output,
        "id,l,r",
        "right",
        |raw| matches!(raw, Value::Map(map) if map.get("id") == Some(&Value::Integer(7))),
    );
}

#[test]
fn xml_fan_out_limit_emits_n_then_dlqs_ordered_raw_occurrences() {
    const PIPELINE: &str = r#"
pipeline:
  name: xml_source_fan_out_limit
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: xml
      path: input.xml
      options:
        record_path: Root/Order
      split_to_rows:
        - { field: A, mode: split }
        - { field: B, mode: split }
      max_output_rows_per_input: 4
      schema:
        - { name: id, type: int }
        - { name: A.a, type: int }
        - { name: B.b, type: int }
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: output.csv
"#;
    const INPUT: &[u8] = concat!(
        "<Root><Order><id>7</id>",
        "<A><a>0</a></A><A><a>1</a></A>",
        "<B><b>0</b></B><B><b>1</b></B><B><b>2</b></B>",
        "</Order></Root>",
    )
    .as_bytes();

    let (report, output) = run(PIPELINE, INPUT, "input.xml");
    assert_common_limit_result(
        &report,
        &output,
        "id,A.a,B.b",
        "B",
        |raw| matches!(raw, Value::Array(occurrences) if occurrences.len() >= 6),
    );
}
