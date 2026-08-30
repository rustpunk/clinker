//! End-to-end proof that a CSV sink joins a `multiple:` field into one
//! delimited cell, and that a `join_values` `on_conflict: error` collision
//! dead-letters the offending record (naming the field and value) instead of
//! aborting the run — the central acceptance criterion of #917.
//!
//! The join logic and the collision error are unit-tested in `clinker-format`;
//! this guards the executor wiring the unit tests cannot see: the writer factory
//! threading `join_values`, and the Output dispatch arms routing the collision
//! error to the DLQ under `Continue`. Both the buffered arm (source → output)
//! and the streaming fused arm (source → transform → output) are exercised.

mod common;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_core_types::dlq::DlqErrorCategory;
use clinker_exec::executor::{ExecutionReport, PipelineRunParams, SourceReaders};
use clinker_plan::error::PipelineError;
use clinker_record::Value;

fn params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// A JSON source (native arrays) feeding a CSV sink. Row 1's `tags` array holds
/// a value containing the join delimiter `;`; row 2 is clean.
const JSON_INPUT: &str = r#"[
  {"order_id":"1","tags":["a;b","c"],"optional":"rejected-only","displaced":"rejected-only"},
  {"order_id":"2","tags":["x","y"]}
]"#;

fn json_reader() -> SourceReaders {
    HashMap::from([(
        "orders".to_string(),
        clinker_exec::executor::single_file_reader(
            "in.json",
            Box::new(std::io::Cursor::new(JSON_INPUT.as_bytes().to_vec())),
        ),
    )])
}

fn run_input(
    pipeline: &str,
    source: &str,
    filename: &str,
    input: &[u8],
) -> (Result<ExecutionReport, PipelineError>, SharedBuffer) {
    let config = clinker_plan::config::parse_config(pipeline).expect("pipeline parses");
    let readers = HashMap::from([(
        source.to_string(),
        clinker_exec::executor::single_file_reader(
            filename,
            Box::new(std::io::Cursor::new(input.to_vec())),
        ),
    )]);
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    (
        common::run_config(&config, readers, writers, &params()),
        output,
    )
}

#[test]
fn csv_declared_multiple_read_write_round_trip() {
    const PIPELINE: &str = r#"
pipeline:
  name: csv_multi_value_round_trip
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: ./in.csv
      split_values:
        - tags
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: sink
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    const INPUT: &[u8] = b"order_id,tags\n1,a;b\n2,x;y;z\n";
    let (result, output) = run_input(PIPELINE, "orders", "in.csv", INPUT);
    result.expect("declared multi-value CSV round-trip succeeds");
    assert_eq!(output.contents(), INPUT);
}

#[test]
fn csv_undeclared_array_fails_on_buffered_output() {
    const PIPELINE: &str = r#"
pipeline:
  name: csv_array_backstop_buffered
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./in.json
      schema:
        - { name: payload, type: any }
  - type: sink
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let (result, output) = run_input(PIPELINE, "orders", "in.json", br#"[{"payload":["a","b"]}]"#);
    let error = result.expect_err("an undeclared array must fail the CSV write");
    let message = error.to_string();
    assert!(
        message.contains("CSV") && message.contains("payload"),
        "{message}"
    );
    assert_eq!(
        output.as_string(),
        "payload\n",
        "the schema header may be staged, but the failed record must not be serialized"
    );
}

#[test]
fn csv_undeclared_array_fails_on_streaming_output() {
    const PIPELINE: &str = r#"
pipeline:
  name: csv_array_backstop_streaming
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./in.json
      schema:
        - { name: order_id, type: string }
  - type: transform
    name: collect
    input: orders
    config:
      cxl: |
        emit payload = [order_id]
  - type: sink
    name: out
    input: collect
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let (result, _output) = run_input(PIPELINE, "orders", "in.json", br#"[{"order_id":"1"}]"#);
    let error = result.expect_err("an undeclared array must fail the fused CSV write");
    let message = error.to_string();
    assert!(
        message.contains("CSV") && message.contains("payload"),
        "{message}"
    );
}

/// The buffered Output arm (a bare `source → output` chain is not streaming
/// fused): the clean record is joined and written; the colliding record is
/// dead-lettered with the field and value attached, and the run still succeeds.
#[test]
fn csv_join_values_collision_dead_letters_on_buffered_arm() {
    const PIPELINE: &str = r#"
pipeline:
  name: csv_join_values_buffered
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./in.json
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: sink
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: ./out.csv
      mapping:
        - order_id
        - tags
        - optional_copy: optional
        - displaced: order_id
"#;
    let config = clinker_plan::config::parse_config(PIPELINE).expect("pipeline parses");
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report = common::run_config(&config, json_reader(), writers, &params())
        .expect("run succeeds — the collision dead-letters, it does not abort");

    // Only the clean record reached the CSV; the colliding record did not.
    let output = String::from_utf8(buf.contents()).expect("utf-8 output");
    assert_eq!(
        output, "order_id,tags,optional_copy,displaced\n2,x;y,,2\n",
        "got: {output}"
    );

    assert_join_collision_entry(&report.dlq_entries);
    assert_rejected_row_did_not_affect_mapping_advisories(&report.advisories);
    // The colliding record is counted once (as DLQ), not as both written and
    // dead-lettered: one row written/ok (row 2), one row dead-lettered (row 1).
    assert_eq!(report.counters.records_written, 1, "only row 2 was written");
    assert_eq!(report.counters.ok_count, 1, "only row 2 is ok");
    assert_eq!(report.counters.dlq_count, 1, "row 1 collided");
}

/// The streaming fused Output arm (a `source → transform → output` chain fuses
/// the passthrough transform onto the streaming writer): the collision must
/// dead-letter there too, drained to the DLQ at thread-join.
#[test]
fn csv_join_values_collision_dead_letters_on_streaming_arm() {
    const PIPELINE: &str = r#"
pipeline:
  name: csv_join_values_streaming
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: json
      path: ./in.json
      schema:
        - { name: order_id, type: string }
        - { name: tags, type: string, multiple: true }
  - type: transform
    name: passthrough
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit tags = tags
  - type: sink
    name: out
    input: passthrough
    config:
      name: out
      type: csv
      path: ./out.csv
      mapping:
        - order_id
        - tags
        - optional_copy: optional
        - displaced: order_id
"#;
    let config = clinker_plan::config::parse_config(PIPELINE).expect("pipeline parses");
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report = common::run_config(&config, json_reader(), writers, &params())
        .expect("run succeeds — the streaming collision dead-letters, it does not abort");

    let output = String::from_utf8(buf.contents()).expect("utf-8 output");
    assert_eq!(
        output, "order_id,tags,optional_copy,displaced\n2,x;y,,2\n",
        "got: {output}"
    );

    assert_join_collision_entry(&report.dlq_entries);
    assert_rejected_row_did_not_affect_mapping_advisories(&report.advisories);
    assert_eq!(report.counters.records_written, 1, "only row 2 was written");
    assert_eq!(report.counters.ok_count, 1, "only row 2 is ok");
    assert_eq!(report.counters.dlq_count, 1, "row 1 collided");
}

/// The colliding row is the only one carrying `optional` and the passthrough
/// `displaced`. Because it was dead-lettered, it neither resolves the empty
/// mapped column nor creates a displaced-column warning for the written file.
fn assert_rejected_row_did_not_affect_mapping_advisories(advisories: &[String]) {
    assert_eq!(advisories.len(), 1, "{advisories:?}");
    assert!(advisories[0].contains("W365"), "{}", advisories[0]);
    assert!(advisories[0].contains("'optional'"), "{}", advisories[0]);
    assert!(
        !advisories.iter().any(|advisory| advisory.contains("W366")),
        "a passthrough present only on the rejected row was not displaced in the file: \
         {advisories:?}"
    );
}

/// Exactly one `MultiValueJoinCollision` entry, naming the `tags` field and the
/// offending `a;b` value, stamped with the `output:out` sink stage.
fn assert_join_collision_entry(entries: &[clinker_exec::executor::DlqEntry]) {
    let collisions: Vec<_> = entries
        .iter()
        .filter(|e| e.category == DlqErrorCategory::MultiValueJoinCollision)
        .collect();
    assert_eq!(
        collisions.len(),
        1,
        "one collision entry expected, got {:?}",
        entries
            .iter()
            .map(|e| e.category.as_str())
            .collect::<Vec<_>>()
    );
    let entry = collisions[0];
    assert_eq!(
        entry.triggering_field.as_deref(),
        Some("tags"),
        "the entry names the offending field"
    );
    assert_eq!(
        entry.triggering_value,
        Some(Value::String("a;b".into())),
        "the entry carries the offending value"
    );
    assert_eq!(
        entry.stage.as_deref(),
        Some("output:out"),
        "the entry is stamped with the sink-write stage"
    );
    assert!(entry.trigger, "the failing record is its own trigger");
}
