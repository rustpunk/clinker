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
use clinker_exec::executor::{PipelineRunParams, SourceReaders};
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
const JSON_INPUT: &str =
    r#"[{"order_id":"1","tags":["a;b","c"]},{"order_id":"2","tags":["x","y"]}]"#;

fn json_reader() -> SourceReaders {
    HashMap::from([(
        "orders".to_string(),
        clinker_exec::executor::single_file_reader(
            "in.json",
            Box::new(std::io::Cursor::new(JSON_INPUT.as_bytes().to_vec())),
        ),
    )])
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
  - type: output
    name: out
    input: orders
    config:
      name: out
      type: csv
      path: ./out.csv
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
    assert_eq!(output, "order_id,tags\n2,x;y\n", "got: {output}");

    assert_join_collision_entry(&report.dlq_entries);
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
  - type: output
    name: out
    input: passthrough
    config:
      name: out
      type: csv
      path: ./out.csv
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
    assert_eq!(output, "order_id,tags\n2,x;y\n", "got: {output}");

    assert_join_collision_entry(&report.dlq_entries);
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
