//! A production sort compares its rows by column position.
//!
//! The sort comparator resolves each sort field's column once and reads a row
//! at that position only when the row carries the resolved schema handle; a
//! row behind any other handle, even one with equal columns, is read by name.
//! Both paths order identically, so an equivalence test cannot tell them
//! apart. This test counts the reads instead, through a test-only probe the
//! comparator attaches when it is resolved, and pins that a Sink's
//! `sort_order` takes the positional path for every row.

use super::*;
use crate::pipeline::sort_key::fast_path_probe;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

const ROWS: usize = 1_000;

/// `ROWS` CSV rows: a name, a grade with eleven distinct values, and a
/// score that is empty (null) on every seventh row.
fn csv() -> String {
    let mut csv = String::from("name,grade,score\n");
    for i in 0..ROWS {
        let score = if i % 7 == 0 {
            String::new()
        } else {
            ((i * 7_919) % 1_009).to_string()
        };
        csv.push_str(&format!("n{i:04},{},{score}\n", (i * 31) % 11));
    }
    csv
}

const PIPELINE: &str = r#"
pipeline:
  name: sort_fast_path
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: src.csv
      schema:
        - { name: name, type: string }
        - { name: grade, type: int }
        - { name: score, type: { nullable: int } }
  - type: sink
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
      sort_order:
        - { field: grade, order: desc }
        - { field: score, null_order: first }
"#;

/// The (grade, score) pairs of a written CSV, in output order; a null score
/// is `None`.
fn keys(output: &str) -> Vec<(i64, Option<i64>)> {
    let mut lines = output.lines();
    assert_eq!(lines.next(), Some("name,grade,score"), "the header row");
    lines
        .map(|line| {
            let fields: Vec<&str> = line.split(',').collect();
            assert_eq!(fields.len(), 3, "row {line:?}");
            let score = (!fields[2].is_empty()).then(|| fields[2].parse().unwrap());
            (fields[1].parse().unwrap(), score)
        })
        .collect()
}

#[test]
fn an_authored_sort_compares_every_row_by_position() {
    let installed = fast_path_probe::install();

    let config = clinker_plan::config::parse_config(PIPELINE).expect("parse pipeline YAML");
    let readers: crate::executor::SourceReaders = [(
        "src".to_string(),
        crate::executor::single_file_reader(
            "src.csv".to_string(),
            Box::new(std::io::Cursor::new(csv().into_bytes())),
        ),
    )]
    .into_iter()
    .collect();
    let output = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = [(
        "out".to_string(),
        Box::new(output.clone()) as Box<dyn std::io::Write + Send>,
    )]
    .into_iter()
    .collect();
    let params = PipelineRunParams {
        execution_id: "sort-fast-path".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };
    PipelineExecutor::run_with_readers_writers(&config, readers, writers.into(), &params)
        .expect("the sorted run succeeds");

    let written = keys(&output.as_string());
    assert_eq!(written.len(), ROWS, "every row is written");
    let mut expected = written.clone();
    // Grade descending, then score ascending with nulls first: `None`
    // already sorts before every `Some`.
    expected.sort_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    assert_eq!(written, expected, "the output is in the authored order");

    let probe = installed.probe();
    assert!(
        probe.reads_by_position() > 0,
        "the sort's comparator was resolved on this thread and read fields"
    );
    assert_eq!(
        probe.reads_by_name(),
        0,
        "every row the sort compared carried the schema handle its comparator \
         was resolved against ({} positional reads)",
        probe.reads_by_position()
    );
}
