//! Aggregate dispatch integration tests.
//!
//! Full Source → Aggregate → Output pipeline runs that exercise the
//! executor's `PlanNode::Aggregation` dispatch arm through the public
//! entry point: a basic GROUP BY and the empty-input global-fold case.
//! The white-box aggregation unit tests (hash/streaming accumulator
//! internals, `single_predecessor`, two-phase byte encoder, group-boundary
//! sort-order verification) read crate-private symbols and stay inline in
//! `executor/tests/aggregation.rs`.

mod common;

use std::collections::HashMap;
use std::io::{Cursor, Write};

use clinker_core::executor::{DlqEntry, PipelineRunParams, SourceReaders, single_file_reader};
use clinker_plan::config::parse_config;
use clinker_plan::error::PipelineError;
use clinker_record::PipelineCounters;

/// Run a single-source, single-output pipeline with the given YAML config
/// and CSV input. Returns `(counters, dlq_entries, output_csv)`.
fn run_test(
    yaml: &str,
    csv_input: &str,
) -> Result<(PipelineCounters, Vec<DlqEntry>, String), PipelineError> {
    let config = parse_config(yaml).unwrap();
    let output_buf = clinker_bench_support::io::SharedBuffer::new();

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: SourceReaders = HashMap::from([(
        primary,
        single_file_reader(
            "test.csv",
            Box::new(Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(output_buf.clone()) as Box<dyn Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = common::run_config(&config, readers, writers, &params)?;
    Ok((report.counters, report.dlq_entries, output_buf.as_string()))
}

#[test]
fn test_aggregation_dispatch_basic_group_by() {
    let yaml = r#"
pipeline:
  name: agg_basic
nodes:
- type: source
  name: src
  config:
    name: src
    path: in.csv
    type: csv
    schema:
      - { name: dept, type: string }
      - { name: salary, type: float }

- type: aggregate
  name: by_dept
  input: src
  config:
    group_by:
    - dept
    cxl: 'emit dept = dept

      emit total = sum(salary)

      emit n = count(*)

      '
- type: output
  name: out
  input: by_dept
  config:
    name: out
    type: csv
    path: out.csv
    include_unmapped: true
"#;
    let csv = "dept,salary\neng,100\neng,200\nsales,50\n";
    let (counters, dlq, output) = run_test(yaml, csv).expect("pipeline runs");
    assert_eq!(dlq.len(), 0, "no DLQ entries expected");
    assert_eq!(counters.ok_count, 2, "two output groups");

    // Set-equality on output rows (order is hash-table arbitrary).
    // With salary declared as float, `sum(salary)` produces the real
    // numeric total per group — eng = 100+200 = 300, sales = 50.
    let mut lines: Vec<&str> = output.lines().skip(1).collect();
    lines.sort();
    let expected_a = "eng,300,2";
    let expected_b = "sales,50,1";
    assert!(
        lines.contains(&expected_a) && lines.contains(&expected_b),
        "got lines = {lines:?}"
    );
}

#[test]
fn test_aggregation_dispatch_global_fold_empty_input_emits_one_row() {
    let yaml = r#"
pipeline:
  name: agg_global_empty
nodes:
- type: source
  name: src
  config:
    name: src
    path: in.csv
    type: csv
    schema:
      - { name: x, type: string }

- type: aggregate
  name: total
  input: src
  config:
    group_by: []
    cxl: 'emit n = count(*)

      '
- type: output
  name: out
  input: total
  config:
    name: out
    type: csv
    path: out.csv
    include_unmapped: true
"#;
    // Header-only input — zero data rows.
    let csv = "x\n";
    let (counters, dlq, output) = run_test(yaml, csv).expect("pipeline runs");
    assert_eq!(dlq.len(), 0);
    assert_eq!(
        counters.ok_count, 1,
        "global fold emits one row even on empty input"
    );
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert_eq!(body, vec!["0"], "count(*) over empty = 0");
}
