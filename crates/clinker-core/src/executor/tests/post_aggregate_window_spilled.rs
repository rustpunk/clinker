//! Post-aggregate window correctness when the aggregator's memory
//! budget forces an unsorted, possibly-spilled emit.
//!
//! Hash aggregation produces emit-order non-determinism — the iteration
//! order of `HashMap<GroupKey, AccState>` is randomized per process and
//! varies further when the aggregator spills partials to disk. The
//! windowed Transform downstream sees this emit order at its node-rooted
//! arena's `record_pos`. Window value correctness must NOT depend on
//! that order: partition assignment is field-value-keyed (`group_by`),
//! and `IndexSpec.sort_by` (when present) re-sorts within partition. The
//! test asserts every per-department `running_total` equals the dept's
//! aggregate `total`, regardless of the order in which the aggregator
//! emitted department rows.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

/// Pipeline with a tight `memory_limit` and many distinct group keys —
/// exercises the hash-aggregate path under memory pressure (and the
/// spill admission path on hosts with `rss_bytes()` available).
#[test]
fn post_aggregate_window_correct_under_memory_pressure() {
    let yaml = r#"
pipeline:
  name: post_aggregate_window_spilled
  memory_limit: "8M"
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: src
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount)
- type: transform
  name: running
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: running
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#;
    // Build a CSV with 50 distinct departments × 4 rows each so the
    // aggregator's group state is non-trivial and emit order is hash-
    // determined.
    let mut csv = String::from("department,amount\n");
    for d in 0..50 {
        for k in 1..=4 {
            csv.push_str(&format!("dept_{d:03},{}\n", d * 100 + k * 10));
        }
    }

    let config = crate::config::parse_config(yaml).expect("parse");
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
    };
    let primary = "src".to_string();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers.into(), &params)
        .expect("pipeline must run under memory pressure");
    let output = buf.as_string();

    // For each emitted row, running_total must equal total — every
    // partition is size 1 because the aggregate emits exactly one row
    // per department. Bucket iteration order does not change the
    // arithmetic.
    let mut lines = output.lines();
    let header_line = lines.next().expect("header");
    let headers: Vec<&str> = header_line.split(',').collect();
    let dept_idx = headers.iter().position(|h| *h == "department").unwrap();
    let total_idx = headers.iter().position(|h| *h == "total").unwrap();
    let running_idx = headers.iter().position(|h| *h == "running_total").unwrap();

    let mut row_count = 0;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let v: Vec<&str> = line.split(',').collect();
        let dept = v[dept_idx];
        let total: i64 = v[total_idx].parse().expect("total parses");
        let running: i64 = v[running_idx].parse().expect("running_total parses");
        assert_eq!(
            running, total,
            "running_total {running} must equal total {total} for dept {dept} \
             (single-row partition, regardless of aggregator's hash-bucket \
             emit order)"
        );
        row_count += 1;
    }
    assert_eq!(
        row_count, 50,
        "50 distinct departments must all reach output"
    );
}
