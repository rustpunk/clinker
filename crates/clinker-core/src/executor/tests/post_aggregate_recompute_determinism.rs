//! Determinism guard for the deferred-region windowed-Transform
//! dispatch counter.
//!
//! Runs the post-aggregate window fixture (HR retracted by `1/(60-60)`)
//! 100 times and asserts that the resulting `partitions_dispatched`
//! value is identical across every run. The fixture is deterministic by
//! construction; any value churn surfaces HashMap-iteration leakage into
//! the counter, a silent-partial-behavior smell.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::{HashMap, HashSet};

const PIPELINE: &str = r#"
pipeline:
  name: agg_then_window_buffer_recompute_determinism
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: order_id
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: src
  config:
    group_by: [department]
    cxl: 'emit department = department

      emit total = sum(amount)

      '
- type: transform
  name: running
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
      emit ratio = 1 / (total - 60)
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

const CSV: &str = "\
order_id,department,amount
O1,HR,10
O2,HR,20
O3,HR,30
O4,ENG,100
O5,ENG,200
O6,ENG,300
";

fn run_once() -> u64 {
    let config = crate::config::parse_config(PIPELINE).unwrap();
    let params = PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: config
            .pipeline
            .vars
            .as_ref()
            .map(crate::config::convert_pipeline_vars)
            .unwrap_or_default(),
        shutdown_token: None,
    };
    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(CSV.as_bytes().to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .expect("buffer-recompute pipeline must execute");
    report.counters.retraction.partitions_dispatched
}

#[test]
fn partitions_dispatched_is_deterministic_across_100_runs() {
    let counts: HashSet<u64> = (0..100).map(|_| run_once()).collect();
    assert_eq!(
        counts.len(),
        1,
        "non-deterministic partitions_dispatched: observed values = {counts:?}"
    );
    // The deferred-region dispatcher walks every windowed-Transform
    // member of every reachable region exactly once per commit-pass
    // iteration; the fixture's geometry has the `running` windowed
    // Transform inside the `dept_totals`-rooted region, so the
    // counter is fully determined by the topology, not by HashMap
    // iteration order.
    let value = *counts.iter().next().unwrap();
    assert!(
        value > 0,
        "the post-aggregate windowed-Transform must dispatch at least \
         once on the deferred-region commit pass; got {value}"
    );
}
