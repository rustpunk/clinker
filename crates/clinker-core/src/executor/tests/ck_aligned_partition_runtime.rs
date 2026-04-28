//! CK-aligned partition runtime: `partition_by ⊇ ck_set` short-circuits
//! the buffer-recompute path.
//!
//! When a windowed Transform's `partition_by` contains every member of
//! the upstream's correlation-key set (including the synthetic
//! `$ck.aggregate.<name>` column), every retraction event affects at
//! most one partition's row, so the engine does NOT have to rebuild
//! partitions wholesale. The orchestrator's auto-flip at
//! `derive_window_buffer_recompute_flags` keeps the spec on the
//! streaming-emit path; the per-window `partitions_recomputed` counter
//! stays at zero even when a downstream Transform DLQs records.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

/// Source(CK = order_id) → Aggregate(relaxed, group_by = department)
/// → Transform(window with `partition_by: [department, $ck.aggregate.<name>]`)
/// → Transform(divide-by-zero on a known department's row) → Output.
///
/// Each aggregate output row has its own synthetic CK; a window
/// `partition_by` that includes that synthetic CK gives each row a
/// CK-aligned partition of size 1. The DLQ on the failing department
/// never triggers a window-partition recompute.
#[test]
fn ck_aligned_partition_failure_does_not_engage_partition_recompute() {
    let yaml = r#"
pipeline:
  name: ck_aligned_runtime
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
    cxl: |
      emit department = department
      emit total = sum(amount)
- type: transform
  name: ck_aligned_window
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
    analytic_window:
      group_by: [department, '$ck.aggregate.dept_totals']
- type: transform
  name: ratio
  input: ck_aligned_window
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = running_total
      emit ratio = 1 / (total - 60)
- type: output
  name: out
  input: ratio
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    // HR's total = 60 → divisor 0 → DLQ; ENG's total = 600 → output.
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,HR,30
o4,ENG,100
o5,ENG,200
o6,ENG,300
";
    let config = crate::config::parse_config(yaml).expect("parse");
    let primary = "src".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        "src".to_string(),
        Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())) as Box<dyn std::io::Read + Send>,
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "test-exec".to_string(),
        batch_id: "test-batch".to_string(),
        pipeline_vars: Default::default(),
        shutdown_token: None,
    };
    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .expect("pipeline must run");
    let counters = report.counters;

    // The HR row hits divide-by-zero → 1 DLQ. The ENG row reaches the
    // writer.
    assert_eq!(counters.dlq_count, 1, "HR row must hit DLQ on /0");

    // CK-aligned partition: the windowed spec stays on streaming-emit
    // mode, so the recompute pass never kicks in. `partitions_recomputed`
    // is the per-pipeline counter — it MUST be zero.
    assert_eq!(
        counters.retraction.partitions_recomputed, 0,
        "CK-aligned partition (partition_by ⊇ ck_set including synthetic \
         $ck.aggregate.<name>) must NOT engage the window-partition \
         recompute pass — the FastPath / streaming-emit predicate is \
         load-bearing for performance"
    );
}
