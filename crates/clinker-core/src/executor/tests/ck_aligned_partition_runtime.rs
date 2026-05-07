//! CK-aligned partition runtime: an aggregate whose `group_by ⊇ ck_set`
//! stays on the strict commit path (no relaxed-CK aggregate, no
//! deferred region), so the deferred-region dispatcher never runs and
//! `partitions_dispatched` stays at zero even when a downstream
//! Transform DLQs records.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

/// Source → Transform(divide-by-zero on a known row) → Output. Without
/// a relaxed-CK aggregate the planner classifies the pipeline as
/// strict and registers no deferred region. The downstream Transform's
/// divide-by-zero hits the strict commit body's per-record DLQ path
/// directly; the deferred-region dispatcher never runs and
/// `partitions_dispatched` stays at zero — the load-bearing FastPath
/// performance contract for any pipeline whose aggregates cover their
/// CK lineage.
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
- type: transform
  name: ratio
  input: src
  config:
    cxl: |
      emit order_id = order_id
      emit department = department
      emit amount = amount
      emit ratio = 1 / (amount - 30)
- type: output
  name: out
  input: ratio
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    // o3 (HR, 30) hits divide-by-zero → 1 DLQ. The other rows reach
    // the writer.
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
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
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
    let report = PipelineExecutor::run_with_readers_writers(
        &config,
        &primary,
        readers,
        writers.into(),
        &params,
    )
    .expect("pipeline must run");
    let counters = report.counters;

    // o3 (HR, 30) hits divide-by-zero → 1 DLQ. Other rows reach the
    // writer.
    assert_eq!(
        counters.dlq_count, 1,
        "exactly one row (o3, total=30) hits divide-by-zero in `ratio`"
    );

    // CK-aligned aggregate (group_by ⊇ ck_set): the deferred-region
    // dispatcher does not run (strict commit path), so
    // `partitions_dispatched` stays at zero — load-bearing for the
    // FastPath performance contract.
    assert_eq!(
        counters.retraction.partitions_dispatched, 0,
        "CK-aligned aggregate (group_by ⊇ ck_set) must NOT engage the \
         deferred-region dispatcher — the FastPath / streaming-emit \
         predicate is load-bearing for performance"
    );
}
