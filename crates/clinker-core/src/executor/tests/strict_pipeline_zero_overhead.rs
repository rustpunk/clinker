//! Strict-pipeline zero-overhead invariant.
//!
//! A pipeline whose every aggregate's `group_by` covers the upstream
//! correlation key (or has no upstream CK at all) stays on the strict
//! commit path: the planner registers no deferred regions, the
//! orchestrator selects `CommitStepPath::FastPath`, and the cascading-
//! retraction loop never runs. Pinning this is load-bearing for the
//! performance contract — strict pipelines must not pay any cost
//! introduced by the relaxed-CK retract machinery.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

/// Source(CK=order_id) → Aggregate(group_by ⊇ ck_set via
/// `[order_id, department]`) → Output. The aggregate covers every CK
/// field so the planner's deferred-region detector finds no relaxed
/// producer.
const STRICT_PIPELINE: &str = r#"
pipeline:
  name: strict_zero_overhead
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
  name: order_totals
  input: src
  config:
    group_by: [order_id, department]
    cxl: |
      emit order_id = order_id
      emit department = department
      emit total = sum(amount)
- type: output
  name: out
  input: order_totals
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;

#[test]
fn strict_pipeline_has_no_deferred_regions_at_plan_time() {
    use crate::config::{CompileContext, parse_config};
    let config = parse_config(STRICT_PIPELINE).expect("parse");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile must succeed for the strict pipeline")
        .dag()
        .clone();

    assert!(
        plan.deferred_regions.is_empty(),
        "strict pipeline (every aggregate's group_by ⊇ ck_set) must \
         register zero deferred regions; got {:?}",
        plan.deferred_regions.keys().collect::<Vec<_>>()
    );
}

#[test]
fn strict_pipeline_runs_without_engaging_deferred_dispatcher() {
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,ENG,100
";
    let primary = "src".to_string();
    let readers: HashMap<String, Box<dyn std::io::Read + Send>> = HashMap::from([(
        primary.clone(),
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
    let config = crate::config::parse_config(STRICT_PIPELINE).expect("parse");
    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .expect("strict pipeline must run on the FastPath without error");

    // Strict pipelines emit every record through the strict commit
    // body — no deferred-Output speculation, no cascading-retraction
    // loop, no buffer-recompute. Counters that only the deferred path
    // increments stay at zero.
    let r = &report.counters.retraction;
    assert_eq!(
        r.partitions_dispatched, 0,
        "strict pipeline must not engage the deferred-region dispatcher; \
         got partitions_dispatched={}",
        r.partitions_dispatched
    );
    assert_eq!(
        r.groups_recomputed, 0,
        "strict pipeline must not invoke recompute_aggregates; got \
         groups_recomputed={}",
        r.groups_recomputed
    );
    assert_eq!(
        r.synthetic_ck_columns_emitted_total, 0,
        "strict aggregate must not stamp the synthetic CK column; got \
         synthetic_ck_columns_emitted_total={}",
        r.synthetic_ck_columns_emitted_total
    );

    let written = buf.as_string();
    let lines: Vec<&str> = written.lines().filter(|l| !l.is_empty()).collect();
    assert!(
        lines.len() >= 3,
        "strict pipeline emits header + every aggregate row through the \
         FastPath; got {lines:?}"
    );
}
