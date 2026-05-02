//! Forward-pass deferred-dispatch wiring smoke test.
//!
//! Exercises the operator-arm short-circuit and the producer-projection
//! split landed alongside this module: a relaxed-CK Aggregate with
//! downstream Transform + Output forms a deferred region whose member
//! and exit operators must NOT execute on the forward pass. The
//! commit-time deferred dispatcher (a later phase) is the only legal
//! caller of those arms; the forward pass observes the producer's
//! buffer projected to the region's `buffer_schema` and nothing flowing
//! into the writer.
//!
//! The downstream retraction tests
//! (`correlated_post_aggregate_retract`,
//! `correlated_window_after_aggregate_retract`,
//! `correlated_dlq_retract`, `window_recompute_correctness`) are
//! expected to be CI-red until the commit-time deferred dispatcher
//! lands; they assert end-to-end values that depend on the deferred
//! arms running, which they will once a follow-up commit wires the
//! post-recompute re-dispatch.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::{BTreeSet, HashMap};

/// Source(`order_id` CK) → Aggregate(`group_by: [department]`, relaxed
/// because `group_by` omits the source CK) → Transform → Output. The
/// Aggregate seeds a deferred region whose member is the Transform and
/// whose exit is the Output.
const DEFERRED_PIPELINE: &str = r#"
pipeline:
  name: deferred_smoke
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
  name: scaled
  input: dept_totals
  config:
    cxl: |
      emit department = department
      emit total = total
      emit scaled = total * 2
- type: output
  name: out
  input: scaled
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;

/// Plan-side: the planner registers a deferred region whose producer is
/// the relaxed Aggregate and whose `buffer_schema` covers exactly the
/// columns the deferred Transform reaches via `Expr::support_into`.
/// `members` includes the Transform; `outputs` includes the
/// correlation-buffered Output. The region's NodeIndex membership
/// is reachable from `dag.deferred_region_at` for every participant.
#[test]
fn relaxed_aggregate_seeds_a_deferred_region_with_downstream_members() {
    use crate::config::{CompileContext, parse_config};
    use crate::plan::execution::PlanNode;

    let config = parse_config(DEFERRED_PIPELINE).expect("parse");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile must succeed for the deferred-region smoke pipeline")
        .dag()
        .clone();

    let agg_idx = plan
        .graph
        .node_indices()
        .find(|&i| matches!(&plan.graph[i], PlanNode::Aggregation { .. }))
        .expect("Aggregate node must be present");
    let transform_idx = plan
        .graph
        .node_indices()
        .find(|&i| matches!(&plan.graph[i], PlanNode::Transform { name, .. } if name == "scaled"))
        .expect("Transform 'scaled' must be present");
    let output_idx = plan
        .graph
        .node_indices()
        .find(|&i| matches!(&plan.graph[i], PlanNode::Output { name, .. } if name == "out"))
        .expect("Output 'out' must be present");

    let region = plan
        .deferred_region_at_producer(agg_idx)
        .expect("Aggregate must seed a deferred region");
    assert!(
        region.members.contains(&transform_idx),
        "deferred region members must include the downstream Transform"
    );
    assert!(
        region.outputs.contains(&output_idx),
        "deferred region outputs must include the correlation-buffered Output"
    );

    // `is_deferred_consumer` flips true for member + output (so their
    // arms short-circuit) but stays false for the producer (which still
    // runs its aggregation kernel, then projects to buffer_schema).
    assert!(
        plan.is_deferred_consumer(transform_idx),
        "Transform should be flagged as deferred consumer"
    );
    assert!(
        plan.is_deferred_consumer(output_idx),
        "Output should be flagged as deferred consumer"
    );
    assert!(
        !plan.is_deferred_consumer(agg_idx),
        "Aggregate (the producer) must NOT be flagged as deferred consumer"
    );

    // The buffer_schema is the column-pruned union the deferred
    // operators reach. The Transform reads `department` and `total`,
    // so both must appear — and the schema is sorted alphabetically
    // for deterministic --explain rendering.
    let schema_set: BTreeSet<&str> = region.buffer_schema.iter().map(String::as_str).collect();
    assert!(
        schema_set.contains("department"),
        "buffer_schema must retain 'department': {:?}",
        region.buffer_schema
    );
    assert!(
        schema_set.contains("total"),
        "buffer_schema must retain 'total': {:?}",
        region.buffer_schema
    );
    let mut sorted = region.buffer_schema.clone();
    sorted.sort();
    assert_eq!(
        region.buffer_schema, sorted,
        "buffer_schema must be sorted alphabetically"
    );
}

/// Runtime side: with the deferred-dispatch short-circuit in place,
/// running the pipeline end-to-end produces an empty writer payload —
/// the deferred Transform and Output arms return without admitting any
/// row. A follow-up commit's commit-time deferred dispatcher will
/// re-run the deferred arms and land records in the writer; this test
/// locks the forward-pass intermediate.
///
/// Counters reflect the same: no DLQ entries (no errors fired), no
/// records written by the strict commit body (correlation_buffers are
/// empty because the Output arm short-circuited before admitting
/// anything).
#[test]
fn deferred_consumers_emit_nothing_on_the_forward_pass() {
    let config = crate::config::parse_config(DEFERRED_PIPELINE).expect("parse");
    let primary = "src".to_string();
    let csv = "\
order_id,department,amount
o1,HR,10
o2,HR,20
o3,ENG,100
";
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
    let report =
        PipelineExecutor::run_with_readers_writers(&config, &primary, readers, writers, &params)
            .expect("pipeline must run without error");

    let written = buf.as_string();
    // The deferred Output's arm short-circuited — no projected rows
    // landed in `correlation_buffers`, so the strict-commit flush has
    // nothing to drain. The body is empty (the writer never opened
    // because no record reached `build_format_writer`).
    let body_lines: Vec<&str> = written.lines().collect();
    assert!(
        body_lines.is_empty(),
        "deferred-dispatch forward pass must not write to the output sink \
         (commit-time re-dispatch lands in a follow-up commit); got {body_lines:?}"
    );
    assert_eq!(
        report.counters.records_written, 0,
        "deferred Output writes zero rows on the forward pass; got {}",
        report.counters.records_written
    );
    assert_eq!(
        report.counters.dlq_count, 0,
        "no upstream errors expected in the smoke fixture; got {}",
        report.counters.dlq_count
    );
}
