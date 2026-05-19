//! CK-aligned partition: `requires_buffer_recompute = false`.
//!
//! When a windowed Transform's `partition_by` covers every member of
//! the upstream's correlation-key set (including any synthetic
//! `$ck.aggregate.<name>` column emitted by a relaxed aggregate), every
//! retraction event lands in at most one partition. The auto-flip in
//! `derive_window_buffer_recompute_flags` keeps the spec on the
//! streaming-emit path. This test pins the plan-time decision; the
//! runtime counterpart lives in
//! `executor/tests/ck_aligned_partition_runtime.rs`.

use crate::config::{CompileContext, parse_config};
use crate::plan::execution::PlanNode;

#[test]
fn ck_aligned_partition_does_not_auto_flip_to_buffer_recompute() {
    let yaml = r#"
pipeline:
  name: ck_aligned_partition
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
- type: output
  name: out
  input: ck_aligned_window
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let dag = plan.dag();

    // Locate the windowed Transform's IndexSpec.
    let mut window_idx_num = None;
    for idx in dag.graph.node_indices() {
        if let PlanNode::Transform {
            name, window_index, ..
        } = &dag.graph[idx]
            && name == "ck_aligned_window"
        {
            window_idx_num = *window_index;
        }
    }
    let idx_num = window_idx_num.expect("ck_aligned_window has window_index");
    let spec = &dag.indices_to_build[idx_num];

    // The CK-aligned partition (partition_by ⊇ ck_set) keeps the
    // streaming-emit path — `requires_buffer_recompute` stays false.
    assert!(
        !spec.requires_buffer_recompute,
        "CK-aligned partition_by (covers $ck.aggregate.dept_totals) \
         must NOT trigger the buffer-recompute auto-flip"
    );
    assert!(
        spec.group_by
            .contains(&"$ck.aggregate.dept_totals".to_string()),
        "spec must carry the synthetic CK column in its group_by; \
         got: {:?}",
        spec.group_by
    );
}

#[test]
fn non_ck_aligned_partition_does_auto_flip_to_buffer_recompute() {
    // Same upstream but partition_by drops the synthetic CK column —
    // the relaxed-aggregate's $ck.aggregate.dept_totals lives in the
    // post-aggregate ck_set but NOT in the window's partition_by, so
    // the planner auto-flips into buffer-recompute mode.
    let yaml = r#"
pipeline:
  name: non_ck_aligned_partition
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
  name: dept_window
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
  input: dept_window
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let dag = plan.dag();
    let idx_num = dag
        .graph
        .node_indices()
        .find_map(|i| match &dag.graph[i] {
            PlanNode::Transform {
                name, window_index, ..
            } if name == "dept_window" => *window_index,
            _ => None,
        })
        .expect("dept_window has window_index");
    let spec = &dag.indices_to_build[idx_num];

    assert!(
        spec.requires_buffer_recompute,
        "partition_by missing the synthetic CK column must auto-flip \
         to buffer-recompute mode (the contrast assertion to the CK-aligned \
         case above)"
    );
}

/// CK-aligned aggregate (group_by ⊇ ck_set): the planner classifies the
/// aggregate as strict and registers no deferred region, so the
/// commit-time deferred dispatcher never runs and the executor stays on
/// the FastPath. Pins the architectural complement to
/// `non_ck_aligned_partition_does_auto_flip_to_buffer_recompute`: the
/// upstream aggregate's `group_by` covers every CK field, so retraction
/// bookkeeping is unnecessary.
#[test]
fn ck_aligned_aggregate_skips_deferred_region() {
    let yaml = r#"
pipeline:
  name: ck_aligned_aggregate
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
    let config = parse_config(yaml).expect("parse");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let dag = plan.dag();

    assert!(
        dag.deferred_regions.is_empty(),
        "CK-aligned aggregate (group_by ⊇ ck_set) must not seed any deferred region; \
         got {:?}",
        dag.deferred_regions.keys().collect::<Vec<_>>()
    );
}
