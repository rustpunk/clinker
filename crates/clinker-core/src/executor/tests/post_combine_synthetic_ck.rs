//! Synthetic-CK propagation through Combine into a downstream window.
//!
//! Source(CK) → Aggregate(relaxed) → Combine(driver) → Transform(window
//! references `$ck.aggregate.<name>`). The combine's output schema
//! carries the synthetic CK column the upstream relaxed aggregate
//! emitted; a window referencing that column in `partition_by` must NOT
//! raise E150b (the column is in the combine's `anchor_schema`), and
//! the resulting partition is CK-aligned so `requires_buffer_recompute`
//! stays false.

use crate::plan::execution::PlanNode;

/// Compile a Source→Aggregate(relaxed)→Combine→Transform(window with
/// synthetic-CK in partition_by) pipeline; assert the synthetic CK
/// column is present on the combine's output schema and the windowed
/// Transform compiles cleanly.
#[test]
fn post_combine_window_partition_by_synthetic_ck_compiles() {
    let yaml = r#"
pipeline:
  name: post_combine_synthetic_ck
error_handling:
  strategy: continue
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    correlation_key: order_id
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: dept_totals
  input: orders
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount)
- type: source
  name: lookup
  config:
    name: lookup
    path: lookup.csv
    type: csv
    schema:
      - { name: department, type: string }
      - { name: budget, type: int }
- type: combine
  name: enriched
  input:
    a: dept_totals
    l: lookup
  config:
    where: "a.department == l.department"
    match: first
    on_miss: skip
    cxl: |
      emit department = a.department
      emit total = a.total
      emit budget = l.budget
    propagate_ck: driver
- type: transform
  name: running
  input: enriched
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
    analytic_window:
      group_by: [department, '$ck.aggregate.dept_totals']
- type: output
  name: out
  input: running
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = crate::config::parse_config(yaml).expect("parse");
    let plan = config
        .compile(&crate::config::CompileContext::default())
        .expect("post-combine window referencing $ck.aggregate.<name> must compile");

    // Combine's output schema carries the synthetic CK column.
    let dag = plan.dag();
    let combine_idx = dag
        .graph
        .node_indices()
        .find(|i| dag.graph[*i].name() == "enriched")
        .expect("combine node present");
    if let PlanNode::Combine { output_schema, .. } = &dag.graph[combine_idx] {
        let cols: Vec<String> = output_schema
            .columns()
            .iter()
            .map(|c| c.to_string())
            .collect();
        assert!(
            cols.iter().any(|c| c == "$ck.aggregate.dept_totals"),
            "combine output schema must carry synthetic CK column from \
             upstream relaxed aggregate; got: {cols:?}"
        );
    } else {
        panic!("combine node 'enriched' is not a PlanNode::Combine");
    }

    // The windowed Transform's `partition_by` includes
    // `$ck.aggregate.dept_totals`, so the partition is CK-aligned and
    // `requires_buffer_recompute` is false (no auto-flip).
    let transform_idx = dag
        .graph
        .node_indices()
        .find(|i| dag.graph[*i].name() == "running")
        .expect("transform node present");
    let window_idx = match &dag.graph[transform_idx] {
        PlanNode::Transform { window_index, .. } => *window_index,
        _ => panic!("'running' is not a PlanNode::Transform"),
    };
    let idx_num = window_idx.expect("running carries a window_index");
    let spec = &dag.indices_to_build[idx_num];
    assert!(
        !spec.requires_buffer_recompute,
        "CK-aligned partition (partition_by includes \
         $ck.aggregate.dept_totals) must NOT auto-flip into \
         buffer-recompute mode"
    );
}
