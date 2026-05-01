//! Dedup-by-root: two windowed Transforms downstream of the same
//! Aggregate share a single IndexSpec.
//!
//! `deduplicate_indices` keys by `(root, group_by, sort_by)` and unions
//! the per-Transform `arena_fields` sets. Two Transforms whose windows
//! root at the same node and declare the same partition keys collapse
//! to one IndexSpec carrying the merged `arena_fields`.

use crate::config::{CompileContext, parse_config};
use crate::plan::execution::PlanNode;
use crate::plan::index::PlanIndexRoot;

/// Two Transforms downstream of the same Aggregate, both
/// `partition_by: [department]`, one referencing `total`, one
/// referencing `count`. After dedup the plan has one IndexSpec rooted
/// at the Aggregate with `arena_fields = ["count", "department",
/// "total"]` (sorted) and both Transforms point at the same
/// `window_index`.
#[test]
fn two_post_aggregate_windows_at_same_root_share_one_index_spec() {
    let yaml = r#"
pipeline:
  name: dedup_node_rooted
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
  name: dept_stats
  input: src
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount)
      emit count = count(*)
- type: transform
  name: total_window
  input: dept_stats
  config:
    cxl: |
      emit department = department
      emit total = total
      emit running_total = $window.sum(total)
    analytic_window:
      group_by: [department]
- type: transform
  name: count_window
  input: dept_stats
  config:
    cxl: |
      emit department = department
      emit count = count
      emit running_count = $window.sum(count)
    analytic_window:
      group_by: [department]
- type: output
  name: out_total
  input: total_window
  config:
    name: out_total
    path: total.csv
    type: csv
    include_unmapped: true
- type: output
  name: out_count
  input: count_window
  config:
    name: out_count
    path: count.csv
    type: csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile must succeed");
    let dag = plan.dag();

    // Locate the Aggregate's NodeIndex and both Transforms'
    // `window_index`.
    let mut agg_idx = None;
    let mut total_window = None;
    let mut count_window = None;
    for idx in dag.graph.node_indices() {
        match &dag.graph[idx] {
            PlanNode::Aggregation { name, .. } if name == "dept_stats" => agg_idx = Some(idx),
            PlanNode::Transform {
                name, window_index, ..
            } => {
                if name == "total_window" {
                    total_window = *window_index;
                } else if name == "count_window" {
                    count_window = *window_index;
                }
            }
            _ => {}
        }
    }
    let agg_idx = agg_idx.expect("aggregate present");
    let total_window = total_window.expect("total_window has window_index");
    let count_window = count_window.expect("count_window has window_index");

    // Same window_index → shared IndexSpec.
    assert_eq!(
        total_window, count_window,
        "two transforms with identical (root, group_by, sort_by) must \
         share one window_index after deduplicate_indices"
    );

    // Spec is rooted at the Aggregate.
    let spec = &dag.indices_to_build[total_window];
    match &spec.root {
        PlanIndexRoot::Node { upstream, .. } => assert_eq!(
            *upstream, agg_idx,
            "shared IndexSpec must root at the upstream Aggregate"
        ),
        other => panic!("expected PlanIndexRoot::Node, got {other:?}"),
    }
    assert_eq!(
        spec.group_by,
        vec!["department".to_string()],
        "dedup must preserve identical group_by"
    );

    // arena_fields = sorted union {department, total, count}.
    let mut expected = vec![
        "count".to_string(),
        "department".to_string(),
        "total".to_string(),
    ];
    expected.sort();
    let mut actual = spec.arena_fields.clone();
    actual.sort();
    assert_eq!(
        actual, expected,
        "merged arena_fields must be the sorted union of every \
         contributing transform's referenced fields"
    );

    // Exactly one IndexSpec rooted at this Aggregate (no duplication).
    let count_at_agg = dag
        .indices_to_build
        .iter()
        .filter(|s| matches!(&s.root, PlanIndexRoot::Node { upstream, .. } if *upstream == agg_idx))
        .count();
    assert_eq!(
        count_at_agg, 1,
        "exactly one IndexSpec rooted at the Aggregate after dedup"
    );
}
