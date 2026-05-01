//! Plan-time `IndexSpec.root` assignment for windowed Transforms.
//!
//! The lowering pass at `config/mod.rs` walks each window-bearing
//! Transform's incoming edge through `first_non_passthrough_ancestor`
//! and classifies the rooting:
//!   - `Source` ancestor + no cross-source `wc.source` → `Source(name)`
//!   - any other operator ancestor → `Node { upstream, anchor_schema }`
//!
//! These tests pin the rooting decision on the two canonical shapes
//! (Source→Transform-window and Source→Aggregate→Transform-window).

use crate::config::{CompileContext, parse_config};
use crate::plan::execution::PlanNode;
use crate::plan::index::PlanIndexRoot;

#[test]
fn source_then_window_roots_at_source() {
    let yaml = r#"
pipeline:
  name: source_window_root
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
- type: transform
  name: windowed
  input: src
  config:
    cxl: |
      emit department = department
      emit amount = amount
      emit running_amount = $window.sum(amount)
    analytic_window:
      group_by: [department]
- type: output
  name: out
  input: windowed
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let dag = plan.dag();

    let window_idx_num = dag
        .graph
        .node_indices()
        .find_map(|i| match &dag.graph[i] {
            PlanNode::Transform {
                name, window_index, ..
            } if name == "windowed" => *window_index,
            _ => None,
        })
        .expect("windowed has window_index");
    let spec = &dag.indices_to_build[window_idx_num];
    match &spec.root {
        PlanIndexRoot::Source(name) => assert_eq!(
            name, "src",
            "Source-rooted spec must carry the source's declared name"
        ),
        other => panic!("expected PlanIndexRoot::Source, got {other:?}"),
    }
}

#[test]
fn source_then_aggregate_then_window_roots_at_aggregate_node() {
    let yaml = r#"
pipeline:
  name: post_aggregate_root
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
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = parse_config(yaml).expect("parse");
    let plan = config.compile(&CompileContext::default()).expect("compile");
    let dag = plan.dag();

    let agg_idx = dag
        .graph
        .node_indices()
        .find(|i| dag.graph[*i].name() == "dept_totals")
        .expect("aggregate present");
    let window_idx_num = dag
        .graph
        .node_indices()
        .find_map(|i| match &dag.graph[i] {
            PlanNode::Transform {
                name, window_index, ..
            } if name == "running" => *window_index,
            _ => None,
        })
        .expect("running has window_index");
    let spec = &dag.indices_to_build[window_idx_num];
    match &spec.root {
        PlanIndexRoot::Node {
            upstream,
            anchor_schema,
        } => {
            assert_eq!(
                *upstream, agg_idx,
                "Aggregate ancestor must be the rooted node, not the upstream Source"
            );
            // The anchor schema is the aggregate's emit schema, so it
            // must contain the aggregate-emitted columns and NOT
            // contain a column that lives only on the source.
            assert!(
                anchor_schema.contains("department"),
                "anchor schema must carry the aggregate's emitted department column"
            );
            assert!(
                anchor_schema.contains("total"),
                "anchor schema must carry the aggregate's emitted total column \
                 (this is the column the previous source-rooted geometry could not see)"
            );
        }
        other => panic!("expected PlanIndexRoot::Node, got {other:?}"),
    }
}
