//! Under `dlq_granularity: document` the compiled order puts every Sink
//! after every other node, so each document's verdict is final before any
//! Sink writes. The same pipeline under `dlq_granularity: record` keeps
//! the order the topological sort gives it.

use super::dag::parse_fixture;
use crate::config::CompileContext;
use crate::plan::execution::{ExecutionPlanDag, PlanNode};
use petgraph::graph::NodeIndex;

/// Two sibling branches off one Source: `t2` condemns a document and feeds
/// `out2`; `t1` passes every record to `out1`. `{granularity}` is the
/// Source's `dlq_granularity`.
const SIBLING_BRANCHES_YAML: &str = r#"
pipeline: { name: sibling_branches }
error_handling: { strategy: continue, dlq: { path: rejected.csv } }
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      dlq_granularity: {granularity}
      files: { on_no_match: skip }
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
  - type: transform
    name: t2
    input: events
    config:
      cxl: |
        emit id = id
        emit val = value.to_int()
  - type: sink
    name: out2
    input: t2
    config: { name: out2, type: csv, path: out2.csv, include_unmapped: true }
  - type: transform
    name: t1
    input: events
    config:
      cxl: |
        emit id = id
        emit val = value
  - type: sink
    name: out1
    input: t1
    config: { name: out1, type: csv, path: out1.csv, include_unmapped: true }
"#;

fn compile(granularity: &str) -> ExecutionPlanDag {
    let yaml = SIBLING_BRANCHES_YAML.replace("{granularity}", granularity);
    parse_fixture(&yaml)
        .compile(&CompileContext::default())
        .expect("compile")
        .dag()
        .clone()
}

fn is_sink(dag: &ExecutionPlanDag, idx: NodeIndex) -> bool {
    matches!(dag.graph[idx], PlanNode::Sink { .. })
}

fn names(dag: &ExecutionPlanDag, order: impl IntoIterator<Item = NodeIndex>) -> Vec<String> {
    order
        .into_iter()
        .map(|idx| dag.graph[idx].name().to_owned())
        .collect()
}

fn position(dag: &ExecutionPlanDag, name: &str) -> usize {
    dag.topo_order
        .iter()
        .position(|&idx| dag.graph[idx].name() == name)
        .unwrap_or_else(|| panic!("no node named {name:?} in topo_order"))
}

#[test]
fn document_policy_orders_sinks_after_operators() {
    let dag = compile("document");
    let last_operator = dag
        .topo_order
        .iter()
        .rposition(|&idx| !is_sink(&dag, idx))
        .expect("the pipeline has operators");
    let first_sink = dag
        .topo_order
        .iter()
        .position(|&idx| is_sink(&dag, idx))
        .expect("the pipeline has Sinks");
    assert!(
        last_operator < first_sink,
        "every Sink follows every other node, got {:?}",
        names(&dag, dag.topo_order.iter().copied())
    );

    let at = |idx: NodeIndex| dag.topo_order.iter().position(|&n| n == idx).unwrap();
    for edge in dag.graph.edge_indices() {
        let (from, to) = dag.graph.edge_endpoints(edge).expect("edge endpoints");
        assert!(at(from) < at(to), "every edge points forward");
    }

    let record = compile("record");
    let operators = |dag: &ExecutionPlanDag| {
        let order: Vec<NodeIndex> = dag
            .topo_order
            .iter()
            .copied()
            .filter(|&idx| !is_sink(dag, idx))
            .collect();
        names(dag, order)
    };
    assert_eq!(
        operators(&dag),
        operators(&record),
        "the non-Sinks keep the relative order they have under record granularity"
    );
}

/// Under `record` the topological sort visits `t1` and `out1` before `t2`.
/// This is the order in which a Sink would publish a document its sibling
/// branch later condemns, so it pins the shape the document barrier exists
/// for.
#[test]
fn record_policy_keeps_the_topological_order() {
    let dag = compile("record");
    assert!(
        position(&dag, "out1") < position(&dag, "t2"),
        "under record granularity out1 precedes t2, got {:?}",
        names(&dag, dag.topo_order.iter().copied())
    );
}
