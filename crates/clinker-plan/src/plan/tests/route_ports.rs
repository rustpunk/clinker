//! Producer-side output-port tagging on Route fan-out edges.
//!
//! A Route declares one named output port per branch plus its default.
//! Each outgoing edge is tagged with [`crate::plan::execution::PlanEdge`]'s
//! `producer_port` so the dispatcher routes records by output port off the
//! live graph. These gates pin the tagging contract for both reference
//! shapes a consumer can use to draw from a branch.

use super::dag::parse_fixture;
use crate::config::CompileContext;
use crate::plan::execution::PlanNode;
use petgraph::Direction;
use petgraph::visit::EdgeRef;

/// Compile a fixture and return the producer-port-tagged targets of the
/// single Route node, as `(producer_port, consumer_id_slug)` pairs sorted
/// for deterministic comparison. An untagged outgoing edge surfaces as a
/// `None` producer port so a wiring regression is visible rather than
/// silently dropped.
fn route_port_targets(yaml: &str) -> Vec<(Option<String>, String)> {
    let config = parse_fixture(yaml);
    let dag = config
        .compile(&CompileContext::default())
        .expect("compile")
        .dag()
        .clone();
    let route_idx = dag
        .graph
        .node_indices()
        .find(|&i| matches!(dag.graph[i], PlanNode::Route { .. }))
        .expect("fixture has a Route node");
    let mut out: Vec<(Option<String>, String)> = dag
        .graph
        .edges_directed(route_idx, Direction::Outgoing)
        .map(|e| {
            (
                e.weight().producer_port.clone(),
                dag.graph[e.target()].id_slug(),
            )
        })
        .collect();
    out.sort();
    out
}

const SOURCE_AND_CLASSIFY: &str = r#"pipeline:
  name: route_ports
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: string }
  - type: transform
    name: classify
    input: orders
    config:
      cxl: |
        emit id = id
        emit amount = amount
"#;

/// Port-form references (`<route>.<branch>`) tag every Route outgoing
/// edge with the branch — including the default — it draws from.
#[test]
fn port_form_references_tag_each_branch() {
    let yaml = format!(
        "{SOURCE_AND_CLASSIFY}{}",
        r#"  - type: route
    name: split
    input: classify
    config:
      mode: exclusive
      conditions:
        high: "amount.to_int() > 100"
      default: low
  - type: output
    name: high_out
    input: split.high
    config:
      name: high_out
      type: csv
      path: high.csv
  - type: output
    name: low_out
    input: split.low
    config:
      name: low_out
      type: csv
      path: low.csv
"#
    );

    assert_eq!(
        route_port_targets(&yaml),
        vec![
            (Some("high".to_string()), "output.high_out".to_string()),
            (Some("low".to_string()), "output.low_out".to_string()),
        ],
    );
}

/// Bare references (`input: <route>`) on consumers whose own name equals a
/// branch tag the edge with that branch — the one-consumer-per-branch
/// shorthand, with the default reachable the same way.
#[test]
fn bare_name_references_tag_the_matching_branch() {
    let yaml = format!(
        "{SOURCE_AND_CLASSIFY}{}",
        r#"  - type: route
    name: split
    input: classify
    config:
      mode: exclusive
      conditions:
        high: "amount.to_int() > 100"
      default: low
  - type: output
    name: high
    input: split
    config:
      name: high
      type: csv
      path: high.csv
  - type: output
    name: low
    input: split
    config:
      name: low
      type: csv
      path: low.csv
"#
    );

    assert_eq!(
        route_port_targets(&yaml),
        vec![
            (Some("high".to_string()), "output.high".to_string()),
            (Some("low".to_string()), "output.low".to_string()),
        ],
    );
}

/// A single branch consumed by more than one downstream node tags every
/// such edge, so the dispatcher fans one branch to all its consumers.
#[test]
fn one_branch_fans_to_multiple_consumers() {
    let yaml = format!(
        "{SOURCE_AND_CLASSIFY}{}",
        r#"  - type: route
    name: split
    input: classify
    config:
      mode: exclusive
      conditions:
        high: "amount.to_int() > 100"
      default: low
  - type: output
    name: high_a
    input: split.high
    config:
      name: high_a
      type: csv
      path: high_a.csv
  - type: output
    name: high_b
    input: split.high
    config:
      name: high_b
      type: csv
      path: high_b.csv
  - type: output
    name: low_out
    input: split.low
    config:
      name: low_out
      type: csv
      path: low.csv
"#
    );

    assert_eq!(
        route_port_targets(&yaml),
        vec![
            (Some("high".to_string()), "output.high_a".to_string()),
            (Some("high".to_string()), "output.high_b".to_string()),
            (Some("low".to_string()), "output.low_out".to_string()),
        ],
    );
}
