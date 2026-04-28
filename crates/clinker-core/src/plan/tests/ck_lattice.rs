//! Per-node CK-set lattice tests.
//!
//! Walks the lattice rules `compute_one` applies to every variant —
//! Source, Transform, Aggregate (strict + relaxed), Combine (driver /
//! all / named), Merge — and asserts the `NodeProperties.ck_set`
//! computed for every node in a fixture pipeline.
//!
//! Tests construct `PipelineConfig` from inline YAML to stay consistent
//! with the established planner-test idiom in `plan/tests/dag.rs`.

use std::collections::BTreeSet;

use crate::config::{CompileContext, PipelineConfig, parse_config};
use crate::plan::execution::PlanNode;
use crate::plan::properties::NodeProperties;

fn compile(yaml: &str) -> crate::plan::execution::ExecutionPlanDag {
    let config: PipelineConfig = parse_config(yaml).expect("parse");
    config
        .compile(&CompileContext::default())
        .expect("compile")
        .dag()
        .clone()
}

fn ck_set_for(
    plan: &crate::plan::execution::ExecutionPlanDag,
    node_name: &str,
) -> BTreeSet<String> {
    for idx in plan.graph.node_indices() {
        if plan.graph[idx].name() == node_name {
            return plan
                .node_properties
                .get(&idx)
                .map(|p: &NodeProperties| p.ck_set.clone())
                .unwrap_or_default();
        }
    }
    panic!("node {node_name:?} not found in plan");
}

fn set_of(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|s| s.to_string()).collect()
}

/// Strict mode: aggregate group_by covers the correlation key, so the
/// CK set is preserved unchanged at every node downstream of the
/// aggregate.
#[test]
fn ck_lattice_strict_aggregate_preserves_set() {
    let yaml = r#"
pipeline:
  name: ck_lattice_strict
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: int }
  - type: transform
    name: identity
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit amount = amount
  - type: aggregate
    name: per_order
    input: identity
    config:
      group_by: [order_id]
      cxl: |
        emit total = sum(amount)
  - type: output
    name: out
    input: per_order
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile(yaml);
    let expected = set_of(&["order_id"]);
    assert_eq!(ck_set_for(&plan, "orders"), expected);
    assert_eq!(ck_set_for(&plan, "identity"), expected);
    assert_eq!(ck_set_for(&plan, "per_order"), expected);
    assert_eq!(ck_set_for(&plan, "out"), expected);
}

/// Relaxed mode: aggregate omits the CK field from `group_by`, so the
/// engine routes it through the retraction protocol and that field
/// disappears from the downstream CK set.
#[test]
fn ck_lattice_relaxed_aggregate_drops_omitted_ck_field() {
    let yaml = r#"
pipeline:
  name: ck_lattice_relaxed
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: transform
    name: identity
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit department = department
        emit amount = amount
  - type: aggregate
    name: dept_totals
    input: identity
    config:
      group_by: [department]
      cxl: |
        emit total = sum(amount)
  - type: output
    name: out
    input: dept_totals
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile(yaml);
    assert_eq!(ck_set_for(&plan, "orders"), set_of(&["order_id"]));
    assert_eq!(ck_set_for(&plan, "identity"), set_of(&["order_id"]));
    // Relaxed aggregate omitted `order_id` from group_by — CK set drops it.
    assert_eq!(ck_set_for(&plan, "dept_totals"), BTreeSet::new());
    assert_eq!(ck_set_for(&plan, "out"), BTreeSet::new());
}

/// `propagate_ck: driver` keeps only the driver input's CK set on the
/// combine output. The build-side input contributes fields but its CK
/// columns do not propagate.
#[test]
fn ck_lattice_combine_driver_only() {
    let yaml = r#"
pipeline:
  name: ck_lattice_combine_driver
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: amount, type: int }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
  - type: combine
    name: enriched
    input:
      o: orders
      p: products
    config:
      where: "o.product_id == p.product_id"
      match: first
      on_miss: skip
      cxl: |
        emit order_id = o.order_id
        emit product_id = o.product_id
        emit amount = o.amount
        emit name = p.name
      propagate_ck: driver
  - type: output
    name: out
    input: enriched
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile(yaml);
    let expected = set_of(&["order_id"]);
    assert_eq!(ck_set_for(&plan, "orders"), expected);
    // Per-source CK: `products` declares no `correlation_key:`, so its
    // lattice set is empty.
    assert_eq!(ck_set_for(&plan, "products"), BTreeSet::new());
    assert_eq!(ck_set_for(&plan, "enriched"), expected);
    assert_eq!(ck_set_for(&plan, "out"), expected);
}

/// `propagate_ck: all` unions every parent's CK set on the combine
/// output. With per-source CK, each source declares its own key
/// independently — `orders` carries `order_id` and `products` carries
/// `product_id`, so the combine's union is `{order_id, product_id}`.
#[test]
fn ck_lattice_combine_all() {
    let yaml = r#"
pipeline:
  name: ck_lattice_combine_all
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      correlation_key: product_id
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
  - type: combine
    name: enriched
    input:
      o: orders
      p: products
    config:
      where: "o.product_id == p.product_id"
      match: first
      on_miss: skip
      cxl: |
        emit order_id = o.order_id
        emit product_id = o.product_id
        emit name = p.name
      propagate_ck: all
  - type: output
    name: out
    input: enriched
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile(yaml);
    assert_eq!(
        ck_set_for(&plan, "enriched"),
        set_of(&["order_id", "product_id"])
    );
    assert_eq!(
        ck_set_for(&plan, "out"),
        set_of(&["order_id", "product_id"])
    );
}

/// `propagate_ck: { named: [...] }` selects an explicit subset
/// intersected with the upstream union. Named fields that don't appear
/// in any parent's CK set are silently dropped at this lattice level —
/// the validation that flags an entirely unreachable named field is a
/// future concern of the relaxation runtime.
#[test]
fn ck_lattice_combine_named_intersects_with_upstream_union() {
    let yaml = r#"
pipeline:
  name: ck_lattice_combine_named
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: [order_id, customer_id]
      schema:
        - { name: order_id, type: string }
        - { name: customer_id, type: string }
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: products.csv
      correlation_key: [order_id, customer_id]
      schema:
        - { name: order_id, type: string }
        - { name: customer_id, type: string }
        - { name: product_id, type: string }
        - { name: name, type: string }
  - type: combine
    name: enriched
    input:
      o: orders
      p: products
    config:
      where: "o.product_id == p.product_id"
      match: first
      on_miss: skip
      cxl: |
        emit order_id = o.order_id
        emit customer_id = o.customer_id
        emit name = p.name
      propagate_ck:
        named: [order_id]
  - type: output
    name: out
    input: enriched
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile(yaml);
    // Named subset of {order_id, customer_id} — only `order_id` carries
    // through, even though both inputs see both CK fields.
    assert_eq!(ck_set_for(&plan, "enriched"), set_of(&["order_id"]));
}

/// Merge intersects parent CK sets. Both upstream branches in this
/// fixture see the same single CK field, so the intersection equals
/// either parent's set.
#[test]
fn ck_lattice_merge_intersects() {
    let yaml = r#"
pipeline:
  name: ck_lattice_merge
error_handling:
  strategy: continue
nodes:
  - type: source
    name: east
    config:
      name: east
      type: csv
      path: east.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: int }
  - type: source
    name: west
    config:
      name: west
      type: csv
      path: west.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: int }
  - type: merge
    name: all_orders
    inputs: [east, west]
  - type: output
    name: out
    input: all_orders
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile(yaml);
    let expected = set_of(&["order_id"]);
    assert_eq!(ck_set_for(&plan, "east"), expected);
    assert_eq!(ck_set_for(&plan, "west"), expected);
    assert_eq!(ck_set_for(&plan, "all_orders"), expected);
    assert_eq!(ck_set_for(&plan, "out"), expected);
}

/// E15Y: an aggregate whose `group_by` omits a correlation-key field
/// (so the engine routes it through retraction mode) plus
/// `strategy: streaming` fails at compile time. Streaming aggregates
/// emit at group-boundary close, before the terminal correlation
/// commit, defeating the rollback window the retraction protocol needs.
#[test]
fn ck_lattice_relaxed_plus_streaming_emits_e15y() {
    let yaml = r#"
pipeline:
  name: e15y_gate
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: order_id
      sort_order: [department]
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: dept_totals
    input: orders
    config:
      group_by: [department]
      strategy: streaming
      cxl: |
        emit total = sum(amount)
  - type: output
    name: out
    input: dept_totals
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config: PipelineConfig = parse_config(yaml).expect("parse");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("E15Y must reject retraction-mode aggregate + streaming");
    assert!(
        diags.iter().any(|d| d.code == "E15Y"),
        "expected an E15Y diagnostic, got: {:?}",
        diags.iter().map(|d| &d.code).collect::<Vec<_>>()
    );
}

/// A pipeline without `error_handling.correlation_key` carries an empty
/// CK set at every node — there is no shadow column to track.
#[test]
fn ck_lattice_no_correlation_key_means_empty_set() {
    let yaml = r#"
pipeline:
  name: no_ck
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: int }
  - type: transform
    name: identity
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit amount = amount
  - type: aggregate
    name: by_id
    input: identity
    config:
      group_by: [order_id]
      cxl: |
        emit total = sum(amount)
  - type: output
    name: out
    input: by_id
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile(yaml);
    for name in ["orders", "identity", "by_id", "out"] {
        assert_eq!(
            ck_set_for(&plan, name),
            BTreeSet::new(),
            "node {name} should carry empty CK set"
        );
    }
}

// Defensive coverage: make sure every node in every fixture above
// actually has a NodeProperties entry. A missing entry would silently
// short-circuit `ck_set_for` to an empty set and hide a regression.
//
// Worth: catches the failure mode where a future planner pass forgets
// to extend `node_properties` after rewriting the graph.
fn assert_every_node_has_properties(plan: &crate::plan::execution::ExecutionPlanDag) {
    for idx in plan.graph.node_indices() {
        // CorrelationCommit is planner-injected AFTER compute_node_properties
        // runs, so its entry is not populated. Skip it — the lattice
        // never participates in a CK chain past the commit boundary.
        if matches!(plan.graph[idx], PlanNode::CorrelationCommit { .. }) {
            continue;
        }
        assert!(
            plan.node_properties.contains_key(&idx),
            "node {} ({}) has no NodeProperties entry",
            plan.graph[idx].name(),
            match &plan.graph[idx] {
                PlanNode::Source { .. } => "source",
                PlanNode::Transform { .. } => "transform",
                PlanNode::Sort { .. } => "sort",
                PlanNode::Aggregation { .. } => "aggregation",
                PlanNode::Route { .. } => "route",
                PlanNode::Merge { .. } => "merge",
                PlanNode::Combine { .. } => "combine",
                PlanNode::Output { .. } => "output",
                PlanNode::Composition { .. } => "composition",
                PlanNode::CorrelationCommit { .. } => "correlation_commit",
            }
        );
    }
}

#[test]
fn every_node_has_node_properties_strict_pipeline() {
    let yaml = r#"
pipeline:
  name: properties_coverage
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: per_order
    input: orders
    config:
      group_by: [order_id]
      cxl: |
        emit total = sum(amount)
  - type: output
    name: out
    input: per_order
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile(yaml);
    assert_every_node_has_properties(&plan);
}
