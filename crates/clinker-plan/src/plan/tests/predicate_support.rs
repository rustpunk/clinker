//! Structured per-predicate input-column support
//! ([`crate::plan::predicate_support`]).
//!
//! These pin that the accessor recovers the columns each control-flow /
//! grouping predicate reads from the node's retained typechecked forms —
//! Route branch programs, the Combine decomposed `where:` predicate, and
//! the Cull OR-combined decision aggregate — matching what a re-parse of
//! the raw CXL would yield, but without re-parsing. Qualified `input.field`
//! references survive for Combine; the Cull case exercises the aggregate
//! path where a simple-field arg (`sum(amount)`) carries only a schema
//! index on the binding, not a name.

use std::collections::BTreeSet;

use crate::config::{CompileContext, parse_config};
use crate::plan::execution::PlanNode;
use crate::plan::predicate_support::{PredicateSupport, predicate_support};

/// Compile `yaml` to a `CompiledPlan`, panicking on any diagnostic.
fn compile_ok(yaml: &str) -> crate::plan::CompiledPlan {
    parse_config(yaml)
        .expect("parse_config")
        .compile(&CompileContext::default())
        .expect("compile should succeed")
}

/// Borrow the top-level node named `name` from a compiled plan.
fn node<'a>(compiled: &'a crate::plan::CompiledPlan, name: &str) -> &'a PlanNode {
    let g = &compiled.dag().graph;
    let idx = g
        .node_indices()
        .find(|&i| g[i].name() == name)
        .unwrap_or_else(|| panic!("node {name:?} present"));
    &g[idx]
}

/// Build a `BTreeSet<String>` from string literals for terse assertions.
fn cols(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| s.to_string()).collect()
}

/// A multi-branch Route exposes one support set per branch, aligned with
/// the node's `branches` order, each holding exactly the columns that
/// branch's condition reads.
#[test]
fn route_branch_support_per_branch() {
    let yaml = r#"
pipeline:
  name: route_support
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: route
    name: split
    input: src
    config:
      mode: exclusive
      conditions:
        high: "amount > 100"
        flagged: "status == 'urgent'"
      default: low
  - type: output
    name: high_out
    input: split.high
    config:
      name: high_out
      type: csv
      path: high.csv
  - type: output
    name: flagged_out
    input: split.flagged
    config:
      name: flagged_out
      type: csv
      path: flagged.csv
  - type: output
    name: low_out
    input: split.low
    config:
      name: low_out
      type: csv
      path: low.csv
"#;
    let compiled = compile_ok(yaml);
    let route = node(&compiled, "split");
    let PlanNode::Route { branches, .. } = route else {
        panic!("`split` is not a Route");
    };
    let Some(PredicateSupport::RouteBranches(per_branch)) = predicate_support(route) else {
        panic!("Route should yield RouteBranches support");
    };
    // One set per branch, aligned with the node's branch order.
    assert_eq!(
        per_branch.len(),
        branches.len(),
        "one support set per branch"
    );
    let by_branch: std::collections::HashMap<&str, &BTreeSet<String>> = branches
        .iter()
        .map(String::as_str)
        .zip(per_branch.iter())
        .collect();
    assert_eq!(by_branch["high"], &cols(&["amount"]), "high reads amount");
    assert_eq!(
        by_branch["flagged"],
        &cols(&["status"]),
        "flagged reads status"
    );
}

/// A Combine `where:` predicate exposes the union of its decomposed
/// equality and residual reads, with qualified `input.field` references
/// preserved.
#[test]
fn combine_where_support_qualified_union() {
    let yaml = r#"
pipeline:
  name: combine_support
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders_src
    config:
      name: orders_src
      type: csv
      path: orders.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: quantity, type: int }
  - type: source
    name: products_src
    config:
      name: products_src
      type: csv
      path: products.csv
      correlation_key: product_id
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: price, type: float }
  - type: combine
    name: join
    input:
      orders: orders_src
      products: products_src
    config:
      where: "orders.product_id == products.product_id and orders.quantity > 0"
      match: first
      on_miss: null_fields
      cxl: |
        emit order_id = orders.order_id
        emit pname = products.name
      propagate_ck: driver
  - type: output
    name: out
    input: join
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let compiled = compile_ok(yaml);
    let combine = node(&compiled, "join");
    let Some(PredicateSupport::CombineWhere(support)) = predicate_support(combine) else {
        panic!("Combine should yield CombineWhere support");
    };
    // Equality conjunct contributes both qualified sides; the residual
    // (`orders.quantity > 0`) contributes its qualified field.
    assert_eq!(
        support,
        cols(&[
            "orders.product_id",
            "products.product_id",
            "orders.quantity",
        ]),
        "where support is the qualified union over equality + residual"
    );
}

/// A multi-rule Cull exposes the union of columns its OR-combined
/// `drop_group_when` decision reads — across both the complex-expr
/// aggregate arg (`sum(if status == 'error' …)`) and the simple-field
/// aggregate arg (`sum(amount)`, stored as a schema index on the
/// binding). The synthetic decision-emit target and the `partition_by`
/// grouping key are not reads and do not appear.
#[test]
fn cull_drop_support_unions_aggregate_args() {
    let yaml = r#"
pipeline:
  name: cull_support
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
        - { name: status, type: string }
  - type: cull
    name: cd
    input: src
    config:
      partition_by: [id]
      removed_to: removed
      rules:
        - name: drop_errors
          drop_group_when: "sum(if status == 'error' then 1 else 0) > 0"
        - name: drop_big
          drop_group_when: "sum(amount) > 100"
  - type: output
    name: out
    input: cd
    config:
      name: out
      type: csv
      path: out.csv
  - type: output
    name: audit
    input: cd.removed
    config:
      name: audit
      type: csv
      path: audit.csv
"#;
    let compiled = compile_ok(yaml);
    let cull = node(&compiled, "cd");
    let Some(PredicateSupport::CullDrop(support)) = predicate_support(cull) else {
        panic!("Cull should yield CullDrop support");
    };
    assert_eq!(
        support,
        cols(&["amount", "status"]),
        "drop predicate reads amount + status; not the group key `id` nor the decision target"
    );
    // Defensively pin the two exclusions the qualification rules promise.
    assert!(
        !support.contains("id"),
        "partition key is not a predicate read"
    );
    assert!(
        support.iter().all(|c| !c.starts_with("$ck")),
        "synthetic decision column is never surfaced"
    );
}

/// Nodes without a control-flow / grouping predicate yield `None`.
#[test]
fn non_predicate_nodes_have_no_support() {
    let yaml = r#"
pipeline:
  name: no_predicate
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
  - type: transform
    name: passthrough
    input: src
    config:
      cxl: |
        emit id = id
        emit amount = amount
  - type: output
    name: out
    input: passthrough
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let compiled = compile_ok(yaml);
    assert!(
        predicate_support(node(&compiled, "src")).is_none(),
        "Source has no predicate support"
    );
    assert!(
        predicate_support(node(&compiled, "passthrough")).is_none(),
        "Transform has no predicate support"
    );
}
