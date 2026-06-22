//! Combine `cxl:` body program travels on `PlanNode::Combine.typed`.
//!
//! The combine body program is carried on the node like
//! `PlanTransformPayload.typed` / `Aggregation.compiled`, so a lineage
//! walker that walks `dag().graph` (top-level) and each `body_of().graph`
//! (composition body) reaches a combine's emitted columns without a
//! name-keyed side-table lookup. This asserts both scopes: a TOP-LEVEL
//! combine and a body combine, each with a `where:` predicate and a
//! `cxl:` body.

use crate::plan::execution::PlanNode;
use petgraph::graph::DiGraph;

use super::deferred_region::compile_with_dir_full;

/// Read the typed `cxl:` body program off the named `PlanNode::Combine`
/// in `graph`, panicking if the node is absent, is not a Combine, or
/// carries no program.
fn combine_body_typed<'a>(
    graph: &'a DiGraph<PlanNode, crate::plan::execution::PlanEdge>,
    node_name: &str,
) -> &'a cxl::typecheck::TypedProgram {
    let idx = graph
        .node_indices()
        .find(|&i| graph[i].name() == node_name)
        .unwrap_or_else(|| panic!("combine node {node_name:?} not found in graph"));
    let PlanNode::Combine { typed, .. } = &graph[idx] else {
        panic!("node {node_name:?} is not a Combine");
    };
    typed
        .as_ref()
        .unwrap_or_else(|| panic!("combine {node_name:?} carries no typed body program"))
        .as_ref()
}

/// A top-level combine and a body combine, each with a `where:` + `cxl:`
/// body, both surface their typed body program off `PlanNode::Combine.typed`
/// with the emitted lineage columns visible in `program.statements` /
/// `output_row`.
#[test]
fn combine_body_program_travels_on_node_top_level_and_body() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    // Body combine: joins the two signature input ports and emits an
    // enriched row. Mirrors the `combine_enrich.comp.yaml` fixture shape.
    std::fs::write(
        comp_dir.join("combine_body.comp.yaml"),
        r#"_compose:
  name: combine_body
  inputs:
    orders:
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: quantity, type: int }
    products:
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
        - { name: price, type: float }
  outputs:
    enriched: body_combine
  config_schema: {}

nodes:
  - type: combine
    name: body_combine
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: first
      on_miss: null_fields
      cxl: |
        emit order_id = orders.order_id
        emit body_product_name = products.name
        emit body_total = products.price * orders.quantity
      propagate_ck: driver
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    // The top-level pipeline carries its own `top_combine` joining the two
    // sources directly, plus a composition whose body holds `body_combine`.
    let yaml = r#"
pipeline:
  name: combine_body_on_node
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
    name: top_combine
    input:
      orders: orders_src
      products: products_src
    config:
      where: "orders.product_id == products.product_id"
      match: first
      on_miss: null_fields
      cxl: |
        emit order_id = orders.order_id
        emit top_product_name = products.name
        emit top_total = products.price * orders.quantity
      propagate_ck: driver
  - type: composition
    name: body
    input: orders_src
    use: ../compositions/combine_body.comp.yaml
    inputs:
      orders: orders_src
      products: products_src
  - type: output
    name: out_top
    input: top_combine
    config:
      name: out_top
      type: csv
      path: out_top.csv
      include_unmapped: true
  - type: output
    name: out_body
    input: body
    config:
      name: out_body
      type: csv
      path: out_body.csv
      include_unmapped: true
"#;
    let compiled = compile_with_dir_full(yaml, workspace.path());

    // 1. TOP-LEVEL combine: program is reachable off the node in
    //    `dag().graph`, has statements, and `output_row` exposes the
    //    emitted lineage columns.
    let top = combine_body_typed(&compiled.dag().graph, "top_combine");
    assert!(
        !top.program.statements.is_empty(),
        "top-level combine body program should have statements"
    );
    let top_emitted: Vec<&str> = top
        .output_row
        .field_names()
        .map(|qf| qf.name.as_ref())
        .collect();
    assert!(
        top_emitted.contains(&"top_product_name") && top_emitted.contains(&"top_total"),
        "top combine output_row should expose emitted columns, got {top_emitted:?}"
    );

    // 2. BODY combine: program is reachable off the node in the
    //    composition body's `graph`, with the same fidelity.
    let body_id = compiled
        .artifacts()
        .composition_body_assignments
        .get("body")
        .copied()
        .expect("composition body assignment for `body`");
    let bound = compiled.body_of(body_id).expect("bound body");
    let body = combine_body_typed(&bound.graph, "body_combine");
    assert!(
        !body.program.statements.is_empty(),
        "body combine body program should have statements"
    );
    let body_emitted: Vec<&str> = body
        .output_row
        .field_names()
        .map(|qf| qf.name.as_ref())
        .collect();
    assert!(
        body_emitted.contains(&"body_product_name") && body_emitted.contains(&"body_total"),
        "body combine output_row should expose emitted columns, got {body_emitted:?}"
    );
}
