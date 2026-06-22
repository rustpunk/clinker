//! Body-`Combine` typed-artifact accessor tests.
//!
//! A composition body whose `Combine` carries both a `where:` clause
//! and a `cxl:` body must expose its typed `where`/body programs on the
//! `BoundBody`, so downstream lineage tooling can trace a body combine's
//! computed columns and join-key predicate columns at the same fidelity
//! it already has for body `Transform`/`Aggregation` nodes.

use cxl::ast::Statement;

use super::deferred_region::compile_with_dir_full;

/// A body `Combine` with both a `where:` predicate and a `cxl:` body,
/// fed by two signature input ports, surfaces both typed programs via
/// `BoundBody::combine_typed` / `combine_where_typed`; non-combine and
/// unknown ids return `None` from both.
#[test]
fn body_combine_exposes_typed_where_and_body() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
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
    enriched: enrich_combine
  config_schema: {}

nodes:
  - type: combine
    name: enrich_combine
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: first
      on_miss: null_fields
      cxl: |
        emit order_id = orders.order_id
        emit product_name = products.name
        emit total = products.price * orders.quantity
      propagate_ck: driver
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    let yaml = r#"
pipeline:
  name: combine_body_typed
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
  - type: composition
    name: body
    input: orders_src
    use: ../compositions/combine_body.comp.yaml
    inputs:
      orders: orders_src
      products: products_src
  - type: output
    name: out
    input: body
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let compiled = compile_with_dir_full(yaml, workspace.path());
    let artifacts = compiled.artifacts();
    let body_id = artifacts
        .composition_body_assignments
        .get("body")
        .copied()
        .expect("composition body assignment for `body`");
    let bound = compiled.body_of(body_id).expect("bound body");

    // 1. The combine's typed `cxl:` body program is reachable and
    //    carries the emitted lineage columns.
    let body_program = bound
        .combine_typed("enrich_combine")
        .expect("typed body program for combine");
    assert!(
        !body_program.program.statements.is_empty(),
        "combine body program should have statements"
    );
    let emitted: Vec<&str> = body_program
        .output_row
        .field_names()
        .map(|qf| qf.name.as_ref())
        .collect();
    assert!(
        emitted.contains(&"product_name"),
        "combine body output_row should expose the emitted `product_name` column, got {emitted:?}"
    );
    assert!(
        emitted.contains(&"total"),
        "combine body output_row should expose the emitted `total` column, got {emitted:?}"
    );

    // 2. The combine's typed `where:` predicate program is reachable
    //    and its first statement is the join-key Filter.
    let where_program = bound
        .combine_where_typed("enrich_combine")
        .expect("typed where program for combine");
    assert!(
        !where_program.program.statements.is_empty(),
        "combine where program should have statements"
    );
    assert!(
        matches!(
            where_program.program.statements.first(),
            Some(Statement::Filter { .. })
        ),
        "combine where program's first statement should be a Filter, got {:?}",
        where_program.program.statements.first()
    );

    // 3. A non-combine body node (a signature input port lowered to a
    //    body Source) returns `None` from both accessors.
    assert!(
        bound.combine_typed("orders").is_none(),
        "input-port node is not a Combine"
    );
    assert!(
        bound.combine_where_typed("orders").is_none(),
        "input-port node is not a Combine"
    );

    // 4. An unknown id returns `None` from both accessors.
    assert!(bound.combine_typed("does_not_exist").is_none());
    assert!(bound.combine_where_typed("does_not_exist").is_none());
}
