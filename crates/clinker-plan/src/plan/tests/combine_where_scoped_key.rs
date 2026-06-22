//! Scope-qualified keying of the compile-time `combine_where_typed`
//! side-table.
//!
//! The `where:` predicate program of a Combine lands in the
//! `CompileArtifacts.combine_where_typed` side-table keyed by
//! [`ScopedNodeId`]. Before scope-qualified keying it was keyed by the
//! bare node name, so a combine named the same at top level and inside a
//! composition body overwrote one entry with the other — the surviving
//! program was whichever the bind pass visited last, and klinx could not
//! read the body combine's `where` program scope-correctly.
//!
//! This test compiles a pipeline where a TOP-LEVEL combine and a BODY
//! combine share the SAME node name (`join_x`) but carry DIFFERENT `where:`
//! predicates, and asserts both scope-keyed entries survive with their own
//! distinct program.

use crate::{NodeScope, ScopedNodeId};

use super::deferred_region::compile_with_dir_full;

/// A top-level combine and a composition-body combine both named `join_x`,
/// each with a distinct `where:` predicate, keep separate
/// `combine_where_typed` entries under their scope-qualified keys.
#[test]
fn combine_where_typed_scoped_key_does_not_collide_across_scopes() {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    // Body composition: a combine named `join_x` whose `where:` predicate
    // gates on `quantity > 500` — distinct from the top-level `join_x`
    // below, which gates on `quantity > 5`.
    std::fs::write(
        comp_dir.join("scoped_combine.comp.yaml"),
        r#"_compose:
  name: scoped_combine
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
    enriched: join_x
  config_schema: {}

nodes:
  - type: combine
    name: join_x
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id and orders.quantity > 500"
      match: first
      on_miss: null_fields
      cxl: |
        emit order_id = orders.order_id
        emit body_name = products.name
      propagate_ck: driver
"#,
    )
    .expect("write comp");

    let pipelines_dir = workspace.path().join("pipelines");
    std::fs::create_dir_all(&pipelines_dir).expect("mkdir pipelines");

    // Top-level pipeline: a combine ALSO named `join_x`, gating on
    // `quantity > 5`, plus a composition whose body holds the body `join_x`.
    let yaml = r#"
pipeline:
  name: combine_where_scoped
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
    name: join_x
    input:
      orders: orders_src
      products: products_src
    config:
      where: "orders.product_id == products.product_id and orders.quantity > 5"
      match: first
      on_miss: null_fields
      cxl: |
        emit order_id = orders.order_id
        emit top_name = products.name
      propagate_ck: driver
  - type: composition
    name: body
    input: orders_src
    use: ../compositions/scoped_combine.comp.yaml
    inputs:
      orders: orders_src
      products: products_src
  - type: output
    name: out_top
    input: join_x
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
    let artifacts = compiled.artifacts();

    // The body's scope id comes from the composition-body assignment, the
    // same handle klinx would use to build the read key.
    let body_id = artifacts
        .composition_body_assignments
        .get("body")
        .copied()
        .expect("composition body assignment for `body`");

    // Both scope-qualified keys resolve — with bare-name keying only one of
    // these would have been populated (last writer wins).
    let top_key = ScopedNodeId {
        scope: NodeScope::TopLevel,
        name: "join_x".to_string(),
    };
    let body_key = ScopedNodeId {
        scope: NodeScope::Body(body_id),
        name: "join_x".to_string(),
    };

    let top_where = artifacts
        .combine_where_typed
        .get(&top_key)
        .expect("top-level join_x where program present under TopLevel scope key");
    let body_where = artifacts
        .combine_where_typed
        .get(&body_key)
        .expect("body join_x where program present under Body scope key");

    // The two programs are distinct — distinct `where:` predicates produce
    // distinct typed `Statement::Filter` bodies. The collision-regression:
    // with bare-name keying one would have overwritten the other, so a single
    // surviving program would be shared (or one key absent entirely, already
    // caught by the `expect`s above).
    let top_dbg = format!("{:?}", top_where.program.statements);
    let body_dbg = format!("{:?}", body_where.program.statements);
    assert_ne!(
        top_dbg, body_dbg,
        "top-level and body `join_x` must carry DIFFERENT where programs; \
         a shared program means the scope key collided"
    );
    // The top predicate gates on `> 5`, the body on `> 500`; each literal
    // must appear only in its own program.
    assert!(
        top_dbg.contains('5') && !top_dbg.contains("500"),
        "top-level where program should reflect the `> 5` predicate, got {top_dbg}"
    );
    assert!(
        body_dbg.contains("500"),
        "body where program should reflect the `> 500` predicate, got {body_dbg}"
    );
}
