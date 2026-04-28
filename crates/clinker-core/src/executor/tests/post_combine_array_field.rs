//! E150e: window over a `match: collect` combine's array column.
//!
//! `match: collect` emits one driver row per match group with the build
//! side's columns rolled into `Array` typed fields. Window builtins
//! (`$window.sum`, `$window.avg`, ...) operate on scalar values; an
//! array reference would silently see `Value::Null` at runtime. Lowering
//! must reject with E150e at compile time.

#[test]
fn match_collect_combine_window_over_array_field_raises_e150e() {
    let yaml = r#"
pipeline:
  name: combine_collect_window_e150e
error_handling:
  strategy: continue
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: orders.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: product_id, type: string }
- type: source
  name: products
  config:
    name: products
    path: products.csv
    type: csv
    schema:
      - { name: product_id, type: string }
      - { name: price, type: int }
- type: combine
  name: collected
  input:
    orders: orders
    products: products
  config:
    where: "orders.product_id == products.product_id"
    match: collect
    on_miss: null_fields
    cxl: ""
    propagate_ck: driver
- type: transform
  name: aggregate_window
  input: collected
  config:
    cxl: |
      emit order_id = order_id
      emit product_id = product_id
      emit total_count = $window.count()
      emit products_array = products
    analytic_window:
      group_by: [products]
- type: output
  name: out
  input: aggregate_window
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;
    let config = crate::config::parse_config(yaml).expect("parse");
    let result = config.compile(&crate::config::CompileContext::default());
    let diags = result.expect_err("E150e expected on window over match:collect array-typed column");
    let codes: Vec<&str> = diags.iter().map(|d| d.code.as_str()).collect();
    assert!(
        codes.contains(&"E150e"),
        "expected E150e among diagnostics, got: {codes:?}"
    );
}
