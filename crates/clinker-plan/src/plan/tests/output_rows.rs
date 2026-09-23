//! Top-level typed output rows retained on `CompiledPlan`.
//!
//! Tooling reads each top-level node's typed output `Row` (field names and
//! CXL types) off the compiled plan through `CompiledPlan::output_row`, the
//! top-level counterpart of `BoundBody::body_rows`. These tests pin that every
//! top-level node kind resolves a row matching the types it was bound with,
//! and that the rows describe the effective (post-overlay) plan.

use std::path::PathBuf;

use cxl::typecheck::{Row, Type};

use crate::config::{CompileContext, parse_config};
use crate::overlay_ops::{LayeredOp, OverlayLayer, OverlayOp};
use crate::plan::CompiledPlan;
use crate::yaml::Spanned;

const GATE_COMP: &str = r#"_compose:
  name: gate
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: val, type: float }
  outputs:
    out: gated
nodes:
  - type: transform
    name: gated
    input: inp
    config:
      cxl: |
        filter val >= 0.0
        emit id = id
        emit val = val
"#;

const PIPELINE: &str = r#"
pipeline:
  name: output_rows
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: orders.csv
      schema:
        - { name: id, type: string }
        - { name: val, type: float }
  - type: source
    name: rates
    config:
      name: rates
      type: csv
      path: rates.csv
      schema:
        - { name: rate_id, type: string }
        - { name: rate, type: int }
  - type: transform
    name: doubled
    input: orders
    config:
      cxl: |
        emit id = id
        emit twice = val * 2.0
        emit big = val > 100.0
  - type: aggregate
    name: totals
    input: doubled
    config:
      group_by: [id]
      cxl: |
        emit total = sum(twice)
  - type: combine
    name: joined
    input:
      o: doubled
      r: rates
    config:
      where: "o.id == r.rate_id"
      match: first
      on_miss: skip
      cxl: |
        emit id = o.id
        emit rate = r.rate
      propagate_ck: driver
  - type: composition
    name: gate
    input: orders
    use: ../compositions/gate.comp.yaml
    inputs:
      inp: orders
  - type: merge
    name: all_orders
    inputs: [orders, gate]
  - type: sink
    name: totals_out
    input: totals
    config:
      name: totals_out
      type: csv
      path: totals.csv
  - type: sink
    name: joined_out
    input: joined
    config:
      name: joined_out
      type: csv
      path: joined.csv
  - type: sink
    name: all_out
    input: all_orders
    config:
      name: all_out
      type: csv
      path: all.csv
"#;

/// Author-visible `(name, type)` pairs of a row, in row order. Engine-stamped
/// `$`-namespaced columns are not author vocabulary and are left out so the
/// assertions pin only the fields the pipeline declares or emits.
fn author_fields(row: &Row) -> Vec<(String, Type)> {
    row.fields()
        .filter(|(field, _)| !field.name.starts_with('$'))
        .map(|(field, ty)| (field.name.to_string(), ty.clone()))
        .collect()
}

fn fields(pairs: &[(&str, Type)]) -> Vec<(String, Type)> {
    pairs
        .iter()
        .map(|(name, ty)| (name.to_string(), ty.clone()))
        .collect()
}

fn row_of<'a>(plan: &'a CompiledPlan, name: &str) -> &'a Row {
    plan.output_row(name)
        .unwrap_or_else(|| panic!("top-level node {name:?} has an output row"))
}

fn compile_workspace_pipeline() -> CompiledPlan {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(comp_dir.join("gate.comp.yaml"), GATE_COMP).expect("write comp");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    parse_config(PIPELINE)
        .expect("parse pipeline")
        .compile(&ctx)
        .unwrap_or_else(|diags| panic!("pipeline compiles: {diags:#?}"))
}

#[test]
fn every_top_level_node_kind_resolves_its_bound_row() {
    let plan = compile_workspace_pipeline();
    let source_row = fields(&[("id", Type::String), ("val", Type::Float)]);

    assert_eq!(author_fields(row_of(&plan, "orders")), source_row);
    assert_eq!(
        author_fields(row_of(&plan, "rates")),
        fields(&[("rate_id", Type::String), ("rate", Type::Int)]),
    );
    assert_eq!(
        author_fields(row_of(&plan, "doubled")),
        // A Transform carries its input's columns and appends what it emits.
        fields(&[
            ("id", Type::String),
            ("val", Type::Float),
            ("twice", Type::Float),
            ("big", Type::Bool),
        ]),
    );
    assert_eq!(
        author_fields(row_of(&plan, "totals")),
        fields(&[("id", Type::String), ("total", Type::Float)]),
    );
    assert_eq!(
        author_fields(row_of(&plan, "joined")),
        fields(&[("id", Type::String), ("rate", Type::Int)]),
    );
    // A Composition node's row is its first output port's row: the body's
    // `gated` transform re-emits the input port's columns.
    assert_eq!(author_fields(row_of(&plan, "gate")), source_row);
    assert_eq!(author_fields(row_of(&plan, "all_orders")), source_row);
    // Sinks pass their input row through unchanged.
    assert_eq!(
        author_fields(row_of(&plan, "totals_out")),
        author_fields(row_of(&plan, "totals")),
    );
    assert_eq!(
        author_fields(row_of(&plan, "joined_out")),
        author_fields(row_of(&plan, "joined")),
    );
    assert_eq!(author_fields(row_of(&plan, "all_out")), source_row);

    // One entry per top-level node, in declaration order.
    let declared: Vec<&str> = plan
        .config()
        .nodes
        .iter()
        .map(|node| node.value.name())
        .collect();
    let retained: Vec<&str> = plan.output_rows().keys().map(String::as_str).collect();
    assert_eq!(retained, declared);

    // Composition-body nodes are reached through the body, not the top level.
    assert!(plan.output_row("gated").is_none());
    assert!(plan.output_row("no_such_node").is_none());
}

#[test]
fn output_rows_reflect_the_effective_overlay_plan() {
    let base = r#"
pipeline:
  name: overlay_rows
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
    name: normalize
    input: orders
    config:
      cxl: "emit order_id = order_id\nemit amount = amount"
  - type: sink
    name: sink
    input: normalize
    config:
      name: sink
      type: csv
      path: out.csv
"#;
    let op = crate::yaml::from_str::<Spanned<OverlayOp>>(
        r#"
op: set
target: normalize
field: config.cxl
value: "emit order_id = order_id\nemit flagged = amount > 0"
"#,
    )
    .expect("parse op");
    let ctx = CompileContext {
        overlay_ops: vec![LayeredOp::new(OverlayLayer::ChannelPerTarget, op)],
        ..CompileContext::default()
    };
    let plan = parse_config(base)
        .expect("parse base")
        .compile(&ctx)
        .unwrap_or_else(|diags| panic!("overlay plan compiles: {diags:#?}"));

    let effective = fields(&[
        ("order_id", Type::String),
        ("amount", Type::Int),
        ("flagged", Type::Bool),
    ]);
    assert_eq!(author_fields(row_of(&plan, "normalize")), effective);
    assert_eq!(author_fields(row_of(&plan, "sink")), effective);
}
