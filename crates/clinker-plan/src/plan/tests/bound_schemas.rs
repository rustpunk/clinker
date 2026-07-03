//! `CompiledPlan::bound_schemas`: the resolved unified `SourceSchema` per
//! source, retained on the plan so the ingest path reads the schema from the
//! plan rather than re-reading an external file at runtime.

use crate::config::{CompileContext, parse_config};
use clinker_format::SourceSchema;

#[test]
fn bound_schemas_retains_resolved_source_schema() {
    let yaml = r#"
pipeline:
  name: bound_schemas
nodes:
- type: source
  name: orders
  config:
    name: orders
    path: input.csv
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: amount, type: int }
- type: output
  name: out
  input: orders
  config:
    name: out
    path: out.csv
    type: csv
"#;
    let config = parse_config(yaml).expect("parse");
    let plan = config.compile(&CompileContext::default()).expect("compile");

    let bound = plan.bound_schemas();
    // One entry per source node, keyed by node name.
    let schema = bound.get("orders").expect("source `orders` bound");
    let cols = match schema {
        SourceSchema::Columns(cols) => cols,
        other => panic!("expected an inline column list, got {other:?}"),
    };
    let names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names, vec!["order_id", "amount"]);
    assert_eq!(cols[1].ty, cxl::typecheck::Type::Int);
}
