//! Integration tests for the `bind_schema` pass (Phase 16c.1.4).
//!
//! These tests verify that:
//! - Source rows are seeded from SchemaDecl as Row::closed
//! - Row types propagate through Transform nodes
//! - BoundSchemas are persisted on CompiledPlan
//! - The module rename from cxl_compile is complete

use clinker_core::config::{CompileContext, PipelineConfig};

fn compile_yaml(yaml: &str) -> clinker_core::plan::CompiledPlan {
    let config: PipelineConfig = clinker_core::yaml::from_str(yaml).expect("fixture must parse");
    config
        .compile(&CompileContext::default())
        .expect("fixture must compile")
}

/// Gate test: a pipeline with a source declaring `schema: [{name: a, type: string}]`
/// produces `bound_schemas.output_of("source1") == Row::closed({a: String})`.
#[test]
fn test_bind_schema_seeds_source_row_from_schemadecl() {
    let yaml = r#"
pipeline:
  name: seed-test
nodes:
  - type: source
    name: source1
    config:
      name: source1
      type: csv
      path: dummy.csv
      schema:
        - { name: a, type: string }
        - { name: b, type: int }
  - type: output
    name: out1
    input: source1
    config:
      name: out1
      type: csv
      path: out.csv
"#;
    let plan = compile_yaml(yaml);
    let row = plan
        .schema_for_node_name("source1")
        .expect("source1 must have a bound row");
    assert_eq!(row.declared.len(), 2);
    assert!(row.declared.contains_key("a"), "expected column 'a'");
    assert!(row.declared.contains_key("b"), "expected column 'b'");
    assert_eq!(
        row.tail,
        cxl::typecheck::row::RowTail::Closed,
        "source rows must be Closed"
    );
}

/// Gate test: a `Source → Transform(emit y = a)` pipeline produces
/// bound_schemas with both `a` (from source) and `y` (from emit) in
/// the transform's output row.
#[test]
fn test_bind_schema_propagates_row_through_transform() {
    let yaml = r#"
pipeline:
  name: propagate-test
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: dummy.csv
      schema:
        - { name: x, type: string }
  - type: transform
    name: tx
    input: src
    config:
      cxl: "emit y = x"
  - type: output
    name: out
    input: tx
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile_yaml(yaml);
    let row = plan
        .schema_for_node_name("tx")
        .expect("transform must have a bound row");
    assert!(
        row.declared.contains_key("x"),
        "upstream column 'x' must propagate"
    );
    assert!(
        row.declared.contains_key("y"),
        "emitted column 'y' must appear"
    );
}

/// Gate test: after `compile(ctx)`, `compiled_plan.schema_for_node_name("s1")`
/// returns `Some(&Row)`; same for all non-Composition nodes.
#[test]
fn test_bind_schema_persists_per_node_rows() {
    let yaml = r#"
pipeline:
  name: persist-test
nodes:
  - type: source
    name: s1
    config:
      name: s1
      type: csv
      path: dummy.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: t1
    input: s1
    config:
      cxl: "emit doubled = id"
  - type: output
    name: o1
    input: t1
    config:
      name: o1
      type: csv
      path: out.csv
"#;
    let plan = compile_yaml(yaml);
    assert!(
        plan.schema_for_node_name("s1").is_some(),
        "source must have bound row"
    );
    assert!(
        plan.schema_for_node_name("t1").is_some(),
        "transform must have bound row"
    );
    assert!(
        plan.schema_for_node_name("o1").is_some(),
        "output must have bound row"
    );
    assert!(
        plan.schema_for_node_name("nonexistent").is_none(),
        "nonexistent node must return None"
    );
}

/// Gate test: `grep -r 'cxl_compile' crates/ --include="*.rs"` returns
/// empty (no code references to the old module name, only comments).
#[test]
fn test_module_rename_cxl_compile_absent() {
    let source = include_str!("../src/config/mod.rs");
    assert!(
        !source.contains("pub mod cxl_compile"),
        "config/mod.rs must not declare pub mod cxl_compile"
    );
    let compiled = include_str!("../src/plan/compiled.rs");
    assert!(
        !compiled.contains("config::cxl_compile"),
        "compiled.rs must not reference config::cxl_compile"
    );
}
