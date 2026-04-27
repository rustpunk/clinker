//! Integration tests for the `bind_schema` pass.
//!
//! These tests verify that:
//! - Source rows are seeded from SchemaDecl as Row::closed
//! - Row types propagate through Transform nodes
//! - Per-node output rows are reachable via
//!   `CompiledPlan::typed_output_row` for every bound node
//! - The module rename from cxl_compile is complete

use clinker_core::config::{CompileContext, PipelineConfig};

fn compile_yaml(yaml: &str) -> clinker_core::plan::CompiledPlan {
    let config: PipelineConfig = clinker_core::yaml::from_str(yaml).expect("fixture must parse");
    config
        .compile(&CompileContext::default())
        .expect("fixture must compile")
}

/// Gate test: a pipeline with a source declaring `schema: [{name: a, type: string}]`
/// produces `typed_output_row("source1") == Row::closed({a: String})`.
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
        .typed_output_row("source1")
        .expect("source1 must have a bound row");
    assert_eq!(row.field_count(), 2);
    assert!(row.has_field("a"), "expected column 'a'");
    assert!(row.has_field("b"), "expected column 'b'");
    assert_eq!(
        row.tail,
        cxl::typecheck::row::RowTail::Closed,
        "source rows must be Closed"
    );
}

/// Gate test: a `Source → Transform(emit y = a)` pipeline produces a
/// transform whose `typed_output_row` carries both `a` (from source)
/// and `y` (from emit) in its declared-columns map.
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
        .typed_output_row("tx")
        .expect("transform must have a bound row");
    assert!(row.has_field("x"), "upstream column 'x' must propagate");
    assert!(row.has_field("y"), "emitted column 'y' must appear");
}

/// Gate test: after `compile(ctx)`, `compiled_plan.typed_output_row("s1")`
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
        plan.typed_output_row("s1").is_some(),
        "source must have bound row"
    );
    assert!(
        plan.typed_output_row("t1").is_some(),
        "transform must have bound row"
    );
    assert!(
        plan.typed_output_row("o1").is_some(),
        "output must have bound row"
    );
    assert!(
        plan.typed_output_row("nonexistent").is_none(),
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

// ─────────────────────────────────────────────────────────────────────
// Frozen-identity ($ck.<field>) shadow column widening.
// ─────────────────────────────────────────────────────────────────────

use clinker_core::plan::execution::PlanNode;

/// Locate a Source node by name in the compiled plan's DAG.
fn source_node<'a>(plan: &'a clinker_core::plan::CompiledPlan, name: &str) -> &'a PlanNode {
    plan.dag()
        .graph
        .node_weights()
        .find(|n| matches!(n, PlanNode::Source { name: n_name, .. } if n_name == name))
        .unwrap_or_else(|| panic!("source node {name:?} not found"))
}

fn source_output_schema<'a>(
    plan: &'a clinker_core::plan::CompiledPlan,
    name: &str,
) -> &'a std::sync::Arc<clinker_record::Schema> {
    match source_node(plan, name) {
        PlanNode::Source { output_schema, .. } => output_schema,
        other => panic!(
            "expected PlanNode::Source for {name:?}, got {:?}",
            other.type_tag()
        ),
    }
}

/// A pipeline with no `error_handling.correlation_key` widens the
/// Source's `output_schema` with no `$ck.*` columns; user-declared
/// columns sit at the same indices the YAML lists them.
#[test]
fn test_source_widening_correlation_key_unset_leaves_schema_alone() {
    let yaml = r#"
pipeline:
  name: ck-unset
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: employee_id, type: string }
        - { name: salary, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile_yaml(yaml);
    let schema = source_output_schema(&plan, "src");
    assert_eq!(schema.column_count(), 2);
    assert_eq!(&*schema.columns()[0], "employee_id");
    assert_eq!(&*schema.columns()[1], "salary");
    assert!(
        schema.field_metadata_by_name("employee_id").is_none(),
        "user-declared column must not carry engine-stamp metadata"
    );
    assert!(
        !schema.contains("$ck.employee_id"),
        "absent correlation_key must produce no shadow column"
    );
}

/// A pipeline with `correlation_key: employee_id` tail-appends one
/// `$ck.employee_id` shadow column to the Source's `output_schema`,
/// preserving user-declared positional indices and stamping the
/// engine-stamp metadata that points back at the source field.
#[test]
fn test_source_widening_single_correlation_key_appends_one_shadow() {
    let yaml = r#"
pipeline:
  name: ck-single
error_handling:
  correlation_key: employee_id
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: employee_id, type: string }
        - { name: salary, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile_yaml(yaml);
    let schema = source_output_schema(&plan, "src");

    // User-declared columns stay at their declared positions.
    assert_eq!(schema.column_count(), 3);
    assert_eq!(&*schema.columns()[0], "employee_id");
    assert_eq!(&*schema.columns()[1], "salary");

    // Shadow column tail-appended.
    assert_eq!(
        &*schema.columns()[2],
        "$ck.employee_id",
        "shadow column must sit at schema tail (Spark `_metadata` shape)"
    );
    assert_eq!(schema.index("$ck.employee_id"), Some(2));

    // Engine-stamp metadata points back at the user-declared field.
    let stamp = schema
        .field_metadata_by_name("$ck.employee_id")
        .expect("shadow column must carry engine-stamp metadata");
    assert_eq!(stamp.snapshot_of.as_deref(), Some("employee_id"));
    assert!(stamp.is_engine_stamped());

    // User-declared columns remain unannotated.
    assert!(schema.field_metadata_by_name("employee_id").is_none());
    assert!(schema.field_metadata_by_name("salary").is_none());

    // The same widening lands on the typecheck Row produced by
    // `bind_schema` — confirms the source goes through `columns_from_decl`.
    let row = plan.typed_output_row("src").expect("bound row");
    assert!(row.has_field("$ck.employee_id"));
}

/// `correlation_key: [a, b]` (compound) tail-appends one shadow column
/// per declared field, in declaration order, each typed identically to
/// the corresponding user-declared field.
#[test]
fn test_source_widening_compound_correlation_key_appends_in_order() {
    let yaml = r#"
pipeline:
  name: ck-compound
error_handling:
  correlation_key: [tenant, employee_id]
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: employee_id, type: string }
        - { name: tenant, type: int }
        - { name: salary, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile_yaml(yaml);
    let schema = source_output_schema(&plan, "src");

    assert_eq!(schema.column_count(), 5);
    // Tail-append order matches `correlation_key.fields()` order.
    assert_eq!(&*schema.columns()[3], "$ck.tenant");
    assert_eq!(&*schema.columns()[4], "$ck.employee_id");

    assert_eq!(
        schema
            .field_metadata_by_name("$ck.tenant")
            .and_then(|m| m.snapshot_of.as_deref()),
        Some("tenant"),
    );
    assert_eq!(
        schema
            .field_metadata_by_name("$ck.employee_id")
            .and_then(|m| m.snapshot_of.as_deref()),
        Some("employee_id"),
    );
}

/// The `$ck.*` shadow columns propagate through the DAG: a Transform
/// downstream of a widened Source reads the same column name from its
/// own output_schema (with the same metadata), so any operator that
/// inherits the upstream row inherits the marker.
#[test]
fn test_source_widening_propagates_through_transform() {
    let yaml = r#"
pipeline:
  name: ck-propagate
error_handling:
  correlation_key: id
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: id, type: string }
        - { name: amount, type: int }
  - type: transform
    name: tx
    input: src
    config:
      cxl: "emit doubled = amount"
  - type: output
    name: out
    input: tx
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile_yaml(yaml);
    let row = plan.typed_output_row("tx").expect("tx row");
    assert!(
        row.has_field("$ck.id"),
        "transform downstream of widened source must inherit shadow column"
    );

    // PlanNode::Transform also exposes output_schema with the same marker.
    let tx_node = plan
        .dag()
        .graph
        .node_weights()
        .find(|n| matches!(n, PlanNode::Transform { name, .. } if name == "tx"))
        .expect("transform tx");
    let tx_schema = match tx_node {
        PlanNode::Transform { output_schema, .. } => output_schema,
        _ => unreachable!(),
    };
    assert_eq!(
        tx_schema
            .field_metadata_by_name("$ck.id")
            .and_then(|m| m.snapshot_of.as_deref()),
        Some("id"),
        "engine-stamp metadata must travel with the column through the DAG"
    );
}

/// A `correlation_key` listing a field absent from the source schema
/// is silently skipped by the widening pass. The widening only
/// synthesizes shadow columns it can type unambiguously; the missing-
/// field case is the responsibility of a future validation pass (no
/// such pass exists today; the runtime stamping path that depends on
/// the shadow column lands in a later wave). This test pins
/// bind_schema's local invariant: the pass does not over-promise on
/// fields it can't see in the source's `schema:` block.
#[test]
fn test_source_widening_skips_undeclared_correlation_field() {
    let yaml = r#"
pipeline:
  name: ck-missing
error_handling:
  correlation_key: nonexistent
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: employee_id, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let plan = compile_yaml(yaml);
    let schema = source_output_schema(&plan, "src");
    assert_eq!(schema.column_count(), 1);
    assert_eq!(&*schema.columns()[0], "employee_id");
    assert!(
        !schema.contains("$ck.nonexistent"),
        "undeclared correlation-key field must not produce a phantom shadow column"
    );
}

/// Composition body Sources synthesized from input ports inherit the
/// parent pipeline's widening: the port-synthetic `PlanNode::Source`
/// generated at composition entry exposes `$ck.<field>` columns with
/// the same engine-stamp metadata as the parent Source. Verifies
/// LD-16c-21 recursion: the widening sub-pass piggybacks on
/// `bind_schema`'s composition descent.
#[test]
fn test_source_widening_recurses_into_composition_port() {
    use std::path::PathBuf;
    let yaml = r#"
pipeline:
  name: ck-comp
error_handling:
  correlation_key: id
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: data/a.csv
      schema:
        - { name: id, type: string }
  - type: composition
    name: passthrough
    input: src
    use: ../compositions/passthrough_check.comp.yaml
    inputs:
      data: src
  - type: output
    name: out
    input: passthrough.data
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let cfg: clinker_core::config::PipelineConfig =
        clinker_core::yaml::from_str(yaml).expect("parse pipeline");
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures");
    let ctx = CompileContext::with_pipeline_dir(&root, PathBuf::from("pipelines"));
    let plan = cfg.compile(&ctx).expect("compile pipeline");

    // Top-level src: widened with $ck.id.
    let parent_schema = source_output_schema(&plan, "src");
    assert!(parent_schema.contains("$ck.id"));

    // Body port-synthetic Source ("data" port): walks the composition
    // body's mini-DAG and finds a PlanNode::Source named after the
    // port. Its output_schema must mirror the parent's widening.
    let body_id = plan
        .artifacts()
        .composition_body_assignments
        .get("passthrough")
        .copied()
        .expect("composition body assigned");
    let body = plan.body_of(body_id).expect("body present");
    let port_node = body
        .graph
        .node_weights()
        .find(|n| matches!(n, PlanNode::Source { name, .. } if name == "data"))
        .expect("body must carry a port-synthetic Source named 'data'");
    let port_schema = match port_node {
        PlanNode::Source { output_schema, .. } => output_schema,
        _ => unreachable!(),
    };
    assert!(
        port_schema.contains("$ck.id"),
        "composition body port-synthetic Source must inherit `$ck.<field>` from parent"
    );
    assert_eq!(
        port_schema
            .field_metadata_by_name("$ck.id")
            .and_then(|m| m.snapshot_of.as_deref()),
        Some("id"),
        "port-synthetic Source must carry the same engine-stamp metadata"
    );
}
