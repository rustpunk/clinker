//! A composition body binds against the row its call site feeds it.
//!
//! A call behaves as its body written inline at the call site: the body's
//! input port carries the upstream node's records untouched, in the
//! upstream's column order, so every position the planner compiles for a
//! body node must address that order. The port's declared columns are the
//! vocabulary the body may name; every other upstream column passes through
//! unnamed, and a body that names one is refused at compile time.

use std::path::PathBuf;

use clinker_core_types::Diagnostic;
use cxl::plan::BindingArg;

use crate::config::{CompileContext, parse_config};
use crate::plan::CompiledPlan;
use crate::plan::execution::PlanNode;

/// The upstream every case reads: `order_id, department, v, wt`.
const PIPELINE: &str = r#"
pipeline:
  name: port_layout
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: v, type: int }
        - { name: wt, type: int }
  - type: composition
    name: call
    input: src
    use: ../compositions/c.comp.yaml
    inputs:
      inp: src
  - type: sink
    name: out
    input: call
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// A composition whose port declares `v, department` (a subset of the
/// upstream, in a different order) and whose body is `body`.
fn comp(body: &str) -> String {
    format!(
        r#"_compose:
  name: c
  inputs:
    inp:
      schema:
        - {{ name: v, type: int }}
        - {{ name: department, type: string }}
  outputs:
    out: last
  config_schema: {{}}

nodes:
{body}"#
    )
}

fn compile(comp_yaml: &str) -> Result<CompiledPlan, Vec<Diagnostic>> {
    compile_in(PIPELINE, &[("compositions/c.comp.yaml", comp_yaml)])
}

/// A workspace holding `files` (paths relative to its root) and an empty
/// `pipelines/` directory the pipeline compiles from.
fn workspace(files: &[(&str, &str)]) -> tempfile::TempDir {
    let workspace = tempfile::tempdir().expect("tempdir");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");
    for (relative, contents) in files {
        let path = workspace.path().join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, contents).expect("write file");
    }
    workspace
}

/// Compile `pipeline` in a workspace holding `files`.
fn compile_in(pipeline: &str, files: &[(&str, &str)]) -> Result<CompiledPlan, Vec<Diagnostic>> {
    let workspace = workspace(files);
    let config = parse_config(pipeline).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    config.compile(&ctx)
}

fn describe(diags: &[Diagnostic]) -> Vec<String> {
    diags
        .iter()
        .map(|d| {
            format!(
                "{}: {}\nhelp: {}",
                d.code,
                d.message,
                d.help.as_deref().unwrap_or("")
            )
        })
        .collect()
}

/// The column names of the port Source the body's `inp` port seeds: the
/// layout of the records the body receives.
fn port_layout(plan: &CompiledPlan) -> Vec<String> {
    let bodies = plan.composition_bodies();
    assert_eq!(bodies.len(), 1, "one bound body");
    let body = bodies.values().next().expect("one body");
    let idx = body.port_name_to_node_idx["inp"];
    match &body.graph[idx] {
        PlanNode::Source { output_schema, .. } => output_schema
            .columns()
            .iter()
            .map(|c| c.to_string())
            .collect(),
        other => panic!("the port is a synthetic Source, got {other:?}"),
    }
}

fn body_node<'a>(plan: &'a CompiledPlan, name: &str) -> &'a PlanNode {
    let body = plan.composition_bodies().values().next().expect("one body");
    &body.graph[body.name_to_idx[name]]
}

/// The column a compiled field index reads in the port layout.
fn column_at(layout: &[String], idx: u32) -> &str {
    layout
        .get(idx as usize)
        .map(String::as_str)
        .unwrap_or("<out of range>")
}

#[test]
fn body_aggregate_positions_address_the_call_site_row() {
    let plan = compile(&comp(
        r#"  - type: aggregate
    name: last
    input: inp
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit s = sum(v)
"#,
    ))
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
    let layout = port_layout(&plan);
    assert_eq!(
        &layout[..4],
        ["order_id", "department", "v", "wt"],
        "the body receives the upstream's records in the upstream's order"
    );
    let PlanNode::Aggregation { compiled, .. } = body_node(&plan, "last") else {
        panic!("`last` is an Aggregation");
    };
    let grouped: Vec<&str> = compiled
        .group_by_indices
        .iter()
        .map(|&i| column_at(&layout, i))
        .collect();
    assert_eq!(grouped, ["department"], "group_by reads `department`");
    let summed: Vec<&str> = compiled
        .bindings
        .iter()
        .map(|b| match &b.arg {
            BindingArg::Field(i) => column_at(&layout, *i),
            other => panic!("sum(v) is a field binding, got {other:?}"),
        })
        .collect();
    assert_eq!(summed, ["v"], "sum(v) reads `v`");
}

#[test]
fn body_cull_positions_address_the_call_site_row() {
    let plan = compile(&comp(
        r#"  - type: cull
    name: last
    input: inp
    config:
      partition_by: [department]
      removed_to: gone
      rules:
        - name: big
          drop_group_when: "sum(v) > 10"
"#,
    ))
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
    let layout = port_layout(&plan);
    let PlanNode::Cull { compiled, .. } = body_node(&plan, "last") else {
        panic!("`last` is a Cull");
    };
    let grouped: Vec<&str> = compiled
        .group_by_indices
        .iter()
        .map(|&i| column_at(&layout, i))
        .collect();
    assert_eq!(grouped, ["department"], "partition_by reads `department`");
    let summed: Vec<&str> = compiled
        .bindings
        .iter()
        .filter_map(|b| match &b.arg {
            BindingArg::Field(i) => Some(column_at(&layout, *i)),
            _ => None,
        })
        .collect();
    assert_eq!(summed, ["v"], "sum(v) reads `v`");
}

/// The one diagnostic a body naming `column` draws, with every fragment the
/// house style requires: the composition, the port, the column, and the
/// declaration to paste.
fn assert_undeclared_column_refused(body: &str, column: &str, declaration: &str) {
    assert_refused(compile(&comp(body)), "last", column, "inp", declaration);
}

/// [`assert_undeclared_column_refused`] for any compile: exactly one
/// diagnostic, E169, naming composition `call`, node `node`, `column` and
/// port `port`, with the declaration to paste.
fn assert_refused(
    compiled: Result<CompiledPlan, Vec<Diagnostic>>,
    node: &str,
    column: &str,
    port: &str,
    declaration: &str,
) {
    let diags = match compiled {
        Ok(_) => panic!("a body naming the undeclared column {column:?} must not compile"),
        Err(diags) => diags,
    };
    let refused: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == "E169").collect();
    assert_eq!(
        refused.len(),
        1,
        "exactly one E169 and nothing else, got {:#?}",
        describe(&diags)
    );
    assert_eq!(
        diags.len(),
        1,
        "nothing beside E169, got {:#?}",
        describe(&diags)
    );
    let diag = refused[0];
    let first_sentence = diag.message.split(". ").next().unwrap_or_default();
    for fragment in [
        "composition \"call\"".to_string(),
        format!("node \"{node}\""),
        format!("`{column}`"),
        format!("input port `{port}`"),
    ] {
        assert!(
            first_sentence.contains(&fragment),
            "the first sentence names {fragment:?}: {}",
            diag.message
        );
    }
    let help = diag.help.as_deref().expect("E169 carries the fix");
    let port_line = format!("{port}:");
    for fragment in [
        "c.comp.yaml",
        "_compose:",
        "inputs:",
        port_line.as_str(),
        "schema:",
        declaration,
    ] {
        assert!(
            help.contains(fragment),
            "the help carries {fragment:?}: {help}"
        );
    }
    assert!(
        !diag.message.contains('$') && !help.contains('$'),
        "no engine column in the diagnostic: {}\n{help}",
        diag.message
    );
}

#[test]
fn a_body_expression_naming_an_undeclared_column_is_refused() {
    assert_undeclared_column_refused(
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v * wt
"#,
        "wt",
        "- { name: wt, type: int }",
    );
}

#[test]
fn a_body_group_by_naming_an_undeclared_column_is_refused() {
    assert_undeclared_column_refused(
        r#"  - type: aggregate
    name: last
    input: inp
    config:
      group_by: [order_id]
      cxl: |
        emit s = sum(v)
"#,
        "order_id",
        "- { name: order_id, type: string }",
    );
}

#[test]
fn a_body_reading_a_column_it_wrote_itself_compiles() {
    // `wt` reaches the body undeclared, but the first Transform writes its
    // own `wt`; the second reads the body's column, not the caller's.
    compile(&comp(
        r#"  - type: transform
    name: weigh
    input: inp
    config:
      cxl: |
        emit wt = 2
  - type: transform
    name: last
    input: weigh
    config:
      cxl: |
        emit r = v * wt
"#,
    ))
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
}

/// [`PIPELINE`] with the source's `v` typed `ty`.
fn pipeline_with_v(ty: &str) -> String {
    PIPELINE.replace(
        "{ name: v, type: int }",
        &format!("{{ name: v, type: {ty} }}"),
    )
}

/// The type the body's node `node` gives `column`.
fn body_column_type(plan: &CompiledPlan, node: &str, column: &str) -> cxl::typecheck::Type {
    let body = plan.composition_bodies().values().next().expect("one body");
    match body.body_rows[node].lookup(column) {
        cxl::typecheck::ColumnLookup::Declared(ty) => ty.clone(),
        other => panic!("{node:?} declares {column:?}: {other:?}"),
    }
}

#[test]
fn a_column_the_port_types_is_seen_with_the_port_type() {
    // The port declares `v: int`; this caller's `v` is untyped. The body is
    // typed by its port, as a function body is typed by its parameters, so
    // the identity emit is an `int` at every call.
    let composition = comp(
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit y = v
"#,
    );
    let plan = compile_in(
        &pipeline_with_v("any"),
        &[("compositions/c.comp.yaml", &composition)],
    )
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
    assert_eq!(
        body_column_type(&plan, "last", "y"),
        cxl::typecheck::Type::Int
    );
}

#[test]
fn a_column_the_port_declares_any_is_seen_as_the_call_provides_it() {
    let composition = r#"_compose:
  name: c
  inputs:
    inp:
      schema:
        - { name: v, type: any }
        - { name: department, type: any }
  outputs:
    out: last
  config_schema: {}

nodes:
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit y = v
"#;
    let plan = compile_in(PIPELINE, &[("compositions/c.comp.yaml", composition)])
        .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
    assert_eq!(
        body_column_type(&plan, "last", "y"),
        cxl::typecheck::Type::Int,
        "this call provides `v` as an int"
    );
}

#[test]
fn a_type_error_on_a_column_declared_any_names_the_composition_and_the_port() {
    // `department` is declared `any` and this call provides text, so
    // `department + 1` fails only at this call: the error says so.
    let composition = r#"_compose:
  name: c
  inputs:
    inp:
      schema:
        - { name: v, type: int }
        - { name: department, type: any }
  outputs:
    out: last
  config_schema: {}

nodes:
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit s = department + 1
"#;
    let Err(diags) = compile_in(PIPELINE, &[("compositions/c.comp.yaml", composition)]) else {
        panic!("`department + 1` over text is a type error");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    let diag = &diags[0];
    assert_eq!(diag.code, "E200", "{:#?}", describe(&diags));
    let first_sentence = diag.message.split("; ").next().unwrap_or_default();
    for fragment in [
        "composition \"call\"",
        "`department`",
        "node \"last\"",
        "`string`",
        "input port `inp`",
        "`any`",
    ] {
        assert!(
            first_sentence.contains(fragment),
            "the first sentence names {fragment:?}: {}",
            diag.message
        );
    }
    let help = diag.help.as_deref().expect("a fix");
    assert!(
        help.contains("c.comp.yaml") && help.contains("type: any"),
        "the help points at the port declaration: {help}"
    );
}
