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
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(comp_dir.join("c.comp.yaml"), comp_yaml).expect("write comp");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");
    let config = parse_config(PIPELINE).expect("parse pipeline");
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
    let diags = match compile(&comp(body)) {
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
        "\"last\"".to_string(),
        format!("`{column}`"),
        "input port `inp`".to_string(),
    ] {
        assert!(
            first_sentence.contains(&fragment),
            "the first sentence names {fragment:?}: {}",
            diag.message
        );
    }
    let help = diag.help.as_deref().expect("E169 carries the fix");
    for fragment in [
        "c.comp.yaml",
        "_compose:",
        "inputs:",
        "inp:",
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
