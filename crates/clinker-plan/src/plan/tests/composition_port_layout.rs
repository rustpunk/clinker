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

/// A composition `c` whose port `inp` declares `columns` (YAML flow
/// entries, one per line) and whose body is `body`, read out of node `last`.
fn comp_declaring(columns: &[&str], body: &str) -> String {
    let schema: String = columns
        .iter()
        .map(|column| format!("        - {column}\n"))
        .collect();
    format!(
        r#"_compose:
  name: c
  inputs:
    inp:
      schema:
{schema}  outputs:
    out: last
  config_schema: {{}}

nodes:
{body}"#
    )
}

const IDENTITY_BODY: &str = r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit y = v
"#;

/// The one E102 a compile draws, with every fragment the house style
/// requires in its first sentence and its fix.
fn assert_port_refuses(
    compiled: Result<CompiledPlan, Vec<Diagnostic>>,
    sentence: &[&str],
    fix: &[&str],
) {
    let Err(diags) = compiled else {
        panic!("the port must refuse the caller's column");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    let diag = &diags[0];
    assert_eq!(diag.code, "E102", "{:#?}", describe(&diags));
    for fragment in sentence {
        assert!(
            diag.message.contains(fragment),
            "the message names {fragment:?}: {}",
            diag.message
        );
    }
    assert!(
        !diag.message.contains("Nullable(") && !diag.message.contains("Int"),
        "no internal type names: {}",
        diag.message
    );
    let help = diag.help.as_deref().expect("E102 carries the fix");
    for fragment in fix {
        assert!(
            help.contains(fragment),
            "the fix carries {fragment:?}: {help}"
        );
    }
}

#[test]
fn an_untyped_caller_column_is_refused_by_a_typed_port() {
    // The port declares `v: int`; this caller's `v` has no declared type.
    // The body is typed by its port, so the call has to guarantee the type.
    assert_port_refuses(
        compile_in(
            &pipeline_with_v("any"),
            &[("compositions/c.comp.yaml", &comp(IDENTITY_BODY))],
        ),
        &[
            "composition \"call\"",
            "input port `inp`",
            "`v`",
            "`int`",
            "\"src\"",
            "`any`",
        ],
        &["- { name: v, type: int }", "source \"src\""],
    );
}

#[test]
fn an_untyped_caller_column_from_a_transform_is_refused_with_a_conversion() {
    let pipeline = PIPELINE
        .replace("{ name: v, type: int }", "{ name: v, type: any }")
        .replace(
            "  - type: composition\n    name: call\n    input: src\n",
            "  - type: transform\n    name: pre\n    input: src\n    config:\n      cxl: |\n        emit tag = 1\n  - type: composition\n    name: call\n    input: pre\n",
        )
        .replace("      inp: src\n", "      inp: pre\n");
    assert_port_refuses(
        compile_in(
            &pipeline,
            &[("compositions/c.comp.yaml", &comp(IDENTITY_BODY))],
        ),
        &["input port `inp`", "`v`", "\"pre\""],
        &["emit v = v.to_int()"],
    );
}

#[test]
fn a_port_declared_nullable_admits_the_non_empty_type_and_types_the_body_nullable() {
    let composition = comp_declaring(
        &[
            "{ name: v, type: { nullable: int } }",
            "{ name: department, type: string }",
        ],
        IDENTITY_BODY,
    );
    let plan =
        compile_in(PIPELINE, &[("compositions/c.comp.yaml", &composition)]).unwrap_or_else(|d| {
            panic!(
                "`int` is admitted into `{{ nullable: int }}`: {:#?}",
                describe(&d)
            )
        });
    assert_eq!(
        body_column_type(&plan, "last", "y"),
        cxl::typecheck::Type::nullable(cxl::typecheck::Type::Int),
        "the body sees the port's type, not the caller's"
    );
}

/// [`PIPELINE`] with a Transform `pre` before the call that writes `w2`,
/// empty on the first row: the call's caller column `w2` is nullable.
fn pipeline_with_nullable_w2() -> String {
    PIPELINE
        .replace(
            "  - type: composition\n    name: call\n    input: src\n",
            "  - type: transform\n    name: pre\n    input: src\n    config:\n      cxl: |\n        emit w2 = if v > 5 then 1 else null\n  - type: composition\n    name: call\n    input: pre\n",
        )
        .replace("      inp: src\n", "      inp: pre\n")
}

#[test]
fn a_typed_port_refuses_a_nullable_caller_column() {
    let composition = comp_declaring(
        &[
            "{ name: v, type: int }",
            "{ name: department, type: string }",
            "{ name: w2, type: int }",
        ],
        IDENTITY_BODY,
    );
    assert_port_refuses(
        compile_in(
            &pipeline_with_nullable_w2(),
            &[("compositions/c.comp.yaml", &composition)],
        ),
        &["`w2`", "`int`", "`{ nullable: int }`", "\"pre\""],
        &["- { name: w2, type: { nullable: int } }", "c.comp.yaml"],
    );
}

#[test]
fn a_port_declared_any_admits_every_caller_type() {
    let composition = comp_declaring(
        &[
            "{ name: v, type: any }",
            "{ name: department, type: string }",
            "{ name: w2, type: any }",
        ],
        IDENTITY_BODY,
    );
    let pipeline =
        pipeline_with_nullable_w2().replace("{ name: v, type: int }", "{ name: v, type: any }");
    compile_in(&pipeline, &[("compositions/c.comp.yaml", &composition)])
        .unwrap_or_else(|d| panic!("`any` admits anything: {:#?}", describe(&d)));
}

#[test]
fn the_e169_fix_for_a_nullable_caller_column_compiles_when_pasted() {
    let body = r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = w2
"#;
    let pipeline = pipeline_with_nullable_w2();
    let Err(diags) = compile_in(&pipeline, &[("compositions/c.comp.yaml", &comp(body))]) else {
        panic!("`w2` is not declared on the port");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    let help = diags[0].help.as_deref().expect("E169 carries the fix");
    let declaration = help
        .lines()
        .map(str::trim)
        .find(|line| line.starts_with("- { name: w2"))
        .unwrap_or_else(|| panic!("the fix declares `w2`: {help}"));
    assert_eq!(declaration, "- { name: w2, type: { nullable: int } }");
    let pasted = comp_declaring(
        &[
            "{ name: v, type: int }",
            "{ name: department, type: string }",
            declaration.trim_start_matches("- "),
        ],
        body,
    );
    compile_in(&pipeline, &[("compositions/c.comp.yaml", &pasted)])
        .unwrap_or_else(|d| panic!("the pasted fix compiles: {:#?}", describe(&d)));
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

#[test]
fn a_reshape_set_on_an_undeclared_column_is_refused() {
    // A Reshape cannot add a column: `set: { wt: ... }` needs the caller's
    // `wt`, and every row the rule does not touch keeps the caller's value.
    // So the body depends on `wt` and the port has to declare it.
    assert_refused(
        compile(&comp(
            r#"  - type: reshape
    name: rsh
    input: inp
    config:
      partition_by: [department]
      rules:
        - name: r1
          when: "v > 6"
          mutate:
            set:
              wt: "v"
  - type: transform
    name: last
    input: rsh
    config:
      cxl: |
        emit w2 = wt * 10
"#,
        )),
        "rsh",
        "wt",
        "inp",
        "- { name: wt, type: int }",
    );
}

#[test]
fn a_reshape_override_on_an_undeclared_column_is_refused() {
    assert_refused(
        compile(&comp(
            r#"  - type: reshape
    name: last
    input: inp
    config:
      partition_by: [department]
      rules:
        - name: r1
          when: "v > 6"
          synthesize:
            copy_from: trigger
            overrides:
              wt: "v"
"#,
        )),
        "last",
        "wt",
        "inp",
        "- { name: wt, type: int }",
    );
}

#[test]
fn a_distinct_by_an_undeclared_column_is_refused() {
    assert_undeclared_column_refused(
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        distinct by wt
        emit r = v
"#,
        "wt",
        "- { name: wt, type: int }",
    );
}

#[test]
fn a_window_naming_its_input_by_source_refuses_an_undeclared_key() {
    // A body window's `source:` can only be its own input root, so its
    // partition keys are names on the row the body receives.
    assert_undeclared_column_refused(
        r#"  - type: transform
    name: last
    input: inp
    config:
      analytic_window:
        source: inp
        group_by: [wt]
      cxl: |
        emit t = $window.sum(v)
"#,
        "wt",
        "- { name: wt, type: int }",
    );
}

#[test]
fn a_window_lookup_in_a_body_is_refused() {
    // `on:` would read the window's input by a key expression; a body window
    // reads only its own input, so `on:` is refused whatever it names.
    let Err(diags) = compile(&comp(
        r#"  - type: transform
    name: last
    input: inp
    config:
      analytic_window:
        source: inp
        group_by: [department]
        on: "wt"
      cxl: |
        emit t = $window.sum(v)
"#,
    )) else {
        panic!("a body window with `on:` must not compile");
    };
    assert!(
        diags.iter().any(|d| d.code == "E165" || d.code == "E169"),
        "{:#?}",
        describe(&diags)
    );
}
