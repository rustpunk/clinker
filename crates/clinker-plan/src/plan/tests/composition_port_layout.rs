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

/// Bind `pipeline` in a workspace holding `files` and return every
/// diagnostic the bind raised, warnings included (a compile that succeeds
/// does not return its warnings).
fn bind_in(pipeline: &str, files: &[(&str, &str)]) -> Vec<Diagnostic> {
    let workspace = workspace(files);
    let config = parse_config(pipeline).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let symbol_table = crate::config::composition::scan_workspace_signatures(workspace.path())
        .expect("compositions load");
    let mut diags = Vec::new();
    crate::plan::bind_schema::bind_schema(
        &config.nodes,
        &mut diags,
        &ctx,
        &symbol_table,
        &ctx.pipeline_dir,
        Default::default(),
    );
    diags
}

/// Bind `pipeline` as [`bind_in`] does and return how many times a
/// composition body was bound, counting a body's check against its ports'
/// declared types and the bind that produces its call's rows separately.
/// The bind must raise no error.
fn body_binds_in(pipeline: &str, files: &[(&str, &str)]) -> u32 {
    let workspace = workspace(files);
    let config = parse_config(pipeline).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let symbol_table = crate::config::composition::scan_workspace_signatures(workspace.path())
        .expect("compositions load");
    let mut diags = Vec::new();
    let artifacts = crate::plan::bind_schema::bind_schema(
        &config.nodes,
        &mut diags,
        &ctx,
        &symbol_table,
        &ctx.pipeline_dir,
        Default::default(),
    );
    assert!(
        diags
            .iter()
            .all(|d| d.severity != clinker_core_types::Severity::Error),
        "binds cleanly: {:#?}",
        describe(&diags)
    );
    artifacts.body_binds
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
fn a_port_declared_nullable_admits_the_non_empty_type_and_the_call_carries_it() {
    // The body is checked as if `v` could be empty (see
    // `a_body_that_does_not_handle_an_empty_value_is_refused_at_every_call`);
    // what the call runs and carries out is the caller's `int`, so the
    // column the body writes from it is an `int`, as it is inline.
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
        cxl::typecheck::Type::Int,
        "the call carries the caller's type"
    );
    // A caller whose `v` can be empty gets a possibly empty `y`.
    let plan = compile_in(
        &pipeline_with_v("{ nullable: int }"),
        &[("compositions/c.comp.yaml", &composition)],
    )
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
    assert_eq!(
        body_column_type(&plan, "last", "y"),
        cxl::typecheck::Type::nullable(cxl::typecheck::Type::Int)
    );
}

/// A body node that cannot take an empty value from a `{ nullable: int }`
/// port, called by a caller whose `v` is `caller_v`.
fn possibly_empty_refusal(caller_v: &str, body: &str) -> Vec<Diagnostic> {
    let composition = comp_declaring(&["{ name: v, type: { nullable: int } }"], body);
    let Err(diags) = compile_in(
        &pipeline_with_v(caller_v),
        &[("compositions/c.comp.yaml", &composition)],
    ) else {
        panic!("the body does not handle an empty `v`");
    };
    diags
}

#[test]
fn a_body_that_does_not_handle_an_empty_value_is_refused_at_every_call() {
    let body = r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        filter v > 6
"#;
    for caller in ["int", "{ nullable: int }"] {
        let diags = possibly_empty_refusal(caller, body);
        assert_eq!(diags.len(), 1, "{caller}: {:#?}", describe(&diags));
        let diag = &diags[0];
        assert_eq!(diag.code, "E200", "{:#?}", describe(&diags));
        assert!(
            diag.message.starts_with(
                "node \"last\" in composition \"call\" cannot use `v` while it may be empty: \
                 input port `inp` declares `v` as `{ nullable: int }`"
            ),
            "{}",
            diag.message
        );
        let help = diag.help.as_deref().expect("a fix");
        assert!(
            help.contains("`filter (v ?? 0) > 6`")
                && help.contains("`- { name: v, type: int }`")
                && help.contains("c.comp.yaml"),
            "{help}"
        );
    }
}

#[test]
fn an_error_that_is_not_about_an_empty_value_is_not_blamed_on_one() {
    // `v.upper()` fails whether or not `v` can be empty: the plain E200.
    let diags = possibly_empty_refusal(
        "int",
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit u = v.upper()
"#,
    );
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags[0].code, "E200");
    assert!(
        !diags[0].message.contains("may be empty"),
        "{}",
        diags[0].message
    );
}

#[test]
fn a_nested_call_that_needs_a_never_empty_value_is_refused_behind_a_possibly_empty_port() {
    // The outer body is checked as if `v` could be empty, so its call to a
    // composition whose port needs a never-empty `v` fails at every call.
    // The empty value comes from the outer composition's own declaration,
    // so the error names that port, and its first fix is the outer port
    // line to paste.
    let outer = comp_declaring(
        &["{ name: v, type: { nullable: int } }"],
        r#"  - type: composition
    name: last
    input: inp
    use: ./inner.comp.yaml
    inputs:
      inp: inp
"#,
    );
    let inner = comp_declaring(&["{ name: v, type: int }"], IDENTITY_BODY);
    let Err(diags) = compile_in(
        PIPELINE,
        &[
            ("compositions/c.comp.yaml", &outer),
            ("compositions/inner.comp.yaml", &inner),
        ],
    ) else {
        panic!("the outer body hands a possibly empty `v` to a port that needs an `int`");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    let diag = &diags[0];
    assert_eq!(diag.code, "E102", "{:#?}", describe(&diags));
    assert!(
        diag.message.starts_with(
            "composition \"call\" passes a `v` that may be empty to composition \"last\", \
             whose input port `inp` needs `v` never empty (`int`)"
        ) && diag.message.contains(
            "input port `inp` of \"call\" declares `v` as `{ nullable: int }` in \
             `compositions/c.comp.yaml`"
        ),
        "{}",
        diag.message
    );
    let help = diag.help.as_deref().expect("a fix");
    assert!(
        help.starts_with(
            "declare `v` as never empty on input port `inp` in `compositions/c.comp.yaml`: \
             `- { name: v, type: int }`"
        ) && help.contains(
            "`compositions/inner.comp.yaml` instead: `- { name: v, type: { nullable: int } }`"
        ),
        "{help}"
    );

    // The first fix, pasted, compiles.
    let fixed = outer.replace(
        "- { name: v, type: { nullable: int } }",
        "- { name: v, type: int }",
    );
    compile_in(
        PIPELINE,
        &[
            ("compositions/c.comp.yaml", &fixed),
            ("compositions/inner.comp.yaml", &inner),
        ],
    )
    .unwrap_or_else(|d| panic!("the pasted fix compiles: {:#?}", describe(&d)));
}

#[test]
fn a_body_that_compiles_against_its_declared_types_compiles_at_every_admitted_call() {
    // Nullability monotonicity: every typing rule that accepts a possibly
    // empty value accepts a never-empty one. A body that passes the check
    // against `{ nullable: int }` must therefore bind for a caller whose `v`
    // is a never-empty `int`; if it ever does not, the compile reports E000
    // rather than trusting the check.
    let transforms = [
        "emit a = v + 1",
        "emit a = v - 1",
        "emit a = v * 2",
        "emit a = v / 2",
        "emit a = v % 2",
        "emit a = -v",
        "emit a = v * 2.5",
        "emit a = v > 3",
        "emit a = v >= 3 and v <= 9",
        "emit a = v == 5",
        "emit a = v != 5",
        "emit a = v ?? 0",
        "emit a = (v ?? 0) > 6",
        "emit a = v.to_string()",
        "emit a = v.to_float()",
        "emit a = v.abs()",
        "emit a = v.is_null()",
        "emit a = v.type_of()",
        "emit v = v",
        "emit v = v + 1",
        "let x = v + 1\nemit a = x",
        "let x = v\nemit a = x ?? 0",
        "filter (v ?? 0) > 6",
        "filter v.is_null()",
        "filter (v > 6) ?? false",
        "emit a = v.to_string().upper()",
        "emit a = v.to_string() + \"x\"",
    ];
    let mut bodies: Vec<String> = transforms
        .iter()
        .map(|cxl| {
            let cxl = cxl.replace('\n', "\n        ");
            format!(
                "  - type: transform\n    name: last\n    input: inp\n    config:\n      cxl: |\n        {cxl}\n"
            )
        })
        .collect();
    bodies.push(
        r#"  - type: aggregate
    name: last
    input: inp
    config:
      group_by: [v]
      cxl: |
        emit n = count(*)
"#
        .to_string(),
    );
    bodies.push(
        r#"  - type: aggregate
    name: last
    input: inp
    config:
      group_by: []
      cxl: |
        emit s = sum(v)
        emit m = max(v)
"#
        .to_string(),
    );
    bodies.push(
        r#"  - type: reshape
    name: last
    input: inp
    config:
      partition_by: [v]
      rules:
        - name: r1
          when: "v == 5"
          mutate:
            set:
              v: "v"
"#
        .to_string(),
    );
    let mut checked = 0;
    for body in &bodies {
        let composition = comp_declaring(&["{ name: v, type: { nullable: int } }"], body);
        let files = [("compositions/c.comp.yaml", composition.as_str())];
        let declared = compile_in(&pipeline_with_v("{ nullable: int }"), &files);
        let provided = compile_in(PIPELINE, &files);
        if let Err(diags) = &provided {
            assert!(
                diags.iter().all(|d| d.code != "E000"),
                "{body}\n{:#?}",
                describe(diags)
            );
        }
        if declared.is_ok() {
            checked += 1;
            assert!(
                provided.is_ok(),
                "passes against `{{ nullable: int }}` but not for an `int` caller:\n{body}\n{:#?}",
                provided.err().map(|d| describe(&d))
            );
        }
    }
    assert!(
        checked >= 20,
        "the property ran on {checked} bodies that pass the check"
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

#[test]
fn a_body_node_listed_before_its_input_is_still_checked() {
    // `last` reads `a`, which is listed after it. The body binds in input
    // order, so `last` binds against `a`'s row and its undeclared `wt` is
    // refused like any other.
    assert_undeclared_column_refused(
        r#"  - type: transform
    name: last
    input: a
    config:
      cxl: |
        emit z = wt
  - type: transform
    name: a
    input: inp
    config:
      cxl: |
        emit r = v
"#,
        "wt",
        "- { name: wt, type: int }",
    );
}

/// A composition `c` like [`comp`] whose first output port reads `out`.
fn comp_out(out: &str, body: &str) -> String {
    comp(body).replace("    out: last\n", &format!("    out: {out}\n"))
}

/// Every surface outside a CXL expression where a body names a column: each
/// is refused with E169 when the column is a caller column its port does
/// not declare.
#[test]
fn each_column_list_naming_an_undeclared_column_is_refused() {
    let cases: &[(&str, &str, &str)] = &[
        (
            "route condition",
            "last.high",
            r#"  - type: route
    name: last
    input: inp
    config:
      conditions:
        high: "wt > 0"
      default: low
"#,
        ),
        (
            "cull partition_by",
            "last",
            r#"  - type: cull
    name: last
    input: inp
    config:
      partition_by: [wt]
      removed_to: gone
      rules:
        - name: r
          drop_group_when: "count(*) > 1"
"#,
        ),
        (
            "cull order_by",
            "last",
            r#"  - type: cull
    name: last
    input: inp
    config:
      partition_by: [department]
      order_by:
        - field: wt
      removed_to: gone
      rules:
        - name: r
          drop_group_when: "count(*) > 1"
"#,
        ),
        (
            "cull rule",
            "last",
            r#"  - type: cull
    name: last
    input: inp
    config:
      partition_by: [department]
      removed_to: gone
      rules:
        - name: r
          drop_group_when: "sum(wt) > 0"
"#,
        ),
        (
            "reshape partition_by",
            "last",
            r#"  - type: reshape
    name: last
    input: inp
    config:
      partition_by: [wt]
      rules:
        - name: r1
          when: "v > 5"
          mutate:
            set:
              department: "'z'"
"#,
        ),
        (
            "reshape order_by",
            "last",
            r#"  - type: reshape
    name: last
    input: inp
    config:
      partition_by: [department]
      order_by:
        - field: wt
      rules:
        - name: r1
          when: "v > 5"
          mutate:
            set:
              v: "v + 1"
"#,
        ),
        (
            "reshape when",
            "last",
            r#"  - type: reshape
    name: last
    input: inp
    config:
      partition_by: [department]
      rules:
        - name: r1
          when: "wt > 0"
          mutate:
            set:
              v: "v + 1"
"#,
        ),
        (
            "aggregate argument",
            "last",
            r#"  - type: aggregate
    name: last
    input: inp
    config:
      group_by: [department]
      cxl: |
        emit s = sum(wt)
"#,
        ),
        (
            "window argument",
            "last",
            r#"  - type: transform
    name: last
    input: inp
    config:
      analytic_window:
        group_by: [department]
      cxl: |
        emit t = $window.sum(wt)
"#,
        ),
        (
            "window sort_by",
            "last",
            r#"  - type: transform
    name: last
    input: inp
    config:
      analytic_window:
        group_by: [department]
        sort_by:
          - field: wt
      cxl: |
        emit t = $window.sum(v)
"#,
        ),
        (
            "log fields",
            "last",
            r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
      log:
        - name: l
          level: info
          when: per_record
          every: 1
          fields: [wt]
          message: "m"
"#,
        ),
        (
            "log condition",
            "last",
            r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
      log:
        - name: l
          level: info
          when: per_record
          every: 1
          condition: "wt > 0"
          message: "m"
"#,
        ),
        (
            "validation field",
            "last",
            r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
      validations:
        - field: wt
          check: "not_empty"
"#,
        ),
    ];
    for (surface, out, body) in cases {
        let node = out.split('.').next().unwrap_or(out);
        let compiled = compile(&comp_out(out, body));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_refused(compiled, node, "wt", "inp", "- { name: wt, type: int }")
        }));
        assert!(outcome.is_ok(), "{surface}: see the panic above");
    }
}

/// A body Sink names columns in its `mapping:`, `exclude:`, split key and
/// sort order: each is refused on an undeclared column.
#[test]
fn a_body_sink_naming_an_undeclared_column_is_refused() {
    let sinks: &[(&str, &str)] = &[
        ("mapping", "      mapping:\n        - w: wt\n"),
        ("exclude", "      exclude: [wt]\n"),
        ("sort_order", "      sort_order:\n        - field: wt\n"),
        (
            "split",
            "      split:\n        max_records: 10\n        group_key: wt\n",
        ),
    ];
    for (surface, config) in sinks {
        let body = format!(
            r#"  - type: transform
    name: t
    input: inp
    config:
      cxl: |
        emit r = v
  - type: sink
    name: keep
    input: t
    config:
      name: keep
      type: csv
      path: keep.csv
{config}"#
        );
        let compiled = compile(&comp_out("t", &body));
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_refused(compiled, "keep", "wt", "inp", "- { name: wt, type: int }")
        }));
        assert!(outcome.is_ok(), "{surface}: see the panic above");
    }
}

/// [`PIPELINE`] with a second Source `dep` (`department, label, extra`)
/// feeding the call's port `pb`.
const COMBINE_PIPELINE: &str = r#"
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
  - type: source
    name: dep
    config:
      name: dep
      type: csv
      path: in2.csv
      schema:
        - { name: department, type: string }
        - { name: label, type: string }
        - { name: extra, type: string }
  - type: composition
    name: call
    input: src
    use: ../compositions/c.comp.yaml
    inputs:
      pa: src
      pb: dep
  - type: sink
    name: out
    input: call
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// A two-port composition: `pa` declares `department, v`, `pb` declares
/// `department, label`; a Combine `j` with `where_clause` and `cxl`.
fn combine_comp(where_clause: &str, cxl: &str) -> String {
    combine_comp_with_v("int", where_clause, cxl)
}

/// [`combine_comp`] with `pa` declaring `v` as `v_type`.
fn combine_comp_with_v(v_type: &str, where_clause: &str, cxl: &str) -> String {
    format!(
        r#"_compose:
  name: c
  inputs:
    pa:
      schema:
        - {{ name: department, type: string }}
        - {{ name: v, type: {v_type} }}
    pb:
      schema:
        - {{ name: department, type: string }}
        - {{ name: label, type: string }}
  outputs:
    out: j
  config_schema: {{}}

nodes:
  - type: combine
    name: j
    input:
      a: pa
      b: pb
    config:
      where: "{where_clause}"
      match: first
      on_miss: null_fields
      propagate_ck: driver
      cxl: |
        {cxl}
"#
    )
}

#[test]
fn a_combine_naming_an_undeclared_column_of_one_input_is_refused() {
    for (surface, where_clause, cxl, port, column, declaration) in [
        (
            "body",
            "a.department == b.department",
            "emit label = b.extra",
            "pb",
            "extra",
            "- { name: extra, type: string }",
        ),
        (
            "where",
            "a.department == b.department and a.wt > 0",
            "emit label = b.label",
            "pa",
            "wt",
            "- { name: wt, type: int }",
        ),
    ] {
        let compiled = compile_in(
            COMBINE_PIPELINE,
            &[("compositions/c.comp.yaml", &combine_comp(where_clause, cxl))],
        );
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            assert_refused(compiled, "j", column, port, declaration)
        }));
        assert!(outcome.is_ok(), "{surface}: see the panic above");
    }
}

#[test]
fn a_combine_reading_declared_columns_of_both_inputs_compiles() {
    compile_in(
        COMBINE_PIPELINE,
        &[(
            "compositions/c.comp.yaml",
            &combine_comp(
                "a.department == b.department",
                "emit department = a.department\n        emit v = a.v\n        emit label = b.label",
            ),
        )],
    )
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
}

/// The inner composition of a nested call: its port `ip` declares `ip_columns`.
fn inner_comp(ip_columns: &str) -> String {
    format!(
        r#"_compose:
  name: inner
  inputs:
    ip:
      schema:
{ip_columns}  outputs:
    out: it
  config_schema: {{}}

nodes:
  - type: transform
    name: it
    input: ip
    config:
      cxl: |
        emit r = v * 2
"#
    )
}

#[test]
fn a_nested_port_declaring_a_column_the_outer_port_does_not_is_refused() {
    let outer = comp_out(
        "ic",
        r#"  - type: composition
    name: ic
    input: inp
    use: ./inner.comp.yaml
    inputs:
      ip: inp
"#,
    );
    assert_refused(
        compile_in(
            PIPELINE,
            &[
                ("compositions/c.comp.yaml", &outer),
                (
                    "compositions/inner.comp.yaml",
                    &inner_comp(
                        "        - { name: v, type: int }\n        - { name: wt, type: int }\n",
                    ),
                ),
            ],
        ),
        "ic",
        "wt",
        "inp",
        "- { name: wt, type: int }",
    );
}

#[test]
fn a_caller_column_passed_through_a_nested_call_stays_unnamed() {
    // The inner call passes `order_id` through unchanged; it is still the
    // outer caller's column, which the outer port does not declare.
    let outer = comp_out(
        "after",
        r#"  - type: composition
    name: ic
    input: inp
    use: ./inner.comp.yaml
    inputs:
      ip: inp
  - type: transform
    name: after
    input: ic
    config:
      cxl: |
        emit z = order_id
"#,
    );
    assert_refused(
        compile_in(
            PIPELINE,
            &[
                ("compositions/c.comp.yaml", &outer),
                (
                    "compositions/inner.comp.yaml",
                    &inner_comp("        - { name: v, type: int }\n"),
                ),
            ],
        ),
        "after",
        "order_id",
        "inp",
        "- { name: order_id, type: string }",
    );
}

#[test]
fn a_column_a_nested_call_writes_is_the_outer_body_s_own() {
    // The inner call writes `r`; the outer body may read it.
    let outer = comp_out(
        "after",
        r#"  - type: composition
    name: ic
    input: inp
    use: ./inner.comp.yaml
    inputs:
      ip: inp
  - type: transform
    name: after
    input: ic
    config:
      cxl: |
        emit z = r + v
"#,
    );
    compile_in(
        PIPELINE,
        &[
            ("compositions/c.comp.yaml", &outer),
            (
                "compositions/inner.comp.yaml",
                &inner_comp("        - { name: v, type: int }\n"),
            ),
        ],
    )
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
}

#[test]
fn a_nested_call_with_no_input_ports_returns_the_outer_body_s_own_columns() {
    // The inner composition reads its own Source and binds no input port.
    // Called inside the outer body, its columns are the outer body's own,
    // so the outer body may name them.
    let outer = comp_out(
        "after",
        r#"  - type: composition
    name: ic
    input: inp
    use: ./inner.comp.yaml
    inputs: {}
    resources: { own_data: shared }
  - type: transform
    name: after
    input: ic
    config:
      cxl: |
        emit z = k + 1
"#,
    );
    let inner = r#"_compose:
  name: inner
  inputs: {}
  outputs:
    out: own
  config_schema: {}
  resources_schema:
    own_data: { kind: file, required: true }

nodes:
  - type: source
    name: own
    config:
      name: own
      type: csv
      resource: own_data
      schema:
        - { name: k, type: int }
"#;
    compile_in(
        PIPELINE,
        &[
            ("compositions/c.comp.yaml", &outer),
            ("compositions/inner.comp.yaml", inner),
            ("data/k.csv", "k\n1\n"),
            (
                "clinker.toml",
                "[catalog.resources.shared]\nkind = \"file\"\npath = \"data/k.csv\"\naccess = \"read\"\n",
            ),
        ],
    )
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
}

/// The W101 warnings a bind of `pipeline` with composition `comp_yaml` raises.
fn w101(pipeline: &str, comp_yaml: &str) -> Vec<Diagnostic> {
    bind_in(pipeline, &[("compositions/c.comp.yaml", comp_yaml)])
        .into_iter()
        .filter(|d| d.code == "W101")
        .collect()
}

#[test]
fn w101_warns_where_the_body_writes_a_column_the_port_passes_through() {
    let warned = w101(
        PIPELINE,
        &comp(
            r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit wt = 2
"#,
        ),
    );
    assert_eq!(warned.len(), 1, "{:#?}", describe(&warned));
    assert!(
        warned[0].message.contains("`wt`") && warned[0].message.contains("\"inp\""),
        "{}",
        warned[0].message
    );
}

#[test]
fn w101_stays_silent_for_a_new_column() {
    let warned = w101(
        PIPELINE,
        &comp(
            r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v * 2
"#,
        ),
    );
    assert!(warned.is_empty(), "{:#?}", describe(&warned));
}

#[test]
fn w101_stays_silent_for_a_column_one_port_declares_and_another_passes_through() {
    // `department` reaches the body through both ports; `pa` declares it.
    // The output carries `pa`'s `department` unchanged, which shadows
    // nothing, although `pb` would pass its own through undeclared.
    let composition = r#"_compose:
  name: c
  inputs:
    pa:
      schema:
        - { name: department, type: string }
        - { name: v, type: int }
    pb:
      schema:
        - { name: label, type: string }
  outputs:
    out: t
  config_schema: {}

nodes:
  - type: transform
    name: t
    input: pa
    config:
      cxl: |
        emit r = v
"#;
    let warned = w101(COMBINE_PIPELINE, composition);
    assert!(warned.is_empty(), "{:#?}", describe(&warned));
}

#[test]
fn a_typo_is_never_answered_with_an_undeclared_column() {
    // `wtt` is one edit from the caller's `wt`, which the port does not
    // declare: suggesting it would lead straight to E169.
    let Err(diags) = compile(&comp(
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = wtt
"#,
    )) else {
        panic!("`wtt` names nothing");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags[0].code, "E203", "{:#?}", describe(&diags));
    let help = diags[0].help.as_deref().unwrap_or_default();
    assert!(
        !help.contains("'wt'"),
        "the suggestion is a column the body may use: {help}"
    );
}

#[test]
fn a_synthesized_row_need_not_override_an_undeclared_column() {
    // `copy_from: none` makes every column of a synthesized row empty unless
    // an override sets it. The body may not name `order_id` or `wt`, so it
    // cannot override them: they stay empty on the synthesized row, and the
    // body compiles at every call whatever extra columns the caller has.
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
            copy_from: none
            overrides:
              v: "v + 1"
              department: "department"
"#,
    ))
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
}

/// A body Sink `keep` reading Transform `t`, with `config` appended to its
/// `config:` block.
fn body_sink(config: &str) -> String {
    comp_out(
        "t",
        &format!(
            r#"  - type: transform
    name: t
    input: inp
    config:
      cxl: |
        emit r = v
  - type: sink
    name: keep
    input: t
    config:
      name: keep
      type: csv
      path: keep.csv
{config}"#
        ),
    )
}

#[test]
fn a_body_sink_mapping_is_checked_against_the_row_its_call_feeds_it() {
    // The body Sink's row is the caller's row plus `r`: a mapping reading a
    // column no row carries is E365, as it is inline.
    let Err(diags) = compile(&body_sink(
        "      include_unmapped: false\n      mapping:\n        - total: nope\n",
    )) else {
        panic!("`nope` is in no row");
    };
    assert!(
        diags
            .iter()
            .any(|d| d.code == "E365" && d.message.contains("'nope'")),
        "{:#?}",
        describe(&diags)
    );
}

#[test]
fn a_body_sink_mapping_colliding_with_a_passed_through_column_is_refused() {
    // `include_unmapped: true` writes the caller's `wt`, which passes
    // through the body, beside the mapped `wt`: E364, as it is inline.
    let Err(diags) = compile(&body_sink(
        "      include_unmapped: true\n      mapping:\n        - wt: r\n",
    )) else {
        panic!("two `wt` columns");
    };
    assert!(
        diags
            .iter()
            .any(|d| d.code == "E364" && d.message.contains("'wt'")),
        "{:#?}",
        describe(&diags)
    );
}

#[test]
fn a_log_field_typo_is_never_answered_with_an_undeclared_column() {
    // `wtt` is one edit from the caller's `wt`, which the port does not
    // declare: neither the suggestion nor the listed columns may offer it.
    let Err(diags) = compile(&comp(
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
      log:
        - name: l
          level: info
          when: per_record
          every: 1
          fields: [wtt]
          message: "m"
"#,
    )) else {
        panic!("`wtt` names nothing");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags[0].code, "E374", "{:#?}", describe(&diags));
    assert!(
        !diags[0].message.contains("`wt`") && !diags[0].message.contains("[wt]"),
        "only columns the body may use are offered: {}",
        diags[0].message
    );
}

#[test]
fn a_body_sink_mapping_typo_is_never_answered_with_an_undeclared_column() {
    let Err(diags) = compile(&body_sink(
        "      include_unmapped: false\n      mapping:\n        - total: wtt\n",
    )) else {
        panic!("`wtt` is in no row");
    };
    let refused: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == "E365").collect();
    assert_eq!(refused.len(), 1, "{:#?}", describe(&diags));
    let help = refused[0].help.as_deref().unwrap_or_default();
    assert!(
        !help.contains("'wt'") && !help.contains("total: wt`"),
        "only columns the body may use are offered: {help}"
    );
}

#[test]
fn an_int_caller_column_is_refused_by_a_float_port() {
    // Nothing converts a value at the port, so an `int` column does not fit a
    // `float` declaration.
    let composition = comp_declaring(
        &[
            "{ name: v, type: float }",
            "{ name: department, type: string }",
        ],
        IDENTITY_BODY,
    );
    assert_port_refuses(
        compile_in(PIPELINE, &[("compositions/c.comp.yaml", &composition)]),
        &["input port `inp`", "`v`", "`float`", "`int`", "\"src\""],
        &["emit v = v.to_float()"],
    );
}

/// [`COMBINE_PIPELINE`] with `wt` on the second Source too, so both
/// Combine inputs carry a `wt`.
fn combine_pipeline_with_wt_on_both() -> String {
    COMBINE_PIPELINE.replace(
        "        - { name: extra, type: string }\n",
        "        - { name: extra, type: string }\n        - { name: wt, type: int }\n",
    )
}

#[test]
fn a_combine_naming_a_column_both_inputs_carry_undeclared_is_refused() {
    // Neither port declares `wt`, though both callers have it: the bare
    // name is a column the body may not use, not a missing one.
    let compiled = compile_in(
        &combine_pipeline_with_wt_on_both(),
        &[(
            "compositions/c.comp.yaml",
            &combine_comp("a.department == b.department", "emit z = wt"),
        )],
    );
    let Err(diags) = compiled else {
        panic!("`wt` is not declared on either port");
    };
    let refused: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == "E169").collect();
    assert_eq!(refused.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert!(
        refused[0].message.contains("`wt`"),
        "{}",
        refused[0].message
    );
}

#[test]
fn an_ambiguous_name_offers_only_the_inputs_the_body_may_name() {
    // `pb` declares `wt`, `pa` does not: the help names `b.wt` only.
    let composition = combine_comp("a.department == b.department", "emit z = wt");
    let composition = composition.replace(
        "    pb:\n      schema:\n        - { name: department, type: string }\n        - { name: label, type: string }\n",
        "    pb:\n      schema:\n        - { name: department, type: string }\n        - { name: label, type: string }\n        - { name: wt, type: int }\n",
    );
    let Err(diags) = compile_in(
        &combine_pipeline_with_wt_on_both(),
        &[("compositions/c.comp.yaml", &composition)],
    ) else {
        panic!("a bare `wt` over two inputs must be qualified");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    let text = format!(
        "{}\n{}",
        diags[0].message,
        diags[0].help.as_deref().unwrap_or_default()
    );
    assert!(text.contains("b.wt"), "the help names `b.wt`: {text}");
    assert!(
        !text.contains("a.wt"),
        "`a.wt` is not the body's to name: {text}"
    );
}

#[test]
fn a_qualified_reference_to_an_undeclared_column_is_refused() {
    // Outside a Combine the qualifier names no input, and the value read is
    // the caller's `wt`.
    assert_undeclared_column_refused(
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit z = anything.wt
"#,
        "wt",
        "- { name: wt, type: int }",
    );
}

#[test]
fn a_type_error_unrelated_to_an_any_column_is_not_blamed_on_it() {
    let composition = comp_declaring(
        &["{ name: v, type: int }", "{ name: department, type: any }"],
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit a = department
        emit b = v.upper()
"#,
    );
    let Err(diags) = compile_in(PIPELINE, &[("compositions/c.comp.yaml", &composition)]) else {
        panic!("`v.upper()` on an int is a type error");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags[0].code, "E200", "{:#?}", describe(&diags));
    assert!(
        !diags[0].message.contains("department")
            && !diags[0]
                .help
                .as_deref()
                .unwrap_or_default()
                .contains("department"),
        "the error is about `v`, not `department`: {:#?}",
        describe(&diags)
    );
}

/// The one diagnostic `body` draws, which must have code `code` and name
/// node `node` in its message.
fn assert_body_refused_with(body: &str, code: &str, node: &str) {
    let Err(diags) = compile(&comp(body)) else {
        panic!("the body must not compile");
    };
    let coded: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == code).collect();
    assert!(
        !coded.is_empty(),
        "{code} expected: {:#?}",
        describe(&diags)
    );
    assert!(
        coded
            .iter()
            .any(|d| d.message.contains(&format!("\"{node}\""))),
        "{code} names node {node:?}: {:#?}",
        describe(&diags)
    );
    assert!(
        diags.iter().all(|d| d.code == code),
        "only {code}: {:#?}",
        describe(&diags)
    );
}

#[test]
fn a_body_cycle_is_refused() {
    assert_body_refused_with(
        r#"  - type: transform
    name: a
    input: b
    config:
      cxl: |
        emit p = wt
  - type: transform
    name: b
    input: a
    config:
      cxl: |
        emit q = 1
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
"#,
        "E107",
        "a",
    );
}

#[test]
fn a_body_node_reading_itself_is_refused() {
    assert_body_refused_with(
        r#"  - type: transform
    name: a
    input: a
    config:
      cxl: |
        emit p = wt
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
"#,
        "E107",
        "a",
    );
}

#[test]
fn a_body_node_reading_a_name_the_body_does_not_have_is_refused() {
    assert_body_refused_with(
        r#"  - type: transform
    name: a
    input: inpp
    config:
      cxl: |
        emit p = wt
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
"#,
        "E004",
        "a",
    );
}

#[test]
fn a_top_level_node_listed_before_its_input_is_bound() {
    // The bind walk orders every scope by input, the pipeline's own nodes
    // included.
    let pipeline = r#"
pipeline:
  name: forward
nodes:
  - type: sink
    name: out
    input: t
    config:
      name: out
      type: csv
      path: out.csv
  - type: transform
    name: t
    input: src
    config:
      cxl: |
        emit r = v
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: v, type: int }
"#;
    let plan =
        compile_in(pipeline, &[]).unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
    let transforms = plan
        .dag()
        .graph
        .node_weights()
        .filter(|node| matches!(node, PlanNode::Transform { .. }))
        .count();
    assert_eq!(
        transforms, 1,
        "the Transform listed after its reader is lowered"
    );
}

#[test]
fn a_port_declared_nullable_any_admits_every_caller_type() {
    let composition = comp_declaring(
        &[
            "{ name: v, type: { nullable: any } }",
            "{ name: department, type: string }",
        ],
        IDENTITY_BODY,
    );
    compile_in(PIPELINE, &[("compositions/c.comp.yaml", &composition)])
        .unwrap_or_else(|d| panic!("`{{ nullable: any }}` admits an int: {:#?}", describe(&d)));
}

#[test]
fn e102_describes_an_always_empty_caller_column_in_plain_words() {
    let pipeline = PIPELINE
        .replace(
            "  - type: composition\n    name: call\n    input: src\n",
            "  - type: transform\n    name: pre\n    input: src\n    config:\n      cxl: |\n        emit w2 = null\n  - type: composition\n    name: call\n    input: pre\n",
        )
        .replace("      inp: src\n", "      inp: pre\n");
    let composition = comp_declaring(
        &[
            "{ name: v, type: int }",
            "{ name: department, type: string }",
            "{ name: w2, type: { nullable: int } }",
        ],
        IDENTITY_BODY,
    );
    let Err(diags) = compile_in(&pipeline, &[("compositions/c.comp.yaml", &composition)]) else {
        panic!("an always-empty column is not an int");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags[0].code, "E102");
    assert!(
        !diags[0].message.contains("as `any`") && diags[0].message.contains("always empty"),
        "{}",
        diags[0].message
    );
}

#[test]
fn e102_for_a_port_fed_by_a_route_branch_says_why_and_how_to_fix_it() {
    let pipeline = PIPELINE.replace(
        "  - type: composition\n    name: call\n    input: src\n    use: ../compositions/c.comp.yaml\n    inputs:\n      inp: src\n",
        "  - type: route\n    name: r\n    input: src\n    config:\n      conditions:\n        high: \"v > 5\"\n      default: low\n  - type: composition\n    name: call\n    input: r.high\n    use: ../compositions/c.comp.yaml\n    inputs:\n      inp: r.high\n",
    );
    let Err(diags) = compile_in(
        &pipeline,
        &[("compositions/c.comp.yaml", &comp(IDENTITY_BODY))],
    ) else {
        panic!("a Route branch cannot feed a port yet");
    };
    let e102: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == "E102").collect();
    assert_eq!(e102.len(), 1, "{:#?}", describe(&diags));
    assert!(
        e102[0].message.contains("\"r.high\"")
            && e102[0].message.contains("cannot read")
            && !e102[0].message.contains("produces no rows"),
        "{}",
        e102[0].message
    );
    let help = e102[0].help.as_deref().unwrap_or_default();
    assert!(
        help.contains("input: r.high") && help.contains("filter true") && help.contains("#888"),
        "{help}"
    );
}

#[test]
fn w101_points_at_the_body_line_that_writes_the_column_and_gives_a_fix() {
    let composition = comp(
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit wt = 2
"#,
    );
    let warned = w101(PIPELINE, &composition);
    assert_eq!(warned.len(), 1, "{:#?}", describe(&warned));
    let line = composition
        .lines()
        .position(|l| l.contains("- type: transform"))
        .expect("the node line") as u32
        + 1;
    assert_eq!(
        warned[0].primary.span,
        clinker_core_types::Span::line_only(line)
    );
    let help = warned[0].help.as_deref().expect("W101 carries a fix");
    assert!(
        help.contains("- { name: wt, type: int }") && help.contains("c.comp.yaml"),
        "{help}"
    );
}

#[test]
fn a_column_a_merge_input_carries_undeclared_stays_unnamed() {
    // `pa` declares `wt`, `pb` does not: after the Merge a row may come from
    // `pb`, so `wt` is still the caller's.
    let composition = r#"_compose:
  name: c
  inputs:
    pa:
      schema:
        - { name: v, type: int }
        - { name: wt, type: int }
    pb:
      schema:
        - { name: v, type: int }
  outputs:
    out: last
  config_schema: {}

nodes:
  - type: merge
    name: m
    inputs: [pa, pb]
  - type: transform
    name: last
    input: m
    config:
      cxl: |
        emit z = wt
"#;
    let pipeline = PIPELINE.replace("      inp: src\n", "      pa: src\n      pb: src\n");
    assert_refused(
        compile_in(&pipeline, &[("compositions/c.comp.yaml", composition)]),
        "last",
        "wt",
        "pb",
        "- { name: wt, type: int }",
    );
}

#[test]
fn a_column_carried_through_a_reshape_stays_unnamed() {
    assert_refused(
        compile(&comp(
            r#"  - type: reshape
    name: rsh
    input: inp
    config:
      partition_by: [department]
      rules:
        - name: r1
          when: "v > 5"
          mutate:
            set:
              v: "v + 100"
  - type: transform
    name: last
    input: rsh
    config:
      cxl: |
        emit z = wt
"#,
        )),
        "last",
        "wt",
        "inp",
        "- { name: wt, type: int }",
    );
}

#[test]
fn a_driver_column_carried_through_a_collect_combine_stays_unnamed() {
    // Collect mode passes the driver's columns through; `order_id` is the
    // caller's and `pa` does not declare it.
    let composition = r#"_compose:
  name: c
  inputs:
    pa:
      schema:
        - { name: department, type: string }
        - { name: v, type: int }
    pb:
      schema:
        - { name: department, type: string }
        - { name: label, type: string }
  outputs:
    out: last
  config_schema: {}

nodes:
  - type: combine
    name: j
    input:
      a: pa
      b: pb
    config:
      where: "a.department == b.department"
      match: collect
      on_miss: null_fields
      propagate_ck: driver
      drive: a
      cxl: ""
  - type: transform
    name: last
    input: j
    config:
      cxl: |
        emit z = order_id
"#;
    assert_refused(
        compile_in(
            COMBINE_PIPELINE,
            &[("compositions/c.comp.yaml", composition)],
        ),
        "last",
        "order_id",
        "pa",
        "- { name: order_id, type: string }",
    );
}

#[test]
fn an_aggregate_group_key_stays_nameable_after_the_aggregate() {
    // The group key is the caller's declared column: later nodes may still
    // name it, and an undeclared column is still refused after a nested
    // call that aggregates.
    compile(&comp(
        r#"  - type: aggregate
    name: g
    input: inp
    config:
      group_by: [department]
      cxl: |
        emit s = sum(v)
  - type: transform
    name: last
    input: g
    config:
      cxl: |
        emit d = department
"#,
    ))
    .unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
}

/// A composition `inner` whose body declares node `t` twice.
const DUPLICATE_NODE_COMP: &str = r#"_compose:
  name: inner
  inputs:
    inp:
      schema:
        - { name: v, type: { nullable: int } }
  outputs:
    out: t
  config_schema: {}

nodes:
  - type: transform
    name: t
    input: inp
    config:
      cxl: |
        emit w = 1
  - type: transform
    name: t
    input: inp
    config:
      cxl: |
        emit w = 2
"#;

/// A composition `outer` whose body calls `inner.comp.yaml`.
const OUTER_OF_INNER_COMP: &str = r#"_compose:
  name: outer
  inputs:
    inp:
      schema:
        - { name: v, type: { nullable: int } }
  outputs:
    out: nn
  config_schema: {}

nodes:
  - type: composition
    name: nn
    input: inp
    use: ./inner.comp.yaml
    inputs:
      inp: inp
"#;

#[test]
fn a_duplicate_node_name_in_a_nested_body_is_reported_once_and_alone() {
    // A body that is refused before it binds must leave the calling scope
    // as it found it: the pipeline's own nodes are not reported as bound
    // without composition marks.
    let pipeline = PIPELINE.replace("c.comp.yaml", "outer.comp.yaml");
    let diags = bind_in(
        &pipeline,
        &[
            ("compositions/outer.comp.yaml", OUTER_OF_INNER_COMP),
            ("compositions/inner.comp.yaml", DUPLICATE_NODE_COMP),
        ],
    );
    let codes: Vec<&str> = diags.iter().map(|d| d.code.as_str()).collect();
    assert_eq!(codes, ["E001"], "{:#?}", describe(&diags));
    assert!(
        diags[0].message.contains("duplicate node name: \"t\""),
        "{}",
        diags[0].message
    );
}

#[test]
fn duplicate_node_names_at_two_calls_are_reported_at_each_and_alone() {
    let pipeline = format!(
        "{}  - type: composition
    name: second
    input: src
    use: ../compositions/inner.comp.yaml
    inputs:
      inp: src
",
        pipeline_with_v("{ nullable: int }").replace("c.comp.yaml", "outer.comp.yaml")
    );
    let diags = bind_in(
        &pipeline,
        &[
            ("compositions/outer.comp.yaml", OUTER_OF_INNER_COMP),
            ("compositions/inner.comp.yaml", DUPLICATE_NODE_COMP),
        ],
    );
    let codes: Vec<&str> = diags.iter().map(|d| d.code.as_str()).collect();
    assert_eq!(codes, ["E001", "E001"], "{:#?}", describe(&diags));
}

#[test]
fn a_dotted_node_name_in_a_nested_body_is_reported_alone() {
    let dotted = DUPLICATE_NODE_COMP.replacen("    name: t\n", "    name: a.b\n", 1);
    let pipeline = PIPELINE.replace("c.comp.yaml", "outer.comp.yaml");
    let diags = bind_in(
        &pipeline,
        &[
            ("compositions/outer.comp.yaml", OUTER_OF_INNER_COMP),
            ("compositions/inner.comp.yaml", &dotted),
        ],
    );
    let codes: Vec<&str> = diags.iter().map(|d| d.code.as_str()).collect();
    assert_eq!(codes, ["E010"], "{:#?}", describe(&diags));
}

/// `PIPELINE` with a second call of the same composition, `again`, fed by
/// `feed` (a node the caller adds with `extra_nodes`).
fn pipeline_calling_twice(extra_nodes: &str, feed: &str) -> String {
    format!(
        "{}{extra_nodes}  - type: composition
    name: again
    input: {feed}
    use: ../compositions/c.comp.yaml
    inputs:
      inp: {feed}
",
        PIPELINE
    )
}

#[test]
fn a_body_that_does_not_handle_an_empty_value_is_refused_at_each_call_of_one_pipeline() {
    // The check fails the same way at both calls; each call reports it,
    // naming itself.
    let composition = comp_declaring(
        &["{ name: v, type: { nullable: int } }"],
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        filter v > 6
"#,
    );
    let Err(diags) = compile_in(
        &pipeline_calling_twice("", "src"),
        &[("compositions/c.comp.yaml", &composition)],
    ) else {
        panic!("the body does not handle an empty `v`");
    };
    let named: Vec<&str> = diags
        .iter()
        .map(|d| {
            assert_eq!(d.code, "E200", "{:#?}", describe(&diags));
            if d.message.contains("in composition \"call\"") {
                "call"
            } else if d.message.contains("in composition \"again\"") {
                "again"
            } else {
                panic!("names a call: {}", d.message)
            }
        })
        .collect();
    assert_eq!(named, ["call", "again"], "{:#?}", describe(&diags));
}

#[test]
fn a_check_that_passed_for_one_call_does_not_pass_a_call_with_other_types() {
    // `w` is declared `any`, so the check sees the type each call provides:
    // it passes for the `int` caller and must still fail for the `string`
    // one, although both calls also narrow `v`.
    let composition = comp_declaring(
        &[
            "{ name: v, type: { nullable: int } }",
            "{ name: w, type: any }",
        ],
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit a = w + 1
"#,
    );
    let pipeline = pipeline_calling_twice(
        r#"  - type: transform
    name: as_text
    input: src
    config:
      cxl: |
        emit w = w.to_string()
"#,
        "as_text",
    )
    .replace("{ name: wt, type: int }", "{ name: w, type: int }");
    let Err(diags) = compile_in(&pipeline, &[("compositions/c.comp.yaml", &composition)]) else {
        panic!("`w + 1` does not compile for a string `w`");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags[0].code, "E200", "{:#?}", describe(&diags));
    assert!(
        diags[0].message.contains("composition \"again\""),
        "{}",
        diags[0].message
    );
}

/// A chain of `depth` compositions: `c.comp.yaml` calls `l2.comp.yaml`,
/// which calls `l3.comp.yaml`, and so on; the innermost adds `flag`. Every
/// port declares `v` as `{ nullable: int }`, so a pipeline feeding an `int`
/// has each level checked against its declared types and then bound
/// against the type it is fed.
fn nullable_chain(depth: usize) -> Vec<(String, String)> {
    (1..=depth)
        .map(|level| {
            let file = if level == 1 {
                "compositions/c.comp.yaml".to_string()
            } else {
                format!("compositions/l{level}.comp.yaml")
            };
            let body = if level == depth {
                r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit flag = 1
"#
                .to_string()
            } else {
                format!(
                    r#"  - type: composition
    name: last
    input: inp
    use: ./l{}.comp.yaml
    inputs:
      inp: inp
"#,
                    level + 1
                )
            };
            (
                file,
                comp_declaring(&["{ name: v, type: { nullable: int } }"], &body),
            )
        })
        .collect()
}

#[test]
fn body_binds_grow_linearly_with_nesting_depth() {
    // Each level adds the same number of binds: a check of its body against
    // the declared types, and the bind that produces its call's rows. A
    // check that passed once is not run again for the nested call the
    // producing bind reaches, so the count does not grow with the square
    // of the depth.
    let binds = |depth: usize| {
        let files = nullable_chain(depth);
        let refs: Vec<(&str, &str)> = files
            .iter()
            .map(|(path, contents)| (path.as_str(), contents.as_str()))
            .collect();
        body_binds_in(PIPELINE, &refs)
    };
    let (at_4, at_6, at_8) = (binds(4), binds(6), binds(8));
    assert!(at_4 > 0);
    assert_eq!(
        at_8 - at_6,
        at_6 - at_4,
        "two more levels add the same number of binds: {at_4} at depth 4, {at_6} at 6, {at_8} at 8"
    );
}

#[test]
fn a_check_that_passed_for_one_call_stands_for_a_call_fed_the_same_columns_from_another_source() {
    // Both Sources declare the same columns with the same types; the check
    // of the body against its declared types reads nothing else of the
    // rows, so the second call only binds against what it is fed.
    let composition = comp_declaring(&["{ name: v, type: { nullable: int } }"], IDENTITY_BODY);
    let pipeline = pipeline_calling_twice(
        r#"  - type: source
    name: src2
    config:
      name: src2
      type: csv
      path: in2.csv
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: v, type: int }
        - { name: wt, type: int }
"#,
        "src2",
    );
    assert_eq!(
        body_binds_in(&pipeline, &[("compositions/c.comp.yaml", &composition)]),
        3,
        "a check and a produce for the first call, a produce for the second"
    );
}

/// A composition with a second input port `opt`, declared optional, that
/// body node `u` reads; `last` reads `inp` as every case does.
const OPTIONAL_PORT_COMPOSITION: &str = r#"_compose:
  name: c
  inputs:
    inp:
      schema:
        - { name: v, type: int }
    opt:
      required: false
      schema:
        - { name: v, type: int }
  outputs:
    out: last
  config_schema: {}

nodes:
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit a = v
  - type: transform
    name: u
    input: opt
    config:
      cxl: |
        emit b = v
"#;

#[test]
fn a_body_node_reading_an_input_port_the_call_does_not_bind_is_refused() {
    // `opt` is optional, so the call may leave it unbound; but `u` reads
    // it and would receive no row. The call is refused, naming the call,
    // the node and the port, with the binding to add at the call site.
    let Err(diags) = compile(OPTIONAL_PORT_COMPOSITION) else {
        panic!("a body node reading an unbound port must not compile");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    let refused = &diags[0];
    assert_eq!(refused.code, "E104", "{:#?}", describe(&diags));
    for fragment in [
        "composition \"call\"",
        "node \"u\"",
        "in `compositions/c.comp.yaml`",
        "reads input port `opt`",
        "this call does not bind",
    ] {
        assert!(
            refused.message.contains(fragment),
            "{fragment}: {}",
            refused.message
        );
    }
    let help = refused.help.as_deref().unwrap_or_default();
    for fragment in [
        "add `opt: <node>` under `inputs:` of composition \"call\"",
        "remove node \"u\" from `compositions/c.comp.yaml`",
    ] {
        assert!(help.contains(fragment), "{fragment}: {help}");
    }
}

#[test]
fn binding_the_port_the_help_names_compiles_the_call() {
    let bound = PIPELINE.replace("      inp: src\n", "      inp: src\n      opt: src\n");
    compile_in(
        &bound,
        &[("compositions/c.comp.yaml", OPTIONAL_PORT_COMPOSITION)],
    )
    .unwrap_or_else(|d| panic!("compiles once `opt` is bound: {:#?}", describe(&d)));
}

#[test]
fn an_optional_port_no_node_reads_may_stay_unbound() {
    let unread = OPTIONAL_PORT_COMPOSITION.replace("    input: opt\n", "    input: inp\n");
    compile(&unread).unwrap_or_else(|d| panic!("compiles: {:#?}", describe(&d)));
}

/// A composition `enrich` with two output ports, `out` and `audit`.
const TWO_OUTPUT_COMPOSITION: &str = r#"_compose:
  name: enrich
  inputs:
    inp:
      schema:
        - { name: v, type: int }
  outputs:
    out: t
    audit: a
  config_schema: {}

nodes:
  - type: transform
    name: t
    input: inp
    config:
      cxl: |
        emit a = v
  - type: transform
    name: a
    input: inp
    config:
      cxl: |
        emit extra = 1
"#;

/// The one E102 a call whose port reads `upstream` draws.
fn port_upstream_refusal(upstream: &str, extra_nodes: &str) -> Diagnostic {
    let pipeline = PIPELINE
        .replace("      inp: src\n", &format!("      inp: {upstream}\n"))
        .replace(
            "  - type: composition\n    name: call\n    input: src\n",
            &format!("{extra_nodes}  - type: composition\n    name: call\n    input: src\n"),
        );
    let Err(diags) = compile_in(
        &pipeline,
        &[
            ("compositions/c.comp.yaml", &comp(IDENTITY_BODY)),
            ("compositions/enrich.comp.yaml", TWO_OUTPUT_COMPOSITION),
        ],
    ) else {
        panic!("a port reading {upstream} must not compile");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags[0].code, "E102", "{:#?}", describe(&diags));
    diags[0].clone()
}

#[test]
fn a_port_reading_another_output_port_of_a_composition_is_told_what_a_call_carries() {
    let refused = port_upstream_refusal(
        "enrich.audit",
        r#"  - type: composition
    name: enrich
    input: src
    use: ../compositions/enrich.comp.yaml
    inputs:
      inp: src
"#,
    );
    for fragment in [
        "cannot read \"enrich.audit\"",
        "output port `audit` of composition \"enrich\"",
        "only the rows of its first output port",
    ] {
        assert!(
            refused.message.contains(fragment),
            "{fragment}: {}",
            refused.message
        );
    }
    assert!(
        !refused.message.contains("Route or a Cull"),
        "{}",
        refused.message
    );
    let help = refused.help.as_deref().unwrap_or_default();
    assert!(help.contains("`inp: enrich`"), "{help}");
}

#[test]
fn a_port_reading_a_port_of_a_node_without_ports_is_told_so() {
    let refused = port_upstream_refusal("src.rows", "");
    assert!(
        refused
            .message
            .contains("\"src\" is a source node, which has no output port `rows`"),
        "{}",
        refused.message
    );
    let help = refused.help.as_deref().unwrap_or_default();
    assert!(help.contains("`inp: src`"), "{help}");
}

/// The E107s a body with `nodes` raises; nothing else may be raised.
fn cycle_reports(nodes: &str) -> Vec<Diagnostic> {
    let Err(diags) = compile(&comp(nodes)) else {
        panic!("a body with a cycle must not compile");
    };
    assert!(
        diags.iter().all(|d| d.code == "E107"),
        "only E107: {:#?}",
        describe(&diags)
    );
    diags
}

#[test]
fn a_cycle_is_reported_once_naming_every_node_on_it() {
    let two = cycle_reports(
        r#"  - type: transform
    name: a
    input: b
    config:
      cxl: |
        emit p = 1
  - type: transform
    name: b
    input: a
    config:
      cxl: |
        emit q = 1
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
"#,
    );
    assert_eq!(two.len(), 1, "{:#?}", describe(&two));
    assert!(
        two[0]
            .message
            .contains("composition nodes \"a\" and \"b\" read each other in a cycle"),
        "{}",
        two[0].message
    );
    let three = cycle_reports(
        r#"  - type: transform
    name: a
    input: c
    config:
      cxl: |
        emit p = 1
  - type: transform
    name: b
    input: a
    config:
      cxl: |
        emit q = 1
  - type: transform
    name: c
    input: b
    config:
      cxl: |
        emit s = 1
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
"#,
    );
    assert_eq!(three.len(), 1, "{:#?}", describe(&three));
    assert!(
        three[0]
            .message
            .contains("composition nodes \"a\", \"b\" and \"c\" read each other in a cycle"),
        "{}",
        three[0].message
    );
}

/// The one E200 a body behind a `{ nullable: int }` `v` draws when a node
/// cannot use `v` while it may be empty.
fn empty_value_refusal(body: &str) -> Diagnostic {
    let composition = comp_declaring(
        &[
            "{ name: v, type: { nullable: int } }",
            "{ name: department, type: string }",
        ],
        body,
    );
    let Err(diags) = compile_in(PIPELINE, &[("compositions/c.comp.yaml", &composition)]) else {
        panic!("a body that does not handle an empty `v` must not compile");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags[0].code, "E200", "{:#?}", describe(&diags));
    assert!(
        diags[0]
            .message
            .contains("cannot use `v` while it may be empty"),
        "{}",
        diags[0].message
    );
    diags[0].clone()
}

/// The help's paste-ready line: the text between the backticks that follow
/// the body file's name.
fn pasted_line(help: &str) -> &str {
    help.split("`: `")
        .nth(1)
        .and_then(|rest| rest.split("`, so the composition").next())
        .unwrap_or_else(|| panic!("the help prints a line to paste: {help}"))
}

#[test]
fn the_empty_value_help_prints_the_line_that_fails_not_the_first_read() {
    // `v + 1` compiles over a possibly empty `v`; `filter v > 6` does not.
    // The help prints the filter line.
    let refused = empty_value_refusal(
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit a = v + 1
        filter v > 6
"#,
    );
    let help = refused.help.as_deref().unwrap_or_default();
    assert_eq!(pasted_line(help), "filter (v ?? 0) > 6", "{help}");
}

#[test]
fn the_empty_value_message_keeps_its_detail_after_the_fix_in_the_author_s_spelling() {
    let refused = empty_value_refusal(
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        filter v > 6
"#,
    );
    assert!(
        !refused.message.contains('(') && !refused.message.contains("Nullable"),
        "the first sentence carries no type checker detail: {}",
        refused.message
    );
    let help = refused.help.as_deref().unwrap_or_default();
    let (fix, detail) = help
        .split_once(". Technical detail: ")
        .unwrap_or_else(|| panic!("the detail follows the fix: {help}"));
    assert!(fix.contains("`filter (v ?? 0) > 6`"), "{fix}");
    assert!(
        detail.contains("{ nullable: bool }") && !detail.contains("Nullable("),
        "{detail}"
    );
}

#[test]
fn the_empty_value_help_prints_each_authored_entry_as_the_author_wrote_it() {
    // A condition the planner wraps in its own program is printed as the
    // YAML entry the author wrote, never as the wrapper. (A Cull rule is
    // wired the same way, but an aggregate over a possibly empty column
    // compiles, so no Cull rule reaches this error.)
    let cases: &[(&str, &str, &str)] = &[
        (
            "route branch",
            "high: '(v ?? 0) > 6'",
            r#"  - type: route
    name: r
    input: inp
    config:
      mode: exclusive
      conditions:
        high: 'v > 6'
      default: low
  - type: transform
    name: last
    input: r.high
    config:
      cxl: |
        emit a = 1
"#,
        ),
        (
            "reshape when",
            "when: '(v ?? 0) > 6'",
            r#"  - type: reshape
    name: last
    input: inp
    config:
      partition_by: [department]
      rules:
        - name: r1
          when: 'v > 6'
          mutate:
            set:
              v: '0'
"#,
        ),
        (
            "log condition",
            "condition: '(v ?? 0) > 6'",
            r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit a = 1
      log:
        - name: l
          level: info
          when: per_record
          every: 1
          condition: 'v > 6'
          message: "m"
"#,
        ),
    ];
    for (label, expected, body) in cases {
        let refused = empty_value_refusal(body);
        let help = refused.help.as_deref().unwrap_or_default();
        assert_eq!(pasted_line(help), *expected, "{label}: {help}");
        let fix = help.split(". Technical detail: ").next().unwrap_or(help);
        assert!(!fix.contains("filter"), "{label} prints no wrapper: {fix}");
    }
}

#[test]
fn only_the_column_whose_emptiness_fails_the_node_is_blamed() {
    // The failing filter reads neither column directly; `v` never empty
    // on its own makes it compile, `w` never empty on its own does not.
    let composition = comp_declaring(
        &[
            "{ name: v, type: { nullable: int } }",
            "{ name: wt, type: { nullable: int } }",
        ],
        r#"  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        let x = v
        let y = wt
        filter x > 6
        emit b = y ?? 0
"#,
    );
    let Err(diags) = compile_in(PIPELINE, &[("compositions/c.comp.yaml", &composition)]) else {
        panic!("the filter does not handle an empty `v`");
    };
    assert_eq!(diags.len(), 1, "one column blamed: {:#?}", describe(&diags));
    assert!(
        diags[0]
            .message
            .contains("cannot use `v` while it may be empty"),
        "{}",
        diags[0].message
    );
    let help = diags[0].help.as_deref().unwrap_or_default();
    assert_eq!(pasted_line(help), "let x = (v ?? 0)", "{help}");
}

/// The one E200 a Combine `j` over a `pa` declaring `v` as
/// `{ nullable: int }` draws, with `where_clause` and `cxl`, called with a
/// caller whose `v` is `caller_v`.
fn combine_empty_value_refusal(caller_v: &str, where_clause: &str, cxl: &str) -> Diagnostic {
    let pipeline = COMBINE_PIPELINE.replace(
        "        - { name: v, type: int }",
        &format!("        - {{ name: v, type: {caller_v} }}"),
    );
    let composition = combine_comp_with_v("{ nullable: int }", where_clause, cxl);
    let Err(diags) = compile_in(&pipeline, &[("compositions/c.comp.yaml", &composition)]) else {
        panic!("the Combine does not handle an empty `a.v`");
    };
    assert_eq!(diags.len(), 1, "{caller_v}: {:#?}", describe(&diags));
    let diag = &diags[0];
    assert_eq!(diag.code, "E200", "{:#?}", describe(&diags));
    assert!(
        diag.message.starts_with(
            "node \"j\" in composition \"call\" cannot use `v` while it may be empty: \
             input port `pa` declares `v` as `{ nullable: int }`"
        ),
        "{}",
        diag.message
    );
    diag.clone()
}

const COMBINE_EMITS: &str =
    "emit department = a.department\n        emit v = a.v\n        emit label = b.label";

#[test]
fn a_combine_where_that_does_not_handle_an_empty_value_is_refused_at_every_call() {
    // `a.v > 5` is empty when `a.v` is: the predicate is not a yes or no.
    // The help prints the `where:` entry with the empty case handled.
    for caller in ["int", "{ nullable: int }"] {
        let refused = combine_empty_value_refusal(
            caller,
            "a.department == b.department and a.v > 5",
            COMBINE_EMITS,
        );
        let help = refused.help.as_deref().unwrap_or_default();
        assert_eq!(
            pasted_line(help),
            "where: 'a.department == b.department and (a.v ?? 0) > 5'",
            "{caller}: {help}"
        );
        assert!(
            help.contains("`- { name: v, type: int }`") && help.contains("c.comp.yaml"),
            "{help}"
        );
    }
}

#[test]
fn a_combine_body_that_does_not_handle_an_empty_value_is_refused_at_every_call() {
    // The body reads `a.v` by its qualified name; the check that the body
    // handles an empty `a.v` reads it as it reads a bare `v`.
    for caller in ["int", "{ nullable: int }"] {
        let refused = combine_empty_value_refusal(
            caller,
            "a.department == b.department and (a.v ?? 0) > 5",
            &format!("filter a.v > 5\n        {COMBINE_EMITS}"),
        );
        let help = refused.help.as_deref().unwrap_or_default();
        assert_eq!(
            pasted_line(help),
            "filter (a.v ?? 0) > 5",
            "{caller}: {help}"
        );
    }
}

/// A three-node cycle `a` <- `c` <- `b` <- `a`, beside `last`.
const THREE_CYCLE: &str = r#"  - type: transform
    name: a
    input: c
    config:
      cxl: |
        emit p = 1
  - type: transform
    name: b
    input: a
    config:
      cxl: |
        emit q = 1
  - type: transform
    name: c
    input: b
    config:
      cxl: |
        emit s = 1
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
"#;

/// The help of the one E107 a body with `comp_yaml` raises.
fn cycle_help(comp_yaml: &str) -> String {
    let Err(diags) = compile(comp_yaml) else {
        panic!("a body with a cycle must not compile");
    };
    assert_eq!(diags.len(), 1, "{:#?}", describe(&diags));
    assert_eq!(diags[0].code, "E107", "{:#?}", describe(&diags));
    diags[0].help.clone().expect("a fix")
}

#[test]
fn the_e107_fix_for_a_cycle_never_leaves_a_node_reading_the_output_node() {
    // Output `a`: changing `b`, the node that reads `a`, leaves
    // `inp` -> `b` -> `c` -> `a`. Changing `a` or `c` would leave `b`
    // reading the output node, which fails when the call runs.
    let help = cycle_help(&comp_out("a", THREE_CYCLE));
    assert_eq!(
        help,
        "set `input:` of node \"b\" to an input port of the composition: `input: inp`"
    );
    // Output `last`: no node on the cycle reads an output node, so
    // changing the first one is enough.
    let help = cycle_help(&comp(THREE_CYCLE));
    assert_eq!(
        help,
        "set `input:` of node \"a\" to an input port of the composition: `input: inp`"
    );
}

#[test]
fn the_e107_fix_for_a_node_reading_itself_reads_a_port() {
    let help = cycle_help(&comp(
        r#"  - type: transform
    name: a
    input: a
    config:
      cxl: |
        emit p = 1
  - type: transform
    name: last
    input: inp
    config:
      cxl: |
        emit r = v
"#,
    ));
    assert_eq!(
        help,
        "set `input:` of node \"a\" to an input port of the composition: `input: inp`"
    );
}
