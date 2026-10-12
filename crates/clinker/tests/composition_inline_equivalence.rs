//! A composition call writes what its body written inline would write.
//!
//! Each case runs two pipelines through the real `clinker` binary: one that
//! calls a composition, and its reference, the same nodes written inline at
//! the call site. The call's output must match the reference byte for byte,
//! and both must match the bytes the case states. The port's columns are
//! only the names the body may use; the body receives the caller's records
//! as they are, so every column the port does not declare passes through in
//! the caller's position.

use std::path::Path;
use std::process::{Command, Output};

/// The pipeline file, relative to the workspace root.
const PIPELINE: &str = "pipelines/pipeline.yaml";

/// Orders split across two departments, with a weight column the
/// compositions never declare.
const ORDERS: &str = "order_id,department,v,wt\nO1,a,5,0\nO1,b,6,1\nO2,a,7,1\n";

/// The Source every orders case reads: `order_id, department, v, wt`.
const ORDERS_SOURCE: &str = r#"  - type: source
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
"#;

fn pipeline(name: &str, nodes: &str) -> String {
    format!(
        r#"pipeline:
  name: {name}
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
{nodes}"#
    )
}

/// A Sink `out` reading `input`, sorted by `sort` so hash-grouped output is
/// written in one order.
fn sink(input: &str, sort: &[&str]) -> String {
    let order: String = sort
        .iter()
        .map(|field| format!("        - field: {field}\n"))
        .collect();
    format!(
        r#"  - type: sink
    name: out
    input: {input}
    config:
      name: out
      type: csv
      path: out.csv
      sort_order:
{order}"#
    )
}

/// A call `name` of `compositions/{file}`, its port `inp` fed by `upstream`.
fn call(name: &str, file: &str, upstream: &str) -> String {
    format!(
        r#"  - type: composition
    name: {name}
    input: {upstream}
    use: ../compositions/{file}
    inputs:
      inp: {upstream}
"#
    )
}

/// A composition `name` whose port `inp` declares `columns` and whose first
/// output port reads `out_node`.
fn comp(name: &str, columns: &[(&str, &str)], out_node: &str, body: &str) -> String {
    let schema: String = columns
        .iter()
        .map(|(column, ty)| format!("        - {{ name: {column}, type: {ty} }}\n"))
        .collect();
    format!(
        r#"_compose:
  name: {name}
  inputs:
    inp:
      schema:
{schema}  outputs:
    out: {out_node}
  config_schema: {{}}

nodes:
{body}"#
    )
}

fn write_files(root: &Path, files: &[(&str, &str)]) {
    for (relative, contents) in files {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&path, contents).expect("write file");
    }
}

fn run(root: &Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(root)
        .arg("run")
        .arg(PIPELINE)
        .arg("--base-dir")
        .arg(root)
        .arg("--force")
        .output()
        .expect("spawn clinker")
}

fn describe(output: &Output) -> String {
    format!(
        "exit {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Run `files` (the pipeline, any compositions, and `pipelines/in.csv`) in a
/// fresh workspace; return the run and the bytes of `out.csv`.
fn run_workspace(files: &[(&str, &str)]) -> (Output, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    write_files(dir.path(), files);
    let output = run(dir.path());
    let written = std::fs::read(dir.path().join("out.csv"))
        .map(|bytes| String::from_utf8(bytes).expect("utf-8 output"))
        .unwrap_or_else(|_| format!("<no out.csv>\n{}", describe(&output)));
    (output, written)
}

/// Run the composition pipeline and its inline reference; both must exit
/// `exit` and write `expected` to `out.csv`.
fn assert_inline_equivalent(
    composed: &[(&str, &str)],
    inline: &[(&str, &str)],
    exit: i32,
    expected: &str,
) {
    let (inline_run, inline_out) = run_workspace(inline);
    assert_eq!(
        inline_run.status.code(),
        Some(exit),
        "the inline reference: {}",
        describe(&inline_run)
    );
    assert_eq!(inline_out, expected, "the inline reference's out.csv");
    let (composed_run, composed_out) = run_workspace(composed);
    assert_eq!(
        composed_run.status.code(),
        Some(exit),
        "the composition call: {}",
        describe(&composed_run)
    );
    assert_eq!(
        composed_out, inline_out,
        "the composition call writes what its body inline writes"
    );
}

const COMP3_BODY: &str = r#"  - type: aggregate
    name: totals
    input: inp
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit s = sum(v)
"#;

/// The body Aggregate of `COMP3_BODY` written inline, reading `upstream`.
fn comp3_inline(upstream: &str) -> String {
    COMP3_BODY.replace("input: inp", &format!("input: {upstream}"))
}

#[test]
fn a_body_aggregate_behind_a_reordered_subset_port_sums_the_declared_column() {
    let composition = comp(
        "c",
        &[("v", "int"), ("department", "string")],
        "totals",
        COMP3_BODY,
    );
    let composed = pipeline(
        "comp3",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            call("call", "c.comp.yaml", "src"),
            sink("call", &["department"])
        ),
    );
    let inline = pipeline(
        "comp3_inline",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            comp3_inline("src"),
            sink("totals", &["department"])
        ),
    );
    assert_inline_equivalent(
        &[
            (PIPELINE, &composed),
            ("compositions/c.comp.yaml", &composition),
            ("pipelines/in.csv", ORDERS),
        ],
        &[(PIPELINE, &inline), ("pipelines/in.csv", ORDERS)],
        0,
        "department,s\na,12\nb,6\n",
    );
}

#[test]
fn a_body_aggregate_behind_a_prefix_ordered_port_sums_the_declared_column() {
    let composition = comp(
        "c",
        &[
            ("order_id", "string"),
            ("department", "string"),
            ("v", "int"),
        ],
        "totals",
        COMP3_BODY,
    );
    let composed = pipeline(
        "prefix",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            call("call", "c.comp.yaml", "src"),
            sink("call", &["department"])
        ),
    );
    let inline = pipeline(
        "prefix_inline",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            comp3_inline("src"),
            sink("totals", &["department"])
        ),
    );
    assert_inline_equivalent(
        &[
            (PIPELINE, &composed),
            ("compositions/c.comp.yaml", &composition),
            ("pipelines/in.csv", ORDERS),
        ],
        &[(PIPELINE, &inline), ("pipelines/in.csv", ORDERS)],
        0,
        "department,s\na,12\nb,6\n",
    );
}

#[test]
fn a_body_aggregate_then_transform_writes_what_the_inline_nodes_write() {
    // `12 / (s - 6)` divides by zero for O1/b, whose total is 6; that group
    // is dead-lettered and the run exits 2, inline and composed alike.
    let body = r#"  - type: aggregate
    name: totals
    input: inp
    config:
      group_by: [order_id, department]
      cxl: |
        emit order_id = order_id
        emit department = department
        emit s = sum(v)
  - type: transform
    name: ratio
    input: totals
    config:
      cxl: |
        emit r = 12 / (s - 6)
"#;
    let composition = comp(
        "cov",
        &[
            ("v", "int"),
            ("order_id", "string"),
            ("department", "string"),
        ],
        "ratio",
        body,
    );
    let composed = pipeline(
        "comp",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            call("call", "cov.comp.yaml", "src"),
            sink("call", &["order_id", "department"])
        ),
    );
    let inline = pipeline(
        "comp_inline",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            body.replace("input: inp", "input: src"),
            sink("ratio", &["order_id", "department"])
        ),
    );
    assert_inline_equivalent(
        &[
            (PIPELINE, &composed),
            ("compositions/cov.comp.yaml", &composition),
            ("pipelines/in.csv", ORDERS),
        ],
        &[(PIPELINE, &inline), ("pipelines/in.csv", ORDERS)],
        2,
        "order_id,department,s,r\nO1,a,5,-12\nO2,a,7,12\n",
    );
}

#[test]
fn a_keyed_subset_port_drops_the_group_a_sibling_failed() {
    // The Source is keyed on `order_id`; a sibling Transform outside the
    // composition fails O1's first row (`v / wt` with `wt = 0`), so every O1
    // record is dead-lettered, the body's O1 totals included.
    let keyed_source = ORDERS_SOURCE.replace(
        "      path: in.csv\n",
        "      path: in.csv\n      correlation_key: [order_id]\n",
    );
    let body = r#"  - type: aggregate
    name: totals
    input: inp
    config:
      group_by: [order_id, department]
      cxl: |
        emit order_id = order_id
        emit department = department
        emit s = sum(v)
"#;
    let sibling = r#"  - type: transform
    name: per_weight
    input: src
    config:
      cxl: |
        emit q = v / wt
  - type: sink
    name: lines
    input: per_weight
    config:
      name: lines
      type: csv
      path: lines.csv
"#;
    let keyed_sink = |input: &str| {
        sink(input, &["order_id", "department"]).replace(
            "      path: out.csv\n",
            "      path: out.csv\n      include_correlation_keys: true\n",
        )
    };
    let composition = comp(
        "cov",
        &[
            ("order_id", "string"),
            ("department", "string"),
            ("v", "int"),
        ],
        "totals",
        body,
    );
    let composed = pipeline(
        "comp2",
        &format!(
            "{keyed_source}{}{}{sibling}",
            call("call", "cov.comp.yaml", "src"),
            keyed_sink("call")
        ),
    );
    let inline = pipeline(
        "comp2_inline",
        &format!(
            "{keyed_source}{}{}{sibling}",
            body.replace("input: inp", "input: src"),
            keyed_sink("totals")
        ),
    );
    assert_inline_equivalent(
        &[
            (PIPELINE, &composed),
            ("compositions/cov.comp.yaml", &composition),
            ("pipelines/in.csv", ORDERS),
        ],
        &[(PIPELINE, &inline), ("pipelines/in.csv", ORDERS)],
        2,
        "order_id,department,s,$ck.order_id\nO2,a,7,O2\n",
    );
}

#[test]
fn a_transform_body_behind_a_reordered_subset_port_passes_undeclared_columns_through() {
    let body = r#"  - type: transform
    name: t
    input: inp
    config:
      cxl: |
        emit department = department
        emit r = v * 2
"#;
    let composition = comp("c", &[("v", "int"), ("department", "string")], "t", body);
    let composed = pipeline(
        "reordered",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            call("call", "c.comp.yaml", "src"),
            sink("call", &["order_id", "department"])
        ),
    );
    let inline = pipeline(
        "reordered_inline",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            body.replace("input: inp", "input: src"),
            sink("t", &["order_id", "department"])
        ),
    );
    assert_inline_equivalent(
        &[
            (PIPELINE, &composed),
            ("compositions/c.comp.yaml", &composition),
            ("pipelines/in.csv", ORDERS),
        ],
        &[(PIPELINE, &inline), ("pipelines/in.csv", ORDERS)],
        0,
        "order_id,department,v,wt,r\nO1,a,5,0,10\nO1,b,6,1,12\nO2,a,7,1,14\n",
    );
}

#[test]
fn a_composition_fed_by_a_transform_that_adds_a_column_passes_it_through() {
    // The shape issue #1221 reports: a Transform in front of the call adds
    // `marker`, which the port does not declare.
    let source = r#"  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
  - type: transform
    name: widen
    input: src
    config:
      cxl: |
        emit id = id
        emit amount = amount
        emit marker = "widened"
"#;
    let body = r#"  - type: transform
    name: ratio
    input: inp
    config:
      cxl: |
        emit id = id
        emit ratio = id / amount
"#;
    let composition = comp("ratio", &[("id", "int"), ("amount", "int")], "ratio", body);
    let composed = pipeline(
        "comp_widened",
        &format!(
            "{source}{}{}",
            call("ratio_call", "ratio.comp.yaml", "widen"),
            sink("ratio_call", &["id"])
        ),
    );
    let inline = pipeline(
        "comp_widened_inline",
        &format!(
            "{source}{}{}",
            body.replace("input: inp", "input: widen"),
            sink("ratio", &["id"])
        ),
    );
    let input = "id,amount\n1,1\n2,2\n3,3\n";
    assert_inline_equivalent(
        &[
            (PIPELINE, &composed),
            ("compositions/ratio.comp.yaml", &composition),
            ("pipelines/in.csv", input),
        ],
        &[(PIPELINE, &inline), ("pipelines/in.csv", input)],
        0,
        "id,amount,marker,ratio\n1,1,widened,1\n2,2,widened,1\n3,3,widened,1\n",
    );
}

#[test]
fn one_body_called_from_two_upstream_layouts_reads_each_layout() {
    // The second Source lists the same columns in another order; each call
    // site binds the body against its own upstream.
    let second_source = r#"  - type: source
    name: src2
    config:
      name: src2
      type: csv
      path: in2.csv
      schema:
        - { name: wt, type: int }
        - { name: v, type: int }
        - { name: order_id, type: string }
        - { name: department, type: string }
"#;
    let second_input = "wt,v,order_id,department\n0,5,O1,a\n1,6,O1,b\n1,7,O2,a\n";
    let composition = comp(
        "c",
        &[("v", "int"), ("department", "string")],
        "totals",
        COMP3_BODY,
    );
    let second_sink = |input: &str| {
        sink(input, &["department"])
            .replace("name: out\n", "name: out2\n")
            .replace("path: out.csv", "path: out2.csv")
    };
    let composed = pipeline(
        "two_sites",
        &format!(
            "{ORDERS_SOURCE}{second_source}{}{}{}{}",
            call("first", "c.comp.yaml", "src"),
            call("second", "c.comp.yaml", "src2"),
            sink("first", &["department"]),
            second_sink("second")
        ),
    );
    let dir = tempfile::tempdir().expect("tempdir");
    write_files(
        dir.path(),
        &[
            (PIPELINE, &composed),
            ("compositions/c.comp.yaml", &composition),
            ("pipelines/in.csv", ORDERS),
            ("pipelines/in2.csv", second_input),
        ],
    );
    let output = run(dir.path());
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    for file in ["out.csv", "out2.csv"] {
        let written = std::fs::read_to_string(dir.path().join(file)).expect("read output");
        assert_eq!(written, "department,s\na,12\nb,6\n", "{file}");
    }
}

#[test]
fn a_nested_composition_reads_the_columns_its_own_port_declares() {
    // The outer port declares `v, department`; the inner port declares
    // `department, v`. Each body binds against the row that reaches it.
    let inner = comp(
        "inner",
        &[("department", "string"), ("v", "int")],
        "totals",
        COMP3_BODY,
    );
    let outer = comp(
        "outer",
        &[("v", "int"), ("department", "string")],
        "nested",
        r#"  - type: composition
    name: nested
    input: inp
    use: ./inner.comp.yaml
    inputs:
      inp: inp
"#,
    );
    let composed = pipeline(
        "nested",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            call("call", "outer.comp.yaml", "src"),
            sink("call", &["department"])
        ),
    );
    let inline = pipeline(
        "nested_inline",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            comp3_inline("src"),
            sink("totals", &["department"])
        ),
    );
    assert_inline_equivalent(
        &[
            (PIPELINE, &composed),
            ("compositions/outer.comp.yaml", &outer),
            ("compositions/inner.comp.yaml", &inner),
            ("pipelines/in.csv", ORDERS),
        ],
        &[(PIPELINE, &inline), ("pipelines/in.csv", ORDERS)],
        0,
        "department,s\na,12\nb,6\n",
    );
}
