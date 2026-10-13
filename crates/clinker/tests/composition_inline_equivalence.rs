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

#[test]
fn a_body_node_listed_before_its_input_writes_what_the_inline_nodes_write() {
    // The body lists `b` before `a`, the node it reads. The call binds the
    // nodes in input order and writes every row, as the same nodes written
    // inline in input order do.
    let body = |first_input: &str| {
        format!(
            r#"  - type: transform
    name: b
    input: a
    config:
      cxl: |
        emit z = v
  - type: transform
    name: a
    input: {first_input}
    config:
      cxl: |
        emit r = v
"#
        )
    };
    let composition = comp(
        "c",
        &[("v", "int"), ("department", "string")],
        "b",
        &body("inp"),
    );
    let composed = pipeline(
        "forward",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            call("call", "c.comp.yaml", "src"),
            sink("call", &["v"])
        ),
    );
    let inline_nodes = r#"  - type: transform
    name: a
    input: src
    config:
      cxl: |
        emit r = v
  - type: transform
    name: b
    input: a
    config:
      cxl: |
        emit z = v
"#;
    let inline = pipeline(
        "forward_inline",
        &format!("{ORDERS_SOURCE}{inline_nodes}{}", sink("b", &["v"])),
    );
    assert_inline_equivalent(
        &[
            (PIPELINE, &composed),
            ("compositions/c.comp.yaml", &composition),
            ("pipelines/in.csv", ORDERS),
        ],
        &[(PIPELINE, &inline), ("pipelines/in.csv", ORDERS)],
        0,
        "order_id,department,v,wt,r,z\nO1,a,5,0,5,5\nO1,b,6,1,6,6\nO2,a,7,1,7,7\n",
    );
}

/// Departments the orders join to, with a column no port declares.
const DEPARTMENTS: &str = "department,label,extra\na,alpha,X\nb,beta,Y\n";

const DEPARTMENTS_SOURCE: &str = r#"  - type: source
    name: dep
    config:
      name: dep
      type: csv
      path: dep.csv
      schema:
        - { name: department, type: string }
        - { name: label, type: string }
        - { name: extra, type: string }
"#;

/// A Combine `j` of inputs `a` and `b` on `department`.
fn combine_node(a: &str, b: &str) -> String {
    format!(
        r#"  - type: combine
    name: j
    input:
      a: {a}
      b: {b}
    config:
      where: "a.department == b.department"
      match: first
      on_miss: null_fields
      propagate_ck: driver
      cxl: |
        emit department = a.department
        emit v = a.v
        emit label = b.label
"#
    )
}

#[test]
fn a_body_combine_reads_each_input_by_the_column_it_names() {
    // Each port declares fewer columns than its caller has, in another
    // order. Before the body bound against the caller's rows, the Combine
    // read `label` and `department` by the wrong position and wrote
    // `O1,a,` and the like, exit 0.
    let composition = format!(
        r#"_compose:
  name: c
  inputs:
    pa:
      schema:
        - {{ name: department, type: string }}
        - {{ name: v, type: int }}
    pb:
      schema:
        - {{ name: department, type: string }}
        - {{ name: label, type: string }}
  outputs:
    out: j
  config_schema: {{}}

nodes:
{}"#,
        combine_node("pa", "pb")
    );
    let composed = pipeline(
        "comb",
        &format!(
            r#"{ORDERS_SOURCE}{DEPARTMENTS_SOURCE}  - type: composition
    name: call
    input: src
    use: ../compositions/c.comp.yaml
    inputs:
      pa: src
      pb: dep
{}"#,
            sink("call", &["v"])
        ),
    );
    let inline = pipeline(
        "comb_inline",
        &format!(
            "{ORDERS_SOURCE}{DEPARTMENTS_SOURCE}{}{}",
            combine_node("src", "dep"),
            sink("j", &["v"])
        ),
    );
    assert_inline_equivalent(
        &[
            (PIPELINE, &composed),
            ("compositions/c.comp.yaml", &composition),
            ("pipelines/in.csv", ORDERS),
            ("pipelines/dep.csv", DEPARTMENTS),
        ],
        &[
            (PIPELINE, &inline),
            ("pipelines/in.csv", ORDERS),
            ("pipelines/dep.csv", DEPARTMENTS),
        ],
        0,
        "department,v,label\na,5,alpha\nb,6,beta\na,7,alpha\n",
    );
}

#[test]
fn engine_columns_pass_through_a_body_as_they_pass_through_inline_nodes() {
    // The source's correlation key column and `$source.file` reach the body
    // undeclared and come out where the inline Transform puts them.
    let source = ORDERS_SOURCE.replace(
        "      path: in.csv\n",
        "      path: in.csv\n      correlation_key: [order_id]\n",
    );
    let body = r#"  - type: transform
    name: t1
    input: inp
    config:
      cxl: |
        emit v = v + 100
        emit r = v * 2
        emit f = $source.file
"#;
    let composition = comp("c", &[("v", "int"), ("department", "string")], "t1", body);
    let keyed_sink = |input: &str| {
        sink(input, &["v"]).replace(
            "      path: out.csv\n",
            "      path: out.csv\n      include_correlation_keys: true\n",
        )
    };
    let composed = pipeline(
        "keyed",
        &format!(
            "{source}{}{}",
            call("call", "c.comp.yaml", "src"),
            keyed_sink("call")
        ),
    );
    let inline = pipeline(
        "keyed_inline",
        &format!(
            "{source}{}{}",
            body.replace("input: inp", "input: src"),
            keyed_sink("t1")
        ),
    );
    let (inline_run, inline_out) =
        run_workspace(&[(PIPELINE, &inline), ("pipelines/in.csv", ORDERS)]);
    assert_eq!(
        inline_run.status.code(),
        Some(0),
        "{}",
        describe(&inline_run)
    );
    let header = inline_out.lines().next().unwrap_or_default();
    assert_eq!(
        header, "order_id,department,v,wt,$ck.order_id,r,f",
        "the inline reference writes the key column: {inline_out}"
    );
    assert_eq!(inline_out.lines().count(), 4, "{inline_out}");
    let (composed_run, composed_out) = run_workspace(&[
        (PIPELINE, &composed),
        ("compositions/c.comp.yaml", &composition),
        ("pipelines/in.csv", ORDERS),
    ]);
    assert_eq!(
        composed_run.status.code(),
        Some(0),
        "{}",
        describe(&composed_run)
    );
    assert_eq!(
        composed_out, inline_out,
        "the composition call writes what its body inline writes"
    );
}

/// Run `files` with `--lineage` under a local lineage policy; return each
/// output column's sources as `(input column, transformation subtypes)`.
fn column_lineage(files: &[(&str, &str)]) -> Vec<(String, Vec<(String, String)>)> {
    let dir = tempfile::tempdir().expect("tempdir");
    write_files(dir.path(), files);
    write_files(
        dir.path(),
        &[(
            "clinker.toml",
            "[observability.lineage]\nidentity_mode = \"local_diagnostic_paths\"\n",
        )],
    );
    let output = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(dir.path())
        .arg("run")
        .arg(PIPELINE)
        .arg("--base-dir")
        .arg(dir.path())
        .arg("--force")
        .arg("--lineage")
        .arg("lin.ndjson")
        .output()
        .expect("spawn clinker");
    assert_eq!(output.status.code(), Some(0), "{}", describe(&output));
    let events = std::fs::read_to_string(dir.path().join("lin.ndjson")).expect("read lineage");
    let complete: serde_json::Value = events
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| serde_json::from_str::<serde_json::Value>(line).expect("lineage line is JSON"))
        .find(|event| event["eventType"] == "COMPLETE")
        .expect("a COMPLETE event");
    let fields = complete["outputs"][0]["facets"]["columnLineage"]["fields"]
        .as_object()
        .unwrap_or_else(|| panic!("an output with column lineage: {complete}"));
    let mut lineage: Vec<(String, Vec<(String, String)>)> = fields
        .iter()
        .map(|(column, entry)| {
            let mut sources: Vec<(String, String)> = entry["inputFields"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|input| {
                    let kinds: Vec<String> = input["transformations"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .map(|t| t["subtype"].as_str().unwrap_or_default().to_string())
                        .collect();
                    (
                        input["field"].as_str().unwrap_or_default().to_string(),
                        kinds.join("+"),
                    )
                })
                .collect();
            sources.sort();
            (column.clone(), sources)
        })
        .collect();
    lineage.sort();
    lineage
}

#[test]
fn a_call_s_column_lineage_is_the_lineage_of_its_body_written_inline() {
    // A body Aggregate behind a reordered subset port: `s` comes from `v`.
    let composition = comp(
        "c",
        &[("v", "int"), ("department", "string")],
        "totals",
        COMP3_BODY,
    );
    let composed = column_lineage(&[
        (
            PIPELINE,
            &pipeline(
                "comp3",
                &format!(
                    "{ORDERS_SOURCE}{}{}",
                    call("call", "c.comp.yaml", "src"),
                    sink("call", &["department"])
                ),
            ),
        ),
        ("compositions/c.comp.yaml", &composition),
        ("pipelines/in.csv", ORDERS),
    ]);
    let inline = column_lineage(&[
        (
            PIPELINE,
            &pipeline(
                "comp3_inline",
                &format!(
                    "{ORDERS_SOURCE}{}{}",
                    comp3_inline("src"),
                    sink("totals", &["department"])
                ),
            ),
        ),
        ("pipelines/in.csv", ORDERS),
    ]);
    assert_eq!(composed, inline);
    assert!(
        composed.contains(&(
            "s".to_string(),
            vec![("v".to_string(), "AGGREGATION".to_string())]
        )),
        "`s` sums `v`: {composed:?}"
    );

    // A Transform body: the columns the port does not declare pass through
    // with identity lineage, as they do inline.
    let body = r#"  - type: transform
    name: t
    input: inp
    config:
      cxl: |
        emit r = v * 2
"#;
    let composition = comp("c", &[("v", "int"), ("department", "string")], "t", body);
    let composed = column_lineage(&[
        (
            PIPELINE,
            &pipeline(
                "pass",
                &format!(
                    "{ORDERS_SOURCE}{}{}",
                    call("call", "c.comp.yaml", "src"),
                    sink("call", &["v"])
                ),
            ),
        ),
        ("compositions/c.comp.yaml", &composition),
        ("pipelines/in.csv", ORDERS),
    ]);
    let inline = column_lineage(&[
        (
            PIPELINE,
            &pipeline(
                "pass_inline",
                &format!(
                    "{ORDERS_SOURCE}{}{}",
                    body.replace("input: inp", "input: src"),
                    sink("t", &["v"])
                ),
            ),
        ),
        ("pipelines/in.csv", ORDERS),
    ]);
    assert_eq!(composed, inline);
    for column in ["order_id", "wt"] {
        assert!(
            composed.iter().any(|(name, sources)| name == column
                && sources == &[(column.to_string(), "IDENTITY".to_string())]),
            "`{column}` passes through with identity lineage: {composed:?}"
        );
    }
}

/// `sink(input, sort)` writing only the columns its input emits.
fn emitted_only_sink(input: &str, sort: &[&str]) -> String {
    sink(input, sort).replace(
        "      path: out.csv\n",
        "      path: out.csv\n      include_unmapped: false\n",
    )
}

#[test]
fn a_sink_writing_only_emitted_columns_writes_what_it_writes_after_the_inline_nodes() {
    // After the inline Transform the Sink writes only `v` and `r`, the
    // columns the Transform emits. After the call it writes the same: the
    // call emits what the node behind its output port emits.
    let body = r#"  - type: transform
    name: t1
    input: inp
    config:
      cxl: |
        emit v = v + 100
        emit r = v * 2
"#;
    let composition = comp("c", &[("v", "int"), ("department", "string")], "t1", body);
    let composed = pipeline(
        "emitted",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            call("call", "c.comp.yaml", "src"),
            emitted_only_sink("call", &["v"])
        ),
    );
    let inline = pipeline(
        "emitted_inline",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            body.replace("input: inp", "input: src"),
            emitted_only_sink("t1", &["v"])
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
        "v,r\n105,10\n106,12\n107,14\n",
    );
}

#[test]
fn a_body_cycle_stops_the_run_instead_of_writing_nothing() {
    let body = r#"  - type: transform
    name: a
    input: b
    config:
      cxl: |
        emit p = v
  - type: transform
    name: b
    input: a
    config:
      cxl: |
        emit q = 1
"#;
    let composition = comp("c", &[("v", "int"), ("department", "string")], "a", body);
    let composed = pipeline(
        "cycle",
        &format!(
            "{ORDERS_SOURCE}{}{}",
            call("call", "c.comp.yaml", "src"),
            sink("call", &["v"])
        ),
    );
    let (run, _) = run_workspace(&[
        (PIPELINE, &composed),
        ("compositions/c.comp.yaml", &composition),
        ("pipelines/in.csv", ORDERS),
    ]);
    assert_eq!(run.status.code(), Some(1), "{}", describe(&run));
    assert!(
        String::from_utf8_lossy(&run.stderr).contains("E107"),
        "{}",
        describe(&run)
    );
}

#[test]
fn a_pipeline_listing_a_node_before_its_input_writes_what_the_ordered_pipeline_writes() {
    let transform = r#"  - type: transform
    name: t
    input: src
    config:
      cxl: |
        emit r = v * 2
"#;
    let ordered = pipeline(
        "ordered",
        &format!("{ORDERS_SOURCE}{transform}{}", sink("t", &["v"])),
    );
    let reversed = pipeline(
        "reversed",
        &format!("{}{transform}{ORDERS_SOURCE}", sink("t", &["v"])),
    );
    let (ordered_run, ordered_out) =
        run_workspace(&[(PIPELINE, &ordered), ("pipelines/in.csv", ORDERS)]);
    assert_eq!(
        ordered_run.status.code(),
        Some(0),
        "{}",
        describe(&ordered_run)
    );
    let (reversed_run, reversed_out) =
        run_workspace(&[(PIPELINE, &reversed), ("pipelines/in.csv", ORDERS)]);
    assert_eq!(
        reversed_run.status.code(),
        Some(0),
        "{}",
        describe(&reversed_run)
    );
    assert_eq!(reversed_out, ordered_out);
}
