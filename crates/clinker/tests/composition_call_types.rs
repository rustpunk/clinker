//! A composition call's output follows what its caller passed.
//!
//! A composition's input ports are its contract: the body is checked against
//! the declared types at every call. What a call carries out is the body run
//! on the caller's values, so the column types after a call are the types
//! the same nodes written inline give them. Each case runs one pipeline that
//! calls a composition and one with the body written inline, through the
//! real `clinker` binary, and compares the exit code, the bytes of
//! `out.csv`, and the column types the compiler gives the Sink (or, when the
//! pipeline does not compile, the diagnostic codes).

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use clinker_core_types::Diagnostic;
use clinker_plan::config::{CompileContext, parse_config};

/// The pipeline file, relative to the workspace root.
const PIPELINE: &str = "pipelines/pipeline.yaml";

const ORDERS: &str = "order_id,department,v,wt\nO1,a,5,0\nO1,b,6,1\nO2,a,7,1\nO3,c,9,2\n";

/// The same orders with one empty `v`.
const ORDERS_WITH_EMPTY: &str = "order_id,department,v,wt\nO1,a,5,0\nO1,b,,1\nO2,a,7,1\nO3,c,9,2\n";

const DEPARTMENTS: &str = "department,label,extra\na,alpha,X\nb,beta,Y\n";

/// The Source `src`: `order_id, department, v, wt`, with `v` typed `v_type`.
fn source(v_type: &str) -> String {
    format!(
        r#"  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - {{ name: order_id, type: string }}
        - {{ name: department, type: string }}
        - {{ name: v, type: {v_type} }}
        - {{ name: wt, type: int }}
"#
    )
}

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

fn pipeline(nodes: &str) -> String {
    format!(
        r#"pipeline:
  name: p
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
{nodes}  - type: sink
    name: out
    input: second
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// A call `name` of `compositions/{file}`, each `(port, upstream)` bound.
fn call(name: &str, file: &str, ports: &[(&str, &str)]) -> String {
    let inputs: String = ports
        .iter()
        .map(|(port, upstream)| format!("      {port}: {upstream}\n"))
        .collect();
    format!(
        r#"  - type: composition
    name: {name}
    input: {}
    use: ../compositions/{file}
    inputs:
{inputs}"#,
        ports[0].1
    )
}

/// A composition `name` with `ports` (port name and its schema lines) and
/// first output port `out`.
fn comp(name: &str, ports: &[(&str, &[&str])], out: &str, body: &str) -> String {
    let inputs: String = ports
        .iter()
        .map(|(port, columns)| {
            let schema: String = columns
                .iter()
                .map(|column| format!("        - {column}\n"))
                .collect();
            format!("    {port}:\n      schema:\n{schema}")
        })
        .collect();
    format!(
        "_compose:\n  name: {name}\n  inputs:\n{inputs}  outputs:\n    out: {out}\n  \
         config_schema: {{}}\n\nnodes:\n{body}"
    )
}

/// The call `second` that every case ends with: its port needs `v` as a
/// never-empty `int`.
fn second() -> String {
    comp(
        "second",
        &[("inp", &["{ name: v, type: int }"])],
        "t2",
        r#"  - type: transform
    name: t2
    input: inp
    config:
      cxl: |
        emit s = v * 2
"#,
    )
}

/// `body` with every `input: <port>` (and Combine/Merge input line) rewired
/// from the body's ports to the pipeline nodes that feed them.
fn inline_body(body: &str, ports: &[(&str, &str)]) -> String {
    body.lines()
        .map(|line| {
            let mut line = line.to_string();
            for (port, upstream) in ports {
                for prefix in ["input: ", "body: ", "a: ", "b: ", "- "] {
                    let from = format!("{prefix}{port}");
                    if line.trim_start() == from {
                        line = line.replace(&from, &format!("{prefix}{upstream}"));
                    }
                }
            }
            line + "\n"
        })
        .collect()
}

/// A shape: a composition `c` with `ports`, output node `out` and `body`,
/// called as `first` on `bindings`, then `second`; and the same nodes inline.
struct Shape<'a> {
    ports: &'a [(&'a str, &'a [&'a str])],
    bindings: &'a [(&'a str, &'a str)],
    out: &'a str,
    body: &'a str,
    /// Nodes before the call (sources, and any Transform feeding a port).
    upstream: String,
    input: &'a str,
}

impl Shape<'_> {
    fn call_files(&self) -> Vec<(String, String)> {
        let nodes = format!(
            "{}{}{}",
            self.upstream,
            call("first", "c.comp.yaml", self.bindings),
            call("second", "second.comp.yaml", &[("inp", "first")]),
        );
        vec![
            (PIPELINE.into(), pipeline(&nodes)),
            (
                "compositions/c.comp.yaml".into(),
                comp("c", self.ports, self.out, self.body),
            ),
            ("compositions/second.comp.yaml".into(), second()),
            ("pipelines/in.csv".into(), self.input.into()),
            ("pipelines/dep.csv".into(), DEPARTMENTS.into()),
        ]
    }

    fn inline_files(&self) -> Vec<(String, String)> {
        let nodes = format!(
            "{}{}{}",
            self.upstream,
            inline_body(self.body, self.bindings),
            call("second", "second.comp.yaml", &[("inp", self.out)]),
        );
        vec![
            (PIPELINE.into(), pipeline(&nodes)),
            ("compositions/second.comp.yaml".into(), second()),
            ("pipelines/in.csv".into(), self.input.into()),
            ("pipelines/dep.csv".into(), DEPARTMENTS.into()),
        ]
    }
}

fn write_files(root: &Path, files: &[(String, String)]) {
    for (relative, contents) in files {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&path, contents).expect("write file");
    }
}

fn describe(output: &Output) -> String {
    format!(
        "exit {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// The `(code, message)` pairs of a compile that failed, sorted.
type Failures = Vec<(String, String)>;

/// What one pipeline did: its exit code, `out.csv`, and either the Sink's
/// column types or the compile diagnostics.
struct Outcome {
    run: Output,
    out: Option<String>,
    compiled: Result<Vec<(String, String)>, Vec<Diagnostic>>,
}

impl Outcome {
    fn exit(&self) -> Option<i32> {
        self.run.status.code()
    }

    /// The Sink's column types, or the sorted diagnostics as `(code,
    /// message)` pairs with the node feeding `second` written `"<node>"`:
    /// the call `first` on one side, the body's output node `out` on the
    /// other, so the same failure compares equal on both.
    fn typing(&self, out: &str) -> Result<&[(String, String)], Failures> {
        match &self.compiled {
            Ok(types) => Ok(types),
            Err(diags) => {
                let mut failures: Failures = diags
                    .iter()
                    .map(|d| {
                        (
                            d.code.to_string(),
                            d.message
                                .replace("\"first\"", "\"<node>\"")
                                .replace(&format!("\"{out}\""), "\"<node>\""),
                        )
                    })
                    .collect();
                failures.sort();
                Err(failures)
            }
        }
    }

    /// The sorted, deduplicated codes of the diagnostics; empty when the
    /// pipeline compiled.
    fn codes(&self) -> Vec<&str> {
        let mut codes: Vec<&str> = self
            .compiled
            .as_ref()
            .err()
            .map(|diags| diags.iter().map(|d| d.code.as_ref()).collect())
            .unwrap_or_default();
        codes.sort_unstable();
        codes.dedup();
        codes
    }
}

fn outcome(files: &[(String, String)]) -> Outcome {
    let dir = tempfile::tempdir().expect("tempdir");
    write_files(dir.path(), files);
    let run = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(dir.path())
        .arg("run")
        .arg(PIPELINE)
        .arg("--base-dir")
        .arg(dir.path())
        .arg("--force")
        .output()
        .expect("spawn clinker");
    let out = std::fs::read(dir.path().join("out.csv"))
        .ok()
        .map(|bytes| String::from_utf8(bytes).expect("utf-8 output"));
    let yaml = std::fs::read_to_string(dir.path().join(PIPELINE)).expect("read pipeline");
    let config = parse_config(&yaml).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(dir.path(), PathBuf::from("pipelines"));
    let compiled = config.compile(&ctx).map(|plan| {
        plan.output_row("out")
            .expect("the Sink has a row")
            .fields()
            .map(|(field, ty)| (field.name.to_string(), format!("{ty:?}")))
            .collect()
    });
    Outcome { run, out, compiled }
}

/// The call and its inline nodes exit `exit`, write the same bytes, and give
/// the Sink the same column types (or fail with the same codes).
fn assert_inline_equivalent(shape: &Shape<'_>, exit: i32) -> (Outcome, Outcome) {
    let inline = outcome(&shape.inline_files());
    assert_eq!(
        inline.exit(),
        Some(exit),
        "the inline nodes: {}",
        describe(&inline.run)
    );
    let called = outcome(&shape.call_files());
    assert_eq!(
        called.exit(),
        Some(exit),
        "the call: {}",
        describe(&called.run)
    );
    assert_eq!(
        called.out, inline.out,
        "the call writes what the inline nodes write"
    );
    assert_eq!(
        called.typing(shape.out),
        inline.typing(shape.out),
        "the call types its output as the inline nodes do"
    );
    (called, inline)
}

/// A port `inp` declaring `v` as possibly empty, beside `order_id`.
const NULLABLE_V: &[(&str, &[&str])] = &[(
    "inp",
    &[
        "{ name: order_id, type: string }",
        "{ name: v, type: { nullable: int } }",
    ],
)];

const FED_BY_SRC: &[(&str, &str)] = &[("inp", "src")];

#[test]
fn a_column_a_call_does_not_write_keeps_the_type_it_arrived_with() {
    // The WR-13 chain: `first` reads `v` as possibly empty and only adds
    // `flag`; `v` stays the source's `int`, so `second` accepts it.
    assert_inline_equivalent(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out: "flag",
            body: r#"  - type: transform
    name: flag
    input: inp
    config:
      cxl: |
        emit flag = 1
"#,
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

#[test]
fn a_column_a_call_writes_from_the_caller_s_value_has_the_inline_type() {
    // `bwrite`: the body writes `v` from itself. The source's `v` is never
    // empty, so neither is the written one.
    assert_inline_equivalent(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out: "flag",
            body: r#"  - type: transform
    name: flag
    input: inp
    config:
      cxl: |
        emit v = v
        emit flag = 1
"#,
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

#[test]
fn a_possibly_empty_caller_column_stays_possibly_empty_through_a_write() {
    // `bwrite-null`: the source's `v` can be empty, so the written `v` can
    // be too, and `second` refuses it as it does inline.
    let (called, _) = assert_inline_equivalent(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out: "flag",
            body: r#"  - type: transform
    name: flag
    input: inp
    config:
      cxl: |
        emit v = v
        emit flag = 1
"#,
            upstream: source("{ nullable: int }"),
            input: ORDERS_WITH_EMPTY,
        },
        1,
    );
    assert_eq!(called.codes(), ["E102"]);
}

const RESHAPE_SET: &str = r#"  - type: reshape
    name: rsh
    input: inp
    config:
      partition_by: [order_id]
      rules:
        - name: r1
          when: "order_id == 'O1'"
          mutate:
            set:
              v: "v"
"#;

#[test]
fn a_reshape_set_in_a_call_types_its_target_as_inline() {
    assert_inline_equivalent(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out: "rsh",
            body: RESHAPE_SET,
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

#[test]
fn a_reshape_override_in_a_call_types_its_target_as_inline() {
    assert_inline_equivalent(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out: "rsh",
            body: r#"  - type: reshape
    name: rsh
    input: inp
    config:
      partition_by: [order_id]
      rules:
        - name: r1
          when: "order_id == 'O1'"
          synthesize:
            copy_from: trigger
            overrides:
              v: "v"
"#,
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

/// Two ports `pa` and `pb`, each declaring `v` as `v_type` beside
/// `department`.
fn two_ports(v_type: &str) -> Vec<String> {
    vec![
        format!("{{ name: v, type: {v_type} }}"),
        "{ name: department, type: string }".to_string(),
    ]
}

const BOTH_FED_BY_SRC: &[(&str, &str)] = &[("pa", "src"), ("pb", "src")];

/// A Merge `m` of Transforms `xa` (reading `pa`) and `xb` (reading `pb`).
fn merge_body(xa: &str, xb: &str) -> String {
    format!(
        r#"  - type: transform
    name: xa
    input: pa
    config:
      cxl: |
        {xa}
  - type: transform
    name: xb
    input: pb
    config:
      cxl: |
        {xb}
  - type: merge
    name: m
    inputs:
      - xa
      - xb
    config: {{}}
"#
    )
}

#[test]
fn a_merge_of_two_possibly_empty_ports_types_its_rows_as_inline() {
    // `mdecl`: both ports read `v` as possibly empty; neither writes it.
    let columns = two_ports("{ nullable: int }");
    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
    let body = merge_body("filter department == \"a\"", "filter department != \"a\"");
    assert_inline_equivalent(
        &Shape {
            ports: &[("pa", columns.as_slice()), ("pb", columns.as_slice())],
            bindings: BOTH_FED_BY_SRC,
            out: "m",
            body: &body,
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

/// The `second` of the Merge shapes below reads `wt` instead of `v`.
fn merge_wt_shape(body: &str, upstream: String, bindings: &[(&str, &str)]) -> (Outcome, Outcome) {
    let ports: &[(&str, &[&str])] = &[
        ("pa", &["{ name: v, type: int }"]),
        ("pb", &["{ name: v, type: int }"]),
    ];
    let second_wt = comp(
        "second",
        &[("inp", &["{ name: wt, type: int }"])],
        "t2",
        r#"  - type: transform
    name: t2
    input: inp
    config:
      cxl: |
        emit s = wt
"#,
    );
    let shape = Shape {
        ports,
        bindings,
        out: "m",
        body,
        upstream,
        input: ORDERS,
    };
    let with_second = |mut files: Vec<(String, String)>| {
        for (path, contents) in &mut files {
            if path == "compositions/second.comp.yaml" {
                *contents = second_wt.clone();
            }
        }
        files
    };
    let inline = outcome(&with_second(shape.inline_files()));
    let called = outcome(&with_second(shape.call_files()));
    assert_eq!(
        called.exit(),
        inline.exit(),
        "call: {}\ninline: {}",
        describe(&called.run),
        describe(&inline.run)
    );
    assert_eq!(called.out, inline.out);
    assert_eq!(called.typing("m"), inline.typing("m"));
    (called, inline)
}

#[test]
fn a_merge_whose_first_input_writes_a_column_has_the_written_type() {
    // `mtype2`: `xa` writes `wt` as a float; `xb` passes the caller's `int`
    // `wt` through. The Merge's rows have its first input's types, so `wt`
    // is a float and `second`, which needs an `int`, refuses it.
    let body = merge_body("emit wt = 1.5", "filter v > 6");
    let (called, _) = merge_wt_shape(&body, source("int"), BOTH_FED_BY_SRC);
    assert_eq!(called.exit(), Some(1), "{}", describe(&called.run));
    assert_eq!(called.codes(), ["E102"]);
}

#[test]
fn a_merge_of_two_differently_typed_callers_has_its_first_input_s_type() {
    // `mtype3`: `pa` is fed `wt` as an `int`, `pb` as a string. The Merge's
    // rows have the first input's types, so `wt` is an `int`.
    let body = merge_body("filter v > 6", "filter v <= 6");
    let upstream = format!(
        "{}{}",
        source("int"),
        r#"  - type: transform
    name: pre
    input: src
    config:
      cxl: |
        emit wt = "x"
"#
    );
    let (called, _) = merge_wt_shape(&body, upstream, &[("pa", "src"), ("pb", "pre")]);
    assert_eq!(called.exit(), Some(0), "{}", describe(&called.run));
}

#[test]
fn a_merge_of_a_written_and_a_passed_column_stays_nameable_as_inline() {
    // `mown`: `xa` writes `v`, `xb` passes the caller's `v`; a node after
    // the Merge names `v`.
    let columns = two_ports("int");
    let columns: Vec<&str> = columns.iter().map(String::as_str).collect();
    let body = format!(
        "{}{}",
        merge_body("emit v = v + 100", "filter v <= 6"),
        r#"  - type: transform
    name: t
    input: m
    config:
      cxl: |
        emit z = v
"#
    );
    assert_inline_equivalent(
        &Shape {
            ports: &[("pa", columns.as_slice()), ("pb", columns.as_slice())],
            bindings: BOTH_FED_BY_SRC,
            out: "t",
            body: &body,
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

#[test]
fn a_combine_in_a_call_types_what_it_emits_from_the_caller_s_values() {
    let body = r#"  - type: combine
    name: j
    input:
      a: pa
      b: pb
    config:
      where: "a.department == b.department"
      match: first
      on_miss: null_fields
      propagate_ck: driver
      cxl: |
        emit department = a.department
        emit v = a.v
        emit label = b.label
"#;
    assert_inline_equivalent(
        &Shape {
            ports: &[
                (
                    "pa",
                    &[
                        "{ name: department, type: string }",
                        "{ name: v, type: { nullable: int } }",
                    ],
                ),
                (
                    "pb",
                    &[
                        "{ name: department, type: string }",
                        "{ name: label, type: string }",
                    ],
                ),
            ],
            bindings: &[("pa", "src"), ("pb", "dep")],
            out: "j",
            body,
            upstream: format!("{}{DEPARTMENTS_SOURCE}", source("int")),
            input: ORDERS,
        },
        0,
    );
}

#[test]
fn an_envelope_in_a_call_passes_its_body_s_types_on_as_inline() {
    assert_inline_equivalent(
        &Shape {
            ports: &[("inp", &["{ name: v, type: { nullable: int } }"])],
            bindings: FED_BY_SRC,
            out: "framed",
            body: r#"  - type: envelope
    name: framed
    body: inp
    config:
      strategy: preserve
"#,
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

/// The nested shapes: `first`'s port declares `v` as `outer`, and its body
/// is one call `inner` of `inner.comp.yaml`, whose port declares `v` as
/// possibly empty and whose body only adds `flag`.
fn nested(outer: &str) -> (Outcome, Outcome) {
    let shape = Shape {
        ports: &[],
        bindings: FED_BY_SRC,
        out: "flag",
        body: r#"  - type: transform
    name: flag
    input: inp
    config:
      cxl: |
        emit flag = 1
"#,
        upstream: source("int"),
        input: ORDERS,
    };
    let inline = outcome(&shape.inline_files());
    let mut files = shape.call_files();
    let outer_comp = comp(
        "c",
        &[("inp", &[&format!("{{ name: v, type: {outer} }}")])],
        "inner",
        r#"  - type: composition
    name: inner
    input: inp
    use: ./inner.comp.yaml
    inputs:
      inp: inp
"#,
    );
    let inner_comp = comp(
        "inner",
        &[("inp", &["{ name: v, type: { nullable: int } }"])],
        "flag",
        shape.body,
    );
    for (path, contents) in &mut files {
        if path == "compositions/c.comp.yaml" {
            *contents = outer_comp.clone();
        }
    }
    files.push(("compositions/inner.comp.yaml".into(), inner_comp));
    let called = outcome(&files);
    assert_eq!(inline.exit(), Some(0), "{}", describe(&inline.run));
    assert_eq!(called.exit(), Some(0), "{}", describe(&called.run));
    assert_eq!(called.out, inline.out);
    assert_eq!(called.typing("flag"), inline.typing("flag"));
    (called, inline)
}

#[test]
fn a_nested_call_s_port_type_does_not_leak_through_an_outer_any_port() {
    nested("any");
}

#[test]
fn a_nested_call_behind_a_possibly_empty_port_keeps_the_caller_s_type() {
    nested("{ nullable: int }");
}

const FILTER_BODY: &str = r#"  - type: transform
    name: flag
    input: inp
    config:
      cxl: |
        filter v > 6
        emit flag = 1
"#;

/// `bfilter`: a body that does not handle the empty `v` its port admits
/// fails at every call, whatever the caller passes; written inline over a
/// never-empty `v` the same node runs. This is the one deliberate
/// difference from inline.
fn assert_filter_body_refused(v_type: &str, input: &str) -> Diagnostic {
    let shape = Shape {
        ports: NULLABLE_V,
        bindings: FED_BY_SRC,
        out: "flag",
        body: FILTER_BODY,
        upstream: source(v_type),
        input,
    };
    let called = outcome(&shape.call_files());
    assert_eq!(called.exit(), Some(1), "{}", describe(&called.run));
    let Err(diags) = &called.compiled else {
        panic!("the body does not handle an empty `v`");
    };
    assert_eq!(diags.len(), 1, "one error, no follower: {diags:#?}");
    let refused = &diags[0];
    assert_eq!(refused.code, "E200", "{refused:#?}");
    assert!(
        refused
            .message
            .contains("node \"flag\" in composition \"first\"")
            && refused.message.contains("input port `inp` declares `v`")
            && refused
                .message
                .contains("cannot use `v` while it may be empty"),
        "{}",
        refused.message
    );
    let help = refused.help.as_deref().unwrap_or_default();
    assert!(
        help.contains("`filter (v ?? 0) > 6`") && help.contains("- { name: v, type: int }"),
        "{help}"
    );
    refused.clone()
}

/// The help's paste-ready line: the text between the backticks that follow
/// the body file's name.
fn pasted_line(help: &str) -> &str {
    help.split("`: `")
        .nth(1)
        .and_then(|rest| rest.split("`, so the composition").next())
        .unwrap_or_else(|| panic!("the help prints a line to paste: {help}"))
}

/// The one E200 the call of a `c` with `body` behind a possibly empty `v`
/// draws, fed a never-empty `v`.
fn empty_value_refusal(body: &str, out: &str) -> Diagnostic {
    let called = outcome(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out,
            body,
            upstream: source("int"),
            input: ORDERS,
        }
        .call_files(),
    );
    assert_eq!(called.exit(), Some(1), "{}", describe(&called.run));
    let Err(diags) = &called.compiled else {
        panic!("the body does not handle an empty `v`");
    };
    assert_eq!(diags.len(), 1, "one error: {diags:#?}");
    assert_eq!(diags[0].code, "E200", "{diags:#?}");
    diags[0].clone()
}

#[test]
fn a_body_that_does_not_handle_an_empty_value_fails_at_a_never_empty_call() {
    assert_filter_body_refused("int", ORDERS);
    let inline = outcome(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out: "flag",
            body: FILTER_BODY,
            upstream: source("int"),
            input: ORDERS,
        }
        .inline_files(),
    );
    assert_eq!(inline.exit(), Some(0), "{}", describe(&inline.run));
}

#[test]
fn a_body_that_does_not_handle_an_empty_value_fails_at_a_possibly_empty_call() {
    assert_filter_body_refused("{ nullable: int }", ORDERS_WITH_EMPTY);
}

#[test]
fn the_pasted_fix_for_an_unhandled_empty_value_runs_as_inline() {
    // The line the help prints replaces the failing line, as the author
    // would paste it; the call then runs as the inline nodes do.
    let refused = assert_filter_body_refused("int", ORDERS);
    let line = pasted_line(refused.help.as_deref().unwrap_or_default()).to_string();
    assert_eq!(line, "filter (v ?? 0) > 6");
    assert_inline_equivalent(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out: "flag",
            body: &FILTER_BODY.replace("filter v > 6", &line),
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

const ROUTE_BODY: &str = r#"  - type: route
    name: r
    input: inp
    config:
      mode: exclusive
      conditions:
        high: 'v > 6'
      default: low
  - type: transform
    name: flag
    input: r.high
    config:
      cxl: |
        emit flag = 1
"#;

#[test]
fn a_route_condition_that_does_not_handle_an_empty_value_is_refused_and_its_pasted_entry_runs_as_inline()
 {
    let refused = empty_value_refusal(ROUTE_BODY, "flag");
    assert!(
        refused
            .message
            .contains("node \"r\" in composition \"first\""),
        "{}",
        refused.message
    );
    let entry = pasted_line(refused.help.as_deref().unwrap_or_default()).to_string();
    assert_eq!(entry, "high: '(v ?? 0) > 6'");
    assert_inline_equivalent(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out: "flag",
            body: &ROUTE_BODY.replace("high: 'v > 6'", &entry),
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

const RESHAPE_WHEN_BODY: &str = r#"  - type: reshape
    name: rsh
    input: inp
    config:
      partition_by: [order_id]
      rules:
        - name: r1
          when: 'v > 6'
          mutate:
            set:
              v: '0'
"#;

#[test]
fn a_reshape_condition_that_does_not_handle_an_empty_value_is_refused_and_its_pasted_entry_runs_as_inline()
 {
    let refused = empty_value_refusal(RESHAPE_WHEN_BODY, "rsh");
    assert!(
        refused
            .message
            .contains("node \"rsh\" in composition \"first\""),
        "{}",
        refused.message
    );
    let entry = pasted_line(refused.help.as_deref().unwrap_or_default()).to_string();
    assert_eq!(entry, "when: '(v ?? 0) > 6'");
    assert_inline_equivalent(
        &Shape {
            ports: NULLABLE_V,
            bindings: FED_BY_SRC,
            out: "rsh",
            body: &RESHAPE_WHEN_BODY.replace("when: 'v > 6'", &entry),
            upstream: source("int"),
            input: ORDERS,
        },
        0,
    );
}

#[test]
fn an_aggregate_over_a_possibly_empty_port_feeds_a_strict_call_as_inline() {
    // `aggint`: the body sums `v` per department; the sum of a never-empty
    // `v` is never empty, so `second` accepts it as it does inline. A hash
    // Aggregate writes its groups in no fixed order (#1010), so the rows
    // are compared sorted.
    let sorted = |out: &Option<String>| {
        let mut lines: Vec<&str> = out.as_deref().unwrap_or_default().lines().collect();
        lines.sort_unstable();
        lines.join("\n")
    };
    let shape = Shape {
        ports: &[(
            "inp",
            &[
                "{ name: department, type: string }",
                "{ name: v, type: { nullable: int } }",
            ],
        )],
        bindings: FED_BY_SRC,
        out: "totals",
        body: r#"  - type: aggregate
    name: totals
    input: inp
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit v = sum(v)
"#,
        upstream: source("int"),
        input: ORDERS,
    };
    let inline = outcome(&shape.inline_files());
    let called = outcome(&shape.call_files());
    assert_eq!(inline.exit(), Some(0), "{}", describe(&inline.run));
    assert_eq!(called.exit(), Some(0), "{}", describe(&called.run));
    assert_eq!(sorted(&called.out), sorted(&inline.out));
    assert_eq!(called.typing("totals"), inline.typing("totals"));
}

/// The three-level nest: `first`'s port declares `v` as possibly empty
/// and calls `mid`, whose port declares it `any` and calls `inner`, whose
/// port declares it possibly empty again and adds `flag`.
fn three_level_files(shape: &Shape<'_>) -> Vec<(String, String)> {
    let mut files = shape.call_files();
    let outer = comp(
        "c",
        &[("inp", &["{ name: v, type: { nullable: int } }"])],
        "mid",
        r#"  - type: composition
    name: mid
    input: inp
    use: ./mid.comp.yaml
    inputs:
      inp: inp
"#,
    );
    let mid = comp(
        "mid",
        &[("inp", &["{ name: v, type: any }"])],
        "inner",
        r#"  - type: composition
    name: inner
    input: inp
    use: ./inner.comp.yaml
    inputs:
      inp: inp
"#,
    );
    let inner = comp(
        "inner",
        &[("inp", &["{ name: v, type: { nullable: int } }"])],
        "flag",
        shape.body,
    );
    for (path, contents) in &mut files {
        if path == "compositions/c.comp.yaml" {
            *contents = outer.clone();
        }
    }
    files.push(("compositions/mid.comp.yaml".into(), mid));
    files.push(("compositions/inner.comp.yaml".into(), inner));
    files
}

#[test]
fn a_three_level_nest_keeps_the_caller_s_type_as_inline() {
    let shape = Shape {
        ports: &[],
        bindings: FED_BY_SRC,
        out: "flag",
        body: r#"  - type: transform
    name: flag
    input: inp
    config:
      cxl: |
        emit flag = 1
"#,
        upstream: source("int"),
        input: ORDERS,
    };
    let inline = outcome(&shape.inline_files());
    let called = outcome(&three_level_files(&shape));
    assert_eq!(inline.exit(), Some(0), "{}", describe(&inline.run));
    assert_eq!(called.exit(), Some(0), "{}", describe(&called.run));
    assert_eq!(called.out, inline.out);
    assert_eq!(called.typing("flag"), inline.typing("flag"));
}

#[test]
fn a_nested_call_with_no_input_ports_runs_as_its_reader_written_inline() {
    // The outer body calls a composition that binds no input port and only
    // reads its own Source, then emits `v` from what it read.
    let reader = r#"_compose:
  name: reader
  inputs: {}
  outputs: { out: read }
  config_schema: {}
  resources_schema:
    orders:
      kind: file
      required: true

nodes:
  - type: source
    name: read
    config:
      name: read
      type: csv
      resource: orders
      schema:
        - { name: k, type: int }
"#;
    let shape = Shape {
        ports: &[("inp", &["{ name: v, type: int }"])],
        bindings: FED_BY_SRC,
        out: "flag",
        body: r#"  - type: composition
    name: cr
    input: inp
    use: ./reader.comp.yaml
    inputs: {}
    resources: { orders: shared_orders }
  - type: transform
    name: flag
    input: cr
    config:
      cxl: |
        emit v = k
"#,
        upstream: source("int"),
        input: ORDERS,
    };
    let catalog = (
        "clinker.toml".to_string(),
        "[catalog.resources.shared_orders]\nkind = \"file\"\npath = \"pipelines/k.csv\"\n\
         access = \"read\"\n"
            .to_string(),
    );
    let data = ("pipelines/k.csv".to_string(), "k\n1\n2\n".to_string());
    let mut called_files = shape.call_files();
    called_files.push(("compositions/reader.comp.yaml".into(), reader.into()));
    called_files.extend([catalog.clone(), data.clone()]);
    // Inline, the reader's Source stands where the call stood, reading the
    // same file by path.
    let inline_body = r#"  - type: source
    name: cr
    config:
      name: cr
      type: csv
      path: k.csv
      schema:
        - { name: k, type: int }
  - type: transform
    name: flag
    input: cr
    config:
      cxl: |
        emit v = k
"#;
    let mut inline_files = Shape {
        body: inline_body,
        ..shape
    }
    .inline_files();
    inline_files.extend([catalog, data]);
    let called = outcome(&called_files);
    let inline = outcome(&inline_files);
    assert_eq!(inline.exit(), Some(0), "{}", describe(&inline.run));
    assert_eq!(called.exit(), Some(0), "{}", describe(&called.run));
    assert_eq!(called.out, inline.out);
    assert_eq!(called.typing("flag"), inline.typing("flag"));
}

#[test]
fn a_call_leaving_unbound_a_port_a_node_reads_is_refused_and_binding_it_runs_as_inline() {
    let body = r#"  - type: transform
    name: flag
    input: inp
    config:
      cxl: |
        emit flag = 1
  - type: transform
    name: u
    input: opt
    config:
      cxl: |
        emit b = 2
"#;
    let ports: &[(&str, &[&str])] = &[
        ("inp", &["{ name: v, type: int }"]),
        ("opt", &["{ name: v, type: int }"]),
    ];
    let optional = |composition: String| {
        composition.replace("    opt:\n", "    opt:\n      required: false\n")
    };
    let shape = Shape {
        ports,
        bindings: FED_BY_SRC,
        out: "flag",
        body,
        upstream: source("int"),
        input: ORDERS,
    };
    let mut files = shape.call_files();
    for (path, contents) in &mut files {
        if path == "compositions/c.comp.yaml" {
            *contents = optional(contents.clone());
        }
    }
    let called = outcome(&files);
    assert_eq!(called.exit(), Some(1), "{}", describe(&called.run));
    assert_eq!(called.codes(), ["E104"]);
    let Err(diags) = &called.compiled else {
        panic!("the unbound port is refused");
    };
    assert!(
        diags[0].message.contains("node \"u\"")
            && diags[0].message.contains("reads input port `opt`"),
        "{}",
        diags[0].message
    );
    assert!(
        diags[0]
            .help
            .as_deref()
            .unwrap_or_default()
            .contains("add `opt: <node>` under `inputs:`"),
        "{:?}",
        diags[0].help
    );
    // Bound as the help says, the call runs as the inline nodes do, with
    // `u` reading the same rows.
    let bound = Shape {
        bindings: &[("inp", "src"), ("opt", "src")],
        ..shape
    };
    let mut bound_files = bound.call_files();
    for (path, contents) in &mut bound_files {
        if path == "compositions/c.comp.yaml" {
            *contents = optional(contents.clone());
        }
    }
    let called = outcome(&bound_files);
    let inline = outcome(&bound.inline_files());
    assert_eq!(inline.exit(), Some(0), "{}", describe(&inline.run));
    assert_eq!(called.exit(), Some(0), "{}", describe(&called.run));
    assert_eq!(called.out, inline.out);
    assert_eq!(called.typing("flag"), inline.typing("flag"));
}

#[test]
fn one_composition_called_with_two_provided_types_runs_each_call_as_inline() {
    // `first` is fed the source's never-empty `v`; `again` is fed a `v`
    // that can be empty. Each call carries out what the body computes from
    // what it was fed, as two copies of the nodes inline would.
    let body = r#"  - type: transform
    name: flag
    input: inp
    config:
      cxl: |
        emit flag = v ?? -1
"#;
    let nullify = r#"  - type: transform
    name: holes
    input: src
    config:
      cxl: |
        emit v = if wt > 0 then v else null
"#;
    let second_sink = r#"  - type: sink
    name: out2
    input: again
    config:
      name: out2
      type: csv
      path: out2.csv
"#;
    let shape = Shape {
        ports: NULLABLE_V,
        bindings: FED_BY_SRC,
        out: "flag",
        body,
        upstream: format!("{}{nullify}", source("int")),
        input: ORDERS,
    };
    let add_again = |files: Vec<(String, String)>, again: &str| -> Vec<(String, String)> {
        files
            .into_iter()
            .map(|(path, contents)| {
                if path == PIPELINE {
                    let extra = format!("{again}{second_sink}");
                    (
                        path,
                        contents.replacen(
                            "  - type: sink\n",
                            &format!("{extra}  - type: sink\n"),
                            1,
                        ),
                    )
                } else {
                    (path, contents)
                }
            })
            .collect()
    };
    let called_files = add_again(
        shape.call_files(),
        &call("again", "c.comp.yaml", &[("inp", "holes")]),
    );
    let inline_again = inline_body(body, &[("inp", "holes")]).replace("name: flag", "name: again");
    let inline_files = add_again(shape.inline_files(), &inline_again);
    let read_out2 = |files: &[(String, String)]| {
        let dir = tempfile::tempdir().expect("tempdir");
        write_files(dir.path(), files);
        let run = Command::new(env!("CARGO_BIN_EXE_clinker"))
            .current_dir(dir.path())
            .args(["run", PIPELINE, "--base-dir"])
            .arg(dir.path())
            .arg("--force")
            .output()
            .expect("spawn clinker");
        let out = std::fs::read_to_string(dir.path().join("out.csv")).ok();
        let out2 = std::fs::read_to_string(dir.path().join("out2.csv")).ok();
        (run, out, out2)
    };
    let (called_run, called_out, called_out2) = read_out2(&called_files);
    let (inline_run, inline_out, inline_out2) = read_out2(&inline_files);
    assert_eq!(
        inline_run.status.code(),
        Some(0),
        "{}",
        describe(&inline_run)
    );
    assert_eq!(
        called_run.status.code(),
        Some(0),
        "{}",
        describe(&called_run)
    );
    assert_eq!(called_out, inline_out, "the never-empty call");
    assert_eq!(called_out2, inline_out2, "the possibly empty call");
    assert!(
        called_out2.as_deref().unwrap_or_default().contains("-1"),
        "the possibly empty call fills its holes: {called_out2:?}"
    );
}
