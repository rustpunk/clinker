//! E378: a composition body may not declare a Sink while a Source declares
//! `dlq_granularity: document`.
//!
//! Document granularity runs every Sink after every other node so each
//! document's verdict is final before any Sink writes. A body Sink runs
//! inside its composition's dispatch, where that ordering cannot reach it,
//! so the combination is refused at compile time. The same pipeline under
//! `dlq_granularity: record` compiles.

use std::path::PathBuf;

use clinker_core_types::{Diagnostic, Span};

use crate::config::{CompileContext, parse_config};

/// Compile a pipeline whose Source `events` declares `dlq_granularity:
/// {granularity}` and whose composition call `enrich` binds a body holding a
/// Transform `shape` and a Sink `audit` reading it.
fn compile_with_body_sink(granularity: &str) -> Result<(), Vec<Diagnostic>> {
    let workspace = tempfile::tempdir().expect("tempdir");
    let comp_dir = workspace.path().join("compositions");
    std::fs::create_dir_all(&comp_dir).expect("mkdir compositions");
    std::fs::write(
        comp_dir.join("audited.comp.yaml"),
        r#"_compose:
  name: audited
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
  outputs:
    out: shape
  config_schema: {}

nodes:
  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit id = id
        emit value = value
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
"#,
    )
    .expect("write comp");
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");

    let yaml = format!(
        r#"
pipeline:
  name: document_dlq_body_sink
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      dlq_granularity: {granularity}
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
  - type: composition
    name: enrich
    input: events
    use: ../compositions/audited.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: out
    input: enrich
    config:
      name: out
      type: csv
      path: out.csv
"#
    );
    let config = parse_config(&yaml).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    config.compile(&ctx).map(|_| ())
}

#[test]
fn document_granularity_rejects_a_composition_body_sink() {
    let diags = compile_with_body_sink("document")
        .expect_err("a body Sink under document granularity must fail compilation");
    let e378: Vec<&Diagnostic> = diags.iter().filter(|d| d.code == "E378").collect();
    assert_eq!(
        e378.len(),
        1,
        "exactly one E378 diagnostic, got {:?}",
        diags
            .iter()
            .map(|d| format!("{}: {}", d.code, d.message))
            .collect::<Vec<_>>()
    );
    let diag = e378[0];

    for fragment in [
        "composition \"enrich\"",
        "Sink \"audit\"",
        "source \"events\"",
        "`dlq_granularity: document`",
        "pipeline level",
    ] {
        assert!(
            diag.message.contains(fragment),
            "the message names {fragment:?}: {}",
            diag.message
        );
    }
    let help = diag.help.as_deref().expect("E378 carries a help text");
    for fragment in ["input: enrich.audit", "audit: shape", "path: audit.csv"] {
        assert!(
            help.contains(fragment),
            "the help carries {fragment:?}: {help}"
        );
    }
    assert!(
        !help.contains('<'),
        "the help prints the Sink's configuration, not a placeholder: {help}"
    );
    assert!(
        !help.contains("dlq_granularity:"),
        "the help gives one fix, the move: {help}"
    );
    // The alternative and the issue that tracks body Sinks live on the
    // explain page, not in the help.
    let explain = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/explain/E378.md"),
    )
    .expect("read docs/explain/E378.md");
    for fragment in ["dlq_granularity: record", "#1242"] {
        assert!(
            explain.contains(fragment),
            "the explain page carries {fragment:?}"
        );
    }
    assert_ne!(
        diag.primary.span,
        Span::SYNTHETIC,
        "the diagnostic is located at the body Sink"
    );
    assert_eq!(
        diag.secondary.len(),
        1,
        "the composition call site is labelled"
    );
}

#[test]
fn record_granularity_keeps_composition_body_sinks() {
    if let Err(diags) = compile_with_body_sink("record") {
        panic!(
            "a body Sink under record granularity compiles, got {:?}",
            diags
                .iter()
                .map(|d| format!("{}: {}", d.code, d.message))
                .collect::<Vec<_>>()
        );
    }
}

/// Compile a pipeline against `compositions` (workspace-relative path,
/// contents) whose Source `events` declares `dlq_granularity: document` and
/// whose remaining nodes are `nodes`, and return its E378 diagnostics. Any
/// other diagnostic fails the test.
fn e378_for(compositions: &[(&str, &str)], nodes: &str) -> Vec<Diagnostic> {
    let workspace = tempfile::tempdir().expect("tempdir");
    for (relative, contents) in compositions {
        let path = workspace.path().join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(path, contents).expect("write comp");
    }
    std::fs::create_dir_all(workspace.path().join("pipelines")).expect("mkdir pipelines");
    let yaml = format!(
        r#"
pipeline:
  name: document_dlq_body_sinks
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      dlq_granularity: document
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
{nodes}"#
    );
    let config = parse_config(&yaml).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(workspace.path(), PathBuf::from("pipelines"));
    let diags = config
        .compile(&ctx)
        .expect_err("a body Sink under document granularity must fail compilation");
    assert!(
        diags.iter().all(|d| d.code == "E378"),
        "only E378 is expected, got {:?}",
        diags
            .iter()
            .map(|d| format!("{}: {}", d.code, d.message))
            .collect::<Vec<_>>()
    );
    diags
}

/// A composition file named `name` with the given `_compose.outputs:`
/// entries and body nodes after its Transform `shape`.
fn composition(name: &str, outputs: &str, body: &str) -> String {
    format!(
        r#"_compose:
  name: {name}
  inputs:
    inp:
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
  outputs:
{outputs}
  config_schema: {{}}

nodes:
  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit id = id
        emit value = value
{body}"#
    )
}

/// A body Sink `name` reading `shape` and writing `{name}.csv`.
fn body_sink(name: &str) -> String {
    format!(
        r#"  - type: sink
    name: {name}
    input: shape
    config:
      name: {name}
      type: csv
      path: {name}.csv
"#
    )
}

/// The call `enrich` of `compositions/audited.comp.yaml` and a pipeline
/// Sink reading it by its bare name.
const ENRICH_CALL: &str = r#"  - type: composition
    name: enrich
    input: events
    use: ../compositions/audited.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: main
    input: enrich
    config:
      name: main
      type: csv
      path: main.csv
"#;

/// The help of the diagnostic for the body Sink `sink`.
fn help_for<'a>(diags: &'a [Diagnostic], sink: &str) -> &'a str {
    let quoted = format!("Sink \"{sink}\"");
    diags
        .iter()
        .find(|d| d.message.contains(&quoted))
        .unwrap_or_else(|| panic!("an E378 for {quoted}"))
        .help
        .as_deref()
        .expect("E378 carries a help text")
}

#[test]
fn e378_states_the_port_count_after_every_body_sink_moves() {
    let comp = composition(
        "audited",
        "    out: shape",
        &(body_sink("audit") + &body_sink("mirror")),
    );
    let diags = e378_for(&[("compositions/audited.comp.yaml", &comp)], ENRICH_CALL);
    assert_eq!(diags.len(), 2, "one E378 per body Sink");
    for sink in ["audit", "mirror"] {
        let help = help_for(&diags, sink);
        for fragment in [
            format!("\n    {sink}: shape\n"),
            format!("input: enrich.{sink}\n"),
            "composition \"enrich\" then has 3 output ports".to_owned(),
            "read `enrich.out` wherever the pipeline reads `enrich` without a port".to_owned(),
        ] {
            assert!(
                help.contains(&fragment),
                "the help for {sink} carries {fragment:?}: {help}"
            );
        }
    }
}

#[test]
fn e378_proposes_a_port_no_output_already_uses() {
    // `out` and `out_2` are ports already; `out_3` is a body Sink whose own
    // name the first Sink's port takes, so it moves on to the next suffix.
    let comp = composition(
        "audited",
        "    out: shape\n    out_2: shape",
        &(body_sink("out") + &body_sink("audit") + &body_sink("out_3")),
    );
    let diags = e378_for(&[("compositions/audited.comp.yaml", &comp)], ENRICH_CALL);
    assert_eq!(diags.len(), 3, "one E378 per body Sink");
    for (sink, port) in [("out", "out_3"), ("audit", "audit"), ("out_3", "out_3_2")] {
        let help = help_for(&diags, sink);
        for fragment in [
            format!("\n    {port}: shape\n"),
            format!("\n    name: {sink}\n    input: enrich.{port}\n"),
        ] {
            assert!(
                help.contains(&fragment),
                "the help for {sink} carries {fragment:?}: {help}"
            );
        }
        assert!(
            !help.contains("without a port"),
            "the composition already had two ports: {help}"
        );
    }
}

/// The outer composition of the nested-call fixtures: it calls
/// `./inner.comp.yaml` as `inner` and reads it by its bare name.
const OUTER_COMP: &str = r#"_compose:
  name: outer
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
  outputs:
    out: stamp
  config_schema: {}

nodes:
  - type: composition
    name: inner
    input: inp
    use: ./inner.comp.yaml
    inputs:
      inp: inp
  - type: transform
    name: stamp
    input: inner
    config:
      cxl: |
        emit id = id
        emit value = value
"#;

/// A body Transform `doubled` reading `shape`.
const DOUBLED: &str = r#"  - type: transform
    name: doubled
    input: shape
    config:
      cxl: |
        emit id = id
        emit value = value
"#;

/// The help of a body Sink that cannot be moved today: one sentence naming
/// the explain page as the one next step, then the reason. No numbered
/// steps, no second option, no issue number.
fn assert_next_step(help: &str, sink: &str, reason: &str) {
    let expected = format!(
        "run `clinker explain --code E378` and follow its steps for declaring Sink \
         \"{sink}\" at pipeline level: moving it through a composition output port does \
         not work here, because {reason}"
    );
    assert_eq!(help, expected, "the help is the one next step");
}

#[test]
fn e378_gives_the_next_step_for_a_sink_in_a_nested_call() {
    let inner = composition("inner", "    out: shape", &body_sink("audit"));
    let diags = e378_for(
        &[
            ("compositions/inner.comp.yaml", &inner),
            ("compositions/outer.comp.yaml", OUTER_COMP),
        ],
        r#"  - type: composition
    name: wrap
    input: events
    use: ../compositions/outer.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: main
    input: wrap
    config:
      name: main
      type: csv
      path: main.csv
"#,
    );
    assert_eq!(diags.len(), 1, "one E378 for the one body Sink");
    assert_next_step(
        help_for(&diags, "audit"),
        "audit",
        "composition \"inner\" is called inside another composition",
    );
}

#[test]
fn e378_gives_the_next_step_when_the_sink_reads_another_port_node() {
    let comp = composition(
        "audited",
        "    out: doubled",
        &(DOUBLED.to_owned() + &body_sink("audit")),
    );
    let diags = e378_for(&[("compositions/audited.comp.yaml", &comp)], ENRICH_CALL);
    assert_eq!(diags.len(), 1, "one E378 for the one body Sink");
    assert_next_step(
        help_for(&diags, "audit"),
        "audit",
        "Sink \"audit\" reads \"shape\", and only the first output port of composition \
         \"enrich\", `out` from \"doubled\", carries rows to the pipeline",
    );
}

#[test]
fn e378_gives_the_next_step_when_the_port_node_has_another_body_reader() {
    let comp = composition(
        "audited",
        "    out: shape",
        &(DOUBLED.to_owned() + &body_sink("audit")),
    );
    let diags = e378_for(&[("compositions/audited.comp.yaml", &comp)], ENRICH_CALL);
    assert_eq!(diags.len(), 1, "one E378 for the one body Sink");
    assert_next_step(
        help_for(&diags, "audit"),
        "audit",
        "\"shape\" also feeds \"doubled\" inside composition \"enrich\", and a composition \
         output port cannot yet carry rows from a node that another node in the \
         composition reads",
    );
}

#[test]
fn e378_gives_the_next_step_when_two_calls_reach_the_same_sink() {
    let comp = composition("audited", "    out: shape", &body_sink("audit"));
    let diags = e378_for(
        &[("compositions/audited.comp.yaml", &comp)],
        &(ENRICH_CALL.to_owned()
            + r#"  - type: composition
    name: enrich_again
    input: events
    use: ../compositions/audited.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: again
    input: enrich_again
    config:
      name: again
      type: csv
      path: again.csv
"#),
    );
    assert_eq!(diags.len(), 2, "one E378 per call's body Sink");
    for diag in &diags {
        assert_next_step(
            diag.help.as_deref().expect("E378 carries a help text"),
            "audit",
            "`compositions/audited.comp.yaml` is used by 2 composition calls, so the moved \
             Sink would be declared once per call, each writing the same output",
        );
    }
}

#[test]
fn e378_names_the_moved_sink_so_no_pipeline_node_already_uses_it() {
    // A pipeline Sink is already called `audit`, and two compositions each
    // move a Sink called `audit`: the first takes `audit_2`, the second
    // `audit_3`, and each renders its `config.name` to match.
    let audited = composition("audited", "    out: shape", &body_sink("audit"));
    let checked = composition("checked", "    out: shape", &body_sink("audit"));
    let diags = e378_for(
        &[
            ("compositions/audited.comp.yaml", &audited),
            ("compositions/checked.comp.yaml", &checked),
        ],
        &(ENRICH_CALL.to_owned()
            + r#"  - type: composition
    name: check
    input: events
    use: ../compositions/checked.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: audit
    input: check
    config:
      name: audit
      type: csv
      path: pipeline_audit.csv
"#),
    );
    assert_eq!(diags.len(), 2, "one E378 per body Sink");
    for (call, name) in [("enrich", "audit_2"), ("check", "audit_3")] {
        let diag = diags
            .iter()
            .find(|d| d.message.contains(&format!("composition \"{call}\"")))
            .unwrap_or_else(|| panic!("an E378 for the Sink in {call}"));
        let help = diag.help.as_deref().expect("E378 carries a help text");
        let fragment = format!(
            "\n  - type: sink\n    name: {name}\n    input: {call}.audit\n    config:\n      \
             name: {name}\n      path: audit.csv\n"
        );
        assert!(
            help.contains(&fragment),
            "the help for {call} carries {fragment:?}: {help}"
        );
    }
}

#[test]
fn e378_gives_the_next_step_when_the_first_port_reads_a_route_branch() {
    // The port and the Sink read the same Route, so the Sink reads the node
    // behind the first port, but that node has ports of its own.
    let comp = composition(
        "audited",
        "    out: split.high",
        r#"  - type: route
    name: split
    input: shape
    config:
      conditions:
        high: value == "x"
      default: low
  - type: sink
    name: audit
    input: split.high
    config:
      name: audit
      type: csv
      path: audit.csv
"#,
    );
    let diags = e378_for(&[("compositions/audited.comp.yaml", &comp)], ENRICH_CALL);
    assert_eq!(diags.len(), 1, "one E378 for the one body Sink");
    assert_next_step(
        help_for(&diags, "audit"),
        "audit",
        "the first output port of composition \"enrich\", `out`, reads \"split\", which \
         sends rows to output ports of its own, and a composition output port cannot yet \
         carry rows from one of those",
    );
}

#[test]
fn e378_gives_the_next_step_when_the_first_port_reads_a_nested_call() {
    // The composition is called from the pipeline, and its Sink reads the
    // node behind its first port, but that node is a composition call.
    let inner = composition("inner", "    out: shape", "");
    let outer = r#"_compose:
  name: outer
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
  outputs:
    out: inner
  config_schema: {}

nodes:
  - type: composition
    name: inner
    input: inp
    use: ./inner.comp.yaml
    inputs:
      inp: inp
  - type: sink
    name: audit
    input: inner
    config:
      name: audit
      type: csv
      path: audit.csv
"#;
    let diags = e378_for(
        &[
            ("compositions/inner.comp.yaml", &inner),
            ("compositions/outer.comp.yaml", outer),
        ],
        r#"  - type: composition
    name: wrap
    input: events
    use: ../compositions/outer.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: main
    input: wrap
    config:
      name: main
      type: csv
      path: main.csv
"#,
    );
    assert_eq!(diags.len(), 1, "one E378 for the one body Sink");
    assert_next_step(
        help_for(&diags, "audit"),
        "audit",
        "the first output port of composition \"wrap\", `out`, reads \"inner\", which \
         sends rows to output ports of its own, and a composition output port cannot yet \
         carry rows from one of those",
    );
}

#[test]
fn e378_gives_the_next_step_when_the_sink_excludes_every_column_its_input_emits() {
    // `shape` emits only `value` and passes `id` through. The body Sink writes
    // only emitted columns and excludes `value`, so no `mapping:` can say
    // what it writes, and at pipeline level it would write `id`.
    let comp = r#"_compose:
  name: audited
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
  outputs:
    out: shape
  config_schema: {}

nodes:
  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit value = value
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
      include_unmapped: false
      exclude: [value]
"#;
    let diags = e378_for(&[("compositions/audited.comp.yaml", comp)], ENRICH_CALL);
    assert_eq!(diags.len(), 1, "one E378 for the one body Sink");
    assert_next_step(
        help_for(&diags, "audit"),
        "audit",
        "Sink \"audit\" sets `include_unmapped: false` and its `exclude:` removes every \
         column \"shape\" emits, so at pipeline level it would write the columns \
         composition \"enrich\" passes through instead",
    );
}

#[test]
fn e378_counts_every_declared_output_port() {
    // `audit: nosuch` names no body node, so the compile drops it, but it is
    // still a declared output: after the move the file declares `out`,
    // `audit` and the new `audit_2`.
    let comp = composition(
        "audited",
        "    out: shape\n    audit: nosuch",
        &body_sink("audit"),
    );
    let diags = e378_for(&[("compositions/audited.comp.yaml", &comp)], ENRICH_CALL);
    assert_eq!(diags.len(), 1, "one E378 for the one body Sink");
    let help = help_for(&diags, "audit");
    for fragment in [
        "\n    audit_2: shape\n",
        "composition \"enrich\" then has 3 output ports",
    ] {
        assert!(
            help.contains(fragment),
            "the help carries {fragment:?}: {help}"
        );
    }
}
