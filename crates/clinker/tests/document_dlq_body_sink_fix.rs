//! E378's help is a fix an author can paste, and pasting it works.
//!
//! Each case writes a pipeline whose Source declares `dlq_granularity:
//! document` and whose composition body declares a Sink, takes every E378
//! help an in-process compile produces, and applies exactly what each help
//! says to the files: the line it adds under a composition's
//! `_compose.outputs:`, the body Sink it removes, the pipeline Sink block it
//! prints (with the body Sink's own configuration), and the port-qualified
//! reads it asks for. The edited pipeline must then compile with no
//! diagnostic and run through the real `clinker` binary.
//!
//! The reference a moved Sink is compared with is the same work written
//! without the composition: the Source on `dlq_granularity: record`, the body
//! Transforms at pipeline level, and each Sink reading the node it read in
//! the body, with its own configuration. A Sink inside a composition body
//! does not write in a run today (the explain page names the issue that
//! tracks it), so that pipeline, not the original one, is what the moved
//! Sinks have to reproduce byte for byte.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use clinker_core_types::Diagnostic;
use clinker_format::FormatReader;
use clinker_plan::config::{CompileContext, parse_config};

/// The pipeline file, relative to the workspace root.
const PIPELINE: &str = "pipelines/pipeline.yaml";

/// The single-port composition with one body Sink: `shape` feeds the `out`
/// port and the Sink `audit`. `shape` converts `value` to an integer, so a
/// non-numeric value fails the record in the body.
const AUDITED_COMP: &str = r#"_compose:
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
        emit value = value.to_int()
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

/// The Source every case reads: one document per file under `in/`.
fn source_node(granularity: &str) -> String {
    format!(
        r#"  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./in/*.csv
      dlq_granularity: {granularity}
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
"#
    )
}

/// The pipeline head shared by every case; `nodes:` is the last key, so a
/// pipeline Sink block appended at the end of the file joins `nodes:`.
fn pipeline_head(name: &str) -> String {
    format!(
        r#"pipeline:
  name: {name}
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
"#
    )
}

/// The one-Sink case as authored: the composition call `enrich` and the
/// pipeline Sink `out` reading it by its bare name.
fn one_sink_pipeline() -> String {
    format!(
        "{head}{source}{rest}",
        head = pipeline_head("document_dlq_body_sink"),
        source = source_node("document"),
        rest = r#"  - type: composition
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
"#,
    )
}

/// The one-Sink case without the composition: the body's Transform at
/// pipeline level and both Sinks reading it, on record granularity.
fn one_sink_reference() -> String {
    format!(
        "{head}{source}{rest}",
        head = pipeline_head("document_dlq_body_sink_reference"),
        source = source_node("record"),
        rest = r#"  - type: transform
    name: shape
    input: events
    config:
      cxl: |
        emit id = id
        emit value = value.to_int()
  - type: sink
    name: out
    input: shape
    config:
      name: out
      type: csv
      path: out.csv
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
"#,
    )
}

/// Write `files` (workspace-relative path, contents) under `root`.
fn write_files(root: &Path, files: &[(&str, &str)]) {
    for (relative, contents) in files {
        let path = root.join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
        std::fs::write(&path, contents).expect("write file");
    }
}

/// Compile the workspace's pipeline in-process, as the CLI anchors it: the
/// workspace is the composition root and the pipeline sits in `pipelines/`.
fn compile(root: &Path) -> Result<(), Vec<Diagnostic>> {
    let yaml = std::fs::read_to_string(root.join(PIPELINE)).expect("read pipeline");
    let config = parse_config(&yaml).expect("parse pipeline");
    let ctx = CompileContext::with_pipeline_dir(root, PathBuf::from("pipelines"));
    config.compile(&ctx).map(|_| ())
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

/// Every E378 help the compiler gives for the workspace, in emission order.
/// Any other diagnostic fails the test: the pipeline must be wrong for this
/// reason only.
fn e378_helps(root: &Path) -> Vec<String> {
    let diags = compile(root).expect_err("a body Sink under document granularity is refused");
    assert!(
        diags.iter().all(|d| d.code == "E378"),
        "only E378 is expected, got {:#?}",
        describe(&diags)
    );
    diags
        .iter()
        .map(|d| d.help.clone().expect("E378 carries a help text"))
        .collect()
}

/// The text between the first pair of backticks in `line`.
fn first_backticked(line: &str) -> &str {
    let start = line.find('`').expect("a backticked fragment") + 1;
    let len = line[start..].find('`').expect("a closing backtick");
    &line[start..start + len]
}

/// Every backticked fragment in `line`, in order.
fn backticked(line: &str) -> Vec<&str> {
    line.split('`').skip(1).step_by(2).collect()
}

/// The name inside the first double-quoted name in `line`.
fn first_quoted(line: &str) -> &str {
    let start = line.find('"').expect("a quoted name") + 1;
    let len = line[start..].find('"').expect("a closing quote");
    &line[start..start + len]
}

/// Insert `fragment` as the first entry under `_compose.outputs:`.
fn add_output_line(path: &Path, fragment: &str) {
    let text = std::fs::read_to_string(path).expect("read composition");
    let mut out = String::new();
    let mut inserted = false;
    for line in text.lines() {
        out.push_str(line);
        out.push('\n');
        if !inserted && line == "  outputs:" {
            out.push_str(fragment);
            out.push('\n');
            inserted = true;
        }
    }
    assert!(inserted, "{} has `_compose.outputs:`", path.display());
    std::fs::write(path, out).expect("write composition");
}

/// Remove the `nodes:` entry for the Sink `name`: from its `- type: sink`
/// line to the next entry or the end of the file.
fn remove_sink(path: &Path, name: &str) {
    let text = std::fs::read_to_string(path).expect("read composition");
    let lines: Vec<&str> = text.lines().collect();
    let start = lines
        .iter()
        .enumerate()
        .position(|(i, line)| {
            *line == "  - type: sink"
                && lines.get(i + 1) == Some(&format!("    name: {name}").as_str())
        })
        .unwrap_or_else(|| panic!("{} declares Sink {name}", path.display()));
    let end = lines[start + 1..]
        .iter()
        .position(|line| line.starts_with("  - "))
        .map_or(lines.len(), |offset| start + 1 + offset);
    let mut kept: Vec<&str> = lines[..start].to_vec();
    kept.extend_from_slice(&lines[end..]);
    std::fs::write(path, kept.join("\n") + "\n").expect("write composition");
}

/// Rewrite every read of `bare` that names no port to read `qualified`: an
/// `input:`, a call's `inputs:` entry or a `_compose.outputs:` entry whose
/// value is `bare`. A node's own `name:` is not a read.
fn qualify_reads(path: &Path, bare: &str, qualified: &str) {
    let text = std::fs::read_to_string(path).expect("read file");
    let suffix = format!(": {bare}");
    let rewritten: Vec<String> = text
        .lines()
        .map(|line| match line.strip_suffix(&suffix) {
            Some(head) if head.trim() != "name" => format!("{head}: {qualified}"),
            _ => line.to_owned(),
        })
        .collect();
    std::fs::write(path, rewritten.join("\n") + "\n").expect("write file");
}

/// Apply one E378 help to the workspace, step by step, exactly as written.
/// A step this function does not recognise fails the test, so a help that
/// grows a step nobody applies cannot pass.
fn apply_help(root: &Path, help: &str) {
    let mut lines = help.lines().peekable();
    let first = lines.next().expect("a first line");
    assert!(
        !first.starts_with(char::is_numeric),
        "the help opens with a plain sentence: {help}"
    );
    while let Some(step) = lines.next() {
        let (number, text) = step
            .split_once(". ")
            .unwrap_or_else(|| panic!("a numbered step, got {step:?} in:\n{help}"));
        assert!(number.parse::<u32>().is_ok(), "a numbered step: {step:?}");
        // The fragment lines a step carries: every following line indented
        // by at least two spaces.
        let mut fragment = Vec::new();
        while let Some(next) = lines.peek() {
            if next.starts_with("  ") {
                fragment.push(lines.next().expect("peeked"));
            } else {
                break;
            }
        }
        if text.contains("under `_compose.outputs:`, add:") {
            let file = first_backticked(text);
            assert_eq!(fragment.len(), 1, "one outputs line: {step:?}");
            add_output_line(&root.join(file), fragment[0]);
        } else if text.contains("remove Sink") {
            let file = first_backticked(text);
            assert!(fragment.is_empty(), "no fragment: {step:?}");
            remove_sink(&root.join(file), first_quoted(text));
        } else if text.starts_with("under the pipeline's `nodes:`, add:") {
            assert!(!fragment.is_empty(), "a Sink block: {step:?}");
            let path = root.join(PIPELINE);
            let mut pipeline = std::fs::read_to_string(&path).expect("read pipeline");
            for line in &fragment {
                pipeline.push_str(line);
                pipeline.push('\n');
            }
            std::fs::write(&path, pipeline).expect("write pipeline");
        } else if text.contains("without a port") {
            assert!(fragment.is_empty(), "no fragment: {step:?}");
            let fragments = backticked(text);
            let (qualified, file, bare) = match fragments.as_slice() {
                [qualified, bare] if text.contains("wherever the pipeline reads") => {
                    (*qualified, PIPELINE, *bare)
                }
                [qualified, file, bare] => (*qualified, *file, *bare),
                other => panic!("unexpected fragments {other:?} in {step:?}"),
            };
            qualify_reads(&root.join(file), bare, qualified);
        } else {
            panic!("a step the test does not know how to apply: {step:?}\n{help}");
        }
    }
}

/// Run `clinker run` on the workspace's pipeline.
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

fn stderr(output: &Output) -> String {
    format!(
        "exit {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Every `.csv` file a run wrote: relative Sink and dead-letter paths resolve
/// against the working directory, the workspace root here.
fn outputs(root: &Path) -> BTreeMap<String, Vec<u8>> {
    std::fs::read_dir(root)
        .expect("read the workspace root")
        .map(|entry| entry.expect("dir entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "csv"))
        .map(|path| {
            (
                path.file_name()
                    .expect("name")
                    .to_string_lossy()
                    .into_owned(),
                std::fs::read(&path).expect("read output"),
            )
        })
        .collect()
}

/// A fresh workspace holding `files` and the input documents.
fn workspace(files: &[(&str, &str)], inputs: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    write_files(dir.path(), files);
    for (name, contents) in inputs {
        write_files(dir.path(), &[(&format!("pipelines/in/{name}"), contents)]);
    }
    dir
}

/// Apply every E378 help to `root`, then require a clean compile and a run
/// that exits `expected_exit`.
fn apply_every_help_and_run(root: &Path, expected_helps: usize, expected_exit: i32) {
    let helps = e378_helps(root);
    assert_eq!(helps.len(), expected_helps, "E378 helps: {helps:#?}");
    for help in &helps {
        apply_help(root, help);
    }
    if let Err(diags) = compile(root) {
        panic!(
            "the pipeline with every E378 fix applied compiles cleanly, got {:#?}\nhelps: {helps:#?}",
            describe(&diags)
        );
    }
    let run = run(root);
    assert_eq!(run.status.code(), Some(expected_exit), "{}", stderr(&run));
}

const CLEAN_DOCUMENT: &str = "id,value\n1,10\n2,20\n";

#[test]
fn applying_the_e378_fix_compiles_and_writes_every_sink() {
    let pipeline = one_sink_pipeline();
    let files = [
        ("compositions/audited.comp.yaml", AUDITED_COMP),
        (PIPELINE, pipeline.as_str()),
    ];
    let inputs = [("a.csv", CLEAN_DOCUMENT)];

    // As authored, the CLI refuses the pipeline at compile time with E378.
    let original = workspace(&files, &inputs);
    let refused = run(original.path());
    assert_eq!(refused.status.code(), Some(1), "{}", stderr(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("E378"),
        "{}",
        stderr(&refused)
    );

    let fixed = workspace(&files, &inputs);
    apply_every_help_and_run(fixed.path(), 1, 0);

    let reference_pipeline = one_sink_reference();
    let reference = workspace(&[(PIPELINE, reference_pipeline.as_str())], &inputs);
    let reference_run = run(reference.path());
    assert_eq!(
        reference_run.status.code(),
        Some(0),
        "{}",
        stderr(&reference_run)
    );

    let written = outputs(fixed.path());
    let expected = outputs(reference.path());
    assert_eq!(
        written.keys().collect::<Vec<_>>(),
        ["audit.csv", "out.csv"],
        "both Sinks write, and nothing is dead-lettered"
    );
    assert_eq!(
        written, expected,
        "every Sink writes what the reference writes"
    );
}

#[test]
fn applying_the_e378_fix_keeps_a_rejected_document_out_of_the_moved_sink() {
    let pipeline = one_sink_pipeline();
    let files = [
        ("compositions/audited.comp.yaml", AUDITED_COMP),
        (PIPELINE, pipeline.as_str()),
    ];
    // `b.csv` is one document: `3` converts, `4` does not, so the document
    // is rejected whole.
    let inputs = [
        ("a.csv", CLEAN_DOCUMENT),
        ("b.csv", "id,value\n3,30\n4,x\n"),
    ];
    let fixed = workspace(&files, &inputs);
    apply_every_help_and_run(fixed.path(), 1, 2);

    let written = outputs(fixed.path());
    for sink in ["out.csv", "audit.csv"] {
        let text = String::from_utf8_lossy(&written[sink]).into_owned();
        assert_eq!(
            text.lines().collect::<Vec<_>>(),
            ["id,value", "1,10", "2,20"],
            "{sink} holds the clean document only"
        );
    }
    let mut ids = dead_letter_ids(&fixed.path().join("rejected.csv"));
    ids.sort_unstable();
    assert_eq!(
        ids,
        ["3", "4"],
        "the dead-letter file holds each row of the rejected document once:\n{}",
        String::from_utf8_lossy(&written["rejected.csv"])
    );
}

/// The `id` cell of every row of a dead-letter file, in file order.
fn dead_letter_ids(path: &Path) -> Vec<String> {
    let mut reader = clinker_format::csv::CsvReader::from_reader(
        std::fs::File::open(path).expect("open the dead-letter file"),
        Default::default(),
    );
    let mut ids = Vec::new();
    while let Some(record) = reader.next_record().expect("a dead-letter row parses") {
        match record.get("id") {
            Some(clinker_record::Value::String(id)) => ids.push(id.to_string()),
            other => panic!("a dead-letter row carries its `id`, got {other:?}"),
        }
    }
    ids
}

/// A single-port composition with two body Sinks: `out`, named like the
/// existing port, and `audit`.
const TWO_SINK_COMP: &str = r#"_compose:
  name: two_sinks
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
        emit value = value.to_int()
  - type: sink
    name: out
    input: shape
    config:
      name: out
      type: csv
      path: out_copy.csv
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

/// The composition called inside `OUTER_COMP`, with one body Sink.
const INNER_COMP: &str = r#"_compose:
  name: inner
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
        emit value = value.to_int()
  - type: sink
    name: inner_audit
    input: shape
    config:
      name: inner_audit
      type: csv
      path: inner_audit.csv
"#;

/// A single-port composition that calls `INNER_COMP` and reads it by its
/// bare name.
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
        emit value = value * 2
"#;

/// A composition whose Sink `audit` reads `shape` while its one port reads
/// `doubled`, which reads `shape` too. `port` picks the node behind `out`.
fn shared_node_comp(port: &str) -> String {
    format!(
        r#"_compose:
  name: shared
  inputs:
    inp:
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
  outputs:
    out: {port}
  config_schema: {{}}

nodes:
  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit id = id
        emit value = value.to_int()
  - type: transform
    name: doubled
    input: shape
    config:
      cxl: |
        emit id = id
        emit value = value * 2
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
"#
    )
}

/// One call per `(name, use)` pair, each read by a pipeline Sink of its own
/// by its bare name.
fn calls_pipeline(calls: &[(&str, &str)]) -> String {
    let mut nodes = String::new();
    for (name, file) in calls {
        nodes.push_str(&format!(
            r#"  - type: composition
    name: {name}
    input: events
    use: ../compositions/{file}
    inputs:
      inp: events
  - type: sink
    name: {name}_out
    input: {name}
    config:
      name: {name}_out
      type: csv
      path: {name}_out.csv
"#
        ));
    }
    format!(
        "{head}{source}{nodes}",
        head = pipeline_head("document_dlq_body_sink_calls"),
        source = source_node("document"),
    )
}

/// The two-Sink call, with a pipeline Sink already called `audit`.
fn two_sinks_pipeline() -> String {
    format!(
        "{head}{source}{rest}",
        head = pipeline_head("document_dlq_body_sinks"),
        source = source_node("document"),
        rest = r#"  - type: composition
    name: enrich
    input: events
    use: ../compositions/two_sinks.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: primary
    input: enrich
    config:
      name: primary
      type: csv
      path: primary.csv
  - type: sink
    name: audit
    input: enrich
    config:
      name: audit
      type: csv
      path: primary_audit.csv
"#,
    )
}

/// The same work without the composition, on record granularity: the body
/// Transform at pipeline level and every Sink reading it, the moved `audit`
/// under the name the help gives it.
fn two_sinks_reference() -> String {
    format!(
        "{head}{source}{rest}",
        head = pipeline_head("document_dlq_body_sinks_reference"),
        source = source_node("record"),
        rest = r#"  - type: transform
    name: shape
    input: events
    config:
      cxl: |
        emit id = id
        emit value = value.to_int()
  - type: sink
    name: primary
    input: shape
    config:
      name: primary
      type: csv
      path: primary.csv
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: primary_audit.csv
  - type: sink
    name: out
    input: shape
    config:
      name: out
      type: csv
      path: out_copy.csv
  - type: sink
    name: audit_2
    input: shape
    config:
      name: audit_2
      type: csv
      path: audit.csv
"#,
    )
}

#[test]
fn applying_every_e378_fix_for_two_body_sinks_and_a_taken_sink_name() {
    let pipeline = two_sinks_pipeline();
    let files = [
        ("compositions/two_sinks.comp.yaml", TWO_SINK_COMP),
        (PIPELINE, pipeline.as_str()),
    ];
    let inputs = [("a.csv", CLEAN_DOCUMENT)];
    let fixed = workspace(&files, &inputs);
    apply_every_help_and_run(fixed.path(), 2, 0);

    let reference_pipeline = two_sinks_reference();
    let reference = workspace(&[(PIPELINE, reference_pipeline.as_str())], &inputs);
    let reference_run = run(reference.path());
    assert_eq!(
        reference_run.status.code(),
        Some(0),
        "{}",
        stderr(&reference_run)
    );

    let written = outputs(fixed.path());
    assert_eq!(
        written.keys().collect::<Vec<_>>(),
        [
            "audit.csv",
            "out_copy.csv",
            "primary.csv",
            "primary_audit.csv"
        ],
        "every Sink writes, and nothing is dead-lettered"
    );
    assert_eq!(
        written,
        outputs(reference.path()),
        "every Sink writes what the reference writes"
    );
}

#[test]
fn e378_gives_one_next_step_where_the_move_would_not_run() {
    let shared_first = shared_node_comp("shape");
    let shared_other = shared_node_comp("doubled");
    let cases = [
        (
            "a Sink in a nested call",
            vec![
                ("compositions/inner.comp.yaml", INNER_COMP),
                ("compositions/outer.comp.yaml", OUTER_COMP),
            ],
            calls_pipeline(&[("wrap", "outer.comp.yaml")]),
        ),
        (
            "a Sink reading a node no first port reads",
            vec![("compositions/shared.comp.yaml", shared_other.as_str())],
            calls_pipeline(&[("enrich", "shared.comp.yaml")]),
        ),
        (
            "a port node another body node reads",
            vec![("compositions/shared.comp.yaml", shared_first.as_str())],
            calls_pipeline(&[("enrich", "shared.comp.yaml")]),
        ),
        (
            "one body Sink reached by two calls",
            vec![("compositions/audited.comp.yaml", AUDITED_COMP)],
            calls_pipeline(&[
                ("enrich", "audited.comp.yaml"),
                ("again", "audited.comp.yaml"),
            ]),
        ),
    ];
    for (case, compositions, pipeline) in cases {
        let mut files = compositions;
        files.push((PIPELINE, pipeline.as_str()));
        let dir = workspace(&files, &[("a.csv", CLEAN_DOCUMENT)]);
        for help in e378_helps(dir.path()) {
            assert!(
                help.starts_with("run `clinker explain --code E378` and follow its steps")
                    && !help.contains('\n'),
                "{case}: the help is the one next step, got:\n{help}"
            );
        }
        let refused = run(dir.path());
        assert_eq!(
            refused.status.code(),
            Some(1),
            "{case}: {}",
            stderr(&refused)
        );
        let refused_stderr = String::from_utf8_lossy(&refused.stderr);
        assert!(
            refused_stderr.contains("E378"),
            "{case}: {}",
            stderr(&refused)
        );
    }

    // The page the step names covers why these shapes cannot move today.
    let explain = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .args(["explain", "--code", "E378"])
        .output()
        .expect("spawn clinker");
    assert!(explain.status.success(), "{}", stderr(&explain));
    let page = String::from_utf8_lossy(&explain.stdout);
    assert!(
        page.contains("#1315"),
        "`clinker explain --code E378` names the issue that blocks the move:\n{page}"
    );
}

/// A composition that declares a second output, `audit`, naming a node the
/// body does not have. The compile ignores that output, but the name is
/// still a key under `_compose.outputs:`, so a port proposed under it would
/// be a duplicate key.
const DANGLING_OUTPUT_COMP: &str = r#"_compose:
  name: dangling
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
  outputs:
    out: shape
    audit: nosuch
  config_schema: {}

nodes:
  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit id = id
        emit value = value.to_int()
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

/// The work of `calls_pipeline(&[("enrich", ..)])` over a composition whose
/// body Sink `audit` reads `audit_from` and whose port feeds `enrich_out`
/// from `out_from`, written without the composition, on record granularity.
/// `body` is the body's non-Sink nodes, already reading `events`.
fn inline_reference(body: &str, audit_from: &str, out_from: &str) -> String {
    format!(
        r#"{head}{source}{body}  - type: sink
    name: enrich_out
    input: {out_from}
    config:
      name: enrich_out
      type: csv
      path: enrich_out.csv
  - type: sink
    name: audit
    input: {audit_from}
    config:
      name: audit
      type: csv
      path: audit.csv
"#,
        head = pipeline_head("document_dlq_body_sink_inline_reference"),
        source = source_node("record"),
    )
}

/// `shape` from the fixtures above, reading `events`.
const SHAPE_AT_PIPELINE: &str = r#"  - type: transform
    name: shape
    input: events
    config:
      cxl: |
        emit id = id
        emit value = value.to_int()
"#;

/// Applying the help, then compiling and running, must reproduce `reference`
/// byte for byte, and the moved Sink's port must not collide with any
/// declared output, resolved or not.
fn assert_applied_help_matches(files: &[(&str, &str)], reference: &str, written: &[&str]) {
    let inputs = [("a.csv", CLEAN_DOCUMENT)];
    let fixed = workspace(files, &inputs);
    apply_every_help_and_run(fixed.path(), 1, 0);
    let reference = workspace(&[(PIPELINE, reference)], &inputs);
    let reference_run = run(reference.path());
    assert_eq!(
        reference_run.status.code(),
        Some(0),
        "{}",
        stderr(&reference_run)
    );
    let outputs_fixed = outputs(fixed.path());
    assert_eq!(
        outputs_fixed.keys().map(String::as_str).collect::<Vec<_>>(),
        written,
        "every Sink writes, and nothing is dead-lettered"
    );
    assert_eq!(
        outputs_fixed,
        outputs(reference.path()),
        "every Sink writes what the reference writes"
    );
}

#[test]
fn e378_proposes_a_port_no_declared_output_uses() {
    let pipeline = calls_pipeline(&[("enrich", "dangling.comp.yaml")]);
    let files = [
        ("compositions/dangling.comp.yaml", DANGLING_OUTPUT_COMP),
        (PIPELINE, pipeline.as_str()),
    ];
    let probe = workspace(&files, &[("a.csv", CLEAN_DOCUMENT)]);
    let helps = e378_helps(probe.path());
    assert_eq!(helps.len(), 1, "E378 helps: {helps:#?}");
    assert!(
        helps[0].contains("\n    audit_2: shape\n"),
        "the new port skips the declared `audit` output:\n{}",
        helps[0]
    );
    assert_applied_help_matches(
        &files,
        &inline_reference(SHAPE_AT_PIPELINE, "shape", "shape"),
        &["audit.csv", "enrich_out.csv"],
    );
}

/// `shape` and `doubled` from `shared_node_comp`, reading `events`.
const SHARED_BODY_AT_PIPELINE: &str = r#"  - type: transform
    name: shape
    input: events
    config:
      cxl: |
        emit id = id
        emit value = value.to_int()
  - type: transform
    name: doubled
    input: shape
    config:
      cxl: |
        emit id = id
        emit value = value * 2
"#;

/// The explain page's rewrite, applied by hand to the call `enrich` of
/// `shared_node_comp("doubled")`, where the help gives the next step because
/// the body Sink reads `shape` and the first port reads `doubled`. The
/// composition's nodes replace the call:
///
/// 1. no pipeline node uses `shape`, `doubled` or `audit`, so nothing is
///    renamed;
/// 2. `shape` read the input port `inp`, which `events` fed, so it reads
///    `events`;
/// 3. the composition takes no `$config` value;
/// 4. the Sink `audit` is declared at pipeline level with its own
///    `config:`, reading `shape` as it did in the body;
/// 5. `enrich_out` read the call's only port, `out`, behind which is
///    `doubled`, so it reads `doubled`.
///
/// The Source keeps `dlq_granularity: document`.
fn shared_node_rewritten() -> String {
    let reference = inline_reference(SHARED_BODY_AT_PIPELINE, "shape", "doubled");
    reference
        .replace("dlq_granularity: record", "dlq_granularity: document")
        .replace(
            "document_dlq_body_sink_inline_reference",
            "document_dlq_body_sink_inline",
        )
}

#[test]
fn the_explain_pages_rewrite_runs_under_document_granularity() {
    // The shape gets the next step, not the move.
    let comp = shared_node_comp("doubled");
    let original = calls_pipeline(&[("enrich", "shared.comp.yaml")]);
    let authored = workspace(
        &[
            ("compositions/shared.comp.yaml", comp.as_str()),
            (PIPELINE, original.as_str()),
        ],
        &[("a.csv", CLEAN_DOCUMENT)],
    );
    for help in e378_helps(authored.path()) {
        assert!(
            help.starts_with("run `clinker explain --code E378` and follow its steps"),
            "the help is the next step: {help}"
        );
    }

    // The steps the rewrite follows are the page's.
    let explain = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .args(["explain", "--code", "E378"])
        .output()
        .expect("spawn clinker");
    assert!(explain.status.success(), "{}", stderr(&explain));
    let page = String::from_utf8_lossy(&explain.stdout);
    for step in [
        "1. Rename the nodes if a pipeline node already uses the name.",
        "2. Point the nodes that read an input port at the node that fed that port.",
        "3. Replace any `$config` value with the value the call passed.",
        "4. Declare the Sink at pipeline level with its own `config:`, reading the node",
        "5. Point the nodes that read the call at the node behind the port they read.",
    ] {
        assert!(
            page.contains(step),
            "the page carries step {step:?}:\n{page}"
        );
    }

    let rewritten = shared_node_rewritten();
    let reference_pipeline = inline_reference(SHARED_BODY_AT_PIPELINE, "shape", "doubled");

    // A clean document: the rewrite compiles with no diagnostic, runs, and
    // writes what the reference writes.
    let inputs = [("a.csv", CLEAN_DOCUMENT)];
    let fixed = workspace(&[(PIPELINE, rewritten.as_str())], &inputs);
    if let Err(diags) = compile(fixed.path()) {
        panic!("the rewrite compiles cleanly, got {:#?}", describe(&diags));
    }
    let fixed_run = run(fixed.path());
    assert_eq!(fixed_run.status.code(), Some(0), "{}", stderr(&fixed_run));
    let reference = workspace(&[(PIPELINE, reference_pipeline.as_str())], &inputs);
    let reference_run = run(reference.path());
    assert_eq!(
        reference_run.status.code(),
        Some(0),
        "{}",
        stderr(&reference_run)
    );
    let written = outputs(fixed.path());
    assert_eq!(
        written.keys().map(String::as_str).collect::<Vec<_>>(),
        ["audit.csv", "enrich_out.csv"],
        "both Sinks write, and nothing is dead-lettered"
    );
    assert_eq!(
        written,
        outputs(reference.path()),
        "every Sink writes what the reference writes"
    );

    // A rejected document reaches neither Sink, and each of its rows is
    // dead-lettered once.
    let inputs = [
        ("a.csv", CLEAN_DOCUMENT),
        ("b.csv", "id,value\n3,30\n4,x\n"),
    ];
    let rejecting = workspace(&[(PIPELINE, rewritten.as_str())], &inputs);
    let rejecting_run = run(rejecting.path());
    assert_eq!(
        rejecting_run.status.code(),
        Some(2),
        "{}",
        stderr(&rejecting_run)
    );
    let written = outputs(rejecting.path());
    for (sink, rows) in [
        ("audit.csv", ["id,value", "1,10", "2,20"]),
        ("enrich_out.csv", ["id,value", "1,20", "2,40"]),
    ] {
        let text = String::from_utf8_lossy(&written[sink]).into_owned();
        assert_eq!(
            text.lines().collect::<Vec<_>>(),
            rows,
            "{sink} holds the clean document only"
        );
    }
    let mut ids = dead_letter_ids(&rejecting.path().join("rejected.csv"));
    ids.sort_unstable();
    assert_eq!(ids, ["3", "4"], "each row of the rejected document once");
}

/// A composition whose first output port aliases its input port, and whose
/// only node is a Sink reading that input port. Moving the Sink would leave
/// the body with no node, which does not compile (E111), so the help must be
/// the one next step. A port that reads an input port is refused the move
/// whatever else the body holds; that the move runs there is not shown.
const INPUT_ALIAS_COMP: &str = r#"_compose:
  name: passthrough
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
  outputs:
    out: inp
  config_schema: {}

nodes:
  - type: sink
    name: audit
    input: inp
    config:
      name: audit
      type: csv
      path: audit.csv
"#;

#[test]
fn e378_gives_the_next_step_when_the_first_port_aliases_the_input_port() {
    let pipeline = calls_pipeline(&[("enrich", "passthrough.comp.yaml")]);
    let files = [
        ("compositions/passthrough.comp.yaml", INPUT_ALIAS_COMP),
        (PIPELINE, pipeline.as_str()),
    ];
    let dir = workspace(&files, &[("a.csv", CLEAN_DOCUMENT)]);
    let helps = e378_helps(dir.path());
    assert_eq!(
        helps,
        [
            "run `clinker explain --code E378` and follow its steps for declaring Sink \
             \"audit\" at pipeline level: moving it through a composition output port does \
             not work here, because the first output port of composition \"enrich\", `out`, \
             reads input port \"inp\" rather than a node of the composition"
        ],
        "the help is the one next step"
    );
    let refused = run(dir.path());
    assert_eq!(refused.status.code(), Some(1), "{}", stderr(&refused));
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("E378"),
        "{}",
        stderr(&refused)
    );
}

/// A composition whose `shape` emits three of its four input columns in
/// place, in an order other than the schema's, and passes `extra` through.
/// Its body Sink writes only emitted columns and excludes `flag`, so in the
/// body it writes `id,value`, in schema order.
const PROJECTED_COMP: &str = r#"_compose:
  name: projected
  inputs:
    inp:
      schema:
        - { name: id, type: string }
        - { name: value, type: string }
        - { name: extra, type: string }
        - { name: flag, type: string }
  outputs:
    out: shape
  config_schema: {}

nodes:
  - type: transform
    name: shape
    input: inp
    config:
      cxl: |
        emit flag = flag
        emit value = value
        emit id = id
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
      include_unmapped: false
      exclude: [flag]
"#;

/// The Source of the projected case: one document per file under `in/`,
/// with the four columns `PROJECTED_COMP` reads.
fn projected_source(granularity: &str) -> String {
    format!(
        r#"  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./in/*.csv
      dlq_granularity: {granularity}
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
        - {{ name: extra, type: string }}
        - {{ name: flag, type: string }}
"#
    )
}

#[test]
fn applying_the_e378_fix_writes_only_the_columns_the_body_sink_wrote() {
    let pipeline = format!(
        r#"{head}{source}  - type: composition
    name: enrich
    input: events
    use: ../compositions/projected.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: enrich_out
    input: enrich
    config:
      name: enrich_out
      type: csv
      path: enrich_out.csv
"#,
        head = pipeline_head("document_dlq_body_sink_projected"),
        source = projected_source("document"),
    );
    // The same work inline, on record granularity: `shape` at pipeline level,
    // and the Sink `audit` with the body Sink's own configuration.
    let reference = format!(
        r#"{head}{source}  - type: transform
    name: shape
    input: events
    config:
      cxl: |
        emit flag = flag
        emit value = value
        emit id = id
  - type: sink
    name: enrich_out
    input: shape
    config:
      name: enrich_out
      type: csv
      path: enrich_out.csv
  - type: sink
    name: audit
    input: shape
    config:
      name: audit
      type: csv
      path: audit.csv
      include_unmapped: false
      exclude: [flag]
"#,
        head = pipeline_head("document_dlq_body_sink_projected_reference"),
        source = projected_source("record"),
    );
    let files = [
        ("compositions/projected.comp.yaml", PROJECTED_COMP),
        (PIPELINE, pipeline.as_str()),
    ];
    let inputs = [("a.csv", "id,value,extra,flag\n1,10,x,p\n2,20,y,q\n")];
    let fixed = workspace(&files, &inputs);
    apply_every_help_and_run(fixed.path(), 1, 0);
    let reference = workspace(&[(PIPELINE, reference.as_str())], &inputs);
    let reference_run = run(reference.path());
    assert_eq!(
        reference_run.status.code(),
        Some(0),
        "{}",
        stderr(&reference_run)
    );

    let written = outputs(fixed.path());
    let expected = outputs(reference.path());
    assert_eq!(
        String::from_utf8_lossy(&expected["audit.csv"])
            .lines()
            .collect::<Vec<_>>(),
        ["id,value", "1,10", "2,20"],
        "the reference writes only the emitted columns the Sink keeps"
    );
    assert_eq!(
        written.keys().map(String::as_str).collect::<Vec<_>>(),
        ["audit.csv", "enrich_out.csv"],
        "both Sinks write, and nothing is dead-lettered"
    );
    assert_eq!(
        written, expected,
        "every Sink writes what the reference writes"
    );
}

/// A composition `projected` over `projected_source`'s four columns, whose
/// port `out` reads `port_node` from the body nodes `body` and whose body
/// Sink `audit` reads the same node, writing only the columns that node
/// emits, less `flag`.
fn projecting_comp(body: &str, port_node: &str) -> String {
    format!(
        r#"_compose:
  name: projected
  inputs:
    inp:
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
        - {{ name: extra, type: string }}
        - {{ name: flag, type: string }}
  outputs:
    out: {port_node}
  config_schema: {{}}

nodes:
{body}  - type: sink
    name: audit
    input: {port_node}
    config:
      name: audit
      type: csv
      path: audit.csv
      include_unmapped: false
      exclude: [flag]
"#
    )
}

/// Every E378 help the compiler gives for the workspace, as [`e378_helps`]
/// returns them, where the diagnostics may also hold the codes in
/// `tolerated` and nothing else.
fn e378_helps_beside(root: &Path, tolerated: &[&str]) -> Vec<String> {
    let diags = compile(root).expect_err("a body Sink under document granularity is refused");
    assert!(
        diags
            .iter()
            .all(|d| d.code == "E378" || tolerated.iter().any(|code| d.code == *code)),
        "only E378 and {tolerated:?} are expected, got {:#?}",
        describe(&diags)
    );
    diags
        .iter()
        .filter(|d| d.code == "E378")
        .map(|d| d.help.clone().expect("E378 carries a help text"))
        .collect()
}

/// Apply E378's help to a pipeline that calls `projecting_comp(body,
/// port_node)`, and require every Sink to write, byte for byte, what the
/// same work written inline writes: `inline_body` (the body's nodes reading
/// `events`) at pipeline level on record granularity, with `audit` keeping
/// the body Sink's configuration. The inline `audit.csv` must read
/// `audit_lines`, and the help must name no engine-stamped column. The
/// compile may also report the codes in `tolerated`, before and after the
/// help is applied.
fn assert_projecting_move_matches(
    body: &str,
    inline_body: &str,
    port_node: &str,
    audit_lines: &[&str],
    tolerated: &[&str],
) {
    let pipeline = format!(
        r#"{head}{source}  - type: composition
    name: enrich
    input: events
    use: ../compositions/projected.comp.yaml
    inputs:
      inp: events
  - type: sink
    name: enrich_out
    input: enrich
    config:
      name: enrich_out
      type: csv
      path: enrich_out.csv
"#,
        head = pipeline_head("document_dlq_body_sink_projected"),
        source = projected_source("document"),
    );
    let reference = format!(
        r#"{head}{source}{inline_body}  - type: sink
    name: enrich_out
    input: {port_node}
    config:
      name: enrich_out
      type: csv
      path: enrich_out.csv
  - type: sink
    name: audit
    input: {port_node}
    config:
      name: audit
      type: csv
      path: audit.csv
      include_unmapped: false
      exclude: [flag]
"#,
        head = pipeline_head("document_dlq_body_sink_projected_reference"),
        source = projected_source("record"),
    );
    let comp = projecting_comp(body, port_node);
    let files = [
        ("compositions/projected.comp.yaml", comp.as_str()),
        (PIPELINE, pipeline.as_str()),
    ];
    let inputs = [("a.csv", "id,value,extra,flag\n1,10,x,p\n2,20,y,q\n")];
    let fixed = workspace(&files, &inputs);
    let helps = e378_helps_beside(fixed.path(), tolerated);
    assert_eq!(helps.len(), 1, "E378 helps: {helps:#?}");
    assert!(
        helps.iter().all(|help| !help.contains('$')),
        "the help names no engine-stamped column: {helps:#?}"
    );
    apply_help(fixed.path(), &helps[0]);
    if let Err(diags) = compile(fixed.path()) {
        assert!(
            diags
                .iter()
                .all(|d| tolerated.iter().any(|code| d.code == *code)),
            "the pipeline with the E378 fix applied compiles, got {:#?}\nhelps: {helps:#?}",
            describe(&diags)
        );
    }
    let fixed_run = run(fixed.path());
    assert_eq!(fixed_run.status.code(), Some(0), "{}", stderr(&fixed_run));
    let reference = workspace(&[(PIPELINE, reference.as_str())], &inputs);
    let reference_run = run(reference.path());
    assert_eq!(
        reference_run.status.code(),
        Some(0),
        "{}",
        stderr(&reference_run)
    );

    let written = outputs(fixed.path());
    let expected = outputs(reference.path());
    assert_eq!(
        String::from_utf8_lossy(&expected["audit.csv"])
            .lines()
            .collect::<Vec<_>>(),
        audit_lines,
        "the reference writes the Sink's columns and no engine column"
    );
    assert_eq!(
        written.keys().map(String::as_str).collect::<Vec<_>>(),
        ["audit.csv", "enrich_out.csv"],
        "both Sinks write, and nothing is dead-lettered"
    );
    assert_eq!(
        written, expected,
        "every Sink writes what the reference writes\nhelps: {helps:#?}"
    );
}

#[test]
fn applying_the_e378_fix_writes_what_a_body_sink_after_a_merge_of_the_input_port_wrote() {
    let merge = |input: &str| {
        format!(
            r#"  - type: merge
    name: joined
    inputs:
      - {input}
    config: {{}}
"#
        )
    };
    assert_projecting_move_matches(
        &merge("inp"),
        &merge("events"),
        "joined",
        &["id,value,extra", "1,10,x", "2,20,y"],
        &[],
    );
}

#[test]
fn applying_the_e378_fix_writes_what_a_body_sink_after_a_reshape_wrote() {
    let reshape = |input: &str| {
        format!(
            r#"  - type: reshape
    name: classify
    input: {input}
    config:
      partition_by: [id]
      rules:
        - name: mark
          when: "flag == 'p'"
          mutate:
            set:
              extra: "'seen'"
"#
        )
    };
    assert_projecting_move_matches(
        &reshape("inp"),
        &reshape("events"),
        "classify",
        &["id,value,extra", "1,10,seen", "2,20,y"],
        // A Reshape in a composition body draws W101 for the `$meta.*`
        // columns it stamps, before and after the move. That warning is
        // its own defect; this case is about the columns the move writes.
        &["W101"],
    );
}
