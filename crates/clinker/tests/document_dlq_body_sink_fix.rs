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
