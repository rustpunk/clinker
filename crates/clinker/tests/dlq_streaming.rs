//! `clinker run` streams dead-letter rows into staged bucket files while the
//! run executes and publishes them with the attempt. The exit code and the
//! `write_meta` sidecar counts come from the run's counters, not from rows
//! held for the whole run.

use std::path::Path;
use std::process::{Command, Output};

use clinker_format::FormatReader;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_record::Value;

/// Write `files` into `dir`, then run `clinker run pipeline.yaml` there with
/// `extra` arguments.
fn run_clinker(dir: &Path, pipeline: &str, files: &[(&str, &str)], extra: &[&str]) -> Output {
    for (name, contents) in files {
        std::fs::write(dir.join(name), contents).expect("write input");
    }
    let pipeline_path = dir.join("pipeline.yaml");
    std::fs::write(&pipeline_path, pipeline).expect("write pipeline");
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(dir)
        .arg("run")
        .arg(&pipeline_path)
        .args(extra)
        .output()
        .expect("spawn clinker")
}

fn describe(output: &Output) -> String {
    format!(
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

/// The names of the regular files in `dir` ending in `.csv`, sorted.
fn csv_files(dir: &Path) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(dir)
        .expect("read dir")
        .map(|entry| entry.expect("dir entry"))
        .filter(|entry| entry.file_type().expect("file type").is_file())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".csv"))
        .collect();
    names.sort();
    names
}

/// The value of `column` in every data row of the CSV file at `path`, read
/// under the file's own header.
fn column_values(path: &Path, column: &str) -> Vec<String> {
    let mut reader = clinker_format::csv::CsvReader::from_reader(
        std::fs::File::open(path).expect("open DLQ"),
        Default::default(),
    );
    let mut values = Vec::new();
    while let Some(row) = reader.next_record().expect("DLQ row parses") {
        values.push(match row.get(column) {
            Some(Value::String(s)) => s.to_string(),
            Some(Value::Null) | None => String::new(),
            Some(other) => panic!("{column}: unexpected cell {other:?}"),
        });
    }
    values
}

/// `[src_a, src_b] → merge → ratio → out`; `ratio` fails on every row whose
/// `amount` is zero. `dlq` is the body of the dead-letter block.
fn two_source_pipeline(dlq: &str) -> String {
    format!(
        r#"pipeline:
  name: dlq_streaming
error_handling:
  strategy: continue
  dlq:
{dlq}
nodes:
- type: source
  name: src_a
  config:
    name: src_a
    path: a.csv
    type: csv
    schema:
      - {{ name: id, type: int }}
      - {{ name: amount, type: int }}
- type: source
  name: src_b
  config:
    name: src_b
    path: b.csv
    type: csv
    schema:
      - {{ name: id, type: int }}
      - {{ name: amount, type: int }}
- type: merge
  name: m
  inputs: [src_a, src_b]
- type: transform
  name: ratio
  input: m
  config:
    cxl: |
      emit id = id
      emit ratio = id / amount
- type: sink
  name: out
  input: ratio
  config:
    name: out
    path: out.csv
    type: csv
"#
    )
}

const SINGLE_SOURCE: &str = r#"pipeline:
  name: dlq_streaming_single
error_handling:
  strategy: continue
  dlq:
    path: rejects.csv
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: int }
      - { name: amount, type: int }
- type: transform
  name: ratio
  input: src
  config:
    cxl: |
      emit id = id
      emit ratio = id / amount
- type: sink
  name: out
  input: ratio
  config:
    name: out
    path: out.csv
    type: csv
"#;

#[test]
fn streamed_rows_publish_under_plan_header() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = run_clinker(
        dir.path(),
        SINGLE_SOURCE,
        &[("input.csv", "id,amount\n1,1\n2,0\n3,0\n4,4\n5,0\n6,6\n")],
        &[],
    );
    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));

    let plan = parse_config(SINGLE_SOURCE)
        .expect("pipeline parses")
        .compile(&CompileContext::new(dir.path().to_path_buf()))
        .expect("pipeline compiles");
    let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
    let bucket = layout.bucket(layout.bucket_for_source("src").expect("src has a bucket"));

    let path = dir.path().join("rejects.csv");
    let written = std::fs::read_to_string(&path).expect("DLQ file published");
    assert_eq!(
        written.lines().next(),
        Some(bucket.header().join(",").as_str()),
        "the published header is the compiled bucket header"
    );
    assert_eq!(
        column_values(&path, "id"),
        ["2", "3", "5"],
        "one row per failing record, in dispatch order"
    );
}

#[test]
fn empty_bucket_publishes_no_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = two_source_pipeline(
        "    per_source:\n      src_a:\n        path: a_rejects.csv\n      src_b:\n        path: b_rejects.csv",
    );
    let output = run_clinker(
        dir.path(),
        &pipeline,
        &[
            ("a.csv", "id,amount\n1,0\n2,2\n"),
            ("b.csv", "id,amount\n10,1\n11,2\n"),
        ],
        &[],
    );
    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));

    assert_eq!(
        csv_files(dir.path()),
        ["a.csv", "a_rejects.csv", "b.csv", "out.csv"],
        "src_b failed nothing, so its bucket has no file"
    );
    assert_eq!(
        column_values(&dir.path().join("a_rejects.csv"), "id"),
        ["1"]
    );
}

#[test]
fn no_destination_rows_count_and_write_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Only src_a has a destination; src_b's dead letters have none.
    let pipeline =
        two_source_pipeline("    per_source:\n      src_a:\n        path: a_rejects.csv");
    let output = run_clinker(
        dir.path(),
        &pipeline,
        &[
            ("a.csv", "id,amount\n1,1\n2,2\n"),
            ("b.csv", "id,amount\n10,0\n11,0\n"),
        ],
        &[],
    );
    assert_eq!(
        output.status.code(),
        Some(2),
        "a dead letter with no destination still counts: {}",
        describe(&output)
    );
    assert_eq!(
        csv_files(dir.path()),
        ["a.csv", "b.csv", "out.csv"],
        "src_b's rows are written nowhere, and src_a failed nothing"
    );
}

#[test]
fn write_meta_counts_output_stage_dead_letters() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Rows 1 and 2 hold a `tags` value containing the join delimiter, so the
    // CSV Sink's `join_values` refuses them at the output stage.
    let pipeline = r#"pipeline:
  name: dlq_streaming_write_meta
error_handling:
  strategy: continue
  dlq:
    path: rejects.csv
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: json
    path: in.json
    schema:
      - { name: order_id, type: string }
      - { name: tags, type: string, multiple: true }
- type: sink
  name: out
  input: orders
  config:
    name: out
    type: csv
    path: out.csv
    write_meta: true
    mapping:
      - order_id
      - tags
"#;
    let output = run_clinker(
        dir.path(),
        pipeline,
        &[(
            "in.json",
            r#"[
  {"order_id":"1","tags":["a;b","c"]},
  {"order_id":"2","tags":["d","e;f"]},
  {"order_id":"3","tags":["x","y"]}
]"#,
        )],
        &[],
    );
    assert_eq!(output.status.code(), Some(2), "{}", describe(&output));

    let sidecar: serde_json::Value = serde_json::from_slice(
        &std::fs::read(dir.path().join("out.csv.meta.json")).expect("read sidecar"),
    )
    .expect("parse sidecar");
    assert_eq!(
        sidecar["dlq_counts"],
        serde_json::json!({ "MultiValueJoinCollision": 2 }),
        "the sidecar counts the Sink's own dead letters by category"
    );
    assert_eq!(
        column_values(&dir.path().join("rejects.csv"), "_cxl_dlq_stage"),
        ["output:out", "output:out"],
        "both collisions were written to the dead-letter file"
    );
}

/// A Source feeding a Sink directly; a value that does not parse as its
/// declared type is dead-lettered at the source stage.
const SOURCE_TO_SINK: &str = r#"pipeline:
  name: dlq_streaming_preview
error_handling:
  strategy: continue
  dlq:
    path: rejects.csv
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: int }
      - { name: amount, type: int }
- type: sink
  name: out
  input: src
  config:
    name: out
    path: out.csv
    type: csv
"#;

#[test]
fn preview_exit_code_reflects_dead_letters() {
    let dir = tempfile::tempdir().expect("tempdir");
    let output = run_clinker(
        dir.path(),
        SOURCE_TO_SINK,
        &[("input.csv", "id,amount\n1,1\n2,x\n3,3\n")],
        &["--dry-run", "-n", "10"],
    );
    assert_eq!(
        output.status.code(),
        Some(2),
        "a preview that dead-letters exits 2: {}",
        describe(&output)
    );
    assert!(
        !dir.path().join("rejects.csv").exists(),
        "a preview writes no dead-letter file"
    );
    assert_eq!(
        csv_files(dir.path()),
        ["input.csv"],
        "a preview publishes nothing"
    );
}

/// `src → ratio → split → {high, low}`: `ratio` fails on a row whose
/// `amount` is zero, and `split`'s condition fails on a row whose `d` is
/// zero. One source and no interleave, so the row order is fixed by the
/// input: the walk dispatches node by node, so the Transform's failures
/// come first, in input order, then the Route's. `input` is the source file.
fn transform_and_route_failures(input: &Path) -> String {
    format!(
        r#"pipeline:
  name: dlq_determinism
error_handling:
  strategy: continue
  dlq:
    path: rejects.csv
nodes:
- type: source
  name: src
  config:
    name: src
    path: '{}'
    type: csv
    schema:
      - {{ name: id, type: int }}
      - {{ name: amount, type: int }}
      - {{ name: d, type: int }}
- type: transform
  name: ratio
  input: src
  config:
    cxl: |
      emit id = id
      emit d = d
      emit ratio = id / amount
- type: route
  name: split
  input: ratio
  config:
    conditions:
      high: 100 / d > 10
    default: low
- type: sink
  name: high
  input: split
  config:
    name: high
    path: high.csv
    type: csv
- type: sink
  name: low
  input: split
  config:
    name: low
    path: low.csv
    type: csv
"#,
        input.display()
    )
}

/// Transform failures on rows 2, 5 and 9; route failures on rows 3, 7 and 8.
const DETERMINISM_INPUT: &str = "id,amount,d\n\
1,1,5\n2,0,5\n3,3,0\n4,4,20\n5,0,1\n6,6,2\n7,7,0\n8,8,0\n9,0,4\n10,10,50\n";

/// The bytes of the DLQ file at `path` with every `_cxl_dlq_id` and
/// `_cxl_dlq_timestamp` cell replaced by a fixed placeholder, and how many
/// data rows it has.
///
/// Each cell is found by parsing the file under its own header, so the
/// columns are located by name. Each parsed value is then replaced in the
/// raw bytes, after checking it occurs there exactly as often as in those
/// cells, so no other byte of the file can be rewritten. Every id must be a
/// version-7 UUID and every timestamp RFC 3339.
fn masked_dead_letters(path: &Path) -> (Vec<u8>, usize) {
    let ids = column_values(path, "_cxl_dlq_id");
    let timestamps = column_values(path, "_cxl_dlq_timestamp");
    assert_eq!(ids.len(), timestamps.len());
    let mut bytes = std::fs::read_to_string(path).expect("read DLQ");
    for id in &ids {
        let parsed = uuid::Uuid::parse_str(id).unwrap_or_else(|e| panic!("{id:?}: {e}"));
        assert_eq!(parsed.get_version_num(), 7, "{id} is a version-7 UUID");
        assert_eq!(bytes.matches(id.as_str()).count(), 1, "{id} occurs once");
        bytes = bytes.replace(id.as_str(), "<id>");
    }
    let mut distinct = timestamps.clone();
    distinct.sort();
    distinct.dedup();
    for timestamp in &distinct {
        chrono::DateTime::parse_from_rfc3339(timestamp)
            .unwrap_or_else(|e| panic!("{timestamp:?} is RFC 3339: {e}"));
        let cells = timestamps.iter().filter(|t| *t == timestamp).count();
        assert_eq!(
            bytes.matches(timestamp.as_str()).count(),
            cells,
            "{timestamp} occurs only in its timestamp cells"
        );
        bytes = bytes.replace(timestamp.as_str(), "<timestamp>");
    }
    (bytes.into_bytes(), ids.len())
}

#[test]
fn two_runs_produce_identical_dlq_bytes_modulo_id_and_timestamp() {
    // Both runs read the same input file, whose path is recorded in every
    // row's `_cxl_dlq_source_file`; each run publishes into its own directory.
    let inputs = tempfile::tempdir().expect("input tempdir");
    let input = inputs.path().join("input.csv");
    std::fs::write(&input, DETERMINISM_INPUT).expect("write input");
    let pipeline = transform_and_route_failures(&input);
    let mut runs = Vec::new();
    for _ in 0..2 {
        let dir = tempfile::tempdir().expect("tempdir");
        let output = run_clinker(dir.path(), &pipeline, &[], &["--allow-absolute-paths"]);
        assert_eq!(output.status.code(), Some(2), "{}", describe(&output));
        let path = dir.path().join("rejects.csv");
        let raw = std::fs::read(&path).expect("DLQ file published");
        assert_eq!(
            column_values(&path, "id"),
            ["2", "5", "9", "3", "7", "8"],
            "one row per failure, in dispatch order"
        );
        assert_eq!(
            column_values(&path, "_cxl_dlq_stage"),
            [
                "transform:ratio",
                "transform:ratio",
                "transform:ratio",
                "route_eval",
                "route_eval",
                "route_eval"
            ],
            "both Transform and Route failures are dead-lettered"
        );
        runs.push((raw, masked_dead_letters(&path)));
    }
    let (first_raw, (first, rows)) = &runs[0];
    let (second_raw, (second, _)) = &runs[1];
    assert_eq!(*rows, 6);
    assert_ne!(
        first_raw, second_raw,
        "each run stamps its own ids, so the raw files differ"
    );
    assert_eq!(
        String::from_utf8_lossy(first),
        String::from_utf8_lossy(second),
        "the files are byte-identical once ids and timestamps are masked"
    );
    assert_eq!(first, second);
}
