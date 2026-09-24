//! Every walk-thread place that can dead-letter a record writes inside the
//! header the compiled plan fixed for that record's bucket, and the header
//! does not depend on which stages failed.
//!
//! Each case runs the real `clinker run` in a temporary directory, compiles
//! the same YAML in-process, and compares every dead-letter file the run
//! published with the compiled bucket header, byte for byte. A row the
//! layout did not admit fails the run with an internal error rather than
//! being written, so each case also asserts that no such refusal happened.
//!
//! A site fed straight from a Source would prove nothing about its own rule:
//! under `continue` the Source's rejection schema already admits every
//! declared column into the same file. So each case past the Source puts a
//! Transform that adds a `marker` column in front of the site, and a row
//! carrying `marker` shows the site's own rule admitted its input schema.
//!
//! Dead-letter paths in the YAML are absolute (`{dir}` is replaced with the
//! temporary directory), so the in-process bucket identity and the child
//! process agree whatever the test process's working directory is.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::{Command, Output};

use clinker_format::FormatReader;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::plan::CompiledPlan;
use clinker_record::Value;

/// One dead-letter row, keyed by header column. Empty and null cells are
/// both the empty string.
type Row = BTreeMap<String, String>;

/// One dead-letter file a run published.
struct DlqFile {
    /// The file name of the bucket's path.
    name: String,
    /// The first line of the file, exactly as written.
    header_line: String,
    /// Every row, in file order.
    rows: Vec<Row>,
}

/// Replace `{dir}` in a YAML template with the run directory. The path goes
/// inside single-quoted YAML scalars, where a Windows backslash is literal.
fn render(yaml: &str, dir: &Path) -> String {
    yaml.replace("{dir}", &dir.display().to_string())
}

fn write_inputs(dir: &Path, inputs: &[(&str, &str)]) {
    for (name, contents) in inputs {
        std::fs::write(dir.join(name), contents).expect("write input");
    }
}

/// Run `clinker run` on `yaml` with `dir` as the working directory.
fn run_clinker(dir: &Path, yaml: &str) -> Output {
    let pipeline_path = dir.join("pipeline.yaml");
    std::fs::write(&pipeline_path, yaml).expect("write pipeline");
    Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(dir)
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker")
}

/// Compile `yaml` in-process the way the CLI does for a pipeline in `dir`.
fn compile(dir: &Path, yaml: &str) -> CompiledPlan {
    parse_config(yaml)
        .expect("pipeline parses")
        .compile(&CompileContext::new(dir.to_path_buf()))
        .expect("pipeline compiles")
}

fn describe(output: &Output) -> String {
    format!(
        "exit: {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    )
}

/// Read every bucket file of `plan` that a run wrote, asserting that each file's
/// header line is the compiled header of the bucket whose path it is, and
/// that every row has exactly that many cells.
fn read_buckets(plan: &CompiledPlan) -> Vec<DlqFile> {
    let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
    let mut files = Vec::new();
    for bucket in layout.buckets() {
        let path = bucket.path();
        if !path.exists() {
            continue;
        }
        let written = std::fs::read(path).expect("read DLQ file");
        let first_line = written.split(|b| *b == b'\n').next().expect("header line");
        let header_line = std::str::from_utf8(first_line)
            .expect("utf-8 header")
            .to_owned();
        assert_eq!(
            header_line,
            bucket.header().join(","),
            "{} carries the compiled header of its bucket, byte for byte",
            path.display()
        );

        let mut reader = clinker_format::csv::CsvReader::from_reader(
            std::fs::File::open(path).expect("open DLQ"),
            Default::default(),
        );
        let mut rows = Vec::new();
        while let Some(record) = reader.next_record().expect("DLQ row parses") {
            let schema = record.schema();
            assert_eq!(
                schema.column_count(),
                bucket.header().len(),
                "every row of {} is read under the compiled header",
                path.display()
            );
            let row: Row = schema
                .columns()
                .iter()
                .map(|column| {
                    let cell = match record.get(column) {
                        Some(Value::String(s)) => s.to_string(),
                        Some(Value::Null) | None => String::new(),
                        Some(other) => panic!("{column}: unexpected cell {other:?}"),
                    };
                    (column.to_string(), cell)
                })
                .collect();
            rows.push(row);
        }
        files.push(DlqFile {
            name: path
                .file_name()
                .expect("bucket path names a file")
                .to_string_lossy()
                .into_owned(),
            header_line,
            rows,
        });
    }
    files
}

/// Run `yaml` (a template over `{dir}`) on `inputs` through the real binary,
/// then compare each dead-letter file it wrote with the compiled layout.
///
/// Asserts the run exits 2 (completed with dead-lettered rows), that it
/// refused no row as outside its bucket header, that it wrote at least one
/// dead-letter file, and that every file it wrote carries its bucket's
/// compiled header. Returns the files in bucket order.
fn run_and_compare(yaml: &str, inputs: &[(&str, &str)]) -> Vec<DlqFile> {
    let dir = tempfile::tempdir().expect("tempdir");
    let yaml = render(yaml, dir.path());
    write_inputs(dir.path(), inputs);
    let output = run_clinker(dir.path(), &yaml);
    // The dead-letter row encoder's refusal for a column outside its
    // bucket's compiled header.
    assert!(
        !String::from_utf8_lossy(&output.stderr)
            .contains("outside the compiled dead-letter header"),
        "no dead-letter row is refused as outside its compiled header.\n{}",
        describe(&output)
    );
    assert_eq!(
        output.status.code(),
        Some(2),
        "a run that dead-letters rows exits 2.\n{}",
        describe(&output)
    );
    let plan = compile(dir.path(), &yaml);
    let files = read_buckets(&plan);
    assert!(
        !files.is_empty(),
        "an exit-2 run of this pipeline publishes a dead-letter file.\n{}",
        describe(&output)
    );
    files
}

fn file<'a>(files: &'a [DlqFile], name: &str) -> &'a DlqFile {
    files.iter().find(|f| f.name == name).unwrap_or_else(|| {
        panic!(
            "{name} was published; files: {:?}",
            files.iter().map(|f| &f.name).collect::<Vec<_>>()
        )
    })
}

fn cell<'a>(row: &'a Row, column: &str) -> &'a str {
    row.get(column)
        .unwrap_or_else(|| panic!("{column} is a header column; row: {row:?}"))
}

/// The one file of a single-bucket run, holding `rows` rows.
fn only_file(files: &[DlqFile], rows: usize) -> &DlqFile {
    assert_eq!(files.len(), 1, "one dead-letter file is published");
    assert_eq!(
        files[0].rows.len(),
        rows,
        "{} rows in {}: {:?}",
        rows,
        files[0].name,
        files[0].rows
    );
    &files[0]
}

// ---------------------------------------------------------------------------
// Source rejections (site 1)
// ---------------------------------------------------------------------------

#[test]
fn source_declared_type_rejection() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_source_csv
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: int }
      - { name: amount, type: int }
      - { name: note, type: string }
- type: sink
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[("input.csv", "id,amount,note\n1,10,a\n2,ten,b\n3,30,c\n")],
    );
    let dlq = only_file(&files, 1);
    let row = &dlq.rows[0];
    assert_eq!(cell(row, "_cxl_dlq_source_name"), "src");
    assert_eq!(cell(row, "id"), "2");
    assert_eq!(cell(row, "note"), "b");
}

#[test]
fn source_fixed_width_rejection() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_source_fixed_width
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: src
  config:
    name: src
    type: fixed_width
    path: input.txt
    schema:
      - { name: id, type: int, start: 0, width: 3 }
      - { name: amount, type: int, start: 3, width: 4 }
- type: sink
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
"#;
    // Row 2's amount is blank: the reader yields null, which the declared
    // non-nullable `int` rejects.
    let files = run_and_compare(PIPELINE, &[("input.txt", "001  10\n002    \n003  30\n")]);
    let dlq = only_file(&files, 1);
    assert_eq!(cell(&dlq.rows[0], "_cxl_dlq_source_name"), "src");
    assert!(
        dlq.header_line
            .ends_with(",id,amount,_cxl_dlq_source_record"),
        "declared columns, then the raw-record column: {}",
        dlq.header_line
    );
}

#[test]
fn source_multi_record_unknown_discriminator() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_source_multi_record
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: payments
  config:
    name: payments
    type: fixed_width
    path: payments.txt
    schema:
      discriminator: { start: 0, width: 1 }
      records:
        - { id: detail, tag: D, columns: [ { name: id, type: int, start: 1, width: 5 }, { name: amount, type: int, start: 6, width: 4 } ] }
- type: sink
  name: out
  input: payments
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[("payments.txt", "D00001 100\nX99999 999\nD00002 200\n")],
    );
    let dlq = only_file(&files, 1);
    let row = &dlq.rows[0];
    assert_eq!(
        cell(row, "_cxl_dlq_source_record"),
        "X99999 999",
        "the unknown-tag row carries its physical line"
    );
    assert_eq!(
        cell(row, "_cxl_dlq_error_category"),
        "structural_validation"
    );
}

#[test]
fn source_json_rejection() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_source_json
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: src
  config:
    name: src
    type: json
    path: input.json
    schema:
      - { name: id, type: int }
      - { name: name, type: string }
- type: sink
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[(
            "input.json",
            r#"[{"id":1,"name":"a"},{"id":"two","name":"b"},{"id":3,"name":"c"}]"#,
        )],
    );
    let dlq = only_file(&files, 1);
    assert_eq!(cell(&dlq.rows[0], "name"), "b");
}

#[test]
fn source_fan_out_ceiling() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_source_fan_out
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: src
  config:
    name: src
    type: json
    path: input.json
    split_to_rows: [left, right]
    max_output_rows_per_input: 4
    schema:
      - { name: id, type: int }
      - { name: l, type: int }
      - { name: r, type: int }
- type: sink
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[(
            "input.json",
            r#"[{"id":7,"left":[{"l":0},{"l":1}],"right":[{"r":0},{"r":1},{"r":2}]}]"#,
        )],
    );
    let dlq = only_file(&files, 1);
    assert_eq!(
        cell(&dlq.rows[0], "_cxl_dlq_error_category"),
        "expansion_limit_exceeded"
    );
}

// ---------------------------------------------------------------------------
// Transform, Route and Reshape (sites 2, 4, 14)
// ---------------------------------------------------------------------------

#[test]
fn transform_eval_failure() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_transform
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
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
- type: transform
  name: ratio
  input: widen
  config:
    cxl: |
      emit id = id
      emit ratio = id / amount
- type: sink
  name: out
  input: ratio
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let files = run_and_compare(PIPELINE, &[("input.csv", "id,amount\n1,1\n2,0\n3,3\n")]);
    let dlq = only_file(&files, 1);
    let row = &dlq.rows[0];
    assert_eq!(cell(row, "id"), "2");
    assert_eq!(cell(row, "amount"), "0");
    assert_eq!(
        cell(row, "marker"),
        "widened",
        "the row carries the Transform's own input schema"
    );
    assert_eq!(cell(row, "_cxl_dlq_source_record"), "");
}

#[test]
fn route_predicate_failure() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_route
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: int }
      - { name: amount, type: int }
      - { name: gate, type: int }
- type: transform
  name: widen
  input: src
  config:
    cxl: |
      emit id = id
      emit amount = amount
      emit gate = gate
      emit marker = "widened"
- type: route
  name: split
  input: widen
  config:
    mode: exclusive
    conditions:
      big: 'amount / gate > 10'
    default: small
- type: sink
  name: big
  input: split.big
  config:
    name: big
    type: csv
    path: big.csv
- type: sink
  name: small
  input: split.small
  config:
    name: small
    type: csv
    path: small.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[("input.csv", "id,amount,gate\n1,100,1\n2,50,0\n3,5,1\n")],
    );
    let dlq = only_file(&files, 1);
    let row = &dlq.rows[0];
    assert_eq!(cell(row, "id"), "2");
    assert_eq!(cell(row, "gate"), "0");
    assert_eq!(
        cell(row, "marker"),
        "widened",
        "the row carries the Route's own input schema"
    );
}

#[test]
fn reshape_group_conflict() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_reshape
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: rows
  config:
    name: rows
    type: csv
    path: input.csv
    schema:
      - { name: gid, type: string }
      - { name: amount, type: int }
      - { name: tier, type: string }
- type: transform
  name: widen
  input: rows
  config:
    cxl: |
      emit gid = gid
      emit amount = amount
      emit tier = tier
      emit marker = "widened"
- type: reshape
  name: classify
  input: widen
  config:
    partition_by: [gid]
    rules:
      - name: bump_high
        when: "amount > 50"
        mutate:
          set:
            tier: "'high'"
      - name: bump_big
        when: "amount > 80"
        mutate:
          set:
            tier: "'big'"
- type: sink
  name: out
  input: classify
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[(
            "input.csv",
            "gid,amount,tier\nX,100,base\nX,30,base\nY,60,base\n",
        )],
    );
    let dlq = &files[0];
    assert_eq!(files.len(), 1, "one dead-letter file is published");
    assert!(
        dlq.rows.iter().all(|row| cell(row, "gid") == "X"
            && cell(row, "marker") == "widened"
            && cell(row, "_cxl_dlq_source_name") == "rows"),
        "only the conflicting group is dead-lettered: {:?}",
        dlq.rows
    );
    assert!(
        dlq.rows
            .iter()
            .any(|row| cell(row, "_cxl_dlq_error_category") == "mutation_conflict"),
        "the conflict itself is dead-lettered: {:?}",
        dlq.rows
    );
}

/// A `copy_from: none` Reshape row dead-lettered downstream is attributed to
/// its trigger's Source. Every stamp Source here has a `per_source` file, so
/// the pipeline-wide header admits no Source columns: a row that lost its
/// trigger's `$source.name` would fall to that file and be refused.
#[test]
fn synthesized_row_dead_letters_under_trigger_source() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_reshape_synthesized
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
    per_source:
      rows:
        path: '{dir}/row_rejects.csv'
nodes:
- type: source
  name: rows
  config:
    name: rows
    type: csv
    path: input.csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: int }
      - { name: divisor, type: int }
- type: reshape
  name: summarize
  input: rows
  config:
    partition_by: [id]
    rules:
      - name: big
        when: "amount > 100"
        synthesize:
          copy_from: none
          overrides:
            id: "id"
            amount: "amount"
            divisor: "0"
- type: transform
  name: ratio
  input: summarize
  config:
    cxl: |
      emit id = id
      emit ratio = amount / divisor
- type: sink
  name: out
  input: ratio
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[("input.csv", "id,amount,divisor\nA,150,1\nB,50,1\n")],
    );

    // Only the synthesized row (the one with a zero divisor) fails `ratio`.
    let rows = file(&files, "row_rejects.csv");
    assert_eq!(rows.rows.len(), 1, "one row dead-lettered: {:?}", rows.rows);
    let row = &rows.rows[0];
    assert_eq!(cell(row, "_cxl_dlq_source_name"), "rows");
    assert_eq!(cell(row, "_cxl_dlq_stage"), "transform:ratio");
    assert_eq!(cell(row, "id"), "A");
    assert_eq!(cell(row, "amount"), "150");
    assert_eq!(cell(row, "divisor"), "0");
    assert!(
        files
            .iter()
            .filter(|f| f.name != "row_rejects.csv")
            .all(|f| f.rows.is_empty()),
        "nothing reaches the pipeline-wide file"
    );
}

// ---------------------------------------------------------------------------
// Aggregates (sites 8-11)
// ---------------------------------------------------------------------------

#[test]
fn time_window_late_record() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_late_record
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: clicks
  config:
    name: clicks
    type: csv
    path: input.csv
    watermark: { column: event_ts }
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: transform
  name: widen
  input: clicks
  config:
    cxl: |
      emit user_id = user_id
      emit event_ts = event_ts
      emit marker = "widened"
- type: aggregate
  name: hourly
  input: widen
  config:
    group_by: [user_id]
    time_window:
      tumbling: { size: 1h }
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: sink
  name: out
  input: hourly
  config:
    name: out
    type: csv
    path: out.csv
"#;
    // 10:30 advances the watermark past 10:00, so 09:30 arrives after its
    // window [09:00, 10:00) closed.
    let files = run_and_compare(
        PIPELINE,
        &[(
            "input.csv",
            "user_id,event_ts\nu1,2026-05-14T09:00:00\nu1,2026-05-14T10:30:00\nu1,2026-05-14T09:30:00\n",
        )],
    );
    let dlq = only_file(&files, 1);
    let row = &dlq.rows[0];
    assert_eq!(cell(row, "_cxl_dlq_error_category"), "late_record");
    assert_eq!(cell(row, "_cxl_dlq_stage"), "time_window:hourly");
    assert_eq!(cell(row, "user_id"), "u1");
    assert_eq!(
        cell(row, "marker"),
        "widened",
        "the row carries the time window's own input schema"
    );
}

#[test]
fn aggregate_add_error() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_aggregate_add
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: g, type: string }
      - { name: amount, type: int }
      - { name: divisor, type: int }
- type: transform
  name: widen
  input: src
  config:
    cxl: |
      emit g = g
      emit amount = amount
      emit divisor = divisor
      emit marker = "widened"
- type: aggregate
  name: totals
  input: widen
  config:
    group_by: [g]
    cxl: |
      emit g = g
      emit total = sum(amount / divisor)
- type: sink
  name: out
  input: totals
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[("input.csv", "g,amount,divisor\na,10,2\na,20,0\nb,30,3\n")],
    );
    let dlq = only_file(&files, 1);
    let row = &dlq.rows[0];
    assert_eq!(cell(row, "_cxl_dlq_stage"), "aggregate:totals");
    assert_eq!(cell(row, "amount"), "20");
    assert_eq!(cell(row, "divisor"), "0");
    assert_eq!(
        cell(row, "marker"),
        "widened",
        "the row carries the Aggregate's own input schema"
    );
}

/// The empty-row branch of the aggregate finalize site (assumption A2).
///
/// A global fold whose finalize fails with no buffered record dead-letters an
/// empty row of the Aggregate's own output schema, so the layout admits that
/// schema into the pipeline-wide file. No current accumulator can fail on
/// zero inputs: the only fallible finalize is the integer or decimal sum's
/// overflow, and a sum that saw no value finalizes to null first. So the
/// empty-row branch is not observable through the binary today; what is
/// observable is asserted here instead:
///
/// - a header-only input finalizes the fold's one defaulted row and
///   dead-letters nothing;
/// - the pipeline-wide header admits the fold's output columns;
/// - a sum that does overflow dead-letters its first buffered input record
///   (the finalize site's other branch), and a declared-type rejection at the
///   Source lands under the same header in the same file.
///
/// The fold reads its Source directly. That keeps it on the materialized
/// ingest arm, which dead-letters a finalize failure under `continue`; the
/// streaming-ingest arm (taken, for example, behind a Transform) fails the
/// run on the same overflow instead.
#[test]
fn global_fold_finalize_without_rows() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_global_fold
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: int }
      - { name: amount, type: int }
- type: aggregate
  name: fold
  input: src
  config:
    group_by: []
    cxl: |
      emit rows = count(*)
      emit total = sum(amount)
- type: sink
  name: out
  input: fold
  config:
    name: out
    type: csv
    path: out.csv
"#;
    // Zero input rows: the fold emits its defaulted row and nothing fails.
    let empty = tempfile::tempdir().expect("tempdir");
    let yaml = render(PIPELINE, empty.path());
    write_inputs(empty.path(), &[("input.csv", "id,amount\n")]);
    let output = run_clinker(empty.path(), &yaml);
    assert_eq!(
        output.status.code(),
        Some(0),
        "a fold over zero rows finalizes cleanly.\n{}",
        describe(&output)
    );
    let plan = compile(empty.path(), &yaml);
    let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
    assert!(
        read_buckets(&plan).is_empty(),
        "nothing is dead-lettered for zero input rows"
    );
    let fallback = layout
        .bucket_for_source("fold")
        .expect("a row attributed to the node reaches the pipeline-wide file");
    assert!(layout.is_fallback(fallback));
    let user_columns = layout.bucket(fallback).user_columns();
    for column in ["rows", "total"] {
        assert!(
            user_columns.iter().any(|c| c == column),
            "the pipeline-wide header admits the fold output column {column}: {user_columns:?}"
        );
    }

    // The sum overflows i64 at finalize, and one row fails its declared type.
    let files = run_and_compare(
        PIPELINE,
        &[(
            "input.csv",
            "id,amount\n1,9223372036854775807\n2,oops\n3,1\n",
        )],
    );
    let dlq = only_file(&files, 2);
    assert_eq!(
        dlq.header_line,
        layout.bucket(fallback).header().join(","),
        "every run of this pipeline writes the same header"
    );
    let finalize = dlq
        .rows
        .iter()
        .find(|row| cell(row, "_cxl_dlq_error_category") == "aggregate_finalize")
        .unwrap_or_else(|| panic!("the overflow is dead-lettered: {:?}", dlq.rows));
    assert_eq!(cell(finalize, "_cxl_dlq_stage"), "aggregate:fold");
    assert_eq!(
        cell(finalize, "id"),
        "1",
        "a finalize failure with buffered records carries the first one"
    );
    assert_eq!(cell(finalize, "total"), "");
    assert!(
        dlq.rows.iter().any(|row| cell(row, "id") == "2"),
        "the declared-type rejection shares the file: {:?}",
        dlq.rows
    );
}

// ---------------------------------------------------------------------------
// Combine (sites 12, 13)
// ---------------------------------------------------------------------------

#[test]
fn combine_probe_and_build_rows() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_site_combine
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
    per_source:
      src_bld:
        path: '{dir}/build_rejects.csv'
nodes:
- type: source
  name: src_drv
  config:
    name: src_drv
    type: csv
    path: drv.csv
    schema:
      - { name: id, type: int }
      - { name: amt, type: int }
- type: source
  name: src_bld
  config:
    name: src_bld
    type: csv
    path: bld.csv
    schema:
      - { name: id, type: int }
      - { name: factor, type: int }
- type: transform
  name: drv
  input: src_drv
  config:
    cxl: |
      emit id = id
      emit amt = amt
      emit drv_marker = "probe"
- type: transform
  name: bld
  input: src_bld
  config:
    cxl: |
      emit id = id
      emit factor = factor
      emit bld_marker = "build"
- type: combine
  name: enriched
  input:
    d: drv
    b: bld
  config:
    where: 'd.id == b.id'
    match: first
    on_miss: skip
    propagate_ck: driver
    cxl: |
      emit id = d.id
      emit ratio = d.amt / b.factor
- type: sink
  name: out
  input: enriched
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[
            ("drv.csv", "id,amt\n1,10\n2,20\n3,30\n"),
            ("bld.csv", "id,factor\n3,5\n1,2\n2,0\n"),
        ],
    );
    assert_eq!(files.len(), 2, "the probe and build rows split by source");

    let probe = file(&files, "rejects.csv");
    assert_eq!(probe.rows.len(), 1, "{:?}", probe.rows);
    assert_eq!(cell(&probe.rows[0], "_cxl_dlq_source_name"), "src_drv");
    assert_eq!(cell(&probe.rows[0], "amt"), "20");
    assert_eq!(cell(&probe.rows[0], "drv_marker"), "probe");
    assert!(
        !probe.header_line.contains("factor") && !probe.header_line.contains("bld_marker"),
        "the probe file admits no build column: {}",
        probe.header_line
    );

    let build = file(&files, "build_rejects.csv");
    assert_eq!(build.rows.len(), 1, "{:?}", build.rows);
    assert_eq!(cell(&build.rows[0], "_cxl_dlq_source_name"), "src_bld");
    assert_eq!(cell(&build.rows[0], "factor"), "0");
    assert_eq!(cell(&build.rows[0], "bld_marker"), "build");
    assert!(
        !build.header_line.contains("amt") && !build.header_line.contains("drv_marker"),
        "the build file admits no probe column: {}",
        build.header_line
    );
}

// ---------------------------------------------------------------------------
// Stable shape and per-source buckets
// ---------------------------------------------------------------------------

/// The dead-letter header is a property of the pipeline, not of which stages
/// happened to fail: three inputs that each fail a different stage write the
/// same header line.
#[test]
fn header_is_identical_whichever_stages_fail() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_stable_shape
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: int }
      - { name: amount, type: int }
      - { name: divisor, type: int }
      - { name: gate, type: int }
- type: transform
  name: scale
  input: src
  config:
    cxl: |
      emit id = id
      emit amount = amount
      emit gate = gate
      emit scaled = amount / divisor
- type: route
  name: split
  input: scale
  config:
    mode: exclusive
    conditions:
      big: 'amount / gate > 10'
    default: small
- type: sink
  name: big
  input: split.big
  config:
    name: big
    type: csv
    path: big.csv
- type: sink
  name: small
  input: split.small
  config:
    name: small
    type: csv
    path: small.csv
"#;
    const HEADER: &str = "id,amount,divisor,gate\n";
    let transform_fails = run_and_compare(
        PIPELINE,
        &[("input.csv", &format!("{HEADER}1,100,0,1\n2,5,1,1\n"))],
    );
    let route_fails = run_and_compare(
        PIPELINE,
        &[("input.csv", &format!("{HEADER}1,100,1,0\n2,5,1,1\n"))],
    );
    let source_fails = run_and_compare(
        PIPELINE,
        &[("input.csv", &format!("{HEADER}1,lots,1,1\n2,5,1,1\n"))],
    );

    let stage = |files: &[DlqFile]| -> String {
        let dlq = only_file(files, 1);
        cell(&dlq.rows[0], "_cxl_dlq_stage").to_owned()
    };
    let stages = [
        stage(&transform_fails),
        stage(&route_fails),
        stage(&source_fails),
    ];
    assert!(
        stages[0] != stages[1] && stages[1] != stages[2] && stages[0] != stages[2],
        "each input fails a different stage: {stages:?}"
    );

    let dir = tempfile::tempdir().expect("tempdir");
    let plan = compile(dir.path(), &render(PIPELINE, dir.path()));
    let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
    let header = layout
        .bucket(layout.bucket_for_source("src").expect("routed"))
        .header()
        .join(",");
    for files in [&transform_fails, &route_fails, &source_fails] {
        assert_eq!(
            files[0].header_line, header,
            "the header line is the compiled header whichever stage failed"
        );
    }
}

/// A `per_source.<name>.path` override gives that Source its own file, and
/// each file carries only the columns its own bucket admits.
#[test]
fn per_source_override_splits_headers() {
    const PIPELINE: &str = r#"pipeline:
  name: dlq_per_source
error_handling:
  strategy: continue
  dlq:
    path: '{dir}/rejects.csv'
    per_source:
      vendors:
        path: '{dir}/vendor_rejects.csv'
nodes:
- type: source
  name: orders
  config:
    name: orders
    type: csv
    path: orders.csv
    schema:
      - { name: order_id, type: int }
      - { name: qty, type: int }
- type: source
  name: vendors
  config:
    name: vendors
    type: csv
    path: vendors.csv
    schema:
      - { name: vendor_id, type: int }
      - { name: rating, type: int }
      - { name: region, type: string }
- type: sink
  name: order_out
  input: orders
  config:
    name: order_out
    type: csv
    path: order_out.csv
- type: sink
  name: vendor_out
  input: vendors
  config:
    name: vendor_out
    type: csv
    path: vendor_out.csv
"#;
    let files = run_and_compare(
        PIPELINE,
        &[
            ("orders.csv", "order_id,qty\n1,2\n2,many\n"),
            (
                "vendors.csv",
                "vendor_id,rating,region\n7,high,north\n8,4,south\n",
            ),
        ],
    );
    assert_eq!(files.len(), 2, "each bucket gets its own file");

    let orders = file(&files, "rejects.csv");
    assert_eq!(orders.rows.len(), 1);
    assert_eq!(cell(&orders.rows[0], "order_id"), "2");
    assert!(
        orders
            .header_line
            .ends_with(",order_id,qty,_cxl_dlq_source_record"),
        "the pipeline-wide file admits only the orders columns: {}",
        orders.header_line
    );

    let vendors = file(&files, "vendor_rejects.csv");
    assert_eq!(vendors.rows.len(), 1);
    assert_eq!(cell(&vendors.rows[0], "vendor_id"), "7");
    assert!(
        vendors
            .header_line
            .ends_with(",vendor_id,rating,region,_cxl_dlq_source_record"),
        "the vendors file admits only the vendors columns: {}",
        vendors.header_line
    );
    assert_ne!(orders.header_line, vendors.header_line);
}
