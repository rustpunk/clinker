//! The dead-letter file `clinker run` writes carries exactly the header the
//! compiled plan fixed for its bucket, whatever rows failed.

use std::process::Command;

use clinker_format::FormatReader;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_record::Value;

const PIPELINE: &str = r#"pipeline:
  name: dlq_plan_header
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
      - { name: note, type: string }
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
    include_unmapped: true
"#;

#[test]
fn single_source_bucket_header_matches_plan() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("input.csv"),
        "id,amount,note\n1,1,first\n2,0,zero one\n3,3,third\n4,0,zero two\n",
    )
    .expect("write input");
    let pipeline_path = dir.path().join("pipeline.yaml");
    std::fs::write(&pipeline_path, PIPELINE).expect("write pipeline");

    let output = Command::new(env!("CARGO_BIN_EXE_clinker"))
        .current_dir(dir.path())
        .arg("run")
        .arg(&pipeline_path)
        .output()
        .expect("spawn clinker");
    assert_eq!(
        output.status.code(),
        Some(2),
        "a run that dead-letters rows exits 2.\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );

    let plan = parse_config(PIPELINE)
        .expect("pipeline parses")
        .compile(&CompileContext::new(dir.path().to_path_buf()))
        .expect("pipeline compiles");
    let layout = plan.dlq_layout().expect("a DLQ block yields a layout");
    let id = layout
        .bucket_for_source("src")
        .expect("src routes to the pipeline-wide file");
    assert!(layout.is_fallback(id));
    let bucket = layout.bucket(id);
    assert_eq!(
        bucket.user_columns(),
        ["id", "amount", "note", "_cxl_dlq_source_record"],
        "declared columns in order, then the rejection column `continue` admits"
    );

    let path = dir.path().join("rejects.csv");
    let written = std::fs::read(&path).expect("DLQ file published");
    let first_line = written.split(|b| *b == b'\n').next().expect("header line");
    assert_eq!(
        std::str::from_utf8(first_line).expect("utf-8 header"),
        bucket.header().join(","),
        "the published header is the compiled bucket header, byte for byte"
    );

    // Rows carry a volatile id and timestamp; read them with the format
    // reader and check the stable cells.
    let mut reader = clinker_format::csv::CsvReader::from_reader(
        std::fs::File::open(&path).expect("open DLQ"),
        Default::default(),
    );
    let mut failing = Vec::new();
    while let Some(row) = reader.next_record().expect("DLQ row parses") {
        assert_eq!(
            row.schema().column_count(),
            bucket.header().len(),
            "every row is read under the compiled header"
        );
        let text = |name: &str| match row.get(name) {
            Some(Value::String(s)) => s.to_string(),
            Some(Value::Null) | None => String::new(),
            Some(other) => panic!("{name}: unexpected cell {other:?}"),
        };
        assert_eq!(
            text("_cxl_dlq_source_record"),
            "",
            "a transform failure carries no raw source record"
        );
        failing.push([text("id"), text("amount"), text("note")]);
    }
    assert_eq!(
        failing,
        [["2", "0", "zero one"], ["4", "0", "zero two"]],
        "each failing input row appears once"
    );
}
