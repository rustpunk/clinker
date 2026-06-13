//! End-to-end SWIFT MT ingestion.
//!
//! Covers: (1) a SWIFT source surfaces its service blocks (1/2/3/5) as
//! file-level `$doc` envelope sections on every block-4 body record, proving
//! the message-level document context flows through the executor; (2) each
//! block-4 `:tag:value` line streams as one `[block, tag, value]` record;
//! (3) a header-only message still produces a balanced document level with a
//! clean drain; (4) a malformed message (unbalanced brace / missing `-}`
//! trailer) surfaces as a clean run failure rather than a panic.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

/// A single-customer-credit-transfer MT103 with all five blocks: basic
/// header (1), application header (2), user header (3) with a nested
/// service-id sub-block, the message text (4), and a trailer (5). Every
/// block-4 field is one physical line so a CSV body row maps one-to-one.
fn mt103() -> String {
    "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}\
     {3:{108:MSGREF12345}}\
     {4:\r\n:20:REFERENCE12345\r\n:23B:CRED\r\n:32A:240101USD1000,00\r\n-}\
     {5:{CHK:1234567890AB}}"
        .to_string()
}

/// An MT940 statement with repeated `:61:` / `:86:` block-4 tags — one record
/// per statement line. Exercises repeated block-4 tags streaming in order.
fn mt940() -> String {
    "{1:F01BANKBEBBAXXX0000000000}{2:I940BANKDEFFXXXXN}\
     {4:\r\n:20:STMT001\r\n:25:12345678\r\n:28C:1/1\r\n\
     :60F:C240101USD5000,00\r\n\
     :61:2401010101D100,00NTRFREF1//BANK1\r\n:86:PAYMENT ONE\r\n\
     :61:2401020102C200,00NTRFREF2//BANK2\r\n:86:PAYMENT TWO\r\n\
     :62F:C240102USD5100,00\r\n-}"
        .to_string()
}

fn run(
    yaml: &str,
    source_name: &str,
    fixture: &str,
    file_name: &str,
    out_name: &str,
) -> Result<(clinker_exec::executor::ExecutionReport, String), String> {
    let config = parse_config(yaml).map_err(|e| format!("parse: {e:?}"))?;
    let plan = config
        .compile(&CompileContext::default())
        .map_err(|e| format!("compile: {e:?}"))?;

    let file = FileSlot::new(
        PathBuf::from(file_name),
        Box::new(Cursor::new(fixture.as_bytes().to_vec())),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        source_name.to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file]),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        out_name.to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .map_err(|e| format!("run: {e:?}"))?;
    Ok((
        report,
        String::from_utf8_lossy(&buf.contents()).into_owned(),
    ))
}

#[test]
fn swift_source_surfaces_service_blocks_as_doc_sections() {
    // Every block-4 body record carries the message-level document context,
    // whose declared sections expose the service blocks. The Transform reads
    // one field from each declared block:
    //   block 1 (basic header) under `basic`,
    //   block 2 (application header) under `app`,
    //   block 3 (user header, with its nested sub-block) under `user`,
    //   block 5 (trailer) under `tlr`.
    let yaml = r#"
pipeline:
  name: swift_envelope
nodes:
  - type: source
    name: message
    config:
      name: message
      type: swift
      glob: ./*.swift
      envelope:
        sections:
          basic:
            extract: { segment: "1" }
          app:
            extract: { segment: "2" }
          user:
            extract: { segment: "3" }
          tlr:
            extract: { segment: "5" }
      schema:
        - { name: block, type: string }
        - { name: tag, type: string }
        - { name: value, type: string }
  - type: transform
    name: tag
    input: message
    config:
      cxl: |
        emit tag = tag
        emit basic = $doc.basic.body
        emit app = $doc.app.body
        emit user = $doc.user.body
        emit tlr = $doc.tlr.body
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
      include_header: false
      include_unmapped: false
"#;
    let (report, output) =
        run(yaml, "message", &mt103(), "po.swift", "out").expect("run swift pipeline");
    // Three block-4 fields: :20:, :23B:, :32A:.
    assert_eq!(report.counters.total_count, 3, "output was: {output}");
    assert_eq!(report.counters.dlq_count, 0);

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 3, "3 body rows, no header; got: {output}");
    for row in &lines {
        // Every body row resolves all four declared service-block sections.
        assert!(
            row.contains("F01BANKBEBBAXXX0000000000"),
            "row missing $doc.basic.body: {row}"
        );
        assert!(
            row.contains("I103BANKDEFFXXXXN"),
            "row missing $doc.app.body: {row}"
        );
        // Block 3 keeps its nested {108:...} sub-block verbatim.
        assert!(
            row.contains("{108:MSGREF12345}"),
            "row missing $doc.user.body: {row}"
        );
        assert!(
            row.contains("{CHK:1234567890AB}"),
            "row missing $doc.tlr.body: {row}"
        );
    }
    // First body record is the :20: reference field's tag.
    assert!(lines[0].starts_with("20,"), "first row: {}", lines[0]);
}

#[test]
fn swift_block4_tag_lines_stream_as_records() {
    // Each block-4 `:tag:value` line becomes one record carrying the tag and
    // value; the constant `block` column stamps `4` on every body row.
    let yaml = r#"
pipeline:
  name: swift_body
nodes:
  - type: source
    name: message
    config:
      name: message
      type: swift
      glob: ./*.swift
      schema:
        - { name: block, type: string }
        - { name: tag, type: string }
        - { name: value, type: string }
  - type: transform
    name: tag
    input: message
    config:
      cxl: |
        emit block = block
        emit tag = tag
        emit value = value
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
      include_header: false
"#;
    let (report, output) =
        run(yaml, "message", &mt940(), "stmt.swift", "out").expect("run mt940 pipeline");
    // MT940 carries nine block-4 fields (including two repeated :61:/:86:).
    assert_eq!(report.counters.total_count, 9, "output was: {output}");
    assert_eq!(report.counters.dlq_count, 0);

    let rows: Vec<&str> = output.lines().collect();
    assert_eq!(rows.len(), 9, "9 body rows, no header; got: {output}");
    // Every row stamps the block-4 id and carries its tag/value.
    for row in &rows {
        assert!(row.starts_with("4,"), "block column wrong in row: {row}");
    }
    // The first statement-line tag (:61:) streams with its value (CSV quotes
    // the comma-bearing amount, so match on the embedded content).
    assert!(
        rows[4].starts_with("4,61,") && rows[4].contains("2401010101D100,00NTRFREF1//BANK1"),
        "first :61: row: {}",
        rows[4]
    );
    // The second :61: streams as a distinct record.
    assert!(
        rows[6].starts_with("4,61,") && rows[6].contains("2401020102C200,00NTRFREF2//BANK2"),
        "second :61: row: {}",
        rows[6]
    );
}

#[test]
fn swift_header_only_message_drains_clean() {
    // A header/trailer-only message (no block 4) emits no body records and
    // still drains cleanly — the balanced message-level document open/close
    // pair must not corrupt the document-context stack or panic.
    let header_only = "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}{5:{CHK:ABC}}";
    let yaml = r#"
pipeline:
  name: swift_header_only
nodes:
  - type: source
    name: message
    config:
      name: message
      type: swift
      glob: ./*.swift
      schema:
        - { name: block, type: string }
        - { name: tag, type: string }
        - { name: value, type: string }
  - type: output
    name: out
    input: message
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let (report, output) = run(yaml, "message", header_only, "hdr.swift", "out")
        .expect("header-only message must drain cleanly");
    assert_eq!(report.counters.total_count, 0, "no body records: {output}");
    assert_eq!(report.counters.dlq_count, 0);
}

#[test]
fn swift_unbalanced_block_fails_the_run() {
    // Block 2 never closes — an unbalanced brace the reader surfaces as a
    // clean run failure, not a panic.
    let bad = "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN";
    let yaml = r#"
pipeline:
  name: swift_bad_brace
nodes:
  - type: source
    name: message
    config:
      name: message
      type: swift
      glob: ./*.swift
      schema:
        - { name: block, type: string }
        - { name: tag, type: string }
        - { name: value, type: string }
  - type: output
    name: out
    input: message
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err = run(yaml, "message", bad, "bad.swift", "out")
        .expect_err("unbalanced brace must fail the run");
    assert!(
        err.contains("truncated") || err.contains("SWIFT"),
        "error should name the SWIFT block failure: {err}"
    );
}

#[test]
fn swift_missing_block4_trailer_fails_the_run() {
    // Block 4 with no `-}` trailer runs to EOF unbalanced — a truncation the
    // reader surfaces cleanly.
    let bad = "{1:F01BANKBEBBAXXX0000000000}{4:\r\n:20:REF\r\n";
    let yaml = r#"
pipeline:
  name: swift_bad_trailer
nodes:
  - type: source
    name: message
    config:
      name: message
      type: swift
      glob: ./*.swift
      schema:
        - { name: block, type: string }
        - { name: tag, type: string }
        - { name: value, type: string }
  - type: output
    name: out
    input: message
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err = run(yaml, "message", bad, "bad.swift", "out")
        .expect_err("missing block-4 trailer must fail the run");
    assert!(
        err.contains("truncated") || err.contains("SWIFT"),
        "error should name the SWIFT block failure: {err}"
    );
}
