//! End-to-end SWIFT MT ingestion and emission.
//!
//! Covers: (1) a SWIFT source surfaces its service blocks (1/2/3/5) as
//! file-level `$doc` envelope sections on every block-4 body record, proving
//! the message-level document context flows through the executor; (2) each
//! block-4 `:tag:value` line streams as one `[block, tag, value]` record;
//! (3) a header-only message still produces a balanced document level with a
//! clean drain; (4) a malformed message (unbalanced brace / missing `-}`
//! trailer) surfaces as a clean run failure rather than a panic; (5) a
//! SWIFT → SWIFT → SWIFT round-trip re-frames the block structure and
//! reproduces every field value byte-faithfully through the writer's executor
//! path — multi-line continuation values, interior braces, mid-line `-}`, and
//! interior blank lines all survive; (6) the `swift`+`split` combination is
//! rejected at config time (diagnostic `E342`).

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

/// Build a SWIFT source → SWIFT output round-trip pipeline that declares an
/// envelope section per service block the fixture carries and echoes each one
/// back through the writer's matching `*_from_doc` option. Only the blocks the
/// message actually carries are declared — the reader's pre-scan rejects a
/// section over an absent block, so a fixture without (say) block 2 must not
/// declare an `app` section.
fn swift_round_trip_yaml(blocks: &[(&str, u8, &str)]) -> String {
    let mut sections = String::new();
    let mut options = String::new();
    for (section, block_id, option) in blocks {
        sections.push_str(&format!(
            "          {section}:\n            extract: {{ segment: \"{block_id}\" }}\n"
        ));
        options.push_str(&format!("        {option}: {section}\n"));
    }
    format!(
        r#"
pipeline:
  name: swift_round_trip
nodes:
  - type: source
    name: message
    config:
      name: message
      type: swift
      glob: ./*.swift
      envelope:
        sections:
{sections}      schema:
        - {{ name: block, type: string }}
        - {{ name: tag, type: string }}
        - {{ name: value, type: string }}
  - type: output
    name: out
    input: message
    config:
      name: out
      type: swift
      path: out.swift
      options:
{options}"#
    )
}

/// Feed a SWIFT message through SWIFT → SWIFT (block-structure reconstruction)
/// using the given per-block section/option mapping, then re-read the
/// reconstructed output through SWIFT → CSV, returning the reconstructed SWIFT
/// bytes and the `block,tag,value` CSV rows so a test can assert the field
/// stream round-trips in order.
fn swift_to_swift_then_csv(fixture: &str, blocks: &[(&str, u8, &str)]) -> (String, String) {
    let yaml = swift_round_trip_yaml(blocks);
    let (_report, swift_out) =
        run(&yaml, "message", fixture, "in.swift", "out").expect("swift -> swift round-trip");

    let csv_yaml = r#"
pipeline:
  name: swift_to_csv
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
      include_header: false
"#;
    let (_report, csv_out) = run(csv_yaml, "message", &swift_out, "rt.swift", "out")
        .expect("reconstructed swift -> csv");
    (swift_out, csv_out)
}

/// Extract the `tag` column (CSV field index 1 of `block,tag,value`) from each
/// re-read body row, in order.
fn csv_tag_column(csv_out: &str) -> Vec<&str> {
    csv_out
        .lines()
        .map(|l| l.split(',').nth(1).unwrap_or(""))
        .collect()
}

#[test]
fn swift_round_trip_reparses_identically() {
    // The reconstructed output re-frames the full envelope in order, and the
    // re-read tag stream matches the source's block-4 fields exactly.
    let (swift_out, csv_out) = swift_to_swift_then_csv(
        &mt103(),
        &[
            ("basic", 1, "basic_header_from_doc"),
            ("app", 2, "app_header_from_doc"),
            ("user", 3, "user_header_from_doc"),
            ("trailer", 5, "trailer_from_doc"),
        ],
    );

    // Service blocks re-framed in order: {1:}{2:}{3:{108:}} then block 4,
    // then {5:}. Block 3's nested sub-block re-emits byte-for-byte.
    assert!(
        swift_out.starts_with(
            "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}{3:{108:MSGREF12345}}{4:\r\n"
        ),
        "envelope framing wrong: {swift_out:?}"
    );
    assert!(
        swift_out.ends_with("-}{5:{CHK:1234567890AB}}"),
        "trailer framing wrong: {swift_out:?}"
    );

    // The re-read tag column round-trips in order.
    assert_eq!(
        csv_tag_column(&csv_out),
        vec!["20", "23B", "32A"],
        "csv was: {csv_out}"
    );
}

#[test]
fn swift_mt940_repeated_tags_round_trip() {
    // Repeated :61:/:86: statement lines re-emit in arrival order; the re-read
    // tag stream preserves both repeats. MT940 carries only blocks 1 and 2.
    let (_swift_out, csv_out) = swift_to_swift_then_csv(
        &mt940(),
        &[
            ("basic", 1, "basic_header_from_doc"),
            ("app", 2, "app_header_from_doc"),
        ],
    );
    assert_eq!(
        csv_tag_column(&csv_out),
        vec!["20", "25", "28C", "60F", "61", "86", "61", "86", "62F"],
        "csv was: {csv_out}"
    );
}

/// Read a SWIFT message through the reader and collect its block-4
/// `(tag, value)` field stream, with each value's folded continuation breaks
/// intact. Used to assert value faithfulness through a round-trip.
fn read_swift_fields(data: &str) -> Vec<(String, Option<String>)> {
    use clinker_format::swift::reader::{SwiftReader, SwiftReaderConfig};
    use clinker_format::traits::FormatReader;
    use clinker_record::Value;
    let mut r = SwiftReader::new(
        Cursor::new(data.as_bytes().to_vec()),
        SwiftReaderConfig::default(),
    );
    let mut out = Vec::new();
    while let Some(rec) = r.next_record().unwrap() {
        let tag = match rec.get("tag") {
            Some(Value::String(s)) => s.to_string(),
            other => panic!("unexpected tag: {other:?}"),
        };
        let value = match rec.get("value") {
            Some(Value::String(s)) => Some(s.to_string()),
            Some(Value::Null) | None => None,
            other => panic!("unexpected value: {other:?}"),
        };
        out.push((tag, value));
    }
    out
}

#[test]
fn swift_round_trip_preserves_multiline_values_through_writer() {
    // The tag-column round-trip tests use fixtures with no folded continuation
    // values, so value faithfulness through the SWIFT WRITER's executor path
    // is otherwise unverified. This fixture carries a multi-line `:50K:`
    // ordering-customer block and a `:77E:` narrative with an interior blank
    // line, both of which fold continuation breaks into one value. Round-trip
    // SWIFT -> SWIFT through the executor, then re-read the writer's output and
    // assert every (tag, value) — values included — survives byte-faithfully.
    let fixture = "{1:F01BANKBEBBAXXX0000000000}{2:I103BANKDEFFXXXXN}\
        {4:\r\n:20:REF12345\r\n:32A:240101USD1000,00\r\n\
        :50K:/12345678\r\nJOHN DOE\r\n123 MAIN ST\r\n\
        :77E:NARRATIVE LINE ONE\r\n\r\nNARRATIVE LINE THREE\r\n-}";

    let (swift_out, _csv) = swift_to_swift_then_csv(
        fixture,
        &[
            ("basic", 1, "basic_header_from_doc"),
            ("app", 2, "app_header_from_doc"),
        ],
    );

    let original = read_swift_fields(fixture);
    let reconstructed = read_swift_fields(&swift_out);
    assert_eq!(
        original, reconstructed,
        "value stream not byte-faithful through the writer pipeline"
    );

    // Spot-check that the multi-line values specifically survived, not just
    // the single-line ones — the folded continuation breaks are the point.
    assert_eq!(
        original
            .iter()
            .find(|(t, _)| t == "50K")
            .and_then(|(_, v)| v.as_deref()),
        Some("/12345678\nJOHN DOE\n123 MAIN ST"),
        "multi-line :50K: value lost: {original:?}"
    );
    assert_eq!(
        original
            .iter()
            .find(|(t, _)| t == "77E")
            .and_then(|(_, v)| v.as_deref()),
        Some("NARRATIVE LINE ONE\n\nNARRATIVE LINE THREE"),
        "interior-blank-line :77E: value lost: {original:?}"
    );
}

#[test]
fn swift_interior_block4_braces_dashes_blanks_round_trip() {
    // The load-bearing faithfulness test: a value carrying an interior brace
    // (`:79:A{B`), a mid-line `-}` (`:79:SEE-}NOTE`), and an interior blank
    // line in a `:77E:` value must all reproduce byte-identically on re-read.
    // Block-4 free text is opaque, so the writer escapes nothing and the
    // reader re-parses each value unchanged.
    let fixture = "{1:F01BANKBEBBAXXX0000000000}\
        {4:\r\n:79:A{B\r\n:79:SEE-}NOTE\r\n:77E:LINE1\r\n\r\nLINE3\r\n:86:PLAIN\r\n-}";

    // Round-trip SWIFT -> SWIFT, then read the reconstructed output back
    // through a SWIFT reader and assert each value is byte-identical to the
    // value the original reader produced. The fixture carries only block 1.
    let (swift_out, _csv) =
        swift_to_swift_then_csv(fixture, &[("basic", 1, "basic_header_from_doc")]);

    let original = read_swift_fields(fixture);
    let reconstructed = read_swift_fields(&swift_out);
    assert_eq!(
        original, reconstructed,
        "field stream not byte-faithful after round-trip"
    );

    // Spot-check the load-bearing values survive verbatim.
    assert!(
        original
            .iter()
            .any(|(t, v)| t == "79" && v.as_deref() == Some("SEE-}NOTE")),
        "mid-line dash-brace value missing: {original:?}"
    );
    assert!(
        original
            .iter()
            .any(|(t, v)| t == "77E" && v.as_deref() == Some("LINE1\n\nLINE3")),
        "interior blank line value missing: {original:?}"
    );
    assert!(
        original
            .iter()
            .any(|(t, v)| t == "79" && v.as_deref() == Some("A{B")),
        "interior brace value missing: {original:?}"
    );
}

#[test]
fn swift_output_with_split_is_rejected_at_config_time() {
    // A SWIFT MT message is one indivisible brace-balanced envelope; byte-limit
    // splitting would finalize block 4 mid-stream. The combination must fail
    // config validation, not run and emit a corrupt message.
    let yaml = r#"
pipeline:
  name: swift_split
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
      type: swift
      path: out.swift
      split:
        max_bytes: 1024
"#;
    let err = parse_config(yaml).expect_err("swift + split must fail config validation");
    let msg = format!("{err:?}");
    assert!(msg.contains("E342"), "expected E342 diagnostic, got: {msg}");
}

#[test]
fn swift_max_fields_option_caps_block4_field_count() {
    // The source's `max_fields` option flows through to the reader. A message
    // carrying more block-4 fields than the configured cap fails the run with
    // a precise error naming the option — exercising the
    // options -> build_swift_reader_config -> reader path end-to-end.
    let over_cap = "{1:F01BANKBEBBAXXX0000000000}\
        {4:\r\n:20:ONE\r\n:21:TWO\r\n:22:THREE\r\n-}";
    let yaml = r#"
pipeline:
  name: swift_max_fields
nodes:
  - type: source
    name: message
    config:
      name: message
      type: swift
      glob: ./*.swift
      options:
        max_fields: 2
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
    let err = run(yaml, "message", over_cap, "big.swift", "out")
        .expect_err("an over-cap message must fail the run");
    assert!(
        err.contains("max_fields") || err.contains("SWIFT"),
        "error should name the field-count cap: {err}"
    );
}
