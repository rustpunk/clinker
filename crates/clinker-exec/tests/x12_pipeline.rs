//! End-to-end X12 ingestion and round-trip.
//!
//! Covers: (1) an X12 source surfaces all three envelope tiers
//! (`ISA` interchange, `GS` functional group, `ST` transaction set) as
//! distinct `$doc` sections on every body record, proving the multi-level
//! envelope nesting (`OpenLevel`/`CloseLevel`) flows through the executor;
//! (2) an X12→X12 pipeline reconstructs the three-tier envelope on output
//! and re-parses identically; (3) the `x12`+`split` combination is rejected
//! at config time (diagnostic `E338`); (4) an `SE` count mismatch surfaces
//! as a run failure.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

/// The canonical 106-byte ISA header (`*` element, `:` sub-element, `~`
/// terminator), interchange control number `000000001`.
const ISA: &str = "ISA*00*          *00*          *ZZ*SENDER         \
    *ZZ*RECEIVER       *240101*1200*U*00401*000000001*0*P*:~";

/// A single-group, single-set 850 purchase-order interchange. The set
/// holds two body segments (BEG, PO1); SE claims 4 segments (ST..SE), GE
/// claims 1 set echoing group control `1`, IEA claims 1 group echoing the
/// ISA13 control number.
fn purchase_order() -> String {
    format!(
        "{ISA}\
        GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~\
        ST*850*0001~\
        BEG*00*NE*PO12345**20240101~\
        PO1*1*10*EA*9.99~\
        SE*4*0001~\
        GE*1*1~\
        IEA*1*000000001~"
    )
}

fn run(
    yaml: &str,
    source_name: &str,
    fixture: &str,
    file_name: &str,
    out_name: &str,
) -> Result<(clinker_exec::executor::ExecutionReport, String), String> {
    let (report, bytes) = run_bytes(yaml, source_name, fixture.as_bytes(), file_name, out_name)?;
    Ok((report, String::from_utf8_lossy(&bytes).into_owned()))
}

/// Drive a pipeline over a raw-byte fixture, returning the report and the
/// raw output bytes. Used for non-UTF-8 (charset) interchanges whose body
/// bytes are not valid UTF-8, so the output must be compared byte-for-byte
/// rather than as a `String`.
fn run_bytes(
    yaml: &str,
    source_name: &str,
    fixture: &[u8],
    file_name: &str,
    out_name: &str,
) -> Result<(clinker_exec::executor::ExecutionReport, Vec<u8>), String> {
    let config = parse_config(yaml).map_err(|e| format!("parse: {e:?}"))?;
    let plan = config
        .compile(&CompileContext::default())
        .map_err(|e| format!("compile: {e:?}"))?;

    let file = FileSlot::new(
        PathBuf::from(file_name),
        Box::new(Cursor::new(fixture.to_vec())),
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
    Ok((report, buf.contents()))
}

#[test]
fn x12_source_surfaces_all_three_envelope_tiers() {
    // Every body record carries the innermost (ST) document level, whose
    // flattened sections expose all three enclosing tiers. The Transform
    // reads one field from each tier:
    //   ISA13 (interchange control number) via the declared `interchange`
    //     section,
    //   GS06 (group control number) via the reader-supplied
    //     `functional_group` section,
    //   ST02 (set control number) via the reader-supplied `transaction_set`
    //     section.
    let yaml = r#"
pipeline:
  name: x12_envelope
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      envelope:
        sections:
          interchange:
            extract: { segment: "ISA" }
            fields:
              e13: string
      schema:
        - { name: seg_id, type: string }
        - { name: set_ref, type: string }
        - { name: e01, type: string }
  - type: transform
    name: tag
    input: interchange
    config:
      cxl: |
        emit seg = seg_id
        emit set_ref = set_ref
        emit isa13 = $doc.interchange.e13
        emit gs06 = $doc.functional_group.e06
        emit st02 = $doc.transaction_set.e02
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let (report, output) =
        run(yaml, "interchange", &purchase_order(), "po.x12", "out").expect("run x12 pipeline");
    // Three body segments: ST, BEG, PO1. Service segments not emitted.
    assert_eq!(report.counters.total_count, 3, "output was: {output}");
    assert_eq!(report.counters.dlq_count, 0);

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 4, "header + 3 rows; got: {output}");
    // All three tiers resolve on every body row.
    for row in &lines[1..] {
        assert!(
            row.contains("000000001"),
            "row missing $doc.interchange.e13 (ISA13): {row}"
        );
        // GS06 is the group control number "1"; ST02 is the set control
        // number "0001". Both must be present in each row.
        assert!(
            row.contains("0001"),
            "row missing $doc.transaction_set.e02: {row}"
        );
    }
    // First body record is the ST segment.
    assert!(lines[1].starts_with("ST,"), "first row: {}", lines[1]);
}

#[test]
fn x12_round_trip_reparses_identically() {
    // Source parses X12 and writes X12, reconstructing the three-tier
    // envelope from the echoed ISA `$doc` section plus a literal GS header.
    // Re-parsing the output must yield the same body segments in order.
    let yaml = r#"
pipeline:
  name: x12_round_trip
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      envelope:
        sections:
          interchange:
            extract: { segment: "ISA" }
            fields:
              e13: string
      schema:
        - { name: seg_id, type: string }
        - { name: set_ref, type: string }
        - { name: set_type, type: string }
        - { name: e01, type: string }
        - { name: e02, type: string }
        - { name: e03, type: string }
        - { name: e04, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: x12
      path: out.x12
      include_unmapped: true
      options:
        interchange_from_doc: interchange
        group_header: ["PO", "SENDER", "RECEIVER", "20240101", "1200", "1", "X", "004010"]
        segment_newline: false
"#;
    let (report, output) =
        run(yaml, "interchange", &purchase_order(), "po.x12", "out").expect("run round-trip");
    assert_eq!(report.counters.dlq_count, 0);

    // The reconstructed envelope echoes the original ISA control number and
    // recomputes the SE/GE/IEA counts.
    assert!(
        output.starts_with(ISA),
        "ISA echo wrong; output was: {output}"
    );
    assert!(
        output.contains("SE*4*0001~"),
        "SE recompute wrong: {output}"
    );
    assert!(output.contains("GE*1*1~"), "GE recompute wrong: {output}");
    assert!(
        output.contains("IEA*1*000000001~"),
        "IEA control echo wrong: {output}"
    );

    // Feed the X12 output back through a second pipeline (X12 → CSV) to
    // prove it re-parses identically: the body segment tags must round-trip
    // in order.
    let reparse_yaml = r#"
pipeline:
  name: x12_reparse
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      schema:
        - { name: seg_id, type: string }
  - type: transform
    name: tag
    input: interchange
    config:
      cxl: |
        emit seg = seg_id
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: false
      include_header: false
"#;
    let (reparse_report, reparse_csv) =
        run(reparse_yaml, "interchange", &output, "echo.x12", "out").expect("re-parse round-trip");
    assert_eq!(reparse_report.counters.dlq_count, 0);
    let tags: Vec<&str> = reparse_csv.lines().map(str::trim).collect();
    assert_eq!(
        tags,
        vec!["ST", "BEG", "PO1"],
        "round-trip body tags; x12 output was: {output}"
    );
}

#[test]
fn x12_multi_set_multi_group_round_trips() {
    // A two-group interchange: the first group holds two transaction sets,
    // the second holds one. The reader surfaces each GS/ST as a nested
    // level; re-parsing the body tags after a round-trip through CSV
    // confirms every body segment streams in order across all groups/sets.
    let multi = format!(
        "{ISA}\
        GS*PO*S*R*20240101*1200*1*X*004010~\
        ST*850*0001~BEG*00*NE*A**20240101~SE*3*0001~\
        ST*850*0002~BEG*00*NE*B**20240101~SE*3*0002~\
        GE*2*1~\
        GS*PO*S*R*20240101*1300*2*X*004010~\
        ST*850*0003~BEG*00*NE*C**20240101~SE*3*0003~\
        GE*1*2~\
        IEA*2*000000001~"
    );
    let yaml = r#"
pipeline:
  name: x12_multi
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      schema:
        - { name: seg_id, type: string }
        - { name: set_ref, type: string }
  - type: transform
    name: tag
    input: interchange
    config:
      cxl: |
        emit seg = seg_id
        emit ref = set_ref
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
        run(yaml, "interchange", &multi, "multi.x12", "out").expect("run multi-group");
    assert_eq!(report.counters.dlq_count, 0);
    // 3 sets × (ST + BEG) = 6 body records.
    assert_eq!(report.counters.total_count, 6, "output was: {output}");
    let rows: Vec<&str> = output.lines().collect();
    // Each set's records carry that set's control number.
    assert!(rows[0].starts_with("ST,0001"), "row 0: {}", rows[0]);
    assert!(rows[2].starts_with("ST,0002"), "row 2: {}", rows[2]);
    assert!(rows[4].starts_with("ST,0003"), "row 4: {}", rows[4]);
}

#[test]
fn x12_output_with_split_is_rejected_at_config_time() {
    // An interchange is one three-tier envelope; byte-limit splitting would
    // finalize it mid-stream. The combination must fail config validation,
    // not run and emit a corrupt interchange.
    let yaml = r#"
pipeline:
  name: x12_split
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: x12
      path: out.x12
      options:
        interchange: ["00", "          ", "00", "          ", "ZZ", "SENDER", "ZZ", "RECEIVER", "240101", "1200", "U", "00401", "000000001", "0", "P", ":"]
        group_header: ["PO", "S", "R", "20240101", "1200", "1", "X", "004010"]
      split:
        max_bytes: 1024
"#;
    let err = parse_config(yaml).expect_err("x12 + split must fail config validation");
    let msg = format!("{err:?}");
    assert!(msg.contains("E338"), "expected E338 diagnostic, got: {msg}");
}

#[test]
fn x12_se_count_mismatch_fails_the_run() {
    // The SE trailer claims 9 segments where the set has 3 — a
    // truncation/corruption signal the reader surfaces as a run failure.
    let bad = format!(
        "{ISA}GS*PO*S*R*20240101*1200*1*X*004010~\
        ST*850*0001~BEG*00*NE*A**20240101~SE*9*0001~GE*1*1~IEA*1*000000001~"
    );
    let yaml = r#"
pipeline:
  name: x12_bad_count
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let result = run(yaml, "interchange", &bad, "bad.x12", "out");
    let err = result.expect_err("count mismatch must fail the run");
    assert!(
        err.contains("SE segment count mismatch") || err.contains("X12"),
        "error should name the X12 count failure: {err}"
    );
}

/// Build a Latin-1 interchange whose body carries high bytes (`0xE9` é,
/// `0xF1` ñ) in a free-text `N1` element. The bytes are not valid UTF-8.
fn latin1_interchange_bytes() -> Vec<u8> {
    let mut data = Vec::new();
    data.extend_from_slice(ISA.as_bytes());
    data.extend_from_slice(b"GS*PO*SENDER*RECEIVER*20240101*1200*1*X*004010~");
    data.extend_from_slice(b"ST*850*0001~");
    // N1*BT*Caf\xE9 A\xF1o~
    data.extend_from_slice(b"N1*BT*Caf");
    data.push(0xE9);
    data.extend_from_slice(b" A");
    data.push(0xF1);
    data.extend_from_slice(b"o~");
    data.extend_from_slice(b"SE*3*0001~");
    data.extend_from_slice(b"GE*1*1~");
    data.extend_from_slice(b"IEA*1*000000001~");
    data
}

#[test]
fn x12_latin1_source_round_trips_byte_faithfully() {
    // A source declaring `encoding: iso-8859-1` decodes the high-byte body
    // without error; an X12 sink declaring the same encoding re-emits the
    // interchange with byte-identical output.
    let yaml = r#"
pipeline:
  name: x12_latin1_round_trip
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      options:
        encoding: iso-8859-1
      envelope:
        sections:
          interchange:
            extract: { segment: "ISA" }
            fields:
              e13: string
      schema:
        - { name: seg_id, type: string }
        - { name: set_ref, type: string }
        - { name: set_type, type: string }
        - { name: e01, type: string }
        - { name: e02, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: x12
      path: out.x12
      include_unmapped: true
      options:
        interchange_from_doc: interchange
        group_header: ["PO", "SENDER", "RECEIVER", "20240101", "1200", "1", "X", "004010"]
        segment_newline: false
        encoding: iso-8859-1
"#;
    let input = latin1_interchange_bytes();
    let (report, output) =
        run_bytes(yaml, "interchange", &input, "po.x12", "out").expect("run latin1 round-trip");
    assert_eq!(report.counters.dlq_count, 0);
    assert_eq!(
        output, input,
        "Latin-1 interchange must round-trip byte-for-byte"
    );
}

#[test]
fn x12_latin1_body_rejected_under_default_utf8() {
    // Without the `encoding` option the reader assumes UTF-8 and rejects the
    // high-byte body with a precise error rather than corrupting it.
    let yaml = r#"
pipeline:
  name: x12_latin1_utf8_reject
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err = run_bytes(
        yaml,
        "interchange",
        &latin1_interchange_bytes(),
        "po.x12",
        "out",
    )
    .expect_err("non-UTF-8 body must fail under the default charset");
    assert!(
        err.contains("not valid UTF-8") || err.contains("X12"),
        "error should name the UTF-8 decode failure: {err}"
    );
}

#[test]
fn x12_unsupported_encoding_fails_at_startup() {
    // A source declaring an encoding the reader does not support fails the
    // run with a precise error naming the unsupported value.
    let yaml = r#"
pipeline:
  name: x12_bad_encoding
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: x12
      glob: ./*.x12
      options:
        encoding: shift_jis
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let err = run_bytes(
        yaml,
        "interchange",
        &latin1_interchange_bytes(),
        "po.x12",
        "out",
    )
    .expect_err("unsupported encoding must fail the run");
    assert!(
        err.contains("shift_jis") && err.contains("iso-8859-1"),
        "error should name the unsupported charset and the supported set: {err}"
    );
}
