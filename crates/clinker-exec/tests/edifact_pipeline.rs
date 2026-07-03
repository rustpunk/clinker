//! End-to-end EDIFACT ingestion and round-trip.
//!
//! Covers: (1) an EDIFACT source declaring a `UNB` envelope section feeds
//! `$doc.<section>.<field>` into a Transform and writes CSV; (2) an
//! EDIFACT→EDIFACT pipeline reconstructs the interchange envelope on
//! output and re-parses identically (single-message ORDERS and a
//! representative multi-message INVOIC with composite elements and line
//! groups); (3) the `edifact`+`split` combination is rejected at config
//! time; (4) a UNT count mismatch surfaces as a run failure.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

/// A single-message ORDERS interchange (no UNA; Level-A defaults). UNB
/// control reference `REF1`; one UNH..UNT message of 4 segments.
const ORDERS: &str = "UNB+UNOA:1+SENDER+RECEIVER+240101:1200+REF1'\
    UNH+M1+ORDERS:D:96A:UN'\
    BGM+220+12345'\
    NAD+BY+ACME'\
    UNT+4+M1'\
    UNZ+1+REF1'";

/// A representative INVOIC interchange: a `UNA` service-string advice
/// header (explicit Level-A delimiters), two messages, composite elements
/// (`DTM`/`MOA` data-element groups split on `:`), repeated `LIN`/`PIA`/`QTY`
/// line-item groups, and recomputed `UNT`/`UNZ` control counts. Exercises a
/// multi-segment-group message shape — not just a 3-segment synthetic one.
const INVOIC: &str = "UNA:+.? '\
    UNB+UNOC:3+SUPPLIER:14+BUYER:14+240310:0900+IC0007'\
    UNH+INV1+INVOIC:D:96A:UN'\
    BGM+380+INV-9001+9'\
    DTM+137:20240310:102'\
    NAD+SU+SUPPLIER::92'\
    NAD+BY+BUYER::92'\
    LIN+1++0764569910:EN'\
    PIA+1+ABC123:SA'\
    QTY+47:10'\
    MOA+203:150.00'\
    LIN+2++0764569927:EN'\
    PIA+1+ABC124:SA'\
    QTY+47:5'\
    MOA+203:75.00'\
    MOA+86:225.00'\
    UNT+15+INV1'\
    UNH+INV2+INVOIC:D:96A:UN'\
    BGM+380+INV-9002+9'\
    DTM+137:20240311:102'\
    NAD+SU+SUPPLIER::92'\
    NAD+BY+BUYER::92'\
    LIN+1++0764569934:EN'\
    QTY+47:3'\
    MOA+203:30.00'\
    MOA+86:30.00'\
    UNT+10+INV2'\
    UNZ+2+IC0007'";

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
    Ok((report, buf.as_string()))
}

#[test]
fn edifact_source_exposes_unb_envelope_to_transform() {
    let yaml = r#"
pipeline:
  name: edifact_envelope
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: edifact
      glob: ./*.edi
      envelope:
        sections:
          unb:
            extract: { segment: "UNB" }
            fields:
              e05: string
      schema:
        - { name: seg_id, type: string }
        - { name: msg_ref, type: string }
        - { name: e01, type: string }
  - type: transform
    name: tag
    input: interchange
    config:
      cxl: |
        emit seg = seg_id
        emit ref = msg_ref
        emit control = $doc.unb.e05
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let (report, output) =
        run(yaml, "interchange", ORDERS, "orders.edi", "out").expect("run edifact pipeline");
    // Three body segments: UNH, BGM, NAD. Service segments not emitted.
    assert_eq!(report.counters.total_count, 3, "output was: {output}");
    assert_eq!(report.counters.dlq_count, 0);

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 4, "header + 3 rows; got: {output}");
    // The UNB control reference (element 5) resolves on every body row.
    for row in &lines[1..] {
        assert!(
            row.contains("REF1"),
            "row missing $doc.unb.e05 control ref: {row}"
        );
    }
    // First body record is the UNH segment.
    assert!(lines[1].starts_with("UNH,"), "first row: {}", lines[1]);
}

#[test]
fn edifact_round_trip_reparses_identically() {
    // Source parses EDIFACT and writes EDIFACT, reconstructing the
    // interchange envelope from literal header elements. Re-parsing the
    // output must yield the same body segments as the input.
    let yaml = r#"
pipeline:
  name: edifact_round_trip
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: edifact
      glob: ./*.edi
      schema:
        - { name: seg_id, type: string }
        - { name: msg_ref, type: string }
        - { name: msg_type, type: string }
        - { name: e01, type: string }
        - { name: e02, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: edifact
      path: out.edi
      include_unmapped: true
      options:
        interchange: ["UNOA:1", "SENDER", "RECEIVER", "240101:1200", "REF1"]
        segment_newline: false
"#;
    let (report, output) =
        run(yaml, "interchange", ORDERS, "orders.edi", "out").expect("run round-trip");
    assert_eq!(report.counters.dlq_count, 0);

    // The reconstructed envelope echoes the original control reference
    // and recomputes the message count.
    assert!(
        output.contains("UNZ+1+REF1'"),
        "round-trip UNZ control ref echo; output was: {output}"
    );

    // Feed the EDIFACT output back through a second pipeline (EDIFACT →
    // CSV) to prove it re-parses identically: the body segment tags must
    // round-trip in order.
    let reparse_yaml = r#"
pipeline:
  name: edifact_reparse
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: edifact
      glob: ./*.edi
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
        run(reparse_yaml, "interchange", &output, "echo.edi", "out").expect("re-parse round-trip");
    assert_eq!(reparse_report.counters.dlq_count, 0);
    let tags: Vec<&str> = reparse_csv.lines().map(str::trim).collect();
    assert_eq!(
        tags,
        vec!["UNH", "BGM", "NAD"],
        "round-trip body tags; edifact output was: {output}"
    );
}

#[test]
fn edifact_round_trip_echoes_unb_from_doc_context() {
    // The output reconstructs the UNB header from the source's `$doc`
    // envelope section (`interchange_from_doc`) rather than literal
    // config — the full reader → document-context → writer path. The UNB
    // here carries an empty middle element (an empty prep date/time, which
    // keeps the control reference at the standard 5th-element position) and
    // a release-escaped apostrophe in a body value; both must survive.
    let input = "UNB+UNOA:1+SENDER+RECEIVER++REF9'\
        UNH+M1+ORDERS:D:96A:UN'\
        BGM+220+12345'\
        NAD+BY+O?'BRIEN'\
        UNT+4+M1'\
        UNZ+1+REF9'";
    let yaml = r#"
pipeline:
  name: edifact_from_doc
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: edifact
      glob: ./*.edi
      envelope:
        sections:
          unb:
            extract: { segment: "UNB" }
            fields:
              e05: string
      schema:
        - { name: seg_id, type: string }
        - { name: msg_ref, type: string }
        - { name: msg_type, type: string }
        - { name: e01, type: string }
        - { name: e02, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: edifact
      path: out.edi
      include_unmapped: true
      options:
        interchange_from_doc: unb
        segment_newline: false
"#;
    let (report, output) =
        run(yaml, "interchange", input, "orders.edi", "out").expect("run from-doc round-trip");
    assert_eq!(report.counters.dlq_count, 0);
    // Empty middle element preserved; control reference echoed by UNZ.
    assert!(
        output.starts_with("UNB+UNOA:1+SENDER+RECEIVER++REF9'"),
        "UNB reconstruction truncated the empty element: {output}"
    );
    assert!(output.contains("UNZ+1+REF9'"), "output was: {output}");
    // The apostrophe is re-escaped on output, not emitted raw.
    assert!(
        output.contains("NAD+BY+O?'BRIEN'"),
        "apostrophe not release-escaped on output: {output}"
    );
}

#[test]
fn edifact_invoic_multi_message_round_trips() {
    // A representative two-message INVOIC interchange with a UNA header,
    // composite elements, and repeated line-item groups must re-parse to
    // the same body segment tags in order, and the writer must recompute
    // both UNT message counts and the UNZ message count.
    let yaml = r#"
pipeline:
  name: edifact_invoic
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: edifact
      glob: ./*.edi
      envelope:
        sections:
          unb:
            extract: { segment: "UNB" }
            fields:
              e05: string
      schema:
        - { name: seg_id, type: string }
        - { name: msg_ref, type: string }
        - { name: msg_type, type: string }
        - { name: e01, type: string }
        - { name: e02, type: string }
        - { name: e03, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: edifact
      path: out.edi
      include_unmapped: true
      options:
        interchange_from_doc: unb
        segment_newline: false
"#;
    let (report, output) =
        run(yaml, "interchange", INVOIC, "invoice.edi", "out").expect("run INVOIC round-trip");
    assert_eq!(report.counters.dlq_count, 0);
    // Two messages; UNZ echoes the original interchange control reference.
    assert!(
        output.contains("UNZ+2+IC0007'"),
        "INVOIC UNZ control echo wrong: {output}"
    );
    // Recomputed per-message UNT counts (UNH + body + UNT). The first
    // message has 13 body segments, the second 8.
    assert!(
        output.contains("UNT+15+INV1'"),
        "first UNT recompute wrong: {output}"
    );
    assert!(
        output.contains("UNT+10+INV2'"),
        "second UNT recompute wrong: {output}"
    );

    // Re-parse the written interchange and confirm the body segment tags
    // round-trip in order across both messages.
    let reparse_yaml = r#"
pipeline:
  name: edifact_invoic_reparse
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: edifact
      glob: ./*.edi
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
        run(reparse_yaml, "interchange", &output, "echo.edi", "out")
            .expect("re-parse INVOIC round-trip");
    assert_eq!(reparse_report.counters.dlq_count, 0);
    let tags: Vec<&str> = reparse_csv.lines().map(str::trim).collect();
    assert_eq!(
        tags,
        vec![
            "UNH", "BGM", "DTM", "NAD", "NAD", "LIN", "PIA", "QTY", "MOA", "LIN", "PIA", "QTY",
            "MOA", "MOA", "UNH", "BGM", "DTM", "NAD", "NAD", "LIN", "QTY", "MOA", "MOA",
        ],
        "INVOIC body tags did not round-trip; edifact output was: {output}"
    );
}

#[test]
fn edifact_output_with_split_is_rejected_at_config_time() {
    // An interchange is one envelope; byte-limit splitting would finalize
    // it mid-stream. The combination must fail config validation, not run
    // and emit a corrupt interchange.
    let yaml = r#"
pipeline:
  name: edifact_split
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: edifact
      glob: ./*.edi
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: interchange
    config:
      name: out
      type: edifact
      path: out.edi
      options:
        interchange: ["UNOA:1", "S", "R", "240101:1200", "REF1"]
      split:
        max_bytes: 1024
"#;
    let err = parse_config(yaml).expect_err("edifact + split must fail config validation");
    let msg = format!("{err:?}");
    assert!(msg.contains("E323"), "expected E323 diagnostic, got: {msg}");
}

#[test]
fn edifact_unt_count_mismatch_fails_the_run() {
    // The UNT trailer claims 9 segments where the message has 4 — a
    // truncation/corruption signal the reader surfaces as a run failure.
    let bad = "UNB+UNOA:1+S+R+240101:1200+REF1'\
        UNH+M1+ORDERS:D:96A:UN'BGM+220'UNT+9+M1'UNZ+1+REF1'";
    let yaml = r#"
pipeline:
  name: edifact_bad_count
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: edifact
      glob: ./*.edi
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
    let result = run(yaml, "interchange", bad, "bad.edi", "out");
    let err = result.expect_err("count mismatch must fail the run");
    assert!(
        err.contains("UNT segment count mismatch") || err.contains("EDIFACT"),
        "error should name the EDIFACT count failure: {err}"
    );
}

#[test]
fn edifact_generated_schema_runs_end_to_end() {
    // `schema: { generated: {} }` lets the engine synthesize the EDIFACT
    // positional schema (`seg_id`/`msg_ref`/`msg_type`/`e01…`) from the
    // format's `max_elements` — the author declares no columns. The synthesized
    // columns are typed `string` at compile time (see `bind_schema_test`), so
    // the Transform below references `seg_id`/`e01` as concrete columns and the
    // pipeline runs to a CSV sink.
    let yaml = r#"
pipeline:
  name: edifact_generated
nodes:
  - type: source
    name: interchange
    config:
      name: interchange
      type: edifact
      glob: ./*.edi
      schema: { generated: {} }
  - type: transform
    name: project
    input: interchange
    config:
      cxl: |
        emit seg = seg_id
        emit first = e01
  - type: output
    name: out
    input: project
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let (report, output) = run(yaml, "interchange", ORDERS, "orders.edi", "out")
        .expect("run generated-schema edifact pipeline");
    // Three body segments: UNH, BGM, NAD (service segments not emitted).
    assert_eq!(report.counters.total_count, 3, "output was: {output}");
    assert_eq!(report.counters.dlq_count, 0);
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 4, "header + 3 rows; got: {output}");
    // seg_id + e01 resolved per body segment: UNH/M1, BGM/220, NAD/BY.
    assert!(output.contains("UNH,M1"), "UNH row seg_id/e01: {output}");
    assert!(output.contains("BGM,220"), "BGM row seg_id/e01: {output}");
    assert!(output.contains("NAD,BY"), "NAD row seg_id/e01: {output}");
}
