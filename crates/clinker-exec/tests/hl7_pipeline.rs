//! End-to-end HL7 v2 ingestion and round-trip.
//!
//! Covers: (1) an HL7 source surfaces the message-level envelope as a
//! `$doc` section on every body record, proving the nested-envelope nesting
//! (`OpenLevel`/`CloseLevel`) flows through the executor; (2) an HL7→HL7
//! pipeline re-emits the MSH/body segments (escaping field data) and
//! re-parses identically; (3) the `hl7`+`split` combination is rejected at
//! config time (diagnostic `E339`); (4) a batch/file envelope surfaces
//! file-level `$doc` fields and a BTS count mismatch fails the run.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

/// A representative ADT^A01 (admit) message: MSH header, EVN event, PID
/// patient, PV1 visit. Segments are CR-separated; the MSH declares the
/// conventional `|^~\&` delimiters. The message control id is `ADT0001`.
fn adt_a01() -> String {
    [
        "MSH|^~\\&|ADMIT|HOSP|REG|HOSP|20240101120000||ADT^A01|ADT0001|P|2.5",
        "EVN|A01|20240101120000",
        "PID|1||PATID123^^^HOSP^MR||DOE^JOHN^Q||19700101|M",
        "PV1|1|I|WARD^101^A",
    ]
    .join("\r")
}

/// A representative ORU^R01 (observation result) message: MSH header, PID
/// patient, OBR order, two OBX results. The message control id is `ORU0001`.
fn oru_r01() -> String {
    [
        "MSH|^~\\&|LAB|HOSP|EHR|HOSP|20240102093000||ORU^R01|ORU0001|P|2.5",
        "PID|1||PATID123^^^HOSP^MR||DOE^JOHN",
        "OBR|1||LAB42|CBC^Complete Blood Count",
        "OBX|1|NM|WBC^White Blood Cell||7.2|10*9/L|4.0-11.0|N",
        "OBX|2|NM|HGB^Hemoglobin||14.1|g/dL|13.5-17.5|N",
    ]
    .join("\r")
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
    Ok((report, buf.as_string()))
}

#[test]
fn hl7_source_surfaces_message_envelope_and_streams_segments() {
    // Every body record carries the message-level document, whose
    // `transaction_set` section exposes the MSH fields. A Transform reads
    // the segment tag, the stamped control id (`set_ref`), and MSH-9
    // (message type) via the `$doc.transaction_set.f08` positional field.
    let yaml = r#"
pipeline:
  name: hl7_envelope
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      schema:
        - { name: seg_id, type: string }
        - { name: set_ref, type: string }
        - { name: f01, type: string }
  - type: transform
    name: tag
    input: messages
    config:
      cxl: |
        emit seg = seg_id
        emit ctrl = set_ref
        emit mtype = $doc.transaction_set.f08
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let (report, output) =
        run(yaml, "messages", &adt_a01(), "adt.hl7", "out").expect("run hl7 pipeline");
    // MSH, EVN, PID, PV1 are all body records.
    assert_eq!(report.counters.total_count, 4, "output was: {output}");
    assert_eq!(report.counters.dlq_count, 0);

    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 5, "header + 4 rows; got: {output}");
    // Every body row carries the message control id and the message type
    // resolved from the message-level $doc section.
    for row in &lines[1..] {
        assert!(row.contains("ADT0001"), "row missing set_ref: {row}");
        assert!(
            row.contains("ADT^A01"),
            "row missing $doc.transaction_set.f08 (MSH-9): {row}"
        );
    }
    // First body record is the MSH segment.
    assert!(lines[1].starts_with("MSH,"), "first row: {}", lines[1]);
}

#[test]
fn hl7_round_trip_reparses_identically() {
    // Source parses HL7 and writes HL7, re-emitting the MSH and body
    // segments. Re-parsing the output must yield the same segment tags in
    // order, and an escaped field must survive the round-trip.
    let yaml = r#"
pipeline:
  name: hl7_round_trip
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: messages
    config:
      name: out
      type: hl7
      path: out.hl7
      include_unmapped: true
      options:
        segment_newline: false
"#;
    let (report, output) =
        run(yaml, "messages", &oru_r01(), "oru.hl7", "out").expect("run round-trip");
    assert_eq!(report.counters.dlq_count, 0);

    // The re-emitted file opens with the original MSH and carries the two
    // OBX result segments.
    assert!(
        output.starts_with("MSH|^~\\&|LAB|"),
        "MSH echo wrong: {output}"
    );
    assert!(output.contains("OBX|1|NM|"), "OBX1 missing: {output}");
    assert!(output.contains("OBX|2|NM|"), "OBX2 missing: {output}");

    // Composite fields must survive byte-identically — their component '^'
    // separators are part of the field structure and are NOT escaped on
    // write. The PID-3 CX field and the OBX-3 CE field round-trip verbatim.
    assert!(
        output.contains("PID|1||PATID123^^^HOSP^MR"),
        "composite PID-3 not byte-identical: {output}"
    );
    assert!(
        output.contains("OBX|1|NM|WBC^White Blood Cell"),
        "composite OBX-3 not byte-identical: {output}"
    );
    // No component separator may have been turned into an \S\ escape.
    assert!(
        !output.contains("\\S\\"),
        "component separator wrongly escaped on output: {output}"
    );

    // Feed the HL7 output back through a second pipeline (HL7 → CSV) to
    // prove it re-parses identically: the segment tags must round-trip.
    let reparse_yaml = r#"
pipeline:
  name: hl7_reparse
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      schema:
        - { name: seg_id, type: string }
  - type: transform
    name: tag
    input: messages
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
        run(reparse_yaml, "messages", &output, "echo.hl7", "out").expect("re-parse round-trip");
    assert_eq!(reparse_report.counters.dlq_count, 0);
    let tags: Vec<&str> = reparse_csv.lines().map(str::trim).collect();
    assert_eq!(
        tags,
        vec!["MSH", "PID", "OBR", "OBX", "OBX"],
        "round-trip segment tags; hl7 output was: {output}"
    );
}

#[test]
fn hl7_batch_envelope_surfaces_file_doc_and_validates_counts() {
    // An FHS..FTS file wrapping a BHS..BTS batch of two messages. The FHS
    // file header is extractable as a declared file-level `$doc` section;
    // every body record resolves a field from it. The BTS/FTS counts match,
    // so the run succeeds.
    let msg2 = oru_r01().replace("ORU0001", "ORU0002");
    let batched = format!(
        "FHS|^~\\&|LAB|HOSP|EHR|HOSP|20240102|FILE7\r\
         BHS|^~\\&|LAB|HOSP|EHR|HOSP|20240102|BATCH3\r\
         {}\r{}\r\
         BTS|2\r\
         FTS|1",
        adt_a01(),
        msg2
    );
    let yaml = r#"
pipeline:
  name: hl7_batch
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      envelope:
        sections:
          file:
            extract: { segment: "FHS" }
            fields:
              f07: string
      schema:
        - { name: seg_id, type: string }
        - { name: set_ref, type: string }
  - type: transform
    name: tag
    input: messages
    config:
      cxl: |
        emit seg = seg_id
        emit ctrl = set_ref
        emit file_id = $doc.file.f07
        emit batch_id = $doc.batch.f07
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
        run(yaml, "messages", &batched, "batch.hl7", "out").expect("run batch envelope");
    assert_eq!(report.counters.dlq_count, 0);
    // ADT (MSH, EVN, PID, PV1 = 4) + ORU (MSH, PID, OBR, OBX, OBX = 5) = 9.
    assert_eq!(report.counters.total_count, 9, "output was: {output}");
    // Every row resolves the FHS file id (file-level $doc) and the BHS batch
    // id (the nested batch-level $doc section, keyed positionally).
    for row in output.lines() {
        assert!(row.contains("FILE7"), "row missing $doc.file.f07: {row}");
        assert!(row.contains("BATCH3"), "row missing $doc.batch.f07: {row}");
    }
}

#[test]
fn hl7_output_with_split_is_rejected_at_config_time() {
    // A batch/file envelope is one FHS..FTS structure; byte-limit splitting
    // would finalize it mid-stream. The combination must fail config
    // validation, not run and emit a corrupt file.
    let yaml = r#"
pipeline:
  name: hl7_split
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: messages
    config:
      name: out
      type: hl7
      path: out.hl7
      options:
        file_header: ["^~\\&", "LAB", "HOSP"]
      split:
        max_bytes: 1024
"#;
    let err = parse_config(yaml).expect_err("hl7 + split must fail config validation");
    let msg = format!("{err:?}");
    assert!(msg.contains("E339"), "expected E339 diagnostic, got: {msg}");
}

#[test]
fn hl7_bts_count_mismatch_fails_the_run() {
    // The BTS trailer claims 9 messages where the batch has 1 — a
    // truncation/corruption signal the reader surfaces as a run failure.
    let bad = format!("BHS|^~\\&|LAB\r{}\rBTS|9", adt_a01());
    let yaml = r#"
pipeline:
  name: hl7_bad_count
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: messages
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let result = run(yaml, "messages", &bad, "bad.hl7", "out");
    let err = result.expect_err("count mismatch must fail the run");
    assert!(
        err.contains("BTS batch message count mismatch") || err.contains("HL7"),
        "error should name the HL7 count failure: {err}"
    );
}
