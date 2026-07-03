//! End-to-end HL7 v2 ingestion and round-trip.
//!
//! Covers: (1) an HL7 source surfaces the message-level envelope as a
//! `$doc` section on every body record, proving the nested-envelope nesting
//! (`OpenLevel`/`CloseLevel`) flows through the executor; (2) an HL7→HL7
//! pipeline re-emits the MSH/body segments (escaping field data) and
//! re-parses identically; (3) the `hl7`+`split` combination is rejected at
//! config time (diagnostic `E339`); (4) a batch/file envelope surfaces
//! file-level `$doc` fields and a BTS count mismatch fails the run;
//! (5) opt-in composite-field splitting exposes component columns to CXL and
//! re-assembles them byte-identically on an HL7→HL7 round-trip.

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

#[test]
fn hl7_split_fields_expose_component_columns_to_cxl() {
    // Splitting MSH-9 (f08) into two components lets a Transform read the
    // message code and trigger event without CXL string-splitting. PID-3
    // (f03) splits into four components so the patient id and assigning
    // authority surface separately.
    let yaml = r#"
pipeline:
  name: hl7_split_read
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      options:
        split_fields:
          - { field: f08, components: 2 }
          - { field: f03, components: 5 }
      schema:
        - { name: seg_id, type: string }
        - { name: f08_c1, type: string }
        - { name: f08_c2, type: string }
        - { name: f03_c1, type: string }
  - type: transform
    name: tag
    input: messages
    config:
      cxl: |
        emit seg = seg_id
        emit code = f08_c1
        emit trigger = f08_c2
        emit patid = f03_c1
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
        run(yaml, "messages", &adt_a01(), "adt.hl7", "out").expect("run split read");
    assert_eq!(report.counters.dlq_count, 0);
    // The MSH row resolves the message code/trigger from the split f08.
    let msh_row = output
        .lines()
        .find(|l| l.starts_with("MSH,"))
        .expect("MSH row present");
    assert!(msh_row.contains("ADT"), "MSH-9.1 message code: {msh_row}");
    assert!(msh_row.contains("A01"), "MSH-9.2 trigger event: {msh_row}");
    // The PID row resolves the patient id from the split f03.
    let pid_row = output
        .lines()
        .find(|l| l.starts_with("PID,"))
        .expect("PID row present");
    assert!(
        pid_row.contains("PATID123"),
        "PID-3.1 patient id: {pid_row}"
    );
}

#[test]
fn hl7_split_field_round_trips_byte_identically() {
    // An HL7→HL7 pipeline that splits PID-3 (f03) into four components must
    // re-assemble the exact composite on output — the `^` separators are
    // reinserted verbatim, never escaped — so the re-emitted PID-3 is
    // byte-identical to the source and a re-parse reproduces it.
    let yaml = r#"
pipeline:
  name: hl7_split_round_trip
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      options:
        split_fields:
          - { field: f03, components: 5 }
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
        run(yaml, "messages", &adt_a01(), "adt.hl7", "out").expect("run split round-trip");
    assert_eq!(report.counters.dlq_count, 0);

    // PID-3 (`PATID123^^^HOSP^MR`) re-assembles byte-identically from its
    // four split component columns; no component separator became an `\S\`.
    assert!(
        output.contains("PID|1||PATID123^^^HOSP^MR"),
        "split PID-3 not byte-identical on round-trip: {output}"
    );
    assert!(
        !output.contains("\\S\\"),
        "component separator wrongly escaped on output: {output}"
    );

    // Re-parse the output (no split) to prove the wire field is intact.
    let reparse_yaml = r#"
pipeline:
  name: hl7_split_reparse
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      schema:
        - { name: seg_id, type: string }
        - { name: f03, type: string }
  - type: transform
    name: tag
    input: messages
    config:
      cxl: |
        emit seg = seg_id
        emit f3 = f03
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
      include_header: false
"#;
    let (reparse_report, reparse_csv) =
        run(reparse_yaml, "messages", &output, "echo.hl7", "out").expect("re-parse round-trip");
    assert_eq!(reparse_report.counters.dlq_count, 0);
    // The PID row carries the re-assembled composite verbatim.
    let pid_row = reparse_csv
        .lines()
        .find(|l| l.starts_with("PID,"))
        .expect("PID row present");
    assert!(
        pid_row.contains("PATID123^^^HOSP^MR"),
        "re-parsed PID-3 wrong: {pid_row}"
    );
}

#[test]
fn hl7_split_field_past_max_fields_is_rejected_at_config_time() {
    // A split naming a position past `max_fields` is unreachable; reject it
    // at config validation rather than silently dropping the column.
    let yaml = r#"
pipeline:
  name: hl7_split_overflow
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      options:
        max_fields: 4
        split_fields:
          - { field: f08, components: 2 }
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
    let err = parse_config(yaml).expect_err("split past max_fields must fail config validation");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("max_fields") && msg.contains("f08"),
        "expected a max_fields overflow naming f08, got: {msg}"
    );
}

#[test]
fn hl7_generated_schema_with_split_runs_end_to_end() {
    // `schema: { generated: {} }` synthesizes the HL7 positional schema from
    // `max_fields`, applying the declared `split_fields` so the composite MSH-9
    // (`f08` = `ADT^A01`) is exposed as its component leaves `f08_c1`/`f08_c2`.
    // The author declares no columns; the Transform references the synthesized
    // split leaves directly.
    let yaml = r#"
pipeline:
  name: hl7_generated
nodes:
  - type: source
    name: messages
    config:
      name: messages
      type: hl7
      glob: ./*.hl7
      options:
        split_fields:
          - { field: f08, components: 2 }
      schema: { generated: {} }
  - type: transform
    name: project
    input: messages
    config:
      cxl: |
        emit seg = seg_id
        emit mtype = f08_c1
        emit mtrigger = f08_c2
  - type: output
    name: out
    input: project
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let (report, output) =
        run(yaml, "messages", &adt_a01(), "adt.hl7", "out").expect("run generated hl7 pipeline");
    // Four body segments: MSH, EVN, PID, PV1.
    assert_eq!(report.counters.total_count, 4, "output was: {output}");
    assert_eq!(report.counters.dlq_count, 0);
    // MSH-9 (`ADT^A01`) split into component leaves on the MSH row.
    assert!(
        output.contains("MSH,ADT,A01"),
        "MSH row must expose split MSH-9 leaves ADT/A01: {output}"
    );
}
