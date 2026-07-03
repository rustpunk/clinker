//! End-to-end seam for per-document output envelope reconstruction.
//!
//! With `reconstruct_envelope: true` on the Output, the executor routes that
//! Output through a dedicated dispatch arm that detects document boundaries
//! from each record's `doc_ctx().source_file()` and fires the writer's
//! `begin_document` / `end_document` around each document's records. The
//! Output is also excluded from the fused streaming-writer thread, which does
//! no framing.
//!
//! No writer renders an envelope yet — `begin_document` / `end_document`
//! default to no-ops — so this seam landing must leave the bytes identical to
//! the flag-off path. These tests run the same pipeline with the flag off and
//! on and assert byte-equal output (and equal counters), proving the new arm
//! streams every body record through unchanged across document boundaries —
//! including across multiple documents through an intermediate Transform.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

fn pipeline_yaml(reconstruct_envelope: bool) -> String {
    let flag = if reconstruct_envelope {
        "\n      reconstruct_envelope: true"
    } else {
        ""
    };
    format!(
        r#"
pipeline:
  name: envelope_seam
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: json
      glob: ./*.json
      options:
        record_path: records
      envelope:
        sections:
          BatchInfo:
            extract: {{ json_pointer: "/BatchInfo" }}
            fields:
              batch_id: string
      schema:
        - {{ name: amount, type: int }}
  - type: transform
    name: tag
    input: payments
    config:
      cxl: |
        emit amount = amount
        emit batch = $doc.BatchInfo.batch_id
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv{flag}
"#
    )
}

const DOC_JSON: &str = r#"{
  "BatchInfo": { "batch_id": "RUN-001" },
  "records": [
    { "amount": 10 },
    { "amount": 20 },
    { "amount": 30 }
  ]
}"#;

/// Run the pipeline and return the written bytes plus the run counters.
fn run(reconstruct_envelope: bool) -> (String, clinker_record::PipelineCounters) {
    let yaml = pipeline_yaml(reconstruct_envelope);
    let config = parse_config(&yaml).expect("parse envelope pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile envelope pipeline");

    let file = FileSlot::new(
        PathBuf::from("payments.json"),
        Box::new(Cursor::new(DOC_JSON.as_bytes().to_vec())),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file]),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
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
        .expect("run envelope pipeline");
    assert_eq!(report.counters.total_count, 3, "three body records");
    assert_eq!(report.counters.dlq_count, 0);
    (buf.as_string(), report.counters)
}

#[test]
fn reconstruct_envelope_flag_streams_records_byte_identically() {
    let (baseline, baseline_counters) = run(false);
    let (with_envelope, envelope_counters) = run(true);

    // Sanity: the baseline carried the body rows the envelope arm must also
    // carry.
    let lines: Vec<&str> = baseline.lines().collect();
    assert_eq!(lines.len(), 4, "header + 3 data rows; got: {baseline}");
    for row in &lines[1..] {
        assert!(row.contains("RUN-001"), "missing $doc value: {row}");
    }

    // The seam's no-op `begin_document` / `end_document` render nothing, so
    // routing through the envelope arm leaves the output byte-for-byte equal
    // to the records-only path.
    assert_eq!(
        with_envelope, baseline,
        "envelope-reconstruction arm must stream records byte-identically while the hooks are no-ops",
    );
    // Counters must also be invariant under the flag — not just the bytes.
    assert_eq!(
        envelope_counters.records_written, baseline_counters.records_written,
        "records_written must match flag-on vs flag-off",
    );
    assert_eq!(
        envelope_counters.ok_count, baseline_counters.ok_count,
        "ok_count must match flag-on vs flag-off",
    );
    // Three records reached the Output and all three counted.
    assert_eq!(baseline_counters.records_written, 3);
    assert_eq!(baseline_counters.ok_count, 3);
}

const MULTI_DOC_YAML: &str = r#"
pipeline:
  name: envelope_multi
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: json
      glob: ./*.json
      options:
        record_path: records
      envelope:
        sections:
          BatchInfo:
            extract: { json_pointer: "/BatchInfo" }
            fields:
              batch_id: string
      schema:
        - { name: amount, type: int }
  - type: transform
    name: tag
    input: payments
    config:
      cxl: |
        emit amount = amount
        emit batch = $doc.BatchInfo.batch_id
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
"#;

fn doc_json(batch: &str, amounts: &[i64]) -> String {
    let recs: Vec<String> = amounts
        .iter()
        .map(|a| format!("{{ \"amount\": {a} }}"))
        .collect();
    format!(
        "{{ \"BatchInfo\": {{ \"batch_id\": \"{batch}\" }}, \"records\": [ {} ] }}",
        recs.join(", ")
    )
}

/// Run the multi-FILE pipeline (two documents, distinct source files) through
/// an intermediate Transform, returning bytes + counters. `reconstruct` is
/// appended to the Output config.
fn run_multi_doc(reconstruct: bool) -> (String, clinker_record::PipelineCounters) {
    let yaml = if reconstruct {
        MULTI_DOC_YAML.replace(
            "      path: out.csv\n",
            "      path: out.csv\n      reconstruct_envelope: true\n",
        )
    } else {
        MULTI_DOC_YAML.to_string()
    };
    let config = parse_config(&yaml).expect("parse multi-doc pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile multi-doc pipeline");

    // Two files with DISTINCT paths → two documents with distinct
    // `source_file` Arcs, so the envelope arm sees a real document boundary
    // between them. A multi-file source reads them sequentially, so file A's
    // records all precede file B's.
    let file_a = FileSlot::new(
        PathBuf::from("batch_a.json"),
        Box::new(Cursor::new(doc_json("RUN-A", &[10, 20]).into_bytes())),
    );
    let file_b = FileSlot::new(
        PathBuf::from("batch_b.json"),
        Box::new(Cursor::new(doc_json("RUN-B", &[30, 40, 50]).into_bytes())),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file_a, file_b]),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
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
        .expect("run multi-doc pipeline");
    (buf.as_string(), report.counters)
}

#[test]
fn multi_document_through_transform_streams_byte_identically() {
    // The case the punctuation-driven design broke: two documents, an
    // intermediate Transform, and an envelope Output. The records-only arm
    // and the envelope arm must produce identical bytes — proving the
    // record-driven boundary detection streams every record of BOTH documents
    // across the document boundary (the Transform preserves each record's
    // `doc_ctx`, so the boundary survives to the Output).
    let (baseline, baseline_counters) = run_multi_doc(false);
    let (with_envelope, envelope_counters) = run_multi_doc(true);

    // Sanity: both documents' rows are present, each tagged with its own
    // batch id read from that file's envelope.
    assert!(baseline.contains("RUN-A"), "missing doc A rows: {baseline}");
    assert!(baseline.contains("RUN-B"), "missing doc B rows: {baseline}");
    let data_rows = baseline.lines().count() - 1; // minus header
    assert_eq!(
        data_rows, 5,
        "2 + 3 records across both documents: {baseline}"
    );

    assert_eq!(
        with_envelope, baseline,
        "envelope arm must stream both documents' records byte-identically through the Transform",
    );
    assert_eq!(
        envelope_counters.records_written, baseline_counters.records_written,
        "records_written invariant across the two documents",
    );
    assert_eq!(envelope_counters.ok_count, baseline_counters.ok_count);
    assert_eq!(baseline_counters.records_written, 5);
}

/// Parse a pipeline with `reconstruct_envelope` plus one incompatible feature
/// and return the config-validation error text, or panic if it validated.
/// `validate_config` runs inside `parse_config`, so the E347 rejections
/// surface here rather than at `compile`.
fn expect_validation_error(yaml: &str) -> String {
    match parse_config(yaml) {
        Ok(_) => panic!("expected a validation error, but the config validated"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn reconstruct_envelope_with_split_is_rejected() {
    let yaml = r#"
pipeline:
  name: env_split
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      glob: ./*.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
      split:
        max_records: 100
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E347"), "expected E347, got: {err}");
    assert!(err.contains("split"), "message should mention split: {err}");
}

#[test]
fn reconstruct_envelope_with_document_dlq_is_rejected() {
    let yaml = r#"
pipeline:
  name: env_dlq
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: x12
      glob: ./*.edi
      dlq_granularity: document
      schema:
        - { name: seg_id, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: x12
      path: out.edi
      reconstruct_envelope: true
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E347"), "expected E347, got: {err}");
    assert!(
        err.contains("dlq_granularity"),
        "message should mention document-DLQ: {err}"
    );
}

#[test]
fn reconstruct_envelope_with_correlation_key_is_rejected() {
    let yaml = r#"
pipeline:
  name: env_corr
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      glob: ./*.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E347"), "expected E347, got: {err}");
    assert!(
        err.contains("correlation_key"),
        "message should mention correlation_key: {err}"
    );
}

#[test]
fn envelope_unknown_header_section_is_rejected_e346() {
    // The source declares a `BatchInfo` envelope section, but the CSV output's
    // `header_from_doc` names `Headr` (a typo) — E346 catches it at plan time.
    let yaml = r#"
pipeline:
  name: env_unknown_section
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: json
      glob: ./*.json
      options:
        record_path: records
      envelope:
        sections:
          BatchInfo:
            extract: { json_pointer: "/BatchInfo" }
            fields:
              batch_id: string
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: payments
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: Headr
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E346"), "expected E346, got: {err}");
    assert!(
        err.contains("Headr"),
        "message should name the bad section: {err}"
    );
    assert!(
        err.contains("BatchInfo"),
        "message should list the declared sections: {err}"
    );
}

#[test]
fn envelope_computed_footer_on_fixed_width_is_rejected_e346() {
    // A computed footer record count is unsupported on fixed-width output —
    // its positional lines have no field to inject a count into. E346 rejects
    // it even though the referenced section is otherwise declared.
    let yaml = r#"
pipeline:
  name: env_fw_count
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: json
      glob: ./*.json
      options:
        record_path: records
      envelope:
        sections:
          Foot:
            extract: { json_pointer: "/Foot" }
            fields:
              checksum: string
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: payments
    config:
      name: out
      type: fixed_width
      path: out.txt
      reconstruct_envelope: true
      schema:
        - { name: amount, type: int, width: 10 }
      options:
        envelope:
          footer_from_doc: Foot
          footer_record_count_field: record_count
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E346"), "expected E346, got: {err}");
    assert!(
        err.contains("footer_record_count_field"),
        "message should name the unsupported option: {err}"
    );
    assert!(
        err.contains("fixed_width") || err.contains("fixed-width"),
        "message should name the format: {err}"
    );
}

#[test]
fn envelope_known_sections_validate_clean() {
    // The control: header + footer naming declared sections, and a computed
    // footer count on CSV (a format that supports it), validate without E346.
    let yaml = r#"
pipeline:
  name: env_ok
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: json
      glob: ./*.json
      options:
        record_path: records
      envelope:
        sections:
          Head:
            extract: { json_pointer: "/Head" }
            fields:
              batch_id: string
          Foot:
            extract: { json_pointer: "/Foot" }
            fields:
              checksum: string
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: payments
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: Head
          footer_from_doc: Foot
          footer_record_count_field: record_count
"#;
    parse_config(yaml).expect("a well-formed envelope config validates clean");
}

/// A document with a Head + Foot section and a configurable batch id, for the
/// end-to-end CSV envelope framing test.
fn doc_with_envelope(batch: &str, amounts: &[i64]) -> String {
    let recs: Vec<String> = amounts
        .iter()
        .map(|a| format!("{{ \"amount\": {a} }}"))
        .collect();
    format!(
        "{{ \"Head\": {{ \"batch_id\": \"{batch}\" }}, \
            \"Foot\": {{ \"checksum\": \"SUM-{batch}\" }}, \
            \"records\": [ {} ] }}",
        recs.join(", ")
    )
}

/// End-to-end CSV envelope reconstruction over two files (two documents),
/// asserting per-document header + body + footer framing with a streaming
/// record count. Proves the writer hooks render the envelope, the grain frames
/// per file, and the count resets per document.
#[test]
fn csv_envelope_reconstructs_header_body_footer_per_document() {
    let yaml = r#"
pipeline:
  name: csv_envelope_e2e
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: json
      glob: ./*.json
      options:
        record_path: records
      envelope:
        sections:
          Head:
            extract: { json_pointer: "/Head" }
            fields:
              batch_id: string
          Foot:
            extract: { json_pointer: "/Foot" }
            fields:
              checksum: string
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: payments
    config:
      name: out
      type: csv
      path: out.csv
      include_header: false
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: Head
          footer_from_doc: Foot
          footer_record_count_field: record_count
"#;
    let config = parse_config(yaml).expect("parse csv envelope pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile csv envelope pipeline");

    let file_a = FileSlot::new(
        PathBuf::from("batch_a.json"),
        Box::new(Cursor::new(doc_with_envelope("A", &[10, 20]).into_bytes())),
    );
    let file_b = FileSlot::new(
        PathBuf::from("batch_b.json"),
        Box::new(Cursor::new(
            doc_with_envelope("B", &[30, 40, 50]).into_bytes(),
        )),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file_a, file_b]),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
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
        .expect("run csv envelope pipeline");
    assert_eq!(report.counters.records_written, 5, "2 + 3 body records");

    // Each file frames as its own document: header row (batch id), body rows,
    // footer row (checksum + streaming record count for that document).
    let out = buf.as_string();
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(
        lines,
        vec![
            "A",       // doc A header (Head.batch_id)
            "10",      // body
            "20",      // body
            "SUM-A,2", // doc A footer (Foot.checksum + record_count=2)
            "B",       // doc B header
            "30",      // body
            "40",      // body
            "50",      // body
            "SUM-B,3", // doc B footer (record_count=3, reset per document)
        ],
        "got:\n{out}"
    );
}

#[test]
fn reconstruct_envelope_downstream_of_combine_is_rejected_e347() {
    // A cross-document Combine emits `<merged>`-lineage rows with no
    // originating document. Routing those into an enveloped (JSON) Output
    // would produce body bytes outside any open document object — malformed
    // JSON. The plan-time guard rejects the shape before it can run.
    let yaml = r#"
pipeline:
  name: env_combine
nodes:
  - type: source
    name: left
    config:
      name: left
      type: json
      glob: ./left/*.json
      options:
        record_path: records
      schema:
        - { name: id, type: int }
  - type: source
    name: right
    config:
      name: right
      type: json
      glob: ./right/*.json
      options:
        record_path: records
      schema:
        - { name: id, type: int }
  - type: combine
    name: joined
    input:
      a: left
      b: right
    config:
      where: "a.id == b.id"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit id = a.id
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: json
      path: out.json
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: Head
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E347"), "expected E347, got: {err}");
    assert!(
        err.contains("strips document lineage") && err.contains("joined"),
        "message should name the lineage-stripping Combine: {err}"
    );
}

#[test]
fn reconstruct_envelope_downstream_of_aggregate_is_rejected_e347() {
    // An Aggregate finalizes group rows through the merged eval context, so
    // every emitted row is synthetic-grained — same hazard as Combine.
    let yaml = r#"
pipeline:
  name: env_agg
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: json
      glob: ./*.json
      options:
        record_path: records
      schema:
        - { name: amount, type: int }
  - type: aggregate
    name: totals
    input: payments
    config:
      group_by: []
      cxl: |
        emit total = sum(amount)
  - type: output
    name: out
    input: totals
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: Head
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E347"), "expected E347, got: {err}");
    assert!(
        err.contains("strips document lineage"),
        "message should explain the lineage stripping: {err}"
    );
}

#[test]
fn envelope_section_on_non_feeding_source_is_rejected_e346() {
    // Two sources, two independent Output legs. `out_a` is fed only by
    // `src_a` (which declares `HeadA`), but its envelope names `HeadB` —
    // declared only on the NON-feeding `src_b`. Scoping the check to the
    // feeding source(s) catches this; a pipeline-wide union would not.
    let yaml = r#"
pipeline:
  name: env_multi_source
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: json
      glob: ./a/*.json
      options:
        record_path: records
      envelope:
        sections:
          HeadA:
            extract: { json_pointer: "/HeadA" }
            fields:
              id: string
      schema:
        - { name: amount, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: json
      glob: ./b/*.json
      options:
        record_path: records
      envelope:
        sections:
          HeadB:
            extract: { json_pointer: "/HeadB" }
            fields:
              id: string
      schema:
        - { name: amount, type: int }
  - type: output
    name: out_a
    input: src_a
    config:
      name: out_a
      type: csv
      path: out_a.csv
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: HeadB
  - type: output
    name: out_b
    input: src_b
    config:
      name: out_b
      type: csv
      path: out_b.csv
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E346"), "expected E346, got: {err}");
    assert!(
        err.contains("HeadB") && err.contains("out_a"),
        "message should name the unreachable section and the output: {err}"
    );
    // And the feeding source's own section is listed as the valid choice.
    assert!(
        err.contains("HeadA"),
        "should list the feeding source's sections: {err}"
    );
}

#[test]
fn footer_count_without_footer_section_is_rejected_e346() {
    let yaml = r#"
pipeline:
  name: env_count_only
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: json
      glob: ./*.json
      options:
        record_path: records
      envelope:
        sections:
          Head:
            extract: { json_pointer: "/Head" }
            fields:
              batch_id: string
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: payments
    config:
      name: out
      type: csv
      path: out.csv
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: Head
          footer_record_count_field: record_count
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E346"), "expected E346, got: {err}");
    assert!(
        err.contains("footer_record_count_field") && err.contains("footer_from_doc"),
        "message should explain the count requires a footer section: {err}"
    );
}

#[test]
fn csv_envelope_missing_section_in_a_document_writes_no_framing() {
    // Two files: file A's document carries the `Head` section; file B's does
    // not (its JSON has no `/Head`). The writer must emit a header row only
    // for A — B's missing section writes NOTHING (not a blank row).
    let yaml = r#"
pipeline:
  name: env_missing_section
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: json
      glob: ./*.json
      options:
        record_path: records
      envelope:
        sections:
          Head:
            extract: { json_pointer: "/Head" }
            fields:
              batch_id: string
      schema:
        - { name: amount, type: int }
  - type: output
    name: out
    input: payments
    config:
      name: out
      type: csv
      path: out.csv
      include_header: false
      reconstruct_envelope: true
      options:
        envelope:
          header_from_doc: Head
"#;
    let config = parse_config(yaml).expect("parse missing-section pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile missing-section pipeline");

    let file_a = FileSlot::new(
        PathBuf::from("a.json"),
        Box::new(Cursor::new(
            br#"{ "Head": { "batch_id": "A" }, "records": [ { "amount": 1 } ] }"#.to_vec(),
        )),
    );
    // File B has NO `Head` section — its document lacks the configured header.
    let file_b = FileSlot::new(
        PathBuf::from("b.json"),
        Box::new(Cursor::new(
            br#"{ "records": [ { "amount": 2 } ] }"#.to_vec(),
        )),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file_a, file_b]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
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
        .expect("run missing-section pipeline");
    assert_eq!(report.counters.records_written, 2);

    let out = buf.as_string();
    let lines: Vec<&str> = out.lines().collect();
    // A: header row "A" then body "1". B: NO header row, just body "2".
    assert_eq!(lines, vec!["A", "1", "2"], "got:\n{out}");
}

// ─────────────────────────────────────────────────────────────────────────
// #570: plan-time document-cardinality gate (E355) + E347 lineage relaxation
// ─────────────────────────────────────────────────────────────────────────

#[test]
fn single_document_output_from_two_input_merge_is_rejected_e355() {
    // A >=2-input Merge is a shape-provable multi-document body; a SWIFT output
    // frames one message, so it is rejected at plan time with a remedy rather
    // than silently merging the two documents into one envelope.
    let yaml = r#"
pipeline:
  name: e355_merge
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
  - type: merge
    name: both
    inputs: [a, b]
    config:
      mode: concat
  - type: output
    name: out
    input: both
    config:
      name: out
      type: swift
      path: out.swift
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E355"), "expected E355, got: {err}");
    assert!(
        err.contains("out") && err.contains("envelope"),
        "message should name the output and the envelope remedy: {err}"
    );
}

#[test]
fn single_document_output_from_explicit_paths_list_is_rejected_e355() {
    // An explicit `paths:` list of >= 2 files is shape-provably multi-document.
    let yaml = r#"
pipeline:
  name: e355_paths
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      paths: [a.csv, b.csv]
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: x12
      path: out.x12
"#;
    let err = expect_validation_error(yaml);
    assert!(err.contains("E355"), "expected E355, got: {err}");
}

#[test]
fn multi_document_output_from_two_input_merge_is_allowed() {
    // A JSON output frames a valid document sequence, so a multi-document body
    // is fine — the gate only applies to single-document formats.
    let yaml = r#"
pipeline:
  name: e355_json_ok
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
  - type: merge
    name: both
    inputs: [a, b]
    config:
      mode: concat
  - type: output
    name: out
    input: both
    config:
      name: out
      type: json
      path: out.json
"#;
    parse_config(yaml).expect("multi-document JSON output accepts a multi-document body");
}

#[test]
fn single_document_output_through_envelope_concat_is_allowed() {
    // An `envelope` node with `strategy: concat` consolidates the multi-document
    // body into one document, satisfying the single-document SWIFT output.
    let yaml = r#"
pipeline:
  name: e355_concat_ok
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
  - type: merge
    name: both
    inputs: [a, b]
    config:
      mode: concat
  - type: envelope
    name: one_doc
    body: both
    config:
      strategy: concat
  - type: output
    name: out
    input: one_doc
    config:
      name: out
      type: swift
      path: out.swift
"#;
    parse_config(yaml).expect("envelope concat consolidates the body for a single-document output");
}

#[test]
fn single_document_output_through_envelope_preserve_is_rejected_e355() {
    // `strategy: preserve` keeps per-document grains — it does NOT consolidate —
    // so the multi-document body still reaches the single-document output.
    let yaml = r#"
pipeline:
  name: e355_preserve
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
  - type: merge
    name: both
    inputs: [a, b]
    config:
      mode: concat
  - type: envelope
    name: passthrough
    body: both
    config:
      strategy: preserve
  - type: output
    name: out
    input: passthrough
    config:
      name: out
      type: swift
      path: out.swift
"#;
    let err = expect_validation_error(yaml);
    assert!(
        err.contains("E355"),
        "preserve does not consolidate; expected E355, got: {err}"
    );
}

#[test]
fn combine_then_envelope_concat_allows_reconstruct_envelope_e347_relaxed() {
    // The E347 lineage ban is relaxed: re-enveloping downstream of a Combine is
    // legal THROUGH an Envelope node that consolidates the merged stream. Same
    // shape as `reconstruct_envelope_downstream_of_combine_is_rejected_e347`
    // (which has no Envelope node) — inserting the Envelope node clears E347.
    let yaml = r#"
pipeline:
  name: e347_relaxed
nodes:
  - type: source
    name: left
    config:
      name: left
      type: json
      glob: ./left/*.json
      options:
        record_path: records
      schema:
        - { name: id, type: int }
  - type: source
    name: right
    config:
      name: right
      type: json
      glob: ./right/*.json
      options:
        record_path: records
      schema:
        - { name: id, type: int }
  - type: combine
    name: joined
    input:
      a: left
      b: right
    config:
      where: "a.id == b.id"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit id = a.id
  - type: envelope
    name: framed
    body: joined
    config:
      strategy: concat
  - type: output
    name: out
    input: framed
    config:
      name: out
      type: json
      path: out.json
      reconstruct_envelope: true
"#;
    parse_config(yaml)
        .expect("an envelope node between the Combine and the Output relaxes the E347 ban");
}

#[test]
fn reconstruct_envelope_downstream_of_combine_through_preserve_envelope_is_rejected_e347() {
    // A `preserve` Envelope does NOT consolidate — it passes the Combine's
    // synthetic `<merged>` grain through unchanged — so the reconstruct arm
    // would still stream those records unframed. The relaxation must NOT fire
    // for a preserve envelope; E347 still applies.
    let yaml = r#"
pipeline:
  name: e347_preserve
nodes:
  - type: source
    name: left
    config:
      name: left
      type: json
      glob: ./left/*.json
      options:
        record_path: records
      schema:
        - { name: id, type: int }
  - type: source
    name: right
    config:
      name: right
      type: json
      glob: ./right/*.json
      options:
        record_path: records
      schema:
        - { name: id, type: int }
  - type: combine
    name: joined
    input:
      a: left
      b: right
    config:
      where: "a.id == b.id"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit id = a.id
  - type: envelope
    name: framed
    body: joined
    config:
      strategy: preserve
  - type: output
    name: out
    input: framed
    config:
      name: out
      type: json
      path: out.json
      reconstruct_envelope: true
"#;
    let err = expect_validation_error(yaml);
    assert!(
        err.contains("E347"),
        "preserve does not consolidate; expected E347, got: {err}"
    );
    assert!(
        err.contains("joined"),
        "message should name the lineage stripper: {err}"
    );
}

#[test]
fn reconstruct_envelope_with_concat_envelope_on_sibling_branch_is_rejected_e347() {
    // A `concat` Envelope only consolidates ITS body branch. A stripper on a
    // sibling merge branch is still unconsolidated, so its `<merged>` records
    // reach the reconstruct-envelope Output unframed — E347 must still fire.
    let yaml = r#"
pipeline:
  name: e347_sibling
nodes:
  - type: source
    name: left
    config:
      name: left
      type: json
      glob: ./left/*.json
      options:
        record_path: records
      schema:
        - { name: id, type: int }
  - type: source
    name: right
    config:
      name: right
      type: json
      glob: ./right/*.json
      options:
        record_path: records
      schema:
        - { name: id, type: int }
  - type: source
    name: other
    config:
      name: other
      type: json
      glob: ./other/*.json
      options:
        record_path: records
      schema:
        - { name: id, type: int }
  - type: combine
    name: joined
    input:
      a: left
      b: right
    config:
      where: "a.id == b.id"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit id = a.id
  - type: envelope
    name: framed
    body: other
    config:
      strategy: concat
  - type: merge
    name: both
    inputs: [joined, framed]
    config:
      mode: concat
  - type: output
    name: out
    input: both
    config:
      name: out
      type: json
      path: out.json
      reconstruct_envelope: true
"#;
    let err = expect_validation_error(yaml);
    assert!(
        err.contains("E347"),
        "a sibling-branch concat envelope must not shield the Combine branch; expected E347, got: {err}"
    );
    assert!(
        err.contains("joined"),
        "message should name the unconsolidated stripper: {err}"
    );
}

#[test]
fn single_document_output_with_per_source_file_fanout_is_allowed() {
    // Per-file fan-out (`{source_file}` path template) routes each document to
    // its own output file, so each lands in its own valid single-document
    // envelope — E355 must not reject it.
    let yaml = r#"
pipeline:
  name: e355_fanout_ok
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      paths: [a.csv, b.csv]
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: x12
      path: "out_{source_file}.x12"
"#;
    parse_config(yaml)
        .expect("per-file fan-out gives each document its own single-document envelope");
}
