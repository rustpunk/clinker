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
