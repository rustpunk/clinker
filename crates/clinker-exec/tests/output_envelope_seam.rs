//! End-to-end seam for per-document output envelope reconstruction.
//!
//! A JSON source declares envelope sections, so its ingest emits a
//! `DocumentOpen` before the body and a `DocumentClose` after it. With
//! `reconstruct_envelope: true` on the Output, the executor routes that
//! Output through the punctuation-aware dispatch arm (which reads the
//! interleaved record/boundary stream and fires the writer's
//! `begin_document` / `end_document` at the outermost boundaries) instead of
//! the records-only fast path, and excludes it from the fused
//! streaming-writer thread so the materialized `DocumentClose` reaches the
//! writer in band.
//!
//! No writer renders an envelope yet — `begin_document` / `end_document`
//! default to no-ops — so this seam landing must leave the bytes identical to
//! the flag-off path. The test runs the same pipeline with the flag off and
//! on and asserts byte-equal output, proving the new arm streams every body
//! record through unchanged and the fused-path exclusion does not strand the
//! close.

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

fn run(reconstruct_envelope: bool) -> String {
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
    buf.as_string()
}

#[test]
fn reconstruct_envelope_flag_streams_records_byte_identically() {
    let baseline = run(false);
    let with_envelope = run(true);

    // Sanity: the baseline carried the body rows the punctuation-aware arm
    // must also carry.
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
}
