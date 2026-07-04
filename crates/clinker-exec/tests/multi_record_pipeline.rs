//! End-to-end multi-record flat-file ingestion.
//!
//! A single fixed-width file interleaves three record types — a header (`H`),
//! body detail rows (`D`), and a trailer (`T`) — distinguished by a one-byte
//! discriminator. The reader streams one record per line on a discriminator-
//! driven superset schema: the header surfaces as a `$doc` envelope section
//! (`record_type` extract), the detail rows route on the `record_type` column,
//! and the trailer's declared count is validated against the streamed body
//! count at document close.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

/// A three-record-type fixed-width payment file: one `H` header (batch id), two
/// `D` detail rows (id + amount), and a `T` trailer claiming 2 body records.
const PAYMENTS_OK: &str = "HBATCH0001\nD00001 100\nD00002 200\nT00002    \n";

/// Same shape, but the trailer claims 5 records where only 2 streamed — a
/// structural-count mismatch.
const PAYMENTS_BAD_COUNT: &str = "HBATCH0001\nD00001 100\nD00002 200\nT00005    \n";

/// Layout per line (`H`/`D`/`T` discriminator at byte 0):
/// - header `H`: `batch_id` at bytes 1..10
/// - detail `D`: `id` at 1..6, `amount` at 6..10
/// - trailer `T`: `count` at 1..6
fn pipeline_yaml() -> &'static str {
    r#"
pipeline:
  name: multi_record_payments
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: fixed_width
      glob: ./*.txt
      schema:
        discriminator: { start: 0, width: 1 }
        structure:
          - { record: trailer, count: count }
        records:
          - id: header
            tag: H
            columns:
              - { name: batch_id, type: string, start: 1, width: 9 }
          - id: detail
            tag: D
            columns:
              - { name: id, type: int, start: 1, width: 5 }
              - { name: amount, type: int, start: 6, width: 4 }
          - id: trailer
            tag: T
            columns:
              - { name: count, type: int, start: 1, width: 5 }
      envelope:
        sections:
          head:
            extract: { record_type: H }
            fields:
              batch_id: string
  - type: transform
    name: tag
    input: payments
    config:
      cxl: |
        emit kind = record_type
        emit id = id
        emit amount = amount
        emit batch = $doc.head.batch_id
  - type: route
    name: classify
    input: tag
    config:
      conditions:
        big: amount > 150
      default: small
  - type: output
    name: out_big
    input: classify.big
    config:
      name: out_big
      type: csv
      path: big.csv
  - type: output
    name: out_small
    input: classify.small
    config:
      name: out_small
      type: csv
      path: small.csv
"#
}

/// Run the pipeline over one in-memory fixed-width file, capturing both route
/// outputs.
fn run(
    yaml: &str,
    fixture: &str,
) -> Result<(clinker_exec::executor::ExecutionReport, String, String), String> {
    run_files(yaml, &[("payments.txt", fixture)])
}

/// Run the pipeline over an ordered set of named in-memory files, capturing
/// both route outputs. Models a multi-file source (each file its own document).
fn run_files(
    yaml: &str,
    files: &[(&str, &str)],
) -> Result<(clinker_exec::executor::ExecutionReport, String, String), String> {
    let config = parse_config(yaml).map_err(|e| format!("parse: {e:?}"))?;
    let plan = config
        .compile(&CompileContext::default())
        .map_err(|e| format!("compile: {e:?}"))?;

    let slots: Vec<FileSlot> = files
        .iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(name),
                Box::new(Cursor::new(body.as_bytes().to_vec())),
            )
        })
        .collect();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )]);

    let big = SharedBuffer::new();
    let small = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "out_big".to_string(),
            Box::new(big.clone()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "out_small".to_string(),
            Box::new(small.clone()) as Box<dyn std::io::Write + Send>,
        ),
    ]);

    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .map_err(|e| format!("run: {e:?}"))?;
    Ok((report, big.as_string(), small.as_string()))
}

#[test]
fn header_surfaces_as_doc_body_routes_trailer_validates() {
    let (report, big, small) =
        run(pipeline_yaml(), PAYMENTS_OK).expect("run multi-record pipeline");
    // Only the two `D` body rows stream: the `H` header is captured by the
    // pre-scan, the `T` trailer feeds structural validation.
    assert_eq!(report.counters.total_count, 2, "big={big}\nsmall={small}");
    assert_eq!(report.counters.dlq_count, 0);

    // The detail with amount 200 routes to `big` (> 150); amount 100 to
    // `small`. Both carry the header's batch id via `$doc.head.batch_id`.
    let big_lines: Vec<&str> = big.lines().collect();
    let small_lines: Vec<&str> = small.lines().collect();
    assert_eq!(big_lines.len(), 2, "header + one big row; got: {big}");
    assert_eq!(small_lines.len(), 2, "header + one small row; got: {small}");
    assert!(
        big_lines[1].contains("BATCH0001"),
        "big row missing $doc.head.batch_id: {big}"
    );
    assert!(
        big_lines[1].contains("200"),
        "big row should carry amount 200: {big}"
    );
    assert!(
        small_lines[1].contains("100"),
        "small row should carry amount 100: {small}"
    );
    // The body row's `kind` column is the matched record type id.
    assert!(
        big_lines[1].contains("detail"),
        "body row record_type should be 'detail': {big}"
    );
}

#[test]
fn trailer_count_mismatch_fails_the_run() {
    // The trailer claims 5 body records where only 2 streamed — a structural
    // count failure. With no document-DLQ strategy this aborts the run.
    let result = run(pipeline_yaml(), PAYMENTS_BAD_COUNT);
    let err = result.expect_err("trailer-count mismatch must fail the run");
    assert!(
        err.contains("declares count 5") || err.contains("structural count"),
        "error should name the trailer-count failure: {err}"
    );
}

#[test]
fn two_files_keep_independent_doc_context_and_trailer_counts() {
    // Each file is its own document: file A's header `$doc.head.batch_id` must
    // not leak into file B's records, and each trailer validates against its
    // own body count. A single fresh reader per file makes this hold; this
    // pins it end-to-end across a file boundary.
    let file_a = "HBATCH_AAA\nD00001 100\nD00002 200\nT00002    \n";
    let file_b = "HBATCH_BBB\nD00003 300\nT00001    \n";
    let (report, big, small) = run_files(pipeline_yaml(), &[("a.txt", file_a), ("b.txt", file_b)])
        .expect("run two-file multi-record pipeline");
    // 2 detail rows from A + 1 from B = 3 body records; both trailers (T2, T1)
    // validate against their own file's body count.
    assert_eq!(report.counters.total_count, 3, "big={big}\nsmall={small}");
    assert_eq!(report.counters.dlq_count, 0);

    let combined = format!("{big}\n{small}");
    // File A's rows carry BATCH_AAA; file B's row carries BATCH_BBB — no leak.
    assert!(
        combined.contains("BATCH_AAA"),
        "file A header must reach its rows: {combined}"
    );
    assert!(
        combined.contains("BATCH_BBB"),
        "file B header must reach its rows: {combined}"
    );
    // The amount-300 row (file B) must carry BATCH_BBB, never BATCH_AAA.
    let b_row = combined
        .lines()
        .find(|l| l.contains("300"))
        .expect("a row with amount 300");
    assert!(
        b_row.contains("BATCH_BBB") && !b_row.contains("BATCH_AAA"),
        "file B row leaked file A's $doc: {b_row}"
    );
}

/// A document-DLQ variant of the pipeline: the same `H`/`D`/`T` layout with
/// the trailer's declared count validated by `structure:`, but the source
/// opts into `dlq_granularity: document` under a `continue` strategy, so a
/// structural failure (an unknown tag, a trailer count mismatch) condemns
/// the offending file to the DLQ instead of aborting the run.
fn document_dlq_yaml() -> &'static str {
    r#"
pipeline:
  name: multi_record_dlq
error_handling:
  strategy: continue
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: fixed_width
      glob: ./*.txt
      dlq_granularity: document
      files:
        on_no_match: skip
      schema:
        discriminator: { start: 0, width: 1 }
        structure:
          - { record: trailer, count: count }
        records:
          - { id: header,  tag: H, columns: [ { name: batch_id, type: string, start: 1, width: 9 } ] }
          - { id: detail,  tag: D, columns: [ { name: id, type: int, start: 1, width: 5 }, { name: amount, type: int, start: 6, width: 4 } ] }
          - { id: trailer, tag: T, columns: [ { name: count, type: int, start: 1, width: 5 } ] }
  - type: transform
    name: tag
    input: payments
    config:
      cxl: |
        emit kind = record_type
        emit amount = amount
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
"#
}

/// Run one in-memory fixed-width file through the document-DLQ pipeline,
/// returning the execution report and the success-sink contents. The run
/// itself must complete: under `dlq_granularity: document` a structural
/// failure condemns the file, not the run.
fn run_document_dlq(fixture: &str) -> (clinker_exec::executor::ExecutionReport, String) {
    let config = parse_config(document_dlq_yaml()).expect("parse dlq pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile dlq pipeline");
    let slot = FileSlot::new(
        PathBuf::from("bad.txt"),
        Box::new(Cursor::new(fixture.as_bytes().to_vec())),
    );
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![slot]),
    )]);
    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("document-DLQ run must complete, not abort");
    (report, out.as_string())
}

#[test]
fn unknown_tag_aborts_the_run_by_default() {
    // Under the default `dlq_granularity: record` an unknown discriminator
    // aborts the run at the offending line — the document-DLQ disposition is
    // strictly opt-in.
    let bad_tag = "HBATCH0001\nD00001 100\nX99999 999\nT00001    \n";
    let err = run(pipeline_yaml(), bad_tag).expect_err("an unknown tag must abort the run");
    assert!(
        err.contains("E345") && err.contains("unknown record-type discriminator"),
        "error should name the unknown-tag failure: {err}"
    );
}

#[test]
fn unknown_tag_under_document_dlq_condemns_the_file_not_the_run() {
    // An unknown discriminator value is a document-structural failure: under
    // `dlq_granularity: document` it dead-letters the whole file rather than
    // aborting the run, so the run completes and the bad file's records land
    // in the DLQ instead of the success sink.
    //
    // One detail, then an unknown `X` tag → the document is condemned.
    let (report, sink) = run_document_dlq("D00001 100\nX99999 999\n");
    let triggers: Vec<_> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(
        triggers.len(),
        1,
        "exactly one root-cause trigger for the condemned file"
    );
    assert_eq!(
        triggers[0].category,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation
    );
    assert!(
        triggers[0].error_message.contains("E345")
            && triggers[0]
                .error_message
                .contains("unknown record-type discriminator"),
        "the trigger names the unknown tag, got {:?}",
        triggers[0].error_message
    );
    assert_eq!(
        report.dlq_entries.iter().filter(|e| !e.trigger).count(),
        1,
        "the one already-streamed detail row is rejected with the file"
    );
    // The already-streamed detail row is rejected with the file, so the
    // success sink holds no body rows.
    let body: Vec<&str> = sink.lines().skip(1).collect();
    assert!(
        body.is_empty(),
        "condemned file's records must not reach the success sink: {body:?}"
    );
}

#[test]
fn trailer_count_mismatch_under_document_dlq_condemns_the_file_not_the_run() {
    // The trailer claims 5 body records where only 2 streamed. Under
    // `dlq_granularity: document` the count mismatch condemns the file to the
    // DLQ — the same disposition as an unknown tag — and the run completes
    // instead of aborting.
    let (report, sink) = run_document_dlq("D00001 100\nD00002 200\nT00005    \n");
    let triggers: Vec<_> = report.dlq_entries.iter().filter(|e| e.trigger).collect();
    assert_eq!(
        triggers.len(),
        1,
        "exactly one root-cause trigger for the condemned file"
    );
    assert_eq!(
        triggers[0].category,
        clinker_core_types::dlq::DlqErrorCategory::StructuralValidation
    );
    assert!(
        triggers[0].error_message.contains("declares count 5"),
        "the trigger names the count mismatch, got {:?}",
        triggers[0].error_message
    );
    assert_eq!(
        report.dlq_entries.iter().filter(|e| !e.trigger).count(),
        2,
        "both already-streamed detail rows are rejected with the file"
    );
    let body: Vec<&str> = sink.lines().skip(1).collect();
    assert!(
        body.is_empty(),
        "condemned file's records must not reach the success sink: {body:?}"
    );
}
