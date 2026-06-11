//! Regression coverage for the streaming-ingest aggregate arm.
//!
//! These tests pin behaviors of the bounded-channel ingest path a strict
//! Aggregate takes when its producer streams (a fused, streaming-certified
//! `Source → Transform`, or a non-fused Merge):
//!
//! 1. An ingest error must not deadlock. The producer streams into a bounded
//!    channel on the dispatch thread while a scoped thread drives
//!    `add_record`; if that thread returned on an error without first
//!    draining the channel, a producer blocked on a full bounded `send`
//!    would hang forever and the join would never complete. A `fail_fast`
//!    run whose aggregate errors early — with far more than the channel's
//!    depth still queued behind it — must surface the error and terminate.
//!
//! 2. A global fold (no group-by) over an empty stream still owes its single
//!    defaulted row, matching what a standalone empty run emits via
//!    `HashAggregator::finalize`. A grouped fold owes nothing. The fix also
//!    forces a table open on a global fold's zero-record `DocumentClose` so
//!    an empty closing document emits its defaulted row, while the
//!    disconnect-time empty-input flush is gated so a run whose closes
//!    already paid that debt gains no phantom duplicate. (A plain multi-file
//!    CSV glob delivers an empty trailing file as no document at all, so the
//!    empty case the tests here exercise is the empty stream. The per-document
//!    zero-record close IS reachable, though: an envelope source emits a
//!    balanced `DocumentOpen`/`DocumentClose` pair with no records between for
//!    a header-only / body-less document — e.g. a header-only EDI interchange
//!    — and the force-open ensures such a close still emits its global-fold
//!    defaulted row.)
//!
//! 3. Per-document bucketing under tail-batched closes. The streaming ingest
//!    aggregate keys group state per `DocumentId`. When the producer is
//!    non-fused (a concat Merge of distinct single-document sources), it
//!    emits every record first and every `DocumentClose` afterward, so the
//!    closes arrive tail-batched rather than in stream order. The bucket
//!    model still flushes each document's groups on its own close, splitting
//!    the documents rather than folding them into one cross-document result.
//!
//! The fused tests assert (via the `--explain` `buffer: streaming`
//! classification) that the fused producer actually drives the
//! streaming-ingest path rather than the materialized drain.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::mpsc;
use std::time::Duration;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

/// Slice the indented `--explain` property block for the node whose stanza
/// starts with `slug:`, up to the next blank line. The physical-properties
/// stanzas are two-space-indented and blank-line separated.
fn properties_block(explain: &str, slug: &str) -> String {
    let marker = format!("{slug}:");
    explain
        .lines()
        .skip_while(|l| l.trim_start() != marker)
        .skip(1)
        .take_while(|l| !l.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Assert the fused transform feeding the aggregate classifies `streaming`
/// (so the run drives the scoped-thread ingest path, not the materialized
/// drain) and routes no charged inter-stage slot into the aggregate.
fn assert_streaming_ingest(config_yaml: &str, transform_slug: &str, aggregate_slug: &str) {
    let config = parse_config(config_yaml).expect("parse streaming-ingest pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile streaming-ingest pipeline");
    let (dag, _) = PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let explain = dag.explain_text(&config);
    let block = properties_block(&explain, transform_slug);
    assert!(
        block.contains("buffer: streaming") && !block.contains("buffer: materialized"),
        "expected {transform_slug} feeding {aggregate_slug} to classify \
         streaming (so the streaming-ingest aggregate path runs), got:\n{block}"
    );
    assert!(
        !explain.contains(&format!("edge {transform_slug} -> {aggregate_slug}")),
        "a streaming-ingest aggregate must consume no charged inter-stage \
         slot from its producer, got:\n{explain}"
    );
}

/// Build the readers map for a CSV glob `events` source from in-memory file
/// bodies, each fed as its own `FileSlot` (hence its own document).
fn file_readers(files: &[(&str, String)]) -> clinker_exec::executor::SourceReaders {
    let slots: Vec<FileSlot> = files
        .iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(*name),
                Box::new(Cursor::new(body.as_bytes().to_vec())),
            )
        })
        .collect();
    HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )])
}

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// A fused `Source → Transform` feeding a strict hash Aggregate over a CSV
/// glob, with `fail_fast` error handling and a per-record division whose
/// divisor is read straight from the input. A `divisor` of `0` divides by
/// zero inside the aggregate's `add_record`, the deterministic mid-stream
/// ingest error the deadlock test relies on.
const FAIL_FAST_DIVIDE_YAML: &str = r#"
pipeline:
  name: streaming_ingest_fail_fast
error_handling:
  strategy: fail_fast
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: category, type: string }
        - { name: divisor, type: int }
  - type: transform
    name: passthrough
    input: events
    config:
      cxl: |
        emit category = category
        emit divisor = divisor
  - type: aggregate
    name: by_category
    input: passthrough
    config:
      group_by:
        - category
      cxl: |
        emit category = category
        emit ratio = sum(100 / divisor)
  - type: output
    name: out
    input: by_category
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

#[test]
fn streaming_ingest_fail_fast_error_terminates_without_hanging() {
    // Confirm the run actually drives the streaming-ingest path.
    assert_streaming_ingest(
        FAIL_FAST_DIVIDE_YAML,
        "transform.passthrough",
        "aggregation.by_category",
    );

    // One record with `divisor = 0` near the front poisons the aggregate's
    // `add_record` on the second row, while several thousand records queue
    // behind it. The bounded ingest channel holds 256 events, so the
    // producer's `send` blocks long before the consumer reaches end-of-input
    // — the exact condition under which a non-draining error return would
    // deadlock the join. The fix drains the channel before surfacing the
    // error, so the producer's blocked `send` makes progress, the producer
    // finishes and drops its sender, and the run terminates with an error.
    let mut body = String::from("category,divisor\n");
    body.push_str("a,2\n");
    body.push_str("a,0\n"); // poison: 100 / 0 → division by zero
    for _ in 0..4000 {
        body.push_str("a,2\n");
    }
    let config = parse_config(FAIL_FAST_DIVIDE_YAML).expect("parse");
    let plan = config.compile(&CompileContext::default()).expect("compile");

    // Run on a worker thread and join with a timeout: a regression that
    // reintroduces the non-draining return would hang here rather than
    // returning an error, and the timeout converts that hang into a test
    // failure instead of a stuck suite.
    let (done_tx, done_rx) = mpsc::channel::<bool>();
    let worker = std::thread::spawn(move || {
        let readers = file_readers(&[("events.csv", body)]);
        let buf = SharedBuffer::new();
        let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
            "out".to_string(),
            Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
        )]);
        let result =
            PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params());
        // Signal completion with whether the run errored, then let the thread
        // finish so its resources release.
        let _ = done_tx.send(result.is_err());
    });

    match done_rx.recv_timeout(Duration::from_secs(30)) {
        Ok(errored) => {
            worker.join().expect("ingest worker thread panicked");
            assert!(
                errored,
                "a fail_fast division-by-zero in the aggregate must surface as \
                 a run error, not a silent success"
            );
        }
        Err(mpsc::RecvTimeoutError::Timeout) => {
            panic!(
                "streaming-ingest run did not complete within 30s — the ingest \
                 thread likely returned on the aggregate error without draining \
                 the bounded channel, deadlocking the producer's send"
            );
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            panic!("ingest worker thread dropped its completion channel");
        }
    }
}

/// A fused `Source → Transform` feeding a GLOBAL-fold Aggregate (no
/// group-by) over a CSV glob. The fold sums `amount` across the document.
const GLOBAL_FOLD_YAML: &str = r#"
pipeline:
  name: streaming_ingest_global_fold
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: amount, type: int }
  - type: transform
    name: passthrough
    input: events
    config:
      cxl: |
        emit amount = amount
  - type: aggregate
    name: total
    input: passthrough
    config:
      group_by: []
      cxl: |
        emit total = sum(amount)
        emit n = count(*)
  - type: output
    name: out
    input: total
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// The same data into a GROUPED Aggregate (group-by `amount`). An empty
/// document owes no group row, so the empty document emits nothing.
const GROUPED_FOLD_YAML: &str = r#"
pipeline:
  name: streaming_ingest_grouped_fold
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: amount, type: int }
  - type: transform
    name: passthrough
    input: events
    config:
      cxl: |
        emit amount = amount
  - type: aggregate
    name: total
    input: passthrough
    config:
      group_by:
        - amount
      cxl: |
        emit amount = amount
        emit n = count(*)
  - type: output
    name: out
    input: total
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// Run a streaming-ingest pipeline over the given CSV files; return the
/// sorted output body lines and the ok / dlq counts.
fn run_streaming(yaml: &str, files: &[(&str, String)]) -> (Vec<String>, u64, u64) {
    let config = parse_config(yaml).expect("parse streaming-ingest pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile streaming-ingest pipeline");
    let readers = file_readers(files);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("run streaming-ingest pipeline");
    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    (body, report.counters.ok_count, report.counters.dlq_count)
}

#[test]
fn empty_input_global_fold_emits_one_defaulted_row() {
    assert_streaming_ingest(
        GLOBAL_FOLD_YAML,
        "transform.passthrough",
        "aggregation.total",
    );

    // A lone header-only file delivers no records and no document close to
    // the aggregate, so the stream is empty. A global fold still owes its
    // single defaulted row (`total` empty, `n=0`) — exactly what a standalone
    // empty run emits via `HashAggregator::finalize`'s zero-rows-seen,
    // no-group-by branch. The disconnect-time `ensure_open` forces a table
    // into existence so that finalize runs and the defaulted row is not
    // silently dropped. Exactly one row — never doubled.
    let (body, ok, dlq) = run_streaming(GLOBAL_FOLD_YAML, &[("only.csv", "amount\n".to_string())]);
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec![",0".to_string()],
        "an empty global fold emits exactly one defaulted row (empty total, n=0)"
    );
    assert_eq!(ok, 1, "exactly one defaulted row, not a doubled phantom");
}

#[test]
fn multiple_empty_files_global_fold_emit_one_defaulted_row() {
    assert_streaming_ingest(
        GLOBAL_FOLD_YAML,
        "transform.passthrough",
        "aggregation.total",
    );

    // Several header-only files still deliver no records and no closes, so
    // the aggregate sees one empty stream and owes exactly one defaulted
    // row at disconnect — the disconnect force-open fires only when no
    // document was flushed (the empty-`flushed` gate), so the single
    // disconnect-time finalize is not multiplied into a phantom row per
    // file.
    let (body, ok, dlq) = run_streaming(
        GLOBAL_FOLD_YAML,
        &[
            ("a.csv", "amount\n".to_string()),
            ("b.csv", "amount\n".to_string()),
            ("c.csv", "amount\n".to_string()),
        ],
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec![",0".to_string()],
        "empty files collapse to exactly one defaulted global-fold row"
    );
    assert_eq!(ok, 1, "one defaulted row total, never one per empty file");
}

#[test]
fn populated_global_fold_emits_no_phantom_row() {
    assert_streaming_ingest(
        GLOBAL_FOLD_YAML,
        "transform.passthrough",
        "aggregation.total",
    );

    // A single populated document closes and flushes its real fold row
    // (`total=7`, `n=2`). The disconnect-time flush must add no extra
    // defaulted row — a closing document that DID see records already paid
    // its debt, so the global-fold force-open never fabricates a phantom.
    let (body, ok, dlq) =
        run_streaming(GLOBAL_FOLD_YAML, &[("a.csv", "amount\n3\n4\n".to_string())]);
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["7,2".to_string()],
        "a populated global fold emits its single real row and no phantom"
    );
    assert_eq!(ok, 1, "one fold row, no extra defaulted row at disconnect");
}

#[test]
fn multi_document_populated_global_fold_emits_one_row_per_document() {
    assert_streaming_ingest(
        GLOBAL_FOLD_YAML,
        "transform.passthrough",
        "aggregation.total",
    );

    // Two populated documents, each closing at its own file boundary. The
    // global fold force-opens on each `DocumentClose`, but the table is
    // already open (records arrived), so the force-open is a no-op and each
    // document emits exactly its own real fold row — no phantom defaulted
    // row, and no cross-document collapse.
    let (body, ok, dlq) = run_streaming(
        GLOBAL_FOLD_YAML,
        &[
            ("a.csv", "amount\n3\n4\n".to_string()),
            ("b.csv", "amount\n10\n".to_string()),
        ],
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["10,1".to_string(), "7,2".to_string()],
        "each populated document emits its own global-fold row (7,2 and 10,1)"
    );
    assert_eq!(ok, 2, "one fold row per document, no phantom");
}

#[test]
fn empty_files_grouped_aggregate_emit_nothing() {
    assert_streaming_ingest(
        GROUPED_FOLD_YAML,
        "transform.passthrough",
        "aggregation.total",
    );

    // Same multi-file shape, but grouped by `amount`. File A yields one
    // group per distinct amount; the empty file B owes nothing — a grouped
    // aggregate has no defaulted row for an empty document or an empty
    // stream, so the global-fold force-open path is correctly NOT taken.
    let (body, ok, dlq) = run_streaming(
        GROUPED_FOLD_YAML,
        &[
            ("a.csv", "amount\n3\n4\n3\n".to_string()),
            ("b.csv", "amount\n".to_string()),
        ],
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["3,2".to_string(), "4,1".to_string()],
        "the grouped aggregate emits only document A's groups; the empty \
         file contributes no row"
    );
    assert_eq!(ok, 2, "two groups from document A, none for the empty file");
}

#[test]
fn empty_input_grouped_aggregate_emits_nothing() {
    assert_streaming_ingest(
        GROUPED_FOLD_YAML,
        "transform.passthrough",
        "aggregation.total",
    );

    // A lone header-only file is an empty stream into a grouped aggregate:
    // no groups, no defaulted row. The disconnect-time `ensure_open` fires
    // (input was empty) but the grouped finalize emits nothing, exactly as a
    // standalone empty grouped run does.
    let (body, ok, dlq) = run_streaming(GROUPED_FOLD_YAML, &[("only.csv", "amount\n".to_string())]);
    assert_eq!(dlq, 0);
    assert!(
        body.is_empty(),
        "an empty grouped aggregate emits no rows, got: {body:?}"
    );
    assert_eq!(ok, 0, "no rows for an empty grouped aggregate");
}

/// A `concat` Merge of two distinct single-document sources feeds a grouped
/// Aggregate. A non-fused Merge is a certified streaming producer, so the
/// Aggregate runs the streaming-ingest path — but the Merge tail-batches its
/// forwarded `DocumentClose` punctuations after every record (records first,
/// then both closes), so the closes do NOT arrive in stream order. The
/// per-document bucket model must still flush each document's groups on its
/// own close, splitting the two documents instead of folding them into one
/// cross-document result.
const NON_FUSED_MERGE_GROUPED_YAML: &str = r#"
pipeline:
  name: non_fused_streaming_buckets
nodes:
  - type: source
    name: sa
    config:
      name: sa
      type: csv
      path: ./sa.csv
      schema:
        - { name: category, type: string }
  - type: source
    name: sb
    config:
      name: sb
      type: csv
      path: ./sb.csv
      schema:
        - { name: category, type: string }
  - type: merge
    name: m
    inputs: [sa, sb]
    config:
      mode: concat
  - type: aggregate
    name: by_category
    input: m
    config:
      group_by:
        - category
      cxl: |
        emit category = category
        emit n = count(*)
  - type: output
    name: out
    input: by_category
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

#[test]
fn non_fused_streaming_multi_document_buckets_per_document() {
    let config =
        parse_config(NON_FUSED_MERGE_GROUPED_YAML).expect("parse non-fused-merge pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile non-fused-merge pipeline");

    // doc A (sa): x×2, y×1. doc B (sb): x×3. The Merge forwards both
    // documents' closes (open == close == 1 per document) but tail-batches
    // them after every record. The streaming Aggregate buckets per document,
    // so each close flushes only its own bucket: x=2,y=1 from doc A and a
    // SEPARATE x=3 from doc B — NOT a folded x=5.
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([
        (
            "sa".to_string(),
            clinker_exec::executor::single_file_reader(
                "sa.csv",
                Box::new(Cursor::new(b"category\nx\nx\ny\n".to_vec())),
            ),
        ),
        (
            "sb".to_string(),
            clinker_exec::executor::single_file_reader(
                "sb.csv",
                Box::new(Cursor::new(b"category\nx\nx\nx\n".to_vec())),
            ),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("run non-fused-merge pipeline");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "the streaming Aggregate must bucket per document under tail-batched \
         closes (x=2,y=1 from doc A; x=3 from doc B), not fold to x=5"
    );
}

/// A fused single no-envelope source into a global fold: every record
/// carries the synthetic document id, so the bucket model keeps exactly one
/// table (the sentinel) and finalizes it once at disconnect — byte-identical
/// to the prior single-table flush for the dominant no-document case.
const NO_DOCUMENT_GLOBAL_FOLD_YAML: &str = r#"
pipeline:
  name: streaming_no_document_global_fold
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: ./events.csv
      schema:
        - { name: amount, type: int }
  - type: transform
    name: passthrough
    input: events
    config:
      cxl: |
        emit amount = amount
  - type: aggregate
    name: total
    input: passthrough
    config:
      group_by: []
      cxl: |
        emit total = sum(amount)
        emit n = count(*)
  - type: output
    name: out
    input: total
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

#[test]
fn streaming_no_document_single_table_byte_identical() {
    assert_streaming_ingest(
        NO_DOCUMENT_GLOBAL_FOLD_YAML,
        "transform.passthrough",
        "aggregation.total",
    );

    // A single populated no-envelope source: every record buckets to the
    // synthetic sentinel, so exactly one table is built and finalized once at
    // disconnect — one global-fold row (total=12, n=3), byte-identical to the
    // old single-table flush.
    let config = parse_config(NO_DOCUMENT_GLOBAL_FOLD_YAML).expect("parse no-document pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile no-document pipeline");
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::single_file_reader(
            "events.csv",
            Box::new(Cursor::new(b"amount\n3\n4\n5\n".to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("run no-document global-fold pipeline");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    assert_eq!(
        body,
        vec!["12,3".to_string()],
        "a no-document global fold buckets every record to the sentinel and \
         emits exactly one row (total=12, n=3)"
    );
    assert_eq!(
        report.counters.ok_count, 1,
        "exactly one global-fold row for the single sentinel bucket"
    );
}
