//! Per-document Aggregate flush on the document-close boundary.
//!
//! A document-aware source feeds each source file to an Aggregate as its
//! own document, carrying a `DocumentClose` boundary at its tail. Correct
//! per-document aggregation flushes the groups belonging to a closing
//! document at that boundary, so a multi-document run produces one set of
//! grouped rows per document rather than a single cross-document
//! aggregate. These tests drive that end to end through a multi-file CSV
//! glob source (each file is a document) into a group-by `count(*)`
//! aggregate, and assert per-group counts match each document's body. A
//! no-document / single-document control proves the dominant path is
//! unchanged, a tiny memory budget exercises the spilling strategy across
//! a document boundary, the streaming-ingest path is covered via a fused
//! producer, and `concat` / `interleave` Merges of two single-document
//! sources prove documents whose close never reaches the aggregate fold
//! together into one cross-document result rather than splitting — even
//! when an interleave Merge delivers their records non-contiguously.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

/// Slice the indented `--explain` property block for the node whose
/// stanza starts with `slug:`, up to the next blank line. The physical
/// properties stanzas are two-space-indented and blank-line separated.
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

/// Group-by `count(*)` over a CSV glob source, output to CSV. The
/// `memory.limit` is templated so the same pipeline drives both the
/// in-memory and the spilling strategy.
fn count_by_category_yaml(memory_limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: per_doc_agg
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
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
        - {{ name: category, type: string }}
  - type: aggregate
    name: by_category
    input: events
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
"#
    )
}

/// Run the count-by-category pipeline over a set of in-memory CSV files,
/// each fed as a distinct `FileSlot` (hence a distinct document). Returns
/// the sorted body lines (`category,n`) and the run's ok/dlq counts.
fn run_multi_file(memory_limit: &str, files: &[(&str, &str)]) -> (Vec<String>, u64, u64) {
    let yaml = count_by_category_yaml(memory_limit);
    let config = parse_config(&yaml).expect("parse per-document aggregate pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile per-document aggregate pipeline");

    let slots: Vec<FileSlot> = files
        .iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(*name),
                Box::new(Cursor::new(body.as_bytes().to_vec())),
            )
        })
        .collect();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
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
        .expect("run per-document aggregate pipeline");

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    (body, report.counters.ok_count, report.counters.dlq_count)
}

#[test]
fn multi_document_emits_one_group_set_per_document() {
    // File A is one document: category x appears twice, y once. File B is
    // a separate document: category x appears three times. Per-document
    // aggregation must emit x=2 and y=1 for document A and a SEPARATE x=3
    // for document B — a single cross-document aggregate would instead
    // collapse to x=5, y=1.
    let (body, ok, dlq) = run_multi_file(
        "512M",
        &[
            ("a.csv", "category\nx\nx\ny\n"),
            ("b.csv", "category\nx\nx\nx\n"),
        ],
    );
    assert_eq!(dlq, 0, "no DLQ entries expected");
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "expected per-document groups (x=2,y=1 for doc A; x=3 for doc B), \
         not a single cross-document x=5",
    );
    assert_eq!(ok, 3, "three grouped rows across the two documents");
}

#[test]
fn three_documents_each_flush_independently() {
    // Three documents, each contributing its own group set; the same
    // category key recurs in every document with a different count, so a
    // correct run yields three distinct rows for it rather than one
    // summed row.
    let (body, ok, dlq) = run_multi_file(
        "512M",
        &[
            ("d1.csv", "category\na\na\n"),
            ("d2.csv", "category\na\nb\n"),
            ("d3.csv", "category\na\na\na\nb\nb\n"),
        ],
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec![
            "a,1".to_string(), // d2
            "a,2".to_string(), // d1
            "a,3".to_string(), // d3
            "b,1".to_string(), // d2
            "b,2".to_string(), // d3
        ],
    );
    assert_eq!(ok, 5);
}

#[test]
fn single_document_emits_one_aggregate_unchanged() {
    // One file = one document: the dominant path must accumulate every row
    // and emit a single aggregate per group, byte-identical to the
    // pre-per-document behavior. x=2, y=1 — never split.
    let (body, ok, dlq) = run_multi_file("512M", &[("only.csv", "category\nx\nx\ny\n")]);
    assert_eq!(dlq, 0);
    assert_eq!(body, vec!["x,2".to_string(), "y,1".to_string()]);
    assert_eq!(ok, 2, "single document emits one aggregate per group");
}

#[test]
fn spilling_strategy_flushes_per_document_across_boundary() {
    // A 1 KB budget clamps the in-memory group cap so the aggregator
    // spills to disk; the per-document flush must still split groups by
    // document across the spill round-trip. Many distinct keys per
    // document force spill within each document's bucket.
    let mut doc_a = String::from("category\n");
    for i in 0..200 {
        doc_a.push_str(&format!("a{}\n", i % 50));
    }
    let mut doc_b = String::from("category\n");
    for i in 0..200 {
        doc_b.push_str(&format!("b{}\n", i % 50));
    }
    let (body, ok, dlq) = run_multi_file(
        "1K",
        &[("a.csv", doc_a.as_str()), ("b.csv", doc_b.as_str())],
    );
    assert_eq!(dlq, 0, "spilling run produces no DLQ entries");
    // 50 distinct `a*` keys (each count 4) from document A, 50 distinct
    // `b*` keys (each count 4) from document B — 100 groups total, every
    // key namespaced to its own document.
    assert_eq!(ok, 100, "50 groups per document, kept per-document");
    assert_eq!(body.len(), 100);
    let a_groups = body.iter().filter(|r| r.starts_with("a")).count();
    let b_groups = body.iter().filter(|r| r.starts_with("b")).count();
    assert_eq!(a_groups, 50, "document A contributes 50 groups");
    assert_eq!(b_groups, 50, "document B contributes 50 groups");
    for row in &body {
        assert!(
            row.ends_with(",4"),
            "every key appears 4 times within its document: {row}"
        );
    }
}

/// A fused `Source → Transform` feeding a strict hash Aggregate is
/// certified for the streaming-ingest substrate: the aggregate drives
/// `add_record` off a bounded channel rather than draining a charged
/// inter-stage slot. This exercises the per-document flush on THAT path —
/// the `DocumentClose` arriving in stream order flushes its document's
/// groups inside the scoped ingest thread.
const STREAMING_INGEST_YAML: &str = r#"
pipeline:
  name: per_doc_agg_streaming
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
  - type: transform
    name: passthrough
    input: events
    config:
      cxl: |
        emit category = category
  - type: aggregate
    name: by_category
    input: passthrough
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
fn streaming_ingest_path_flushes_per_document() {
    let config = parse_config(STREAMING_INGEST_YAML).expect("parse streaming-ingest pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile streaming-ingest pipeline");

    // Confirm the planner certified the fused Transform → Aggregate edge
    // for streaming ingest, so this run actually drives the scoped-thread
    // ingest path (not the materialized drain). The transform classifies
    // `buffer: streaming` and emits no charged node_buffer edge to the
    // aggregate.
    let (dag, _) =
        clinker_exec::executor::PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let explain = dag.explain_text(&config);
    let block = properties_block(&explain, "transform.passthrough");
    assert!(
        block.contains("buffer: streaming") && !block.contains("buffer: materialized"),
        "expected the fused transform feeding the aggregate to classify \
         streaming (so the streaming-ingest aggregate path runs), got:\n{block}"
    );
    assert!(
        !explain.contains("edge transform.passthrough -> aggregation.by_category"),
        "a streaming-ingest aggregate must consume no charged inter-stage \
         slot from its producer, got:\n{explain}"
    );

    // Same multi-document data as the materialized test: per-document
    // flush must hold on the streaming-ingest path too.
    let slots = vec![
        FileSlot::new(
            PathBuf::from("a.csv"),
            Box::new(Cursor::new(b"category\nx\nx\ny\n".to_vec())),
        ),
        FileSlot::new(
            PathBuf::from("b.csv"),
            Box::new(Cursor::new(b"category\nx\nx\nx\n".to_vec())),
        ),
    ];
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
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
        .expect("run streaming-ingest pipeline");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "streaming-ingest path must also split groups per document",
    );
}

/// A `concat` Merge of two distinct single-document sources feeds the
/// Aggregate records carrying two different document ids — but the Merge
/// reconciles boundaries and forwards a `DocumentClose` only when every
/// branch closed the same document, which never happens for distinct
/// per-source documents. With no close reaching the Aggregate, the two
/// documents must fold together into ONE cross-document aggregate, exactly
/// as a no-envelope stream would: per-document flush must NOT split an
/// unreconciled merge into one aggregate per source file.
const UNRECONCILED_MERGE_YAML: &str = r#"
pipeline:
  name: unreconciled_merge_agg
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
fn unreconciled_merge_folds_documents_together() {
    let config = parse_config(UNRECONCILED_MERGE_YAML).expect("parse unreconciled-merge pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile unreconciled-merge pipeline");

    // sa contributes x×2, y×1; sb contributes x×1. With the two documents
    // unreconciled (no forwarded close), the aggregate folds them together:
    // x=3 (2 from sa + 1 from sb), y=1 — a single cross-document result,
    // NOT a per-document split (which would yield x=2, y=1, x=1).
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
                Box::new(Cursor::new(b"category\nx\n".to_vec())),
            ),
        ),
    ]);
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
        .expect("run unreconciled-merge pipeline");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    assert_eq!(
        body,
        vec!["x,3".to_string(), "y,1".to_string()],
        "an unreconciled merge must fold its source documents into one \
         aggregate (x=3, y=1), not split per source file",
    );
}

/// An `interleave` Merge round-robins records from two distinct-document
/// sources, so the aggregate's input carries the two documents' records
/// non-contiguously. Their closes are unreconciled (never forwarded), so
/// the records must still fold into one aggregate regardless of arrival
/// order — proving the per-document flush keys group state by document
/// rather than by a running "current document" that interleaving would
/// corrupt.
#[test]
fn interleaved_unreconciled_documents_fold_together() {
    let yaml = r#"
pipeline:
  name: interleave_merge_agg
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
      mode: interleave
      interleave_seed: 7
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
    let config = parse_config(yaml).expect("parse interleave-merge pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile interleave-merge pipeline");

    // sa: x×3; sb: x×2, y×1. Interleaved and unreconciled → one aggregate:
    // x=5, y=1.
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([
        (
            "sa".to_string(),
            clinker_exec::executor::single_file_reader(
                "sa.csv",
                Box::new(Cursor::new(b"category\nx\nx\nx\n".to_vec())),
            ),
        ),
        (
            "sb".to_string(),
            clinker_exec::executor::single_file_reader(
                "sb.csv",
                Box::new(Cursor::new(b"category\nx\nx\ny\n".to_vec())),
            ),
        ),
    ]);
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
        .expect("run interleave-merge pipeline");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    assert_eq!(
        body,
        vec!["x,5".to_string(), "y,1".to_string()],
        "interleaved unreconciled documents must fold into one aggregate \
         (x=5, y=1) regardless of round-robin arrival order",
    );
}
