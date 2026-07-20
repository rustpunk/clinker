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
//! producer, and `concat` / seeded-`interleave` / unseeded-`interleave`
//! Merges of two single-document sources prove every Merge mode forwards
//! each source's per-document close, so each source document flushes
//! independently downstream, even when a seeded interleave delivers their
//! records non-contiguously.
//!
//! The unseeded interleave is the FUSED all-Source path: with no
//! `interleave_seed`, the Merge takes ownership of its Source receivers and
//! runs a `crossbeam_channel::Select` over them, whereas the seeded sibling
//! stays non-fused so its deterministic schedule survives over a
//! pre-buffered Vec. The fused arm collects the per-Source document
//! boundaries off the live channels and reconciles them at close, the same
//! cross-input fold the non-fused arm runs over pre-buffered input
//! punctuations, so a fused Merge forwards per-document closes too.
//!
//! No streaming fused `Merge.interleave -> single Output` boundary test
//! lives here: an Output consumes document punctuations at finalize and
//! never writes them into the CSV body, so a fused-streaming boundary is
//! not observable in the writer's output. The materialized fused case
//! below (an Aggregate downstream of the Merge) is where forwarding the
//! boundary changes observable output, so that is the regression guard.

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

/// Per-document identity must survive an UPSTREAM record spill, not just the
/// Aggregate's own accumulator spill. A multi-document CSV source fans out
/// through a Route whose materialized branch `node_buffers` spill records to
/// disk under a 1 MiB budget (the RSS-soft predicate from
/// `route_fanout_soft_spill.rs`), then those re-hydrated records feed a
/// per-document `count(*)` Aggregate. If the document context were lost on
/// the record-half spill round-trip, every post-spill record would re-hydrate
/// to the SYNTHETIC document and the per-document flush would fold all
/// documents into a single cross-document group set. The per-document counts
/// holding across the spill is the end-to-end proof that `doc_ctx` survived.
const UPSTREAM_SPILL_YAML: &str = r#"
pipeline:
  name: per_doc_upstream_spill
  memory: { limit: "1M", backpressure: spill }
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
        - { name: payload, type: string }
  - type: route
    name: split
    input: events
    config:
      mode: exclusive
      conditions:
        keep: "true"
      default: drop
  - type: aggregate
    name: by_category
    input: split.keep
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
fn per_document_identity_survives_upstream_record_spill() {
    // RSS-based spill predicate: with no RSS reading the upstream node_buffer
    // never spills, so the record-half round-trip under test never fires.
    // Skip rather than assert a false negative.
    if clinker_exec::pipeline::memory::rss_bytes().is_none() {
        return;
    }

    // Two documents (one CSV file each). Document A: category x×400, y×200.
    // Document B: category x×300. A wide `payload` column inflates per-row
    // node_buffer charge so the Route branch slot crosses the RSS soft floor
    // and spills its records through the inter-stage SpillWriter.
    let wide = "p".repeat(64);
    let mut doc_a = String::from("category,payload\n");
    for _ in 0..400 {
        doc_a.push_str(&format!("x,{wide}\n"));
    }
    for _ in 0..200 {
        doc_a.push_str(&format!("y,{wide}\n"));
    }
    let mut doc_b = String::from("category,payload\n");
    for _ in 0..300 {
        doc_b.push_str(&format!("x,{wide}\n"));
    }

    let config = parse_config(UPSTREAM_SPILL_YAML).expect("parse upstream-spill pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile upstream-spill pipeline");

    let slots = vec![
        FileSlot::new(
            PathBuf::from("a.csv"),
            Box::new(Cursor::new(doc_a.into_bytes())),
        ),
        FileSlot::new(
            PathBuf::from("b.csv"),
            Box::new(Cursor::new(doc_b.into_bytes())),
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
        .expect("run upstream-spill pipeline");
    assert_eq!(report.counters.dlq_count, 0, "no DLQ entries expected");

    // Fail loudly if the run stopped spilling — the test's whole point is to
    // exercise the upstream record-spill round-trip, so a zero here means the
    // assertion below would pass vacuously.
    assert!(
        report.cumulative_spill_bytes > 0,
        "upstream node_buffer must have spilled records under the 1 MiB budget; \
         cumulative_spill_bytes = {}",
        report.cumulative_spill_bytes,
    );

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    // Per-document flush: document A keeps x=400, y=200; document B keeps a
    // SEPARATE x=300 — never a folded x=700 that a SYNTHETIC collapse would
    // produce.
    assert_eq!(
        body,
        vec![
            "x,300".to_string(),
            "x,400".to_string(),
            "y,200".to_string()
        ],
        "each document's counts must stay attributed across the upstream record \
         spill (x=400,y=200 for doc A; x=300 for doc B), with no cross-document \
         fold into a synthetic document",
    );
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
/// Aggregate records carrying two different document ids. The non-fused
/// Merge reconciles boundaries by open-coverage: each source document has
/// open count == close count == 1 on its single branch, so the Merge
/// forwards each document's `DocumentClose`. A non-fused Merge is a
/// certified streaming producer, so the downstream Aggregate runs the
/// streaming ingest path and buckets group state per document — flushing
/// each source document's bucket on its close even though the closes arrive
/// tail-batched after every record. The per-document flush therefore splits
/// into one group set per source file.
const PER_SOURCE_MERGE_YAML: &str = r#"
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
fn concat_merge_forwards_per_source_document_close() {
    let config = parse_config(PER_SOURCE_MERGE_YAML).expect("parse per-source-merge pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile per-source-merge pipeline");

    // sa contributes x×2, y×1; sb contributes x×1. The non-fused Merge
    // forwards each source document's close (open == close == 1 per
    // document), so the aggregate flushes per document: x=2, y=1 from sa's
    // document and a SEPARATE x=1 from sb's document — not a folded x=3.
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
        .expect("run per-source-merge pipeline");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    assert_eq!(
        body,
        vec!["x,1".to_string(), "x,2".to_string(), "y,1".to_string()],
        "a non-fused concat merge forwards each source document's close, so \
         the aggregate flushes per document (x=2,y=1 from sa; x=1 from sb), \
         not a folded x=3",
    );
}

/// A SEEDED `interleave` Merge round-robins records from two
/// distinct-document sources, so the aggregate's input carries the two
/// documents' records non-contiguously. A seeded interleave is non-fused
/// (the seed excludes it from the all-Source fusion fast path so its
/// schedule stays deterministic over a pre-buffered Vec), so it reconciles
/// boundaries and forwards each source's per-document close, and — being a
/// certified streaming producer — feeds the streaming Aggregate ingest path,
/// which buckets group state per document even though the closes arrive
/// tail-batched after all records. The aggregate therefore flushes per
/// document regardless of round-robin arrival order — proving the
/// per-document bucketing keys group state by document rather than by a
/// running "current document" that interleaving would corrupt.
#[test]
fn seeded_interleave_merge_forwards_per_source_document_close() {
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

    // sa: x×3 (one document); sb: x×2, y×1 (another document). The seeded
    // interleave is non-fused, so it reconciles boundaries and forwards
    // each source's per-document close → per-document flush: x=3 from sa's
    // document, and x=2, y=1 from sb's document.
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
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "a seeded interleave is non-fused, so it forwards each source's \
         per-document close → per-document flush (x=3 from sa; x=2,y=1 from \
         sb), regardless of round-robin arrival order",
    );
}

/// An UNSEEDED `interleave` Merge of two distinct-document Sources takes the
/// fused all-Source path: the Merge owns the Source receivers and runs a
/// `crossbeam_channel::Select` over the live channels rather than draining
/// pre-buffered input slots. The fused arm collects each Source's document
/// boundaries off its channel and reconciles them at close — the same
/// cross-input fold the non-fused arm runs — then admits the reconciled
/// closes into its node buffer trailing the records, so the downstream
/// per-document Aggregate flushes each Source's document independently.
///
/// This is the regression guard for the fused path. With boundaries dropped
/// on the fused arm (the historical behavior), no `DocumentClose` reaches
/// the Aggregate until end-of-input, so it folds both documents into a
/// single `x,5` + `y,1`. Forwarding the reconciled boundaries splits the
/// fold per document: `x,3` from sa's document and `x,2` + `y,1` from sb's.
/// The split is impossible without forwarding, so the body assertion alone
/// pins the fix; the explicit fused-path assertion below removes any doubt
/// that the run actually took the fused arm rather than an accidental
/// non-fused one.
#[test]
fn fused_interleave_merge_forwards_per_source_document_close() {
    let yaml = r#"
pipeline:
  name: fused_interleave_merge_agg
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
    let config = parse_config(yaml).expect("parse fused-interleave-merge pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile fused-interleave-merge pipeline");

    // Prove this run takes the fused all-Source interleave arm: an unseeded
    // interleave Merge whose every predecessor is a Source claims both
    // Source receivers, so both names land in the fused set. The seeded
    // sibling test, by contrast, has `interleave_seed` set and so produces
    // an empty fused set. This is the same predicate the dispatcher consults
    // at executor entry, so it can never drift from the runtime decision.
    let (dag, _) =
        clinker_exec::executor::PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let fused =
        clinker_plan::plan::execution::compute_merge_interleave_fused_sources(&dag, plan.config());
    assert!(
        fused.contains("sa") && fused.contains("sb"),
        "an unseeded all-Source interleave Merge must fuse both Sources \
         (so the fused merge_fused_interleave arm runs), got fused set: {fused:?}",
    );

    // sa: x×3 (one document); sb: x×2, y×1 (another document). The fused
    // Merge reconciles each Source's per-document close and forwards it, so
    // the Aggregate flushes per document: x=3 from sa's document, and x=2,
    // y=1 from sb's document — never a folded x=5.
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
        .expect("run fused-interleave-merge pipeline");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "the fused interleave Merge forwards each Source's per-document close, \
         so the Aggregate flushes per document (x=3 from sa; x=2,y=1 from sb), \
         not a folded x=5",
    );
}

/// The MATERIALIZED counterpart to
/// `fused_interleave_merge_forwards_per_source_document_close`. That test feeds
/// the fused Merge straight into a group-by Aggregate — a streaming edge, so the
/// reconciled boundaries flow through the bounded channel at the tail. Here a
/// passthrough Transform sits between the fused Merge and the Aggregate.
///
/// The Transform's sole upstream is the Merge (not a Source), so it is NOT a
/// fused `Source → Transform`, and a Transform is not a certified streaming
/// consumer kind. The fused Merge therefore installs no streaming sender and
/// instead admits its records and reconciled per-document boundaries into a
/// materialized node buffer — the `FusedMergeOutput` materialized path — which
/// the Transform drains and forwards into its own node buffer. The downstream
/// group-by Aggregate is likewise materialized (its producer, the non-fused
/// Transform, is not a streaming producer), so it drains that buffer and
/// flushes per document.
///
/// This is the regression guard for the materialized fused-interleave boundary
/// path. If the fused Merge dropped its reconciled closes when admitting the
/// node buffer, no `DocumentClose` would reach the Aggregate until end-of-input
/// and both documents would fold into a single `x,5` + `y,1`. Forwarding them
/// splits the fold per document — `x,3` from sa and `x,2` + `y,1` from sb.
#[test]
fn fused_interleave_merge_materialized_consumer_preserves_document_boundaries() {
    let yaml = r#"
pipeline:
  name: fused_interleave_materialized_agg
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
  - type: transform
    name: pass
    input: m
    config:
      cxl: |
        emit category = category
  - type: aggregate
    name: by_category
    input: pass
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
    let config = parse_config(yaml).expect("parse fused-interleave-materialized pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile fused-interleave-materialized pipeline");

    // The Merge fuses both Sources (unseeded all-Source interleave), so the
    // fused arm runs regardless of what sits downstream of it.
    let (dag, _) =
        clinker_exec::executor::PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let fused =
        clinker_plan::plan::execution::compute_merge_interleave_fused_sources(&dag, plan.config());
    assert!(
        fused.contains("sa") && fused.contains("sb"),
        "an unseeded all-Source interleave Merge must fuse both Sources, got: {fused:?}",
    );

    // Prove the fused Merge takes the MATERIALIZED output path, not the
    // streaming one: a passthrough Transform is not a streaming consumer kind,
    // so the Merge admits a charged inter-stage node-buffer slot (which the
    // explain renders as an edge) rather than installing a streaming sender.
    // The streaming sibling test asserts the ABSENCE of such an edge.
    let explain = dag.explain_text(&config);
    assert!(
        explain.contains("edge merge.m -> transform.pass"),
        "the fused Merge must feed the Transform through a materialized \
         node-buffer edge (the FusedMergeOutput materialized path), got:\n{explain}",
    );

    // sa: x×3 (one document); sb: x×2, y×1 (another document). The fused Merge
    // reconciles each Source's per-document close and admits it into the
    // materialized node buffer; the Transform forwards it; the Aggregate
    // flushes per document — x=3 from sa's document, x=2 and y=1 from sb's,
    // never a folded x=5.
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
        .expect("run fused-interleave-materialized pipeline");
    assert_eq!(report.counters.dlq_count, 0);

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "the materialized fused interleave Merge must forward each Source's \
         per-document close through the node buffer, so the Aggregate flushes \
         per document (x=3 from sa; x=2,y=1 from sb), not a folded x=5",
    );
}
