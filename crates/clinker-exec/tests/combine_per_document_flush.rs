//! Per-document Aggregate flush downstream of a Combine (join), across every
//! Combine strategy.
//!
//! A document-aware driver source (a multi-file CSV glob, each file its own
//! document) joins against a small build/lookup source, then a group-by
//! Aggregate rolls the joined records up. Correct document-boundary handling
//! forwards each driver document's `DocumentClose` through the Combine to the
//! Aggregate, which flushes that document's groups at its boundary — so a
//! multi-document run emits one group set per driver document rather than one
//! cross-document fold.
//!
//! The Combine forwards reconciled boundaries on EVERY strategy: the inline
//! hash build-probe arm and the streaming-probe arm (both certified streaming
//! producers, so the downstream Aggregate runs the streaming-ingest bucket
//! path), and the materialized IEJoin / grace-hash / sort-merge arms (blocking
//! producers → the materialized Aggregate bucket path). The inline and
//! streaming-probe tests are the load-bearing ones — they exercise the
//! forwarded-boundary path that previously dropped punctuations; the
//! materialized-strategy tests are regression guards proving no break.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// Build a driver `SourceInput::Files` from in-memory bodies (each file a
/// distinct document) plus a single-file build/lookup reader, run the plan,
/// and return the sorted output body lines and the dlq count.
fn run_combine(
    yaml: &str,
    driver_name: &str,
    driver_files: &[(&str, &str)],
    build_name: &str,
    build_file: (&str, &str),
) -> (Vec<String>, u64) {
    let config = parse_config(yaml).expect("parse combine per-document pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile combine per-document pipeline");

    let slots: Vec<FileSlot> = driver_files
        .iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(*name),
                Box::new(Cursor::new(body.as_bytes().to_vec())),
            )
        })
        .collect();
    let mut readers: clinker_exec::executor::SourceReaders = HashMap::new();
    readers.insert(
        driver_name.to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    );
    readers.insert(
        build_name.to_string(),
        clinker_exec::executor::single_file_reader(
            build_file.0,
            Box::new(Cursor::new(build_file.1.as_bytes().to_vec())),
        ),
    );

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("run combine per-document pipeline");
    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    (body, report.counters.dlq_count)
}

/// Assert the Combine selected `expected_tag` — the `[combine:<tag>]` glyph
/// in the `--explain` DAG topology (`hash_build_probe`, `iejoin`,
/// `sort_merge`, `grace_hash`) — so each test forces the arm it claims to
/// exercise.
fn assert_combine_strategy(yaml: &str, expected_tag: &str) {
    let config = parse_config(yaml).expect("parse pipeline for explain");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline for explain");
    let (dag, _) = PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let explain = dag.explain_text(&config);
    assert!(
        explain.contains(&format!("[combine:{expected_tag}]")),
        "expected Combine strategy [combine:{expected_tag}], got explain:\n{explain}"
    );
}

/// Slice the indented `--explain` property block for the node whose stanza
/// starts with `slug:`, up to the next blank line.
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

/// Assert the driver `producer_slug` classifies `streaming` and emits no
/// charged `node_buffer` edge to `consumer_slug` — the positive proof that
/// the producer's output streams into the consumer (the streaming-probe
/// runtime arm) rather than crossing a materialized inter-stage buffer.
fn assert_streaming_into(yaml: &str, producer_slug: &str, consumer_slug: &str) {
    let config = parse_config(yaml).expect("parse pipeline for explain");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline for explain");
    let (dag, _) = PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let explain = dag.explain_text(&config);
    let block = properties_block(&explain, producer_slug);
    assert!(
        block.contains("buffer: streaming") && !block.contains("buffer: materialized"),
        "{producer_slug} should classify streaming, got:\n{block}"
    );
    assert!(
        !explain.contains(&format!("edge {producer_slug} -> {consumer_slug}")),
        "a streaming {producer_slug} must emit no charged node_buffer edge to \
         {consumer_slug}, got:\n{explain}"
    );
}

// ---------------------------------------------------------------------------
// Inline hash build-probe Combine (pure-equi) → per-document Aggregate.
// ---------------------------------------------------------------------------

/// Pure-equi join (default `HashBuildProbe`) of a document-aware driver glob
/// against a single-row lookup, then a group-by Aggregate. The build is a
/// single-row lookup that is NOT a fused `Source→Transform`, so the
/// streaming-probe edge does not certify and the Combine runs the
/// materialized inline hash build-probe arm.
const INLINE_EQUI_YAML: &str = r#"
pipeline:
  name: inline_combine_per_doc
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: category, type: string }
        - { name: k, type: string }
  - type: source
    name: lookup
    config:
      name: lookup
      type: csv
      path: ./lookup.csv
      schema:
        - { name: k, type: string }
        - { name: label, type: string }
  - type: combine
    name: j
    input:
      driver: driver
      lookup: lookup
    config:
      where: "driver.k == lookup.k"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit category = driver.category
        emit k = driver.k
  - type: aggregate
    name: by_category
    input: j
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
fn inline_combine_then_per_document_aggregate_flushes_per_document() {
    assert_combine_strategy(INLINE_EQUI_YAML, "hash_build_probe");

    // doc A: x×2, y×1. doc B: x×3. Every record's `k=1` matches the single
    // lookup row, so the join is a passthrough that preserves each driver
    // document. The Combine forwards each document's close to the Aggregate,
    // which buckets per document → x=2,y=1 from doc A and a SEPARATE x=3 from
    // doc B, not a folded x=5.
    let (body, dlq) = run_combine(
        INLINE_EQUI_YAML,
        "driver",
        &[
            ("a.csv", "category,k\nx,1\nx,1\ny,1\n"),
            ("b.csv", "category,k\nx,1\nx,1\nx,1\n"),
        ],
        "lookup",
        ("lookup.csv", "k,label\n1,one\n"),
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "an inline Combine must forward each driver document's close so the \
         downstream Aggregate flushes per document (x=2,y=1; x=3), not x=5"
    );
}

// ---------------------------------------------------------------------------
// Streaming-probe Combine (fused Source→Transform driver) → per-document Agg.
// ---------------------------------------------------------------------------

/// Pure-equi join whose driver is a fused `Source→Transform` — the only
/// producer kind that certifies the streaming-probe edge (a bare Source never
/// certifies). The driver streams its records (and its document-boundary
/// punctuations) over the bounded probe channel; the probe collects the
/// punctuations and reconciles them with the build side after the join.
const STREAMING_PROBE_EQUI_YAML: &str = r#"
pipeline:
  name: streaming_probe_combine_per_doc
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: category, type: string }
        - { name: k, type: string }
  - type: transform
    name: drive_t
    input: driver
    config:
      cxl: |
        emit category = category
        emit k = k
  - type: source
    name: lookup
    config:
      name: lookup
      type: csv
      path: ./lookup.csv
      schema:
        - { name: k, type: string }
        - { name: label, type: string }
  - type: combine
    name: j
    input:
      drive_t: drive_t
      lookup: lookup
    config:
      where: "drive_t.k == lookup.k"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit category = drive_t.category
        emit k = drive_t.k
  - type: aggregate
    name: by_category
    input: j
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
fn streaming_probe_combine_then_per_document_aggregate_flushes_per_document() {
    assert_combine_strategy(STREAMING_PROBE_EQUI_YAML, "hash_build_probe");
    // Positively prove the streaming-probe arm runs: the fused
    // `Source→Transform` driver classifies `buffer: streaming` and emits no
    // charged node_buffer edge into the Combine, so its records (and document
    // punctuations) reach the probe over the channel rather than a
    // materialized drain. Without this the correct per-document output would
    // be reachable via the materialized arm too, leaving the streaming-probe
    // punctuation-collection path unexercised.
    assert_streaming_into(STREAMING_PROBE_EQUI_YAML, "transform.drive_t", "combine.j");

    // Same per-document split as the inline test, but the driver streams over
    // the probe channel, so this exercises the channel-collected driver
    // punctuations reconciled with the build side after the join.
    let (body, dlq) = run_combine(
        STREAMING_PROBE_EQUI_YAML,
        "driver",
        &[
            ("a.csv", "category,k\nx,1\nx,1\ny,1\n"),
            ("b.csv", "category,k\nx,1\nx,1\nx,1\n"),
        ],
        "lookup",
        ("lookup.csv", "k,label\n1,one\n"),
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "a streaming-probe Combine must collect the driver's punctuations off \
         the channel and forward them so the Aggregate flushes per document"
    );
}

// ---------------------------------------------------------------------------
// Materialized strategy regression guards: IEJoin / GraceHash / SortMerge.
// ---------------------------------------------------------------------------

/// Pure-range join (`driver.v < lookup.cap`) with no `sort_order`, forcing
/// IEJoin. A single high-cap lookup row matches every driver record.
const IEJOIN_RANGE_YAML: &str = r#"
pipeline:
  name: iejoin_combine_per_doc
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: category, type: string }
        - { name: v, type: int }
  - type: source
    name: lookup
    config:
      name: lookup
      type: csv
      path: ./lookup.csv
      schema:
        - { name: cap, type: int }
        - { name: label, type: string }
  - type: combine
    name: j
    input:
      driver: driver
      lookup: lookup
    config:
      where: "driver.v < lookup.cap"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit category = driver.category
        emit v = driver.v
  - type: aggregate
    name: by_category
    input: j
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
fn iejoin_combine_preserves_document_boundaries() {
    assert_combine_strategy(IEJOIN_RANGE_YAML, "iejoin");

    // doc A: x×2, y×1 (v small). doc B: x×3. Every v < 100, so the single
    // lookup row matches all. The materialized IEJoin arm forwards reconciled
    // boundaries → per-document flush.
    let (body, dlq) = run_combine(
        IEJOIN_RANGE_YAML,
        "driver",
        &[
            ("a.csv", "category,v\nx,1\nx,2\ny,3\n"),
            ("b.csv", "category,v\nx,4\nx,5\nx,6\n"),
        ],
        "lookup",
        ("lookup.csv", "cap,label\n100,hi\n"),
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "a materialized IEJoin Combine must preserve document boundaries → \
         per-document flush (x=2,y=1; x=3)"
    );
}

/// Pure-equi join with a `strategy: grace_hash` hint on the Combine node,
/// forcing the disk-spilling grace-hash arm.
const GRACE_HASH_EQUI_YAML: &str = r#"
pipeline:
  name: grace_hash_combine_per_doc
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: category, type: string }
        - { name: k, type: string }
  - type: source
    name: lookup
    config:
      name: lookup
      type: csv
      path: ./lookup.csv
      schema:
        - { name: k, type: string }
        - { name: label, type: string }
  - type: combine
    name: j
    input:
      driver: driver
      lookup: lookup
    config:
      where: "driver.k == lookup.k"
      match: first
      on_miss: skip
      propagate_ck: driver
      strategy: grace_hash
      cxl: |
        emit category = driver.category
        emit k = driver.k
  - type: aggregate
    name: by_category
    input: j
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
fn grace_hash_combine_preserves_document_boundaries() {
    assert_combine_strategy(GRACE_HASH_EQUI_YAML, "grace_hash");

    let (body, dlq) = run_combine(
        GRACE_HASH_EQUI_YAML,
        "driver",
        &[
            ("a.csv", "category,k\nx,1\nx,1\ny,1\n"),
            ("b.csv", "category,k\nx,1\nx,1\nx,1\n"),
        ],
        "lookup",
        ("lookup.csv", "k,label\n1,one\n"),
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "a grace-hash Combine must preserve document boundaries → per-document \
         flush (x=2,y=1; x=3)"
    );
}

/// Single-range join with `sort_order` on both inputs' range key, forcing
/// SortMerge over IEJoin.
const SORT_MERGE_RANGE_YAML: &str = r#"
pipeline:
  name: sort_merge_combine_per_doc
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      sort_order:
        - field: v
      schema:
        - { name: category, type: string }
        - { name: v, type: int }
  - type: source
    name: lookup
    config:
      name: lookup
      type: csv
      path: ./lookup.csv
      sort_order:
        - field: cap
      schema:
        - { name: cap, type: int }
        - { name: label, type: string }
  - type: combine
    name: j
    input:
      driver: driver
      lookup: lookup
    config:
      where: "driver.v < lookup.cap"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit category = driver.category
        emit v = driver.v
  - type: aggregate
    name: by_category
    input: j
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
fn sort_merge_combine_preserves_document_boundaries() {
    assert_combine_strategy(SORT_MERGE_RANGE_YAML, "sort_merge");

    // Each file is pre-sorted on v (ascending). The lookup cap=100 matches
    // every driver record. The materialized SortMerge arm forwards reconciled
    // boundaries → per-document flush.
    let (body, dlq) = run_combine(
        SORT_MERGE_RANGE_YAML,
        "driver",
        &[
            ("a.csv", "category,v\nx,1\nx,2\ny,3\n"),
            ("b.csv", "category,v\nx,4\nx,5\nx,6\n"),
        ],
        "lookup",
        ("lookup.csv", "cap,label\n100,hi\n"),
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "a SortMerge Combine must preserve document boundaries → per-document \
         flush (x=2,y=1; x=3)"
    );
}

// ---------------------------------------------------------------------------
// Boundary-exactness: spanning doc, driver-only doc, no-document byte-identity.
// ---------------------------------------------------------------------------

/// Both the driver and the build input of an inline Combine carry one
/// single-file document each (the same bytes). These are NOT the same
/// document id — each source allocates its own `DocumentId` from the global
/// counter — but the join's output rows carry only the DRIVER's document id,
/// and only the driver's records populate a per-document bucket downstream.
/// The reconciliation forwards each input's one open + one close (open ==
/// close == 1 per input), so the driver's close flushes its bucket exactly
/// once and the build's separate close lands on an empty bucket (grouped
/// fold → no-op). The result is one group set, never a doubled one. (The
/// genuine same-id spanning case — one document id whose boundary arrives on
/// both join inputs — is covered by the `reconcile_*` unit tests in
/// `stream_event.rs`, which a fork-rejoin topology is not needed to exercise.)
const SPANNING_DOC_YAML: &str = r#"
pipeline:
  name: spanning_doc_combine
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      path: ./shared.csv
      schema:
        - { name: category, type: string }
        - { name: k, type: string }
  - type: source
    name: build
    config:
      name: build
      type: csv
      path: ./shared.csv
      schema:
        - { name: category, type: string }
        - { name: k, type: string }
  - type: combine
    name: j
    input:
      driver: driver
      build: build
    config:
      where: "driver.k == build.k"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit category = driver.category
        emit k = driver.k
  - type: aggregate
    name: by_category
    input: j
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
fn spanning_document_emits_exactly_one_open_and_one_close() {
    // Each join input carries one single-file document. The output rows carry
    // the driver's document id, so the driver's close flushes its bucket once;
    // the build input's separate close lands on an empty grouped bucket and is
    // a no-op. The per-document flush therefore fires exactly once, producing
    // one group set — a regression that double-fired a close would duplicate
    // it.
    let config = parse_config(SPANNING_DOC_YAML).expect("parse spanning-doc pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile spanning-doc pipeline");
    let mut readers: clinker_exec::executor::SourceReaders = HashMap::new();
    readers.insert(
        "driver".to_string(),
        clinker_exec::executor::single_file_reader(
            "shared.csv",
            Box::new(Cursor::new(b"category,k\nx,1\nx,1\ny,1\n".to_vec())),
        ),
    );
    readers.insert(
        "build".to_string(),
        clinker_exec::executor::single_file_reader(
            "shared.csv",
            Box::new(Cursor::new(b"category,k\nx,1\nx,1\ny,1\n".to_vec())),
        ),
    );
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("run spanning-doc pipeline");
    assert_eq!(report.counters.dlq_count, 0);
    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    // One flush over the driver's document: x=2, y=1 emitted exactly once (a
    // double-fired close would duplicate the group set).
    assert_eq!(
        body,
        vec!["x,2".to_string(), "y,1".to_string()],
        "the driver document must flush exactly once (x=2, y=1); the build's \
         separate close must not double-fire the per-document group set"
    );
}

/// A driver-only document (the build side is a disjoint lookup document) now
/// forwards its close, so its per-document Aggregate flushes — the core Fix-1
/// win. Uses the inline arm; the driver glob carries two documents, the build
/// is a separate single-row lookup document.
#[test]
fn driver_only_document_now_forwards_its_close() {
    assert_combine_strategy(INLINE_EQUI_YAML, "hash_build_probe");

    // Two driver documents, each carried only on the driver input; the build
    // lookup is its own disjoint document. Each driver document's open ==
    // close == 1, so the reconciliation forwards each close → per-document
    // flush. Before Fix 1, a one-sided document's close was withheld (it never
    // reached the old degree-2 threshold) and the documents folded.
    let (body, dlq) = run_combine(
        INLINE_EQUI_YAML,
        "driver",
        &[
            ("a.csv", "category,k\nx,1\nx,1\ny,1\n"),
            ("b.csv", "category,k\nx,1\nx,1\nx,1\n"),
        ],
        "lookup",
        ("lookup.csv", "k,label\n1,one\n"),
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["x,2".to_string(), "x,3".to_string(), "y,1".to_string()],
        "a driver-only document must forward its close so its per-document \
         Aggregate flushes (x=2,y=1; x=3), not fold to x=5"
    );
}

/// A Combine of two no-envelope single-file sources: the punctuation union is
/// empty (no document boundaries), so the reconciliation folds to empty and
/// the dominant no-document path is byte-identical to a single cross-document
/// aggregate.
const NO_DOCUMENT_YAML: &str = r#"
pipeline:
  name: no_document_combine
nodes:
  - type: source
    name: driver
    config:
      name: driver
      type: csv
      path: ./driver.csv
      schema:
        - { name: category, type: string }
        - { name: k, type: string }
  - type: source
    name: lookup
    config:
      name: lookup
      type: csv
      path: ./lookup.csv
      schema:
        - { name: k, type: string }
        - { name: label, type: string }
  - type: combine
    name: j
    input:
      driver: driver
      lookup: lookup
    config:
      where: "driver.k == lookup.k"
      match: first
      on_miss: skip
      propagate_ck: driver
      cxl: |
        emit category = driver.category
        emit k = driver.k
  - type: aggregate
    name: by_category
    input: j
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
fn no_document_combine_path_byte_identical() {
    // Two single-file no-envelope sources: each carries one file-level
    // document, but those documents are disjoint and each closes once, so the
    // join's records fold into a single per-driver-document aggregate. The
    // driver's lone document closes once → one flush over all its records:
    // x=3 (2 from the joined x rows + 1 more), y=1. This is the dominant
    // single-document shape; the punctuation handling adds no extra rows.
    let (body, dlq) = run_combine(
        NO_DOCUMENT_YAML,
        "driver",
        &[("driver.csv", "category,k\nx,1\nx,1\nx,1\ny,1\n")],
        "lookup",
        ("lookup.csv", "k,label\n1,one\n"),
    );
    assert_eq!(dlq, 0);
    assert_eq!(
        body,
        vec!["x,3".to_string(), "y,1".to_string()],
        "a single-document Combine folds its one driver document into one \
         aggregate (x=3, y=1), unchanged by the boundary handling"
    );
}
