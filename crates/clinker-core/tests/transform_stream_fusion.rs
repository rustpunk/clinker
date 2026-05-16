//! Integration tests for the Source → Transform fusion fast path
//! (issue #74).
//!
//! When a `PlanNode::Transform`'s sole upstream is a `PlanNode::Source`
//! with a parked receiver in `ExecutorContext::source_records`, the
//! Transform takes ownership of the receiver and drives `recv().await`
//! per-record evaluation directly. The Source dispatch arm short-
//! circuits via the same `fused_sources` membership check that
//! `merge_fused_interleave` uses (issue #67 pattern).
//!
//! These tests pin two pieces of behavior:
//!
//! 1. A `src (slow reader) → transform → out` pipeline produces the
//!    correct output. If the fusion plumbing has misrouted the Source's
//!    receiver — e.g. the Source arm runs even though `fused_sources`
//!    contains the name, or the Transform arm doesn't take the receiver
//!    out of `source_records` — the Source arm's defense-in-depth
//!    `Internal` error at `dispatch.rs` fires loudly. Passing the test
//!    proves fusion is wired end-to-end.
//!
//! 2. A windowed Transform (`window:` block on the parent transform)
//!    is NOT eligible for fusion and continues to consume a pre-drained
//!    Vec from the upstream Source's `node_buffers` entry. The
//!    eligibility predicate in `compute_transform_fused_sources` is
//!    what enforces this; the test verifies the predicate's exclusion
//!    by running a windowed transform against a slow source and
//!    asserting the windowed output is still well-formed.

use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clinker_bench_support::io::{SharedBuffer, fast_reader, slow_reader};
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_core::source::multi_file::FileSlot;

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

fn writer(buf: &SharedBuffer) -> Box<dyn Write + Send> {
    Box::new(buf.clone())
}

fn slow_slot(name: &str, csv: &str, delay: Duration) -> FileSlot {
    FileSlot::new(
        PathBuf::from(format!("{name}.csv")),
        slow_reader(csv, delay),
    )
}

fn fast_slot(name: &str, csv: &str) -> FileSlot {
    FileSlot::new(PathBuf::from(format!("{name}.csv")), fast_reader(csv))
}

fn body_lines(buf: &SharedBuffer) -> Vec<String> {
    buf.as_string().lines().skip(1).map(String::from).collect()
}

/// Single-branch `Source → Transform → Output` pipeline whose Transform
/// has nothing exotic about it (no `window:`, no Route fan-out, no
/// composition body). Eligible for the fused streaming path.
///
/// A 25 ms-per-row slow reader on the Source verifies that:
/// - The fused Transform arm takes the receiver out of
///   `ctx.source_records` and drives per-record evaluation via
///   `recv().await` without tripping the Source arm's
///   "no ingested records" defense-in-depth `Internal` error.
/// - Schema canonicalization, `seed_record_vars`, and
///   `seed_source_vars_for_record` run inside the fused loop so the
///   Transform's projection still sees the same Source-stamped fields
///   the buffered path produces.
/// - DLQ counters land on the upstream Source's name (verified via
///   `report.counters.dlq_count` being zero — no record should DLQ on
///   the well-formed input).
#[tokio::test(flavor = "multi_thread")]
async fn fused_transform_streams_source_records_correctly() {
    let yaml = r#"
pipeline:
  name: transform_stream_fusion_smoke
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: transform
    name: rename
    input: src
    config:
      cxl: |
        emit id = id
        emit label = tag
  - type: output
    name: out
    input: rename
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let mut csv = String::from("id,tag\n");
    for i in 1..=8 {
        csv.push_str(&format!("{i},row-{i}\n"));
    }
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        vec![slow_slot("in", &csv, Duration::from_millis(25))],
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("pipeline executes under fused Transform arm");
    assert_eq!(report.counters.dlq_count, 0, "no records should DLQ");

    let lines = body_lines(&buf);
    assert_eq!(lines.len(), 8, "expected 8 records, got {lines:?}");
    // Per-source FIFO preserved inside the fused loop.
    let expected: Vec<String> = (1..=8).map(|i| format!("{i},row-{i}")).collect();
    let actual: Vec<String> = lines
        .iter()
        .map(|l| l.trim_end_matches('\r').to_string())
        .collect();
    assert_eq!(
        actual, expected,
        "fused Transform output diverged from input order — \
         per-record canonicalize/eval is supposed to preserve the \
         upstream Source's row FIFO under streaming consumption"
    );
}

/// A windowed Transform (`window:` block) must NOT take the fused
/// streaming path: the windowed evaluator (`evaluate_single_transform_windowed`)
/// indexes records into the upstream operator's already-materialized
/// arena via the row's enumerate position. Streaming consumption has
/// no upstream arena yet at the moment the per-record eval would
/// dereference it, so the eligibility predicate excludes
/// `window_index: Some(_)` Transforms.
///
/// This test exercises a windowed Transform over a slow source and
/// asserts the output is the well-formed windowed-aggregate result.
/// If the predicate ever regressed to accept windowed Transforms, the
/// fused arm would dereference an unbuilt arena and either panic or
/// produce nonsense; this test would surface that.
#[tokio::test(flavor = "multi_thread")]
async fn windowed_transform_keeps_buffered_path() {
    let yaml = r#"
pipeline:
  name: transform_stream_fusion_windowed
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: int }
        - { name: grp, type: string }
        - { name: amt, type: int }
  - type: transform
    name: enrich
    input: src
    config:
      analytic_window:
        group_by: [grp]
      cxl: |
        emit id = id
        emit grp = grp
        emit amt = amt
        emit running = $window.sum(amt)
  - type: output
    name: out
    input: enrich
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let csv = "id,grp,amt\n\
        1,a,10\n\
        2,a,20\n\
        3,b,5\n\
        4,a,30\n\
        5,b,15\n";
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        vec![slow_slot("in", csv, Duration::from_millis(5))],
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("windowed pipeline executes via buffered path");
    assert_eq!(report.counters.dlq_count, 0);

    let mut lines: Vec<String> = body_lines(&buf)
        .into_iter()
        .map(|l| l.trim_end_matches('\r').to_string())
        .collect();
    // The window evaluator's emit order is partition-stable but the
    // cross-partition order depends on the runtime's partition
    // iteration; sort by id to make the assertion independent of that.
    lines.sort_by_key(|l| {
        l.split(',')
            .next()
            .and_then(|s| s.parse::<u32>().ok())
            .unwrap_or(u32::MAX)
    });
    assert_eq!(
        lines,
        vec![
            "1,a,10,60",
            "2,a,20,60",
            "3,b,5,20",
            "4,a,30,60",
            "5,b,15,20",
        ],
        "windowed partition-sum diverged — either the fused path is \
         incorrectly being applied to a windowed Transform (which would \
         dereference an unbuilt arena), or window evaluation broke \
         under a slow-reader input. Group `a` records (ids 1/2/4) each \
         see partition-sum 60; group `b` records (ids 3/5) each see 20."
    );
}

/// Two independent `Source → Transform → Output` branches in one
/// pipeline. Each Source has its own slot; each Transform's upstream
/// is a single Source (fusion-eligible). The pipeline runs as a
/// whole; we measure total wall-clock and verify the slow branch's
/// drain does not multiply by the fast branch's record count (which
/// it would if the dispatcher serialized the two branches' Source
/// drains).
///
/// With fusion: src_a's arm skips; transform_a streams over src_a's
/// receiver for ~250 ms; src_b's arm skips; transform_b streams over
/// src_b's receiver in ~5 μs since src_b's ingest task produced all
/// records into the channel during transform_a's tenure. Total wall-
/// clock is bounded by transform_a's drain.
///
/// The discriminating bound: total runtime stays comfortably under
/// 700 ms (the sum of the two streaming arm runtimes would be
/// ≤ 250 ms + 5 μs; serialized non-fused would still be the same
/// magnitude because of concurrent source ingest, so this test is a
/// regression guard rather than a fusion benefit proof). Wall-clock
/// proof of the per-record latency claim from issue #74's AC#4
/// requires either streaming Output (issue #72) or parallel branch
/// dispatch — neither is in scope for #74, but both are unblocked by
/// the per-record streaming Transform arm landed here.
#[tokio::test(flavor = "multi_thread")]
async fn two_branch_fused_transforms_run_without_serializing() {
    let yaml = r#"
pipeline:
  name: transform_stream_fusion_two_branch
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: transform
    name: t_a
    input: src_a
    config:
      cxl: |
        emit id = id
        emit tag = tag
  - type: transform
    name: t_b
    input: src_b
    config:
      cxl: |
        emit id = id
        emit tag = tag
  - type: output
    name: out_a
    input: t_a
    config:
      name: out_a
      type: csv
      path: out_a.csv
  - type: output
    name: out_b
    input: t_b
    config:
      name: out_b
      type: csv
      path: out_b.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let mut a_csv = String::from("id,tag\n");
    for i in 1..=5 {
        a_csv.push_str(&format!("{i},a-{i}\n"));
    }
    let mut b_csv = String::from("id,tag\n");
    for i in 1..=5 {
        b_csv.push_str(&format!("{i},b-{i}\n"));
    }
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            vec![slow_slot("a", &a_csv, Duration::from_millis(50))],
        ),
        ("src_b".to_string(), vec![fast_slot("b", &b_csv)]),
    ]);
    let buf_a = SharedBuffer::new();
    let buf_b = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([
        ("out_a".to_string(), writer(&buf_a)),
        ("out_b".to_string(), writer(&buf_b)),
    ]);

    let start = Instant::now();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("pipeline executes");
    let elapsed = start.elapsed();

    assert_eq!(report.counters.dlq_count, 0);

    let a_lines: Vec<String> = body_lines(&buf_a)
        .into_iter()
        .map(|l| l.trim_end_matches('\r').to_string())
        .collect();
    let b_lines: Vec<String> = body_lines(&buf_b)
        .into_iter()
        .map(|l| l.trim_end_matches('\r').to_string())
        .collect();
    assert_eq!(
        a_lines.len(),
        5,
        "src_a → t_a → out_a lost records: {a_lines:?}"
    );
    assert_eq!(
        b_lines.len(),
        5,
        "src_b → t_b → out_b lost records: {b_lines:?}"
    );
    let expected_a: Vec<String> = (1..=5).map(|i| format!("{i},a-{i}")).collect();
    let expected_b: Vec<String> = (1..=5).map(|i| format!("{i},b-{i}")).collect();
    assert_eq!(a_lines, expected_a);
    assert_eq!(b_lines, expected_b);

    // Slow branch's drain is ~250 ms. Total pipeline runtime must
    // stay well under the worst-case serialization of two slow drains
    // (~500 ms) — 700 ms gives generous CI jitter headroom while still
    // bounding the regression.
    assert!(
        elapsed < Duration::from_millis(700),
        "pipeline took {elapsed:?}, expected < 700 ms — \
         two-branch fused transforms appear to be serializing on the slow \
         Source's drain. With fusion the slow branch's transform_a arm \
         consumes src_a's live channel without blocking src_b's parallel ingest."
    );
}
