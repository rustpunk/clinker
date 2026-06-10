//! Integration tests for the Source → Transform fusion fast path
//! (issue #74).
//!
//! When a `PlanNode::Transform`'s sole upstream is a `PlanNode::Source`
//! with a parked receiver in `ExecutorContext::source_records`, the
//! Transform takes ownership of the receiver and drives `recv()`
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
use std::io::{Cursor, Write};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clinker_bench_support::io::{SharedBuffer, fast_reader, slow_reader};
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
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
///   `recv()` without tripping the Source arm's
///   "no ingested records" defense-in-depth `Internal` error.
/// - Schema canonicalization, `seed_record_vars`, and
///   `seed_source_vars_for_record` run inside the fused loop so the
///   Transform's projection still sees the same Source-stamped fields
///   the buffered path produces.
/// - DLQ counters land on the upstream Source's name (verified via
///   `report.counters.dlq_count` being zero — no record should DLQ on
///   the well-formed input).
#[test]
fn fused_transform_streams_source_records_correctly() {
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
      include_unmapped: false
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
        clinker_exec::executor::SourceInput::Files(vec![slow_slot(
            "in",
            &csv,
            Duration::from_millis(25),
        )]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
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

/// A fused `Source → Transform → Output` over an envelope-bearing XML
/// document keeps document context flowing through the fused arm.
///
/// The Source ingest path emits a `DocumentOpen` punctuation before the
/// first body record and a `DocumentClose` after the last. The fused
/// Transform arm consumes the live Source channel directly; it must
/// forward those punctuations onto its output rather than dropping them,
/// so a document-scoped downstream operator sees the same boundaries it
/// would on the non-fused path. This test pins the user-observable half
/// of that contract: each body record's `$doc.<section>.<field>`
/// resolves correctly through the single fused Transform (the head
/// section `BatchInfo` and the tail section `Summary`, the latter proving
/// the pre-scan pulled the trailer up front). A regression that dropped
/// the document boundary — or mis-canonicalized the record's document
/// context under streaming consumption — would blank these fields.
///
/// The substrate-level ordering guarantee (a document's `DocumentClose`
/// trailing its records across batch splits, and two documents keeping
/// their boundaries separate) is pinned directly in
/// `executor::batch_handoff`'s unit tests.
#[test]
fn fused_transform_preserves_document_context() {
    let yaml = r#"
pipeline:
  name: transform_stream_fusion_envelope
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
            fields:
              batch_id: string
          Summary:
            extract: { xml_path: "/doc/Summary" }
            fields:
              total: int
      schema:
        - { name: amount, type: int }
  - type: transform
    name: tag
    input: payments
    config:
      cxl: |
        emit amount = amount
        emit batch = $doc.BatchInfo.batch_id
        emit declared_total = $doc.Summary.total
  - type: output
    name: out
    input: tag
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let doc_xml = r#"<doc>
  <BatchInfo><batch_id>RUN-042</batch_id></BatchInfo>
  <records>
    <record><amount>10</amount></record>
    <record><amount>20</amount></record>
    <record><amount>30</amount></record>
  </records>
  <Summary><total>3</total></Summary>
</doc>"#;

    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let file = FileSlot::new(
        PathBuf::from("payments.xml"),
        Box::new(Cursor::new(doc_xml.as_bytes().to_vec())),
    );
    let readers: SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![file]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("fused Transform over an envelope document executes");
    assert_eq!(report.counters.total_count, 3, "three body records");
    assert_eq!(report.counters.dlq_count, 0, "no records should DLQ");

    let lines = body_lines(&buf);
    assert_eq!(lines.len(), 3, "expected 3 body rows, got {lines:?}");
    for (i, row) in lines.iter().enumerate() {
        assert!(
            row.contains("RUN-042"),
            "row {i} lost $doc.BatchInfo.batch_id through the fused Transform: {row}"
        );
        assert!(
            row.contains(",3"),
            "row {i} lost $doc.Summary.total through the fused Transform: {row}"
        );
    }
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
#[test]
fn windowed_transform_keeps_buffered_path() {
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
      include_unmapped: false
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
        clinker_exec::executor::SourceInput::Files(vec![slow_slot(
            "in",
            csv,
            Duration::from_millis(5),
        )]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
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
#[test]
fn two_branch_fused_transforms_run_without_serializing() {
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
            clinker_exec::executor::SourceInput::Files(vec![slow_slot(
                "a",
                &a_csv,
                Duration::from_millis(50),
            )]),
        ),
        (
            "src_b".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![fast_slot("b", &b_csv)]),
        ),
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

    // Slow branch's drain is ~250 ms. Total pipeline runtime must stay well
    // under the worst-case serialization of two slow drains (~500 ms). The
    // fused/serial separation is only ~1.4x, narrower than the several-fold
    // inflation CI runners (macOS especially) impose on short sleeps, so a
    // fixed bound cannot hold on both platforms. Calibrate to this platform's
    // measured sleep cost so the assertion tests the separation, not absolute
    // wall time.
    let cal = Instant::now();
    std::thread::sleep(Duration::from_millis(50));
    let slack = (cal.elapsed().as_secs_f64() / 0.050).max(1.0);
    let bound = Duration::from_secs_f64(0.700 * slack);
    assert!(
        elapsed < bound,
        "pipeline took {elapsed:?} (slack {slack:.1}x, bound {bound:?}) — \
         two-branch fused transforms appear to be serializing on the slow \
         Source's drain. With fusion the slow branch's transform_a arm \
         consumes src_a's live channel without blocking src_b's parallel ingest."
    );
}

/// A fused `Source → Transform → Output` chain must NOT charge the
/// Transform's full output against the memory limit. The Transform
/// streams each bounded batch to the Output thread over a back-pressured
/// channel and admits no `node_buffers` slot, so peak inter-stage memory
/// is one batch rather than the whole stage.
///
/// The user-observable proof is the buffer classification the dispatcher
/// and `--explain` share: the Transform reports `buffer: streaming`, and
/// no `node_buffer` edge is emitted out of it — a materialized Transform
/// would report `buffer: materialized` and a `node_buffer (slot=N)` edge
/// to the Output. Because the explain classification is derived from the
/// exact `certify_streaming_edge` predicate the runtime sender-install
/// consults, this annotation reflects the dispatcher's runtime behavior.
#[test]
fn fused_transform_streams_to_output_without_charging_full_stage() {
    let yaml = r#"
pipeline:
  name: transform_stream_fusion_no_charge
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
      include_unmapped: false
"#;
    let config = parse_config(yaml).expect("parse_config");
    let compiled = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");
    let (dag, _) = PipelineExecutor::explain_plan_dag(&compiled).expect("explain_dag");
    let explain = dag.explain_text(&config);

    // The fused Transform is streaming: its output never crosses a
    // charged `node_buffers` slot.
    let transform_section = explain
        .split("transform.rename:")
        .nth(1)
        .expect("explain output names transform.rename");
    // `split` leaves the text right after the marker, which begins with a
    // newline; skip leading blank lines, then take the indented property
    // block up to the next blank-line separator.
    let transform_block: String = transform_section
        .lines()
        .skip_while(|l| l.trim().is_empty())
        .take_while(|l| !l.trim().is_empty())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        transform_block.contains("buffer: streaming"),
        "fused Source→Transform→Output should classify the Transform as \
         streaming (no charged node_buffers slot), got:\n{transform_block}"
    );
    assert!(
        !transform_block.contains("buffer: materialized"),
        "fused Transform must not be materialized:\n{transform_block}"
    );

    // No buffer edge leaves the streaming Transform. A materialized
    // Transform would emit `edge transform.rename -> output.out` with a
    // `node_buffer (slot=N)` line; its absence proves the Transform's
    // full output is never admitted as a charged slot.
    assert!(
        !explain.contains("edge transform.rename -> output.out"),
        "a streaming fused Transform must emit no node_buffer edge to its \
         Output (the whole point: no full-stage charge), got:\n{explain}"
    );
}

/// A fused `Source → Transform → Output` over a multi-document Source
/// keeps each document's records and boundaries flowing in arrival order
/// through the streaming handoff. With a small `batch_size` the documents'
/// records split across several batches, exercising the trailing-close
/// invariant at the dispatch level (the substrate's unit tests pin it in
/// isolation; this pins it end-to-end through the live streaming sender).
///
/// Each row's `$doc.batch_id` must resolve to its own document's batch id,
/// proving no document's context bled into another across the batch
/// splits the streaming handoff introduces.
#[test]
fn fused_transform_streams_multiple_documents_in_order() {
    let yaml = r#"
pipeline:
  name: transform_stream_fusion_multidoc
  batch_size: 2
nodes:
  - type: source
    name: payments
    config:
      name: payments
      type: xml
      glob: ./*.xml
      options:
        record_path: doc/records/record
      envelope:
        sections:
          BatchInfo:
            extract: { xml_path: "/doc/BatchInfo" }
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
    // Two documents, each with several records, fed through one glob
    // Source into a single fused Transform. `batch_size: 2` forces each
    // document's records to span multiple batches.
    let doc_a = r#"<doc>
  <BatchInfo><batch_id>RUN-A</batch_id></BatchInfo>
  <records>
    <record><amount>10</amount></record>
    <record><amount>11</amount></record>
    <record><amount>12</amount></record>
  </records>
</doc>"#;
    let doc_b = r#"<doc>
  <BatchInfo><batch_id>RUN-B</batch_id></BatchInfo>
  <records>
    <record><amount>20</amount></record>
    <record><amount>21</amount></record>
    <record><amount>22</amount></record>
  </records>
</doc>"#;

    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let files = vec![
        FileSlot::new(
            PathBuf::from("a.xml"),
            Box::new(Cursor::new(doc_a.as_bytes().to_vec())),
        ),
        FileSlot::new(
            PathBuf::from("b.xml"),
            Box::new(Cursor::new(doc_b.as_bytes().to_vec())),
        ),
    ];
    let readers: SourceReaders = HashMap::from([(
        "payments".to_string(),
        clinker_exec::executor::SourceInput::Files(files),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("fused Transform over multiple documents executes");
    assert_eq!(report.counters.total_count, 6, "six body records");
    assert_eq!(report.counters.dlq_count, 0, "no records should DLQ");

    let lines: Vec<String> = body_lines(&buf)
        .into_iter()
        .map(|l| l.trim_end_matches('\r').to_string())
        .collect();
    assert_eq!(lines.len(), 6, "expected 6 body rows, got {lines:?}");

    // Each record carries its OWN document's batch id — amounts in the
    // 10s belong to RUN-A, amounts in the 20s to RUN-B. A document-context
    // leak across the streaming batch splits would mislabel a row.
    for row in &lines {
        let amount: i64 = row
            .split(',')
            .next()
            .and_then(|s| s.parse().ok())
            .unwrap_or_else(|| panic!("row has no leading amount: {row}"));
        let expected_batch = if amount < 20 { "RUN-A" } else { "RUN-B" };
        assert!(
            row.contains(expected_batch),
            "row {row} should carry {expected_batch} ($doc.BatchInfo.batch_id \
             bled across documents through the streaming handoff)"
        );
    }
}

/// A fused `Source → Transform → Output` chain's charged footprint stays
/// bounded to the in-flight working set — never the whole stage output.
///
/// The producer `add_bytes` each flushed batch into its slot's charge
/// handle and the writer thread `sub_bytes` each consumed record, so the
/// arbitrator's summed charged bytes track only what is buffered between
/// the bounded source-ingest channel, the bounded streaming-output
/// channel, and one in-flight batch. That in-flight bound is a constant
/// fixed by the two channel capacities (1024 + 256 records) and the batch
/// size — independent of the total record count. The high-water mark
/// (`report.peak_consumer_usage_bytes`) therefore stays an arbitrarily
/// small fraction of the full-stage estimate as the input grows: a
/// materialized admit would charge all `ROWS` records as one slot, which
/// scales with the input, while the streaming charge does not.
#[test]
fn fused_transform_charges_one_batch_not_full_stage() {
    // Large enough that the constant in-flight bound is a small fraction
    // of the full-stage estimate (which scales with ROWS).
    const ROWS: usize = 100_000;
    // Conservative bytes-per-row lower bound for this 2-column schema; the
    // real per-row cost (`record_byte_cost`) exceeds this, so the
    // full-stage estimate computed from it is a safe lower bound.
    const PER_ROW_LOWER: usize = 48;
    // Generous bytes-per-row upper bound for the in-flight ceiling.
    const PER_ROW_UPPER: usize = 256;
    // Bounded in-flight capacity, summed across the two registered memory
    // consumers whose live charge the arbitrator peaks over:
    //   - the source ingest channel, `crossbeam_channel::bounded(1024)` —
    //     its `SourceConsumer` charge is `queued * per-record-bytes`, capped at
    //     the channel's 1024-event depth;
    //   - the streaming-output slot, `crossbeam_channel::bounded(256)` — the
    //     fused Transform `add_bytes` a whole batch before sending its events,
    //     so the slot's charged-but-undischarged footprint is at most the
    //     channel's 256-event depth plus the one batch (`batch_size = 64`)
    //     charged ahead of the blocking sends.
    // Hence the tight ceiling is input_cap + output_cap + one real batch =
    // 1024 + 256 + 64, independent of total input size. The last term is the
    // fixture's `batch_size` (64), not the output channel depth — the channel
    // depth is already the 256 term and must not be double-counted as a batch.
    const IN_FLIGHT_RECORDS: usize = 1024 + 256 + 64;

    let yaml = r#"
pipeline:
  name: transform_stream_fusion_one_batch
  batch_size: 64
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
      include_unmapped: false
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let mut csv = String::from("id,tag\n");
    for i in 1..=ROWS {
        csv.push_str(&format!("{i},row-{i}\n"));
    }
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![fast_slot("in", &csv)]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("fused stream executes");
    assert_eq!(report.counters.dlq_count, 0, "no records should DLQ");
    assert_eq!(
        body_lines(&buf).len(),
        ROWS,
        "every record must reach the streaming Output"
    );

    // The streaming charge must have fired (a fully materialized pipeline
    // would leave this at 0).
    assert!(
        report.peak_consumer_usage_bytes > 0,
        "streaming per-batch charge never sampled the arbitrator peak"
    );

    // The peak charged footprint stays bounded to the in-flight working
    // set — the two bounded channels plus one batch — never the whole
    // stage. With ROWS far larger than the in-flight bound, the full-stage
    // estimate dwarfs the peak.
    let full_stage_lower = (ROWS * PER_ROW_LOWER) as u64;
    let in_flight_ceiling = (IN_FLIGHT_RECORDS * PER_ROW_UPPER) as u64;
    assert!(
        report.peak_consumer_usage_bytes <= in_flight_ceiling,
        "fused Transform charged {} bytes at peak — expected <= {} \
         (source channel + output channel + one batch); per-batch \
         admit/discharge is not bounding the charge to the in-flight set",
        report.peak_consumer_usage_bytes,
        in_flight_ceiling,
    );
    assert!(
        report.peak_consumer_usage_bytes * 4 < full_stage_lower,
        "fused Transform peak charge {} is not well under the full-stage \
         estimate {} — the stage appears to charge its whole output at once",
        report.peak_consumer_usage_bytes,
        full_stage_lower,
    );
}
