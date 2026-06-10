//! Integration tests for the non-fused streaming operator arms.
//!
//! Beyond the fused `Source → Transform → Output` path (covered in
//! `transform_stream_fusion.rs`), four more operator kinds hand their
//! output straight to a single downstream sink `Output` over the bounded
//! crossbeam channel instead of crossing a charged `node_buffers` slot:
//!
//! - single-branch `Route`,
//! - non-fused `Merge` (concat, or interleave with non-Source inputs),
//! - `streaming`-strategy `Aggregate` (planner-certified pre-sorted
//!   input),
//! - `Combine` probe-side (hash build-probe; the build side stays
//!   materialized).
//!
//! Each test pins two properties:
//!
//! 1. **Memory model.** The producer reports `buffer: streaming` in
//!    `--explain` and emits NO `node_buffer` edge to its Output. Because
//!    the explain classifier is derived from the exact
//!    `certify_streaming_edge` predicate the runtime sender-install
//!    consults, the annotation reflects what the dispatcher does: the
//!    producer's output is not admitted to a charged, spill-eligible slot
//!    that the Output would re-drain.
//! 2. **Equivalence.** The streamed output is the same record set, with
//!    no loss or duplication and no spurious dead-letters, that the
//!    materialized path would have produced.
//!
//! The non-fused `Merge` test additionally drives two documents through a
//! small `batch_size` so the document-boundary punctuations the Merge
//! forwards (`deduped_puncts`) span several batches — pinning that the
//! streaming handoff preserves per-document ordering end-to-end. (The
//! substrate's trailing-`DocumentClose`-across-batch-splits invariant is
//! pinned in isolation by `executor::batch_handoff`'s unit tests.)
//!
//! A second family covers the streaming-ingest *consumer*: an eligible
//! producer streams record-at-a-time into an `Aggregate`'s `add_record`
//! over the bounded channel rather than the Aggregate pre-draining the
//! producer's whole output from a charged `node_buffers` slot. The
//! producer runs inside the Aggregate's dispatch turn (its own topo turn a
//! no-op), streaming each batch while a scoped thread drives ingest; the
//! Aggregate finalizes on channel disconnect. These tests pin the same two
//! properties — the producer reports `buffer: streaming` and emits no
//! `node_buffer` edge to the Aggregate, and the aggregated value matches
//! the materialized path — across `Source → Transform → Aggregate` and
//! `Merge → Aggregate` shapes, plus a back-pressure regression that drives
//! far more records than the bounded channel holds (a deadlock would hang
//! the run rather than complete).

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::path::PathBuf;

use clinker_bench_support::io::{SharedBuffer, fast_reader};
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

fn fast_slot(name: &str, csv: &str) -> FileSlot {
    FileSlot::new(PathBuf::from(format!("{name}.csv")), fast_reader(csv))
}

fn body_lines(buf: &SharedBuffer) -> Vec<String> {
    buf.as_string()
        .lines()
        .skip(1)
        .map(|l| l.trim_end_matches('\r').to_string())
        .collect()
}

/// Slice the indented property block for the node whose `--explain`
/// stanza starts with `slug:` (e.g. `route.r:`), up to the next blank
/// line. The `=== Physical Properties ===` stanzas are two-space-indented
/// and blank-line separated.
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

/// Assert the producer named `producer_slug` is classified `streaming`
/// and emits no charged `node_buffer` edge to `consumer_slug` — the
/// user-observable proof that its output never crosses a spill-eligible
/// inter-stage buffer. `consumer_slug` is any downstream sink: an
/// `output.*` writer or an `aggregation.*` streaming-ingest consumer.
fn assert_streaming_to_output(explain: &str, producer_slug: &str, consumer_slug: &str) {
    let block = properties_block(explain, producer_slug);
    assert!(
        block.contains("buffer: streaming"),
        "{producer_slug} should classify streaming, got:\n{block}"
    );
    assert!(
        !block.contains("buffer: materialized"),
        "{producer_slug} must not be materialized:\n{block}"
    );
    let edge = format!("edge {producer_slug} -> {consumer_slug}");
    assert!(
        !explain.contains(&edge),
        "a streaming {producer_slug} must emit no node_buffer edge to its \
         consumer (the whole point: no charged inter-stage slot), got:\n{explain}"
    );
}

fn explain_of(
    config: &clinker_plan::config::PipelineConfig,
    plan: &clinker_plan::plan::CompiledPlan,
) -> String {
    let (dag, _) = PipelineExecutor::explain_plan_dag(plan).expect("explain_dag");
    dag.explain_text(config)
}

/// A single-branch `Route` whose sole successor is a sink `Output`
/// streams the routed records to the writer thread with no loss, and
/// admits no charged `node_buffers` slot. A `true` catch-all condition
/// routes every record down the single `all` branch.
#[test]
fn single_branch_route_streams_to_output() {
    let yaml = r#"
pipeline:
  name: streaming_route
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: ./orders.csv
      options: { has_header: true }
      schema:
        - { name: id, type: string }
        - { name: amount, type: string }
  - type: route
    name: r
    input: orders
    config:
      mode: exclusive
      conditions:
        all: "true"
      default: all
  - type: output
    name: out
    input: r.all
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    assert_streaming_to_output(&explain_of(&config, &plan), "route.r", "output.out");

    let mut csv = String::from("id,amount\n");
    for i in 1..=50 {
        csv.push_str(&format!("o-{i},{}\n", i * 10));
    }
    let readers: SourceReaders = HashMap::from([(
        "orders".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![fast_slot("orders", &csv)]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("single-branch route streams");
    let lines = body_lines(&buf);
    assert_eq!(
        lines.len(),
        50,
        "route streaming lost/dup records: {lines:?}"
    );
    assert_eq!(report.counters.records_written, 50);
    assert_eq!(report.counters.dlq_count, 0);
}

/// A streaming stage flushes one batch at a time to disk on a
/// soft-threshold trip instead of materializing the whole stage. A
/// single-branch `Route → Output` streams its records through the slot's
/// per-batch charge handle, whose `should_spill()` poll fires per flushed
/// batch; under a 1 MiB budget the test process's own RSS crosses the
/// 80 % soft floor, so each batch's records round-trip through a
/// `SpillFile<u64>` (re-read and forwarded to the writer). The run
/// completes — the streaming path never polls the hard-limit
/// `should_abort`, so a tiny test budget spills rather than aborts, the
/// same posture the materialized `route_fanout_soft_spill` test relies on.
///
/// `backpressure: spill` is required because the budget is below the
/// process's baseline RSS: under the default `pause` policy such a budget
/// is rejected at startup (E312, the unsatisfiable-budget guard), so a
/// sub-baseline budget that intends to *spill* rather than *pause* must
/// select the spill policy, which never pauses a producer and so is not
/// rejected.
///
/// Asserts every record is delivered in order and
/// `cumulative_spill_bytes > 0`. The companion `route_fanout_soft_spill`
/// pins the blocking full-stage spill path; the unit test
/// `batch_handoff::charge_handle_spills_a_batch_preserving_order_and_close`
/// pins the per-batch spill round-trip in isolation.
///
/// Skipped silently when `rss_bytes()` is unavailable — the spill
/// predicate is RSS-based, so without it the path stays in memory.
#[test]
fn streaming_arm_soft_spills_under_one_megabyte_budget() {
    if clinker_exec::pipeline::memory::rss_bytes().is_none() {
        return;
    }

    const ROWS: usize = 4_000;
    let yaml = r#"
pipeline:
  name: streaming_route_soft_spill
  batch_size: 128
  memory: { limit: "1M", backpressure: spill }
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: ./orders.csv
      options: { has_header: true }
      schema:
        - { name: id, type: string }
        - { name: payload, type: string }
        - { name: amount, type: int }
  - type: route
    name: r
    input: orders
    config:
      mode: exclusive
      conditions:
        all: "true"
      default: all
  - type: output
    name: out
    input: r.all
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    // The Route is a streaming arm (single outgoing edge to the Output).
    assert_streaming_to_output(&explain_of(&config, &plan), "route.r", "output.out");

    let mut csv = String::from("id,payload,amount\n");
    for i in 1..=ROWS {
        csv.push_str(&format!("o-{i},payload_{i},{}\n", i * 10));
    }
    let readers: SourceReaders = HashMap::from([(
        "orders".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![fast_slot("orders", &csv)]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("streaming soft-spill run must complete under a 1 MiB budget");

    assert_eq!(
        report.counters.total_count as usize, ROWS,
        "total ingested row count must match the input"
    );
    assert_eq!(
        body_lines(&buf).len(),
        ROWS,
        "every record must reach the Output even after batch spills"
    );
    assert!(
        report.cumulative_spill_bytes > 0,
        "a streaming pipeline under a 1 MiB budget must spill at least one \
         batch; report.cumulative_spill_bytes = {}",
        report.cumulative_spill_bytes,
    );
}

/// A `Combine` resolving to the hash build-probe strategy whose sole
/// downstream is a sink `Output` streams its probe-side emit; the build
/// relation stays materialized in the hash table. Equi-join on
/// `product_id`, one matched row per driver order, with one miss
/// (`p3`) that `on_miss: skip` drops.
#[test]
fn hash_combine_probe_streams_to_output() {
    let yaml = r#"
pipeline:
  name: streaming_combine
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: ./orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: amount, type: int }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: ./products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
  - type: combine
    name: c
    input:
      orders: orders
      products: products
    config:
      where: "orders.product_id == products.product_id"
      match: first
      on_miss: skip
      drive: orders
      cxl: |
        emit order_id = orders.order_id
        emit product_name = products.name
      propagate_ck: driver
  - type: output
    name: out
    input: c
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    assert_streaming_to_output(&explain_of(&config, &plan), "combine.c", "output.out");

    let orders = "order_id,product_id,amount\no1,p1,5\no2,p2,6\no3,p1,7\no4,p3,8\n";
    let products = "product_id,name\np1,Prod1\np2,Prod2\n";
    let readers: SourceReaders = HashMap::from([
        (
            "orders".to_string(),
            clinker_exec::executor::single_file_reader(
                "orders.csv",
                Box::new(Cursor::new(orders.as_bytes().to_vec())),
            ),
        ),
        (
            "products".to_string(),
            clinker_exec::executor::single_file_reader(
                "products.csv",
                Box::new(Cursor::new(products.as_bytes().to_vec())),
            ),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("hash combine probe streams");
    let lines = body_lines(&buf);
    // o1->Prod1, o2->Prod2, o3->Prod1 match; o4 (p3) misses and is skipped.
    assert_eq!(
        lines.len(),
        3,
        "combine streaming wrong row count: {lines:?}"
    );
    assert_eq!(report.counters.dlq_count, 0);
}

/// A `streaming`-strategy `Aggregate` whose sole downstream is a sink
/// `Output` streams its finalized group rows. The upstream `distinct by`
/// destroys input ordering in a way the planner certifies lets the
/// global aggregate emit without buffering for a downstream arm
/// (`AggregateStrategy::Streaming`); the explain classifier must reflect
/// that the aggregate's output is streamed, not admitted to a charged
/// slot. The aggregate's value (one global `total` / `n`) must match the
/// materialized path exactly.
#[test]
fn streaming_aggregate_streams_to_output() {
    let yaml = r#"
pipeline:
  name: streaming_aggregate
nodes:
  - type: source
    name: sales
    config:
      name: sales
      type: csv
      path: ./sales.csv
      options: { has_header: true }
      schema:
        - { name: id, type: string }
        - { name: amount, type: string }
        - { name: status, type: string }
  - type: transform
    name: active_only
    input: sales
    config:
      cxl: |
        filter status == "active"
        distinct by id
        emit id = id
        emit amount = amount
        emit status = status
  - type: aggregate
    name: totals
    input: active_only
    config:
      group_by: []
      cxl: |
        emit total = sum(amount.to_int())
        emit n = count(*)
  - type: output
    name: out
    input: totals
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let explain = explain_of(&config, &plan);
    // The aggregate resolved to the streaming strategy (the
    // `distinct`-destroyed ordering certifies it) — if it had stayed hash
    // strategy this assertion catches the regression.
    assert!(
        explain.contains("[aggregation:streaming] totals"),
        "expected the global aggregate over distinct-destroyed ordering to \
         resolve to the streaming strategy, got:\n{explain}"
    );
    assert_streaming_to_output(&explain, "aggregation.totals", "output.out");

    // Five active rows (ids 1..=5), one duplicate id (5) deduped away, plus
    // one inactive row filtered out. Active distinct amounts: 10+20+30+40+50.
    let csv = "id,amount,status\n\
        1,10,active\n\
        2,20,active\n\
        3,30,active\n\
        4,40,active\n\
        5,50,active\n\
        5,50,active\n\
        6,99,inactive\n";
    let readers: SourceReaders = HashMap::from([(
        "sales".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![fast_slot("sales", csv)]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("streaming aggregate streams");
    let lines = body_lines(&buf);
    assert_eq!(lines.len(), 1, "global aggregate emits one row: {lines:?}");
    assert!(
        lines[0].contains("150") && lines[0].ends_with("5"),
        "aggregate value diverged — expected total=150, n=5, got: {}",
        lines[0]
    );
    assert_eq!(report.counters.dlq_count, 0);
}

/// A non-fused `Merge` (`concat` of two Transform outputs — predecessors
/// are Transforms, not Sources, so the fused-interleave path is not
/// taken) feeding a sink `Output` drains its predecessors' buffers into
/// the merged result and streams it. All records from both branches must
/// reach the writer with no loss, and the Merge must admit no charged
/// slot.
#[test]
fn nonfused_merge_concat_streams_to_output() {
    let yaml = r#"
pipeline:
  name: streaming_merge
nodes:
  - type: source
    name: sa
    config:
      name: sa
      type: csv
      path: ./sa.csv
      schema:
        - { name: id, type: string }
  - type: source
    name: sb
    config:
      name: sb
      type: csv
      path: ./sb.csv
      schema:
        - { name: id, type: string }
  - type: transform
    name: ta
    input: sa
    config:
      cxl: |
        emit id = id
  - type: transform
    name: tb
    input: sb
    config:
      cxl: |
        emit id = id
  - type: merge
    name: m
    inputs: [ta, tb]
    config:
      mode: concat
  - type: output
    name: out
    input: m
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    assert_streaming_to_output(&explain_of(&config, &plan), "merge.m", "output.out");

    let sa = "id\na1\na2\na3\n";
    let sb = "id\nb1\nb2\n";
    let readers: SourceReaders = HashMap::from([
        (
            "sa".to_string(),
            clinker_exec::executor::single_file_reader(
                "sa.csv",
                Box::new(Cursor::new(sa.as_bytes().to_vec())),
            ),
        ),
        (
            "sb".to_string(),
            clinker_exec::executor::single_file_reader(
                "sb.csv",
                Box::new(Cursor::new(sb.as_bytes().to_vec())),
            ),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("non-fused merge concat streams");
    let lines = body_lines(&buf);
    assert_eq!(
        lines.len(),
        5,
        "non-fused merge concat lost records: {lines:?}"
    );
    assert_eq!(report.counters.records_written, 5);
    assert_eq!(report.counters.dlq_count, 0);
}

/// A non-fused `Merge` over two envelope-bearing documents, streamed to a
/// sink `Output` under a small `batch_size`, keeps each document's
/// records and boundaries flowing in arrival order through the streaming
/// handoff. Each Source feeds a Transform (so the Merge is non-fused and
/// takes the concat/forward path that dedups and forwards the document
/// punctuations), and `batch_size: 2` forces each document's records to
/// span several batches. A document-context leak across the batch splits
/// the streaming handoff introduces would mislabel a row's
/// `$doc.BatchInfo.batch_id`.
#[test]
fn nonfused_merge_streams_documents_in_order() {
    let yaml = r#"
pipeline:
  name: streaming_merge_multidoc
  batch_size: 2
nodes:
  - type: source
    name: pa
    config:
      name: pa
      type: xml
      glob: ./a*.xml
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
  - type: source
    name: pb
    config:
      name: pb
      type: xml
      glob: ./b*.xml
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
    name: ta
    input: pa
    config:
      cxl: |
        emit amount = amount
        emit batch = $doc.BatchInfo.batch_id
  - type: transform
    name: tb
    input: pb
    config:
      cxl: |
        emit amount = amount
        emit batch = $doc.BatchInfo.batch_id
  - type: merge
    name: m
    inputs: [ta, tb]
    config:
      mode: concat
  - type: output
    name: out
    input: m
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    assert_streaming_to_output(&explain_of(&config, &plan), "merge.m", "output.out");

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
    let readers: SourceReaders = HashMap::from([
        (
            "pa".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("a.xml"),
                Box::new(Cursor::new(doc_a.as_bytes().to_vec())),
            )]),
        ),
        (
            "pb".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("b.xml"),
                Box::new(Cursor::new(doc_b.as_bytes().to_vec())),
            )]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("non-fused merge over multiple documents streams");
    assert_eq!(report.counters.total_count, 6, "six body records");
    assert_eq!(report.counters.dlq_count, 0, "no records should DLQ");

    let lines = body_lines(&buf);
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
             bled across documents through the streaming Merge handoff)"
        );
    }
}

/// A fused `Source → Transform` whose sole downstream is a (strict, hash)
/// `Aggregate` streams record-at-a-time into the Aggregate's ingest rather
/// than pre-draining its whole output from a charged `node_buffers` slot.
/// The Transform reports `buffer: streaming` and emits no `node_buffer`
/// edge to the Aggregate, and the grouped totals match the materialized
/// path exactly. The `group_by: [dept]` keeps the aggregate on the hash
/// strategy (no pre-sorted ordering), so this exercises the hash ingest.
#[test]
fn fused_transform_streams_ingest_into_aggregate() {
    let yaml = r#"
pipeline:
  name: streaming_agg_ingest
nodes:
  - type: source
    name: sales
    config:
      name: sales
      type: csv
      path: ./sales.csv
      options: { has_header: true }
      schema:
        - { name: dept, type: string }
        - { name: amount, type: string }
        - { name: status, type: string }
  - type: transform
    name: active_only
    input: sales
    config:
      cxl: |
        filter status == "active"
        emit dept = dept
        emit amount = amount
  - type: aggregate
    name: totals
    input: active_only
    config:
      group_by: [dept]
      cxl: |
        emit dept = dept
        emit total = sum(amount.to_int())
        emit n = count(*)
  - type: output
    name: out
    input: totals
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let explain = explain_of(&config, &plan);
    // The aggregate stays on the hash strategy (grouped, unsorted input).
    assert!(
        explain.contains("[aggregation:hash] totals"),
        "expected a grouped aggregate over unsorted input to resolve hash, \
         got:\n{explain}"
    );
    // The fused Transform streams its ingest into the Aggregate: no charged
    // node_buffer edge crosses transform.active_only -> aggregation.totals.
    assert_streaming_to_output(&explain, "transform.active_only", "aggregation.totals");

    // a:10+30=40 (n=2), b:20+40=60 (n=2); one inactive row filtered out.
    let csv = "dept,amount,status\n\
        a,10,active\n\
        b,20,active\n\
        a,30,active\n\
        b,40,active\n\
        a,99,inactive\n";
    let readers: SourceReaders = HashMap::from([(
        "sales".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![fast_slot("sales", csv)]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("fused transform streams ingest into aggregate");
    let mut lines = body_lines(&buf);
    lines.sort();
    assert_eq!(lines.len(), 2, "two groups expected: {lines:?}");
    assert!(
        lines.iter().any(|l| l.starts_with("a,40,2")),
        "dept a total diverged from the materialized path: {lines:?}"
    );
    assert!(
        lines.iter().any(|l| l.starts_with("b,60,2")),
        "dept b total diverged from the materialized path: {lines:?}"
    );
    assert_eq!(report.counters.dlq_count, 0);
}

/// A non-fused `Merge` (`concat` of two Transform outputs) whose sole
/// downstream is a strict `Aggregate` streams the merged stream into the
/// Aggregate's ingest. The Merge reports `buffer: streaming` and emits no
/// `node_buffer` edge to the Aggregate, and the global total over both
/// branches matches the materialized path.
#[test]
fn nonfused_merge_streams_ingest_into_aggregate() {
    let yaml = r#"
pipeline:
  name: streaming_merge_agg_ingest
nodes:
  - type: source
    name: sa
    config:
      name: sa
      type: csv
      path: ./sa.csv
      schema:
        - { name: amount, type: string }
  - type: source
    name: sb
    config:
      name: sb
      type: csv
      path: ./sb.csv
      schema:
        - { name: amount, type: string }
  - type: transform
    name: ta
    input: sa
    config:
      cxl: |
        emit amount = amount
  - type: transform
    name: tb
    input: sb
    config:
      cxl: |
        emit amount = amount
  - type: merge
    name: m
    inputs: [ta, tb]
    config:
      mode: concat
  - type: aggregate
    name: totals
    input: m
    config:
      group_by: []
      cxl: |
        emit total = sum(amount.to_int())
        emit n = count(*)
  - type: output
    name: out
    input: totals
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    assert_streaming_to_output(&explain_of(&config, &plan), "merge.m", "aggregation.totals");

    let sa = "amount\n10\n20\n30\n";
    let sb = "amount\n40\n50\n";
    let readers: SourceReaders = HashMap::from([
        (
            "sa".to_string(),
            clinker_exec::executor::single_file_reader(
                "sa.csv",
                Box::new(Cursor::new(sa.as_bytes().to_vec())),
            ),
        ),
        (
            "sb".to_string(),
            clinker_exec::executor::single_file_reader(
                "sb.csv",
                Box::new(Cursor::new(sb.as_bytes().to_vec())),
            ),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("non-fused merge streams ingest into aggregate");
    let lines = body_lines(&buf);
    assert_eq!(lines.len(), 1, "global aggregate emits one row: {lines:?}");
    // 10+20+30+40+50 = 150 over 5 records.
    assert!(
        lines[0].starts_with("150,5"),
        "merged global aggregate diverged: {}",
        lines[0]
    );
    assert_eq!(report.counters.dlq_count, 0);
}

/// Streaming ingest must not deadlock under back-pressure: the producer's
/// bounded `send` blocks when the channel fills, and the Aggregate's scoped
/// ingest thread drains it concurrently. Drive far more records than the
/// 256-event channel bound through a fused `Source → Transform → Aggregate`
/// chain — a deadlock would hang the run rather than complete. The global
/// `count` must equal every input row, and the run must finish.
#[test]
fn aggregate_ingest_no_deadlock_under_backpressure() {
    const ROWS: usize = 5_000;
    let yaml = r#"
pipeline:
  name: streaming_agg_ingest_backpressure
  batch_size: 64
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: ./events.csv
      options: { has_header: true }
      schema:
        - { name: id, type: string }
        - { name: amount, type: string }
  - type: transform
    name: pass
    input: events
    config:
      cxl: |
        emit amount = amount
  - type: aggregate
    name: totals
    input: pass
    config:
      group_by: []
      cxl: |
        emit total = sum(amount.to_int())
        emit n = count(*)
  - type: output
    name: out
    input: totals
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    assert_streaming_to_output(
        &explain_of(&config, &plan),
        "transform.pass",
        "aggregation.totals",
    );

    let mut csv = String::from("id,amount\n");
    let mut expected_total: i64 = 0;
    for i in 1..=ROWS {
        csv.push_str(&format!("e-{i},{i}\n"));
        expected_total += i as i64;
    }
    let readers: SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![fast_slot("events", &csv)]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("streaming aggregate ingest must not deadlock under back-pressure");
    let lines = body_lines(&buf);
    assert_eq!(lines.len(), 1, "global aggregate emits one row: {lines:?}");
    assert!(
        lines[0].starts_with(&format!("{expected_total},{ROWS}")),
        "back-pressured aggregate ingest dropped/duplicated records: {}",
        lines[0]
    );
    assert_eq!(report.counters.total_count as usize, ROWS);
    assert_eq!(report.counters.dlq_count, 0);
}

/// A fused `Source → Transform` whose sole downstream is the driver
/// (probe) side of a hash build-probe `Combine` streams its records
/// record-at-a-time into the probe rather than the Combine pre-draining
/// the driver's whole output. The build side (`products`) stays
/// materialized in the hash table; the driver Transform reports
/// `buffer: streaming` and emits no `node_buffer` edge to the Combine, and
/// the joined result matches the materialized path.
#[test]
fn fused_transform_streams_probe_into_combine() {
    let yaml = r#"
pipeline:
  name: streaming_combine_probe
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: ./orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: ./products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
  - type: transform
    name: norm
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit product_id = product_id
  - type: combine
    name: c
    input:
      norm: norm
      products: products
    config:
      where: "norm.product_id == products.product_id"
      match: first
      on_miss: skip
      drive: norm
      cxl: |
        emit order_id = norm.order_id
        emit product_name = products.name
      propagate_ck: driver
  - type: output
    name: out
    input: c
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    // The fused Transform streams its probe into the Combine: no charged
    // node_buffer edge from the driver Transform to the Combine.
    assert_streaming_to_output(&explain_of(&config, &plan), "transform.norm", "combine.c");

    let orders = "order_id,product_id\no1,p1\no2,p2\no3,p1\no4,p3\n";
    let products = "product_id,name\np1,Prod1\np2,Prod2\n";
    let readers: SourceReaders = HashMap::from([
        (
            "orders".to_string(),
            clinker_exec::executor::single_file_reader(
                "orders.csv",
                Box::new(Cursor::new(orders.as_bytes().to_vec())),
            ),
        ),
        (
            "products".to_string(),
            clinker_exec::executor::single_file_reader(
                "products.csv",
                Box::new(Cursor::new(products.as_bytes().to_vec())),
            ),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("fused transform streams probe into combine");
    let mut lines = body_lines(&buf);
    lines.sort();
    // o1->Prod1, o2->Prod2, o3->Prod1 match; o4 (p3) misses and is skipped.
    assert_eq!(
        lines,
        vec!["o1,Prod1", "o2,Prod2", "o3,Prod1"],
        "streaming combine probe diverged from the materialized join"
    );
    assert_eq!(report.counters.dlq_count, 0);
}

/// A non-fused `Merge` whose sole downstream is the driver (probe) side of
/// a hash build-probe `Combine` streams the merged stream into the probe.
/// The Merge reports `buffer: streaming` and emits no `node_buffer` edge to
/// the Combine, and the joined result over both merged driver inputs
/// matches the materialized path.
#[test]
fn nonfused_merge_streams_probe_into_combine() {
    let yaml = r#"
pipeline:
  name: streaming_merge_combine_probe
nodes:
  - type: source
    name: oa
    config:
      name: oa
      type: csv
      path: ./oa.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
  - type: source
    name: ob
    config:
      name: ob
      type: csv
      path: ./ob.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: ./products.csv
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
  - type: transform
    name: ta
    input: oa
    config:
      cxl: |
        emit order_id = order_id
        emit product_id = product_id
  - type: transform
    name: tb
    input: ob
    config:
      cxl: |
        emit order_id = order_id
        emit product_id = product_id
  - type: merge
    name: m
    inputs: [ta, tb]
    config:
      mode: concat
  - type: combine
    name: c
    input:
      m: m
      products: products
    config:
      where: "m.product_id == products.product_id"
      match: first
      on_miss: skip
      drive: m
      cxl: |
        emit order_id = m.order_id
        emit product_name = products.name
      propagate_ck: driver
  - type: output
    name: out
    input: c
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    assert_streaming_to_output(&explain_of(&config, &plan), "merge.m", "combine.c");

    let oa = "order_id,product_id\no1,p1\no2,p2\n";
    let ob = "order_id,product_id\no3,p1\no4,p3\n";
    let products = "product_id,name\np1,Prod1\np2,Prod2\n";
    let readers: SourceReaders = HashMap::from([
        (
            "oa".to_string(),
            clinker_exec::executor::single_file_reader(
                "oa.csv",
                Box::new(Cursor::new(oa.as_bytes().to_vec())),
            ),
        ),
        (
            "ob".to_string(),
            clinker_exec::executor::single_file_reader(
                "ob.csv",
                Box::new(Cursor::new(ob.as_bytes().to_vec())),
            ),
        ),
        (
            "products".to_string(),
            clinker_exec::executor::single_file_reader(
                "products.csv",
                Box::new(Cursor::new(products.as_bytes().to_vec())),
            ),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("non-fused merge streams probe into combine");
    let mut lines = body_lines(&buf);
    lines.sort();
    // o1->Prod1, o2->Prod2, o3->Prod1 match; o4 (p3) misses and is skipped.
    assert_eq!(
        lines,
        vec!["o1,Prod1", "o2,Prod2", "o3,Prod1"],
        "streaming merged combine probe diverged from the materialized join"
    );
    assert_eq!(report.counters.dlq_count, 0);
}

/// Streaming combine probe must not deadlock under back-pressure: the
/// driver producer's bounded `send` blocks when the channel fills, and the
/// probe consumer thread drains it concurrently — after the build side is
/// materialized into the hash table. Drive far more driver rows than the
/// 256-event channel bound through a fused `Source → Transform → Combine`
/// chain; a deadlock would hang the run rather than complete. Every driver
/// row matches the single build row, so the output row count equals the
/// driver row count and the run must finish.
#[test]
fn combine_probe_no_deadlock_under_backpressure() {
    const ROWS: usize = 5_000;
    let yaml = r#"
pipeline:
  name: streaming_combine_probe_backpressure
  batch_size: 64
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: ./orders.csv
      options: { has_header: true }
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: ./products.csv
      options: { has_header: true }
      schema:
        - { name: product_id, type: string }
        - { name: name, type: string }
  - type: transform
    name: norm
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit product_id = product_id
  - type: combine
    name: c
    input:
      norm: norm
      products: products
    config:
      where: "norm.product_id == products.product_id"
      match: first
      on_miss: skip
      drive: norm
      cxl: |
        emit order_id = norm.order_id
        emit product_name = products.name
      propagate_ck: driver
  - type: output
    name: out
    input: c
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    assert_streaming_to_output(&explain_of(&config, &plan), "transform.norm", "combine.c");

    // One build row (all driver rows share product_id `p1`) so every driver
    // row matches exactly once — the output row count equals the driver row
    // count.
    let products = "product_id,name\np1,Prod1\n";
    let mut orders = String::from("order_id,product_id\n");
    for i in 1..=ROWS {
        orders.push_str(&format!("o-{i},p1\n"));
    }
    let readers: SourceReaders = HashMap::from([
        (
            "orders".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![fast_slot("orders", &orders)]),
        ),
        (
            "products".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![fast_slot("products", products)]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("streaming combine probe must not deadlock under back-pressure");
    let lines = body_lines(&buf);
    assert_eq!(
        lines.len(),
        ROWS,
        "back-pressured combine probe dropped/duplicated rows"
    );
    assert_eq!(report.counters.dlq_count, 0);
}

/// Per-source dead-letter rewind must survive a streaming driver. A
/// streaming-probe `Combine` whose matched body divides by zero on one
/// driver row routes only that row to the DLQ under
/// `error_handling: continue`, while the surviving driver rows reach the
/// writer and the run completes. This exercises the streaming path's
/// deferred-failure replay: the probe thread cannot touch the DLQ, so it
/// accumulates the failure and the dispatch thread replays it through the
/// same `dispatch_combine_output_error` rewind the materialized path uses,
/// after merging the driver-side pre-fold snapshot floor.
#[test]
fn streaming_combine_probe_preserves_dlq_rewind() {
    let yaml = r#"
pipeline:
  name: streaming_combine_probe_dlq
error_handling:
  strategy: continue
nodes:
  - type: source
    name: orders
    config:
      name: orders
      type: csv
      path: ./orders.csv
      schema:
        - { name: order_id, type: string }
        - { name: product_id, type: string }
        - { name: qty, type: int }
  - type: source
    name: products
    config:
      name: products
      type: csv
      path: ./products.csv
      schema:
        - { name: product_id, type: string }
        - { name: divisor, type: int }
  - type: transform
    name: norm
    input: orders
    config:
      cxl: |
        emit order_id = order_id
        emit product_id = product_id
        emit qty = qty
  - type: combine
    name: c
    input:
      norm: norm
      products: products
    config:
      where: "norm.product_id == products.product_id"
      match: first
      on_miss: skip
      drive: norm
      cxl: |
        emit order_id = norm.order_id
        emit ratio = norm.qty / products.divisor
      propagate_ck: driver
  - type: output
    name: out
    input: c
    config:
      name: out
      type: csv
      path: ./out.csv
"#;
    let config = parse_config(yaml).expect("parse_config");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    assert_streaming_to_output(&explain_of(&config, &plan), "transform.norm", "combine.c");

    // Driver `o2` matches build `p2` whose divisor is 0 → the matched body
    // `qty / divisor` divides by zero, routing o2 to the DLQ. o1 and o3
    // (divisor 2) survive.
    let orders = "order_id,product_id,qty\no1,p1,10\no2,p2,20\no3,p1,30\n";
    let products = "product_id,divisor\np1,2\np2,0\n";
    let readers: SourceReaders = HashMap::from([
        (
            "orders".to_string(),
            clinker_exec::executor::single_file_reader(
                "orders.csv",
                Box::new(Cursor::new(orders.as_bytes().to_vec())),
            ),
        ),
        (
            "products".to_string(),
            clinker_exec::executor::single_file_reader(
                "products.csv",
                Box::new(Cursor::new(products.as_bytes().to_vec())),
            ),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .expect("streaming combine probe completes under continue strategy");
    let mut lines = body_lines(&buf);
    lines.sort();
    // o1 -> 10/2 = 5, o3 -> 30/2 = 15 survive; o2 div-by-zero is DLQ'd.
    assert_eq!(
        lines,
        vec!["o1,5", "o3,15"],
        "surviving streaming-probe rows diverged"
    );
    // The matched-body failure routes two DLQ entries — the driver row
    // (trigger) and its contributing build row (attribution) — exactly as
    // the materialized path does via `dispatch_combine_output_error`.
    assert_eq!(
        report.counters.dlq_count, 2,
        "the failing driver row and its matched build row both route to the DLQ"
    );
}
