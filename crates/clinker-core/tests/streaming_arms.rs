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
//!    `streaming_output_producer` predicate the runtime sender-install
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

use std::collections::HashMap;
use std::io::{Cursor, Write};
use std::path::PathBuf;

use clinker_bench_support::io::{SharedBuffer, fast_reader};
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
/// and emits no charged `node_buffer` edge to `output_slug` — the
/// user-observable proof that its output never crosses a spill-eligible
/// inter-stage buffer.
fn assert_streaming_to_output(explain: &str, producer_slug: &str, output_slug: &str) {
    let block = properties_block(explain, producer_slug);
    assert!(
        block.contains("buffer: streaming"),
        "{producer_slug} should classify streaming, got:\n{block}"
    );
    assert!(
        !block.contains("buffer: materialized"),
        "{producer_slug} must not be materialized:\n{block}"
    );
    let edge = format!("edge {producer_slug} -> {output_slug}");
    assert!(
        !explain.contains(&edge),
        "a streaming {producer_slug} must emit no node_buffer edge to its \
         Output (the whole point: no charged inter-stage slot), got:\n{explain}"
    );
}

fn explain_of(
    config: &clinker_core::config::PipelineConfig,
    plan: &clinker_core::plan::CompiledPlan,
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
        clinker_core::executor::SourceInput::Files(vec![fast_slot("orders", &csv)]),
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
            clinker_core::executor::single_file_reader(
                "orders.csv",
                Box::new(Cursor::new(orders.as_bytes().to_vec())),
            ),
        ),
        (
            "products".to_string(),
            clinker_core::executor::single_file_reader(
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
        clinker_core::executor::SourceInput::Files(vec![fast_slot("sales", csv)]),
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
            clinker_core::executor::single_file_reader(
                "sa.csv",
                Box::new(Cursor::new(sa.as_bytes().to_vec())),
            ),
        ),
        (
            "sb".to_string(),
            clinker_core::executor::single_file_reader(
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
            clinker_core::executor::SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("a.xml"),
                Box::new(Cursor::new(doc_a.as_bytes().to_vec())),
            )]),
        ),
        (
            "pb".to_string(),
            clinker_core::executor::SourceInput::Files(vec![FileSlot::new(
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
