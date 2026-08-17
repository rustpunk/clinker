//! Hard-limit overshoot coverage for the `node_buffers` admission's
//! disk-spill-quota gate, surfacing the dedicated
//! `PipelineError::SpillCapExceeded` (E320) shape on a diamond topology.
//!
//! Under the RSS-based arbitration model a `node_buffers` slot only
//! raises the disk-cap error through the disk-spill-quota path in
//! `admit_node_buffer`: when the soft limit has tripped (`should_spill()`
//! true) the slot flushes to a spill file, and an over-quota cumulative
//! disk total (`record_spill_bytes` past `max_spill_bytes`) returns the
//! structured `SpillCapExceeded` error — deliberately distinct from the
//! memory-budget E310 so a spilled-out volume never reads as OOM.
//! Every materialized slot is spill-eligible. The diamond's shared
//! `stage_split` producer spills once, then each branch opens a fresh
//! sequential scan over the same immutable spill backing.
//!
//! The arbitrator is seeded deterministically: `peak_rss` above the soft
//! limit (so `should_spill` trips) but below the hard limit (so no
//! whole-process abort fires first), and `max_spill_bytes` set to one
//! byte so the first branch flush overflows the quota. This drives the
//! gate without engineering a workload large enough to push the test
//! process's real RSS past a tight budget — pull-mode makes that race
//! the framework footprint.
//!
//! The assertion destructures the typed variant — no substring matching
//! on rendered diagnostics.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;
use std::sync::Arc;

/// Hard limit far above any realistic test-process RSS so the seeded
/// `peak_rss` dominates the `fetch_max` fold inside `observe()`.
const HARD_LIMIT: u64 = 100 * 1024 * 1024 * 1024;
const SPILL_FRAC: f64 = 0.80;

/// Build a pipeline-scoped arbitrator seeded above the soft limit (spill
/// active) but below the hard limit (no abort), with a one-byte disk
/// quota so the first spill flush overflows.
fn spill_tripped_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    let arb = crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        SPILL_FRAC,
        0.70,
        Box::new(crate::pipeline::memory::Priority),
    );
    // 90 GiB: above the 80 GiB soft limit, below the 100 GiB hard limit.
    arb.set_peak_rss_for_test(90 * 1024 * 1024 * 1024);
    arb.set_max_spill_bytes(1);
    Arc::new(arb)
}

/// Build a pipeline-scoped arbitrator with spilling forced and enough disk
/// quota for the shared slot and its branch outputs to complete.
fn forced_spill_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    let arb = crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        SPILL_FRAC,
        0.70,
        Box::new(crate::pipeline::memory::Priority),
    );
    arb.set_peak_rss_for_test(90 * 1024 * 1024 * 1024);
    arb.set_max_spill_bytes(u64::MAX);
    Arc::new(arb)
}

const PIPELINE_YAML: &str = r#"
pipeline:
  name: diamond_node_buffer_overshoot
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: transform
  name: stage_split
  input: events
  config:
    cxl: |
      emit id = id
      emit region = region
- type: transform
  name: branch_a
  input: stage_split
  config:
    cxl: |
      emit id = id
      emit region = region
- type: transform
  name: branch_b
  input: stage_split
  config:
    cxl: |
      emit id = id
      emit region = region
- type: merge
  name: joined
  inputs:
    - branch_a
    - branch_b
- type: sink
  name: out
  input: joined
  config:
    name: out
    type: csv
    path: out.csv
"#;

#[test]
fn diamond_branch_admission_overshoots_spill_quota_as_node_buffer() {
    let config = clinker_plan::config::parse_config(PIPELINE_YAML).expect("parse pipeline YAML");

    let mut csv = String::from("id,region\n");
    for i in 0..50 {
        csv.push_str(&format!("id_{i},a\n"));
    }
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        crate::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )]);

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "diamond-node-buffer-overshoot".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };

    let err = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &params,
        clinker_plan::config::CompileContext::default(),
        spill_tripped_arbitrator(),
    )
    .expect_err("one-byte spill quota must abort the first branch flush");

    match err {
        PipelineError::SpillCapExceeded {
            node,
            cap,
            attempted,
            current,
        } => {
            assert!(
                node == "stage_split" || node == "branch_a" || node == "branch_b",
                "a materialized diamond slot must report the quota failure; got node {node:?}",
            );
            assert_eq!(cap, 1, "reported cap must equal the one-byte quota");
            assert!(attempted > 0, "the overflowing flush must report its size");
            assert!(
                current > cap,
                "reported cumulative spilled ({current}) must exceed the cap ({cap})",
            );
        }
        other => panic!("expected SpillCapExceeded; got: {other:?}"),
    }
}

#[test]
fn diamond_fanout_completes_exactly_through_shared_spill_rescans() {
    let config = clinker_plan::config::parse_config(PIPELINE_YAML).expect("parse pipeline YAML");

    let mut csv = String::from("id,region\n");
    for i in 0..50 {
        csv.push_str(&format!("id_{i},a\n"));
    }
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        crate::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )]);

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "diamond-node-buffer-shared-spill".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };

    let report = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &params,
        clinker_plan::config::CompileContext::default(),
        forced_spill_arbitrator(),
    )
    .expect("forced-spill diamond must complete");

    assert!(
        report.cumulative_spill_bytes > 0,
        "the run must exercise spill"
    );
    let csv = out.as_string();
    let mut rows: Vec<&str> = csv.lines().skip(1).collect();
    rows.sort_unstable();
    let mut expected = Vec::with_capacity(100);
    for id in 0..50 {
        expected.push(format!("id_{id},a"));
        expected.push(format!("id_{id},a"));
    }
    expected.sort_unstable();
    assert_eq!(
        rows,
        expected.iter().map(String::as_str).collect::<Vec<_>>(),
        "both diamond consumers must deliver every source row exactly once"
    );
}

const THREE_CONSUMER_PIPELINE_YAML: &str = r#"
pipeline:
  name: three_consumer_node_buffer_spill
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: id, type: string }
      - { name: region, type: string }
- type: transform
  name: shared_stage
  input: events
  config:
    cxl: |
      emit id = id
      emit region = region
- type: transform
  name: branch_a
  input: shared_stage
  config: { cxl: "emit id = id\nemit region = region" }
- type: transform
  name: branch_b
  input: shared_stage
  config: { cxl: "emit id = id\nemit region = region" }
- type: transform
  name: branch_c
  input: shared_stage
  config: { cxl: "emit id = id\nemit region = region" }
- type: merge
  name: joined
  inputs: [branch_a, branch_b, branch_c]
- type: sink
  name: out
  input: joined
  config: { name: out, type: csv, path: out.csv }
"#;

#[test]
fn three_consumer_fanout_completes_exactly_through_shared_spill_rescans() {
    let config = clinker_plan::config::parse_config(THREE_CONSUMER_PIPELINE_YAML)
        .expect("parse three-consumer pipeline YAML");
    let mut csv = String::from("id,region\n");
    for i in 0..50 {
        csv.push_str(&format!("id_{i},a\n"));
    }
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        crate::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )]);
    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let report = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &PipelineRunParams {
            execution_id: "three-consumer-node-buffer-spill".to_string(),
            batch_id: "batch-0".to_string(),
            ..Default::default()
        },
        clinker_plan::config::CompileContext::default(),
        forced_spill_arbitrator(),
    )
    .expect("forced-spill three-consumer fan-out must complete");

    assert!(
        report.cumulative_spill_bytes > 0,
        "the run must exercise spill"
    );
    let rendered = out.as_string();
    let mut rows: Vec<&str> = rendered.lines().skip(1).collect();
    rows.sort_unstable();
    let mut expected = Vec::with_capacity(150);
    for id in 0..50 {
        for _ in 0..3 {
            expected.push(format!("id_{id},a"));
        }
    }
    expected.sort_unstable();
    assert_eq!(
        rows,
        expected.iter().map(String::as_str).collect::<Vec<_>>(),
        "all three consumers must deliver every source row exactly once"
    );
}

const PORT_ISOLATION_PIPELINE_YAML: &str = r#"
pipeline:
  name: port_isolation_shared_spill
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema: [ { name: id, type: string }, { name: value, type: int } ]
- type: source
  name: empty_b
  config:
    name: empty_b
    type: csv
    path: empty_b.csv
    schema: [ { name: id, type: string }, { name: value, type: int } ]
- type: source
  name: empty_c
  config:
    name: empty_c
    type: csv
    path: empty_c.csv
    schema: [ { name: id, type: string }, { name: value, type: int } ]
- type: source
  name: empty_d
  config:
    name: empty_d
    type: csv
    path: empty_d.csv
    schema: [ { name: id, type: string }, { name: value, type: int } ]
- type: route
  name: splitter
  input: events
  config:
    mode: exclusive
    conditions:
      low: "value < 10"
      high: "value >= 10"
    default: low
- type: merge
  name: low_one
  inputs: [splitter.low, empty_b]
- type: merge
  name: low_two
  inputs: [splitter.low, empty_c]
- type: merge
  name: high_one
  inputs: [splitter.high, empty_d]
- type: sink
  name: low_out_one
  input: low_one
  config: { name: low_out_one, type: csv, path: low-one.csv }
- type: sink
  name: low_out_two
  input: low_two
  config: { name: low_out_two, type: csv, path: low-two.csv }
- type: sink
  name: high_out
  input: high_one
  config: { name: high_out, type: csv, path: high.csv }
"#;

#[test]
fn shared_route_port_spills_without_crossing_producer_port_boundaries() {
    let config = clinker_plan::config::parse_config(PORT_ISOLATION_PIPELINE_YAML)
        .expect("parse port-isolation pipeline YAML");
    let readers: crate::executor::SourceReaders = [
        (
            "events",
            "id,value\nlow_a,1\nhigh_a,10\nlow_b,2\nhigh_b,20\n",
        ),
        ("empty_b", "id,value\n"),
        ("empty_c", "id,value\n"),
        ("empty_d", "id,value\n"),
    ]
    .into_iter()
    .map(|(name, csv)| {
        (
            name.to_string(),
            crate::executor::single_file_reader(
                format!("{name}.csv"),
                Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
            ),
        )
    })
    .collect();
    let low_one = SharedBuffer::new();
    let low_two = SharedBuffer::new();
    let high = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "low_out_one".to_string(),
            Box::new(low_one.clone()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "low_out_two".to_string(),
            Box::new(low_two.clone()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "high_out".to_string(),
            Box::new(high.clone()) as Box<dyn std::io::Write + Send>,
        ),
    ]);

    let report = PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers,
        writers.into(),
        &PipelineRunParams {
            execution_id: "port-isolation-node-buffer-spill".to_string(),
            batch_id: "batch-0".to_string(),
            ..Default::default()
        },
        clinker_plan::config::CompileContext::default(),
        forced_spill_arbitrator(),
    )
    .expect("forced-spill port fan-out must complete");

    assert!(
        report.cumulative_spill_bytes > 0,
        "the run must exercise spill"
    );
    let rows = |buffer: &SharedBuffer| {
        let rendered = buffer.as_string();
        let mut rows: Vec<String> = rendered.lines().skip(1).map(str::to_string).collect();
        rows.sort_unstable();
        rows
    };
    let low_expected = vec!["low_a,1".to_string(), "low_b,2".to_string()];
    let high_expected = vec!["high_a,10".to_string(), "high_b,20".to_string()];
    assert_eq!(rows(&low_one), low_expected);
    assert_eq!(rows(&low_two), low_expected);
    assert_eq!(rows(&high), high_expected);
}
