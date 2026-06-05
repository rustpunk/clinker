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
//! Producer-side spill is gated to single-consumer slots
//! (`node_buffer_spill_allowed`), so the diamond's `stage_split`
//! producer — which fans out to two branches — stays in memory; the
//! spillable slot is a single-consumer branch feeding the Merge.
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
        Box::new(crate::pipeline::memory::Priority),
    );
    // 90 GiB: above the 80 GiB soft limit, below the 100 GiB hard limit.
    arb.set_peak_rss_for_test(90 * 1024 * 1024 * 1024);
    arb.set_max_spill_bytes(1);
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
- type: output
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
                node == "branch_a" || node == "branch_b",
                "the spillable slot is a single-consumer branch (stage_split fans out \
                 to two consumers and cannot spill); got node {node:?}",
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
