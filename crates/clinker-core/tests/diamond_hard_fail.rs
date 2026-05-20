//! Hard-fail end-to-end coverage for `node_buffers` admission on a
//! diamond topology.
//!
//! Drives a Source → Transform(stage_split) → {branch_a, branch_b}
//! → Merge → Output pipeline under a 512 KiB memory limit.
//! `stage_split` has two outgoing edges, so its slot fails the
//! `node_buffer_spill_allowed` predicate and producer-side spill is
//! disabled at that admission site. When `stage_split` widens the
//! schema from 3 columns to 8 columns, the per-row admission cost
//! crosses the hard limit and the run aborts with a structured
//! `PipelineError::MemoryBudgetExceeded { source: BudgetCategory::
//! NodeBuffer, .. }` naming the producer.
//!
//! Per-row admission cost is `size_of::<Value>() * cols +
//! size_of::<(Record, u64)>()` (see `estimate_node_buffer_bytes` in
//! `executor/dispatch.rs`). For the 3-column input schema this is
//! ~216 B/row; for the 8-column widened schema this is ~336 B/row.
//! 2 000 rows give an upstream admission charge of ~432 KiB (under
//! 524 288 B hard) and a downstream charge of ~672 KiB (over hard),
//! placing the hard-fail at `stage_split`'s admit.
//!
//! The assertion destructures the error variant directly — no
//! substring matching on diagnostic codes or rendered messages.

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, PipelineConfig};
use clinker_core::error::PipelineError;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use clinker_core::pipeline::memory::BudgetCategory;
use std::collections::HashMap;
use std::io::Write;

const PIPELINE_YAML: &str = r#"
pipeline:
  name: diamond_hard_fail
  memory_limit: "512K"
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
        - { name: value, type: int }
  - type: transform
    name: stage_split
    input: events
    config:
      cxl: |
        emit id = id
        emit region = region
        emit value = value
        emit derived_a = id
        emit derived_b = region
        emit derived_c = id
        emit derived_d = region
        emit derived_e = id
  - type: transform
    name: branch_a
    input: stage_split
    config:
      cxl: |
        emit id = id
        emit region = region
        emit value = value
        emit derived_a = derived_a
        emit derived_b = derived_b
        emit derived_c = derived_c
        emit derived_d = derived_d
        emit derived_e = derived_e
  - type: transform
    name: branch_b
    input: stage_split
    config:
      cxl: |
        emit id = id
        emit region = region
        emit value = value
        emit derived_a = derived_a
        emit derived_b = derived_b
        emit derived_c = derived_c
        emit derived_d = derived_d
        emit derived_e = derived_e
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

const TOTAL_ROWS: usize = 2_000;

fn build_events_csv() -> String {
    let mut s = String::with_capacity(TOTAL_ROWS * 32);
    s.push_str("id,region,value\n");
    for i in 1..=TOTAL_ROWS as u64 {
        let region = match i % 3 {
            0 => 'a',
            1 => 'b',
            _ => 'c',
        };
        s.push_str(&format!("id_{i},{region},{i}\n"));
    }
    s
}

#[tokio::test(flavor = "multi_thread")]
async fn diamond_topology_hard_fails_at_widening_producer() {
    let csv = build_events_csv();
    let config: PipelineConfig =
        clinker_core::yaml::from_str(PIPELINE_YAML).expect("parse pipeline YAML");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let mut readers: clinker_core::executor::SourceReaders = HashMap::new();
    readers.insert(
        "events".to_string(),
        clinker_core::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    );

    let out = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "diamond-hard-fail".to_string(),
        batch_id: "batch-0".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let err = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .await
        .expect_err("512 KiB budget must abort the widening admit");

    match err {
        PipelineError::MemoryBudgetExceeded {
            node,
            source,
            used,
            limit,
            ..
        } => {
            assert_eq!(
                node, "stage_split",
                "diamond hard-fail must name the multi-consumer producer",
            );
            assert!(
                matches!(source, BudgetCategory::NodeBuffer),
                "diamond hard-fail must surface as NodeBuffer category; got {source:?}",
            );
            assert!(
                used > limit,
                "reported used ({used}) must exceed limit ({limit}) at hard-fail",
            );
        }
        other => panic!("expected MemoryBudgetExceeded; got: {other:?}"),
    }
}
