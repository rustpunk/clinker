//! Soft-spill end-to-end coverage for `node_buffers` admission.
//!
//! Drives a Source → Route(a, b, c) → 3 Outputs pipeline under a
//! 1 MiB memory limit. The Source admits its full output buffer to
//! its own `node_buffers` slot; the Source has a single outgoing
//! edge (port=None) so the slot qualifies for producer-side spill.
//! The 80 % soft threshold (≈819 KiB) is crossed by the test
//! process's own RSS — the spill predicate is RSS-based, not
//! counter-based, so the admit's spill arm fires on every supported
//! platform. Counter-based hard limit (1 MiB) stays above the
//! admit's charge so the run completes.
//!
//! Per-row admission cost is the `estimate_node_buffer_bytes`
//! formula in `executor/dispatch.rs`: `size_of::<Value>() * cols +
//! size_of::<(Record, u64)>()`. For the 5-column schema below this
//! resolves to ~264 B/row, so 2 000 rows charge ~528 KiB — under the
//! 1 MiB hard limit but well above the 819 KiB soft floor against
//! which the RSS check is compared.
//!
//! Asserts:
//! - the run completes with the expected per-branch row counts,
//! - `ExecutionReport.cumulative_spill_bytes > 0` so the budget
//!   recorded at least one spill commit.
//!
//! Skipped silently when `rss_bytes()` is unavailable — the spill
//! predicate gates on RSS, so without it the path stays in memory
//! and the test would assert against a counter that never moves.

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, PipelineConfig};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams};
use std::collections::HashMap;
use std::io::Write;

const PIPELINE_YAML: &str = r#"
pipeline:
  name: route_fanout_soft_spill
  memory: { limit: "1M" }
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
        - { name: payload, type: string }
        - { name: value, type: int }
        - { name: ts, type: int }
  - type: route
    name: by_region
    input: events
    config:
      mode: exclusive
      conditions:
        a: "region == \"a\""
        b: "region == \"b\""
      default: c
  - type: output
    name: out_a
    input: by_region.a
    config:
      name: out_a
      type: csv
      path: out_a.csv
  - type: output
    name: out_b
    input: by_region.b
    config:
      name: out_b
      type: csv
      path: out_b.csv
  - type: output
    name: out_c
    input: by_region.c
    config:
      name: out_c
      type: csv
      path: out_c.csv
"#;

const ROWS_A: usize = 1_500;
const ROWS_B: usize = 300;
const ROWS_C: usize = 200;
const TOTAL_ROWS: usize = ROWS_A + ROWS_B + ROWS_C;

fn build_events_csv() -> String {
    let mut s = String::with_capacity(TOTAL_ROWS * 48);
    s.push_str("id,region,payload,value,ts\n");
    let push_block = |s: &mut String, region: char, count: usize, mut id: u64| -> u64 {
        for _ in 0..count {
            id += 1;
            s.push_str(&format!("id_{id},{region},payload_{id},{id},{id}\n"));
        }
        id
    };
    let id = push_block(&mut s, 'a', ROWS_A, 0);
    let id = push_block(&mut s, 'b', ROWS_B, id);
    let _ = push_block(&mut s, 'c', ROWS_C, id);
    s
}

fn count_data_rows(csv_bytes: &str) -> usize {
    csv_bytes.lines().skip(1).filter(|l| !l.is_empty()).count()
}

#[test]
fn route_fanout_emits_spill_under_one_megabyte_budget() {
    // RSS-based spill predicate: no RSS reading, no spill firing — skip
    // rather than assert a false-negative.
    if clinker_core::pipeline::memory::rss_bytes().is_none() {
        return;
    }

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

    let out_a = SharedBuffer::new();
    let out_b = SharedBuffer::new();
    let out_c = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([
        (
            "out_a".to_string(),
            Box::new(out_a.clone()) as Box<dyn Write + Send>,
        ),
        (
            "out_b".to_string(),
            Box::new(out_b.clone()) as Box<dyn Write + Send>,
        ),
        (
            "out_c".to_string(),
            Box::new(out_c.clone()) as Box<dyn Write + Send>,
        ),
    ]);

    let params = PipelineRunParams {
        execution_id: "route-fanout-soft-spill".to_string(),
        batch_id: "batch-0".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("soft-spill run must complete");

    assert_eq!(
        report.counters.total_count as usize, TOTAL_ROWS,
        "total ingested row count must match the input",
    );
    assert!(
        report.cumulative_spill_bytes > 0,
        "node_buffers admission must have spilled at least once under 1 MiB budget; \
         report.cumulative_spill_bytes = {}",
        report.cumulative_spill_bytes,
    );

    assert_eq!(
        count_data_rows(&out_a.as_string()),
        ROWS_A,
        "branch `a` output row count mismatch",
    );
    assert_eq!(
        count_data_rows(&out_b.as_string()),
        ROWS_B,
        "branch `b` output row count mismatch",
    );
    assert_eq!(
        count_data_rows(&out_c.as_string()),
        ROWS_C,
        "branch `c` output row count mismatch",
    );
}
