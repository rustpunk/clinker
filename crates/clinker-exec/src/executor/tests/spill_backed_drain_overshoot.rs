//! Hard-limit overshoot coverage for the re-materialized-drain gate on
//! `node_buffers`, surfacing the reserved `BudgetCategory::NodeBuffer`
//! (E310) tag.
//!
//! A blocking consumer that drains a *spilled* predecessor slot streams the
//! records back off disk into a fresh `Vec`. That slot's admission charge
//! was discharged when its consumer unregistered it, so absent a gate the
//! bytes landing back in RAM are invisible to the budget — a slot that
//! spilled precisely because it outgrew the budget could re-inflate past the
//! hard limit with no abort. `NodeBuffer::drain_split_metered` (wired into
//! the single-consumer Transform and Aggregate drain sites) compares the
//! growing vector's footprint against the hard limit and aborts with the
//! typed `MemoryBudgetExceeded { source: NodeBuffer }`.
//!
//! The hard limit is a small 64 KiB budget: the ~1,500-record re-materialized
//! input alone exceeds it, so the abort fires on the re-materialized
//! footprint deterministically — never on whole-process RSS, which for any
//! sub-baseline budget is always over and would abort legitimate spill
//! round-trips whose finite input fits the budget. `peak_rss` is seeded above
//! the soft threshold only so the producer spills its output at admission on
//! every platform. Both chains route the input so the drained slot is a real
//! spill-backed `node_buffers` entry (a fused Source→Transform chain would
//! stream off the Source receiver instead). The aggregate case is a
//! relaxed-CK (value-buffering) aggregate: a `correlation_key` source feeding
//! a `min` binding grouped on a non-key column. That shape is excluded from
//! the streaming-ingest channel a strict aggregate would take, so the
//! aggregate drains its predecessor's spilled slot through
//! `drain_split_metered` and is itself the first stage to re-materialize a
//! spilled buffer. The assertions destructure the typed variant — no
//! substring matching.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;
use std::sync::Arc;

/// A small 64 KiB budget: the ~1,500-record re-materialized input footprint
/// crosses it on the first per-batch poll, so the drain gate fires on the
/// re-materialized bytes rather than on whole-process RSS.
const HARD_LIMIT: u64 = 64 * 1024;
const SPILL_FRAC: f64 = 0.80;

/// Build a pipeline-scoped arbitrator whose consumer re-materialized drain
/// aborts on the input footprint. `peak_rss` is seeded above the soft
/// threshold only so the producer spills its output at admission on every
/// platform (independent of the real RSS reading); the disk quota is left
/// unlimited so the producer's admission spill never trips the E320 disk-cap
/// path first — the abort under test is the memory-budget E310.
fn abort_seeded_arbitrator() -> Arc<crate::pipeline::memory::MemoryArbitrator> {
    let arb = crate::pipeline::memory::MemoryArbitrator::with_policy(
        HARD_LIMIT,
        SPILL_FRAC,
        0.70,
        Box::new(crate::pipeline::memory::Priority),
    );
    // 1 MiB: above the ~52 KiB soft threshold so the producer spills.
    arb.set_peak_rss_for_test(1024 * 1024);
    Arc::new(arb)
}

/// >1024 rows so the metered drain reaches its first per-batch poll.
fn transform_csv() -> String {
    let mut csv = String::from("id,amount\n");
    for i in 0..1500 {
        csv.push_str(&format!("id_{i},{i}\n"));
    }
    csv
}

/// >1024 rows with the extra grouping column the relaxed aggregate keys on.
fn aggregate_csv() -> String {
    let mut csv = String::from("id,dept,amount\n");
    for i in 0..1500 {
        let dept = if i % 2 == 0 { "a" } else { "b" };
        csv.push_str(&format!("id_{i},{dept},{i}\n"));
    }
    csv
}

fn readers(csv: String) -> crate::executor::SourceReaders {
    HashMap::from([(
        "events".to_string(),
        crate::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )])
}

fn writers(out: &SharedBuffer) -> HashMap<String, Box<dyn std::io::Write + Send>> {
    HashMap::from([(
        "out".to_string(),
        Box::new(out.clone()) as Box<dyn std::io::Write + Send>,
    )])
}

const TRANSFORM_CHAIN_YAML: &str = r#"
pipeline:
  name: spill_backed_transform_drain
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: int }
- type: transform
  name: stage_one
  input: events
  config:
    cxl: |
      emit id = id
      emit amount = amount
- type: transform
  name: stage_two
  input: stage_one
  config:
    cxl: |
      emit id = id
      emit amount = amount
- type: output
  name: out
  input: stage_two
  config:
    name: out
    type: csv
    path: out.csv
"#;

const AGGREGATE_CHAIN_YAML: &str = r#"
pipeline:
  name: spill_backed_aggregate_drain
nodes:
- type: source
  name: events
  config:
    name: events
    type: csv
    path: events.csv
    correlation_key: id
    schema:
      - { name: id, type: string }
      - { name: dept, type: string }
      - { name: amount, type: int }
- type: aggregate
  name: rollup
  input: events
  config:
    group_by: [dept]
    cxl: |
      emit dept = dept
      emit lo = min(amount)
- type: output
  name: out
  input: rollup
  config:
    name: out
    type: csv
    path: out.csv
"#;

fn run(yaml: &str, csv: String) -> PipelineError {
    let config = clinker_plan::config::parse_config(yaml).expect("parse pipeline YAML");
    let out = SharedBuffer::new();
    let params = PipelineRunParams {
        execution_id: "spill-backed-drain-overshoot".to_string(),
        batch_id: "batch-0".to_string(),
        ..Default::default()
    };
    PipelineExecutor::run_with_readers_writers_with_arbitrator(
        &config,
        readers(csv),
        writers(&out).into(),
        &params,
        clinker_plan::config::CompileContext::default(),
        abort_seeded_arbitrator(),
    )
    .expect_err("re-materializing the spilled slot past the hard limit must abort")
}

#[test]
fn spilled_transform_input_metered_drain_aborts_as_node_buffer() {
    match run(TRANSFORM_CHAIN_YAML, transform_csv()) {
        PipelineError::MemoryBudgetExceeded { node, source, .. } => {
            assert_eq!(
                node, "stage_two",
                "the consumer draining the spilled predecessor is the aborting stage",
            );
            assert_eq!(
                source,
                clinker_plan::BudgetCategory::NodeBuffer,
                "a re-materialized node-buffer drain is tagged NodeBuffer, not Arena",
            );
        }
        other => panic!("expected MemoryBudgetExceeded {{ NodeBuffer }}; got: {other:?}"),
    }
}

#[test]
fn spilled_aggregate_input_metered_drain_aborts_as_node_buffer() {
    match run(AGGREGATE_CHAIN_YAML, aggregate_csv()) {
        PipelineError::MemoryBudgetExceeded { node, source, .. } => {
            assert_eq!(
                node, "rollup",
                "the aggregate draining the spilled predecessor is the aborting stage",
            );
            assert_eq!(
                source,
                clinker_plan::BudgetCategory::NodeBuffer,
                "a re-materialized node-buffer drain is tagged NodeBuffer, not Arena",
            );
        }
        other => panic!("expected MemoryBudgetExceeded {{ NodeBuffer }}; got: {other:?}"),
    }
}
