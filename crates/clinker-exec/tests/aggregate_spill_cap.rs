//! Aggregate spill files must enter the run's disk-cap accounting.
//!
//! A hash aggregate that outgrows its memory budget spills group state to
//! disk. Those spill files count toward the run's cumulative on-disk spill
//! total, so they must both:
//!
//! * be attributed to the aggregate node in `ExecutionReport.per_stage_spill_bytes`
//!   (and roll into `cumulative_spill_bytes`), and
//! * trip the configured `storage.spill.disk_cap_bytes` quota (E320) the
//!   moment the cumulative total crosses it — a hard abort under every error
//!   strategy, never a per-record DLQ route, mirroring the reshape/cull and
//!   sort spill paths.
//!
//! The aggregate spills deterministically via the group-count threshold
//! (RSS-independent) by feeding many distinct group keys under a clamped
//! `memory.limit`; `backpressure: spill` keeps the non-pausing policy so the
//! sub-baseline budget reaches the spill path instead of being rejected at
//! startup (E312).

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;

/// The aggregate node's name, shared by the pipeline template and the
/// per-stage attribution assertion.
const AGG_NODE: &str = "by_category";

/// Group-by `count(*)` over a single in-memory CSV, output to CSV. A clamped
/// `memory_limit` shrinks the in-memory group cap so a many-key input spills
/// deterministically through the group-count threshold; `strategy` is the
/// error-handling disposition under test.
fn spill_cap_yaml(memory_limit: &str, strategy: &str) -> String {
    format!(
        r#"
pipeline:
  name: agg_spill_cap
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: {strategy}
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - {{ name: category, type: string }}
  - type: aggregate
    name: {AGG_NODE}
    input: events
    config:
      group_by:
        - category
      cxl: |
        emit category = category
        emit n = count(*)
  - type: output
    name: out
    input: {AGG_NODE}
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
    )
}

/// 400 rows over 200 distinct keys: enough distinct groups to overflow the
/// clamped in-memory cap several times, so the aggregate flushes more than
/// one spill file.
fn many_key_csv() -> String {
    let mut csv = String::from("category\n");
    for i in 0..400 {
        csv.push_str(&format!("k{}\n", i % 200));
    }
    csv
}

/// Compile and run the many-key aggregate pipeline, returning the raw
/// executor result so both the success and the E320-abort assertions can
/// inspect it.
fn run(
    memory_limit: &str,
    strategy: &str,
    disk_cap: Option<u64>,
) -> Result<ExecutionReport, PipelineError> {
    let yaml = spill_cap_yaml(memory_limit, strategy);
    let config = parse_config(&yaml).expect("parse agg-spill-cap pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile agg-spill-cap pipeline");

    let slots = vec![FileSlot::new(
        PathBuf::from("events.csv"),
        Box::new(Cursor::new(many_key_csv().into_bytes())),
    )];
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        spill_disk_cap_bytes: disk_cap,
        ..Default::default()
    };
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
}

#[test]
fn aggregate_spill_charges_per_stage_disk_accounting() {
    // Clamped budget, unlimited disk cap: the aggregate spills and the run
    // completes. Its spilled bytes must be attributed to the aggregate node
    // and roll into the pipeline-wide cumulative total.
    let report = run("1K", "fail_fast", None).expect("run completes under an unlimited disk cap");
    assert!(
        report.cumulative_spill_bytes > 0,
        "the clamped budget must force the aggregate to spill; \
         cumulative_spill_bytes = {}",
        report.cumulative_spill_bytes
    );
    let attributed = report
        .per_stage_spill_bytes
        .get(AGG_NODE)
        .copied()
        .unwrap_or(0);
    assert!(
        attributed > 0,
        "aggregate spill bytes must be attributed to `{AGG_NODE}`; got per_stage_spill_bytes = {:?}",
        report.per_stage_spill_bytes
    );
    let per_stage_sum: u64 = report.per_stage_spill_bytes.values().sum();
    assert_eq!(
        per_stage_sum, report.cumulative_spill_bytes,
        "per-stage spill bytes must sum to the cumulative total"
    );
}

#[test]
fn spill_cap_aborts_the_run_under_fail_fast() {
    // A one-byte disk cap must abort the spilling pipeline with E320 rather
    // than writing past the quota. A tiny shared budget makes the upstream
    // node-buffer and the aggregate both spill, so whichever crosses the cap
    // first names the node; the aggregate's own cap charge is asserted in
    // isolation by the `spill_past_disk_cap_returns_spill_cap_exceeded` unit
    // test. Here the end-to-end guarantee under test is that E320 fires with
    // the configured cap and a real overflow.
    let err = run("1K", "fail_fast", Some(1))
        .expect_err("a one-byte disk cap must abort the spilling run");
    match err {
        PipelineError::SpillCapExceeded {
            cap,
            attempted,
            current,
            ..
        } => {
            assert_eq!(cap, 1, "reported cap must equal the configured quota");
            assert!(attempted > 0, "the overflowing flush must report its size");
            assert!(
                current > cap,
                "cumulative spilled ({current}) must exceed the cap ({cap})"
            );
        }
        other => panic!("disk-cap overflow must surface SpillCapExceeded; got {other:?}"),
    }
}

#[test]
fn aggregate_spill_cap_hard_aborts_under_continue() {
    // A spill-cap breach is resource exhaustion, not a per-record data
    // fault: even under `continue` it hard-aborts rather than routing the
    // record to the DLQ, matching the reshape/cull spill-cap paths.
    let err = run("1K", "continue", Some(1))
        .expect_err("a spill-cap breach must hard-abort even under continue, not route to the DLQ");
    assert!(
        matches!(err, PipelineError::SpillCapExceeded { .. }),
        "continue must still surface E320 for a disk-cap breach; got {err:?}"
    );
}
