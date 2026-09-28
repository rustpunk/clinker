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
//! budget. Each run is configured with an ample `memory.limit` and held to
//! the clamped budget as ledger capacity, so the startup check (E312) judges
//! a realistic limit while the run still reaches the spill path.

#![cfg(feature = "test-utils")]

#[path = "common/pipeline_resource_fixtures.rs"]
mod resource_fixtures;

#[path = "common/memory_pressure.rs"]
mod memory_pressure;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, MemoryTestOverrides, PipelineExecutor, PipelineRunParams,
};
use clinker_plan::config::utils::parse_memory_limit_bytes;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;
use memory_pressure::{assert_capacity_below_ample_peak, assert_spill_engaged};

/// The aggregate node's name, shared by the pipeline template and the
/// per-stage attribution assertion.
const AGG_NODE: &str = "by_category";

/// The `memory.limit` every run is configured with; a smaller budget is
/// applied as ledger capacity.
const AMPLE_LIMIT: &str = "512M";

/// The ledger capacity the spilling run is held to: 88 KiB.
///
/// The test ran under `memory.limit: 48K` plus the measured CSV workspace
/// before capacity existed, 114,688 bytes, which is above what the same
/// input charges with ample memory (96,600 to 103,000 bytes at its peak,
/// varying with scheduling), so that figure could not force a spill on its
/// own. 88 KiB lies below that peak and above what cannot spill: under
/// about 78,360 bytes the Source's 16,384-byte document admission falls
/// short beside the 61,976 bytes already charged, and the run fails
/// instead of completing.
const SPILL_CAPACITY: u64 = 88 * 1024;

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
  - type: transform
    name: passthrough
    input: events
    config:
      cxl: "emit category = category"
  - type: aggregate
    name: {AGG_NODE}
    input: passthrough
    config:
      group_by:
        - category
      cxl: |
        emit category = category
        emit n = count(*)
  - type: sink
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

/// Compile and run the many-key aggregate pipeline held to `budget`,
/// returning the raw executor result so both the success and the
/// E320-abort assertions can inspect it.
///
/// The run's ledger capacity is the limit a run configured with `budget`
/// enforced before capacity existed ([`budget_limit_bytes`]); its
/// `memory.limit` is [`AMPLE_LIMIT`].
fn run(
    budget: &str,
    strategy: &str,
    disk_cap: Option<u64>,
) -> Result<ExecutionReport, PipelineError> {
    run_at(
        Some(budget_limit_bytes(budget, strategy, disk_cap)),
        strategy,
        disk_cap,
    )
}

/// The limit a run configured with `memory.limit: budget` enforces: the
/// budget itself, plus the measured CSV workspace when no disk cap is set
/// (the success path adds it; the disk-cap aborts must not).
fn budget_limit_bytes(budget: &str, strategy: &str, disk_cap: Option<u64>) -> u64 {
    let mut config =
        parse_config(&spill_cap_yaml(budget, strategy)).expect("parse agg-spill-cap pipeline");
    if disk_cap.is_none() {
        resource_fixtures::add_csv_workspace(&mut config, &CompileContext::default());
    }
    parse_memory_limit_bytes(config.pipeline.memory.limit.as_deref()).expect("parse the budget")
}

/// Compile and run the pipeline at [`AMPLE_LIMIT`], held to `capacity`
/// bytes of ledger when one is given.
fn run_at(
    capacity: Option<u64>,
    strategy: &str,
    disk_cap: Option<u64>,
) -> Result<ExecutionReport, PipelineError> {
    let yaml = spill_cap_yaml(AMPLE_LIMIT, strategy);
    let mut config = parse_config(&yaml).expect("parse agg-spill-cap pipeline");
    if disk_cap.is_none() {
        resource_fixtures::add_csv_workspace(&mut config, &CompileContext::default());
    }
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile agg-spill-cap pipeline");

    // Supply already-decoded records so this budget exercises aggregate/spill
    // accounting rather than the independent CSV decoder admission boundary.
    let readers = HashMap::from([(
        "events".to_string(),
        resource_fixtures::predecoded_csv_source(
            &config,
            &CompileContext::default(),
            "events",
            &[("events.csv", &many_key_csv())],
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf) as Box<dyn std::io::Write + Send>,
    )]);
    let memory_test = match capacity {
        Some(bytes) => MemoryTestOverrides::default().with_ledger_capacity(bytes),
        None => MemoryTestOverrides::default(),
    };
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        spill_disk_cap_bytes: disk_cap,
        memory_test,
        ..Default::default()
    };
    PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
}

#[test]
fn aggregate_spill_charges_per_stage_disk_accounting() {
    // A fused passthrough streams the input into Aggregate, isolating its group
    // table from an unrelated full-input scan reservation. The capacity
    // admits the 200-row terminal materialization while the 200-key table still
    // crosses its deterministic spill threshold. The run completes, and its
    // spilled bytes are attributed to the aggregate node.
    assert!(
        SPILL_CAPACITY <= budget_limit_bytes("48K", "fail_fast", None),
        "the capacity never exceeds the limit the test ran under before capacity existed"
    );
    let report = run_at(Some(SPILL_CAPACITY), "fail_fast", None)
        .expect("run completes under an unlimited disk cap");
    let ample = run_at(None, "fail_fast", None).expect("the ample run completes");
    assert_spill_engaged(&report);
    assert_capacity_below_ample_peak(SPILL_CAPACITY, &ample);
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
