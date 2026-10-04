//! The shared two-direction pressure helper refuses every weakened pair.
//!
//! `common/memory_pressure.rs` is what the Memory budget checklist in
//! `docs/ai/32_NODE_OBLIGATIONS.md` points consumer tests at. These tests
//! prove it has teeth: a node with no charged state, a pair whose state fits
//! under the low limit, an ample run that spilled, a low run that did not
//! spill the node or held the whole state, differing outputs, and a limit
//! that differs from the one the run enforced are each refused. The tests
//! that read real runs check the helper's field mapping against the executor
//! rather than restating it: the node's own charged peak, never the run-wide
//! sum, and the bytes a stage wrote to spill files, even when it deleted them
//! before the run ended.

#[path = "common/memory_pressure.rs"]
mod memory_pressure;
#[path = "common/pipeline_resource_fixtures.rs"]
mod resource_fixtures;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
#[cfg(feature = "test-utils")]
use clinker_exec::executor::{MemoryTestOverrides, single_file_reader};
#[cfg(feature = "test-utils")]
use clinker_plan::config::PipelineConfig;
use clinker_plan::config::{CompileContext, parse_config};
use memory_pressure::{PressureRun, assert_arbitrated};

const MIB: u64 = 1024 * 1024;

/// The node every synthetic pair names.
const NODE: &str = "by_key";

/// A pair that satisfies every check: the node's state (10 MiB) is larger
/// than the low limit (2 MiB), the low run spilled the node and peaked at
/// 1 MiB, the ample run wrote nothing, and both produced the same output.
fn passing_pair() -> (PressureRun, PressureRun) {
    let low = PressureRun {
        limit_bytes: 2 * MIB,
        peak_charged_bytes: Some(MIB),
        node_spill_bytes: 4 * MIB,
        total_spill_bytes: 4 * MIB,
        output: b"k,n\na,1\n".to_vec(),
    };
    let ample = PressureRun {
        limit_bytes: 512 * MIB,
        peak_charged_bytes: Some(10 * MIB),
        node_spill_bytes: 0,
        total_spill_bytes: 0,
        output: b"k,n\na,1\n".to_vec(),
    };
    (low, ample)
}

#[test]
fn a_state_larger_than_the_low_limit_passes() {
    let (low, ample) = passing_pair();
    assert_arbitrated(NODE, &low, &ample);
}

#[test]
#[should_panic(expected = "no charged state is attributed")]
fn a_node_with_no_charged_state_is_refused() {
    let (low, mut ample) = passing_pair();
    // The node's state never registered with the arbitrator under its name,
    // so nothing proves it was charged at all.
    ample.peak_charged_bytes = None;
    assert_arbitrated(NODE, &low, &ample);
}

#[test]
#[should_panic(expected = "state fits under the low limit")]
fn a_state_that_fits_the_low_limit_is_refused() {
    let (low, mut ample) = passing_pair();
    ample.peak_charged_bytes = Some(MIB);
    assert_arbitrated(NODE, &low, &ample);
}

#[test]
#[should_panic(expected = "ample memory spilled")]
fn an_ample_run_that_spills_is_refused() {
    let (low, mut ample) = passing_pair();
    ample.total_spill_bytes = 1;
    assert_arbitrated(NODE, &low, &ample);
}

#[test]
#[should_panic(expected = "did not spill")]
fn a_low_run_that_never_spilled_the_node_is_refused() {
    let (mut low, ample) = passing_pair();
    // Another node spilled; the node under test did not.
    low.node_spill_bytes = 0;
    assert_arbitrated(NODE, &low, &ample);
}

#[test]
#[should_panic(expected = "held the whole state")]
fn a_low_run_that_held_the_whole_state_is_refused() {
    let (mut low, ample) = passing_pair();
    low.peak_charged_bytes = ample.peak_charged_bytes;
    assert_arbitrated(NODE, &low, &ample);
}

#[test]
#[should_panic(expected = "output differs")]
fn differing_outputs_are_refused() {
    let (mut low, ample) = passing_pair();
    low.output = b"k,n\na,2\n".to_vec();
    assert_arbitrated(NODE, &low, &ample);
}

/// A two-row CSV Source feeding a CSV Sink under a 512M limit.
const PASSTHROUGH_YAML: &str = r#"
pipeline:
  name: pressure_helper_passthrough
  memory: { limit: "512M" }
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
        - { name: category, type: string }
  - type: sink
    name: out
    input: events
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// Run the passthrough pipeline over two rows.
fn run_passthrough() -> (ExecutionReport, Vec<u8>) {
    let config = parse_config(PASSTHROUGH_YAML).expect("parse passthrough pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile passthrough pipeline");
    let readers = HashMap::from([(
        "events".to_string(),
        resource_fixtures::predecoded_csv_source(
            &config,
            &CompileContext::default(),
            "events",
            &[("events.csv", "category\na\nb\n")],
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("the two-row passthrough completes");
    let output = buf.contents();
    assert!(!output.is_empty(), "the sink wrote the two rows");
    (report, output)
}

#[test]
#[should_panic(expected = "is not the limit the run enforced (536870912 bytes)")]
fn a_limit_the_run_did_not_enforce_is_refused() {
    let (report, output) = run_passthrough();
    // The pipeline runs under 512M; a test that claims 2 MiB would compare
    // the node's state against a limit the run never had.
    let _ = PressureRun::from_report(&report, "events", 2 * MIB, output);
}

#[test]
fn from_report_reads_the_nodes_own_charged_peak() {
    let (mut report, output) = run_passthrough();

    let run = PressureRun::from_report(&report, "events", 512 * MIB, output.clone());
    assert_eq!(run.limit_bytes, 512 * MIB);
    assert_eq!(run.output, output);
    let events_peak = report
        .per_node_peak_charged_bytes
        .get("events")
        .copied()
        .expect("the Source registers its ingest channel under its own name");
    assert!(events_peak > 0, "the Source charged the rows it read");
    assert_eq!(run.peak_charged_bytes, Some(events_peak));

    // The Sink registers no state of its own, so it has no peak, even though
    // the run as a whole charged bytes.
    let sink = PressureRun::from_report(&report, "out", 512 * MIB, output.clone());
    assert_eq!(sink.peak_charged_bytes, None);

    // Other nodes' charges and the run-wide sampled sum never reach the
    // node's figure.
    report
        .per_node_peak_charged_bytes
        .insert("other_node".to_string(), 64 * MIB);
    report.peak_consumer_usage_bytes = 128 * MIB;
    let again = PressureRun::from_report(&report, "events", 512 * MIB, output);
    assert_eq!(again.peak_charged_bytes, Some(events_peak));

    // Ample memory: nothing reached disk, so the node reads as unspilled.
    assert!(report.per_stage_spill_bytes_written.is_empty());
    assert_eq!(again.node_spill_bytes, 0);
    assert_eq!(again.total_spill_bytes, 0);
}

#[test]
fn from_report_reads_spill_bytes_written_not_bytes_left_on_disk() {
    let (mut report, output) = run_passthrough();
    // Every run the node wrote was deleted before the report: nothing is
    // left on disk, but the node did spill.
    report.per_stage_spill_bytes.insert("out".to_string(), 0);
    report.cumulative_spill_bytes = 0;
    report
        .per_stage_spill_bytes_written
        .insert("events".to_string(), 7);
    report
        .per_stage_spill_bytes_written
        .insert("out".to_string(), 11);
    let run = PressureRun::from_report(&report, "out", 512 * MIB, output);
    assert_eq!(run.node_spill_bytes, 11, "the node's own written bytes");
    assert_eq!(run.total_spill_bytes, 18, "every stage's written bytes");
}

/// A Source that declares `sort_order: [key]` and repairs unsorted input.
/// Its order barrier sorts the rows through spilled runs under a small
/// limit, merges them, and deletes every run before the report is taken.
#[cfg(feature = "test-utils")]
fn order_repair_yaml(limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: pressure_helper_order_repair
  memory: {{ limit: "{limit}", backpressure: spill }}
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: csv
      path: rows.csv
      schema:
        - {{ name: key, type: int }}
        - {{ name: payload, type: string }}
      sort_order: [key]
      on_unsorted: warn
  - type: sink
    name: out
    input: rows
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// The order-repair config at `limit`, with the CSV workspace added.
#[cfg(feature = "test-utils")]
fn order_repair_config(limit: &str) -> PipelineConfig {
    let mut config: PipelineConfig =
        clinker_plan::yaml::from_str(&order_repair_yaml(limit)).expect("parse order repair");
    resource_fixtures::add_csv_workspace(&mut config, &CompileContext::default());
    config
}

#[cfg(feature = "test-utils")]
fn run_order_repair(limit: &str, memory_test: MemoryTestOverrides) -> (ExecutionReport, Vec<u8>) {
    let config = order_repair_config(limit);
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile order repair");
    let mut csv = String::from("key,payload\n");
    for key in (0..128).rev() {
        csv.push_str(&format!("{key},row-{key:03}-{}\n", "x".repeat(96)));
    }
    let readers = HashMap::from([(
        "rows".to_string(),
        single_file_reader("rows.csv", Box::new(std::io::Cursor::new(csv.into_bytes()))),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        writers,
        &PipelineRunParams {
            memory_test,
            ..Default::default()
        },
    )
    .expect("the order repair completes");
    (report, buf.contents())
}

#[cfg(feature = "test-utils")]
#[test]
fn a_sort_that_deleted_its_runs_still_reads_as_spilled() {
    use memory_pressure::{assert_capacity_below_ample_peak, assert_spill_engaged};

    // The order barrier records its repair runs under this stage key.
    const REPAIR: &str = "source-order:rows:records";

    // The low run is held to 88 KiB of ledger. It completes at 80,000 bytes
    // (at 76,000 the CSV reader's 16 KiB admission falls short), and the same
    // input with ample memory charges 99,568 bytes at its peak. The limit it
    // had before capacity existed, the authored 40K plus the CSV workspace the
    // fixture adds (106,496 bytes), lies above that peak, so the capacity is
    // taken inside the window, and never above that old limit.
    const CAPACITY: u64 = 88 * 1024;
    let old_limit = clinker_plan::config::utils::parse_memory_limit_bytes(
        order_repair_config("40K").pipeline.memory.limit.as_deref(),
    )
    .expect("parse the old limit");
    assert!(CAPACITY <= old_limit);
    let capacity = CAPACITY;
    let (report, low_output) = run_order_repair(
        "64M",
        MemoryTestOverrides::default().with_ledger_capacity(capacity),
    );
    assert_eq!(
        report
            .per_stage_spill_bytes
            .get(REPAIR)
            .copied()
            .unwrap_or(0),
        0,
        "the repair merged and deleted every run before the report"
    );
    let written = report
        .per_stage_spill_bytes_written
        .get(REPAIR)
        .copied()
        .unwrap_or(0);
    assert!(
        written > 0,
        "the repair spilled under its ledger capacity: {:?}",
        report.per_stage_spill_bytes_written
    );
    // The run enforces its ledger capacity, more than the authored 40K; the
    // helper would refuse 40 * 1024 here, so the test states the limit the
    // run's report says it enforced.
    assert_eq!(
        report.memory_limit_bytes, CAPACITY,
        "the low run enforces the ledger capacity it was held to"
    );
    let low_limit = report.memory_limit_bytes;
    assert!(low_limit > 40 * 1024);
    let low = PressureRun::from_report(&report, REPAIR, low_limit, low_output.clone());
    assert_eq!(low.node_spill_bytes, written);
    assert!(low.total_spill_bytes >= written);
    let low_report = report;

    assert_spill_engaged(&low_report);
    let (report, ample_output) = run_order_repair("64M", MemoryTestOverrides::default());
    assert_capacity_below_ample_peak(capacity, &report);
    let ample_limit = report.memory_limit_bytes;
    let ample = PressureRun::from_report(&report, REPAIR, ample_limit, ample_output);
    assert_eq!(ample.node_spill_bytes, 0);
    assert_eq!(ample.total_spill_bytes, 0);
    assert_eq!(low.output, ample.output, "spilling changes no output byte");
}
