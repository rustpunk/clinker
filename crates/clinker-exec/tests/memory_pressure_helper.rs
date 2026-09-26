//! The shared two-direction pressure helper refuses every weakened pair.
//!
//! `common/memory_pressure.rs` is what the Memory budget checklist in
//! `docs/ai/32_NODE_OBLIGATIONS.md` points consumer tests at. These tests
//! prove it has teeth: a pair whose state fits under the low limit, an ample
//! run that spilled, a low run that did not spill the node or held the whole
//! state, and differing outputs are each refused. The last test reads a real
//! run's report, so the helper's field mapping is checked against the
//! executor rather than restated.

#[path = "common/memory_pressure.rs"]
mod memory_pressure;
#[path = "common/pipeline_resource_fixtures.rs"]
mod resource_fixtures;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, parse_config};
use memory_pressure::{PressureRun, assert_arbitrated};

const MIB: u64 = 1024 * 1024;

/// The node every synthetic pair names.
const NODE: &str = "by_key";

/// A pair that satisfies every check: the state (10 MiB) is larger than the
/// low limit (2 MiB), the low run spilled the node and peaked at 1 MiB, the
/// ample run wrote nothing, and both produced the same output.
fn passing_pair() -> (PressureRun, PressureRun) {
    let low = PressureRun {
        limit_bytes: 2 * MIB,
        peak_charged_bytes: MIB,
        node_spill_bytes: 4 * MIB,
        total_spill_bytes: 4 * MIB,
        output: b"k,n\na,1\n".to_vec(),
    };
    let ample = PressureRun {
        limit_bytes: 512 * MIB,
        peak_charged_bytes: 10 * MIB,
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
#[should_panic(expected = "state fits under the low limit")]
fn a_state_that_fits_the_low_limit_is_refused() {
    let (low, mut ample) = passing_pair();
    ample.peak_charged_bytes = MIB;
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

/// A two-row CSV Source feeding a CSV Sink.
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

#[test]
fn from_report_reads_the_charged_peak_and_spill_attribution() {
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
    let mut report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
            .expect("the two-row passthrough completes");
    let output = buf.contents();
    assert!(!output.is_empty(), "the sink wrote the two rows");

    let run = PressureRun::from_report(&report, "out", 512 * MIB, output.clone());
    assert_eq!(run.limit_bytes, 512 * MIB);
    assert_eq!(run.peak_charged_bytes, report.peak_consumer_usage_bytes);
    assert_eq!(run.total_spill_bytes, report.cumulative_spill_bytes);
    assert_eq!(
        run.node_spill_bytes,
        report
            .per_stage_spill_bytes
            .get("out")
            .copied()
            .unwrap_or(0)
    );
    assert_eq!(run.output, output);

    // Ample memory: nothing reached disk, so the node reads as unspilled.
    assert_eq!(run.total_spill_bytes, 0);
    assert_eq!(run.node_spill_bytes, 0);

    // The node's own attribution is read, not another node's or the total.
    report.per_stage_spill_bytes.insert("events".to_string(), 7);
    report.per_stage_spill_bytes.insert("out".to_string(), 11);
    report.cumulative_spill_bytes = 18;
    let attributed = PressureRun::from_report(&report, "out", 512 * MIB, output);
    assert_eq!(attributed.node_spill_bytes, 11);
    assert_eq!(attributed.total_spill_bytes, 18);
}
