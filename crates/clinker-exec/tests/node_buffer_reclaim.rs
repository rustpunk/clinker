//! Node-buffer slots answer the ledger: a short request on the walk spills
//! another resident slot before it is refused.

#![cfg(feature = "test-utils")]

#[path = "common/memory_pressure.rs"]
mod memory_pressure;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, MemoryTestOverrides, PipelineExecutor, PipelineRunParams,
};
use clinker_plan::config::{CompileContext, PipelineConfig};
use memory_pressure::{assert_capacity_below_ample_peak, assert_spill_engaged};
use std::collections::HashMap;
use std::io::Write;

/// `events` feeds both the Route and a copy Output, so the Route reads a
/// shared scan and collects it into a resident vector (a transient
/// materialization). `others` is published before the Route runs and read
/// only by the last Output, so its slot is resident while the Route
/// materializes.
const MATERIALIZATION_YAML: &str = r#"
pipeline:
  name: node_buffer_reclaim_materialization
  memory: { limit: "512M", backpressure: spill }
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
  - type: source
    name: others
    config:
      name: others
      type: csv
      path: others.csv
      schema:
        - { name: id, type: string }
        - { name: payload, type: string }
        - { name: value, type: int }
  - type: route
    name: by_region
    input: events
    config:
      mode: exclusive
      conditions:
        a: "region == \"a\""
      default: rest
  - type: sink
    name: out_a
    input: by_region.a
    config: { name: out_a, type: csv, path: out_a.csv }
  - type: sink
    name: out_rest
    input: by_region.rest
    config: { name: out_rest, type: csv, path: out_rest.csv }
  - type: sink
    name: out_all
    input: events
    config: { name: out_all, type: csv, path: out_all.csv }
  - type: sink
    name: out_others
    input: others
    config: { name: out_others, type: csv, path: out_others.csv }
"#;

const EVENT_ROWS: usize = 2_000;
const OTHER_ROWS: usize = 2_000;

fn events_csv() -> String {
    let mut csv = String::from("id,region,payload,value\n");
    for row in 0..EVENT_ROWS {
        let region = if row % 3 == 0 { "a" } else { "b" };
        csv.push_str(&format!("e{row},{region},payload-{row:05},{row}\n"));
    }
    csv
}

fn others_csv() -> String {
    let mut csv = String::from("id,payload,value\n");
    for row in 0..OTHER_ROWS {
        csv.push_str(&format!("o{row},other-{row:05},{row}\n"));
    }
    csv
}

const OUTPUTS: [&str; 4] = ["out_a", "out_rest", "out_all", "out_others"];

/// Run the materialization fixture at its ample `memory.limit`, held to
/// `capacity` bytes of ledger when one is given; return its report and every
/// Output's bytes.
fn run_materialization(capacity: Option<u64>) -> (ExecutionReport, Vec<u8>) {
    let config: PipelineConfig =
        clinker_plan::yaml::from_str(MATERIALIZATION_YAML).expect("fixture parses");
    let plan = config
        .compile(&CompileContext::default())
        .expect("fixture compiles");
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([
        (
            "events".to_string(),
            clinker_exec::executor::single_file_reader(
                "events.csv",
                Box::new(std::io::Cursor::new(events_csv().into_bytes())),
            ),
        ),
        (
            "others".to_string(),
            clinker_exec::executor::single_file_reader(
                "others.csv",
                Box::new(std::io::Cursor::new(others_csv().into_bytes())),
            ),
        ),
    ]);
    let buffers: Vec<SharedBuffer> = OUTPUTS.iter().map(|_| SharedBuffer::new()).collect();
    let writers: HashMap<String, Box<dyn Write + Send>> = OUTPUTS
        .iter()
        .zip(&buffers)
        .map(|(name, buffer)| {
            (
                name.to_string(),
                Box::new(buffer.clone()) as Box<dyn Write + Send>,
            )
        })
        .collect();
    let memory_test = match capacity {
        Some(bytes) => MemoryTestOverrides::default().with_ledger_capacity(bytes),
        None => MemoryTestOverrides::default(),
    };
    let params = PipelineRunParams {
        execution_id: "node-buffer-reclaim".to_string(),
        batch_id: "batch-0".to_string(),
        memory_test,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("the materialization fixture completes");
    let mut output = Vec::new();
    for (name, buffer) in OUTPUTS.iter().zip(&buffers) {
        output.extend_from_slice(name.as_bytes());
        output.push(b'\n');
        output.extend_from_slice(buffer.as_string().as_bytes());
    }
    (report, output)
}

/// The ledger capacity the materialization run is held to. The Route's scan
/// collects the whole `events` input while the `others` slot is resident,
/// and at this capacity both do not fit: the Route's reservation completes
/// only by spilling `others` first. It is below the charged peak of the
/// same input with ample memory, 1,387,312 bytes.
const MATERIALIZATION_CAPACITY: u64 = 1_000_000;

#[test]
fn materialization_reclaims_another_slot_before_refusing() {
    let (low, low_output) = run_materialization(Some(MATERIALIZATION_CAPACITY));
    let (ample, ample_output) = run_materialization(None);
    assert_spill_engaged(&low);
    assert_capacity_below_ample_peak(MATERIALIZATION_CAPACITY, &ample);
    assert!(
        low.per_stage_spill_bytes_written
            .get("others")
            .is_some_and(|&bytes| bytes > 0),
        "the resident `others` slot must be spilled to make room: {:?}",
        low.per_stage_spill_bytes_written
    );
    assert!(
        ample.per_stage_spill_bytes_written.is_empty(),
        "ample memory spills nothing: {:?}",
        ample.per_stage_spill_bytes_written
    );
    assert!(
        low_output == ample_output,
        "spilling a slot to make room must not change any Output's bytes"
    );
}
