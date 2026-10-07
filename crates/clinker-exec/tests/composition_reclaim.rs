//! A request made inside a composition body that does not fit spills a
//! resident node-buffer slot of the body's caller before it is refused.

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
use std::path::PathBuf;

/// `widened` feeds the composition and, with the composition's output, the
/// `both` Merge, which cannot run before the call site returns. The
/// composition takes a copy of the rows, so the `widened` slot stays resident
/// in the caller's scope for the whole body. The body doubles every row and
/// hands the result back to its call site, which materializes it inside the
/// body's scope.
const PIPELINE_YAML: &str = r#"
pipeline:
  name: composition_reclaim
  memory: { limit: "512M", backpressure: spill }
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      schema:
        - { name: a, type: int }
  - type: transform
    name: widened
    input: events
    config:
      cxl: |
        emit a = a
        emit computed = a
  - type: composition
    name: double_call
    input: widened
    use: ../compositions/exec_transform_check.comp.yaml
    inputs:
      inp: widened
  - type: merge
    name: both
    inputs:
      - double_call
      - widened
  - type: sink
    name: out
    input: both
    config: { name: out, type: csv, path: out.csv }
"#;

const EVENT_ROWS: usize = 1_500;

const OUTPUTS: [&str; 1] = ["out"];

fn events_csv() -> String {
    let mut csv = String::from("a\n");
    for row in 0..EVENT_ROWS {
        csv.push_str(&format!("{row}\n"));
    }
    csv
}

fn fixture_workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
}

/// Run the fixture at its ample `memory.limit`, held to `capacity` bytes of
/// ledger when one is given; return its report and every Output's bytes.
fn run(capacity: Option<u64>) -> (ExecutionReport, Vec<u8>) {
    let config: PipelineConfig =
        clinker_plan::yaml::from_str(PIPELINE_YAML).expect("fixture parses");
    let plan = config
        .compile(&CompileContext::with_pipeline_dir(
            fixture_workspace_root(),
            PathBuf::from("pipelines"),
        ))
        .expect("fixture compiles");
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(events_csv().into_bytes())),
        ),
    )]);
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
        execution_id: "composition-reclaim".to_string(),
        batch_id: "batch-0".to_string(),
        memory_test,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("the composition fixture completes");
    let mut output = Vec::new();
    for (name, buffer) in OUTPUTS.iter().zip(&buffers) {
        output.extend_from_slice(name.as_bytes());
        output.push(b'\n');
        output.extend_from_slice(buffer.as_string().as_bytes());
    }
    (report, output)
}

/// The ledger capacity the low run is held to. The body's Source copies its
/// 408,000-byte seed while the seed and the caller's 408,000-byte `widened`
/// slot are both charged, which does not fit, so the copy completes only by
/// spilling `widened`. It is below the charged peak of the same input with
/// ample memory, 1,416,032 bytes, and leaves the Output's writer room to
/// stage its rows once the Merge's result is read back.
const CAPACITY: u64 = 1_200_000;

#[test]
fn body_request_spills_a_parent_slot() {
    let (low, low_output) = run(Some(CAPACITY));
    let (ample, ample_output) = run(None);
    assert_spill_engaged(&low);
    assert_capacity_below_ample_peak(CAPACITY, &ample);
    assert!(
        low.per_stage_spill_bytes_written
            .get("widened")
            .is_some_and(|&bytes| bytes > 0),
        "the caller's resident `widened` slot must be spilled to make room: {:?}",
        low.per_stage_spill_bytes_written
    );
    assert!(
        ample.per_stage_spill_bytes_written.is_empty(),
        "ample memory spills nothing: {:?}",
        ample.per_stage_spill_bytes_written
    );
    assert!(
        low_output == ample_output,
        "spilling a caller's slot to make room must not change any Output's bytes"
    );
}
