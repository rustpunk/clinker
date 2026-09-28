//! The test levers on a run's memory figures.
//!
//! A test can hold a run's ledger to a small capacity while `memory.limit`
//! stays ample, so the startup check (E312) judges a real, satisfiable limit
//! and the run still takes the reserve, shortfall, spill and E310 paths a
//! small limit forces. An in-process run judges `memory.limit` against an
//! injected baseline rather than the shared test process's resident memory,
//! and a whole-unit spill-then-reload test can force exactly one shortfall.

#![cfg(feature = "test-utils")]

use std::collections::HashMap;
use std::io::Write;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, IN_PROCESS_BASELINE_BYTES, MemoryTestOverrides, PipelineExecutor,
    PipelineRunParams, SourceReaders, single_file_reader,
};
use clinker_plan::config::{CompileContext, PipelineConfig};
use clinker_plan::error::PipelineError;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

/// Source → Route(a, b, c) → three Outputs. Every row is admitted to the
/// Source's own node buffer, whose spill arm fires once the soft threshold
/// of a 1 MiB limit is crossed.
fn route_fanout_yaml(limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: ledger_capacity_route_fanout
  memory: {{ limit: "{limit}", backpressure: spill }}
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: region, type: string }}
        - {{ name: payload, type: string }}
        - {{ name: value, type: int }}
        - {{ name: ts, type: int }}
  - type: route
    name: by_region
    input: events
    config:
      mode: exclusive
      conditions:
        a: "region == \"a\""
        b: "region == \"b\""
      default: c
  - type: sink
    name: out_a
    input: by_region.a
    config:
      name: out_a
      type: csv
      path: out_a.csv
  - type: sink
    name: out_b
    input: by_region.b
    config:
      name: out_b
      type: csv
      path: out_b.csv
  - type: sink
    name: out_c
    input: by_region.c
    config:
      name: out_c
      type: csv
      path: out_c.csv
"#
    )
}

const ROWS_A: usize = 1_500;
const ROWS_B: usize = 300;
const ROWS_C: usize = 200;

fn events_csv() -> String {
    let mut csv = String::from("id,region,payload,value,ts\n");
    let mut id = 0u64;
    for (region, count) in [('a', ROWS_A), ('b', ROWS_B), ('c', ROWS_C)] {
        for _ in 0..count {
            id += 1;
            csv.push_str(&format!("id_{id},{region},payload_{id},{id},{id}\n"));
        }
    }
    csv
}

fn events_reader(csv: String) -> SourceReaders {
    HashMap::from([(
        "events".to_string(),
        single_file_reader(
            "events.csv",
            Box::new(std::io::Cursor::new(csv.into_bytes())),
        ),
    )])
}

/// One route fan-out run: its report and each branch's output.
struct FanoutRun {
    report: ExecutionReport,
    outputs: [Vec<u8>; 3],
}

fn run_route_fanout(limit: &str, memory_test: MemoryTestOverrides) -> FanoutRun {
    let config: PipelineConfig =
        clinker_plan::yaml::from_str(&route_fanout_yaml(limit)).expect("parse route fan-out");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile route fan-out");
    let buffers = [
        SharedBuffer::new(),
        SharedBuffer::new(),
        SharedBuffer::new(),
    ];
    let writers: HashMap<String, Box<dyn Write + Send>> = ["out_a", "out_b", "out_c"]
        .into_iter()
        .zip(buffers.iter())
        .map(|(name, buffer)| {
            (
                name.to_string(),
                Box::new(buffer.clone()) as Box<dyn Write + Send>,
            )
        })
        .collect();
    let params = PipelineRunParams {
        memory_test,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        events_reader(events_csv()),
        writers,
        &params,
    )
    .expect("the route fan-out completes");
    FanoutRun {
        report,
        outputs: buffers.map(|buffer| buffer.contents()),
    }
}

fn data_rows(output: &[u8]) -> usize {
    String::from_utf8_lossy(output)
        .lines()
        .skip(1)
        .filter(|line| !line.is_empty())
        .count()
}

fn spill_bytes_written(report: &ExecutionReport) -> u64 {
    report.per_stage_spill_bytes_written.values().sum()
}

#[test]
fn override_runs_like_the_limit_it_replaces() {
    let at_limit = run_route_fanout("1M", MemoryTestOverrides::default());
    let at_capacity = run_route_fanout(
        "512M",
        MemoryTestOverrides::default().with_ledger_capacity(MIB),
    );

    for (name, run) in [("1M limit", &at_limit), ("1 MiB capacity", &at_capacity)] {
        assert_eq!(
            run.report.memory_limit_bytes, MIB,
            "{name}: the arbitrator enforces 1 MiB"
        );
        assert!(
            spill_bytes_written(&run.report) > 0,
            "{name}: the run spilled: {:?}",
            run.report.per_stage_spill_bytes_written
        );
        assert_eq!(
            run.outputs.each_ref().map(|output| data_rows(output)),
            [ROWS_A, ROWS_B, ROWS_C],
            "{name}: per-branch row counts"
        );
    }
    assert_eq!(
        at_limit.outputs, at_capacity.outputs,
        "a capacity override produces the bytes the same limit does"
    );
}

/// Source → Output under the default `pause` policy, one small CSV.
fn passthrough_yaml(limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: ledger_capacity_passthrough
  memory: {{ limit: "{limit}" }}
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      schema:
        - {{ name: id, type: string }}
        - {{ name: region, type: string }}
        - {{ name: payload, type: string }}
        - {{ name: value, type: int }}
        - {{ name: ts, type: int }}
  - type: sink
    name: out
    input: events
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

fn run_passthrough(
    limit: &str,
    memory_test: MemoryTestOverrides,
) -> Result<ExecutionReport, PipelineError> {
    let config: PipelineConfig =
        clinker_plan::yaml::from_str(&passthrough_yaml(limit)).expect("parse passthrough");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile passthrough");
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(SharedBuffer::new()) as Box<dyn Write + Send>,
    )]);
    let params = PipelineRunParams {
        memory_test,
        ..Default::default()
    };
    let csv = "id,region,payload,value,ts\nid_1,a,payload_1,1,1\nid_2,b,payload_2,2,2\n";
    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        events_reader(csv.to_string()),
        writers,
        &params,
    )
}

#[test]
fn override_never_raises_the_limit() {
    let report = run_passthrough(
        "64M",
        MemoryTestOverrides::default().with_ledger_capacity(1 << 30),
    )
    .expect("a 64M passthrough completes");
    assert_eq!(
        report.memory_limit_bytes,
        64 * MIB,
        "a capacity above memory.limit leaves the limit in force"
    );
}

#[test]
fn override_keeps_e312_on_the_configured_limit() {
    match run_passthrough("1", MemoryTestOverrides::default()) {
        Err(PipelineError::UnsatisfiableMemoryBudget { limit, .. }) => assert_eq!(limit, 1),
        other => panic!("a 1-byte memory.limit under pause is refused at startup: {other:?}"),
    }

    // The same pipeline at an ample limit held to 1 KiB passes the startup
    // check; whatever the 1 KiB ledger then does, it is not an E312.
    let held = run_passthrough(
        "512M",
        MemoryTestOverrides::default().with_ledger_capacity(KIB),
    );
    assert!(
        !matches!(held, Err(PipelineError::UnsatisfiableMemoryBudget { .. })),
        "the startup check judges memory.limit, not the capacity: {held:?}"
    );
    if let Ok(report) = held {
        assert_eq!(report.memory_limit_bytes, KIB);
    }
}

#[test]
fn injected_baseline_decides_e312() {
    match run_passthrough(
        "64M",
        MemoryTestOverrides::default().with_baseline_rss(100 * MIB),
    ) {
        Err(PipelineError::UnsatisfiableMemoryBudget {
            limit,
            baseline_rss,
        }) => {
            assert_eq!(limit, 64 * MIB);
            assert_eq!(baseline_rss, 104_857_600, "the injected baseline is judged");
        }
        other => panic!("64M is below an injected 100 MiB baseline: {other:?}"),
    }

    run_passthrough("64M", MemoryTestOverrides::default().with_baseline_rss(MIB))
        .expect("64M is above an injected 1 MiB baseline");
}

#[test]
fn in_process_default_injects_the_baseline() {
    assert_eq!(IN_PROCESS_BASELINE_BYTES, 16 * MIB);
    assert_eq!(
        MemoryTestOverrides::default().injected_baseline_rss(),
        Some(16 * MIB),
        "an in-process run judges memory.limit against the injected baseline"
    );
    assert_eq!(
        MemoryTestOverrides::default()
            .with_process_memory()
            .injected_baseline_rss(),
        None,
        "opting into process memory measures the baseline"
    );
    assert_eq!(MemoryTestOverrides::process().injected_baseline_rss(), None);
}
