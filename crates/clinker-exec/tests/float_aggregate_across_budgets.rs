//! Float `sum` gives the same answer whether an Aggregate holds every group in
//! memory or spills its hash table and merges partial states.
//!
//! A spilled hash Aggregate drains its whole table into one run at each spill,
//! and at finalize merges each group's partial states from the runs in the
//! order the runs were written. A group whose rows arrive in two passes, a
//! float first and its integers later, therefore meets its float in an early
//! run and its integers in later runs that hold no float at all. Merging those
//! partials must keep every addend: the integers are added exactly and the
//! float sum is rounded once, at the end (#1289).
//!
//! The mixed column comes from an upstream Transform whose `if` has an
//! integer and a float branch, because a typed Source holds one type per
//! column.

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
use clinker_plan::config::{CompileContext, parse_config};

/// One run's report and its CSV Sink's bytes.
struct Run {
    report: ExecutionReport,
    csv: String,
}

/// Run a one-Source pipeline over `csv` fed to the Source `src`, capturing
/// the Sink `csv`, held to `capacity` bytes of ledger when one is given.
fn run(yaml: &str, csv: &str, capacity: Option<u64>) -> Run {
    let mut config = parse_config(yaml).expect("fixture parses");
    let context = CompileContext::default();
    resource_fixtures::add_csv_workspace(&mut config, &context);
    let plan = config.compile(&context).expect("fixture compiles");
    let readers = HashMap::from([(
        "src".to_string(),
        resource_fixtures::predecoded_csv_source(&config, &context, "src", &[("in.csv", csv)]),
    )]);
    let buffer = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "csv".to_string(),
        Box::new(buffer.clone()) as Box<dyn std::io::Write + Send>,
    )]);
    let memory_test = match capacity {
        Some(bytes) => MemoryTestOverrides::default().with_ledger_capacity(bytes),
        None => MemoryTestOverrides::default(),
    };
    let params = PipelineRunParams {
        execution_id: "float-aggregate-across-budgets".to_string(),
        batch_id: "b".to_string(),
        memory_test,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("the pipeline runs to completion");
    Run {
        report,
        csv: buffer.as_string(),
    }
}

/// The Sink's lines, sorted: a hash Aggregate writes its groups in hash-table
/// order, which differs between two runs with ample memory.
fn sorted_lines(output: &str) -> Vec<String> {
    let mut lines: Vec<String> = output.lines().map(str::to_string).collect();
    lines.sort();
    lines
}

/// Groups in the two-pass fixture. The hash table's periodic memory check
/// runs every 4,096 folded rows, so with 4,096 groups it falls exactly where
/// the first pass ends.
const TWO_PASS_GROUPS: i64 = 4_096;

/// A Source of `g`, `kind` and `num`; a Transform `vals` turning them into the
/// mixed column `v` (a float for kind `f`, else an integer); and a hash
/// Aggregate `grouped` emitting `sum(v)` per `g` to the CSV Sink.
const TWO_PASS_YAML: &str = r#"
pipeline:
  name: float_sum_two_passes
  memory: { limit: "512M", backpressure: spill }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: g, type: int }
        - { name: kind, type: string }
        - { name: num, type: string }
  - type: transform
    name: vals
    input: src
    config:
      cxl: |
        emit v = if kind == "f" then num.to_float() else num.to_int()
  - type: aggregate
    name: grouped
    input: vals
    config:
      group_by: [g]
      cxl: |
        emit s = sum(v)
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// The first pass gives every group the float `0.5`; the second gives every
/// group the integers `1`, `2` and `3`, round by round.
fn two_pass_rows() -> String {
    let mut csv = String::from("g,kind,num\n");
    for g in 0..TWO_PASS_GROUPS {
        csv.push_str(&format!("{g},f,0.5\n"));
    }
    for n in 1..=3 {
        for g in 0..TWO_PASS_GROUPS {
            csv.push_str(&format!("{g},i,{n}\n"));
        }
    }
    csv
}

/// Ledger capacity of the spilling run.
const TWO_PASS_SPILL_CAPACITY: u64 = 1024 * 1024;

/// A group whose float lands in one spill run and whose integers land only in
/// later runs still sums every value: `0.5 + 1 + 2 + 3 = 6.5` in every group,
/// with ample memory and with a capacity at which the hash table spills
/// between the two passes.
#[test]
fn mixed_integer_float_sum_survives_a_spill_with_an_integer_only_run() {
    let rows = two_pass_rows();
    let mut expected: Vec<String> = (0..TWO_PASS_GROUPS).map(|g| format!("{g},6.5")).collect();
    expected.push("g,s".to_string());
    expected.sort();

    let ample = run(TWO_PASS_YAML, &rows, None);
    assert_eq!(sorted_lines(&ample.csv), expected, "ample memory");

    let spilled = run(TWO_PASS_YAML, &rows, Some(TWO_PASS_SPILL_CAPACITY));
    assert_eq!(
        sorted_lines(&spilled.csv),
        expected,
        "spilled at {TWO_PASS_SPILL_CAPACITY} bytes"
    );

    memory_pressure::assert_spill_engaged(&spilled.report);
    memory_pressure::assert_capacity_below_ample_peak(TWO_PASS_SPILL_CAPACITY, &ample.report);
    assert!(
        spilled
            .report
            .per_stage_spill_bytes_written
            .get("grouped")
            .is_some_and(|bytes| *bytes > 0),
        "the Aggregate itself must spill at {TWO_PASS_SPILL_CAPACITY} bytes: {:?}",
        spilled.report.per_stage_spill_bytes_written
    );
}
