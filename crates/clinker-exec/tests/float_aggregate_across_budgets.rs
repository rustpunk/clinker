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

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, MemoryTestOverrides, PipelineExecutor, PipelineRunParams,
};
use clinker_plan::config::{CompileContext, parse_config};

/// One run's report, its CSV Sink's bytes and every dead-letter row it wrote.
struct Run {
    report: ExecutionReport,
    csv: String,
    dlq: Vec<dlq_sink::DlqRow>,
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
    // Every Sink other than `csv` writes into a buffer the test never reads.
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = config
        .sink_configs()
        .map(|sink| {
            let target = if sink.name == "csv" {
                buffer.clone()
            } else {
                SharedBuffer::new()
            };
            (
                sink.name.clone(),
                Box::new(target) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
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
    let sink = dlq_sink::CollectingDlqSink::new();
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        dlq_sink::registry(writers, &sink),
        &params,
    )
    .expect("the pipeline runs to completion");
    Run {
        report,
        csv: buffer.as_string(),
        dlq: sink.rows(),
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

/// Ledger capacity of the spilling run: 1 MiB (1,048,576 bytes).
///
/// With ample memory the fixture charges P = 2,229,080 bytes at its peak, of
/// which the Aggregate's own state is 1,277,952: 4,096 groups, each holding
/// its exact float sum's 312-byte state. It completes at M = 767,592 bytes
/// and is refused at 744,564, where the Aggregate's 4,096 buffered output
/// rows (720,896 bytes, which cannot spill) no longer fit. 1 MiB lies between
/// M and P. The hash table's periodic memory check spills it at row 4,096,
/// where the first pass ends, so the first pass's floats and the second
/// pass's integers reach the finalize merge in different runs. The fixture
/// is new, so there is no earlier limit L; every run's `memory.limit` is
/// 512M.
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

/// One Source with `correlation_key: order_id` feeding two branches. `prep`
/// converts the price and weight columns to floats for a relaxed hash
/// Aggregate (its `group_by` omits the correlation key) emitting the two
/// bindings that retract by subtraction, `avg` and `weighted_avg`. `check`
/// converts the quantity column to an integer and writes it to a second Sink,
/// so a line with a bad quantity fails after its price and weight have
/// already entered the Aggregate.
const RETRACT_YAML: &str = r#"
pipeline:
  name: float_avg_retract
  memory: { limit: "512M", backpressure: spill }
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      correlation_key: order_id
      schema:
        - { name: order_id, type: string }
        - { name: department, type: string }
        - { name: price, type: string }
        - { name: weight, type: string }
        - { name: qty, type: string }
  - type: transform
    name: prep
    input: src
    config:
      cxl: |
        emit department = department
        emit p = price.to_float()
        emit w = weight.to_float()
  - type: aggregate
    name: grouped
    input: prep
    config:
      group_by: [department]
      cxl: |
        emit department = department
        emit mean = avg(p)
        emit weighted = weighted_avg(p, w)
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
  - type: transform
    name: check
    input: src
    config:
      cxl: |
        emit order_id = order_id
        emit n = qty.to_int()
  - type: sink
    name: checked
    input: check
    config:
      name: checked
      type: csv
      path: checked.csv
      include_unmapped: true
"#;

/// Two lines carry a bad quantity, `O2,HR` and `O3,ENG`: one failure in each
/// department, in the middle of the arrival order. The prices and weights are not exactly representable, so a
/// subtraction that rounded at each step would differ from a fresh fold in
/// the last digits.
const RETRACT_ROWS: &str = "\
order_id,department,price,weight,qty
O1,HR,0.1,0.3,3
O2,HR,0.2,0.7,BAD
O3,HR,0.7,1.1,7
O4,ENG,1.1,2.3,2
O5,ENG,2.2,0.9,4
O3,ENG,3.3,1.9,BAD
O6,HR,0.3,0.1,11
O7,ENG,0.6,3.7,6
";

/// `RETRACT_ROWS` without the two lines whose quantity is bad. A failed line
/// is retracted on its own: its order's other lines stay in the Aggregate.
const RETRACT_BASELINE_ROWS: &str = "\
order_id,department,price,weight,qty
O1,HR,0.1,0.3,3
O3,HR,0.7,1.1,7
O4,ENG,1.1,2.3,2
O5,ENG,2.2,0.9,4
O6,HR,0.3,0.1,11
O7,ENG,0.6,3.7,6
";

/// A relaxed Aggregate of float `avg` and `weighted_avg` retracts a line that
/// failed on another branch, after its price and weight were folded in, to the
/// bytes of a rerun over the input without that line.
#[test]
fn relaxed_avg_and_weighted_avg_retract_like_a_baseline_rerun() {
    let failed = run(RETRACT_YAML, RETRACT_ROWS, None);
    let baseline = run(RETRACT_YAML, RETRACT_BASELINE_ROWS, None);

    assert!(baseline.dlq.is_empty(), "the baseline has no failure");
    let rows: Vec<_> = failed
        .dlq
        .iter()
        .map(|row| (row.source_file(), row.error_detail()))
        .collect();
    assert_eq!(
        failed.dlq.iter().filter(|row| row.trigger()).count(),
        2,
        "exactly the two lines that failed to convert trigger: {rows:?}"
    );

    assert_eq!(
        sorted_lines(&failed.csv),
        sorted_lines(&baseline.csv),
        "the retract-corrected output must equal the baseline rerun's:\n\
         got:\n{}\nbaseline:\n{}",
        failed.csv,
        baseline.csv
    );
}
