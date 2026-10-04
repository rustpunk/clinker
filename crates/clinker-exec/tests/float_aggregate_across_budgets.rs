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

// ---- an ill-conditioned column, resident, spilled, streamed and windowed ----

/// Groups in the ill-conditioned fixture. The hash table's periodic memory
/// check runs every 4,096 folded rows, so with 4,096 groups it falls exactly
/// where each pass ends.
const ILL_GROUPS: i64 = 4_096;

/// The event time of every row: one tumbling window holds the whole input.
const ILL_EVENT_TS: &str = "2026-05-14T10:00:00";

/// Each group's `(x, d)` cells, pass by pass: a float column whose partial
/// sums are not representable, and a decimal column of the same shape.
const ILL_PASSES: [(&str, &str); 3] = [
    ("10000000000000000", "10000000000000000.1"),
    ("1", "0.01"),
    ("-10000000000000000", "-10000000000000000.1"),
];

/// How the Aggregate runs over the ill-conditioned fixture.
#[derive(Clone, Copy, Debug)]
enum Strategy {
    /// The default (hash) strategy.
    Hash,
    /// `strategy: streaming`, over a Source that declares its order on `g`.
    Streaming,
    /// A tumbling time window over a Source that declares a watermark.
    Window,
}

/// A typed Source of `g`, `x`, `w`, `d` and `event_ts` feeding an Aggregate
/// `grouped` directly, with no Transform, emitting the float `sum`, `avg` and
/// `weighted_avg` of `x` and the decimal `sum` of `d` per `g`.
fn ill_conditioned_yaml(strategy: Strategy) -> String {
    let (source_extra, aggregate_extra, key_emit) = match strategy {
        Strategy::Hash => ("", "", ""),
        Strategy::Streaming => (
            "\n      sort_order:\n        - { field: g, order: asc }",
            "\n      strategy: streaming",
            "",
        ),
        Strategy::Window => (
            "\n      watermark: { column: event_ts }",
            "\n      time_window:\n        tumbling: { size: 1h }",
            // A windowed Aggregate writes only the columns it emits.
            "\n        emit g = g",
        ),
    };
    format!(
        r#"
pipeline:
  name: ill_conditioned_sum
  memory: {{ limit: "512M", backpressure: spill }}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv{source_extra}
      schema:
        - {{ name: g, type: int }}
        - {{ name: x, type: float }}
        - {{ name: w, type: int }}
        - {{ name: d, type: decimal }}
        - {{ name: event_ts, type: date_time }}
  - type: aggregate
    name: grouped
    input: src
    config:
      group_by: [g]{aggregate_extra}
      cxl: |{key_emit}
        emit s = sum(x)
        emit a = avg(x)
        emit wa = weighted_avg(x, w)
        emit ds = sum(d)
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
"#
    )
}

/// The fixture's rows. In passes (every group's first value, then every
/// group's second, then every group's third) unless `by_group`, which writes
/// each group's three rows together, in pass order, for the streaming
/// strategy's sorted input.
fn ill_conditioned_rows(by_group: bool) -> String {
    let mut cells: Vec<(i64, usize)> = Vec::new();
    if by_group {
        for g in 0..ILL_GROUPS {
            cells.extend((0..ILL_PASSES.len()).map(|pass| (g, pass)));
        }
    } else {
        for pass in 0..ILL_PASSES.len() {
            cells.extend((0..ILL_GROUPS).map(|g| (g, pass)));
        }
    }
    let mut csv = String::from("g,x,w,d,event_ts\n");
    for (g, pass) in cells {
        let (x, d) = ILL_PASSES[pass];
        csv.push_str(&format!("{g},{x},1,{d},{ILL_EVENT_TS}\n"));
    }
    csv
}

/// Group `g`'s expected line. The CSV writer prints a float with Rust's
/// shortest round-trip form, so `1.0` is `1` and the double nearest 1/3 is
/// `0.3333333333333333`, and a decimal at its scale, so `0.01` is `0.01`.
fn ill_conditioned_line(g: i64) -> String {
    format!("{g},1,0.3333333333333333,0.3333333333333333,0.01")
}

/// Ledger capacity of the spilling ill-conditioned run: 6 MiB (6,291,456
/// bytes).
///
/// With ample memory the fixture charges P = 9,437,784 bytes at its peak, of
/// which the Aggregate's own state is 4,915,200 and the Source's buffer
/// feeding it 4,521,984. It completes at M = 4,523,000 bytes and is refused
/// at 4,520,000, where the Source buffer's materialization (4,522,584 bytes
/// projected) no longer fits. 6 MiB lies between M and P. At it the hash
/// table spills at rows 2,098, 4,096, 6,194, 8,192, 10,290 and 12,288: its
/// periodic memory check spills it at 4,096 and 8,192, where the first and
/// second passes end, so no spill run holds rows of two passes and each
/// group's three addends reach the finalize merge from three different runs.
/// The fixture is new, so there is no earlier limit L; every run's
/// `memory.limit` is 512M.
const ILL_SPILL_CAPACITY: u64 = 6 * 1024 * 1024;

/// `sum`, `avg` and `weighted_avg` of an ill-conditioned float column, and
/// `sum` of a decimal column, are exact whether the Aggregate holds every
/// group in memory, spills its hash table between passes so that each
/// pass's partial state is merged from its own run, streams over input
/// sorted on the group key, or runs as a tumbling time window.
///
/// Every group's `x` is `1e16`, `1` and `-1e16`, with weight `1`. The exact
/// total is `1`. A left-to-right fold gives `0`: `1e16 + 1` lies halfway
/// between the doubles `1e16` and `1e16 + 2` (the spacing of doubles there is
/// 2) and rounds to the even `1e16`, which `-1e16` then cancels. `-1e16 + 1`
/// rounds to `-1e16` the same way, so every fold that does not add the `1`
/// last loses it, and only an exact sum gives `1` whatever the order, the
/// spill runs or the merge. A column whose partial sums are all representable
/// gives the same total in every order, so it cannot tell an exact sum from
/// an order-dependent one; this one can. `avg` and `weighted_avg` divide the exact total,
/// rounded once, by 3, which gives the double nearest 1/3. The decimal `d` is
/// `10000000000000000.1`, `0.01` and `-10000000000000000.1`: its exact total
/// is `0.01`, at the largest input scale, 2.
///
/// A hash Aggregate writes its groups in hash-table order, so those outputs
/// are compared as sets of lines; the streaming run's output is in key order
/// and is compared byte for byte.
#[test]
fn an_ill_conditioned_sum_is_exact_at_every_capacity_and_strategy() {
    let mut expected: Vec<String> = (0..ILL_GROUPS).map(ill_conditioned_line).collect();
    expected.push("g,s,a,wa,ds".to_string());
    expected.sort();
    let passes = ill_conditioned_rows(false);

    let ample = run(&ill_conditioned_yaml(Strategy::Hash), &passes, None);
    assert_eq!(sorted_lines(&ample.csv), expected, "hash, ample memory");

    let spilled = run(
        &ill_conditioned_yaml(Strategy::Hash),
        &passes,
        Some(ILL_SPILL_CAPACITY),
    );
    assert_eq!(
        sorted_lines(&spilled.csv),
        expected,
        "hash, spilled at {ILL_SPILL_CAPACITY} bytes"
    );
    memory_pressure::assert_spill_engaged(&spilled.report);
    memory_pressure::assert_capacity_below_ample_peak(ILL_SPILL_CAPACITY, &ample.report);
    assert!(
        spilled
            .report
            .per_stage_spill_bytes_written
            .get("grouped")
            .is_some_and(|bytes| *bytes > 0),
        "the Aggregate itself must spill at {ILL_SPILL_CAPACITY} bytes: {:?}",
        spilled.report.per_stage_spill_bytes_written
    );

    let streamed = run(
        &ill_conditioned_yaml(Strategy::Streaming),
        &ill_conditioned_rows(true),
        None,
    );
    let mut in_key_order = String::from("g,s,a,wa,ds\n");
    for g in 0..ILL_GROUPS {
        in_key_order.push_str(&ill_conditioned_line(g));
        in_key_order.push('\n');
    }
    assert_eq!(streamed.csv, in_key_order, "streaming");

    let windowed = run(&ill_conditioned_yaml(Strategy::Window), &passes, None);
    assert_eq!(sorted_lines(&windowed.csv), expected, "time window");
}
