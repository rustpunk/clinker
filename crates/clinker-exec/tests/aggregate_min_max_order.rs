//! Aggregate `min` and `max` pick by the one value order, so their answer
//! depends only on the values a group holds.
//!
//! Numbers compare by exact value across integers, floats and decimals, NaN
//! is the largest value, and nulls are skipped. Among values the order ties
//! (`1` and `1.0`, `-0.0` and `0.0`, NaNs of either sign), `min` returns a
//! fixed representative — an integer before a decimal before a float, a
//! decimal with fewer fractional digits first, and the float with the smaller
//! sign — and `max` the reverse. No value is skipped as incomparable, so the
//! answer does not change with the order rows arrive in.
//!
//! NaN values and columns mixing integers with floats come from an upstream
//! Transform (`"NaN".to_float()`; an `if` whose branches are an integer and a
//! float), because a typed Source rejects non-finite floats and holds one type
//! per column. The CSV writer prints `NaN` but writes an integral float
//! without `.0`, and the JSON writer rejects a NaN but keeps `0` and `0.0`
//! apart, so a NaN result is checked in CSV and a tie representative in JSON.
//!
//! Because the answer depends only on the values, it is also the same when
//! the Aggregate's hash table spills and merges partial states from several
//! spill runs, under the streaming strategy, and through a time window. The
//! Transform that builds the mixed column does not emit the group key, so the
//! Source's declared order and watermark reach the Aggregate and both the
//! streaming and the time-window cases run as pipelines.

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
use clinker_plan::error::PipelineError;

/// The `memory.limit` every run is configured with; a spilling run is held to
/// a smaller ledger capacity through the test override.
const AMPLE_LIMIT: &str = "512M";

const TWO_POW_53: i64 = 9_007_199_254_740_992;

// ---- running pipelines ----------------------------------------------------

/// One run's report and each Sink's bytes, by Sink name.
struct Run {
    report: ExecutionReport,
    outputs: HashMap<String, String>,
}

impl Run {
    fn out(&self, sink: &str) -> &str {
        &self.outputs[sink]
    }
}

/// Run a one-Source pipeline over `csv` fed to the Source `src`, capturing
/// every Sink named in `sinks`, held to `capacity` bytes of ledger when one
/// is given.
fn try_run(
    yaml: &str,
    csv: &str,
    sinks: &[&str],
    capacity: Option<u64>,
) -> Result<Run, PipelineError> {
    let mut config = parse_config(yaml).expect("fixture parses");
    let context = CompileContext::default();
    resource_fixtures::add_csv_workspace(&mut config, &context);
    let plan = config.compile(&context).expect("fixture compiles");
    let readers = HashMap::from([(
        "src".to_string(),
        resource_fixtures::predecoded_csv_source(&config, &context, "src", &[("in.csv", csv)]),
    )]);
    let buffers: Vec<(String, SharedBuffer)> = sinks
        .iter()
        .map(|name| (name.to_string(), SharedBuffer::new()))
        .collect();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = buffers
        .iter()
        .map(|(name, buf)| {
            (
                name.clone(),
                Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let memory_test = match capacity {
        Some(bytes) => MemoryTestOverrides::default().with_ledger_capacity(bytes),
        None => MemoryTestOverrides::default(),
    };
    let params = PipelineRunParams {
        execution_id: "aggregate-min-max-order".to_string(),
        batch_id: "b".to_string(),
        memory_test,
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)?;
    let outputs = buffers
        .into_iter()
        .map(|(name, buf)| (name, buf.as_string()))
        .collect();
    Ok(Run { report, outputs })
}

fn run(yaml: &str, csv: &str, sinks: &[&str], capacity: Option<u64>) -> Run {
    try_run(yaml, csv, sinks, capacity).expect("the pipeline runs to completion")
}

/// Every ordering of `items`.
fn permutations<T: Clone>(items: &[T]) -> Vec<Vec<T>> {
    if items.len() <= 1 {
        return vec![items.to_vec()];
    }
    let mut all = Vec::new();
    for i in 0..items.len() {
        let mut rest = items.to_vec();
        let first = rest.remove(i);
        for mut tail in permutations(&rest) {
            tail.insert(0, first.clone());
            all.push(tail);
        }
    }
    all
}

// ---- tests ----------------------------------------------------------------

/// A column whose integer and float rows interleave, grouped into one group.
const INTERLEAVED_YAML: &str = r#"
pipeline:
  name: min_max_interleaved
  memory: { limit: "512M", backpressure: spill }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: k, type: string }
        - { name: n, type: int }
  - type: transform
    name: vals
    input: src
    config:
      cxl: |
        emit g = "all"
        emit m = if k == "a" then n else n.to_float() + 0.5
  - type: aggregate
    name: grouped
    input: vals
    config:
      group_by: [g]
      cxl: |
        emit hi = max(m)
        emit lo = min(m)
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
  - type: sink
    name: json
    input: grouped
    config:
      name: json
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: true
"#;

/// One group's `max` goes to a CSV Sink (which can print NaN) and its `min`
/// to a JSON Sink (which writes `0` and `0.0` apart).
const TIE_YAML: &str = r#"
pipeline:
  name: min_max_ties
  memory: { limit: "512M", backpressure: spill }
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      schema:
        - { name: id, type: string }
        - { name: kind, type: string }
        - { name: num, type: string }
  - type: transform
    name: vals
    input: src
    config:
      cxl: |
        emit g = "all"
        emit v = if num == "" then null else if kind == "f" then num.to_float() else num.to_int()
  - type: aggregate
    name: highest
    input: vals
    config:
      group_by: [g]
      cxl: |
        emit hi = max(v)
  - type: aggregate
    name: lowest
    input: vals
    config:
      group_by: [g]
      cxl: |
        emit lo = min(v)
  - type: sink
    name: csv
    input: highest
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
  - type: sink
    name: json
    input: lowest
    config:
      name: json
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: true
"#;

/// `min` and `max` over a column that mixes integers and floats are the true
/// extremes whatever order the rows arrive in, and among tied values they
/// return the same representative in every order.
#[test]
fn aggregate_min_max_ignore_arrival_order() {
    // (a,5) (b,3) (a,1) (b,7) give m = 5, 3.5, 1, 7.5: the largest value is
    // a float and the smallest an integer.
    let rows = ["a,5", "b,3", "a,1", "b,7"];
    let orders = permutations(&rows);
    assert_eq!(orders.len(), 24);
    for order in orders {
        let csv = format!("k,n\n{}\n", order.join("\n"));
        let out = run(INTERLEAVED_YAML, &csv, &["csv", "json"], None);
        assert_eq!(out.report.counters.dlq_count, 0, "no row is dead-lettered");
        assert_eq!(
            out.out("csv"),
            "g,hi,lo\nall,7.5,1\n",
            "CSV for arrival order {order:?}"
        );
        assert_eq!(
            out.out("json"),
            "{\"g\":\"all\",\"hi\":7.5,\"lo\":1}\n",
            "JSON for arrival order {order:?}"
        );
    }

    // Integer 0 ties -0.0 and 0.0, and `min` returns the integer; NaN is
    // above every number, so `max` is NaN; the integer 2^53 + 1 is above the
    // float 2^53; the null is skipped.
    let two_pow_53_plus_1 = TWO_POW_53 + 1;
    let rows = [
        "zero,i,0".to_string(),
        "neg_zero,f,-0.0".to_string(),
        "pos_zero,f,0.0".to_string(),
        "one,i,1".to_string(),
        "one_float,f,1.0".to_string(),
        format!("big,i,{two_pow_53_plus_1}"),
        format!("big_float,f,{TWO_POW_53}.0"),
        "nan,f,NaN".to_string(),
        "none,f,".to_string(),
    ];
    let reversed: Vec<String> = rows.iter().rev().cloned().collect();
    for order in [rows.to_vec(), reversed] {
        let csv = format!("id,kind,num\n{}\n", order.join("\n"));
        let out = run(TIE_YAML, &csv, &["csv", "json"], None);
        assert_eq!(
            out.out("csv"),
            "g,hi\nall,NaN\n",
            "max for arrival order {order:?}"
        );
        assert_eq!(
            out.out("json"),
            "{\"g\":\"all\",\"lo\":0}\n",
            "min for arrival order {order:?}"
        );
    }
}

// ---- resident, spilled, streamed and windowed ------------------------------

/// Groups in the fixture. Each holds seven rows, so the hash table's state
/// outgrows the spill capacities below.
const GROUPS: i64 = 2_400;

/// Values, and so rows, per group.
const VALUES_PER_GROUP: usize = 7;

/// The event time of every row: one tumbling window holds the whole input.
const EVENT_TS: &str = "2026-05-14T10:00:00";

/// How the Aggregate runs over the fixture.
#[derive(Clone, Copy, Debug)]
enum Strategy {
    /// The default (hash) strategy.
    Hash,
    /// `strategy: streaming`, over a Source that declares its order on `g`.
    Streaming,
    /// A tumbling time window over a Source that declares a watermark.
    Window,
}

/// A Source of `g`, `kind`, `num` and `event_ts`; a Transform `vals` turning
/// `kind`/`num` into the mixed column `v` (an integer, a float, a negated
/// float, or null) without emitting `g`, so the Source's declared order and
/// watermark survive it; an Aggregate `grouped` emitting `min(v)` and
/// `max(v)` per `g` to the CSV Sink; and a Transform `nan_free` passing only
/// the groups built without NaN on to the JSON Sink, which rejects NaN.
fn min_max_yaml(strategy: Strategy) -> String {
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
  name: min_max_across_strategies
  memory: {{ limit: "{AMPLE_LIMIT}", backpressure: spill }}
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv{source_extra}
      schema:
        - {{ name: g, type: int }}
        - {{ name: kind, type: string }}
        - {{ name: num, type: string }}
        - {{ name: event_ts, type: date_time }}
  - type: transform
    name: vals
    input: src
    config:
      cxl: |
        emit v = if num == "" then null else if kind == "neg" then -(num.to_float()) else if kind == "f" then num.to_float() else num.to_int()
  - type: aggregate
    name: grouped
    input: vals
    config:
      group_by: [g]{aggregate_extra}
      cxl: |{key_emit}
        emit lo = min(v)
        emit hi = max(v)
  - type: sink
    name: csv
    input: grouped
    config:
      name: csv
      type: csv
      path: out.csv
      include_unmapped: true
  - type: transform
    name: nan_free
    input: grouped
    config:
      cxl: |
        filter g % 4 >= 2
  - type: sink
    name: json
    input: nan_free
    config:
      name: json
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: true
"#
    )
}

/// Group `g`'s values as `(kind, num)` cells, by `g % 4`. Every group mixes
/// integers and floats that tie, and a null:
///
/// - 0: NaN, -NaN, -0.0, 0.0, g + 1 and (g + 1).0 — min -0.0, max NaN;
/// - 1: -NaN, 0, 0.0, -0.0, g and g.0 — min the integer 0, max the NaN;
/// - 2: g, g.0, -0.0, 0.0, 1 and 1.0 — min -0.0, max the float g.0;
/// - 3: 2^53 + 1, 2^53.0, g, g.0, 0 and 0.0 — min the integer 0, max the
///   integer 2^53 + 1.
fn group_values(g: i64) -> [(&'static str, String); VALUES_PER_GROUP] {
    let int = |n: i64| ("i", n.to_string());
    let float = |text: String| ("f", text);
    let null = ("f", String::new());
    match g % 4 {
        0 => [
            float("NaN".into()),
            ("neg", "NaN".into()),
            float("-0.0".into()),
            float("0.0".into()),
            int(g + 1),
            float(format!("{}.0", g + 1)),
            null,
        ],
        1 => [
            ("neg", "NaN".into()),
            int(0),
            float("0.0".into()),
            float("-0.0".into()),
            int(g),
            float(format!("{g}.0")),
            null,
        ],
        2 => [
            int(g),
            float(format!("{g}.0")),
            float("-0.0".into()),
            float("0.0".into()),
            int(1),
            float("1.0".into()),
            null,
        ],
        _ => [
            int(TWO_POW_53 + 1),
            float(format!("{TWO_POW_53}.0")),
            int(g),
            float(format!("{g}.0")),
            int(0),
            float("0.0".into()),
            null,
        ],
    }
}

/// The fixture's rows. Interleaved: round by round, one row of every group
/// per round, so a spill run holds part of every group and the merge meets
/// each group in several runs. Otherwise sorted on `g`, for the streaming
/// strategy. Each group's values are rotated by `g`, so the groups of one
/// kind see their values arrive in different orders.
fn min_max_rows(interleaved: bool) -> String {
    let mut cells: Vec<(i64, usize)> = Vec::new();
    if interleaved {
        for round in 0..VALUES_PER_GROUP {
            cells.extend((0..GROUPS).map(|g| (g, round)));
        }
    } else {
        for g in 0..GROUPS {
            cells.extend((0..VALUES_PER_GROUP).map(|round| (g, round)));
        }
    }
    let mut csv = String::from("g,kind,num,event_ts\n");
    for (g, round) in cells {
        let (kind, num) = &group_values(g)[(round + g as usize) % VALUES_PER_GROUP];
        csv.push_str(&format!("{g},{kind},{num},{EVENT_TS}\n"));
    }
    csv
}

/// Group `g`'s expected `(csv, json)` lines, from the rule: the CSV writer
/// prints `-0.0` as `-0` and an integral float without `.0`; only the groups
/// built without NaN reach JSON.
fn expected_group(g: i64) -> (String, Option<String>) {
    match g % 4 {
        0 => (format!("{g},-0,NaN"), None),
        1 => (format!("{g},0,NaN"), None),
        2 => (
            format!("{g},-0,{g}"),
            Some(format!(r#"{{"g":{g},"lo":-0.0,"hi":{g}.0}}"#)),
        ),
        _ => {
            let big = TWO_POW_53 + 1;
            (
                format!("{g},0,{big}"),
                Some(format!(r#"{{"g":{g},"lo":0,"hi":{big}}}"#)),
            )
        }
    }
}

/// Every group's expected CSV and JSON lines, each sorted, CSV header first.
fn expected_min_max() -> (Vec<String>, Vec<String>) {
    let mut csv = vec!["g,lo,hi".to_string()];
    let mut json = Vec::new();
    for g in 0..GROUPS {
        let (c, j) = expected_group(g);
        csv.push(c);
        json.extend(j);
    }
    csv.sort();
    json.sort();
    (csv, json)
}

/// The Sink's lines, sorted: a hash Aggregate writes its groups in hash-table
/// order, which differs between two runs with ample memory.
fn sorted_lines(output: &str) -> Vec<String> {
    let mut lines: Vec<String> = output.lines().map(str::to_string).collect();
    lines.sort();
    lines
}

fn run_min_max(strategy: Strategy, capacity: Option<u64>) -> Run {
    let interleaved = !matches!(strategy, Strategy::Streaming);
    run(
        &min_max_yaml(strategy),
        &min_max_rows(interleaved),
        &["csv", "json"],
        capacity,
    )
}

/// Assert that `run` wrote every group's expected min and max, as sets of
/// lines, and three groups' rows literally.
fn assert_min_max_output(label: &str, run: &Run, expected: &(Vec<String>, Vec<String>)) {
    let csv = sorted_lines(run.out("csv"));
    let json = sorted_lines(run.out("json"));
    for line in ["0,-0,NaN", "1,0,NaN", "3,0,9007199254740993"] {
        assert!(csv.iter().any(|l| l == line), "{label}: CSV lacks {line}");
    }
    for line in [
        r#"{"g":2,"lo":-0.0,"hi":2.0}"#,
        r#"{"g":3,"lo":0,"hi":9007199254740993}"#,
    ] {
        assert!(json.iter().any(|l| l == line), "{label}: JSON lacks {line}");
    }
    assert_eq!(csv, expected.0, "{label}: CSV min/max differ");
    assert_eq!(json, expected.1, "{label}: JSON min/max differ");
}

/// Run `strategy` at `capacity`, asserting that the Aggregate itself spilled
/// and that the ample run's charged peak lies above the capacity.
fn run_spilled(strategy: Strategy, capacity: u64, ample: &Run) -> Run {
    let spilled = run_min_max(strategy, Some(capacity));
    memory_pressure::assert_spill_engaged(&spilled.report);
    memory_pressure::assert_capacity_below_ample_peak(capacity, &ample.report);
    assert!(
        spilled
            .report
            .per_stage_spill_bytes_written
            .get("grouped")
            .is_some_and(|bytes| *bytes > 0),
        "{strategy:?}: the Aggregate itself must spill at {capacity} bytes: {:?}",
        spilled.report.per_stage_spill_bytes_written
    );
    spilled
}

/// Ledger capacities of the spilling hash-strategy runs: 1 MiB (1,048,576
/// bytes) and 640 KiB (655,360 bytes).
///
/// With ample memory the fixture charges P = 1,248,600 bytes at its peak (the
/// Aggregate's own state peaks at 499,200). It completes at M = 548,595 bytes
/// and is refused at 532,137, where the Source's 16,384-byte document
/// admission falls short. Both capacities lie between M and P, far enough
/// apart that the hash table's spill runs end at different rows. The fixture
/// is new, so there is no earlier limit L to stay under; every run's
/// `memory.limit` is 512M.
const HASH_SPILL_CAPACITIES: [u64; 2] = [1024 * 1024, 640 * 1024];

/// Ledger capacity of the spilling time-window run: 6,400 KiB (6,553,600
/// bytes).
///
/// A windowed Aggregate materializes its input before it windows it, so with
/// ample memory it charges P = 6,682,200 bytes at its peak, of which the
/// materialized input is 6,182,400 and cannot spill. It completes at
/// M = 6,287,281 bytes and is refused at 6,098,662 with E310, where that
/// materialization no longer fits. The capacity lies between the two, with
/// no earlier limit L (new fixture; `memory.limit` 512M).
const WINDOW_SPILL_CAPACITY: u64 = 6_400 * 1024;

/// `min` and `max` are the same values whether the Aggregate holds every
/// group in memory, spills its hash table at two capacities (so each group's
/// partial states are merged from different spill runs), streams over input
/// sorted on the group key, or runs as a tumbling time window, with and
/// without spilling. Every group mixes integers and floats that tie, and
/// half the groups hold NaN of one or both signs.
///
/// A hash Aggregate writes its groups in hash-table order, which differs
/// between two runs with ample memory, so those outputs are compared as sets
/// of lines; the streaming run's output is in key order and is compared byte
/// for byte.
#[test]
fn min_max_are_identical_resident_spilled_and_streamed() {
    let expected = expected_min_max();

    let ample = run_min_max(Strategy::Hash, None);
    assert_min_max_output("hash, ample", &ample, &expected);
    for capacity in HASH_SPILL_CAPACITIES {
        let spilled = run_spilled(Strategy::Hash, capacity, &ample);
        assert_min_max_output(&format!("hash, {capacity} bytes"), &spilled, &expected);
    }

    // The Transform does not emit the group key, so the Source's declared
    // order reaches the Aggregate and the streaming strategy runs as a
    // pipeline; it writes the groups in key order.
    let streamed = run_min_max(Strategy::Streaming, None);
    let mut csv = String::from("g,lo,hi\n");
    let mut json = String::new();
    for g in 0..GROUPS {
        let (c, j) = expected_group(g);
        csv.push_str(&c);
        csv.push('\n');
        if let Some(j) = j {
            json.push_str(&j);
            json.push('\n');
        }
    }
    assert_eq!(streamed.out("csv"), csv, "streaming CSV");
    assert_eq!(streamed.out("json"), json, "streaming JSON");

    // The Transform passes the Source's event time through, so its watermark
    // drives the time window; every row falls in one window.
    let windowed = run_min_max(Strategy::Window, None);
    assert_min_max_output("time window, ample", &windowed, &expected);
    let spilled = run_spilled(Strategy::Window, WINDOW_SPILL_CAPACITY, &windowed);
    assert_min_max_output(
        &format!("time window, {WINDOW_SPILL_CAPACITY} bytes"),
        &spilled,
        &expected,
    );
}
