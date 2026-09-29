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

#![cfg(feature = "test-utils")]

#[path = "common/pipeline_resource_fixtures.rs"]
mod resource_fixtures;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    ExecutionReport, MemoryTestOverrides, PipelineExecutor, PipelineRunParams,
};
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;

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
