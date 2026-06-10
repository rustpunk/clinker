//! End-to-end coverage for the compiled CXL evaluator on the live
//! production transform hot loop.
//!
//! The per-record transform path constructs its `ProgramEvaluator` and
//! runs the compiled program, so any pipeline whose transform body
//! carries non-trivial CXL exercises the compiled evaluator through the
//! full stack — YAML parse → DAG compile → executor dispatch → writer —
//! with no test-local re-wiring. These tests drive a real multi-record
//! source through a transform that combines string/numeric method calls,
//! arrow-syntax closures over a nested array, an `if/then/else`
//! conditional, the null-coalesce operator, `distinct`, and `emit each`
//! fan-out, then assert the exact emitted records. They confirm a genuine
//! production caller exercises the compiled path end-to-end.

use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::sync::{Arc, Mutex};

use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
use clinker_plan::config::{CompileContext, PipelineConfig, parse_config};

#[derive(Clone, Default)]
struct SharedBuffer(Arc<Mutex<Vec<u8>>>);

impl SharedBuffer {
    fn new() -> Self {
        Self::default()
    }
    fn as_string(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl Write for SharedBuffer {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().write(buf)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.lock().unwrap().flush()
    }
}

fn test_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "compiled-e2e".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

fn run_with_payload(yaml: &str, src_path: &str, payload: &[u8]) -> (ExecutionReport, String) {
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let src_name = config.source_configs().next().unwrap().name.clone();
    let readers = HashMap::from([(
        src_name,
        clinker_exec::executor::single_file_reader(
            src_path,
            Box::new(Cursor::new(payload.to_vec())),
        ),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &test_params())
            .expect("pipeline run");
    (report, buf.as_string())
}

fn parse_ndjson(output: &str) -> Vec<serde_json::Value> {
    output
        .lines()
        .filter(|l| !l.is_empty())
        .map(|l| serde_json::from_str(l).expect("each ndjson line parses"))
        .collect()
}

/// A transform body combining string method chains, a numeric
/// conditional, the null-coalesce operator, and an arrow-syntax closure
/// over a nested array, run per record over a real multi-record source.
/// Every emitted field is asserted exactly, so a wrong compiled node for
/// any of these surfaces would change the output.
#[test]
fn compiled_path_methods_conditionals_and_closures_emit_exact_records() {
    let yaml = r#"
pipeline:
  name: compiled_methods
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: json
      options:
        format: ndjson
      path: rows.ndjson
      schema:
        - { name: name, type: string }
        - { name: amount, type: int }
        - { name: note, type: string }
        - { name: prices, type: any }
  - type: transform
    name: derive
    input: rows
    config:
      cxl: |
        emit label = name.upper()
        emit tier = if amount > 100 then "gold" else "standard"
        emit comment = (note ?? "none").trim()
        emit expensive = prices.filter(it => it > 10)
        emit price_count = prices.length()
  - type: output
    name: out
    input: derive
    config:
      name: out
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: false
      exclude: [name, amount, note, prices]
"#;
    // Three records: one gold (amount > 100) with a null note, two
    // standard. The closure filters the nested `prices` array per record.
    let payload = concat!(
        r#"{"name":"acme","amount":250,"note":null,"prices":[5,20,30]}"#,
        "\n",
        r#"{"name":"globex","amount":80,"note":"  spaced  ","prices":[1,2]}"#,
        "\n",
        r#"{"name":"initech","amount":100,"note":"exact","prices":[50]}"#,
        "\n",
    );
    let (_report, output) = run_with_payload(yaml, "rows.ndjson", payload.as_bytes());
    let records = parse_ndjson(&output);
    assert_eq!(records.len(), 3, "one output record per input: {output:?}");

    // Record 0: acme — upper() label, gold tier (250 > 100), null note
    // coalesces to "none" then trims unchanged, filter keeps 20 and 30.
    assert_eq!(records[0]["label"], serde_json::json!("ACME"));
    assert_eq!(records[0]["tier"], serde_json::json!("gold"));
    assert_eq!(records[0]["comment"], serde_json::json!("none"));
    assert_eq!(records[0]["expensive"], serde_json::json!([20, 30]));
    assert_eq!(records[0]["price_count"], serde_json::json!(3));

    // Record 1: globex — standard tier (80 <= 100), note present so
    // coalesce keeps it and trim strips the surrounding spaces, filter
    // keeps nothing (no price > 10).
    assert_eq!(records[1]["label"], serde_json::json!("GLOBEX"));
    assert_eq!(records[1]["tier"], serde_json::json!("standard"));
    assert_eq!(records[1]["comment"], serde_json::json!("spaced"));
    assert_eq!(records[1]["expensive"], serde_json::json!([]));
    assert_eq!(records[1]["price_count"], serde_json::json!(2));

    // Record 2: initech — boundary amount 100 is NOT > 100 → standard.
    assert_eq!(records[2]["label"], serde_json::json!("INITECH"));
    assert_eq!(records[2]["tier"], serde_json::json!("standard"));
    assert_eq!(records[2]["comment"], serde_json::json!("exact"));
    assert_eq!(records[2]["expensive"], serde_json::json!([50]));
    assert_eq!(records[2]["price_count"], serde_json::json!(1));
}

/// `distinct by` running across the live compiled path: cross-record
/// dedup state lives on the per-transform `ProgramEvaluator` the executor
/// constructs, so the compiled evaluator dedups the record stream across
/// records. Exact-duplicate keys collapse to one row; the first
/// occurrence wins.
#[test]
fn compiled_path_distinct_dedups_record_stream() {
    let yaml = r#"
pipeline:
  name: compiled_distinct
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: json
      options:
        format: ndjson
      path: rows.ndjson
      schema:
        - { name: region, type: string }
        - { name: seq, type: int }
  - type: transform
    name: dedup
    input: rows
    config:
      cxl: |
        distinct by region
        emit region_upper = region.upper()
        emit seq = seq
  - type: output
    name: out
    input: dedup
    config:
      name: out
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: false
      exclude: [region]
"#;
    // Five records over three distinct regions; us-east and us-west repeat.
    let payload = concat!(
        r#"{"region":"us-east","seq":1}"#,
        "\n",
        r#"{"region":"us-west","seq":2}"#,
        "\n",
        r#"{"region":"us-east","seq":3}"#,
        "\n",
        r#"{"region":"eu-west","seq":4}"#,
        "\n",
        r#"{"region":"us-west","seq":5}"#,
        "\n",
    );
    let (report, output) = run_with_payload(yaml, "rows.ndjson", payload.as_bytes());
    let records = parse_ndjson(&output);
    assert_eq!(
        records.len(),
        3,
        "distinct collapses to one row per region: {output:?}"
    );
    // First occurrence of each region wins, in input order.
    let regions: Vec<&str> = records
        .iter()
        .map(|r| r["region_upper"].as_str().expect("region_upper"))
        .collect();
    assert_eq!(regions, vec!["US-EAST", "US-WEST", "EU-WEST"]);
    let seqs: Vec<i64> = records
        .iter()
        .map(|r| r["seq"].as_i64().expect("seq"))
        .collect();
    assert_eq!(seqs, vec![1, 2, 4], "first record per region is kept");
    assert_eq!(
        report.counters.distinct_count, 2,
        "two duplicate rows were dropped"
    );
}

/// `emit each` fan-out with a per-element conditional running through the
/// compiled path: one input record expands to one output record per
/// array element, and the body's conditional/method emits are evaluated
/// per element by the compiled evaluator.
#[test]
fn compiled_path_emit_each_fans_out_with_per_element_logic() {
    let yaml = r#"
pipeline:
  name: compiled_emit_each
error_handling:
  strategy: continue
nodes:
  - type: source
    name: rows
    config:
      name: rows
      type: json
      options:
        format: ndjson
      path: rows.ndjson
      schema:
        - { name: items, type: any }
  - type: transform
    name: explode
    input: rows
    config:
      cxl: |
        emit each it in items {
          emit sku = it["sku"].upper()
          emit band = if it["qty"] >= 100 then "bulk" else "retail"
        }
  - type: output
    name: out
    input: explode
    config:
      name: out
      type: json
      options:
        format: ndjson
      path: out.json
      include_unmapped: false
      exclude: [items]
"#;
    let payload = concat!(
        r#"{"items":[{"sku":"a","qty":150},{"sku":"b","qty":40},{"sku":"c","qty":100}]}"#,
        "\n",
    );
    let (_report, output) = run_with_payload(yaml, "rows.ndjson", payload.as_bytes());
    let records = parse_ndjson(&output);
    assert_eq!(records.len(), 3, "one output per array element: {output:?}");

    let skus: Vec<&str> = records
        .iter()
        .map(|r| r["sku"].as_str().expect("sku"))
        .collect();
    assert_eq!(skus, vec!["A", "B", "C"], "upper() applied per element");

    let bands: Vec<&str> = records
        .iter()
        .map(|r| r["band"].as_str().expect("band"))
        .collect();
    // qty >= 100: 150 bulk, 40 retail, 100 bulk (boundary is inclusive).
    assert_eq!(bands, vec!["bulk", "retail", "bulk"]);
}
