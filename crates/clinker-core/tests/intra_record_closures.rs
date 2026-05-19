//! End-to-end integration tests for intra-record CXL operations:
//! arrow-syntax closures, bracket indexing, array/map builtins, and
//! `emit each` fan-out. Each test runs the full pipeline (YAML parse
//! → DAG compile → executor dispatch → JSON output) and asserts a
//! behavior the CXL language surface must satisfy.
//!
//! Fixtures use NDJSON whose `items` field carries a JSON array of
//! objects. The reader produces `Value::Array(Vec<Value::Map>)` so the
//! closure-bearing builtins receive a `Value::Map` per iteration —
//! that's the nested-structure surface the array/map builtins target.

use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::sync::{Arc, Mutex};

use clinker_core::config::{CompileContext, PipelineConfig, parse_config};
use clinker_core::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};

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
        execution_id: "intra-record".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

async fn run_with_payload(yaml: &str, src_path: &str, payload: &[u8]) -> (ExecutionReport, String) {
    let config = parse_config(yaml).expect("parse pipeline yaml");
    let plan = PipelineConfig::compile(&config, &CompileContext::default()).expect("compile");
    let src_name = config.source_configs().next().unwrap().name.clone();
    let readers = HashMap::from([(
        src_name,
        clinker_core::executor::single_file_reader(
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
            .await
            .expect("pipeline run");
    (report, buf.as_string())
}

/// Closure-bearing array builtins (`.filter`, `.map`) running against
/// a nested `items` array — each element is a `Value::Map` and the
/// closure body resolves bracket-index access on the binding.
#[tokio::test(flavor = "multi_thread")]
async fn ndjson_filter_and_map_closures_resolve_against_nested_arrays() {
    let yaml = r#"
pipeline:
  name: filter_map
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
    name: derive
    input: rows
    config:
      cxl: |
        emit kept = items.filter(it => it["price"] > 5)
        emit prices = items.map(it => it["price"])
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
      exclude: [items]
"#;
    let payload =
        br#"{"items":[{"sku":"a","price":10},{"sku":"b","price":20},{"sku":"c","price":5}]}
"#;
    let (_report, output) = run_with_payload(yaml, "rows.ndjson", payload).await;
    let line = output.lines().next().expect("at least one record");
    let parsed: serde_json::Value = serde_json::from_str(line).expect("ndjson output parses");
    let kept = parsed.get("kept").and_then(|v| v.as_array()).expect("kept");
    assert_eq!(kept.len(), 2, "filter result (price > 5): {parsed}");
    let prices: Vec<i64> = parsed
        .get("prices")
        .and_then(|v| v.as_array())
        .expect("prices")
        .iter()
        .map(|v| v.as_i64().expect("price is int"))
        .collect();
    assert_eq!(prices, vec![10, 20, 5], ".map ran over every element");
}

/// `emit each` fans one input record into one output record per array
/// element. Each emitted record carries the body's emits (`sku`,
/// `price`); upstream fields are suppressed via `include_unmapped:
/// false` plus `exclude: [items]`.
#[tokio::test(flavor = "multi_thread")]
async fn ndjson_emit_each_fans_one_record_into_array_length_records() {
    let yaml = r#"
pipeline:
  name: emit_each_fanout
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
          emit sku = it["sku"]
          emit price = it["price"]
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
    let payload =
        br#"{"items":[{"sku":"a","price":10},{"sku":"b","price":20},{"sku":"c","price":5}]}
"#;
    let (_report, output) = run_with_payload(yaml, "rows.ndjson", payload).await;
    let lines: Vec<&str> = output.lines().filter(|l| !l.is_empty()).collect();
    assert_eq!(lines.len(), 3, "one record per array element: {output:?}");
    let records: Vec<serde_json::Value> = lines
        .iter()
        .map(|l| serde_json::from_str(l).expect("each ndjson line parses"))
        .collect();
    let skus: Vec<&str> = records
        .iter()
        .map(|r| r.get("sku").and_then(|v| v.as_str()).expect("sku"))
        .collect();
    assert_eq!(skus, vec!["a", "b", "c"]);
    let prices: Vec<i64> = records
        .iter()
        .map(|r| r.get("price").and_then(|v| v.as_i64()).expect("price"))
        .collect();
    assert_eq!(prices, vec![10, 20, 5]);
}

/// `max_expansion` (set on `TransformBody`) caps the cumulative number
/// of records `emit each` can produce from a single original input. On
/// exceed, the originating record routes to the DLQ with category
/// `expansion_limit_exceeded`.
#[tokio::test(flavor = "multi_thread")]
async fn emit_each_overflow_routes_to_expansion_dlq() {
    let yaml = r#"
pipeline:
  name: emit_each_overflow
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
      max_expansion: 3
      cxl: |
        emit each it in items {
          emit val = it
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
"#;
    let payload = br#"{"items":[1,2,3,4,5,6,7,8,9,10]}
"#;
    let (report, _output) = run_with_payload(yaml, "rows.ndjson", payload).await;
    let saw_expansion = report
        .dlq_entries
        .iter()
        .any(|e| e.category.as_str() == "expansion_limit_exceeded");
    assert!(
        saw_expansion,
        "DLQ entries should carry expansion_limit_exceeded: {:#?}",
        report.dlq_entries
    );
}

/// Map builtins running inside a closure: `.keys()`, `.values()`,
/// `.set()`, `.remove_field()`. The closure binding `it` is each
/// element of the items array (a `Value::Map`); the builtins return
/// new `Value::Map`/`Value::Array` per element, surfaced through the
/// outer `.map` that wraps them.
#[tokio::test(flavor = "multi_thread")]
async fn ndjson_map_builtins_run_inside_array_closures() {
    let yaml = r#"
pipeline:
  name: map_builtins
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
    name: derive
    input: rows
    config:
      cxl: |
        emit key_sets = items.map(it => it.keys())
        emit value_sets = items.map(it => it.values())
        emit enriched = items.map(it => it.set("region", "us-east"))
        emit slim = items.map(it => it.remove_field("internal_id"))
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
      exclude: [items]
"#;
    let payload = br#"{"items":[{"sku":"a","price":10,"internal_id":"ix-1"},{"sku":"b","price":20,"internal_id":"ix-2"}]}
"#;
    let (_report, output) = run_with_payload(yaml, "rows.ndjson", payload).await;
    let line = output.lines().next().expect("at least one record");
    let parsed: serde_json::Value = serde_json::from_str(line).expect("ndjson parses");
    let key_sets = parsed
        .get("key_sets")
        .and_then(|v| v.as_array())
        .expect("key_sets array");
    assert_eq!(key_sets.len(), 2, ".map produced one entry per element");
    let first_keys: Vec<&str> = key_sets[0]
        .as_array()
        .expect("each key set is array")
        .iter()
        .map(|v| v.as_str().expect("key is string"))
        .collect();
    assert!(first_keys.contains(&"sku"), ".keys() lifted sku: {parsed}");
    assert!(first_keys.contains(&"price"), ".keys() lifted price");
    assert!(
        first_keys.contains(&"internal_id"),
        ".keys() lifted internal_id"
    );
    let enriched = parsed
        .get("enriched")
        .and_then(|v| v.as_array())
        .expect("enriched array");
    let first_enriched = enriched[0].as_object().expect("enriched[0] is object");
    assert_eq!(
        first_enriched.get("region").and_then(|v| v.as_str()),
        Some("us-east"),
        ".set added region: {parsed}"
    );
    let slim = parsed
        .get("slim")
        .and_then(|v| v.as_array())
        .expect("slim array");
    let first_slim = slim[0].as_object().expect("slim[0] is object");
    assert!(
        !first_slim.contains_key("internal_id"),
        ".remove_field dropped internal_id: {parsed}"
    );
    assert!(
        first_slim.contains_key("sku"),
        ".remove_field preserved sku"
    );
}

/// Array-only builtins plus bracket-index access on an array
/// receiver: `items[0]`, `.find`, `.any`, `.remove(idx)`.
#[tokio::test(flavor = "multi_thread")]
async fn ndjson_array_builtins_round_trip() {
    let yaml = r#"
pipeline:
  name: array_builtins
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
    name: derive
    input: rows
    config:
      cxl: |
        emit first = items[0]
        emit best = items.find(it => it["price"] > 15)
        emit any_cheap = items.any(it => it["price"] < 10)
        emit dropped_second = items.remove(1)
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
      exclude: [items]
"#;
    let payload =
        br#"{"items":[{"sku":"a","price":10},{"sku":"b","price":20},{"sku":"c","price":5}]}
"#;
    let (_report, output) = run_with_payload(yaml, "rows.ndjson", payload).await;
    let parsed: serde_json::Value =
        serde_json::from_str(output.lines().next().expect("a record")).expect("parses");
    let first = parsed
        .get("first")
        .and_then(|v| v.as_object())
        .expect("first is object");
    assert_eq!(
        first.get("sku").and_then(|v| v.as_str()),
        Some("a"),
        "bracket index on array returns element 0: {parsed}"
    );
    let best = parsed
        .get("best")
        .and_then(|v| v.as_object())
        .expect("best is object");
    assert_eq!(
        best.get("sku").and_then(|v| v.as_str()),
        Some("b"),
        ".find returned first element matching predicate"
    );
    assert_eq!(
        parsed.get("any_cheap").and_then(|v| v.as_bool()),
        Some(true),
        ".any returns bool"
    );
    let dropped = parsed
        .get("dropped_second")
        .and_then(|v| v.as_array())
        .expect("dropped is array");
    assert_eq!(dropped.len(), 2, ".remove(1) shrinks by one");
    assert_eq!(
        dropped[0].get("sku").and_then(|v| v.as_str()),
        Some("a"),
        ".remove(1) preserves index 0"
    );
    assert_eq!(
        dropped[1].get("sku").and_then(|v| v.as_str()),
        Some("c"),
        ".remove(1) shifts index 2 down to 1"
    );
}
