//! Multi-output routing tests.
//!
//! Tests in this module exercise the multi-writer registry, route condition
//! evaluation, per-output channels, and DLQ stage/route extensions.

use super::*;
use clinker_bench_support::io::SharedBuffer;
use std::collections::HashMap;

/// Build a multi-output test fixture with the given YAML config.
///
/// Returns the parsed `PipelineConfig` and a `HashMap<String, SharedBuffer>`
/// with one entry per output defined in the config. The buffer names match
/// the output `name` fields in the YAML.
fn multi_output_fixture(
    yaml: &str,
) -> (crate::config::PipelineConfig, HashMap<String, SharedBuffer>) {
    let config = crate::config::parse_config(yaml).unwrap();
    let buffers: HashMap<String, SharedBuffer> = config
        .output_configs()
        .map(|o| (o.name.clone(), SharedBuffer::new()))
        .collect();
    (config, buffers)
}

/// Build default `PipelineRunParams` for tests.
fn test_params(config: &crate::config::PipelineConfig) -> PipelineRunParams {
    let pipeline_vars = config
        .pipeline
        .vars
        .as_ref()
        .map(|v| crate::config::convert_pipeline_vars(v))
        .unwrap_or_default();
    PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars,
        shutdown_token: None,
    }
}

/// Helper: compile a route config against a set of field names.
fn compile_test_route(route_yaml: &str, fields: &[&str]) -> CompiledRoute {
    let route_config: crate::config::RouteConfig = crate::yaml::from_str(route_yaml).unwrap();
    let emitted_fields: Vec<String> = fields.iter().map(|s| s.to_string()).collect();
    PipelineExecutor::compile_route(&route_config, &emitted_fields, &Default::default()).unwrap()
}

/// Helper: build an EvalContext for route evaluation tests.
fn test_eval_context() -> cxl::eval::EvalContext<'static> {
    use std::sync::{Arc, OnceLock};
    static STABLE: OnceLock<cxl::eval::StableEvalContext> = OnceLock::new();
    static SOURCE_FILE: OnceLock<Arc<str>> = OnceLock::new();
    cxl::eval::EvalContext::test_with_file(
        STABLE.get_or_init(cxl::eval::StableEvalContext::test_default),
        SOURCE_FILE.get_or_init(|| Arc::from("test.csv")),
        1,
    )
}

/// Helper: assemble a Record from an `(emitted, metadata)` field pair
/// for the post-rip `CompiledRoute::evaluate(&Record, ...)` shape.
/// Schema columns take insertion order of `emitted`; metadata writes
/// route through `Record::set_meta` so the resolver's `$meta.*`
/// prefix-strip sees them.
fn test_record(
    emitted: &indexmap::IndexMap<String, Value>,
    metadata: &indexmap::IndexMap<String, Value>,
) -> clinker_record::Record {
    use std::sync::Arc;
    let columns: Vec<Box<str>> = emitted.keys().map(|k| k.as_str().into()).collect();
    let schema = Arc::new(clinker_record::Schema::new(columns));
    let values: Vec<Value> = emitted.values().cloned().collect();
    let mut record = clinker_record::Record::new(schema, values);
    for (key, value) in metadata {
        let _ = record.set_meta(key, value.clone());
    }
    record
}

// --- Route evaluation unit tests ---

#[test]
fn test_route_eval_exclusive_first_match() {
    let mut route = compile_test_route(
        r#"
mode: exclusive
branches:
  - name: high
    condition: "amount > 10000"
  - name: medium
    condition: "amount > 1000"
default: low
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(50000))]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["high"]);
}

#[test]
fn test_route_eval_exclusive_second_match() {
    let mut route = compile_test_route(
        r#"
mode: exclusive
branches:
  - name: high
    condition: "amount > 10000"
  - name: medium
    condition: "amount > 1000"
default: low
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(5000))]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["medium"]);
}

#[test]
fn test_route_eval_exclusive_default() {
    let mut route = compile_test_route(
        r#"
mode: exclusive
branches:
  - name: high
    condition: "amount > 10000"
  - name: medium
    condition: "amount > 1000"
default: low
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(500))]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["low"]);
}

#[test]
fn test_route_eval_inclusive_all_match() {
    let mut route = compile_test_route(
        r#"
mode: inclusive
branches:
  - name: audit
    condition: "amount > 1000"
  - name: report
    condition: "amount > 500"
default: standard
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(5000))]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["audit", "report"]);
}

#[test]
fn test_route_eval_inclusive_some_match() {
    let mut route = compile_test_route(
        r#"
mode: inclusive
branches:
  - name: audit
    condition: "amount > 10000"
  - name: report
    condition: "amount > 500"
  - name: archive
    condition: "amount > 50000"
default: standard
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(5000))]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["report"]);
}

#[test]
fn test_route_eval_inclusive_none_match() {
    let mut route = compile_test_route(
        r#"
mode: inclusive
branches:
  - name: audit
    condition: "amount > 10000"
  - name: report
    condition: "amount > 5000"
default: standard
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Integer(100))]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["standard"]);
}

#[test]
fn test_route_eval_complex_and_or() {
    let mut route = compile_test_route(
        r#"
branches:
  - name: intl_high
    condition: "amount > 10000 and country != 'US'"
default: standard
"#,
        &["amount", "country"],
    );

    let ctx = test_eval_context();

    // Matches: high amount AND non-US
    let emitted = indexmap::IndexMap::from([
        ("amount".to_string(), Value::Integer(50000)),
        ("country".to_string(), Value::String("UK".into())),
    ]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["intl_high"]);

    // Does not match: high amount but US
    let emitted = indexmap::IndexMap::from([
        ("amount".to_string(), Value::Integer(50000)),
        ("country".to_string(), Value::String("US".into())),
    ]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["standard"]);
}

#[test]
fn test_route_eval_null_field_in_condition() {
    let mut route = compile_test_route(
        r#"
branches:
  - name: high
    condition: "amount > 10000"
default: standard
"#,
        &["amount"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("amount".to_string(), Value::Null)]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    // Null > 10000 is not true → default
    assert_eq!(targets, vec!["standard"]);
}

#[test]
fn test_route_eval_emitted_field_in_condition() {
    // Route condition references a field that was emitted by a transform (not original input)
    let mut route = compile_test_route(
        r#"
branches:
  - name: high
    condition: "computed_score > 90"
default: standard
"#,
        &["computed_score"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("computed_score".to_string(), Value::Integer(95))]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["high"]);
}

#[test]
fn test_route_eval_let_binding_in_condition() {
    // Route conditions don't have let bindings (they're filter-only),
    // but they can reference fields that were created by let+emit in transforms.
    // This test verifies that emitted fields are accessible.
    let mut route = compile_test_route(
        r#"
branches:
  - name: vip
    condition: "tier == 'gold'"
default: regular
"#,
        &["tier"],
    );

    let ctx = test_eval_context();
    let emitted = indexmap::IndexMap::from([("tier".to_string(), Value::String("gold".into()))]);
    let targets = route
        .evaluate(&test_record(&emitted, &indexmap::IndexMap::new()), &ctx)
        .unwrap();
    assert_eq!(targets, vec!["vip"]);
}

// --- Multi-output channel integration tests ---

/// Helper: run a multi-output pipeline and return per-output CSV strings.
fn run_multi_output(
    yaml: &str,
    csv_input: &str,
) -> Result<(PipelineCounters, Vec<DlqEntry>, HashMap<String, String>), PipelineError> {
    let (config, buffers) = multi_output_fixture(yaml);
    let params = test_params(&config);

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: crate::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv_input.as_bytes().to_vec())),
        ),
    )]);
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = buffers
        .iter()
        .map(|(name, buf)| {
            (
                name.clone(),
                Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();

    let report = PipelineExecutor::run_with_readers_writers(
        &config,
        &primary,
        readers,
        writers.into(),
        &params,
    )?;

    let outputs: HashMap<String, String> = buffers
        .iter()
        .map(|(name, buf)| (name.clone(), buf.as_string()))
        .collect();

    Ok((report.counters, report.dlq_entries, outputs))
}

#[test]
fn test_multi_output_two_writers() {
    let yaml = r#"
pipeline:
  name: test_two_outputs
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 100
    default: low
- type: output
  name: high
  input: classify
  config:
    name: high
    path: high.csv
    type: csv
    include_unmapped: true
- type: output
  name: low
  input: classify
  config:
    name: low
    path: low.csv
    type: csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,200\n2,50\n3,300\n4,10\n";
    let (counters, _, outputs) = run_multi_output(yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 4);
    let high = &outputs["high"];
    let low = &outputs["low"];

    // High: rows with amount > 100 (ids 1, 3)
    assert!(high.contains("1,200"), "high should contain id=1: {high}");
    assert!(high.contains("3,300"), "high should contain id=3: {high}");
    assert!(
        !high.contains("2,50"),
        "high should not contain id=2: {high}"
    );

    // Low: rows with amount <= 100 (ids 2, 4)
    assert!(low.contains("2,50"), "low should contain id=2: {low}");
    assert!(low.contains("4,10"), "low should contain id=4: {low}");
    assert!(!low.contains("1,200"), "low should not contain id=1: {low}");
}

#[test]
fn test_multi_output_three_writers() {
    let yaml = r#"
pipeline:
  name: test_three_outputs
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 1000
      medium: amount_val > 100
    default: low
- type: output
  name: high
  input: classify
  config:
    name: high
    path: high.csv
    type: csv
    include_unmapped: true
- type: output
  name: medium
  input: classify
  config:
    name: medium
    path: medium.csv
    type: csv
    include_unmapped: true
- type: output
  name: low
  input: classify
  config:
    name: low
    path: low.csv
    type: csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,5000\n2,500\n3,50\n";
    let (counters, _, outputs) = run_multi_output(yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 3);
    assert!(outputs["high"].contains("1,5000"));
    assert!(outputs["medium"].contains("2,500"));
    assert!(outputs["low"].contains("3,50"));
}

#[test]
fn test_multi_output_record_counts() {
    let yaml = r#"
pipeline:
  name: test_record_counts
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      big: amount_val > 50
    default: small
- type: output
  name: big
  input: classify
  config:
    name: big
    path: big.csv
    type: csv
    include_unmapped: true
- type: output
  name: small
  input: classify
  config:
    name: small
    path: small.csv
    type: csv
    include_unmapped: true
"#;

    let csv = "id,amount\n1,100\n2,10\n3,200\n4,20\n5,300\n";
    let (counters, _, outputs) = run_multi_output(yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 5);
    assert_eq!(counters.total_count, 5);

    // Count data rows (subtract header line)
    let big_rows = outputs["big"].lines().count() - 1;
    let small_rows = outputs["small"].lines().count() - 1;
    assert_eq!(big_rows + small_rows, 5);
    assert_eq!(big_rows, 3); // 100, 200, 300
    assert_eq!(small_rows, 2); // 10, 20
}

#[test]
fn test_multi_output_order_preserved() {
    let yaml = r#"
pipeline:
  name: test_order
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      big: amount_val > 50
    default: small
- type: output
  name: big
  input: classify
  config:
    name: big
    path: big.csv
    type: csv
    include_unmapped: true
- type: output
  name: small
  input: classify
  config:
    name: small
    path: small.csv
    type: csv
    include_unmapped: true
"#;

    // All go to "big" — order must match input order
    let csv = "id,amount\n1,100\n2,200\n3,300\n4,400\n5,500\n";
    let (_, _, outputs) = run_multi_output(yaml, csv).unwrap();

    let big_lines: Vec<&str> = outputs["big"].lines().skip(1).collect();
    assert_eq!(big_lines.len(), 5);
    // Verify sequential IDs appear in order (column position depends on output config)
    let ids: Vec<&str> = big_lines
        .iter()
        .map(|line| {
            // Find the id value — it's the column matching the "id" header position
            let header_cols: Vec<&str> =
                outputs["big"].lines().next().unwrap().split(',').collect();
            let id_idx = header_cols.iter().position(|&c| c == "id").unwrap();
            line.split(',').nth(id_idx).unwrap()
        })
        .collect();
    assert_eq!(
        ids,
        vec!["1", "2", "3", "4", "5"],
        "records must preserve input order"
    );
}

#[test]
fn test_multi_output_writer_error_propagated() {
    /// A writer that fails after N bytes.
    struct FailingWriter {
        remaining: usize,
    }
    impl std::io::Write for FailingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            if self.remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "simulated write failure",
                ));
            }
            let n = buf.len().min(self.remaining);
            self.remaining -= n;
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            if self.remaining == 0 {
                Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "simulated flush failure",
                ))
            } else {
                Ok(())
            }
        }
    }

    let yaml = r#"
pipeline:
  name: test_writer_error
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      good: amount_val > 50
    default: bad
- type: output
  name: good
  input: classify
  config:
    name: good
    path: good.csv
    type: csv
    include_unmapped: true
- type: output
  name: bad
  input: classify
  config:
    name: bad
    path: bad.csv
    type: csv
    include_unmapped: true
"#;

    let config = crate::config::parse_config(yaml).unwrap();
    let params = test_params(&config);

    let csv = "id,amount\n1,100\n2,10\n3,200\n";
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let good_buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "good".to_string(),
            Box::new(good_buf.clone()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "bad".to_string(),
            // Fail after writing 10 bytes (enough for header but not data)
            Box::new(FailingWriter { remaining: 10 }) as Box<dyn std::io::Write + Send>,
        ),
    ]);

    let result = PipelineExecutor::run_with_readers_writers(
        &config,
        "src",
        readers,
        writers.into(),
        &params,
    );
    assert!(result.is_err(), "should propagate writer error");
}

#[test]
fn test_multi_output_inclusive_duplicate() {
    let yaml = r#"
pipeline:
  name: test_inclusive
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      audit: amount_val > 100
      report: amount_val > 50
    default: standard
    mode: inclusive
- type: output
  name: audit
  input: classify
  config:
    name: audit
    path: audit.csv
    type: csv
    include_unmapped: true
- type: output
  name: report
  input: classify
  config:
    name: report
    path: report.csv
    type: csv
    include_unmapped: true
- type: output
  name: standard
  input: classify
  config:
    name: standard
    path: standard.csv
    type: csv
    include_unmapped: true
"#;

    // amount=500 matches both audit (>100) and report (>50)
    let csv = "id,amount\n1,500\n2,30\n";
    let (counters, _, outputs) = run_multi_output(yaml, csv).unwrap();

    // Dual counters:
    //   ok_count = 2 (both input records reached at least one Output)
    //   records_written = 3 (record 1 fans out to {audit, report},
    //                        record 2 routes to {standard})
    assert_eq!(
        counters.ok_count, 2,
        "ok_count should be 2 distinct successful inputs"
    );
    assert_eq!(
        counters.records_written, 3,
        "records_written should be 3 — record 1 written to 2 sinks + record 2 to 1 sink"
    );

    // Record 1 should appear in both audit and report
    assert!(outputs["audit"].contains("1,500"));
    assert!(outputs["report"].contains("1,500"));
    // Record 2 matches neither → default
    assert!(outputs["standard"].contains("2,30"));
}

#[test]
fn test_single_output_no_channel_overhead() {
    // No route config → single-output direct write path (no channels spawned)
    let yaml = r#"
pipeline:
  name: test_single
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }

- type: transform
  name: passthrough
  input: src
  config:
    cxl: 'emit val = id

      '
- type: output
  name: out
  input: passthrough
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;

    let csv = "id\n1\n2\n3\n";
    let (counters, _, outputs) = run_multi_output(yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 3);
    assert!(outputs["out"].contains("1\n"));
    assert!(outputs["out"].contains("2\n"));
    assert!(outputs["out"].contains("3\n"));
}

#[test]
fn test_multi_output_empty_route() {
    let yaml = r#"
pipeline:
  name: test_empty_route
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      special: amount_val > 99999
    default: normal
- type: output
  name: special
  input: classify
  config:
    name: special
    path: special.csv
    type: csv
    include_unmapped: true
- type: output
  name: normal
  input: classify
  config:
    name: normal
    path: normal.csv
    type: csv
    include_unmapped: true
"#;

    // No records match "special" (all < 99999)
    let csv = "id,amount\n1,100\n2,200\n";
    let (_, _, outputs) = run_multi_output(yaml, csv).unwrap();

    // Special output should have just a header (or be empty)
    let special_lines: Vec<&str> = outputs["special"]
        .lines()
        .filter(|l| !l.is_empty())
        .collect();
    assert!(
        special_lines.len() <= 1,
        "special should be empty or header-only, got: {:?}",
        special_lines
    );

    // Normal should have both records
    assert!(outputs["normal"].contains("1,100"));
    assert!(outputs["normal"].contains("2,200"));
}

#[test]
fn test_multi_output_writer_panic_propagated() {
    /// A writer that panics on the first write call (not flush).
    /// BufWriter buffers everything, so the inner write is only called when BufWriter flushes.
    /// This guarantees a single panic (no double-panic on drop).
    struct PanickingWriter {
        panicked: std::sync::atomic::AtomicBool,
    }
    impl std::io::Write for PanickingWriter {
        fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
            if !self
                .panicked
                .swap(true, std::sync::atomic::Ordering::Relaxed)
            {
                panic!("simulated writer panic");
            }
            // During unwind/drop, don't panic again
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "already panicked",
            ))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let yaml = r#"
pipeline:
  name: test_panic
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      good: amount_val > 50
    default: bad
- type: output
  name: good
  input: classify
  config:
    name: good
    path: good.csv
    type: csv
    include_unmapped: true
- type: output
  name: bad
  input: classify
  config:
    name: bad
    path: bad.csv
    type: csv
    include_unmapped: true
"#;

    let config = crate::config::parse_config(yaml).unwrap();
    let params = test_params(&config);

    let csv = "id,amount\n1,100\n2,10\n";
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "good".to_string(),
            Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "bad".to_string(),
            Box::new(PanickingWriter {
                panicked: std::sync::atomic::AtomicBool::new(false),
            }) as Box<dyn std::io::Write + Send>,
        ),
    ]);

    // Panic should be caught and propagated, not hang or abort
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        PipelineExecutor::run_with_readers_writers(&config, "src", readers, writers.into(), &params)
    }));

    // The panic is re-raised via resume_unwind, so catch_unwind catches it
    assert!(result.is_err(), "writer panic should propagate");
}

#[test]
fn test_multi_output_cancel_drains_and_flushes() {
    // Cancel scenario: pipeline errors mid-stream but already-dispatched
    // records should be flushed. We test this by having a FailFast error
    // strategy with a record that triggers an eval error, but records before
    // the error should still have been dispatched to writer threads.
    //
    // Since FailFast returns immediately, the writer threads get their
    // channels dropped → they drain remaining items and flush.
    let yaml = r#"
pipeline:
  name: test_cancel
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      big: amount_val > 50
    default: small
- type: output
  name: big
  input: classify
  config:
    name: big
    path: big.csv
    type: csv
    include_unmapped: true
- type: output
  name: small
  input: classify
  config:
    name: small
    path: small.csv
    type: csv
    include_unmapped: true
"#;

    // "bad" will fail to_int → DLQ, but other records should still be written
    let csv = "id,amount\n1,100\n2,bad\n3,200\n";
    let (counters, dlq, outputs) = run_multi_output(yaml, csv).unwrap();

    // Record 2 should fail and go to DLQ
    assert_eq!(counters.dlq_count, 1);
    assert_eq!(dlq.len(), 1);

    // Records 1 and 3 should be in "big"
    assert!(outputs["big"].contains("1,100"));
    assert!(outputs["big"].contains("3,200"));
}

#[test]
fn test_multi_output_send_error_disconnected() {
    /// A writer that errors after writing N records (simulating a writer dying mid-stream).
    struct DyingWriter {
        inner: SharedBuffer,
        writes_remaining: std::sync::atomic::AtomicU32,
    }
    impl std::io::Write for DyingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            let remaining = self
                .writes_remaining
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            if remaining == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "writer died",
                ));
            }
            self.inner.write(buf)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            self.inner.flush()
        }
    }

    let yaml = r#"
pipeline:
  name: test_disconnected
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      a: amount_val > 50
    default: b
- type: output
  name: a
  input: classify
  config:
    name: a
    path: a.csv
    type: csv
    include_unmapped: true
- type: output
  name: b
  input: classify
  config:
    name: b
    path: b.csv
    type: csv
    include_unmapped: true
"#;

    let config = crate::config::parse_config(yaml).unwrap();
    let params = test_params(&config);

    // Enough records that "b" gets traffic and will error
    let csv = "id,amount\n1,100\n2,10\n3,200\n4,20\n";
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "a".to_string(),
            Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
        ),
        (
            "b".to_string(),
            Box::new(DyingWriter {
                inner: SharedBuffer::new(),
                writes_remaining: std::sync::atomic::AtomicU32::new(3), // header + ~2 writes
            }) as Box<dyn std::io::Write + Send>,
        ),
    ]);

    // Should not deadlock — producer handles SendError::Disconnected
    let result = PipelineExecutor::run_with_readers_writers(
        &config,
        "src",
        readers,
        writers.into(),
        &params,
    );
    // Either error (from the dying writer) or success if the writer survived long enough
    // The key assertion: no deadlock, no hang — the test completes
    let _ = result;
}

#[test]
fn test_multi_output_multiple_errors_collected() {
    /// A writer that always fails on flush.
    struct FlushFailWriter;
    impl std::io::Write for FlushFailWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            Ok(buf.len()) // Accept writes
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "flush failed",
            ))
        }
    }

    let yaml = r#"
pipeline:
  name: test_multiple_errors
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      a: amount_val > 50
    default: b
- type: output
  name: a
  input: classify
  config:
    name: a
    path: a.csv
    type: csv
    include_unmapped: true
- type: output
  name: b
  input: classify
  config:
    name: b
    path: b.csv
    type: csv
    include_unmapped: true
"#;

    let config = crate::config::parse_config(yaml).unwrap();
    let params = test_params(&config);

    let csv = "id,amount\n1,100\n2,10\n";
    let readers: crate::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    // Both writers will fail on flush → PipelineError::Multiple
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([
        (
            "a".to_string(),
            Box::new(FlushFailWriter) as Box<dyn std::io::Write + Send>,
        ),
        (
            "b".to_string(),
            Box::new(FlushFailWriter) as Box<dyn std::io::Write + Send>,
        ),
    ]);

    let result = PipelineExecutor::run_with_readers_writers(
        &config,
        "src",
        readers,
        writers.into(),
        &params,
    );
    // The DAG-walk Output arm collects every Output's write/flush
    // failure across the topo walk before aggregating into
    // PipelineError::Multiple. With both writers failing on flush, the
    // result MUST be Multiple with both errors — strictly tighter than
    // the old matcher that accepted `Multiple | Io | Format(Io)` to
    // tolerate a first-fail short-circuit.
    match result {
        Err(PipelineError::Multiple(errors)) => {
            assert_eq!(errors.len(), 2, "should collect both flush errors");
        }
        other => {
            panic!("expected PipelineError::Multiple with 2 collected flush errors, got: {other:?}")
        }
    }
}

// --- DLQ stage/route extension tests ---

#[test]
fn test_dlq_stage_source() {
    // Unit test: verify DlqEntry::stage_source() produces "source"
    // and a DlqEntry with stage "source" has route: None.
    let schema = std::sync::Arc::new(clinker_record::Schema::new(vec!["id".into()]));
    let record = clinker_record::Record::new(schema, vec![Value::String("1".into())]);
    let entry = DlqEntry {
        source_row: 1,
        category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
        error_message: "source read error".to_string(),
        original_record: record,
        stage: Some(DlqEntry::stage_source()),
        route: None,
        trigger: true,
    };
    assert_eq!(entry.stage, Some("source".to_string()));
    assert_eq!(entry.route, None);
}

#[test]
fn test_dlq_stage_transform() {
    // Run a pipeline with a transform eval error (non-integer value + to_int),
    // verify DLQ entry has stage "transform:classify".
    let yaml = r#"
pipeline:
  name: test_dlq_transform
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 100
    default: low
- type: output
  name: high
  input: classify
  config:
    name: high
    path: high.csv
    type: csv
    include_unmapped: true
- type: output
  name: low
  input: classify
  config:
    name: low
    path: low.csv
    type: csv
    include_unmapped: true
"#;

    // "bad_value" cannot be converted to int → transform eval error
    let csv = "id,amount\n1,bad_value\n";
    let (counters, dlq, _) = run_multi_output(yaml, csv).unwrap();

    assert_eq!(counters.dlq_count, 1);
    assert_eq!(dlq.len(), 1);
    assert_eq!(
        dlq[0].stage,
        Some("transform:classify_emit".to_string()),
        "stage should be transform:classify_emit"
    );
    assert_eq!(
        dlq[0].route, None,
        "pre-routing error should have null route"
    );
}

#[test]
fn test_dlq_stage_route_eval() {
    // Route condition that causes division by zero → DLQ with stage "route_eval".
    let yaml = r#"
pipeline:
  name: test_dlq_route_eval
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }
      - { name: zero, type: string }

- type: transform
  name: calc_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      emit divisor = zero.to_int()

      '
- type: route
  name: calc
  input: calc_emit
  config:
    conditions:
      special: amount_val / divisor > 0
    default: normal
- type: output
  name: special
  input: calc
  config:
    name: special
    path: special.csv
    type: csv
    include_unmapped: true
- type: output
  name: normal
  input: calc
  config:
    name: normal
    path: normal.csv
    type: csv
    include_unmapped: true
"#;

    let csv = "id,amount,zero\n1,100,0\n";
    let (counters, dlq, _) = run_multi_output(yaml, csv).unwrap();

    assert_eq!(
        counters.dlq_count, 1,
        "division by zero in route condition should produce DLQ entry"
    );
    assert_eq!(dlq.len(), 1);
    assert_eq!(
        dlq[0].stage,
        Some("route_eval".to_string()),
        "stage should be route_eval"
    );
    assert_eq!(dlq[0].route, None, "route eval error has null route");
}

#[test]
fn test_dlq_stage_output() {
    // Unit test: verify DlqEntry::stage_output() produces "output:{name}"
    // and that a DlqEntry can carry both stage and route.
    let schema = std::sync::Arc::new(clinker_record::Schema::new(vec!["id".into()]));
    let record = clinker_record::Record::new(schema, vec![Value::String("1".into())]);
    let entry = DlqEntry {
        source_row: 1,
        category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
        error_message: "write error".to_string(),
        original_record: record,
        stage: Some(DlqEntry::stage_output("results")),
        route: Some("high_value".to_string()),
        trigger: true,
    };
    assert_eq!(entry.stage, Some("output:results".to_string()));
    assert_eq!(entry.route, Some("high_value".to_string()));
}

#[test]
fn test_dlq_pre_routing_null_route() {
    // Pre-routing errors (transform eval) always have route: None.
    let yaml = r#"
pipeline:
  name: test_dlq_pre_routing
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: classify_emit
  input: src
  config:
    cxl: 'emit amount_val = amount.to_int()

      '
- type: route
  name: classify
  input: classify_emit
  config:
    conditions:
      high: amount_val > 100
    default: low
- type: output
  name: high
  input: classify
  config:
    name: high
    path: high.csv
    type: csv
    include_unmapped: true
- type: output
  name: low
  input: classify
  config:
    name: low
    path: low.csv
    type: csv
    include_unmapped: true
"#;

    // Two records fail, one succeeds
    let csv = "id,amount\n1,bad\n2,worse\n3,200\n";
    let (counters, dlq, _) = run_multi_output(yaml, csv).unwrap();

    assert_eq!(counters.dlq_count, 2);
    for entry in &dlq {
        assert_eq!(entry.route, None, "pre-routing errors must have null route");
        assert!(
            entry.stage.as_ref().unwrap().starts_with("transform:"),
            "pre-routing errors should have transform stage"
        );
    }
}

#[test]
fn test_dlq_columns_in_csv() {
    // Verify that DLQ CSV output includes _cxl_dlq_stage and _cxl_dlq_route columns.
    use crate::dlq::write_dlq;

    let schema = std::sync::Arc::new(clinker_record::Schema::new(vec!["name".into()]));
    let record = clinker_record::Record::new(schema.clone(), vec![Value::String("Alice".into())]);
    let entries = vec![DlqEntry {
        source_row: 1,
        category: crate::dlq::DlqErrorCategory::TypeCoercionFailure,
        error_message: "eval error".to_string(),
        original_record: record,
        stage: Some(DlqEntry::stage_transform("my_transform")),
        route: None,
        trigger: true,
    }];

    let mut buf = Vec::new();
    write_dlq(&mut buf, &entries, &schema, "input.csv", true, true).unwrap();
    let output = String::from_utf8(buf).unwrap();

    let header = output.lines().next().unwrap();
    assert!(
        header.contains("_cxl_dlq_stage"),
        "header should contain _cxl_dlq_stage: {header}"
    );
    assert!(
        header.contains("_cxl_dlq_route"),
        "header should contain _cxl_dlq_route: {header}"
    );

    // Check data row has the stage value
    let data = output.lines().nth(1).unwrap();
    assert!(
        data.contains("transform:my_transform"),
        "data should contain stage value: {data}"
    );
}

#[test]
fn test_dlq_backward_compat() {
    // Single-output pipeline DLQ has new columns with null (empty) values.
    let yaml = r#"
pipeline:
  name: test_dlq_compat
error_handling:
  strategy: continue
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    type: csv
    schema:
      - { name: id, type: string }
      - { name: amount, type: string }

- type: transform
  name: calc
  input: src
  config:
    cxl: 'emit val = amount.to_int()

      '
- type: output
  name: out
  input: calc
  config:
    name: out
    path: out.csv
    type: csv
    include_unmapped: true
"#;

    // "bad" fails to_int → DLQ
    let csv = "id,amount\n1,bad\n2,100\n";
    let (counters, dlq, _) = run_multi_output(yaml, csv).unwrap();

    assert_eq!(counters.dlq_count, 1);
    assert_eq!(dlq.len(), 1);
    // Single-output pipeline still populates stage
    assert!(
        dlq[0].stage.is_some(),
        "single-output DLQ should have stage field populated"
    );
    assert_eq!(
        dlq[0].route, None,
        "single-output DLQ should have null route"
    );

    // Verify the DLQ CSV includes new columns
    use crate::dlq::write_dlq;
    let schema = dlq[0].original_record.schema().clone();
    let mut buf = Vec::new();
    write_dlq(&mut buf, &dlq, &schema, "input.csv", true, true).unwrap();
    let output = String::from_utf8(buf).unwrap();
    let header = output.lines().next().unwrap();
    assert!(header.contains("_cxl_dlq_stage"));
    assert!(header.contains("_cxl_dlq_route"));
}

// --- HashMap registry backward compat tests ---

#[test]
fn test_single_writer_hashmap_backward_compat() {
    let yaml = r#"
pipeline:
  name: backward_compat_writer
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: id, type: string }
      - { name: name, type: string }

- type: transform
  name: identity
  input: src
  config:
    cxl: 'emit id_val = id

      '
- type: output
  name: dest
  input: identity
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;
    let (config, buffers) = multi_output_fixture(yaml);
    let params = test_params(&config);
    let input_csv = "id,name\n1,Alice\n2,Bob\n";

    let readers: crate::executor::SourceReaders = [(
        "src".to_string(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(input_csv.to_string())),
        ),
    )]
    .into_iter()
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

    let result = PipelineExecutor::run_with_readers_writers(
        &config,
        "src",
        readers,
        writers.into(),
        &params,
    );
    assert!(
        result.is_ok(),
        "single-writer HashMap should work: {result:?}"
    );

    let output = buffers["dest"].as_string();
    assert!(output.contains("Alice"), "output should contain Alice");
    assert!(output.contains("Bob"), "output should contain Bob");
}

#[test]
fn test_reader_hashmap_backward_compat() {
    let yaml = r#"
pipeline:
  name: backward_compat_reader
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: input.csv
    schema:
      - { name: x, type: string }

- type: transform
  name: identity
  input: src
  config:
    cxl: 'emit x_val = x

      '
- type: output
  name: dest
  input: identity
  config:
    name: dest
    type: csv
    path: output.csv
    include_unmapped: true
"#;
    let (config, buffers) = multi_output_fixture(yaml);
    let params = test_params(&config);
    let input_csv = "x\n42\n";

    let readers: crate::executor::SourceReaders = [(
        "src".to_string(),
        crate::executor::single_file_reader(
            "test.csv",
            Box::new(std::io::Cursor::new(input_csv.to_string())),
        ),
    )]
    .into_iter()
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

    let result = PipelineExecutor::run_with_readers_writers(
        &config,
        "src",
        readers,
        writers.into(),
        &params,
    );
    assert!(
        result.is_ok(),
        "single-reader HashMap should work: {result:?}"
    );

    let output = buffers["dest"].as_string();
    assert!(
        output.contains("42"),
        "output should contain data from single reader"
    );
}

// --- Route condition typecheck tests ---

#[test]
fn test_route_config_non_boolean_condition_typecheck_error() {
    // "amount + 1" returns Int, not Bool — compile_route should reject it
    let route_yaml = r#"
mode: exclusive
branches:
  - name: bad
    condition: "amount + 1"
default: fallback
"#;
    let route_config: crate::config::RouteConfig = crate::yaml::from_str(route_yaml).unwrap();
    let emitted_fields = vec!["amount".to_string()];
    let result =
        PipelineExecutor::compile_route(&route_config, &emitted_fields, &Default::default());
    match result {
        Err(e) => {
            let err = e.to_string();
            assert!(
                err.contains("Bool"),
                "error should mention Bool type requirement: {err}"
            );
        }
        Ok(_) => panic!("non-boolean route condition should fail typecheck"),
    }
}

#[test]
fn test_route_config_boolean_condition_passes() {
    // "amount > 10000" returns Bool — compile_route should succeed
    let route_yaml = r#"
mode: exclusive
branches:
  - name: high
    condition: "amount > 10000"
default: low
"#;
    let route_config: crate::config::RouteConfig = crate::yaml::from_str(route_yaml).unwrap();
    let emitted_fields = vec!["amount".to_string()];
    let result =
        PipelineExecutor::compile_route(&route_config, &emitted_fields, &Default::default());
    assert!(
        result.is_ok(),
        "boolean route condition should pass typecheck: {}",
        result.err().map(|e| e.to_string()).unwrap_or_default()
    );
}
