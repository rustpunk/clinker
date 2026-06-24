//! Multi-output routing integration tests.
//!
//! Drive the multi-writer registry, per-output channels, and DLQ
//! stage/route extensions end-to-end through the public executor entry
//! point. The route-evaluation white-box tests (build + evaluate a route
//! through the crate-private `CompiledRoute::from_node` / `CompiledRoute`
//! seam) stay inline in `executor/tests/multi_output.rs`.

mod common;
#[path = "common/multi_output_fixtures.rs"]
mod multi_output_fixtures;

use std::collections::HashMap;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{DlqEntry, PipelineRunParams};
use clinker_plan::error::PipelineError;
use clinker_record::{PipelineCounters, Value};

/// Counters, DLQ entries, and per-output CSV bodies keyed by output name —
/// the result of driving a multi-output pipeline to completion.
type MultiOutputResult =
    Result<(PipelineCounters, Vec<DlqEntry>, HashMap<String, String>), PipelineError>;

/// Build a multi-output test fixture with the given YAML config.
///
/// Returns the parsed `PipelineConfig` and a `HashMap<String, SharedBuffer>`
/// with one entry per output defined in the config. The buffer names match
/// the output `name` fields in the YAML.
fn multi_output_fixture(
    yaml: &str,
) -> (
    clinker_plan::config::PipelineConfig,
    HashMap<String, SharedBuffer>,
) {
    let config = clinker_plan::config::parse_config(yaml).unwrap();
    let buffers: HashMap<String, SharedBuffer> = config
        .output_configs()
        .map(|o| (o.name.clone(), SharedBuffer::new()))
        .collect();
    (config, buffers)
}

/// Build default `PipelineRunParams` for tests.
fn test_params() -> PipelineRunParams {
    let pipeline_vars = indexmap::IndexMap::new();
    PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars,
        shutdown_token: None,
        ..Default::default()
    }
}

/// Helper: run a multi-output pipeline and return per-output CSV strings.
fn run_multi_output(yaml: &str, csv_input: &str) -> MultiOutputResult {
    let (config, buffers) = multi_output_fixture(yaml);
    let params = test_params();

    let primary = config.source_configs().next().unwrap().name.clone();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        primary.clone(),
        clinker_exec::executor::single_file_reader(
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

    let report = common::run_config(&config, readers, writers, &params)?;

    let outputs: HashMap<String, String> = buffers
        .iter()
        .map(|(name, buf)| (name.clone(), buf.as_string()))
        .collect();

    Ok((report.counters, report.dlq_entries, outputs))
}

#[test]
fn test_multi_output_two_writers() {
    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_two_outputs",
        r#"- type: route
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
"#,
    );

    let csv = "id,amount\n1,200\n2,50\n3,300\n4,10\n";
    let (counters, _, outputs) = run_multi_output(&yaml, csv).unwrap();

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
    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_three_outputs",
        r#"- type: route
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
"#,
    );

    let csv = "id,amount\n1,5000\n2,500\n3,50\n";
    let (counters, _, outputs) = run_multi_output(&yaml, csv).unwrap();

    assert_eq!(counters.ok_count, 3);
    assert!(outputs["high"].contains("1,5000"));
    assert!(outputs["medium"].contains("2,500"));
    assert!(outputs["low"].contains("3,50"));
}

#[test]
fn test_multi_output_record_counts() {
    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_record_counts",
        r#"- type: route
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
"#,
    );

    let csv = "id,amount\n1,100\n2,10\n3,200\n4,20\n5,300\n";
    let (counters, _, outputs) = run_multi_output(&yaml, csv).unwrap();

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
    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_order",
        r#"- type: route
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
"#,
    );

    // All go to "big" — order must match input order
    let csv = "id,amount\n1,100\n2,200\n3,300\n4,400\n5,500\n";
    let (_, _, outputs) = run_multi_output(&yaml, csv).unwrap();

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
                return Err(std::io::Error::other("simulated write failure"));
            }
            let n = buf.len().min(self.remaining);
            self.remaining -= n;
            Ok(n)
        }
        fn flush(&mut self) -> std::io::Result<()> {
            if self.remaining == 0 {
                Err(std::io::Error::other("simulated flush failure"))
            } else {
                Ok(())
            }
        }
    }

    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_writer_error",
        r#"- type: route
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
"#,
    );

    let config = clinker_plan::config::parse_config(&yaml).unwrap();
    let params = test_params();

    let csv = "id,amount\n1,100\n2,10\n3,200\n";
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_exec::executor::single_file_reader(
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

    let result = common::run_config(&config, readers, writers, &params);
    assert!(result.is_err(), "should propagate writer error");
}

#[test]
fn test_multi_output_inclusive_duplicate() {
    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_inclusive",
        r#"- type: route
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
"#,
    );

    // amount=500 matches both audit (>100) and report (>50)
    let csv = "id,amount\n1,500\n2,30\n";
    let (counters, _, outputs) = run_multi_output(&yaml, csv).unwrap();

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
    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_empty_route",
        r#"- type: route
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
"#,
    );

    // No records match "special" (all < 99999)
    let csv = "id,amount\n1,100\n2,200\n";
    let (_, _, outputs) = run_multi_output(&yaml, csv).unwrap();

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
            Err(std::io::Error::other("already panicked"))
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_panic",
        r#"- type: route
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
"#,
    );

    let config = clinker_plan::config::parse_config(&yaml).unwrap();
    let params = test_params();

    let csv = "id,amount\n1,100\n2,10\n";
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_exec::executor::single_file_reader(
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

    // Panic should be caught and propagated, not hang or abort. Run the
    // executor on a dedicated OS thread so a panic inside it surfaces
    // through `JoinHandle::join` returning `Err` — the synchronous
    // analogue of catching an unwinding panic at the join point.
    let join = std::thread::spawn(move || common::run_config(&config, readers, writers, &params));
    let join_result = join.join();
    assert!(
        join_result.is_err(),
        "writer panic should propagate as a thread-join error, got: {:?}",
        join_result.as_ref().map(|_| "Ok(_)")
    );
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

    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_disconnected",
        r#"- type: route
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
"#,
    );

    let config = clinker_plan::config::parse_config(&yaml).unwrap();
    let params = test_params();

    // Enough records that "b" gets traffic and will error
    let csv = "id,amount\n1,100\n2,10\n3,200\n4,20\n";
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_exec::executor::single_file_reader(
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
    let result = common::run_config(&config, readers, writers, &params);
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
            Err(std::io::Error::other("flush failed"))
        }
    }

    let yaml = multi_output_fixtures::multi_output_pipeline(
        "test_multiple_errors",
        r#"- type: route
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
"#,
    );

    let config = clinker_plan::config::parse_config(&yaml).unwrap();
    let params = test_params();

    let csv = "id,amount\n1,100\n2,10\n";
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_exec::executor::single_file_reader(
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

    let result = common::run_config(&config, readers, writers, &params);
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

#[test]
fn test_dlq_stage_source() {
    // Unit test: verify DlqEntry::stage_source() produces "source"
    // and a DlqEntry with stage "source" has route: None.
    let schema = std::sync::Arc::new(clinker_record::Schema::new(vec!["id".into()]));
    let record = clinker_record::Record::new(schema, vec![Value::String("1".into())]);
    let entry = DlqEntry {
        source_row: 1,
        category: clinker_core_types::dlq::DlqErrorCategory::TypeCoercionFailure,
        error_message: "source read error".to_string(),
        original_record: record,
        stage: Some(DlqEntry::stage_source()),
        route: None,
        trigger: true,
        source_name: std::sync::Arc::from("test_source"),
        triggering_field: None,
        triggering_value: None,
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
        category: clinker_core_types::dlq::DlqErrorCategory::TypeCoercionFailure,
        error_message: "write error".to_string(),
        original_record: record,
        stage: Some(DlqEntry::stage_output("results")),
        route: Some("high_value".to_string()),
        trigger: true,
        source_name: std::sync::Arc::from("test_source"),
        triggering_field: None,
        triggering_value: None,
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
    use clinker_exec::dlq::write_dlq;

    let schema = std::sync::Arc::new(clinker_record::Schema::new(vec!["name".into()]));
    let record = clinker_record::Record::new(schema.clone(), vec![Value::String("Alice".into())]);
    let entries = vec![DlqEntry {
        source_row: 1,
        category: clinker_core_types::dlq::DlqErrorCategory::TypeCoercionFailure,
        error_message: "eval error".to_string(),
        original_record: record,
        stage: Some(DlqEntry::stage_transform("my_transform")),
        route: None,
        trigger: true,
        source_name: std::sync::Arc::from("test_source"),
        triggering_field: None,
        triggering_value: None,
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
    use clinker_exec::dlq::write_dlq;
    let schema = dlq[0].original_record.schema().clone();
    let mut buf = Vec::new();
    write_dlq(&mut buf, &dlq, &schema, "input.csv", true, true).unwrap();
    let output = String::from_utf8(buf).unwrap();
    let header = output.lines().next().unwrap();
    assert!(header.contains("_cxl_dlq_stage"));
    assert!(header.contains("_cxl_dlq_route"));
}

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
    let params = test_params();
    let input_csv = "id,name\n1,Alice\n2,Bob\n";

    let readers: clinker_exec::executor::SourceReaders = [(
        "src".to_string(),
        clinker_exec::executor::single_file_reader(
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

    let result = common::run_config(&config, readers, writers, &params);
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
    let params = test_params();
    let input_csv = "x\n42\n";

    let readers: clinker_exec::executor::SourceReaders = [(
        "src".to_string(),
        clinker_exec::executor::single_file_reader(
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

    let result = common::run_config(&config, readers, writers, &params);
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
