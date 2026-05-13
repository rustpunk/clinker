//! Integration tests for per-source / per-file event-time watermark
//! propagation. Covers the four-granularity contract:
//!
//! - `per_source_file_watermarks` — finest granularity, one entry per
//!   `(source_name, source_file_path)` pair.
//! - `per_source_watermarks` — `min` rollup across each source's
//!   files.
//! - `effective_watermark` — `min` rollup across declared sources.
//! - Sources without `watermark:` declared contribute nothing at any
//!   level; sources declared but with zero observed records appear
//!   in the per-source map with `None`.
//!
//! Acceptance criterion 1 of https://github.com/rustpunk/clinker/issues/52
//! ("aggregate waits for src_b to advance past each window's end")
//! is exercised structurally here — the engine has no time-windowed
//! aggregate operator today (tracked in
//! https://github.com/rustpunk/clinker/issues/61), so this suite
//! pins the carrier and rollup contract that operator will read.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, SourceReaders, WriterRegistry};
use clinker_core::source::multi_file::FileSlot;

fn slot(name: &str, csv: &str) -> FileSlot {
    FileSlot::new(
        PathBuf::from(format!("{name}.csv")),
        Box::new(Cursor::new(csv.as_bytes().to_vec())),
    )
}

fn writer(buf: &SharedBuffer) -> Box<dyn std::io::Write + Send> {
    Box::new(buf.clone())
}

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// ISO 8601 second-precision datetime parsed by Clinker's default
/// coercion chain (`DEFAULT_DATETIME_FORMATS`). Returns the i64
/// nanoseconds-since-epoch the watermark map folds into its max.
fn iso_dt_nanos(s: &str) -> i64 {
    let nd =
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S").expect("parse ISO datetime");
    nd.and_utc().timestamp_nanos_opt().expect("nanos in range")
}

/// Two sources of differing event-time density. Pins per-source +
/// effective-watermark rollups. Acceptance criterion 2 of
/// https://github.com/rustpunk/clinker/issues/52.
#[tokio::test(flavor = "multi_thread")]
async fn two_sources_differing_density() {
    let yaml = r#"
pipeline:
  name: per_source_watermarks
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      watermark:
        column: event_time
      schema:
        - { name: id, type: int }
        - { name: event_time, type: date_time }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      watermark:
        column: event_time
      schema:
        - { name: id, type: int }
        - { name: event_time, type: date_time }
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            vec![slot(
                "a",
                "id,event_time\n1,2026-01-01T00:00:00\n2,2026-01-01T00:01:00\n3,2026-01-01T00:02:00\n",
            )],
        ),
        (
            "src_b".to_string(),
            vec![slot(
                "b",
                "id,event_time\n10,2026-01-01T00:00:30\n11,2026-01-01T00:00:45\n",
            )],
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("execute multi-source watermarked pipeline");

    let a_max = iso_dt_nanos("2026-01-01T00:02:00");
    let b_max = iso_dt_nanos("2026-01-01T00:00:45");
    assert_eq!(
        report.per_source_watermarks.get("src_a"),
        Some(&Some(a_max))
    );
    assert_eq!(
        report.per_source_watermarks.get("src_b"),
        Some(&Some(b_max))
    );
    assert_eq!(report.effective_watermark, Some(a_max.min(b_max)));

    // Per-file map mirrors the source-level rollup for the single-
    // file-per-source case.
    let a_file_key = ("src_a".to_string(), "a.csv".to_string());
    let b_file_key = ("src_b".to_string(), "b.csv".to_string());
    assert_eq!(
        report.per_source_file_watermarks.get(&a_file_key),
        Some(&Some(a_max)),
    );
    assert_eq!(
        report.per_source_file_watermarks.get(&b_file_key),
        Some(&Some(b_max)),
    );

    // Merge correctness regression — both sources' records survive.
    assert_eq!(report.counters.total_count, 5);
}

/// One source pulling two files. The file with the trailing watermark
/// holds the source-level rollup back to its max — matching the
/// Flink/Arroyo per-partition + min reducer pattern. This is the
/// invariant the `fan_out_per_source_file` 1:1 source-file → sink
/// topology relies on.
#[tokio::test(flavor = "multi_thread")]
async fn glob_source_multi_file_per_partition() {
    let yaml = r#"
pipeline:
  name: glob_source_watermark
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      paths: [a.csv, b.csv]
      watermark:
        column: event_time
      schema:
        - { name: id, type: int }
        - { name: event_time, type: date_time }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        vec![
            slot(
                "a",
                "id,event_time\n1,2026-01-01T00:00:01\n2,2026-01-01T00:00:02\n3,2026-01-01T00:00:03\n",
            ),
            slot(
                "b",
                "id,event_time\n10,2026-01-01T01:00:00\n11,2026-01-01T02:00:00\n12,2026-01-01T03:00:00\n",
            ),
        ],
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("execute glob source");

    let a_max = iso_dt_nanos("2026-01-01T00:00:03");
    let b_max = iso_dt_nanos("2026-01-01T03:00:00");
    let a_key = ("src".to_string(), "a.csv".to_string());
    let b_key = ("src".to_string(), "b.csv".to_string());
    assert_eq!(
        report.per_source_file_watermarks.get(&a_key),
        Some(&Some(a_max))
    );
    assert_eq!(
        report.per_source_file_watermarks.get(&b_key),
        Some(&Some(b_max))
    );
    // Source-level rollup = min across files — file b's late watermark
    // cannot finalize file a's data unilaterally.
    assert_eq!(report.per_source_watermarks.get("src"), Some(&Some(a_max)));
    assert_eq!(report.effective_watermark, Some(a_max));
}

/// Acceptance criterion 4: single-source pipelines still produce a
/// per-source-watermarks report entry equal to `max(event_time)`,
/// with no regression in pipeline output.
#[tokio::test(flavor = "multi_thread")]
async fn single_source_unaffected() {
    let yaml = r#"
pipeline:
  name: single_source_watermark
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: a.csv
      watermark:
        column: event_time
      schema:
        - { name: id, type: int }
        - { name: event_time, type: date_time }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        vec![slot(
            "a",
            "id,event_time\n1,2026-01-01T00:00:01\n2,2026-01-01T00:00:02\n",
        )],
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("execute single-source watermarked pipeline");

    let expected = iso_dt_nanos("2026-01-01T00:00:02");
    assert_eq!(
        report.per_source_watermarks.get("src"),
        Some(&Some(expected))
    );
    assert_eq!(report.effective_watermark, Some(expected));
    assert_eq!(report.counters.total_count, 2);
}

/// A source without `watermark:` declared contributes nothing to any
/// rollup. The `effective_watermark` then equals the declared source's
/// rollup alone — the undeclared source does not pull the min toward
/// `None`.
#[tokio::test(flavor = "multi_thread")]
async fn undeclared_source_skipped_at_min() {
    let yaml = r#"
pipeline:
  name: undeclared_skipped
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      watermark:
        column: event_time
      schema:
        - { name: id, type: int }
        - { name: event_time, type: date_time }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: event_time, type: date_time }
  - type: merge
    name: merged
    inputs: [src_a, src_b]
  - type: output
    name: out
    input: merged
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            vec![slot(
                "a",
                "id,event_time\n1,2026-01-01T00:00:01\n2,2026-01-01T00:00:02\n",
            )],
        ),
        (
            "src_b".to_string(),
            vec![slot(
                "b",
                "id,event_time\n100,2030-01-01T00:00:00\n200,2030-01-01T00:00:01\n",
            )],
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("execute mixed-watermark pipeline");

    let a_max = iso_dt_nanos("2026-01-01T00:00:02");
    assert_eq!(
        report.per_source_watermarks.get("src_a"),
        Some(&Some(a_max))
    );
    // src_b is undeclared — does not appear in per_source_watermarks.
    assert!(!report.per_source_watermarks.contains_key("src_b"));
    // Cross-source min = the only declared source's rollup.
    assert_eq!(report.effective_watermark, Some(a_max));
}

/// Pipeline with zero `watermark:` declarations: report is well-formed
/// (empty maps, `None` effective watermark), pipeline output is
/// unchanged from a pre-sprint baseline.
#[tokio::test(flavor = "multi_thread")]
async fn no_watermarks_anywhere() {
    let yaml = r#"
pipeline:
  name: no_watermarks
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders =
        HashMap::from([("src".to_string(), vec![slot("a", "id,tag\n1,foo\n2,bar\n")])]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("execute no-watermark pipeline");

    assert!(report.per_source_watermarks.is_empty());
    assert!(report.per_source_file_watermarks.is_empty());
    assert_eq!(report.effective_watermark, None);
    assert_eq!(report.counters.total_count, 2);
}

/// A source with `watermark:` declared but zero matching records (the
/// file is empty / contains only headers): the declared-source list
/// still surfaces the entry with `None`, and rollups gracefully
/// degrade.
#[tokio::test(flavor = "multi_thread")]
async fn zero_records_keeps_declared_entry_with_none() {
    let yaml = r#"
pipeline:
  name: zero_records_watermark
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: a.csv
      watermark:
        column: event_time
      schema:
        - { name: id, type: int }
        - { name: event_time, type: date_time }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();
    let readers: SourceReaders =
        HashMap::from([("src".to_string(), vec![slot("a", "id,event_time\n")])]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("execute empty-input watermarked pipeline");

    // Declared, but with no observations: source entry exists with
    // None. Per-file map is empty because no record was observed.
    assert_eq!(report.per_source_watermarks.get("src"), Some(&None));
    assert!(report.per_source_file_watermarks.is_empty());
    // min across {None} = None (Spark-style skip-`None` semantics).
    assert_eq!(report.effective_watermark, None);
}

/// End-to-end: a glob source feeding a `fan_out_per_source_file`
/// output. Per-file watermarks must remain separate in the report
/// AND each output file must contain only its own source-file's
/// records. Exercises the 1:1 source-file → sink invariant the
/// granularity decision is designed to preserve.
#[tokio::test(flavor = "multi_thread")]
async fn fan_out_per_source_file_watermark_isolation() {
    let yaml = r#"
pipeline:
  name: fan_out_watermark_isolation
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      paths: [a.csv, b.csv]
      watermark:
        column: event_time
      schema:
        - { name: id, type: int }
        - { name: event_time, type: date_time }
  - type: output
    name: out
    input: src
    config:
      name: out
      type: csv
      path: out_{source_file}.csv
"#;
    let config = parse_config(yaml).unwrap();
    let plan = config.compile(&CompileContext::default()).unwrap();

    // The reader builds its own Arc<str> for `$source.file` from the
    // FileSlot path. Matching by string value (Arc<str> compares by
    // contents via HashMap's Borrow<str> impl).
    let arc_a: Arc<str> = Arc::from("a.csv");
    let arc_b: Arc<str> = Arc::from("b.csv");
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        vec![
            FileSlot::new(
                PathBuf::from(arc_a.as_ref()),
                Box::new(Cursor::new(
                    "id,event_time\n1,2026-01-01T00:00:01\n2,2026-01-01T00:00:02\n3,2026-01-01T00:00:03\n"
                        .as_bytes()
                        .to_vec(),
                )),
            ),
            FileSlot::new(
                PathBuf::from(arc_b.as_ref()),
                Box::new(Cursor::new(
                    "id,event_time\n10,2026-01-01T05:00:00\n11,2026-01-01T06:00:00\n"
                        .as_bytes()
                        .to_vec(),
                )),
            ),
        ],
    )]);

    let buf_a = SharedBuffer::new();
    let buf_b = SharedBuffer::new();
    let mut per_file: HashMap<Arc<str>, Box<dyn std::io::Write + Send>> = HashMap::new();
    per_file.insert(
        Arc::clone(&arc_a),
        Box::new(buf_a.clone()) as Box<dyn std::io::Write + Send>,
    );
    per_file.insert(
        Arc::clone(&arc_b),
        Box::new(buf_b.clone()) as Box<dyn std::io::Write + Send>,
    );
    let mut fan_out: HashMap<String, HashMap<Arc<str>, Box<dyn std::io::Write + Send>>> =
        HashMap::new();
    fan_out.insert("out".to_string(), per_file);
    let writers = WriterRegistry {
        single: HashMap::new(),
        fan_out,
    };

    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .await
            .expect("run fan-out watermark pipeline");

    // Per-file watermarks reported independently — the early file's
    // max watermark does not creep into the late file's slot, and
    // vice versa.
    let a_key = ("src".to_string(), "a.csv".to_string());
    let b_key = ("src".to_string(), "b.csv".to_string());
    let a_max = iso_dt_nanos("2026-01-01T00:00:03");
    let b_max = iso_dt_nanos("2026-01-01T06:00:00");
    assert_eq!(
        report.per_source_file_watermarks.get(&a_key),
        Some(&Some(a_max))
    );
    assert_eq!(
        report.per_source_file_watermarks.get(&b_key),
        Some(&Some(b_max))
    );
    // Source-level rollup = min over files (file a's max).
    assert_eq!(report.per_source_watermarks.get("src"), Some(&Some(a_max)));

    // The 1:1 source-file → sink invariant: each output file's buffer
    // contains exactly that source file's records, no leakage in
    // either direction.
    let out_a = buf_a.as_string();
    let out_b = buf_b.as_string();
    for early_id in ["1", "2", "3"] {
        assert!(
            out_a.contains(&format!(",{early_id},")) || out_a.contains(&format!("\n{early_id},")),
            "buf_a missing id {early_id}: {out_a}"
        );
        assert!(
            !(out_b.contains(&format!(",{early_id},"))
                || out_b.contains(&format!("\n{early_id},"))),
            "buf_b leaked id {early_id}: {out_b}"
        );
    }
    for late_id in ["10", "11"] {
        assert!(
            out_b.contains(&format!(",{late_id},")) || out_b.contains(&format!("\n{late_id},")),
            "buf_b missing id {late_id}: {out_b}"
        );
        assert!(
            !(out_a.contains(&format!(",{late_id},")) || out_a.contains(&format!("\n{late_id},"))),
            "buf_a leaked id {late_id}: {out_a}"
        );
    }
}
