//! End-to-end integration tests for the time-windowed aggregate
//! primitive landing in <https://github.com/rustpunk/clinker/issues/61>.
//!
//! Coverage by AC:
//!
//! - AC1 — multi-source watermark gating. Records flowing through
//!   `[src_a, src_b] → merge → time_window aggregate` emit only after
//!   `min_across_sources` covers each window's `end +
//!   allowed_lateness`; out-of-order arrivals on the later-streaming
//!   side route to the DLQ as `LateRecord`.
//! - AC2 — `WatermarkConfig.delay` shifts the per-source watermark
//!   earlier; `allowed_lateness` shifts the operator-side close
//!   threshold later. Verified by asserting the per-source rollup on
//!   the `ExecutionReport` and the DLQ membership under different
//!   knob values.
//! - AC3 — `LateRecord` DLQ routing fires when a record's window
//!   has already closed at its arrival time. Verified end-to-end
//!   against the DLQ entries' `category` field.
//! - AC4 — single-source pipelines without `time_window` stay on
//!   the positional aggregate path unchanged; the watermark
//!   plumbing is a no-op for them.
//! - AC5 — `TimeWindowSpec::Hopping` emits one row per (key,
//!   window) and `TimeWindowSpec::Session` emits one row per (key,
//!   session). Verified by output-row enumeration.

use std::collections::HashMap;
use std::io::{self, Cursor, Write};
use std::sync::{Arc, Mutex};

use clinker_core::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
use clinker_core_types::dlq::DlqErrorCategory;
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
        execution_id: "time-window-test".to_string(),
        batch_id: "batch-001".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

fn run_pipeline(
    yaml: &str,
    sources: &[(&str, &str, &str)], // (source_name, file_label, csv)
) -> (ExecutionReport, String) {
    let config = parse_config(yaml).expect("parse_config");
    let params = test_params();

    let mut readers = HashMap::new();
    for (src_name, file_label, csv) in sources {
        readers.insert(
            src_name.to_string(),
            clinker_core::executor::single_file_reader(
                file_label,
                Box::new(Cursor::new(csv.as_bytes().to_vec())),
            ),
        );
    }

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        config.output_configs().next().unwrap().name.clone(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);

    let report = PipelineExecutor::run_plan_with_readers_writers(
        &PipelineConfig::compile(&config, &CompileContext::default()).expect("compile"),
        readers,
        writers,
        &params,
    )
    .expect("run");

    (report, buf.as_string())
}

fn sorted_body_lines(output: &str) -> Vec<String> {
    let mut lines: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    lines.sort();
    lines
}

// ─────────────────────────────────────────────────────────────────────────
// AC1 — multi-source watermark gating
// ─────────────────────────────────────────────────────────────────────────

/// [src_a, src_b] merged into a tumbling(size=1h) aggregate. Both
/// sources have records spanning the same time range; every record
/// emits because no source is lagging. Verifies the multi-source
/// merge path produces correct per-window counts.
#[test]
fn ac1_multi_source_emits_when_both_sources_in_sync() {
    let yaml = r#"
pipeline:
  name: ac1_in_sync
nodes:
- type: source
  name: src_a
  config:
    name: src_a
    type: csv
    path: a.csv
    watermark:
      column: event_ts
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: source
  name: src_b
  config:
    name: src_b
    type: csv
    path: b.csv
    watermark:
      column: event_ts
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: merge
  name: all
  inputs: [src_a, src_b]
- type: aggregate
  name: hourly
  input: all
  config:
    group_by: [user_id]
    time_window:
      tumbling: { size: 1h }
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: output
  name: out
  input: hourly
  config:
    name: out
    type: csv
    path: out.csv
error_handling:
  strategy: fail_fast
"#;
    let csv_a = "user_id,event_ts\n\
u1,2026-05-14T09:00:00\n\
u1,2026-05-14T09:10:00\n\
u2,2026-05-14T09:20:00\n";
    let csv_b = "user_id,event_ts\n\
u1,2026-05-14T09:30:00\n\
u2,2026-05-14T09:40:00\n";

    let (report, output) = run_pipeline(
        yaml,
        &[("src_a", "a.csv", csv_a), ("src_b", "b.csv", csv_b)],
    );

    let lines = sorted_body_lines(&output);
    // All five records land in window [09:00, 10:00):
    // - u1 = {src_a:9:00, src_a:9:10, src_b:9:30} = 3
    // - u2 = {src_a:9:20, src_b:9:40} = 2
    // No record's event-time falls behind the per-record running
    // watermark by more than allowed_lateness (0), so no LateRecord.
    assert_eq!(
        lines,
        vec!["u1,3".to_string(), "u2,2".to_string()],
        "output (sorted by user_id): {lines:?}",
    );
    assert_eq!(report.dlq_entries.len(), 0, "no late records expected");
}

// ─────────────────────────────────────────────────────────────────────────
// AC2 — delay + allowed_lateness
// ─────────────────────────────────────────────────────────────────────────

/// `WatermarkConfig.delay` shifts the source's effective watermark
/// earlier than its observed max event-time. With delay=10m, a
/// record observed at event-time T contributes T-10m to the
/// watermark. This is the source-side knob.
///
/// The test asserts the per-source rollup on the report reflects
/// the delay subtraction.
#[test]
fn ac2_watermark_delay_shifts_per_source_rollup_earlier() {
    let yaml = r#"
pipeline:
  name: ac2_delay
nodes:
- type: source
  name: clicks
  config:
    name: clicks
    type: csv
    path: in.csv
    watermark:
      column: event_ts
      delay: 10m
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: aggregate
  name: hourly
  input: clicks
  config:
    group_by: [user_id]
    time_window:
      tumbling: { size: 1h }
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: output
  name: out
  input: hourly
  config:
    name: out
    type: csv
    path: out.csv
error_handling:
  strategy: fail_fast
"#;
    let csv = "user_id,event_ts\nu1,2026-05-14T10:00:00\n";

    let (report, _output) = run_pipeline(yaml, &[("clicks", "in.csv", csv)]);

    let per_source = report
        .per_source_watermarks
        .get("clicks")
        .copied()
        .flatten()
        .expect("per-source watermark for clicks");
    // Event-time = 2026-05-14T10:00:00 UTC = 1747216800000000000 nanos.
    // Delay = 10m = 600s. Effective = event_time - delay.
    let event_nanos =
        chrono::NaiveDateTime::parse_from_str("2026-05-14T10:00:00", "%Y-%m-%dT%H:%M:%S")
            .unwrap()
            .and_utc()
            .timestamp_nanos_opt()
            .unwrap();
    let delay_nanos: i64 = 600 * 1_000_000_000;
    assert_eq!(
        per_source,
        event_nanos - delay_nanos,
        "delay must subtract from each observation before fold"
    );
}

/// `allowed_lateness` on the aggregate widens the close window. A
/// record arriving with a watermark that would otherwise mark its
/// window closed stays in-window when within `allowed_lateness`.
///
/// Setup: tumbling(size=1h), records straddling a window boundary,
/// then a deliberate out-of-order arrival from a different source
/// that pushes the watermark past the earlier window's end. With
/// `allowed_lateness: 0` the out-of-order record routes to DLQ;
/// with `allowed_lateness: 30m` the same record is admitted.
#[test]
fn ac2_allowed_lateness_widens_close_threshold() {
    fn pipeline(allowed_lateness: &str) -> String {
        format!(
            r#"
pipeline:
  name: ac2_lateness
nodes:
- type: source
  name: src_a
  config:
    name: src_a
    type: csv
    path: a.csv
    watermark: {{ column: event_ts }}
    schema:
      - {{ name: user_id, type: string }}
      - {{ name: event_ts, type: date_time }}
- type: source
  name: src_b
  config:
    name: src_b
    type: csv
    path: b.csv
    watermark: {{ column: event_ts }}
    schema:
      - {{ name: user_id, type: string }}
      - {{ name: event_ts, type: date_time }}
- type: merge
  name: all
  inputs: [src_a, src_b]
- type: aggregate
  name: hourly
  input: all
  config:
    group_by: [user_id]
    time_window:
      tumbling: {{ size: 1h }}
    allowed_lateness: {allowed_lateness}
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: output
  name: out
  input: hourly
  config:
    name: out
    type: csv
    path: out.csv
error_handling:
  strategy: continue
"#
        )
    }
    // src_a advances the running watermark to 10:25 over its two
    // records. src_b then delivers a record at 09:30 — its window
    // [09:00, 10:00) has end=10:00, and the per-record running
    // watermark at that moment is 10:25 (src_a's max) since src_b
    // hasn't observed yet. With allowed_lateness=0 the close
    // threshold equals 10:00; 10:25 >= 10:00 closes the window and
    // the record routes to DLQ. With allowed_lateness=30m the close
    // threshold widens to 10:30; 10:25 < 10:30 keeps the window
    // open and the record is admitted.
    let csv_a = "user_id,event_ts\nu1,2026-05-14T09:00:00\nu1,2026-05-14T10:25:00\n";
    let csv_b = "user_id,event_ts\nu1,2026-05-14T09:30:00\n";

    // Case A: allowed_lateness = 0 → late record.
    let (report_a, _out_a) = run_pipeline(
        &pipeline("0s"),
        &[("src_a", "a.csv", csv_a), ("src_b", "b.csv", csv_b)],
    );
    let late_count_a = report_a
        .dlq_entries
        .iter()
        .filter(|d| d.category == DlqErrorCategory::LateRecord)
        .count();
    assert_eq!(
        late_count_a, 1,
        "with allowed_lateness=0 the out-of-order src_b record must route as LateRecord"
    );

    // Case B: allowed_lateness = 30m → admitted.
    let (report_b, output_b) = run_pipeline(
        &pipeline("30m"),
        &[("src_a", "a.csv", csv_a), ("src_b", "b.csv", csv_b)],
    );
    let late_count_b = report_b
        .dlq_entries
        .iter()
        .filter(|d| d.category == DlqErrorCategory::LateRecord)
        .count();
    assert_eq!(
        late_count_b, 0,
        "with allowed_lateness=30m the same record must be admitted (DLQ entries: {:?})",
        report_b.dlq_entries
    );
    // Two u1 records in [09:00, 10:00): src_a@9:00 + src_b@9:30 = 2.
    // One u1 record in [10:00, 11:00): src_a@10:25 = 1.
    let lines = sorted_body_lines(&output_b);
    assert_eq!(lines, vec!["u1,1".to_string(), "u1,2".to_string()]);
}

// ─────────────────────────────────────────────────────────────────────────
// AC3 — LateRecord DLQ routing
// ─────────────────────────────────────────────────────────────────────────

/// A single source delivering deliberately out-of-order records.
/// The later-arriving record falls in a window that the
/// streaming-watermark has already closed by the time the record is
/// processed. Asserts the DLQ entry carries the expected category.
#[test]
fn ac3_late_record_routes_to_dlq_with_late_record_category() {
    let yaml = r#"
pipeline:
  name: ac3_late
nodes:
- type: source
  name: clicks
  config:
    name: clicks
    type: csv
    path: in.csv
    watermark: { column: event_ts }
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: aggregate
  name: hourly
  input: clicks
  config:
    group_by: [user_id]
    time_window:
      tumbling: { size: 1h }
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: output
  name: out
  input: hourly
  config:
    name: out
    type: csv
    path: out.csv
error_handling:
  strategy: continue
"#;
    // Records: 09:00, 10:30 (advances watermark past 10:00), 09:30
    // (out-of-order; window [09:00, 10:00) already closed).
    let csv = "user_id,event_ts\nu1,2026-05-14T09:00:00\nu1,2026-05-14T10:30:00\nu1,2026-05-14T09:30:00\n";

    let (report, _output) = run_pipeline(yaml, &[("clicks", "in.csv", csv)]);
    let late_entries: Vec<_> = report
        .dlq_entries
        .iter()
        .filter(|d| d.category == DlqErrorCategory::LateRecord)
        .collect();
    assert_eq!(
        late_entries.len(),
        1,
        "expected exactly one LateRecord DLQ entry; got {:?}",
        report.dlq_entries
    );
    let entry = late_entries[0];
    assert!(
        entry
            .stage
            .as_deref()
            .map(|s| s.starts_with("time_window:"))
            .unwrap_or(false),
        "LateRecord stage should be `time_window:<node>`; got {:?}",
        entry.stage
    );
    assert!(
        entry.error_message.contains("[") && entry.error_message.contains(")"),
        "LateRecord detail should carry the window bounds; got {:?}",
        entry.error_message
    );
}

// ─────────────────────────────────────────────────────────────────────────
// AC4 — single-source positional aggregate unaffected
// ─────────────────────────────────────────────────────────────────────────

/// Positional aggregate (no `time_window` declared) on a source
/// WITHOUT `watermark.column`. The plan-time E156 guard MUST NOT
/// fire, and execution produces the standard single-bucket-per-key
/// output. Confirms the time-windowed surface adds no overhead /
/// no spurious validation for pipelines that don't opt in.
#[test]
fn ac4_positional_aggregate_without_watermark_compiles_and_runs() {
    let yaml = r#"
pipeline:
  name: ac4_positional
nodes:
- type: source
  name: clicks
  config:
    name: clicks
    type: csv
    path: in.csv
    schema:
      - { name: user_id, type: string }
- type: aggregate
  name: totals
  input: clicks
  config:
    group_by: [user_id]
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: output
  name: out
  input: totals
  config:
    name: out
    type: csv
    path: out.csv
error_handling:
  strategy: fail_fast
"#;
    let csv = "user_id\nu1\nu1\nu1\nu2\nu2\n";
    let (report, output) = run_pipeline(yaml, &[("clicks", "in.csv", csv)]);
    assert_eq!(report.dlq_entries.len(), 0);
    let lines = sorted_body_lines(&output);
    assert_eq!(lines, vec!["u1,3".to_string(), "u2,2".to_string()]);
}

// ─────────────────────────────────────────────────────────────────────────
// AC5 — HOP and SESSION emission counts
// ─────────────────────────────────────────────────────────────────────────

/// Hopping(size=10m, slide=5m) over a single source. Each record at
/// time `t` belongs to up to 2 overlapping windows (`size / slide`).
/// Verifies the operator emits one row per (key, window-start) and
/// that overlap math matches Flink TVF HOP.
#[test]
fn ac5_hop_emits_one_row_per_key_per_window() {
    let yaml = r#"
pipeline:
  name: ac5_hop
nodes:
- type: source
  name: clicks
  config:
    name: clicks
    type: csv
    path: in.csv
    watermark: { column: event_ts }
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: aggregate
  name: sliding
  input: clicks
  config:
    group_by: [user_id]
    time_window:
      hopping:
        size: 10m
        slide: 5m
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: output
  name: out
  input: sliding
  config:
    name: out
    type: csv
    path: out.csv
error_handling:
  strategy: fail_fast
"#;
    // Single record at 09:00. Hop windows containing it:
    // - w_start = 8:55 → [8:55, 9:05) ✓ (8:55 <= 9:00 < 9:05)
    // - w_start = 9:00 → [9:00, 9:10) ✓
    // So 2 windows; 2 emit rows. (w_start = 8:50 ends at 9:00 which
    // does NOT contain 9:00 by the half-open convention.)
    let csv = "user_id,event_ts\nu1,2026-05-14T09:00:00\n";
    let (report, output) = run_pipeline(yaml, &[("clicks", "in.csv", csv)]);
    assert_eq!(report.dlq_entries.len(), 0);
    let lines = sorted_body_lines(&output);
    assert_eq!(
        lines.len(),
        2,
        "single record must emit one row per overlapping window; got {lines:?}"
    );
    assert!(lines.iter().all(|l| l == "u1,1"));
}

/// Session(gap=5m) per user. Records cluster into sessions; the
/// operator emits one row per (user, session_idx). Verifies the
/// session boundary discovery (sort-by-event-time + gap-walk)
/// against a deliberately gap-crossing input.
#[test]
fn ac5_session_emits_one_row_per_key_per_session() {
    let yaml = r#"
pipeline:
  name: ac5_session
nodes:
- type: source
  name: logins
  config:
    name: logins
    type: csv
    path: in.csv
    watermark: { column: event_ts }
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: aggregate
  name: sessions
  input: logins
  config:
    group_by: [user_id]
    time_window:
      session: { gap: 5m }
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: output
  name: out
  input: sessions
  config:
    name: out
    type: csv
    path: out.csv
error_handling:
  strategy: fail_fast
"#;
    // u1 has three records at 09:00, 09:03, 09:04 — single session.
    // Then 09:20, 09:22 — second session (20 > 04 + 5min).
    // u2 has one record at 09:10 — single session.
    let csv = "user_id,event_ts\n\
u1,2026-05-14T09:00:00\n\
u1,2026-05-14T09:03:00\n\
u1,2026-05-14T09:04:00\n\
u2,2026-05-14T09:10:00\n\
u1,2026-05-14T09:20:00\n\
u1,2026-05-14T09:22:00\n";

    let (report, output) = run_pipeline(yaml, &[("logins", "in.csv", csv)]);
    assert_eq!(report.dlq_entries.len(), 0);
    let lines = sorted_body_lines(&output);
    // u1: two sessions (counts 3 and 2). u2: one session (count 1).
    // Sorted lexically: "u1,2", "u1,3", "u2,1".
    assert_eq!(
        lines,
        vec!["u1,2".to_string(), "u1,3".to_string(), "u2,1".to_string()],
    );
}
