//! Plan-time validation for `SourceConfig.watermark` and the
//! aggregate-side `time_window` consumer.
//!
//! Three negative paths surface as compile-time diagnostics:
//! - E154 — the named watermark column is not present in the source's
//!   declared `schema:` block.
//! - E155 — the named watermark column exists but its declared CXL
//!   type is not `date_time` or `date` (not event-time-coercible).
//! - E156 — an aggregate declares `time_window:` but at least one
//!   upstream-reachable Source does not declare `watermark.column`,
//!   which would hold `min_across_sources` at `None` forever and
//!   silently suppress every window emission.

use crate::config::{CompileContext, parse_config};

fn compile_diagnostics(yaml: &str) -> Vec<(String, String)> {
    let config = parse_config(yaml).expect("parse_config");
    let diags = config
        .compile(&CompileContext::default())
        .expect_err("compile should fail");
    diags
        .iter()
        .map(|d| (d.code.clone(), d.message.clone()))
        .collect()
}

#[test]
fn watermark_column_not_in_schema_emits_e154() {
    let yaml = r#"
pipeline:
  name: watermark_missing_column
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: a.csv
    watermark:
      column: nonexistent_column
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
    let diags = compile_diagnostics(yaml);
    let hit = diags
        .iter()
        .find(|(code, _)| code == "E154")
        .unwrap_or_else(|| panic!("expected E154, got {diags:?}"));
    assert!(
        hit.1.contains("nonexistent_column"),
        "E154 message should name the missing column: {}",
        hit.1
    );
}

#[test]
fn watermark_column_wrong_type_emits_e155() {
    let yaml = r#"
pipeline:
  name: watermark_wrong_type
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: a.csv
    watermark:
      column: id
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
    let diags = compile_diagnostics(yaml);
    let hit = diags
        .iter()
        .find(|(code, _)| code == "E155")
        .unwrap_or_else(|| panic!("expected E155, got {diags:?}"));
    assert!(
        hit.1.contains("id"),
        "E155 message should name the column: {}",
        hit.1
    );
}

#[test]
fn watermark_on_date_column_compiles_clean() {
    let yaml = r#"
pipeline:
  name: watermark_date_ok
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: a.csv
    watermark:
      column: day
    schema:
      - { name: id, type: int }
      - { name: day, type: date }
- type: output
  name: out
  input: src
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    config
        .compile(&CompileContext::default())
        .expect("Date-typed watermark column compiles clean");
}

#[test]
fn time_window_aggregate_without_upstream_watermark_emits_e156() {
    let yaml = r#"
pipeline:
  name: time_window_missing_watermark
nodes:
- type: source
  name: clicks
  config:
    name: clicks
    type: csv
    path: clicks.csv
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: aggregate
  name: hourly_clicks
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
  input: hourly_clicks
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let diags = compile_diagnostics(yaml);
    let hit = diags
        .iter()
        .find(|(code, _)| code == "E156")
        .unwrap_or_else(|| panic!("expected E156, got {diags:?}"));
    assert!(
        hit.1.contains("\"clicks\""),
        "E156 message should name the watermark-less source: {}",
        hit.1
    );
    assert!(
        hit.1.contains("time_window"),
        "E156 message should mention time_window: {}",
        hit.1
    );
}

#[test]
fn time_window_aggregate_with_watermark_on_every_upstream_source_compiles_clean() {
    let yaml = r#"
pipeline:
  name: time_window_ok
nodes:
- type: source
  name: clicks
  config:
    name: clicks
    type: csv
    path: clicks.csv
    watermark:
      column: event_ts
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: aggregate
  name: hourly_clicks
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
  input: hourly_clicks
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    config
        .compile(&CompileContext::default())
        .expect("time-windowed aggregate with watermarked source compiles clean");
}

#[test]
fn time_window_aggregate_partial_upstream_watermark_through_merge_emits_e156() {
    // One source watermarked, one not — E156 must fire because
    // min_across_sources can't include a source that emits no
    // watermark observations.
    let yaml = r#"
pipeline:
  name: time_window_partial_watermark
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
    schema:
      - { name: user_id, type: string }
      - { name: event_ts, type: date_time }
- type: merge
  name: merged
  inputs: [src_a, src_b]
- type: aggregate
  name: hourly_clicks
  input: merged
  config:
    group_by: [user_id]
    time_window:
      tumbling: { size: 1h }
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: output
  name: out
  input: hourly_clicks
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let diags = compile_diagnostics(yaml);
    let hit = diags
        .iter()
        .find(|(code, _)| code == "E156")
        .unwrap_or_else(|| panic!("expected E156, got {diags:?}"));
    assert!(
        hit.1.contains("\"src_b\""),
        "E156 message should name the watermark-less source: {}",
        hit.1
    );
    assert!(
        !hit.1.contains("\"src_a\""),
        "E156 message must not name the watermarked source: {}",
        hit.1
    );
}

#[test]
fn aggregate_without_time_window_does_not_require_upstream_watermark() {
    // Today's positional aggregate path — no time_window means no
    // watermark dependency. E156 must NOT fire.
    let yaml = r#"
pipeline:
  name: positional_aggregate_no_watermark
nodes:
- type: source
  name: clicks
  config:
    name: clicks
    type: csv
    path: clicks.csv
    schema:
      - { name: user_id, type: string }
- type: aggregate
  name: by_user
  input: clicks
  config:
    group_by: [user_id]
    cxl: |
      emit user_id = user_id
      emit n = count(*)
- type: output
  name: out
  input: by_user
  config:
    name: out
    type: csv
    path: out.csv
"#;
    let config = parse_config(yaml).expect("parse");
    config
        .compile(&CompileContext::default())
        .expect("positional aggregate without time_window stays watermark-agnostic");
}

#[test]
fn watermark_absent_compiles_clean() {
    let yaml = r#"
pipeline:
  name: no_watermark
nodes:
- type: source
  name: src
  config:
    name: src
    type: csv
    path: a.csv
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
    let config = parse_config(yaml).expect("parse");
    config
        .compile(&CompileContext::default())
        .expect("absent watermark compiles clean");
}
