//! Build-direct Combine rollback coverage.
//!
//! A Source feeding straight into a Combine *build* side never traverses
//! a Transform / Route / Aggregate, so the only point its per-source
//! rollback cursor advances is the Combine arm's operator-entry walk over
//! `build_buf`. That advance is load-bearing the moment a recoverable
//! Combine-output-row failure can rewind: without it the build source's
//! captured pre-fold floor would be a stale seed rather than a meaningful
//! cursor, and the rewind would have nothing scoped to the contributing
//! build records.
//!
//! This test isolates the build-direct case from the both-sides-direct
//! gate test (`per_source_rollback::ac3_combine_output_row_recoverable_dlq_rewinds_both_sources`)
//! by routing the *driver* through an upstream passthrough Transform. The
//! driver's cursor therefore advances through the Transform's clean exit
//! to a non-zero floor, while the build source's only forward advance is
//! the Combine operator-entry walk — so its captured floor is 0. A
//! build-value-dependent matched-body division-by-zero then rewinds the
//! build source to that build-direct floor of 0, proving the
//! operator-entry advance is the cursor the restore writes back, and that
//! the driver's independent Transform-fed floor is untouched by the
//! build-side rewind.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::dlq::DlqErrorCategory;
use clinker_core::executor::{DlqEntry, PipelineExecutor, PipelineRunParams, SourceReaders};
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
        execution_id: "test".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// A recoverable Combine matched-body eval failure whose error depends on
/// a *build-side* column value, where the build side is a direct Source,
/// rewinds the build source to its build-direct pre-fold floor of 0 while
/// leaving the Transform-fed driver source at its own (non-zero) floor.
///
/// Topology: `src_drv -> passthrough -> Combine(d) ; src_bld -> Combine(b)`.
/// The body `emit ratio = d.amt / b.factor` divides by the build-side
/// `factor`; build row `id=2` carries `factor=0`, so the matched-body
/// eval for driver row `id=2` divides by zero. Under
/// `error_handling: strategy: continue` the failing output row routes to
/// the DLQ with category `combine_output_row`, attributed to the
/// contributing driver source (trigger) and the matched build source
/// (non-trigger) — never the synthetic `<merged>` source. The build
/// source rewinds to its captured floor of 0 because its sole forward
/// advance was the Combine operator-entry walk; the driver source, having
/// advanced through the passthrough Transform's clean exit, keeps its own
/// floor independent of the build-side rewind. No `correlation_key` is
/// declared, so the recoverable path takes the direct `push_dlq` branch.
#[test]
fn build_direct_combine_output_row_failure_rewinds_build_source_to_zero() {
    let yaml = r#"
pipeline:
  name: build_direct_combine_rollback
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_drv
    config:
      name: src_drv
      type: csv
      path: drv.csv
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: source
    name: src_bld
    config:
      name: src_bld
      type: csv
      path: bld.csv
      schema:
        - { name: id, type: int }
        - { name: factor, type: int }
  - type: transform
    name: passthrough
    input: src_drv
    config:
      cxl: |
        emit id = id
        emit amt = amt
  - type: combine
    name: enriched
    input:
      d: passthrough
      b: src_bld
    config:
      where: 'd.id == b.id'
      match: first
      on_miss: skip
      cxl: |
        emit id = d.id
        emit ratio = d.amt / b.factor
      propagate_ck: driver
  - type: output
    name: out
    input: enriched
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    // Build row id=2 carries factor=0, so the matched-body
    // `d.amt / b.factor` eval divides by zero for driver row id=2 only.
    let readers: SourceReaders = HashMap::from([
        (
            "src_drv".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("drv", "id,amt\n1,10\n2,20\n")]),
        ),
        (
            "src_bld".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("bld", "id,factor\n1,2\n2,0\n")]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    // The build-value-dependent failure produces a `combine_output_row`
    // DLQ pair: a trigger attributed to the driver source plus a
    // non-trigger entry attributed to the *real* build source. Neither
    // falls back to the synthetic `<merged>` source.
    let combine_dlq: Vec<&DlqEntry> = report
        .dlq_entries
        .iter()
        .filter(|e| e.category == DlqErrorCategory::CombineOutputRow)
        .collect();
    assert_eq!(
        combine_dlq.len(),
        2,
        "one trigger (driver) + one build-side entry for the failing row: {:?}",
        report
            .dlq_entries
            .iter()
            .map(|e| (e.source_name.as_ref(), e.category.as_str(), e.trigger))
            .collect::<Vec<_>>()
    );
    assert!(
        combine_dlq
            .iter()
            .any(|e| e.source_name.as_ref() == "src_drv" && e.trigger),
        "driver row is the attributed trigger on src_drv"
    );
    let build_entry = combine_dlq
        .iter()
        .find(|e| !e.trigger)
        .expect("a non-trigger build-side entry is emitted for the matched build row");
    assert_eq!(
        build_entry.source_name.as_ref(),
        "src_bld",
        "the build-side entry names the real build source, never the synthetic <merged> source"
    );

    // The build source's only forward advance was the Combine
    // operator-entry walk over `build_buf`, so its captured pre-fold
    // floor is 0; the recoverable failure rewinds it back to that floor.
    assert_eq!(
        report.per_source_rollback_cursors.get("src_bld"),
        Some(&0),
        "build-direct source rewinds to its operator-entry-walk pre-fold floor of 0"
    );
    // The driver advanced through the passthrough Transform's clean exit
    // before the fold, so its captured floor is that post-Transform
    // position; the per-source-min rewind never lowers it below the
    // floor, and the build-side rewind does not touch it.
    assert_eq!(
        report.per_source_rollback_cursors.get("src_drv"),
        Some(&2),
        "Transform-fed driver keeps its own non-zero floor, independent of the build-side rewind"
    );

    // The clean id=1 driver row (factor=2) still reaches the writer; the
    // failing id=2 row (factor=0) is dropped from the output.
    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert_eq!(
        body.len(),
        1,
        "only the clean id=1 row reaches the output: {output}"
    );
    assert!(
        body[0].starts_with("1,"),
        "the surviving row is id=1: {output}"
    );
}
