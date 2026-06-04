//! End-to-end coverage for the per-source rollback rewind on a
//! correlation group that overflows its `max_group_buffer`.
//!
//! A group that exceeds `error_handling.max_group_buffer` is DLQ'd at
//! `CorrelationCommit`: one `group_size_exceeded` root-cause trigger plus
//! a `correlated` collateral for every other buffered row of the group.
//! Independently of the DLQ shape, every Source that contributed a slot
//! to the overflowing group rewinds its `rollback_cursors` entry to the
//! lowest `row_num` it contributed — narrowing the replay anchor so a
//! downstream resume reprocesses every record that fed the group,
//! including ones whose forward Transform clean-exit had already advanced
//! the cursor past them.
//!
//! These tests pin the two-source case: with `[src_a, src_b]` both keyed
//! on `id` and merged into one buffered Output, an overflow on the shared
//! group rewinds BOTH sources' cursors to their respective per-source
//! minima, not to a single pipeline-wide floor and not to the synthetic
//! merged source.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_core::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

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

/// Two sources share the `id=1` correlation group. With
/// `max_group_buffer: 3` the group of four buffered rows (two from each
/// source) overflows. After the overflow DLQ disposition, each source's
/// rollback cursor rewinds to the lowest `row_num` it contributed to the
/// group — `1` for both — even though their forward Transform clean-exit
/// had already advanced each cursor to its highest row (`2`). The rewind
/// is per-source: `src_a` and `src_b` each land at their own minimum, and
/// neither attributes to the synthetic merged source.
#[test]
fn overflow_rewinds_both_sources_to_per_source_min() {
    let yaml = r#"
pipeline:
  name: overflow_two_source_rewind
error_handling:
  strategy: continue
  max_group_buffer: 3
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: merge
    name: m
    inputs: [src_a, src_b]
  - type: transform
    name: tfm
    input: m
    config:
      cxl: |
        emit id = id
        emit amt = amt
  - type: output
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    // All four rows share id=1, so they land in one correlation group.
    // The group's four buffered records exceed max_group_buffer=3, so the
    // commit arm flips the overflow disposition.
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("a", "id,amt\n1,10\n1,11\n")]),
        ),
        (
            "src_b".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("b", "id,amt\n1,20\n1,21\n")]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    // Overflow disposition: exactly one root-cause GroupSizeExceeded
    // trigger; every other buffered row is a `correlated` collateral.
    let trigger_count = report
        .dlq_entries
        .iter()
        .filter(|e| e.category == clinker_core_types::dlq::DlqErrorCategory::GroupSizeExceeded)
        .count();
    assert_eq!(
        trigger_count,
        1,
        "one GroupSizeExceeded root-cause trigger: {:?}",
        report
            .dlq_entries
            .iter()
            .map(|e| (e.source_name.as_ref(), e.category.as_str(), e.trigger))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        report.dlq_entries.len(),
        4,
        "every buffered row of the overflowing group is DLQ'd (1 trigger + 3 collateral)"
    );

    // The trigger and collaterals are attributed to the real contributing
    // sources, never to the synthetic merged source.
    let dlq_by_source: HashMap<&str, usize> =
        report
            .dlq_entries
            .iter()
            .fold(HashMap::new(), |mut acc, e| {
                *acc.entry(e.source_name.as_ref()).or_insert(0) += 1;
                acc
            });
    assert_eq!(
        dlq_by_source.get("<merged>").copied().unwrap_or(0),
        0,
        "no entry falls back to the synthetic merged source"
    );
    assert_eq!(
        dlq_by_source.get("src_a").copied().unwrap_or(0),
        2,
        "both src_a rows are DLQ'd by the overflow"
    );
    assert_eq!(
        dlq_by_source.get("src_b").copied().unwrap_or(0),
        2,
        "both src_b rows are DLQ'd by the overflow"
    );

    // Both contributing sources rewind to the lowest row_num they
    // contributed to the overflowing group (1 each), undoing the
    // Transform clean-exit advance that had moved each cursor to 2.
    assert_eq!(
        report.per_source_rollback_cursors.get("src_a"),
        Some(&1),
        "src_a rewinds to its per-source minimum row in the group"
    );
    assert_eq!(
        report.per_source_rollback_cursors.get("src_b"),
        Some(&1),
        "src_b rewinds to its per-source minimum row in the group"
    );

    // The overflowing group is fully DLQ'd, so nothing reaches the writer.
    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert!(
        body.is_empty(),
        "an overflowing group flushes no records to the output: {output}"
    );
}

/// The per-source rewind takes each source's OWN minimum independently:
/// when the two sources contribute different lowest rows to the same
/// overflowing group, each cursor lands on its own floor rather than on a
/// shared pipeline-wide minimum. Here `src_a`'s first contributing row to
/// group `id=7` is its row 1, while `src_b`'s first contributing row to
/// that group is its row 2 (its row 1 belongs to the non-overflowing
/// group `id=9`). After the overflow rewind, `src_a` lands at 1 and
/// `src_b` at 2 — distinct per-source floors.
#[test]
fn overflow_rewind_is_per_source_not_pipeline_wide() {
    let yaml = r#"
pipeline:
  name: overflow_distinct_per_source_floor
error_handling:
  strategy: continue
  max_group_buffer: 3
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: merge
    name: m
    inputs: [src_a, src_b]
  - type: transform
    name: tfm
    input: m
    config:
      cxl: |
        emit id = id
        emit amt = amt
  - type: output
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    // Group id=7 overflows (max_group_buffer=3, four members: src_a rows
    // 1+2, src_b rows 2+3). src_b's row 1 belongs to the separate,
    // non-overflowing group id=9 and so does not pull src_b's rewind
    // floor below 2.
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("a", "id,amt\n7,10\n7,11\n")]),
        ),
        (
            "src_b".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot(
                "b",
                "id,amt\n9,90\n7,20\n7,21\n",
            )]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    // Only group id=7 overflows; group id=9 (a single src_b row) is clean.
    let trigger_count = report
        .dlq_entries
        .iter()
        .filter(|e| e.category == clinker_core_types::dlq::DlqErrorCategory::GroupSizeExceeded)
        .count();
    assert_eq!(trigger_count, 1, "only group id=7 overflows");

    // Per-source rewind floors differ: src_a's minimum contributed row to
    // the overflowing group is 1; src_b's is 2 (its row 1 fed the clean
    // group id=9). A pipeline-wide floor would have collapsed both to 1.
    assert_eq!(
        report.per_source_rollback_cursors.get("src_a"),
        Some(&1),
        "src_a rewinds to its own minimum contributing row (1)"
    );
    assert_eq!(
        report.per_source_rollback_cursors.get("src_b"),
        Some(&2),
        "src_b rewinds to its own minimum contributing row (2), not a shared pipeline floor"
    );

    // The clean group id=9 still flushes its single src_b row.
    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert_eq!(
        body.len(),
        1,
        "the non-overflowing group id=9 flushes its one row: {output}"
    );
    assert!(
        body[0].starts_with("9,"),
        "the surviving row belongs to the clean group id=9: {output}"
    );
}
