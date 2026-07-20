//! End-to-end coverage for `ExecutionReport.per_source_record_counts`.
//!
//! The Source dispatch arm and the fused `Merge.interleave` arm both
//! finalize each source's count via `finalize_source_count` when the
//! upstream `mpsc::Receiver` returns `None`. These tests pin both
//! paths so a regression in either finalize call drops a source from
//! the surfaced map.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_exec::source::multi_file::FileSlot;
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

/// Default `concat`-mode Merge: each Source's records drain through
/// the non-fused Source dispatch arm. Per-source ingest counts must
/// reflect each source's row count independently of the aggregate
/// `counters.total_count`.
#[test]
fn concat_merge_per_source_counts() {
    let yaml = r#"
pipeline:
  name: per_source_counts_concat
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
  - type: merge
    name: m
    inputs: [src_a, src_b]
  - type: output
    name: out
    input: m
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot("a", "id\n1\n2\n3\n4\n5\n")]),
        ),
        (
            "src_b".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot("b", "id\n10\n20\n")]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    assert_eq!(
        report.per_source_record_counts.get("src_a"),
        Some(&5),
        "src_a ingest count surfaces with its 5 rows"
    );
    assert_eq!(
        report.per_source_record_counts.get("src_b"),
        Some(&2),
        "src_b ingest count surfaces with its 2 rows"
    );
    assert_eq!(
        report.per_source_record_counts.len(),
        2,
        "only declared sources surface — no synthetic '<merged>' key leaks"
    );
    let sum: u64 = report.per_source_record_counts.values().sum();
    assert_eq!(
        sum, report.counters.total_count,
        "per-source counts sum to the aggregate total"
    );
}

/// Interleave-mode Merge with two Source predecessors and no seed:
/// the executor's pre-pass fuses the Merge with both Sources, draining
/// every record through the fused `Merge.interleave` arm. Each source's
/// finalize stamp must still land on `source_count_per_source` so both
/// names surface in the report's per-source map.
#[test]
fn fused_interleave_merge_per_source_counts() {
    let yaml = r#"
pipeline:
  name: per_source_counts_fused_interleave
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
  - type: merge
    name: m
    inputs: [src_a, src_b]
    config:
      mode: interleave
  - type: output
    name: out
    input: m
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot(
                "a",
                "id\n1\n2\n3\n4\n5\n6\n7\n",
            )]),
        ),
        (
            "src_b".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot("b", "id\n10\n20\n30\n")]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    assert_eq!(
        report.per_source_record_counts.get("src_a"),
        Some(&7),
        "fused-Merge arm finalized src_a's slot"
    );
    assert_eq!(
        report.per_source_record_counts.get("src_b"),
        Some(&3),
        "fused-Merge arm finalized src_b's slot"
    );
    assert_eq!(
        report.per_source_record_counts.len(),
        2,
        "no synthetic '<merged>' key leaks through the report layer"
    );
    let sum: u64 = report.per_source_record_counts.values().sum();
    assert_eq!(
        sum, report.counters.total_count,
        "per-source counts under fused Merge.interleave sum to the aggregate total"
    );
}

/// Single-source `Source -> Output` with no Merge: the plain Source dispatch
/// arm finalizes the sole source's count. A regression in the `<merged>`
/// filter that mis-applied to a single-source pipeline would drop the source
/// from the surfaced map; a lone declared source must still surface with its
/// exact row count and no synthetic key alongside it.
#[test]
fn single_source_per_source_count_surfaces() {
    let yaml = r#"
pipeline:
  name: per_source_counts_single
nodes:
  - type: source
    name: only
    config:
      name: only
      type: csv
      path: only.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: only
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let readers: SourceReaders = HashMap::from([(
        "only".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![slot("only", "id\n1\n2\n3\n")]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    assert_eq!(
        report.per_source_record_counts.get("only"),
        Some(&3),
        "the sole source's ingest count surfaces"
    );
    assert_eq!(
        report.per_source_record_counts.len(),
        1,
        "no synthetic '<merged>' key leaks in the single-source case"
    );
    let sum: u64 = report.per_source_record_counts.values().sum();
    assert_eq!(sum, report.counters.total_count);
}

/// A declared source whose input is empty (header only, zero data rows)
/// finalizes with a count of zero. The record-count map keeps that `Some(0)`
/// entry — "stream closed with zero records" is a distinct, reported state —
/// while the DLQ map (which drops zero-entry sources) omits it.
#[test]
fn zero_row_source_surfaces_zero_in_record_map() {
    let yaml = r#"
pipeline:
  name: per_source_counts_zero_row
nodes:
  - type: source
    name: empty_src
    config:
      name: empty_src
      type: csv
      path: empty.csv
      schema:
        - { name: id, type: int }
  - type: output
    name: out
    input: empty_src
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let readers: SourceReaders = HashMap::from([(
        "empty_src".to_string(),
        // Header only, no data rows.
        clinker_exec::executor::SourceInput::Files(vec![slot("empty", "id\n")]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    assert_eq!(
        report.per_source_record_counts.get("empty_src"),
        Some(&0),
        "an empty source finalizes and surfaces Some(0) — the record map does \
         not drop zero counts"
    );
    assert_eq!(report.counters.total_count, 0);
    assert!(
        !report.per_source_dlq_counts.contains_key("empty_src"),
        "the DLQ map drops zero-entry sources, so an empty source is absent \
         there"
    );
}

/// `Source -> Transform -> Output` (single source): the executor pre-pass fuses
/// the Transform directly onto the Source, draining the ingest receiver inside
/// the Transform arm, which finalizes the source's count from that arm rather
/// than the plain Source arm. The count must still land on the surfaced map.
#[test]
fn fused_transform_per_source_counts_finalize() {
    let yaml = r#"
pipeline:
  name: per_source_counts_fused_transform
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      schema:
        - { name: id, type: int }
  - type: transform
    name: shape
    input: events
    config:
      cxl: |
        emit id = id
  - type: output
    name: out
    input: shape
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let readers: SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![slot("events", "id\n1\n2\n3\n4\n5\n")]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    assert_eq!(
        report.per_source_record_counts.get("events"),
        Some(&5),
        "the fused Source->Transform arm finalized the source's ingest count"
    );
    assert_eq!(
        report.per_source_record_counts.len(),
        1,
        "no synthetic '<merged>' key leaks through the fused-Transform path"
    );
    let sum: u64 = report.per_source_record_counts.values().sum();
    assert_eq!(sum, report.counters.total_count);
}
