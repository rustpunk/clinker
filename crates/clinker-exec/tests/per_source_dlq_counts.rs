//! End-to-end coverage for `ExecutionReport.per_source_dlq_counts`.
//!
//! The `push_dlq` funnel increments `ctx.dlq_per_source[entry.source_name]`
//! alongside the aggregate `counters.dlq_count`. The report layer
//! surfaces that per-source view, omitting sources with zero DLQ
//! entries to match the `per_source_rollback_cursors` precedent.

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

/// Two sources merged through a transform that forces a division-by-
/// zero only for records originating from `src_bad`. Every `src_bad`
/// record DLQs; every `src_good` record clears. The per-source DLQ
/// map must surface `src_bad` with the failing count and omit
/// `src_good` entirely (zero-entry sources are absent).
#[test]
fn per_source_dlq_counts_attribute_failures_to_offending_source() {
    let yaml = r#"
pipeline:
  name: per_source_dlq_counts
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_good
    config:
      name: src_good
      type: csv
      path: g.csv
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: source
    name: src_bad
    config:
      name: src_bad
      type: csv
      path: b.csv
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: merge
    name: m
    inputs: [src_good, src_bad]
  - type: transform
    name: tfm
    input: m
    config:
      cxl: |
        emit id = id
        emit ratio = if($source.name == "src_bad") then (1 / 0) else amt
  - type: output
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    let readers: SourceReaders = HashMap::from([
        (
            "src_good".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot(
                "g",
                "id,amt\n1,10\n2,20\n3,30\n",
            )]),
        ),
        (
            "src_bad".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot("b", "id,amt\n100,1\n200,2\n")]),
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
        report.per_source_dlq_counts.get("src_bad"),
        Some(&2),
        "both src_bad records DLQ'd from the divide-by-zero"
    );
    assert!(
        !report.per_source_dlq_counts.contains_key("src_good"),
        "src_good has zero DLQ entries — must be absent from the map, \
         not surfaced as Some(0)"
    );
    assert_eq!(
        report.per_source_dlq_counts.len(),
        1,
        "only the offending source surfaces; no synthetic '<merged>' key"
    );

    let attributed: u64 = report.per_source_dlq_counts.values().sum();
    assert_eq!(
        attributed, report.counters.dlq_count,
        "per-source DLQ counts sum to the aggregate dlq_count"
    );
}
