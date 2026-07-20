//! Regression coverage for the `per_source_dlq_counts` sum-<=-aggregate
//! contract when the executor attributes DLQ entries to the synthetic
//! `<merged>` rollup.
//!
//! Post-aggregate synthetic rows carry `source_row: 0` and no engine
//! `SourceName` stamp, so a failure downstream of an Aggregate is attributed
//! to `<merged>` at the DLQ funnel. That slot is filtered out of the
//! report-facing `per_source_dlq_counts`, so its sum is strictly below
//! `counters.dlq_count` — the inequality the report doc promises and the
//! strict `assert_eq!` in `per_source_dlq_counts.rs` cannot express.

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

/// A single source feeds a pre-aggregate Transform (which DLQs one row under
/// the real source), an Aggregate, then a post-aggregate Transform that
/// divides by zero on every group's synthetic output row (each DLQ'd under
/// `<merged>`). The declared source's DLQ count still surfaces and is correct,
/// but the `<merged>`-attributed group failures are excluded — so the surfaced
/// sum is strictly below the aggregate `dlq_count`.
#[test]
fn merged_attributed_dlq_keeps_per_source_sum_below_aggregate() {
    let yaml = r#"
pipeline:
  name: per_source_merged_dlq
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_a
    config:
      name: src_a
      type: csv
      path: a.csv
      schema:
        - { name: category, type: string }
        - { name: amount, type: int }
  - type: transform
    name: pre
    input: src_a
    config:
      cxl: |
        emit category = category
        emit amount = if(amount < 0) then (1 / 0) else amount
  - type: aggregate
    name: agg
    input: pre
    config:
      group_by:
        - category
      cxl: |
        emit category = category
        emit total = sum(amount)
  - type: transform
    name: post
    input: agg
    config:
      cxl: |
        emit category = category
        emit ratio = total / (total - total)
  - type: output
    name: out
    input: post
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    // Rows: category x (10, 5) and y (20) aggregate into two clean groups whose
    // post-aggregate `total / (total - total)` divides by zero → two `<merged>`
    // DLQ entries. The z row is negative, so the pre-aggregate Transform DLQs it
    // under the real source src_a and it never reaches the aggregate.
    let readers: SourceReaders = HashMap::from([(
        "src_a".to_string(),
        clinker_exec::executor::SourceInput::Files(vec![slot(
            "a",
            "category,amount\nx,10\nx,5\ny,20\nz,-1\n",
        )]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    // Three DLQ entries total: one real-source (the z row at the pre-aggregate
    // Transform) plus one per clean group (x, y) at the post-aggregate divide.
    assert_eq!(
        report.counters.dlq_count, 3,
        "one pre-aggregate real-source failure plus two post-aggregate group \
         failures"
    );

    // The synthetic rollup key never leaks into the surfaced map.
    assert!(
        !report.per_source_dlq_counts.contains_key("<merged>"),
        "the '<merged>' rollup slot is filtered out of the per-source map: {:?}",
        report.per_source_dlq_counts
    );

    // The one declared-source failure still surfaces, attributed correctly.
    assert_eq!(
        report.per_source_dlq_counts.get("src_a"),
        Some(&1),
        "the pre-aggregate failure is attributed to its real source src_a"
    );
    assert_eq!(
        report.per_source_dlq_counts.len(),
        1,
        "only the declared source surfaces; the two '<merged>' group failures \
         are excluded"
    );

    // The contract: the surfaced sum is strictly below the aggregate because
    // the two post-aggregate failures are attributed to '<merged>'.
    let attributed: u64 = report.per_source_dlq_counts.values().sum();
    assert!(
        attributed < report.counters.dlq_count,
        "the surfaced per-source sum ({attributed}) is strictly below the \
         aggregate dlq_count ({}) — the '<merged>'-attributed entries are \
         excluded",
        report.counters.dlq_count
    );
    assert_eq!(
        attributed, 1,
        "only the single declared-source failure is attributed in the map"
    );

    // Cross-check against the raw DLQ entries: exactly two entries carry the
    // synthetic '<merged>' source name, confirming the gap is the merged
    // rollup and not a dropped entry.
    let merged_entries = report
        .dlq_entries
        .iter()
        .filter(|e| e.source_name.as_ref() == "<merged>")
        .count();
    assert_eq!(
        merged_entries, 2,
        "the two post-aggregate group failures are stamped '<merged>'"
    );
}
