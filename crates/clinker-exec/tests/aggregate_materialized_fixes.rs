//! Per-document Aggregate flush over a fully-materialized input: DLQ
//! attribution and global-fold row semantics.
//!
//! The materialized strict-aggregate path drains its predecessor to a
//! `Vec`, buckets records into per-document group tables, and flushes each
//! closing document at its boundary. These tests cover, end to end through a
//! multi-file CSV glob (each file is a document):
//!
//! - A finalize failure (integer `sum` overflow) in a *later* document must
//!   stamp the resulting DLQ entry with a record from THAT document, not the
//!   batch's first document. Before the fix every finalize DLQ entry was
//!   attributed to the first record of the whole batch.
//! - A global fold (no `group_by`) owes exactly one row per closing document
//!   and one row for a whole-empty input — never a phantom extra row; a
//!   grouped fold owes nothing for an empty input. These guard the
//!   `flushed` de-duplication and the whole-empty synthetic-sentinel gate.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

/// Run `yaml` over a set of in-memory CSV files, each fed as a distinct
/// `FileSlot` (hence a distinct document) through the `events` source.
/// Returns the sorted output body lines (header stripped) and the run's
/// full [`ExecutionReport`] so a caller can inspect DLQ entries.
fn run_multi_file(yaml: &str, files: &[(&str, &str)]) -> (Vec<String>, ExecutionReport) {
    let config = parse_config(yaml).expect("parse pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile pipeline");

    let slots: Vec<FileSlot> = files
        .iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(*name),
                Box::new(Cursor::new(body.as_bytes().to_vec())),
            )
        })
        .collect();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )]);

    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn std::io::Write + Send>,
    )]);

    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };

    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("run pipeline");

    let output = buf.as_string();
    let mut body: Vec<String> = output.lines().skip(1).map(|s| s.to_string()).collect();
    body.sort();
    (body, report)
}

/// Group-by `sum(amount)` over a multi-file glob, with `continue` error
/// handling so a finalize-time integer overflow routes to the DLQ rather
/// than aborting the run.
const SUM_BY_CATEGORY_YAML: &str = r#"
pipeline:
  name: sum_by_category
error_handling:
  strategy: continue
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: category, type: string }
        - { name: amount, type: int }
  - type: aggregate
    name: by_category
    input: events
    config:
      group_by:
        - category
      cxl: |
        emit category = category
        emit total = sum(amount)
  - type: output
    name: out
    input: by_category
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

#[test]
fn finalize_failure_attributes_dlq_to_the_failing_document() {
    // Two documents. Document A (a.csv) sums cleanly; document B (b.csv)
    // sums two `i64::MAX` values whose i128 accumulator overflows `i64` at
    // finalize, so group "b" finalize fails. Under `continue` that failure
    // routes to the DLQ. The entry must be attributed to a record from
    // document B (the document that actually failed) — before the fix it was
    // stamped with document A's first record, since the whole batch's first
    // record was used regardless of which document failed.
    let i64_max = i64::MAX.to_string();
    let doc_a = "category,amount\na,1\na,2\n".to_string();
    let doc_b = format!("category,amount\nb,{i64_max}\nb,{i64_max}\n");

    let (body, report) = run_multi_file(
        SUM_BY_CATEGORY_YAML,
        &[("a.csv", doc_a.as_str()), ("b.csv", doc_b.as_str())],
    );

    // Document A still emits its clean group row; document B's overflow
    // landed in the DLQ instead of an output row.
    assert_eq!(body, vec!["a,3".to_string()], "document A sums cleanly");
    assert_eq!(report.dlq_entries.len(), 1, "one finalize DLQ entry");

    let entry = &report.dlq_entries[0];
    assert_eq!(
        entry.category,
        clinker_core_types::dlq::DlqErrorCategory::AggregateFinalize,
        "finalize-time overflow is an aggregate-finalize DLQ entry",
    );
    assert!(
        entry.error_message.contains("SumOverflow"),
        "the failure is an integer sum overflow, got: {}",
        entry.error_message,
    );
    // The decisive attribution check: the entry's original record belongs to
    // the FAILING document (B, category "b"), not the batch's first document
    // (A, category "a"). Before the fix this was always "a".
    assert_eq!(
        entry.original_record.get("category"),
        Some(&clinker_record::Value::from("b")),
        "DLQ entry must be attributed to the failing document's record, \
         not the batch's first document",
    );
}

/// Global fold (no `group_by`) directly over a multi-file glob: `count(*)`.
/// A direct Source → Aggregate edge keeps the materialized per-document flush
/// path (a bare Source is not a certified streaming producer).
const COUNT_GLOBAL_YAML: &str = r#"
pipeline:
  name: count_global
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: category, type: string }
  - type: aggregate
    name: total
    input: events
    config:
      group_by: []
      cxl: |
        emit n = count(*)
  - type: output
    name: out
    input: total
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

/// Grouped fold directly over the same multi-file glob: `count(*)` per
/// category.
const COUNT_GROUPED_YAML: &str = r#"
pipeline:
  name: count_grouped
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      files:
        on_no_match: skip
      schema:
        - { name: category, type: string }
  - type: aggregate
    name: by_category
    input: events
    config:
      group_by:
        - category
      cxl: |
        emit category = category
        emit n = count(*)
  - type: output
    name: out
    input: by_category
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;

#[test]
fn whole_empty_input_emits_global_fold_default_row() {
    // A single header-only file produces zero records and no closing
    // document, so the materialized aggregate drains an empty input. A global
    // fold still owes its single defaulted row, which the synthetic-sentinel
    // fallback emits — `count(*) == 0`. This guards the `flushed.is_empty()`
    // gate added alongside the per-document empty-document fix: the whole-empty
    // path must still produce exactly one global-fold row.
    let (body, report) = run_multi_file(COUNT_GLOBAL_YAML, &[("empty.csv", "category\n")]);
    assert_eq!(report.dlq_entries.len(), 0, "no DLQ entries expected");
    assert_eq!(
        body,
        vec!["0".to_string()],
        "a whole-empty global fold emits its single defaulted row",
    );
}

#[test]
fn whole_empty_input_emits_nothing_for_grouped_fold() {
    // The control for the global-fold case: a whole-empty grouped fold owns
    // no group, so it emits nothing — the synthetic sentinel must not
    // synthesize a phantom grouped row.
    let (body, report) = run_multi_file(COUNT_GROUPED_YAML, &[("empty.csv", "category\n")]);
    assert_eq!(report.dlq_entries.len(), 0, "no DLQ entries expected");
    assert!(
        body.is_empty(),
        "a whole-empty grouped fold emits nothing, got: {body:?}",
    );
}

#[test]
fn multi_document_global_fold_emits_one_row_per_document() {
    // Each file is a closing document; a global fold flushes each document at
    // its boundary in the punct-loop and owes exactly one row per document.
    // This guards the `flushed` de-duplication and the punct-loop global-fold
    // flush against emitting a phantom extra row for a document that
    // contributed records.
    let (body, report) = run_multi_file(
        COUNT_GLOBAL_YAML,
        &[
            ("a.csv", "category\nx\ny\n"),
            ("b.csv", "category\nz\n"),
            ("c.csv", "category\np\nq\nr\n"),
        ],
    );
    assert_eq!(report.dlq_entries.len(), 0, "no DLQ entries expected");
    assert_eq!(
        body,
        vec!["1".to_string(), "2".to_string(), "3".to_string()],
        "three documents each flush exactly one global-fold row (counts \
         2, 1, 3), with no phantom extra row",
    );
}
