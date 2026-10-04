//! The failing records of a failed document under `dlq_granularity:
//! document`, held until the document is rejected.
//!
//! The shape is the job the bounded dead-letter work exists for: a column
//! the Transform cannot coerce in any row, so every record of every document
//! fails, and every failing record is held until the Sink phase rejects its
//! document. The held rows are charged to the memory budget, leave memory
//! only when the budget needs it, and reach the dead-letter output in the
//! order a rejection writes them: the trigger, then the document's other
//! failing records in failure order.
//!
//! The ample-memory tests read the dead-letter state's own charged peak
//! through a `test-utils` seam, because the report has no per-node figure
//! for run-scoped state; they run only with that feature.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{ExecutionReport, PipelineExecutor, PipelineRunParams};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use dlq_sink::{CollectingDlqSink, DlqRow};

/// Documents (files) in the run.
const FILES: usize = 30;
/// Records per document.
const ROWS_PER_FILE: usize = 400;
/// Width of each record's `pad` cell, so the held rows outweigh a small
/// budget several times over.
///
/// The rows are 100 bytes wide rather than 300 because a Source cannot yet
/// wait for memory: at 300 bytes, under the small budget, a Source's record
/// admission is refused and the run fails before the held rows are what is
/// measured. The change that lets a Source wait for memory (#1247, #1250)
/// restores 300-byte rows.
const PAD_BYTES: usize = 100;

/// The engine columns that differ between two runs of the same input: each
/// row's id and time, and the trigger id every row of a document carries.
#[cfg(feature = "test-utils")]
const RUN_VARIANT_COLUMNS: [&str; 3] = ["_cxl_dlq_id", "_cxl_dlq_trigger_id", "_cxl_dlq_timestamp"];

/// The pipeline: `validate` coerces `value`, which is non-numeric in every
/// row, so every record fails there and is held until its document is
/// rejected at `out`.
fn held_log_yaml(memory_limit: &str) -> String {
    format!(
        r#"
pipeline:
  name: doc_dlq_held_log
  memory: {{ limit: "{memory_limit}", backpressure: spill }}
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      glob: ./*.csv
      dlq_granularity: document
      files:
        on_no_match: skip
      schema:
        - {{ name: id, type: string }}
        - {{ name: value, type: string }}
        - {{ name: pad, type: string }}
  - type: transform
    name: validate
    input: events
    config:
      cxl: |
        emit id = id
        emit val = value.to_int()
        emit pad = pad
  - type: sink
    name: out
    input: validate
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#
    )
}

/// The id of row `row` (zero-based) of file `file`.
fn row_id(file: usize, row: usize) -> String {
    format!("d{file:02}-r{row:03}")
}

/// The input files, in order: every row's `value` is non-numeric.
fn input_files() -> Vec<(String, String)> {
    input_files_with(|row| format!("not-a-number-{row}"))
}

/// The same files with a numeric `value` in every row, so no record fails
/// and the document dead-letter state holds nothing.
#[cfg(feature = "test-utils")]
fn passing_input_files() -> Vec<(String, String)> {
    input_files_with(|row| row.to_string())
}

/// The input files, in order, with `value(row)` in each row's `value` cell.
fn input_files_with(value: impl Fn(usize) -> String) -> Vec<(String, String)> {
    (0..FILES)
        .map(|file| {
            let mut body = String::from("id,value,pad\n");
            for row in 0..ROWS_PER_FILE {
                let pad: String =
                    std::iter::repeat_n(char::from(b'a' + (row % 26) as u8), PAD_BYTES).collect();
                body.push_str(&format!("{},{},{pad}\n", row_id(file, row), value(row)));
            }
            (format!("d{file:02}.csv"), body)
        })
        .collect()
}

/// One run of the pipeline under `memory_limit`.
struct HeldLogRun {
    report: ExecutionReport,
    /// The dead-letter rows in written order.
    rows: Vec<DlqRow>,
    /// The header the rows were written under.
    header: Vec<String>,
    /// The Sink's body lines.
    body: Vec<String>,
}

fn run_held_log(memory_limit: &str) -> HeldLogRun {
    let (report, sink, body) = run_files(memory_limit, input_files());
    let rows = sink.rows();
    let header = rows
        .first()
        .and_then(|row| sink.header_for(row.bucket_path()))
        .expect("the run dead-letters rows");
    HeldLogRun {
        report,
        rows,
        header,
        body,
    }
}

/// One run of the pipeline over `files` under `memory_limit`: the report,
/// the dead-letter sink, and the Sink's body lines.
fn run_files(
    memory_limit: &str,
    files: Vec<(String, String)>,
) -> (ExecutionReport, Arc<CollectingDlqSink>, Vec<String>) {
    let yaml = held_log_yaml(memory_limit);
    let config = parse_config(&yaml).expect("parse held-log pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile held-log pipeline");
    let slots: Vec<FileSlot> = files
        .into_iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(name),
                Box::new(Cursor::new(body.into_bytes())),
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
    let sink = CollectingDlqSink::new();
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        dlq_sink::registry(writers, &sink),
        &params,
    )
    .expect("run held-log pipeline");
    let output = buf.as_string();
    let body: Vec<String> = output.lines().skip(1).map(str::to_string).collect();
    (report, sink, body)
}

/// A lower bound on the bytes the dead-letter rows occupy in their file
/// without its header: every cell, a separator between cells and a line
/// terminator per row. Quoting only adds bytes, so the file is at least
/// this large.
fn held_bytes(rows: &[DlqRow], header: &[String]) -> u64 {
    rows.iter()
        .map(|row| {
            header
                .iter()
                .map(|column| row.field(column).map_or(0, str::len) as u64 + 1)
                .sum::<u64>()
        })
        .sum()
}

/// Every row as its cells in header order, with the run-variant columns
/// masked.
#[cfg(feature = "test-utils")]
fn masked(rows: &[DlqRow], header: &[String]) -> Vec<Vec<String>> {
    rows.iter()
        .map(|row| {
            header
                .iter()
                .map(|column| {
                    if RUN_VARIANT_COLUMNS.contains(&column.as_str()) {
                        "<masked>".to_string()
                    } else {
                        row.field(column).unwrap_or_default().to_string()
                    }
                })
                .collect()
        })
        .collect()
}

/// The dead-letter rows are, per file in file order, the trigger (the
/// file's first row, under its own category) and then every other row of
/// the file as `document_rejected`, in row order; every row of a file
/// carries its trigger's id.
fn assert_rejection_order(rows: &[DlqRow]) {
    let ids: Vec<&str> = rows
        .iter()
        .map(|row| {
            row.field("id")
                .expect("the rows carry the source's id column")
        })
        .collect();
    let expected: Vec<String> = (0..FILES)
        .flat_map(|file| (0..ROWS_PER_FILE).map(move |row| row_id(file, row)))
        .collect();
    assert_eq!(ids.len(), expected.len(), "every record dead-letters once");
    assert!(
        ids.iter()
            .zip(&expected)
            .all(|(id, want)| *id == want.as_str()),
        "rows are written per file in file order, trigger first, then row order"
    );
    for (file, document) in rows.chunks(ROWS_PER_FILE).enumerate() {
        let trigger = &document[0];
        assert!(trigger.trigger(), "file {file}'s first row is its trigger");
        assert_ne!(
            trigger.category(),
            Some("document_rejected"),
            "the trigger keeps its own category"
        );
        let trigger_id = trigger
            .field("_cxl_dlq_id")
            .expect("every dead-letter header carries _cxl_dlq_id");
        assert_eq!(trigger.field("_cxl_dlq_trigger_id"), Some(trigger_id));
        for collateral in &document[1..] {
            assert!(!collateral.trigger());
            assert_eq!(collateral.category(), Some("document_rejected"));
            assert_eq!(
                collateral.field("_cxl_dlq_trigger_id"),
                Some(trigger_id),
                "every other failing row of file {file} carries its trigger's id"
            );
        }
    }
}

#[test]
fn held_failing_rows_spill_under_a_low_limit_and_keep_their_order() {
    let HeldLogRun {
        report,
        rows,
        header,
        body,
    } = run_held_log("2M");
    let held = held_bytes(&rows, &header);

    assert!(
        report
            .per_stage_spill_bytes
            .get("validate")
            .is_some_and(|&b| b > 0),
        "the held rows leave memory, attributed to the failing Transform; per-stage spill = {:?}",
        report.per_stage_spill_bytes
    );
    assert!(
        report.peak_consumer_usage_bytes < held / 2,
        "the charged peak {} stays below half the {held} held bytes",
        report.peak_consumer_usage_bytes
    );
    assert_rejection_order(&rows);
    assert_eq!(report.counters.dlq_count as usize, FILES * ROWS_PER_FILE);
    assert_eq!(report.counters.ok_count, 0);
    assert!(body.is_empty(), "no record reaches the Sink: {body:?}");
}

#[cfg(feature = "test-utils")]
#[test]
fn held_failing_rows_stay_in_memory_with_ample_memory() {
    let low = run_held_log("2M");
    let _ = clinker_exec::executor::take_document_dlq_peak_charged_bytes_for_testing();
    let HeldLogRun {
        report,
        rows,
        header,
        body,
    } = run_held_log("100G");
    let charge = held_rows_charge();
    let held = held_bytes(&rows, &header);

    assert_eq!(
        report.cumulative_spill_bytes, 0,
        "no extent is written when memory suffices; per-stage spill = {:?}",
        report.per_stage_spill_bytes
    );
    assert!(
        charge >= held,
        "the held rows are charged: the dead-letter state's peak {charge} against {held} held bytes"
    );
    assert!(
        charge > 2 * 1024 * 1024,
        "the held rows' charge {charge} exceeds the low run's 2 MiB limit"
    );
    assert_rejection_order(&rows);
    assert_eq!(report.counters.dlq_count, low.report.counters.dlq_count);
    assert!(body.is_empty(), "no record reaches the Sink: {body:?}");
    assert_eq!(
        masked(&rows, &header),
        masked(&low.rows, &low.header),
        "the rows do not depend on whether they spilled"
    );
}

/// The charged peak the ample-memory check compares with the held rows'
/// bytes: the document dead-letter state's own mark in the run that last
/// finished on this thread. The state is run-scoped, so the report has no
/// per-node figure for it, and the run-wide peak mixes in every other
/// node's charge.
#[cfg(feature = "test-utils")]
fn held_rows_charge() -> u64 {
    clinker_exec::executor::take_document_dlq_peak_charged_bytes_for_testing()
        .expect("the run used dlq_granularity: document")
}

/// The ample-memory check must read the document dead-letter state's own
/// charge. With every record passing, the state holds nothing, while the
/// Transform's buffer and the Sink's per-document buckets still hold every
/// row: a run-wide figure covers the failing run's held bytes anyway, so a
/// check on it would pass however little the held rows were charged.
#[cfg(feature = "test-utils")]
#[test]
fn the_held_rows_charge_is_the_dead_letter_states_own() {
    let failing = run_held_log("100G");
    let held = held_bytes(&failing.rows, &failing.header);
    let _ = clinker_exec::executor::take_document_dlq_peak_charged_bytes_for_testing();

    let (report, sink, body) = run_files("100G", passing_input_files());
    let charge = held_rows_charge();
    assert!(sink.rows().is_empty(), "no record fails");
    assert_eq!(
        body.len(),
        FILES * ROWS_PER_FILE,
        "every record reaches the Sink"
    );
    assert!(
        report.peak_consumer_usage_bytes >= held,
        "the run-wide peak {} covers the failing run's {held} held bytes with nothing held",
        report.peak_consumer_usage_bytes
    );
    assert!(
        charge < held,
        "with nothing held, the held rows' charge {charge} stays below the failing run's {held} held bytes; per-node peaks = {:?}",
        report.per_node_peak_charged_bytes
    );
}

/// `held_log_yaml(memory_limit)` with the pipeline-wide dead-letter rate
/// ceiling `max_rate`, checked once `min_records` source rows are read.
#[cfg(feature = "test-utils")]
fn held_log_yaml_with_rate(memory_limit: &str, max_rate: &str, min_records: u64) -> String {
    let yaml = held_log_yaml(memory_limit);
    let dlq = "  dlq:\n    path: rejected.csv\n";
    assert!(
        yaml.contains(dlq),
        "the pipeline declares its dead-letter file"
    );
    yaml.replacen(
        dlq,
        &format!("{dlq}    max_rate: {max_rate}\n    min_records: {min_records}\n"),
        1,
    )
}

/// One run of `yaml` over `files`, returning the run's result rather than
/// requiring it to succeed.
#[cfg(feature = "test-utils")]
fn try_run_yaml(
    yaml: &str,
    files: Vec<(String, String)>,
) -> Result<ExecutionReport, clinker_plan::error::PipelineError> {
    let config = parse_config(yaml).expect("parse held-log pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile held-log pipeline");
    let slots: Vec<FileSlot> = files
        .into_iter()
        .map(|(name, body)| {
            FileSlot::new(
                PathBuf::from(name),
                Box::new(Cursor::new(body.into_bytes())),
            )
        })
        .collect();
    let readers: clinker_exec::executor::SourceReaders = HashMap::from([(
        "events".to_string(),
        clinker_exec::executor::SourceInput::Files(slots),
    )]);
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    };
    let sink = CollectingDlqSink::new();
    PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        dlq_sink::registry(writers, &sink),
        &params,
    )
}

/// What the document dead-letter state of the last run on this thread saw
/// as it dropped. The held log's one file is inside the run's spill
/// directory, so the state must drop, closing it, before the directory's
/// guard removes the directory: an open file can block that removal on
/// Windows. Linux removes the directory anyway, so the tests read the order.
#[cfg(feature = "test-utils")]
fn teardown() -> clinker_exec::executor::DocumentDlqTeardown {
    clinker_exec::executor::take_document_dlq_teardown_for_testing()
        .expect("the run used dlq_granularity: document")
}

#[cfg(feature = "test-utils")]
#[test]
fn the_held_log_closes_before_the_spill_directory_is_removed_when_the_run_succeeds() {
    let _ = clinker_exec::executor::take_document_dlq_teardown_for_testing();
    let run = run_held_log("2M");
    assert!(
        run.report
            .per_stage_spill_bytes
            .get("validate")
            .is_some_and(|&b| b > 0),
        "the held rows flush to the held log's file; per-stage spill = {:?}",
        run.report.per_stage_spill_bytes
    );
    let teardown = teardown();
    assert!(
        teardown.held_file_created,
        "the held log created its file: {teardown:?}"
    );
    assert!(
        teardown.spill_dir_present,
        "the spill directory outlives the held log's file: {teardown:?}"
    );
}

#[cfg(feature = "test-utils")]
#[test]
fn the_held_log_closes_before_the_spill_directory_is_removed_when_the_run_fails() {
    let _ = clinker_exec::executor::take_document_dlq_teardown_for_testing();
    // Every row fails, so the first rejection's rows alone pass the ceiling.
    let yaml = held_log_yaml_with_rate("2M", "0.01", 100);
    let error = try_run_yaml(&yaml, input_files()).expect_err("the dead-letter rate stops the run");
    match error {
        clinker_plan::error::PipelineError::DlqRateExceeded { source: None, .. } => {}
        other => panic!("expected the pipeline-wide rate stop (E315), got {other:?}"),
    }
    let teardown = teardown();
    assert!(
        teardown.held_file_created,
        "the held log created its file before the run stopped: {teardown:?}"
    );
    assert!(
        teardown.spill_dir_present,
        "the spill directory outlives the held log's file on the error path: {teardown:?}"
    );
}
