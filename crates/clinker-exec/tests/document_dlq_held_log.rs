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

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

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
const PAD_BYTES: usize = 100;

/// The engine columns that differ between two runs of the same input: each
/// row's id and time, and the trigger id every row of a document carries.
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
    (0..FILES)
        .map(|file| {
            let mut body = String::from("id,value,pad\n");
            for row in 0..ROWS_PER_FILE {
                let pad: String =
                    std::iter::repeat_n(char::from(b'a' + (row % 26) as u8), PAD_BYTES).collect();
                body.push_str(&format!("{},not-a-number-{row},{pad}\n", row_id(file, row)));
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
    let yaml = held_log_yaml(memory_limit);
    let config = parse_config(&yaml).expect("parse held-log pipeline");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile held-log pipeline");
    let slots: Vec<FileSlot> = input_files()
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

#[test]
fn held_failing_rows_stay_in_memory_with_ample_memory() {
    let low = run_held_log("2M");
    let HeldLogRun {
        report,
        rows,
        header,
        body,
    } = run_held_log("100G");
    let held = held_bytes(&rows, &header);

    assert_eq!(
        report.cumulative_spill_bytes, 0,
        "no extent is written when memory suffices; per-stage spill = {:?}",
        report.per_stage_spill_bytes
    );
    assert!(
        report.peak_consumer_usage_bytes >= held,
        "the held rows are charged: peak {} against {held} held bytes",
        report.peak_consumer_usage_bytes
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
