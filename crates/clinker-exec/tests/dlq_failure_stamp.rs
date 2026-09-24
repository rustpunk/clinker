//! A dead-letter row's `_cxl_dlq_id` and `_cxl_dlq_timestamp` describe the
//! moment its failure was observed, not the moment the row was written.
//!
//! Correlation buffering holds each failure until its group commits. The
//! source is sorted by its correlation key, so failures occur in key-value
//! order, while the commit visits groups in the order of their rendered keys.
//! For integer keys the two differ (`9` precedes `10`, but `"[10]"` precedes
//! `"[9]"`), so rows are written in a different order from the one their
//! failures occurred in. UUIDv7 ids increase in the order they are taken,
//! which makes the id order a direct witness of when each stamp was taken.

use std::collections::HashMap;

use clinker_exec::executor::{PipelineRunParams, SourceReaders, single_file_reader};
use clinker_plan::config::parse_config;

#[path = "common/dlq_fixtures.rs"]
mod dlq_fixtures;
#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use dlq_sink::{DlqRow, run_config_with_dlq};

fn run_correlated(csv: &str) -> Vec<DlqRow> {
    let yaml = dlq_fixtures::dlq_validate_pipeline(
        "failure_stamp",
        "employee_id",
        "      - { name: employee_id, type: int }\n      - { name: value, type: string }\n\n",
    );
    let config = parse_config(&yaml).expect("pipeline parses");
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        single_file_reader(
            "input.csv",
            Box::new(std::io::Cursor::new(csv.as_bytes().to_vec())),
        ),
    )]);
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(std::io::sink()) as Box<dyn std::io::Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "e".to_string(),
        batch_id: "b".to_string(),
        ..Default::default()
    };
    let (_, rows) = run_config_with_dlq(&config, readers, writers, &params).expect("run succeeds");
    rows
}

fn id(row: &DlqRow) -> uuid::Uuid {
    let cell = row.field("_cxl_dlq_id").expect("id column");
    let id = uuid::Uuid::parse_str(cell).expect("id is a UUID");
    assert_eq!(id.get_version_num(), 7, "id is a UUIDv7");
    id
}

fn timestamp(row: &DlqRow) -> chrono::DateTime<chrono::FixedOffset> {
    let cell = row.field("_cxl_dlq_timestamp").expect("timestamp column");
    chrono::DateTime::parse_from_rfc3339(cell).expect("timestamp is RFC 3339")
}

fn row_for(rows: &[DlqRow], source_row: u64) -> &DlqRow {
    rows.iter()
        .find(|row| row.source_row() == source_row)
        .unwrap_or_else(|| panic!("a dead letter for source row {source_row}"))
}

/// Group `10` holds rows 1 (failing) and 3; group `9` holds row 2
/// (failing). The key sort evaluates group `9` first, so row 2 fails before
/// row 1. The commit writes group `10` first (row 1's trigger, then row 3's
/// collateral) and group `9` last. A trigger carries the stamp taken at its
/// failure, so row 2's id precedes row 1's although row 2 is written last; a
/// collateral is stamped when its group is condemned at commit, after both
/// failures.
#[test]
fn buffered_trigger_keeps_the_stamp_taken_at_its_failure() {
    let rows = run_correlated("employee_id,value\n10,bad\n9,bad\n10,100\n");
    assert_eq!(rows.len(), 3, "two triggers and one collateral");

    let late_key_trigger = row_for(&rows, 1);
    let early_key_trigger = row_for(&rows, 2);
    let collateral = row_for(&rows, 3);
    assert!(late_key_trigger.trigger() && early_key_trigger.trigger());
    assert!(!collateral.trigger());

    let written: Vec<u64> = rows.iter().map(DlqRow::source_row).collect();
    assert_eq!(
        written,
        [1, 3, 2],
        "the commit writes group 10 before group 9, the reverse of failure order"
    );

    assert!(
        id(early_key_trigger) < id(late_key_trigger),
        "row 2 failed first, so its id was taken first although its row was written last"
    );
    assert!(
        id(late_key_trigger) < id(collateral),
        "the collateral is stamped at commit, after every failure"
    );
    assert!(timestamp(early_key_trigger) <= timestamp(late_key_trigger));
    assert!(timestamp(late_key_trigger) <= timestamp(collateral));
}
