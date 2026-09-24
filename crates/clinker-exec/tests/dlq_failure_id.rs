//! `_cxl_dlq_failure_id` pairs every dead-letter row one failure produced.
//!
//! The column holds the `_cxl_dlq_id` of the trigger row of the failure
//! behind a row. A failure that wrote one row carries its own id. A
//! correlation group's collaterals carry the id of the group's first
//! trigger, the error their detail quotes, while each trigger keeps its own.
//! A rejected document's other records, including a second failing record,
//! carry the document trigger's id. A Combine build row carries its driver's
//! id wherever it is held.
//!
//! Rows are read by column name through the collecting test sink, never by
//! position.

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core_types::dlq::DlqErrorCategory;
use clinker_exec::executor::{PipelineRunParams, SourceInput, SourceReaders};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::parse_config;

use dlq_sink::DlqRow;

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "failure-id".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// Feed each source its files, one `FileSlot` (one document) per file.
fn readers(sources: &[(&str, &[(&str, &str)])]) -> SourceReaders {
    sources
        .iter()
        .map(|(source, files)| {
            let slots = files
                .iter()
                .map(|(name, body)| {
                    FileSlot::new(
                        PathBuf::from(*name),
                        Box::new(Cursor::new(body.as_bytes().to_vec())),
                    )
                })
                .collect();
            (source.to_string(), SourceInput::Files(slots))
        })
        .collect()
}

/// Run `yaml` with every named Sink writing to a discarded buffer, and
/// return the dead-letter rows. Asserts every counted dead letter was
/// written as a row.
fn run(yaml: &str, sinks: &[&str], sources: &[(&str, &[(&str, &str)])]) -> Vec<DlqRow> {
    let config = parse_config(yaml).expect("pipeline parses");
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = sinks
        .iter()
        .map(|name| {
            (
                name.to_string(),
                Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let (report, rows) =
        dlq_sink::run_config_with_dlq(&config, readers(sources), writers, &run_params())
            .expect("pipeline runs");
    assert_eq!(
        rows.len() as u64,
        report.counters.dlq_count,
        "every dead letter is written as a row"
    );
    rows
}

fn id(row: &DlqRow) -> &str {
    row.field("_cxl_dlq_id")
        .expect("every dead-letter header carries _cxl_dlq_id")
}

fn failure_id(row: &DlqRow) -> &str {
    row.field("_cxl_dlq_failure_id")
        .expect("every dead-letter header carries _cxl_dlq_failure_id")
}

fn describe(rows: &[DlqRow]) -> Vec<(u64, bool, Option<&str>, Option<&str>)> {
    rows.iter()
        .map(|row| {
            (
                row.source_row(),
                row.trigger(),
                row.category(),
                row.field("_cxl_dlq_failure_id"),
            )
        })
        .collect()
}

/// A correlation group with two failing rows and one clean row. Each
/// trigger keeps its own failure id; the collateral carries the first
/// trigger's, and its detail quotes that trigger's message.
#[test]
fn multi_trigger_group_collaterals_pair_with_the_first_trigger() {
    let yaml = r#"
pipeline:
  name: failure_id_multi_trigger
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: input.csv
      correlation_key: employee_id
      schema:
        - { name: employee_id, type: string }
        - { name: value, type: string }
  - type: transform
    name: validate
    input: src
    config:
      cxl: |
        emit emp_id = employee_id
        emit val = value.to_int()
  - type: sink
    name: out
    input: validate
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let rows = run(
        yaml,
        &["out"],
        &[(
            "src",
            &[(
                "input.csv",
                "employee_id,value\nA,first_bad\nA,100\nA,second_bad\nB,400\n",
            )],
        )],
    );
    assert_eq!(
        rows.len(),
        3,
        "group A is condemned whole: {:?}",
        describe(&rows)
    );

    let mut triggers: Vec<&DlqRow> = rows.iter().filter(|row| row.trigger()).collect();
    triggers.sort_by_key(|row| row.source_row());
    assert_eq!(triggers.len(), 2, "{:?}", describe(&rows));
    for trigger in &triggers {
        assert_eq!(
            failure_id(trigger),
            id(trigger),
            "each trigger of a multi-trigger group keeps its own failure id"
        );
    }
    let first = triggers[0];
    assert_eq!(first.source_row(), 1, "row 1 fails first");

    let collateral: Vec<&DlqRow> = rows.iter().filter(|row| !row.trigger()).collect();
    assert_eq!(collateral.len(), 1, "{:?}", describe(&rows));
    let collateral = collateral[0];
    assert_eq!(
        collateral.category(),
        Some(DlqErrorCategory::Correlated.as_str())
    );
    assert_eq!(collateral.source_row(), 2);
    assert_eq!(
        failure_id(collateral),
        id(first),
        "the collateral pairs with the group's first trigger"
    );
    let first_detail = first.error_detail().expect("include_reason defaults on");
    assert!(
        collateral
            .error_detail()
            .expect("include_reason defaults on")
            .contains(first_detail),
        "the collateral's detail quotes the trigger it pairs with: {:?} vs {:?}",
        collateral.error_detail(),
        first_detail
    );
}

/// A document with two failing records and a clean one. The first failure
/// is the document's trigger; the second failure and the clean record are
/// its collaterals and carry the trigger's id.
#[test]
fn document_extra_failure_pairs_with_the_document_trigger() {
    let yaml = r#"
pipeline:
  name: failure_id_document
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
        - { name: id, type: string }
        - { name: value, type: string }
  - type: transform
    name: validate
    input: events
    config:
      cxl: |
        emit id = id
        emit val = value.to_int()
  - type: sink
    name: out
    input: validate
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: true
"#;
    let rows = run(
        yaml,
        &["out"],
        &[(
            "events",
            &[
                ("a.csv", "id,value\na1,bad\na2,200\na3,nope\n"),
                ("b.csv", "id,value\nb1,1\n"),
            ],
        )],
    );
    assert_eq!(
        rows.len(),
        3,
        "document a is rejected whole: {:?}",
        describe(&rows)
    );

    let triggers: Vec<&DlqRow> = rows.iter().filter(|row| row.trigger()).collect();
    assert_eq!(triggers.len(), 1, "{:?}", describe(&rows));
    let trigger = triggers[0];
    assert_eq!(trigger.source_row(), 1, "the first failure is the trigger");
    assert_eq!(failure_id(trigger), id(trigger));

    let mut collaterals: Vec<&DlqRow> = rows.iter().filter(|row| !row.trigger()).collect();
    collaterals.sort_by_key(|row| row.source_row());
    assert_eq!(
        collaterals
            .iter()
            .map(|row| row.source_row())
            .collect::<Vec<_>>(),
        vec![2, 3],
        "the clean record and the second failure are collaterals"
    );
    for row in collaterals {
        assert_eq!(
            row.category(),
            Some(DlqErrorCategory::DocumentRejected.as_str())
        );
        assert_eq!(
            failure_id(row),
            id(trigger),
            "row {} pairs with the document's trigger",
            row.source_row()
        );
        assert_ne!(id(row), id(trigger), "each row keeps its own id");
    }
}

/// A Combine whose body fails for a driver, with sources correlated on a
/// column the join does not use, so the driver and its matched build row
/// fall in different groups. However the build row's group commits, it
/// carries the driver trigger's id.
#[test]
fn combine_build_row_held_by_a_correlation_group_keeps_the_driver_failure_id() {
    let yaml = r#"
pipeline:
  name: failure_id_combine_build
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src_drv
    config:
      name: src_drv
      type: csv
      path: drv.csv
      correlation_key: cid
      schema:
        - { name: cid, type: string }
        - { name: k, type: int }
        - { name: amt, type: int }
  - type: source
    name: src_bld
    config:
      name: src_bld
      type: csv
      path: bld.csv
      correlation_key: cid
      schema:
        - { name: cid, type: string }
        - { name: k, type: int }
        - { name: div, type: int }
  - type: combine
    name: enriched
    input:
      d: src_drv
      b: src_bld
    config:
      where: 'd.k == b.k'
      match: first
      on_miss: skip
      cxl: |
        emit cid = d.cid
        emit ratio = d.amt / b.div
      propagate_ck: driver
  - type: sink
    name: out
    input: enriched
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let rows = run(
        yaml,
        &["out"],
        &[
            ("src_drv", &[("drv.csv", "cid,k,amt\nA,1,10\n")]),
            ("src_bld", &[("bld.csv", "cid,k,div\nB,1,0\n")]),
        ],
    );
    let driver = rows
        .iter()
        .find(|row| row.source_name() == "src_drv")
        .unwrap_or_else(|| panic!("the failing driver is dead-lettered: {:?}", describe(&rows)));
    let build = rows
        .iter()
        .find(|row| row.source_name() == "src_bld")
        .unwrap_or_else(|| {
            panic!(
                "the matched build row is dead-lettered: {:?}",
                describe(&rows)
            )
        });
    assert_eq!(
        failure_id(driver),
        id(driver),
        "the driver is the failure's trigger"
    );
    assert_eq!(
        failure_id(build),
        id(driver),
        "the build row keeps its driver's failure id through its correlation group"
    );
    assert_ne!(id(build), id(driver), "the build row has its own id");
}

/// Transform and Route failures with no correlation key each write one
/// row, which carries its own id as its failure id.
#[test]
fn standalone_triggers_carry_their_own_id() {
    let yaml = r#"
pipeline:
  name: failure_id_standalone
error_handling:
  strategy: continue
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: input.csv
      schema:
        - { name: id, type: int }
        - { name: amount, type: int }
        - { name: gate, type: int }
  - type: transform
    name: ratio
    input: src
    config:
      cxl: |
        emit id = id
        emit amount = amount
        emit gate = gate
        emit ratio = id / amount
  - type: route
    name: split
    input: ratio
    config:
      mode: exclusive
      conditions:
        big: 'amount / gate > 10'
      default: small
  - type: sink
    name: big
    input: split.big
    config:
      name: big
      type: csv
      path: big.csv
  - type: sink
    name: small
    input: split.small
    config:
      name: small
      type: csv
      path: small.csv
"#;
    let rows = run(
        yaml,
        &["big", "small"],
        &[(
            "src",
            &[(
                "input.csv",
                "id,amount,gate\n1,100,1\n2,0,1\n3,50,0\n4,5,1\n5,0,2\n",
            )],
        )],
    );
    assert_eq!(rows.len(), 3, "{:?}", describe(&rows));
    let stages: Vec<Option<&str>> = rows.iter().map(DlqRow::stage).collect();
    assert!(
        stages.contains(&Some("transform:ratio")) && stages.contains(&Some("route_eval")),
        "both a Transform and a Route failure are written: {stages:?}"
    );
    for row in &rows {
        assert!(row.trigger(), "a standalone failure is its own trigger");
        assert_eq!(
            failure_id(row),
            id(row),
            "row {} carries its own id as its failure id",
            row.source_row()
        );
        let parsed = uuid::Uuid::parse_str(failure_id(row)).expect("the failure id is a UUID");
        assert_eq!(
            parsed.get_version_num(),
            7,
            "the failure id is a version-7 UUID"
        );
    }
}
