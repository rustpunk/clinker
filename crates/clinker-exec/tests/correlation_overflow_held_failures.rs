//! A correlation group that overflows `max_group_buffer` while it holds
//! failures.
//!
//! Every failure the group holds is written once, as its own trigger with its
//! own category and id. The group's buffered rows that did not fail on their
//! own follow under one `group_size_exceeded` trigger, as `correlated` rows
//! paired with it; that trigger is stamped when the group crossed the cap. A
//! group holding only failures writes no `group_size_exceeded` row. Every
//! written row counts toward `dlq_count` and the rate breakers, and every
//! contributing Source rewinds its rollback cursor, including one that
//! contributed only failures.

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core_types::dlq::DlqErrorCategory;
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders,
    single_file_reader,
};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;
use dlq_sink::{CollectingDlqSink, DlqRow};

const TYPE_COERCION: &str = "type_coercion_failure";

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "test-exec-id".to_string(),
        batch_id: "test-batch-id".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// Run `yaml` over `readers`, one in-memory writer per name in `sinks`.
/// Returns the report and every dead-letter row, or the run's error.
fn run(
    yaml: &str,
    readers: SourceReaders,
    sinks: &[&str],
    params: &PipelineRunParams,
) -> Result<(ExecutionReport, Vec<DlqRow>), PipelineError> {
    let config = parse_config(yaml).expect("fixture pipeline parses");
    let plan = config
        .compile(&CompileContext::default())
        .expect("fixture pipeline compiles");
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = sinks
        .iter()
        .map(|name| {
            (
                (*name).to_string(),
                Box::new(SharedBuffer::new()) as Box<dyn std::io::Write + Send>,
            )
        })
        .collect();
    let sink = CollectingDlqSink::new();
    let report = PipelineExecutor::run_plan_with_readers_writers(
        &plan,
        readers,
        dlq_sink::registry(writers, &sink),
        params,
    )?;
    Ok((report, sink.rows()))
}

fn one_source(name: &str, csv: &str) -> SourceReaders {
    HashMap::from([(
        name.to_string(),
        single_file_reader("input.csv", Box::new(Cursor::new(csv.as_bytes().to_vec()))),
    )])
}

fn file_slot(name: &str, csv: &str) -> SourceInput {
    SourceInput::Files(vec![FileSlot::new(
        PathBuf::from(format!("{name}.csv")),
        Box::new(Cursor::new(csv.as_bytes().to_vec())),
    )])
}

fn id(row: &DlqRow) -> &str {
    row.field("_cxl_dlq_id")
        .expect("every dead-letter header carries _cxl_dlq_id")
}

fn trigger_id(row: &DlqRow) -> &str {
    row.field("_cxl_dlq_trigger_id")
        .expect("every dead-letter header carries _cxl_dlq_trigger_id")
}

fn uuid_of(cell: &str) -> uuid::Uuid {
    uuid::Uuid::parse_str(cell).unwrap_or_else(|_| panic!("{cell:?} is not a UUID"))
}

fn describe(rows: &[DlqRow]) -> Vec<(String, u64, Option<String>, bool)> {
    rows.iter()
        .map(|r| {
            (
                r.source_name().to_string(),
                r.source_row(),
                r.category().map(str::to_string),
                r.trigger(),
            )
        })
        .collect()
}

/// A single Source keyed on `employee_id`, one Transform that parses `value`
/// as an int, one Sink.
fn validate_pipeline(max_group_buffer: u64, dlq_extra: &str) -> String {
    format!(
        r#"
pipeline:
  name: overflow_held_failures
error_handling:
  strategy: continue
  max_group_buffer: {max_group_buffer}
  dlq:
    path: rejected.csv
{dlq_extra}
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: employee_id
    type: csv
    schema:
      - {{ name: employee_id, type: string }}
      - {{ name: value, type: string }}
- type: transform
  name: validate
  input: src
  config:
    cxl: |
      emit employee_id = employee_id
      emit val = value.to_int()
- type: sink
  name: out
  input: validate
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
"#
    )
}

/// Group A holds four buffered rows and one failure (row 2): five held
/// entries over a cap of 3. Row 2 is written first as its own
/// `type_coercion_failure` trigger. Row 1, the first buffered row, is the
/// `group_size_exceeded` trigger, and rows 3-5 are `correlated` rows paired
/// with it. Every row of the group is counted.
#[test]
fn overflow_writes_a_held_transform_failure_as_its_own_trigger() {
    let yaml = validate_pipeline(3, "");
    let csv = "employee_id,value\nA,100\nA,bad\nA,300\nA,400\nA,500\nB,600\n";
    let (report, rows) = run(&yaml, one_source("src", csv), &["out"], &run_params()).unwrap();

    assert_eq!(rows.len(), 5, "every row of group A: {:?}", describe(&rows));
    assert_eq!(report.counters.dlq_count, 5, "every row of group A counts");
    assert_eq!(report.counters.ok_count, 1, "only group B is written");

    let failure = &rows[0];
    assert_eq!(failure.source_row(), 2, "{:?}", describe(&rows));
    assert_eq!(failure.category(), Some(TYPE_COERCION));
    assert!(failure.trigger(), "the failure is its own trigger");
    assert_eq!(
        trigger_id(failure),
        id(failure),
        "the failure pairs with itself"
    );
    assert_eq!(failure.stage(), Some("transform:validate"));

    let overflow = &rows[1];
    assert_eq!(overflow.source_row(), 1, "{:?}", describe(&rows));
    assert_eq!(
        overflow.category(),
        Some(DlqErrorCategory::GroupSizeExceeded.as_str())
    );
    assert!(overflow.trigger());
    assert_eq!(trigger_id(overflow), id(overflow));
    assert_ne!(id(overflow), id(failure));
    let detail = overflow.error_detail().unwrap();
    assert!(
        detail.contains("max_group_buffer") && detail.contains('3'),
        "the overflow names the cap: {detail}"
    );
    assert!(
        detail.contains("5 entries"),
        "the overflow names how many entries the group held: {detail}"
    );

    let collateral: Vec<&DlqRow> = rows[2..].iter().collect();
    assert_eq!(
        collateral
            .iter()
            .map(|r| r.source_row())
            .collect::<Vec<_>>(),
        [3, 4, 5]
    );
    for row in collateral {
        assert_eq!(row.category(), Some(DlqErrorCategory::Correlated.as_str()));
        assert!(!row.trigger());
        assert_eq!(trigger_id(row), id(overflow), "paired with the overflow");
    }
}

/// Group A holds only failures, three of them over a cap of 2. Every failure
/// is written as its own trigger and no `group_size_exceeded` row is
/// invented: the overflow condemned nothing that had not already failed.
#[test]
fn a_failures_only_group_over_the_cap_writes_every_failure_and_no_overflow_row() {
    let yaml = validate_pipeline(2, "");
    let csv = "employee_id,value\nA,bad1\nA,bad2\nA,bad3\nB,100\n";
    let (report, rows) = run(&yaml, one_source("src", csv), &["out"], &run_params()).unwrap();

    assert_eq!(
        describe(&rows),
        [
            ("src".to_string(), 1, Some(TYPE_COERCION.to_string()), true),
            ("src".to_string(), 2, Some(TYPE_COERCION.to_string()), true),
            ("src".to_string(), 3, Some(TYPE_COERCION.to_string()), true),
        ]
    );
    for row in &rows {
        assert_eq!(trigger_id(row), id(row), "each failure pairs with itself");
    }
    assert_eq!(report.counters.dlq_count, 3);
    assert_eq!(report.counters.ok_count, 1);
}

/// An inclusive Route sends every row down both branches. Branch `a` parses
/// `value` and fails on row 1; branch `b` buffers row 1 at its Sink. Group A
/// holds six entries over a cap of 3. Row 1 is written once, as its own
/// failure, and never as `correlated` or as the overflow trigger.
#[test]
fn a_row_both_failed_and_buffered_is_written_once_as_its_own_failure() {
    let yaml = r#"
pipeline:
  name: overflow_route_fanout
error_handling:
  strategy: continue
  max_group_buffer: 3
  dlq:
    path: rejected.csv
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: employee_id
    type: csv
    schema:
      - { name: employee_id, type: string }
      - { name: value, type: string }
- type: route
  name: split
  input: src
  config:
    mode: inclusive
    conditions:
      a: 'employee_id != ""'
      b: 'employee_id != ""'
    default: a
- type: transform
  name: parse
  input: split.a
  config:
    cxl: |
      emit employee_id = employee_id
      emit val = value.to_int()
- type: sink
  name: out_a
  input: parse
  config:
    name: out_a
    path: out_a.csv
    type: csv
    include_unmapped: true
- type: sink
  name: out_b
  input: split.b
  config:
    name: out_b
    path: out_b.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "employee_id,value\nA,bad\nA,200\nA,300\n";
    let (report, rows) = run(
        yaml,
        one_source("src", csv),
        &["out_a", "out_b"],
        &run_params(),
    )
    .unwrap();

    let row_one: Vec<&DlqRow> = rows.iter().filter(|r| r.source_row() == 1).collect();
    assert_eq!(row_one.len(), 1, "row 1 once: {:?}", describe(&rows));
    let failure = row_one[0];
    assert_eq!(failure.category(), Some(TYPE_COERCION));
    assert!(failure.trigger());
    assert_eq!(trigger_id(failure), id(failure));
    assert_eq!(failure.stage(), Some("transform:parse"));

    let overflow: Vec<&DlqRow> = rows
        .iter()
        .filter(|r| r.category() == Some(DlqErrorCategory::GroupSizeExceeded.as_str()))
        .collect();
    assert_eq!(overflow.len(), 1, "{:?}", describe(&rows));
    assert_eq!(overflow[0].source_row(), 2);
    let correlated: Vec<&DlqRow> = rows
        .iter()
        .filter(|r| r.category() == Some(DlqErrorCategory::Correlated.as_str()))
        .collect();
    assert_eq!(correlated.len(), 1, "{:?}", describe(&rows));
    assert_eq!(correlated[0].source_row(), 3);
    assert_eq!(trigger_id(correlated[0]), id(overflow[0]));

    assert_eq!(
        rows.len(),
        3,
        "one row per source row: {:?}",
        describe(&rows)
    );
    assert_eq!(report.counters.dlq_count, 3);
}

/// Ten rows: group A holds three failures and one buffered row over a cap of
/// 2; six single-row groups are clean. Counting the failures, four of ten rows
/// dead-letter, which crosses `max_rate: 0.3`; counting only the overflow's
/// one buffered row would leave the rate at 0.1.
#[test]
fn held_failures_count_toward_the_rate_breaker() {
    let yaml = validate_pipeline(2, "    max_rate: 0.3\n    min_records: 1\n");
    let csv = "employee_id,value\nA,bad1\nA,bad2\nA,bad3\nA,100\n\
               B,1\nC,2\nD,3\nE,4\nF,5\nG,6\n";
    let err = run(&yaml, one_source("src", csv), &["out"], &run_params())
        .expect_err("the held failures push the dead-letter rate over 0.3");
    match err {
        PipelineError::DlqRateExceeded {
            source,
            max_rate,
            total_count,
            ..
        } => {
            assert!(source.is_none(), "the pipeline-wide breaker (E315) fires");
            assert_eq!(max_rate, 0.3);
            assert_eq!(total_count, 10);
        }
        other => panic!("expected E315 DlqRateExceeded, got: {other:?}"),
    }
}

/// `src_b` contributes only a failure (its row 1) to the overflowing group
/// `id=1`; its row 2 belongs to the clean group `id=9` and advances its
/// cursor to 2. The overflow rewinds `src_b` to row 1 all the same.
#[test]
fn a_source_that_contributed_only_failures_rewinds_its_cursor() {
    let yaml = r#"
pipeline:
  name: overflow_failure_only_source_rewind
error_handling:
  strategy: continue
  max_group_buffer: 3
  dlq:
    path: rejected.csv
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
        - { name: amt, type: string }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: amt, type: string }
  - type: merge
    name: m
    inputs: [src_a, src_b]
  - type: transform
    name: tfm
    input: m
    config:
      cxl: |
        emit id = id
        emit amt = amt.to_int()
  - type: sink
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let readers: SourceReaders = HashMap::from([
        (
            "src_a".to_string(),
            file_slot("a", "id,amt\n1,10\n1,11\n1,12\n"),
        ),
        ("src_b".to_string(), file_slot("b", "id,amt\n1,bad\n9,90\n")),
    ]);
    let (report, rows) = run(yaml, readers, &["out"], &run_params()).unwrap();

    assert_eq!(report.per_source_rollback_cursors.get("src_a"), Some(&1));
    assert_eq!(
        report.per_source_rollback_cursors.get("src_b"),
        Some(&1),
        "src_b rewinds over the failure it contributed to the overflowing group"
    );

    let src_b: Vec<&DlqRow> = rows.iter().filter(|r| r.source_name() == "src_b").collect();
    assert_eq!(src_b.len(), 1, "{:?}", describe(&rows));
    assert_eq!(src_b[0].source_row(), 1);
    assert_eq!(src_b[0].category(), Some(TYPE_COERCION));
    assert!(src_b[0].trigger());
    assert_eq!(report.counters.dlq_count, 4, "{:?}", describe(&rows));
}

/// The relaxed-key variant of the Transform-failure case: an Aggregate that
/// does not group by the correlation key makes the pipeline commit through
/// the retraction orchestrator. The strict Sink `out` buffers group `O1`'s
/// clean rows 1 and 3; row 2 fails. Three held entries exceed a cap of 2.
#[test]
fn relaxed_key_overflow_writes_a_held_failure_as_its_own_trigger() {
    let yaml = r#"
pipeline:
  name: relaxed_overflow_held_failure
error_handling:
  strategy: continue
  max_group_buffer: 2
  dlq:
    path: rejected.csv
nodes:
- type: source
  name: src
  config:
    name: src
    path: input.csv
    correlation_key: order_id
    type: csv
    schema:
      - { name: order_id, type: string }
      - { name: department, type: string }
      - { name: amount, type: string }
- type: transform
  name: validate
  input: src
  config:
    cxl: |
      emit order_id = order_id
      emit department = department
      emit amount_int = amount.to_int()
- type: sink
  name: out
  input: validate
  config:
    name: out
    path: output.csv
    type: csv
    include_unmapped: true
- type: aggregate
  name: dept_totals
  input: validate
  config:
    group_by: [department]
    cxl: |
      emit department = department
      emit total = sum(amount_int)
- type: sink
  name: totals
  input: dept_totals
  config:
    name: totals
    path: totals.csv
    type: csv
    include_unmapped: true
"#;
    let csv = "order_id,department,amount\nO1,HR,10\nO1,HR,BAD\nO1,HR,30\nO2,ENG,100\n";
    let (report, rows) = run(
        yaml,
        one_source("src", csv),
        &["out", "totals"],
        &run_params(),
    )
    .unwrap();

    assert_eq!(
        describe(&rows),
        [
            ("src".to_string(), 2, Some(TYPE_COERCION.to_string()), true),
            (
                "src".to_string(),
                1,
                Some(DlqErrorCategory::GroupSizeExceeded.as_str().to_string()),
                true
            ),
            (
                "src".to_string(),
                3,
                Some(DlqErrorCategory::Correlated.as_str().to_string()),
                false
            ),
        ]
    );
    assert_eq!(trigger_id(&rows[0]), id(&rows[0]));
    assert_eq!(trigger_id(&rows[1]), id(&rows[1]));
    assert_eq!(trigger_id(&rows[2]), id(&rows[1]));
    assert_eq!(report.counters.dlq_count, 3);
}

/// The `group_size_exceeded` row is stamped when its group crosses the cap,
/// not when the group is committed. Group `Z` crosses a cap of 2 at its
/// third row, before group `A` does at its own third row, but commits after
/// `A` (groups commit in key order). Ids are UUIDv7 from one process-wide
/// generator, so they increase in the order stamps are taken: `Z`'s overflow
/// id sorts before `A`'s only if each was taken at its crossing.
#[test]
fn the_overflow_is_stamped_when_the_group_crosses_the_cap() {
    let yaml = validate_pipeline(2, "");
    let csv = "employee_id,value\nZ,1\nZ,2\nZ,3\nA,4\nA,5\nA,6\n";
    let (_report, rows) = run(&yaml, one_source("src", csv), &["out"], &run_params()).unwrap();

    let overflow_of = |key_first_row: u64| -> &DlqRow {
        rows.iter()
            .find(|r| {
                r.source_row() == key_first_row
                    && r.category() == Some(DlqErrorCategory::GroupSizeExceeded.as_str())
            })
            .unwrap_or_else(|| panic!("row {key_first_row} is an overflow: {:?}", describe(&rows)))
    };
    let z = overflow_of(1);
    let a = overflow_of(4);
    assert!(
        uuid_of(id(z)) < uuid_of(id(a)),
        "Z crossed the cap first, so its overflow id sorts first: Z={} A={}",
        id(z),
        id(a)
    );
    let z_at = z.field("_cxl_dlq_timestamp").unwrap();
    let a_at = a.field("_cxl_dlq_timestamp").unwrap();
    let parse = |s: &str| chrono::DateTime::parse_from_rfc3339(s).unwrap();
    assert!(parse(z_at) <= parse(a_at), "Z={z_at} A={a_at}");

    // The group's correlated rows are condemned at commit, after the
    // crossing, so they sort after the overflow they pair with.
    for row in rows
        .iter()
        .filter(|r| trigger_id(r) == id(z) && !r.trigger())
    {
        assert!(uuid_of(id(z)) < uuid_of(id(row)));
    }
}
