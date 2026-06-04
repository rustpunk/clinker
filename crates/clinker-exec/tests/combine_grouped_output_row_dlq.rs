//! Grouped (correlation-key) coverage for the recoverable Combine
//! output-row dead-letter path.
//!
//! The existing `combine_output_row` recovery tests declare no
//! `correlation_key`, so a recoverable output-stage eval failure takes
//! the ungrouped direct-push branch. This test declares `correlation_key`
//! on both Combine sources, so the failure parks under its `$ck` group
//! cell and surfaces through group-commit — exercising that the parked
//! failure's category (`combine_output_row`) and stage (`combine:<name>`)
//! survive group-commit verbatim rather than being rewritten to the
//! generic correlation-commit disposition, and that the failing group
//! flushes atomically while clean groups still reach the writer.

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

/// A recoverable Combine output-row eval failure parks under its
/// correlation group cell and surfaces through group-commit with its
/// original category and stage preserved.
///
/// Both Combine sources declare `correlation_key: id` and the Combine
/// equijoins on `id`, so the driver row and its matched build row share
/// one `$ck` group cell per id. The matched-body `d.amt / b.factor` eval
/// divides by zero on `id=2` (build factor=0), so that group's cell goes
/// dirty; ids 1 and 3 land in their own clean cells. At group-commit the
/// dirty cell re-emits its parked failures as `combine_output_row`
/// triggers (category and `combine:enriched` stage copied verbatim from
/// the parked records — NOT rewritten to the generic correlation-commit
/// disposition), while the two clean cells flush their surviving rows to
/// the writer. The failing group is a self-contained single-member group,
/// so no clean survivor is co-grouped with the failure and DLQ'd as
/// collateral.
#[test]
fn grouped_combine_output_row_parks_under_group_cell_and_flushes_atomically() {
    let yaml = r#"
pipeline:
  name: grouped_combine_output_row
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_drv
    config:
      name: src_drv
      type: csv
      path: drv.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: source
    name: src_bld
    config:
      name: src_bld
      type: csv
      path: bld.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: factor, type: int }
  - type: combine
    name: enriched
    input:
      d: src_drv
      b: src_bld
    config:
      where: 'd.id == b.id'
      match: first
      on_miss: skip
      cxl: |
        emit id = d.id
        emit ratio = d.amt / b.factor
      propagate_ck: driver
  - type: output
    name: out
    input: enriched
    config:
      name: out
      type: csv
      path: out.csv
"#;
    let config = parse_config(yaml).unwrap();
    // Build row id=2 carries factor=0, so the matched-body
    // `d.amt / b.factor` eval divides by zero for driver row id=2 only;
    // ids 1 and 3 evaluate cleanly into their own group cells.
    let readers: SourceReaders = HashMap::from([
        (
            "src_drv".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot(
                "drv",
                "id,amt\n1,10\n2,20\n3,30\n",
            )]),
        ),
        (
            "src_bld".to_string(),
            clinker_exec::executor::SourceInput::Files(vec![slot(
                "bld",
                "id,factor\n1,2\n2,0\n3,5\n",
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

    // (a) The parked failure surfaces through group-commit as
    // `combine_output_row` — category and stage preserved verbatim, not
    // rewritten to the generic correlation-commit disposition.
    let combine_dlq: Vec<&clinker_exec::executor::DlqEntry> = report
        .dlq_entries
        .iter()
        .filter(|e| e.category == clinker_core_types::dlq::DlqErrorCategory::CombineOutputRow)
        .collect();
    assert_eq!(
        combine_dlq.len(),
        2,
        "the failing group parks the driver row and its matched build row, \
         both surfacing as combine_output_row through group-commit: {:?}",
        report
            .dlq_entries
            .iter()
            .map(|e| (e.source_name.as_ref(), e.category.as_str(), e.trigger))
            .collect::<Vec<_>>()
    );

    let driver_entry = combine_dlq
        .iter()
        .find(|e| e.source_name.as_ref() == "src_drv")
        .expect("driver row of the failing group is attributed to src_drv");
    assert!(
        driver_entry.trigger,
        "the driver row is the group's trigger after group-commit"
    );
    assert_eq!(
        driver_entry.stage.as_deref(),
        Some("combine:enriched"),
        "the parked Combine stage survives group-commit, not rewritten to \
         the generic correlation-commit stage"
    );
    assert_eq!(
        driver_entry.source_row, 2,
        "the failing driver source row is id=2's 1-based source row"
    );

    let build_entry = combine_dlq
        .iter()
        .find(|e| e.source_name.as_ref() == "src_bld")
        .expect("matched build row is attributed to src_bld, not the merged source");
    assert_eq!(
        build_entry.stage.as_deref(),
        Some("combine:enriched"),
        "the parked build-side Combine stage also survives group-commit"
    );

    // (b) The failing group is a self-contained single-member dirty cell,
    // so no co-grouped survivor is collaterally DLQ'd: no `correlated`
    // entry is produced for id=2's group.
    assert!(
        !report
            .dlq_entries
            .iter()
            .any(|e| e.category == clinker_core_types::dlq::DlqErrorCategory::Correlated),
        "id=2 is the only member of its dirty group, so no collateral \
         (Correlated) entry is produced: {:?}",
        report
            .dlq_entries
            .iter()
            .map(|e| (e.source_name.as_ref(), e.category.as_str()))
            .collect::<Vec<_>>()
    );

    // The clean id=1 and id=3 groups flush their surviving rows; the
    // failing id=2 output is dropped.
    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert_eq!(
        body.len(),
        2,
        "the clean id=1 and id=3 groups each flush one surviving row: {output}"
    );
    assert!(
        body.iter().any(|line| line.starts_with("1,")),
        "the id=1 survivor reaches the output: {output}"
    );
    assert!(
        body.iter().any(|line| line.starts_with("3,")),
        "the id=3 survivor reaches the output: {output}"
    );
    assert!(
        !body.iter().any(|line| line.starts_with("2,")),
        "the failing id=2 row is dropped from the output: {output}"
    );
}
