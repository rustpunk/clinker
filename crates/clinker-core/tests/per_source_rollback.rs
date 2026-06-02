//! End-to-end coverage for per-source `$ck` rollback narrowing.
//!
//! Topology models the multi-source umbrella's load-bearing
//! `[src_a, src_b] → merge → tfm → out` shape with a shared
//! `correlation_key` on `id`. `tfm` fires `1 / 0` on records originating
//! from `src_b` only, so the buffered correlation group at commit time
//! has a clean per-source partition between trigger (src_b) and
//! co-grouped collateral candidate (src_a). The pre-sprint-7 executor
//! would have collateral-DLQ'd every co-grouped slot regardless of
//! origin; per-source narrowing now spares src_a's slots — `src_a` had
//! no causal role in the src_b-triggered failure.
//!
//! The single-source regression test (AC4) pins that per-source
//! narrowing reduces to today's pipeline-wide collateral DLQ when the
//! pipeline has exactly one Source. The narrowing is a no-op there
//! because every co-grouped slot shares the failing source.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core::config::{CompileContext, parse_config};
use clinker_core::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_core::source::multi_file::FileSlot;

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

/// AC2 regression: src_b's mid-window failure must NOT collaterally
/// DLQ src_a's co-grouped records. Two sources share the `id`
/// correlation key. `tfm` rewrites a column using a CXL conditional
/// that divides by zero only when `$source.name == "src_b"`. With
/// per-source narrowing the dirty correlation group for `id=1` has:
/// a single trigger (src_b id=1) and src_a id=1 spared (different
/// source). Same for `id=2`. Two trigger DLQs total; both src_a rows
/// reach the output.
#[test]
fn ac2_collateral_narrowing_spares_other_source() {
    let yaml = r#"
pipeline:
  name: ac2_collateral_narrowing
error_handling:
  strategy: continue
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
        - { name: amt, type: int }
  - type: source
    name: src_b
    config:
      name: src_b
      type: csv
      path: b.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: merge
    name: m
    inputs: [src_a, src_b]
  - type: transform
    name: tfm
    input: m
    config:
      cxl: |
        emit id = id
        emit ratio = if($source.name == "src_b") then (1 / 0) else amt
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
            "src_a".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("a", "id,amt\n1,10\n2,20\n")]),
        ),
        (
            "src_b".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("b", "id,amt\n1,99\n2,99\n")]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    let dlq_by_source: HashMap<&str, usize> =
        report
            .dlq_entries
            .iter()
            .fold(HashMap::new(), |mut acc, e| {
                *acc.entry(e.source_name.as_ref()).or_insert(0) += 1;
                acc
            });

    assert_eq!(
        dlq_by_source.get("src_b").copied().unwrap_or(0),
        2,
        "both src_b records DLQ as triggers"
    );
    assert_eq!(
        dlq_by_source.get("src_a").copied().unwrap_or(0),
        0,
        "src_a's co-grouped records are spared by per-source narrowing"
    );

    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert_eq!(body.len(), 2, "both src_a rows reach the output: {output}");
    assert!(body.iter().any(|l| l.contains("1,10")));
    assert!(body.iter().any(|l| l.contains("2,20")));

    assert_eq!(
        report.per_source_rollback_cursors.get("src_a"),
        Some(&2),
        "src_a cursor advances through both clean records"
    );
    assert_eq!(
        report.per_source_rollback_cursors.get("src_b"),
        None,
        "src_b never cleanly cleared an operator — no cursor entry"
    );
}

/// AC4 single-source regression: per-source narrowing is a no-op on a
/// single-source pipeline because every co-grouped slot shares the
/// failing source. The pre-sprint-7 collateral DLQ behavior must be
/// bit-identical. Both `bad` and the co-grouped `good` row of group
/// `id=1` land in DLQ — the narrowing branch never spares anything.
#[test]
fn ac4_single_source_regression_unchanged() {
    let yaml = r#"
pipeline:
  name: ac4_single_source
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src
    config:
      name: src
      type: csv
      path: in.csv
      correlation_key: id
      schema:
        - { name: id, type: int }
        - { name: tag, type: string }
        - { name: amt, type: int }
  - type: transform
    name: tfm
    input: src
    config:
      cxl: |
        emit id = id
        emit ratio = if(tag == "bad") then (1 / 0) else amt
  - type: output
    name: out
    input: tfm
    config:
      name: out
      type: csv
      path: out.csv
      include_unmapped: false
"#;
    let config = parse_config(yaml).unwrap();
    let readers: SourceReaders = HashMap::from([(
        "src".to_string(),
        clinker_core::executor::SourceInput::Files(vec![slot(
            "in",
            "id,tag,amt\n1,good,10\n1,bad,20\n2,good,30\n",
        )]),
    )]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    let triggers = report.dlq_entries.iter().filter(|e| e.trigger).count();
    let collaterals = report.dlq_entries.iter().filter(|e| !e.trigger).count();
    assert_eq!(triggers, 1, "the bad row is the single trigger");
    assert_eq!(
        collaterals, 1,
        "group id=1's surviving good row is collateral-DLQ'd: \
         same-source as the trigger so per-source narrowing leaves \
         today's pipeline-wide behavior intact"
    );

    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert_eq!(
        body.len(),
        1,
        "only group id=2's good row clears to output: {output}"
    );
    assert!(body[0].contains("2,30"));

    assert_eq!(
        report.per_source_rollback_cursors.get("src"),
        Some(&3),
        "cursor advances through every clean record including the \
         collateral-DLQ'd one (its clean-branch advance fires before \
         the dirty-group flush at commit)"
    );
}

/// AC3 (partial): a multi-source Combine ingests records from BOTH
/// driver and build sources, snapshots their per-source cursors at
/// fold entry, and on a clean run leaves the surfaced cursors
/// reflecting forward progress for the driver side. The snapshot
/// machinery underpinning the AC3 cursor-rewind is in place; this
/// test covers the clean-run capture half (snapshot taken at fold
/// start, dropped at fold exit). Setup-time
/// `PipelineError::Internal { op: "combine" }` invariant violations
/// still fail-fast and bypass the rewind; the recoverable-DLQ
/// rewind path is covered by separate Combine-failure tests.
#[test]
fn ac3_combine_snapshot_capture_clean_run() {
    let yaml = r#"
pipeline:
  name: ac3_combine_snapshot
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
        - { name: dept, type: string }
  - type: transform
    name: passthrough
    input: src_drv
    config:
      cxl: |
        emit id = id
        emit amt = amt
  - type: combine
    name: enriched
    input:
      d: passthrough
      b: src_bld
    config:
      where: 'd.id == b.id'
      match: first
      on_miss: skip
      cxl: |
        emit id = d.id
        emit amt = d.amt
        emit dept = b.dept
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
    let readers: SourceReaders = HashMap::from([
        (
            "src_drv".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("drv", "id,amt\n1,10\n2,20\n")]),
        ),
        (
            "src_bld".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("bld", "id,dept\n1,HR\n2,ENG\n")]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    assert!(
        report.dlq_entries.is_empty(),
        "clean Combine run produces no DLQ entries"
    );
    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert_eq!(
        body.len(),
        2,
        "every driver row matched a build row and reached the writer: {output}"
    );

    // Driver-side records flow through the passthrough Transform's
    // clean branch, so `advance_cursor` fires for them. The cursor
    // surfaces on the report as the highest committed row_num
    // (per-source) at run completion.
    assert_eq!(
        report.per_source_rollback_cursors.get("src_drv"),
        Some(&2),
        "driver source advanced through both Transform clean exits"
    );
    // Build-side records flow straight from ingest into Combine's
    // build buffer. The Combine arm now advances per-source cursors
    // at operator entry for every build (and driver) record before
    // the `row_num` is discarded, so `src_bld` surfaces here at the
    // highest contributed row_num. The Combine arm's snapshot
    // captures the pre-fold cursor state independently for each
    // contributing source so a recoverable Combine-output failure
    // can rewind each source's cursor symmetrically.
    assert_eq!(
        report.per_source_rollback_cursors.get("src_bld"),
        Some(&2),
        "build source advances at Combine operator-entry walk over build_buf"
    );
}

/// A recoverable Combine output-row eval failure under
/// `error_handling: strategy: continue` routes the failing driver row to
/// the DLQ with category `combine_output_row`, attributes the entry to
/// the contributing input sources (driver + matched build), and rewinds
/// BOTH contributing sources' rollback cursors to the captured pre-fold
/// floor.
///
/// Topology clones `ac3_combine_snapshot_capture_clean_run` but routes
/// the driver direct into the Combine (no upstream Transform) so both
/// sources' captured snapshot floor is 0. The Combine arm's
/// operator-entry walk advances both cursors to 2 before the probe loop;
/// the matched-body division-by-zero on driver row `id=2` then rewinds
/// each contributing source back to its floor of 0. The clean `id=1`
/// driver row still reaches the output. No `correlation_key` is declared,
/// so the recoverable path takes the direct `push_dlq` branch rather than
/// parking under a correlation group cell.
#[test]
fn ac3_combine_output_row_recoverable_dlq_rewinds_both_sources() {
    let yaml = r#"
pipeline:
  name: ac3_combine_output_row_recoverable
error_handling:
  strategy: continue
nodes:
  - type: source
    name: src_drv
    config:
      name: src_drv
      type: csv
      path: drv.csv
      schema:
        - { name: id, type: int }
        - { name: amt, type: int }
  - type: source
    name: src_bld
    config:
      name: src_bld
      type: csv
      path: bld.csv
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
    // `d.amt / b.factor` eval divides by zero for driver row id=2 only.
    let readers: SourceReaders = HashMap::from([
        (
            "src_drv".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("drv", "id,amt\n1,10\n2,20\n")]),
        ),
        (
            "src_bld".to_string(),
            clinker_core::executor::SourceInput::Files(vec![slot("bld", "id,factor\n1,2\n2,0\n")]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), writer(&buf))]);

    let plan = config.compile(&CompileContext::default()).unwrap();
    let report =
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &run_params())
            .unwrap();

    // The failing output row produces a `combine_output_row` DLQ entry
    // scoped to the contributing sources — a trigger on the driver and a
    // non-trigger entry on the matched build. Neither falls back to the
    // synthetic merged source.
    let combine_dlq: Vec<&clinker_core::executor::DlqEntry> = report
        .dlq_entries
        .iter()
        .filter(|e| e.category == clinker_core::dlq::DlqErrorCategory::CombineOutputRow)
        .collect();
    assert_eq!(
        combine_dlq.len(),
        2,
        "one trigger (driver) + one build-side entry for the failing row: {:?}",
        report
            .dlq_entries
            .iter()
            .map(|e| (e.source_name.as_ref(), e.category.as_str(), e.trigger))
            .collect::<Vec<_>>()
    );
    assert!(
        combine_dlq
            .iter()
            .any(|e| e.source_name.as_ref() == "src_drv" && e.trigger),
        "driver row is the attributed trigger on src_drv"
    );
    assert!(
        combine_dlq
            .iter()
            .any(|e| e.source_name.as_ref() == "src_bld" && !e.trigger),
        "matched build row is attributed to src_bld, not the merged source"
    );

    // Both contributing sources rewind to their captured pre-fold floor
    // (0), undoing the operator-entry advance to row 2. Asserted exactly
    // like the GroupSizeExceeded per-source rewind above.
    assert_eq!(
        report.per_source_rollback_cursors.get("src_drv"),
        Some(&0),
        "driver source rewinds to its pre-fold snapshot floor of 0"
    );
    assert_eq!(
        report.per_source_rollback_cursors.get("src_bld"),
        Some(&0),
        "build source rewinds to its pre-fold snapshot floor of 0"
    );

    // The clean id=1 driver row still reaches the writer; the failing
    // id=2 row is dropped from the output.
    let output = buf.as_string();
    let body: Vec<&str> = output.lines().skip(1).collect();
    assert_eq!(
        body.len(),
        1,
        "only the clean id=1 row reaches the output: {output}"
    );
    assert!(
        body[0].starts_with("1,"),
        "the surviving row is id=1: {output}"
    );
}
