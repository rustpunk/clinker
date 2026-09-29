//! A Combine output failure's build-side dead letter under a correlation
//! key.
//!
//! When a Combine body fails for a driver row, the matched build record is
//! dead-lettered too. Under correlation buffering that build-side dead
//! letter is held with the failing driver's correlation group, as a
//! collateral of that group: it is written, or rolled back, exactly when
//! the driver's group is. It never condemns a group by itself, so another
//! driver that matched the same build record keeps its output unless its
//! own group failed. Each failing driver writes its own copy of the build
//! row, paired with that driver's failure, even when two failing drivers
//! share a group.
//!
//! The hash, grace-hash and IEJoin cases join on a column that is not the
//! correlation key, so the build record's correlation value is independent
//! of its drivers'. Sort-merge joins on a range over the correlation key,
//! the only shape that keeps its sort licence under correlation. Each case
//! runs once per physical join strategy that reaches the Combine output
//! dead-letter path, and asserts through `--explain` that the strategy it
//! names is the one selected.
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
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

use dlq_sink::DlqRow;

/// The build record's `base`; a clean driver with `div = 2` emits half of it.
const BASE: f64 = 10.0;

/// One physical join strategy that reaches the Combine output dead-letter
/// path: its `--explain` tag, its `where` clause, an optional strategy hint,
/// and whether both sources declare a `sort_order` on the correlation key.
struct Strategy {
    tag: &'static str,
    predicate: &'static str,
    hint: Option<&'static str>,
    sorted_on_key: bool,
}

/// The equi strategies join on `k`; IEJoin on `v`. None of these joins on
/// the correlation key `cid`.
///
/// Sort-merge is absent here because no shape of this kind selects it. Each
/// correlated source is re-sorted on its correlation key first (`cid, lo`
/// and `cid, v` for declared `sort_order`s on `lo` and `v`), so a range on
/// a non-key column loses its sort licence and the planner picks IEJoin.
/// [`SORT_MERGE`] reaches it with a range on the correlation key itself.
const STRATEGIES: &[Strategy] = &[
    Strategy {
        tag: "hash_build_probe",
        predicate: "d.k == b.k",
        hint: None,
        sorted_on_key: false,
    },
    Strategy {
        tag: "grace_hash",
        predicate: "d.k == b.k",
        hint: Some("grace_hash"),
        sorted_on_key: false,
    },
    Strategy {
        tag: "iejoin",
        predicate: "d.lo <= b.v and d.hi >= b.v",
        hint: None,
        sorted_on_key: false,
    },
];

/// Sort-merge: both sources declare `sort_order` on an int correlation key
/// and the Combine, qualifying its inputs by Source name, joins on a single
/// range over it, so the sort licence survives the correlation sort.
/// Because it joins on the correlation key, it fits only cases whose build
/// row's `cid` is at or after every driver's, with numeric `cid` values.
const SORT_MERGE: Strategy = Strategy {
    tag: "sort_merge",
    predicate: "src_drv.cid <= src_bld.cid",
    hint: None,
    sorted_on_key: true,
};

/// A two-source correlated Combine: both sources declare
/// `correlation_key: cid`, the body divides the build's `base` by the
/// driver's `div`, and the output keeps the driver's correlation key.
fn yaml(strategy: &Strategy) -> String {
    yaml_with(strategy, "", "")
}

/// [`yaml`] with `error_extra` appended to the `error_handling` block and
/// `extra_nodes` appended to the node list.
fn yaml_with(strategy: &Strategy, error_extra: &str, extra_nodes: &str) -> String {
    let hint = strategy
        .hint
        .map(|hint| format!("\n      strategy: {hint}"))
        .unwrap_or_default();
    let predicate = strategy.predicate;
    // A range needs an orderable key, so the sort-merge shape types `cid`
    // as an int. The planner finds a range input's sort licence only when
    // its qualifier names the Source, so that shape qualifies by Source
    // name.
    let (sort_order, cid_type, dq, bq) = if strategy.sorted_on_key {
        (
            "\n      sort_order:\n        - field: cid",
            "int",
            "src_drv",
            "src_bld",
        )
    } else {
        ("", "string", "d", "b")
    };
    format!(
        r#"
pipeline:
  name: combine_correlated_build_collateral
error_handling:
  strategy: continue{error_extra}
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src_drv
    config:
      name: src_drv
      type: csv
      path: drv.csv
      correlation_key: cid{sort_order}
      schema:
        - {{ name: did, type: int }}
        - {{ name: cid, type: {cid_type} }}
        - {{ name: k, type: int }}
        - {{ name: lo, type: int }}
        - {{ name: hi, type: int }}
        - {{ name: div, type: int }}
  - type: source
    name: src_bld
    config:
      name: src_bld
      type: csv
      path: bld.csv
      correlation_key: cid{sort_order}
      schema:
        - {{ name: bid, type: int }}
        - {{ name: cid, type: {cid_type} }}
        - {{ name: k, type: int }}
        - {{ name: v, type: int }}
        - {{ name: base, type: int }}
  - type: combine
    name: enriched
    input:
      {dq}: src_drv
      {bq}: src_bld
    config:
      where: '{predicate}'
      match: first
      on_miss: skip{hint}
      cxl: |
        emit did = {dq}.did
        emit q = {bq}.base / {dq}.div
      propagate_ck: driver
  - type: sink
    name: out
    input: enriched
    config:
      name: out
      type: csv
      path: out.csv
{extra_nodes}"#
    )
}

/// Driver rows `(did, cid, div)`. Every driver matches the one build row
/// under every strategy: `k = 1`, and `lo..=hi` brackets the build's `v`.
fn drivers(rows: &[(i64, &str, i64)]) -> String {
    let mut csv = String::from("did,cid,k,lo,hi,div\n");
    for (did, cid, div) in rows {
        csv.push_str(&format!("{did},{cid},1,1,10,{div}\n"));
    }
    csv
}

/// The single build row, with correlation value `cid`.
fn build(cid: &str) -> String {
    format!("bid,cid,k,v,base\n1,{cid},1,5,{BASE}\n", BASE = BASE as i64)
}

fn run_params() -> PipelineRunParams {
    PipelineRunParams {
        execution_id: "build-collateral".to_string(),
        batch_id: "batch".to_string(),
        pipeline_vars: indexmap::IndexMap::new(),
        shutdown_token: None,
        ..Default::default()
    }
}

/// Assert the compiled Combine selects `strategy` (the `[combine:<tag>]`
/// glyph in `--explain`), so each case exercises the path it names.
fn assert_strategy(yaml: &str, strategy: &Strategy) {
    let config = parse_config(yaml).expect("parse for explain");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile for explain");
    let (dag, _) = PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let explain = dag.explain_text(&config);
    assert!(
        explain.contains(&format!("[combine:{}]", strategy.tag)),
        "expected [combine:{}], got explain:\n{explain}",
        strategy.tag
    );
}

/// One output row, keyed by column name.
type OutputRow = HashMap<String, String>;

/// Run `strategy` over the given drivers and build row. Returns the output
/// rows and the dead-letter rows in written order, after checking that
/// every counted dead letter was written as a row.
fn run(
    strategy: &Strategy,
    driver_rows: &[(i64, &str, i64)],
    build_cid: &str,
) -> (Vec<OutputRow>, Vec<DlqRow>) {
    let yaml = yaml(strategy);
    let (mut out, rows) = run_yaml(&yaml, strategy, driver_rows, build_cid, &["out"]);
    (out.remove("out").unwrap_or_default(), rows)
}

/// Run `yaml`, which must select `strategy`, over the given drivers and
/// build row. Returns each named sink's rows and the dead-letter rows in
/// written order, after checking that every counted dead letter was
/// written as a row.
fn run_yaml(
    yaml: &str,
    strategy: &Strategy,
    driver_rows: &[(i64, &str, i64)],
    build_cid: &str,
    sinks: &[&str],
) -> (HashMap<String, Vec<OutputRow>>, Vec<DlqRow>) {
    assert_strategy(yaml, strategy);
    let config = parse_config(yaml).expect("pipeline parses");
    let readers: SourceReaders = HashMap::from([
        (
            "src_drv".to_string(),
            SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("drv.csv"),
                Box::new(Cursor::new(drivers(driver_rows).into_bytes())),
            )]),
        ),
        (
            "src_bld".to_string(),
            SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("bld.csv"),
                Box::new(Cursor::new(build(build_cid).into_bytes())),
            )]),
        ),
    ]);
    let buffers: Vec<(String, SharedBuffer)> = sinks
        .iter()
        .map(|sink| (sink.to_string(), SharedBuffer::new()))
        .collect();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> = buffers
        .iter()
        .map(|(sink, buf)| (sink.clone(), Box::new(buf.clone()) as _))
        .collect();
    let (report, rows) = dlq_sink::run_config_with_dlq(&config, readers, writers, &run_params())
        .unwrap_or_else(|error| panic!("[{}] pipeline runs: {error:?}", strategy.tag));
    assert_eq!(
        rows.len() as u64,
        report.counters.dlq_count,
        "[{}] every dead letter is written as a row",
        strategy.tag
    );
    let out = buffers
        .into_iter()
        .map(|(sink, buf)| (sink, parse_output(&buf.as_string())))
        .collect();
    (out, rows)
}

/// A sink's CSV text as rows keyed by column name.
fn parse_output(output: &str) -> Vec<OutputRow> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(output.as_bytes());
    let header: Vec<String> = reader
        .headers()
        .map(|h| h.iter().map(str::to_owned).collect())
        .unwrap_or_default();
    reader
        .records()
        .map(|record| {
            let record = record.expect("output row parses");
            header
                .iter()
                .cloned()
                .zip(record.iter().map(str::to_owned))
                .collect()
        })
        .collect()
}

fn id(row: &DlqRow) -> &str {
    row.field("_cxl_dlq_id")
        .expect("every dead-letter header carries _cxl_dlq_id")
}

fn trigger_id(row: &DlqRow) -> &str {
    row.field("_cxl_dlq_trigger_id")
        .expect("every dead-letter header carries _cxl_dlq_trigger_id")
}

fn describe(rows: &[DlqRow]) -> Vec<(String, u64, bool, Option<String>)> {
    rows.iter()
        .map(|row| {
            (
                row.source_name().to_owned(),
                row.source_row(),
                row.trigger(),
                row.category().map(str::to_owned),
            )
        })
        .collect()
}

/// The output row for driver `did`, if it was written.
fn output_for(out: &[OutputRow], did: i64) -> Option<&OutputRow> {
    out.iter()
        .find(|row| row.get("did").map(String::as_str) == Some(did.to_string().as_str()))
}

/// Assert `out` holds driver `did`'s row, with `q` equal to half the
/// build's `base`.
fn assert_clean_output(tag: &str, out: &[OutputRow], did: i64) {
    let row = output_for(out, did)
        .unwrap_or_else(|| panic!("[{tag}] driver {did}'s output is written: {out:?}"));
    let q: f64 = row
        .get("q")
        .unwrap_or_else(|| panic!("[{tag}] the output has a q column: {row:?}"))
        .parse()
        .unwrap_or_else(|_| panic!("[{tag}] q is numeric: {row:?}"));
    assert_eq!(q, BASE / 2.0, "[{tag}] q is half the build's base: {row:?}");
}

/// Assert `trigger` is driver `did`'s `combine_output_row` trigger and
/// `build` is the matched build row, held right after it as that driver's
/// collateral and paired with its failure.
fn assert_driver_then_build(tag: &str, trigger: &DlqRow, build: &DlqRow, did: u64) {
    let combine_output_row = Some(DlqErrorCategory::CombineOutputRow.as_str());
    assert_eq!(
        trigger.source_name(),
        "src_drv",
        "[{tag}] driver row source"
    );
    assert_eq!(
        trigger.source_row(),
        did,
        "[{tag}] the failing driver's row"
    );
    assert_eq!(
        trigger.category(),
        combine_output_row,
        "[{tag}] driver category"
    );
    assert!(trigger.trigger(), "[{tag}] the failing driver is a trigger");
    assert_eq!(
        trigger_id(trigger),
        id(trigger),
        "[{tag}] the driver is its own failure's trigger"
    );

    assert_eq!(
        build.source_name(),
        "src_bld",
        "[{tag}] the build row names the build source"
    );
    assert_eq!(
        build.source_row(),
        1,
        "[{tag}] the build row names its own row"
    );
    assert_eq!(
        build.category(),
        combine_output_row,
        "[{tag}] build category"
    );
    assert_eq!(
        build.stage(),
        Some("combine:enriched"),
        "[{tag}] the build row keeps the Combine stage"
    );
    assert!(
        !build.trigger(),
        "[{tag}] the build row is a collateral of the failing driver's group"
    );
    assert_eq!(
        trigger_id(build),
        id(trigger),
        "[{tag}] the build row pairs with its own driver's failure"
    );
}

/// The build row's `cid` equals the succeeding driver's (`B`), and the
/// failing driver's is `A`. The build row's dead letter follows the failing
/// driver's group `A`, so group `B` stays clean and the succeeding driver's
/// output is written.
#[test]
fn build_collateral_follows_the_failing_driver_group() {
    for strategy in STRATEGIES {
        let tag = strategy.tag;
        let (out, rows) = run(strategy, &[(1, "A", 0), (2, "B", 2)], "B");

        assert_eq!(
            out.len(),
            1,
            "[{tag}] the output holds exactly the succeeding driver's row: {out:?}; dlq: {:?}",
            describe(&rows)
        );
        assert_clean_output(tag, &out, 2);

        assert_eq!(
            rows.len(),
            2,
            "[{tag}] the dead letters are the failing driver and its build row, \
             and no correlated row: {:?}",
            describe(&rows)
        );
        assert!(
            !rows
                .iter()
                .any(|row| row.category() == Some(DlqErrorCategory::Correlated.as_str())),
            "[{tag}] no output is condemned: {:?}",
            describe(&rows)
        );
        assert_driver_then_build(tag, &rows[0], &rows[1], 1);
    }
}

/// Two failing drivers in different groups (`A`, `C`) and one succeeding
/// driver in group `B` all match one build row whose `cid` is `B`. The
/// build row is held once with each failing driver's group, right after
/// that driver's trigger and paired with its failure, and the succeeding
/// driver's output is written.
#[test]
fn build_collateral_is_written_once_per_failing_driver_group() {
    for strategy in STRATEGIES {
        let tag = strategy.tag;
        let (out, rows) = run(strategy, &[(1, "A", 0), (2, "B", 2), (3, "C", 0)], "B");

        assert_eq!(
            out.len(),
            1,
            "[{tag}] the output holds exactly the succeeding driver's row: {out:?}; dlq: {:?}",
            describe(&rows)
        );
        assert_clean_output(tag, &out, 2);

        assert_eq!(
            rows.len(),
            4,
            "[{tag}] each failing driver is followed by the build row, \
             and no correlated row: {:?}",
            describe(&rows)
        );
        let mut failing_drivers: Vec<u64> = Vec::new();
        for pair in rows.chunks(2) {
            assert_driver_then_build(tag, &pair[0], &pair[1], pair[0].source_row());
            failing_drivers.push(pair[0].source_row());
        }
        failing_drivers.sort_unstable();
        assert_eq!(
            failing_drivers,
            vec![1, 3],
            "[{tag}] each failing driver's group holds the build row once: {:?}",
            describe(&rows)
        );
    }
}

/// Guard: group atomicity. Both drivers share `cid = A` and the build row's
/// is `C`. The first driver fails, so group `A` is condemned whole, and the
/// second driver's successful output is dead-lettered as a `correlated` row
/// of that group, whatever happens to the build row.
#[test]
fn failing_driver_still_condemns_its_own_group() {
    for strategy in STRATEGIES {
        let tag = strategy.tag;
        let (out, rows) = run(strategy, &[(1, "A", 0), (2, "A", 2)], "C");

        assert!(
            output_for(&out, 2).is_none(),
            "[{tag}] the second driver's output is condemned with group A: {out:?}"
        );
        let trigger = rows
            .iter()
            .find(|row| row.source_name() == "src_drv" && row.trigger())
            .unwrap_or_else(|| {
                panic!(
                    "[{tag}] the failing driver is group A's trigger: {:?}",
                    describe(&rows)
                )
            });
        assert_eq!(trigger.source_row(), 1, "[{tag}] the failing driver's row");
        let correlated: Vec<&DlqRow> = rows
            .iter()
            .filter(|row| row.category() == Some(DlqErrorCategory::Correlated.as_str()))
            .collect();
        assert_eq!(
            correlated.len(),
            1,
            "[{tag}] exactly the second driver's output is condemned: {:?}",
            describe(&rows)
        );
        let condemned = correlated[0];
        assert_eq!(
            condemned.field("did"),
            Some("2"),
            "[{tag}] the condemned row is the second driver's output"
        );
        assert!(
            !condemned.trigger(),
            "[{tag}] the condemned output is a collateral"
        );
        assert_eq!(
            trigger_id(condemned),
            id(trigger),
            "[{tag}] the condemned output pairs with group A's failing driver"
        );
    }
}

/// Two failing drivers in the same group (`A`, or `1` for sort-merge) match
/// one build row in another group (`B`, or `2`). Each failure writes its
/// own copy of the build row: the build row is written once per failing
/// driver, right after that driver's trigger and paired with its failure,
/// not once per group.
#[test]
fn build_row_is_written_once_per_failing_driver_in_one_group() {
    for strategy in STRATEGIES.iter().chain(std::iter::once(&SORT_MERGE)) {
        let tag = strategy.tag;
        let (group, build_group) = if strategy.sorted_on_key {
            ("1", "2")
        } else {
            ("A", "B")
        };
        let (out, rows) = run(strategy, &[(1, group, 0), (2, group, 0)], build_group);

        assert!(
            out.is_empty(),
            "[{tag}] both drivers failed, so no output is written: {out:?}"
        );
        assert_eq!(
            rows.len(),
            4,
            "[{tag}] each failing driver is followed by its own copy of the build row, \
             and no correlated row: {:?}",
            describe(&rows)
        );
        for (pair, did) in rows.chunks(2).zip([1, 2]) {
            assert_driver_then_build(tag, &pair[0], &pair[1], did);
        }
        assert_ne!(
            trigger_id(&rows[1]),
            trigger_id(&rows[3]),
            "[{tag}] the two build rows pair with different failures: {:?}",
            describe(&rows)
        );
    }
}

/// Every strategy under test, sort-merge included.
fn all_strategies() -> impl Iterator<Item = &'static Strategy> {
    STRATEGIES.iter().chain(std::iter::once(&SORT_MERGE))
}

/// A driver group and a build group whose value sorts after it, spelled for
/// `strategy`'s `cid` type.
fn driver_and_build_groups(strategy: &Strategy) -> (&'static str, &'static str) {
    if strategy.sorted_on_key {
        ("1", "2")
    } else {
        ("A", "B")
    }
}

/// The build Source also feeds an Aggregate grouped by `k`, which omits the
/// correlation key, so the relaxed-key retraction runs at commit.
const BUILD_AGGREGATE: &str = r#"  - type: aggregate
    name: bld_totals
    input: src_bld
    config:
      group_by: [k]
      cxl: |
        emit k = k
        emit total = sum(base)
  - type: sink
    name: agg_out
    input: bld_totals
    config:
      name: agg_out
      type: csv
      path: agg_out.csv
"#;

/// A relaxed-key retraction takes back only rows that failed. The build row
/// held with a failing driver's group did not fail, so the Aggregate over
/// the build Source keeps its contribution: its total is the build row's
/// `base`.
#[test]
fn build_row_held_with_a_failure_is_not_retracted_from_a_relaxed_aggregate() {
    for strategy in all_strategies() {
        let tag = strategy.tag;
        let (group, build_group) = driver_and_build_groups(strategy);
        let yaml = yaml_with(strategy, "", BUILD_AGGREGATE);
        let (out, rows) = run_yaml(
            &yaml,
            strategy,
            &[(1, group, 0)],
            build_group,
            &["out", "agg_out"],
        );
        assert_eq!(
            rows.len(),
            2,
            "[{tag}] the failing driver and its build row: {:?}",
            describe(&rows)
        );
        assert_driver_then_build(tag, &rows[0], &rows[1], 1);
        let totals = &out["agg_out"];
        assert_eq!(
            totals.len(),
            1,
            "[{tag}] the build Source's one group is aggregated: {totals:?}"
        );
        let total: f64 = totals[0]
            .get("total")
            .unwrap_or_else(|| panic!("[{tag}] the aggregate has a total: {totals:?}"))
            .parse()
            .unwrap_or_else(|_| panic!("[{tag}] total is numeric: {totals:?}"));
        assert_eq!(
            total, BASE,
            "[{tag}] the build row's contribution is not retracted: {totals:?}"
        );
    }
}

/// `max_group_buffer` counts a failure once. One failing and one succeeding
/// driver share a group under a cap of 2: the failure and the succeeding
/// driver's output are the group's two held entries. The build row held
/// with the failure is part of that failure, not a third entry, so the
/// group does not overflow and the succeeding driver's output is written as
/// an ordinary `correlated` row, not a `group_size_exceeded` one.
#[test]
fn build_row_held_with_a_failure_is_not_a_second_group_entry() {
    for strategy in all_strategies() {
        let tag = strategy.tag;
        let (group, build_group) = driver_and_build_groups(strategy);
        let yaml = yaml_with(strategy, "\n  max_group_buffer: 2", "");
        let (_, rows) = run_yaml(
            &yaml,
            strategy,
            &[(1, group, 0), (2, group, 2)],
            build_group,
            &["out"],
        );
        let categories: Vec<Option<&str>> = rows.iter().map(DlqRow::category).collect();
        assert!(
            !categories.contains(&Some(DlqErrorCategory::GroupSizeExceeded.as_str())),
            "[{tag}] the group holds two entries under a cap of 2 and does not overflow: {:?}",
            describe(&rows)
        );
        assert_eq!(
            rows.len(),
            3,
            "[{tag}] the failing driver, its build row, and the succeeding driver's \
             condemned output: {:?}",
            describe(&rows)
        );
        assert_driver_then_build(tag, &rows[0], &rows[1], 1);
        assert_eq!(
            rows[2].category(),
            Some(DlqErrorCategory::Correlated.as_str()),
            "[{tag}] the succeeding driver's output is condemned by the failure: {:?}",
            describe(&rows)
        );
        assert_eq!(
            trigger_id(&rows[2]),
            id(&rows[0]),
            "[{tag}] the condemned output pairs with the failing driver"
        );
    }
}
