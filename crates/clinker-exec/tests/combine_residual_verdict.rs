//! A Combine residual that fails to evaluate is neither a match nor a miss.
//!
//! For one driver, each key-matching build row gets exactly one predicate
//! outcome from the residual (the part of `where:` the join keys do not
//! cover): it is true, it is not true (false or null), or it failed to
//! evaluate. A failed evaluation has said nothing about the pair, so:
//!
//! - a driver with no true candidate and at least one failed one is not
//!   unmatched: each failure is dead-lettered and `on_miss` does not fire;
//! - `match: first` stops at the first candidate, in build arrival order,
//!   whose outcome is not "not true": a failure there is the driver's only
//!   result, and a candidate after a true one is never part of it;
//! - `match: collect` writes no row for a driver any of whose candidates
//!   failed;
//! - `match: all` keeps every true pair next to every failed one, and a
//!   driver whose bodies all skip or fail still matched.
//!
//! Every case runs on every join strategy, with and without a correlation
//! key, and asserts through `--explain` that the strategy it names is the one
//! selected. The residual reads both inputs, `src_drv.hi / src_bld.base > 1`
//! with `hi = 10`: a build row's `base` of 5 makes it true, 20 makes it not
//! true, and 0 makes it fail. Build rows arrive with ids 1, 2, 3 in that
//! order.

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_core_types::dlq::DlqErrorCategory;
use clinker_exec::executor::{
    ExecutionReport, PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders,
};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};
use clinker_plan::error::PipelineError;

use dlq_sink::DlqRow;

/// A build row whose residual is true.
const TRUE: i64 = 5;
/// A build row whose residual is false, so not a match.
const NOT_TRUE: i64 = 20;
/// A build row whose residual fails to evaluate (division by zero).
const FAILS: i64 = 0;

/// One physical join strategy: its `--explain` tag and its `where:` clause,
/// residual included, and an optional strategy hint. The key part of every
/// predicate matches every build row for every driver.
struct Strategy {
    tag: &'static str,
    predicate: &'static str,
    hint: Option<&'static str>,
}

const STRATEGIES: &[Strategy] = &[
    Strategy {
        tag: "hash_build_probe",
        predicate: "src_drv.k == src_bld.k and src_drv.hi / src_bld.base > 1",
        hint: None,
    },
    Strategy {
        tag: "grace_hash",
        predicate: "src_drv.k == src_bld.k and src_drv.hi / src_bld.base > 1",
        hint: Some("grace_hash"),
    },
    Strategy {
        tag: "hash_partition_iejoin",
        predicate: "src_drv.k == src_bld.k and src_drv.lo <= src_bld.v \
                    and src_drv.hi / src_bld.base > 1",
        hint: None,
    },
    Strategy {
        tag: "iejoin",
        predicate: "src_drv.lo <= src_bld.v and src_drv.hi >= src_bld.v \
                    and src_drv.hi / src_bld.base > 1",
        hint: None,
    },
    Strategy {
        tag: "sort_merge",
        predicate: "src_drv.cid <= src_bld.cid and src_drv.hi / src_bld.base > 1",
        hint: None,
    },
];

/// The Combine body. `Enrich` emits the driver and the build id; `Skip`
/// filters every matched row out but keeps an `on_miss: null_fields` row,
/// whose build fields are null, so a driver wrongly routed to `on_miss`
/// shows in the output; `Fail` divides by the driver's `div`,
/// which the failing drivers set to 0; `Collect` is the empty body a
/// `match: collect` Combine takes.
#[derive(Clone, Copy)]
enum Body {
    Enrich,
    Skip,
    Fail,
    Collect,
}

impl Body {
    fn cxl(self) -> &'static str {
        match self {
            Body::Enrich => "|\n        emit did = src_drv.did\n        emit bid = src_bld.bid",
            Body::Skip => {
                "|\n        filter (src_bld.bid ?? -1) < 0\n        emit did = src_drv.did\n        \
                 emit bid = src_bld.bid"
            }
            Body::Fail => {
                "|\n        emit did = src_drv.did\n        emit bid = src_bld.bid\n        \
                 emit q = src_bld.base / src_drv.div"
            }
            Body::Collect => "\"\"",
        }
    }
}

/// One case: the Combine's `match` and `on_miss`, its body, the error
/// strategy, an optional `max_output_rows`, the drivers' `div` values (one
/// driver per value, `did` 1..), and the build rows' `base` values in
/// arrival order.
struct Case<'a> {
    match_mode: &'a str,
    on_miss: &'a str,
    body: Body,
    fail_fast: bool,
    max_output_rows: Option<u64>,
    drivers: &'a [i64],
    bases: &'a [i64],
}

impl<'a> Case<'a> {
    fn new(match_mode: &'a str, bases: &'a [i64]) -> Self {
        Self {
            match_mode,
            on_miss: "skip",
            body: if match_mode == "collect" {
                Body::Collect
            } else {
                Body::Enrich
            },
            fail_fast: false,
            max_output_rows: None,
            drivers: &[2],
            bases,
        }
    }

    fn on_miss(mut self, on_miss: &'a str) -> Self {
        self.on_miss = on_miss;
        self
    }

    fn body(mut self, body: Body) -> Self {
        self.body = body;
        self
    }

    fn drivers(mut self, drivers: &'a [i64]) -> Self {
        self.drivers = drivers;
        self
    }

    fn fail_fast(mut self) -> Self {
        self.fail_fast = true;
        self
    }

    fn max_output_rows(mut self, cap: u64) -> Self {
        self.max_output_rows = Some(cap);
        self
    }
}

fn yaml(strategy: &Strategy, case: &Case<'_>, keyed: bool) -> String {
    let hint = strategy
        .hint
        .map(|hint| format!("\n      strategy: {hint}"))
        .unwrap_or_default();
    let cap = case
        .max_output_rows
        .map(|cap| format!("\n      max_output_rows: {cap}"))
        .unwrap_or_default();
    let key = if keyed {
        "\n      correlation_key: cid"
    } else {
        ""
    };
    let error_strategy = if case.fail_fast {
        "fail_fast"
    } else {
        "continue"
    };
    let predicate = strategy.predicate;
    let match_mode = case.match_mode;
    let on_miss = case.on_miss;
    let body = case.body.cxl();
    format!(
        r#"
pipeline:
  name: combine_residual_verdict
error_handling:
  strategy: {error_strategy}
  dlq:
    path: rejected.csv
nodes:
  - type: source
    name: src_drv
    config:
      name: src_drv
      type: csv
      path: drv.csv{key}
      sort_order:
        - field: cid
      schema:
        - {{ name: did, type: int }}
        - {{ name: cid, type: int }}
        - {{ name: k, type: int }}
        - {{ name: lo, type: int }}
        - {{ name: hi, type: int }}
        - {{ name: div, type: int }}
  - type: source
    name: src_bld
    config:
      name: src_bld
      type: csv
      path: bld.csv{key}
      sort_order:
        - field: cid
      schema:
        - {{ name: bid, type: int }}
        - {{ name: cid, type: int }}
        - {{ name: k, type: int }}
        - {{ name: v, type: int }}
        - {{ name: base, type: int }}
  - type: combine
    name: joined
    input:
      src_drv: src_drv
      src_bld: src_bld
    config:
      where: '{predicate}'
      match: {match_mode}
      on_miss: {on_miss}{hint}{cap}
      cxl: {body}
      propagate_ck: driver
  - type: sink
    name: out
    input: joined
    config:
      name: out
      type: json
      options:
        format: ndjson
      path: out.ndjson
      include_unmapped: true
"#
    )
}

fn assert_strategy(yaml: &str, strategy: &Strategy, label: &str) {
    let config = parse_config(yaml).expect("parse for explain");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile for explain");
    let (dag, _) = PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let explain = dag.explain_text(&config);
    assert!(
        explain.contains(&format!("[combine:{}]", strategy.tag)),
        "[{label}] expected [combine:{}], got explain:\n{explain}",
        strategy.tag
    );
}

/// What one run wrote.
struct Run {
    report: ExecutionReport,
    out: Vec<serde_json::Value>,
    dlq: Vec<DlqRow>,
}

fn try_run(strategy: &Strategy, case: &Case<'_>, keyed: bool) -> Result<Run, PipelineError> {
    let label = label(strategy, case, keyed);
    let yaml = yaml(strategy, case, keyed);
    assert_strategy(&yaml, strategy, &label);
    let config = parse_config(&yaml).expect("pipeline parses");
    let mut drivers = String::from("did,cid,k,lo,hi,div\n");
    for (n, div) in case.drivers.iter().enumerate() {
        drivers.push_str(&format!("{},1,1,1,10,{div}\n", n + 1));
    }
    let mut builds = String::from("bid,cid,k,v,base\n");
    for (n, base) in case.bases.iter().enumerate() {
        builds.push_str(&format!("{},1,1,5,{base}\n", n + 1));
    }
    let readers: SourceReaders = HashMap::from([
        (
            "src_drv".to_string(),
            SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("drv.csv"),
                Box::new(Cursor::new(drivers.into_bytes())),
            )]),
        ),
        (
            "src_bld".to_string(),
            SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("bld.csv"),
                Box::new(Cursor::new(builds.into_bytes())),
            )]),
        ),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(buf.clone()) as _)]);
    let params = PipelineRunParams {
        execution_id: "combine-residual-verdict".to_string(),
        batch_id: "batch".to_string(),
        ..Default::default()
    };
    let (report, dlq) = dlq_sink::run_config_with_dlq(&config, readers, writers, &params)?;
    assert_eq!(
        dlq.len() as u64,
        report.counters.dlq_count,
        "[{label}] every dead letter is written as a row"
    );
    dlq_sink::assert_pairing_integrity(&dlq);
    let out = buf
        .as_string()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("an NDJSON output row"))
        .collect();
    Ok(Run { report, out, dlq })
}

fn run(strategy: &Strategy, case: &Case<'_>, keyed: bool) -> Run {
    try_run(strategy, case, keyed).unwrap_or_else(|error| {
        panic!(
            "[{}] the run completes: {error}",
            label(strategy, case, keyed)
        )
    })
}

fn label(strategy: &Strategy, case: &Case<'_>, keyed: bool) -> String {
    format!(
        "{} match: {} on_miss: {} bases {:?} keyed={keyed}",
        strategy.tag, case.match_mode, case.on_miss, case.bases
    )
}

/// Every strategy, keyless and keyed.
fn each_run(mut check: impl FnMut(&Strategy, bool)) {
    for strategy in STRATEGIES {
        for keyed in [false, true] {
            check(strategy, keyed);
        }
    }
}

fn int(row: &serde_json::Value, field: &str) -> i64 {
    row.get(field)
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| panic!("output row has integer {field}: {row}"))
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

/// The failures in `rows`, excluding rows a failing correlation group
/// condemned: each trigger's driver row and the build row written after it.
fn failures(rows: &[DlqRow]) -> Vec<(u64, Option<u64>)> {
    let correlated = Some(DlqErrorCategory::Correlated.as_str());
    let kept: Vec<&DlqRow> = rows
        .iter()
        .filter(|row| row.category() != correlated)
        .collect();
    let mut out = Vec::new();
    let mut n = 0;
    while n < kept.len() {
        let trigger = kept[n];
        assert!(trigger.trigger(), "a failure starts with its trigger row");
        assert_eq!(
            trigger.source_name(),
            "src_drv",
            "the trigger is a driver row"
        );
        assert_eq!(
            trigger.category(),
            Some(DlqErrorCategory::CombineOutputRow.as_str()),
            "a residual failure is a combine output-row failure"
        );
        let build = kept
            .get(n + 1)
            .filter(|row| !row.trigger() && row.source_name() == "src_bld");
        out.push((trigger.source_row(), build.map(|row| row.source_row())));
        n += if build.is_some() { 2 } else { 1 };
    }
    out
}

/// Assert `run` wrote no output row, and dead-lettered exactly `expected`
/// failures as `(driver row, build row)` pairs.
fn assert_only_failures(run: &Run, expected: &[(u64, Option<u64>)], label: &str) {
    assert!(
        run.out.is_empty(),
        "[{label}] the driver writes no output row: {:?}; dlq: {:?}",
        run.out,
        describe(&run.dlq)
    );
    assert_eq!(
        failures(&run.dlq),
        expected,
        "[{label}] the dead letters are exactly the failed candidates: {:?}",
        describe(&run.dlq)
    );
}

/// A `where:` conjunct that is neither an equality nor a range still
/// filters the pairs on every strategy, next to one or two range conjuncts
/// as well as without any: the build row whose conjunct is false is no
/// match.
#[test]
fn a_non_range_conjunct_filters_on_every_strategy() {
    each_run(|strategy, keyed| {
        let case = Case::new("all", &[NOT_TRUE, TRUE]);
        let label = label(strategy, &case, keyed);
        let run = run(strategy, &case, keyed);
        let picked: Vec<(i64, i64)> = run
            .out
            .iter()
            .map(|row| (int(row, "did"), int(row, "bid")))
            .collect();
        assert_eq!(
            picked,
            [(1, 2)],
            "[{label}] only the build row whose conjunct is true matches"
        );
        assert!(run.dlq.is_empty(), "[{label}] {:?}", describe(&run.dlq));
    });
}

// T1: an error is not a miss.

/// One driver, one candidate whose residual fails: whatever `on_miss` says,
/// the driver is dead-lettered with that build row, writes no output row and
/// the run completes. `on_miss: error` does not abort with E319, and
/// `null_fields` adds no null-filled row.
#[test]
fn a_failing_residual_is_not_a_miss_for_any_on_miss() {
    for match_mode in ["all", "first"] {
        for on_miss in ["skip", "null_fields", "error"] {
            each_run(|strategy, keyed| {
                let case = Case::new(match_mode, &[FAILS]).on_miss(on_miss);
                let label = label(strategy, &case, keyed);
                let run = try_run(strategy, &case, keyed).unwrap_or_else(|error| {
                    panic!("[{label}] a residual failure must not abort the run: {error}")
                });
                assert_only_failures(&run, &[(1, Some(1))], &label);
            });
        }
    }
}

/// Candidates `[not true, failed]`: the not-true candidate is no match and
/// the failed one is dead-lettered, so the driver is still not unmatched.
#[test]
fn a_not_true_candidate_beside_a_failed_one_is_not_a_miss() {
    for match_mode in ["all", "first"] {
        each_run(|strategy, keyed| {
            let case = Case::new(match_mode, &[NOT_TRUE, FAILS]).on_miss("null_fields");
            let label = label(strategy, &case, keyed);
            let run = run(strategy, &case, keyed);
            assert_only_failures(&run, &[(1, Some(2))], &label);
        });
    }
}

/// Three failed candidates under `match: all`: three failure rows, each with
/// its own build row, and no `on_miss`.
#[test]
fn every_failed_candidate_is_dead_lettered_under_all() {
    each_run(|strategy, keyed| {
        let case = Case::new("all", &[FAILS, FAILS, FAILS]).on_miss("error");
        let label = label(strategy, &case, keyed);
        let run = run(strategy, &case, keyed);
        assert_only_failures(&run, &[(1, Some(1)), (1, Some(2)), (1, Some(3))], &label);
    });
}

// T2: `match: first` stops at the first candidate that is not "not true".

/// Three failed candidates under `match: first`: the first one decides, so
/// the driver writes exactly one failure, for the earliest build row.
#[test]
fn first_writes_only_the_deciding_failure() {
    each_run(|strategy, keyed| {
        let case = Case::new("first", &[FAILS, FAILS, FAILS]).on_miss("error");
        let label = label(strategy, &case, keyed);
        let run = run(strategy, &case, keyed);
        assert_only_failures(&run, &[(1, Some(1))], &label);
    });
}

/// Candidates `[failed, true]`: the failure decides, so the driver is
/// dead-lettered and a later match is never picked.
#[test]
fn first_stops_at_a_failing_candidate() {
    each_run(|strategy, keyed| {
        let case = Case::new("first", &[FAILS, TRUE]);
        let label = label(strategy, &case, keyed);
        let run = run(strategy, &case, keyed);
        assert_only_failures(&run, &[(1, Some(1))], &label);
    });
}

/// Candidates `[true, failed]`: the true candidate decides, so the driver is
/// enriched with it and the later candidate's failure is never written.
#[test]
fn first_ignores_a_failure_after_its_match() {
    each_run(|strategy, keyed| {
        let case = Case::new("first", &[TRUE, FAILS]);
        let label = label(strategy, &case, keyed);
        let run = run(strategy, &case, keyed);
        assert!(
            run.dlq.is_empty(),
            "[{label}] no candidate after the match is part of the result: {:?}",
            describe(&run.dlq)
        );
        let picked: Vec<(i64, i64)> = run
            .out
            .iter()
            .map(|row| (int(row, "did"), int(row, "bid")))
            .collect();
        assert_eq!(
            picked,
            [(1, 1)],
            "[{label}] the earliest build row is picked"
        );
    });
}

/// Candidates `[not true, true, true]`: every strategy picks the same row,
/// the earliest build row whose residual is true.
#[test]
fn first_picks_the_earliest_true_candidate_on_every_strategy() {
    each_run(|strategy, keyed| {
        let case = Case::new("first", &[NOT_TRUE, TRUE, TRUE]);
        let label = label(strategy, &case, keyed);
        let run = run(strategy, &case, keyed);
        assert!(run.dlq.is_empty(), "[{label}] {:?}", describe(&run.dlq));
        let picked: Vec<(i64, i64)> = run
            .out
            .iter()
            .map(|row| (int(row, "did"), int(row, "bid")))
            .collect();
        assert_eq!(
            picked,
            [(1, 2)],
            "[{label}] the earliest true build row is picked"
        );
    });
}

// T3: `match: collect` writes no row for a driver with a failed candidate.

/// Candidates `[true, failed]`: the array is unknown, so no row is written,
/// only the failure.
#[test]
fn collect_writes_no_row_for_a_driver_with_a_failed_candidate() {
    each_run(|strategy, keyed| {
        let case = Case::new("collect", &[TRUE, FAILS]);
        let label = label(strategy, &case, keyed);
        let run = run(strategy, &case, keyed);
        assert_only_failures(&run, &[(1, Some(2))], &label);
    });
}

/// Candidates `[failed]`: no empty-array row, only the failure.
#[test]
fn collect_writes_no_empty_array_for_a_failed_candidate() {
    each_run(|strategy, keyed| {
        let case = Case::new("collect", &[FAILS]);
        let label = label(strategy, &case, keyed);
        let run = run(strategy, &case, keyed);
        assert_only_failures(&run, &[(1, Some(1))], &label);
    });
}

// T4: `match: all` is decided per pair.

/// Candidates `[true, failed]`: the true pair is written and the failed one
/// dead-lettered.
#[test]
fn all_keeps_a_true_pair_beside_a_failed_one() {
    for strategy in STRATEGIES {
        let case = Case::new("all", &[TRUE, FAILS]);
        let label = label(strategy, &case, false);
        let run = run(strategy, &case, false);
        let picked: Vec<(i64, i64)> = run
            .out
            .iter()
            .map(|row| (int(row, "did"), int(row, "bid")))
            .collect();
        assert_eq!(picked, [(1, 1)], "[{label}] the true pair is written");
        assert_eq!(
            failures(&run.dlq),
            [(1, Some(2))],
            "[{label}] the failed pair is dead-lettered: {:?}",
            describe(&run.dlq)
        );
    }
}

/// Every matched body skips: the driver matched, so `on_miss` does not fire
/// and no null-filled row is written.
#[test]
fn all_with_every_body_skipping_is_not_a_miss() {
    each_run(|strategy, keyed| {
        let case = Case::new("all", &[TRUE, TRUE])
            .on_miss("null_fields")
            .body(Body::Skip);
        let label = label(strategy, &case, keyed);
        let run = run(strategy, &case, keyed);
        assert!(
            run.out.is_empty(),
            "[{label}] a body skip drops its row and adds no on_miss row: {:?}",
            run.out
        );
        assert!(run.dlq.is_empty(), "[{label}] {:?}", describe(&run.dlq));
    });
}

/// Every matched body fails under `on_miss: error`: each body failure is
/// dead-lettered and the run does not abort with E319.
#[test]
fn all_with_every_body_failing_is_not_a_miss() {
    each_run(|strategy, keyed| {
        let case = Case::new("all", &[TRUE, TRUE])
            .on_miss("error")
            .body(Body::Fail)
            .drivers(&[0]);
        let label = label(strategy, &case, keyed);
        let run = try_run(strategy, &case, keyed).unwrap_or_else(|error| {
            panic!("[{label}] a body failure must not abort the run: {error}")
        });
        assert_only_failures(&run, &[(1, Some(1)), (1, Some(2))], &label);
    });
}

// T5: keyed runs write the keyless failure rows.

/// The keyed run writes the same failures as the keyless run, and none of
/// the failing driver's group reaches the Sink.
#[test]
fn keyed_runs_write_the_keyless_failures() {
    let cases = [
        Case::new("all", &[NOT_TRUE, FAILS, TRUE]).on_miss("null_fields"),
        Case::new("first", &[FAILS, TRUE]).on_miss("null_fields"),
        Case::new("collect", &[TRUE, FAILS]),
    ];
    for strategy in STRATEGIES {
        for case in &cases {
            let keyless = run(strategy, case, false);
            let keyed = run(strategy, case, true);
            let label = label(strategy, case, true);
            assert_eq!(
                failures(&keyed.dlq),
                failures(&keyless.dlq),
                "[{label}] the keyed run writes the keyless failures: {:?} vs {:?}",
                describe(&keyed.dlq),
                describe(&keyless.dlq)
            );
            assert!(
                keyed.out.is_empty(),
                "[{label}] the failing driver's group reaches no Sink: {:?}",
                keyed.out
            );
        }
    }
}

// T6: `fail_fast`.

/// Under `fail_fast` a failing residual aborts the run with its evaluation
/// error, never with E319.
#[test]
fn fail_fast_reports_the_evaluation_error_not_a_missing_match() {
    for match_mode in ["all", "first", "collect"] {
        for strategy in STRATEGIES {
            let case = Case::new(match_mode, &[FAILS]).on_miss("error").fail_fast();
            let label = label(strategy, &case, false);
            let error = match try_run(strategy, &case, false) {
                Ok(_) => panic!("[{label}] a fail_fast run with a failing residual aborts"),
                Err(error) => error.to_string(),
            };
            assert!(
                !error.contains("E319"),
                "[{label}] the abort is the evaluation error, not a missing match: {error}"
            );
        }
    }
}

// T7: counters.

/// A driver whose only candidate failed reaches no output and consumes no
/// `max_output_rows` allowance: two such drivers under a cap of one write
/// nothing, so the cap is never reached and the run completes.
#[test]
fn a_failed_driver_consumes_no_output_allowance() {
    for strategy in STRATEGIES {
        let case = Case::new("first", &[FAILS])
            .on_miss("null_fields")
            .drivers(&[2, 2])
            .max_output_rows(1);
        let label = label(strategy, &case, false);
        let run = try_run(strategy, &case, false).unwrap_or_else(|error| {
            panic!("[{label}] no output row is written, so no cap is reached: {error}")
        });
        assert!(run.out.is_empty(), "[{label}] {:?}", run.out);
        assert_eq!(
            run.report.counters.records_written, 0,
            "[{label}] no record is written for a dead-lettered driver"
        );
        assert_eq!(
            run.report.counters.ok_count, 0,
            "[{label}] a dead-lettered driver is not an ok record"
        );
    }
}

// T8: N-ary decomposition.

/// A decomposed three-input Combine whose first step is a pure-range IEJoin
/// carries the whole predicate's residual, which names an input that step
/// has not joined. The step must not evaluate it, or the unjoined input's
/// fields read as null and every driver silently misses.
#[test]
fn a_pure_range_step_does_not_evaluate_a_residual_naming_a_later_input() {
    let yaml = r#"
pipeline:
  name: nary_range_step
nodes:
  - type: source
    name: a
    config:
      name: a
      type: csv
      path: a.csv
      schema:
        - { name: id, type: int }
        - { name: x, type: int }
        - { name: k, type: int }
  - type: source
    name: b
    config:
      name: b
      type: csv
      path: b.csv
      schema:
        - { name: bid, type: int }
        - { name: y, type: int }
  - type: source
    name: c
    config:
      name: c
      type: csv
      path: c.csv
      schema:
        - { name: k, type: int }
        - { name: v, type: int }
  - type: combine
    name: joined
    input:
      a: a
      b: b
      c: c
    config:
      where: 'a.x < b.y and a.k == c.k and c.v > 0'
      match: all
      on_miss: skip
      cxl: |
        emit id = a.id
        emit bid = b.bid
      propagate_ck: driver
  - type: sink
    name: out
    input: joined
    config:
      name: out
      type: json
      options:
        format: ndjson
      path: out.ndjson
"#;
    let config = parse_config(yaml).expect("pipeline parses");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile for explain");
    let (dag, _) = PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let explain = dag.explain_text(&config);
    assert!(
        explain.contains("[combine:iejoin]"),
        "the first step is a pure-range IEJoin, got explain:\n{explain}"
    );
    let file = |name: &str, body: &str| {
        SourceInput::Files(vec![FileSlot::new(
            PathBuf::from(name),
            Box::new(Cursor::new(body.as_bytes().to_vec())),
        )])
    };
    let readers: SourceReaders = HashMap::from([
        ("a".to_string(), file("a.csv", "id,x,k\n1,1,1\n2,5,1\n")),
        ("b".to_string(), file("b.csv", "bid,y\n1,3\n2,10\n")),
        ("c".to_string(), file("c.csv", "k,v\n1,7\n")),
    ]);
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn std::io::Write + Send>> =
        HashMap::from([("out".to_string(), Box::new(buf.clone()) as _)]);
    let params = PipelineRunParams {
        execution_id: "nary-range-step".to_string(),
        batch_id: "batch".to_string(),
        ..Default::default()
    };
    let (_, dlq) = dlq_sink::run_config_with_dlq(&config, readers, writers, &params)
        .expect("the run completes");
    assert!(dlq.is_empty(), "no pair fails: {dlq:?}");
    let mut pairs: Vec<(i64, i64)> = buf
        .as_string()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let row: serde_json::Value = serde_json::from_str(line).expect("an NDJSON row");
            (int(&row, "id"), int(&row, "bid"))
        })
        .collect();
    pairs.sort_unstable();
    assert_eq!(pairs, [(1, 1), (1, 2), (2, 2)]);
}
