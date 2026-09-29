//! A Combine's candidate order is the build input's arrival order, on every
//! join strategy.
//!
//! For one driver, the key-matching build rows are ordered by the position
//! the build input delivered them in. `match: first` picks the earliest of
//! them, `match: all` emits its rows in that order, and `match: collect`
//! builds its array in that order. The same input therefore gives the same
//! output whichever join strategy the planner picks, with and without a
//! correlation key, and whatever shape the `where:` predicate has.
//!
//! The build rows arrive with ids 30, 10, 20, so arrival order differs from
//! both id order and newest-first order.

#[path = "common/dlq_sink.rs"]
mod dlq_sink;

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceInput, SourceReaders};
use clinker_exec::source::multi_file::FileSlot;
use clinker_plan::config::{CompileContext, parse_config};

/// Build row ids in the order the build input delivers them.
const ARRIVAL: [i64; 3] = [30, 10, 20];

/// One physical join strategy: its `--explain` tag, its `where:` clause and
/// an optional strategy hint. Every predicate matches every build row for
/// every driver.
struct Strategy {
    tag: &'static str,
    predicate: &'static str,
    hint: Option<&'static str>,
}

const STRATEGIES: &[Strategy] = &[
    Strategy {
        tag: "hash_build_probe",
        predicate: "src_drv.k == src_bld.k",
        hint: None,
    },
    Strategy {
        tag: "grace_hash",
        predicate: "src_drv.k == src_bld.k",
        hint: Some("grace_hash"),
    },
    // The same equality with a range conjunct every build row satisfies:
    // the planner picks the hash-partitioned IEJoin.
    Strategy {
        tag: "hash_partition_iejoin",
        predicate: "src_drv.k == src_bld.k and src_drv.lo <= src_bld.v",
        hint: None,
    },
    Strategy {
        tag: "iejoin",
        predicate: "src_drv.lo <= src_bld.v and src_drv.hi >= src_bld.v",
        hint: None,
    },
    // A single range over the ascending-sorted correlation column keeps the
    // sort licence, so the planner picks sort-merge.
    Strategy {
        tag: "sort_merge",
        predicate: "src_drv.cid <= src_bld.cid",
        hint: None,
    },
];

/// The pipeline for `strategy` and `match_mode`, with or without a
/// correlation key. Both Sources declare `sort_order` on `cid`, which every
/// row shares, so the declared order leaves arrival order as it is.
fn yaml(strategy: &Strategy, match_mode: &str, keyed: bool) -> String {
    let hint = strategy
        .hint
        .map(|hint| format!("\n      strategy: {hint}"))
        .unwrap_or_default();
    let key = if keyed {
        "\n      correlation_key: cid"
    } else {
        ""
    };
    let body = if match_mode == "collect" {
        "\"\"".to_string()
    } else {
        "|\n        emit did = src_drv.did\n        emit bid = src_bld.bid".to_string()
    };
    let predicate = strategy.predicate;
    format!(
        r#"
pipeline:
  name: combine_build_arrival_order
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
  - type: combine
    name: joined
    input:
      src_drv: src_drv
      src_bld: src_bld
    config:
      where: '{predicate}'
      match: {match_mode}
      on_miss: skip{hint}
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

/// Run `strategy` and return every output row, in written order.
fn run(strategy: &Strategy, match_mode: &str, keyed: bool) -> Vec<serde_json::Value> {
    let label = format!("{} {match_mode} keyed={keyed}", strategy.tag);
    let yaml = yaml(strategy, match_mode, keyed);
    assert_strategy(&yaml, strategy, &label);
    let config = parse_config(&yaml).expect("pipeline parses");
    let drivers = "did,cid,k,lo,hi\n1,1,1,1,10\n2,1,1,1,10\n";
    let mut builds = String::from("bid,cid,k,v\n");
    for bid in ARRIVAL {
        builds.push_str(&format!("{bid},1,1,5\n"));
    }
    let readers: SourceReaders = HashMap::from([
        (
            "src_drv".to_string(),
            SourceInput::Files(vec![FileSlot::new(
                PathBuf::from("drv.csv"),
                Box::new(Cursor::new(drivers.as_bytes().to_vec())),
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
        execution_id: "build-arrival-order".to_string(),
        batch_id: "batch".to_string(),
        ..Default::default()
    };
    dlq_sink::run_config_with_dlq(&config, readers, writers, &params)
        .unwrap_or_else(|error| panic!("[{label}] pipeline runs: {error:?}"));
    buf.as_string()
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("an NDJSON output row"))
        .collect()
}

fn int(row: &serde_json::Value, field: &str) -> i64 {
    row.get(field)
        .and_then(serde_json::Value::as_i64)
        .unwrap_or_else(|| panic!("output row has integer {field}: {row}"))
}

/// The `(did, bid)` pairs of the output, in written order.
fn pairs(rows: &[serde_json::Value]) -> Vec<(i64, i64)> {
    rows.iter()
        .map(|row| (int(row, "did"), int(row, "bid")))
        .collect()
}

/// Every strategy, keyed and keyless.
fn each_run(mut check: impl FnMut(&Strategy, bool, &str)) {
    for strategy in STRATEGIES {
        for keyed in [false, true] {
            let label = format!("{} keyed={keyed}", strategy.tag);
            check(strategy, keyed, &label);
        }
    }
}

/// `match: first` picks the build row that arrived first, on every strategy.
#[test]
fn first_selects_the_earliest_build_row_on_every_strategy() {
    each_run(|strategy, keyed, label| {
        let rows = run(strategy, "first", keyed);
        assert_eq!(
            pairs(&rows),
            [(1, ARRIVAL[0]), (2, ARRIVAL[0])],
            "[{label}] each driver is enriched with the earliest build row"
        );
    });
}

/// `match: all` emits a driver's rows in build arrival order, on every
/// strategy.
#[test]
fn all_rows_follow_build_arrival_order_within_a_driver() {
    each_run(|strategy, keyed, label| {
        let rows = run(strategy, "all", keyed);
        let expected: Vec<(i64, i64)> = [1, 2]
            .into_iter()
            .flat_map(|did| ARRIVAL.map(|bid| (did, bid)))
            .collect();
        assert_eq!(
            pairs(&rows),
            expected,
            "[{label}] each driver's rows follow build arrival order"
        );
    });
}

/// `match: collect` builds each array in build arrival order, on every
/// strategy.
#[test]
fn collect_array_follows_build_arrival_order() {
    each_run(|strategy, keyed, label| {
        let rows = run(strategy, "collect", keyed);
        assert_eq!(rows.len(), 2, "[{label}] one collect row per driver");
        for row in &rows {
            let collected: Vec<i64> = row
                .get("src_bld")
                .and_then(serde_json::Value::as_array)
                .unwrap_or_else(|| panic!("[{label}] a collect array: {row}"))
                .iter()
                .map(|element| int(element, "bid"))
                .collect();
            assert_eq!(
                collected, ARRIVAL,
                "[{label}] the array follows build arrival order"
            );
        }
    });
}

/// Adding a range conjunct every build row satisfies moves an equality join
/// from the hash strategy to the hash-partitioned IEJoin; the pick does not
/// change.
#[test]
fn first_does_not_depend_on_predicate_shape() {
    for keyed in [false, true] {
        let equality = run(&STRATEGIES[0], "first", keyed);
        let equality_and_range = run(&STRATEGIES[2], "first", keyed);
        assert_eq!(
            pairs(&equality),
            pairs(&equality_and_range),
            "keyed={keyed}: an always-true range conjunct does not change the pick"
        );
    }
}
