//! Nanosecond-exact datetime keys end-to-end: range joins and group-by.
//!
//! `Value::DateTime` carries nanosecond resolution, and every order-bearing
//! datetime encoder now reduces through one canonical i128-nanosecond key. These
//! tests drive whole pipelines through the public executor to prove the two
//! defects the earlier microsecond encoders caused are gone:
//!
//! * A single-inequality datetime range join whose match hinges on a
//!   SUB-MICROSECOND difference emits exactly the nanosecond-exact oracle — the
//!   diagonal matches a microsecond-truncated key silently dropped — and does so
//!   identically across memory budgets (spill vs resident) AND across BOTH
//!   strategies (presorted → SortMerge, unsorted → IEJoin).
//! * A spilled hash aggregation grouping on datetimes that differ only in
//!   nanoseconds keeps them as DISTINCT groups; the spill k-way merge decides
//!   group identity by sort-key bytes, so a truncated key would have merged them.
//!
//! The CSV writer renders datetimes to whole seconds, so assertions key off
//! derived id/count columns rather than the rendered timestamps. These tests
//! exercise sub-microsecond ordering, for which the calendar era is irrelevant,
//! so the fixtures stay in the ordinary era; out-of-range key exactness is pinned
//! by the in-crate `sort_key` unit tests and out-of-range `Value::DateTime`
//! spill/reload exactness by the `clinker-record` value round-trip tests.

use std::collections::HashMap;
use std::io::{Cursor, Write};

use chrono::{Duration, NaiveDate, NaiveDateTime};
use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
use clinker_plan::config::{CompileContext, PipelineConfig};

/// Tight budget: small enough that the block-band external-sort threshold binds
/// and the sides spill; the hash aggregate's group-count threshold also trips.
const TIGHT_LIMIT: &str = "1M";
/// Roomy budget: far above the working set, so nothing spills — the resident
/// half of every across-budget pair.
const ROOMY_LIMIT: &str = "512M";

/// Base instant all generated datetimes are offset from.
fn base() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_nano_opt(0, 0, 0, 0)
        .unwrap()
}

/// Render `base() + offset_ns` as an ISO string with a full 9-digit fraction,
/// so a `%Y-%m-%dT%H:%M:%S%.f` column parses it back at nanosecond resolution.
fn dt_string(offset_ns: i64) -> String {
    (base() + Duration::nanoseconds(offset_ns))
        .format("%Y-%m-%dT%H:%M:%S%.9f")
        .to_string()
}

/// Outcome of one pipeline run: the primary output CSV plus the total committed
/// spill bytes.
struct RunResult {
    output: String,
    spill_bytes: u64,
}

/// Compile a config and run it over in-memory CSV inputs, capturing the named
/// output. Source readers are keyed by source-node name.
fn run_config(config: PipelineConfig, inputs: &[(&str, &str)], output_name: &str) -> RunResult {
    let plan = config
        .compile(&CompileContext::default())
        .expect("datetime yaml must compile");
    let readers: SourceReaders = inputs
        .iter()
        .map(|(name, data)| {
            (
                (*name).to_string(),
                clinker_exec::executor::single_file_reader(
                    "test.csv",
                    Box::new(Cursor::new(data.as_bytes().to_vec())),
                ),
            )
        })
        .collect();
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        output_name.to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "datetime-nanos".to_string(),
        batch_id: "datetime-nanos-batch".to_string(),
        ..Default::default()
    };
    let report = PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
        .expect("datetime pipeline must execute");
    RunResult {
        output: buf.as_string(),
        spill_bytes: report.cumulative_spill_bytes,
    }
}

/// Run an inline template with its `__LIMIT__` / `__SORT_DRV__` / `__SORT_BLD__`
/// placeholders filled, over in-memory CSV inputs.
fn run(
    yaml_template: &str,
    limit: &str,
    sort_drv: &str,
    sort_bld: &str,
    inputs: &[(&str, &str)],
    output_name: &str,
) -> RunResult {
    let yaml = yaml_template
        .replace("__LIMIT__", limit)
        .replace("__SORT_DRV__", sort_drv)
        .replace("__SORT_BLD__", sort_bld);
    let config: PipelineConfig =
        clinker_plan::yaml::from_str(&yaml).expect("datetime yaml must parse");
    run_config(config, inputs, output_name)
}

/// Parse an output CSV into a header-keyed row multiset (sorted `Vec`), so two
/// runs compare as record sets independent of emission order.
fn rows(csv: &str) -> Vec<Vec<String>> {
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(csv.as_bytes());
    let mut out: Vec<Vec<String>> = rdr
        .records()
        .map(|r| {
            r.expect("output row parses")
                .iter()
                .map(String::from)
                .collect()
        })
        .collect();
    out.sort();
    out
}

/// A fixed prime stride so driver microsecond-slots tile `[0, span)` rather than
/// cluster, forcing the block-band sort buffer past the tight threshold into
/// multiple spilled runs. Mirrors the pure-range prop-test's `tiled_key`.
fn tiled_slot(i: i64, span: i64) -> i64 {
    i.wrapping_mul(2_617).rem_euclid(span)
}

// ── single-inequality datetime range join, sub-µs boundary ───────────────────

/// `driver.ts < build.threshold`, `match: all`. `__SORT_DRV__` / `__SORT_BLD__`
/// are either an empty string (no `sort_order` → IEJoin) or a `sort_order:`
/// block on the range axis (→ SortMerge, single inequality is SortMerge-
/// eligible). Every other line is identical, so the two only differ by routing.
const JOIN_YAML: &str = r#"
pipeline:
  name: datetime_nanos_join
  memory: { limit: "__LIMIT__", backpressure: spill }
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
__SORT_DRV__
      schema:
        - { name: did, type: int }
        - { name: ts, type: date_time, format: "%Y-%m-%dT%H:%M:%S%.f" }
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
__SORT_BLD__
      schema:
        - { name: bid, type: int }
        - { name: threshold, type: date_time, format: "%Y-%m-%dT%H:%M:%S%.f" }
  - type: combine
    name: joined
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.ts < builds.threshold"
      match: all
      on_miss: skip
      cxl: |
        emit did = drivers.did
        emit bid = builds.bid
      propagate_ck: driver
  - type: output
    name: out
    input: joined
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// The `sort_order:` block that routes a source to SortMerge, indented to sit
/// under `config:`.
fn sort_order_block(field: &str) -> String {
    format!("      sort_order:\n        - field: {field}")
}

/// The single build threshold sits `+500 ns` into microsecond-slot `t_slot`.
/// Tiled drivers sit `+300 ns` into their slot, so a driver landing exactly on
/// `t_slot` satisfies `driver.ts < threshold` by 200 ns — a match a microsecond-
/// truncated key drops (`t_slot < t_slot` is false). Two explicit boundary
/// drivers straddle the threshold sub-microsecond: one 300 ns below (matches;
/// the divergence case) and one 300 ns above (never matches). The threshold sits
/// LOW so only a small fraction of the 8000 drivers match — enough to keep the
/// SortMerge output well inside the tight budget while the 8000-row driver side
/// still overflows the block-band sort into spilled runs. Each driver matches
/// the one build or nothing, so `match: all` is unambiguous. Returns
/// `(driver_csv, build_csv, oracle_rows, divergence_did)`.
fn join_workload(
    n_drivers: i64,
    span: i64,
    t_slot: i64,
) -> (String, String, Vec<Vec<String>>, i64) {
    let threshold_ns = t_slot * 1_000 + 500;
    let divergence_did = 1_000_000; // matches only under the nanosecond key
    let mut drivers: Vec<(i64, i64)> = (0..n_drivers) // (did, driver_ns)
        .map(|i| (i, tiled_slot(i, span) * 1_000 + 300))
        .collect();
    drivers.push((divergence_did, t_slot * 1_000 + 200)); // < threshold by 300 ns
    drivers.push((divergence_did + 1, t_slot * 1_000 + 800)); // > threshold by 300 ns
    // Sorted by ts so the SortMerge presorted contract holds (IEJoin tolerates
    // the same ordering).
    drivers.sort_by_key(|(did, ns)| (*ns, *did));
    let mut driver_csv = String::from("did,ts\n");
    for (did, ns) in &drivers {
        driver_csv.push_str(&format!("{did},{}\n", dt_string(*ns)));
    }
    let build_csv = format!("bid,threshold\n0,{}\n", dt_string(threshold_ns));
    // Exact nanosecond oracle: every driver with driver_ns < threshold_ns joins
    // the single build (bid 0).
    let mut oracle: Vec<Vec<String>> = drivers
        .iter()
        .filter(|(_, dns)| *dns < threshold_ns)
        .map(|(did, _)| vec![did.to_string(), "0".to_string()])
        .collect();
    oracle.sort();
    (driver_csv, build_csv, oracle, divergence_did)
}

/// The join match set is nanosecond-exact and identical across BOTH strategies
/// and BOTH budgets. The boundary driver clears the threshold by 300 ns inside
/// one microsecond, so a microsecond-truncated key would drop it — the FIX makes
/// every routing emit exactly the nanosecond oracle, and the tight budget proves
/// the datetime keys survive the block-band spill/merge unchanged.
#[test]
fn datetime_single_inequality_matches_oracle_across_strategies_and_budgets() {
    let n_drivers = 8_000i64;
    let span = 1_000i64;
    let t_slot = 25i64; // low, so only a small fraction of drivers match
    let (driver_csv, build_csv, oracle, divergence_did) = join_workload(n_drivers, span, t_slot);
    let inputs = [
        ("drivers", driver_csv.as_str()),
        ("builds", build_csv.as_str()),
    ];
    assert!(!oracle.is_empty(), "the datetime join must produce matches");
    // The sub-µs divergence driver MUST be in the oracle (matches under the
    // nanosecond key) and its +300 ns twin MUST NOT be, or the test would not
    // exercise the defect it guards.
    let divergence_row = vec![divergence_did.to_string(), "0".to_string()];
    let above_row = vec![(divergence_did + 1).to_string(), "0".to_string()];
    assert!(
        oracle.contains(&divergence_row),
        "the sub-µs boundary driver must be an expected match"
    );
    assert!(
        !oracle.contains(&above_row),
        "the +300 ns twin must not match"
    );

    let drv_sort = sort_order_block("ts");
    let bld_sort = sort_order_block("threshold");

    // IEJoin (no sort_order) at both budgets.
    let iejoin_tight = run(JOIN_YAML, TIGHT_LIMIT, "", "", &inputs, "out");
    let iejoin_roomy = run(JOIN_YAML, ROOMY_LIMIT, "", "", &inputs, "out");
    // SortMerge (both sources presorted on the range axis) at both budgets.
    let sortmerge_tight = run(JOIN_YAML, TIGHT_LIMIT, &drv_sort, &bld_sort, &inputs, "out");
    let sortmerge_roomy = run(JOIN_YAML, ROOMY_LIMIT, &drv_sort, &bld_sort, &inputs, "out");

    for (label, r) in [
        ("iejoin/tight", &iejoin_tight),
        ("iejoin/roomy", &iejoin_roomy),
        ("sortmerge/tight", &sortmerge_tight),
        ("sortmerge/roomy", &sortmerge_roomy),
    ] {
        assert_eq!(
            rows(&r.output),
            oracle,
            "{label} datetime join diverged from the nanosecond-exact oracle"
        );
    }
    // Routing-independence: the two strategies agree row for row.
    assert_eq!(
        rows(&iejoin_tight.output),
        rows(&sortmerge_tight.output),
        "IEJoin and SortMerge produced different datetime match sets"
    );
    // The tight budget must actually spill on at least one strategy, proving the
    // datetime keys ride the block-band spill/merge intact.
    assert!(
        iejoin_tight.spill_bytes > 0,
        "the tight budget must spill the IEJoin block-band sides"
    );
}

// ── sub-µs group-by: no merge through a spilled aggregation ───────────────────

const GROUP_BY_YAML: &str = r#"
pipeline:
  name: datetime_nanos_group_by
  memory: { limit: "__LIMIT__", backpressure: spill }
nodes:
  - type: source
    name: events
    config:
      name: events
      type: csv
      path: events.csv
      schema:
        - { name: ts, type: date_time, format: "%Y-%m-%dT%H:%M:%S%.f" }
  - type: aggregate
    name: by_ts
    input: events
    config:
      group_by:
        - ts
      cxl: |
        emit ts = ts
        emit n = count(*)
  - type: output
    name: out
    input: by_ts
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// A hash aggregation grouping on datetime keys keeps two datetimes that differ
/// by a single nanosecond (inside one microsecond) as DISTINCT groups even when
/// the run spills and finalizes through the byte-wise k-way merge. Many distinct
/// second-spaced keys force the group-count spill; two extra keys share a
/// microsecond and differ by 1 ns. A microsecond-truncated sort key would merge
/// them into one group (count 2, one row); the nanosecond key keeps them apart.
#[test]
fn datetime_group_by_keeps_sub_microsecond_keys_distinct_through_spill() {
    // 200 distinct one-second-spaced filler keys → enough groups to overflow the
    // clamped in-memory cap and spill (mirrors the aggregate spill-cap workload).
    let n_filler = 200i64;
    let mut csv = String::from("ts\n");
    for i in 0..n_filler {
        csv.push_str(&format!("{}\n", dt_string(i * 1_000_000_000)));
    }
    // Two keys sharing one microsecond at a second (500) no filler uses, so the
    // rendered second isolates exactly these two rows in the output.
    let special_ns = 500 * 1_000_000_000;
    csv.push_str(&format!("{}\n", dt_string(special_ns + 100))); // …:20.000000100
    csv.push_str(&format!("{}\n", dt_string(special_ns + 900))); // …:20.000000900

    let config: PipelineConfig =
        clinker_plan::yaml::from_str(&GROUP_BY_YAML.replace("__LIMIT__", TIGHT_LIMIT))
            .expect("group-by yaml must parse");
    let result = run_config(config, &[("events", &csv)], "out");

    assert!(
        result.spill_bytes > 0,
        "the clamped budget must force the aggregate to spill; spill_bytes = {}",
        result.spill_bytes
    );

    let out_rows = rows(&result.output);
    // Every key is fed exactly once, so if none merged every group has count 1
    // and there are exactly `n_filler + 2` rows.
    assert_eq!(
        out_rows.len() as i64,
        n_filler + 2,
        "a merged sub-µs group would drop the row count below {}; got {} rows",
        n_filler + 2,
        out_rows.len()
    );
    // Locate the `n` column and assert no group merged (every count is 1).
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(result.output.as_bytes());
    let headers = rdr.headers().expect("output has a header").clone();
    let ts_idx = headers.iter().position(|h| h == "ts").expect("ts column");
    let n_idx = headers.iter().position(|h| h == "n").expect("n column");
    let second_500 = (base() + Duration::seconds(500))
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();
    let mut special_rows = 0;
    for rec in rdr.records() {
        let rec = rec.expect("row parses");
        let n: i64 = rec[n_idx].parse().expect("count is an integer");
        assert_eq!(
            n, 1,
            "no group may merge: every distinct-nanosecond key is fed once, \
             but a group at ts={} has count {n}",
            &rec[ts_idx]
        );
        if rec[ts_idx].starts_with(&second_500) {
            special_rows += 1;
        }
    }
    assert_eq!(
        special_rows, 2,
        "the two sub-microsecond keys (rendered to the same second) must remain \
         two distinct groups, not one merged group"
    );
}
