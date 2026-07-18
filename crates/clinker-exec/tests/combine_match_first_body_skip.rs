//! `match: first` treats the combine body as a post-match projection: the first
//! predicate-matching build is selected, the body runs once, and a body that
//! skips (or a recoverable body-error defer) drops only that output row. It does
//! NOT retry a later build and does NOT route the driver to `on_miss` — `on_miss`
//! fires only for drivers with zero predicate matches.
//!
//! These tests pin that semantics identically across all five physical combine
//! strategies. Each pipeline joins one driver whose chosen build emits (`keep=1`),
//! one whose chosen build skips (`keep=0`), and — except under `on_miss: error` —
//! one that matches no build at all. The body is
//! `filter builds.keep.is_null() or builds.keep == 1`: a matched `keep=1` build
//! emits the row, a matched `keep=0` build skips it, and a true miss (whose build
//! side is null on the `on_miss` path) passes the filter and emits under
//! `null_fields`.
//!
//! The `keep=0` skip driver is therefore observable. Under the retired
//! retry/route-to-miss reading it would surface as a `null_fields` row (the miss
//! body sees a null build and passes the filter) or abort under `on_miss: error`;
//! under the post-match-projection reading it silently produces nothing. Each
//! driver matches exactly one build, so the "first" selection is deterministic
//! regardless of which strategy visits builds in which order.

use std::collections::HashMap;
use std::io::{Cursor, Write};

use clinker_bench_support::io::SharedBuffer;
use clinker_exec::executor::{
    PipelineExecutor, PipelineRunParams, SourceReaders, single_file_reader,
};
use clinker_plan::config::{CompileContext, parse_config};

/// Outcome of one successful pipeline run.
struct RunOutcome {
    /// Full primary-output CSV text (header included).
    output: String,
    /// Total committed spill bytes for the run.
    spill_bytes: u64,
}

/// Compile and execute `yaml` over the named in-memory CSV inputs, returning the
/// captured `out` CSV and spill total, or the formatted pipeline error.
fn try_run(yaml: &str, inputs: &[(&str, String)]) -> Result<RunOutcome, String> {
    let config = parse_config(yaml).map_err(|e| format!("parse: {e:?}"))?;
    let plan = config
        .compile(&CompileContext::default())
        .map_err(|e| format!("compile: {e:?}"))?;
    let readers: SourceReaders = inputs
        .iter()
        .map(|(name, data)| {
            (
                (*name).to_string(),
                single_file_reader("test.csv", Box::new(Cursor::new(data.as_bytes().to_vec()))),
            )
        })
        .collect();
    let buf = SharedBuffer::new();
    let writers: HashMap<String, Box<dyn Write + Send>> = HashMap::from([(
        "out".to_string(),
        Box::new(buf.clone()) as Box<dyn Write + Send>,
    )]);
    let params = PipelineRunParams {
        execution_id: "mb-f-855".to_string(),
        batch_id: "mb-f-855-batch".to_string(),
        ..Default::default()
    };
    match PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params) {
        Ok(report) => Ok(RunOutcome {
            output: buf.as_string(),
            spill_bytes: report.cumulative_spill_bytes,
        }),
        Err(e) => Err(format!("run: {e:?}")),
    }
}

/// Parse an output CSV into a header-skipped, sorted `Vec` of rows so two runs
/// compare as record sets independent of each strategy's emission order.
fn sorted_rows(csv: &str) -> Vec<Vec<String>> {
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

/// Assert the compiled Combine selects `expected_tag` (the `[combine:<tag>]`
/// glyph in `--explain`), so each pipeline forces the strategy it claims.
fn assert_strategy(yaml: &str, expected_tag: &str) {
    let config = parse_config(yaml).expect("parse for explain");
    let plan = config
        .compile(&CompileContext::default())
        .expect("compile for explain");
    let (dag, _) = PipelineExecutor::explain_plan_dag(&plan).expect("explain dag");
    let explain = dag.explain_text(&config);
    assert!(
        explain.contains(&format!("[combine:{expected_tag}]")),
        "expected [combine:{expected_tag}], got explain:\n{explain}"
    );
}

/// The shared post-match-projection body: emit `did` unless the *matched* build
/// carries `keep = 0`. A null build (the `on_miss` path) passes via `is_null`.
const BODY: &str =
    "        filter builds.keep.is_null() or builds.keep == 1\n        emit did = drivers.did\n";

/// One physical-strategy scenario: its `--explain` tag, a YAML builder taking the
/// `on_miss` policy, and driver / build CSV builders taking `include_miss`.
struct Scenario {
    tag: &'static str,
    yaml: fn(&str) -> String,
    drivers: fn(bool) -> String,
    builds: fn() -> String,
}

// ── in-memory hash (pure-equi) ────────────────────────────────────────────────

fn hash_yaml(on_miss: &str) -> String {
    format!(
        r#"
pipeline:
  name: mb_f_855_hash
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - {{ name: did, type: int }}
        - {{ name: mk, type: int }}
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      schema:
        - {{ name: bid, type: int }}
        - {{ name: mk, type: int }}
        - {{ name: keep, type: int }}
  - type: combine
    name: j
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.mk == builds.mk"
      match: first
      on_miss: {on_miss}
      propagate_ck: driver
      cxl: |
{BODY}
  - type: output
    name: out
    input: j
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

fn hash_drivers(include_miss: bool) -> String {
    let mut s = String::from("did,mk\n10,1\n20,2\n");
    if include_miss {
        s.push_str("30,9\n");
    }
    s
}

fn hash_builds() -> String {
    String::from("bid,mk,keep\n100,1,1\n200,2,0\n")
}

// ── grace hash (pure-equi, forced) ────────────────────────────────────────────

fn grace_yaml(on_miss: &str) -> String {
    // Identical to the pure-equi hash pipeline plus the `strategy: grace_hash`
    // hint, so the two differ only in physical strategy.
    format!(
        r#"
pipeline:
  name: mb_f_855_grace
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - {{ name: did, type: int }}
        - {{ name: mk, type: int }}
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      schema:
        - {{ name: bid, type: int }}
        - {{ name: mk, type: int }}
        - {{ name: keep, type: int }}
  - type: combine
    name: j
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.mk == builds.mk"
      match: first
      on_miss: {on_miss}
      strategy: grace_hash
      propagate_ck: driver
      cxl: |
{BODY}
  - type: output
    name: out
    input: j
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

// ── sort-merge (pure-range, single inequality, presorted) ─────────────────────

fn sort_merge_yaml(on_miss: &str) -> String {
    format!(
        r#"
pipeline:
  name: mb_f_855_sort_merge
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      sort_order:
        - field: v
      schema:
        - {{ name: did, type: int }}
        - {{ name: v, type: int }}
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      sort_order:
        - field: hi
      schema:
        - {{ name: bid, type: int }}
        - {{ name: hi, type: int }}
        - {{ name: keep, type: int }}
  - type: combine
    name: j
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.v < builds.hi"
      match: first
      on_miss: {on_miss}
      propagate_ck: driver
      cxl: |
{BODY}
  - type: output
    name: out
    input: j
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

/// Ascending by `v`. `v=1` sees only the `hi=50` (keep=1) build first; `v=60`
/// clears `hi=50` and lands on the `hi=100` (keep=0) build; `v=200` clears both.
fn sort_merge_drivers(include_miss: bool) -> String {
    let mut s = String::from("did,v\n10,1\n20,60\n");
    if include_miss {
        s.push_str("30,200\n");
    }
    s
}

fn sort_merge_builds() -> String {
    String::from("bid,hi,keep\n100,50,1\n200,100,0\n")
}

// ── hash-partitioned IEJoin (equi + range) ────────────────────────────────────

fn equi_range_yaml(on_miss: &str) -> String {
    format!(
        r#"
pipeline:
  name: mb_f_855_equi_range
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - {{ name: did, type: int }}
        - {{ name: mk, type: int }}
        - {{ name: v, type: int }}
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      schema:
        - {{ name: bid, type: int }}
        - {{ name: mk, type: int }}
        - {{ name: hi, type: int }}
        - {{ name: keep, type: int }}
  - type: combine
    name: j
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.mk == builds.mk and drivers.v < builds.hi"
      match: first
      on_miss: {on_miss}
      propagate_ck: driver
      cxl: |
{BODY}
  - type: output
    name: out
    input: j
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

fn equi_range_drivers(include_miss: bool) -> String {
    let mut s = String::from("did,mk,v\n10,1,1\n20,2,2\n");
    if include_miss {
        s.push_str("30,9,1\n");
    }
    s
}

fn equi_range_builds() -> String {
    String::from("bid,mk,hi,keep\n100,1,100,1\n200,2,100,0\n")
}

// ── block-band IEJoin (pure-range, two-conjunct band) ─────────────────────────

fn block_yaml(on_miss: &str) -> String {
    format!(
        r#"
pipeline:
  name: mb_f_855_block
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - {{ name: did, type: int }}
        - {{ name: v, type: int }}
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      schema:
        - {{ name: bid, type: int }}
        - {{ name: lo, type: int }}
        - {{ name: hi, type: int }}
        - {{ name: keep, type: int }}
  - type: combine
    name: j
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.v >= builds.lo and drivers.v < builds.hi"
      match: first
      on_miss: {on_miss}
      propagate_ck: driver
      cxl: |
{BODY}
  - type: output
    name: out
    input: j
    config:
      name: out
      type: csv
      path: out.csv
"#
    )
}

fn block_drivers(include_miss: bool) -> String {
    let mut s = String::from("did,v\n10,10\n20,60\n");
    if include_miss {
        s.push_str("30,200\n");
    }
    s
}

fn block_builds() -> String {
    // Disjoint bands: [0,50) keep=1, [50,100) keep=0.
    String::from("bid,lo,hi,keep\n100,0,50,1\n200,50,100,0\n")
}

fn scenarios() -> Vec<Scenario> {
    vec![
        Scenario {
            tag: "hash_build_probe",
            yaml: hash_yaml,
            drivers: hash_drivers,
            builds: hash_builds,
        },
        Scenario {
            tag: "grace_hash",
            yaml: grace_yaml,
            drivers: hash_drivers,
            builds: hash_builds,
        },
        Scenario {
            tag: "sort_merge",
            yaml: sort_merge_yaml,
            drivers: sort_merge_drivers,
            builds: sort_merge_builds,
        },
        Scenario {
            tag: "hash_partition_iejoin",
            yaml: equi_range_yaml,
            drivers: equi_range_drivers,
            builds: equi_range_builds,
        },
        Scenario {
            tag: "iejoin",
            yaml: block_yaml,
            drivers: block_drivers,
            builds: block_builds,
        },
    ]
}

/// Every strategy forces the tag it claims — proof the differential test truly
/// spans all five physical paths rather than the same one five times.
#[test]
fn each_pipeline_forces_its_strategy() {
    for s in scenarios() {
        assert_strategy(&(s.yaml)("skip"), s.tag);
    }
}

/// `on_miss: skip` — the skip driver and the true miss both vanish, so every
/// strategy emits exactly the `keep=1` driver.
#[test]
fn body_skip_identical_across_strategies_on_miss_skip() {
    let expected = vec![vec!["10".to_string()]];
    for s in scenarios() {
        let inputs = [("drivers", (s.drivers)(true)), ("builds", (s.builds)())];
        let out = try_run(&(s.yaml)("skip"), &inputs)
            .unwrap_or_else(|e| panic!("[{}] run failed: {e}", s.tag));
        assert_eq!(
            sorted_rows(&out.output),
            expected,
            "[{}] on_miss:skip output diverged",
            s.tag
        );
    }
}

/// `on_miss: null_fields` — the true miss (did 30) emits through the null-build
/// path; the `keep=0` skip driver (did 20) must NOT appear. The retired reading
/// would surface did 20 as a null-fields row, so this is the discriminating case.
#[test]
fn body_skip_identical_across_strategies_on_miss_null_fields() {
    let expected = vec![vec!["10".to_string()], vec!["30".to_string()]];
    for s in scenarios() {
        let inputs = [("drivers", (s.drivers)(true)), ("builds", (s.builds)())];
        let out = try_run(&(s.yaml)("null_fields"), &inputs)
            .unwrap_or_else(|e| panic!("[{}] run failed: {e}", s.tag));
        assert_eq!(
            sorted_rows(&out.output),
            expected,
            "[{}] on_miss:null_fields must emit the miss but not the body-skip driver",
            s.tag
        );
    }
}

/// `on_miss: error` with no true miss — the `keep=0` body skip must not be
/// mistaken for a miss, so the run succeeds and emits only the `keep=1` driver.
/// Under the retired reading the skip driver would route to `on_miss: error` and
/// abort the pipeline.
#[test]
fn body_skip_does_not_trip_on_miss_error_across_strategies() {
    let expected = vec![vec!["10".to_string()]];
    for s in scenarios() {
        let inputs = [("drivers", (s.drivers)(false)), ("builds", (s.builds)())];
        let out = try_run(&(s.yaml)("error"), &inputs).unwrap_or_else(|e| {
            panic!(
                "[{}] a body-skip driver must not trip on_miss:error: {e}",
                s.tag
            )
        });
        assert_eq!(
            sorted_rows(&out.output),
            expected,
            "[{}] on_miss:error output diverged",
            s.tag
        );
    }
}

// ── block path: byte-identical across memory budgets ──────────────────────────

/// Tight budget: the block-band external-sort threshold (`soft / 4`) binds and
/// the sides spill. Under `backpressure: spill` a sub-baseline-RSS limit is not
/// rejected at startup but completes by spilling.
const TIGHT_LIMIT: &str = "1M";
/// Roomy budget: everything stays resident, so no block-band spill occurs.
const ROOMY_LIMIT: &str = "512M";

/// A fixed prime stride so driver keys tile `[0, span)` and overflow the tight
/// `soft / 4` sort threshold into multiple blocks and spilled runs.
fn tiled_key(i: i64, span: i64) -> i64 {
    i.wrapping_mul(2_617).rem_euclid(span)
}

const BLOCK_BUDGET_YAML: &str = r#"
pipeline:
  name: mb_f_855_block_budget
  memory: { limit: "__LIMIT__", backpressure: spill }
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - { name: did, type: int }
        - { name: v, type: int }
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      schema:
        - { name: bid, type: int }
        - { name: lo, type: int }
        - { name: hi, type: int }
        - { name: keep, type: int }
  - type: combine
    name: j
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.v >= builds.lo and drivers.v < builds.hi"
      match: first
      on_miss: null_fields
      propagate_ck: driver
      cxl: |
        filter builds.keep.is_null() or builds.keep == 1
        emit did = drivers.did
  - type: output
    name: out
    input: j
    config:
      name: out
      type: csv
      path: out.csv
"#;

/// Build the block-band budget workload: `n_bands` disjoint bands tiling
/// `[0, span)` with alternating `keep`, `n_drivers` drivers tiled across the
/// span, plus `n_miss` out-of-range drivers. Returns the driver/build CSVs and
/// the expected sorted `did` output under `on_miss: null_fields` (drivers whose
/// band keeps, plus every out-of-range miss).
fn block_budget_workload(
    n_drivers: i64,
    n_bands: i64,
    span: i64,
    n_miss: i64,
) -> (String, String, Vec<Vec<String>>) {
    let bstep = span / n_bands;
    let mut build_csv = String::from("bid,lo,hi,keep\n");
    for b in 0..n_bands {
        let keep = if b % 2 == 0 { 1 } else { 0 };
        build_csv.push_str(&format!(
            "{},{},{},{}\n",
            b,
            b * bstep,
            b * bstep + bstep,
            keep
        ));
    }

    let mut driver_csv = String::from("did,v\n");
    let mut expected: Vec<Vec<String>> = Vec::new();
    for i in 0..n_drivers {
        let v = tiled_key(i, span);
        driver_csv.push_str(&format!("{i},{v}\n"));
        // Which band does v fall in, and does that band keep?
        let band = v / bstep;
        if band % 2 == 0 {
            expected.push(vec![i.to_string()]);
        }
    }
    for j in 0..n_miss {
        let did = n_drivers + j;
        driver_csv.push_str(&format!("{did},{span}\n")); // v == span → no band → miss
        // null_fields miss: the null-build path passes the filter and emits.
        expected.push(vec![did.to_string()]);
    }
    expected.sort();
    (driver_csv, build_csv, expected)
}

/// The block-band path emits byte-identical output whether it spills (tight
/// budget) or stays resident (roomy budget), and the retired retry/route-to-miss
/// reading is absent at both: `keep=0` bands drop their drivers silently while
/// out-of-range misses emit under `null_fields`. The tight run must actually
/// spill so the two layouts genuinely differ.
#[test]
fn block_path_body_skip_byte_identical_across_budgets() {
    let (driver_csv, build_csv, expected) = block_budget_workload(8_000, 40, 1_000, 200);
    let inputs = [("drivers", driver_csv), ("builds", build_csv)];

    let tight = try_run(
        &BLOCK_BUDGET_YAML.replace("__LIMIT__", TIGHT_LIMIT),
        &inputs,
    )
    .expect("tight-budget block run");
    let roomy = try_run(
        &BLOCK_BUDGET_YAML.replace("__LIMIT__", ROOMY_LIMIT),
        &inputs,
    )
    .expect("roomy-budget block run");

    assert!(
        tight.spill_bytes > 0,
        "the tight budget must spill the block-band sides to disk"
    );
    assert_eq!(
        tight.output, roomy.output,
        "block-band output must be byte-identical across memory budgets"
    );
    assert_eq!(
        sorted_rows(&tight.output),
        expected,
        "block-band body-skip output diverged from the oracle"
    );
}
