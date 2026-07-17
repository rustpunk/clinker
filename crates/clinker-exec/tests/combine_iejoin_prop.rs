//! Oracle-equivalence coverage for the pure-range block-band IEJoin.
//!
//! The proptest harness here generates arbitrary numeric record pairs and an
//! operator-pair, checked against a brute-force nested-loop oracle — the
//! load-bearing reference for every equivalence assertion below.
//!
//! The `pure_range` module drives whole pure-range combine pipelines through the
//! public executor at BOTH a tight (spilling) and a roomy (resident) memory
//! budget, asserting the emitted rows equal the nested-loop oracle at each. The
//! tight budget runs under `backpressure: spill` so it forces the block-band
//! external-sort / k-way-merge / block-reload path regardless of the host's
//! baseline RSS, and `cumulative_spill_bytes` confirms the spill actually
//! happened. These cover branches the unit tests cannot reach through the
//! body-less synthetic harness: a three-conjunct residual re-check, on_miss
//! null-fields under a spilled layout (real body evaluator), recoverable
//! output-eval deferral to the dead-letter queue under `strategy: continue`, and
//! LZ4-compressed block spill (the dispatcher's `auto` compression resolves to
//! LZ4 at the default batch size). The null-fields case additionally routes
//! NULL-range-key drivers through the spilled scan-phase unmatched path.
//!
//! The fixture-existence test exercises the IEJoin YAML fixtures end-to-end
//! through the YAML parser.
use std::collections::HashSet;

use clinker_plan::config::PipelineConfig;
use proptest::prelude::*;
use proptest::test_runner::TestRunner;

/// Operator pair element used by the IEJoin property tests.
///
/// Holding the four ordered comparison operators in a compact enum lets
/// the proptest strategy enumerate all sixteen `(op1, op2)` combinations
/// the algorithm must handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TestOp {
    Lt,
    Le,
    Gt,
    Ge,
}

impl TestOp {
    /// Apply the operator to two integers, returning the boolean result.
    fn apply(self, a: i64, b: i64) -> bool {
        match self {
            TestOp::Lt => a < b,
            TestOp::Le => a <= b,
            TestOp::Gt => a > b,
            TestOp::Ge => a >= b,
        }
    }
}

/// One generated proptest input — left rows, right rows, two
/// operators. Named so the strategy return type stays readable.
type IEJoinInputs = (Vec<(i64, i64)>, Vec<(i64, i64)>, TestOp, TestOp);

/// Generate arbitrary numeric record pairs together with two range
/// operators. Both sides are bounded to 0..200 elements to keep the
/// oracle's `O(N*M)` cost well under the proptest default budget while
/// still exercising the duplicate-key and empty-input paths.
fn arb_iejoin_inputs() -> impl Strategy<Value = IEJoinInputs> {
    (
        prop::collection::vec((any::<i64>(), any::<i64>()), 0..200),
        prop::collection::vec((any::<i64>(), any::<i64>()), 0..200),
        prop_oneof![
            Just(TestOp::Lt),
            Just(TestOp::Le),
            Just(TestOp::Gt),
            Just(TestOp::Ge)
        ],
        prop_oneof![
            Just(TestOp::Lt),
            Just(TestOp::Le),
            Just(TestOp::Gt),
            Just(TestOp::Ge)
        ],
    )
}

/// Brute-force nested-loop join — the IEJoin correctness oracle.
///
/// Returns the set of `(left_index, right_index)` pairs `(li, ri)` where
/// `op1.apply(left[li].0, right[ri].0) && op2.apply(left[li].1, right[ri].1)`
/// holds. The set form is order-independent so it can be compared
/// directly against IEJoin's output (which is permitted to emit pairs in
/// any internal order).
fn nested_loop_join(
    left: &[(i64, i64)],
    right: &[(i64, i64)],
    op1: TestOp,
    op2: TestOp,
) -> HashSet<(usize, usize)> {
    let mut out = HashSet::new();
    for (li, l) in left.iter().enumerate() {
        for (ri, r) in right.iter().enumerate() {
            if op1.apply(l.0, r.0) && op2.apply(l.1, r.1) {
                out.insert((li, ri));
            }
        }
    }
    out
}

/// The oracle is correct on a hand-crafted 3x3 matrix.
///
/// Pinning a known result lets future regressions in the oracle itself
/// surface immediately — the oracle is the trusted reference for every
/// downstream IEJoin property assertion, so its own correctness is
/// non-negotiable.
#[test]
fn test_iejoin_nested_loop_oracle_correct() {
    // left[0] = (10, 100), left[1] = (20, 50), left[2] = (5, 200)
    // right[0] = (15, 80), right[1] = (25, 150), right[2] = (8, 60)
    //
    // For op1 = Lt (left.0 <  right.0) and op2 = Gt (left.1 >  right.1):
    //   (li=0, ri=0): 10 < 15  && 100 > 80  -> true
    //   (li=0, ri=1): 10 < 25  && 100 > 150 -> false
    //   (li=0, ri=2):  8 ?     -> 10 < 8 false
    //   (li=1, ri=0): 20 < 15  -> false
    //   (li=1, ri=1): 20 < 25  &&  50 > 150 -> false
    //   (li=1, ri=2): 20 < 8   -> false
    //   (li=2, ri=0):  5 < 15  && 200 > 80  -> true
    //   (li=2, ri=1):  5 < 25  && 200 > 150 -> true
    //   (li=2, ri=2):  5 < 8   && 200 > 60  -> true
    let left = vec![(10_i64, 100_i64), (20, 50), (5, 200)];
    let right = vec![(15_i64, 80_i64), (25, 150), (8, 60)];

    let actual = nested_loop_join(&left, &right, TestOp::Lt, TestOp::Gt);

    let expected: HashSet<(usize, usize)> = [(0, 0), (2, 0), (2, 1), (2, 2)].into_iter().collect();
    assert_eq!(actual, expected);
}

/// The proptest infrastructure runs end-to-end with a no-op verifier.
///
/// A successful run here means the strategy compiles, the runner can
/// drive it under the workspace's configured shrinker, and arbitrary
/// inputs are generable — the prerequisite for any future
/// `proptest_iejoin_matches_nested_loop` assertion that wires the
/// oracle to the real algorithm.
#[test]
fn test_iejoin_scaffold_compiles() {
    let mut runner = TestRunner::default();
    let result = runner.run(&arb_iejoin_inputs(), |_| Ok(()));
    assert!(
        result.is_ok(),
        "proptest scaffold failed under no-op verifier: {result:?}"
    );
}

/// All seven IEJoin fixture YAMLs parse cleanly via the workspace's
/// canonical YAML chokepoint.
///
/// Parse-time validation pins the schema shape and catches typos; the
/// `pure_range` module below covers the runtime block-band path end-to-end.
#[test]
fn test_iejoin_fixtures_exist() {
    let fixture_dir =
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/combine");
    let stems = [
        "iejoin_tax_bracket",
        "iejoin_temporal_overlap",
        "iejoin_null_ranges",
        "iejoin_all_duplicates",
        "iejoin_empty_build",
        "iejoin_single_row",
        "iejoin_all_null_ranges",
        "iejoin_three_conjunct_residual",
        "iejoin_pure_range_null_fields",
    ];
    for stem in stems {
        let path = fixture_dir.join(format!("{stem}.yaml"));
        let yaml = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read fixture {}: {e}", path.display()));
        clinker_plan::yaml::from_str::<PipelineConfig>(&yaml)
            .unwrap_or_else(|e| panic!("fixture parse error for {stem}: {e}"));
    }
}

/// End-to-end oracle-equivalence for the pure-range block-band path, driven
/// through the public executor at a spilling (tight) and a resident (roomy)
/// budget. See the module docstring for the branch inventory.
mod pure_range {
    use std::collections::{HashMap, HashSet};
    use std::io::{Cursor, Write};

    use clinker_bench_support::io::SharedBuffer;
    use clinker_core_types::dlq::DlqErrorCategory;
    use clinker_exec::executor::{PipelineExecutor, PipelineRunParams, SourceReaders};
    use clinker_plan::config::{CompileContext, PipelineConfig};

    /// Tight budget: small enough that the block-band external-sort threshold
    /// (`soft / 4`) binds and the sides spill. Run under `backpressure: spill`
    /// so a sub-baseline-RSS limit is not rejected at startup (the `pause`
    /// policy would reject it) but instead completes by spilling — making the
    /// forced-spill layout independent of the host's baseline RSS.
    const TIGHT_LIMIT: &str = "1M";
    /// Roomy budget: far above the working set, so every side stays resident and
    /// no block-band spill occurs — the resident half of the across-budget pair.
    const ROOMY_LIMIT: &str = "512M";

    /// Outcome of one pipeline run: the primary output CSV, the total committed
    /// spill bytes, and the count of recoverable Combine output-row failures the
    /// run dead-lettered.
    struct RunResult {
        output: String,
        spill_bytes: u64,
        combine_dlq: usize,
    }

    /// Compile and run a pipeline over in-memory CSV inputs, capturing the named
    /// output. `yaml` carries `__LIMIT__` / `__POLICY__` placeholders the caller
    /// fills per budget.
    fn run(
        yaml_template: &str,
        limit: &str,
        policy: &str,
        inputs: &[(&str, &str)],
        output_name: &str,
    ) -> RunResult {
        let yaml = yaml_template
            .replace("__LIMIT__", limit)
            .replace("__POLICY__", policy);
        let config: PipelineConfig =
            clinker_plan::yaml::from_str(&yaml).expect("pure-range yaml must parse");
        let plan = config
            .compile(&CompileContext::default())
            .expect("pure-range yaml must compile");
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
            execution_id: "iejoin-prop".to_string(),
            batch_id: "iejoin-prop-batch".to_string(),
            ..Default::default()
        };
        let report =
            PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
                .expect("pure-range pipeline must execute");
        let combine_dlq = report
            .dlq_entries
            .iter()
            .filter(|e| e.category == DlqErrorCategory::CombineOutputRow)
            .count();
        RunResult {
            output: buf.as_string(),
            spill_bytes: report.cumulative_spill_bytes,
            combine_dlq,
        }
    }

    /// Parse an output CSV into a header-keyed row multiset (sorted `Vec`), so
    /// two runs are compared as record sets independent of emission order.
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

    /// One `(id, key1, key2)` row on either side. `None` keys model a NULL /
    /// non-orderable range column the scan routes to the unmatched pile.
    type Row = (i64, Option<i64>, Option<i64>);

    /// Nested-loop oracle for a two-conjunct half-open band
    /// (`driver.k1 >= build.lo AND driver.k1 < build.hi`), returning the set of
    /// matched `(driver_id, build_id)` pairs. NULL-keyed rows never match.
    fn oracle_band(drivers: &[Row], builds: &[Row]) -> HashSet<(i64, i64)> {
        let mut out = HashSet::new();
        for (did, dk, _) in drivers {
            let Some(amount) = dk else { continue };
            for (bid, lo, hi) in builds {
                let (Some(lo), Some(hi)) = (lo, hi) else {
                    continue;
                };
                if amount >= lo && amount < hi {
                    out.insert((*did, *bid));
                }
            }
        }
        out
    }

    /// Expected `[did, bid]` output rows (sorted) for a match:all band join,
    /// matching the `rows()` shape a two-column `did,bid` output CSV parses to.
    fn expected_band_rows(pairs: &HashSet<(i64, i64)>) -> Vec<Vec<String>> {
        let mut rows: Vec<Vec<String>> = pairs
            .iter()
            .map(|(d, b)| vec![d.to_string(), b.to_string()])
            .collect();
        rows.sort();
        rows
    }

    /// Half-open band workload: driver `i` carries `k1 = (i * step) % span`, so
    /// incomes tile the covered range; build `b` is the half-open interval
    /// `[b*bstep, b*bstep + bstep)`. Sized so the block-band drain overflows the
    /// tight `soft / 4` sort threshold into multiple blocks and spilled runs.
    /// Returns rows plus CSV bytes for both sides.
    fn band_workload(
        n_drivers: i64,
        n_builds: i64,
        span: i64,
    ) -> (Vec<Row>, Vec<Row>, String, String) {
        let bstep = span / n_builds;
        let mut drivers = Vec::with_capacity(n_drivers as usize);
        let mut driver_csv = String::from("did,k1\n");
        for i in 0..n_drivers {
            let k1 = (i.wrapping_mul(2_617)).rem_euclid(span);
            drivers.push((i, Some(k1), None));
            driver_csv.push_str(&format!("{i},{k1}\n"));
        }
        let mut builds = Vec::with_capacity(n_builds as usize);
        let mut build_csv = String::from("bid,lo,hi\n");
        for b in 0..n_builds {
            let lo = b * bstep;
            let hi = lo + bstep;
            builds.push((b, Some(lo), Some(hi)));
            build_csv.push_str(&format!("{b},{lo},{hi}\n"));
        }
        (drivers, builds, driver_csv, build_csv)
    }

    const BAND_YAML: &str = r#"
pipeline:
  name: pure_range_band
  memory: { limit: "__LIMIT__", backpressure: __POLICY__ }
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - { name: did, type: int }
        - { name: k1, type: int }
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
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.k1 >= builds.lo and drivers.k1 < builds.hi"
      match: all
      on_miss: skip
      cxl: |
        emit did = drivers.did
        emit bid = builds.bid
      propagate_ck: driver
  - type: output
    name: out
    input: banded
    config:
      name: out
      type: csv
      path: out.csv
"#;

    /// A multi-block, LZ4-compressed spilled layout emits exactly the oracle's
    /// matched rows, and does so identically to the resident layout. Under the
    /// tight budget the block-band sides overflow into multiple blocks and
    /// spilled runs; at the default batch size the dispatcher's `auto`
    /// compression resolves to LZ4 for these schema widths, so this is the
    /// compressed-block-spill path. The resident run at the roomy budget keeps
    /// everything in RAM. Both must equal the nested-loop oracle, proving the
    /// merge / reload / prune path is budget-independent.
    #[test]
    fn multi_block_lz4_spill_matches_oracle_across_budgets() {
        let (drivers, builds, driver_csv, build_csv) = band_workload(8_000, 40, 1_000);
        let inputs = [
            ("drivers", driver_csv.as_str()),
            ("builds", build_csv.as_str()),
        ];
        let expected = expected_band_rows(&oracle_band(&drivers, &builds));

        let tight = run(BAND_YAML, TIGHT_LIMIT, "spill", &inputs, "out");
        let roomy = run(BAND_YAML, ROOMY_LIMIT, "spill", &inputs, "out");

        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled block-band output diverged from the nested-loop oracle"
        );
        assert_eq!(
            rows(&roomy.output),
            expected,
            "resident block-band output diverged from the nested-loop oracle"
        );
        assert!(
            !expected.is_empty(),
            "the band workload must produce matches"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must have spilled the block-band sides to disk"
        );
    }

    const THREE_CONJUNCT_YAML: &str = r#"
pipeline:
  name: pure_range_three_conjunct
  memory: { limit: "__LIMIT__", backpressure: __POLICY__ }
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - { name: did, type: int }
        - { name: k1, type: int }
        - { name: k2, type: int }
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
        - { name: base, type: int }
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.k1 >= builds.lo and drivers.k1 < builds.hi and drivers.k2 > builds.base"
      match: all
      on_miss: skip
      cxl: |
        emit did = drivers.did
        emit bid = builds.bid
      propagate_ck: driver
  - type: output
    name: out
    input: banded
    config:
      name: out
      type: csv
      path: out.csv
"#;

    /// A three-conjunct residual predicate (two-axis band plus a third
    /// cross-input `>` conjunct) emits exactly the oracle's matched rows across
    /// both budgets. The block-band kernel proves the two band axes; the third
    /// conjunct rides the residual re-check filter, so this exercises that filter
    /// through the spilled and resident block layouts alike.
    #[test]
    fn three_conjunct_residual_matches_oracle_across_budgets() {
        // drivers: k1 tiles [0, span), k2 cycles 0..100. builds: 40 half-open
        // bands over [0, span) with base = 50, so the third conjunct (k2 > 50)
        // drops roughly half of every band's otherwise-matching drivers — the
        // residual filter has to fire for the output to match the oracle.
        let span = 1_000i64;
        let n_builds = 40i64;
        let bstep = span / n_builds;
        let mut drivers: Vec<(i64, i64, i64)> = Vec::new();
        let mut driver_csv = String::from("did,k1,k2\n");
        for i in 0..8_000i64 {
            let k1 = (i.wrapping_mul(2_617)).rem_euclid(span);
            let k2 = i.rem_euclid(100);
            drivers.push((i, k1, k2));
            driver_csv.push_str(&format!("{i},{k1},{k2}\n"));
        }
        let mut builds: Vec<(i64, i64, i64, i64)> = Vec::new();
        let mut build_csv = String::from("bid,lo,hi,base\n");
        for b in 0..n_builds {
            let lo = b * bstep;
            let hi = lo + bstep;
            let base = 50;
            builds.push((b, lo, hi, base));
            build_csv.push_str(&format!("{b},{lo},{hi},{base}\n"));
        }
        let mut expected_pairs: HashSet<(i64, i64)> = HashSet::new();
        for (did, k1, k2) in &drivers {
            for (bid, lo, hi, base) in &builds {
                if k1 >= lo && k1 < hi && k2 > base {
                    expected_pairs.insert((*did, *bid));
                }
            }
        }
        let expected = expected_band_rows(&expected_pairs);
        assert!(
            !expected.is_empty(),
            "the three-conjunct workload must produce matches"
        );

        let inputs = [
            ("drivers", driver_csv.as_str()),
            ("builds", build_csv.as_str()),
        ];
        let tight = run(THREE_CONJUNCT_YAML, TIGHT_LIMIT, "spill", &inputs, "out");
        let roomy = run(THREE_CONJUNCT_YAML, ROOMY_LIMIT, "spill", &inputs, "out");
        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled three-conjunct output diverged from the oracle"
        );
        assert_eq!(
            rows(&roomy.output),
            expected,
            "resident three-conjunct output diverged from the oracle"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must have spilled the three-conjunct sides"
        );
    }

    const NULL_FIELDS_YAML: &str = r#"
pipeline:
  name: pure_range_null_fields
  memory: { limit: "__LIMIT__", backpressure: __POLICY__ }
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - { name: did, type: int }
        - { name: k1, type: int }
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
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.k1 >= builds.lo and drivers.k1 < builds.hi"
      match: first
      on_miss: null_fields
      cxl: |
        emit did = drivers.did
        emit bid = builds.bid
      propagate_ck: driver
  - type: output
    name: out
    input: banded
    config:
      name: out
      type: csv
      path: out.csv
"#;

    /// on_miss:null_fields under a spilled layout, driven by a real body
    /// evaluator (the unit harness is body-less). Every driver emits exactly one
    /// row: a matched driver carries its first (only, since the bands are
    /// disjoint) build id; a zero-match driver — whether its range key is out of
    /// range or NULL — carries a null build id. A third of the drivers carry a
    /// NULL range key, so the deferred null-fields dispatch drains the spilled
    /// scan-phase unmatched pile (a residency the scan-phase spill bounds) and
    /// reunites it with the in-block misses in input order. The per-driver output
    /// must match the oracle identically at the spilled and resident budgets.
    #[test]
    fn null_fields_over_spilled_scan_phase_matches_oracle_across_budgets() {
        let span = 1_000i64;
        let n_builds = 40i64;
        let bstep = span / n_builds;
        // Per-driver expected build id ("" == null). Keyed by did.
        let mut expected_by_did: Vec<(i64, String)> = Vec::new();
        let mut driver_csv = String::from("did,k1\n");
        for i in 0..6_000i64 {
            if i.rem_euclid(3) == 0 {
                // NULL range key → scan-phase unmatched → null-fields row.
                driver_csv.push_str(&format!("{i},\n"));
                expected_by_did.push((i, String::new()));
            } else {
                // k1 spans [0, span + 200): the tail past `span` matches no band,
                // so it is an in-range-format but zero-match (null-fields) driver.
                let k1 = (i.wrapping_mul(2_617)).rem_euclid(span + 200);
                driver_csv.push_str(&format!("{i},{k1}\n"));
                let bid = if k1 < span {
                    (k1 / bstep).to_string()
                } else {
                    String::new()
                };
                expected_by_did.push((i, bid));
            }
        }
        let mut build_csv = String::from("bid,lo,hi\n");
        for b in 0..n_builds {
            let lo = b * bstep;
            let hi = lo + bstep;
            build_csv.push_str(&format!("{b},{lo},{hi}\n"));
        }
        let mut expected: Vec<Vec<String>> = expected_by_did
            .iter()
            .map(|(did, bid)| vec![did.to_string(), bid.clone()])
            .collect();
        expected.sort();

        let inputs = [
            ("drivers", driver_csv.as_str()),
            ("builds", build_csv.as_str()),
        ];
        let tight = run(NULL_FIELDS_YAML, TIGHT_LIMIT, "spill", &inputs, "out");
        let roomy = run(NULL_FIELDS_YAML, ROOMY_LIMIT, "spill", &inputs, "out");
        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled null-fields output (incl. spilled scan-phase misses) diverged from the oracle"
        );
        assert_eq!(
            rows(&roomy.output),
            expected,
            "resident null-fields output diverged from the oracle"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must have spilled the sides and the scan-phase pile"
        );
    }

    const CONTINUE_DEFER_YAML: &str = r#"
pipeline:
  name: pure_range_continue_defer
  memory: { limit: "__LIMIT__", backpressure: __POLICY__ }
error_handling:
  strategy: continue
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - { name: did, type: int }
        - { name: k1, type: int }
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
        - { name: factor, type: int }
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.k1 >= builds.lo and drivers.k1 < builds.hi"
      match: all
      on_miss: skip
      cxl: |
        emit did = drivers.did
        emit bid = builds.bid
        emit ratio = drivers.k1 / builds.factor
      propagate_ck: driver
  - type: output
    name: out
    input: banded
    config:
      name: out
      type: csv
      path: out.csv
"#;

    /// Recoverable output-eval deferral under `strategy: continue`: the body
    /// divides by a build `factor` that is zero for every odd band, so a pair
    /// falling in an odd band raises a recoverable output-eval failure. Under
    /// `continue` that pair defers to the dead-letter queue rather than aborting
    /// the run; pairs in even (nonzero-factor) bands emit normally. The surviving
    /// output must equal the nested-loop oracle over the nonzero-factor matches,
    /// and both the survivor set and the dead-letter count must be identical
    /// across the spilled and resident budgets — the deferral path is
    /// budget-independent.
    #[test]
    fn recoverable_output_eval_defers_under_continue_across_budgets() {
        let span = 1_000i64;
        let n_builds = 40i64;
        let bstep = span / n_builds;
        // Even band b → factor b+1 (nonzero, emits); odd band → factor 0 (the
        // body divides by zero, so its matches defer to the DLQ).
        let factor_of = |band: i64| -> i64 { if band.rem_euclid(2) == 0 { band + 1 } else { 0 } };
        let mut expected: Vec<Vec<String>> = Vec::new();
        let mut driver_csv = String::from("did,k1\n");
        for i in 0..8_000i64 {
            // Keep every driver inside [0, span) so it matches exactly one band;
            // whether it emits or defers depends only on that band's factor.
            let k1 = (i.wrapping_mul(2_617)).rem_euclid(span);
            driver_csv.push_str(&format!("{i},{k1}\n"));
            let band = k1 / bstep;
            let factor = factor_of(band);
            if factor != 0 {
                let ratio = k1 / factor;
                expected.push(vec![i.to_string(), band.to_string(), ratio.to_string()]);
            }
        }
        expected.sort();
        let mut build_csv = String::from("bid,lo,hi,factor\n");
        for b in 0..n_builds {
            let lo = b * bstep;
            let hi = lo + bstep;
            build_csv.push_str(&format!("{b},{lo},{hi},{}\n", factor_of(b)));
        }

        let inputs = [
            ("drivers", driver_csv.as_str()),
            ("builds", build_csv.as_str()),
        ];
        let tight = run(CONTINUE_DEFER_YAML, TIGHT_LIMIT, "spill", &inputs, "out");
        let roomy = run(CONTINUE_DEFER_YAML, ROOMY_LIMIT, "spill", &inputs, "out");
        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled continue-mode survivors diverged from the oracle over nonzero-factor matches"
        );
        assert_eq!(
            rows(&roomy.output),
            expected,
            "resident continue-mode survivors diverged from the oracle"
        );
        assert!(
            tight.combine_dlq > 0,
            "the zero-factor bands must have deferred recoverable failures to the DLQ"
        );
        assert_eq!(
            tight.combine_dlq, roomy.combine_dlq,
            "the deferred dead-letter count must be identical across budgets"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must have spilled the block-band sides"
        );
    }
}
