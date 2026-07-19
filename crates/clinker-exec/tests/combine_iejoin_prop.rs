//! Oracle-equivalence coverage for the pure-range block-band IEJoin.
//!
//! Every equivalence assertion in this file checks the engine against one shared
//! brute-force `nested_loop_oracle`, so a tie- or NULL-handling fix to the
//! reference propagates everywhere. The proptest harness generates arbitrary
//! numeric record pairs and an operator-pair; the `pure_range` tests drive whole
//! combine pipelines.
//!
//! The `pure_range` module drives whole pure-range combine pipelines through the
//! public executor at BOTH a tight (spilling) and a roomy (resident) memory
//! budget, asserting the emitted rows equal the oracle at each. The tight budget
//! runs under `backpressure: spill` so it forces the block-band external-sort /
//! k-way-merge / block-reload path regardless of the host's baseline RSS, and
//! `cumulative_spill_bytes` confirms the spill actually happened. These cover
//! branches the unit tests cannot reach through the body-less synthetic harness:
//! a three-conjunct residual re-check, on_miss null-fields under a spilled layout
//! (real body evaluator), recoverable output-eval deferral to the dead-letter
//! queue under `strategy: continue`, and LZ4-compressed block spill (the
//! dispatcher's `auto` compression resolves to LZ4 at the default batch size).
//! The null-fields cases additionally route NULL-range-key drivers through the
//! spilled scan-phase pile and orderable-but-out-of-range drivers through the
//! spilled in-block pile — both bounded to disk.
//!
//! The `iejoin_three_conjunct_residual` and `iejoin_pure_range_null_fields`
//! fixtures are the single source of truth for their pipeline shapes: the
//! `pure_range` module executes each one both over its own documented CSV data
//! (asserting the fixture's documented matches against the oracle) and over a
//! generated spilling workload. The other IEJoin fixtures are parse-checked by
//! `test_iejoin_fixtures_exist`.
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

/// Shared brute-force nested-loop oracle — the single trusted reference behind
/// every IEJoin equivalence assertion in this file.
///
/// For each `(driver, build)` pair the `matched` predicate accepts, it collects
/// `entry(di, driver, bi, build)`. Callers encode NULL / out-of-range skips in
/// `matched` and choose the comparison key in `entry`, so the index-pair proptest
/// oracle and the id-pair pipeline oracles share one loop — a tie- or
/// NULL-handling fix lives in exactly one place. The set form is
/// order-independent, so it compares directly against IEJoin's output (which may
/// emit in any internal order).
fn nested_loop_oracle<D, B, K: Eq + std::hash::Hash>(
    drivers: &[D],
    builds: &[B],
    matched: impl Fn(&D, &B) -> bool,
    entry: impl Fn(usize, &D, usize, &B) -> K,
) -> HashSet<K> {
    let mut out = HashSet::new();
    for (di, d) in drivers.iter().enumerate() {
        for (bi, b) in builds.iter().enumerate() {
            if matched(d, b) {
                out.insert(entry(di, d, bi, b));
            }
        }
    }
    out
}

/// Two-operator index-pair oracle over numeric rows — the proptest reference,
/// expressed over the shared [`nested_loop_oracle`]. Returns the set of
/// `(left_index, right_index)` pairs where
/// `op1.apply(left.0, right.0) && op2.apply(left.1, right.1)` holds.
fn nested_loop_join(
    left: &[(i64, i64)],
    right: &[(i64, i64)],
    op1: TestOp,
    op2: TestOp,
) -> HashSet<(usize, usize)> {
    nested_loop_oracle(
        left,
        right,
        |l, r| op1.apply(l.0, r.0) && op2.apply(l.1, r.1),
        |li, _, ri, _| (li, ri),
    )
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

/// The seven parse-only IEJoin fixture YAMLs parse cleanly via the workspace's
/// canonical YAML chokepoint.
///
/// Parse-time validation pins the schema shape and catches typos. The two
/// pure-range fixtures (`iejoin_three_conjunct_residual`,
/// `iejoin_pure_range_null_fields`) are not listed here because the `pure_range`
/// module below executes them end-to-end against the nested-loop oracle — both
/// over their own documented data and over a generated spilling workload — which
/// subsumes a parse check.
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
    use clinker_plan::config::{BackpressureKnob, CompileContext, PipelineConfig};

    use super::nested_loop_oracle;

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

    /// Fill an inline template's `__LIMIT__` / `__POLICY__` placeholders per
    /// budget, then compile and run it over in-memory CSV inputs.
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
        run_config(config, inputs, output_name)
    }

    /// Map a `backpressure` YAML string to its knob.
    fn backpressure_of(policy: &str) -> BackpressureKnob {
        match policy {
            "spill" => BackpressureKnob::Spill,
            "pause" => BackpressureKnob::Pause,
            "both" => BackpressureKnob::Both,
            other => panic!("unknown backpressure policy: {other}"),
        }
    }

    /// Load a combine fixture from disk and inject the memory config for this
    /// budget, so the on-disk fixture is the single pipeline definition executed
    /// at both the documented small scale (its own CSVs) and the generated spill
    /// scale. The fixtures carry no `memory:` block, so this is the one place the
    /// budget is set; source readers are injected by name, so the fixture's own
    /// `path:` values are never opened.
    fn fixture_config(stem: &str, limit: &str, policy: &str) -> PipelineConfig {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/combine")
            .join(format!("{stem}.yaml"));
        let yaml = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read fixture {}: {e}", path.display()));
        let mut config: PipelineConfig = clinker_plan::yaml::from_str(&yaml)
            .unwrap_or_else(|e| panic!("fixture {stem} must parse: {e}"));
        config.pipeline.memory.limit = Some(limit.to_string());
        config.pipeline.memory.backpressure = backpressure_of(policy);
        config
    }

    /// Execute a fixture pipeline at the given budget over in-memory CSV inputs.
    fn fixture_run(
        stem: &str,
        limit: &str,
        policy: &str,
        inputs: &[(&str, &str)],
        output_name: &str,
    ) -> RunResult {
        run_config(fixture_config(stem, limit, policy), inputs, output_name)
    }

    /// Compile a config and run it over in-memory CSV inputs, capturing the named
    /// output. Source readers are keyed by source-node name; the writer by output
    /// name.
    fn run_config(config: PipelineConfig, inputs: &[(&str, &str)], output_name: &str) -> RunResult {
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
    /// Expressed over the shared [`nested_loop_oracle`].
    fn oracle_band(drivers: &[Row], builds: &[Row]) -> HashSet<(i64, i64)> {
        nested_loop_oracle(
            drivers,
            builds,
            |(_, dk, _), (_, lo, hi)| match (dk, lo, hi) {
                (Some(amount), Some(lo), Some(hi)) => amount >= lo && amount < hi,
                _ => false,
            },
            |_, (did, _, _), _, (bid, _, _)| (*did, *bid),
        )
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

    /// The block-band driver key for driver `i` over `[0, span)`: a fixed prime
    /// stride so the keys tile the covered range rather than cluster, which makes
    /// the sort buffer overflow the tight `soft / 4` threshold into multiple
    /// blocks and spilled runs. Centralized so every band workload tiles the same
    /// way and a tiling change lives in one place.
    fn tiled_key(i: i64, span: i64) -> i64 {
        i.wrapping_mul(2_617).rem_euclid(span)
    }

    /// The `n_builds` contiguous half-open bands `[b*bstep, b*bstep + bstep)` that
    /// tile `[0, span)` (`bstep = span / n_builds`), as `(bid, lo, hi)`.
    /// Centralized so every band workload lays out the same bands; callers that
    /// carry an extra build column (a min-score, a divisor) append it per band.
    fn half_open_bands(n_builds: i64, span: i64) -> Vec<(i64, i64, i64)> {
        let bstep = span / n_builds;
        (0..n_builds)
            .map(|b| (b, b * bstep, b * bstep + bstep))
            .collect()
    }

    /// Half-open band workload: driver `i` carries `k1 = tiled_key(i, span)`, so
    /// incomes tile the covered range; build `b` is the half-open interval
    /// `[b*bstep, b*bstep + bstep)`. Sized so the block-band drain overflows the
    /// tight `soft / 4` sort threshold into multiple blocks and spilled runs.
    /// Returns rows plus CSV bytes for both sides.
    fn band_workload(
        n_drivers: i64,
        n_builds: i64,
        span: i64,
    ) -> (Vec<Row>, Vec<Row>, String, String) {
        let mut drivers = Vec::with_capacity(n_drivers as usize);
        let mut driver_csv = String::from("did,k1\n");
        for i in 0..n_drivers {
            let k1 = tiled_key(i, span);
            drivers.push((i, Some(k1), None));
            driver_csv.push_str(&format!("{i},{k1}\n"));
        }
        let bands = half_open_bands(n_builds, span);
        let mut builds = Vec::with_capacity(n_builds as usize);
        let mut build_csv = String::from("bid,lo,hi\n");
        for (b, lo, hi) in &bands {
            builds.push((*b, Some(*lo), Some(*hi)));
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

    const EQUI_BAND_YAML: &str = r#"
pipeline:
  name: equi_range_band
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
        - { name: region, type: string }
        - { name: k1, type: int }
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      schema:
        - { name: bid, type: int }
        - { name: region, type: string }
        - { name: lo, type: int }
        - { name: hi, type: int }
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.region == builds.region and drivers.k1 >= builds.lo and drivers.k1 < builds.hi"
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

    /// A multi-region equi+range workload: each region carries a full tiling of
    /// `bands_per_region` half-open bands over `[0, span)`, and every driver in a
    /// region falls in exactly one band of ITS region. Returns the driver and
    /// build CSVs plus the sorted `(did, bid)` oracle rows, computed by an
    /// independent nested loop over region equality AND `lo <= k1 < hi`.
    fn equi_band_workload(
        n_drivers: i64,
        regions: i64,
        bands_per_region: i64,
        span: i64,
    ) -> (String, String, Vec<Vec<String>>) {
        // drivers: (did, region_index, k1)
        let mut driver_csv = String::from("did,region,k1\n");
        let mut drivers: Vec<(i64, i64, i64)> = Vec::with_capacity(n_drivers as usize);
        for i in 0..n_drivers {
            let region = i % regions;
            let k1 = tiled_key(i, span);
            drivers.push((i, region, k1));
            driver_csv.push_str(&format!("{i},r{region},{k1}\n"));
        }
        // builds: a full band tiling per region; (bid, region_index, lo, hi)
        let mut build_csv = String::from("bid,region,lo,hi\n");
        let mut builds: Vec<(i64, i64, i64, i64)> = Vec::new();
        for region in 0..regions {
            for (b, lo, hi) in half_open_bands(bands_per_region, span) {
                let bid = region * bands_per_region + b;
                builds.push((bid, region, lo, hi));
                build_csv.push_str(&format!("{bid},r{region},{lo},{hi}\n"));
            }
        }
        let mut expected: Vec<Vec<String>> = Vec::new();
        for (did, dregion, k1) in &drivers {
            for (bid, bregion, lo, hi) in &builds {
                if dregion == bregion && k1 >= lo && k1 < hi {
                    expected.push(vec![did.to_string(), bid.to_string()]);
                }
            }
        }
        expected.sort();
        (driver_csv, build_csv, expected)
    }

    /// The equi+range generalization end-to-end: an equality conjunct
    /// (`region == region`) plus a two-axis band, driven through the real planner
    /// and executor. The planner selects `HashPartitionIEJoin`, which now runs
    /// the block-band path with equality as an added prune axis, so a tight
    /// budget spills the sides and the result still equals the nested-loop oracle
    /// (region equality AND `lo <= k1 < hi`) — byte-identical to the resident
    /// roomy run. Region-only matches (same k1 band, different region) must NOT
    /// appear, proving the per-pair canonical equality re-verify holds through
    /// the spilled block layout.
    #[test]
    fn equi_range_band_matches_oracle_across_budgets() {
        let (driver_csv, build_csv, expected) = equi_band_workload(6_000, 4, 40, 1_000);
        let inputs = [
            ("drivers", driver_csv.as_str()),
            ("builds", build_csv.as_str()),
        ];

        let tight = run(EQUI_BAND_YAML, TIGHT_LIMIT, "spill", &inputs, "out");
        let roomy = run(EQUI_BAND_YAML, ROOMY_LIMIT, "spill", &inputs, "out");

        assert!(
            !expected.is_empty(),
            "the equi+range workload must produce matches"
        );
        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled equi+range block-band output diverged from the region-eq + band oracle"
        );
        assert_eq!(
            rows(&roomy.output),
            expected,
            "resident equi+range block-band output diverged from the region-eq + band oracle"
        );
        assert_eq!(
            tight.output, roomy.output,
            "equi+range output must be byte-identical across budgets"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must have spilled the equi+range block-band sides to disk"
        );
    }

    /// A three-conjunct residual predicate (two-axis band plus a third
    /// cross-input `>` conjunct) emits exactly the oracle's matched rows across
    /// both budgets. The block-band kernel proves the two band axes; the third
    /// conjunct rides the residual re-check filter, so this exercises that filter
    /// through the spilled and resident block layouts alike. Driven through the
    /// `iejoin_three_conjunct_residual` fixture — the single source of truth for
    /// this pipeline shape — with a generated workload large enough to spill.
    #[test]
    fn three_conjunct_residual_matches_oracle_across_budgets() {
        // applicants: income tiles [0, span), score cycles 0..100. tiers: 40
        // half-open bands over [0, span) with min_score = 50, so the third
        // conjunct (score > 50) drops roughly half of every band's
        // otherwise-matching applicants — the residual filter has to fire for the
        // output to match the oracle.
        let span = 1_000i64;
        let n_builds = 40i64;
        // (did, income, score): income tiles the covered range as in every band
        // workload; score cycles 0..100 for the third conjunct.
        let mut applicants: Vec<(i64, i64, i64)> = Vec::new();
        let mut applicant_csv = String::from("did,income,score\n");
        for i in 0..8_000i64 {
            let income = tiled_key(i, span);
            let score = i.rem_euclid(100);
            applicants.push((i, income, score));
            applicant_csv.push_str(&format!("{i},{income},{score}\n"));
        }
        // (bid, lo, hi, min_score): the shared half-open bands, each carrying a
        // constant min-score the residual conjunct filters on.
        let min_score = 50;
        let mut tiers: Vec<(i64, i64, i64, i64)> = Vec::new();
        let mut tier_csv = String::from("bid,lo,hi,min_score\n");
        for (b, lo, hi) in half_open_bands(n_builds, span) {
            tiers.push((b, lo, hi, min_score));
            tier_csv.push_str(&format!("{b},{lo},{hi},{min_score}\n"));
        }
        let expected = expected_band_rows(&nested_loop_oracle(
            &applicants,
            &tiers,
            |(_, income, score), (_, lo, hi, min_score)| {
                income >= lo && income < hi && score > min_score
            },
            |_, (did, _, _), _, (bid, _, _, _)| (*did, *bid),
        ));
        assert!(
            !expected.is_empty(),
            "the three-conjunct workload must produce matches"
        );

        let inputs = [
            ("applicants", applicant_csv.as_str()),
            ("tiers", tier_csv.as_str()),
        ];
        let tight = fixture_run(
            "iejoin_three_conjunct_residual",
            TIGHT_LIMIT,
            "spill",
            &inputs,
            "tiered_out",
        );
        let roomy = fixture_run(
            "iejoin_three_conjunct_residual",
            ROOMY_LIMIT,
            "spill",
            &inputs,
            "tiered_out",
        );
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

    /// on_miss:null_fields under a spilled layout, driven by a real body
    /// evaluator (the unit harness is body-less). Every driver emits exactly one
    /// row: a matched driver carries its first (only, since the bands are
    /// disjoint) build id; a zero-match driver — whether its range key is out of
    /// range or NULL — carries a null build id. A third of the drivers carry a
    /// NULL range key (routed to the deferred scan-phase pile), and the
    /// in-range-format tail past `span` matches no band (routed to the deferred
    /// in-block pile), so the null-fields dispatch reunites BOTH spilled deferred
    /// piles in input order — the residency both spills bound. The per-driver
    /// output must match the oracle identically at the spilled and resident
    /// budgets. Driven through the `iejoin_pure_range_null_fields` fixture — the
    /// single source of truth for this pipeline shape.
    #[test]
    fn null_fields_over_spilled_scan_phase_matches_oracle_across_budgets() {
        let span = 1_000i64;
        let n_builds = 40i64;
        // readings: (did, Option<amount>); None models a NULL range key.
        let mut readings: Vec<(i64, Option<i64>)> = Vec::new();
        let mut reading_csv = String::from("did,amount\n");
        for i in 0..6_000i64 {
            if i.rem_euclid(3) == 0 {
                // NULL range key → scan-phase unmatched → null-fields row.
                reading_csv.push_str(&format!("{i},\n"));
                readings.push((i, None));
            } else {
                // amount tiles [0, span + 200): the tail past `span` matches no
                // band, so it is an in-range-format but zero-match (in-block miss).
                let amount = tiled_key(i, span + 200);
                reading_csv.push_str(&format!("{i},{amount}\n"));
                readings.push((i, Some(amount)));
            }
        }
        // bands: the shared half-open tiling of [0, span).
        let bands = half_open_bands(n_builds, span);
        let mut band_csv = String::from("bid,lo,hi\n");
        for (b, lo, hi) in &bands {
            band_csv.push_str(&format!("{b},{lo},{hi}\n"));
        }
        // match: first over disjoint bands → at most one build per reading; the
        // shared oracle's match set gives that bid, and every unmatched reading
        // (NULL or out-of-range) carries a null bid.
        let expected = expected_null_fields_rows(
            &readings,
            &nested_loop_oracle(
                &readings,
                &bands,
                |(_, amount), (_, lo, hi)| matches!(amount, Some(a) if a >= lo && a < hi),
                |_, (did, _), _, (bid, _, _)| (*did, *bid),
            ),
        );

        let inputs = [
            ("readings", reading_csv.as_str()),
            ("bands", band_csv.as_str()),
        ];
        let tight = fixture_run(
            "iejoin_pure_range_null_fields",
            TIGHT_LIMIT,
            "spill",
            &inputs,
            "banded_out",
        );
        let roomy = fixture_run(
            "iejoin_pure_range_null_fields",
            ROOMY_LIMIT,
            "spill",
            &inputs,
            "banded_out",
        );
        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled null-fields output (incl. both spilled deferred piles) diverged from the oracle"
        );
        assert_eq!(
            rows(&roomy.output),
            expected,
            "resident null-fields output diverged from the oracle"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must have spilled the sides and both deferred piles"
        );
    }

    /// Expected sorted `[did, bid]` output rows for a `match: first` +
    /// on_miss:null_fields band join: every reading emits one row carrying its
    /// matched bid (from the oracle set) or an empty bid when it matched nothing.
    fn expected_null_fields_rows(
        readings: &[(i64, Option<i64>)],
        matched: &HashSet<(i64, i64)>,
    ) -> Vec<Vec<String>> {
        let by_did: HashMap<i64, i64> = matched.iter().copied().collect();
        let mut rows: Vec<Vec<String>> = readings
            .iter()
            .map(|(did, _)| {
                let bid = by_did.get(did).map(i64::to_string).unwrap_or_default();
                vec![did.to_string(), bid]
            })
            .collect();
        rows.sort();
        rows
    }

    /// Read a fixture data CSV (under `tests/fixtures/combine/data`) and return
    /// its raw bytes plus its numeric body parsed into rows of optional `i64`
    /// cells — an empty field is `None`, modelling a NULL range key. The raw bytes
    /// feed the pipeline and the parsed rows feed the oracle, so the on-disk CSV is
    /// the single source both check against.
    fn read_fixture_data(name: &str) -> (String, Vec<Vec<Option<i64>>>) {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/combine/data")
            .join(name);
        let csv = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("cannot read fixture data {}: {e}", path.display()));
        let parsed = csv::ReaderBuilder::new()
            .has_headers(true)
            .from_reader(csv.as_bytes())
            .records()
            .map(|r| {
                r.expect("fixture data row parses")
                    .iter()
                    .map(|cell| {
                        if cell.is_empty() {
                            None
                        } else {
                            Some(cell.parse::<i64>().expect("integer fixture cell"))
                        }
                    })
                    .collect()
            })
            .collect();
        (csv, parsed)
    }

    /// Both new pure-range fixtures, executed end-to-end over their OWN documented
    /// CSV data, match the shared nested-loop oracle — so the "Expected matches"
    /// each fixture documents are asserted against the real pipeline, not left to
    /// drift. Runs at the roomy budget: the data is tiny, so correctness (not
    /// spill) is the point here.
    #[test]
    fn fixtures_match_oracle_over_documented_data() {
        // Three-conjunct residual: applicants (did, income, score) ⋈ tiers
        // (bid, lo, hi, min_score) on income ∈ [lo, hi) AND score > min_score,
        // match: all. The fixture's own data has no NULLs, so every cell is Some.
        let (applicant_csv, applicants) = read_fixture_data("iejoin_3c_drivers.csv");
        let (tier_csv, tiers) = read_fixture_data("iejoin_3c_builds.csv");
        let expected = expected_band_rows(&nested_loop_oracle(
            &applicants,
            &tiers,
            |a, t| {
                a[1].unwrap() >= t[1].unwrap()
                    && a[1].unwrap() < t[2].unwrap()
                    && a[2].unwrap() > t[3].unwrap()
            },
            |_, a, _, t| (a[0].unwrap(), t[0].unwrap()),
        ));
        assert_eq!(
            expected,
            vec![
                vec!["2".to_string(), "10".to_string()],
                vec!["4".to_string(), "11".to_string()]
            ],
            "the three-conjunct fixture's documented matches changed; update the fixture comment"
        );
        let out = fixture_run(
            "iejoin_three_conjunct_residual",
            ROOMY_LIMIT,
            "spill",
            &[("applicants", &applicant_csv), ("tiers", &tier_csv)],
            "tiered_out",
        );
        assert_eq!(
            rows(&out.output),
            expected,
            "three-conjunct fixture output diverged from the oracle over its documented data"
        );

        // Null-fields: readings (did, amount) ⋈ bands (bid, lo, hi) on
        // amount ∈ [lo, hi), match: first, on_miss: null_fields — every reading
        // emits a row (matched bid or null). The fixture's reading 2 has a NULL
        // amount and reading 4 (amount 999) is out of range, so both emit null.
        let (reading_csv, reading_rows) = read_fixture_data("iejoin_prnf_drivers.csv");
        let (band_csv, band_rows) = read_fixture_data("iejoin_prnf_builds.csv");
        let readings: Vec<(i64, Option<i64>)> =
            reading_rows.iter().map(|r| (r[0].unwrap(), r[1])).collect();
        let bands: Vec<(i64, i64, i64)> = band_rows
            .iter()
            .map(|r| (r[0].unwrap(), r[1].unwrap(), r[2].unwrap()))
            .collect();
        let expected = expected_null_fields_rows(
            &readings,
            &nested_loop_oracle(
                &readings,
                &bands,
                |(_, amount), (_, lo, hi)| matches!(amount, Some(a) if a >= lo && a < hi),
                |_, (did, _), _, (bid, _, _)| (*did, *bid),
            ),
        );
        // Reading 1 matches band 10, reading 3 matches band 11; reading 2 (NULL
        // amount) and reading 4 (out of range) carry a null bid.
        assert_eq!(
            expected,
            vec![
                vec!["1".to_string(), "10".to_string()],
                vec!["2".to_string(), String::new()],
                vec!["3".to_string(), "11".to_string()],
                vec!["4".to_string(), String::new()],
            ],
            "the null-fields fixture's documented result changed; update the fixture comment"
        );
        let out = fixture_run(
            "iejoin_pure_range_null_fields",
            ROOMY_LIMIT,
            "spill",
            &[("readings", &reading_csv), ("bands", &band_csv)],
            "banded_out",
        );
        assert_eq!(
            rows(&out.output),
            expected,
            "null-fields fixture output diverged from the oracle over its documented data"
        );
    }

    const ALL_OUT_OF_RANGE_NULL_FIELDS_YAML: &str = r#"
pipeline:
  name: pure_range_all_out_of_range_null_fields
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

    /// The pathological in-block-pile shape: every driver carries an
    /// orderable range key that falls OUTSIDE every build range, so each enters a
    /// block but matches nothing and routes to the deferred in-block on-miss pile
    /// — under match: all + on_miss: null_fields the maximal case, holding one
    /// deferred record per driver. The pile drains into its spillable buffer and
    /// streams back through the two-way merge, so the per-driver null-fields output
    /// is byte-identical across a spilling (tight) and a resident (roomy) budget,
    /// and the tight run bounds the whole pile to disk rather than holding an O(N)
    /// resident vec.
    #[test]
    fn all_out_of_range_inblock_pile_null_fields_matches_across_budgets() {
        let span = 1_000i64;
        let n_builds = 40i64;
        let bstep = span / n_builds;
        // Every driver's k1 is >= span, so it falls outside every band and is an
        // in-block miss that emits a null build id.
        let mut driver_csv = String::from("did,k1\n");
        let mut expected: Vec<Vec<String>> = Vec::new();
        for i in 0..6_000i64 {
            let k1 = span + i.rem_euclid(500);
            driver_csv.push_str(&format!("{i},{k1}\n"));
            expected.push(vec![i.to_string(), String::new()]);
        }
        expected.sort();
        let mut build_csv = String::from("bid,lo,hi\n");
        for b in 0..n_builds {
            let lo = b * bstep;
            let hi = lo + bstep;
            build_csv.push_str(&format!("{b},{lo},{hi}\n"));
        }
        let inputs = [
            ("drivers", driver_csv.as_str()),
            ("builds", build_csv.as_str()),
        ];
        let tight = run(
            ALL_OUT_OF_RANGE_NULL_FIELDS_YAML,
            TIGHT_LIMIT,
            "spill",
            &inputs,
            "out",
        );
        let roomy = run(
            ALL_OUT_OF_RANGE_NULL_FIELDS_YAML,
            ROOMY_LIMIT,
            "spill",
            &inputs,
            "out",
        );
        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled all-out-of-range in-block pile diverged from the resident run"
        );
        assert_eq!(
            rows(&roomy.output),
            expected,
            "resident all-out-of-range null-fields output unexpected"
        );
        assert_eq!(
            rows(&tight.output).len(),
            6_000,
            "every driver emits exactly one null-fields row"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must bound the 6000-driver in-block pile to disk"
        );
    }

    /// The adversarial finalize shape: a match: all + on_miss: null_fields input
    /// dominated by zero-match drivers split across BOTH deferred piles — NULL /
    /// non-orderable range keys route to the scan-phase pile, orderable-but-out-of-
    /// range keys to the in-block pile — with only a minority actually matching.
    /// Under a tight budget both piles spill heavily, so at end-of-join the deferred
    /// dispatch holds the scan-phase AND the in-block k-way mergers open at once,
    /// alongside the still-resident sides and the output sort. That finalize phase
    /// is the one the per-pair gates do not cover; the finalize gate bounds it. This
    /// asserts the run completes within the tight budget rather than OOMing, and
    /// that its per-driver output is byte-identical to the roomy resident run — the
    /// determinism-across-budgets contract, now over the two-merger finalize.
    #[test]
    fn all_both_deferred_piles_dominant_null_fields_matches_across_budgets() {
        let span = 1_000i64;
        let n_builds = 40i64;
        // 40% NULL range key (scan-phase pile), 40% orderable-out-of-range
        // (in-block pile), 20% in-range (matched): the run is dominated by misses,
        // and both piles are large enough to overflow the tight sort threshold.
        let mut driver_csv = String::from("did,k1\n");
        let mut readings: Vec<(i64, Option<i64>)> = Vec::new();
        for i in 0..6_000i64 {
            match i.rem_euclid(10) {
                0..=3 => {
                    driver_csv.push_str(&format!("{i},\n"));
                    readings.push((i, None));
                }
                4..=7 => {
                    let k1 = span + i.rem_euclid(500);
                    driver_csv.push_str(&format!("{i},{k1}\n"));
                    readings.push((i, Some(k1)));
                }
                _ => {
                    let k1 = tiled_key(i, span);
                    driver_csv.push_str(&format!("{i},{k1}\n"));
                    readings.push((i, Some(k1)));
                }
            }
        }
        let bands = half_open_bands(n_builds, span);
        let mut build_csv = String::from("bid,lo,hi\n");
        for (b, lo, hi) in &bands {
            build_csv.push_str(&format!("{b},{lo},{hi}\n"));
        }
        // Disjoint half-open bands → each in-range driver matches exactly one build,
        // so match: all still emits one row per driver; every miss (NULL or
        // out-of-range) carries a null build id.
        let expected = expected_null_fields_rows(
            &readings,
            &nested_loop_oracle(
                &readings,
                &bands,
                |(_, amount), (_, lo, hi)| matches!(amount, Some(a) if a >= lo && a < hi),
                |_, (did, _), _, (bid, _, _)| (*did, *bid),
            ),
        );
        let inputs = [
            ("drivers", driver_csv.as_str()),
            ("builds", build_csv.as_str()),
        ];
        let tight = run(
            ALL_OUT_OF_RANGE_NULL_FIELDS_YAML,
            TIGHT_LIMIT,
            "spill",
            &inputs,
            "out",
        );
        let roomy = run(
            ALL_OUT_OF_RANGE_NULL_FIELDS_YAML,
            ROOMY_LIMIT,
            "spill",
            &inputs,
            "out",
        );
        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled two-pile finalize output diverged from the oracle"
        );
        assert_eq!(
            rows(&tight.output),
            rows(&roomy.output),
            "two-pile finalize output changed between the tight and roomy budgets"
        );
        assert_eq!(
            rows(&tight.output).len(),
            6_000,
            "every driver emits exactly one row under match: all + null_fields over disjoint bands"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must spill both deferred piles and the sides to disk"
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
            let k1 = tiled_key(i, span);
            driver_csv.push_str(&format!("{i},{k1}\n"));
            let band = k1 / bstep;
            let factor = factor_of(band);
            if factor != 0 {
                let ratio = k1 / factor;
                expected.push(vec![i.to_string(), band.to_string(), ratio.to_string()]);
            }
        }
        expected.sort();
        // The shared half-open bands, each carrying the divisor the body reads.
        let mut build_csv = String::from("bid,lo,hi,factor\n");
        for (b, lo, hi) in half_open_bands(n_builds, span) {
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

    // ── decimal + mixed-numeric range axes ───────────────────────────────
    //
    // The block-band numeric axis carries range keys as i128 so a decimal band
    // join (monetary bracket/tier lookups) and a mixed integer/float range join
    // land in one order-preserving space instead of silently dropping every row.
    // These oracles compare the engine to a brute-force reference over the REAL
    // typed values, across the tight (spilling) and roomy (resident) budgets, so
    // the decimal keys flowing through the block-band drain/sort/spill are proven
    // deterministic and byte-identical regardless of memory pressure.

    use rust_decimal::Decimal;

    /// One decimal-band driver row (`id`, `amount`) and build row
    /// (`id`, `lo`, `hi`); a mixed-band build row carries float bounds. Named so
    /// the workload return types stay readable (mirrors the `Row` alias above).
    type DecDriver = (i64, Decimal);
    type DecBuild = (i64, Decimal, Decimal);
    type MixBuild = (i64, f64, f64);

    const DEC_BAND_YAML: &str = r#"
pipeline:
  name: pure_range_decimal_band
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
        - { name: amount, type: decimal, scale: 4 }
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      schema:
        - { name: bid, type: int }
        - { name: lo, type: decimal, scale: 2 }
        - { name: hi, type: decimal, scale: 2 }
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.amount >= builds.lo and drivers.amount < builds.hi"
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

    /// Half-open decimal band workload: driver `i` carries a scale-4 amount
    /// tiling `[0, span)` ten-thousandths, band `b` is the half-open dollar
    /// interval `[b.00, (b+1).00)` at scale 2. The two sides deliberately carry
    /// DIFFERENT scales so the amounts that land exactly on a whole-dollar floor
    /// (e.g. `5.0000` vs `5.00`) exercise the fixed-point grid's scale-invariance
    /// (`2.5` and `2.50` compare equal). Sized to overflow the tight sort
    /// threshold into spilled runs.
    fn decimal_band_workload() -> (Vec<DecDriver>, Vec<DecBuild>, String, String) {
        let n_builds = 40i64;
        let span_e4 = n_builds * 10_000; // [0.0000, 40.0000) as ten-thousandths
        let n_drivers = 8_000i64;
        let mut drivers = Vec::with_capacity(n_drivers as usize);
        let mut driver_csv = String::from("did,amount\n");
        for i in 0..n_drivers {
            let amount = Decimal::new(tiled_key(i, span_e4), 4);
            drivers.push((i, amount));
            driver_csv.push_str(&format!("{i},{amount}\n"));
        }
        // Explicit cross-scale equality boundaries: a scale-4 amount exactly
        // equal to a scale-2 band floor must match that band (inclusive `>=`),
        // proving `b.0000 == b.00` on the grid.
        for (k, b) in [(0i64, 7i64), (1, 13), (2, 25), (3, 39)] {
            let did = n_drivers + k;
            let amount = Decimal::new(b * 10_000, 4); // b.0000
            drivers.push((did, amount));
            driver_csv.push_str(&format!("{did},{amount}\n"));
        }
        let mut builds = Vec::with_capacity(n_builds as usize);
        let mut build_csv = String::from("bid,lo,hi\n");
        for b in 0..n_builds {
            let lo = Decimal::new(b * 100, 2); // b.00
            let hi = Decimal::new((b + 1) * 100, 2); // (b+1).00
            builds.push((b, lo, hi));
            build_csv.push_str(&format!("{b},{lo},{hi}\n"));
        }
        (drivers, builds, driver_csv, build_csv)
    }

    /// Nested-loop oracle for a decimal half-open band (`amount >= lo AND
    /// amount < hi`) over exact `Decimal` values, as `(driver_id, build_id)`.
    fn decimal_oracle_band(drivers: &[DecDriver], builds: &[DecBuild]) -> HashSet<(i64, i64)> {
        let mut out = HashSet::new();
        for (did, amount) in drivers {
            for (bid, lo, hi) in builds {
                if amount >= lo && amount < hi {
                    out.insert((*did, *bid));
                }
            }
        }
        out
    }

    /// A decimal band join emits exactly the `Decimal`-oracle match set and does
    /// so identically at a spilling and a resident budget — the decimal keys ride
    /// the same i128 block-band drain/sort/spill, so the result is budget-
    /// independent. Cross-scale floors (`5.0000` vs `5.00`) match, proving the
    /// fixed-point grid is scale-invariant end to end.
    #[test]
    fn decimal_band_matches_oracle_across_budgets() {
        let (drivers, builds, driver_csv, build_csv) = decimal_band_workload();
        let inputs = [
            ("drivers", driver_csv.as_str()),
            ("builds", build_csv.as_str()),
        ];
        let expected = expected_band_rows(&decimal_oracle_band(&drivers, &builds));
        assert!(
            !expected.is_empty(),
            "the decimal band workload must produce matches"
        );

        let tight = run(DEC_BAND_YAML, TIGHT_LIMIT, "spill", &inputs, "out");
        let roomy = run(DEC_BAND_YAML, ROOMY_LIMIT, "spill", &inputs, "out");
        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled decimal band output diverged from the Decimal oracle"
        );
        assert_eq!(
            rows(&roomy.output),
            expected,
            "resident decimal band output diverged from the Decimal oracle"
        );
        assert_eq!(
            rows(&tight.output),
            rows(&roomy.output),
            "the decimal band result must be byte-identical across budgets"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must have spilled the decimal band sides to disk"
        );
    }

    const MIXED_BAND_YAML: &str = r#"
pipeline:
  name: pure_range_mixed_band
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
        - { name: amount, type: int }
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      schema:
        - { name: bid, type: int }
        - { name: lo, type: float }
        - { name: hi, type: float }
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.amount >= builds.lo and drivers.amount < builds.hi"
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

    /// Mixed integer/float band workload: driver `i` carries an INTEGER amount
    /// tiling `[0, span)`; band `b` is the FLOAT half-open interval
    /// `[b·bstep + 0.5, (b+1)·bstep + 0.5)`. The `.5` offsets keep every integer
    /// amount strictly off a band boundary, so the match is unambiguous. The
    /// integer and float sides must reduce to one comparable axis (the engine
    /// coerces the integer through `f64`, mirroring `int <=> float` comparison).
    fn mixed_band_workload() -> (Vec<(i64, i64)>, Vec<MixBuild>, String, String) {
        let span = 1_000i64;
        let n_builds = 40i64;
        let bstep = span / n_builds;
        let n_drivers = 8_000i64;
        let mut drivers = Vec::with_capacity(n_drivers as usize);
        let mut driver_csv = String::from("did,amount\n");
        for i in 0..n_drivers {
            let amount = tiled_key(i, span);
            drivers.push((i, amount));
            driver_csv.push_str(&format!("{i},{amount}\n"));
        }
        let mut builds = Vec::with_capacity(n_builds as usize);
        let mut build_csv = String::from("bid,lo,hi\n");
        for b in 0..n_builds {
            let lo = (b * bstep) as f64 + 0.5;
            let hi = ((b + 1) * bstep) as f64 + 0.5;
            builds.push((b, lo, hi));
            build_csv.push_str(&format!("{b},{lo},{hi}\n"));
        }
        (drivers, builds, driver_csv, build_csv)
    }

    /// A mixed int/float range join emits exactly the reference match set —
    /// computed by coercing the integer amount through `f64`, the same widening
    /// the runtime comparison applies — identically across budgets.
    #[test]
    fn mixed_int_float_band_matches_oracle_across_budgets() {
        let (drivers, builds, driver_csv, build_csv) = mixed_band_workload();
        let inputs = [
            ("drivers", driver_csv.as_str()),
            ("builds", build_csv.as_str()),
        ];
        let expected = expected_band_rows(&nested_loop_oracle(
            &drivers,
            &builds,
            |(_, amount), (_, lo, hi)| (*amount as f64) >= *lo && (*amount as f64) < *hi,
            |_, (did, _), _, (bid, _, _)| (*did, *bid),
        ));
        assert!(
            !expected.is_empty(),
            "the mixed int/float band workload must produce matches"
        );

        let tight = run(MIXED_BAND_YAML, TIGHT_LIMIT, "spill", &inputs, "out");
        let roomy = run(MIXED_BAND_YAML, ROOMY_LIMIT, "spill", &inputs, "out");
        assert_eq!(
            rows(&tight.output),
            expected,
            "spilled mixed int/float band diverged from the f64-coerced oracle"
        );
        assert_eq!(
            rows(&roomy.output),
            expected,
            "resident mixed int/float band diverged from the f64-coerced oracle"
        );
        assert_eq!(
            rows(&tight.output),
            rows(&roomy.output),
            "the mixed band result must be byte-identical across budgets"
        );
        assert!(
            tight.spill_bytes > 0,
            "the tight budget must have spilled the mixed band sides to disk"
        );
    }

    const OVERFLOW_YAML: &str = r#"
pipeline:
  name: decimal_range_overflow
  memory: { limit: "512M", backpressure: spill }
nodes:
  - type: source
    name: drivers
    config:
      name: drivers
      type: csv
      path: drivers.csv
      schema:
        - { name: did, type: int }
        - { name: amount, type: decimal, scale: 0 }
  - type: source
    name: builds
    config:
      name: builds
      type: csv
      path: builds.csv
      schema:
        - { name: bid, type: int }
        - { name: threshold, type: decimal, scale: 0 }
  - type: combine
    name: banded
    input:
      drivers: drivers
      builds: builds
    config:
      where: "drivers.amount >= builds.threshold"
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

    /// Compile and run a pipeline over in-memory CSVs, returning the raw
    /// execution result so a test can assert a fail-loud runtime error rather
    /// than unwrapping it. Mirrors [`run_config`] minus the success assertion.
    fn run_config_result(
        config: PipelineConfig,
        inputs: &[(&str, &str)],
        output_name: &str,
    ) -> Result<(), clinker_plan::PipelineError> {
        let plan = config
            .compile(&CompileContext::default())
            .expect("overflow yaml must compile");
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
        PipelineExecutor::run_plan_with_readers_writers(&plan, readers, writers, &params)
            .map(|_| ())
    }

    /// A decimal range value whose scaled magnitude overflows the fixed-point
    /// axis fails LOUD with E326 rather than dropping the row or emitting a wrong
    /// match. `Decimal::MAX` scaled onto the 10^18 grid exceeds `i128`, so the
    /// scan raises `CombineRangeKeyOutOfRange` naming the combine.
    #[test]
    fn decimal_range_value_beyond_fixed_point_fails_loud() {
        // Decimal::MAX ≈ 7.9e28; on the 10^18 grid that is ~7.9e46, far past i128.
        let d_csv = "did,amount\n1,79228162514264337593543950335\n";
        let b_csv = "bid,threshold\n1,1\n";
        let config: PipelineConfig =
            clinker_plan::yaml::from_str(OVERFLOW_YAML).expect("overflow yaml must parse");
        let err = run_config_result(config, &[("drivers", d_csv), ("builds", b_csv)], "out")
            .expect_err("a decimal range value beyond the fixed-point axis must fail loud");
        match err {
            clinker_plan::PipelineError::CombineRangeKeyOutOfRange { combine, .. } => {
                assert_eq!(combine, "banded", "E326 must name the offending combine");
            }
            other => panic!("expected E326 CombineRangeKeyOutOfRange, got {other:?}"),
        }
    }
}
