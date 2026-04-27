//! Property-test scaffolding for the IEJoin algorithm.
//!
//! The proptest harness here generates arbitrary numeric record pairs
//! and an operator-pair under which a future IEJoin implementation can
//! be checked against a brute-force nested-loop oracle. The oracle is
//! the load-bearing piece — once it's a real, tested implementation,
//! the oracle assertion in the IEJoin commit becomes a one-line
//! `prop_assert_eq!` call.
//!
//! The fixture-existence test exercises the seven IEJoin YAML fixtures
//! end-to-end through the YAML parser. Their runtime behaviour will be
//! covered separately by the IEJoin executor commit.
use std::collections::HashSet;

use clinker_core::config::PipelineConfig;
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
/// Runtime execution of these fixtures requires the IEJoin executor —
/// out of scope for this commit. Parse-time validation is enough to
/// pin the schema shape and catch typos before the algorithm lands.
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
        clinker_core::yaml::from_str::<PipelineConfig>(&yaml)
            .unwrap_or_else(|e| panic!("fixture parse error for {stem}: {e}"));
    }
}
