//! `ExactSum` checked against an independent exact float sum.
//!
//! `bitrep::SumF64` is a separately written fixed-point exact accumulator. It
//! is a dev-dependency of this crate only, used here as an oracle, and never
//! reaches a runtime build. Every comparison is bit for bit on the correctly
//! rounded sum of finite addends: fixed cases built to defeat compensated
//! summation, and ten thousand seeded sequences mixing exponents across the
//! whole finite range, signs and cancellation, folded, split and merged both
//! ways, and with one addend subtracted.
//!
//! Non-finite addends and signed zero are not compared: the oracle keeps NaN
//! and the infinities as flags that a subtraction cannot remove, and returns
//! `+0.0` for every exact zero, while `ExactSum` counts them so a retraction
//! is exact and follows IEEE-754's signed-zero rule. The accumulator's unit
//! tests assert those rules directly.

use bitrep::SumF64;
use clinker_record::accumulator::ExactSum;

fn exact(values: &[f64]) -> ExactSum {
    let mut sum = ExactSum::new();
    for value in values {
        sum.add_f64(*value);
    }
    sum
}

fn oracle(values: &[f64]) -> SumF64 {
    let mut sum = SumF64::new();
    for value in values {
        sum.add(*value);
    }
    sum
}

fn assert_same_bits(ours: f64, theirs: f64, context: &str) {
    assert_eq!(
        ours.to_bits(),
        theirs.to_bits(),
        "{context}: ExactSum gives {ours:e}, the oracle {theirs:e}"
    );
}

/// A deterministic generator (SplitMix64), so every run checks the same
/// sequences.
struct SplitMix(u64);

impl SplitMix {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    /// A finite float with the given biased exponent (0 for a subnormal), a
    /// random fraction and a random sign.
    fn float_at(&mut self, exponent: u64) -> f64 {
        let fraction = self.next() & ((1 << 52) - 1);
        let sign = self.next() & (1 << 63);
        finite(f64::from_bits(sign | (exponent << 52) | fraction))
    }

    /// One sequence of 1 to 200 finite addends: values anywhere in the finite
    /// range, values clustered around one exponent so they interact, exact and
    /// near cancellations of earlier values, small integers and subnormals.
    fn sequence(&mut self) -> Vec<f64> {
        if self.below(8) == 0 {
            return self.tie_sequence();
        }
        let len = 1 + self.below(200) as usize;
        let center = self.below(2047);
        let mut values: Vec<f64> = Vec::with_capacity(len);
        while values.len() < len {
            let value = match self.below(7) {
                0 => {
                    let exponent = self.below(2047);
                    self.float_at(exponent)
                }
                1 | 2 => {
                    let exponent = (center + self.below(121)).saturating_sub(60).min(2046);
                    self.float_at(exponent)
                }
                3 if !values.is_empty() => {
                    let earlier = values[self.below(values.len() as u64) as usize];
                    -earlier
                }
                4 if !values.is_empty() => {
                    let earlier = values[self.below(values.len() as u64) as usize];
                    let perturbed = f64::from_bits(earlier.to_bits() ^ (self.next() & 0xFF));
                    -finite(perturbed)
                }
                5 => (self.below(2001) as f64) - 1000.0,
                _ => self.float_at(0),
            };
            values.push(finite(value));
        }
        values
    }

    /// A sequence whose exact sum lies exactly halfway between two doubles
    /// (or, half the time, a smallest subnormal past halfway): a float `x`,
    /// half of `x`'s spacing with either sign, and pairs of values that cancel
    /// exactly, shuffled. Random sequences almost never land on a tie, and a
    /// tie is where rounding to even differs from other rules.
    fn tie_sequence(&mut self) -> Vec<f64> {
        let exponent = 2 + self.below(2044);
        let x = self.float_at(exponent);
        // x's spacing is 2^(exponent - 1075); half of it is a normal float
        // from biased exponent 54 up, else a subnormal.
        let half_spacing = if exponent >= 54 {
            f64::from_bits((exponent - 53) << 52)
        } else {
            f64::from_bits(1 << (exponent - 2))
        };
        let mut values = vec![x];
        values.push(if self.next() & 1 == 0 {
            half_spacing
        } else {
            -half_spacing
        });
        if self.next() & 1 == 0 {
            let tiny = f64::from_bits(1);
            values.push(if self.next() & 1 == 0 { tiny } else { -tiny });
        }
        for _ in 0..self.below(50) {
            let magnitude = self.below(exponent + 1);
            let y = self.float_at(magnitude);
            values.push(y);
            values.push(-y);
        }
        for i in (1..values.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            values.swap(i, j);
        }
        values.into_iter().map(finite).collect()
    }
}

/// `value` with a negative zero made positive: the oracle drops the sign of
/// zero, so sequences hold only `+0.0`.
fn finite(value: f64) -> f64 {
    assert!(value.is_finite(), "the generator makes finite values");
    if value == 0.0 { 0.0 } else { value }
}

fn powi2(exponent: i32) -> f64 {
    2.0_f64.powi(exponent)
}

#[test]
fn exact_sum_matches_bitrep_on_ill_conditioned_cases() {
    let tiny = f64::from_bits(1);
    let largest_subnormal = f64::from_bits((1 << 52) - 1);
    let mut cases: Vec<(&str, Vec<f64>)> = vec![
        ("cancelling 1e16", vec![1e16, 1.0, -1e16]),
        ("ten tenths", vec![0.1; 10]),
        (
            "f64::MAX twice and back",
            vec![f64::MAX, f64::MAX, -f64::MAX],
        ),
        ("f64::MAX overflow", vec![f64::MAX, f64::MAX]),
        ("negative overflow", vec![-f64::MAX, -f64::MAX, 1.0]),
        (
            "subnormals",
            vec![tiny, 2.0 * tiny, 3.0 * tiny, -4.0 * tiny, 5.0 * tiny],
        ),
        ("ten thousand tenths", vec![0.1; 10_000]),
        (
            "straddling the subnormal boundary",
            vec![
                f64::MIN_POSITIVE,
                -largest_subnormal,
                largest_subnormal,
                tiny,
                -f64::MIN_POSITIVE / 2.0,
                f64::MIN_POSITIVE * 3.0,
            ],
        ),
        ("tie to even", vec![powi2(53), 1.0]),
        ("tie up", vec![powi2(53), 3.0]),
        ("sticky bit", vec![powi2(53), 1.0, tiny]),
        ("exact zero", vec![1.0, -1.0, 0.5, -0.5]),
    ];
    let mut one_and_tenths = vec![1.0];
    one_and_tenths.extend([1e-16; 10]);
    cases.push(("one and ten 1e-16", one_and_tenths));
    // Alternating +/-2^1000 with small values in between: every partial sum
    // of the large values is 0 or 2^1000, which swamps the small ones.
    let mut alternating = Vec::new();
    for i in 0..1_000 {
        alternating.push(if i % 2 == 0 {
            powi2(1000)
        } else {
            -powi2(1000)
        });
        alternating.push(f64::from(i) * 1e-3 + 0.1);
        alternating.push(-powi2(-900) * f64::from(i));
    }
    cases.push(("alternating 2^1000", alternating));

    for (name, values) in &cases {
        assert_same_bits(exact(values).round_with(0), oracle(values).value(), name);
        let mut reversed = values.clone();
        reversed.reverse();
        assert_same_bits(
            exact(&reversed).round_with(0),
            oracle(values).value(),
            &format!("{name}, reversed"),
        );
    }
}

#[test]
fn exact_sum_matches_bitrep_on_generated_sequences() {
    let mut rng = SplitMix(0x0E5A_C75B);
    for index in 0..10_000 {
        let values = rng.sequence();
        let ours = exact(&values);
        assert_same_bits(
            ours.round_with(0),
            oracle(&values).value(),
            &format!("sequence {index} ({} values)", values.len()),
        );
        // An integer part up to 2^53 is an exact float, so the oracle can
        // take it as one more addend.
        let int_part = rng.below(1 << 54) as i64 - (1 << 53);
        let mut with_int = values.clone();
        with_int.push(int_part as f64);
        assert_same_bits(
            ours.round_with(i128::from(int_part)),
            oracle(&with_int).value(),
            &format!("sequence {index} with integer part {int_part}"),
        );
    }
}

#[test]
fn split_and_merge_matches_bitrep() {
    let mut rng = SplitMix(0x5B11_73E6);
    for index in 0..10_000 {
        let values = rng.sequence();
        let whole = oracle(&values).value();
        let at = rng.below(values.len() as u64 + 1) as usize;
        let (left, right) = values.split_at(at);

        let mut theirs = oracle(left);
        theirs.merge(&oracle(right));
        assert_same_bits(
            theirs.value(),
            whole,
            &format!("sequence {index}: the oracle's own merge"),
        );
        let mut left_right = exact(left);
        left_right.merge(&exact(right));
        let mut right_left = exact(right);
        right_left.merge(&exact(left));
        for (order, merged) in [("left+right", left_right), ("right+left", right_left)] {
            assert_same_bits(
                merged.round_with(0),
                theirs.value(),
                &format!("sequence {index} split at {at}, {order}"),
            );
        }

        // Subtract one addend: the result is the survivors' sum.
        let removed = rng.below(values.len() as u64) as usize;
        let mut ours = exact(&values);
        ours.sub_f64(values[removed]);
        let survivors: Vec<f64> = values
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != removed)
            .map(|(_, v)| *v)
            .collect();
        let mut unmerged = oracle(&values);
        assert!(unmerged.try_unmerge(&oracle(&[values[removed]])));
        assert_same_bits(
            unmerged.value(),
            oracle(&survivors).value(),
            &format!("sequence {index}: the oracle's own subtraction"),
        );
        assert_same_bits(
            ours.round_with(0),
            oracle(&survivors).value(),
            &format!("sequence {index} without addend {removed}"),
        );
        assert_eq!(
            ours,
            exact(&survivors),
            "sequence {index}: state after subtraction"
        );
    }
}

/// `int_part` as three float addends whose exact sum is `int_part`: bits
/// [0, 42) and [42, 84) unsigned, and the signed remainder from bit 84, each
/// scaled by its power of two. Each part has at most 44 significant bits, so
/// every addend is an exact float, which a single `int_part as f64` is not
/// beyond 2^53.
fn integer_part_addends(int_part: i128) -> [f64; 3] {
    let mask: i128 = (1 << 42) - 1;
    let low = (int_part & mask) as f64;
    let middle = ((int_part >> 42) & mask) as f64 * powi2(42);
    let high = (int_part >> 84) as f64 * powi2(84);
    [low, middle, high]
}

/// An integer part anywhere in the `i128` range, not only up to 2^53 where
/// it is itself an exact float, rounds together with the float addends once.
#[test]
fn exact_sum_matches_bitrep_with_integer_parts_across_i128() {
    let fixed: [i128; 4] = [i128::MIN, i128::MAX, (1 << 53) + 1, -(1 << 100) + 3];
    let mut rng = SplitMix(0x1A7E_6E12);
    for index in 0..2_000 {
        let values = rng.sequence();
        let ours = exact(&values);
        let random = ((u128::from(rng.next()) << 64) | u128::from(rng.next())) as i128;
        for int_part in fixed.into_iter().chain([random]) {
            let mut with_int = values.clone();
            with_int.extend(integer_part_addends(int_part));
            assert_same_bits(
                ours.round_with(int_part),
                oracle(&with_int).value(),
                &format!("sequence {index} with integer part {int_part}"),
            );
        }
    }
}
