//! `ExactDecimalSum` checked against two independent references.
//!
//! The first is an exact arbitrary-precision total, `num_bigint::BigInt`,
//! reduced to a decimal by a half-even reduction written here from the rule:
//! the result scale is the largest addend scale, and when the mantissa at that
//! scale needs more than 96 bits the exact total is divided once by the
//! smallest power of ten whose half-even quotient fits; no fitting scale down
//! to 0 means out of range. The second is `rust_decimal`'s own correctly
//! rounded addition: the exact total split into two decimals that add to it
//! exactly, added once. `num-bigint` is a dev-dependency of this crate only,
//! used here as an oracle, and never reaches a runtime build.
//!
//! Ten thousand seeded sequences of up to 200 decimals are folded, split and
//! merged both ways, and checked with one addend subtracted: mantissas of up
//! to 96 bits at scales 0 to 28, both signs, exact cancellations, integer
//! parts across the `i128` range, and one sequence in eight built to land
//! exactly on a half-way tie, or one unit past it, where rounding half-even
//! differs from other rules.

use clinker_record::accumulator::ExactDecimalSum;
use num_bigint::{BigInt, BigUint, Sign};
use rust_decimal::Decimal;

/// A decimal's largest mantissa is `2^96 - 1`.
const MANTISSA_BITS: u64 = 96;

/// The decimal unit is `10^-28`.
const MAX_SCALE: u32 = 28;

fn ten_to(exponent: u32) -> BigUint {
    BigUint::from(10_u32).pow(exponent)
}

fn to_u128(value: &BigUint) -> u128 {
    let digits = value.to_u64_digits();
    assert!(digits.len() <= 2, "{value} does not fit a u128");
    digits
        .iter()
        .rev()
        .fold(0_u128, |acc, digit| (acc << 64) | u128::from(*digit))
}

/// What the rule gives for `values` plus `int_part`: `Some((mantissa, scale,
/// dropped))` with the number of digits the reduction dropped, or `None` when
/// the total is out of range.
fn reference(values: &[Decimal], int_part: i128) -> Option<(i128, u32, u32)> {
    let mut total = BigInt::from(int_part) * BigInt::from_biguint(Sign::Plus, ten_to(MAX_SCALE));
    for value in values {
        total += BigInt::from(value.mantissa())
            * BigInt::from_biguint(Sign::Plus, ten_to(MAX_SCALE - value.scale()));
    }
    let scale = values.iter().map(Decimal::scale).max().unwrap_or(0);
    let (sign, magnitude) = total.into_parts();
    let unit = ten_to(MAX_SCALE - scale);
    assert_eq!(
        &magnitude % &unit,
        BigUint::from(0_u32),
        "every addend is a whole number of units at the largest scale"
    );
    let at_scale = magnitude / unit;
    (0..=scale).find_map(|dropped| {
        let divisor = ten_to(dropped);
        let quotient = &at_scale / &divisor;
        let twice_remainder = (&at_scale % &divisor) * BigUint::from(2_u32);
        let rounded =
            if twice_remainder > divisor || (twice_remainder == divisor && quotient.bit(0)) {
                quotient + BigUint::from(1_u32)
            } else {
                quotient
            };
        (rounded.bits() <= MANTISSA_BITS).then(|| {
            let mantissa = to_u128(&rounded) as i128;
            let signed = if sign == Sign::Minus {
                -mantissa
            } else {
                mantissa
            };
            (signed, scale - dropped, dropped)
        })
    })
}

fn fold(values: &[Decimal]) -> ExactDecimalSum {
    let mut sum = ExactDecimalSum::new();
    for value in values {
        sum.add_decimal(*value);
    }
    sum
}

/// Our result as `(mantissa, scale)`, comparable with the reference.
fn ours(sum: &ExactDecimalSum, int_part: i128) -> Option<(i128, u32)> {
    sum.round_with(int_part).map(|d| (d.mantissa(), d.scale()))
}

fn assert_matches(sum: &ExactDecimalSum, values: &[Decimal], int_part: i128, context: &str) {
    let expected = reference(values, int_part).map(|(mantissa, scale, _)| (mantissa, scale));
    assert_eq!(
        ours(sum, int_part),
        expected,
        "{context}: integer part {int_part}, values {values:?}"
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

    fn coin(&mut self) -> bool {
        self.next() & 1 == 1
    }

    /// A random value of at most `bits` bits.
    fn bits(&mut self, bits: u32) -> u128 {
        let value = (u128::from(self.next()) << 64) | u128::from(self.next());
        if bits >= 128 {
            value
        } else {
            value & ((1 << bits) - 1)
        }
    }

    fn decimal(&mut self, mantissa: u128, scale: u32, negative: bool) -> Decimal {
        let signed = mantissa as i128;
        Decimal::from_i128_with_scale(if negative { -signed } else { signed }, scale)
    }

    /// A decimal with a mantissa of up to 96 random bits at a random scale.
    fn any_decimal(&mut self, max_scale: u32) -> Decimal {
        let bits = self.below(MANTISSA_BITS + 1) as u32;
        let mantissa = self.bits(bits);
        let scale = self.below(u64::from(max_scale) + 1) as u32;
        let negative = self.coin();
        self.decimal(mantissa, scale, negative)
    }

    /// An integer part: zero, small, or anywhere in the `i128` range.
    fn int_part(&mut self) -> i128 {
        let magnitude = match self.below(3) {
            0 => 0,
            1 => self.bits(64),
            _ => {
                let bits = self.below(128) as u32;
                self.bits(bits)
            }
        };
        let signed = magnitude as i128;
        if self.coin() { -signed } else { signed }
    }

    /// One sequence of 1 to 200 decimals: values at any magnitude and scale,
    /// values clustered at one scale so they interact, exact and near
    /// cancellations of earlier values, and small integers.
    fn sequence(&mut self) -> Vec<Decimal> {
        if self.below(8) == 0 {
            return self.tie_sequence();
        }
        let len = 1 + self.below(200) as usize;
        let cluster = self.below(29) as u32;
        let mut values: Vec<Decimal> = Vec::with_capacity(len);
        while values.len() < len {
            let value = match self.below(6) {
                0 | 1 => self.any_decimal(MAX_SCALE),
                2 => {
                    let mantissa = self.bits(96);
                    let negative = self.coin();
                    self.decimal(mantissa, cluster, negative)
                }
                3 if !values.is_empty() => {
                    let earlier = values[self.below(values.len() as u64) as usize];
                    -earlier
                }
                4 if !values.is_empty() => {
                    let earlier = values[self.below(values.len() as u64) as usize];
                    let nudged = earlier.mantissa().unsigned_abs() ^ u128::from(self.next() & 0xFF);
                    let nudged = nudged & ((1 << MANTISSA_BITS) - 1);
                    self.decimal(nudged, earlier.scale(), !earlier.is_sign_negative())
                }
                _ => Decimal::from(self.below(2001) as i64 - 1000),
            };
            values.push(value);
        }
        values
    }

    /// A sequence whose exact total is `q + 0.5` at a scale `s` (or one unit
    /// of `10^-s` past it), with `q` large enough that no scale above 0 fits
    /// 96 bits: the reduction lands exactly on a tie between `q` and `q + 1`,
    /// or just past it. The total is spread over pieces of `q`, the half at
    /// scale `s`, and pairs that cancel exactly, shuffled, and negated half
    /// the time.
    fn tie_sequence(&mut self) -> Vec<Decimal> {
        let low = (1_u128 << MANTISSA_BITS) / 10 + 1;
        let high = (1_u128 << MANTISSA_BITS) - 2;
        let q = low + (self.bits(96) % (high - low));
        let scale = 1 + self.below(u64::from(MAX_SCALE)) as u32;
        let negative = self.coin();
        let piece = self.bits(96) % q;
        let mut values = vec![
            self.decimal(piece, 0, negative),
            self.decimal(q - piece, 0, negative),
            self.decimal(5 * 10_u128.pow(scale - 1), scale, negative),
        ];
        if self.coin() {
            values.push(self.decimal(1, scale, negative));
        }
        for _ in 0..self.below(50) {
            let y = self.any_decimal(scale);
            values.push(y);
            values.push(-y);
        }
        for i in (1..values.len()).rev() {
            let j = self.below(i as u64 + 1) as usize;
            values.swap(i, j);
        }
        values
    }
}

#[test]
fn exact_decimal_sum_matches_bigint_on_generated_sequences() {
    let mut rng = SplitMix(0xDEC1_0A11);
    let (mut rounded, mut out_of_range) = (0, 0);
    for index in 0..10_000 {
        let values = rng.sequence();
        let sum = fold(&values);
        assert_eq!(sum.count(), values.len() as u64, "sequence {index}");
        assert_matches(&sum, &values, 0, &format!("sequence {index}"));
        let int_part = rng.int_part();
        assert_matches(&sum, &values, int_part, &format!("sequence {index}"));
        match reference(&values, 0) {
            Some((_, _, dropped)) if dropped > 0 => rounded += 1,
            None => out_of_range += 1,
            Some(_) => {}
        }
    }
    // The generator reaches both outcomes the rounding decides.
    assert!(rounded > 1_000, "only {rounded} sequences needed rounding");
    assert!(
        out_of_range > 100,
        "only {out_of_range} sequences were out of range"
    );
}

#[test]
fn split_merge_and_subtract_match_bigint() {
    let mut rng = SplitMix(0x5B11_73E7);
    for index in 0..10_000 {
        let values = rng.sequence();
        let whole = fold(&values);
        let at = rng.below(values.len() as u64 + 1) as usize;
        let (left, right) = values.split_at(at);
        let mut left_right = fold(left);
        left_right.merge(&fold(right));
        let mut right_left = fold(right);
        right_left.merge(&fold(left));
        for (order, merged) in [("left+right", left_right), ("right+left", right_left)] {
            assert_eq!(
                merged, whole,
                "sequence {index} split at {at}, {order}: state"
            );
            assert_matches(
                &merged,
                &values,
                0,
                &format!("sequence {index} split at {at}, {order}"),
            );
        }

        // Subtract one addend: the result is the survivors' sum, and the state
        // is a fresh fold of them.
        let removed = rng.below(values.len() as u64) as usize;
        let mut sum = whole.clone();
        sum.sub_decimal(values[removed]);
        let survivors: Vec<Decimal> = values
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != removed)
            .map(|(_, v)| *v)
            .collect();
        assert_eq!(
            sum,
            fold(&survivors),
            "sequence {index}: state after subtraction"
        );
        let int_part = rng.int_part();
        assert_matches(
            &sum,
            &survivors,
            int_part,
            &format!("sequence {index} without addend {removed}"),
        );
    }
}

#[test]
fn rounding_matches_a_single_decimal_add() {
    // Where the exact total needs rounding, split it into the truncated high
    // part `a` (at the result scale) and the remainder `b` (at the largest
    // addend scale). Both are decimals and `a + b` is the exact total, so
    // `rust_decimal`'s one correctly rounded addition must give our value;
    // the scale is the reference's.
    let mut rng = SplitMix(0x0AD0_0ACE);
    let mut checked = 0;
    for index in 0..10_000 {
        let values = rng.sequence();
        let Some((mantissa, scale, dropped)) = reference(&values, 0) else {
            continue;
        };
        if dropped == 0 {
            continue;
        }
        let largest = scale + dropped;
        let mut total = BigInt::from(0);
        for value in &values {
            total += BigInt::from(value.mantissa())
                * BigInt::from_biguint(Sign::Plus, ten_to(largest - value.scale()));
        }
        let (sign, magnitude) = total.into_parts();
        let divisor = ten_to(dropped);
        let high = to_u128(&(&magnitude / &divisor)) as i128;
        let low = to_u128(&(&magnitude % &divisor)) as i128;
        let signed = |m: i128| if sign == Sign::Minus { -m } else { m };
        let a = Decimal::from_i128_with_scale(signed(high), scale);
        let b = Decimal::from_i128_with_scale(signed(low), largest);
        let single = a.checked_add(b).expect("a + b is in range");
        let ours = fold(&values).round_with(0).expect("in range");
        assert_eq!(ours, single, "sequence {index}: {a} + {b}");
        assert_eq!(
            (ours.mantissa(), ours.scale()),
            (mantissa, scale),
            "sequence {index}"
        );
        checked += 1;
    }
    assert!(checked > 1_000, "only {checked} sequences needed rounding");
}
