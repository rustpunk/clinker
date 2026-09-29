//! Exact decimal summation with one rounding: [`ExactDecimalSum`].
//!
//! A decimal is a mantissa below 2^96 with a scale from 0 to 28, so every
//! decimal is an integer number of 10^-28 units. `ExactDecimalSum` keeps the
//! sum of its addends as that integer, exactly, in two's complement over four
//! 64-bit limbs, and counts its addends per input scale. Adding, subtracting
//! and merging are integer additions with carry, so none of them rounds;
//! [`ExactDecimalSum::round_with`] is the only rounding, once, half-even. The
//! result therefore depends only on the multiset of addends: not on the order
//! they arrived in, not on how partial sums were split and merged (a spilled
//! aggregate merges one partial per spill run), and not on which addends were
//! added and later subtracted. Its scale and whether it is in range are
//! functions of that multiset too.
//!
//! Four limbs suffice. One addend is `mantissa × 10^(28 − scale)` units,
//! below 2^96 × 10^28 < 2^189.1; 2^64 of them stay below 2^253.1, and an
//! integer part (an `i128`, below 2^127, times 10^28 < 2^220.1) added at
//! rounding keeps the total below 2^254, under the sign bit 255.
//!
//! This extends the same fixed-point design as [`ExactSum`](super::ExactSum),
//! through the same limb arithmetic, with a decimal unit instead of a binary
//! one. `tests/exact_decimal_oracle.rs` checks it against an independent
//! arbitrary-precision total and against `rust_decimal`'s own correctly
//! rounded addition; that oracle is a dev-dependency only.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use super::limbs;

/// Limbs of the fixed-point integer. Bit 0 weighs 10^-28 and the top bit is
/// the sign.
const LIMBS: usize = 4;

/// The largest decimal scale; one unit is `10^-MAX_SCALE`.
const MAX_SCALE: u32 = 28;

/// Scales 0 to 28.
const SCALES: usize = MAX_SCALE as usize + 1;

/// The largest power of ten a `u64` holds.
const MAX_U64_POWER: u32 = 19;

/// Limbs of the rounding workspace: the four-limb total plus an integer part
/// of up to three limbs (below 2^191) times 10^28 stays below 2^285.
const WORK_LIMBS: usize = 5;

/// An exact, mergeable, reversible sum of decimal addends, rounded once when
/// read.
///
/// Holds one pointer inline. The limbs and per-scale counts are allocated at
/// the first decimal addend (or the first merge from an allocated sum) and
/// freed when the last decimal addend is subtracted, so a sum with no decimal
/// addends allocates nothing and a sum retracted back to none is identical to
/// one that never saw any. [`heap_size`](Self::heap_size) reports that
/// allocation.
///
/// Serializes (serde) to its exact state, so a spilled partial sum reloads
/// unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExactDecimalSum {
    parts: Option<Box<Parts>>,
}

/// The allocated state of a sum with at least one decimal addend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Parts {
    /// The sum of the addends in 10^-28 units, two's complement, least
    /// significant limb first.
    #[serde(with = "limbs::serde_seq")]
    limbs: [u64; LIMBS],
    /// Addends held at each scale 0 to 28. The addend count is their sum,
    /// derived rather than stored, and the allocation is freed when it
    /// returns to zero.
    scale_counts: [u64; SCALES],
}

/// Bytes of the allocation an [`ExactDecimalSum`] holds once it has a decimal
/// addend: four limbs and 29 counts.
const ALLOCATION_BYTES: usize = std::mem::size_of::<Parts>();

impl Parts {
    fn empty() -> Self {
        Self {
            limbs: [0; LIMBS],
            scale_counts: [0; SCALES],
        }
    }

    fn count(&self) -> u64 {
        self.scale_counts.iter().sum()
    }

    /// Add or subtract one addend exactly, and count it at its scale.
    fn apply(&mut self, value: Decimal, subtract: bool) {
        let scale = value.scale();
        let mut magnitude = [0_u64; LIMBS];
        limbs::add_shifted(&mut magnitude, value.mantissa().unsigned_abs(), 0);
        scale_up(&mut magnitude, MAX_SCALE - scale);
        if value.is_sign_negative() != subtract {
            limbs::sub_assign(&mut self.limbs, &magnitude);
        } else {
            limbs::add_assign(&mut self.limbs, &magnitude);
        }
        let count = &mut self.scale_counts[scale as usize];
        if subtract {
            debug_assert!(*count > 0, "subtracted an addend the sum does not hold");
            *count = count.saturating_sub(1);
        } else {
            *count += 1;
        }
    }

    fn merge(&mut self, other: &Parts) {
        limbs::add_assign(&mut self.limbs, &other.limbs);
        for (ours, theirs) in self.scale_counts.iter_mut().zip(other.scale_counts.iter()) {
            *ours += *theirs;
        }
    }

    /// The largest scale with an addend, zeros included.
    fn largest_scale(&self) -> u32 {
        self.scale_counts
            .iter()
            .rposition(|count| *count > 0)
            .map_or(0, |scale| scale as u32)
    }
}

impl ExactDecimalSum {
    /// An empty sum. Allocates nothing.
    pub const fn new() -> Self {
        Self { parts: None }
    }

    /// Add one decimal addend exactly. Returns the bytes this call allocated:
    /// the state's size at the sum's first decimal addend, else 0.
    pub fn add_decimal(&mut self, value: Decimal) -> usize {
        let allocated = self.parts.is_none();
        self.parts
            .get_or_insert_with(|| Box::new(Parts::empty()))
            .apply(value, false);
        if allocated { ALLOCATION_BYTES } else { 0 }
    }

    /// Subtract one decimal addend the sum holds, exactly: the inverse of
    /// [`add_decimal`](Self::add_decimal) with the same value, scale
    /// included. Returns the heap delta: minus the state's size when this
    /// removes the last decimal addend (the state is then freed and the sum
    /// is empty), else 0.
    ///
    /// The caller must only subtract an addend it added; subtracting one the
    /// sum does not hold is a logic error (debug-asserted) and leaves a sum
    /// that no multiset of addends describes.
    pub fn sub_decimal(&mut self, value: Decimal) -> isize {
        let Some(parts) = self.parts.as_mut() else {
            debug_assert!(
                false,
                "subtracted a decimal from a sum with no decimal addend"
            );
            return 0;
        };
        parts.apply(value, true);
        if parts.count() > 0 {
            return 0;
        }
        debug_assert!(
            limbs::is_zero(&parts.limbs),
            "a sum with no decimal addend left holds a nonzero value"
        );
        self.parts = None;
        -(ALLOCATION_BYTES as isize)
    }

    /// Add every addend of `other` exactly. Allocates (reported by
    /// [`heap_size`](Self::heap_size), not returned) when `self` has no
    /// decimal addend and `other` has.
    pub fn merge(&mut self, other: &ExactDecimalSum) {
        let Some(theirs) = other.parts.as_deref() else {
            return;
        };
        match self.parts.as_deref_mut() {
            Some(ours) => ours.merge(theirs),
            None => self.parts = Some(Box::new(theirs.clone())),
        }
    }

    /// The exact value `int_part + Σ addends`, rounded once, or `None` when
    /// it is outside the decimal range.
    ///
    /// The result's scale is the largest scale among the addends, zeros
    /// included (an integer counts as scale 0), so `1.00 + -1.00 + 2` is
    /// `2.00` whatever the order. When the exact value at that scale needs
    /// more than 96 mantissa bits it is divided once by the smallest power of
    /// ten that makes the half-even-rounded mantissa fit, at the cost of that
    /// many fractional digits; it is out of range only when no scale down to
    /// 0 fits. Range is decided on the exact total, never on scale loss. Pure;
    /// copies the limbs.
    pub fn round_with(&self, int_part: i128) -> Option<Decimal> {
        self.round_with_limbs(&limbs::from_i128(int_part))
    }

    /// [`round_with`](Self::round_with) for an integer part given as `M`
    /// two's-complement limbs, which may exceed an `i128` (at most three
    /// limbs).
    pub(crate) fn round_with_limbs<const M: usize>(&self, int_part: &[u64; M]) -> Option<Decimal> {
        const { assert!(M <= 3, "an integer part of at most three limbs") };
        let (mut total, scale) = match self.parts.as_deref() {
            Some(parts) => (
                limbs::sign_extend::<LIMBS, WORK_LIMBS>(&parts.limbs),
                parts.largest_scale(),
            ),
            None => ([0; WORK_LIMBS], 0),
        };
        let (int_negative, int_magnitude) = limbs::sign_and_magnitude(int_part);
        let mut int_units = [0_u64; WORK_LIMBS];
        int_units[..M].copy_from_slice(&int_magnitude);
        scale_up(&mut int_units, MAX_SCALE);
        if int_negative {
            limbs::sub_assign(&mut total, &int_units);
        } else {
            limbs::add_assign(&mut total, &int_units);
        }

        let (negative, mut magnitude) = limbs::sign_and_magnitude(&total);
        // Every addend and the integer part are whole multiples of the unit
        // at the result scale, so this division is exact.
        let remainder = scale_down(&mut magnitude, MAX_SCALE - scale);
        debug_assert_eq!(remainder, 0, "an addend finer than the largest scale");

        (0..=scale).find_map(|dropped| {
            let mantissa = if dropped == 0 {
                magnitude
            } else {
                round_half_even(&magnitude, dropped)
            };
            decimal_of(negative, &mantissa, scale - dropped)
        })
    }

    /// True when the sum has no decimal addend (and so holds no allocation).
    pub fn is_empty(&self) -> bool {
        self.parts.is_none()
    }

    /// The number of decimal addends the sum holds, of every scale.
    pub fn count(&self) -> u64 {
        self.parts.as_deref().map_or(0, Parts::count)
    }

    /// Bytes allocated for the state: its size once the sum has a decimal
    /// addend, else 0.
    pub fn heap_size(&self) -> usize {
        if self.parts.is_some() {
            ALLOCATION_BYTES
        } else {
            0
        }
    }
}

/// `magnitude *= 10^exponent`, in factors a `u64` holds. The caller sizes the
/// array so the product fits.
fn scale_up<const N: usize>(magnitude: &mut [u64; N], mut exponent: u32) {
    while exponent > 0 {
        let step = exponent.min(MAX_U64_POWER);
        let carry = limbs::mul_small(magnitude, 10_u64.pow(step));
        debug_assert_eq!(carry, 0, "a scaled decimal outgrew its limbs");
        exponent -= step;
    }
}

/// `magnitude /= 10^exponent`, truncating, for `exponent` at most 28.
/// Returns the remainder, below `10^exponent`: the divisions by the `u64`
/// factors combine as `r = r_second × d_first + r_first`.
fn scale_down<const N: usize>(magnitude: &mut [u64; N], exponent: u32) -> u128 {
    debug_assert!(exponent <= MAX_SCALE);
    let first = exponent.min(MAX_U64_POWER);
    let first_divisor = 10_u64.pow(first);
    let first_remainder = limbs::div_small(magnitude, first_divisor);
    let second = exponent - first;
    let second_remainder = limbs::div_small(magnitude, 10_u64.pow(second));
    u128::from(second_remainder) * u128::from(first_divisor) + u128::from(first_remainder)
}

/// `magnitude / 10^dropped`, rounded half-even on the exact remainder: one
/// division, so there is no double rounding.
fn round_half_even(magnitude: &[u64; WORK_LIMBS], dropped: u32) -> [u64; WORK_LIMBS] {
    let mut quotient = *magnitude;
    let remainder = scale_down(&mut quotient, dropped);
    let divisor = 10_u128.pow(dropped);
    let twice = remainder * 2;
    if twice > divisor || (twice == divisor && quotient[0] & 1 == 1) {
        limbs::add_shifted(&mut quotient, 1, 0);
    }
    quotient
}

/// The decimal with this sign, mantissa and scale, or `None` when the mantissa
/// needs more than 96 bits.
fn decimal_of(negative: bool, mantissa: &[u64; WORK_LIMBS], scale: u32) -> Option<Decimal> {
    if mantissa[2..].iter().any(|limb| *limb != 0) || mantissa[1] >> 32 != 0 {
        return None;
    }
    let magnitude = (i128::from(mantissa[1]) << 64) | i128::from(mantissa[0]);
    let signed = if negative { -magnitude } else { magnitude };
    Decimal::try_from_i128_with_scale(signed, scale).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dec(text: &str) -> Decimal {
        text.parse().expect("a decimal literal")
    }

    fn sum_of(values: &[&str]) -> ExactDecimalSum {
        let mut sum = ExactDecimalSum::new();
        for value in values {
            sum.add_decimal(dec(value));
        }
        sum
    }

    fn text(sum: &ExactDecimalSum, int_part: i128) -> Option<String> {
        sum.round_with(int_part).map(|d| d.to_string())
    }

    #[test]
    fn exact_decimal_sum_rounds_known_sums_once() {
        assert_eq!(
            text(&sum_of(&["0.10", "0.20", "0.30"]), 0).as_deref(),
            Some("0.60")
        );
        assert_eq!(
            text(&sum_of(&["1.00", "-1.00"]), 2).as_deref(),
            Some("2.00")
        );
        assert_eq!(text(&sum_of(&["1.5"]), -3).as_deref(), Some("-1.5"));
        // 1e28 + 0.8 needs 30 digits: one half-even rounding to scale 0.
        assert_eq!(
            text(&sum_of(&["10000000000000000000000000000", "0.4", "0.4"]), 0).as_deref(),
            Some("10000000000000000000000000001")
        );
        // A tie rounds to even: ...0.5 down, ...1.5 up.
        assert_eq!(
            text(&sum_of(&["10000000000000000000000000000", "0.5"]), 0).as_deref(),
            Some("10000000000000000000000000000")
        );
        assert_eq!(
            text(&sum_of(&["10000000000000000000000000001", "0.5"]), 0).as_deref(),
            Some("10000000000000000000000000002")
        );
        // Two digits dropped in one division, not one then another: at scale
        // 1 the total would need ...0355, past the largest mantissa, so it
        // rounds to scale 0, where .46 rounds down. Rounding to .5 first
        // would make a tie and round the odd total up.
        assert_eq!(
            text(&sum_of(&["7922816251426433759354395035", "0.46"]), 0).as_deref(),
            Some("7922816251426433759354395035")
        );
        // The largest decimal, and one unit past it.
        let max = Decimal::MAX.to_string();
        assert_eq!(text(&sum_of(&[&max]), 0), Some(max.clone()));
        assert_eq!(text(&sum_of(&[&max]), 1), None);
        assert_eq!(text(&sum_of(&[&max, &max]), 0), None);
        assert_eq!(
            text(&sum_of(&[&max, &max]), -max.parse::<i128>().unwrap()),
            Some(max)
        );
        // An i128 integer part far outside the range alone.
        assert_eq!(text(&ExactDecimalSum::new(), i128::MAX), None);
        assert_eq!(text(&ExactDecimalSum::new(), 7), Some("7".to_string()));
        // An integer part beyond an i128, cancelled back into range.
        let mut wide = [0_u64; 3];
        wide[2] = 1; // 2^128
        let mut minus = sum_of(&[]);
        minus.add_decimal(Decimal::from(-5));
        assert_eq!(minus.round_with_limbs(&wide), None);
    }

    #[test]
    fn exact_decimal_sum_add_sub_and_merge_are_exact() {
        let values = [
            "79228162514264337593543950335",
            "-0.0000000000000000000000000001",
            "1.5",
            "-79228162514264337593543950335",
            "0.00",
            "123456789.123456789",
        ];
        let whole = sum_of(&values);
        for at in 0..=values.len() {
            let (left, right) = values.split_at(at);
            let mut merged = sum_of(left);
            merged.merge(&sum_of(right));
            assert_eq!(merged, whole, "split at {at}");
        }
        let mut sum = whole.clone();
        let mut freed = 0;
        for value in values {
            freed += sum.sub_decimal(dec(value));
        }
        assert_eq!(sum, ExactDecimalSum::new());
        assert_eq!(freed, -(ALLOCATION_BYTES as isize));
        assert_eq!(sum.heap_size(), 0);
    }
}
