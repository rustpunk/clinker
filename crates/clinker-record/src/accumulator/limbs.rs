//! Fixed-width two's-complement integers over 64-bit limbs, least significant
//! limb first: the one exact-integer primitive the exact sums share.
//!
//! [`ExactSum`](super::ExactSum) keeps a float total in 2^-1074 units over 34
//! limbs and [`ExactDecimalSum`](super::ExactDecimalSum) keeps a decimal total
//! in 10^-28 units over four; both add, subtract and merge through these
//! functions and differ only in how they round. Every function is pure over
//! the array it is given and allocates nothing. Arithmetic wraps at the top
//! limb, which is how a two's-complement total changes sign: each caller sizes
//! its array so a reachable total never reaches the sign bit by magnitude.

use serde::{Deserialize, Serialize};

/// `value << offset` as limbs: at most three words starting at `offset / 64`.
fn shifted_words(value: u128, offset: usize) -> (usize, [u64; 3]) {
    let shift = (offset % 64) as u32;
    let low = value as u64;
    let high = (value >> 64) as u64;
    let words = if shift == 0 {
        [low, high, 0]
    } else {
        [
            low << shift,
            (high << shift) | (low >> (64 - shift)),
            high >> (64 - shift),
        ]
    };
    (offset / 64, words)
}

/// `limbs += value << offset`, carrying through the top limb.
pub(crate) fn add_shifted<const N: usize>(limbs: &mut [u64; N], value: u128, offset: usize) {
    let (start, words) = shifted_words(value, offset);
    let mut carry = false;
    for (index, limb) in limbs.iter_mut().enumerate().skip(start) {
        let word = words.get(index - start).copied().unwrap_or(0);
        if word == 0 && !carry && index - start >= words.len() {
            break;
        }
        let (sum, c1) = limb.overflowing_add(word);
        let (sum, c2) = sum.overflowing_add(u64::from(carry));
        *limb = sum;
        carry = c1 || c2;
    }
}

/// `limbs -= value << offset`, borrowing through the top limb.
pub(crate) fn sub_shifted<const N: usize>(limbs: &mut [u64; N], value: u128, offset: usize) {
    let (start, words) = shifted_words(value, offset);
    let mut borrow = false;
    for (index, limb) in limbs.iter_mut().enumerate().skip(start) {
        let word = words.get(index - start).copied().unwrap_or(0);
        if word == 0 && !borrow && index - start >= words.len() {
            break;
        }
        let (difference, b1) = limb.overflowing_sub(word);
        let (difference, b2) = difference.overflowing_sub(u64::from(borrow));
        *limb = difference;
        borrow = b1 || b2;
    }
}

/// `limbs += other`, carrying through the top limb.
pub(crate) fn add_assign<const N: usize>(limbs: &mut [u64; N], other: &[u64; N]) {
    let mut carry = false;
    for (ours, theirs) in limbs.iter_mut().zip(other.iter()) {
        let (sum, c1) = ours.overflowing_add(*theirs);
        let (sum, c2) = sum.overflowing_add(u64::from(carry));
        *ours = sum;
        carry = c1 || c2;
    }
}

/// `limbs -= other`, borrowing through the top limb.
pub(crate) fn sub_assign<const N: usize>(limbs: &mut [u64; N], other: &[u64; N]) {
    let mut borrow = false;
    for (ours, theirs) in limbs.iter_mut().zip(other.iter()) {
        let (difference, b1) = ours.overflowing_sub(*theirs);
        let (difference, b2) = difference.overflowing_sub(u64::from(borrow));
        *ours = difference;
        borrow = b1 || b2;
    }
}

/// Two's complement negation in place.
pub(crate) fn negate<const N: usize>(limbs: &mut [u64; N]) {
    let mut carry = true;
    for limb in limbs.iter_mut() {
        let (value, overflow) = (!*limb).overflowing_add(u64::from(carry));
        *limb = value;
        carry = overflow;
    }
}

/// True when the top bit, the sign, is set.
pub(crate) fn is_negative<const N: usize>(limbs: &[u64; N]) -> bool {
    limbs[N - 1] >> 63 == 1
}

/// True when every limb is zero.
pub(crate) fn is_zero<const N: usize>(limbs: &[u64; N]) -> bool {
    limbs.iter().all(|limb| *limb == 0)
}

/// The two's-complement value of `limbs` widened to `M ≥ N` limbs, the sign
/// copied into the new top limbs.
pub(crate) fn sign_extend<const N: usize, const M: usize>(limbs: &[u64; N]) -> [u64; M] {
    const { assert!(M >= N, "sign_extend only widens") };
    let fill = if is_negative(limbs) { u64::MAX } else { 0 };
    let mut wide = [fill; M];
    wide[..N].copy_from_slice(limbs);
    wide
}

/// An `i128` as two limbs, two's complement.
pub(crate) fn from_i128(value: i128) -> [u64; 2] {
    [value as u64, (value >> 64) as u64]
}

/// The sign of a two's-complement value and its magnitude, in the same width.
/// The most negative value's magnitude does not fit its width; callers keep
/// every reachable value away from it.
pub(crate) fn sign_and_magnitude<const N: usize>(limbs: &[u64; N]) -> (bool, [u64; N]) {
    let negative = is_negative(limbs);
    let mut magnitude = *limbs;
    if negative {
        negate(&mut magnitude);
    }
    (negative, magnitude)
}

/// Index of the highest set bit, or `None` for zero.
pub(crate) fn highest_set_bit<const N: usize>(limbs: &[u64; N]) -> Option<usize> {
    limbs
        .iter()
        .enumerate()
        .rev()
        .find(|(_, limb)| **limb != 0)
        .map(|(index, limb)| index * 64 + 63 - limb.leading_zeros() as usize)
}

/// `count` (at most 64) bits of `limbs` starting at bit `start`.
pub(crate) fn bits_at<const N: usize>(limbs: &[u64; N], start: usize, count: u32) -> u64 {
    let index = start / 64;
    let shift = (start % 64) as u32;
    let mut value = limbs[index] >> shift;
    if shift > 0 && index + 1 < N {
        value |= limbs[index + 1] << (64 - shift);
    }
    if count < 64 {
        value & ((1 << count) - 1)
    } else {
        value
    }
}

/// True when any bit below bit `end` is set.
pub(crate) fn any_bit_below<const N: usize>(limbs: &[u64; N], end: usize) -> bool {
    let whole = end / 64;
    let partial = (end % 64) as u32;
    limbs[..whole].iter().any(|limb| *limb != 0)
        || (partial > 0 && limbs[whole] & ((1 << partial) - 1) != 0)
}

/// `magnitude *= factor` for an unsigned value. Returns the carry out of the
/// top limb, which is zero when the product fits.
pub(crate) fn mul_small<const N: usize>(magnitude: &mut [u64; N], factor: u64) -> u64 {
    let mut carry = 0_u64;
    for limb in magnitude.iter_mut() {
        let product = u128::from(*limb) * u128::from(factor) + u128::from(carry);
        *limb = product as u64;
        carry = (product >> 64) as u64;
    }
    carry
}

/// `magnitude /= divisor` for an unsigned value, truncating. Returns the
/// remainder. The divisor must be nonzero.
pub(crate) fn div_small<const N: usize>(magnitude: &mut [u64; N], divisor: u64) -> u64 {
    debug_assert!(divisor != 0, "division by zero");
    let mut remainder = 0_u128;
    for limb in magnitude.iter_mut().rev() {
        let current = (remainder << 64) | u128::from(*limb);
        *limb = (current / u128::from(divisor)) as u64;
        remainder = current % u128::from(divisor);
    }
    remainder as u64
}

/// A signed integer total held exactly over three limbs (192 bits).
///
/// Wide enough for the sum of 2^64 products of two `i64`s (each at most 2^126
/// in magnitude, so a total below 2^190), which an `i128` is not: two rows of
/// `i64::MIN × i64::MIN` already leave its range. Adds and subtracts exactly,
/// so a total retracts to the state of a fresh fold. Serializes as three
/// integers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct WideInt([u64; 3]);

impl WideInt {
    /// `self += value`.
    pub(crate) fn add_i128(&mut self, value: i128) {
        add_assign(&mut self.0, &sign_extend(&from_i128(value)));
    }

    /// `self -= value`.
    pub(crate) fn sub_i128(&mut self, value: i128) {
        sub_assign(&mut self.0, &sign_extend(&from_i128(value)));
    }

    /// `self += other`.
    pub(crate) fn add(&mut self, other: &WideInt) {
        add_assign(&mut self.0, &other.0);
    }

    pub(crate) fn is_zero(&self) -> bool {
        is_zero(&self.0)
    }

    /// The total as three two's-complement limbs.
    pub(crate) fn limbs(&self) -> &[u64; 3] {
        &self.0
    }
}

/// A limb array as a sequence, checked for length on the way in: serde's
/// derive covers arrays of at most 32 elements.
pub(crate) mod serde_seq {
    use serde::{Deserialize, Deserializer, Serializer};

    struct Limbs(usize);

    impl serde::de::Expected for Limbs {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(formatter, "exactly {} limbs", self.0)
        }
    }

    pub(crate) fn serialize<S: Serializer, const N: usize>(
        limbs: &[u64; N],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(limbs.iter())
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>, const N: usize>(
        deserializer: D,
    ) -> Result<[u64; N], D::Error> {
        let limbs = Vec::<u64>::deserialize(deserializer)?;
        let len = limbs.len();
        limbs
            .try_into()
            .map_err(|_| serde::de::Error::invalid_length(len, &Limbs(N)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A value as limbs from its `u128` magnitude and a sign.
    fn signed<const N: usize>(magnitude: u128, negative: bool) -> [u64; N] {
        let mut limbs = [0_u64; N];
        add_shifted(&mut limbs, magnitude, 0);
        if negative {
            negate(&mut limbs);
        }
        limbs
    }

    fn carries_and_borrows_across_every_limb<const N: usize>() {
        // All ones below the top limb, plus one, carries into the top limb.
        let mut limbs = [u64::MAX; N];
        limbs[N - 1] = 0;
        add_shifted(&mut limbs, 1, 0);
        let mut expected = [0_u64; N];
        expected[N - 1] = 1;
        assert_eq!(limbs, expected, "carry across {N} limbs");
        sub_shifted(&mut limbs, 1, 0);
        let mut back = [u64::MAX; N];
        back[N - 1] = 0;
        assert_eq!(limbs, back, "borrow across {N} limbs");

        // A shifted value straddling a limb boundary, added and subtracted.
        let value = (1_u128 << 127) | 0xDEAD_BEEF;
        for offset in [0, 1, 63, 64, 65, 64 * (N - 3) + 17] {
            let mut limbs = [0_u64; N];
            add_shifted(&mut limbs, value, offset);
            assert_eq!(
                highest_set_bit(&limbs),
                Some(offset + 127),
                "offset {offset}"
            );
            sub_shifted(&mut limbs, value, offset);
            assert!(is_zero(&limbs), "offset {offset}");
        }

        // Whole-array add and subtract, through a sign change.
        let a = signed::<N>(5, false);
        let b = signed::<N>(7, true);
        let mut sum = a;
        add_assign(&mut sum, &b);
        assert_eq!(sum, signed::<N>(2, true));
        assert!(is_negative(&sum));
        sub_assign(&mut sum, &b);
        assert_eq!(sum, a);

        // Negation is its own inverse, and zero negates to zero.
        let mut limbs = signed::<N>(u128::MAX, false);
        negate(&mut limbs);
        assert!(is_negative(&limbs));
        assert_eq!(
            sign_and_magnitude(&limbs),
            (true, signed::<N>(u128::MAX, false))
        );
        negate(&mut limbs);
        assert_eq!(limbs, signed::<N>(u128::MAX, false));
        let mut zero = [0_u64; N];
        negate(&mut zero);
        assert!(is_zero(&zero));

        // Multiplying by a factor and dividing by it round-trips, with the
        // remainder of an added offset coming back.
        let mut limbs = signed::<N>(0x1234_5678_9ABC_DEF0_1122_3344, false);
        let original = limbs;
        for factor in [10_u64, 10_000_000_000_000_000_000, u64::MAX] {
            assert_eq!(mul_small(&mut limbs, factor), 0, "factor {factor}");
            add_shifted(&mut limbs, 3, 0);
            assert_eq!(div_small(&mut limbs, factor), 3, "factor {factor}");
            assert_eq!(limbs, original, "factor {factor}");
        }

        // Bit reads across a limb boundary.
        let limbs = signed::<N>(0b1011 << 62, false);
        assert_eq!(bits_at(&limbs, 62, 4), 0b1011);
        assert!(any_bit_below(&limbs, 63));
        assert!(!any_bit_below(&limbs, 62));
    }

    #[test]
    fn limbs_arithmetic_is_exact_for_four_and_thirty_four_limbs() {
        carries_and_borrows_across_every_limb::<4>();
        carries_and_borrows_across_every_limb::<34>();
    }

    #[test]
    fn mul_small_reports_the_carry_out() {
        let mut limbs = [u64::MAX; 4];
        assert_eq!(mul_small(&mut limbs, 2), 1);
    }

    #[test]
    fn sign_extension_keeps_the_value() {
        let narrow = from_i128(-5);
        let wide: [u64; 4] = sign_extend(&narrow);
        assert_eq!(sign_and_magnitude(&wide), (true, [5, 0, 0, 0]));
        let wide: [u64; 4] = sign_extend(&from_i128(i128::MAX));
        assert_eq!(wide, [u64::MAX, u64::MAX >> 1, 0, 0]);
    }

    #[test]
    fn wide_int_holds_totals_beyond_an_i128() {
        let square = i128::from(i64::MIN) * i128::from(i64::MIN);
        let mut total = WideInt::default();
        for _ in 0..3 {
            total.add_i128(square);
        }
        // 3 × 2^126 = 2^127 + 2^126.
        assert_eq!(total.limbs(), &[0, 3 << 62, 0]);
        for _ in 0..3 {
            total.sub_i128(square);
        }
        assert!(total.is_zero());
        total.sub_i128(1);
        assert_eq!(total.limbs(), &[u64::MAX; 3]);
    }
}
