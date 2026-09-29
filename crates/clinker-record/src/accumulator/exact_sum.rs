//! Exact float summation with one rounding: [`ExactSum`].
//!
//! Every finite `f64` is an integer multiple of 2^-1074, the smallest
//! subnormal, so a sum of finite floats is an integer number of 2^-1074
//! units. `ExactSum` keeps that integer exactly, in two's complement over
//! 64-bit limbs wide enough that 2^64 additions of `f64::MAX` cannot overflow,
//! and counts the addends that have no finite value (NaN and the infinities)
//! and the negative zeros that decide the sign of a zero result. Adding,
//! subtracting and merging are integer additions with carry, so none of them
//! rounds; [`ExactSum::round_with`] is the only rounding, once, to nearest
//! with ties to even. The rounded sum therefore depends only on the multiset
//! of addends: not on the order they arrived in, not on how partial sums were
//! split and merged (a spilled aggregate merges one partial per spill run),
//! and not on which addends were added and later subtracted.
//!
//! This is the fixed-point exact accumulator of the exact-summation
//! literature, implemented here because a partial sum must also serialize
//! into a spill run and subtract exactly for retraction, which the maintained
//! float-summation crates do not offer together. `tests/exact_sum_oracle.rs`
//! checks it bit for bit against an independent implementation, a
//! dev-dependency only, on fixed ill-conditioned cases and on generated
//! sequences, folded, merged and with an addend subtracted.

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Limbs of the fixed-point integer. Bit 0 weighs 2^-1074 and the top bit is
/// the sign. A finite float's magnitude is below 2^1024, which is 2^2098
/// units; 2^64 of them stay below 2^2162, and a 128-bit integer part below
/// 2^1201 units, so 2,176 bits (34 limbs) hold any reachable sum with the
/// sign bit to spare.
const LIMBS: usize = 34;

/// Bit offset of 2^0: an integer part is added at this offset.
const UNIT_OFFSET: usize = 1074;

/// The explicit fraction bits of an `f64`.
const FRACTION_BITS: u32 = 52;
const FRACTION_MASK: u64 = (1 << FRACTION_BITS) - 1;

/// Biased exponent of the infinities and NaN.
const EXPONENT_SPECIAL: u64 = 0x7FF;

/// An exact, mergeable, reversible sum of `f64` addends, rounded once when
/// read.
///
/// Holds one pointer inline. The limbs and counts are allocated at the first
/// float addend (or the first merge from an allocated sum) and freed when the
/// last float addend is subtracted, so a sum with no float addends allocates
/// nothing and a sum retracted back to no floats is identical to one that
/// never saw any. [`heap_size`](Self::heap_size) reports that allocation.
///
/// Serializes (serde) to its exact state, so a spilled partial sum reloads
/// unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExactSum {
    parts: Option<Box<Parts>>,
}

/// The allocated state of a sum with at least one float addend.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Parts {
    /// The sum of the finite addends in 2^-1074 units, two's complement,
    /// least significant limb first.
    #[serde(with = "limbs_serde")]
    limbs: [u64; LIMBS],
    /// Float addends of every kind, including zeros, NaN and infinities. The
    /// allocation is freed when this returns to zero.
    floats: u64,
    nans: u64,
    positive_infinities: u64,
    negative_infinities: u64,
    negative_zeros: u64,
}

/// Bytes of the allocation an [`ExactSum`] holds once it has a float addend.
const ALLOCATION_BYTES: usize = std::mem::size_of::<Parts>();

#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    Add,
    Subtract,
}

impl Direction {
    fn step(self, count: &mut u64) {
        match self {
            Direction::Add => *count += 1,
            Direction::Subtract => {
                debug_assert!(*count > 0, "subtracted an addend the sum does not hold");
                *count = count.saturating_sub(1);
            }
        }
    }
}

impl Parts {
    fn empty() -> Self {
        Self {
            limbs: [0; LIMBS],
            floats: 0,
            nans: 0,
            positive_infinities: 0,
            negative_infinities: 0,
            negative_zeros: 0,
        }
    }

    /// Add or subtract one float addend, counted in `floats` and, when it has
    /// no finite value or is a negative zero, in its own count.
    fn apply(&mut self, x: f64, direction: Direction) {
        direction.step(&mut self.floats);
        if x.is_nan() {
            direction.step(&mut self.nans);
        } else if x == f64::INFINITY {
            direction.step(&mut self.positive_infinities);
        } else if x == f64::NEG_INFINITY {
            direction.step(&mut self.negative_infinities);
        } else if x == 0.0 {
            if x.is_sign_negative() {
                direction.step(&mut self.negative_zeros);
            }
        } else {
            let (mantissa, offset) = decompose(x);
            let subtract = x.is_sign_negative() != (direction == Direction::Subtract);
            if subtract {
                sub_shifted(&mut self.limbs, u128::from(mantissa), offset);
            } else {
                add_shifted(&mut self.limbs, u128::from(mantissa), offset);
            }
        }
    }

    fn merge(&mut self, other: &Parts) {
        let mut carry = false;
        for (ours, theirs) in self.limbs.iter_mut().zip(other.limbs.iter()) {
            let (sum, c1) = ours.overflowing_add(*theirs);
            let (sum, c2) = sum.overflowing_add(u64::from(carry));
            *ours = sum;
            carry = c1 || c2;
        }
        self.floats += other.floats;
        self.nans += other.nans;
        self.positive_infinities += other.positive_infinities;
        self.negative_infinities += other.negative_infinities;
        self.negative_zeros += other.negative_zeros;
    }
}

impl ExactSum {
    /// An empty sum. Allocates nothing.
    pub const fn new() -> Self {
        Self { parts: None }
    }

    /// Add one float addend exactly. Returns the bytes this call allocated:
    /// the state's size at the sum's first float addend, else 0.
    pub fn add_f64(&mut self, x: f64) -> usize {
        let allocated = self.parts.is_none();
        self.parts
            .get_or_insert_with(|| Box::new(Parts::empty()))
            .apply(x, Direction::Add);
        if allocated { ALLOCATION_BYTES } else { 0 }
    }

    /// Subtract one float addend the sum holds, exactly: the inverse of
    /// [`add_f64`](Self::add_f64) with the same value. Returns the heap delta:
    /// minus the state's size when this removes the last float addend (the
    /// state is then freed and the sum is empty), else 0.
    ///
    /// The caller must only subtract an addend it added; subtracting one the
    /// sum does not hold is a logic error (debug-asserted) and leaves a sum
    /// that no multiset of addends describes.
    pub fn sub_f64(&mut self, x: f64) -> isize {
        let Some(parts) = self.parts.as_mut() else {
            debug_assert!(false, "subtracted a float from a sum with no float addend");
            return 0;
        };
        parts.apply(x, Direction::Subtract);
        if parts.floats > 0 {
            return 0;
        }
        debug_assert!(
            parts.limbs.iter().all(|limb| *limb == 0),
            "a sum with no float addend left holds a nonzero value"
        );
        self.parts = None;
        -(ALLOCATION_BYTES as isize)
    }

    /// Add every addend of `other` exactly. Allocates (reported by
    /// [`heap_size`](Self::heap_size), not returned) when `self` has no float
    /// addend and `other` has.
    pub fn merge(&mut self, other: &ExactSum) {
        let Some(theirs) = other.parts.as_deref() else {
            return;
        };
        match self.parts.as_deref_mut() {
            Some(ours) => ours.merge(theirs),
            None => self.parts = Some(Box::new(theirs.clone())),
        }
    }

    /// The exact value `int_part + Σ addends`, rounded once to the nearest
    /// `f64`, ties to even, with IEEE-754's rules for an exactly computed sum:
    ///
    /// - NaN when any addend is NaN, or when there are infinities of both
    ///   signs;
    /// - otherwise an infinity of one sign when any addend is one;
    /// - a finite sum beyond the `f64` range rounds to the infinity of its
    ///   sign;
    /// - an exact zero is `-0.0` only when every addend was `-0.0` and
    ///   `int_part` is zero, else `+0.0`. A caller whose integer addends total
    ///   zero must read a zero result as `+0.0` (an integer zero is `+0`).
    ///
    /// A nonzero exact sum never rounds to zero: every nonzero multiple of
    /// 2^-1074 is at least the smallest subnormal. Pure; copies the limbs.
    pub fn round_with(&self, int_part: i128) -> f64 {
        if let Some(parts) = self.parts.as_deref() {
            if parts.nans > 0 || (parts.positive_infinities > 0 && parts.negative_infinities > 0) {
                return f64::NAN;
            }
            if parts.positive_infinities > 0 {
                return f64::INFINITY;
            }
            if parts.negative_infinities > 0 {
                return f64::NEG_INFINITY;
            }
        }
        let mut limbs = self
            .parts
            .as_deref()
            .map_or([0; LIMBS], |parts| parts.limbs);
        if int_part < 0 {
            sub_shifted(&mut limbs, int_part.unsigned_abs(), UNIT_OFFSET);
        } else {
            add_shifted(&mut limbs, int_part.unsigned_abs(), UNIT_OFFSET);
        }
        let negative = limbs[LIMBS - 1] >> 63 == 1;
        if negative {
            negate(&mut limbs);
        }
        let Some(high) = highest_set_bit(&limbs) else {
            return if int_part == 0 && self.all_negative_zero() {
                -0.0
            } else {
                0.0
            };
        };
        let magnitude = round_magnitude(&limbs, high);
        if negative { -magnitude } else { magnitude }
    }

    /// True when the sum has no float addend (and so holds no allocation).
    pub fn is_empty(&self) -> bool {
        self.parts.is_none()
    }

    /// The number of float addends the sum holds, of every kind.
    pub fn count(&self) -> u64 {
        self.parts.as_deref().map_or(0, |parts| parts.floats)
    }

    /// True when the sum has float addends and every one is `-0.0`.
    pub fn all_negative_zero(&self) -> bool {
        self.parts
            .as_deref()
            .is_some_and(|parts| parts.negative_zeros == parts.floats)
    }

    /// Bytes allocated for the state: its size once the sum has a float
    /// addend, else 0.
    pub fn heap_size(&self) -> usize {
        if self.parts.is_some() {
            ALLOCATION_BYTES
        } else {
            0
        }
    }
}

/// A finite nonzero float's magnitude as `mantissa × 2^offset` units of
/// 2^-1074: a subnormal is its fraction at offset 0; a normal float with
/// biased exponent `e` is its fraction with the implicit bit at `e - 1`.
fn decompose(x: f64) -> (u64, usize) {
    let bits = x.to_bits();
    let exponent = (bits >> FRACTION_BITS) & EXPONENT_SPECIAL;
    let fraction = bits & FRACTION_MASK;
    if exponent == 0 {
        (fraction, 0)
    } else {
        (fraction | (1 << FRACTION_BITS), (exponent - 1) as usize)
    }
}

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

/// `limbs += value << offset`, carrying through the top limb (two's
/// complement wraps there, which is how a negative total turns positive).
fn add_shifted(limbs: &mut [u64; LIMBS], value: u128, offset: usize) {
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
fn sub_shifted(limbs: &mut [u64; LIMBS], value: u128, offset: usize) {
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

/// Two's complement negation in place.
fn negate(limbs: &mut [u64; LIMBS]) {
    let mut carry = true;
    for limb in limbs.iter_mut() {
        let (value, overflow) = (!*limb).overflowing_add(u64::from(carry));
        *limb = value;
        carry = overflow;
    }
}

/// Index of the highest set bit, or `None` for zero.
fn highest_set_bit(limbs: &[u64; LIMBS]) -> Option<usize> {
    limbs
        .iter()
        .enumerate()
        .rev()
        .find(|(_, limb)| **limb != 0)
        .map(|(index, limb)| index * 64 + 63 - limb.leading_zeros() as usize)
}

/// `count` (at most 64) bits of `limbs` starting at bit `start`.
fn bits_at(limbs: &[u64; LIMBS], start: usize, count: u32) -> u64 {
    let index = start / 64;
    let shift = (start % 64) as u32;
    let mut value = limbs[index] >> shift;
    if shift > 0 && index + 1 < LIMBS {
        value |= limbs[index + 1] << (64 - shift);
    }
    if count < 64 {
        value & ((1 << count) - 1)
    } else {
        value
    }
}

/// True when any bit below bit `end` is set.
fn any_bit_below(limbs: &[u64; LIMBS], end: usize) -> bool {
    let whole = end / 64;
    let partial = (end % 64) as u32;
    limbs[..whole].iter().any(|limb| *limb != 0)
        || (partial > 0 && limbs[whole] & ((1 << partial) - 1) != 0)
}

/// Round a nonzero magnitude whose highest set bit is `high` to the nearest
/// `f64`, ties to even; `+∞` when it rounds beyond `f64::MAX`.
fn round_magnitude(limbs: &[u64; LIMBS], high: usize) -> f64 {
    let significant = FRACTION_BITS as usize;
    if high <= significant {
        // Below 2^53 units the value is exact, and its units are the float's
        // bits: a subnormal's fraction, or at 2^52 and above biased exponent 1
        // with the implicit bit landing in the exponent field.
        return f64::from_bits(limbs[0]);
    }
    let shift = high - significant;
    let mut mantissa = bits_at(limbs, shift, FRACTION_BITS + 1);
    let guard = bits_at(limbs, shift - 1, 1) == 1;
    let sticky = any_bit_below(limbs, shift - 1);
    if guard && (sticky || mantissa & 1 == 1) {
        mantissa += 1;
    }
    // `mantissa × 2^(shift - 1074)` with the mantissa in [2^52, 2^53) has
    // biased exponent `shift + 1`.
    let mut exponent = shift as u64 + 1;
    if mantissa == 1 << (FRACTION_BITS + 1) {
        mantissa >>= 1;
        exponent += 1;
    }
    if exponent >= EXPONENT_SPECIAL {
        return f64::INFINITY;
    }
    f64::from_bits((exponent << FRACTION_BITS) | (mantissa & FRACTION_MASK))
}

/// The limb array as a sequence, checked for length on the way in: serde's
/// derive covers arrays of at most 32 elements.
mod limbs_serde {
    use super::{Deserialize, Deserializer, LIMBS, Serializer};

    pub(super) fn serialize<S: Serializer>(
        limbs: &[u64; LIMBS],
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(limbs.iter())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<[u64; LIMBS], D::Error> {
        let limbs = Vec::<u64>::deserialize(deserializer)?;
        let len = limbs.len();
        limbs
            .try_into()
            .map_err(|_| serde::de::Error::invalid_length(len, &"the exact sum's 34 limbs"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sum_of(values: &[f64]) -> ExactSum {
        let mut sum = ExactSum::new();
        for value in values {
            sum.add_f64(*value);
        }
        sum
    }

    fn assert_bits(actual: f64, expected: f64, context: &str) {
        assert_eq!(
            actual.to_bits(),
            expected.to_bits(),
            "{context}: got {actual:e}, expected {expected:e}"
        );
    }

    #[test]
    fn exact_sum_rounds_known_sums_once() {
        // 1e16 + 1 is not a double (the spacing there is 2), so a left-to-
        // right fold loses the 1; the exact sum keeps it.
        assert_bits(sum_of(&[1e16, 1.0, -1e16]).round_with(0), 1.0, "cancel");
        assert_bits(sum_of(&[0.5, 0.25]).round_with(0), 0.75, "exact");
        assert_bits(sum_of(&[0.5]).round_with(6), 6.5, "integer part");
        assert_bits(sum_of(&[-0.5]).round_with(-6), -6.5, "negative");
        assert_bits(sum_of(&[0.5]).round_with(-1), -0.5, "sign change");
        // 2^53 + 1 is a tie between 2^53 and 2^53 + 2: to even, 2^53.
        let two_53 = 9_007_199_254_740_992.0;
        assert_bits(sum_of(&[two_53, 1.0]).round_with(0), two_53, "tie to even");
        // 2^53 + 3 is a tie between 2^53 + 2 and 2^53 + 4: to even, + 4.
        assert_bits(sum_of(&[two_53, 3.0]).round_with(0), two_53 + 4.0, "tie up");
        // Just above the tie rounds up.
        assert_bits(
            sum_of(&[two_53, 1.0, f64::from_bits(1)]).round_with(0),
            two_53 + 2.0,
            "sticky bit",
        );
        // The smallest subnormals add exactly and cross into the normals.
        let tiny = f64::from_bits(1);
        assert_bits(
            sum_of(&[tiny, tiny, tiny]).round_with(0),
            f64::from_bits(3),
            "subnormal",
        );
        let largest_subnormal = f64::from_bits(FRACTION_MASK);
        assert_bits(
            sum_of(&[largest_subnormal, tiny]).round_with(0),
            f64::MIN_POSITIVE,
            "subnormal to normal",
        );
        assert_bits(
            sum_of(&[f64::MAX, f64::MAX, -f64::MAX]).round_with(0),
            f64::MAX,
            "beyond the range and back",
        );
        assert_bits(
            sum_of(&[-f64::MAX, -f64::MAX]).round_with(0),
            f64::NEG_INFINITY,
            "negative overflow",
        );
        // i128 extremes as the integer part.
        assert_bits(
            ExactSum::new().round_with(i128::MIN),
            -(2.0_f64.powi(127)),
            "i128::MIN",
        );
        assert_bits(
            ExactSum::new().round_with(i128::MAX),
            2.0_f64.powi(127),
            "i128::MAX",
        );
    }

    #[test]
    fn exact_sum_add_sub_and_merge_are_exact() {
        let values = [
            1e300,
            -3.5,
            1e-300,
            f64::from_bits(7),
            -1e300,
            2.0_f64.powi(-1022),
        ];
        let whole = sum_of(&values);
        for at in 0..=values.len() {
            let (left, right) = values.split_at(at);
            let mut merged = sum_of(left);
            merged.merge(&sum_of(right));
            assert_eq!(merged, whole, "split at {at}");
        }
        // Subtracting every addend returns the empty, unallocated sum.
        let mut sum = whole.clone();
        let mut freed = 0;
        for value in values {
            freed += sum.sub_f64(value);
        }
        assert_eq!(sum, ExactSum::new());
        assert_eq!(freed, -(ALLOCATION_BYTES as isize));
        assert_eq!(sum.heap_size(), 0);
    }

    #[test]
    fn exact_sum_reports_its_one_allocation() {
        let mut sum = ExactSum::new();
        assert_eq!(sum.heap_size(), 0);
        assert_eq!(sum.add_f64(1.5), ALLOCATION_BYTES);
        assert_eq!(sum.add_f64(2.5), 0);
        assert_eq!(sum.heap_size(), ALLOCATION_BYTES);
        assert_eq!(sum.sub_f64(1.5), 0);
        assert_eq!(sum.sub_f64(2.5), -(ALLOCATION_BYTES as isize));
        assert!(sum.is_empty());
    }

    #[test]
    fn exact_sum_rejects_a_limb_array_of_the_wrong_length() {
        let json = serde_json::to_value(sum_of(&[1.0])).expect("serialize");
        let mut short = json.clone();
        short["limbs"]
            .as_array_mut()
            .expect("limbs are a sequence")
            .pop();
        assert!(serde_json::from_value::<ExactSum>(short).is_err());
        assert_eq!(
            serde_json::from_value::<ExactSum>(json).expect("deserialize"),
            sum_of(&[1.0])
        );
    }
}
