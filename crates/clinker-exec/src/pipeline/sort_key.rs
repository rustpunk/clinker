//! The authored sort comparator and its memcomparable key.
//!
//! [`compare_authored_keys`] orders records by the fields, directions and null
//! placement an author declared; [`encode_sort_key`] writes the same order as a
//! byte sequence whose lexicographic comparison (`memcmp`) equals it. The loser
//! tree of an external merge sort and the spilled and streaming aggregates
//! compare the bytes; the in-memory sort, the declared-order check and the
//! window partition sort call the comparator. Both order non-null values by
//! the one value order, [`clinker_record::order`], so the in-memory and the
//! spilled path of every sort agree.
//!
//! Key layout, per sort field:
//! - `[null_sentinel: 1 byte] [value key: N bytes]`
//! - Null (or an absent field): sentinel only, `0x00` for nulls-first and
//!   `0x02` for nulls-last
//! - Non-null: sentinel `0x01`, then [`clinker_record::order::encode`]: a rank
//!   tag (bool < number < string < date < datetime < array < map); a number as
//!   the orderable bits of the largest `f64` not above it, an exactness byte,
//!   and the exact scale-28 decimal grid when it is not an `f64`, so integers,
//!   floats and decimals compare by exact value; every NaN as one value above
//!   `+inf`; `-0.0` as `0.0`; strings with escaped NULs and a two-byte
//!   terminator
//! - Descending: XOR the value key bytes with 0xFF; null placement remains
//!   exactly as authored

use std::cmp::Ordering;

use clinker_record::{Record, Value};

use clinker_plan::config::{NullOrder, SortField, SortOrder};

/// Compare two records using only the fields, directions, and null placement
/// the author declared.
///
/// Returning [`Ordering::Equal`] is intentional when every authored key is
/// equal. Callers use stable sorting/merge metadata to preserve arrival order;
/// source identity, filenames, hashes, and other undeclared values never enter
/// this comparison.
pub fn compare_authored_keys(a: &Record, b: &Record, sort_by: &[SortField]) -> Ordering {
    for field in sort_by {
        let ordering = compare_authored_values_with_nulls(
            a.get(&field.field),
            b.get(&field.field),
            field.order,
            field.null_order.unwrap_or(NullOrder::Last),
        );
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    Ordering::Equal
}

/// Build the memcomparable form of exactly the authored key.
///
/// The returned bytes contain no identity tie-breaker. For values admitted by
/// one resolved sort field, lexicographic byte comparison matches
/// [`compare_authored_keys`].
pub fn stable_sort_key_for_record(record: &Record, sort_by: &[SortField]) -> Vec<u8> {
    encode_sort_key(record, sort_by)
}

/// Compare optional record values under one authored direction/null rule.
pub fn compare_authored_values_with_nulls(
    a: Option<&Value>,
    b: Option<&Value>,
    order: SortOrder,
    null_order: NullOrder,
) -> Ordering {
    let a_null = a.is_none() || a.is_some_and(Value::is_null);
    let b_null = b.is_none() || b.is_some_and(Value::is_null);

    match (a_null, b_null) {
        (true, true) => Ordering::Equal,
        (true, false) => match null_order {
            NullOrder::First => Ordering::Less,
            NullOrder::Last | NullOrder::Drop => Ordering::Greater,
        },
        (false, true) => match null_order {
            NullOrder::First => Ordering::Greater,
            NullOrder::Last | NullOrder::Drop => Ordering::Less,
        },
        (false, false) => {
            let (Some(a), Some(b)) = (a, b) else {
                return Ordering::Equal;
            };
            let base = compare_authored_values(a, b);
            match order {
                SortOrder::Asc => base,
                SortOrder::Desc => base.reverse(),
            }
        }
    }
}

/// Compare two non-null values in ascending order: the one value order,
/// [`clinker_record::order::compare`].
///
/// Total over every value, NaN and mixed types included, so a stable sort's
/// output never depends on where run boundaries fall. Nulls never reach it:
/// [`compare_authored_values_with_nulls`] places them by the authored
/// `null_order` first.
pub fn compare_authored_values(a: &Value, b: &Value) -> Ordering {
    clinker_record::order::compare(a, b)
}

/// Encode a record's sort fields as a memcomparable byte sequence.
pub fn encode_sort_key(record: &Record, sort_by: &[SortField]) -> Vec<u8> {
    let mut key = Vec::with_capacity(sort_by.len() * 10);
    encode_sort_key_into(record, sort_by, &mut key);
    key
}

fn encode_sort_key_into(record: &Record, sort_by: &[SortField], key: &mut Vec<u8>) {
    key.clear();
    for sf in sort_by {
        let null_order = sf.null_order.unwrap_or(NullOrder::Last);
        match record.get(&sf.field) {
            None | Some(Value::Null) => {
                key.push(match null_order {
                    NullOrder::First => 0x00,
                    NullOrder::Last | NullOrder::Drop => 0x02,
                });
            }
            Some(value) => {
                key.push(0x01); // non-null sentinel
                let value_start = key.len();
                clinker_record::order::encode(value, key);
                if sf.order == SortOrder::Desc {
                    for byte in &mut key[value_start..] {
                        *byte ^= 0xFF;
                    }
                }
            }
        }
    }
}

/// Order-preserving `f64` → `i64`: signed-`i64` comparison of the result
/// matches the value order of finite floats, `-0.0` equal to `+0.0`.
///
/// Derived from [`clinker_record::order::f64_orderable_bits`], the one float
/// key, so a range join orders floats exactly as a sort does. The range-join
/// kernels carry keys as `i64` and compare them directly, so they need a
/// monotone `i64` rather than the memcomparable byte key. They exclude
/// non-finite keys before reaching here. A raw `f.to_bits() as i64` is NOT
/// monotone: IEEE negatives set the sign bit, so their bit patterns grow as
/// the value shrinks, and any range predicate over a float column with
/// negative values would match wrongly.
#[inline]
pub(crate) fn float_to_orderable_i64(f: f64) -> i64 {
    // Flip the high bit to turn the unsigned-ordered u64 into a
    // two's-complement-ordered i64 (smallest u64 → i64::MIN).
    (clinker_record::order::f64_orderable_bits(f) ^ (1 << 63)) as i64
}

/// Order-preserving `f64` → `i128`: the sign-extended widening of
/// [`float_to_orderable_i64`], so it too derives from
/// [`clinker_record::order::f64_orderable_bits`].
///
/// The inequality-join range axis carries keys as `i128` so it can also hold
/// the fixed-point decimal grid (see [`decimal_to_orderable_i128`]). Sign
/// extension preserves the signed ordering exactly, so a float range key
/// compares identically to the earlier `i64` axis — the widening is
/// behavior-preserving for float and integer axes.
#[inline]
pub(crate) fn float_to_orderable_i128(f: f64) -> i128 {
    float_to_orderable_i64(f) as i128
}

/// Canonical fixed-point decimal order grid — coarse (`N = 18`) resolution; the
/// `i128`-axis member of the grid family documented on
/// [`clinker_record::order::DECIMAL_SORT_KEY_SCALE`].
///
/// Every decimal on a decimal inequality-join axis is placed on this common
/// `10^18` fixed-point grid. 18 fractional digits are preserved exactly, and the
/// integer magnitude reaches `i128::MAX / 10^18 ≈ 1.7e20` — far beyond any
/// realistic monetary value. Chosen so a full `i64` operand widened onto the same
/// grid always fits (`i64::MAX · 10^18 < i128::MAX`), keeping a mixed
/// decimal/integer axis exact. This grid is exactly the finer scale-28 sort/group
/// key grid rescaled by `10^-10`, so the range axis and the memcomparable
/// sort/group key never disagree on order (pinned by the differential test
/// `decimal_encoders_induce_one_total_order`).
pub(crate) const DECIMAL_RANGE_SCALE: u32 = 18;

/// Order-preserving, value-canonical `Decimal` → `i128` for the inequality-join
/// range axis: the decimal placed on the fixed `10^18` grid as the integer
/// `mantissa × 10^(18 − scale)`.
///
/// Exact and injective for the representable range: `2.5` and `2.50` map to the
/// same `i128` (scale-invariant), distinct values never collide, and signed
/// ordering matches numeric ordering because `mantissa` already carries the
/// sign. Returns `None` — never a wrong key — when the value cannot be placed
/// exactly: a scale beyond 18 fractional digits would truncate, or the scaled
/// magnitude would overflow `i128`. The caller turns `None` into a fail-loud
/// typed error rather than dropping the row.
#[inline]
pub(crate) fn decimal_to_orderable_i128(d: rust_decimal::Decimal) -> Option<i128> {
    // `rust_decimal` does not auto-strip trailing fractional zeros, so a value
    // like `1.5` stored at scale 20 (`1.50000000000000000000`) carries scale 20
    // even though it is exactly representable at scale 1. Normalize away those
    // trailing zeros before the truncation check so an exactly-representable
    // value is never a false out-of-range. Only pay the normalize cost when the
    // scale actually exceeds the grid — the common path is untouched.
    let d = if d.scale() > DECIMAL_RANGE_SCALE {
        d.normalize()
    } else {
        d
    };
    let scale = d.scale();
    if scale > DECIMAL_RANGE_SCALE {
        return None; // more significant fractional digits than the grid holds → truncation
    }
    // `mantissa` is a 96-bit integer, so it always fits `i128`; the scaling
    // multiply is the only step that can overflow.
    let pow = 10i128.pow(DECIMAL_RANGE_SCALE - scale);
    d.mantissa().checked_mul(pow)
}

/// Place an integer operand of a decimal range axis on the same `10^18` grid as
/// [`decimal_to_orderable_i128`], so a mixed `decimal`/`integer` comparison
/// (the CXL typechecker widens the integer exactly into the decimal context)
/// stays exact on one axis. Always `Some` for an `i64`: `i64::MAX · 10^18`
/// stays within `i128`.
#[inline]
pub(crate) fn integer_on_decimal_grid(i: i64) -> Option<i128> {
    (i as i128).checked_mul(10i128.pow(DECIMAL_RANGE_SCALE))
}

/// The one datetime key, defined in the value order so the memcomparable key,
/// the inequality-join axis and the sort-merge range comparator reduce
/// datetimes identically.
pub(crate) use clinker_record::order::datetime_to_orderable_i128;

/// Owning wrapper around a `Vec<SortField>` that encodes and compares
/// memcomparable sort keys with zero steady-state allocation.
///
/// The encoder holds the field list once and exposes:
///
/// * [`SortKeyEncoder::encode_into`] — write a key into a caller-owned
///   scratch `Vec<u8>`, reusing its backing capacity across calls.
/// * [`SortKeyEncoder::compare_encoded`] — raw byte comparison; direction
///   and null ordering are already baked into the bytes by
///   [`encode_sort_key`], so `<` / `==` / `>` on the byte slices yields
///   the declared-order result by construction.
/// * [`SortKeyEncoder::debug_decode_pair`] — hex-dump both keys for
///   `PipelineError::SortOrderViolation` messages. The encoding is not
///   losslessly decodable (strings, for example, lose length framing
///   after the terminator XOR on DESC order), so the debug renderer
///   reports the field list together with both hex byte sequences.
#[derive(Debug, Clone)]
pub struct SortKeyEncoder {
    sort_by: Vec<SortField>,
}

impl SortKeyEncoder {
    /// Construct an encoder from a list of sort fields. The order is
    /// significant — lexicographic key comparison walks the fields in
    /// the supplied order, so this must match the declared sort order
    /// of the upstream.
    pub fn new(sort_by: Vec<SortField>) -> Self {
        Self { sort_by }
    }

    /// The sort fields this encoder was built with.
    pub fn sort_fields(&self) -> &[SortField] {
        &self.sort_by
    }

    /// Encode `record` into the caller-owned scratch `out` buffer.
    ///
    /// The buffer is cleared (`Vec::clear`, which preserves capacity)
    /// and then re-populated in place. After the first call, the
    /// steady-state allocation cost is zero — subsequent calls reuse
    /// the same backing allocation as long as the caller holds onto
    /// the `Vec`. This is the streaming-aggregator hot path's
    /// contract with the sort-key layer.
    pub fn encode_into(&self, record: &Record, out: &mut Vec<u8>) {
        encode_sort_key_into(record, &self.sort_by, out);
    }

    /// Compare two pre-encoded sort keys.
    ///
    /// Because `encode_sort_key` / [`SortKeyEncoder::encode_into`]
    /// bakes direction (ASC/DESC) and null ordering into the emitted
    /// bytes, raw lexicographic `memcmp` of the two slices yields the
    /// declared-order result by construction. No knowledge of the
    /// field list is required at comparison time.
    pub fn compare_encoded(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    /// Render a human-readable debug string for a pair of pre-encoded
    /// keys. Used by `PipelineError::SortOrderViolation` messages so
    /// the user can see which two keys collided.
    ///
    /// The memcomparable format is not losslessly decodable without
    /// type hints (and after DESC XOR the original bytes are masked),
    /// so this intentionally surfaces the field list alongside both
    /// hex byte sequences rather than pretending to reconstruct the
    /// original values. Callers embed the result directly in the
    /// `SortOrderViolation` message.
    pub fn debug_decode_pair(&self, prev: &[u8], next: &[u8]) -> String {
        use std::fmt::Write as _;
        let mut out = String::new();
        out.push_str("sort_fields=[");
        for (i, sf) in self.sort_by.iter().enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            let dir = match sf.order {
                SortOrder::Asc => "ASC",
                SortOrder::Desc => "DESC",
            };
            let nulls = match sf.null_order.unwrap_or(NullOrder::Last) {
                NullOrder::First => "NULLS FIRST",
                NullOrder::Last => "NULLS LAST",
                NullOrder::Drop => "NULLS DROP",
            };
            let _ = write!(out, "{} {} {}", sf.field, dir, nulls);
        }
        out.push_str("] prev=0x");
        for b in prev {
            let _ = write!(out, "{b:02x}");
        }
        out.push_str(" next=0x");
        for b in next {
            let _ = write!(out, "{b:02x}");
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime};
    use clinker_record::Schema;
    use clinker_record::order::DECIMAL_SORT_KEY_SCALE;
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use std::sync::Arc;

    fn dec_key(d: Decimal) -> Vec<u8> {
        let mut buf = Vec::new();
        clinker_record::order::encode(&Value::Decimal(d), &mut buf);
        buf
    }

    #[test]
    fn float_to_orderable_i64_preserves_order() {
        // Smallest positive subnormal and its negation bracket zero more
        // tightly than any normal value can.
        let smallest_subnormal = f64::from_bits(1);
        // Strictly ascending in IEEE order, except the signed-zero pair,
        // which is equal. Spans both signs, subnormals, and the extremes.
        let ascending = [
            f64::MIN,
            -1.0e300,
            -2.0,
            -1.0,
            -f64::MIN_POSITIVE,
            -smallest_subnormal,
            -0.0,
            0.0,
            smallest_subnormal,
            f64::MIN_POSITIVE,
            1.0,
            2.0,
            1.0e300,
            f64::MAX,
        ];
        let encoded: Vec<i64> = ascending
            .iter()
            .map(|&f| float_to_orderable_i64(f))
            .collect();
        for (i, w) in encoded.windows(2).enumerate() {
            // Only the -0.0/+0.0 adjacency is an equality; every other
            // adjacency is a strict increase.
            let signed_zero_pair = ascending[i] == 0.0 && ascending[i + 1] == 0.0;
            if signed_zero_pair {
                assert_eq!(w[0], w[1], "signed zeros must encode to the same i64");
            } else {
                assert!(
                    w[0] < w[1],
                    "encoding not order-preserving at {}: {} then {} → {} then {}",
                    i,
                    ascending[i],
                    ascending[i + 1],
                    w[0],
                    w[1]
                );
            }
        }
        // The raw `f.to_bits() as i64` this replaced fails two ways, each
        // pinned by an assert here: `-0.0` maps to `i64::MIN` instead of
        // `0`, and among negatives a larger magnitude maps to a larger i64,
        // inverting their order. Cross-sign order happened to survive the
        // old cast (negatives already sat below positives), so the middle
        // assert is only a sanity check, not a discriminator.
        assert_eq!(float_to_orderable_i64(-0.0), float_to_orderable_i64(0.0));
        assert!(float_to_orderable_i64(-1.0) < float_to_orderable_i64(1.0));
        assert!(float_to_orderable_i64(-1.0e300) < float_to_orderable_i64(-2.0));
    }

    #[test]
    fn decimal_sort_key_is_value_canonical() {
        // Equal values of differing scale MUST produce identical keys — the
        // spilled-aggregation group-identity invariant.
        assert_eq!(dec_key(Decimal::new(250, 2)), dec_key(Decimal::new(25, 1))); // 2.50 == 2.5
        assert_eq!(dec_key(Decimal::new(0, 0)), dec_key(Decimal::new(0, 5))); // 0 == 0.00000
        assert_eq!(dec_key(Decimal::new(4200, 2)), dec_key(Decimal::new(42, 0))); // 42.00 == 42
        // Distinct values MUST NOT collide (a lossy f64 projection would).
        assert_ne!(dec_key(Decimal::new(250, 2)), dec_key(Decimal::new(251, 2)));
        // Two decimals that round to the same f64 must still differ.
        assert_ne!(
            dec_key(Decimal::new(9007199254740992, 0)),
            dec_key(Decimal::new(9007199254740993, 0))
        );
    }

    #[test]
    fn decimal_sort_key_is_order_preserving() {
        let mut vals = vec![
            Decimal::new(-99999, 2),
            Decimal::new(-1, 0),
            Decimal::new(-1, 2),
            Decimal::new(0, 0),
            Decimal::new(1, 2),
            Decimal::new(1, 0),
            Decimal::new(250, 2),
            Decimal::new(251, 2),
            Decimal::new(99999, 2),
            Decimal::MAX,
            Decimal::MIN,
        ];
        vals.sort();
        for w in vals.windows(2) {
            assert!(
                dec_key(w[0]) <= dec_key(w[1]),
                "byte order must match numeric order: {} vs {}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn float_to_orderable_i128_is_sign_extension() {
        // The i128 axis form must be the exact sign-extension of the i64 form,
        // so a float range key compares identically under either width.
        for f in [
            f64::MIN,
            -1.0e300,
            -1.0,
            -0.0,
            0.0,
            1.0,
            2.5,
            1.0e300,
            f64::MAX,
        ] {
            assert_eq!(
                float_to_orderable_i128(f),
                float_to_orderable_i64(f) as i128,
                "i128 float encoding must sign-extend the i64 form for {f}"
            );
        }
    }

    #[test]
    fn decimal_to_orderable_i128_is_canonical_and_order_preserving() {
        use rust_decimal::Decimal;
        // Scale-invariant: equal values of differing scale share one key.
        assert_eq!(
            decimal_to_orderable_i128(Decimal::new(250, 2)), // 2.50
            decimal_to_orderable_i128(Decimal::new(25, 1))   // 2.5
        );
        assert_eq!(
            decimal_to_orderable_i128(Decimal::new(0, 0)),
            decimal_to_orderable_i128(Decimal::new(0, 5))
        );
        // An integer widened onto the grid equals the same-valued decimal.
        assert_eq!(
            integer_on_decimal_grid(42),
            decimal_to_orderable_i128(Decimal::new(42, 0))
        );
        // Distinct values never collide.
        assert_ne!(
            decimal_to_orderable_i128(Decimal::new(250, 2)),
            decimal_to_orderable_i128(Decimal::new(251, 2))
        );
        // Order-preserving across a spread of magnitudes and signs.
        let mut vals = vec![
            Decimal::new(-99999, 2),
            Decimal::new(-1, 0),
            Decimal::new(-1, 2),
            Decimal::new(0, 0),
            Decimal::new(1, 2),
            Decimal::new(1, 0),
            Decimal::new(250, 2),
            Decimal::new(251, 2),
            Decimal::new(99999, 2),
        ];
        vals.sort();
        for w in vals.windows(2) {
            let a = decimal_to_orderable_i128(w[0]).expect("in range");
            let b = decimal_to_orderable_i128(w[1]).expect("in range");
            assert!(
                a <= b,
                "i128 order must match numeric order: {} vs {}",
                w[0],
                w[1]
            );
        }
    }

    #[test]
    fn decimal_to_orderable_i128_rejects_out_of_range() {
        use rust_decimal::Decimal;
        // Truncation: more than 18 fractional digits cannot be placed exactly.
        assert_eq!(decimal_to_orderable_i128(Decimal::new(1, 19)), None);
        assert_eq!(decimal_to_orderable_i128(Decimal::new(123, 28)), None);
        // Overflow: a large magnitude at a small scale exceeds i128 once scaled.
        assert_eq!(decimal_to_orderable_i128(Decimal::MAX), None);
        // A realistic monetary value (~1e9 at 2 dp: 999,999,999.99) stays
        // representable.
        assert!(decimal_to_orderable_i128(Decimal::new(99_999_999_999, 2)).is_some());
    }

    #[test]
    fn decimal_to_orderable_i128_normalizes_trailing_zeros() {
        use std::str::FromStr;
        // `rust_decimal` keeps trailing fractional zeros, so this value carries
        // scale 22 even though it is exactly `1.5`. It must NOT be a false
        // out-of-range: normalize strips the zeros before the scale-18 check, and
        // it reduces to the same key as the scale-1 form.
        let padded = Decimal::from_str("1.5000000000000000000000").unwrap();
        assert!(
            padded.scale() > DECIMAL_RANGE_SCALE,
            "value must be padded past 18 dp"
        );
        assert_eq!(
            decimal_to_orderable_i128(padded),
            decimal_to_orderable_i128(Decimal::new(15, 1))
        );
        assert!(decimal_to_orderable_i128(padded).is_some());
        // A value with genuine (nonzero) digits past 18 places still truncates.
        let genuine = Decimal::from_str("1.0000000000000000001").unwrap();
        assert_eq!(decimal_to_orderable_i128(genuine), None);
    }

    /// The single differential guard for decimals: over an adversarial corpus,
    /// the two decimal order encoders — the IEJoin i128 range axis (through the
    /// real `value_to_i128` consumer) and the memcomparable sort/group-key bytes
    /// (through `encode_sort_key`) — MUST induce the identical total order as the
    /// Sort node's `compare_values` (the one value order). The two grids run at different
    /// resolutions (scale 28 vs 18) for hard width-budget reasons, so this pins
    /// that the split can never drift them into disagreeing on order.
    #[test]
    fn decimal_encoders_induce_one_total_order() {
        use crate::pipeline::iejoin::value_to_i128;
        use crate::pipeline::sort::compare_values;
        use clinker_plan::plan::combine::RangeKeyType;
        use std::str::FromStr;

        // Every value is representable on BOTH grids (scale ≤ 18, in i128 range),
        // so all three legs participate: cross-scale equals (2.5 / 2.50 / 2.500,
        // 42 / 42.00, 0 / 0.00000), a sub-10^-17 near-tie of 2.5, the finest axis
        // grid point (±10^-18), negatives, and magnitudes near the axis ceiling.
        let decimals = [
            Decimal::new(0, 0),
            Decimal::new(0, 5),  // 0.00000 — cross-scale zero
            Decimal::new(1, 18), // 0.000000000000000001 — finest axis point
            Decimal::new(2, 18),
            Decimal::new(-1, 18),
            Decimal::new(1, 2), // 0.01
            Decimal::new(-1, 2),
            Decimal::new(1, 0),
            Decimal::new(-1, 0),
            Decimal::new(25, 1),                                // 2.5
            Decimal::new(250, 2),                               // 2.50  — cross-scale equal of 2.5
            Decimal::new(2500, 3),                              // 2.500 — cross-scale equal of 2.5
            Decimal::from_str("2.500000000000000001").unwrap(), // sub-10^-17 successor of 2.5
            Decimal::new(-25, 1),                               // -2.5
            Decimal::new(-250, 2),                              // -2.50
            Decimal::new(42, 0),
            Decimal::new(4200, 2), // 42.00 — cross-scale equal of 42
            Decimal::new(99_999_999_999_999_999, 0), // ~1e17, well inside the axis ceiling
            Decimal::new(-99_999_999_999_999_999, 0),
        ];

        let sort_field = [sf("d", SortOrder::Asc)];
        let byte_key = |d: Decimal| -> Vec<u8> {
            encode_sort_key(&make_record(&[("d", Value::Decimal(d))]), &sort_field)
        };
        let axis = |v: &Value| -> i128 {
            value_to_i128(RangeKeyType::Decimal, v)
                .expect("in-range decimal/integer never errors")
                .expect("value reduces to an axis key")
        };

        for &a in &decimals {
            for &b in &decimals {
                let (va, vb) = (Value::Decimal(a), Value::Decimal(b));
                let oracle = compare_values(&va, &vb);
                assert_eq!(
                    axis(&va).cmp(&axis(&vb)),
                    oracle,
                    "i128 range-axis order disagrees with compare_values for {a} vs {b}"
                );
                assert_eq!(
                    byte_key(a).cmp(&byte_key(b)),
                    oracle,
                    "memcomparable byte-key order disagrees with compare_values for {a} vs {b}"
                );
                assert_eq!(
                    decimal_to_orderable_i128(a).cmp(&decimal_to_orderable_i128(b)),
                    oracle,
                    "reducer order disagrees with compare_values for {a} vs {b}"
                );
            }
        }

        // The mixed decimal/integer axis: an `int` operand widened onto the same
        // 10^18 grid (`integer_on_decimal_grid`, via `value_to_i128`) must order
        // against the decimals exactly as `compare_values` widens int into the
        // decimal context. Only the axis leg participates here; the byte key's
        // integer/decimal order is the value order's, which its property suite
        // proves.
        for i in [-2i64, -1, 0, 1, 2, 3, 42, -42] {
            let iv = Value::Integer(i);
            for &d in &decimals {
                let dv = Value::Decimal(d);
                assert_eq!(
                    axis(&iv).cmp(&axis(&dv)),
                    compare_values(&iv, &dv),
                    "widened-integer axis order disagrees with compare_values for {i} vs {d}"
                );
            }
        }
    }

    /// The scale-18 range axis is exactly the scale-28 sort/group-key grid
    /// rescaled by `10^(28−18) = 10^10`: for any decimal whose scale-28 grid
    /// magnitude also fits an `i128`, `axis × 10^10 == mantissa × 10^(28−scale)`.
    /// This pins the single `10^10` factor the two grid `const`s encode, so
    /// changing one scale without the other fails here rather than silently
    /// splitting the two encodings onto unrelated grids.
    #[test]
    fn decimal_axis_is_ten_pow_ten_rescaling_of_sort_key_grid() {
        // Magnitudes chosen so `mantissa × 10^(28−scale)` stays inside i128
        // (|value| ≲ 1.7e10), letting the finer grid be computed as a plain i128.
        let corpus = [
            Decimal::new(0, 0),
            Decimal::new(1, 18),
            Decimal::new(-1, 18),
            Decimal::new(25, 1),
            Decimal::new(250, 2),
            Decimal::new(-25, 1),
            Decimal::new(42, 0),
            Decimal::new(123_456, 3),
            Decimal::new(-987_654_321, 4),
        ];
        let factor = 10i128.pow(DECIMAL_SORT_KEY_SCALE - DECIMAL_RANGE_SCALE);
        for &d in &corpus {
            let fine = d
                .mantissa()
                .checked_mul(10i128.pow(DECIMAL_SORT_KEY_SCALE - d.scale()))
                .expect("corpus chosen so the scale-28 grid fits i128");
            let axis = decimal_to_orderable_i128(d).expect("representable on the axis");
            assert_eq!(
                axis.checked_mul(factor)
                    .expect("axis × 10^10 fits i128 here"),
                fine,
                "axis key must be the scale-28 grid magnitude rescaled by 10^10 for {d}"
            );
        }
    }

    /// Build a `NaiveDateTime` at nanosecond resolution.
    fn ndt(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32, nano: u32) -> NaiveDateTime {
        NaiveDate::from_ymd_opt(y, mo, d)
            .unwrap()
            .and_hms_nano_opt(h, mi, s, nano)
            .unwrap()
    }

    /// The canonical datetime key is injective at nanosecond resolution — the
    /// exact property the microsecond encoder lacked. Two datetimes that share a
    /// microsecond but differ by a nanosecond MUST map to distinct keys, or a
    /// spilled aggregation would merge them into one group and a range sweep
    /// would drop the boundary match.
    #[test]
    fn datetime_to_orderable_i128_is_injective_at_nanos() {
        let a = ndt(2024, 1, 1, 0, 0, 0, 1_000); // …000001000  (micros = 1)
        let b = ndt(2024, 1, 1, 0, 0, 0, 1_500); // …000001500  (micros = 1)
        assert_ne!(
            datetime_to_orderable_i128(a),
            datetime_to_orderable_i128(b),
            "sub-microsecond datetimes must not collide onto one key"
        );
        // The key is exactly the signed nanosecond count.
        assert_eq!(datetime_to_orderable_i128(a), 1_704_067_200_000_001_000);
        assert_eq!(
            datetime_to_orderable_i128(b) - datetime_to_orderable_i128(a),
            500
        );
    }

    /// Order preservation across the full representable range, including dates
    /// OUTSIDE the `i64`-nanosecond window (1677–2262) that `timestamp_nanos_opt`
    /// saturates: a year-1400 instant is negative and a year-3000 instant is a
    /// large positive, and their key order matches chronological order exactly.
    #[test]
    fn datetime_to_orderable_i128_is_order_preserving_full_range() {
        // Strictly ascending in time, spanning pre-epoch, sub-microsecond ties,
        // and both ends of the out-of-`i64`-nanos range.
        let ascending = [
            ndt(1400, 1, 1, 0, 0, 0, 0), // deep pre-epoch → negative key
            ndt(1677, 1, 1, 0, 0, 0, 0), // just outside the i64-nanos floor
            ndt(1969, 12, 31, 23, 59, 59, 999_999_999), // one nanosecond before epoch
            ndt(1970, 1, 1, 0, 0, 0, 0), // the epoch → key 0
            ndt(2024, 1, 1, 0, 0, 0, 1_000), // micros = 1, nanos 1000
            ndt(2024, 1, 1, 0, 0, 0, 1_001), // sub-µs successor
            ndt(2024, 6, 15, 10, 30, 0, 0),
            ndt(2262, 6, 1, 0, 0, 0, 0), // just outside the i64-nanos ceiling
            ndt(3000, 6, 15, 12, 0, 0, 123_456_789),
        ];
        assert_eq!(datetime_to_orderable_i128(ndt(1970, 1, 1, 0, 0, 0, 0)), 0);
        assert!(
            datetime_to_orderable_i128(ascending[0]) < 0,
            "pre-epoch is negative"
        );
        for w in ascending.windows(2) {
            assert!(
                datetime_to_orderable_i128(w[0]) < datetime_to_orderable_i128(w[1]),
                "key order must match chronological order: {} then {}",
                w[0],
                w[1]
            );
        }
    }

    /// The single differential guard: over an adversarial datetime corpus, the
    /// four order-bearing datetime encoders — the IEJoin i128 axis
    /// (`value_to_i128`), the memcomparable sort-key bytes (`encode_sort_key`),
    /// the sort-merge comparator (`cmp_range_keys`), and the reducer
    /// (`datetime_to_orderable_i128`) — MUST induce the identical total order as
    /// the Sort node's `compare_values` (the one value order). Before unification the microsecond
    /// encoders disagreed with the nanosecond `compare_values` on sub-µs ties;
    /// this pins that they no longer can.
    #[test]
    fn datetime_encoders_induce_one_total_order() {
        use crate::pipeline::iejoin::value_to_i128;
        use crate::pipeline::sort::compare_values;
        use crate::pipeline::sort_merge_join::cmp_range_keys;
        use clinker_plan::plan::combine::RangeKeyType;

        // Adversarial corpus: sub-microsecond near-ties (differ only in nanos),
        // whole-second pre-1677 and post-2262 dates, the epoch and its immediate
        // neighbours, and ordinary timestamps.
        let corpus = [
            ndt(1400, 7, 4, 12, 0, 0, 0),
            ndt(1400, 7, 4, 12, 0, 0, 1),
            ndt(1676, 12, 31, 23, 59, 59, 999_999_999),
            ndt(1969, 12, 31, 23, 59, 59, 999_999_000),
            ndt(1969, 12, 31, 23, 59, 59, 999_999_999),
            ndt(1970, 1, 1, 0, 0, 0, 0),
            ndt(1970, 1, 1, 0, 0, 0, 1),
            ndt(2024, 1, 1, 0, 0, 0, 1_000),
            ndt(2024, 1, 1, 0, 0, 0, 1_001),
            ndt(2024, 1, 1, 0, 0, 0, 1_999),
            ndt(2024, 1, 1, 0, 0, 0, 2_000),
            ndt(2024, 6, 15, 10, 30, 0, 0),
            ndt(2262, 4, 11, 23, 47, 16, 854_775_807),
            ndt(3000, 6, 15, 12, 0, 0, 123_456_789),
        ];

        let sort_field = [sf("ts", SortOrder::Asc)];
        let sort_key = |dt: NaiveDateTime| -> Vec<u8> {
            encode_sort_key(&make_record(&[("ts", Value::DateTime(dt))]), &sort_field)
        };
        let axis_key = |dt: NaiveDateTime| -> i128 {
            value_to_i128(RangeKeyType::DateTime, &Value::DateTime(dt))
                .ok()
                .flatten()
                .expect("datetime always reduces to an axis key")
        };

        for &a in &corpus {
            for &b in &corpus {
                let va = Value::DateTime(a);
                let vb = Value::DateTime(b);
                let oracle = compare_values(&va, &vb);
                assert_eq!(
                    axis_key(a).cmp(&axis_key(b)),
                    oracle,
                    "IEJoin axis order disagrees with compare_values for {a} vs {b}"
                );
                assert_eq!(
                    sort_key(a).cmp(&sort_key(b)),
                    oracle,
                    "memcomparable sort-key order disagrees with compare_values for {a} vs {b}"
                );
                assert_eq!(
                    cmp_range_keys(&va, &vb),
                    oracle,
                    "sort-merge comparator disagrees with compare_values for {a} vs {b}"
                );
                assert_eq!(
                    datetime_to_orderable_i128(a).cmp(&datetime_to_orderable_i128(b)),
                    oracle,
                    "reducer order disagrees with compare_values for {a} vs {b}"
                );
            }
        }
    }

    proptest! {
        /// The same one-total-order invariant under randomized datetimes,
        /// including seconds outside the `i64`-nanosecond window. Independent
        /// second draws essentially never collide, so half the cases pin `b` to
        /// `a`'s second with an independent nanosecond — that is what actually
        /// stresses the sub-microsecond tie ordering the key exists for (and hits
        /// exact ties when the nanos also match); the other half draws `b`'s
        /// second freely to cover cross-era monotonicity. No random pair may make
        /// any encoder disagree with `compare_values`.
        #[test]
        fn prop_datetime_encoders_agree_with_compare_values(
            // Bounded to `chrono::DateTime::from_timestamp`'s valid second range
            // (± ~8.3e12 s ≈ year ±262 000). Still ~870× past the i64-nanosecond
            // window (± ~9.2e9 s), so it richly covers the out-of-`i64`-nanos era.
            a_s in -8_000_000_000_000i64..=8_000_000_000_000i64,
            a_n in 0u32..1_000_000_000,
            share_second in proptest::bool::ANY,
            b_s in -8_000_000_000_000i64..=8_000_000_000_000i64,
            b_n in 0u32..1_000_000_000,
        ) {
            use crate::pipeline::iejoin::value_to_i128;
            use crate::pipeline::sort::compare_values;
            use crate::pipeline::sort_merge_join::cmp_range_keys;
            use clinker_plan::plan::combine::RangeKeyType;

            let a = chrono::DateTime::from_timestamp(a_s, a_n).unwrap().naive_utc();
            let b_sec = if share_second { a_s } else { b_s };
            let b = chrono::DateTime::from_timestamp(b_sec, b_n).unwrap().naive_utc();
            let (va, vb) = (Value::DateTime(a), Value::DateTime(b));
            let oracle = compare_values(&va, &vb);

            let sort_field = [sf("ts", SortOrder::Asc)];
            let ka = encode_sort_key(&make_record(&[("ts", va.clone())]), &sort_field);
            let kb = encode_sort_key(&make_record(&[("ts", vb.clone())]), &sort_field);
            prop_assert_eq!(ka.cmp(&kb), oracle);

            let axis = |v: &Value| {
                value_to_i128(RangeKeyType::DateTime, v)
                    .ok()
                    .flatten()
                    .unwrap()
            };
            prop_assert_eq!(axis(&va).cmp(&axis(&vb)), oracle);
            prop_assert_eq!(cmp_range_keys(&va, &vb), oracle);
            prop_assert_eq!(
                datetime_to_orderable_i128(a).cmp(&datetime_to_orderable_i128(b)),
                oracle
            );
        }
    }

    fn make_record(fields: &[(&str, Value)]) -> Record {
        let schema = clinker_record::owned_storage::SharedStorage::from_arc(Arc::new(Schema::new(
            fields.iter().map(|(k, _)| (*k).into()).collect(),
        )));
        let values = fields.iter().map(|(_, v)| v.clone()).collect();
        Record::new(schema, values)
    }

    fn sf(field: &str, order: SortOrder) -> SortField {
        SortField {
            field: field.into(),
            order,
            null_order: None,
        }
    }

    fn sf_nulls(field: &str, order: SortOrder, nulls: NullOrder) -> SortField {
        SortField {
            field: field.into(),
            order,
            null_order: Some(nulls),
        }
    }

    #[test]
    fn test_encode_sort_key_integer_ordering() {
        let r1 = make_record(&[("x", Value::Integer(-100))]);
        let r2 = make_record(&[("x", Value::Integer(0))]);
        let r3 = make_record(&[("x", Value::Integer(100))]);
        let keys = &[sf("x", SortOrder::Asc)];
        assert!(encode_sort_key(&r1, keys) < encode_sort_key(&r2, keys));
        assert!(encode_sort_key(&r2, keys) < encode_sort_key(&r3, keys));
    }

    #[test]
    fn test_encode_sort_key_float_ordering() {
        let r1 = make_record(&[("x", Value::Float(-1.5))]);
        let r2 = make_record(&[("x", Value::Float(0.0))]);
        let r3 = make_record(&[("x", Value::Float(1.5))]);
        let r4 = make_record(&[("x", Value::Float(f64::MAX))]);
        let keys = &[sf("x", SortOrder::Asc)];
        assert!(encode_sort_key(&r1, keys) < encode_sort_key(&r2, keys));
        assert!(encode_sort_key(&r2, keys) < encode_sort_key(&r3, keys));
        assert!(encode_sort_key(&r3, keys) < encode_sort_key(&r4, keys));
    }

    #[test]
    fn test_encode_sort_key_string_ordering() {
        let r1 = make_record(&[("x", Value::String("abc".into()))]);
        let r2 = make_record(&[("x", Value::String("abd".into()))]);
        let r3 = make_record(&[("x", Value::String("b".into()))]);
        let keys = &[sf("x", SortOrder::Asc)];
        assert!(encode_sort_key(&r1, keys) < encode_sort_key(&r2, keys));
        assert!(encode_sort_key(&r2, keys) < encode_sort_key(&r3, keys));
    }

    #[test]
    fn test_encode_sort_key_date_ordering() {
        let d1 = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let d2 = NaiveDate::from_ymd_opt(2025, 6, 15).unwrap();
        let d3 = NaiveDate::from_ymd_opt(2026, 12, 31).unwrap();
        let r1 = make_record(&[("x", Value::Date(d1))]);
        let r2 = make_record(&[("x", Value::Date(d2))]);
        let r3 = make_record(&[("x", Value::Date(d3))]);
        let keys = &[sf("x", SortOrder::Asc)];
        assert!(encode_sort_key(&r1, keys) < encode_sort_key(&r2, keys));
        assert!(encode_sort_key(&r2, keys) < encode_sort_key(&r3, keys));
    }

    #[test]
    fn test_encode_sort_key_datetime_ordering() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        let dt1 = d.and_hms_opt(10, 0, 0).unwrap();
        let dt2 = d.and_hms_opt(10, 30, 0).unwrap();
        let dt3 = d.and_hms_opt(23, 59, 59).unwrap();
        let r1 = make_record(&[("x", Value::DateTime(dt1))]);
        let r2 = make_record(&[("x", Value::DateTime(dt2))]);
        let r3 = make_record(&[("x", Value::DateTime(dt3))]);
        let keys = &[sf("x", SortOrder::Asc)];
        assert!(encode_sort_key(&r1, keys) < encode_sort_key(&r2, keys));
        assert!(encode_sort_key(&r2, keys) < encode_sort_key(&r3, keys));
    }

    #[test]
    fn test_encode_sort_key_null_first() {
        let r_null = make_record(&[("x", Value::Null)]);
        let r_val = make_record(&[("x", Value::Integer(1))]);
        let keys = &[sf_nulls("x", SortOrder::Asc, NullOrder::First)];
        assert!(encode_sort_key(&r_null, keys) < encode_sort_key(&r_val, keys));
    }

    #[test]
    fn test_encode_sort_key_null_last() {
        let r_null = make_record(&[("x", Value::Null)]);
        let r_val = make_record(&[("x", Value::Integer(1))]);
        let keys = &[sf_nulls("x", SortOrder::Asc, NullOrder::Last)];
        assert!(encode_sort_key(&r_null, keys) > encode_sort_key(&r_val, keys));
    }

    #[test]
    fn test_encode_sort_key_desc_inverts() {
        let r1 = make_record(&[("x", Value::Integer(1))]);
        let r2 = make_record(&[("x", Value::Integer(2))]);
        let asc = &[sf("x", SortOrder::Asc)];
        let desc = &[sf("x", SortOrder::Desc)];
        // Ascending: 1 < 2
        assert!(encode_sort_key(&r1, asc) < encode_sort_key(&r2, asc));
        // Descending: 1 > 2 (inverted)
        assert!(encode_sort_key(&r1, desc) > encode_sort_key(&r2, desc));
    }

    #[test]
    fn test_encode_sort_key_compound() {
        // Sort by (dept ASC, salary DESC)
        let keys = &[sf("dept", SortOrder::Asc), sf("salary", SortOrder::Desc)];
        let r_a100 = make_record(&[
            ("dept", Value::String("A".into())),
            ("salary", Value::Integer(100)),
        ]);
        let r_a50 = make_record(&[
            ("dept", Value::String("A".into())),
            ("salary", Value::Integer(50)),
        ]);
        let r_b200 = make_record(&[
            ("dept", Value::String("B".into())),
            ("salary", Value::Integer(200)),
        ]);
        // A/100 < A/50 (same dept, salary DESC: 100 > 50 so 100 comes first)
        assert!(encode_sort_key(&r_a100, keys) < encode_sort_key(&r_a50, keys));
        // A/* < B/* (dept ASC)
        assert!(encode_sort_key(&r_a50, keys) < encode_sort_key(&r_b200, keys));
    }

    #[test]
    fn test_encode_sort_key_cross_type_numeric() {
        // Integers, floats and decimals share one numeric key: equal values
        // give identical bytes whatever their type, and unequal values order
        // by exact value, never through an `f64` widening.
        let key = |v: Value| encode_sort_key(&make_record(&[("x", v)]), &[sf("x", SortOrder::Asc)]);
        let integer = key(Value::Integer(42));
        assert_eq!(integer, key(Value::Integer(42)));
        assert_eq!(integer, key(Value::Float(42.0)));
        assert_eq!(integer, key(Value::Decimal(Decimal::new(42, 0))));
        assert_eq!(integer, key(Value::Decimal(Decimal::new(4200, 2))));

        assert!(key(Value::Float(41.5)) < integer);
        assert!(integer < key(Value::Decimal(Decimal::new(42_000_000_000_000_001, 15))));
        // 2^53 + 1 is not an f64; widening it would tie it with the float 2^53.
        let above = key(Value::Integer((1 << 53) + 1));
        assert!(key(Value::Float(9_007_199_254_740_992.0)) < above);
        assert!(above < key(Value::Float(9_007_199_254_740_994.0)));
        // The float 0.1 is slightly above the decimal 0.1.
        assert!(key(Value::Decimal(Decimal::new(1, 1))) < key(Value::Float(0.1)));
    }

    #[test]
    fn test_encode_sort_key_empty_string() {
        let r_null = make_record(&[("x", Value::Null)]);
        let r_empty = make_record(&[("x", Value::String("".into()))]);
        let keys = &[sf_nulls("x", SortOrder::Asc, NullOrder::First)];
        // null < "" (null sentinel 0x00 < non-null sentinel 0x01)
        assert!(encode_sort_key(&r_null, keys) < encode_sort_key(&r_empty, keys));
    }

    // ---- SortKeyEncoder ----

    #[test]
    fn test_sort_key_encoder_new_stores_fields() {
        let fields = vec![sf("a", SortOrder::Asc), sf("b", SortOrder::Desc)];
        let enc = SortKeyEncoder::new(fields.clone());
        assert_eq!(enc.sort_fields().len(), 2);
        assert_eq!(enc.sort_fields()[0].field, "a");
        assert_eq!(enc.sort_fields()[1].order, SortOrder::Desc);
    }

    #[test]
    fn test_sort_key_encoder_encode_into_matches_free_fn() {
        let enc = SortKeyEncoder::new(vec![
            sf("dept", SortOrder::Asc),
            sf("salary", SortOrder::Desc),
        ]);
        let rec = make_record(&[
            ("dept", Value::String("eng".into())),
            ("salary", Value::Integer(100)),
        ]);
        let mut scratch = Vec::new();
        enc.encode_into(&rec, &mut scratch);
        let expected = encode_sort_key(&rec, enc.sort_fields());
        assert_eq!(scratch, expected);
    }

    #[test]
    fn test_sort_key_encoder_encode_into_reuses_buffer() {
        // Verify clear-and-reuse semantics: a second encode into the
        // same buffer must leave it equal to a fresh encode, and the
        // allocation capacity must not shrink between calls.
        let enc = SortKeyEncoder::new(vec![sf("x", SortOrder::Asc)]);
        let r1 = make_record(&[("x", Value::Integer(1))]);
        let r2 = make_record(&[("x", Value::Integer(2))]);
        let mut scratch = Vec::with_capacity(64);
        let initial_cap = scratch.capacity();

        enc.encode_into(&r1, &mut scratch);
        let k1 = scratch.clone();

        enc.encode_into(&r2, &mut scratch);
        let k2_fresh = encode_sort_key(&r2, enc.sort_fields());
        assert_eq!(scratch, k2_fresh);
        assert_ne!(scratch, k1);
        assert!(
            scratch.capacity() >= initial_cap,
            "encode_into must reuse capacity: before={initial_cap}, after={}",
            scratch.capacity()
        );
    }

    #[test]
    fn test_sort_key_encoder_compare_encoded_asc() {
        let enc = SortKeyEncoder::new(vec![sf("x", SortOrder::Asc)]);
        let r1 = make_record(&[("x", Value::Integer(1))]);
        let r2 = make_record(&[("x", Value::Integer(2))]);
        let mut a = Vec::new();
        let mut b = Vec::new();
        enc.encode_into(&r1, &mut a);
        enc.encode_into(&r2, &mut b);
        assert_eq!(enc.compare_encoded(&a, &b), Ordering::Less);
        assert_eq!(enc.compare_encoded(&b, &a), Ordering::Greater);
        assert_eq!(enc.compare_encoded(&a, &a), Ordering::Equal);
    }

    #[test]
    fn test_sort_key_encoder_compare_encoded_desc_inverts() {
        // DESC direction is baked into the bytes by encode_into, so
        // compare_encoded returns the declared-order result directly.
        let enc = SortKeyEncoder::new(vec![sf("x", SortOrder::Desc)]);
        let r1 = make_record(&[("x", Value::Integer(1))]);
        let r2 = make_record(&[("x", Value::Integer(2))]);
        let mut a = Vec::new();
        let mut b = Vec::new();
        enc.encode_into(&r1, &mut a);
        enc.encode_into(&r2, &mut b);
        // Under DESC, 2 sorts before 1 — so the encoded bytes for r2
        // must compare as Less than those for r1.
        assert_eq!(enc.compare_encoded(&b, &a), Ordering::Less);
    }

    #[test]
    fn test_sort_key_encoder_debug_decode_pair_mentions_fields_and_hex() {
        let enc = SortKeyEncoder::new(vec![
            sf_nulls("k", SortOrder::Asc, NullOrder::First),
            sf("v", SortOrder::Desc),
        ]);
        let r1 = make_record(&[("k", Value::String("a".into())), ("v", Value::Integer(1))]);
        let r2 = make_record(&[("k", Value::String("b".into())), ("v", Value::Integer(2))]);
        let mut a = Vec::new();
        let mut b = Vec::new();
        enc.encode_into(&r1, &mut a);
        enc.encode_into(&r2, &mut b);
        let rendered = enc.debug_decode_pair(&a, &b);
        assert!(
            rendered.contains("k ASC NULLS FIRST"),
            "field list missing: {rendered}"
        );
        assert!(
            rendered.contains("v DESC NULLS LAST"),
            "field list missing: {rendered}"
        );
        assert!(rendered.contains("prev=0x"), "prev hex missing: {rendered}");
        assert!(rendered.contains("next=0x"), "next hex missing: {rendered}");
    }

    #[test]
    fn test_encode_sort_key_roundtrip_deterministic() {
        let r = make_record(&[
            ("a", Value::String("hello".into())),
            ("b", Value::Integer(42)),
        ]);
        let keys = &[sf("a", SortOrder::Asc), sf("b", SortOrder::Desc)];
        let k1 = encode_sort_key(&r, keys);
        let k2 = encode_sort_key(&r, keys);
        assert_eq!(k1, k2);
    }
}
