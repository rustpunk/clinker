//! Memcomparable sort key encoding.
//!
//! Encodes a record's sort fields as a byte sequence where lexicographic
//! comparison (`memcmp`) equals semantic sort ordering. Used by the loser
//! tree during external merge sort — the `MergeEntry` implements `Ord`
//! via byte comparison on the encoded key.
//!
//! Encoding rules per value type:
//! - Each field segment: `[null_sentinel: 1 byte] [encoded_value: N bytes]`
//! - Null: sentinel only (0x00 for nulls-first, 0x02 for nulls-last)
//! - Non-null sentinel: 0x01
//! - Integer: sign-flipped big-endian i64 (8 bytes)
//! - Float: IEEE-to-signed reinterpretation, big-endian (8 bytes)
//! - String: UTF-8 bytes + 0x00 terminator
//! - Bool: 0x00 (false) or 0x01 (true)
//! - Date: sign-flipped big-endian i32 days since Unix epoch (4 bytes)
//! - DateTime: sign-flipped big-endian i128 nanoseconds since Unix epoch (16 bytes)
//! - Descending: XOR all segment bytes with 0xFF

use std::cmp::Ordering;

use chrono::{NaiveDate, NaiveDateTime};
use clinker_record::{Record, Value};

use clinker_plan::config::{NullOrder, SortField, SortOrder};

/// Encode a record's sort fields as a memcomparable byte sequence.
pub fn encode_sort_key(record: &Record, sort_by: &[SortField]) -> Vec<u8> {
    let mut key = Vec::with_capacity(sort_by.len() * 10);
    for sf in sort_by {
        let start = key.len();
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
                encode_value(value, &mut key);
            }
        }
        if sf.order == SortOrder::Desc {
            for byte in &mut key[start..] {
                *byte ^= 0xFF;
            }
        }
    }
    key
}

fn encode_value(value: &Value, buf: &mut Vec<u8>) {
    match value {
        Value::Bool(b) => buf.push(if *b { 0x01 } else { 0x00 }),
        Value::Integer(n) => {
            let mut bytes = n.to_be_bytes();
            bytes[0] ^= 0x80; // sign-flip
            buf.extend_from_slice(&bytes);
        }
        Value::Float(f) => {
            debug_assert!(
                !f.is_nan(),
                "NaN should be DLQ'd before reaching sort encoder"
            );
            encode_f64_order(*f, buf);
        }
        Value::Decimal(d) => encode_decimal_order(*d, buf),
        Value::String(s) => {
            buf.extend_from_slice(s.as_bytes());
            buf.push(0x00); // null terminator
        }
        Value::Date(d) => {
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = d.signed_duration_since(epoch).num_days() as i32;
            let mut bytes = days.to_be_bytes();
            bytes[0] ^= 0x80;
            buf.extend_from_slice(&bytes);
        }
        Value::DateTime(dt) => {
            // Canonical nanosecond key (16 bytes), sign-flipped so the signed
            // i128 orders as unsigned big-endian — the same reduction the
            // IEJoin axis and the sort-merge comparator use, so a datetime
            // sorts identically everywhere and sub-microsecond group keys stay
            // distinct through a spilled aggregation's byte-wise k-way merge.
            let mut bytes = datetime_to_orderable_i128(*dt).to_be_bytes();
            bytes[0] ^= 0x80;
            buf.extend_from_slice(&bytes);
        }
        Value::Null => {}                     // handled by caller
        Value::Array(_) | Value::Map(_) => {} // not a valid sort key; defensive no-op
    }
}

/// Append the EXACT, value-canonical, order-preserving memcomparable encoding
/// of a `Decimal` (a fixed 33 bytes).
///
/// The decimal sort key is load-bearing for GROUP IDENTITY, not just ordering:
/// the spilled-aggregation path ([`crate::aggregation::spill`]) compares group
/// keys by these bytes, so the encoding must be **value-canonical** — equal
/// values (`2.50` and `2.5`) must produce byte-identical keys, and distinct
/// values must not collide — as well as order-preserving. A lossy `f64`
/// projection is monotone but NOT injective, so it would silently merge two
/// distinct high-precision decimal groups once aggregation spills; this exact
/// encoding avoids that divergence from the in-memory `GroupByKey::Decimal`
/// path.
///
/// Encoding: the value scaled to a fixed 10^28 grid is the integer
/// `mantissa × 10^(28 − scale)` (identical for `2.50` and `2.5`), which fits
/// in 256 bits over `rust_decimal`'s range. It is emitted as a sign marker
/// (`0x00` negative sorts before `0x01` non-negative) followed by the 32-byte
/// big-endian magnitude — bit-inverted for negatives so a larger magnitude
/// sorts earlier. `-0` normalizes to `0`, so there is no signed-zero ambiguity.
fn encode_decimal_order(d: rust_decimal::Decimal, buf: &mut Vec<u8>) {
    const FIXED_SCALE: u32 = 28; // rust_decimal's maximum scale
    let negative = d.is_sign_negative() && !d.is_zero();
    let magnitude = d.mantissa().unsigned_abs();
    // scale ≤ 28, so 28 − scale ≥ 0 and 10^(28−scale) ≤ 10^28 < u128::MAX.
    let pow = 10u128.pow(FIXED_SCALE - d.scale());
    let scaled = mul_u128_to_u256_be(magnitude, pow);
    if negative {
        buf.push(0x00);
        buf.extend(scaled.iter().map(|b| !b));
    } else {
        buf.push(0x01);
        buf.extend_from_slice(&scaled);
    }
}

/// Full 256-bit product of two `u128`s as a 32-byte big-endian array, via
/// schoolbook multiplication over four 64-bit limbs. Used to place a decimal on
/// a fixed 10^28 grid without a bignum dependency; the operands here are bounded
/// (`magnitude < 2^96`, `pow ≤ 10^28 < 2^94`) so the product never exceeds 2^190.
fn mul_u128_to_u256_be(a: u128, b: u128) -> [u8; 32] {
    const MASK: u128 = u64::MAX as u128;
    let (a_lo, a_hi) = (a & MASK, a >> 64);
    let (b_lo, b_hi) = (b & MASK, b >> 64);

    let ll = a_lo * b_lo;
    let lh = a_lo * b_hi;
    let hl = a_hi * b_lo;
    let hh = a_hi * b_hi;

    let r0 = ll & MASK;
    let mid = (ll >> 64) + (lh & MASK) + (hl & MASK);
    let r1 = mid & MASK;
    let hi = (mid >> 64) + (lh >> 64) + (hl >> 64) + (hh & MASK);
    let r2 = hi & MASK;
    let r3 = (hi >> 64) + (hh >> 64);

    let mut out = [0u8; 32];
    out[0..8].copy_from_slice(&(r3 as u64).to_be_bytes());
    out[8..16].copy_from_slice(&(r2 as u64).to_be_bytes());
    out[16..24].copy_from_slice(&(r1 as u64).to_be_bytes());
    out[24..32].copy_from_slice(&(r0 as u64).to_be_bytes());
    out
}

/// Order-preserving `f64` → `u64`: the *unsigned* ordering of the result
/// matches IEEE-754 ordering over finite values. `-0.0` canonicalizes to
/// `+0.0` so signed zeros compare equal, agreeing with `value_to_group_key`.
///
/// This is the single source of the float bit-twiddle. The memcomparable
/// byte key ([`encode_f64_order`]) and the range-join kernels' signed
/// [`float_to_orderable_i64`] both derive from it, so a monotone float
/// order is guaranteed identically everywhere it matters.
#[inline]
fn f64_to_orderable_u64(f: f64) -> u64 {
    let f = if f == 0.0 { 0.0 } else { f };
    let bits = f.to_bits();
    if bits >> 63 == 1 {
        bits ^ u64::MAX // negative: flip all bits — larger magnitude sorts earlier
    } else {
        bits ^ (1 << 63) // positive/zero: flip sign bit only
    }
}

/// Order-preserving `f64` → `i64`: signed-`i64` comparison of the result
/// matches IEEE-754 ordering over finite values. `-0.0` encodes equal to
/// `+0.0`.
///
/// The range-join kernels carry keys as `i64` and compare them directly,
/// so they need a monotone `i64` rather than the memcomparable byte key.
/// A raw `f.to_bits() as i64` is NOT monotone: IEEE negatives set the sign
/// bit, so their bit patterns grow as the value shrinks, and any range
/// predicate over a float column with negative values would match wrongly.
#[inline]
pub(crate) fn float_to_orderable_i64(f: f64) -> i64 {
    // Flip the high bit to turn the unsigned-ordered u64 into a
    // two's-complement-ordered i64 (smallest u64 → i64::MIN).
    (f64_to_orderable_u64(f) ^ (1 << 63)) as i64
}

/// Order-preserving `f64` → `i128`: the sign-extended widening of
/// [`float_to_orderable_i64`].
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

/// Canonical scale of the inequality-join decimal range axis: every decimal on
/// a decimal axis is placed on a common `10^18` fixed-point grid. 18 fractional
/// digits are preserved exactly, and the integer magnitude reaches
/// `i128::MAX / 10^18 ≈ 1.7e20` — far beyond any realistic monetary value.
/// Chosen so a full `i64` operand widened onto the same grid always fits
/// (`i64::MAX · 10^18 < i128::MAX`), keeping a mixed decimal/integer axis exact.
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

/// Order-preserving, injective `NaiveDateTime` → `i128`: the UTC instant as a
/// nanosecond count since the Unix epoch, assembled as
/// `timestamp_seconds · 10^9 + subsecond_nanos`.
///
/// This is the SINGLE canonical datetime range / sort / join key. `Value::DateTime`
/// (a `chrono::NaiveDateTime`), [`compare_values`](crate::pipeline::sort::compare_values),
/// and the windowing path all carry nanosecond resolution, so every
/// order-bearing datetime encoder — the inequality-join i128 axis, the
/// memcomparable sort key, and the sort-merge range comparator — reduces
/// through THIS function to induce one identical total order. The microsecond
/// key it supersedes was monotone but NOT injective, so it both dropped
/// sub-microsecond join matches (a lossy sole-arbiter key enumerates the wrong
/// candidate set) and, because the spilled-aggregation k-way merge decides
/// group identity by sort-key bytes, silently merged two distinct
/// sub-microsecond groups once aggregation spilled.
///
/// Exact and injective for every non-leap-second `NaiveDateTime`, so no
/// fail-loud arm is needed: chrono caps the year at ±262 143, bounding
/// `|timestamp| < 8.3e12` seconds, so the nanosecond result stays under
/// `8.3e12 · 10^9 ≈ 8.3e21` in magnitude — more than sixteen orders below
/// `i128::MAX ≈ 1.7e38`, so the multiply cannot overflow. Assembling the value
/// directly (rather than via `timestamp_nanos_opt`, whose `i64` result
/// saturates outside 1677–2262) keeps pre-1677 and post-2262 datetimes exact.
/// `timestamp()` floors toward negative infinity and `timestamp_subsec_nanos()`
/// is the non-negative offset within that floored second, so the sum is the
/// correct signed nanosecond count for pre-epoch instants too. Pure arithmetic
/// on an owned value: no allocation, no I/O, streaming-safe.
///
/// Leap seconds are the lone exception: chrono stores them as second `:59` with
/// a sub-second field in `[10^9, 2·10^9)`, which Unix `timestamp()` does not
/// count, so this key places a leap instant onto the following second. That
/// matches Unix-time convention and the `i128`-nanosecond `Value::DateTime` spill
/// serialization (which collapses leap seconds identically), but not
/// `NaiveDateTime::cmp`. All three encoders reduce through this function, so they
/// still agree with EACH OTHER on leap seconds — cross-strategy join/sort/group
/// results stay identical; only the axis-vs-`compare_values` order can differ at
/// a leap instant, which no linear nanosecond key can represent distinctly.
#[inline]
pub(crate) fn datetime_to_orderable_i128(dt: NaiveDateTime) -> i128 {
    let utc = dt.and_utc();
    (utc.timestamp() as i128) * 1_000_000_000 + utc.timestamp_subsec_nanos() as i128
}

/// Append the order-preserving 8-byte memcomparable encoding of an `f64`.
fn encode_f64_order(f: f64, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&f64_to_orderable_u64(f).to_be_bytes());
}

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
        out.clear();
        for sf in &self.sort_by {
            let start = out.len();
            let null_order = sf.null_order.unwrap_or(NullOrder::Last);
            match record.get(&sf.field) {
                None | Some(Value::Null) => {
                    out.push(match null_order {
                        NullOrder::First => 0x00,
                        NullOrder::Last | NullOrder::Drop => 0x02,
                    });
                }
                Some(value) => {
                    out.push(0x01);
                    encode_value(value, out);
                }
            }
            if sf.order == SortOrder::Desc {
                for byte in &mut out[start..] {
                    *byte ^= 0xFF;
                }
            }
        }
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
    use chrono::NaiveDate;
    use clinker_record::Schema;
    use proptest::prelude::*;
    use rust_decimal::Decimal;
    use std::sync::Arc;

    fn dec_key(d: Decimal) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_value(&Value::Decimal(d), &mut buf);
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
    /// the evaluator's own `compare_values`. Before unification the microsecond
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
        let schema = Arc::new(Schema::new(
            fields.iter().map(|(k, _)| (*k).into()).collect(),
        ));
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
        // Integer and Float should be comparable via f64 widening
        // However, memcomparable encoding uses type-specific encoding,
        // so cross-type comparison is not guaranteed to be correct.
        // The in-memory comparator handles cross-type; the encoder
        // produces type-specific bytes. This test verifies determinism.
        let r1 = make_record(&[("x", Value::Integer(42))]);
        let r2 = make_record(&[("x", Value::Integer(42))]);
        let keys = &[sf("x", SortOrder::Asc)];
        assert_eq!(encode_sort_key(&r1, keys), encode_sort_key(&r2, keys));
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
