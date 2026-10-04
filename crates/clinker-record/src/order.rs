//! The one value order.
//!
//! Every comparison that sorts, groups, merges sorted runs or checks a declared
//! order belongs on this module, so an in-memory path and a spilled path cannot
//! disagree about which of two values comes first or whether two values fall in
//! the same group. [`compare`] is a total order over every [`Value`]; [`encode`]
//! writes a memcomparable key whose unsigned byte order is exactly [`compare`];
//! [`ties`] is the comparator's equality and [`hash_tie_class`] a hash that
//! agrees with it. Group keys carry their own equality, hash and tie bytes on
//! `GroupByKey`, which a property test proves agree with [`ties`].
//!
//! The order:
//!
//! - Values of different domains order by a fixed rank:
//!   null < bool < number < string < date < datetime < array < map.
//! - Integers, floats and decimals are one numeric domain compared by exact
//!   value, never through an `f64` widening: `9007199254740993` sorts after the
//!   float `9007199254740992.0`; `1`, `1.0` and the decimal `1` tie; the decimal
//!   `0.1` sorts before the float `0.1`, whose exact binary value is larger.
//! - `-0.0` ties `+0.0`. Every NaN, whatever its sign or payload, ties every
//!   other NaN and sorts above `+inf`, so a NaN is a placeable value instead of
//!   a barrier that breaks transitivity.
//! - Strings by UTF-8 bytes (no collation); `false` before `true`; dates by
//!   day; datetimes by [`datetime_to_orderable_i128`].
//! - Arrays element by element, a shorter prefix first; maps by their entries
//!   sorted by key (key bytes, then value), a shorter prefix first.
//!
//! Null placement is not part of the value order. A sort field's authored
//! `null_order` places a top-level null, so callers handle `Value::Null` before
//! reaching this module; the null rank only decides where a null inside an
//! array sorts.
//!
//! This module defines ordering, not predicates. A comparison operator still
//! decides what `NaN < 1` or `null < 1` means; for comparable operands its
//! answer should be this order.
//!
//! Every function here is pure: no allocation except map ordering (which sorts
//! a borrowed entry list) and the caller's output buffer, no I/O, no retained
//! state.

use std::cmp::Ordering;
use std::hash::{DefaultHasher, Hash, Hasher};

use chrono::{Datelike, NaiveDate, NaiveDateTime};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

use crate::owned_storage::OwnedMap;
use crate::value::Value;

// Rank tags, in rank order. Every key starts with its value's tag, so values of
// different domains order by rank before any payload byte is compared.
const TAG_NULL: u8 = 0x01;
const TAG_BOOL: u8 = 0x02;
const TAG_NUMBER: u8 = 0x03;
const TAG_STRING: u8 = 0x04;
const TAG_DATE: u8 = 0x05;
const TAG_DATETIME: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_MAP: u8 = 0x08;

/// Framing of array elements and map entries: each is preceded by
/// `ELEMENT`, and the sequence ends with `END`, which sorts below it so a
/// shorter prefix sorts first.
const ELEMENT: u8 = 0x01;
const END: u8 = 0x00;

/// A number's key byte after its `f64` bracket: `EXACT` when the number is that
/// `f64`, else `INEXACT` followed by its exact decimal grid bytes. Every inexact
/// number in a bracket is above the bracket's `f64`, so `EXACT` sorts first.
const EXACT: u8 = 0x00;
const INEXACT: u8 = 0x01;

/// Resolution of the exact decimal grid a number's key falls back to when the
/// number is not exactly an `f64`: `rust_decimal`'s maximum scale, so every
/// decimal and every `i64` lands on the grid exactly.
///
/// A value `v` (a 96-bit signed `mantissa` at some `scale`) sits on the grid as
/// the integer `mantissa × 10^(28 − scale)`, which is order-preserving and
/// scale-invariant (`2.5` and `2.50` land on the same point). The magnitude
/// reaches about 2^190, so it is written as 256 bits, not an `i128`.
pub const DECIMAL_SORT_KEY_SCALE: u32 = 28;

/// `2^96`: every decimal's magnitude is below it (`rust_decimal`'s mantissa is
/// 96 bits), so a float at or above it is larger than every decimal.
const TWO_POW_96: f64 = f64::from_bits((1023 + 96) << 52);

/// `2^-94`: every nonzero decimal's magnitude is at least `10^-28`, which is
/// above it, so a nonzero float below it is smaller than every nonzero decimal.
const TWO_POW_MINUS_94: f64 = f64::from_bits((1023 - 94) << 52);

/// `2^63`: the first float above every `i64`; `-2^63` is `i64::MIN` exactly.
const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;

/// Days from 0001-01-01 (CE day 1) to 1970-01-01.
const UNIX_EPOCH_DAYS_FROM_CE: i32 = 719_163;

/// A number reduced to its domain, without its `Value` wrapper.
#[derive(Clone, Copy)]
enum Number {
    Int(i64),
    Float(f64),
    Dec(Decimal),
}

/// A value's domain with its payload borrowed, so [`compare`], [`encode`] and
/// [`hash_tie_class`] dispatch through one table of domains.
enum Domain<'a> {
    Null,
    Bool(bool),
    Number(Number),
    Str(&'a str),
    Date(NaiveDate),
    DateTime(NaiveDateTime),
    Array(&'a [Value]),
    Map(&'a OwnedMap),
}

impl Domain<'_> {
    fn of(v: &Value) -> Domain<'_> {
        match v {
            Value::Null => Domain::Null,
            Value::Bool(b) => Domain::Bool(*b),
            Value::Integer(i) => Domain::Number(Number::Int(*i)),
            Value::Float(f) => Domain::Number(Number::Float(*f)),
            Value::Decimal(d) => Domain::Number(Number::Dec(*d)),
            Value::String(s) => Domain::Str(s.as_str()),
            Value::Date(d) => Domain::Date(*d),
            Value::DateTime(dt) => Domain::DateTime(*dt),
            Value::Array(items) => Domain::Array(items.as_slice()),
            Value::Map(map) => Domain::Map(map),
        }
    }

    fn tag(&self) -> u8 {
        match self {
            Domain::Null => TAG_NULL,
            Domain::Bool(_) => TAG_BOOL,
            Domain::Number(_) => TAG_NUMBER,
            Domain::Str(_) => TAG_STRING,
            Domain::Date(_) => TAG_DATE,
            Domain::DateTime(_) => TAG_DATETIME,
            Domain::Array(_) => TAG_ARRAY,
            Domain::Map(_) => TAG_MAP,
        }
    }
}

/// Compare two values under the one value order.
///
/// Total over every [`Value`]: reflexive, antisymmetric and transitive, NaN and
/// mixed domains included, so a stable sort's output does not depend on where
/// run boundaries fall. Allocates only to order a map's entries by key.
///
/// Two integers, two floats or two strings are compared here directly, with
/// the same expression their domain's arm uses; every other pair, mixed
/// numeric domains included, takes the domain dispatch. Sort keys are
/// usually one type per field, and this keeps that case small enough to
/// inline into a sort's comparison loop.
#[inline]
pub fn compare(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Integer(x), Value::Integer(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => f64_orderable_bits(*x).cmp(&f64_orderable_bits(*y)),
        (Value::String(x), Value::String(y)) => x.as_str().as_bytes().cmp(y.as_str().as_bytes()),
        _ => compare_domains(a, b),
    }
}

/// [`compare`] through the domain table, for every pair of values.
fn compare_domains(a: &Value, b: &Value) -> Ordering {
    match (Domain::of(a), Domain::of(b)) {
        (Domain::Null, Domain::Null) => Ordering::Equal,
        (Domain::Bool(x), Domain::Bool(y)) => x.cmp(&y),
        (Domain::Number(x), Domain::Number(y)) => compare_numbers(x, y),
        (Domain::Str(x), Domain::Str(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Domain::Date(x), Domain::Date(y)) => x.cmp(&y),
        (Domain::DateTime(x), Domain::DateTime(y)) => {
            datetime_to_orderable_i128(x).cmp(&datetime_to_orderable_i128(y))
        }
        (Domain::Array(x), Domain::Array(y)) => compare_sequences(x, y),
        (Domain::Map(x), Domain::Map(y)) => compare_maps(x, y),
        (x, y) => x.tag().cmp(&y.tag()),
    }
}

/// Whether two values tie under [`compare`]. This is the equality group keys
/// must use, because a spilled or streamed grouping detects a group boundary
/// by a tie of the order.
pub fn ties(a: &Value, b: &Value) -> bool {
    compare(a, b) == Ordering::Equal
}

fn compare_numbers(a: Number, b: Number) -> Ordering {
    match (a, b) {
        (Number::Float(x), Number::Float(y)) => f64_orderable_bits(x).cmp(&f64_orderable_bits(y)),
        (Number::Float(x), _) if x.is_nan() => Ordering::Greater,
        (_, Number::Float(y)) if y.is_nan() => Ordering::Less,
        (Number::Int(x), Number::Int(y)) => x.cmp(&y),
        (Number::Dec(x), Number::Dec(y)) => x.cmp(&y),
        // Every i64 fits a decimal's 96-bit mantissa, so this is exact.
        (Number::Int(x), Number::Dec(y)) => Decimal::from(x).cmp(&y),
        (Number::Dec(x), Number::Int(y)) => x.cmp(&Decimal::from(y)),
        (Number::Int(x), Number::Float(y)) => compare_i64_to_f64(x, y),
        (Number::Float(x), Number::Int(y)) => compare_i64_to_f64(y, x).reverse(),
        (Number::Dec(x), Number::Float(y)) => compare_decimal_to_f64(x, y),
        (Number::Float(x), Number::Dec(y)) => compare_decimal_to_f64(y, x).reverse(),
    }
}

/// Exact `i64` against a non-NaN `f64`, without widening the integer.
fn compare_i64_to_f64(integer: i64, float: f64) -> Ordering {
    if float >= TWO_POW_63 {
        return Ordering::Less;
    }
    if float < -TWO_POW_63 {
        return Ordering::Greater;
    }
    // In range, the float's integer part is an exact i64; its fraction breaks
    // a tie on the integer part.
    match integer.cmp(&(float.trunc() as i64)) {
        Ordering::Equal => {
            let fraction = float.fract();
            if fraction > 0.0 {
                Ordering::Less
            } else if fraction < 0.0 {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        ordering => ordering,
    }
}

/// Exact decimal against a non-NaN `f64`.
fn compare_decimal_to_f64(decimal: Decimal, float: f64) -> Ordering {
    let decimal_sign = if decimal.is_zero() {
        0
    } else if decimal.is_sign_negative() {
        -1
    } else {
        1
    };
    let float_sign = if float == 0.0 {
        0
    } else if float < 0.0 {
        -1
    } else {
        1
    };
    if decimal_sign != float_sign {
        return decimal_sign.cmp(&float_sign);
    }
    if decimal_sign == 0 {
        return Ordering::Equal;
    }
    let magnitude = compare_decimal_magnitude_to_f64(
        decimal.mantissa().unsigned_abs(),
        decimal.scale(),
        float.abs(),
    );
    if decimal_sign < 0 {
        magnitude.reverse()
    } else {
        magnitude
    }
}

/// `mantissa / 10^scale` against a positive float (`+inf` included), both
/// nonzero, by cross-multiplying into 256-bit integers.
fn compare_decimal_magnitude_to_f64(mantissa: u128, scale: u32, float: f64) -> Ordering {
    if float >= TWO_POW_96 {
        return Ordering::Less;
    }
    if float < TWO_POW_MINUS_94 {
        return Ordering::Greater;
    }
    // Within [2^-94, 2^96) the float is normal: significand · 2^exponent with a
    // 53-bit significand and exponent in [-146, 43].
    let bits = float.to_bits();
    let significand = u128::from((bits & ((1u64 << 52) - 1)) | (1u64 << 52));
    let exponent = ((bits >> 52) & 0x7FF) as i32 - 1075;
    // mantissa / 10^scale  vs  significand · 2^exponent, with both sides
    // multiplied by 10^scale and by 2^-exponent when the exponent is negative.
    // The left side stays below 2^96 · 2^146 and the right below
    // 2^53 · 10^28 · 2^43, so neither leaves 256 bits.
    let left = U256::from_u128(mantissa).shl(exponent.min(0).unsigned_abs());
    let right = U256::mul(significand, 10u128.pow(scale)).shl(exponent.max(0).unsigned_abs());
    left.cmp(&right)
}

fn compare_sequences(a: &[Value], b: &[Value]) -> Ordering {
    for (x, y) in a.iter().zip(b) {
        match compare(x, y) {
            Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    a.len().cmp(&b.len())
}

fn compare_maps(a: &OwnedMap, b: &OwnedMap) -> Ordering {
    let (a, b) = (entries_by_key(a), entries_by_key(b));
    for ((ka, va), (kb, vb)) in a.iter().zip(&b) {
        match ka
            .as_bytes()
            .cmp(kb.as_bytes())
            .then_with(|| compare(va, vb))
        {
            Ordering::Equal => {}
            ordering => return ordering,
        }
    }
    a.len().cmp(&b.len())
}

/// A map's entries in key order. Keys are unique, so the order is total and
/// insertion order cannot affect a comparison or a key.
fn entries_by_key(map: &OwnedMap) -> Vec<(&str, &Value)> {
    let mut entries: Vec<(&str, &Value)> = map.iter().map(|(k, v)| (k.as_str(), v)).collect();
    entries.sort_unstable_by(|(a, _), (b, _)| a.as_bytes().cmp(b.as_bytes()));
    entries
}

/// Append the memcomparable key of `v` to `out`.
///
/// For any two values, `encode(a).cmp(encode(b)) == compare(a, b)`, the two
/// keys are byte-equal exactly when the values tie, and no key is a proper
/// prefix of another, so keys concatenate into compound keys and survive a
/// descending field's byte inversion. The key is:
///
/// - a rank tag byte, then the domain's payload;
/// - a number: the big-endian [`f64_orderable_bits`] of the largest `f64` not
///   above it, then `0x00` when the number is that `f64` exactly, else `0x01`
///   and the number's 33-byte position on the exact [`DECIMAL_SORT_KEY_SCALE`]
///   grid (a sign byte, then the 32-byte magnitude, inverted when negative).
///   Every float is exact, so only integers and decimals that are not exactly
///   an `f64` carry grid bytes; every NaN is one pattern above `+inf`;
/// - a string: its UTF-8 bytes with each NUL written `00 FF`, then `00 00`;
/// - a bool: `00` or `01`; a date: its days since 1970-01-01 as a sign-flipped
///   big-endian `i32`; a datetime: [`datetime_to_orderable_i128`] as a
///   sign-flipped big-endian `i128`;
/// - an array: `01` and the element's key per element, then `00`; a map: the
///   same over its entries in key order, each entry being the key string's
///   payload followed by the value's key. A null element is its tag alone.
///
/// Streams into `out` and holds nothing; a map allocates to order its entries.
pub fn encode(v: &Value, out: &mut Vec<u8>) {
    match Domain::of(v) {
        Domain::Null => out.push(TAG_NULL),
        Domain::Bool(b) => encode_bool(b, out),
        Domain::Number(n) => encode_number(n, out),
        Domain::Str(s) => encode_str(s, out),
        Domain::Date(d) => encode_date(d, out),
        Domain::DateTime(dt) => encode_datetime(dt, out),
        Domain::Array(items) => {
            out.push(TAG_ARRAY);
            for item in items {
                out.push(ELEMENT);
                encode(item, out);
            }
            out.push(END);
        }
        Domain::Map(map) => {
            out.push(TAG_MAP);
            for (k, v) in entries_by_key(map) {
                out.push(ELEMENT);
                push_escaped_str(k, out);
                encode(v, out);
            }
            out.push(END);
        }
    }
}

/// Append the key of an integer, byte-identical to [`encode`] of
/// `Value::Integer(v)`, without building a `Value`.
pub fn encode_i64(v: i64, out: &mut Vec<u8>) {
    encode_number(Number::Int(v), out);
}

/// Append the key of a float, byte-identical to [`encode`] of
/// `Value::Float(v)`. Every NaN writes the same bytes, as `-0.0` and `0.0` do.
pub fn encode_f64(v: f64, out: &mut Vec<u8>) {
    encode_number(Number::Float(v), out);
}

/// Append the key of a decimal, byte-identical to [`encode`] of
/// `Value::Decimal(v)`. Equal values at different scales write the same bytes.
pub fn encode_decimal(v: Decimal, out: &mut Vec<u8>) {
    encode_number(Number::Dec(v), out);
}

/// Append the key of a string, byte-identical to [`encode`] of
/// `Value::String` holding `v`.
pub fn encode_str(v: &str, out: &mut Vec<u8>) {
    out.push(TAG_STRING);
    push_escaped_str(v, out);
}

/// Append the key of a bool, byte-identical to [`encode`] of `Value::Bool(v)`.
pub fn encode_bool(v: bool, out: &mut Vec<u8>) {
    out.extend_from_slice(&[TAG_BOOL, u8::from(v)]);
}

/// Append the key of a date, byte-identical to [`encode`] of `Value::Date(v)`.
pub fn encode_date(v: NaiveDate, out: &mut Vec<u8>) {
    // chrono's years stay within ±262 143, so the day count fits an i32.
    let days = v.num_days_from_ce() - UNIX_EPOCH_DAYS_FROM_CE;
    let mut bytes = days.to_be_bytes();
    bytes[0] ^= 0x80;
    out.push(TAG_DATE);
    out.extend_from_slice(&bytes);
}

/// Append the key of a datetime, byte-identical to [`encode`] of
/// `Value::DateTime(v)`.
pub fn encode_datetime(v: NaiveDateTime, out: &mut Vec<u8>) {
    let mut bytes = datetime_to_orderable_i128(v).to_be_bytes();
    bytes[0] ^= 0x80;
    out.push(TAG_DATETIME);
    out.extend_from_slice(&bytes);
}

/// Zero-escape plus a two-byte terminator: prefix-free even when the string
/// holds a NUL, and ordered as the raw bytes are.
fn push_escaped_str(s: &str, out: &mut Vec<u8>) {
    for byte in s.bytes() {
        if byte == 0 {
            out.extend_from_slice(&[0x00, 0xFF]);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

fn encode_number(n: Number, out: &mut Vec<u8>) {
    out.push(TAG_NUMBER);
    match f64_bracket(n) {
        Bracket::Exact(f) => {
            out.extend_from_slice(&f64_orderable_bits(f).to_be_bytes());
            out.push(EXACT);
        }
        Bracket::Above(floor, exact) => {
            out.extend_from_slice(&f64_orderable_bits(floor).to_be_bytes());
            out.push(INEXACT);
            encode_decimal_grid(exact, out);
        }
    }
}

/// Where a number sits relative to the floats.
enum Bracket {
    /// The number is exactly this float (every float is; NaN included).
    Exact(f64),
    /// The number lies strictly between this float and the next one up, and
    /// equals this decimal exactly.
    Above(f64, Decimal),
}

fn f64_bracket(n: Number) -> Bracket {
    match n {
        Number::Float(f) => Bracket::Exact(f),
        Number::Int(i) => {
            // Round-to-nearest lands within half an ulp, so one step down at
            // most reaches the floor.
            let rounded = i as f64;
            match compare_i64_to_f64(i, rounded) {
                Ordering::Equal => Bracket::Exact(rounded),
                Ordering::Greater => Bracket::Above(rounded, Decimal::from(i)),
                Ordering::Less => Bracket::Above(rounded.next_down(), Decimal::from(i)),
            }
        }
        Number::Dec(d) => {
            // The approximation is within a few ulps; the exact comparison then
            // walks it onto the largest float not above `d`. Decimals lie far
            // inside the finite floats, so neither walk reaches an infinity.
            let mut floor = d.mantissa() as f64 / 10f64.powi(d.scale() as i32);
            while compare_decimal_to_f64(d, floor) == Ordering::Less {
                floor = floor.next_down();
            }
            while compare_decimal_to_f64(d, floor.next_up()) != Ordering::Less {
                floor = floor.next_up();
            }
            if compare_decimal_to_f64(d, floor) == Ordering::Equal {
                Bracket::Exact(floor)
            } else {
                Bracket::Above(floor, d)
            }
        }
    }
}

/// Append a decimal's exact, value-canonical position on the
/// [`DECIMAL_SORT_KEY_SCALE`] grid (33 bytes): `0x00` then the bit-inverted
/// 32-byte big-endian magnitude for a negative value, so a larger magnitude
/// sorts earlier, or `0x01` then the magnitude. `-0` writes as `0`.
fn encode_decimal_grid(d: Decimal, out: &mut Vec<u8>) {
    let negative = d.is_sign_negative() && !d.is_zero();
    // scale ≤ 28, so 10^(28 − scale) ≤ 10^28 fits a u128.
    let pow = 10u128.pow(DECIMAL_SORT_KEY_SCALE - d.scale());
    let scaled = U256::mul(d.mantissa().unsigned_abs(), pow).to_be_bytes();
    if negative {
        out.push(0x00);
        out.extend(scaled.iter().map(|b| !b));
    } else {
        out.push(0x01);
        out.extend_from_slice(&scaled);
    }
}

/// An unsigned 256-bit integer, just wide enough for the exact decimal grid and
/// the exact decimal/float comparison without a bignum dependency. The derived
/// order compares `hi` first, which is numeric order.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct U256 {
    hi: u128,
    lo: u128,
}

impl U256 {
    fn from_u128(v: u128) -> Self {
        U256 { hi: 0, lo: v }
    }

    /// The full product of two `u128`s, by schoolbook multiplication over
    /// 64-bit limbs.
    fn mul(a: u128, b: u128) -> Self {
        const MASK: u128 = u64::MAX as u128;
        let (a_lo, a_hi) = (a & MASK, a >> 64);
        let (b_lo, b_hi) = (b & MASK, b >> 64);
        let ll = a_lo * b_lo;
        let lh = a_lo * b_hi;
        let hl = a_hi * b_lo;
        let hh = a_hi * b_hi;
        let mid = (ll >> 64) + (lh & MASK) + (hl & MASK);
        U256 {
            hi: hh + (lh >> 64) + (hl >> 64) + (mid >> 64),
            lo: (ll & MASK) | (mid << 64),
        }
    }

    /// Shift left by `n < 256` bits. Callers keep the result below 2^256.
    fn shl(self, n: u32) -> Self {
        match n {
            0 => self,
            1..=127 => U256 {
                hi: (self.hi << n) | (self.lo >> (128 - n)),
                lo: self.lo << n,
            },
            _ => U256 {
                hi: self.lo << (n - 128),
                lo: 0,
            },
        }
    }

    fn to_be_bytes(self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[..16].copy_from_slice(&self.hi.to_be_bytes());
        out[16..].copy_from_slice(&self.lo.to_be_bytes());
        out
    }
}

/// The class a number falls in under [`ties`], as a small `Copy` value: two
/// numbers tie exactly when their classes are equal, whatever their domains.
///
/// A number that is an integer in `i64` range is `Integer`; otherwise one that
/// is exactly an `f64` is `Float` with [`f64_orderable_bits`] (so every NaN is
/// one class and `-0.0` is `Integer(0)`); otherwise it is a decimal that no
/// integer or float equals, `Decimal` with its normalized serialized bytes.
/// Hashing a class never allocates.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum NumericTieClass {
    Integer(i64),
    Float(u64),
    Decimal([u8; 16]),
}

impl NumericTieClass {
    pub fn from_i64(v: i64) -> Self {
        NumericTieClass::Integer(v)
    }

    pub fn from_f64(v: f64) -> Self {
        if v.fract() == 0.0 && (-TWO_POW_63..TWO_POW_63).contains(&v) {
            NumericTieClass::Integer(v as i64)
        } else {
            NumericTieClass::Float(f64_orderable_bits(v))
        }
    }

    pub fn from_decimal(v: Decimal) -> Self {
        if v.fract().is_zero()
            && let Some(i) = v.to_i64()
        {
            return NumericTieClass::Integer(i);
        }
        match f64_bracket(Number::Dec(v)) {
            Bracket::Exact(f) => NumericTieClass::Float(f64_orderable_bits(f)),
            Bracket::Above(..) => NumericTieClass::Decimal(v.normalize().serialize()),
        }
    }

    fn of(n: Number) -> Self {
        match n {
            Number::Int(i) => NumericTieClass::from_i64(i),
            Number::Float(f) => NumericTieClass::from_f64(f),
            Number::Dec(d) => NumericTieClass::from_decimal(d),
        }
    }
}

/// Feed `v`'s tie class into `state`: values that tie under [`compare`] feed
/// identical input, so they hash equally under any [`Hasher`].
///
/// Writes the rank tag, then the domain's class: a number's
/// [`NumericTieClass`], a datetime's [`datetime_to_orderable_i128`], a
/// string's bytes, a date's day. An array feeds its length and each element's
/// class; a map feeds its length and an order-independent sum of per-entry
/// digests, so insertion order does not change the hash and no entry list is
/// sorted. Never allocates.
pub fn hash_tie_class<H: Hasher>(v: &Value, state: &mut H) {
    let domain = Domain::of(v);
    state.write_u8(domain.tag());
    match domain {
        Domain::Null => {}
        Domain::Bool(b) => b.hash(state),
        Domain::Number(n) => NumericTieClass::of(n).hash(state),
        Domain::Str(s) => s.hash(state),
        Domain::Date(d) => d.num_days_from_ce().hash(state),
        Domain::DateTime(dt) => datetime_to_orderable_i128(dt).hash(state),
        Domain::Array(items) => {
            items.len().hash(state);
            for item in items {
                hash_tie_class(item, state);
            }
        }
        Domain::Map(map) => {
            map.len().hash(state);
            let mut digest_sum = 0u64;
            for (k, v) in map.iter() {
                let mut entry = DefaultHasher::new();
                k.as_str().hash(&mut entry);
                hash_tie_class(v, &mut entry);
                digest_sum = digest_sum.wrapping_add(entry.finish());
            }
            digest_sum.hash(state);
        }
    }
}

/// Order-preserving `f64` to `u64`: unsigned order of the result is the value
/// order of floats. `-0.0` maps to `+0.0`'s image and every NaN, whatever its
/// sign or payload, to `u64::MAX`, which is above `+inf`'s image.
///
/// This is the one float bit transform; every float key derives from it.
#[inline]
pub fn f64_orderable_bits(f: f64) -> u64 {
    if f.is_nan() {
        return u64::MAX;
    }
    let bits = if f == 0.0 { 0 } else { f.to_bits() };
    if bits >> 63 == 1 {
        !bits // negative: flip every bit, so a larger magnitude sorts earlier
    } else {
        bits | (1 << 63) // positive or zero: set the sign bit only
    }
}

/// Order-preserving `NaiveDateTime` to `i128`: the UTC instant as a nanosecond
/// count since the Unix epoch, `timestamp_seconds · 10^9 + subsecond_nanos`.
///
/// This is the one datetime key: [`compare`], the byte key, the inequality-join
/// axis and the sort-merge range comparator all reduce datetimes through it, so
/// a datetime orders identically everywhere and sub-microsecond values stay
/// distinct through a byte-wise spilled merge.
///
/// Exact and injective for every non-leap-second `NaiveDateTime`: chrono caps
/// the year at ±262 143, bounding `|timestamp| < 8.3e12` seconds, so the result
/// stays under `8.3e21` in magnitude, far below `i128::MAX`. Assembling the
/// value directly (rather than via `timestamp_nanos_opt`, whose `i64` result
/// saturates outside 1677–2262) keeps pre-1677 and post-2262 datetimes exact.
/// `timestamp()` floors toward negative infinity and `timestamp_subsec_nanos()`
/// is the non-negative offset within that second, so the sum is correct for
/// pre-epoch instants too. Pure arithmetic: no allocation, no I/O.
///
/// Leap seconds are the one exception: chrono stores them as second `:59` with
/// a sub-second field in `[10^9, 2·10^9)`, which Unix `timestamp()` does not
/// count, so this key places a leap instant onto the following second, where
/// it ties that second's instant with the same fraction. That matches Unix-time
/// convention and the `Value::DateTime` spill serialization, which collapses
/// leap seconds identically, but not `NaiveDateTime::cmp`, which keeps the leap
/// instant first. The value order follows this key, so every consumer agrees
/// at a leap second; no linear nanosecond key could keep the two apart.
#[inline]
pub fn datetime_to_orderable_i128(dt: NaiveDateTime) -> i128 {
    let utc = dt.and_utc();
    (utc.timestamp() as i128) * 1_000_000_000 + utc.timestamp_subsec_nanos() as i128
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::owned_storage::OwnedValues;
    use std::hash::DefaultHasher;

    fn int(v: i64) -> Value {
        Value::Integer(v)
    }

    fn float(v: f64) -> Value {
        Value::Float(v)
    }

    fn dec(text: &str) -> Value {
        Value::Decimal(text.parse().expect("test decimal literal"))
    }

    fn string(text: &str) -> Value {
        Value::from(text)
    }

    fn array(items: Vec<Value>) -> Value {
        Value::Array(OwnedValues::from_vec(items))
    }

    fn date(y: i32, m: u32, d: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, d).expect("test date")
    }

    fn datetime(y: i32, m: u32, d: u32, h: u32, min: u32, s: u32, nano: u32) -> NaiveDateTime {
        date(y, m, d)
            .and_hms_nano_opt(h, min, s, nano)
            .expect("test datetime")
    }

    fn key(v: &Value) -> Vec<u8> {
        let mut out = Vec::new();
        encode(v, &mut out);
        out
    }

    fn tie_hash(v: &Value) -> u64 {
        let mut state = DefaultHasher::new();
        hash_tie_class(v, &mut state);
        state.finish()
    }

    /// NaNs of both signs, quiet and signalling, with several payloads.
    fn nans() -> [f64; 5] {
        [
            f64::NAN,
            -f64::NAN,
            f64::from_bits(0x7FF0_0000_0000_0001),
            f64::from_bits(0xFFF4_0000_0000_0ABC),
            f64::from_bits(0x7FFF_FFFF_FFFF_FFFF),
        ]
    }

    /// The last datetime of 2016 was a leap second; chrono stores it as
    /// second 59 with a sub-second field above one second.
    fn leap_second() -> NaiveDateTime {
        datetime(2016, 12, 31, 23, 59, 59, 1_500_000_000)
    }

    fn after_leap_second() -> NaiveDateTime {
        datetime(2017, 1, 1, 0, 0, 0, 500_000_000)
    }

    /// Every fixed case below, for the all-pairs agreement tests.
    fn fixed_cases() -> Vec<Value> {
        let mut cases = vec![
            Value::Null,
            Value::Bool(false),
            Value::Bool(true),
            int(i64::MIN),
            int(i64::MIN + 1),
            int(-9_007_199_254_740_993),
            int(-1),
            int(0),
            int(1),
            int(2),
            int(9_007_199_254_740_991),
            int(9_007_199_254_740_992),
            int(9_007_199_254_740_993),
            int(i64::MAX - 1),
            int(i64::MAX),
            float(f64::NEG_INFINITY),
            float(f64::MIN),
            float(-9_223_372_036_854_775_808.0),
            float(-1.5),
            float(-1.0),
            float(-0.0),
            float(0.0),
            float(-f64::from_bits(1)),
            float(f64::from_bits(1)),
            float(f64::MIN_POSITIVE),
            float(1e-30),
            float(0.1),
            float(0.5),
            float(1.0),
            float(1.0f64.next_up()),
            float(2.5),
            float(9_007_199_254_740_992.0),
            float(9_223_372_036_854_775_808.0),
            float(1e30),
            float(f64::MAX),
            float(f64::INFINITY),
            dec("0"),
            dec("0.00"),
            dec("-0"),
            dec("1"),
            dec("1.00"),
            dec("-1.5"),
            dec("0.1"),
            dec("0.5"),
            dec("2.5"),
            dec("2.50"),
            dec("1.0000000000000001"),
            dec("0.0000000000000000000000000001"),
            dec("-0.0000000000000000000000000001"),
            dec("9007199254740993"),
            dec("9223372036854775808"),
            dec("79228162514264337593543950335"),
            dec("-79228162514264337593543950335"),
            dec("7.9228162514264337593543950335"),
            string(""),
            string("a"),
            string("a\0"),
            string("a\0b"),
            string("ab"),
            string("é"),
            string("\u{10FFFF}"),
            Value::Date(date(-4000, 1, 1)),
            Value::Date(date(1969, 12, 31)),
            Value::Date(date(1970, 1, 1)),
            Value::Date(date(9999, 12, 31)),
            Value::DateTime(datetime(1969, 12, 31, 23, 59, 59, 999_999_999)),
            Value::DateTime(datetime(1970, 1, 1, 0, 0, 0, 0)),
            Value::DateTime(leap_second()),
            Value::DateTime(after_leap_second()),
            Value::DateTime(datetime(2017, 1, 1, 0, 0, 0, 500_000_001)),
            array(vec![]),
            array(vec![Value::Null]),
            array(vec![Value::Null, int(1)]),
            array(vec![int(1)]),
            array(vec![float(1.0)]),
            array(vec![int(1), int(2)]),
            array(vec![array(vec![int(1)])]),
            array(vec![string("a")]),
            Value::empty_map(),
            Value::map([("a", int(1))]),
            Value::map([("a", int(1)), ("b", int(2))]),
            Value::map([("b", int(2)), ("a", int(1))]),
            Value::map([("a", float(1.0)), ("b", dec("2"))]),
            Value::map([("a", int(2))]),
            Value::map([("b", int(1))]),
        ];
        cases.extend(nans().into_iter().map(float));
        cases
    }

    #[test]
    fn nan_is_one_value_above_infinity() {
        let above = [
            float(f64::INFINITY),
            float(f64::MAX),
            float(0.0),
            float(f64::NEG_INFINITY),
            int(i64::MAX),
            int(i64::MIN),
            dec("79228162514264337593543950335"),
            dec("-0.5"),
        ];
        for a in nans() {
            for b in nans() {
                assert_eq!(compare(&float(a), &float(b)), Ordering::Equal);
                assert_eq!(key(&float(a)), key(&float(b)));
            }
            for other in &above {
                assert_eq!(
                    compare(&float(a), other),
                    Ordering::Greater,
                    "{a:?} vs {other:?}"
                );
                assert_eq!(
                    compare(other, &float(a)),
                    Ordering::Less,
                    "{other:?} vs {a:?}"
                );
                assert!(key(&float(a)) > key(other), "{a:?} vs {other:?}");
            }
        }
    }

    #[test]
    fn negative_zero_ties_zero() {
        let zeros = [float(-0.0), float(0.0), int(0), dec("0.00"), dec("-0")];
        for a in &zeros {
            for b in &zeros {
                assert!(ties(a, b), "{a:?} vs {b:?}");
                assert_eq!(key(a), key(b), "{a:?} vs {b:?}");
            }
            assert_eq!(compare(a, &float(-f64::from_bits(1))), Ordering::Greater);
            assert_eq!(compare(a, &float(f64::from_bits(1))), Ordering::Less);
        }
    }

    #[test]
    fn integers_above_two_pow_53_stay_exact() {
        let two_pow_53 = 9_007_199_254_740_992i64;
        assert_eq!(
            compare(&int(two_pow_53 + 1), &float(9_007_199_254_740_992.0)),
            Ordering::Greater
        );
        assert_eq!(
            compare(&int(two_pow_53 + 1), &int(two_pow_53)),
            Ordering::Greater
        );
        assert!(key(&int(two_pow_53 + 1)) > key(&float(9_007_199_254_740_992.0)));
        assert!(ties(&int(two_pow_53), &float(9_007_199_254_740_992.0)));
        assert_eq!(
            compare(&int(i64::MAX), &float(9_223_372_036_854_775_808.0)),
            Ordering::Less
        );
        assert!(key(&int(i64::MAX)) < key(&float(9_223_372_036_854_775_808.0)));
        assert!(ties(&int(i64::MIN), &float(-9_223_372_036_854_775_808.0)));
        assert_eq!(
            key(&int(i64::MIN)),
            key(&float(-9_223_372_036_854_775_808.0))
        );
        assert_eq!(
            compare(&int(i64::MAX), &int(i64::MAX - 1)),
            Ordering::Greater
        );
        assert!(key(&int(i64::MAX)) > key(&int(i64::MAX - 1)));
    }

    #[test]
    fn numbers_compare_by_exact_value_across_types() {
        let ones = [int(1), float(1.0), dec("1"), dec("1.00")];
        for a in &ones {
            for b in &ones {
                assert!(ties(a, b), "{a:?} vs {b:?}");
                assert_eq!(key(a), key(b), "{a:?} vs {b:?}");
            }
        }
        // The float nearest 0.1 is 0.1000000000000000055511151231257827...
        assert_eq!(compare(&dec("0.1"), &float(0.1)), Ordering::Less);
        assert!(key(&dec("0.1")) < key(&float(0.1)));
        assert!(ties(&dec("2.5"), &float(2.5)));
        assert_eq!(key(&dec("2.5")), key(&float(2.5)));

        // 1 + 1e-16 lies strictly between 1.0 and the next float up.
        let between = dec("1.0000000000000001");
        assert_eq!(compare(&float(1.0), &between), Ordering::Less);
        assert_eq!(compare(&between, &float(1.0f64.next_up())), Ordering::Less);
        assert!(key(&float(1.0)) < key(&between));
        assert!(key(&between) < key(&float(1.0f64.next_up())));

        // Floats beyond the decimal range decide by sign.
        assert_eq!(
            compare(&dec("79228162514264337593543950335"), &float(1e30)),
            Ordering::Less
        );
        assert_eq!(
            compare(&dec("-79228162514264337593543950335"), &float(-1e30)),
            Ordering::Greater
        );
        assert_eq!(
            compare(&dec("0.0000000000000000000000000001"), &float(1e-30)),
            Ordering::Greater
        );
    }

    #[test]
    fn type_rank_is_fixed() {
        let ranked = [
            Value::Null,
            Value::Bool(true),
            float(f64::NEG_INFINITY),
            int(i64::MAX),
            float(f64::NAN),
            string(""),
            Value::Date(date(-4000, 1, 1)),
            Value::DateTime(datetime(1970, 1, 1, 0, 0, 0, 0)),
            array(vec![]),
            Value::empty_map(),
        ];
        for (i, a) in ranked.iter().enumerate() {
            for b in &ranked[i + 1..] {
                if matches!((a, b), (Value::Float(_), Value::Integer(_)))
                    || matches!((a, b), (Value::Integer(_), Value::Float(_)))
                    || matches!((a, b), (Value::Float(_), Value::Float(_)))
                {
                    continue;
                }
                assert_eq!(compare(a, b), Ordering::Less, "{a:?} vs {b:?}");
                assert!(key(a) < key(b), "{a:?} vs {b:?}");
            }
        }
        assert_eq!(
            compare(&Value::Bool(false), &Value::Bool(true)),
            Ordering::Less
        );
        let null_first = array(vec![Value::Null]);
        for v in [
            Value::Bool(false),
            int(i64::MIN),
            float(f64::NEG_INFINITY),
            string(""),
        ] {
            let other = array(vec![v]);
            assert_eq!(compare(&null_first, &other), Ordering::Less, "{other:?}");
            assert!(key(&null_first) < key(&other), "{other:?}");
        }
    }

    #[test]
    fn arrays_and_maps_compare_by_content() {
        assert_eq!(
            compare(&array(vec![int(1)]), &array(vec![int(1), int(0)])),
            Ordering::Less
        );
        assert_eq!(
            compare(&array(vec![int(1), int(2)]), &array(vec![int(2)])),
            Ordering::Less
        );
        assert!(ties(&array(vec![int(1)]), &array(vec![float(1.0)])));
        assert_eq!(key(&array(vec![int(1)])), key(&array(vec![float(1.0)])));
        assert_eq!(
            compare(&array(vec![]), &array(vec![Value::Null])),
            Ordering::Less
        );

        let ab = Value::map([("a", int(1)), ("b", int(2))]);
        let ba = Value::map([("b", int(2)), ("a", int(1))]);
        assert!(ties(&ab, &ba));
        assert_eq!(key(&ab), key(&ba));
        assert_eq!(compare(&Value::map([("a", int(1))]), &ab), Ordering::Less);
        assert_eq!(compare(&ab, &Value::map([("a", int(2))])), Ordering::Less);
        assert_eq!(compare(&ab, &Value::map([("b", int(1))])), Ordering::Less);
        assert!(ties(
            &ab,
            &Value::map([("a", dec("1.0")), ("b", float(2.0))])
        ));
    }

    #[test]
    fn datetime_orders_by_its_canonical_key() {
        let leap = Value::DateTime(leap_second());
        let next = Value::DateTime(after_leap_second());
        assert!(ties(&leap, &next));
        assert_eq!(key(&leap), key(&next));
        assert_eq!(tie_hash(&leap), tie_hash(&next));
        // chrono's own order keeps them apart; the one order does not.
        assert_eq!(leap_second().cmp(&after_leap_second()), Ordering::Less);

        let before = Value::DateTime(datetime(1969, 12, 31, 23, 59, 59, 999_999_999));
        let epoch = Value::DateTime(datetime(1970, 1, 1, 0, 0, 0, 0));
        assert_eq!(compare(&before, &epoch), Ordering::Less);
        assert!(key(&before) < key(&epoch));
        assert_eq!(
            datetime_to_orderable_i128(datetime(1970, 1, 1, 0, 0, 0, 1)),
            1
        );
        assert_eq!(
            datetime_to_orderable_i128(datetime(1969, 12, 31, 23, 59, 59, 999_999_999)),
            -1
        );
    }

    #[test]
    fn encoder_agrees_on_the_fixed_cases() {
        let cases = fixed_cases();
        for a in &cases {
            for b in &cases {
                let (ka, kb) = (key(a), key(b));
                assert_eq!(ka.cmp(&kb), compare(a, b), "{a:?} vs {b:?}");
                assert_eq!(ka == kb, ties(a, b), "{a:?} vs {b:?}");
                assert_eq!(compare(a, b), compare(b, a).reverse(), "{a:?} vs {b:?}");
                if ka != kb {
                    assert!(
                        !ka.starts_with(&kb) && !kb.starts_with(&ka),
                        "{a:?} vs {b:?}"
                    );
                }
            }
        }
    }

    /// The same-type shortcut in `compare` must answer exactly what the
    /// domain dispatch answers, on every ordered pair of the fixed cases:
    /// NaNs of every sign and payload, both zeros, the integer and float
    /// extremes, strings with NUL and non-ASCII bytes, and every mixed pair,
    /// which must fall through to the dispatch unchanged.
    #[test]
    fn same_type_shortcut_agrees_with_the_domain_dispatch() {
        let cases = fixed_cases();
        let mut shortcut_pairs = 0;
        for a in &cases {
            for b in &cases {
                shortcut_pairs += usize::from(matches!(
                    (a, b),
                    (Value::Integer(_), Value::Integer(_))
                        | (Value::Float(_), Value::Float(_))
                        | (Value::String(_), Value::String(_))
                ));
                assert_eq!(compare(a, b), compare_domains(a, b), "{a:?} vs {b:?}");
            }
        }
        // 12 integers, 26 floats (5 of them NaN) and 7 strings.
        assert_eq!(shortcut_pairs, 12 * 12 + 26 * 26 + 7 * 7);
    }

    #[test]
    fn tie_class_hash_is_equal_on_ties() {
        let cases = fixed_cases();
        let mut tied_pairs = 0;
        for a in &cases {
            for b in &cases {
                if ties(a, b) {
                    tied_pairs += usize::from(!std::ptr::eq(a, b));
                    assert_eq!(tie_hash(a), tie_hash(b), "{a:?} vs {b:?}");
                }
            }
        }
        assert!(
            tied_pairs > 0,
            "the fixed cases must contain cross-representation ties"
        );
        assert_ne!(tie_hash(&int(1)), tie_hash(&int(2)));
        assert_ne!(tie_hash(&int(1)), tie_hash(&string("1")));
        assert_ne!(
            tie_hash(&int(9_007_199_254_740_993)),
            tie_hash(&int(9_007_199_254_740_992))
        );

        assert_eq!(
            NumericTieClass::from_f64(-0.0),
            NumericTieClass::from_i64(0)
        );
        assert_eq!(
            NumericTieClass::from_decimal("42.00".parse().expect("decimal")),
            NumericTieClass::from_i64(42)
        );
        assert_eq!(
            NumericTieClass::from_decimal("2.50".parse().expect("decimal")),
            NumericTieClass::from_f64(2.5)
        );
        assert_eq!(
            NumericTieClass::from_f64(f64::NAN),
            NumericTieClass::from_f64(-f64::NAN)
        );
        assert_ne!(
            NumericTieClass::from_f64(0.1),
            NumericTieClass::from_decimal("0.1".parse().expect("decimal"))
        );
    }
}
