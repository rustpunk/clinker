//! Combine runtime primitives: hashing, float canonicalization, and (in
//! later sub-tasks) the index-separated build-side hash table.
//!
//! **Scope of this module:** runtime data structures consumed by the
//! `PlanNode::Combine` executor arm (wired up in C.2.2). Plan-time vocabulary
//! — `CombineStrategy`, `DecomposedPredicate`, `EqualityConjunct`,
//! `CombineInput` — lives in `crate::plan::combine`. Do not cross-pollinate.
//!
//! **Concurrency model (spec §Concurrency Model):** every public struct
//! declared here is designed to be immutable after construction. `RandomState`
//! is fixed at build time and shared with probe; no interior mutability.
//!
//! **Memory model:** combine is pipeline-breaking on the build side. All
//! build records are materialized into a single index-separated table before
//! any probe runs. Accounted allocation sources (per DataFusion #14222,
//! #5490, #6170): hash index, chain vector, record arena, cached build-side
//! key values.
//!
//! Sub-tasks currently landed in this module:
//!   - C.2.1.1 — `canonical_f64`/`canonical_f32`, `hash_composite_key`,
//!     `keys_equal_canonicalized`, module-level constants.
//!
//! Sub-tasks pending (to be added in order by subsequent commits):
//!   - C.2.1.2 — `KeyExtractor` (holds `Vec<Arc<TypedProgram>>`).
//!   - C.2.1.3 — `CombineHashTable` with `build()` / `probe()` /
//!     `ProbeIter`.
//!   - C.2.1.4 — `memory_bytes()` accounting across all four allocation
//!     sources.
//!   - C.2.1.5 — proptest round-trip.

use ahash::RandomState;
use chrono::{Datelike, Timelike};
use clinker_record::Value;
use std::hash::{BuildHasher, Hasher};

/// Period (measured in records processed) between
/// [`crate::pipeline::memory::MemoryBudget::should_abort`] checks during
/// `CombineHashTable::build` AND during probe-side fan-out emission.
///
/// Chosen by research (`docs/internal/research/RESEARCH-c2-preimplementation-gaps.md`
/// Q1): matches ClickHouse per-block cadence and is the same order of
/// magnitude as DataFusion's default 8192-row RecordBatch. Per-row RSS
/// polling is infeasible (~10 µs per `/proc/self/status` read ⇒ ~10 s/sec
/// overhead at 1 M rec/sec).
pub const MEMORY_CHECK_INTERVAL: usize = 10_000;

/// Maximum number of build-side matches accumulated into a single driver's
/// `Value::Array` under `MatchMode::Collect`. Overflow truncates and emits a
/// W-level diagnostic once per driver record.
///
/// Chosen by research (`RESEARCH-collect-semantics.md`): no ETL tool has a
/// built-in cap and every surveyed SQL engine (DuckDB, Spark, Polars) has
/// documented OOMs at hot keys under `collect_list`. Clinker sets its own
/// default; per-combine configurability is a future knob, out of C.2 scope.
pub const COLLECT_PER_GROUP_CAP: usize = 10_000;

/// End-of-chain sentinel for `CombineHashTable::chain`. Selected so the entire
/// `u32` index range except `u32::MAX` is available for records — a combine
/// with 4 billion build rows is not a realistic scenario.
pub const SENTINEL: u32 = u32::MAX;

/// Canonical quiet-NaN bit pattern for `f64`. Matches Polars
/// `polars_utils::total_ord::canonical_f64` and Spark
/// `NormalizeFloatingNumbers` (both pick the identical value — de-facto
/// Rust/JVM community standard).
pub const CANONICAL_NAN_F64: u64 = 0x7ff8_0000_0000_0000;

/// Canonical quiet-NaN bit pattern for `f32`.
pub const CANONICAL_NAN_F32: u32 = 0x7fc0_0000;

// Type-discriminator bytes written before each `Value` in
// `hash_composite_key`. Different variants MUST hash to different byte
// prefixes to prevent cross-type collisions (`Integer(42)` must not hash the
// same as `String("42")`). The sentinel for `Null` is deliberately placed at
// the top of the byte range to avoid any visual overlap with real tags.
const TAG_BOOL: u8 = 0x01;
const TAG_INTEGER: u8 = 0x02;
const TAG_FLOAT: u8 = 0x03;
const TAG_STRING: u8 = 0x04;
const TAG_DATE: u8 = 0x05;
const TAG_DATETIME: u8 = 0x06;
const TAG_ARRAY: u8 = 0x07;
const TAG_MAP: u8 = 0x08;
const TAG_NULL: u8 = 0xFF;

/// Canonicalize an `f64` for hashing and equality. Collapses `-0.0 → +0.0`
/// and every NaN bit pattern (signaling, quiet, with any payload or sign) to
/// `CANONICAL_NAN_F64`.
///
/// The `+ 0.0` step is the community-standard idiom shared by Polars, Spark,
/// and `ordered-float`. It exploits IEEE-754: `-0.0 + 0.0 == +0.0`, and any
/// NaN operand produces a NaN result (so the branch captures every NaN input
/// in one check).
#[inline]
pub fn canonical_f64(x: f64) -> f64 {
    let x = x + 0.0;
    if x.is_nan() {
        f64::from_bits(CANONICAL_NAN_F64)
    } else {
        x
    }
}

/// Canonicalize an `f32` for hashing and equality. See [`canonical_f64`].
#[inline]
pub fn canonical_f32(x: f32) -> f32 {
    let x = x + 0.0;
    if x.is_nan() {
        f32::from_bits(CANONICAL_NAN_F32)
    } else {
        x
    }
}

/// Hash a composite key formed by a sequence of `Value`s. Feeds every value
/// through a single streaming AHasher with a one-byte type tag preceding each
/// value's content bytes.
///
/// **Properties:**
///   - Type-disjoint: `hash([Integer(42)]) != hash([String("42")])` (with
///     overwhelming probability) because the type tags differ.
///   - Null-stable: `Value::Null` always hashes identically under the same
///     `RandomState`, independent of position.
///   - Float-canonical: `NaN`-variants and `±0.0` all hash identically via
///     [`canonical_f64`] / [`canonical_f32`].
///   - Order-sensitive for arrays/maps (array elements are fed in order; map
///     entries are fed in sorted key order for determinism across two maps
///     that have the same entries inserted in different orders).
///
/// Matches the streaming-hash design per Polars PR #15559 (migration away
/// from per-column `combine_hash`) and DataFusion's
/// `create_hashes`/`combine_hashes` seam.
pub fn hash_composite_key(values: &[Value], state: &RandomState) -> u64 {
    let mut h = state.build_hasher();
    for v in values {
        hash_value_into(v, &mut h);
    }
    h.finish()
}

/// Writes a `Value` into a running `Hasher`. Recursive over `Array` and `Map`.
fn hash_value_into<H: Hasher>(v: &Value, h: &mut H) {
    match v {
        Value::Null => h.write_u8(TAG_NULL),
        Value::Bool(b) => {
            h.write_u8(TAG_BOOL);
            h.write_u8(u8::from(*b));
        }
        Value::Integer(i) => {
            h.write_u8(TAG_INTEGER);
            h.write_i64(*i);
        }
        Value::Float(f) => {
            h.write_u8(TAG_FLOAT);
            h.write_u64(canonical_f64(*f).to_bits());
        }
        Value::String(s) => {
            h.write_u8(TAG_STRING);
            h.write_usize(s.len()); // length prefix prevents concatenation collisions
            h.write(s.as_bytes());
        }
        Value::Date(d) => {
            h.write_u8(TAG_DATE);
            h.write_i32(d.num_days_from_ce());
        }
        Value::DateTime(dt) => {
            h.write_u8(TAG_DATETIME);
            // `and_utc()` reinterprets the naive datetime as UTC so we can
            // extract a stable (timestamp_seconds, nanoseconds) pair that
            // losslessly identifies every chrono-representable instant.
            let utc = dt.and_utc();
            h.write_i64(utc.timestamp());
            h.write_u32(utc.nanosecond());
        }
        Value::Array(arr) => {
            h.write_u8(TAG_ARRAY);
            h.write_usize(arr.len());
            for x in arr {
                hash_value_into(x, h);
            }
        }
        Value::Map(m) => {
            h.write_u8(TAG_MAP);
            h.write_usize(m.len());
            // Maps hash in sorted-key order so two Maps with the same entries
            // hash identically regardless of insertion order. IndexMap
            // preserves insertion order, which is useful for display but
            // undesired for hash determinism.
            let mut keys: Vec<&Box<str>> = m.keys().collect();
            keys.sort_unstable();
            for k in keys {
                h.write_usize(k.len());
                h.write(k.as_bytes());
                // `IndexMap::get` is O(1); lookup per key is fine.
                hash_value_into(m.get(k.as_ref()).expect("key from iteration"), h);
            }
        }
    }
}

/// Canonicalized element-wise key equality. Applied AFTER hash match in the
/// probe path to filter out hash collisions (DataFusion #843 — emitting
/// no-match rows before exhausting the collision chain causes phantom
/// output).
///
/// **Semantics:**
///   - Lengths must match; otherwise unequal.
///   - Per-position cross-type (e.g., `Integer` vs `String`) is unequal.
///   - Per SQL 3VL, `Null` compared to ANY value (including another `Null`)
///     is unequal. In practice the probe path short-circuits when any
///     probe-side key is `Null`, so this branch handles build-side `Null`
///     keys colliding with non-null probe keys via hash; the equality check
///     rejects them, so they never match.
///   - `Float` uses [`canonical_f64`] on both sides, then compares `to_bits`
///     — so `NaN == NaN` (both canonicalize) and `+0.0 == -0.0` (both
///     collapse to `+0.0`).
///   - `Array` and `Map` recurse via the same rules. Maps compare
///     order-independently (same keys, same values per key).
// The `cfg_attr` is a brief bridge: these functions are consumed by
// `CombineHashTable::probe` in sub-task C.2.1.3 (next commit), but they
// already have unit-test coverage that would block C.2.1.1 landing without
// it. Remove the attribute when C.2.1.3 wires them into non-test code.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn keys_equal_canonicalized(left: &[Value], right: &[Value]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right.iter())
        .all(|(l, r)| value_equal_canonicalized(l, r))
}

#[cfg_attr(not(test), allow(dead_code))]
fn value_equal_canonicalized(a: &Value, b: &Value) -> bool {
    match (a, b) {
        // SQL 3VL: NULL is never equal to anything, including NULL.
        (Value::Null, _) | (_, Value::Null) => false,
        (Value::Bool(x), Value::Bool(y)) => x == y,
        (Value::Integer(x), Value::Integer(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => {
            canonical_f64(*x).to_bits() == canonical_f64(*y).to_bits()
        }
        (Value::String(x), Value::String(y)) => x == y,
        (Value::Date(x), Value::Date(y)) => x == y,
        (Value::DateTime(x), Value::DateTime(y)) => x == y,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y.iter())
                    .all(|(l, r)| value_equal_canonicalized(l, r))
        }
        (Value::Map(x), Value::Map(y)) => {
            if x.len() != y.len() {
                return false;
            }
            // Order-independent: every key in x exists in y with an equal
            // canonicalized value.
            x.iter().all(|(k, vx)| {
                y.get(k.as_ref())
                    .is_some_and(|vy| value_equal_canonicalized(vx, vy))
            })
        }
        _ => false, // cross-type
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use indexmap::IndexMap;

    // Deterministic RandomState across tests so hashes are stable within a
    // test run; a fixed seed disables per-process randomization.
    fn deterministic_state() -> RandomState {
        RandomState::with_seeds(0xA5A5_A5A5, 0x5A5A_5A5A, 0xDEAD_BEEF, 0xCAFE_F00D)
    }

    #[test]
    fn canonical_f64_collapses_neg_zero() {
        assert_eq!(canonical_f64(-0.0).to_bits(), canonical_f64(0.0).to_bits());
        assert_eq!(canonical_f64(0.0).to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn canonical_f64_collapses_all_nan_bit_patterns() {
        // Multiple NaN bit patterns — quiet, signaling, negative-sign,
        // with distinct payloads — all collapse to CANONICAL_NAN_F64.
        let patterns = [
            f64::NAN,
            f64::from_bits(0x7ff8_0000_0000_0001), // quiet NaN with payload 1
            f64::from_bits(0x7ff0_0000_0000_0001), // signaling NaN
            f64::from_bits(0xfff8_0000_0000_0000), // negative-sign quiet NaN
            f64::from_bits(0xfff0_0000_0000_0042), // negative-sign signaling NaN with payload
        ];
        for p in patterns {
            assert!(p.is_nan(), "test input {p:?} must be NaN");
            assert_eq!(
                canonical_f64(p).to_bits(),
                CANONICAL_NAN_F64,
                "bit pattern {:#x} did not canonicalize",
                p.to_bits()
            );
        }
    }

    #[test]
    fn canonical_f64_preserves_ordinary_values() {
        for x in [
            1.0_f64,
            -1.0,
            1.5,
            f64::MIN,
            f64::MAX,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ] {
            assert_eq!(canonical_f64(x).to_bits(), x.to_bits(), "{x} corrupted");
        }
    }

    #[test]
    fn canonical_f32_collapses_neg_zero_and_nans() {
        assert_eq!(canonical_f32(-0.0).to_bits(), canonical_f32(0.0).to_bits());
        assert_eq!(canonical_f32(0.0).to_bits(), 0.0_f32.to_bits());
        for p in [
            f32::NAN,
            f32::from_bits(0x7fc0_0001),
            f32::from_bits(0xffc0_0000),
            f32::from_bits(0x7f80_0001), // signaling NaN
        ] {
            assert!(p.is_nan());
            assert_eq!(canonical_f32(p).to_bits(), CANONICAL_NAN_F32);
        }
    }

    #[test]
    fn hash_composite_key_type_discriminates() {
        let s = deterministic_state();
        let as_int = hash_composite_key(&[Value::Integer(42)], &s);
        let as_str = hash_composite_key(&[Value::String("42".into())], &s);
        assert_ne!(
            as_int, as_str,
            "integer vs string with same display must not collide"
        );

        let as_bool = hash_composite_key(&[Value::Bool(true)], &s);
        let as_int_one = hash_composite_key(&[Value::Integer(1)], &s);
        assert_ne!(
            as_bool, as_int_one,
            "bool true vs integer 1 must not collide"
        );
    }

    #[test]
    fn hash_composite_key_null_sentinel_stable() {
        let s = deterministic_state();
        let a = hash_composite_key(&[Value::Null, Value::Integer(7)], &s);
        let b = hash_composite_key(&[Value::Null, Value::Integer(7)], &s);
        assert_eq!(a, b, "null sentinel must hash identically across calls");

        // Null in different position produces different hash (positional).
        let c = hash_composite_key(&[Value::Integer(7), Value::Null], &s);
        assert_ne!(a, c, "null position matters");
    }

    #[test]
    fn hash_composite_key_nan_equivalence() {
        let s = deterministic_state();
        let nan1 = Value::Float(f64::NAN);
        let nan2 = Value::Float(f64::from_bits(0x7ff8_0000_0000_0001));
        let nan3 = Value::Float(f64::from_bits(0xfff0_0000_0000_0042));
        let h1 = hash_composite_key(&[nan1], &s);
        let h2 = hash_composite_key(&[nan2], &s);
        let h3 = hash_composite_key(&[nan3], &s);
        assert_eq!(
            h1, h2,
            "different NaN payloads must hash identically after canonicalization"
        );
        assert_eq!(h1, h3, "sign-bit-flipped NaN must also canonicalize");
    }

    #[test]
    fn hash_composite_key_neg_zero_equivalence() {
        let s = deterministic_state();
        let pos = hash_composite_key(&[Value::Float(0.0)], &s);
        let neg = hash_composite_key(&[Value::Float(-0.0)], &s);
        assert_eq!(pos, neg, "-0.0 and +0.0 must hash identically");
    }

    #[test]
    fn hash_composite_key_array_order_sensitive() {
        // Arrays are ordered: [1, 2] and [2, 1] are different values and
        // must hash differently.
        let s = deterministic_state();
        let a = hash_composite_key(
            &[Value::Array(vec![Value::Integer(1), Value::Integer(2)])],
            &s,
        );
        let b = hash_composite_key(
            &[Value::Array(vec![Value::Integer(2), Value::Integer(1)])],
            &s,
        );
        assert_ne!(a, b);
    }

    #[test]
    fn hash_composite_key_map_order_independent() {
        // Maps with the same (k, v) pairs must hash identically regardless
        // of insertion order.
        let s = deterministic_state();
        let mut m1: IndexMap<Box<str>, Value> = IndexMap::new();
        m1.insert("a".into(), Value::Integer(1));
        m1.insert("b".into(), Value::Integer(2));
        let mut m2: IndexMap<Box<str>, Value> = IndexMap::new();
        m2.insert("b".into(), Value::Integer(2));
        m2.insert("a".into(), Value::Integer(1));
        let h1 = hash_composite_key(&[Value::Map(Box::new(m1))], &s);
        let h2 = hash_composite_key(&[Value::Map(Box::new(m2))], &s);
        assert_eq!(h1, h2);
    }

    #[test]
    fn hash_composite_key_string_length_prefix_prevents_concat_collisions() {
        // Without a length prefix, ["ab", "c"] and ["a", "bc"] would produce
        // identical content bytes. With the length prefix they must diverge.
        let s = deterministic_state();
        let a = hash_composite_key(&[Value::String("ab".into()), Value::String("c".into())], &s);
        let b = hash_composite_key(&[Value::String("a".into()), Value::String("bc".into())], &s);
        assert_ne!(a, b);
    }

    #[test]
    fn keys_equal_canonicalized_float_nan_equal() {
        let a = [Value::Float(f64::NAN)];
        let b = [Value::Float(f64::from_bits(0x7ff8_0000_0000_00ff))];
        assert!(keys_equal_canonicalized(&a, &b));
    }

    #[test]
    fn keys_equal_canonicalized_neg_zero_equal() {
        let a = [Value::Float(-0.0)];
        let b = [Value::Float(0.0)];
        assert!(keys_equal_canonicalized(&a, &b));
    }

    #[test]
    fn keys_equal_canonicalized_null_never_equal() {
        // SQL 3VL: NULL == anything is false, including NULL.
        assert!(!keys_equal_canonicalized(&[Value::Null], &[Value::Null]));
        assert!(!keys_equal_canonicalized(
            &[Value::Null],
            &[Value::Integer(0)]
        ));
        assert!(!keys_equal_canonicalized(
            &[Value::Integer(0)],
            &[Value::Null]
        ));
    }

    #[test]
    fn keys_equal_canonicalized_cross_type_unequal() {
        assert!(!keys_equal_canonicalized(
            &[Value::Integer(1)],
            &[Value::Bool(true)]
        ));
        assert!(!keys_equal_canonicalized(
            &[Value::Integer(42)],
            &[Value::String("42".into())]
        ));
    }

    #[test]
    fn keys_equal_canonicalized_length_mismatch() {
        assert!(!keys_equal_canonicalized(
            &[Value::Integer(1), Value::Integer(2)],
            &[Value::Integer(1)]
        ));
    }

    #[test]
    fn keys_equal_canonicalized_datetime_exact() {
        let dt1 = NaiveDate::from_ymd_opt(2026, 4, 18)
            .unwrap()
            .and_hms_nano_opt(12, 34, 56, 123_456_789)
            .unwrap();
        let dt2 = NaiveDate::from_ymd_opt(2026, 4, 18)
            .unwrap()
            .and_hms_nano_opt(12, 34, 56, 123_456_789)
            .unwrap();
        assert!(keys_equal_canonicalized(
            &[Value::DateTime(dt1)],
            &[Value::DateTime(dt2)]
        ));

        // Differ by one nanosecond → unequal.
        let dt3 = NaiveDate::from_ymd_opt(2026, 4, 18)
            .unwrap()
            .and_hms_nano_opt(12, 34, 56, 123_456_790)
            .unwrap();
        assert!(!keys_equal_canonicalized(
            &[Value::DateTime(dt1)],
            &[Value::DateTime(dt3)]
        ));
    }

    #[test]
    fn keys_equal_canonicalized_map_order_independent() {
        let mut m1: IndexMap<Box<str>, Value> = IndexMap::new();
        m1.insert("k1".into(), Value::Integer(1));
        m1.insert("k2".into(), Value::Integer(2));
        let mut m2: IndexMap<Box<str>, Value> = IndexMap::new();
        m2.insert("k2".into(), Value::Integer(2));
        m2.insert("k1".into(), Value::Integer(1));
        assert!(keys_equal_canonicalized(
            &[Value::Map(Box::new(m1))],
            &[Value::Map(Box::new(m2))]
        ));
    }

    #[test]
    fn hash_and_equal_agree_on_canonicalization() {
        // Anti-regression: if two Value sequences compare equal via
        // `keys_equal_canonicalized`, they MUST hash identically via
        // `hash_composite_key`. The canonicalization rules have to be shared
        // between the two paths.
        let s = deterministic_state();
        let pairs: Vec<([Value; 1], [Value; 1])> = vec![
            ([Value::Float(0.0)], [Value::Float(-0.0)]),
            (
                [Value::Float(f64::NAN)],
                [Value::Float(f64::from_bits(0x7ff8_0000_0000_ab00))],
            ),
        ];
        for (a, b) in pairs {
            assert!(
                keys_equal_canonicalized(&a, &b),
                "pair claimed equal but isn't"
            );
            assert_eq!(
                hash_composite_key(&a, &s),
                hash_composite_key(&b, &s),
                "equal pair hashed differently"
            );
        }
    }
}
