//! Deterministic, exact serde for [`rust_decimal::Decimal`] as a fixed
//! 16-byte array, for `#[serde(with = "crate::decimal_serde")]` on derived
//! structs (the aggregate accumulator state).
//!
//! `rust_decimal`'s own `serde` feature is intentionally left off (see the
//! workspace `Cargo.toml`): its serde format is text/float-flavored and carries
//! representation ambiguity. `Decimal::serialize() -> [u8; 16]` /
//! `Decimal::deserialize([u8; 16])` is the crate's own bit-exact, endian-stable
//! round-trip — the same pair the spill wire form uses for `Value::Decimal` —
//! so a spilled accumulator reloads byte-for-byte identical.

use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Serialize a `Decimal` as its exact 16-byte little-endian representation.
pub fn serialize<S>(d: &Decimal, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    // `d.serialize()` is the inherent `Decimal -> [u8; 16]` (arity 0 besides
    // `self`), distinct from the `Serialize` trait method.
    let bytes: [u8; 16] = d.serialize();
    bytes.serialize(serializer)
}

/// Reconstruct a `Decimal` from its exact 16-byte representation.
pub fn deserialize<'de, D>(deserializer: D) -> Result<Decimal, D::Error>
where
    D: Deserializer<'de>,
{
    let bytes = <[u8; 16]>::deserialize(deserializer)?;
    // Inherent `Decimal::deserialize([u8; 16]) -> Decimal`; the array argument
    // (not a `Deserializer`) selects it over the trait method.
    Ok(Decimal::deserialize(bytes))
}

#[cfg(test)]
mod invariant_tests {
    use rust_decimal::Decimal;
    #[test]
    fn eq_implies_same_normalized_bytes() {
        // The grouping/hash invariant: value-equal decimals must produce
        // identical normalized 16-byte keys.
        let pairs = [
            (Decimal::new(250, 2), Decimal::new(25, 1)), // 2.50 == 2.5
            (Decimal::new(0, 0), Decimal::new(0, 5)),    // 0 == 0.00000
            (Decimal::new(-1000, 3), Decimal::new(-1, 0)), // -1.000 == -1
            (Decimal::new(42, 0), Decimal::new(4200, 2)), // 42 == 42.00
        ];
        for (a, b) in pairs {
            assert_eq!(a, b, "precondition: values equal");
            assert_eq!(
                a.normalize().serialize(),
                b.normalize().serialize(),
                "equal decimals must normalize to identical bytes: {a} vs {b}"
            );
        }
        // And distinct values must differ.
        assert_ne!(
            Decimal::new(250, 2).normalize().serialize(),
            Decimal::new(251, 2).normalize().serialize()
        );
    }
    #[test]
    fn to_f64_is_monotone_on_representative_decimals() {
        use rust_decimal::prelude::ToPrimitive;
        let mut vals: Vec<Decimal> = vec![
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
            let fa = w[0].to_f64().unwrap();
            let fb = w[1].to_f64().unwrap();
            assert!(
                fa <= fb,
                "monotone violated: {} -> {} but {} -> {}",
                w[0],
                fa,
                w[1],
                fb
            );
        }
    }
}
