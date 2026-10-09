//! Group-key shapes the hash Aggregate's group-table bench and its counting
//! tests share, so the bench times the keys the tests pin, and the permutation
//! both use to visit a count in a scrambled order.

use chrono::NaiveDate;
use clinker_record::Value;

/// The key shapes: a 16-byte string, an integer, a decimal whose scale varies,
/// and a string, an integer and a date.
pub const GROUP_KEY_SHAPES: [&str; 4] = ["str16", "int", "decimal", "mixed3"];

/// The `n`-th distinct key of `shape`, as the values the operator groups by.
/// Distinct for every `n` below the count. Panics on an unknown shape.
pub fn group_key_values(shape: &str, n: u64) -> Vec<Value> {
    match shape {
        "str16" => vec![Value::String(format!("k{n:015}").into())],
        "int" => vec![Value::Integer(n as i64)],
        "decimal" => {
            // Scale 0-4; the fractional digits spell the scale, so no two `n`
            // normalize to the same value: `n` at scale 3 is `n.003`.
            let scale = (n % 5) as usize;
            let text = if scale == 0 {
                n.to_string()
            } else {
                format!("{n}.{scale:0>scale$}")
            };
            vec![Value::Decimal(text.parse().expect("a decimal literal"))]
        }
        "mixed3" => {
            let epoch = NaiveDate::from_ymd_opt(2020, 1, 1).expect("a valid date");
            vec![
                Value::String(format!("name-{}", n % 1_000).into()),
                Value::Integer((n / 1_000) as i64),
                Value::Date(epoch + chrono::Days::new(n % 365)),
            ]
        }
        other => panic!("unknown group-key shape {other}"),
    }
}

/// An odd prime multiplier, so `i * PERMUTE % n` visits each of `0..n` exactly
/// once in a scrambled order for every count below it, with no random-number
/// dependency.
pub const PERMUTE: u64 = 0x9E37_79B1;

/// The `i`-th element of the scrambled order over `0..n`.
pub fn permuted(i: usize, n: usize) -> u64 {
    (i as u64).wrapping_mul(PERMUTE) % n as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn every_shape_yields_distinct_keys() {
        for shape in GROUP_KEY_SHAPES {
            let keys: HashSet<String> = (0..2_000)
                .map(|n| format!("{:?}", group_key_values(shape, n)))
                .collect();
            assert_eq!(keys.len(), 2_000, "{shape}");
        }
    }

    #[test]
    fn decimal_keys_carry_their_scale() {
        let Value::Decimal(d) = &group_key_values("decimal", 7)[0] else {
            panic!("a decimal");
        };
        assert_eq!(d.scale(), 2);
        assert_eq!(d.to_string(), "7.02");
    }

    #[test]
    fn permuted_visits_every_index_once() {
        for n in [1usize, 2, 7, 1_000, 16_384] {
            let seen: HashSet<u64> = (0..n).map(|i| permuted(i, n)).collect();
            assert_eq!(seen.len(), n, "{n}");
        }
    }
}
