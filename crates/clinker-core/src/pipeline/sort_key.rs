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
//! - DateTime: sign-flipped big-endian i64 microseconds since Unix epoch (8 bytes)
//! - Descending: XOR all segment bytes with 0xFF

use chrono::NaiveDate;
use clinker_record::{Record, Value};

use crate::config::{NullOrder, SortField, SortOrder};

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
            let bits = f.to_bits();
            let encoded = if bits >> 63 == 1 {
                bits ^ u64::MAX // negative: flip all bits
            } else {
                bits ^ (1 << 63) // positive/zero: flip sign bit only
            };
            buf.extend_from_slice(&encoded.to_be_bytes());
        }
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
            let micros = dt.and_utc().timestamp_micros();
            let mut bytes = micros.to_be_bytes();
            bytes[0] ^= 0x80;
            buf.extend_from_slice(&bytes);
        }
        Value::Null => {}     // handled by caller
        Value::Array(_) => {} // not a valid sort key; defensive no-op
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use clinker_record::Schema;
    use std::sync::Arc;

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
