//! Deterministic JSON payload generators for benchmarks.

use crate::FieldKind;

/// Write a JSON field value, adding quotes for string-like types.
fn write_json_field(
    buf: &mut Vec<u8>,
    kind: FieldKind,
    rng: &mut fastrand::Rng,
    string_len: usize,
) {
    match kind {
        FieldKind::Int | FieldKind::Float => {
            crate::write_field_value(buf, kind, rng, string_len);
        }
        FieldKind::Bool => {
            crate::write_field_value(buf, kind, rng, string_len);
        }
        FieldKind::String | FieldKind::Date => {
            buf.push(b'"');
            crate::write_field_value(buf, kind, rng, string_len);
            buf.push(b'"');
        }
    }
}

/// Generate NDJSON byte buffer with deterministic content.
///
/// Uses `seed` for reproducible output across runs.
pub fn generate_ndjson(
    record_count: usize,
    field_types: &[FieldKind],
    string_len: usize,
    seed: u64,
) -> Vec<u8> {
    let field_count = field_types.len();
    let mut buf = Vec::with_capacity(record_count * field_count * (string_len + 10));
    let mut rng = fastrand::Rng::with_seed(seed);

    for _ in 0..record_count {
        buf.push(b'{');
        for (i, &kind) in field_types.iter().enumerate() {
            if i > 0 {
                buf.push(b',');
            }
            buf.extend_from_slice(format!("\"f{i}\":").as_bytes());
            write_json_field(&mut buf, kind, &mut rng, string_len);
        }
        buf.extend_from_slice(b"}\n");
    }
    buf
}

/// Generate JSON array byte buffer with deterministic content.
///
/// Uses `seed` for reproducible output across runs.
pub fn generate_json_array(
    record_count: usize,
    field_types: &[FieldKind],
    string_len: usize,
    seed: u64,
) -> Vec<u8> {
    let field_count = field_types.len();
    let mut buf = Vec::with_capacity(record_count * field_count * (string_len + 10));
    let mut rng = fastrand::Rng::with_seed(seed);

    buf.push(b'[');
    for row in 0..record_count {
        if row > 0 {
            buf.push(b',');
        }
        buf.push(b'{');
        for (i, &kind) in field_types.iter().enumerate() {
            if i > 0 {
                buf.push(b',');
            }
            buf.extend_from_slice(format!("\"f{i}\":").as_bytes());
            write_json_field(&mut buf, kind, &mut rng, string_len);
        }
        buf.push(b'}');
    }
    buf.push(b']');
    buf
}
