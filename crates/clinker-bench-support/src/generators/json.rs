//! Deterministic JSON payload generators for benchmarks.

/// Generate NDJSON byte buffer with deterministic content.
///
/// Uses `seed` for reproducible output across runs.
pub fn generate_ndjson(
    record_count: usize,
    field_count: usize,
    string_len: usize,
    seed: u64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(record_count * field_count * (string_len + 10));
    let mut rng = fastrand::Rng::with_seed(seed);

    for _ in 0..record_count {
        buf.push(b'{');
        for i in 0..field_count {
            if i > 0 {
                buf.push(b',');
            }
            buf.extend_from_slice(format!("\"f{i}\":").as_bytes());
            if i % 2 == 0 {
                buf.extend_from_slice(rng.i64(0..1_000_000).to_string().as_bytes());
            } else {
                buf.push(b'"');
                for _ in 0..string_len {
                    buf.push(rng.u8(b'a'..=b'z'));
                }
                buf.push(b'"');
            }
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
    field_count: usize,
    string_len: usize,
    seed: u64,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(record_count * field_count * (string_len + 10));
    let mut rng = fastrand::Rng::with_seed(seed);

    buf.push(b'[');
    for row in 0..record_count {
        if row > 0 {
            buf.push(b',');
        }
        buf.push(b'{');
        for i in 0..field_count {
            if i > 0 {
                buf.push(b',');
            }
            buf.extend_from_slice(format!("\"f{i}\":").as_bytes());
            if i % 2 == 0 {
                buf.extend_from_slice(rng.i64(0..1_000_000).to_string().as_bytes());
            } else {
                buf.push(b'"');
                for _ in 0..string_len {
                    buf.push(rng.u8(b'a'..=b'z'));
                }
                buf.push(b'"');
            }
        }
        buf.push(b'}');
    }
    buf.push(b']');
    buf
}
