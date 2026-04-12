//! Deterministic CSV payload generator for benchmarks.

/// Generate a CSV byte buffer with deterministic content.
pub struct CsvPayload;

impl CsvPayload {
    /// Generate CSV bytes with a header row and `record_count` data rows.
    ///
    /// Columns: `f0` (integer), `f1` (string), ..., alternating types.
    /// Uses `seed` for reproducible output across runs.
    pub fn generate(
        record_count: usize,
        field_count: usize,
        string_len: usize,
        seed: u64,
    ) -> Vec<u8> {
        let mut buf = Vec::with_capacity(record_count * field_count * (string_len + 4));
        let mut rng = fastrand::Rng::with_seed(seed);

        // Header
        for i in 0..field_count {
            if i > 0 {
                buf.push(b',');
            }
            buf.extend_from_slice(format!("f{i}").as_bytes());
        }
        buf.push(b'\n');

        // Data rows
        for _ in 0..record_count {
            for i in 0..field_count {
                if i > 0 {
                    buf.push(b',');
                }
                if i % 2 == 0 {
                    buf.extend_from_slice(rng.i64(0..1_000_000).to_string().as_bytes());
                } else {
                    let s: String = (0..string_len)
                        .map(|_| (rng.u8(b'a'..=b'z')) as char)
                        .collect();
                    buf.extend_from_slice(s.as_bytes());
                }
            }
            buf.push(b'\n');
        }

        buf
    }
}
