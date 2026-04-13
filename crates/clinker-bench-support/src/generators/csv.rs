//! Deterministic CSV payload generator for benchmarks.

use crate::FieldKind;

/// Generate a CSV byte buffer with deterministic content.
pub struct CsvPayload;

impl CsvPayload {
    /// Generate CSV bytes with a header row and `record_count` data rows.
    ///
    /// `field_types` controls the kind of value each column generates.
    /// Uses `seed` for reproducible output across runs.
    pub fn generate(
        record_count: usize,
        field_types: &[FieldKind],
        string_len: usize,
        seed: u64,
    ) -> Vec<u8> {
        let field_count = field_types.len();
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
            for (i, &kind) in field_types.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                crate::write_field_value(&mut buf, kind, &mut rng, string_len);
            }
            buf.push(b'\n');
        }

        buf
    }
}
