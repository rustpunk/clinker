//! Deterministic data generators for Phase Combine benchmarks.
//!
//! Produces two correlated record sets (build + probe) with configurable
//! cardinality, overlap ratio, and column width. Values are generated via
//! modular arithmetic — no RNG, no wall-clock seed — so two calls with the
//! same inputs produce byte-identical output.
//!
//! Follows the H2O db-benchmark pattern: uniform key distribution, a
//! tunable probe-side overlap ratio (default 90%), and string non-key
//! columns of deterministic length.

use clinker_record::{Record, Schema, SchemaBuilder, Value};
use std::sync::Arc;

/// Generates two correlated record sets for combine benchmarks.
///
/// - **build** is the smaller side (loaded into a hash table / lookup
///   table). Its `key` column holds values `0..min(build_rows, key_cardinality)`;
///   if `build_rows > key_cardinality` the keys cycle by modular arithmetic,
///   producing duplicate keys on the build side.
/// - **probe** is the driving side. A fraction `overlap_ratio` of its rows
///   have keys drawn from `0..key_cardinality` (guaranteed match); the rest
///   use keys `key_cardinality..(key_cardinality + miss_count)` (guaranteed
///   miss).
///
/// Both sides share one `key` column (integer) and `extra_columns`
/// deterministic string columns. The primary column is always named `key`,
/// followed by `c0`, `c1`, ... `cN-1`.
///
/// # Determinism
///
/// No RNG is used — values derive from the row index via modular
/// arithmetic. Output is byte-identical across runs.
///
/// # Example
///
/// ```ignore
/// let data_gen = CombineDataGen {
///     build_rows: 10_000,
///     probe_rows: 100_000,
///     overlap_ratio: 0.9,
///     key_cardinality: 10_000,
///     extra_columns: 3,
/// };
/// let build = data_gen.build_records();
/// let probe = data_gen.probe_records();
/// ```
pub struct CombineDataGen {
    pub build_rows: usize,
    pub probe_rows: usize,
    /// Fraction of probe keys that have a match in build (0.0 - 1.0).
    pub overlap_ratio: f64,
    /// Number of unique key values in the build side. If larger than
    /// `build_rows`, the effective cardinality is clamped to `build_rows`.
    pub key_cardinality: usize,
    /// Number of non-key columns per record.
    pub extra_columns: usize,
}

impl CombineDataGen {
    /// Shared schema for both build and probe records.
    ///
    /// Column layout: `key` (int), then `c0`, `c1`, ... `c{extra_columns-1}`
    /// (string).
    fn schema(&self) -> Arc<Schema> {
        SchemaBuilder::with_capacity(1 + self.extra_columns)
            .with_field("key")
            .extend((0..self.extra_columns).map(|i| format!("c{i}")))
            .build()
    }

    /// Deterministically generate an ASCII-lowercase string from a row index
    /// and column index. Fixed length 8 — wide enough for unique-ish output
    /// within a bench but cheap to allocate.
    fn det_string(row: usize, col: usize) -> Box<str> {
        const LEN: usize = 8;
        let mut bytes = [0u8; LEN];
        // Mix row and col into a 32-bit lane, then peel off 5-bit (a-z +
        // overflow) chunks. Fully deterministic; identical input ⇒ identical
        // output.
        let mut mix: u32 =
            (row as u32).wrapping_mul(0x9E37_79B1) ^ (col as u32).wrapping_mul(0x85EB_CA6B);
        for b in bytes.iter_mut() {
            *b = b'a' + ((mix & 0x1F) % 26) as u8;
            mix = mix.rotate_left(7).wrapping_add(1);
        }
        // SAFETY: all bytes in [b'a', b'a' + 25], always valid ASCII.
        unsafe { String::from_utf8_unchecked(bytes.to_vec()) }.into_boxed_str()
    }

    /// Build a record with the given integer key and deterministic string
    /// non-key columns derived from `row`.
    fn make_record(&self, schema: &Arc<Schema>, row: usize, key: i64) -> Record {
        let mut values = Vec::with_capacity(1 + self.extra_columns);
        values.push(Value::Integer(key));
        for col in 0..self.extra_columns {
            values.push(Value::String(Self::det_string(row, col)));
        }
        Record::new(Arc::clone(schema), values)
    }

    /// Effective build-side cardinality: `min(key_cardinality, build_rows)`.
    /// Guarantees at least one build row per unique key (assuming
    /// `build_rows >= 1`), and never produces more than `build_rows`
    /// distinct keys.
    fn effective_cardinality(&self) -> usize {
        self.key_cardinality.min(self.build_rows).max(1)
    }

    /// Generate build-side records. Keys are `i % effective_cardinality` for
    /// row `i` in `0..build_rows`. When `key_cardinality >= build_rows`,
    /// every build row has a unique key.
    pub fn build_records(&self) -> Vec<Record> {
        let schema = self.schema();
        let card = self.effective_cardinality();
        let mut out = Vec::with_capacity(self.build_rows);
        for i in 0..self.build_rows {
            let key = (i % card) as i64;
            out.push(self.make_record(&schema, i, key));
        }
        out
    }

    /// Generate probe-side records.
    ///
    /// The first `floor(probe_rows * overlap_ratio)` records use keys in
    /// `0..effective_cardinality` (guaranteed match against build). The
    /// remaining records use keys in `effective_cardinality..
    /// (effective_cardinality + miss_count)` (guaranteed miss). Keys cycle
    /// by modular arithmetic within each segment.
    pub fn probe_records(&self) -> Vec<Record> {
        let schema = self.schema();
        let card = self.effective_cardinality();
        let match_count = ((self.probe_rows as f64) * self.overlap_ratio).floor() as usize;
        let match_count = match_count.min(self.probe_rows);
        let miss_count = self.probe_rows - match_count;

        let mut out = Vec::with_capacity(self.probe_rows);
        for i in 0..match_count {
            let key = (i % card) as i64;
            out.push(self.make_record(&schema, i, key));
        }
        // Miss keys: start past the build-side range. Modular cycling within
        // a miss window of size `max(miss_count, 1)` keeps the generator
        // purely arithmetic.
        let miss_window = miss_count.max(1);
        for j in 0..miss_count {
            let key = (card as i64) + (j % miss_window) as i64;
            let row = match_count + j;
            out.push(self.make_record(&schema, row, key));
        }
        out
    }

    /// Serialize build-side records to CSV bytes (with header row).
    /// Columns: `key,c0,c1,...,c{extra_columns-1}`. Deterministic.
    pub fn build_csv(&self) -> Vec<u8> {
        self.records_to_csv(&self.build_records())
    }

    /// Serialize probe-side records to CSV bytes (with header row).
    pub fn probe_csv(&self) -> Vec<u8> {
        self.records_to_csv(&self.probe_records())
    }

    fn records_to_csv(&self, records: &[Record]) -> Vec<u8> {
        // Estimated width: 10 bytes for key + 9 bytes per string column.
        let approx = records.len() * (10 + 9 * self.extra_columns);
        let mut buf = Vec::with_capacity(approx);
        // Header.
        buf.extend_from_slice(b"key");
        for i in 0..self.extra_columns {
            buf.push(b',');
            buf.extend_from_slice(format!("c{i}").as_bytes());
        }
        buf.push(b'\n');
        // Rows.
        for rec in records {
            let Some(Value::Integer(k)) = rec.get("key") else {
                panic!("CombineDataGen record missing integer key");
            };
            buf.extend_from_slice(k.to_string().as_bytes());
            for i in 0..self.extra_columns {
                buf.push(b',');
                match rec.get(&format!("c{i}")) {
                    Some(Value::String(s)) => buf.extend_from_slice(s.as_bytes()),
                    other => panic!("CombineDataGen record missing c{i}: got {other:?}"),
                }
            }
            buf.push(b'\n');
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_record::Value;

    /// Gate test for C.0.5.2: generator produces the requested number of
    /// records on both sides, and the probe/build overlap falls within
    /// ±5% of the requested ratio.
    #[test]
    fn test_combine_data_generator_produces_records() {
        let data_gen = CombineDataGen {
            build_rows: 100,
            probe_rows: 1000,
            overlap_ratio: 0.9,
            key_cardinality: 100,
            extra_columns: 3,
        };
        let build = data_gen.build_records();
        let probe = data_gen.probe_records();
        assert_eq!(build.len(), 100);
        assert_eq!(probe.len(), 1000);

        // Collect build keys.
        let mut build_keys = std::collections::HashSet::new();
        for rec in &build {
            match rec.get("key") {
                Some(Value::Integer(k)) => {
                    build_keys.insert(*k);
                }
                other => panic!("expected Integer key, got {other:?}"),
            }
        }

        // Verify ~90% of probe keys exist in build.
        let matching = probe
            .iter()
            .filter(|r| match r.get("key") {
                Some(Value::Integer(k)) => build_keys.contains(k),
                _ => false,
            })
            .count();
        let observed = matching as f64 / probe.len() as f64;
        assert!(
            (0.85..=0.95).contains(&observed),
            "observed overlap {observed:.3} outside [0.85, 0.95]"
        );
    }

    /// Generator is deterministic: two successive invocations produce
    /// byte-identical output.
    #[test]
    fn test_combine_data_generator_is_deterministic() {
        let data_gen = CombineDataGen {
            build_rows: 50,
            probe_rows: 200,
            overlap_ratio: 0.8,
            key_cardinality: 50,
            extra_columns: 2,
        };
        let a = data_gen.build_records();
        let b = data_gen.build_records();
        assert_eq!(a.len(), b.len());
        for (ra, rb) in a.iter().zip(b.iter()) {
            assert_eq!(ra.get("key"), rb.get("key"));
            assert_eq!(ra.get("c0"), rb.get("c0"));
            assert_eq!(ra.get("c1"), rb.get("c1"));
        }

        let pa = data_gen.probe_records();
        let pb = data_gen.probe_records();
        for (ra, rb) in pa.iter().zip(pb.iter()) {
            assert_eq!(ra.get("key"), rb.get("key"));
        }
    }

    /// When `key_cardinality > build_rows`, effective cardinality is
    /// clamped so that the build side has exactly `build_rows` unique
    /// keys.
    #[test]
    fn test_combine_data_generator_clamps_cardinality() {
        let data_gen = CombineDataGen {
            build_rows: 10,
            probe_rows: 50,
            overlap_ratio: 1.0,
            key_cardinality: 1_000,
            extra_columns: 1,
        };
        let build = data_gen.build_records();
        let mut keys = std::collections::HashSet::new();
        for rec in &build {
            if let Some(Value::Integer(k)) = rec.get("key") {
                keys.insert(*k);
            }
        }
        assert_eq!(
            keys.len(),
            10,
            "expected 10 unique keys, got {}",
            keys.len()
        );
    }
}
