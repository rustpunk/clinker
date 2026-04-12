//! Shared deterministic data generators for Clinker benchmarks.
//!
//! All generators use explicit seeds for reproducibility across runs.

#[cfg(feature = "bench-alloc")]
pub mod alloc;

pub mod cache;
pub mod generators;

use clinker_record::{Record, Schema, Value};
use std::sync::Arc;

// Explicit re-exports for backward compatibility (D-9: no glob re-exports)
pub use generators::csv::CsvPayload;
pub use generators::json::{generate_json_array, generate_ndjson};
pub use generators::xml::generate_xml;

// ── Scale constants ────────────────────────────────────────────────

pub const SMALL: usize = 1_000;
pub const MEDIUM: usize = 10_000;
pub const LARGE: usize = 100_000;

// ── Record generation ──────────────────────────────────────────────

/// Deterministic record factory with configurable field layout.
pub struct RecordFactory {
    schema: Arc<Schema>,
    rng: fastrand::Rng,
    string_len: usize,
    null_ratio: f64,
}

impl RecordFactory {
    /// Create a factory that produces records with `field_count` columns.
    ///
    /// Fields are named `f0`, `f1`, ... `fN-1`.
    /// Even-indexed fields are integers, odd-indexed are strings.
    /// `null_ratio` in [0.0, 1.0] controls the fraction of null values.
    pub fn new(field_count: usize, string_len: usize, null_ratio: f64, seed: u64) -> Self {
        let columns: Vec<Box<str>> = (0..field_count)
            .map(|i| format!("f{i}").into_boxed_str())
            .collect();
        Self {
            schema: Arc::new(Schema::new(columns)),
            rng: fastrand::Rng::with_seed(seed),
            string_len,
            null_ratio,
        }
    }

    /// Schema used by all records from this factory.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// Generate a single record.
    pub fn next_record(&mut self) -> Record {
        let field_count = self.schema.column_count();
        let mut values = Vec::with_capacity(field_count);
        for i in 0..field_count {
            if self.null_ratio > 0.0 && self.rng.f64() < self.null_ratio {
                values.push(Value::Null);
            } else if i % 2 == 0 {
                values.push(Value::Integer(self.rng.i64(0..1_000_000)));
            } else {
                values.push(Value::String(self.random_string()));
            }
        }
        Record::new(Arc::clone(&self.schema), values)
    }

    /// Generate `count` records.
    pub fn generate(&mut self, count: usize) -> Vec<Record> {
        (0..count).map(|_| self.next_record()).collect()
    }

    fn random_string(&mut self) -> Box<str> {
        let bytes: Vec<u8> = (0..self.string_len)
            .map(|_| self.rng.u8(b'a'..=b'z'))
            .collect();
        // SAFETY: all bytes are ASCII a-z
        unsafe { String::from_utf8_unchecked(bytes) }.into_boxed_str()
    }
}

// ── CXL program templates ──────────────────────────────────────────

/// Complexity tiers for CXL program benchmarks.
pub enum CxlComplexity {
    /// Single emit: `emit x = f0 + 1`
    Simple,
    /// 10 emits with conditionals, coalesce, string methods
    Medium,
    /// 30+ emits with match expressions, nested conditionals, filters
    Complex,
}

impl CxlComplexity {
    /// Return CXL source for this complexity tier.
    ///
    /// Programs reference fields named `f0`, `f1`, ... matching `RecordFactory` output.
    pub fn source(&self) -> &'static str {
        match self {
            CxlComplexity::Simple => "emit x = f0 + 1",
            CxlComplexity::Medium => MEDIUM_CXL,
            CxlComplexity::Complex => COMPLEX_CXL,
        }
    }

    /// Return the list of field names the program references.
    pub fn required_fields(&self) -> Vec<&'static str> {
        match self {
            CxlComplexity::Simple => vec!["f0"],
            CxlComplexity::Medium => vec!["f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7"],
            CxlComplexity::Complex => {
                vec![
                    "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "f10", "f11",
                    "f12", "f13",
                ]
            }
        }
    }
}

const MEDIUM_CXL: &str = r#"
let base = f0 + f2
emit out0 = base * 2
emit out1 = f1.upper()
emit out2 = f3.trim()
emit out3 = if f0 > 500000 then "high" else "low"
emit out4 = f5 ?? f3 ?? "default"
emit out5 = f0.to_float().round(2)
emit out6 = f1.length()
emit out7 = f7.lower()
emit out8 = base + f4
emit out9 = f0.to_string()
"#;

const COMPLEX_CXL: &str = r#"
let base = f0 + f2
let ratio = f0.to_float() / (f2.to_float() + 1.0)

emit c0 = base * 2
emit c1 = f1.upper()
emit c2 = f3.trim()
emit c3 = if f0 > 500000 then "high" else if f0 > 250000 then "medium" else "low"
emit c4 = f5 ?? f3 ?? "default"
emit c5 = ratio.round(4)
emit c6 = f1.length()
emit c7 = f7.lower()
emit c8 = base + f4
emit c9 = f0.to_string()
emit c10 = match f3 {
    "alpha" => 1
    "beta" => 2
    "gamma" => 3
    "delta" => 4
    _ => 0
}
emit c11 = f1.replace("a", "x")
emit c12 = f1.substring(0, 5)
emit c13 = f0.abs()
emit c14 = f1.starts_with("a")
emit c15 = f3.contains("e")
emit c16 = if f0 > 100 and f2 > 100 then base else 0
emit c17 = f0.to_float().sqrt().round(2)
emit c18 = f1.pad_left(20, " ")
emit c19 = f1.reverse()
emit c20 = f0 % 7
emit c21 = if f0 > 0 then f0 * 2 else f0 * -1
emit c22 = f1.trim().upper()
emit c23 = f3.lower().trim()
emit c24 = ratio + base.to_float()
emit c25 = f0.to_string().length()
emit c26 = f5 ?? f7 ?? f1
emit c27 = if ratio > 1.0 then "above" else "below"
emit c28 = base * base
emit c29 = f1.left(3)

filter f0 > 100
"#;

#[cfg(test)]
mod tests {
    /// Verify CsvPayload is accessible via crate-root re-export after extraction.
    #[test]
    fn test_csv_payload_backward_compat_generates_bytes() {
        use crate::CsvPayload;

        let bytes = CsvPayload::generate(10, 4, 8, 42);

        assert!(!bytes.is_empty());
        assert!(bytes.starts_with(b"f0,f1,f2,f3\n"));
    }

    /// Verify generate_ndjson is accessible via crate-root re-export after extraction.
    #[test]
    fn test_json_ndjson_backward_compat_generates_bytes() {
        use crate::generate_ndjson;

        let bytes = generate_ndjson(10, 4, 8, 42);

        assert!(!bytes.is_empty());
        assert!(bytes[0] == b'{');
    }
}
