//! Shared deterministic data generators for Clinker benchmarks.
//!
//! All generators use explicit seeds for reproducibility across runs.

#[cfg(feature = "bench-alloc")]
pub mod alloc;

pub mod cache;
pub mod combine;
pub mod generators;

use clinker_record::{Record, Schema, Value};
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Returns the workspace root by walking up from `CARGO_MANIFEST_DIR`.
///
/// Single source of truth for workspace root resolution in benchmarks and tests.
/// Panics if the workspace root cannot be found.
pub fn workspace_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest
        .ancestors()
        .find(|p| {
            let toml = p.join("Cargo.toml");
            toml.exists()
                && std::fs::read_to_string(&toml)
                    .map(|s| s.contains("[workspace]"))
                    .unwrap_or(false)
        })
        .expect("could not find workspace root from CARGO_MANIFEST_DIR ancestors")
        .to_path_buf()
}

// ── Pipeline config discovery ─────────────────────────────────────

/// A discovered benchmark pipeline config.
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    /// Directory category (e.g. "format", "cxl_ops", "execution_mode").
    pub category: String,
    /// Config name without extension (e.g. "csv_passthrough").
    pub name: String,
    /// Absolute path to the YAML file.
    pub path: PathBuf,
}

/// Walk `base` directory, discover all `*.yaml` configs, exclude `future/`,
/// return sorted entries for deterministic Criterion registration order.
pub fn discover_pipeline_configs(base: &Path) -> Vec<ConfigEntry> {
    let pattern = base.join("**/*.yaml");
    let Some(pattern_str) = pattern.to_str() else {
        return Vec::new();
    };
    let Ok(paths) = glob::glob(pattern_str) else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    for entry in paths {
        let Ok(path) = entry else { continue };
        if path.components().any(|c| c.as_os_str() == "future") {
            continue;
        }
        let category = path
            .parent()
            .and_then(|p| p.file_name())
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        let name = path
            .file_stem()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        entries.push(ConfigEntry {
            category,
            name,
            path,
        });
    }
    entries.sort_by(|a, b| (&a.category, &a.name).cmp(&(&b.category, &b.name)));
    entries
}

// Explicit re-exports for backward compatibility (D-9: no glob re-exports)
pub use cache::DataSpec;
pub use combine::CombineDataGen;
pub use generators::csv::CsvPayload;
pub use generators::fixed_width::{
    BenchFieldSpec, BenchFieldType, BenchJustify, field_specs_for, generate_fixed_width,
};
pub use generators::json::{generate_json_array, generate_ndjson};
pub use generators::xml::generate_xml;

// ── Scale constants ────────────────────────────────────────────────

pub const SMALL: usize = 1_000;
pub const MEDIUM: usize = 10_000;
pub const LARGE: usize = 100_000;
pub const XLARGE: usize = 1_000_000;

/// Benchmark data scale tiers.
///
/// Labels appear in Criterion benchmark IDs and HTML reports.
/// Use `Scale::ALL` to iterate all variants for benchmark matrices.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scale {
    Small,
    Medium,
    Large,
    XLarge,
}

impl Scale {
    /// All scale variants in ascending order.
    pub const ALL: &[Scale] = &[Self::Small, Self::Medium, Self::Large, Self::XLarge];

    /// Number of records for this scale tier.
    pub fn record_count(self) -> usize {
        match self {
            Self::Small => SMALL,
            Self::Medium => MEDIUM,
            Self::Large => LARGE,
            Self::XLarge => XLARGE,
        }
    }

    /// Short label for Criterion benchmark IDs.
    pub fn label(self) -> &'static str {
        match self {
            Self::Small => "1k",
            Self::Medium => "10k",
            Self::Large => "100k",
            Self::XLarge => "1m",
        }
    }
}

impl fmt::Display for Scale {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

// ── Field kind for typed data generation ──────────────────────────

/// Kind of value to generate for a benchmark field.
///
/// Used by `DataSpec` and all format generators to produce type-appropriate
/// values. `repr(u8)` ensures stable discriminants for cache hashing.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[repr(u8)]
pub enum FieldKind {
    /// Integer: 0..1_000_000
    Int = 0,
    /// Random lowercase ASCII string
    String = 1,
    /// ISO 8601 date: `yyyy-MM-dd` in range 2020-01-01..2026-12-31
    Date = 2,
    /// Float: 0.0..1_000_000.0 with 2 decimal places
    Float = 3,
    /// Boolean: `true` or `false`
    Bool = 4,
}

impl FieldKind {
    /// Default field kind layout: even-indexed fields are Int, odd are String.
    /// This matches the legacy generator behavior.
    pub fn default_layout(field_count: usize) -> Vec<FieldKind> {
        (0..field_count)
            .map(|i| {
                if i % 2 == 0 {
                    FieldKind::Int
                } else {
                    FieldKind::String
                }
            })
            .collect()
    }
}

/// Write a single field value into `buf` according to its `FieldKind`.
///
/// Shared by all format generators. The value is written as a UTF-8 string
/// without any quoting or escaping — callers handle format-specific wrapping.
pub fn write_field_value(
    buf: &mut Vec<u8>,
    kind: FieldKind,
    rng: &mut fastrand::Rng,
    string_len: usize,
) {
    match kind {
        FieldKind::Int => {
            buf.extend_from_slice(rng.i64(0..1_000_000).to_string().as_bytes());
        }
        FieldKind::String => {
            for _ in 0..string_len {
                buf.push(rng.u8(b'a'..=b'z'));
            }
        }
        FieldKind::Date => {
            // Deterministic date in 2020-01-01..2026-12-31 range (~2556 days)
            let day_offset = rng.u32(0..2556);
            let base = chrono::NaiveDate::from_ymd_opt(2020, 1, 1).unwrap();
            let date = base + chrono::Duration::days(day_offset as i64);
            buf.extend_from_slice(date.format("%Y-%m-%d").to_string().as_bytes());
        }
        FieldKind::Float => {
            let val = rng.f64() * 1_000_000.0;
            buf.extend_from_slice(format!("{val:.2}").as_bytes());
        }
        FieldKind::Bool => {
            if rng.bool() {
                buf.extend_from_slice(b"true");
            } else {
                buf.extend_from_slice(b"false");
            }
        }
    }
}

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
        use crate::{CsvPayload, FieldKind};

        let bytes = CsvPayload::generate(10, &FieldKind::default_layout(4), 8, 42);

        assert!(!bytes.is_empty());
        assert!(bytes.starts_with(b"f0,f1,f2,f3\n"));
    }

    /// Verify generate_ndjson is accessible via crate-root re-export after extraction.
    #[test]
    fn test_json_ndjson_backward_compat_generates_bytes() {
        use crate::{FieldKind, generate_ndjson};

        let bytes = generate_ndjson(10, &FieldKind::default_layout(4), 8, 42);

        assert!(!bytes.is_empty());
        assert!(bytes[0] == b'{');
    }

    /// Verify Scale enum record counts match the corresponding constants
    /// and Display produces row-count labels.
    #[test]
    fn test_scale_record_counts_match_constants() {
        use crate::{LARGE, MEDIUM, SMALL, Scale, XLARGE};

        assert_eq!(Scale::Small.record_count(), SMALL);
        assert_eq!(Scale::Medium.record_count(), MEDIUM);
        assert_eq!(Scale::Large.record_count(), LARGE);
        assert_eq!(Scale::XLarge.record_count(), XLARGE);

        assert_eq!(Scale::Small.to_string(), "1k");
        assert_eq!(Scale::Medium.to_string(), "10k");
        assert_eq!(Scale::Large.to_string(), "100k");
        assert_eq!(Scale::XLarge.to_string(), "1m");

        assert_eq!(Scale::ALL.len(), 4);
    }

    /// Verify discovery finds all non-future configs (~41 expected).
    #[test]
    fn test_discover_configs_finds_all_non_future() {
        let base = crate::workspace_root().join("benches/pipelines");
        let entries = crate::discover_pipeline_configs(&base);
        // 7 format + 10 cxl_ops + 10 execution_mode + 12 features + 2 scale + 8 realistic = 49
        assert!(
            entries.len() >= 49,
            "Expected at least 49 configs, found {}",
            entries.len()
        );
    }

    /// Verify no entries come from the future/ directory.
    #[test]
    fn test_discover_configs_excludes_future() {
        let base = crate::workspace_root().join("benches/pipelines");
        let entries = crate::discover_pipeline_configs(&base);
        for entry in &entries {
            assert_ne!(
                entry.category, "future",
                "future/ config should be excluded: {}",
                entry.name
            );
        }
    }

    /// Two calls return identical order — deterministic for Criterion bench IDs.
    #[test]
    fn test_discover_configs_sorted_deterministic() {
        let base = crate::workspace_root().join("benches/pipelines");
        let a = crate::discover_pipeline_configs(&base);
        let b = crate::discover_pipeline_configs(&base);
        assert_eq!(a.len(), b.len());
        for (x, y) in a.iter().zip(b.iter()) {
            assert_eq!(x.category, y.category);
            assert_eq!(x.name, y.name);
            assert_eq!(x.path, y.path);
        }
    }

    /// Every entry's category equals its YAML file's parent directory name.
    #[test]
    fn test_discover_configs_category_matches_parent_dirname() {
        let base = crate::workspace_root().join("benches/pipelines");
        let entries = crate::discover_pipeline_configs(&base);
        for entry in &entries {
            let parent_name = entry
                .path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|n| n.to_str())
                .unwrap_or("");
            assert_eq!(
                entry.category, parent_name,
                "category mismatch for {}",
                entry.name
            );
        }
    }

    /// Empty directory returns empty Vec without panicking.
    #[test]
    fn test_discover_configs_empty_base_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let entries = crate::discover_pipeline_configs(tmp.path());
        assert!(entries.is_empty());
    }
}
