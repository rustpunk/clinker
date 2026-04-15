//! Benchmark data file cache with `.meta.json` sidecars.
//!
//! Generates data files on first access, caches them in `bench-data/`,
//! and regenerates when metadata doesn't match (blake3 hash invalidation).

use crate::generators::{
    csv::CsvPayload,
    fixed_width::generate_fixed_width,
    json::{generate_json_array, generate_ndjson},
    xml::generate_xml,
};
use crate::{FieldKind, Scale};
use std::fs;
use std::path::{Path, PathBuf};

const GENERATOR_VERSION: u32 = 1;

/// Specification for a benchmark data file.
/// Derives Hash for blake3 cache key computation (D-7).
#[derive(Debug, Clone, Hash)]
pub struct DataSpec {
    pub format: DataFormat,
    pub scale: Scale,
    pub field_count: usize,
    pub string_len: usize,
    pub seed: u64,
    /// Per-field type layout. Length must equal `field_count`.
    /// Determines what kind of value each field generates.
    pub field_types: Vec<FieldKind>,
    /// Optional tag for cache file disambiguation.
    ///
    /// When multiple sources share the same format/scale/field_count/string_len
    /// but differ in field_types, the tag prevents cache file thrashing.
    /// Set to the source node name for multi-source pipelines; empty string
    /// for single-source pipelines (preserves existing cache filenames).
    pub tag: String,
    /// Optional nesting wrapper for JSON/XML formats.
    ///
    /// When set, the generated flat data is wrapped in a parent structure
    /// so that format readers with `record_path` can navigate to the records.
    pub wrapper: Option<NestedWrapper>,
}

/// Nesting wrapper for benchmark data generation.
///
/// Wraps flat generated records in a parent structure so that format
/// readers configured with `record_path` can find them.
#[derive(Debug, Clone, Hash)]
pub enum NestedWrapper {
    /// Wraps JSON array in nested object: `{"seg1":{"seg2":[...]}}`
    Json { path_segments: Vec<String> },
    /// Wraps XML records in parent elements: `<seg1><seg2><record>...`
    Xml { parent_elements: Vec<String> },
}

/// Supported data formats for benchmark generation.
/// repr(u32) ensures stable discriminants for blake3 cache hashing (V-1-1 fix).
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
#[repr(u32)]
pub enum DataFormat {
    Csv,
    JsonNdjson,
    JsonArray,
    Xml,
    FixedWidth,
}

/// Sidecar metadata for cache validation.
#[derive(serde::Serialize, serde::Deserialize)]
struct CacheMeta {
    hash: String,
    generated_at: String,
}

/// Cached benchmark data directory.
///
/// Default: `{workspace_root}/bench-data/`. Override via `CLINKER_BENCH_DATA`
/// env var. Files are generated on first access and cached with `.meta.json`
/// sidecars for invalidation (D-7: blake3 content hash).
pub struct BenchDataCache {
    root: PathBuf,
}

impl DataSpec {
    /// blake3 hash of all generation parameters + GENERATOR_VERSION.
    /// Used as cache invalidation key (D-7).
    pub fn cache_hash(&self) -> String {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&GENERATOR_VERSION.to_le_bytes());
        hasher.update(&(self.format as u32).to_le_bytes());
        hasher.update(&(self.scale.record_count() as u64).to_le_bytes());
        hasher.update(&(self.field_count as u64).to_le_bytes());
        hasher.update(&(self.string_len as u64).to_le_bytes());
        hasher.update(&self.seed.to_le_bytes());
        for ft in &self.field_types {
            hasher.update(&[*ft as u8]);
        }
        hasher.update(self.tag.as_bytes());
        match &self.wrapper {
            None => {
                hasher.update(&[0u8]);
            }
            Some(NestedWrapper::Json { path_segments }) => {
                hasher.update(&[1u8]);
                for seg in path_segments {
                    hasher.update(seg.as_bytes());
                    hasher.update(&[0u8]); // separator
                }
            }
            Some(NestedWrapper::Xml { parent_elements }) => {
                hasher.update(&[2u8]);
                for elem in parent_elements {
                    hasher.update(elem.as_bytes());
                    hasher.update(&[0u8]);
                }
            }
        }
        hasher.finalize().to_hex().to_string()
    }

    /// File extension for this format.
    pub fn extension(&self) -> &'static str {
        match self.format {
            DataFormat::Csv => "csv",
            DataFormat::JsonNdjson => "ndjson",
            DataFormat::JsonArray => "json",
            DataFormat::Xml => "xml",
            DataFormat::FixedWidth => "txt",
        }
    }

    /// Cache file name: `{format}_{scale}_{fields}f_{strlen}s[_{tag}][_nested].{ext}`
    pub fn file_name(&self) -> String {
        let fmt = match self.format {
            DataFormat::Csv => "csv",
            DataFormat::JsonNdjson => "ndjson",
            DataFormat::JsonArray => "json_array",
            DataFormat::Xml => "xml",
            DataFormat::FixedWidth => "fixed_width",
        };
        let tag_suffix = if self.tag.is_empty() {
            String::new()
        } else {
            format!("_{}", self.tag)
        };
        let nested_suffix = if self.wrapper.is_some() {
            "_nested"
        } else {
            ""
        };
        format!(
            "{}_{}_{field_count}f_{string_len}s{tag}{nested}.{ext}",
            fmt,
            self.scale.label(),
            field_count = self.field_count,
            string_len = self.string_len,
            tag = tag_suffix,
            nested = nested_suffix,
            ext = self.extension(),
        )
    }
}

impl BenchDataCache {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    /// Default cache dir: `{workspace_root}/bench-data/`.
    /// Override via `CLINKER_BENCH_DATA` env var (D-2: arrow-rs pattern).
    pub fn default_location() -> Self {
        if let Ok(dir) = std::env::var("CLINKER_BENCH_DATA") {
            return Self::new(PathBuf::from(dir));
        }
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
        let workspace_root = manifest
            .ancestors()
            .find(|p| {
                let toml = p.join("Cargo.toml");
                toml.exists()
                    && fs::read_to_string(&toml)
                        .map(|s| {
                            s.contains("[workspace]")
                                || s.contains("[workspace.members]")
                                || s.contains("workspace.members")
                        })
                        .unwrap_or(false)
            })
            .expect("could not find workspace root from CARGO_MANIFEST_DIR ancestors");
        debug_assert!(
            workspace_root.join("Cargo.toml").exists(),
            "workspace root detection failed: no Cargo.toml at {workspace_root:?}"
        );
        Self::new(workspace_root.join("bench-data"))
    }

    /// Return path to cached data file, generating if needed.
    ///
    /// Cache validation: blake3 hash in `.meta.json` sidecar (D-7).
    /// Atomic writes: write to temp file, rename to final path (D-3).
    /// Force regeneration: set `CLINKER_BENCH_REGENERATE=1`.
    pub fn get_or_generate(&self, spec: &DataSpec) -> PathBuf {
        fs::create_dir_all(&self.root).expect("failed to create bench-data dir");

        let data_path = self.root.join(spec.file_name());
        let meta_path = data_path.with_extension(format!("{}.meta.json", spec.extension()));
        let expected_hash = spec.cache_hash();
        let force = std::env::var("CLINKER_BENCH_REGENERATE")
            .map(|v| v == "1")
            .unwrap_or(false);

        // Check cache validity
        if !force
            && data_path.exists()
            && meta_path.exists()
            && let Ok(meta_json) = fs::read_to_string(&meta_path)
            && let Ok(meta) = serde_json::from_str::<CacheMeta>(&meta_json)
            && meta.hash == expected_hash
        {
            return data_path;
        }

        // Generate data
        let data = self.generate_data(spec);

        // Atomic write: temp file then rename (D-3).
        // Use PID + thread ID to avoid collisions when parallel test threads
        // generate the same cache entry concurrently.
        let pid = std::process::id();
        let tid = format!("{:?}", std::thread::current().id());
        let tmp_data = self
            .root
            .join(format!("{}.tmp.{pid}.{tid}", spec.file_name()));
        let tmp_meta = self
            .root
            .join(format!("{}.meta.json.tmp.{pid}.{tid}", spec.file_name()));

        fs::write(&tmp_data, &data).expect("failed to write bench data");
        let meta = CacheMeta {
            hash: expected_hash,
            generated_at: chrono::Utc::now().to_rfc3339(),
        };
        fs::write(&tmp_meta, serde_json::to_string_pretty(&meta).unwrap())
            .expect("failed to write meta");

        // Rename to final path. If another thread already placed the file,
        // that's fine — deterministic content means last-writer-wins is correct (D-3).
        let _ = fs::rename(&tmp_data, &data_path);
        let _ = fs::rename(&tmp_meta, &meta_path);
        // Clean up temp files in case rename failed (another thread won)
        let _ = fs::remove_file(&tmp_data);
        let _ = fs::remove_file(&tmp_meta);

        data_path
    }

    fn generate_data(&self, spec: &DataSpec) -> Vec<u8> {
        let rc = spec.scale.record_count();
        let sl = spec.string_len;
        let seed = spec.seed;
        let ft = &spec.field_types;
        let data = match spec.format {
            DataFormat::Csv => CsvPayload::generate(rc, ft, sl, seed),
            DataFormat::JsonNdjson => generate_ndjson(rc, ft, sl, seed),
            DataFormat::JsonArray => generate_json_array(rc, ft, sl, seed),
            DataFormat::Xml => generate_xml(rc, ft, sl, seed),
            DataFormat::FixedWidth => generate_fixed_width(rc, ft, sl, seed).0,
        };
        match &spec.wrapper {
            None => data,
            Some(wrapper) => wrap_nested(data, spec.format, wrapper),
        }
    }
}

/// Wrap flat generated data in a nesting structure for `record_path` testing.
fn wrap_nested(data: Vec<u8>, format: DataFormat, wrapper: &NestedWrapper) -> Vec<u8> {
    match (format, wrapper) {
        (DataFormat::JsonArray, NestedWrapper::Json { path_segments }) => {
            // Wrap JSON array `[...]` in nested objects: `{"seg1":{"seg2":[...]}}`
            let mut buf = Vec::with_capacity(data.len() + path_segments.len() * 20);
            for seg in path_segments {
                buf.extend_from_slice(b"{\"");
                buf.extend_from_slice(seg.as_bytes());
                buf.extend_from_slice(b"\":");
            }
            buf.extend_from_slice(&data);
            buf.extend(std::iter::repeat_n(b'}', path_segments.len()));
            buf
        }
        (DataFormat::Xml, NestedWrapper::Xml { parent_elements }) => {
            // Wrap XML: insert parent elements after prolog, before <records>,
            // and close them after </records>.
            // Current XML format: `<?xml ...?>\n<records>\n...\n</records>\n`
            // We replace `<records>` with `<parent1><parent2>` and keep <record> as-is.
            //
            // Every structural marker the wrapper depends on (`?>\n`,
            // `<records>\n`, `</records>\n`) is guarded up front with a
            // `let-else` so a future format change falls through to the
            // same unchanged-bytes fallback instead of panicking in a
            // subsequent `.unwrap()`.
            let data_str = String::from_utf8(data).expect("XML is UTF-8");
            let Some(prolog_end) = data_str.find("?>\n") else {
                return data_str.into_bytes();
            };
            let Some(records_start) = data_str.find("<records>\n") else {
                return data_str.into_bytes();
            };
            let Some(records_end) = data_str.find("</records>\n") else {
                return data_str.into_bytes();
            };
            let mut buf = Vec::with_capacity(data_str.len() + parent_elements.len() * 30);
            buf.extend_from_slice(&data_str.as_bytes()[..prolog_end + 3]);
            // Open parent elements
            for elem in parent_elements {
                buf.extend_from_slice(b"<");
                buf.extend_from_slice(elem.as_bytes());
                buf.extend_from_slice(b">");
            }
            buf.push(b'\n');
            // Emit the record elements (between <records>\n and </records>\n)
            buf.extend_from_slice(
                &data_str.as_bytes()[records_start + "<records>\n".len()..records_end],
            );
            // Close parent elements in reverse
            for elem in parent_elements.iter().rev() {
                buf.extend_from_slice(b"</");
                buf.extend_from_slice(elem.as_bytes());
                buf.extend_from_slice(b">");
            }
            buf.push(b'\n');
            buf
        }
        _ => data, // No wrapping for other format/wrapper combinations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_spec() -> DataSpec {
        DataSpec {
            format: DataFormat::Csv,
            scale: Scale::Small,
            field_count: 4,
            string_len: 8,
            seed: 42,
            field_types: FieldKind::default_layout(4),
            tag: String::new(),
            wrapper: None,
        }
    }

    /// First call to get_or_generate creates both data file and .meta.json sidecar.
    #[test]
    fn test_cache_generates_on_first_call() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BenchDataCache::new(dir.path().to_path_buf());
        let spec = test_spec();

        let path = cache.get_or_generate(&spec);

        assert!(path.exists());
        assert!(path.with_extension("csv.meta.json").exists());
        let bytes = fs::read(&path).unwrap();
        assert!(!bytes.is_empty());
    }

    /// Second call with identical spec returns cached file without regenerating.
    /// Verified by checking that content hash is identical between calls.
    #[test]
    fn test_cache_reuses_existing_file() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BenchDataCache::new(dir.path().to_path_buf());
        let spec = test_spec();

        let path1 = cache.get_or_generate(&spec);
        let hash1 = blake3::hash(&fs::read(&path1).unwrap())
            .to_hex()
            .to_string();

        let path2 = cache.get_or_generate(&spec);
        let hash2 = blake3::hash(&fs::read(&path2).unwrap())
            .to_hex()
            .to_string();

        assert_eq!(path1, path2);
        assert_eq!(hash1, hash2);
    }

    /// Changing a spec parameter produces a different cache hash,
    /// causing regeneration even though a file exists for the old spec.
    #[test]
    fn test_cache_regenerates_on_meta_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BenchDataCache::new(dir.path().to_path_buf());

        let spec1 = test_spec();
        let _path1 = cache.get_or_generate(&spec1);

        // Write a meta with wrong hash to simulate parameter change
        let data_path = dir.path().join(spec1.file_name());
        let meta_path = data_path.with_extension("csv.meta.json");
        let bad_meta = CacheMeta {
            hash: "wrong_hash".to_string(),
            generated_at: "2026-01-01T00:00:00Z".to_string(),
        };
        fs::write(&meta_path, serde_json::to_string(&bad_meta).unwrap()).unwrap();

        let _path2 = cache.get_or_generate(&spec1);

        // File was regenerated — meta now has correct hash
        let meta: CacheMeta =
            serde_json::from_str(&fs::read_to_string(&meta_path).unwrap()).unwrap();
        assert_eq!(meta.hash, spec1.cache_hash());
    }

    /// Tag field produces a distinct filename and cache hash.
    #[test]
    fn test_tag_produces_distinct_filename() {
        let mut a = test_spec();
        a.tag = String::new();
        let mut b = test_spec();
        b.tag = "orders".to_string();

        assert_ne!(a.file_name(), b.file_name());
        assert_ne!(a.cache_hash(), b.cache_hash());
        assert!(b.file_name().contains("_orders"));
    }

    /// Nested wrapper produces a distinct filename with _nested suffix.
    #[test]
    fn test_nested_wrapper_produces_distinct_filename() {
        let mut flat = test_spec();
        flat.format = DataFormat::JsonArray;

        let mut nested = flat.clone();
        nested.wrapper = Some(NestedWrapper::Json {
            path_segments: vec!["data".into(), "records".into()],
        });

        assert_ne!(flat.file_name(), nested.file_name());
        assert_ne!(flat.cache_hash(), nested.cache_hash());
        assert!(nested.file_name().contains("_nested"));
    }

    /// JSON nesting wraps array in nested objects.
    #[test]
    fn test_wrap_nested_json() {
        let data = b"[{\"f0\":1},{\"f0\":2}]".to_vec();
        let wrapped = wrap_nested(
            data,
            DataFormat::JsonArray,
            &NestedWrapper::Json {
                path_segments: vec!["data".into(), "records".into()],
            },
        );
        let s = String::from_utf8(wrapped).unwrap();
        assert!(s.starts_with("{\"data\":{\"records\":"));
        assert!(s.ends_with("}}"));
        // Verify valid JSON
        let _: serde_json::Value = serde_json::from_str(&s).unwrap();
    }

    /// XML nesting wraps records in parent elements.
    #[test]
    fn test_wrap_nested_xml() {
        let data =
            b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<records>\n<record><f0>1</f0></record>\n</records>\n"
                .to_vec();
        let wrapped = wrap_nested(
            data,
            DataFormat::Xml,
            &NestedWrapper::Xml {
                parent_elements: vec!["root".into(), "data".into()],
            },
        );
        let s = String::from_utf8(wrapped).unwrap();
        assert!(s.contains("<root><data>"));
        assert!(s.contains("</data></root>"));
        assert!(s.contains("<record><f0>1</f0></record>"));
        assert!(!s.contains("<records>"));
    }

    /// CLINKER_BENCH_REGENERATE=1 forces regeneration regardless of cache state.
    #[test]
    #[serial_test::serial]
    fn test_cache_regenerate_env_var() {
        let dir = tempfile::tempdir().unwrap();
        let cache = BenchDataCache::new(dir.path().to_path_buf());
        let spec = test_spec();

        let path1 = cache.get_or_generate(&spec);

        // Corrupt the cached file so we can detect regeneration
        fs::write(&path1, b"corrupted").unwrap();

        // Set env var to force regen (unsafe in edition 2024 — V-7-1)
        unsafe { std::env::set_var("CLINKER_BENCH_REGENERATE", "1") };
        let path2 = cache.get_or_generate(&spec);
        unsafe { std::env::remove_var("CLINKER_BENCH_REGENERATE") };

        assert_eq!(path1, path2);
        let content = fs::read(&path2).unwrap();
        assert_ne!(content, b"corrupted", "file should have been regenerated");
    }
}
