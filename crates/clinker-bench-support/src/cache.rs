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

    /// Cache file name: `{format}_{scale}_{fields}f_{strlen}s.{ext}`
    pub fn file_name(&self) -> String {
        let fmt = match self.format {
            DataFormat::Csv => "csv",
            DataFormat::JsonNdjson => "ndjson",
            DataFormat::JsonArray => "json_array",
            DataFormat::Xml => "xml",
            DataFormat::FixedWidth => "fixed_width",
        };
        format!(
            "{}_{}_{field_count}f_{string_len}s.{ext}",
            fmt,
            self.scale.label(),
            field_count = self.field_count,
            string_len = self.string_len,
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
        match spec.format {
            DataFormat::Csv => CsvPayload::generate(rc, ft, sl, seed),
            DataFormat::JsonNdjson => generate_ndjson(rc, ft, sl, seed),
            DataFormat::JsonArray => generate_json_array(rc, ft, sl, seed),
            DataFormat::Xml => generate_xml(rc, ft, sl, seed),
            DataFormat::FixedWidth => generate_fixed_width(rc, ft, sl, seed).0,
        }
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
