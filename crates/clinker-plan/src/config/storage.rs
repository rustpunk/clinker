//! Workspace-level `[storage]` configuration parsed from `clinker.toml`.
//!
//! Unlike pipeline-level knobs (which live in the per-pipeline YAML under
//! `pipeline:`), storage settings are a property of the *workspace* — the
//! same physical spill volume and staging policy apply to every pipeline run
//! anchored at a given `clinker.toml`. They are deserialized here from the
//! workspace-root `clinker.toml`, validated once at executor startup, and
//! threaded into the run as runtime parameters rather than as part of the
//! compiled plan.
//!
//! Today only the spill root directory is honored at runtime; the staging
//! block parses but stays inert until the source-staging skeleton lands.
//! Parsing it now keeps `clinker.toml` files that opt into staging from
//! tripping `deny_unknown_fields` before the feature exists.

use crate::config::utils::ByteSize;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Top-level `clinker.toml` document.
///
/// Only the `[storage]` table is modeled. Channel/workspace discovery keys
/// that may also live in `clinker.toml` are not deserialized here — this
/// type is consulted solely for the storage scaffold, so unknown top-level
/// tables are tolerated rather than rejected.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClinkerToml {
    /// The `[storage]` table. Absent → every field defaults (spill to the
    /// OS temp dir, staging off), matching pre-config behavior exactly.
    #[serde(default)]
    pub storage: StorageConfig,
}

impl ClinkerToml {
    /// Parse a `clinker.toml` document from its raw text.
    ///
    /// # Errors
    ///
    /// Returns [`StorageConfigError::Parse`] when the text is not valid TOML
    /// or contains a key whose type does not match the schema.
    pub fn parse(text: &str) -> Result<Self, StorageConfigError> {
        toml::from_str(text).map_err(|e| StorageConfigError::Parse(e.to_string()))
    }

    /// Read and parse the `clinker.toml` at `workspace_root`, returning the
    /// default (empty) document when no such file exists.
    ///
    /// A missing `clinker.toml` is not an error: a workspace with no storage
    /// opinions runs with the inherited defaults. An unreadable or malformed
    /// file *is* an error so a typo in the storage block surfaces at startup
    /// rather than being silently ignored.
    ///
    /// # Errors
    ///
    /// Returns [`StorageConfigError::Read`] when the file exists but cannot
    /// be read, or [`StorageConfigError::Parse`] when its contents are not
    /// valid TOML.
    pub fn load_from_workspace(workspace_root: &Path) -> Result<Self, StorageConfigError> {
        let path = workspace_root.join("clinker.toml");
        match std::fs::read_to_string(&path) {
            Ok(text) => Self::parse(&text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(StorageConfigError::Read {
                path: path.clone(),
                source: e.to_string(),
            }),
        }
    }
}

/// The `[storage]` block: spill and staging policy for a workspace.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    /// `[storage.spill]` — where blocking operators (Aggregate, sort,
    /// grace-hash Combine, node-buffer overflow) write their spill files.
    #[serde(default)]
    pub spill: SpillConfig,
    /// `[storage.staging]` — opt-in copy of source files to local disk
    /// before execution. Parsed but inert until the staging skeleton lands.
    #[serde(default)]
    pub staging: StagingConfig,
}

/// `[storage.spill]` — spill-file root directory.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpillConfig {
    /// Root directory under which the per-run `clinker-spill-*` directory is
    /// created. `None` (key omitted) → the OS temp dir
    /// ([`std::env::temp_dir`]), preserving the historical default.
    ///
    /// A relative path is resolved against the process working directory by
    /// the filesystem layer, not normalized here — operators redirecting
    /// spill to a mounted volume are expected to give an absolute path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<PathBuf>,
    /// Cumulative disk-spill quota for the run, in bytes. `None` (key
    /// omitted) → unlimited, preserving the historical default. Accepts a
    /// bare integer (bytes) or a human-readable string (`"500MB"`, `"2GB"`)
    /// through the same [`ByteSize`] parser the source-filter size knobs use,
    /// so a `clinker.toml` author writes one unit grammar across the file.
    ///
    /// When the summed on-disk size of every spill file a run writes crosses
    /// this cap, the run aborts with a dedicated cap-exceeded diagnostic
    /// rather than continuing to fill the volume. The cap is deliberately
    /// distinct from the RSS `memory.limit`: a run can sit comfortably inside
    /// its memory envelope yet still exhaust local disk through an unbounded
    /// stream of spill files, and the operator needs to see "you hit the disk
    /// cap" — not an out-of-memory message — when that happens (the confusing
    /// surface DuckDB hit in duckdb/duckdb#14142, where a temp-dir cap
    /// rendered as "OOM with 187 GiB available").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_cap_bytes: Option<ByteSize>,
}

impl SpillConfig {
    /// Cumulative spill-quota cap in bytes, or `None` when no
    /// `disk_cap_bytes` was configured (unlimited spill).
    ///
    /// The executor folds `Some(cap)` into the run's memory arbitrator as
    /// the disk-spill quota; `None` leaves the quota at its unlimited
    /// default. Returns a plain `u64` so the executor stays free of the
    /// `ByteSize` newtype.
    pub fn disk_cap(&self) -> Option<u64> {
        self.disk_cap_bytes.map(|ByteSize(n)| n)
    }
}

impl SpillConfig {
    /// Validate that the configured spill `dir` exists and is a writable
    /// directory, returning the resolved root or `None` when no directory
    /// was configured (the OS-temp-dir default).
    ///
    /// Validation runs once at executor startup so a misconfigured spill
    /// volume fails the run before any work begins, rather than at the first
    /// spill — the failure mode DuckDB hit when its `temp_directory` setting
    /// was honored only lazily (duckdb/duckdb#9401).
    ///
    /// # Errors
    ///
    /// Returns [`StorageConfigError::SpillDirMissing`] when the path does not
    /// exist, [`StorageConfigError::SpillDirNotADirectory`] when it exists
    /// but is a file, or [`StorageConfigError::SpillDirNotWritable`] when a
    /// probe write into it fails (permissions, read-only mount).
    pub fn resolve(&self) -> Result<Option<PathBuf>, StorageConfigError> {
        let Some(dir) = self.dir.as_ref() else {
            return Ok(None);
        };
        let meta = std::fs::metadata(dir).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageConfigError::SpillDirMissing { path: dir.clone() }
            } else {
                StorageConfigError::SpillDirNotWritable {
                    path: dir.clone(),
                    source: e.to_string(),
                }
            }
        })?;
        if !meta.is_dir() {
            return Err(StorageConfigError::SpillDirNotADirectory { path: dir.clone() });
        }
        // Probe writability with a real create-and-delete: directory
        // permission bits alone do not guarantee a write succeeds (read-only
        // mount, SELinux, ACLs), and a probe that actually writes catches
        // every case a `mode` inspection would miss.
        let probe = tempfile::Builder::new()
            .prefix(".clinker-spill-probe-")
            .tempfile_in(dir)
            .map_err(|e| StorageConfigError::SpillDirNotWritable {
                path: dir.clone(),
                source: e.to_string(),
            })?;
        // `probe` drops here, unlinking the probe file.
        drop(probe);
        Ok(Some(dir.clone()))
    }
}

/// `[storage.staging]` — source-file staging policy.
///
/// Inert in the current engine: `enabled` parses and round-trips but no
/// staging copy runs yet. The block exists now so `clinker.toml` files that
/// pre-declare staging intent parse cleanly before the feature ships.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagingConfig {
    /// Whether source-file staging is enabled. Defaults to `false`.
    #[serde(default)]
    pub enabled: bool,
}

/// Failure modes when loading or validating `[storage]` configuration.
///
/// Carries enough context (the offending path, the underlying OS message)
/// for the CLI to render a `miette` diagnostic that names the exact
/// `clinker.toml` setting the operator must fix.
#[derive(Debug)]
pub enum StorageConfigError {
    /// `clinker.toml` exists but could not be read.
    Read { path: PathBuf, source: String },
    /// `clinker.toml` is not valid TOML, or a storage key has the wrong type.
    Parse(String),
    /// `storage.spill.dir` points at a path that does not exist.
    SpillDirMissing { path: PathBuf },
    /// `storage.spill.dir` exists but is a file, not a directory.
    SpillDirNotADirectory { path: PathBuf },
    /// `storage.spill.dir` exists and is a directory but cannot be written
    /// (permissions, read-only mount).
    SpillDirNotWritable { path: PathBuf, source: String },
}

impl std::fmt::Display for StorageConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Read { path, source } => {
                write!(f, "failed to read {}: {source}", path.display())
            }
            Self::Parse(msg) => write!(f, "invalid clinker.toml: {msg}"),
            Self::SpillDirMissing { path } => write!(
                f,
                "storage.spill.dir {} does not exist; create it or point at an existing volume",
                path.display()
            ),
            Self::SpillDirNotADirectory { path } => write!(
                f,
                "storage.spill.dir {} is a file, not a directory",
                path.display()
            ),
            Self::SpillDirNotWritable { path, source } => write!(
                f,
                "storage.spill.dir {} is not writable: {source}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for StorageConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_document_defaults_to_temp_dir_spill() {
        let doc = ClinkerToml::parse("").unwrap();
        assert!(doc.storage.spill.dir.is_none());
        assert!(!doc.storage.staging.enabled);
    }

    #[test]
    fn missing_clinker_toml_yields_defaults() {
        let empty = tempfile::tempdir().unwrap();
        let doc = ClinkerToml::load_from_workspace(empty.path()).unwrap();
        assert!(doc.storage.spill.dir.is_none());
    }

    #[test]
    fn storage_block_parses_spill_dir_and_staging() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.spill]
            dir = "/var/clinker/spill"

            [storage.staging]
            enabled = true
            "#,
        )
        .unwrap();
        assert_eq!(
            doc.storage.spill.dir.as_deref(),
            Some(Path::new("/var/clinker/spill"))
        );
        assert!(doc.storage.staging.enabled);
    }

    #[test]
    fn unknown_storage_key_is_rejected() {
        let err = ClinkerToml::parse(
            r#"
            [storage.spill]
            directory = "/typo/key"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn resolve_none_when_dir_absent() {
        let cfg = SpillConfig::default();
        assert!(cfg.resolve().unwrap().is_none());
    }

    #[test]
    fn resolve_ok_for_existing_writable_dir() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = SpillConfig {
            dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let resolved = cfg.resolve().unwrap();
        assert_eq!(resolved.as_deref(), Some(dir.path()));
    }

    #[test]
    fn resolve_errors_for_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        let cfg = SpillConfig {
            dir: Some(missing),
            ..Default::default()
        };
        assert!(matches!(
            cfg.resolve().unwrap_err(),
            StorageConfigError::SpillDirMissing { .. }
        ));
    }

    #[test]
    fn resolve_errors_when_path_is_a_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let cfg = SpillConfig {
            dir: Some(file.path().to_path_buf()),
            ..Default::default()
        };
        assert!(matches!(
            cfg.resolve().unwrap_err(),
            StorageConfigError::SpillDirNotADirectory { .. }
        ));
    }

    #[test]
    fn disk_cap_absent_yields_none() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.spill]
            dir = "/var/clinker/spill"
            "#,
        )
        .unwrap();
        assert_eq!(doc.storage.spill.disk_cap(), None);
    }

    #[test]
    fn disk_cap_parses_human_readable_string() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.spill]
            disk_cap_bytes = "10GB"
            "#,
        )
        .unwrap();
        // Decimal units, matching the ByteSize grammar used elsewhere:
        // 10 GB = 10_000_000_000 bytes.
        assert_eq!(doc.storage.spill.disk_cap(), Some(10_000_000_000));
    }

    #[test]
    fn disk_cap_parses_bare_integer_as_bytes() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.spill]
            disk_cap_bytes = 1048576
            "#,
        )
        .unwrap();
        assert_eq!(doc.storage.spill.disk_cap(), Some(1_048_576));
    }

    #[test]
    fn disk_cap_rejects_unparseable_size() {
        let err = ClinkerToml::parse(
            r#"
            [storage.spill]
            disk_cap_bytes = "ten gigabytes"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }
}
