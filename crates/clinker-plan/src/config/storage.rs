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
//! The spill root directory and the staging block are both honored at
//! runtime. This module owns the *decision and validation* surface: it parses
//! the `[storage.staging]` block, validates it at startup
//! ([`StagingPolicy::validate`]), and matches source paths against the
//! configured patterns ([`StagingPolicy::pattern_matches`]). The copy itself —
//! single-pass streamed copy with inline BLAKE3 verification and atomic
//! publish — lives in `clinker-channel`'s staging-copy engine, which consumes
//! the decision this module makes.

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
    /// before execution. Off by default; activated per-pipeline by pattern
    /// match. Validated at startup, then driven by `clinker-channel`'s
    /// staging-copy engine per matched source.
    #[serde(default)]
    pub staging: StagingPolicy,
}

/// How spill files are compressed: `auto` (the default), `off`, or `on`.
///
/// Spill bodies are postcard-encoded record streams. LZ4 frame compression
/// shrinks large spilled runs, but on small spills the per-frame fixed cost
/// — clearing the compressor's internal state on every frame reset — can
/// dominate the byte savings. The LZ4 v1.8.2 release notes call this out
/// directly, and Pentaho Kettle ships explicit guidance to disable spill
/// compression for small rows. `Auto` encodes that guidance as a heuristic
/// so the common case needs no tuning; `Off` / `On` force the choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressMode {
    /// Compress only when the projected spill is large enough that LZ4's
    /// per-frame fixed cost is amortized (see [`CompressMode::resolve`]).
    #[default]
    Auto,
    /// Never compress: postcard records are written straight to disk with no
    /// LZ4 frame wrapping. Cheapest for small spills; largest on-disk size.
    Off,
    /// Always compress with an LZ4 frame, regardless of projected size. The
    /// pre-knob behavior, kept for spills of large, compressible rows.
    On,
}

/// Minimum projected bytes-per-batch at which `auto` enables compression.
///
/// Below ~4 KiB a spilled batch fits inside a single LZ4 block, so the frame
/// reset cost the v1.8.2 release notes flag is paid with little compressible
/// volume to offset it. 4 KiB matches the small-row threshold Pentaho Kettle
/// documents.
const AUTO_COMPRESS_MIN_BYTES_PER_BATCH: u64 = 4 * 1024;

/// Minimum projected rows-per-batch at which `auto` enables compression.
///
/// Pairs with [`AUTO_COMPRESS_MIN_BYTES_PER_BATCH`]: a batch must be both
/// wide (≥ 4 KiB) and tall (≥ 1024 rows) before compression pays for itself.
/// 1024 is the small-row row-count threshold from the same Pentaho guidance.
const AUTO_COMPRESS_MIN_ROWS_PER_BATCH: u64 = 1024;

/// Per-column byte estimate used to project a spilled batch's size from a
/// schema's column count alone.
///
/// A `Value` slot is 24 bytes; `32` adds a small heap allowance for the
/// typical mix of short strings and fixed-width scalars a spilled row holds.
/// The projection only has to land on the correct side of the 4 KiB
/// threshold, so a coarse per-column constant is sufficient and keeps the
/// heuristic a pure function of the schema width and batch size.
const ESTIMATED_BYTES_PER_COLUMN: u64 = 32;

impl CompressMode {
    /// Resolve this mode against a projected spill batch's size into a
    /// concrete "compress this file?" decision.
    ///
    /// `On` and `Off` ignore the projection. `Auto` compresses only when the
    /// batch is projected to be both ≥ 4 KiB and ≥ 1024 rows — the point at
    /// which LZ4's per-frame fixed cost is amortized by enough compressible
    /// volume (see [`CompressMode`]).
    pub fn resolve(self, projected_bytes_per_batch: u64, projected_rows_per_batch: u64) -> bool {
        match self {
            CompressMode::On => true,
            CompressMode::Off => false,
            CompressMode::Auto => {
                projected_bytes_per_batch >= AUTO_COMPRESS_MIN_BYTES_PER_BATCH
                    && projected_rows_per_batch >= AUTO_COMPRESS_MIN_ROWS_PER_BATCH
            }
        }
    }

    /// Project a spilled batch's byte size from its schema width and the
    /// configured rows-per-batch, then resolve to a compression decision.
    ///
    /// Convenience over [`CompressMode::resolve`] for callers that hold a
    /// column count and batch size rather than a pre-computed byte figure:
    /// the projection is `column_count × 32 bytes × rows_per_batch`. The
    /// `--explain` plan and the runtime spill writer call this so the
    /// reported mode matches the mode the run actually applies.
    pub fn resolve_for_schema(self, column_count: usize, rows_per_batch: u64) -> bool {
        let bytes_per_row = column_count as u64 * ESTIMATED_BYTES_PER_COLUMN;
        let bytes_per_batch = bytes_per_row.saturating_mul(rows_per_batch);
        self.resolve(bytes_per_batch, rows_per_batch)
    }

    /// Lowercase mode label for JSON output, matching the YAML surface
    /// grammar (`auto` / `off` / `on`). The text `--explain` path renders
    /// the `Debug` form (`Auto` / `Off` / `On`); JSON consumers expect the
    /// wire grammar they would write back, so the two surfaces differ in
    /// case by design.
    pub fn json_label(self) -> &'static str {
        match self {
            CompressMode::Auto => "auto",
            CompressMode::Off => "off",
            CompressMode::On => "on",
        }
    }
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
    /// Whether spill files are LZ4-compressed. Defaults to [`CompressMode::Auto`],
    /// which compresses only when a spilled batch is projected large enough
    /// to amortize LZ4's per-frame fixed cost. `off` and `on` force the
    /// choice. See [`CompressMode`] for the rationale and threshold.
    #[serde(default)]
    pub compress: CompressMode,
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
/// When `enabled`, source files whose path matches one of `patterns` are
/// copied to local disk under `dir` before the pipeline reads them, so a
/// run over a flaky network share (NFS soft-mount, SMB) reads from a stable
/// local copy instead. This mirrors the NiFi `FetchFile` / Airbyte
/// `smart_open` posture: decide-what-to-stage is separated from do-the-copy.
///
/// This type owns the *decision* and *validation* halves:
/// [`StagingPolicy::pattern_matches`] reports whether a path is selected, and
/// [`StagingPolicy::validate`] runs the startup checks. The copy half — the
/// streamed copy + BLAKE3 verify + atomic publish — lives in `clinker-channel`.
/// Off by default: an empty or absent block leaves every source reading in
/// place exactly as before.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StagingPolicy {
    /// Whether source-file staging is enabled. Defaults to `false`, in which
    /// case `patterns` is ignored and every source reads in place.
    #[serde(default)]
    pub enabled: bool,
    /// Local directory the staged copies are written under. Required when
    /// `enabled` — validated at startup to exist, be writable, and sit on a
    /// different volume than every matched source (see
    /// [`StagingPolicy::validate`]). `None` with `enabled = true` is a
    /// startup error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dir: Option<PathBuf>,
    /// Cumulative cap on bytes copied into the staging dir for one run, in
    /// bytes. `None` (omitted) → unlimited. Accepts a bare integer or a
    /// human-readable size (`"50GB"`) through the same [`ByteSize`] grammar
    /// the spill cap uses. Charged by the copy engine before each file is
    /// copied, so a file that would push the run over the cap is refused
    /// rather than copied and rolled back.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_cap_bytes: Option<ByteSize>,
    /// Whether a staged copy is integrity-checked against its source after
    /// the copy. Defaults to [`StagingVerify::Blake3`] — a BLAKE3 digest of
    /// source and copy must match, catching the silent truncation an NFS
    /// soft-mount can produce. Consumed by the copy engine.
    #[serde(default)]
    pub verify: StagingVerify,
    /// What to do when a staging-dir copy with the target name already
    /// exists (e.g. a prior crashed run). Defaults to
    /// [`OnExisting::Overwrite`]. Consumed by the copy engine.
    #[serde(default)]
    pub on_existing: OnExisting,
    /// Whether staged copies are removed when the run finishes. Defaults to
    /// [`Cleanup::OnSuccess`] — keep copies after a failure so a re-run can
    /// inspect them, delete them after a clean run. Consumed by the copy
    /// engine.
    #[serde(default)]
    pub cleanup: Cleanup,
    /// Glob patterns selecting which source paths are staged. A source is
    /// staged only when `enabled` and its path matches at least one pattern.
    /// Matched with POSIX/gitignore semantics via the `glob` crate — the
    /// same matcher the source-discovery `exclude:` list uses — against both
    /// the full path and its basename. Empty (the default) ⇒ no source
    /// matches, so `enabled = true` with no patterns stages nothing.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub patterns: Vec<String>,
}

/// Post-copy integrity check applied to a staged file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StagingVerify {
    /// Hash source and copy with BLAKE3 and require the digests to match.
    /// The default: an NFS soft-mount can silently truncate a read, and a
    /// content digest is the only check that catches it (a size match does
    /// not). `blake3` reuses the workspace's existing BLAKE3 dependency.
    #[default]
    Blake3,
    /// Skip the post-copy check. Faster, but a truncated or corrupted copy
    /// passes unnoticed — only sensible on a transport already trusted to
    /// deliver complete bytes.
    None,
}

/// What to do when the staging destination already holds a file with the
/// target name (typically left by an earlier crashed run).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnExisting {
    /// Replace the existing file with a fresh copy. The default: a partial
    /// copy from a crashed run must not be trusted, so it is overwritten.
    #[default]
    Overwrite,
    /// Reuse the existing file without re-copying. Trades a re-copy for the
    /// risk of reusing a partial file; only safe when paired with a
    /// post-copy `verify`.
    Reuse,
    /// Fail the run rather than touch an existing destination.
    Error,
}

/// When staged copies are deleted relative to run outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Cleanup {
    /// Delete staged copies after a successful run; keep them after a
    /// failure so the inputs a failed run saw can be inspected and re-run
    /// without re-fetching. The default.
    #[default]
    OnSuccess,
    /// Always delete staged copies when the run ends, success or failure.
    Always,
    /// Never delete staged copies; the operator reclaims the staging dir.
    Never,
}

/// A source path after staging resolution.
///
/// `original` is the path the pipeline author wrote; `staged` is the local
/// copy the reader should open instead, or `None` when the source reads in
/// place (staging disabled or no pattern match). Threading this through the
/// reader keeps the read side agnostic to whether staging happened: it opens
/// [`StagedPath::read_path`] regardless.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StagedPath {
    /// The path declared in the pipeline (the network-share path when
    /// staging is in play).
    pub original: PathBuf,
    /// The local staged copy to read instead, or `None` to read `original`
    /// in place.
    pub staged: Option<PathBuf>,
}

impl StagedPath {
    /// A path that reads in place — `staged` is `None`, so the reader opens
    /// `original` directly.
    pub fn in_place(original: PathBuf) -> Self {
        Self {
            original,
            staged: None,
        }
    }

    /// The path the reader should actually open: the staged copy when one
    /// exists, otherwise the original.
    pub fn read_path(&self) -> &Path {
        self.staged.as_deref().unwrap_or(&self.original)
    }
}

impl StagingPolicy {
    /// Whether `path` matches at least one staging pattern.
    ///
    /// Returns `false` when staging is disabled or no pattern is configured.
    /// Patterns are matched against both the full path string and the
    /// basename (gitignore-style), mirroring the source-discovery
    /// `exclude:` matcher, so `*.csv` matches a deep path by basename while
    /// `/mnt/nfs/**` matches by full path. An unparseable pattern never
    /// matches — pattern validity is reported separately by
    /// [`StagingPolicy::validate`] so a typo fails the run at startup rather
    /// than silently disabling staging here.
    pub fn pattern_matches(&self, path: &Path) -> bool {
        if !self.enabled {
            return false;
        }
        let path_str = path.to_string_lossy();
        let basename = path
            .file_name()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        self.patterns.iter().any(|p| match glob::Pattern::new(p) {
            Ok(pat) => pat.matches(&path_str) || pat.matches(&basename),
            Err(_) => false,
        })
    }

    /// Validate staging configuration at startup against the set of source
    /// paths a run will read.
    ///
    /// Validation runs once before any input is opened so a misconfigured
    /// staging dir fails the run immediately rather than at the first copy.
    /// When staging is disabled this is a no-op. When enabled it requires
    /// `dir` to be set, to exist as a writable directory, and to sit on a
    /// different volume than every *matched* source — staging onto the same
    /// volume copies bytes without moving I/O off the slow share, a
    /// well-documented anti-pattern. It also rejects an unparseable pattern
    /// so a glob typo surfaces here rather than silently matching nothing.
    ///
    /// # Errors
    ///
    /// Returns [`StorageConfigError::StagingDirUnset`] when enabled without a
    /// `dir`; [`StorageConfigError::StagingDirMissing`] /
    /// [`StorageConfigError::StagingDirNotADirectory`] /
    /// [`StorageConfigError::StagingDirNotWritable`] for a bad dir;
    /// [`StorageConfigError::StagingPatternInvalid`] for an unparseable
    /// pattern; and [`StorageConfigError::StagingSameVolume`] when the
    /// staging dir shares a volume with a matched source.
    pub fn validate(&self, source_paths: &[PathBuf]) -> Result<(), StorageConfigError> {
        if !self.enabled {
            return Ok(());
        }

        for p in &self.patterns {
            glob::Pattern::new(p).map_err(|e| StorageConfigError::StagingPatternInvalid {
                pattern: p.clone(),
                source: e.to_string(),
            })?;
        }

        let dir = self
            .dir
            .as_ref()
            .ok_or(StorageConfigError::StagingDirUnset)?;

        let meta = std::fs::metadata(dir).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageConfigError::StagingDirMissing { path: dir.clone() }
            } else {
                StorageConfigError::StagingDirNotWritable {
                    path: dir.clone(),
                    source: e.to_string(),
                }
            }
        })?;
        if !meta.is_dir() {
            return Err(StorageConfigError::StagingDirNotADirectory { path: dir.clone() });
        }
        // Probe writability with a real create-and-delete: directory mode
        // bits alone do not prove a write succeeds (read-only mount, ACLs).
        let probe = tempfile::Builder::new()
            .prefix(".clinker-staging-probe-")
            .tempfile_in(dir)
            .map_err(|e| StorageConfigError::StagingDirNotWritable {
                path: dir.clone(),
                source: e.to_string(),
            })?;
        drop(probe);

        // Same-volume refusal applies only to matched sources: a source the
        // patterns do not select is read in place regardless of where it
        // lives, so its volume is irrelevant. The same-device probe is the
        // shared filesystem-detection facade — the config layer and the
        // executor-startup checks resolve "same volume?" through one
        // implementation rather than each carrying its own.
        for src in source_paths {
            if !self.pattern_matches(src) {
                continue;
            }
            let same = crate::config::fs_type::same_device(dir, src).map_err(|e| {
                StorageConfigError::StagingDirNotWritable {
                    path: src.clone(),
                    source: e.to_string(),
                }
            })?;
            if same {
                return Err(StorageConfigError::StagingSameVolume {
                    staging_dir: dir.clone(),
                    source: src.clone(),
                });
            }
        }

        Ok(())
    }
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
    /// `storage.staging.enabled = true` but no `storage.staging.dir` was set.
    StagingDirUnset,
    /// `storage.staging.dir` points at a path that does not exist.
    StagingDirMissing { path: PathBuf },
    /// `storage.staging.dir` exists but is a file, not a directory.
    StagingDirNotADirectory { path: PathBuf },
    /// `storage.staging.dir` exists and is a directory but cannot be written,
    /// or a path's storage volume could not be determined.
    StagingDirNotWritable { path: PathBuf, source: String },
    /// A `storage.staging.patterns` entry is not a valid glob.
    StagingPatternInvalid { pattern: String, source: String },
    /// `storage.staging.dir` sits on the same volume as a matched source, so
    /// staging would copy bytes without moving I/O off the slow volume.
    StagingSameVolume {
        staging_dir: PathBuf,
        source: PathBuf,
    },
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
            Self::StagingDirUnset => write!(
                f,
                "storage.staging.enabled is true but storage.staging.dir is not set; \
                 set it to a local directory on a different volume than the staged sources"
            ),
            Self::StagingDirMissing { path } => write!(
                f,
                "storage.staging.dir {} does not exist; create it or point at an existing volume",
                path.display()
            ),
            Self::StagingDirNotADirectory { path } => write!(
                f,
                "storage.staging.dir {} is a file, not a directory",
                path.display()
            ),
            Self::StagingDirNotWritable { path, source } => write!(
                f,
                "storage.staging.dir {} is not writable: {source}",
                path.display()
            ),
            Self::StagingPatternInvalid { pattern, source } => write!(
                f,
                "storage.staging.patterns entry {pattern:?} is not a valid glob: {source}"
            ),
            Self::StagingSameVolume {
                staging_dir,
                source,
            } => write!(
                f,
                "storage.staging.dir {} is on the same volume as source {}; \
                 staging onto the same volume copies bytes without moving I/O off the \
                 source volume — point storage.staging.dir at a local disk on a different volume",
                staging_dir.display(),
                source.display()
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

    #[test]
    fn compress_defaults_to_auto() {
        let doc = ClinkerToml::parse("").unwrap();
        assert_eq!(doc.storage.spill.compress, CompressMode::Auto);
    }

    #[test]
    fn compress_parses_each_mode() {
        for (text, expected) in [
            ("auto", CompressMode::Auto),
            ("off", CompressMode::Off),
            ("on", CompressMode::On),
        ] {
            let doc =
                ClinkerToml::parse(&format!("[storage.spill]\ncompress = \"{text}\"\n")).unwrap();
            assert_eq!(doc.storage.spill.compress, expected, "mode {text}");
        }
    }

    #[test]
    fn compress_rejects_unknown_mode() {
        let err = ClinkerToml::parse(
            r#"
            [storage.spill]
            compress = "gzip"
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn resolve_on_off_ignore_projection() {
        // `on` / `off` are forced regardless of the projected batch size.
        assert!(CompressMode::On.resolve(0, 0));
        assert!(!CompressMode::Off.resolve(u64::MAX, u64::MAX));
    }

    #[test]
    fn resolve_auto_needs_both_thresholds() {
        // Both the byte and the row threshold must be met.
        assert!(CompressMode::Auto.resolve(4096, 1024));
        assert!(CompressMode::Auto.resolve(64 * 1024, 4096));
        // Wide but too few rows → no compression.
        assert!(!CompressMode::Auto.resolve(64 * 1024, 1023));
        // Many rows but too few bytes → no compression.
        assert!(!CompressMode::Auto.resolve(4095, 8192));
    }

    #[test]
    fn resolve_for_schema_projects_from_width_and_rows() {
        // 1 column × 32 B/col × 1024 rows = 32 KiB ≥ 4 KiB and rows ≥ 1024 →
        // compress.
        assert!(CompressMode::Auto.resolve_for_schema(1, 1024));
        // 8 columns × 32 B/col × 16 rows = 4 KiB of bytes but only 16 rows →
        // below the row threshold, so no compression.
        assert!(!CompressMode::Auto.resolve_for_schema(8, 16));
    }

    #[test]
    fn staging_defaults_are_off_and_safe() {
        let p = StagingPolicy::default();
        assert!(!p.enabled);
        assert!(p.dir.is_none());
        assert!(p.patterns.is_empty());
        assert_eq!(p.verify, StagingVerify::Blake3);
        assert_eq!(p.on_existing, OnExisting::Overwrite);
        assert_eq!(p.cleanup, Cleanup::OnSuccess);
    }

    #[test]
    fn staging_block_parses_all_knobs() {
        let doc = ClinkerToml::parse(
            r#"
            [storage.staging]
            enabled = true
            dir = "/var/clinker/staging"
            disk_cap_bytes = "50GB"
            verify = "none"
            on_existing = "reuse"
            cleanup = "always"
            patterns = ["/mnt/nfs/data/**", "*.csv"]
            "#,
        )
        .unwrap();
        let s = &doc.storage.staging;
        assert!(s.enabled);
        assert_eq!(s.dir.as_deref(), Some(Path::new("/var/clinker/staging")));
        assert_eq!(s.disk_cap_bytes.map(|ByteSize(n)| n), Some(50_000_000_000));
        assert_eq!(s.verify, StagingVerify::None);
        assert_eq!(s.on_existing, OnExisting::Reuse);
        assert_eq!(s.cleanup, Cleanup::Always);
        assert_eq!(s.patterns, vec!["/mnt/nfs/data/**", "*.csv"]);
    }

    #[test]
    fn unknown_staging_key_is_rejected() {
        let err = ClinkerToml::parse(
            r#"
            [storage.staging]
            enabledx = true
            "#,
        )
        .unwrap_err();
        assert!(matches!(err, StorageConfigError::Parse(_)));
    }

    #[test]
    fn pattern_matches_full_path_and_basename() {
        let p = StagingPolicy {
            enabled: true,
            patterns: vec!["/mnt/nfs/**".into(), "*.csv".into()],
            ..Default::default()
        };
        // Full-path match.
        assert!(p.pattern_matches(Path::new("/mnt/nfs/data/orders.json")));
        // Basename match on a path the full-path glob does not cover.
        assert!(p.pattern_matches(Path::new("/local/data/orders.csv")));
        // No match.
        assert!(!p.pattern_matches(Path::new("/local/data/orders.json")));
    }

    #[test]
    fn pattern_matches_is_false_when_disabled() {
        let p = StagingPolicy {
            enabled: false,
            patterns: vec!["**".into()],
            ..Default::default()
        };
        assert!(!p.pattern_matches(Path::new("/anything")));
    }

    #[test]
    fn staged_path_read_path_prefers_staged_copy() {
        let in_place = StagedPath::in_place(PathBuf::from("/a/b.csv"));
        assert_eq!(in_place.read_path(), Path::new("/a/b.csv"));
        let copied = StagedPath {
            original: PathBuf::from("/mnt/nfs/b.csv"),
            staged: Some(PathBuf::from("/local/b.csv")),
        };
        assert_eq!(copied.read_path(), Path::new("/local/b.csv"));
    }

    #[test]
    fn validate_noop_when_disabled() {
        let p = StagingPolicy::default();
        assert!(p.validate(&[PathBuf::from("/mnt/nfs/x.csv")]).is_ok());
    }

    #[test]
    fn validate_requires_dir_when_enabled() {
        let p = StagingPolicy {
            enabled: true,
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[]).unwrap_err(),
            StorageConfigError::StagingDirUnset
        ));
    }

    #[test]
    fn validate_rejects_missing_dir() {
        let dir = tempfile::tempdir().unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(dir.path().join("nope")),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[]).unwrap_err(),
            StorageConfigError::StagingDirMissing { .. }
        ));
    }

    #[test]
    fn validate_rejects_dir_that_is_a_file() {
        let file = tempfile::NamedTempFile::new().unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(file.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[]).unwrap_err(),
            StorageConfigError::StagingDirNotADirectory { .. }
        ));
    }

    #[test]
    fn validate_rejects_invalid_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(dir.path().to_path_buf()),
            patterns: vec!["[".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[]).unwrap_err(),
            StorageConfigError::StagingPatternInvalid { .. }
        ));
    }

    #[test]
    fn validate_rejects_same_volume_matched_source() {
        // A matched source on the staging dir's own volume is refused (both
        // live under the same tempdir, hence the same device).
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("orders.csv");
        std::fs::write(&src, b"a,b\n").unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(dir.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(matches!(
            p.validate(&[src]).unwrap_err(),
            StorageConfigError::StagingSameVolume { .. }
        ));
    }

    #[test]
    fn validate_ignores_same_volume_unmatched_source() {
        // An unmatched source on the same volume is fine: it reads in place,
        // so the same-volume rule does not apply to it.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("orders.json");
        std::fs::write(&src, b"{}").unwrap();
        let p = StagingPolicy {
            enabled: true,
            dir: Some(dir.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        assert!(p.validate(&[src]).is_ok());
    }
}
