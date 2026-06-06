//! Comprehensive executor-startup validation of the workspace `[storage]`
//! configuration.
//!
//! [`validate_storage_config`] is the single storage-config validator the run
//! passes through after the plan compiles and before any source-ingest thread
//! spawns. It is the *one* place that orchestrates the filesystem-type and
//! same-device rejections — there is no parallel probe elsewhere. The
//! per-volume physical questions ("is this tmpfs?", "is this the same device?")
//! are answered by the shared [`clinker_plan::config::classify`] /
//! [`clinker_plan::config::same_device`] facade so every storage check on
//! every platform shares one detection implementation.
//!
//! Two halves compose here without duplicating work:
//!
//! - **Directory validity + staging same-device** are owned by
//!   [`clinker_plan::config::StagingPolicy::validate`] (the spill side by
//!   [`clinker_plan::config::SpillConfig::resolve`]). This validator *reuses*
//!   those rather than re-implementing them, so the same-device check lives in
//!   exactly one orchestration.
//! - **Filesystem-class rejections, the spill-equals-staging rejection, and
//!   the free-space preflight** are new and owned here.
//!
//! All five rejections fail the run at startup — while it is still cheap to
//! abandon — rather than at the first spill or first staged copy, the lazy
//! trap DuckDB fell into when its temp-directory setting was honored only at
//! the first spill (duckdb/duckdb#9401).

use std::path::{Path, PathBuf};

use clinker_plan::config::{
    FsKind, StagingPolicy, StorageConfig, StorageConfigError, classify, same_device,
};

/// The validated, resolved storage decision plus any non-fatal preflight
/// advisory the startup pass produced.
#[derive(Debug)]
pub struct ResolvedStorage {
    /// The spill root directory the run should use, or `None` when no
    /// `storage.spill.dir` was configured (the OS-temp-dir default). Already
    /// proven to exist, be a writable directory, and sit on a durable local
    /// filesystem — the executor treats it as vetted.
    pub spill_root_dir: Option<PathBuf>,
    /// A free-space preflight advisory, present only when the spill volume's
    /// available space is below the run's estimated spill volume. The default
    /// policy is to *warn*, not abort, so this rides back to the caller (which
    /// logs it) rather than being raised as an error — the disk may still hold
    /// enough once spill compression and the streaming drain are accounted for,
    /// and the runtime disk cap (E320) / full-volume (E321) surfaces remain the
    /// hard backstops.
    pub free_space_warning: Option<FreeSpaceWarning>,
    /// A cap-headroom advisory, present only when a `storage.spill.disk_cap_bytes`
    /// is configured AND the run's estimated spill volume exceeds 80% of it. The
    /// run is likely to abort mid-stream with the spill-cap diagnostic (E320), so
    /// surfacing the signal at startup lets the operator raise the cap or reduce
    /// the footprint before committing. Advisory, not fatal — the estimate is a
    /// coarse upper bound, and a run that compresses well or never trips its
    /// memory budget may finish comfortably under the cap.
    pub cap_headroom_warning: Option<CapHeadroomWarning>,
}

/// A startup free-space preflight finding: the spill volume looks too small
/// for the run's estimated spill footprint.
///
/// Advisory by design — see [`ResolvedStorage::free_space_warning`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreeSpaceWarning {
    /// The spill root the preflight probed (the configured dir, or the OS temp
    /// dir when none was configured).
    pub spill_dir: PathBuf,
    /// Bytes available to a non-privileged user on the spill volume, as
    /// reported by the cross-platform `available_space` probe.
    pub available_bytes: u64,
    /// The run's coarse plan-time estimate of the bytes it could spill.
    pub estimated_spill_bytes: u64,
}

impl std::fmt::Display for FreeSpaceWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "W330: spill volume {} has {} bytes free but the run is estimated to spill \
             up to {} bytes; the run may abort with a full-volume error (E321) at the \
             final spill — point storage.spill.dir at a larger volume or reduce the spill \
             footprint (raise memory.limit, partition the input)",
            self.spill_dir.display(),
            self.available_bytes,
            self.estimated_spill_bytes,
        )
    }
}

/// The fraction of `storage.spill.disk_cap_bytes` above which a run's estimated
/// spill volume trips the [`CapHeadroomWarning`]. 80% is the headroom-warning
/// threshold #176 specifies — enough margin that the estimate's coarseness does
/// not constantly false-alarm, while still firing before a run that would
/// realistically blow the cap.
pub const CAP_HEADROOM_WARN_FRACTION: f64 = 0.80;

/// A startup cap-headroom finding: the run's estimated spill volume is within
/// 80% of (or above) the configured `storage.spill.disk_cap_bytes`, so it is
/// likely to abort with the spill-cap diagnostic (E320) mid-stream.
///
/// Advisory by design — see [`ResolvedStorage::cap_headroom_warning`]. The
/// figure is **per invocation**: under the partition-and-run model several
/// sibling `clinker` invocations can share one spill volume and one cap, so the
/// real headroom is smaller than any single invocation sees. The rendered
/// message states this so an operator running siblings does not read the
/// per-invocation headroom as the whole-volume headroom.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapHeadroomWarning {
    /// The configured cumulative spill cap, in bytes
    /// (`storage.spill.disk_cap_bytes`).
    pub disk_cap_bytes: u64,
    /// The run's coarse plan-time estimate of the bytes it could spill.
    pub estimated_spill_bytes: u64,
}

impl std::fmt::Display for CapHeadroomWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let pct = if self.disk_cap_bytes == 0 {
            0.0
        } else {
            (self.estimated_spill_bytes as f64 / self.disk_cap_bytes as f64) * 100.0
        };
        write!(
            f,
            "W331: this run is estimated to spill up to {} bytes, which is {:.0}% of the \
             configured spill cap storage.spill.disk_cap_bytes ({} bytes); the run may abort \
             with a spill-cap error (E320) before it finishes — raise disk_cap_bytes or reduce \
             the spill footprint (raise memory.limit, partition the input). This headroom is \
             per invocation: if you partition the input and run several clinker invocations \
             against the same spill volume and cap, they share the cap, so the real headroom \
             is smaller than this figure",
            self.estimated_spill_bytes, pct, self.disk_cap_bytes,
        )
    }
}

/// Failure modes the startup storage-config validation rejects.
///
/// Each variant renders with a stable diagnostic code, the offending
/// `clinker.toml` field, and a `clinker explain --code <CODE>` pointer. The
/// directory-validity and staging-pattern failures the reused
/// [`StagingPolicy::validate`] / [`SpillConfig::resolve`] produce are carried
/// through unchanged as [`Self::Dir`].
#[derive(Debug)]
pub enum StorageValidationError {
    /// E330 — `storage.spill.dir` resolves to an in-memory filesystem (Linux
    /// tmpfs / ramfs, Windows RAM disk). Spilling there trades RSS for
    /// page-cache pressure without moving bytes off RAM, defeating the spill.
    SpillDirInMemory { path: PathBuf },
    /// E331 — `storage.spill.dir` resolves to a network / userspace-bridged
    /// filesystem (NFS / SMB / CIFS / FUSE). A spill target on a soft-mount is
    /// prone to silent truncation and mmap data loss, the failure modes
    /// staging exists to escape.
    SpillDirNetwork { path: PathBuf },
    /// E332 — `storage.staging.dir` resolves to a network filesystem. Staging
    /// *onto* a network share reintroduces the very fragility staging is meant
    /// to escape.
    StagingDirNetwork { path: PathBuf },
    /// E333 — `storage.staging.dir` sits on the same physical device as a
    /// matched (staged) source, so the copy moves no I/O off the source
    /// volume. Reuses the same-device decision
    /// [`StagingPolicy::validate`] makes.
    StagingSameDevice {
        staging_dir: PathBuf,
        source: PathBuf,
    },
    /// E334 — `storage.spill.dir` and `storage.staging.dir` resolve to the
    /// same directory, so spill files and staged source copies would contend
    /// for the same space and the same cleanup. They must be distinct paths.
    SpillEqualsStaging { dir: PathBuf },
    /// A directory-validity or pattern failure produced by the reused
    /// [`StagingPolicy::validate`] / [`SpillConfig::resolve`] checks, carried
    /// through so the run surfaces the single message that check already
    /// renders.
    Dir(StorageConfigError),
    /// A filesystem probe (`classify` / `same_device`) failed against a path
    /// that should be probeable at startup. Names the path and the OS reason.
    Probe { path: PathBuf, source: String },
}

impl std::fmt::Display for StorageValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SpillDirInMemory { path } => write!(
                f,
                "E330 storage.spill.dir {} is on an in-memory filesystem (tmpfs/ramdisk); \
                 spilling there keeps the bytes in RAM and defeats the memory budget — point \
                 storage.spill.dir at a path on a real block device. See: clinker explain --code E330",
                path.display(),
            ),
            Self::SpillDirNetwork { path } => write!(
                f,
                "E331 storage.spill.dir {} is on a network filesystem (NFS/SMB/CIFS/FUSE); \
                 a spill target on a soft-mounted share risks silent truncation and data loss — \
                 point storage.spill.dir at a local disk. See: clinker explain --code E331",
                path.display(),
            ),
            Self::StagingDirNetwork { path } => write!(
                f,
                "E332 storage.staging.dir {} is on a network filesystem (NFS/SMB/CIFS/FUSE); \
                 staging onto a network share reintroduces the fragility staging exists to escape — \
                 point storage.staging.dir at a local disk. See: clinker explain --code E332",
                path.display(),
            ),
            Self::StagingSameDevice {
                staging_dir,
                source,
            } => write!(
                f,
                "E333 storage.staging.dir {} is on the same physical device as staged source {}; \
                 staging onto the same device copies bytes without moving I/O off the source volume — \
                 point storage.staging.dir at a local disk on a different device. \
                 See: clinker explain --code E333",
                staging_dir.display(),
                source.display(),
            ),
            Self::SpillEqualsStaging { dir } => write!(
                f,
                "E334 storage.spill.dir and storage.staging.dir both resolve to {}; \
                 spill files and staged source copies must not share a directory — give them \
                 distinct paths. See: clinker explain --code E334",
                dir.display(),
            ),
            Self::Dir(e) => write!(f, "{e}"),
            Self::Probe { path, source } => write!(
                f,
                "failed to probe the storage volume for {}: {source}",
                path.display(),
            ),
        }
    }
}

impl std::error::Error for StorageValidationError {}

/// Validate the workspace `[storage]` config at executor startup.
///
/// Runs after the plan compiles and before any source-ingest thread spawns.
/// `source_paths` is every discovered source path (matched or not); the
/// staging same-device rule resolves which are *matched* internally through
/// the policy's own pattern matcher. `estimated_spill_bytes` is the run's
/// plan-time spill-volume estimate (`0` when unknown), used only by the
/// non-fatal free-space preflight.
///
/// On success returns the resolved spill root (vetted: exists, writable,
/// durable, local) and any free-space advisory. The five hard rejections —
/// spill on tmpfs (E330), spill on a network FS (E331), staging on a network
/// FS (E332), staging same-device as a staged source (E333), and spill dir ==
/// staging dir (E334) — abort the run.
///
/// # Errors
///
/// Returns the [`StorageValidationError`] for the first rejection, or a
/// carried-through directory-validity / pattern error from the reused
/// [`StagingPolicy::validate`] / [`SpillConfig::resolve`] checks.
pub fn validate_storage_config(
    storage: &StorageConfig,
    source_paths: &[PathBuf],
    estimated_spill_bytes: u64,
) -> Result<ResolvedStorage, StorageValidationError> {
    // Spill-dir validity (exists / is-a-dir / writable) is owned by
    // SpillConfig::resolve; reuse it so there is one spill-dir validator. It
    // returns the resolved dir or `None` for the OS-temp-dir default.
    let spill_root_dir = storage
        .spill
        .resolve()
        .map_err(StorageValidationError::Dir)?;

    // Filesystem-class rejection for the spill dir. Only a configured dir is
    // probed; the OS-temp-dir default is the operator's own machine and is not
    // second-guessed (a developer with a tmpfs /tmp who set no dir keeps the
    // historical behavior rather than being blocked).
    if let Some(dir) = spill_root_dir.as_deref()
        && let Some(rejection) = spill_fs_rejection(probe_kind(dir)?, dir)
    {
        return Err(rejection);
    }

    // Staging-dir validity + pattern + same-device are owned by
    // StagingPolicy::validate; reuse it (a no-op when staging is disabled) so
    // the same-device check lives in exactly one orchestration. Its
    // StagingSameVolume variant is the staged-source same-device rejection,
    // re-presented here with the E333 code; every other variant carries
    // through unchanged.
    if let Err(e) = storage.staging.validate(source_paths) {
        return Err(lift_staging_error(e));
    }

    // Filesystem-class rejection for the staging dir, plus the spill ==
    // staging guard. Both only apply when staging is enabled with a dir set
    // (StagingPolicy::validate above already required the dir when enabled).
    if storage.staging.enabled
        && let Some(staging_dir) = staging_dir(&storage.staging)
    {
        if let Some(rejection) = staging_fs_rejection(probe_kind(staging_dir)?, staging_dir) {
            return Err(rejection);
        }
        // Spill == staging: both directories are configured and resolve to the
        // same device-and-inode. A configured spill dir and a configured
        // staging dir pointed at one path would interleave spill files with
        // staged copies under one cleanup, so reject it. Compared by
        // same_device + path equality after canonicalization so a relative and
        // an absolute spelling of one dir still collide.
        if let Some(spill_dir) = spill_root_dir.as_deref()
            && dirs_are_same(spill_dir, staging_dir)?
        {
            return Err(StorageValidationError::SpillEqualsStaging {
                dir: staging_dir.to_path_buf(),
            });
        }
    }

    // Free-space preflight (advisory). Probe the spill volume — the configured
    // dir, or the OS temp dir when none was configured — and warn when its
    // available space is below the estimated spill footprint. Skipped when the
    // estimate is unknown (`0`): with no on-disk seed there is nothing to
    // compare against.
    let free_space_warning = if estimated_spill_bytes > 0 {
        let probe_dir: PathBuf = spill_root_dir.clone().unwrap_or_else(std::env::temp_dir);
        free_space_preflight(&probe_dir, estimated_spill_bytes)?
    } else {
        None
    };

    // Cap-headroom preflight (advisory). When a cumulative spill cap is
    // configured and the run's estimate is within 80% of it, warn at startup so
    // the operator sees the signal before the run aborts with E320 mid-stream.
    // Skipped when the estimate is unknown (`0`) or no cap is set (unlimited
    // spill has no headroom to overrun).
    let cap_headroom_warning =
        cap_headroom_preflight(storage.spill.disk_cap(), estimated_spill_bytes);

    Ok(ResolvedStorage {
        spill_root_dir,
        free_space_warning,
        cap_headroom_warning,
    })
}

/// Produce a [`CapHeadroomWarning`] when a cumulative spill cap is configured
/// and the run's estimated spill volume reaches 80% of it.
///
/// Pure over its two inputs (no filesystem probe), so the warn/no-warn boundary
/// is testable without a real volume. Returns `None` when no cap is set, the
/// estimate is unknown (`0`), or the estimate sits below the threshold.
fn cap_headroom_preflight(
    disk_cap_bytes: Option<u64>,
    estimated_spill_bytes: u64,
) -> Option<CapHeadroomWarning> {
    let cap = disk_cap_bytes?;
    if cap == 0 || estimated_spill_bytes == 0 {
        return None;
    }
    let threshold = (cap as f64 * CAP_HEADROOM_WARN_FRACTION) as u64;
    if estimated_spill_bytes >= threshold {
        Some(CapHeadroomWarning {
            disk_cap_bytes: cap,
            estimated_spill_bytes,
        })
    } else {
        None
    }
}

/// The configured staging directory, if `enabled` set one. The `enabled`
/// gate is the caller's; this only unwraps the optional `dir`.
fn staging_dir(policy: &StagingPolicy) -> Option<&Path> {
    policy.dir.as_deref()
}

/// Classify `path` through the shared facade, mapping a probe failure to a
/// startup-validation error that names the path.
fn probe_kind(path: &Path) -> Result<FsKind, StorageValidationError> {
    classify(path).map_err(|e| StorageValidationError::Probe {
        path: path.to_path_buf(),
        source: e.to_string(),
    })
}

/// Map a spill directory's filesystem class to its rejection, if any.
///
/// In-memory ⇒ E330 (spilling keeps bytes in RAM); network ⇒ E331 (a spill
/// target on a soft-mount risks truncation); local ⇒ accepted. Pure mapping
/// over the already-probed [`FsKind`], so the reject/allow decision is
/// testable without a real tmpfs / network mount.
fn spill_fs_rejection(kind: FsKind, dir: &Path) -> Option<StorageValidationError> {
    match kind {
        FsKind::InMemory => Some(StorageValidationError::SpillDirInMemory {
            path: dir.to_path_buf(),
        }),
        FsKind::Network => Some(StorageValidationError::SpillDirNetwork {
            path: dir.to_path_buf(),
        }),
        FsKind::Local => None,
    }
}

/// Map a staging directory's filesystem class to its rejection, if any.
///
/// Network ⇒ E332 (staging onto a share reintroduces the fragility staging
/// escapes); in-memory and local ⇒ accepted (a staging dir on tmpfs is
/// pointless but not dangerous, and the same-device rule already guards the
/// no-I/O-moved case). Pure mapping over the already-probed [`FsKind`].
fn staging_fs_rejection(kind: FsKind, dir: &Path) -> Option<StorageValidationError> {
    match kind {
        FsKind::Network => Some(StorageValidationError::StagingDirNetwork {
            path: dir.to_path_buf(),
        }),
        FsKind::InMemory | FsKind::Local => None,
    }
}

/// Whether two directories resolve to the same physical location — same device
/// *and* same canonical path. `same_device` alone is too coarse (two distinct
/// dirs on one volume share a device); pairing it with a canonical-path
/// compare catches the "both keys point at one directory" case without
/// rejecting two sibling dirs on the same disk.
fn dirs_are_same(a: &Path, b: &Path) -> Result<bool, StorageValidationError> {
    let same_dev = same_device(a, b).map_err(|e| StorageValidationError::Probe {
        path: a.to_path_buf(),
        source: e.to_string(),
    })?;
    if !same_dev {
        return Ok(false);
    }
    let canon = |p: &Path| -> Result<PathBuf, StorageValidationError> {
        std::fs::canonicalize(p).map_err(|e| StorageValidationError::Probe {
            path: p.to_path_buf(),
            source: e.to_string(),
        })
    };
    Ok(canon(a)? == canon(b)?)
}

/// Re-present the `StagingSameVolume` variant from the reused
/// [`StagingPolicy::validate`] as the coded E333 rejection; carry every other
/// directory-validity / pattern variant through unchanged.
fn lift_staging_error(e: StorageConfigError) -> StorageValidationError {
    match e {
        StorageConfigError::StagingSameVolume {
            staging_dir,
            source,
        } => StorageValidationError::StagingSameDevice {
            staging_dir,
            source,
        },
        other => StorageValidationError::Dir(other),
    }
}

/// Query free bytes on the volume backing `dir` and return a warning when it
/// is below `estimated_spill_bytes`.
///
/// `fs4::available_space` returns a `u64` of bytes available to a
/// non-privileged user (it normalizes `statvfs`'s `f_bavail * f_frsize` on
/// unix and `GetDiskFreeSpaceExW`'s available-to-caller figure on Windows), so
/// the historical 32-bit `f_bavail` truncation never reaches this layer — the
/// byte count is already widened to 64 bits inside the crate. The comparison
/// stays in `u64` end-to-end with no `as u32` / `as usize` narrowing.
fn free_space_preflight(
    dir: &Path,
    estimated_spill_bytes: u64,
) -> Result<Option<FreeSpaceWarning>, StorageValidationError> {
    let available_bytes = fs4::available_space(dir).map_err(|e| StorageValidationError::Probe {
        path: dir.to_path_buf(),
        source: e.to_string(),
    })?;
    Ok(if available_bytes < estimated_spill_bytes {
        Some(FreeSpaceWarning {
            spill_dir: dir.to_path_buf(),
            available_bytes,
            estimated_spill_bytes,
        })
    } else {
        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_plan::config::SpillConfig;

    fn storage_with(spill: SpillConfig, staging: StagingPolicy) -> StorageConfig {
        StorageConfig { spill, staging }
    }

    #[test]
    fn local_spill_dir_validates_clean() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage_with(
            SpillConfig {
                dir: Some(dir.path().to_path_buf()),
                ..Default::default()
            },
            StagingPolicy::default(),
        );
        let resolved = validate_storage_config(&storage, &[], 0).unwrap();
        assert_eq!(resolved.spill_root_dir.as_deref(), Some(dir.path()));
        assert!(resolved.free_space_warning.is_none());
    }

    #[test]
    fn no_dir_resolves_to_temp_default() {
        let storage = StorageConfig::default();
        let resolved = validate_storage_config(&storage, &[], 0).unwrap();
        assert!(resolved.spill_root_dir.is_none());
    }

    #[test]
    fn spill_missing_dir_carries_through() {
        let dir = tempfile::tempdir().unwrap();
        let storage = storage_with(
            SpillConfig {
                dir: Some(dir.path().join("does-not-exist")),
                ..Default::default()
            },
            StagingPolicy::default(),
        );
        assert!(matches!(
            validate_storage_config(&storage, &[], 0).unwrap_err(),
            StorageValidationError::Dir(StorageConfigError::SpillDirMissing { .. })
        ));
    }

    #[test]
    fn staging_same_device_is_e333() {
        // A matched source on the staging dir's own device is rejected; the
        // reused StagingPolicy::validate produces StagingSameVolume, which the
        // validator re-presents as the coded same-device rejection.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("orders.csv");
        std::fs::write(&src, b"a,b\n").unwrap();
        let staging = StagingPolicy {
            enabled: true,
            dir: Some(dir.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        };
        let storage = storage_with(SpillConfig::default(), staging);
        let err = validate_storage_config(&storage, &[src], 0).unwrap_err();
        assert!(
            matches!(err, StorageValidationError::StagingSameDevice { .. }),
            "expected E333 same-device, got {err}"
        );
        assert!(err.to_string().contains("E333"));
    }

    #[test]
    fn free_space_warning_when_estimate_exceeds_available() {
        // Estimating an absurd spill volume guarantees the real tempdir's
        // free space is below it, so the advisory fires.
        let dir = tempfile::tempdir().unwrap();
        let storage = storage_with(
            SpillConfig {
                dir: Some(dir.path().to_path_buf()),
                ..Default::default()
            },
            StagingPolicy::default(),
        );
        let resolved = validate_storage_config(&storage, &[], u64::MAX).unwrap();
        let warning = resolved
            .free_space_warning
            .expect("an exabyte spill estimate must trip the free-space preflight");
        assert_eq!(warning.estimated_spill_bytes, u64::MAX);
        assert!(warning.to_string().contains("W330"));
    }

    #[test]
    fn spill_fs_rejection_maps_each_class() {
        let p = Path::new("/x");
        assert!(matches!(
            spill_fs_rejection(FsKind::InMemory, p),
            Some(StorageValidationError::SpillDirInMemory { .. })
        ));
        assert!(matches!(
            spill_fs_rejection(FsKind::Network, p),
            Some(StorageValidationError::SpillDirNetwork { .. })
        ));
        assert!(spill_fs_rejection(FsKind::Local, p).is_none());
    }

    #[test]
    fn staging_fs_rejection_only_rejects_network() {
        let p = Path::new("/x");
        assert!(matches!(
            staging_fs_rejection(FsKind::Network, p),
            Some(StorageValidationError::StagingDirNetwork { .. })
        ));
        // A staging dir on tmpfs is pointless but not dangerous, and a local
        // dir is the happy path — neither is rejected on filesystem class.
        assert!(staging_fs_rejection(FsKind::InMemory, p).is_none());
        assert!(staging_fs_rejection(FsKind::Local, p).is_none());
    }

    #[test]
    fn spill_equals_staging_is_e334() {
        // Spill and staging both pointed at one directory are rejected. With no
        // matched source the same-device rule (E333) is a no-op, so E334 is the
        // sole rejection regardless of how the host lays out /tmp.
        let shared = tempfile::tempdir().unwrap();
        let storage = storage_with(
            SpillConfig {
                dir: Some(shared.path().to_path_buf()),
                ..Default::default()
            },
            StagingPolicy {
                enabled: true,
                dir: Some(shared.path().to_path_buf()),
                // A pattern that matches nothing in `source_paths`, so the
                // same-device check never runs and E334 stands alone.
                patterns: vec!["*.never-matches".into()],
                ..Default::default()
            },
        );
        let err = validate_storage_config(&storage, &[], 0).unwrap_err();
        assert!(
            matches!(err, StorageValidationError::SpillEqualsStaging { .. }),
            "expected E334 spill-equals-staging, got {err}"
        );
        assert!(err.to_string().contains("E334"));
    }

    #[test]
    fn cap_headroom_warns_at_or_above_80_percent() {
        // 8 GB estimate against a 10 GB cap is exactly 80% → warn.
        let w = cap_headroom_preflight(Some(10_000_000_000), 8_000_000_000)
            .expect("80% of the cap must trip the headroom warning");
        assert_eq!(w.disk_cap_bytes, 10_000_000_000);
        assert_eq!(w.estimated_spill_bytes, 8_000_000_000);
        let msg = w.to_string();
        assert!(
            msg.contains("W331"),
            "message must carry the W331 code: {msg}"
        );
        // enh-6 #311: the per-invocation disclaimer must be present so an
        // operator running siblings does not misread the headroom.
        assert!(
            msg.contains("per invocation"),
            "the headroom must state it is per-invocation: {msg}"
        );

        // An estimate above the cap warns just as well.
        assert!(cap_headroom_preflight(Some(10_000_000_000), 11_000_000_000).is_some());
    }

    #[test]
    fn cap_headroom_silent_below_threshold_or_when_no_cap() {
        // 7 GB against a 10 GB cap is 70% → below the 80% threshold, no warn.
        assert!(cap_headroom_preflight(Some(10_000_000_000), 7_000_000_000).is_none());
        // No cap configured → unlimited spill has no headroom to overrun.
        assert!(cap_headroom_preflight(None, 8_000_000_000).is_none());
        // Unknown estimate (0) → nothing to compare.
        assert!(cap_headroom_preflight(Some(10_000_000_000), 0).is_none());
    }

    #[test]
    fn validate_surfaces_cap_headroom_warning() {
        // Drive the full validator: a tiny cap with a large estimate trips the
        // headroom advisory without aborting the run.
        let dir = tempfile::tempdir().unwrap();
        let storage = storage_with(
            SpillConfig {
                dir: Some(dir.path().to_path_buf()),
                disk_cap_bytes: Some(clinker_plan::config::ByteSize(1_000)),
                ..Default::default()
            },
            StagingPolicy::default(),
        );
        // 2000-byte estimate vs a 1000-byte cap = 200% → warn. Use a small
        // estimate so the free-space preflight on the real tempdir does not
        // also fire and obscure the cap-headroom assertion.
        let resolved = validate_storage_config(&storage, &[], 2_000).unwrap();
        let warning = resolved
            .cap_headroom_warning
            .expect("an estimate above the cap must trip the cap-headroom warning");
        assert_eq!(warning.disk_cap_bytes, 1_000);
        assert_eq!(warning.estimated_spill_bytes, 2_000);
    }

    #[test]
    fn no_warning_when_estimate_fits() {
        // A 1-byte estimate fits on any real volume, so no advisory.
        let dir = tempfile::tempdir().unwrap();
        let storage = storage_with(
            SpillConfig {
                dir: Some(dir.path().to_path_buf()),
                ..Default::default()
            },
            StagingPolicy::default(),
        );
        let resolved = validate_storage_config(&storage, &[], 1).unwrap();
        assert!(resolved.free_space_warning.is_none());
    }
}
