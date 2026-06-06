//! End-to-end coverage for `validate_storage_config`, the run-startup
//! storage-config validator.
//!
//! Each of the five rejections (E330–E334) plus the free-space preflight
//! (W330) is exercised against real directories. The filesystem-class
//! rejections that need a special mount (tmpfs for E330, a network share for
//! E331/E332) are platform-gated: the tmpfs case runs on Linux hosts that
//! expose `/dev/shm`, and the network case has no portable mount to create, so
//! its decision is proven by the crate's unit tests over the `fs_type` facade.
//! The same-device (E333), spill-equals-staging (E334), and free-space cases
//! run on every platform from ordinary temp directories.

use clinker_exec::executor::{StorageValidationError, validate_storage_config};
use clinker_plan::config::{SpillConfig, StagingPolicy, StorageConfig};
use std::path::PathBuf;

fn storage(spill: SpillConfig, staging: StagingPolicy) -> StorageConfig {
    StorageConfig { spill, staging }
}

/// E330 — a spill dir on tmpfs is rejected. Linux-only: `/dev/shm` is the
/// portable tmpfs mount present on essentially every Linux host; macOS has no
/// native tmpfs and Windows exposes none in CI, so the rejection's decision
/// logic is covered by the crate's `spill_fs_rejection` unit test on those
/// platforms.
#[test]
#[cfg(target_os = "linux")]
fn e330_spill_dir_on_tmpfs_is_rejected() {
    let shm = std::path::Path::new("/dev/shm");
    if !shm.is_dir() {
        // Some minimal containers omit /dev/shm; nothing to assert there.
        eprintln!("skipping e330: /dev/shm not present");
        return;
    }
    // Confirm /dev/shm is actually tmpfs before relying on it; if a host
    // mounted it as something else the test would be meaningless.
    let kind = clinker_plan::config::classify(shm).expect("classify /dev/shm");
    if kind != clinker_plan::config::FsKind::InMemory {
        eprintln!("skipping e330: /dev/shm is not tmpfs on this host ({kind:?})");
        return;
    }
    let dir = tempfile::Builder::new()
        .prefix("clinker-e330-")
        .tempdir_in(shm)
        .expect("create tmpfs spill dir");
    let cfg = storage(
        SpillConfig {
            dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        },
        StagingPolicy::default(),
    );
    let err = validate_storage_config(&cfg, &[], 0).unwrap_err();
    assert!(
        matches!(err, StorageValidationError::SpillDirInMemory { .. }),
        "expected E330 in-memory spill rejection, got {err}"
    );
    assert!(err.to_string().contains("E330"));
}

/// E333 — a staging dir on the same device as a matched source is rejected.
/// Runs on every platform: a source created under the staging tempdir shares
/// its device by construction.
#[test]
fn e333_staging_same_device_is_rejected() {
    let dir = tempfile::tempdir().expect("staging dir");
    let src = dir.path().join("orders.csv");
    std::fs::write(&src, b"a,b\n").expect("write source");
    let cfg = storage(
        SpillConfig::default(),
        StagingPolicy {
            enabled: true,
            dir: Some(dir.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        },
    );
    let err = validate_storage_config(&cfg, &[src], 0).unwrap_err();
    assert!(
        matches!(err, StorageValidationError::StagingSameDevice { .. }),
        "expected E333 staging same-device rejection, got {err}"
    );
    assert!(err.to_string().contains("E333"));
}

/// E334 — spill dir equal to staging dir is rejected. Runs everywhere: with no
/// matched source the same-device check is a no-op, so E334 stands alone.
#[test]
fn e334_spill_equals_staging_is_rejected() {
    let shared = tempfile::tempdir().expect("shared dir");
    let cfg = storage(
        SpillConfig {
            dir: Some(shared.path().to_path_buf()),
            ..Default::default()
        },
        StagingPolicy {
            enabled: true,
            dir: Some(shared.path().to_path_buf()),
            patterns: vec!["*.never-matches".into()],
            ..Default::default()
        },
    );
    let err = validate_storage_config(&cfg, &[], 0).unwrap_err();
    assert!(
        matches!(err, StorageValidationError::SpillEqualsStaging { .. }),
        "expected E334 spill-equals-staging rejection, got {err}"
    );
    assert!(err.to_string().contains("E334"));
}

/// A clean local config (separate spill and staging dirs, an unmatched source
/// on the staging volume) validates without error on every platform.
#[test]
fn clean_local_config_validates() {
    let spill = tempfile::tempdir().expect("spill dir");
    let staging = tempfile::tempdir().expect("staging dir");
    // An unmatched source on the staging volume is fine — it reads in place.
    let src = staging.path().join("orders.json");
    std::fs::write(&src, b"{}").expect("write source");
    let cfg = storage(
        SpillConfig {
            dir: Some(spill.path().to_path_buf()),
            ..Default::default()
        },
        StagingPolicy {
            enabled: true,
            dir: Some(staging.path().to_path_buf()),
            patterns: vec!["*.csv".into()],
            ..Default::default()
        },
    );
    let resolved = validate_storage_config(&cfg, &[src], 0).expect("clean config validates");
    assert_eq!(resolved.spill_root_dir.as_deref(), Some(spill.path()));
    assert!(resolved.free_space_warning.is_none());
}

/// The free-space preflight (W330) warns — without erroring — when the
/// estimated spill volume exceeds the spill volume's available space.
#[test]
fn free_space_preflight_warns_on_low_space() {
    let dir = tempfile::tempdir().expect("spill dir");
    let cfg = storage(
        SpillConfig {
            dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        },
        StagingPolicy::default(),
    );
    // An exabyte estimate exceeds any real volume's free space, so the advisory
    // fires; validation still succeeds (the preflight warns, it does not abort).
    let resolved =
        validate_storage_config(&cfg, &[], u64::MAX).expect("preflight warns, does not error");
    let warning = resolved
        .free_space_warning
        .expect("low-free-space advisory must be present");
    assert_eq!(warning.estimated_spill_bytes, u64::MAX);
    assert!(warning.to_string().contains("W330"));
}

/// The cap-headroom preflight (W331, #176 AC#4) warns — without erroring —
/// when the estimated spill volume reaches 80% of the configured spill cap. The
/// message disclaims that the headroom is per-invocation (#311).
#[test]
fn cap_headroom_preflight_warns_above_eighty_percent() {
    let dir = tempfile::tempdir().expect("spill dir");
    let cfg = storage(
        SpillConfig {
            dir: Some(dir.path().to_path_buf()),
            disk_cap_bytes: Some(clinker_plan::config::ByteSize(1_000)),
            ..Default::default()
        },
        StagingPolicy::default(),
    );
    // 900-byte estimate against a 1000-byte cap = 90%, over the 80% threshold.
    // Small enough that the real tempdir's free space does not also trip W330.
    let resolved =
        validate_storage_config(&cfg, &[], 900).expect("cap-headroom warns, does not error");
    let warning = resolved
        .cap_headroom_warning
        .expect("cap-headroom advisory must fire above 80% of the cap");
    assert_eq!(warning.disk_cap_bytes, 1_000);
    assert_eq!(warning.estimated_spill_bytes, 900);
    let msg = warning.to_string();
    assert!(msg.contains("W331"), "message must carry W331: {msg}");
    assert!(
        msg.contains("per invocation"),
        "the headroom must disclaim sibling invocations: {msg}"
    );
}

/// No cap-headroom warning when the estimate sits comfortably below 80% of the
/// cap, or when no cap is configured.
#[test]
fn cap_headroom_silent_below_threshold() {
    let dir = tempfile::tempdir().expect("spill dir");
    let cfg = storage(
        SpillConfig {
            dir: Some(dir.path().to_path_buf()),
            disk_cap_bytes: Some(clinker_plan::config::ByteSize(1_000_000_000)),
            ..Default::default()
        },
        StagingPolicy::default(),
    );
    // 1 KB estimate against a 1 GB cap is far under the threshold.
    let resolved = validate_storage_config(&cfg, &[], 1_000).expect("clean validate");
    assert!(resolved.cap_headroom_warning.is_none());
}

/// A directory-validity failure from the reused spill-dir check carries through
/// as the same message, on every platform.
#[test]
fn missing_spill_dir_carries_through() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cfg = storage(
        SpillConfig {
            dir: Some(dir.path().join("does-not-exist")),
            ..Default::default()
        },
        StagingPolicy::default(),
    );
    let err = validate_storage_config(&cfg, &[PathBuf::new()], 0).unwrap_err();
    assert!(
        matches!(
            err,
            StorageValidationError::Dir(
                clinker_plan::config::StorageConfigError::SpillDirMissing { .. }
            )
        ),
        "expected carried-through spill-dir-missing error, got {err}"
    );
}
