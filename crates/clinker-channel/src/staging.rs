//! Source-staging resolution hook.
//!
//! The channel layer is where a source's declared path meets the workspace
//! [`StagingPolicy`]: [`resolve_source`] decides, per source, whether the
//! reader should open a local staged copy instead of the original path, and
//! [`validate_staging`] runs the one-time startup checks that fail a
//! misconfigured staging dir before any input is opened.
//!
//! The split mirrors NiFi's `ListFile` + `FetchFile`: deciding *what* to
//! stage is kept separate from doing the copy, so each layer is testable in
//! isolation. The copy itself is not wired yet — [`resolve_source`] reports a
//! [`StagedPath`] whose `staged` is always `None` (read in place), so a run
//! with staging enabled behaves identically to today until the copy lands.

use std::path::PathBuf;

use clinker_plan::config::{StagedPath, StagingPolicy, StorageConfigError};

/// Resolve one source path against the workspace staging policy.
///
/// Returns the [`StagedPath`] the reader should honor: it opens
/// [`StagedPath::read_path`] regardless of whether staging fired, so the read
/// side stays agnostic to staging. When the policy is enabled and `original`
/// matches a staging pattern the source is *selected* for staging, but —
/// because the copy step is not yet wired — the returned path still reads in
/// place (`staged == None`). Wiring the copy means returning the local copy
/// from the matched arm of [`StagingPolicy::resolve_one`]; nothing here
/// changes.
pub fn resolve_source(policy: &StagingPolicy, original: PathBuf) -> StagedPath {
    policy.resolve_one(original)
}

/// Validate the workspace staging policy against the paths a run will read.
///
/// A no-op when staging is disabled. When enabled, requires `dir` to be set,
/// to exist as a writable directory, and to sit on a different volume than
/// every *matched* source, and rejects an unparseable pattern. Called once at
/// startup so a misconfigured staging dir fails the run before any input is
/// opened rather than at the first copy.
///
/// # Errors
///
/// Propagates the [`StorageConfigError`] variant
/// [`StagingPolicy::validate`] produces for the specific misconfiguration
/// (dir unset / missing / not-a-directory / not-writable, invalid pattern, or
/// a staging dir sharing a volume with a matched source).
pub fn validate_staging(
    policy: &StagingPolicy,
    source_paths: &[PathBuf],
) -> Result<(), StorageConfigError> {
    policy.validate(source_paths)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clinker_plan::config::StagingPolicy;

    fn policy_with(patterns: &[&str], dir: Option<PathBuf>) -> StagingPolicy {
        StagingPolicy {
            enabled: true,
            dir,
            patterns: patterns.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    #[test]
    fn disabled_policy_reads_in_place() {
        let policy = StagingPolicy::default();
        let resolved = resolve_source(&policy, PathBuf::from("/mnt/nfs/data/orders.csv"));
        assert_eq!(resolved.staged, None);
        assert_eq!(
            resolved.read_path(),
            std::path::Path::new("/mnt/nfs/data/orders.csv")
        );
    }

    #[test]
    fn matched_source_still_reads_in_place_until_copy_lands() {
        // Staging is enabled and the path matches, but the copy step is not
        // wired, so the resolved path still reads in place.
        let policy = policy_with(&["/mnt/nfs/**"], Some(PathBuf::from("/tmp")));
        let resolved = resolve_source(&policy, PathBuf::from("/mnt/nfs/data/orders.csv"));
        assert_eq!(resolved.staged, None);
    }

    #[test]
    fn validate_is_noop_when_disabled() {
        let policy = StagingPolicy::default();
        assert!(validate_staging(&policy, &[PathBuf::from("/mnt/nfs/x.csv")]).is_ok());
    }

    #[test]
    fn validate_errors_when_enabled_without_dir() {
        let policy = policy_with(&["**/*.csv"], None);
        assert!(matches!(
            validate_staging(&policy, &[]).unwrap_err(),
            StorageConfigError::StagingDirUnset
        ));
    }

    #[test]
    fn validate_ok_for_unmatched_source_on_same_volume() {
        // The staging dir and the source share a volume (both under tempdir),
        // but the source does not match any pattern, so it is read in place
        // and the same-volume rule does not apply.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("orders.json");
        std::fs::write(&src, b"{}").unwrap();
        let policy = policy_with(&["*.csv"], Some(dir.path().to_path_buf()));
        assert!(validate_staging(&policy, &[src]).is_ok());
    }

    #[test]
    fn validate_rejects_same_volume_matched_source() {
        // A matched source on the same volume as the staging dir is refused.
        let dir = tempfile::tempdir().unwrap();
        let src = dir.path().join("orders.csv");
        std::fs::write(&src, b"a,b\n").unwrap();
        let policy = policy_with(&["*.csv"], Some(dir.path().to_path_buf()));
        assert!(matches!(
            validate_staging(&policy, &[src]).unwrap_err(),
            StorageConfigError::StagingSameVolume { .. }
        ));
    }

    #[test]
    fn validate_rejects_invalid_pattern() {
        let dir = tempfile::tempdir().unwrap();
        let policy = policy_with(&["["], Some(dir.path().to_path_buf()));
        assert!(matches!(
            validate_staging(&policy, &[]).unwrap_err(),
            StorageConfigError::StagingPatternInvalid { .. }
        ));
    }
}
