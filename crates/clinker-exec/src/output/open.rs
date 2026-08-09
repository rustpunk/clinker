//! Policy-aware file opener for output sinks.
//!
//! The single chokepoint that replaces direct `File::create` calls in
//! both the non-split sink path (`clinker::main`) and the split file
//! factory (`executor::build_format_writer`).

use std::fs::File;
use std::path::{Path, PathBuf};

use clinker_plan::config::{
    ConfigError, DestinationProfile, IfExistsPolicy, ResolvedPublicationPolicy,
};
use clinker_plan::error::PipelineError;
use clinker_plan::security::{check_overwrite, validate_path};

use super::containment::{ContainmentError, OutputContainment, PromotionDisposition, StagedOutput};

/// Open the next valid output sink given the active collision policy.
///
/// `path_for_n` produces the candidate path for a given collision
/// counter — `None` means "bare path with no counter applied". For
/// `UniqueSuffix`, this function walks `1..=u64::MAX`, acquiring an exclusive
/// destination-local reservation until one slot wins. The race-safe
/// reservation ensures concurrent runs writing into the same directory each
/// get a distinct final name without exposing a partial final.
///
/// `--force` (`cli_force = true`) downgrades `Error` → `Overwrite` for
/// ad-hoc CLI runs without rewriting the pipeline YAML.
pub fn open_output<F>(
    policy: IfExistsPolicy,
    cli_force: bool,
    path_for_n: F,
) -> Result<(PathBuf, File, StagedOutput), PipelineError>
where
    F: FnMut(Option<u64>) -> Result<PathBuf, ConfigError>,
{
    open_output_inner(None, policy, cli_force, path_for_n)
}

/// Open an output through a validated publication policy.
///
/// This is the policy-driven routing seam for run-owned attempts. The legacy
/// [`open_output`] entry point remains only until its CLI call site is migrated;
/// it performs detected-filesystem admission to preserve existing behavior.
pub fn open_output_with_policy<F>(
    publication: &ResolvedPublicationPolicy,
    policy: IfExistsPolicy,
    cli_force: bool,
    path_for_n: F,
) -> Result<(PathBuf, File, StagedOutput), PipelineError>
where
    F: FnMut(Option<u64>) -> Result<PathBuf, ConfigError>,
{
    open_output_inner(Some(publication), policy, cli_force, path_for_n)
}

fn open_output_inner<F>(
    publication: Option<&ResolvedPublicationPolicy>,
    policy: IfExistsPolicy,
    cli_force: bool,
    mut path_for_n: F,
) -> Result<(PathBuf, File, StagedOutput), PipelineError>
where
    F: FnMut(Option<u64>) -> Result<PathBuf, ConfigError>,
{
    let bare = path_for_n(None).map_err(PipelineError::Config)?;
    // Once for the whole search. The unique-suffix loop asked the kernel for
    // the same working directory on every candidate it tried, so a
    // destination already holding a few thousand numbered files paid a few
    // thousand identical `getcwd` calls before writing a byte.
    let base = std::env::current_dir().map_err(PipelineError::Io)?;

    match policy {
        IfExistsPolicy::Overwrite => {
            stage_candidate(bare, PromotionDisposition::Replace, publication, &base)
        }
        IfExistsPolicy::Error => {
            if cli_force {
                return stage_candidate(bare, PromotionDisposition::Replace, publication, &base);
            }
            match stage_candidate(
                bare.clone(),
                PromotionDisposition::NoReplace,
                publication,
                &base,
            ) {
                Err(error) if is_already_exists(&error) => Err(existing_output_error(&bare)),
                result => result,
            }
        }
        IfExistsPolicy::UniqueSuffix => {
            let mut search = SuffixSearch::default();
            let mut advance = |error: &PipelineError| search.advance(error);

            match stage_candidate(
                bare.clone(),
                PromotionDisposition::NoReplace,
                publication,
                &base,
            ) {
                Ok(output) => return Ok(output),
                Err(error) if advance(&error) => {}
                Err(error) => return Err(error),
            }

            for n in 1u64..=u64::MAX {
                let candidate = path_for_n(Some(n)).map_err(PipelineError::Config)?;
                match stage_candidate(
                    candidate,
                    PromotionDisposition::NoReplace,
                    publication,
                    &base,
                ) {
                    Ok(output) => return Ok(output),
                    Err(error) if advance(&error) => continue,
                    Err(error) => return Err(error),
                }
            }
            Err(PipelineError::Io(std::io::Error::other(
                "exhausted u64 collision counter for unique_suffix policy",
            )))
        }
    }
}

/// Append `-{n}` before the path's extension. Used when the user's
/// template lacks a `{n}` token but the policy is `UniqueSuffix`.
pub fn append_suffix_before_ext(path: &Path, suffix: &str) -> PathBuf {
    let parent = path.parent();
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let ext = path.extension().and_then(|s| s.to_str());
    let new_name = match ext {
        Some(ext) => format!("{stem}{suffix}.{ext}"),
        None => format!("{stem}{suffix}"),
    };
    match parent {
        Some(p) if !p.as_os_str().is_empty() => p.join(new_name),
        _ => PathBuf::from(new_name),
    }
}

fn stage_candidate(
    path: PathBuf,
    disposition: PromotionDisposition,
    publication: Option<&ResolvedPublicationPolicy>,
    base: &Path,
) -> Result<(PathBuf, File, StagedOutput), PipelineError> {
    let boundary = contained_boundary(&path, publication, base)?;
    let (staged, file) = boundary.stage(disposition).map_err(containment_error)?;
    Ok((path, file, staged))
}

fn contained_boundary(
    path: &Path,
    publication: Option<&ResolvedPublicationPolicy>,
    base: &Path,
) -> Result<OutputContainment, PipelineError> {
    let validated = validate_path(path, base, path.is_absolute()).map_err(|diagnostic| {
        PipelineError::Config(ConfigError::Validation(format!(
            "{}: {}",
            diagnostic.code, diagnostic.message
        )))
    })?;
    let profile = publication.map_or("detected-filesystem", |policy| {
        containment_profile(policy.destination_profile())
    });
    OutputContainment::for_profile(validated, profile).map_err(containment_error)
}

fn containment_profile(profile: DestinationProfile) -> &'static str {
    match profile {
        DestinationProfile::Local => "local-filesystem",
        DestinationProfile::NfsV4_1 => "linux-nfsv4.1-loopback-ci",
        DestinationProfile::Smb3_1_1 => "linux-smb3.1.1-loopback-ci",
    }
}

pub(crate) fn containment_error(error: ContainmentError) -> PipelineError {
    match error {
        ContainmentError::Io {
            operation,
            path,
            source,
        } => {
            let kind = source.kind();
            PipelineError::Io(std::io::Error::new(
                kind,
                format!(
                    "output containment {operation} failed for {}: {source}",
                    path.display()
                ),
            ))
        }
        error @ (ContainmentError::VisibleButUnsynced { .. }
        | ContainmentError::PublishedCleanup { .. }) => {
            PipelineError::Io(std::io::Error::other(error.to_string()))
        }
        other => PipelineError::Config(ConfigError::Validation(other.to_string())),
    }
}

/// Decides whether a unique-suffix search may move to the next candidate.
///
/// A name already taken is always a reason to advance. A sharing violation —
/// how Windows reports a name another thread is creating at that moment — is
/// too, but only for as long as it looks like contention. A directory this
/// process may not write into reports the identical error and never stops, so
/// advancing on it forever means a run that neither finishes nor says why.
///
/// The two are told apart by whether the denials are consecutive. Contention
/// clears the moment the competing thread finishes, so a successful advance
/// past a taken name resets the count; a denial that repeats on every
/// candidate reaches the bound and is reported.
#[derive(Default)]
pub(crate) struct SuffixSearch {
    consecutive_denials: u32,
    total_denials: u32,
}

impl SuffixSearch {
    /// How many denials in a row still look like contention.
    const MAX_CONSECUTIVE_DENIALS: u32 = 64;
    /// How many a whole search may meet before the answer is a denial
    /// regardless of spacing. A destination that is both permanently denied
    /// and concurrently written alternates the two errors, so the consecutive
    /// count alone never reaches its bound and the search never terminates.
    const MAX_TOTAL_DENIALS: u32 = 4096;

    /// Move past a candidate whose name is taken for a reason this search
    /// cannot read off an I/O error — another output in the same run having
    /// claimed it, which arrives as a validation failure.
    ///
    /// It is a taken name like any other, so it resets the contention count.
    /// Callers that recognised such a candidate themselves and skipped
    /// `advance` entirely left the count standing, and a destination that
    /// alternated claimed names with sharing violations reached the bound and
    /// failed a run whose next suffix was free.
    pub(crate) fn advance_past_taken_name(&mut self) -> bool {
        self.consecutive_denials = 0;
        true
    }

    pub(crate) fn advance(&mut self, error: &PipelineError) -> bool {
        if is_already_exists(error) {
            return self.advance_past_taken_name();
        }
        if !is_candidate_unavailable(error) {
            return false;
        }
        self.consecutive_denials = self.consecutive_denials.saturating_add(1);
        self.total_denials = self.total_denials.saturating_add(1);
        self.consecutive_denials <= Self::MAX_CONSECUTIVE_DENIALS
            && self.total_denials <= Self::MAX_TOTAL_DENIALS
    }
}

fn is_already_exists(error: &PipelineError) -> bool {
    matches!(error, PipelineError::Io(source) if source.kind() == std::io::ErrorKind::AlreadyExists)
}

/// Whether this candidate name is unavailable and the search should move on.
///
/// Exclusive creation reports a name already taken as `AlreadyExists`
/// everywhere. Windows has a second way to say it: a name another thread is
/// creating at the same moment can come back as a sharing violation, which the
/// standard library maps to `PermissionDenied`. Treating that as a hard error
/// made `unique_suffix` fail a run whenever two writers raced for one path,
/// rather than taking the next suffix — which is the whole point of the policy.
///
/// Deliberately not applied on the other platforms. There, exclusive creation
/// never reports a taken name this way, so a refused permission means the
/// directory genuinely cannot be written and must surface.
fn is_candidate_unavailable(error: &PipelineError) -> bool {
    if is_already_exists(error) {
        return true;
    }
    // `cfg!` rather than a `#[cfg]` block: every platform compiles the same
    // tokens and the branch folds away, so this cannot build on one host and
    // fail on the other — which matters for a rule that exists to describe the
    // host it cannot be compiled on here.
    cfg!(windows)
        && matches!(
            error,
            PipelineError::Io(source) if source.kind() == std::io::ErrorKind::PermissionDenied
        )
}

fn existing_output_error(path: &Path) -> PipelineError {
    let detail = match check_overwrite(path) {
        Err(diagnostic) => diagnostic.message,
        Ok(()) => format!(
            "output file already exists: {path:?} — use --force or set if_exists: overwrite"
        ),
    };
    PipelineError::Config(ConfigError::Validation(format!("E-SEC-001: {detail}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn touch(path: &Path) {
        let mut f = File::create(path).unwrap();
        f.write_all(b"x").unwrap();
    }

    /// Every way of moving past a taken name clears the contention count, not
    /// just the one spelled as an I/O error. A caller that recognised a taken
    /// candidate itself and returned early never reached this type at all, so
    /// a destination alternating claimed names with sharing violations
    /// accumulated denials that nothing reset and failed a run whose next
    /// suffix was free.
    ///
    /// The count is asserted directly because the denial it counts is only
    /// recognised on Windows: a behaviour-level test on this host would pass
    /// without the rule holding.
    #[test]
    fn any_taken_name_clears_the_contention_count() {
        type Clear<'a> = &'a dyn Fn(&mut SuffixSearch) -> bool;

        let taken = PipelineError::Io(std::io::Error::from(std::io::ErrorKind::AlreadyExists));
        let clears: [(&str, Clear<'_>); 2] = [
            (
                "a name this run already claimed",
                &|search: &mut SuffixSearch| search.advance_past_taken_name(),
            ),
            ("a name already on disk", &|search: &mut SuffixSearch| {
                search.advance(&taken)
            }),
        ];

        for (name, clear) in clears {
            let mut search = SuffixSearch {
                consecutive_denials: SuffixSearch::MAX_CONSECUTIVE_DENIALS,
                total_denials: 7,
            };

            assert!(clear(&mut search), "{name} is a reason to advance");
            assert_eq!(
                search.consecutive_denials, 0,
                "{name} ends the run of denials"
            );
            assert_eq!(
                search.total_denials, 7,
                "{name} is not itself a denial, so the whole-search bound is untouched"
            );
        }
    }

    #[test]
    fn overwrite_stages_without_touching_existing() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("out.csv");
        touch(&target);
        let (path, mut file, staged) = open_output(IfExistsPolicy::Overwrite, false, |n| {
            assert!(n.is_none());
            Ok(target.clone())
        })
        .unwrap();
        assert_eq!(path, target);
        file.write_all(b"new").unwrap();
        drop(file);
        assert_eq!(std::fs::read(&target).unwrap(), b"x");
        staged.commit().unwrap();
        assert_eq!(std::fs::read(&target).unwrap(), b"new");
    }

    #[test]
    fn error_refuses_when_present() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("out.csv");
        touch(&target);
        let result = open_output(IfExistsPolicy::Error, false, |_| Ok(target.clone()));
        let error = result.expect_err("existing output must be rejected");
        let rendered = error.to_string();
        assert!(rendered.contains("E-SEC-001"));
        assert!(rendered.contains("use --force or set if_exists: overwrite"));
    }

    #[test]
    fn error_with_cli_force_overwrites() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("out.csv");
        touch(&target);
        let (path, _f, _staged) =
            open_output(IfExistsPolicy::Error, true, |_| Ok(target.clone())).unwrap();
        assert_eq!(path, target);
        assert_eq!(std::fs::read(&target).unwrap(), b"x");
    }

    #[test]
    fn error_succeeds_when_absent() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("fresh.csv");
        let (path, _f, _staged) =
            open_output(IfExistsPolicy::Error, false, |_| Ok(target.clone())).unwrap();
        assert_eq!(path, target);
    }

    #[test]
    fn unique_suffix_finds_fresh_path() {
        let dir = tempdir().unwrap();
        let bare = dir.path().join("out.csv");
        touch(&bare);
        let (path, _f, _staged) = open_output(IfExistsPolicy::UniqueSuffix, false, |n| match n {
            None => Ok(bare.clone()),
            Some(k) => Ok(append_suffix_before_ext(&bare, &format!("-{k}"))),
        })
        .unwrap();
        assert_eq!(path, dir.path().join("out-1.csv"));
    }

    #[test]
    fn unique_suffix_walks_past_existing_counters() {
        let dir = tempdir().unwrap();
        let bare = dir.path().join("out.csv");
        touch(&bare);
        touch(&dir.path().join("out-1.csv"));
        touch(&dir.path().join("out-2.csv"));
        let (path, _f, _staged) = open_output(IfExistsPolicy::UniqueSuffix, false, |n| match n {
            None => Ok(bare.clone()),
            Some(k) => Ok(append_suffix_before_ext(&bare, &format!("-{k}"))),
        })
        .unwrap();
        assert_eq!(path, dir.path().join("out-3.csv"));
    }

    #[test]
    fn unique_suffix_uses_bare_when_free() {
        let dir = tempdir().unwrap();
        let bare = dir.path().join("fresh.csv");
        let (path, _f, _staged) =
            open_output(IfExistsPolicy::UniqueSuffix, false, |_| Ok(bare.clone())).unwrap();
        assert_eq!(path, bare);
    }

    #[test]
    fn staging_does_not_create_a_missing_parent_directory() {
        let dir = tempdir().unwrap();
        let parent = dir.path().join("missing");
        let target = parent.join("out.csv");

        let error = open_output(IfExistsPolicy::Overwrite, false, |_| Ok(target.clone()))
            .expect_err("missing parent must fail before staging");

        assert!(!parent.exists(), "output admission must not create parents");
        assert!(error.to_string().contains("missing"), "{error}");
    }

    #[test]
    fn append_suffix_handles_missing_extension() {
        let p = Path::new("/tmp/out");
        assert_eq!(append_suffix_before_ext(p, "-1"), Path::new("/tmp/out-1"));
    }

    #[test]
    fn append_suffix_preserves_parent() {
        let p = Path::new("/a/b/out.csv");
        assert_eq!(
            append_suffix_before_ext(p, "-7"),
            Path::new("/a/b/out-7.csv")
        );
    }
}
