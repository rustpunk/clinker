//! Policy-aware file opener for output sinks.
//!
//! The single chokepoint that replaces direct `File::create` calls in
//! both the non-split sink path (`clinker::main`) and the split file
//! factory (`executor::build_format_writer`).

use std::fs::File;
use std::path::{Path, PathBuf};

use clinker_plan::config::{ConfigError, IfExistsPolicy};
use clinker_plan::error::PipelineError;
use clinker_plan::security::{check_overwrite, validate_path};

use super::containment::{ContainmentError, OpenDisposition, OutputContainment};

/// Open the next valid output sink given the active collision policy.
///
/// `path_for_n` produces the candidate path for a given collision
/// counter — `None` means "bare path with no counter applied". For
/// `UniqueSuffix`, this function walks `1..=u64::MAX`, retrying via
/// `OpenOptions::create_new(true)` until one slot wins. The
/// race-safe `create_new` semantics ensure two concurrent runs writing
/// into the same directory each get a distinct file.
///
/// `--force` (`cli_force = true`) downgrades `Error` → `Overwrite` for
/// ad-hoc CLI runs without rewriting the pipeline YAML.
pub fn open_output<F>(
    policy: IfExistsPolicy,
    cli_force: bool,
    mut path_for_n: F,
) -> Result<(PathBuf, File), PipelineError>
where
    F: FnMut(Option<u64>) -> Result<PathBuf, ConfigError>,
{
    let bare = path_for_n(None).map_err(PipelineError::Config)?;

    match policy {
        IfExistsPolicy::Overwrite => {
            let f = contained_open(&bare, OpenDisposition::Truncate)?;
            Ok((bare, f))
        }
        IfExistsPolicy::Error => {
            if cli_force {
                let f = contained_open(&bare, OpenDisposition::Truncate)?;
                return Ok((bare, f));
            }
            match contained_open(&bare, OpenDisposition::CreateNew) {
                Ok(file) => Ok((bare, file)),
                Err(error) if is_already_exists(&error) => Err(existing_output_error(&bare)),
                Err(error) => Err(error),
            }
        }
        IfExistsPolicy::UniqueSuffix => {
            match contained_open(&bare, OpenDisposition::CreateNew) {
                Ok(f) => return Ok((bare, f)),
                Err(e) if !is_already_exists(&e) => {
                    return Err(e);
                }
                Err(_) => {}
            }

            for n in 1u64..=u64::MAX {
                let candidate = path_for_n(Some(n)).map_err(PipelineError::Config)?;
                match contained_open(&candidate, OpenDisposition::CreateNew) {
                    Ok(f) => return Ok((candidate, f)),
                    Err(e) if is_already_exists(&e) => continue,
                    Err(e) => return Err(e),
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

fn contained_open(path: &Path, disposition: OpenDisposition) -> Result<File, PipelineError> {
    let base = std::env::current_dir().map_err(PipelineError::Io)?;
    let validated = validate_path(path, &base, path.is_absolute()).map_err(|diagnostic| {
        PipelineError::Config(ConfigError::Validation(format!(
            "{}: {}",
            diagnostic.code, diagnostic.message
        )))
    })?;
    OutputContainment::for_profile(validated, "detected-filesystem")
        .and_then(|boundary| boundary.open(disposition))
        .map_err(containment_error)
}

fn containment_error(error: ContainmentError) -> PipelineError {
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
        other => PipelineError::Config(ConfigError::Validation(other.to_string())),
    }
}

fn is_already_exists(error: &PipelineError) -> bool {
    matches!(error, PipelineError::Io(source) if source.kind() == std::io::ErrorKind::AlreadyExists)
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

    #[test]
    fn overwrite_truncates_existing() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("out.csv");
        touch(&target);
        let (path, _f) = open_output(IfExistsPolicy::Overwrite, false, |n| {
            assert!(n.is_none());
            Ok(target.clone())
        })
        .unwrap();
        assert_eq!(path, target);
        assert_eq!(std::fs::metadata(&target).unwrap().len(), 0);
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
        let (path, _f) = open_output(IfExistsPolicy::Error, true, |_| Ok(target.clone())).unwrap();
        assert_eq!(path, target);
        assert_eq!(std::fs::metadata(&target).unwrap().len(), 0);
    }

    #[test]
    fn error_succeeds_when_absent() {
        let dir = tempdir().unwrap();
        let target = dir.path().join("fresh.csv");
        let (path, _f) = open_output(IfExistsPolicy::Error, false, |_| Ok(target.clone())).unwrap();
        assert_eq!(path, target);
    }

    #[test]
    fn unique_suffix_finds_fresh_path() {
        let dir = tempdir().unwrap();
        let bare = dir.path().join("out.csv");
        touch(&bare);
        let (path, _f) = open_output(IfExistsPolicy::UniqueSuffix, false, |n| match n {
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
        let (path, _f) = open_output(IfExistsPolicy::UniqueSuffix, false, |n| match n {
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
        let (path, _f) =
            open_output(IfExistsPolicy::UniqueSuffix, false, |_| Ok(bare.clone())).unwrap();
        assert_eq!(path, bare);
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
