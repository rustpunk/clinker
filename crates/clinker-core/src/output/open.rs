//! Policy-aware file opener for output sinks.
//!
//! The single chokepoint that replaces direct `File::create` calls in
//! both the non-split sink path (`clinker::main`) and the split file
//! factory (`executor::build_format_writer`).

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use crate::config::{ConfigError, IfExistsPolicy};
use crate::error::PipelineError;
use crate::security::check_overwrite;

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
            let f = File::create(&bare).map_err(PipelineError::Io)?;
            Ok((bare, f))
        }
        IfExistsPolicy::Error => {
            if cli_force {
                let f = File::create(&bare).map_err(PipelineError::Io)?;
                return Ok((bare, f));
            }
            check_overwrite(&bare).map_err(|d| {
                PipelineError::Config(ConfigError::Validation(format!(
                    "{}: {}",
                    d.code, d.message
                )))
            })?;
            let f = File::create(&bare).map_err(PipelineError::Io)?;
            Ok((bare, f))
        }
        IfExistsPolicy::UniqueSuffix => {
            match create_new(&bare) {
                Ok(f) => return Ok((bare, f)),
                Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => {
                    return Err(PipelineError::Io(e));
                }
                Err(_) => {}
            }
            for n in 1u64..=u64::MAX {
                let candidate = path_for_n(Some(n)).map_err(PipelineError::Config)?;
                match create_new(&candidate) {
                    Ok(f) => return Ok((candidate, f)),
                    Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(e) => return Err(PipelineError::Io(e)),
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

/// Resolve the final output path given the active collision policy
/// without opening any file.  Sibling to [`open_output`] for callers
/// that want to manage file creation themselves (e.g. atomic
/// tempfile-and-rename in the binary).
///
/// `Overwrite` returns `bare` unchanged.  `Error` returns `bare` if
/// the path is free or `cli_force` is set, otherwise returns a
/// `Diagnostic`-derived `ConfigError`.  `UniqueSuffix` walks
/// `1..=u64::MAX` integer suffixes and returns the first free slot.
///
/// Race window: callers that rely on the resolved path remaining free
/// must take whatever locking is appropriate downstream — this helper
/// only checks existence at the time of the call.
#[allow(clippy::result_large_err)]
pub fn resolve_output_path(
    bare: &Path,
    policy: IfExistsPolicy,
    cli_force: bool,
    unique_suffix_width: u8,
) -> Result<PathBuf, ConfigError> {
    match policy {
        IfExistsPolicy::Overwrite => Ok(bare.to_path_buf()),
        IfExistsPolicy::Error => {
            if cli_force || !bare.exists() {
                return Ok(bare.to_path_buf());
            }
            crate::security::check_overwrite(bare)
                .map_err(|d| ConfigError::Validation(format!("{}: {}", d.code, d.message)))?;
            Ok(bare.to_path_buf())
        }
        IfExistsPolicy::UniqueSuffix => {
            if !bare.exists() {
                return Ok(bare.to_path_buf());
            }
            for k in 1u64..=u64::MAX {
                let suffix = if unique_suffix_width == 0 {
                    format!("-{k}")
                } else {
                    format!("-{:0>width$}", k, width = unique_suffix_width as usize)
                };
                let candidate = append_suffix_before_ext(bare, &suffix);
                if !candidate.exists() {
                    return Ok(candidate);
                }
            }
            Err(ConfigError::Validation(
                "exhausted u64 collision counter for unique_suffix policy".into(),
            ))
        }
    }
}

fn create_new(path: &Path) -> std::io::Result<File> {
    OpenOptions::new().write(true).create_new(true).open(path)
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
        assert!(result.is_err());
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
