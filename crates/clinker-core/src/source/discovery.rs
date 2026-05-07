//! File discovery for [`crate::config::SourceConfig`].
//!
//! Resolves the matcher (`path` / `glob` / `regex` / `paths`) and applies
//! the post-discovery filters (`exclude`, `modified_after`/`modified_before`,
//! `min_size`/`max_size`, `files.sort_by` + `files.take_first`/`take_last`,
//! `files.recursive`, `files.on_no_match`). Returns the file set sorted as
//! requested with metadata stamps the executor reuses for `$source.*`
//! provenance.
//!
//! Cross-platform notes:
//! - Glob semantics follow POSIX/gitignore: `*` does not cross path
//!   separators. `**` matches across separators; presence of `**` flips
//!   `recursive` to true unless explicitly set.
//! - Regex matches against the full canonical path string (forward
//!   slashes on Linux/macOS, backslashes on Windows; the helper
//!   normalizes to forward slashes before matching for portable patterns).
//! - File "creation" time uses `Metadata::created()` where supported and
//!   falls back to `modified()` otherwise — mirrors `find -newer` and
//!   Spark's `latestFirst` semantics.

use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::config::{FileSortKey, NoMatchPolicy, SortDir, SourceConfig, TimeBound};

/// One discovered file with its metadata stamps. Returned in the order
/// the discovery pipeline produced (post-sort, post-take).
#[derive(Debug, Clone)]
pub struct DiscoveredFile {
    pub path: PathBuf,
    pub modified: SystemTime,
    pub created: SystemTime,
    pub size: u64,
}

/// Errors surfaced by [`discover`]. Caller maps to E210–E218 diagnostics
/// at the outer config-validation layer.
#[derive(Debug)]
pub enum DiscoveryError {
    /// More than one of `{path, glob, regex, paths}` was set. E210.
    MultipleMatchers { which: Vec<&'static str> },
    /// None of `{path, glob, regex, paths}` was set. E211.
    NoMatcher,
    /// Glob compilation failed. E212.
    InvalidGlob { pattern: String, message: String },
    /// Regex compilation failed. E213.
    InvalidRegex { pattern: String, message: String },
    /// Both `take_first` and `take_last` were set. E218.
    TakeBothSpecified,
    /// Discovery matched zero files and `on_no_match: error`. E216.
    NoMatch { matcher: String },
    /// Filesystem walk hit an I/O error while enumerating.
    Io(std::io::Error),
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DiscoveryError::MultipleMatchers { which } => write!(
                f,
                "source declares more than one matcher: {}",
                which.join(", ")
            ),
            DiscoveryError::NoMatcher => f.write_str(
                "source declares no matcher; set exactly one of `path`, `glob`, `regex`, `paths`",
            ),
            DiscoveryError::InvalidGlob { pattern, message } => {
                write!(f, "invalid glob pattern {pattern:?}: {message}")
            }
            DiscoveryError::InvalidRegex { pattern, message } => {
                write!(f, "invalid regex pattern {pattern:?}: {message}")
            }
            DiscoveryError::TakeBothSpecified => {
                f.write_str("files.take_first and files.take_last are mutually exclusive")
            }
            DiscoveryError::NoMatch { matcher } => {
                write!(f, "source matched zero files (matcher: {matcher})")
            }
            DiscoveryError::Io(e) => write!(f, "filesystem error during discovery: {e}"),
        }
    }
}

impl std::error::Error for DiscoveryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DiscoveryError::Io(e) => Some(e),
            _ => None,
        }
    }
}

/// Outcome of discovery. `Skip` and `Warn` (per `on_no_match`) yield an
/// empty file set with a structured signal; `Error` raises an error
/// instead of returning here.
#[derive(Debug)]
pub enum DiscoveryOutcome {
    /// One or more files matched.
    Found(Vec<DiscoveredFile>),
    /// Zero files matched, and the policy is `Warn`. Caller should log.
    EmptyWarn { matcher: String },
    /// Zero files matched, and the policy is `Skip`. Caller proceeds
    /// silently with an empty record stream.
    EmptySkip,
}

impl DiscoveryOutcome {
    /// Borrow the discovered file list. `EmptyWarn` and `EmptySkip` both
    /// return an empty slice.
    pub fn files(&self) -> &[DiscoveredFile] {
        match self {
            DiscoveryOutcome::Found(v) => v,
            _ => &[],
        }
    }
}

/// Run the full discovery pipeline against `source` rooted at `base_dir`.
///
/// `base_dir` is the workspace root (typically the directory containing
/// the YAML pipeline file). Relative matchers resolve against it.
pub fn discover(
    source: &SourceConfig,
    base_dir: &Path,
) -> Result<DiscoveryOutcome, DiscoveryError> {
    // 1. Validate exactly-one matcher.
    let matcher = pick_matcher(source)?;

    // 2. Validate take_first / take_last mutual exclusion early.
    if let Some(controls) = source.files.as_ref()
        && controls.take_first.is_some()
        && controls.take_last.is_some()
    {
        return Err(DiscoveryError::TakeBothSpecified);
    }

    // 3. Enumerate primary candidates.
    let recursive = effective_recursive(source, &matcher);
    let mut candidates = enumerate(&matcher, base_dir, recursive)?;

    // 4. Drop entries matching any `exclude` pattern.
    if let Some(excludes) = source.exclude.as_deref() {
        let compiled = compile_globs(excludes)?;
        candidates.retain(|p| !matches_any(p, &compiled));
    }

    // 5. Stat each survivor. Drop those that fail size / mtime filters.
    let now = SystemTime::now();
    let mut discovered: Vec<DiscoveredFile> = Vec::with_capacity(candidates.len());
    for path in candidates {
        let meta = match std::fs::metadata(&path) {
            Ok(m) => m,
            Err(e) => return Err(DiscoveryError::Io(e)),
        };
        if !meta.is_file() {
            continue;
        }
        let size = meta.len();
        if let Some(min) = source.min_size
            && size < min.0
        {
            continue;
        }
        if let Some(max) = source.max_size
            && size > max.0
        {
            continue;
        }
        let modified = meta.modified().unwrap_or(now);
        let created = meta.created().unwrap_or(modified);
        if !time_bound_pass(modified, source.modified_after, source.modified_before) {
            continue;
        }
        discovered.push(DiscoveredFile {
            path,
            modified,
            created,
            size,
        });
    }

    // 6. Sort by configured key + direction (defaults: name ascending).
    let (sort_key, sort_dir) = sort_spec(source);
    sort_files(&mut discovered, sort_key, sort_dir);

    // 7. Apply take_first / take_last.
    if let Some(controls) = source.files.as_ref() {
        if let Some(n) = controls.take_first {
            discovered.truncate(n);
        } else if let Some(n) = controls.take_last
            && discovered.len() > n
        {
            let drop = discovered.len() - n;
            discovered.drain(..drop);
        }
    }

    // 8. Enforce on_no_match.
    if discovered.is_empty() {
        let policy = source
            .files
            .as_ref()
            .and_then(|c| c.on_no_match)
            .unwrap_or_default();
        return Ok(match policy {
            NoMatchPolicy::Error => {
                return Err(DiscoveryError::NoMatch {
                    matcher: matcher_display(&matcher),
                });
            }
            NoMatchPolicy::Warn => DiscoveryOutcome::EmptyWarn {
                matcher: matcher_display(&matcher),
            },
            NoMatchPolicy::Skip => DiscoveryOutcome::EmptySkip,
        });
    }

    Ok(DiscoveryOutcome::Found(discovered))
}

// ──────────────────────────────────────────────────────────────────────
// internals
// ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
enum Matcher {
    Path(String),
    Glob(String),
    Regex(String),
    Paths(Vec<String>),
}

fn pick_matcher(source: &SourceConfig) -> Result<Matcher, DiscoveryError> {
    let mut which: Vec<&'static str> = Vec::new();
    if source.path.is_some() {
        which.push("path");
    }
    if source.glob.is_some() {
        which.push("glob");
    }
    if source.regex.is_some() {
        which.push("regex");
    }
    if source.paths.is_some() {
        which.push("paths");
    }
    match which.as_slice() {
        [] => Err(DiscoveryError::NoMatcher),
        [one] => Ok(match *one {
            "path" => Matcher::Path(source.path.clone().unwrap()),
            "glob" => Matcher::Glob(source.glob.clone().unwrap()),
            "regex" => Matcher::Regex(source.regex.clone().unwrap()),
            "paths" => Matcher::Paths(source.paths.clone().unwrap()),
            _ => unreachable!(),
        }),
        _ => Err(DiscoveryError::MultipleMatchers {
            which: which.clone(),
        }),
    }
}

fn matcher_display(m: &Matcher) -> String {
    match m {
        Matcher::Path(p) => format!("path: {p}"),
        Matcher::Glob(g) => format!("glob: {g}"),
        Matcher::Regex(r) => format!("regex: {r}"),
        Matcher::Paths(ps) => format!("paths: [{}]", ps.join(", ")),
    }
}

/// Decide whether to walk subdirectories. Explicit `files.recursive`
/// wins. Otherwise: glob with `**` walks recursively, everything else
/// stays at the configured directory level.
fn effective_recursive(source: &SourceConfig, matcher: &Matcher) -> bool {
    if let Some(ctrl) = source.files.as_ref()
        && let Some(r) = ctrl.recursive
    {
        return r;
    }
    matches!(matcher, Matcher::Glob(g) if g.contains("**")) || matches!(matcher, Matcher::Regex(_))
}

/// Resolve the matcher to an absolute candidate path list. Symlinks are
/// not followed (mirrors composition-walk security policy).
fn enumerate(
    matcher: &Matcher,
    base_dir: &Path,
    recursive: bool,
) -> Result<Vec<PathBuf>, DiscoveryError> {
    match matcher {
        Matcher::Path(p) => {
            let resolved = resolve(p, base_dir);
            Ok(vec![resolved])
        }
        Matcher::Paths(ps) => Ok(ps.iter().map(|p| resolve(p, base_dir)).collect()),
        Matcher::Glob(pattern) => {
            let abs_pattern = if Path::new(pattern).is_absolute() {
                pattern.clone()
            } else {
                base_dir.join(pattern).to_string_lossy().into_owned()
            };
            let opts = glob::MatchOptions {
                case_sensitive: true,
                require_literal_separator: !pattern.contains("**"),
                require_literal_leading_dot: false,
            };
            let entries =
                glob::glob_with(&abs_pattern, opts).map_err(|e| DiscoveryError::InvalidGlob {
                    pattern: pattern.clone(),
                    message: e.to_string(),
                })?;
            let mut out = Vec::new();
            for entry in entries {
                match entry {
                    Ok(p) => out.push(p),
                    Err(glob_err) => {
                        return Err(DiscoveryError::Io(std::io::Error::other(
                            glob_err.to_string(),
                        )));
                    }
                }
            }
            Ok(out)
        }
        Matcher::Regex(pattern) => {
            let re = regex::Regex::new(pattern).map_err(|e| DiscoveryError::InvalidRegex {
                pattern: pattern.clone(),
                message: e.to_string(),
            })?;
            let mut out = Vec::new();
            let walker = walkdir::WalkDir::new(base_dir)
                .follow_links(false)
                .max_depth(if recursive { usize::MAX } else { 1 })
                .into_iter()
                .filter_entry(|e| !e.file_type().is_symlink());
            for entry in walker {
                let entry = match entry {
                    Ok(e) => e,
                    Err(_) => continue,
                };
                if !entry.file_type().is_file() {
                    continue;
                }
                let path = entry.path();
                let candidate = path.to_string_lossy().replace('\\', "/");
                if re.is_match(&candidate) {
                    out.push(path.to_path_buf());
                }
            }
            Ok(out)
        }
    }
}

fn resolve(p: &str, base_dir: &Path) -> PathBuf {
    let path = Path::new(p);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base_dir.join(path)
    }
}

fn compile_globs(patterns: &[String]) -> Result<Vec<glob::Pattern>, DiscoveryError> {
    let mut out = Vec::with_capacity(patterns.len());
    for p in patterns {
        let pat = glob::Pattern::new(p).map_err(|e| DiscoveryError::InvalidGlob {
            pattern: p.clone(),
            message: e.to_string(),
        })?;
        out.push(pat);
    }
    Ok(out)
}

/// Match a path against any of the compiled exclude patterns. Tested
/// against both the basename (gitignore-style) and the full path string
/// — Logstash's `exclude` works similarly.
fn matches_any(path: &Path, patterns: &[glob::Pattern]) -> bool {
    let path_str = path.to_string_lossy();
    let basename = path
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    patterns
        .iter()
        .any(|p| p.matches(&path_str) || p.matches(&basename))
}

fn time_bound_pass(
    modified: SystemTime,
    after: Option<TimeBound>,
    before: Option<TimeBound>,
) -> bool {
    if let Some(a) = after
        && modified < a.0
    {
        return false;
    }
    if let Some(b) = before
        && modified > b.0
    {
        return false;
    }
    true
}

fn sort_spec(source: &SourceConfig) -> (FileSortKey, SortDir) {
    let ctrl = source.files.as_ref();
    let key = ctrl.and_then(|c| c.sort_by).unwrap_or_default();
    let dir = ctrl.and_then(|c| c.sort_order).unwrap_or_default();
    (key, dir)
}

fn sort_files(files: &mut [DiscoveredFile], key: FileSortKey, dir: SortDir) {
    match key {
        FileSortKey::Name => files.sort_by(|a, b| a.path.cmp(&b.path)),
        FileSortKey::Created => files.sort_by(|a, b| a.created.cmp(&b.created)),
        FileSortKey::Modified => files.sort_by(|a, b| a.modified.cmp(&b.modified)),
    }
    if matches!(dir, SortDir::Desc) {
        files.reverse();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{
        ByteSize, FileListingControls, FileSortKey, InputFormat, NoMatchPolicy, SortDir,
        SourceConfig, TimeBound,
    };
    use std::fs::File;
    use std::io::Write;
    use std::time::Duration;

    /// Build a minimal SourceConfig with the given matcher fields. All
    /// other discovery-relevant fields default to `None`.
    fn cfg() -> SourceConfig {
        SourceConfig {
            name: "test".into(),
            path: None,
            glob: None,
            regex: None,
            paths: None,
            exclude: None,
            modified_after: None,
            modified_before: None,
            min_size: None,
            max_size: None,
            files: None,
            schema: None,
            schema_overrides: None,
            array_paths: None,
            sort_order: None,
            format: InputFormat::Csv(None),
            notes: None,
        }
    }

    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        let mut f = File::create(&p).unwrap();
        f.write_all(body.as_bytes()).unwrap();
        p
    }

    #[test]
    fn literal_path_finds_one_file() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "a.csv", "x");
        let mut s = cfg();
        s.path = Some("a.csv".into());
        let out = discover(&s, tmp.path()).unwrap();
        assert_eq!(out.files().len(), 1);
        assert_eq!(out.files()[0].path.file_name().unwrap(), "a.csv");
    }

    #[test]
    fn glob_finds_matching_files() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "orders_2026_01.csv", "");
        write(tmp.path(), "orders_2026_02.csv", "");
        write(tmp.path(), "products.csv", "");
        let mut s = cfg();
        s.glob = Some("orders_*.csv".into());
        let out = discover(&s, tmp.path()).unwrap();
        assert_eq!(out.files().len(), 2);
    }

    #[test]
    fn regex_walks_recursively_by_default() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("sub")).unwrap();
        write(tmp.path(), "a.csv", "");
        write(&tmp.path().join("sub"), "b.csv", "");
        let mut s = cfg();
        s.regex = Some(r"\.csv$".into());
        let out = discover(&s, tmp.path()).unwrap();
        assert_eq!(out.files().len(), 2);
    }

    #[test]
    fn paths_list_resolves_each_entry() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "a.csv", "");
        write(tmp.path(), "b.csv", "");
        let mut s = cfg();
        s.paths = Some(vec!["a.csv".into(), "b.csv".into()]);
        let out = discover(&s, tmp.path()).unwrap();
        assert_eq!(out.files().len(), 2);
    }

    #[test]
    fn multiple_matchers_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = cfg();
        s.path = Some("a.csv".into());
        s.glob = Some("*.csv".into());
        let err = discover(&s, tmp.path()).unwrap_err();
        match err {
            DiscoveryError::MultipleMatchers { which } => {
                assert!(which.contains(&"path"));
                assert!(which.contains(&"glob"));
            }
            other => panic!("expected MultipleMatchers, got {other:?}"),
        }
    }

    #[test]
    fn no_matcher_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let s = cfg();
        assert!(matches!(
            discover(&s, tmp.path()).unwrap_err(),
            DiscoveryError::NoMatcher
        ));
    }

    #[test]
    fn exclude_drops_matching_files() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "good.csv", "");
        write(tmp.path(), "bad.tmp", "");
        let mut s = cfg();
        s.glob = Some("*".into());
        s.exclude = Some(vec!["*.tmp".into()]);
        let out = discover(&s, tmp.path()).unwrap();
        let names: Vec<_> = out
            .files()
            .iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert!(names.contains(&"good.csv".to_string()));
        assert!(!names.contains(&"bad.tmp".to_string()));
    }

    #[test]
    fn min_size_filter() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "small.csv", ""); // 0 bytes
        write(tmp.path(), "big.csv", "0123456789"); // 10 bytes
        let mut s = cfg();
        s.glob = Some("*.csv".into());
        s.min_size = Some(ByteSize(5));
        let out = discover(&s, tmp.path()).unwrap();
        assert_eq!(out.files().len(), 1);
        assert_eq!(out.files()[0].path.file_name().unwrap(), "big.csv");
    }

    #[test]
    fn max_size_filter() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "small.csv", "x"); // 1 byte
        write(tmp.path(), "big.csv", "0123456789"); // 10 bytes
        let mut s = cfg();
        s.glob = Some("*.csv".into());
        s.max_size = Some(ByteSize(5));
        let out = discover(&s, tmp.path()).unwrap();
        assert_eq!(out.files().len(), 1);
        assert_eq!(out.files()[0].path.file_name().unwrap(), "small.csv");
    }

    #[test]
    fn modified_after_skips_old_files() {
        let tmp = tempfile::tempdir().unwrap();
        let p = write(tmp.path(), "a.csv", "");
        // Backdate the file to an hour and a half ago.
        let ninety_min_ago = SystemTime::now() - Duration::from_secs(60 * 90);
        File::options()
            .write(true)
            .open(&p)
            .unwrap()
            .set_modified(ninety_min_ago)
            .unwrap();

        let mut s = cfg();
        s.glob = Some("*.csv".into());
        s.modified_after = Some(TimeBound(SystemTime::now() - Duration::from_secs(60 * 60)));
        s.files = Some(FileListingControls {
            on_no_match: Some(NoMatchPolicy::Skip),
            ..Default::default()
        });
        let out = discover(&s, tmp.path()).unwrap();
        assert_eq!(out.files().len(), 0);
    }

    #[test]
    fn sort_by_name_asc_default() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "c.csv", "");
        write(tmp.path(), "a.csv", "");
        write(tmp.path(), "b.csv", "");
        let mut s = cfg();
        s.glob = Some("*.csv".into());
        let out = discover(&s, tmp.path()).unwrap();
        let names: Vec<_> = out
            .files()
            .iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["a.csv", "b.csv", "c.csv"]);
    }

    #[test]
    fn sort_desc_take_first() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "a.csv", "");
        write(tmp.path(), "b.csv", "");
        write(tmp.path(), "c.csv", "");
        let mut s = cfg();
        s.glob = Some("*.csv".into());
        s.files = Some(FileListingControls {
            sort_by: Some(FileSortKey::Name),
            sort_order: Some(SortDir::Desc),
            take_first: Some(2),
            ..Default::default()
        });
        let out = discover(&s, tmp.path()).unwrap();
        let names: Vec<_> = out
            .files()
            .iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["c.csv", "b.csv"]);
    }

    #[test]
    fn take_last_keeps_tail() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "a.csv", "");
        write(tmp.path(), "b.csv", "");
        write(tmp.path(), "c.csv", "");
        let mut s = cfg();
        s.glob = Some("*.csv".into());
        s.files = Some(FileListingControls {
            take_last: Some(1),
            ..Default::default()
        });
        let out = discover(&s, tmp.path()).unwrap();
        let names: Vec<_> = out
            .files()
            .iter()
            .map(|f| f.path.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(names, vec!["c.csv"]);
    }

    #[test]
    fn take_first_and_last_mutually_exclusive() {
        let tmp = tempfile::tempdir().unwrap();
        write(tmp.path(), "a.csv", "");
        let mut s = cfg();
        s.glob = Some("*.csv".into());
        s.files = Some(FileListingControls {
            take_first: Some(1),
            take_last: Some(1),
            ..Default::default()
        });
        assert!(matches!(
            discover(&s, tmp.path()).unwrap_err(),
            DiscoveryError::TakeBothSpecified
        ));
    }

    #[test]
    fn no_match_default_errors() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = cfg();
        s.glob = Some("nonexistent_*.csv".into());
        match discover(&s, tmp.path()).unwrap_err() {
            DiscoveryError::NoMatch { matcher } => assert!(matcher.contains("nonexistent_")),
            other => panic!("expected NoMatch, got {other:?}"),
        }
    }

    #[test]
    fn no_match_warn_returns_empty_warn() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = cfg();
        s.glob = Some("nonexistent_*.csv".into());
        s.files = Some(FileListingControls {
            on_no_match: Some(NoMatchPolicy::Warn),
            ..Default::default()
        });
        assert!(matches!(
            discover(&s, tmp.path()).unwrap(),
            DiscoveryOutcome::EmptyWarn { .. }
        ));
    }

    #[test]
    fn no_match_skip_returns_empty_skip() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = cfg();
        s.glob = Some("nonexistent_*.csv".into());
        s.files = Some(FileListingControls {
            on_no_match: Some(NoMatchPolicy::Skip),
            ..Default::default()
        });
        assert!(matches!(
            discover(&s, tmp.path()).unwrap(),
            DiscoveryOutcome::EmptySkip
        ));
    }

    #[test]
    fn invalid_glob_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = cfg();
        s.glob = Some("[".into()); // unterminated character class
        match discover(&s, tmp.path()).unwrap_err() {
            DiscoveryError::InvalidGlob { pattern, .. } => assert_eq!(pattern, "["),
            other => panic!("expected InvalidGlob, got {other:?}"),
        }
    }

    #[test]
    fn invalid_regex_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = cfg();
        s.regex = Some("(unterminated".into());
        match discover(&s, tmp.path()).unwrap_err() {
            DiscoveryError::InvalidRegex { pattern, .. } => {
                assert_eq!(pattern, "(unterminated")
            }
            other => panic!("expected InvalidRegex, got {other:?}"),
        }
    }

    #[test]
    fn double_star_glob_walks_recursively() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir(tmp.path().join("sub")).unwrap();
        write(tmp.path(), "a.csv", "");
        write(&tmp.path().join("sub"), "b.csv", "");
        let mut s = cfg();
        s.glob = Some("**/*.csv".into());
        let out = discover(&s, tmp.path()).unwrap();
        assert_eq!(out.files().len(), 2);
    }
}
